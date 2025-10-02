// Linux-only XDP deps
// taken from agave
// re-export since this is needed at validator startup
#[cfg(target_os = "linux")]
use agave_xdp::{
    device::{NetworkDevice, QueueId},
    load_xdp_program,
    tx_loop::tx_loop,
};
use crossbeam_channel::{Sender, TrySendError, TryRecvError};
use rayon::{iter::{ParallelDrainRange, ParallelIterator}, ThreadPool};
use solana_perf::packet::PacketBatch;
use std::{error::Error, io, net::SocketAddr, num::ParseIntError, sync::{atomic::Ordering, Arc}};
use std::thread::{self, Builder};
use std::time::Duration;
use crate::forwarder::ShredMetrics;

#[derive(Clone, Debug)]
pub struct XdpConfig {
    pub device_name: Option<String>,
    pub cpus: Vec<usize>,
    pub zero_copy_enabled: bool,
    // The capacity of the channel that sits between retransmit stage and each XDP thread that
    // enqueues packets to the NIC.
    pub rtx_channel_cap: usize,
}

impl XdpConfig {
    const DEFAULT_RTX_CHANNEL_CAP: usize = 1_000_000;
}

impl Default for XdpConfig {
    fn default() -> Self {
        Self {
            device_name: None,
            cpus: vec![],
            zero_copy_enabled: false,
            rtx_channel_cap: Self::DEFAULT_RTX_CHANNEL_CAP,
        }
    }
}

impl XdpConfig {
    pub fn new(device_name: Option<impl Into<String>>, cpus: Vec<usize>, zero_copy_enabled: bool) -> Self {
        Self {
            device_name: device_name.map(|s| s.into()),
            cpus,
            zero_copy_enabled,
            rtx_channel_cap: XdpConfig::DEFAULT_RTX_CHANNEL_CAP,
        }
    }
}

#[derive(Clone)]
pub struct XdpSender {
    senders: Vec<Sender<(XdpAddrs, Vec<u8>)>>,
}

#[derive(Clone)]
pub enum XdpAddrs {
    Single(SocketAddr),
    Multi(Arc<Vec<SocketAddr>>),
}

impl From<SocketAddr> for XdpAddrs {
    #[inline]
    fn from(addr: SocketAddr) -> Self {
        XdpAddrs::Single(addr)
    }
}

impl From<Arc<Vec<SocketAddr>>> for XdpAddrs {
    #[inline]
    fn from(addrs: Arc<Vec<SocketAddr>>) -> Self {
        XdpAddrs::Multi(addrs.clone())
    }
}

impl AsRef<[SocketAddr]> for XdpAddrs {
    #[inline]
    fn as_ref(&self) -> &[SocketAddr] {
        match self {
            XdpAddrs::Single(addr) => std::slice::from_ref(addr),
            XdpAddrs::Multi(addrs) => addrs,
        }
    }
}

impl XdpSender {
    #[inline]
    pub fn try_send(
        &self,
        sender_index: usize,
        addr: impl Into<XdpAddrs>,
        payload: Vec<u8>,
    ) -> Result<(), TrySendError<(XdpAddrs, Vec<u8>)>> {
        self.senders[sender_index % self.senders.len()].try_send((addr.into(), payload))
    }
    
    #[inline]
    pub fn try_send_batch(
        &self,
        dests: impl Into<XdpAddrs>,
        packet_batch: &PacketBatch,
    ) -> Result<(), Box<dyn Error>> {
        let addrs = dests.into();
        for (i, packet) in packet_batch.iter().enumerate() {
            if let Some(packet_data) = packet.data(..) {
                let payload = packet_data[..packet.meta().size].to_vec();
                if let Err(_) = self.try_send(i, addrs.clone(), payload) {
                    return Err("XDP channel full".into());
                }
            }
        }
        Ok(())
    }
    
    // #[inline]
    // pub fn try_send_compat(
    //     &self,
    //     dests: Arc<[SocketAddr]>,
    //     packet_batch: Arc<PacketBatch>,
    // ) -> Result<(), TrySendError<(XdpAddrs, Vec<u8>)>> {
    //     // Convert Arc<[SocketAddr]> to Vec<SocketAddr> for XdpAddrs
    //     let addrs = XdpAddrs::Multi(dests.to_vec());
        
    //     // Send each packet in the batch individually
    //     for (i, packet) in packet_batch.iter().enumerate() {
    //         if let Some(packet_data) = packet.data(..) {
    //             let payload = packet_data[..packet.meta().size].to_vec();
    //             self.try_send(i, addrs.clone(), payload)?;
    //         }
    //     }
    //     Ok(())
    // }

    pub fn batch_send_xdp(
        &self,
        thread_pool: &ThreadPool,
        packet_batches: &mut Vec<PacketBatch>,
        destination_sockets: &[SocketAddr],
        metrics: Arc<ShredMetrics>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let dests: Arc<Vec<SocketAddr>> = Arc::new(destination_sockets.to_owned());
        thread_pool.install(|| {
            packet_batches
                .par_drain(..)
                .for_each(|packet_batch| {
                    if self.try_send_batch(<Arc<Vec<SocketAddr>> as Into<XdpAddrs>>::into(dests.clone()), &packet_batch).is_ok() {
                        // Count only non-discarded packets (same as UDP filtering logic)
                        let sent_count = packet_batch.iter().filter(|pkt| pkt.data(..).is_some()).count();
                        metrics.success_forward.fetch_add(sent_count as u64, Ordering::Relaxed);
                    }
                })
        });
        Ok(())
    }
}

#[derive(Default)]
pub struct PacketTransmitter {
    threads: Vec<thread::JoinHandle<()>>,
}


impl PacketTransmitter {
    #[cfg(not(target_os = "linux"))]
    pub fn new(_config: XdpConfig, _src_port: u16) -> Result<(Self, XdpSender), Box<dyn Error>> {
        Err("XDP is only supported on Linux".into())
    }

    #[cfg(target_os = "linux")]
    pub fn new(config: XdpConfig, src_port: u16) -> Result<(Self, XdpSender), Box<dyn Error>> {
        use caps::{
            CapSet,
            Capability::{CAP_BPF, CAP_NET_ADMIN, CAP_NET_RAW},
        };
        const DROP_CHANNEL_CAP: usize = 1_000_000;

        // switch to higher caps while we setup XDP. We assume that an error in
        // this function is irrecoverable so we don't try to drop on errors.
        for cap in [CAP_NET_ADMIN, CAP_NET_RAW, CAP_BPF] {
            caps::raise(None, CapSet::Effective, cap)
                .map_err(|e| format!("failed to raise {cap:?} capability: {e}"))?;
        }

        let dev = Arc::new(if let Some(device_name) = config.device_name {
            NetworkDevice::new(device_name).unwrap()
        } else {
            NetworkDevice::new_from_default_route().unwrap()
        });

        let ebpf = if config.zero_copy_enabled {
            Some(
                load_xdp_program(dev.if_index())
                    .map_err(|e| format!("failed to attach xdp program: {e}"))?,
            )
        } else {
            None
        };

        for cap in [CAP_NET_ADMIN, CAP_NET_RAW, CAP_BPF] {
            caps::drop(None, CapSet::Effective, cap).unwrap();
        }

        let (senders, receivers) = (0..config.cpus.len())
            .map(|_| crossbeam_channel::bounded(config.rtx_channel_cap))
            .unzip::<_, _, Vec<_>, Vec<_>>();

        let mut threads = vec![];

        let (drop_sender, drop_receiver) = crossbeam_channel::bounded(DROP_CHANNEL_CAP);
        threads.push(
            Builder::new()
                .name("PacketRetransmDrop".to_owned())
                .spawn(move || {
                    loop {
                        // drop shreds in a dedicated thread so that we never lock/madvise() from
                        // the xdp thread
                        match drop_receiver.try_recv() {
                            Ok(i) => {
                                drop(i);
                            }
                            Err(TryRecvError::Empty) => {
                                thread::sleep(Duration::from_millis(1));
                            }
                            Err(TryRecvError::Disconnected) => break,
                        }
                    }
                    // move the ebpf program here so it stays attached until we exit
                    drop(ebpf);
                })
                .unwrap(),
        );

        for (i, (receiver, cpu_id)) in receivers
            .into_iter()
            .zip(config.cpus.into_iter())
            .enumerate()
        {
            let dev = Arc::clone(&dev);
            let drop_sender = drop_sender.clone();
            threads.push(
                Builder::new()
                    .name(format!("PacketRetransmIO{i:02}"))
                    .spawn(move || {
                        tx_loop(
                            cpu_id,
                            &dev,
                            QueueId(i as u64),
                            config.zero_copy_enabled,
                            None,
                            None,
                            src_port,
                            dev.mac_addr().ok(),
                            receiver,
                            drop_sender,
                        )
                    })
                    .unwrap(),
            );
        }

        Ok((Self { threads }, XdpSender { senders }))
    }

    pub fn join(self) -> thread::Result<()> {
        for handle in self.threads {
            handle.join()?;
        }
        Ok(())
    }
}

pub fn parse_cpu_ranges(data: &str) -> Result<Vec<usize>, std::io::Error> {
    data.split(',')
        .map(|range| {
            let mut iter = range
                .split('-')
                .map(|s| s.parse::<usize>().map_err(|ParseIntError { .. }| range));
            let start = iter.next().unwrap()?; // str::split always returns at least one element.
            let end = match iter.next() {
                None => start,
                Some(end) => {
                    if iter.next().is_some() {
                        return Err(range);
                    }
                    end?
                }
            };
            Ok(start..=end)
        })
        .try_fold(Vec::new(), |mut cpus, range| {
            let range = range.map_err(|range| io::Error::new(io::ErrorKind::InvalidData, range))?;
            cpus.extend(range);
            Ok(cpus)
        })
}

