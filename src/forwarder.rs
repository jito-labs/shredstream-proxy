use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket},
    panic,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread::{Builder, JoinHandle},
    time::Duration,
};

use crossbeam_channel::RecvError;
use itertools::Itertools;
use log::{error, info, warn};
use solana_metrics::{datapoint_info, datapoint_warn};
use solana_perf::{
    packet::{PacketBatch, PacketBatchRecycler},
    recycler::Recycler,
    sigverify::Deduper,
};
use solana_streamer::{
    sendmmsg::{batch_send, SendPktsError},
    streamer,
    streamer::StreamerReceiveStats,
};

use crate::{heartbeat::LogContext, ShredstreamProxyError};

/// Bind to ports and start forwarding shreds
pub fn start_forwarder_threads(
    dst_sockets: Arc<Mutex<Vec<SocketAddr>>>,
    src_port: u16,
    num_threads: Option<usize>,
    log_context: Option<LogContext>,
    exit: Arc<AtomicBool>,
) -> Vec<JoinHandle<()>> {
    let num_threads =
        num_threads.unwrap_or_else(|| usize::from(std::thread::available_parallelism().unwrap()));

    // all forwarder threads share these sockets
    let outgoing_sockets = dst_sockets
        .lock()
        .unwrap()
        .iter()
        .map(|dst| {
            let sock =
                UdpSocket::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0)).unwrap();
            (sock, *dst)
        })
        .collect::<Vec<_>>();
    let outgoing_sockets = Arc::new(outgoing_sockets);
    let recycler: PacketBatchRecycler = Recycler::warmed(100, 1024);
    let metrics = Arc::new(Mutex::new(ShredMetrics::new(log_context)));

    // spawn a thread for each listen socket. linux kernel will load balance amongst shared sockets
    solana_net_utils::multi_bind_in_range(
        IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
        (src_port, src_port + 1),
        num_threads,
    )
        .unwrap_or_else(|_| {
            panic!("Failed to bind listener sockets. Check that port {src_port} is not in use.")
        })
        .1
        .into_iter()
        .enumerate()
        .flat_map(|(thread_id, incoming_shred_socket)| {
            let exit = exit.clone();
            let metrics = metrics.clone();
            let (packet_sender, packet_receiver) = crossbeam_channel::unbounded();
            let listen_thread = streamer::receiver(
                Arc::new(incoming_shred_socket),
                exit.clone(),
                packet_sender,
                recycler.clone(),
                Arc::new(StreamerReceiveStats::new("shredstream_proxy-listen_thread")),
                0, // do not coalesce since batching consumes more cpu cycles and adds latency.
                true,
                None,
            );

            let outgoing_sockets = outgoing_sockets.clone();
            let send_thread = Builder::new()
                .name(format!("shredstream_proxy-send_thread_{thread_id}"))
                .spawn(move || {
                    const MAX_DEDUPER_AGE: Duration = Duration::from_secs(2);
                    const MAX_DEDUPER_ITEMS: u32 = 1_000_000;
                    let mut deduper = Deduper::new(MAX_DEDUPER_ITEMS, MAX_DEDUPER_AGE);

                    let metrics_tick = crossbeam_channel::tick(Duration::from_secs(30));
                    while !exit.load(Ordering::Relaxed) {
                        deduper.reset();
                        crossbeam_channel::select! {
                            recv(packet_receiver) -> maybe_packet_batch => {
                                recv_from_channel_and_send_multiple_dest(
                                maybe_packet_batch,
                                &deduper,
                                &outgoing_sockets,
                                &metrics,
                                ).unwrap()
                            }
                            recv(metrics_tick) -> _ => {
                                metrics.lock().unwrap().report();
                            }
                        }
                    }

                    let metrics_lock = metrics.lock().unwrap();
                    info!(
                    "Exiting send thread {thread_id}, sent {} successful, {} failed shreds, {} duplicate shreds.",
                    metrics_lock.agg_success_forward,
                    metrics_lock.agg_fail_forward,
                    metrics_lock.duplicate,
                );
                })
                .unwrap();

            [listen_thread, send_thread]
        })
        .collect::<Vec<JoinHandle<()>>>()
}

/// broadcasts same packet to multiple recipients
/// for best performance, connect sockets to their destinations
/// Returns Err when unable to receive packets.
fn recv_from_channel_and_send_multiple_dest(
    maybe_packet_batch: Result<PacketBatch, RecvError>,
    deduper: &Deduper,
    outgoing_sockets: &Arc<Vec<(UdpSocket, SocketAddr)>>,
    metrics: &Arc<Mutex<ShredMetrics>>,
) -> Result<(), ShredstreamProxyError> {
    let packet_batch = match maybe_packet_batch {
        Ok(x) => Ok(x),
        Err(e) => Err(ShredstreamProxyError::RecvError(e)),
    }?;
    info!(
        "Got packet_batch of size {}, total size: {}",
        packet_batch.len(),
        packet_batch.iter().map(|x| x.meta.size).sum::<usize>()
    );
    let mut packet_batch_vec = vec![packet_batch];

    let num_deduped = deduper.dedup_packets_and_count_discards(
        &mut packet_batch_vec,
        #[inline(always)]
        |_received_packet, _is_already_marked_as_discard, _is_dup| {},
    );

    outgoing_sockets.iter().for_each(|(outgoing_socket, outgoing_socketaddr)| {
        let packets = packet_batch_vec[0].iter().filter(|pkt| !pkt.meta.discard()).filter_map(|pkt| {
            let data = pkt.data(..)?;
            let addr = outgoing_socketaddr;
            Some((data, addr))
        }).collect::<Vec<_>>();

        let num_packets = packets.len() as u64;
        match batch_send(outgoing_socket, &packets) {
            Ok(_) => {
                let mut metrics_guard = metrics.lock().unwrap();
                metrics_guard.agg_success_forward = metrics_guard.agg_success_forward.saturating_add(num_packets);
                metrics_guard.duplicate = metrics_guard.duplicate.saturating_add(num_deduped);
            }
            Err(SendPktsError::IoError(err, num_failed)) => {
                let mut metrics_guard = metrics.lock().unwrap();
                metrics_guard.agg_fail_forward = metrics_guard.agg_fail_forward.saturating_add(packets.len() as u64);
                metrics_guard.duplicate = metrics_guard.duplicate.saturating_add(num_deduped);
                error!("Failed to send batch of size {num_packets} to {outgoing_socket:?}. {num_failed} packets failed. Error: {err}");
            }
        }
    });
    Ok(())
}

// starts a thread that updates our destinations used by the forwarder threads
pub fn start_destination_refresh_thread(
    endpoint_discovery_url: String,
    discovered_endpoints_port: u16,
    dest_sockets: Vec<SocketAddr>,
    shared_sockets: Arc<Mutex<Vec<SocketAddr>>>,
    log_context: Option<LogContext>,
    exit: Arc<AtomicBool>,
) -> JoinHandle<()> {
    Builder::new()
        .name("shredstream_proxy-destination_refresh_thread".to_string())
        .spawn(move || {
            let socket_tick = crossbeam_channel::tick(Duration::from_secs(30));
            while !exit.load(Ordering::Relaxed) {
                socket_tick.recv().unwrap();
                let fetched_sockets = fetch_discovered_socketaddrs(
                    &endpoint_discovery_url,
                    discovered_endpoints_port,
                    dest_sockets.clone(),
                );
                let new_sockets = match fetched_sockets {
                    Ok(s) => {
                        info!("Received {} destinations: {s:?}", s.len());
                        s
                    }
                    Err(e) => {
                        warn!("Failed to fetch from discovery service, retrying. Error: {e}");
                        if let Some(log_ctx) = &log_context {
                            datapoint_warn!("shredstream_proxy-destination_refresh_error",
                                            "solana_cluster" => log_ctx.solana_cluster,
                                            "region" => log_ctx.region,
                                            ("errors", 1, i64),
                                            ("error_str", e.to_string(), String),
                            );
                        }
                        continue;
                    }
                };
                let mut sockets = shared_sockets.lock().unwrap();
                sockets.clear();
                sockets.extend(new_sockets);
                drop(sockets);
            }
        })
        .unwrap()
}

// combines dynamically discovered endpoints with CLI arg defined endpoints
fn fetch_discovered_socketaddrs(
    endpoint_discovery_url: &str,
    discovered_endpoints_port: u16,
    dest_sockets: Vec<SocketAddr>,
) -> Result<Vec<SocketAddr>, ShredstreamProxyError> {
    let bytes = reqwest::blocking::get(endpoint_discovery_url)?.bytes()?;

    let sockets_json = match serde_json::from_slice::<Vec<IpAddr>>(&bytes) {
        Ok(s) => s,
        Err(e) => {
            warn!(
                "Failed to parse json from: {:?}",
                std::str::from_utf8(&bytes)
            );
            return Err(ShredstreamProxyError::from(e));
        }
    };

    let all_discovered_sockets = sockets_json
        .into_iter()
        .map(|ip| SocketAddr::new(ip, discovered_endpoints_port))
        .chain(dest_sockets.into_iter())
        .unique()
        .collect::<Vec<SocketAddr>>();
    Ok(all_discovered_sockets)
}

pub struct ShredMetrics {
    /// Total number of shreds received. Includes duplicates when receiving shreds from multiple regions
    pub agg_received: u64,
    /// Total number of shreds successfully forwarded, accounting for all destinations
    pub agg_success_forward: u64,
    /// Total number of shreds failed to forward, accounting for all destinations
    pub agg_fail_forward: u64,
    /// Number of duplicate shreds received
    pub duplicate: u64,
    pub log_context: Option<LogContext>,
}

impl ShredMetrics {
    pub fn new(log_context: Option<LogContext>) -> Self {
        Self {
            agg_received: 0,
            agg_success_forward: 0,
            agg_fail_forward: 0,
            duplicate: 0,
            log_context,
        }
    }

    pub fn report(&self) {
        if let Some(log_context) = &self.log_context {
            datapoint_info!("shredstream_proxy-connection_metrics",
            "solana_cluster" => log_context.solana_cluster,
            "region" => log_context.region,
            ("success", self.agg_success_forward, i64),
            ("fail", self.agg_fail_forward, i64),
            ("duplicate", self.duplicate, i64),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket},
        str::FromStr,
        sync::{Arc, Mutex},
        thread,
        thread::sleep,
        time::Duration,
    };

    use solana_perf::{
        packet::{Meta, Packet, PacketBatch},
        sigverify::Deduper,
    };
    use solana_sdk::packet::{PacketFlags, PACKET_DATA_SIZE};

    use crate::forwarder::{recv_from_channel_and_send_multiple_dest, ShredMetrics};

    fn listen_and_collect(listen_socket: UdpSocket, received_packets: Arc<Mutex<Vec<Vec<u8>>>>) {
        let mut buf = [0u8; PACKET_DATA_SIZE];
        loop {
            listen_socket.recv(&mut buf).unwrap();
            received_packets.lock().unwrap().push(Vec::from(buf));
        }
    }

    #[test]
    fn test_2shreds_3destinations() {
        let packet_batch = PacketBatch::new(vec![
            Packet::new(
                [1; PACKET_DATA_SIZE],
                Meta {
                    size: PACKET_DATA_SIZE,
                    addr: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
                    port: 48289, // received on random port
                    flags: PacketFlags::empty(),
                    sender_stake: 0,
                },
            ),
            Packet::new(
                [2; PACKET_DATA_SIZE],
                Meta {
                    size: PACKET_DATA_SIZE,
                    addr: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
                    port: 9999,
                    flags: PacketFlags::empty(),
                    sender_stake: 0,
                },
            ),
        ]);
        let (packet_sender, packet_receiver) = crossbeam_channel::unbounded::<PacketBatch>();
        packet_sender.send(packet_batch).unwrap();

        let dest_socketaddrs = vec![
            SocketAddr::from_str("0.0.0.0:32881").unwrap(),
            SocketAddr::from_str("0.0.0.0:33881").unwrap(),
            SocketAddr::from_str("0.0.0.0:34881").unwrap(),
        ];

        let test_listeners = dest_socketaddrs
            .iter()
            .map(|socketaddr| {
                (
                    UdpSocket::bind(socketaddr).unwrap(),
                    *socketaddr,
                    // store results in vec of packet, where packet is Vec<u8>
                    Arc::new(Mutex::new(vec![])),
                )
            })
            .collect::<Vec<_>>();

        let udp_sender = UdpSocket::bind("0.0.0.0:10000").unwrap();
        let destinations = dest_socketaddrs
            .iter()
            .map(|destination| (udp_sender.try_clone().unwrap(), destination.to_owned()))
            .collect::<Vec<_>>();

        // spawn listeners
        test_listeners
            .iter()
            .for_each(|(listen_socket, _socketaddr, to_receive)| {
                let socket = listen_socket.try_clone().unwrap();
                let to_receive = to_receive.to_owned();
                thread::spawn(move || listen_and_collect(socket, to_receive));
            });
        const MAX_DEDUPER_AGE: Duration = Duration::from_secs(2);
        const MAX_DEDUPER_ITEMS: u32 = 1_000_000;

        // send packets
        recv_from_channel_and_send_multiple_dest(
            packet_receiver.recv(),
            &Deduper::new(MAX_DEDUPER_ITEMS, MAX_DEDUPER_AGE),
            &Arc::new(destinations),
            &Arc::new(Mutex::new(ShredMetrics::new(None))),
        )
        .unwrap();

        // allow packets to be received
        sleep(Duration::from_millis(500));

        let received = test_listeners
            .iter()
            .map(|(_, _, results)| results.clone())
            .collect::<Vec<_>>();

        // check results
        for received in received.iter() {
            let received = received.lock().unwrap();
            assert_eq!(received.len(), 2);
            assert!(received
                .iter()
                .all(|packet| packet.len() == PACKET_DATA_SIZE));
            assert_eq!(received[0], [1; PACKET_DATA_SIZE]);
            assert_eq!(received[1], [2; PACKET_DATA_SIZE]);
        }

        assert_eq!(
            received
                .iter()
                .fold(0, |acc, elem| acc + elem.lock().unwrap().len()),
            6
        );
    }
}
