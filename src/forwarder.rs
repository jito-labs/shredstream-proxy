use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket},
    panic,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, RwLock,
    },
    thread::{Builder, JoinHandle},
    time::Duration,
};

use arc_swap::ArcSwap;
use crossbeam_channel::{Receiver, RecvError};
use itertools::Itertools;
use log::{debug, error, info, warn};
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
    unioned_dest_sockets: Arc<ArcSwap<Vec<SocketAddr>>>, /* sockets shared between endpoint discovery thread and forwarders */
    src_port: u16,
    num_threads: Option<usize>,
    metrics: Arc<Mutex<ShredMetrics>>,
    deduper: Arc<RwLock<Deduper>>,
    shutdown_receiver: Receiver<()>,
    exit: Arc<AtomicBool>,
) -> Vec<JoinHandle<()>> {
    let num_threads = num_threads
        .unwrap_or_else(|| usize::from(std::thread::available_parallelism().unwrap()).max(8));

    let recycler: PacketBatchRecycler = Recycler::warmed(100, 1024);

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

        let deduper = deduper.clone();
        let unioned_dest_sockets = unioned_dest_sockets.clone();
        let metrics = metrics.clone();
        let shutdown_receiver = shutdown_receiver.clone();
        let exit = exit.clone();

        let send_thread = Builder::new()
            .name(format!("shredstream_proxy-send_thread_{thread_id}"))
            .spawn(move || {
                let send_socket =
                    UdpSocket::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0))
                        .expect("to bind to udp port for forwarding");
                let mut local_dest_sockets = unioned_dest_sockets.load();
                let refresh_subscribers_tick = crossbeam_channel::tick(Duration::from_secs(30));
                while !exit.load(Ordering::Relaxed) {
                    crossbeam_channel::select! {
                        // forward packets
                        recv(packet_receiver) -> maybe_packet_batch => {
                           let res = recv_from_channel_and_send_multiple_dest(
                               maybe_packet_batch,
                               &deduper,
                               &send_socket,
                               &local_dest_sockets,
                               &metrics,
                           );

                            // avoid unwrap to prevent log spam from panic handler in each thread
                            if res.is_err(){
                                break;
                            }
                        }
                        // refresh thread-local subscribers
                        recv(refresh_subscribers_tick) -> _ => {
                            local_dest_sockets = unioned_dest_sockets.load();
                        }
                        // handle shutdown (avoid using sleep since it will hang under SIGINT)
                        recv(shutdown_receiver) -> _ => {
                            break;
                        }
                    }
                }
                info!("Exiting forwarder thread {thread_id}.");
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
    deduper: &Arc<RwLock<Deduper>>,
    send_socket: &UdpSocket,
    local_dest_sockets: &Arc<Vec<SocketAddr>>,
    metrics: &Arc<Mutex<ShredMetrics>>,
) -> Result<(), ShredstreamProxyError> {
    let packet_batch = match maybe_packet_batch {
        Ok(x) => Ok(x),
        Err(e) => Err(ShredstreamProxyError::RecvError(e)),
    }?;
    debug!(
        "Got batch of {} packets, total size in bytes: {}",
        packet_batch.len(),
        packet_batch.iter().map(|x| x.meta.size).sum::<usize>()
    );
    let mut packet_batch_vec = vec![packet_batch];

    let num_deduped = deduper.read().unwrap().dedup_packets_and_count_discards(
        &mut packet_batch_vec,
        |_received_packet, _is_already_marked_as_discard, _is_dup| {},
    );

    local_dest_sockets.iter().for_each(|outgoing_socketaddr| {
        let packets = packet_batch_vec[0].iter().filter(|pkt| !pkt.meta.discard()).filter_map(|pkt| {
            let data = pkt.data(..)?;
            let addr = outgoing_socketaddr;
            Some((data, addr))
        }).collect::<Vec<_>>();

        match batch_send(send_socket, &packets) {
            Ok(_) => {
                let mut metrics_guard = metrics.lock().unwrap();
                metrics_guard.agg_success_forward = metrics_guard.agg_success_forward.saturating_add(packets.len() as u64);
                metrics_guard.duplicate = metrics_guard.duplicate.saturating_add(num_deduped);
            }
            Err(SendPktsError::IoError(err, num_failed)) => {
                let mut metrics_guard = metrics.lock().unwrap();
                metrics_guard.agg_fail_forward = metrics_guard.agg_fail_forward.saturating_add(packets.len() as u64);
                metrics_guard.duplicate = metrics_guard.duplicate.saturating_add(num_failed as u64);
                error!("Failed to send batch of size {} to {outgoing_socketaddr:?}. {num_failed} packets failed. Error: {err}", packets.len());
            }
        }
    });
    Ok(())
}

// starts a thread that updates our destinations used by the forwarder threads
pub fn start_destination_refresh_thread(
    endpoint_discovery_url: String,
    discovered_endpoints_port: u16,
    static_dest_sockets: Vec<SocketAddr>,
    unioned_dest_sockets: Arc<ArcSwap<Vec<SocketAddr>>>,
    log_context: Option<LogContext>,
    shutdown_receiver: Receiver<()>,
    exit: Arc<AtomicBool>,
) -> JoinHandle<()> {
    Builder::new()
        .name("shredstream_proxy-destination_refresh_thread".to_string())
        .spawn(move || {
            let socket_tick = crossbeam_channel::tick(Duration::from_secs(30));
            let metrics_tick = crossbeam_channel::tick(Duration::from_secs(30));
            let mut socket_count = static_dest_sockets.len();
            while !exit.load(Ordering::Relaxed) {
                crossbeam_channel::select! {
                    recv(socket_tick) -> _ => {
                        let fetched = fetch_unioned_destinations(
                            &endpoint_discovery_url,
                            discovered_endpoints_port,
                            static_dest_sockets.clone(),
                        );
                        let new_sockets = match fetched {
                            Ok(s) => {
                                info!("Sending shreds to {} destinations: {s:?}", s.len());
                                s
                            }
                            Err(e) => {
                                warn!("Failed to fetch from discovery service, retrying. Error: {e}");
                                if let Some(log_ctx) = &log_context {
                                    datapoint_warn!("shredstream_proxy-destination_refresh_error",
                                                    "solana_cluster" => log_ctx.solana_cluster,
                                                    "region" => log_ctx.region,
                                                    ("prev_unioned_dest_count", socket_count, i64),
                                                    ("errors", 1, i64),
                                                    ("error_str", e.to_string(), String),
                                    );
                                }
                                continue;
                            }
                        };
                        socket_count = new_sockets.len();
                        unioned_dest_sockets.store(Arc::new(new_sockets));
                    }
                    recv(metrics_tick) -> _ => {
                        if let Some(log_ctx) = &log_context {
                            datapoint_info!("shredstream_proxy-destination_refresh_stats",
                                            "solana_cluster" => log_ctx.solana_cluster,
                                            "region" => log_ctx.region,
                                            ("destination_count", socket_count, i64),
                            );
                        }
                    }
                    recv(shutdown_receiver) -> _ => {
                        break;
                    }
                }
            }
        })
        .unwrap()
}

// combines dynamically discovered endpoints with CLI arg defined endpoints
fn fetch_unioned_destinations(
    endpoint_discovery_url: &str,
    discovered_endpoints_port: u16,
    static_dest_sockets: Vec<SocketAddr>,
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

    let unioned_dest_sockets = sockets_json
        .into_iter()
        .map(|ip| SocketAddr::new(ip, discovered_endpoints_port))
        .chain(static_dest_sockets.into_iter())
        .unique()
        .collect::<Vec<SocketAddr>>();
    Ok(unioned_dest_sockets)
}

pub fn start_forwarder_accessory_thread(
    metrics: Arc<Mutex<ShredMetrics>>,
    deduper: Arc<RwLock<Deduper>>,
    shutdown_receiver: Receiver<()>,
    exit: Arc<AtomicBool>,
) -> JoinHandle<()> {
    Builder::new()
        .name("shredstream_proxy-accessory_thread".to_string())
        .spawn(move || {
            let metrics_tick = crossbeam_channel::tick(Duration::from_secs(30));
            let deduper_tick = crossbeam_channel::tick(Duration::from_secs(2));
            while !exit.load(Ordering::Relaxed) {
                crossbeam_channel::select! {
                    // reset deduper to avoid false positives
                    recv(deduper_tick) -> _ => {
                        deduper.write().unwrap().reset();
                    }
                    // send metrics to influx
                    recv(metrics_tick) -> _ => {
                        metrics.lock().unwrap().report();
                    }
                    // handle SIGINT shutdown
                    recv(shutdown_receiver) -> _ => {
                        break;
                    }
                }
            }
        })
        .unwrap()
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
            ("agg_received", self.agg_received, i64),
            ("agg_success_forward", self.agg_success_forward, i64),
            ("agg_fail_forward", self.agg_fail_forward, i64),
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
        sync::{Arc, Mutex, RwLock},
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
            &Arc::new(RwLock::new(Deduper::new(
                MAX_DEDUPER_ITEMS,
                MAX_DEDUPER_AGE,
            ))),
            &udp_sender,
            &Arc::new(dest_socketaddrs),
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
