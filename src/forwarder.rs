use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket},
    ops::Sub,
    panic,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    thread::{sleep, Builder, JoinHandle},
    time::Duration,
};

use crossbeam_channel::RecvTimeoutError;
use log::{error, info, warn};
use solana_metrics::datapoint_warn;
use solana_perf::{packet::PacketBatchRecycler, recycler::Recycler};
use solana_streamer::{
    sendmmsg::{batch_send, SendPktsError},
    streamer,
    streamer::{PacketBatchReceiver, StreamerError, StreamerReceiveStats},
};
use tokio::time::Instant;

use crate::{heartbeat::LogContext, ShredstreamProxyError};

/// broadcasts same packet to multiple recipients
/// for best performance, connect sockets to their destinations
/// Returns Err when unable to receive packets.
fn send_multiple_destination_from_receiver(
    receiver: &PacketBatchReceiver,
    outgoing_sockets: &Arc<Vec<(UdpSocket, SocketAddr)>>,
    successful_shred_count: &Arc<AtomicU64>,
    failed_shred_count: &Arc<AtomicU64>,
) -> Result<(), ShredstreamProxyError> {
    let packet_batch = match receiver.recv_timeout(Duration::from_secs(1)) {
        Ok(x) => Ok(x),
        Err(RecvTimeoutError::Timeout) => return Ok(()),
        Err(e) => Err(ShredstreamProxyError::StreamerError(StreamerError::from(e))),
    }?;

    outgoing_sockets
        .iter()
        .for_each(|(outgoing_socket, outgoing_socketaddr)| {
            let packets = packet_batch
                .iter()
                .filter(|pkt| !pkt.meta.discard())
                .filter_map(|pkt| {
                    let data = pkt.data(..)?;
                    let addr = outgoing_socketaddr;
                    Some((data, addr))
                })
                .collect::<Vec<_>>();

            match batch_send(outgoing_socket, &packets) {
                Ok(_) => {
                    successful_shred_count.fetch_add(packets.len() as u64, Ordering::SeqCst);
                }
                Err(SendPktsError::IoError(err, num_failed)) => {
                    failed_shred_count.fetch_add(num_failed as u64, Ordering::SeqCst);
                    error!(
                        "Failed to send batch of size {} to {outgoing_socket:?}. {num_failed} packets failed. Error: {err}",
                        packets.len()
                    );
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
    let sockets = shared_sockets.clone();
    let heartbeat_interval = Duration::from_secs(30);
    Builder::new()
        .name("shredstream_proxy-destination_refresh_thread".to_string())
        .spawn(move || {
            while !exit.load(Ordering::Relaxed) {
                let start = Instant::now();
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
                        warn!("Failed to connect to discovery service, retrying. Error: {e}");
                        if let Some(log_ctx) = &log_context {
                            datapoint_warn!("shredstream_proxy-destination_refresh_error",
                                            "solana_cluster" => log_ctx.solana_cluster,
                                            "region" => log_ctx.region,
                                            ("errors", 1, i64),
                                            ("error_str", e.to_string(), String),
                            );
                        }
                        sleep(heartbeat_interval);
                        continue;
                    }
                };
                let mut sockets = sockets.lock().unwrap();
                sockets.clear();
                sockets.extend(new_sockets);
                drop(sockets);

                let elapsed = start.elapsed();
                if elapsed.lt(&heartbeat_interval) {
                    sleep(heartbeat_interval.sub(elapsed));
                }
            }
        })
        .unwrap()
}

fn fetch_discovered_socketaddrs(
    endpoint_discovery_url: &str,
    discovered_endpoints_port: u16,
    dest_sockets: Vec<SocketAddr>,
) -> reqwest::Result<Vec<SocketAddr>> {
    let sockets = reqwest::blocking::get(endpoint_discovery_url)?
        .json::<Vec<IpAddr>>()?
        .iter()
        .map(|ip| SocketAddr::new(*ip, discovered_endpoints_port))
        .chain(dest_sockets.into_iter())
        .collect::<Vec<SocketAddr>>();
    Ok(sockets)
}

/// Bind to ports and start forwarding shreds
pub fn start_forwarder_threads(
    dst_sockets: Arc<Mutex<Vec<SocketAddr>>>,
    src_port: u16,
    num_threads: Option<usize>,
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
            // sock.connect(dst).unwrap();
            (sock, *dst)
        })
        .collect::<Vec<_>>();
    let outgoing_sockets = Arc::new(outgoing_sockets);
    let recycler: PacketBatchRecycler = Recycler::warmed(100, 1024);
    let successful_shred_count = Arc::new(AtomicU64::new(0));
    let failed_shred_count = Arc::new(AtomicU64::new(0));

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
        let successful_shred_count = successful_shred_count.clone();
        let failed_shred_count = failed_shred_count.clone();
        let send_thread = Builder::new()
            .name(format!("shredstream_proxy-send_thread_{thread_id}"))
            .spawn(move || {
                while !exit.load(Ordering::Relaxed) {
                    send_multiple_destination_from_receiver(
                        &packet_receiver,
                        &outgoing_sockets,
                        &successful_shred_count,
                        &failed_shred_count,
                    )
                    .unwrap();
                }

                info!(
                    "Exiting send thread {thread_id}, sent {} successful, {} failed shreds",
                    successful_shred_count.load(Ordering::SeqCst),
                    failed_shred_count.load(Ordering::SeqCst)
                );
            })
            .unwrap();

        [listen_thread, send_thread]
    })
    .collect::<Vec<JoinHandle<()>>>()
}

#[cfg(test)]
mod tests {
    use std::{
        net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket},
        str::FromStr,
        sync::{atomic::AtomicU64, Arc, Mutex},
        thread,
        thread::sleep,
        time::Duration,
    };

    use solana_perf::packet::{Meta, Packet, PacketBatch};
    use solana_sdk::packet::{PacketFlags, PACKET_DATA_SIZE};

    use crate::forwarder::send_multiple_destination_from_receiver;

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

        // send packets
        send_multiple_destination_from_receiver(
            &packet_receiver,
            &Arc::new(destinations),
            &Arc::new(AtomicU64::new(0)),
            &Arc::new(AtomicU64::new(0)),
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
