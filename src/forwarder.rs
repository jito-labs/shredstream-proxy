use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket},
    panic,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    thread::{Builder, JoinHandle},
    time::Duration,
};

use crossbeam_channel::RecvTimeoutError;
use log::{error, info};
use solana_perf::{packet::PacketBatchRecycler, recycler::Recycler};
use solana_streamer::{
    sendmmsg::{batch_send, SendPktsError},
    streamer,
    streamer::{PacketBatchReceiver, StreamerError, StreamerReceiveStats},
};

use crate::ShredstreamProxyError;

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

/// Bind to ports and start forwarding shreds
pub fn start_forwarder_threads(
    dst_sockets: Vec<SocketAddr>,
    src_port: u16,
    num_threads: Option<usize>,
    exit: Arc<AtomicBool>,
) -> Vec<JoinHandle<()>> {
    let num_threads =
        num_threads.unwrap_or_else(|| usize::from(std::thread::available_parallelism().unwrap()));

    // all forwarder threads share these sockets
    let outgoing_sockets = dst_sockets
        .iter()
        .map(|dst| {
            let sock =
                UdpSocket::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0)).unwrap();
            sock.connect(dst).unwrap();
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
