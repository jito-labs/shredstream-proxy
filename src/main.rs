use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket},
    panic,
    path::Path,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    thread::{Builder, JoinHandle},
    time::Duration,
};

use clap::Parser;
use crossbeam_channel::RecvTimeoutError;
use env_logger::TimestampPrecision;
use jito_protos::{
    auth::{auth_service_client::AuthServiceClient, Role},
    shredstream::shredstream_client::ShredstreamClient,
};
use log::*;
use solana_client::client_error::ClientError;
use solana_metrics::set_host_id;
use solana_perf::{packet::PacketBatchRecycler, recycler::Recycler};
use solana_sdk::signature::{read_keypair_file, Keypair, Signer};
use solana_streamer::sendmmsg::SendPktsError;
use solana_streamer::{
    sendmmsg::batch_send,
    streamer::{self, PacketBatchReceiver, StreamerError, StreamerReceiveStats},
};
use thiserror::Error;
use tokio::runtime::Runtime;
use tonic::{codegen::InterceptedService, transport::Channel, Status};

use crate::token_authenticator::{
    create_grpc_channel, BlockEngineConnectionError, ClientInterceptor,
};

mod heartbeat;
mod token_authenticator;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Address for Jito Block Engine. See https://jito-labs.gitbook.io/mev/systems/connecting
    #[arg(long, env)]
    block_engine_url: String,

    /// Path to keypair file used to authenticate with the backend
    #[arg(long, env)]
    auth_keypair: String,

    /// Desired regions to receive heartbeats from.
    /// Receives `n` different streams. Requires at least 1 region, comma separated.
    #[arg(long, env, value_delimiter = ',', required(true))]
    desired_regions: Vec<String>,

    /// Address where Shredstream proxy listens on.
    #[arg(long, env, default_value_t = IpAddr::V4(std::net::Ipv4Addr::new(0, 0, 0, 0)))]
    src_bind_addr: IpAddr,

    /// Port where Shredstream proxy on. `0` for random ephemeral port.
    #[arg(long, env, default_value_t = 10_000)]
    src_bind_port: u16,

    /// IP:Port where Shredstream proxy forwards shreds to. Requires at least one IP:Port, comma separated. Eg. `10.0.0.1:9000,10.0.0.2:9000`
    #[arg(long, env, value_delimiter = ',', required(true))]
    dest_sockets: Vec<SocketAddr>,

    /// Number of threads to use. Defaults to use all cores.
    #[arg(long, env)]
    num_threads: Option<usize>,
}

pub async fn get_grpc_client(
    block_engine_url: &str,
    auth_keypair: &Arc<Keypair>,
) -> Result<ShredstreamClient<InterceptedService<Channel, ClientInterceptor>>, ShredstreamProxyError>
{
    let auth_channel = create_grpc_channel(block_engine_url).await?;
    let client_interceptor = ClientInterceptor::new(
        AuthServiceClient::new(auth_channel),
        auth_keypair,
        Role::ShredstreamSubscriber,
    )
    .await?;

    let searcher_channel = create_grpc_channel(block_engine_url).await?;
    let searcher_client = ShredstreamClient::with_interceptor(searcher_channel, client_interceptor);
    Ok(searcher_client)
}

#[derive(Debug, Error)]
pub enum ShredstreamProxyError {
    #[error("TonicError {0}")]
    TonicError(#[from] tonic::transport::Error),
    #[error("GrpcError {0}")]
    GrpcError(#[from] Status),
    #[error("RpcError {0}")]
    RpcError(#[from] ClientError),
    #[error("BlockEngineConnectionError {0}")]
    BlockEngineConnectionError(#[from] BlockEngineConnectionError),
    #[error("StreamerError {0}")]
    StreamerError(#[from] StreamerError),
    #[error("IoError {0}")]
    IoError(#[from] std::io::Error),
    #[error("Shutdown")]
    Shutdown,
}

/// broadcasts same packet to multiple recipients
/// for best performance, connect sockets to their destinations
/// Returns Err when unable to receive packets.
fn send_multiple_destination_from_receiver(
    receiver: &PacketBatchReceiver,
    outgoing_sockets: &Arc<Vec<(UdpSocket, SocketAddr)>>,
    successful_shred_count: &Arc<AtomicU64>,
    failed_shred_count: &Arc<AtomicU64>,
) -> Result<(), ShredstreamProxyError> {
    let packet_batch = match receiver.recv_timeout(Duration::from_micros(500)) {
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

fn main() -> Result<(), ShredstreamProxyError> {
    env_logger::builder()
        .format_timestamp(Some(TimestampPrecision::Micros))
        .init();
    let args = Args::parse();

    let auth_keypair = Arc::new(
        read_keypair_file(Path::new(&args.auth_keypair)).expect("unable parse keypair file"),
    );
    set_host_id(auth_keypair.pubkey().to_string());

    let runtime = Runtime::new().unwrap();
    let shredstream_client = runtime
        .block_on(get_grpc_client(&args.block_engine_url, &auth_keypair))
        .expect("Shredstream client needed");

    let exit = Arc::new(AtomicBool::new(false));
    let panic_hook = panic::take_hook();
    let exit_signal = exit.clone();
    panic::set_hook(Box::new(move |panic_info| {
        // invoke the default handler and exit the process
        exit_signal.store(true, Ordering::SeqCst);
        panic_hook(panic_info);
        error!("exiting process");
    }));
    let heartbeat_hdl = heartbeat::heartbeat_loop_thread(
        shredstream_client,
        args.desired_regions,
        SocketAddr::new(args.src_bind_addr, args.src_bind_port),
        runtime,
        exit.clone(),
    );
    let forward_hdls = start_forwarder_threads(
        args.dest_sockets,
        args.src_bind_port,
        args.num_threads,
        exit,
    );

    for thread in [heartbeat_hdl].into_iter().chain(forward_hdls.into_iter()) {
        thread.join().expect("thread panicked");
    }
    Ok(())
}

/// Bind to ports and start forwarding shreds
fn start_forwarder_threads(
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
    .flat_map(|incoming_shred_socket| {
        let exit = exit.clone();
        let (packet_sender, packet_receiver) = crossbeam_channel::unbounded();
        let listen_thread = streamer::receiver(
            Arc::new(incoming_shred_socket),
            exit.clone(),
            packet_sender,
            recycler.clone(),
            Arc::new(StreamerReceiveStats::new("shredstream-proxy-listen-thread")),
            1,
            true,
            None,
        );

        let outgoing_sockets = outgoing_sockets.clone();
        let successful_shred_count = successful_shred_count.clone();
        let failed_shred_count = failed_shred_count.clone();
        let send_thread = Builder::new()
            .name("shredstream-proxy-send-thread".to_string())
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
                    "Exiting main forward thread, sent {} successful, {} failed shreds",
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
        sync::{Arc, Mutex},
        thread,
        thread::sleep,
        time::Duration,
    };

    use solana_perf::packet::{Meta, Packet, PacketBatch};
    use solana_sdk::packet::{PacketFlags, PACKET_DATA_SIZE};

    use super::*;

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
