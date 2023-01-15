mod token_authenticator;

use std::net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket};
use std::ops::Sub;
use std::str::FromStr;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;
use std::thread::{sleep, Builder, JoinHandle};
use std::time::Instant;
use std::{path::Path, sync::Arc, time::Duration};

use clap::Parser;
use crossbeam_channel::RecvTimeoutError;
use env_logger::TimestampPrecision;
use histogram::Histogram;
use log::*;
use solana_client::client_error::ClientError;
use solana_metrics::{datapoint_error, set_host_id};
use solana_sdk::packet::{PacketFlags, PACKET_DATA_SIZE};
use solana_sdk::signature::{read_keypair_file, Keypair, Signer};
use thiserror::Error;
use tokio::runtime::Runtime;
use tonic::{codegen::InterceptedService, transport::Channel, Status};

use jito_protos::auth::auth_service_client::AuthServiceClient;
use jito_protos::auth::Role;

use jito_protos::shredstream::shredstream_client::ShredstreamClient;
use jito_protos::shredstream::Heartbeat;

use crate::token_authenticator::{
    create_grpc_channel, BlockEngineConnectionError, ClientInterceptor,
};

use solana_perf::{
    cuda_runtime::PinnedVec,
    packet::{Meta, Packet, PacketBatch, PacketBatchRecycler},
    recycler::Recycler,
};
use solana_streamer::sendmmsg::{batch_send, multi_target_send, SendPktsError};
use solana_streamer::streamer::{self, PacketBatchReceiver, StreamerError, StreamerReceiveStats};
use tokio::time::interval;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Address for auth service
    #[arg(long, env)]
    auth_addr: String,

    /// Address for searcher service
    #[arg(long, env)]
    shredstream_addr: String,

    /// Path to keypair file used to authenticate with the backend
    #[arg(long, env)]
    auth_keypair: String,

    /// Desired regions to receive heartbeats from.
    /// Receives `n` different streams. Requires at least 1 region, comma separated.
    #[arg(long, env, value_delimiter = ',', required(true))]
    desired_regions: Vec<String>,

    /// Address where Shredstream proxy listens on.
    #[clap(long, env, default_value_t = IpAddr::V4(std::net::Ipv4Addr::new(0, 0, 0, 0)))]
    src_bind_addr: IpAddr,

    /// Port where Shredstream proxy on. `0` for random ephemeral port.
    #[clap(long, env, default_value_t = 10_000)]
    src_bind_port: u16,

    /// IP:Port where Shredstream proxy forwards shreds to. Requires at least one IP:Port, comma separated. Eg. `10.0.0.1:9000,10.0.0.2:9000`
    #[arg(long, env, value_delimiter = ',', required(true))]
    dst_sockets: Vec<SocketAddr>,
}

pub async fn get_client(
    auth_addr: &str,
    searcher_addr: &str,
    auth_keypair: &Arc<Keypair>,
) -> Result<ShredstreamClient<InterceptedService<Channel, ClientInterceptor>>, ShredstreamProxyError>
{
    let auth_channel = create_grpc_channel(auth_addr).await?;
    let client_interceptor = ClientInterceptor::new(
        AuthServiceClient::new(auth_channel),
        auth_keypair,
        Role::ShredstreamSubscriber,
    )
    .await?;

    let searcher_channel = create_grpc_channel(searcher_addr).await?;
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

/// broadcasts same message to multiple recipients
/// for best performance, connect sockets to their destinations
fn send_multiple_destination_from_receiver(
    receiver: &PacketBatchReceiver,
    dst_sockets: Arc<Vec<UdpSocket>>,
) -> Result<(), ShredstreamProxyError> {
    let packet_batch = match receiver.recv_timeout(Duration::from_micros(500)) {
        Ok(x) => Ok(x),
        Err(RecvTimeoutError::Timeout) => return Ok(()),
        Err(e) => Err(ShredstreamProxyError::StreamerError(StreamerError::from(e))),
    }?;

    let packets = packet_batch
        .iter()
        .filter_map(|pkt| {
            let addr = pkt.meta.socket_addr();
            let data = pkt.data(..)?;
            Some((data, addr))
        })
        .collect::<Vec<_>>();
    dst_sockets.iter().for_each(|dst_socket| {
        match batch_send(dst_socket, &packets) {
            Ok(_) => {}
            Err(e) => error!("Failed to batch send to {dst_socket:?} with error: {e}"), //TODO: check if this should be pkt.ip instead
        }
    });
    Ok(())
}
// fn send_from_receiver(
//     tx_socket: &UdpSocket,
//     receiver: &PacketBatchReceiver,
// ) -> Result<(), StreamerError> {
//     let packet_batch = receiver.recv_timeout(Duration::from_micros(500))?;
//     let packets = packet_batch
//         .iter()
//         .filter_map(|pkt| {
//             let addr = pkt.meta.socket_addr();
//             let data = pkt.data(..)?;
//             Some((data, addr))
//         })
//         .collect::<Vec<_>>();
//
//     batch_send(tx_socket, &packets)?;
//     Ok(())
// }
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
    // let shredstream_client = runtime
    //     .block_on(get_client(
    //         &args.auth_addr,
    //         &args.shredstream_addr,
    //         &auth_keypair,
    //     ))
    //     .expect("Shredstream client needed");

    let recv_socketaddr = SocketAddr::new(args.src_bind_addr, args.src_bind_port);
    let exit = Arc::new(AtomicBool::new(false));

    // let heartbeat_hdl = heartbeat_loop_thread(
    //     shredstream_client,
    //     args.desired_regions,
    //     recv_socketaddr,
    //     exit.clone(),
    // );
    let forward_hdls = forward_threads(args.dst_sockets, args.src_bind_port, exit);

    for thread in [].into_iter().chain(forward_hdls.into_iter()) {
        // for thread in [heartbeat_hdl].into_iter().chain(forward_hdls.into_iter()) {
        thread.join().expect("thread panicked");
    }
    Ok(())
}

fn forward_threads(
    dst_sockets: Vec<SocketAddr>,
    src_port: u16,
    exit: Arc<AtomicBool>,
) -> Vec<JoinHandle<()>> {
    // Bind to ports and initialize ShredFetchStage
    let num_threads = usize::from(std::thread::available_parallelism().unwrap());

    let outgoing_sockets = dst_sockets
        .iter()
        .map(|dst| {
            let sock =
                UdpSocket::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0)).unwrap();
            sock.connect(dst).unwrap();
            sock
        })
        .collect::<Vec<_>>();
    let outgoing_sockets = Arc::new(outgoing_sockets);

    let recycler: PacketBatchRecycler = Recycler::warmed(100, 1024);

    // spawn a thread for each listen socket. linux kernel will load balance amongst shared sockets
    let thread_handles = solana_net_utils::multi_bind_in_range(
        IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
        (src_port, src_port + 1),
        num_threads,
    )
    .unwrap_or_else(|_| panic!("to bind shred sockets. Check that port {src_port} is not in use."))
    .1
    .into_iter()
    .flat_map(|incoming_shred_socket| {
        let exit = exit.clone();
        let (packet_sender, packet_receiver) = crossbeam_channel::unbounded();
        let listen_thread = streamer::receiver(
            Arc::new(incoming_shred_socket),
            exit.clone(),
            packet_sender.clone(),
            recycler.clone(),
            Arc::new(StreamerReceiveStats::new("shredstream-proxy-receiver")),
            1,
            true,
            None,
        );
        let outgoing_sockets = outgoing_sockets.clone();

        let sender_thread = Builder::new()
            .name("solReceiver".to_string())
            .spawn(move || {
                while !exit.load(Ordering::Relaxed) {
                    send_multiple_destination_from_receiver(
                        &packet_receiver,
                        outgoing_sockets.clone(),
                    )
                    .unwrap();
                }
            })
            .unwrap();
        [listen_thread, sender_thread]
    })
    .collect::<Vec<JoinHandle<()>>>();
    thread_handles
}

// expects send_socket to be connected to `dst_bind_addr`
pub async fn main_loop(
    recv_socket: UdpSocket,
    send_socket: Arc<UdpSocket>,
    exit: Arc<AtomicBool>,
) -> Result<(), ShredstreamProxyError> {
    let successful_shred_count = Arc::new(AtomicU64::new(0));
    let failed_shred_count = Arc::new(AtomicU64::new(0));
    let histogram = Arc::new(Mutex::new(Histogram::new()));
    let mut buf = [0; PACKET_DATA_SIZE];

    while !exit.load(Ordering::Relaxed) {
        let len = recv_socket.recv(&mut buf)?;
        let start = Instant::now();
        let send_socket = send_socket.clone();
        info!("{:?} bytes received from {:?}", len, send_socket);
        let successful_shred_count = successful_shred_count.clone();
        let failed_shred_count = failed_shred_count.clone();
        let histogram = histogram.clone();
        tokio::spawn(async move {
            match send_socket.send(&buf[..len]) {
                Ok(_) => {
                    let time = start.elapsed();
                    successful_shred_count.fetch_add(1, Ordering::SeqCst);
                    info!("{:?} bytes sent", len);
                    let mut lock = histogram.lock().unwrap();
                    let _ = lock.increment(time.as_micros() as u64);
                }
                Err(_) => {
                    failed_shred_count.fetch_add(1, SeqCst);
                }
            };
        });
    }

    info!(
        "Exiting main forward thread, sent {} successful, {} failed shreds",
        successful_shred_count.load(Ordering::SeqCst),
        failed_shred_count.load(Ordering::SeqCst)
    );
    Err(ShredstreamProxyError::Shutdown)
}

fn heartbeat_loop_thread(
    mut shredstream_client: ShredstreamClient<InterceptedService<Channel, ClientInterceptor>>,
    regions: Vec<String>,
    recv_socket: SocketAddr,
    exit: Arc<AtomicBool>,
) -> JoinHandle<()> {
    Builder::new()
        .name("heartbeat_loop".to_string())
        .spawn(move || {

            let mut successful_heartbeat_count: u64 = 0;
            let mut failed_heartbeat_count: u64 = 0;

            let heartbeat_socket = jito_protos::shared::Socket {
                ip: recv_socket.ip().to_string(),
                port: recv_socket.port() as i64,
            };

            let mut heartbeat_interval = Duration::from_millis(100); //start with 100ms, change based on server suggestion
            let rt = Runtime::new().unwrap();
            while !exit.load(Ordering::Relaxed) {
                let start = Instant::now();
                let result = rt.block_on(shredstream_client
                    .send_heartbeat(Heartbeat {
                        socket: Some(heartbeat_socket.clone()),
                        regions: regions.clone(),
                    }));

                match result {
                    Ok(x) => {
                        heartbeat_interval = Duration::from_millis(x.get_ref().ttl_ms as u64);
                        successful_heartbeat_count += 1;
                    }
                    Err(err) => {
                        warn!("Error sending heartbeat: {err}");
                        datapoint_error!("heartbeat_send_error", ("errors", 1, i64));
                        failed_heartbeat_count += 1;
                    }
                }
                let elapsed= start.elapsed();
                if elapsed.lt(&heartbeat_interval){
                    sleep(heartbeat_interval.sub(elapsed));
                }
            }
            info!("Exiting heartbeat thread, sent {successful_heartbeat_count} successful, {failed_heartbeat_count} failed");
        })
        .unwrap()
}

#[cfg(test)]
mod tests {

    use std::net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket};
    use std::ops::Sub;
    use std::str::FromStr;
    use std::sync::atomic::Ordering::SeqCst;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::Mutex;
    use std::thread::{sleep, Builder, JoinHandle};
    use std::time::Instant;
    use std::{path::Path, sync::Arc, time::Duration};

    use clap::Parser;
    use crossbeam_channel::RecvTimeoutError;
    use env_logger::TimestampPrecision;
    use histogram::Histogram;
    use log::*;
    use solana_client::client_error::ClientError;
    use solana_metrics::{datapoint_error, set_host_id};
    use solana_sdk::packet::{PacketFlags, PACKET_DATA_SIZE};
    use solana_sdk::signature::{read_keypair_file, Keypair, Signer};
    use thiserror::Error;
    use tokio::runtime::Runtime;
    use tonic::{codegen::InterceptedService, transport::Channel, Status};

    use jito_protos::auth::auth_service_client::AuthServiceClient;
    use jito_protos::auth::Role;

    use jito_protos::shredstream::shredstream_client::ShredstreamClient;
    use jito_protos::shredstream::Heartbeat;

    use crate::token_authenticator::{
        create_grpc_channel, BlockEngineConnectionError, ClientInterceptor,
    };

    use solana_perf::{
        cuda_runtime::PinnedVec,
        packet::{Meta, Packet, PacketBatch, PacketBatchRecycler},
        recycler::Recycler,
    };
    use solana_streamer::sendmmsg::{batch_send, multi_target_send, SendPktsError};
    use solana_streamer::streamer::{
        self, PacketBatchReceiver, StreamerError, StreamerReceiveStats,
    };
    use tokio::time::interval;
    fn test() {
        let (packet_sender, packet_receiver) = crossbeam_channel::unbounded::<PacketBatch>();
        let packet_batch = PacketBatch::new(vec![
            Packet::new(
                [1; 1232],
                Meta {
                    size: 1232,
                    addr: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
                    port: 48289,
                    flags: PacketFlags::empty(),
                    sender_stake: 0,
                },
            ),
            Packet::new(
                [2; 1232],
                Meta {
                    size: 1232,
                    addr: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
                    port: 9999,
                    flags: PacketFlags::empty(),
                    sender_stake: 0,
                },
            ),
        ]);
        packet_sender.send(packet_batch).unwrap();
        let udp_sender = UdpSocket::bind("0.0.0.0:10000").unwrap();
        // let incoming_shred_sockets = solana_net_utils::multi_bind_in_range(
        //     IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
        //     (10000, 10000 + 1),
        //     usize::from(std::thread::available_parallelism().unwrap()),
        // )
        // .unwrap();
        // let udp_sender = &incoming_shred_sockets.1[0];
        udp_sender
            .send_to(&[1; 1232], SocketAddr::from_str("127.0.0.1:11000").unwrap())
            .unwrap();
        let mut a = [0u8; 10000];
        loop {
            udp_sender.recv(&mut a);
            dbg!(a);
            udp_sender
                .send_to(&[1; 1232], SocketAddr::from_str("127.0.0.1:9000").unwrap())
                .unwrap();
        }

        // std::thread::sleep(Duration::from_secs(500));
        // //todo check if listening from multiple sources (A,b,c), and can send to d on same udp socket

        // udp_sender.connect(SocketAddr::from_str("").unwrap());
        // udp_sender.send(&[1; 1232]).unwrap();
        // send_from_receiver(&udp_sender, &packet_receiver).unwrap();

        // return Ok(());
    }
}
