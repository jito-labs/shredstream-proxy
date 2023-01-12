mod token_authenticator;

use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;
use std::{path::Path, sync::Arc, time::Duration};

use clap::Parser;
use env_logger::TimestampPrecision;
use histogram::Histogram;
use log::*;
use solana_client::client_error::ClientError;
use solana_metrics::{datapoint_error, set_host_id};
use solana_sdk::packet::PACKET_DATA_SIZE;
use solana_sdk::signature::{read_keypair_file, Keypair, Signer};
use thiserror::Error;
use tokio::net::UdpSocket;
use tokio::runtime::Runtime;
use tokio::sync::Mutex;
use tokio::time::interval;
use tonic::{codegen::InterceptedService, transport::Channel, Status};

use jito_protos::auth::auth_service_client::AuthServiceClient;
use jito_protos::auth::Role;

use jito_protos::shredstream::shredstream_client::ShredstreamClient;
use jito_protos::shredstream::Heartbeat;

use crate::token_authenticator::{
    create_grpc_channel, BlockEngineConnectionError, ClientInterceptor,
};
use crate::SearcherProxyError::Shutdown;

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
) -> Result<ShredstreamClient<InterceptedService<Channel, ClientInterceptor>>, SearcherProxyError> {
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
pub enum SearcherProxyError {
    #[error("TonicError {0}")]
    TonicError(#[from] tonic::transport::Error),
    #[error("GrpcError {0}")]
    GrpcError(#[from] Status),
    #[error("RpcError {0}")]
    RpcError(#[from] ClientError),
    #[error("BlockEngineConnectionError {0}")]
    BlockEngineConnectionError(#[from] BlockEngineConnectionError),
    #[error("IoError {0}")]
    IoError(#[from] std::io::Error),
    #[error("Shutdown")]
    Shutdown,
}

fn main() -> Result<(), SearcherProxyError> {
    env_logger::builder()
        .format_timestamp(Some(TimestampPrecision::Micros))
        .init();
    let args = Args::parse();

    let auth_keypair =
        Arc::new(read_keypair_file(Path::new(&args.auth_keypair)).expect("parse kp file"));

    let exit = Arc::new(AtomicBool::new(false));
    set_host_id(auth_keypair.pubkey().to_string());
    let runtime = Runtime::new().unwrap();
    runtime.block_on(async move {
        let shredstream_client = get_client(&args.auth_addr, &args.shredstream_addr, &auth_keypair)
            .await
            .expect("Shredstream client needed");
        let recv_socketaddr = SocketAddr::new(args.src_bind_addr, args.src_bind_port);
        let recv_socket = UdpSocket::bind(recv_socketaddr).await.expect("Must bind");

        let sock = Arc::new(UdpSocket::bind("0.0.0.0:0").await.expect("Must bind)"));
        // sock.connect(send_socketaddr).await.expect("must bind");
        let heartbeat_hdl = tokio::spawn(heartbeat_loop(
            shredstream_client,
            args.desired_regions,
            recv_socketaddr,
            exit.clone(),
        ));
        let fwd_hdl = tokio::spawn(main_loop(recv_socket, sock, exit));

        let thread_hdls = vec![heartbeat_hdl, fwd_hdl];
        thread_hdls
    });
    Ok(())
}

// expects send_socket to be connected to `dst_bind_addr`
pub async fn main_loop(
    recv_socket: UdpSocket,
    send_socket: Arc<UdpSocket>,
    exit: Arc<AtomicBool>,
) -> Result<(), SearcherProxyError> {
    let successful_shred_count = Arc::new(AtomicU64::new(0));
    let failed_shred_count = Arc::new(AtomicU64::new(0));
    let histogram = Arc::new(Mutex::new(Histogram::new()));
    let mut buf = [0; PACKET_DATA_SIZE];

    while !exit.load(Ordering::Relaxed) {
        let len = recv_socket.recv(&mut buf).await?;
        let start = Instant::now();
        let send_socket = send_socket.clone();
        info!("{:?} bytes received from {:?}", len, send_socket);
        let successful_shred_count = successful_shred_count.clone();
        let failed_shred_count = failed_shred_count.clone();
        let histogram = histogram.clone();
        tokio::spawn(async move {
            match send_socket.send(&buf[..len]).await {
                Ok(_) => {
                    let time = start.elapsed();
                    successful_shred_count.fetch_add(1, Ordering::SeqCst);
                    info!("{:?} bytes sent", len);
                    let mut lock = histogram.lock().await;
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
    Err(Shutdown)
}

pub async fn heartbeat_loop(
    mut shredstream_client: ShredstreamClient<InterceptedService<Channel, ClientInterceptor>>,
    regions: Vec<String>,
    recv_socket: SocketAddr,
    exit: Arc<AtomicBool>,
) -> Result<(), SearcherProxyError> {
    let mut successful_heartbeat_count: u64 = 0;
    let mut failed_heartbeat_count: u64 = 0;

    let heartbeat_socket = jito_protos::shared::Socket {
        ip: recv_socket.ip().to_string(),
        port: recv_socket.port() as i64,
    };

    let mut heartbeat_interval = interval(Duration::from_millis(100)); //start with 100ms, change based on server suggestion
    while !exit.load(Ordering::Relaxed) {
        let result = shredstream_client
            .send_heartbeat(Heartbeat {
                socket: Some(heartbeat_socket.clone()),
                regions: regions.clone(),
            })
            .await;

        match result {
            Ok(x) => {
                heartbeat_interval = interval(Duration::from_millis(x.get_ref().ttl_ms as u64));
                successful_heartbeat_count += 1;
            }
            Err(err) => {
                warn!("Error sending heartbeat: {err}");
                datapoint_error!("heartbeat_send_error", ("errors", 1, i64));
                failed_heartbeat_count += 1;
            }
        }
        heartbeat_interval.tick().await;
    }
    info!("Exiting heartbeat thread, sent {successful_heartbeat_count} successful, {failed_heartbeat_count} failed");
    Err(Shutdown)
}

#[cfg(test)]
mod tests {}
