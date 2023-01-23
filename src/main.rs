use std::{
    net::{IpAddr, SocketAddr},
    panic,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use clap::Parser;
use env_logger::TimestampPrecision;
use log::*;
use solana_client::client_error::{reqwest, ClientError};
use solana_metrics::set_host_id;
use solana_sdk::signature::{read_keypair_file, Signer};
use solana_streamer::streamer::StreamerError;
use thiserror::Error;
use tokio::runtime::Runtime;
use tonic::Status;

use crate::token_authenticator::BlockEngineConnectionError;

mod forwarder;
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
    auth_keypair: PathBuf,

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

fn get_public_ip() -> IpAddr {
    info!("reading public ip from ifconfig.me...");
    let response = reqwest::blocking::get("https://ifconfig.me")
        .expect("response from ifconfig.me")
        .text()
        .expect("public ip response");
    let public_ip = IpAddr::from_str(&response).unwrap();
    info!("public ip: {:?}", public_ip);

    public_ip
}

fn main() -> Result<(), ShredstreamProxyError> {
    env_logger::builder()
        .format_timestamp(Some(TimestampPrecision::Micros))
        .init();
    let args = Args::parse();

    let auth_keypair = Arc::new(
        read_keypair_file(Path::new(&args.auth_keypair)).unwrap_or_else(|e| {
            panic!(
                "Unable parse keypair file. Ensure that {:?} is readable. Error: {e}",
                args.auth_keypair
            )
        }),
    );
    set_host_id(auth_keypair.pubkey().to_string());

    let exit = Arc::new(AtomicBool::new(false));
    let panic_hook = panic::take_hook();
    let exit_signal = exit.clone();
    panic::set_hook(Box::new(move |panic_info| {
        // invoke the default handler and exit the process
        exit_signal.store(true, Ordering::SeqCst);
        panic_hook(panic_info);
        error!("exiting process");
    }));
    let runtime = Runtime::new().unwrap();
    let heartbeat_hdl = heartbeat::heartbeat_loop_thread(
        args.block_engine_url,
        &auth_keypair,
        args.desired_regions,
        SocketAddr::new(get_public_ip(), args.src_bind_port),
        runtime,
        exit.clone(),
    );
    let forward_hdls = forwarder::start_forwarder_threads(
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
