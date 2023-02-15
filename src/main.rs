use std::{
    net::{IpAddr, SocketAddr},
    panic,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};

use clap::{arg, Parser};
use env_logger::TimestampPrecision;
use log::*;
use solana_client::client_error::{reqwest, ClientError};
use solana_metrics::set_host_id;
use solana_sdk::signature::read_keypair_file;
use solana_streamer::streamer::StreamerError;
use thiserror::Error;
use tokio::runtime::Runtime;
use tonic::Status;

use crate::{heartbeat::LogContext, token_authenticator::BlockEngineConnectionError};

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

    /// Static set of IP:Port where Shredstream proxy forwards shreds to, comma separated. Eg. `10.0.0.1:9000,10.0.0.2:9000`.
    #[arg(long, env, value_delimiter = ',')]
    dest_ip_ports: Vec<SocketAddr>,

    /// Http JSON endpoint to dynamically get IPs for Shredstream proxy to forward shreds. Endpoints are then set-union with `dest-sockets`.
    #[arg(long, env)]
    endpoint_discovery_url: Option<String>,

    /// Port to send shreds to for hosts fetched via `endpoint-discovery-url`. Port can be found using `scripts/get_tvu_port.sh`.
    #[arg(long, env)]
    discovered_endpoints_port: Option<u16>,

    /// Solana cluster e.g. testnet, mainnet, devnet. Used for logging purposes.
    #[arg(long, env)]
    solana_cluster: Option<String>,

    /// Cluster region. Used for logging purposes.
    #[arg(long, env)]
    region: Option<String>,

    /// Public IP address to use.
    /// Overrides value fetched from `ifconfig.me`.
    #[arg(long, env)]
    public_ip: Option<IpAddr>,

    /// Heartbeat stats sampling probability. Defaults to 100%.
    #[arg(long, env, default_value_t = 1.0)]
    heartbeat_stats_sampling_prob: f64,

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
    #[error("ReqwestError {0}")]
    ReqwestError(#[from] reqwest::Error),
    #[error("SerdeJsonError {0}")]
    SerdeJsonError(#[from] serde_json::Error),
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
    info!("getting public ip from ifconfig.me...");
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
                "Unable parse keypair file. Ensure that file {:?} is readable. Error: {e}",
                args.auth_keypair
            )
        }),
    );

    set_host_id(hostname::get().unwrap().into_string().unwrap());
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

    let log_context = match args.solana_cluster.is_some() && args.region.is_some() {
        true => Some(LogContext {
            solana_cluster: args.solana_cluster.unwrap(),
            region: args.region.unwrap(),
        }),
        false => None,
    };

    // share var between refresh and forwarder thread
    let shared_sockets = Arc::new(Mutex::new(args.dest_ip_ports.clone()));

    let heartbeat_hdl = heartbeat::heartbeat_loop_thread(
        args.block_engine_url,
        &auth_keypair,
        args.desired_regions,
        SocketAddr::new(
            args.public_ip.unwrap_or_else(get_public_ip),
            args.src_bind_port,
        ),
        log_context.clone(),
        args.heartbeat_stats_sampling_prob,
        runtime,
        exit.clone(),
    );
    let mut thread_handles = forwarder::start_forwarder_threads(
        shared_sockets.clone(),
        args.src_bind_port,
        args.num_threads,
        exit.clone(),
    );

    thread_handles.push(heartbeat_hdl);

    if (args.endpoint_discovery_url.is_none() && args.discovered_endpoints_port.is_some())
        || (args.endpoint_discovery_url.is_some() && args.discovered_endpoints_port.is_none())
    {
        panic!("Invalid arguments provided, shredstream proxy requires both --endpoint-discovery-url and --discovered-endpoints-port.")
    }
    if args.endpoint_discovery_url.is_none()
        && args.discovered_endpoints_port.is_none()
        && args.dest_ip_ports.is_empty()
    {
        panic!("No destinations found. You must provide values for --dest-ip-ports or --endpoint-discovery-url.")
    }
    if args.endpoint_discovery_url.is_some() && args.discovered_endpoints_port.is_some() {
        let refresh_handle = forwarder::start_destination_refresh_thread(
            args.endpoint_discovery_url.unwrap(),
            args.discovered_endpoints_port.unwrap(),
            args.dest_ip_ports,
            shared_sockets,
            log_context,
            exit,
        );
        thread_handles.push(refresh_handle);
    }

    for thread in thread_handles {
        thread.join().expect("thread panicked");
    }
    Ok(())
}
