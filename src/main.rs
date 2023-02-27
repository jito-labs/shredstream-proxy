use std::{
    io,
    net::{IpAddr, SocketAddr},
    panic,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread,
    thread::sleep,
    time::Duration,
};

use clap::{arg, Parser};
use crossbeam_channel::{Receiver, RecvError, Sender};
use env_logger::TimestampPrecision;
use log::*;
use signal_hook::consts::{SIGINT, SIGTERM};
use solana_client::client_error::{reqwest, ClientError};
use solana_metrics::set_host_id;
use solana_sdk::signature::read_keypair_file;
use thiserror::Error;
use tokio::runtime::Runtime;
use tonic::Status;

use crate::{
    forwarder::ShredMetrics, heartbeat::LogContext, token_authenticator::BlockEngineConnectionError,
};

mod forwarder;
mod heartbeat;
mod token_authenticator;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Address for Jito Block Engine.
    /// See https://jito-labs.gitbook.io/mev/searcher-resources/block-engine#connection-details
    #[arg(long, env)]
    block_engine_url: String,

    /// Path to keypair file used to authenticate with the backend.
    #[arg(long, env)]
    auth_keypair: PathBuf,

    /// Desired regions to receive heartbeats from.
    /// Receives `n` different streams. Requires at least 1 region, comma separated.
    #[arg(long, env, value_delimiter = ',', required(true))]
    desired_regions: Vec<String>,

    /// Address where Shredstream proxy listens.
    #[arg(long, env, default_value_t = IpAddr::V4(std::net::Ipv4Addr::new(0, 0, 0, 0)))]
    src_bind_addr: IpAddr,

    /// Port where Shredstream proxy listens. Use `0` for random ephemeral port.
    #[arg(long, env, default_value_t = 20_000)]
    src_bind_port: u16,

    /// Static set of IP:Port where Shredstream proxy forwards shreds to, comma separated.
    /// Eg. `127.0.0.1:8002,10.0.0.1:8002`.
    #[arg(long, env, value_delimiter = ',')]
    dest_ip_ports: Vec<SocketAddr>,

    /// Http JSON endpoint to dynamically get IPs for Shredstream proxy to forward shreds.
    /// Endpoints are then set-union with `dest-ip-ports`.
    #[arg(long, env)]
    endpoint_discovery_url: Option<String>,

    /// Port to send shreds to for hosts fetched via `endpoint-discovery-url`.
    /// Port can be found using `scripts/get_tvu_port.sh`.
    /// See https://jito-labs.gitbook.io/mev/searcher-services/shredstream#running-shredstream
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
    #[error("RecvError {0}")]
    RecvError(#[from] RecvError),
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
// Creates a channel that gets a message every time `SIGINT` is signalled.
fn shutdown_notifier() -> io::Result<(Sender<()>, Receiver<()>)> {
    let (s, r) = crossbeam_channel::bounded(100);
    let mut signals = signal_hook::iterator::Signals::new([SIGINT, SIGTERM])?;

    let s_thread = s.clone();
    thread::spawn(move || {
        for _ in signals.forever() {
            // send shutdown signal multiple times since crossbeam doesn't have broadcast channels
            // each thread will consume a shutdown signal
            for _ in 0..128 {
                if s_thread.send(()).is_err() {
                    break;
                }
            }
        }
    });

    Ok((s, r))
}
fn main() -> Result<(), ShredstreamProxyError> {
    env_logger::builder()
        .format_timestamp(Some(TimestampPrecision::Micros))
        .init();
    let args = Args::parse();
    set_host_id(hostname::get().unwrap().into_string().unwrap());
    if (args.endpoint_discovery_url.is_none() && args.discovered_endpoints_port.is_some())
        || (args.endpoint_discovery_url.is_some() && args.discovered_endpoints_port.is_none())
    {
        panic!("Invalid arguments provided, dynamic endpoints requires both --endpoint-discovery-url and --discovered-endpoints-port.")
    }
    if args.endpoint_discovery_url.is_none()
        && args.discovered_endpoints_port.is_none()
        && args.dest_ip_ports.is_empty()
    {
        panic!("No destinations found. You must provide values for --dest-ip-ports or --endpoint-discovery-url.")
    }

    let (shutdown_sender, shutdown_receiver) =
        shutdown_notifier().expect("Failed to set up signal handler");
    let exit = Arc::new(AtomicBool::new(false));
    let panic_hook = panic::take_hook();
    {
        let exit = exit.clone();
        panic::set_hook(Box::new(move |panic_info| {
            exit.store(true, Ordering::SeqCst);
            let _ = shutdown_sender.send(());
            error!("exiting process");
            sleep(Duration::from_secs(1));
            // invoke the default handler and exit the process
            panic_hook(panic_info);
        }));
    }
    signal_hook::flag::register(signal_hook::consts::SIGINT, exit.clone())?;
    signal_hook::flag::register(signal_hook::consts::SIGTERM, exit.clone())?;

    let log_context = match args.solana_cluster.is_some() && args.region.is_some() {
        true => Some(LogContext {
            solana_cluster: args.solana_cluster.unwrap(),
            region: args.region.unwrap(),
        }),
        false => None,
    };

    let runtime = Runtime::new().unwrap();
    let auth_keypair = Arc::new(
        read_keypair_file(Path::new(&args.auth_keypair)).unwrap_or_else(|e| {
            panic!(
                "Unable parse keypair file. Ensure that file {:?} is readable. Error: {e}",
                args.auth_keypair
            )
        }),
    );
    let heartbeat_hdl = heartbeat::heartbeat_loop_thread(
        args.block_engine_url,
        &auth_keypair,
        args.desired_regions,
        SocketAddr::new(
            args.public_ip.unwrap_or_else(get_public_ip),
            args.src_bind_port,
        ),
        log_context.clone(),
        runtime,
        shutdown_receiver.clone(),
        exit.clone(),
    );
    // share var between refresh and forwarder thread
    let shared_sockets = Arc::new(Mutex::new(args.dest_ip_ports.clone()));
    let metrics = Arc::new(Mutex::new(ShredMetrics::new(log_context.clone())));
    let mut thread_handles = forwarder::start_forwarder_threads(
        shared_sockets.clone(),
        args.src_bind_port,
        args.num_threads,
        metrics.clone(),
        shutdown_receiver.clone(),
        exit.clone(),
    );
    thread_handles.push(heartbeat_hdl);

    if args.endpoint_discovery_url.is_some() && args.discovered_endpoints_port.is_some() {
        let refresh_handle = forwarder::start_destination_refresh_thread(
            args.endpoint_discovery_url.unwrap(),
            args.discovered_endpoints_port.unwrap(),
            args.dest_ip_ports,
            shared_sockets,
            log_context,
            shutdown_receiver,
            exit,
        );
        thread_handles.push(refresh_handle);
    }

    for thread in thread_handles {
        thread.join().expect("thread panicked");
    }

    let metrics_lock = metrics.lock().unwrap();
    info!(
        "Exiting shredstream, sent {} successful, {} failed shreds, {} duplicate shreds.",
        metrics_lock.agg_success_forward, metrics_lock.agg_fail_forward, metrics_lock.duplicate,
    );
    Ok(())
}
