use std::{
    io,
    net::{IpAddr, SocketAddr},
    panic,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
    thread,
    thread::{sleep, JoinHandle},
    time::Duration,
};

use arc_swap::ArcSwap;
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

#[derive(Clone, Debug, Parser)]
#[clap(author, version, about, long_about = None)]
// https://docs.rs/clap/latest/clap/_derive/_cookbook/git_derive/index.html
struct Args {
    #[command(subcommand)]
    shredstream_args: ProxySubcommands,
}

#[derive(Clone, Debug, clap::Subcommand)]
enum ProxySubcommands {
    /// Requests shreds from Jito and sends to all destinations.
    Shredstream(ShredstreamArgs),

    /// Does not request shreds from Jito. Sends anything received on `src-bind-addr`:`src-bind-port` to all destinations.
    ForwardOnly(CommonArgs),
}

#[derive(clap::Args, Clone, Debug)]
struct ShredstreamArgs {
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

    #[clap(flatten)]
    common_args: CommonArgs,
}

#[derive(clap::Args, Clone, Debug)]
struct CommonArgs {
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

    /// Interval between logging stats to CLI and influx
    #[arg(long, env, default_value_t = 15_000)]
    metrics_report_interval_ms: u64,

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
fn shutdown_notifier(exit: Arc<AtomicBool>) -> io::Result<(Sender<()>, Receiver<()>)> {
    let (s, r) = crossbeam_channel::bounded(256);
    let mut signals = signal_hook::iterator::Signals::new([SIGINT, SIGTERM])?;

    let s_thread = s.clone();
    thread::spawn(move || {
        for _ in signals.forever() {
            exit.store(true, Ordering::SeqCst);
            // send shutdown signal multiple times since crossbeam doesn't have broadcast channels
            // each thread will consume a shutdown signal
            for _ in 0..256 {
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
    let all_args: Args = Args::parse();

    let shredstream_args = all_args.shredstream_args.clone();
    // common args
    let args = match all_args.shredstream_args {
        ProxySubcommands::Shredstream(x) => x.common_args,
        ProxySubcommands::ForwardOnly(x) => x,
    };
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

    let exit = Arc::new(AtomicBool::new(false));
    let (shutdown_sender, shutdown_receiver) =
        shutdown_notifier(exit.clone()).expect("Failed to set up signal handler");
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

    let log_context = match args.solana_cluster.is_some() && args.region.is_some() {
        true => Some(LogContext {
            solana_cluster: args.solana_cluster.unwrap(),
            region: args.region.unwrap(),
        }),
        false => None,
    };

    let runtime = Runtime::new().unwrap();
    let (grpc_restart_signal_s, grpc_restart_signal_r) = crossbeam_channel::bounded(1);
    let mut thread_handles = vec![];
    if let ProxySubcommands::Shredstream(args) = shredstream_args {
        let heartbeat_hdl = start_heartbeat(
            args,
            &exit,
            &shutdown_receiver,
            &log_context,
            runtime,
            grpc_restart_signal_r,
        );
        thread_handles.push(heartbeat_hdl);
    }

    // share sockets between refresh and forwarder thread
    let unioned_dest_sockets = Arc::new(ArcSwap::from_pointee(args.dest_ip_ports.clone()));

    // share deduper + metrics between forwarder <-> accessory thread
    const MAX_DEDUPER_AGE: Duration = Duration::from_secs(2);
    const MAX_DEDUPER_ITEMS: u32 = 1_000_000;
    let deduper = Arc::new(RwLock::new(solana_perf::sigverify::Deduper::new(
        MAX_DEDUPER_ITEMS,
        MAX_DEDUPER_AGE,
    )));
    // use mutex since metrics are write heavy. cheaper than rwlock
    let metrics = Arc::new(ShredMetrics::new(log_context.clone()));

    let use_discovery_service =
        args.endpoint_discovery_url.is_some() && args.discovered_endpoints_port.is_some();
    let forwarder_hdls = forwarder::start_forwarder_threads(
        unioned_dest_sockets.clone(),
        args.src_bind_port,
        args.num_threads,
        deduper.clone(),
        metrics.clone(),
        use_discovery_service,
        shutdown_receiver.clone(),
        exit.clone(),
    );
    thread_handles.extend(forwarder_hdls);

    let metrics_hdl = forwarder::start_forwarder_accessory_thread(
        deduper,
        metrics.clone(),
        args.metrics_report_interval_ms,
        grpc_restart_signal_s,
        shutdown_receiver.clone(),
        exit.clone(),
    );
    thread_handles.push(metrics_hdl);
    if use_discovery_service {
        let refresh_handle = forwarder::start_destination_refresh_thread(
            args.endpoint_discovery_url.unwrap(),
            args.discovered_endpoints_port.unwrap(),
            args.dest_ip_ports,
            unioned_dest_sockets,
            log_context,
            shutdown_receiver,
            exit,
        );
        thread_handles.push(refresh_handle);
    }

    info!(
        "Shredstream started, listening on {}:{}/udp.",
        args.src_bind_addr, args.src_bind_port
    );

    for thread in thread_handles {
        thread.join().expect("thread panicked");
    }

    info!(
        "Exiting Shredstream, {} received , {} sent successfully, {} failed, {} duplicate shreds.",
        metrics.agg_received_cumulative.load(Ordering::Relaxed),
        metrics
            .agg_success_forward_cumulative
            .load(Ordering::Relaxed),
        metrics.agg_fail_forward_cumulative.load(Ordering::Relaxed),
        metrics.duplicate_cumulative.load(Ordering::Relaxed),
    );
    Ok(())
}

fn start_heartbeat(
    args: ShredstreamArgs,
    exit: &Arc<AtomicBool>,
    shutdown_receiver: &Receiver<()>,
    log_context: &Option<LogContext>,
    runtime: Runtime,
    grpc_restart_signal_r: Receiver<()>,
) -> JoinHandle<()> {
    let auth_keypair = Arc::new(
        read_keypair_file(Path::new(&args.auth_keypair)).unwrap_or_else(|e| {
            panic!(
                "Unable to parse keypair file. Ensure that file {:?} is readable. Error: {e}",
                args.auth_keypair
            )
        }),
    );
    let heartbeat_hdl = heartbeat::heartbeat_loop_thread(
        args.block_engine_url,
        &auth_keypair,
        args.desired_regions,
        SocketAddr::new(
            args.common_args.public_ip.unwrap_or_else(get_public_ip),
            args.common_args.src_bind_port,
        ),
        log_context.clone(),
        runtime,
        grpc_restart_signal_r,
        shutdown_receiver.clone(),
        exit.clone(),
    );
    heartbeat_hdl
}
