use std::{
    collections::{BTreeSet, HashMap},
    fs,
    io::{self, Error, ErrorKind},
    net::{IpAddr, Ipv4Addr, SocketAddr, ToSocketAddrs},
    panic,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, RwLock,
    },
    thread::{self, sleep, spawn, JoinHandle},
    time::Duration,
};

use agave_xdp::set_cpu_affinity;
use arc_swap::ArcSwap;
use clap::{arg, Parser, ValueEnum};
use crossbeam_channel::{Receiver, RecvError, Sender};
use log::*;
use signal_hook::consts::{SIGINT, SIGTERM};
use solana_client::client_error::{reqwest, ClientError};
use solana_ledger::shred::Shred;
use solana_metrics::set_host_id;
use solana_perf::deduper::Deduper;
use solana_sdk::{clock::Slot, signature::read_keypair_file};
use solana_streamer::streamer::StreamerReceiveStats;
use thiserror::Error;
use tokio::{runtime::Runtime, sync::broadcast::Sender as BroadcastSender};
use tonic::Status;

use crate::{
    forwarder::ShredMetrics, multicast_config::create_multicast_socket_on_device,
    token_authenticator::BlockEngineConnectionError, xdp::PacketTransmitter,
};
mod deshred;
pub mod forwarder;
mod heartbeat;
mod multicast_config;
mod server;
mod token_authenticator;
mod xdp;

#[cfg(target_os = "linux")]
use xdp::XdpConfig;

#[derive(Clone, Debug, Parser)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    shredstream_args: ProxySubcommands,
}

#[derive(Clone, Debug, clap::Subcommand)]
enum ProxySubcommands {
    Shredstream(ShredstreamArgs),
    ForwardOnly(CommonArgs),
}

#[derive(clap::ValueEnum, Clone, Debug)]
enum PacketTransmissionMode {
    Xdp,
    Udp,
}

#[derive(ValueEnum, Clone, Debug)]
enum XdpCpuMode {
    Auto,
    Manual,
}

#[derive(clap::Args, Clone, Debug)]
struct ShredstreamArgs {
    /// Address for Jito Block Engine.
    #[arg(long, env)]
    block_engine_url: String,

    /// Manual override for auth service address. For internal use.
    #[arg(long, env)]
    auth_url: Option<String>,

    /// Path to keypair file used to authenticate with the backend.
    #[arg(long, env)]
    auth_keypair: PathBuf,

    /// Desired regions to receive heartbeats from.
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

    /// Multicast IP to listen for shreds. If none provided, attempts to
    /// parse multicast routes for the device specified by `--multicast-device`
    /// via `ip --json route show dev <device>`.
    #[arg(long, env)]
    multicast_bind_ip: Option<IpAddr>,

    /// Network device to use for multicast route discovery and interface selection.
    /// Example: `eth0`, `en0`, or `doublezero1`.
    #[arg(long, env, default_value = "doublezero1")]
    multicast_device: String,

    /// Port to receive multicast shreds
    #[arg(long, env, default_value_t = 20001)]
    multicast_subscribe_port: u16,

    /// Static set of IP:Port where Shredstream proxy forwards shreds to, comma separated.
    /// Eg. `127.0.0.1:8001,10.0.0.1:8001`.
    // Note: store the original string, so we can do hostname resolution when refreshing destinations
    /// Static set of IP:Port to forward to, comma separated.
    #[arg(long, env, value_delimiter = ',', value_parser = resolve_hostname_port)]
    dest_ip_ports: Vec<(SocketAddr, String)>,

    /// JSON endpoint to dynamically get IPs for forwarding.
    #[arg(long, env)]
    endpoint_discovery_url: Option<String>,

    /// Port to send shreds to for hosts fetched via discovery URL.
    #[arg(long, env)]
    discovered_endpoints_port: Option<u16>,

    /// Interval between logging stats to stdout and influx
    #[arg(long, env, default_value_t = 15_000)]
    metrics_report_interval_ms: u64,

    /// Logs trace shreds to stdout and influx
    #[arg(long, env, default_value_t = false)]
    debug_trace_shred: bool,

    /// GRPC port for serving decoded shreds as Solana entries
    #[arg(long, env)]
    grpc_service_port: Option<u16>,

    /// Public IP address override.
    #[arg(long, env)]
    public_ip: Option<IpAddr>,

    /// Packet transmission mode: XDP or UDP
    #[arg(long, env, value_enum, default_value_t = PacketTransmissionMode::Udp)]
    packet_transmission_mode: PacketTransmissionMode,

    /// Network interface used for XDP and auto CPU discovery
    #[arg(long, env, default_value = "enp13s0u5")]
    iface: String,

    /// CPU selection mode for XDP TX threads
    #[arg(long, env, value_enum, default_value_t = XdpCpuMode::Auto)]
    xdp_cpu_mode: XdpCpuMode,

    /// CPUs (ranges) for manual mode, e.g. "8-11" or "0,2,4,6"
    #[arg(long, env)]
    xdp_cpus: Option<String>,
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
    IoError(#[from] io::Error),
    #[error("Shutdown")]
    Shutdown,
}

fn resolve_hostname_port(hostname_port: &str) -> io::Result<(SocketAddr, String)> {
    let socketaddr = hostname_port.to_socket_addrs()?.next().ok_or_else(|| {
        Error::new(
            ErrorKind::AddrNotAvailable,
            format!("Could not find destination {hostname_port}"),
        )
    })?;
    Ok((socketaddr, hostname_port.to_string()))
}

/// Returns public-facing IPV4 address
pub fn get_public_ip() -> reqwest::Result<IpAddr> {
    info!("Requesting public ip from ifconfig.me...");
    let client = reqwest::blocking::Client::builder()
        .local_address(IpAddr::V4(Ipv4Addr::UNSPECIFIED))
        .build()?;
    let response = client.get("https://ifconfig.me/ip").send()?.text()?;
    let public_ip = IpAddr::from_str(&response).unwrap();
    info!("Retrieved public ip: {public_ip:?}");
    Ok(public_ip)
}

fn shutdown_notifier(exit: Arc<AtomicBool>) -> io::Result<(Sender<()>, Receiver<()>)> {
    let (s, r) = crossbeam_channel::bounded(256);
    let mut signals = signal_hook::iterator::Signals::new([SIGINT, SIGTERM])?;

    let s_thread = s.clone();
    thread::spawn(move || {
        for _ in signals.forever() {
            exit.store(true, Ordering::SeqCst);
            // broadcast-ish: push many tokens so all threads can drain one
            for _ in 0..256 {
                if s_thread.try_send(()).is_err() {
                    break;
                }
            }
        }
    });
    Ok((s, r))
}

pub type ReconstructedShredsMap = HashMap<Slot, HashMap<u32 /* fec_set_index */, Vec<Shred>>>;

fn main() -> Result<(), ShredstreamProxyError> {
    env_logger::builder().init();

    let all_args: Args = Args::parse();

    let shredstream_args = all_args.shredstream_args.clone();
    let args = match all_args.shredstream_args {
        ProxySubcommands::Shredstream(x) => x.common_args,
        ProxySubcommands::ForwardOnly(x) => x,
    };

    set_host_id(hostname::get()?.into_string().unwrap());

    if (args.endpoint_discovery_url.is_none() && args.discovered_endpoints_port.is_some())
        || (args.endpoint_discovery_url.is_some() && args.discovered_endpoints_port.is_none())
    {
        return Err(ShredstreamProxyError::IoError(io::Error::new(ErrorKind::InvalidInput, "Invalid arguments provided, dynamic endpoints requires both --endpoint-discovery-url and --discovered-endpoints-port.")));
    }
    if args.endpoint_discovery_url.is_none()
        && args.discovered_endpoints_port.is_none()
        && args.dest_ip_ports.is_empty()
    {
        return Err(ShredstreamProxyError::IoError(io::Error::new(ErrorKind::InvalidInput, "No destinations found. You must provide values for --dest-ip-ports or --endpoint-discovery-url.")));
    }

    let num_threads: usize;

    let (transmitter_threads, xdp_sender) =
        if matches!(args.packet_transmission_mode, PacketTransmissionMode::Xdp) {
            // Choose CPUs for XDP TX threads
            let xdp_cpus: Vec<usize> = match args.xdp_cpu_mode {
                XdpCpuMode::Auto => match discover_nic_queue_cpus(&args.iface) {
                    Ok(v) if !v.is_empty() => {
                        info!(
                            "XDP auto: discovered queue CPUs for {}: {:?}",
                            &args.iface, v
                        );
                        v
                    }
                    _ => {
                        let fallback = vec![8, 9, 10, 11];
                        warn!(
                        "XDP auto: could not discover CPUs for {}, falling back to default {:?}",
                        &args.iface, fallback
                    );
                        fallback
                    }
                },
                XdpCpuMode::Manual => {
                    if let Some(s) = args.xdp_cpus.as_deref() {
                        parse_cpu_ranges(s).expect("failed to parse --xdp-cpus")
                    } else {
                        let fallback = vec![8, 9, 10, 11];
                        warn!(
                            "XDP manual: --xdp-cpus not provided, using default {:?}",
                            fallback
                        );
                        fallback
                    }
                }
            };

            // Reserve those CPUs for XDP; pin other threads to the complement
            let all: BTreeSet<usize> = core_affinity::get_core_ids()
                .as_ref()
                .into_iter()
                .flatten()
                .map(|c| c.id)
                .collect();
            let reserved: BTreeSet<usize> = xdp_cpus.iter().copied().collect();
            let available: Vec<usize> = all.difference(&reserved).copied().collect();

            info!("Reserved (XDP) CPUs: {:?}", xdp_cpus);
            info!("Available CPUs for other threads: {:?}", available);

            if !available.is_empty() {
                set_cpu_affinity(available.clone()).unwrap();
            }

            num_threads = available.len().max(1);

            let xdp_config = XdpConfig {
                device_name: Some(args.iface.clone()),
                cpus: xdp_cpus.clone(),
                zero_copy_enabled: false,
                rtx_channel_cap: 1_000_000,
            };

            let (transmitter_threads, xdp_sender) =
                PacketTransmitter::new(xdp_config, args.src_bind_port)
                    .expect("Failed to create xdp transmitter");
            (transmitter_threads, Some(Arc::new(xdp_sender)))
        } else {
            num_threads = usize::from(std::thread::available_parallelism().unwrap()).min(4);
            (PacketTransmitter::default(), None)
        };

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
            panic_hook(panic_info);
        }));
    }

    let metrics = Arc::new(ShredMetrics::new(args.grpc_service_port.is_some()));

    let runtime = Runtime::new()?;
    let mut thread_handles = vec![];

    if let ProxySubcommands::Shredstream(args) = shredstream_args {
        if args.desired_regions.len() > 2 {
            warn!(
                "Too many regions requested, only regions: {:?} will be used",
                &args.desired_regions[..2]
            );
        }
        let heartbeat_hdl =
            start_heartbeat(args, &exit, &shutdown_receiver, runtime, metrics.clone());
        thread_handles.push(heartbeat_hdl);
    }

    let unioned_dest_sockets = Arc::new(ArcSwap::from_pointee(
        args.dest_ip_ports
            .iter()
            .map(|x| x.0)
            .collect::<Vec<SocketAddr>>(),
    ));

    let deduper = Arc::new(RwLock::new(Deduper::<2, [u8]>::new(
        &mut rand::thread_rng(),
        forwarder::DEDUPER_NUM_BITS,
    )));

    let entry_sender = Arc::new(BroadcastSender::new(100));
    let forward_stats = Arc::new(StreamerReceiveStats::new("shredstream_proxy-listen_thread"));
    let refresh_signal_counter = Arc::new(AtomicU64::new(0));
    let use_discovery_service =
        args.endpoint_discovery_url.is_some() && args.discovered_endpoints_port.is_some();
    let maybe_multicast_socket = create_multicast_socket_on_device(
        &args.multicast_device,
        args.multicast_subscribe_port,
        args.multicast_bind_ip,
    )
    .inspect(|mcast_socket| info!("Multicast listeners found: {mcast_socket:?}."));

    let forwarder_hdls = forwarder::start_forwarder_threads_new(
        unioned_dest_sockets.clone(),
        args.src_bind_addr,
        args.src_bind_port,
        num_threads,
        deduper.clone(),
        args.grpc_service_port.is_some(),
        entry_sender.clone(),
        args.debug_trace_shred,
        use_discovery_service,
        forward_stats.clone(),
        metrics.clone(),
        refresh_signal_counter.clone(),
        shutdown_receiver.clone(),
        exit.clone(),
        xdp_sender.clone(),
    );
    thread_handles.extend(forwarder_hdls);

    let report_metrics_thread = {
        let exit = exit.clone();
        spawn(move || {
            while !exit.load(Ordering::Relaxed) {
                sleep(Duration::from_secs(1));
                forward_stats.report();
            }
        })
    };
    thread_handles.push(report_metrics_thread);

    let metrics_hdl = forwarder::start_forwarder_accessory_thread(
        deduper,
        metrics.clone(),
        args.metrics_report_interval_ms,
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
            shutdown_receiver.clone(),
            refresh_signal_counter.clone(),
            exit.clone(),
        );
        thread_handles.push(refresh_handle);
    }

    if let Some(port) = args.grpc_service_port {
        let server_hdl = server::start_server_thread(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port),
            entry_sender.clone(),
            exit.clone(),
            shutdown_receiver.clone(),
        );
        thread_handles.push(server_hdl);
    }

    // Proactively drop the XDP sender before waiting on other threads
    if let Some(sender) = xdp_sender {
        drop(sender);
    }

    for thread in thread_handles {
        thread.join().expect("thread panicked");
    }

    transmitter_threads
        .join()
        .expect("xdp worker threads panicked");

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
    runtime: Runtime,
    metrics: Arc<ShredMetrics>,
) -> JoinHandle<()> {
    let auth_keypair = Arc::new(
        read_keypair_file(Path::new(&args.auth_keypair)).unwrap_or_else(|e| {
            panic!(
                "Unable to parse keypair file. Ensure that file {:?} is readable. Error: {e}",
                args.auth_keypair
            )
        }),
    );

    heartbeat::heartbeat_loop_thread(
        args.block_engine_url.clone(),
        args.auth_url.unwrap_or(args.block_engine_url),
        auth_keypair,
        args.desired_regions,
        SocketAddr::new(
            args.common_args
                .public_ip
                .unwrap_or_else(|| get_public_ip().unwrap()),
            args.common_args.src_bind_port,
        ),
        runtime,
        "shredstream_proxy".to_string(),
        metrics,
        shutdown_receiver.clone(),
        exit.clone(),
    )
}

/// YOUR helper: parse comma/range list like "8-11" or "0,2,4,6"
pub fn parse_cpu_ranges(data: &str) -> Result<Vec<usize>, std::io::Error> {
    use std::num::ParseIntError;
    data.split(',')
        .map(|range| {
            let mut iter = range
                .split('-')
                .map(|s| s.parse::<usize>().map_err(|ParseIntError { .. }| range));
            let start = iter.next().unwrap()?; // str::split always returns at least one element.
            let end = match iter.next() {
                None => start,
                Some(end) => {
                    if iter.next().is_some() {
                        return Err(range);
                    }
                    end?
                }
            };
            Ok(start..=end)
        })
        .try_fold(Vec::new(), |mut cpus, range| {
            let range = range.map_err(|range| io::Error::new(io::ErrorKind::InvalidData, range))?;
            cpus.extend(range);
            Ok(cpus)
        })
}

/// Read /proc/interrupts and return IRQs for `iface` TxRx queues
fn find_iface_queue_irqs(iface: &str) -> io::Result<Vec<u32>> {
    let data = fs::read_to_string("/proc/interrupts")?;
    let mut irqs = Vec::new();
    for line in data.lines() {
        if line.contains(iface) && line.contains("TxRx") {
            if let Some((left, _)) = line.split_once(':') {
                if let Ok(irq) = left.trim().parse::<u32>() {
                    irqs.push(irq);
                }
            }
        }
    }
    Ok(irqs)
}

/// Read /proc/irq/<IRQ>/smp_affinity_list and parse using parse_cpu_ranges
fn read_irq_affinity_list(irq: u32) -> io::Result<Vec<usize>> {
    let path = format!("/proc/irq/{}/smp_affinity_list", irq);
    let s = fs::read_to_string(&path)?.trim().to_string();
    parse_cpu_ranges(&s)
}

/// Discover NIC queue CPUs for `iface`
fn discover_nic_queue_cpus(iface: &str) -> io::Result<Vec<usize>> {
    let irqs = find_iface_queue_irqs(iface)?;
    let mut cpus = Vec::new();
    for irq in irqs {
        if let Ok(mut v) = read_irq_affinity_list(irq) {
            cpus.append(&mut v);
        }
    }
    cpus.sort_unstable();
    cpus.dedup();
    Ok(cpus)
}
