use std::{
    net::{IpAddr, Ipv6Addr, SocketAddr, UdpSocket},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, RwLock,
    },
    thread::{Builder, JoinHandle},
    time::{Duration, SystemTime},
};

use arc_swap::ArcSwap;
use crossbeam_channel::{Receiver, RecvError};
use dashmap::DashMap;
use itertools::Itertools;
use jito_protos::shredstream::{Entry as PbEntry, TraceShred};
use log::{debug, error, info, warn};
use prost::Message;
use solana_client::client_error::reqwest;
use solana_ledger::shred::{
    merkle::{ShredCode as MerkleCodeShred, ShredData as MerkleDataShred},
    traits::Shred as ShredTrait,
    ReedSolomonCache,
};
use solana_metrics::{datapoint_info, datapoint_warn};
use solana_net_utils::SocketConfig;
use solana_perf::{
    deduper::Deduper,
    packet::{PacketBatch, PacketBatchRecycler},
    recycler::Recycler,
};
use solana_sdk::clock::Slot;
use solana_streamer::{
    sendmmsg::{batch_send, SendPktsError},
    streamer::{self, StreamerReceiveStats},
};
use tokio::sync::broadcast::Sender;

use crate::{deshred, resolve_hostname_port, ShredstreamProxyError};

// values copied from https://github.com/solana-labs/solana/blob/33bde55bbdde13003acf45bb6afe6db4ab599ae4/core/src/sigverify_shreds.rs#L20
pub const DEDUPER_FALSE_POSITIVE_RATE: f64 = 0.001;
pub const DEDUPER_NUM_BITS: u64 = 637_534_199; // 76MB
pub const DEDUPER_RESET_CYCLE: Duration = Duration::from_secs(5 * 60);
pub const RECONSTRUCT_QUEUE_CAPACITY: usize = 40_960;

fn dedup_key_slice(data: &[u8]) -> &[u8] {
    if data.len() <= solana_ledger::shred::SIZE_OF_SIGNATURE {
        return data;
    }

    let (expected_len, strip_retransmitter_signature) =
        match data[solana_ledger::shred::SIZE_OF_SIGNATURE] & 0xF0 {
            // MerkleCode: 0x40 (unchained), 0x60 (chained), 0x70 (chained resigned)
            0x40 | 0x60 => (<MerkleCodeShred as ShredTrait>::SIZE_OF_PAYLOAD, false),
            0x70 => (<MerkleCodeShred as ShredTrait>::SIZE_OF_PAYLOAD, true),
            // MerkleData: 0x80 (unchained), 0x90 (chained), 0xB0 (chained resigned)
            0x80 | 0x90 => (<MerkleDataShred as ShredTrait>::SIZE_OF_PAYLOAD, false),
            0xB0 => (<MerkleDataShred as ShredTrait>::SIZE_OF_PAYLOAD, true),
            _ => return data, // not a merkle shred (or unknown) -> hash full packet bytes
        };
    if data.len() < expected_len {
        return data;
    }

    // For resigned Merkle shreds, strip retransmitter signature bytes from the dedup key.
    // Different retransmitters can sign the same leader shred payload, and we want those
    // packets to dedup to avoid reconstruction queue pressure.
    let dedup_len = if strip_retransmitter_signature {
        expected_len.saturating_sub(solana_ledger::shred::SIZE_OF_SIGNATURE)
    } else {
        expected_len
    };
    if data.len() < dedup_len {
        return data;
    }
    &data[..dedup_len]
}

/// Bind to ports and start forwarding shreds
#[allow(clippy::too_many_arguments)]
pub fn start_forwarder_threads(
    unioned_dest_sockets: Arc<ArcSwap<Vec<SocketAddr>>>, /* sockets shared between endpoint discovery thread and forwarders */
    src_addr: IpAddr,
    src_port: u16,
    maybe_multicast_socket: Option<Vec<UdpSocket>>,
    num_threads: Option<usize>,
    deduper: Arc<RwLock<Deduper<2, [u8]>>>,
    should_reconstruct_shreds: bool,
    reconstruct_slot_lookback: Slot,
    reconstruct_slot_future: Slot,
    reconstruct_unknown_start_max_positions: u16,
    entry_sender: Arc<Sender<PbEntry>>,
    debug_trace_shred: bool,
    use_discovery_service: bool,
    forward_stats: Arc<StreamerReceiveStats>,
    metrics: Arc<ShredMetrics>,
    shutdown_receiver: Receiver<()>,
    exit: Arc<AtomicBool>,
) -> (u16, Vec<JoinHandle<()>>) {
    let num_threads = num_threads
        .unwrap_or_else(|| usize::from(std::thread::available_parallelism().unwrap()).min(4));

    let recycler: PacketBatchRecycler = Recycler::warmed(100, 1024);

    // multi_bind_in_range returns (port, Vec<UdpSocket>)
    let (bound_port, sockets) = solana_net_utils::multi_bind_in_range_with_config(
        src_addr,
        (src_port, src_port + 1),
        SocketConfig::default().reuseport(true),
        num_threads,
    )
    .unwrap_or_else(|_| {
        panic!("Failed to bind listener sockets. Check that port {src_port} is not in use.")
    });

    let (reconstruct_tx, reconstruct_rx) =
        crossbeam_channel::bounded::<PacketBatch>(RECONSTRUCT_QUEUE_CAPACITY);
    let mut thread_hdls = Vec::with_capacity(num_threads * 2 + 2);

    if should_reconstruct_shreds {
        let reconstruct_cfg = deshred::ReconstructShredsConfig {
            slot_lookback: reconstruct_slot_lookback,
            slot_future: reconstruct_slot_future,
            unknown_start_max_positions: reconstruct_unknown_start_max_positions,
        };
        let metrics = metrics.clone();
        let exit = exit.clone();
        // receives shreds from recv_from_channel_and_send_multiple_dest and calls deshred::reconstruct_shreds
        let hdl = std::thread::Builder::new()
            .name("shred_reconstructor".to_string())
            .spawn(move || {
                let mut all_shreds = ahash::HashMap::default();
                let mut slot_fec_keys_to_iterate = Vec::<(Slot, deshred::FecSetKey)>::new();
                let mut deshredded_entries =
                    Vec::<(Slot, Vec<solana_entry::entry::Entry>, Vec<u8>)>::new();
                let mut highest_slot_seen: Slot = 0; // Monotonic high-water mark for eviction
                let rs_cache = ReedSolomonCache::default();
                let mut reconstruct_scratch = deshred::ReconstructScratch::default();

                let mut packet_batches = Vec::new();
                while !exit.load(Ordering::Relaxed) {
                    // Block until at least one batch arrives, then drain all queued
                    // batches so Phase 1 ingests the maximum number of shreds before
                    // running FEC recovery and deshredding.
                    match reconstruct_rx.recv_timeout(Duration::from_millis(100)) {
                        Ok(first_batch) => {
                            packet_batches.clear();
                            packet_batches.push(first_batch);
                            while let Ok(batch) = reconstruct_rx.try_recv() {
                                packet_batches.push(batch);
                            }
                            let batches = std::mem::take(&mut packet_batches);
                            deshred::reconstruct_shreds(
                                batches,
                                &mut all_shreds,
                                &mut slot_fec_keys_to_iterate,
                                &mut deshredded_entries,
                                &mut highest_slot_seen,
                                &rs_cache,
                                reconstruct_cfg,
                                &metrics,
                                &mut reconstruct_scratch,
                            );

                            deshredded_entries.drain(..).for_each(
                                |(slot, _entries, entries_bytes)| {
                                    let _ = entry_sender.send(PbEntry {
                                        slot,
                                        entries: entries_bytes,
                                    });
                                },
                            );
                        }
                        Err(crossbeam_channel::RecvTimeoutError::Timeout) => {} // do nothing
                        Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
                    }
                }
            })
            .unwrap();
        thread_hdls.push(hdl);
    };

    let io_thread_hdls = sockets
        .into_iter()
        .chain(maybe_multicast_socket.into_iter().flatten())
        .enumerate()
        .flat_map(|(thread_id, incoming_shred_socket)| {
            let (packet_sender, packet_receiver) = crossbeam_channel::unbounded();
            let listen_thread = streamer::receiver(
                format!("ssListen{thread_id}"),
                Arc::new(incoming_shred_socket),
                exit.clone(),
                packet_sender,
                recycler.clone(),
                forward_stats.clone(),
                Duration::default(),
                false,
                None,
                false,
            );

            let deduper = deduper.clone();
            let unioned_dest_sockets = unioned_dest_sockets.clone();
            let metrics = metrics.clone();
            let shutdown_receiver = shutdown_receiver.clone();
            let reconstruct_tx = reconstruct_tx.clone();
            let exit = exit.clone();

            let send_thread = Builder::new()
                .name(format!("ssPxyTx_{thread_id}"))
                .spawn(move || {
                    let send_socket =
                        UdpSocket::bind(SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0))
                            .expect("to bind to udp port for forwarding");
                    let mut local_dest_sockets = unioned_dest_sockets.load();

                    let refresh_subscribers_tick = if use_discovery_service {
                        crossbeam_channel::tick(Duration::from_secs(30))
                    } else {
                        crossbeam_channel::tick(Duration::MAX)
                    };

                    while !exit.load(Ordering::Relaxed) {
                        crossbeam_channel::select! {
                            // forward packets
                            recv(packet_receiver) -> maybe_packet_batch => {
                                let res = recv_from_channel_and_send_multiple_dest(
                                    maybe_packet_batch,
                                    &deduper,
                                    &send_socket,
                                    &local_dest_sockets,
                                    should_reconstruct_shreds,
                                    &reconstruct_tx,
                                    debug_trace_shred,
                                    &metrics,
                                );

                                // If the channel is closed or error, break out
                                if res.is_err() {
                                    break;
                                }
                            }

                            // refresh thread-local subscribers
                            recv(refresh_subscribers_tick) -> _ => {
                                local_dest_sockets = unioned_dest_sockets.load();
                            }

                            // handle shutdown (avoid using sleep since it can hang)
                            recv(shutdown_receiver) -> _ => {
                                break;
                            }
                        }
                    }
                    info!("Exiting forwarder thread {thread_id}.");
                })
                .unwrap();

            vec![listen_thread, send_thread]
        })
        .collect::<Vec<JoinHandle<()>>>();

    thread_hdls.extend(io_thread_hdls);

    (bound_port, thread_hdls)
}

/// Broadcasts the same packet to multiple recipients, parses it into a Shred if possible,
/// and stores that shred in `all_shreds`.
#[allow(clippy::too_many_arguments)]
fn recv_from_channel_and_send_multiple_dest(
    maybe_packet_batch: Result<PacketBatch, RecvError>,
    deduper: &RwLock<Deduper<2, [u8]>>,
    send_socket: &UdpSocket,
    local_dest_sockets: &[SocketAddr],
    should_reconstruct_shreds: bool,
    reconstruct_tx: &crossbeam_channel::Sender<PacketBatch>,
    debug_trace_shred: bool,
    metrics: &ShredMetrics,
) -> Result<(), ShredstreamProxyError> {
    let mut packet_batch = maybe_packet_batch.map_err(ShredstreamProxyError::RecvError)?;
    let trace_shred_received_time = SystemTime::now();
    metrics
        .received
        .fetch_add(packet_batch.len() as u64, Ordering::Relaxed);
    debug!(
        "Got batch of {} packets, total size in bytes: {}",
        packet_batch.len(),
        packet_batch.iter().map(|x| x.meta().size).sum::<usize>()
    );

    // Deduplicate (Bloom filter) and mark duplicates as discarded, hashing only the shred payload
    // bytes (ignoring any packet trailing bytes).
    let mut num_deduped = 0u64;
    let deduper = deduper.read().unwrap();
    for packet in packet_batch.iter_mut() {
        if packet.meta().discard() {
            continue;
        }
        let Some(data) = packet.data(..) else {
            packet.meta_mut().set_discard(true);
            continue;
        };

        if deduper.dedup(dedup_key_slice(data)) {
            packet.meta_mut().set_discard(true);
            num_deduped += 1;
        }
    }
    metrics.duplicate.fetch_add(num_deduped, Ordering::Relaxed);

    // Store stats for each Packet
    packet_batch.iter().for_each(|packet| {
        metrics
            .packets_received
            .entry(packet.meta().addr)
            .and_modify(|(discarded, not_discarded)| {
                *discarded += packet.meta().discard() as u64;
                *not_discarded += (!packet.meta().discard()) as u64;
            })
            .or_insert_with(|| {
                (
                    packet.meta().discard() as u64,
                    (!packet.meta().discard()) as u64,
                )
            });
    });

    // send out to RPCs
    for outgoing_socketaddr in local_dest_sockets {
        let packets_with_dest = packet_batch
            .iter()
            .filter(|pkt| !pkt.meta().discard())
            .filter_map(|pkt| Some((pkt.data(..)?, outgoing_socketaddr)))
            .collect::<Vec<(&[u8], &SocketAddr)>>();

        if packets_with_dest.is_empty() {
            continue;
        }

        match batch_send(send_socket, &packets_with_dest) {
            Ok(_) => {
                metrics
                    .success_forward
                    .fetch_add(packets_with_dest.len() as u64, Ordering::Relaxed);
            }
            Err(SendPktsError::IoError(err, num_failed)) => {
                metrics.success_forward.fetch_add(
                    packets_with_dest
                        .len()
                        .saturating_sub(num_failed)
                        .try_into()
                        .unwrap_or_default(),
                    Ordering::Relaxed,
                );
                metrics
                    .fail_forward
                    .fetch_add(num_failed as u64, Ordering::Relaxed);
                error!(
                    "Failed to send batch of size {} to {outgoing_socketaddr:?}. \
                     {num_failed} packets failed. Error: {err}",
                    packets_with_dest.len()
                );
            }
        }
    }

    // Count TraceShred shreds
    if debug_trace_shred {
        packet_batch
            .iter()
            .filter(|p| !p.meta().discard())
            .filter_map(|p| TraceShred::decode(p.data(..)?).ok())
            .for_each(|trace_shred| {
                let TraceShred {
                    region,
                    created_at,
                    seq_num,
                } = trace_shred;

                // Don't drop datapoint if timestamp is invalid or delta is negative (clock skew)
                let elapsed = created_at
                    .and_then(|ts| SystemTime::try_from(ts).ok())
                    .map(|created_at| {
                        trace_shred_received_time
                            .duration_since(created_at)
                            .unwrap_or_default()
                    })
                    .unwrap_or_default();

                datapoint_info!(
                    "shredstream_proxy-trace_shred_latency",
                    "trace_region" => region,
                    ("trace_seq_num", seq_num, i64),
                    ("elapsed_micros", elapsed.as_micros(), i64),
                );
            });
    }

    // Last: enqueue for reconstruction (avoid cloning large batches).
    if should_reconstruct_shreds {
        // Skip enqueue when dedup/ingress filtering discarded every packet in this batch.
        // This avoids burning queue slots on batches that cannot contribute to reconstruction.
        let non_discard_count = packet_batch.iter().filter(|p| !p.meta().discard()).count();
        if non_discard_count == 0 {
            return Ok(());
        }
        match reconstruct_tx.try_send(packet_batch) {
            Ok(()) => {
                let qlen = reconstruct_tx.len() as u64;
                metrics
                    .reconstruct_queue_len_last
                    .store(qlen, Ordering::Relaxed);
                metrics
                    .reconstruct_queue_len_high_watermark
                    .fetch_max(qlen, Ordering::Relaxed);
            }
            Err(crossbeam_channel::TrySendError::Full(_packet_batch)) => {
                metrics
                    .reconstruct_packet_drop_count
                    .fetch_add(non_discard_count as u64, Ordering::Relaxed);
                metrics
                    .reconstruct_batch_drop_full_count
                    .fetch_add(1, Ordering::Relaxed);
            }
            Err(crossbeam_channel::TrySendError::Disconnected(_packet_batch)) => {}
        }
    }

    Ok(())
}

/// Starts a thread that updates our destinations used by the forwarder threads
pub fn start_destination_refresh_thread(
    endpoint_discovery_url: String,
    discovered_endpoints_port: u16,
    static_dest_sockets: Vec<(SocketAddr, String)>,
    unioned_dest_sockets: Arc<ArcSwap<Vec<SocketAddr>>>,
    shutdown_receiver: Receiver<()>,
    exit: Arc<AtomicBool>,
) -> JoinHandle<()> {
    Builder::new().name("ssPxyDstRefresh".to_string()).spawn(move || {
        let fetch_socket_tick = crossbeam_channel::tick(Duration::from_secs(30));
        let metrics_tick = crossbeam_channel::tick(Duration::from_secs(30));
        let mut socket_count = static_dest_sockets.len();
        while !exit.load(Ordering::Relaxed) {
            crossbeam_channel::select! {
                    recv(fetch_socket_tick) -> _ => {
                        let fetched = fetch_unioned_destinations(
                            &endpoint_discovery_url,
                            discovered_endpoints_port,
                            &static_dest_sockets,
                        );
                        let new_sockets = match fetched {
                            Ok(s) => {
                                info!("Sending shreds to {} destinations: {s:?}", s.len());
                                s
                            }
                            Err(e) => {
                                warn!("Failed to fetch from discovery service, retrying. Error: {e}");
                                datapoint_warn!("shredstream_proxy-destination_refresh_error",
                                                ("prev_unioned_dest_count", socket_count, i64),
                                                ("errors", 1, i64),
                                                ("error_str", e.to_string(), String),
                                );
                                continue;
                            }
                        };
                        socket_count = new_sockets.len();
                        unioned_dest_sockets.store(Arc::new(new_sockets));
                    }
                    recv(metrics_tick) -> _ => {
                        datapoint_info!("shredstream_proxy-destination_refresh_stats",
                                        ("destination_count", socket_count, i64),
                        );
                    }
                    recv(shutdown_receiver) -> _ => {
                        break;
                    }
                }
        }
    }).unwrap()
}

/// Returns dynamically discovered endpoints with CLI arg defined endpoints
fn fetch_unioned_destinations(
    endpoint_discovery_url: &str,
    discovered_endpoints_port: u16,
    static_dest_sockets: &[(SocketAddr, String)],
) -> Result<Vec<SocketAddr>, ShredstreamProxyError> {
    let bytes = reqwest::blocking::get(endpoint_discovery_url)?.bytes()?;

    let sockets_json = match serde_json::from_slice::<Vec<IpAddr>>(&bytes) {
        Ok(s) => s,
        Err(e) => {
            warn!(
                "Failed to parse json from: {:?}",
                std::str::from_utf8(&bytes)
            );
            return Err(ShredstreamProxyError::from(e));
        }
    };

    // resolve again since ip address could change
    let static_dest_sockets = static_dest_sockets
        .iter()
        .filter_map(|(_socketaddr, hostname_port)| {
            Some(resolve_hostname_port(hostname_port).ok()?.0)
        })
        .collect::<Vec<_>>();

    let unioned_dest_sockets = sockets_json
        .into_iter()
        .map(|ip| SocketAddr::new(ip, discovered_endpoints_port))
        .chain(static_dest_sockets)
        .unique()
        .collect::<Vec<SocketAddr>>();
    Ok(unioned_dest_sockets)
}

/// Reset dedup + send metrics to influx
pub fn start_forwarder_accessory_thread(
    deduper: Arc<RwLock<Deduper<2, [u8]>>>,
    metrics: Arc<ShredMetrics>,
    metrics_update_interval_ms: u64,
    shutdown_receiver: Receiver<()>,
    exit: Arc<AtomicBool>,
) -> JoinHandle<()> {
    Builder::new()
        .name("ssPxyAccessory".to_string())
        .spawn(move || {
            let metrics_tick =
                crossbeam_channel::tick(Duration::from_millis(metrics_update_interval_ms));
            let deduper_reset_tick = crossbeam_channel::tick(Duration::from_secs(2));
            let mut rng = rand::thread_rng();
            while !exit.load(Ordering::Relaxed) {
                crossbeam_channel::select! {
                    // reset deduper to avoid false positives
                    recv(deduper_reset_tick) -> _ => {
                        deduper
                            .write()
                            .unwrap()
                            .maybe_reset(&mut rng, DEDUPER_FALSE_POSITIVE_RATE, DEDUPER_RESET_CYCLE);
                    }

                    // send metrics to influx
                    recv(metrics_tick) -> _ => {
                        metrics.report();
                        metrics.reset();
                    }

                    // handle SIGINT shutdown
                    recv(shutdown_receiver) -> _ => {
                        break;
                    }
                }
            }
        })
        .unwrap()
}

pub struct ShredMetrics {
    // receive stats
    /// Total number of shreds received. Includes duplicates when receiving shreds from multiple regions
    pub received: AtomicU64,
    /// Total number of shreds successfully forwarded, accounting for all destinations
    pub success_forward: AtomicU64,
    /// Total number of shreds failed to forward, accounting for all destinations
    pub fail_forward: AtomicU64,
    /// Number of duplicate shreds received
    pub duplicate: AtomicU64,
    /// (discarded, not discarded, from other shredstream instances)
    pub packets_received: DashMap<IpAddr, (u64, u64)>,

    // service metrics
    pub enabled_grpc_service: bool,
    /// Number of shreds dropped because the reconstruction queue was full
    pub reconstruct_packet_drop_count: AtomicU64,
    /// Number of reconstruction batches dropped because the reconstruction queue was full
    pub reconstruct_batch_drop_full_count: AtomicU64,
    /// Shreds dropped by reconstruction ingress filtering (`should_discard_shred` checks)
    pub reconstruct_ingress_filter_drop_count: AtomicU64,
    /// Shreds dropped because they fell outside the reconstruction slot window (too old)
    pub reconstruct_slot_window_drop_old_count: AtomicU64,
    /// Shreds dropped because they fell outside the reconstruction slot window (too far in the future)
    pub reconstruct_slot_window_drop_future_count: AtomicU64,
    /// Last observed reconstruction queue length (messages)
    pub reconstruct_queue_len_last: AtomicU64,
    /// High-water mark reconstruction queue length for the reporting interval (messages)
    pub reconstruct_queue_len_high_watermark: AtomicU64,
    /// Number of data shreds recovered using coding shreds
    pub recovered_count: AtomicU64,
    /// Number of deshred errors (Shredder::deshred failures), all start modes combined.
    pub deshred_error_count: AtomicU64,
    /// Number of deshred errors with a known start boundary.
    pub deshred_error_known_start_count: AtomicU64,
    /// Number of deshred errors with an unknown (gap-inferred) start boundary.
    pub deshred_error_unknown_start_count: AtomicU64,
    /// Number of successfully deshredded entry sets emitted (bytes emitted)
    pub deshred_set_count: AtomicU64,
    /// Total number of bytes emitted as deshredded entry sets
    pub deshred_bytes_count: AtomicU64,
    /// Number of Solana entries decoded from shreds
    pub entry_count: AtomicU64,
    /// Number of transactions decoded from shreds
    pub txn_count: AtomicU64,
    /// Number of unknown-start candidate positions attempted.
    /// This is per candidate attempt, not per FEC identity.
    pub unknown_start_position_count: AtomicU64,
    /// Number of FEC recovery errors
    pub fec_recovery_error_count: AtomicU64,
    /// Number of FEC recovery errors due to merkle root/signature mismatch
    pub fec_recovery_invalid_merkle_root_count: AtomicU64,
    /// Number of bincode Entry deserialization errors
    pub bincode_deserialize_error_count: AtomicU64,
    /// Number of decoded entry sets rejected by sanity checks
    pub entry_sanity_error_count: AtomicU64,
    /// Number of unknown-start candidate attempts that failed (deshred, bincode, or sanity reject).
    pub unknown_start_position_error_count: AtomicU64,
    /// Sum of (first-shred -> unknown-start completion) latency across completed FEC sets.
    pub fec_set_decode_unknown_start_latency_us_sum: AtomicU64,
    /// Number of FEC identities that reached unknown-start completion latency observation.
    /// A single FEC identity may later also contribute to known-start completion.
    pub fec_set_decode_unknown_start_latency_count: AtomicU64,
    /// Sum of (first-shred -> known-start completion) latency across completed FEC sets.
    pub fec_set_decode_known_start_latency_us_sum: AtomicU64,
    /// Number of FEC identities that reached known-start completion latency observation.
    pub fec_set_decode_known_start_latency_count: AtomicU64,
    /// Number of finalized FEC identities that reached unknown-start completion
    /// but never reached known-start completion.
    pub fec_set_decode_unknown_start_only_count: AtomicU64,
    /// Number of finalized FEC identities that reached known-start completion
    /// but never reached unknown-start completion.
    pub fec_set_decode_known_start_only_count: AtomicU64,
    /// Number of finalized FEC identities that reached both unknown-start and
    /// known-start completion at different times.
    pub fec_set_decode_both_start_modes_count: AtomicU64,

    // cumulative metrics (persist after reset)
    pub agg_received_cumulative: AtomicU64,
    pub agg_success_forward_cumulative: AtomicU64,
    pub agg_fail_forward_cumulative: AtomicU64,
    pub duplicate_cumulative: AtomicU64,
}

impl Default for ShredMetrics {
    fn default() -> Self {
        Self::new(false)
    }
}

impl ShredMetrics {
    pub fn new(enabled_grpc_service: bool) -> Self {
        Self {
            enabled_grpc_service,
            received: Default::default(),
            success_forward: Default::default(),
            fail_forward: Default::default(),
            duplicate: Default::default(),
            packets_received: DashMap::with_capacity(10),
            reconstruct_packet_drop_count: Default::default(),
            reconstruct_batch_drop_full_count: Default::default(),
            reconstruct_ingress_filter_drop_count: Default::default(),
            reconstruct_slot_window_drop_old_count: Default::default(),
            reconstruct_slot_window_drop_future_count: Default::default(),
            reconstruct_queue_len_last: Default::default(),
            reconstruct_queue_len_high_watermark: Default::default(),
            recovered_count: Default::default(),
            deshred_error_count: Default::default(),
            deshred_error_known_start_count: Default::default(),
            deshred_error_unknown_start_count: Default::default(),
            deshred_set_count: Default::default(),
            deshred_bytes_count: Default::default(),
            entry_count: Default::default(),
            txn_count: Default::default(),
            unknown_start_position_count: Default::default(),
            fec_recovery_error_count: Default::default(),
            fec_recovery_invalid_merkle_root_count: Default::default(),
            bincode_deserialize_error_count: Default::default(),
            entry_sanity_error_count: Default::default(),
            unknown_start_position_error_count: Default::default(),
            fec_set_decode_unknown_start_latency_us_sum: Default::default(),
            fec_set_decode_unknown_start_latency_count: Default::default(),
            fec_set_decode_known_start_latency_us_sum: Default::default(),
            fec_set_decode_known_start_latency_count: Default::default(),
            fec_set_decode_unknown_start_only_count: Default::default(),
            fec_set_decode_known_start_only_count: Default::default(),
            fec_set_decode_both_start_modes_count: Default::default(),
            agg_received_cumulative: Default::default(),
            agg_success_forward_cumulative: Default::default(),
            agg_fail_forward_cumulative: Default::default(),
            duplicate_cumulative: Default::default(),
        }
    }

    pub fn report(&self) {
        datapoint_info!(
            "shredstream_proxy-connection_metrics",
            ("received", self.received.load(Ordering::Relaxed), i64),
            (
                "success_forward",
                self.success_forward.load(Ordering::Relaxed),
                i64
            ),
            (
                "fail_forward",
                self.fail_forward.load(Ordering::Relaxed),
                i64
            ),
            ("duplicate", self.duplicate.load(Ordering::Relaxed), i64),
        );

        if self.enabled_grpc_service {
            datapoint_info!(
                "shredstream_proxy-service_metrics",
                (
                    "reconstruct_packet_drop_count",
                    self.reconstruct_packet_drop_count
                        .swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "reconstruct_batch_drop_full_count",
                    self.reconstruct_batch_drop_full_count
                        .swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "reconstruct_ingress_filter_drop_count",
                    self.reconstruct_ingress_filter_drop_count
                        .swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "reconstruct_slot_window_drop_old_count",
                    self.reconstruct_slot_window_drop_old_count
                        .swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "reconstruct_slot_window_drop_future_count",
                    self.reconstruct_slot_window_drop_future_count
                        .swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "reconstruct_queue_len_last",
                    self.reconstruct_queue_len_last.load(Ordering::Relaxed),
                    i64
                ),
                (
                    "reconstruct_queue_len_high_watermark",
                    self.reconstruct_queue_len_high_watermark
                        .swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "recovered_count",
                    self.recovered_count.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "deshred_error_count",
                    self.deshred_error_count.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "deshred_error_known_start_count",
                    self.deshred_error_known_start_count
                        .swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "deshred_error_unknown_start_count",
                    self.deshred_error_unknown_start_count
                        .swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "deshred_set_count",
                    self.deshred_set_count.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "deshred_bytes_count",
                    self.deshred_bytes_count.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "entry_count",
                    self.entry_count.swap(0, Ordering::Relaxed),
                    i64
                ),
                ("txn_count", self.txn_count.swap(0, Ordering::Relaxed), i64),
                (
                    "unknown_start_position_count",
                    self.unknown_start_position_count.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "fec_recovery_error_count",
                    self.fec_recovery_error_count.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "fec_recovery_invalid_merkle_root_count",
                    self.fec_recovery_invalid_merkle_root_count
                        .swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "bincode_deserialize_error_count",
                    self.bincode_deserialize_error_count
                        .swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "entry_sanity_error_count",
                    self.entry_sanity_error_count.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "unknown_start_position_error_count",
                    self.unknown_start_position_error_count
                        .swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "fec_set_decode_unknown_start_latency_us_sum",
                    self.fec_set_decode_unknown_start_latency_us_sum
                        .swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "fec_set_decode_unknown_start_latency_count",
                    self.fec_set_decode_unknown_start_latency_count
                        .swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "fec_set_decode_known_start_latency_us_sum",
                    self.fec_set_decode_known_start_latency_us_sum
                        .swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "fec_set_decode_known_start_latency_count",
                    self.fec_set_decode_known_start_latency_count
                        .swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "fec_set_decode_unknown_start_only_count",
                    self.fec_set_decode_unknown_start_only_count
                        .swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "fec_set_decode_known_start_only_count",
                    self.fec_set_decode_known_start_only_count
                        .swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "fec_set_decode_both_start_modes_count",
                    self.fec_set_decode_both_start_modes_count
                        .swap(0, Ordering::Relaxed),
                    i64
                ),
            );
        }

        self.packets_received
            .retain(|addr, (discarded_packets, not_discarded_packets)| {
                datapoint_info!("shredstream_proxy-receiver_stats",
                    "addr" => addr.to_string(),
                    ("discarded_packets", *discarded_packets, i64),
                    ("not_discarded_packets", *not_discarded_packets, i64),
                );
                false
            });
    }

    /// resets current values, increments cumulative values
    pub fn reset(&self) {
        self.agg_received_cumulative
            .fetch_add(self.received.swap(0, Ordering::Relaxed), Ordering::Relaxed);
        self.agg_success_forward_cumulative.fetch_add(
            self.success_forward.swap(0, Ordering::Relaxed),
            Ordering::Relaxed,
        );
        self.agg_fail_forward_cumulative.fetch_add(
            self.fail_forward.swap(0, Ordering::Relaxed),
            Ordering::Relaxed,
        );
        self.duplicate_cumulative
            .fetch_add(self.duplicate.swap(0, Ordering::Relaxed), Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use std::{
        net::{IpAddr, Ipv4Addr, UdpSocket},
        sync::{Arc, Mutex, RwLock},
        thread,
        thread::sleep,
        time::Duration,
    };

    use solana_perf::{
        deduper::Deduper,
        packet::{Meta, Packet, PacketBatch},
    };
    use solana_sdk::packet::{PacketFlags, PACKET_DATA_SIZE};

    use crate::forwarder::{recv_from_channel_and_send_multiple_dest, ShredMetrics};

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
                },
            ),
            Packet::new(
                [2; PACKET_DATA_SIZE],
                Meta {
                    size: PACKET_DATA_SIZE,
                    addr: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
                    port: 9999,
                    flags: PacketFlags::empty(),
                },
            ),
        ]);
        let (packet_sender, packet_receiver) = crossbeam_channel::unbounded::<PacketBatch>();
        packet_sender.send(packet_batch).unwrap();

        // This test uses UDP sockets. In some sandboxed environments, creating sockets is not
        // permitted; skip the test in that case.
        let mut test_listeners = Vec::with_capacity(3);
        for _ in 0..3 {
            let listen_socket = match UdpSocket::bind("127.0.0.1:0") {
                Ok(s) => s,
                Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => return,
                Err(e) => panic!("Failed to bind UDP listener socket: {e}"),
            };
            let socketaddr = listen_socket.local_addr().unwrap();
            test_listeners.push((
                listen_socket,
                socketaddr,
                // store results in vec of packet, where packet is Vec<u8>
                Arc::new(Mutex::new(vec![])),
            ));
        }
        let dest_socketaddrs = test_listeners
            .iter()
            .map(|(_listen_socket, socketaddr, _)| *socketaddr)
            .collect::<Vec<_>>();

        let udp_sender = match UdpSocket::bind("127.0.0.1:0") {
            Ok(s) => s,
            Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => return,
            Err(e) => panic!("Failed to bind UDP sender socket: {e}"),
        };

        // spawn listeners
        test_listeners
            .iter()
            .for_each(|(listen_socket, _socketaddr, to_receive)| {
                let socket = listen_socket.try_clone().unwrap();
                let to_receive = to_receive.to_owned();
                thread::spawn(move || listen_and_collect(socket, to_receive));
            });

        let (reconstruct_tx, _reconstruct_rx) =
            crossbeam_channel::bounded::<PacketBatch>(crate::forwarder::RECONSTRUCT_QUEUE_CAPACITY);
        // send packets
        recv_from_channel_and_send_multiple_dest(
            packet_receiver.recv(),
            &Arc::new(RwLock::new(Deduper::<2, [u8]>::new(
                &mut rand::thread_rng(),
                crate::forwarder::DEDUPER_NUM_BITS,
            ))),
            &udp_sender,
            &Arc::new(dest_socketaddrs),
            true,
            &reconstruct_tx,
            false,
            &Arc::new(ShredMetrics::default()),
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

    #[test]
    fn test_dedup_ignores_trailing_packet_bytes() {
        // This test uses UDP sockets. In some sandboxed environments, creating sockets is not
        // permitted; skip the test in that case.
        let udp_sender = match UdpSocket::bind("127.0.0.1:0") {
            Ok(s) => s,
            Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => return,
            Err(e) => panic!("Failed to bind UDP sender socket: {e}"),
        };

        let payload_len = <solana_ledger::shred::merkle::ShredData as solana_ledger::shred::traits::Shred>::SIZE_OF_PAYLOAD;
        const TRAILING_LEN: usize = 8;
        assert!(payload_len + TRAILING_LEN <= PACKET_DATA_SIZE);

        let mut pkt1 = [0u8; PACKET_DATA_SIZE];
        let mut pkt2 = [0u8; PACKET_DATA_SIZE];

        // Same shred payload bytes...
        pkt1[..payload_len].fill(0xAA);
        pkt2[..payload_len].fill(0xAA);
        // Ensure it looks like a MerkleData shred at the variant byte offset.
        pkt1[solana_ledger::shred::SIZE_OF_SIGNATURE] = 0x96; // tag=0x90 (data), proof_size=6
        pkt2[solana_ledger::shred::SIZE_OF_SIGNATURE] = 0x96;
        // ...but different trailing packet bytes beyond the payload slice.
        pkt1[payload_len..payload_len + TRAILING_LEN].fill(0x01);
        pkt2[payload_len..payload_len + TRAILING_LEN].fill(0x02);

        let packet_batch = PacketBatch::new(vec![
            Packet::new(
                pkt1,
                Meta {
                    size: payload_len + TRAILING_LEN,
                    addr: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
                    port: 1111,
                    flags: PacketFlags::empty(),
                },
            ),
            Packet::new(
                pkt2,
                Meta {
                    size: payload_len + TRAILING_LEN,
                    addr: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
                    port: 2222,
                    flags: PacketFlags::empty(),
                },
            ),
        ]);

        let (reconstruct_tx, reconstruct_rx) = crossbeam_channel::bounded::<PacketBatch>(10);
        let metrics = Arc::new(ShredMetrics::default());

        recv_from_channel_and_send_multiple_dest(
            Ok(packet_batch),
            &Arc::new(RwLock::new(Deduper::<2, [u8]>::new(
                &mut rand::thread_rng(),
                crate::forwarder::DEDUPER_NUM_BITS,
            ))),
            &udp_sender,
            &[],
            true,
            &reconstruct_tx,
            false,
            &metrics,
        )
        .unwrap();

        let reconstructed = reconstruct_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("expected packet batch to be enqueued for reconstruction");
        let discarded = reconstructed.iter().filter(|p| p.meta().discard()).count();
        assert_eq!(discarded, 1, "expected one packet to be dedup-discarded");
        assert_eq!(
            metrics.duplicate.load(std::sync::atomic::Ordering::Relaxed),
            1
        );
    }

    #[test]
    fn test_dedup_ignores_resigned_retransmitter_signature() {
        // This test uses UDP sockets. In some sandboxed environments, creating sockets is not
        // permitted; skip the test in that case.
        let udp_sender = match UdpSocket::bind("127.0.0.1:0") {
            Ok(s) => s,
            Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => return,
            Err(e) => panic!("Failed to bind UDP sender socket: {e}"),
        };

        let payload_len = <solana_ledger::shred::merkle::ShredData as solana_ledger::shred::traits::Shred>::SIZE_OF_PAYLOAD;
        assert!(payload_len > solana_ledger::shred::SIZE_OF_SIGNATURE);

        let mut pkt1 = [0u8; PACKET_DATA_SIZE];
        let mut pkt2 = [0u8; PACKET_DATA_SIZE];

        // Same resigned MerkleData payload bytes...
        pkt1[..payload_len].fill(0xAA);
        pkt2[..payload_len].fill(0xAA);
        // Variant byte: tag=0xB0 (MerkleData resigned), proof_size=6.
        pkt1[solana_ledger::shred::SIZE_OF_SIGNATURE] = 0xB6;
        pkt2[solana_ledger::shred::SIZE_OF_SIGNATURE] = 0xB6;

        // ...except retransmitter signature bytes at the tail of payload differ.
        let retransmitter_sig_start = payload_len - solana_ledger::shred::SIZE_OF_SIGNATURE;
        pkt1[retransmitter_sig_start..payload_len].fill(0x11);
        pkt2[retransmitter_sig_start..payload_len].fill(0x22);

        let packet_batch = PacketBatch::new(vec![
            Packet::new(
                pkt1,
                Meta {
                    size: payload_len,
                    addr: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
                    port: 1111,
                    flags: PacketFlags::empty(),
                },
            ),
            Packet::new(
                pkt2,
                Meta {
                    size: payload_len,
                    addr: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
                    port: 2222,
                    flags: PacketFlags::empty(),
                },
            ),
        ]);

        let (reconstruct_tx, reconstruct_rx) = crossbeam_channel::bounded::<PacketBatch>(10);
        let metrics = Arc::new(ShredMetrics::default());

        recv_from_channel_and_send_multiple_dest(
            Ok(packet_batch),
            &Arc::new(RwLock::new(Deduper::<2, [u8]>::new(
                &mut rand::thread_rng(),
                crate::forwarder::DEDUPER_NUM_BITS,
            ))),
            &udp_sender,
            &[],
            true,
            &reconstruct_tx,
            false,
            &metrics,
        )
        .unwrap();

        let reconstructed = reconstruct_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("expected packet batch to be enqueued for reconstruction");
        let discarded = reconstructed.iter().filter(|p| p.meta().discard()).count();
        assert_eq!(discarded, 1, "expected one packet to be dedup-discarded");
        assert_eq!(
            metrics.duplicate.load(std::sync::atomic::Ordering::Relaxed),
            1
        );
    }

    #[test]
    fn test_reconstruct_queue_skips_all_discarded_batches() {
        // This test uses UDP sockets. In some sandboxed environments, creating sockets is not
        // permitted; skip the test in that case.
        let udp_sender = match UdpSocket::bind("127.0.0.1:0") {
            Ok(s) => s,
            Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => return,
            Err(e) => panic!("Failed to bind UDP sender socket: {e}"),
        };

        let mut packet = Packet::new(
            [7; PACKET_DATA_SIZE],
            Meta {
                size: PACKET_DATA_SIZE,
                addr: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
                port: 3333,
                flags: PacketFlags::empty(),
            },
        );
        packet.meta_mut().set_discard(true);

        let (reconstruct_tx, reconstruct_rx) = crossbeam_channel::bounded::<PacketBatch>(10);
        let metrics = Arc::new(ShredMetrics::default());

        recv_from_channel_and_send_multiple_dest(
            Ok(PacketBatch::new(vec![packet])),
            &Arc::new(RwLock::new(Deduper::<2, [u8]>::new(
                &mut rand::thread_rng(),
                crate::forwarder::DEDUPER_NUM_BITS,
            ))),
            &udp_sender,
            &[],
            true,
            &reconstruct_tx,
            false,
            &metrics,
        )
        .unwrap();

        assert!(
            reconstruct_rx
                .recv_timeout(Duration::from_millis(200))
                .is_err(),
            "all-discarded batches should not be enqueued for reconstruction"
        );
    }
}
