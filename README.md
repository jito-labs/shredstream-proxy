# Jito Shredstream Proxy

ShredStream provides the lowest latency to shreds from leaders on Solana.

See more at https://docs.jito.wtf/lowlatencytxnfeed/

## Performance logging

The forwarder hot path (`recv_from_channel_and_send_multiple_dest` in
`proxy/src/forwarder.rs`) carries optional, per-batch latency + throughput
instrumentation that is **gated on the `trace` log level**. When trace is off,
no `Instant::now()` clock reads, no atomic accumulator updates, and no log
output happen on the data path.

### Run with performance logging OFF (default / production)

Use any log level at or below `debug`. None of the perf instrumentation runs.

```bash
# silent
cargo run --release --bin jito-shredstream-proxy -- forward-only \
  --src-bind-port 20000 \
  --dest-ip-ports 127.0.0.1:8001

# info-level (operational logs only, no perf overhead)
RUST_LOG=info cargo run --release --bin jito-shredstream-proxy -- forward-only \
  --src-bind-port 20000 \
  --dest-ip-ports 127.0.0.1:8001
```

### Run with performance logging ON

Enable trace for the proxy crate only — keeps third-party crates quiet.

```bash
RUST_LOG=info,jito_shredstream_proxy::forwarder=trace \
  cargo run --release --bin jito-shredstream-proxy -- forward-only \
    --src-bind-port 20000 \
    --dest-ip-ports 127.0.0.1:8001 \
    --metrics-report-interval-ms 10000
```

## Emitted metrics — full reference

All datapoints below are emitted via `solana_metrics::datapoint_info!`, which
prints to **stderr** at INFO level and (if `SOLANA_METRICS_CONFIG` is set)
also ships to InfluxDB. They're emitted on the
`--metrics-report-interval-ms` schedule (default 15000 ms).

### Pre-existing (always emitted; no log-level gating)

These existed before the fanout/latency work and are emitted regardless of
`RUST_LOG` setting.

#### `shredstream_proxy-connection_metrics`

End-to-end throughput counters for the forwarder, per reporting interval.

| Field | Meaning |
|---|---|
| `received` | Total packets received from upstream this interval |
| `success_forward` | Total (packet × destination) pairs successfully sent |
| `fail_forward` | Total (packet × destination) pairs that failed to send |
| `duplicate` | Number of packets marked duplicate by the bloom-filter dedup |

> **Note:** `success_forward` and `fail_forward` are multiplied by the
> destination count (one entry per `packet × dest`). Divide by `worker_count`
> (see below) to get per-destination success/fail.

#### `shredstream_proxy-receiver_stats` (one row per upstream source IP)

Per-source view of inbound packets and dedup behavior. Tags: `addr`.

| Field | Meaning |
|---|---|
| `discarded_packets` | Packets from this source that were marked discard (dedup'd) |
| `not_discarded_packets` | Packets from this source that passed dedup |

#### `shredstream_proxy-listen_thread`

Emitted by `solana_streamer::StreamerReceiveStats` from the listener side,
once per second.

| Field | Meaning |
|---|---|
| `packets_count` | Total packets received this second |
| `packet_batches_count` | Total batches received this second |
| `full_packet_batches_count` | Batches that hit the recvmmsg max-iov cap (1024) |
| `channel_len` | Current depth of the listener → coordinator channel |

#### `shredstream_proxy-service_metrics` (only with `--grpc-service-port`)

Decoded-entries pipeline for the gRPC service path.

| Field | Meaning |
|---|---|
| `recovered_count` | Data shreds recovered from coding shreds via Reed-Solomon |
| `entry_count` | Number of Solana entries decoded from shreds |
| `txn_count` | Number of transactions decoded |
| `unknown_start_position_count` | Times the prior `DATA_COMPLETE_SHRED` was missing |
| `fec_recovery_error_count` | Reed-Solomon recovery failures |
| `bincode_deserialize_error_count` | Entry deserialization failures |
| `unknown_start_position_error_count` | Deshred attempts that failed due to missing start position |

#### `shredstream_proxy-trace_shred_latency` (only with `--debug-trace-shred`)

End-to-end wall-clock from upstream (`TraceShred.created_at`) to local
recv. Tags: `trace_region`.

| Field | Meaning |
|---|---|
| `trace_seq_num` | Sequence number of the trace shred |
| `elapsed_micros` | Wall-clock from `created_at` to local receive (µs) |

#### `shredstream_proxy-destination_refresh_stats` (only with `--endpoint-discovery-url`)

Emitted every 30s by `ssPxyDstRefresh`.

| Field | Meaning |
|---|---|
| `destination_count` | Number of destinations currently in the union set |

---

### New (gated on `forwarder=trace`)

Added by the fanout/latency optimization work. **Gated on
`RUST_LOG=...jito_shredstream_proxy::forwarder=trace`** — when trace is not
enabled for the forwarder module, no `Instant::now()` calls and no atomic
accumulator updates run on the data path, and these datapoints are not
emitted.

#### `shredstream_proxy-forwarding_perf` — coordinator side

Recv → dedup → stats → dispatch pipeline running in `ssPxyTx_<i>` threads.

| Field | Meaning |
|---|---|
| `batches` | Coordinator batches handled this interval |
| `packets` | Total packets across all batches (use to derive packets/sec) |
| `dest_sends` | Σ destination count across batches (fanout multiplier × batches) |
| `worker_count` | Active per-destination worker threads at emission time |
| `worker_dropped_batches` | Batches dropped because a worker's channel was full — backpressure indicator |
| `avg_packets_per_batch` | `packets / batches` |
| `avg_dests_per_batch` | `dest_sends / batches` |
| `avg_total_us` | Avg time in coordinator per batch (recv → dispatch return) |
| `avg_dedup_us` | Avg time spent in the dedup bloom filter |
| `avg_stats_us` | Avg time updating per-source stats DashMap |
| `avg_reconstruct_clone_us` | Avg time cloning batch + try_send for the gRPC reconstruct path |
| `avg_fanout_dispatch_us` | Avg time dispatching Arc clones to all D worker channels (now sub-µs; UDP send happens in workers) |
| `max_total_us` | Slowest coordinator pass observed this interval |
| `max_fanout_dispatch_us` | Slowest dispatch pass observed this interval |

#### `shredstream_proxy-worker_perf` — per-destination UDP send workers

Running in `ssPxyDst_<ip>_<port>` threads. Each row is an aggregate across
all workers; per-destination breakdown is not currently tagged.

| Field | Meaning |
|---|---|
| `worker_batches` | Σ batches processed across all workers this interval |
| `worker_packets_sent` | Σ packets sent across all workers (≈ `success_forward`) |
| `avg_worker_send_us` | Avg `sendmmsg` wall-clock per worker per batch — **the per-destination UDP send cost** |
| `max_worker_send_us` | Slowest worker send observed — the tail-latency number |
| `avg_worker_packets_per_batch` | Packets per worker batch |

> **Why one knob (`forwarder=trace`) controls a datapoint emitted at INFO?**
> The trace level is used as a zero-overhead gate — when off, no
> `Instant::now()` clock reads and no atomic accumulator updates happen on the
> data path. The actual emission goes through `solana_metrics`, which always
> logs at INFO. There is no per-batch `trace!` line; the per-interval datapoints
> are the only output.

**Packets/sec** is intentionally not emitted on any datapoint — divide
`packets` (or `worker_packets_sent`) by `metrics_report_interval_ms / 1000`
downstream.

### Mapping metrics to the fanout-optimization wins

| Improvement claim | Metric to compare before/after |
|---|---|
| Last-destination latency no longer scales with D | `max_worker_send_us` (should stay near `avg_worker_send_us` regardless of `worker_count`) |
| Coordinator is no longer the bottleneck | `avg_total_us`, `max_total_us` (drops dramatically — no D-way send loop in coordinator) |
| Channel dispatch is essentially free | `avg_fanout_dispatch_us` (now sub-µs even at D=100) |
| Per-destination send cost in isolation | `avg_worker_send_us` (sendmmsg wall-clock for P packets to one dest) |
| Slow destination detection | `worker_dropped_batches > 0` (a worker fell behind and lost batches) |
| Throughput | `packets` / interval, `worker_packets_sent` / interval |

### Forwarder architecture (post-optimization)

The forwarder is split into two thread tiers:

1. **Coordinator threads** (one per listener socket, named `ssPxyTx_<i>`):
   receive `PacketBatch` from the listener, dedup, update per-source stats,
   wrap in `Arc<PacketBatch>`, and dispatch to per-destination workers via
   bounded crossbeam channels. No UDP I/O happens here.
2. **Per-destination worker threads** (one per active destination, named
   `ssPxyDst_<ip>_<port>`): own a `UdpSocket`, receive `Arc<PacketBatch>`,
   build a reused scratch `Vec<(&[u8], &SocketAddr)>`, and call `batch_send`
   (= `sendmmsg(2)` on Linux — one syscall for up to 1024 packets).

A **dest-manager thread** (`ssPxyDstMgr`) polls the destination list every 5s
and spawns/drops workers as destinations are added/removed.

End-to-end latency for the slowest destination no longer scales linearly
with the number of destinations — sends happen in parallel, so latency is
`max(T_per_dest)` rather than `sum(T_per_dest)`. Watch `avg_worker_send_us`
and `max_worker_send_us` to measure the per-destination UDP cost in
isolation.

### Extracting for the HTML dashboard

The aggregated `datapoint_info!` rows go to **stderr** via `env_logger`. Merge
stderr into stdout with `2>&1` so you can pipe/redirect. (If
`SOLANA_METRICS_CONFIG` is set, `datapoint_info!` is *also* shipped to
InfluxDB; the local stderr line still prints either way.)

**Option A — capture full log, grep afterwards.** Best when you want to re-grep
different fields without re-running.

```bash
RUST_LOG=info,jito_shredstream_proxy::forwarder=trace \
  cargo run --release --bin jito-shredstream-proxy -- forward-only ... \
  > shredstream.log 2>&1

# stop with Ctrl-C, then extract:
grep "shredstream_proxy-forwarding_perf" shredstream.log > perf_forwarding.txt
grep "shredstream_proxy-worker_perf"     shredstream.log > perf_workers.txt
```

**Option B — live filter + keep the raw log.** Best when watching a profiling
run for a fixed window.

```bash
RUST_LOG=info,jito_shredstream_proxy::forwarder=trace \
  cargo run --release --bin jito-shredstream-proxy -- forward-only ... \
  2>&1 | tee shredstream.log \
       | grep --line-buffered -E "shredstream_proxy-(forwarding|worker)_perf"
```

- `tee shredstream.log` keeps the full raw log on disk for later re-grep.
- `grep --line-buffered` flushes matches to your terminal immediately
  (without it, pipe buffering can delay output by seconds).

Feed `perf_forwarding.txt` and `perf_workers.txt` (or whatever you `tee` to)
into the `latency_dashboard.html` / `fanout_optimization.html` data tables.
