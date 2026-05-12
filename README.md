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

You get two streams of perf data:

1. **Per-batch `trace!` lines** (one per `PacketBatch` handled). Stable
   key=value format for easy `grep`/`awk` extraction:

   ```
   fwd_batch packets=128 dests=200 total_us=987 dedup_us=72 \
             fanout_send_us=860 max_per_dest_us=12 stats_us=18 \
             reconstruct_clone_us=11 deduped=4
   ```

2. **Aggregated `datapoint_info!("shredstream_proxy-forwarding_perf", …)`**
   emitted once per `--metrics-report-interval-ms` (default 15000). Fields:

   | Field | Meaning |
   |---|---|
   | `batches`, `packets`, `dest_sends` | Interval throughput counters |
   | `avg_packets_per_batch`, `avg_dests_per_batch` | Sizing |
   | `avg_total_us` | Avg time to forward one packet batch (entry → exit) |
   | `avg_dedup_us` | Avg dedup time per batch |
   | `avg_fanout_send_us` | Avg total fanout time (all destinations) per batch |
   | `avg_stats_us`, `avg_reconstruct_clone_us` | Other stage costs |
   | `avg_max_per_dest_us` | Avg of the slowest single-destination send per batch |
   | `max_total_us`, `max_fanout_send_us`, `max_per_dest_us` | Tail latencies |

   **Packets/sec** is intentionally not emitted — divide `packets` by
   `metrics_report_interval_ms / 1000` downstream.

### Extracting for the HTML dashboard

Both the per-batch `trace!` lines and the aggregated `datapoint_info!` rows go
to **stderr** via `env_logger`. Merge stderr into stdout with `2>&1` so you can
pipe/redirect. (If `SOLANA_METRICS_CONFIG` is set, `datapoint_info!` is *also*
shipped to InfluxDB; the local stderr line still prints either way.)

**Option A — capture full log, grep afterwards.** Best when you want to re-grep
different fields without re-running.

```bash
RUST_LOG=info,jito_shredstream_proxy::forwarder=trace \
  cargo run --release --bin jito-shredstream-proxy -- forward-only ... \
  > shredstream.log 2>&1

# stop with Ctrl-C, then extract:
grep "shredstream_proxy-forwarding_perf" shredstream.log > perf_aggregated.txt
grep "fwd_batch"                          shredstream.log > perf_per_batch.txt
```

**Option B — live filter + keep the raw log.** Best when watching a profiling
run for a fixed window.

```bash
RUST_LOG=info,jito_shredstream_proxy::forwarder=trace \
  cargo run --release --bin jito-shredstream-proxy -- forward-only ... \
  2>&1 | tee shredstream.log | grep --line-buffered "shredstream_proxy-forwarding_perf"
```

- `tee shredstream.log` keeps the full raw log on disk for later re-grep.
- `grep --line-buffered` flushes matches to your terminal immediately
  (without it, pipe buffering can delay output by seconds).

Feed `perf_aggregated.txt` (or whatever you `tee` to) into the
`latency_dashboard.html` / `fanout_optimization.html` data tables.
