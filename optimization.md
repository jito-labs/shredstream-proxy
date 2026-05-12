# Fanout Optimization

This document describes a class of latency problems specific to UDP fanout in
the shredstream-proxy forwarder, the available optimizations, their tradeoffs,
and what was implemented in this iteration.

---

## The problem

`shredstream-proxy` forwards every received shred to every address listed in
`--dest-ip-ports` (and any addresses returned by the discovery service). The
observed symptom was that **end-to-end latency for the last destination grew
linearly with the number of destinations** `D`. With 1 destination it was
fast; with 20 destinations the last one saw roughly 20× the per-destination
cost.

### Root cause (original code)

Before this change, `recv_from_channel_and_send_multiple_dest` in
`proxy/src/forwarder.rs` used the following structure:

```rust
local_dest_sockets.iter().for_each(|dest| {
    let packets_with_dest = packet_batch_vec[0]
        .iter()
        .filter_map(|pkt| Some((pkt.data(..)?, dest)))
        .collect::<Vec<(&[u8], &SocketAddr)>>();   // (1) per-destination Vec alloc
    batch_send(send_socket, &packets_with_dest);    // (2) per-destination sendmmsg
});
```

Three compounding costs scaled with `D`:

1. **Serial loop, single send socket.** Destination `i+1` could not begin
   sending until destination `i`'s `batch_send` had returned from the kernel.
   The last destination observed `D × T_per_dest`.

2. **`batch_send` is itself O(P) in kernel work** (P = packets per batch). It
   issues `sendmmsg` syscalls (up to `UIO_MAXIOV` = 1024 iovecs each), so each
   call is a real round-trip into the kernel. Total: D syscalls per batch.

3. **Per-destination `Vec<(&[u8], &SocketAddr)>` allocation.** The vec only
   varies by the address; the byte slices are identical for every destination.
   Yet a fresh heap allocation of size P happened D times per batch.

Secondary amplifiers:

- **Single shared `send_socket`.** All D destinations share one UDP socket;
  the kernel send buffer becomes a contended resource.
- **Default `SO_SNDBUF`** (~200–400 KB on Linux). Once full, `sendmmsg` blocks
  and tail latency for late destinations grows non-linearly.
- **No connected sockets.** `send_to` re-runs route lookup per call;
  connected sockets cache the route.
- **No GSO** (Linux). Every datagram is its own kernel walk.

---

## Catalog of fanout optimizations

Listed in roughly descending ROI for this codebase. Each entry includes the
mechanism, the tradeoff, and the expected effect on latency vs. `D`.

### 1. Single `batch_send` for all `(packet, dest)` pairs (IMPLEMENTED, in spirit)

The original proposal was to build one flat `Vec<(&[u8], &SocketAddr)>` of
length `P*D` and call `batch_send` once. After also adopting optimization #3
(per-destination workers, below), the implementation no longer has a single
fanout call site — each per-destination worker performs its own `batch_send`
covering `P` packets. The "one syscall per destination" guarantee is
preserved (since `batch_send` is `sendmmsg(2)` on Linux), and the workers
run concurrently, so the **total wall-clock** is `max(T_per_dest)` rather
than `sum(T_per_dest)`.

**Effect:** Per worker, `P` packets leave the kernel in 1 syscall (or
`ceil(P / 1024)`). All `D` workers run in parallel, so the **observed
fanout cost is flat in `D`** (assuming sufficient CPU and kernel parallelism).

**Tradeoffs**
- (+) Latency stops scaling linearly with D.
- (+) Per-destination partial-send attribution is preserved (each worker
  reports its own `num_failed`), unlike a flat `P*D` call which loses
  per-destination attribution.
- (−) D worker threads instead of one fanout loop.

### 2. Reused scratch `Vec` across batches (IMPLEMENTED)

Each per-destination worker owns a
`Vec<(&'static [u8], &'static SocketAddr)>` initialized with capacity
`DEST_SCRATCH_INITIAL_CAPACITY` (128). On each batch it pushes references
into the Vec, calls `batch_send`, and `clear()`s before the next iteration.
The `'static` lifetime is a phantom — the references are transmuted only for
storage purposes; `clear()` runs before the source `Arc<PacketBatch>` is
dropped, so no transmuted reference ever escapes.

**Effect:** Zero allocations on the steady-state hot path of UDP fanout
inside each worker.

**Tradeoffs**
- (+) Zero allocations after warmup. The destination `SocketAddr` is
  boxed once per worker; the box address is stable for the worker's
  lifetime, so the destination reference is genuinely safe to transmute
  to `'static`.
- (−) Two `unsafe { std::mem::transmute }` blocks (one for the data
  reference per push, one for the destination at worker startup), both
  documented with SAFETY comments.

### 3. Per-destination connected sockets + parallel sends (IMPLEMENTED, partially)

Each destination has a dedicated worker thread (named
`ssPxyDst_<ip>_<port>`) that owns its own `UdpSocket` and a bounded
`crossbeam_channel::Receiver<Arc<PacketBatch>>`. The recv-side coordinator
thread (`ssPxyTx_<i>`) does dedup + stats once, wraps the batch in `Arc`, and
dispatches a clone of the `Arc` to each per-destination worker channel. The
workers run their UDP sends concurrently.

**Effect:** Worker `i+1` does not wait for worker `i`. End-to-end fanout
latency becomes `max(T_per_dest)`. Slow destinations no longer block fast
ones; they only build up backpressure on their own bounded channel, which
drops batches (counted in `worker_dropped_batches`) rather than slow the
hot path.

**What is *not* implemented:** the `connect()` refinement. The
`solana_streamer::sendmmsg::batch_send` API takes `&[(T, S)]` and calls
`send_to`, which is invalid on a connected socket. To use connected sockets
we would need a custom `sendmmsg` variant with `msg_name = NULL` per
`mmsghdr`. The implementation uses unconnected per-destination
`UdpSocket`s (different ephemeral source port per dest, but route lookup
is *not* cached). Estimated cost of skipping connect(): 10–20% extra per
packet inside the kernel.

**Tradeoffs**
- (+) Truly O(1) per-destination latency in `D`.
- (+) Each worker has its own kernel send buffer — no contention.
- (+) Dynamic destination set: a dedicated manager thread (`ssPxyDstMgr`)
  reconciles workers against `unioned_dest_sockets` every
  `DEST_RECONCILE_INTERVAL`.
- (−) D additional threads and D additional UDP sockets.
- (−) `Arc<PacketBatch>` is cloned `D` times per batch (cheap — ref-count
  increment only).
- (−) Connected-socket route caching not realized (see above).
- (−) For very small D (≤ 4), the channel-hop cost may outweigh the
  parallelism. Pre-existing single-threaded fanout would be faster in that
  regime, but the regime of interest is `D ≥ 20`.

### 4. `UDP_GSO` / `UDP_SEGMENT` (Linux ≥ 4.18) (NOT IMPLEMENTED)

Set `UDP_SEGMENT` via `setsockopt`. The kernel splits one large buffer of
`N × MTU` into N UDP datagrams in a single `sendmsg`. Combined with #1, an
entire batch (`P × D` packets) can leave the host in a single syscall.

**Tradeoffs**
- (+) Massive: one syscall for arbitrarily large fanout.
- (−) Linux-specific; doesn't work on macOS dev machines.
- (−) All destinations must share segment size; mismatched MTU causes
  fragmentation.
- (−) Per-packet error info is lost.

### 5. Larger `SO_SNDBUF` (NOT IMPLEMENTED)

Raise the send-side socket buffer to 8–16 MB via
`solana_net_utils::SocketConfig`.

**Tradeoffs**
- (+) Reduces probability that `sendmmsg` blocks under bursty traffic with
  large `D`.
- (−) Larger kernel memory footprint per send socket; with D workers each
  having its own socket, this multiplies.
- (−) Doesn't help when the actual cost is per-destination iteration; this is
  a downstream amplifier mitigation, not a root-cause fix.

### 6. Multicast (NOT IMPLEMENTED — already supported via separate path)

When destinations live on a multicast-capable network, send once and let the
network duplicate. `D` disappears from the cost equation entirely.

**Tradeoffs**
- (+) Optimal where applicable.
- (−) Requires multicast-capable network. Doesn't work over WAN, most cloud
  VPCs, or NAT-ed environments. The codebase already supports multicast via
  `multicast_config.rs`, so this option exists for operators with the right
  network topology.

### 7. Shard destinations across send threads (SUPERSEDED by #3)

The recv side now spawns N coordinator threads (`ssPxyTx_<i>`) and each one
dispatches to *all* D destination workers. The actual UDP send parallelism
is D, independent of N. This subsumes the proposed sharding scheme.

### 8. Skip route lookups via `send_to` → `send` on connected sockets (PART OF #3)

Folded into optimization #3. **Not yet realized** because of the
`batch_send` API constraint described in #3. Implementing this requires
writing a `sendmmsg`-with-`msg_name=NULL` helper or switching to per-packet
`send()` (which gives up the sendmmsg batching benefit). Filed as a future
optimization.

---

## What was implemented in this iteration

### Architecture (`proxy/src/forwarder.rs`)

```
[N listener threads (solana_streamer::receiver)]
        ↓ PacketBatch via unbounded crossbeam channel
[N coordinator threads (ssPxyTx_<i>)]
        - recv PacketBatch
        - try_send clone to reconstruct channel (gRPC service path)
        - dedup in place (sets discard flag on packets)
        - update per-source stats (DashMap)
        - wrap in Arc<PacketBatch>
        - try_send Arc clone to each per-dest worker channel
        ↓ Arc<PacketBatch> via bounded crossbeam channel (per destination)
[D per-destination worker threads (ssPxyDst_<ip>_<port>)]
        - own one UdpSocket (unconnected)
        - own reused scratch Vec<(&'static [u8], &'static SocketAddr)>
        - recv Arc<PacketBatch>
        - push (data, dest) refs into scratch (skipping discarded packets)
        - batch_send (= sendmmsg(2) on Linux, 1 syscall for ≤1024 packets)
        - clear() scratch
        ↓
[ssPxyDstMgr thread]
        - polls unioned_dest_sockets every DEST_RECONCILE_INTERVAL
        - spawns workers for newly added destinations
        - drops senders for removed destinations (worker exits on Disconnected)
```

### Code changes

1. **Optimization #1 (per-worker `batch_send`).** Each worker handles only
   one destination; `batch_send` therefore packs `P` packets to that
   destination into one `sendmmsg` syscall.

2. **Optimization #2 (reused scratch Vec).** Per-worker, with `'static`
   phantom lifetime and SAFETY-annotated `transmute`.

3. **Optimization #3 (per-destination parallel workers).** New thread tier
   plus `ssPxyDstMgr` for dynamic reconciliation. `connect()` refinement
   not applied (see #3 above).

4. **Metrics correctness fixes.**
   - `metrics.duplicate` used to be incremented `D` times per batch (once
     per destination). Now incremented once per batch.
   - `metrics.fail_forward` used to report the entire batch size on partial
     failure. Now reports the actual `num_failed` from `SendPktsError`.

5. **Performance instrumentation (gated on `RUST_LOG=...forwarder=trace`).**
   Two aggregated datapoints emitted per `--metrics-report-interval-ms`:
   - `shredstream_proxy-forwarding_perf`: coordinator-side breakdown
     (`avg_total_us`, `avg_dedup_us`, `avg_stats_us`, `avg_fanout_dispatch_us`,
     `worker_count`, `worker_dropped_batches`, etc.).
   - `shredstream_proxy-worker_perf`: per-destination send timings
     (`avg_worker_send_us`, `max_worker_send_us`, `worker_batches`,
     `worker_packets_sent`).

   When trace is off, no `Instant::now()` calls and no atomic accumulator
   updates run on the data path.

### Tests

| Test | Status |
|---|---|
| `test_2shreds_3destinations` | Ported to the new worker-based signature: spawns workers per destination, dispatches via `DestSenderMap`, joins workers on teardown. Passes. |

The richer test matrix listed in the previous iteration of this doc
(`test_fanout_many_destinations`, `test_scratch_buffer_reused_across_batches`,
`test_zero_destinations_is_noop`, `test_single_destination_preserves_order`,
`test_scratch_grows_then_reuses`) has **not** been ported to the new
architecture in this iteration. Adding them is straightforward — each spawns
workers via `spawn_dest_worker`, dispatches one or more batches, and asserts
on the same listener-side invariants as before.

### Expected impact

For a representative workload (P=64 packets per batch, D=20 destinations):

| Cost component | Original | After #1+#2 only | After #1+#2+#3 (this iteration) |
|---|---|---|---|
| `sendmmsg` syscalls per batch | 20 (serial) | 1 (single flat call) | 20 (parallel, one per worker) |
| Heap allocations per batch | 20 | 0 (steady state) | 0 (steady state, per-worker) |
| Recv-side wall-clock | dominated by D | dominated by 1 send call | dominated by dispatch (~µs) |
| **Last-destination wall-clock** | `~D × T_per_dest` | `~T_per_dest` | `~T_per_dest` (in parallel) |
| Slow-dest poisons fast dest? | Yes | Yes (single serial loop) | **No** (per-dest channel) |

The headline metric to watch in `shredstream_proxy-worker_perf` is
`avg_worker_send_us` and `max_worker_send_us` — these are the per-destination
UDP send latencies, now decoupled from D.

---

## Configurable settings and constants

All currently-tunable behavior lives in two places: module-level `const`
declarations in `proxy/src/forwarder.rs` and existing CLI flags in
`proxy/src/main.rs`.

### Module constants (require a rebuild to change)

| Constant | Value | Where used | Tuning guidance |
|---|---|---|---|
| `DEST_CHANNEL_CAPACITY` | `1024` | Bound on each per-destination channel | Raise if `worker_dropped_batches > 0` is observed and you can afford the memory (per-dest channel can hold up to N × Arc<PacketBatch>; the inner allocation is shared). Lower to bound memory at the cost of more drops to slow destinations. |
| `DEST_SCRATCH_INITIAL_CAPACITY` | `128` | Initial capacity of each worker's reused scratch Vec | Set close to the typical packets-per-batch value to avoid a few growth reallocations during warmup. The Vec grows monotonically anyway; this is only a startup-cost knob. |
| `DEST_RECONCILE_INTERVAL` | `Duration::from_secs(5)` | How often `ssPxyDstMgr` polls `unioned_dest_sockets` to add/remove workers | Lower if destination churn is high and you need workers to come online faster. Raise to reduce reconcile-thread overhead. |
| `DEDUPER_FALSE_POSITIVE_RATE` | `0.001` | Bloom filter false-positive rate | Pre-existing. Lower => larger filter, more memory, fewer false-positive drops. |
| `DEDUPER_NUM_BITS` | `637_534_199` (~76 MB) | Bloom filter size | Pre-existing. |
| `DEDUPER_RESET_CYCLE` | `Duration::from_secs(5 * 60)` | How often to recycle the dedup filter | Pre-existing. |

### CLI / environment flags (no rebuild required)

| Flag | Default | Effect on perf |
|---|---|---|
| `--num-threads <N>` | `min(available_parallelism, 4)` | Number of listener + coordinator pairs. With the new architecture, `N` only parallelizes the recv side (the send side is parallelized by destination workers regardless). Raise if `avg_total_us` is dominated by dedup + stats (rare) and listener sockets are CPU-bound. |
| `--metrics-report-interval-ms <ms>` | `15000` | Period for `shredstream_proxy-*` datapoint emission. Lower for finer-grained profiling, higher for less log volume. |
| `--debug-trace-shred` | `false` | Enables `TraceShred` decoding for end-to-end latency measurement against the upstream sender. Adds per-packet decode cost; keep off in production. |
| `--src-bind-addr` / `--src-bind-port` | `0.0.0.0:20000` | Receive socket; `src_bind_port + 1` is also bound when `multi_bind_in_range_with_config` allocates a range. |

### `RUST_LOG` gating (no rebuild required)

The performance instrumentation has zero runtime cost when trace level is not
enabled for `jito_shredstream_proxy::forwarder`. To enable:

```bash
RUST_LOG=info,jito_shredstream_proxy::deshred=off,jito_shredstream_proxy::forwarder=trace
```

To run with zero perf-instrumentation overhead:

```bash
RUST_LOG=info
```

### Kernel-level tunables to consider (not yet wired into CLI)

These would require either `setsockopt` calls in `spawn_dest_worker` or
sysctl changes on the host. They are **not currently set by the binary**:

| Tunable | Reason to change |
|---|---|
| `SO_SNDBUF` on each per-dest socket | Default ~212 KB on Linux. Raise to 4–16 MB per socket if `worker_dropped_batches > 0` or `max_worker_send_us` shows occasional spikes (kernel send buffer full, sendmmsg blocking). |
| `net.core.wmem_max` (sysctl) | Must be raised to allow processes to set `SO_SNDBUF` above the system cap. |
| `net.core.netdev_max_backlog` | Helps the *receive* side under burst, not directly the sender. |
| CPU affinity for `ssPxyDst_*` workers | If you have D > N_cores and you observe NUMA effects, pin each worker to a core. Not currently done. |

---

## What is left to do

Listed roughly in order of expected ROI:

1. **Connected sockets in workers (finish #3 + #8).** Write a custom
   `sendmmsg(msg_name=NULL)` helper or switch to per-packet `send()` on
   connected sockets. Expected gain: 10–20% per-packet on the kernel send
   path. Requires unsafe libc bindings or a `nix::sys::socket::sendmmsg`
   wrapper.

2. **`SO_SNDBUF` tuning.** Add a CLI flag, e.g.
   `--per-dest-sndbuf-bytes`, and call `setsockopt(SO_SNDBUF, ...)` in
   `spawn_dest_worker`. Pair with a documented `net.core.wmem_max` bump.

3. **UDP_GSO (optimization #4) on Linux.** Largest theoretical win, biggest
   implementation effort. Requires per-destination MTU consistency and a
   fallback path.

4. **Port the wider test matrix** (`test_fanout_many_destinations`,
   `test_zero_destinations_is_noop`, `test_scratch_grows_then_reuses`, etc.)
   to the worker-based architecture.

5. **Per-destination latency attribution in metrics.** Currently
   `avg_worker_send_us` is a global average over all workers. Per-dest
   breakdown would require tagged datapoint emission (one per dest per
   interval) — useful for spotting one slow destination among many.

6. **CPU affinity / NUMA pinning** for worker threads in deployments with
   `D > num_cores`.
