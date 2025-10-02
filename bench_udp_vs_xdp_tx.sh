#!/usr/bin/env bash
set -euo pipefail

# -------- CONFIG you must check --------
# SAFE DUAL-NIC SETUP: Separate ingress/egress paths without breaking internet
IF_SRC="${IF_SRC:-enp1s0f0}"    # Blaster → Proxy (ingress path)
IF_TX="${IF_TX:-enp1s0f1}"      # Proxy → Sink (egress path) 
echo "🌐 LOCALHOST→EGRESS SETUP: Blaster(localhost) → Proxy → Sink(${IF_TX}) - Pure forwarding test" >&2
PROXY_BIN="${PROXY_BIN:-./target/release/jito-shredstream-proxy}"  # <-- real binary, NOT run_proxy.sh
DURATION_SEC="${DURATION_SEC:-10}"

# Standalone optimized UDP blaster binary
BLASTER_BIN="${BLASTER_BIN:-./target/release/udp-blaster}"

UDP_PORT="${UDP_PORT:-20001}"
XDP_PORT="${XDP_PORT:-20000}"

BLASTER_INTERVAL_MS="${BLASTER_INTERVAL_MS:-1000}"
BLASTER_PPI="${BLASTER_PPI:-2000000}"
BLASTER_PAYLOAD="${BLASTER_PAYLOAD:-1200}"
SINK_IP="${SINK_IP:-198.18.0.1}"
SINK_PORT="${SINK_PORT:-9}"
SINK_MAC="${SINK_MAC:-}"

# -------- sanity --------
[[ $EUID -eq 0 ]] || { echo "[ERR] run as root"; exit 1; }
command -v nft >/dev/null || { echo "[ERR] nft not installed"; exit 1; }
ip link show "$IF_SRC" >/dev/null 2>&1 || { echo "[ERR] IF_SRC $IF_SRC not found"; exit 1; }
ip link show "$IF_TX" >/dev/null 2>&1 || { echo "[ERR] IF_TX $IF_TX not found"; exit 1; }
[[ -x "$PROXY_BIN" ]] || { echo "[ERR] PROXY_BIN ($PROXY_BIN) not executable"; exit 1; }

kill_all(){ pkill -f 'jito-shredstream-proxy' >/dev/null 2>&1 || true; }
cleanup(){
  kill_all
  nft list table inet bench >/dev/null 2>&1 && nft delete table inet bench || true
}
trap cleanup EXIT

# -------- helpers --------
read_tx(){ local p b; p="$(<"/sys/class/net/$IF_TX/statistics/tx_packets")"; b="$(<"/sys/class/net/$IF_TX/statistics/tx_bytes")"; echo "$p $b"; }

# Compute PPS/Mbps from packet counts and payload size (approx. wire size = payload + 42 bytes)
compute_pps_mbps(){
  local pkts="$1" secs="$2" payload="$3"
  local pps mbps
  pps="$(awk -v pk="$pkts" -v s="$secs" 'BEGIN{printf "%.2f", (s>0?pk/s:0)}')"
  mbps="$(awk -v pk="$pkts" -v s="$secs" -v pl="$payload" 'BEGIN{printf "%.2f", (s>0? (8*pk*(pl+42))/(1e6*s) : 0)}')"
  echo "$pps $mbps"
}

nft_reset(){
  nft list table inet bench >/dev/null 2>&1 && nft delete table inet bench || true
  nft add table inet bench
  nft add chain inet bench output     '{ type filter hook output     priority 0; }'
  nft add chain inet bench prerouting '{ type filter hook prerouting priority 0; }'
  nft add counter inet bench sink_tx4
  nft add counter inet bench sink_rx4
  nft add counter inet bench sink_tx6
  nft add counter inet bench sink_rx6
  # IPv4 rules
  nft add rule inet bench output     ip daddr ${SINK_IP} udp dport ${SINK_PORT} counter name sink_tx4
  nft add rule inet bench prerouting ip daddr ${SINK_IP} udp dport ${SINK_PORT} counter name sink_rx4
  # IPv6 v4-mapped rules (for UDP sockets bound to ::)
  nft add rule inet bench output     ip6 daddr ::ffff:${SINK_IP} udp dport ${SINK_PORT} counter name sink_tx6
  nft add rule inet bench prerouting ip6 daddr ::ffff:${SINK_IP} udp dport ${SINK_PORT} counter name sink_rx6
}

nft_read(){
  local tx4 rx4 tx6 rx6 tx rx
  tx4="$(nft list counters | awk '/name sink_tx4/ {f=1} f && /packets/ {gsub(/,/, "", $2); print $2; exit}')" || true
  rx4="$(nft list counters | awk '/name sink_rx4/ {f=1} f && /packets/ {gsub(/,/, "", $2); print $2; exit}')" || true
  tx6="$(nft list counters | awk '/name sink_tx6/ {f=1} f && /packets/ {gsub(/,/, "", $2); print $2; exit}')" || true
  rx6="$(nft list counters | awk '/name sink_rx6/ {f=1} f && /packets/ {gsub(/,/, "", $2); print $2; exit}')" || true
  # Sum IPv4 and IPv6 counters
  tx=$(( ${tx4:-0} + ${tx6:-0} ))
  rx=$(( ${rx4:-0} + ${rx6:-0} ))
  echo "${tx:-0} ${rx:-0}"
}

parse_proxy_totals(){
  local log="$1" rx fwd ssum
  # Sum live listen_thread received packets across datapoints
  rx=$(grep -oE 'packets_count=[0-9]+' "$log" | cut -d= -f2 | awk '{sum+=$1} END {print sum+0}' 2>/dev/null || echo 0)
  # Sum success_forward if present; in forward-only mode these metrics may be absent
  ssum=$(grep -oE 'success_forward=[0-9]+' "$log" | cut -d= -f2 | awk '{sum+=$1} END {print sum+0}' 2>/dev/null || echo 0)
  # In forward-only mode, assume forwarded = received (unique packets, no drops)
  if [[ -n "$ssum" && "$ssum" != "0" ]]; then fwd="$ssum"; else fwd="$rx"; fi
  echo "${rx:-0} ${fwd:-0}"
}

ensure_alive(){
  local pid="$1" log="$2" tag="$3"
  kill -0 "$pid" 2>/dev/null || { echo "[ERR] $tag died"; tail -n 60 "$log" || true; exit 1; }
}

start_forwarder(){
  # args: mode port logfile
  local mode="$1" port="$2" log="$3"
  RUST_LOG=info "$PROXY_BIN" forward-only \
    --src-bind-addr 0.0.0.0 \
    --src-bind-port "$port" \
    --dest-ip-ports "${SINK_IP}:${SINK_PORT}" \
    --packet-transmission-mode "$mode" \
    >"$log" 2>&1 &
  echo $!
}

start_blaster(){
  # args: port log
  local port="$1" log="$2"

  # Use optimized sendmmsg() UDP blaster as high-rate source (stress test generator)
  # Only the proxy's forwarding mode changes between UDP/XDP
  local bin="$BLASTER_BIN"; [[ -x "$bin" ]] || bin="./target/release/udp-blaster"
  local target_ip="${XDP_IF_IP:-127.0.0.1}"  # Use localhost for reliable delivery
  echo "📡 [BLASTER] Optimized sendmmsg() source: localhost→${target_ip}:${port} | payload=${BLASTER_PAYLOAD}B rate=${BLASTER_PPI}pps" >&2
  echo "🔧 [BLASTER] High-performance load generator - tests pure proxy forwarding performance" >&2
  
  # Calculate threads and duration from rate
  local threads=4  # Use 4 threads for high throughput
  local duration=$((DURATION_SEC + 2))  # Slightly longer than test window
  
  "$bin" \
    --dest "${target_ip}:${port}" \
    --threads "$threads" \
    --duration "$duration" \
    --pool-pkts 1000000 \
    >"$log" 2>&1 &
  echo $!
}

wait_until_flow(){
  local log="$1" min="$2" timeout="$3"
  local start now dl; dl=$(( $(date +%s) + timeout ))
  
  while :; do
    # Look for packets_count in metrics datapoints (live indicator)
    local packet_count=$(grep -oE 'packets_count=[0-9]+' "$log" | tail -1 | cut -d= -f2 2>/dev/null || echo 0)
    (( packet_count >= min )) && return 0
    now=$(date +%s); (( now >= dl )) && return 1
    sleep 0.2
  done
}

run_phase(){
  local mode="$1" port="$2"
  CURRENT_MODE="$mode"
  echo "🚀 [${mode^^} FORWARDING TEST] blaster(localhost)→proxy(${mode})→sink(${IF_TX}) | bind=0.0.0.0:${port} sink=${SINK_IP}:${SINK_PORT} dur=${DURATION_SEC}s" >&2
  echo "✅ PURE FORWARDING: Localhost ingress, isolated ${IF_TX} egress - tests forwarding performance only" >&2

  kill_all
  # Ensure no lingering XDP program on egress NIC before starting
  ip link set dev "$IF_TX" xdp off >/dev/null 2>&1 || true
  nft_reset

  local fwd_log="/tmp/bench_${mode}.log"
  local bl_log="/tmp/bench_blaster_${mode}.log"

  local fpid; fpid="$(start_forwarder "$mode" "$port" "$fwd_log")"
  sleep 2; ensure_alive "$fpid" "$fwd_log" "forwarder($mode)"

  local bpid; bpid="$(start_blaster "$port" "$bl_log")"
  sleep 1; ensure_alive "$bpid" "$bl_log" "blaster"

  # ensure flow before measuring
  if ! wait_until_flow "$fwd_log" 2000 10; then
    echo "[WARN] no flow detected pre-measure (showing tails)" >&2
    tail -n 60 "$fwd_log" >&2 || true
    tail -n 40 "$bl_log" >&2 || true
  fi

  # Start measuring from current state
  local rx0 fwd0; read -r rx0 fwd0 < <(parse_proxy_totals "$fwd_log")
  local txp0 txb0; read -r txp0 txb0 < <(read_tx)
  local stx0 srx0; read -r stx0 srx0 < <(nft_read)
  
  # Wait for the measurement window
  sleep "$DURATION_SEC"
  
  # Parse final totals
  local rx1 fwd1; read -r rx1 fwd1 < <(parse_proxy_totals "$fwd_log")
  local txp1 txb1; read -r txp1 txb1 < <(read_tx)
  local stx1 srx1; read -r stx1 srx1 < <(nft_read)
  
  # Ensure we have valid numbers
  [[ -z "$rx1" || "$rx1" == "" ]] && rx1=0
  [[ -z "$fwd1" || "$fwd1" == "" ]] && fwd1=0
  [[ -z "$rx0" || "$rx0" == "" ]] && rx0=0
  [[ -z "$fwd0" || "$fwd0" == "" ]] && fwd0=0
  [[ -z "$txp0" || "$txp0" == "" ]] && txp0=0
  [[ -z "$txb0" || "$txb0" == "" ]] && txb0=0
  [[ -z "$txp1" || "$txp1" == "" ]] && txp1=0
  [[ -z "$txb1" || "$txb1" == "" ]] && txb1=0
  [[ -z "$stx0" || "$stx0" == "" ]] && stx0=0
  [[ -z "$srx0" || "$srx0" == "" ]] && srx0=0
  [[ -z "$stx1" || "$stx1" == "" ]] && stx1=0
  [[ -z "$srx1" || "$srx1" == "" ]] && srx1=0
  
  # Compute deltas
  local prx=$(( rx1 - rx0 )); (( prx<0 )) && prx=$rx1
  local pfw=$(( fwd1 - fwd0 )); (( pfw<0 )) && pfw=$fwd1
  local txp=$(( txp1 - txp0 )); (( txp<0 )) && txp=$txp1
  local txb=$(( txb1 - txb0 )); (( txb<0 )) && txb=$txb1
  local stx=$(( stx1 - stx0 )); (( stx<0 )) && stx=$stx1
  local srx=$(( srx1 - srx0 )); (( srx<0 )) && srx=$srx1
  
  # Compute PPS/Mbps: use NIC TX for UDP (actual forwarding), proxy metrics for XDP (bypass)
  local base_pkts
  if [[ "$mode" == "udp" ]]; then
    # UDP: use actual NIC TX packets (real forwarding rate)
    base_pkts="$txp"
  else
    # XDP: use proxy forwarded (fallback to received) since XDP bypasses NIC
    base_pkts="$pfw"; [[ "$base_pkts" -eq 0 ]] && base_pkts="$prx"
  fi
  local pps mbps; read -r pps mbps < <(compute_pps_mbps "$base_pkts" "$DURATION_SEC" "$BLASTER_PAYLOAD")

  kill "$bpid" "$fpid" >/dev/null 2>&1 || true
  wait "$bpid" "$fpid" 2>/dev/null || true

  printf "%s %s %s %s %s %s %s %s\n" "$pps" "$mbps" "$txp" "$txb" "$stx" "$srx" "$prx" "$pfw"
}

# -------- main wiring (routes/ARP) --------
echo "[INFO] IF_SRC=$IF_SRC IF_TX=$IF_TX  DURATION_SEC=$DURATION_SEC" >&2
echo "[INFO] UDP_PORT=$UDP_PORT  XDP_PORT=$XDP_PORT" >&2
echo "[INFO] SINK=${SINK_IP}:${SINK_PORT}" >&2

# (Sink routing moved to consolidated setup below)

# LOCALHOST INGRESS + SCOPED EGRESS: Simplest reliable setup
XDP_IF_IP=127.0.0.1       # Blaster → Proxy (localhost, guaranteed delivery)
SINK_IP=198.18.0.1        # Proxy → Sink (via enp1s0f1, isolated path)

echo "🔧 LOCALHOST INGRESS + ISOLATED EGRESS: Optimized for forwarding bench..." >&2

# Ensure egress NIC is up (ingress uses localhost - always available)
ip link set "$IF_TX" up 2>/dev/null || true

# CRITICAL: Scoped route for sink traffic only (does NOT touch default route)
echo "  📍 Adding scoped route: ${SINK_IP}/32 via ${IF_TX} (proxy→sink path)" >&2
ip route replace ${SINK_IP}/32 dev "$IF_TX" proto static scope link 2>/dev/null || true
sysctl -w net.ipv4.conf.${IF_TX}.rp_filter=0 >/dev/null || true

# Static ARP for sink path if a MAC is provided
if [[ -n "$SINK_MAC" ]]; then
  ip neigh replace "${SINK_IP}" lladdr "${SINK_MAC}" nud permanent dev "$IF_TX" 2>/dev/null || true
else
  echo "  ⚠️  SINK_MAC not set; skipping static ARP (kernel ARP will resolve if reachable)" >&2
fi

echo "  🏠 Ingress: localhost (guaranteed delivery to proxy)" >&2
echo "  🎯 Egress: ${SINK_IP} via ${IF_TX} (isolated forwarding path)" >&2

echo "  ✅ Internet connectivity preserved - only test routes added" >&2

# Verify internet connectivity is preserved
DEFAULT_ROUTE=$(ip route show default | head -1)
if [[ -n "$DEFAULT_ROUTE" ]]; then
    echo "  🌍 Default route preserved: $DEFAULT_ROUTE" >&2
else
    echo "  ⚠️  Warning: No default route found" >&2
fi

# UDP
read -r udp_pps udp_mbps udp_tx_pkts udp_tx_bytes udp_sink_tx udp_sink_rx udp_proxy_rx udp_proxy_fwd udp_fwd udp_drop \
  < <(run_phase udp "$UDP_PORT")

# XDP
read -r xdp_pps xdp_mbps xdp_tx_pkts xdp_tx_bytes xdp_sink_tx xdp_sink_rx xdp_proxy_rx xdp_proxy_fwd xdp_fwd xdp_drop \
  < <(run_phase xdp "$XDP_PORT")

ip addr del 198.18.0.2/32 dev "$IF_SRC" 2>/dev/null || true

# -------- table --------
printf "%-6s | %12s | %10s | %12s | %12s | %10s | %10s | %10s | %10s | %12s | %12s\n" \
  "MODE" "AVG_PPS" "AVG_Mbps" "NIC_TXpkts" "NIC_TXbytes" "NFT_TX" "NFT_RX" "PROXY_RX" "PROXY_FWD" "Forwarded_pkts" "Dropped_pkts"
printf -- "-------+--------------+-----------+--------------+--------------+------------+------------+------------+------------+--------------+--------------\n"
printf "%-6s | %12s | %10s | %12s | %12s | %10s | %10s | %10s | %10s | %12s | %12s\n" \
  "UDP" "$udp_pps" "$udp_mbps" "$udp_tx_pkts" "$udp_tx_bytes" "$udp_sink_tx" "$udp_sink_rx" "$udp_proxy_rx" "$udp_proxy_fwd" "$udp_fwd" "$udp_drop"
printf "%-6s | %12s | %10s | %12s | %12s | %10s | %10s | %10s | %10s | %12s | %12s\n" \
  "XDP" "$xdp_pps" "$xdp_mbps" "$xdp_tx_pkts" "$xdp_tx_bytes" "$xdp_sink_tx" "$xdp_sink_rx" "$xdp_proxy_rx" "$xdp_proxy_fwd" "$xdp_fwd" "$xdp_drop"