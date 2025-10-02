#!/usr/bin/env bash
set -euo pipefail

# -------- config --------
DURATION_SEC="${DURATION_SEC:-20}"
PROXY_BIN="${PROXY_BIN:-./target/release/jito-shredstream-proxy}"

UDP_PORT="${UDP_PORT:-20001}"
XDP_PORT="${XDP_PORT:-20000}"

SINK_IP="${SINK_IP:-198.18.0.1}"
SINK_PORT="${SINK_PORT:-9}"
IF_TX="${IF_TX:-enp1s0f1}"
ENABLE_SCOPED_ROUTE="${ENABLE_SCOPED_ROUTE:-1}"
SINK_MAC="${SINK_MAC:-}"
ENABLE_IF_ADDR="${ENABLE_IF_ADDR:-0}"

# BE auth/regions
BE_URL="${BE_URL:-https://ny.mainnet.block-engine.jito.wtf}"
AUTH_KEYPAIR="${AUTH_KEYPAIR:-/root/.config/solana/id.json}"
DESIRED_REGIONS="${DESIRED_REGIONS:-ny}"
AUTH_URL="${AUTH_URL:-}"
PUBLIC_IP="${PUBLIC_IP:-}"
METRICS_MS="${METRICS_MS:-2000}"

[[ -x "${PROXY_BIN}" ]] || { echo "[ERR] PROXY_BIN not executable: ${PROXY_BIN}" >&2; exit 1; }

kill_all(){ pkill -f 'jito-shredstream-proxy shredstream' >/dev/null 2>&1 || true; }

effective_tx_dev(){
  # If IF_TX is enslaved (e.g., to bond0), use the master for routing/ARP and counters
  local m
  m=$(readlink "/sys/class/net/${IF_TX}/master" 2>/dev/null || true)
  if [[ -n "$m" ]]; then
    basename "$m"
  else
    echo "${IF_TX}"
  fi
}

# Determine current kernel-selected egress device for SINK_IP
route_dev_for_ip(){
  local out dev
  out=$(ip -o route get "${SINK_IP}" 2>/dev/null || true)
  dev=$(sed -n 's/.* dev \([^ ]*\).*/\1/p' <<<"$out" | head -1)
  if [[ -n "$dev" ]]; then echo "$dev"; else echo "$(effective_tx_dev)"; fi
}

pre_detach_xdp(){
  ip link set dev "${IF_TX}" xdp off 2>/dev/null || true
  ip link set dev "${IF_TX}" xdpgeneric off 2>/dev/null || true
}

ensure_scoped_route(){
  if [[ "${ENABLE_SCOPED_ROUTE}" = "1" ]]; then
    local tx_dev; tx_dev="$(effective_tx_dev)"
    echo "[ROUTE] ${SINK_IP}/32 via ${tx_dev} (scoped route for bench)" >&2
    local rt_cmd=(ip route replace "${SINK_IP}/32" dev "${tx_dev}" proto static scope link)
    if [[ "${ENABLE_IF_ADDR}" = "1" ]]; then
      rt_cmd+=(src 198.18.0.2)
    fi
    "${rt_cmd[@]}" 2>/dev/null || true
    if [[ -n "${SINK_MAC}" ]]; then
      echo "[ARP] Static ${SINK_IP} → ${SINK_MAC} on ${tx_dev}" >&2
      ip neigh replace "${SINK_IP}" lladdr "${SINK_MAC}" nud permanent dev "${tx_dev}" 2>/dev/null || true
    fi
  fi
}

cleanup_scoped_route(){
  if [[ "${ENABLE_SCOPED_ROUTE}" = "1" ]]; then
    local tx_dev; tx_dev="$(effective_tx_dev)"
    ip route del "${SINK_IP}/32" dev "${tx_dev}" 2>/dev/null || true
    if [[ -n "${SINK_MAC}" ]]; then
      ip neigh del "${SINK_IP}" dev "${tx_dev}" 2>/dev/null || true
    fi
  fi
}

# Optional: attach a temporary IPv4 on IF_TX to mirror fast_dedup environment
ADDED_ADDR=0
ensure_temp_if_addr(){
  if [[ "${ENABLE_IF_ADDR}" = "1" ]]; then
    local tx_dev; tx_dev="$(effective_tx_dev)"
    echo "[ADDR] Adding temporary 198.18.0.2/31 on ${tx_dev}" >&2
    ip link set "${tx_dev}" up 2>/dev/null || true
    if ip addr add 198.18.0.2/31 dev "${tx_dev}" 2>/dev/null; then ADDED_ADDR=1; fi
  fi
}

cleanup_temp_if_addr(){
  if [[ "${ENABLE_IF_ADDR}" = "1" && "${ADDED_ADDR}" = "1" ]]; then
    local tx_dev; tx_dev="$(effective_tx_dev)"
    echo "[ADDR] Removing temporary 198.18.0.2/31 from ${tx_dev}" >&2
    ip addr del 198.18.0.2/31 dev "${tx_dev}" 2>/dev/null || true
  fi
}
trap cleanup_temp_if_addr EXIT

read_tx(){
  local p b tx_dev
  tx_dev="${TX_DEV_OVERRIDE:-$(effective_tx_dev)}"
  p=$(<"/sys/class/net/${tx_dev}/statistics/tx_packets" 2>/dev/null || echo 0)
  b=$(<"/sys/class/net/${tx_dev}/statistics/tx_bytes" 2>/dev/null || echo 0)
  echo "${p:-0} ${b:-0}"
}

parse_totals(){
  local log="$1"
  local final
  final=$(grep "Exiting Shredstream" "$log" | tail -1 || true)
  if [[ -z "$final" ]]; then echo "0 0 0 0 0 0"; return; fi
  local received forwarded duplicates
  received=$(echo "$final" | grep -o '[0-9]* received' | grep -o '[0-9]*' || echo 0)
  forwarded=$(echo "$final" | grep -o '[0-9]* sent successfully' | grep -o '[0-9]*' || echo 0)
  duplicates=$(echo "$final" | grep -o '[0-9]* duplicate' | grep -o '[0-9]*' || echo 0)
  local recv_pps=$(( DURATION_SEC>0 ? received / DURATION_SEC : 0 ))
  local fwd_pps=$(( DURATION_SEC>0 ? forwarded / DURATION_SEC : 0 ))
  local eff=0 dup_rate=0
  if [[ ${received} -gt 0 ]]; then
    eff=$(( forwarded * 100 / received ))
    dup_rate=$(( duplicates * 100 / received ))
  fi
  printf "%s %s %s %s %s %s\n" "$recv_pps" "$fwd_pps" "$eff" "$dup_rate" "$received" "$forwarded"
}

run_mode(){
  local mode="$1" port="$2"
  local log="/tmp/bench_be_${mode}.log"
  kill_all
  echo "[${mode^^}] BE→Proxy(${mode})→Sink ${SINK_IP}:${SINK_PORT} for ${DURATION_SEC}s (sequential bench)" >&2
  local txp0 txb0 txp1 txb1
  # Ensure traffic to sink goes out the intended interface for BOTH modes
  if [[ "$mode" == "xdp" ]]; then
    pre_detach_xdp
  fi
  # Add temporary interface address if requested (used also by UDP for proper src selection)
  ensure_temp_if_addr
  ensure_scoped_route
  # Pick counters device based on current route to SINK_IP
  TX_DEV_OVERRIDE="$(route_dev_for_ip)"
  echo "[EGRESS] Using TX counters on dev=${TX_DEV_OVERRIDE}" >&2
  # Ensure egress interface is up for TX accounting
  # Ensure egress device is up for TX accounting
  local tx_dev; tx_dev="${TX_DEV_OVERRIDE:-$(effective_tx_dev)}"
  ip link set dev "${tx_dev}" up 2>/dev/null || true
  read -r txp0 txb0 < <(read_tx)
  timeout "${DURATION_SEC}" env RUST_LOG=info \
    ${mode:+$( [[ "$mode" == xdp ]] && echo XDP_FAST_DEDUP=1 )} \
    "${PROXY_BIN}" shredstream \
      --block-engine-url "${BE_URL}" \
      --auth-keypair "${AUTH_KEYPAIR}" \
      --desired-regions "${DESIRED_REGIONS}" \
      --src-bind-addr 0.0.0.0 \
      --src-bind-port "${port}" \
      --dest-ip-ports "${SINK_IP}:${SINK_PORT}" \
      --metrics-report-interval-ms "${METRICS_MS}" \
      --packet-transmission-mode "${mode}" \
      ${AUTH_URL:+--auth-url "$AUTH_URL"} \
      ${PUBLIC_IP:+--public-ip "$PUBLIC_IP"} \
      >"${log}" 2>&1 || true
  read -r txp1 txb1 < <(read_tx)

  # Parse totals from logs (recv + dup), then override forward metrics with NIC TX deltas for BOTH modes
  read -r recv_pps fwd_pps eff dup_rate total_recv total_fwd < <(parse_totals "$log")
  local nic_tx_pkts=$(( txp1 - txp0 )); (( nic_tx_pkts<0 )) && nic_tx_pkts=$txp1
  local nic_tx_bytes=$(( txb1 - txb0 )); (( nic_tx_bytes<0 )) && nic_tx_bytes=$txb1
  total_fwd="$nic_tx_pkts"
  fwd_pps=$(( DURATION_SEC>0 ? nic_tx_pkts / DURATION_SEC : 0 ))
  if [[ ${total_recv:-0} -gt 0 ]]; then eff=$(( total_fwd * 100 / total_recv )); else eff=0; fi
  printf "%s %s %s %s %s %s %s %s\n" "$recv_pps" "$fwd_pps" "$eff" "$dup_rate" "$total_recv" "$total_fwd" "$nic_tx_pkts" "$nic_tx_bytes"

  # Clean up bench-specific networking changes
  cleanup_scoped_route
  if [[ "$mode" == "xdp" ]]; then
    cleanup_temp_if_addr
  fi
}

echo "[INFO] Duration=${DURATION_SEC}s  Sink=${SINK_IP}:${SINK_PORT}" >&2
echo "[INFO] BE=${BE_URL}  Regions=${DESIRED_REGIONS}  Keypair=${AUTH_KEYPAIR}" >&2
echo "[INFO] IF_TX=${IF_TX} (tx_counters_dev=$(effective_tx_dev))  ENABLE_SCOPED_ROUTE=${ENABLE_SCOPED_ROUTE}  ENABLE_IF_ADDR=${ENABLE_IF_ADDR}" >&2

read -r udp_recv_pps udp_fwd_pps udp_eff udp_dup udp_recv udp_fwd udp_nic_pkts udp_nic_bytes < <(run_mode udp "$UDP_PORT")
read -r xdp_recv_pps xdp_fwd_pps xdp_eff xdp_dup xdp_recv xdp_fwd xdp_nic_pkts xdp_nic_bytes < <(run_mode xdp "$XDP_PORT")

printf "MODE   | %12s | %12s | %11s | %10s | %12s | %12s | %12s | %12s\n" \
  "RECV_PPS" "FWD_PPS(NIC)" "EFFICIENCY%" "DUP_RATE%" "TOTAL_RECV" "TOTAL_FWD(NIC)" "NIC_TXpkts" "NIC_TXbytes"
printf -- "-------+--------------+--------------+-------------+------------+--------------+--------------+--------------\n"
printf "%-6s | %12s | %12s | %10s%% | %9s%% | %12s | %12s | %12s | %12s\n" \
  "UDP" "$udp_recv_pps" "$udp_fwd_pps" "$udp_eff" "$udp_dup" "$udp_recv" "$udp_fwd" "$udp_nic_pkts" "$udp_nic_bytes"
printf "%-6s | %12s | %12s | %10s%% | %9s%% | %12s | %12s | %12s | %12s\n" \
  "XDP" "$xdp_recv_pps" "$xdp_fwd_pps" "$xdp_eff" "$xdp_dup" "$xdp_recv" "$xdp_fwd" "$xdp_nic_pkts" "$xdp_nic_bytes"


