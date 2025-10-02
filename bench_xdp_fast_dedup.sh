#!/usr/bin/env bash
set -euo pipefail

# -------- CONFIG --------
IF_SRC="${IF_SRC:-enp1s0f0}"           # ingress to proxy (localhost blaster)
IF_TX="${IF_TX:-enp1s0f1}"             # egress from proxy → sink
PROXY_BIN="${PROXY_BIN:-./target/release/jito-shredstream-proxy}"
BLASTER_BIN="${BLASTER_BIN:-./target/release/udp-blaster}"
DURATION_SEC="${DURATION_SEC:-15}"

UDP_PORT="${UDP_PORT:-20001}"
XDP_PORT="${XDP_PORT:-20000}"
SINK_IP="${SINK_IP:-198.18.0.1}"
SINK_PORT="${SINK_PORT:-9}"
SINK_MAC="${SINK_MAC:-}"

# -------- sanity --------
[[ $EUID -eq 0 ]] || { echo "[ERR] run as root"; exit 1; }
ip link show "$IF_TX" >/dev/null 2>&1 || { echo "[ERR] IF_TX $IF_TX not found"; exit 1; }
[[ -x "$PROXY_BIN" ]] || { echo "[ERR] PROXY_BIN ($PROXY_BIN) not executable"; exit 1; }

kill_all(){ pkill -f 'jito-shredstream-proxy' >/dev/null 2>&1 || true; }
cleanup(){ kill_all; ip addr del 198.18.0.2/31 dev "$IF_TX" 2>/dev/null || true; }
trap cleanup EXIT

# -------- helpers --------
read_tx(){ local p b; p="$(<"/sys/class/net/$IF_TX/statistics/tx_packets")"; b="$(<"/sys/class/net/$IF_TX/statistics/tx_bytes")"; echo "$p $b"; }
compute_pps_mbps(){ local pkts="$1" secs="$2" payload="$3"; local pps mbps; pps="$(awk -v pk="$pkts" -v s="$secs" 'BEGIN{printf "%.2f", (s>0?pk/s:0)}')"; mbps="$(awk -v pk="$pkts" -v s="$secs" -v pl="$payload" 'BEGIN{printf "%.2f", (s>0? (8*pk*(pl+42))/(1e6*s) : 0)}')"; echo "$pps $mbps"; }

parse_proxy_totals(){
  local log="$1" rx fwd ssum dup
  rx=$(grep -oE 'packets_count=[0-9]+' "$log" | cut -d= -f2 | awk '{sum+=$1} END {print sum+0}' 2>/dev/null || echo 0)
  ssum=$(grep -oE 'success_forward=[0-9]+' "$log" | cut -d= -f2 | awk '{sum+=$1} END {print sum+0}' 2>/dev/null || echo 0)
  dup=$(grep -oE 'duplicate=[0-9]+' "$log" | cut -d= -f2 | awk '{sum+=$1} END {print sum+0}' 2>/dev/null || echo 0)
  if [[ -n "$ssum" && "$ssum" != "0" ]]; then fwd="$ssum"; else fwd="$rx"; fi
  echo "${rx:-0} ${fwd:-0} ${dup:-0}"
}

start_forwarder(){
  local mode="$1" port="$2" log="$3"
  if [[ "$mode" == "xdp_fast_dedup" ]]; then
    XDP_FAST_DEDUP=1 XDP_SINGLE_QUEUE=1 RUST_LOG=info "$PROXY_BIN" forward-only \
      --src-bind-addr 0.0.0.0 \
      --src-bind-port "$port" \
      --dest-ip-ports "${SINK_IP}:${SINK_PORT}" \
      --packet-transmission-mode xdp \
      >"$log" 2>&1 &
  else
    RUST_LOG=info "$PROXY_BIN" forward-only \
      --src-bind-addr 0.0.0.0 \
      --src-bind-port "$port" \
      --dest-ip-ports "${SINK_IP}:${SINK_PORT}" \
      --packet-transmission-mode "$mode" \
      >"$log" 2>&1 &
  fi
  echo $!
}

start_blaster(){ local port="$1" log="$2"; "$BLASTER_BIN" --dest "127.0.0.1:${port}" --threads 4 --duration "$((DURATION_SEC+2))" --pool-pkts 1000000 >"$log" 2>&1 & echo $!; }
ensure_alive(){ local pid="$1" log="$2" tag="$3"; kill -0 "$pid" 2>/dev/null || { echo "[ERR] $tag died"; tail -n 80 "$log" || true; exit 1; }; }

run_phase(){
  local mode="$1" port="$2" label="$3"
  kill_all

  # Ensure egress NIC is up and routed to sink; add temp IPv4 for XDP L3 source crafting
  ip link set "$IF_TX" up 2>/dev/null || true
  ip addr add 198.18.0.2/31 dev "$IF_TX" 2>/dev/null || true
  ip route replace ${SINK_IP}/32 dev "$IF_TX" proto static scope link 2>/dev/null || true
  [[ -n "$SINK_MAC" ]] && ip neigh replace "$SINK_IP" lladdr "$SINK_MAC" nud permanent dev "$IF_TX" 2>/dev/null || true

  local fwd_log="/tmp/bench_${mode}.log"; local bl_log="/tmp/bench_blaster_${mode}.log"
  local fpid; fpid="$(start_forwarder "$mode" "$port" "$fwd_log")"; sleep 1; ensure_alive "$fpid" "$fwd_log" "forwarder($mode)"
  local bpid; bpid="$(start_blaster "$port" "$bl_log")"; sleep 1; ensure_alive "$bpid" "$bl_log" "blaster"

  local rx0 fwd0 dup0; read -r rx0 fwd0 dup0 < <(parse_proxy_totals "$fwd_log")
  local txp0 txb0; read -r txp0 txb0 < <(read_tx)
  sleep "$DURATION_SEC"
  local rx1 fwd1 dup1; read -r rx1 fwd1 dup1 < <(parse_proxy_totals "$fwd_log")
  local txp1 txb1; read -r txp1 txb1 < <(read_tx)

  local prx=$(( rx1 - rx0 )); (( prx<0 )) && prx=$rx1
  local pfw=$(( fwd1 - fwd0 )); (( pfw<0 )) && pfw=$fwd1
  local pdup=$(( dup1 - dup0 )); (( pdup<0 )) && pdup=$dup1
  local txp=$(( txp1 - txp0 )); (( txp<0 )) && txp=$txp1
  local txb=$(( txb1 - txb0 )); (( txb<0 )) && txb=$txb1

  # Compute PPS/Mbps base from NIC TX
  local base_pkts="$txp"
  local pps mbps; read -r pps mbps < <(compute_pps_mbps "$base_pkts" "$DURATION_SEC" 1200)

  kill "$bpid" "$fpid" >/dev/null 2>&1 || true; wait "$bpid" "$fpid" 2>/dev/null || true
  # Forwarded and dropped (human-friendly)
  local fwd_pkts drop_pkts
  if [[ "$mode" == "udp" ]]; then
    fwd_pkts="${pfw:-0}"  # proxy success_forward reflects UDP sendmmsg success
  else
    fwd_pkts="$txp"       # for XDP, rely on NIC TX (fast-path may not bump success_forward)
  fi
  drop_pkts=$(( prx - fwd_pkts )); (( drop_pkts<0 )) && drop_pkts=0

  printf "%s %s %s %s %s %s %s %s %s\n" "$label" "$pps" "$mbps" "$txp" "$txb" "$prx" "$fwd_pkts" "$drop_pkts" "$pdup"
}

echo "[INFO] IF_SRC=$IF_SRC IF_TX=$IF_TX  DURATION_SEC=$DURATION_SEC" >&2
echo "[INFO] UDP_PORT=$UDP_PORT  XDP_PORT=$XDP_PORT" >&2
echo "[INFO] SINK=${SINK_IP}:${SINK_PORT}" >&2

# UDP (baseline)
read -r udp_label udp_pps udp_mbps udp_tx_pkts udp_tx_bytes udp_proxy_rx udp_fwd udp_drop udp_dedup < <(run_phase udp "$UDP_PORT" UDP)

# XDP fast dedup (batch-level enqueue + dedup)
read -r xdp_label xdp_pps xdp_mbps xdp_tx_pkts xdp_tx_bytes xdp_proxy_rx xdp_fwd xdp_drop xdp_dedup < <(run_phase xdp_fast_dedup "$XDP_PORT" XDP)

printf "% -14s | %12s | %10s | %12s | %12s | %10s | %12s | %12s | %12s\n" \
  "MODE" "AVG_PPS" "AVG_Mbps" "NIC_TXpkts" "NIC_TXbytes" "Packets_Received" "Packets_Forwarded" "Packets_Dropped" "Packets_Deduped"
printf -- "----------------+--------------+-----------+--------------+--------------+------------+--------------+--------------+--------------\n"
printf "% -14s | %12s | %10s | %12s | %12s | %10s | %12s | %12s | %12s\n" "$udp_label" "$udp_pps" "$udp_mbps" "$udp_tx_pkts" "$udp_tx_bytes" "$udp_proxy_rx" "$udp_fwd" "$udp_drop" "$udp_dedup"
printf "% -14s | %12s | %10s | %12s | %12s | %10s | %12s | %12s | %12s\n" "$xdp_label" "$xdp_pps" "$xdp_mbps" "$xdp_tx_pkts" "$xdp_tx_bytes" "$xdp_proxy_rx" "$xdp_fwd" "$xdp_drop" "$xdp_dedup"

echo ""
echo "=== TOTAL PACKETS SUMMARY ==="
echo "UDP Mode:"
echo "  - Total Packets Received: $udp_proxy_rx"
echo "  - Total Packets Forwarded: $udp_fwd"
echo "  - Total Packets Dropped: $udp_drop"
echo "  - Total Packets Deduped: $udp_dedup"
echo ""
echo "XDP Mode (Single Queue):"
echo "  - Total Packets Received: $xdp_proxy_rx"
echo "  - Total Packets Forwarded: $xdp_fwd"
echo "  - Total Packets Dropped: $xdp_drop"
echo "  - Total Packets Deduped: $xdp_dedup"
echo ""
echo "=== PERFORMANCE COMPARISON ==="
echo "XDP is $(awk -v xdp="$xdp_pps" -v udp="$udp_pps" 'BEGIN{printf "%.1f", (udp>0?xdp/udp:0)}')x faster than UDP in PPS"
echo "XDP forwarded $(awk -v xdp="$xdp_fwd" -v udp="$udp_fwd" 'BEGIN{printf "%.1f", (udp>0?xdp/udp:0)}')x more packets than UDP"


