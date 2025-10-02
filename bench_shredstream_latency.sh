#!/usr/bin/env bash
set -euo pipefail

# ---------- config ----------
DURATION_SEC="${DURATION_SEC:-15}"
UDP_PORT="${UDP_PORT:-20001}"
XDP_PORT="${XDP_PORT:-20000}"
SINK_IP="${SINK_IP:-198.18.0.1}"
SINK_PORT="${SINK_PORT:-9}"

# Shredstream config
BE_URL="${BE_URL:-https://ny.mainnet.block-engine.jito.wtf}"
AUTH_KEYPAIR="${AUTH_KEYPAIR:-/root/.config/solana/id.json}"
DESIRED_REGIONS="${DESIRED_REGIONS:-ny}"

kill_all() {
  pkill -f 'jito-shredstream-proxy' >/dev/null 2>&1 || true
}

run_shredstream_test() {
  local mode="$1" port="$2"
  local log="/tmp/shred_${mode}.log"
  
  echo "[${mode^^} phase] Receiving mainnet shreds, forwarding to sink=${SINK_IP}:${SINK_PORT}" >&2
  
  kill_all
  sleep 1
  
  # Run shredstream mode
  DEST_IP_PORTS="${SINK_IP}:${SINK_PORT}" \
  BE_URL="${BE_URL}" \
  AUTH_KEYPAIR="${AUTH_KEYPAIR}" \
  DESIRED_REGIONS="${DESIRED_REGIONS}" \
    timeout $DURATION_SEC ./run_proxy.sh shredstream -p "${mode}" >"${log}" 2>&1 &
  local proxy_pid=$!
  
  # Wait for completion
  sleep $DURATION_SEC
  kill $proxy_pid >/dev/null 2>&1 || true
  wait $proxy_pid 2>/dev/null || true
  
  # Extract final metrics from log
  local final_line
  final_line=$(grep "Exiting Shredstream" "$log" | tail -1)
  
  echo "Debug: Final line: $final_line" >&2
  
  if [[ -n "$final_line" ]]; then
    # Parse: "Exiting Shredstream, X received , Y sent successfully, Z failed, W duplicate shreds."
    local received=$(echo "$final_line" | grep -o '[0-9]* received' | grep -o '[0-9]*')
    local forwarded=$(echo "$final_line" | grep -o '[0-9]* sent successfully' | grep -o '[0-9]*')
    local duplicates=$(echo "$final_line" | grep -o '[0-9]* duplicate' | grep -o '[0-9]*')
    
    # Defaults if parsing fails
    received=${received:-0}
    forwarded=${forwarded:-0}
    duplicates=${duplicates:-0}
    
    # Calculate metrics
    local avg_recv_pps=$((received / DURATION_SEC))
    local avg_fwd_pps=$((forwarded / DURATION_SEC))
    local efficiency=0
    if [[ $received -gt 0 ]]; then
      efficiency=$((forwarded * 100 / received))
    fi
    local dup_rate=0
    if [[ $received -gt 0 ]]; then
      dup_rate=$((duplicates * 100 / received))
    fi
    
    # Output: recv_pps fwd_pps efficiency% dup_rate% total_recv total_fwd
    printf "%s %s %s %s %s %s\n" "$avg_recv_pps" "$avg_fwd_pps" "$efficiency" "$dup_rate" "$received" "$forwarded"
  else
    echo "0 0 0 0 0 0"
  fi
}

# ---------- main ----------
echo "[INFO] Duration: ${DURATION_SEC}s per test" >&2
echo "[INFO] UDP_PORT=$UDP_PORT  XDP_PORT=$XDP_PORT" >&2
echo "[INFO] NY Block Engine → Sink: ${SINK_IP}:${SINK_PORT}" >&2
echo "[INFO] Receiving REAL mainnet shreds in parallel..." >&2

# UDP phase
read -r udp_recv_pps udp_fwd_pps udp_efficiency udp_dup_rate udp_total_recv udp_total_fwd < <(run_shredstream_test udp "$UDP_PORT")

# XDP phase  
read -r xdp_recv_pps xdp_fwd_pps xdp_efficiency xdp_dup_rate xdp_total_recv xdp_total_fwd < <(run_shredstream_test xdp "$XDP_PORT")

# Final comparison table
printf "MODE   | %12s | %12s | %11s | %10s | %12s | %12s\n" "RECV_PPS" "FWD_PPS" "EFFICIENCY%" "DUP_RATE%" "TOTAL_RECV" "TOTAL_FWD"
printf -- "-------+--------------+--------------+-------------+------------+--------------+--------------\n"
printf "%-6s | %12s | %12s | %10s%% | %9s%% | %12s | %12s\n" "UDP" "$udp_recv_pps" "$udp_fwd_pps" "$udp_efficiency" "$udp_dup_rate" "$udp_total_recv" "$udp_total_fwd"
printf "%-6s | %12s | %12s | %10s%% | %9s%% | %12s | %12s\n" "XDP" "$xdp_recv_pps" "$xdp_fwd_pps" "$xdp_efficiency" "$xdp_dup_rate" "$xdp_total_recv" "$xdp_total_fwd"

echo ""
echo "Lower DUP_RATE% = Lower latency (less deduplication overhead)"
echo "Higher EFFICIENCY% = Better performance (more packets forwarded per received)"

kill_all
