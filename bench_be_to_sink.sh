#!/usr/bin/env bash
set -euo pipefail

# -------- config --------
DURATION_SEC="${DURATION_SEC:-15}"
MODE="${MODE:-both}"               # xdp | udp | both

PROXY_BIN="${PROXY_BIN:-./target/release/jito-shredstream-proxy}"
SRC_BIND_ADDR="${SRC_BIND_ADDR:-0.0.0.0}"
# Port is auto-chosen per mode if not specified: xdp→20000, udp→20001
SRC_BIND_PORT="${SRC_BIND_PORT:-}"
METRICS_MS="${METRICS_MS:-2000}"

SINK_IP="${SINK_IP:-127.0.0.1}"
SINK_PORT="${SINK_PORT:-8001}"

# Shredstream (Jito Block Engine) config
BE_URL="${BE_URL:-https://ny.mainnet.block-engine.jito.wtf}"
AUTH_KEYPAIR="${AUTH_KEYPAIR:-/root/.config/solana/id.json}"
DESIRED_REGIONS="${DESIRED_REGIONS:-ny}"
# Optional overrides
AUTH_URL="${AUTH_URL:-}"
PUBLIC_IP="${PUBLIC_IP:-}"

[[ -x "${PROXY_BIN}" ]] || { echo "[ERR] PROXY_BIN not executable: ${PROXY_BIN}" >&2; exit 1; }

kill_all() { pkill -f 'jito-shredstream-proxy shredstream' >/dev/null 2>&1 || true; }

pre_detach_xdp() {
  ip link set dev "${IF_TX:-enp1s0f1}" xdp off 2>/dev/null || true
  ip link set dev "${IF_TX:-enp1s0f1}" xdpgeneric off 2>/dev/null || true
}

parse_final_metrics() {
  # Input: logfile; Output: six fields: recv_pps fwd_pps efficiency dup_rate total_recv total_fwd
  local log="$1"
  local final_line
  final_line=$(grep "Exiting Shredstream" "$log" | tail -1 || true)
  if [[ -n "$final_line" ]]; then
    local received forwarded duplicates
    received=$(echo "$final_line" | grep -o '[0-9]* received' | grep -o '[0-9]*' || echo 0)
    forwarded=$(echo "$final_line" | grep -o '[0-9]* sent successfully' | grep -o '[0-9]*' || echo 0)
    duplicates=$(echo "$final_line" | grep -o '[0-9]* duplicate' | grep -o '[0-9]*' || echo 0)

    received=${received:-0}
    forwarded=${forwarded:-0}
    duplicates=${duplicates:-0}

    local avg_recv_pps avg_fwd_pps efficiency dup_rate
    if [[ ${DURATION_SEC} -gt 0 ]]; then
      avg_recv_pps=$((received / DURATION_SEC))
      avg_fwd_pps=$((forwarded / DURATION_SEC))
    else
      avg_recv_pps=0; avg_fwd_pps=0
    fi
    if [[ $received -gt 0 ]]; then
      efficiency=$((forwarded * 100 / received))
      dup_rate=$((duplicates * 100 / received))
    else
      efficiency=0; dup_rate=0
    fi
    printf "%s %s %s %s %s %s\n" "$avg_recv_pps" "$avg_fwd_pps" "$efficiency" "$dup_rate" "$received" "$forwarded"
  else
    printf "0 0 0 0 0 0\n"
  fi
}

run_one_mode() {
  # args: mode(xdp|udp)
  local mode="$1"
  local port="$SRC_BIND_PORT"
  if [[ -z "$port" ]]; then
    if [[ "$mode" == "udp" ]]; then port=20001; else port=20000; fi
  fi

  local log="/tmp/shred_be_${mode}.log"
  kill_all

  echo "[${mode^^}] Connecting to Block Engine and forwarding to sink=${SINK_IP}:${SINK_PORT} (dur=${DURATION_SEC}s)" >&2

  # Launch proxy in shredstream mode without any NIC changes
  if [[ "$mode" == "xdp" ]]; then pre_detach_xdp; fi
  # Note: src-bind-* are unused for inbound generation in this mode but kept consistent
  timeout "${DURATION_SEC}" \
    env RUST_LOG=info \
    "${PROXY_BIN}" shredstream \
      --block-engine-url "${BE_URL}" \
      --auth-keypair "${AUTH_KEYPAIR}" \
      --desired-regions "${DESIRED_REGIONS}" \
      --src-bind-addr "${SRC_BIND_ADDR}" \
      --src-bind-port "${port}" \
      --dest-ip-ports "${SINK_IP}:${SINK_PORT}" \
      --metrics-report-interval-ms "${METRICS_MS}" \
      --packet-transmission-mode "${mode}" \
      ${AUTH_URL:+--auth-url "$AUTH_URL"} \
      ${PUBLIC_IP:+--public-ip "$PUBLIC_IP"} \
      >"${log}" 2>&1 || true

  parse_final_metrics "${log}"
}

echo "[INFO] Duration: ${DURATION_SEC}s | Sink: ${SINK_IP}:${SINK_PORT}" >&2
echo "[INFO] Block Engine: ${BE_URL} | Regions: ${DESIRED_REGIONS} | Keypair: ${AUTH_KEYPAIR}" >&2

case "${MODE}" in
  xdp)
    read -r xdp_recv_pps xdp_fwd_pps xdp_efficiency xdp_dup_rate xdp_total_recv xdp_total_fwd < <(run_one_mode xdp)
    printf "MODE   | %12s | %12s | %11s | %10s | %12s | %12s\n" "RECV_PPS" "FWD_PPS" "EFFICIENCY%" "DUP_RATE%" "TOTAL_RECV" "TOTAL_FWD"
    printf -- "-------+--------------+--------------+-------------+------------+--------------+--------------\n"
    printf "%-6s | %12s | %12s | %10s%% | %9s%% | %12s | %12s\n" "XDP" "$xdp_recv_pps" "$xdp_fwd_pps" "$xdp_efficiency" "$xdp_dup_rate" "$xdp_total_recv" "$xdp_total_fwd"
    ;;
  udp)
    read -r udp_recv_pps udp_fwd_pps udp_efficiency udp_dup_rate udp_total_recv udp_total_fwd < <(run_one_mode udp)
    printf "MODE   | %12s | %12s | %11s | %10s | %12s | %12s\n" "RECV_PPS" "FWD_PPS" "EFFICIENCY%" "DUP_RATE%" "TOTAL_RECV" "TOTAL_FWD"
    printf -- "-------+--------------+--------------+-------------+------------+--------------+--------------\n"
    printf "%-6s | %12s | %12s | %10s%% | %9s%% | %12s | %12s\n" "UDP" "$udp_recv_pps" "$udp_fwd_pps" "$udp_efficiency" "$udp_dup_rate" "$udp_total_recv" "$udp_total_fwd"
    ;;
  both)
    read -r udp_recv_pps udp_fwd_pps udp_efficiency udp_dup_rate udp_total_recv udp_total_fwd < <(run_one_mode udp)
    read -r xdp_recv_pps xdp_fwd_pps xdp_efficiency xdp_dup_rate xdp_total_recv xdp_total_fwd < <(run_one_mode xdp)
    printf "MODE   | %12s | %12s | %11s | %10s | %12s | %12s\n" "RECV_PPS" "FWD_PPS" "EFFICIENCY%" "DUP_RATE%" "TOTAL_RECV" "TOTAL_FWD"
    printf -- "-------+--------------+--------------+-------------+------------+--------------+--------------\n"
    printf "%-6s | %12s | %12s | %10s%% | %9s%% | %12s | %12s\n" "UDP" "$udp_recv_pps" "$udp_fwd_pps" "$udp_efficiency" "$udp_dup_rate" "$udp_total_recv" "$udp_total_fwd"
    printf "%-6s | %12s | %12s | %10s%% | %9s%% | %12s | %12s\n" "XDP" "$xdp_recv_pps" "$xdp_fwd_pps" "$xdp_efficiency" "$xdp_dup_rate" "$xdp_total_recv" "$xdp_total_fwd"
    ;;
  *)
    echo "[ERR] MODE must be one of: xdp | udp | both" >&2; exit 1 ;;
esac

kill_all


