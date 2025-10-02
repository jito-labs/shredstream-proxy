#!/usr/bin/env bash
set -euo pipefail

# ---------- config ----------
PARALLEL_INSTANCES="${PARALLEL_INSTANCES:-4}"      # Number of parallel proxy instances per mode
DURATION_SEC="${DURATION_SEC:-30}"                 # Test duration per phase
BLASTER_INTERVAL_MS="${BLASTER_INTERVAL_MS:-100}"  # Faster blasting: 10x per second
UDP_BASE_PORT="${UDP_BASE_PORT:-20001}"           # Base port for UDP instances
XDP_BASE_PORT="${XDP_BASE_PORT:-20000}"           # Base port for XDP instances
SINK_BASE_PORT="${SINK_BASE_PORT:-30000}"         # Base port for sink servers
BIN="${BIN:-./run_proxy.sh}"                       # Proxy launcher script

# Hardware configuration
IF="${IF:-enp1s0f1}"                               # Network interface for measurements
SINK_IP="${SINK_IP:-198.18.0.1}"                   # Sink IP for forwarded packets

# ---------- helpers ----------
kill_all() {
  echo "Cleaning up previous processes..." >&2
  pkill -f 'jito-shredstream-proxy' >/dev/null 2>&1 || true
  pkill -f 'nc -u -l' >/dev/null 2>&1 || true
  pkill -f 'socat' >/dev/null 2>&1 || true
  sleep 2
}

# Create a simple UDP sink that counts packets received
start_sink() {
  local port="$1" log_file="$2" instance_id="$3"
  echo "[SINK-$instance_id] Starting sink on port $port" >&2

  # Use socat for reliable high-speed UDP reception with packet counting
  timeout $((DURATION_SEC + 5)) socat -u UDP-LISTEN:$port,fork STDOUT |
    awk -v port="$port" -v instance="$instance_id" '
      BEGIN {
        packet_count = 0;
        start_time = systime();
        printf "[SINK-%s] Started on port %s at %s\n", instance, port, strftime("%H:%M:%S", start_time) > "/dev/stderr"
      }
      {
        packet_count++;
        if (packet_count % 10000 == 0) {
          elapsed = systime() - start_time;
          pps = packet_count / elapsed;
          printf "[SINK-%s] Received %d packets, %.0f pps\n", instance, packet_count, pps > "/dev/stderr"
        }
      }
      END {
        elapsed = systime() - start_time;
        if (elapsed > 0) {
          final_pps = packet_count / elapsed;
          printf "[SINK-%s] Final: %d packets in %.1fs = %.0f pps\n", instance, packet_count, elapsed, final_pps > "/dev/stderr"
        }
        printf "%d %.1f\n", packet_count, final_pps
      }
    ' > "$log_file" &
  echo $!
}

# Start a proxy instance with blaster
start_proxy_instance() {
  local mode="$1" port="$2" sink_port="$3" instance_id="$4" log_file="$5"

  echo "[PROXY-$instance_id] Starting $mode proxy on port $port -> sink:$sink_port" >&2

  SRC_BIND_ADDR="0.0.0.0" \
  SRC_BIND_PORT="$port" \
  DEST_IP_PORTS="${SINK_IP}:$sink_port" \
  PACKET_MODE="$mode" \
  BLASTER_INTERVAL_MS="$BLASTER_INTERVAL_MS" \
    timeout $((DURATION_SEC + 10)) "${BIN}" forward-only \
      --blaster \
      --blaster-interval-ms "$BLASTER_INTERVAL_MS" \
      >"$log_file" 2>&1 &
  echo $!
}

# Run parallel test phase
run_parallel_phase() {
  local mode="$1" base_port="$2" sink_base_port="$3"
  local instance_pids=()
  local sink_pids=()
  local log_files=()
  local sink_log_files=()

  echo "=== Starting $mode phase with $PARALLEL_INSTANCES parallel instances ===" >&2

  # Start all sink servers first
  for ((i=0; i<PARALLEL_INSTANCES; i++)); do
    local sink_port=$((sink_base_port + i))
    local sink_log="/tmp/sink_${mode}_${i}.log"
    sink_log_files+=("$sink_log")

    local sink_pid
    sink_pid=$(start_sink "$sink_port" "$sink_log" "${mode}-${i}")
    sink_pids+=("$sink_pid")
    echo "Started sink $i (PID: $sink_pid) on port $sink_port" >&2
  done

  sleep 2  # Let sinks stabilize

  # Start all proxy instances
  for ((i=0; i<PARALLEL_INSTANCES; i++)); do
    local proxy_port=$((base_port + i))
    local sink_port=$((sink_base_port + i))
    local proxy_log="/tmp/proxy_${mode}_${i}.log"
    log_files+=("$proxy_log")

    local proxy_pid
    proxy_pid=$(start_proxy_instance "$mode" "$proxy_port" "$sink_port" "${mode}-${i}" "$proxy_log")
    instance_pids+=("$proxy_pid")
    echo "Started proxy $i (PID: $proxy_pid) on port $proxy_port" >&2
  done

  echo "All $mode instances running. Testing for $DURATION_SEC seconds..." >&2

  # Wait for test duration
  sleep "$DURATION_SEC"

  echo "Stopping $mode instances..." >&2

  # Stop all proxies
  for pid in "${instance_pids[@]}"; do
    kill "$pid" >/dev/null 2>&1 || true
  done

  # Stop all sinks
  for pid in "${sink_pids[@]}"; do
    kill "$pid" >/dev/null 2>&1 || true
  done

  # Wait for all processes to finish
  for pid in "${instance_pids[@]}" "${sink_pids[@]}"; do
    wait "$pid" 2>/dev/null || true
  done

  sleep 2  # Allow final packets to be processed

  # Collect results
  local total_packets=0
  local total_pps=0.0
  local instance_count=0

  echo "=== $mode Results ===" >&2
  for ((i=0; i<PARALLEL_INSTANCES; i++)); do
    local sink_log="${sink_log_files[$i]}"

    if [[ -f "$sink_log" ]]; then
      # Read the final line from sink log (format: "packet_count final_pps")
      local last_line
      last_line=$(tail -n 1 "$sink_log" 2>/dev/null | grep -E '^[0-9]+ [0-9.]+$') || true

      if [[ -n "$last_line" ]]; then
        local packets pps
        read -r packets pps <<< "$last_line"
        echo "Instance $i: $packets packets, $pps pps" >&2
        total_packets=$((total_packets + packets))
        total_pps=$(awk -v total="$total_pps" -v pps="$pps" 'BEGIN{printf "%.1f", total + pps}')
        ((instance_count++))
      else
        echo "Instance $i: No data received" >&2
      fi
    else
      echo "Instance $i: Log file missing" >&2
    fi
  done

  # Calculate aggregate metrics
  local avg_pps_per_instance=0.0
  if [[ $instance_count -gt 0 ]]; then
    avg_pps_per_instance=$(awk -v total="$total_pps" -v count="$instance_count" 'BEGIN{printf "%.1f", total / count}')
  fi

  local total_theoretical_pps=0
  if [[ $BLASTER_INTERVAL_MS -gt 0 ]]; then
    # Each blaster sends 100,000 packets per interval
    local packets_per_interval=100000
    local intervals_per_second=$((1000 / BLASTER_INTERVAL_MS))
    total_theoretical_pps=$((packets_per_interval * intervals_per_second * PARALLEL_INSTANCES))
  fi

  local efficiency=0.0
  if [[ $total_theoretical_pps -gt 0 ]]; then
    efficiency=$(awk -v actual="$total_pps" -v theoretical="$total_theoretical_pps" 'BEGIN{printf "%.1f", (actual / theoretical) * 100}')
  fi

  echo "$mode: $total_packets $total_pps $avg_pps_per_instance $total_theoretical_pps $efficiency"
}

# ---------- main ----------
echo "=== Parallel UDP vs XDP Transmission Rate Benchmark ==="
echo "Parallel instances per mode: $PARALLEL_INSTANCES"
echo "Test duration: ${DURATION_SEC}s"
echo "Blaster interval: ${BLASTER_INTERVAL_MS}ms"
echo "Network interface: $IF"
echo "Sink IP: $SINK_IP"
echo ""

# Clean up any existing processes
kill_all

# Configure network for testing
echo "Configuring network routing..."
ip route replace "${SINK_IP}/32" dev "${IF}" proto static scope link 2>/dev/null || true
MAC="$(ip -o link show "$IF" 2>/dev/null | awk '{for(i=1;i<=NF;i++){if($i=="link/ether"){print $(i+1); exit}}}' || echo "00:00:00:00:00:00")"
if [[ "$MAC" != "00:00:00:00:00:00" ]]; then
  ip neigh replace "${SINK_IP}" lladdr "${MAC}" nud permanent dev "${IF}" 2>/dev/null || true
fi

echo "Starting UDP phase..."
udp_results=$(run_parallel_phase "udp" "$UDP_BASE_PORT" "$SINK_BASE_PORT")

echo ""
echo "Starting XDP phase..."
xdp_results=$(run_parallel_phase "xdp" "$XDP_BASE_PORT" "$((SINK_BASE_PORT + PARALLEL_INSTANCES))")

echo ""
echo "=== FINAL COMPARISON ==="

# Parse results
read -r udp_mode udp_total udp_pps udp_avg udp_theoretical udp_eff <<< "$udp_results"
read -r xdp_mode xdp_total xdp_pps xdp_avg xdp_theoretical xdp_eff <<< "$xdp_results"

# Print comparison table
printf "%-6s | %12s | %12s | %12s | %15s | %12s\n" "MODE" "TOTAL_PKTS" "TOTAL_PPS" "AVG_PPS/INST" "THEORETICAL_PPS" "EFFICIENCY%"
printf -- "-------+--------------+--------------+--------------+-----------------+-------------\n"
printf "%-6s | %12s | %12s | %12s | %15s | %11s%%\n" "UDP" "$udp_total" "$udp_pps" "$udp_avg" "$udp_theoretical" "$udp_eff"
printf "%-6s | %12s | %12s | %12s | %15s | %11s%%\n" "XDP" "$xdp_total" "$xdp_pps" "$xdp_avg" "$xdp_theoretical" "$xdp_eff"

echo ""
echo "=== Analysis ==="
echo "• Total PPS: Total packets per second across all parallel instances"
echo "• Avg PPS/Inst: Average packets per second per individual instance"
echo "• Theoretical PPS: Expected rate based on blaster configuration"
echo "• Efficiency: Actual vs theoretical performance percentage"
echo ""
echo "Higher efficiency indicates better performance."
echo "XDP should generally outperform UDP due to kernel bypass."

# Final cleanup
kill_all
echo "Benchmark complete!"
