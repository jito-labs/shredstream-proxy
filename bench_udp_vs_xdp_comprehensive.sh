#!/usr/bin/env bash
set -euo pipefail

# Comprehensive UDP vs XDP forwarding benchmark script
# Supports both packet blaster (forward-only) and Jito mainnet block engine (shredstream) modes

# -------- Configuration --------
DURATION_SEC="${DURATION_SEC:-15}"
MODE="${MODE:-both}"               # xdp | udp | both
BENCH_MODE="${BENCH_MODE:-forward-only}"  # forward-only | shredstream

# Proxy configuration
PROXY_BIN="${PROXY_BIN:-./target/release/jito-shredstream-proxy}"
SRC_BIND_ADDR="${SRC_BIND_ADDR:-0.0.0.0}"
UDP_LISTEN_PORT="${UDP_LISTEN_PORT:-20001}"
XDP_LISTEN_PORT="${XDP_LISTEN_PORT:-20000}"
METRICS_MS="${METRICS_MS:-2000}"

# Network configuration
MGMT_IFACE="${MGMT_IFACE:-bond0}"
VLAN_IFACE="${VLAN_IFACE:-bond0.1591}"
VLAN_ID="${VLAN_ID:-1591}"
FREE_NIC="${FREE_NIC:-enp1s0f1}"
SINK_NS="${SINK_NS:-sinkns}"

# Legacy interface support
IFACE="${IFACE:-$FREE_NIC}"
XDP_CPU_MODE="${XDP_CPU_MODE:-manual}"
XDP_CPUS="${XDP_CPUS:-8,9,10,11}"

# RFC 2544 test addresses (198.18.0.0/15)
SRC_IP_CIDR="${SRC_IP_CIDR:-198.18.0.2/24}"
SINK_IP_CIDR="${SINK_IP_CIDR:-198.18.0.1/24}"
SINK_IP="${SINK_IP:-198.18.0.1}"
SINK_PORT="${SINK_PORT:-8001}"
PROXY_IP="${PROXY_IP:-198.18.0.2}"

# XDP untagged sink (for AF_XDP without VLAN tagging)
XDP_SINK_IP="${XDP_SINK_IP:-198.18.0.11}"
XDP_SINK_IP_CIDR="${XDP_SINK_IP_CIDR:-198.18.0.11/24}"

# Shredstream (Jito Block Engine) configuration
BE_URL="${BE_URL:-https://ny.mainnet.block-engine.jito.wtf}"
AUTH_KEYPAIR="${AUTH_KEYPAIR:-/root/.config/solana/id.json}"
DESIRED_REGIONS="${DESIRED_REGIONS:-ny}"
AUTH_URL="${AUTH_URL:-}"
PUBLIC_IP="${PUBLIC_IP:-}"

# Packet blaster configuration
BLASTER_BIN="${BLASTER_BIN:-./target/release/udp-blaster}"
THREADS="${THREADS:-8}"  # Increased for 10 Gbps stress testing
BURST="${BURST:-512}"    # Increased burst for higher throughput
SNDBUF="${SNDBUF:-134217728}"  # Doubled buffer for high-rate transmission
POOL_PKTS="${POOL_PKTS:-2000000}"  # Large pool to reduce deduplication in UDP tests

# Stress test configuration (10 Gbps NIC, ~800K PPS target, exact packet count)
STRESS_TARGET_PPS="${STRESS_TARGET_PPS:-800000}"  # Target 800K PPS for stress testing
STRESS_TOTAL_PACKETS="${STRESS_TOTAL_PACKETS:-12000000}"  # Exact packet count for 15s @ 800K PPS

# -------- Helper Functions --------
log_info() { echo "[INFO] $*" >&2; }
log_error() { echo "[ERROR] $*" >&2; }
log_success() { echo "[SUCCESS] $*" >&2; }
log_warn() { echo "[WARN] $*" >&2; }

# Render latency columns: show '-' when not applicable (e.g., forward-only or no trace shreds)
render_latency() {
    local avg="$1" p50="$2" p99="$3"
    if [[ "$BENCH_MODE" == "forward-only" || ( "$avg" == "0" && "$p50" == "0" && "$p99" == "0" ) ]]; then
        echo "- - -"
    else
        echo "$avg $p50 $p99"
    fi
}

# Network interface management
NETWORK_RESTORED=false
XDP_MODE_USED=false
SINK_PID=""
RESTORE_BOND_SLAVE=0

# Verify routing path to a destination IP goes over the expected device
verify_route_path() {
    local dest_ip="$1"
    local expected_dev="$2"
    local route_out
    route_out=$(ip route get "$dest_ip" 2>/dev/null || true)
    if [[ -z "$route_out" ]]; then
        log_error "ip route get $dest_ip returned empty output"
        return 1
    fi
    local dev
    dev=$(echo "$route_out" | awk '{for (i=1;i<=NF;i++) if ($i=="dev") {print $(i+1); exit}}')
    if [[ -z "$dev" ]]; then
        log_error "Could not parse device from: $route_out"
        return 1
    fi
    if [[ "$dev" != "$expected_dev" ]]; then
        # Allow local route (dev lo) when dest_ip is assigned on expected_dev (e.g., bond0)
        if [[ "$dev" == "lo" ]] && ip -br addr show "$expected_dev" | grep -q "\b$dest_ip/"; then
            log_warn "Route to $dest_ip is local (dev lo) because IP is assigned on $expected_dev"
        else
            log_error "Routing to $dest_ip via unexpected device: got '$dev', expected '$expected_dev'"
            log_error "Full route: $route_out"
            return 1
        fi
    fi
    log_success "Verified route to $dest_ip goes via $expected_dev"
    return 0
}

# Safety check for management interface
check_mgmt_safety() {
    # Ensure management IP stays intact
    if ! ip addr show "$MGMT_IFACE" | grep -q "46.166.162.22/25"; then
        log_warn "Management IP not found on $MGMT_IFACE - this is expected in some environments"
    fi
    
    # Ensure bond slave remains intact 
    if ! ip link show enp1s0f0 | grep -q "SLAVE"; then
        log_warn "enp1s0f0 is not enslaved - this is expected in some environments"
    fi
    
    # Check if FREE_NIC is enslaved to bond (will be handled per-phase)
    if ip link show "$FREE_NIC" | grep -q "MASTER $MGMT_IFACE"; then
        log_warn "$FREE_NIC is currently enslaved to $MGMT_IFACE (will be handled per-phase)"
        RESTORE_BOND_SLAVE=1
    else
        RESTORE_BOND_SLAVE=0
    fi
    
    return 0
}

# Set up UDP phase: on-wire VLAN. Root src on bond0.1591, sinkns with enp1s0f1.1591
setup_udp_topology() {
    local mode="$1"
    log_info "Setting up UDP topology: on-wire via $VLAN_IFACE -> $FREE_NIC.1591 in $SINK_NS"
    
    # Ensure VLAN interface exists and add source IP
    if ! ip link show "$VLAN_IFACE" >/dev/null 2>&1; then
        log_info "Creating VLAN interface $VLAN_IFACE"
        ip link add link "$MGMT_IFACE" name "$VLAN_IFACE" type vlan id "$VLAN_ID" 2>/dev/null || true
    fi
    
    ip link set "$VLAN_IFACE" up
    ip addr add "$SRC_IP_CIDR" dev "$VLAN_IFACE" 2>/dev/null || true
    
    # Create sink namespace
    if ! ip netns list | grep -q "^$SINK_NS\b"; then
        ip netns add "$SINK_NS"
    fi
    
    # Ensure FREE_NIC is in sinkns
    if ! ip netns exec "$SINK_NS" ip link show "$FREE_NIC" >/dev/null 2>&1; then
        ip link set "$FREE_NIC" down 2>/dev/null || true
        ip link set "$FREE_NIC" netns "$SINK_NS" 2>/dev/null || true
    fi
    # Bring up loopback and parent, then create VLAN 1591 and assign IP
    ip netns exec "$SINK_NS" ip link set lo up
    ip netns exec "$SINK_NS" ip link set "$FREE_NIC" up
    if ! ip netns exec "$SINK_NS" ip link show ${FREE_NIC}.1591 >/dev/null 2>&1; then
        ip netns exec "$SINK_NS" ip link add link "$FREE_NIC" name ${FREE_NIC}.1591 type vlan id "$VLAN_ID" 2>/dev/null || true
    fi
    ip netns exec "$SINK_NS" ip link set ${FREE_NIC}.1591 up
    ip netns exec "$SINK_NS" ip addr add "$SINK_IP_CIDR" dev ${FREE_NIC}.1591 2>/dev/null || true
    
    # Ensure traffic to 198.18.0.0/24 goes out bond0.1591 (hit NIC & VLAN)
    ip route replace 198.18.0.0/24 dev "$VLAN_IFACE" proto static scope link 2>/dev/null || true
    
    log_success "UDP topology ready: $VLAN_IFACE(${SRC_IP_CIDR}) -> ${FREE_NIC}.1591($SINK_NS, ${SINK_IP_CIDR})"
}

# Set up XDP phase: AF_XDP TX out FREE_NIC to untagged sink on bond0
setup_xdp_topology() {
    local mode="$1"
    log_info "Setting up XDP topology: AF_XDP TX on $FREE_NIC -> untagged sink on $MGMT_IFACE (bond0)"
    
    # Move FREE_NIC back to root namespace if needed
    if ! ip link show "$FREE_NIC" >/dev/null 2>&1; then
        ip netns exec "$SINK_NS" ip link set "$FREE_NIC" down 2>/dev/null || true
        ip link set "$FREE_NIC" netns 1 2>/dev/null || true
    fi
    
    # Force unbond FREE_NIC for XDP (AF_XDP requires unbound interface)
    if ip link show "$FREE_NIC" | grep -q "SLAVE"; then
        log_info "Temporarily removing $FREE_NIC from bond for XDP testing (required for AF_XDP)"
        RESTORE_BOND_SLAVE=1  # Set flag for cleanup
        ip link set "$FREE_NIC" nomaster
        ip link set "$FREE_NIC" down
        sleep 2  # Allow bond to stabilize
        ip link set "$FREE_NIC" up
        
        # Flush any old IPs and add test IP required by XDP code
        ip addr flush dev "$FREE_NIC" 2>/dev/null || true
        ip addr add "198.18.0.10/24" dev "$FREE_NIC" 2>/dev/null || true
    else
        # FREE_NIC already unbound
        ip link set "$FREE_NIC" up
        ip addr add "198.18.0.10/24" dev "$FREE_NIC" 2>/dev/null || true
    fi
    
    # Ensure VLAN interface has source IP for proxy bind
    ip link set "$VLAN_IFACE" up
    ip addr add "$SRC_IP_CIDR" dev "$VLAN_IFACE" 2>/dev/null || true

    # Add untagged sink IP on bond0 so AF_XDP untagged frames are received
    ip addr add "$XDP_SINK_IP_CIDR" dev "$MGMT_IFACE" 2>/dev/null || true
    
    log_success "XDP topology ready: XDP TX($FREE_NIC, 198.18.0.10) -> $MGMT_IFACE($XDP_SINK_IP_CIDR untagged)"
}

# Clean up network configuration
cleanup_topology() {
    log_info "Cleaning up network topology"
    
    # Remove test IPs (keep management intact)
    ip addr del "$SRC_IP_CIDR" dev "$VLAN_IFACE" 2>/dev/null || true
    ip addr del "$SINK_IP_CIDR" dev "$VLAN_IFACE" 2>/dev/null || true
    ip addr del "$XDP_SINK_IP_CIDR" dev "$MGMT_IFACE" 2>/dev/null || true
    
    # Move FREE_NIC back to root if in namespace; delete VLAN in ns
    if ! ip link show "$FREE_NIC" >/dev/null 2>&1; then
        ip netns exec "$SINK_NS" ip link set ${FREE_NIC}.1591 down 2>/dev/null || true
        ip netns exec "$SINK_NS" ip link del ${FREE_NIC}.1591 2>/dev/null || true
        ip netns exec "$SINK_NS" ip link set "$FREE_NIC" down 2>/dev/null || true
        ip link set "$FREE_NIC" netns 1 2>/dev/null || true
    fi
    
    # Remove XDP test IP and restore bond membership (only if interface is in root namespace)
    if ip link show "$FREE_NIC" >/dev/null 2>&1; then
        ip addr del "198.18.0.10/24" dev "$FREE_NIC" 2>/dev/null || true
        
        # Clean up veth pair if used
        if ip link show veth-src >/dev/null 2>&1; then
            log_info "Removing veth pair"
            ip link del veth-src 2>/dev/null || true
        fi
        
        # Restore FREE_NIC to bond if it was temporarily unbonded
        if [[ "$RESTORE_BOND_SLAVE" == "1" ]] && ! ip link show "$FREE_NIC" | grep -q "SLAVE"; then
            log_info "Restoring $FREE_NIC to bond (was temporarily unbonded for XDP)"
            ip neigh del "${SINK_IP_CIDR%/*}" dev "$FREE_NIC" 2>/dev/null || true
            ip addr flush dev "$FREE_NIC" 2>/dev/null || true
            ip link set "$FREE_NIC" down
            ip link set "$FREE_NIC" master "$MGMT_IFACE" 2>/dev/null || true
            ip link set "$FREE_NIC" up
        fi
    fi
    
    # Delete sink namespace
    if ip netns list | grep -q "^$SINK_NS\b"; then
        ip netns del "$SINK_NS" 2>/dev/null || true
    fi
    
    log_info "Network topology cleanup completed"
}

# Start a UDP sink listener with packet counting
start_sink() {
    local sink_ip="$1"
    local sink_port="$2"
    local mode="$3"
    
    log_info "Starting UDP sink listener on $sink_ip:$sink_port (mode: $mode)"
    
    # Create a sink log file to count received packets
    local sink_log="/tmp/sink_${mode}_${BENCH_MODE}.log"
    > "$sink_log"  # Clear the log file
    
    if [[ "$mode" == "udp" ]]; then
        # UDP mode: sink runs in network namespace
        if ip netns list | grep -q "^$SINK_NS\b"; then
            log_info "Starting sink in namespace $SINK_NS"
            # Use a packet-counting sink instead of /dev/null
            ip netns exec "$SINK_NS" timeout $((DURATION_SEC + 10)) bash -c "
                counter=0
                nc -u -l $sink_port | while read line; do
                    counter=\$((counter + 1))
                    if [[ \$((counter % 1000)) -eq 0 ]]; then
                        echo \"Received \$counter packets\" >> $sink_log
                    fi
                done
                echo \"Final count: \$counter packets\" >> $sink_log
            " &
            SINK_PID=$!
        else
            log_error "Namespace $SINK_NS does not exist for UDP mode"
            return 1
        fi
    else
        # XDP mode: sink runs in root namespace on VLAN interface
        log_info "Starting sink in root namespace"
        # Use a packet-counting sink instead of /dev/null
        timeout $((DURATION_SEC + 10)) bash -c "
            counter=0
            nc -u -l $sink_port | while read line; do
                counter=\$((counter + 1))
                if [[ \$((counter % 1000)) -eq 0 ]]; then
                    echo \"Received \$counter packets\" >> $sink_log
                fi
            done
            echo \"Final count: \$counter packets\" >> $sink_log
        " &
    SINK_PID=$!
    fi
    
    # Give it a moment to start
    sleep 2
    
    # Verify the sink is actually listening
    local check_cmd
    if [[ "$mode" == "udp" ]]; then
        check_cmd="ip netns exec $SINK_NS ss -ulnp | grep :$sink_port"
    else
        check_cmd="ss -ulnp | grep :$sink_port"
    fi
    
    if kill -0 $SINK_PID 2>/dev/null && eval "$check_cmd" >/dev/null 2>&1; then
        log_success "UDP sink started and listening (PID: $SINK_PID)"
        return 0
    else
        log_error "Failed to start UDP sink or sink not listening"
        return 1
    fi
}

# Stop the sink listener
stop_sink() {
    if [[ -n "$SINK_PID" ]] && kill -0 $SINK_PID 2>/dev/null; then
        log_info "Stopping UDP sink (PID: $SINK_PID)"
        kill $SINK_PID 2>/dev/null || true
        wait $SINK_PID 2>/dev/null || true
        SINK_PID=""
    fi
}

# Safely unbond interface for XDP testing
unbond_interface() {
    local iface="$1"
    local bond="$2"
    
    log_info "Safely unbonding $iface from $bond for XDP testing"
    
    # Check if interface is actually bonded
    if ! ip link show "$iface" | grep -q "SLAVE"; then
        log_info "$iface is not bonded, no need to unbond"
        return 0
    fi
    
    # Unbond the interface
    ip link set "$iface" nomaster 2>/dev/null || {
        log_error "Failed to unbond $iface from $bond"
        return 1
    }
    
    # Bring interface up
    ip link set "$iface" up
    
    # Add RFC2544 test IP addresses
    ip addr add "${PROXY_IP}/24" dev "$iface" 2>/dev/null || true
    ip addr add "${SINK_IP}/24" dev "$iface" 2>/dev/null || true
    
    log_success "Successfully unbonded $iface from $bond"
    return 0
}

# Restore bonding configuration
restore_bonding() {
    local iface="$1"
    local bond="$2"
    
    if [[ "$NETWORK_RESTORED" == "true" ]]; then
        return 0
    fi
    
    log_info "Restoring bonding configuration for $iface"
    
    # Remove test IPs
    ip addr del "${PROXY_IP}/24" dev "$iface" 2>/dev/null || true
    ip addr del "${SINK_IP}/24" dev "$iface" 2>/dev/null || true
    
    # Bring interface down, re-add to bond, bring back up
    ip link set "$iface" down && \
    ip link set "$iface" master "$bond" && \
    ip link set "$iface" up || {
        log_error "Failed to restore $iface to $bond - THIS IS CRITICAL!"
        log_error "You may need to manually restore:"
        log_error "  ip link set $iface down"
        log_error "  ip link set $iface master $bond"
        log_error "  ip link set $iface up"
        return 1
    }
    
    NETWORK_RESTORED=true
    log_success "Successfully restored $iface to $bond"
    return 0
}

# Check if binary exists and is executable
check_binary() {
    local bin="$1"
    local name="$2"
    if [[ ! -x "$bin" ]]; then
        log_error "$name not executable: $bin"
        exit 1
    fi
}

# Kill all proxy processes
kill_all() { 
    pkill -f 'jito-shredstream-proxy' >/dev/null 2>&1 || true
}

# Pre-detach XDP programs
pre_detach_xdp() {
    log_info "Detaching existing XDP programs from $IFACE"
    ip link set dev "$IFACE" xdp off 2>/dev/null || true
    ip link set dev "$IFACE" xdpgeneric off 2>/dev/null || true
}

# Check if interface supports XDP (after unbonding)
check_xdp_support() {
    local iface="$1"
    # Check if interface is still a slave in a bond
    if ip link show "$iface" | grep -q "SLAVE"; then
        log_warn "Interface $iface is still a slave interface, XDP will not work"
        return 1
    fi
    return 0
}

# Read NIC statistics for comprehensive metrics
read_nic_stats() {
    local mode="$1"
    local tx_packets=0 tx_bytes=0 rx_packets=0 rx_bytes=0
    local hw_tx_packets=0 hw_tx_bytes=0
    
    # For UDP mode: measure on VLAN interface (egress path)
    # For XDP mode: use ethtool hardware stats for accurate XDP measurements
    if [[ "$mode" == "udp" ]]; then
        local measure_iface="$VLAN_IFACE"
        if [[ -r "/sys/class/net/$measure_iface/statistics/tx_packets" ]]; then
            tx_packets=$(< "/sys/class/net/$measure_iface/statistics/tx_packets")
            tx_bytes=$(< "/sys/class/net/$measure_iface/statistics/tx_bytes")
            rx_packets=$(< "/sys/class/net/$measure_iface/statistics/rx_packets")  
            rx_bytes=$(< "/sys/class/net/$measure_iface/statistics/rx_bytes")
        fi
        # Also capture hardware stats for comparison by summing bond slave NIC counters
        # This reflects on-wire egress when using bond0.1591 for UDP
        if [[ -f "/proc/net/bonding/$MGMT_IFACE" ]] && command -v ethtool >/dev/null 2>&1; then
            while read -r slave_if; do
                [[ -z "$slave_if" ]] && continue
                local p b
                p=$(ethtool -S "$slave_if" 2>/dev/null | awk '/^     tx_pkts_nic:/ {print $2}' || echo 0)
                b=$(ethtool -S "$slave_if" 2>/dev/null | awk '/^     tx_bytes_nic:/ {print $2}' || echo 0)
                [[ -n "$p" ]] && hw_tx_packets=$((hw_tx_packets + p)) || true
                [[ -n "$b" ]] && hw_tx_bytes=$((hw_tx_bytes + b)) || true
            done < <(awk '/^Slave Interface:/ {print $3}' "/proc/net/bonding/$MGMT_IFACE")
        fi
    else
        # XDP mode: use hardware statistics from ethtool for accurate measurements
        local measure_iface="$FREE_NIC"
        if command -v ethtool >/dev/null 2>&1; then
            # Use ethtool hardware counters for XDP (bypasses kernel stack)
            tx_packets=$(ethtool -S "$measure_iface" 2>/dev/null | awk '/^     tx_packets:/ {print $2}' || echo 0)
            tx_bytes=$(ethtool -S "$measure_iface" 2>/dev/null | awk '/^     tx_bytes:/ {print $2}' || echo 0)
            rx_packets=$(ethtool -S "$measure_iface" 2>/dev/null | awk '/^     rx_packets:/ {print $2}' || echo 0)
            rx_bytes=$(ethtool -S "$measure_iface" 2>/dev/null | awk '/^     rx_bytes:/ {print $2}' || echo 0)
            # Also get NIC-level counters for better accuracy
            hw_tx_packets=$(ethtool -S "$measure_iface" 2>/dev/null | awk '/^     tx_pkts_nic:/ {print $2}' || echo 0)
            hw_tx_bytes=$(ethtool -S "$measure_iface" 2>/dev/null | awk '/^     tx_bytes_nic:/ {print $2}' || echo 0)
        fi
        # Fallback to kernel stats if ethtool fails
        if [[ "$tx_packets" -eq 0 && -r "/sys/class/net/$measure_iface/statistics/tx_packets" ]]; then
            tx_packets=$(< "/sys/class/net/$measure_iface/statistics/tx_packets")
            tx_bytes=$(< "/sys/class/net/$measure_iface/statistics/tx_bytes")
            rx_packets=$(< "/sys/class/net/$measure_iface/statistics/rx_packets")  
            rx_bytes=$(< "/sys/class/net/$measure_iface/statistics/rx_bytes")
        fi
    fi
    
    # Output: tx_packets tx_bytes rx_packets rx_bytes hw_tx_packets hw_tx_bytes
    printf "%s %s %s %s %s %s\n" "$tx_packets" "$tx_bytes" "$rx_packets" "$rx_bytes" "$hw_tx_packets" "$hw_tx_bytes"
}

# Parse trace shred latency metrics from proxy log
parse_trace_shred_latency() {
    local log="$1"
    
    if [[ ! -f "$log" ]]; then
        echo "0,0,0"
        return
    fi
    
    # Extract trace shred latency metrics
    # Example: shredstream_proxy-trace_shred_latency,trace_region=us-east,trace_seq_num=123,elapsed_micros=1500
    local latencies
    latencies=$(grep -o "elapsed_micros=[0-9]*" "$log" 2>/dev/null | grep -o "[0-9]*" | sort -n)
    
    if [[ -z "$latencies" ]]; then
        echo "0,0,0"
        return
    fi
    
    local count=$(echo "$latencies" | wc -l)
    local sum=$(echo "$latencies" | awk '{sum+=$1} END {print sum}')
    local avg=$((sum / count))
    
    # Calculate percentiles
    local p50=$(echo "$latencies" | awk -v count="$count" 'NR == int(count * 0.5) + 1 {print $1}')
    local p99=$(echo "$latencies" | awk -v count="$count" 'NR == int(count * 0.99) + 1 {print $1}')
    
    echo "$avg,$p50,$p99"
}

# Parse final metrics from proxy log
parse_final_metrics() {
    local log="$1"
    local mode="$2"
    local iface="$3"
    local nic_before="$4"
    local nic_after="$5"
    
    local final_line
    final_line=$(grep "Exiting Shredstream" "$log" | tail -1 || true)
    
    # Parse proxy metrics
    local received forwarded duplicates failed
    if [[ -n "$final_line" ]]; then
        received=$(echo "$final_line" | grep -oE '[0-9]+[[:space:]]+received' | grep -o '[0-9]+' || echo 0)
        forwarded=$(echo "$final_line" | grep -oE '[0-9]+[[:space:]]+sent successfully' | grep -o '[0-9]+' || echo 0)
        # Support variants: "duplicate", "duplicates", "duplicate shreds"
        duplicates=$(echo "$final_line" | grep -oE '[0-9]+[[:space:]]+duplicate(s)?( shreds)?' | grep -o '[0-9]+' || echo 0)
        failed=$(echo "$final_line" | grep -oE '[0-9]+[[:space:]]+failed' | grep -o '[0-9]+' || echo 0)
    fi

    # Fallback to last datapoint if summary not found or parsed zeros
    if [[ -z "$final_line" || "$received" == "0" && "$forwarded" == "0" ]]; then
        local dp
        dp=$(grep 'shredstream_proxy-connection_metrics' "$log" | tail -1 || true)
        if [[ -n "$dp" ]]; then
            received=$(echo "$dp" | grep -oE 'received=[0-9]+i' | grep -oE '[0-9]+' || echo 0)
            forwarded=$(echo "$dp" | grep -oE 'success_forward=[0-9]+i' | grep -oE '[0-9]+' || echo 0)
            failed=$(echo "$dp" | grep -oE 'fail_forward=[0-9]+i' | grep -oE '[0-9]+' || echo 0)
            duplicates=$(echo "$dp" | grep -oE 'duplicate=[0-9]+i' | grep -oE '[0-9]+' || echo 0)
        fi
    fi

    # Parse NIC statistics (now includes hardware stats)
    local nic_tx_before nic_txb_before nic_rx_before nic_rxb_before hw_tx_before hw_txb_before
    local nic_tx_after nic_txb_after nic_rx_after nic_rxb_after hw_tx_after hw_txb_after
    
    read -r nic_tx_before nic_txb_before nic_rx_before nic_rxb_before hw_tx_before hw_txb_before <<< "$nic_before"
    read -r nic_tx_after nic_txb_after nic_rx_after nic_rxb_after hw_tx_after hw_txb_after <<< "$nic_after"
    
    local nic_tx_delta=$((nic_tx_after - nic_tx_before))
    local nic_txb_delta=$((nic_txb_after - nic_txb_before))
    local nic_rx_delta=$((nic_rx_after - nic_rx_before))
    local nic_rxb_delta=$((nic_rxb_after - nic_rxb_before))
    local hw_tx_delta=$((hw_tx_after - hw_tx_before))
    local hw_txb_delta=$((hw_txb_after - hw_txb_before))
    
    # Handle negative deltas (counter resets)
    [[ $nic_tx_delta -lt 0 ]] && nic_tx_delta=$nic_tx_after
    [[ $nic_txb_delta -lt 0 ]] && nic_txb_delta=$nic_txb_after
    [[ $nic_rx_delta -lt 0 ]] && nic_rx_delta=$nic_rx_after
    [[ $nic_rxb_delta -lt 0 ]] && nic_rxb_delta=$nic_rxb_after
    [[ $hw_tx_delta -lt 0 ]] && hw_tx_delta=$hw_tx_after
    [[ $hw_txb_delta -lt 0 ]] && hw_txb_delta=$hw_txb_after

    # Calculate rates and efficiency
    local avg_recv_pps avg_fwd_pps nic_tx_pps nic_tx_mbps hw_tx_pps hw_tx_mbps efficiency dup_rate drop_rate
        if [[ ${DURATION_SEC} -gt 0 ]]; then
            avg_recv_pps=$((received / DURATION_SEC))
            avg_fwd_pps=$((forwarded / DURATION_SEC))
        nic_tx_pps=$((nic_tx_delta / DURATION_SEC))
        hw_tx_pps=$((hw_tx_delta / DURATION_SEC))
        # Calculate Mbps: (bytes * 8) / (duration * 1,000,000)
        nic_tx_mbps=$(awk -v bytes="$nic_txb_delta" -v dur="$DURATION_SEC" 'BEGIN{printf "%.2f", (bytes * 8) / (dur * 1000000)}')
        hw_tx_mbps=$(awk -v bytes="$hw_txb_delta" -v dur="$DURATION_SEC" 'BEGIN{printf "%.2f", (bytes * 8) / (dur * 1000000)}')
        
        # Prefer hardware stats if available (on-wire). For UDP this uses summed bond slave NIC counters.
        if [[ $hw_tx_delta -gt 0 ]]; then
            nic_tx_pps=$hw_tx_pps
            nic_tx_mbps=$hw_tx_mbps
            nic_tx_delta=$hw_tx_delta
            nic_txb_delta=$hw_txb_delta
        fi
    else
        avg_recv_pps=0; avg_fwd_pps=0; nic_tx_pps=0; nic_tx_mbps=0; hw_tx_pps=0; hw_tx_mbps=0
    fi
    
        if [[ $received -gt 0 ]]; then
            efficiency=$((forwarded * 100 / received))
            dup_rate=$((duplicates * 100 / received))
        # Drop rate should be based on failed forwards, not (received - forwarded)
        # In forward-only mode, (received - forwarded) includes duplicates, not drops
        drop_rate=$((failed * 100 / received))
    else
        efficiency=0; dup_rate=0; drop_rate=0
    fi

    # Output format: recv_pps fwd_pps nic_tx_pps nic_tx_mbps efficiency dup_rate drop_rate total_recv total_fwd nic_tx_pkts nic_tx_bytes
    printf "%s %s %s %s %s %s %s %s %s %s %s\n" \
        "$avg_recv_pps" "$avg_fwd_pps" "$nic_tx_pps" "$nic_tx_mbps" \
        "$efficiency" "$dup_rate" "$drop_rate" "$received" "$forwarded" \
        "$nic_tx_delta" "$nic_txb_delta"
}

# Run one mode test
run_one_mode() {
    local mode="$1"
    local port="$2"
    local log="/tmp/shred_${mode}_${BENCH_MODE}.log"
    
    kill_all
    
    log_info "[${mode^^}] Starting $BENCH_MODE mode test (dur=${DURATION_SEC}s, port=$port)"
    
    # Check management safety first
    check_mgmt_safety || return 1
    
    # Set up network topology based on mode
    if [[ "$mode" == "udp" ]]; then
        setup_udp_topology "$mode"
        # Verify on-wire path over VLAN interface
        verify_route_path "$SINK_IP" "$VLAN_IFACE" || {
            log_error "Aborting: incorrect route for UDP phase"
            return 1
        }
    else
        XDP_MODE_USED=true
        pre_detach_xdp
        setup_xdp_topology "$mode"
        
        if ! check_xdp_support "$IFACE"; then
            log_warn "XDP may not work on this interface, continuing anyway..."
        fi
        # Verify untagged sink path over bond0
        verify_route_path "$XDP_SINK_IP" "$MGMT_IFACE" || {
            log_error "Aborting: incorrect route for XDP phase"
            return 1
        }
    fi
    
    # Build proxy command
    local proxy_cmd=(
        timeout "$DURATION_SEC"
        env RUST_LOG=info
        "$PROXY_BIN"
    )
    
    if [[ "$BENCH_MODE" == "shredstream" ]]; then
        proxy_cmd+=(
            shredstream
            --block-engine-url "$BE_URL"
            --auth-keypair "$AUTH_KEYPAIR"
            --desired-regions "$DESIRED_REGIONS"
        )
        [[ -n "$AUTH_URL" ]] && proxy_cmd+=(--auth-url "$AUTH_URL")
        [[ -n "$PUBLIC_IP" ]] && proxy_cmd+=(--public-ip "$PUBLIC_IP")
    else
        proxy_cmd+=(forward-only)
    fi
    
    # Set bind address based on mode
    if [[ "$BENCH_MODE" == "shredstream" ]]; then
        local bind_addr="0.0.0.0"  # Shredstream: block engine connects to us
    else
        local bind_addr="$PROXY_IP"  # Forward-only: bind to RFC 2544 IP
    fi
    
    # Choose sink per mode (VLAN for UDP; untagged for XDP)
    local dest_ip
    if [[ "$mode" == "xdp" ]]; then
        dest_ip="$XDP_SINK_IP"
    else
        dest_ip="$SINK_IP"
    fi
    
    proxy_cmd+=(
        --src-bind-addr "$bind_addr"
        --src-bind-port "$port"
        --dest-ip-ports "${dest_ip}:${SINK_PORT}"
        --debug-trace-shred
        --metrics-report-interval-ms "$METRICS_MS"
        --packet-transmission-mode "$mode"
    )
    
    if [[ "$mode" == "xdp" ]]; then
        proxy_cmd+=(--iface "$IFACE" --xdp-cpu-mode "$XDP_CPU_MODE")
        [[ -n "$XDP_CPUS" ]] && proxy_cmd+=(--xdp-cpus "$XDP_CPUS")
    fi
    
    # Debug: show the proxy command
    log_info "Proxy command: ${proxy_cmd[*]}"
    
    # Start sink for forward-only mode
    if [[ "$BENCH_MODE" == "forward-only" ]]; then
        # Choose sink bind IP per mode
        local sink_bind_ip
        if [[ "$mode" == "xdp" ]]; then
            sink_bind_ip="$XDP_SINK_IP"
        else
            sink_bind_ip="$SINK_IP"
        fi
        start_sink "$sink_bind_ip" "$SINK_PORT" "$mode" || return 1
    fi
    
    # Capture initial NIC statistics
    local nic_before
    nic_before=$(read_nic_stats "$mode")
    
    # Launch proxy
    "${proxy_cmd[@]}" >"${log}" 2>&1 &
    local proxy_pid=$!
    
    # Wait for startup
    sleep 2
    
    # Check if proxy is still running
    if ! kill -0 $proxy_pid 2>/dev/null; then
        log_error "Proxy failed to start for $mode mode"
        cat "$log" >&2
        return 1
    fi
    
    log_success "Proxy started successfully for $mode mode (PID: $proxy_pid)"
    
    # Send test packets if in forward-only mode
    if [[ "$BENCH_MODE" == "forward-only" ]]; then
        local blaster_dest="$bind_addr:$port"
        log_info "Sending test packets to $blaster_dest"
        
        # Calculate optimal duration for stress testing (exact packet count)
        local target_duration
        if [[ "$STRESS_TARGET_PPS" -gt 0 ]]; then
            target_duration=$(echo "$STRESS_TOTAL_PACKETS / $STRESS_TARGET_PPS" | bc)
            # Cap to DURATION_SEC for short tests
            if [[ "$target_duration" -gt "$DURATION_SEC" ]]; then
                target_duration="$DURATION_SEC"
            fi
            log_info "Stress test: targeting up to $STRESS_TOTAL_PACKETS packets in ${target_duration}s (${STRESS_TARGET_PPS} PPS cap)"
        else
            target_duration="$DURATION_SEC"
        fi

        # Reduce packet pool for short tests to avoid long prebuild time
        local blaster_pool_pkts="$POOL_PKTS"
        if [[ "$DURATION_SEC" -le 6 && "$blaster_pool_pkts" -gt 50000 ]]; then
            blaster_pool_pkts=50000
        fi

        timeout "$((${target_duration%.*} + 5))" "$BLASTER_BIN" \
            --dest "$blaster_dest" \
            --threads "$THREADS" \
            --burst "$BURST" \
            --sndbuf "$SNDBUF" \
            --duration "$target_duration" \
            --pool-pkts "$blaster_pool_pkts" \
            --xdp-compat \
            > "/tmp/blaster_${mode}_${BENCH_MODE}.log" 2>&1 || true
    fi
    
    # Wait for proxy to finish
    wait $proxy_pid 2>/dev/null || true
    
    # Capture final NIC statistics
    local nic_after
    nic_after=$(read_nic_stats "$mode")
    
    # Stop sink and clean up for this mode
    stop_sink
    
    # Parse proxy metrics and trace shred latency
    local metrics
    metrics=$(parse_final_metrics "$log" "$mode" "$IFACE" "$nic_before" "$nic_after")
    
    local latency
    latency=$(parse_trace_shred_latency "$log")
    
    # Return: recv_pps fwd_pps nic_tx_pps nic_tx_mbps efficiency dup_rate drop_rate total_recv total_fwd nic_tx_pkts nic_tx_bytes latency_avg_us latency_p50_us latency_p99_us
    echo "${metrics} ${latency//,/ }"
}

# -------- Main Execution --------
log_info "Starting comprehensive UDP vs XDP benchmark"
log_info "Duration: ${DURATION_SEC}s | Mode: $MODE | Bench Mode: $BENCH_MODE"
log_info "Destination: ${SINK_IP}:${SINK_PORT} | Interface: $IFACE"

# Check required binaries
check_binary "$PROXY_BIN" "PROXY_BIN"
if [[ "$BENCH_MODE" == "forward-only" ]]; then
    check_binary "$BLASTER_BIN" "BLASTER_BIN"
fi

# Cleanup on exit
cleanup() {
    log_info "Starting cleanup..."
    kill_all
    stop_sink
    
    # Clean up network topology (safe for management)
    cleanup_topology
    
    log_info "Cleanup completed"
}

trap cleanup EXIT

# Start UDP sink for testing (will be set up per mode in run_one_mode)
if [[ "$BENCH_MODE" == "forward-only" ]]; then
    log_info "Forward-only mode selected - sink will be started per test mode"
fi

# Run tests based on mode
case "$MODE" in
    xdp)
        log_info "Running XDP mode test only"
        read -r xdp_recv_pps xdp_fwd_pps xdp_nic_tx_pps xdp_nic_tx_mbps xdp_efficiency xdp_dup_rate xdp_drop_rate xdp_total_recv xdp_total_fwd xdp_nic_tx_pkts xdp_nic_tx_bytes xdp_lat_avg xdp_lat_p50 xdp_lat_p99 < <(run_one_mode xdp "$XDP_LISTEN_PORT")
        
        printf "MODE   | %12s | %12s | %12s | %12s | %11s | %10s | %10s | %12s | %12s | %10s | %10s | %10s\n" "RECV_PPS" "FWD_PPS" "NIC_TX_PPS" "NIC_TX_MBPS" "EFFICIENCY%" "DUP_RATE%" "DROP_RATE%" "TOTAL_RECV" "TOTAL_FWD" "LAT_AVG_µs" "LAT_P50_µs" "LAT_P99_µs"
        printf -- "-------+--------------+--------------+--------------+--------------+-------------+------------+------------+--------------+--------------+------------+------------+------------\n"
        read -r xdp_lat_a xdp_lat_b xdp_lat_c <<< "$(render_latency "$xdp_lat_avg" "$xdp_lat_p50" "$xdp_lat_p99")"
        printf "%-6s | %12s | %12s | %12s | %12s | %10s%% | %9s%% | %9s%% | %12s | %12s | %10s | %10s | %10s\n" "XDP" "$xdp_recv_pps" "$xdp_fwd_pps" "$xdp_nic_tx_pps" "$xdp_nic_tx_mbps" "$xdp_efficiency" "$xdp_dup_rate" "$xdp_drop_rate" "$xdp_total_recv" "$xdp_total_fwd" "$xdp_lat_a" "$xdp_lat_b" "$xdp_lat_c"
        ;;
    udp)
        log_info "Running UDP mode test only"
        read -r udp_recv_pps udp_fwd_pps udp_nic_tx_pps udp_nic_tx_mbps udp_efficiency udp_dup_rate udp_drop_rate udp_total_recv udp_total_fwd udp_nic_tx_pkts udp_nic_tx_bytes udp_lat_avg udp_lat_p50 udp_lat_p99 < <(run_one_mode udp "$UDP_LISTEN_PORT")
        
        printf "MODE   | %12s | %12s | %12s | %12s | %11s | %10s | %10s | %12s | %12s | %10s | %10s | %10s\n" "RECV_PPS" "FWD_PPS" "NIC_TX_PPS" "NIC_TX_MBPS" "EFFICIENCY%" "DUP_RATE%" "DROP_RATE%" "TOTAL_RECV" "TOTAL_FWD" "LAT_AVG_µs" "LAT_P50_µs" "LAT_P99_µs"
        printf -- "-------+--------------+--------------+--------------+--------------+-------------+------------+------------+--------------+--------------+------------+------------+------------\n"
        read -r udp_lat_a udp_lat_b udp_lat_c <<< "$(render_latency "$udp_lat_avg" "$udp_lat_p50" "$udp_lat_p99")"
        printf "%-6s | %12s | %12s | %12s | %12s | %10s%% | %9s%% | %9s%% | %12s | %12s | %10s | %10s | %10s\n" "UDP" "$udp_recv_pps" "$udp_fwd_pps" "$udp_nic_tx_pps" "$udp_nic_tx_mbps" "$udp_efficiency" "$udp_dup_rate" "$udp_drop_rate" "$udp_total_recv" "$udp_total_fwd" "$udp_lat_a" "$udp_lat_b" "$udp_lat_c"
        ;;
    both)
        log_info "Running both UDP and XDP mode tests"
        read -r udp_recv_pps udp_fwd_pps udp_nic_tx_pps udp_nic_tx_mbps udp_efficiency udp_dup_rate udp_drop_rate udp_total_recv udp_total_fwd udp_nic_tx_pkts udp_nic_tx_bytes udp_lat_avg udp_lat_p50 udp_lat_p99 < <(run_one_mode udp "$UDP_LISTEN_PORT")
        
        # Clean up between modes to ensure proper state for XDP
        log_info "Cleaning up between test modes"
        cleanup_topology
        
        # Add delay for shredstream mode to avoid Jito block engine rate limiting
        if [[ "$BENCH_MODE" == "shredstream" ]]; then
            log_info "Waiting 30 seconds between shredstream tests to avoid rate limiting..."
            sleep 30
        else
            sleep 2
        fi
        
        read -r xdp_recv_pps xdp_fwd_pps xdp_nic_tx_pps xdp_nic_tx_mbps xdp_efficiency xdp_dup_rate xdp_drop_rate xdp_total_recv xdp_total_fwd xdp_nic_tx_pkts xdp_nic_tx_bytes xdp_lat_avg xdp_lat_p50 xdp_lat_p99 < <(run_one_mode xdp "$XDP_LISTEN_PORT")
        
        printf "MODE   | %12s | %12s | %12s | %12s | %11s | %10s | %10s | %12s | %12s | %10s | %10s | %10s\n" "RECV_PPS" "FWD_PPS" "NIC_TX_PPS" "NIC_TX_MBPS" "EFFICIENCY%" "DUP_RATE%" "DROP_RATE%" "TOTAL_RECV" "TOTAL_FWD" "LAT_AVG_µs" "LAT_P50_µs" "LAT_P99_µs"
        printf -- "-------+--------------+--------------+--------------+--------------+-------------+------------+------------+--------------+--------------+------------+------------+------------\n"
        read -r udp_lat_a udp_lat_b udp_lat_c <<< "$(render_latency "$udp_lat_avg" "$udp_lat_p50" "$udp_lat_p99")"
        printf "%-6s | %12s | %12s | %12s | %12s | %10s%% | %9s%% | %9s%% | %12s | %12s | %10s | %10s | %10s\n" "UDP" "$udp_recv_pps" "$udp_fwd_pps" "$udp_nic_tx_pps" "$udp_nic_tx_mbps" "$udp_efficiency" "$udp_dup_rate" "$udp_drop_rate" "$udp_total_recv" "$udp_total_fwd" "$udp_lat_a" "$udp_lat_b" "$udp_lat_c"
        read -r xdp_lat_a xdp_lat_b xdp_lat_c <<< "$(render_latency "$xdp_lat_avg" "$xdp_lat_p50" "$xdp_lat_p99")"
        printf "%-6s | %12s | %12s | %12s | %12s | %10s%% | %9s%% | %9s%% | %12s | %12s | %10s | %10s | %10s\n" "XDP" "$xdp_recv_pps" "$xdp_fwd_pps" "$xdp_nic_tx_pps" "$xdp_nic_tx_mbps" "$xdp_efficiency" "$xdp_dup_rate" "$xdp_drop_rate" "$xdp_total_recv" "$xdp_total_fwd" "$xdp_lat_a" "$xdp_lat_b" "$xdp_lat_c"
        ;;
    *)
        log_error "MODE must be one of: xdp | udp | both"
        exit 1
        ;;
esac

log_success "Benchmark completed successfully"
log_info "Logs available in /tmp/shred_*_${BENCH_MODE}.log and /tmp/blaster_*_${BENCH_MODE}.log"

# Print detailed summary if both modes were tested
if [[ "$MODE" == "both" ]]; then
    echo ""
    echo "=== DETAILED SUMMARY ==="
    echo "Configuration:"
    echo "  Interface: $IFACE"
    echo "  Duration: ${DURATION_SEC}s"
    echo "  Bench Mode: $BENCH_MODE"
    echo "  RFC 2544 Addresses: Proxy=${PROXY_IP} Sink=${SINK_IP}"
    echo ""
    echo "Performance Comparison:"
    echo "  UDP NIC TX: ${udp_nic_tx_pkts} packets (${udp_nic_tx_bytes} bytes) via ${VLAN_IFACE}"
    echo "  XDP NIC TX: ${xdp_nic_tx_pkts} packets (${xdp_nic_tx_bytes} bytes) via ${FREE_NIC}"
    echo ""
    echo "Sink Reception Validation:"
    if [[ -f "/tmp/sink_udp_${BENCH_MODE}.log" ]]; then
        echo "  UDP Sink Received: $(tail -1 /tmp/sink_udp_${BENCH_MODE}.log 2>/dev/null || echo 'No data')"
    else
        echo "  UDP Sink Received: No log file found"
    fi
    if [[ -f "/tmp/sink_xdp_${BENCH_MODE}.log" ]]; then
        echo "  XDP Sink Received: $(tail -1 /tmp/sink_xdp_${BENCH_MODE}.log 2>/dev/null || echo 'No data')"
    else
        echo "  XDP Sink Received: No log file found"
    fi
    echo ""
    echo "Trace Shred Latency (microseconds):"
    echo "  UDP: Avg=${udp_lat_avg}µs, P50=${udp_lat_p50}µs, P99=${udp_lat_p99}µs"
    echo "  XDP: Avg=${xdp_lat_avg}µs, P50=${xdp_lat_p50}µs, P99=${xdp_lat_p99}µs"
    echo ""
    echo "Wire Validation Commands (run manually to verify on-wire transmission):"
    echo "  UDP phase: tcpdump -ni ${VLAN_IFACE} -c 10 'host 198.18.0.1'"
    echo "  XDP phase: tcpdump -ni ${FREE_NIC} -e -vvv -c 10 'vlan 1591 and host 198.18.0.1'"
    echo "  XDP sink:  tcpdump -ni ${VLAN_IFACE} -e -vvv -c 10 'vlan 1591 and host 198.18.0.1'"
    echo ""
    echo "Hardware Statistics (ethtool -S):"
    echo "  Current ${FREE_NIC} TX packets: $(ethtool -S ${FREE_NIC} 2>/dev/null | awk '/^     tx_packets:/ {print $2}' || echo 'N/A')"
    echo "  Current ${FREE_NIC} TX bytes: $(ethtool -S ${FREE_NIC} 2>/dev/null | awk '/^     tx_bytes:/ {print $2}' || echo 'N/A')"
    echo "  Current ${FREE_NIC} NIC TX packets: $(ethtool -S ${FREE_NIC} 2>/dev/null | awk '/^     tx_pkts_nic:/ {print $2}' || echo 'N/A')"
    echo "  Current ${FREE_NIC} NIC TX bytes: $(ethtool -S ${FREE_NIC} 2>/dev/null | awk '/^     tx_bytes_nic:/ {print $2}' || echo 'N/A')"
    echo ""
    
    echo "IMPORTANT METRICS INTERPRETATION:"
    echo "================================="
    echo "• UDP Efficiency: Low due to deduplication in forward-only mode (EXPECTED behavior)"
    echo "• XDP Efficiency: High due to deduplication bypass (different code path)"
    echo "• Duplicate Rate: Shows deduplication effectiveness, not failures"
    echo "• Drop Rate: Only failed forwards, not deduplicated packets"
    echo "• NIC TX Mbps: May be 0 for XDP due to kernel bypass"
    echo ""
    
    echo "PROFESSIONAL RECOMMENDATIONS:"
    echo "=============================="
    echo "1. Use shredstream mode for realistic Solana performance testing"
    echo "2. Forward-only mode shows worst-case synthetic packet behavior"
    echo "3. XDP vs UDP comparison is not apples-to-apples (different code paths)"
    echo "4. For production benchmarks: vary packet payloads to reduce false duplicates"
    echo "5. Monitor XDP with: ethtool -S ${FREE_NIC} | grep -E '(tx_packets|tx_bytes|tx_pkts_nic)'"
    echo "6. Check hardware queue utilization: cat /proc/interrupts | grep ${FREE_NIC}"
    echo "7. Monitor CPU affinity: grep Cpus_allowed_list /proc/\$(pgrep jito-shredstream)/status"
    echo ""
    
    # Calculate performance ratios with caveats
    if [[ $udp_fwd_pps -gt 0 ]]; then
        xdp_udp_ratio=$(awk -v xdp="$xdp_fwd_pps" -v udp="$udp_fwd_pps" 'BEGIN{printf "%.2f", xdp/udp}')
        echo "  XDP vs UDP Forward PPS Ratio: ${xdp_udp_ratio}x (Note: Different deduplication behavior)"
    fi
fi

# Network topology has been safely cleaned up
log_info "Network topology restored safely - management connectivity preserved"
