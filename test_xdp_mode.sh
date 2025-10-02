#!/usr/bin/env bash
set -euo pipefail

# Test XDP forwarding mode
echo "Testing XDP forwarding mode..."

# Kill any existing processes
pkill -f 'jito-shredstream-proxy' >/dev/null 2>&1 || true

# Test XDP mode with forward-only
echo "Starting XDP proxy in forward-only mode..."
timeout 10s ./target/release/jito-shredstream-proxy forward-only \
  --src-bind-addr 0.0.0.0 \
  --src-bind-port 20000 \
  --dest-ip-ports 127.0.0.1:8001 \
  --packet-transmission-mode xdp \
  --iface enp1s0f1 \
  --xdp-cpu-mode auto \
  --metrics-report-interval-ms 1000 \
  > /tmp/xdp_test.log 2>&1 &

XDP_PID=$!
echo "XDP proxy started with PID: $XDP_PID"

# Wait a bit for startup
sleep 2

# Check if the process is still running
if kill -0 $XDP_PID 2>/dev/null; then
    echo "✅ XDP proxy is running successfully"
    
    # Send some test packets
    echo "Sending test packets..."
    timeout 5s ./target/release/udp-blaster --dest 127.0.0.1:20000 --threads 1 --burst 10 --duration 5 > /tmp/blaster_test.log 2>&1 || true
    
    # Wait for the proxy to finish
    wait $XDP_PID 2>/dev/null || true
    
    echo "XDP test completed. Check logs:"
    echo "Proxy log: /tmp/xdp_test.log"
    echo "Blaster log: /tmp/blaster_test.log"
    
    # Show final metrics
    echo "Final metrics from proxy log:"
    grep "Exiting Shredstream" /tmp/xdp_test.log || echo "No exit metrics found"
    
else
    echo "❌ XDP proxy failed to start"
    echo "Error log:"
    cat /tmp/xdp_test.log
    exit 1
fi
