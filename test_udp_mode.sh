#!/usr/bin/env bash
set -euo pipefail

# Test UDP forwarding mode
echo "Testing UDP forwarding mode..."

# Kill any existing processes
pkill -f 'jito-shredstream-proxy' >/dev/null 2>&1 || true

# Test UDP mode with forward-only
echo "Starting UDP proxy in forward-only mode..."
timeout 10s ./target/release/jito-shredstream-proxy forward-only \
  --src-bind-addr 0.0.0.0 \
  --src-bind-port 20001 \
  --dest-ip-ports 127.0.0.1:8001 \
  --packet-transmission-mode udp \
  --metrics-report-interval-ms 1000 \
  > /tmp/udp_test.log 2>&1 &

UDP_PID=$!
echo "UDP proxy started with PID: $UDP_PID"

# Wait a bit for startup
sleep 2

# Check if the process is still running
if kill -0 $UDP_PID 2>/dev/null; then
    echo "✅ UDP proxy is running successfully"
    
    # Send some test packets
    echo "Sending test packets..."
    timeout 5s ./target/release/udp-blaster --dest 127.0.0.1:20001 --threads 1 --burst 10 --duration 5 > /tmp/blaster_udp_test.log 2>&1 || true
    
    # Wait for the proxy to finish
    wait $UDP_PID 2>/dev/null || true
    
    echo "UDP test completed. Check logs:"
    echo "Proxy log: /tmp/udp_test.log"
    echo "Blaster log: /tmp/blaster_udp_test.log"
    
    # Show final metrics
    echo "Final metrics from proxy log:"
    grep "Exiting Shredstream" /tmp/udp_test.log || echo "No exit metrics found"
    
else
    echo "❌ UDP proxy failed to start"
    echo "Error log:"
    cat /tmp/udp_test.log
    exit 1
fi
