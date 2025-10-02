#!/usr/bin/env bash
set -euo pipefail

# Forward-only UDP vs XDP bench: packet blaster -> proxy -> sink
# Supports "purist" topology with a netns LACP peer for UDP, then AF_XDP to bond sink.

# ---------------- config ----------------
DURATION_SEC="${DURATION_SEC:-10}"
UDP_LISTEN_PORT="${UDP_LISTEN_PORT:-20001}"
XDP_LISTEN_PORT="${XDP_LISTEN_PORT:-20000}"

SENDER_IF_BOND="${SENDER_IF_BOND:-bond0}"
SENDER_IF_BOND_SLAVE_FOR_SINK_MAC="${SENDER_IF_BOND_SLAVE_FOR_SINK_MAC:-enp1s0f0}"
XDP_TX_IF="${XDP_TX_IF:-enp1s0f1}"

USE_VLAN="${USE_VLAN:-1}"
VLAN_ID="${VLAN_ID:-1591}"

RFC_SENDER_IP="${RFC_SENDER_IP:-198.18.0.2/24}"
RFC_SINK_IP="${RFC_SINK_IP:-198.18.0.1/24}"
RFC_SINK_IP_ADDR="${RFC_SINK_IP_ADDR:-198.18.0.1}"
SINK_UDP_PORT="${SINK_UDP_PORT:-20001}"

PROXY_BIN="${PROXY_BIN:-./target/release/jito-shredstream-proxy}"
BLASTER_BIN="${BLASTER_BIN:-./target/release/udp_blaster}"

THREADS="${THREADS:-4}"
BURST="${BURST:-256}"
SNDBUF="${SNDBUF:-67108864}"

# ---------------- helpers ----------------
dev_vlan(){ local dev="$1"; if [[ "$USE_VLAN" = "1" ]]; then echo "${dev}.${VLAN_ID}"; else echo "$dev"; fi; }
ns_exec(){ ip netns exec sinkns bash -lc "$*"; }

cleanup(){
  set +e
  pkill -f "jito-shredstream-proxy forward-only" >/dev/null 2>&1 || true
  if ip netns list | grep -q "^sinkns\b"; then
    ns_exec "ip link set ${XDP_TX_IF} nomaster || true"
    ns_exec "ip link del bondns0.${VLAN_ID} 2>/dev/null || true"
    ns_exec "ip link del bondns0 2>/dev/null || true"
    ip netns exec sinkns ip link set ${XDP_TX_IF} netns 1 2>/dev/null || true
    ip netns del sinkns 2>/dev/null || true
  fi
  # remove static neighbor that we may set during XDP phase
  ip neigh del ${RFC_SINK_IP_ADDR} dev ${XDP_TX_IF} 2>/dev/null || true
}
trap cleanup EXIT

[[ -x "${PROXY_BIN}" ]] || { echo "[ERR] PROXY_BIN not executable: ${PROXY_BIN}" >&2; exit 1; }
[[ -x "${BLASTER_BIN}" ]] || { echo "[ERR] BLASTER_BIN not executable: ${BLASTER_BIN}" >&2; exit 1; }

bond_dev="$(dev_vlan "${SENDER_IF_BOND}")"

echo "[SETUP] Sender path on ${bond_dev} (${RFC_SENDER_IP})"
ip link set "${SENDER_IF_BOND}" up 2>/dev/null || true
if [[ "${USE_VLAN}" = "1" ]]; then
  ip link add link "${SENDER_IF_BOND}" name "${bond_dev}" type vlan id "${VLAN_ID}" 2>/dev/null || true
fi
ip link set "${bond_dev}" up
ip addr flush dev "${bond_dev}"
ip addr add "${RFC_SENDER_IP}" dev "${bond_dev}"

echo "[UDP] Creating LACP peer in netns on ${XDP_TX_IF} → bondns0 (sink ${RFC_SINK_IP})"
ip netns add sinkns 2>/dev/null || true
ip link set "${XDP_TX_IF}" down
ip link set "${XDP_TX_IF}" netns sinkns
ns_exec "modprobe bonding 2>/dev/null || true"
ns_exec "ip link add bondns0 type bond mode 802.3ad"
ns_exec "echo fast > /sys/class/net/bondns0/bonding/lacp_rate"
ns_exec "ip link set ${XDP_TX_IF} up"
ns_exec "ip link set ${XDP_TX_IF} master bondns0"
ns_exec "ip link set bondns0 up"
if [[ "${USE_VLAN}" = "1" ]]; then
  ns_exec "ip link add link bondns0 name bondns0.${VLAN_ID} type vlan id ${VLAN_ID}"
  ns_exec "ip addr add ${RFC_SINK_IP} dev bondns0.${VLAN_ID}"
  ns_exec "ip link set bondns0.${VLAN_ID} up"
else
  ns_exec "ip addr add ${RFC_SINK_IP} dev bondns0"
fi

echo "[LACP] Root bond state:"
cat /proc/net/bonding/${SENDER_IF_BOND} | sed -n '1,120p' || true
echo "[LACP] Netns bond state:"
ns_exec "cat /proc/net/bonding/bondns0 | sed -n '1,120p'" || true

udp_sink_dev="$(dev_vlan bondns0)"

echo "[COUNTERS] UDP before: root TX on ${bond_dev}=$(< /sys/class/net/${bond_dev}/statistics/tx_packets)"
ns_exec "echo \"[COUNTERS] UDP before: sink RX on ${udp_sink_dev}=\$((\$(< /sys/class/net/${udp_sink_dev}/statistics/rx_packets)))\""

echo "[RUN] Proxy forward-only UDP on :${UDP_LISTEN_PORT} → ${RFC_SINK_IP_ADDR}:${SINK_UDP_PORT}"
timeout "${DURATION_SEC}" "${PROXY_BIN}" forward-only \
  --src-bind-addr 0.0.0.0 \
  --src-bind-port "${UDP_LISTEN_PORT}" \
  --dest-ip-ports "${RFC_SINK_IP_ADDR}:${SINK_UDP_PORT}" \
  --packet-transmission-mode udp > /tmp/proxy_udp.log 2>&1 &
UDP_PID=$!
sleep 1

echo "[BLAST] sendmmsg to 127.0.0.1:${UDP_LISTEN_PORT} for ${DURATION_SEC}s"
"${BLASTER_BIN}" --dest 127.0.0.1:"${UDP_LISTEN_PORT}" --threads "${THREADS}" --burst "${BURST}" --sndbuf "${SNDBUF}" --duration "${DURATION_SEC}" > /tmp/blaster_udp.log 2>&1 || true
sleep 1
kill "${UDP_PID}" >/dev/null 2>&1 || true; wait "${UDP_PID}" 2>/dev/null || true

echo "[COUNTERS] UDP after: root TX on ${bond_dev}=$(< /sys/class/net/${bond_dev}/statistics/tx_packets)"
ns_exec "echo \"[COUNTERS] UDP after: sink RX on ${udp_sink_dev}=\$((\$(< /sys/class/net/${udp_sink_dev}/statistics/rx_packets)))\""

echo "[TEARDOWN] Removing netns LACP sink"
ns_exec "ip link set ${XDP_TX_IF} nomaster || true"
if [[ "${USE_VLAN}" = "1" ]]; then
  ns_exec "ip link del bondns0.${VLAN_ID} 2>/dev/null || true"
fi
ns_exec "ip link del bondns0 2>/dev/null || true"
ip netns exec sinkns ip link set "${XDP_TX_IF}" netns 1
ip netns del sinkns

echo "[XDP] Configure sink on ${bond_dev} (${RFC_SINK_IP}) and TX on ${XDP_TX_IF}"
ip addr flush dev "${bond_dev}"
ip addr add "${RFC_SINK_IP}" dev "${bond_dev}"
ip link set "${bond_dev}" up

ip addr flush dev "${XDP_TX_IF}"
ip link set "${XDP_TX_IF}" up
ip addr add "${RFC_SENDER_IP}" dev "${XDP_TX_IF}"

sink_mac="$(cat "/sys/class/net/${SENDER_IF_BOND_SLAVE_FOR_SINK_MAC}/address")"
ip neigh replace "${RFC_SINK_IP_ADDR}" lladdr "${sink_mac}" nud permanent dev "${XDP_TX_IF}"

echo "[COUNTERS] XDP before: ${XDP_TX_IF} tx_pkts=$(< /sys/class/net/${XDP_TX_IF}/statistics/tx_packets)  ${bond_dev} rx_pkts=$(< /sys/class/net/${bond_dev}/statistics/rx_packets)"

echo "[RUN] Proxy forward-only XDP on :${XDP_LISTEN_PORT} → ${RFC_SINK_IP_ADDR}:${SINK_UDP_PORT}"
timeout "${DURATION_SEC}" "${PROXY_BIN}" forward-only \
  --src-bind-addr 0.0.0.0 \
  --src-bind-port "${XDP_LISTEN_PORT}" \
  --dest-ip-ports "${RFC_SINK_IP_ADDR}:${SINK_UDP_PORT}" \
  --packet-transmission-mode xdp > /tmp/proxy_xdp.log 2>&1 &
XDP_PID=$!
sleep 1

echo "[BLAST] sendmmsg to 127.0.0.1:${XDP_LISTEN_PORT} for ${DURATION_SEC}s"
"${BLASTER_BIN}" --dest 127.0.0.1:"${XDP_LISTEN_PORT}" --threads "${THREADS}" --burst "${BURST}" --sndbuf "${SNDBUF}" --duration "${DURATION_SEC}" > /tmp/blaster_xdp.log 2>&1 || true
sleep 1
kill "${XDP_PID}" >/dev/null 2>&1 || true; wait "${XDP_PID}" 2>/dev/null || true

echo "[COUNTERS] XDP after:  ${XDP_TX_IF} tx_pkts=$(< /sys/class/net/${XDP_TX_IF}/statistics/tx_packets)  ${bond_dev} rx_pkts=$(< /sys/class/net/${bond_dev}/statistics/rx_packets)"

echo "\n==== Summary ===="
echo "UDP   root TX pkts (bond path): $(< /sys/class/net/${bond_dev}/statistics/tx_packets)"
echo "UDP   sink RX pkts (netns bond): see above"
echo "XDP   ${XDP_TX_IF} TX pkts:     $(< /sys/class/net/${XDP_TX_IF}/statistics/tx_packets)"
echo "XDP   bond RX pkts:             $(< /sys/class/net/${bond_dev}/statistics/rx_packets)"
echo "Logs: /tmp/proxy_udp.log /tmp/blaster_udp.log /tmp/proxy_xdp.log /tmp/blaster_xdp.log"




