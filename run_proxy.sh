#!/usr/bin/env bash
set -euo pipefail

# ---------- defaults ----------
DEV="${DEV:-enp1s0f1}"                               # AF_XDP NIC used by the proxy
DEV_SECONDARY="${DEV_SECONDARY:-}"                   # optional second NIC to detach XDP from on rollback
GW="${GW:-46.166.162.1}"                             # Default gateway (for watchdog checks)
BIN="${BIN:-/root/shredstream-proxy-mod/target/release/jito-shredstream-proxy}"

SRC_BIND_ADDR="${SRC_BIND_ADDR:-0.0.0.0}"
SRC_BIND_PORT="${SRC_BIND_PORT:-}"                   # resolved below per mode
DEST_IP_PORTS="${DEST_IP_PORTS:-127.0.0.1:8001}"
METRICS_MS="${METRICS_MS:-2000}"
PACKET_MODE="${PACKET_MODE:-xdp}"                    # env default; CLI -p can override

# optional internal blaster switches (used when --blaster passed)
BLASTER="${BLASTER:-0}"                              # 1 to enable; or pass --blaster
BLASTER_INTERVAL_MS="${BLASTER_INTERVAL_MS:-1000}"
BLASTER_TARGETS="${BLASTER_TARGETS:-}"

BE_URL="${BE_URL:-https://ny.mainnet.block-engine.jito.wtf}"
AUTH_KEYPAIR="${AUTH_KEYPAIR:-/root/.config/solana/id.json}"
DESIRED_REGIONS="${DESIRED_REGIONS:-ny}"

usage() {
  cat <<EOF
Usage: $0 <mode> [-p xdp|udp] [--blaster] [--blaster-interval-ms N] [--blaster-targets LIST]

Modes:
  shredstream   Request shreds from Block Engine and forward them
  forward-only  Do NOT request shreds; just forward anything received on :\$SRC_BIND_PORT

Flags:
  -p, --packet-mode           xdp | udp   (default: xdp, or PACKET_MODE env)
  --blaster                   enable internal UDP shred blaster
  --blaster-interval-ms N     pacing interval (ms), default 1000
  --blaster-ppi N             packets per interval, default 200000
  --blaster-targets LIST      comma-separated list of IP:PORT blaster sends to
                              (defaults to SRC_BIND_ADDR:SRC_BIND_PORT with 0.0.0.0→127.0.0.1)

Environment overrides:
  DEV, DEV_SECONDARY, GW, BIN, SRC_BIND_ADDR, SRC_BIND_PORT, DEST_IP_PORTS, METRICS_MS,
  BE_URL, AUTH_KEYPAIR, DESIRED_REGIONS
EOF
}

# ---------- sanity checks ----------
MODE="${1:-}"; shift || true
if [[ -z "${MODE}" || ( "${MODE}" != "shredstream" && "${MODE}" != "forward-only" ) ]]; then
  usage; exit 1
fi
if [[ $EUID -ne 0 ]]; then
  echo "[ERROR] Please run as root (needs XDP/routing/sysctls)" >&2
  exit 1
fi
if [[ ! -x "${BIN}" ]]; then
  echo "[ERROR] BIN not found or not executable: ${BIN}" >&2
  exit 1
fi

PACKET_MODE_CLI=""
EXTRA_ARGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    -p|--packet-mode) PACKET_MODE_CLI="${2:-}"; shift 2 ;;
    --blaster)        BLASTER=1; EXTRA_ARGS+=( "--blaster" ); shift ;;
    --blaster-interval-ms) BLASTER_INTERVAL_MS="${2:-1000}"; EXTRA_ARGS+=( "--blaster-interval-ms" "${2:-1000}" ); shift 2 ;;
    --blaster-ppi)         BLASTER_PPI="${2:-200000}"; EXTRA_ARGS+=( "--blaster-ppi" "${2:-200000}" ); shift 2 ;;
    --blaster-targets)     BLASTER_TARGETS="${2:-}"; EXTRA_ARGS+=( "--blaster-targets" "${2:-}" ); shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $1"; usage; exit 1 ;;
  esac
done

if [[ -n "${PACKET_MODE_CLI}" ]]; then PACKET_MODE="${PACKET_MODE_CLI}"; fi
case "${PACKET_MODE,,}" in
  xdp|udp) ;; *)
    echo "Invalid PACKET_MODE: ${PACKET_MODE}. Use xdp|udp."; exit 1 ;;
esac

# resolve port default per mode (avoid duplicates across modes)
if [[ -z "${SRC_BIND_PORT}" ]]; then
  if [[ "${PACKET_MODE}" == "udp" ]]; then SRC_BIND_PORT="20001"; else SRC_BIND_PORT="20000"; fi
fi

# if --blaster requested but no targets, default to local listener
if [[ "${BLASTER}" = "1" && -z "${BLASTER_TARGETS}" ]]; then
  host_for_blaster="${SRC_BIND_ADDR}"
  if [[ "${host_for_blaster}" == "0.0.0.0" ]]; then host_for_blaster="127.0.0.1"; fi
  BLASTER_TARGETS="${host_for_blaster}:${SRC_BIND_PORT}"
  EXTRA_ARGS+=( "--blaster-targets" "${BLASTER_TARGETS}" "--blaster-interval-ms" "${BLASTER_INTERVAL_MS}" )
  [[ -n "${BLASTER_PPI:-}" ]] && EXTRA_ARGS+=( "--blaster-ppi" "${BLASTER_PPI}" )
fi

rollback() {
  echo "[ROLLBACK] Detaching XDP and restoring routing…" >&2
  # Detach XDP from primary + optional secondary NICs
  ip link set "$DEV" xdp off 2>/dev/null || true
  if [[ -n "${DEV_SECONDARY}" ]]; then
    ip link set "$DEV_SECONDARY" xdp off 2>/dev/null || true
  fi
  ip neigh flush all 2>/dev/null || true
  ip route replace default via "$GW" dev bond0 2>/dev/null || true
  systemctl try-restart systemd-networkd 2>/dev/null || systemctl try-restart networking 2>/dev/null || true
  # Try to restore DNS on bond0; fallback to static resolv.conf
  resolvectl dns bond0 1.1.1.1 9.9.9.9 2>/dev/null || echo -e "nameserver 1.1.1.1\nnameserver 9.9.9.9" >/etc/resolv.conf
}
watchdog() {
  sleep 20
  if ! ping -c1 -W2 "$GW" >/dev/null 2>&1; then
    echo "[WATCHDOG] Gateway check failed; rolling back…" >&2
    rollback
  fi
}
trap 'rollback' ERR INT

# ---------- best-effort UDP fairness: raise socket/net buffers ----------
# These help avoid UDP being bottlenecked by tiny defaults. Ignore errors on locked systems.
sysctl -qw net.core.rmem_max=268435456 || true
sysctl -qw net.core.wmem_max=268435456 || true
sysctl -qw net.core.rmem_default=262144 || true
sysctl -qw net.core.wmem_default=262144 || true
sysctl -qw net.core.netdev_max_backlog=250000 || true
ulimit -n 1048576 || true

COMMON_ARGS=(
  --packet-transmission-mode "${PACKET_MODE}"
  --src-bind-addr "${SRC_BIND_ADDR}"
  --src-bind-port "${SRC_BIND_PORT}"
  --dest-ip-ports "${DEST_IP_PORTS}"
  --metrics-report-interval-ms "${METRICS_MS}"
)
if [[ "${MODE}" == "shredstream" ]]; then
  CMD=( "${BIN}" shredstream
        --block-engine-url "${BE_URL}"
        --auth-keypair "${AUTH_KEYPAIR}"
        --desired-regions "${DESIRED_REGIONS}"
        "${COMMON_ARGS[@]}" "${EXTRA_ARGS[@]}" )
else
  CMD=( "${BIN}" forward-only "${COMMON_ARGS[@]}" "${EXTRA_ARGS[@]}" )
fi

echo "[INFO] Mode: ${MODE}"
echo "[INFO] Packet mode: ${PACKET_MODE}  | Bind: ${SRC_BIND_ADDR}:${SRC_BIND_PORT}"
echo "[INFO] Destinations: ${DEST_IP_PORTS}"
[[ "${BLASTER}" = "1" ]] && echo "[INFO] Blaster: interval_ms=${BLASTER_INTERVAL_MS} targets=${BLASTER_TARGETS}"
echo "[INFO] NIC (AF_XDP): ${DEV}${DEV_SECONDARY:+, $DEV_SECONDARY}  | GW: ${GW}"
if [[ "${MODE}" == "shredstream" ]]; then
  echo "[INFO] Block Engine: ${BE_URL}"
  echo "[INFO] Keypair: ${AUTH_KEYPAIR}"
  echo "[INFO] Regions: ${DESIRED_REGIONS}"
fi

watchdog &
echo "[CMD] RUST_LOG=info ${CMD[*]}"
exec env RUST_LOG=info "${CMD[@]}"