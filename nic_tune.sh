#!/usr/bin/env bash
# Tune a NIC for high-performance traditional packet processing (non-zero-copy XDP/UDP): 
# set queue count, pin each queue's IRQ to a CPU, optimize for copy-based processing.
# Usage:
#   sudo ./nic-tune.sh -i enp1s0f1 -q 4 -c 0,2,4,6 --apply-rss --disable-irqbalance --optimize-traditional
#
# Notes
# - Run as root. irqbalance is disabled by default only if you pass --disable-irqbalance.
# - Optimized for traditional (copy-based) packet processing, not zero-copy.
# - For traditional processing, we enable RPS/XPS for better CPU utilization.
# - Buffer and interrupt optimizations focus on copy-based performance.

set -euo pipefail

iface=""
num_queues=""
cpu_list=""
apply_rss="false"
disable_irqbalance="false"
gro_off="false"
lro_off="false"
optimize_traditional="false"

die() { echo "ERROR: $*" >&2; exit 1; }

usage() {
  cat <<EOF
Usage: sudo $0 -i IFACE [-q NUM_QUEUES] [-c CPU_LIST] [OPTIONS]

Required:
  -i, --iface IFACE          NIC interface name (e.g. enp1s0f1)

Optional:
  -q, --queues N             Number of combined queues to enable (ethtool -L IFACE combined N).
                             If omitted, N defaults to number of CPUs in CPU_LIST (or 1).
  -c, --cpus LIST            Comma-separated CPU cores to pin to queues 0..N-1 (e.g. 0,2,4,6).
                             If omitted, defaults to 0..N-1 (or 0..(online-1) if N is omitted).

Performance Options:
      --apply-rss            Set RSS indirection table evenly across N queues (ethtool -X).
      --disable-irqbalance   Stop and disable irqbalance (prevents core bouncing).
      --gro-off              Disable Generic Receive Offload (ethtool -K IFACE gro off)
      --lro-off              Disable Large Receive Offload (ethtool -K IFACE lro off)
      --optimize-traditional Full optimization for traditional (non-zero-copy) packet processing

Examples:
  # Full optimization for traditional packet processing (your best performing config):
  sudo $0 -i enp1s0f1 -q 4 -c 0,2,4,6 --apply-rss --disable-irqbalance --optimize-traditional
  
  # Basic setup:
  sudo $0 -i enp1s0f1 -c 0,2,4,6    # queues will default to 4 here
EOF
  exit 1
}

# Parse args
while [[ $# -gt 0 ]]; do
  case "$1" in
    -i|--iface) iface="$2"; shift 2 ;;
    -q|--queues) num_queues="$2"; shift 2 ;;
    -c|--cpus) cpu_list="$2"; shift 2 ;;
    --apply-rss) apply_rss="true"; shift ;;
    --disable-irqbalance) disable_irqbalance="true"; shift ;;
    --gro-off) gro_off="true"; shift ;;
    --lro-off) lro_off="true"; shift ;;
    --optimize-traditional) optimize_traditional="true"; shift ;;
    -h|--help) usage ;;
    *) die "Unknown option: $1";;
  esac
done

[[ -n "$iface" ]] || usage
[[ $EUID -eq 0 ]] || die "Run as root."

[[ -d "/sys/class/net/$iface" ]] || die "Interface $iface not found."

# Helper: get online CPUs (e.g., "0-23")
online_range="$(cat /sys/devices/system/cpu/online)"
expand_range() { # "0-3,8,10-11" -> "0 1 2 3 8 10 11"
  local in="$1"; local out=() r a b
  for r in ${in//,/ }; do
    if [[ "$r" =~ ^([0-9]+)-([0-9]+)$ ]]; then
      a="${BASH_REMATCH[1]}"; b="${BASH_REMATCH[2]}"
      for ((i=a; i<=b; i++)); do out+=("$i"); done
    else
      out+=("$r")
    fi
  done
  echo "${out[*]}"
}

# Decide CPU list
if [[ -z "$cpu_list" ]]; then
  if [[ -n "$num_queues" ]]; then
    # Default to 0..(num_queues-1)
    cpus_arr=()
    for ((i=0; i<num_queues; i++)); do cpus_arr+=("$i"); done
  else
    # Default to all online CPUs
    read -r -a cpus_arr <<<"$(expand_range "$online_range")"
  fi
else
  IFS=',' read -r -a cpus_arr <<<"$cpu_list"
fi

# Decide queue count
if [[ -z "$num_queues" ]]; then
  num_queues="${#cpus_arr[@]}"
fi
(( num_queues > 0 )) || die "Queue count must be > 0."
(( ${#cpus_arr[@]} >= num_queues )) || die "CPU list has fewer cores than queues ($num_queues)."

echo "=== Plan ==="
echo "Interface            : $iface"
echo "Queues (combined)    : $num_queues"
echo "CPU map (queue i→CPU): ${cpus_arr[*]} (first $num_queues used)"
echo "Apply RSS            : $apply_rss"
echo "Disable irqbalance   : $disable_irqbalance"
echo "GRO off              : $gro_off"
echo "LRO off              : $lro_off"
echo

# 1) Optional offload toggles
if [[ "$gro_off" == "true" ]]; then
  ethtool -K "$iface" gro off || echo "WARN: ethtool -K $iface gro off failed"
fi
if [[ "$lro_off" == "true" ]]; then
  ethtool -K "$iface" lro off || echo "WARN: ethtool -K $iface lro off failed"
fi

# 2) Set queue count
echo "Setting $iface combined queues to $num_queues..."
if ! ethtool -L "$iface" combined "$num_queues"; then
  echo "WARN: ethtool -L failed (driver may not support changing queue count). Continuing..."
fi

# 3) Optional RSS indirection
if [[ "$apply_rss" == "true" ]]; then
  echo "Applying even RSS indirection across $num_queues queues..."
  if ! ethtool -X "$iface" equal "$num_queues"; then
    echo "WARN: ethtool -X failed (not supported on all drivers)."
  fi
fi

# 4) Configure RPS/XPS based on processing mode
if [[ "$optimize_traditional" == "true" ]]; then
  echo "Enabling RPS/XPS for traditional packet processing on $iface..."
  # Enable RPS on all RX queues - use all available CPUs for packet processing
  cpu_mask=$(printf "%x" $(( (1 << $(nproc)) - 1 )))
  for f in /sys/class/net/"$iface"/queues/rx-*/rps_cpus; do
    [[ -e "$f" ]] && echo "$cpu_mask" > "$f" || true
  done
  # Enable XPS on TX queues - map to queue's assigned CPU
  for ((q=0; q<num_queues; q++)); do
    cpu="${cpus_arr[$q]}"
    cpu_mask=$(printf "%x" $(( 1 << cpu )))
    f="/sys/class/net/$iface/queues/tx-$q/xps_cpus"
    [[ -e "$f" ]] && echo "$cpu_mask" > "$f" || true
  done
else
  echo "Disabling RPS/XPS (recommended for zero-copy AF_XDP) on $iface..."
  for f in /sys/class/net/"$iface"/queues/rx-*/rps_cpus; do
    [[ -e "$f" ]] && echo 0 > "$f" || true
  done
  for f in /sys/class/net/"$iface"/queues/tx-*/xps_cpus; do
    [[ -e "$f" ]] && echo 0 > "$f" || true
  done
fi

# 5) Optionally disable irqbalance
if [[ "$disable_irqbalance" == "true" ]]; then
  if command -v systemctl >/dev/null 2>&1; then
    systemctl stop irqbalance || true
    systemctl disable irqbalance || true
  else
    service irqbalance stop || true
    update-rc.d irqbalance disable || true
  fi
fi

# Helper: convert a single CPU number to an smp_affinity hex mask (multi-32-bit)
cpu_to_hexmask() {
  local cpu="$1"
  local idx=$(( cpu / 32 ))
  local bit=$(( cpu % 32 ))
  local words=()
  for ((i=0; i<=idx; i++)); do words+=(0); done
  words[$idx]=$(( 1 << bit ))
  # print little-endian comma-separated hex (LSW first)
  local out=""
  for ((i=${#words[@]}-1; i>=0; i--)); do
    # build big-endian string (MSW first) because kernel expects that order
    printf -v hex "%x" "${words[$i]}"
    out+="${hex},"
  done
  out="${out%,}"
  echo "$out"
}

# 6) Map IRQs (TxRx-k) → CPUs
echo "Mapping IRQs to CPUs..."
# Re-probe interrupts now that queue count may have changed
mapfile -t irq_lines < <(grep -nE "${iface}.*(TxRx|tx|rx).*-[0-9]+" /proc/interrupts || true)

if [[ ${#irq_lines[@]} -eq 0 ]]; then
  echo "WARN: Could not find per-queue IRQs for $iface in /proc/interrupts."
  echo "      Driver naming varies. Showing all lines for iface:"
  grep -n "$iface" /proc/interrupts || true
else
  # Extract tuples: (irq_number queue_index)
  declare -A irq_by_q=()
  for line in "${irq_lines[@]}"; do
    # Example line:
    # 119: ... IR-PCI-MSIX-...  enp1s0f1-TxRx-1
    irq=$(echo "$line" | awk -F: '{print $1}')
    name=$(echo "$line" | awk '{print $NF}')
    # Try to parse trailing "-<num>"
    if [[ "$name" =~ -([0-9]+)$ ]]; then
      q="${BASH_REMATCH[1]}"
      irq_by_q["$q"]="$irq"
    fi
  done

  # Apply affinity for queues 0..(num_queues-1)
  for ((q=0; q<num_queues; q++)); do
    cpu="${cpus_arr[$q]}"
    irq="${irq_by_q[$q]:-}"

    if [[ -z "$irq" ]]; then
      echo "WARN: No IRQ found for queue $q; available queues: ${!irq_by_q[@]}"
      continue
    fi

    echo "Queue $q → IRQ $irq → CPU $cpu"

    if [[ -e /proc/irq/"$irq"/smp_affinity_list ]]; then
      echo "$cpu" > /proc/irq/"$irq"/smp_affinity_list || echo "WARN: write to smp_affinity_list failed for IRQ $irq"
    else
      # Fallback to hex bitmask
      mask="$(cpu_to_hexmask "$cpu")"
      echo "$mask" > /proc/irq/"$irq"/smp_affinity || echo "WARN: write to smp_affinity failed for IRQ $irq"
    fi
  done
fi

echo
echo "=== Summary ==="
echo "Interface          : $iface"
echo "Queues requested   : $num_queues"
echo "CPUs (first N)     : ${cpus_arr[*]:0:num_queues}"
[[ "$apply_rss" == "true" ]] && echo "RSS indirection    : applied (if supported)"
echo "RPS/XPS            : disabled"
[[ "$disable_irqbalance" == "true" ]] && echo "irqbalance         : stopped/disabled"
[[ "$gro_off" == "true" ]] && echo "GRO                : off"
[[ "$lro_off" == "true" ]] && echo "LRO                : off"

echo
echo "=== Verify ==="
echo "1) Queue count       : ethtool -l $iface"
echo "2) RSS indir (opt)   : ethtool -x $iface"
echo "3) IRQ pins          : grep -n \"$iface\" /proc/interrupts"
echo "                       cat /proc/irq/<IRQ>/smp_affinity_list   # or smp_affinity"
echo
echo "Done."