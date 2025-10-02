#!/usr/bin/env bash
set -euo pipefail

# ---------- Config (override via env) ----------
IF="${IF:-enp1s0f1}"                  # NIC to probe on
BASE="${BASE:-198.18}"                 # "198.18" or "198.19"
OCT3_RANGE="${OCT3_RANGE:-0-0}"       # e.g. 0-255 or 0-0
OCT4_RANGE="${OCT4_RANGE:-1-254}"     # e.g. 1-254
COUNT="${COUNT:-3}"                    # arping -c
DEADLINE="${DEADLINE:-0.5}"           # arping per-IP deadline seconds
CONC="${CONC:-64}"                    # concurrency
OUT="${OUT:-/tmp/arp_hits.csv}"       # output CSV
STOP_ON_FIRST_HIT="${STOP_ON_FIRST_HIT:-0}"  # 1 = stop after first hit

usage() {
  cat <<EOF
Usage: [env VAR=...] ./arp_grind.sh
Vars:
  IF=enp1s0f1  BASE=198.18  OCT3_RANGE=0-255  OCT4_RANGE=1-254
  COUNT=3  DEADLINE=0.5  CONC=64  OUT=/tmp/arp_hits.csv
  STOP_ON_FIRST_HIT=1   (exit after first hit)
EOF
}

[[ "${1:-}" == "-h" || "${1:-}" == "--help" ]] && { usage; exit 0; }

# ---------- Sanity ----------
[[ $EUID -eq 0 ]] || { echo "[ERR] run as root"; exit 1; }
command -v arping >/dev/null || { echo "[ERR] arping not found"; exit 1; }
ip link show "$IF" >/dev/null 2>&1 || { echo "[ERR] interface $IF not found"; exit 1; }

# ---------- Helpers ----------
expand_range() {
  # Be tolerant to missing/empty arg even with 'set -u'
  local r="${1-}"
  if [[ -z "${r}" ]]; then
    echo "[ERR] empty range" >&2
    exit 1
  fi
  local a="${r%-*}" b="${r#*-}"
  if ! [[ "$a" =~ ^[0-9]+$ && "$b" =~ ^[0-9]+$ && $a -le $b ]]; then
    echo "[ERR] bad range: $r" >&2
    exit 1
  fi
  seq "$a" "$b"
}

# ---------- Build target list ----------
mapfile -t oct3_list < <(expand_range "$OCT3_RANGE")
mapfile -t oct4_list < <(expand_range "$OCT4_RANGE")

# Header
echo "IP,MAC,RTT_ms,Interface" > "$OUT"

# Worklist
worklist="$(mktemp)"
first_hit_flag="${OUT}.first_hit"
: > "$worklist"
: > "$first_hit_flag" || true

# Ensure cleanup
helper=""
cleanup() { rm -f "$worklist" "$first_hit_flag" "$helper"; }
trap cleanup EXIT

for o3 in "${oct3_list[@]}"; do
  for o4 in "${oct4_list[@]}"; do
    echo "${BASE}.${o3}.${o4}" >> "$worklist"
  done
done

echo "[INFO] Probing $(wc -l < "$worklist") IPs on $IF (BASE=$BASE, OCT3=$OCT3_RANGE, OCT4=$OCT4_RANGE) ..." >&2

# ---------- Create a helper script each worker runs ----------
helper="$(mktemp)"
cat >"$helper" <<'H'
#!/usr/bin/env bash
set -euo pipefail

ip="$1"
IF="$2"
COUNT="$3"
DEADLINE="$4"
OUT="$5"
FIRST="$6"

# Some arping variants don’t support -w; try with it, fall back without.
run_arping() {
  arping -I "$IF" -c "$COUNT" -w "$DEADLINE" "$ip" 2>/dev/null && return 0
  arping -I "$IF" -c "$COUNT" "$ip" 2>/dev/null || true
}
out="$(run_arping || true)"

if grep -q "Unicast reply from" <<<"$out"; then
  mac="$(grep -m1 -oE '\[[0-9a-f:]{17}\]' <<<"$out" | tr -d '[]' || true)"
  rtt="$(grep -m1 -oE '[0-9]+(\.[0-9]+)?ms' <<<"$out" || echo "NA")"
  {
    flock -w 5 9
    echo "$ip,$mac,${rtt/ms/},$IF" >> "$OUT"
  } 9>>"$OUT"
  echo "[HIT] $ip  mac=$mac  rtt=$rtt" >&2
  if [[ "$FIRST" == "1" ]]; then
    echo "$ip" > "${OUT}.first_hit"
  fi
else
  echo "[MISS] $ip" >&2
fi
H
chmod +x "$helper"

# ---------- Run in parallel ----------
if command -v parallel >/dev/null 2>&1; then
  halt_opt=()
  [[ "$STOP_ON_FIRST_HIT" == "1" ]] && halt_opt=(--halt now,success=1)
  parallel -j "$CONC" "${halt_opt[@]}" \
    "$helper" {} "$IF" "$COUNT" "$DEADLINE" "$OUT" "$STOP_ON_FIRST_HIT" \
    :::: "$worklist"
else
  # xargs fallback
  xargs -a "$worklist" -n1 -P "$CONC" \
    "$helper" \
    "$IF" "$COUNT" "$DEADLINE" "$OUT" "$STOP_ON_FIRST_HIT"
fi

if [[ "$STOP_ON_FIRST_HIT" == "1" && -s "$first_hit_flag" ]]; then
  echo "[INFO] Stopped early after first hit: $(cat "$first_hit_flag")" >&2
fi

echo "[DONE] CSV written to $OUT" >&2