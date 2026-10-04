#!/bin/bash
# The costs of NEW's # dependability mechanisms, on both versions, in 
# alternating blocks.
#
#   evaluation/campaign_costs.sh <out-dir> [blocks] [reps-per-block]
#
# Default: 2 blocks x 3 repetitions = n=6 per point and arm. Arms: OLD, NEW as
# shipped, and NEW with the mechanism under test switched off (the variant).

set -uo pipefail

OUT="${1:?usage: campaign_costs.sh <out-dir> [blocks] [reps-per-block]}"
BLOCKS="${2:-2}"
REPS="${3:-3}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PY="$HERE/.venv/bin/python"
UC="$HERE/../use-cases/file_based_energy_pipeline/docker-compose.yml"
mkdir -p "$OUT"
OUT="$(cd "$OUT" && pwd)"
"$HERE/env_snapshot.sh" "$OUT" >/dev/null
docker ps --format '{{.Names}}\t{{.Image}}\t{{.Status}}' > "$OUT/containers.txt"  # other load on the host

unset ORCHESTRATOR_API_KEY SERVICE_API_KEY ORCHESTRATOR_SECRET

DELAYS_MS=(0 10 25 50 100 200)
WORKFLOWS=10
WINDOW=120
INTERVAL=10
parts=(${COSTS:-delay hung running})
AUTH_OFF=(SERVICE_AUTH_CHECK=false)
CHECKS_OFF=(SERVICE_AUTH_CHECK=false VERIFY_ON_COMPLETION=false STALL_DETECTION=false)

docker build -q -t eval-netem "$HERE/netem" >/dev/null || exit 1
docker compose -f "$UC" up -d

clear_delays() {  
    for svc in input-provider data-generator data-analyzer report-generator; do
        cid=$(docker compose -f "$UC" ps -q "$svc")
        [ -n "$cid" ] && docker run --rm --net "container:$cid" --cap-add NET_ADMIN eval-netem \
            qdisc del dev eth0 root 2>/dev/null
    done
    return 0
}
trap clear_delays EXIT
clear_delays

runner() {  
    local v=$1 variant=$2 s=$3
    shift 3
    (cd "$HERE" && "$PY" runner.py --version "$v" --variant "$variant" --scenario "$s" \
        --use-case file_based --out "$OUT" "$@") | tee -a "$OUT/campaign.log"
}

run_arm() {  
    local v=$1 variant=$2 d
    for part in "${parts[@]}"; do
        case "$part" in
            delay)
                [ "$variant" = checks-off ] && continue
                for d in "${DELAYS_MS[@]}"; do
                    runner "$v" "$variant" service-delay --delay-ms "$d" --workflows "$WORKFLOWS" \
                        --reps "$REPS" --warmup 1
                done
                ;;
            hung)
                [ "$variant" = checks-off ] && continue
                runner "$v" "$variant" hung-submit --reps "$REPS" --warmup 1
                ;;
            running)
                [ "$variant" = auth-check-off ] && continue
                runner "$v" "$variant" running-cost --window "$WINDOW" --interval "$INTERVAL" --reps "$REPS"
                ;;
            *) echo "unknown part: $part" >&2; exit 1 ;;
        esac
    done
}

run_block() {  # version
    if [ "$1" = old ]; then
        "$HERE/stack.sh" old >/dev/null || exit 1
        run_arm old default
    else
        "$HERE/stack.sh" new >/dev/null || exit 1
        run_arm new default
        env "${AUTH_OFF[@]}" "$HERE/stack.sh" new >/dev/null || exit 1
        run_arm new auth-check-off
        env "${CHECKS_OFF[@]}" "$HERE/stack.sh" new >/dev/null || exit 1
        run_arm new checks-off
        "$HERE/stack.sh" new >/dev/null   # back to the shipped configuration
    fi
}

for blk in $(seq 1 "$BLOCKS"); do
    if (( blk % 2 )); then order=(old new); else order=(new old); fi
    for v in "${order[@]}"; do
        echo "### block $blk: $v" | tee -a "$OUT/campaign.log"
        run_block "$v"
    done
done

"$PY" "$HERE/analyze.py" "$OUT"
