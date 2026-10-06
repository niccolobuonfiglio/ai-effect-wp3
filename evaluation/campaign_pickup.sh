#!/bin/bash
# Diagnostic (Amendment 6, not pre-registered): where nominal latency goes.
#
#   evaluation/campaign_pickup.sh <out-dir> [blocks] [reps-per-block]
#
# Default: 2 blocks x 15 repetitions = n=30 per use case, jitter and version,
# as for nominal. Jitter 0 repeats the campaign's back-to-back submissions;
# jitter 1 (one worker poll interval) spreads them over the poll cycle.
set -uo pipefail

OUT="${1:?usage: campaign_pickup.sh <out-dir> [blocks] [reps-per-block]}"
BLOCKS="${2:-2}"
REPS="${3:-15}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PY="$HERE/.venv/bin/python"
mkdir -p "$OUT"
OUT="$(cd "$OUT" && pwd)"
"$HERE/env_snapshot.sh" "$OUT" >/dev/null

unset ORCHESTRATOR_API_KEY SERVICE_API_KEY ORCHESTRATOR_SECRET

JITTERS=(0 1)
declare -A COMPOSE=(
  [file_based]=file_based_energy_pipeline
  [protobuf]=protobuf_based_energy_pipeline
)
use_cases=(file_based protobuf)
for uc in "${use_cases[@]}"; do
    docker compose -f "$HERE/../use-cases/${COMPOSE[$uc]}/docker-compose.yml" up -d
done

run_block() {  # version
    local v=$1 uc j
    for uc in "${use_cases[@]}"; do
        for j in "${JITTERS[@]}"; do
            (cd "$HERE" && "$PY" runner.py --version "$v" --scenario pickup --use-case "$uc" \
                --start-jitter "$j" --reps "$REPS" --warmup 2 --out "$OUT") | tee -a "$OUT/campaign.log"
        done
    done
}

for blk in $(seq 1 "$BLOCKS"); do
    if (( blk % 2 )); then order=(old new); else order=(new old); fi
    for v in "${order[@]}"; do
        echo "### block $blk: $v" | tee -a "$OUT/campaign.log"
        "$HERE/stack.sh" "$v" >/dev/null || exit 1
        run_block "$v"
    done
done

"$PY" "$HERE/analyze.py" "$OUT"
