#!/bin/bash
#
#   evaluation/campaign.sh <out-dir> [blocks] [reps-per-block]
#
# Default: 2 blocks x 5 repetitions = n=10 per scenario and version (n=30 for
# nominal). Blocks alternate the order OLD/NEW so that slow drift (thermal,
# caches, background load) does not line up with one version. Each block
# restarts the stack, so each starts from an empty Redis.
set -uo pipefail

OUT="${1:?usage: campaign.sh <out-dir> [blocks] [reps-per-block]}"
BLOCKS="${2:-2}"
REPS="${3:-5}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PY="$HERE/.venv/bin/python"
mkdir -p "$OUT"
OUT="$(cd "$OUT" && pwd)"   
"$HERE/env_snapshot.sh" "$OUT" >/dev/null


unset ORCHESTRATOR_API_KEY SERVICE_API_KEY ORCHESTRATOR_SECRET

use_cases=(${CAMPAIGN_USE_CASES:-file_based germany protobuf})
declare -A SCENARIOS=(
  [file_based]="nominal burst outage short-outage request-error concurrent-outage worker-death credential-misconfig corrupt-inline"
  [germany]="nominal outage short-outage request-error worker-death credential-misconfig corrupt-inline outage-during-task"
  [protobuf]="nominal outage short-outage request-error worker-death credential-misconfig corrupt-inline"
)
declare -A COMPOSE=(
  [file_based]=file_based_energy_pipeline
  [germany]=germany-node
  [protobuf]=protobuf_based_energy_pipeline
)
for uc in "${use_cases[@]}"; do
    [ -n "${SCENARIOS[$uc]:-}" ] || { echo "unknown use case: $uc" >&2; exit 1; }
    # Their ports do not clash: 18081-4, 18091-3, 18181-4 (+ gRPC 50152-4)
    docker compose -f "$HERE/../use-cases/${COMPOSE[$uc]}/docker-compose.yml" up -d
done

selected() {  
    local s out=()
    for s in ${SCENARIOS[$1]}; do
        if [ -z "${CAMPAIGN_SCENARIOS:-}" ] || [[ " $CAMPAIGN_SCENARIOS " == *" $s "* ]]; then
            out+=("$s")
        fi
    done
    echo "${out[@]}"
}


run_cell() {  
    local v=$1 uc=$2 s=$3 reps=$REPS extra=()
    [ "$s" = nominal ] && { reps=$((REPS * 3)); extra=(--warmup 2); }
    [ "$s" = worker-death ] && extra=(--timeout 120)   # lease 20 s + slack; OLD never finishes
    if [ "$s" = worker-death ] && [ "$v" = new ]; then
        LEASE_SECONDS=20 "$HERE/stack.sh" new >/dev/null
    fi
    (cd "$HERE" && "$PY" runner.py --version "$v" --scenario "$s" --reps "$reps" \
        --use-case "$uc" --out "$OUT" "${extra[@]}") | tee -a "$OUT/campaign.log"
    if [ "$s" = worker-death ] && [ "$v" = new ]; then
        "$HERE/stack.sh" new >/dev/null   # back to the default lease
    fi

    if [ "$s" = nominal ] && [ "$v" = new ] && [ "$uc" = file_based ]; then
        SERVICE_AUTH_CHECK=false BLUEPRINT_TYPE_CHECK=off "$HERE/stack.sh" new >/dev/null
        (cd "$HERE" && "$PY" runner.py --version new --scenario nominal --variant checks-off \
            --reps "$reps" --use-case file_based --out "$OUT" "${extra[@]}") | tee -a "$OUT/campaign.log"
        "$HERE/stack.sh" new >/dev/null   # back to the default checks
    fi
}

run_block() {  # version
    local v=$1 uc s
    for uc in "${use_cases[@]}"; do
        for s in $(selected "$uc"); do
            run_cell "$v" "$uc" "$s"
        done
    done

    if [[ " ${use_cases[*]} " == *" file_based "* ]] && [ -z "${CAMPAIGN_SCENARIOS:-}" ]; then
        (cd "$HERE" && "$PY" runner.py --version "$v" --scenario burst --reps 2 --stats \
            --use-case file_based --out "$OUT/resources") | tee -a "$OUT/campaign.log"
    fi
}

for b in $(seq 1 "$BLOCKS"); do
    if (( b % 2 )); then order=(old new); else order=(new old); fi
    for v in "${order[@]}"; do
        echo "### block $b: $v" | tee -a "$OUT/campaign.log"
        "$HERE/stack.sh" "$v" >/dev/null || exit 1
        run_block "$v"
    done
done

"$PY" "$HERE/analyze.py" "$OUT"
