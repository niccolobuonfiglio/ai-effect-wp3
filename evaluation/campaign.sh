#!/bin/bash
# Layer 3 of the evaluation: every scenario, on both versions, in alternating blocks.
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
OUT="$(cd "$OUT" && pwd)"   # absolute: the runner is started from evaluation/
"$HERE/env_snapshot.sh" "$OUT" >/dev/null

# The runner sends no service key, so neither the orchestrators nor the
# pipeline may run with one: a key left in the shell would make every call fail
# on both versions. Starting the pipeline here also recreates it without one.
unset ORCHESTRATOR_API_KEY SERVICE_API_KEY ORCHESTRATOR_SECRET

# The file-based pipeline carries the fault scenarios; its services must be up.
UC="$HERE/../use-cases/file_based_energy_pipeline/docker-compose.yml"
docker compose -f "$UC" up -d
# V-A2 (outage during a long task) runs on the Germany node, the only use case
# with a task long enough to be interrupted (PROTOCOL.md, layer 3). Its ports
# (18091-18093) do not clash with the file-based pipeline's.
GERMANY="$HERE/../use-cases/germany-node/docker-compose.yml"
docker compose -f "$GERMANY" up -d

# NEW's lease is shortened for the worker-death scenario only; everything else
# runs with the shipped defaults of each version. CAMPAIGN_SCENARIOS selects a
# subset (space-separated), e.g. for a smoke run.
scenarios=(${CAMPAIGN_SCENARIOS:-nominal burst outage short-outage request-error concurrent-outage worker-death credential-misconfig corrupt-inline})

run_block() {  # version
    local v=$1
    for s in "${scenarios[@]}"; do
        [ "$s" = outage-during-task ] && continue   # Germany only, run below
        local reps=$REPS extra=()
        [ "$s" = nominal ] && { reps=$((REPS * 3)); extra=(--warmup 2); }
        [ "$s" = worker-death ] && extra=(--timeout 120)   # lease 20 s + slack; OLD never finishes
        if [ "$s" = worker-death ] && [ "$v" = new ]; then
            LEASE_SECONDS=20 "$HERE/stack.sh" new >/dev/null
        fi
        (cd "$HERE" && "$PY" runner.py --version "$v" --scenario "$s" --reps "$reps" \
            --use-case file_based --out "$OUT" "${extra[@]}") | tee -a "$OUT/campaign.log"
        if [ "$s" = worker-death ] && [ "$v" = new ]; then
            "$HERE/stack.sh" new >/dev/null   # back to the default lease
        fi
        # RQ6 attribution (PROTOCOL.md 8.4): the same nominal workload on NEW
        # with the submission-time checks switched off.
        if [ "$s" = nominal ] && [ "$v" = new ]; then
            SERVICE_AUTH_CHECK=false BLUEPRINT_TYPE_CHECK=off "$HERE/stack.sh" new >/dev/null
            (cd "$HERE" && "$PY" runner.py --version new --scenario nominal --variant checks-off \
                --reps "$reps" --use-case file_based --out "$OUT" "${extra[@]}") | tee -a "$OUT/campaign.log"
            "$HERE/stack.sh" new >/dev/null   # back to the default checks
        fi
    done
    # V-A2 on the Germany node. CAMPAIGN_SCENARIOS can leave it out by not
    # listing outage-during-task.
    if [ -z "${CAMPAIGN_SCENARIOS:-}" ] || [[ " $CAMPAIGN_SCENARIOS " == *" outage-during-task "* ]]; then
        (cd "$HERE" && "$PY" runner.py --version "$v" --scenario outage-during-task --reps "$REPS" \
            --use-case germany --out "$OUT") | tee -a "$OUT/campaign.log"
    fi
    # CPU/RAM are sampled in a separate burst run: "docker stats" itself costs
    # CPU and would bias the latency and throughput measured above.
    (cd "$HERE" && "$PY" runner.py --version "$v" --scenario burst --reps 2 --stats \
        --use-case file_based --out "$OUT/resources") | tee -a "$OUT/campaign.log"
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
