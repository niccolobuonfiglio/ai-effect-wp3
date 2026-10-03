#!/bin/bash
# Amendment 4 of the evaluation (PROTOCOL.md 11): scaling and data-size sweeps,
# on both versions, in alternating blocks.
#
#   evaluation/campaign_sweeps.sh <out-dir> [blocks] [reps-per-block]
#
# Default: 2 blocks x 3 repetitions = n=6 per point and version. Blocks
# alternate the order OLD/NEW, as in campaign.sh. File-based use case only:
# it is the only one that runs workflows concurrently, and the only one with
# an output oracle. SWEEPS selects sweeps (space-separated: scale inline data),
# e.g. for a pilot.
#
# Use a separate <out-dir> from campaign.sh: analyze.py treats the sweeps on
# their own, but keeping them apart keeps each run's env.json exact.
set -uo pipefail

OUT="${1:?usage: campaign_sweeps.sh <out-dir> [blocks] [reps-per-block]}"
BLOCKS="${2:-2}"
REPS="${3:-3}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PY="$HERE/.venv/bin/python"
mkdir -p "$OUT"
OUT="$(cd "$OUT" && pwd)"
"$HERE/env_snapshot.sh" "$OUT" >/dev/null

# As in campaign.sh: the runner sends no key, so none may be configured.
unset ORCHESTRATOR_API_KEY SERVICE_API_KEY ORCHESTRATOR_SECRET

# The points of each sweep (PROTOCOL.md 11.2 and 11.3), fixed before measuring.
WORKERS=(1 3 6)
WORKFLOWS=(1 5 10 25 50 100)
INLINE_BYTES=(0 1024 16384 65536 65537 262144 1048576)   # 64 KiB = NEW's limit
RECORDS=(10 100 1000 10000 100000)
sweeps=(${SWEEPS:-scale inline data})

# --build: the input provider must carry the num_records option (PROTOCOL.md 11.3)
docker compose -f "$HERE/../use-cases/file_based_energy_pipeline/docker-compose.yml" up -d --build

runner() {  # version scenario [runner args...]
    local v=$1 s=$2
    shift 2
    (cd "$HERE" && "$PY" runner.py --version "$v" --scenario "$s" --use-case file_based \
        --out "$OUT" "$@") | tee -a "$OUT/campaign.log"
}

run_block() {  # version
    local v=$1 w k b n
    for sweep in "${sweeps[@]}"; do
        case "$sweep" in
            scale)
                for w in "${WORKERS[@]}"; do
                    for k in "${WORKFLOWS[@]}"; do
                        # 1 warm-up per point; 100 workflows on 1 worker take minutes
                        runner "$v" scale --workers "$w" --workflows "$k" --reps "$REPS" \
                            --warmup 1 --timeout 900
                    done
                done
                "$HERE/stack.sh" "$v" >/dev/null   # back to the shipped 3 workers
                ;;
            inline)
                for b in "${INLINE_BYTES[@]}"; do
                    runner "$v" inline-size --size "$b" --reps "$REPS" --warmup 1
                done
                ;;
            data)
                for n in "${RECORDS[@]}"; do
                    runner "$v" data-size --records "$n" --reps "$REPS" --warmup 1 --timeout 600
                done
                ;;
            *) echo "unknown sweep: $sweep" >&2; exit 1 ;;
        esac
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
