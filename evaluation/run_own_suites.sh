#!/bin/bash
# Layer 2 of the evaluation: own test suites, crossed with both code versions.
#
#   evaluation/run_own_suites.sh <out-dir> [levels]     levels default: "unit integration"
#
#   cell               tests from   code under test   answers
#   old-on-old         OLD          OLD               baseline suite size, pass rate, coverage
#   new-on-new         NEW          NEW               same for NEW
#   old-on-new         OLD          NEW               RQ7 non-regression: does NEW keep OLD's contract?
#   diff-on-old        differential OLD               should FAIL: the defects exist in OLD
#   diff-on-new        differential NEW               should PASS: the defects are fixed
#
# Every cell runs with the same interpreter (orchestrator/.venv), so the only
# difference between cells is the code. Results: <out>/suites/<cell>/junit.xml,
# coverage.json and pytest.log. Integration tests need Docker (testcontainers).
set -uo pipefail

# Both versions read these at import time; a key left in the shell turns every
# test that expects an open API into a 401.
unset ORCHESTRATOR_API_KEY SERVICE_API_KEY ORCHESTRATOR_SECRET

OUT="${1:?usage: run_own_suites.sh <out-dir> [levels]}"
LEVELS="${2:-unit integration}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NEW_ROOT="${EVAL_NEW_ROOT:-$(cd "$HERE/.." && pwd)}"
OLD_ROOT="${EVAL_OLD_ROOT:-$(cd "$NEW_ROOT/.." && pwd)/oldCode/ai-effect-wp3}"
PY="${EVAL_PYTHON:-$NEW_ROOT/orchestrator/.venv/bin/python}"
WORK="$OUT/suites/work"
# OLD's integration tests start their test service on fixed ports 18081/18082
# (NEW picks free ports). If a use-case stack holds them, OLD's tests talk to
# the wrong service and fail for reasons unrelated to the code.
busy=$(ss -ltnH 2>/dev/null | awk '{print $4}' | grep -oE ':(1808[1-4])$' | sort -u | tr '\n' ' ')
if [ -n "$busy" ]; then
    echo "ports in use:$busy - stop the use-case stacks first, e.g." >&2
    echo "  docker compose -f use-cases/file_based_energy_pipeline/docker-compose.yml stop" >&2
    exit 1
fi

mkdir -p "$WORK"
OUT="$(cd "$OUT" && pwd)"
WORK="$(cd "$WORK" && pwd)"

# A throwaway tree: tests from one version, src (symlinked) from the other.
# The suites' conftest puts ../src on sys.path, so this is all it takes.
tree() {  # name tests_root src_root
    local dir="$WORK/$1"
    rm -rf "$dir" && mkdir -p "$dir"
    cp -r "$2/orchestrator/tests" "$dir/tests"
    cp "$2/orchestrator/pyproject.toml" "$dir/"
    ln -s "$3/orchestrator/src" "$dir/src"
    find "$dir/tests" -name __pycache__ -prune -exec rm -rf {} +
    echo "$dir"
}

cell() {  # name dir paths...
    local name="$1" dir="$2"; shift 2
    local res="$OUT/suites/$name"
    mkdir -p "$res"
    echo "=== $name"
    (cd "$dir" && PYTHONPATH="$dir/src" "$PY" -m pytest "$@" -q -p no:cacheprovider \
        --junitxml="$res/junit.xml" \
        --cov=src --cov-branch --cov-report="json:$res/coverage.json" --cov-report=term \
        >"$res/pytest.log" 2>&1)
    echo "exit=$?" >>"$res/pytest.log"
    tail -n 3 "$res/pytest.log"
}

paths() { for l in $LEVELS; do printf 'tests/%s ' "$l"; done; }

cell old-on-old "$(tree old-on-old "$OLD_ROOT" "$OLD_ROOT")" $(paths)
cell new-on-new "$(tree new-on-new "$NEW_ROOT" "$NEW_ROOT")" $(paths)
cell old-on-new "$(tree old-on-new "$OLD_ROOT" "$NEW_ROOT")" $(paths)

for v in old new; do
    root=$OLD_ROOT; [ $v = new ] && root=$NEW_ROOT
    dir="$WORK/diff-on-$v"
    rm -rf "$dir" && mkdir -p "$dir"
    cp "$HERE/differential/test_differential.py" "$dir/"
    ln -s "$root/orchestrator/src" "$dir/src"
    mkdir -p "$OUT/suites/diff-on-$v"
    # Record measured rates (fan-in race) next to the junit results.
    export DIFF_OUT="$OUT/suites/diff-on-$v"
    cell "diff-on-$v" "$dir" test_differential.py
done

echo
echo "Results in $OUT/suites; summarise with: $PY $HERE/analyze.py $OUT"
