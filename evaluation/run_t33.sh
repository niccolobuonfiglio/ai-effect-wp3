#!/bin/bash
# Layer 1 of the evaluation: the independent Task 3.3 suites on OLD and NEW.
#
#   evaluation/run_t33.sh <german|portugal|dutch> <old|new> <out-dir> [script-number ...]
#   evaluation/run_t33.sh prepare-dutch

set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NEW_ROOT="${EVAL_NEW_ROOT:-$(cd "$HERE/.." && pwd)}"
T33_ROOT="${EVAL_T33_ROOT:-$(cd "$NEW_ROOT/.." && pwd)/ai-effect-task3-3-tests}"
DUTCH_FORK="${EVAL_DUTCH_FORK:-https://github.com/paulban/ai-effect-wp3.git}"
DUTCH_COMMIT=22fbb6eca71265a61bc1f0cee16b6d03f372d554

declare -A SCRIPTS=(
  [german]="00-Preflight-And-Dependency-Check.ps1
01-Start-Core-And-Germany-Services.ps1
02-Test-Authentication.ps1
11-Check-Packaged-Generator-Input.ps1
40-Test-Workflow-Definition-Validation.ps1
50-Test-Secret-Lifecycle.ps1
60-Check-VILLAS-Closure-Gate.ps1
99-Collect-Closeout-Evidence.ps1"
  [portugal]="00-Prepare-Portugal-Fixture.ps1
01-Start-Core-And-Portugal-Services.ps1
02-Test-Orchestrator-Authentication.ps1
03-Record-Knowledge-Store-Health-Configuration.ps1
04-Record-Synthetic-Data-Health-Configuration.ps1
10-Test-Portugal-Data-Load.ps1
11-Test-Portugal-Feature-Engineering.ps1
20-Test-Portugal-Workflow.ps1
22-Test-Portugal-Invalid-Input.ps1
23-Test-Repeatability-And-Performance.ps1 -Repetitions 3 -MaxRows 1000
24-Test-Restart-And-Rerun.ps1
25-Test-Bounded-Concurrency.ps1 -ParallelWorkflows 2 -MaxRows 1000
30-Prepare-Sidecar-Workspace.ps1
31-Start-Sidecar-Services.ps1
99-Collect-Closeout-Evidence.ps1
35-Stop-Portugal-Test-Services.ps1"
  [dutch]="00-Start-Core-Services.ps1
01-Test-Authentication.ps1
10-Test-Synthesizer.ps1
11-Validate-Synthesizer-Artifact.ps1
20-Test-Benchmark.ps1
21-Validate-Benchmark-Artifact.ps1
22-Test-Invalid-Algorithm.ps1
29-Prepare-HAI-Simulator.ps1
30-Attempt-HAI-Startup.ps1 -BuildImages
31-Test-HAI-Slot-Capacity.ps1
32-Test-HAI-Timeout-Reclaim.ps1
34-Test-HAI-Signed-Proxy-Link.ps1
35-Test-HAI-Technical-Lifecycle.ps1
36-Test-HAI-Restart-And-Rerun.ps1
40-Check-Studio-Manifest.ps1
50-Measure-Dutch-Reference.ps1 -Repetitions 3 -Workload Both"
)

prepare_dutch() {
    local work="$HOME/t33-work"
    mkdir -p "$work"
    if [ ! -d "$work/dutch-old/.git" ]; then
        git clone "$DUTCH_FORK" "$work/dutch-old"
    fi
    git -C "$work/dutch-old" checkout --quiet "$DUTCH_COMMIT"
    git -C "$work/dutch-old" submodule update --init --recursive --quiet

    rm -rf "$work/dutch-new"
    git clone --quiet "$work/dutch-old" "$work/dutch-new"
    git -C "$work/dutch-new" checkout --quiet -b eval-new-orchestrator "$DUTCH_COMMIT"
    git -C "$work/dutch-new" submodule update --init --recursive --quiet
    git -C "$work/dutch-new" rm -r --quiet orchestrator
    git -C "$NEW_ROOT" archive HEAD orchestrator | tar -x -C "$work/dutch-new"

    local compose="$work/dutch-new/orchestrator/docker-compose.yml"
    cp "$compose" "$work/compose.orig"
    sed -i -e 's/^  api:$/  api:\n    container_name: orchestrator-api/' \
           -e 's/"16379:6379"/"127.0.0.1:16379:6379"/' \
           -e 's/"18000:8000"/"127.0.0.1:18000:8000"/' "$compose"
    diff -u "$work/compose.orig" "$compose" >"$work/dutch-deployment.diff" || true
    rm -f "$work/compose.orig"

    git -C "$work/dutch-new" add orchestrator
    git -C "$work/dutch-new" -c user.name=evaluation -c user.email=evaluation@localhost \
        commit --quiet -m "evaluation: orchestrator replaced by NEW $(git -C "$NEW_ROOT" rev-parse --short HEAD)"


    git -C "$work/dutch-old" diff --stat "$DUTCH_COMMIT" "$(git -C "$work/dutch-new" rev-parse HEAD)" -- orchestrator \
        >"$work/dutch-orchestrator-swap.stat" 2>/dev/null || true
    echo "prepared $work/dutch-old ($DUTCH_COMMIT) and $work/dutch-new ($(git -C "$work/dutch-new" rev-parse HEAD))"
}

if [ "${1:-}" = "prepare-dutch" ]; then
    prepare_dutch
    exit 0
fi

NODE="${1:?node: german|portugal|dutch}"
VERSION="${2:?version: old|new}"
OUT="${3:?out-dir}"
shift 3
ONLY=("$@")
[ -n "${SCRIPTS[$NODE]:-}" ] || { echo "unknown node $NODE" >&2; exit 1; }


for key in ORCHESTRATOR_API_KEY SERVICE_API_KEY; do
    [ -n "${!key:-}" ] || { echo "export $key first (see the node's START-HERE.md)" >&2; exit 1; }
done
if [ "$NODE" = dutch ]; then
    [ -n "${HAI_SESSION_TOKEN_SECRET:-}" ] || {
        echo "export HAI_SESSION_TOKEN_SECRET first (dutch-node/START-HERE.md)" >&2; exit 1; }
    export NODE_PUBLIC_BASE_URL="${NODE_PUBLIC_BASE_URL:-http://localhost:8443}"
    export HAI_PUBLIC_BASE_URL="${HAI_PUBLIC_BASE_URL:-http://localhost:8443}"
    export HAI_PROXY_HOST_PORT="${HAI_PROXY_HOST_PORT:-8443}"
    export HAI_SESSION_SLOT_COUNT="${HAI_SESSION_SLOT_COUNT:-2}"
    export HAI_CAB_URL="${HAI_CAB_URL:-http://frontend:80}"
    export HAI_SIM_ENV_NAME="${HAI_SIM_ENV_NAME:-Ressources/env_ICAPS_input_data_test}"
fi

NODE_DIR="$T33_ROOT/$NODE-node"
TEMPLATE="$HERE/t33/$NODE-$VERSION.psd1"
mkdir -p "$OUT/t33-logs/$NODE-$VERSION"
OUT="$(cd "$OUT" && pwd)"
CSV="$OUT/t33.csv"
[ -f "$CSV" ] || echo "node,version,script,verdict,exit_code,started_at,seconds,message" >"$CSV"
git -C "$T33_ROOT" diff >"$OUT/t33-harness.diff"
git -C "$T33_ROOT" rev-parse HEAD >"$OUT/t33-commit.txt"

# --- settings 
repo_root=$(sed -n "s/^ *RepositoryRoot *= *'\(.*\)'.*/\1/p" "$TEMPLATE")
commit=$(git -C "$repo_root" rev-parse HEAD) || { echo "not a git repo: $repo_root" >&2; exit 1; }
SETTINGS="$NODE_DIR/Test-Settings.psd1"
BACKUP=""
if [ -f "$SETTINGS" ]; then
    BACKUP="$(mktemp)"
    cp "$SETTINGS" "$BACKUP"
fi
restore() {
    if [ -n "$BACKUP" ]; then mv "$BACKUP" "$SETTINGS"; else rm -f "$SETTINGS"; fi
}
trap restore EXIT
sed "s/ExpectedCommit\( *\)= *'HEAD'/ExpectedCommit\1= '$commit'/" "$TEMPLATE" >"$SETTINGS"
cp "$SETTINGS" "$OUT/t33-logs/$NODE-$VERSION/Test-Settings.psd1"
mkdir -p "$(sed -n "s/^ *EvidenceRoot *= *'\(.*\)'.*/\1/p" "$TEMPLATE")"

# --- stack 
if [ -n "$(git -C "$repo_root" status --porcelain)" ]; then
    echo "$repo_root is not clean; the T3.3 suites will refuse to run. Commit first:" >&2
    git -C "$repo_root" status --short >&2
    exit 1
fi
CORE="$repo_root/orchestrator/docker-compose.yml"
core() { docker compose -p orchestrator -f "$CORE" "$@"; }
restore() {
    if [ -n "$BACKUP" ]; then mv "$BACKUP" "$SETTINGS"; else rm -f "$SETTINGS"; fi
    if [ "$repo_root" != "$NEW_ROOT" ]; then
        echo "restoring NEW's orchestrator images"
        core down --remove-orphans >/dev/null 2>&1
        docker compose -p orchestrator -f "$NEW_ROOT/orchestrator/docker-compose.yml" build >/dev/null
    fi
}
"$HERE/stack.sh" down >/dev/null 2>&1
core up -d --build || exit 1
for _ in $(seq 1 60); do curl -sf "${ORCHESTRATOR_URL:-http://localhost:18000}/health" >/dev/null && break; sleep 1; done
live=$("$HERE/stack.sh" which)
case "$NODE-$VERSION" in
    dutch-*) ;;  # the fork has its own API surface; checked by the suite itself
    *) [ "$live" = "$VERSION" ] || { echo "expected $VERSION to be live, found $live" >&2; exit 1; } ;;
esac
echo "live: $live (project orchestrator, built from $repo_root @ ${commit:0:7})"

prime_service_key() {  # log
    local api="${ORCHESTRATOR_URL:-http://localhost:18000}" export="$repo_root/use-cases/germany-node/export"
    local payload wf status="" t
    # VILLASnode crashes on its second restart, and
    # script 01 has just restarted it: start from a fresh container.
    docker compose -f "$repo_root/use-cases/germany-node/docker-compose.yml" \
        up -d --force-recreate villas-node >/dev/null 2>&1
    sleep 3
    payload=$(jq -n --argjson b "$(cat "$export/blueprint.json")" --argjson d "$(cat "$export/dockerinfo.json")" \
        --arg k "${SERVICE_API_KEY:?SERVICE_API_KEY must be set for script 50}" \
        '{blueprint:$b, dockerinfo:$d, services_api_key:$k}')
    wf=$(curl -s -X POST "$api/workflows" -H "Authorization: Bearer ${ORCHESTRATOR_API_KEY:-}" \
        -H 'Content-Type: application/json' -d "$payload" | jq -r .workflow_id)
    echo "    preparing script 50: workflow $wf submitted, waiting up to 300 s for it to finish"
    for t in $(seq 1 300); do
        status=$(curl -s "$api/workflows/$wf" -H "Authorization: Bearer ${ORCHESTRATOR_API_KEY:-}" | jq -r .status)
        case "$status" in completed|failed) break ;; esac
        (( t % 15 == 0 )) && echo "    ... ${t}s, status: $status"
        sleep 1
    done
    echo "primed script 50 with workflow $wf (final status: $status)" | tee "$1"
}

# --- scripts 
while IFS= read -r line; do
    [ -z "$line" ] && continue
    script="${line%% *}"
    args=()
    [ "$line" != "$script" ] && read -r -a args <<<"${line#* }"
    num="${script%%-*}"
    if [ ${#ONLY[@]} -gt 0 ] && [[ ! " ${ONLY[*]} " =~ " $num " ]]; then
        continue
    fi
    log="$OUT/t33-logs/$NODE-$VERSION/$script.log"
    if [ "$NODE-$num" = "german-50" ]; then
        prime_service_key "$OUT/t33-logs/$NODE-$VERSION/50-prime.log"
    fi
    started=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    t0=$(date +%s)
    echo "=== $NODE $VERSION $script ${args[*]}"
    (cd "$NODE_DIR" && NO_COLOR=1 pwsh -NoLogo -NoProfile -File "./$script" "${args[@]}" </dev/null) 2>&1 \
        | sed 's/\x1b\[[0-9;]*m//g' >"$log"
    code=${PIPESTATUS[0]}
    secs=$(( $(date +%s) - t0 ))
    result=$(grep -a 'RESULT:' "$log" | tail -n 1 | sed 's/.*RESULT:[[:space:]]*//')
    if [ -z "$result" ]; then
        verdict="NO RESULT"
        message="$(grep -av '^\s*$' "$log" | tail -n 1)"
    elif [[ "$result" =~ ^([A-Z][A-Z\ ]*[A-Z])[[:space:]]+(-|—)[[:space:]]+(.*)$ ]]; then
        verdict="${BASH_REMATCH[1]}"
        message="${BASH_REMATCH[3]}"
    elif [[ "$result" =~ ^[A-Z][A-Z\ ]*[A-Z]\.?$ ]]; then
        verdict="${result%.}"
        message=""
    else
        verdict="UNLABELLED"
        message="$result"
    fi
    printf '%s,%s,%s,"%s",%s,%s,%s,"%s"\n' "$NODE" "$VERSION" "$script" "${verdict//\"/\'}" \
        "$code" "$started" "$secs" "${message//\"/\'}" >>"$CSV"
    echo "    -> $verdict (exit $code, ${secs}s)"
done <<<"${SCRIPTS[$NODE]}"

echo "verdicts appended to $CSV"
