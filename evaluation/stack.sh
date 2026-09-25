#!/bin/bash
# Switch the live orchestrator between OLD (486ccc7) and NEW (HEAD).
#
#   evaluation/stack.sh old|new [--build]   bring that version up (the other goes down)
#   evaluation/stack.sh down                 stop both
#   evaluation/stack.sh which                print the live version
#
# Both stacks bind port 18000, so only one runs at a time. OLD uses the compose
# project "orch-old" so that it never overwrites NEW's containers and images.
# Each switch removes the previous Redis container: every block of trials
# starts from an empty state. Environment variables such as LEASE_SECONDS or
# ORCHESTRATOR_API_KEY are passed through to docker compose.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NEW_ROOT="${EVAL_NEW_ROOT:-$(cd "$HERE/.." && pwd)}"
OLD_ROOT="${EVAL_OLD_ROOT:-$(cd "$NEW_ROOT/.." && pwd)/oldCode/ai-effect-wp3}"
API="${ORCHESTRATOR_URL:-http://localhost:18000}"

old() { docker compose -p orch-old -f "$OLD_ROOT/orchestrator/docker-compose.yml" "$@"; }
new() { docker compose -p orchestrator -f "$NEW_ROOT/orchestrator/docker-compose.yml" "$@"; }

which_live() {
    local code auth=()
    [ -n "${ORCHESTRATOR_API_KEY:-}" ] && auth=(-H "Authorization: Bearer $ORCHESTRATOR_API_KEY")
    code=$(curl -s -o /dev/null -w '%{http_code}' "${auth[@]}" "$API/orchestrator/health/services" || true)
    case "$code" in
        000) echo none ;;
        404) echo old ;;
        *)   echo new ;;
    esac
}

wait_up() {
    for _ in $(seq 1 60); do
        curl -sf "$API/health" >/dev/null && return 0
        sleep 1
    done
    echo "orchestrator did not come up on $API" >&2
    return 1
}

docker network inspect ai-effect-services &>/dev/null || docker network create ai-effect-services >/dev/null

build=()
[ "${2:-}" = "--build" ] && build=(--build)

case "${1:-}" in
    old)
        new down --remove-orphans
        old up -d "${build[@]}"
        wait_up
        ;;
    new)
        old down --remove-orphans
        new up -d "${build[@]}"
        wait_up
        ;;
    down)
        old down --remove-orphans
        new down --remove-orphans
        exit 0
        ;;
    which)
        which_live
        exit 0
        ;;
    *)
        sed -n '2,12p' "$0"
        exit 1
        ;;
esac

live=$(which_live)
if [ "$live" != "$1" ]; then
    echo "expected $1 to be live, found $live" >&2
    exit 1
fi
echo "live: $live"
