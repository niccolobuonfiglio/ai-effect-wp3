#!/bin/bash
# Record the experimental environment into <out>/env.json.
#
#   evaluation/env_snapshot.sh evaluation/results/<run-id>
#
set -euo pipefail

OUT="${1:?usage: env_snapshot.sh <out-dir>}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NEW_ROOT="${EVAL_NEW_ROOT:-$(cd "$HERE/.." && pwd)}"
OLD_ROOT="${EVAL_OLD_ROOT:-$(cd "$NEW_ROOT/.." && pwd)/oldCode/ai-effect-wp3}"
T33_ROOT="${EVAL_T33_ROOT:-$(cd "$NEW_ROOT/.." && pwd)/ai-effect-task3-3-tests}"
mkdir -p "$OUT"

repo() {  # path -> {"path","commit","dirty"}
    local head dirty
    head=$(git -C "$1" rev-parse HEAD 2>/dev/null || echo unknown)
    dirty=$(git -C "$1" status --porcelain 2>/dev/null | grep -v '^??' | wc -l)
    jq -n --arg p "$1" --arg c "$head" --argjson d "$dirty" '{path:$p, commit:$c, modified_files:$d}'
}

images() {  
    docker images --no-trunc --format '{{.Repository}}:{{.Tag}} {{.ID}}' \
        | grep -E 'orch|file_based|germany|portugal|dutch|villas|redis|hai|tef' \
        | jq -R 'split(" ") | {(.[0]): .[1]}' | jq -s 'add // {}'
}

fixtures() {  
    find "$NEW_ROOT/use-cases/file_based_energy_pipeline/export" \
         "$NEW_ROOT/use-cases/germany-node/export" \
         "$NEW_ROOT/use-cases/germany-node/data" -maxdepth 1 -type f 2>/dev/null \
        | sort | xargs -r sha256sum \
        | jq -R 'split("  ") | {(.[1]): .[0]}' | jq -s 'add // {}'
}

jq -n \
    --arg taken "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --argjson new "$(repo "$NEW_ROOT")" \
    --argjson old "$(repo "$OLD_ROOT")" \
    --argjson t33 "$(repo "$T33_ROOT")" \
    --arg docker "$(docker version --format '{{.Server.Version}}')" \
    --arg compose "$(docker compose version --short)" \
    --arg pwsh "$(pwsh -NoLogo -NoProfile -Command '$PSVersionTable.PSVersion.ToString()' 2>/dev/null || echo none)" \
    --arg python "$(python3 --version 2>&1)" \
    --arg kernel "$(uname -sr)" \
    --arg cpu "$(lscpu | sed -n 's/^Model name:[[:space:]]*//p')" \
    --arg cores "$(nproc)" \
    --arg mem "$(free -g | awk '/^Mem:/{print $2" GiB"}')" \
    --argjson images "$(images)" \
    --argjson fixtures "$(fixtures)" \
    '{taken:$taken, repos:{new:$new, old:$old, t33:$t33},
      tools:{docker:$docker, compose:$compose, pwsh:$pwsh, python:$python},
      host:{kernel:$kernel, cpu:$cpu, cores:($cores|tonumber), memory:$mem},
      images:$images, fixtures:$fixtures}' > "$OUT/env.json"

echo "wrote $OUT/env.json"
jq '{repos, tools, host}' "$OUT/env.json"
