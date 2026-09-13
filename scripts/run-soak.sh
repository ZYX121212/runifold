#!/usr/bin/env bash
set -euo pipefail

runifold_soak_minutes="${RUNIFOLD_SOAK_MINUTES:-180}"
runifold_soak_output="${RUNIFOLD_SOAK_EVIDENCE_PATH:-target/reliability-evidence/soak.json}"

if [[ ! "$runifold_soak_minutes" =~ ^[0-9]+$ ]] ||
  (( runifold_soak_minutes < 5 || runifold_soak_minutes > 360 )); then
  echo "RUNIFOLD_SOAK_MINUTES must be an integer between 5 and 360" >&2
  exit 2
fi

runifold_soak_started="$(date +%s)"
runifold_soak_deadline="$((runifold_soak_started + runifold_soak_minutes * 60))"
runifold_soak_iterations=0
runifold_soak_suite="starting"
runifold_soak_log="$(mktemp)"

finish_soak() {
  local status=$?
  local result=passed
  if (( status != 0 )); then result=failed; fi
  python3 scripts/write-soak-evidence.py \
    --output "$runifold_soak_output" \
    --revision "${RUNIFOLD_EVIDENCE_REVISION:-unknown}" \
    --started "$runifold_soak_started" \
    --finished "$(date +%s)" \
    --iterations "$runifold_soak_iterations" \
    --result "$result" --exit-code "$status" \
    --suite "$runifold_soak_suite" --log "$runifold_soak_log"
  rm -f "$runifold_soak_log"
}
trap finish_soak EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

run_suite() {
  runifold_soak_suite="$1"
  shift
  "$@" 2>&1 | tee "$runifold_soak_log"
}

while (( $(date +%s) < runifold_soak_deadline )); do
  run_suite postgres cargo test -p runifold-store-postgres --tests --locked -- --test-threads=1
  run_suite sqlite-session cargo test -p runifold-store-sqlite --tests --locked
  run_suite effect cargo test -p runifold-effect --locked
  run_suite provider cargo test -p runifold-providers \
    --features anthropic,gemini,ollama,openai \
    --test openai_control_http \
    --test openai_reliability \
    --locked
  runifold_soak_iterations="$((runifold_soak_iterations + 1))"
done
runifold_soak_suite="complete"
