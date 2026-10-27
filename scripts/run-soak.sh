#!/usr/bin/env bash
set -euo pipefail

runifold_soak_minutes="${RUNIFOLD_SOAK_MINUTES:-180}"
runifold_soak_output="${RUNIFOLD_SOAK_EVIDENCE_PATH:-target/reliability-evidence/soak.json}"
# Never preserve an earlier pass after a failed prerequisite or invalid duration.
rm -f "$runifold_soak_output"
if [[ ! "$runifold_soak_minutes" =~ ^[0-9]+$ ]] ||
  (( runifold_soak_minutes < 5 || runifold_soak_minutes > 360 )); then
  echo "RUNIFOLD_SOAK_MINUTES must be an integer between 5 and 360" >&2
  exit 2
fi
runifold_soak_before="$(mktemp)"
runifold_soak_after="$(mktemp)"
runifold_soak_log="$(mktemp)"
runifold_soak_started="$(date +%s)"
runifold_soak_clock="$(python3 -c 'import time; print(time.monotonic())')"
runifold_soak_iterations=0
runifold_soak_suite="starting"
runifold_soak_revision="unknown"
elapsed_soak() {
  python3 -c 'import sys,time; print(int(time.monotonic()-float(sys.argv[1])))' "$runifold_soak_clock"
}
finish_soak() {
  local status=$?
  local result=passed
  if (( status != 0 )); then result=failed; fi
  python3 scripts/source-identity.py > "$runifold_soak_after" || status=1
  if (( status != 0 )); then result=failed; fi
  python3 scripts/write-soak-evidence.py \
    --output "$runifold_soak_output" --revision "$runifold_soak_revision" \
    --started "$runifold_soak_started" --finished "$(date +%s)" \
    --elapsed-seconds "$(elapsed_soak)" --requested-minutes "$runifold_soak_minutes" \
    --iterations "$runifold_soak_iterations" --result "$result" --exit-code "$status" \
    --suite "$runifold_soak_suite" --log "$runifold_soak_log" \
    --source-before "$runifold_soak_before" --source-after "$runifold_soak_after" || { if (( status == 0 )); then status=1; fi; }
  rm -f "$runifold_soak_log" "$runifold_soak_before" "$runifold_soak_after"
  trap - EXIT
  exit "$status"
}
trap finish_soak EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
python3 scripts/source-identity.py > "$runifold_soak_before"
runifold_soak_revision="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["revision"])' "$runifold_soak_before")"
if [[ -n "${RUNIFOLD_EVIDENCE_REVISION:-}" && "$RUNIFOLD_EVIDENCE_REVISION" != "$runifold_soak_revision" ]]; then
  echo "Requested evidence revision does not match the tested checkout" >&2
  exit 2
fi
run_suite() {
  runifold_soak_suite="$1"
  shift
  "$@" 2>&1 | tee -a "$runifold_soak_log"
}
while (( $(elapsed_soak) < runifold_soak_minutes * 60 )); do
  run_suite postgres cargo test -p runifold-store-postgres --tests --locked -- --test-threads=1
  run_suite sqlite-session cargo test -p runifold-store-sqlite --tests --locked
  run_suite effect cargo test -p runifold-effect --locked
  run_suite model cargo test -p runifold-model scheduling:: --locked
  run_suite request-stability cargo test -p runifold --test request_stability --locked -- --nocapture
  run_suite provider cargo test -p runifold-providers \
    --features anthropic,gemini,ollama,openai-realtime \
    --test openai_control_http --test openai_reliability --test error_privacy \
    --test openai_continuation_http --test openai_stream_consistency --test openai_responses_ws --locked
  runifold_soak_iterations="$((runifold_soak_iterations + 1))"
done
runifold_soak_suite="complete"
