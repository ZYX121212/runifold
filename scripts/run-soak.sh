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

while (( $(date +%s) < runifold_soak_deadline )); do
  cargo test -p runifold-store-postgres --tests --locked -- --test-threads=1
  cargo test -p runifold-effect --locked
  cargo test -p runifold-providers \
    --features anthropic,gemini,ollama,openai \
    --test openai_control_http \
    --test openai_reliability \
    --locked
  runifold_soak_iterations="$((runifold_soak_iterations + 1))"
done

python3 scripts/write-soak-evidence.py \
  --output "$runifold_soak_output" \
  --revision "${RUNIFOLD_EVIDENCE_REVISION:-unknown}" \
  --started "$runifold_soak_started" \
  --finished "$(date +%s)" \
  --iterations "$runifold_soak_iterations"
