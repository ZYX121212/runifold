#!/usr/bin/env bash
set -euo pipefail

runifold_evidence_path="${RUNIFOLD_EVIDENCE_PATH:-target/reliability-evidence/rich-artifacts.json}"
runifold_evidence_revision="${RUNIFOLD_EVIDENCE_REVISION:-unknown}"
runifold_started_at="$(date +%s)"

cargo test -p runifold-model --lib artifact::tests --locked
cargo test -p runifold-tool --lib output_contract_and_size_are_enforced_after_execution --locked
cargo test -p runifold-agent --lib agent_preserves_rich_and_structured_tool_output --locked
cargo test -p runifold-mcp --test mcp_tools \
  rich_tool_results_round_trip_through_mcp_without_text_flattening \
  --locked -- --exact
cargo test -p runifold-mcp --test mcp_sampling \
  tool_sampling_history_preserves_rich_result_extensions \
  --locked -- --exact
cargo test -p runifold-providers --all-features --lib --locked
cargo test -p runifold-store-sqlite --lib sqlite_artifacts_survive_idempotent_replay --locked
cargo test -p runifold-store-postgres --test live_conversation \
  transcript_summary_memory_and_concurrent_cas_survive_reconnect \
  --locked -- --exact

python3 scripts/write-rich-artifact-evidence.py \
  --output "$runifold_evidence_path" \
  --revision "$runifold_evidence_revision" \
  --started "$runifold_started_at" \
  --finished "$(date +%s)" \
  --rustc "$(rustc --version)"
