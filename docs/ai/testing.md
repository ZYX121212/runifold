# Testing AI-friendly changes

Run `python3 scripts/ai-knowledge.py --check` to verify generated knowledge and
local documentation links. CI separately executes every distinct recipe validation
command so source extraction cannot substitute for compilation/behavior checks.
`cargo check --example` checks a credential-requiring example without running it.

The recipe tests cover typed Tool schema/state/error conversion, local structured
validation, stream ordering/backpressure, reviewer gating, conversation semantics,
effect replay and native provider wire behavior. CLI integration tests cover JSON,
argument compatibility, Cargo input errors and context lookup.

Use the affected crate's guide for targeted tests. The full [testing guide](../TESTING.md)
and [CI](../../.github/workflows/ci.yml) define broader gates. PostgreSQL tests may
start disposable Docker containers. Live vendor canaries are ignored unless
explicitly selected; they are not necessary for AI recipe validation.

After a real AI coding failure, retain the failing task and expected behavior.
Add a regression at the responsible boundary, then update its API explanation,
recipe or diagnostic. Re-run representative application tasks from a clean project
when evaluating end-to-end DX; do not claim a model success rate from Rust tests.
