# AI coding evaluation suite

This suite measures task completion separately from compilation and architectural
correctness. It includes five frozen tasks, one deliberate cancellation defect,
a command grader and an initially empty failure ledger. No tool score is claimed.

## Run a trial

1. Pin a baseline commit and create an isolated candidate worktree. Record tool,
   exact model/version, condition, prompt, limits and trial number externally.
2. For `cancellation-repair`, run `python3 ai-evals/fixtures/prepare.py --candidate
   /absolute/candidate` and confirm `cargo test -p runifold-core --locked` fails.
3. Give the task JSON prompt to the selected coding agent in that candidate. Keep
   its trace/metrics. Do not change the grading task or canonical grader to help it pass.
4. Run the grader from the controlling checkout. Pass `--candidate`, pinned `--base`,
   `--task`, `--agent`, `--condition` and an `--output` outside the candidate. Without
   `--execute` it prints the plan and candidate fingerprint. With `--execute` it
   runs fixed validation commands and stores logs and hashes.
5. Have an independent reviewer inspect the patch against `review_rubric`, including
   task-specific assertions and any test weakening. Supply this JSON through `--review`:

```json
{
  "candidate_fingerprint": "fingerprint from plan",
  "reviewer": "reviewer identity",
  "architecture_correct": true,
  "task_satisfied": true,
  "no_test_weakening": true
}
```

Review cannot be reused for another candidate fingerprint. Missing review leaves
semantic grades null. A passing test command never automatically establishes task
success. A nonzero test exit makes task_success false. Logs stay outside the candidate
so grading does not alter its fingerprint.

Optional `--metrics` retains operator-supplied first-pass status, repair turns,
tool calls, tokens and elapsed agent time with a source hash. These are trace inputs,
not inferred by the grader. Test runtime, files touched and patch hashes are measured.

Use the same task set, base, limits and grading rubric for A/B trials. Report sample
counts and unknown grades; architecture correctness needs independent review. Capture
repeated discoverability/API/diagnostic failures in `failure-ledger.json`, with task,
agent, root cause, fix and regression reference. Do not enter hypothetical failures
as observed evidence.
