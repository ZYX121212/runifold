#!/usr/bin/env python3
"""Write fail-closed, source-bound evidence; short runs are never release acceptance."""
import argparse
from collections import deque
import json
import math
import os
from pathlib import Path
import re
import tempfile


def identity(path):
    source = Path(path)
    if source.stat().st_size > 65536:
        raise ValueError("source identity exceeds the inspection bound")
    value = json.loads(source.read_text())
    if value.get("schema_version") != 1:
        raise ValueError("unsupported source identity schema")
    for key, sizes in [("revision", (40, 64)), ("source_sha256", (64,))]:
        text = value.get(key, "")
        if not isinstance(text, str) or len(text) not in sizes or not re.fullmatch(r"[0-9a-f]+", text):
            raise ValueError("invalid source identity")
    if type(value.get("source_files")) is not int or value["source_files"] < 1:
        raise ValueError("invalid source identity")
    return {key: value[key] for key in ("revision", "source_sha256", "source_files")}


def main():
    parser = argparse.ArgumentParser()
    for name in ("output", "revision", "started", "finished", "iterations"):
        parser.add_argument("--" + name, required=True, type=int if name in ("started", "finished", "iterations") else str)
    parser.add_argument("--result", choices=["passed", "failed"], default="passed")
    parser.add_argument("--exit-code", type=int, default=0)
    parser.add_argument("--suite", default="complete")
    parser.add_argument("--log")
    parser.add_argument("--requested-minutes", type=int, default=180)
    parser.add_argument("--elapsed-seconds", type=float)
    parser.add_argument("--source-before")
    parser.add_argument("--source-after")
    args = parser.parse_args()
    output = Path(args.output)
    output.unlink(missing_ok=True)
    report = {"schema_version": 3, "suite": "runifold.reliability-soak", "result": "failed",
              "release_eligible": False, "exit_code": args.exit_code,
              "iterations": args.iterations, "credential_material": "excluded", "model_content": "excluded"}
    boundary = "argument_consistency"
    successful = False
    try:
        elapsed = args.elapsed_seconds if args.elapsed_seconds is not None else args.finished - args.started
        if not 5 <= args.requested_minutes <= 360 or not math.isfinite(elapsed) or elapsed < 0 or args.finished < args.started:
            raise ValueError("invalid duration")
        if args.iterations < 0 or (args.exit_code == 0) != (args.result == "passed"):
            raise ValueError("invalid completion")
        boundary = "source_identity"
        before, after = identity(args.source_before), identity(args.source_after)
        report["source"] = before
        report["source_unchanged"] = before == after
        if before != after or args.revision != before["revision"]:
            raise ValueError("source changed or revision disagrees")
        boundary = "completed_suites"
        failures, summaries, summary_count, has_failures = [], deque(maxlen=128), 0, False
        resources = {"samples": 0, "requests": 0, "physical_attempts": 0, "maximum_resident_growth_kib": 0, "peak_concurrency": 0}
        if args.log:
            with Path(args.log).open(errors="replace") as log:
                while line := log.readline(8192):
                    # Serial libtest writes the test label before uncaptured
                    # stdout. Accept that bounded framing as well as plain JSON.
                    observation = re.sub(r"^test [A-Za-z0-9_:]{1,256} \.\.\. (?=\{)", "", line)
                    if observation.startswith("{"):
                        try:
                            sample = json.loads(observation)
                        except (ValueError, TypeError):
                            sample = None
                        if isinstance(sample, dict) and sample.get("suite") == "request_stability":
                            keys = ("requests", "physical_attempts", "peak_concurrency", "resident_baseline_kib", "resident_peak_kib", "maximum_resident_growth_kib")
                            if sample.get("result") != "passed" or not all(type(sample.get(key)) is int and 0 <= sample[key] < 2**63 for key in keys):
                                raise ValueError("invalid resource observation")
                            growth = sample["resident_peak_kib"] - sample["resident_baseline_kib"]
                            if not (sample["requests"] >= 16000 and 0 < sample["physical_attempts"] <= sample["requests"]
                                    and 0 < sample["peak_concurrency"] <= 4 and 0 <= growth <= sample["maximum_resident_growth_kib"] <= 8192):
                                raise ValueError("resource acceptance bound exceeded")
                            resources["samples"] += 1
                            resources["requests"] += sample["requests"]
                            resources["physical_attempts"] += sample["physical_attempts"]
                            resources["maximum_resident_growth_kib"] = max(resources["maximum_resident_growth_kib"], growth)
                            resources["peak_concurrency"] = max(resources["peak_concurrency"], sample["peak_concurrency"])
                    failure = re.fullmatch(r"test ([A-Za-z0-9_:]{1,256}) \.\.\. FAILED\n?", line)
                    if failure and len(failures) < 128:
                        failures.append(failure.group(1))
                    summary = re.match(r"test result: (ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored;", line)
                    if summary:
                        summaries.append(summary.group(0))
                        summary_count += 1
                        has_failures |= summary.group(1) != "ok" or int(summary.group(3)) != 0
        report.update({"failed_tests": failures, "test_summaries": list(summaries), "test_summary_count": summary_count,
                       "duration_seconds": elapsed, "requested_duration_seconds": args.requested_minutes * 60,
                       "acceptance_profile": "release" if args.requested_minutes >= 180 else "short_run",
                       "last_suite": args.suite if args.suite in ("starting", "postgres", "sqlite-session", "effect", "model", "request-stability", "provider", "complete") else "unknown"})
        if args.result != "passed" or failures or has_failures or args.suite != "complete" or args.iterations < 1 or not summaries:
            raise ValueError("suite did not complete")
        boundary = "request_resource_observations"
        report["request_resources"] = resources
        if resources["samples"] != args.iterations:
            raise ValueError("each completed iteration requires one bounded resource observation")
        boundary = "requested_duration"
        if elapsed < args.requested_minutes * 60:
            raise ValueError("run shorter than requested")
        report["result"] = "passed"
        report["release_eligible"] = args.requested_minutes >= 180 and elapsed >= 180 * 60
        report["boundaries"] = ["postgres_durability_restart_and_leases", "sqlite_sessions_and_process_recovery",
                                "effect_ambiguity_and_reconciliation", "model_request_lifecycle_and_capacity",
                                "provider_http_stream_and_continuation_faults"]
        successful = True
    except Exception:
        report["failed_boundary"] = boundary
    # Always replace stale pass evidence, including invalid source/clock inputs.
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", dir=output.parent, delete=False) as stream:
        json.dump(report, stream, indent=2)
        stream.write("\n")
        temporary = stream.name
    os.replace(temporary, output)
    if not successful:
        raise SystemExit("Soak evidence rejected; inspect the failed_boundary field")


if __name__ == "__main__":
    main()
