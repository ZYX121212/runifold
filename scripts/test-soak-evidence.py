#!/usr/bin/env python3
"""Evidence regression tests; synthetic clocks never claim an actual soak run."""
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parent.parent


class SoakEvidenceTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.output = self.root / "evidence.json"
        self.before = self.root / "before.json"
        self.after = self.root / "after.json"
        self.identity = {"schema_version": 1, "revision": "a" * 40, "source_sha256": "b" * 64, "source_files": 3}
        for path in (self.before, self.after):
            path.write_text(json.dumps(self.identity))
        self.log = self.root / "tests.log"
        self.resource = {"suite":"request_stability", "result":"passed", "requests":16896, "physical_attempts":14454, "peak_concurrency":4,
                         "resident_baseline_kib":1000, "resident_peak_kib":1040, "maximum_resident_growth_kib":8192}
        self.log.write_text(json.dumps(self.resource) + "\nprivate prompt must not be exported\ntest result: ok. 3 passed; 0 failed; 0 ignored;\n")

    def run_writer(self, minutes=5, elapsed=300, extra=()):
        self.output.write_text('{"result":"passed","revision":"stale"}')
        process = subprocess.run([sys.executable, str(ROOT / "scripts/write-soak-evidence.py"),
            "--output", str(self.output), "--revision", "a" * 40,
            "--started", "1000", "--finished", str(1000 + elapsed),
            "--iterations", "1", "--suite", "complete", "--log", str(self.log),
            "--source-before", str(self.before), "--source-after", str(self.after),
            "--requested-minutes", str(minutes), "--elapsed-seconds", str(elapsed), *extra],
            capture_output=True, check=False)
        return process, json.loads(self.output.read_text())

    def test_short_run_pass_is_not_release_acceptance(self):
        process, report = self.run_writer()
        self.assertEqual(process.returncode, 0)
        self.assertEqual(report["result"], "passed")
        self.assertEqual(report["acceptance_profile"], "short_run")
        self.assertFalse(report["release_eligible"])
        self.assertNotIn("private prompt", json.dumps(report))

    def test_serial_libtest_prefix_preserves_bounded_resource_observations(self):
        self.log.write_text("test repeated_failures_cancellations_and_eof_release_capacity_without_unbounded_growth ... "
                            + json.dumps(self.resource) + "\nok\ntest result: ok. 1 passed; 0 failed; 0 ignored;\n")
        process, report = self.run_writer()
        self.assertEqual(process.returncode, 0)
        self.assertEqual(report["request_resources"]["samples"], 1)
        self.assertEqual(report["request_resources"]["requests"], 16896)
        self.assertEqual(report["request_resources"]["maximum_resident_growth_kib"], 40)
        self.assertFalse(report["release_eligible"])
        self.resource["resident_peak_kib"] = 100000
        self.log.write_text("test fixture ... " + json.dumps(self.resource)
                            + "\nok\ntest result: ok. 1 passed; 0 failed; 0 ignored;\n")
        process, report = self.run_writer()
        self.assertNotEqual(process.returncode, 0)
        self.assertEqual(report["result"], "failed")

    def test_premature_release_run_replaces_stale_pass(self):
        process, report = self.run_writer(minutes=180, elapsed=300)
        self.assertNotEqual(process.returncode, 0)
        self.assertEqual(report["result"], "failed")
        self.assertEqual(report["failed_boundary"], "requested_duration")
        self.assertFalse(report["release_eligible"])

    def test_source_changes_and_unknown_revisions_fail_closed(self):
        self.after.write_text(json.dumps(dict(self.identity, source_sha256="c" * 64)))
        process, report = self.run_writer()
        self.assertNotEqual(process.returncode, 0)
        self.assertEqual(report["failed_boundary"], "source_identity")
        self.before.write_text('{"revision":"unknown"}')
        process, report = self.run_writer()
        self.assertNotEqual(process.returncode, 0)
        self.assertEqual(report["result"], "failed")

    def test_failed_summary_is_rejected_even_without_a_named_failure(self):
        self.log.write_text("test result: FAILED. 1 passed; 1 failed; 0 ignored;\nprivate-secret\n")
        process, report = self.run_writer()
        self.assertNotEqual(process.returncode, 0)
        self.assertEqual(report["result"], "failed")
        self.assertNotIn("private-secret", json.dumps(report))

    def test_inconsistent_arguments_and_missing_sources_replace_stale_pass(self):
        for extra in [("--result", "failed"), ("--iterations", "0"), ("--revision", "unknown")]:
            process, report = self.run_writer(extra=extra)
            self.assertNotEqual(process.returncode, 0)
            self.assertEqual(report["result"], "failed")
            self.assertFalse(report["release_eligible"])
        self.before.unlink()
        process, report = self.run_writer()
        self.assertNotEqual(process.returncode, 0)
        self.assertEqual(report["failed_boundary"], "source_identity")

    def test_nonfinite_clocks_and_missing_test_summaries_are_rejected(self):
        process, report = self.run_writer(extra=("--elapsed-seconds", "nan"))
        self.assertNotEqual(process.returncode, 0)
        self.assertEqual(report["result"], "failed")
        self.log.write_text("no test executed\n")
        process, report = self.run_writer()
        self.assertNotEqual(process.returncode, 0)
        self.assertFalse(report["release_eligible"])

    def test_missing_or_excessive_resource_observations_cannot_pass(self):
        self.log.write_text("test result: ok. 3 passed; 0 failed; 0 ignored;\n")
        process, report = self.run_writer()
        self.assertNotEqual(process.returncode, 0)
        self.assertEqual(report["failed_boundary"], "request_resource_observations")
        self.resource["resident_peak_kib"] = 100000
        self.log.write_text(json.dumps(self.resource) + "\ntest result: ok. 3 passed; 0 failed; 0 ignored;\n")
        process, report = self.run_writer()
        self.assertNotEqual(process.returncode, 0)
        self.assertEqual(report["result"], "failed")

    def test_invalid_runner_duration_cannot_preserve_a_previous_pass(self):
        import os
        self.output.write_text('{"result":"passed"}')
        process = subprocess.run(["bash", str(ROOT / "scripts/run-soak.sh")], cwd=ROOT,
            env=dict(os.environ, RUNIFOLD_SOAK_MINUTES="0", RUNIFOLD_SOAK_EVIDENCE_PATH=str(self.output)),
            capture_output=True, check=False)
        self.assertNotEqual(process.returncode, 0)
        self.assertFalse(self.output.exists())


class SourceIdentityTest(unittest.TestCase):
    def setUp(self):
        import importlib.util
        spec = importlib.util.spec_from_file_location("runifold_source_identity", ROOT / "scripts/source-identity.py")
        self.module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.module)
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        (self.root / "crates").mkdir()
        (self.root / "Cargo.toml").write_text("[workspace]\n")
        (self.root / "crates/source.rs").write_text("pub fn initial() {}\n")
        subprocess.run(["git", "init", "-q", str(self.root)], check=True)
        subprocess.run(["git", "-C", str(self.root), "add", "."], check=True)
        subprocess.run(["git", "-C", str(self.root), "-c", "user.name=fixture", "-c", "user.email=fixture@example.invalid", "-c", "core.hooksPath=/dev/null", "commit", "-q", "--no-gpg-sign", "-m", "fixture"], check=True)

    def test_tracked_and_untracked_code_changes_alter_the_identity(self):
        initial = self.module.capture(self.root)
        (self.root / "crates/new.rs").write_text("pub fn added() {}\n")
        added = self.module.capture(self.root)
        self.assertNotEqual(initial["source_sha256"], added["source_sha256"])
        (self.root / "crates/new.rs").unlink()
        (self.root / "crates/source.rs").write_text("pub fn changed() {}\n")
        self.assertNotEqual(initial["source_sha256"], self.module.capture(self.root)["source_sha256"])

    def test_tracked_release_policy_outside_source_directories_is_bound(self):
        policy = self.root / "release-policy.toml"
        policy.write_text("strict=true\n")
        subprocess.run(["git", "-C", str(self.root), "add", "release-policy.toml"], check=True)
        before = self.module.capture(self.root)
        policy.write_text("strict=false\n")
        self.assertNotEqual(before["source_sha256"], self.module.capture(self.root)["source_sha256"])

    def test_private_environment_files_and_python_caches_are_not_collected(self):
        initial = self.module.capture(self.root)
        (self.root / "crates/.env").write_text("PRIVATE=do-not-collect\n")
        (self.root / "scripts/__pycache__").mkdir(parents=True)
        (self.root / "scripts/__pycache__/cache.pyc").write_bytes(b"cache")
        after = self.module.capture(self.root)
        self.assertEqual(initial["source_sha256"], after["source_sha256"])
        self.assertNotIn("do-not-collect", json.dumps(after))

    @unittest.skipUnless(sys.platform != "win32", "Unix source symlink contract")
    def test_symlinks_cannot_extend_the_source_boundary(self):
        (self.root / "crates/escape.rs").symlink_to("/etc/hosts")
        with self.assertRaises(ValueError):
            self.module.capture(self.root)


if __name__ == "__main__":
    unittest.main()
