#!/usr/bin/env python3
"""Check release-baseline selection against real branching Git history."""
import os
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


class SemverBaselineTest(unittest.TestCase):
    def test_unmerged_release_is_excluded_and_explicit_baseline_is_preserved(self):
        with tempfile.TemporaryDirectory(prefix="runifold-semver-") as directory:
            root = Path(directory)
            scripts = root / "scripts"
            scripts.mkdir()
            shutil.copy2(Path(__file__).with_name("semver-check.sh"), scripts)
            binary = root / "bin"
            binary.mkdir()
            cargo = binary / "cargo"
            cargo.write_text('''#!/bin/sh
if [ "$1" = metadata ]; then
  printf '%s\\n' "$SEMVER_TEST_METADATA"
else
  printf '%s\\n' "$@" > "$SEMVER_TEST_ARGUMENTS"
  printf '%s\\n' "$CARGO_TARGET_DIR" > "$SEMVER_TEST_TARGET_CAPTURE"
fi
''')
            cargo.chmod(0o755)
            def git(*args):
                return subprocess.check_output(["git", "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", *args], cwd=root, stderr=subprocess.DEVNULL).decode().strip()
            git("init", "--quiet")
            (root / "input").write_text("first")
            git("add", "input")
            git("commit", "--quiet", "-m", "first")
            git("tag", "v0.10.0")
            (root / "input").write_text("current")
            git("commit", "--quiet", "-am", "current")
            git("tag", "v0.10.1")
            current = git("rev-parse", "HEAD")
            (root / "input").write_text("later")
            git("commit", "--quiet", "-am", "later")
            git("tag", "v0.10.3")
            git("checkout", "--quiet", "--detach", current)
            arguments = root / "arguments"
            resolved_target = root / "fixture build"
            target_capture = root / "target-capture"
            metadata = {"packages": [{"name": "fixture", "version": "0.10.1"}], "target_directory": str(resolved_target)}
            environment = dict(os.environ, PATH=str(binary) + os.pathsep + os.environ["PATH"],
                               SEMVER_TEST_ARGUMENTS=str(arguments), SEMVER_TEST_METADATA=json.dumps(metadata),
                               SEMVER_TEST_TARGET_CAPTURE=str(target_capture), CARGO_TARGET_DIR=str(root / "caller-build"))
            environment.pop("SEMVER_BASELINE", None)
            for explicit, expected in ((None, "v0.10.0"), ("v0.10.1", "v0.10.1")):
                if explicit:
                    environment["SEMVER_BASELINE"] = explicit
                result = subprocess.run(["bash", str(scripts / "semver-check.sh")], env=environment, capture_output=True)
                self.assertEqual(result.returncode, 0, result.stderr.decode())
                selected = arguments.read_text().splitlines()
                self.assertEqual(selected[selected.index("--baseline-rev") + 1], expected)
                self.assertEqual(target_capture.read_text().strip(), str(resolved_target / "semver-build"))
            environment.pop("SEMVER_BASELINE", None)
            git("tag", "--delete", "v0.10.0")
            arguments.unlink()
            result = subprocess.run(["bash", str(scripts / "semver-check.sh")], env=environment, capture_output=True)
            self.assertEqual(result.returncode, 0, result.stderr.decode())
            self.assertFalse(arguments.exists())


if __name__ == "__main__":
    unittest.main()
