#!/usr/bin/env python3
"""Verify that an unavailable prerequisite cannot preserve stale pass evidence."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


class UpgradeEvidenceFailureTest(unittest.TestCase):
    def test_missing_git_replaces_a_previous_pass_with_failure(self):
        script = Path(__file__).resolve().with_name("verify-workflow-upgrade.py")
        with tempfile.TemporaryDirectory(prefix="runifold-upgrade-evidence-") as directory:
            root = Path(directory)
            output = root / "evidence.json"
            output.write_text(json.dumps({"result": "passed", "revision": "stale"}))
            empty_path = root / "empty-path"
            empty_path.mkdir()
            environment = dict(os.environ, PATH=str(empty_path))
            result = subprocess.run(
                [sys.executable, str(script), "--output", str(output)],
                env=environment, capture_output=True, check=False,
            )
            self.assertNotEqual(result.returncode, 0)
            report = json.loads(output.read_text())
            self.assertEqual(report["result"], "failed")
            self.assertEqual(report["failed_boundary"], "source-identity")
            self.assertNotEqual(report.get("revision"), "stale")


if __name__ == "__main__":
    unittest.main()
