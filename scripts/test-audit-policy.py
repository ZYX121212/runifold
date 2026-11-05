#!/usr/bin/env python3
"""Maintenance exceptions cannot suppress vulnerabilities or outlive their review."""
import copy
import datetime
import importlib.util
import sys
from pathlib import Path
import tomllib
import unittest

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parent.parent
SPEC = importlib.util.spec_from_file_location("audit_policy", ROOT / "scripts/audit-dependencies.py")
POLICY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(POLICY)
TODAY = datetime.date(2026, 11, 5)


def report():
    return {
        "settings": {"ignore": [], "severity": None, "target_arch": [], "target_os": [],
                     "informational_warnings": ["unmaintained", "unsound", "notice"]},
        "database": {"advisory-count": 1409}, "lockfile": {"dependency-count": 451},
        "vulnerabilities": {"found": False, "count": 0, "list": []}, "warnings": {},
    }


def warning(identifier, package, version):
    return {"kind": "unmaintained", "advisory": {"id": identifier, "package": package,
            "informational": "unmaintained"}, "package": {"name": package, "version": version}}


class AuditPolicyTest(unittest.TestCase):
    def setUp(self):
        self.config = tomllib.loads((ROOT / "advisory-exceptions.toml").read_text())
        self.allowed = POLICY.exceptions(self.config, TODAY)

    def test_secondary_maintenance_lint_cannot_bypass_the_canonical_gate(self):
        for name in ["ci.yml", "release.yml"]:
            text = (ROOT / ".github/workflows" / name).read_text()
            primary = "python3 scripts/audit-dependencies.py"
            secondary = "cargo deny check --allow unmaintained advisories licenses sources"
            self.assertIn(primary, text)
            self.assertIn(secondary, text)
            self.assertLess(text.index(primary), text.index(secondary))
            self.assertNotIn("--ignore", text)

    def test_exact_reviewed_maintenance_notices_are_accepted(self):
        data = report()
        data["warnings"]["unmaintained"] = [warning(i, e["package"], e["version"])
                                                for i, e in self.allowed.items()]
        self.assertEqual(set(POLICY.validate_report(data, self.allowed, 1)), set(self.allowed))

    def test_clean_audit_passes_without_consuming_exceptions(self):
        self.assertEqual(POLICY.validate_report(report(), self.allowed, 0), [])

    def test_vulnerability_is_denied_even_when_its_id_was_maintenance_approved(self):
        data = report()
        finding = warning("RUSTSEC-2026-0441", "pin-utils", "0.1.0")
        finding["advisory"]["informational"] = None
        data["vulnerabilities"] = {"found": True, "count": 1, "list": [finding]}
        with self.assertRaises(ValueError):
            POLICY.validate_report(data, self.allowed, 1)

    def test_other_warning_categories_ids_packages_and_versions_are_denied(self):
        for category, identifier, package, version in [
            ("unsound", "RUSTSEC-2026-0441", "pin-utils", "0.1.0"),
            ("notice", "RUSTSEC-2026-0441", "pin-utils", "0.1.0"),
            ("yanked", "RUSTSEC-2026-0441", "pin-utils", "0.1.0"),
            ("unmaintained", "RUSTSEC-2099-9999", "pin-utils", "0.1.0"),
            ("unmaintained", "RUSTSEC-2026-0441", "another-crate", "0.1.0"),
            ("unmaintained", "RUSTSEC-2026-0441", "pin-utils", "0.1.1"),
        ]:
            with self.subTest(category=category, identifier=identifier, package=package, version=version):
                data = report()
                data["warnings"][category] = [warning(identifier, package, version)]
                with self.assertRaises(ValueError):
                    POLICY.validate_report(data, self.allowed, 1)

    def test_reclassification_within_warning_is_denied(self):
        data = report()
        finding = warning("RUSTSEC-2026-0441", "pin-utils", "0.1.0")
        finding["advisory"]["informational"] = "unsound"
        data["warnings"]["unmaintained"] = [finding]
        with self.assertRaises(ValueError):
            POLICY.validate_report(data, self.allowed, 1)
        finding["advisory"]["informational"] = "unmaintained"
        finding["kind"] = "unsound"
        with self.assertRaises(ValueError):
            POLICY.validate_report(data, self.allowed, 1)

    def test_expiry_is_exclusive_and_cannot_be_extended_beyond_thirty_days(self):
        for date in [datetime.date(2026, 11, 4), datetime.date(2026, 12, 5)]:
            with self.assertRaises(ValueError):
                POLICY.exceptions(self.config, date)
        bad = copy.deepcopy(self.config)
        bad["exceptions"][0]["expires"] = "2027-12-05"
        with self.assertRaises(ValueError):
            POLICY.exceptions(bad, TODAY)

    def test_new_or_duplicate_exceptions_are_denied(self):
        for entry in [dict(self.config["exceptions"][0]),
                      dict(self.config["exceptions"][0], id="RUSTSEC-2099-9999")]:
            data = copy.deepcopy(self.config)
            data["exceptions"].append(entry)
            with self.assertRaises(ValueError):
                POLICY.exceptions(data, TODAY)

    def test_hidden_filters_and_failed_or_empty_audits_are_denied(self):
        for key, value in [("ignore", ["RUSTSEC-2026-0441"]), ("severity", "high"),
                           ("target_arch", ["x86_64"]), ("target_os", ["linux"]),
                           ("informational_warnings", ["unmaintained"])]:
            data = report()
            data["settings"][key] = value
            with self.assertRaises(ValueError):
                POLICY.validate_report(data, self.allowed, 0)
        for code in [1, 2, -9]:
            with self.assertRaises(ValueError):
                POLICY.validate_report(report(), self.allowed, code)
        data = report()
        data["database"]["advisory-count"] = 0
        with self.assertRaises(ValueError):
            POLICY.validate_report(data, self.allowed, 0)


if __name__ == "__main__":
    unittest.main()
