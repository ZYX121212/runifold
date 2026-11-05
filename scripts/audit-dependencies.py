#!/usr/bin/env python3
"""Deny all audit findings except explicitly approved, expiring maintenance notices."""
import argparse
import datetime
import json
from pathlib import Path
import subprocess
import sys
import tomllib

ROOT = Path(__file__).resolve().parent.parent
AUTHORIZED = {
    "RUSTSEC-2026-0440": ("astral-tokio-tar", "0.6.4"),
    "RUSTSEC-2026-0441": ("pin-utils", "0.1.0"),
}


def exceptions(policy, today):
    if type(policy.get("schema_version")) is not int or policy["schema_version"] != 1:
        raise ValueError("unknown advisory exception schema")
    allowed = {}
    for entry in policy.get("exceptions", []):
        identifier = entry["id"]
        if identifier in allowed or AUTHORIZED.get(identifier) != (entry["package"], entry["version"]):
            raise ValueError("exception is not an approved exact advisory/package/version")
        approved = datetime.date.fromisoformat(entry["approved_on"])
        expires = datetime.date.fromisoformat(entry["expires"])
        if not 0 < (expires - approved).days <= 30 or not approved <= today < expires:
            raise ValueError(f"exception is expired, premature or exceeds 30 days: {identifier}")
        if entry.get("kind") != "unmaintained" or not entry.get("reason", "").strip():
            raise ValueError("only documented maintenance notices can be excepted")
        allowed[identifier] = entry
    return allowed


def validate_report(report, allowed, returncode):
    if returncode not in (0, 1) or report.get("error") or report.get("errors"):
        raise ValueError("audit did not complete successfully")
    settings = report["settings"]
    if (settings.get("ignore") != [] or settings["severity"] is not None
            or settings.get("target_arch") != [] or settings.get("target_os") != []):
        raise ValueError("audit settings must not suppress or filter findings")
    if not {"unmaintained", "unsound", "notice"}.issubset(settings["informational_warnings"]):
        raise ValueError("audit must report every informational category")
    if any(type(value) is not int or value <= 0 for value in (
            report["database"]["advisory-count"], report["lockfile"]["dependency-count"])):
        raise ValueError("empty audit database or dependency graph")
    vulnerabilities = report["vulnerabilities"]
    if (vulnerabilities["found"] is not False or type(vulnerabilities["count"]) is not int or vulnerabilities["count"] != 0
            or vulnerabilities["list"] != []):
        raise ValueError("vulnerability findings are never excepted")
    accepted = []
    for category, findings in report["warnings"].items():
        for finding in findings:
            advisory, package = finding["advisory"], finding["package"]
            entry = allowed.get(advisory["id"])
            if (category != "unmaintained" or finding.get("kind") != category
                    or advisory.get("informational") != "unmaintained"
                    or entry is None or advisory["package"] != entry["package"]
                    or package["name"] != entry["package"] or package["version"] != entry["version"]):
                raise ValueError(f"unapproved audit warning: {advisory.get('id', 'unknown')}")
            accepted.append(advisory["id"])
    if returncode != 0 and not accepted:
        raise ValueError("audit exited unsuccessfully without an approved maintenance finding")
    return accepted


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--file", default="Cargo.lock", type=Path)
    args = parser.parse_args()
    try:
        policy = tomllib.loads((ROOT / "advisory-exceptions.toml").read_text())
        allowed = exceptions(policy, datetime.datetime.now(datetime.timezone.utc).date())
        # Never pass --ignore: vulnerabilities remain visible even if a reviewed
        # maintenance advisory is later reclassified by RustSec.
        result = subprocess.run(
            ["cargo", "audit", "--json", "--deny", "warnings", "--file", str(args.file)],
            cwd=ROOT, capture_output=True, text=True, check=False,
        )
        if result.stderr:
            print(result.stderr, file=sys.stderr, end="")
        report = json.loads(result.stdout)
        accepted = validate_report(report, allowed, result.returncode)
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f"Dependency audit rejected: {error}", file=sys.stderr)
        return 1
    print(json.dumps({"result": "passed", "maintenance_exceptions": sorted(set(accepted)),
                      "vulnerability_exceptions": [], "expiry_utc": "2026-12-05"}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
