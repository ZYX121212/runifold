#!/usr/bin/env python3
"""Derive the private binding version from the Rust workspace; never the prompt."""
import argparse
from pathlib import Path
import re
import tomllib

root = Path(__file__).resolve().parent.parent
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--write', action='store_true')
args = parser.parse_args()
version = tomllib.loads((root / 'Cargo.toml').read_text())['workspace']['package']['version']
manifest = root / 'bindings/python/Cargo.toml'
source = manifest.read_text()
expected = re.sub(r'(?m)^version = "[^"]+"', f'version = "{version}"', source, count=1)
expected = re.sub(r'(?m)^(runifold[\w-]* = \{ version = ")[^"]+', lambda m: m[1] + '=' + version, expected)
if args.write:
    manifest.write_text(expected)
elif source != expected:
    raise SystemExit('Python SDK version drift: run python3 scripts/python-sdk-version.py --write')
for name in ['LICENSE-MIT', 'LICENSE-APACHE']:
    original = (root / name).read_bytes()
    copy = root / 'bindings/python' / name
    if args.write:
        copy.write_bytes(original)
    elif copy.read_bytes() != original:
        raise SystemExit(f'SDK license drift: {name}')
print(f'Python SDK and exact native path requirements: {version}')
