#!/usr/bin/env python3
"""Apply the intentional cancellation defect only to an explicitly named clean worktree."""
import argparse
import json
import pathlib
import subprocess

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--candidate', required=True)
args = parser.parse_args()
root = pathlib.Path(args.candidate).resolve()
if not (root / '.git').is_file():
    raise SystemExit('candidate must be an isolated Git worktree, not the main checkout')
status = subprocess.run(['git', '-C', str(root), 'status', '--porcelain'], check=True, capture_output=True)
if status.stdout:
    raise SystemExit('candidate worktree must be clean before fixture application')
fixture = json.loads(pathlib.Path(__file__).with_name('cancellation.json').read_text())
path = root / fixture['path']
source = path.read_text()
if source.count(fixture['find']) != fixture['expected_matches']:
    raise SystemExit('fixture no longer matches baseline; update the task explicitly')
path.write_text(source.replace(fixture['find'], fixture['replace']))
print('Applied cancellation fixture. Confirm the baseline test fails before assigning the task.')
