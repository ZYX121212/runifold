#!/usr/bin/env python3
"""Grade an explicit candidate using fixed task checks and evidence-bound review.

Default is plan-only. --execute runs validations and writes a result. A separate
human/independent reviewer must attest semantic correctness against the candidate
fingerprint. Missing review leaves task_success and architecture_correct null.
"""
import argparse
import hashlib
import importlib.util
import json
import pathlib
import subprocess
import sys
import time

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.dont_write_bytecode = True


def git(repo, *args):
    return subprocess.run(['git', '-C', str(repo), *args], check=True, capture_output=True).stdout


def fingerprint(repo, base):
    revision = git(repo, 'rev-parse', '--verify', '--end-of-options', f'{base}^{{commit}}').decode().strip()
    patch = git(repo, 'diff', '--binary', '--no-ext-diff', revision, '--')
    untracked = git(repo, 'ls-files', '--others', '--exclude-standard', '-z').split(b'\0')
    digest = hashlib.sha256(revision.encode() + patch)
    for file in sorted(filter(None, untracked)):
        path = repo / file.decode('utf-8', 'surrogateescape')
        if path.is_symlink():
            payload = str(path.readlink()).encode()
        else:
            payload = path.read_bytes()
        digest.update(file + b'\0' + hashlib.sha256(payload).digest())
    return revision, patch, digest.hexdigest()


def review_grade(review, candidate_fingerprint):
    if not review:
        return None
    if review.get('candidate_fingerprint') != candidate_fingerprint:
        raise ValueError('review fingerprint does not match the candidate')
    required = ['architecture_correct', 'task_satisfied', 'no_test_weakening']
    if not isinstance(review.get('reviewer'), str) or not review['reviewer'].strip():
        raise ValueError('reviewer identity is required')
    if any(type(review.get(key)) is not bool for key in required):
        raise ValueError('review must supply explicit boolean grades')
    return all(review[key] for key in required)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--candidate', required=True)
    parser.add_argument('--base', required=True)
    parser.add_argument('--task', required=True)
    parser.add_argument('--agent', required=True, help='Tool and exact model/version, supplied by the operator')
    parser.add_argument('--condition', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--review')
    parser.add_argument('--metrics', help='Optional measured agent trace metrics JSON; retained as supplied evidence')
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    task_path = ROOT / 'tasks' / f'{args.task}.json'
    if task_path.parent.resolve() != (ROOT / 'tasks').resolve():
        raise ValueError('invalid task identifier')
    task = json.loads(task_path.read_text())
    repo = pathlib.Path(args.candidate).resolve()
    output = pathlib.Path(args.output).resolve()
    if output.is_relative_to(repo):
        raise ValueError('write grade evidence outside the candidate checkout to keep its fingerprint stable')
    base, patch, digest = fingerprint(repo, args.base)
    commands = [*task['validation']]
    spec = importlib.util.spec_from_file_location('runifold_ai_check', ROOT.parent / 'scripts/ai-check.py')
    checker = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(checker)
    laws_path = ROOT.parent / 'ai/architecture.json'
    laws = json.loads(laws_path.read_text())['laws']
    graph, _, kinds = checker.cargo_graph(repo)
    violations = checker.architecture_issues(laws, graph, kinds)
    review = json.loads(pathlib.Path(args.review).read_text()) if args.review else None
    semantic_pass = review_grade(review, digest)
    report = {'schema_version':1, 'task':task['id'], 'task_sha256':hashlib.sha256(task_path.read_bytes()).hexdigest(), 'agent':args.agent, 'condition':args.condition, 'baseline_commit':base, 'candidate_fingerprint':digest, 'patch_sha256':hashlib.sha256(patch).hexdigest(), 'validation_commands':commands, 'review_rubric':task['review_rubric'], 'executed':False, 'task_success':None, 'architecture_correct':False if violations else (review['architecture_correct'] if review else None), 'review':review, 'results':[], 'architecture_violations': violations, 'architecture_policy_sha256': hashlib.sha256(laws_path.read_bytes()).hexdigest()}
    if args.execute:
        report['executed'] = True
        started = time.monotonic()
        for command in commands:
            result = subprocess.run(command, cwd=repo, capture_output=True, timeout=1800)
            report['results'].append({'command':command, 'exit_code':result.returncode, 'stdout_sha256':hashlib.sha256(result.stdout).hexdigest(), 'stderr_sha256':hashlib.sha256(result.stderr).hexdigest()})
            output.parent.mkdir(parents=True, exist_ok=True)
            index = len(report['results'])
            output.with_suffix(f'.check-{index}.log').write_bytes(result.stdout + result.stderr)
            if result.returncode:
                break
        if fingerprint(repo, args.base)[2] != digest:
            raise ValueError('candidate changed during validation; rerun and obtain a matching review')
        report['validation_seconds'] = time.monotonic() - started
        machine_pass = not violations and all(r['exit_code'] == 0 for r in report['results']) and len(report['results']) == len(commands)
        report['required_tests_pass'] = machine_pass
        report['task_success'] = False if not machine_pass else semantic_pass
        report['diff_numstat'] = git(repo, 'diff', '--numstat', base, '--').decode('utf-8', 'replace')
        report['files_touched'] = len(set(filter(None, git(repo, 'diff', '--name-only', '-z', base, '--').split(b'\0') + git(repo, 'ls-files', '--others', '--exclude-standard', '-z').split(b'\0'))))
        if args.metrics:
            payload = pathlib.Path(args.metrics).read_bytes()
            report['operator_supplied_metrics'] = json.loads(payload)
            report['metrics_source_sha256'] = hashlib.sha256(payload).hexdigest()
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report,indent=2))
    return 1 if report['task_success'] is False else 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (ValueError, OSError, subprocess.SubprocessError) as error:
        sys.exit(str(error))
