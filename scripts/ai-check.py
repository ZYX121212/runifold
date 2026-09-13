#!/usr/bin/env python3
"""Select affected validation and enforce the checkable architectural laws.

Runs offline analysis by default. --execute explicitly runs the printed argument
vectors without a shell. Python 3.11+ and Git are required; Cargo/Docker may be
required by selected validations. JSON mode keeps command logs out of stdout.
"""
import argparse
import json
import pathlib
import shlex
import subprocess
import sys
import tomllib


def cargo_graph(root):
    workspace = tomllib.loads((root / 'Cargo.toml').read_text())
    inherited = workspace.get('workspace', {}).get('dependencies', {})
    members = workspace.get('workspace', {}).get('members', [])
    if not members:
        members = ['.']
    graph, paths, kinds = {}, {}, {}
    expanded = []
    for member in members:
        expanded.extend(path.relative_to(root).as_posix() for path in root.glob(member) if (path / 'Cargo.toml').is_file())
    for member in sorted(set(expanded)):
        data = tomllib.loads((root / member / 'Cargo.toml').read_text())
        if 'package' not in data:
            continue
        name = data['package']['name']
        paths[name] = member
        graph[name], kinds[name] = set(), {}
        tables = [(kind, data.get(key, {})) for kind, key in [('normal', 'dependencies'), ('dev', 'dev-dependencies'), ('build', 'build-dependencies')]]
        for config in data.get('target', {}).values():
            tables.extend((kind, config.get(key, {})) for kind, key in [('normal', 'dependencies'), ('dev', 'dev-dependencies'), ('build', 'build-dependencies')])
        for kind, dependencies in tables:
            for alias, declaration in dependencies.items():
                declaration = declaration if isinstance(declaration, dict) else {}
                if declaration.get('workspace'):
                    base = inherited.get(alias, {})
                    declaration = {**(base if isinstance(base, dict) else {}), **declaration}
                dependency = declaration.get('package', alias)
                graph[name].add(dependency)
                kinds[name].setdefault(dependency, set()).add(kind)
    return graph, paths, kinds


def architecture_issues(laws, graph, kinds):
    issues = []
    for law in laws:
        if law['check'] != 'forbidden_dependency':
            continue
        allowed_kinds = set(law.get('dependency_kinds', ['normal', 'build']))
        for owner in law['scope']:
            pending = [(owner, [owner])]
            seen = set()
            while pending:
                current, chain = pending.pop()
                if current in seen:
                    continue
                seen.add(current)
                for dependency in sorted(graph.get(current, [])):
                    if not kinds.get(current, {}).get(dependency, set()) & allowed_kinds:
                        continue
                    trail = [*chain, dependency]
                    if dependency in law['forbid']:
                        issues.append({'code': law['id'], 'severity': 'error', 'message': 'Forbidden runtime/build dependency', 'evidence': trail, 'recommended_action': 'Move the edge dependency to its owning adapter; preserve the neutral kernel boundary.'})
                    elif dependency in graph:
                        pending.append((dependency, trail))
    return issues


def changed_files(root, base):
    revision = subprocess.run(['git', '-C', str(root), 'rev-parse', '--verify', '--end-of-options', f'{base}^{{commit}}'], capture_output=True, check=True).stdout.decode().strip()
    tracked = subprocess.run(['git', '-C', str(root), 'diff', '--name-only', '--no-renames', '-z', revision, '--'], capture_output=True, check=True).stdout
    untracked = subprocess.run(['git', '-C', str(root), 'ls-files', '--others', '--exclude-standard', '-z'], capture_output=True, check=True).stdout
    # Git paths are NUL-delimited: spaces, Unicode and renames must not corrupt selection.
    return sorted(set(p.decode('utf-8', 'surrogateescape') for p in (tracked + untracked).split(b'\0') if p))


def affected_crates(files, graph, paths, policy):
    affected = set()
    for file in files:
        if file in policy['global_files'] or file.endswith('/Cargo.toml') or any(file.startswith(prefix) for prefix in policy['global_prefixes']):
            return set(graph)
        owners = [name for name, path in paths.items() if file.startswith(path.rstrip('/') + '/')]
        affected.update(owners)
        if file.startswith('crates/') and not owners:
            # Deleted/renamed crate: the current graph no longer knows old edges.
            return set(graph)
    while True:
        consumers = {name for name, dependencies in graph.items() if dependencies & affected}
        widened = affected | consumers
        if widened == affected:
            return affected
        affected = widened


def build_plan(root, files):
    graph, paths, kinds = cargo_graph(root)
    policy_path = root / 'ai/validation.json'
    law_path = root / 'ai/architecture.json'
    if not policy_path.is_file() or not law_path.is_file():
        raise ValueError('Runifold ai check requires the Runifold source workspace (ai/validation.json and ai/architecture.json); use doctor --ai for an application.')
    policy = json.loads(policy_path.read_text())
    laws = json.loads(law_path.read_text())['laws']
    affected = sorted(affected_crates(files, graph, paths, policy))
    commands = [['python3', 'scripts/ai-knowledge.py', '--check']]
    if files:
        commands.append(['cargo', 'fmt', '--all', '--', '--check'])
    for name in affected:
        for command in policy['profiles'].get(name, [f'cargo test -p {name} --locked']):
            argv = shlex.split(command)
            if argv not in commands:
                commands.append(argv)
    if affected:
        clippy = ['cargo', 'clippy']
        for name in affected:
            clippy.extend(['-p', name])
        commands.append([*clippy, '--all-targets', '--all-features', '--locked', '--', '-D', 'warnings'])
    return {'schema_version': 1, 'mode': 'affected_validation', 'changed_files': files, 'affected_crates': affected, 'validation_commands': commands, 'architecture_issues': architecture_issues(laws, graph, kinds), 'review_required': [law for law in laws if law['check'] == 'review_and_tests' and set(law['scope']) & set(affected)], 'selection': 'working tree vs selected base plus untracked paths; reverse workspace dependencies include normal/dev/build and all target declarations', 'executed': False, 'results': []}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', default='.')
    parser.add_argument('--base', default='HEAD')
    parser.add_argument('--json', action='store_true')
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args(argv)
    root = pathlib.Path(args.root).resolve()
    try:
        files = changed_files(root, args.base)
        report = build_plan(root, files)
        # Freshness is a check, not merely a suggestion, even without --execute.
        freshness = subprocess.run(['python3', 'scripts/ai-knowledge.py', '--check'], cwd=root, capture_output=True, text=True)
        report['knowledge_fresh'] = freshness.returncode == 0
        if freshness.returncode:
            report['architecture_issues'].append({'code': 'RF-AI-DOC-001', 'severity': 'error', 'message': 'Knowledge freshness/validation failed', 'recommended_action': 'Run python3 scripts/ai-knowledge.py --check for details; regenerate and review the diff.'})
        if args.execute and not report['architecture_issues']:
            report['executed'] = True
            for command in report['validation_commands']:
                print('Running: ' + shlex.join(command), file=sys.stderr, flush=True)
                result = subprocess.run(command, cwd=root, stdout=sys.stderr, stderr=sys.stderr, check=False)
                report['results'].append({'command': command, 'exit_code': result.returncode})
                if result.returncode:
                    break
        report['healthy'] = not report['architecture_issues'] and all(result['exit_code'] == 0 for result in report['results'])
    except (ValueError, OSError, KeyError, subprocess.CalledProcessError) as error:
        report = {'schema_version': 1, 'healthy': False, 'issues': [{'code': 'RF-AI-CHECK-001', 'message': str(error), 'recommended_action': 'Select the Runifold workspace and a valid Git base; install Python 3.11+ and Git.'}]}
    if args.json:
        print(json.dumps(report, indent=2))
    else:
        print('Runifold affected validation')
        print('Affected crates: ' + ', '.join(report.get('affected_crates', [])))
        for command in report.get('validation_commands', []):
            print(shlex.join(command))
        for issue in report.get('architecture_issues', []) + report.get('issues', []):
            print(f'{issue["code"]}: {issue["message"]}')
        print('Semantic laws requiring review: ' + ', '.join(law['id'] for law in report.get('review_required', [])))
        print('Checks passed: ' + str(report['healthy']))
    return 0 if report['healthy'] else 1


if __name__ == '__main__':
    sys.exit(main())
