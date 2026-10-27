#!/usr/bin/env python3
"""Exercise publication ordering and a controller outside the clean tag tree."""
import copy
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / 'scripts/publish-crates.sh'


def metadata():
    return json.loads(subprocess.check_output(
        ['cargo', 'metadata', '--no-deps', '--format-version', '1'], cwd=ROOT))


class PublishOrderTests(unittest.TestCase):
    def test_actual_workspace_orders_all_internal_dependency_kinds(self):
        graph = metadata()
        order = subprocess.check_output([str(SCRIPT), '--list'], cwd=ROOT, text=True).splitlines()
        positions = {name: index for index, name in enumerate(order)}
        self.assertEqual(set(positions), {package['name'] for package in graph['packages']})
        self.assertEqual(len(order), len(positions))
        for package in graph['packages']:
            for dependency in package['dependencies']:
                if dependency.get('path') and dependency['name'] in positions:
                    with self.subTest(package=package['name'], dependency=dependency['name'], kind=dependency['kind']):
                        self.assertLess(positions[dependency['name']], positions[package['name']])

    def fixture(self, directory, graph):
        root = Path(directory)
        source = root / 'release-source'
        source.mkdir()
        binary = root / 'bin'
        binary.mkdir()
        cargo = binary / 'cargo'
        cargo.write_text('#!/bin/sh\nif [ "$1" = metadata ]; then\n  cat "$PUBLISH_TEST_METADATA"\nelse\n  exit 91\nfi\n')
        cargo.chmod(0o755)
        curl = binary / 'curl'
        curl.write_text('#!/bin/sh\nexit 0\n')
        curl.chmod(0o755)
        data = root / 'metadata.json'
        data.write_text(json.dumps(graph))
        controller = root / 'release-tools/scripts/publish-crates.sh'
        controller.parent.mkdir(parents=True)
        shutil.copy2(SCRIPT, controller)
        env = dict(os.environ, PATH=f'{binary}{os.pathsep}{os.environ["PATH"]}',
                   PUBLISH_TEST_METADATA=str(data), RUNIFOLD_PUBLISH_ROOT=str(source))
        return source, controller, env

    def test_build_development_and_target_dependency_inversions_are_rejected(self):
        actual = metadata()
        for kind, target in [('dev', None), ('build', None), ('dev', 'cfg(unix)')]:
            with self.subTest(kind=kind, target=target), tempfile.TemporaryDirectory() as directory:
                graph = copy.deepcopy(actual)
                core = next(p for p in graph['packages'] if p['name'] == 'runifold-core')
                core['dependencies'].append({'name': 'runifold-mcp', 'path': '/fixture/mcp',
                                             'kind': kind, 'target': target})
                _, controller, env = self.fixture(directory, graph)
                result = subprocess.run([str(controller), '--list'], cwd=directory, env=env,
                                        capture_output=True, text=True)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn('runifold-core appears before internal dependency runifold-mcp', result.stderr)

    def test_external_controller_requires_clean_detached_tag(self):
        graph = metadata()
        version = graph['packages'][0]['version']
        with tempfile.TemporaryDirectory() as directory:
            source, controller, env = self.fixture(directory, graph)
            env['CONFIRM_RUNIFOLD_PUBLISH'] = version

            def git(*args):
                return subprocess.run(['git', '-c', 'user.name=Release Fixture',
                                       '-c', 'user.email=fixture@example.invalid',
                                       '-c', 'commit.gpgSign=false', '-c', 'tag.gpgSign=false',
                                       *args], cwd=source, check=True, capture_output=True, text=True)

            git('init', '-b', 'main')
            (source / 'README').write_text('release fixture\n')
            git('add', 'README')
            git('commit', '-m', 'fixture')
            git('tag', f'v{version}')

            def execute():
                return subprocess.run([str(controller), '--execute'], cwd=directory, env=env,
                                      capture_output=True, text=True)

            branch = execute()
            self.assertNotEqual(branch.returncode, 0)
            self.assertIn('refusing to publish from a branch', branch.stderr)
            git('checkout', '--detach', f'v{version}')
            clean = execute()
            self.assertEqual(clean.returncode, 0, clean.stderr)
            self.assertEqual(clean.stdout.count('already exists; skipping'), len(graph['packages']))
            self.assertEqual(git('status', '--porcelain').stdout, '')
            (source / 'uncommitted').write_text('dirty\n')
            dirty = execute()
            self.assertNotEqual(dirty.returncode, 0)
            self.assertIn('refusing to publish from a dirty worktree', dirty.stderr)


if __name__ == '__main__':
    unittest.main()
