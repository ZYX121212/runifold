#!/usr/bin/env python3
"""Regression tests for affected selection, architecture and schema enforcement."""
import importlib.util
import json
import pathlib
import tempfile
import unittest
from unittest import mock
import contextlib
import io
import subprocess
import sys
sys.dont_write_bytecode = True

ROOT = pathlib.Path(__file__).resolve().parents[1]


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    loaded = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(loaded)
    return loaded


check = module('ai_check', ROOT / 'scripts/ai-check.py')
knowledge = module('ai_knowledge', ROOT / 'scripts/ai-knowledge.py')


class SelectionTests(unittest.TestCase):
    def setUp(self):
        self.graph = {'core': set(), 'agent': {'core'}, 'app': {'agent'}, 'other': set()}
        self.paths = {name: f'crates/{name}' for name in self.graph}
        self.policy = {'global_files': ['Cargo.toml', 'Cargo.lock'], 'global_prefixes': ['scripts/']}

    def test_reverse_transitive_consumers_and_scoped_changes(self):
        self.assertEqual(check.affected_crates(['crates/core/src/a.rs'], self.graph, self.paths, self.policy), {'core', 'agent', 'app'})
        self.assertEqual(check.affected_crates(['crates/app/src/a.rs'], self.graph, self.paths, self.policy), {'app'})

    def test_deleted_crate_and_dependency_changes_are_conservative(self):
        for path in ['crates/deleted/src/lib.rs', 'crates/agent/Cargo.toml', 'Cargo.lock', 'scripts/new.py']:
            self.assertEqual(check.affected_crates([path], self.graph, self.paths, self.policy), set(self.graph))

    def test_unrelated_docs_do_not_schedule_all_runtime_tests(self):
        self.assertEqual(check.affected_crates(['docs/notes.md'], self.graph, self.paths, self.policy), set())

    def test_transitive_forbidden_dependency_is_rejected_but_dev_edge_is_not(self):
        laws = [{'id': 'RF-LAW-001', 'check': 'forbidden_dependency', 'scope': ['core'], 'forbid': ['reqwest']}]
        graph = {'core': {'bridge'}, 'bridge': {'reqwest'}}
        kinds = {'core': {'bridge': {'normal'}}, 'bridge': {'reqwest': {'build'}}}
        issues = check.architecture_issues(laws, graph, kinds)
        self.assertEqual(issues[0]['evidence'], ['core', 'bridge', 'reqwest'])
        kinds['core']['bridge'] = {'dev'}
        self.assertEqual(check.architecture_issues(laws, graph, kinds), [])

    def test_renamed_workspace_and_target_dependencies_keep_real_package_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            (root / 'Cargo.toml').write_text('[workspace]\nmembers=["core"]\n[workspace.dependencies]\nwire={package="reqwest",version="0.13"}\n')
            (root / 'core').mkdir()
            (root / 'core/Cargo.toml').write_text('[package]\nname="core"\nversion="0.1.0"\n[target.\'cfg(unix)\'.dependencies]\nwire.workspace=true\n')
            graph, _, kinds = check.cargo_graph(root)
            self.assertIn('reqwest', graph['core'])
            self.assertEqual(kinds['core']['reqwest'], {'normal'})


    def test_execute_uses_argument_vectors_and_stops_after_failure(self):
        report = {'architecture_issues': [], 'validation_commands': [['cargo', 'test', '-p', 'core'], ['cargo', 'test', '-p', 'agent']], 'results': [], 'executed': False}
        results = [subprocess.CompletedProcess([], 0), subprocess.CompletedProcess([], 1)]
        with mock.patch.object(check, 'changed_files', return_value=[]), mock.patch.object(check, 'build_plan', return_value=report), mock.patch.object(check.subprocess, 'run', side_effect=results) as run, contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            status = check.main(['--execute', '--json'])
        self.assertEqual(status, 1)
        self.assertTrue(report['executed'])
        self.assertEqual(len(report['results']), 1)
        self.assertEqual(run.call_args_list[1].args[0], ['cargo', 'test', '-p', 'core'])
        self.assertNotIn('shell', run.call_args_list[1].kwargs)

    def test_architecture_failure_prevents_execution(self):
        report = {'architecture_issues': [{'code': 'RF-LAW-001'}], 'validation_commands': [['cargo', 'test']], 'results': [], 'executed': False}
        with mock.patch.object(check, 'changed_files', return_value=[]), mock.patch.object(check, 'build_plan', return_value=report), mock.patch.object(check.subprocess, 'run', return_value=subprocess.CompletedProcess([], 0)) as run, contextlib.redirect_stdout(io.StringIO()):
            status = check.main(['--execute', '--json'])
        self.assertEqual(status, 1)
        self.assertFalse(report['executed'])
        self.assertEqual(run.call_count, 1)


class KnowledgeTests(unittest.TestCase):
    def test_schema_rejects_missing_fields_and_wrong_version_type(self):
        schema = json.loads((ROOT / 'ai/schemas/runifold-ai.schema.json').read_text())
        manifest = json.loads((ROOT / 'runifold.ai.json').read_text())
        knowledge.validate_schema(manifest, schema)
        manifest['schema_version'] = '1'
        with self.assertRaises(ValueError):
            knowledge.validate_schema(manifest, schema)
        del manifest['recipes']
        with self.assertRaises(ValueError):
            knowledge.validate_schema(manifest, schema)

    def test_generated_bundle_and_alias_are_identical_to_manifest(self):
        manifest = json.loads((ROOT / 'runifold.ai.json').read_text())
        bundle = json.loads((ROOT / 'crates/runifold-cli/src/ai-knowledge.json').read_text())
        self.assertEqual(manifest, bundle['manifest'])
        self.assertEqual(manifest, json.loads((ROOT / 'runifold.manifest.json').read_text()))
        self.assertGreaterEqual(len(bundle['documents']), 12)


class EvaluationTests(unittest.TestCase):
    def test_unreviewed_tasks_have_no_semantic_grade_and_stale_reviews_fail(self):
        grader = module('ai_grader', ROOT / 'ai-evals/graders/grade.py')
        self.assertIsNone(grader.review_grade(None, 'abc'))
        review = {'candidate_fingerprint': 'abc', 'reviewer': 'independent', 'architecture_correct': True, 'task_satisfied': True, 'no_test_weakening': True}
        self.assertTrue(grader.review_grade(review, 'abc'))
        with self.assertRaises(ValueError):
            grader.review_grade(review, 'changed')
        review['task_satisfied'] = False
        self.assertFalse(grader.review_grade(review, 'abc'))


if __name__ == '__main__':
    unittest.main()
