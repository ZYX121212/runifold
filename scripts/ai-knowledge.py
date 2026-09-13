#!/usr/bin/env python3
"""Generate/check versioned AI knowledge from Cargo metadata and tested source.

Python 3.11+; no third-party packages. Run --check in CI or --write after edits.
Semantic descriptions live in docs/ai/catalog.json. Never hand-edit generated files.
"""
import argparse
import hashlib
import json
import pathlib
import re
import sys
import textwrap
import tomllib

ROOT = pathlib.Path(__file__).resolve().parents[1]


def validate_schema(value, schema, path='$'):
    """Validate the explicit schema's supported JSON Schema vocabulary, fail closed."""
    supported = {'$schema', 'title', 'type', 'required', 'properties', 'additionalProperties', 'items', 'minLength', 'minItems', 'minProperties', 'const', 'enum'}
    if set(schema) - supported:
        raise ValueError(f'unsupported schema keywords at {path}: {set(schema) - supported}')
    types = {'object': dict, 'array': list, 'string': str, 'integer': int, 'boolean': bool}
    if 'type' in schema and type(value) is not types[schema['type']]:
        raise ValueError(f'schema type mismatch at {path}')
    if 'const' in schema and (type(value) is not type(schema['const']) or value != schema['const']):
        raise ValueError(f'schema constant mismatch at {path}')
    if 'enum' in schema and value not in schema['enum']:
        raise ValueError(f'schema enum mismatch at {path}')
    for keyword in ['minLength', 'minItems', 'minProperties']:
        if keyword in schema and len(value) < schema[keyword]:
            raise ValueError(f'schema {keyword} failed at {path}')
    if isinstance(value, dict):
        missing = set(schema.get('required', [])) - set(value)
        if missing:
            raise ValueError(f'missing fields at {path}: {sorted(missing)}')
        for key, child in value.items():
            child_schema = schema.get('properties', {}).get(key, schema.get('additionalProperties', {}))
            if child_schema is False:
                raise ValueError(f'unexpected field {path}.{key}')
            if isinstance(child_schema, dict):
                validate_schema(child, child_schema, f'{path}.{key}')
    elif isinstance(value, list) and 'items' in schema:
        for index, child in enumerate(value):
            validate_schema(child, schema['items'], f'{path}[{index}]')


def read_toml(path):
    return tomllib.loads(path.read_text())


def source_excerpt(recipe):
    text = (ROOT / recipe['source']).read_text()
    pieces = []
    for span in recipe.get('spans', [recipe]):
        start = span['start']
        if text.count(start) != 1:
            raise ValueError(f"{recipe['id']}: source start must occur exactly once: {start!r}")
        excerpt = text[text.index(start):]
        end = span.get('end')
        if end:
            if end not in excerpt:
                raise ValueError(f"{recipe['id']}: missing source end {end!r}")
            excerpt = excerpt[:excerpt.index(end)]
        pieces.append(textwrap.dedent(excerpt).strip())
    return '\n\n'.join(pieces)


def generate():
    workspace = read_toml(ROOT / 'Cargo.toml')
    version = workspace['workspace']['package']['version']
    catalog = json.loads((ROOT / 'docs/ai/catalog.json').read_text())
    manifest = {
        'schema_version': 1, 'project': 'runifold', 'version': version,
        'rust_version': workspace['workspace']['package']['rust-version'],
        'generated_by': 'python3 scripts/ai-knowledge.py --write',
        'architecture': {}, 'crates': {},
        'laws': [
            {'id': 'core_must_not_depend_on_wire_types', 'rule': 'Keep provider and protocol wire types outside core.', 'source': 'docs/CHARTER.md'},
            {'id': 'child_authority_must_not_amplify', 'rule': 'Grant child capabilities and budgets explicitly; never amplify parent authority.', 'source': 'docs/CHARTER.md'},
            {'id': 'simple_api_uses_canonical_execution', 'rule': 'Simple APIs wrap the canonical execution path.', 'source': 'docs/CHARTER.md'},
            {'id': 'retry_requires_explicit_safety', 'rule': 'Visible output and external effects affect retry safety; ambiguous writes need explicit recovery.', 'source': 'docs/CHARTER.md'},
            {'id': 'history_journal_memory_are_distinct', 'rule': 'Keep conversation history, execution journal and semantic memory distinct.', 'source': 'docs/CHARTER.md'},
        ],
        'recommended_api': {
            'provider_agent': 'ProviderModelExt -> ProviderRuntime -> AgentBuilder -> Agent',
            'explicit_execution': 'Agent::run(input, &RunContext)',
            'offline_testing': 'ScriptedModel -> Agent::builder',
            'durable_conversation': 'AgentSession::run(request_id, input, &RunContext)',
        },
        'recipes': {},
        'validation': {
            'knowledge': ['python3 scripts/ai-knowledge.py --check'],
            'workspace': ['cargo fmt --all -- --check', 'cargo clippy --workspace --all-targets --all-features --locked -- -D warnings', 'cargo test --workspace --all-features --locked'],
        },
    }
    manifest['laws'] = json.loads((ROOT / 'ai/architecture.json').read_text())['laws']
    manifest['canonical_paths'] = {'agent_execution': 'runifold-agent::Agent::run', 'tool_execution': 'runifold-tool::ToolRegistry', 'effect_execution': 'runifold-effect::EffectExecutor'}
    manifest['validation_profiles'] = json.loads((ROOT / 'ai/validation.json').read_text())['profiles']
    manifest['diagnostics'] = json.loads((ROOT / 'ai/diagnostics.json').read_text())['diagnostics']
    manifest['knowledge_priority'] = ['current source and compiler', 'machine architectural rules', 'nearest scoped guide', 'root guide', 'current-version recipe', 'rustdoc', 'RFC rationale', 'historical migration docs']
    outputs = {}
    for member in workspace['workspace']['members']:
        cargo = read_toml(ROOT / member / 'Cargo.toml')
        name = cargo['package']['name']
        semantics = catalog['crates'][name]
        entry = f"{member}/{semantics['entry']}"
        if not (ROOT / entry).is_file():
            raise ValueError(f'missing entry {entry}')
        deps = {}
        tables = [('normal', cargo.get('dependencies', {})), ('dev', cargo.get('dev-dependencies', {})), ('build', cargo.get('build-dependencies', {}))]
        for target, config in cargo.get('target', {}).items():
            for kind in ['dependencies', 'dev-dependencies', 'build-dependencies']:
                tables.append((f'{target}:{kind}', config.get(kind, {})))
        for kind, dependencies in tables:
            for alias, definition in dependencies.items():
                actual = definition.get('package', alias) if isinstance(definition, dict) else alias
                if actual.startswith('runifold'):
                    deps.setdefault(actual, []).append(kind)
        data = {'path': member, 'owns': semantics['owns'], 'does_not_own': semantics['does_not_own'], 'entry': entry, 'workspace_dependencies': deps, 'features': cargo.get('features', {}), 'validation': [semantics['test']]}
        manifest['crates'][name] = data
        manifest['architecture'].setdefault(semantics['area'], []).append(name)
        dependencies = '\n'.join(f'- `{dep}`: {", ".join(kinds)}' for dep, kinds in sorted(deps.items())) or '- No Runifold workspace dependencies.'
        local_files = semantics.get('key_files', [semantics['entry']])
        for local_file in local_files:
            if not (ROOT / member / local_file).is_file():
                raise ValueError(f'missing local entry {member}/{local_file}')
        local_detail = '\n'.join(f'- `{file}`' for file in local_files)
        local_rules = '\n'.join(f'- {rule}' for rule in semantics.get('local_rules', []))
        extension = semantics.get('extension', f'Extend {semantics["owns"].lower()} within this crate; consult current public exports before adding another abstraction.')
        outputs[f'{member}/AGENTS.md'] = f'''<!-- Generated by scripts/ai-knowledge.py; edit docs/ai/catalog.json. -->
# {name} Agent guide

Read ../../AGENTS.md first. This guide adds local boundaries.

## Purpose

Owns: {semantics['owns']}.

Does not own: {semantics['does_not_own']}.

## Key files and execution path

Start at `{semantics['entry']}` and follow its module declarations and calls.

{local_detail}

## Extension points

{extension}

{local_rules}

Public exports are in the crate entry point. Consult
[the architecture map](../../docs/ai/architecture.md) before crossing a crate boundary.
Tests next to implementation exercise local invariants; `tests/` (where present)
exercises public or protocol boundaries. Keep extensions within this ownership.

## Dependencies

{dependencies}

## Invariants and common mistakes

Preserve the root architectural laws. Do not introduce a second execution path,
ambient authority, hidden retry, or vendor wire types into neutral contracts.
Do not infer a public API from historical RFC examples; verify current exports.
Changes to a public contract need a regression test and release documentation.

## Tests to run

```sh
{semantics['test']}
cargo clippy -p {name} --all-targets --all-features --locked -- -D warnings
```

Database tests may require Docker; see [testing](../../docs/ai/testing.md).
After changing features, dependencies, entry points or recipes, run
`python3 scripts/ai-knowledge.py --write` and `--check` at the workspace root.
'''
    if set(catalog['crates']) != set(manifest['crates']):
        raise ValueError('catalog crate list differs from workspace')
    recipe_documents = {}
    for recipe in catalog['recipes']:
        path = f"docs/ai/recipes/{recipe['id']}.md"
        if recipe['validation_profile'] not in manifest['validation_profiles']:
            raise ValueError(f'unknown validation profile for {recipe["id"]}')
        for related in [recipe['source'], *recipe['related']]:
            if not (ROOT / related).is_file() and related not in {f'docs/ai/recipes/{r["id"]}.md' for r in catalog['recipes']}:
                raise ValueError(f'missing recipe reference {related}')
        entry = {key: recipe[key] for key in ['title', 'audience', 'api', 'requirements', 'validation', 'applies_to', 'validation_profile', 'difficulty', 'uses', 'runtime_path']}
        entry.update({'path': path, 'source': recipe['source'], 'related': recipe['related'], 'source_sha256': hashlib.sha256((ROOT / recipe['source']).read_bytes()).hexdigest()})
        manifest['recipes'][recipe['id']] = entry
        requirements = '\n'.join(f'- {item}' for item in recipe['requirements'])
        dont = '\n'.join(f'- {item}' for item in recipe['do_not'])
        tests = '\n'.join(recipe['validation'])
        links = '\n'.join(f'- [{ref}](../../../{ref})' for ref in recipe['related'])
        frontmatter = '\n'.join([
            '---', f'id: runifold.{recipe["id"].replace("-", "_")}',
            'schema: 1', f'verified_against: "{version}"',
            'verification: source_bound_ci_required',
            f'difficulty: {recipe["difficulty"]}',
            f'validation_profile: {recipe["validation_profile"]}',
            'applies_to:', *[f'  - "{scope}"' for scope in recipe['applies_to']],
            'uses: ' + json.dumps(recipe['uses']), '---',
        ])
        diagnostic_codes = [item['code'] for item in manifest['diagnostics'] if item['recipe'] == recipe['id']]
        diagnostic_help = '\n'.join(f'- `runifold ai explain {code} --json`' for code in diagnostic_codes) or 'Use the typed nested cause and the common-errors guide.'
        production_example = ''
        if recipe.get('production_source'):
            production_code = (ROOT / recipe['production_source']).read_text().strip()
            production_example = '\n\nCompile-tested application source ([complete file](../../../' + recipe['production_source'] + ')):\n\n```rust\n' + production_code + '\n```'
        content = f'''{frontmatter}
<!-- Generated by scripts/ai-knowledge.py; edit catalog.json or source. -->
# {recipe['title']}

Runifold {version} · Audience: {recipe['audience']}.

## Goal

{recipe['title']} through the canonical runtime contracts.

## Use this API

`{recipe['api']}`

Dependencies / prerequisites (use the same Runifold release family):

{requirements}

## Minimal example

The following is extracted from [the compiled source](../../../{recipe['source']}).
Excerpts may use imports, fixtures and helper types from that file; use the complete
source for a runnable example. Test-only assertions and mocks are not application setup.

```rust
{source_excerpt(recipe)}
```

## Production example

{recipe['production']}{production_example}

## Runtime path

{recipe['runtime_path']}

Preserve the owning crate's canonical execution and authority checks. The full
source linked above provides the executable context; related contracts explain
production persistence and failure boundaries.

## Do not do this

{dont}

## Diagnostics

{diagnostic_help}

## Tests

Run from the Runifold workspace. Check-only examples do not invoke providers.
For a downstream application use its own package/test name after copying the pattern.

```sh
{tests}
```

## Related types and contracts

{links}
'''
        outputs[path] = content
        recipe_documents[recipe['id']] = content
    inputs = {'Cargo.toml', 'docs/ai/catalog.json', 'ai/architecture.json', 'ai/diagnostics.json', 'ai/validation.json', 'scripts/ai-knowledge.py', 'scripts/ai-check.py', 'ai/schemas/runifold-ai.schema.json', 'crates/runifold/tests/ai_api_contract.rs', 'AGENTS.md'}
    inputs.update(f'{member}/Cargo.toml' for member in workspace['workspace']['members'])
    inputs.update(recipe['source'] for recipe in catalog['recipes'])
    inputs.update(recipe['production_source'] for recipe in catalog['recipes'] if recipe.get('production_source'))
    inputs.update(ref for recipe in catalog['recipes'] for ref in recipe['related'] if not ref.startswith('docs/ai/recipes/'))
    inputs.update(item['source'] for item in manifest['diagnostics'] if item.get('source'))
    inputs.add('docs/CHARTER.md')
    api_index = {}
    for symbol in catalog['symbols']:
        source = (ROOT / symbol['source']).read_text()
        pattern = r'pub\s+(?:(?:async|const)\s+)?' + symbol['kind'] + r'\s+' + re.escape(symbol['name']) + r'\b'
        matches = list(re.finditer(pattern, source))
        if len(matches) != 1:
            raise ValueError(f'RF-AI-DOC-001: public declaration missing/ambiguous: {symbol["symbol"]}')
        start = matches[0].start()
        signature = source[start:source.index('{', start)].strip()
        api_index[symbol['symbol']] = {'source': symbol['source'], 'declaration': signature, 'source_sha256': hashlib.sha256(source.encode()).hexdigest(), 'verification': 'source_declaration_plus_compile_contract', 'compile_contract': 'crates/runifold/tests/typed_tool_macro.rs' if symbol['symbol'] == 'runifold::tool' else 'crates/runifold/tests/ai_api_contract.rs'}
        inputs.add(symbol['source'])
    for recipe in catalog['recipes']:
        for symbol in recipe['uses']:
            if symbol not in api_index:
                raise ValueError(f'RF-AI-DOC-001: recipe references unknown symbol {symbol}')
    manifest['api_index'] = 'docs/ai/api-index.json'
    manifest['freshness'] = {'algorithm': 'sha256', 'inputs': {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in sorted(inputs)}}
    manifest['freshness']['digest'] = hashlib.sha256(json.dumps(manifest['freshness']['inputs'], sort_keys=True).encode()).hexdigest()
    outputs['docs/ai/api-index.json'] = json.dumps({'schema_version': 1, 'version': version, 'symbols': api_index}, indent=2) + '\n'
    outputs['docs/ai/crate-map.json'] = json.dumps(manifest['crates'], indent=2) + '\n'
    adapter = '<!-- Generated; edit canonical AGENTS.md / catalog, then regenerate. -->\n# Runifold instructions\n\nRead AGENTS.md, then the nearest crate AGENTS.md before modifying code.\nUse one relevant docs/ai/recipes task and its validation profile.\nArchitectural laws are enforced by runtime, tests and CI.\n'
    outputs['.github/copilot-instructions.md'] = adapter
    outputs['CLAUDE.md'] = adapter
    for name, crate in manifest['crates'].items():
        guidance = f'Read AGENTS.md and {crate["path"]}/AGENTS.md.\nUse docs/ai/recipes for the current task; run the local validation profile.\n'
        outputs[f'.github/instructions/{name}.instructions.md'] = f'---\napplyTo: "{crate["path"]}/**"\n---\n<!-- Generated from canonical crate guide. -->\n{guidance}'
        outputs[f'.cursor/rules/{name}.mdc'] = f'---\ndescription: "Runifold {name} boundaries"\nglobs: "{crate["path"]}/**"\nalwaysApply: false\n---\n<!-- Generated from canonical crate guide. -->\n{guidance}'
    llms = f'# Runifold {version}\n\n> Typed, observable and reliable AI application runtime in Rust.\n\nUse recipes for application development; read architecture for framework changes.\n\n## AI Coding\n\n'
    base = f'https://raw.githubusercontent.com/ZYX121212/runifold/v{version}/'
    for name in ['docs/ai/README.md', 'docs/ai/architecture.md', 'docs/ai/api-map.md', 'runifold.ai.json', *[item['path'] for item in manifest['recipes'].values()]]:
        llms += f'- [{name}]({base}{name})\n'
    outputs['llms.txt'] = llms
    outputs['docs/llms.txt'] = llms
    validate_schema(manifest, json.loads((ROOT / 'ai/schemas/runifold-ai.schema.json').read_text()))
    codes = set()
    for diagnostic in manifest['diagnostics']:
        for code in [diagnostic['code'], *diagnostic.get('aliases', [])]:
            if code in codes:
                raise ValueError(f'duplicate diagnostic {code}')
            codes.add(code)
        if diagnostic['recipe'] not in manifest['recipes']:
            raise ValueError(f'unknown diagnostic recipe {diagnostic["recipe"]}')
        if diagnostic.get('source'):
            source = (ROOT / diagnostic['source']).read_text()
            if diagnostic['symbol'].split('::')[-1] not in source:
                raise ValueError(f'missing diagnostic source symbol {diagnostic["symbol"]}')
            if '::' in diagnostic['symbol'] and f'{diagnostic["symbol"]} => "{diagnostic["code"]}"' not in source:
                raise ValueError(f'diagnostic mapping drift: {diagnostic["code"]}')
    outputs['runifold.ai.json'] = json.dumps(manifest, indent=2) + '\n'
    outputs['crates/runifold-cli/src/ai-check.py'] = (ROOT / 'scripts/ai-check.py').read_text()
    outputs['runifold.manifest.json'] = json.dumps(manifest, indent=2) + '\n'
    outputs['crates/runifold-cli/src/ai-knowledge.json'] = json.dumps({'manifest': manifest, 'documents': recipe_documents, 'api_index': api_index}, indent=2) + '\n'
    return outputs


def check_links(outputs):
    for path in [ROOT / 'AGENTS.md', *(ROOT / 'docs/ai').rglob('*.md')]:
        for target in re.findall(r'\]\(([^)]+)\)', path.read_text()):
            if '://' in target or target.startswith('#'):
                continue
            destination = (path.parent / target.split('#')[0]).resolve()
            if not destination.exists():
                raise ValueError(f'{path.relative_to(ROOT)}: broken link {target}')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--write', action='store_true')
    mode.add_argument('--check', action='store_true')
    args = parser.parse_args()
    outputs = generate()
    stale = []
    for name, content in outputs.items():
        path = ROOT / name
        if args.write:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content)
        elif not path.is_file() or path.read_text() != content:
            stale.append(name)
    if stale:
        print('AI knowledge is stale; run python3 scripts/ai-knowledge.py --write:\n' + '\n'.join(stale), file=sys.stderr)
        return 1
    check_links(outputs)
    print(f'AI knowledge {"generated" if args.write else "verified"}: {len(outputs)} files')
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (ValueError, KeyError, OSError) as error:
        sys.exit(str(error))
