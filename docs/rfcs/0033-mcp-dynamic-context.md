# RFC 0033: MCP Dynamic Context

- Status: implemented
- Scope: `runifold-mcp`
- Protocol baseline: MCP `2025-11-25`

## Decision

Runifold extends its MCP context edge with stable pagination, RFC 6570 Resource
Template contracts, resource subscriptions, and argument completion.

This layer implements:

- paginated `tools/list`, `resources/list`, `resources/templates/list`, and
  `prompts/list`;
- `resources/subscribe` and `resources/unsubscribe`;
- `notifications/resources/updated` and
  `notifications/resources/list_changed`;
- `completion/complete` for Prompt and Resource Template references.

Sampling remains outside this layer because it reverses model authority: an MCP
server asks its client to invoke a model. That boundary needs a separate host
policy, budget, approval, and model-selection decision.

## Pagination

The server selects a non-zero page size. Cursors encode a collection identity,
offset, and random session namespace. They are opaque to clients and are
rejected with invalid parameters when malformed, out of range, used for another
collection, or reused by another session.

Registries are immutable while shared by a server, so deterministic
`BTreeMap` ordering provides a stable list snapshot. `McpClient` exposes page
methods and complete-list methods. Complete-list traversal rejects repeated
cursors and has a configurable maximum page count.

## Resource Templates

`ResourceTemplateDescriptor` advertises an RFC 6570 URI template and owns a
Resource capability. `ResourceTemplateHandler::matches_uri` is a host-supplied,
non-I/O ownership predicate. Exact resources take precedence; template
resolution is deterministic.

Registration validates template structure and variable expressions. Reading a
matched URI repeats capability authorization, creates an attenuated child run,
honors cancellation and deadline, and applies the same URI, base64, and decoded
size validation as exact resources.

## Subscriptions and notifications

Subscriptions belong to one MCP session and contain exact authorized resource
URIs. Unknown and unauthorized URIs are concealed. Update notifications are
emitted only while that session remains subscribed.

The same notification stream is available over in-process, multiplexed stdio,
and Streamable HTTP. HTTP notifications retain the existing bounded replay and
`Last-Event-ID` behavior. List-change notifications do not grant read
authority; clients must list again through the normal capability filter.

## Completion

`CompletionRegistry` keeps suggestion providers separate from Prompt rendering
and Resource reading. A provider references one registered Prompt name or exact
Resource Template string and shares its capability identity.

The server verifies that the reference exists and that the requested argument
is declared before invoking a provider. Completion then repeats authorization
inside an attenuated child run and honors cancellation and deadline. Results
are capped at 100 non-empty values, and an advertised total may not be smaller
than the returned count.

Completion inputs and values are never logged by the MCP layer. Applications
remain responsible for provider-specific rate limits and for avoiding sensitive
suggestions.

## Verification

Tests cover:

- automatic multi-page traversal and foreign-session cursor rejection;
- template discovery, matching, reading, and concealed misses;
- Prompt completion reference and argument validation;
- subscribe, update, unsubscribe behavior;
- subscribed update delivery over in-process, stdio, and real loopback
  Streamable HTTP, including HTTP replay.
