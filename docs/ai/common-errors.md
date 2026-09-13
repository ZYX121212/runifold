# Common errors and corrective actions

| Symptom / category | Inspect | Corrective action |
| --- | --- | --- |
| Missing provider module | Dependency feature declaration | Enable the provider feature on `runifold-providers`, not a guessed facade feature |
| `.runtime` / `.agent` extension unavailable | Imports and dependency versions | Import `runifold::ProviderModelExt`; use matching-version recipes |
| Tool capability denied | Tool descriptor and supplied RunContext | Register the Tool, then explicitly grant the intended capability; do not broaden all authority |
| `AgentBuildError::CallableNameCollision` | Tool and child Agent names | Assign distinct model-facing names |
| `AgentBuildError::ZeroMaxTurns` | Agent configuration | Supply a nonzero turn limit |
| Invalid structured output | Output type and typed completion error | Keep local validation; explicitly configure bounded repair if desired |
| Ambiguous effect / in-flight recovery | Durable effect evidence | Reconcile external receipt or use the documented explicit safe recovery path |
| Request replay conflict | Request ID, input and Agent definition | Reuse identity only for the same logical request and compatible definition |
| Host-state field appears in input schema | Tool signature | Inject `State<T>` instead of adding host state to the model-controlled Input |

`doctor --ai --json` reports stable diagnostic codes, evidence and suggested actions.
Its Cargo analysis cannot prove runtime Tool effect classes, store injection or
external recovery safety. Those checks are explicitly reported as not checked.
Follow [durable effects](recipes/add-effect.md) and [conversation](recipes/conversation.md)
for execution-level evidence.
