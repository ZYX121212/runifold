# Python SDK adapter

Read ../../AGENTS.md and RFC 0079. This is an independent, private native
extension workspace, not a crates.io workspace member.

- Delegate execution to ProviderRuntime and Agent. Event forwarding is allowed;
  another model/Tool/Agent execution loop is not.
- Keep Python types and task-local data out of the Rust core crates.
- Retain callback leases until synchronous work actually returns. Cancellation
  of a wait does not establish that an external write stopped.
- Use a dedicated Cargo output directory. Test an installed wheel against native
  execution and real local HTTP fixtures; Python mocks alone are insufficient.
- Check SDK/native manifest versions against the root workspace version. Build
  both wheel and sdist, then install the sdist in an isolated environment.
- Keep credentials, payloads and callback exception values out of ordinary errors.
- Do not claim PyPI publication or platform wheel compatibility without evidence.
