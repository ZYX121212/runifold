"""Installed-extension contracts: every execution below runs the Rust Agent."""
import asyncio
import contextvars
from importlib.metadata import version
import json
import threading

from pydantic import BaseModel, ConfigDict
import pytest

from runifold import Budget, Client, RetrySafety, RunifoldError, ToolContext, ToolOutput, tool
from runifold.testing import recorded_requests, scripted_client, text, tool_call


@tool(effect="pure")
async def greet(name: str, context: ToolContext) -> str:
    assert not context.cancelled
    assert context.run_id and context.invocation_id
    return f"Hello, {name}!"


@tool(effect="pure")
def null_result() -> None:
    return None


class Answer(BaseModel):
    model_config = ConfigDict(extra="forbid")
    answer: str
    score: int


async def eventually(predicate, timeout=2):
    async def poll():
        while not predicate():
            await asyncio.sleep(0.005)
    await asyncio.wait_for(poll(), timeout)


async def test_installed_version_and_payload_free_fingerprints():
    async with scripted_client(text("ready")) as client:
        environment = client.environment()
        assert environment["sdk_version"] == environment["native_version"] == version("runifold-sdk")
        assert len(environment["native_lock_sha256"]) == len(environment["native_artifact_sha256"]) == 64
        assert all("/Users/" not in value for value in environment.values())
        assert (await client.agent("a").run("start")).text == "ready"


async def test_typed_async_tool_runs_through_native_transcript_and_budget():
    async with scripted_client(tool_call("greet", {"name": "Ada"}), text("done")) as client:
        result = await client.agent("greeter", tools=[greet]).run("greet", budget=Budget(turns=2, tool_calls=1))
        assert result.text == "done" and result.turns == 2 and result.tool_calls == 1
        assert result.usage["tokens"] == 4 and result.usage["tool_calls"] == 1
        assert "Hello, Ada!" in json.dumps(result.transcript)
        assert len(recorded_requests(client)) == 2
        assert client.snapshot()["active_callbacks"] == 0


async def test_sync_tool_preserves_explicit_structured_null():
    async with scripted_client(tool_call("null_result", {}), text("done")) as client:
        result = await client.agent("nullable", tools=[null_result]).run("call")
        tool_messages = [item for item in result.transcript if item["role"] == "tool"]
        assert tool_messages
        part = tool_messages[0]["content"][0]
        assert "structured_content" in part and part["structured_content"] is None


async def test_native_stream_retains_deltas_and_complete_outcome():
    async with scripted_client(text("hello", chunks=["he", "llo"], metadata={"scripted.opaque": {"x": 1}})) as client:
        async with client.agent("streamer").stream("start") as stream:
            events = [event async for event in stream]
        assert "".join(event.text_delta for event in events) == "hello"
        assert events[-1].type == "completed"
        assert stream.outcome.text == "hello"
        assert stream.outcome.response["provider_metadata"]["scripted.opaque"] == {"x": 1}


async def test_zero_budget_rejects_before_native_provider_work():
    async with scripted_client(text("unused")) as client:
        with pytest.raises(RunifoldError) as caught:
            await client.agent("bounded").run("start", budget=Budget(turns=0))
        assert caught.value.kind == "BudgetExceeded"
        assert recorded_requests(client) == []


async def test_pydantic_output_and_safe_validation_errors():
    async with scripted_client(text('{"answer":"yes","score":3}'), text('{"answer":"private-value","score":"secret-value"}')) as client:
        agent = client.agent("structured", output_type=Answer)
        result = await agent.run("start")
        assert result.parsed == Answer(answer="yes", score=3)
        with pytest.raises(RunifoldError) as caught:
            await agent.run("invalid")
        assert caught.value.retry_safety is RetrySafety.UNSAFE
        assert "private-value" not in str(caught.value) and "secret-value" not in str(caught.value)


async def test_task_cancel_releases_native_model_and_run_capacity():
    client = scripted_client(text("a long stream", chunks=list("a long stream")), delay_ms=30)
    task = asyncio.create_task(client.agent("cancel").run("start"))
    await eventually(lambda: client.snapshot()["model"]["pools"][0]["active"] == 1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 1)
    await client.aclose()
    assert client.snapshot()["active_runs"] == 0
    assert client.snapshot()["model"]["pools"][0]["active"] == 0


async def test_cancel_queued_run_retains_native_conservative_classification():
    client = scripted_client(text("first"), text("second"), delay_ms=50, model_concurrency=1)
    first = asyncio.create_task(client.agent("first").run("one"))
    await eventually(lambda: client.snapshot()["model"]["pools"][0]["active"] == 1)
    run = client.agent("queued").start("two")
    second = asyncio.create_task(run.result())
    await eventually(lambda: client.snapshot()["model"]["queued"] == 1)
    run.cancel()
    with pytest.raises(RunifoldError) as caught:
        await second
    assert caught.value.kind == "Cancelled" and caught.value.retry_safety is RetrySafety.UNKNOWN
    assert len(recorded_requests(client)) == 1
    await first
    await client.aclose()


async def test_sync_callback_retains_capacity_after_local_cancellation():
    entered, release = threading.Event(), threading.Event()

    @tool(effect="pure")
    def blocked() -> str:
        entered.set()
        assert release.wait(3)
        return "finished"

    client = scripted_client(tool_call("blocked", {}), text("unused"), tool_concurrency=1)
    task = asyncio.create_task(client.agent("worker", tools=[blocked]).run("start"))
    try:
        await eventually(entered.is_set)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 1)
        assert client.snapshot()["active_runs"] == 0
        assert client.snapshot()["active_callbacks"] == 1
    finally:
        release.set()
        await client.aclose()
    assert client.snapshot()["active_callbacks"] == 0


async def test_async_callback_cancel_and_deadline_are_bounded():
    entered, stopped = asyncio.Event(), asyncio.Event()

    @tool(effect="pure")
    async def blocked() -> str:
        entered.set()
        try:
            await asyncio.sleep(60)
            return "unreachable"
        finally:
            stopped.set()

    client = scripted_client(tool_call("blocked", {}))
    try:
        with pytest.raises(RunifoldError) as caught:
            await asyncio.wait_for(client.agent("deadline", tools=[blocked]).run("start", timeout=0.1), 1)
        assert caught.value.kind == "DeadlineExceeded"
        assert entered.is_set()
        await asyncio.wait_for(stopped.wait(), 1)
    finally:
        await client.aclose()


async def test_checkpoint_reopens_and_completed_resume_does_not_execute(tmp_path):
    path = tmp_path / "state.sqlite"
    async with scripted_client(text("persisted")) as client:
        original = await client.agent("durable").run_checkpointed("start", path=path)
    async with scripted_client() as reopened:
        restored = await reopened.agent("durable").resume(path=path, checkpoint_id=original.checkpoint_id)
        assert restored.text == original.text and restored.usage == original.usage
        assert restored.checkpoint_id == original.checkpoint_id
        assert recorded_requests(reopened) == []


def test_client_reuse_across_event_loops_preserves_contextvars():
    trace = contextvars.ContextVar("trace")

    @tool(effect="pure")
    async def inspect_trace() -> str:
        return trace.get()

    client = scripted_client(tool_call("inspect_trace", {}), text("one"), tool_call("inspect_trace", {}), text("two"))

    async def call(value):
        trace.set(value)
        result = await client.agent("reusable", tools=[inspect_trace]).run("start")
        assert value in json.dumps(result.transcript)

    asyncio.run(call("loop-one"))
    asyncio.run(call("loop-two"))
    asyncio.run(client.aclose())


def test_production_client_does_not_leak_endpoint_or_credentials():
    secret = "credential-do-not-print"
    with pytest.raises(ValueError) as caught:
        Client(model="test", api_key=secret, base_url=f"https://example.invalid/?key={secret}")
    assert secret not in str(caught.value)
    client = Client(model="test", api_key=secret, base_url="https://example.invalid/v1")
    assert secret not in repr(client)
    assert secret not in json.dumps(client.snapshot())
    client.close()


async def test_early_stream_close_releases_native_resources():
    async with scripted_client(text('abcdef', chunks=['a','b','c','d','e','f']), delay_ms=20) as client:
        async with client.agent('early').stream('hello') as stream:
            async for event in stream:
                if event.text_delta:
                    break
        assert client.snapshot()['active_runs'] == 0
        assert all(pool['active'] == 0 for pool in client.snapshot()['model']['pools'])


async def test_run_admission_is_bounded_and_safe():
    async with scripted_client(text('one'), delay_ms=20, run_concurrency=1) as client:
        first = client.agent('first').start('one')
        with pytest.raises(RunifoldError) as caught:
            client.agent('second').start('two')
        assert client.snapshot()['limits']['run_concurrency'] == 1
        assert caught.value.kind == 'AdmissionDenied'
        assert caught.value.retry_safety is RetrySafety.SAFE
        await first.aclose()
        assert client.snapshot()['active_runs'] == 0


async def test_checkpoint_rejects_changed_tool_contract(tmp_path):
    path = str(tmp_path / 'schema.sqlite')
    async with scripted_client(text('saved')) as client:
        saved = await client.agent('stable', tools=[greet]).run_checkpointed('hello', path=path)
    async with scripted_client() as client:
        with pytest.raises(RunifoldError):
            await client.agent('stable').resume(path=path, checkpoint_id=saved.checkpoint_id)
        assert recorded_requests(client) == []


async def test_raw_callback_exception_is_not_exposed_and_write_is_not_repeated():
    calls = []
    @tool(effect='non_idempotent_write')
    async def write() -> str:
        calls.append(1)
        raise RuntimeError('private password and payload')
    async with scripted_client(tool_call('write', {}), text('handled')) as client:
        result = await client.agent('writer', tools=[write]).run('write once')
        assert calls == [1]
        assert 'private password' not in json.dumps(result.canonical)
        assert result.tool_calls == 1


async def test_cancelled_sqlite_startup_retains_admission_until_actual_completion(tmp_path):
    import subprocess
    import sys
    path = str(tmp_path / 'locked.sqlite')
    script = """
import sqlite3, sys
connection = sqlite3.connect(sys.argv[1])
connection.execute('CREATE TABLE blocker(value)')
connection.commit()
connection.execute('BEGIN EXCLUSIVE')
print('locked', flush=True)
sys.stdin.readline()
connection.rollback()
connection.close()
"""
    # Use a separate process so this is a real OS-level competing database lock.
    holder = subprocess.Popen([sys.executable, '-u', '-c', script, path],
                              stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    assert await asyncio.wait_for(asyncio.to_thread(holder.stdout.readline), 5) == 'locked\n'
    client = scripted_client(text('unused'), run_concurrency=1)
    try:
        task = asyncio.create_task(client.agent('locked').run_checkpointed('hello', path=path))
        await eventually(lambda: client.snapshot()['active_checkpoint_operations'] == 1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 1)
        assert client.snapshot()['active_runs'] == 1
        with pytest.raises(RunifoldError) as caught:
            client.agent('bounded').start('must not start another database operation')
        assert caught.value.kind == 'AdmissionDenied'
        with pytest.raises(TimeoutError):
            await client.aclose(timeout=0.05)
        assert client.snapshot()['active_checkpoint_operations'] == 1
        assert recorded_requests(client) == []
    finally:
        await asyncio.to_thread(holder.communicate, "release\n", timeout=5)
        await client.aclose(timeout=5)
    assert client.snapshot()['active_runs'] == 0
    assert client.snapshot()['active_checkpoint_operations'] == 0
