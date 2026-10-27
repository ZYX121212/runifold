"""Host API misuse must not cancel unrelated consumers or start hidden work."""

import asyncio
import gc
import warnings

import pytest
from runifold import Client, RetrySafety, RunifoldError, ToolContext, tool
from runifold.testing import recorded_requests, scripted_client, text, tool_call
from test_native_sdk import eventually


async def test_second_result_consumer_does_not_cancel_owner():
    async with scripted_client(text("kept"), delay_ms=20) as client:
        run = client.agent("owner").start("hello")
        owner = asyncio.create_task(run.result())
        await eventually(lambda: client.snapshot()["model"]["pools"][0]["active"] == 1)
        with pytest.raises(RuntimeError, match="consumer"):
            await run.result()
        assert (await owner).text == "kept"
        assert (await run.result()).text == "kept"


async def test_wrong_loop_result_does_not_cancel_owner():
    async with scripted_client(text("kept"), delay_ms=20) as client:
        run = client.agent("owner").start("hello")
        with pytest.raises(RuntimeError, match="loop"):
            await asyncio.to_thread(lambda: asyncio.run(run.result()))
        assert (await run.result()).text == "kept"


async def test_wrong_loop_close_does_not_cancel_owner():
    async with scripted_client(text("kept"), delay_ms=20) as client:
        run = client.agent("owner").start("hello")
        with pytest.raises(RuntimeError, match="loop"):
            await asyncio.to_thread(lambda: asyncio.run(run.aclose()))
        assert (await run.result()).text == "kept"


async def test_overlapping_stream_polls_do_not_close_owner():
    async with scripted_client(text("kept"), delay_ms=50) as client:
        async with client.agent("stream").stream("hello") as stream:
            while (await stream.__anext__()).type != "turn_started":
                pass
            owner = asyncio.create_task(stream.__anext__())
            await asyncio.sleep(0)
            with pytest.raises(RuntimeError, match="consumer"):
                await stream.__anext__()
            await owner
            async for _ in stream:
                pass
        assert stream.outcome.text == "kept"


async def test_closed_unstarted_stream_cannot_restart():
    async with scripted_client(text("unused")) as client:
        stream = client.agent("closed").stream("hello")
        await stream.aclose()
        with pytest.raises(RuntimeError, match="closed"):
            async with stream:
                pass
        assert recorded_requests(client) == []


@pytest.mark.parametrize("timeout", [0, -1, float("nan"), float("inf"), 3601, True])
async def test_invalid_close_timeout_does_not_close_client(timeout):
    async with scripted_client(text("still usable")) as client:
        with pytest.raises((ValueError, TypeError)):
            await client.aclose(timeout=timeout)
        assert client.snapshot()["closed"] is False
        assert (await client.agent("live").run("hello")).text == "still usable"


@pytest.mark.parametrize("name,version", [("", "1"), ("   ", "1"), ("ok", ""), ("ok", "   ")])
def test_tool_identity_requires_explicit_nonempty_values(name, version):
    with pytest.raises(ValueError):

        @tool(effect="pure", name=name, version=version)
        def example(value: int) -> int:
            return value


def test_positional_only_context_rejected_at_registration():
    with pytest.raises(TypeError):

        @tool(effect="pure")
        def invalid(context: ToolContext, /) -> str:
            return context.run_id


def test_underscore_argument_rejected_before_pydantic_can_erase_it():
    with pytest.raises(TypeError):

        @tool(effect="pure")
        def invalid(_value: int) -> int:
            return _value


async def test_coroutine_returned_by_sync_tool_is_closed_without_execution():
    effects = []

    async def hidden():
        effects.append("unexpected write")
        return "hidden"

    @tool(effect="non_idempotent_write")
    def mistake() -> str:
        return hidden()

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        async with scripted_client(tool_call("mistake", {}), text("handled")) as client:
            result = await client.agent("safe", tools=[mistake]).run("hello")
        gc.collect()
    assert effects == []
    assert not [warning for warning in caught if issubclass(warning.category, RuntimeWarning)]
    assert "awaitable" in str(result.transcript)


async def test_closed_client_error_is_stable_while_admission_is_full():
    async with scripted_client(text("pending"), delay_ms=50, run_concurrency=1) as client:
        run = client.agent("owner").start("hello")
        client.close()
        with pytest.raises(RunifoldError) as caught:
            client.agent("closed").start("must not start")
        assert caught.value.code == "runifold.client_closed"
        await run.aclose()


@pytest.mark.parametrize("flag", [0, 1, "yes", None])
async def test_invalid_callback_close_flag_does_not_close_client(flag):
    async with scripted_client(text("ready")) as client:
        with pytest.raises(TypeError):
            await client.aclose(wait_for_callbacks=flag)
        assert client.snapshot()["closed"] is False


async def test_closed_client_cannot_enter_a_new_context():
    client = scripted_client()
    await client.aclose()
    with pytest.raises(RuntimeError, match="closed"):
        async with client:
            pytest.fail("closed Client entered its context")


def test_failed_constructor_has_safe_repr():
    client = Client.__new__(Client)
    assert "closed=True" in repr(client)


async def test_rich_tool_content_and_metadata_survive_canonical_execution():
    from runifold import ToolOutput

    @tool(effect="read_only")
    async def rich(_context: ToolContext) -> ToolOutput:
        assert _context.run_id
        return ToolOutput(
            content=[
                {"type": "text", "text": "chart"},
                {
                    "type": "image",
                    "source": {
                        "type": "url",
                        "url": "https://example.invalid/chart.png",
                        "media_type": "image/png",
                    },
                },
            ],
            structured_content={"width": 1},
            metadata={"python.audit": {"id": "safe"}},
        )

    async with scripted_client(tool_call("rich", {}), text("done")) as client:
        result = await client.agent("rich", tools=[rich]).run("hello")
        part = next(message for message in result.transcript if message["role"] == "tool")[
            "content"
        ][0]
        assert part["content"][1]["type"] == "image"
        assert part["structured_content"] == {"width": 1}
        assert part["metadata"] == {"python.audit": {"id": "safe"}}


@pytest.mark.parametrize(
    "wire_safety",
    [
        "Safe",
        "RequiresIdempotency",
        "UnsafeAfterVisibleOutput",
        "UnsafeAfterSideEffect",
        "Unknown",
        "Unsafe",
    ],
)
async def test_tool_failure_retry_categories_remain_valid_native_contracts(wire_safety):
    from runifold import ToolFailure

    safety = RetrySafety(wire_safety)

    @tool(effect="non_idempotent_write")
    async def refusal() -> str:
        raise ToolFailure("declared public failure", retry_safety=safety)

    async with scripted_client(tool_call("refusal", {}), text("handled")) as client:
        result = await client.agent("refusal", tools=[refusal]).run("hello")
        assert "declared public failure" in str(result.transcript)
        assert len(recorded_requests(client)) == 2


async def test_unknown_allowed_tool_is_rejected_before_provider_dispatch():
    async with scripted_client(text("unused")) as client:
        with pytest.raises(ValueError, match="unregistered"):
            await client.agent("restricted").run("hello", allowed_tools=["invented"])
        assert recorded_requests(client) == []


async def test_denied_tool_capability_does_not_execute_callback():
    effects = []

    @tool(effect="non_idempotent_write")
    async def denied() -> str:
        effects.append("must not execute")
        return "written"

    async with scripted_client(tool_call("denied", {}), text("handled")) as client:
        with pytest.raises(RunifoldError) as caught:
            await client.agent("restricted", tools=[denied]).run("hello", allowed_tools=[])
        assert caught.value.code == "RF-TOOL-003"
        assert caught.value.kind == "CapabilityDenied"
        assert effects == []
        assert len(recorded_requests(client)) == 1
        assert client.snapshot()["active_callbacks"] == 0


async def test_close_accepts_a_float_subclass_without_coercing_boolean():
    class Seconds(float):
        pass

    client = scripted_client()
    await client.aclose(timeout=Seconds(5))
    assert client.snapshot()["closed"] is True


def test_installed_distribution_does_not_own_a_global_examples_namespace():
    from importlib.metadata import distribution

    files = distribution("runifold-sdk").files
    assert files is not None
    assert not any(str(file).startswith("examples/") for file in files)


async def test_usage_projection_cannot_mutate_canonical_execution_facts():
    async with scripted_client(text("done")) as client:
        result = await client.agent("facts").run("hello")
        original = result.canonical
        result.usage["tokens"] = 999
        assert result.canonical == original
