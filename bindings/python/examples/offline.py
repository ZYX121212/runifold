"""Typed Tools, canonical streaming, a budget and explicit stream ownership."""

import asyncio

from runifold import Budget, tool
from runifold.testing import scripted_client, text, tool_call


@tool(effect="pure")
async def greet(name: str) -> str:
    return f"Hello, {name}!"


async def main():
    async with scripted_client(tool_call("greet", {"name": "Ada"}), text("Hello, Ada!")) as client:
        agent = client.agent("greeter", tools=[greet])
        async with agent.stream(
            "Greet Ada", budget=Budget(turns=2, tool_calls=1), timeout=5
        ) as stream:
            async for event in stream:
                print(event.text_delta, end="", flush=True)
        assert stream.outcome.text == "Hello, Ada!"
        assert stream.outcome.tool_calls == 1
        print("\nUsage:", stream.outcome.usage)
        print("Resources:", client.snapshot())


if __name__ == "__main__":
    asyncio.run(main())
