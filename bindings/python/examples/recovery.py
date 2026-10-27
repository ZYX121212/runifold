"""Reopen SQLite and replay a completed checkpoint without a second model call."""
import asyncio
from pathlib import Path
import tempfile
from runifold.testing import recorded_requests, scripted_client, text

async def main():
    with tempfile.TemporaryDirectory() as folder:
        path = str(Path(folder) / 'checkpoint.sqlite')
        async with scripted_client(text('Committed.')) as client:
            first = await client.agent('durable').run_checkpointed('hello', path=path)
        async with scripted_client() as reopened:
            result = await reopened.agent('durable').resume(path=path, checkpoint_id=first.checkpoint_id)
            assert result.text == 'Committed.'
            assert recorded_requests(reopened) == []
            print(result.text, result.usage)

if __name__ == '__main__':
    asyncio.run(main())
