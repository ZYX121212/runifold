"""Real loopback HTTP exercised through the installed native extension."""
import asyncio
import json
from contextlib import asynccontextmanager

import pytest
from runifold import Client, RunifoldError


def sse(*events):
    return ''.join('data: ' + json.dumps(event) + '\n\n' for event in events).encode()


@asynccontextmanager
async def endpoint(body, *, status=200, delay=0):
    requests = []
    tasks = set()

    async def handle(reader, writer):
        task = asyncio.current_task()
        tasks.add(task)
        try:
            headers = await reader.readuntil(b'\r\n\r\n')
            length = next((int(line.split(b':', 1)[1]) for line in headers.split(b'\r\n') if line.lower().startswith(b'content-length:')), 0)
            payload = await reader.readexactly(length)
            requests.append((headers.decode(), json.loads(payload)))
            await asyncio.sleep(delay)
            writer.write(f'HTTP/1.1 {status} Response\r\nContent-Type: text/event-stream\r\nContent-Length: {len(body)}\r\nConnection: close\r\n\r\n'.encode() + body)
            await writer.drain()
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        finally:
            writer.close()
            await writer.wait_closed()
            tasks.discard(task)

    server = await asyncio.start_server(handle, '127.0.0.1', 0)
    port = server.sockets[0].getsockname()[1]
    try:
        yield f'http://127.0.0.1:{port}', requests
    finally:
        server.close()
        await server.wait_closed()
        for task in list(tasks):
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


RESPONSES = sse(
    {'type': 'response.created', 'sequence_number': 0, 'response': {'id': 'resp', 'model': 'test'}},
    {'type': 'response.content_part.added', 'sequence_number': 1, 'output_index': 0, 'content_index': 0, 'part': {'type': 'output_text', 'text': ''}},
    {'type': 'response.output_text.delta', 'sequence_number': 2, 'output_index': 0, 'content_index': 0, 'delta': 'ok'},
    {'type': 'response.content_part.done', 'sequence_number': 3, 'output_index': 0, 'content_index': 0, 'part': {'type': 'output_text', 'text': 'ok'}},
    {'type': 'response.completed', 'sequence_number': 4, 'response': {'status': 'completed', 'usage': {'input_tokens': 2, 'output_tokens': 1}}})
CHAT = sse({'id': 'chat', 'model': 'test', 'choices': [{'index': 0, 'delta': {'role': 'assistant', 'content': 'ok'}, 'finish_reason': None}]},
           {'id': 'chat', 'model': 'test', 'choices': [{'index': 0, 'delta': {}, 'finish_reason': 'stop'}]}) + b'data: [DONE]\n\n'
ANTHROPIC = sse(
    {'type': 'message_start', 'message': {'id': 'msg', 'model': 'test', 'usage': {'input_tokens': 2, 'output_tokens': 0}}},
    {'type': 'content_block_start', 'index': 0, 'content_block': {'type': 'text', 'text': ''}},
    {'type': 'content_block_delta', 'index': 0, 'delta': {'type': 'text_delta', 'text': 'ok'}},
    {'type': 'content_block_stop', 'index': 0},
    {'type': 'message_delta', 'delta': {'stop_reason': 'end_turn'}, 'usage': {'output_tokens': 1}},
    {'type': 'message_stop'})
GEMINI = sse({'responseId': 'gem', 'candidates': [{'content': {'parts': [{'text': 'ok'}]}, 'finishReason': 'STOP'}]})
OLLAMA = b'{"model":"test","message":{"content":"ok"},"done":true,"done_reason":"stop"}\n'


@pytest.mark.parametrize('provider,body,path', [
    ('openai', RESPONSES, '/v1/responses'),
    ('compatible', CHAT, '/v1/chat/completions'),
    ('anthropic', ANTHROPIC, '/v1/messages'),
    ('gemini', GEMINI, '/v1beta/models/test:streamGenerateContent'),
    ('ollama', OLLAMA, '/api/chat'),
])
async def test_native_http_adapters(provider, body, path):
    async with endpoint(body) as (url, requests):
        base_url = url + ('/v1/' if provider in ('openai', 'compatible', 'anthropic') else '/v1beta/' if provider == 'gemini' else '/')
        async with Client(provider=provider, model='test', api_key='test-key', base_url=base_url) as client:
            async with client.agent('http').stream('hello', timeout=5) as stream:
                chunks = [event.text_delta async for event in stream]
            assert ''.join(chunks) == 'ok'
            assert stream.outcome.text == 'ok'
            assert client.snapshot()['active_runs'] == 0
        assert len(requests) == 1
        assert requests[0][0].startswith('POST ' + path)
        assert requests[0][1]


async def test_http_auth_failure_redacts_payload_and_does_not_retry():
    private = 'upstream-private-credential'
    async with endpoint(json.dumps({'error': {'message': private, 'type': 'authentication_error'}}).encode(), status=401) as (url, requests):
        async with Client(model='test', api_key=private, base_url=url + '/v1/') as client:
            with pytest.raises(RunifoldError) as caught:
                await client.agent('failure').run('private prompt', timeout=5)
            assert private not in str(caught.value)
            assert 'private prompt' not in str(caught.value)
            assert len(requests) == 1
            assert client.snapshot()['active_runs'] == 0


async def test_http_timeout_closes_native_capacity():
    async with endpoint(RESPONSES, delay=1) as (url, requests):
        async with Client(model='test', api_key='test', base_url=url + '/v1/') as client:
            with pytest.raises(RunifoldError) as caught:
                await client.agent('timeout').run('hello', timeout=0.1)
            assert caught.value.kind == 'DeadlineExceeded'
            assert len(requests) == 1
            assert client.snapshot()['active_runs'] == 0
            assert all(pool['active'] == 0 for pool in client.snapshot()['model']['pools'])
