#!/usr/bin/env python3
"""CORS-enabled streaming cassette for the headless WASM Provider gate."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import socket
import socketserver
import struct
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


STUN_MAGIC_COOKIE = 0x2112A442


class StunHandler(socketserver.BaseRequestHandler):
    """Minimal RFC 5389 binding responder for deterministic browser ICE."""

    def handle(self) -> None:
        payload, connection = self.request
        if len(payload) < 20:
            return
        message_type, message_length, cookie = struct.unpack("!HHI", payload[:8])
        if (
            message_type != 0x0001
            or cookie != STUN_MAGIC_COOKIE
            or message_length > len(payload) - 20
        ):
            return
        host, port = self.client_address
        address = int.from_bytes(socket.inet_aton(host), "big") ^ STUN_MAGIC_COOKIE
        attribute = struct.pack(
            "!HHBBHI",
            0x0020,
            8,
            0,
            0x01,
            port ^ (STUN_MAGIC_COOKIE >> 16),
            address,
        )
        response = struct.pack(
            "!HHI12s",
            0x0101,
            len(attribute),
            STUN_MAGIC_COOKIE,
            payload[8:20],
        )
        connection.sendto(response + attribute, self.client_address)


SUCCESS_EVENTS = (
    'data: {"type":"response.created","response":{"id":"resp_browser",'
    '"model":"gpt-browser"}}\n\n',
    'data: {"type":"response.content_part.added","output_index":0,'
    '"content_index":0,"part":{"type":"output_text"}}\n\n',
    'data: {"type":"response.output_text.delta","output_index":0,'
    '"content_index":0,"delta":"browser-"}\n\n',
    'data: {"type":"response.output_text.delta","output_index":0,'
    '"content_index":0,"delta":"stream"}\n\n',
    'data: {"type":"response.content_part.done","output_index":0,'
    '"content_index":0,"part":{"type":"output_text"}}\n\n',
    'data: {"type":"response.completed","response":{"usage":'
    '{"input_tokens":4,"output_tokens":2}}}\n\n',
)

ANTHROPIC_EVENTS = (
    'event: message\ndata: {"type":"message_start","message":{"id":"msg_browser",'
    '"model":"claude-browser","usage":{"input_tokens":4}}}\n\n',
    'event: message\ndata: {"type":"content_block_start","index":0,'
    '"content_block":{"type":"text","text":""}}\n\n',
    'event: message\ndata: {"type":"content_block_delta","index":0,'
    '"delta":{"type":"text_delta","text":"anthropic-browser"}}\n\n',
    'event: message\ndata: {"type":"content_block_stop","index":0}\n\n',
    'event: message\ndata: {"type":"message_delta","delta":{"stop_reason":"end_turn"},'
    '"usage":{"output_tokens":2}}\n\n',
    'event: message\ndata: {"type":"message_stop"}\n\n',
)

GEMINI_EVENTS = (
    'data: {"responseId":"gemini-browser-response","modelVersion":"gemini-browser",'
    '"candidates":[{"content":{"parts":[{"text":"gemini-"}]}}],'
    '"usageMetadata":{"promptTokenCount":4}}\n\n',
    'data: {"candidates":[{"content":{"parts":[{"text":"browser"}]},'
    '"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":4,'
    '"candidatesTokenCount":2}}\n\n',
)

OLLAMA_EVENTS = (
    '{"model":"qwen-browser","message":{"role":"assistant",'
    '"content":"ollama-"},"done":false}\n',
    '{"model":"qwen-browser","message":{"role":"assistant",'
    '"content":"browser"},"done":false}\n',
    '{"model":"qwen-browser","message":{"role":"assistant","content":""},'
    '"done":true,"done_reason":"stop","prompt_eval_count":4,'
    '"eval_count":2,"total_duration":50000}\n',
)

ORDERED_EMBEDDINGS = {
    "data": [
        {"embedding": [0.0, 1.0], "index": 1},
        {"embedding": [1.0, 0.0], "index": 0},
    ],
    "usage": {"prompt_tokens": 7},
}

GEMINI_EMBEDDINGS = {
    "embeddings": [{"values": [1.0, 0.0]}, {"values": [0.0, 1.0]}],
    "usageMetadata": {"promptTokenCount": 7},
}

OLLAMA_EMBEDDINGS = {
    "embeddings": [[1.0, 0.0], [0.0, 1.0]],
    "prompt_eval_count": 7,
    "total_duration": 50000,
}

BATCH_BASE = {
    "id": "batch_browser",
    "input_file_id": "file_browser",
    "endpoint": "/v1/responses",
    "output_file_id": None,
    "error_file_id": None,
    "metadata": {"runtime": "browser"},
}


class ProviderHandler(BaseHTTPRequestHandler):
    turn_fault_marker: Path | None = None
    turn_recover_marker: Path | None = None
    realtime_gateway_attempts = 0
    realtime_gateway_lock = threading.Lock()

    protocol_version = "HTTP/1.1"

    def log_message(self, message: str, *args: object) -> None:
        print(f"browser-provider: {message % args}", flush=True)

    def cors_headers(self) -> None:
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header(
            "Access-Control-Allow-Headers",
            "accept, anthropic-beta, anthropic-version, content-type, "
            "x-client-request-id",
        )
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS, POST")
        self.send_header(
            "Access-Control-Expose-Headers",
            "retry-after, x-request-id",
        )
        self.send_header("Cross-Origin-Resource-Policy", "cross-origin")

    def do_OPTIONS(self) -> None:
        self.send_response(204)
        self.cors_headers()
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self) -> None:
        if self.path in (
            "/v1/realtime?model=gpt-realtime-browser",
            "/v1/realtime?model=gpt-realtime-audio-browser",
            "/v1/realtime?model=gpt-realtime-overflow",
        ) and (
            self.headers.get("Upgrade", "").lower() == "websocket"
        ):
            self.realtime_websocket()
            return
        if self.path == "/turn-fault":
            if self.turn_fault_marker is None:
                self.send_error(404)
                return
            self.turn_fault_marker.touch(exist_ok=True)
            self.send_response(204)
            self.cors_headers()
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if self.path == "/turn-recover":
            if self.turn_recover_marker is None:
                self.send_error(404)
                return
            self.turn_recover_marker.touch(exist_ok=True)
            self.send_response(204)
            self.cors_headers()
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if self.path == "/health":
            self.send_response(204)
            self.cors_headers()
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if not self.request_is_safe(body_required=False):
            self.json_response(
                400,
                {"error": {"message": "unsafe or incomplete browser request"}},
            )
            return
        if self.path == "/v1/models":
            self.json_response(
                200,
                {
                    "data": [
                        {
                            "id": "gpt-browser",
                            "created": 7,
                            "owned_by": "browser-gateway",
                        }
                    ]
                },
            )
        elif self.path == "/v1/batches/batch_browser":
            self.batch_response("completed")
        else:
            self.send_error(404)

    def realtime_websocket(self) -> None:
        if (
            self.headers.get("Authorization") is not None
            or self.headers.get("Api-Key") is not None
            or self.headers.get("X-Api-Key") is not None
        ):
            self.send_error(400, "browser Realtime must be credential-free")
            return
        key = self.headers.get("Sec-WebSocket-Key")
        if key is None:
            self.send_error(400, "missing WebSocket key")
            return
        accept = base64.b64encode(
            hashlib.sha1(
                f"{key}258EAFA5-E914-47DA-95CA-C5AB0DC85B11".encode("ascii")
            ).digest()
        ).decode("ascii")
        self.send_response(101, "Switching Protocols")
        self.send_header("Upgrade", "websocket")
        self.send_header("Connection", "Upgrade")
        self.send_header("Sec-WebSocket-Accept", accept)
        self.end_headers()
        self.wfile.flush()

        self.websocket_text(
            {
                "type": "session.created",
                "session": {"id": "sess_browser"},
            }
        )
        if self.path.endswith("model=gpt-realtime-overflow"):
            for index in range(64):
                self.websocket_text(
                    {
                        "type": "cassette.overflow",
                        "sequence": index,
                    }
                )
            time.sleep(0.1)
            self.close_connection = True
            return
        while True:
            command = self.read_websocket_text()
            if command is None:
                break
            event_type = command.get("type")
            is_audio = self.path.endswith("model=gpt-realtime-audio-browser")
            if event_type == "session.update" and is_audio:
                expected_format = {"type": "audio/pcm", "rate": 24000}
                session = command.get("session", {})
                audio = session.get("audio", {}) if isinstance(session, dict) else {}
                input_audio = audio.get("input", {}) if isinstance(audio, dict) else {}
                if input_audio.get("format") != expected_format:
                    self.websocket_text(
                        {
                            "type": "error",
                            "error": {
                                "code": "invalid_audio_format",
                                "message": "expected bounded PCM24 browser audio",
                            },
                        }
                    )
            elif event_type == "input_audio_buffer.append" and is_audio:
                if base64.b64decode(command.get("audio", ""), validate=True) != bytes(
                    (0, 1, 2, 255)
                ):
                    self.websocket_text(
                        {
                            "type": "error",
                            "error": {
                                "code": "invalid_audio",
                                "message": "unexpected browser audio bytes",
                            },
                        }
                    )
            elif event_type == "input_audio_buffer.commit" and is_audio:
                self.websocket_text(
                    {
                        "type": "input_audio_buffer.committed",
                        "previous_item_id": None,
                        "item_id": "item_browser_audio",
                    }
                )
            if event_type == "response.create":
                events = (
                    {
                        "type": "response.created",
                        "response": {
                            "id": (
                                "resp_browser_audio"
                                if is_audio
                                else "resp_realtime_browser"
                            )
                        },
                    },
                )
                if is_audio:
                    events += (
                        {
                            "type": "response.output_audio.delta",
                            "response_id": "resp_browser_audio",
                            "item_id": "item_browser_output",
                            "output_index": 0,
                            "content_index": 0,
                            "delta": base64.b64encode(bytes((9, 8, 7))).decode(
                                "ascii"
                            ),
                        },
                        {
                            "type": "response.output_audio_transcript.delta",
                            "response_id": "resp_browser_audio",
                            "item_id": "item_browser_output",
                            "output_index": 0,
                            "content_index": 0,
                            "delta": "browser audio",
                        },
                        {
                            "type": "response.output_audio.done",
                            "response_id": "resp_browser_audio",
                            "item_id": "item_browser_output",
                            "output_index": 0,
                            "content_index": 0,
                        },
                        {
                            "type": "response.output_audio_transcript.done",
                            "response_id": "resp_browser_audio",
                            "item_id": "item_browser_output",
                            "output_index": 0,
                            "content_index": 0,
                            "transcript": "browser audio",
                        },
                    )
                else:
                    events += (
                        {
                            "type": "response.output_text.delta",
                            "response_id": "resp_realtime_browser",
                            "delta": "realtime-browser",
                        },
                    )
                response_id = (
                    "resp_browser_audio" if is_audio else "resp_realtime_browser"
                )
                events += (
                    {
                        "type": "response.done",
                        "response": {
                            "id": response_id,
                            "status": "completed",
                            "usage": {"input_tokens": 2, "output_tokens": 1},
                        },
                    },
                )
                for event in events:
                    self.websocket_text(event)
            elif event_type not in (
                "session.update",
                "conversation.item.create",
                "input_audio_buffer.append",
                "input_audio_buffer.commit",
                "input_audio_buffer.clear",
                "response.cancel",
            ):
                self.websocket_text(
                    {
                        "type": "error",
                        "error": {
                            "code": "unsupported_event",
                            "message": "unsupported browser cassette command",
                        },
                    }
                )
        self.close_connection = True

    def websocket_text(self, payload: dict[str, object]) -> None:
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        if len(body) >= 126:
            header = bytes((0x81, 126)) + struct.pack("!H", len(body))
        else:
            header = bytes((0x81, len(body)))
        self.wfile.write(header + body)
        self.wfile.flush()

    def read_websocket_text(self) -> dict[str, object] | None:
        header = self.rfile.read(2)
        if len(header) != 2:
            return None
        opcode = header[0] & 0x0F
        masked = header[1] & 0x80
        length = header[1] & 0x7F
        if length == 126:
            length = struct.unpack("!H", self.rfile.read(2))[0]
        elif length == 127:
            length = struct.unpack("!Q", self.rfile.read(8))[0]
        if length > 1024 * 1024:
            return None
        mask = self.rfile.read(4) if masked else b""
        body = bytearray(self.rfile.read(length))
        if masked:
            for index in range(len(body)):
                body[index] ^= mask[index % 4]
        if opcode == 0x8:
            return None
        if opcode != 0x1:
            return None
        value = json.loads(body.decode("utf-8"))
        return value if isinstance(value, dict) else None

    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        if self.path == "/realtime-gateway-retry":
            if (
                self.headers.get("Authorization") is not None
                or self.headers.get("Api-Key") is not None
                or self.headers.get("X-Api-Key") is not None
                or self.headers.get("Content-Type") != "application/sdp"
                or not body.startswith(b"v=0")
            ):
                self.json_response(
                    400,
                    {"error": {"message": "unsafe Realtime Gateway request"}},
                )
                return
            with self.realtime_gateway_lock:
                type(self).realtime_gateway_attempts += 1
                attempt = type(self).realtime_gateway_attempts
            self.send_response(503 if attempt == 1 else 400)
            self.cors_headers()
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        body_required = self.path != "/v1/batches/batch_browser/cancel"
        if not self.request_is_safe(body_required=body_required) or (
            body_required and not body
        ):
            self.json_response(
                400,
                {"error": {"message": "unsafe or incomplete browser request"}},
            )
            return

        if self.path == "/v1/responses":
            self.stream_events(SUCCESS_EVENTS, "text/event-stream", initial_delay=0.0)
        elif self.path == "/slow/responses":
            self.stream_events(SUCCESS_EVENTS, "text/event-stream", initial_delay=1.0)
        elif self.path == "/rate/responses":
            self.json_response(
                429,
                {
                    "error": {
                        "message": "browser cassette rate limit",
                        "type": "rate_limit_error",
                        "code": "browser_rate_limit",
                    }
                },
                retry_after="1",
            )
        elif self.path == "/anthropic/v1/messages":
            self.stream_events(ANTHROPIC_EVENTS, "text/event-stream")
        elif self.path == "/anthropic-slow/v1/messages":
            self.stream_events(
                ANTHROPIC_EVENTS,
                "text/event-stream",
                initial_delay=1.0,
            )
        elif self.path == (
            "/gemini/v1beta/models/gemini-browser:streamGenerateContent?alt=sse"
        ):
            self.stream_events(GEMINI_EVENTS, "text/event-stream")
        elif self.path == (
            "/gemini-slow/v1beta/models/gemini-browser:streamGenerateContent?alt=sse"
        ):
            self.stream_events(
                GEMINI_EVENTS,
                "text/event-stream",
                initial_delay=1.0,
            )
        elif self.path == "/ollama/api/chat":
            self.stream_events(OLLAMA_EVENTS, "application/x-ndjson")
        elif self.path == "/ollama-slow/api/chat":
            self.stream_events(
                OLLAMA_EVENTS,
                "application/x-ndjson",
                initial_delay=1.0,
            )
        elif self.path == "/v1/embeddings":
            self.json_response(200, ORDERED_EMBEDDINGS)
        elif self.path == (
            "/gemini/v1beta/models/gemini-embedding:batchEmbedContents"
        ):
            self.json_response(200, GEMINI_EMBEDDINGS)
        elif self.path == "/ollama/api/embed":
            self.json_response(200, OLLAMA_EMBEDDINGS)
        elif self.path == "/v1/files":
            content_type = self.headers.get("Content-Type", "")
            if (
                not content_type.startswith("multipart/form-data; boundary=")
                or b'name="purpose"' not in body
                or b"batch" not in body
                or b'filename="browser.jsonl"' not in body
            ):
                self.json_response(
                    400,
                    {"error": {"message": "invalid browser multipart upload"}},
                )
                return
            self.json_response(
                200,
                {
                    "id": "file_browser",
                    "filename": "browser.jsonl",
                    "purpose": "batch",
                    "bytes": len(body),
                    "created_at": 7,
                    "status": "processed",
                },
            )
        elif self.path == "/v1/realtime/client_secrets":
            payload = json.loads(body.decode("utf-8"))
            if (
                payload.get("expires_after")
                != {"anchor": "created_at", "seconds": 300}
                or payload.get("session", {}).get("model") != "gpt-realtime"
            ):
                self.json_response(
                    400,
                    {"error": {"message": "invalid Realtime client secret request"}},
                )
                return
            self.json_response(
                200,
                {
                    "value": "ek_browser_secret",
                    "expires_at": 1_800_000_000,
                    "session": {
                        "type": "realtime",
                        "id": "sess_browser_secret",
                        "model": "gpt-realtime",
                    },
                },
            )
        elif self.path == "/v1/batches":
            self.batch_response("validating")
        elif self.path == "/v1/batches/batch_browser/cancel":
            self.batch_response("cancelling")
        else:
            self.json_response(404, {"error": {"message": "unknown cassette"}})

    def request_is_safe(self, body_required: bool) -> bool:
        return (
            self.headers.get("Authorization") is None
            and self.headers.get("Api-Key") is None
            and self.headers.get("X-Api-Key") is None
            and self.headers.get("X-Goog-Api-Key") is None
            and self.headers.get("X-Client-Request-Id") is not None
            and (not body_required or self.headers.get("Content-Length") != "0")
        )

    def batch_response(self, status: str) -> None:
        payload = dict(BATCH_BASE)
        payload["status"] = status
        self.json_response(200, payload)

    def stream_events(
        self,
        events: tuple[str, ...],
        content_type: str,
        initial_delay: float = 0.0,
    ) -> None:
        self.send_response(200)
        self.cors_headers()
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", "no-store")
        self.send_header("Connection", "close")
        self.send_header("X-Request-Id", "browser-request")
        self.end_headers()
        self.wfile.flush()
        time.sleep(initial_delay)
        try:
            for event in events:
                self.wfile.write(event.encode("utf-8"))
                self.wfile.flush()
                time.sleep(0.015)
        except (BrokenPipeError, ConnectionResetError):
            pass
        self.close_connection = True

    def json_response(
        self,
        status: int,
        payload: dict[str, object],
        retry_after: str | None = None,
    ) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.cors_headers()
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        if retry_after is not None:
            self.send_header("Retry-After", retry_after)
        self.end_headers()
        self.wfile.write(body)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", default=38087, type=int)
    parser.add_argument("--turn-fault-marker", type=Path)
    parser.add_argument("--turn-recover-marker", type=Path)
    args = parser.parse_args()
    if not 1024 <= args.port <= 65534:
        raise SystemExit("port must be in 1024..=65534")

    ProviderHandler.turn_fault_marker = args.turn_fault_marker
    ProviderHandler.turn_recover_marker = args.turn_recover_marker
    server = ThreadingHTTPServer(("127.0.0.1", args.port), ProviderHandler)
    stun = socketserver.ThreadingUDPServer(("127.0.0.1", args.port + 1), StunHandler)
    stun.daemon_threads = True
    stun_thread = threading.Thread(target=stun.serve_forever, daemon=True)
    stun_thread.start()
    try:
        server.serve_forever()
    finally:
        stun.shutdown()
        stun.server_close()


if __name__ == "__main__":
    main()
