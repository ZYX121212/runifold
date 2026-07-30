# RFC 0071: OpenAI GA Realtime transports

## Status

Implemented for text, bounded WebSocket audio, function-call argument deltas,
short-lived client-secret creation, and browser WebRTC media.

## Decision

`OpenAiClient::realtime(model)` creates a model-bound connector.
`connect(context)` opens `/v1/realtime?model=...` and returns an explicitly
driven connection. The connection accepts typed session-update, user-text,
input-audio append/commit/clear, response-create and response-cancel commands
and yields one typed server event per `next_event` call. PCM24, PCMU, and PCMA
session formats use the GA nested `session.audio.input/output.format` shape.

Session state and socket I/O remain separate. The local state machine requires
`session.created` before commands, allows one response at a time, correlates
deltas and terminal events to the active response, and preserves unknown
events losslessly for forward compatibility.

## Bounded execution

Every text frame is limited to 1 MiB, command text fields are limited to
256 KiB, and each decoded raw audio chunk is limited to 512 KiB. This is
intentionally stricter than the Provider's 15 MiB append limit so Base64 and
JSON expansion cannot exceed the transport frame bound. Malformed or
oversized output audio fails closed before reaching the caller.

Native Tungstenite reads are consumer-driven. The browser adapter
uses a 32-event bounded queue and closes with code 1009 on overflow instead of
allowing an unbounded JavaScript callback backlog.

Every connect, send, receive, and close operation honors Runifold
cancellation and monotonic deadlines.

## Credential boundary

Native server-to-server connections can attach the configured bearer,
organization, and project headers. Browser WebSocket builds reject any
configured long-lived credential and require `OpenAiConfig::custom` pointed at
an application-controlled, credential-free Gateway.

The browser path intentionally does not imitate authentication with query
parameters. `OpenAiControlPlane::create_realtime_client_secret` creates a
bounded 10-second to 2-hour `ek_...` credential through
`POST /v1/realtime/client_secrets`. The returned secret is held in
`SecretString` and redacted from `Debug`; production browser deployments
should call this through their application Gateway.

## Browser WebRTC

`prepare_webrtc` captures optional microphone media, prepares an autoplay
element for remote model audio, creates the official `oai-events` data
channel, gathers ICE candidates, and exposes a bounded SDP offer. Applications
can exchange that offer directly with a short-lived `ek_...` secret, through a
credential-free Gateway, or by forwarding it to the typed server-side
`create_realtime_call` multipart control plane.

The WebRTC data channel reuses the same typed commands, events, lifecycle,
cancellation and deadline behavior as WebSocket. Its receive queue is bounded
to 32 events and closes on overflow. Pending negotiation can be aborted, and
session shutdown stops every owned media track even if transport close fails.
Safety identifiers are validated and attached only by the server-side
client-secret or unified-call methods.

ICE configuration uses validated STUN/TURN domain types. At most eight servers
are accepted; embedded URL credentials, invalid schemes, controls and
oversized values fail before browser peer creation. TURN secrets use
`SecretString` and remain redacted from `Debug`. Applications can require
relay-only candidates, inspect aggregate Peer and detailed ICE state, and ask
the canonical state machine whether replacement is safe before starting a new
session.

## Disconnect semantics

There is no automatic transcript or command replay. A close before
`session.created` is safe to reconnect. An idle close can start a new empty
server session. A close while a response is active is explicitly
`AmbiguousResponseInFlight`, because output may already have committed before
the connection disappeared.

`OpenAiRealtimeReconnectController` automates only safe replacement
connections. Its validated policy bounds attempts and exponential delay,
applies deterministic per-invocation full jitter, and truncates backoff and
connection establishment at cancellation or deadline. The mutable controller
borrow prevents competing loops from using one controller concurrently.

The application supplies a connection factory. It is called once for every
attempt and receives a fresh child attempt context plus an attempt value that
requires fresh credentials. A WebRTC factory must obtain a new client secret,
create a new peer and SDP offer, and negotiate a new answer inside every
invocation. The controller never stores secrets, SDP, transcript, commands, or
model output.

`reconnect_webrtc_with_gateway` is the browser golden path. It creates a new
Peer, offer and data channel for each attempt, then sends the offer to the
credential-free Gateway. The Gateway must issue a fresh ephemeral Provider
credential per request. HTTP 408, 429 and 5xx exchange responses are typed as
retryable; other non-success statuses are permanent. Failed HTTP exchange,
invalid answer, remote-description and data-channel-open paths explicitly stop
the owned media tracks and close the pending Peer before another attempt.

Observers receive only bounded attempt numbers, selected delays, redacted
failure kinds, and terminal reasons. Invalid requests and protocol failures
stop immediately. Transport, safe close, and browser WebRTC failures may retry
within policy. Any ambiguous close stops without consuming further authority.

## Evidence

Native loopback WebSocket tests verify URL construction, bearer
authentication, command shapes, Base64 audio round trips, event order, state
transitions, client-secret redaction and deadline abort. The mandatory
pinned-Chrome gate performs a credential-free real WebSocket handshake and
completes both text and PCM24 input/output/transcript lifecycles. It also
creates a short-lived client secret through the credential-free browser
Gateway cassette. Two real browser peers complete SDP offer/answer and typed
`oai-events` exchange; fake-device capture proves the microphone/audio SDP and
remote playback setup, and an event flood proves bounded fail-closed behavior.
The cassette also runs a local RFC 5389 STUN responder and requires a
server-reflexive candidate, without depending on public network services.
Two relay-only browser peers also exchange typed events through a
digest-pinned coturn container. CI then stops that exact container and requires
the public ICE state to become disconnected or failed while preserving the
idle recovery disposition. CI restarts coturn, drives the controller to create
a new relay-only Peer/SDP/DataChannel, and proves an event queued on the lost
connection cannot enter the replacement session. A credential-free Gateway
cassette separately proves transient-status retry, permanent-status stop, and
pending-Peer cleanup paths.

The manual `Live OpenAI Realtime canary` closes the remaining Provider
boundary. It requests two short-lived credentials from the official
`/v1/realtime/client_secrets` endpoint, supplies a stable non-user canary
safety identifier, and requires distinct credential values and effective
session IDs. Comparisons occur only in memory. Its evidence schema has no
secret or session-ID field, and the workflow rejects credential-shaped output.
The canary is opt-in because it requires an external API key; normal CI never
silently spends live Provider authority.

## Non-goals

This slice does not implement DOM insertion or user-gesture handling for
autoplay, production TURN provisioning, session persistence, transcript
replay, or application-specific reconciliation of ambiguous responses.
Real OpenAI calls remain an opt-in live integration concern; deterministic CI
uses two real browser peers and a local control-plane cassette.

## Protocol references

- [OpenAI Realtime overview](https://developers.openai.com/api/docs/guides/realtime)
- [OpenAI Realtime WebSocket guide](https://developers.openai.com/api/docs/guides/realtime-websocket)
- [OpenAI Realtime WebRTC guide](https://developers.openai.com/api/docs/guides/realtime-webrtc)
- [OpenAI Realtime client events](https://developers.openai.com/api/reference/resources/realtime/client-events)
- [OpenAI Realtime server events](https://developers.openai.com/api/reference/resources/realtime/server-events)
- [Create Realtime client secret](https://developers.openai.com/api/reference/resources/realtime/subresources/client_secrets/methods/create)
