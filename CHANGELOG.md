# Changelog

All notable changes to `BabelQueue.Redis` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this package adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The envelope wire format is versioned separately by `meta.schema_version`
(currently **1**) — see the contract at [babelqueue.com](https://babelqueue.com).

## [Unreleased]

### Added
- **OTel `traceparent` propagation (ADR-0028).** `RedisPublisher.PublishWithHeadersAsync(urn, data,
  headers, traceId)` carries an out-of-band header carrier (e.g. a W3C `traceparent` from
  `Telemetry.PublishAsync(…, headers, …)`) in a transport-owned `__bq_frame` JSON frame
  (`{"__bq_frame":1,"headers":{…},"body":"<raw wire envelope>"}`) that wraps the **bare** envelope
  beside it (GR-1), byte-compatible with the Go/PHP frame. `RPUSH` stores the frame, so the stored
  value **is** the `LREM` ack handle and the reliable-queue semantics are untouched. Framing is
  **opt-in and backward compatible**: a header-less publish stores the bare envelope byte-for-byte,
  and `RedisConsumer` unframes a reserved value to `(body, headers)` — a bare value (older or
  cross-version publisher) still consumes with no headers. The `BabelHandler` delegate now also
  receives the out-of-band `headers` (hand them to `Telemetry.Wrap(handler, headers)` so a consumer
  span becomes a true child of the producer span); with no `traceparent` it falls back to the v0.1
  `trace_id` mapping (no regression).

### Changed
- Require `BabelQueue.Core 1.4.0` (the header-carrier seam version).
- `BabelHandler` gains a fourth parameter, `IReadOnlyDictionary<string,string> headers` (the
  out-of-band transport headers; empty when none) — register handlers as
  `(envelope, rawBody, headers, ct) => …`.

## [1.0.0] - 2026-06-14

### Added
- Initial release. A Redis transport on `BabelQueue.Core` + StackExchange.Redis,
  implementing the broker-bindings §1 reliable-queue list pattern: `RedisPublisher`
  (canonical-envelope `RPUSH` — the list element is the envelope JSON byte-for-byte,
  with no wrapping and no native-metadata projection, since Redis lists carry no
  headers) and `RedisConsumer` (reserve by `LMOVE <queue> <queue>:processing LEFT RIGHT`
  → URN-routed `BabelHandler`s → ack by `LREM`; a throwing handler leaves the element
  reserved on the processing list for at-least-once retry; `OnError`/`OnUnknownUrn`
  hooks; configurable processing suffix / batch size / idle delay). A `.NET-owned`
  reliable queue — full parity with Laravel's reserved-sorted-set reservation on a
  *shared* Redis queue is a separate task (broker-bindings §1.4). `net8.0`, Roslyn
  analyzers (latest-recommended, warnings-as-errors); 21 xUnit tests (incl. the
  cross-SDK Redis binding payload-identity conformance) run with a Moq-mocked
  `IDatabase` (no Redis, no network). The envelope is unchanged
  (`schema_version: 1`); Redis support is purely additive.

[Unreleased]: https://github.com/BabelQueue/babelqueue-dotnet-redis/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/BabelQueue/babelqueue-dotnet-redis/releases/tag/v1.0.0
