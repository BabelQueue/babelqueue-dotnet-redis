# Changelog

All notable changes to `BabelQueue.Redis` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this package adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The envelope wire format is versioned separately by `meta.schema_version`
(currently **1**) — see the contract at [babelqueue.com](https://babelqueue.com).

## [Unreleased]

### Added

- **Zset layout (opt-in):** `RedisLayout.Zset` on `RedisConsumerOptions.Layout` / `RedisPublisherOptions.Layout` speaks Laravel's sorted-set queue (`queues:<name>`, `:delayed`, `:reserved`, `:notify`) with Laravel's Lua pop / release / migrate scripts (ADR-0034 §1). Options: `KeyPrefix`, `RetryAfter`, `MigrationBatchSize`. `RedisPublisher.PublishDelayedAsync` (zset only) parks a message on `:delayed`.
- `RedisConsumerOptions.OnHandlerFailure` (`RedisFailureAction.Leave` | `Release`) and `ReleaseDelay`: opt-in immediate release of a message whose handler threw (default `Leave`, behaviour unchanged).
- Legacy layout: a deadline sweep (`<queue>:processing:deadlines`) returns an expired reservation to the ready list byte for byte, so a crashed worker's message is no longer stranded. The default `:processing` mode and `ProcessingSuffix` are unchanged.
- CI runs a `redis:7` service with `BQ_REQUIRE_REDIS=1` (the `redis_layout` behavioural cases and the Lua integration tests must not skip) in `ci.yml` and `release.yml`.

### Fixed

- `RedisConsumerOptions.IdleDelay` is clamped to 1 s (`RedisConsumerOptions.MaxIdleDelay`), the ADR-0033 section 10 cap; negative values become zero.
- The conformance test project did not copy `conformance/CONFORMANCE_VERSION` to its output directory, so the live `results.json` run (`RedisConformanceSuite`) would have thrown `FileNotFoundException` on a runner with Redis.
- `release.yml` now writes `results.json` (`BQ_CONFORMANCE_RESULTS`) and attaches it to the GitHub release (ADR-0032).

### Docs

- README: a non-conformant envelope or an unmapped URN without `OnUnknownUrn` is only reported to `OnError` and redelivered every `retry_after` (zset `attempts` grows unbounded). Unchanged behaviour, now documented.

- README: zset reservations are keyed by the job body, so byte-identical elements (notably identical poison copies handled concurrently) share one `:reserved` member and only one reaches the DLQ (ADR-0033 section 3 contract limit).

### Tests

- Live tests: late ack leaves the redelivered copy alone and the third worker cleans up; identical zset copies collapse into one reservation member (asserted outside the handler, `OnError` empty); identical poison copies: only one DLQ move wins; a `RetryAfter = null` consumer still sweeps a foreign expired member and then reserves it (broker-bindings section 1.7); null-mode acks raise no `OnWarning`; the release test compares against a literal expected body (not `RedisReleaseBody.Build`).

### Limits

- On the zset layout the `__bq_frame` key is forbidden, so header frames are dropped and `traceparent` is not carried (A3). On Redis Cluster the queue name needs a hash tag (`{orders}`).

### Changed

- Vendored conformance suite updated to v1.6.0; CI pins the `fetch.sh` it downloads by sha256 (`BQ_FETCH_SH_SHA256`).

## [1.1.0] - 2026-06-21

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

[Unreleased]: https://github.com/BabelQueue/babelqueue-dotnet-redis/compare/v1.1.0...HEAD
[1.1.0]: https://github.com/BabelQueue/babelqueue-dotnet-redis/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/BabelQueue/babelqueue-dotnet-redis/releases/tag/v1.0.0
