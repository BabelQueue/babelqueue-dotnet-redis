# Changelog

All notable changes to `BabelQueue.Redis` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this package adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The envelope wire format is versioned separately by `meta.schema_version`
(currently **1**) — see the contract at [babelqueue.com](https://babelqueue.com).

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
