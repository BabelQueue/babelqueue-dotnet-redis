# BabelQueue.Redis

Redis transport for [BabelQueue](https://babelqueue.com) — "Polyglot Queues,
Simplified." Built on [StackExchange.Redis](https://www.nuget.org/packages/StackExchange.Redis)
and the framework-agnostic
[`BabelQueue.Core`](https://www.nuget.org/packages/BabelQueue.Core).

A canonical-envelope **publisher** and a URN-routed **consumer**, so a Redis-based .NET
service speaks the same wire contract (envelope shape, URN identity, trace propagation)
as the PHP/Laravel, Python, Go, Node and Java SDKs. Implements
[§1 of the broker-bindings contract](https://babelqueue.com) — the reliable-queue list
pattern.

## Install

```bash
dotnet add package BabelQueue.Redis
```

It pulls `BabelQueue.Core` and `StackExchange.Redis` transitively.

## Use

```csharp
using BabelQueue.Redis;
using StackExchange.Redis;

var redis = await ConnectionMultiplexer.ConnectAsync("localhost:6379");
IDatabase db = redis.GetDatabase();

// produce
var id = await new RedisPublisher(db, "orders")
    .PublishAsync("urn:babel:orders:created", new Dictionary<string, object?> { ["order_id"] = 1042 });

// consume
var handlers = new Dictionary<string, BabelHandler>
{
    ["urn:babel:orders:created"] = async (env, rawBody, headers, ct) =>
    {
        // env.Data, env.TraceId, env.Attempts ...; headers carries any out-of-band traceparent
    },
};
var consumer = new RedisConsumer(db, "orders", handlers, new RedisConsumerOptions
{
    OnError = (err, env, raw) => Console.Error.WriteLine(err),
});
await consumer.RunAsync(cancellationToken); // polls until cancelled
```

## Contract mapping (§1)

Unlike the SQS/RabbitMQ bindings, a Redis list element carries **no native metadata**, so
there is **no header/attribute projection**: the list element **is** the canonical envelope
JSON, byte-for-byte, with no wrapping and no added fields. A consumer in any language pops
that same body and decodes it. (The one exception is opt-in out-of-band headers — see
[OpenTelemetry `traceparent` propagation](#opentelemetry-traceparent-propagation-adr-0028) —
which ride in a transport-owned frame that wraps the bare envelope; a header-less publish still
stores the bare envelope byte-for-byte.)

| Envelope | Redis |
| :--- | :--- |
| body | the list element (byte-identical across SDKs, no wrapping) |
| `job` (URN) | read from the decoded body (no native metadata to route on) |
| `trace_id` / `meta.id` / … | read from the decoded body |
| produce | `RPUSH <queue> <envelope>` |
| reserve | `LMOVE <queue> <queue>:processing LEFT RIGHT` (an in-flight message survives a crash) |
| ack | `LREM <queue>:processing 1 <envelope>` |

Reservation follows the **reliable-queue** pattern (mirroring the Go reference): the head of
the queue is atomically moved onto a per-queue `<queue>:processing` list, then removed on
ack. A throwing handler leaves the element on the processing list (at-least-once); the poll
loop never stops on a bad message — observe via `OnError` / `OnUnknownUrn`; an unroutable message
is dead-lettered (see below). The envelope is
unchanged (`schema_version` stays `1`); Redis support is purely additive.

> **Reliable-queue scope.** The default (`RedisLayout.Legacy`) is a **.NET-owned reliable queue**:
> produce/reserve/ack are self-consistent and crash-safe on a queue this SDK owns end-to-end. A
> reservation carries a deadline on `<queue>:processing:deadlines`; before every reserve a sweep
> returns expired reservations to the ready list byte for byte. To share a queue with Laravel,
> opt in to the zset layout below.

## Laravel-compatible zset layout (ADR-0034 §1)

```csharp
var options = new RedisConsumerOptions
{
    Layout = RedisLayout.Zset,      // queues:<name> + :delayed + :reserved + :notify
    RetryAfter = TimeSpan.FromSeconds(90),
    OnHandlerFailure = RedisFailureAction.Release,  // default: Leave (retried after RetryAfter)
    ReleaseDelay = TimeSpan.FromSeconds(5),
};
var consumer = new RedisConsumer(db, "orders", handlers, options);

var publisher = new RedisPublisher(db, "orders", new RedisPublisherOptions { Layout = RedisLayout.Zset });
await publisher.PublishDelayedAsync("urn:babel:orders:created", data, TimeSpan.FromSeconds(30));
```

Every reserve first migrates due `:delayed` and `:reserved` members, then runs Laravel's pop script
(attempts + 1, deadline `floor(now) + retry_after`). An element the script cannot parse is
dead-lettered as poison to `DeadLetterKey`. `KeyPrefix` defaults to `queues:`.

Limits of the zset layout:

- The `__bq_frame` key is forbidden there, so header frames are dropped and **`traceparent` is not
  carried** (the out-of-band propagation below only works on the legacy layout).
- On Redis Cluster the queue name must carry a hash tag (`{orders}`) so the keys share a slot.
- A handler that throws leaves the message reserved by default; it returns after `RetryAfter`. Set
  `OnHandlerFailure = Release` to park it on `:delayed` straight away (attempts + 1).

Messages that cannot be routed (both layouts):

- An unmapped URN with no `OnUnknownUrn` hook invokes `OnError` and is then **dead-lettered**: the envelope
  goes to `<queue>.dlq` (`DeadLetterKey`) with a `dead_letter` block (`reason: "unknown_urn"`, `error`,
  `exception`, `original_queue`, `attempts`, `lang: "dotnet"`, `failed_at`) and the reservation is removed in
  the same gated Lua step (legacy: `LREM` + deadline `ZREM` + `RPUSH`; zset: `ZREM :reserved` + `RPUSH`, same
  scripts as a poison element). Legacy keeps the `traceparent` header frame around the DLQ record; the zset
  layout stores the bare record. It no longer loops. If the reservation already expired, nothing is written
  and `OnWarning` reports `reservation expired`.
- A non-conformant envelope (`Accepts == false`, e.g. an unsupported `schema_version`) invokes `OnError` and is
  dead-lettered **byte for byte** (it is never parsed or re-encoded, so no `dead_letter` block is added),
  exactly like a poison element; the reservation is removed atomically.
- An `OnUnknownUrn` hook takes precedence: it runs, then the message is acked and **not** dead-lettered. There
  is no switch to turn the Redis DLQ off (a poison element is always dead-lettered too); point `DeadLetterKey`
  at the queue you want.
- Zset layout only: the `:reserved` member is the (re-encoded) job body itself (ADR-0033 section 3), so
  byte-identical elements share **one** reservation. If two workers hold identical copies at the same
  time, the first ack / dead-letter wins and the other finds the reservation gone (`reservation expired`
  warning). For poison this means that of N identical concurrent poison copies only one reaches the
  dead-letter list; the count is lost. This affects any byte-identical duplicates, whether hand-pushed or produced by
  an at-least-once producer resending the same `meta.id` (for example an outbox relay retry);
  ADR-0033 section 12 records this as Laravel's unchanged behaviour.
- `IdleDelay` (sleep between empty polls) starts at 100 ms, doubles, and is capped at 1 s (ADR-0033
  section 10); a larger configured value is clamped to `RedisConsumerOptions.MaxIdleDelay`.

## OpenTelemetry `traceparent` propagation (ADR-0028)

A Redis list element has no native metadata channel, so to carry the active producer span's W3C
`traceparent` for true cross-hop **span** linkage the transport owns a tiny JSON **frame** that wraps
the bare envelope, beside it (never inside it):

```json
{"__bq_frame":1,"headers":{"traceparent":"00-…"},"body":"<raw wire envelope>"}
```

`RPUSH` stores the frame, so the stored value **is** the `LREM` ack handle and the reliable-queue
semantics are untouched. Framing is **opt-in and backward compatible**: a header-less publish stores
the bare envelope byte-for-byte, and the consumer un-frames a reserved value (a bare value from an
older/cross-version publisher still consumes with no headers). The frame is byte-compatible with the
Go/PHP Redis frame, so cross-language queues interoperate.

```csharp
using BabelQueue.Tracing;

// produce: BabelQueue.Core fills the carrier with the active span's traceparent
var headers = new Dictionary<string, string>();
await Telemetry.PublishAsync("urn:babel:orders:created", data, headers,
    env => new RedisPublisher(db, "orders").PublishWithHeadersAsync("urn:babel:orders:created", data, headers));

// consume: the handler receives the un-framed body + headers; wrap to link the span
["urn:babel:orders:created"] = (env, rawBody, headers, ct) =>
    Telemetry.Wrap(async e => { /* ... */ }, headers)(env)
```

With no `traceparent` the consumer falls back to the v0.1 `trace_id` mapping. Requires
`BabelQueue.Core 1.4.0`.

## Build & test

```bash
dotnet test
```

`IDatabase` is an interface, so the unit tests mock it with Moq — no Redis, no network.

## License

MIT
