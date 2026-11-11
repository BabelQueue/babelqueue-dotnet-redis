using System.Diagnostics;
using System.Text.Json;
using BabelQueue;
using BabelQueue.Redis;
using BabelQueue.Tracing;
using Moq;
using StackExchange.Redis;
using Xunit;

namespace BabelQueue.Redis.Tests;

/// <summary>
/// ADR-0028 out-of-band header carrier for Redis: a transport-owned <c>__bq_frame</c> JSON frame
/// (byte-compatible with the Go/PHP frame) wraps the bare wire envelope so a W3C <c>traceparent</c>
/// rides beside it (GR-1 — never inside the envelope, <c>schema_version</c> stays 1). Framing is
/// opt-in: a header-less publish stores the bare envelope byte-for-byte, and a bare value still
/// consumes (back-compat). The stored frame is the <c>LREM</c> ack handle. No Redis server.
/// </summary>
public sealed class RedisHeadersTests
{
    private const string Queue = "orders";
    private const string Traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

    private static (Mock<IDatabase> Mock, Func<RedisValue> Pushed) MockDatabase()
    {
        RedisValue pushed = RedisValue.Null;
        var mock = new Mock<IDatabase>();
        mock.Setup(d => d.ListRightPushAsync(Queue, It.IsAny<RedisValue>(), It.IsAny<When>(), It.IsAny<CommandFlags>()))
            .Callback<RedisKey, RedisValue, When, CommandFlags>((_, value, _, _) => pushed = value)
            .ReturnsAsync(1L);
        return (mock, () => pushed);
    }

    // ---- Frame produce-side ----

    [Fact]
    public async Task PublishWithHeadersStoresAFrameWrappingTheBareEnvelope()
    {
        var (mock, pushed) = MockDatabase();
        var headers = new Dictionary<string, string> { ["traceparent"] = Traceparent };

        await new RedisPublisher(mock.Object, Queue)
            .PublishWithHeadersAsync("urn:babel:orders:created", new Dictionary<string, object?> { ["order_id"] = 1 }, headers);

        var stored = (string)pushed()!;
        using var doc = JsonDocument.Parse(stored);

        // The stored value is the transport frame, not the bare envelope.
        Assert.Equal(1, doc.RootElement.GetProperty("__bq_frame").GetInt32());
        Assert.Equal(Traceparent, doc.RootElement.GetProperty("headers").GetProperty("traceparent").GetString());

        // GR-1: the frame's body is the verbatim wire envelope — schema_version 1, no traceparent in it.
        var body = doc.RootElement.GetProperty("body").GetString()!;
        using var inner = JsonDocument.Parse(body);
        Assert.Equal(1, inner.RootElement.GetProperty("meta").GetProperty("schema_version").GetInt32());
        Assert.False(inner.RootElement.TryGetProperty("traceparent", out _));
        Assert.False(inner.RootElement.GetProperty("meta").TryGetProperty("traceparent", out _));

        // The body inside the frame is a byte-identical canonical envelope (re-encode round-trips).
        Assert.Equal(EnvelopeCodec.Encode(EnvelopeCodec.Decode(body)), body);
    }

    [Fact]
    public async Task HeaderLessPublishStoresTheBareEnvelopeByteForByte()
    {
        var (frameMock, framePushed) = MockDatabase();
        var (plainMock, plainPushed) = MockDatabase();
        const string trace = "11111111-2222-3333-4444-555555555555";

        // No usable headers -> degrades to the bare form, byte-identical to plain Publish.
        await new RedisPublisher(frameMock.Object, Queue).PublishWithHeadersAsync("urn:x:y", null, headers: null, traceId: trace);
        await new RedisPublisher(plainMock.Object, Queue).PublishAsync("urn:x:y", traceId: trace);

        var framed = (string)framePushed()!;
        Assert.DoesNotContain("__bq_frame", framed, StringComparison.Ordinal);
        // Both bodies are bare envelopes carrying the same continued trace (only meta.id differs).
        using var a = JsonDocument.Parse(framed);
        using var b = JsonDocument.Parse((string)plainPushed()!);
        Assert.Equal(trace, a.RootElement.GetProperty("trace_id").GetString());
        Assert.Equal(trace, b.RootElement.GetProperty("trace_id").GetString());
    }

    [Fact]
    public async Task BlankHeadersDegradeToTheBareForm()
    {
        var (mock, pushed) = MockDatabase();
        var headers = new Dictionary<string, string> { [""] = "v", ["k"] = "" };

        await new RedisPublisher(mock.Object, Queue).PublishWithHeadersAsync("urn:x:y", null, headers);

        Assert.DoesNotContain("__bq_frame", (string)pushed()!, StringComparison.Ordinal);
    }

    // ---- Frame consume-side (round-trip + back-compat) ----

    [Fact]
    public void FrameRoundTripsThroughUnframe()
    {
        var env = EnvelopeCodec.Encode(EnvelopeCodec.Make("urn:x:y", new Dictionary<string, object?> { ["a"] = 1 }, Queue));
        var headers = new Dictionary<string, string> { ["traceparent"] = Traceparent, ["tracestate"] = "vendor=1" };

        var stored = RedisFrame.FrameValue(env, headers);
        var (body, recovered) = RedisFrame.Unframe(stored);

        Assert.Equal(env, body);
        Assert.Equal(Traceparent, recovered["traceparent"]);
        Assert.Equal("vendor=1", recovered["tracestate"]);
    }

    [Fact]
    public void BareEnvelopeUnframesToItselfWithNoHeaders()
    {
        var env = EnvelopeCodec.Encode(EnvelopeCodec.Make("urn:x:y", null, Queue));

        var (body, headers) = RedisFrame.Unframe(env);

        Assert.Equal(env, body);
        Assert.Empty(headers);
    }

    [Fact]
    public void NonJsonAndSentinelLessValuesAreTreatedAsBare()
    {
        var (b1, h1) = RedisFrame.Unframe("not json");
        Assert.Equal("not json", b1);
        Assert.Empty(h1);

        var (b2, h2) = RedisFrame.Unframe("{\"job\":\"urn:x:y\"}"); // JSON, no sentinel -> bare
        Assert.Equal("{\"job\":\"urn:x:y\"}", b2);
        Assert.Empty(h2);

        var (b3, h3) = RedisFrame.Unframe(string.Empty);
        Assert.Equal(string.Empty, b3);
        Assert.Empty(h3);
    }

    [Fact]
    public void FrameValueIsByteCompatibleWithTheGoAndPhpFrame()
    {
        // {"__bq_frame":1,"headers":{"traceparent":"…"},"body":"…"} — same key order/shape as Go/PHP.
        var stored = RedisFrame.FrameValue("BODY", new Dictionary<string, string> { ["traceparent"] = Traceparent });
        Assert.StartsWith("{\"__bq_frame\":1,\"headers\":{", stored, StringComparison.Ordinal);
        Assert.Contains("\"body\":\"BODY\"", stored, StringComparison.Ordinal);
    }

    // ---- Consumer end-to-end over the frame ----

    [Fact]
    public async Task ConsumerUnframesAndSurfacesHeadersThenAcksWithTheStoredValue()
    {
        var env = EnvelopeCodec.Encode(EnvelopeCodec.Make("urn:babel:orders:created", new Dictionary<string, object?> { ["x"] = 1 }, Queue));
        var stored = RedisFrame.FrameValue(env, new Dictionary<string, string> { ["traceparent"] = Traceparent });

        var db = new ScriptDb()
            .Queue(RedisScripts.LegacyReserve, ScriptDb.Text(stored))
            .On(RedisScripts.LegacyAck, _ => ScriptDb.Integer(1))
            .On(RedisScripts.LegacySweep, _ => ScriptDb.Integer(0));

        string? rawSeen = null;
        IReadOnlyDictionary<string, string>? headersSeen = null;
        var handlers = new Dictionary<string, BabelHandler>
        {
            ["urn:babel:orders:created"] = (_, raw, headers, _) => { rawSeen = raw; headersSeen = headers; return Task.CompletedTask; },
        };

        await new RedisConsumer(db.Object, Queue, handlers).PollAsync();

        // The handler sees the un-framed wire envelope, plus the carried traceparent header.
        Assert.Equal(env, rawSeen);
        Assert.Equal(Traceparent, headersSeen!["traceparent"]);
        // Ack must present the *stored* value (the frame), not the un-framed body.
        var ack = Assert.Single(db.Of(RedisScripts.LegacyAck));
        Assert.Equal(stored, (string)ack.Values[1]!);
        Assert.NotEqual(env, (string)ack.Values[1]!);
    }

    [Fact]
    public async Task ExtractedHeadersRebuildTheRemoteParentForTelemetryWrap()
    {
        var env = EnvelopeCodec.Encode(EnvelopeCodec.Make("urn:babel:orders:created", null, Queue));
        var stored = RedisFrame.FrameValue(env, new Dictionary<string, string> { ["traceparent"] = Traceparent });

        var db = new ScriptDb()
            .Queue(RedisScripts.LegacyReserve, ScriptDb.Text(stored))
            .On(RedisScripts.LegacyAck, _ => ScriptDb.Integer(1))
            .On(RedisScripts.LegacySweep, _ => ScriptDb.Integer(0));

        System.Diagnostics.ActivityContext? parent = null;
        var handlers = new Dictionary<string, BabelHandler>
        {
            ["urn:babel:orders:created"] = (_, _, headers, _) =>
            {
                parent = BabelQueue.Tracing.Traceparent.RemoteParentFromHeaders(headers);
                return Task.CompletedTask;
            },
        };

        await new RedisConsumer(db.Object, Queue, handlers).PollAsync();

        Assert.NotNull(parent);
        Assert.Equal("0af7651916cd43dd8448eb211c80319c", parent!.Value.TraceId.ToHexString());
        Assert.Equal("b7ad6b7169203331", parent.Value.SpanId.ToHexString());
    }

    // O6: the envelope Telemetry.PublishAsync builds is the one that is sent, so the producer span's
    // messaging.message.id is the meta.id on the wire (and the traceparent rides beside the envelope).
    [Fact]
    public async Task TelemetryPublishEnvelopeRecordsTheWireMessageId()
    {
        var (mock, pushed) = MockDatabase();
        var publisher = new RedisPublisher(mock.Object, Queue);
        const string envelopeUrn = "urn:babel:orders:envelope-id";
        var activities = new List<Activity>();
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == Telemetry.ActivitySourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
            ActivityStopped = activity =>
            {
                if (activity.Kind == ActivityKind.Producer && Equals(activity.GetTagItem("babelqueue.urn"), envelopeUrn))
                {
                    lock (activities)
                    {
                        activities.Add(activity);
                    }
                }
            },
        };
        ActivitySource.AddActivityListener(listener);

        var headers = new Dictionary<string, string>();
        var id = await Telemetry.PublishAsync(
            envelopeUrn,
            new Dictionary<string, object?> { ["order_id"] = 7 },
            headers,
            env => publisher.PublishEnvelopeAsync(env, headers),
            "orders");

        var producerSpan = Assert.Single(activities);
        var stored = (string)pushed()!;
        string body = stored;
        string? traceparent = null;
        using (var frame = JsonDocument.Parse(stored))
        {
            if (frame.RootElement.TryGetProperty("__bq_frame", out _))
            {
                body = frame.RootElement.GetProperty("body").GetString()!;
                traceparent = frame.RootElement.GetProperty("headers").GetProperty("traceparent").GetString();
            }
        }
        var wire = EnvelopeCodec.Decode(body);

        Assert.Equal(id, wire.Meta!.Id);
        Assert.Equal(wire.Meta.Id, producerSpan.GetTagItem("messaging.message.id"));
        Assert.Equal(Telemetry.UuidOf(producerSpan.TraceId), wire.TraceId);
        Assert.Equal(wire.TraceId, producerSpan.GetTagItem("messaging.message.conversation_id"));
        Assert.Equal(EnvelopeCodec.SchemaVersion, wire.Meta.SchemaVersion);
        Assert.Equal(headers["traceparent"], traceparent);
        Assert.Contains(producerSpan.TraceId.ToHexString(), traceparent, StringComparison.Ordinal);
        Assert.DoesNotContain("traceparent", body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PublishEnvelopeSendsTheGivenEnvelopeUnchanged()
    {
        var (mock, pushed) = MockDatabase();
        var publisher = new RedisPublisher(mock.Object, Queue);
        var made = EnvelopeCodec.Make(
            "urn:babel:orders:created", new Dictionary<string, object?> { ["order_id"] = 7 }, "orders", "trace-fixed");
        var envelope = made with
        {
            Extras = new Dictionary<string, JsonElement> { ["x_tenant"] = JsonSerializer.SerializeToElement("t1") },
            Meta = made.Meta! with
            {
                Extras = new Dictionary<string, JsonElement> { ["x_region"] = JsonSerializer.SerializeToElement("eu") },
            },
        };

        var id = await publisher.PublishEnvelopeAsync(envelope);

        // no headers: the stored value is the bare envelope, byte-for-byte (no frame)
        var body = (string)pushed()!;
        Assert.DoesNotContain("__bq_frame", body, StringComparison.Ordinal);
        Assert.Equal(envelope.Meta!.Id, id);
        Assert.Equal(EnvelopeCodec.Encode(envelope), body);
        var wire = EnvelopeCodec.Decode(body);
        Assert.Equal("trace-fixed", wire.TraceId);
        Assert.Equal(EnvelopeCodec.SchemaVersion, wire.Meta!.SchemaVersion);
        Assert.Equal(envelope.Meta.Id, wire.Meta.Id);
        Assert.Equal("t1", wire.Extras!["x_tenant"].GetString());
        Assert.Equal("eu", wire.Meta.Extras!["x_region"].GetString());
    }

    // Documents the PublishEnvelopeAsync contract: meta.queue is whatever the envelope carries. Telemetry stamps
    // its queue argument (default "default"), not this publisher's target, so callers must pass the queue.
    [Fact]
    public async Task TelemetryPublishWithoutAQueueArgumentStampsDefaultMetaQueue()
    {
        var (mock, pushed) = MockDatabase();
        var publisher = new RedisPublisher(mock.Object, Queue);
        await Telemetry.PublishAsync(
            "urn:babel:orders:created", null, env => publisher.PublishEnvelopeAsync(env));

        // no headers: the stored value is the bare envelope, byte-for-byte (no frame)
        var body = (string)pushed()!;
        Assert.DoesNotContain("__bq_frame", body, StringComparison.Ordinal);
        Assert.Equal("default", EnvelopeCodec.Decode(body).Meta!.Queue);
    }
}
