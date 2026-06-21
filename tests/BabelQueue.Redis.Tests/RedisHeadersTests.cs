using System.Text.Json;
using BabelQueue;
using BabelQueue.Redis;
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
    private const string Processing = "orders:processing";
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

        var queue = new Queue<string>(new[] { stored });
        var mock = new Mock<IDatabase>();
        mock.Setup(d => d.ListMoveAsync(Queue, Processing, ListSide.Left, ListSide.Right, It.IsAny<CommandFlags>()))
            .ReturnsAsync(() => queue.Count > 0 ? queue.Dequeue() : RedisValue.Null);
        mock.Setup(d => d.ListRemoveAsync(Processing, It.IsAny<RedisValue>(), It.IsAny<long>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync(1L);

        string? rawSeen = null;
        IReadOnlyDictionary<string, string>? headersSeen = null;
        var handlers = new Dictionary<string, BabelHandler>
        {
            ["urn:babel:orders:created"] = (_, raw, headers, _) => { rawSeen = raw; headersSeen = headers; return Task.CompletedTask; },
        };

        await new RedisConsumer(mock.Object, Queue, handlers).PollAsync();

        // The handler sees the un-framed wire envelope, plus the carried traceparent header.
        Assert.Equal(env, rawSeen);
        Assert.Equal(Traceparent, headersSeen!["traceparent"]);
        // Ack must LREM the *stored* value (the frame), not the un-framed body.
        mock.Verify(d => d.ListRemoveAsync(Processing, stored, 1, It.IsAny<CommandFlags>()), Times.Once);
        mock.Verify(d => d.ListRemoveAsync(Processing, env, 1, It.IsAny<CommandFlags>()), Times.Never);
    }

    [Fact]
    public async Task ExtractedHeadersRebuildTheRemoteParentForTelemetryWrap()
    {
        var env = EnvelopeCodec.Encode(EnvelopeCodec.Make("urn:babel:orders:created", null, Queue));
        var stored = RedisFrame.FrameValue(env, new Dictionary<string, string> { ["traceparent"] = Traceparent });

        var queue = new Queue<string>(new[] { stored });
        var mock = new Mock<IDatabase>();
        mock.Setup(d => d.ListMoveAsync(Queue, Processing, ListSide.Left, ListSide.Right, It.IsAny<CommandFlags>()))
            .ReturnsAsync(() => queue.Count > 0 ? queue.Dequeue() : RedisValue.Null);
        mock.Setup(d => d.ListRemoveAsync(Processing, It.IsAny<RedisValue>(), It.IsAny<long>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync(1L);

        System.Diagnostics.ActivityContext? parent = null;
        var handlers = new Dictionary<string, BabelHandler>
        {
            ["urn:babel:orders:created"] = (_, _, headers, _) =>
            {
                parent = BabelQueue.Tracing.Traceparent.RemoteParentFromHeaders(headers);
                return Task.CompletedTask;
            },
        };

        await new RedisConsumer(mock.Object, Queue, handlers).PollAsync();

        Assert.NotNull(parent);
        Assert.Equal("0af7651916cd43dd8448eb211c80319c", parent!.Value.TraceId.ToHexString());
        Assert.Equal("b7ad6b7169203331", parent.Value.SpanId.ToHexString());
    }
}
