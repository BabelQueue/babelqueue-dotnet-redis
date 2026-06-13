using System.Text.Json;
using BabelQueue;
using BabelQueue.Redis;
using Moq;
using StackExchange.Redis;
using Xunit;

namespace BabelQueue.Redis.Tests;

/// <summary>
/// Redis binding conformance against the vendored canonical suite's <c>redis</c> block
/// (broker-bindings §1). Redis lists carry no native metadata, so the only cross-SDK
/// invariant is <b>payload identity</b>: the queue element is the canonical envelope JSON,
/// byte-for-byte, with no wrapping and no added fields. No Redis, no network.
/// </summary>
public sealed class RedisConformanceTests
{
    private const string Queue = "orders";
    private static readonly string Dir = Path.Combine(AppContext.BaseDirectory, "conformance");

    private static JsonElement Redis()
    {
        using var doc = JsonDocument.Parse(File.ReadAllText(Path.Combine(Dir, "manifest.json")));
        return doc.RootElement.GetProperty("redis").Clone();
    }

    [Fact]
    public async Task PayloadIdentityMatchesGolden()
    {
        var identity = Redis().GetProperty("payload_identity");
        var fixtureBody = File.ReadAllText(Path.Combine(Dir, identity.GetProperty("envelope_file").GetString()!));
        var fixtureEnvelope = EnvelopeCodec.Decode(fixtureBody);

        // Capture exactly what RedisPublisher pushes onto the list.
        RedisValue pushed = RedisValue.Null;
        var mock = new Mock<IDatabase>();
        mock.Setup(d => d.ListRightPushAsync(Queue, It.IsAny<RedisValue>(), It.IsAny<When>(), It.IsAny<CommandFlags>()))
            .Callback<RedisKey, RedisValue, When, CommandFlags>((_, value, _, _) => pushed = value)
            .ReturnsAsync(1L);

        // Re-publish the fixture's logical message (same urn/data/trace) and assert the
        // stored element decodes to the same envelope and carries no extra wrapping.
        await new RedisPublisher(mock.Object, Queue).PublishAsync(
            EnvelopeCodec.Urn(fixtureEnvelope),
            fixtureEnvelope.Data,
            fixtureEnvelope.TraceId);

        var stored = (string)pushed!;

        // The element IS the envelope JSON — no wrapping object around it.
        using var storedDoc = JsonDocument.Parse(stored);
        Assert.Equal(JsonValueKind.Object, storedDoc.RootElement.ValueKind);
        Assert.Equal(
            new[] { "job", "trace_id", "data", "meta", "attempts" },
            storedDoc.RootElement.EnumerateObject().Select(p => p.Name).ToArray());

        // Byte-for-byte: the stored element equals the canonical encoding of the envelope it
        // decodes to (idempotent round-trip — no mutation, no added metadata).
        Assert.Equal(EnvelopeCodec.Encode(EnvelopeCodec.Decode(stored)), stored);

        // Contract fields are preserved verbatim from the fixture (per-message ids excepted).
        var storedEnvelope = EnvelopeCodec.Decode(stored);
        Assert.Equal(fixtureEnvelope.Job, storedEnvelope.Job);
        Assert.Equal(fixtureEnvelope.TraceId, storedEnvelope.TraceId);
        Assert.Equal(fixtureEnvelope.Attempts, storedEnvelope.Attempts);
        Assert.Equal(
            fixtureEnvelope.Data!["order_id"]!.ToString(),
            storedEnvelope.Data!["order_id"]!.ToString());
    }

    [Fact]
    public void ManifestRedisBlockHasNoProjection()
    {
        // Lock the contract intent: the redis block defines payload identity only — no
        // property/attribute projection (unlike sqs/asb/pulsar/kafka/rabbitmq).
        var redis = Redis();
        Assert.True(redis.TryGetProperty("payload_identity", out _));
        Assert.False(redis.TryGetProperty("property_projection", out _));
        Assert.False(redis.TryGetProperty("attribute_projection", out _));
    }
}
