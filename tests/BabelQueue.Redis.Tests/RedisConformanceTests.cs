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
    public async Task PayloadIdentityMatchesGolden() =>
        await RedisConformanceSuite.Find("redis", "payload_identity").Run();

    public static IEnumerable<object[]> PureCases() =>
        RedisConformanceSuite.All().Where(c => !c.NeedsRedis && c.Section != "redis").Select(c => new object[] { c.Section, c.Id });

    public static IEnumerable<object[]> LiveCases() =>
        RedisConformanceSuite.All().Where(c => c.NeedsRedis).Select(c => new object[] { c.Section, c.Id });

    /// <summary><c>redis_delay</c> score formula and <c>redis_layout.cases</c> key names: pure, no Redis.</summary>
    [Theory]
    [MemberData(nameof(PureCases))]
    public async Task PureCase(string section, string id) =>
        await RedisConformanceSuite.Find(section, id).Run();

    /// <summary>
    /// The behavioural <c>redis_layout</c> blocks against a live Redis (the real Lua scripts). Skipped
    /// without Redis unless <c>BQ_REQUIRE_REDIS</c> is set, in which case an unreachable Redis fails.
    /// </summary>
    [LiveTheory]
    [MemberData(nameof(LiveCases))]
    public async Task BehaviouralCase(string section, string id) =>
        await RedisConformanceSuite.Find(section, id).Run();

    /// <summary>
    /// Runs every Redis-scoped case and writes <c>results.json</c> to <c>BQ_CONFORMANCE_RESULTS</c> when
    /// that is set (ADR-0032 section 2). Never records a skip: an unreachable Redis is a failure.
    /// </summary>
    [LiveFact]
    public async Task ResultsHaveNoFailuresAndAreWrittenOnRequest()
    {
        var json = await RedisConformanceSuite.WriteResultsAsync(Environment.GetEnvironmentVariable("BQ_CONFORMANCE_RESULTS"));
        using var doc = JsonDocument.Parse(json);
        var failures = doc.RootElement.GetProperty("sections").EnumerateObject()
            .SelectMany(s => s.Value.EnumerateObject().Select(c => (Section: s.Name, Case: c.Name, Result: c.Value)))
            .Where(x => x.Result.GetProperty("status").GetString() != "pass")
            .Select(x => $"{x.Section}/{x.Case}: {x.Result.GetProperty("reason").GetString()}")
            .ToList();
        Assert.True(failures.Count == 0, string.Join(Environment.NewLine, failures));
        Assert.Equal("redis", Assert.Single(doc.RootElement.GetProperty("bindings").EnumerateArray()).GetString());
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
