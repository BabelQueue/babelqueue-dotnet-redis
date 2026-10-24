using System.Diagnostics;
using System.Text.Json;
using System.Text.RegularExpressions;
using BabelQueue;
using BabelQueue.Redis;
using StackExchange.Redis;
using Xunit;

namespace BabelQueue.Redis.Tests;

/// <summary>
/// End-to-end behaviour of both layouts against a real Redis (CI runs a <c>redis:7</c> service). These
/// run the Lua scripts for real — the scripted-database tests only prove which scripts are called.
/// Skipped locally when Redis is unreachable and <c>BQ_REQUIRE_REDIS</c> is unset.
/// </summary>
public sealed class RedisLiveTests : IDisposable
{
    private const string Urn = "urn:babel:orders:created";

    private readonly IDatabase _db = RedisLive.Required || RedisLive.Available ? RedisLive.Database() : null!;
    private readonly string _queue = "bqlive-" + Guid.NewGuid().ToString("N");
    private double _now = 1_700_000_000.4;

    private Func<double> Clock => () => _now;

    public void Dispose()
    {
        if (_db is null)
        {
            return;
        }

        foreach (var suffix in new[] { "", ":delayed", ":reserved", ":notify", ".dlq", ".dlq:notify", ":processing", ":processing:deadlines" })
        {
            _db.KeyDelete("queues:" + _queue + suffix);
            _db.KeyDelete(_queue + suffix);
        }
    }

    private RedisConsumer Zset(Dictionary<string, BabelHandler> handlers, Action<RedisConsumerOptions>? tweak = null)
    {
        var options = new RedisConsumerOptions { Layout = RedisLayout.Zset };
        tweak?.Invoke(options);
        return new RedisConsumer(_db, _queue, handlers, options, Clock);
    }

    private RedisConsumer Legacy(Dictionary<string, BabelHandler> handlers, Action<RedisConsumerOptions>? tweak = null)
    {
        var options = new RedisConsumerOptions();
        tweak?.Invoke(options);
        return new RedisConsumer(_db, _queue, handlers, options, Clock);
    }

    private RedisPublisher ZsetPublisher() => new(_db, _queue, new RedisPublisherOptions { Layout = RedisLayout.Zset }, Clock);

    private static Dictionary<string, BabelHandler> Collect(List<(Envelope Env, string Raw)> into)
        => new() { [Urn] = (env, raw, _, _) => { into.Add((env, raw)); return Task.CompletedTask; } };

    private static Dictionary<string, BabelHandler> Failing()
        => new() { [Urn] = (_, _, _, _) => throw new InvalidOperationException("boom") };

    private static string Body(int attempts = 0, string extra = "")
    {
        var env = EnvelopeCodec.Make(Urn, new Dictionary<string, object?> { ["order_id"] = 7 }, "orders");
        var json = EnvelopeCodec.Encode(env with { Attempts = attempts });
        return extra.Length == 0 ? json : json[..^1] + "," + extra + "}";
    }

    // ------------------------------------------------------------------ zset layout

    [LiveFact]
    public async Task ZsetPublishAndConsumeRoundTrips()
    {
        var seen = new List<(Envelope, string)>();
        var id = await ZsetPublisher().PublishAsync(Urn, new Dictionary<string, object?> { ["order_id"] = 1 });

        Assert.Equal(1, await Zset(Collect(seen)).PollAsync());

        var (env, _) = Assert.Single(seen);
        Assert.Equal(id, env.Meta!.Id);
        Assert.Equal(1, env.Attempts);
        Assert.Equal(0, await _db.SortedSetLengthAsync("queues:" + _queue + ":reserved"));
        Assert.Equal(0, await _db.ListLengthAsync("queues:" + _queue));
    }

    [LiveFact]
    public async Task ZsetReservedButUnackedMessageIsRecoveredAfterRetryAfterWithAttemptsPlusOne()
    {
        await ZsetPublisher().PublishAsync(Urn, new Dictionary<string, object?> { ["order_id"] = 1 });
        var first = new List<(Envelope, string)>();
        var crashing = Zset(
            new Dictionary<string, BabelHandler> { [Urn] = (env, raw, _, _) => { first.Add((env, raw)); throw new InvalidOperationException("crash"); } },
            o => o.RetryAfter = TimeSpan.FromSeconds(60));
        await crashing.PollAsync();
        Assert.Equal(1, Assert.Single(first).Item1.Attempts);
        Assert.Equal(1, await _db.SortedSetLengthAsync("queues:" + _queue + ":reserved"));

        // Before the deadline the second consumer sees nothing; after it the entry migrates back.
        var second = new List<(Envelope, string)>();
        _now += 30;
        Assert.Equal(0, await Zset(Collect(second), o => o.RetryAfter = TimeSpan.FromSeconds(60)).PollAsync());
        _now += 40;
        Assert.Equal(1, await Zset(Collect(second), o => o.RetryAfter = TimeSpan.FromSeconds(60)).PollAsync());

        Assert.Equal(2, Assert.Single(second).Item1.Attempts);
    }

    [LiveFact]
    public async Task ZsetPoisonElementsAreDeadLetteredAndLeaveNothingReserved()
    {
        var undecodable = "{not json at all";
        var stringAttempts = Body().Replace("\"attempts\":0", "\"attempts\":\"0\"", StringComparison.Ordinal);
        await _db.ListRightPushAsync("queues:" + _queue, undecodable);
        await _db.ListRightPushAsync("queues:" + _queue, stringAttempts);
        await _db.ListRightPushAsync("queues:" + _queue + ":notify", new RedisValue[] { "1", "1" });
        var seen = new List<(Envelope, string)>();
        var warnings = new List<string>();
        var consumer = Zset(Collect(seen), o => o.OnWarning = warnings.Add);

        await consumer.PollAsync();
        await consumer.PollAsync();

        Assert.Empty(seen);
        Assert.Equal(0, await _db.SortedSetLengthAsync("queues:" + _queue + ":reserved"));
        var dead = (await _db.ListRangeAsync("queues:" + _queue + ".dlq")).Select(v => (string)v!).ToList();
        Assert.Equal(2, dead.Count);
        using (var wrapped = System.Text.Json.JsonDocument.Parse(dead[0]))
        {
            Assert.Equal(undecodable, wrapped.RootElement.GetProperty("raw").GetString());
        }
        RedisConsumerTests.AssertPoisonBlock(dead[0], _queue);
        RedisConsumerTests.AssertSpliced(stringAttempts, dead[1]);
        RedisConsumerTests.AssertPoisonBlock(dead[1], _queue);
    }

    [LiveFact]
    public async Task ZsetReleaseKeepsEveryFieldAndOnlyBumpsAttempts()
    {
        var raw = Body(extra: "\"extra_obj\":{},\"extra_arr\":[],\"big\":123456789012345,\"ratio\":1.5,\"nothing\":null,\"flag\":true,\"label\":\"x\",\"nested\":{\"a\":[1,2,{}]}");
        Assert.Single(Regex.Matches(raw, "\"attempts\":0", RegexOptions.None, TimeSpan.FromSeconds(1)));
        await _db.ListRightPushAsync("queues:" + _queue, raw);
        await _db.ListRightPushAsync("queues:" + _queue + ":notify", "1");
        var consumer = Zset(Failing(), o => { o.OnHandlerFailure = RedisFailureAction.Release; o.ReleaseDelay = TimeSpan.FromSeconds(3.2); o.OnError = (_, _, _) => { }; });

        await consumer.PollAsync();

        Assert.Equal(0, await _db.SortedSetLengthAsync("queues:" + _queue + ":reserved"));
        var parked = await _db.SortedSetRangeByRankWithScoresAsync("queues:" + _queue + ":delayed");
        var entry = Assert.Single(parked);
        Assert.Equal(Math.Floor(_now) + 4, entry.Score);

        // The expected body is the original text with ONLY the attempts value changed: it is built by a plain
        // string replacement, never by RedisReleaseBody, so a regression in Build cannot hide itself.
        var expected = raw.Replace("\"attempts\":0", "\"attempts\":1", StringComparison.Ordinal);
        Assert.Equal(expected, (string)entry.Element!);
        Assert.Contains("\"extra_obj\":{}", expected, StringComparison.Ordinal);
        Assert.Contains("\"extra_arr\":[]", expected, StringComparison.Ordinal);
    }

    [LiveFact]
    public async Task ZsetDelayedMessageArrivesWithinTheDocumentedWindow()
    {
        var options = new RedisPublisherOptions { Layout = RedisLayout.Zset };
        var publisher = new RedisPublisher(_db, _queue, options);
        var seen = new List<(Envelope, string)>();
        var consumer = new RedisConsumer(_db, _queue, Collect(seen), new RedisConsumerOptions { Layout = RedisLayout.Zset });

        var watch = Stopwatch.StartNew();
        await publisher.PublishDelayedAsync(Urn, null, TimeSpan.FromSeconds(2));
        Assert.Equal(0, await consumer.PollAsync());
        Assert.Equal(0, await _db.ListLengthAsync("queues:" + _queue + ":notify"));

        while (seen.Count == 0 && watch.Elapsed < TimeSpan.FromSeconds(6))
        {
            await consumer.PollAsync();
            await Task.Delay(100);
        }

        Assert.Single(seen);
        Assert.InRange(watch.Elapsed.TotalSeconds, 1.0, 3.5);
    }

    [LiveFact]
    public async Task ZsetAckAfterTheReservationExpiredWarnsAndTouchesNothing()
    {
        await ZsetPublisher().PublishAsync(Urn, null);
        var warnings = new List<string>();
        var consumer = Zset(
            new Dictionary<string, BabelHandler>
            {
                [Urn] = async (_, _, _, _) =>
                {
                    // The reservation lapses and is recovered by someone else while the handler is still running.
                    await _db.KeyDeleteAsync("queues:" + _queue + ":reserved");
                },
            },
            o => o.OnWarning = warnings.Add);

        await consumer.PollAsync();

        Assert.Equal(new[] { "reservation expired" }, warnings);
    }

    [LiveFact]
    public async Task ZsetIdenticalPoisonCopiesShareOneReservationSoOnlyOneReachesTheDeadLetterList()
    {
        // Known contract-level limit (ADR-0033 section 3: the raw job is the :reserved member): two workers
        // that pop byte-identical poison elements collapse into one member, and only the first DLQ move wins.
        var poison = "{not json at all";
        await _db.ListRightPushAsync("queues:" + _queue, new RedisValue[] { poison, poison });
        await _db.ListRightPushAsync("queues:" + _queue + ":notify", new RedisValue[] { "1", "1" });
        var queue = new RedisQueue(_db, _queue, RedisLayout.Zset, RedisQueue.DefaultKeyPrefix, ":processing", TimeSpan.FromSeconds(90), -1, Clock);

        var a = await queue.ReserveAsync();
        var b = await queue.ReserveAsync();

        Assert.True(a!.IsPoison);
        Assert.True(b!.IsPoison);
        Assert.Equal(1, await _db.SortedSetLengthAsync("queues:" + _queue + ":reserved"));
        Assert.True(await queue.DeadLetterAsync(a, a.HandlerBody));
        Assert.False(await queue.DeadLetterAsync(b, b.HandlerBody));
        Assert.Equal(new RedisValue[] { poison }, await _db.ListRangeAsync("queues:" + _queue + ".dlq"));
    }

    [LiveFact]
    public async Task ZsetIdenticalCopiesCollapseIntoOneReservationMember()
    {
        // The zset layout keys a reservation by the (re-encoded) body (ADR-0033 section 3), so two
        // byte-identical copies share ONE :reserved member; they do not hold independent reservations.
        var raw = Body();
        await _db.ListRightPushAsync("queues:" + _queue, new RedisValue[] { raw, raw });
        await _db.ListRightPushAsync("queues:" + _queue + ":notify", new RedisValue[] { "1", "1" });
        var warnings = new List<string>();
        var errors = new List<Exception>();
        var seen = new List<(Envelope, string)>();
        var second = Zset(Collect(seen), o => { o.OnWarning = warnings.Add; o.OnError = (e, _, _) => errors.Add(e); });
        var nestedPolls = new List<int>();
        long reservedWhileBothHeld = -1;
        var first = Zset(
            new Dictionary<string, BabelHandler>
            {
                [Urn] = async (_, _, _, _) =>
                {
                    // While the first copy is still being handled, a second consumer reserves, handles and
                    // acks the identical copy. Asserts run outside the handler (it would swallow them).
                    nestedPolls.Add(await second.PollAsync());
                    reservedWhileBothHeld = await _db.SortedSetLengthAsync("queues:" + _queue + ":reserved");
                },
            },
            o => { o.OnWarning = warnings.Add; o.OnError = (e, _, _) => errors.Add(e); });

        Assert.Equal(1, await first.PollAsync());

        Assert.Empty(errors);
        Assert.Equal(new[] { 1 }, nestedPolls);
        Assert.Single(seen);

        // The second consumer's ack removed the shared member, so the first consumer's ack finds nothing.
        Assert.Equal(0, reservedWhileBothHeld);
        Assert.Equal(new[] { RedisQueue.ExpiredWarning }, warnings);
        Assert.Equal(0, await _db.SortedSetLengthAsync("queues:" + _queue + ":reserved"));
    }

    [LiveFact]
    public async Task ZsetUnknownUrnIsDeadLetteredAndLeavesNothingReserved()
    {
        await ZsetPublisher().PublishAsync(Urn, new Dictionary<string, object?> { ["order_id"] = 1 });
        var errors = new List<Exception>();

        Assert.Equal(1, await Zset(new Dictionary<string, BabelHandler>(), o => o.OnError = (e, _, _) => errors.Add(e)).PollAsync());

        Assert.Single(errors);
        var dead = Assert.Single(await _db.ListRangeAsync("queues:" + _queue + ".dlq"));
        using var doc = JsonDocument.Parse((string)dead!);
        Assert.Equal("unknown_urn", doc.RootElement.GetProperty("dead_letter").GetProperty("reason").GetString());
        Assert.Equal(1, await _db.ListLengthAsync("queues:" + _queue + ".dlq"));
        Assert.Equal(0, await _db.SortedSetLengthAsync("queues:" + _queue + ":reserved"));
    }

    [LiveFact]
    public async Task ZsetRetryAfterNullAckDoesNotWarn()
    {
        await ZsetPublisher().PublishAsync(Urn, null);
        var warnings = new List<string>();
        var seen = new List<(Envelope, string)>();

        Assert.Equal(1, await Zset(Collect(seen), o => { o.RetryAfter = null; o.OnWarning = warnings.Add; }).PollAsync());

        Assert.Single(seen);
        Assert.Empty(warnings);
        Assert.Equal(0, await _db.SortedSetLengthAsync("queues:" + _queue + ":reserved"));
    }

    [LiveFact]
    public async Task ZsetRetryAfterNullDisablesRecovery()
    {
        await ZsetPublisher().PublishAsync(Urn, null);
        var first = Zset(Failing(), o => { o.RetryAfter = null; o.OnError = (_, _, _) => { }; });
        await first.PollAsync();
        var reserved = await _db.SortedSetRangeByRankWithScoresAsync("queues:" + _queue + ":reserved");
        Assert.True(double.IsPositiveInfinity(Assert.Single(reserved).Score));

        _now += 100_000;
        var seen = new List<(Envelope, string)>();
        Assert.Equal(0, await Zset(Collect(seen), o => o.RetryAfter = null).PollAsync());
        Assert.Equal(1, await _db.SortedSetLengthAsync("queues:" + _queue + ":reserved"));
    }

    // ------------------------------------------------------------------ legacy layout

    [LiveFact]
    public async Task LegacyPublishConsumeAndAckEmptiesBothKeys()
    {
        await new RedisPublisher(_db, _queue).PublishAsync(Urn, new Dictionary<string, object?> { ["order_id"] = 1 });
        var seen = new List<(Envelope, string)>();

        Assert.Equal(1, await Legacy(Collect(seen)).PollAsync());

        Assert.Single(seen);
        Assert.Equal(0, await _db.ListLengthAsync(_queue + ":processing"));
        Assert.Equal(0, await _db.SortedSetLengthAsync(_queue + ":processing:deadlines"));
    }

    [LiveFact]
    public async Task LegacyUnknownUrnIsDeadLetteredAndLeavesNothingProcessing()
    {
        await new RedisPublisher(_db, _queue).PublishAsync(Urn, new Dictionary<string, object?> { ["order_id"] = 1 });
        var errors = new List<Exception>();

        Assert.Equal(1, await Legacy(new Dictionary<string, BabelHandler>(), o => o.OnError = (e, _, _) => errors.Add(e)).PollAsync());

        Assert.Single(errors);
        var dead = Assert.Single(await _db.ListRangeAsync(_queue + ".dlq"));
        using var doc = JsonDocument.Parse((string)dead!);
        Assert.Equal("unknown_urn", doc.RootElement.GetProperty("dead_letter").GetProperty("reason").GetString());
        Assert.Equal(1, await _db.ListLengthAsync(_queue + ".dlq"));
        Assert.Equal(0, await _db.ListLengthAsync(_queue + ":processing"));
        Assert.Equal(0, await _db.SortedSetLengthAsync(_queue + ":processing:deadlines"));
    }

    [LiveFact]
    public async Task LegacyKilledConsumerIsRecoveredByteIdenticalWithoutAnAttemptsIncrement()
    {
        var raw = Body();
        await _db.ListRightPushAsync(_queue, raw);
        var dying = Legacy(Failing(), o => { o.RetryAfter = TimeSpan.FromSeconds(60); o.OnError = (_, _, _) => { }; });
        await dying.PollAsync();
        Assert.Equal(1, await _db.ListLengthAsync(_queue + ":processing"));

        _now += 61;
        var seen = new List<(Envelope, string)>();
        Assert.Equal(1, await Legacy(Collect(seen), o => o.RetryAfter = TimeSpan.FromSeconds(60)).PollAsync());

        var (env, delivered) = Assert.Single(seen);
        Assert.Equal(raw, delivered);
        Assert.Equal(0, env.Attempts);
    }

    [LiveFact]
    public async Task LegacyProcessingItemWithoutADeadlineStays()
    {
        await _db.ListRightPushAsync(_queue + ":processing", Body());
        var seen = new List<(Envelope, string)>();
        _now += 1_000_000;

        Assert.Equal(0, await Legacy(Collect(seen)).PollAsync());

        Assert.Equal(1, await _db.ListLengthAsync(_queue + ":processing"));
    }

    [LiveFact]
    public async Task LegacyLateAckWarnsAndLeavesTheRedeliveredCopyAlone()
    {
        var raw = Body();
        await _db.ListRightPushAsync(_queue, raw);
        var warnings = new List<string>();
        var redelivered = Legacy(Failing(), o => { o.RetryAfter = TimeSpan.FromSeconds(60); o.OnError = (_, _, _) => { }; });
        var consumer = Legacy(
            new Dictionary<string, BabelHandler>
            {
                [Urn] = async (_, _, _, _) =>
                {
                    // A's reservation expires while its handler is still running: another worker's sweep returns
                    // the element to the ready list and B reserves it (and holds it).
                    _now += 61;
                    Assert.Equal(1, await redelivered.PollAsync());
                },
            },
            o => { o.RetryAfter = TimeSpan.FromSeconds(60); o.OnWarning = warnings.Add; });

        await consumer.PollAsync();

        // A's late ack warned and did not remove B's copy or B's deadline member.
        Assert.Equal(new[] { "reservation expired" }, warnings);
        Assert.Equal(1, await _db.ListLengthAsync(_queue + ":processing"));
        Assert.Equal(1, await _db.SortedSetLengthAsync(_queue + ":processing:deadlines"));

        // B's reservation lapses in turn: a third worker recovers the byte-identical body, acks it, and nothing is left.
        _now += 61;
        var seen = new List<(Envelope, string)>();
        var cleanupWarnings = new List<string>();
        Assert.Equal(1, await Legacy(Collect(seen), o => { o.RetryAfter = TimeSpan.FromSeconds(60); o.OnWarning = cleanupWarnings.Add; }).PollAsync());

        Assert.Equal(raw, Assert.Single(seen).Item2);
        Assert.Empty(cleanupWarnings);
        Assert.Equal(0, await _db.ListLengthAsync(_queue));
        Assert.Equal(0, await _db.ListLengthAsync(_queue + ":processing"));
        Assert.Equal(0, await _db.SortedSetLengthAsync(_queue + ":processing:deadlines"));
    }

    [LiveFact]
    public async Task LegacyIdenticalCopiesAreAckedIndependentlyWhileTheOtherIsHeld()
    {
        var raw = Body();
        await _db.ListRightPushAsync(_queue, new RedisValue[] { raw, raw });
        var warnings = new List<string>();
        var holder = Legacy(Failing(), o => { o.RetryAfter = TimeSpan.FromSeconds(60); o.OnError = (_, _, _) => { }; });
        var nested = new List<int>();
        var acker = Legacy(
            new Dictionary<string, BabelHandler>
            {
                [Urn] = async (_, _, _, _) =>
                {
                    // The identical second copy is reserved (and held) while this reservation is still open.
                    nested.Add(await holder.PollAsync());
                    Assert.Equal(2, await _db.SortedSetLengthAsync(_queue + ":processing:deadlines"));
                },
            },
            o => { o.RetryAfter = TimeSpan.FromSeconds(60); o.OnWarning = warnings.Add; });

        Assert.Equal(1, await acker.PollAsync());

        // Both reservations succeeded: the acker's ack succeeded (no warning) and removed exactly one copy.
        Assert.Equal(new[] { 1 }, nested);
        Assert.Empty(warnings);
        Assert.Equal(1, await _db.ListLengthAsync(_queue + ":processing"));
        Assert.Equal(1, await _db.SortedSetLengthAsync(_queue + ":processing:deadlines"));

        // The held copy is recovered after its deadline and acked on its own.
        _now += 61;
        var seen = new List<(Envelope, string)>();
        Assert.Equal(1, await Legacy(Collect(seen), o => o.RetryAfter = TimeSpan.FromSeconds(60)).PollAsync());
        Assert.Equal(raw, Assert.Single(seen).Item2);
        Assert.Equal(0, await _db.ListLengthAsync(_queue + ":processing"));
        Assert.Equal(0, await _db.SortedSetLengthAsync(_queue + ":processing:deadlines"));
    }

    [LiveFact]
    public async Task LegacyRetryAfterNullReservesWithAnInfiniteScoreButStillSweepsAForeignExpiredMember()
    {
        // A finite reservation held by another (sweeping) worker...
        var foreign = Body(extra: "\"tag\":\"foreign\"");
        await _db.ListRightPushAsync(_queue, foreign);
        await Legacy(Failing(), o => { o.RetryAfter = TimeSpan.FromSeconds(60); o.OnError = (_, _, _) => { }; }).PollAsync();

        // ...and one reserved by a RetryAfter=null consumer, which uses an infinite score.
        var own = Body(extra: "\"tag\":\"own\"");
        await _db.ListRightPushAsync(_queue, own);
        await Legacy(Failing(), o => { o.RetryAfter = null; o.OnError = (_, _, _) => { }; }).PollAsync();
        var members = await _db.SortedSetRangeByRankWithScoresAsync(_queue + ":processing:deadlines");
        Assert.Equal(2, members.Length);
        Assert.Single(members, m => double.IsPositiveInfinity(m.Score));
        Assert.Single(members, m => !double.IsInfinity(m.Score));

        // Long after the finite deadline, a null consumer is not a kill switch (broker-bindings section 1.7):
        // its sweep still returns the foreign expired member to ready, and it then reserves and handles it.
        _now += 1_000_000;
        var seen = new List<(Envelope, string)>();
        Assert.Equal(1, await Legacy(Collect(seen), o => o.RetryAfter = null).PollAsync());

        Assert.Equal(foreign, Assert.Single(seen).Item2);
        Assert.Equal(0, await _db.ListLengthAsync(_queue));

        // Only the null consumer's own reservation is left, still held with the +inf score.
        Assert.Equal(new RedisValue[] { own }, await _db.ListRangeAsync(_queue + ":processing"));
        var remaining = Assert.Single(await _db.SortedSetRangeByRankWithScoresAsync(_queue + ":processing:deadlines"));
        Assert.True(double.IsPositiveInfinity(remaining.Score));
    }

    [LiveFact]
    public async Task LegacyRetryAfterNullAckDoesNotWarn()
    {
        await _db.ListRightPushAsync(_queue, Body());
        var warnings = new List<string>();
        var seen = new List<(Envelope, string)>();

        Assert.Equal(1, await Legacy(Collect(seen), o => { o.RetryAfter = null; o.OnWarning = warnings.Add; }).PollAsync());

        Assert.Single(seen);
        Assert.Empty(warnings);
        Assert.Equal(0, await _db.ListLengthAsync(_queue + ":processing"));
        Assert.Equal(0, await _db.SortedSetLengthAsync(_queue + ":processing:deadlines"));
    }

    [LiveFact]
    public async Task LegacyReservationTokensAre32LowercaseHex()
    {
        await _db.ListRightPushAsync(_queue, Body());
        await Legacy(Failing(), o => { o.RetryAfter = TimeSpan.FromSeconds(60); o.OnError = (_, _, _) => { }; }).PollAsync();

        var member = (string)(await _db.SortedSetRangeByRankAsync(_queue + ":processing:deadlines")).Single()!;

        Assert.Matches(new Regex("^[0-9a-f]{32}:", RegexOptions.None, TimeSpan.FromSeconds(1)), member);
        Assert.Equal(':', member[32]);
    }

    [Fact]
    public void TokenSourceUsesOnlyTheCryptographicGenerator()
    {
        var source = File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", "..", "src", "BabelQueue.Redis", "RedisQueue.cs"));

        Assert.Contains("RandomNumberGenerator", source, StringComparison.Ordinal);
        Assert.DoesNotContain("System.Random", source, StringComparison.Ordinal);
        Assert.DoesNotContain("new Random", source, StringComparison.Ordinal);
        Assert.Matches(new Regex("^[0-9a-f]{32}$", RegexOptions.None, TimeSpan.FromSeconds(1)), RedisQueue.NewToken());
    }
}
