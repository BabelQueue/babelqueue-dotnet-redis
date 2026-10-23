using System.Reflection;
using System.Text.Json;
using BabelQueue;
using BabelQueue.Redis;
using StackExchange.Redis;
using Xunit;

namespace BabelQueue.Redis.Tests;

/// <summary>
/// Zset (Laravel) layout behaviour over a scripted database: which scripts run, with which keys and
/// arguments, and how their results are interpreted. The Lua itself runs in the live-Redis tests.
/// </summary>
public sealed class RedisZsetTests
{
    private const string Urn = "urn:babel:orders:created";
    private const double Now = 1000.7;

    private static RedisConsumerOptions Zset(Action<RedisConsumerOptions>? tweak = null)
    {
        var options = new RedisConsumerOptions { Layout = RedisLayout.Zset };
        tweak?.Invoke(options);
        return options;
    }

    private static string Body(int attempts = 0)
    {
        var env = EnvelopeCodec.Make(Urn, new Dictionary<string, object?> { ["order_id"] = 7 }, "orders");
        return EnvelopeCodec.Encode(env with { Attempts = attempts });
    }

    private static ScriptDb Reserving(params RedisResult[] results)
        => new ScriptDb()
            .Queue(RedisScripts.ZsetReserve, results)
            .On(RedisScripts.ZsetMigrate, _ => ScriptDb.Integer(0))
            .On(RedisScripts.ZsetRelease, _ => ScriptDb.Integer(1))
            .On(RedisScripts.ZsetDeadLetter, _ => ScriptDb.Integer(1));

    // A normal delivery: the reserved member is the job with attempts bumped, so it differs from the raw job.
    private static RedisResult Delivered(string job, string? reserved = null) => ScriptDb.Pair(job, reserved ?? RedisReleaseBody.Build(job));

    // A poison delivery: Laravel's pop script answers {job, job} for an element it could not decode.
    private static RedisResult Poisoned(string job) => ScriptDb.Pair(job, job);

    private static RedisConsumer Consumer(ScriptDb db, BabelHandler? handler, RedisConsumerOptions? options = null)
        => new(
            db.Object,
            "orders",
            handler is null ? new Dictionary<string, BabelHandler>() : new Dictionary<string, BabelHandler> { [Urn] = handler },
            options ?? Zset(),
            () => Now);

    [Fact]
    public async Task EveryReserveMigratesDelayedThenReservedBeforePopping()
    {
        var db = Reserving();
        await Consumer(db, null).PollAsync();

        var order = db.Calls.Select(c => c.Script).ToList();
        Assert.Equal(new[] { RedisScripts.ZsetMigrate, RedisScripts.ZsetMigrate, RedisScripts.ZsetReserve }, order);

        var migrations = db.Of(RedisScripts.ZsetMigrate);
        Assert.Equal(new[] { "queues:orders:delayed", "queues:orders", "queues:orders:notify" }, migrations[0].Keys);
        Assert.Equal(new[] { "queues:orders:reserved", "queues:orders", "queues:orders:notify" }, migrations[1].Keys);
        Assert.Equal("1000", (string)migrations[0].Values[0]!);
        Assert.Equal("-1", (string)migrations[0].Values[1]!);
    }

    [Fact]
    public async Task RetryAfterNullSkipsTheReservedMigration()
    {
        var db = Reserving();
        await Consumer(db, null, Zset(o => o.RetryAfter = null)).PollAsync();

        Assert.Single(db.Of(RedisScripts.ZsetMigrate));
        Assert.Equal("queues:orders:delayed", db.Of(RedisScripts.ZsetMigrate)[0].Keys[0]);
        Assert.Equal("+inf", (string)db.Of(RedisScripts.ZsetReserve)[0].Values[0]!);
    }

    [Fact]
    public async Task MigrationBatchSizeIsPassedToTheScript()
    {
        var db = Reserving();
        await Consumer(db, null, Zset(o => o.MigrationBatchSize = 25)).PollAsync();
        Assert.All(db.Of(RedisScripts.ZsetMigrate), c => Assert.Equal("25", (string)c.Values[1]!));
    }

    [Fact]
    public async Task ReserveUsesLaravelKeysAndFloorNowPlusRetryAfter()
    {
        var db = Reserving();
        await Consumer(db, null).PollAsync();

        var reserve = Assert.Single(db.Of(RedisScripts.ZsetReserve));
        Assert.Equal(new[] { "queues:orders", "queues:orders:reserved", "queues:orders:notify" }, reserve.Keys);
        Assert.Equal("1090", (string)reserve.Values[0]!);
    }

    [Fact]
    public async Task CustomKeyPrefixAndRetryAfterAreHonoured()
    {
        var db = Reserving();
        var consumer = Consumer(db, null, Zset(o => { o.KeyPrefix = "bq:"; o.RetryAfter = TimeSpan.FromSeconds(30); }));
        await consumer.PollAsync();

        Assert.Equal("bq:orders", consumer.ReadyKey);
        Assert.Equal("bq:orders:reserved", consumer.ReservedKey);
        Assert.Equal("bq:orders.dlq", consumer.DeadLetterKey);
        Assert.Equal("1030", (string)db.Of(RedisScripts.ZsetReserve)[0].Values[0]!);
    }

    [Fact]
    public async Task HandlerSeesTheRawBodyWithAttemptsPlusOneAndTheAckRemovesTheReservedMember()
    {
        var raw = Body(attempts: 2);
        var reservedMember = raw.Replace("\"attempts\":2", "\"attempts\":3", StringComparison.Ordinal);
        var db = Reserving(Delivered(raw, reservedMember));
        string? seenRaw = null;
        Envelope? seen = null;

        await Consumer(db, (env, body, _, _) => { seen = env; seenRaw = body; return Task.CompletedTask; }).PollAsync();

        Assert.Equal(raw, seenRaw);
        Assert.Equal(3, seen!.Attempts);
        var removal = Assert.Single(db.SortedRemoves);
        Assert.Equal("queues:orders:reserved", removal.Key);
        Assert.Equal(reservedMember, (string)removal.Member!);
    }

    [Fact]
    public async Task PoisonElementIsDeadLetteredAndNeverReachesTheHandler()
    {
        const string poison = "{not json";
        var db = Reserving(Poisoned(poison));
        var warnings = new List<string>();
        var called = false;

        await Consumer(db, (_, _, _, _) => { called = true; return Task.CompletedTask; }, Zset(o => o.OnWarning = warnings.Add)).PollAsync();

        Assert.False(called);
        var dlq = Assert.Single(db.Of(RedisScripts.ZsetDeadLetter));
        Assert.Equal(new[] { "queues:orders:reserved", "queues:orders.dlq", "queues:orders.dlq:notify" }, dlq.Keys);
        Assert.Equal(poison, (string)dlq.Values[0]!);
        Assert.Equal(poison, (string)dlq.Values[1]!);
        Assert.Empty(db.SortedRemoves);
        Assert.Equal(new[] { "poison element moved to the dead-letter list" }, warnings);
    }

    [Fact]
    public async Task PoisonWhoseReservationExpiredWarnsInsteadOfWriting()
    {
        var db = Reserving(Poisoned("[]")).On(RedisScripts.ZsetDeadLetter, _ => ScriptDb.Integer(0));
        var warnings = new List<string>();

        await Consumer(db, null, Zset(o => o.OnWarning = warnings.Add)).PollAsync();

        Assert.Equal(new[] { RedisQueue.ExpiredWarning }, warnings);
    }

    [Fact]
    public async Task FailingHandlerLeavesTheMessageReservedByDefault()
    {
        var db = Reserving(Delivered(Body()));
        var errors = new List<Exception>();

        await Consumer(db, (_, _, _, _) => throw new InvalidOperationException("boom"), Zset(o => o.OnError = (e, _, _) => errors.Add(e))).PollAsync();

        Assert.Single(errors);
        Assert.Empty(db.Of(RedisScripts.ZsetRelease));
        Assert.Empty(db.SortedRemoves);
    }

    [Fact]
    public async Task FailingHandlerWithReleaseParksTheBodyOnDelayedWithAttemptsPlusOne()
    {
        var raw = Body(attempts: 1);
        var db = Reserving(Delivered(raw, "reserved-member"));

        await Consumer(
            db,
            (_, _, _, _) => throw new InvalidOperationException("boom"),
            Zset(o => { o.OnHandlerFailure = RedisFailureAction.Release; o.ReleaseDelay = TimeSpan.FromSeconds(4.2); })).PollAsync();

        var release = Assert.Single(db.Of(RedisScripts.ZsetRelease));
        Assert.Equal(new[] { "queues:orders:reserved", "queues:orders:delayed" }, release.Keys);
        Assert.Equal("reserved-member", (string)release.Values[0]!);
        Assert.Equal("1005", (string)release.Values[1]!); // floor(1000.7) + ceil(4.2)
        using var parked = JsonDocument.Parse((string)release.Values[2]!);
        Assert.Equal(2, parked.RootElement.GetProperty("attempts").GetInt32());
        Assert.Equal(Urn, parked.RootElement.GetProperty("job").GetString());
    }

    [Fact]
    public async Task ReleaseOnAnExpiredReservationWarns()
    {
        var db = Reserving(Delivered(Body())).On(RedisScripts.ZsetRelease, _ => ScriptDb.Integer(0));
        var warnings = new List<string>();

        await Consumer(
            db,
            (_, _, _, _) => throw new InvalidOperationException("boom"),
            Zset(o => { o.OnHandlerFailure = RedisFailureAction.Release; o.OnWarning = warnings.Add; })).PollAsync();

        Assert.Equal(new[] { RedisQueue.ExpiredWarning }, warnings);
    }

    [Fact]
    public async Task LateAckWarnsWhenTheReservedMemberIsGone()
    {
        var db = Reserving(Delivered(Body()));
        db.SortedRemoveResult = false;
        var warnings = new List<string>();

        await Consumer(db, (_, _, _, _) => Task.CompletedTask, Zset(o => o.OnWarning = warnings.Add)).PollAsync();

        Assert.Equal(new[] { RedisQueue.ExpiredWarning }, warnings);
    }

    [Fact]
    public async Task HeaderFramesAreNotUnwrappedOnTheZsetLayout()
    {
        var frame = RedisFrame.FrameValue(Body(), new Dictionary<string, string> { ["traceparent"] = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01" });
        var db = Reserving(Poisoned(frame));
        var errors = new List<Exception>();
        var called = false;

        await Consumer(db, (_, _, _, _) => { called = true; return Task.CompletedTask; }, Zset(o => o.OnError = (e, _, _) => errors.Add(e))).PollAsync();

        // A frame is not a conformant zset body: it is never unframed into a handler call.
        Assert.False(called);
    }

    [Fact]
    public async Task UnknownUrnWithoutFallbackStaysReservedAndReports()
    {
        var db = Reserving(Delivered(Body()));
        var errors = new List<Exception>();

        await Consumer(db, null, Zset(o => o.OnError = (e, _, _) => errors.Add(e))).PollAsync();

        Assert.IsType<UnknownUrnException>(Assert.Single(errors));
        Assert.Empty(db.SortedRemoves);
    }

    [Fact]
    public async Task UnknownUrnFallbackIsAcked()
    {
        var db = Reserving(Delivered(Body()));
        var seen = 0;

        await Consumer(db, null, Zset(o => o.OnUnknownUrn = (_, _, _) => { seen++; return Task.CompletedTask; })).PollAsync();

        Assert.Equal(1, seen);
        Assert.Single(db.SortedRemoves);
    }

    [Fact]
    public async Task CrossSlotIsSurfacedWithAHashTagExplanation()
    {
        var crossSlot = NewServerException("CROSSSLOT Keys in request don't hash to the same slot");
        var db = new ScriptDb().On(RedisScripts.ZsetMigrate, _ => throw crossSlot);

        var error = await Assert.ThrowsAsync<BabelQueueException>(() => Consumer(db, null).PollAsync());

        Assert.Contains("CROSSSLOT", error.Message, StringComparison.Ordinal);
        Assert.Contains("hash tag", error.Message, StringComparison.Ordinal);
        Assert.Same(crossSlot, error.InnerException);
    }

    private static RedisServerException NewServerException(string message)
    {
        var ctor = typeof(RedisServerException)
            .GetConstructors(BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance)
            .First(c => c.GetParameters().Length == 1 && c.GetParameters()[0].ParameterType == typeof(string));
        return (RedisServerException)ctor.Invoke(new object[] { message });
    }

    // ------------------------------------------------------------------ publisher

    [Fact]
    public async Task PublishPushesBodyAndNotifyTokenWithOneScript()
    {
        var db = new ScriptDb().On(RedisScripts.ZsetPush, _ => ScriptDb.Integer(1));
        var publisher = new RedisPublisher(db.Object, "orders", new RedisPublisherOptions { Layout = RedisLayout.Zset });

        var id = await publisher.PublishAsync(Urn, new Dictionary<string, object?> { ["order_id"] = 1 });

        Assert.Equal("queues:orders", publisher.ReadyKey);
        var push = Assert.Single(db.Of(RedisScripts.ZsetPush));
        Assert.Equal(new[] { "queues:orders", "queues:orders:notify" }, push.Keys);
        Assert.Equal(id, EnvelopeCodec.Decode((string)push.Values[0]!).Meta!.Id);
        Assert.Empty(db.Pushed);
    }

    [Fact]
    public async Task PublishWithHeadersDropsTheHeadersOnTheZsetLayout()
    {
        var db = new ScriptDb().On(RedisScripts.ZsetPush, _ => ScriptDb.Integer(1));
        var publisher = new RedisPublisher(db.Object, "orders", new RedisPublisherOptions { Layout = RedisLayout.Zset });

        await publisher.PublishWithHeadersAsync(
            Urn,
            null,
            new Dictionary<string, string> { ["traceparent"] = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01" });

        var stored = (string)Assert.Single(db.Of(RedisScripts.ZsetPush)).Values[0]!;
        Assert.DoesNotContain("__bq_frame", stored, StringComparison.Ordinal);
        Assert.DoesNotContain("traceparent", stored, StringComparison.Ordinal);
        Assert.Equal(EnvelopeCodec.Encode(EnvelopeCodec.Decode(stored)), stored);
    }

    [Theory]
    [InlineData(1000.7, 1.5, 1002)]
    [InlineData(1000.0, 2.0, 1002)]
    [InlineData(1000.99, 0.001, 1001)]
    [InlineData(1000.2, 0.0, 1000)]
    public async Task DelayedPublishAddsToDelayedWithoutANotifyToken(double now, double delay, double score)
    {
        var db = new ScriptDb();
        var publisher = new RedisPublisher(db.Object, "orders", new RedisPublisherOptions { Layout = RedisLayout.Zset, Clock = () => DateTimeOffset.FromUnixTimeMilliseconds((long)(now * 1000)) });

        await publisher.PublishDelayedAsync(Urn, null, TimeSpan.FromSeconds(delay));

        var added = Assert.Single(db.SortedAdds);
        Assert.Equal("queues:orders:delayed", added.Key);
        Assert.Equal(score, added.Score);
        Assert.Empty(db.Calls);
        Assert.Empty(db.Pushed);
    }

    [Fact]
    public async Task DelayedPublishNeedsTheZsetLayout()
    {
        var publisher = new RedisPublisher(new ScriptDb().Object, "orders");
        await Assert.ThrowsAsync<NotSupportedException>(() => publisher.PublishDelayedAsync(Urn, null, TimeSpan.FromSeconds(1)));
    }

    [Fact]
    public async Task NegativeDelayIsRejected()
    {
        var publisher = new RedisPublisher(new ScriptDb().Object, "orders", new RedisPublisherOptions { Layout = RedisLayout.Zset });
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => publisher.PublishDelayedAsync(Urn, null, TimeSpan.FromSeconds(-1)));
    }

    // ------------------------------------------------------------------ released body

    [Fact]
    public void ReleaseBodyChangesOnlyAttemptsAndKeepsUnknownKeysTypesAndOrder()
    {
        const string body = "{\"job\":\"urn:x\",\"extra_obj\":{},\"extra_arr\":[],\"big\":123456789012345,\"flag\":true,\"attempts\":2,\"nothing\":null}";
        var released = RedisReleaseBody.Build(body);
        Assert.Equal("{\"job\":\"urn:x\",\"extra_obj\":{},\"extra_arr\":[],\"big\":123456789012345,\"flag\":true,\"attempts\":3,\"nothing\":null}", released);
    }

    [Fact]
    public void ReleaseBodyLastDuplicateKeyWins()
    {
        Assert.Equal("{\"job\":\"b\",\"attempts\":1}", RedisReleaseBody.Build("{\"job\":\"a\",\"job\":\"b\",\"attempts\":0}"));
    }

    [Fact]
    public void ReleaseBodyHandlesFractionalAttempts()
    {
        Assert.Equal("{\"attempts\":1.5}", RedisReleaseBody.Build("{\"attempts\":0.5}"));
    }

    [Theory]
    [InlineData("not json")]
    [InlineData("[]")]
    [InlineData("{\"job\":\"x\"}")]
    [InlineData("{\"attempts\":\"2\"}")]
    public void ReleaseBodyRejectsWhatItCannotRewrite(string body)
    {
        Assert.Throws<BabelQueueException>(() => RedisReleaseBody.Build(body));
    }

    [Theory]
    [InlineData("{\"attempts\":0}", 1)]
    [InlineData("{\"attempts\":4}", 5)]
    [InlineData("{\"attempts\":-3}", 1)]
    [InlineData("{\"attempts\":\"x\"}", 1)]
    [InlineData("{}", 1)]
    [InlineData("[]", 1)]
    [InlineData("nope", 1)]
    public void ReportedAttemptsIsJobAttemptsPlusOne(string body, int expected)
    {
        Assert.Equal(expected, RedisReleaseBody.ReportedAttempts(body));
    }
}
