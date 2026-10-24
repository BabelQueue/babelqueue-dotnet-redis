using System.Text.Json;
using System.Text.RegularExpressions;
using BabelQueue;
using BabelQueue.Redis;
using StackExchange.Redis;
using Xunit;

namespace BabelQueue.Redis.Tests;

/// <summary>Legacy (<c>:processing</c>) layout consumer behaviour over a scripted database.</summary>
public sealed class RedisConsumerTests
{
    private const string Queue = "orders";
    private const string Processing = "orders:processing";
    private const string Deadlines = "orders:processing:deadlines";
    private const double Now = 1000.7;

    private static Func<double> Clock => () => Now;

    /// <summary>A database that yields the given bodies once each from the reserve script (then drains); acks succeed.</summary>
    private static ScriptDb Reserving(params string[] bodies)
        => new ScriptDb()
            .Queue(RedisScripts.LegacyReserve, bodies.Select(ScriptDb.Text).ToArray())
            .On(RedisScripts.LegacyAck, _ => ScriptDb.Integer(1))
            .On(RedisScripts.LegacyRelease, _ => ScriptDb.Integer(1))
            .On(RedisScripts.LegacyDeadLetter, _ => ScriptDb.Integer(1))
            .On(RedisScripts.LegacySweep, _ => ScriptDb.Integer(0));

    /// <summary>The <c>dead_letter</c> block of a poison record, asserted to be the ADR-0035 poison block of <c>orders</c>.</summary>
    internal static JsonElement AssertPoisonBlock(string record, string queue = Queue)
    {
        using var doc = JsonDocument.Parse(record);
        var block = doc.RootElement.GetProperty("dead_letter").Clone();
        Assert.Equal("poison", block.GetProperty("reason").GetString());
        Assert.Equal(queue, block.GetProperty("original_queue").GetString());
        Assert.Equal(0, block.GetProperty("attempts").GetInt32());
        Assert.Equal("dotnet", block.GetProperty("lang").GetString());
        Assert.True(block.GetProperty("failed_at").GetInt64() > 0);
        return block;
    }

    /// <summary>A spliced record keeps every original byte: <paramref name="body"/> minus its closing brace, then the block.</summary>
    internal static void AssertSpliced(string body, string record)
    {
        Assert.StartsWith(body[..^1] + ",\"dead_letter\":{", record, StringComparison.Ordinal);
        Assert.EndsWith("}}", record, StringComparison.Ordinal);
    }

    private static string Envelope(int attempts = 0)
    {
        var env = EnvelopeCodec.Make("urn:babel:orders:created", new Dictionary<string, object?> { ["order_id"] = 7 }, Queue);
        return EnvelopeCodec.Encode(env with { Attempts = attempts });
    }

    private static Dictionary<string, BabelHandler> Handlers(BabelHandler handler)
        => new() { ["urn:babel:orders:created"] = handler };

    private static RedisConsumer Consumer(ScriptDb db, Dictionary<string, BabelHandler>? handlers = null, RedisConsumerOptions? options = null)
        => new(db.Object, Queue, handlers ?? new Dictionary<string, BabelHandler>(), options, Clock);

    [Fact]
    public async Task RoutesValidMessageThenAcksBehindTheTokenGate()
    {
        var body = Envelope();
        var db = Reserving(body);
        Envelope? seen = null;
        string? rawSeen = null;

        var processed = await Consumer(db, Handlers((env, raw, _, _) => { seen = env; rawSeen = raw; return Task.CompletedTask; })).PollAsync();

        Assert.Equal(1, processed);
        Assert.Equal("urn:babel:orders:created", seen!.Job);
        Assert.Equal(body, rawSeen);

        var reserve = db.Of(RedisScripts.LegacyReserve)[0];
        var ack = Assert.Single(db.Of(RedisScripts.LegacyAck));
        Assert.Equal(new[] { Processing, Deadlines }, ack.Keys);
        Assert.Equal(body, (string)ack.Values[1]!);
        // The token that scored the deadline member is the token the ack presents.
        Assert.Equal((string)reserve.Values[1]!, (string)ack.Values[0]!);
    }

    [Fact]
    public async Task SweepRunsBeforeEveryReserve()
    {
        var db = Reserving(Envelope(), Envelope());
        await Consumer(db, Handlers((_, _, _, _) => Task.CompletedTask)).PollAsync();

        var order = db.Calls.Select(c => c.Script).Where(s => s is RedisScripts.LegacySweep or RedisScripts.LegacyReserve).ToList();
        Assert.Equal(
            new[]
            {
                RedisScripts.LegacySweep, RedisScripts.LegacyReserve,
                RedisScripts.LegacySweep, RedisScripts.LegacyReserve,
                RedisScripts.LegacySweep, RedisScripts.LegacyReserve,
            },
            order);
    }

    [Fact]
    public async Task ReserveKeysAndDeadlineScoreFollowTheClock()
    {
        var db = Reserving(Envelope());
        await Consumer(db, Handlers((_, _, _, _) => Task.CompletedTask)).PollAsync();

        var reserve = db.Of(RedisScripts.LegacyReserve)[0];
        Assert.Equal(new[] { Queue, Processing, Deadlines }, reserve.Keys);
        // floor(1000.7) + 90 s default retry_after
        Assert.Equal("1090", (string)reserve.Values[0]!);
        var sweep = db.Of(RedisScripts.LegacySweep)[0];
        Assert.Equal(new[] { Queue, Processing, Deadlines }, sweep.Keys);
        Assert.Equal("1000", (string)sweep.Values[0]!);
    }

    [Fact]
    public async Task NullRetryAfterScoresPositiveInfinity()
    {
        var db = Reserving(Envelope());
        var options = new RedisConsumerOptions { RetryAfter = null };
        await Consumer(db, Handlers((_, _, _, _) => Task.CompletedTask), options).PollAsync();

        Assert.Equal("+inf", (string)db.Of(RedisScripts.LegacyReserve)[0].Values[0]!);
    }

    [Fact]
    public async Task TokenIs32LowercaseHexAndFreshPerReservation()
    {
        var db = Reserving(Envelope(), Envelope(), Envelope());
        await Consumer(db, Handlers((_, _, _, _) => Task.CompletedTask)).PollAsync();

        var tokens = db.Of(RedisScripts.LegacyReserve).Select(c => (string)c.Values[1]!).ToList();
        Assert.Equal(4, tokens.Count);
        Assert.All(tokens, t => Assert.Matches("^[0-9a-f]{32}$", t));
        Assert.Equal(tokens.Count, tokens.Distinct().Count());
    }

    [Fact]
    public void NegativeRetryAfterIsAConfigurationError()
    {
        var options = new RedisConsumerOptions { RetryAfter = TimeSpan.FromSeconds(-1) };
        Assert.Throws<ArgumentOutOfRangeException>(() => Consumer(Reserving(), null, options));
    }

    [Fact]
    public async Task PreservesAttemptsFromBody()
    {
        var db = Reserving(Envelope(attempts: 4));
        var attempts = -1;
        await Consumer(db, Handlers((env, _, _, _) => { attempts = env.Attempts; return Task.CompletedTask; })).PollAsync();

        // Legacy never increments attempts.
        Assert.Equal(4, attempts);
    }

    [Fact]
    public async Task ThrowingHandlerLeavesReservedAndReportsOnError()
    {
        var db = Reserving(Envelope());
        Exception? captured = null;
        var options = new RedisConsumerOptions { OnError = (e, _, _) => captured = e };

        await Consumer(db, Handlers((_, _, _, _) => throw new InvalidOperationException("boom")), options).PollAsync();

        Assert.IsType<InvalidOperationException>(captured);
        Assert.Empty(db.Of(RedisScripts.LegacyAck));
        Assert.Empty(db.Of(RedisScripts.LegacyRelease));
    }

    [Fact]
    public async Task ReleaseOnFailureReturnsTheMessageToReady()
    {
        var db = Reserving(Envelope());
        var options = new RedisConsumerOptions { OnHandlerFailure = RedisFailureAction.Release };

        await Consumer(db, Handlers((_, _, _, _) => throw new InvalidOperationException("boom")), options).PollAsync();

        var release = Assert.Single(db.Of(RedisScripts.LegacyRelease));
        Assert.Equal(new[] { Processing, Deadlines, Queue }, release.Keys);
        Assert.Empty(db.Of(RedisScripts.LegacyAck));
    }

    [Fact]
    public async Task NonConformantEnvelopeIsReportedAndDeadLetteredAsAPoisonRecordWithoutAnAck()
    {
        const string raw = "{\"not\":\"an envelope\"}";
        var db = Reserving(raw);
        Exception? captured = null;
        var warnings = new List<string>();
        var options = new RedisConsumerOptions { OnError = (e, _, _) => captured = e, OnWarning = warnings.Add };

        await Consumer(db, null, options).PollAsync();

        Assert.IsType<BabelQueueException>(captured);
        var dlq = Assert.Single(db.Of(RedisScripts.LegacyDeadLetter));
        Assert.Equal(new[] { Processing, Deadlines, "orders.dlq" }, dlq.Keys);
        Assert.Equal(raw, (string)dlq.Values[1]!);
        AssertSpliced(raw, (string)dlq.Values[2]!);
        AssertPoisonBlock((string)dlq.Values[2]!);
        Assert.Matches("^[0-9a-f]{32}$", (string)dlq.Values[0]!);
        Assert.Empty(db.Of(RedisScripts.LegacyAck));
        Assert.Equal(new[] { "non-conformant envelope moved to the dead-letter list" }, warnings);
    }

    [Fact]
    public async Task DecodableEnvelopeWithInvalidUtf8IsPoisonNeverDeliveredAndDeadLetteredInTheBase64Wrapper()
    {
        // A conformant envelope whose data carries a lone 0xFF byte: the codec could decode it, yet it is poison (K-G).
        var stored = BadUtf8Envelope();
        var db = new ScriptDb()
            .Queue(RedisScripts.LegacyReserve, ScriptDb.Bytes(stored))
            .On(RedisScripts.LegacyAck, _ => ScriptDb.Integer(1))
            .On(RedisScripts.LegacyRelease, _ => ScriptDb.Integer(1))
            .On(RedisScripts.LegacyDeadLetter, _ => ScriptDb.Integer(1))
            .On(RedisScripts.LegacySweep, _ => ScriptDb.Integer(0));
        var called = false;
        var handlers = new Dictionary<string, BabelHandler> { ["urn:babel:orders:created"] = (_, _, _, _) => { called = true; return Task.CompletedTask; } };

        await Consumer(db, handlers).PollAsync();

        Assert.False(called);
        var dlq = Assert.Single(db.Of(RedisScripts.LegacyDeadLetter));
        Assert.Equal(stored, (byte[])dlq.Values[1]!);
        using (var doc = JsonDocument.Parse((string)dlq.Values[2]!))
        {
            Assert.Equal("base64", doc.RootElement.GetProperty("raw_encoding").GetString());
            Assert.Equal(stored, Convert.FromBase64String(doc.RootElement.GetProperty("raw").GetString()!));
        }
        AssertPoisonBlock((string)dlq.Values[2]!);
        Assert.Empty(db.Of(RedisScripts.LegacyAck));
        Assert.Empty(db.Of(RedisScripts.LegacyRelease));
        Assert.Empty(db.Pushed);
    }

    /// <summary>A valid, decodable envelope body with one invalid UTF-8 byte (0xFF) inside a data string.</summary>
    internal static byte[] BadUtf8Envelope()
    {
        var env = EnvelopeCodec.Make("urn:babel:orders:created", new Dictionary<string, object?> { ["note"] = "XX" }, Queue);
        var text = EnvelopeCodec.Encode(env);
        var bytes = System.Text.Encoding.UTF8.GetBytes(text);
        var at = text.IndexOf("XX", StringComparison.Ordinal);
        bytes[at] = 0xFF;
        return bytes;
    }

    [Fact]
    public async Task NonObjectPoisonIsDeadLetteredInTheRawWrapper()
    {
        const string raw = "not json at all";
        var db = Reserving(raw);

        await Consumer(db, null, null).PollAsync();

        var dlq = Assert.Single(db.Of(RedisScripts.LegacyDeadLetter));
        Assert.Equal(raw, (string)dlq.Values[1]!);
        using var doc = JsonDocument.Parse((string)dlq.Values[2]!);
        Assert.Equal(raw, doc.RootElement.GetProperty("raw").GetString());
        Assert.False(doc.RootElement.TryGetProperty("raw_encoding", out _));
        AssertPoisonBlock((string)dlq.Values[2]!);
    }

    [Fact]
    public async Task FramedNonConformantValueIsDeadLetteredUnwrappedInTheAdrShape()
    {
        const string inner = "{\"not\":\"an envelope\"}";
        var framed = RedisFrame.FrameValue(inner, new Dictionary<string, string> { ["traceparent"] = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01" });
        var db = Reserving(framed);

        await Consumer(db, null, null).PollAsync();

        var dlq = Assert.Single(db.Of(RedisScripts.LegacyDeadLetter));
        // The reservation handle is the stored element; the DLQ element is the ADR-0035 record of the body itself.
        Assert.Equal(framed, (string)dlq.Values[1]!);
        var record = (string)dlq.Values[2]!;
        AssertSpliced(inner, record);
        Assert.DoesNotContain("__bq_frame", record, StringComparison.Ordinal);
        AssertPoisonBlock(record);
    }

    [Fact]
    public async Task UnsupportedSchemaVersionIsDeadLetteredAsAPoisonRecordKeepingEveryByte()
    {
        var raw = Envelope().Replace("\"schema_version\":1", "\"schema_version\":99", StringComparison.Ordinal);
        Assert.Contains("\"schema_version\":99", raw, StringComparison.Ordinal);
        var db = Reserving(raw);

        await Consumer(db, Handlers((_, _, _, _) => Task.CompletedTask)).PollAsync();

        var dlq = Assert.Single(db.Of(RedisScripts.LegacyDeadLetter));
        // Version-gated: not parsed, so every original byte survives inside the record.
        AssertSpliced(raw, (string)dlq.Values[2]!);
        AssertPoisonBlock((string)dlq.Values[2]!);
    }

    [Fact]
    public async Task UnknownUrnWithHookAcksAndNeverDeadLetters()
    {
        var db = Reserving(Envelope());
        string? unknown = null;
        var options = new RedisConsumerOptions { OnUnknownUrn = (env, _, _) => { unknown = EnvelopeCodec.Urn(env); return Task.CompletedTask; } };

        await Consumer(db, null, options).PollAsync();

        Assert.Equal("urn:babel:orders:created", unknown);
        Assert.Single(db.Of(RedisScripts.LegacyAck));
        Assert.Empty(db.Of(RedisScripts.LegacyDeadLetter));
    }

    [Fact]
    public async Task UnknownUrnWithoutHookIsDeadLetteredWithAnUnknownUrnBlockAndNotAcked()
    {
        var original = Envelope(attempts: 2);
        var db = Reserving(original);
        Exception? captured = null;
        var warnings = new List<string>();
        var options = new RedisConsumerOptions { OnError = (e, _, _) => captured = e, OnWarning = warnings.Add };

        await Consumer(db, null, options).PollAsync();

        Assert.IsType<UnknownUrnException>(captured);
        Assert.Empty(db.Of(RedisScripts.LegacyAck));
        var dlq = Assert.Single(db.Of(RedisScripts.LegacyDeadLetter));
        Assert.Equal(new[] { Processing, Deadlines, "orders.dlq" }, dlq.Keys);
        Assert.Equal(original, (string)dlq.Values[1]!);

        var record = EnvelopeCodec.Decode((string)dlq.Values[2]!);
        var dead = record.DeadLetter!;
        Assert.Equal("unknown_urn", dead.Reason);
        Assert.Equal(Queue, dead.OriginalQueue);
        Assert.Equal(2, dead.Attempts);
        Assert.Equal("dotnet", dead.Lang);
        Assert.Equal(typeof(UnknownUrnException).FullName, dead.Exception);
        var source = EnvelopeCodec.Decode(original);
        Assert.Equal(source.Meta!.Id, record.Meta!.Id);
        Assert.Equal(source.TraceId, record.TraceId);
        Assert.Equal(new[] { "unknown URN moved to the dead-letter list" }, warnings);
    }

    [Fact]
    public async Task UnknownUrnDeadLetterRecordKeepsTheHeaderFrame()
    {
        var headers = new Dictionary<string, string> { ["traceparent"] = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01" };
        var framed = RedisFrame.FrameValue(Envelope(), headers);
        var db = Reserving(framed);

        await Consumer(db).PollAsync();

        var dlq = Assert.Single(db.Of(RedisScripts.LegacyDeadLetter));
        Assert.Equal(framed, (string)dlq.Values[1]!);
        var (body, carried) = RedisFrame.Unframe((string)dlq.Values[2]!);
        Assert.Equal(headers, carried);
        Assert.Equal("unknown_urn", EnvelopeCodec.Decode(body).DeadLetter!.Reason);
    }

    [Fact]
    public async Task UnknownUrnWhoseReservationExpiredWarnsInsteadOfWriting()
    {
        var db = Reserving(Envelope()).On(RedisScripts.LegacyDeadLetter, _ => ScriptDb.Integer(0));
        var warnings = new List<string>();

        await Consumer(db, null, new RedisConsumerOptions { OnWarning = warnings.Add }).PollAsync();

        Assert.Equal(new[] { RedisQueue.ExpiredWarning }, warnings);
    }

    [Fact]
    public async Task UnknownUrnUsesTheConfiguredDeadLetterKey()
    {
        var db = Reserving(Envelope());

        await Consumer(db, null, new RedisConsumerOptions { DeadLetterKey = "custom.dlq" }).PollAsync();

        Assert.Equal("custom.dlq", Assert.Single(db.Of(RedisScripts.LegacyDeadLetter)).Keys[2]);
    }

    [Fact]
    public async Task LateAckTouchesNothingAndWarns()
    {
        var db = Reserving(Envelope()).On(RedisScripts.LegacyAck, _ => ScriptDb.Integer(0));
        var warnings = new List<string>();
        var options = new RedisConsumerOptions { OnWarning = warnings.Add };

        var processed = await Consumer(db, Handlers((_, _, _, _) => Task.CompletedTask), options).PollAsync();

        Assert.Equal(1, processed);
        Assert.Equal(new[] { "reservation expired" }, warnings);
    }

    [Fact]
    public async Task PollReservesUpToMaxMessages()
    {
        var db = Reserving(Envelope(), Envelope(), Envelope());
        var count = 0;
        var options = new RedisConsumerOptions { MaxMessages = 2 };

        var processed = await Consumer(db, Handlers((_, _, _, _) => { count++; return Task.CompletedTask; }), options).PollAsync();

        Assert.Equal(2, processed);
        Assert.Equal(2, count);
    }

    [Fact]
    public async Task EmptyPollReturnsZero()
    {
        var processed = await Consumer(Reserving()).PollAsync();
        Assert.Equal(0, processed);
    }

    [Fact]
    public async Task CustomProcessingSuffixIsUsed()
    {
        var db = Reserving(Envelope());
        var options = new RedisConsumerOptions { ProcessingSuffix = ":inflight" };
        var consumer = Consumer(db, Handlers((_, _, _, _) => Task.CompletedTask), options);

        Assert.Equal("orders:inflight", consumer.ProcessingKey);
        await consumer.PollAsync();
        Assert.Equal(new[] { Queue, "orders:inflight", "orders:inflight:deadlines" }, db.Of(RedisScripts.LegacyReserve)[0].Keys);
    }

    [Fact]
    public void KeyAccessorsReflectTheLegacyLayout()
    {
        var consumer = Consumer(Reserving());
        Assert.Equal(Queue, consumer.ReadyKey);
        Assert.Equal(Processing, consumer.ProcessingKey);
        Assert.Equal("orders.dlq", consumer.DeadLetterKey);
        Assert.Equal(string.Empty, consumer.ReservedKey);
    }

    [Fact]
    public async Task RunStopsWhenCancelled()
    {
        var db = Reserving();
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await Consumer(db).RunAsync(cts.Token);

        Assert.Empty(db.Calls);
    }

    [Fact]
    public async Task RunProcessesThenStopsOnCancellationAfterIdle()
    {
        using var cts = new CancellationTokenSource();
        var db = Reserving(Envelope());
        var handled = 0;
        var options = new RedisConsumerOptions { IdleDelay = TimeSpan.FromMilliseconds(10) };

        var run = Consumer(db, Handlers((_, _, _, _) => { handled++; return Task.CompletedTask; }), options).RunAsync(cts.Token);
        await cts.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run);

        Assert.Equal(1, handled);
    }

    [Fact]
    public void IdleDelayIsClampedToTheOneSecondCapOfAdr0033()
    {
        Assert.Equal(TimeSpan.FromSeconds(1), new RedisConsumerOptions().IdleDelay);
        Assert.Equal(TimeSpan.FromSeconds(1), new RedisConsumerOptions { IdleDelay = TimeSpan.FromSeconds(30) }.IdleDelay);
        Assert.Equal(TimeSpan.FromMilliseconds(250), new RedisConsumerOptions { IdleDelay = TimeSpan.FromMilliseconds(250) }.IdleDelay);
        Assert.Equal(TimeSpan.Zero, new RedisConsumerOptions { IdleDelay = TimeSpan.FromSeconds(-5) }.IdleDelay);
        Assert.Equal(TimeSpan.FromSeconds(1), RedisConsumerOptions.MaxIdleDelay);
    }

    [Fact]
    public async Task IdleBackoffDoublesFromOneHundredMillisecondsAndResetsOnMessage()
    {
        // Empty, empty, then a message, then empty again: just proves the loop keeps polling and exits on cancel.
        using var cts = new CancellationTokenSource();
        var polls = 0;
        var db = new ScriptDb()
            .On(RedisScripts.LegacySweep, _ => ScriptDb.Integer(0))
            .On(RedisScripts.LegacyReserve, _ => { if (++polls >= 3) { cts.Cancel(); } return ScriptDb.Nil(); });
        var options = new RedisConsumerOptions { IdleDelay = TimeSpan.FromMilliseconds(250) };

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Consumer(db, null, options).RunAsync(cts.Token));

        Assert.True(polls >= 3);
    }

    [Fact]
    public void TokenSourceUsesOnlyTheCsprng()
    {
        var source = File.ReadAllText(Path.Combine(RepoRoot(), "src", "BabelQueue.Redis", "RedisQueue.cs"));
        Assert.Contains("RandomNumberGenerator", source, StringComparison.Ordinal);
        Assert.DoesNotContain("System.Random", source, StringComparison.Ordinal);
        Assert.DoesNotContain("new Random", source, StringComparison.Ordinal);
        Assert.DoesNotContain("Random.Shared", source, StringComparison.Ordinal);
        Assert.Matches(new Regex("^[0-9a-f]{32}$"), RedisQueueAccess.NewToken());
    }

    internal static string RepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null && !File.Exists(Path.Combine(dir.FullName, "BabelQueue.Redis.sln")) && !Directory.Exists(Path.Combine(dir.FullName, "src")))
        {
            dir = dir.Parent;
        }

        return dir?.FullName ?? throw new InvalidOperationException("repository root not found");
    }
}

internal static class RedisQueueAccess
{
    internal static string NewToken() => RedisQueue.NewToken();
}
