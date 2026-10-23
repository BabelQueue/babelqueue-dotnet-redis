using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using BabelQueue;
using BabelQueue.Redis;
using Moq;
using StackExchange.Redis;

namespace BabelQueue.Redis.Tests;

/// <summary>One manifest case of a section this package carries.</summary>
internal sealed record SuiteCase(string Section, string Id, bool NeedsRedis, Func<Task> Run);

/// <summary>
/// The Redis-scoped sections of the vendored conformance manifest (<c>redis</c>, <c>redis_delay</c>,
/// <c>redis_layout</c>), each case runnable on its own. Pure cases need no Redis; the behavioural
/// <c>redis_layout</c> blocks run the real Lua against a live Redis (CI: <c>redis:7</c>).
/// </summary>
internal static class RedisConformanceSuite
{
    private static readonly string Dir = Path.Combine(AppContext.BaseDirectory, "conformance");
    private static readonly Regex Hex32 = new("^[0-9a-f]{32}$", RegexOptions.Compiled);
    private static readonly string[] BehaviouralBlocks =
    {
        "delayed_score", "migrate", "reserve", "pcall_poison", "release",
        "processing_sweep", "processing_token", "late_ack", "identical_copies", "retry_after_null",
        "lrem_count", "malformed_member", "orphan", "legacy_release_gate", "legacy_dlq_gate",
    };

    private static readonly Lazy<JsonElement> Manifest = new(() =>
    {
        using var doc = JsonDocument.Parse(File.ReadAllText(Path.Combine(Dir, "manifest.json")));
        return doc.RootElement.Clone();
    });

    internal static string ConformanceVersion => File.ReadAllText(Path.Combine(Dir, "CONFORMANCE_VERSION")).Trim();

    internal static JsonElement ManifestRoot => Manifest.Value;

    internal static IReadOnlyList<SuiteCase> All()
    {
        var cases = new List<SuiteCase>();
        var root = Manifest.Value;

        cases.Add(new SuiteCase("redis", "payload_identity", false, () => PayloadIdentityAsync(root.GetProperty("redis").GetProperty("payload_identity"))));

        foreach (var (c, index) in root.GetProperty("redis_delay").GetProperty("cases").EnumerateArray().Select((c, i) => (c, i)))
        {
            cases.Add(new SuiteCase("redis_delay", Name(c, "redis_delay#" + index), false, () => DelayScoreAsync(c)));
        }

        var layout = root.GetProperty("redis_layout");
        foreach (var (c, index) in layout.GetProperty("cases").EnumerateArray().Select((c, i) => (c, i)))
        {
            cases.Add(new SuiteCase("redis_layout", Name(c, "redis_layout#" + index), false, () => KeyNamesAsync(c)));
        }

        foreach (var block in BehaviouralBlocks)
        {
            foreach (var (c, index) in layout.GetProperty(block).GetProperty("cases").EnumerateArray().Select((c, i) => (c, i)))
            {
                cases.Add(new SuiteCase("redis_layout", block + "/" + Name(c, index.ToString(CultureInfo.InvariantCulture)), true, () => BehaviourAsync(c)));
            }
        }

        return cases;
    }

    internal static SuiteCase Find(string section, string id) =>
        All().Single(c => c.Section == section && c.Id == id);

    private static string Name(JsonElement c, string fallback) =>
        c.TryGetProperty("name", out var n) ? n.GetString()! : fallback;

    // ------------------------------------------------------------------ redis.payload_identity

    private static async Task PayloadIdentityAsync(JsonElement identity)
    {
        var fixtureBody = File.ReadAllText(Path.Combine(Dir, identity.GetProperty("envelope_file").GetString()!));
        var fixtureEnvelope = EnvelopeCodec.Decode(fixtureBody);

        var db = new ScriptDb();
        await new RedisPublisher(db.Object, "orders", new RedisPublisherOptions { Layout = RedisLayout.Legacy }).PublishAsync(
            EnvelopeCodec.Urn(fixtureEnvelope), fixtureEnvelope.Data, fixtureEnvelope.TraceId);

        var stored = (string)Check(db.Pushed.Single().Value)!;
        using var storedDoc = JsonDocument.Parse(stored);
        Require(storedDoc.RootElement.ValueKind == JsonValueKind.Object, "stored element is not a JSON object");
        Require(
            storedDoc.RootElement.EnumerateObject().Select(p => p.Name).SequenceEqual(new[] { "job", "trace_id", "data", "meta", "attempts" }),
            "stored element has unexpected top-level keys");
        Require(EnvelopeCodec.Encode(EnvelopeCodec.Decode(stored)) == stored, "stored element is not the canonical encoding");
        var storedEnvelope = EnvelopeCodec.Decode(stored);
        Require(fixtureEnvelope.Job == storedEnvelope.Job && fixtureEnvelope.TraceId == storedEnvelope.TraceId && fixtureEnvelope.Attempts == storedEnvelope.Attempts, "contract fields differ");
    }

    private static RedisValue? Check(RedisValue value) => value;

    // ------------------------------------------------------------------ redis_delay (pure)

    private static async Task DelayScoreAsync(JsonElement c)
    {
        var now = c.GetProperty("now").GetDouble();
        var delay = c.GetProperty("delay").GetDouble();
        var expected = c.GetProperty("expect_score").GetDouble();

        var db = new ScriptDb();
        var queue = new RedisQueue(db.Object, "orders", RedisLayout.Zset, RedisQueue.DefaultKeyPrefix, ":processing", TimeSpan.FromSeconds(90), -1, () => now);
        await queue.PushDelayedAsync("{}", TimeSpan.FromSeconds(delay));

        var added = db.SortedAdds.Single();
        Require(added.Key == "queues:orders:delayed", $"delayed key was {added.Key}");
        Require(added.Score == expected, $"score for now={now} delay={delay}: expected {expected}, got {added.Score}");
    }

    // ------------------------------------------------------------------ redis_layout.cases (pure)

    private static Task KeyNamesAsync(JsonElement c)
    {
        var expect = c.GetProperty("expect");
        var queue = new RedisQueue(new ScriptDb().Object, c.GetProperty("queue").GetString()!, RedisLayout.Zset, c.GetProperty("key_prefix").GetString()!, ":processing", TimeSpan.FromSeconds(90), -1, null);
        Require(queue.Ready == expect.GetProperty("ready").GetString(), $"ready key {queue.Ready}");
        Require(queue.Delayed == expect.GetProperty("delayed").GetString(), $"delayed key {queue.Delayed}");
        Require(queue.Reserved == expect.GetProperty("reserved").GetString(), $"reserved key {queue.Reserved}");
        Require(queue.Notify == expect.GetProperty("notify").GetString(), $"notify key {queue.Notify}");
        return Task.CompletedTask;
    }

    // ------------------------------------------------------------------ redis_layout behavioural blocks (live Redis)

    private static async Task BehaviourAsync(JsonElement c)
    {
        var db = RedisLive.Database();
        var layout = c.GetProperty("layout").GetString() == "zset" ? RedisLayout.Zset : RedisLayout.Legacy;
        var retryAfterElement = c.GetProperty("retry_after");
        TimeSpan? retryAfter = retryAfterElement.ValueKind == JsonValueKind.Null ? null : TimeSpan.FromSeconds(retryAfterElement.GetDouble());
        var batch = c.TryGetProperty("migration_batch_size", out var b) ? b.GetInt32() : -1;
        var queueName = c.GetProperty("queue").GetString()! + "-" + Guid.NewGuid().ToString("N")[..10];

        double now = 0;
        var queue = new RedisQueue(db, queueName, layout, RedisQueue.DefaultKeyPrefix, ":processing", retryAfter, batch, () => now);
        var keys = new[] { queue.Ready, queue.Delayed, queue.Reserved, queue.Notify, queue.Processing, queue.Deadlines, queue.DeadLetter, queue.DeadLetterNotify }
            .Where(k => k.Length > 0).Select(k => (RedisKey)k).ToArray();

        try
        {
            await db.KeyDeleteAsync(keys);
            await SeedAsync(db, queue, c.GetProperty("state"));

            var reservations = new Dictionary<string, RedisReservation>();
            var stepIndex = 0;
            foreach (var step in c.GetProperty("steps").EnumerateArray())
            {
                stepIndex++;
                now = step.GetProperty("now").GetDouble();
                RedisReservation? reserved = null;
                var outcome = string.Empty;
                switch (step.GetProperty("op").GetString())
                {
                    case "push_delayed":
                        await queue.PushDelayedAsync(step.GetProperty("body").GetString()!, TimeSpan.FromSeconds(step.GetProperty("delay").GetDouble()));
                        break;
                    case "migrate":
                        await queue.MigrateAsync();
                        break;
                    case "sweep":
                        await queue.SweepAsync();
                        break;
                    case "reserve":
                        reserved = await queue.ReserveAsync();
                        outcome = reserved is null ? "empty" : reserved.IsPoison ? "poison" : "delivered";
                        if (reserved is not null && step.TryGetProperty("as", out var name))
                        {
                            reservations[name.GetString()!] = reserved;
                        }

                        break;
                    case "release":
                        await queue.ReleaseAsync(Resolve(step.GetProperty("reservation"), reservations), TimeSpan.FromSeconds(step.GetProperty("delay").GetDouble()));
                        break;
                    case "ack":
                        await queue.AckAsync(Resolve(step.GetProperty("reservation"), reservations));
                        break;
                    case "dead_letter":
                        var target = Resolve(step.GetProperty("reservation"), reservations);
                        await queue.DeadLetterAsync(target, target.HandlerBody);
                        break;
                    default:
                        throw new InvalidOperationException("unknown op " + step.GetProperty("op").GetString());
                }

                if (step.TryGetProperty("expect", out var expect))
                {
                    try
                    {
                        await VerifyAsync(db, queue, expect, reserved, outcome);
                    }
                    catch (ConformanceFailure e)
                    {
                        throw new ConformanceFailure($"step {stepIndex} ({step.GetProperty("op").GetString()}): {e.Message}");
                    }
                }
            }
        }
        finally
        {
            await db.KeyDeleteAsync(keys);
        }
    }

    private static RedisReservation Resolve(JsonElement reference, Dictionary<string, RedisReservation> named)
    {
        if (reference.ValueKind == JsonValueKind.String)
        {
            return named[reference.GetString()!];
        }

        var raw = reference.GetProperty("raw").GetString()!;
        return new RedisReservation(raw, raw, reference.GetProperty("token").GetString(), isPoison: false, attempts: 1);
    }

    private static async Task SeedAsync(IDatabase db, RedisQueue queue, JsonElement state)
    {
        foreach (var property in state.EnumerateObject())
        {
            switch (property.Name)
            {
                case "ready":
                    foreach (var item in property.Value.EnumerateArray())
                    {
                        await db.ListRightPushAsync(queue.Ready, item.GetString());
                    }

                    break;
                case "processing":
                    foreach (var item in property.Value.EnumerateArray())
                    {
                        await db.ListRightPushAsync(queue.Processing, item.GetString());
                    }

                    break;
                case "notify":
                    for (var i = 0; i < property.Value.GetInt32(); i++)
                    {
                        await db.ListRightPushAsync(queue.Notify, "1");
                    }

                    break;
                case "delayed":
                    await SeedSortedAsync(db, queue.Delayed, property.Value, null);
                    break;
                case "reserved":
                    await SeedSortedAsync(db, queue.Reserved, property.Value, null);
                    break;
                case "deadlines":
                    await SeedSortedAsync(db, queue.Deadlines, property.Value, null);
                    break;
                case "delayed_generate":
                    var count = property.Value.GetProperty("count").GetInt32();
                    var pattern = property.Value.GetProperty("member").GetString()!;
                    var score = property.Value.GetProperty("score").GetDouble();
                    var entries = Enumerable.Range(0, count)
                        .Select(i => new SortedSetEntry(pattern.Replace("%03d", i.ToString("D3", CultureInfo.InvariantCulture)), score))
                        .ToArray();
                    await db.SortedSetAddAsync(queue.Delayed, entries);
                    break;
                default:
                    throw new InvalidOperationException("unknown state key " + property.Name);
            }
        }
    }

    private static async Task SeedSortedAsync(IDatabase db, string key, JsonElement items, object? unused)
    {
        _ = unused;
        foreach (var item in items.EnumerateArray())
        {
            await db.SortedSetAddAsync(key, MemberOf(item), Score(item.GetProperty("score")));
        }
    }

    private static string MemberOf(JsonElement item)
    {
        if (item.TryGetProperty("member", out var member))
        {
            return member.GetString()!;
        }

        if (item.TryGetProperty("token", out var token))
        {
            return token.GetString() + ":" + item.GetProperty("raw").GetString();
        }

        throw new InvalidOperationException("sorted-set item has no member / token+raw");
    }

    private static double Score(JsonElement score) =>
        score.ValueKind == JsonValueKind.String && score.GetString() == "+inf" ? double.PositiveInfinity : score.GetDouble();

    private static async Task VerifyAsync(IDatabase db, RedisQueue queue, JsonElement expect, RedisReservation? reserved, string outcome)
    {
        foreach (var property in expect.EnumerateObject())
        {
            switch (property.Name)
            {
                case "outcome":
                    Require(outcome == property.Value.GetString(), $"outcome was '{outcome}', expected '{property.Value.GetString()}'");
                    break;
                case "handler_body":
                    Require(reserved is not null && (string?)reserved.HandlerBody == property.Value.GetString(), $"handler body was '{(string?)reserved?.HandlerBody}'");
                    break;
                case "reported_attempts":
                    Require(reserved is not null && reserved.Attempts == property.Value.GetInt32(), $"reported attempts was {reserved?.Attempts}");
                    break;
                case "ready":
                    SameList(await ListAsync(db, queue.Ready), property.Value, "ready");
                    break;
                case "processing":
                    SameList(await ListAsync(db, queue.Processing), property.Value, "processing");
                    break;
                case "ready_count":
                    Require(await db.ListLengthAsync(queue.Ready) == property.Value.GetInt64(), $"ready length {await db.ListLengthAsync(queue.Ready)}");
                    break;
                case "dlq_count":
                    Require(await db.ListLengthAsync(queue.DeadLetter) == property.Value.GetInt64(), $"dead-letter length {await db.ListLengthAsync(queue.DeadLetter)}");
                    break;
                case "notify":
                    Require(await db.ListLengthAsync(queue.Notify) == property.Value.GetInt64(), $"notify length {await db.ListLengthAsync(queue.Notify)}");
                    break;
                case "ready_generate":
                    var count = property.Value.GetProperty("count").GetInt32();
                    var pattern = property.Value.GetProperty("member").GetString()!;
                    var want = Enumerable.Range(0, count).Select(i => pattern.Replace("%03d", i.ToString("D3", CultureInfo.InvariantCulture))).ToList();
                    Require(want.SequenceEqual(await ListAsync(db, queue.Ready)), "ready list is not the generated members in order");
                    break;
                case "delayed":
                    await SameSortedAsync(db, queue.Delayed, property.Value, "delayed");
                    break;
                case "reserved":
                    await SameSortedAsync(db, queue.Reserved, property.Value, "reserved");
                    break;
                case "deadlines":
                    await SameSortedAsync(db, queue.Deadlines, property.Value, "deadlines");
                    break;
                case "tokens_distinct":
                    var tokens = (await db.SortedSetRangeByRankAsync(queue.Deadlines)).Select(m => ((string)m!)[..32]).ToList();
                    Require((tokens.Distinct().Count() == tokens.Count) == property.Value.GetBoolean(), "deadline tokens distinctness differs");
                    break;
                default:
                    throw new InvalidOperationException("unknown expect key " + property.Name);
            }
        }
    }

    private static async Task<List<string>> ListAsync(IDatabase db, string key) =>
        (await db.ListRangeAsync(key, 0, -1)).Select(v => (string)v!).ToList();

    private static void SameList(List<string> actual, JsonElement expected, string what)
    {
        var want = expected.EnumerateArray().Select(e => e.GetString()!).ToList();
        Require(want.SequenceEqual(actual), $"{what}: expected [{string.Join(", ", want)}] got [{string.Join(", ", actual)}]");
    }

    private static async Task SameSortedAsync(IDatabase db, string key, JsonElement expected, string what)
    {
        var actual = (await db.SortedSetRangeByRankWithScoresAsync(key)).Select(e => ((string)e.Element!, e.Score)).ToList();
        var wanted = expected.EnumerateArray().ToList();
        Require(wanted.Count == actual.Count, $"{what}: expected {wanted.Count} member(s), found {actual.Count} [{string.Join(" | ", actual.Select(a => a.Item1 + "@" + a.Score.ToString(CultureInfo.InvariantCulture)))}]");
        foreach (var item in wanted)
        {
            var index = actual.FindIndex(a => Matches(item, a.Item1, a.Score));
            Require(index >= 0, $"{what}: no member matches {item.GetRawText()}; found [{string.Join(" | ", actual.Select(a => a.Item1 + "@" + a.Score.ToString(CultureInfo.InvariantCulture)))}]");
            actual.RemoveAt(index);
        }
    }

    private static bool Matches(JsonElement item, string member, double score)
    {
        if (Score(item.GetProperty("score")) != score)
        {
            return false;
        }

        if (item.TryGetProperty("member", out var literal))
        {
            return literal.GetString() == member;
        }

        if (item.TryGetProperty("member_json", out var memberJson) || item.TryGetProperty("body_json", out memberJson))
        {
            try
            {
                using var actual = JsonDocument.Parse(member);
                return JsonEqual(memberJson, actual.RootElement);
            }
            catch (JsonException)
            {
                return false;
            }
        }

        var token = item.GetProperty("token").GetString()!;
        var raw = item.GetProperty("raw").GetString()!;
        if (member.Length < 34 || member[32] != ':' || member[33..] != raw)
        {
            return false;
        }

        return token == "<any>" ? Hex32.IsMatch(member[..32]) : member[..32] == token;
    }

    private static bool JsonEqual(JsonElement a, JsonElement b)
    {
        if (a.ValueKind != b.ValueKind)
        {
            return false;
        }

        switch (a.ValueKind)
        {
            case JsonValueKind.Object:
                var left = a.EnumerateObject().ToList();
                var right = b.EnumerateObject().ToDictionary(p => p.Name, p => p.Value);
                return left.Count == right.Count && left.All(p => right.TryGetValue(p.Name, out var other) && JsonEqual(p.Value, other));
            case JsonValueKind.Array:
                var xs = a.EnumerateArray().ToList();
                var ys = b.EnumerateArray().ToList();
                return xs.Count == ys.Count && xs.Zip(ys).All(t => JsonEqual(t.First, t.Second));
            case JsonValueKind.Number:
                return a.GetDecimal() == b.GetDecimal();
            case JsonValueKind.String:
                return a.GetString() == b.GetString();
            default:
                return true;
        }
    }

    private static void Require(bool condition, string message)
    {
        if (!condition)
        {
            throw new ConformanceFailure(message);
        }
    }

    // ------------------------------------------------------------------ results.json (ADR-0032 section 2)

    internal static async Task<string> WriteResultsAsync(string? path)
    {
        var sections = new SortedDictionary<string, SortedDictionary<string, (string Status, string? Reason)>>(StringComparer.Ordinal);
        foreach (var c in All())
        {
            (string, string?) outcome;
            try
            {
                if (c.NeedsRedis && !RedisLive.Available)
                {
                    throw new ConformanceFailure($"live Redis not reachable at {RedisLive.Url} ({RedisLive.Unavailable})");
                }

                await c.Run();
                outcome = ("pass", null);
            }
            catch (Exception e)
            {
                outcome = ("fail", e.Message);
            }

            if (!sections.TryGetValue(c.Section, out var bucket))
            {
                bucket = sections[c.Section] = new SortedDictionary<string, (string, string?)>(StringComparer.Ordinal);
            }

            bucket[c.Id] = outcome;
        }

        using var stream = new MemoryStream();
        using (var w = new Utf8JsonWriter(stream, new JsonWriterOptions { Indented = true }))
        {
            w.WriteStartObject();
            w.WriteString("sdk", "babelqueue-dotnet-redis");
            w.WriteString("version", PackageVersion());
            w.WriteString("conformance_version", ConformanceVersion);
            w.WriteString("generated_at", DateTimeOffset.UtcNow.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture));
            w.WriteStartArray("bindings");
            w.WriteStringValue("redis");
            w.WriteEndArray();
            w.WriteStartObject("sections");
            foreach (var (section, cases) in sections)
            {
                w.WriteStartObject(section);
                foreach (var (id, (status, reason)) in cases)
                {
                    w.WriteStartObject(id);
                    w.WriteString("status", status);
                    if (reason is not null)
                    {
                        w.WriteString("reason", reason);
                    }

                    w.WriteEndObject();
                }

                w.WriteEndObject();
            }

            w.WriteEndObject();
            w.WriteEndObject();
        }

        var json = Encoding.UTF8.GetString(stream.ToArray()) + "\n";
        if (!string.IsNullOrWhiteSpace(path))
        {
            await File.WriteAllTextAsync(path, json);
        }

        return json;
    }

    private static string PackageVersion()
    {
        var info = typeof(RedisPublisher).Assembly
            .GetCustomAttributes(typeof(System.Reflection.AssemblyInformationalVersionAttribute), false)
            .Cast<System.Reflection.AssemblyInformationalVersionAttribute>()
            .Single().InformationalVersion;
        var plus = info.IndexOf('+', StringComparison.Ordinal);
        return plus < 0 ? info : info[..plus];
    }
}

internal sealed class ConformanceFailure : Exception
{
    public ConformanceFailure(string message) : base(message)
    {
    }
}
