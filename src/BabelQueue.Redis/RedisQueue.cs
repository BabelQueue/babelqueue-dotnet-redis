using System.Globalization;
using System.Security.Cryptography;
using StackExchange.Redis;

namespace BabelQueue.Redis;

/// <summary>
/// The one engine behind <see cref="RedisConsumer"/> and <see cref="RedisPublisher"/>: key naming,
/// the Lua scripts and the reserve / ack / release / dead-letter / migrate / sweep operations for
/// both layouts (broker-bindings §1.1, §1.6, §1.7; ADR-0033). The clock is injectable so the
/// conformance runner can pin time.
/// </summary>
internal sealed class RedisQueue
{
    internal const string DefaultKeyPrefix = "queues:";
    internal const string ExpiredWarning = "reservation expired";
    private const int TokenBytes = 16;

    private readonly IDatabase _database;
    private readonly Func<double> _clock;
    private readonly long? _retryAfterSeconds;
    private readonly int _batch;

    internal RedisQueue(
        IDatabase database,
        string queue,
        RedisLayout layout,
        string keyPrefix,
        string processingSuffix,
        TimeSpan? retryAfter,
        int migrationBatchSize,
        Func<double>? clock,
        string? deadLetterKey = null)
    {
        ArgumentNullException.ThrowIfNull(database);
        ArgumentException.ThrowIfNullOrEmpty(queue);
        ArgumentNullException.ThrowIfNull(keyPrefix);
        ArgumentException.ThrowIfNullOrEmpty(processingSuffix);
        if (retryAfter is { } span && span < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(retryAfter), retryAfter, "retry_after must not be negative (use null to disable recovery).");
        }

        _database = database;
        _clock = clock ?? SystemClock;
        _retryAfterSeconds = retryAfter is { } value ? (long)Math.Ceiling(value.TotalSeconds) : null;
        _batch = migrationBatchSize;
        Layout = layout;

        if (layout == RedisLayout.Zset)
        {
            Ready = keyPrefix + queue;
            Delayed = Ready + ":delayed";
            Reserved = Ready + ":reserved";
            Notify = Ready + ":notify";
            DeadLetter = deadLetterKey ?? (keyPrefix + queue + ".dlq");
            DeadLetterNotify = DeadLetter + ":notify";
            Processing = string.Empty;
            Deadlines = string.Empty;
        }
        else
        {
            Ready = queue;
            Processing = queue + processingSuffix;
            Deadlines = Processing + ":deadlines";
            DeadLetter = deadLetterKey ?? (queue + ".dlq");
            DeadLetterNotify = string.Empty;
            Delayed = string.Empty;
            Reserved = string.Empty;
            Notify = string.Empty;
        }
    }

    internal RedisLayout Layout { get; }

    internal string Ready { get; }

    internal string Delayed { get; }

    internal string Reserved { get; }

    internal string Notify { get; }

    internal string Processing { get; }

    internal string Deadlines { get; }

    internal string DeadLetter { get; }

    internal string DeadLetterNotify { get; }

    private static double SystemClock() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() / 1000.0;

    private long FloorNow() => (long)Math.Floor(_clock());

    private RedisValue Deadline(long floorNow)
        => _retryAfterSeconds is { } seconds
            ? (RedisValue)(floorNow + seconds).ToString(CultureInfo.InvariantCulture)
            : (RedisValue)"+inf";

    /// <summary>Appends <paramref name="body"/> to the ready list (zset layout also leaves a notify token).</summary>
    internal async Task PushAsync(RedisValue body)
    {
        if (Layout == RedisLayout.Zset)
        {
            await EvalAsync(RedisScripts.ZsetPush, new RedisKey[] { Ready, Notify }, new[] { body })
                .ConfigureAwait(false);
        }
        else
        {
            await _database.ListRightPushAsync(Ready, body).ConfigureAwait(false);
        }
    }

    /// <summary>Zset layout: schedules <paramref name="body"/> on <c>:delayed</c> at <c>floor(now) + ceil(delay)</c> — no notify token (§1.6).</summary>
    internal async Task PushDelayedAsync(RedisValue body, TimeSpan delay)
    {
        if (Layout != RedisLayout.Zset)
        {
            throw new NotSupportedException("Delayed publish needs the zset (Laravel) layout.");
        }

        ArgumentOutOfRangeException.ThrowIfLessThan(delay, TimeSpan.Zero);
        var score = FloorNow() + (long)Math.Ceiling(delay.TotalSeconds);
        await _database.SortedSetAddAsync(Delayed, body, score).ConfigureAwait(false);
    }

    /// <summary>
    /// Moves everything due from <c>:delayed</c> (and, when recovery is enabled, <c>:reserved</c>) to the
    /// ready list, one notify token per item. Zset layout only; returns the number moved.
    /// </summary>
    internal async Task<long> MigrateAsync()
    {
        if (Layout != RedisLayout.Zset)
        {
            return 0;
        }

        var floorNow = FloorNow().ToString(CultureInfo.InvariantCulture);
        var moved = await MigrateSetAsync(Delayed, floorNow).ConfigureAwait(false);
        if (_retryAfterSeconds is not null)
        {
            moved += await MigrateSetAsync(Reserved, floorNow).ConfigureAwait(false);
        }

        return moved;
    }

    private async Task<long> MigrateSetAsync(string source, string floorNow)
    {
        var result = await EvalAsync(
            RedisScripts.ZsetMigrate,
            new RedisKey[] { source, Ready, Notify },
            new RedisValue[] { floorNow, _batch }).ConfigureAwait(false);
        return (long)result;
    }

    /// <summary>
    /// Legacy layout: returns expired reservations (<c>:processing:deadlines</c> score &lt;= floor(now))
    /// to the ready list. Runs before every reserve; returns the number of members handled.
    /// </summary>
    internal async Task<long> SweepAsync()
    {
        if (Layout != RedisLayout.Legacy)
        {
            return 0;
        }

        var result = await EvalAsync(
            RedisScripts.LegacySweep,
            new RedisKey[] { Ready, Processing, Deadlines },
            new RedisValue[] { FloorNow().ToString(CultureInfo.InvariantCulture), _batch }).ConfigureAwait(false);
        return (long)result;
    }

    /// <summary>
    /// Reserves the head of the queue (after migrating / sweeping what is due). Returns null when
    /// nothing is ready.
    /// </summary>
    internal async Task<RedisReservation?> ReserveAsync()
    {
        return Layout == RedisLayout.Zset
            ? await ReserveZsetAsync().ConfigureAwait(false)
            : await ReserveLegacyAsync().ConfigureAwait(false);
    }

    private async Task<RedisReservation?> ReserveZsetAsync()
    {
        await MigrateAsync().ConfigureAwait(false);

        var result = await EvalAsync(
            RedisScripts.ZsetReserve,
            new RedisKey[] { Ready, Reserved, Notify },
            new[] { Deadline(FloorNow()) }).ConfigureAwait(false);
        if (result.IsNull)
        {
            return null;
        }

        var pair = (RedisResult[])result!;
        RedisValue job = (RedisValue)pair[0];
        RedisValue reserved = (RedisValue)pair[1];
        var poison = job == reserved;
        return new RedisReservation(job, reserved, token: null, poison, poison ? 1 : RedisReleaseBody.ReportedAttempts(job.ToString()));
    }

    private async Task<RedisReservation?> ReserveLegacyAsync()
    {
        await SweepAsync().ConfigureAwait(false);

        var token = NewToken();
        var result = await EvalAsync(
            RedisScripts.LegacyReserve,
            new RedisKey[] { Ready, Processing, Deadlines },
            new[] { Deadline(FloorNow()), (RedisValue)token }).ConfigureAwait(false);
        if (result.IsNull)
        {
            return null;
        }

        RedisValue raw = (RedisValue)result;
        return new RedisReservation(raw, raw, token, isPoison: false, attempts: 1);
    }

    /// <summary>Acks a reservation. Returns false (and touches nothing) when it had already expired.</summary>
    internal async Task<bool> AckAsync(RedisReservation reservation)
    {
        ArgumentNullException.ThrowIfNull(reservation);
        if (Layout == RedisLayout.Zset)
        {
            return await _database.SortedSetRemoveAsync(Reserved, reservation.Reserved).ConfigureAwait(false);
        }

        return await EvaluateGateAsync(
            RedisScripts.LegacyAck,
            new RedisKey[] { Processing, Deadlines },
            reservation).ConfigureAwait(false);
    }

    /// <summary>
    /// Releases a reservation for redelivery. Zset: one Lua that drops the <c>:reserved</c> member and parks the
    /// SDK-codec body (<c>attempts + 1</c>, everything else preserved) on <c>:delayed</c> at
    /// <c>floor(now) + ceil(delay)</c>. Legacy: the body goes back to the ready list (no delay, attempts untouched).
    /// Returns false when the reservation had already expired.
    /// </summary>
    internal async Task<bool> ReleaseAsync(RedisReservation reservation, TimeSpan delay)
    {
        ArgumentNullException.ThrowIfNull(reservation);
        if (Layout == RedisLayout.Zset)
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(delay, TimeSpan.Zero);
            var body = RedisReleaseBody.Build(reservation.HandlerBody.ToString());
            var score = (FloorNow() + (long)Math.Ceiling(delay.TotalSeconds)).ToString(CultureInfo.InvariantCulture);
            var done = await EvalAsync(
                RedisScripts.ZsetRelease,
                new RedisKey[] { Reserved, Delayed },
                new RedisValue[] { reservation.Reserved, score, body }).ConfigureAwait(false);
            return (long)done == 1;
        }

        return await EvaluateGateAsync(
            RedisScripts.LegacyRelease,
            new RedisKey[] { Processing, Deadlines, Ready },
            reservation).ConfigureAwait(false);
    }

    /// <summary>
    /// Moves a reserved element to the dead-letter list, removing the reservation in the same script.
    /// <paramref name="body"/> is what lands on the DLQ (poison: the raw element). Returns false when the
    /// reservation had already expired (nothing is written).
    /// </summary>
    internal async Task<bool> DeadLetterAsync(RedisReservation reservation, RedisValue body)
    {
        ArgumentNullException.ThrowIfNull(reservation);
        if (Layout == RedisLayout.Zset)
        {
            var done = await EvalAsync(
                RedisScripts.ZsetDeadLetter,
                new RedisKey[] { Reserved, DeadLetter, DeadLetterNotify },
                new[] { reservation.Reserved, body }).ConfigureAwait(false);
            return (long)done == 1;
        }

        var result = await EvalAsync(
            RedisScripts.LegacyDeadLetter,
            new RedisKey[] { Processing, Deadlines, DeadLetter },
            new[] { (RedisValue)reservation.Token, reservation.Reserved, body }).ConfigureAwait(false);
        return (long)result == 1;
    }

    private async Task<bool> EvaluateGateAsync(string script, RedisKey[] keys, RedisReservation reservation)
    {
        var result = await EvalAsync(
            script,
            keys,
            new[] { (RedisValue)reservation.Token, reservation.Reserved }).ConfigureAwait(false);
        return (long)result == 1;
    }

    /// <summary>
    /// Runs a script; a Redis Cluster <c>CROSSSLOT</c> refusal is re-raised with the cause spelled out
    /// (ADR-0033 §6: every key of a queue must share one hash tag — put it in the queue name, e.g. <c>{orders}</c>).
    /// </summary>
    private async Task<RedisResult> EvalAsync(string script, RedisKey[] keys, RedisValue[] values)
    {
        try
        {
            return await _database.ScriptEvaluateAsync(script, keys, values).ConfigureAwait(false);
        }
        catch (RedisServerException error) when (error.Message.StartsWith("CROSSSLOT", StringComparison.Ordinal))
        {
            throw new BabelQueueException(
                "Redis Cluster refused a multi-key script (CROSSSLOT): the keys of queue '" + Ready +
                "' do not share a hash slot. Give the queue name a hash tag (for example '{orders}') so all of its keys map to one slot.",
                error);
        }
    }

    /// <summary>A fresh 128-bit reservation token as 32 lowercase hex characters, from the OS CSPRNG.</summary>
    internal static string NewToken()
        => Convert.ToHexString(RandomNumberGenerator.GetBytes(TokenBytes)).ToLowerInvariant();
}
