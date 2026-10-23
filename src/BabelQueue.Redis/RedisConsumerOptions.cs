namespace BabelQueue.Redis;

/// <summary>Tuning and hooks for <see cref="RedisConsumer"/>.</summary>
public sealed class RedisConsumerOptions
{
    /// <summary>
    /// Which key layout to speak (broker-bindings §1.1). Default <see cref="RedisLayout.Legacy"/>
    /// (the .NET-owned reliable queue, key names unchanged); <see cref="RedisLayout.Zset"/> is the
    /// Laravel-compatible sorted-set layout for a queue shared with PHP.
    /// </summary>
    public RedisLayout Layout { get; set; } = RedisLayout.Legacy;

    /// <summary>
    /// Key prefix of the <see cref="RedisLayout.Zset"/> layout: the ready list is
    /// <c>&lt;KeyPrefix&gt;&lt;queue&gt;</c>. Default <c>queues:</c> (Laravel). Ignored by
    /// <see cref="RedisLayout.Legacy"/>. On Redis Cluster the queue name must carry a hash tag
    /// (<c>{orders}</c>) so every key lands in one slot; the SDK uses the name as given.
    /// </summary>
    public string KeyPrefix { get; set; } = "queues:";

    /// <summary>
    /// Suffix appended to the queue key to name the per-queue processing (reservation)
    /// list of the <see cref="RedisLayout.Legacy"/> layout. Default <c>:processing</c>.
    /// </summary>
    public string ProcessingSuffix { get; set; } = ":processing";

    /// <summary>
    /// How long a reservation may stay unacknowledged before it is recovered (Laravel
    /// <c>retry_after</c>). Default 90 seconds. <c>null</c> disables recovery (zset: <c>:reserved</c> is
    /// never migrated; legacy: deadlines are written as <c>+inf</c>). A negative value is a configuration
    /// error (<see cref="ArgumentOutOfRangeException"/> when the consumer is created).
    /// </summary>
    public TimeSpan? RetryAfter { get; set; } = TimeSpan.FromSeconds(90);

    /// <summary>
    /// Max members moved per migrate / sweep pass. Default -1 (no limit); a positive value bounds
    /// the work done before each reserve.
    /// </summary>
    public int MigrationBatchSize { get; set; } = -1;

    /// <summary>Overrides the clock (tests / conformance). Default: the system clock.</summary>
    public Func<DateTimeOffset>? Clock { get; set; }

    /// <summary>
    /// The dead-letter list poison messages are written to. Default <c>&lt;ready key&gt;.dlq</c>
    /// (<c>queues:&lt;name&gt;.dlq</c> for zset, <c>&lt;name&gt;.dlq</c> for legacy).
    /// </summary>
    public string? DeadLetterKey { get; set; }

    /// <summary>What to do with a message whose handler threw. Default <see cref="RedisFailureAction.Leave"/>.</summary>
    public RedisFailureAction OnHandlerFailure { get; set; } = RedisFailureAction.Leave;

    /// <summary>Delay applied by <see cref="RedisFailureAction.Release"/> on the zset layout. Default zero.</summary>
    public TimeSpan ReleaseDelay { get; set; } = TimeSpan.Zero;

    /// <summary>Max messages reserved and routed per <see cref="RedisConsumer.PollAsync"/> call (default 10).</summary>
    public int MaxMessages { get; set; } = 10;

    /// <summary>
    /// Upper bound of the sleep between empty polls in <see cref="RedisConsumer.RunAsync"/>: the wait starts
    /// at 100 ms, doubles on every empty poll up to this cap and resets when a message arrives
    /// (StackExchange.Redis has no blocking <c>BLPOP</c> on its multiplexed connection, so both layouts
    /// sleep-poll). Default 1 second, which is also the maximum (ADR-0033 section 10: 100 ms doubling, 1 s cap):
    /// a larger value is clamped to <see cref="MaxIdleDelay"/>, a negative one to zero.
    /// <see cref="TimeSpan.Zero"/> polls continuously.
    /// </summary>
    public TimeSpan IdleDelay
    {
        get => _idleDelay;
        set => _idleDelay = value < TimeSpan.Zero ? TimeSpan.Zero : value > MaxIdleDelay ? MaxIdleDelay : value;
    }

    /// <summary>The hard upper bound of <see cref="IdleDelay"/> (1 second).</summary>
    public static readonly TimeSpan MaxIdleDelay = TimeSpan.FromSeconds(1);

    private TimeSpan _idleDelay = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Called for a non-conformant envelope, an unmapped URN (with no
    /// <see cref="OnUnknownUrn"/>), or a throwing handler. The poll loop never stops.
    /// </summary>
    public Action<Exception, Envelope, string>? OnError { get; set; }

    /// <summary>Called instead of erroring when a URN has no handler; the message is then acked (removed).</summary>
    public Func<Envelope, string, CancellationToken, Task>? OnUnknownUrn { get; set; }

    /// <summary>
    /// Called with a short message for non-fatal events: a poison element dead-lettered, or an ack /
    /// release on a reservation that already expired ("reservation expired" — not an error).
    /// </summary>
    public Action<string>? OnWarning { get; set; }
}
