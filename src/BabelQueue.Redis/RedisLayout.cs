namespace BabelQueue.Redis;

/// <summary>
/// Which Redis key layout a <see cref="RedisConsumer"/> / <see cref="RedisPublisher"/> speaks
/// (broker-bindings §1.1, ADR-0033).
/// </summary>
public enum RedisLayout
{
    /// <summary>
    /// The .NET-owned reliable queue (the default, unchanged key names): ready list <c>&lt;queue&gt;</c>,
    /// in-flight list <c>&lt;queue&gt;:processing</c> (<see cref="RedisConsumerOptions.ProcessingSuffix"/>)
    /// and a deadline sorted set <c>&lt;queue&gt;:processing:deadlines</c> so a crashed worker's
    /// reservation is swept back to the ready list after <see cref="RedisConsumerOptions.RetryAfter"/>.
    /// Neither reads nor writes the Laravel notify list.
    /// </summary>
    Legacy = 0,

    /// <summary>
    /// The Laravel (sorted-set) layout: ready list <c>queues:&lt;name&gt;</c>, <c>:delayed</c> and
    /// <c>:reserved</c> sorted sets and the <c>:notify</c> list, so a .NET worker can share a queue
    /// with a Laravel (PHP) producer/worker. Opt-in.
    /// </summary>
    Zset = 1,
}
