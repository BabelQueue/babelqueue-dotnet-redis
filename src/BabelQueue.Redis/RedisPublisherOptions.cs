namespace BabelQueue.Redis;

/// <summary>Layout selection for <see cref="RedisPublisher"/>.</summary>
public sealed class RedisPublisherOptions
{
    /// <summary>Key layout. Default <see cref="RedisLayout.Legacy"/> (a plain list, keys unchanged).</summary>
    public RedisLayout Layout { get; set; } = RedisLayout.Legacy;

    /// <summary>Key prefix of the zset layout. Default <c>queues:</c>.</summary>
    public string KeyPrefix { get; set; } = "queues:";

    /// <summary>Overrides the clock used to score delayed publishes. Default: the system clock.</summary>
    public Func<DateTimeOffset>? Clock { get; set; }
}
