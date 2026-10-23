namespace BabelQueue.Redis;

/// <summary>What <see cref="RedisConsumer"/> does with a reserved message whose handler threw.</summary>
public enum RedisFailureAction
{
    /// <summary>
    /// Leave the message reserved (the default, unchanged behaviour): it comes back after
    /// <see cref="RedisConsumerOptions.RetryAfter"/> through the migrate (zset) / sweep (legacy) pass.
    /// </summary>
    Leave = 0,

    /// <summary>
    /// Release it straight away. Zset: one script drops the <c>:reserved</c> member and parks the body
    /// (<c>attempts + 1</c>, unknown keys preserved) on <c>:delayed</c> at
    /// <c>floor(now) + ceil(<see cref="RedisConsumerOptions.ReleaseDelay"/>)</c>. Legacy: the body goes back
    /// to the ready list byte for byte (a delay is not available on the legacy layout).
    /// </summary>
    Release = 1,
}
