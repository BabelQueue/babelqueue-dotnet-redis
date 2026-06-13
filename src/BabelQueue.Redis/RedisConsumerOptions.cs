namespace BabelQueue.Redis;

/// <summary>Tuning and hooks for <see cref="RedisConsumer"/>.</summary>
public sealed class RedisConsumerOptions
{
    /// <summary>
    /// Suffix appended to the queue key to name the per-queue processing (reservation)
    /// list. A message moved here is in-flight; <c>LREM</c> on ack removes it. Default
    /// <c>:processing</c> (matches the Go reference reliable-queue).
    /// </summary>
    public string ProcessingSuffix { get; set; } = ":processing";

    /// <summary>Max messages reserved and routed per <see cref="RedisConsumer.PollAsync"/> call (default 10).</summary>
    public int MaxMessages { get; set; } = 10;

    /// <summary>
    /// Delay between polls when a poll reserved nothing, so an empty <see cref="RedisConsumer.RunAsync"/>
    /// loop does not spin. Default 1 second. Set to <see cref="TimeSpan.Zero"/> to poll continuously.
    /// </summary>
    public TimeSpan IdleDelay { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Called for a non-conformant envelope, an unmapped URN (with no
    /// <see cref="OnUnknownUrn"/>), or a throwing handler. The poll loop never stops.
    /// </summary>
    public Action<Exception, Envelope, string>? OnError { get; set; }

    /// <summary>Called instead of erroring when a URN has no handler; the message is then acked (removed).</summary>
    public Func<Envelope, string, CancellationToken, Task>? OnUnknownUrn { get; set; }
}
