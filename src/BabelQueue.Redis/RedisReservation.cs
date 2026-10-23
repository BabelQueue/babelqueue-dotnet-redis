using StackExchange.Redis;

namespace BabelQueue.Redis;

/// <summary>
/// One reserved (in-flight) queue element: the body the handler sees plus the handle needed to
/// ack / release / dead-letter it.
/// </summary>
internal sealed class RedisReservation
{
    internal RedisReservation(RedisValue handlerBody, RedisValue reserved, string? token, bool isPoison, int attempts)
    {
        HandlerBody = handlerBody;
        Reserved = reserved;
        Token = token;
        IsPoison = isPoison;
        Attempts = attempts;
    }

    /// <summary>The raw list element exactly as it was stored (what the handler sees).</summary>
    internal RedisValue HandlerBody { get; }

    /// <summary>
    /// Zset layout: the <c>:reserved</c> member (the cjson re-encoded job, or the raw job for a
    /// poison element). Legacy layout: the raw element (also the <c>LREM</c> handle).
    /// </summary>
    internal RedisValue Reserved { get; }

    /// <summary>Legacy layout only: the 32-hex-char reservation token (null in the zset layout).</summary>
    internal string? Token { get; }

    /// <summary>True when the zset pop script could not decode the element (A1) — it must be dead-lettered.</summary>
    internal bool IsPoison { get; }

    /// <summary>The attempt number this delivery counts as: <c>attempts + 1</c>, or 1 for a poison element.</summary>
    internal int Attempts { get; }
}
