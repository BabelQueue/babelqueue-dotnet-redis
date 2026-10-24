using System.Text;
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
        IsPoison = isPoison || HasInvalidUtf8(handlerBody) || HasInvalidUtf8(reserved);
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

    /// <summary>
    /// True when the element must be dead-lettered rather than delivered: the zset pop script could not decode it (A1),
    /// or the stored bytes (the element, or the zset <c>:reserved</c> member) are not valid UTF-8 — poison in both
    /// layouts even when the codec could decode it (broker-bindings §1.1). Such an element is never handed to a handler
    /// and never retried or re-published; it is dead-lettered byte for byte on first delivery.
    /// </summary>
    internal bool IsPoison { get; }

    /// <summary>The attempt number this delivery counts as: <c>attempts + 1</c>, or 1 for a poison element.</summary>
    internal int Attempts { get; }

    private static readonly UTF8Encoding StrictUtf8 = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    private static bool HasInvalidUtf8(RedisValue value)
    {
        if (value.IsNull)
        {
            return false;
        }

        try
        {
            StrictUtf8.GetCharCount((byte[]?)value ?? []);
            return false;
        }
        catch (DecoderFallbackException)
        {
            return true;
        }
    }
}
