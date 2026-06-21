namespace BabelQueue.Redis;

/// <summary>
/// Processes one decoded, validated envelope, the raw <b>wire-envelope body</b> it arrived on
/// (un-framed — the §1.2 verbatim envelope JSON, even when it was delivered inside an out-of-band
/// header frame), and any out-of-band transport <paramref name="headers"/> that rode beside it
/// (ADR-0028; empty when none). Hand <paramref name="headers"/> to <c>Telemetry.Wrap(handler,
/// headers)</c> to start the consumer span as a true child of the producer span (a W3C
/// <c>traceparent</c> carried over the frame); with no usable header the core falls back to the
/// v0.1 <c>trace_id</c> mapping. Completing normally acknowledges the message (the consumer
/// <c>LREM</c>s the reserved element — frame or bare — from the processing list); throwing leaves
/// it reserved for a later sweep/retry.
/// </summary>
public delegate Task BabelHandler(
    Envelope envelope,
    string rawBody,
    IReadOnlyDictionary<string, string> headers,
    CancellationToken cancellationToken);
