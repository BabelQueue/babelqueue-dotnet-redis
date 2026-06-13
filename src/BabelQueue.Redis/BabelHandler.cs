namespace BabelQueue.Redis;

/// <summary>
/// Processes one decoded, validated envelope and the raw queue element it arrived on
/// (the verbatim envelope JSON — the reservation handle on the <c>&lt;queue&gt;:processing</c>
/// list). Completing normally acknowledges it (the consumer <c>LREM</c>s it from the
/// processing list); throwing leaves it reserved for a later sweep/retry.
/// </summary>
public delegate Task BabelHandler(Envelope envelope, string rawBody, CancellationToken cancellationToken);
