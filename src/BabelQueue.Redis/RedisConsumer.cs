using StackExchange.Redis;

namespace BabelQueue.Redis;

/// <summary>
/// Drains a Redis list with the reliable-queue pattern (broker-bindings §1, mirroring the
/// Go reference): each message is <b>reserved</b> by atomically moving the head of the queue
/// onto a per-queue <c>&lt;queue&gt;:processing</c> list (<c>LMOVE LEFT RIGHT</c>), so an
/// in-flight message survives a worker crash; it is then decoded, validated, routed to the
/// handler registered for its URN, and <b>acked</b> by removing it from the processing list
/// (<c>LREM</c>) on success. A throwing handler leaves the reserved element on the processing
/// list (at-least-once); the poll loop never stops on a bad message — observe via the option
/// hooks. The list element is the canonical envelope JSON verbatim — there is no native
/// metadata and no projection (§1.2), so routing always decodes the body.
/// </summary>
/// <remarks>
/// This is a <b>.NET-owned reliable queue</b>: produce/reserve/ack are self-consistent and
/// crash-safe on a queue this SDK owns end-to-end. Full parity with Laravel's
/// reserved-sorted-set reservation on a <i>shared</i> PHP+.NET Redis queue
/// (<c>queues:&lt;name&gt;:reserved</c> scored by <c>retry_after</c>, attempts incremented by
/// the reservation Lua script) is a separate task — see broker-bindings §1.4.
/// </remarks>
public sealed class RedisConsumer
{
    private readonly IDatabase _database;
    private readonly string _queue;
    private readonly string _processing;
    private readonly IReadOnlyDictionary<string, BabelHandler> _handlers;
    private readonly RedisConsumerOptions _options;

    public RedisConsumer(
        IDatabase database,
        string queue,
        IReadOnlyDictionary<string, BabelHandler> handlers,
        RedisConsumerOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(database);
        ArgumentException.ThrowIfNullOrEmpty(queue);
        ArgumentNullException.ThrowIfNull(handlers);
        _database = database;
        _queue = queue;
        _handlers = handlers;
        _options = options ?? new RedisConsumerOptions();
        _processing = queue + _options.ProcessingSuffix;
    }

    /// <summary>The per-queue processing (reservation) list key in-flight messages are moved to.</summary>
    public string ProcessingKey => _processing;

    /// <summary>
    /// Reserve up to <see cref="RedisConsumerOptions.MaxMessages"/> messages, route each, and
    /// ack the ones handled. Returns how many were reserved (0 when the queue was empty).
    /// </summary>
    public async Task<int> PollAsync(CancellationToken cancellationToken = default)
    {
        var reserved = 0;
        for (var i = 0; i < _options.MaxMessages; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            // Reserve: atomically move the head of the queue onto the processing list (LMOVE).
            var element = await _database
                .ListMoveAsync(_queue, _processing, ListSide.Left, ListSide.Right)
                .ConfigureAwait(false);
            if (element.IsNull)
            {
                break;
            }

            reserved++;
            await HandleAsync(element!, cancellationToken).ConfigureAwait(false);
        }

        return reserved;
    }

    /// <summary>
    /// Poll until <paramref name="cancellationToken"/> is cancelled, waiting
    /// <see cref="RedisConsumerOptions.IdleDelay"/> between empty polls so the loop does not spin.
    /// </summary>
    public async Task RunAsync(CancellationToken cancellationToken = default)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            var reserved = await PollAsync(cancellationToken).ConfigureAwait(false);
            if (reserved == 0 && _options.IdleDelay > TimeSpan.Zero)
            {
                await Task.Delay(_options.IdleDelay, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private async Task HandleAsync(string rawBody, CancellationToken cancellationToken)
    {
        var envelope = EnvelopeCodec.Decode(rawBody);

        if (!EnvelopeCodec.Accepts(envelope))
        {
            _options.OnError?.Invoke(
                new BabelQueueException("Rejected a non-conformant BabelQueue envelope from Redis."),
                envelope, rawBody);
            return;
        }

        var urn = EnvelopeCodec.Urn(envelope);
        if (!_handlers.TryGetValue(urn, out var handler))
        {
            if (_options.OnUnknownUrn is not null)
            {
                await _options.OnUnknownUrn(envelope, rawBody, cancellationToken).ConfigureAwait(false);
                await AckAsync(rawBody).ConfigureAwait(false);
            }
            else
            {
                _options.OnError?.Invoke(new UnknownUrnException(urn), envelope, rawBody);
            }

            return;
        }

        try
        {
            await handler(envelope, rawBody, cancellationToken).ConfigureAwait(false);
            await AckAsync(rawBody).ConfigureAwait(false);
        }
#pragma warning disable CA1031 // The consume loop must survive any handler exception.
        catch (Exception error)
#pragma warning restore CA1031
        {
            // Leave the element on the processing list — it stays reserved for a later sweep/retry.
            _options.OnError?.Invoke(error, envelope, rawBody);
        }
    }

    /// <summary>Ack: remove the reserved element from the processing list (LREM, count 1).</summary>
    private async Task AckAsync(string rawBody)
        => await _database.ListRemoveAsync(_processing, rawBody, 1).ConfigureAwait(false);
}
