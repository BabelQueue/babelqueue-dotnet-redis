using StackExchange.Redis;

namespace BabelQueue.Redis;

/// <summary>
/// Pushes canonical-envelope messages onto one Redis list (the reliable-queue
/// pattern, broker-bindings §1). Unlike the SQS/RabbitMQ bindings there is
/// <b>no</b> native-metadata projection: a Redis list element carries no headers,
/// so the stored element <b>is</b> the canonical envelope JSON, byte-for-byte, with
/// no wrapping and no added fields (contract §1.2, conformance <c>redis.payload_identity</c>).
/// A Go/Python/... consumer pops and decodes that same body. The envelope is
/// unchanged (<c>schema_version</c> stays 1); Redis support is purely additive.
/// With <see cref="RedisLayout.Zset"/> the publisher speaks Laravel's layout (<c>RPUSH queues:&lt;name&gt;</c> +
/// a <c>:notify</c> token, delayed publishes on <c>:delayed</c>); headers are not carried there.
/// </summary>
public sealed class RedisPublisher
{
    private readonly RedisQueue _engine;
    private readonly string _queue;

    /// <param name="database">The StackExchange.Redis database (<see cref="IDatabase"/> — mockable in tests).</param>
    /// <param name="queue">The Redis list key messages are appended to (the logical queue name).</param>
    public RedisPublisher(IDatabase database, string queue)
        : this(database, queue, new RedisPublisherOptions())
    {
    }

    /// <param name="database">The StackExchange.Redis database.</param>
    /// <param name="queue">The logical queue name.</param>
    /// <param name="options">Layout selection (<see cref="RedisLayout.Zset"/> publishes to a Laravel-style queue).</param>
    public RedisPublisher(IDatabase database, string queue, RedisPublisherOptions options)
        : this(database, queue, options, clock: null)
    {
    }

    internal RedisPublisher(IDatabase database, string queue, RedisPublisherOptions options, Func<double>? clock)
    {
        ArgumentNullException.ThrowIfNull(database);
        ArgumentException.ThrowIfNullOrEmpty(queue);
        ArgumentNullException.ThrowIfNull(options);
        _queue = queue;
        _engine = new RedisQueue(
            database,
            queue,
            options.Layout,
            options.KeyPrefix,
            processingSuffix: ":processing",
            retryAfter: null,
            migrationBatchSize: -1,
            clock ?? RedisConsumer.ClockOf(options.Clock));
    }

    /// <summary>The ready list key messages are appended to.</summary>
    public string ReadyKey => _engine.Ready;

    /// <summary>
    /// Zset layout only: publishes <c>(urn, data)</c> to be delivered after <paramref name="delay"/>
    /// (<c>ZADD :delayed floor(now)+ceil(delay)</c>, no notify token — §1.6). Returns <c>meta.id</c>.
    /// </summary>
    /// <exception cref="NotSupportedException">The legacy layout has no delayed set.</exception>
    public async Task<string> PublishDelayedAsync(
        string urn,
        IReadOnlyDictionary<string, object?>? data,
        TimeSpan delay,
        string? traceId = null,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var envelope = EnvelopeCodec.Make(urn, data, _queue, traceId);
        await _engine.PushDelayedAsync(EnvelopeCodec.Encode(envelope), delay).ConfigureAwait(false);
        return envelope.Meta?.Id ?? string.Empty;
    }

    /// <summary>
    /// Builds the canonical envelope for <c>(urn, data)</c>, appends it verbatim to the
    /// queue list (<c>RPUSH</c>), and returns the message id (<c>meta.id</c>).
    /// </summary>
    public Task<string> PublishAsync(
        string urn,
        IReadOnlyDictionary<string, object?>? data = null,
        string? traceId = null,
        CancellationToken cancellationToken = default)
        => PublishWithHeadersAsync(urn, data, headers: null, traceId, cancellationToken);

    /// <summary>
    /// The header-aware (ADR-0028) counterpart of
    /// <see cref="PublishAsync(string, IReadOnlyDictionary{string, object?}?, string?, CancellationToken)"/>:
    /// when <paramref name="headers"/> carries usable entries (e.g. a W3C <c>traceparent</c> from
    /// <c>Telemetry.PublishAsync(..., headers, ...)</c>), the value <c>RPUSH</c>ed is a
    /// transport-owned frame (<c>{"__bq_frame":1,"headers":…,"body":&lt;raw envelope&gt;}</c>) that
    /// carries the headers beside the frozen envelope (GR-1) — never in it. With no usable headers
    /// (or a <c>null</c>/empty map) it degrades to a byte-identical bare
    /// <see cref="PublishAsync(string, IReadOnlyDictionary{string, object?}?, string?, CancellationToken)"/>,
    /// so nothing regresses. The stored frame <b>is</b> the <c>LREM</c> ack handle, so the
    /// reliable-queue semantics are untouched.
    /// </summary>
    /// <param name="urn">The message URN to publish.</param>
    /// <param name="data">The message payload, or <c>null</c> for an empty body.</param>
    /// <param name="headers">The out-of-band transport headers to ride beside the envelope.</param>
    /// <param name="traceId">An existing trace id to continue, or <c>null</c> to mint one.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task<string> PublishWithHeadersAsync(
        string urn,
        IReadOnlyDictionary<string, object?>? data,
        IReadOnlyDictionary<string, string>? headers,
        string? traceId = null,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var envelope = EnvelopeCodec.Make(urn, data, _queue, traceId);
        return await PublishEnvelopeAsync(envelope, headers, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Pushes an <b>already-built</b> <paramref name="envelope"/> exactly as given (the same bare / framed value and
    /// <c>RPUSH</c> as <see cref="PublishWithHeadersAsync"/>) without rebuilding it: <c>meta.id</c>, <c>trace_id</c>
    /// and every extension field reach the wire unchanged. Use it as the <c>send</c> callback of
    /// <c>Telemetry.PublishAsync(..., headers, env =&gt; publisher.PublishEnvelopeAsync(env, headers), queue)</c>, so the
    /// producer span's <c>messaging.message.id</c> is the <c>meta.id</c> on the wire. Returns <c>meta.id</c>.
    /// </summary>
    /// <param name="envelope">The envelope to push; its <c>meta.queue</c> is the caller's responsibility.</param>
    /// <remarks>
    /// <c>meta.queue</c> is whatever the envelope carries; it is not rewritten to this publisher's target. An envelope
    /// built by <c>Telemetry.PublishAsync</c> carries that call's <c>queue</c> argument (default <c>"default"</c>), so pass
    /// the queue/topic name there, or the wire <c>meta.queue</c> and the span's destination will not name it.
    /// </remarks>
    /// <param name="headers">The out-of-band transport headers to ride beside the envelope (legacy layout only).</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task<string> PublishEnvelopeAsync(
        Envelope envelope,
        IReadOnlyDictionary<string, string>? headers = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(envelope);
        cancellationToken.ThrowIfCancellationRequested();

        // Bare: the list element IS the envelope JSON byte-for-byte (§1.2). Framed (headers present):
        // the headers ride in a transport-owned frame beside the frozen envelope (ADR-0028).
        // The zset layout forbids the frame key, so headers are dropped there (traceparent is not carried).
        var body = EnvelopeCodec.Encode(envelope);
        var value = _engine.Layout == RedisLayout.Zset ? body : RedisFrame.FrameValue(body, headers);
        await _engine.PushAsync(value).ConfigureAwait(false);

        return envelope.Meta?.Id ?? string.Empty;
    }
}
