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
/// </summary>
public sealed class RedisPublisher
{
    private readonly IDatabase _database;
    private readonly string _queue;

    /// <param name="database">The StackExchange.Redis database (<see cref="IDatabase"/> — mockable in tests).</param>
    /// <param name="queue">The Redis list key messages are appended to (the logical queue name).</param>
    public RedisPublisher(IDatabase database, string queue)
    {
        ArgumentNullException.ThrowIfNull(database);
        ArgumentException.ThrowIfNullOrEmpty(queue);
        _database = database;
        _queue = queue;
    }

    /// <summary>
    /// Builds the canonical envelope for <c>(urn, data)</c>, appends it verbatim to the
    /// queue list (<c>RPUSH</c>), and returns the message id (<c>meta.id</c>).
    /// </summary>
    public async Task<string> PublishAsync(
        string urn,
        IReadOnlyDictionary<string, object?>? data = null,
        string? traceId = null,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var envelope = EnvelopeCodec.Make(urn, data, _queue, traceId);

        // The list element IS the envelope JSON, byte-for-byte — no wrapping (§1.2).
        await _database.ListRightPushAsync(_queue, EnvelopeCodec.Encode(envelope)).ConfigureAwait(false);

        return envelope.Meta?.Id ?? string.Empty;
    }
}
