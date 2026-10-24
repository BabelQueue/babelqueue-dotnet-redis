using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using StackExchange.Redis;

namespace BabelQueue.Redis;

/// <summary>
/// Drains one Redis queue with a crash-safe reservation (broker-bindings §1, ADR-0033), then decodes,
/// validates, routes each message to the handler registered for its URN and acks it on success.
/// </summary>
/// <remarks>
/// <para>
/// <b>Legacy layout</b> (<see cref="RedisLayout.Legacy"/>, the default, key names unchanged): the head of
/// <c>&lt;queue&gt;</c> is moved onto <c>&lt;queue&gt;:processing</c> and a deadline member
/// <c>&lt;token&gt;:&lt;raw&gt;</c> (128-bit CSPRNG token, 32 lowercase hex) is scored
/// <c>floor(now) + retry_after</c> on <c>&lt;queue&gt;:processing:deadlines</c>. Before every reserve a sweep
/// returns expired reservations to the ready list byte for byte (attempts not incremented), so a crashed
/// worker's message is not stranded. Ack / release / dead-letter act only while the deadline member still
/// exists; a late ack on an expired reservation touches nothing and raises a "reservation expired" warning.
/// </para>
/// <para>
/// <b>Zset layout</b> (<see cref="RedisLayout.Zset"/>, opt-in): Laravel's sorted-set layout
/// (<c>queues:&lt;name&gt;</c>, <c>:delayed</c>, <c>:reserved</c>, <c>:notify</c>). Every reserve first migrates
/// due <c>:delayed</c> and <c>:reserved</c> members, then runs Laravel's pop script (attempts + 1, deadline
/// <c>floor(now) + retry_after</c>). An element the script cannot parse is dead-lettered as poison. A handler
/// that throws leaves the message reserved (default) or releases it (<see cref="RedisFailureAction.Release"/>).
/// Header frames are not carried on this layout (the <c>__bq_frame</c> key is forbidden there).
/// </para>
/// <para>
/// A non-conformant envelope (raw element) and an unmapped URN with no <see cref="RedisConsumerOptions.OnUnknownUrn"/>
/// hook (the envelope plus a <c>dead_letter</c> block, <c>reason: unknown_urn</c>) are moved to the dead-letter list
/// and their reservation is acked in the same script.
/// </para>
/// <para>
/// The loop never stops on a bad message — observe it via the option hooks. A handler that keeps failing on a
/// message that is released or recovered will redeliver it indefinitely; bound it with the handler or move it
/// aside with <see cref="RedisConsumerOptions.OnError"/>.
/// </para>
/// </remarks>
public sealed class RedisConsumer
{
    private const int InitialIdleMilliseconds = 100;

    private static readonly UTF8Encoding StrictUtf8 = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    private static readonly JsonWriterOptions PoisonWriterOptions = new()
    {
        Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
        Indented = false,
    };

    private readonly string _queue;
    private readonly RedisQueue _engine;
    private readonly IReadOnlyDictionary<string, BabelHandler> _handlers;
    private readonly RedisConsumerOptions _options;

    public RedisConsumer(
        IDatabase database,
        string queue,
        IReadOnlyDictionary<string, BabelHandler> handlers,
        RedisConsumerOptions? options = null)
        : this(database, queue, handlers, options, clock: null)
    {
    }

    internal RedisConsumer(
        IDatabase database,
        string queue,
        IReadOnlyDictionary<string, BabelHandler> handlers,
        RedisConsumerOptions? options,
        Func<double>? clock)
    {
        ArgumentNullException.ThrowIfNull(database);
        ArgumentException.ThrowIfNullOrEmpty(queue);
        ArgumentNullException.ThrowIfNull(handlers);
        _queue = queue;
        _handlers = handlers;
        _options = options ?? new RedisConsumerOptions();
        _engine = new RedisQueue(
            database,
            queue,
            _options.Layout,
            _options.KeyPrefix,
            _options.ProcessingSuffix,
            _options.RetryAfter,
            _options.MigrationBatchSize,
            clock ?? ClockOf(_options.Clock),
            _options.DeadLetterKey);
    }

    /// <summary>The legacy processing (reservation) list key; empty on the zset layout (see <see cref="ReservedKey"/>).</summary>
    public string ProcessingKey => _engine.Processing;

    /// <summary>The ready list key.</summary>
    public string ReadyKey => _engine.Ready;

    /// <summary>The zset <c>:reserved</c> sorted-set key; empty on the legacy layout.</summary>
    public string ReservedKey => _engine.Reserved;

    /// <summary>The dead-letter list key poison messages are written to.</summary>
    public string DeadLetterKey => _engine.DeadLetter;

    internal static Func<double>? ClockOf(Func<DateTimeOffset>? clock)
        => clock is null ? null : () => clock().ToUnixTimeMilliseconds() / 1000.0;

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

            var reservation = await _engine.ReserveAsync().ConfigureAwait(false);
            if (reservation is null)
            {
                break;
            }

            reserved++;
            await HandleAsync(reservation, cancellationToken).ConfigureAwait(false);
        }

        return reserved;
    }

    /// <summary>
    /// Poll until <paramref name="cancellationToken"/> is cancelled. Between empty polls it sleeps 100 ms,
    /// doubling up to <see cref="RedisConsumerOptions.IdleDelay"/>, and resets when a message arrives.
    /// </summary>
    public async Task RunAsync(CancellationToken cancellationToken = default)
    {
        var cap = _options.IdleDelay;
        var wait = cap < TimeSpan.FromMilliseconds(InitialIdleMilliseconds) ? cap : TimeSpan.FromMilliseconds(InitialIdleMilliseconds);
        while (!cancellationToken.IsCancellationRequested)
        {
            var reserved = await PollAsync(cancellationToken).ConfigureAwait(false);
            if (reserved > 0)
            {
                wait = cap < TimeSpan.FromMilliseconds(InitialIdleMilliseconds) ? cap : TimeSpan.FromMilliseconds(InitialIdleMilliseconds);
                continue;
            }

            if (wait > TimeSpan.Zero)
            {
                await Task.Delay(wait, cancellationToken).ConfigureAwait(false);
                wait = wait + wait > cap ? cap : wait + wait;
            }
        }
    }

    private async Task HandleAsync(RedisReservation reservation, CancellationToken cancellationToken)
    {
        if (reservation.IsPoison)
        {
            await DeadLetterPoisonAsync(reservation).ConfigureAwait(false);
            return;
        }

        // Legacy: the reserved value may be a header frame (ADR-0028) — unframe it so the handler sees
        // the §1.2 envelope body. Zset: the element is the bare body; framing is forbidden (A3).
        string body;
        IReadOnlyDictionary<string, string> headers;
        if (_engine.Layout == RedisLayout.Legacy)
        {
            (body, headers) = RedisFrame.Unframe(reservation.HandlerBody.ToString());
        }
        else
        {
            body = reservation.HandlerBody.ToString();
            headers = new Dictionary<string, string>();
        }

        var envelope = EnvelopeCodec.Decode(body);
        if (_engine.Layout == RedisLayout.Zset)
        {
            envelope = envelope with { Attempts = reservation.Attempts };
        }

        if (!EnvelopeCodec.Accepts(envelope))
        {
            _options.OnError?.Invoke(
                new BabelQueueException("Rejected a non-conformant BabelQueue envelope from Redis."),
                envelope, body);

            // Never best-effort re-encoded (an unsupported schema is not parsed): the body's bytes go to the
            // dead-letter list unchanged inside the ADR-0035 record — the same record a poison element gets.
            await DeadLetterAsync(
                reservation,
                PoisonRecordFor(reservation, Encoding.UTF8.GetBytes(body)),
                "non-conformant envelope moved to the dead-letter list").ConfigureAwait(false);
            return;
        }

        var urn = EnvelopeCodec.Urn(envelope);
        if (!_handlers.TryGetValue(urn, out var handler))
        {
            if (_options.OnUnknownUrn is not null)
            {
                await _options.OnUnknownUrn(envelope, body, cancellationToken).ConfigureAwait(false);
                await AckAsync(reservation).ConfigureAwait(false);
            }
            else
            {
                var error = new UnknownUrnException(urn);
                _options.OnError?.Invoke(error, envelope, body);

                // error-handling.md section 6: an unroutable message is dead-lettered (reason unknown_urn), not
                // left to lapse and redeliver forever.
                await DeadLetterAsync(
                    reservation,
                    UnknownUrnRecord(envelope, headers, error, reservation),
                    "unknown URN moved to the dead-letter list").ConfigureAwait(false);
            }

            return;
        }

        try
        {
            await handler(envelope, body, headers, cancellationToken).ConfigureAwait(false);
        }
#pragma warning disable CA1031 // The consume loop must survive any handler exception.
        catch (Exception error)
#pragma warning restore CA1031
        {
            _options.OnError?.Invoke(error, envelope, body);
            if (_options.OnHandlerFailure == RedisFailureAction.Release)
            {
                await ReleaseAsync(reservation).ConfigureAwait(false);
            }

            // Leave: the element stays reserved and is recovered after RetryAfter.
            return;
        }

        await AckAsync(reservation).ConfigureAwait(false);
    }

    private Task DeadLetterPoisonAsync(RedisReservation reservation)
        => DeadLetterAsync(reservation, PoisonRecordFor(reservation, PoisonBodyBytes(reservation)), "poison element moved to the dead-letter list");

    /// <summary>
    /// The bytes of a poison element as the DLQ record carries them: the stored element, with a legacy header frame
    /// (ADR-0028) unwrapped so the record is the ADR-0035 record of the envelope body itself. An element that is not
    /// valid UTF-8 is kept byte for byte (it cannot be a frame).
    /// </summary>
    private byte[] PoisonBodyBytes(RedisReservation reservation)
    {
        var stored = (byte[]?)reservation.HandlerBody ?? [];
        if (_engine.Layout != RedisLayout.Legacy)
        {
            return stored;
        }

        try
        {
            var (body, _) = RedisFrame.Unframe(StrictUtf8.GetString(stored));
            return Encoding.UTF8.GetBytes(body);
        }
        catch (DecoderFallbackException)
        {
            return stored;
        }
    }

    /// <summary>
    /// The poison DLQ record (ADR-0035, broker-bindings §1.1 / §1.3): <paramref name="body"/> spliced byte for byte
    /// with an additive <c>dead_letter</c> block (<c>reason: poison</c>, <c>original_queue</c>, <c>attempts: 0</c>,
    /// <c>lang: dotnet</c>), or the <c>raw</c> wrapper when the body is not a spliceable JSON object. If the record
    /// cannot be built the stored element is dead-lettered instead — the message is never lost.
    /// </summary>
    private RedisValue PoisonRecordFor(RedisReservation reservation, byte[] body)
    {
        try
        {
            using var stream = new MemoryStream();
            using (var writer = new Utf8JsonWriter(stream, PoisonWriterOptions))
            {
                writer.WriteStartObject();
                writer.WriteString("reason", "poison");
                writer.WriteString("error", "Redis value is not a conformant BabelQueue envelope.");
                writer.WriteString("exception", typeof(BabelQueueException).FullName);
                writer.WriteNumber("failed_at", DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
                writer.WriteString("original_queue", _queue);
                writer.WriteNumber("attempts", 0);
                writer.WriteString("lang", EnvelopeCodec.SourceLang);
                writer.WriteEndObject();
            }

            return PoisonRecord.Build(body, Encoding.UTF8.GetString(stream.ToArray()));
        }
#pragma warning disable CA1031 // Never lose the message: fall back to the stored element.
        catch (Exception)
#pragma warning restore CA1031
        {
            return reservation.HandlerBody;
        }
    }

    private async Task DeadLetterAsync(RedisReservation reservation, RedisValue body, string movedWarning)
    {
        var moved = await _engine.DeadLetterAsync(reservation, body).ConfigureAwait(false);
        _options.OnWarning?.Invoke(moved ? movedWarning : RedisQueue.ExpiredWarning);
    }

    /// <summary>
    /// The DLQ record of an unmapped URN (ADR-0009): the original envelope plus the additive <c>dead_letter</c>
    /// block (<c>reason: unknown_urn</c>, <c>original_queue</c>, <c>attempts</c>, <c>lang: dotnet</c>). On the
    /// legacy layout the header frame is carried over so <c>traceparent</c> survives; if the envelope cannot be
    /// re-encoded the raw element is dead-lettered instead — the message is never lost.
    /// </summary>
    private RedisValue UnknownUrnRecord(
        Envelope envelope,
        IReadOnlyDictionary<string, string> headers,
        UnknownUrnException error,
        RedisReservation reservation)
    {
        try
        {
            var dead = DeadLetters.Annotate(
                envelope,
                "unknown_urn",
                _queue,
                error: error.Message,
                exception: typeof(UnknownUrnException).FullName);
            var encoded = EnvelopeCodec.Encode(dead);
            return _engine.Layout == RedisLayout.Legacy ? RedisFrame.FrameValue(encoded, headers) : encoded;
        }
#pragma warning disable CA1031 // A record that cannot be built must not strand the message.
        catch (Exception)
#pragma warning restore CA1031
        {
            return reservation.HandlerBody;
        }
    }

    private async Task AckAsync(RedisReservation reservation)
    {
        if (!await _engine.AckAsync(reservation).ConfigureAwait(false))
        {
            _options.OnWarning?.Invoke(RedisQueue.ExpiredWarning);
        }
    }

    private async Task ReleaseAsync(RedisReservation reservation)
    {
        if (!await _engine.ReleaseAsync(reservation, _options.ReleaseDelay).ConfigureAwait(false))
        {
            _options.OnWarning?.Invoke(RedisQueue.ExpiredWarning);
        }
    }
}
