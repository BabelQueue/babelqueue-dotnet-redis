using System.Text.Json;
using System.Text.Json.Serialization;

namespace BabelQueue.Redis;

/// <summary>
/// The transport-owned header <i>frame</i> for the Redis binding (ADR-0028) — byte-compatible with
/// the Go <c>headerFrame</c> and the PHP <c>RedisTransport</c> frame.
/// </summary>
/// <remarks>
/// <para>
/// Redis stores only the raw list value (the <c>LREM</c> ack handle <b>is</b> that value), so —
/// unlike SQS <c>MessageAttributes</c> or AMQP headers — there is no native per-message metadata
/// channel. To carry out-of-band headers (e.g. a W3C <c>traceparent</c> for cross-hop span linkage)
/// the transport owns a tiny JSON frame distinct from the wire envelope:
/// </para>
/// <code>{"__bq_frame":1,"headers":{"traceparent":"00-…"},"body":"&lt;raw wire envelope&gt;"}</code>
/// <para>
/// <c>RPUSH</c> stores the frame, so the <c>LREM</c> ack handle stays byte-for-byte what was pushed
/// and the reliable-queue semantics (RPUSH/LMOVE/LREM/processing list) are untouched. The frozen
/// wire envelope (GR-1) travels verbatim inside <c>body</c>; the headers ride beside it, never in
/// it. Framing is <b>opt-in and backward compatible</b>: only a non-empty header map writes a frame;
/// otherwise the bare envelope is stored byte-for-byte. <see cref="Unframe"/> detects frame-vs-bare
/// by the reserved <c>__bq_frame</c> sentinel — a frozen wire envelope can never carry it — so a
/// bare value yields <c>(value, empty)</c> and cross-version queues interoperate.
/// </para>
/// </remarks>
internal static class RedisFrame
{
    /// <summary>The reserved frame discriminator key — a frozen wire envelope never has it.</summary>
    internal const string Sentinel = "__bq_frame";

    /// <summary>The current header-frame schema version (the value of <see cref="Sentinel"/>).</summary>
    internal const int Version = 1;

    private static readonly JsonSerializerOptions FrameOptions = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
    };

    /// <summary>
    /// The pure produce-side decision: the exact string to <c>RPUSH</c> for <paramref name="body"/>
    /// + <paramref name="headers"/>. With no usable headers it returns <paramref name="body"/>
    /// verbatim (the bare form, so plain publish and header-less publish store byte-identical
    /// values); otherwise it returns the transport-owned frame JSON. Kept pure so the framing
    /// decision is unit-testable without a broker.
    /// </summary>
    internal static string FrameValue(string body, IReadOnlyDictionary<string, string>? headers)
    {
        var clean = Sanitize(headers);
        if (clean.Count == 0)
        {
            return body;
        }

        return JsonSerializer.Serialize(new FramePayload { Version = Version, Headers = clean, Body = body }, FrameOptions);
    }

    /// <summary>
    /// Interprets a stored Redis list value: returns the unframed wire-envelope body plus the
    /// carried headers. A value is a header frame iff it is a JSON object carrying the reserved
    /// <see cref="Sentinel"/> (a frozen wire envelope never has it); then it yields the unframed
    /// body plus headers. Any other value — a bare envelope, non-JSON, or JSON without the sentinel
    /// — is returned verbatim as the body with empty headers, so older/cross-version queue values
    /// consume exactly as before.
    /// </summary>
    internal static (string Body, Dictionary<string, string> Headers) Unframe(string value)
    {
        // Cheap reject: a frame is always a JSON object and the sentinel substring must appear.
        // This avoids a full parse for the overwhelmingly common bare-envelope case; the substring
        // check only short-circuits negatives.
        if (string.IsNullOrEmpty(value)
            || value[0] != '{'
            || !value.Contains("\"" + Sentinel + "\"", StringComparison.Ordinal))
        {
            return (value, new Dictionary<string, string>(StringComparer.Ordinal));
        }

        FramePayload? frame;
        try
        {
            frame = JsonSerializer.Deserialize<FramePayload>(value, FrameOptions);
        }
        catch (JsonException)
        {
            return (value, new Dictionary<string, string>(StringComparer.Ordinal));
        }

        if (frame is null || frame.Version == 0 || frame.Body is null)
        {
            return (value, new Dictionary<string, string>(StringComparer.Ordinal));
        }

        return (frame.Body, Sanitize(frame.Headers));
    }

    /// <summary>
    /// Copies <paramref name="headers"/>, dropping blank keys and blank values. Returns an empty map
    /// when nothing survives, so callers treat the result as "no headers" with <c>Count == 0</c>.
    /// </summary>
    private static Dictionary<string, string> Sanitize(IReadOnlyDictionary<string, string>? headers)
    {
        var clean = new Dictionary<string, string>(StringComparer.Ordinal);
        if (headers is null)
        {
            return clean;
        }

        foreach (var (key, value) in headers)
        {
            if (!string.IsNullOrEmpty(key) && !string.IsNullOrEmpty(value))
            {
                clean[key] = value;
            }
        }

        return clean;
    }

    private sealed class FramePayload
    {
        [JsonPropertyName(Sentinel)]
        public int Version { get; set; }

        [JsonPropertyName("headers")]
        public Dictionary<string, string>? Headers { get; set; }

        [JsonPropertyName("body")]
        public string? Body { get; set; }
    }
}
