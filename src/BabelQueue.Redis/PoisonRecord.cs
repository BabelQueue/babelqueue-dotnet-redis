// KEEP IN SYNC with BabelQueue.PoisonRecord in babelqueue-dotnet (BabelQueue.Core): this is a verbatim copy
// (namespace and visibility aside), because the BabelQueue.Core release this package references predates the class.
// Delete this file and call BabelQueue.PoisonRecord.Build once the reference is bumped to the release that ships it.

using System.Buffers;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;

namespace BabelQueue.Redis;

/// <summary>
/// Builds the DLQ record of a poison body (ADR-0035, error-handling.md section 6). A body that is a single strict
/// RFC 8259 JSON object, valid UTF-8, without a top-level <c>dead_letter</c> member, is spliced at byte level: every
/// original byte is kept and only the <c>dead_letter</c> member is inserted, so no value is re-encoded (a 1e400 or a
/// 64-bit integer stays as written, a repeated member name is not collapsed). Anything else is wrapped as
/// <c>{"raw": &lt;string&gt;, "raw_encoding"?: "base64", "dead_letter": ...}</c>: <c>raw</c> is the body as plain text
/// when it is valid UTF-8 and otherwise the standard base64 of the exact bytes with <c>"raw_encoding": "base64"</c>.
/// U+FFFD substitution never happens.
/// </summary>
internal static class PoisonRecord
{
    private static readonly UTF8Encoding StrictUtf8 = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    private static readonly JsonWriterOptions WriterOptions = new()
    {
        Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
        Indented = false,
    };

    // The reader default (64) would wrap a deeper, still valid object. 10_100 sits just above Go's 10000-level cap, so a
    // body nested 10001..10100 levels deep is spliced here but wrapped by Go; ADR-0035 section 1 permits either shape.
    private static readonly JsonReaderOptions ReaderOptions = new()
    {
        MaxDepth = 10_100,
        CommentHandling = JsonCommentHandling.Disallow,
        AllowTrailingCommas = false,
    };

    private static readonly byte[] SplicePrefix = Encoding.ASCII.GetBytes(",\"dead_letter\":");

    /// <summary>The record for <paramref name="body"/> with <paramref name="deadLetterJson"/> as a top-level member.</summary>
    internal static byte[] Build(byte[] body, string deadLetterJson)
    {
        ArgumentNullException.ThrowIfNull(deadLetterJson);
        var deadLetter = Encoding.UTF8.GetBytes(deadLetterJson);
        if (!IsSingleJsonObject(deadLetter))
        {
            throw new ArgumentException("deadLetterJson must be the JSON text of a single object.", nameof(deadLetterJson));
        }
        var span = new ReadOnlySpan<byte>(body);
        if (IsSpliceableObject(span))
        {
            var start = 0;
            while (IsJsonSpace(span[start]))
            {
                start++;
            }
            var end = span.Length;
            while (IsJsonSpace(span[end - 1]))
            {
                end--;
            }
            // span[start] is '{' and span[end - 1] is '}'.
            var empty = true;
            for (var i = start + 1; i < end - 1; i++)
            {
                if (!IsJsonSpace(span[i]))
                {
                    empty = false;
                    break;
                }
            }
            var output = new ArrayBufferWriter<byte>(body.Length + deadLetter.Length + 32);
            if (empty)
            {
                Write(output, "{\"dead_letter\":"u8);
            }
            else
            {
                Write(output, span[start..(end - 1)]);
                Write(output, SplicePrefix);
            }
            Write(output, deadLetter);
            Write(output, "}"u8);
            return output.WrittenSpan.ToArray();
        }

        var wrapped = new ArrayBufferWriter<byte>(body.Length * 2 + deadLetter.Length + 64);
        Write(wrapped, "{\"raw\":"u8);
        string raw;
        var base64 = false;
        try
        {
            raw = StrictUtf8.GetString(body);
        }
        catch (DecoderFallbackException)
        {
            raw = Convert.ToBase64String(body);
            base64 = true;
        }
        using (var stream = new MemoryStream())
        {
            using (var writer = new Utf8JsonWriter(stream, WriterOptions))
            {
                writer.WriteStringValue(raw);
            }
            Write(wrapped, stream.ToArray());
        }
        if (base64)
        {
            Write(wrapped, ",\"raw_encoding\":\"base64\""u8);
        }
        Write(wrapped, ",\"dead_letter\":"u8);
        Write(wrapped, deadLetter);
        Write(wrapped, "}"u8);
        return wrapped.WrittenSpan.ToArray();
    }

    // The dead_letter block is spliced in verbatim, so anything but one JSON object would corrupt the record.
    private static bool IsSingleJsonObject(byte[] json)
    {
        try
        {
            var reader = new Utf8JsonReader(json, ReaderOptions);
            if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject)
            {
                return false;
            }
            while (reader.Read())
            {
            }
            return reader.IsFinalBlock;
        }
        catch (JsonException)
        {
            return false;
        }
    }

    private static void Write(ArrayBufferWriter<byte> output, ReadOnlySpan<byte> bytes)
    {
        var target = output.GetSpan(bytes.Length);
        bytes.CopyTo(target);
        output.Advance(bytes.Length);
    }

    private static bool IsJsonSpace(byte c) => c == (byte)' ' || c == (byte)'\t' || c == (byte)'\r' || c == (byte)'\n';

    /// <summary>
    /// True when <paramref name="body"/> is valid UTF-8, a single strict JSON object (no BOM, no trailing data, no
    /// NaN / Infinity, no unpaired surrogate escape) and has no top-level <c>dead_letter</c> member (ADR-0035 section 1).
    /// </summary>
    private static bool IsSpliceableObject(ReadOnlySpan<byte> body)
    {
        try
        {
            _ = StrictUtf8.GetCharCount(body);
        }
        catch (DecoderFallbackException)
        {
            return false;
        }
        var first = 0;
        while (first < body.Length && IsJsonSpace(body[first]))
        {
            first++;
        }
        if (first >= body.Length || body[first] != (byte)'{')
        {
            return false; // empty, a BOM, or a non-object value
        }

        try
        {
            var reader = new Utf8JsonReader(body, ReaderOptions);
            while (reader.Read())
            {
                switch (reader.TokenType)
                {
                    case JsonTokenType.PropertyName when reader.CurrentDepth == 1 && reader.ValueTextEquals("dead_letter"u8):
                        return false;
                    case JsonTokenType.PropertyName:
                    case JsonTokenType.String:
                        if (HasUnpairedSurrogateEscape(reader.HasValueSequence ? reader.ValueSequence.ToArray() : reader.ValueSpan))
                        {
                            return false;
                        }
                        break;
                    default:
                        break;
                }
            }
            return reader.IsFinalBlock && reader.BytesConsumed > 0;
        }
        catch (JsonException)
        {
            return false;
        }
    }

    // `raw` is the still-escaped content of a string token (syntax already validated by the reader).
    private static bool HasUnpairedSurrogateEscape(ReadOnlySpan<byte> raw)
    {
        var i = 0;
        while (i < raw.Length)
        {
            if (raw[i] != (byte)'\\')
            {
                i++;
                continue;
            }
            if (raw[i + 1] != (byte)'u')
            {
                i += 2;
                continue;
            }
            var cp = Hex4(raw, i + 2);
            i += 6;
            if (cp is >= 0xD800 and <= 0xDBFF)
            {
                if (i + 1 >= raw.Length || raw[i] != (byte)'\\' || raw[i + 1] != (byte)'u')
                {
                    return true;
                }
                var low = Hex4(raw, i + 2);
                if (low is < 0xDC00 or > 0xDFFF)
                {
                    return true;
                }
                i += 6;
            }
            else if (cp is >= 0xDC00 and <= 0xDFFF)
            {
                return true;
            }
        }
        return false;
    }

    private static int Hex4(ReadOnlySpan<byte> raw, int at)
    {
        var value = 0;
        for (var k = 0; k < 4; k++)
        {
            var c = raw[at + k];
            value = (value << 4) | (c <= (byte)'9' ? c - '0' : (c | 0x20) - 'a' + 10);
        }
        return value;
    }
}
