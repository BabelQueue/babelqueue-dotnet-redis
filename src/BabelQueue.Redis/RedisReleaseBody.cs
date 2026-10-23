using System.Text.Encodings.Web;
using System.Text.Json;

namespace BabelQueue.Redis;

/// <summary>
/// Builds the body a released message is parked with on <c>:delayed</c> (broker-bindings §1.1, A2):
/// the handler's body with only <c>attempts</c> replaced by <c>attempts + 1</c>. Every other
/// top-level key — known or unknown — keeps its position, type and exact value text (ADR-0031), so
/// <c>{}</c>/<c>[]</c> stay as they were and a 15-digit integer is never exponent-encoded. The core
/// referenced by this package predates envelope extras, so the body is rewritten token by token.
/// </summary>
internal static class RedisReleaseBody
{
    private static readonly JsonWriterOptions WriterOptions = new()
    {
        Indented = false,
        Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
    };

    private static readonly JsonDocumentOptions DocumentOptions = new()
    {
        CommentHandling = JsonCommentHandling.Disallow,
        AllowTrailingCommas = false,
    };

    /// <summary>Returns the released body; throws <see cref="BabelQueueException"/> when <paramref name="body"/> is not a JSON object with a numeric <c>attempts</c>.</summary>
    internal static string Build(string body)
    {
        JsonDocument document;
        try
        {
            document = JsonDocument.Parse(body, DocumentOptions);
        }
        catch (JsonException error)
        {
            throw new BabelQueueException("Cannot release a message whose body is not valid JSON.", error);
        }

        using (document)
        {
            var root = document.RootElement;
            if (root.ValueKind != JsonValueKind.Object)
            {
                throw new BabelQueueException("Cannot release a message whose body is not a JSON object.");
            }

            // Duplicate top-level keys: the last occurrence wins (what cjson / System.Text.Json agree on).
            var lastIndex = new Dictionary<string, int>(StringComparer.Ordinal);
            var index = 0;
            foreach (var property in root.EnumerateObject())
            {
                lastIndex[property.Name] = index++;
            }

            if (!lastIndex.ContainsKey("attempts"))
            {
                throw new BabelQueueException("Cannot release a message without a numeric 'attempts'.");
            }

            using var stream = new MemoryStream();
            using (var writer = new Utf8JsonWriter(stream, WriterOptions))
            {
                writer.WriteStartObject();
                index = 0;
                foreach (var property in root.EnumerateObject())
                {
                    var current = index++;
                    if (lastIndex[property.Name] != current)
                    {
                        continue;
                    }

                    if (property.NameEquals("attempts"))
                    {
                        WriteIncremented(writer, property.Value);
                    }
                    else
                    {
                        writer.WritePropertyName(property.Name);
                        property.Value.WriteTo(writer);
                    }
                }

                writer.WriteEndObject();
            }

            return System.Text.Encoding.UTF8.GetString(stream.ToArray());
        }
    }

    private static void WriteIncremented(Utf8JsonWriter writer, JsonElement value)
    {
        if (value.ValueKind != JsonValueKind.Number)
        {
            throw new BabelQueueException("Cannot release a message without a numeric 'attempts'.");
        }

        writer.WritePropertyName("attempts");
        if (value.TryGetInt64(out var whole))
        {
            writer.WriteNumberValue(whole + 1);
        }
        else
        {
            writer.WriteNumberValue(value.GetDouble() + 1);
        }
    }

    /// <summary>The attempt number a delivery counts as: <c>attempts + 1</c> of the (last) <c>attempts</c> key; 1 when it cannot be read.</summary>
    internal static int ReportedAttempts(string body)
    {
        try
        {
            using var document = JsonDocument.Parse(body, DocumentOptions);
            if (document.RootElement.ValueKind != JsonValueKind.Object)
            {
                return 1;
            }

            double? attempts = null;
            foreach (var property in document.RootElement.EnumerateObject())
            {
                if (property.NameEquals("attempts"))
                {
                    attempts = property.Value.ValueKind == JsonValueKind.Number ? property.Value.GetDouble() : null;
                }
            }

            return attempts is { } value ? (int)Math.Min(int.MaxValue - 1, Math.Max(0, value)) + 1 : 1;
        }
        catch (JsonException)
        {
            return 1;
        }
    }
}
