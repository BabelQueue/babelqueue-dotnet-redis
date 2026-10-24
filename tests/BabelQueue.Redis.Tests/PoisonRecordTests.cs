using System;
using System.Text;
using System.Text.Json;
using BabelQueue.Redis;
using Xunit;

namespace BabelQueue.Redis.Tests;

/// <summary>Direct tests of the ADR-0035 poison record writer (splice, raw wrapper, argument validation).</summary>
public sealed class PoisonRecordTests
{
    private const string Block = "{\"reason\":\"poison\"}";

    private static string Text(byte[] record) => Encoding.UTF8.GetString(record);

    [Fact]
    public void SplicesTheDeadLetterBlockIntoAnObjectByteForByte()
    {
        var body = Encoding.UTF8.GetBytes("{\"a\":1e400,\"a\":18446744073709551615}");

        Assert.Equal("{\"a\":1e400,\"a\":18446744073709551615,\"dead_letter\":" + Block + "}", Text(PoisonRecord.Build(body, Block)));
    }

    [Fact]
    public void WrapsANonObjectAsRawText()
    {
        var record = PoisonRecord.Build(Encoding.UTF8.GetBytes("not json"), Block);

        Assert.Equal("{\"raw\":\"not json\",\"dead_letter\":" + Block + "}", Text(record));
    }

    [Fact]
    public void WrapsInvalidUtf8AsPaddedBase64WithoutSubstitution()
    {
        var body = new byte[] { 0x7B, 0xFF, 0xFE };

        var record = Text(PoisonRecord.Build(body, Block));

        Assert.Equal("{\"raw\":\"e//+\",\"raw_encoding\":\"base64\",\"dead_letter\":" + Block + "}", record);
        Assert.DoesNotContain("\uFFFD", record, StringComparison.Ordinal);
    }

    [Fact]
    public void WrapsAnObjectThatAlreadyHasADeadLetterKey()
    {
        var record = Text(PoisonRecord.Build(Encoding.UTF8.GetBytes("{\"dead_letter\":1}"), Block));

        Assert.StartsWith("{\"raw\":\"{\\\"dead_letter\\\":1}\",\"dead_letter\":", record, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("")]
    [InlineData("null")]
    [InlineData("[]")]
    [InlineData("\"text\"")]
    [InlineData("{} {}")]
    [InlineData("{\"a\":1} x")]
    [InlineData("{\"a\":")]
    public void RejectsADeadLetterThatIsNotASingleJsonObject(string deadLetter)
    {
        Assert.Throws<ArgumentException>(() => PoisonRecord.Build(Encoding.UTF8.GetBytes("{\"a\":1}"), deadLetter));
    }

    [Fact]
    public void RejectsANullDeadLetter()
    {
        Assert.Throws<ArgumentNullException>(() => PoisonRecord.Build(Encoding.UTF8.GetBytes("{}"), null!));
    }

    [Fact]
    public void ProducesParseableJsonForEveryShape()
    {
        foreach (var body in new[] { "{}", "  {\"k\": [1, 2]}  ", "[1]", "" })
        {
            using var doc = JsonDocument.Parse(PoisonRecord.Build(Encoding.UTF8.GetBytes(body), Block));
            Assert.Equal("poison", doc.RootElement.GetProperty("dead_letter").GetProperty("reason").GetString());
        }
    }
}
