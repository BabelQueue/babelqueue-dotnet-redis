using System.Text.Json;
using BabelQueue;
using BabelQueue.Redis;
using Moq;
using StackExchange.Redis;
using Xunit;

namespace BabelQueue.Redis.Tests;

public sealed class RedisPublisherTests
{
    private const string Queue = "orders";

    private static (Mock<IDatabase> Mock, Func<RedisValue> Pushed) MockDatabase()
    {
        RedisValue pushed = RedisValue.Null;
        var mock = new Mock<IDatabase>();
        mock.Setup(d => d.ListRightPushAsync(
                Queue, It.IsAny<RedisValue>(), It.IsAny<When>(), It.IsAny<CommandFlags>()))
            .Callback<RedisKey, RedisValue, When, CommandFlags>((_, value, _, _) => pushed = value)
            .ReturnsAsync(1L);
        return (mock, () => pushed);
    }

    [Fact]
    public async Task PublishStoresEnvelopeVerbatim()
    {
        var (mock, pushed) = MockDatabase();
        var data = new Dictionary<string, object?> { ["order_id"] = 1042 };

        var id = await new RedisPublisher(mock.Object, Queue).PublishAsync("urn:babel:orders:created", data);

        // The pushed element must equal exactly the codec's encoding — no wrapping, no added fields.
        var body = (string)pushed()!;
        using var doc = JsonDocument.Parse(body);
        Assert.Equal("urn:babel:orders:created", doc.RootElement.GetProperty("job").GetString());
        Assert.Equal(id, doc.RootElement.GetProperty("meta").GetProperty("id").GetString());
        Assert.Equal(0, doc.RootElement.GetProperty("attempts").GetInt32());

        // Byte-for-byte identity with re-encoding the decoded envelope (no extra keys around it).
        Assert.Equal(EnvelopeCodec.Encode(EnvelopeCodec.Decode(body)), body);

        mock.Verify(d => d.ListRightPushAsync(Queue, It.IsAny<RedisValue>(), It.IsAny<When>(), It.IsAny<CommandFlags>()), Times.Once);
    }

    [Fact]
    public async Task PublishStampsQueueAndDotnetLang()
    {
        var (mock, pushed) = MockDatabase();

        await new RedisPublisher(mock.Object, Queue).PublishAsync("urn:babel:orders:created");

        using var doc = JsonDocument.Parse((string)pushed()!);
        var meta = doc.RootElement.GetProperty("meta");
        Assert.Equal("orders", meta.GetProperty("queue").GetString());
        Assert.Equal("dotnet", meta.GetProperty("lang").GetString());
        Assert.Equal(1, meta.GetProperty("schema_version").GetInt32());
    }

    [Fact]
    public async Task PublishContinuesTraceId()
    {
        var (mock, pushed) = MockDatabase();
        const string trace = "11111111-2222-3333-4444-555555555555";

        await new RedisPublisher(mock.Object, Queue).PublishAsync("urn:x:y", traceId: trace);

        using var doc = JsonDocument.Parse((string)pushed()!);
        Assert.Equal(trace, doc.RootElement.GetProperty("trace_id").GetString());
    }

    [Fact]
    public async Task PublishReturnsGeneratedMessageId()
    {
        var (mock, _) = MockDatabase();

        var id = await new RedisPublisher(mock.Object, Queue).PublishAsync("urn:x:y");

        Assert.False(string.IsNullOrEmpty(id));
    }

    [Fact]
    public async Task CancelledTokenThrowsBeforePush()
    {
        var (mock, _) = MockDatabase();
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await Assert.ThrowsAsync<OperationCanceledException>(
            () => new RedisPublisher(mock.Object, Queue).PublishAsync("urn:x:y", cancellationToken: cts.Token));

        mock.Verify(d => d.ListRightPushAsync(It.IsAny<RedisKey>(), It.IsAny<RedisValue>(), It.IsAny<When>(), It.IsAny<CommandFlags>()), Times.Never);
    }

    [Fact]
    public async Task ClientErrorPropagates()
    {
        var mock = new Mock<IDatabase>();
        mock.Setup(d => d.ListRightPushAsync(It.IsAny<RedisKey>(), It.IsAny<RedisValue>(), It.IsAny<When>(), It.IsAny<CommandFlags>()))
            .ThrowsAsync(new InvalidOperationException("redis down"));

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => new RedisPublisher(mock.Object, Queue).PublishAsync("urn:x:y"));
        Assert.Equal("redis down", ex.Message);
    }

    [Fact]
    public async Task BlankUrnThrowsBabelQueueException()
    {
        var (mock, _) = MockDatabase();

        await Assert.ThrowsAsync<BabelQueueException>(
            () => new RedisPublisher(mock.Object, Queue).PublishAsync(""));
    }

    [Fact]
    public void NullArgumentsAreRejected()
    {
        Assert.Throws<ArgumentNullException>(() => new RedisPublisher(null!, Queue));
        Assert.Throws<ArgumentException>(() => new RedisPublisher(new Mock<IDatabase>().Object, ""));
    }
}
