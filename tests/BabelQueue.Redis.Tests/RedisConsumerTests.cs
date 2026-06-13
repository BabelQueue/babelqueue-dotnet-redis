using BabelQueue;
using BabelQueue.Redis;
using Moq;
using StackExchange.Redis;
using Xunit;

namespace BabelQueue.Redis.Tests;

public sealed class RedisConsumerTests
{
    private const string Queue = "orders";
    private const string Processing = "orders:processing";

    /// <summary>A database that yields the given bodies once each (then drains), tracking LREM acks.</summary>
    private static Mock<IDatabase> MockReserving(params string[] bodies)
    {
        var queue = new Queue<string>(bodies);
        var mock = new Mock<IDatabase>();
        mock.Setup(d => d.ListMoveAsync(Queue, Processing, ListSide.Left, ListSide.Right, It.IsAny<CommandFlags>()))
            .ReturnsAsync(() => queue.Count > 0 ? queue.Dequeue() : RedisValue.Null);
        mock.Setup(d => d.ListRemoveAsync(Processing, It.IsAny<RedisValue>(), It.IsAny<long>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync(1L);
        return mock;
    }

    private static string Envelope(int attempts = 0)
    {
        var env = EnvelopeCodec.Make("urn:babel:orders:created", new Dictionary<string, object?> { ["order_id"] = 7 }, Queue);
        return EnvelopeCodec.Encode(env with { Attempts = attempts });
    }

    [Fact]
    public async Task RoutesValidMessageThenAcks()
    {
        var body = Envelope();
        var mock = MockReserving(body);
        Envelope? seen = null;
        string? rawSeen = null;

        var handlers = new Dictionary<string, BabelHandler>
        {
            ["urn:babel:orders:created"] = (env, raw, _) => { seen = env; rawSeen = raw; return Task.CompletedTask; },
        };
        var processed = await new RedisConsumer(mock.Object, Queue, handlers).PollAsync();

        Assert.Equal(1, processed);
        Assert.Equal("urn:babel:orders:created", seen!.Job);
        Assert.Equal(body, rawSeen);
        mock.Verify(d => d.ListRemoveAsync(Processing, body, 1, It.IsAny<CommandFlags>()), Times.Once);
    }

    [Fact]
    public async Task ReservesViaLeftToRightMove()
    {
        var mock = MockReserving(Envelope());
        var handlers = new Dictionary<string, BabelHandler>
        {
            ["urn:babel:orders:created"] = (_, _, _) => Task.CompletedTask,
        };

        await new RedisConsumer(mock.Object, Queue, handlers).PollAsync();

        // Reliable-queue reserve: LMOVE <queue> <queue>:processing LEFT RIGHT (matches the Go reference).
        mock.Verify(d => d.ListMoveAsync(Queue, Processing, ListSide.Left, ListSide.Right, It.IsAny<CommandFlags>()), Times.AtLeastOnce);
    }

    [Fact]
    public async Task PreservesAttemptsFromBody()
    {
        var mock = MockReserving(Envelope(attempts: 4));
        var attempts = -1;
        var handlers = new Dictionary<string, BabelHandler>
        {
            ["urn:babel:orders:created"] = (env, _, _) => { attempts = env.Attempts; return Task.CompletedTask; },
        };

        await new RedisConsumer(mock.Object, Queue, handlers).PollAsync();

        Assert.Equal(4, attempts);
    }

    [Fact]
    public async Task ThrowingHandlerLeavesReservedAndReportsOnError()
    {
        var mock = MockReserving(Envelope());
        Exception? captured = null;
        var handlers = new Dictionary<string, BabelHandler>
        {
            ["urn:babel:orders:created"] = (_, _, _) => throw new InvalidOperationException("boom"),
        };
        var options = new RedisConsumerOptions { OnError = (e, _, _) => captured = e };

        await new RedisConsumer(mock.Object, Queue, handlers, options).PollAsync();

        Assert.IsType<InvalidOperationException>(captured);
        // Not acked: the element stays on the processing list for a later sweep/retry.
        mock.Verify(d => d.ListRemoveAsync(It.IsAny<RedisKey>(), It.IsAny<RedisValue>(), It.IsAny<long>(), It.IsAny<CommandFlags>()), Times.Never);
    }

    [Fact]
    public async Task NonConformantEnvelopeReportsOnErrorAndLeavesReserved()
    {
        var mock = MockReserving("{\"not\":\"an envelope\"}");
        Exception? captured = null;
        var options = new RedisConsumerOptions { OnError = (e, _, _) => captured = e };

        await new RedisConsumer(mock.Object, Queue, new Dictionary<string, BabelHandler>(), options).PollAsync();

        Assert.IsType<BabelQueueException>(captured);
        mock.Verify(d => d.ListRemoveAsync(It.IsAny<RedisKey>(), It.IsAny<RedisValue>(), It.IsAny<long>(), It.IsAny<CommandFlags>()), Times.Never);
    }

    [Fact]
    public async Task UnknownUrnWithHookAcksAndReportsWithoutHook()
    {
        var withHook = MockReserving(Envelope());
        string? unknown = null;
        var optionsA = new RedisConsumerOptions { OnUnknownUrn = (env, _, _) => { unknown = EnvelopeCodec.Urn(env); return Task.CompletedTask; } };
        await new RedisConsumer(withHook.Object, Queue, new Dictionary<string, BabelHandler>(), optionsA).PollAsync();
        Assert.Equal("urn:babel:orders:created", unknown);
        withHook.Verify(d => d.ListRemoveAsync(Processing, It.IsAny<RedisValue>(), 1, It.IsAny<CommandFlags>()), Times.Once);

        var noHook = MockReserving(Envelope());
        Exception? captured = null;
        var optionsB = new RedisConsumerOptions { OnError = (e, _, _) => captured = e };
        await new RedisConsumer(noHook.Object, Queue, new Dictionary<string, BabelHandler>(), optionsB).PollAsync();
        Assert.IsType<UnknownUrnException>(captured);
        noHook.Verify(d => d.ListRemoveAsync(It.IsAny<RedisKey>(), It.IsAny<RedisValue>(), It.IsAny<long>(), It.IsAny<CommandFlags>()), Times.Never);
    }

    [Fact]
    public async Task PollReservesUpToMaxMessages()
    {
        var mock = MockReserving(Envelope(), Envelope(), Envelope());
        var count = 0;
        var handlers = new Dictionary<string, BabelHandler>
        {
            ["urn:babel:orders:created"] = (_, _, _) => { count++; return Task.CompletedTask; },
        };
        var options = new RedisConsumerOptions { MaxMessages = 2 };

        var processed = await new RedisConsumer(mock.Object, Queue, handlers, options).PollAsync();

        Assert.Equal(2, processed);
        Assert.Equal(2, count);
    }

    [Fact]
    public async Task EmptyPollReturnsZero()
    {
        var mock = MockReserving();
        var processed = await new RedisConsumer(mock.Object, Queue, new Dictionary<string, BabelHandler>()).PollAsync();
        Assert.Equal(0, processed);
    }

    [Fact]
    public async Task CustomProcessingSuffixIsUsed()
    {
        const string customProcessing = "orders:inflight";
        var body = Envelope();
        var queue = new Queue<string>(new[] { body });
        var mock = new Mock<IDatabase>();
        mock.Setup(d => d.ListMoveAsync(Queue, customProcessing, ListSide.Left, ListSide.Right, It.IsAny<CommandFlags>()))
            .ReturnsAsync(() => queue.Count > 0 ? queue.Dequeue() : RedisValue.Null);
        mock.Setup(d => d.ListRemoveAsync(customProcessing, It.IsAny<RedisValue>(), It.IsAny<long>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync(1L);

        var handlers = new Dictionary<string, BabelHandler>
        {
            ["urn:babel:orders:created"] = (_, _, _) => Task.CompletedTask,
        };
        var options = new RedisConsumerOptions { ProcessingSuffix = ":inflight" };
        var consumer = new RedisConsumer(mock.Object, Queue, handlers, options);

        Assert.Equal(customProcessing, consumer.ProcessingKey);
        await consumer.PollAsync();
        mock.Verify(d => d.ListRemoveAsync(customProcessing, body, 1, It.IsAny<CommandFlags>()), Times.Once);
    }

    [Fact]
    public async Task RunStopsWhenCancelled()
    {
        var mock = MockReserving();
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await new RedisConsumer(mock.Object, Queue, new Dictionary<string, BabelHandler>()).RunAsync(cts.Token);

        mock.Verify(d => d.ListMoveAsync(It.IsAny<RedisKey>(), It.IsAny<RedisKey>(), It.IsAny<ListSide>(), It.IsAny<ListSide>(), It.IsAny<CommandFlags>()), Times.Never);
    }

    [Fact]
    public async Task RunProcessesThenStopsOnCancellationAfterIdle()
    {
        // One message available, then empty; cancel via the idle delay so RunAsync exits.
        using var cts = new CancellationTokenSource();
        var queue = new Queue<string>(new[] { Envelope() });
        var mock = new Mock<IDatabase>();
        mock.Setup(d => d.ListMoveAsync(Queue, Processing, ListSide.Left, ListSide.Right, It.IsAny<CommandFlags>()))
            .ReturnsAsync(() => queue.Count > 0 ? queue.Dequeue() : RedisValue.Null);
        mock.Setup(d => d.ListRemoveAsync(Processing, It.IsAny<RedisValue>(), It.IsAny<long>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync(1L);

        var handled = 0;
        var handlers = new Dictionary<string, BabelHandler>
        {
            ["urn:babel:orders:created"] = (_, _, _) => { handled++; return Task.CompletedTask; },
        };
        // Tiny idle delay; cancel as soon as the idle delay is entered.
        var options = new RedisConsumerOptions { IdleDelay = TimeSpan.FromMilliseconds(10) };

        var run = new RedisConsumer(mock.Object, Queue, handlers, options).RunAsync(cts.Token);
        await cts.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run);

        Assert.Equal(1, handled);
    }
}
