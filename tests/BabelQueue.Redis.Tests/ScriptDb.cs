using BabelQueue.Redis;
using Moq;
using StackExchange.Redis;

namespace BabelQueue.Redis.Tests;

/// <summary>
/// A scripted <see cref="IDatabase"/> double: every <c>EVAL</c> is recorded and answered by a
/// per-script responder (the Lua itself is exercised by the live-Redis tests, not here).
/// </summary>
internal sealed class ScriptDb
{
    internal sealed record Call(string Script, string[] Keys, RedisValue[] Values);

    private readonly Dictionary<string, Func<Call, RedisResult>> _responders = new();

    internal ScriptDb()
    {
        Mock = new Mock<IDatabase>();
        Mock.Setup(d => d.ScriptEvaluateAsync(It.IsAny<string>(), It.IsAny<RedisKey[]?>(), It.IsAny<RedisValue[]?>(), It.IsAny<CommandFlags>()))
            .Returns((string script, RedisKey[]? keys, RedisValue[]? values, CommandFlags _) =>
            {
                var call = new Call(script, (keys ?? Array.Empty<RedisKey>()).Select(k => (string)k!).ToArray(), values ?? Array.Empty<RedisValue>());
                Calls.Add(call);
                return Task.FromResult(_responders.TryGetValue(script, out var responder) ? responder(call) : Integer(0));
            });
        Mock.Setup(d => d.ListRightPushAsync(It.IsAny<RedisKey>(), It.IsAny<RedisValue>(), It.IsAny<When>(), It.IsAny<CommandFlags>()))
            .Callback<RedisKey, RedisValue, When, CommandFlags>((k, v, _, _) => Pushed.Add(((string)k!, v)))
            .ReturnsAsync(1L);
        Mock.Setup(d => d.SortedSetAddAsync(It.IsAny<RedisKey>(), It.IsAny<RedisValue>(), It.IsAny<double>(), It.IsAny<CommandFlags>()))
            .Callback<RedisKey, RedisValue, double, CommandFlags>((k, v, s, _) => SortedAdds.Add(((string)k!, v, s)))
            .ReturnsAsync(true);
        Mock.Setup(d => d.SortedSetAddAsync(It.IsAny<RedisKey>(), It.IsAny<RedisValue>(), It.IsAny<double>(), It.IsAny<SortedSetWhen>(), It.IsAny<CommandFlags>()))
            .Callback<RedisKey, RedisValue, double, SortedSetWhen, CommandFlags>((k, v, s, _, _) => SortedAdds.Add(((string)k!, v, s)))
            .ReturnsAsync(true);
        Mock.Setup(d => d.SortedSetRemoveAsync(It.IsAny<RedisKey>(), It.IsAny<RedisValue>(), It.IsAny<CommandFlags>()))
            .Callback<RedisKey, RedisValue, CommandFlags>((k, v, _) => SortedRemoves.Add(((string)k!, v)))
            .ReturnsAsync(() => SortedRemoveResult);
    }

    internal Mock<IDatabase> Mock { get; }

    internal IDatabase Object => Mock.Object;

    internal List<Call> Calls { get; } = new();

    internal List<(string Key, RedisValue Value)> Pushed { get; } = new();

    internal List<(string Key, RedisValue Member, double Score)> SortedAdds { get; } = new();

    internal List<(string Key, RedisValue Member)> SortedRemoves { get; } = new();

    internal bool SortedRemoveResult { get; set; } = true;

    internal static RedisResult Integer(long value) => RedisResult.Create((RedisValue)value);

    internal static RedisResult Nil() => RedisResult.Create(RedisValue.Null);

    internal static RedisResult Text(string value) => RedisResult.Create((RedisValue)value);

    /// <summary>A bulk reply carrying the exact <paramref name="value"/> bytes (which need not be valid UTF-8).</summary>
    internal static RedisResult Bytes(byte[] value) => RedisResult.Create((RedisValue)value);

    internal static RedisResult BytePair(byte[] first, byte[] second) => RedisResult.Create(new RedisValue[] { first, second });

    internal static RedisResult Pair(string first, string second) => RedisResult.Create(new RedisValue[] { first, second });

    /// <summary>Answers every evaluation of <paramref name="script"/> with <paramref name="responder"/>.</summary>
    internal ScriptDb On(string script, Func<Call, RedisResult> responder)
    {
        _responders[script] = responder;
        return this;
    }

    /// <summary>Answers evaluations of <paramref name="script"/> from a queue of results, then <see cref="Nil"/>.</summary>
    internal ScriptDb Queue(string script, params RedisResult[] results)
    {
        var queue = new Queue<RedisResult>(results);
        return On(script, _ => queue.Count > 0 ? queue.Dequeue() : Nil());
    }

    internal IReadOnlyList<Call> Of(string script) => Calls.Where(c => c.Script == script).ToList();
}
