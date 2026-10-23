using StackExchange.Redis;
using Xunit;

namespace BabelQueue.Redis.Tests;

/// <summary>
/// Access to a live Redis for the integration and behavioural-conformance tests (CI runs a
/// <c>redis:7</c> service). The address comes from <c>REDIS_URL</c> (default
/// <c>redis://localhost:6379/0</c>) and is probed once per process.
/// <para>
/// Runner contract (conformance <c>redis_layout</c>): when <c>BQ_REQUIRE_REDIS</c> is set and Redis
/// cannot be reached the live tests FAIL; when it is unset they are skipped and the reason is logged.
/// </para>
/// </summary>
internal static class RedisLive
{
    internal const string DefaultUrl = "redis://localhost:6379/0";

    private static readonly Lazy<(IConnectionMultiplexer? Mux, string Reason)> Connection = new(Connect);

    /// <summary>True when <c>BQ_REQUIRE_REDIS</c> is set to any non-empty value.</summary>
    internal static bool Required => !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("BQ_REQUIRE_REDIS"));

    internal static string Url => Environment.GetEnvironmentVariable("REDIS_URL") is { Length: > 0 } url ? url : DefaultUrl;

    internal static bool Available => Connection.Value.Mux is not null;

    /// <summary>Why live tests cannot run, or null when Redis is reachable.</summary>
    internal static string? Unavailable => Connection.Value.Mux is null ? Connection.Value.Reason : null;

    /// <summary>
    /// The skip reason for live tests: null when they must run (Redis reachable, or
    /// <c>BQ_REQUIRE_REDIS</c> set so an unreachable Redis fails instead of skipping).
    /// </summary>
    internal static string? SkipReason
    {
        get
        {
            if (Available || Required)
            {
                return null;
            }

            var reason = $"live Redis not reachable at {Url} ({Connection.Value.Reason}); BQ_REQUIRE_REDIS is unset, skipping";
            Console.Error.WriteLine("[babelqueue-redis] " + reason);
            return reason;
        }
    }

    /// <summary>The connected database; throws (failing the test) when Redis is unreachable.</summary>
    internal static IDatabase Database()
    {
        var (mux, reason) = Connection.Value;
        if (mux is null)
        {
            throw new InvalidOperationException($"Redis is not reachable at {Url} ({reason}); BQ_REQUIRE_REDIS={(Required ? "set" : "unset")}.");
        }

        return mux.GetDatabase();
    }

    private static (IConnectionMultiplexer?, string) Connect()
    {
        try
        {
            var uri = new Uri(Url);
            var options = new ConfigurationOptions
            {
                AbortOnConnectFail = false,
                ConnectTimeout = 2000,
                SyncTimeout = 5000,
                AsyncTimeout = 5000,
            };
            options.EndPoints.Add(uri.Host, uri.Port > 0 ? uri.Port : 6379);
            if (uri.UserInfo.Length > 0)
            {
                var parts = uri.UserInfo.Split(':', 2);
                if (parts.Length == 2)
                {
                    options.User = Uri.UnescapeDataString(parts[0]);
                    options.Password = Uri.UnescapeDataString(parts[1]);
                }
                else
                {
                    options.Password = Uri.UnescapeDataString(parts[0]);
                }
            }

            var path = uri.AbsolutePath.Trim('/');
            if (path.Length > 0 && int.TryParse(path, out var db))
            {
                options.DefaultDatabase = db;
            }

            var mux = ConnectionMultiplexer.Connect(options);
            if (!mux.IsConnected)
            {
                mux.Dispose();
                return (null, "connection refused or timed out");
            }

            mux.GetDatabase().Ping();
            AppDomain.CurrentDomain.ProcessExit += (_, _) => mux.Dispose();
            return (mux, string.Empty);
        }
        catch (Exception e)
        {
            return (null, e.GetType().Name + ": " + e.Message);
        }
    }
}

/// <summary>A <c>[Fact]</c> that needs a live Redis (skipped when unreachable unless <c>BQ_REQUIRE_REDIS</c> is set).</summary>
public sealed class LiveFactAttribute : FactAttribute
{
    public LiveFactAttribute()
    {
        Skip = RedisLive.SkipReason;
    }
}

/// <summary>A <c>[Theory]</c> that needs a live Redis (skipped when unreachable unless <c>BQ_REQUIRE_REDIS</c> is set).</summary>
public sealed class LiveTheoryAttribute : TheoryAttribute
{
    public LiveTheoryAttribute()
    {
        Skip = RedisLive.SkipReason;
    }
}
