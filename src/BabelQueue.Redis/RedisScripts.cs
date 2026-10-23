namespace BabelQueue.Redis;

/// <summary>
/// The Lua scripts behind the Redis bindings (broker-bindings §1.1, §1.6, §1.7; ADR-0033). Every
/// key the script touches is passed in <c>KEYS</c> so the scripts are Redis-Cluster safe (all keys
/// of one queue must share a hash tag, ADR-0033 §6).
/// </summary>
internal static class RedisScripts
{
    /// <summary>Normal push (zset layout): <c>RPUSH q</c> + <c>RPUSH q:notify 1</c>. KEYS = [ready, notify]; ARGV = [body].</summary>
    internal const string ZsetPush = @"
redis.call('rpush', KEYS[1], ARGV[1])
redis.call('rpush', KEYS[2], 1)
return 1";

    /// <summary>
    /// Migrate due members of a sorted set (<c>:delayed</c> or <c>:reserved</c>) to the ready list,
    /// moving each body byte for byte, in chunks of 100, with one notify token per item.
    /// KEYS = [source zset, ready, notify]; ARGV = [floor(now), batch (&lt;= 0 = no limit)]. Returns the count.
    /// </summary>
    internal const string ZsetMigrate = @"
local val
if tonumber(ARGV[2]) > 0 then
  val = redis.call('zrangebyscore', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
else
  val = redis.call('zrangebyscore', KEYS[1], '-inf', ARGV[1])
end
if #val > 0 then
  redis.call('zremrangebyrank', KEYS[1], 0, #val - 1)
  for i = 1, #val, 100 do
    redis.call('rpush', KEYS[2], unpack(val, i, math.min(i + 99, #val)))
  end
  for i = 1, #val do
    redis.call('rpush', KEYS[3], 1)
  end
end
return #val";

    /// <summary>
    /// Laravel pop with a hardened (pcall) decode (A1). The script must never fail after the LPOP.
    /// KEYS = [ready, reserved, notify]; ARGV = [deadline score]. Returns nil (empty) or {job, reservedMember};
    /// reservedMember == job means the element is poison.
    /// </summary>
    internal const string ZsetReserve = @"
local job = redis.call('lpop', KEYS[1])
if job == false then
  return false
end
local reserved = job
local ok, t = pcall(cjson.decode, job)
if ok and type(t) == 'table' and type(t['attempts']) == 'number' then
  t['attempts'] = t['attempts'] + 1
  local ok2, enc = pcall(cjson.encode, t)
  if ok2 and type(enc) == 'string' then
    reserved = enc
  end
end
redis.call('zadd', KEYS[2], ARGV[1], reserved)
redis.call('lpop', KEYS[3])
return {job, reserved}";

    /// <summary>
    /// Atomic release (A2): drop the reservation and park the SDK-codec body on <c>:delayed</c>.
    /// Gated on the reservation still being held. KEYS = [reserved, delayed]; ARGV = [reserved member, score, body].
    /// </summary>
    internal const string ZsetRelease = @"
if redis.call('zrem', KEYS[1], ARGV[1]) == 1 then
  redis.call('zadd', KEYS[2], ARGV[2], ARGV[3])
  return 1
end
return 0";

    /// <summary>
    /// Dead-letter a reserved element (gated on the reservation still being held).
    /// KEYS = [reserved, dlq, dlq notify]; ARGV = [reserved member, body].
    /// </summary>
    internal const string ZsetDeadLetter = @"
if redis.call('zrem', KEYS[1], ARGV[1]) == 1 then
  redis.call('rpush', KEYS[2], ARGV[2])
  redis.call('rpush', KEYS[3], 1)
  return 1
end
return 0";

    /// <summary>
    /// Legacy reserve: <c>LMOVE ready processing LEFT RIGHT</c> + a deadline member <c>&lt;token&gt;:&lt;raw&gt;</c>.
    /// KEYS = [ready, processing, deadlines]; ARGV = [deadline score, token]. Returns nil or the raw body.
    /// </summary>
    internal const string LegacyReserve = @"
local raw = redis.call('lmove', KEYS[1], KEYS[2], 'LEFT', 'RIGHT')
if not raw then
  return false
end
redis.call('zadd', KEYS[3], ARGV[1], ARGV[2] .. ':' .. raw)
return raw";

    /// <summary>
    /// Legacy ack gate. KEYS = [processing, deadlines]; ARGV = [token, raw]. Returns 1 when this
    /// reservation was still held (and the element was removed), 0 when it had expired.
    /// </summary>
    internal const string LegacyAck = @"
if redis.call('zrem', KEYS[2], ARGV[1] .. ':' .. ARGV[2]) == 1 then
  redis.call('lrem', KEYS[1], 1, ARGV[2])
  return 1
end
return 0";

    /// <summary>Legacy release gate: like <see cref="LegacyAck"/>, then push the body back to ready. KEYS = [processing, deadlines, ready].</summary>
    internal const string LegacyRelease = @"
if redis.call('zrem', KEYS[2], ARGV[1] .. ':' .. ARGV[2]) == 1 then
  redis.call('lrem', KEYS[1], 1, ARGV[2])
  redis.call('rpush', KEYS[3], ARGV[2])
  return 1
end
return 0";

    /// <summary>Legacy dead-letter gate: like <see cref="LegacyAck"/>, then <c>RPUSH</c> the body to the DLQ. KEYS = [processing, deadlines, dlq]; ARGV = [token, raw, body].</summary>
    internal const string LegacyDeadLetter = @"
if redis.call('zrem', KEYS[2], ARGV[1] .. ':' .. ARGV[2]) == 1 then
  redis.call('lrem', KEYS[1], 1, ARGV[2])
  redis.call('rpush', KEYS[3], ARGV[3])
  return 1
end
return 0";

    /// <summary>
    /// Legacy sweep, run before EVERY reserve: expired deadline members go back to the ready list
    /// byte for byte (no attempts change). A malformed member (shorter than 34 bytes, or byte 33 is
    /// not ':') and an orphan (the element is no longer on processing) are only removed.
    /// KEYS = [ready, processing, deadlines]; ARGV = [floor(now), batch (&lt;= 0 = no limit)]. Returns the member count handled.
    /// </summary>
    internal const string LegacySweep = @"
local members
if tonumber(ARGV[2]) > 0 then
  members = redis.call('zrangebyscore', KEYS[3], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
else
  members = redis.call('zrangebyscore', KEYS[3], '-inf', ARGV[1])
end
for _, m in ipairs(members) do
  if #m >= 34 and string.sub(m, 33, 33) == ':' then
    local raw = string.sub(m, 34)
    if redis.call('lrem', KEYS[2], 1, raw) == 1 then
      redis.call('rpush', KEYS[1], raw)
    end
  end
  redis.call('zrem', KEYS[3], m)
end
return #members";
}
