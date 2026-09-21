"""Redis 持久层：屏障元数据、原子报到（Lua）、冻结快照、报到审计事件与事件通知。

每个屏障使用如下键（``bid`` 为屏障 id）：

``barrier:{bid}``           hash  元数据（threshold、deadline、ttl_ms 等）
``barrier:{bid}:roster``    set   创建时声明的参与者名单
``barrier:{bid}:checked``   set   所有有效报到（含达标后的迟到报到）
``barrier:{bid}:frozen``    hash  达标冻结快照，仅在达标后存在
                            （quorum/frozen_at/group_counts）
``barrier:{bid}:cancel``    hash  主动取消终态，仅在取消后存在（reason/cancelled_at）
``barrier:{bid}:events``    list  报到审计事件（JSON，seq 从 1 递增，只追加）
``barrier:{bid}:gmap``      hash  参与者 -> 分组名（仅启用分组配额时存在）
``barrier:{bid}:gcheck``    hash  各分组去重后的报到人数（仅启用分组配额时存在）

``frozen`` 与 ``cancel`` 互斥：达标报到与主动取消由各自的 Lua 脚本原子竞争，
Redis 单线程串行执行脚本，先发生者成为唯一终态，任何路径都不得改写。
运行中延期（PATCH deadline）由第三段 Lua 脚本原子完成：同样先校验
frozen / cancel / 截止三种终态，终态存在时绝不复活；只在 waiting 且
新截止时间严格晚于当前截止时间时写入新 deadline 并同步刷新全部键的 TTL。

频道 ``barrier:{bid}:events`` 在达标冻结后发布 ``reached`` 消息、
在主动取消后发布 ``cancelled`` 消息、在成功延期后发布 ``deadline_extended``
消息（唤醒等待者立即回读最新截止时间，继续等待）。
"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass
from typing import Any

from redis.asyncio import Redis

# 报到脚本。返回数组：{状态, 是否新加入(0/1), quorum JSON, frozen_at 字符串}
#
# 状态取值：
#   unknown             屏障不存在
#   not_participant     报到者不在名单内
#   late                屏障达标后报到，快照不变
#   cancelled           屏障已被主动取消（与取消脚本原子竞争的败者）
#   expired             已过截止时间
#   reached             本次报到使屏障达标，已原子写入冻结快照
#   checked_in          报到成功，屏障仍在等待
#   already_checked_in  幂等重复报到（未达标前）
#
# 启用分组配额时，SADD 成功（去重后的新报到）才会同步 HINCRBY 对应分组计数；
# 重复 / 迟到报到都不重复累计。达标条件为「总报到数 >= threshold 且
# 每个分组计数 >= 该组配额」，满足时在同一脚本内冻结 quorum 与分组计数快照。
#
# 四种成功状态（reached/checked_in/already_checked_in/late）都会在同一脚本内
# 向 events 列表追加一条审计事件（seq 递增）；错误路径不产生任何事件。
CHECKIN_SCRIPT = """
local meta_key    = KEYS[1]
local roster_key  = KEYS[2]
local checked_key = KEYS[3]
local frozen_key  = KEYS[4]
local events_key  = KEYS[5]
local cancel_key  = KEYS[6]
local gmap_key    = KEYS[7]
local gcheck_key  = KEYS[8]
local pid         = ARGV[1]
local now         = tonumber(ARGV[2])

local function refresh_ttl()
    local ttl_ms = tonumber(redis.call('HGET', meta_key, 'ttl_ms'))
    if ttl_ms then
        redis.call('PEXPIRE', meta_key, ttl_ms)
        redis.call('PEXPIRE', roster_key, ttl_ms)
        redis.call('PEXPIRE', checked_key, ttl_ms)
        redis.call('PEXPIRE', frozen_key, ttl_ms)
        redis.call('PEXPIRE', events_key, ttl_ms)
        redis.call('PEXPIRE', cancel_key, ttl_ms)
        redis.call('PEXPIRE', gmap_key, ttl_ms)
        redis.call('PEXPIRE', gcheck_key, ttl_ms)
    end
end

-- 审计事件：与状态变更处于同一原子脚本，seq 自 1 递增（下标 = seq - 1）。
local function record_event(result)
    local seq = redis.call('LLEN', events_key) + 1
    redis.call('RPUSH', events_key, cjson.encode({
        seq = seq,
        participant_id = pid,
        result = result,
        ts = now
    }))
end

-- 分组配额是否全部满足：gcheck 中的每个分组计数均不低于 meta 里登记的配额。
-- 配额表由创建校验保证不大于组人数，因此只有真正报到才能把计数推过配额。
local function group_quotas_satisfied()
    local raw = redis.call('HGET', meta_key, 'group_thresholds')
    if not raw or raw == '' then
        return true
    end
    local thresholds = cjson.decode(raw)
    for group, need in pairs(thresholds) do
        if tonumber(redis.call('HGET', gcheck_key, group) or '0') < tonumber(need) then
            return false
        end
    end
    return true
end

-- 冻结快照：quorum 成员名单 + 冻结瞬间的分组计数，一次原子写定，永不改写。
local function freeze_quorum(now_ts)
    local members = redis.call('SMEMBERS', checked_key)
    table.sort(members)
    local counts = redis.call('HGETALL', gcheck_key)
    local counts_map = {}
    for i = 1, #counts, 2 do
        counts_map[counts[i]] = tonumber(counts[i + 1])
    end
    redis.call('HSET', frozen_key,
        'quorum', cjson.encode(members),
        'frozen_at', tostring(now_ts),
        'group_counts', cjson.encode(counts_map))
    return cjson.encode(members)
end

if redis.call('HEXISTS', meta_key, 'threshold') == 0 then
    return {'unknown', '0', '', '0'}
end

-- 名单校验优先于一切状态：非名单成员永远无法报到，也不能获得冻结快照。
if redis.call('SISMEMBER', roster_key, pid) == 0 then
    return {'not_participant', '0', '', '0'}
end

-- 达标冻结之后：冻结键已存在，任何后续报到都不得改写快照。
local quorum = redis.call('HGET', frozen_key, 'quorum')
if quorum then
    -- 仅记录迟到事实，总数 / 分组计数 / quorum / frozen_at 均保持不变。
    redis.call('SADD', checked_key, pid)
    record_event('late')
    refresh_ttl()
    return {'late', '0', quorum, redis.call('HGET', frozen_key, 'frozen_at')}
end

-- 主动取消之后：取消键已存在，报到不得再写入任何状态（连 checked 也不追加）。
-- cancelled 与 reached 同为终态：若能走到这里说明取消脚本先于本脚本提交。
if redis.call('HEXISTS', cancel_key, 'cancelled_at') == 1 then
    return {'cancelled', '0', '', '0'}
end

local deadline = tonumber(redis.call('HGET', meta_key, 'deadline'))
if now >= deadline then
    return {'expired', '0', '', '0'}
end

-- 去重后的唯一报到才累计总数与分组计数；重复报到（newly_added == 0）
-- 只追加审计事件，任何计数都不变。
local newly_added = redis.call('SADD', checked_key, pid)
if newly_added == 1 then
    local group = redis.call('HGET', gmap_key, pid)
    if group then
        redis.call('HINCRBY', gcheck_key, group, 1)
    end
end
local count = redis.call('SCARD', checked_key)
local threshold = tonumber(redis.call('HGET', meta_key, 'threshold'))

-- 首次同时满足「总数达标 + 各组配额达标」：在同一脚本内冻结成员名单与
-- 分组计数，杜绝并发覆盖（跨组并发报到也只有一个请求进入此分支）。
if count >= threshold and group_quotas_satisfied() then
    local payload = freeze_quorum(now)
    record_event('reached')
    refresh_ttl()
    return {'reached', tostring(newly_added), payload, tostring(now)}
end

if newly_added == 1 then
    record_event('checked_in')
    refresh_ttl()
    return {'checked_in', '0', '', '0'}
end
record_event('already_checked_in')
refresh_ttl()
return {'already_checked_in', '0', '', '0'}
"""


# 取消脚本。返回数组：
#   {'unknown'}                          屏障不存在
#   {'reached', quorum, frozen_at}       已达标，终态不可改写，取消被拒绝
#   {'expired'}                          已过截止时间，取消被拒绝
#   {'cancelled', reason, cancelled_at}  本次请求完成取消（唯一一次写入）
#   {'already_cancelled', reason, ts}    重复取消，幂等返回既有取消结果
#
# 与报到脚本的竞争完全由 Redis 单线程串行化保证：本脚本检查 frozen/cancel 与
# 写入 cancel 键之间不可能插入达标报到；反之报到脚本也会先看到 cancel 键。
CANCEL_SCRIPT = """
local meta_key   = KEYS[1]
local frozen_key = KEYS[4]
local cancel_key = KEYS[6]
local reason     = ARGV[1]
local now        = tonumber(ARGV[2])

if redis.call('HEXISTS', meta_key, 'threshold') == 0 then
    return {'unknown'}
end

-- 既有终态一律不得改写：已取消则幂等返回原结果；已达标则拒绝。
local cancelled_at = redis.call('HGET', cancel_key, 'cancelled_at')
if cancelled_at then
    return {'already_cancelled', redis.call('HGET', cancel_key, 'reason'), cancelled_at}
end

local quorum = redis.call('HGET', frozen_key, 'quorum')
if quorum then
    return {'reached', quorum, redis.call('HGET', frozen_key, 'frozen_at')}
end

-- 已截止也是终态（expired），取消不得覆盖。
local deadline = tonumber(redis.call('HGET', meta_key, 'deadline'))
if now >= deadline then
    return {'expired'}
end

local now_str = tostring(now)
redis.call('HSET', cancel_key, 'reason', reason, 'cancelled_at', now_str)

local ttl_ms = tonumber(redis.call('HGET', meta_key, 'ttl_ms'))
if ttl_ms then
    redis.call('PEXPIRE', cancel_key, ttl_ms)
end
return {'cancelled', reason, now_str}
"""


# 延期脚本。返回数组：
#   {'unknown'}                                      屏障不存在
#   {'reached'}                                      已达标冻结，终态不可延期
#   {'cancelled'}                                    已主动取消，终态不可延期
#   {'expired'}                                      已过截止时间，终态不可复活
#   {'conflict', current_deadline}                   新时间不晚于当前截止时间
#   {'extended', new_deadline, current_before}       本次原子完成延期
#
# 与报到 / 取消脚本的竞争同样由 Redis 单线程串行化保证：检查 frozen / cancel /
# 截止状态与写入新 deadline 处于同一脚本内，之间不可能插入达标或取消；
# 多个并发延期按 Redis 实际执行顺序逐个比较，只接受严格更晚的时间。
# 延期成功后在同一脚本内按新 deadline 重算 TTL 并刷新全部相关键的存活期，
# 使屏障各键仍然同生共死、随新截止点之后的缓冲期一起被回收。
EXTEND_SCRIPT = """
local meta_key    = KEYS[1]
local roster_key  = KEYS[2]
local checked_key = KEYS[3]
local frozen_key  = KEYS[4]
local events_key  = KEYS[5]
local cancel_key  = KEYS[6]
local gmap_key    = KEYS[7]
local gcheck_key  = KEYS[8]
local new_deadline = tonumber(ARGV[1])
local now          = tonumber(ARGV[2])
local buffer_ms    = tonumber(ARGV[3])

if redis.call('HEXISTS', meta_key, 'threshold') == 0 then
    return {'unknown'}
end

-- 终态一律不得复活：已达标 / 已取消 / 已截止都拒绝，且不触碰任何键。
if redis.call('HGET', frozen_key, 'quorum') then
    return {'reached'}
end
if redis.call('HEXISTS', cancel_key, 'cancelled_at') == 1 then
    return {'cancelled'}
end

local current = tonumber(redis.call('HGET', meta_key, 'deadline'))
if now >= current then
    return {'expired'}
end

-- 只允许延期到严格更晚的时刻；相同或更早一律冲突（409）。
-- 并发延期由此以 Redis 实际生效顺序为准：每个请求只与“当前生效值”比较。
if new_deadline <= current then
    return {'conflict', tostring(current)}
end

local new_deadline_str = string.format('%.3f', new_deadline)
redis.call('HSET', meta_key, 'deadline', new_deadline_str)

-- 以新截止时间重算存活期并刷新全部相关键：分组键在未启用分组时不存在，
-- PEXPIRE 对不存在的键无副作用，与报到脚本的 refresh_ttl 行为一致。
local ttl_ms = math.floor((new_deadline - now) * 1000 + buffer_ms)
redis.call('HSET', meta_key, 'ttl_ms', ttl_ms)
redis.call('PEXPIRE', meta_key, ttl_ms)
redis.call('PEXPIRE', roster_key, ttl_ms)
redis.call('PEXPIRE', checked_key, ttl_ms)
redis.call('PEXPIRE', frozen_key, ttl_ms)
redis.call('PEXPIRE', events_key, ttl_ms)
redis.call('PEXPIRE', cancel_key, ttl_ms)
redis.call('PEXPIRE', gmap_key, ttl_ms)
redis.call('PEXPIRE', gcheck_key, ttl_ms)

return {'extended', new_deadline_str, tostring(current)}
"""


class BarrierNotFound(Exception):
    """屏障不存在或已过期清理。"""


class NotAParticipant(Exception):
    """报到者不在屏障参与者名单内。"""


class BarrierExpired(Exception):
    """屏障已过截止时间且未达标。"""


class BarrierReached(Exception):
    """屏障已达标冻结，终态不可改写（取消被拒绝）。"""


class BarrierCancelled(Exception):
    """屏障已被主动取消，后续报到被拒绝。"""


class BarrierDeadlineConflict(Exception):
    """延期冲突：屏障处于终态（不可复活），或新截止时间不晚于当前值。

    ``reason`` 为 Lua 脚本返回的冲突状态：
    reached / cancelled / expired / conflict。
    """

    def __init__(self, barrier_id: str, reason: str, current_deadline: float | None = None):
        super().__init__(barrier_id, reason)
        self.reason = reason
        self.current_deadline = current_deadline


@dataclass(frozen=True)
class ExtendResult:
    # 成功延期后的最新截止时间（Unix 秒）
    deadline: float
    # 延期前的截止时间（Unix 秒）
    previous_deadline: float


@dataclass(frozen=True)
class BarrierMeta:
    barrier_id: str
    participants: list[str]
    threshold: int
    deadline: float  # Unix 秒
    created_at: float  # Unix 秒
    ttl_ms: int
    creator_id: str | None
    # 参与者 -> 分组名；空字典 / None 表示未启用分组配额
    participant_groups: dict[str, str]
    # 分组名 -> 配额；空字典表示未启用分组配额
    group_thresholds: dict[str, int]

    @property
    def groups_enabled(self) -> bool:
        return bool(self.group_thresholds)


@dataclass(frozen=True)
class CheckinResult:
    status: str
    newly_added: bool
    quorum: list[str] | None
    frozen_at: float | None

    @property
    def triggered(self) -> bool:
        return self.status == "reached"

    @property
    def reached(self) -> bool:
        """屏障是否处于已达标状态（含本次触发与达标后报到）。"""
        return self.status in ("reached", "late")


@dataclass(frozen=True)
class CancelResult:
    # cancelled（本次完成取消）| already_cancelled（重复取消，幂等）
    status: str
    reason: str
    cancelled_at: float

    @property
    def idempotent(self) -> bool:
        return self.status == "already_cancelled"


class BarrierStore:
    def __init__(self, redis: Redis) -> None:
        self.redis = redis
        self._checkin = redis.register_script(CHECKIN_SCRIPT)
        self._cancel = redis.register_script(CANCEL_SCRIPT)
        self._extend = redis.register_script(EXTEND_SCRIPT)

    @staticmethod
    def channel(barrier_id: str) -> str:
        return f"barrier:{barrier_id}:events"

    def _keys(self, barrier_id: str) -> tuple[str, ...]:
        return (
            f"barrier:{barrier_id}",
            f"barrier:{barrier_id}:roster",
            f"barrier:{barrier_id}:checked",
            f"barrier:{barrier_id}:frozen",
            f"barrier:{barrier_id}:events",
            f"barrier:{barrier_id}:cancel",
            f"barrier:{barrier_id}:gmap",
            f"barrier:{barrier_id}:gcheck",
        )

    async def create(
        self,
        participants: list[str],
        threshold: int,
        deadline_ts: float,
        created_at: float,
        ttl_ms: int,
        creator_id: str | None = None,
        participant_groups: dict[str, str] | None = None,
        group_thresholds: dict[str, int] | None = None,
    ) -> BarrierMeta:
        barrier_id = uuid.uuid4().hex
        (
            meta_key,
            roster_key,
            _,
            _,
            _,
            _,
            gmap_key,
            gcheck_key,
        ) = self._keys(barrier_id)
        groups = participant_groups or {}
        quotas = group_thresholds or {}
        mapping: dict[str, Any] = {
            "threshold": threshold,
            "deadline": f"{deadline_ts:.3f}",
            "created_at": f"{created_at:.3f}",
            "ttl_ms": ttl_ms,
        }
        if creator_id:
            mapping["creator_id"] = creator_id
        if quotas:
            mapping["group_thresholds"] = json.dumps(quotas, ensure_ascii=False)

        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.hset(meta_key, mapping=mapping)
            pipe.sadd(roster_key, *participants)
            pipe.pexpire(meta_key, ttl_ms)
            pipe.pexpire(roster_key, ttl_ms)
            if groups:
                pipe.hset(gmap_key, mapping=groups)
                pipe.pexpire(gmap_key, ttl_ms)
                # gcheck 计数键提前建立并设置 TTL：各分组从 0 开始，由报到脚本
                # HINCRBY 累加；即使全程无人报到也会随屏障一起被回收。
                pipe.hset(gcheck_key, mapping={g: 0 for g in quotas})
                pipe.pexpire(gcheck_key, ttl_ms)
            await pipe.execute()

        return BarrierMeta(
            barrier_id=barrier_id,
            participants=sorted(participants),
            threshold=threshold,
            deadline=deadline_ts,
            created_at=created_at,
            ttl_ms=ttl_ms,
            creator_id=creator_id,
            participant_groups=dict(groups),
            group_thresholds=dict(quotas),
        )

    async def get_meta(self, barrier_id: str) -> BarrierMeta | None:
        meta_key, roster_key, _, _, _, _, gmap_key, _ = self._keys(barrier_id)
        data = await self.redis.hgetall(meta_key)
        if not data:
            return None
        participants = sorted(await self.redis.smembers(roster_key))
        group_of: dict[str, str] = {}
        if "group_thresholds" in data:
            group_of = await self.redis.hgetall(gmap_key)
        return BarrierMeta(
            barrier_id=barrier_id,
            participants=participants,
            threshold=int(data["threshold"]),
            deadline=float(data["deadline"]),
            created_at=float(data["created_at"]),
            ttl_ms=int(data["ttl_ms"]),
            creator_id=data.get("creator_id"),
            participant_groups=group_of,
            group_thresholds=(
                json.loads(data["group_thresholds"])
                if "group_thresholds" in data
                else {}
            ),
        )

    async def checkin(self, barrier_id: str, participant_id: str) -> CheckinResult:
        """参与者幂等报到；达标判定（含分组配额）与冻结在 Lua 中原子完成。"""
        keys = self._keys(barrier_id)
        raw = await self._checkin(
            keys=list(keys), args=[participant_id, f"{time.time():.3f}"]
        )
        status, newly, quorum_json, frozen_at = raw

        if status == "unknown":
            raise BarrierNotFound(barrier_id)
        if status == "not_participant":
            raise NotAParticipant(participant_id)
        if status == "expired":
            raise BarrierExpired(barrier_id)
        if status == "cancelled":
            raise BarrierCancelled(barrier_id)

        result = CheckinResult(
            status=status,
            newly_added=newly == "1",
            quorum=json.loads(quorum_json) if quorum_json else None,
            frozen_at=float(frozen_at) if frozen_at not in ("", "0") else None,
        )

        # 冻结发生在 Lua 内部；脚本返回后立刻发布唤醒事件。
        # 即使发布丢失，等待方也有轮询兜底，不会睡死。
        if status == "reached":
            await self.redis.publish(self.channel(barrier_id), "reached")

        return result

    async def cancel(self, barrier_id: str, reason: str) -> CancelResult:
        """主动取消；与达标报到原子竞争，先发生者成为唯一终态。

        已取消时幂等返回同一取消结果；已达标 / 已截止抛出终态冲突异常。
        """
        keys = self._keys(barrier_id)
        raw = await self._cancel(
            keys=list(keys), args=[reason, f"{time.time():.3f}"]
        )
        status = raw[0]

        if status == "unknown":
            raise BarrierNotFound(barrier_id)
        if status == "reached":
            raise BarrierReached(barrier_id)
        if status == "expired":
            raise BarrierExpired(barrier_id)

        result = CancelResult(
            status=status,
            reason=raw[1],
            cancelled_at=float(raw[2]),
        )

        # 仅本次真正完成取消时发布唤醒；重复取消不重复广播。
        if status == "cancelled":
            await self.redis.publish(self.channel(barrier_id), "cancelled")

        return result

    async def extend_deadline(
        self,
        barrier_id: str,
        new_deadline: float,
        ttl_buffer_ms: int,
    ) -> ExtendResult:
        """运行中延期：仅 waiting 且新时间严格晚于当前 deadline 时原子生效。

        终态（reached / cancelled / expired）一律拒绝且不得复活；相同或更早
        的时间返回冲突。多个并发延期以 Redis 实际执行顺序为准，逐个与当前
        生效值比较。成功后刷新全部键的 TTL 并发布 ``deadline_extended`` 唤醒等待者。
        """
        keys = self._keys(barrier_id)
        raw = await self._extend(
            keys=list(keys),
            args=[
                f"{new_deadline:.3f}",
                f"{time.time():.3f}",
                ttl_buffer_ms,
            ],
        )
        status = raw[0]

        if status == "unknown":
            raise BarrierNotFound(barrier_id)
        if status == "reached":
            raise BarrierDeadlineConflict(barrier_id, "reached")
        if status == "cancelled":
            raise BarrierDeadlineConflict(barrier_id, "cancelled")
        if status == "expired":
            raise BarrierDeadlineConflict(barrier_id, "expired")
        if status == "conflict":
            raise BarrierDeadlineConflict(
                barrier_id, "conflict", current_deadline=float(raw[1])
            )

        result = ExtendResult(deadline=float(raw[1]), previous_deadline=float(raw[2]))

        # 与 reached / cancelled 一样：脚本提交成功后立刻发布唤醒；
        # 即使发布丢失，等待者也有轮询兜底，不会按旧截止时间误报 expired。
        await self.redis.publish(self.channel(barrier_id), "deadline_extended")
        return result

    async def get_frozen(self, barrier_id: str) -> dict[str, Any] | None:
        _, _, _, frozen_key, _, _, _, _ = self._keys(barrier_id)
        data = await self.redis.hgetall(frozen_key)
        if not data:
            return None
        frozen: dict[str, Any] = {
            "quorum": json.loads(data["quorum"]),
            "frozen_at": float(data["frozen_at"]),
        }
        # 旧快照或未启用分组配额时为空映射；视图据此不再补算分组进度。
        raw_counts = data.get("group_counts")
        frozen["group_counts"] = json.loads(raw_counts) if raw_counts else {}
        return frozen

    async def get_cancelled(self, barrier_id: str) -> dict[str, Any] | None:
        _, _, _, _, _, cancel_key, _, _ = self._keys(barrier_id)
        data = await self.redis.hgetall(cancel_key)
        if not data:
            return None
        return {
            "reason": data["reason"],
            "cancelled_at": float(data["cancelled_at"]),
        }

    async def checked_members(self, barrier_id: str) -> set[str]:
        _, _, checked_key, _, _, _, _, _ = self._keys(barrier_id)
        return await self.redis.smembers(checked_key)

    async def group_check_counts(self, barrier_id: str) -> dict[str, int]:
        """读取各分组当前去重报到人数；未启用分组配额时为空字典。"""
        _, _, _, _, _, _, _, gcheck_key = self._keys(barrier_id)
        raw = await self.redis.hgetall(gcheck_key)
        return {group: int(count) for group, count in raw.items()}

    async def list_events(
        self, barrier_id: str, cursor: int, limit: int
    ) -> tuple[list[dict[str, Any]], int]:
        """按游标读取报到审计事件：返回 seq > cursor 的至多 limit 条及新游标。

        事件只追加且 seq 与列表下标一一对应（下标 = seq - 1），并发报到
        只会让游标右侧变长，已读区间稳定，因此游标分页不重不漏。
        """
        _, _, _, _, events_key, _, _, _ = self._keys(barrier_id)
        raw = await self.redis.lrange(events_key, cursor, cursor + limit - 1)
        events = [json.loads(item) for item in raw]
        next_cursor = events[-1]["seq"] if events else cursor
        return events, next_cursor

    async def ping(self) -> None:
        await self.redis.ping()

    async def aclose(self) -> None:
        await self.redis.aclose()
