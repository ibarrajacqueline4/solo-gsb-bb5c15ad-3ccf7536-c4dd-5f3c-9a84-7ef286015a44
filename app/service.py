"""屏障视图组装与阻塞等待逻辑。"""

from __future__ import annotations

import time
from datetime import datetime, timezone

from .barrier_store import BarrierMeta, BarrierStore
from .models import BarrierView, GroupProgress

# 不可再变的终态：达标、截止、主动取消
SETTLED_STATUSES = frozenset({"reached", "expired", "cancelled"})


def _dt(ts: float) -> datetime:
    return datetime.fromtimestamp(ts, tz=timezone.utc)


def _group_progress(
    meta: BarrierMeta, counts: dict[str, int]
) -> list[GroupProgress] | None:
    """组装分组配额进度；未启用分组配额时返回 None（保持旧版响应形态）。

    reached 状态传入的 counts 是冻结快照，其余状态是实时计数；
    satisfied 统一按 counts 与配额比较，终态视图因此不再回读可变数据。
    """
    if not meta.groups_enabled:
        return None
    return [
        GroupProgress(
            group=group,
            threshold=meta.group_thresholds[group],
            checked_count=int(counts.get(group, 0)),
            satisfied=int(counts.get(group, 0)) >= meta.group_thresholds[group],
        )
        for group in sorted(meta.group_thresholds)
    ]


async def build_view(store: BarrierStore, meta: BarrierMeta, now: float | None = None) -> BarrierView:
    """读取当前屏障状态并组装视图。

    状态机：
      reached    —— 存在冻结快照（达标成员已被原子固定）
      cancelled  —— 存在取消终态（reason/cancelled_at 已原子写入）
      expired    —— 无冻结/取消快照且 now >= deadline
      waiting    —— 其余情况

    reached 与 cancelled 由 Redis Lua 脚本原子竞争，互斥且都不可改写。
    """
    now = now if now is not None else time.time()
    frozen = await store.get_frozen(meta.barrier_id)
    cancelled = await store.get_cancelled(meta.barrier_id)
    checked = await store.checked_members(meta.barrier_id)
    # reached 使用冻结的分组计数快照；其余状态读实时计数（终态不会再变化）。
    group_counts = (
        frozen["group_counts"]
        if frozen is not None
        else await store.group_check_counts(meta.barrier_id)
    )
    groups = meta.participant_groups if meta.groups_enabled else None

    if frozen is not None:
        quorum = frozen["quorum"]
        quorum_set = set(quorum)
        late = sorted(checked - quorum_set)
        return BarrierView(
            barrier_id=meta.barrier_id,
            status="reached",
            participants=meta.participants,
            threshold=meta.threshold,
            deadline=_dt(meta.deadline),
            created_at=_dt(meta.created_at),
            checked_in=sorted(checked),
            checked_count=len(quorum),
            quorum=quorum,
            frozen_at=_dt(frozen["frozen_at"]),
            late=late or None,
            creator_id=meta.creator_id,
            participant_groups=groups,
            group_progress=_group_progress(meta, group_counts),
        )

    if cancelled is not None:
        # 取消时已有的报到作为快照保留，终态信息不可再变。
        checked_sorted = sorted(checked)
        return BarrierView(
            barrier_id=meta.barrier_id,
            status="cancelled",
            participants=meta.participants,
            threshold=meta.threshold,
            deadline=_dt(meta.deadline),
            created_at=_dt(meta.created_at),
            checked_in=checked_sorted,
            checked_count=len(checked_sorted),
            cancel_reason=cancelled["reason"],
            cancelled_at=_dt(cancelled["cancelled_at"]),
            creator_id=meta.creator_id,
            participant_groups=groups,
            group_progress=_group_progress(meta, group_counts),
        )

    if now >= meta.deadline:
        roster_set = set(meta.participants)
        checked_sorted = sorted(checked)
        missing = sorted(roster_set - checked)
        return BarrierView(
            barrier_id=meta.barrier_id,
            status="expired",
            participants=meta.participants,
            threshold=meta.threshold,
            deadline=_dt(meta.deadline),
            created_at=_dt(meta.created_at),
            checked_in=checked_sorted,
            checked_count=len(checked_sorted),
            missing=missing,
            creator_id=meta.creator_id,
            participant_groups=groups,
            group_progress=_group_progress(meta, group_counts),
        )

    checked_sorted = sorted(checked)
    return BarrierView(
        barrier_id=meta.barrier_id,
        status="waiting",
        participants=meta.participants,
        threshold=meta.threshold,
        deadline=_dt(meta.deadline),
        created_at=_dt(meta.created_at),
        checked_in=checked_sorted,
        checked_count=len(checked_sorted),
        creator_id=meta.creator_id,
        participant_groups=groups,
        group_progress=_group_progress(meta, group_counts),
    )


async def wait_for_settle(
    store: BarrierStore,
    meta: BarrierMeta,
    poll_interval_s: float,
    max_wait_s: float,
) -> BarrierView:
    """阻塞直到屏障达标、截止或被取消。

    主通道为 Pub/Sub：先订阅，再检查一次状态（消除“先检查后订阅”的丢消息窗口）；
    轮询作为兜底，覆盖发布丢失 / 截止时间到达（截止不发布消息）两种情况。
    多个等待者各自订阅同一频道，达标（reached）或取消（cancelled）发布时全部被唤醒。
    """
    deadline_d = meta.deadline

    # 已经有结果则立即返回（达标、取消快照或截止结果）
    first = await build_view(store, meta)
    if first.status in SETTLED_STATUSES:
        return first

    pubsub = store.redis.pubsub()
    await pubsub.subscribe(store.channel(meta.barrier_id))
    try:
        # 订阅成功后再次检查：终态发布可能发生在第一次检查与订阅之间
        second = await build_view(store, meta)
        if second.status in SETTLED_STATUSES:
            return second

        start = time.time()
        while True:
            now = time.time()
            remaining_budget = max_wait_s - (now - start)
            if remaining_budget <= 0:
                return await build_view(store, meta, now)

            # get_message 阻塞至多 timeout；Pub/Sub 消息与轮询共用这一等待
            timeout = min(
                poll_interval_s,
                max(0.0, deadline_d - now),
                remaining_budget,
            )
            if timeout > 0:
                await pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=timeout
                )

            # 无论被消息唤醒还是轮询到期，都回读真实状态：
            # 达标 / 取消则立即返回终态快照；到达截止则返回报到与缺席名单。
            view = await build_view(store, meta)
            if view.status in SETTLED_STATUSES:
                return view
    finally:
        try:
            await pubsub.unsubscribe(store.channel(meta.barrier_id))
        finally:
            await pubsub.aclose()
