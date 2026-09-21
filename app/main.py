"""并行任务协调收尾屏障 API。

路由：
  POST   /barriers                           创建屏障（参与者名单、达标人数、截止时间）
  GET    /barriers/{bid}                     查询当前状态（不阻塞）
  POST   /barriers/{bid}/checkin             参与者幂等报到
  POST   /barriers/{bid}/cancel              主动取消（与达标报到原子竞争，先发生者为唯一终态）
  PATCH  /barriers/{bid}/deadline            运行中延期（仅 waiting 且新时间更晚，原子更新并唤醒等待者）
  GET    /barriers/{bid}/wait                阻塞等待达标 / 取消 / 截止
  GET    /barriers/{bid}/events              报到审计事件（cursor/limit 分页）

交互文档：GET /docs（Swagger UI）
"""

from __future__ import annotations

import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from redis.asyncio import Redis
from redis.exceptions import RedisError

from .barrier_store import (
    BarrierCancelled,
    BarrierDeadlineConflict,
    BarrierExpired,
    BarrierNotFound,
    BarrierReached,
    BarrierStore,
    NotAParticipant,
)
from .config import load_settings
from .models import (
    BarrierEventsPage,
    BarrierView,
    CancelRequest,
    CancelResponse,
    CheckinEvent,
    CheckinRequest,
    CheckinResponse,
    CreateBarrierRequest,
    ExtendDeadlineRequest,
)
from .service import _dt, build_view, wait_for_settle

settings = load_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    app.state.store = BarrierStore(redis)
    yield
    await app.state.store.aclose()


app = FastAPI(
    title="任务收尾屏障 API",
    description="并行任务协调收尾：创建屏障、幂等报到、阻塞等待达标、取消、截止或运行中延期。",
    version="1.3.0",
    lifespan=lifespan,
)


def store(request: Request) -> BarrierStore:
    return request.app.state.store


@app.exception_handler(RedisError)
async def redis_error_handler(request: Request, exc: RedisError) -> JSONResponse:
    return JSONResponse(
        status_code=503, content={"detail": f"Redis 暂不可用: {exc}"}
    )


@app.get("/", tags=["meta"])
async def root() -> dict[str, object]:
    return {
        "service": "task-barrier",
        "docs": "/docs",
        "health": "/health",
        "endpoints": {
            "create": "POST /barriers",
            "get": "GET /barriers/{barrier_id}",
            "checkin": "POST /barriers/{barrier_id}/checkin",
            "cancel": "POST /barriers/{barrier_id}/cancel",
            "extend_deadline": "PATCH /barriers/{barrier_id}/deadline",
            "wait": "GET /barriers/{barrier_id}/wait",
            "events": "GET /barriers/{barrier_id}/events",
        },
    }


@app.get("/health", tags=["meta"])
async def health(request: Request) -> dict[str, str]:
    await store(request).ping()
    return {"status": "ok"}


@app.post("/barriers", response_model=BarrierView, status_code=201, tags=["barriers"])
async def create_barrier(payload: CreateBarrierRequest, request: Request) -> BarrierView:
    """创建屏障。截止时间过后 BARRIER_TTL_BUFFER_MS 毫秒内数据可查询，之后自动回收。

    可选传入 participant_groups（每个参与者恰属一组）与 group_thresholds
    启用分组配额：只有总报到数达到 threshold 且各组报到数均达到对应配额才达标。
    两者必须同时提供或同时省略，分组/配额矛盾（未知成员、漏分组、非正配额、
    配额超过组人数、配额之和超过总 threshold）返回 422。
    """
    created_at = time.time()
    deadline_ts = payload.deadline_ts()

    if deadline_ts <= created_at:
        raise HTTPException(status_code=422, detail="deadline 必须是未来时间")

    threshold = (
        payload.threshold
        if payload.threshold is not None
        else len(payload.participants)
    )
    ttl_ms = int((deadline_ts - created_at) * 1000) + settings.ttl_buffer_ms

    meta = await store(request).create(
        participants=payload.participants,
        threshold=threshold,
        deadline_ts=deadline_ts,
        created_at=created_at,
        ttl_ms=ttl_ms,
        creator_id=payload.creator_id,
        participant_groups=payload.participant_groups,
        group_thresholds=payload.group_thresholds,
    )
    return await build_view(store(request), meta, created_at)


async def _load_meta(request: Request, barrier_id: str):
    meta = await store(request).get_meta(barrier_id)
    if meta is None:
        raise HTTPException(status_code=404, detail="屏障不存在或已过期回收")
    return meta


@app.get("/barriers/{barrier_id}", response_model=BarrierView, tags=["barriers"])
async def get_barrier(barrier_id: str, request: Request) -> BarrierView:
    """非阻塞查询屏障当前状态。"""
    meta = await _load_meta(request, barrier_id)
    return await build_view(store(request), meta)


@app.post(
    "/barriers/{barrier_id}/checkin",
    response_model=CheckinResponse,
    tags=["barriers"],
)
async def checkin(
    barrier_id: str, payload: CheckinRequest, request: Request
) -> CheckinResponse:
    """参与者幂等报到。

    - 重复报到返回 already_checked_in / late，不改变任何结果；
    - 并发报到首次同时满足「总达标人数 + 各组配额」时，达标成员与分组计数在
      Redis Lua 脚本内被原子冻结；跨组并发报到也只有一个请求 triggered=true；
    - 冻结之后的报到只会记录为迟到，quorum、frozen_at 与 group_progress 永不改写；
    - 屏障被取消后报到返回 409，取消终态不受任何影响。
    """
    meta = await _load_meta(request, barrier_id)
    svc = store(request)
    try:
        result = await svc.checkin(barrier_id, payload.participant_id)
    except NotAParticipant:
        raise HTTPException(
            status_code=403,
            detail=f"参与者 {payload.participant_id!r} 不在该屏障名单内",
        )
    except BarrierExpired:
        raise HTTPException(status_code=409, detail="屏障已过截止时间，报到被拒绝")
    except BarrierCancelled:
        raise HTTPException(status_code=409, detail="屏障已被取消，报到被拒绝")
    except BarrierNotFound:
        raise HTTPException(status_code=404, detail="屏障不存在或已过期回收")

    view = await build_view(svc, meta)

    return CheckinResponse(
        barrier_id=barrier_id,
        status=result.status,
        triggered=result.triggered,
        quorum=result.quorum,
        frozen_at=_dt(result.frozen_at) if result.frozen_at is not None else None,
        view=view,
    )


@app.post(
    "/barriers/{barrier_id}/cancel",
    response_model=CancelResponse,
    tags=["barriers"],
)
async def cancel_barrier(
    barrier_id: str, payload: CancelRequest, request: Request
) -> CancelResponse:
    """主动取消屏障。

    - 取消与达标报到在 Redis 中原子竞争：先发生者成为唯一终态；
    - 取消成功后状态为 cancelled 并保存 reason / cancelled_at，此后报到返回 409；
    - 已达标或已截止后取消返回 409，任何路径都不得改写既有终态；
    - 重复取消幂等：返回首次保存的同一份 reason 与 cancelled_at；
    - 取消成功会唤醒全部已在 /wait 挂起的等待者。
    """
    meta = await _load_meta(request, barrier_id)
    svc = store(request)
    try:
        result = await svc.cancel(barrier_id, payload.reason)
    except BarrierReached:
        raise HTTPException(status_code=409, detail="屏障已达标，终态不可取消")
    except BarrierExpired:
        raise HTTPException(status_code=409, detail="屏障已过截止时间，取消被拒绝")
    except BarrierNotFound:
        raise HTTPException(status_code=404, detail="屏障不存在或已过期回收")

    view = await build_view(svc, meta)
    return CancelResponse(
        barrier_id=barrier_id,
        status=result.status,
        reason=result.reason,
        cancelled_at=_dt(result.cancelled_at),
        view=view,
    )


@app.patch(
    "/barriers/{barrier_id}/deadline",
    response_model=BarrierView,
    tags=["barriers"],
)
async def extend_deadline(
    barrier_id: str, payload: ExtendDeadlineRequest, request: Request
) -> BarrierView:
    """运行中延期：把截止时间更新为更晚的绝对时间。

    - 仅允许屏障仍处于 waiting；reached / cancelled / expired 均为终态，
      返回 409，绝不复活，任何终态数据都不被触碰；
    - 新截止时间必须**严格晚于**当前生效的 deadline，相同或更早返回 409；
    - 状态校验、新旧时间比较、deadline 写入与全部相关键 TTL 刷新在同一
      Redis Lua 脚本内原子完成；
    - 多个并发延期以 Redis 实际生效顺序为准：每个请求只与当时的生效值比较，
      较早或相同的请求返回 409；
    - 延期成功后发布 ``deadline_extended`` 通知，已挂起的 /wait 立即重读最新
      deadline 继续等待，不会按旧截止时间误报 expired；
    - 成功时返回更新后的完整 BarrierView（status 仍为 waiting）。
    """
    meta = await _load_meta(request, barrier_id)
    svc = store(request)
    new_deadline = payload.deadline_ts()

    # 快速失败：请求时间不晚于当前已知截止时间直接拒绝；权威比较仍以 Lua 内
    # 读到的 Redis 当前值为准（并发延期下这里可能已过期）。
    if new_deadline <= meta.deadline:
        raise HTTPException(
            status_code=409,
            detail=(
                f"新截止时间必须晚于当前截止时间 "
                f"{_dt(meta.deadline).isoformat()}"
            ),
        )

    try:
        await svc.extend_deadline(
            barrier_id, new_deadline, settings.ttl_buffer_ms
        )
    except BarrierDeadlineConflict as exc:
        if exc.reason == "conflict":
            current = (
                _dt(exc.current_deadline).isoformat()
                if exc.current_deadline is not None
                else "未知"
            )
            raise HTTPException(
                status_code=409,
                detail=f"新截止时间必须晚于当前截止时间 {current}",
            )
        if exc.reason == "reached":
            raise HTTPException(status_code=409, detail="屏障已达标，终态不可延期")
        if exc.reason == "cancelled":
            raise HTTPException(status_code=409, detail="屏障已被取消，终态不可延期")
        raise HTTPException(
            status_code=409, detail="屏障已过截止时间，终态不可延期复活"
        )
    except BarrierNotFound:
        raise HTTPException(status_code=404, detail="屏障不存在或已过期回收")

    # 重读元数据后组装完整视图：deadline / TTL 已由 Lua 原子更新。
    latest = await _load_meta(request, barrier_id)
    return await build_view(svc, latest)


@app.get("/barriers/{barrier_id}/wait", response_model=BarrierView, tags=["barriers"])
async def wait_barrier(barrier_id: str, request: Request) -> BarrierView:
    """阻塞等待直到屏障达标、被取消（立即返回终态快照）或截止（返回报到成员与缺席名单）。

    多个等待者同时挂起：达标或取消时通过 Redis Pub/Sub 一并唤醒，轮询为兜底。
    最长挂起时间受 BARRIER_MAX_WAIT_S 限制。
    """
    meta = await _load_meta(request, barrier_id)
    return await wait_for_settle(
        store(request),
        meta,
        poll_interval_s=settings.poll_interval_s,
        max_wait_s=settings.max_wait_s,
    )


@app.get(
    "/barriers/{barrier_id}/events",
    response_model=BarrierEventsPage,
    tags=["barriers"],
)
async def list_events(
    barrier_id: str,
    request: Request,
    cursor: int = Query(
        default=0, ge=0, description="已读到的最大事件序号；0 表示从头开始"
    ),
    limit: int = Query(
        default=100, ge=1, le=1000, description="单页最大事件数（1-1000）"
    ),
) -> BarrierEventsPage:
    """查询报到审计事件（按 seq 升序，cursor/limit 分页）。

    每次名单成员报到（首次、重复、达标、迟到）都会在与状态变更同一 Redis
    原子操作中追加一条事件；非名单成员、截止后报到等错误请求不产生事件。
    事件只增不改，游标分页在并发报到下稳定不重不漏；屏障回收后事件一并消失。
    """
    await _load_meta(request, barrier_id)
    events, next_cursor = await store(request).list_events(barrier_id, cursor, limit)
    return BarrierEventsPage(
        barrier_id=barrier_id,
        events=[
            CheckinEvent(
                seq=e["seq"],
                participant_id=e["participant_id"],
                result=e["result"],
                ts=_dt(e["ts"]),
            )
            for e in events
        ],
        next_cursor=next_cursor,
    )
