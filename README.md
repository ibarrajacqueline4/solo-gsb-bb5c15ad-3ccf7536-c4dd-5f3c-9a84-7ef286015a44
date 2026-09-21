# 任务收尾屏障 API（Task Barrier API）

基于 **Python 3.12 + FastAPI + Redis** 的并行任务协调收尾服务：

- 创建屏障时指定**参与者名单**、**达标人数（quorum）**和**截止时间**；
- 可选启用**分组配额达标策略**：创建时传 `participant_groups`（每个参与者恰属一组）与 `group_thresholds`（各组配额），启用后只有**总报到数达到 `threshold` 且每个分组的报到数都达到对应配额**才进入 `reached`；
- 参与者以自己的标识**幂等报到**；
- 并发报到中**首次同时满足总人数与各组配额的瞬间，达标成员与分组计数在 Redis 内被原子冻结**为快照，之后的任何报到（哪怕来自其他名单成员）都不会改写 `quorum` / `frozen_at` / `group_progress`；跨组并发报到也**只有一个请求** `triggered=true`；
- 支持**主动取消**：取消与达标报到在 Redis 中**原子竞争**，先发生者成为唯一终态（`cancelled` 与 `reached` 互斥）；取消保存 `reason` / `cancelled_at` 并唤醒全部等待者；
- 支持**运行中延期**：`PATCH /barriers/{id}/deadline` 传入新的绝对截止时间，仅当屏障仍在 `waiting` 且新时间**严格晚于**当前 deadline 时，由 Redis Lua 原子校验 `reached / cancelled / expired` 终态并同时更新 deadline 与所有键的存活期；成功后唤醒全部已挂起的 `/wait` 立即按新时间继续等待；
- 提供**阻塞等待接口**：达标 / 取消时立即返回终态快照；截止时返回已报到成员与缺席名单；多个等待者通过 Redis Pub/Sub 一并被可靠唤醒（轮询兜底，消息丢失也不会睡死）。

## 一条命令启动（含 Redis）

```bash
docker compose up --build
```

该命令会构建 API 镜像并启动 Redis 7（带健康检查与 AOF 持久化），API 等 Redis 就绪后才启动。停止：`Ctrl+C`（或 `docker compose down`，加 `-v` 同时删除数据卷）。

## 访问地址与端口

| 用途 | 地址 |
| --- | --- |
| API Base URL | <http://localhost:8000> |
| Swagger 交互文档 | <http://localhost:8000/docs> |
| OpenAPI schema | <http://localhost:8000/openapi.json> |
| 健康检查（含 Redis ping） | <http://localhost:8000/health> |

Redis 仅在内网暴露：服务名 `redis`，端口 `6379`，不发布到宿主机。

## 必要配置

全部通过环境变量注入（`docker-compose.yml` 中已给默认值，一般无需修改）：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `REDIS_URL` | `redis://redis:6379/0` | Redis 连接地址 |
| `BARRIER_TTL_BUFFER_MS` | `86400000`（24h） | 屏障键在截止时间之后额外保留多久，之后由 Redis 自动回收 |
| `BARRIER_POLL_INTERVAL_S` | `0.5` | 等待接口的轮询兜底间隔（秒），Pub/Sub 为主唤醒通道 |
| `BARRIER_MAX_WAIT_S` | `3600` | 单次等待请求最长挂起时间（秒） |

宿主机端口可在 `docker-compose.yml` 的 `api.ports` 修改（如 `"9000:8000"`）。

## API 一览

### 1. 创建屏障 `POST /barriers`

`threshold` 缺省时等于参与者总人数（全员到齐才达标）。`deadline` 为 ISO-8601，不带时区时按 UTC 处理。

```bash
curl -s -X POST http://localhost:8000/barriers \
  -H 'Content-Type: application/json' \
  -d '{"participants":["node-a","node-b","node-c"],"threshold":2,"deadline":"2026-09-20T15:00:00Z"}'
```

返回（`201`，节选）：

```json
{
  "barrier_id": "8f3c…",
  "status": "waiting",
  "participants": ["node-a", "node-b", "node-c"],
  "threshold": 2,
  "deadline": "2026-09-20T15:00:00Z",
  "checked_in": [],
  "checked_count": 0
}
```

校验：`threshold <= 参与者人数`；`deadline` 必须是未来时间；名单会自动去重、去空白。

#### 分组配额达标（可选）

在创建请求中同时传入 `participant_groups`（**每个参与者恰属一组**，覆盖全部名单成员）与 `group_thresholds`（每个出现过的分组都要有**正整数**配额）。下例要求 3 个机房共至少 4 人报到，且每个机房至少 1 人：

```bash
curl -s -X POST http://localhost:8000/barriers \
  -H 'Content-Type: application/json' \
  -d '{
    "participants":["dc1-a","dc1-b","dc2-a","dc2-b","dc3-a"],
    "threshold":4,
    "deadline":"2026-09-21T15:00:00Z",
    "participant_groups":{
      "dc1-a":"dc1","dc1-b":"dc1",
      "dc2-a":"dc2","dc2-b":"dc2",
      "dc3-a":"dc3"
    },
    "group_thresholds":{"dc1":1,"dc2":1,"dc3":1}
  }'
```

返回（`201`，节选）：

```json
{
  "barrier_id": "8f3c…",
  "status": "waiting",
  "participants": ["dc1-a", "dc1-b", "dc2-a", "dc2-b", "dc3-a"],
  "threshold": 4,
  "checked_count": 0,
  "participant_groups": {"dc1-a": "dc1", "dc1-b": "dc1", "dc2-a": "dc2", "dc2-b": "dc2", "dc3-a": "dc3"},
  "group_progress": [
    {"group": "dc1", "threshold": 1, "checked_count": 0, "satisfied": false},
    {"group": "dc2", "threshold": 1, "checked_count": 0, "satisfied": false},
    {"group": "dc3", "threshold": 1, "checked_count": 0, "satisfied": false}
  ]
}
```

达标规则：**总报到数 ≥ `threshold` 且每个分组的去重报到数都达到其配额**，二者同时满足的瞬间才进入 `reached`。因此上例中即使先有 4 人报到，只要其中没有任何 `dc3` 成员，屏障仍保持 `waiting`。

- `group_progress` 出现在查询（`GET /barriers/{id}`）与报到响应的 `view` 中，逐组给出 `threshold` / `checked_count` / `satisfied`；`reached` 状态下为**冻结瞬间的快照**，迟到报到不再改变。
- 校验（任一不满足返回 `422`）：两个字段必须**同时提供或同时省略**；分组映射不得含名单外（未知）成员、不得漏掉任何参与者；分组名不得为空；配额必须为正整数、不得超过该组人数、不得引用不存在的分组、不得漏掉任何已出现的分组；**各组配额之和不得超过总 `threshold`**。
- 不传这两个字段时响应不含 `group_progress` / `participant_groups`，行为与旧版完全一致。

### 2. 参与者报到 `POST /barriers/{barrier_id}/checkin`

```bash
curl -s -X POST http://localhost:8000/barriers/<id>/checkin \
  -H 'Content-Type: application/json' \
  -d '{"participant_id":"node-a"}'
```

`status` 取值：

- `checked_in`：报到成功，屏障继续等待；
- `already_checked_in`：同一参与者重复报到（幂等，无副作用）；
- `reached`：**本次报到使屏障达标**（同时满足总人数与所有分组配额），`triggered=true`，`quorum` 为冻结成员快照；
- `late`：屏障已达标后才报到，仅记入 `late`，**冻结快照不变**。

非名单成员 → `403`；屏障不存在/已回收 → `404`；截止后或取消后报到 → `409`。

响应中的 `view` 与查询接口同一结构：启用分组配额时附带 `group_progress`；只有总报到数达到 `threshold` 且各组均达到配额时 `triggered` 才会为 `true`。

### 3. 主动取消 `POST /barriers/{barrier_id}/cancel`

请求体必须携带**非空** `reason`（纯空白字符串返回 `422`，首尾空白会被去除后保存）：

```bash
curl -s -X POST http://localhost:8000/barriers/<id>/cancel \
  -H 'Content-Type: application/json' \
  -d '{"reason":"上游已放弃等待，提前收尾"}'
```

返回（`200`）：

```json
{
  "barrier_id": "8f3c…",
  "status": "cancelled",
  "reason": "上游已放弃等待，提前收尾",
  "cancelled_at": "2026-09-20T14:58:00.123000Z",
  "view": { "status": "cancelled", "cancel_reason": "…", "cancelled_at": "…", "checked_in": ["node-a"] }
}
```

终态竞争与冲突规则：

- 取消与达标报到由两段 Redis Lua 脚本原子竞争，**先发生者成为唯一终态**：
  - 取消先发生 → 状态 `cancelled`，保存 `reason` 与 `cancelled_at`，此后任何报到返回 `409`（连 `checked_in` 都不会被追加）；
  - 达标先发生 → 取消返回 `409`，`quorum` / `frozen_at` 不变；
  - 已截止 → 取消返回 `409`，缺席名单不变。**任何路径都不得改写既有终态。**
- **重复取消幂等**：返回 `200` 且 `status` 为 `already_cancelled`，`reason` / `cancelled_at` 始终是首次保存的同一份结果（即使本次请求体里的 `reason` 不同）。
- 取消成功会向 Pub/Sub 频道发布 `cancelled`，**唤醒全部已在 `/wait` 挂起的等待者**；发布丢失时仍有轮询兜底。
- 取消时已有的报到名单作为快照保留在 `checked_in` 中；取消本身不是报到行为，不产生报到审计事件。
- 屏障不存在/已回收 → `404`。

### 4. 运行中延期 `PATCH /barriers/{barrier_id}/deadline`

屏障运行期间把截止时间**延后到一个更晚的绝对时间**（只接受绝对时间，不接受相对偏移；ISO-8601，不带时区时按 UTC 处理）：

```bash
curl -s -X PATCH http://localhost:8000/barriers/<id>/deadline \
  -H 'Content-Type: application/json' \
  -d '{"deadline":"2026-09-21T18:00:00Z"}'
```

成功返回 `200`，响应体即更新后的**完整 BarrierView**（`status` 仍为 `waiting`，报到名单 / 分组进度等字段原样保留，`deadline` 为新值）：

```json
{
  "barrier_id": "8f3c…",
  "status": "waiting",
  "participants": ["node-a", "node-b", "node-c"],
  "threshold": 2,
  "deadline": "2026-09-21T18:00:00Z",
  "checked_in": ["node-a"],
  "checked_count": 1
}
```

冲突语义（**全部返回 `409`**，终态永不复活）：

- `reached`：屏障已达标冻结，终态不可延期，`quorum` / `frozen_at` 不变；
- `cancelled`：屏障已被取消，终态不可延期，`reason` / `cancelled_at` 不变；
- `expired`：已过当前截止时间，不能通过延期把终态复活成 `waiting`，缺席名单不变；
- `conflict`：新截止时间**等于或早于**当前生效的 deadline（错误信息中带当前生效值）。只允许严格更晚的时间，没有“缩短截止时间”能力。
- 屏障不存在 / 已回收 → `404`。

并发语义：

- 终态校验（`frozen` / `cancel` / 是否过截止）、新旧时间比较、deadline 写入、全部相关键的 TTL 刷新在**同一段 Redis Lua 脚本**内原子完成，与报到 / 取消脚本互相串行，因此延期与并发报到、并发取消之间仍有唯一一致结果：谁先在 Redis 内执行谁生效，终态一旦落地，随后的延期必然 `409`；
- **多个并发延期以 Redis 实际生效顺序为准**：每个请求只与它执行那一刻的当前值比较，因此“先提交 +60s、再提交 +80s”两个请求都会成功（各自在生效时刻都是更晚值），而“+80s 先生效、+60s 后执行”则后者 `409`；相同时间的重复请求只有第一个 `200`，其余 `409`。最终生效值恒为所有成功延期后的最大时间；
- 延期成功后按新 deadline 重算并刷新所有屏障键（元数据 / 名单 / 报到 / 冻结占位 / 事件 / 取消占位 / 分组映射 / 分组计数）的存活期，各键依旧同生共死、在新截止点之后 `BARRIER_TTL_BUFFER_MS` 毫秒被回收；
- 延期成功会向 Pub/Sub 频道发布 `deadline_extended`：**已挂起的 `/wait` 被立即唤醒并回读最新 deadline 继续等待**，即使唤醒发生在旧截止时间之后也不会按旧时间误报 `expired`；发布丢失时轮询兜底同样会回读到新 deadline；
- 延期不是报到行为，**不产生报到审计事件**，报到计数与分组进度不变。

### 5. 阻塞等待 `GET /barriers/{barrier_id}/wait`

```bash
curl -s http://localhost:8000/barriers/<id>/wait   # 挂起，直到达标、取消或截止
```

- 达标时立即返回 `status: "reached"`、冻结的 `quorum` 与 `frozen_at`；
- 取消时立即返回 `status: "cancelled"`、`cancel_reason` 与 `cancelled_at`；
- 截止时返回 `status: "expired"`、`checked_in` 已报到名单与 `missing` 缺席名单；
- 任意数量的等待者并发挂起：达标 / 取消各发布一条 Pub/Sub 消息全部唤醒；订阅采用“先订阅后复查 + 轮询兜底”，消息丢失或错过发布也不会漏唤醒（截止不发布消息，由轮询在截止点自然到期兜底）；
- 等待期间屏障被延期（`deadline_extended`）时，等待者被唤醒后**重读最新 deadline 继续等待**：按新截止点判定超时，不会按进入请求时的旧 deadline 提前返回 `expired`。

### 6. 非阻塞查询 `GET /barriers/{barrier_id}`

随时查看当前 `waiting / reached / expired / cancelled` 状态、报到人数、冻结快照、缺席与迟到名单、取消原因与时间。

启用分组配额的屏障还会返回 `participant_groups` 与 `group_progress`：

```json
{
  "status": "waiting",
  "threshold": 4,
  "checked_count": 4,
  "group_progress": [
    {"group": "dc1", "threshold": 1, "checked_count": 2, "satisfied": true},
    {"group": "dc2", "threshold": 1, "checked_count": 2, "satisfied": true},
    {"group": "dc3", "threshold": 1, "checked_count": 0, "satisfied": false}
  ]
}
```

上例中总人数已达 `threshold`，但 `dc3` 配额未满足，屏障仍为 `waiting`。`reached` 后该数组是**冻结快照**（与 `quorum` 同生共死），迟到报到不会推进任何计数；`expired` / `cancelled` 下如实反映终态发生时的进度。未启用分组配额的屏障不返回该字段。

### 7. 报到审计事件 `GET /barriers/{barrier_id}/events`

```bash
curl -s 'http://localhost:8000/barriers/<id>/events?cursor=0&limit=100'
```

每次名单成员报到（`checked_in` / `already_checked_in` / `reached` / `late`）都会在与状态变更**同一 Redis 原子操作**中追加一条有序事件；非名单成员、截止后报到等错误请求不产生事件。

返回（节选）：

```json
{
  "barrier_id": "8f3c…",
  "events": [
    {"seq": 1, "participant_id": "node-a", "result": "checked_in", "ts": "2026-09-20T14:59:50.123000Z"},
    {"seq": 2, "participant_id": "node-b", "result": "reached", "ts": "2026-09-20T14:59:51.456000Z"}
  ],
  "next_cursor": 2
}
```

- `cursor`：已读到的最大事件序号（`0` 表示从头开始）；`limit`：单页条数（1–1000，默认 100）；
- 用上一页返回的 `next_cursor` 作为下一页的 `cursor` 即可顺序读完；无新事件时返回空页且 `next_cursor` 不变；
- 事件只增不改、`seq` 连续递增，游标分页在并发报到下稳定、不重不漏；
- 事件与屏障其他数据同生命周期：回收前可查询，回收后一并消失（`404`）。

## 并发与一致性保证

- 报到判定由一段 Lua 脚本完成：`名单校验 → 终态校验（frozen/cancel/截止）→ 去重 SADD（新成员才同步 HINCRBY 对应分组计数）→ 人数与分组配额判定 → 写入冻结快照（quorum + 分组计数）` 整体原子执行；取消由另一段 Lua 脚本完成：`终态校验（cancel/frozen/截止）→ 写入取消快照` 整体原子执行；延期由第三段 Lua 脚本完成：`终态校验（frozen/cancel/截止）→ 新旧 deadline 严格比较 → 写入 deadline 并重算/刷新全部键 TTL` 整体原子执行。Redis 单线程串行执行脚本，因此达标、取消与延期**必有唯一一致结果**：并发报到（包括不同分组的成员同时补齐各自短板）只有一个请求得到 `reached`，或者取消请求得到 `cancelled`；终态脚本先执行则延期得到 `409` 且终态绝不被复活，延期先执行也不改变随后到达的终态。互斥终态的成员集合、时间点、分组计数、取消原因均唯一确定。
- 多个并发延期只与各自执行时刻 Redis 中的当前 deadline 比较：严格更晚才生效，相同或更早返回 `409`，不存在“缩短截止时间”；生效顺序即 Redis 脚本执行顺序，最终 deadline 是全部成功延期后的最大值。
- 总数与分组计数都只统计**去重后的唯一报到**：重复报到（`already_checked_in`）与达标后的迟到报到（`late`）均不执行 `HINCRBY`，分组计数不重复累计；达标冻结之后连 `gcheck` 计数也一并冻结。
- 报到审计事件在同一 Lua 脚本内追加（`RPUSH` 到 `events` 列表，序号 = 列表长度 + 1），与状态变更同生共死：要么一起成功，要么都不发生；脚本串行执行保证 `seq` 从 1 连续递增、无空洞无重复。
- 终态落地后脚本的所有写路径都不会触碰对方的终态键：迟到报到只追加到 `checked` 集合（反映在 `late` 字段）；取消后的报到不写任何键；已达标 / 已截止 / 已取消后的延期与取消直接返回冲突。
- 报到响应里的 `PUBLISH reached`、取消响应里的 `PUBLISH cancelled`、延期响应里的 `PUBLISH deadline_extended` 都在脚本执行成功之后进行；即使发布失败，等待者每 `BARRIER_POLL_INTERVAL_S` 秒回读一次真实状态与最新 deadline。
- 截止不发布消息（无后台扫描进程），等待者在剩余截止时间上自然到期回读，避免引入额外调度组件；延期后“剩余截止时间”按新 deadline 重新计算。

## 本地不使用容器开发

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
REDIS_URL=redis://localhost:6379/0 uvicorn app.main:app --reload --port 8000
```

需要自行准备一个 Redis 7。

## 项目结构

```
app/
  main.py           FastAPI 路由与错误映射
  barrier_store.py  Redis 键模型 + 原子报到/取消/延期 Lua 脚本（分组配额计数、审计事件）+ Pub/Sub
  service.py        状态机视图组装、阻塞等待
  models.py         请求/响应模型与校验
  config.py         环境变量配置
Dockerfile          API 镜像
docker-compose.yml  api + redis（健康检查、依赖顺序、数据卷）
```
