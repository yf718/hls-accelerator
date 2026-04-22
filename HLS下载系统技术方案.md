# HLS 分片下载任务技术方案

> 版本：v1.0 | 日期：2026-04-22

---

## 目录

1. [系统架构总览](#1-系统架构总览)
2. [核心设计原则](#2-核心设计原则)
3. [数据模型设计](#3-数据模型设计)
4. [aria2 接入层设计](#4-aria2-接入层设计)
5. [业务编排层设计](#5-业务编排层设计)
6. [任务生命周期与状态机](#6-任务生命周期与状态机)
7. [API 接口设计](#7-api-接口设计)
8. [事件驱动 + 定时对账机制](#8-事件驱动--定时对账机制)
9. [分片级精细控制](#9-分片级精细控制)
10. [幂等性与恢复性设计](#10-幂等性与恢复性设计)
11. [部署与配置](#11-部署与配置)
12. [扩展性设计](#12-扩展性设计)

---

## 1. 系统架构总览

```
┌─────────────────────────────────────────────────────────────────┐
│                        外部调用方 / API 层                        │
│              POST /tasks   GET /tasks   PATCH /tasks/:id         │
└────────────────────────────┬────────────────────────────────────┘
                             │ HTTP REST
┌────────────────────────────▼────────────────────────────────────┐
│                       业务编排层 (Orchestrator)                   │
│                                                                  │
│   ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐  │
│   │  Task 管理器  │  │ Item 调度器  │  │  状态聚合 & 对账服务  │  │
│   │  (CRUD/FSM)  │  │ (批量提交)   │  │  (Reconciler)        │  │
│   └──────┬───────┘  └──────┬───────┘  └──────────┬───────────┘  │
│          │                 │                      │              │
│   ┌──────▼─────────────────▼──────────────────────▼───────────┐ │
│   │                   数据库（SQLite / PostgreSQL）             │ │
│   │         task 表            task_item 表                    │ │
│   └────────────────────────────────────────────────────────────┘ │
└────────────────────────────┬────────────────────────────────────┘
                             │ WebSocket JSON-RPC + HTTP JSON-RPC
┌────────────────────────────▼────────────────────────────────────┐
│                    aria2 接入层 (Aria2 Adapter)                   │
│                                                                  │
│   ┌──────────────────┐          ┌────────────────────────────┐  │
│   │ WebSocket Client  │◄────────│ onStart / onComplete /     │  │
│   │ (事件订阅)        │  事件推送 │ onPause / onError / onStop │  │
│   └──────────────────┘          └────────────────────────────┘  │
│   ┌──────────────────┐                                           │
│   │ HTTP RPC Client   │  aria2.addUri / tellStatus /             │
│   │ (批量指令下发)    │  pause / resume / remove                 │
│   │ system.multicall  │                                           │
│   └──────────────────┘                                           │
└─────────────────────────────────────────────────────────────────┘
                             │ 执行
┌────────────────────────────▼────────────────────────────────────┐
│                         aria2c 进程                              │
│                   负责实际的 HTTP 分片下载                        │
└─────────────────────────────────────────────────────────────────┘
```

**分层职责说明：**

| 层 | 职责 | 不负责 |
|---|---|---|
| API 层 | 接收外部请求、参数校验、响应封装 | 业务逻辑 |
| 业务编排层 | m3u8 解析、任务/分片调度、状态聚合 | 实际下载 |
| aria2 接入层 | RPC 指令封装、事件监听、连接管理 | 业务语义 |
| aria2c 进程 | 并发 HTTP 下载、断点续传 | 任何业务状态 |

---

## 2. 核心设计原则

### 2.1 业务任务与 aria2 解耦

- **数据库是唯一真实状态源（Source of Truth）**，aria2 的状态仅作为输入信号
- `task` 代表一个 HLS 视频下载任务，`task_item` 代表一个分片下载单元
- aria2 只感知 `task_item`（通过 gid 映射），不感知 `task`
- `task` 的状态由其所有 `task_item` 的状态聚合计算得出

### 2.2 分片维度调度，任务维度聚合

```
task (1) ──────── (N) task_item
                        │
                        ▼
                  aria2 gid (1:1)
```

### 2.3 事件驱动 + 定时对账双轨制

- **主路径**：WebSocket 实时接收 aria2 事件，触发 `task_item` 状态更新
- **兜底路径**：定时器周期性执行 `tellStatus` 对账，补偿漏事件/重启/断连场景

### 2.4 批量操作优先

所有批量操作（暂停一批、恢复一批、查状态）均通过 `system.multicall` 发送，减少网络往返和鉴权开销。

---

## 3. 数据模型设计

### 3.1 task 表

```sql
CREATE TABLE task (
    id             TEXT        PRIMARY KEY,          -- UUID，任务唯一标识
    name           TEXT        NOT NULL DEFAULT '',  -- 任务名称（可为空）
    url            TEXT        NOT NULL,             -- 原始 m3u8 地址
    status         TEXT        NOT NULL DEFAULT 'pending',
                               -- pending | parsing | downloading
                               -- paused | completed | failed | deleted
    total_items    INTEGER     NOT NULL DEFAULT 0,   -- 解析得到的总分片数
    done_items     INTEGER     NOT NULL DEFAULT 0,   -- 已成功下载分片数
    failed_items   INTEGER     NOT NULL DEFAULT 0,   -- 失败分片数
    output_dir     TEXT        NOT NULL DEFAULT '',  -- 下载根目录
    extra          TEXT        NOT NULL DEFAULT '{}',-- 扩展 JSON（加密信息等）
    created_at     DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at     DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at    DATETIME                          -- 任务完成时间
);

CREATE INDEX idx_task_status ON task(status);
```

**status 枚举说明：**

| 状态 | 含义 |
|---|---|
| `pending` | 已提交，等待解析 m3u8 |
| `parsing` | 正在解析 m3u8，生成 task_item |
| `downloading` | 分片下载中 |
| `paused` | 已暂停（所有活跃 item 已暂停） |
| `completed` | 全部分片下载成功 |
| `failed` | 存在失败分片，任务终止 |
| `deleted` | 已删除，磁盘文件已清理 |

---

### 3.2 task_item 表

```sql
CREATE TABLE task_item (
    id             TEXT        PRIMARY KEY,          -- UUID
    task_id        TEXT        NOT NULL,             -- 关联 task.id
    seq            INTEGER     NOT NULL,             -- 在 m3u8 中的序号（0-based）
    url            TEXT        NOT NULL,             -- 分片下载 URL
    item_type      TEXT        NOT NULL DEFAULT 'segment',
                               -- segment | key（加密密钥）
    aria2_gid      TEXT                 DEFAULT NULL,-- aria2 返回的 gid
    status         TEXT        NOT NULL DEFAULT 'pending',
                               -- pending | queued | downloading
                               -- paused | completed | failed | removed
    file_path      TEXT                 DEFAULT NULL,-- 磁盘绝对路径
    file_size      INTEGER              DEFAULT NULL,-- 文件大小（字节）
    retry_count    INTEGER     NOT NULL DEFAULT 0,   -- 已重试次数
    error_msg      TEXT                 DEFAULT NULL,-- 最近一次错误信息
    created_at     DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at     DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (task_id) REFERENCES task(id) ON DELETE CASCADE
);

CREATE INDEX idx_item_task_id    ON task_item(task_id);
CREATE INDEX idx_item_aria2_gid  ON task_item(aria2_gid);
CREATE INDEX idx_item_status     ON task_item(task_id, status);
```

**item_type 说明：**

| 类型 | 说明 |
|---|---|
| `segment` | 视频分片（.ts / .aac 等） |
| `key` | AES 加密密钥文件（#EXT-X-KEY URI） |

**status 枚举说明：**

| 状态 | 含义 |
|---|---|
| `pending` | 已入库，等待提交给 aria2 |
| `queued` | 已提交给 aria2，aria2 等待下载 |
| `downloading` | aria2 正在下载中 |
| `paused` | 已暂停 |
| `completed` | 下载完成，文件落盘 |
| `failed` | 达到最大重试次数后失败 |
| `removed` | 已从 aria2 移除（配合任务删除） |

---

### 3.3 gid_index 辅助视图（可选）

```sql
-- 快速通过 aria2_gid 查找 task_id，避免全表扫描
CREATE VIEW v_gid_task AS
SELECT aria2_gid, task_id, id AS item_id, status
FROM task_item
WHERE aria2_gid IS NOT NULL;
```

---

## 4. aria2 接入层设计

### 4.1 配置参数

```ini
# aria2.conf 关键配置
enable-rpc=true
rpc-listen-port=6800
rpc-secret=YOUR_SECRET_TOKEN
rpc-listen-all=true

# 每个下载任务的并发连接数
max-connection-per-server=4

# 全局最大并发下载数（分片级并发）
max-concurrent-downloads=16

# 断点续传
continue=true

# 下载目录（业务侧通过 dir 参数覆盖）
dir=/downloads
```

### 4.2 WebSocket 事件监听

```typescript
// aria2 WebSocket 推送的事件类型
type Aria2Event =
  | 'aria2.onDownloadStart'
  | 'aria2.onDownloadPause'
  | 'aria2.onDownloadStop'
  | 'aria2.onDownloadComplete'
  | 'aria2.onDownloadError'
  | 'aria2.onBtDownloadComplete';

interface Aria2EventPayload {
  gid: string;
}

// 事件处理路由
class Aria2EventHandler {
  async handle(event: Aria2Event, payload: Aria2EventPayload) {
    const item = await db.taskItem.findByGid(payload.gid);
    if (!item) return; // 非本系统管理的 gid，忽略

    switch (event) {
      case 'aria2.onDownloadStart':
        await this.onStart(item);
        break;
      case 'aria2.onDownloadComplete':
        await this.onComplete(item);
        break;
      case 'aria2.onDownloadError':
        await this.onError(item);
        break;
      case 'aria2.onDownloadPause':
        await this.onPause(item);
        break;
    }

    // 每次 item 状态变更后，触发 task 聚合
    await taskAggregator.aggregate(item.taskId);
  }
}
```

### 4.3 批量 RPC 封装（system.multicall）

```typescript
// multicall 模板
function buildMulticall(calls: Array<{ method: string; params: any[] }>) {
  return {
    jsonrpc: '2.0',
    id: uuid(),
    method: 'system.multicall',
    params: [
      calls.map(call => ({
        methodName: call.method,
        params: [`token:${RPC_SECRET}`, ...call.params],
      })),
    ],
  };
}

// 示例：批量查询 gid 状态
async function batchTellStatus(gids: string[]) {
  const call = buildMulticall(
    gids.map(gid => ({
      method: 'aria2.tellStatus',
      params: [gid, ['gid', 'status', 'totalLength', 'completedLength', 'files', 'errorMessage']],
    }))
  );
  return await aria2HttpRpc(call);
}

// 示例：批量暂停
async function batchPause(gids: string[]) {
  const call = buildMulticall(
    gids.map(gid => ({ method: 'aria2.pause', params: [gid] }))
  );
  return await aria2HttpRpc(call);
}

// 示例：批量恢复
async function batchUnpause(gids: string[]) {
  const call = buildMulticall(
    gids.map(gid => ({ method: 'aria2.unpause', params: [gid] }))
  );
  return await aria2HttpRpc(call);
}

// 示例：批量移除
async function batchRemove(gids: string[]) {
  const call = buildMulticall(
    gids.map(gid => ({ method: 'aria2.remove', params: [gid] }))
  );
  return await aria2HttpRpc(call);
}
```

### 4.4 addUri 提交分片

```typescript
async function submitItem(item: TaskItem, outputDir: string): Promise<string> {
  const response = await aria2HttpRpc({
    jsonrpc: '2.0',
    id: uuid(),
    method: 'aria2.addUri',
    params: [
      `token:${RPC_SECRET}`,
      [item.url],
      {
        dir: outputDir,
        out: `${item.seq.toString().padStart(6, '0')}_${item.id}.ts`,
        // 为幂等性设置 gid（若 aria2 支持指定 gid）
        // 注意：aria2 不支持自定义 gid，需用 followMetalink 等方式
        // 此处记录 aria2 返回的 gid 到 task_item.aria2_gid
      },
    ],
  });
  return response.result; // aria2 返回的 gid
}
```

---

## 5. 业务编排层设计

### 5.1 m3u8 解析器

```typescript
interface ParsedM3u8 {
  segments: Array<{
    seq: number;
    url: string;         // 绝对 URL
  }>;
  keys: Array<{
    seq: number;         // 关联的首个分片序号
    url: string;         // 密钥绝对 URL
    method: string;      // AES-128 | SAMPLE-AES
    iv?: string;
  }>;
  isVod: boolean;
  targetDuration: number;
}

class M3u8Parser {
  async parse(m3u8Url: string): Promise<ParsedM3u8> {
    const content = await fetch(m3u8Url).then(r => r.text());
    // 1. 处理 Master Playlist -> 选取合适 Variant -> 递归解析
    // 2. 处理 Media Playlist -> 提取所有 #EXTINF + URI
    // 3. 处理 #EXT-X-KEY -> 提取加密密钥 URL
    // 4. 将相对 URL 转为绝对 URL（基于 m3u8Url）
    // 返回 ParsedM3u8
  }
}
```

### 5.2 任务提交流程

```
POST /tasks
    │
    ▼
1. 生成 task（status=pending）存库
    │
    ▼
2. 异步 Worker：解析 m3u8（status=parsing）
    │
    ├─ 解析失败 → task.status = failed
    │
    ▼
3. 生成所有 task_item（含 key 类型）批量插库
   task.status = downloading，task.total_items = N
    │
    ▼
4. 批量提交 task_item 到 aria2（分批，每批 ≤ 50）
   更新 task_item.aria2_gid，status = queued
    │
    ▼
5. 等待事件驱动 / 定时对账更新状态
```

### 5.3 Task 状态聚合器

```typescript
// 根据 task_item 状态聚合 task 状态，每次 item 变更后调用
async function aggregate(taskId: string): Promise<void> {
  const counts = await db.taskItem.countByStatus(taskId);
  // counts = { pending, queued, downloading, paused, completed, failed, removed }

  const total = counts.total;
  const done  = counts.completed;
  const failed = counts.failed;
  const active = counts.queued + counts.downloading;
  const paused = counts.paused;
  const pending = counts.pending;

  let newStatus: TaskStatus;

  if (done === total) {
    newStatus = 'completed';
  } else if (failed > 0 && active === 0 && pending === 0 && paused === 0) {
    newStatus = 'failed';
  } else if (paused > 0 && active === 0 && pending === 0) {
    newStatus = 'paused';
  } else if (active > 0 || pending > 0) {
    newStatus = 'downloading';
  } else {
    newStatus = 'failed'; // 兜底
  }

  await db.task.update(taskId, {
    status: newStatus,
    done_items: done,
    failed_items: failed,
    finished_at: newStatus === 'completed' ? new Date() : null,
  });
}
```

---

## 6. 任务生命周期与状态机

### 6.1 task 状态机

```
                  ┌──────────┐
         提交      │          │
   ──────────────► │ pending  │
                  │          │
                  └────┬─────┘
                       │ 开始解析
                       ▼
                  ┌──────────┐   解析失败
                  │ parsing  ├──────────────► failed ──► (deleted)
                  │          │
                  └────┬─────┘
                       │ 解析完成，item 入库
                       ▼
         ┌──────────────────────────┐
         │       downloading        │◄──────────────────────────┐
         │                          │  恢复(resume)              │
         └─────┬──────┬─────────────┘                          │
               │      │                                         │
          暂停  │      │ 全部完成                               │
               ▼      ▼                                         │
         ┌────────┐  ┌───────────┐  重试失败分片               │
         │ paused │  │ completed │  (retry_failed)              │
         └────┬───┘  └───────────┘                              │
              │                                                  │
              │ 存在失败分片且无活跃 item                        │
              ▼                                                  │
         ┌────────┐  重新提交失败分片                           │
         │ failed ├────────────────────────────────────────────►┘
         └────┬───┘
              │ 删除
              ▼
         ┌─────────┐
         │ deleted │  （磁盘文件已清理）
         └─────────┘
```

### 6.2 task_item 状态机

```
pending ──► queued ──► downloading ──► completed
                │             │
                │             └──► failed（含重试逻辑）
                │
                └──► paused ──► queued（恢复后）
                
任意状态 ──► removed（任务删除时）
```

---

## 7. API 接口设计

### 7.1 提交任务

```
POST /api/v1/tasks
Content-Type: application/json

{
  "name": "xx",       // 可为空
  "url": "xxx.m3u8"
}

Response 201:
{
  "id": "task-uuid",
  "name": "xx",
  "url": "xxx.m3u8",
  "status": "pending",
  "total_items": 0,
  "done_items": 0,
  "failed_items": 0,
  "created_at": "2026-04-22T10:00:00Z"
}
```

### 7.2 获取任务列表

```
GET /api/v1/tasks?status=downloading&page=1&size=20

Response 200:
{
  "total": 50,
  "items": [
    {
      "id": "...",
      "name": "...",
      "status": "downloading",
      "total_items": 120,
      "done_items": 45,
      "failed_items": 0,
      "progress": 0.375,
      "created_at": "..."
    }
  ]
}
```

### 7.3 获取任务详情（含分片列表）

```
GET /api/v1/tasks/:id
GET /api/v1/tasks/:id/items?status=failed&page=1&size=100

Response 200（任务详情）:
{
  "id": "...",
  "name": "...",
  "url": "...",
  "status": "failed",
  "total_items": 120,
  "done_items": 118,
  "failed_items": 2,
  "output_dir": "/downloads/task-uuid/",
  "items": [...]   // 仅在 GET /tasks/:id/items 时返回
}
```

### 7.4 暂停任务

```
POST /api/v1/tasks/:id/pause

逻辑：
1. 查询该 task 下 status IN (queued, downloading) 的所有 item
2. 收集 aria2_gid 列表
3. system.multicall 批量 aria2.pause
4. 更新 task_item.status = paused（事件驱动确认后更新，或乐观更新）
5. 聚合 task.status = paused

Response 200: { "paused_items": 75 }
```

### 7.5 恢复任务

```
POST /api/v1/tasks/:id/resume

逻辑：
1. 查询该 task 下 status = paused 的所有 item
2. system.multicall 批量 aria2.unpause
3. 聚合 task.status = downloading

Response 200: { "resumed_items": 75 }
```

### 7.6 重新提交失败分片

```
POST /api/v1/tasks/:id/retry

逻辑：
1. 查询 status = failed 的 item
2. 重置 item.status = pending，item.retry_count++，清空 aria2_gid
3. 批量重新 addUri 提交到 aria2
4. 更新 item.aria2_gid，status = queued
5. 聚合 task.status = downloading

支持指定分片：
Body: { "item_ids": ["item-uuid-1", "item-uuid-2"] }  // 为空则重试全部失败分片

Response 200: { "retried_items": 2 }
```

### 7.7 删除任务

```
DELETE /api/v1/tasks/:id

逻辑：
1. 查询所有 active item（queued/downloading/paused）的 gid
2. system.multicall 批量 aria2.remove（强制移除）
3. 删除磁盘目录（output_dir）
4. 更新 task.status = deleted（或直接物理删除，按需）

Response 204
```

### 7.8 分片级操作

```
// 暂停单个分片
POST /api/v1/tasks/:id/items/:itemId/pause

// 恢复单个分片
POST /api/v1/tasks/:id/items/:itemId/resume

// 获取单个分片详情
GET /api/v1/tasks/:id/items/:itemId
Response: { id, seq, url, aria2_gid, status, file_path, file_size, retry_count, error_msg }
```

---

## 8. 事件驱动 + 定时对账机制

### 8.1 WebSocket 连接管理

```typescript
class Aria2WsClient {
  private ws: WebSocket;
  private reconnectDelay = 3000;
  private maxReconnectDelay = 30000;

  connect() {
    this.ws = new WebSocket(`ws://localhost:6800/jsonrpc`);
    this.ws.on('message', this.onMessage.bind(this));
    this.ws.on('close', () => {
      // 指数退避重连
      setTimeout(() => this.connect(), this.reconnectDelay);
      this.reconnectDelay = Math.min(this.reconnectDelay * 2, this.maxReconnectDelay);
    });
    this.ws.on('open', () => {
      this.reconnectDelay = 3000; // 重置退避
    });
  }

  private async onMessage(raw: string) {
    const msg = JSON.parse(raw);
    if (msg.method) {
      // 服务端推送事件
      await eventHandler.handle(msg.method, msg.params[0]);
    }
  }
}
```

### 8.2 定时对账（Reconciler）

```typescript
// 每 30 秒执行一次全量对账
@Cron('*/30 * * * * *')
async reconcile() {
  // 1. 查询数据库中所有 status IN (queued, downloading) 的 item
  const activeItems = await db.taskItem.findActive(); // 返回 {id, aria2_gid, status}[]

  if (activeItems.length === 0) return;

  // 2. 批量查询 aria2 实际状态（system.multicall，每批 ≤ 100）
  const chunks = chunk(activeItems, 100);
  for (const batch of chunks) {
    const results = await batchTellStatus(batch.map(i => i.aria2_gid));
    
    // 3. 对比并修复差异
    for (const [idx, item] of batch.entries()) {
      const aria2Status = results[idx];
      await reconcileItem(item, aria2Status);
    }
  }
}

async function reconcileItem(item: TaskItem, aria2Status: Aria2StatusResult) {
  const mappedStatus = mapAria2Status(aria2Status.status);
  // aria2 status: waiting->queued, active->downloading, paused->paused,
  //               complete->completed, error->failed, removed->removed

  if (item.status !== mappedStatus) {
    // 状态不一致，以 aria2 为准进行修复
    await db.taskItem.updateStatus(item.id, mappedStatus, {
      file_path: aria2Status.files?.[0]?.path,
      file_size: aria2Status.completedLength,
      error_msg: aria2Status.errorMessage,
    });
    await taskAggregator.aggregate(item.taskId);
  }
}
```

### 8.3 服务重启恢复流程

```
服务重启
    │
    ▼
1. 查询 task.status IN (downloading, parsing) 的任务
    │
    ▼
2. 对 parsing 状态的 task：重新触发 m3u8 解析（幂等，已存在的 item 跳过）
    │
    ▼
3. 对 downloading 状态的 task：
   - 查询所有 status IN (queued, downloading) 的 item
   - 执行一次全量对账（reconcile）
   - 对 status=pending（未提交）的 item 补充提交到 aria2
    │
    ▼
4. 重新建立 WebSocket 连接，订阅事件
```

---

## 9. 分片级精细控制

### 9.1 加密分片处理

m3u8 中的 `#EXT-X-KEY` 指定 AES-128 加密密钥，需要：
1. 解析时将 key URL 单独作为 `item_type=key` 的 `task_item` 入库
2. key 文件优先下载（seq 设置为 -1，最优先提交）
3. 记录每个 segment 对应的 key 文件 id（通过 `extra` JSON 字段存储 `{"key_item_id":"xxx","iv":"xxx"}`）
4. 完整性校验时确保 key 文件存在

### 9.2 并发控制

```
全局并发策略：
┌─────────────────────────────────────────┐
│ aria2 max-concurrent-downloads = 16      │  全局分片并发数
│ aria2 max-connection-per-server = 4      │  单分片多线程数
│                                          │
│ 业务侧批量提交限速：每次最多提交 50 个    │  避免 aria2 队列积压
│ 提交间隔：500ms                          │
└─────────────────────────────────────────┘
```

### 9.3 重试策略

```typescript
const RETRY_CONFIG = {
  maxRetry: 3,           // 最大重试次数
  retryDelay: [0, 5, 30], // 每次重试等待秒数（指数退避）
};

async function onItemError(item: TaskItem, errorMsg: string) {
  await db.taskItem.update(item.id, {
    status: 'failed',
    error_msg: errorMsg,
    retry_count: item.retry_count + 1,
  });

  if (item.retry_count < RETRY_CONFIG.maxRetry) {
    const delay = RETRY_CONFIG.retryDelay[item.retry_count] * 1000;
    setTimeout(async () => {
      // 重新提交
      const gid = await submitItem(item, outputDir);
      await db.taskItem.update(item.id, { status: 'queued', aria2_gid: gid });
    }, delay);
  }
  // 超过最大重试次数，保持 failed 状态，等待人工 retry API
}
```

---

## 10. 幂等性与恢复性设计

### 10.1 任务提交幂等

```typescript
// 对同一个 m3u8 URL 的重复提交，可通过 url+name 做唯一性检查（业务决策）
// 若需要幂等，提交前查询是否存在 status != deleted 的同 URL 任务
async function createTask(input: CreateTaskInput) {
  const existing = await db.task.findByUrl(input.url);
  if (existing && existing.status !== 'deleted') {
    return existing; // 返回已存在的任务
  }
  // 新建
}
```

### 10.2 分片提交幂等

```typescript
// 提交前检查 item 是否已有有效 gid
async function submitItemIfNeeded(item: TaskItem) {
  if (item.aria2_gid && item.status !== 'failed') {
    // 已提交，对账确认状态即可
    return item.aria2_gid;
  }
  const gid = await submitItem(item, outputDir);
  await db.taskItem.update(item.id, { aria2_gid: gid, status: 'queued' });
  return gid;
}
```

### 10.3 数据库事务保障

```typescript
// 状态变更必须在事务中完成，保证 item + task 的聚合一致性
await db.transaction(async (tx) => {
  await tx.taskItem.update(itemId, newItemStatus);
  const counts = await tx.taskItem.countByStatus(taskId);
  const newTaskStatus = computeTaskStatus(counts);
  await tx.task.update(taskId, { status: newTaskStatus });
});
```

---

## 11. 部署与配置

### 11.1 目录结构

```
/downloads/
└── {task_id}/
    ├── 000000_{item_id}.ts
    ├── 000001_{item_id}.ts
    ├── ...
    ├── key_{item_id}.key        # AES 密钥文件
    └── merged.ts                # 合并后的完整文件（可选）
```

### 11.2 环境变量

```env
# aria2 连接配置
ARIA2_HTTP_URL=http://localhost:6800/jsonrpc
ARIA2_WS_URL=ws://localhost:6800/jsonrpc
ARIA2_SECRET=your_rpc_secret

# 业务配置
DOWNLOAD_BASE_DIR=/downloads
MAX_RETRY_COUNT=3
RECONCILE_INTERVAL_SEC=30
BATCH_SUBMIT_SIZE=50
BATCH_SUBMIT_INTERVAL_MS=500

# 数据库
DATABASE_URL=sqlite:///data/hls_tasks.db
```

### 11.3 组件依赖关系

```
┌─────────────────────────────────────────────┐
│  业务服务（Node.js / Go / Python）            │
│  - REST API Server                           │
│  - m3u8 Parser Worker                        │
│  - Aria2 WebSocket Client                    │
│  - Reconciler (Cron)                         │
└──────────────────┬──────────────────────────┘
                   │ RPC
          ┌────────▼────────┐
          │   aria2c 进程    │
          └────────┬────────┘
                   │ HTTP
          ┌────────▼────────┐
          │  HLS CDN/源站    │
          └─────────────────┘
```

---

## 12. 扩展性设计

### 12.1 多 aria2 实例支持

在 `task` 表增加 `aria2_node_id` 字段，`task_item` 的 gid 归属于特定 aria2 实例，调度器通过负载均衡选择实例。

### 12.2 任务优先级

在 `task` 表增加 `priority` 字段（0-10），`task_item` 提交时通过 aria2 的任务顺序控制下载优先级（aria2 内部按 FIFO 执行，可通过 `changePosition` 调整）。

### 12.3 下载完成后合并

```typescript
// 任务进入 completed 状态后，触发合并 Worker
async function mergeSegments(task: Task) {
  const items = await db.taskItem.findByTaskId(task.id, { type: 'segment', orderBy: 'seq' });
  const filePaths = items.map(i => i.file_path);
  // 执行 ffmpeg 或直接 concat 合并 .ts 文件
  await ffmpegConcat(filePaths, `${task.output_dir}/merged.ts`);
}
```

### 12.4 进度推送（SSE/WebSocket）

业务侧可对外提供 SSE 端点，每次 `task` 状态/进度变更时推送：

```
GET /api/v1/tasks/:id/events  (SSE)

data: {"id":"...","done_items":50,"total_items":120,"progress":0.42,"status":"downloading"}
```

---

## 附录：关键流程时序图

### A. 提交任务完整流程

```
Client          API层          编排层          aria2
  │               │               │               │
  │─POST /tasks──►│               │               │
  │               │──insert task──►               │
  │               │◄──task_id─────│               │
  │◄──201 task_id─│               │               │
  │               │               │               │
  │               │──async parse──►               │
  │               │               │─fetch m3u8    │
  │               │               │◄──segments    │
  │               │               │─insert items──►(DB)
  │               │               │               │
  │               │               │─addUri batch──►│
  │               │               │◄──gid[]────── │
  │               │               │─update gids───►(DB)
  │               │               │               │
  │               │               │       ◄─onComplete(gid)
  │               │               │─update item───►(DB)
  │               │               │─aggregate()───►(DB)
```

### B. 暂停/恢复流程

```
Client          API层          编排层                 aria2
  │               │               │                     │
  │─POST pause───►│               │                     │
  │               │─query items──►│                     │
  │               │◄─gids[]───────│                     │
  │               │               │─multicall pause─────►│
  │               │               │◄─results────────────│
  │               │               │─update paused───────►(DB)
  │               │               │─aggregate()─────────►(DB)
  │◄──200─────────│               │                     │
```

---

*文档结束*
