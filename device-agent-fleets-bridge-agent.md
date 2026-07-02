# Device Agent × Fleets 集成方案（Bridge Agent 独立进程）

> **版本:** v1.1
> **日期:** 2026-07-02
> **状态:** 评审优化版
> **前置文档:** [device-agent-fleets-integration.md](./device-agent-fleets-integration.md)（v2.5，EMQX Rule 方案，已归档）

---

## 目录

1. [背景与动机](#1-背景与动机)
2. [设计目标与原则](#2-设计目标与原则)
3. [架构总览](#3-架构总览)
4. [Bridge Agent 进程设计](#4-bridge-agent-进程设计)
5. [Schema 映射引擎](#5-schema-映射引擎)
6. [统一 Topic 路由表](#6-统一-topic-路由表)
7. [上行协议适配（DA → Fleets）](#7-上行协议适配da--fleets)
8. [下行协议适配（Fleets → DA）](#8-下行协议适配fleets--da)
9. [Jobs 代理层](#9-jobs-代理层)
10. [设备注册与元数据同步](#10-设备注册与元数据同步)
11. [Fleets 与 Device Agent 侧协作](#11-fleets-与-device-agent-侧协作)
12. [认证与安全](#12-认证与安全)
13. [部署与生命周期](#13-部署与生命周期)
14. [高可用与可观测性](#14-高可用与可观测性)
15. [与旧方案对比](#15-与旧方案对比)
16. [实施路线图](#16-实施路线图)
17. [附录](#17-附录)

---

## 1. 背景与动机

[v2.5 旧方案](./device-agent-fleets-integration.md) 以 **EMQX Rule Engine `republish`** 为核心，在 Broker 内完成 DA topic/payload 与 Fleets topic/payload 的转换。该方案适合已实现的**上行**能力（telemetry → Shadow reported、event、lifecycle），但在以下方面存在结构性不足：

| # | 问题 | 说明 |
|---|------|------|
| P1 | **下行能力缺失** | Command、Shadow desired/delta、Jobs 被收入 §12 远期 TODO；Rule republish 难以闭环 |
| P2 | **Rule 无法承载复杂语义** | Shadow desired → DA 命令、writable 字段映射、Jobs 请求/响应状态机、命令 correlation 等需要**有状态逻辑 + Schema 查表**，EMQX Rule SQL 表达力不足 |
| P3 | **DeviceSpec → ThingType 转换受限** | ThingType schema 需在注册阶段人工修订并通过 Fleets API 校验；Rule 无法在运行时引用映射表做 payload 字段级转换 |
| P4 | **无法对齐 Fleets 标准设备步骤** | Fleets 原生设备订阅 `$emqx/commands/.../request`、`shadow/update/delta`、`jobs/*`；DA 设备只订阅 `device-agent/.../commands`。纯 Rule 下行导致 `ErrNoMatchingSubscribers`，且 Jobs 双向协议无法用 republish 模拟 |
| P5 | **运维与版本治理困难** | Rule SQL 与 Product schema 变更耦合；ThingType 修订后须 `SyncRules` 重新注入 SQL，无法热更新映射 |

**新方案核心思路**：在 Fleets 部署侧启动一个**独立的 Bridge Agent 进程**，通过 **MQTT 原生 subscribe/publish** 与 **EMQX REST API publish** 双向接入 Broker，在 MQTT 层完成协议适配；必要时调用 **Fleets REST API** 与 **Device Agent REST API** 补齐 Schema、映射与元数据。

> Fleets **核心服务仍不订阅 MQTT**（架构约束不变）。MQTT 订阅职责由 Bridge Agent 这一**独立 Sidecar 进程**承担，而非嵌入 Fleets HTTP 进程。

---

## 2. 设计目标与原则

### 2.1 目标

| 目标 | 说明 |
|------|------|
| **双向互通** | 上行（telemetry/event/command response/jobs 上报）+ 下行（command/shadow/jobs 通知与代理）一期设计到位 |
| **Fleets 标准路径对齐** | Bridge 对 Fleets 侧仍发布/订阅标准 `$emqx/` 主题；Fleets 预配 EMQX Rule → ThingDatas API 路径**不变** |
| **Schema 可治理** | DeviceSpec ↔ ThingType 映射持久化、版本化，支持人工修订与 API 校验 |
| **最小侵入 Fleets 核心** | Fleets 主进程改动限于：映射表 Migration、Bridge Agent 的部署编排；JobService / ShadowService **不修改** |
| **可选启用** | Bridge Agent 与 DA IoT Platform 插件均可独立开关 |
| **可观测** | 结构化日志、Prometheus 指标、健康检查、trace_id 全链路 |

### 2.2 原则

| 原则 | 说明 |
|------|------|
| **Bridge 专职 MQTT 适配** | 所有 DA↔Fleets topic/payload 转换在 Bridge Agent 内完成，不依赖 EMQX Rule republish |
| **Fleets 预配规则照旧** | Bridge 向 `$emqx/things/{name}/...` 发布后，仍由 Fleets 既有 Rule（shadow/events/command response/jobs）写入 PG / EMQX Tables |
| **DA 主动注册** | Device Agent IoT Platform 插件仍是 Fleets Thing/ThingType 注册的唯一发起方 |
| **映射表为运行时真相源** | Bridge 独写 `da_bridge_*` 表；Schema 映射版本与 Product/ThingType 修订联动 |
| **Fail-safe** | 映射缺失时丢弃并告警，不伪造 payload；命令 correlation 严格校验 |
| **复用 EMQX 凭据** | Bridge 使用 Fleets 已有的 `EMQX_API_KEY` / `EMQX_API_SECRET` 做 REST publish；MQTT 连接使用独立 Bridge 服务账号 |

---

## 3. 架构总览

### 3.1 整体架构图

```
                         ┌─────────────────────────────────────────────┐
                         │              EMQX Broker                     │
                         │                                              │
    DA 主题域             │   v1/{pid}/{did}/telemetry|event            │
    device-agent/...      │   device-agent/{pid}/device/{did}/...       │
                         │                                              │
                         │   Fleets 主题域                              │
                         │   $emqx/things/{name}/shadow|events|jobs/*  │
                         │   $emqx/commands/things/{name}/executions/* │
                         │                                              │
                         │   Fleets 预配规则（不变）                     │
                         │   $emqx/... → ThingDatas API / EMQX Tables │
                         └───────────┬─────────────────────┬────────────┘
                                     │                     │
                          MQTT sub/pub│                     │MQTT
                                     │                     │
              ┌──────────────────────┴──┐         ┌────────┴────────┐
              │   Bridge Agent          │         │ Fleets 原生设备  │
              │   （独立进程）           │         │ + 桥接设备       │
              │                         │         └─────────────────┘
              │ • MQTT Client (sub/pub) │
              │ • EMQX REST API (pub)   │
              │ • Schema Mapper         │
              │ • Topic Router          │
              │ • Protocol Translators  │
              │ • In-Flight Command     │
              │ • Mapping Store (PG)    │
              │ • HTTP /health /metrics │
              └───────────┬─────────────┘
                          │
          ┌───────────────┼───────────────┐
          │ REST          │ REST          │
          ▼               ▼               ▼
   ┌─────────────┐ ┌─────────────┐ ┌─────────────────┐
   │ Fleets API  │ │ DA API      │ │ PostgreSQL      │
   │ Thing/Type  │ │ Products/   │ │ da_bridge_*     │
   │ Shadow/Cmd  │ │ Devices     │ │ (共享或同库)    │
   └─────────────┘ └─────────────┘ └─────────────────┘

┌──────────────────────────────────────────────────────────────────────┐
│ Fleets 实例（HTTP 主进程，不订阅 MQTT）                                │
│  • ThingService / JobService / ShadowService — 标准行为不变           │
│  • da_bridge_* 表 Migration 纳入 Fleets 仓库                          │
│  • docker-compose / systemd 编排 Bridge Agent 生命周期                │
└──────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────┐
│ Device Agent 实例                                                     │
│  • IoT Platform 插件（fleets）：ThingType/Thing 注册 + Schema 草稿   │
│  • 不订阅 MQTT（Bridge 负责 DA↔Fleets 转换）                          │
│  • namespace 感知：API 请求带 ?namespace= 查询参数                    │
└──────────────────────────────────────────────────────────────────────┘
```

### 3.2 与旧方案的关键差异

| 维度 | 旧方案（v2.5 Rule） | 新方案（Bridge Agent） |
|------|---------------------|------------------------|
| 协议转换位置 | EMQX Rule SQL `republish` | Bridge Agent 进程内 Go 逻辑 |
| MQTT 订阅者 | 无（Fleets 不订阅） | Bridge Agent 独立订阅 |
| 下行 Command | §12 远期 Rule #6 | Bridge 订阅 Fleets command request → 发布 DA commands |
| Shadow 下行 | §12 远期 Rule #4/#5 | Bridge 订阅 shadow desired/delta → 按 per-property desiredApply 映射为 DA 命令 |
| Jobs | §12 明确不支持 | Bridge Jobs 代理层（commandMap + proxy 两模式） |
| Schema 转换 | DA 插件注册时一次性 | 映射表 + 运行时查表 + schema_version 版本化 |
| Fleets 改动 | BridgePlugin + Rule Manager | 仅 Migration + 编排 Sidecar |

### 3.3 核心数据流一览

| 方向 | DA 侧 | Bridge 动作 | Fleets 侧 | 存储 |
|------|-------|------------|-----------|------|
| 状态上行 | `v1/.../telemetry` (type=state) | 转 `$emqx/things/{name}/shadow/update` (reported) | 预配 Rule → ThingDatas | PG + EMQX Tables |
| 事件上行 | `v1/.../event` | 转 `$emqx/things/{name}/events/{eventType}` | 预配 Rule → EMQX Tables | EMQX Tables |
| 命令响应上行 | `device-agent/.../responses` | 转 `$emqx/commands/.../response` | 预配 Rule → ThingDatas | PG |
| 命令下行 | `device-agent/.../commands` | 订阅 Fleets request → 发布 DA commands | JobService EMQX publish | PG |
| Shadow 下行 | DA commands（合成） | 订阅 desired/delta → 按 mapping_spec 发 DA 命令 | ShadowService EMQX publish | PG |
| Jobs 下行 | DA commands / 代理请求 | 订阅 notify → commandMap 转发或 proxy 代跑 | JobService EMQX publish | PG |
| Jobs 上行 | DA responses / Bridge 代发 | 转 `$emqx/things/{name}/jobs/{jobId}/update` 等 | 预配 Rule → ThingDatas | PG |
| Lifecycle | EMQX connect/disconnect | **不经 Bridge** | 预配 Rule → ThingDatas | PG |

> Lifecycle 仍依赖 Thing.`mqttClientId` 与 EMQX `clientid` 匹配；由 DA IoT 插件注册时填写（与旧方案一致）。

---

## 4. Bridge Agent 进程设计

### 4.1 定位与边界

**Bridge Agent** 是 Fleets 部署拓扑中的**独立 Sidecar 服务**（建议仓库路径：`fleets/cmd/da-bridge/`）。

| 负责 | 不负责 |
|------|--------|
| DA ↔ Fleets MQTT topic/payload 双向转换 | Fleets 业务逻辑（Shadow 计算、Job 调度、命令持久化） |
| Schema 映射运行时查表与 payload 校验 | DA 设备注册 UI（仍归 Device Agent） |
| 映射表读写、Schema 版本同步 | 修改 Fleets JobService / ShadowService |
| MQTT 连接保活、reconnect + 重订阅 | DA SDK 行为变更 |
| In-flight 命令 PG 持久化 + TTL 清理 | EMQX Broker 认证策略制定 |
| 健康检查、Prometheus 指标、结构化日志 | — |

### 4.2 进程内模块

```
cmd/da-bridge/
├── main.go                  # 入口、信号处理、生命周期
├── config/                  # 环境变量、配置校验
├── lifecycle/               # Start / Stop / Ready / Live
├── mqtt/
│   ├── client.go            # 原生 MQTT 客户端（subscribe + 高吞吐 publish）
│   └── emqx_publish.go      # EMQX REST API v5 publish 封装（复用 EMQX_API_KEY）
├── router/
│   ├── topic.go             # 统一 Topic 路由表 + 参数提取
│   └── dispatch.go          # 消息分发 → translator
├── translator/
│   ├── telemetry.go         # state/status 过滤 + reported 构造
│   ├── event.go             # event + severity 映射
│   ├── command.go           # command request/response 双向 + in-flight 管理
│   ├── shadow.go            # desired/delta → per-property desiredApply → DA 命令
│   └── jobs.go              # Jobs 协议代理（commandMap + proxy）
├── schema/
│   ├── mapper.go            # DeviceSpec ↔ ThingType 转换
│   ├── registry.go          # 内存缓存 + schema_version 选用
│   └── validator.go         # payload 字段校验
├── store/
│   ├── mapping.go           # da_bridge_mappings CRUD
│   ├── schema_mapping.go    # da_bridge_schema_mappings CRUD
│   ├── inflight.go          # in-flight 命令表（PG 持久化）
│   └── migration.go
├── api/
│   ├── health.go            # GET /health, /ready
│   └── admin.go             # 映射 resync、schema 刷新、mapping_spec 写入口
├── da/client.go             # Device Agent REST 客户端
├── fleets/client.go         # Fleets REST 客户端
└── metrics/                 # Prometheus 指标
```

### 4.3 生命周期管理

```
                    ┌─────────────┐
                    │   Starting  │
                    └──────┬──────┘
                           │ Load config
                           │ DB migrate
                           │ Acquire advisory lock (0xDABRIDGE)
                           │ Load schema registry (all status=active)
                           │ Connect MQTT
                           │ Batch subscribe (§6 topic list)
                           │ Register message handler
                           ▼
                    ┌─────────────┐
         ┌─────────│    Ready    │◄────────┐
         │         └──────┬──────┘         │
         │                │                │
         │                ▼                │
         │         ┌─────────────┐         │
         │         │   Running   │─────────┤
         │         └──────┬──────┘         │
         │                │                │ Error threshold
         │    SIGTERM     │                ▼
         │                ▼          ┌─────────────┐
         └───────────────│ Draining  │  Degraded   │
                         └──────┬───┘  (告警仍服务)│
                                │                 │
                                ▼                 │
                         ┌─────────────┐          │
                         │   Stopped   │◄─────────┘
                         └─────────────┘   Fatal / manual stop
```

| 阶段 | 行为 |
|------|------|
| **Start** | 执行 DB migration → 连接 PostgreSQL → 抢 advisory lock → 加载映射表与 Schema 缓存 → 建立 MQTT 连接（cleanSession=false）→ 批量 subscribe → 注册 message handler → 启动 in-flight TTL 清理 goroutine → 暴露 `/health` `/metrics` |
| **Running** | 处理 MQTT 消息；定期从 DA/Fleets API 增量同步 Schema（`SCHEMA_SYNC_INTERVAL`）；清理过期 in-flight 命令并发布 FAILED 响应 |
| **Draining** | SIGTERM：停止消息处理 → 等待 in-flight 命令翻译完成（超时 `SHUTDOWN_TIMEOUT`）→ 断开 MQTT → 关闭 DB → 释放锁 |
| **Ready probe** | MQTT 已连接 + 全部 topic 已 subscribe + DB 可达 + advisory lock 持有 |
| **Live probe** | 进程存活 + MQTT reconnecting 算 live（不触发重启） |

**编排方式**（择一或组合）：

- `docker-compose.yml` 中 `da-bridge` 服务，`depends_on: [fleets, emqx, postgres]`
- Fleets 安装包 systemd unit：`da-bridge.service` + `fleets.service`
- Kubernetes：`Deployment` sidecar 或独立 `Deployment` 共享 ConfigMap/Secret

### 4.4 MQTT 接入策略

| 方式 | 用途 | 说明 |
|------|------|------|
| **MQTT 原生客户端** | **Subscribe** + **上行高吞吐 Publish** | EMQX REST API 不支持持久订阅；下行监听必须走 MQTT client |
| **EMQX REST API v5 `/publish`** | 与 Fleets JobService 相同语义的命令/Jobs 响应 publish | 复用 `EMQX_API_KEY`；QoS 一致 |

**Bridge MQTT 连接参数**：

```bash
DA_BRIDGE_MQTT_URL=mqtt://localhost:1883
DA_BRIDGE_MQTT_CLIENT_ID=da-fleets-bridge-{instanceId}
DA_BRIDGE_MQTT_USERNAME=da-bridge-svc          # 独立服务账号
DA_BRIDGE_MQTT_PASSWORD=...
DA_BRIDGE_MQTT_CLEAN_SESSION=false             # 持久会话，避免重启丢订阅 + 消息积压
DA_BRIDGE_MQTT_KEEPALIVE=60
DA_BRIDGE_MQTT_CONNECT_TIMEOUT=30s
DA_BRIDGE_MQTT_RECONNECT_BACKOFF=1s,5s,15s,30s,60s  # 指数退避，上限 60s
```

> **Restart 行为**：`cleanSession=false` + 持久会话保证重启期间消息不丢。重启后 MQTT broker 将积压消息推送给 Bridge。in-flight 命令表在 PG 中持久化，重启后恢复。

### 4.5 消息处理流水线

```
MQTT OnMessage
    │
    ▼
Topic Router ──► 解析 {productId, deviceId, thingName, executionId, jobId}
    │
    ▼
Mapping Lookup ──► da_bridge_mappings（miss → 丢弃 + metric + warn log）
    │
    ▼
Schema Registry ──► 取 thing 的 schema_version → da_bridge_schema_mappings
    │
    ▼
Translator ──► payload 转换 + 校验
    │
    ▼
Publish ──► MQTT client 或 EMQX REST API
    │
    └──► 结构化日志 + trace_id + latency histogram
```

---

## 5. Schema 映射引擎

旧方案将 DeviceSpec → ThingType 转换主要放在 DA IoT 插件注册时，Rule 层无法引用。新方案将 **Schema 映射** 提升为一等公民。

### 5.1 数据模型

#### `da_bridge_mappings`（设备实例映射）

```sql
CREATE TABLE da_bridge_mappings (
    id              BIGSERIAL PRIMARY KEY,
    da_product_id   TEXT NOT NULL,           -- DA 侧 baseProductId（稳定标识）
    da_device_id    TEXT NOT NULL,
    da_namespace    TEXT NOT NULL DEFAULT 'default',
    thing_type_id   UUID NOT NULL,
    thing_id        UUID NOT NULL UNIQUE,
    thing_name      TEXT NOT NULL UNIQUE,
    mqtt_client_id  TEXT NOT NULL,           -- 用于 lifecycle 匹配；格式 {namespace}/dev/{deviceId}
    schema_version  INTEGER NOT NULL DEFAULT 1,  -- 指向 da_bridge_schema_mappings.schema_version
    da_metadata     JSONB,
    last_synced_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (da_product_id, da_device_id)
);
```

> **`da_product_id` 使用 DA 的 `baseProductId`**（跨版本稳定的产品线标识），而非带版本的 `productId`。ThingType 名 `da-product-{baseProductId}` 因此稳定。Product 版本升级时更新同一 ThingType 的 schema，或用户手动指定新 ThingType 名。

#### `da_bridge_schema_mappings`（Product ↔ ThingType 映射规则）

```sql
CREATE TABLE da_bridge_schema_mappings (
    id                BIGSERIAL PRIMARY KEY,
    da_product_id     TEXT NOT NULL,          -- DA baseProductId
    thing_type_name   TEXT NOT NULL,
    schema_version    INTEGER NOT NULL DEFAULT 1,
    status            TEXT NOT NULL DEFAULT 'active',  -- draft | active | deprecated

    mapping_spec      JSONB NOT NULL,         -- 映射规则（见 §5.2）

    -- 快照（审计与 diff）
    da_device_spec    JSONB,                  -- 注册时的 DA Product DeviceSpec
    fleets_schema     JSONB,                  -- 注册时的 Fleets ThingType schema

    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (da_product_id, schema_version)
);
```

### 5.2 `mapping_spec` 结构（JSONB）

```json
{
  "properties": {
    "temp_a": {
      "fleetsKey": "temp_a",
      "type": "number"
    },
    "reportIntervalSec": {
      "fleetsKey": "reportIntervalSec",
      "type": "integer",
      "writable": true,
      "desiredApply": {
        "strategy": "command",
        "command": "setInterval",
        "paramMap": { "reportIntervalSec": "interval" }
      }
    }
  },
  "events": {
    "overheat": {
      "fleetsEventName": "overheat",
      "fleetsSeverity": "warn",
      "daEventType": "alert",
      "fieldMap": { "zone": "zone", "temperature": "temperature" }
    }
  },
  "commands": {
    "setInterval": {
      "fleetsAction": "setInterval",
      "daCmd": "setInterval",
      "inputMap": { "interval": "interval" },
      "outputMap": {}
    },
    "reboot": {
      "fleetsAction": "reboot",
      "daCmd": "reboot",
      "inputMap": { "delaySeconds": "delaySeconds", "force": "force" }
    }
  },
  "jobs": {
    "mode": "proxy",
    "operationCommandMap": {
      "firmware_update": "startFirmwareUpdate"
    }
  }
}
```

**`desiredApply` 策略**（per-property，Shadow 下行核心）：

| strategy | 行为 |
|----------|------|
| `command` | 将 desired 中该 writable 字段变化映射为一条 DA `commands` 消息 |
| `stateOnly` | 仅更新 Fleets reported（非下行，标记为内部属性） |
| `ignore` | 跳过该字段（无 writable 映射或用户显式禁用） |

> **设计决策**：不设置全局 `shadow.desiredStrategy` / `shadow.deltaStrategy`。每个 writable property 独立声明 `desiredApply`。当 desired/delta 变更涉及多个 property 时，逐字段生成 DA 命令。可通过 `SHADOW_DESIRED_BATCH_MODE` 控制合并行为（见 §8.2）。

### 5.3 Schema 生成与修订流程

```
DA Product 创建/变更（baseProductId 稳定，version 递增）
    │
    ▼
DA IoT Platform 插件：schema-mapper 生成 mapping_spec 草稿 + Fleets ThingType schema 草稿
    │
    ▼
用户修订（DA UI / JSON 导出）
    │
    ▼
POST Fleets /api/v1/thing-types（Fleets 服务端校验）
    │
    ▼
POST Bridge Admin /api/v1/schema-mappings（唯一写入口）
    │  持久化 mapping_spec + schema_version
    │  同时写入 da_bridge_mappings（每个桥接 Thing）
    ▼
Bridge Schema Registry 热加载（watch PG NOTIFY 或轮询检测 max(updated_at)）
```

**默认自动映射规则**（草稿起点）：

| DA DeviceSpec | Fleets ThingType | 默认策略 |
|---------------|------------------|---------|
| `properties.*` | `schema.properties.*` | 同名映射；用户标记 `writable` |
| `events.*` | `schema.events.info.*` | 默认 severity=info，用户可选修订为 warn/error |
| `commands.*` | `schema.commands.*` (sync) | `parameters` → `input`；`type: sync`；outputData（如有） → `output` |
| writable property | desired/delta 下行 | 默认 `desiredApply.strategy=command`，command 名与 property 同名（用户须修订对齐） |

### 5.4 运行时 Schema 缓存

- 启动时全量加载 `status=active` 的 `da_bridge_schema_mappings`
- 每条 MQTT 消息仅查内存 map：`productId → (SchemaMapping, version)`
- Thing 表上 `schema_version` 决定选用哪个版本的 mapping_spec；旧版本标记 `deprecated` 后仍可服务存量设备
- 热更新：Bridge 定期检测 `da_bridge_schema_mappings` 的 `max(updated_at)` 变化，增量重载
- Schema drift 检测：可选定期调用 DA API 与 Fleets API 做 diff → 告警但**不自动覆盖**

---

## 6. 统一 Topic 路由表

以下为 Bridge Agent 订阅/发布的**完整 Topic 列表**，替代旧方案分散定义的 Rule #1–#6。

### 6.1 Bridge 订阅（Subscribe）

| # | 方向 | MQTT Topic Pattern | 触发动作 |
|---|------|-------------------|---------|
| S1 | 上行 | `v1/+/+/telemetry` | telemetry → Shadow reported（§7.1） |
| S2 | 上行 | `v1/+/+/event` | event → Fleets event（§7.2） |
| S3 | 上行 | `device-agent/+/device/+/responses` | DA response → Fleets command response（§7.3） |
| S4 | 下行 | `$emqx/commands/things/da-+/executions/+/request` | Fleets command → DA command（§8.1） |
| S5 | 下行 | `$emqx/things/da-+/shadow/update` | desired 变化 → DA 命令（§8.2） |
| S6 | 下行 | `$emqx/things/da-+/shadow/update/delta` | delta 变化 → DA 命令（§8.2） |
| S7 | 下行 | `$emqx/things/da-+/jobs/notify` | Jobs 通知 → 代理拉取（§9） |
| S8 | 下行 | `$emqx/things/da-+/jobs/notify-next` | 下一个 Job 变化 → 代理拉取（§9） |

> **关于 Jobs 请求类 topic**（`$emqx/things/da-+/jobs/get`、`jobs/start-next`、`jobs/{id}/update` 等）：原生 Fleets 设备自行 publish 这些 topic，Broker 上 EMQX Rule → ThingDatas API。桥接设备不 publish 这些 topic；由 Bridge 在 **proxy 模式**下通过 **HTTPS ThingDatas API** 代调用，或在 **commandMap 模式**下转化后 publish。因此 Bridge **不需要 subscribe** 这些请求 topic。

### 6.2 Bridge 发布（Publish）

| # | 方向 | MQTT Topic Pattern | 发布方式 | 说明 |
|---|------|-------------------|---------|------|
| P1 | 上行 | `$emqx/things/da-{encPid}--{encDid}/shadow/update` | MQTT client | Shadow reported |
| P2 | 上行 | `$emqx/things/da-{encPid}--{encDid}/events/{eventType}` | MQTT client | Event |
| P3 | 上行 | `$emqx/commands/things/da-{encPid}--{encDid}/executions/{id}/response` | MQTT client (或 REST) | Command response |
| P4 | 下行 | `device-agent/{pid}/device/{did}/commands` | MQTT client | Command 下发 |
| P5 | Jobs 上行 | `$emqx/things/da-{encPid}--{encDid}/jobs/{id}/update` | EMQX REST API | Jobs 状态回写（proxy 模式） |

---

## 7. 上行协议适配（DA → Fleets）

### 7.1 Telemetry → Shadow reported

**订阅**：`v1/{productId}/{deviceId}/telemetry`（S1）

DA payload:
```json
{"type":"state","data":{"temp_a":23.5},"ts":1750000000000}
```

| DA `type` | Bridge 行为 |
|-----------|------------|
| `state` | 转 `$emqx/things/{thingName}/shadow/update`，payload `{state:{reported:{...}}}` |
| `status` | **忽略**（lifecycle 由 EMQX 连接事件处理） |
| 其他 | 按 mapping 决定是否忽略 |

**字段映射**：`data` 经 `mapping_spec.properties` 重命名/类型 coercion 后写入 `reported`。

**示例**：
```
IN  v1/p-sensor/device-001/telemetry
    {"type":"state","data":{"temp_a":23.5},"ts":1750000000000}

OUT $emqx/things/da-p-sensor--device-001/shadow/update
    {"state":{"reported":{"temp_a":23.5}}}
```

后续路径：Fleets 预配 Rule → `POST /thing-datas/shadow/reported/pg` + EMQX Tables（与原生设备一致）。

### 7.2 Event → Fleets events

**订阅**：`v1/{productId}/{deviceId}/event`（S2）

DA payload:
```json
{"type":"event","data":{"event":"overheat","zone":"A","temperature":85.3},"ts":...}
```

转换规则：
- `eventType` 取 `data.event`，`severity` 取自 `mapping_spec.events[].fleetsSeverity`（默认 `"info"`）
- `data` 剔除 `event` 字段，保留 output 字段经 `fieldMap` 映射

```
OUT $emqx/things/da-p-sensor--device-001/events/overheat
    {"eventType":"overheat","severity":"warn","data":{"zone":"A","temperature":85.3}}
```

### 7.3 Command Response → Fleets command response

**订阅**：`device-agent/{productId}/device/{deviceId}/responses`（S3）

DA payload:
```json
{
  "requestId": "550e8400-e29b-41d4-a716-446655440000",
  "code": 0,
  "message": "ok",
  "data": { "estimatedDowntime": 45 }
}
```

Bridge 查 **in-flight 命令表**（PG 持久化）：

| 字段 | 说明 |
|------|------|
| `execution_id` | Fleets command execution UUID |
| `thing_name` | 桥接 Thing 名 |
| `da_product_id` / `da_device_id` | DA 侧标识 |
| `da_request_id` | 下发 DA 时使用的 requestId |
| `fleets_action` | 原始 action |
| `expires_at` | TTL |
| `created_at` | 创建时间 |

**转换**：
- 按 `requestId` 查 in-flight 表 → 取得 `executionId` 与 `thingName`
- `code === 0` → `SUCCEEDED`；否则 `FAILED`
- `result` 经 `commands.{action}.outputMap` 转换
- 发布后在 in-flight 表中标记完成或删除

```
OUT $emqx/commands/things/{thingName}/executions/{executionId}/response
    {"status":"SUCCEEDED","result":{"estimatedDowntime":45},"timestamp":1715432405}
```

> **TTL 清理**：后台 goroutine 每 30s 扫描 `expires_at < NOW()` 的条目 → 发布 `FAILED` 响应到 Fleets → 删除条目。

### 7.4 Telemetry 透传（展望）

远期可为需要高频上报且不适合走 Shadow reported 的 telemetry 类型（如 sensor 数据流），Bridge 可转发到用户自定义 topic 并配合 EMQX 规则写入 `fleets_telemetry`。本期不实现。

---

## 8. 下行协议适配（Fleets → DA）

### 8.1 Command 下行

**订阅**：`$emqx/commands/things/da-+/executions/+/request`（S4）

Fleets JobService 发布（标准 payload）：
```json
{
  "commandId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "action": "setInterval",
  "params": { "interval": 30 },
  "timestamp": 1715432400,
  "ttl": 60
}
```

**Bridge 处理**：

1. 从 topic 解析 `thingName`、`executionId`
2. 查 `da_bridge_mappings` 得 `productId`、`deviceId`
3. 查 `mapping_spec.commands[action]` 得 `daCmd`、`inputMap`
4. `requestId` = `executionId`（直接映射，DA 与 Fleets 均用 UUID）
5. 写入 in-flight 表：`(execution_id, thing_name, product_id, device_id, request_id, fleets_action, expires_at=NOW()+ttl)`
6. 发布 DA commands topic：

```json
{
  "requestId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "cmd": "setInterval",
  "params": { "interval": 30 }
}
```

**解决 `ErrNoMatchingSubscribers`**：Bridge 作为 `$emqx/commands/things/da-+/executions/+/request` 的订阅者，Fleets JobService publish 时 Broker 上存在匹配订阅。Execution 状态机与原生设备完全一致。

**Fleets 控制台/API 路径不变**：`POST /api/v1/commands` → JobService → EMQX publish → Bridge → DA 设备。

```
Sequence:
Fleets UI           Fleets API         JobService         EMQX              Bridge            DA Device
   │                    │                  │                │                  │                  │
   │ POST /commands     │                  │                │                  │                  │
   │───────────────────►│                  │                │                  │                  │
   │                    │ CreateExecution  │                │                  │                  │
   │                    │─────────────────►│                │                  │                  │
   │                    │                  │ Publish req    │                  │                  │
   │                    │                  │───────────────►│ Subscribe match  │                  │
   │                    │                  │                │─────────────────►│                  │
   │                    │                  │                │                  │ in-flight write   │
   │                    │                  │                │                  │ pub to DA cmd    │
   │                    │                  │                │                  │─────────────────►│
   │                    │                  │                │                  │                  │ execute
   │                    │                  │                │                  │◄─────────────────│
   │                    │                  │                │◄─────────────────│ response         │
   │                    │                  │◄───────────────│ Publish response │                  │
   │                    │◄─────────────────│ UpdateExecution │                  │                  │
   │◄───────────────────│ SUCCEEDED        │                │                  │                  │
```

### 8.2 Shadow desired / delta 下行

DA **无 Shadow 协议**。Bridge 将 Fleets Shadow 下行**语义映射**为 DA 命令。

**订阅**：`$emqx/things/da-+/shadow/update`（含 desired）、`$emqx/things/da-+/shadow/update/delta`（S5、S6）

**desired payload 示例**：
```json
{
  "state": {
    "desired": {
      "reportIntervalSec": 60,
      "powerMode": "eco"
    }
  },
  "version": 12
}
```

**Bridge 算法**（per-property `desiredApply`）：

1. 解析 desired 变更（对比 delta payload 或读取 Fleets API 当前 shadow）
2. 对每个变更的 writable 字段查 `mapping_spec.properties[field].desiredApply`
3. 无 `desiredApply` 声明的 writable 字段 → 跳过 + warn 日志
4. `strategy=command` → 生成 DA 命令
5. 合并策略（配置项 `SHADOW_DESIRED_BATCH_MODE`）：

| 值 | 行为 |
|----|------|
| `perField` | 每个字段独立发一条 DA 命令（默认，最安全） |
| `singleCommand` | 所有同 cmd 的字段合并为一条命令的参数 |

**delta payload**：Bridge 仅对 delta 中出现的 writable 键执行 `desiredApply`。

```
示例（perField 模式）：
Fleets desired {reportIntervalSec: 60, powerMode: "eco"}
    → desiredApply[reportIntervalSec] → cmd=setInterval, params={interval: 60}
    → desiredApply[powerMode] → cmd=setPowerMode, params={mode: "eco"}
```

**ThingType 要求**：writable 属性须在 `mapping_spec` 中声明 `desiredApply`，否则 Shadow 下行该字段被跳过。

### 8.3 NTP

DA 设备使用 `device-agent/{pid}/device/{did}/ntp/request|response`。Fleets 无对应标准 topic。Bridge **不处理** NTP（文档标注不支持）。DA 端若需 NTP，仍走既有 NTP 通道。

---

## 9. Jobs 代理层

DA 无 `$emqx/things/.../jobs/*` 实现。Bridge 提供两套 Jobs 适配模式：

### 9.1 模式对比

| mode | 适用场景 | Bridge 行为 | DA 参与度 |
|------|---------|-------------|----------|
| `commandMap` | jobDocument 有固定 operation；适合固件升级等单步作业 | 订阅 notify → 解析 jobDocument.operation → 映射为 DA 命令 → 收到 DA 响应后回写 jobs/{id}/update | DA 被动收命令 |
| `proxy` | DA 设备完全无 Jobs 意识；多步进度上报 | Bridge 完整代跑 Jobs 状态机（调用 ThingDatas API） | DA 不参与 |
| `unsupported` | 明确不支持 | Fleets UI 标记桥接 ThingType Jobs 不可用 | — |

**推荐默认**：`commandMap`（Phase 4），覆盖常见 firmware_update、config_apply 等 operation。

### 9.2 commandMap 模式

```
Fleets JobService
    │ publish notify/notify-next
    ▼
Bridge 订阅（S7/S8）：$emqx/things/da-+/jobs/notify(+)
    │
    ▼
解析 notify payload 中 QUEUED 列表
    │
    ▼
对每个 QUEUED execution 调用 ThingDatas API（HTTPS）
    POST /api/v1/thing-datas/jobs/start-next
    │
    ▼
收到 execution → jobDocument.operation
    → mapping_spec.jobs.operationCommandMap[operation] → daCmd
    │
    ▼
发布 device-agent/{pid}/device/{did}/commands
    │
    ▼
收到 DA response → 构造 jobs/{jobId}/update 请求
    POST /api/v1/thing-datas/jobs/{jobId}/update
    body: {thingName, status: "SUCCEEDED"|"FAILED", statusDetails: {...}}
```

**多步进度**（如 commandMap 不适用）：DA 设备发多条 response 表示中间进度时，Bridge 维护 `jobId → (executionNumber, versionNumber)` 映射，逐次调用 `jobs/{jobId}/update` 带上 `expectedVersion` 做乐观锁。

### 9.3 proxy 模式

Bridge 完全代理设备侧 Jobs 协议，使用 HTTPS ThingDatas API：

1. 收到 notify → `POST /api/v1/thing-datas/jobs/get` → 得到 queuedJobs 列表
2. 对每个 queued job → `POST /api/v1/thing-datas/jobs/start-next`
3. 根据 jobDocument 内容，Bridge 自主执行（无需 DA 参与）或通过 commandMap 委托给 DA
4. 进度/完成 → `POST /api/v1/thing-datas/jobs/{jobId}/update`
5. 乐观锁：维护 `expectedVersion`，遇 `VersionMismatch` 重试

**proxy 模式本质**：Bridge 实现了 Fleets Jobs 协议的设备端状态机，但不通过 MQTT 发送请求（走 HTTPS ThingDatas API）。

### 9.4 Bridge Jobs 订阅与发布（汇总）

| # | Topic | 方向 | 模式 |
|---|-------|------|------|
| S7 | `$emqx/things/da-+/jobs/notify` | sub | commandMap + proxy |
| S8 | `$emqx/things/da-+/jobs/notify-next` | sub | commandMap + proxy |
| — | `POST /api/v1/thing-datas/jobs/get` | HTTPS | proxy |
| — | `POST /api/v1/thing-datas/jobs/start-next` | HTTPS | commandMap + proxy |
| — | `POST /api/v1/thing-datas/jobs/{jobId}/update` | HTTPS | commandMap + proxy |
| P4 | `device-agent/{pid}/device/{did}/commands` | pub (MQTT) | commandMap |
| P5 | `$emqx/things/{name}/jobs/{id}/update` | pub (MQTT) | commandMap（备选，直接 publish MQTT 而非 HTTPS） |

> **HTTPS vs MQTT 选择**：commandMap 模式下 `jobs/start-next` 和 `jobs/update` 走 HTTPS ThingDatas API（与 EMQX Rule → ThingDatas 路径一致）。备选方案：Bridge 直接 publish MQTT `$emqx/things/{name}/jobs/start-next` 等，由既有 EMQX Rule 转发，但需额外处理 `/accepted` / `/rejected` 响应 topic 的订阅，增加了复杂度。首期推荐 HTTPS。

---

## 10. 设备注册与元数据同步

### 10.1 注册流程

```
DA Device 创建（baseProductId、deviceId、metadata.mqttClientId）
    → device-management 注册回调
    → DA IoT Platform (fleets) 插件
        1. 读 device.metadata.mqttClientId（格式 {namespace}/dev/{deviceId}）
        2. 生成 ThingType schema 草稿 + mapping_spec 草稿
        3. 用户修订
        4. POST Fleets /api/v1/thing-types（ThingType 名 = da-product-{baseProductId}）
        5. POST Fleets /api/v1/things
           - name: da-{encBasePid}--{encDid}
           - mqttClientId: device.metadata.mqttClientId
           - tags: [source=device-agent, da:product={baseProductId}, da:device={deviceId}]
        6. POST Bridge Admin /api/v1/schema-mappings（写入 mapping_spec）
        7. Bridge Admin 写 da_bridge_mappings 行
    → Bridge Schema Registry 热加载
```

### 10.2 Thing 命名

| 项 | 格式 | 示例 |
|----|------|------|
| Thing | `da-{encodedBaseProductId}--{encodedDeviceId}` | `da-p-sensor--device-001` |
| ThingType | `da-product-{baseProductId}` | `da-product-p-sensor` |
| Tags | `source=device-agent`, `da:product={baseProductId}`, `da:device={deviceId}` | — |

**注意**：使用 DA 的 `baseProductId`（跨版本稳定）而非带版本的 `productId`。

### 10.3 mqttClientId 格式

DA SDK 默认构造：`{namespace}/dev/{deviceId}`（例：`default/dev/device-001`）。

Fleets lifecycle 匹配依赖此值。DA IoT 插件注册 Thing 时从 `device.metadata.mqttClientId` 或 DA SDK 配置中读取并填入。

**集成方责任**：设备实际连接 EMQX 的 Client ID 须与此值一致。

### 10.4 注册时序

| 场景 | 行为 |
|------|------|
| 先注册后发 telemetry | 正常：Bridge 有映射，上行可达 Fleets |
| 先发 telemetry 后注册 | Bridge 无映射 → 丢弃 + `bridge_uplink_dropped_total{reason=no_mapping}` |
| DA 删设备 | DA 插件 DELETE Fleets Thing → Bridge Admin 删映射 → Bridge 缓存失效 |
| Fleets 删 Thing | DA 插件 reconcile；Bridge 下行因无映射自然停止 |

### 10.5 元数据 API 同步

Bridge 定期或在 Admin 触发下：

| API | 用途 |
|-----|------|
| Fleets `GET /api/v1/things?tagName=source=device-agent` | 全量 reconcile 映射 |
| Fleets `GET /api/v1/thing-types/{name}` | 校验 schema 一致性 |
| DA `GET /api/products/{baseProductId}` | 拉取最新 Product（properties/commands/events） |
| DA `GET /api/products/{baseProductId}/devices/{deviceId}` | 设备 metadata（mqttClientId 等） |

> **DA API 路径校正**：DA 实际路径不含 `/v1` 前缀；devices 在 products 下嵌套。

---

## 11. Fleets 与 Device Agent 侧协作

### 11.1 Fleets 主进程改动（最小集）

| 改动 | 说明 |
|------|------|
| `da_bridge_*` 表 migration | 纳入 Fleets `migrations/` 目录 |
| **移除** EMQX da-bridge Rule 注入 | 新方案不再依赖旧方案 BridgePlugin + Rule Manager |
| docker-compose / 安装脚本 | 增加 `da-bridge` 服务定义 |
| 文档 | 桥接设备能力边界、Jobs mode 说明 |

**明确不改动**：`JobService`、`ShadowService`、ThingDatas Handler、Fleets 预配 EMQX 规则、ThingService（旧方案的 BridgeHooks 不再需要）。

### 11.2 Device Agent IoT Platform 插件

保留 [旧方案 §6](./device-agent-fleets-integration.md#6-device-agent-端-iot-platform-插件设计) 的框架定位，调整：

| 变更 | 说明 |
|------|------|
| `schema-mapper.ts` 输出 | 同时生成 Fleets ThingType schema + `mapping_spec` JSON |
| 注册 API | POST Fleets Thing/ThingType + POST Bridge Admin schema-mappings |
| MQTT | 插件**仍不订阅** MQTT |
| namespace | 注册时携带 DA namespace（存 `da_bridge_mappings.da_namespace`） |
| baseProductId | ThingType 名使用 `baseProductId` 而非 `productId` |

### 11.3 能力矩阵（用户可见）

| 能力 | 桥接设备（新方案） |
|------|-------------------|
| Shadow reported 上行 | ✅ |
| Event 上行 | ✅ |
| Lifecycle 在线状态 | ✅ |
| Fleets 控制台 sync 命令 | ✅ |
| Shadow desired/delta 下行 | ✅（映射为 DA 命令） |
| Fleets Jobs | ⚙️ `commandMap` / `proxy` 可配置 |
| 自定义高频 telemetry | 🔜 远期 |

---

## 12. 认证与安全

### 12.1 认证架构

```
Bridge Agent ──MQTT Auth──► EMQX Broker（独立服务账号 ACL）
Bridge Agent ──EMQX_API_KEY/SECRET──► EMQX REST API
Bridge Agent ──Basic Auth──► Fleets REST API（读 shadow/thing/type；Jobs proxy 写 thing-datas）
Bridge Agent ──Bearer/Internal──► Device Agent REST API
DA IoT 插件 ──Basic Auth──► Fleets REST API（注册）
DA 设备 ──MQTT Auth──► EMQX（既有策略，不经 Bridge）
```

### 12.2 Bridge MQTT ACL 建议

| 方向 | Topic 模式 | 权限 |
|------|-----------|------|
| Subscribe | `v1/+/+/telemetry`, `v1/+/+/event` | allow |
| Subscribe | `device-agent/+/device/+/responses` | allow |
| Subscribe | `$emqx/commands/things/da-+/executions/+/request` | allow |
| Subscribe | `$emqx/things/da-+/shadow/#` | allow |
| Subscribe | `$emqx/things/da-+/jobs/notify`, `$emqx/things/da-+/jobs/notify-next` | allow |
| Publish | `$emqx/things/da-+/shadow/update` | allow |
| Publish | `$emqx/things/da-+/events/+` | allow |
| Publish | `$emqx/commands/things/da-+/executions/+/response` | allow |
| Publish | `device-agent/+/device/+/commands` | allow |
| Publish | `$emqx/things/da-+/jobs/+/update` | allow（proxy 模式 HTTPS 则不需要） |

### 12.3 API Key 最小权限

**Bridge → Fleets**：

```json
{
  "permissions": {
    "GET": ["/api/v1/things", "/api/v1/thing-types", "/api/v1/things/*/shadow"],
    "POST": ["/api/v1/thing-datas/jobs/*"]
  }
}
```

**DA IoT 插件 → Fleets**：Thing/ThingType CRUD（与旧方案 §7.2 相同）。

### 12.4 安全考量

- In-flight 命令表防 replay：`requestId` 一次性、TTL 过期自动清理 + 发布 FAILED
- 映射表写入口：仅 Bridge Admin API + DA 插件；Bridge runtime 只读
- 多租户：一 Bridge 实例对应一 Fleets + 一 EMQX + 一 DA namespace（首期不跨 namespace）

---

## 13. 部署与生命周期

### 13.1 环境变量

```bash
# 总开关
DA_BRIDGE_ENABLED=true
DA_BRIDGE_INSTANCE_ID=da-bridge-1

# MQTT
DA_BRIDGE_MQTT_URL=mqtt://emqx:1883
DA_BRIDGE_MQTT_CLIENT_ID=da-fleets-bridge-1
DA_BRIDGE_MQTT_USERNAME=da-bridge-svc
DA_BRIDGE_MQTT_PASSWORD=...

# EMQX REST（复用 Fleets）
EMQX_API_URL=http://emqx:18083
EMQX_API_KEY=...
EMQX_API_SECRET=...

# PostgreSQL（与 Fleets 同库）
DATABASE_URL=postgres://postgres:postgres@postgres:5432/fleets

# Fleets API（Bridge 读 shadow / Jobs proxy）
FLEETS_API_URL=http://fleets:8080
FLEETS_API_KEY=...
FLEETS_API_SECRET=...

# Device Agent API
DA_API_URL=http://device-agent:3000
DA_API_TOKEN=<long-lived token>

# 行为 tuning
DA_BRIDGE_SHADOW_DESIRED_BATCH_MODE=perField       # perField | singleCommand
DA_BRIDGE_JOBS_MODE=commandMap                      # proxy | commandMap | unsupported
DA_BRIDGE_SCHEMA_SYNC_INTERVAL=60s
DA_BRIDGE_INFLIGHT_TTL=120s
DA_BRIDGE_INFLIGHT_CLEANUP_INTERVAL=30s
DA_BRIDGE_HTTP_PORT=8091
DA_BRIDGE_SHUTDOWN_TIMEOUT=30s

# 日志
LOG_LEVEL=info
```

### 13.2 docker-compose 片段

```yaml
services:
  fleets:
    image: fleets:latest

  da-bridge:
    image: fleets-da-bridge:latest
    depends_on:
      - fleets
      - emqx
      - postgres
    environment:
      DA_BRIDGE_ENABLED: "true"
      DATABASE_URL: postgres://postgres:postgres@postgres:5432/fleets
      DA_BRIDGE_MQTT_URL: mqtt://emqx:1883
      EMQX_API_URL: http://emqx:18083
      FLEETS_API_URL: http://fleets:8080
    ports:
      - "8091:8091"
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "wget", "-qO-", "http://localhost:8091/ready"]
      interval: 10s
      timeout: 3s
      retries: 3
```

### 13.3 启动顺序

```
1. PostgreSQL / EMQX Tables
2. EMQX Broker（含 Fleets 预配规则）
3. Fleets HTTP 主进程
4. Bridge Agent（migrate → connect MQTT → subscribe → ready）
5. Device Agent（IoT 插件 start → 注册 ThingType/Thing → 写映射表）
6. DA 设备连接 EMQX 开始上报
```

---

## 14. 高可用与可观测性

### 14.1 多实例 HA

Bridge Agent 可多副本运行，但同一 MQTT clientId 不能并发连接。

| 策略 | 说明 |
|------|------|
| **Active-Standby**（首期） | `pg_try_advisory_lock(0xDABRIDGE)` 选主；主实例持有 MQTT 连接；备实例仅 `/health` + 就绪接管 |

**锁 ID**：`HashToInt64("da-fleets-bridge")`，避免与 Fleets 其他 advisory lock 冲突。

**MQTT clientId**：Active 实例使用固定 `clientId`；Standby 实例不连 MQTT。Failover 时 Standby 获取锁后连接 MQTT，依赖 `cleanSession=false` 恢复订阅与积压消息。

### 14.2 指标（Prometheus）

| 指标 | 说明 |
|------|------|
| `bridge_mqtt_connected` | MQTT 连接状态 gauge |
| `bridge_messages_total{direction,type}` | 上下行消息计数 counter |
| `bridge_translate_errors_total{reason}` | 转换失败 counter |
| `bridge_uplink_dropped_total{reason}` | 丢弃原因（no_mapping, no_schema 等） |
| `bridge_inflight_commands` | 当前 in-flight 命令数 gauge |
| `bridge_inflight_expired_total` | TTL 过期清理数 counter |
| `bridge_translate_latency_seconds` | 处理延迟 histogram |

### 14.3 日志与追踪

- 每条消息：`trace_id`、`topic_in`、`topic_out`、`thing_name`、`product_id`、`device_id`、`latency_ms`
- 命令全链路：`execution_id ↔ request_id` 关联
- 使用 `log/slog`（与 Fleets 一致），JSON 格式

---

## 15. 与旧方案对比

| 维度 | 旧方案 v2.5 | 新方案 Bridge Agent |
|------|------------|---------------------|
| 复杂度分布 | Broker Rule SQL + 薄插件 | 独立进程 + 零 Fleets 核心改动 |
| 上行 | Rule republish | Bridge MQTT sub → pub |
| 下行 | 不支持（§12） | 原生支持 Command + Shadow + Jobs |
| Schema | 注册时转换 | 注册 + 运行时 mapping_spec + 多版本并存 |
| Fleets 侵入 | BridgePlugin + Rule Manager + ThingService hooks | 仅 Migration + 编排 Sidecar |
| EMQX 依赖 | 高（Rule SQL 版本） | 低（仅预配规则；Go 逻辑替代 SQL） |
| 运维 | Rule 漂移 | Bridge 版本 + Schema 版本独立演进 |
| 延迟 | Broker 内 republish ~1ms | Bridge 多一跳 ~5-10ms（可接受） |
| ErrNoMatchingSubscribers | 阻塞性缺陷 | 彻底解决（Bridge 订阅） |

**迁移路径**（若旧方案已 PoC）：

1. 部署 Bridge Agent，启用上行
2. 禁用 `da-bridge-*` EMQX 规则（`DA_BRIDGE_RULES_*=false` 或删除）
3. 验证 Shadow reported / Event 与旧 Rule 等价
4. 启用下行 Command → Shadow → Jobs 分阶段

---

## 16. 实施路线图

### Phase 1：Bridge 骨架 + 上行（2–3 周）

| 任务 | 说明 |
|------|------|
| `cmd/da-bridge` 项目骨架 + 生命周期 | Start/Stop/Ready |
| MQTT client + unified topic router | §6 subscribe S1–S3、publish P1–P2 |
| `da_bridge_mappings` + `da_bridge_schema_mappings` migration | PG 表 |
| Telemetry / Event translator | §7.1, §7.2（含 severity 映射） |
| docker-compose 集成 | 与 Fleets 联调 |
| E2E | DA 注册 → telemetry → Fleets Shadow reported + Event |

### Phase 2：Command 双向（2–3 周）

| 任务 | 说明 |
|------|------|
| In-flight 命令表（PG）+ TTL 清理 goroutine | §7.3 |
| Command 下行 + response 上行 translator | §8.1, §7.3 |
| Command correlation（executionId ↔ requestId） | In-flight 查表 |
| DA IoT 插件输出 mapping_spec（commands 部分） | 与 ThingType 同步注册 |
| Bridge Admin API（schema-mappings CRUD） | POST /api/v1/schema-mappings |
| E2E | Fleets POST /commands → DA 执行 → SUCCEEDED |

### Phase 3：Shadow 下行（1–2 周）

| 任务 | 说明 |
|------|------|
| Per-property desiredApply translator | §8.2 |
| SHADOW_DESIRED_BATCH_MODE | perField / singleCommand |
| E2E | 控制台改 desired → DA 收到命令 → reported 更新 |

### Phase 4：Jobs 代理（2 周）

| 任务 | 说明 |
|------|------|
| jobs.mode=commandMap | notify 订阅 → start-next → DA 命令 → update |
| jobs.mode=proxy | ThingDatas HTTPS API 代调用 |
| 文档与 UI 能力标注 | |
| E2E | Fleets 创建 Job → Bridge 代理 → DA 执行 → Job SUCCEEDED |

### Phase 5：生产化

HA Active-Standby（advisory lock + MQTT failover）、监控告警、映射 resync Admin 完整实现、性能压测、升级回滚文档。

---

## 17. 附录

### A. Topic 对照速查表

| 方向 | DA Topic | Fleets Topic |
|------|----------|--------------|
| 状态上行 | `v1/{pid}/{did}/telemetry` | `$emqx/things/da-{encPid}--{encDid}/shadow/update` |
| 事件上行 | `v1/{pid}/{did}/event` | `$emqx/things/da-{encPid}--{encDid}/events/{eventType}` |
| 命令响应 | `device-agent/{pid}/device/{did}/responses` | `$emqx/commands/things/da-.../executions/{id}/response` |
| 命令下行 | `device-agent/{pid}/device/{did}/commands` | `$emqx/commands/things/da-.../executions/{id}/request` |
| Shadow 下行 | ↑ commands（合成） | `$emqx/things/da-.../shadow/update` |
| Jobs | ↑ commands 或 Bridge 代发 | `$emqx/things/da-.../jobs/*` |

### B. encode/decode（与旧方案兼容）

```go
// encodeBridgeSegment / decodeBridgeSegment
// '/' → '_slash_', '--' → '_dashdash_'
// 先 encode '--' 再 '/'，保证双向对称
```

### C. DA REST API 参考

| 用途 | 实际路径 | 说明 |
|------|---------|------|
| 获取 Product | `GET /api/products/{baseProductId}` | 不含 `/v1` 前缀 |
| 获取 Device | `GET /api/products/{baseProductId}/devices/{deviceId}` | Device 嵌套在 product 下 |
| Product 列表 | `GET /api/products` | 可选 `?lifecycleStatus=published` |

### D. 版本记录

| 版本 | 日期 | 变更 |
|------|------|------|
| v1.0 | 2026-07-02 | 初稿：独立 Bridge Agent 进程方案 |
| **v1.1** | **2026-07-02** | 评审优化：统一 Topic 路由表、per-property desiredApply、in-flight PG 持久化、DA API 路径校正、ThingType 使用 baseProductId、移除旧方案 BridgePlugin、Jobs 双模式详化、command 序列图 |

---

## 18. 待定问题

以下问题需要进一步确认或决策后方可进入 Phase 1 开发。

### 18.1 阻塞项（Phase 1 前必须定案）

| # | 问题 | 背景 | 倾向建议 |
|---|------|------|---------|
| **Q1** | **Bridge Agent 仓库位置** | 同仓库 `fleets/cmd/da-bridge` 共享 `internal/emqx` 包 vs 独立 repo `emqx/da-fleets-bridge` | 同仓库，降低依赖管理成本 |
| **Q2** | **`da_bridge_*` 表归属** | Fleets migration 纳入 vs Bridge 独立 migration | Fleets 仓库纳入，Bridge 进程做 migrate |
| **Q3** | **`mqttClientId` 稳定性** | DA SDK 默认 `{namespace}/dev/{deviceId}` 较为稳定，但 multi-namespace 部署时 namespace 可能变化；lifecycle 依赖精确匹配 | 建议 DA IoT 插件注册时从 `device.metadata.mqttClientId` 读取；若不存在则用 `{namespace}/dev/{deviceId}` 自动生成并回写 metadata |
| **Q4** | **ThingType 用户修订 UI** | 旧方案 §13.1 B3：DA Web 尚无 Fleets 集成编辑入口 | Phase 1 使用 JSON 导出/手工编辑 + Bridge Admin 写入；Phase 2 做 Workspace 设置页 |
| **Q5** | **Bridge Admin API 认证** | POST `/api/v1/schema-mappings` 的调用方（DA IoT 插件）如何认证？ | 使用 Fleets API Key（与 DA 插件相同），或独立 Internal Token |

### 18.2 架构决策

| # | 问题 | 背景 | 倾向建议 |
|---|------|------|---------|
| **Q6** | **多 Product 版本 vs ThingType** | DA Product 有 `version`，`baseProductId` 稳定。版本升级时：默认 PUT 更新同 ThingType？还是创建新 ThingType？ | 默认 PUT 更新同一 ThingType（schema 后向兼容）；用户手动指定新 ThingType 名可隔离版本 |
| **Q7** | **Shadow desired 合并下发** | `perField` 逐命令 vs `singleCommand` 合并，后者可能产生不存在的组合命令 | 默认 `perField`；`singleCommand` 需在 mapping_spec 中显式声明合并命令 |
| **Q8** | **Jobs proxy 模式是否调 ThingDatas MQTT 路径还是 HTTPS** | MQTT 路径需 subscribe `/accepted`/`/rejected` 响应 topic，状态机更复杂 | 推荐 HTTPS ThingDatas API（简单、与 EMQX Rule 路径一致） |
| **Q9** | **Bridge 是否处理未注册设备的 telemetry** | 当前设计：无映射 → 丢弃 + metric。Greptime 侧可能漏数据（因不经 PG 校验）。是否需要为未注册设备缓存并延迟投递？ | 丢弃 + metric（简单可靠）；注册时序问题由 DA 插件保障"先注册后启用上报" |

### 18.3 Device Agent 侧

| # | 问题 | 背景 | 倾向建议 |
|---|------|------|---------|
| **Q10** | **IoT 插件回调挂载点** | `registerDeviceWithResult` 尚无 platform 回调可供 fleets 插件挂钩 | 在 `device-management` 增加 `onDeviceRegistered` / `onProductChanged` 回调；失败不阻断 DA 设备注册 |
| **Q11** | **DA 插件是否直连 PG 写 mapping_spec** | 当前方案：POST Bridge Admin API。若 DA 网络不可达 Bridge？ | 仅 Bridge Admin API（单写入口）；DA 不可达时入 retry queue |
| **Q12** | **DA API 是否暴露 device metadata 中的 mqttClientId** | DA `DeviceInfo.metadata` 是 `Record<string, any>`；插件能否可靠获取 mqttClientId？ | 注册时主动要求 `metadata.mqttClientId` 非空；空则跳过注册 + warn |

### 18.4 远期预决策

| # | 问题 | 说明 |
|---|------|------|
| **Q13** | **Event severity 动态映射** | mapping_spec 已支持 per-event severity，可实现 DA `eventType` → Fleets severity 的动态映射（alert→warn, error→error），无需 EMQX Rule SQL 改动 |
| **Q14** | **Bridge namespace 多实例隔离** | 首期一 Bridge 一 namespace；远期是否支持一 Bridge 多 namespace（需 topic 路由感知 namespace）？ |
| **Q15** | **自定义 telemetry 高频通道** | DA 高频 sensor 数据走 Shadow reported 不合适；远期 Bridge 可转发到自定义 topic + `fleets_telemetry` |

---

> **文档维护者**: 集成团队
> **相关文档**: [旧方案 v2.5](./device-agent-fleets-integration.md) · Fleets [README](https://github.com/emqx/fleets/blob/main/README.md) · Device Agent [CLAUDE.md](https://github.com/emqx/device-agent/blob/main/CLAUDE.md)
