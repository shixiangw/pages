# Device Agent × Fleets 集成方案（Bridge Agent 独立进程）

> **版本:** v2.1
> **日期:** 2026-07-03
> **状态:** 评审优化版（基于 Fleets AGENTS.md + DA CLAUDE.md 双代码库验证后修订）
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
18. [待定问题与不确定性](#18-待定问题与不确定性)

---

## 1. 背景与动机

[v2.5 旧方案](./device-agent-fleets-integration.md) 以 **EMQX Rule Engine `republish`** 为核心，在 Broker 内完成 DA topic/payload 与 Fleets topic/payload 的转换。该方案适合已实现的**上行**能力，但在以下方面存在结构性不足：

| # | 问题 | 说明 |
|---|------|------|
| P1 | **下行能力缺失** | Command、Shadow desired/delta、Jobs 远期 TODO；Rule republish 难以闭环 |
| P2 | **Rule 无法承载复杂语义** | Shadow desired → DA 命令、writable 字段映射、Jobs 请求/响应状态机、命令 correlation 等需要**有状态逻辑 + Schema 查表**，EMQX Rule SQL 表达力不足 |
| P3 | **DeviceSpec → ThingType 转换受限** | ThingType schema 需在注册阶段人工修订并通过 Fleets API 校验；Rule 无法在运行时引用映射表做 payload 字段级转换 |
| P4 | **无法对齐 Fleets 标准设备步骤** | Fleets 原生设备订阅 `$emqx/commands/.../request`、`shadow/update/delta`、`jobs/*`；DA 设备只订阅 `device-agent/.../commands`。纯 Rule 下行导致 `ErrNoMatchingSubscribers` |
| P5 | **运维与版本治理困难** | Rule SQL 与 Product schema 变更耦合；无法热更新映射 |

**新方案核心思路**：在 Fleets 部署侧启动一个**独立的 Bridge Agent 进程**，通过 **MQTT 原生 subscribe/publish** 双向接入 Broker，在 MQTT 层完成协议适配；必要时调用 **Fleets REST API** 与 **Device Agent REST API** 补齐 Schema、映射与元数据。

> Fleets **核心 HTTP 服务仍不订阅 MQTT**（架构约束不变）。MQTT 订阅职责由 Bridge Agent 这一**独立 Sidecar 进程**承担。

### 1.1 代码库验证摘要

本轮基于 Fleets Go 代码和 DA TypeScript 代码的**双代码库交叉验证**，确认以下关键事实：

| 验证项 | 结论 | 影响 |
|--------|------|------|
| DA REST API 无 `/v1` 前缀 | 所有路径在 `/api/` 下 | 方案中 API 路径引用正确 |
| DA 设备表有 `metadata` JSONB | 可用于存储 `mqttClientId` | PC3 方案可行 |
| DA 设备表**无** `mqttClientId` 字段 | 需通过 metadata 约定 | 与方案一致 |
| DA 无 `baseProductId` 概念 | ProductInfo 仅含 `id` | §5.1 mapping 表需重新设计 |
| DA MQTT topic 首段为 `userId`（非 `productId`）| 代码使用 `userId` | §6 路由 key 需对齐 |
| DA 命令响应字段为 `msg`（非 `message`）| 方案写 `message` | §7.3 示例须修正 |
| DA 无设备注册回调 | `DeviceService` 无 EventEmitter | PC2 属实；需 fallback 轮询 |
| DA `platforms/` 目录不存在 | 需新建 | PC1 属实 |
| Fleets 权限使用 `strings.HasPrefix`（非 `*` 通配）| 方案使用 `*` | §12.3 权限配置需修正 |
| Fleets 仅 publish MQTT（不 subscribe） | 代码无误 | 架构约束一致 |
| Fleets Thing 名仅约束 NOT NULL + UNIQUE + 255 字符 | 无格式限制 | `da-` 前缀可行 |
| Fleets `ThingJob.Document` 为 `json.RawMessage` | 无结构化 `operation` 字段 | §9 需约定 Document schema |
| Fleets ShadowService 通过 EMQX V5 REST API 发布 desired/delta | 格式见 §17.C | §8.2 需对齐实际 payload |

### 1.2 前置条件与已知缺口

| # | 前置项 | 说明 | 负责方 | 代码库验证 |
|---|--------|------|--------|-----------|
| **PC1** | **DA IoT Platform 插件开发** | 在 `apps/agent-gateway/src/` 下新建 `platforms/fleets/` 插件模块（DA 当前无 `platforms/` 目录）。实现 ThingType/Thing 注册、schema 草稿生成、mapping_spec 输出。 | **Device Agent 团队** | 已确认不存在 |
| **PC2** | **DA 设备注册回调机制** | DA `packages/device-management/src/service.ts` 当前无 `onDeviceRegistered` / `onProductChanged` 等事件回调。需新增事件发射；**若短期无法实现，Bridge 侧以轮询 fallback 兜底**。 | **Device Agent 团队** | 已确认不存在 |
| **PC3** | **设备 MQTT Client ID 约定** | DA Device 模型无 `mqttClientId` 字段。方案约定 Client ID 格式 `{namespace}/dev/{deviceId}`，通过 `device.metadata.mqttClientId` 存储。 | **集成方 + Device Agent 团队** | metadata 可用 |
| **PC4** | **Bridge Agent 仓库与迁移** | 独立仓库 `emqx/da-fleets-bridge`。Bridge 自行管理 `da_bridge_*` 表 Migration，Fleets 核心不感知。 | **Fleets 团队** | 待确认 |
| **PC5** | **设备实际直连 EMQX Broker** | DA 管理的设备须**直连 EMQX Broker**，使用 PC3 约定的 Client ID 连接并使用 §6 的 MQTT topics 上报数据。 | **集成方** | 待确认 |
| **PC6** | **Fleets「不订阅 MQTT」架构约束例外** | Fleets AGENTS.md §0 规定「Fleets **NEVER** subscribes to any MQTT topic」。Bridge 必须订阅 MQTT。明确为**独立仓库 Sidecar**，Fleets 核心零侵入，不违反约束。 | **Fleets 架构团队** | 待确认 |
| **PC7** | **DA topic 首段语义对齐** | DA 代码中 MQTT topic 首段为 `userId`（DA 用户/租户标识），文档称为 `productId`。Bridge 映射表中 `da_user_id` 须明确：是按 topic 提取的值（实际是 `userId`），还是按 DA Product `id` 字段。 | **Device Agent 团队** | 新发现 |
| **PC8** | **EMQX ACL 配置** | Bridge MQTT 账号需获得 `$emqx/` 系统主题的 publish/subscribe 权限（默认 EMQX 可能禁止客户端访问 `$emqx/`）。 | **运维/EMQX 团队** | 待确认 |

### 1.3 与 Fleets 架构约束的关键张力

Fleets AGENTS.md 对 MQTT 订阅有严格约束：

> Fleets **NEVER** subscribes to any MQTT topic. All device->cloud data enters via EMQX Rule Engine or HTTPS.

本方案引入的 Bridge Agent 为了完成 Command、Shadow desired/delta、Jobs notify 的下行适配，**必须**作为 MQTT subscriber 接入 Broker。**解决方案**：

- **独立仓库 `emqx/da-fleets-bridge`**。Fleets 核心代码不引入任何 MQTT 订阅逻辑，仅通过 REST API 与 Bridge 交互（同设备与 Fleets 的交互方式）。严格保持「Fleets 核心不订阅 MQTT」的架构纯度。
- Bridge 作为 Fleets 部署拓扑中的 **Sidecar**，与 Fleets 共享 PostgreSQL（可选）和 EMQX Broker，但代码独立、发布独立、生命周期独立。

> **评审建议**：Phase 1 启动前由 Fleets 架构团队确认独立仓库方案。Fleets 侧改动限于：docker-compose 编排引用、文档更新；**无代码改动**。

---

## 2. 设计目标与原则

### 2.1 目标

| 目标 | 说明 |
|------|------|
| **双向互通** | 上行（telemetry/event/command response/jobs 上报）+ 下行（command/shadow/jobs 通知与代理）一期设计到位 |
| **Fleets 标准路径对齐** | Bridge 对 Fleets 侧仍发布/订阅标准 `$emqx/` 主题；Fleets 预配 EMQX Rule → ThingDatas API 路径**不变** |
| **Schema 可治理** | DeviceSpec ↔ ThingType 映射持久化、版本化，支持人工修订 |
| **零侵入 Fleets 核心** | Fleets 主进程**不修改任何代码**；JobService / ShadowService / ThingDatas Handler **不修改** |
| **可选启用** | Bridge Agent 与 DA IoT Platform 插件均可独立开关 |
| **可观测** | 结构化日志（`log/slog`，与 Fleets 一致）、Prometheus 指标、健康检查、trace_id 全链路 |

### 2.2 原则

| 原则 | 说明 |
|------|------|
| **Bridge 专职 MQTT 适配** | 所有 DA↔Fleets topic/payload 转换在 Bridge 内完成，不依赖 EMQX Rule republish |
| **Fleets 预配规则照旧** | Bridge 向 `$emqx/things/{name}/...` 发布后，仍由 Fleets 既有 Rule 写入 PG / EMQX Tables |
| **DA 主动注册** | Device Agent IoT Platform 插件仍是 Fleets Thing/ThingType 注册的唯一发起方 |
| **映射表为运行时真相源** | `da_bridge_*` 表是 Bridge 运行时的唯一真相源；写入统一走 Bridge Admin API（Bridge runtime 只读） |
| **Fail-safe** | 映射缺失时丢弃并告警，不伪造 payload |
| **QoS 1 + MQTT 5 NoLocal** | 关键上下行 topic 统一使用 QoS 1；MQTT 5 客户端启用 `NoLocal=true` 避免自发布回环 |
| **per-thing advisory lock** | Bridge 处理 per-thing 变更前须获取 `HashToInt64("da-bridge-" + thingName)` 锁 |
| **per-thing 串行处理** | 对同一 thing 的变更类消息（command、shadow desired、jobs）必须串行处理，避免状态竞争 |

---

## 3. 架构总览

### 3.1 整体架构图

```
                         ┌─────────────────────────────────────────────┐
                         │              EMQX Broker                     │
                         │                                              │
    DA 主题域              │   v1/{uid}/{did}/telemetry|event            │
    device-agent/...       │   device-agent/{uid}/device/{did}/...       │
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
               │ • Schema Mapper         │
              │ • Topic Router          │
              │ • Protocol Translators  │
              │ • In-Flight Command PG  │
              │ • HTTP /health /metrics │
              └───────────┬─────────────┘
                          │
          ┌───────────────┼───────────────┐
          │ REST          │ REST          │
          ▼               ▼               ▼
   ┌─────────────┐ ┌─────────────┐ ┌─────────────────┐
   │ Fleets API  │ │ DA API      │ │ PostgreSQL      │
   │ Thing/Type  │ │ Products/   │ │ da_bridge_*     │
   │ Shadow/Cmd  │ │ Devices     │ │ (Bridge 自有)   │
   └─────────────┘ └─────────────┘ └─────────────────┘

┌──────────────────────────────────────────────────────────────────────┐
│ Fleets 实例（HTTP 主进程，不订阅 MQTT）                                │
│  • ThingService / JobService / ShadowService — 标准行为不变           │
│  • docker-compose / systemd 编排 Bridge Agent 生命周期                │
│  • Fleets 核心代码零改动                                               │
└──────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────┐
│ Device Agent 实例                                                     │
│  • IoT Platform 插件（platforms/fleets/）：ThingType/Thing 注册      │
│  • 不订阅 MQTT（Bridge 负责 DA↔Fleets 转换）                          │
└──────────────────────────────────────────────────────────────────────┘
```

### 3.2 与旧方案的关键差异

| 维度 | 旧方案（v2.5 Rule） | 新方案（Bridge Agent） |
|------|---------------------|------------------------|
| 协议转换位置 | EMQX Rule SQL `republish` | Bridge Agent 进程内 Go 逻辑 |
| MQTT 订阅者 | 无（Fleets 不订阅） | Bridge Agent 独立订阅 |
| 下行 Command | §12 远期 | Bridge 订阅 → 发布 DA commands |
| Shadow 下行 | §12 远期 | Bridge 订阅 desired/delta → per-property 映射 |
| Jobs | §12 明确不支持 | Bridge Jobs 代理层（commandMap + proxy） |
| Schema 转换 | DA 插件注册时一次性 | 映射表 + 运行时查表 + 版本化 |
| Fleets 改动 | BridgePlugin + Rule Manager | **零代码改动**；仅编排 + 文档 |

### 3.3 核心数据流一览

| 方向 | DA 侧 | Bridge 动作 | Fleets 侧 | 存储 |
|------|-------|------------|-----------|------|
| 状态上行 | `v1/{uid}/{did}/telemetry` (type=state) | 转 `$emqx/things/{name}/shadow/update` (reported) | 预配 Rule → ThingDatas | PG + EMQX Tables |
| 事件上行 | `v1/{uid}/{did}/event` | 转 `$emqx/things/{name}/events/{eventType}` | 预配 Rule → EMQX Tables | EMQX Tables |
| 命令响应上行 | `device-agent/{uid}/device/{did}/responses` | 转 `$emqx/commands/.../response` | 预配 Rule → ThingDatas | PG |
| 命令下行 | `device-agent/{uid}/device/{did}/commands` | 订阅 Fleets request → 发布 DA commands | JobService EMQX publish | PG |
| Shadow 下行 | DA commands（合成） | 订阅 desired/delta → 按 mapping_spec 发 DA 命令 | ShadowService EMQX publish | PG |
| Jobs 下行 | DA commands / 代理请求 | 订阅 notify → commandMap 转发或 proxy 代跑 | JobService EMQX publish | PG |
| Jobs 上行 | DA responses / Bridge 代发 | 转 `$emqx/things/{name}/jobs/{jobId}/update` 等 | 预配 Rule → ThingDatas | PG |
| Lifecycle | EMQX connect/disconnect | **不经 Bridge** | 预配 Rule → ThingDatas | PG |

> **注**：表中 `{uid}` 指 DA 侧 topic 首段，在 DA 代码中为 `userId`（不是 `productId`）。Bridge 的 topic 订阅通配符 `+` 可提取此值，但其语义是 DA 用户/租户标识。

> **上行路径重要说明**：Bridge 上行**统一走 MQTT publish 到标准 `$emqx/` 主题**，由 Fleets 预配 EMQX 规则处理。Bridge**不直接调用** ThingDatas API 做上行写入，确保 Fleets 侧数据入口单一、规则统一。

---

## 4. Bridge Agent 进程设计

### 4.1 定位与边界

**Bridge Agent** 是 Fleets 部署拓扑中的**独立 Sidecar 服务**（独立仓库 `emqx/da-fleets-bridge`）。

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
│   ├── client.go            # MQTT 客户端（subscribe + publish）；推荐 eclipse/paho.mqtt.golang
│   └── subscription.go      # 精确订阅管理（动态增删）
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
│   └── migration.go         # golang-migrate 驱动（Bridge 自有）
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
                            │ Load config → DB migrate → HA advisory lock
                            │ Load schema registry → Connect MQTT → Batch subscribe
                           ▼
                    ┌─────────────┐
         ┌─────────│    Ready    │◄────────┐
         │         └──────┬──────┘         │
         │                │                │
         │                ▼                │
         │         ┌─────────────┐         │
         │         │   Running   │─────────┤
         │         └──────┬──────┘         │ Error threshold
         │    SIGTERM     │                ▼
         │                ▼          ┌─────────────┐
         └───────────────│ Draining  │  Degraded   │
                         └──────┬───┘  (告警仍服务)│
                                │                 │
                                ▼                 │
                         ┌─────────────┐          │
                         │   Stopped   │◄─────────┘
                         └─────────────┘
```

| 阶段 | 行为 |
|------|------|
| **Start** | 执行 DB migration → 连接 PostgreSQL → 抢全局 HA advisory lock（`HashToInt64("da-fleets-bridge")`）→ 加载映射表与 Schema 缓存 → 建立 MQTT 连接（cleanSession=false, QoS 1, MQTT 5 NoLocal）→ 按 `da_bridge_mappings` 构造**精确 topic 列表**批量 subscribe → 注册 message handler → 启动 in-flight TTL 清理 goroutine → 暴露 `/health` `/metrics` |
| **Per-Thing Mutation** | 处理命令下发、Jobs update、Shadow desired 映射等 per-thing 变更前，须先获取 per-thing advisory lock（`HashToInt64("da-bridge-" + thingName)`） |
| **Running** | 处理 MQTT 消息；定期增量同步 Schema；清理过期 in-flight 命令 |
| **Draining** | SIGTERM：停止消息处理 → 等待 in-flight 完成（超时 `SHUTDOWN_TIMEOUT`）→ 断开 MQTT → 关闭 DB → 释放锁 |
| **Ready** | MQTT 已连接 + 全部 topic 已 subscribe + DB 可达 + advisory lock 持有 |
| **Live** | 进程存活 + MQTT reconnecting 算 live |

> **动态订阅更新**：当 Admin API 写入新 mapping 后，Bridge 应在运行时新增对该 thing 的精确订阅（`$emqx/commands/things/{thingName}/...` 等），无需重启。

### 4.4 MQTT 接入策略

| 方式 | 用途 | 说明 |
|------|------|------|
| **MQTT 原生客户端** | **Subscribe** + **上行 Publish** | EMQX REST API 不支持持久订阅 |
| **HTTPS ThingDatas API** | Jobs proxy 模式（`start-next`、`update` 等） | 与 EMQX Rule → ThingDatas 路径一致 |

上行所有 publish（Shadow reported、Event、Command response）统一使用 MQTT 原生客户端发布到标准 `$emqx/` 主题，由 Fleets 预配 EMQX 规则处理。**Fleets 内部仍使用 EMQX V5 REST API 发布（不受影响）。**

**Bridge MQTT 连接参数**：

```bash
DA_BRIDGE_MQTT_URL=mqtt://localhost:1883
DA_BRIDGE_MQTT_VERSION=5                     # 强烈推荐 MQTT 5（支持 NoLocal）
DA_BRIDGE_MQTT_CLIENT_ID=da-fleets-bridge-{instanceId}
DA_BRIDGE_MQTT_USERNAME=da-bridge-svc        # 独立服务账号
DA_BRIDGE_MQTT_PASSWORD=...
DA_BRIDGE_MQTT_CLEAN_SESSION=false
DA_BRIDGE_MQTT_QOS=1
DA_BRIDGE_MQTT_NO_LOCAL=true                 # MQTT 5：避免自发布回环
DA_BRIDGE_MQTT_KEEPALIVE=60
DA_BRIDGE_MQTT_CONNECT_TIMEOUT=30s
DA_BRIDGE_MQTT_RECONNECT_BACKOFF=1s,5s,15s,30s,60s
```

> **S5 自发布过滤（关键）**：Bridge 通过 P1 向 `$emqx/things/da-{name}/shadow/update` 发布 `state.reported`，同时通过 S5 订阅了同一 topic 通配符。MQTT 3.1.1 会将自己发布的消息也投递给自身订阅。解决方案：
> - **MQTT 5**：启用 `NoLocal=true`，协议层避免回环
> - **MQTT 3.1.1**：S5 handler 必须 payload 检查 — **仅当存在 `state.desired` 键时才处理**，仅含 `state.reported` 的直接丢弃

> **EMQX ACL 要求（PC8）**：Bridge MQTT 账号必须在 EMQX ACL 中获得 `$emqx/things/da-+/...`、`$emqx/commands/da-+/...` 等系统主题的 sub/pub 权限。默认 EMQX 配置可能拒绝非特权客户端访问 `$emqx/` 前缀主题。

### 4.5 消息处理流水线

```
MQTT OnMessage
    │
    ▼
Topic Router ──► 提取参数 {uid, deviceId, thingName, executionId, jobId}
    │             uid = topic 首段（DA 侧 userId）
    ▼
Mapping Lookup ──► da_bridge_mappings WHERE da_user_id=? AND da_device_id=?
    │              miss → 丢弃 + metric + warn log
    ▼
Per-Thing Advisory Lock（变更类消息：command/shadow/jobs）
    │
    ▼
Schema Lookup ──► da_bridge_schema_mappings（mapping_spec）
    │
    ▼
Translator ──► payload 转换 + 校验
    │
    ▼
Publish ──► MQTT client (QoS 1) 或 HTTPS ThingDatas API
    │
    └──► 结构化日志 + trace_id + latency histogram
```

> **per-thing 串行处理**：对同一 thing 的变更类消息（S4-S8）必须串行处理。实现方式：per-thing 内存队列，或按 `thingName` 哈希到固定 worker goroutine。避免并发处理同一 thing 的 command + shadow desired 导致状态竞争。

---

## 5. Schema 映射引擎

### 5.1 数据模型

> **与 DA 代码库对齐说明**：DA `ProductInfo` 当前仅含 `id` 字段（无 `baseProductId`）。本方案引入的 `base_product_id` 是**设计层面的抽象**——期望 DA 侧未来支持跨版本稳定的产品线标识。短期可行替代：使用 `ProductInfo.id` 同时作为 `base_product_id` 和 versioned `product_id`，待 DA 支持后迁移。

#### `da_bridge_mappings`（设备实例映射）

```sql
CREATE TABLE da_bridge_mappings (
    id                  BIGSERIAL PRIMARY KEY,
    -- DA 侧路由标识
    da_user_id          TEXT NOT NULL,           -- DA MQTT topic 首段（DA 侧为 userId，非 productId）
    da_device_id        TEXT NOT NULL,           -- DA 设备标识
    da_base_product_id  TEXT NOT NULL,           -- DA 侧跨版本稳定的产品线标识（若无则复用 ProductInfo.id）
    da_namespace        TEXT NOT NULL DEFAULT 'default',
    -- Fleets 侧标识
    thing_type_id       UUID NOT NULL,
    thing_id            UUID NOT NULL UNIQUE,
    thing_name          TEXT NOT NULL UNIQUE,    -- da-{encBasePid}--{encDid}（≤255 字符）
    mqtt_client_id      TEXT NOT NULL,           -- 用于 lifecycle 匹配
    schema_version      INTEGER NOT NULL DEFAULT 1,
    da_metadata         JSONB,
    last_synced_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (da_user_id, da_device_id)
);
CREATE INDEX idx_bridge_mappings_base_product ON da_bridge_mappings (da_base_product_id);
CREATE INDEX idx_bridge_mappings_thing_name ON da_bridge_mappings (thing_name);
```

> **`da_user_id` vs `da_product_id` 命名**：DA MQTT topics 首段在代码中为 `userId`（DA 用户/租户标识），非 `productId`。Bridge 映射表使用 `da_user_id` 对齐代码实际语义。若 DA 方确认 topic 首段即为 DA productId，可改回 `da_product_id`。见 [Q7](#187-da-topic-首段语义)。

#### `da_bridge_schema_mappings`（Product ↔ ThingType 映射规则）

```sql
CREATE TABLE da_bridge_schema_mappings (
    id                  BIGSERIAL PRIMARY KEY,
    da_base_product_id  TEXT NOT NULL,          -- DA 跨版本稳定的产品线标识
    thing_type_name     TEXT NOT NULL,
    schema_version      INTEGER NOT NULL DEFAULT 1,
    status              TEXT NOT NULL DEFAULT 'active',  -- draft | active | deprecated

    mapping_spec        JSONB NOT NULL,         -- 映射规则（见 §5.2）

    -- 快照（审计与 diff）
    da_device_spec      JSONB,                  -- 注册时的 DA Product DeviceSpec
    fleets_schema       JSONB,                  -- 注册时的 Fleets ThingType schema

    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (da_base_product_id, schema_version)
);
CREATE INDEX idx_bridge_schema_mappings_base_product ON da_bridge_schema_mappings (da_base_product_id, status);
```

> **删除 `da_user_ids` 字段**：v2.0 中该字段用途不明确。`da_base_product_id` 已足以定位产品线映射；若需租户隔离，应在 `da_bridge_mappings` 中体现。Schema mapping 本身不应按 userId 隔离，避免同一 product 多租户重复定义。

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
    "mode": "commandMap",
    "documentPath": "operation",
    "operationCommandMap": {
      "firmware_update": "startFirmwareUpdate"
    }
  }
}
```

**字段默认值策略**：
- `fleetsKey` 省略时，默认等于源属性名（如 `"temp_a"` 省略 `fleetsKey` 则 Fleets 侧 key 仍为 `"temp_a"`）
- `daCmd` 省略时，默认等于 `fleetsAction`
- `inputMap`/`outputMap`/`paramMap`/`fieldMap` 省略时，默认一一对应（同名字段直接透传）

**`desiredApply` 策略**（per-property，Shadow 下行核心）：

| strategy | 行为 |
|----------|------|
| `command` | 将 desired 中该 writable 字段变化映射为一条 DA `commands` 消息 |
| `stateOnly` | 仅更新 Fleets reported（非下行） |
| `ignore` | 跳过该字段 |

> **设计决策**：per-property 独立声明 `desiredApply`，不设全局 `shadow.desiredStrategy`。可通过 `SHADOW_DESIRED_BATCH_MODE` 控制合并。

### 5.3 Schema 生成与修订流程

```
DA Product 创建/变更
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
Bridge Schema Registry 热加载
```

### 5.4 运行时 Schema 缓存

- 启动时全量加载 `status=active` 的 `da_bridge_schema_mappings`
- 每条 MQTT 消息优先查内存 map；未命中则同步查 PG（带短 TTL 本地缓存）
- `schema_version` 决定选用哪个版本的 mapping_spec
- 热更新：定期检测 `max(updated_at)` 或监听 PG `NOTIFY`（若 Bridge 与写入方共享 PG）

---

## 6. 统一 Topic 路由表

> **关键说明**：DA 侧 MQTT topics 首段在 DA 代码实现中为 `userId`（用户/租户标识），在 DA 文档中称为 `productId`。以下路由表使用 `{uid}` 表示 topic 中实际提取的值（对齐代码实现）。若 DA 团队确认 topic 首段即为 productId，则将所有 `{uid}` 替换为 `{pid}`。

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

> **S5 自发布过滤（必选实现）**：MQTT 3.1.1 不区分 self-publish。S5 handler 必须检查 payload — **仅当存在 `state.desired` 键时才处理**。MQTT 5 启用 `NoLocal=true` 从协议层避免此问题（推荐）。

> **精确订阅策略（推荐）**：启动时从 `da_bridge_mappings` 构造精确 topic 列表（如 `$emqx/commands/things/da-p-sensor--device-001/executions/+/request`），按 thing 批量 subscribe。新 mapping 写入时，Admin API 触发 Bridge 动态新增订阅。精确订阅避免通配符在大规模下的性能开销，且符合 EMQX ACL 最小权限原则。

### 6.2 Bridge 发布（Publish）

| # | 方向 | MQTT Topic Pattern | 方式 | 说明 |
|---|------|-------------------|------|------|
| P1 | 上行 | `$emqx/things/da-{encBasePid}--{encDid}/shadow/update` | MQTT client (QoS 1) | Shadow reported |
| P2 | 上行 | `$emqx/things/da-{encBasePid}--{encDid}/events/{eventType}` | MQTT client (QoS 1) | Event |
| P3 | 上行 | `$emqx/commands/things/da-{encBasePid}--{encDid}/executions/{id}/response` | MQTT client (QoS 1) | Command response |
| P4 | 下行 | `device-agent/{uid}/device/{did}/commands` | MQTT client (QoS 1) | Command 下发 + Shadow desired 映射 + Jobs commandMap |
| P5 | Jobs | `POST /api/v1/thing-datas/jobs/start-next` 等 | **HTTPS** | Jobs proxy 模式（走 ThingDatas API + Basic Auth） |

---

## 7. 上行协议适配（DA → Fleets）

### 7.1 Telemetry → Shadow reported

**订阅**：`v1/{uid}/{deviceId}/telemetry`（S1）

DA payload:
```json
{"type":"state","data":{"temp_a":23.5},"ts":1750000000000}
```

| DA `type` | Bridge 行为 |
|-----------|------------|
| `state` | 转 `$emqx/things/{thingName}/shadow/update`，payload `{"state":{"reported":{...}}}` |
| `status` | **忽略**（lifecycle 由 EMQX 连接事件处理） |
| 其他 | 按 mapping 决定是否忽略 |

**示例**：
```
IN  v1/user-abc/device-001/telemetry
    {"type":"state","data":{"temp_a":23.5},"ts":1750000000000}

OUT $emqx/things/da-p-sensor--device-001/shadow/update (QoS 1)
    {"state":{"reported":{"temp_a":23.5}}}
```

### 7.2 Event → Fleets events

**订阅**：`v1/{uid}/{deviceId}/event`（S2）

DA payload:
```json
{"type":"event","data":{"event":"overheat","zone":"A","temperature":85.3},"ts":...}
```

转换规则：
- `eventType` 取 `data.event`，`severity` 取自 `mapping_spec.events[].fleetsSeverity`（默认 `"info"`）
- `data` 剔除 `event` 字段，保留 output 经 `fieldMap` 映射

```
OUT $emqx/things/da-p-sensor--device-001/events/overheat (QoS 1)
    {"eventType":"overheat","severity":"warn","data":{"zone":"A","temperature":85.3}}
```

### 7.3 Command Response → Fleets command response

**订阅**：`device-agent/{uid}/device/{deviceId}/responses`（S3）

DA payload（**DA 实际响应字段为 `msg`，非 `message`**）：
```json
{
  "requestId": "550e8400-e29b-41d4-a716-446655440000",
  "code": 0,
  "msg": "ok",
  "data": { "estimatedDowntime": 45 }
}
```

Bridge 查 **in-flight 命令表**（PG 持久化）：

```sql
CREATE TABLE da_bridge_inflight_commands (
    id              BIGSERIAL PRIMARY KEY,
    execution_id   UUID NOT NULL UNIQUE,      -- Fleets command execution ID（唯一约束，幂等）
    thing_name     TEXT NOT NULL,
    da_user_id     TEXT NOT NULL,             -- DA topic 首段（用于 topic 重建）
    da_device_id   TEXT NOT NULL,
    da_request_id  TEXT NOT NULL UNIQUE,      -- DA 侧 requestId（correlation key）
    fleets_action  TEXT NOT NULL,
    expires_at     TIMESTAMPTZ NOT NULL,      -- TTL 过期时间
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_inflight_expires ON da_bridge_inflight_commands (expires_at);
CREATE INDEX idx_inflight_da_req_id ON da_bridge_inflight_commands (da_request_id);
```

**转换**：
- 按 `requestId` 查 in-flight 表 → 取得 `executionId` 与 `thingName`
- `code === 0` → `SUCCEEDED`；否则 `FAILED`
- `result` 经 `commands.{action}.outputMap` 转换

```
OUT $emqx/commands/things/{thingName}/executions/{executionId}/response (QoS 1)
    {"status":"SUCCEEDED","result":{"estimatedDowntime":45},"timestamp":1715432405}
```

> **幂等性**：`execution_id` 为 UNIQUE 约束，插入使用 `ON CONFLICT (execution_id) DO NOTHING`。

---

## 8. 下行协议适配（Fleets → DA）

### 8.1 Command 下行

**订阅**：`$emqx/commands/things/da-+/executions/+/request`（S4）

Fleets JobService 通过 EMQX V5 REST API 发布（标准 payload）：
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
2. 获取 per-thing advisory lock（`HashToInt64("da-bridge-" + thingName)`）
3. 查 `da_bridge_mappings` 得 `da_user_id`、`da_device_id`
4. 查 `mapping_spec.commands[action]` 得 `daCmd`、`inputMap`
5. `requestId` = `executionId`（直接映射）
6. 写入 in-flight 表（`ON CONFLICT (execution_id) DO NOTHING`）
7. 发布 DA commands topic（QoS 1）：

```json
{
  "requestId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "cmd": "setInterval",
  "params": { "interval": 30 }
}
```

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
   │                    │                  │                │                  │ in-flight + pub   │
   │                    │                  │                │                  │─────────────────►│
   │                    │                  │                │                  │                  │ execute
   │                    │                  │                │                  │◄─────────────────│
   │                    │                  │                │◄─────────────────│ response (QoS 1) │
   │                    │                  │◄───────────────│ Publish response │                  │
   │                    │◄─────────────────│ UpdateExecution │                  │                  │
   │◄───────────────────│ SUCCEEDED        │                │                  │                  │
```

### 8.2 Shadow desired / delta 下行

DA **无 Shadow 协议**。Bridge 将 Fleets Shadow 下行**语义映射**为 DA 命令。

**订阅**：S5 (`shadow/update`)、S6 (`shadow/update/delta`)

**desired payload**（Fleets ShadowService 通过 EMQX V5 REST API 发布的格式）：
```json
{
  "state": {
    "desired": {
      "reportIntervalSec": 60,
      "powerMode": "eco"
    }
  },
  "clientToken": "thing-name",
  "version": 12
}
```

**Bridge 算法**（per-property `desiredApply`）：

1. 收到 S5 消息：检查 `state.desired` 存在性（仅 `reported` → 跳过）
2. 收到 S6 消息：直接取 payload 中的 delta 键
3. 对每个变更的 writable 字段查 `mapping_spec.properties[field].desiredApply`
4. 无 `desiredApply` 声明的 writable 字段 → 跳过 + warn 日志
5. `strategy=command` → 生成 DA 命令
6. **去重检查**：记录该 thing 最近处理的 `version` + `clientToken`，若重复则跳过
7. 合并策略（`SHADOW_DESIRED_BATCH_MODE`）：

| 值 | 行为 |
|----|------|
| `perField` | 每个字段独立发一条 DA 命令（默认，最安全） |
| `singleCommand` | 所有同 cmd 的字段合并为一条命令的参数 |

```
示例（perField 模式）：
Fleets desired {reportIntervalSec: 60, powerMode: "eco"}
    → desiredApply[reportIntervalSec] → cmd=setInterval, params={interval: 60}
    → desiredApply[powerMode] → cmd=setPowerMode, params={mode: "eco"}
```

> **注意**：Fleets ShadowService 也会向 `$emqx/things/.../shadow/update` 发布 desired update（通过 EMQX V5 REST API）。Bridge 订阅 S5 会收到这些消息，而 NoLocal 不会过滤（因为不是 Bridge 自己发布的）。这是**期望行为**——Bridge 需要处理 Fleets 下发的 desired。

---

## 9. Jobs 代理层

DA 无 `$emqx/things/.../jobs/*` 实现。Bridge 提供两套 Jobs 适配模式：

### 9.1 模式对比

| mode | 适用场景 | Bridge 行为 | DA 参与度 |
|------|---------|-------------|----------|
| `commandMap` | jobDocument 有固定 operation；固件升级、配置下发等 | 订阅 notify → 解析 Document 中 operation → 映射为 DA 命令 → 回写 update | DA 被动收命令 |
| `proxy` | DA 设备无 Jobs 意识 | Bridge 完整代跑 Jobs 状态机（调用 ThingDatas API） | DA 不参与 |

**推荐默认**：`commandMap`（Phase 4）。

> **Job Document 结构说明**（基于 Fleets `model.go` 代码验证）：`ThingJob.Document` 为 `json.RawMessage`，无结构化的 `operation` 字段。Bridge 需从 Document JSON 中按 `mapping_spec.jobs.documentPath`（默认 `"operation"`）提取 operation 值，这需要 DA 与 Fleets 的 Job 创建方约定 Document schema。见 [Q10](#183-fleets-侧)。

### 9.2 commandMap 模式

```
Fleets JobService
    │ publish notify/notify-next (via EMQX V5 REST API)
    ▼
Bridge 订阅（S7/S8）
    │
    ▼
解析 notify 中 QUEUED 列表
    │
    ▼
POST /api/v1/thing-datas/jobs/start-next (HTTPS)
    │
    ▼
收到 execution → Document.{documentPath} → mapping_spec.jobs.operationCommandMap → daCmd
    │
    ▼
发布 device-agent/{uid}/device/{did}/commands (QoS 1)
    │
    ▼
收到 DA response → POST /api/v1/thing-datas/jobs/{jobId}/update (HTTPS)
    body: {thingName, status: "SUCCEEDED"|"FAILED", statusDetails: {...}}
```

### 9.3 Bridge Jobs 订阅与发布（汇总）

| # | Topic/API | 方向 | 模式 |
|---|-----------|------|------|
| S7 | `$emqx/things/da-+/jobs/notify` | MQTT sub | commandMap + proxy |
| S8 | `$emqx/things/da-+/jobs/notify-next` | MQTT sub | commandMap + proxy |
| — | `POST /api/v1/thing-datas/jobs/get` | HTTPS | proxy |
| — | `POST /api/v1/thing-datas/jobs/start-next` | HTTPS | commandMap + proxy |
| — | `POST /api/v1/thing-datas/jobs/{jobId}/update` | HTTPS | commandMap + proxy |
| P4 | `device-agent/{uid}/device/{did}/commands` | MQTT pub | commandMap |

---

## 10. 设备注册与元数据同步

### 10.1 注册流程

```
DA Device 创建（userId, deviceId, device.metadata.mqttClientId）
    → DA IoT Platform (fleets) 插件
        1. 读 device.metadata.mqttClientId（或按约定生成 {namespace}/dev/{deviceId} 并回写）
        2. 生成 ThingType schema 草稿 + mapping_spec 草稿
        3. 用户修订
        4. POST Fleets /api/v1/thing-types（ThingType 名 = da-product-{baseProductId}）
        5. POST Fleets /api/v1/things
           - name: da-{encBasePid}--{encDid}（≤255 字符）
           - thingTypeId: 从步骤 4 获取
           - mqttClientId: 步骤 1 获取的值
           - tags: ["da-bridge"]
        6. POST Bridge Admin /api/v1/schema-mappings
        7. POST Bridge Admin /api/v1/mappings
    → Bridge Schema Registry 热加载 + 动态新增订阅
```

> **PC2 Fallback**：若 DA 短期无法实现设备注册回调（PC2），Bridge 侧启动时及定期轮询 DA API `GET /api/products/{productId}/devices`，对比本地 `da_bridge_mappings` 差异，自动补全缺失映射（类似全量 sync）。此 fallback 在 Phase 1 作为兜底机制实现。

> **Q17 处理**：DA `device.productId` 为可选字段（可为 null）。若设备未绑定 product，Bridge 无法获取 properties/commands/events 定义，**应拒绝注册桥接**并在日志中告警。DA IoT 插件需在注册前校验 `productId != null`。

### 10.2 Thing 命名

| 项 | 格式 | 示例 | 约束 |
|----|------|------|------|
| Thing | `da-{encodedBaseProductId}--{encodedDeviceId}` | `da-p-sensor--device-001` | ≤255 字符 |
| ThingType | `da-product-{baseProductId}` | `da-product-p-sensor` | ≤255 字符 |
| Tags | `["da-bridge"]` | — | — |

> **长度约束**：Fleets Thing 名 `VARCHAR(255)`。`baseProductId` 和 `deviceId` 编码后总长度 + `da-` 前缀 + `--` 分隔符不得超过 255 字符。encode 函数下 `--` 转义为 `_dashdash_`，可能导致膨胀。建议限制 `baseProductId` ≤64、`deviceId` ≤64 字符。见 [Q9](#182-架构决策)。

> **编码建议**：采用 URL-safe base64（无 `=` 填充）或简单字符替换（仅替换 `/`、`+`、`-` 等特殊字符）。避免使用可能导致长度膨胀的转义方案。

### 10.3 mqttClientId 格式

| 层 | 约定 | 示例 |
|----|------|------|
| **设备直连** | 连接 EMQX 时使用 Client ID `{namespace}/dev/{deviceId}` | `default/dev/device-001` |
| **DA IoT 插件** | 注册 Thing 时从 `device.metadata.mqttClientId` 读取；如未设置则自动生成并回写 | — |
| **Fleets lifecycle** | Thing.`mqttClientId` = 上述值，用于 EMQX lifecycle 事件匹配 | — |

---

## 11. Fleets 与 Device Agent 侧协作

### 11.1 Fleets 侧改动（零代码改动）

| 改动 | 说明 |
|------|------|
| ~~`da_bridge_*` 表 migration~~ | **Bridge 独立管理**，不纳入 Fleets `migrations/` |
| 移除 EMQX da-bridge Rule 注入 | 新方案不依赖 Rule republish；Fleets 原生预配规则**保留不变** |
| docker-compose / 安装脚本 | 增加 `da-bridge` 服务定义（引用 Bridge 镜像） |
| AGENTS.md 架构说明更新 | 明确「Bridge Agent 是独立 Sidecar，非 Fleets 核心进程」 |
| 文档 | 桥接设备能力边界、Jobs mode 说明 |

**明确不改动**：Fleets 所有 Go 源代码（`JobService`、`ShadowService`、ThingDatas Handler、预配 EMQX 规则、ThingService）。

### 11.2 Device Agent IoT Platform 插件

> DA 代码库当前**无 `platforms/` 目录**。需新建 `apps/agent-gateway/src/platforms/fleets/`（前置条件 PC1）。

| 变更 | 说明 |
|------|------|
| **新建插件模块** | `apps/agent-gateway/src/platforms/fleets/` |
| `schema-mapper.ts` 输出 | 同时生成 Fleets ThingType schema + `mapping_spec` JSON |
| 注册 API | POST Fleets Thing/ThingType + POST Bridge Admin schema-mappings |
| 不订阅 MQTT | DA 插件不订阅 MQTT |
| baseProductId | 使用跨版本稳定的产品线标识（DA 尚无此概念，暂复用 ProductInfo.id） |
| 回调依赖 | 优先 PC2（事件发射）；若未实现，Bridge 轮询兜底 |
| productId 校验 | 未绑定 product 的设备禁止注册桥接 |

### 11.3 能力矩阵（用户可见）

| 能力 | 桥接设备 |
|------|---------|
| Shadow reported 上行 |  |
| Event 上行 |  |
| Lifecycle 在线状态 |  |
| Fleets 控制台 sync 命令 |  |
| Shadow desired/delta 下行 | （映射为 DA 命令） |
| Fleets Jobs | `commandMap` / `proxy` 可配置 |
| 自定义高频 telemetry | 远期（走 EMQX Rule Bridge → EMQX Tables，不经 Bridge） |

---

## 12. 认证与安全

### 12.1 认证架构

```
Bridge Agent ──MQTT Auth──► EMQX Broker（独立服务账号 ACL）
Bridge Agent ──Basic Auth──► Fleets REST API（读 thing/type/shadow；Jobs proxy 写 thing-datas）
Bridge Agent ──Bearer/Token──► Device Agent REST API
DA IoT 插件 ──Basic Auth──► Fleets REST API（注册 Thing/ThingType）
DA IoT 插件 ──Basic Auth/Internal──► Bridge Admin API（写 mapping_spec）
DA 设备 ──MQTT Auth──► EMQX Broker（既有策略，不经 Bridge）
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

> **EMQX 配置要求**：需在 EMQX 中为 `da-bridge-svc` 账号显式配置上述 ACL。默认 EMQX 可能拒绝客户端访问 `$emqx/` 系统主题（PC8）。

### 12.3 API Key 最小权限

**Bridge → Fleets**：

Fleets 权限使用 `strings.HasPrefix` 前缀匹配（非通配符 `*`），以下配置已对齐：

```json
{
  "name": "da-fleets-bridge",
  "permissions": {
    "GET": ["/api/v1/things", "/api/v1/thing-types"],
    "POST": ["/api/v1/thing-datas/jobs"]
  }
}
```

覆盖范围：
- `GET /api/v1/things`, `/api/v1/things/...`, `/api/v1/thing-types`, `/api/v1/thing-types/...`
- `POST /api/v1/thing-datas/jobs/get`, `/api/v1/thing-datas/jobs/start-next`, `/api/v1/thing-datas/jobs/{id}/update`

> **说明**：`"/api/v1/things"` 前缀已匹配 `/api/v1/things/{id}/shadow`。`"/api/v1/thing-datas/jobs"` 前缀已匹配所有 jobs 子路径。Fleets 不支持 `*` 通配符语法。

**DA IoT 插件 → Fleets**：Thing/ThingType CRUD 独立 API Key（与 Bridge Key 分开）。

**Bridge Admin API 认证**：
- 建议复用 Fleets Basic Auth（读取 Fleets PG `api_keys` 表做 bcrypt 校验），保持认证体系一致
- 独立监听端口 8091，与 Fleets HTTP 服务隔离
- 或独立配置 `BRIDGE_ADMIN_API_KEY` / `SECRET`，不与 Fleets 共享（更解耦）

---

## 13. 部署与生命周期

### 13.1 环境变量

```bash
DA_BRIDGE_ENABLED=true
DA_BRIDGE_INSTANCE_ID=da-bridge-1

# MQTT
DA_BRIDGE_MQTT_URL=mqtt://emqx:1883
DA_BRIDGE_MQTT_VERSION=5
DA_BRIDGE_MQTT_CLIENT_ID=da-fleets-bridge-1
DA_BRIDGE_MQTT_USERNAME=da-bridge-svc
DA_BRIDGE_MQTT_PASSWORD=...
DA_BRIDGE_MQTT_CLEAN_SESSION=false
DA_BRIDGE_MQTT_QOS=1
DA_BRIDGE_MQTT_NO_LOCAL=true
DA_BRIDGE_MQTT_KEEPALIVE=60

# PostgreSQL（与 Fleets 同库或独立库）
DATABASE_URL=postgres://postgres:postgres@postgres:5432/fleets

# Fleets API
FLEETS_API_URL=http://fleets:8080
FLEETS_API_KEY=...
FLEETS_API_SECRET=...

# Device Agent API
DA_API_URL=http://device-agent:3000
DA_API_TOKEN=...

# Tuning
DA_BRIDGE_SHADOW_DESIRED_BATCH_MODE=perField
DA_BRIDGE_JOBS_MODE=commandMap
DA_BRIDGE_JOBS_DOCUMENT_PATH=operation
DA_BRIDGE_SCHEMA_SYNC_INTERVAL=60s
DA_BRIDGE_INFLIGHT_TTL=120s
DA_BRIDGE_INFLIGHT_CLEANUP_INTERVAL=30s
DA_BRIDGE_HTTP_PORT=8091
DA_BRIDGE_SHUTDOWN_TIMEOUT=30s
LOG_LEVEL=info
```

### 13.2 docker-compose 片段

```yaml
services:
  fleets:
    image: fleets:latest
    healthcheck:
      test: ["CMD", "wget", "-qO-", "http://localhost:8080/ready"]
      interval: 10s
      timeout: 3s
      retries: 3

  da-bridge:
    image: da-fleets-bridge:latest
    depends_on:
      emqx:
        condition: service_healthy
      postgres:
        condition: service_healthy
    environment:
      DA_BRIDGE_ENABLED: "true"
      DATABASE_URL: postgres://postgres:postgres@postgres:5432/fleets
      DA_BRIDGE_MQTT_URL: mqtt://emqx:1883
      DA_BRIDGE_MQTT_USERNAME: da-bridge-svc
      DA_BRIDGE_MQTT_PASSWORD: ${DA_BRIDGE_MQTT_PASSWORD}
      FLEETS_API_URL: http://fleets:8080
      FLEETS_API_KEY: ${FLEETS_API_KEY}
      FLEETS_API_SECRET: ${FLEETS_API_SECRET}
      DA_API_URL: http://device-agent:3000
      DA_API_TOKEN: ${DA_API_TOKEN}
    ports:
      - "8091:8091"
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "wget", "-qO-", "http://localhost:8091/ready"]
      interval: 10s
      timeout: 3s
      retries: 3
    deploy:
      resources:
        limits:
          cpus: "1"
          memory: 512M
```

> **去耦合**：Bridge **不依赖 `fleets` 服务的 healthy 状态**。两者并行启动，各自依赖 Postgres + EMQX。避免 Fleets 与 Bridge 形成循环依赖或级联启动阻塞。

---

## 14. 高可用与可观测性

### 14.1 多实例 HA

| 策略 | 说明 |
|------|------|
| **Active-Standby**（首期） | `pg_try_advisory_lock(HashToInt64("da-fleets-bridge"))` 选主；主实例持有 MQTT 连接；Standby 定期抢锁 |
| **Failover** | Standby 抢锁成功 → 建立 MQTT 连接 → 从 PG 加载最新映射 → 批量订阅 → 进入 Running |

> **注意**：MQTT `cleanSession=false` 确保 failover 后重新连接时，Broker 会投递离线期间的消息（QoS 1）。但 Bridge 必须重新订阅（订阅不持久化于客户端外）。

### 14.2 指标（Prometheus）

| 指标 | 说明 |
|------|------|
| `bridge_mqtt_connected` | MQTT 连接状态 gauge |
| `bridge_mqtt_reconnect_total` | MQTT 重连次数 counter |
| `bridge_messages_total{direction,type}` | 上下行消息计数 counter |
| `bridge_translate_errors_total{reason}` | 转换失败 counter |
| `bridge_uplink_dropped_total{reason}` | 丢弃原因 counter |
| `bridge_inflight_commands` | 当前 in-flight 命令数 gauge |
| `bridge_inflight_expired_total` | TTL 过期清理数 counter |
| `bridge_translate_latency_seconds` | 处理延迟 histogram |
| `bridge_schema_cache_hit` | Schema 缓存命中 counter |
| `bridge_schema_cache_miss` | Schema 缓存未命中 counter |

### 14.3 日志与追踪

- 每条消息：`trace_id`、`topic_in`、`topic_out`、`thing_name`、`latency_ms`
- 使用 `log/slog`（与 Fleets 一致），JSON 格式
- 增加 `client_id`（MQTT client id）、`instance_id` 字段

---

## 15. 与旧方案对比

| 维度 | 旧方案 v2.5 | 新方案 Bridge Agent |
|------|------------|---------------------|
| 复杂度分布 | Broker Rule SQL + 薄插件 | 独立进程 + 零 Fleets 核心改动 |
| 上行 | Rule republish | Bridge MQTT sub → pub |
| 下行 | 不支持（§12） | 原生支持 Command + Shadow + Jobs |
| Schema | 注册时转换 | 注册 + 运行时 mapping_spec + 多版本并存 |
| Fleets 侵入 | BridgePlugin + Rule Manager + ThingService hooks | **零代码改动**；仅编排 Sidecar |
| EMQX 依赖 | 高（Rule SQL 版本） | 低（仅预配规则；Go 逻辑替代 SQL） |
| 架构约束 | 不违反 | 独立仓库 Sidecar，Fleets 核心仍不订阅 |
| 运维 | Rule 漂移 | Bridge 版本 + Schema 版本独立演进 |

**迁移路径**（若旧方案已 PoC）：

1. 部署 Bridge Agent，启用上行
2. 禁用 `da-bridge-*` EMQX 规则（Rule republish 规则）
3. 验证 Shadow reported / Event 等价
4. 启用下行 Command → Shadow → Jobs 分阶段

---

## 16. 实施路线图

### Phase 1：Bridge 骨架 + 上行（2–3 周）

> **前置依赖**：PC1（DA 插件）、PC3（mqttClientId）、PC4（独立仓库）、PC6（架构确认）、PC7（topic 语义）、PC8（EMQX ACL）必须在 Phase 1 启动前定案。PC2 可用轮询 fallback 降级。

| 任务 | 说明 |
|------|------|
| 仓库初始化 + 项目骨架 + 生命周期 | 独立仓库 `emqx/da-fleets-bridge` |
| MQTT client（MQTT 5 + NoLocal） + unified topic router | §6 subscribe S1–S3、publish P1–P2（QoS 1） |
| `da_bridge_mappings` + `da_bridge_schema_mappings` migration | Bridge 自有 PG 表 + golang-migrate |
| Telemetry / Event translator | §7.1, §7.2 |
| docker-compose 集成 | 与 Fleets 联调；确认去耦合 |
| PC2 fallback 轮询 | 定期 sync DA devices → mappings |

### Phase 2：Command 双向（2–3 周）

| 任务 | 说明 |
|------|------|
| In-flight 命令表（PG）+ TTL 清理 | §7.3 |
| Command 下行 + response 上行 translator | §8.1, §7.3 |
| Command correlation（executionId ↔ requestId） | In-flight 查表 |
| Bridge Admin API（schema-mappings CRUD） | POST /api/v1/schema-mappings |
| 精确订阅管理 | 动态增删 per-thing 订阅 |

### Phase 3：Shadow 下行（1–2 周）

| 任务 | 说明 |
|------|------|
| Per-property desiredApply translator | §8.2 |
| SHADOW_DESIRED_BATCH_MODE | perField / singleCommand |
| S5 自发布过滤 | §6.1 + payload version 去重 |

### Phase 4：Jobs 代理（2 周）

| 任务 | 说明 |
|------|------|
| jobs.mode=commandMap | notify 订阅 → start-next → DA 命令 → update |
| jobs.mode=proxy | ThingDatas HTTPS API 代调用 |

### Phase 5：生产化

HA Active-Standby、监控告警、映射 resync Admin 完整实现、性能压测。

---

## 17. 附录

### A. Topic 对照速查表

| 方向 | DA Topic | Fleets Topic |
|------|----------|--------------|
| 状态上行 | `v1/{uid}/{did}/telemetry` | `$emqx/things/da-{encBasePid}--{encDid}/shadow/update` |
| 事件上行 | `v1/{uid}/{did}/event` | `$emqx/things/da-{encBasePid}--{encDid}/events/{eventType}` |
| 命令响应 | `device-agent/{uid}/device/{did}/responses` | `$emqx/commands/things/da-.../executions/{id}/response` |
| 命令下行 | `device-agent/{uid}/device/{did}/commands` | `$emqx/commands/things/da-.../executions/{id}/request` |
| Shadow 下行 | ↑ commands（合成） | `$emqx/things/da-.../shadow/update` |

### B. DA REST API 参考（代码验证）

DA REST API **不含 `/v1` 前缀**，所有路径在 `/api/` 下。

| 用途 | 实际路径 | 返回数据 |
|------|---------|---------|
| 获取 Products | `GET /api/products` | `ProductInfo[]` |
| 获取 Product | `GET /api/products/{productId}` | `ProductInfo`（含 `id`, `properties`, `commands`, `events`） |
| 获取 Devices | `GET /api/products/{productId}/devices` | `DeviceInfo[]` |
| 获取 Device | `GET /api/products/{productId}/devices/{deviceId}` | `DeviceInfo`（含 `metadata` JSONB） |

**DA Device 关键字段**（`devices` 表）：
- `id` — deviceId（PK 之一）
- `userId` — 用户/租户标识（PK 之一，也是 MQTT topic 首段值）
- `productId` — 关联的 product id（**可选，可为 null**）
- `metadata` — `Record<string, any>`（用于存储 mqttClientId）
- `shadow` — JSONB，包含 `{desired, reported}`

**DA Command 格式**（`packages/shared/src/mqtt-contract.ts` 代码验证）：
- 请求：`{ cmd, params?, requestId, ts?, metadata? }`
- 响应：`{ code, msg, requestId, data?, ts?, metadata? }`

### C. Fleets Shadow 发布 payload 格式（代码验证）

Fleets `ShadowService.publishDesiredUpdate` 通过 EMQX V5 REST API 发布的格式（`shadow_svc.go`）：

```json
{
  "state": { "desired": { ... } },
  "clientToken": "thing-name",
  "version": 12
}
```

Fleets `ShadowService.publishDelta`（`shadow/update/delta`）：
```json
{
  "version": 12,
  "timestamp": 1715432405,
  "state": { ... },
  "metadata": { ... }
}
```

### D. 版本记录

| 版本 | 日期 | 变更 |
|------|------|------|
| v1.0 | 2026-07-02 | 初稿：独立 Bridge Agent 进程方案 |
| v1.1–v1.4 | 2026-07-02 | 三轮评审优化 |
| v1.5 | 2026-07-02 | 四轮评审：S5 语义修正、QoS/NoLocal、per-thing lock、Thing 名编码一致 |
| v2.0 | 2026-07-03 | 双代码库交叉验证：修正 DA command response 字段名（`msg` 非 `message`）、修正 Fleets 权限模型（`strings.HasPrefix` 非 `*` 通配）、对齐 DA topic 首段语义（`userId` 非 `productId`）、确认 DA 无 `baseProductId` 概念并给出替代方案、补充 Fleets Shadow 实际 payload 格式和 Job Document 结构说明、修正 Fleets API Key 权限配置、新增 PC7 前置条件、新增 Q7–Q18 待定问题 |
| **v2.1** | **2026-07-03** | **评审优化版**：明确 Bridge 独立仓库 + 独立 migration（Fleets 零代码改动）、明确上行统一走 MQTT publish（不直接调用 ThingDatas API）、引入精确订阅策略 + 动态订阅更新、增加 per-thing 串行处理原则、增加 Shadow desired 去重机制、mapping_spec 增加字段默认值策略和 `jobs.documentPath`、删除 `da_user_ids` 冗余字段、增加 PC8（EMQX ACL）、Q17 明确处理（拒绝无 productId 设备）、PC2 增加轮询 fallback、docker-compose 去耦合（移除对 fleets 的 condition 依赖）、细化 HA failover 流程、扩充 metrics、明确 Bridge Admin API 认证选项 |

---

## 18. 待定问题与不确定性

> 以下问题基于对 Fleets（AGENTS.md、Go 代码、`docs/tags.md`、`docs/api-auth-and-key-management.md`）和 Device Agent（CLAUDE.md、TypeScript 代码、`docs/mqtt-runtime-rules.md`）双代码库的实际调研。已解决/归档问题见 [§18.5](#185-已解决问题归档)。

### 18.1 阻塞项（Phase 1 前必须定案）

| # | 问题 | 状态 | 待确认方 |
|---|------|------|---------|
| **Q1** | **Bridge Agent 仓库位置与架构归属** | 建议独立仓库 `emqx/da-fleets-bridge`，Fleets 核心零代码改动。 | **Fleets 架构团队** |
| **Q2** | **`da_bridge_*` 表 migration 归属** | **建议由 Bridge 独立管理**（golang-migrate），不纳入 Fleets `migrations/`。Fleets 仅共享数据库连接。 | **Fleets 团队** |
| **Q3** | **`mqttClientId` 稳定性** | DA Device 模型无内置字段。`device.metadata` JSONB 可用于存储。约定格式 `{namespace}/dev/{deviceId}`。 | **Device Agent 团队** + 集成方 |
| **Q4** | **ThingType 用户修订 UI** | Phase 1 使用 JSON 导出 + 手工编辑。 | 产品经理确认优先级 |
| **Q5** | **Bridge Admin API 认证与权限** | 选项 A：复用 Fleets Basic Auth（读 PG `api_keys`）；选项 B：独立 `BRIDGE_ADMIN_API_KEY`。独立监听端口 8091。 | **Fleets 团队** |
| **Q6** | **MQTT 5 NoLocal 可行性** | 强烈建议。若只能 MQTT 3.1.1，S5 自发布过滤 + payload version 去重必须完整实现。 | **运维/EMQX 团队** |
| **Q7** | **DA topic 首段语义（userId vs productId）** | DA 代码中 MQTT topic 首段为 `userId`，DA 文档称为 `productId`。Bridge 映射表已采用 `da_user_id` 命名。需 DA 团队确认：Bridge 的 mapping key 是按 topic 提取的值，还是按 DA Product `id` 字段。若 topic 首段 = DA userId ≠ Product.id，则需两个维度索引（userId + productId）。见 PC7。 | **Device Agent 团队** |
| **Q8** | **EMQX ACL 配置（PC8）** | Bridge MQTT 账号需获得 `$emqx/` 系统主题的 sub/pub 权限。默认 EMQX 可能拒绝。需运维确认可配置。 | **运维/EMQX 团队** |

### 18.2 架构决策

| # | 问题 | 状态 |
|---|------|------|
| **Q9** | **Shadow desired 合并下发** | 默认 `perField`。`singleCommand` 需在 mapping_spec 中显式声明。保持。 |
| **Q10** | **Thing 名长度约束** | Fleets `VARCHAR(255)`。编码后可能超限。建议限制 `baseProductId` ≤64、`deviceId` ≤64。需 DA IoT 插件校验。 |
| **Q11** | **Job Document 中 operation 的提取约定** | Fleets `ThingJob.Document` 为 `json.RawMessage`，无结构化的 `operation` 字段。Bridge commandMap 模式需与 Job 创建方约定 Document schema（如 `{"operation": "firmware_update", ...}`）。`mapping_spec.jobs.documentPath` 默认 `"operation"`。 |

### 18.3 Fleets 侧

| # | 问题 | 状态 | 待确认方 |
|---|------|------|--------|
| **Q12** | **Job Document schema 约定** | Bridge commandMap 需要从 `Document` JSON 提取 operation。需与 Fleets Job 创建流程约定 schema。 | **Fleets 团队** |
| **Q13** | **动态精确订阅 vs 通配符订阅** | **默认精确订阅**（按 `da_bridge_mappings` 构造 topic 列表）。大规模时可优化为通配符 + 内存过滤。Phase 1 不阻塞。 | — |

### 18.4 Device Agent 侧

| # | 问题 | 状态 | 待确认方 |
|---|------|------|--------|
| **Q14** | **IoT 插件回调挂载点** | DA `packages/device-management/src/service.ts` 当前无 EventEmitter。需新增（PC2）。**短期可用 Bridge 轮询 fallback 替代。** | **DA 团队** |
| **Q15** | **DA 插件是否直连 PG 写 mapping_spec** | 仅通过 Bridge Admin API（单写入口）。 | DA 团队确认 |
| **Q16** | **DA API Token 认证方式** | DA REST API 的认证机制（`DA_API_TOKEN`）需确认具体格式和获取方式。当前 DA 代码使用 Bun 内置 HTTP server，未发现标准 auth middleware。 | **DA 团队** |
| **Q17** | **DA `baseProductId` 何时实现** | DA 当前无此概念。Phase 1 暂复用 `ProductInfo.id` 同时作为 base 和 versioned id。需 DA 团队确认是否计划支持。 | **DA 团队** |
| **Q18** | **DA 设备 multi-productId vs single-productId** | DA `DeviceInfo.productId` 为可选字段（可为 null）。设备可绑定到 product 也可不绑定。Bridge 的 schema mapping 依赖 product 信息。**当 device.productId 为 null 时，拒绝桥接注册**，要求先绑定 product。 | **DA 团队** |
| **Q19** | **DA MQTT client library 的 NoLocal 支持** | DA 设备端用什么 MQTT 客户端库？是否支持 MQTT 5？这决定了 Bridge 下行是否能走直接 publish（而非需要通过 DA REST API 中转）。 | **DA 团队 + 集成方** |

### 18.5 已解决问题（归档）

| # | 问题 | 解决方案 | 版本 |
|---|------|---------|------|
| Q20 | DA API 路径不含 `/v1` | 修正为 `/api/products/...` | v1.2 |
| Q21 | S5 自发布过滤 | MQTT 5 NoLocal + payload `state.desired` 检查 | v1.5 |
| Q22 | Fleets tag 查询参数 | `?tagName=da-bridge` | v1.4 |
| Q23 | Thing 名编码一致性 | 统一使用 `baseProductId`：`da-{encBasePid}--{encDid}` | v1.5 |
| Q24 | per-thing advisory lock | `HashToInt64("da-bridge-" + thingName)` | v1.5 |
| Q25 | in-flight 幂等性 | `execution_id` UNIQUE + `ON CONFLICT DO NOTHING` | v1.5 |
| Q26 | `da_user_ids` 字段冗余 | 删除 `da_user_ids`，以 `da_base_product_id` 统一索引 | v2.1 |
| Q27 | Fleets 核心是否改代码 | 明确零代码改动，独立仓库 Sidecar | v2.1 |

---

> **文档维护者**: 集成团队
> **相关文档**: [旧方案 v2.5](./device-agent-fleets-integration.md) · Fleets [AGENTS.md](https://github.com/emqx/fleets/blob/main/AGENTS.md) · [tags.md](https://github.com/emqx/fleets/blob/main/docs/tags.md) · [api-auth-and-key-management.md](https://github.com/emqx/fleets/blob/main/docs/api-auth-and-key-management.md) · Device Agent [CLAUDE.md](https://github.com/emqx/device-agent/blob/main/CLAUDE.md) · [mqtt-runtime-rules.md](https://github.com/emqx/device-agent/blob/main/docs/mqtt-runtime-rules.md)
