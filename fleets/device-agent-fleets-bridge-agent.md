# Device Agent × Fleets 集成方案（Bridge Agent 独立进程）

> **版本:** v1.5
> **日期:** 2026-07-02
> **状态:** 三轮评审优化版（待关键架构决策确认）
> **前置文档:** [device-agent-fleets-integration.md](./device-agent-fleets-integration.md)（v2.5，EMQX Rule 方案，已归档）

---

## 目录

1. [背景与动机](#1-背景与动机)
   1.3. [前置条件与已知缺口](#13-前置条件与已知缺口)
   1.4. [与 Fleets 架构约束的关键张力](#14-与-fleets-架构约束的关键张力)
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

**新方案核心思路**：在 Fleets 部署侧启动一个**独立的 Bridge Agent 进程**，通过 **MQTT 原生 subscribe/publish** 双向接入 Broker，在 MQTT 层完成协议适配；必要时调用 **Fleets REST API** 与 **Device Agent REST API** 补齐 Schema、映射与元数据。

> Fleets **核心服务仍不订阅 MQTT**（架构约束不变）。MQTT 订阅职责由 Bridge Agent 这一**独立 Sidecar 进程**承担，而非嵌入 Fleets HTTP 进程。

### 1.3 前置条件与已知缺口

以下前置项需要在 Bridge Agent 开发前/同步完成：

| # | 前置项 | 说明 | 负责方 |
|---|--------|------|--------|
| **PC1** | **DA IoT Platform 插件开发** | Device Agent 代码库当前**不存在** Fleets IoT Platform 插件。需在 `apps/agent-gateway/src/` 下新建 `platforms/fleets/` 插件模块（新目录），实现 ThingType/Thing 注册、schema 草稿生成、mapping_spec 输出。详见 [§11.2](#112-device-agent-iot-platform-插件)。 | **Device Agent 团队** |
| **PC2** | **DA 设备注册回调机制** | DA `packages/device-management` 当前无 `onDeviceRegistered` / `onProductChanged` 等平台回调。PC1 的 Fleets 插件需依赖此回调触发注册流程。 | **Device Agent 团队** |
| **PC3** | **设备 MQTT Client ID 约定** | DA Device 模型无 `mqttClientId` 字段。集成方案需约定 Client ID 格式（建议 `{namespace}/dev/{deviceId}`），由实际设备连接 EMQX 时使用，并由 DA IoT 插件注册 Thing 时填入。详见 [§10.3](#103-mqttclientid-格式)。 | **集成方 + Device Agent 团队** |
| **PC4** | **Bridge Agent 仓库与迁移** | 确定仓库位置（建议 `fleets/cmd/da-bridge`）及 `da_bridge_*` 表 Migration 归属。详见 [Q1](#181-阻塞项phase-1-前必须定案)、[Q2](#182-架构决策)。 | **Fleets 团队** |
| **PC5** | **设备实际直连 EMQX Broker** | 此方案假设 DA 管理的设备**直连 EMQX Broker**（而非通过 DA Gateway 代理）。设备须使用 PC3 约定的 Client ID 连接，并使用 §6 列出的 DA MQTT topics 上报数据。 | **集成方** |
| **PC6** | **Fleets「不订阅 MQTT」架构约束例外** | Fleets AGENTS.md §0 规定「Fleets **NEVER** subscribes to any MQTT topic」。Bridge Agent 必须订阅 MQTT 才能完成下行适配。需明确：Bridge 是**独立 Sidecar 进程**且**不嵌入 Fleets HTTP 主进程**，但是否仍被视为「Fleets」的一部分需要架构委员会/文档正式豁免。建议方案见 [§1.4](#14-与-fleets-架构约束的关键张力)。 | **Fleets 架构团队** |

### 1.4 与 Fleets 架构约束的关键张力

Fleets AGENTS.md 对 MQTT 订阅有严格约束：

> Fleets **NEVER** subscribes to any MQTT topic. All device->cloud data enters via EMQX Rule Engine or HTTPS.

本方案引入的 Bridge Agent 为了完成 Command、Shadow desired/delta、Jobs notify 的下行适配，**必须**作为 MQTT subscriber 接入 Broker。这与上述约束存在张力，需要在文档和实施层面明确以下两点：

1. **进程边界**：Bridge Agent 是**独立于 Fleets HTTP 主进程**的 Sidecar，不承担 Fleets 核心 REST API、Shadow 计算、Job 调度等职责。它仅做 DA↔Fleets 的协议转换层。
2. **归属边界**：若 Bridge Agent 代码放在 `fleets/cmd/da-bridge` 并由 Fleets 团队维护，在运维和版本治理上会被视为 Fleets 的一部分。建议：
   - **首选**：在独立仓库（如 `emqx/da-fleets-bridge`）中实现 Bridge Agent，Fleets 仅保留 `da_bridge_*` 表 migration 和 docker-compose 编排引用。这样可严格保持「Fleets 核心不订阅 MQTT」的架构纯度。
   - **备选**：若必须放在 Fleets 仓库，需在 AGENTS.md 中新增明确例外条款，说明「Bridge Agent Sidecar 是唯一的 MQTT 订阅者，Fleets HTTP 服务仍不订阅」。

> **评审建议**：在 Phase 1 启动前，由 Fleets 架构团队确认归属方案，并同步更新 AGENTS.md / 架构文档。

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
| **映射表为运行时真相源** | `da_bridge_*` 表是 Bridge 运行时的唯一真相源；写入统一走 Bridge Admin API（Bridge runtime 只读），Schema 映射版本与 Product/ThingType 修订联动 |
| **Fail-safe** | 映射缺失时丢弃并告警，不伪造 payload；命令 correlation 严格校验 |
| **Bridge 独立 MQTT 账号** | Bridge 使用独立 EMQX MQTT 服务账号订阅/发布，不依赖 EMQX REST API |
| **QoS 1 + MQTT 5 NoLocal** | 事件、Shadow、Command 等关键上下行 topic 统一使用 QoS 1；MQTT 5 客户端启用 `NoLocal=true` 避免收到自己发布的消息 |
| **Source-of-Truth 在 DB** | Bridge 允许维护只读 Schema 缓存，但任何决策以 `da_bridge_*` 表为准；缓存必须支持失效/热重载 |

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
│   ├── client.go            # 原生 MQTT 客户端（subscribe + publish）；推荐 eclipse/paho.mqtt.golang（EPL-2.0），引入前确认许可证兼容
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
                            │ Acquire advisory lock (HashToInt64("da-fleets-bridge"))
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
| **Start** | 执行 DB migration → 连接 PostgreSQL → 抢全局 HA advisory lock（`HashToInt64("da-fleets-bridge")`）→ 加载映射表与 Schema 缓存 → 建立 MQTT 连接（cleanSession=false，QoS 1，MQTT 5 启用 NoLocal）→ 批量 subscribe → 注册 message handler → 启动 in-flight TTL 清理 goroutine → 暴露 `/health` `/metrics` |
| **Per-Thing Mutation** | 处理命令下发、Jobs update、Shadow desired 映射等**变更单个 Thing 状态**的消息前，须先获取 per-thing advisory lock（如 `HashToInt64("da-bridge-" + thingName)`），与 Fleets 核心服务的 per-thing 锁隔离但遵循同一原则 |
| **Running** | 处理 MQTT 消息；定期从 DA/Fleets API 增量同步 Schema（`SCHEMA_SYNC_INTERVAL`）；清理过期 in-flight 命令并发布 FAILED 响应 |
| **Draining** | SIGTERM：停止消息处理 → 等待 in-flight 命令翻译完成（超时 `SHUTDOWN_TIMEOUT`）→ 断开 MQTT → 关闭 DB → 释放锁 |
| **Ready probe** | MQTT 已连接 + 全部 topic 已 subscribe + DB 可达 + advisory lock 持有 |
| **Live probe** | 进程存活 + MQTT reconnecting 算 live（不触发重启） |

**编排方式**（择一或组合）：

- `docker-compose.yml` 中 `da-bridge` 服务，`depends_on: [fleets, emqx, postgres]`（建议使用 `condition: service_healthy` 替代 `depends_on` 默认值）
- Fleets 安装包 systemd unit：`da-bridge.service` + `fleets.service`
- Kubernetes：`Deployment` sidecar 或独立 `Deployment` 共享 ConfigMap/Secret

### 4.4 MQTT 接入策略

| 方式 | 用途 | 说明 |
|------|------|------|
| **MQTT 原生客户端** | **Subscribe** + **上行 Publish** | EMQX REST API 不支持持久订阅；监听 + 上行发布均走 MQTT client |
| **HTTPS ThingDatas API** | Jobs proxy 模式（`start-next`、`update` 等请求） | 与 EMQX Rule → ThingDatas 路径一致；需 Fleets API Key 认证 |

> **决策**：上行所有 publish（Shadow reported、Event、Command response）统一使用 MQTT 原生客户端发布。Jobs proxy 模式下的请求/响应走 HTTPS ThingDatas API（更简单，无需额外订阅 `/accepted`/`/rejected` 响应 topic）。EMQX REST API 不再作为 Bridge 的主要发布通道，降低依赖。Fleets 内部仍使用 EMQX V5 REST API 发布（不受影响）。

**Bridge MQTT 连接参数**：

```bash
DA_BRIDGE_MQTT_URL=mqtt://localhost:1883
DA_BRIDGE_MQTT_CLIENT_ID=da-fleets-bridge-{instanceId}
DA_BRIDGE_MQTT_USERNAME=da-bridge-svc          # 独立服务账号
DA_BRIDGE_MQTT_PASSWORD=...
DA_BRIDGE_MQTT_CLEAN_SESSION=false             # 持久会话，避免重启丢订阅 + 消息积压
DA_BRIDGE_MQTT_QOS=1                           # 关键上下行统一 QoS 1
DA_BRIDGE_MQTT_NO_LOCAL=true                   # MQTT 5：不接收自己发布的消息；MQTT 3.1.1 无法设置，依赖 §6.1 的 payload 过滤
DA_BRIDGE_MQTT_KEEPALIVE=60
DA_BRIDGE_MQTT_CONNECT_TIMEOUT=30s
DA_BRIDGE_MQTT_RECONNECT_BACKOFF=1s,5s,15s,30s,60s  # 指数退避，上限 60s
```

> **Restart 行为**：`cleanSession=false` + QoS 1 保证重启期间消息不丢。重启后 MQTT broker 将积压消息推送给 Bridge。in-flight 命令表在 PG 中持久化，重启后恢复。
>
> **MQTT 版本要求**：强烈建议 Bridge 使用 **MQTT 5** 并启用 `NoLocal=true`。若受设备端限制只能使用 MQTT 3.1.1，则必须依赖 §6.1 所述的 `state.desired` 存在性过滤，避免自发布循环。

### 4.5 消息处理流水线

```
MQTT OnMessage
    │
    ▼
Topic Router ──► 解析 {productId(versioned), deviceId, thingName, executionId, jobId}
    │
    ▼
Mapping Lookup 1 ──► da_bridge_mappings WHERE da_product_id=? AND da_device_id=?
    │                 (productId 来自 topic 提取，使用 versioned 值)
    │                 miss → 丢弃 + metric + warn log
    ▼
Per-Thing Advisory Lock（变更类消息）──► pg_advisory_lock(HashToInt64("da-bridge-" + thingName))
    │
    ▼
Schema Lookup ──► da_bridge_mappings.da_base_product_id + schema_version
    │              → da_bridge_schema_mappings (mapping_spec)
    │
    ▼
Translator ──► payload 转换 + 校验
    │
    ▼
Publish ──► MQTT client (QoS 1) 或 HTTPS ThingDatas API
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
    id                  BIGSERIAL PRIMARY KEY,
    da_product_id       TEXT NOT NULL,           -- DA 侧 versioned productId（与 MQTT topic 中的 productId 一致，用于路由查找）
    da_base_product_id  TEXT NOT NULL,           -- DA 侧 baseProductId（跨版本稳定，用于 ThingType 查找）
    da_device_id        TEXT NOT NULL,
    da_namespace        TEXT NOT NULL DEFAULT 'default',
    thing_type_id       UUID NOT NULL,
    thing_id            UUID NOT NULL UNIQUE,
    thing_name          TEXT NOT NULL UNIQUE,
    mqtt_client_id      TEXT NOT NULL,           -- 用于 lifecycle 匹配
    schema_version      INTEGER NOT NULL DEFAULT 1,  -- 指向 da_bridge_schema_mappings.schema_version
    da_metadata         JSONB,
    last_synced_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (da_product_id, da_device_id)
);
CREATE INDEX idx_bridge_mappings_base_product ON da_bridge_mappings (da_base_product_id);
```

> **双 ID 设计**：DA MQTT topics 使用 versioned `productId`（如 `my-product-v2`），而 ThingType 命名和 schema 映射需要跨版本稳定的 `baseProductId`（如 `my-product`）。Bridge 从 topic 提取 `productId` 做路由查找，再用 `da_base_product_id` 查找 `da_bridge_schema_mappings` 与 ThingType。ThingType 名 `da-product-{baseProductId}` 因此稳定。Product 版本升级时更新同一 ThingType 的 schema，或用户手动指定新 ThingType 名隔离版本。

#### `da_bridge_schema_mappings`（Product ↔ ThingType 映射规则）

```sql
CREATE TABLE da_bridge_schema_mappings (
    id                  BIGSERIAL PRIMARY KEY,
    da_base_product_id  TEXT NOT NULL,          -- DA baseProductId（稳定，跨版本共用）
    thing_type_name     TEXT NOT NULL,
    schema_version      INTEGER NOT NULL DEFAULT 1,
    status              TEXT NOT NULL DEFAULT 'active',  -- draft | active | deprecated

    mapping_spec        JSONB NOT NULL,         -- 映射规则（见 §5.2）

    -- 快照（审计与 diff）
    da_device_spec      JSONB,                  -- 注册时的 DA Product DeviceSpec
    fleets_schema       JSONB,                  -- 注册时的 Fleets ThingType schema
    -- 关联的 versioned productIds（用于 track 哪些 product 版本引用此 schema）
    da_product_ids      TEXT[] NOT NULL DEFAULT '{}',

    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (da_base_product_id, schema_version)
);
CREATE INDEX idx_bridge_schema_mappings_base_product ON da_bridge_schema_mappings (da_base_product_id, status);
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
    "mode": "commandMap",
    "operationCommandMap": {
      "firmware_update": "startFirmwareUpdate"
    }
  }
}
```

> **注**：上例中 `jobs.mode` 从旧方案的 `"proxy"` 改为 `"commandMap"`，与推荐默认值一致。

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

> **注意**：Fleets 核心服务遵循「No In-Memory State」约束（AGENTS.md §7）。Bridge Agent 作为 Sidecar 允许维护**只读 Schema 缓存**，但须满足以下条件：
> - 缓存的权威来源始终是 `da_bridge_schema_mappings` 表；
> - 每条消息处理时若缓存未命中或版本不匹配，必须回退到 DB 查询；
> - 支持缓存失效/热重载，避免单点故障时丢失映射变更。

- 启动时全量加载 `status=active` 的 `da_bridge_schema_mappings`
- 每条 MQTT 消息优先查内存 map：`baseProductId → (SchemaMapping, version)`；未命中则同步查 PG
- Thing 表上 `schema_version` 决定选用哪个版本的 mapping_spec；旧版本标记 `deprecated` 后仍可服务存量设备
- 热更新：Bridge 定期检测 `da_bridge_schema_mappings` 的 `max(updated_at)` 变化，增量重载；同时可监听 PG `NOTIFY` 做近实时失效
- Schema drift 检测：可选定期调用 DA API 与 Fleets API 做 diff → 告警但**不自动覆盖**

---

## 6. 统一 Topic 路由表

以下为 Bridge Agent 订阅/发布的**完整 Topic 列表**，替代旧方案分散定义的 Rule #1–#6。

> **关键：productId 路由**。DA MQTT topics 使用 versioned `productId`（如 `smart-fan`），Bridge 订阅时通过通配符 `+` 匹配并提取该值，在 `da_bridge_mappings` 表中按 `da_product_id`（versioned）做第一次路由查找。找到映射后，再用 `da_base_product_id`（跨版本稳定的产品线标识）查找 `da_bridge_schema_mappings` 和 ThingType。详见 [§5.1](#51-数据模型) 表设计。

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

> **⚠️ 自发布过滤（S5 关键注意）**：Bridge 通过 P1 向 `$emqx/things/da-{encBasePid}--{encDid}/shadow/update` 发布 `state.reported`，同时通过 S5 订阅了同一 topic 通配符。**标准 MQTT 3.1.1 会将自己发布的消息也投递给匹配该 topic 的自身订阅**，除非客户端启用 MQTT 5 的 `NoLocal=true`。因此：
> - **MQTT 5**：Bridge 连接时务必设置 `NoLocal=true`（见 §4.4），从协议层避免自发布回环。
> - **MQTT 3.1.1 / 无法启用 NoLocal**：S5 handler 必须检查 payload，**仅当存在 `state.desired` 键时才处理**；仅含 `state.reported` 的消息直接丢弃。
>
> 该过滤逻辑是**必选**实现，不能仅作为「健壮性补充」。

> **⚠️ MQTT 通配符语义说明（S4–S8）**：MQTT `+` 是**单级通配符**，匹配任意内容（非前缀匹配）。因此 `$emqx/things/da-+/shadow/update` 会匹配 Thing 名**整级以 `da-` 开头**的所有 shadow/update topic。Bridge 在消息处理流水线（§4.5）中通过 `da_bridge_mappings` 表查找来做第二次过滤：查不到映射的消息直接丢弃并计数。
>
> **性能优化建议**：当桥接设备数量大时，通配符订阅会导致 Bridge 收到大量非桥接设备的下行消息（仅被快速过滤）。可考虑：
> - 启动/映射变更时从 `da_bridge_mappings` 动态构造**精确 topic 列表**并订阅，避免通配符；
> - 或使用 EMQX 共享订阅（Shared Subscription）做水平扩展，但会牺牲单设备消息顺序保证，需权衡。

> **关于 Jobs 请求类 topic**（`$emqx/things/da-+/jobs/get`、`jobs/start-next`、`jobs/{id}/update` 等）：原生 Fleets 设备自行 publish 这些 topic，Broker 上 EMQX Rule → ThingDatas API。桥接设备不 publish 这些 topic；由 Bridge 在 **proxy 模式**下通过 **HTTPS ThingDatas API** 代调用，或在 **commandMap 模式**下转化后 publish。因此 Bridge **不需要 subscribe** 这些请求 topic。

### 6.2 Bridge 发布（Publish）

| # | 方向 | MQTT Topic Pattern | 发布方式 | 说明 |
|---|------|-------------------|---------|------|
| P1 | 上行 | `$emqx/things/da-{encBasePid}--{encDid}/shadow/update` | MQTT client (QoS 1) | Shadow reported |
| P2 | 上行 | `$emqx/things/da-{encBasePid}--{encDid}/events/{eventType}` | MQTT client (QoS 1) | Event |
| P3 | 上行 | `$emqx/commands/things/da-{encBasePid}--{encDid}/executions/{id}/response` | MQTT client (QoS 1) | Command response |
| P4 | 下行 | `device-agent/{pid}/device/{did}/commands` | MQTT client (QoS 1) | Command 下发 + Shadow desired 映射 + Jobs commandMap |
| P5 | Jobs | `POST /api/v1/thing-datas/jobs/start-next` 等 | **HTTPS** | Jobs proxy 模式状态机操作（走 ThingDatas API） |

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

OUT $emqx/things/da-p-sensor--device-001/shadow/update (QoS 1)
    {"state":{"reported":{"temp_a":23.5}}}
```

> **注意**：示例 Thing 名 `da-p-sensor--device-001` 由 `baseProductId="p-sensor"` 编码而来，而非 versioned `productId`。

后续路径：Fleets 预配 Rule → `POST /thing-datas/shadow/reported/pg` + EMQX Tables `fleets_shadow_reported`（与原生设备一致）。

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
OUT $emqx/things/da-p-sensor--device-001/events/overheat (QoS 1)
    {"eventType":"overheat","severity":"warn","data":{"zone":"A","temperature":85.3}}
```

> **QoS**：Event 发布使用 QoS 1，与 Fleets 原生设备一致。

后续路径：Fleets 预配 Rule Bridge → EMQX Tables `fleets_events`（与原生设备一致）。

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
| `da_product_id` | DA 侧 versioned productId（用于 topic 重建与 response 下发通道） |
| `da_device_id` | DA 侧标识 |
| `da_request_id` | 下发 DA 时使用的 requestId |
| `fleets_action` | 原始 action |
| `expires_at` | TTL |
| `created_at` | 创建时间 |

**In-flight 命令表结构**（PG）：

```sql
CREATE TABLE da_bridge_inflight_commands (
    id              BIGSERIAL PRIMARY KEY,
    execution_id   UUID NOT NULL,            -- Fleets command execution ID
    thing_name     TEXT NOT NULL,            -- Fleets Thing 名（用于 response topic 构建）
    da_product_id  TEXT NOT NULL,            -- DA versioned productId（用于 DA command topic 构建）
    da_device_id   TEXT NOT NULL,            -- DA deviceId
    da_request_id  TEXT NOT NULL UNIQUE,     -- DA 侧 requestId（也作为 correlation key）
    fleets_action  TEXT NOT NULL,            -- 原始 Fleets action 名
    expires_at     TIMESTAMPTZ NOT NULL,     -- TTL 过期时间
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
- 发布后在 in-flight 表中标记完成或删除

```
OUT $emqx/commands/things/{thingName}/executions/{executionId}/response (QoS 1)
    {"status":"SUCCEEDED","result":{"estimatedDowntime":45},"timestamp":1715432405}
```

后续路径：Fleets 预配 Rule HTTP POST → ThingDatas API `POST /api/v1/thing-datas/command/response` → PG（与原生设备一致）。

> **幂等性**：`execution_id` 是 Fleets 生成的 UUID，in-flight 表以 `execution_id` 为唯一键。若 Bridge 重启后重复收到同一 command request，INSERT 应使用 `ON CONFLICT (execution_id) DO NOTHING`，避免重复下发。
>
> **TTL 清理**：后台 goroutine 每 30s 扫描 `expires_at < NOW()` 的条目 → 发布 `FAILED` 响应到 Fleets → 删除条目。

### 7.4 Telemetry 透传（展望）

远期可为需要高频上报且不适合走 Shadow reported 的 telemetry 类型（如 sensor 数据流），Bridge 可转发到用户自定义 topic 并配合 EMQX 规则写入 `fleets_telemetry`。本期不实现。

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
3. 查 `da_bridge_mappings` 得 `productId`、`deviceId`
4. 查 `mapping_spec.commands[action]` 得 `daCmd`、`inputMap`
5. `requestId` = `executionId`（直接映射，DA 与 Fleets 均用 UUID）
6. 写入 in-flight 表：`(execution_id, thing_name, product_id, device_id, request_id, fleets_action, expires_at=NOW()+ttl)`，使用 `ON CONFLICT (execution_id) DO NOTHING` 保证幂等
7. 发布 DA commands topic（QoS 1）：

```json
{
  "requestId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "cmd": "setInterval",
  "params": { "interval": 30 }
}
```

**解决 `ErrNoMatchingSubscribers`**：Bridge 作为 `$emqx/commands/things/da-+/executions/+/request` 的订阅者，Fleets JobService publish 时 Broker 上存在匹配订阅。Execution 状态机与原生设备完全一致。

**Fleets 控制台/API 路径不变**：`POST /api/v1/commands` → JobService → EMQX V5 REST API publish → Bridge → DA 设备。

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

**订阅**：`$emqx/things/da-+/shadow/update`（S5，过滤 desired 变更）、`$emqx/things/da-+/shadow/update/delta`（S6，delta 变更）

**desired payload 示例**（Fleets ShadowService 通过 EMQX V5 REST API 发布）：
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

1. 收到 S5 消息：检查 payload 是否存在 `state.desired`，若只有 `state.reported` 则跳过（Bridge 自身的上报消息）
2. 收到 S6 消息：直接取 payload 中的 delta 键
3. 对每个变更的 writable 字段查 `mapping_spec.properties[field].desiredApply`
4. 无 `desiredApply` 声明的 writable 字段 → 跳过 + warn 日志
5. `strategy=command` → 生成 DA 命令
6. 合并策略（配置项 `SHADOW_DESIRED_BATCH_MODE`）：

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

**Shadow desired 变更检测策略**：

| 订阅 | 用途 | 依赖 |
|------|------|------|
| S5 (`shadow/update`) | 获取完整 desired 状态。注意过滤 Bridge 自发布的 reported 消息 | payload 中 `state.desired` 存在性判定 |
| S6 (`shadow/update/delta`) | 直接获取 Fleets 计算的 delta diff | Fleets 已计算 delta |

> 建议 S5 作为 S6 的补充：首次启动时通过 S5 获取完整 desired，日常增量变更以 S6 delta 为主。

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
    │ publish notify/notify-next (via EMQX V5 REST API)
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
发布 device-agent/{pid}/device/{did}/commands (QoS 1)
    │
    ▼
收到 DA response → 构造 jobs/{jobId}/update 请求
    POST /api/v1/thing-datas/jobs/{jobId}/update
    body: {thingName, status: "SUCCEEDED"|"FAILED", statusDetails: {...}}
```

**Notify 去抖**：Fleets 可能在 Job 状态变化、新 Job 入队、执行完成等场景多次发布 notify。Bridge 应维护 `thingName → lastNotifiedAt` 或基于 notify payload 的 `timestamp` 做去抖/幂等，避免对同一设备频繁调用 `start-next`。

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

| # | Topic/API | 方向 | 模式 |
|---|-----------|------|------|
| S7 | `$emqx/things/da-+/jobs/notify` | MQTT sub | commandMap + proxy |
| S8 | `$emqx/things/da-+/jobs/notify-next` | MQTT sub | commandMap + proxy（防御性订阅） |
| — | `POST /api/v1/thing-datas/jobs/get` | HTTPS | proxy |
| — | `POST /api/v1/thing-datas/jobs/start-next` | HTTPS | commandMap + proxy |
| — | `POST /api/v1/thing-datas/jobs/{jobId}/update` | HTTPS | commandMap + proxy |
| P4 | `device-agent/{pid}/device/{did}/commands` | MQTT pub | commandMap |

> **HTTPS vs MQTT 决策**：`jobs/start-next` 和 `jobs/{jobId}/update` 统一走 HTTPS ThingDatas API（简单、与 EMQX Rule → ThingDatas 路径一致，无需额外 subscribe `/accepted`/`/rejected` 响应 topic）。`notify` / `notify-next` 走 MQTT subscribe（Fleets JobService 通过 EMQX V5 REST API publish 推送，Bridge 必须订阅才能接收）。

---

## 10. 设备注册与元数据同步

### 10.1 注册流程

```
DA Device 创建（productId=versioned, baseProductId=stable, deviceId, device.metadata.mqttClientId）
    → device-management 注册回调（PC2）
    → DA IoT Platform (fleets) 插件
        1. 读 device.metadata.mqttClientId（或按约定生成 {namespace}/dev/{deviceId} 并回写）
        2. 生成 ThingType schema 草稿 + mapping_spec 草稿
        3. 用户修订
        4. POST Fleets /api/v1/thing-types（ThingType 名 = da-product-{baseProductId}）
        5. POST Fleets /api/v1/things
           - name: da-{encBasePid}--{encDid}
           - thingTypeId: 从步骤 4 获取
           - mqttClientId: 步骤 1 获取的值
           - tags: [source=device-agent, da:product={baseProductId}, da:device={deviceId}]
        6. POST Bridge Admin /api/v1/schema-mappings
           - da_base_product_id = baseProductId
           - da_product_ids[] 追加当前 versioned productId
        7. POST Bridge Admin /api/v1/mappings
           - da_product_id = versioned productId（MQTT topic 中使用的值）
           - da_base_product_id = baseProductId
           - da_device_id = deviceId
           - thing_id / thing_name / mqtt_client_id
    → Bridge Schema Registry 热加载
```

**容错与最终一致**：步骤 4–7 并非原子操作。DA IoT 插件应：
- 将失败步骤放入本地 retry queue（内存或 DB），按指数退避重试；
- 启动时/定时调用 Bridge Admin `/api/v1/mappings/reconcile` 与 Fleets `GET /api/v1/things?tagName=da-bridge` 做全量对账，补全缺失映射；
- Bridge 侧对重复写入使用 `ON CONFLICT DO UPDATE`，保证最终一致。

### 10.2 Thing 命名

| 项 | 格式 | 示例 |
|----|------|------|
| Thing | `da-{encodedBaseProductId}--{encodedDeviceId}` | `da-p-sensor--device-001` |
| ThingType | `da-product-{baseProductId}` | `da-product-p-sensor` |
| Tags | `source=device-agent`, `da:product={baseProductId}`, `da:device={deviceId}` | — |

**注意**：使用 DA 的 `baseProductId`（跨版本稳定）而非带版本的 `productId`。

### 10.3 mqttClientId 格式

DA Device 模型**无内置 `mqttClientId` 字段**。DA `DeviceInfo.metadata` 是 `Record<string, any>`（JSONB），插件可直接在该字段中读写 mqttClientId。本方案采用以下约定：

| 层 | 约定 | 示例 |
|----|------|------|
| **设备直连** | 设备连接 EMQX 时使用 Client ID `{namespace}/dev/{deviceId}` | `default/dev/device-001` |
| **DA IoT 插件** | 注册 Fleets Thing 时，从 `device.metadata.mqttClientId` 读取 Client ID（如未设置则按约定自动生成 `{namespace}/dev/{deviceId}` 并回写 metadata） | — |
| **Fleets lifecycle** | Thing.`mqttClientId` 字段 = 上述值，用于 EMQX lifecycle 事件匹配 | — |

**前置条件**：此约定需要集成方在设备端实际使用该 Client ID 连接 EMQX。若设备端无法满足，需在 `device.metadata.mqttClientId` 显式记录实际 Client ID。

> 此问题需与 Device Agent 团队对齐并写入 DA 文档，见 [§18 Q3](#181-阻塞项phase-1-前必须定案)、[Q12](#183-device-agent-侧)。

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

> **DA API 路径校正**：DA 实际路径不含 `/v1` 前缀；devices 在 products 下嵌套。路径 `GET /api/products/{productId}` 中的 `productId` 为 versioned id（如 `smart-fan-v1-1`）。

> **Fleets tag 过滤（Q23 已确认）**：`docs/tags.md` 确认 Thing/ThingType API 列表过滤使用 `?tagName=a&tagName=b`（按 tag 名称 AND 过滤）或 `?tagId=1&tagId=2`（按 ID AND 过滤）。推荐 tag 命名使用 `da-bridge`（简洁），而非 `source=device-agent`（易被误读为 key=value 对）。对应的 reconcile 查询为 `GET /api/v1/things?tagName=da-bridge`。

---

## 11. Fleets 与 Device Agent 侧协作

### 11.1 Fleets 侧改动（最小集）

| 改动 | 说明 |
|------|------|
| `da_bridge_*` 表 migration | 纳入 Fleets `migrations/` 目录 |
| **移除** EMQX da-bridge Rule 注入 | 新方案不再依赖旧方案 BridgePlugin + Rule Manager |
| docker-compose / 安装脚本 | 增加 `da-bridge` 服务定义（若 Bridge 放在独立仓库，则仅引用镜像） |
| AGENTS.md 架构例外条款（若 Bridge 归 Fleets 仓库） | 明确「Fleets HTTP 核心服务不订阅 MQTT；Bridge Agent 是唯一授权的 MQTT subscriber」 |
| 文档 | 桥接设备能力边界、Jobs mode 说明 |

**明确不改动**：`JobService`、`ShadowService`、ThingDatas Handler、Fleets 预配 EMQX 规则、ThingService（旧方案的 BridgeHooks 不再需要）。

**仓库位置建议**：见 [§1.4](#14-与-fleets-架构约束的关键张力)。推荐独立仓库；次选 `fleets/cmd/da-bridge`。

### 11.2 Device Agent IoT Platform 插件

> **注意**：此插件在 Device Agent 代码库中**尚不存在**（`apps/agent-gateway/src/platforms/` 目录需新建），需新建开发（前置条件 PC1）。以下是目标设计。

保留 [旧方案 §6](./device-agent-fleets-integration.md#6-device-agent-端-iot-platform-插件设计) 的框架定位，调整：

| 变更 | 说明 |
|------|------|
| **新建插件模块** | 在 `apps/agent-gateway/src/` 下创建 `platforms/fleets/` 插件模块（新目录，参考 DA 中 `http-api/`、`transports/mqtt` 等模块组织方式） |
| `schema-mapper.ts` 输出 | 同时生成 Fleets ThingType schema + `mapping_spec` JSON |
| 注册 API | POST Fleets Thing/ThingType + POST Bridge Admin schema-mappings |
| MQTT | 插件**仍不订阅** MQTT |
| namespace | 注册时携带 DA namespace（存 `da_bridge_mappings.da_namespace`） |
| baseProductId | ThingType 名使用 `baseProductId` 而非 `productId` |
| 回调依赖 | 需 DA `packages/device-management/src/service.ts` 提供 `onDeviceRegistered` / `onProductChanged` 回调（前置条件 PC2）。失败不阻断 DA 设备注册主流程 |

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
Bridge Agent ──Basic Auth──► Fleets REST API（读 thing/type/shadow；Jobs proxy 写 thing-datas）
Bridge Agent ──Bearer/Internal──► Device Agent REST API
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

### 12.3 API Key 最小权限

**Bridge → Fleets**（Fleets 使用 Basic Auth 认证，需 API Key + API Secret）：

```json
{
  "permissions": {
    "GET": [
      "/api/v1/things",
      "/api/v1/thing-types",
      "/api/v1/things/*/shadow"
    ],
    "POST": [
      "/api/v1/thing-datas/jobs/get",
      "/api/v1/thing-datas/jobs/start-next",
      "/api/v1/thing-datas/jobs/*/update"
    ]
  }
}
```

> **说明**：权限路径为前缀匹配，`*` 表示单段通配。具体格式需与 Fleets `docs/api-auth-and-key-management.md` 中对 `permissions` 的定义对齐。Bridge 不调用 ThingDatas 的 `/shadow/reported` 等上行入口（这些由 EMQX Rule 处理）。

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

# MQTT（原生客户端，subscribe + 上行 publish；强烈建议 MQTT 5）
DA_BRIDGE_MQTT_URL=mqtt://emqx:1883
DA_BRIDGE_MQTT_VERSION=5                    # 3 | 5；5 支持 NoLocal
DA_BRIDGE_MQTT_CLIENT_ID=da-fleets-bridge-1
DA_BRIDGE_MQTT_USERNAME=da-bridge-svc
DA_BRIDGE_MQTT_PASSWORD=...
DA_BRIDGE_MQTT_CLEAN_SESSION=false
DA_BRIDGE_MQTT_QOS=1
DA_BRIDGE_MQTT_NO_LOCAL=true                # MQTT 5 only
DA_BRIDGE_MQTT_KEEPALIVE=60

# PostgreSQL（与 Fleets 同库）
DATABASE_URL=postgres://postgres:postgres@postgres:5432/fleets

# Fleets API（Bridge 读 shadow/thing/type + Jobs proxy ThingDatas）
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
    healthcheck:
      test: ["CMD", "wget", "-qO-", "http://localhost:8080/ready"]
      interval: 10s
      timeout: 3s
      retries: 3

  da-bridge:
    image: fleets-da-bridge:latest
    depends_on:
      fleets:
        condition: service_healthy
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
        reservations:
          cpus: "0.5"
          memory: 256M

  emqx:
    # ... EMQX 配置
    # 需预创建 da-bridge-svc 用户并配置 ACL：
    #   emqx ctl acl add 'da-bridge-svc' pub '$emqx/things/da-#'
    #   emqx ctl acl add 'da-bridge-svc' pub '$emqx/commands/things/da-#'
    #   emqx ctl acl add 'da-bridge-svc' sub 'v1/+/+/+'
    #   emqx ctl acl add 'da-bridge-svc' sub 'device-agent/+/#'
    #   emqx ctl acl add 'da-bridge-svc' sub '$emqx/commands/things/da-#'
    #   emqx ctl acl add 'da-bridge-svc' sub '$emqx/things/da-#'
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
| **Active-Standby**（首期） | `pg_try_advisory_lock(HashToInt64("da-fleets-bridge"))` 选主；主实例持有 MQTT 连接；备实例仅 `/health` + 就绪接管 |

**锁 ID**：`HashToInt64("da-fleets-bridge")` 仅用于 HA 选主；处理具体消息时的 per-thing 锁见 §4.3。

**MQTT clientId**：Active 实例使用固定 `clientId`；Standby 实例不连 MQTT。Failover 时 Standby 获取锁后连接 MQTT，依赖 `cleanSession=false` 恢复订阅与积压消息。

> **注意**：Fleets 主进程使用 pg advisory lock 做 per-thing 状态锁（非全局锁）。Bridge 使用的 `HashToInt64("da-fleets-bridge")` 是全局 HA 锁，与 per-thing 锁互不冲突。
>
> **局限性**：Active-Standby 是单点转发瓶颈。若 Bridge 所在实例故障，Failover 期间消息由 Broker 队列缓冲；但若希望多 Active 实例负载分担，需要解决消息顺序和 per-thing 状态一致性问题，本期不展开。

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
| Fleets 侵入 | BridgePlugin + Rule Manager + ThingService hooks | 仅 Migration + 编排 Sidecar；但需架构上明确 Bridge 为唯一 MQTT 订阅者 |
| EMQX 依赖 | 高（Rule SQL 版本） | 低（仅预配规则；Go 逻辑替代 SQL） |
| 架构约束 | 不违反「Fleets 不订阅 MQTT」 | 需要显式例外/独立仓库，否则与 AGENTS.md §0 冲突 |
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

> **前置依赖**：PC1（DA IoT Platform 插件）、PC2（DA 注册回调）、PC6（Fleets 架构团队确认 Bridge 归属/例外）必须在 Phase 1 启动前定案。

| 任务 | 说明 |
|------|------|
| 仓库初始化 + 项目骨架 + 生命周期 | 独立仓库或 `fleets/cmd/da-bridge`；Start/Stop/Ready |
| MQTT client（MQTT 5 + NoLocal） + unified topic router | §6 subscribe S1–S3、publish P1–P2（QoS 1） |
| `da_bridge_mappings` + `da_bridge_schema_mappings` migration | PG 表；纳入 Fleets `migrations/000005_da_bridge.*.sql` |
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
| S5 自发布过滤（state.desired 存在性检查） | §6.1 |
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
| 状态上行 | `v1/{pid}/{did}/telemetry` | `$emqx/things/da-{encBasePid}--{encDid}/shadow/update` |
| 事件上行 | `v1/{pid}/{did}/event` | `$emqx/things/da-{encBasePid}--{encDid}/events/{eventType}` |
| 命令响应 | `device-agent/{pid}/device/{did}/responses` | `$emqx/commands/things/da-.../executions/{id}/response` |
| 命令下行 | `device-agent/{pid}/device/{did}/commands` | `$emqx/commands/things/da-.../executions/{id}/request` |
| Shadow 下行 | ↑ commands（合成） | `$emqx/things/da-.../shadow/update` |
| Jobs | ↑ commands 或 Bridge 代发 | `$emqx/things/da-.../jobs/*` |

### B. encode/decode（与旧方案兼容）

```go
// encodeBridgeSegment / decodeBridgeSegment
// 用于 Thing 名中的 baseProductId 和 deviceId 编码
// '/' → '_slash_', '--' → '_dashdash_'
// 先 encode '--' 后 '/'，保证双向对称

// Thing name = "da-" + encodeBridgeSegment(baseProductId) + "--" + encodeBridgeSegment(deviceId)
```

### C. DA REST API 参考

DA REST API **不含 `/v1` 前缀**，所有路径在 `/api/` 下。

| 用途 | 实际路径 | 返回数据 | 说明 |
|------|---------|---------|------|
| 获取 Product（按 id） | `GET /api/products/{productId}` | `ProductInfo`（含 `baseProductId`、`version`、`properties`、`commands`、`events`） | `productId` 为 versioned id（如 `smart-fan-v1-1`） |
| 获取 Device | `GET /api/products/{productId}/devices/{deviceId}` | `DeviceInfo`（含 `metadata` JSONB、`productId`） | Device 嵌套在 product 路径下 |
| Product 列表 | `GET /api/products` | `ProductInfo[]` | 可选 `?lifecycleStatus=published` |
| Device 列表 | `GET /api/products/{productId}/devices` | `DeviceInfo[]` | 分页：`?page=&pageSize=` |
| 发布 Product | `POST /api/products/{productId}/publish` | `ProductInfo` | 状态 developing → published |

**DA Device 模型关键字段**（`DeviceInfo`）：
- `id` — deviceId（全局唯一，MQTT topic 中用此值）
- `productId` — 关联的 versioned product id（可为 null）
- `metadata` — `Record<string, any>`（可用于存储 mqttClientId）

**Fleets ThingDatas API 参考**（Bridge 调用）：

| 用途 | 路径 | 认证方式 |
|------|------|---------|
| Jobs get | `POST /api/v1/thing-datas/jobs/get` | Basic Auth |
| Jobs start-next | `POST /api/v1/thing-datas/jobs/start-next` | Basic Auth |
| Jobs update | `POST /api/v1/thing-datas/jobs/{jobId}/update` | Basic Auth |

### D. 版本记录

| 版本 | 日期 | 变更 |
|------|------|------|
| v1.0 | 2026-07-02 | 初稿：独立 Bridge Agent 进程方案 |
| v1.1 | 2026-07-02 | 评审优化：统一 Topic 路由表、per-property desiredApply、in-flight PG 持久化、DA API 路径校正、ThingType 使用 baseProductId、移除旧方案 BridgePlugin、Jobs 双模式详化、command 序列图 |
| v1.2 | 2026-07-02 | 代码库调研评审：纠正 `da_product_id` 语义（新增 `da_base_product_id` 列）、修正 advisory lock ID、新增前置条件 §1.3、移除 EMQX REST API 依赖、mqttClientId 来源修正、DA IoT 插件标注为待开发、§18 全部问题给出评审结论并新增 Q16–Q20 |
| **v1.3** | **2026-07-02** | **双代码库（Fleets AGENTS.md + DA CLAUDE.md）评审优化**：发现 S5/P1 自发布循环风险并添加过滤机制（§6.1）；确认 Fleets 使用 EMQX V5 REST API 发布下行（非 MQTT，方案正确）；docker-compose 改进为 `condition: service_healthy`（§13.2）；`mapping_spec` 示例中对齐推荐默认值 `jobs.mode: "commandMap"`（§5.2）；Shadow desired 变更检测策略说明（§8.2）；§18 全面重审：已解决问题（Q16–Q20）移入归档，新增 Q21–Q23（自发布过滤、Fleets tag 查询参数不确定性、delta 检测策略）。 |
| **v1.4** | **2026-07-02** | **二轮深度评审（Fleets `docs/tags.md` + DA `docs/mqtt-runtime-rules.md`）**：验证 Q23（tag 查询参数 `?tagName=` 格式，推荐 tag 名 `da-bridge`）；逐条核查 §18 所有问题，Q21–Q23 移入已解决归档；补充 in-flight 命令表 CREATE TABLE SQL（§7.3）；补充 docker-compose 资源限制 + EMQX ACL 配置注释（§13.2）；澄清 MQTT 通配符 `da-+` 语义（§6.1）；DA MQTT topics 与 `mqtt-runtime-rules.md` 逐条一致性验证（核查 C1–C5）。 |
| **v1.5** | **2026-07-02** | **三轮评审优化**：修正 S5 自发布过滤的 MQTT 语义错误（§6.1）；新增 §1.4 阐述与 Fleets「不订阅 MQTT」约束的张力；统一上下行 QoS 1 + MQTT 5 NoLocal（§2.2、§4.3、§4.4、§6.2、§7.x）；引入 per-thing advisory lock（§4.3、§4.5、§8.1、§14.1）；修正 Thing 名编码不一致（encPid → encBaseProductId，§6.2、§7.1、§17.A）；明确 in-flight 幂等性、Jobs notify 去抖、映射注册容错与对账；更新 Phase 1 前置依赖；§18 重审并新增 Q24–Q30。 |

---

## 18. 待定问题

以下问题基于对 Fleets（AGENTS.md、`docs/tags.md`）和 Device Agent（CLAUDE.md、`docs/mqtt-runtime-rules.md`）双代码库的实际架构调研。本轮（v1.5）重点修正了 S5 自发布语义、QoS/NoLocal、per-thing advisory lock、Thing 名编码一致性，并新增与 Fleets「不订阅 MQTT」约束的张力分析。

### 18.1 阻塞项（Phase 1 前必须定案）

| # | 问题 | 评审结论（v1.5 状态） | 待确认 |
|---|------|---------------------|--------|
| **Q1** | **Bridge Agent 仓库位置与架构归属** | 与 Fleets AGENTS.md §0「Fleets NEVER subscribes to MQTT」存在张力。建议：方案 A）独立仓库 `emqx/da-fleets-bridge`，Fleets 仅保留 migration + 编排引用；方案 B）`fleets/cmd/da-bridge` 但须 AGENTS.md 明确例外。详见 [§1.4](#14-与-fleets-架构约束的关键张力)。 | **Fleets 架构团队** |
| **Q2** | **`da_bridge_*` 表归属** | 纳入 Fleets `migrations/` 目录（`000005_da_bridge.up.sql`，已验证无冲突）。无论 Bridge 仓库在哪，表 migration 应随 Fleets schema 演进。 | **Fleets 团队**确认 |
| **Q3** | **`mqttClientId` 稳定性** | DA Device 模型无内置 `mqttClientId` 字段。`DeviceInfo.metadata` 是 JSONB，可用于存储。建议：约定 Client ID 格式 `{namespace}/dev/{deviceId}`，DA IoT 插件注册 Thing 时自动按此生成并写入 `device.metadata.mqttClientId`。 | **Device Agent 团队** + 集成方 |
| **Q4** | **ThingType 用户修订 UI** | Phase 1 使用 JSON 导出 + 手工编辑 + Bridge Admin API 写入。Phase 2 在 DA Workspace 设置页追加。 | 产品经理确认优先级 |
| **Q5** | **Bridge Admin API 认证与权限** | 建议复用 Fleets Basic Auth API Key。Bridge Admin 独立监听端口（如 8091），路径前缀 `/api/v1/*`。具体 `permissions` 格式需与 Fleets 文档对齐。 | **Fleets 团队**确认 |
| **Q6** | **MQTT 5 NoLocal 可行性** | 强烈建议 Bridge 使用 MQTT 5 并启用 `NoLocal=true`。若 EMQX 部署或 MQTT 客户端库限制只能使用 MQTT 3.1.1，则 S5 自发布过滤（`state.desired` 存在性）必须完整实现并通过测试。 | **运维/EMQX 团队**确认 |

### 18.2 架构决策

| # | 问题 | 评审结论（v1.5 状态） | 状态 |
|---|------|---------------------|------|
| **Q7** | **Shadow desired 合并下发** | 默认 `perField`（每个 writable property 独立发一条 DA 命令）。`singleCommand` 需在 mapping_spec 中显式声明。 | 保持 |
| **Q8** | **Jobs proxy 模式走 MQTT 还是 HTTPS** | ✅ **已确认**：HTTPS ThingDatas API。 | **已确认** |
| **Q9** | **未注册设备 telemetry** | 丢弃 + metric。注册时序由 DA IoT 插件保障；Bridge 提供对账 API 补全。 | 保持 |
| **Q10** | **per-thing advisory lock 范围** | Bridge 处理命令下发、Jobs update、Shadow desired 映射等 per-thing 变更前，须获取 `HashToInt64("da-bridge-" + thingName)` 锁。与 Fleets 核心 per-thing 锁隔离。 | 保持 |
| **Q11** | **动态精确订阅 vs 通配符订阅** | 当前方案使用通配符 `da-+` 订阅，实现简单但会收到所有 `da-*` Thing 的下行消息。大规模部署时可改为从 `da_bridge_mappings` 动态构造精确 topic 列表订阅。 | Phase 1 不阻塞 |

### 18.3 Device Agent 侧

| # | 问题 | 评审结论（v1.5 状态） | 待确认 |
|---|------|---------------------|--------|
| **Q12** | **IoT 插件回调挂载点** | DA `packages/device-management/src/service.ts` 当前无设备注册回调。需增加事件发射机制（PC2）。 | **DA 团队**确认设计并排期 |
| **Q13** | **DA 插件是否直连 PG 写 mapping_spec** | 仅通过 Bridge Admin API（单写入口）。DA 不可达时入 retry queue；启动时做对账。 | DA 团队确认 |
| **Q14** | **DA API 是否暴露 device metadata 中的 mqttClientId** | ✅ **已确认**：`DeviceInfo.metadata` 为 JSONB，REST API 返回此字段。 | **已确认** |
| **Q15** | **DA 命令 payload 格式** | 本方案假设 DA 命令为 `{requestId, cmd, params}`，response 为 `{requestId, code, message, data}`。需与 DA `packages/device-sdk` 及 `docs/mqtt-runtime-rules.md` 中的实际协议对齐。 | **DA 团队**确认 |
| **Q16** | **Event severity 动态映射** | ✅ **已确认**：mapping_spec 支持 `fleetsSeverity` 字段。 | **已确认** |
| **Q17** | **Bridge namespace 多实例隔离** | Phase 1 一 Bridge 一 namespace。远期扩展需改造 topic 格式和映射表。 | Phase 1 不阻塞 |
| **Q18** | **自定义 telemetry 高频通道** | 远期。Bridge 不实现该通道，DA `v1/{pid}/{did}/telemetry` 非 state 类型暂不处理。 | 远期，不阻塞 |

### 18.4 已解决问题（归档）

| # | 问题 | 解决方案 | 解决版本 |
|---|------|---------|---------|
| Q19 | `da_product_id` 语义纠正 | 新增 `da_base_product_id` 列；`da_product_id` 存 versioned 值 | v1.2 |
| Q20 | advisory lock ID 一致性 | 全局 HA 锁统一为 `HashToInt64("da-fleets-bridge")` | v1.2 |
| Q21 | EMQX REST API 是否必需 | 已移除 EMQX REST API 依赖 | v1.2 |
| Q22 | DA API 路径不含 `/v1` 前缀 | 修正为 `/api/products/...` | v1.2 |
| Q23 | Product 版本升级时桥设备 Thing 更新 | DA IoT 插件 reconcile + Bridge Admin API 覆盖写入 | v1.2 |
| Q24 | S5 自发布过滤 | 修正 MQTT 语义：MQTT 3.1.1 会自投递；MQTT 5 用 `NoLocal=true`；S5 handler 必选 `state.desired` 过滤 | v1.5 |
| Q25 | Shadow desired 变更检测策略 | S5 获取初始全量 + S6 delta 增量 + Fleets GET API 恢复 | v1.3 |
| Q26 | Fleets tag 查询参数 | ✅ 已验证：`?tagName=da-bridge` | v1.4 |
| Q27 | Thing 名编码一致性 | 统一使用 `baseProductId` 编码：`da-{encBasePid}--{encDid}` | v1.5 |
| Q28 | 上下行 QoS | 统一关键 topic 为 QoS 1 | v1.5 |
| Q29 | per-thing advisory lock | 处理 per-thing 变更前加锁 | v1.5 |
| Q30 | in-flight 命令幂等性 | `execution_id` 唯一键 + `ON CONFLICT DO NOTHING` | v1.5 |

### 18.5 本轮评审核查项（v1.5）

| # | 核查项 | 结论 |
|---|--------|------|
| **C1** | **DA MQTT topics 与方案 §6 一致性** | ✅ 一致。 |
| **C2** | **S5 自发布语义** | ⚠️ **v1.5 修正**：原方案错误认为 EMQX 默认不投递自发布消息。实际 MQTT 3.1.1 会自投递，必须依赖 `NoLocal` 或 payload 过滤。 |
| **C3** | **Fleets「不订阅 MQTT」约束** | ⚠️ **v1.5 新增风险**：Bridge Agent 必须订阅 MQTT，需架构团队明确归属或例外。 |
| **C4** | **Thing 名编码一致性** | ✅ v1.5 统一为 `encBaseProductId`。 |
| **C5** | **QoS/NoLocal** | ✅ v1.5 明确要求关键 topic QoS 1 + MQTT 5 NoLocal。 |
| **C6** | **per-thing advisory lock** | ✅ v1.5 新增，与 Fleets 约束对齐。 |
| **C7** | **in-flight 幂等性与 TTL** | ✅ v1.5 明确 `execution_id` 唯一键与幂等插入。 |
| **C8** | **Jobs notify 去抖** | ✅ v1.5 新增去抖要求。 |
| **C9** | **映射注册容错** | ✅ v1.5 新增 retry queue + reconcile。 |

---

> **文档维护者**: 集成团队
> **相关文档**: [旧方案 v2.5](./device-agent-fleets-integration.md) · Fleets [AGENTS.md](https://github.com/emqx/fleets/blob/main/AGENTS.md) · [tags.md](https://github.com/emqx/fleets/blob/main/docs/tags.md) · Device Agent [CLAUDE.md](https://github.com/emqx/device-agent/blob/main/CLAUDE.md) · [mqtt-runtime-rules.md](https://github.com/emqx/device-agent/blob/main/docs/mqtt-runtime-rules.md)
