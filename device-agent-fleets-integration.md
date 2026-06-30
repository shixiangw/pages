# Device-Agent × Fleets 双端插件集成方案

> **版本:** v2.1  
> **日期:** 2026-06-30  
> **状态:** 最终修订版（EMQX Rule 纯 MQTT topic/payload 转换，不复用 HTTP API）

---

## 目录

1. [核心目标与设计原则](#1-核心目标与设计原则)
2. [架构总览](#2-架构总览)
3. [设备模型差异分析与映射](#3-设备模型差异分析与映射)
4. [EMQX Rule Engine 规则设计](#4-emqx-rule-engine-规则设计)
5. [Fleets 端插件（device-agent-bridge）设计](#5-fleets-端插件-device-agent-bridge-设计)
6. [Device-Agent 端插件（fleets-bridge）设计](#6-device-agent-端插件-fleets-bridge-设计)
7. [认证与安全](#7-认证与安全)
8. [Fleets API 扩展规范](#8-fleets-api-扩展规范)
9. [配置与部署](#9-配置与部署)
10. [实施路线图](#10-实施路线图)
11. [附录](#11-附录)

---

## 1. 核心目标与设计原则

### 1.1 核心目标

**让 device-agent 定义和管理的设备也能被 fleets 同时管理**，最终用户可以通过 fleets 统一控制台查看和操作两类设备（fleets 原生设备 + device-agent 设备），而无需关心底层接入协议的差异。

### 1.2 设计原则

| 原则 | 说明 |
|------|------|
| **可选启用** | 两个插件默认不启用，启用不影响各自系统原生功能 |
| **无侵入** | 不修改 fleets / device-agent 核心代码，以插件形式扩展 |
| **DA 主动注册** | device-agent 端是设备注册的唯一发起方，fleets 端不自动发现 DA 设备 |
| **Fleets 不订阅 MQTT** | Fleets Plugin 不内嵌 MQTT 客户端，通过 EMQX Rule API 注入规则 |
| **复用 EMQX 凭据** | Fleets Plugin 复用 fleets 已有的 `EMQX_API_KEY` / `EMQX_API_SECRET` |
| **松耦合** | 两插件仅通过 EMQX Broker + Fleets REST API 协作 |
| **可观测** | 所有桥接操作均有日志和事件追踪 |

### 1.3 关键概念定义

| 概念 | 说明 |
|------|------|
| **DA 设备** | 通过 device-agent 定义、使用 device-agent SDK 接入 EMQX 的设备 |
| **Fleets 原生设备** | 直接通过 fleets 管理的、遵循 `$emqx/things/{name}/...` 协议的设备 |
| **桥接设备** | DA 设备在 fleets 中的影子表示，以 fleets Thing 形态存在 |
| **EMQX Rule** | EMQX Broker 规则引擎，用于 MQTT 消息的过滤、转换与转发 |
| **设备类型映射** | DA Product → Fleets ThingType 的自动转换规则 |

---

## 2. 架构总览

### 2.1 整体架构图

```
                        ┌──────────────────────────────────────────┐
                        │              EMQX Broker                 │
                        │                                          │
                        │  ┌────────────────────────────────────┐  │
                        │  │  device-agent-bridge 注入的规则     │  │
                        │  │                                    │  │
                        │  │  v1/{pid}/{did}/telemetry           │  │
                        │  │    ↓ DA topic+payload 转换           │  │
                        │  │    ↓ republish $emqx/things/{name}/  │  │
                        │  │      shadow/update                  │  │
                        │  │                                    │  │
                        │  │  v1/{pid}/{did}/event              │  │
                        │  │    ↓ republish $emqx/things/{name}/ │  │
                        │  │      events/update                 │  │
                        │  │                                    │  │
                        │  │  device-agent/{pid}/.../responses  │  │
                        │  │    ↓ republish $emqx/things/{name}/ │  │
                        │  │      command/response              │  │
                        │  └────────────────────────────────────┘  │
                        │                                          │
                        │  DA 原始主题域     Fleets 主题域          │
                        │  v1/{pid}/{did}    $emqx/things/{name}   │
                        └──────────────────────────────────────────┘
                              │                          │
                              │ MQTT                     │ MQTT
                              │                          │
                     ┌────────┴────────┐      ┌─────────┴─────────┐
                     │  DA 设备        │      │  Fleets 原生设备    │
                     │  (DA SDK)       │      │  + 桥接设备        │
                     └─────────────────┘      └───────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│                        Fleets 实例                                    │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │  Fleets 核心                                                    │  │
│  │  ThingService / ThingTypeService / ShadowService / JobService  │  │
│  │  PostgreSQL (things, thing_types, shadows, jobs, ...)          │  │
│  │  da_bridge_mappings 表 (DA ↔ Fleets 映射)                      │  │
│  │  预配的 EMQX 规则 ($emqx/things/{name}/... → ThingDatas API)  │  │
│  └────────────────────────────────────────────────────────────────┘  │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │  device-agent-bridge Plugin (Go)                               │  │
│  │  职责: 通过 EMQX REST API 管理 DA→Fleets 的规则注入/移除        │  │
│  │      复用 fleets 的 EMQX_API_KEY / EMQX_API_SECRET            │  │
│  │  ┌─────────────────────────────────────────────────────────┐  │  │
│  │  │ • EMQX Rule 管理器（增删查 EMQX 规则 + 动作）              │  │  │
│  │  │ • 内置规则模板（DA Telemetry/Event/Command 三套规则）       │  │  │
│  │  │ • 映射表维护（DA Plugin 注册 Thing 时自动写入）             │  │  │
│  │  │ • Rule 健康状况监控                                        │  │  │
│  │  └─────────────────────────────────────────────────────────┘  │  │
│  └────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────┘
                              ▲
                              │ REST API (Basic Auth)
                              │
┌─────────────────────────────┴────────────────────────────────────────┐
│  Device-Agent 实例                                                     │
│                                                                       │
│  ┌─────────────────────────────────────────────────────────────────┐  │
│  │  Device-Agent 核心                                               │  │
│  │  ProductService / DeviceService / Agent Gateway                 │  │
│  └─────────────────────────────────────────────────────────────────┘  │
│                                                                       │
│  ┌─────────────────────────────────────────────────────────────────┐  │
│  │  fleets-bridge Plugin (TypeScript Channel)                       │  │
│  │  职责: 设备注册的唯一入口, 调用 Fleets API 创建 Thing & ThingType │  │
│  │  ┌──────────────────────────────────────────────────────────┐   │  │
│  │  │ • Fleets REST API Client (Basic Auth)                    │   │  │
│  │  │ • 设备注册器 (监听 DeviceService → 调用 fleets API)      │   │  │
│  │  │ • 状态同步器 + 影子同步器                                │   │  │
│  │  │ • 本地 SQLite 映射存储 + 同步队列                        │   │  │
│  │  └──────────────────────────────────────────────────────────┘   │  │
│  └─────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────┘
```

### 2.2 核心数据流

#### 2.2.1 完整的设备接入与数据流转

```
时序1: DA Plugin 注册设备
═══════════════════════════════════════════════════

DA Device              DA Plugin                   Fleets API              EMQX
   │                       │                          │                    │
   │ 1. 设备创建            │                          │                    │
   │─────────────────────►  │                          │                    │
   │                       │                          │                    │
   │                       │ 2. 确保 ThingType 存在    │                    │
   │                       │    POST /api/v1/thing-types│                   │
   │                       │ ────────────────────────►│                    │
   │                       │◄──────────────────────── │                    │
   │                       │                          │                    │
   │                       │ 3. 注册设备               │                    │
   │                       │    POST /api/v1/things    │                    │
   │                       │    (Basic Auth)           │                    │
   │                       │ ────────────────────────►│                    │
   │                       │                          │ 4. 创建 Thing      │
   │                       │                          │    + da_bridge_    │
   │                       │                          │    mappings 写入   │
   │                       │◄──────────────────────── │                    │
   │                       │  (thing ID)               │                    │
   │                       │                          │                    │
   │                       │ 5. 保存本地 SQLite 映射   │                    │
   │                       │                          │                    │


时序2: DA 设备上报 telemetry (经 EMQX Rule 纯 MQTT 转换后进入 fleets 主题域)
═══════════════════════════════════════════════════════════════════════

DA Device                        EMQX (Rule #1)                           Fleets 自身 EMQX 规则         Fleets Core
   │                               │                                           │                        │
   │ 1. Publish                    │                                           │                        │
   │    v1/p-dual-temp-sensor/     │                                           │                        │
   │    device-001/telemetry       │                                           │                        │
   │ ────────────────────────────► │                                           │                        │
   │                               │                                           │                        │
   │                               │ 2. DA-bridge Rule 命中:                   │                        │
   │                               │    FROM "v1/+/+/telemetry"               │                        │
   │                               │                                           │                        │
   │                               │ 3. 转换:                                  │                        │
   │                               │    topic:   v1/.../telemetry              │                        │
   │                               │          → $emqx/things/da:.../          │                        │
   │                               │            shadow/update                 │                        │
   │                               │    payload: {type:"status",data:{...}}    │                        │
   │                               │          → {reported:{temp_a:23.5,...}}  │                        │
   │                               │                                           │                        │
   │                               │ 4. MQTT republish                        │                        │
   │                               │    (不调用 HTTP API)                     │                        │
   │                               │                                           │                        │
   │                               │── republish ────────────────────────────►│                        │
   │                               │   $emqx/things/da:.../shadow/update     │                        │
   │                               │                                           │                        │
   │                               │                                           │ 5. 命中 fleets 自身    │
   │                               │                                           │    规则: FROM "$emqx/   │
   │                               │                                           │    things/+/shadow/    │
   │                               │                                           │    update"             │
   │                               │                                           │    动作: POST Thing-   │
   │                               │                                           │    Datas API           │
   │                               │                                           │───────────────►       │
   │                               │                                           │                        │
   │                               │                                           │ 6. 查 Thing 是否存在   │
   │                               │                                           │    (利用现有机制)       │
   │                               │                                           │    → 存在 → 更新 Shadow│
   │                               │                                           │    → 不存在 → 拒绝     │


时序3: Fleets 下发命令给 DA 设备
═══════════════════════════════════════════════════

Fleets Admin         Fleets Thing Service          Fleets EMQX Publisher          EMQX              DA Device
   │                        │                             │                       │                  │
   │ 1. POST /api/v1/       │                             │                       │                  │
   │    commands            │                             │                       │                  │
   │ ───────────────────►   │                             │                       │                  │
   │                        │                             │                       │                  │
   │                        │ 2. 查映射表 → 是 DA 桥接设备 │                       │                  │
   │                        │───────────────────────────►│                       │                  │
   │                        │                             │                       │                  │
   │                        │                             │ 3. 复用 fleets 现有   │                  │
   │                        │                             │    emqx.Publisher     │                  │
   │                        │                             │    Publish 到 DA 主题  │                  │
   │                        │                             │    device-agent/{pid}/│                  │
   │                        │                             │    device/{did}/cmd   │                  │
   │                        │                             │ ────────────────────►│─────────────────►│
   │                        │                             │                       │                  │
   │                        │                             │                       │◄─────────────────│
   │                        │                             │ 5. DA 设备 Publish    │  4. Publish      │
   │                        │                             │    响应到 EMQX        │  response        │
   │                        │                             │◄────────────────────  │                  │
   │                        │                             │                       │                  │
   │                        │                             │ 6. Rule Engine 命中:  │                  │
   │                        │                             │    device-agent/.../  │                  │
   │                        │                             │    responses          │                  │
   │                        │                             │    → POST 到 Command  │                  │
   │                        │                             │       Response API    │                  │
   │                        │                             │                       │                  │
   │                        │                             │ 7. 回写 Job Execution  │                  │
   │◄───────────────────────│                             │                       │                  │
```

### 2.3 组件依赖关系

```
Fleets 实例
  ├── Go 标准库 + gin/huma                 ← 无变化
  ├── PostgreSQL 连接池                      ← 无变化
  ├── EMQX REST API Client                   ← 无变化，复用
  ├── [device-agent-bridge plugin]          ← 新增，纯 EMQX Rule 管理模式
  │     ├── EMQX Rule Manager (增/删/查规则)
  │     ├── 内置规则模板 (Telemetry/Event/Command)
  │     ├── da_bridge_mappings 表 (PostgreSQL)
  │     └── Rule 健康状态检查
  └── 其他 Fleets 服务                        ← 无变化

Device-Agent 实例
  ├── TypeScript / Bun runtime               ← 无变化
  ├── MQTT v5 Client                         ← 无变化
  ├── DeviceService / ProductService          ← 无变化
  ├── [fleets-bridge plugin]                 ← 新增 Channel
  │     ├── Fleets REST API Client (Basic Auth)
  │     ├── 设备注册协调器
  │     ├── 状态/影子同步器
  │     ├── 本地 SQLite 映射存储 + 同步队列
  │     └── 命令监听器
  └── Agent Gateway                          ← 无变化
```

### 2.4 Fleets Plugin 的独立定位

```
┌─────────────────────────────────────────────────────────────────────┐
│  device-agent-bridge Plugin 的独立定位:                               │
│                                                                     │
│  它不修改 fleets 核心代码                                            │
│  它不内嵌 MQTT 客户端                                                │
│  它不处理任何业务逻辑                                                │
│                                                                     │
│  它的全部工作 = 通过 EMQX REST API v5:                                │
│    1. 创建规则 (POST /api/v5/rules) — 含 SQL + republish 动作        │
│    2. 删除规则 (DELETE /api/v5/rules/{id})                           │
│    3. 查看规则 (GET /api/v5/rules)                                   │
│    4. 启用/禁用规则                                                  │
│                                                                     │
│  它复用的资源:                                                       │
│    - fleets 已有的 EMQX_API_KEY / EMQX_API_SECRET                   │
│    - fleets 的 PostgreSQL (仅 da_bridge_mappings 表)
└─────────────────────────────────────────────────────────────────────┘
```

---

## 3. 设备模型差异分析与映射

（与 v1.1 保持一致，无变化）

### 3.1 核心数据模型对比

| 维度 | Device-Agent | Fleets | 映射策略 |
|------|-------------|--------|---------|
| 设备类型 | **Product** | **ThingType** | DA Plugin 转化 |
| 设备实例 | **Device** | **Thing** | DA Plugin 注册 |
| 影子模型 | 两态 {reported, desired} | 三态 {reported, desired, delta} | 兼容格式 |
| MQTT 主题 | `v1/{pid}/{did}/...` | `$emqx/things/{name}/...` | EMQX Rule 转换 |

### 3.2 类型映射细则

**DA Product → Fleets ThingType**: 由 DA Plugin 在注册时转换并调用 Fleets API 创建。

**DA Device → Fleets Thing**: 由 DA Plugin 调用 `POST /api/v1/things` 创建，嵌入 `da-product-id` 和 `da-device-id` tags。

### 3.3 命名规范

| 场景 | 格式 | 示例 |
|------|------|------|
| DA 设备桥接 | `da:{productId}/{deviceId}` | `da:p-dual-temp-sensor/device-001` |
| ThingType | `da:product/{productId}` | `da:product/p-dual-temp-sensor` |

### 3.4 状态映射矩阵

| DA 状态 | Fleets 状态 | 谁触发 |
|---------|------------|--------|
| `online` | `online` | DA Plugin (API) 或 EMQX Rule (转发 shadow) |
| `offline` | `offline` | DA Plugin (API) 或 EMQX Will 消息 |
| `error` | `offline` (带 metadata) | DA Plugin (API) |

---

## 4. EMQX Rule Engine 规则设计

### 4.1 核心原理

device-agent-bridge 插件注入的 EMQX 规则，职责是 **纯 MQTT topic/payload 转换**：

```
DA 设备 MQTT 消息
  v1/{productId}/{deviceId}/telemetry
  携带 DA 格式 payload
          │
          ▼
  ┌──────────────────────────────────────┐
  │  EMQX Rule #1 (注入的规则)           │
  │                                      │
  │  1. 接收 v1/{pid}/{did}/telemetry    │
  │  2. 解析 DA payload                  │
  │  3. 构造 fleets 格式 payload         │
  │  4. 重新发布到 fleets 主题            │
  │     $emqx/things/da:{pid}/{did}/     │
  │     shadow/update                    │
  └──────────────────────────────────────┘
          │
          ▼
  Fleets 主题域消息
  $emqx/things/da:{productId}/{deviceId}/shadow/update
  携带 Fleets 格式 payload
          │
          ▼
  Fleets 自身预配的 EMQX 规则（与原生设备一致的路径）
  → $emqx/things/{name}/shadow/update
  → EMQX Rule → POST ThingDatas API → 更新 Shadow
```

**关键特点**：
- 桥接规则只做 DA→Fleets 的 **topic 重写 + payload 重塑**，规则动作是 `republish`（重新发布到 MQTT 主题）
- 不调用任何 HTTP API，不耦合 Fleets 部署地址
- 转换后的消息进入 `$emqx/things/{name}/...` 主题，**Fleets 自身预置的 EMQX 规则按原生设备流程处理**
- Thing 不存在的消息自然被 Fleets 端拒绝（利用现有机制，无需额外逻辑）

### 4.2 三套规则总览

| 规则 | 源 Topic | 目标 Topic | 说明 |
|------|---------|-----------|------|
| #1 Telemetry | `v1/{pid}/{did}/telemetry` | `$emqx/things/da:{pid}/{did}/shadow/update` | DA 属性上报 → Fleets 影子更新 |
| #2 Event | `v1/{pid}/{did}/event` | `$emqx/things/da:{pid}/{did}/events/update` | DA 事件上报 → Fleets 事件 |
| #3 Command Response | `device-agent/{pid}/device/{did}/responses` | `$emqx/things/da:{pid}/{did}/command/response` | DA 命令响应 → Fleets 命令回执 |

### 4.3 Rule #1: Telemetry 转换规则

#### 4.3.1 DA 原始格式 → Fleets 目标格式

```json
// DA telemetry 原始 payload (MQTT v1/{pid}/{did}/telemetry)
{
  "type": "status",
  "data": {
    "temp_a": 23.5,
    "temp_b": 24.1,
    "status": "online"
  },
  "ts": 1750000000000
}

// ──→ 转换为 Fleets shadow/update 格式 ──→

// target topic: $emqx/things/da:{productId}/{deviceId}/shadow/update
// target payload:
{
  "reported": {
    "temp_a": 23.5,
    "temp_b": 24.1
  }
}
```

#### 4.3.2 EMQX Rule SQL + Republish Action

```sql
-- EMQX Rule SQL: DA telemetry → Fleets shadow/update
SELECT
  -- 构建目标 topic: $emqx/things/da:{pid}/{did}/shadow/update
  concat('$emqx/things/da:',
    (regex_match(topic, '^v1/([^/]+)/([^/]+)/telemetry$'))[1], '/',
    (regex_match(topic, '^v1/([^/]+)/([^/]+)/telemetry$'))[2],
    '/shadow/update'
  ) as target_topic,

  -- 构建目标 payload: 提取 DA payload 中的 data 字段
  -- DA payload = {"type":"status","data":{...},"ts":...}
  -- → {"reported": {"temp_a":23.5,...}}
  json_encode(map(
    'reported', json_decode(payload)->>'data'
  )) as target_payload

FROM
  "v1/+/+/telemetry"
```

**规则动作 — MQTT Republish**（不是 HTTP POST）：

```json
{
  "function": "republish",
  "args": {
    "topic":   "${target_topic}",
    "payload": "${target_payload}",
    "qos":     1,
    "retain":  false
  }
}
```

### 4.4 Rule #2: Event 转换规则

```json
// DA event 原始 payload
{
  "type": "event",
  "data": {
    "eventName": "overheat",
    "outputData": {
      "zone": "A",
      "temperature": 85.3
    }
  },
  "ts": 1750000000000
}

// ──→ target topic:
// $emqx/things/da:{productId}/{deviceId}/events/update

// ──→ target payload:
{
  "eventName": "overheat",
  "severity": "alert",
  "outputData": {
    "zone": "A",
    "temperature": 85.3
  }
}
```

```sql
SELECT
  concat('$emqx/things/da:',
    (regex_match(topic, '^v1/([^/]+)/([^/]+)/event$'))[1], '/',
    (regex_match(topic, '^v1/([^/]+)/([^/]+)/event$'))[2],
    '/events/update'
  ) as target_topic,

  -- 提取 event data 并添加 severity 默认值
  json_encode(map(
    'eventName',  json_decode(payload)->'data'->>'eventName',
    'severity',   'alert',
    'outputData', json_decode(payload)->'data'->>'outputData'
  )) as target_payload

FROM
  "v1/+/+/event"
```

**动作 — MQTT Republish**（同上）：

```json
{
  "function": "republish",
  "args": {
    "topic":   "${target_topic}",
    "payload": "${target_payload}",
    "qos":     1,
    "retain":  false
  }
}
```

### 4.5 Rule #3: Command Response 转换规则

```json
// DA command response 原始 payload
{
  "code": 0,
  "msg": "ok",
  "requestId": "req-abc-123",
  "data": { "interval": 30 }
}

// ──→ target topic:
// $emqx/things/da:{productId}/{deviceId}/command/response

// ──→ target payload:
{
  "requestId": "req-abc-123",
  "status": "succeeded",
  "output": { "interval": 30 }
}
```

```sql
SELECT
  concat('$emqx/things/da:',
    (regex_match(topic, '^device-agent/([^/]+)/device/([^/]+)/responses$'))[1], '/',
    (regex_match(topic, '^device-agent/([^/]+)/device/([^/]+)/responses$'))[2],
    '/command/response'
  ) as target_topic,

  json_encode(map(
    'requestId', json_decode(payload)->>'requestId',
    'status',    iif(json_decode(payload)->>'code' = '0', 'succeeded', 'failed'),
    'output',    json_decode(payload)->>'data'
  )) as target_payload

FROM
  "device-agent/+/device/+/responses"
```

**动作 — MQTT Republish**：

```json
{
  "function": "republish",
  "args": {
    "topic":   "${target_topic}",
    "payload": "${target_payload}",
    "qos":     1,
    "retain":  false
  }
}
```

### 4.6 EMQX Rule 生命周期管理

```
插件启动 Start()
  │
  ├─ 1. 检查 EMQX API 连通性 (GET /api/v5/rules)
  ├─ 2. 按规则 ID 前缀查重（避免重复创建）
  ├─ 3. 创建 Rule #1 (Telemetry: DA → Fleets shadow/update)
  ├─ 4. 创建 Rule #2 (Event: DA → Fleets events/update)
  ├─ 5. 创建 Rule #3 (Command Response: DA → Fleets command/response)
  └─ 6. 启动规则健康检查协程

插件停止 Stop()
  │
  ├─ 1. 删除 Rule #1
  ├─ 2. 删除 Rule #2
  ├─ 3. 删除 Rule #3
  └─ 4. 关闭健康检查协程
```

### 4.7 消息流转全景

```
   DA 设备                               EMQX Broker
     │                                      │
     │ Publish                              │
     │ v1/p-dual-temp-sensor/               │
     │ device-001/telemetry                 │
     │ payload:{"type":"status","data":{...}}│
     │──────────────────────────────────────►│
     │                                      │
     │                                      │ device-agent-bridge Rule #1
     │                                      │ SQL: FROM "v1/+/+/telemetry"
     │                                      │ 动作: MQTT republish
     │                                      │ topic: $emqx/things/da:p-dual-temp-sensor/
     │                                      │        device-001/shadow/update
     │                                      │ payload: {"reported":{"temp_a":23.5,...}}
     │                                      │
     │                                      │ (消息已进入 Fleets 主题域)
     │                                      │
     │                                      │ Fleets 自身预配规则
     │                                      │ SQL: FROM "$emqx/things/+/shadow/update"
     │                                      │ 动作: POST ThingDatas API
     │                                      │       → 查 Thing 是否存在
     │                                      │       → 存在 → 更新 Shadow
     │                                      │       → 不存在 → 拒绝
     │                                      │
```

**重点**：Fleets Plugin 的规则只负责将 DA 格式转为 Fleets 格式并重发布到 `$emqx/things/{name}/...` 主题。消息进入这个主题后，由 Fleets 自身预设的 EMQX 规则（`setup-emqx-rules` 脚本配置的）负责后续处理——这和原生设备完全一致。

---

## 5. Fleets 端插件（device-agent-bridge）设计

### 5.1 概述

**包名**: `github.com/emqx/fleets/internal/plugins/deviceagent`

Fleets Plugin 的定位是一个**轻量级的 EMQX 规则配置器**，不是 MQTT 消费者。

#### 5.1.1 插件核心职责

| 职责 | 说明 |
|------|------|
| EMQX Rule 生命周期管理 | 启用时注入 #1-#3 规则到 EMQX Broker，停用时清除 |
| 规则模板维护 | 内置三套规则的 SQL + 动作模板，版本可管理 |
| 映射表维护 | da_bridge_mappings 的 DB 读写 + 缓存（供 Thing Service 使用） |
| 规则健康监控 | 定期检查规则状态、连接健康度 |

#### 5.1.2 插件不负责的事

| 不负责 | 原因 |
|--------|------|
| 订阅 MQTT topic | Fleets 架构上不订阅 MQTT，通过 REST API 通信 |
| 设备发现/自动注册 | 已明确由 DA Plugin 唯一发起 |
| 协议转换 | DA→Fleets 的 topic/payload 转换在 EMQX Rule SQL 中完成 |
| 命令下发 MQTT | 复用 fleets 现有的 `emqx.Publisher` 接口 |

### 5.2 核心接口

```go
// plugins/bridge.go - 桥接插件接口
package plugins

// BridgePlugin 所有桥接插件必须实现的接口
type BridgePlugin interface {
    Name() string
    Init(deps BridgeDependencies) error
    Start(ctx context.Context) error
    Stop(ctx context.Context) error
    IsEnabled() bool
    // Status 返回插件运行状态，包括 EMQX 规则状态
    Status(ctx context.Context) (*PluginStatus, error)
}

// BridgeDependencies 插件依赖注入
type BridgeDependencies struct {
    // EMQX REST API 客户端 — 复用 fleets 现有凭据
    EmqxClient   *emqx.Client
    // DB 仅用于 da_bridge_mappings 表
    DB           *pgxpool.Pool
    // Thing Service — 用于创建 Thing 时写入映射 + 命令下发桥接
    ThingSvc     service.ThingService
    Logger       *slog.Logger
    Config       *BridgePluginConfig
}

// BridgePluginConfig 插件配置
type BridgePluginConfig struct {
    Enabled bool `json:"enabled"`
    // EMQX 规则配置
    Rules struct {
        // 规则 ID 前缀，用于识别哪些规则属于本插件
        IDPrefix string `json:"idPrefix"`   // 默认 "da-bridge-"
        // 是否启用每条规则
        TelemetryRuleEnabled  bool `json:"telemetryRuleEnabled"`  // 默认 true
        EventRuleEnabled      bool `json:"eventRuleEnabled"`      // 默认 true
        CommandResponseEnabled bool `json:"commandResponseEnabled"` // 默认 true
    } `json:"rules"`
    // 映射命名配置
    Mapping struct {
        ThingNamePrefix string `json:"thingNamePrefix"` // 默认 "da:"
        TypeNamePrefix  string `json:"typeNamePrefix"`  // 默认 "da:product/"
    } `json:"mapping"`
}

// PluginStatus 插件状态
type PluginStatus struct {
    Name       string         `json:"name"`
    Enabled    bool           `json:"enabled"`
    Running    bool           `json:"running"`
    Rules      []RuleStatus   `json:"rules"`
    MappingsCount int64       `json:"mappingsCount"`
}

type RuleStatus struct {
    ID        string `json:"id"`
    Name      string `json:"name"`
    Enabled   bool   `json:"enabled"`
    Status    string `json:"status"` // "running", "error", "disabled"
    Topic     string `json:"topic"`
}
```

### 5.3 插件包结构

```
internal/plugins/deviceagent/
├── plugin.go                # 插件入口，实现 BridgePlugin 接口
├── config.go                # 配置读取 + 验证
├── emqx_rule/
│   ├── rule_manager.go      # EMQX Rule CRUD 封装
│   ├── rule_templates.go    # 三套规则的 SQL+Republish Action 模板
│   └── rule_monitor.go      # 规则健康状态检查
├── store/
│   ├── mapping_store.go     # da_bridge_mappings 表 CRUD + 缓存
│   └── migration.go         # DDL 迁移
└── model_mapper.go          # DA ↔ Fleets 模型映射工具函数
                              (供 Thing Service 命令下发时使用)
```

**与旧版相比**：
- 移除了 `mqtt/` 目录（不再订阅 MQTT）
- 移除了 `validator/`、`sync/`、`bridge/` 目录
- 移除了 `handler/` 目录（不再需要 ThingDatas Handler 扩展 — 规则只做 MQTT republish）
- 移除了 `converter/` 目录（被 `model_mapper.go` 取代，仅保留模型映射工具函数）
- 新增 `emqx_rule/` 目录（EMQX 规则生命周期管理）

### 5.4 EMQX Rule 管理器

```go
// emqx_rule/rule_manager.go

type RuleManager struct {
    emqxClient *emqx.Client
    templates  *RuleTemplates
    logger     *slog.Logger
    config     *RuleConfig
}

// SyncRules 同步规则状态: 确保期望的规则存在且运行
func (m *RuleManager) SyncRules(ctx context.Context) error {
    // 1. 获取当前 EMQX 中所有规则
    existingRules, err := m.emqxClient.ListRules(ctx)
    if err != nil {
        return fmt.Errorf("list rules: %w", err)
    }

    // 2. 过滤出本插件管理的规则（按 ID 前缀）
    managedRules := m.filterManagedRules(existingRules)

    // 3. 对比期望规则列表，创建缺失的规则
    for _, expected := range m.templates.All() {
        if !m.ruleExists(managedRules, expected.ID) {
            if err := m.createRule(ctx, expected); err != nil {
                m.logger.Error("create rule failed", "id", expected.ID, "error", err)
            }
        }
    }

    return nil
}

// createRule 通过 EMQX REST API 创建一条规则
func (m *RuleManager) createRule(ctx context.Context, rule *RuleDefinition) error {
    // EMQX V5 Rule API: POST /api/v5/rules
    ruleReq := map[string]interface{}{
        "id":         rule.ID,
        "name":       rule.Name,
        "sql":        rule.SQL,
        "actions":    rule.Actions,  // 动作列表
        "enable":     true,
        "description": rule.Description,
    }

    resp, err := m.emqxClient.Request(ctx, "POST", "/api/v5/rules", ruleReq)
    if err != nil {
        return err
    }
    m.logger.Info("emqx rule created", "id", rule.ID, "name", rule.Name)
    return nil
}

// RemoveRules 删除所有本插件管理的规则
func (m *RuleManager) RemoveRules(ctx context.Context) error {
    existingRules, err := m.emqxClient.ListRules(ctx)
    if err != nil {
        return err
    }

    for _, r := range existingRules {
        if strings.HasPrefix(r.ID, m.config.IDPrefix) {
            if err := m.emqxClient.Request(ctx, "DELETE", fmt.Sprintf("/api/v5/rules/%s", r.ID), nil); err != nil {
                m.logger.Error("delete rule failed", "id", r.ID, "error", err)
            }
        }
    }
    return nil
}
```

### 5.5 规则模板定义

```go
// emqx_rule/rule_templates.go

type RuleDefinition struct {
    ID          string          `json:"id"`
    Name        string          `json:"name"`
    SQL         string          `json:"sql"`
    Actions     []RuleAction    `json:"actions"`
    Description string          `json:"description"`
}

type RuleAction struct {
    Function string      `json:"function"` // "data_to_webserver" 等
    Args     interface{} `json:"args"`
}

// All 返回插件管理的所有规则模板
func (t *RuleTemplates) All() []*RuleDefinition {
    return []*RuleDefinition{
        t.TelemetryRule(),
        t.EventRule(),
        t.CommandResponseRule(),
    }
}

// TelemetryRule DA telemetry → Fleets Shadow (MQTT republish)
func (t *RuleTemplates) TelemetryRule() *RuleDefinition {
    ruleID := t.config.IDPrefix + "telemetry"
    return &RuleDefinition{
        ID:   ruleID,
        Name: "DA Telemetry to Fleets Shadow",
        SQL: fmt.Sprintf(`SELECT
  concat('$emqx/things/da:',
    (regex_match(topic, '^v1/([^/]+)/([^/]+)/telemetry$'))[1], '/',
    (regex_match(topic, '^v1/([^/]+)/([^/]+)/telemetry$'))[2],
    '/shadow/update'
  ) as target_topic,
  json_encode(map(
    'reported', json_decode(payload)->>'data'
  )) as target_payload
FROM "v1/+/+/telemetry"`),
        Actions: []RuleAction{{
            Function: "republish",
            Args: map[string]interface{}{
                "topic":   "${target_topic}",
                "payload": "${target_payload}",
                "qos":     1,
                "retain":  false,
            },
        }},
        Description: "Convert DA telemetry to Fleets shadow/update via MQTT republish",
    }
}

// EventRule DA event → Fleets Event
func (t *RuleTemplates) EventRule() *RuleDefinition {
    /* SQL + republish action, 参见 4.4 节 */
}

// CommandResponseRule DA command response → Fleets Job Execution
func (t *RuleTemplates) CommandResponseRule() *RuleDefinition {
    /* SQL + republish action, 参见 4.5 节 */
}
```

### 5.6 插件生命周期

```
┌──────────┐   Init()    ┌──────────┐   Start()     ┌───────────┐
│ Disabled │ ──────────► │  Ready   │ ────────────►  │  Running  │
└──────────┘             └──────────┘                └───────────┘
                                                           │
                                                      Stop()
                                                           │
                                                           ▼
                                                      ┌──────────┐
                                                      │ Stopped   │
                                                      └──────────┘
```

**Start() 实现**：

```go
func (p *DeviceAgentBridgePlugin) Start(ctx context.Context) error {
    p.logger.Info("starting device-agent-bridge plugin")

    // Step 1: 运行数据库迁移（建 da_bridge_mappings 表）
    if err := p.runMigrations(ctx); err != nil {
        return fmt.Errorf("migration: %w", err)
    }

    // Step 2: 验证 EMQX API 连通性
    if err := p.emqxClient.Ping(ctx); err != nil {
        return fmt.Errorf("emqx unreachable: %w", err)
    }

    // Step 3: 检查 EMQX 中本插件的规则是否已存在
    // 如果重启，需识别已有规则避免重复创建
    // 约定: 规则 ID 前缀统一为 da-bridge-

    // Step 4: 同步规则 — 创建缺失的规则
    if err := p.ruleManager.SyncRules(ctx); err != nil {
        return fmt.Errorf("sync emqx rules: %w", err)
    }

    // Step 5: 启动规则健康检查协程
    go p.ruleMonitor.Start(ctx)

    p.logger.Info("device-agent-bridge plugin started")
    return nil
}

// Stop 清理: 删除 EMQX 规则 + 关闭监控协程
func (p *DeviceAgentBridgePlugin) Stop(ctx context.Context) error {
    p.logger.Info("stopping device-agent-bridge plugin")

    if err := p.ruleManager.RemoveRules(ctx); err != nil {
        p.logger.Error("remove rules error", "error", err)
    }

    p.ruleMonitor.Stop()
    return nil
}
```

### 5.7 da_bridge_mappings 表

```sql
-- migrations/device_agent_bridge.up.sql
CREATE TABLE da_bridge_mappings (
    id              BIGSERIAL PRIMARY KEY,
    da_product_id   TEXT NOT NULL,
    da_device_id    TEXT NOT NULL,
    thing_type_id   TEXT NOT NULL,
    thing_id        TEXT NOT NULL UNIQUE,
    thing_name      TEXT NOT NULL UNIQUE,
    da_metadata     JSONB,
    last_synced_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(da_product_id, da_device_id)
);

-- 写入触发: Thing Service 创建 DA 桥接设备时
-- 读取触发: ThingDatas Handler 收到 da-bridge 请求时, 查映射确认
```

### 5.8 Thing Service 的映射写入拦截 + 命令下发桥接

与 v1.1 一致 — 当 DA Plugin 通过 `POST /api/v1/things` 创建 Thing 时，Thing Service 拦截并自动写入 `da_bridge_mappings`。

```go
// service/thing_svc.go (修改点, 与 v1.1 相同)
func (s *ThingService) CreateThing(ctx context.Context, req *CreateThingRequest) (*Thing, error) {
    thing, err := s.thingRepo.Create(ctx, req)
    if err != nil { return nil, err }

    if isDABridgedDevice(thing) {
        daProductID := extractTag(thing.Tags, "da-product-id")
        daDeviceID := extractTag(thing.Tags, "da-device-id")
        if daProductID != "" && daDeviceID != "" {
            s.mappingStore.Save(ctx, &MappingRecord{
                DAProductID: daProductID,
                DADeviceID:  daDeviceID,
                ThingTypeID: *thing.ThingTypeID,
                ThingID:     thing.ID,
                ThingName:   thing.Name,
            })
        }
    }
    return thing, nil
}

func (s *ThingService) DeleteThing(ctx context.Context, id string) error {
    mapping, _ := s.mappingStore.FindByThingID(ctx, id)
    if mapping != nil { s.mappingStore.Delete(ctx, mapping.ID) }
    return s.thingRepo.Delete(ctx, id)
}
```

### 5.9 命令下发：复用 fleets 现有机制 + 映射判断

命令下发**不改由插件处理**。当 fleets 收到命令请求时，Thing Service 查映射表判断是否桥接设备：

```go
// service/thing_svc.go (命令下发部分)

// SendCommand 下发命令
func (s *ThingService) SendCommand(ctx context.Context, thingID string, cmd *CommandRequest) error {
    // 查映射表
    mapping, err := s.mappingStore.FindByThingID(ctx, thingID)
    if err != nil {
        // 不是桥接设备 → 走原始流程 (发到 $emqx/things/{name}/...)
        return s.sendToFleetsTopic(ctx, thingID, cmd)
    }

    // 是桥接 DA 设备 → 发布到 DA 命令主题
    daCmd := s.converter.ToDACommand(cmd.Name, cmd.Params)
    topic := fmt.Sprintf("device-agent/%s/device/%s/commands",
        mapping.DAProductID, mapping.DADeviceID)
    payload, _ := json.Marshal(daCmd)

    // 复用 fleets 的 EMQX Publisher
    return s.emqx.Publish(ctx, topic, payload, 1, false)
}
```

### 5.10 Fleets 应用程序的修改

```go
// app/app.go — 与 v1.1 类似的插件加载逻辑

func New(cfg *config.Config) *App {
    app := &App{ /* 核心依赖初始化 */ }

    if cfg.DeviceAgentBridge.Enabled {
        plugin := deviceagent.NewPlugin(
            deviceagent.WithConfig(cfg.DeviceAgentBridge),
            deviceagent.WithDependencies(BridgeDependencies{
                EmqxClient:   app.emqxClient,  // 复用已存在的 EMQX Client
                DB:           app.pgPool,
                ThingSvc:     app.thingSvc,
                Logger:       app.logger.With("plugin", "device-agent-bridge"),
            }),
        )
        app.plugins = append(app.plugins, plugin)
    }
    return app
}

func (app *App) Start(ctx context.Context) error {
    for _, p := range app.plugins {
        if err := p.Start(ctx); err != nil {
            return fmt.Errorf("start plugin %s: %w", p.Name(), err)
        }
    }
    return nil
}
```

---

## 6. Device-Agent 端插件（fleets-bridge）设计

（与 v1.1 一致，无重大变化）

**包位置**: `apps/agent-gateway/src/plugins/fleets-bridge/`

核心逻辑保持不变：
- 作为设备注册的唯一发起方
- 监听 DeviceService 事件，主动调用 Fleets API
- 创建 ThingType 和 Thing
- 同步设备状态和影子
- 维护本地 SQLite 映射和同步队列

详见 [第 6 节在 v1.1 文档中的完整内容](#6-device-agent-端插件fleets-bridge设计)，此处仅列出配置变更：

### 6.1 配置调整

新增 `onTelemetry` 配置项，控制是否通过 API 同步 shadow 还是依赖 EMQX Rule：

```typescript
sync: z.object({
  onDeviceRegister: z.boolean().default(true),     // 设备注册 → API 创建 Thing
  onDeviceStatusChange: z.boolean().default(true), // 状态变化 → API 更新
  onTelemetry: z.boolean().default(false),          // 建议 false
  // → true: DA Plugin 每个 telemetry 都调用 Fleets API (高延迟)
  // → false: 依赖 EMQX Rule 转发到 ThingDatas API (更低延迟)
  syncIntervalMs: z.number().default(60000),
}),
```

---

## 7. 认证与安全

### 7.1 认证架构

```
Device-Agent (fleets-bridge)
  │
  │ 1. Basic Auth (apiKey:apiSecret) → Fleets REST API
  │    - 注册设备、同步状态、查询
  │
  ▼
Fleets 实例
  │
  │ 2. EMQX_API_KEY / EMQX_API_SECRET → EMQX REST API
  │    - device-agent-bridge 插件复用此凭据注入规则
  │
  ▼
EMQX Broker
  │
  │ 3. MQTT 认证 → EMQX Broker Auth Callback → Fleets broker-auth API
  │    - DA 设备通过 DA SDK 自带的 MQTT 认证接入
```

### 7.2 凭据复用策略

| 凭据 | 用途 | 来源 |
|------|------|------|
| `Fleets API Key/Secret` | DA Plugin 调用 Fleets REST API | Fleets 管理员创建，配给 DA |
| `EMQX_API_KEY/EMQX_API_SECRET` | Fleets Plugin 调用 EMQX Rule API | 复用 fleets 已有配置 |
| DA 设备 MQTT 凭据 | DA 设备接入 EMQX | DA SDK 自带，Fleets Plugin 不过问 |

### 7.3 DA Plugin 的最小权限

（与 v1.1 一致）

```json
{
  "name": "device-agent-bridge",
  "permissions": {
    "GET": ["/api/v1/things", "/api/v1/thing-types"],
    "POST": ["/api/v1/things", "/api/v1/thing-types", "/api/v1/thing-datas"],
    "PUT": ["/api/v1/things", "/api/v1/thing-types"],
    "PATCH": ["/api/v1/things"]
  }
}
```

---

## 8. Fleets API 扩展规范

本节内容无需变更。因为：

- **EMQX Rule 只做 MQTT republish**，不调用 Fleets API，所以 ThingDatas API 不需要扩展
- **桥接设备标识**：Fleets 现有 `GET /api/v1/things?tags=source=device-agent` 可过滤，无需新增端点
- **命令执行状态回调**：DA 命令响应经 EMQX Rule #3 转换后发布到 `$emqx/things/da:{pid}/{did}/command/response`，由 Fleets 自身预设规则转发到 ThingDatas command/response API 处理，无需新增端点

---

## 9. 配置与部署

### 9.1 Fleets 端配置

```bash
# Fleets Plugin: device-agent-bridge (纯 EMQX Rule 管理模式)
DA_BRIDGE_ENABLED=true

# 规则配置 (复用 EMQX_API_KEY / EMQX_API_SECRET)
DA_BRIDGE_RULES_ID_PREFIX=da-bridge-
DA_BRIDGE_RULES_TELEMETRY_ENABLED=true
DA_BRIDGE_RULES_EVENT_ENABLED=true
DA_BRIDGE_RULES_COMMAND_RESPONSE_ENABLED=true

# 映射命名配置 (与 DA 端一致)
DA_BRIDGE_MAPPING_THING_NAME_PREFIX=da:
DA_BRIDGE_MAPPING_TYPE_NAME_PREFIX=da:product/
```

**Fleets 端不需要 MQTT 连接配置** — 插件不直接连接 EMQX，仅通过 REST API 管理规则。

### 9.2 Device-Agent 端配置

（与 v1.1 一致）

### 9.3 启动顺序

```
1. PostgreSQL / GreptimeDB 启动
2. EMQX Broker 启动
3. Fleets 启动（含 device-agent-bridge 插件）
   ├── 运行数据库迁移（da_bridge_mappings 表）
   ├── 启动 HTTP 服务
   └── 通过 EMQX API 注入 3 条规则
4. Device-Agent 启动（含 fleets-bridge 插件）
   ├── 读取配置，初始化 Fleets API Client
   ├── 启动 DeviceService 事件监听
   └── 首次全量同步：遍历已有 DA 设备逐一注册到 fleets
```

### 9.4 EMQX 规则持久性

EMQX 规则持久化在 EMQX Broker 自身的数据库中。即使 Fleets 重启，已注入的 MQTT republish 规则会继续运行（规则不依赖 fleets HTTP 地址）。Fleets Plugin Start() 中先按规则 ID 前缀检查去重，避免重复创建。

---

## 10. 实施路线图

### Phase 1: 基础桥接（2-3 周）

| 任务 | 模块 | 预估 |
|------|------|------|
| 1.1 Fleets Plugin 框架搭建 | `internal/plugins/` + `app.go` | 2d |
| 1.2 EMQX Rule Manager | `emqx_rule/rule_manager.go` | 2d |
| 1.3 规则模板定义 (Telemetry/Event/Command) | `emqx_rule/rule_templates.go` | 2d |
| 1.4 映射表 + migration | `store/mapping_store.go` | 1d |
| 1.5 Thing Service 映射写入拦截 | 改造 `service/thing_svc.go` | 1d |
| 1.6 ThingDatas Handler 桥接扩展 | `handler/thing_datas_delegate.go` | 2d |
| 1.7 DA Plugin 框架搭建 + API Client | DA 端 `fleets-bridge/` | 2d |
| 1.8 DA Plugin 设备注册器 | `sync/device-registrar.ts` | 3d |
| 1.9 端到端测试 | 集成测试 | 2d |

**交付物:** DA Plugin 注册设备到 fleets → Fleets Plugin 注入的 EMQX Rule 自动将 telemetry 转发到 ThingDatas API → 更新 Shadow。

### Phase 2: 命令与事件桥接（2 周）

| 任务 | 模块 | 预估 |
|------|------|------|
| 2.1 Command Response Rule + Handler | `rule_templates.go` + `handler/` | 2d |
| 2.2 Event Rule + Handler | 同上 | 1d |
| 2.3 命令下发 DA 桥接（复用 emqx.Publisher） | `service/thing_svc.go` | 2d |
| 2.4 DA Plugin 命令监听 | `listener/command-events.ts` | 2d |
| 2.5 命令超时与重试 | 两端 | 2d |
| 2.6 端到端测试 | 集成测试 | 2d |

### Phase 3: 健壮性 & Phase 4: 生产化

（与 v1.1 一致）

---

## 11. 附录

### A. 异常场景处理

| 场景 | 处理方式 |
|------|---------|
| **DA 设备未注册** | EMQX Rule 进行 topic/payload 转换并 republish → Fleets 接收后查不到 Thing → 按现有机制拒绝（不会自动创建） |
| **EMQX 重启** | 已注入的规则丢失 → Fleets Plugin 规则监控检测到缺失 → 自动重新注入 |
| **Fleets 重启** | 规则在 EMQX 中持久化运行（rule 不依赖 fleets HTTP 地址，因为只做 MQTT republish）→ 不丢失 → Plugin Start 时按 ID 前缀检查去重 |
| **Fleets 扩展新实例** | 仅有 1 个实例需要管理规则 → 通过 PostgreSQL advisory lock 协调 |
| **Fleets 不可用** | DA Plugin 操作入同步队列重试；EMQX Rule 的 republish 动作不受影响（规则不依赖 fleets 地址） |
| **规则创建失败** | Plugin 告警，不影响 fleets 核心运行 |

### B. 版本变更记录

| 版本 | 日期 | 变更说明 |
|------|------|---------|
| v1.0 | 2026-06-30 | 初始草案（双端 MQTT 订阅模式） |
| v1.1 | 2026-06-30 | 去除 Fleets Plugin 自动发现，DA Plugin 为唯一注册入口 |
| v2.0 | 2026-06-30 | Fleets Plugin 改为 EMQX Rule 注入模式，不复用 MQTT 订阅 |
| **v2.1** | **2026-06-30** | **EMQX Rule 改为纯 MQTT republish，不做 HTTP API 调用** |

---

> **文档维护者**: 集成团队  
> **版本 v2.1 核心变更**:  
> 1. EMQX Rule 的动作从 `data_to_webserver`（HTTP POST→ThingDatas API）改为 `republish`（纯 MQTT 重新发布）  
> 2. DA→Fleets 的 topic/payload 转换完全在 EMQX Rule SQL 中完成，不再需要 fleets 端的 ThingDatas Handler 扩展  
> 3. 桥接消息进入 `$emqx/things/{name}/...` 主题后，由 Fleets 自身预设的 EMQX 规则统一处理  
> 4. 移除了 `handler/thing_datas_delegate.go`、`converter/` 等不再需要的模块  
> **下一步**: 评审文档 → 启动 Phase 1 开发
