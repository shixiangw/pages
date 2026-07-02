# Device-Agent × Fleets 双端插件集成方案

> **版本:** v2.5  
> **日期:** 2026-07-01  
> **状态:** 评审修订（本期：上行 telemetry/event + 设备注册；命令下行 / Shadow 下行 / Jobs 纳入 §12 远期 TODO）

---

## 目录

1. [核心目标与设计原则](#1-核心目标与设计原则)
2. [架构总览](#2-架构总览)
3. [设备模型差异分析与映射](#3-设备模型差异分析与映射)
4. [EMQX Rule Engine 规则设计](#4-emqx-rule-engine-规则设计)
5. [Fleets 端插件（device-agent-bridge）设计](#5-fleets-端插件-device-agent-bridge-设计)
6. [Device-Agent 端 IoT Platform 插件设计](#6-device-agent-端-iot-platform-插件设计)
7. [认证与安全](#7-认证与安全)
8. [Fleets API 扩展规范](#8-fleets-api-扩展规范)
9. [配置与部署](#9-配置与部署)
10. [实施路线图](#10-实施路线图)
11. [附录](#11-附录)
12. [远期待办（Shadow / 命令下行 / Jobs）](#12-远期待办shadow--命令下行--jobs)
13. [评审待定问题](#13-评审待定问题)

---

## 1. 核心目标与设计原则

### 1.1 核心目标

**让 device-agent 定义和管理的设备也能被 fleets 同时管理**，最终用户可以通过 fleets 统一控制台查看和操作两类设备（fleets 原生设备 + device-agent 设备），而无需关心底层接入协议的差异。

**本期范围（v2.5）**：设备注册、telemetry → Shadow reported、event 上报、lifecycle 在线状态。  
**不在本期**：Fleets 控制台对桥接设备的命令下发、Shadow desired/delta 下行、Jobs 异步任务（见 [§12](#12-远期待办shadow--命令下行--jobs)）。

### 1.2 设计原则

| 原则 | 说明 |
|------|------|
| **可选启用** | 两个插件默认不启用，启用不影响各自系统原生功能 |
| **最小侵入** | 以插件为主扩展；Fleets 核心仅保留 Thing 创建/删除时写映射表挂钩 |
| **DA 主动注册** | device-agent 端是设备注册的唯一发起方，fleets 端不自动发现 DA 设备 |
| **Fleets 不订阅 MQTT** | Fleets Plugin 不内嵌 MQTT 客户端，通过 EMQX Rule API 注入规则 |
| **上行 republish** | 上行 DA→Fleets 在 EMQX Rule 中做 topic/payload 转换，不经 Fleets HTTP |
| **下行远期实现** | 命令下行（Rule #6）、Shadow 下行（Rule #4/#5）、Jobs 协议本期不实现，统一纳入 §12 |
| **复用 EMQX 凭据** | Fleets Plugin 复用 fleets 已有的 `EMQX_API_KEY` / `EMQX_API_SECRET` |
| **松耦合** | 两插件通过 EMQX Broker + Fleets REST API（注册/元数据）协作 |
| **可扩展** | Device-Agent 侧抽象 IoT Platform 插件框架，Fleets 为首个实现 |
| **可观测** | 所有桥接操作均有日志和事件追踪 |

### 1.3 关键概念定义

| 概念 | 说明 |
|------|------|
| **DA 设备** | 通过 device-agent 定义、使用 device-agent SDK 接入 EMQX 的设备 |
| **Fleets 原生设备** | 直接通过 fleets 管理的、遵循 `$emqx/things/{name}/...` 协议的设备 |
| **桥接设备** | DA 设备在 fleets 中的影子表示，以 fleets Thing 形态存在 |
| **EMQX Rule** | EMQX Broker 规则引擎，用于 MQTT 消息的过滤、转换与转发（动作均为 `republish`） |
| **设备类型映射** | DA Product → Fleets ThingType 的转换规则（支持用户手工修订） |
| **桥接 Tag** | 将 DA 侧 key-value 编码为单个 tag 字符串，放入 Fleets `tags` 列表 |
| **IoT Platform 插件** | Device-Agent 侧可插拔的云平台集成模块（Fleets 为首个实现） |
| **BridgePlugin** | Fleets 侧桥接插件统一生命周期接口（见 §5.2） |
| **Fleets mqttClientId** | DA 插件向 Fleets `POST /things` 时**主动填写**的 `mqttClientId` 字段；Fleets 仅用于 lifecycle 匹配，不关心 DA SDK 内部的 clientId 生成策略 |

---

## 2. 架构总览

### 2.1 整体架构图

```
                        ┌──────────────────────────────────────────┐
                        │              EMQX Broker                 │
                        │                                          │
                        │  ┌────────────────────────────────────┐  │
                        │  │  device-agent-bridge 注入的规则     │  │
                        │  │  【本期 — Phase 1/2】                │  │
                        │  │  v1/{pid}/{did}/telemetry (state)  │  │
                        │  │    → $emqx/things/{name}/shadow/   │  │
                        │  │      update (state.reported)        │  │
                        │  │  v1/{pid}/{did}/event              │  │
                        │  │    → $emqx/things/{name}/events/  │  │
                        │  │      {eventType}                    │  │
                        │  │  【远期 — §12 TODO】                 │  │
                        │  │  device-agent/.../responses → cmd  │  │
                        │  │  $emqx/commands/.../request → DA   │  │
                        │  │  shadow desired/delta → commands   │  │
                        │  │  jobs notify → DA                    │  │
                        │  └────────────────────────────────────┘  │
                        │                                          │
                        │  DA 原始主题域     Fleets 主题域          │
                        │  v1/{pid}/{did}    $emqx/things/{name}   │
                        │  device-agent/...  $emqx/commands/...     │
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
│  │  Fleets 核心（最小挂钩）                                         │  │
│  │  ThingService（创建/删除时写映射）                               │  │
│  │  JobService / ShadowService — **不修改**                        │  │
│  │  PostgreSQL: da_bridge_mappings + 既有表                        │  │
│  │  预配 EMQX 规则 ($emqx/things/... → ThingDatas API)           │  │
│  └────────────────────────────────────────────────────────────────┘  │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │  device-agent-bridge Plugin (Go) — 实现 BridgePlugin            │  │
│  │  • EMQX Rule 管理器（本期：#1–#2；#3/#6/#4/#5 远期）          │  │
│  │  • da_bridge_mappings 维护                                    │  │
│  │  • 规则健康监控 + advisory lock 协调多实例                      │  │
│  └────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────┘
                              ▲
                              │ REST API (Basic Auth)
                              │
┌─────────────────────────────┴────────────────────────────────────────┐
│  Device-Agent 实例                                                     │
│  ┌─────────────────────────────────────────────────────────────────┐  │
│  │  IoT Platform 插件框架                                           │  │
│  │  └── fleets（首个实现）                                          │  │
│  │       • 挂在 device-management 注册回调                          │  │
│  │       • ThingType + Thing 注册（含 schema 人工修订）             │  │
│  │       • 注册时主动填写 Fleets Thing.mqttClientId                 │  │
│  │       • 本地映射 + 同步队列                                      │  │
│  │       • 不订阅 MQTT（上行由 EMQX Rule 转换）                       │  │
│  └─────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────┘
```

### 2.2 核心数据流

#### 2.2.1 设备注册

```
DA Device         device-management        fleets IoT 插件           Fleets API
   │                    │                        │                        │
   │ 设备创建            │                        │                        │
   │ metadata:          │                        │                        │
   │  mqttClientId      │                        │                        │
   │───────────────────►│ 注册回调触发            │                        │
   │                    │───────────────────────►│                        │
   │                    │                        │ 用户可选手工修订          │
   │                    │                        │ ThingType schema        │
   │                    │                        │ POST /thing-types       │
   │                    │                        │ POST /things            │
   │                    │                        │   name: da-{pid}--{did} │
   │                    │                        │   mqttClientId: 主动填写 │
   │                    │                        │   tags: [source=...]  │
   │                    │                        │ ──────────────────────►│
   │                    │                        │                        │ ThingService
   │                    │                        │                        │ → da_bridge_mappings
   │                    │                        │◄────────────────────────│
   │                    │                        │ 保存本地映射              │
```

**mqttClientId 来源**：DA fleets 插件在 `POST /api/v1/things` 时**主动填写** `mqttClientId`（注册前置条件，通常取自设备 `metadata.mqttClientId`）。Fleets **不关心** DA SDK 内部的 MQTT `clientId` 生成逻辑，仅将注册 payload 中的值持久化到 Thing，并用于 lifecycle 的 `FindByMqttClientID` 匹配。

**运维约定**：设备连接 EMQX 时使用的 Client ID 须与注册时写入 Fleets Thing 的 `mqttClientId` **一致**（由 DA 侧集成方保证，非 Fleets 职责）。未配置则跳过 Fleets 注册并记录告警。

Fleets lifecycle 规则按 `mqttClientId` 查 Thing，桥接设备在线状态与原生设备一致。

#### 2.2.2 上行：Telemetry

```
DA Device          EMQX da-bridge #1          Fleets 预配规则          Fleets Core
   │                      │                         │                      │
   │ v1/{pid}/{did}/      │ republish               │ POST shadow/reported │
   │   telemetry          │ → $emqx/things/{name}/   │ /pg                  │
   │   (type=state)       │   shadow/update         │─────────────────────►│
   │─────────────────────►│ payload.state.reported  │                      │
```

> DA SDK 同时发送 `type: status`（含 `data.status` / 嵌套 `data.state`）与 `type: state`（扁平属性）。Rule #1 **仅处理 `type: state`**，避免污染 Shadow reported。见 §4.3。

#### 2.2.3 上行：Event（Phase 2）

```
DA Device          EMQX da-bridge #2          Fleets 预配规则          Fleets Core
   │                      │                         │                      │
   │ v1/{pid}/{did}/event │ republish               │ INSERT fleets_events │
   │─────────────────────►│ → $emqx/things/.../     │─────────────────────►│
   │                      │   events/{eventType}    │                      │
```

本期 Rule #2 固定 `severity: "info"`（见 §3.6）。

#### 2.2.4 命令下行（远期，§12）

Fleets 控制台 / `POST /commands` 对桥接设备下发命令 **本期不实现**。完整方案（Rule #6、`ErrNoMatchingSubscribers` 等）见 [§12.2](#122-命令下行远期)。

#### 2.2.5 命令响应上行（远期，§12）

Rule #3（DA `responses` → Fleets command response）与命令下行闭环绑定，纳入 §12，本期不注入。

#### 2.2.6 Shadow 下行（远期，§12）

当前 **Device Agent 无 Shadow 功能**，DA SDK 亦不支持 Shadow desired/delta。Fleets 控制台对桥接设备设置 desired **本期不保证到达设备**；完整方案见 [§12.1](#121-shadow-下行远期)。

#### 2.2.7 本期能力边界（用户可见）

| 能力 | 桥接设备本期 |
|------|-------------|
| 查看 Shadow reported | ✅ |
| 查看 Event 历史 | ✅（Phase 2） |
| 在线 / 离线状态 | ✅ |
| Fleets 控制台下发 sync 命令 | ❌ §12 |
| Fleets 控制台改 desired | ❌ §12 |
| Fleets Jobs 异步任务 | ❌ §12 |

### 2.3 组件依赖关系

```
Fleets 实例
  ├── internal/plugins/bridge.go          ← BridgePlugin 接口（新增包，与 emqx_broker_setup_svc 分工：后者管 Fleets 预配规则，前者管 da-bridge 规则）
  ├── [device-agent-bridge plugin]        ← 首个 BridgePlugin 实现
  │     ├── EMQX Rule Manager（本期 #1–#2；#3/#6/#4/#5 模板预留）
  │     ├── da_bridge_mappings
  │     └── Rule 健康检查 + advisory lock
  ├── ThingService（挂钩：创建/删除时写映射）
  └── JobService / ShadowService — 不改动

Device-Agent 实例
  ├── packages/iot-platforms/             ← IoT Platform 插件框架（新增）
  │     ├── types.ts                      ← 插件接口 + 生命周期钩子
  │     ├── registry.ts                   ← 插件注册与启用（支持多插件链式调用）
  │     └── fleets/                       ← Fleets 首个实现
  │           ├── client.ts
  │           ├── schema-mapper.ts
  │           ├── device-registrar.ts
  │           └── sync-queue.ts
  ├── device-management 注册回调挂钩（失败不阻断设备注册）
  └── 不新增 MQTT 订阅（上行靠 EMQX Rule）
```

### 2.4 Fleets Plugin 定位

插件 **不内嵌 MQTT 客户端**，通过 EMQX REST API v5 管理 da-bridge 规则；上行协议转换在 Rule SQL 中完成。核心业务（Shadow 计算、命令持久化）仍走 Fleets 既有服务 + 预配 EMQX 规则。

---

## 3. 设备模型差异分析与映射

### 3.1 核心数据模型对比

| 维度 | Device-Agent | Fleets | 映射策略 |
|------|-------------|--------|---------|
| 设备类型 | **Product** | **ThingType** | fleets 插件注册前转换 schema（可人工修订） |
| 设备实例 | **Device** | **Thing** | fleets 插件调用 `POST /api/v1/things` |
| 状态上报 | telemetry `type:state` | `{state:{reported}}` | Rule #1 仅转 `type:state` |
| 连接状态 | telemetry `type:status` | Thing `online`/`offline` | **不**写入 reported；走 lifecycle |
| Shadow 下行 | **当前不支持** | desired/delta | **§12 远期 TODO** |
| 命令下行 | DA `commands` topic | `$emqx/commands/.../request` | **§12 远期 TODO** |
| Jobs | **不支持** | `$emqx/things/.../jobs/*` | **§12 远期 TODO** |
| MQTT 主题 | `v1/...`、`device-agent/...` | `$emqx/things/...`、`$emqx/commands/...` | EMQX Rule 转换 |
| 标签 | Device `metadata` | `tags: string[]` | key-value 编码为 tag 字符串 |
| MQTT Client ID | DA 插件主动填写 | Thing `mqttClientId` | 注册 payload；Fleets 不校验 DA SDK 策略 |

> **上报频率**：DA `v1/.../telemetry` 映射到 Shadow reported，适合中低频状态快照。若 Product 高频采样，应在 ThingType 修订时评估频率，必要时远期改用自定义 topic + `fleets_telemetry`（见 Fleets `docs/telemetry.md`），本期不实现。

### 3.2 ThingType 映射（DA Product → Fleets ThingType）

#### 3.2.1 自动转换 + 用户手工修订

fleets 插件提供 **默认转换**（`schema-mapper.ts`），在调用 Fleets API **之前**展示/允许用户手工修改、补齐 ThingType `schema`，**通过 Fleets 校验后方可注册成功**。

| 步骤 | 说明 |
|------|------|
| 1. 自动草稿 | 由 DA Product 生成 Fleets ThingType schema 草稿 |
| 2. 用户修订 | 在 Device Agent UI 或配置文件中手工调整（类型映射、`writable`、`commands.type: sync` 等） |
| 3. 本地校验 | 插件按 Fleets schema 规则预校验 |
| 4. API 注册 | `POST /api/v1/thing-types`；Fleets 服务端二次校验 |
| 5. 失败回退 | 校验失败则**不注册**，保留草稿供用户修改 |

**默认转换规则（草稿起点，非最终 schema）**：

| DA Product 字段 | Fleets ThingType `schema` |
|----------------|---------------------------|
| `properties` | `schema.properties`（用户标记 `writable: true` 的可写字段） |
| `events` | `schema.events.info.{eventName}`（**本期固定 info 桶**，见 §3.6） |
| `commands`（`parameters`） | `schema.commands.{name}`（`type: "sync"`，`input`/`output`；下行本期不可用，供 §12 预留） |

**类型映射参考**（用户修订时对照）：

| DA `DeviceSpec` 类型 | Fleets 类型 |
|---------------------|------------|
| `int` | `integer` |
| `float` | `number` |
| `bool` | `boolean` |
| `string` / `enum` / `json` / `array` | 同名或按 Fleets 规范调整 |

ThingType 命名：`da-product-{productId}`（**不含 `/`**）。

**Product 版本变更**：DA Product fork / 版本升级时，默认**沿用**已有 ThingType 名 `da-product-{productId}` 并 `PUT` 更新 schema；若需隔离版本，用户可手工指定新 ThingType 名并在插件配置中覆盖（运维表见 §5.2）。

**转换示例**（草稿 → 用户修订后注册）：

```json
// DA Product（节选）
{
  "properties": {
    "temp_a": { "type": "float", "description": "Zone A" },
    "temp_b": { "type": "float", "description": "Zone B" }
  },
  "events": {
    "overheat": { "eventType": "alert", "outputData": { "zone": { "type": "string", "required": true, "description": "" } } }
  },
  "commands": {
    "setInterval": { "description": "Set interval", "parameters": { "interval": { "type": "int", "required": true, "description": "" } } }
  }
}

// → 用户修订后的 Fleets ThingType schema（节选；本期事件一律 info 桶）
{
  "properties": {
    "temp_a": { "type": "number" },
    "temp_b": { "type": "number" },
    "reportIntervalSec": { "type": "integer", "writable": true }
  },
  "events": {
    "info": {
      "overheat": {
        "description": "Overheat alert",
        "payload": { "zone": { "type": "string" } }
      }
    }
  },
  "commands": {
    "setInterval": {
      "type": "sync",
      "timeoutMs": 5000,
      "input": { "interval": { "type": "integer" } },
      "output": {}
    }
  }
}
```

> **注意**：草稿中**不包含** `shadow.applyDesired` / `shadow.applyDelta`；纳入 §12。`commands` 定义本期仅作文档/schema 预留，控制台下发待 §12 启用。

### 3.3 Thing 命名与 ID 编码

Fleets 预配 EMQX 规则用 `nth(3, split(topic,'/'))` 提取 `thing_name`，**Thing 名不得包含 `/`**。

| 场景 | 格式 | 示例 |
|------|------|------|
| 桥接 Thing | `da-{productId}--{deviceId}` | `da-p-dual-temp-sensor--device-001` |
| ThingType | `da-product-{productId}` | `da-product-p-dual-temp-sensor` |

**编码规则**（`productId` / `deviceId` 含 `-` 以外特殊字符时）：

| 原字符 | 编码 |
|--------|------|
| `/` | `_slash_` |
| `--`（分隔符本身） | `_dashdash_` |

Go / TypeScript 实现 `encodeBridgeSegment(s)` / `decodeBridgeSegment(s)`；EMQX Rule SQL 用 **对称** `replace` 链（PoC 须验证）：

```sql
-- encode_bridge_segment(s)
replace(replace(${s}, '--', '_dashdash_'), '/', '_slash_')

-- decode_bridge_segment(s)
replace(replace(${s}, '_slash_', '/'), '_dashdash_', '--')
```

**双向对称**（不可只 encode 不 decode）：

| 方向 | 源 | 目标 | SQL 操作 |
|------|-----|------|---------|
| 上行 #1/#2 | `v1/{rawPid}/{rawDid}/...` | `$emqx/things/da-{encPid}--{encDid}/...` | encode |
| 上行 #3（远期） | `device-agent/.../responses` | `$emqx/commands/things/da-{encPid}--{encDid}/...` | encode |
| 下行 #6（远期） | `$emqx/commands/things/da-{encPid}--{encDid}/.../request` | `device-agent/{rawPid}/device/{rawDid}/commands` | decode |

### 3.4 Tags：DA key-value 编码为 Fleets tag 字符串

| Tag 字符串 | 含义 |
|-----------|------|
| `source=device-agent` | 标识桥接来源 |
| `da:product={productId}` | DA Product ID |
| `da:device={deviceId}` | DA Device ID |

注册示例：

```json
{
  "name": "da-p-dual-temp-sensor--device-001",
  "mqttClientId": "device-001-ns-001",
  "thingTypeId": "da-product-p-dual-temp-sensor",
  "tags": [
    "source=device-agent",
    "da:product=p-dual-temp-sensor",
    "da:device=device-001"
  ]
}
```

`mqttClientId` 由 **DA fleets 插件在注册时主动填写**（通常来自 `device.metadata.mqttClientId`）。Fleets 将其存为 Thing 字段，用于 lifecycle；**不要求**与 DA SDK 默认 `clientId` 格式相同，但设备实际连接 EMQX 的 Client ID 须与此值一致（集成方责任）。

过滤桥接设备：`GET /api/v1/things?tagName=source=device-agent`。

### 3.5 状态与 Lifecycle

| DA 状态 | Fleets 状态 | 触发方式 |
|---------|------------|---------|
| MQTT 连接 | `online` | EMQX `$events/client_connected` → ThingDatas lifecycle（**主路径**） |
| MQTT 断开 | `offline` | EMQX `$events/client_disconnected` |
| `error` | `offline` | fleets 插件 API 兜底更新（可选） |

**Lifecycle 前置条件**：DA 插件注册 Thing 时已填写 `mqttClientId`。Fleets `UpdateLifecycleStatus` 通过 `FindByMqttClientID(clientId)` 匹配，与 `thing.name` 解耦。

`da_bridge_mappings` 额外存储 `mqtt_client_id` 供排查。

**metadata 示例**（DA 侧，供插件读取后写入 Fleets）：

```json
{
  "mqttClientId": "device-001-ns-001",
  "source": "thermostat-firmware-v2"
}
```

### 3.6 Event severity 映射

**本期策略（v2.5）**：Rule #2 republish 时 **`severity` 固定为 `"info"`**；ThingType 注册时 DA 事件一律放入 `schema.events.info.{eventName}` 桶（无论 DA Product 中 `eventType` 为 `info` / `alert` / `error`）。

| 层级 | 本期行为 |
|------|---------|
| ThingType 注册 | 所有事件 → `events.info` 桶 |
| Rule #2 payload | `"severity": "info"` 硬编码 |
| DA MQTT event payload | 仍使用 `data.event` 作事件名，不含 severity |

**远期 TODO**（§12.4）：按 DA `eventType` 映射到 Fleets `info` / `warn` / `error` 桶，并在 Rule #2 写入对应 severity（可能需注册时生成 lookup 表注入 Rule SQL，或 DA payload 扩展 severity 字段）。

---

## 4. EMQX Rule Engine 规则设计

### 4.1 核心原理

所有桥接规则动作均为 **`republish`**，不调用 Fleets HTTP API。

```
【本期 — 上行】
  DA v1/.../telemetry (type=state) → Rule #1 → $emqx/things/.../shadow/update → Fleets 预配规则 → ThingDatas API
  DA v1/.../event                  → Rule #2 → $emqx/things/.../events/{event} → Fleets 预配规则 → EMQX Tables

【远期 — §12】
  命令响应 #3、命令下行 #6、Shadow #4/#5
```

### 4.2 规则总览

| ID | 方向 | 源 Topic | 目标 Topic | 说明 | 阶段 |
|----|------|---------|-----------|------|------|
| #1 | 上行 | `v1/+/+/telemetry` | `$emqx/things/da-{encPid}--{encDid}/shadow/update` | `type:state` → Shadow reported | **Phase 1** |
| #2 | 上行 | `v1/+/+/event` | `$emqx/things/da-{encPid}--{encDid}/events/{event}` | Event → Fleets event（severity 固定 info） | **Phase 2** |
| #3 | 上行 | `device-agent/+/device/+/responses` | `$emqx/commands/things/da-{encPid}--{encDid}/executions/{requestId}/response` | 命令响应 | **§12 远期** |
| #6 | 下行 | `$emqx/commands/things/+/executions/+/request` | `device-agent/{rawPid}/device/{rawDid}/commands` | 命令请求 → DA | **§12 远期** |
| #4 | 下行 | `$emqx/things/+/shadow/update`（desired） | `device-agent/.../commands` | Shadow desired → DA | **§12 远期** |
| #5 | 下行 | `$emqx/things/+/shadow/update/delta` | `device-agent/.../commands` | Shadow delta → DA | **§12 远期** |

> **Phase 1 必做**：#1。  
> **Phase 2 必做**：#2。  
> **#3/#6/#4/#5**：见 §12。

### 4.3 Rule #1: Telemetry → Shadow reported

DA SDK 发送两类 telemetry（均走 `v1/{pid}/{did}/telemetry`）：

| `type` | `data` 结构 | Rule #1 |
|--------|------------|---------|
| `status` | `{ "status": "online", "state": { ... } }` | **忽略**（lifecycle 处理连接状态） |
| `state` | 扁平属性对象 `{ "temp_a": 23.5, ... }` | **转 reported** |

**`type: state` 原始 payload：**

```json
{
  "type": "state",
  "data": { "temp_a": 23.5, "temp_b": 24.1 },
  "ts": 1750000000000
}
```

**目标 payload**（`state.reported`）：

```json
{
  "state": {
    "reported": {
      "temp_a": 23.5,
      "temp_b": 24.1
    }
  }
}
```

```sql
SELECT
  concat('$emqx/things/da-',
    replace(replace((regex_match(topic, '^v1/([^/]+)/([^/]+)/telemetry$'))[1], '--', '_dashdash_'), '/', '_slash_'),
    '--',
    replace(replace((regex_match(topic, '^v1/([^/]+)/([^/]+)/telemetry$'))[2], '--', '_dashdash_'), '/', '_slash_'),
    '/shadow/update'
  ) as target_topic,
  json_encode(map(
    'state', map(
      'reported', json_decode(payload)->'data'
    )
  )) as target_payload
FROM "v1/+/+/telemetry"
WHERE json_decode(payload)->>'type' = 'state'
  AND is_not_null_var(json_decode(payload)->'data')
```

### 4.4 Rule #2: Event → Fleets events

**目标 payload**（`data` **不含** `event` 元字段；`severity` **固定 `info`**）：

```json
{
  "eventType": "overheat",
  "severity": "info",
  "data": { "zone": "A", "temperature": 85.3 }
}
```

```sql
SELECT
  concat('$emqx/things/da-',
    replace(replace((regex_match(topic, '^v1/([^/]+)/([^/]+)/event$'))[1], '--', '_dashdash_'), '/', '_slash_'),
    '--',
    replace(replace((regex_match(topic, '^v1/([^/]+)/([^/]+)/event$'))[2], '--', '_dashdash_'), '/', '_slash_'),
    '/events/',
    json_decode(payload)->'data'->>'event'
  ) as target_topic,
  json_encode(map(
    'eventType', json_decode(payload)->'data'->>'event',
    'severity',  'info',
    'data',      json_decode(payload)->'data' - 'event'
  )) as target_payload
FROM "v1/+/+/event"
WHERE is_not_null_var(json_decode(payload)->'data'->>'event')
```

> PoC 须验证 `data - 'event'` 剔除事件名字段；若 EMQX 版本不支持，改用手动 `map` 构造 output 字段。远期 severity 分桶见 §12.4。

### 4.5 Rule #3: Command Response → Fleets command response（远期，§12）

模板保留于 `rule_templates.go`，**本期不注入**。`code` 判断使用数值比较（DA 响应 `code` 为 number）：

```sql
json_encode(map(
  'status', iif(json_decode(payload)->'code' = 0, 'SUCCEEDED', 'FAILED'),
  'result', coalesce(json_decode(payload)->'data', json_decode('{}'))
)) as target_payload
```

完整 SQL 见 v2.4 §4.5；启用前与 §12.2 命令下行一并 PoC。

### 4.6 Rule #6: Command Request → DA commands（远期，§12）

JobService 发布 `$emqx/commands/things/{name}/executions/{executionId}/request`，Rule #6 转为 `device-agent/{pid}/device/{did}/commands`（`commandId` → `requestId`，`action` → `cmd`）。

**本期不启用**。启用前须解决 `ErrNoMatchingSubscribers`（桥接设备不订阅 Fleets 命令 topic）等议题，见 §12.2。SQL 草案保留于 v2.4 §4.6。

### 4.7 Rule #4 / #5（远期，§12）

Shadow desired/delta 下行规则设计保留于 §12.1，**本期不注入、不启用**。

### 4.8 规则动作（统一）

```json
{
  "function": "republish",
  "args": {
    "topic": "${target_topic}",
    "payload": "${target_payload}",
    "qos": 1,
    "retain": false
  }
}
```

### 4.9 规则生命周期管理

```
插件 Start()  — 实现 BridgePlugin.Start（§5.2）
  ├─ 1. pg_try_advisory_lock(0xDAB001) — 失败则跳过 SyncRules，仅健康检查
  ├─ 2. 迁移 da_bridge_mappings（OnMigrate）
  ├─ 3. EMQX API 连通性检查（OnHealthCheck）
  ├─ 4. 按 ID 前缀 da-bridge- 查重，创建缺失规则（本期 #1–#2；远期规则默认关闭）
  └─ 5. 启动规则健康检查 goroutine

插件 Stop()  — 实现 BridgePlugin.Stop
  ├─ 1. pg_advisory_unlock(0xDAB001)
  ├─ 2. 删除本插件规则（DA_BRIDGE_KEEP_RULES_ON_STOP=true 时保留）
  └─ 3. 停止健康检查
```

### 4.10 EMQX Rule PoC 与测试

**环境**：Fleets 仓库 `.env`（`EMQX_API_*` + MQTT `tcp://localhost:1883`）。

**PoC 步骤**（Phase 1 第 1 天）：

1. Rule #1：验证 **仅** `type:state` telemetry → `$emqx/things/da-.../shadow/update` + `state.reported`；`type:status` **不**触发。
2. 验证 `regex_match`、`json_decode`、`map`、`replace` encode 链。
3. 固化 SQL 到 `rule_templates.go`；MQTT 订阅集成测试断言 topic/payload。

**Phase 2 PoC 追加**：Rule #2 event → `severity: info` + `data` 无 `event` 字段。

---

## 5. Fleets 端插件（device-agent-bridge）设计

### 5.1 概述

**包名**: `github.com/emqx/fleets/internal/plugins/deviceagent`

> `internal/plugins/` 为**新增模块**；与既有 `emqx_broker_setup_svc`（Fleets 预配规则）职责分离：后者不变，本插件仅管理 `da-bridge-*` 规则。

| 职责 | 说明 |
|------|------|
| 实现 `BridgePlugin` 接口 | 统一生命周期（§5.2） |
| EMQX Rule 生命周期 | 注入/同步/删除 #1–#2（#3/#6/#4/#5 模板预留、默认关闭） |
| 规则模板版本管理 | SQL + republish 动作 |
| `da_bridge_mappings` | DB 读写 + 缓存 |
| 规则健康监控 | 缺失/禁用告警 |
| Advisory lock | 多实例规则同步互斥 |

| 不负责 | 原因 |
|--------|------|
| 订阅 MQTT | Fleets 架构约束 |
| 设备自动发现 | DA 插件唯一注册入口 |
| 修改 JobService / ShadowService | 本期不改；下行远期 §12 |
| 命令 / Shadow / Jobs 下行 | §12 远期 TODO |

### 5.2 BridgePlugin 接口与生命周期规范

Fleets 新增 `internal/plugins/bridge.go`，定义所有桥接插件的统一契约：

```go
// BridgePlugin 是 Fleets 侧 IoT 桥接插件的统一生命周期接口。
// device-agent-bridge 为首个实现；未来可扩展其他平台桥接。
type BridgePlugin interface {
    // Name 返回插件唯一标识，如 "device-agent".
    Name() string

    // Start 在 Fleets 实例启动时调用：迁移、抢锁、注入 EMQX 规则、启动监控。
    Start(ctx context.Context) error

    // Stop 在 Fleets 优雅关闭时调用：释放锁、可选删除规则、停止监控。
    Stop(ctx context.Context) error

    // HealthCheck 返回插件健康状态（规则存在且启用、EMQX 可达等）。
    HealthCheck(ctx context.Context) error

    // SyncRules 幂等同步 EMQX 规则到期望状态（仅持锁实例执行）。
    SyncRules(ctx context.Context) error
}

// BridgeHooks 供核心服务调用的可选挂钩（保持最小面）。
type BridgeHooks interface {
    // OnThingCreated 在 ThingService.Create 成功后调用；桥接 Thing 写映射表。
    OnThingCreated(ctx context.Context, thing *model.Thing, tagNames []string) error

    // OnThingDeleted 在 ThingService.Delete 成功后调用；级联删映射。
    OnThingDeleted(ctx context.Context, thingID string) error
}
```

**生命周期时序**：

| 阶段 | 触发 | 插件行为 | 核心行为 |
|------|------|---------|---------|
| 启动 | `app.go` 组装 | `Start` → 迁移 → 抢锁 → `SyncRules` → 健康监控 | 注入 `BridgeHooks` 到 ThingService |
| 运行 | 定时 / 告警 | `HealthCheck`；规则缺失 → `SyncRules` | Thing 创建/删除触发 `OnThingCreated`/`OnThingDeleted` |
| 关闭 | SIGTERM | `Stop` → 解锁 → 可选删规则 | — |
| 多实例 | 并发启动 | `pg_try_advisory_lock(0xDAB001)`；抢锁失败仅监控 | 共享 PG 映射表 |

**运维场景处理规范**（插件 + 核心协作）：

| 场景 | 处理 |
|------|------|
| DA 设备未注册 | Rule republish → Fleets 查无 Thing → 拒绝（不自动创建） |
| DA 设备删除（DA 侧） | fleets 插件调用 `DELETE /api/v1/things/{id}` + 清本地映射；`OnThingDeleted` 清 `da_bridge_mappings` |
| Thing 重复注册 | `(da_product_id, da_device_id)` UNIQUE；已存在则 PUT/PATCH 元数据或跳过 |
| `mqtt_client_id` 冲突 | Fleets API 返回冲突；fleets 插件记录错误，不覆盖已有 Thing |
| Product schema 变更 | 用户修订 ThingType 后 `PUT /api/v1/thing-types/{id}`；已注册 Thing 继承新 schema |
| Product 版本 fork | 默认更新同名校验 ThingType；需隔离时用户指定新 ThingType 名 |
| `OnThingCreated` 失败 | 记录 error 指标 + 结构化日志；**不**回滚 Thing 创建；Phase 3 增加映射修复 / resync 运维入口 |
| EMQX 重启规则丢失 | `HealthCheck` 告警 → `SyncRules` 重建 |
| Fleets 重启 | 规则在 EMQX 持续运行；`Start` 时查重同步 |
| 多 Fleets 实例 | advisory lock 互斥 `SyncRules` |
| Fleets 不可用 | DA 注册入同步队列重试；EMQX republish 不受影响 |
| 插件禁用 | `DA_BRIDGE_ENABLED=false`；`Stop` 可选保留规则 |
| Thing 名含 `/` | 注册前 `encodeBridgeSegment` 校验拒绝 |
| 控制台对桥接设备发命令 | 本期不可用；Fleets 可创建执行记录但设备收不到（§12） |

### 5.3 配置与依赖

```go
type BridgeDependencies struct {
    EmqxClient   *emqx.Client
    DB           *pgxpool.Pool
    MappingStore store.MappingStore
    Logger       *slog.Logger
    Config       *BridgePluginConfig
}

type BridgePluginConfig struct {
    Enabled bool
    Rules struct {
        IDPrefix               string // "da-bridge-"
        TelemetryEnabled       bool   // #1 — 默认 true
        EventEnabled           bool   // #2 — Phase 2
        CommandResponseEnabled bool   // #3 — 默认 false，§12
        CommandRequestEnabled  bool   // #6 — 默认 false，§12
        ShadowDesiredEnabled   bool   // #4 — 默认 false，§12
        ShadowDeltaEnabled     bool   // #5 — 默认 false，§12
    }
    Mapping struct {
        ThingNamePrefix string // "da-"
        ThingNameSep    string // "--"
        TypeNamePrefix  string // "da-product-"
    }
    KeepRulesOnStop bool
}
```

### 5.4 包结构

```
internal/plugins/
├── bridge.go                    # BridgePlugin + BridgeHooks 接口
└── deviceagent/
    ├── plugin.go                # BridgePlugin 实现
    ├── hooks.go                 # BridgeHooks 实现（映射表读写）
    ├── config.go
    ├── emqx_rule/
    │   ├── rule_manager.go
    │   ├── rule_templates.go    # #1–#2 本期；#3/#6/#4/#5 模板预留
    │   └── rule_monitor.go
    ├── store/
    │   ├── mapping_store.go
    │   └── migration.go
    └── encode.go
```

### 5.5 da_bridge_mappings 表

```sql
CREATE TABLE da_bridge_mappings (
    id              BIGSERIAL PRIMARY KEY,
    da_product_id   TEXT NOT NULL,
    da_device_id    TEXT NOT NULL,
    thing_type_id   UUID NOT NULL,
    thing_id        UUID NOT NULL UNIQUE,
    thing_name      TEXT NOT NULL UNIQUE,
    mqtt_client_id  TEXT NOT NULL,
    da_metadata     JSONB,
    last_synced_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (da_product_id, da_device_id)
);

CREATE INDEX idx_da_bridge_mappings_mqtt_client_id ON da_bridge_mappings (mqtt_client_id);
```

**写入时机**：`BridgeHooks.OnThingCreated`，检测 `source=device-agent` tag，解析 `da:product=` / `da:device=`。

**删除时机**：`BridgeHooks.OnThingDeleted` 级联删除。

### 5.6 ThingService 挂钩（唯一核心改动）

```go
func (s *ThingService) Create(ctx context.Context, t *model.Thing, tagNames []string) (*model.CreateThingResult, error) {
    result, err := s.createThingCore(ctx, t, tagNames)
    if err != nil { return nil, err }

    if s.bridgeHooks != nil {
        if hookErr := s.bridgeHooks.OnThingCreated(ctx, result.Thing, tagNames); hookErr != nil {
            thingHandlerLogger().Warn("bridge hook OnThingCreated failed", "error", hookErr)
            // 指标：bridge_mapping_write_failures_total
        }
    }
    return result, nil
}
```

> **不改动** `JobService`、`ShadowService`、ThingDatas Handler。

### 5.7 核心代码改动清单

| 文件 | 改动 |
|------|------|
| `internal/plugins/bridge.go` | 新增 `BridgePlugin` + `BridgeHooks` 接口 |
| `internal/plugins/deviceagent/**` | device-agent-bridge 实现 |
| `internal/app/app.go` | 加载插件、注入 hooks |
| `internal/config/config.go` | `DA_BRIDGE_*` 配置项 |
| `internal/service/thing_svc.go` | 调用 `BridgeHooks`（创建/删除） |
| `migrations/0000xx_device_agent_bridge.up.sql` | 映射表 |

**明确不改动**：`job_svc.go`、`shadow_svc.go`、ThingDatas Handler、Fleets 预配 EMQX 规则脚本。

---

## 6. Device-Agent 端 IoT Platform 插件设计

### 6.1 插件框架定位

Device-Agent 新增 **IoT Platform 插件**能力，用于将 DA 设备注册到外部 IoT 云平台。**Fleets 为首个实现**；未来可扩展其他平台（各平台独立子包，共享框架接口）。

**不使用 Channel 抽象**（Channel 面向消息总线入出站，不适合设备注册同步）。插件通过 **device-management 注册回调** 触发，在设备/Product 生命周期事件上执行云平台同步。

**回调设计要点**：

- `registry` 支持**多插件链式调用**（按启用顺序）
- 单插件 `onDeviceRegistered` 失败 **不阻断** DA 设备注册；错误写入日志 + sync-queue 重试
- 回调在 `registerDeviceWithResult` 成功返回后异步或 await 调用（实现时择一，须保证不拖死热路径）

**包结构**：

```
packages/iot-platforms/
├── types.ts              # IotPlatformPlugin 接口
├── registry.ts           # 插件注册、启用、配置
├── sync-queue.ts         # 通用重试队列（SQLite 持久化）
└── fleets/               # Fleets 首个实现
    ├── index.ts
    ├── client.ts         # Fleets REST API
    ├── schema-mapper.ts  # Product → ThingType 草稿 + 校验
    ├── device-registrar.ts
    └── config.ts
```

**挂钩点**（`packages/device-management`）：

| 事件 | 回调 | fleets 插件行为 |
|------|------|----------------|
| `registerDeviceWithResult` | `onDeviceRegistered` | 读 `metadata.mqttClientId` → 主动填写 Fleets Thing `mqttClientId` |
| Product 创建/更新 | `onProductChanged` | 生成 ThingType 草稿，提示用户修订后注册 |
| 设备删除 | `onDeviceDeleted` | `DELETE` Fleets Thing + 清本地映射 |

### 6.2 IotPlatformPlugin 接口

```typescript
export interface IotPlatformPlugin {
  readonly platformId: string; // e.g. "fleets"

  start(): Promise<void>;
  stop(): Promise<void>;

  /** 设备注册成功后触发；须能解析出用于 Fleets 注册的 mqttClientId */
  onDeviceRegistered(device: DeviceInfo, product: ProductInfo): Promise<void>;

  /** Product 变更时触发；返回 ThingType 草稿供用户修订 */
  onProductChanged?(product: ProductInfo): Promise<void>;

  /** 设备删除时触发 */
  onDeviceDeleted?(deviceId: string, productId: string): Promise<void>;
}
```

### 6.3 fleets 插件职责

| 职责 | 说明 |
|------|------|
| ThingType 注册 | 自动草稿 + **用户手工修订** + 本地/Fleets 校验通过后 `POST /thing-types` |
| Thing 注册 | `POST /api/v1/things`；**主动填写** `mqttClientId`（通常来自 `device.metadata.mqttClientId`） |
| 命名与 tags | `da-{pid}--{did}`；`source=device-agent`、`da:product=`、`da:device=` |
| 本地映射 | SQLite 记录 DA↔Fleets 映射，供 UI 与重试 |
| 同步队列 | Fleets 不可用时注册/元数据重试 |
| **不订阅 MQTT** | telemetry/event 由 EMQX Rule 处理 |

### 6.4 注册前置条件与 payload

**前置条件**：

1. 可解析出用于 Fleets 注册的 `mqttClientId`（通常 `device.metadata.mqttClientId` 非空）
2. 对应 Product 的 Fleets ThingType 已注册且通过校验
3. Fleets 插件已启用且 API 凭据有效

```typescript
const mqttClientId = device.metadata?.mqttClientId;
if (!mqttClientId) {
  logger.warn("Skip Fleets registration: metadata.mqttClientId is required", { deviceId });
  return;
}

await fleetsClient.createThing({
  name: encodeBridgeThingName(productId, deviceId),
  mqttClientId, // DA 插件主动填写；Fleets 不关心 DA SDK clientId 策略
  thingTypeId: encodeBridgeTypeName(productId),
  tags: [
    "source=device-agent",
    `da:product=${productId}`,
    `da:device=${deviceId}`,
  ],
});
```

**集成方责任**：确保设备连接 EMQX 时使用的 Client ID 与注册时填写的 `mqttClientId` 一致，以便 Fleets lifecycle 正确更新在线状态。

### 6.5 ThingType schema 用户修订流程

```
Product 创建/变更
  → schema-mapper 生成 Fleets ThingType 草稿（事件默认 info 桶）
  → UI 展示草稿（或导出 JSON 供编辑）
  → 用户手工修订（类型、writable、commands.type 等）
  → 插件本地校验（对齐 Fleets thing-model 规则）
  → POST /api/v1/thing-types
  → 成功：记录 thingTypeId 映射；失败：展示 Fleets 错误，不注册设备
```

### 6.6 配置

```typescript
// .env / config.json
IOT_PLATFORM_FLEETS_ENABLED=true
IOT_PLATFORM_FLEETS_API_URL=https://fleets.example.com
IOT_PLATFORM_FLEETS_API_KEY=...
IOT_PLATFORM_FLEETS_API_SECRET=...

// fleets 插件 sync 配置
sync: z.object({
  onDeviceRegister: z.boolean().default(true),
  onProductChange: z.boolean().default(true),
  onDeviceDelete: z.boolean().default(true),
  syncIntervalMs: z.number().default(60000),
}),
```

### 6.7 本期能力限制

| 能力 | 状态 |
|------|------|
| Shadow reported 上行 | ✅ Rule #1 |
| Event 上行 | ✅ Phase 2 Rule #2 |
| Lifecycle | ✅ |
| Fleets 控制台 sync 命令 | ❌ §12 |
| Shadow desired/delta | ❌ §12 |
| Fleets Jobs | ❌ §12 |

完整设计见 [§12](#12-远期待办shadow--命令下行--jobs)。

---

## 7. 认证与安全

### 7.1 认证架构

```
fleets IoT 插件 ──Basic Auth──► Fleets REST API（注册、查询、删除）
device-agent-bridge ──EMQX_API_KEY/SECRET──► EMQX Rule API
DA 设备 ──MQTT Auth──► EMQX ──callback──► Fleets broker-auth（可选）
```

### 7.2 fleets 插件 API Key 最小权限

```json
{
  "name": "device-agent-fleets-bridge",
  "permissions": {
    "GET": ["/api/v1/things", "/api/v1/thing-types"],
    "POST": ["/api/v1/things", "/api/v1/thing-types"],
    "PUT": ["/api/v1/thing-types", "/api/v1/things"],
    "PATCH": ["/api/v1/things"],
    "DELETE": ["/api/v1/things"]
  }
}
```

> 注册阶段不需要 `POST /api/v1/thing-datas`（telemetry 走 EMQX Rule）。

---

## 8. Fleets API 扩展规范

**无需新增端点**：

| 能力 | 实现方式 | 本期 |
|------|---------|------|
| 桥接设备过滤 | `GET /api/v1/things?tagName=source=device-agent` | ✅ |
| Shadow 上报（reported） | Rule #1 → 既有 shadow 规则 → ThingDatas API | ✅ |
| Event 上报 | Rule #2 → 既有 events 规则 → EMQX Tables | Phase 2 |
| 命令下发 | JobService → Rule #6 → DA `commands` | **§12** |
| 命令响应 | Rule #3 → 既有 command response 规则 | **§12** |
| Shadow 下行（desired/delta） | Rule #4/#5 | **§12** |
| Jobs | `$emqx/things/.../jobs/*` | **§12** |
| Lifecycle | Thing `mqttClientId`（DA 注册时填写）→ 既有 lifecycle 规则 | ✅ |

---

## 9. 配置与部署

### 9.1 Fleets 端

```bash
DA_BRIDGE_ENABLED=true
DA_BRIDGE_RULES_ID_PREFIX=da-bridge-
DA_BRIDGE_RULES_TELEMETRY_ENABLED=true
DA_BRIDGE_RULES_EVENT_ENABLED=true
DA_BRIDGE_RULES_COMMAND_RESPONSE_ENABLED=false   # §12
DA_BRIDGE_RULES_COMMAND_REQUEST_ENABLED=false    # §12
DA_BRIDGE_RULES_SHADOW_DESIRED_ENABLED=false
DA_BRIDGE_RULES_SHADOW_DELTA_ENABLED=false
DA_BRIDGE_KEEP_RULES_ON_STOP=false
DA_BRIDGE_MAPPING_THING_NAME_PREFIX=da-
DA_BRIDGE_MAPPING_THING_NAME_SEP=--
DA_BRIDGE_MAPPING_TYPE_NAME_PREFIX=da-product-
```

### 9.2 Device-Agent 端

```bash
IOT_PLATFORM_FLEETS_ENABLED=true
IOT_PLATFORM_FLEETS_API_URL=http://localhost:8080
IOT_PLATFORM_FLEETS_API_KEY=...
IOT_PLATFORM_FLEETS_API_SECRET=...
```

设备 `metadata` 示例（供 DA 插件读取并写入 Fleets `mqttClientId`）：

```json
{ "mqttClientId": "device-001-ns-001" }
```

### 9.3 启动顺序

```
1. PostgreSQL / EMQX Tables
2. EMQX Broker
3. Fleets（device-agent-bridge 插件 Start）
   ├── 迁移 da_bridge_mappings
   ├── advisory lock → 注入规则 #1（Phase 2 追加 #2）
   └── HTTP 服务
4. Device-Agent（fleets IoT 插件 start）
   ├── 确保 Product ThingType 已用户修订并注册
   └── 全量注册已有 DA 设备（须提供 metadata.mqttClientId）
```

---

## 10. 实施路线图

### Phase 1: 基础桥接 + PoC（2–3 周）

| 任务 | 模块 | 预估 |
|------|------|------|
| 1.0 EMQX Rule PoC（Rule #1，`type=state` 过滤） | `.env` Broker + `EMQX_API_*` | 1d |
| 1.1 `BridgePlugin` 接口 + 插件骨架 | `internal/plugins/` | 2d |
| 1.2 Rule Manager + advisory lock | `emqx_rule/` | 2d |
| 1.3 规则模板 #1 | `rule_templates.go` | 1d |
| 1.4 映射表 + migration + `BridgeHooks` | `store/` + `thing_svc.go` | 1d |
| 1.5 DA `iot-platforms` 框架 + fleets 插件骨架 | `packages/iot-platforms/` | 2d |
| 1.6 device-management 注册回调挂钩 | `device-management` | 1d |
| 1.7 ThingType 草稿 + 用户修订 + 设备注册器 | `fleets/schema-mapper.ts` 等 | 3d |
| 1.8 端到端测试 | telemetry → Shadow + lifecycle | 2d |

**交付物**：DA 插件主动填写 `mqttClientId` → Fleets Thing → Rule #1（仅 `type:state`）→ Shadow reported；lifecycle 在线状态可见。

### Phase 2: Event 上报（1 周）

| 任务 | 模块 | 预估 |
|------|------|------|
| 2.1 Rule #2 Event（severity 固定 info） | `rule_templates.go` | 2d |
| 2.2 Event E2E + ThingType info 桶对齐 | 集成测试 + `schema-mapper` | 2d |
| 2.3 运维场景（删除、重试、冲突、映射失败指标） | 插件 + fleets 插件 | 1d |

**不包含**：命令下行/响应（§12）、Shadow 下行（§12）、Jobs（§12）、JobService 改动。

### Phase 3: 健壮性

| 任务 | 说明 |
|------|------|
| 规则版本升级与热同步 | 模板 version bump |
| 多实例 HA 验证 | advisory lock + 规则监控 |
| DA 注册失败重试队列 | SQLite 持久化 |
| encode 边界测试 | 特殊字符 productId/deviceId |
| 映射表 resync 运维入口 | 修复 `OnThingCreated` 失败场景 |
| 第二个 IoT Platform 插件占位验证 | 框架可扩展性 |

### Phase 4: 生产化

监控告警、运维文档、升级回滚流程、桥接设备能力边界用户文档（§2.2.7）。

---

## 11. 附录

### A. 异常场景

见 §5.2 运维场景处理规范表。

### B. 版本变更记录

| 版本 | 日期 | 变更 |
|------|------|------|
| v1.0 | 2026-06-30 | 初始草案 |
| v2.0 | 2026-06-30 | Fleets Plugin 改为 EMQX Rule 注入 |
| v2.2 | 2026-07-01 | 协议对齐 Fleets 契约；命名、Tags、lifecycle |
| v2.3 | 2026-07-01 | encode/decode；EMQX PoC；Shadow 标为待办 |
| v2.4 | 2026-07-01 | 评审定稿：mqttClientId；Shadow §12；ThingType 用户修订；IoT Platform 框架；BridgePlugin；Rule #6 |
| **v2.5** | **2026-07-01** | **评审修订：命令下行+#3 收入 §12；Jobs §12；severity 固定 info；Rule #1 仅 type=state；mqttClientId 由 DA 主动填写、Fleets 不关心 SDK clientId；其余评审项合入** |

### C. v2.5 相对 v2.4 的关键变更

1. **本期范围收窄**：仅注册 + telemetry + event + lifecycle；命令/Shadow/Jobs 下行统一 §12
2. **命令下行**：Rule #6、Rule #3 从 Phase 2 移至 §12（含 `ErrNoMatchingSubscribers` 待研）
3. **Event severity**：本期固定 `info`；分桶映射列入 §12.4 TODO
4. **Rule #1**：增加 `WHERE type = 'state'`，忽略 `type:status` telemetry
5. **mqttClientId**：明确 DA 插件主动填写；Fleets 不校验 DA SDK clientId；lifecycle 一致由集成方保证
6. **Jobs**：桥接设备不支持 Fleets Jobs 协议，§12.3 TODO
7. **其它评审合入**：Rule #3 `code` 数值比较、BridgeHooks 失败指标、Product 版本策略、上报频率说明、`internal/plugins` 与预配规则分工、device-management 多插件回调

---

## 12. 远期待办（Shadow / 命令下行 / Jobs）

> **优先级**: P3（远期） | **状态**: 待开始 | **更新**: 2026-07-01

### 12.1 Shadow 下行（远期）

| 能力 | 本期（v2.5） | 远期 |
|------|-------------|------|
| Shadow reported 上行 | Rule #1 ✅ | — |
| Shadow desired/delta 下行 | **不实现** | Rule #4/#5 + DA Shadow |
| Fleets 控制台改 desired | 仅更新 Fleets PG，**不到达 DA 设备** | E2E 闭环 |
| DA Shadow 功能 | **不存在** | 新增 MQTT/命令层 Shadow |
| DA SDK | 不支持 desired/delta | 对齐 `shadow.applyDesired` / `shadow.applyDelta` |

**待办列表**：

- [ ] **DA Shadow 功能**：desired/delta 应用 + `type: state` 遥测回写
- [ ] **DA SDK 对齐**：`shadow.applyDesired`、`shadow.applyDelta` 命令处理
- [ ] Fleets Rule 模板 #4（desired）、#5（delta）
- [ ] ThingType 增加 `shadow.apply*` 命令定义（用户修订流程内）
- [ ] 启用 `DA_BRIDGE_RULES_SHADOW_DESIRED_ENABLED` / `SHADOW_DELTA_ENABLED`
- [ ] E2E：Fleets PATCH shadow desired → Rule #4 → DA 设备 → telemetry → Fleets reported 更新

**预留设计**：Fleets `ShadowService` 发布 desired/delta → Rule #4/#5 → `device-agent/.../commands`。SQL 草案见 v2.4 §4.7，启用前须 DA Shadow 就绪并重新 PoC。

### 12.2 命令下行（远期）

| 能力 | 本期（v2.5） | 远期 |
|------|-------------|------|
| Fleets 控制台 / API 下发 sync 命令 | **不可用** | Rule #6 + Rule #3 闭环 |
| DA 设备收命令 | 仅 DA 自有通道 | `device-agent/.../commands` |
| JobService | **不修改** | 仍发布标准 topic；Rule #6 转换 |

**待办列表**：

- [ ] Rule #6：`$emqx/commands/.../request` → `device-agent/.../commands`（`commandId`→`requestId`，`action`→`cmd`）
- [ ] Rule #3：DA `responses` → Fleets command response（`code` 数值比较）
- [ ] 解决 **`ErrNoMatchingSubscribers`**：桥接设备不订阅 Fleets 命令 topic 时，`JobService` Publish 可能将执行标为 FAILED（见评审）；备选：EMQX 配置、占位订阅、或 JobService 对 `da-` 前缀例外
- [ ] E2E：Fleets `POST /commands` → DA 执行 → Rule #3 → Fleets 执行状态 SUCCEEDED
- [ ] 启用 `DA_BRIDGE_RULES_COMMAND_REQUEST_ENABLED` / `COMMAND_RESPONSE_ENABLED`
- [ ] 用户文档：桥接设备命令能力启用条件

**预留设计**：SQL 草案见 v2.4 §4.5–4.6；与 Shadow 下行类似，启用前完整 PoC。

### 12.3 Jobs 异步任务（远期）

Fleets **Jobs 协议**（`$emqx/things/{name}/jobs/notify`、`jobs/get`、`jobs/update` 等）与 DA MQTT 契约**无对应实现**。桥接设备本期不支持 Fleets 控制台创建 Job 并在设备侧执行。

**待办列表**：

- [ ] 评估 DA 侧 Jobs 适配策略（新 MQTT 主题 vs 映射为长时 sync 命令 vs 明确不支持）
- [ ] 若支持：设计 EMQX Rule 或 DA 插件下行转换（类比 Rule #6）
- [ ] ThingType / 文档标明桥接设备 Jobs 能力边界
- [ ] E2E（若实现）

### 12.4 Event severity 分桶（远期）

本期 Rule #2 与 ThingType 注册均 **固定 `info`**。远期可选方案：

- [ ] 注册 ThingType 时按 DA `eventType`（`alert`→`warn`，`error`→`error`）分桶
- [ ] 注册时生成 `eventName → severity` lookup，注入 Rule #2 SQL（ThingType 变更时 `SyncRules`）
- [ ] 或 DA event payload 增加 `severity` 字段（需 SDK/文档变更）
- [ ] 与 Fleets `schema.events.{severity}` 及 `fleets_events.severity` 列对齐

---

## 13. 评审待定问题

> **评审日期**: 2026-07-01  
> **评审结论**: v2.5 架构方向正确，与 Fleets「不订阅 MQTT、上行走 EMQX Rule」及 Device Agent MQTT 契约基本对齐；本期范围收窄合理。Phase 1 启动前须优先闭合 **mqttClientId 闭环** 与 **EMQX Rule PoC** 两类阻塞项。

### 13.1 阻塞项（Phase 1 前必须定案）

| # | 问题 | 背景 | 备选方向 |
|---|------|------|---------|
| B1 | **`mqttClientId` 由谁生成、如何持久化？** | DA SDK 默认 `clientId` 为 `{namespace}/{deviceId}/{Date.now()}`，重连会变；Fleets lifecycle 依赖 Thing.`mqttClientId` 与 EMQX `clientid` 精确匹配；当前 DA `device.metadata` 尚无 `mqttClientId` 约定 | A) IoT 插件注册时由 SDK 显式 `clientId` 写入 metadata 并同步 Fleets；B) 插件用 `{namespace}/{deviceId}` 固定格式自动生成；C) 文档要求集成方手工配置，DA 不提供默认值 |
| B2 | **EMQX Rule SQL 能否在目标 Broker 版本跑通？** | PoC 依赖 `regex_match`、`json_decode`、`map`、`data - 'event'`、encode `replace` 链；任一不支持即需改 SQL 或升级 EMQX | Phase 1 第 1 天 PoC 产出「已验证 EMQX 版本 + 固化 SQL」 |
| B3 | **ThingType 用户修订 UI 落在哪？** | §3.2 / §6.5 要求人工修订 schema，但 DA Web 尚无 Fleets 集成入口 | A) Phase 1 仅 JSON 导出/手工编辑 + 配置文件；B) 同步做 Workspace 设置页；C) 首版自动映射 + 失败时再人工介入 |
| B4 | **注册与 telemetry 的时序** | Rule republish 不校验 Thing 是否存在；Fleets 预配 shadow 规则对 Greptime **无 Thing 校验**，对 PG 走 `/shadow/reported/pg` 才 404 | A) 约定「先注册再发 telemetry」；B) DA 插件注册成功后再 `publishStateSnapshot`；C) 接受 Greptime 孤儿数据并定期清理 |

### 13.2 架构与数据一致性

| # | 问题 | 说明 |
|---|------|------|
| A1 | 未注册设备的「拒绝」边界 | §5.2 写「查无 Thing → 拒绝」，实际仅 PG shadow 更新失败；`fleets_shadow_reported` / `fleets_events` Greptime 仍可能写入 `thing_name=da-...` 孤儿时序。是否可接受？是否需在 Fleets 预配规则加存在性校验（改动面大）？ |
| A2 | `da_bridge_mappings` 与 tags 双源 | 映射表由 `OnThingCreated` 解析 tags 写入；若运维在 Fleets 控制台改 tags，映射表可能漂移。是否需要 `GET /bridge-mappings` 运维 API 或 resync 仅信映射表？ |
| A3 | DA 侧删除 vs Fleets 侧删除 | DA 删设备 → 插件 `DELETE /things`；Fleets 手工删 Thing → DA 本地映射残留。双向 reconcile 策略与 Phase 3 resync 入口范围？ |
| A4 | Product schema 变更 | 默认 `PUT` 同名校验 ThingType；已注册 Thing 的 reported 字段若从 schema 移除，Fleets 是否校验 reported 键（当前 `UpdateShadowReportedPG` **不**校验 schema）？ |
| A5 | 多 Fleets 实例共享同一 EMQX | `da-bridge-*` 规则全局唯一；两 Fleets 连同一 Broker 会冲突。是否文档明确「一 Broker 对应一 Fleets」？ |

### 13.3 Device Agent 侧

| # | 问题 | 说明 |
|---|------|------|
| D1 | IoT 插件回调挂载点 | `registerDeviceWithResult` 尚无 platform 回调；异步 vs await 未决，热路径 latency 预算？ |
| D2 | `metadata.mqttClientId` 注入时机 | 设备首次 MQTT 连接前可能尚无 metadata；是否从 gateway MQTT transport 在首次 connect 时回写？ |
| D3 | 全量注册已有设备 | §9.3 要求启动时全量注册；历史设备无 `mqttClientId` 时批量跳过还是阻断启用插件？ |
| D4 | Product fork 与 ThingType | 多版本 Product 共用 `da-product-{productId}` 时 schema 合并策略；fork 后旧设备是否仍绑旧 schema？ |

### 13.4 Fleets 侧

| # | 问题 | 说明 |
|---|------|------|
| F1 | 桥接设备误发命令 | 本期无 Rule #6，但 `POST /commands` 仍可用；`JobService` 遇 `ErrNoMatchingSubscribers` 会将 execution 标 **FAILED**。是否在 API/UI 对 `source=device-agent` tag 提前拒绝或警告？ |
| F2 | `BridgeHooks` 失败不回滚 | Thing 已创建但映射失败 → 孤儿 Thing（有 shadow、无 mapping）。Phase 3 resync 是手动脚本还是 Admin API？ |
| F3 | Advisory lock ID `0xDAB001` | 是否与 Fleets 其他 advisory lock 冲突？是否改用 `HashToInt64("da-bridge-sync")` 等与 shadow 一致的模式？ |
| F4 | 插件与 `emqx_broker_setup_svc` 职责 | 预配规则 ID 前缀 `fleets_*`，桥接规则 `da-bridge-*`；升级时规则版本 bump 与 `SyncRules` 冲突处理策略？ |
| F5 | ThingType 命名 `thingTypeId` | API 接受 UUID 或 **全局唯一 type name**；插件传 `da-product-{productId}` 字符串是否已确认全局唯一约束满足？ |

### 13.5 安全与运维

| # | 问题 | 说明 |
|---|------|------|
| S1 | DA 设备 MQTT 认证 | Fleets broker-auth 按 Thing 凭据；DA 设备可能用独立认证。同一 Broker 上 DA 与 Fleets 原生设备 ACL 如何隔离？ |
| S2 | Fleets API Key 权限 | 最小权限仅 thing/thing-type CRUD；是否需要 `GET /shadow` 做注册后校验？ |
| S3 | 规则停用时行为 | `DA_BRIDGE_KEEP_RULES_ON_STOP=true` 时 Fleets 关闭后 republish 仍运行；与插件版本 SQL 漂移如何治理？ |
| S4 | encode 边界 | `productId`/`deviceId` 含 `_slash_`、`_dashdash_` 字面量时 encode 歧义；PoC 是否覆盖 fuzz 用例？ |

### 13.6 远期（§12）预决策

| # | 问题 | 说明 |
|---|------|------|
| L1 | 命令下行 `ErrNoMatchingSubscribers` | §12.2 列备选：EMQX 配置 / 占位订阅 / JobService 对 `da-` 前缀例外。倾向哪种？是否接受「桥接设备命令 execution 创建但不 Publish」？ |
| L2 | Shadow 下行实现路径 | Rule #4/#5 转 DA `commands` vs 新增 DA Shadow 订阅；与 DA「不发明 shadow topic」文档约束是否冲突？ |
| L3 | Jobs 策略 | §12.3 三选一（新 MQTT / 长时 sync 命令 / 明确不支持）的产品立场？ |
| L4 | Event severity | 注册时 lookup 表注入 Rule SQL vs DA payload 增字段；运维复杂度 vs 运行时灵活性？ |

### 13.7 建议的 Phase 1 验收标准（补充）

1. 单设备：DA 注册（含稳定 `mqttClientId`）→ Fleets Thing + mapping + lifecycle online → Rule #1 仅 `type:state` 更新 PG shadow reported。
2. `type:status`（含嵌套 `data.state`）**不**改变 reported。
3. 未注册设备发 telemetry：PG 不更新；Greptime 行为文档化（接受或拒绝）。
4. EMQX Rule SQL 在目标版本通过集成测试固化。
5. Product → ThingType 至少一条「自动草稿 + 人工修订 + API 校验通过」样例路径跑通。

---

> **文档维护者**: 集成团队  
> **下一步**: Phase 1 PoC（Rule #1 `type=state` + DA 主动填写 mqttClientId + 单设备注册）→ 启动开发
