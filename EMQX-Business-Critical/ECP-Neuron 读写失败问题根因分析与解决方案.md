# ECP-Neuron 读写失败问题根因分析与解决方案

> 分析日期：2026-07-03  
> 涉及项目：EMQX-Business-Critical (ECP 1.11 Beta) / emqx-bc-web (ECP 前端) / Neuron 2.6.3  
> 客户部署规模：191 个边缘服务，5-6 个组织，单 Neuron 70-80 南向驱动，每驱动 50-100+ 点位  
> 客户使用模式：直连模式（Direct Mode）  
> Neuron 前端：已完整集成在 ECP 前端项目 emqx-bc-web 中（228 个文件），ECP 后端仅代理 Neuron API 请求

---

## 一、问题总览

| 序号 | 问题现象 | 严重程度 | 根因归属 |
|------|----------|----------|----------|
| 1 | ECP 跳转 Neuron 页面弹出代理失败报错 | 🔴 高 | ECP 代理层（超时 + 连接池） |
| 2 | Neuron 数据监控页面写入指令失败 | 🔴 高 | ECP 代理超时 + Neuron 内部拥塞 |
| 3 | 多页面切换卡顿 | 🟡 中 | ECP 中间件 + 连接池 + 数据库 |
| 4 | PostgreSQL 频繁重启（26 次） | 🔴 高 | 连接池配置 + 内存压力 |

---

## 二、架构说明

### 2.1 Neuron 前端集成方式

Neuron 前端 UI 已完整集成在 ECP 前端项目 `emqx-bc-web` 中，包含 views、composables、API、types、i18n、router、styles、assets 共 228 个文件，覆盖 Neuron 2.1.0 ~ 2.4.0 四个版本。

ECP 前端通过 axios 请求拦截器将 Neuron API 调用重写为代理路径：

```typescript
// emqx-bc-web/src/utils/http/axios/index.ts:395-398
if (config.url.includes('/v2')) {
  config.url = `/api/edgeservice/proxy/${location.hash.split('/')[5]}${config.url}`
}
```

例如：`/v2/read` → `/api/edgeservice/proxy/{serviceId}/v2/read`

**ECP 后端仅代理 Neuron API 请求（`/v2/*`），不代理前端资源。**

### 2.2 直连模式请求链路

```
浏览器 → ECP 前端(Vue SPA, axios)
  → /api/edgeservice/proxy/{serviceId}/v2/read
  → ECP 后端 Gin Router
    → AuthRequired 中间件（验证用户 JWT + 查询用户信息）
    → EdgeServiceManagementSupported 中间件（查询 license）
    → Tunnel() 函数
      → 查询数据库获取边缘服务信息
      → 生成 Neuron JWT Token
      → 构造目标 URL: http://{endpoint}/v2/read
      → utils.Request()(共享 HTTP Client, MaxConnsPerHost=10, 超时 5s)
      → Neuron:7000 API
  → API 响应原路返回
```

### 2.3 每个代理请求的开销

每个 API 代理请求在 ECP 后端经过以下步骤：

| 步骤 | 操作 | 是否访问数据库 |
|------|------|---------------|
| 1 | `AuthRequired`：验证用户 JWT + `user.GetUserByUserID()` | ✅ 查用户表 |
| 2 | `EdgeServiceManagementSupported`：`license.Modules()` | ✅ 查 license |
| 3 | `Tunnel()`：`edgeservice.GetByServiceID()` | ✅ 查边缘服务表 |
| 4 | `Tunnel()`：`edgeservice.GetToken()` → `jwt.Get()` | ❌ 内存 map（偶发刷新时生成 token） |
| 5 | `utils.Request()`：HTTP 转发到 Neuron | ❌ |

**每个代理请求至少 3 次数据库查询。**

---

## 三、问题 1：ECP 跳转 Neuron 页面代理失败

### 3.1 现象描述

ECP 前端加载 Neuron 页面（Vue 组件）后，组件并发发起多个 API 请求（如 `/v2/node` 获取节点列表、`/v2/group` 获取分组信息等）。部分请求超时或失败，前端显示代理失败报错。

### 3.2 根因分析

#### 根因 1.1：5 秒硬编码超时（最核心问题）

**文件：** `server/proxy/edge_tunnel.go:80`

```go
timeout := 5 * time.Second
```

Neuron 页面加载时，Vue 组件并发发起多个 API 请求。每个请求独立受 5 秒超时约束。在 191 个边缘服务、70-80 个驱动的高负载下，Neuron 响应延迟容易突破 5 秒。

超时触发后，ECP 返回 HTTP 502（`httpresponse.BadGateway(c)`），前端显示代理失败。

#### 根因 1.2：共享 HTTP Client `MaxConnsPerHost: 10`

**文件：** `internal/pkg/utils/http.go:34`

```go
MaxConnsPerHost: 10,
```

全局单例 HTTP Client 被所有代理请求共享。191 个边缘服务中，同一时间可能有多个用户访问同一个 Neuron 节点。`MaxConnsPerHost: 10` 意味着对每个 Neuron 最多 10 个并发 HTTP 连接，超出的请求在 `net/http` 层排队等待，进一步消耗 5 秒超时预算。

#### 根因 1.3：每个请求 3 次数据库查询增加延迟

**文件：** `middlewares/auth.go`、`middlewares/module.go`、`server/proxy/edge_tunnel.go`

每次代理请求前需要：
1. 查用户表验证身份（`AuthRequired`）
2. 查 license 表确认模块可用（`EdgeServiceManagementSupported`）
3. 查边缘服务表获取 endpoint 信息（`Tunnel`）

在高并发下，数据库连接池（`MaxOpenConns=25`）成为瓶颈，查询排队增加延迟。

#### 根因 1.4：Neuron 无 HTTP 请求超时

**文件：** `neuron/plugins/restful/rest.c`

Neuron 的 NNG HTTP Server 没有配置任何请求超时。如果驱动处理缓慢或挂起，对应的 `nng_aio` 永远不会被 finish，HTTP 连接永久占用。在 70-80 个驱动的场景下，这会逐渐耗尽 NNG 的线程池（默认约 4-8 线程），导致新请求无法被处理。

### 3.3 解决方案

| 优先级 | 修复项 | 涉及文件 | 改动说明 |
|--------|--------|----------|----------|
| P0 | 增大代理超时 | `edge_tunnel.go` | 5s → 30s（页面加载类请求），写入类可单独设 15s |
| P0 | 增大 `MaxConnsPerHost` | `utils/http.go` | 10 → 100+ |
| P0 | 给共享 HTTP Client 添加整体超时 | `utils/http.go` | `http.Client{Timeout: 60 * time.Second}` |
| P1 | 缓存边缘服务信息 | `Tunnel()` 函数 | 对 `GetByServiceID()` 结果添加 Redis/内存缓存，减少 DB 查询 |
| P1 | Neuron 添加请求超时 | Neuron REST 插件 | 为 NNG aio 设置超时回调，超时后 finish aio 并返回错误 |

---

## 四、问题 2：Neuron 写入指令失败

### 4.1 现象描述

在 Neuron 数据监控页面对点位执行写入操作时，写指令执行失败。

### 4.2 根因分析

#### 根因 2.1：5 秒代理超时不足以覆盖写入链路

**文件：** `server/proxy/edge_tunnel.go:80`

直连模式下写入请求链路：

```
ECP 前端 → ECP 后端 Tunnel() → utils.Request() → Neuron:7000 /v2/write
  → NNG HTTP Handler (handle_write)
    → JWT 验证
    → JSON 解码
    → neu_plugin_op() → adapter_command()
      → send() 通过 UDP 发送到 127.0.0.1:7788 (Manager)
        → Manager 路由到目标驱动 adapter
          → adapter_loop() 处理
            → 驱动执行写入（Modbus/OPC-UA 等，涉及实际设备 I/O）
          → 响应通过 UDP 返回
      → Dashboard adapter 接收响应
    → neu_http_response(aio, ...) → NNG 返回 HTTP 响应
  → ECP 代理转发给前端
```

链路中任一环节延迟都可能导致 5 秒超时：
- ECP 侧：DB 查询 + 连接排队
- Neuron 侧：UDP 发送 + Manager 路由 + 驱动 I/O + 响应返回

#### 根因 2.2：Neuron `neu_plugin_op()` 返回 `NEU_ERR_IS_BUSY`

**文件：** `neuron/plugins/restful/rw_handle.c:128-134`、`neuron/src/adapter/adapter.c:498`

```c
// adapter.c - adapter_command()
ret = neu_send_msg(adapter->control_fd, msg);  // UDP send to 127.0.0.1:7788
if (0 != ret) {
    neu_msg_free(msg);
    return -1;  // → 触发 NEU_ERR_IS_BUSY
}
```

`neu_send_msg()` 在 UDP socket（`SOCK_NONBLOCK`）上发送 8 字节指针。当发送缓冲区满时（`EAGAIN/EWOULDBLOCK`），返回失败，`adapter_command()` 返回 -1，handler 返回 `NEU_ERR_IS_BUSY`（HTTP 500）。

#### 根因 2.3：Neuron 单线程事件循环 + 无请求超时

每个驱动 adapter 的 `adapter_loop()` 串行处理所有消息。一个慢速驱动（如 Modbus 设备超时 2 秒）会阻塞该 adapter 的所有后续请求。

更严重的是，Neuron HTTP Server 无请求超时：如果驱动挂起不响应，`nng_aio` 永远不会被 finish，HTTP 连接永久占用，逐渐耗尽 NNG 线程池。

#### 根因 2.4：JWT Token 错误被静默吞掉

**文件：** `internal/pkg/jwt/edge_service.go:33`

```go
tokenString, err = GenToken(60*60*24, useEcpNameAsJwtPubKeyFileName, serviceId)
if err != nil {
    return "", nil   // BUG: 应为 return "", err
}
```

当 `GenToken` 失败（如私钥文件缺失）时，错误被静默吞掉，返回空字符串。`GetToken()` 不检查空值，`Tunnel()` 将空 `Authorization: Bearer ` 头发送给 Neuron，Neuron 返回 401，前端显示写入失败。

### 4.3 解决方案

| 优先级 | 修复项 | 涉及文件 | 改动说明 |
|--------|--------|----------|----------|
| P0 | 增大代理超时 | `edge_tunnel.go` | 5s → 15-30s |
| P0 | 增大 `MaxConnsPerHost` | `utils/http.go` | 10 → 100+ |
| P0 | 修复 JWT 错误吞掉 bug | `jwt/edge_service.go:33` | `return "", nil` → `return "", err` |
| P0 | 添加写入重试机制 | `edge_tunnel.go` | 对 Neuron 返回的 `NEU_ERR_IS_BUSY` 实现指数退避重试（最多 3 次） |
| P1 | Neuron 添加 HTTP 请求超时 | Neuron REST 插件 | 为 aio 设置超时回调 |
| P1 | 缓存边缘服务信息 | `Tunnel()` | 减少每次请求的 DB 查询次数 |

---

## 五、问题 3：多页面切换卡顿

### 5.1 根因分析

#### 根因 3.1：共享 HTTP Client 连接池瓶颈

**文件：** `internal/pkg/utils/http.go`

```go
MaxIdleConns:    1024,
MaxConnsPerHost: 10,
IdleConnTimeout: 600 * time.Second,
```

191 个边缘服务，每个服务的页面切换需要多个并发 API 请求。`MaxConnsPerHost: 10` 导致连接排队，页面切换时等待连接释放，产生卡顿感。

#### 根因 3.2：每个请求 3 次 DB 查询 + 中间件开销

**文件：** `middlewares/auth.go`、`middlewares/module.go`

每个代理请求经过 `AuthRequired`（查用户表）+ `EdgeServiceManagementSupported`（查 license 表）+ `Tunnel`（查边缘服务表）。页面快速切换时大量并发 DB 查询争抢 25 个连接，查询延迟增大。

#### 根因 3.3：JWT Token Map 内存泄漏

**文件：** `internal/pkg/jwt/edge_service.go:20`

```go
var ServiceId2JwtMap = make(map[string]string, 1) //TODO 这个map如果不删除过期的token，可能会越来越大
```

Token 24 小时有效，提前 1 小时刷新，但旧 token 永不删除。长期运行后 map 膨胀，增加 GC 压力。

#### 根因 3.4：数据库连接池配置不当

**文件：** `dal/db.go:44-46`

```go
sqlDB.SetMaxOpenConns(25)
sqlDB.SetMaxIdleConns(25)
sqlDB.SetConnMaxLifetime(15 * time.Minute)
// 缺失: sqlDB.SetConnMaxIdleTime(...)
```

`MaxIdleConns == MaxOpenConns` 无优化空间；无 `ConnMaxIdleTime` 导致空闲连接长期占用资源。

### 5.2 解决方案

| 优先级 | 修复项                  | 涉及文件                  | 改动说明                                                                     |
| --- | -------------------- | --------------------- | ------------------------------------------------------------------------ |
| P0  | 增大 `MaxConnsPerHost` | `utils/http.go`       | 10 → 100+                                                                |
| P0  | 修复 JWT Token 内存泄漏    | `jwt/edge_service.go` | 使用 LRU Cache 或带 TTL 的 map，定期清理过期 token                                   |
| P1  | 优化数据库连接池             | `dal/db.go`           | `MaxIdleConns` 设为 `MaxOpenConns/2`，添加 `ConnMaxIdleTime(5 * time.Minute)` |
| P1  | 缓存边缘服务信息和 license 信息 | 中间件 + Tunnel          | 对高频查询添加 Redis/本地缓存，减少 DB 查询                                              |
| P2  | 数据库查询添加分页            | ECP 查询层               | 对边缘服务列表等查询添加分页                                                           |

---

## 六、问题 4：PostgreSQL 频繁重启

### 6.1 根因分析

#### 根因 4.1：数据库连接池配置不当

**文件：** `dal/db.go`

```go
sqlDB.SetMaxOpenConns(25)
sqlDB.SetMaxIdleConns(25)
sqlDB.SetConnMaxLifetime(15 * time.Minute)
```

- `MaxIdleConns == MaxOpenConns`：所有连接空闲时都保持打开，不释放 PostgreSQL 连接槽
- 无 `ConnMaxIdleTime`：空闲连接可能持有服务端资源
- 191 个边缘服务 + 5-6 个组织 × 多个项目，每个代理请求 3 次 DB 查询，25 个连接可能不足

#### 根因 4.2：每个代理请求 3 次 DB 查询放大数据库负载

以 191 个边缘服务、每个服务每秒 1 个 API 请求估算：
- 191 × 3 = 573 次/秒 DB 查询（仅代理路径）
- 加上健康检查、版本查询等后台任务，DB 压力更大

#### 根因 4.3：ECP 进程内存压力

JWT Token Map 无限增长 + 高并发下的 HTTP 连接和缓冲区，可能导致 ECP 进程内存使用率过高。OOM 时大量数据库连接同时断开又重建，对 PostgreSQL 造成冲击。

#### 根因 4.4：K8S 环境资源限制

客户使用 ZStack 管理的 K8S 部署，PostgreSQL Pod 的 CPU/内存 limits 可能设置不当，导致 OOM Kill。

### 6.2 解决方案

| 优先级 | 修复项                        | 改动说明                                                                            |
| --- | -------------------------- | ------------------------------------------------------------------------------- |
| P0  | 优化数据库连接池                   | `MaxIdleConns` 设为 10，`MaxOpenConns` 保持 25，添加 `ConnMaxIdleTime(5 * time.Minute)` |
| P0  | 排查 PostgreSQL Pod 的 OOM 设置 | 检查 K8S deployment 的 `resources.limits.memory`                                   |
| P1  | 缓存边缘服务信息                   | 减少每次代理请求的 DB 查询次数（从 3 次降至 1 次或 0 次）                                             |
| P1  | 修复 JWT Token 内存泄漏          | 同问题 3 解决方案                                                                      |
| P1  | 添加数据库慢查询监控                 | 检查是否有全表扫描或缺失索引的查询                                                               |

---

## 七、ECP 代理层代码缺陷汇总

除上述四个问题的根因外，代码审查还发现以下缺陷：

### 7.1 JWT 错误吞掉（严重）

**文件：** `internal/pkg/jwt/edge_service.go:33`

```go
tokenString, err = GenToken(...)
if err != nil {
    return "", nil   // BUG: 错误被吞掉
}
```

当 JWT 生成失败时，返回空字符串且无错误。`Tunnel()` 不检查空值，将空 Authorization 头发送给 Neuron，导致 401 错误被误报为代理失败。

### 7.2 响应头只取第一个值

**文件：** `server/proxy/edge_tunnel.go:105-107`

```go
for k, v := range res.Header {
    c.Writer.Header().Set(k, v[0])  // 丢弃多值头部
}
```

如果 Neuron 返回多个同名头部（如 `Set-Cookie`），只转发第一个。

### 7.3 响应流式传输后错误处理无效

**文件：** `server/proxy/edge_tunnel.go:109-115`

```go
c.Writer.WriteHeader(res.StatusCode)  // 状态码已提交
_, err = io.Copy(c.Writer, res.Body)
if err != nil {
    httpresponse.Fail(c, ...)  // 无法再修改状态码
}
```

`WriteHeader` 调用后状态码已发送给客户端，后续的 `Fail()` 无效。

### 7.4 `fabricHttpProxy` 无请求超时

**文件：** `logic/proxy/http_proxy.go:71`

```go
cli := &http.Client{} // Timeout = 0
```

虽然客户使用直连模式不经过此路径，但 Dashboard 隧道使用此无超时 Client，可能间接占用系统资源。

---

## 八、Neuron 侧问题汇总

### 8.1 NNG HTTP Server 无请求超时

Neuron 的 REST 插件没有配置任何请求超时。如果驱动挂起不响应，HTTP 连接永久占用，逐渐耗尽 NNG 线程池（默认约 4-8 线程）。

### 8.2 单线程 adapter 事件循环

每个驱动 adapter 串行处理所有消息（读、写、配置变更）。慢速驱动阻塞该 adapter 的所有后续请求。

### 8.3 UDP 通信不可靠

Adapter 间通信基于 `SOCK_NONBLOCK` UDP socket。高负载下 `send()` 返回 `EAGAIN`，消息被丢弃，触发 `NEU_ERR_IS_BUSY`。

### 8.4 `listen(fd, 1)` backlog 过小

**文件：** `neuron/src/connection/connection.c:730`

```c
ret = listen(fd, 1);
```

此为驱动 TCP Server（如 Modbus TCP）的连接队列，非 REST API。backlog=1 导致快速重连场景下连接被拒绝。注意：这是驱动层问题，不影响 ECP 代理的 REST API 调用。

---

## 九、整体改进建议

### 9.1 短期（P0，不改架构，可滚动更新）

```
1. 增大代理超时：5s → 30s（edge_tunnel.go:80）
2. 增大 MaxConnsPerHost：10 → 100+（utils/http.go:34）
3. 修复 JWT 错误吞掉 bug（jwt/edge_service.go:33）
4. 修复 JWT Token Map 内存泄漏（jwt/edge_service.go:20）
5. 给共享 HTTP Client 添加整体超时（utils/http.go）
6. 优化数据库连接池（dal/db.go）
```

### 9.2 中期（P1，架构优化）

```
1. 添加写入重试机制（指数退避，最多 3 次）
2. 缓存边缘服务信息（Redis/本地缓存），减少每次请求的 DB 查询
3. Neuron 添加 HTTP 请求超时（NNG aio 超时回调）
4. 添加代理层 metrics 监控（延迟、错误率、连接数）
```

### 9.3 长期（P2，架构演进）

```
1. Neuron 通信从 UDP 改为本地 TCP/Unix Socket
2. Neuron adapter 事件循环支持多线程写入
3. 数据库读写分离，引入 Redis 缓存层
4. 评估 Neuron NNG HTTP Server 的线程池大小配置
```

---

## 十、风险提示

| 风险 | 说明 | 建议 |
|------|------|------|
| 生产环境升级风险 | 客户要求支持回滚 | 蓝绿部署或灰度发布，先在测试环境验证 |
| 超时参数调整 | 增大超时可能掩盖深层问题 | 配合监控使用，设置合理的上限 |
| Neuron 请求超时改造 | 需修改 Neuron C 代码 | 评估是否可通过升级 Neuron 版本获得 |
| 数据库连接池调整 | 可能影响其他模块 | 调整后观察整体系统稳定性 |

---

## 十一、结论

客户反馈的四个问题**根因主要在 ECP 代理层**，核心矛盾是：

1. **代理超时过短**（5 秒硬编码）无法覆盖高负载下的 API 调用耗时
2. **连接池瓶颈**（`MaxConnsPerHost: 10`）导致 191 个边缘服务的请求排队
3. **每个代理请求 3 次 DB 查询**放大数据库压力，间接导致 PostgreSQL 频繁重启
4. **JWT 错误吞掉 bug** 可能导致静默的认证失败

Neuron 侧的问题（无请求超时、单线程事件循环、UDP 不可靠）在当前部署规模下被放大，但**主要矛盾在 ECP 侧**。

建议**优先修复 ECP 侧 P0 级问题**（超时、连接池、JWT bug、Token 泄漏），这些改动量小、风险低、效果显著，可通过 K8S 滚动更新完成，无需停产。
