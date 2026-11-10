# Sentinel 拓扑感知与故障切换连接管理设计

- 日期：2026-11-10
- 状态：已确认方案，待书面评审
- 项目：valkey-sentinel-proxy
- 选定方案：Sentinel 事件驱动 + 定时校准 + 会话主动断开

## 1. 背景与问题证据

当前代理对每个新下游连接执行一次 `SENTINEL GET-MASTER-ADDR-BY-NAME`，然后直接连接返回地址。连接建立后只做双向 `io.Copy`，不再检查目标角色，也不跟踪 Sentinel 拓扑变化。

由此存在两类问题：

1. **新连接接入从节点**：Sentinel 返回尚未收敛的旧地址，或者地址返回后目标角色发生变化，代理没有执行 `ROLE` 校验，直接接受该目标。
2. **已有连接残留在降级节点**：连接建立时目标可能是主节点，故障切换后目标被降级，但代理没有主动关闭旧会话的机制。

真实 Valkey 8 与三个 Sentinel 的受控实验已经确认第一类路径：真实故障切换后 A 为 `slave`、B 为 `master`，当一个可访问的 Sentinel 仍返回 A 时，当前代理会让新客户端连接到 A，客户端通过代理执行 `ROLE` 得到 `slave`。

当前生产入口没有启用 valkey-go 的 `ReplicaOnly`、`SendToReplicas` 或高层 Sentinel 数据客户端。数据连接地址只来自本次 Sentinel 响应，不是 valkey-go 主动进行读写分离。

## 2. 设计目标

1. 已知角色为 `slave` 的候选节点不得用于新的转发会话。
2. 观察到主节点变化后，主动关闭连接旧地址的会话，促使客户端重连。
3. 正常情况下通过 Sentinel Pub/Sub 及时发现变化。
4. Pub/Sub 消息丢失或订阅重连时，通过主动重新解析和定时校准恢复。
5. 避免拓扑事件与连接建立之间的竞态导致旧地址会话漏关。
6. Sentinel 控制面临时故障时，不批量断开仍有可能是正常的现有会话。
7. 保持连接建立后的透明 TCP 转发，不迁移连接状态、不重放客户端命令。
8. 增加足够日志，区分 Sentinel 陈旧地址、角色校验失败、拓扑切换和控制面故障。

## 3. 非目标与限制

1. 不保证脑裂时只有一个报告 `ROLE=master` 的节点。
2. 不保证故障切换期间不丢写、不重复写或命令 exactly-once。
3. 不透明迁移 `AUTH`、`SELECT`、事务、Pub/Sub 等连接状态。
4. 不实现 RESP 命令级路由、读写分离或写命令自动重放。
5. 不用代理替代 fencing、多数派部署、同步共识或 `min-replicas-to-write` 等部署措施。
6. `ROLE` 只能识别已经降级的节点；如果旧节点仍报告 `master`，需要部署层面的脑裂防护。
7. 校验与实际拨号之间仍存在角色变化窗口，必须通过拓扑监听、会话关闭和 Sentinel 自身的 `CLIENT KILL` 共同收敛，不能声称彻底消除竞态。

## 4. 方案比较

### 4.1 方案 A：仅定时巡检

新连接执行 `ROLE`，后台定时重新解析并关闭旧会话。

优点是改动较少；缺点是旧连接需要等待一个巡检周期，缺少事件驱动的及时性。

### 4.2 方案 B：事件驱动 + 定时校准（选定）

新连接执行 `ROLE`，订阅 `+switch-master`，并保留定时校准。主节点变化后关闭旧会话。

优点是能及时发现变化，同时能在消息丢失后恢复；代价是需要拓扑管理、订阅重连和会话注册表。

### 4.3 方案 C：RESP 感知代理

解析命令、识别 `READONLY` 并跟踪连接状态。

该方案复杂度高，命令重放可能导致重复写入，事务和订阅状态难以安全迁移，首版不采用。

## 5. 总体架构

```text
cmd/main.go
    │ 配置 Sentinel、数据节点认证和监听地址
    ▼
valkeyProxyServer
    ├── TopologyResolver    查询 Sentinel、校验候选主节点
    ├── TopologyWatcher     订阅变化、断线重连、定时校准
    ├── SessionManager      保存当前拓扑版本、解析序号和活动会话
    └── Session             上下游连接、双向转发、幂等关闭

客户端新连接 ──► Resolver 解析并验证 ──► 建立上游连接 ──► 原子注册会话
Sentinel 事件 ──► 重新解析并验证 ──────► 更新拓扑 ──────► 关闭旧地址会话
```

建议将实现拆分为：

- `proxy/server.go`：监听、连接处理和透明转发。
- `proxy/topology.go`：Sentinel 查询、候选选择、订阅和校准。
- `proxy/validator.go`：数据节点 `ROLE` 校验。
- `proxy/session.go`：会话、拓扑版本和会话注册表。
- `cmd/main.go`：命令行配置和生命周期装配。
- 对应的 `_test.go` 文件及 `README.md` 配置说明。

## 6. 配置设计

### 6.1 Sentinel 地址

保留 `-sentinel-addr` 参数名，支持逗号分隔多个地址：

```bash
-sentinel-addr "sentinel-1:26379,sentinel-2:26379,sentinel-3:26379"
```

现有单地址写法保持兼容。解析配置时去除空白，空地址视为配置错误。

每个 Sentinel 使用独立控制客户端，并设置：

```go
ForceSingleClient = true
```

这些控制客户端不设置 `Sentinel.MasterSet`，确保命令发往 Sentinel 本身，而不是 valkey-go 高层 Sentinel 客户端所发现的数据节点。查询和阻塞式 Pub/Sub 使用不同连接。

### 6.2 Sentinel 认证

沿用现有参数：

```bash
-sentinel-user ""
-sentinel-pass ""
```

这些凭据只用于 Sentinel 控制连接。

### 6.3 数据节点认证

新增：

```bash
-node-user ""
-node-pass ""
```

规则：

- 数据节点无认证时保持为空。
- 数据节点与 Sentinel 使用相同凭据时，两处显式配置相同值。
- 数据节点使用独立凭据时，只将数据节点凭据填入 `node-user/node-pass`。
- 不默认把 Sentinel 凭据发送给数据节点。
- `NOAUTH`、`WRONGPASS` 和 ACL 权限不足必须返回清晰错误，日志不得包含密码。

### 6.4 校准周期

新增：

```bash
-topology-refresh-interval 5s
```

该值必须大于零。订阅重连使用内部有界退避，建议从 100ms 开始，最大 5s。单次 Sentinel 查询和 `ROLE` 校验必须使用有界上下文或连接超时，首版不单独增加更多超时参数。

## 7. 组件设计

### 7.1 PrimaryValidator

输入候选数据节点地址，输出该节点是否明确为主节点。

首版使用短生命周期的 valkey-go 单节点客户端：

1. 将 `InitAddress` 设置为候选地址。
2. 设置 `ForceSingleClient=true`，清空所有 Sentinel 路由配置。
3. 使用独立的数据节点认证配置。
4. 执行 `ROLE` 并检查响应第一项是否为 `master`。
5. 无论成功或失败都关闭校验客户端。

结果必须区分：

- **ConfirmedMaster**：明确返回 `master`。
- **ConfirmedNotMaster**：明确返回 `slave`、`sentinel` 或其他非主角色。
- **Inconclusive**：网络错误、超时、认证失败、协议错误或响应不完整。

只有 `ConfirmedMaster` 允许建立新会话。只有当前上游被明确判定为 `ConfirmedNotMaster` 时才据此关闭旧会话；`Inconclusive` 不得触发批量断连。

该设计会增加一次短暂的数据节点连接，换取实现简单和 RESP 处理可靠。首版不手写 RESP 编解码，也不复用 valkey-go 内部连接。

### 7.2 TopologyResolver

负责查询 Sentinel 并验证候选地址：

1. 从上次成功的 Sentinel 开始，按配置顺序遍历。
2. 执行 `SENTINEL GET-MASTER-ADDR-BY-NAME <master-name>`。
3. 无效、空或格式错误的响应按该 Sentinel 失败处理。
4. 对返回的候选地址调用 `PrimaryValidator`。
5. `ConfirmedMaster` 时返回候选地址和本次解析序号。
6. `ConfirmedNotMaster` 时记录候选角色；若候选是当前主节点，则通知状态层进行失效处理，同时继续尝试其他 Sentinel。
7. `Inconclusive` 时继续尝试其他 Sentinel。
8. 所有 Sentinel 都没有给出已验证主节点时返回错误；新连接失败，但仅凭 `Inconclusive` 不关闭旧会话。

每次解析开始时获得单调递增的解析序号。状态层只应用不落后于已成功应用序号的结果，防止较慢的旧解析覆盖较新的有效结果。较新的失败结果属于不确定信息，不阻止较早但有效的结果应用。

### 7.3 TopologyWatcher

每个 Sentinel 使用独立订阅连接监听：

```text
SUBSCRIBE +switch-master
```

处理规则：

- 事件只作为触发信号，不直接把事件中的新地址当作已验证事实。
- 收到事件后，通过容量为 1 的触发队列请求重新解析，合并事件风暴。
- 同一时间最多执行一个后台校准；新连接自己的解析可以并发执行。
- 订阅断开后按有界退避重连，重连成功后立即请求校准。
- 每隔 `topology-refresh-interval` 请求一次校准。
- 校准成功后，将已验证主节点发布给 `SessionManager`。
- 校准失败时保留最后状态，除非有候选节点被明确判定为 `ConfirmedNotMaster`。

### 7.4 SessionManager

`SessionManager` 在同一把短临界区互斥锁下维护：

- 当前主节点地址。
- 当前主节点是否有效。
- 拓扑 generation。
- 最后应用的解析序号。
- 活动会话表。

拓扑 generation 在以下情况递增：

1. 从无有效主节点变为有已验证主节点。
2. 已验证主节点地址发生变化。
3. 当前主节点被明确判定为非主节点，状态变为无效。

重复发布相同且仍有效的地址不递增 generation。

核心操作：

- `PublishPrimary(addr, resolutionID)`：忽略过期解析；应用有效主节点并收集需要关闭的旧地址会话。
- `InvalidatePrimary(addr, resolutionID)`：仅在结果不过期且地址对应当前主节点时，将状态标为无效并收集该地址会话。
- `RegisterSession(session, addr, generation)`：仅当主节点仍有效、地址和 generation 都一致时注册，否则拒绝。
- `RemoveSession(session)`：连接结束时幂等移除。
- `CloseAll()`：服务关闭时取出全部会话。

网络连接的实际关闭在锁外执行，避免持锁等待 I/O。被收集关闭的会话先从活动表中移除，再通过幂等 `Close` 同时关闭上下游连接。

## 8. 关键连接时序

### 8.1 新连接

```text
AcceptTCP
    │
    ▼
启动独立 handleConnection
    │
    ▼
Resolver 获取已验证候选地址及解析序号
    │
    ▼
PublishPrimary，得到 primary + generation 快照
    │
    ▼
建立原始上游 TCP 连接
    │
    ▼
RegisterSession 原子检查 primary + generation
    │ 不一致：关闭上下游，等待客户端重连
    │ 一致
    ▼
开始双向转发
```

`handleConnection` 独立运行，避免单个慢 Sentinel 阻塞监听循环。解析锁和会话状态锁不得包裹网络 I/O。

注册前的原子检查用于解决以下竞态：

```text
新连接已解析 A，但尚未注册
后台确认 A → B，并关闭当前所有 A 会话
新连接随后尝试注册 A
RegisterSession 因 generation 不匹配而拒绝
```

如果拓扑变化发生在注册之后，后台更新会在活动表中找到并关闭该会话。

### 8.2 已有连接

确认主节点从 A 变为 B 后：

1. 原子更新主节点地址和 generation。
2. 从活动表中移除所有上游为 A 的会话。
3. 在锁外同时关闭这些会话的上游和下游连接。
4. 客户端重连后走新连接流程并接入 B。
5. 不重放原连接中的任何命令。

如果明确确认当前上游 A 已是 `slave`，但还没有找到新主节点：

1. 将当前主节点状态标为无效并增加 generation。
2. 关闭 A 上的会话。
3. 新连接暂时失败，直到发现已验证主节点。

如果只是 Sentinel 超时、网络失败或认证错误，不执行上述失效处理。

### 8.3 转发结束与服务关闭

每个 Session 的关闭必须幂等。任意复制方向结束、拓扑失效或服务关闭都应最终关闭上下游连接，使两个复制 goroutine 退出并移除会话。

`valkeyProxyServer.Close` 需要停止 watcher、关闭 listener、关闭 Sentinel 控制客户端并关闭全部活动 Session，防止新增后台 goroutine 泄漏。

## 9. 异常处理矩阵

| 情况 | 新连接 | 已有连接 |
|---|---|---|
| 某个 Sentinel 不可达 | 尝试下一个 | 不受影响 |
| 所有 Sentinel 暂时不可达 | 关闭下游并记录错误 | 保留 |
| Sentinel 不认识 master-set | 尝试下一个，全部失败则拒绝 | 保留 |
| 候选节点明确为 `slave` | 拒绝并尝试其他 Sentinel | 若为当前上游则关闭相关会话 |
| 候选节点认证失败 | 拒绝并提示检查数据节点认证 | 不批量断开 |
| 候选节点网络超时 | 尝试其他 Sentinel | 不据此推断角色 |
| 确认主节点 A → B | 使用 B | 关闭 A 上旧会话 |
| 实际上游拨号失败 | 关闭本次下游，由客户端重连 | 不影响其他会话 |
| Pub/Sub 断开 | 不影响主动解析 | 后台重连并立即校准 |
| Pub/Sub 消息丢失 | 不受影响 | 定时校准兜底 |
| 过期解析结果晚到 | 不发布，当前连接失败并重试 | 不覆盖新拓扑 |

只有明确证据触发断连；不确定错误不得造成连接风暴。

## 10. 日志与可观测性

首版不引入 metrics 依赖，增加以下关键日志：

- 实际使用的 Sentinel 地址。
- 主节点解析结果、解析序号和 generation。
- 候选节点因 `ROLE != master` 被拒绝。
- 已验证主节点从旧地址切换到新地址。
- 当前主节点失效以及因此关闭的会话数量。
- Sentinel 订阅断开、重连和校准失败。
- 数据节点认证失败。

日志不得记录 Sentinel 或数据节点密码。

## 11. 测试设计

### 11.1 单元测试

#### 候选节点选择

- 第一个 Sentinel 返回 `slave`，第二个返回 `master`，最终选择后者。
- 所有候选均为 `slave` 时返回错误。
- 空响应、非法响应和非法地址得到明确处理。
- 较慢的旧解析不能覆盖较新的有效结果。

#### 认证与角色校验

- 无认证数据节点正常校验。
- `node-user/node-pass` 正确传递。
- `NOAUTH`、`WRONGPASS`、ACL 权限不足返回明确错误。
- `master`、`slave`、异常响应分别映射为 ConfirmedMaster、ConfirmedNotMaster、Inconclusive。

#### 会话和并发

- 拨号后 generation 已变化时，注册失败且连接关闭。
- generation 不变时正常注册。
- A → B 只关闭 A 的会话。
- 重复发布相同有效地址不增加 generation、不重复断连。
- Sentinel 查询超时保留旧会话。
- 当前上游明确变为 `slave` 时，即使没有新主节点也关闭对应会话。
- 过期解析结果不改变状态。
- Session 重复关闭不会 panic 或误删其他会话。

#### 拓扑监听

- 多事件合并，不形成并发后台校准风暴。
- 订阅重连后立即校准。
- 无事件时定时校准仍能发现变化。
- Close 后 watcher、校准和会话 goroutine 均退出。

### 11.2 真实 Valkey 集成测试

1. **新连接保护**：A 已降级、B 已提升，S1 返回 A、S2 返回 B；代理不得把新连接接入 A，最终应接入 B。
2. **已有连接保护**：客户端保持通过代理到 A 的连接，真实故障切换到 B；旧连接被关闭，重连后目标为 B 且 `ROLE=master`。
3. **无替换主节点时的失效处理**：当前上游明确变成 `slave`，即使新主节点暂不可用，旧会话仍被关闭。
4. **控制面故障**：Sentinel 暂时不可达时保留已有会话，恢复后自动校准。
5. **消息丢失兜底**：不能依赖 Pub/Sub 时，定时校准仍能关闭旧会话。
6. **并发切换**：故障切换期间持续建立和关闭连接，检查 race、会话残留和 goroutine 泄漏。

集成测试需要真实容器环境；不具备该环境时明确跳过，不把跳过报告为验证通过。单元测试不得因为无法连接本机 Sentinel 而笼统跳过核心逻辑。

### 11.3 验证命令

```bash
go test -race ./...
go vet ./...
```

保留并运行现有转发测试，确认透明 TCP 数据路径没有回归。

## 12. 验收标准

1. 已知为 `slave` 的候选节点不会用于新转发会话。
2. 主节点变化被观察后，旧地址会话会被主动关闭。
3. Pub/Sub 丢失时，定时校准能够完成同样的拓扑收敛。
4. Sentinel 暂时故障不会导致所有现有连接被批量断开。
5. 拓扑更新与会话注册之间不存在漏关旧地址会话的竞态。
6. 不迁移连接状态，不透明重放客户端命令。
7. 正常连接建立后的透明 TCP 转发保持不变。
8. 关闭服务后，新增 watcher 和 Session 相关资源均能释放。

## 13. 文档与发布说明

`README.md` 需要补充：

- 多 Sentinel 地址配置。
- Sentinel 与数据节点认证的区别。
- `-topology-refresh-interval` 的用途。
- 故障切换时旧会话会主动断开，客户端必须启用自动重连。
- `ROLE` 校验会增加一次短连接。
- 脑裂、异步复制和 fencing 的限制。

发布说明需要提示：启用认证的数据节点必须配置 `node-user/node-pass`，否则新连接的 `ROLE` 校验会失败。
