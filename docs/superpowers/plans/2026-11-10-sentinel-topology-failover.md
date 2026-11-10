# Sentinel Topology Failover Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 valkey-sentinel-proxy 增加新连接主节点角色校验、Sentinel 拓扑监听和旧会话主动关闭能力。

**Architecture:** 使用独立 Sentinel 控制客户端解析候选地址，通过短生命周期数据节点客户端执行 `ROLE`。`SessionManager` 原子维护拓扑 generation、解析序号和活动会话；watcher 以 `+switch-master` 驱动校准，并以 5 秒定时校准兜底。

**Tech Stack:** Go 1.25、valkey-go v1.0.50、Go `net`/`context`/`sync`、Podman + Valkey 8（真实集成验证）。

**Spec:** `docs/superpowers/specs/2026-11-10-sentinel-topology-failover-design.md`

## Global Constraints

- 不新增生产依赖；测试优先使用接口 fake 和本地 RESP2 server，不引入 gomock。
- 不设置高层 `valkey.SentinelOption.MasterSet`；Sentinel 控制客户端和 validator 都使用 `ForceSingleClient=true`。
- 不默认复用 Sentinel 凭据作为数据节点凭据；不记录任何密码。
- 不解析或重放客户端命令，不迁移已有连接状态。
- 只有 `ConfirmedMaster` 允许新会话；只有明确的 `ConfirmedNotMaster` 才能据角色关闭旧会话。
- 所有解析、校验、订阅重连和上游拨号使用有界超时；已建立的 TCP 转发不设置人为空闲超时；锁内不得执行网络 I/O 或连接关闭。
- 按用户新增的命名要求统一使用 `NewValkeyProxyServer` 和 `valkeyProxyServer`，不保留旧 Redis 命名的 Go API；构造函数通过 variadic options 扩展。
- 未经用户逐次明确确认，不执行 `git commit`、`git push`、`git reset`、`git rebase` 或其他 git 变更。

## Review Focus

1. **IPv6 或主机名形式的 Sentinel 响应**：必须生成可拨号地址，并避免 validator 与实际拨号之间重复 DNS 解析；由 Task 3 的 `TestParseMasterAddress` 覆盖。
2. **包含空项的多 Sentinel 配置**：`s1:26379,,s2:26379` 必须报配置错误而不是静默忽略；由 Task 6 的 `TestParseSentinelAddrs` 覆盖。
3. **重复拓扑事件发布同一主节点**：不得增加 generation 或断开健康会话；由 Task 1 的 `TestSessionManagerPublishPrimary` 覆盖。
4. **下游半关闭写方向后仍等待响应**：必须保留现有 TCP 半关闭语义；由 Task 5 的 `TestProxyHalfClose` 覆盖。
5. **服务关闭时解析或订阅正阻塞**：必须取消并释放 goroutine；由 Task 4 的 `TestTopologyWatcherClose` 和 Task 5 的 `TestServerCloseUnblocksHandler` 覆盖。

---

### Task 1: Session 与拓扑状态的原子管理

**Files:**
- Create: `proxy/session.go`
- Create: `proxy/session_test.go`

**Interfaces:**
- Consumes: Go `net.Conn`。
- Produces:
  ```go
  type primarySnapshot struct { addr string; generation uint64 }
  type resolution struct { addr string; id uint64 }

  type topologyState interface {
      PublishPrimary(addr string, resolutionID uint64) (primarySnapshot, error)
      InvalidatePrimary(addr string, resolutionID uint64) error
  }

  func newSession(downstream, upstream net.Conn, upstreamAddr string) *session
  func (s *session) Close() error

  func newSessionManager() *sessionManager
  func (m *sessionManager) PublishPrimary(string, uint64) (primarySnapshot, error)
  func (m *sessionManager) InvalidatePrimary(string, uint64) error
  func (m *sessionManager) RegisterSession(*session, string, uint64) error
  func (m *sessionManager) RemoveSession(*session)
  func (m *sessionManager) CloseAll()
  ```
- Errors: `errStaleResolution`、`errTopologyChanged`、`errServerClosed`。

- [ ] **Step 1: 写失败测试**

  在 `proxy/session_test.go` 写以下测试，使用 `net.Pipe` 创建真实可关闭连接：
  - `TestSessionManagerPublishPrimary`：首次发布 A 得到 generation 1；注册两个 A 会话后重复发布 A 不增加 generation、不关闭会话；发布 B 增加 generation 并关闭两个 A 会话；随后注册 B 会话，重复发布 B 不关闭它，再发布 A 时只关闭该 B 会话。
  - `TestSessionManagerInvalidatePrimary`：当前 A 明确失效后 generation 增加、A 会话关闭；随后无有效主节点时注册失败。
  - `TestSessionManagerRejectsStaleRegistration`：拨号前后 generation 不同或地址不同返回 `errTopologyChanged`。
  - `TestSessionManagerIgnoresLateResolution`：应用 resolution 2 的 B 后，resolution 1 的 A 返回 `errStaleResolution` 且不改变状态；相同 resolution 的重复有效发布仍允许。
  - `TestSessionManagerCloseAll`：全部连接关闭，后续注册返回 `errServerClosed`；重复 `CloseAll` 安全。

- [ ] **Step 2: 验证测试失败**

  Run: `go test ./proxy -run 'TestSessionManager'`
  Expected: FAIL，类型和方法尚未定义。

- [ ] **Step 3: 实现 `proxy/session.go`**

  `sessionManager` 用同一互斥锁保护 `primary`、`valid`、`generation`、`lastResolutionID`、`sessions` 和 `closed`。每次接受的 `PublishPrimary` 即使地址不变也推进 `lastResolutionID`，但只在无效→有效或地址变化时推进 generation。

  `InvalidatePrimary` 只处理对应当前主节点且不过期的结果。`PublishPrimary`/`InvalidatePrimary` 在锁内移除目标会话，锁外关闭。`RegisterSession` 在同一临界区检查 `closed/valid/addr/generation` 后插入。`session.Close` 用 `sync.Once` 同时关闭上下游并用 `errors.Join` 返回错误。

- [ ] **Step 4: 验证测试通过**

  Run: `go test -race ./proxy -run 'TestSessionManager|TestSession'`
  Expected: PASS，无 race。

- [ ] **Step 5: 可选提交检查点**

  仅在用户明确确认后执行：`git add proxy/session.go proxy/session_test.go && git commit -m "feat: track sentinel topology sessions"`。

### Task 2: 数据节点 ROLE Validator

**Files:**
- Create: `proxy/validator.go`
- Create: `proxy/validator_test.go`
- Create: `proxy/resp_test.go`

**Interfaces:**
- Consumes: valkey-go v1.0.50。
- Produces:
  ```go
  type roleValidator interface {
      Validate(ctx context.Context, addr string) (role string, err error)
  }
  func newPrimaryValidator(option valkey.ClientOption) *primaryValidator
  func (v *primaryValidator) Validate(context.Context, string) (string, error)
  ```

- [ ] **Step 1: 写 RESP2 测试服务器和失败测试**

  `proxy/resp_test.go` 提供测试专用 RESP array 请求解析器及单连接 server，支持 `AUTH`、`ROLE` 和预设错误；不实现生产 RESP 编解码。

  `proxy/validator_test.go` 写：
  - `TestPrimaryValidatorMaster`：`ROLE` 返回 `*3 [master,0,[]]`，结果为 `"master", nil`。
  - `TestPrimaryValidatorNotMaster`：返回 `slave` 或 `sentinel`，结果为实际角色且无网络错误。
  - `TestPrimaryValidatorAuth`：断言收到预期用户名/密码；`NOAUTH`、`WRONGPASS` 和 ACL 错误返回非 nil error，error 字符串不含密码。
  - `TestPrimaryValidatorMalformedRole`：空数组或首项非字符串返回 error。
  - `TestPrimaryValidatorUnreachable`：未监听地址在短 context/超时内返回 error。

- [ ] **Step 2: 验证测试失败**

  Run: `go test ./proxy -run 'TestPrimaryValidator'`
  Expected: FAIL，validator 尚未定义。

- [ ] **Step 3: 实现 `proxy/validator.go`**

  `Validate` 拷贝 option，设置单一候选 `InitAddress`、`ForceSingleClient=true`、`AlwaysRESP2=true`、`DisableCache=true`、`ClientSetInfo=valkey.DisableClientSetInfo`、空 `Sentinel` 和 5 秒 `Dialer.Timeout`。用 `valkey.NewClient` 建短连接，defer `Close`，通过 `client.B().Role().Build()` 执行 `ROLE`，`ToArray` 后读取首项 `ToString`。

  每次 `Do` 使用最多 5 秒的子 context。角色字符串原样返回；所有构造、网络、认证、协议及空响应问题返回 error。

- [ ] **Step 4: 验证测试通过**

  Run: `go test -race ./proxy -run 'TestPrimaryValidator'`
  Expected: PASS。

- [ ] **Step 5: 可选提交检查点**

  仅在用户明确确认后执行：`git add proxy/validator.go proxy/validator_test.go proxy/resp_test.go && git commit -m "feat: validate primary role before proxying"`。

### Task 3: Sentinel 控制客户端、Resolver 与 Reconciler

**Files:**
- Create: `proxy/sentinel.go`
- Create: `proxy/sentinel_test.go`
- Create: `proxy/topology.go`
- Create: `proxy/topology_test.go`

**Interfaces:**
- Consumes: Task 1 的 `topologyState`/`primarySnapshot`/`resolution`，Task 2 的 `roleValidator`。
- Produces:
  ```go
  type sentinelControl interface {
      MasterAddr(context.Context, string) (string, error)
      Close()
  }
  func newValkeySentinelControl(addr string, option valkey.ClientOption) sentinelControl
  func parseMasterAddress(parts []string) (string, error)

  func newTopologyResolver(masterName string, controls []sentinelControl, validator roleValidator, state topologyState) *topologyResolver
  func (r *topologyResolver) Resolve(context.Context) (resolution, error)

  type topologyReconciler interface {
      Reconcile(context.Context) (primarySnapshot, error)
  }
  func newTopologyController(resolver *topologyResolver, state topologyState) *topologyController
  func (c *topologyController) Reconcile(context.Context) (primarySnapshot, error)
  ```

- [ ] **Step 1: 写失败测试**

  - `TestParseMasterAddress`：正常 IPv4、主机名、IPv6、缺少端口、非法端口和空数组；IPv6 必须使用 `net.JoinHostPort`，主机名只解析一次并返回解析后的 `TCPAddr.String()`。
  - `TestTopologyResolverSkipsReplica`：S1 返回 A 且 validator 为 `slave`，S2 返回 B 且为 `master`；最终 `resolution.addr==B`，并调用 A 的失效处理。
  - `TestTopologyResolverInconclusiveDoesNotInvalidate`：认证/网络错误继续下一个 Sentinel，但不调用 `InvalidatePrimary`。
  - `TestTopologyResolverNoMaster`：全部失败时返回包含 master-name 的 error。
  - `TestTopologyControllerPublishesResolution`：成功解析后调用 `PublishPrimary` 并原样返回 snapshot。

- [ ] **Step 2: 验证测试失败**

  Run: `go test ./proxy -run 'TestParseMasterAddress|TestTopology'`
  Expected: FAIL，相关类型尚未定义。

- [ ] **Step 3: 实现 Sentinel 控制客户端**

  `newValkeySentinelControl` 返回懒连接 wrapper，构造时不进行网络拨号。首次查询才创建 valkey client；创建失败保留下次重试机会，`Close` 幂等。网络客户端在锁外创建，并发创建时只保留一个，多余客户端立即关闭。option 必须单地址、`ForceSingleClient=true`、`AlwaysRESP2=true`、`DisableCache=true`、`ClientSetInfo=valkey.DisableClientSetInfo`、空 `Sentinel`，保留 Sentinel 用户名密码和 5 秒 Dialer.Timeout。

  `MasterAddr` 用 5 秒子 context 执行 Arbitrary `SENTINEL GET-MASTER-ADDR-BY-NAME`，`AsStrSlice` 后交给 `parseMasterAddress`。后者用 `net.JoinHostPort` + `net.ResolveTCPAddr` 返回规范地址。

- [ ] **Step 4: 实现 Resolver 与 Controller**

  `topologyResolver` 用 `atomic.Uint64` 在每次 `Resolve` 开始生成 resolution ID，并从上一次成功索引遍历 controls。每次成功得到 `ConfirmedMaster` 后更新首选索引并返回。非 master 角色调用 `InvalidatePrimary(candidate, id)`；stale resolution 原样返回，其他错误加入 `errors.Join`。

  `Reconcile` 调用 `Resolve`，成功后调用 `PublishPrimary(res.addr, res.id)`；解析失败不发布。

- [ ] **Step 5: 验证测试通过**

  Run: `go test -race ./proxy -run 'TestParseMasterAddress|TestTopology'`
  Expected: PASS。

- [ ] **Step 6: 可选提交检查点**

  仅在用户明确确认后执行：`git add proxy/sentinel.go proxy/sentinel_test.go proxy/topology.go proxy/topology_test.go && git commit -m "feat: resolve validated primary through sentinels"`。

### Task 4: Sentinel Switch-Master Watcher

**Files:**
- Create: `proxy/watcher.go`
- Create: `proxy/watcher_test.go`

**Interfaces:**
- Consumes: Task 3 的 `topologyReconciler`。
- Produces:
  ```go
  type switchMasterSubscriber interface {
      Watch(ctx context.Context, onEvent func()) error
      Close()
  }
  type subscriberFactory func() (switchMasterSubscriber, error)

  func newValkeySubscriberFactory(addr string, option valkey.ClientOption) subscriberFactory
  func newTopologyWatcher(reconciler topologyReconciler, factories []subscriberFactory, interval time.Duration) *topologyWatcher
  func (w *topologyWatcher) Start(context.Context)
  func (w *topologyWatcher) Close()
  ```

- [ ] **Step 1: 写失败测试**

  - `TestTopologyWatcherCoalescesEvents`：首个 Reconcile 阻塞期间收到多个事件，释放后总调用数为 2，而不是事件总数。
  - `TestTopologyWatcherReconnectsAndReconciles`：首个 subscriber 返回错误，第二个成功；重连后立即校准，退避有界。
  - `TestTopologyWatcherPeriodicReconcile`：20ms 测试 interval、无事件时也至少调用两次；用条件轮询和 1 秒上限，不用固定长 sleep。
  - `TestTopologyWatcherClose`：阻塞的 `Watch` 因 context cancel 返回，`Close` 等待全部 goroutine 退出。

- [ ] **Step 2: 验证测试失败**

  Run: `go test ./proxy -run 'TestTopologyWatcher'`
  Expected: FAIL，watcher 尚未定义。

- [ ] **Step 3: 实现 `proxy/watcher.go`**

  每个 factory 一个订阅 goroutine；每次循环创建新 subscriber，在 `Watch(ctx, requestReconcile)` 返回后立即显式 `Close`，不得在循环内累积 `defer`。错误后以 100ms 起步、5s 封顶的退避重连，成功后立即 `requestReconcile`。

  单一 reconcile goroutine 监听容量为 1 的 trigger channel 和 ticker，串行执行 `Reconcile`。`Start` 初始触发一次；`Close` cancel 后用 `sync.WaitGroup` 等待。valkey subscriber 使用 Task 3 相同单节点 RESP2 option，`Receive` 订阅 `+switch-master`，只回调匹配 channel 的事件。

- [ ] **Step 4: 验证测试通过**

  Run: `go test -race ./proxy -run 'TestTopologyWatcher'`
  Expected: PASS，无 goroutine 阻塞。

- [ ] **Step 5: 可选提交检查点**

  仅在用户明确确认后执行：`git add proxy/watcher.go proxy/watcher_test.go && git commit -m "feat: watch sentinel topology changes"`。

### Task 5: 接入 TCP Server 与完整生命周期

**Files:**
- Modify: `proxy/server.go`
- Modify: `proxy/server_test.go`

**Interfaces:**
- Consumes: Tasks 1–4 的全部接口。
- Produces:
  ```go
  type ServerOption func(*serverOptions) error
  func WithNodeCredentials(username, password string) ServerOption
  func WithTopologyRefreshInterval(interval time.Duration) ServerOption

  func NewValkeyProxyServer(listener *net.TCPAddr, clientOption valkey.ClientOption, masterName string, options ...ServerOption) (*valkeyProxyServer, error)
  func (r *valkeyProxyServer) Serve()                 // 保留原签名
  func (r *valkeyProxyServer) Close() error           // 幂等
  ```

- [ ] **Step 1: 写失败测试并改造旧测试**

  - `TestNewValkeyProxyServerOptions`：单/多 Sentinel 配置不需要立即拨号；空地址、空 master、非正 interval 返回 error；node credentials 不复制 Sentinel 凭据。
  - `TestProxyHalfClose`：客户端发送请求后 `CloseWrite`，仍能读到完整 echo，随后连接结束。
  - `TestServerClosesStaleSession`：已注册 A 会话在发布 B 后上下游都关闭，其他会话不受影响。
  - `TestServerRejectsRegistrationRace`：在实际拨号返回前推进 generation，handler 不启动转发且关闭连接。
  - `TestServerCloseUnblocksHandler`：fake reconciler 阻塞至 ctx cancel，`Close` 后 handler 和 `serve()` 都退出。
  - 删除依赖本机 `:26379` 的笼统 skip；核心路径使用 fake reconciler/control 和本地 listener。

- [ ] **Step 2: 验证测试失败**

  Run: `go test ./proxy -run 'TestNewValkeyProxyServerOptions|TestProxyHalfClose|TestServer'`
  Expected: FAIL，新 options、注册及生命周期尚未实现。

- [ ] **Step 3: 装配 Server**

  默认 refresh interval 为 5s。为每个 `clientOption.InitAddress` 创建懒 Sentinel control；用独立 `valkey.ClientOption{Username, Password}` 创建 node validator；装配 manager、resolver、controller 和 watcher。构造阶段只验证配置，不执行网络拨号。

  `Serve` 保留原签名，新增内部 `serve() error` 便于测试；`Serve` 仅对非 nil 错误调用现有 fatal 处理。`serve` 创建并保存 listener、启动 watcher，Accept 后为每个 downstream 启动 handler。

- [ ] **Step 4: 实现 Handler 与转发**

  handler 调用 `Reconcile(ctx)` 得 snapshot，再通过带 5 秒 Timeout 的 `net.Dialer.DialContext` 建立 TCP 上游，创建 Session 并调用 `RegisterSession`。注册失败或任一网络步骤失败时关闭上下游，不重放数据。

  转发在当前 handler 中等待两个 copy 方向完成，对 `*net.TCPConn` 保留 `CloseWrite` 半关闭语义；结束统一 `RemoveSession` + `Close`。拓扑管理器关闭 Session 后 copy 必须退出。

- [ ] **Step 5: 实现幂等 Close**

  先关闭 listener 停止 Accept，再 cancel server context、关闭 watcher和活动 Session，等待 serve/handler goroutine 退出，最后关闭懒 Sentinel controls。避免 `WaitGroup.Add` 与 `Wait` 并发，保证重复 Close 不 panic。

- [ ] **Step 6: 验证测试通过**

  Run: `go test -race ./...`
  Expected: PASS。

- [ ] **Step 7: 可选提交检查点**

  仅在用户明确确认后执行：`git add proxy/server.go proxy/server_test.go && git commit -m "feat: close stale sessions on failover"`。

### Task 6: CLI 配置与用户文档

**Files:**
- Modify: `cmd/main.go`
- Modify: `cmd/main_test.go`
- Modify: `README.md`

**Interfaces:**
- Consumes: Task 5 的 `WithNodeCredentials` 和 `WithTopologyRefreshInterval`。
- Produces:
  ```go
  func parseSentinelAddrs(raw string) ([]string, error)
  ```
  新 flags：`-node-user`、`-node-pass`、`-topology-refresh-interval`；`-sentinel-addr` 支持逗号分隔。

- [ ] **Step 1: 写失败测试**

  `TestParseSentinelAddrs` 覆盖：单地址、逗号多地址、去空白、全空、空项和重复地址保留顺序。更新 flag default 测试及 `TestMain` 保存/恢复新 flags；验证 main 构造的 option 不设置 `Sentinel.MasterSet`，node credentials 只通过 ServerOption 传递。

- [ ] **Step 2: 验证测试失败**

  Run: `go test ./cmd -run 'TestParseSentinelAddrs|TestFlagDefaults|TestClientOption'`
  Expected: FAIL，新解析函数和 flags 尚未定义。

- [ ] **Step 3: 实现 CLI 并更新文档**

  解析失败、refresh interval 非正或 server 构造失败时输出明确配置错误。保持现有单地址命令兼容。

  README 增加多 Sentinel、独立 node 认证、5 秒校准、客户端必须自动重连、短连接 `ROLE` 开销以及脑裂/异步复制/fencing 限制；补充 `_scripts/test_failover.sh` 的集成验证方式。

- [ ] **Step 4: 验证测试与格式**

  Run: `gofmt -w cmd/main.go cmd/main_test.go proxy/*.go && go test -race ./... && go vet ./...`
  Expected: 全部 PASS，无 vet 报告。

- [ ] **Step 5: 可选提交检查点**

  仅在用户明确确认后执行：`git add cmd/main.go cmd/main_test.go README.md && git commit -m "docs: document sentinel failover configuration"`。

### Task 7: 真实故障切换验收

**Files:**
- Create: `_scripts/test_failover.sh`
- Modify: `Makefile`

**Interfaces:**
- Consumes: 完整 CLI 和 proxy 实现。
- Produces: `make test-failover`，使用 `VALKEY_IMAGE`（默认 `docker.io/valkey/valkey:8-alpine`）和 `PODMAN`（默认 `podman`）。

- [ ] **Step 1: 编写真实环境验证脚本**

  脚本使用唯一容器名和动态空闲端口，`trap` 清理全部容器及 proxy 进程。启动两个数据节点和三个 Sentinel，等待复制 `connected` 与 `CKQUORUM` 成功。

  验证：
  1. 通过 proxy 的新连接 `ROLE=master`。
  2. 保持一个旧客户端连接，通过 S2/S3 执行真实 failover；旧连接被关闭，重连后目标为新主节点。
  3. 保留真实 A=slave/B=master 状态，隔离并配置 S1 返回旧 A；只经 S1 的新连接必须失败，不得返回 `slave`；恢复 S2/S3 后可接入 B。
  4. 重复切换并建立连接，脚本结束后无残留容器或 proxy 进程。

- [ ] **Step 2: 运行真实验收**

  Run: `make test-failover`
  Expected: 新连接从未落在已知 replica；旧连接在 failover 后断开；重连为 master；清理成功。若本机没有 Podman/镜像，明确报告“未执行真实验收”，不得以跳过代替通过。

- [ ] **Step 3: 最终全量验证**

  Run: `git diff --check && go test -race ./... && go vet ./... && make build`
  Expected: 全部成功。检查 `git status --short`，确认只有本计划与 spec 列出的文件发生变化。

- [ ] **Step 4: 可选提交检查点**

  仅在用户明确确认后执行：`git add _scripts/test_failover.sh Makefile && git commit -m "test: verify sentinel failover connections"`。
