# Debugger Redesign Plan: 方案 B（Rust 直接持有 debugpy DAP 会话）

本文档记录 `rustykernel` 调试能力的下一阶段重设计决策：

- **不再继续沿着“Python worker 内部并发化”演进**
- **直接切到方案 B：由 Rust 内核主进程直接持有并编排 real debugpy DAP 会话**

目标不是做“更多 shim”，而是把 live debugger 的控制面从 Python worker 的串行执行模型中拆出来。

---

## 1. 当前问题与为什么不再走 A

当前实现已经具备：

- control channel 上的 `debug_request` / `debug_reply`
- in-process `debugpy` attach
- real `debug_event` 实时 IOPub 转发
- `threads` / `stackTrace` / `scopes` / `variables` / `continue` / `next` / `stepIn` / `stepOut`
  对 real debugpy 的优先转发
- 本地 shim fallback

但真实 live debug 的关键阻塞仍然存在：

1. Python worker 仍是 **单 stdin/protocol 主循环**
2. `execute_request` 运行期间，worker 会被长时间占住
3. control channel 的 `debug_request` 最终仍要进同一个 worker 串行入口
4. 因此真实的：
   - pause
   - stopped 后的 stack/scope/variables 查询
   - continue / next / stepIn / stepOut
   - live stopped session 的后续控制
   都无法稳定做到“执行中可并发控制”

这不是某个时序 bug，而是**架构层面的串行瓶颈**。

因此不再继续走 A（worker 内部做更多线程/队列），而是直接切到 B：

> **execute 面继续留在 Python worker，debug control 面迁移到 Rust 主进程直接对接 debugpy DAP。**

---

## 2. 方案 B 的核心设计

### 2.1 新职责边界

#### Rust 主进程负责

- 建立和维护 debugpy DAP socket
- 发送/接收所有 real DAP request/response/event
- 维护 kernel-side debugger state machine
- 把 DAP event 映射成 Jupyter `debug_event` IOPub
- 把 control channel `debug_request` 映射成 DAP request
- 关联 execute parent header 与 live debug event 的 IOPub 路由

#### Python worker 负责

- 执行 cell / IPython shell
- `dumpCell` 与 source file materialization
- 暴露/启动 debugpy listener endpoint
- 必要时提供少量 Python-side helper 能力
- 在 real DAP 不可用时保留最小 fallback

---

## 3. 重设计后的总体结构

### 3.1 新增 Rust 侧 `DebugSession`

建议新增一个独立模块，例如：

- `src/debug_session.rs`

它负责：

- `start_listener_if_needed`
- `connect_dap`
- `send_request`
- `recv_loop`
- `event_dispatch`
- `shutdown`
- request seq / response correlation
- debug state cache

建议核心结构：

```rust
struct DebugSession {
    transport: DebugTransport,
    state: Arc<Mutex<DebugStateCache>>,
    responses: Arc<Mutex<HashMap<i64, Sender<Value>>>>,
    event_tx: Sender<DebugEventEnvelope>,
    next_seq: AtomicI64,
}
```

### 3.2 `DebugStateCache` 负责的状态

Rust 成为调试状态真值来源，至少缓存：

- initialized / attached / configured
- current dap capabilities
- stopped thread set
- last known threads
- last stop reason / allThreadsStopped
- active dumped sources
- source path ↔ execute parent msg id 的映射
- 是否存在 pending debuggee session

### 3.3 event pump

Rust 侧启动后台线程持续读取 DAP socket：

- response -> 投递到等待中的 request
- event -> 更新 `DebugStateCache`
- event -> 转发成 Jupyter `debug_event`

这条链路必须独立于 Python worker execute 生命周期。

---

## 4. Python worker 需要缩减成什么样

### 4.1 worker 不再做 DAP client

当前 `python/rustykernel/worker_main.py` 里的这些职责将逐步迁出：

- `DebugpyDAPClient`
- 大部分 `_forward_dap_request`
- real DAP event drain / callback 转发
- DAP request/response 主链路

### 4.2 worker 最终只保留的 debug 相关职责

#### 必留

- `dumpCell`
- `source`
- `debugInfo` 的少量 worker-local 信息（若仍需要）
- 启动 debugpy listener 并把 endpoint 返回给 Rust

#### 可选保留

- 当 real debugpy 不可用时的最小 fallback：
  - `debugInfo`
  - `dumpCell`
  - `source`
  - 合成 stopped / stackTrace / scopes / variables

但 fallback 必须被清晰标记为 fallback，不能再和 real DAP 主路径混在一起。

### 4.3 worker 需要新增的最小接口

建议新增新的 worker request kind，例如：

- `debug_listen`
  - 作用：启动或返回现有 debugpy listener endpoint
  - 返回：`host`, `port`

可选：

- `debug_reset`
- `debug_shutdown`

这样 Rust 能直接连上 debugpy，而无需再让 Python 代发 DAP request。

---

## 5. control channel 映射重设计

### 5.1 新原则

收到 Jupyter `debug_request` 后：

1. **优先走 Rust `DebugSession`**
2. 只有 worker-local 命令才下发给 Python worker
3. fallback 仅在明确 real DAP unavailable 时启用

### 5.2 命令分层

#### Rust 侧直连 DAP 的命令

- `initialize`
- `attach`
- `configurationDone`
- `disconnect`
- `setBreakpoints`
- `threads`
- `stackTrace`
- `scopes`
- `variables`
- `continue`
- `next`
- `stepIn`
- `stepOut`
- 后续可加 `pause`
- 后续可加 `evaluate`
- 后续可加 `exceptionInfo`

#### worker-local 命令

- `dumpCell`
- `source`
- 可能还有 `debugInfo` 中的临时文件、hash 之类字段

#### 聚合命令

- `debugInfo`
  - Rust state + worker-local info 合并返回

---

## 6. execute 与 debug event 的 parent header 关联

这是 B 方案里最容易被忽略的点。

Jupyter `debug_event` 要发到 IOPub 时，需要决定 parent header。

### 6.1 当前最低目标

对“由一次 execute 触发出来的调试事件”，Rust 维护：

- source path / cell hash -> 最近一次 execute parent header

当 stopped event 来自该 source 时：

- IOPub 的 parent_header 尽量绑定到对应 execute_request

### 6.2 更长期目标

如果 stopped 发生在非 execute 驱动场景（未来 pause / attach 后手动控制）：

- 使用最近相关的 debug control parent
或
- 统一使用空 parent / 最近活跃 debug parent

这一策略需要单独固定，避免同一事件在不同路径 parent 不一致。

---

## 7. 状态机设计

Rust 成为状态机真值来源。

### 7.1 核心状态

- `Detached`
- `Listening`
- `Attached`
- `Configured`
- `Stopped`
- `Running`
- `Terminated`

### 7.2 事件驱动

#### DAP event -> 状态变更

- `initialized` -> `Attached`
- `stopped` -> `Stopped`
- `continued` -> `Running`
- `terminated` / `exited` -> `Terminated`

#### DAP response -> 状态辅助变更

- `attach(success)` -> `Attached`
- `configurationDone(success)` -> `Running`
- `disconnect(success)` -> `Detached`

### 7.3 `debugInfo.stoppedThreads`

以后不再从 worker shim 推导，直接由 Rust `DebugStateCache` 输出。

---

## 8. 测试策略

方案 B 落地后，测试要分三层。

### 8.1 Rust 单元/集成测试

验证：

- DAP request/response correlation
- event 更新状态机
- `debugInfo.stoppedThreads`
- DAP event -> `debug_event` IOPub

### 8.2 kernel 端到端测试

新增稳定 real debugpy 路径：

- initialize -> attach -> configurationDone
- execute 触发 real stopped
- `stackTrace/scopes/variables`
- `continue`
- 最终 execute_reply

### 8.3 fallback 测试

当 real debugpy unavailable 时：

- `dumpCell`
- fallback `stopped`
- fallback `stackTrace/scopes/variables`

确保 fallback 仍然可用，但不再伪装成 fully live debugger。

---

## 9. 分阶段实施计划

### Phase 0：冻结当前行为

- 保持现有实时 `debug_event` / shim 测试全绿
- 不再继续扩张 Python-side DAP client 逻辑

### Phase 1：Rust 侧引入 `DebugSession`

- 新增 `src/debug_session.rs`
- 支持：
  - DAP socket 建连
  - 后台 recv loop
  - request/response correlation
  - event cache

当前进展：

- 已完成第一版 `DebugSession` 基础实现
- 已能通过 worker `debug_listen` 获取 debugpy listener endpoint
- 已能从 Rust 直接连接 debugpy，并成功完成：
  - `initialize`
  - `attach`（通过等待 `initialized` event 的兼容路径）
  - `configurationDone`

### Phase 2：worker 改成只提供 listener endpoint

- 新增 worker request：`debug_listen`
- Rust attach 时不再通过 worker 代发 DAP attach
- Rust 自己连到 debugpy

当前进展：

- `debug_listen` 已实现并已被 Rust 使用
- worker 侧旧的 `debug_request` real-DAP 代理已删除，只保留 listener endpoint、
  `dumpCell`、breakpoint bookkeeping 与 synthetic stopped 载荷

### Phase 3：切 control-plane 命令

- `initialize`
- `attach`
- `configurationDone`
- `setBreakpoints`
- `threads`
- `stackTrace`
- `scopes`
- `variables`
- `continue`
- `next`
- `stepIn`
- `stepOut`

全部改成 Rust 直连 DAP 主路径。

当前进展：

- 上述命令均已切到 Rust `DebugSession` 主路径
- `threads` / `stackTrace` / `scopes` / `variables` 在 real DAP 不返回有效结果时，
  改由 Rust 读取 synthetic stopped snapshot 回答，而不再回退到 worker shim

### Phase 4：清理 Python 侧 DAP client

- 删掉 `DebugpyDAPClient`
- 删掉 worker 内大部分 real DAP forwarding
- 保留 worker-local fallback/debug helpers

### Phase 5：补真实 live debug 测试

- [x] real stopped
- [x] real stack variables
- [x] real continue
- [x] real step（`next` / `stepIn` / `stepOut`）
- 如可行，再补 pause

---

## 10. 文件级改动建议

### 必改

- `src/kernel.rs`
  - `debug_request` 分发改到 Rust `DebugSession`
  - IOPub `debug_event` 路由从 worker update 扩展为 Rust-side DAP event

- `src/worker.rs`
  - 新增 `debug_listen`
  - 删除或弱化现有 `debug_request` 对 real DAP 的代理职责

- `python/rustykernel/worker_main.py`
  - 新增 `debug_listen` request handler
  - 缩减当前 `DebugState` 里 real DAP client 的职责

### 新增

- `src/debug_session.rs`
  - DAP transport / session / state cache / recv loop

### 随后更新

- `tests/test_protocol_smoke.py`
- `docs/protocol-baseline.md`
- `docs/known-behavior-differences.md`
- `TODO.md`

---

## 11. 风险与约束

### 11.1 风险

- Rust 侧要自己维护 DAP 会话生命周期，复杂度上升
- worker restart 时，Rust 侧 debug session 必须同步重建
- DAP event 与 execute parent header 的归属可能有歧义
- fallback 与 real 路径并存时，必须严防状态分裂

### 11.2 约束

- 在 Phase 3 前，不能让两套“谁是真值”的状态机长期共存
- 一旦 Rust 侧开始接管 real DAP，`stoppedThreads` 真值应立即转到 Rust
- Python 侧不得继续隐式吞掉 real DAP event 并自行改状态

---

## 12. 结论

方案 B 的本质不是“换一处写 DAP 代码”，而是：

> **把 live debugger 的控制面从 Python worker 的串行执行模型中彻底解耦出来。**

这会增加 Rust 侧会话管理复杂度，但能从架构上解决当前最关键的问题：

- execute 中可并发控制调试会话
- stopped 后可稳定查询 frame/variables
- continue/step 不再依赖 worker 主循环腾空

因此，接下来调试能力的重设计将以 **Rust 直接持有 debugpy DAP 会话** 为主线推进。
