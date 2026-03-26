# rustykernel Roadmap

目标：把 `rustykernel` 从“可运行的 Jupyter kernel scaffold”逐步推进到“对主流前端足够兼容的 Python kernel”。

原则：

- 先补“协议兼容性缺口”，再补“生态与体验能力”。
- 每个里程碑都要有可验证的测试，不靠手工感觉判断是否完成。
- 优先实现会明显影响 VS Code / Jupyter Notebook / Lab 可用性的能力。
- 不追求一次性追平 `ipykernel`，先把高价值缺口逐个消灭。

非目标：

- 短期内不追求完整复制 `ipykernel` 内部架构。
- 短期内不优先做低频边缘能力，只做能提升主流兼容性的部分。

## M0. 固化当前基线

- [x] 整理一份当前已支持协议面的基线文档，和 README 对齐。
- [x] 将现有 `ipykernel` 对比测试归类成“已对齐 / 部分对齐 / 未覆盖”。
- [x] 补一组 smoke tests，覆盖：
  - [x] `kernel_info_request`
  - [x] `execute_request`
  - [x] `complete_request`
  - [x] `inspect_request`
  - [x] `history_request`
  - [x] `is_complete_request`
  - [x] `shutdown_request`
  - [x] `interrupt_request`
- [x] 明确记录当前和 `ipykernel` 的已知行为差异，避免后续误回归。

完成标准：

- `uv run pytest` 继续全绿。
- README 与测试覆盖范围一致。
- 我们能明确回答“当前到底兼容到了哪里”。

## M1. 补齐高优先级协议缺口

目标：先把最影响前端基本兼容性的协议缺口补上。

- [x] 实现真实的 `comm_info_request`，不再固定返回空 `comms`。
- [x] 支持 `comm_open` / `comm_msg` / `comm_close` 基础消息流。
- [x] 为 comm 层设计最小可行状态模型：
  - [x] comm registry
  - [x] target_name 过滤
  - [x] 生命周期清理
- [x] 修正 `interrupt_request` 语义：
  - [x] 不再把 interrupt 等同于 restart
  - [x] 尽量中断当前执行，而不是清空整个 worker 状态
  - [x] 补测试验证 execution count / namespace / history 在 interrupt 后的行为
- [x] 重新审视 `kernel_info_reply` 中 `supported_features` / `debugger` 字段，确保声明与事实一致。

完成标准：

- 新增 comm 基础兼容测试。
- `interrupt_request` 行为不再破坏当前会话状态。
- 和 `ipykernel` 的差距从“协议缺口明显”收敛到“外围生态未完全实现”。

## M2. 引入更接近 IPython 的执行语义

目标：解决“能执行 Python”与“像 notebook 里的 Python 一样执行”之间的差距。

- [ ] 评估执行层方案：
  - [x] 继续增强自研执行器
  - [x] 或最小接入 IPython 交互层
  - 结论：执行主路径已切到真实 `IPython` `InteractiveShell`，保留 `rustykernel` 自己的 Jupyter 传输层、stdin/comm 桥接，以及少量环境适配。
- [x] 明确支持 `get_ipython()`。
- [x] 评估并补齐 `%magic` 支持的最小集合。
  - 当前已补最小子集：`%pwd`、`%cd`、`%lsmagic`、`%time`、`%%time`、`%timeit`、`%%timeit`、`%autoawait`、`%matplotlib inline`。
- [x] 评估并补齐 autoawait / async cell 语义。
  - 当前已支持顶层 `await`、async cell，以及 `asyncio` 路径下的 `%autoawait` 开关。
- [ ] 对齐 `user_expressions`、display formatter、异常展示细节。
  - 当前执行、`user_expressions`、payload、display formatter 已直接走真实 `IPython` shell。
  - `execute_reply` 错误分支已补齐 `user_expressions` / `payload` 基础结构，并新增一条 `ipykernel` runtime error 对照测试。
  - 已新增 `SyntaxError` 与 interrupt 后 `KeyboardInterrupt` 的 `ipykernel` 对照测试。
  - 异常展示细节、更多异常类型、traceback 格式、与 `ipykernel` 的更细粒度对齐仍未完成。
- [x] 清理当前 `IPython.display` 伪模块实现，决定是继续 shim 还是替换为真实能力接入。
  - 结论：已切换到真实 `IPython.display` 与真实 shell，对外只保留 `rustykernel` 侧消息桥接和少量兼容补丁。

完成标准：

- 常见 notebook 用户代码不再因为缺失 `get_ipython()` / magic / async 语义而直接失效。
- 新增一组“接近日常 notebook 使用方式”的端到端测试。

## M3. 调试与控制面能力

目标：补足 `ipykernel` 控制面中高价值但尚未实现的能力。

- [ ] 评估 `debug_request` 接入路径。
- [ ] 若决定支持调试：
  - [ ] 引入 `debugpy` 集成方案
  - [ ] 支持 `debug_request` / `debug_reply`
  - [ ] 支持 `debug_event`
- [x] 评估是否实现 `usage_request`。
  - 当前已在 control channel 实现 `usage_request -> usage_reply`，返回内核进程树和宿主机的基础资源快照，并补了本地 smoke/runtime 测试。
- [ ] 评估是否支持 subshell：
  - [ ] `create_subshell_request`
  - [ ] `delete_subshell_request`
  - [ ] `list_subshell_request`
- [ ] 在 `kernel_info_reply.supported_features` 中准确暴露已支持能力。

完成标准：

- 控制面不再只有最小 shell/control 响应。
- 新增控制面协议测试，并明确哪些能力仍然不支持。

## M4. GUI / Event Loop / Matplotlib / Embed

目标：补 notebook 和交互式科学计算场景下的关键体验能力。

- [ ] 评估事件循环集成策略：
  - [ ] Qt
  - [ ] asyncio
  - [ ] matplotlib inline
- [ ] 支持最小可用的 GUI/event loop 集成。
- [ ] 明确 matplotlib 的工作目标：
  - [ ] 至少 inline 输出可用
  - [ ] 再考虑 GUI backend
- [ ] 评估是否支持 `embed_kernel()` 类能力。
- [ ] 增加 event loop / matplotlib 相关兼容测试。

完成标准：

- 常见 notebook 图形展示场景可用。
- 不再局限于“纯文本 + 简单 mime bundle”交互。

## M5. 输出捕获与 IO 强化

目标：缩小与 `ipykernel` 在 IO 可靠性上的差距。

- [ ] 评估从 `redirect_stdout/redirect_stderr` 升级到 fd 级别捕获。
- [ ] 处理 C 扩展、子进程、`faulthandler` 等绕过 Python `sys.stdout` 的输出场景。
- [ ] 评估日志、stderr、异常输出在前端中的显示一致性。
- [ ] 补 IO 回归测试。

完成标准：

- 输出捕获不再只覆盖 Python 级 `print()`。
- 复杂运行时场景下前端输出更稳定。

## M6. 工程化与发布准备

目标：在能力逐步补齐后，收敛为可维护、可发布、可回归验证的工程状态。

- [ ] 扩大 `ipykernel` 对照测试矩阵。
  - 当前已新增并分类的对照面包括：`kernel_info_request`、`connect_request`、`execute_request`（success / runtime error / syntax error / interrupt 后 `KeyboardInterrupt`）、`comm_info_request`、`complete_request`、`inspect_request`、`history_request`、`is_complete_request`、`shutdown_request`（`restart: false`）。
  - 当前仍未补进对照矩阵的高价值面主要是 stdin flow、rich display、`user_expressions` 专项场景，以及更广的 stream / 异常类型覆盖。
- [ ] 统计并跟踪：
  - [x] 已对齐能力
  - [x] 明确不支持能力
  - [x] 已知行为差异
  - 当前分别由 `docs/ipykernel-compat-matrix.md`、`docs/protocol-baseline.md`、`docs/known-behavior-differences.md` 跟踪。
- [ ] 增加 CI 中的兼容性任务。
- [ ] 补安装、kernelspec、文档与故障排查说明。
- [ ] 形成一个“支持矩阵”文档。

完成标准：

- 每次改动都能知道有没有打破兼容性。
- 用户能清楚知道 `rustykernel` 当前支持什么、不支持什么。

## 推荐执行顺序

1. M0 基线固化
2. M1 高优先级协议缺口
3. M2 IPython 执行语义
4. M5 输出捕获与 IO 强化
5. M3 调试与控制面
6. M4 GUI / Event Loop / Matplotlib / Embed
7. M6 工程化与发布准备

说明：

- `comm/widgets` 和 `interrupt` 必须优先做，因为它们直接决定前端基础兼容性。
- IPython 语义要早做，否则后面很多兼容性问题都会反复返工。
- debugger / subshell / GUI 能力价值高，但依赖前面执行模型更稳定。
