# rustykernel TODO

目标：把 `rustykernel` 收敛成一个对 `Jupyter / VS Code` 日常使用足够稳定的
Python kernel，而不是继续堆分散的设计文档。

## 当前进度

已完成的核心能力：

- [x] 基础 Jupyter 传输层已成型：`shell` / `iopub` / `stdin` / `control` / `hb`
- [x] Python 执行主路径已切到真实 `IPython InteractiveShell`
- [x] 支持常用 notebook 语义：`get_ipython()`、magic、top-level `await`
- [x] 支持 `execute` / `complete` / `inspect` / `history` / `is_complete`
- [x] 支持 stdin 输入、不中断会话状态的 `interrupt_request`
- [x] 支持基础 comm 生命周期与 `comm_info_request`
- [x] 支持 fd 级 stdout/stderr 捕获和 rich display 输出
- [x] 支持 control 面 `usage_request`
- [x] 支持 debugger 基线链路：`debug_request` / `debug_event` / live step / continue / pause
- [x] 支持 subshell 控制请求与按 `subshell_id` 路由执行
- [x] 支持 inline matplotlib，并已补相应 notebook 语义测试

当前项目状态：

- 已经不是“只会回静态 metadata 的 scaffold”
- 已经具备真实 notebook 运行能力
- 当前主要矛盾从“功能缺失”转成“兼容性硬化 + 发布收口”

## 核心 TODO

### 1. 兼容性硬化

- [ ] 扩大 `ipykernel` 对照测试，优先补：
  - stream 行为
  - 更多异常类型和 traceback 细节
  - stdin / rich display / user expressions 的更强对照
- [ ] 收敛 `kernel_info_reply`、`inspect_reply`、`complete_reply` 的细节差异
- [ ] 明确当前不支持面，避免对外暗示“已完全兼容 ipykernel”

### 2. debugger + subshell 稳定性

- [ ] 补 subshell 生命周期边界测试：
  - [x] 执行中删除
  - [x] restart 时清理
  - [x] shutdown 时清理
  - [x] 多 subshell 并发异常场景
- [x] 补多 subshell 同时提交 `execute_request` 的回归测试
- [ ] 补 debugger 与 subshell 组合测试：
  - [ ] pause / continue / disconnect 竞争路径
  - active debug session 下的 interrupt / shutdown
- [x] 补 debugger `pause` in subshell 的基础 smoke
- [ ] 继续减少 worker 侧兼容分支，让 Rust control-plane 状态更单一

### 3. 发布与工程化

- [ ] 在 CI 中加入更明确的兼容性任务，而不只跑基础测试
- [ ] 收敛安装、kernelspec、故障排查文档
- [ ] 在发布前给出一份简洁支持矩阵：
  - 已支持
  - 部分支持
  - 明确不支持

## 暂不展开

这些方向先不扩张，避免重新把 TODO 写散：

- [ ] GUI backend（Qt 等）
- [ ] `embed_kernel()` 类能力
- [ ] 完整复制 `ipykernel` 内部架构
