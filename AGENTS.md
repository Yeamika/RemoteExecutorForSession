# [`RemoteExecutor`](https://github.com/Yeamika/RemoteExecutor) 的二层包装

该项目是主机端，用于直接对接 agent，同时内置了一个 local 执行器。

该项目属于 Agent Harness（马具）工程。

该项目承载多 RE 管理能力，包括 executor 列表、连接、路由、shell 配置与 reload 等面向 agent 的管理入口。

## 二进制

该项目提供 REFS MCP 与 `refs-ptyt` 等二进制，用于 agent 工具调用、远程执行器访问和 TUI/PTY 控制。

### REFS MCP memory

使用内存作为状态存储库。

### refs-ptyt

`refs-ptyt` 是 REFS 的 agent 监视窗口，同时可用于和 agent 创建的 PTY/窗口进行交互。
