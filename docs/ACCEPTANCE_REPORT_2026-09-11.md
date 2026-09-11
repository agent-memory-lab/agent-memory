# Agent Memory 验收报告

日期：2026-09-11

## 结论

当前版本满足本地 SQLite、PostgreSQL 记忆插件和 alpha 制品发布条件。可信反馈闭环、证据撤销传播、Episode/Procedure 候选、受控晋升、回滚恢复和一个确定性检索策略候选均已实现。PostgreSQL 17.11 隔离库合同已真实执行；多区域生产运维仍由宿主负责，不属于本轮认证范围。

## 环境

- macOS arm64
- CPython 3.13.5
- 项目环境：`.venv`
- 默认 Core 无第三方运行依赖；PostgreSQL、MCP、SDK、LangGraph 与 Evolution 均为可选包

## 已通过

| 验收项 | 结果 | 证据 |
| --- | --- | --- |
| Core 与跨包回归 | 通过 | 86/86，零失败、零跳过；覆盖 Core、Evolution、SDK、LangGraph、MCP 与真实 PostgreSQL 合同 |
| Evolution 专项 | 通过 | 10/10，覆盖反馈整理、授权、幂等、活动指针、恢复与检索策略 |
| 静态检查 | 通过 | Core、全部扩展包、测试与工具 Ruff 无错误 |
| 固定资源负载 | 通过 | 1000 次写入、100 次检索；初始化 4.441ms，写入 1123.507ms，检索 3955.845ms |
| 内存与存储 | 通过 | 峰值 RSS 93,978,624 bytes；Python heap 2,681,993 bytes；SQLite 1,990,656 bytes |
| Token 预算 | 通过 | 最大估算 648，未超过 1200 |
| PostgreSQL 合同 | 通过 | PostgreSQL 17.11、schema v2、真实 v1→v2 迁移、并发幂等、跨 scope、重启和删除传播 |
| PostgreSQL 队列背压 | 通过 | scope 容量上限、job key 去重、满载拒绝与事务锁真实执行 |
| PostgreSQL 数据扫描 | 通过 | 全 scope 健康，无容量违规；数据库敏感信息正则扫描零命中 |
| 制品构建 | 通过 | 6 个包各生成 wheel 与 sdist，共 12 个制品 |
| 发布敏感信息扫描 | 通过 | 123 个源文件、12 个归档；`scan_complete=true`；零命中 |
| 干净制品安装 | 通过 | 临时 Python 3.13.5 环境安装后 6 个包均可导入，preflight 命令可启动 |

## 阻塞项

| 验收项 | 状态 | 原因与解除条件 |
| --- | --- | --- |
| 无 | 已解除 | PostgreSQL 17.11 已安装，并使用动态端口的一次性隔离测试库完成真实验收 |

## 已实现边界

- 宿主定义任务结果、Reward、可信 evaluator、approver 和 scope。
- 插件保存证据并整理 Claim、Episode、Procedure 候选和有界 MemoryBundle。
- 只有完整、有效、同 scope 的反馈链能生成候选；失败轨迹仅作为反例，不自动贡献成功支持。
- Procedure 必须经过 offline、shadow、canary、限时人工审批和部署，不能跳阶段。
- 检索策略候选只能调整通道权重和预算分配，不能扩大 scope 或总 Token 预算。
- 插件不训练模型权重，不自行改写 Agent Prompt、工具、Router 或工作流。

## 复现命令

```bash
source .venv/bin/activate
python -m pytest -q tests packages/*/tests -rs
python -m ruff check src packages/*/src packages/*/tests tests tools
python tools/resource_baseline.py --events 1000 --recalls 100
```

真实 PostgreSQL 验收需设置仅指向隔离测试数据库的 `AGENT_MEMORY_TEST_POSTGRES_DSN`；数据库名必须包含 `test`，否则测试会拒绝执行。不得使用业务数据库。

## 发布判断

可以发布为 SQLite/PostgreSQL alpha 和可插拔包制品。PostgreSQL 合同已在本机真实验证，但这不等于多区域、高可用、备份恢复或业务容量认证；这些部署责任仍属于宿主。
