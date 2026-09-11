# Agent Memory v3 实现差距审计

日期：2026-09-11

## 结论

当前仓库已具备 Event、Claim、Current State、Episode、Procedure、MemoryBlock、Decision、Outcome 和 Reward 的基础对象，支持 SQLite、PostgreSQL provider、MCP、SDK、LangGraph 和受控 Evolution 插件。当前不应宣称完整 v3 反馈闭环，因为 RetrievalTrace、可信反馈关联、更正/乱序处理和真实 PostgreSQL 合同验收尚未完成。

## 已有对象与调用链

| 能力 | 当前实现 | 结论 |
| --- | --- | --- |
| 证据写入 | `MemoryEvent -> Claim -> StateDelta` | 已实现幂等和 supersession |
| 检索 | `MemoryQuery -> MemoryBundle` | 已有 Token/条数上限，缺 RetrievalTrace |
| 反馈 | `DecisionRecord -> OutcomeEvent -> RewardSignal` | 可写入 evolution record，缺可信引用校验与查询收据 |
| 整理 | Event 自动提取、Episode、MemoryBlock consolidation | 已有基础，缺完整反馈驱动的 Procedure 候选生成 |
| 进化 | candidate/evaluated/shadow/canary/active/rollback | 已有状态机和部署失败补偿，缺 scope、评估者授权和持久化版本指针完整验收 |
| 删除 | Event/Claim/MemoryBlock 证据传播 | 已有基础，缺对反馈、评价和候选的失效传播 |

## 权限边界

- scope 必须由宿主或 MCP 认证上下文提供，不允许模型通过工具参数扩大。
- 奖励定义和任务成功判定属于宿主；记忆插件只保存、校验和消费结果。
- `publish_procedure` 仍是直接 provider 入口。ACTIVE Procedure 虽要求事件证据，但尚未强制经过 EvolutionEngine，因此不应对不可信宿主开放该入口。
- shadow/canary 的真实流量和工具副作用由宿主执行，插件只维护状态、证据和审计。

## 后续必须修复

1. 增加 RetrievalTrace 及返回项/实际使用项区分。
2. 对 Decision、Outcome、Evaluation 和 Reward 执行 scope、版本和引用校验。
3. 增加幂等反馈收据、更正关系、有界待关联状态及过期原因。
4. 扩展 SDK、MCP 和 LangGraph，使其调用同一反馈业务规则。
5. 用真实 PostgreSQL 运行 SQLite 同组合同测试。
