# Agent Memory V4 近期实施计划

日期：2026-09-14

状态：待执行。本文定义 V4-P0 至 V4-P4 的近期开发，不表示任务已经实现或验收通过。

设计依据：[Agent Memory 插件化架构设计 v4](AGENT_MEMORY_PLUGIN_ARCHITECTURE_V4.md)。执行清单见 [TASKS_V4.md](TASKS_V4.md)。T01-T18 的历史状态继续以 [TASKS.md](TASKS.md) 和既有验收报告为准。

## 1. 近期目标

把现有可用 MVP 升级为可供第三方稳定集成的 Agent Memory 插件基础设施：

```text
Plugin Protocol v1
    -> automatic trajectory capture
    -> bounded hybrid retrieval
    -> asynchronous consolidation
    -> replay, evaluation and release gates
```

近期目标不包含强制图数据库、内置本地模型、Latent Memory、模型训练或完整 Agent 编排。所有新增能力必须保持可选、可降级、有来源和有资源上限。

## 2. 当前基线

T01-T18 已建立以下基础：

- Event、Claim、Current State、Episode、Procedure 和 MemoryBundle；
- Decision、Outcome、Evaluation、Reward 和可信反馈关联；
- Procedure 候选、评测、shadow、canary、active 和 rollback；
- 检索追踪、上下文预算、作用域隔离和 Forget 传播；
- SQLite 与 PostgreSQL Provider；
- Python SDK、MCP、LangGraph 和 Evolution 可选包；
- Python 3.13、真实 PostgreSQL、构建、扫描和资源基线验收。

V4 不重建上述能力。新任务优先复用现有领域对象、端口、Provider 和测试工具，只在协议存在明确缺口时扩展。

## 3. 实施原则

1. Core 保持零第三方运行依赖。
2. 插件输出候选或提案，Kernel 执行最终校验和提交。
3. Event 先持久化，模型提取和重型索引后执行。
4. 所有插件调用具备超时、容量、批量和并发上限。
5. Scope、授权、证据和删除失败时 fail closed。
6. 可选提取、检索和巩固失败时回退到确定性基础能力。
7. 新增接口必须包含兼容策略、错误语义和 contract tests。
8. 高级能力先与固定基线比较，取得可复现收益后才能进入默认建议配置。

## 4. 里程碑

| 里程碑 | 任务 | 交付物 | 退出条件 |
| --- | --- | --- | --- |
| M4 Plugin Protocol v1 | T19-T23 | Manifest、六类协议、能力协商、Loader、测试工具 | 第三方插件可独立实现；错误插件不能绕过 Kernel 规则 |
| M5 Automatic Capture | T24-T28 | 生命周期信封、脱敏、队列、框架接入 | Agent 主流程不因 Capture 故障中断；事件幂等且可恢复 |
| M6 Hybrid Retrieval | T29-T34 | lexical/semantic/temporal/entity 候选、RRF、Trace | 任一可选通道关闭或失败仍可返回安全的 MemoryBundle |
| M7 Async Consolidation | T35-T39 | Worker、Episode、Claim、Procedure 后台整理 | 中断可恢复、重复执行幂等、删除传播完整、热路径不依赖 Worker |
| M8 Evals and Release Gates | T40-T43 | Snapshot、Replay、Benchmark、资源和发布门 | 效果与成本可复现；失败或跳过不能报告为通过 |

执行顺序为 M4、M5、M6、M7、M8。M8 的评测 Schema 和固定基线可在 M4 完成后并行准备，但发布门必须使用前序阶段冻结的协议和数据版本。

## 5. M4：Plugin Protocol v1

### 目标

把当前 Python Entry Points 和已有 Provider/Adapter 能力提升为正式、可版本化的插件协议。

### 交付

- `PluginManifest`：名称、版本、plugin API、kind、capabilities、依赖、配置 Schema、资源限制和 failure mode；
- `PluginContext`：可信作用域、Logger、Clock、资源预算和受限 Kernel 服务；
- `CaptureAdapter`、`Extractor`、`Retriever`、`Consolidator`、`StorageProvider`、`Evaluator` 六类 Protocol；
- 生命周期：manifest、initialize、health、close；
- 能力协商、版本兼容、重复注册和冲突处理；
- 稳定错误码及 fail-open/fail-closed 分类；
- 第三方插件 contract test kit；
- 最小 reference plugin，不引入第三方依赖。

### 关键约束

插件不得获得未裁剪的跨 scope 数据，不得直接写 active Procedure，不得修改总 Token 预算，不得声明未完成的删除成功。

## 6. M5：Automatic Capture

### 目标

让宿主无需手动拼装每一条 Event，即可从对话、工具和结果轨迹中形成可信证据链。

### 交付

- 通用 `LifecycleEvent` envelope；
- turn、message、tool、decision、outcome、evaluation、reward 事件映射；
- payload 大小限制、引用策略、哈希和敏感信息过滤；
- 有界 Capture Queue、背压、幂等键和恢复状态；
- Python SDK、LangGraph 和 MCP 的参考接入；
- Capture 健康状态和降级可观测性。

### 关键约束

Event 必须先于自动提取提交。CaptureAdapter 不负责决定事实真伪，也不能扩大宿主传入的 Trusted Scope。

## 7. M6：Hybrid Retrieval

### 目标

在保持当前状态优先和硬预算的前提下，引入可替换的多信号检索。

### 交付

- 确定性 lexical baseline；
- 可选 semantic、temporal 和 entity Retriever；
- 并行候选执行、独立超时和熔断；
- Reciprocal Rank Fusion；
- scope、trust、validity 和 provenance 二次校验；
- 冲突、去重、多样性和过期控制；
- embedding/index/policy version；
- 完整 RetrievalTrace 和降级原因。

### 关键约束

向量和图结果只是候选。无来源、越权、已删除或已失效内容不能进入 MemoryBundle。默认只执行一次并行检索。

## 8. M7：Async Consolidation

### 目标

把昂贵整理移出 Agent 热路径，并使整理过程可恢复、可审计和可关闭。

### 交付

- 通用有界 Worker 运行器；
- Episode segmentation；
- Claim 去重、冲突和 supersession proposal；
- Procedure induction proposal；
- 检查点、租约、背压、取消、暂停、重试和死信；
- 来源删除后的任务取消和派生物失效；
- Provider 无关的任务合同。

### 关键约束

Worker 停机不能影响基础 remember/recall/state/forget。整理插件不能直接提交 active 对象，失败不能留下部分生效状态。

## 9. M8：Evals and Release Gates

### 目标

建立能证明记忆效果、正确性和轻量化的可重复验收体系。

### 交付

- 版本化 Dataset Snapshot；
- deterministic Replay；
- no-memory、fixed-policy、candidate-policy 三类基线；
- 记忆准确率、证据覆盖、时间正确性、冲突正确性和删除完整性指标；
- 构建、检索和生成阶段的独立延迟、Token、模型调用、RSS 和存储增长报告；
- 插件 contract、兼容性、故障注入和重复运行套件；
- 发布前源码、制品、真实 SQLite/PostgreSQL 和 Python 3.13 门禁。

### 关键约束

评测结果必须绑定数据快照、代码版本、插件版本、模型版本和评价器版本。模型自评不能作为唯一发布依据。

## 10. 包计划

近期新增三个可选包：

| 包 | 首次出现 | 说明 |
| --- | --- | --- |
| agent-memory-capture | M5 | 生命周期信封和 Capture Adapter |
| agent-memory-hybrid | M6 | 多信号候选与 RRF，不内置本地模型 |
| agent-memory-evals | M8 | Snapshot、Replay、Benchmark 和 contract kit |

Plugin Protocol 的基础类型和 Loader 属于 `agent-memory` Core。通用 Worker 合同放入 Core，具体 PostgreSQL worker 实现在现有 PostgreSQL 包，避免新建只包含薄封装的包。

## 11. 数据库演进

Schema 变更必须满足：

- 迁移可重复执行；
- SQLite 和 PostgreSQL 使用同一行为合同；
- 旧数据库可以读取并明确升级；
- 插件表使用命名空间，不能无约束修改 Core 表；
- derived index 可丢弃并重建；
- 删除和回滚在事务边界内保持一致；
- 双时间字段加入前提供旧记录的确定性默认值。

## 12. 测试策略

每个任务先完成最小针对性测试，里程碑结束时进行复合验收。最终只在 M8 执行完整发布验收。

| 测试层 | 内容 |
| --- | --- |
| Unit | Schema、排序、预算、错误和状态迁移 |
| Contract | 六类插件、SQLite/PostgreSQL 和框架适配器 |
| Fault | 超时、异常、重复、乱序、中断、恢复和删除失败 |
| Integration | Capture 到 Event、Retrieve 到 Bundle、Feedback 到 Candidate |
| Replay | 固定快照下结果与版本可复现 |
| Resource | 冷启动、RSS、延迟、队列、存储和模型调用预算 |
| Release | Python 3.13、真实后端、构建、安装、扫描和文档一致性 |

## 13. 不进入近期范围

- 强制 Graph Retriever；
- Prospective Intent 正式进入 Core；
- 本地 embedding/reranker 模型随默认包安装；
- Latent、KV Cache 或 LoRA Memory；
- PPO、DPO、GRPO 或 SFT 训练；
- Prompt、Harness、工具或 Workflow 自动发布；
- 通用多 Agent 编排平台；
- 托管云控制台。

## 14. 近期完成定义

V4 近期目标只有在以下条件全部满足时完成：

1. 外部开发者可以只依赖公开协议实现并发布一个插件。
2. Agent 生命周期可以自动形成完整、可信、可删除的证据链。
3. 混合检索任一可选通道失败时仍能安全降级。
4. 后台整理中断、重复和恢复不会污染 Current State。
5. 所有返回内容可追溯来源、作用域和策略版本。
6. 效果、资源和发布结果在 Python 3.13 与真实存储后端上可复现。
7. 默认安装仍不需要模型、向量数据库、图数据库或后台服务。

