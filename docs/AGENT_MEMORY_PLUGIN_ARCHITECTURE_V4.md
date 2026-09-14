# Agent Memory 插件化架构设计 v4

更新日期：2026-09-14

状态：目标架构。本文定义下一阶段实现边界，不代表所有列出的能力已经完成。

实施文件：[V4 近期实施计划](PLAN_V4.md) | [V4 执行任务](TASKS_V4.md)

## 1. 架构结论

Agent Memory 采用“稳定 Kernel + 可选能力插件”架构：

> 一个轻量、可插拔、证据驱动的记忆内核，将 Agent 轨迹转换为版本化状态、情节记忆和可复用过程候选，并通过受预算约束的检索、受控巩固、评测、晋升和回滚持续改进记忆。

本项目不组合多个框架的数据模型。Mem0、Graphiti、Hindsight、Letta、Cognee 和 MemOS 中有价值的机制统一映射到现有 Event、Claim、Episode、Procedure 和 MemoryBundle，不建立第二套事实、经验或技能体系。

必须同时满足以下约束：

1. 只有一个权威证据源：Event Ledger。
2. 只有一套核心领域模型。
3. 向量、图、实体和摘要均为可重建派生能力。
4. 默认安装不需要模型、向量数据库、图数据库或后台服务。
5. 插件不能绕过作用域、证据、预算、删除和晋升规则。
6. 宿主负责定义任务成功和权限，插件负责根据可信结果进化记忆。

## 2. 产品边界

```text
外部 Agent / Harness
   |
   | Lifecycle Event / Decision / Outcome / Evaluation / Reward
   v
Agent Memory
   |- 保存原始证据
   |- 维护当前有效状态
   |- 生成 Episode 和 Procedure Candidate
   |- 执行有界检索与上下文组织
   |- 管理冲突、过期、巩固、评测、晋升和回滚记录
   `- 输出 MemoryBundle、RetrievalTrace 和 EvolutionProposal
   |
   v
外部 Agent 继续规划和执行
```

Agent Memory 不在基础产品中负责：

- 修改模型权重；
- 定义 Reward 或业务成功标准；
- 自主重写并发布 Agent Prompt、工具、Router 或 Workflow；
- 构建完整多 Agent 组织；
- 绕过宿主审批发布 Procedure 或 Agent 版本；
- 将模型自评自动提升为可信评价。

Agent 训练和运行编排保留为最后阶段的独立扩展。扩展依赖稳定记忆协议，Memory Kernel 不反向依赖训练框架或调度平台。

## 3. 总体架构

```text
Agent / Runtime
  LangGraph / MCP / Python SDK / custom harness
                    |
                    v
             Capture Adapter
  turn / message / tool / decision / outcome / reward
                    |
                    v
+------------------------------------------------------+
|                  Memory Kernel                       |
|                                                      |
|  Protocol / Policy / Scope / Trust / Budget / Audit |
|                                                      |
|  Event --> Claim --> Current State                   |
|    |----> Episode                                    |
|    `----> Procedure Candidate --> Active Procedure   |
|                                                      |
|  Version / Bi-temporal / Provenance / Forget / Trace|
+------------------------------------------------------+
             |             |              |
             v             v              v
         Extractor     Retriever      Consolidator
          plugins       plugins         plugins
             |             |              |
             +-------------+--------------+
                           |
                           v
              MemoryBundle + RetrievalTrace
```

架构分为四个平面：

| 平面 | 责任 | 是否进入默认安装 |
| --- | --- | --- |
| Protocol Plane | 数据契约、插件清单、能力协商和兼容性 | 是 |
| Data Plane | 写入、状态、检索、预算、删除和审计 | 是 |
| Evolution Plane | 巩固、候选、评测、晋升和回滚 | 可选 |
| Operations Plane | 健康、回放、指标、迁移、资源和发布验收 | 基础能力轻量启用，高级能力可选 |

## 4. 核心领域模型

### 4.1 Event

Event 是唯一原始证据入口，保存来自宿主的生命周期事实。Event 采用追加式、幂等写入，至少包含：

```text
event_id
event_type
occurred_at
recorded_at
scope
trace_id / run_id / session_id
actor
payload or payload_ref
content_hash
trust_label
```

大体积工具输出优先保存受访问控制的引用、摘要和哈希，避免把完整日志复制进热存储。凭据、Token、私钥和受限个人信息必须在持久化之前按宿主策略脱敏。

### 4.2 Claim 和 Current State

Claim 表示从证据中得到的结构化事实、偏好或状态。一个精确分区和 claim key 同时最多存在一个 active 版本。新事实通过 supersession 替代旧事实，不进行无审计覆盖。

Claim 应逐步支持双时间语义：

```text
valid_from / valid_to          # 事实在现实世界中的有效时间
recorded_at / superseded_at    # 系统知道及替换事实的时间
```

Current State 是 active Claim 的受控投影，不是独立事实源。

### 4.3 Episode

Episode 表示有明确开始、结束和结果的任务片段。它可以包含成功、失败、取消、超时或未知结果，但未知不能转换成失败或零分。

Episode 必须保留来源事件、使用过的记忆、决策、工具结果、评价版本和适用范围。摘要只用于降低检索成本，不能取代原始来源。

### 4.4 Procedure

Procedure 是可复用的程序记忆。所有自动提炼结果首先进入 Procedure Candidate，并使用以下生命周期：

```text
candidate -> evaluated -> shadow -> canary -> active
active -> superseded / rolled_back / archived
failed evaluation -> rejected or revised candidate
```

状态迁移由版本化策略和宿主审批控制。Memory 插件保存状态和证据；真实工具执行、流量分配和外部副作用由宿主完成。

### 4.5 Prospective Intent

未来条件触发的意图与历史事实不同，但 v4 不立即把它加入稳定 Core。近期先定义可选扩展记录：

```text
intent_id
trigger_type: time | event | state
trigger_expression
valid_until
required_scope
status
source_event_ids
```

只有经过单独评测后，Prospective Intent 才能成为稳定协议的一部分，避免过早扩张核心模型。

## 5. Plugin Protocol v1

### 5.1 插件类别

首版协议只定义六类插件，避免过度拆分：

| 类型 | 输入 | 输出 | 责任 |
| --- | --- | --- | --- |
| CaptureAdapter | 宿主生命周期事件 | EventDraft | 适配 Agent 框架，不解释记忆语义 |
| Extractor | Event | MemoryProposal | 提取 Claim、Episode 或 Procedure 候选 |
| Retriever | QueryContext | RetrievalCandidate | 提供一个检索通道的候选 |
| Consolidator | 有界证据集合 | EvolutionProposal | 去重、冲突、抽象和整理提案 |
| StorageProvider | Kernel 命令 | 持久化结果 | 实现统一存储合同 |
| Evaluator | 候选、快照和评价配置 | EvaluationReport | 运行可复现评测或接收外部评测 |

插件只产生草稿、候选或提案。Kernel 负责最终校验、授权、提交、排序、裁剪和状态迁移。

### 5.2 Manifest

每个插件必须公开机器可读清单：

```yaml
plugin_api: 1
name: agent-memory-hybrid
version: 0.1.0
kind: retriever
capabilities:
  - lexical_search
  - semantic_search
  - temporal_search
requires:
  core: ">=0.2,<1.0"
config_schema: {}
resource_limits:
  timeout_ms: 500
  max_candidates: 100
failure_mode: fallback
```

Manifest 必须声明协议版本、能力、配置 Schema、依赖、资源上限和失败模式。Kernel 在初始化时执行能力协商，不通过运行时猜测插件功能。

### 5.3 生命周期

逻辑接口为：

```python
class MemoryPlugin:
    def manifest(self) -> PluginManifest: ...
    async def initialize(self, context: PluginContext) -> None: ...
    async def health(self) -> HealthStatus: ...
    async def close(self) -> None: ...
```

插件通过 Python Entry Points 延迟发现。仅导入核心包时，不加载数据库驱动、Agent 框架、模型 SDK、向量运行时或图数据库客户端。

### 5.4 权限和失败边界

| 情况 | 行为 |
| --- | --- |
| Extractor 失败 | Event 已持久化，跳过派生并记录可重试状态 |
| 可选 Retriever 失败 | 使用当前状态和确定性检索降级，不阻断 Agent |
| Consolidator 失败 | 保留现有记忆，不提交部分提案 |
| Evaluator 失败 | 候选保持原状态，不允许晋升 |
| Scope、授权或证据校验失败 | Fail closed，拒绝操作 |
| Forget 传播失败 | 报告不完整删除状态，不得宣称删除完成 |

插件调用需要超时、候选上限、批大小、并发上限和取消机制。第三方插件返回的数据始终视为不可信输入。

## 6. 写入路径

```text
Host Lifecycle Event
        |
        v
Capture Adapter
        |
  normalize / redact / bound
        |
        v
Kernel validation
        |
        v
Append Event and commit
        |
        +--> synchronous deterministic extraction when cheap
        |
        `--> bounded async enrichment queue
                    |
                    v
           Claim / Episode / Procedure proposals
                    |
             policy and evidence validation
                    |
                    v
              commit + index update
```

写入路径遵守以下原则：

- 先提交证据，再调用模型或重型插件；
- 宿主显式声明优先于自动提取结果；
- 自动提取必须记录 provider、model、prompt/schema 和 extractor 版本；
- 重复事件和重复提案必须产生一致结果；
- 异步任务按作用域限流并具备背压、检查点和重启恢复；
- 索引更新失败不回滚已经安全提交的原始证据。

## 7. 检索路径

```text
Query + Trusted Scope + Budget
        |
        +--> Current State
        +--> Lexical Retriever
        +--> Semantic Retriever (optional)
        +--> Temporal Retriever
        +--> Entity Retriever (optional)
        +--> Episode Retriever
        `--> Procedure Retriever
                    |
                    v
            reciprocal rank fusion
                    |
        scope / trust / validity recheck
                    |
        conflict / diversity / deduplication
                    |
             hard token-budget packing
                    |
                    v
       MemoryBundle + RetrievalTrace
```

默认检索是一次、并行、确定性和有预算的。RRF 用于融合不同插件的排名，避免要求各通道共享同一分数尺度。

只有出现证据覆盖不足、时间条件不明确或冲突未解决时，宿主才能显式允许有界的第二轮检索。Agentic Retrieval 不作为默认热路径。

RetrievalTrace 至少记录候选来源、插件和策略版本、过滤原因、裁剪原因、最终条目及预算使用。返回一条记忆不等于 Agent 实际使用了该记忆；使用关系必须由 DecisionRecord 或宿主反馈确认。

## 8. 巩固与记忆进化

### 8.1 实时路径

实时路径只处理对下一次决策必要的状态更新。不得在 Agent 热路径中运行全库聚类、图重建、长上下文反思或模型训练。

### 8.2 后台路径

```text
bounded Event window
    -> deduplicate
    -> episode segmentation
    -> contradiction detection
    -> claim merge or supersession proposal
    -> procedure induction proposal
    -> evaluation
    -> governed promotion
```

后台任务必须支持任务幂等、检查点、取消、暂停、批处理、重试上限和死信原因。每个派生物保留完整来源，权限不能宽于其来源。

### 8.3 反馈闭环

```text
RetrievalTrace
    -> DecisionRecord
    -> OutcomeEvent
    -> EvaluationRecord / RewardSignal
    -> attribution
    -> EvolutionProposal
    -> replay evaluation
    -> shadow / canary / approval
    -> active version or rollback
```

Reward 定义权属于宿主。模型自评、用户评价、确定性测试和人工审批必须使用不同来源标签，不能静默合并成同一可信等级。

## 9. 存储与索引

### 9.1 权威数据和派生索引

权威数据包括 Event、Claim、Episode、Procedure、反馈、版本、生效指针和审计记录。以下内容均为派生索引：

- BM25 或全文索引；
- embedding；
- 向量索引；
- entity index；
- temporal index；
- knowledge graph；
- topic 或聚类摘要。

派生索引必须携带 source version、extractor version 和 index version，并能从权威数据重建。删除来源时必须使所有相关派生索引失效或删除。

### 9.2 部署档位

| Profile | 组成 | 目标场景 |
| --- | --- | --- |
| minimal | Core + SQLite + deterministic/lexical retrieval | 本地 CLI、嵌入式和单 Agent |
| smart | minimal + 外部 embedding/LLM + hybrid retrieval | 普通生产 Agent |
| scale | PostgreSQL/pgvector + worker + observability | 多 Agent、团队和服务端 |
| graph | scale + optional graph retriever | 关系和复杂时间查询密集场景 |

不得把本地 embedding 或 reranker 模型加入默认 Profile。使用外部模型时必须设置超时、费用、批量和降级策略。

## 10. 轻量化约束

基础包继续保持零第三方运行依赖。轻量化不只衡量安装包大小，还包括：

- 冷启动时间；
- 空闲和峰值 RSS；
- 每次写入的同步延迟；
- 每次检索的候选扫描量；
- Token 输出上限；
- 后台队列容量；
- 数据库和索引增长率；
- 每个插件的模型调用次数和费用。

所有集合读取、队列、缓存、候选、批次和并发必须有硬上限。尚未在固定环境测量前，不承诺固定 MB 或延迟指标。

## 11. 安全、隐私与删除

安全边界遵循：

```text
Host authenticates identity
    -> Host derives Trusted Scope
    -> Kernel authorizes operation
    -> Plugin receives minimum required scoped data
```

模型和插件不能自行指定更宽的 tenant、user 或 agent scope。跨租户学习默认关闭，启用时需要单独授权、匿名化策略和可审计的数据许可。

Forget 流程为：

```text
forget request
    -> authorization
    -> delete or tombstone source evidence
    -> invalidate claims, episodes, procedures and feedback
    -> invalidate lexical/vector/entity/graph indexes
    -> produce deletion audit result
```

如果外部备份、缓存、向量服务或图服务未完成删除，不得返回完整成功。训练数据删除与模型遗忘是独立问题，已训练制品需要标记受影响并由宿主决定停用或重训。

## 12. 从公开方案吸收什么

| 方案 | 吸收的机制 | 不吸收的部分 |
| --- | --- | --- |
| Mem0 | 简单接入、自动提取、可替换检索能力 | 不把托管平台语义写入 Core |
| Graphiti | 时间有效窗口、来源 Episode、增量更新、图候选检索 | 不强制 Neo4j/FalkorDB，不让图成为事实源 |
| Hindsight | retain/recall/reflect 分层、并行多信号检索、作用域和运维思路 | 不复制 world/experience/observation/opinion 为第二套模型 |
| Letta | 热上下文与外部记忆分层、后台整理、版本快照 | 不引入完整 Agent Harness 或允许无治理自我改写 |
| Cognee | 可组合处理管线、后台索引和知识结构 | 不把文档知识平台作为基础依赖 |
| MemOS | Agent 无关 Core、适配器边界、轨迹到策略候选 | 不引入 Memory OS、模型权重或复杂调度进入 Core |

参考证据：

- Mem0：https://github.com/mem0ai/mem0
- Graphiti：https://github.com/getzep/graphiti
- Hindsight：https://github.com/vectorize-io/hindsight
- Hindsight ACL 2026：https://aclanthology.org/2026.acl-demo.27/
- Letta Code：https://github.com/letta-ai/letta-code
- Cognee：https://github.com/topoteretes/cognee
- MemOS local plugin：https://github.com/MemTensor/MemOS/tree/main/apps/memos-local-plugin
- Agent Memory systems characterization：https://arxiv.org/abs/2606.06448
- AgeMem：https://aclanthology.org/2026.acl-long.981/
- Memory-R1：https://aclanthology.org/2026.acl-long.583/
- PM-Bench：https://arxiv.org/abs/2607.12385

## 13. 包边界

现有包继续保留：

| 包 | 责任 |
| --- | --- |
| agent-memory | 协议、领域模型、Kernel、SQLite 和确定性回退 |
| agent-memory-postgres | PostgreSQL/pgvector Provider |
| agent-memory-mcp-server | MCP 传输和工具暴露 |
| agent-memory-python-sdk | 客户端门面 |
| agent-memory-langgraph | LangGraph 生命周期适配 |
| agent-memory-evolution | 候选、评价、晋升和回滚 |

近期只新增三个包：

| 包 | 责任 |
| --- | --- |
| agent-memory-capture | 通用生命周期事件和框架 Capture Adapter |
| agent-memory-hybrid | lexical、semantic、temporal、entity 候选与 RRF |
| agent-memory-evals | 回放、基准、数据快照和插件一致性评测 |

`agent-memory-graph` 和 `agent-memory-intent` 推迟到上述三个包完成并取得评测证据后再建立。

## 14. 实施顺序

### V4-P0：协议冻结

- 定义 Plugin Manifest、生命周期、错误码和 capability negotiation；
- 为六类插件提供 Protocol 和最小参考实现；
- 提供第三方插件 contract test kit；
- 定义 SemVer 和兼容性策略。

退出条件：不安装任何可选包时 Core 行为不变；错误插件不能绕过 scope、budget 或 provenance。

### V4-P1：自动轨迹采集

- 定义通用 LifecycleEvent envelope；
- 实现 bounded capture queue、脱敏、截断和幂等；
- 复用 LangGraph、MCP 和 Python SDK 接入；
- 验证插件故障不阻断宿主 Agent。

退出条件：完整任务能自动形成 Event、Decision、Outcome 和可关联反馈，重启后不丢失已确认事件。

### V4-P2：混合检索

- 完成本地 lexical baseline；
- 接入可选 embedding、temporal 和 entity retriever；
- 实现并行候选、RRF、重复和冲突处理；
- 记录完整 RetrievalTrace 和 index version。

退出条件：关闭任一可选通道仍可检索；越权、失效或无来源候选永远不能进入 MemoryBundle。

### V4-P3：异步巩固

- 实现 Episode segmentation、Claim consolidation 和 Procedure induction worker；
- 完成背压、检查点、取消、重试和死信；
- 保证删除传播和来源权限不扩大。

退出条件：worker 中断可恢复；重复执行结果幂等；Agent 热路径不依赖 worker 可用性。

### V4-P4：评测与发布门

- 建立 no-memory、fixed-policy 和 candidate-policy 基线；
- 分别报告构建、检索和生成阶段成本；
- 加入准确率、证据覆盖、时间正确性、删除完整性、延迟、Token 和 RSS 指标；
- 为插件发布建立兼容性、资源、安全和重复运行验收。

退出条件：每项高级能力都有可复现收益和资源成本；未完成、失败和跳过不能报告为通过。

### V4-P5：可选高级能力

- 双时间实体关系和 Graph Retriever；
- Prospective Intent；
- Memory Doctor 和污染诊断；
- 快照迁移、团队共享和长期运行治理。

退出条件：每项能力可单独安装、关闭、迁移和删除，不破坏最小 Profile。

### V4-P6：Agent Evolution 扩展

- DatasetExport 和训练数据许可；
- 学习型 Memory Action Policy；
- Prompt、Harness 和模型候选；
- 单 Agent shadow/canary 运行编排；
- Agent 版本组合发布和回滚。

退出条件：训练和编排停机时基础记忆仍正常；宿主审批、权限、预算和独立留出评测不可绕过。

## 15. 验收矩阵

| 维度 | 必须证明 |
| --- | --- |
| 正确性 | 幂等、乱序、更正、冲突、替代和并发状态正确 |
| 插件兼容 | Manifest 校验、能力协商、版本冲突和延迟发现正确 |
| 降级 | 模型、向量、图、worker 或网络不可用时最小功能可用 |
| 隔离 | 所有插件和存储后端执行相同 Trusted Scope 规则 |
| 证据 | 每个返回和派生对象都能追溯有效来源 |
| 删除 | 权威数据、派生物和外部索引删除状态明确 |
| 预算 | 条目、字符、Token、候选、队列、缓存和并发均有硬限制 |
| 资源 | 固定 Python、数据量和并发下报告冷启动、RSS、延迟和存储增长 |
| 进化治理 | 未评价或未批准候选不能进入 active，回滚可恢复兼容版本 |
| 发布 | 源码与制品扫描、构建、安装和真实后端测试可复现 |

## 16. 架构决策记录

### ADR-V4-01：Core 不依赖向量或图

接受。它们提高部分查询的召回能力，但不是正确维护当前状态、证据和删除的必要条件。

### ADR-V4-02：Graph 是 Retriever，不是第二数据库真相

接受。关系和时间图是派生索引，必须能从 Event、Claim 和 Episode 重建。

### ADR-V4-03：默认检索不使用自主多轮 Agent

接受。一次并行检索更容易限制延迟、成本和失败范围。有界第二轮由明确策略触发。

### ADR-V4-04：插件不能直接提交 active 对象

接受。插件输出 Proposal，Kernel 执行最终证据、权限和生命周期校验。

### ADR-V4-05：训练与运行编排最后实现

接受。先建立可信轨迹、反馈、快照和评测，避免用不可靠记忆训练或发布 Agent。

## 17. 近期交付定义

V4 的近期完整目标不是 Graph 或 Latent Memory，而是：

```text
Plugin Protocol v1
    -> automatic capture
    -> hybrid bounded retrieval
    -> asynchronous consolidation
    -> replay and evaluation
```

完成以上链路后，Agent Memory 才具备面向第三方发布的稳定插件基础。Graph、Prospective Memory 和训练型策略只能以可选包继续演进。
