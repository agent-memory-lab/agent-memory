# Agent Memory V4 执行任务

日期：2026-09-14

状态：T19-T43 待执行。勾选仅表示实现与对应验收证据均已完成。

计划依据：[PLAN_V4.md](PLAN_V4.md)。目标架构见 [AGENT_MEMORY_PLUGIN_ARCHITECTURE_V4.md](AGENT_MEMORY_PLUGIN_ARCHITECTURE_V4.md)。

## 状态规则

- `[ ]`：未开始或没有充分验收证据；
- `[~]`：部分完成，但退出条件尚未全部满足；
- `[x]`：实现、测试、文档和必要迁移全部完成；
- `BLOCKED`：存在可复现的外部阻塞，并记录原因；
- 不允许因为代码存在、单个测试通过或人工观察正常就标记完成。

## M4：Plugin Protocol v1

### T19 协议差距审计

- [ ] 清点现有 Provider、Adapter、Extractor、Embedder、Reranker 和 Evolution 入口。
- [ ] 将现有接口映射到六类 V4 插件，识别重复或越界能力。
- [ ] 记录兼容性、迁移和废弃策略。
- [ ] 明确哪些已有行为必须保持不变。

验收：形成可追踪的差距报告；每个现有入口都有保留、适配、废弃或迁移结论。

### T20 Plugin Manifest 和错误模型

- [ ] 实现 `PluginManifest` 及 Schema 校验。
- [ ] 定义 plugin API、kind、capabilities、requires、resource limits 和 failure mode。
- [ ] 定义稳定错误码和 fail-open/fail-closed 分类。
- [ ] 拒绝未知 kind、重复 capability、无效版本范围和不安全资源配置。

验收：有效和无效 Manifest 均有测试；错误能被 SDK/MCP 稳定序列化。

### T21 六类插件协议

- [ ] 定义 CaptureAdapter Protocol。
- [ ] 定义 Extractor Protocol。
- [ ] 定义 Retriever Protocol。
- [ ] 定义 Consolidator Protocol。
- [ ] 统一或适配 StorageProvider Protocol。
- [ ] 定义 Evaluator Protocol。
- [ ] 定义受限 `PluginContext` 和生命周期接口。

验收：Core 不导入可选依赖；示例实现通过静态和运行时合同检查。

### T22 Loader 和 Capability Negotiation

- [ ] 扩展 Entry Points 延迟发现。
- [ ] 实现版本兼容、能力协商、重复注册和优先级规则。
- [ ] 实现初始化失败回滚和逆序关闭。
- [ ] 健康状态区分 unavailable、degraded 和 ready。

验收：损坏、超时、冲突和不兼容插件不能破坏 Core 启动；未安装插件不产生导入副作用。

### T23 Contract Test Kit

- [ ] 建立第三方插件统一测试夹具。
- [ ] 覆盖生命周期、超时、取消、容量、scope、provenance 和关闭行为。
- [ ] 提供每类插件的最小 reference implementation。
- [ ] 编写第三方插件开发文档和兼容矩阵。

验收：独立示例包只依据公开 API 即可通过合同测试。

## M5：Automatic Capture

### T24 LifecycleEvent Envelope

- [ ] 定义 turn、message、tool、decision、outcome、evaluation 和 reward 事件。
- [ ] 定义 event ID、trace/run/session、actor、时间、scope 和 payload 引用。
- [ ] 明确宿主字段与模型生成字段的信任差异。
- [ ] 提供向现有 Event 的确定性映射。

验收：Schema 支持向前兼容；缺失可信 scope 的事件被拒绝。

### T25 Capture 过滤和幂等

- [ ] 实现 payload 大小、事件数量和嵌套深度限制。
- [ ] 实现敏感字段过滤接口和默认保守规则。
- [ ] 实现 content hash、幂等键和重复提交结果。
- [ ] 支持大工具结果使用引用而非完整复制。

验收：重复事件不产生重复有效证据；敏感样例不会进入持久层。

### T26 Bounded Capture Queue

- [ ] 定义队列容量、背压、超时和溢出策略。
- [ ] 实现提交确认、检查点、恢复和终止原因。
- [ ] 按 Trusted Scope 隔离队列和容量。
- [ ] 确保已确认 Event 不因后续提取失败丢失。

验收：重启、满队列、重复和消费者失败场景均可解释且不越权。

### T27 框架接入

- [ ] Python SDK 接入 Capture API。
- [ ] LangGraph 生命周期适配器接入。
- [ ] MCP 工具或通知接入。
- [ ] 提供自定义 Agent Harness 的最小示例。

验收：同一轨迹经不同 Adapter 产生语义一致的标准事件。

### T28 Capture 复合验收

- [ ] 完整 turn 到反馈关联的端到端测试。
- [ ] 插件异常和超时故障注入。
- [ ] 并发、乱序、更正和重启恢复测试。
- [ ] Capture 关闭时现有手动 remember/feedback 路径不变。

验收：Capture 故障不阻断宿主 Agent；已提交证据可查询、追踪和删除。

## M6：Hybrid Retrieval

### T29 Lexical Baseline

- [ ] 冻结确定性 lexical 排序和 tie-break 规则。
- [ ] 支持 Current State、Episode 和 Procedure 通道。
- [ ] 限制扫描窗口、候选数和查询长度。
- [ ] 记录候选来源和 baseline policy version。

验收：SQLite/PostgreSQL 固定数据集返回一致的确定性结果。

### T30 Semantic Retriever Contract

- [ ] 定义 Embedder 和 Semantic Retriever 的组合边界。
- [ ] 记录 model、dimension、normalization 和 index version。
- [ ] 处理维度变化、模型切换、缺失向量和重建状态。
- [ ] 不提供默认内置模型。

验收：远程 provider 超时或索引不兼容时自动降级且结果可解释。

### T31 Temporal 和 Entity Retriever

- [ ] 实现双时间过滤的最小 Temporal Retriever。
- [ ] 实现可选 Entity 候选接口，不引入强制图数据库。
- [ ] 处理当前、历史、未知和冲突时间查询。
- [ ] 保证派生实体权限不宽于来源。

验收：当前事实和历史事实不会因语义相似度而互相覆盖。

### T32 并行候选和 RRF

- [ ] 并行执行启用的 Retriever。
- [ ] 每个通道使用独立超时、候选上限和取消。
- [ ] 实现 RRF、确定性 tie-break 和通道配额。
- [ ] 避免因同一来源命中多个索引而重复返回。

验收：不同完成顺序不改变最终排序；单通道故障不阻断安全结果。

### T33 最终过滤和 MemoryBundle 打包

- [ ] 融合后重新验证 scope、trust、validity 和 provenance。
- [ ] 实现冲突、重复、多样性和过期处理。
- [ ] 强制执行条目、字符、Token 和通道预算。
- [ ] RetrievalTrace 记录过滤、降级和裁剪原因。

验收：无来源、越权、已删除、已过期候选在任何插件路径都无法进入 Bundle。

### T34 Hybrid Retrieval 评测

- [ ] 建立 no-memory、lexical-only 和 hybrid 数据集。
- [ ] 测量证据召回、时间正确性、冲突正确性、延迟和 Token。
- [ ] 覆盖模型、网络、索引和单通道失败。
- [ ] 记录可选能力带来的资源增量。

验收：Hybrid 相对 baseline 的收益和代价可复现；无收益能力不进入默认建议配置。

## M7：Async Consolidation

### T35 Worker Runtime

- [ ] 定义 Provider 无关的任务、租约、检查点和状态模型。
- [ ] 实现容量、批次、并发、暂停、取消和重试上限。
- [ ] SQLite 提供最小本地执行器。
- [ ] PostgreSQL 复用现有队列和事务能力。

验收：Worker 停机不影响基础 API；中断恢复不重复提交有效结果。

### T36 Episode Segmentation

- [ ] 根据 run/session/terminal outcome 形成 Episode Proposal。
- [ ] 区分成功、失败、取消、超时和未知。
- [ ] 关联 RetrievalTrace、Decision 和使用过的记忆。
- [ ] 限制摘要大小并保留原始来源。

验收：乱序和更正反馈最终形成一致 Episode；未知不计为失败或成功。

### T37 Claim Consolidation

- [ ] 合并重复来源而非复制 Claim。
- [ ] 生成冲突和 supersession proposal。
- [ ] 使用双时间和 trust 规则处理新旧事实。
- [ ] 删除来源后重新计算或失效派生 Claim。

验收：Consolidator 不能绕过唯一 active、scope 或证据要求。

### T38 Procedure Induction

- [ ] 从多条有结果 Episode 生成 Procedure Candidate。
- [ ] 保存适用条件、反例、提取器版本和证据范围。
- [ ] 样本不足、评价不兼容或结果未知时拒绝提炼。
- [ ] 只进入 candidate，不直接晋升。

验收：失败经验和适用边界被保留；候选生命周期继续受 Evolution 策略控制。

### T39 Consolidation 可靠性验收

- [ ] 重复执行、崩溃恢复、锁争用和死信测试。
- [ ] Forget 与运行中任务竞态测试。
- [ ] 跨 scope 队列和来源权限测试。
- [ ] 固定负载下测量热路径和后台峰值资源。

验收：无部分生效、无权限扩大、无删除后重新生成，资源上限可执行。

## M8：Evals and Release Gates

### T40 Snapshot 和 Replay

- [ ] 定义版本化数据快照清单。
- [ ] 记录数据许可、删除状态、时间范围和 train/eval/test 切分。
- [ ] 实现固定 Clock、ID 和插件版本下的 deterministic replay。
- [ ] 支持分页或流式导出，不复制大型制品到 Core。

验收：相同快照和版本产生可比较结果；删除来源会标记相关快照受影响。

### T41 Benchmark Harness

- [ ] 实现 no-memory、fixed-policy 和 candidate-policy 对照。
- [ ] 定义事实、时间、冲突、Episode、Procedure 和 prospective 扩展指标。
- [ ] 支持外部公开数据适配器，但不把数据集打入 Core 包。
- [ ] 输出机器可读和人类可读报告。

验收：报告包含样本量、置信信息、版本、失败、跳过和不可比较原因。

### T42 Phase-aware Resource Evaluation

- [ ] 分别测量 construction、retrieval 和 generation 阶段。
- [ ] 测量冷启动、RSS、延迟、Token、模型调用、队列和存储增长。
- [ ] 对 minimal、smart 和 scale Profile 分开报告。
- [ ] 固定 Python 3.13、数据规模、并发和硬件说明。

验收：重复运行结果处于记录的容差范围；未实测配置不宣称资源指标。

### T43 V4 发布验收

- [ ] 执行全部 Unit、Contract、Fault、Integration、Replay 和 Resource 测试。
- [ ] 在真实 SQLite 和 PostgreSQL 上验收。
- [ ] 构建并安装 Core 与所有可选包。
- [ ] 扫描源码、文档、配置和发布制品中的敏感信息。
- [ ] 核对 README、设计、API、能力矩阵和实际实现。

验收：零未说明失败、零未说明跳过、零敏感信息命中；所有发布声明有对应证据。

## 后续任务池

以下任务不进入 T19-T43 完成范围：

- Graph Retriever 和图后端；
- Prospective Intent 正式协议；
- Memory Doctor；
- 学习型 Memory Action Policy；
- Agent 训练、Prompt/Harness 候选和运行编排；
- Latent、KV Cache 和参数记忆。

只有 T43 完成且取得基线收益、资源和安全证据后，才拆分下一阶段任务。

## 执行记录

| 日期 | 任务 | 状态 | 证据 | 下一步 |
| --- | --- | --- | --- | --- |
| 2026-09-14 | T19-T43 | 待执行 | V4 设计、计划和任务清单 | 从 T19 协议差距审计开始 |

