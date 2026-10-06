# v6.1.0 实现差距审计与首批范围

| 项目 | 值 |
| --- | --- |
| 审计任务 | AM61-T01 |
| 日期 / 修订 | 2026-10-06 / 1 |
| 规范 | [设计 v6.1.0](../AGENT_MEMORY_DESIGN_V6.1.0.md)、[plan](plan.md)、[task](task.md) |
| 设计 SHA-256 | `ece08b05d076d2741aa70bb45293b46d5008aaa312dffdb0bd18f01b8a7bf61b` |
| 原设计代码基线 | `5cff9f47b7061351fd725c3b8e0a89bf5cb51517` |
| 本次工作区基线 | `7a382d9`；相较代码基线的后续提交为设计文档提交 |
| 本批实现 | 尚未提交的首批开发差异，具体文件与验证见 [batch-01](batch-01.md) |

本审计记录第一批边界；后续增量见 [第二批原子接收](batch-02.md) 和 [next-steps](next-steps.md)。
本审计完成代码、入口、责任和运行边界盘点，不表示 44 项开发任务或 66 项设计验收完成。
未启用的目标能力仍为设计；旧能力继续按既有协议运行。本批没有数据库迁移，没有向 capability manifest 宣称完整 v6.1 支持。

## 1. 现有入口与关键调用边界

以下路径相对仓库根目录；与本次变更无关的既有细节以代码为准。

| 入口 | 当前链路 / 持久化边界 | 本轮结论 |
| --- | --- | --- |
| `runtime.py:AgentMemory` | remember/recall/state/forget 委托 Provider；`remember_atoms`、`extract_atoms` 进入 Kernel 接纳入口 | 旧门面已有；不接受本批新增宿主 CaptureProfile 参数 |
| `capture/api.py:submit_capture` | SDK/MCP 的可信 scope、actor 覆盖输入身份，解析 LifecycleEvent 后交给 sink | 新增拒绝正文 payload 中伪造的保留 capture namespace |
| `capture/api.py:submit_profiled_capture` | 宿主独立提供 profile/observation，校验角色、事件类型、时间、声明的内容及省略；复用现有 sink | 本批新增宿主 Python API；它不是开放 MCP 工具参数 |
| `capture/sink.py`、`capture/queue.py` | Direct 同步入 Provider；Queued 在独立 SQLite outbox 接收，worker 再交付 Provider | 已有队列重试；不是 L0 和后续处理请求同事务 |
| `kernel.py:ingest_event` | 旧提取器先执行，随后事务保存 Event/Claims，提交后再调 consolidation scheduler | 普通旧事件仍如此；带回流/派生标记的事件仅保存 L0，不提取或自动调度 |
| `consolidation/atom_extraction.py:process` | 幂等查询 → generator/reviewer（事务外）→ AdmissionEngine 最终事务 | 来源尚未先于模型持久化；崩溃后仍可能重复外部调用，T16/T21 未实现 |
| `consolidation/admission_runtime.py:admit` | 范围锁 → 幂等核对 → Event、候选、决定、Claim 及版本共同提交 | 已有有限 Atom 闭环；本批拒绝上下文回流，并保留有界 capture 注记、纳入幂等指纹 |
| `retrieval/`、`kernel.py:retrieve` | Provider 双时态路径、混合通道、守卫、融合、预算与 bundle | 旧功能复用；本批评分器没有接管真实读取或 ACL 判定 |
| `kernel.py:forget`、`operations/deletion_audit.py` | Provider 删除、现有依赖传播、审计；Atom 采用保守同槽撤下策略 | 不等同新设计的入口 epoch、全 processing 依赖及派生历史删除协议 |
| `packages/python-sdk/` | Embedded 与 MCP client 经可信上下文调用现有工具门面 | 旧协议回归；profiled API 尚未提供 SDK 便利封装 |
| `packages/mcp-server/`、`src/agent_memory/mcp.py` | MCP tool/resource → trusted request context → core；包含 HTTP/stdio 与恢复入口 | 旧工具共享保留命名空间拒绝；不能让模型自行声明宿主观察/权限 |
| `packages/langgraph/` | 框架适配、恢复、捕获 sink 注入 | 现有适配回归；稳定多块 profile、游标与阶段就绪仍待接入 |
| `packages/evolution/` | 可选评测、发布、恢复与演化记录 | 既有能力；不默认提供新的 Observation/Reflect/Procedure 自动演化 |

返回正文的路径不仅包括 recall，还包括 state/history、Block、原文/工件、恢复上下文、诊断及 SDK/MCP 序列化。
T13/T23/T28 后续必须统一检查这些出口；本批 capture 注记只限制接纳资格，不是完整处理授权或交付授权。

## 2. 设计 §2 基础能力核对

| 目标能力 | 当前证据 / 状态 | 缺口与责任 |
| --- | --- | --- |
| 插件、Provider 与宿主集成 | `domain.py`、`ports.py`、`extensions/`；既有协议与架构测试 | T02 新 ID/阶段 token/能力依赖及旧协议投影 |
| Claim 双时态 | SQLite、PostgreSQL admission/temporal_history；`test_bitemporal_claims`、`test_atom_temporal` | T10 独立终止依据、未知/粗粒度时间；T28 派生历史；T40 规模化 |
| 显式 Atom 接纳 | `admission.py`、`admission_runtime.py`；`test_atom_admission`、`test_atom_edges` | T07 类型/实体/槽，T09 字段级支持，T11 范围合成，T12 跨槽更正 |
| 自动抽取与审查 | `atom_extraction.py`、`extraction_rules.py`；`test_atom_extraction` | T08 跨 turn 与条件保真；T13/T21/T25/T36 首次严格模型接入前的依赖 |
| 原文语义与价值判断 | AtomReview 的 faithfulness、retention 分离，来源片段检查已有 | 缺真实领域校准与风险路由，归 T03/T08/T38 |
| 原始来源与最终结果事务 | 最终 admission unit of work 已有；SQLite/PostgreSQL 合同回归 | T16 生成前 L0+任务原子接收；不能以当前最终事务冒充 |
| Worker/租约/重试 | `operations/sqlite_worker_queue.py`、`worker_runtime.py`、PostgreSQL `worker.py` | T17 producer 游标；T18 requested/claimed 水位与资源串行化；T20 重处理协议 |
| 混合检索与守卫 | `retrieval/governed.py`、fusion、bundle；`test_retrieval_trace` | T34 精确证据跨阶段关联；T35 用途路由；T36 模型空间/输入；T37 可选 Reflect |
| Block/Episode/Procedure | `consolidation/`、`ontology/`、`context/recovery.py` 与演化包 | T25–T33 新依赖、facet、失效、定义代次与 full/delta 刷新；T43 可选演化 |
| Observation、自动 L2/L3 | 无本设计完整链路；既有 Block 不能直接算作实现 | T25–T33；当前关闭/不声明新能力 |
| 外部领域核验 | 宿主可提供新 evidence，现有 Atom 资格处理 | T09 字段、时间和同业务版本支持；T19 获准只读核验任务 |
| 擦除与恢复 | 原有 forget/deletion audit/recovery，Atom 保守屏障 | T15/T23 入口 epoch 与全路径删除；T24 双后端恢复；T42 可选 Portable Transfer |
| 捕获 | 原有有界清洗/outbox；本批新增 profile、三值完整性、上下文回流标记 | T06 多块/适配器、原始 query 区分、谱系存在性验证仍未完成 |
| 评测与发布 | 旧 V4 flat-source benchmark/release 保留；本批新增精确 span、AND/OR、冻结质量门 | T03 真实领域 gold；T04 新竞态夹具；T05 校准；T38 实测与消融；T39 完整发布组合 |

## 3. R/N 组合契约差距与运行模式

| 契约 | 本批模式 / 已有证据 | 仍需实现 / 主责 |
| --- | --- | --- |
| R01 范围解释 | 旧 scope 隔离；gold 按已标注查询 scope 判资格 | 无跨适用域合成优先关系，T11 |
| R02 解释替换 | 旧显式更正，原子事务已有 | 缺 additive/reconcile 审查覆盖和删除竞争协议，T20 |
| R03 输入与交付 | 旧读取守卫；未启用严格外部模型能力 | 新输入/dispatch/delivery 凭证与当前安全顺序，T13 |
| R04 全输入依赖 | 本批回流仅携带 parent IDs 并限制资格 | 缺完整生成输入清单、输出受众计算和重新生成规则，T25 |
| R05 部分核验 | 评分器可判断指定时点 AND/OR、字段覆盖与来源族 | 不是运行时 EvidenceLink/SupportExpression，T09 |
| R06 状态终止 | 既有 Claim 双时态已回归 | 独立 TransitionDecision 与撤证后终止依据，T10 |
| R07 跨槽更正 | 不声明新能力 | 原子移除旧贡献/增加新贡献与前置版本，T12 |
| R08 query 屏障 | 无本设计 query dependency 闭环 | 新成员、查询截断、时间边界与登记屏障，T26 |
| R09 历史及当前安全 | 旧 Claim 历史已回归；评分器当前 readable 守卫 | 派生历史双守卫与实际所有出口，T28 |
| R10 入口生命周期 | 旧 scope 权限与队列幂等 | AdmissionTicket/producer epoch，T15 |
| R11 有限就绪 | 旧 capture pending/done、worker 状态 | capture/decision/publication/index 有限目标与闭合清单，T22 |
| R12 全链预算 | 旧请求/队列资源上限；评分器成本统计 | 原子多层 reservation、调用 outbox 与未知费用对账，T21 |
| R13 发布门 | 本批冻结 hash、样本数、覆盖/质量/成本/回归检查；全拒答硬阻断 | 真实数据阈值、能力依赖清单、运行安全/事务门完整集成，T39 |
| N01 捕获与回流 | 本批 text/tool_result profile、unknown coverage、阻止回流三条接纳路径 | 多块/附件/所有宿主 adapter 与父谱系查询验证，T06 |
| N02 producer 追加 | 原队列重试和同 ID 检查 | ack 水位、乱序、多生产者、旧 epoch，T17 |
| N03 合并调度 | 原队列 key 去重和 lease | 新请求不丢、resource key、generation fencing，T18 |
| N04 刷新水位 | 原有 Block 基础 | 定义指纹、full/delta、完整核验后推进水位，T31 |
| N05 分阶段就绪 | 原有限队列状态 | 复合发布 token 与可见性、视图就绪，T22/T33 |
| N06 证据归因 | 本批最小充分分支评分，旧检索 trace 回归 | 各阶段精确 span 观察、公开 trace 最小化，T34 |
| N07 模型合同 | 原 token counter、向量/精排适配器 | 实际 prompt 输入上限、score contract、空间身份，T36 |
| N08 冻结对照 | 17/20 合成场景已适配；CLI 评分测试可重复 | 真实适配器端到端、裁判复核、消融/统计，T38 |

T14/T24 分别承担语义与可靠运行会合验收；T29 失效传播；T30 视图定义；T32 L2/L3；T40 规模迁移；
T41 多模态；T42 Portable Transfer；T43 Procedure 演化；T44 持续运维交付。它们本轮均未整体实施。

## 4. 双后端与兼容边界

- SQLite：`src/agent_memory/sqlite.py` 及 admission unit of work、temporal_history；PostgreSQL：
  `packages/postgres/src/agent_memory_postgres/{repository,admission,temporal_history}.py`。
  本批 capture 元数据使用现有 Event JSON 存储，不增加第二套来源表或新队列。
- PostgreSQL 使用隔离的临时 PostgreSQL 17 数据库验证，非 mock。新共享 capture 测试检查显式接纳拒绝回流、
  提取前拒绝回流、正常确认可接纳、元数据保存、幂等来源修订变化拒绝；同时跑已有双后端合同。
- `LIFECYCLE_SCHEMA_VERSION`、软件包版本和已有数据库 schema 保持原值；新 capture 注记使用内部
  `schema_version=1`；评分器为 `evidence-support/1`，CLI 为 `agent-memory-evidence-run/1`。
- 旧数据缺失 capture_complete/source family/processing lineage 时保持未知；不自动回填。
  新 profile 消费端不能回滚为不识别 memory_context 的旧提取器：需暂停这类写入/重放，保留 L0。
- profile 注记可以随独立 Atom 接纳保存，但“先 capture 同一个 event，再原地把它升级为 Atom”仍不支持；
  原来源及任务关联需要 T16。不可复用既有幂等键绕过已经保存的原始事件。

## 5. 首批范围及下一步

首批是可复用的 P0 评测基础与 T06 采集切片：先准确计量和限制来源，再推进持久抽取。
复杂条件/例外、决定/计划、工具执行证据在合成 gold 中表达，尚未向运行时 PredicateSpec 宣称支持。
运行时仍仅使用已有已登记的 scalar fact/preference/constraint；`city`/`locale` 等测试谓词不代表生产领域泛化。

下一批先完成 T02 的 SourceRevision、AdmissionTicket、processing target/receipt 最小合同，随后实施 T15/T16，
让 L0 与处理请求先持久提交，再接入 T18 调度和已存在的 Atom 管线。外部模型严格能力仍需同时满足
T13/T21/T23/T25/T36 的实际切片。真实 gold、模型/裁判和质量门槛没有数据依据，继续标记待校准。

未进行真实模型实验、Hindsight 对比、线上流量、规模压测或新删除协议故障注入；不能据此宣称这些验收通过。
