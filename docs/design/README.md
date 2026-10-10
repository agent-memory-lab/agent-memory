# Agent Memory 设计版本索引

当前完整目标设计为 **[v7.0.0 统一架构与算法实施设计](AGENT_MEMORY_DESIGN_V7.0.0.md)**；
最新加性运行合同为 **[v7.1.0 抽取、核验与宿主](AGENT_MEMORY_DESIGN_V7.1.0.md)**，包含总体算法架构和数据流图。
状态为 `PROPOSED_BASELINE`：设计正文冻结；B0–B6 的有界工程能力已交付，真实领域质量与全成本验收仍开放。

v7 保留事实抽取/接纳、完整双时态、Hindsight 生命周期借鉴、L0–L3、可靠任务、权限删除与评测，
把问题视图、三类依赖、即时失效、合并调度、确定性增量、证明复用与模型精确缓存组织为统一算法。
正文有 32 章、40 条不变量、32 项 ADR、98 项必要验收规格，以及 10 个算法的输入/输出和实现责任。

直接阅读：[总体架构图](AGENT_MEMORY_DESIGN_V7.0.0.md#algorithm-architecture)、
[写入与刷新数据流](AGENT_MEMORY_DESIGN_V7.0.0.md#write-refresh-flow)、
[读取与模型调用数据流](AGENT_MEMORY_DESIGN_V7.0.0.md#read-flow)。正文另有事务时序及刷新状态图，共 5 张 Mermaid 图。
采用设计 MAJOR 是因为问题身份、内容/证书和有限覆盖目标的合同重组；既有来源/事实/时间/权限和已发布接口保持原义，新增合同 opt-in。

## 当前实施入口

- [v7.1 plan](v7.1.0/plan.md)、[task](v7.1.0/task.md)和[验证](v7.1.0/validation.md)：七项代码能力及实际模型运行；真实业务效果待输入，高层能力范围单独声明。

- [v7 plan.md](v7.0.0/plan.md)：B0–B6 批次、模块边界、依赖、迁移、度量与最终验收。
- [v7 task.md](v7.0.0/task.md)：revision 12；15 项新增任务中 11 项在声明范围 DONE、4 项 IN_PROGRESS，保留 32 项 Q7 主责任及 44 项旧任务状态。
- [最新运行时修复](v7.0.0/runtime-repairs.md)：缓存/结算一致性、显式授权审计归档与共享调度页面维护。
- [本地模型验证](v7.0.0/ollama-smoke.md)：真实 Qwen 9B 的合成运行验证；不替代许可领域和成本验收。
- [B6 工程验证](v7.0.0/batch-b6.md)：记录固定源码 3931 项全量通过；本轮新增入口仅运行专项及受影响回归。
- [文档验证](v7.0.0/design-validation.json)：正文指纹、继承、链接、图表与版本一致性；不是运行验收。

正文冻结时的核对基线仍为 `583e20d` 的[前序审计](v6.1.0/stage-15-audit.md)，其 2374 项通过属于历史记录。
当前前序整合基线为 `cdca16e`：有限当前项目问题、共享冷热/合并调度、确定性增量、证明复用、
受治理精确缓存、有界页面和 GC 已实现；边界以中英文 README 与各批记录为准。
本轮新增模型验证的源码指纹、测试与未覆盖项见最新记录；不同批次成绩不累计。

既有 [v6.1 plan](v6.1.0/plan.md)、[44 项任务](v6.1.0/task.md)、[阶段记录](v6.1.0/next-steps.md)及
[第十六阶段资格父与页面计划](v6.1.0/stage-16-plan.md)保留。revision 25 状态为 DONE 1 / IN_PROGRESS 33 / TODO 10，
M0/M1/M2 尚未整体验收；新版本不重置历史状态。v7 台账后续可更新 revision，正文首次提交后冻结。

## 版本登记

| 版本 | 日期 | 状态 | 说明 |
| --- | --- | --- | --- |
| [7.1.0](AGENT_MEMORY_DESIGN_V7.1.0.md) | 2026-10-10 | IMPLEMENTED_SLICES_VALIDATION_OPEN | 加性运行合同；真实业务效果与扩展历史目标仍开放 |
| [7.0.0](AGENT_MEMORY_DESIGN_V7.0.0.md) | 2026-10-08 | PROPOSED_BASELINE | 当前完整方案；正文冻结基线 `583e20d`，执行进度见 revision 12 |
| [6.1.0](AGENT_MEMORY_DESIGN_V6.1.0.md) | 2026-10-06 | PROPOSED_BASELINE | 前序方案；纳入 N01–N08 与 R05–R13；正文冻结时基线 `5cff9f4` |
| [6.0.0](AGENT_MEMORY_DESIGN_V6.0.0.md) | 2026-10-06 | PROPOSED_BASELINE | 前序方案；R01–R04 修订；历史正文保留 |
| [5.0.0](AGENT_MEMORY_DESIGN_V5.0.0.md) | 2026-10-06 | PROPOSED_BASELINE | 首份统一完整方案；基线 `5cff9f4` |
| [V4](../AGENT_MEMORY_PLUGIN_ARCHITECTURE_V4.md) | 2026-09-14 | 历史目标设计 | 内核、可选插件与演化边界 |
| [V3 演化补充](../EVOLUTION_DESIGN_V3_ADDENDUM.md) | 2026-09-11 | 历史补充 | 结果反馈、Procedure 与 Agent 扩展 |

[变更记录](CHANGELOG.md)解释差异；[versions.json](versions.json)保存元数据与正文 SHA-256。
旧版本条目记录其当时基线，不将其历史元数据改写为新实现状态；V4/V3 不追溯伪造 SemVer。

## 文档优先关系与修订

1. 新开发以 v7 目标合同为参考；当前运行行为以代码、能力声明和对应验收为准。
2. 前序设计、[原接纳提案](../AGENT_MEMORY_ARCHITECTURE_PLAN.md)、[Hindsight 借鉴评估](reviews/HINDSIGHT_2026-10-06_V6_BORROWING_REVIEW.md)保留背景价值，外部项目的成绩不当作本项目的效果。
3. 后续设计变更创建新版本，说明原因、合同、迁移与验收差异；更新本索引、变更记录和校验值。
4. 交付记录按能力注明代码、数据、实际测试和未覆盖项，不能把设计闭合写成生产通过。

设计、软件包、传输协议与数据库版本独立演进。已有实现说明见 [Atom 接纳](../ATOM_ADMISSION.md)、
[自动抽取](../ATOM_EXTRACTION.md)、[双时态](../BITEMPORAL_MEMORY.md)、[代码架构](../ARCHITECTURE.md)。
