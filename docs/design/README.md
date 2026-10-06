# Agent Memory 设计版本索引

当前完整目标设计为 **[v5.0.0 统一架构设计](AGENT_MEMORY_DESIGN_V5.0.0.md)**。
状态为 `PROPOSED_BASELINE`：版本化设计提案，尚未整体实现或完成生产验收。

该设计承接 V4 插件架构，整合事实抽取/接纳、双时态、Hindsight 生命周期借鉴、
Observation 与 L2/L3、可靠任务、删除、检索、迁移和评测。正文含 32 节、20 条不变量、
14 项架构决策和 P0–P6 开发路线。

## 版本登记

| 版本 | 日期 | 状态 | 说明 |
| --- | --- | --- | --- |
| [5.0.0](AGENT_MEMORY_DESIGN_V5.0.0.md) | 2026-10-06 | PROPOSED_BASELINE | 统一完整方案；实现基线 `5cff9f4` |
| [V4](../AGENT_MEMORY_PLUGIN_ARCHITECTURE_V4.md) | 2026-09-14 | 历史目标设计 | 内核、可选插件与演化边界；保留历史文件 |
| [V3 演化补充](../EVOLUTION_DESIGN_V3_ADDENDUM.md) | 2026-09-11 | 历史补充 | 结果反馈、Procedure 与 Agent 扩展阶段 |

[变更记录](CHANGELOG.md)解释版本差异；[versions.json](versions.json)记录结构化元数据和正文 SHA-256。
V4/V3 是旧文档的原始编号，不追溯伪造其 SemVer 或冻结校验值。

## 文档优先关系

1. **目标架构**：v5.0.0 为当前统一提案。与早期目标文件冲突时，新开发设计以本版为参考；
   它不自动改变运行代码或已发布 API。
2. **当前行为**：以实现 Git 版本、能力声明、行为测试及对应接口文档为准。
3. **历史背景**：[接纳修订提案](../AGENT_MEMORY_ARCHITECTURE_PLAN.md)、V4/V3 和调研笔记保留其背景价值，
   不将历史文件改写成今天的实现报告。
4. **交付记录**：后续实施记录必须注明满足哪些设计版本、能力和验收要求，不能仅写“完成 V5”。

已有实现说明：[Atom 接纳](../ATOM_ADMISSION.md)、[自动抽取](../ATOM_EXTRACTION.md)、
[Claim 双时态](../BITEMPORAL_MEMORY.md)、[代码架构](../ARCHITECTURE.md)。

## 修订流程

1. 保留已经提交的版本正文，创建下一版本文件。
2. 在新版本说明变更原因、影响的契约/不变量、兼容与迁移、验收变化。
3. 使用设计专属 SemVer：patch 澄清修正；minor 兼容新增；major 核心语义变更。
4. 更新本索引、CHANGELOG 和 versions.json 的当前版本及正文校验值。
5. 核查链接、版本引用、实现状态和校验值后提交。

设计版本、软件包版本、传输协议版本、数据库 schema/migration 版本独立演进。
本文索引可以更新；固定版本正文应通过新版本修订，便于比较和复核。
