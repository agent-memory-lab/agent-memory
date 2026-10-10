# 代码组织与依赖边界

代码以业务能力聚合，能力内部再按职责分文件。核心契约保持稳定，集成包独立安装；
不为每个类建目录，也不将所有流程塞入统一的 service、manager 或 utils。

## 目录与职责

| 位置 | 职责 | 不应放入的内容 |
| --- | --- | --- |
| 根目录 `domain.py`、`ports.py`、`lifecycle.py` | 共享数据、协议、生命周期契约 | 数据库连接、传输、评测逻辑 |
| 根目录 `kernel.py`、`providers.py`、`sqlite.py` | 核心记忆流程、默认策略、本地事务存储 | 本体运营、上下文恢复、外部 SDK |
| 根目录 `composition.py`、`runtime.py`、`unified_memory.py`、`adapters.py`、`mcp.py` | 装配、宿主入口与协议接入 | 新的检索算法或领域规则 |
| `capture/` | 采集策略、工件、队列、提交接口 | 恢复状态和检索排序 |
| `consolidation/` | Atom 自动抽取、语义/价值判定与接纳核验、Claim 合并、Episode 分段、Procedure 归纳 | 插件发现与数据库适配 |
| `retrieval/` | 双时态状态投影、检索器、候选融合、权限过滤、多样性和预算打包 | 本体版本运营和模型评测 |
| `ontology/` | 本体模型、投影、图查询、存储、版本与索引运营 | 通用采集和上下文恢复 |
| `context/` | 恢复状态、分区、压缩、预算反馈及恢复传输 | 长期记忆检索算法 |
| `extensions/` | 插件契约、发现、注册、生命周期和契约验证 | 具体业务算法 |
| `operations/` | 删除审计、健康诊断、后台任务和工作队列 | 评测数据集与比较实验 |
| `evaluation/` | 基准测试、后端比较、回放、资源与发布评测 | 生产运行路径中的决策逻辑 |
| `packages/` | PostgreSQL、MCP Server、SDK、LangGraph、Evolution | 核心包必须安装的第三方依赖 |

`serialization.py` 和 `token_budget.py` 是具有明确用途的小型共享工具，保留在根目录。
功能包的 `__init__.py` 仅说明职责，不递归导出全部实现。

## 依赖规则

1. 领域契约不依赖存储实现、插件运行时或传输层。`ports.py` 依赖 `domain.py`；
   本体 `model.py` 仅依赖共享领域数据、序列化与标准库。
2. 能力内部使用相对导入，跨能力直接导入拥有该职责的模块，避免通过顶层公共 API
   或兼容门面绕行。宿主装配层负责组合能力；不会再叠加通用服务层。
3. 生产实现不导入 `evaluation/`。为保持现有 API，顶层包仍导出原有评测接口；
   这是兼容入口，并不代表核心流程可以依赖评测。
4. 框架和外部数据库依赖留在独立集成包。可选 tokenizer、比较后端只在显式使用时加载。
5. 本体 `model.py` 管数据、校验和投影规则；`store.py` 管事务与 SQL；
   `plugins.py` 管检索与归并插件生命周期。`queries.py` 管共享 SQL 查询和行解码。
   这些实现不反向依赖兼容门面 `memory.py`。

当前是模块化单体，不宣称全部功能都能单独加载。插件注册仍提供默认本地 Provider，
部分检索适配器仍直接使用 SQLite。若以后需要独立部署，再依据实际依赖提炼端口。

记忆提取、事实接纳与 L0–L3 派生的目标设计见
[Agent Memory 目标架构与实施方案](AGENT_MEMORY_ARCHITECTURE_PLAN.md)；该文档是提案，
首期已实现的范围见 [Atom 接纳接口](ATOM_ADMISSION.md)。

首期接纳沿用这些边界：`domain.py` 定义候选、来源权限和证据，
`consolidation/admission.py` 处理纯接纳规则，`admission_runtime.py` 编排事务与核验，
`retrieval/atom_state.py` 从候选版本投影双时态状态。`kernel.py` 只连接入口和检索守卫；
SQLite/PostgreSQL 适配器负责批次原子性、版本比较和删除屏障，不判断事实语义。

自动抽取沿用同一事务与状态模型：`consolidation/atom_extraction.py` 在事务外调用
生成器和审查器，完成输入、语义和复用价值判断；`extraction_rules.py` 提供有限语法参考实现。
最终通过 `admission_runtime.py` 原子保存原文、抽取报告与候选。`runtime.py` 和 `kernel.py`
仅新增装配入口。使用方式与失败语义见 [自动 Atom 抽取](ATOM_EXTRACTION.md)。

条件事实复用同一接纳账本：`conditions.py` 与 `evidence_support.py` 是跨写入/读取/删除共享的
纯值对象和规则；`consolidation/qualification.py` 校验宿主核验、字段支持与候选版本；
`retrieval/contextual_state.py` 根据可信上下文计算适用性、时间支持和合成结果。数据库适配器只执行
同事务的快照清理与版本更新。候选不进入无条件 Claim 表；完整合同及边界见
[第二阶段实施记录](design/v6.1.0/stage-02.md)。

最新目标设计见 [v7 架构与算法](design/AGENT_MEMORY_DESIGN_V7.0.0.md)。有限当前问题视图、共享调度和精确缓存已按 B0–B6 交付；
其支持边界及后续工作见 [v7 计划](design/v7.0.0/plan.md)。新设计不代表其中全部能力已经交付。
本地真实模型验证入口位于 `evaluation/ollama_smoke.py`，复用现有生产能力而不被生产代码导入；
冻结计划、运行数据流及合成证据边界见 [本地 Ollama 验证](design/v7.0.0/ollama-smoke.md)。

## 拆分与合并标准

- 同一数据、事务边界及变更原因的代码放在一起。`sqlite.py` 的本地仓储和工作单元
  维持集中，以便检查事务、权限和证据一致性；不按行数拆成多个 mixin。
- 一个文件出现独立的存储、领域规则、生命周期职责时才拆分。本体原有约 1,556 行
  的混合实现已据此拆为模型、存储和插件三个实现文件。
- 小文件可以代表真实边界，例如 Capture API、预算协议、来源适配器；不要仅为减少
  文件数把它们并入大型服务。新的微小辅助函数优先放在使用它的模块。
- 能力目录只保留一层。新建子目录需要真实的独立职责或依赖隔离需求，不按类名建层级。
- 新业务代码优先进入已有能力目录。根目录的职责是共享契约、核心流程与装配入口，
  不再按功能前缀无限增加文件。

## 导入兼容

推荐新代码直接使用所属能力的路径：

```python
from agent_memory.capture.policy import CaptureSanitizer
from agent_memory.retrieval.governed import GovernedRecallPipeline
from agent_memory.context.recovery import RecoveryState
from agent_memory.ontology.model import OntologySchema
from agent_memory.ontology.store import SQLiteOntologyStore
```

原来的 `agent_memory.capture_policy`、`agent_memory.recovery` 等路径仍可使用。
`_compat.py` 中的固定表将 69 个旧路径绑定到实际模块对象，保留类身份、旧路径
monkeypatch 和历史 pickle 全局引用解析。`ontology/memory.py` 只重导出拆分前的本体符号。
核心模块及集成包已使用新路径，现有测试继续覆盖旧路径。

兼容表在公共 API 初始化完成后注册，会加载映射中的标准库实现；它不创建连接、
启动任务或加载可选 SDK。不要向兼容表增加新功能模块。删除旧路径须另做版本迁移。

## 验证

`tests/test_architecture.py` 检查领域依赖、生产与评测边界、旧导入、模块身份、
历史 pickle 引用和可选依赖。行为回归沿用核心测试及各集成包的契约测试。

```bash
python -m pytest tests packages/evolution/tests packages/langgraph/tests \
  packages/python-sdk/tests packages/mcp-server/tests packages/postgres/tests
python -m build --wheel
```

需要数据库、远端服务或专用配置的测试仍按各自条件启用；结构迁移不改变其运行条件。


## V7 运行时维护补充

[运行时修复](design/v7.0.0/runtime-repairs.md)将已发布项目页面维护接入同一个
`RefreshHost`/共享队列的独立处理器路由，沿用父依赖、配额、租约与提交证明；不新增队列。
受治理模型在调用前持久预留容量，最终费用状态来自当前账本。授权审计通过显式宿主
归档/确认协议释放已归档行；财务债务、回执、删除日志与其他有限保留约束不受该协议删除。
既有真实 Ollama smoke 证据保留；上述工程合同不宣告抽取质量或全成本收益验收完成。

来源级遗漏审计由 `consolidation/source_audit.py` 承担独立的有界合同，
`atom_extraction.py` 接入可选阶段；它不把候选忠实度审查当成召回证明。
补提候选只进入既有待核验接纳流程，持久任务与批次收尾沿用同一事务的授权检查。
现有元数据承载版本化范围记录，不增加表或调度器；参见
[来源遗漏审计 B2](source-omission-audits.md)。


## 有限关系维护补充

`derived/relation_questions.py` 将宿主注册的有限依赖边/日期连接接入现有项目风险
QuestionView。它复用 `ontology/rules.py` 的类型检查与有界关系链内核；输入仍是同一事务
授权、语义核验的完整项目 census，不另建图数据库或授权路径。候选推论保留前提血缘、
有效时间交集、未知/冲突与聚合资格，沿用完整查询前沿、逆依赖、删除屏障及
`next_transition_at` 调度。范围、上界和协调索引升级见
[关系视图维护](design/incremental/relation-views.md)。
