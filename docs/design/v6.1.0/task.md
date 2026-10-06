# Agent Memory v6.1.0 实施任务台账

| 项目 | 内容 |
| --- | --- |
| 配套计划 | [plan.md](plan.md) |
| 规范依据 | [完整设计 v6.1.0](../AGENT_MEMORY_DESIGN_V6.1.0.md)；设计规定语义，本文件安排实施 |
| 设计 SHA-256 | `ece08b05d076d2741aa70bb45293b46d5008aaa312dffdb0bd18f01b8a7bf61b` |
| 实现基线 | `5cff9f47b7061351fd725c3b8e0a89bf5cb51517` |
| 台账版本 | revision 4；2026-10-06；可随实施更新 |
| 当前状态 | DONE 1 项；IN_PROGRESS 22 项；TODO 21 项；本地持久 L1、宿主恢复与事实资格切片已验证，M0/M1 未整体验收 |
| 范围 | P0–P6 能力阶段；基础能力与可选扩展分别发布 |

## 1. 使用与依赖规则

1. 任务状态采用 TODO、IN_PROGRESS、BLOCKED、DONE、DEFERRED。只有所有声明交付与验收证据齐全才勾选 `[x]`；其余保持 `[ ]` 并写明状态。现有文件、旧测试通过或设计完成都不能直接把本批任务标为 DONE。
2. `docs/`、`tests/`、`src/`、`packages/` 开头的路径相对仓库根目录；未写前缀的核心代码路径（如 `domain.py`、`consolidation/`）相对 `src/agent_memory/`。明确标注 PostgreSQL 的模块名相对 `packages/postgres/src/agent_memory_postgres/`，SDK 模块名相对 `packages/python-sdk/src/agent_memory_sdk/`。优先复用现有模块；“新增内部实现”表示允许在既有责任目录内增加文件，不预定新服务。`domain.py`/`ports.py` 不依赖数据库、模型供应商或评测实现。
3. “依赖”指进入该任务实现/验收所需的已明确合同或对应能力切片，不要求依赖任务所有扩展同时完成。任务中记录切片与证据；切片通过不代表总任务可打勾。合同变动须同步上下游、[plan.md](plan.md) 与本台账，语义变动走设计版本修订。
4. P1 与 P2 在 P0 合同固定后可以并行。T14 是受控本地同步语义验收；外部模型、严格持久接入及公开交付的默认启用，必须另通过所需 P2 事务、预算、删除恢复，以及 T34/T38/T39 的当前能力切片。所有生成型 L1 首次启用前均须完成适用的 T25 processing 依赖和输出权限计算切片，单来源同样适用，不因其归入 P3 而后置。
5. T34 基础 trace、T38 基线评测、T39 发布门从早期接入，按 `enabled_capabilities` 验收；不硬依赖完整 P3–P6。P5 是高级检索/可选 Reflect 的能力归组，不是推迟读取守卫与质量验证的理由。T44 持续积累运行文档，最终汇总已启用能力。
6. 所有新写入能力同时提供对应读取、更正、撤回、权限与删除边界。无法支持的组合通过 capability/稳定错误明确拒绝，不能暂时放开后续再补。可选能力 DEFERRED 仍须证明未被意外启用；不得把 deferred 或 unsupported 计入支持该能力的通过数。
7. 基础读取从首次启用即不得静默截断，向量空间身份从首次建索引即必须隔离；P6 扩展的是规模与批量迁移能力。L1 已声明的双时态、更正/撤回和安全不可延期；可选跨槽纠错、历史派生未启用时须显式 unsupported。基础备份/崩溃恢复和擦除日志在 T23/T24 验证，不等到可选 T42。
8. 不设置虚构工期、模型、吞吐量、费用和质量阈值。T05 冻结实际 AcceptanceProfile；缺少有效数据、合同或阈值时相关能力不默认启用。故障测试、真实双后端结果与质量实验分别记录。

## 2. 阶段与首批启动

| 阶段 | 任务 | 阶段目标 |
| --- | --- | --- |
| P0 | T01–T05 | 实现差距、合同、可信 gold、故障夹具与发布口径 |
| P1 | T06–T14 | 有限领域事实语义与受控同步闭环 |
| P2 | T15–T24 | 可靠接入、异步核验、重处理、预算、就绪与删除恢复 |
| P3 | T25–T29 | 依赖、查询覆盖、Observation 与历史安全 |
| P4 | T30–T33 | 可维护 Block、L2/L3 与视图就绪 |
| P5 | T34–T39 | 分阶段诊断、用途检索、模型合同、评测及发布门 |
| P6 | T40–T44 | 规模、可选扩展与运行交付 |

首批可直接启动 T01 实现差距审计、T03 可信 gold 场景。T02 可先起草合同并与 T01 发现对账；T04 可先构造受控调度/故障骨架，协议断言待 T02 固定。T05 在 T03/T04 建立可用数据与评测口径后冻结配置。随后 T06–T13 的语义工作与 T15–T18 的可靠性工作并行；T34/T38/T39 先实现该有限闭环的基础切片。任务编号是稳定追踪标识，不要求按编号串行执行。

## 3. P0：合同与可信验收基础

### AM61-T01 — 实现差距审计

- [x] 状态/阶段：DONE / P0。依赖：无；以指定实现基线为起点。
- revision 2 证据：已完成[实现差距审计](implementation-audit.md)，覆盖入口、事务/模型/输出/删除边界、双后端、R01–R13/N01–N08 缺口及责任；证据与实际基线见 [batch-01](batch-01.md)。 详见 [batch-01](batch-01.md)。
- 已有落点：`docs/IMPLEMENTATION_AUDIT.md`、`src/agent_memory/`、`packages/`、`tests/`；沿用已有架构与能力清单。
- 交付：逐项核对设计第 2 节和能力清单，区分已有、部分、缺失、可选；将旧测试映射到具体能力，不引用历史总通过数代替新验收；记录事务、模型调用、返回正文及删除入口。
- 验收：每个目标能力均有当前证据、缺口、责任任务与明确运行模式；所有入口和两种存储后端可追踪；未运行项目写“未验证”。
- 设计引用：§2、§4.3、§22.4、§26、§31；R01–R13/N01–N08 作为审计目录。

### AM61-T02 — 领域模型与版本化协议合同

- [ ] 状态/阶段：IN_PROGRESS / P0。依赖：T01 的入口/模型盘点；可提前起草。
- revision 4 证据：来源、处理 request、阶段与 publication 身份分开；新资格字段不改变旧默认身份 hash。 详见 [batch-03-05](batch-03-05.md)。
- revision 2 证据：已实现 capture profile 与离线 evidence/profile 版本合同及往返校验；完整来源/候选/任务/阶段 token/能力依赖合同仍待补齐。 详见 [batch-01](batch-01.md)。
- revision 3 证据：固定 durable-receive/1 的 AdmissionTicket、RetentionReceipt 与可选 UoW 端口；ticket 绑定完整来源/配置指纹，接收前不持久正文；发布身份与阶段 token 待 B03。 见 [batch-02](batch-02.md) 与[后续计划](next-steps.md)。
- 已有落点：`domain.py`、`ports.py`、`serialization.py`、`runtime.py`、`kernel.py`、`extensions/protocol.py`。
- 交付：固定来源、候选、证据、断言版本、槽、处理目标、依赖和模型输入清单的身份及关系；定义能力依赖、错误码、兼容投影与完整字段往返；区分事实资格、来源归属、领域支持、处理状态。
- 验收：合同示例可以序列化并往返；未支持字段被明确拒绝；旧接口保持已发布语义；新 ID、阶段 token、能力名称不会混用。
- 设计引用：§7、§10、§22、§26；INV-01/16/17/20。

### AM61-T03 — 可信 gold 与语义场景集

- [ ] 状态/阶段：IN_PROGRESS / P0。依赖：无；T02 定稿后固定 schema。
- revision 2 证据：已适配 17 事件/20 检查、精确 span/hash、trusted host 角色、双时态及权限 gold；当前为合成种子，复杂运行状态标注与真实领域覆盖仍需扩充。 详见 [batch-01](batch-01.md)。
- 已有落点：`evaluation/extraction.py`、`evaluation/retrieval.py`、`evaluation/memory.py`、`tests/test_memory_evaluation_protocol_acceptance.py`。
- 交付：建立语言偏好、项目约束、决定/计划、工具执行结果的正反例；覆盖条件、例外、否定、转述、时间、权限与来源独立性；适配调研 17 事件/20 检查，固定可信身份和 Q15/Q16/Q20 口径；标明数据许可、合成来源与未覆盖领域。
- 验收：每场景有输入事件、valid/known、答复资格、必须/禁止证据、AND/OR 最小充分支持集及预期状态；未知与拒答不混为成功；未执行样例保持未执行标记。
- 设计引用：§25.1/25.7、§30；N08-02，R01–R13 的语义输入。

### AM61-T04 — 确定性回放与故障夹具

- [ ] 状态/阶段：IN_PROGRESS / P0。依赖：T02 合同、T03 场景；回放框架可先搭建。
- revision 4 证据：新增发布回滚、租约、丢确认、游标事务与终止历史测试，SQLite/真实 PG 同合同。 详见 [batch-03-05](batch-03-05.md)。
- revision 2 证据：新增确定性评分/捕获/重试夹具及 SQLite、真实 PostgreSQL 接纳回归；新事务崩溃点、费用未知和 epoch 竞态未实现。 详见 [batch-01](batch-01.md)。
- revision 3 证据：新增同事务两个取消注入点、丢确认、并发去重/容量、删除竞争与跨连接恢复合同；SQLite/真实 PostgreSQL 共同执行；未做断电/进程强杀。 见 [batch-02](batch-02.md) 与[后续计划](next-steps.md)。
- 已有落点：`evaluation/replay.py`、`tests/test_snapshot_replay.py`、`tests/test_consolidation_reliability.py`、`tests/test_worker_runtime.py`、`packages/postgres/tests/test_live_contract.py`。
- 交付：实现可控时钟、供应商桩、事务崩溃点、确认丢失、乱序投递、旧租约、并发擦除/撤权、费用未知等夹具；冻结输入/配置/事件顺序；支持 SQLite 与真实 PostgreSQL 同合同回放。
- 验收：相同输入重复回放得到相同合法状态；能观察事务提交边界与供应商实际收到的输入；数据库桩不冒充真实后端验证；模型桩不冒充外部调用效果。
- 设计引用：§14.5、§25.5–25.9；R02/R03/R10/R12、N02/N03/N05 的竞态前提。

### AM61-T05 — 冻结 AcceptanceProfile

- [ ] 状态/阶段：IN_PROGRESS / P0。依赖：T02、T03、T04 的基线夹具。
- revision 2 证据：冻结 profile 模型与阻断检查已实现；测试数值仅用于评分器回归，生产领域样本、裁判及阈值尚未校准。 详见 [batch-01](batch-01.md)。
- 已有落点：`evaluation/release.py`、`evaluation/benchmark.py`、`evaluation/resources.py`、`tests/test_release_acceptance.py`。
- 交付：按目标领域与启用能力冻结样本、裁判/人工复核、证据召回底线、允许退化、有效覆盖、任务收益、延迟/成本和统计口径；区分硬性语义约束、实验指标与待校准项；定义缺配置时不启用。
- 验收：阈值来源与数据规模可审查；全拒答、系统错误或低覆盖不能被安全分掩盖；后改数据/裁判/阈值会生成新 profile 并重新评测。
- 设计引用：§25.3/25.7/25.8；R13-01/02、N08-03；INV-32。

## 4. P1：有限领域事实闭环

### AM61-T06 — CaptureProfile 与记忆回流

- [ ] 状态/阶段：IN_PROGRESS / P1。依赖：T02、T03。
- revision 4 证据：SDK/MCP durable 身份和来源角色绑定；保留多块与完整谱系剩余项。 详见 [batch-03-05](batch-03-05.md)。
- revision 2 证据：已实现 text/tool_result 宿主 profile、三值完整性、注入/已知改写限制、来源注记保存与重试指纹；多块、父谱系验证和 SDK/MCP 专用适配待实现。 详见 [batch-01](batch-01.md)。
- 已有落点：`capture/api.py`、`capture/policy.py`、`capture/artifacts.py`、`capture/sink.py`；宿主集成复用 `packages/python-sdk/`。
- 交付：版本化宿主事件/角色/内容块/工具结果/省略映射；保存 capture_complete 的 true/false/unknown；记录注入来源、receipt、source family 与处理谱系；区分动作请求、执行结果和助手自述。
- 验收：工具独立响应、多块输入与中断可定位；无事件全集时不伪造完整率；注入副本和已知改写不增加独立证据；正文不能伪造角色资格。
- 设计引用：§6.1–6.5、§9.5；N01-01/02/03。

### AM61-T07 — PredicateSpec、实体与状态槽

- [ ] 状态/阶段：IN_PROGRESS / P1。依赖：T02、T03。
- revision 4 证据：增加 required_evidence_fields 与有界限定/字段证据值对象；复杂谓词与实体歧义仍待实现。 详见 [batch-03-05](batch-03-05.md)。
- 已有落点：`domain.py`、`consolidation/admission.py`、`consolidation/claims.py`、`ontology/model.py`、`ontology/registry.py`、`retrieval/entity.py`。
- 交付：为首批领域固定类型、基数、可比较性、证据资格、必要限定与适用范围政策；增加实体绑定/歧义状态；保持身份、别名、槽和适用域切片的区分；禁止自动实体合并扩权。
- 验收：同值不合并不同事件或时间区间；实体歧义不强行补全；谓词政策版本可追溯；无合同的复杂类型明确不支持。
- 设计引用：§7–§8、§13.1/13.2；R01/R05/R07 的模型前提。

### AM61-T08 — 上下文抽取、语义审查与记忆价值判断

- [ ] 状态/阶段：IN_PROGRESS / P1。依赖：T06、T07；本地受控模型输入合同采用 T02。
- revision 4 证据：prepare/publish 分离、限定保留、未知语义字段拒绝、零/待决/失败区分；跨 turn 仍待实现。 详见 [batch-03-05](batch-03-05.md)。
- 已有落点：`consolidation/atom_extraction.py`、`consolidation/extraction_rules.py`、`consolidation/admission.py`、`capture/artifacts.py`。
- 交付：有界上下文与跨 turn 指代；类型化字段保留否定、条件、例外、数值单位和事件角色；分别记录语义支持审查与保留价值判断；实现 dry-run、失败/零候选/部分覆盖的独立处置。
- 验收：T03 金标准中关键限定不丢失；价值较低不等于语义错误；规则覆盖范围公开；外部模型启用须额外通过 T13/T16/T21/T23/T36 及适用的 T25 输入谱系/输出权限切片。
- 设计引用：§6.2/6.3、§9–§10；INV-03/04/16/17/19，N01/N07。

### AM61-T09 — 字段核验与部分支持表达式

- [ ] 状态/阶段：IN_PROGRESS / P1。依赖：T07、T08 的候选合同。
- revision 4 证据：同一来源修订的字段 span 与 AND/OR 门；跨来源及有效时间组合/撤证仍待实现。 详见 [batch-03-05](batch-03-05.md)。
- 已有落点：`consolidation/admission.py`、`consolidation/admission_runtime.py`、`domain.py`、`ports.py`；核验编排作为 `consolidation/` 内部实现。
- 交付：分开原文定位、语义支持与领域核验；持久化字段/时间级 EvidenceLink；实现版本化 AND/OR SupportExpression；只从完整支持的字段集合生成独立完整投影，保留原候选待决。
- 验收：不同业务版本不能拼成伪支持；AND 时间取交、OR 取并且不填间隙；必要限定不能被丢弃；撤去一条 OR 分支不误伤独立幸存支持。
- 设计引用：§10、§11，尤其 §11.5；R05-01/02。

### AM61-T10 — 双时态与状态终止依据

- [ ] 状态/阶段：IN_PROGRESS / P1。依赖：T07、T09。
- revision 4 证据：独立来源终止与 valid/known 快照、不恢复旧值；独立贡献幸存及 transition 更正仍待实现。 详见 [batch-03-05](batch-03-05.md)。
- 已有落点：`consolidation/claims.py`、`retrieval/atom_state.py`、`retrieval/temporal.py`、`retrieval/temporal_history.py`、`sqlite.py`、`packages/postgres/src/agent_memory_postgres/admission.py`。
- 交付：完整保存 valid 区间、系统版本与来源事件时间；表达粗粒度/未知/点事件；TransitionDecision 分开旧值终止和新值开始依据；迟到、更正、现实变化与撤证分别处理。
- 验收：valid/known 矩阵与历史认知稳定；撤回 B 不自动恢复 A；明确纠正 transition 才按合格证据重算边界；不把新认知回写过去系统版本。
- 设计引用：§12、§13.1/13.4；R06-01/02、INV-06/07/08。

### AM61-T11 — 跨适用范围的可信解释合成

- [ ] 状态/阶段：TODO / P1。依赖：T07、T10。
- 已有落点：`retrieval/atom_state.py`、`retrieval/guard.py`、`retrieval/governed.py`、`domain.py`。
- 交付：由宿主可信 QueryContext 驱动范围选择；谓词级偏序/合取/集合政策及三值条件；保留同存储槽不同适用域切片；实现争议、屏障、缺失与到期的不同回退规则。
- 验收：顺序无关；优先关系循环被拒绝；高优先争议不伪回退；团队禁止不能被个人例外撤销；历史合成政策与当前安全分开。
- 设计引用：§8.3、§18.1、§21.1；R01-01 至 R01-06。

### AM61-T12 — 同槽更正与可选跨槽纠错

- [ ] 状态/阶段：IN_PROGRESS / P1。依赖：T09、T10、T11；持久并发验收复用 T16 的 UoW 合同。
- revision 4 证据：同槽独立证据终止已接宿主 API；既有 correction 回归通过；跨槽及贡献级对账未启用。 详见 [batch-03-05](batch-03-05.md)。
- 已有落点：`consolidation/admission_runtime.py`、`consolidation/claims.py`、`sqlite.py`、`packages/postgres/src/agent_memory_postgres/admission.py`。
- 交付：显式区分 correction/retract/change/conflict；更正只影响被选证据贡献；可选跨槽纠错按稳定锁序原子退出旧贡献、发布新贡献、记录关联及失效；不支持跨隔离域事务时整项拒绝。
- 验收：并发版本变化无半提交；E1 纠错不撤去独立 E2；目标 scope 越权整项失败；旧认知历史保留，擦除约束优先。未启用跨槽能力仍验证明确拒绝。
- 设计引用：§13.1–13.3/13.5、§14.4；R07-01/02。

### AM61-T13 — 输入、dispatch 与最终交付守卫

- [ ] 状态/阶段：TODO / P1。依赖：T02、T04；基础资格读取复用 T09/T11 的可用切片。
- 已有落点：`retrieval/guard.py`、`retrieval/governed.py`、`runtime.py`、`kernel.py`、`ports.py`、模型 Provider/插件适配边界。
- 交付：核验实际模型输入、供应商/区域/用途处理许可及完整输入清单；定义 dispatch 与交付线性化点；默认缓冲输出；缓存命中复查；流式交付仅在逐 chunk 授权能力通过时开启。
- 验收：供应商实际输入不含已撤权正文；读权不等于外发权；删除在各授权点前后有确定结果；历史、缓存、错误消息均不能泄漏；无法验证远程隐含上下文则拒绝严格能力。
- 设计引用：§18.5、§21.2/21.4、§22.4；R03-01 至 R03-06；R04 的基础 input manifest。

### AM61-T14 — 有限同步语义闭环验收

- [ ] 状态/阶段：IN_PROGRESS / P1。依赖：T03–T13 对首批谓词的实现切片；不依赖整个 P2 完成。
- revision 4 证据：本地确定性规则经 SDK/MCP→接收→后台→L1 可运行；复杂语义及生产校准未整体通过。 详见 [batch-03-05](batch-03-05.md)。
- 已有落点：`runtime.py`、`kernel.py`、`tests/test_atom_extraction.py`、`tests/test_atom_admission.py`、`tests/test_bitemporal_claims.py`、`tests/test_governed_recall_integration.py`。
- 交付：使用受控本地输入/模型桩贯通捕获、抽取、审查、核验、接纳、双时态检索、更正和删除守卫；记录候选/待决/确定/争议输出；整理不支持组合。
- 验收：限定与依据可回溯；关键语义和权限测试通过；这是同步语义验收，不声称 durable 接收、真实外部模型或生产就绪。首个公开能力组合另走 T24、T34、T38、T39 的适用切片。
- 设计引用：§1.4、§25.2、§27、§30.1–30.4；R01/R05/R06/R07/R03 的首批场景。

## 5. P2：可靠处理、核验与运行合同

### AM61-T15 — 入口生命周期凭证

- [ ] 状态/阶段：IN_PROGRESS / P2。依赖：T02、T04；可与 P1 并行。
- revision 4 证据：会话绑定 actor/scope/config/epoch，稳定来源身份；SDK/MCP 接入；revise 尚未实现。 详见 [batch-03-05](batch-03-05.md)。
- revision 3 证据：已实现可信宿主 ticket、exact-scope epoch、期限、来源身份保留；既有 archive/erase 在同事务撤销票据；producer epoch/SDK 离线生命周期仍待实现。 见 [batch-02](batch-02.md) 与[后续计划](next-steps.md)。
- 已有落点：`capture/api.py`、`runtime.py`、`kernel.py`、`lifecycle.py`、`operations/deletion_audit.py`。
- 交付：在受治理请求开始时签发绑定 request/producer/scope epoch 的 AdmissionTicket；事务 A 复查代次；明确票据、身份与过期规则；离线旧事件不能自动换新身份/epoch。
- 验收：票据签发后、A 前擦除导致提交失败；新授权输入与旧重传不同；连接开始不冒充受治理接受点；未签发入口不得预存可处理正文。
- 设计引用：§21.4/21.5；R10-01/02、N02-03。

### AM61-T16 — L0 与处理任务原子事务

- [ ] 状态/阶段：IN_PROGRESS / P2。依赖：T02、T04、T15；先支持当前有限候选类型。
- revision 4 证据：事务 B 与阶段复用、单发布回执、来源/lease/config 复核；显式重处理对账未实现。 详见 [batch-03-05](batch-03-05.md)。
- revision 3 证据：事务 A 已同 UoW 保存 L0 与处理 request，并恢复首次回执；没有 worker 消费、阶段产物、事务 B 或索引 outbox，下一批继续。 见 [batch-02](batch-02.md) 与[后续计划](next-steps.md)。
- 已有落点：`capture/queue.py`、`capture/sink.py`、`operations/sqlite_worker_queue.py`、`sqlite.py`、`consolidation/admission_runtime.py`、PostgreSQL `repository.py`/`jobs.py`/`admission.py`。
- 交付：事务 A 原子保存 L0、请求/幂等登记、任务/outbox 与接收回执；事务 B 原子发布决策/证据/版本/失效/后续任务；同一 UoW 连接贯穿，供应商调用置于事务外。
- 验收：各提交点崩溃均为完整旧状态或完整新状态；确认丢失可返回首次回执；重复操作不重复发布；SQLite 与真实 PostgreSQL 保持相同可观察合同。
- 设计引用：§14.1/14.2/14.4/14.5、§15.1、§22.3；INV-05/10/11/12。

### AM61-T17 — Producer 游标与可靠追加

- [ ] 状态/阶段：IN_PROGRESS / P2。依赖：T06、T15、T16。
- revision 4 证据：本地 outbox、连续 ack/缺口、丢确认重传、服务端 epoch 与本地 purge；自动多设备协调待补。 详见 [batch-03-05](batch-03-05.md)。
- 已有落点：`capture/queue.py`、`capture/api.py`、`packages/python-sdk/src/agent_memory_sdk/client.py`、SDK `recovery.py`、MCP 捕获适配器。
- 交付：发送前持久 PendingSubmission；保存 producer/epoch/event ID/sequence/hash；服务端原子幂等 append 或 CAS；连续确认游标与 ack 缺口分开；区分 append/resubmit/revise/resume。
- 验收：丢确认不重复来源；同 ID 异载荷冲突；乱序确认不越缺口；多 producer 不丢轮次或编造全局顺序；旧 outbox 不能在擦除后改 epoch 重放。
- 设计引用：§6.6、§21.5；N02-01/02/03。

### AM61-T18 — 任务去重、资源租约与运行中新工作

- [ ] 状态/阶段：IN_PROGRESS / P2。依赖：T02、T04、T16。
- revision 4 证据：复用 BoundedWorker 的最小租约接口，原子领取、失效租约阻断与次数上限；通用合并/刷新待补。 详见 [batch-03-05](batch-03-05.md)。
- 已有落点：`operations/worker_tasks.py`、`operations/worker_runtime.py`、`operations/sqlite_worker_queue.py`、PostgreSQL `jobs.py`/`worker.py`。
- 交付：分离 dedupe_key、serialization_key、generation；固定 claimed_through 并维护 requested_through；完成与剩余任务检查原子化；租约 fencing、心跳/提交进度、背压、重试、取消分别建模。
- 验收：运行中新增变更不会吞掉；同资源排他而其他资源可前进；旧租约不能发布；defer 不增加失败数；无提交不能无限续命；取消准确报告已提交范围。
- 设计引用：§14.3/14.4/14.7、§24.3；N03-01/02/03。

### AM61-T19 — 持久异步领域核验

- [ ] 状态/阶段：TODO / P2。依赖：T09、T13、T16、T18；真实外部调用另依赖 T21/T23。
- 已有落点：`consolidation/admission.py`、`consolidation/admission_runtime.py`、`operations/worker_tasks.py`、`ports.py`、`extensions/`。
- 交付：获准只读适配器输入/输出合同；候选待决、字段覆盖、证据新鲜度、部分核验、超时/撤回持久状态机；每次发布复查目标版本、来源、策略、权限与租约。
- 验收：超时不是反证；核验资格不能超出适配器授权字段；重复结果幂等；在途删除阻止发布；任务恢复后保持原证据归属与系统时间。
- 设计引用：§11.3–11.5、§14、§23；R05，R03/R10/R12 的核验路径。

### AM61-T20 — 解释替代与重处理对账

- [ ] 状态/阶段：TODO / P2。依赖：T08–T10、T12、T16、T18。
- 已有落点：`consolidation/atom_extraction.py`、`consolidation/admission_runtime.py`、`sqlite.py`、PostgreSQL `admission.py`/`consolidation.py`。
- 交付：显式 additive/replace_interpretation；固定来源身份、解释 stream/head、generation 与覆盖清单；旧新并集逐项 supported/unsupported/unknown 对账；统一解释头 CAS 原子激活。
- 验收：零新候选不等于旧事实全撤回；partial 不激活；独立证据幸存；并发替代/additive 的失败者明确版本冲突并以新请求重做；失败/擦除无半个解释头或过去认知回写。
- 设计引用：§6.4、§14.6；R02-01 至 R02-06。

### AM61-T21 — 多层费用预留与幂等结算

- [ ] 状态/阶段：TODO / P2。依赖：T02、T04、T13、T16；可与 T18 并行。
- 已有落点：`token_budget.py`、`operations/` 的新增内部账本实现、`ports.py`、`sqlite.py`、PostgreSQL 事务适配器。
- 交付：BudgetAccount/Reservation 及稳定内部 call_id；全部适用账户原子预留；reserved→dispatch_intent→dispatched→settled；未决调用 reconciliation_pending；供应商账单/回执去重，重试另预留。
- 验收：多 worker 竞争最后额度不超支；多层约束不重复计费；供应商接受后本地崩溃仍保留未决预留；租约过期不释放未知债务；无费用上界时不声明 strict_cost_budget。
- 设计引用：§23.4；R12-01/02，INV-31。

### AM61-T22 — 分阶段回执与有限目标就绪

- [ ] 状态/阶段：IN_PROGRESS / P2。依赖：T16、T18；L1/index 切片接入相应 T19/T20 发布能力。
- revision 4 证据：SDK/MCP status 提供 source_persisted/l1_decided；零候选与全部待决可区分；index_visible 明确 unsupported。 详见 [batch-03-05](batch-03-05.md)。
- 已有落点：`runtime.py`、`domain.py`、`operations/worker_tasks.py`、`ontology/readiness.py` 的既有思路、SDK 状态查询。
- 交付：区分 capture/publication token；实现有限目标 publication_manifest 与 closed；source_persisted/l1_decided/index_visible 状态与 wait_until；固定 target/deadline、连续水位、缺口及 no_outputs。
- 验收：空未闭合 manifest 不假成功；全部待决仍明确无可用新事实；多个发布需全部覆盖；超时不取消任务；新输入不漂移旧目标；查询状态遵守当前权限。Observation/view 阶段由 T33 补齐。
- 设计引用：§22.3/22.6；N05-01/02/03、R11-01/02。

### AM61-T23 — 擦除、撤回与恢复屏障

- [ ] 状态/阶段：IN_PROGRESS / P2。依赖：T13、T15、T16、T18；为每种已启用写入路径逐步接入。
- revision 3 证据：新增接收 ledger 的删除联动、scope epoch 与未提交 ticket 屏障；历史/派生/模型输入/外部缓存全链路删除协议尚未完成。 见 [batch-02](batch-02.md) 与[后续计划](next-steps.md)。
- 已有落点：`lifecycle.py`、`operations/deletion_audit.py`、`operations/doctor.py`、`capture/queue.py`、`sqlite.py`、PostgreSQL 删除/恢复适配边界。
- 交付：scope/source 代次、删除日志和立即不可读屏障；覆盖任务、缓存、候选、索引、历史及备份恢复；区分 retract/archive/erase；基础备份恢复、崩溃恢复与擦除日志回放在本任务验证，恢复前重放最新删除事实；财务审计仅保留获准最小字段。
- 验收：旧任务、旧输入、旧索引和旧快照不能复活擦除内容；先不可读再异步清理可观察；清理部分失败可恢复；独立幸存证据按合同重评估而非一律清空。
- 设计引用：§21.3–21.5、§26.4/26.7；R02-06、R03-03/04、R10、INV-13。

### AM61-T24 — 双后端与 SDK/MCP 纵向集成

- [ ] 状态/阶段：IN_PROGRESS / P2。依赖：T14 及 T15–T23 中本次启用能力所需切片；持续接入，不等待可选扩展。
- revision 3 证据：新接收合同已在 SQLite 与真实 PostgreSQL 验证，覆盖跨连接初始化/回执恢复；SDK/MCP 全闭环、进程强杀及完整恢复矩阵待执行。 见 [batch-02](batch-02.md) 与[后续计划](next-steps.md)。
- 已有落点：`packages/postgres/`、`packages/python-sdk/`、`packages/mcp-server/`、`src/agent_memory/mcp.py`、`extensions/testing.py`。
- 交付：新版本请求/回执/状态/错误的端到端投影；SQLite/PostgreSQL 真数据库合同与迁移；SDK 重试/游标/等待；MCP 可信身份注入及旧客户端兼容；能力依赖不能被插件绕过。
- 验收：嵌入式与远程入口一致；旧客户端不会静默变异步；未支持组合显式拒绝；真实后端故障证据齐全。默认启用另需 T34/T38/T39 对该组合通过，不将本任务单独当发布许可。
- 设计引用：§4、§15、§22、§25.5、§26；全部已启用 R/N 通过对应入口再验证。

## 6. P3：Observation 与完整依赖

### AM61-T25 — 事实支持与全部生成输入依赖

- [ ] 状态/阶段：IN_PROGRESS / P3。依赖：T09、T13、T16、T23；L1 模型输入清单基础从 T13 提前落地。
- revision 4 证据：单来源输入清单与 exact-scope 输出、在途删除阻断；多来源权限传播和依赖图未实现。 详见 [batch-03-05](batch-03-05.md)。
- 已有落点：`domain.py`、`consolidation/`、`retrieval/guard.py`、`context/`、`sqlite.py`、PostgreSQL 持久层。
- 交付：分别持久 support/processing 边及输入 revision；覆盖多轮、few-shot、旧摘要、缓存、增强索引文本与传递输入；检测依赖环；受众/用途/保留交集；独立公开重建与可选受控级别转换边界。
- 验收：少引用或可选引用不扩大权限；公开重建不夹带旧私有状态；delta 修改块继承本次全部输入；受控转换保留擦除谱系；基础版本未支持级别转换则拒绝。
- 设计引用：§17.1/17.2、§18.5、§21.2；R04-01 至 R04-06。

### AM61-T26 — 查询覆盖依赖与发布屏障

- [ ] 状态/阶段：TODO / P3。依赖：T10、T11、T16、T25。
- 已有落点：`retrieval/governed.py`、`ontology/queries.py`、`ontology/delta.py`、`consolidation/`、两种存储事务适配器。
- 交付：QueryDependency 固定查询/过滤/范围/时间/版本；查询前已有订阅屏障或保守父代次；查询与代次同快照；源发布事务更新代次；发布 CAS 和定时生效边界覆盖否定/集合结论。
- 验收：首次订阅前、查询中、发布后新增成员均不漏；截断/故障不当闭合否定；删除、资格变化、时间到期均可使当前完整结论失效；无关变化不强制全库重建。
- 设计引用：§17.7；R08-01/02，N04-03。

### AM61-T27 — 按 facet 组织 Observation

- [ ] 状态/阶段：TODO / P3。依赖：T18、T22、T25、T26；输入类型取已验收 T07–T11 子集。
- 已有落点：`consolidation/` 内部整理实现、`ontology/layers.py`、`domain.py`、`operations/worker_tasks.py`。
- 交付：主体/facet/分组政策身份；基于合格 L1 的小批整理、反例/冲突处置；区分当前状态和时间线模式、明确陈述与推断；记录依据版本、查询覆盖与处理水位。
- 验收：同名不同主体/侧面不误合并；推断不回写为独立 L1 证据；来源更正即时使不合格 Observation 停用；整理运行中新工作不丢失。
- 设计引用：§16、§17.1、§14.7；N03/N04、R04/R08。

### AM61-T28 — 历史解释与当前安全双守卫

- [ ] 状态/阶段：TODO / P3。依赖：T10、T11、T23、T25、T26。
- 已有落点：`retrieval/guard.py`、`retrieval/temporal_history.py`、`retrieval/atom_state.py`、PostgreSQL `temporal_history.py`。
- 交付：semantic_guard(valid_at, known_at) 与 current_safety_guard；版本化支持/政策/适用范围/依赖；区分 semantic 和 safety 失效；历史缺口与不支持能力明确返回。
- 验收：当前更正不会抹去合法旧认知；今天撤权/擦除阻止所有历史正文交付；历史缺失不返回当前摘要；当前安全代次永不回退。
- 设计引用：§17.8、§12、§21.2/21.4；R09-01/02、R01-06。

### AM61-T29 — 派生删除与依赖失效传播

- [ ] 状态/阶段：TODO / P3。依赖：T23、T25、T26、T27；历史路径接 T28。
- 已有落点：`lifecycle.py`、`operations/deletion_audit.py`、`retrieval/guard.py`、`consolidation/`、两种存储反向依赖查询。
- 交付：来源/断言/政策/query 变化沿相应边传播；事务内必要屏障、异步重建及清理任务；覆盖候选、缓存、Observation、页面、历史及索引；独立 OR 支持精细重评估。
- 验收：受限后代立即不可用；重建失败不会恢复资格；失效环与重复投递可终结；支持撤回和处理权限撤回不同；擦除传递不被少引用或级别转换切断。
- 设计引用：§17.2、§21.2–21.4、§26.4；R04-04/06、R05-02、R09-02。

## 7. P4：场景、画像与可维护文档

### AM61-T30 — Block 与知识页面定义

- [ ] 状态/阶段：TODO / P4。依赖：T25–T27。
- 已有落点：`domain.py` 既有 Block、`consolidation/`、`ontology/layers.py`、`tests/test_memory_blocks.py`、`tests/test_memory_block_evidence.py`。
- 交付：统一 DerivedView/Block 定义，保存 source query/filter、模板/schema、范围、政策、输出模式及 definition_fingerprint；稳定块 ID 与结构依赖；页面围绕长期问题组织。
- 验收：定义改变产生新 generation；同名页面不共享错误水位；不同块的真实生成输入可追溯；不另建与现有 Block 平行的权威对象。
- 设计引用：§17.1/17.5/17.9、§21.2；N04-01、R04-03。

### AM61-T31 — Full/delta 刷新与结果水位

- [ ] 状态/阶段：TODO / P4。依赖：T18、T26、T29、T30。
- 已有落点：`consolidation/`、`operations/worker_tasks.py`、`ontology/delta.py` 的既有增量思路、Block 存储适配器。
- 交付：固定有限刷新目标；full 默认与显式兼容迁移；验证/apply delta、有限协议修复；持久 applied/noop_verified/incomplete/apply_failed/retraction_unresolved；原子更新内容、依赖和连续水位。
- 验收：空 edits 不冒充 verified noop；无效补丁不推进；撤回内容立即停用；刷新中新成员使当前完整发布失败或明确 as_of；修改块继承本轮全部输入，未修改块保留原依赖。
- 设计引用：§17.6/17.9、§14.7；N04-01/02/03、R04-03。

### AM61-T32 — L2 场景与 L3 画像

- [ ] 状态/阶段：TODO / P4。依赖：T27、T29–T31。
- 已有落点：`consolidation/`、`ontology/layers.py`、`context/recovery.py`、`retrieval/bundle.py`。
- 交付：围绕项目/场景组织 L2；L3 区分稳定明确事实与推断模式，记录证据、反例、适用范围和更新政策；长期状态从 L1/Observation 引用，避免无限总结自身；允许禁用预计算。
- 验收：例外/时效/冲突保真；画像不能扩权或将单次行为固化为长期事实；输入撤回后的受影响部分失效；只有真实业务收益才默认启用对应文档。
- 设计引用：§17.3/17.4、§9.3、§21.2、§25.4；R04/R08/N04。

### AM61-T33 — Observation 与视图就绪

- [ ] 状态/阶段：TODO / P4。依赖：T22、T27、T31；L2/L3 目标按需接 T32。
- 已有落点：`operations/worker_tasks.py`、`runtime.py`、`ontology/readiness.py`、SDK 状态/等待投影。
- 交付：扩展 observation_covered/view_covered；固定 facet/view/definition/generation 及有限目标；报告未纳入、失败、剩余范围、as_of 和当前新鲜度；与查询屏障及连续水位一致。
- 验收：后来输入不漂移旧等待目标；旧 definition 成功不能满足新定义；处理完成不等于当前仍新鲜；删除/撤权后的状态响应不泄漏；未启用阶段返回 unsupported。
- 设计引用：§22.6、§17.9；N05-02/03、R11-02、N04-03。

## 8. P5：检索、诊断与发布验证

### AM61-T34 — 证据阶段 trace

- [ ] 状态/阶段：IN_PROGRESS / P5，基础切片从 P0/P1 实施。依赖：T02–T04；按启用能力接入对应生产阶段，不硬依赖全部 P3/P4。
- revision 2 证据：离线 span/AND/OR 充分支持评分已实现；尚未贯通各运行阶段 evidence trace 或新公开 trace 投影。 详见 [batch-01](batch-01.md)。
- 已有落点：`retrieval/governed.py`、`retrieval/bundle.py`、`evaluation/retrieval.py`、`tests/test_retrieval_trace.py`。
- 交付：统一 capture→extraction→candidate→guard→fusion/rerank→packing→answer 的证据 ID/revision/排名/排除原因；输出充分支持集覆盖；受控内部与公开 trace 分离；不额外日志化私有正文。
- 验收：可分别定位候选、精排、装包损失；AND/OR 充分集合评分正确；正确权限过滤不计应召回遗漏；公开 trace 不泄漏禁止来源的 ID 或存在性。
- 设计引用：§18.6、§24、§25.7；N06-01/02/03。

### AM61-T35 — 按用途多路检索与 MemoryBundle

- [ ] 状态/阶段：TODO / P5。依赖：T11、T13、T22、T34；检索派生需额外 T25–T29，历史需 T28，页面需 T31–T33。
- 已有落点：`retrieval/`、`context/model_token_counter.py`、`token_budget.py`、`runtime.py`。
- 交付：问答/整理/历史用途策略，词法/语义/实体/时间候选复用；统一最终资格守卫与预算；区分事实、待决、冲突、推断和原文；索引落后明确回退或错误。
- 验收：所有通道及旧插件受共同约束；历史不混入当前证据；全响应预算覆盖引用和结构；读取截断明确标记不完整，不能作为完整状态/否定结论；未支持的派生不会被暗中开启；故障与无结果可区分。
- 设计引用：§18.1–18.4、§22.4、§25.4；R01/R03/R09、N06。

### AM61-T36 — 模型输入、评分与向量空间合同

- [ ] 状态/阶段：TODO / P5；首个模型接入前完成其输入切片。依赖：T02、T13、T21；高级检索接 T34/T35。
- 已有落点：`context/model_token_counter.py`、`token_budget.py`、`retrieval/semantic.py`、`retrieval/fusion.py`、PostgreSQL `semantic.py`/`vector.py`、Provider 适配器。
- 交付：逐适配器固定 tokenizer、输入上限、输出预留、截断映射；按最终实际 prompt 重算；定义 rank/relative/calibrated score 与阈值/降级；记录模型修订/预处理/归一化等 vector space identity。
- 验收：长中文/emoji/尾部否定和例外不静默损坏；provider failover 不沿用不兼容阈值；同维度不同模型不混用；空间切换由 T40 执行且需已验证水位/质量。
- 设计引用：§15.5、§18.7、§23；N07-01/02/03。

### AM61-T37 — 可选只读 Reflect

- [ ] 状态/阶段：TODO / P5，可 DEFERRED。依赖：T13、T21、T23、T25、T34–T36；仅依赖所读取派生类型对应任务。
- 已有落点：`retrieval/`、`ports.py`、`runtime.py`、可选 `extensions/` Provider；内部工具复用统一读取门面。
- 交付：有界迭代工具、轮次/费用/时间预算、引用验证、推断/未知输出；每轮实际输入与最终交付授权；明确不自动写回，另行保存必须走常规接纳。
- 验收：无持久隐式学习和自我证实；伪引用被拒绝；全部历史上下文继承处理依赖；失败/超预算有明确结果；未启用时基础记忆闭环不依赖 Reflect。
- 设计引用：§19、§18.5、§21.2、§23.4；R03/R04/R12。

### AM61-T38 — 冻结评测、端到端对照与消融

- [ ] 状态/阶段：IN_PROGRESS / P5，基线切片从 P0 实施。依赖：T03–T05、T34；对每个已启用能力取其实现切片，不硬依赖 T35–T37 全部完成。
- revision 2 证据：17/20 合成场景和 CLI 评分可重放；observations 为人工评分器输入，真实适配器端到端、独立裁判实验与消融未执行。 详见 [batch-01](batch-01.md)。
- 已有落点：`evaluation/extraction.py`、`evaluation/retrieval.py`、`evaluation/comparison.py`、`evaluation/benchmark.py`、`evaluation/replay.py`。
- 交付：冻结 L1/index 检索集与原事件端到端集；固定裁判/人工复核协议；按阶段、能力、充分证据、任务成功、拒答/系统错误、成本/延迟分组；按启用子集运行消融，保留实验 manifest。
- 验收：可区分抽取损失与检索损失；17/20 场景正确适配且 Q20 单列；同数据配置可重放；报告置信区间/有效样本和未覆盖范围；无真实实验时不声称 Hindsight 借鉴提升效果。
- 设计引用：§25.1/25.4/25.7/25.8；N08-01/02/03，R13。

### AM61-T39 — 按能力组合的发布门

- [ ] 状态/阶段：IN_PROGRESS / P5，首个交付前启用基础切片。依赖：T05、T24、T34、T38 对拟启用能力的证据；附加每项 capability 的安全/事务/预算前提，不依赖未启用扩展。
- revision 2 证据：质量检查可转为现有 REPLAY 发布检查；全拒答、退化、配置变化与未校准均可阻断；完整能力依赖清单和运行合同发布门待实现。 详见 [batch-01](batch-01.md)。
- 已有落点：`evaluation/release.py`、`extensions/testing.py`、`runtime.py` 能力检查、`tests/test_release_acceptance.py`。
- 交付：维护能力→任务切片→合同场景→质量/成本门槛的发布清单；安全语义失败直接阻断；缺数据/阈值/不支持后端组合不默认启用；冻结发布配置与回滚目标。
- 验收：全拒答、来源覆盖退化、抽取合格但任务退化均能阻断；配置/裁判后改需新验证；有限闭环可独立发布，无须等待可选 Reflect/多模态/迁移；发布记录不会把测试数量当业务收益。
- 设计引用：§22.4、§25.8、§26、§27；R13-01/02、N08-03。

## 9. P6：规模与可选扩展

### AM61-T40 — 历史分页与规模索引迁移

- [ ] 状态/阶段：TODO / P6。依赖：T10、T22、T23、T28、T36；以已发布基础能力为前提。
- 已有落点：`retrieval/temporal_history.py`、`retrieval/sqlite_source.py`、`sqlite.py`、PostgreSQL `temporal_history.py`/`vector.py`、迁移目录。
- 交付：稳定历史快照/游标及完整性说明；索引新 generation 构建、连续覆盖与删除代次检查；模型空间迁移、双读/回滚边界和资源基线；避免全历史无界扫描。
- 验收：分页无重漏且无法跨权限；迁移期间不混空间、不复活擦除；切换需固定质量门槛与水位；回滚不能恢复旧安全状态；真实规模与限制记录，未测不承诺吞吐。
- 设计引用：§15.4/15.5、§24、§26；N07-03，R09/R11。

### AM61-T41 — 可选多模态与有界拆分

- [ ] 状态/阶段：TODO / P6，可 DEFERRED。依赖：T06、T08、T13、T18、T21、T25、T36。
- 已有落点：`capture/artifacts.py`、`capture/api.py`、`consolidation/atom_extraction.py`、`ports.py` 及可选插件。
- 交付：有序稳定内容块、类型化 locator、解析/视觉输入谱系；SplitPlan 父子身份、覆盖与剩余范围；语义单元保护、深度/数量/费用边界；最终失败与局部成功分别回执。
- 验收：附件、页码、时间段来源可追溯；分片不丢条件或复制独立证据；预算/权限贯穿解析与模型；不支持媒体明确拒绝，基础安装不新增强制依赖。
- 设计引用：§6.7、§9.2、§18.7；N01/N07、R03/R04/R12。

### AM61-T42 — 可选归档恢复与 Portable Transfer

- [ ] 状态/阶段：TODO / P6，可 DEFERRED。依赖：T23、T25、T28、T29、T40。
- 已有落点：`lifecycle.py`、`operations/deletion_audit.py`、`serialization.py`、两种存储备份/迁移适配器。
- 交付：恢复以当前授权/删除状态生成当前系统版本；portable manifest 包含身份、政策、依赖、删除与快照界限；操作独占 staging；区分可迁移语义数据与物理备份恢复；旧租约/未决外部副作用不盲目重放。
- 验收：导入不越权、不回写过去、不复活擦除；依赖/因果/support 与检索边按类型验证；失败清理只影响本操作资源；未决费用与 outbox 有明确恢复政策。
- 设计引用：§26.7、§21.4、§23.4；R09/R10/R12。

### AM61-T43 — 可选 Procedure 受控演化

- [ ] 状态/阶段：TODO / P6，可 DEFERRED。依赖：T25、T29、T38、T39 的现有能力切片。
- 已有落点：`consolidation/episodes.py`、`consolidation/procedures.py`、`packages/evolution/src/agent_memory_evolution/`。
- 交付：Episode/Procedure 引用真实执行证据及版本；区分建议、实践成功、可复用程序；反例、回滚、反馈许可与发布状态机；演化候选按新配置重新进入评测/发布门，避免本任务与门槛任务循环依赖。
- 验收：单次成功不伪泛化；无证据自评不加可信度；来源撤回使相关程序重新评估；未经已定义门槛不得自动部署；用户约束与当前权限始终有效。
- 设计引用：§20、§21.2、§25.4/25.8；INV-03/14/15/32。

### AM61-T44 — 运行手册与最终交付汇总

- [ ] 状态/阶段：TODO / P6；从首个运行能力开始持续更新。依赖：拟交付子集已通过 T39；不要求 DEFERRED 扩展实现。
- 已有落点：`docs/single-host-deployment.md`、`docs/recovery-operations.md`、`docs/RESOURCE_BASELINE.md`、`operations/doctor.py`、本目录 plan/task。
- 交付：部署/升级/备份/擦除/费用对账/队列卡住/回滚运行手册；支持矩阵和未覆盖边界；迁移预演、告警与诊断；汇总实现提交、profile、各阶段证据及可选能力决定。
- 验收：另一操作者可按手册重现受支持闭环、定位故障并安全恢复；配置与发布能力一致；未决事项具名且不伪完成；仅在本轮声明交付范围全部验收后勾选，后续新增能力启动新台账 revision。
- 设计引用：§24、§26–§28、§31–§32；已启用全部 R/N。

## 10. 66 项设计验收责任映射

本表沿用设计 §25.6/§25.9 的固定 ID，只分配责任，不修改预期。责任任务负责合并跨模块证据；协作任务提供对应实现或入口场景，包含后续可选变体，不自动成为每个能力组合的完整任务前置；发布仍须通过所启用行为的实际验收。初始为“未执行”；本批仅登记评分/宿主基础切片，不把其结果替代完整运行场景。同一任务可能分多个能力切片完成；可选能力未启用须验证拒绝边界，不得宣称其功能场景通过。

| 验收 ID | 场景定位（完整预期见设计） | 责任任务 | 协作任务 | 验证状态 |
| --- | --- | --- | --- | --- |
| R01-01 | 用户中文，项目 A 英文，批准覆盖；另测同用户存储槽分别适用 A/B 的两条偏好 | AM61-T11 | T07/T10/T13/T28 | 未执行 |
| R01-02 | 不可比较范围异值；交换入库/候选顺序；循环优先政策 | AM61-T11 | T07/T10/T13/T28 | 未执行 |
| R01-03 | 高优先项分别为争议、无记录、到期、block_inheritance | AM61-T11 | T07/T10/T13/T28 | 未执行 |
| R01-04 | 用户周五例外与团队维护禁止同时适用；时区未知 | AM61-T11 | T07/T10/T13/T28 | 未执行 |
| R01-05 | Python/SQL 部分成员观察与 Python 反证 | AM61-T11 | T07/T10/T13/T28 | 未执行 |
| R01-06 | K1/K2 合成政策不同，今天撤权；历史政策缺失 | AM61-T11 | T07/T10/T13/T28 | 未执行 |
| R02-01 | 新提取零候选，但旧候选逐项复核仍 supported | AM61-T20 | T08/T09/T16/T18/T23 | 未执行 |
| R02-02 | 旧误提被明确判为 unsupported，新解释通过 | AM61-T20 | T08/T09/T16/T18/T23 | 未执行 |
| R02-03 | 生成截断/审查缺项；完整审查但资格未知 | AM61-T20 | T08/T09/T16/T18/T23 | 未执行 |
| R02-04 | 撤去来源 A 支持，独立来源 B 仍支持同值 | AM61-T20 | T08/T09/T16/T18/T23 | 未执行 |
| R02-05 | 覆盖重叠或不相交的两个替代及并发 additive | AM61-T20 | T08/T09/T16/T18/T23 | 未执行 |
| R02-06 | 对账期间删除/撤权、提交崩溃或回执丢失 | AM61-T20 | T08/T09/T16/T18/T23 | 未执行 |
| R03-01 | 旧索引命中已撤权正文，拟送外部精排 | AM61-T13 | T04/T21/T23/T24 | 未执行 |
| R03-02 | 有读取权但无指定供应商/区域处理权 | AM61-T13 | T04/T21/T23/T24 | 未执行 |
| R03-03 | Reflect 读取后、交付授权前擦除；缓存命中也同样 | AM61-T13 | T04/T21/T23/T24 | 未执行 |
| R03-04 | 删除分别发生在 dispatch/交付授权点前后 | AM61-T13 | T04/T21/T23/T24 | 未执行 |
| R03-05 | 输出中途撤权、适配器无逐 chunk 授权 | AM61-T13 | T04/T21/T23/T24 | 未执行 |
| R03-06 | 外部 ACL 不可验证、远程模型暗含额外上下文 | AM61-T13 | T04/T21/T23/T24 | 未执行 |
| R04-01 | 模型读 P 公开与 S 私有，但只引用 P 或将 S 标可选 | AM61-T25 | T13/T29/T31 | 未执行 |
| R04-02 | 多轮、旧摘要、few-shot、缓存、L1 解释携带私有输入 | AM61-T25 | T13/T29/T31 | 未执行 |
| R04-03 | delta 仅改一个块，提示词含私有资料 | AM61-T25 | T13/T29/T31 | 未执行 |
| R04-04 | 因省略引用而不再“必要”的来源撤权/擦除 | AM61-T25 | T13/T29/T31 | 未执行 |
| R04-05 | 仅公开输入的全新上下文重新生成 | AM61-T25 | T13/T29/T31 | 未执行 |
| R04-06 | 模型自称已脱敏与独立获准级别转换 | AM61-T25 | T13/T29/T31 | 未执行 |
| N01-01 | 纯工具响应、多块内容、流中断 | AM61-T06 | T03/T08/T25 | 部分：工具结果/省略，未覆盖多块 |
| N01-02 | 注入记忆再回流及已知改写 | AM61-T06 | T03/T08/T25 | 部分：宿主标记与三条接纳入口，未覆盖完整谱系 |
| N01-03 | 无宿主事件全集或伪造来源角色 | AM61-T06 | T03/T08/T25 | 部分：宿主 origin/actor/scope 绑定及 SDK/MCP 同合同已测；完整事件全集适配待补 |
| N02-01 | 确认丢失、重复投递、同ID异载荷 | AM61-T17 | T15/T16/T23/T24 | 部分：SDK/MCP 丢确认重传、重复/异载荷冲突已测；revise/重处理待补 |
| N02-02 | 乱序ack、多个生产者并发追加 | AM61-T17 | T15/T16/T23/T24 | 部分：单 producer 乱序 ack/缺口已测；多 producer 并发待补 |
| N02-03 | 离线outbox遇scope擦除 | AM61-T17 | T15/T16/T23/T24 | 部分：服务端 epoch 拒旧会话、本地 purge 撤销已测；自动离线设备联动待补 |
| N03-01 | 整理运行中收到新变更 | AM61-T18 | T04/T16/T27/T31 | 未执行 |
| N03-02 | 同资源多worker及旧租约回报 | AM61-T18 | T04/T16/T27/T31 | 部分：多 worker 原子领取、旧租约不可发布已测；派生刷新并发待补 |
| N03-03 | 背压defer、心跳无提交、部分提交后取消 | AM61-T18 | T04/T16/T27/T31 | 未执行 |
| N04-01 | source query/模板语义变化 | AM61-T31 | T26/T29/T30/T33 | 未执行 |
| N04-02 | 零edits、补丁无效、待撤回内容 | AM61-T31 | T26/T29/T30/T33 | 未执行 |
| N04-03 | 刷新期间新增相关成员 | AM61-T31 | T26/T29/T30/T33 | 未执行 |
| N05-01 | 来源接收但提取未完成 | AM61-T22 | T16/T24/T33 | 部分：SDK/MCP source_persisted/l1_decided 查询已测；完整目标水位待补 |
| N05-02 | 全部待决、多次发布、部分索引落后 | AM61-T22 | T16/T24/T33 | 部分：零候选/全部待决与失败已测；多发布/索引水位未启用 |
| N05-03 | 持续新增、deadline、权限撤回 | AM61-T22 | T16/T24/T33 | 未执行 |
| N06-01 | gold分别在候选/精排/装包阶段丢失 | AM61-T34 | T03/T13/T35/T38 | 未执行 |
| N06-02 | gold需要多个桥梁证据或有替代支持 | AM61-T34 | T03/T13/T35/T38 | 部分：离线 AND/OR，运行 trace 待补 |
| N06-03 | 无权/过期来源被过滤 | AM61-T34 | T03/T13/T35/T38 | 部分：gold 资格，实时读取资格待补 |
| N07-01 | 长中文/emoji/尾部否定及例外 | AM61-T36 | T08/T13/T21/T40 | 未执行 |
| N07-02 | 相对rerank分数、provider failover | AM61-T36 | T08/T13/T21/T40 | 未执行 |
| N07-03 | 同维度embedding模型替换 | AM61-T36 | T08/T13/T21/T40 | 未执行 |
| N08-01 | 冻结L1检索与原事件端到端两组 | AM61-T38 | T03/T05/T34/T39 | 未执行 |
| N08-02 | 17事件/20检查适配 | AM61-T38 | T03/T05/T34/T39 | 部分：17/20 已适配，端到端未执行 |
| N08-03 | 抽取达标但最终召回/任务退化 | AM61-T38 | T03/T05/T34/T39 | 未执行 |
| R05-01 | 部分字段/时间核验 | AM61-T09 | T10/T19/T29 | 部分：字段必要性与同来源 span/AND/OR 已测；跨来源时态投影待补 |
| R05-02 | 联合证据不同业务版本、OR时段不重合、AND无共同区间、撤项 | AM61-T09 | T10/T19/T29 | 未执行 |
| R06-01 | A被B替换，撤回B支持但结束A依据幸存 | AM61-T10 | T09/T12 | 未执行 |
| R06-02 | 明确纠正错误transition及其起点 | AM61-T10 | T09/T12 | 未执行 |
| R07-01 | E1误提家庭地址，E2独立支持家庭地址；更正E1为工作地址 | AM61-T12 | T09/T16/T24 | 未执行 |
| R07-02 | 一个贡献或槽版本变化、目标越权、隔离域不支持 | AM61-T12 | T09/T16/T24 | 未执行 |
| R08-01 | 首次订阅前、查询中、发布检查后分别新增阻塞 | AM61-T26 | T25/T29/T31 | 未执行 |
| R08-02 | 查询截断、删除成员、无写入时间边界 | AM61-T26 | T25/T29/T31 | 未执行 |
| R09-01 | K20更正A1后查询K15历史派生 | AM61-T28 | T23/T25/T29 | 未执行 |
| R09-02 | 今天撤权/擦除后查询任意known_at | AM61-T28 | T23/T25/T29 | 未执行 |
| R10-01 | 入口ticket签发后、事务A前scope擦除 | AM61-T15 | T17/T23/T24 | 部分：原子 producer 追加与双后端回滚已测；远端在途交付屏障待补 |
| R10-02 | 新授权输入与旧离线重传 | AM61-T15 | T17/T23/T24 | 部分：旧 producer/session 与 ticket 被 epoch 撤销；完整 revise 协议待补 |
| R11-01 | capture token到位、publication manifest未闭合且为空 | AM61-T22 | T16/T24/T33 | 未执行 |
| R11-02 | 多个发布token与不连续索引完成 | AM61-T22 | T16/T24/T33 | 未执行 |
| R12-01 | 十个worker竞争多层账户最后一次调用额度 | AM61-T21 | T04/T13/T23 | 未执行 |
| R12-02 | 供应商接受后、本地dispatched前崩溃，费用未知、账单重放 | AM61-T21 | T04/T13/T23 | 未执行 |
| R13-01 | 全部拒答换取安全分、检索有效覆盖退化 | AM61-T39 | T03/T05/T34/T38 | 部分：质量门回归，实际适配器待接入 |
| R13-02 | 缺门槛配置/数据量不足/事后改裁判 | AM61-T39 | T03/T05/T34/T38 | 部分：冻结/样本/配置检查，真实校准待补 |

## 11. 实施证据与状态变更模板

任务每次状态变更记录以下内容；可在对应任务后补充或链接独立报告，避免在本文件堆积完整日志。

```text
任务：AM61-Txx
状态：TODO / IN_PROGRESS / BLOCKED / DONE / DEFERRED
台账 revision / 日期 / 责任人：
本次 capability 与切片：
依赖合同版本、实现 commit、工作区差异：
实际改动文件及 schema/迁移版本：
验证环境：SQLite / 真实 PostgreSQL / 模型桩 / 实际 provider（分别列明）
设计验收 ID、执行命令或回放入口、fixture hash：
预期 / 实际 / 通过与失败 / 跳过原因：
AcceptanceProfile、scorer、实验 manifest 与结果文件：
故障恢复、权限/擦除、成本及兼容性证据：
剩余切片、未解决问题、不能启用的能力：
审查结论与回滚方法：
```

首批实际证据见 [batch-01](batch-01.md) 和 [验证记录](validation-batch-01.json)；本轮见 [batch-03-05](batch-03-05.md) 与 [验证记录](validation-batch-03-05.json)；第二批见 [batch-02](batch-02.md)、[验证记录](validation-batch-02.json)；开发顺序见 [next-steps](next-steps.md)。执行任务时只运行与变更及已启用合同相关的验证，再由发布门执行冻结组合；不因简单文档更新重复运行完整运行时测试。

## 12. 台账修订记录

| revision | 日期 | 变更 | 实施状态 |
| --- | --- | --- | --- |
| 1 | 2026-10-06 | 从完整设计 v6.1.0 分解 44 项任务，映射 66 项设计验收，明确能力切片依赖与发布门 | 全部 TODO，新增实现与验收均未在本台账执行 |
| 2 | 2026-10-06 | T01 审计完成；实现评测/质量门和宿主 capture 切片，关联真实回归证据 | DONE 1；IN_PROGRESS 8；TODO 35；未完成整体里程碑 |
| 3 | 2026-10-06 | 新增 ticket、删除 epoch、L0+请求原子接收及双后端验证；列出 B03–B09 顺序 | DONE 1；IN_PROGRESS 12；TODO 31；仅接收端，抽取 worker/事务 B 待开发 |
| 4 | 2026-10-06 | 本地持久执行、SDK/MCP producer/outbox、限定字段与双时态终止切片；见 [batch-03-05](batch-03-05.md) | DONE 1；IN_PROGRESS 22；TODO 21；B03/B04/B05 未整体完成 |
