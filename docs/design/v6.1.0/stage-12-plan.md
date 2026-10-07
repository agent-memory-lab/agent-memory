# 第十二阶段设计与实施计划：受控 Observation 依赖、失效与重建

| 元数据 | 值 |
| --- | --- |
| 方案标识 / 版本 | AM61-ST12 / 1.1，2026-10-07 |
| 状态 | IMPLEMENTED；D01–D07 完成；仅启用本阶段限定组合，证据见 [stage-12.md](stage-12.md) |
| 实现基线 | `f70eec434b57dd0cf40d198c80daa5bd2d1bf0ae`，第十一阶段 |
| 规范 / 台账 | [设计 v6.1.0](../AGENT_MEMORY_DESIGN_V6.1.0.md)；执行台账 revision 17 |
| 主任务 | T25 support/processing 依赖、T26 查询覆盖屏障、T27 facet Observation |
| 必须配套的切片 | T13 输入/交付守卫、T18 刷新、T22 有限回执、T23/T29 派生删除、T28 当前安全守卫、T24/T44 验收与手册 |
| 执行清单 | [stage-12-tasks.md](stage-12-tasks.md) |

## 1. 阶段目标与范围

完整交付一个可追溯、会立即失效、可恢复重建的本地 Observation，而不是只保存一段摘要。
首个真实运行样例：在同 exact scope 内，为 canonical subject=alice、facet=communication.language 整理已登记 `locale` 谓词。
使用确定性整理器表达合格 L1 中明确的语言偏好；不把它推断成性格、团队规则或跨项目偏好。
首版注册器仅开放此 facet/predicate/template；其他组合具名拒绝，扩展须先补合同与语义/覆盖测试。

本阶段支持：多来源但同 exact scope、当前状态快照、版本化 full rebuild、显式宿主查询及可选 `derived_context` 接入。
采用受控本地处理，不接外部 LLM，不做跨 scope 合成、派生页面再生成、delta 页面补丁、演化叙事或历史 Observation 交付。
明确不支持的输入类型、受众转换和历史接口返回具名错误，不能用当前摘要替代历史。
T25–T29 只按此组合声明完成切片，不把整个 B07/M2 或真实抽取质量标为通过。

示例完整行为：L1 locale=zh-CN → Observation 明确表达该偏好；来源修订、同槽新冲突、到期或删除 → 原 Observation 立即不可作为当前上下文；
刷新按最新合格集合重建。删除最后一个来源 → 清除派生正文并记录完整零输出结果，不能继续展示旧偏好或推断“用户没有偏好”。

## 2. 代码架构与复用

| 实际位置 | 职责 |
| --- | --- |
| `derived/model.py` | 纯契约：定义、版本、support/processing/query 边、输入清单、状态、构建结果及时间/限制 |
| `derived/service.py` | 单一编排入口：宿主定义注册、快照、准备、发布 CAS、读取/交付守卫、失效与清理协调 |
| `derived/observation.py` | facet 分组、确定性 full 整理、逐项事实依据校验；不掌管事务、授权或调度 |
| `ports.py` | 新增 DerivedUnitOfWork 能力：版本/head、依赖反查、facet barrier、可信处理许可与同事务变更/outbox |
| `operations/facet_refresh.py` | 独立的版本化 facet 单元/有限回执，复用 BoundedWorker；旧 `resource_refresh.py` 来源合同和守卫不变 |
| `operations/sqlite_derived.py` / PostgreSQL `derived.py` | 同等 SQL 持久化、锁、CAS、代次与清理；不判断事实语义 |
| 既有接纳/资格/终止/修订/删除写入边界 | 在原 UoW 中记录变化并推进受影响 barrier，不能在提交后补写失效信号 |
| SDK/MCP 与 governed retrieval | 只读、宿主启用、完整最终守卫；不提供模型登记定义、解除失效或发布派生事实的权力 |

原则：纯契约 → 编排/整理 → ports → 存储适配器。依赖协议不导入执行器或数据库。
类型拆分须遵守既有架构检查，只允许纯契约作为 ports/domain 的新增依赖，不允许 service 倒流进 domain。
不按每个 schema 建一文件，不新建后台服务，不把所有逻辑塞入 kernel、domain 或 repository 主文件。
L0 与 L1 继续作为事实依据；Observation 不写回独立 Claim、不增加独立证据数、不替代 L1 的双时态投影。

## 3. 最小数据合同

| 对象 | 必须记录 |
| --- | --- |
| ObservationDefinition | 宿主身份、exact scope、canonical subject、facet、谓词/完整查询过滤、current_snapshot 模式、策略/模板版本、允许用途、fingerprint |
| DerivedRevision | view_id/revision/head generation、内容块、明确表达标签、构建 valid_at/known_at、有效覆盖、input manifest、query generation、实际处理集合、next_transition_at、状态/原因 |
| SupportDependency | 支持哪个输出块/字段、L1 assertion/record revision、现有 EvidenceSupport/AND/OR 依据与时间范围 |
| ProcessingDependency | 整理器实际读到的全部内容修订及相关 L0/资格证据、模板/配置身份、可信许可版本；是否引用不影响记录 |
| QueryDependency | 定义/正规化查询、subject/predicate 范围、上下文与时间、完整性、同快照捕获的 barrier generation |
| ProcessingGrant | 可信宿主登记的输入/处理许可、scope/用途/敏感级别上限、保留到期和安全版本；不信任模型或任意 source.metadata 自报授权 |
| BuildReceipt | 固定 build/request ID、定义/head/barrier 期望、实际完成责任、outcome、提交证明与未完成原因 |

support 表达“为何成立”；processing 表达“实际看过什么以及受何限制”；query 表达“集合是否仍完整”。三者分别持久化。
首版只允许声明的 L0/L1 父类型，不读取旧 Observation 正文作为生成输入；自引用、未知谱系、派生父输入与跨 scope 拒绝。
因此首版图严格为来源/断言 → Observation。通用传递派生/DAG 能力需要后续版本，不能只写一个 cycle 标志就声称完整实现。

处理许可不足则不把正文交给整理器。输出受全部 processing 输入的受众、用途、敏感级别和保留条件交集限制，
不能只采用 support 引用的限制。首版通过同 exact scope 保守保持受众，不开启匿名化后公开、动态 ACL 服务或级别转换。
可信宿主更新已登记许可须单调推进安全版本；更窄用途/到期/撤销立即阻断当前和历史正文交付，不能以旧版本授权。

## 4. 查询覆盖与原子发布

必须先实现 T26 的本范围屏障，再交付“当前完整” Observation。
采用一直维护的 subject/predicate barrier；查询前无需先搜索结果再注册订阅。
对记录接纳/资格更改/撤回、来源修订、初次解释闭合、重处理激活、删除及相关许可变更，在原事务中推进受影响的语义或安全代次。
facet 只读取宿主登记的谓词集合；不相干 subject/predicate 的普通变化不触发全库重建。
范围擦除可采用保守 scope safety epoch；未知影响范围时允许保守失效，不能漏掉变化。

1. **固定快照**：在 scope 锁和同一 UoW 中读取 barrier、定义/head、完整合格集合、反例/待决状态与可信处理许可；捕获实际修订。
2. **事务外准备**：完整输入清单由 service 构造，确定性整理器输出带引用的提案，不决定 scope/用途/发布权。
3. **同事务发布**：复查定义、head、每个输入修订、输入安全版本、query generation、scope epoch、租约和时间边界。
   一起保存派生修订、三类依赖、input manifest、实际处理覆盖、head 与刷新证明。
4. **发生变化**：CAS 失败则重算或保留为明确不可供当前交付的诊断结果；不得标 ready 或推进未覆盖水位。
5. **持续新增**：运行中变化持久形成后继责任，不扩张已经领取的有限集合或已返回的 build target。

首版查询只纳入已闭合初次解释/原子已激活解释中的合格 L1，过滤规则进入 definition fingerprint。
未闭合初次来源不成为完整整理输入；其最终 closure 必须推进相应 facet barrier，避免漏掉随后合格成员。
来源发布后新成员、首次空快照期间发布、准备期间发布、提交后发布均须被 barrier 捕获。
预算截断/存储错误/资格不支持为 incomplete/unknown，不等于 complete empty。

## 5. 失效、时间与读取

view 状态与 worker 状态分开：building/retry/dead 是执行状态；ready/stale/invalid/erased 是产物资格状态。
普通语义变化保留受允许的旧审计版本并将当前产物 stale；后台重建失败时仍不得作为当前事实。
许可撤销、保留到期或擦除走 safety 路径，立即阻断正文；擦除同时清理正文/块、历史、输入片段、索引/缓存及准备数据。
只保存内容最小化的失效原因和修订身份，不把已擦正文留在理由、队列或恢复日志中。

每次当前读取和最终交付再次执行：

- semantic guard：输入事实资格/修订、query generation、定义版本、条件上下文、双时态有效覆盖、冲突和 next_transition_at；
- current safety guard：所有 processing 谱系的当前许可、范围/用途、保留期限、来源存活与单调安全代次。

无新写入时，到未来生效/到期边界也立即停止使用旧快照。宿主通过既有有界 runner 执行到期刷新，读端不依赖它准时运行。
首版历史正文读取明确 unsupported；合法旧认知的审计版本与今天的安全守卫仍分别记录，供后续 T28 完整历史能力接入。
对象/范围删除和 PurgeRestore 回放共用完整清理路径，旧备份、旧刷新租约与旧有限目标均不能恢复被擦除内容。

## 6. 刷新队列的必要适配

当前 ResourceRefreshQueue 的每个工作单元要求 1…16 个仍存活的来源。
**不能直接用该合同表示“最后一个来源已经删除后的空 facet 刷新”**，也不能造一个假来源绕过检查。

新增宿主绑定的 facet 工作单元版本，依赖稳定 definition/facet 身份、scope epoch 和变更 generation，
而不是要求被删来源继续存活。真正参与生成的来源在本次 snapshot/input manifest 中逐项授权与复查。
旧来源工作单元保留原语义和删除阻断；两种合同显式协商，存储清理能够识别，不能宽松允许所有空 sources。
同 UoW 持久失效信号/刷新责任，复用现有资源 serialization、随机 lease、claim/requested 集合及 BoundedWorker。
安全失效不等待新任务容量；任务背压时仍保留可重试的权威失效责任。

完整查询为零成员时允许完成有限刷新：outcome=applied、no_outputs=true，原子清除当前可读 head 并记录固定覆盖，不能发出新的否定事实。
该完成回执只表示已处理目标，不表示存在 ready 正文；普通空集合保留允许的旧审计，擦除则清理所有受影响正文/历史。
内容不变也要重新核验依赖/授权/query 覆盖后才给 noop_verified；生成器返回空内容不自动推进水位。
定义变更创建新 definition generation，执行 full rebuild，不混用旧 checkpoint；本阶段不实现 delta 或通用定义迁移。

## 7. 实施顺序与退出条件

| 子步骤 | 交付 | 完成后可声明 |
| --- | --- | --- |
| D01 契约与能力边界 | 纯模型、定义/许可、错误码、目标/outcome、读写端口、支持矩阵 | 协议可评审，尚未启用 |
| D02 双后端持久层与变化屏障 | SQLite/PG 同合同表、反向依赖、单调代次、迁移、全部写入触发点 | T25/T26 基础能够拒绝旧快照 |
| D03 facet 工作单元与快照 | 队列兼容、空/删除责任、查询快照/授权清单/完整性 | 来源删除后仍可合法刷新 |
| D04 实际 Observation 构建 | 确定性 full 整理、输出逐项核验、CAS 原子发布/有限回执 | 一个 facet 可真实构建 |
| D05 全链守卫与擦除 | semantic/safety/time 守卫、修订/许可失效、完整擦除与旧备份回放 | 陈旧/越权/被删正文不可交付 |
| D06 宿主与只读 SDK/MCP | 注册/触发由宿主管理，版本化只读状态/读取及显式 derived_context | 两种客户端与真实读取闭环 |
| D07 专项验收与交付 | 双后端竞态/进程恢复/迁移、包构建、示例、阶段证据、提交推送 | 仅本阶段支持组合通过 |

D02 必须先于 D04；D05 是发布必需条件，不能上线 D04 后再补安全。
D01/D02 定义会合点后才能扩展其他 facet。最后以所有本阶段用例通过作为单次完整阶段交付。

## 8. 验证、容量与后续

仅新增专项和受影响的接纳、资格/时间、来源/重处理、多批发布、刷新、删除回放、SDK/MCP 和迁移回归，不跑全量测试。
重点矩阵见 [执行清单](stage-12-tasks.md)：少引用的私有输入、新成员/冲突、全部来源删除、时间到期、并发刷新、提交强杀与旧备份。
独立报告确定性协议正确性与真实质量；本轮不设置没有 gold 依据的模型质量阈值。

冻结首版安全容量：每快照最多 64 条 L1、128 个实际内容输入、512 条依赖边、16 个输出块、32768 个 canonical JSON 字符输出、
262144 个 canonical JSON 字节 manifest；每 facet 最多 128 个保留修订，每 scope 最多 4096 个修订，达到上限明确拒绝并保持失效。
这些是资源边界，不是质量阈值；D01 结合既有端口预算冻结，不能截断后仍宣称完整。
沿用刷新任务的活跃/重试/寿命限制，分别核对现有来源单元的 256 来源和 checkpoint 8192 字节上限；大准备数据不塞进旧 checkpoint。
采用新增 additive migration PostgreSQL 015；原事实/索引账本不重写。
升级前备份及重复初始化要验证；存在新派生账本/安全屏障时不能盲目降级，只关闭派生 capability 保留 L1 读取。

本阶段通过后，再按依赖推进：通用 query/动态 ACL 和历史派生读取 → L2 Scenario / L3 Core-Persona → 版本化知识页面与 delta。
独立 journal 部署/规模恢复、真实领域 gold 和外部模型治理继续单独验收。

## 9. 1.1 实施落点与验收

D01–D07 全部完成，详见 [实施记录](stage-12.md) 和 [机器验证记录](validation-stage-12.json)。
facet 单元采用 sibling FacetRefreshQueue，避免旧 source 单元错误依赖已经删除的来源；执行器仍是原 BoundedWorker。
准备数据不落 checkpoint，确定性重建重取授权输入；完整零输出保留无正文的审计修订及依赖证明。
语言 facet 没有 QueryContext，条件/例外/否定明确 unsupported；此限制不计作条件化能力通过。
本版细化执行落点，不修改冻结设计，不声明通用 DAG、历史派生或完整 P3/M2 已完成。
