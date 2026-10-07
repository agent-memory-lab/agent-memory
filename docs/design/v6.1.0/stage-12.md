# 第十二阶段：受控 Observation 完整闭环

版本 AM61-ST12-IMPLEMENTATION / 1.0，2026-10-07；台账 revision 17。
实施依据：[阶段方案 1.1](stage-12-plan.md)、[阶段任务](stage-12-tasks.md)、冻结设计 v6.1.0。
本阶段 D01–D07、ST12-01–ST12-10 完成；T25–T29 仍按切片记 IN_PROGRESS，B07/M2 未整体验收。
实际证据：[validation-stage-12.json](validation-stage-12.json)。不把规划验证记录或旧测试成绩当作本轮运行结果。

## 已启用的组合

宿主显式启用；同 exact scope、canonical subject 等于 scope.user_id；登记的字符串谓词 `locale`；
`communication.language` 当前状态快照；确定性 full rebuild；一个或多个来源。
原始事实来自已闭合的初次 primary 发布或已原子激活的替换解释；独立终止依据作为 L0 上下文输入也必须有宿主许可。
普通 L1 检索保持原入口；SDK/MCP 仅通过 `memory_derived` 显式读 Observation 或请求 `derived_context`。

每个值保留 candidate、有效区间、来源证据和 `source_assertion` / `assumed_continuity` 支持方式。
CONTESTED 返回冲突块；待核验候选不变成已接受事实。多个来源不增加置信概率，Observation 不回写独立 Claim。
包含条件、例外、否定或 qualification 的 locale 输入明确返回 `derived_qualification_unsupported`，不删除限定后发布无条件事实。

## 代码架构和事务边界

| 模块 | 责任 |
| --- | --- |
| `derived/model.py` | 定义、可信 ProcessingGrant、版本化 FacetRefreshUnit、纯身份/边/擦除计划合同 |
| `derived/service.py` | 宿主注册与许可 CAS、授权快照、输出验证、发布、读端和交付端守卫 |
| `derived/observation.py` | 复用 L1 双时态投影，校验原话依据，确定性构造事实/冲突块和下一时间边界 |
| `operations/facet_refresh.py` | 固定查询工作单元、有限目标、去重、排他租约、随机 fencing、尝试/寿命限制和后继调度 |
| `operations/sqlite_derived.py` / PostgreSQL `derived.py` | 元数据查询头、版本账本、三类反向边、删除及恢复的 SQL 适配 |
| `ports.py`、既有 repository/UoW | 可选同事务端口；在原接纳、解释切换和删除事务中维护 query/safety 屏障 |
| core MCP / SDK / MCP server | 同一个只读业务合同；宿主绑定 scope/actor，模型参数不提供登记、许可或发布权力 |

队列使用独立的 `facet-refresh-unit/1` 合同和账本种类，复用现有 BoundedWorker。
旧 ResourceRefreshQueue 来源单元及其所有来源守卫保持原合同；没有把空 facet 伪装成空来源列表。
这项落点细化见方案 1.1；没有新增 daemon 或第二套 worker runtime。

SQLite 用 BEGIN IMMEDIATE；PostgreSQL 与原 admission/deletion 一样先取 namespace 锁。
源码入口、两后端和独立连接竞争均有专项覆盖。准备阶段在事务外，但事实正文只能在快照授权之后进入整理器。

## 写入、失效和重建

1. 每次 admission 保存同时维护不含正文的查询元数据头、slot generation 和匹配定义的 durable dirty 标记。
   屏障始终维护，不依赖“查到成员以后才订阅”；没有定义时的新成员也会推进屏障。
2. 初次 interpretation closure 和原子重处理 activation 推进对应 slot generation。
   来源修订、资格/更正/终止沿既有 admission 保存入口推进同样屏障。
3. 宿主 ProcessingGrant 按版本 CAS 更新；用途与受众取所有实际输入的交集，敏感级别取最严格、保留类别取最短。
   不读取来源 metadata 中的“自授权”。未引用的背景输入仍纳入 processing 清单与删除谱系。
4. dirty 是原写事务中的刷新责任；队列背压不丢失标记。领取后单元包含固定定义、query、安全、时间和 epoch 代次。
   新变化成为后继单元，不扩大旧领取范围或旧目标。
5. snapshot 先检查所有来源的许可，再读取 L0/L1 正文；固定完整候选集合、反例、解释头、文档头、输入版本和配置指纹。
   预算溢出、资格不支持或闭合失败是 incomplete，不是零输出。
6. 发布再次检查完整查询集合、定义与输入版本、许可、head CAS、随机租约、epoch 和时间覆盖。
   修订、三类边、manifest、head 和完成 token 一起提交或一起回滚；输出必须等于确定性重新验证的结果。
7. 同内容仅在全部依赖和授权重新验证后可记 noop；仍发布新的不可变修订和当前覆盖，不复用旧清单。
   内容相同但权限、配置、事实或查询代次改变时不能凭文本 hash 跳过检查。
8. 完整零输出保存不含正文的 empty audit revision 和查询/processing 证明，原子清除 readable head。
   结果是 `applied/no_outputs=true`，不产生“用户没有偏好”的事实。

准备数据仅在进程内短暂存在；不把正文塞入旧 checkpoint，也不持久化可被复用的旧准备文本。
提交前强杀恢复重新取得授权快照；提交后强杀按原子完成证明终结，不重复发布。

## 读取和删除

读端检查定义/query/safety/time/epoch、当前事实版本、来源头、许可、修订正文及清单完整性。
`ready/stale/invalid/erased/empty` 描述产物资格；`pending/running/retry/dead/superseded/completed` 描述工作状态。
时间到 `next_transition_at` 后直接阻断旧正文，不依赖新写入或 worker 已运行。
显式 derived_context 在最终交付边界再次读取；撤权、删除或时间变化不因缓存而被绕过。
历史参数始终返回 `derived_history_unsupported`，不使用当前结果替代历史结果。

删除在原 UoW 中清除受影响 facet 的所有修订正文、输入 manifest、反向边与 readable head。
当前和历史受限正文均不再可读；只保留最小身份和状态。
L1 删除可能连带撤回同槽其他候选，派生查询头也在同一事务中与这些实际结果同步，避免删除后永久重建失败。
对象删除后的稳定 facet 仍可完成空重建；scope 擦除取消旧任务和目标、禁用旧定义，新的 epoch 不复用旧 target。
PurgeRestore 调用同一删除边界；真实 SQLite backup 和 PostgreSQL pg_dump 回放均覆盖新表，旧正文不能复活。

## 容量、运行和升级

| 资源 | 首版上限 |
| --- | --- |
| 完整 L1 查询集合 | 64；SQL 取到第 65 条即报不完整 |
| 实际内容输入 | 128 个 L0/L1 输入 |
| 依赖边 / 输出块 | 512 / 16 |
| 输出 / manifest | 32768 个 canonical JSON 字符 / 262144 个 canonical JSON 字节 |
| 修订 | 每 facet 128；每 scope 4096 |
| 定义 / grant | 每 scope 128 / 4096 |
| 活跃单元 / 全部单元 / 有限目标 | 默认 128 / 4096 / 4096 |
| 失败尝试 / 默认寿命 | 默认 3 次 / 86400 秒；随机租约和任务截止期同时复核 |

达到上限明确拒绝，不截断后宣称完整。旧来源刷新继续沿用原寿命/无进展/checkpoint 合同；
facet 单元只在整次原子提交时记录进度，由 BoundedWorker 的超时、租约、截止期和尝试上限约束。
长期规模清理和修订归档尚未启用，达到保留上限需宿主处理，不能静默覆盖旧版本。

PostgreSQL 新增 additive `015_derived_observations.sql`；SQLite initialize 加入等价表。
升级与重复初始化从旧 primary 数据补查询元数据头，不重写事实、来源或索引账本。
降级时关闭派生 capability，保留新账本和安全屏障；不盲目删除新表。

运行示例：安装 core + Python SDK 后执行 `python examples/derived_observation.py`。
示例贯通持久接收 → L1 → Observation → SDK derived_context；运行结果已核对 `ready/zh-CN`。
宿主可用 `ObservationService.register/grant` 和 `FacetRefreshQueue.request`；模型仅有 capabilities/read/status/derived_context。

## 验证和剩余范围

新增 90 项行为专项、1 项纯契约架构检查、1 项迁移检查；与受影响回归合并后 558 个唯一用例全部通过、0 跳过。
重复运行取每个用例的最新结果，不累计次数。包括 4 次新真实 SIGKILL、4 次新真实备份回放及独立连接发布/删除竞争。
四个 wheel（core、PostgreSQL、SDK、MCP）与当前源码核对，015 在 PostgreSQL wheel 内。
只运行专项和受影响回归；未运行全量测试。确定性协议正确性不等于真实对话抽取质量。

明确关闭：其他 facet、条件化 Observation、跨 scope 合成、派生父输入/通用传递图、动态 ACL 服务、历史派生、
时间线叙事、delta 页面、L2/L3、外部 LLM/模型 dispatch。
下一步优先扩展可信 QueryContext 与资格保持的 facet；再处理通用查询/权限图和历史解释；随后推进 Scenario 与版本化知识页面。
真实 gold、外部模型费用/许可治理与发布门继续独立验收。
