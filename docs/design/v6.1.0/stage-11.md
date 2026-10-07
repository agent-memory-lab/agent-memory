# 第十一阶段：单请求分批发布与完整清单覆盖

编制：2026-10-07；设计保持 v6.1.0，计划/任务台账 revision 15。承接 [stage-10](stage-10.md)。
本阶段完成 T16/T22 的单请求多发布闭环，并覆盖其提取、索引、恢复、重处理、删除和客户端依赖。
44 项任务仍为 DONE 1、IN_PROGRESS 24、TODO 19；本阶段完成不等同于整个 P2、B03/B04 或 M0/M1 完成。

## 架构与职责

| 模块 | 责任 |
| --- | --- |
| `operations/publication_manifest.py` | 版本化清单、不可变宿主策略、批次内容与计划摘要、提交令牌、完整性验证、逐令牌投影 |
| `operations/publication_batches.py` | 整事实槽分组、准备产物切片、事务协调、历史回执聚合、初次发布续跑、原子替代的批次证明 |
| `operations/extraction_worker.py` | 原有领取/租约/输入/配置/来源守卫；宿主显式恢复终止失败请求；选择旧路径或新分批路径 |
| `operations/indexing.py` | 每个实际批次的同事务 outbox、定位条目与完成证明、全部令牌及连续前缀覆盖 |
| `operations/index_recovery.py` | 对单个批次实际修复；从当前有效的已提交批次构建新流基线，包括尚未闭合请求 |
| `operations/readiness.py` | 固定请求目标与阶段状态；复用清单协议，不推测未提交批次或索引完成 |
| `source_revisions.py` / `reprocessing.py` | 修订/删除仍阻断旧执行；未闭合初次解释不得成为整来源重处理基线 |
| SQLite / PostgreSQL | 继续通过原有 UoW 和 scope 锁保存 JSON 账本、候选/历史、head 与索引任务，无新增数据库或调度服务 |

清单协议不依赖执行流程或数据库适配器。`readiness.begin/close` 保留既有导入兼容；统一验证归清单协议所有。
SDK/MCP 查询继续透传版本化清单，contracts 新增支持的清单 schema 列表；发布策略和恢复入口由可信宿主绑定，不成为模型写入工具。

## 启用合同与语义

宿主同时向 `processing_configuration_sha256` 和 `DurableAtomHandler` 传入 `PublicationPolicy(batch_size=N)`。
策略被纳入处理配置摘要，不能在已有请求中改变分组规则。未传策略时，旧配置摘要、候选身份、单次原子发布和 v1 清单保持原样。

先完成整个来源的有限生成和审查，并保存 prepared/input_manifest；再按 subject/predicate/scope 定义的事实槽分组。
同槽候选始终在一个接纳事务内判定，冲突值不会因先后发布变成覆盖关系。batch_size 是目标大小，不是拆开同槽的硬上限。
非连续出现的同槽候选也归并；相同候选沿用接纳引擎去重。各批切片保留完整来源输入指纹和已审查 gates，局部 draft_index 正确重映射。

初次请求可逐批公开已接纳事实。部分成功表示已提交事实可读，**不表示整个请求 L1 完成**。
每批保存当前解释 head 的已提交候选集合，并标记 publication_closed=false；其代次/身份/集合在每次后续写入前检查。
全部批次提交后，单独的 closure 事务才把 head 标为闭合、清单 closed=true、请求 completed，并移除 prepared。
需要整来源解释作为基线的 snapshot/submit 在闭合前返回 initial_interpretation_not_ready。

整来源 additive/replace_interpretation 沿用完整旧/新并集复核和 head CAS。
其候选更改、撤去旧支持、激活 head、多个逐槽发布证明和 outbox **仍共用一个事务**；不存在逐批替换的可见中间解释。
多个证明对应实际提交的互不重叠责任集合，不声称替代请求能独立部分激活。

## 清单与完成判断

publication-manifest/2 固定 mode、policy、plan、prepared_sha256 和 plan_sha256。
每个 publication 保存 batch_index、实际接纳回执、互不重叠 dispositions、committed_at 和 token。
令牌绑定 exact scope、epoch、请求、配置、整个计划摘要以及本批内容；既有提交的令牌和回执不会因后续批次或恢复被重写。

version = 已提交执行批次数 + 闭合位；批次索引必须是从 0 开始的完整前缀。
空生成保留一个空执行批次，只有完成该批并闭合后才报告 no_outputs=true；不为零输出制造发布令牌。
no_indexable_outputs 按聚合回执中的 Claim 判断，待决/拒绝候选可以产生发布令牌而没有新 Claim。
closed=false 时两个 no_* 字段保持 null。聚合结果必须与各批不可变历史回执一致，不能凭当前可变候选动作拼接完成结果。

L1 reached 要求固定目标内每个请求的合法清单均闭合且 completed。
index_visible 还要求每个发布令牌的实际定位条目/证明和连续索引前缀均到位；后面的批次先完成不能越过缺口。
部分请求已提交的批次可以提前索引、修复和切流，但不会因此变成整请求 reached。
有限目标只固定请求，不预先猜测其最终令牌数；后续输入、不同处理请求和新索引流仍不扩张旧目标。

## 失败、恢复与删除

每批候选/Claim/历史、解释 head、清单回执和 outbox 同 UoW 提交；任何写入/背压失败都回滚当前批次。
之前已提交的批次保留。新随机租约恢复时复用整来源 prepared，跳过已提交前缀，只发布剩余批次或补 closure。
每次实际提交仍复查配置、来源当前修订、scope epoch、租约和 head。旧 worker、修改的准备数据或不同 head 不能接续写入。

重试次数耗尽后请求 dead，有限目标 failed，不自动把部分成功说成完成，也不自动无限重试。
宿主可以显式调用：

```python
await extraction_queue.resume(
    request_id,
    expected_manifest_version=version,
    actor="operator",
    reason="index capacity restored",
)
```

仅接受当前来源、相同配置、合法未闭合 v2 清单、有已提交批次和 prepared 的 dead 请求。
恢复按版本和状态 CAS，去掉旧租约、重置本轮尝试次数，并保留 actor/reason/次数/时间审计；已有回执、计划和版本不变。
重复 queued/completed 请求、过时版本、删除、配置变化、损坏清单及超过恢复上限均拒绝。它不重新审查旧事实或绕过来源守卫。

索引任务只保存自己的 token_dispositions；apply/coverage/repair/rollover 都复用同一个逐令牌映射。
新流基线包含当前合法来源已实际提交的全部批次；同请求同时间按批次序号稳定排序。
切流发生于请求运行中时，已有批次在基线里，后续批次入当前新流；旧有限目标和旧流租约继续受原合同约束。

对象/scope 擦除或来源修订同步使未完成执行失效，清理 prepared、输入、聚合结果和整个发布清单。
已发布候选、历史及各索引流沿原完整删除/修订路径处理。第十阶段离线恢复复用完整删除事务，所以旧备份里的部分清单、旧租约和阶段数据不能恢复已删来源。
归档仍遵守既有不可读合同；没有新增外部供应商/缓存擦除声明。

## 容量、升级与操作

- 一次来源生成仍有 32 默认 / 64 配置上限，内容及 prepared 预算沿用既有合同；本轮不增加无限流式生成。
- batch_size 为 1…64；初次请求最多 64 个执行批次；原子替代并集最多 128 个责任身份/批次，active 解释继续受 64 上限约束。
- 清单 JSON 最多 320000 字节；每请求显式恢复最多 32 次；每轮自动尝试沿用队列 1…100 配置上限。
- 索引仍有 128 活跃 outbox、100000 历史任务、切流 256 发布/4096 候选等原限制；本轮不提高这些容量。

升级 core 即可使用新 JSON 合同，无新增 DDL；真实 PostgreSQL 使用现有 migration 014 初始化并验证。
接入方应配套更新 worker/索引/恢复/readiness 代码。存在 v2 账本时不能直接降级到只理解 v1 的旧 core。
停止新请求后应先完成或显式处置未闭合请求；不能通过改 closed/status 字段、重建新 ID 或丢弃清单绕过恢复协议。

`examples/publication_batches.py` 展示真实 SQLite、一个来源、两个事实槽、两个发布令牌及全部索引覆盖。
运行 `.venv/bin/python examples/publication_batches.py`；宿主构造处理策略，SDK 负责固定目标与状态查询。
未配置 candidate-locator 的宿主仍可启用分批 L1；index_visible 延续 unsupported，而不是推测索引已经完成。

## 验证与后续

本轮仅运行新增专项及受影响回归，最终 **575 项唯一用例全部通过、0 skipped**，其中 83 项新增行为、1 项新增架构检查。未运行全量测试；重复运行不累计数量，分次结果与指纹见 [验证记录](validation-stage-11.json)。
覆盖 SQLite 与真实 PostgreSQL 17、embedded/MCP、同槽冲突/去重、零输出/全部待决/拒绝、64 批上限、
提交回滚与阶段复用、闭合前索引、终止后显式续跑、配置/阶段/head/旧租约守卫、损坏证明、索引修复/切流、
来源修订/删除、实际旧备份回放，以及批次/闭合/替代提交前后共 12 项新增真实 SIGKILL。
进程强杀不是数据库服务器断电；确定性夹具不代表真实抽取质量。

下一阶段建议完整推进受控本地派生依赖闭环（T25/T26/T27）：区分 support/processing 边、固定输入修订与用途交集，
让既有资源刷新在删除/修订后实际失效并重建一个具体派生投影。query 新成员/动态 ACL、真实领域 gold/校准、
外部模型治理和独立 journal 部署/规模恢复继续按其验收边界推进。
