# 第十四阶段 B.2：连续知识时间覆盖

版本 AM61-ST14-B.2 / 1.0；2026-10-07；DONE（有界切片）；台账 revision 21。
设计依据保持冻结 v6.1.0；实现基线 `fac9eb41c6a75e147821862b4ebb6c43014cdede`。
方案见 [stage-14-plan.md](stage-14-plan.md)，任务见 [stage-14-tasks.md](stage-14-tasks.md)，机器证据见 [validation-stage-14b2.json](validation-stage-14b2.json)。
原有未提交的架构文档保持字节不变，不纳入本次提交。

## 交付行为

宿主与定义同时选择 `history_mode="published-interval/1"`，绑定 query 和 authority，才能启用。
完整发布保存冻结定义、query、政策、L1 候选全集、来源版本和完成证书，并开启知识时间覆盖。
已证实的区间为 `[known_from, known_to)`；开放区间只能读取到当前可信宿主时钟，未来请求拒绝。
`known_at` 选择当时认知，`valid_at` 独立投影该认知下的现实有效事实。

候选、新成员、来源解释、文档 head、query、定义或政策首次改变，在原写入事务内关闭旧区间。
变化到下一次完整发布之间返回 `derived_history_coverage_unavailable`，不会用前后检查点插值。
空全集也有完整覆盖证明；新来源文档进入时即保守关闭同范围区间，不等待 L1 产生。
普通更正或撤回保留旧区间中的旧认知，后续变化不会延长第一次关闭边界。
权限续期、grant 变化和 valid-time 边界不改变旧认知；当前安全独立复核。

开放区间校验定义/政策、query 屏障及全部来源解释/document head 摘要。
新检查点只有在这些版本一致时才把旧开放区间封存为稳定覆盖；发现未跟踪变化则标记 uncertain。
没有区间证明、损坏证明、读时钟落后写入前沿或历史缺口时均拒绝，精确点仍使用原不可变检查点合同。
写入时钟倒退使已有区间 uncertain；新的完整检查点达到前沿后可以重新开启覆盖，不伪修复过去缺口。

## 架构与原子性

- `derived/history.py` 负责不可变归档、点证明、历史解释和当前授权。
- `derived/coverage.py` 负责区间元数据、首次变化关闭、输入摘要及可信时钟前沿，不依赖数据库或读取正文。
- `derived/service.py` 与 `derived/registry.py` 在既有 UoW 内协调发布、候选和控制变化。
- SQLite/PostgreSQL 在候选、解释和文档原写入事务中调用共享关闭逻辑；定义/query 变化与 dirty outbox 保持同事务。
- 可选 `DerivedCoverageUnitOfWork` 的 `write-hooks/1` 是必要后端合同；不具备时拒绝区间能力，不影响原点模式。
- 复用原 ledger 的 `history_interval` kind、scope 锁、租约、CAS 和完成证书；无需新表、独立队列或 DDL migration。

每 facet 最多 128 个历史点及对应区间，每 scope 最多 4096 个点，归档上限 256 KiB。
容量不足或发布时间早于控制前沿，整笔发布回滚。区间证明保存版本和摘要，不复制来源正文。

## 权限、删除与宿主接入

读取先验证当前定义与当时定义的 audience/purpose、当前 authority/floor、全部实际来源 grant 和完成证书，
随后读取历史归档；当前非引用来源撤权也会阻断交付。历史时间不恢复过期或撤销权限。
物理擦除清除受影响 facet 的全部归档和点/区间证明，包括过去空全集；真实备份删除日志回放复用同一清理计划。

SDK/MCP 沿用只读 `history_points`、`read` 和 `derived_context`。
覆盖返回区间起止、右端不包含和本次观察时间；发现列表包含各点的区间状态。
最终两次读取保持原 known_at/valid_at；其间普通语义变化仍可读取已经封存的过去，撤权则阻断交付。
capabilities 显示 `published-interval/1` 与 `certified_intervals`，仅支持非条件 `locale-snapshot/1`。

启用前升级同一存储的全部写入进程，统一可信 UTC 时钟域；不支持旧/新写入二进制混跑、直接 SQL 或绕过 UoW。
输入摘要是对遗漏变化的防御检查，不能代替写入合同或证明被绕过写入后又复原的状态。
`published-point/1` 保持原义；切换定义采用正常 CAS，不给旧点之间回填区间。
原代码不能证明新启用的区间；回退时关闭区间服务，恢复原点/当前服务需显式匹配定义及当前权限合同。
运行示例：`python examples/derived_interval_history.py`（安装 core 与 Python SDK）。

## 验证与后续

最终新增及受影响回归 818 项全部通过，0 failed、0 skipped；没有运行全量测试。
新增连续覆盖用例 78 项，SQLite/真实 PostgreSQL 17 各 39 项；包含候选/query/发布提交前后的 12 项真实 SIGKILL。
另外验证独立连接发布竞争、时钟倒退/前沿、未跟踪变化、当前权限先于正文、迟到成员、实际撤回、
真实 SQLite backup/pg_dump 删除日志回放，以及 SDK/MCP 最终固定双时间交付。
安装新 core/PostgreSQL wheel 后另复核 66 项和四个示例；12 项 SIGKILL 已在源代码真实进程验证，安装重复成绩不计入唯一总数。
core/PostgreSQL 的 sdist/wheel 均构建成功，172 个源码/类型标记文件逐字节匹配测试版本；SDK/MCP 本轮无生产代码改动。
合成协议测试不代表真实抽取质量或远端 ACL 同步保证。

下一步 B.3：冻结历史 QueryContext、资格证据、适用范围与偏序版本；验证条件/例外/争议、政策迁移、
迟到更正、当前权限和恢复矩阵后再开放历史 `locale-context/1`。
完成 B01–B04 整体验收后推进 C 的传递 processing 图，随后 L2 Scenario/版本化页面 full rebuild。
完整 T28、第十四阶段及 M2 保持 IN_PROGRESS；不声明任意时间的完整事件重建或条件历史已经实现。
