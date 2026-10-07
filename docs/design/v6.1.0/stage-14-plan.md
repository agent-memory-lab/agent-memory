# 第十四阶段方案：查询控制、权限与历史覆盖

版本 AM61-ST14 / 1.1；2026-10-07；IN_PROGRESS；执行台账 revision 20。
依据冻结 v6.1.0，推进 T25/T26/T28/T29 及协议、恢复与运行文档的必要切片。
任务见 [stage-14-tasks.md](stage-14-tasks.md)，A 切片结果见 [stage-14a.md](stage-14a.md)，B.1 结果见 [stage-14b.md](stage-14b.md)。

## A：当前查询与本地 authority（已实现）

1. `QueryDefinition` 明确 ID/version、exact scope、主体、完整谓词槽集合、current 时间模式和 all_candidates 成员规则。
   不提供 accepted-only、value、confidence、grant 或 top-k 过滤；反例与待核验输入必须进入处理权限检查和 manifest。
   注册合同可以表达多个已登记谓词，但 consumer 必须支持整个查询；当前语言 renderer 只接受 locale 集合，不把其他谓词悄悄丢掉。
2. 登记 query 使用 CAS，当前 generation/hash 进入 opt-in `facet-refresh-unit/2`。
   查询变更与所有订阅定义的 dirty outbox 同事务写入，固定已领取单元不能漂移。原逐槽新成员屏障继续始终维护。
   当前语言 consumer 的谓词/主体变更必须兼容；不兼容时拒绝整笔更新。
3. `HostGrantAuthority` 是可信本地宿主的有期限授权记录，含 ID、读者、用途、期限和撤销状态。
   服务构造绑定 authority ID，宿主 `set_authority` CAS 推进单调版本；期限最多 24 小时。
   启动绑定服务必须显式提供由宿主从备份之外取得的 `authority_min_version`；0 仅用于全新 authority 创建，不能读取或续改已存在的 authority。
   `set_authority` 成功提交后才推进服务 floor；宿主必须在采用新 ACL 前把返回版本持久保存到独立于数据库备份的位置。
   恢复时 authority 行低于这个 floor 立即拒绝，不能降低 floor 或直接读取备份里的版本来证明当前权限。
   宿主必须先提交该 ACL 修订再采用该修订；本地期限不能证明未同步的远端 ACL，capabilities 明确 remote_acl=false。
4. 来源 `ProcessingGrant` 绑定 authority ID/version，读者和用途不得超出 authority。
   authority 的任何替换或续期使旧 grant 不再有效；必须显式重新授权来源，不能自动复活旧输出。
   本轮不新增远端 ACL provider，不把 authority 同名字符串当成身份认证凭证；写方法只可由受信宿主调用。
5. 发布和当前交付复核 registry、query/authority 代次、完整候选、实际来源权限、正文版本、租约、head CAS、时间覆盖和擦除屏障。
   发布先授权实际查询全集再读取任何 L0/L1，不能仅相信调用者提交的 manifest 子集。
   期限进入下一重建边界；空重建也受当前 authority 守卫。SDK/MCP 保持只读，最终交付重新读取。
6. authority/context 路由隔离领取与状态访问；未绑定宿主不能覆盖已绑定定义或来源 grant。
   可以用明确 expected_version 将原本未绑定的来源 grant 迁入 authority；原未绑定消费者此后不能读取该受保护来源，迁移需要同步重绑消费者。
7. scope 擦除使 query/authority 只剩单调墓碑，清除主体、读者、用途和期限；对象擦除继续撤销来源 grant、清除相关派生正文与完整 manifest。
   两个后端的擦除枚举包含新 ledger kinds；原删除日志备份回放复用同一擦除计划。
   本切片验证旧权限备份被独立 floor 阻断；当前 authority 控制状态的恢复须走宿主受信恢复流程，不提供降低 pin 的自动修复。

## B：历史 coverage（B.1 已实现，完整 B 尚未完成）

B.1 开放宿主显式启用的 `published-point/1`：

1. `known_at` 必须精确命中已成功发布的完整候选检查点；`valid_at` 独立投影该认知下的事实。
   认知时间由发布事务内的宿主时钟写入，不能用调用者的快照时间证明更早覆盖。
   检查点之间、迁移以前、未发布时段均不插值或回填；两个时间必须同时提供、有时区，未来请求拒绝。
2. 同一次事务保存不可变的定义/代次、query/代次、政策、完整 L1 行版本、来源解释/文档版本、输入 manifest、空覆盖及完成证书。
   普通修订、更正或撤回不重解释已发布的旧认知；读取使用冻结合同和候选行，不读取最新 L1 作为旧语义。
   同时刻相同语义检查点去重；不同语义冲突使新发布整体回滚。
3. 历史语义仍受当前定义读者/用途、当时读者/用途、当前 authority/floor、全部实际来源 grant 和物理删除屏障约束。
   检查权限先于历史正文；authority 续期后必须重新授权来源。历史时间不会复活旧权限。
   对象擦除保守清除相关 facet 的所有历史正文及覆盖点，包括此前空查询；备份删除日志回放执行相同计划。
4. SDK/MCP 只提供 `history_points`、`read`、`derived_context` 等读取能力；最终交付的两次读取都保持原 known_at/valid_at。
   `capabilities` 仅在宿主及定义 opt-in 时声明 `historical_mode=published-point/1`，历史 renderer 仅 `locale-snapshot/1`。
   当前 `locale-context/1` 的期限/上下文代次合同不能冒充历史 context；条件化历史显式不支持。
5. 每 facet 最多 128 个覆盖点，每 scope 最多 4096 个；每检查点归档上限 256 KiB，保留既有候选/来源/输出上限。
   容量不足拒绝整笔发布，墓碑不能被重新分配；分页/压缩属于后续规模任务。

B.2 下一步补足连续知识时间覆盖：先设计候选、解释、query/definition/policy 的版本区间及事务起点，
再证明已发布点之间的完整成员变化和空区间；无法证明的区间仍返回 coverage_unavailable。
B.3 随后定义历史 QueryContext、适用范围/偏序及资格证据版本；复用当前 authority 与删除守卫，
验证条件历史及政策迁移矩阵后才开放 `locale-context/1` 的历史读取。
因此 B01–B04 的完整上下文/连续覆盖验收仍是 IN_PROGRESS，B.1 不等于完整 T28 或 M2。

## C：传递 processing 图（B 后，未实现）

显式绑定每个实际派生父版本及输入 manifest，验证当次实际使用而非事后支持引用。
计算所有实际父/来源处理许可的交集，限制循环、深度、数量及输出容量；父变更/撤销/删除必须传递失效并物理擦除。
不接受未知父、自己或当前未支持的派生父，验收通过后才允许新的依赖类型。

## 责任边界与退出条件

- derived/contracts.py：QueryDefinition/HostGrantAuthority/HistoricalQuery 纯合同。
- derived/registry.py：宿主登记、CAS、控制证明和权限交集；不依赖数据库实现或 renderer。
- derived/model.py：facet/unit 合同与共享擦除计划。
- derived/service.py：授权快照、纯准备、原子发布与最终交付；renderer 不管理 authority。
- derived/history.py：有界不可变检查点、历史语义重建与当前安全守卫；复用同一 UoW，不新增数据库适配器。
- operations/facet_refresh.py：既有有限单元队列、路由、租约和固定回执。
- SQLite/PostgreSQL：复用原 ledger/UoW/锁和索引，当前无需新表或 DDL migration。

旧定义没有 query_id/authority_id 时不增加序列化字段，保持 v1 fingerprint/unit 身份；升级不会自动绑定权限或查询。
SQLite 同步事务的跨连接竞争在独立线程/事件循环中执行，PostgreSQL 使用异步连接；不新增异步 SQLite I/O 实现。
每切片跑新增与受影响测试、真实双后端及相应恢复验证，独立记录结果；不默认全量测试。
第十四阶段全部完成需 A/B/C 各自验收；A 完成不等于 T25–T29/M2 全部完成。
