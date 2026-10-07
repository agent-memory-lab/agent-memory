# 第十四阶段方案：查询控制、权限与历史覆盖

版本 AM61-ST14 / 1.3；2026-10-07；IN_PROGRESS；执行台账 revision 22。
依据冻结 v6.1.0，推进 T25/T26/T28/T29 及协议、恢复与运行文档的必要切片。
任务见 [stage-14-tasks.md](stage-14-tasks.md)，A 切片结果见 [stage-14a.md](stage-14a.md)，B.1 结果见 [stage-14b.md](stage-14b.md)，B.2 结果见 [stage-14b2.md](stage-14b2.md)，B.3 结果见 [stage-14b3.md](stage-14b3.md)。

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

## B：历史 coverage（B.1/B.2/B.3 的有界语言组合已验收）

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
   B.1 当时未开放条件历史；B.3 另定义冻结上下文合同，不能把当前上下文自动当作过去认知。
5. 每 facet 最多 128 个覆盖点，每 scope 最多 4096 个；每检查点归档上限 256 KiB，保留既有候选/来源/输出上限。
   容量不足拒绝整笔发布，墓碑不能被重新分配；分页/压缩属于后续规模任务。

B.2 已开放显式 `published-interval/1` 的非条件 `locale-snapshot/1`；B.3 在同一覆盖合同上扩展条件 renderer：

1. 完整发布开启 `[known_from, known_to)`，候选、解释、文档、query 或定义/政策的首次变更在原 UoW 内关闭它。
   变化发生后到下一次完整发布之间是缺口；空全集也使用同样证明，不能把缺口当作没有事实。
2. 覆盖点与输入版本摘要、查询屏障、区间证明同事务提交。当前开放区间复核语义单元和来源 head 摘要；
   连续两个检查点之间只有版本一致才封存覆盖，检测到未跟踪变化则标为 uncertain。
3. 当前权限/续授权与 valid-time 边界不改写旧语义区间；交付仍先复核当前 authority、完整来源 grant 和删除屏障。
   擦除及真实备份回放清除相关 facet 的全部区间证明，包括此前空覆盖。
4. 后端必须声明 `write-hooks/1`。启用前同一存储的所有写入进程必须升级；不支持旧/新写入二进制混跑或绕过 UoW。
   发布/写入使用同一可信 UTC 时钟域；时钟倒退使已有区间 uncertain，写入前沿领先读取时钟则拒绝区间。
   不从迁移前状态回填历史；原点模式仍保持原义，B.2 单独不等于历史资格或完整 T28/M2 验收。

B.3 已开放宿主显式绑定 `context_token` 的历史 `locale-context/1`，支持上述两种 mode：

1. 冻结当时可信路由属性、时区、snapshot token、范围/偏序政策、L1 资格与字段证据版本。
   请求只选择 known_at/valid_at，不能从 SDK/MCP 提供或替换历史属性、政策或资格。
2. 上下文认知期限约束 known_at，独立 valid_at 可以早于上下文登记或晚于其到期。
   这表示在已登记路由下投影事实时间，不证明路由属性是过去现实中的事实。
   当前新处理仍受上下文到期约束；历史读取不要求旧上下文今天仍有效，当前权限与强制用途限制继续检查。
3. 发布点保存上下文摘要及认知期限，区间上限取首次语义变化与旧上下文到期的最早边界。
   新上下文登记或到期后的变更不能延长旧区间；切换路由 token 后使用当前匹配宿主读取旧归档，旧宿主路由拒绝。
4. 历史条件/例外保持三值；weekday 使用 requested valid_at 与冻结时区；AND/OR 字段支持、间隙和 point 边界独立投影。
   高优先解释的争议、未知或支持失效不回退低优先事实；历史缺资格/政策不套用今天的版本。
5. 双后端实际来源撤回、政策迁移、跨连接上下文/发布竞争、真实 SIGKILL、备份删除回放及固定双时间 SDK/MCP 已验收。
   B01–B04 的本阶段有界语言组合标 DONE；完整 T28、一般谓词/跨存储范围、M2 及第十四阶段 C 仍待后续验收。

## C：传递 processing 图（下一步，未实现）

显式绑定每个实际派生父版本及输入 manifest，验证当次实际使用而非事后支持引用。
计算所有实际父/来源处理许可的交集，限制循环、深度、数量及输出容量；父变更/撤销/删除必须传递失效并物理擦除。
不接受未知父、自己或当前未支持的派生父，验收通过后才允许新的依赖类型。

实施顺序：C01 固定实际父输入 manifest 与版本并保持 support/processing 分离；
C02 在同一 scope/UoW 内验证当前处理许可交集、循环/深度/容量及父变更阻断；
C03 完成父擦除的传递物理清理、恢复回放和最终交付验收，再宣布派生父 capability。

## 责任边界与退出条件

- derived/contracts.py：QueryDefinition/HostGrantAuthority/HistoricalQuery 纯合同。
- derived/registry.py：宿主登记、CAS、控制证明和权限交集；不依赖数据库实现或 renderer。
- derived/model.py：facet/unit、可信上下文与历史认知期限合同、共享擦除计划。
- derived/service.py：授权快照、纯准备、原子发布与最终交付；renderer 不管理 authority。
- derived/history.py：不可变点/上下文证明、冻结历史语义重建与当前安全守卫；contextual.py 只负责纯条件与字段支持合成。
- derived/coverage.py：元数据区间证明、首次变化关闭、版本一致性及可信时钟前沿；复用调用方 UoW 和锁。
- operations/facet_refresh.py：既有有限单元队列、路由、租约和固定回执。
- SQLite/PostgreSQL：复用原 ledger/UoW/锁和索引，当前无需新表或 DDL migration。

旧定义没有 query_id/authority_id 时不增加序列化字段，保持 v1 fingerprint/unit 身份；升级不会自动绑定权限或查询。
SQLite 同步事务的跨连接竞争在独立线程/事件循环中执行，PostgreSQL 使用异步连接；不新增异步 SQLite I/O 实现。
每切片跑新增与受影响测试、真实双后端及相应恢复验证，独立记录结果；不默认全量测试。
第十四阶段全部完成需 A/B/C 各自验收；A 完成不等于 T25–T29/M2 全部完成。
