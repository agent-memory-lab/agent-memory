# 第十四阶段方案：查询控制、权限与历史覆盖

版本 AM61-ST14 / 1.0；2026-10-07；IN_PROGRESS；执行台账 revision 19。
依据冻结 v6.1.0，推进 T25/T26/T28/T29 及协议、恢复与运行文档的必要切片。
任务见 [stage-14-tasks.md](stage-14-tasks.md)，A 切片结果见 [stage-14a.md](stage-14a.md)。

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

## B：历史 coverage（下一切片，未实现）

先固定历史请求的 known_at/valid_at、query/definition/policy/context 版本，不允许读取时偷偷使用最新政策重解释过去。
保存可验证的历史候选全集、来源/解释版本和有效覆盖区间；区分当时采用的解释与当前授权许可。
历史输出必须同时通过当前 authority、来源权限、当前擦除及生成依赖安全守卫。
当前 registry 只保存最新控制版本，A 的 hash/代次证明不能冒充政策历史或历史重建证明。
历史修订不足、空覆盖不能证明或权限不可验证时拒绝；测试前不改 historical=false。

## C：传递 processing 图（B 后，未实现）

显式绑定每个实际派生父版本及输入 manifest，验证当次实际使用而非事后支持引用。
计算所有实际父/来源处理许可的交集，限制循环、深度、数量及输出容量；父变更/撤销/删除必须传递失效并物理擦除。
不接受未知父、自己或当前未支持的派生父，验收通过后才允许新的依赖类型。

## 责任边界与退出条件

- derived/contracts.py：QueryDefinition/HostGrantAuthority 纯合同。
- derived/registry.py：宿主登记、CAS、控制证明和权限交集；不依赖数据库实现或 renderer。
- derived/model.py：facet/unit 合同与共享擦除计划。
- derived/service.py：授权快照、纯准备、原子发布与最终交付；renderer 不管理 authority。
- operations/facet_refresh.py：既有有限单元队列、路由、租约和固定回执。
- SQLite/PostgreSQL：复用原 ledger/UoW/锁和索引，当前无需新表或 DDL migration。

旧定义没有 query_id/authority_id 时不增加序列化字段，保持 v1 fingerprint/unit 身份；升级不会自动绑定权限或查询。
SQLite 同步事务的跨连接竞争在独立线程/事件循环中执行，PostgreSQL 使用异步连接；不新增异步 SQLite I/O 实现。
每切片跑新增与受影响测试、真实双后端及相应恢复验证，独立记录结果；不默认全量测试。
第十四阶段全部完成需 A/B/C 各自验收；A 完成不等于 T25–T29/M2 全部完成。
