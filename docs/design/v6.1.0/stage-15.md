# 第十五阶段：版本化 L2 页面完整重建

版本 AM61-ST15 / 1.1；2026-10-07；设计 v6.1.0；执行台账 revision 24。
状态：ST15-01–07 全部完成，限定本阶段声明的当前语言页面。
方案：[stage-15-plan.md](stage-15-plan.md)；任务：[stage-15-tasks.md](stage-15-tasks.md)。
独立验证：[validation-stage-15.json](validation-stage-15.json)。

## 页面合同与架构

宿主登记独立 `ScenarioDefinition` 和 `PageDefinition`，包含场景版本/状态/标题、主体、用途、
读者、模板版本及 1–4 个当前语言 Observation 父。Scenario 元数据用于组织页面，不是抽取的新事实。
`language-scenario/1` 按父生成语言块，完整保留争议、有效时间、来源和支持；不增加独立证据数量。
本阶段仅支持同一 exact scope、兼容主体/用途/authority、当前非条件父，不支持页面作父。

稳定 block ID 绑定 scope、page ID 和父身份；块修订 ID 绑定稳定 ID、内容摘要和整轮输入清单摘要。
即使一个块内容没变，只要另一个处理输入改变，该 full rebuild 的块版本也继承新完整输入。
页面版本单独保存块引用与场景结构；`page_block` ledger 保存不可变块修订及完整 manifest。
这些都是可重建派生投影，L1 和来源仍是事实依据，未新增平行权威对象。

模块责任：`derived/page_model.py` 保存类型合同；`derived/pages.py` 负责页面生成、块存储和交付；
`derived/service.py` 复用原授权快照、CAS 和原子发布；`parents.py` 与共享擦除计划处理传递输入与失效。
`FacetRefreshQueue` 继续承担刷新和有限目标，没有增加调度系统、服务或数据库 DDL。
页面与 Observation 在内部共享资源 ID 空间，禁止把既有资源悄悄切换类型。

## 全量重建与就绪

固定 `facet-refresh-unit/3` 绑定每个实际父的具体 revision/head/定义摘要；manifest/2 绑定完整传递 header census。
空父仍有 processing 依赖；非空输出父另有 support 依赖。旧页面只作 head CAS 比较，不进入生成上下文。
发布重新核验存储实际输入并确定性重算输出；页面修订、全部块修订、head/header 和完成证书在一个 UoW 提交。
容量或写入失败全部回滚；过期租约、父版本变更和伪造输入不能发布。

SDK/MCP 的 `page_status(target_id)` 区分三种证明：

| 字段 | 含义 |
| --- | --- |
| `complete` | 回执绑定的固定刷新工作单元已取得真实完成证书 |
| `page_complete` | 当前页面经交付守卫通过，并且 head 对应该固定单元；完整空输出也可以完成 |
| `page_ready` | 上述证明成立且页面包含可使用的非空正文 |

旧目标完成不证明当前页面就绪。`page_read` 区分未构建、构建中、重试/失败、空输出、失效和擦除。
来源捕获、L1 决策、父 Observation 与页面的就绪仍通过各自阶段 API 判断，不能互相代替。
普通失败可保留旧不可变版本，但旧 head 不能作为新目标的当前正文交付。

## 权限、删除与运行边界

生成和读取先遍历整条处理链，核验当前 audience/purpose、authority/floor、来源 grant 版本、撤销与期限，
全部通过后才加载任何父、页面块、L0 或 L1 正文。未引用来源仍限制权限。
页面块还核验内容、manifest、版本 ID 与页面所属版本，损坏或缺失时整个页面停止交付。
父写入在原事务中留下 dirty 责任，读端动态复核立即阻断旧页；队列按父优先刷新。

源/页面版本/块/块版本删除保守擦除受影响页面的全部版本、块、manifest/header 和反向依赖。
显式删除 page ID 会停用页面定义并清除场景元数据；宿主如需恢复必须显式 CAS 重新登记。
删除某个投影版本/块不会删除其独立 L1 来源；后续只能从仍许可的当前输入完整重建，不能复用被擦除正文。
scope 擦除同时清除页面定义的场景标题等元数据。旧备份恢复使用同一权威删除日志与擦除计划。

启用前升级同一存储的所有写入、删除和恢复进程；后端须声明 `page-full-rebuild/1`。
旧后端不声明能力并明确拒绝新页面。回退先停用页面 consumer；不能把不理解 page_block 擦除的旧二进制混跑。
每页最多 4 父，路径最多 4 节点、图最多 32 节点，输入 256 KiB、物化输出 32 KiB；
复用每页 128 / scope 4096 页面与 Observation 修订预算，另限制 scope 最多 4096 块修订（含 tombstone）。
容量不足不丢弃证明或静默裁剪，而是整体失败；扩大预算和清理策略须另验收。

## 接口、验证和后续

只读 SDK/MCP：`page_capabilities`、`page_read`、`page_status`、`page_context`。
`page_context` 在最终交付再次完整核验；写入和刷新仍是宿主 API。
示例：[derived_scenario_page.py](../../../examples/derived_scenario_page.py)。

全量测试 2320 个唯一用例通过，0 failed、0 skipped；新增页面行为 115 项（两后端各 57 项、纯合同 1 项），另加 1 项架构约束。
页面新增 4 项真实 SIGKILL；新 wheel 安装后另通过 204 项页面/父图专项与八个示例，重复不计入唯一总数。
core/PG/SDK/MCP 的 sdist 和 wheel 共 189 个生产源码/类型标记逐字节匹配，新增/修改行无 lint 违规；保留既有 21 处 E501。
全量报告及构建指纹见独立验收 JSON。
验证覆盖 SQLite/真实 PostgreSQL、跨连接发布与撤权/擦除、真实 SIGKILL、实际备份回放、
空页面/旧目标/失败、完整输入与多父块版本继承、数据损坏、权限检查先于正文、SDK 与真实 MCP 调用。
安装包另验证生产源码逐字节匹配与页面示例；重复安装测试不计入全量唯一用例总数。
合成协议夹具不证明真实抽取质量、远端 ACL 新鲜度或长期画像推断质量。

本阶段任务全部完成，一般 T30–T33 仍是 IN_PROGRESS；delta、历史/条件父、一般场景模板和 L3 未启用。
后续评估与计划：[stage-16-plan.md](stage-16-plan.md)。GitHub 提交按核心能力、接口示例和验收文档组织。

## 后续审计修复

2026-10-08 的 [前序实现审计](stage-15-audit.md) 修复四类实现边界；
本节以上保留最初第十五阶段的验收成绩，本轮重新验收见 [validation-stage-15-audit.json](validation-stage-15-audit.json)。
