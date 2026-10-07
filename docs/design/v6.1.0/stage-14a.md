# 第十四阶段 A：版本化当前查询与本地宿主权限

版本 AM61-ST14-A / 1.0；2026-10-07；IMPLEMENTED；台账 revision 19。
方案：[stage-14-plan.md](stage-14-plan.md)；任务：[stage-14-tasks.md](stage-14-tasks.md)；证据：[validation-stage-14a.json](validation-stage-14a.json)。

## 可使用的闭环

宿主登记完整 query 与 expiring authority，再将语言 facet 和来源 grant 绑定到它们。
查询包含同 exact scope、同主体、指定槽的全部候选；待核验与反例不被权限/接受状态过滤。
scope 内最多 128 query/authority，完整快照最多 64 候选、候选与来源合计 128。
query 改版、来源新成员、上下文变更、权限更新、时间到期与删除都能阻断旧输出或旧领取单元。

authority 任意更新都要求重新确认来源 grant，读者与用途必须属于 authority 许可交集。
空结果也需当前权限；权限不可验证时返回稳定拒绝，不输出原文或缓存正文。
旧宿主不能把已绑定定义/来源授权降级成未绑定状态，也不能领取其他 authority/context 的任务。
SDK/MCP 继续仅提供 capabilities/read/status/derived_context，工具不能登记 query 或 authority。

## 宿主接入

```python
service = ObservationService(
    repository, scope, admission_policy,
    authority_id="host-a",
    authority_min_version=0,  # 仅限全新 authority 创建；已有库必须给外置当前 pin。
)
row = await service.set_authority(
    HostGrantAuthority("host-a", ("alice",), expires_at),
)
# 宿主先把 row["version"] 保存到独立于数据库备份的可信位置，再采用新 ACL。
await service.register_query(QueryDefinition("language-inputs", scope, "alice", ("locale",)))
await service.register(FacetDefinition(
    "language", "alice", query_id="language-inputs", authority_id="host-a",
))
await service.grant(ProcessingGrant(source_id, ("alice",)))
queue = FacetRefreshQueue(service)
target = await queue.request("language", dedupe_key="fixed-request")
```

运行完整 SQLite 示例：`python examples/derived_controls.py`（需要 core + Python SDK）。
随后通过原 `BoundedWorker` 执行 memory.facet_refresh，通过 SDK/MCP 只读接口交付。
已有服务重新启动时使用宿主独立保存的当前 `authority_min_version`，不能从待恢复备份读取这个值。
floor=0 不能读取或修改已存在的 authority；低于 floor 的控制行在正文读取前以 derived_authority_rollback 拒绝。
`set_authority` 成功提交后才推进服务 floor；事务失败不会推进。authority 恢复须由宿主受信流程先同步当前控制状态；本切片不提供下调 pin 的修复入口。

更新 query 使用 `expected_generation`，更新 authority/来源 grant 使用 `expected_version`。
更新控制与 dirty outbox 原子提交；绑定值和当次代次/hash 写入 facet-refresh-unit/2、manifest 和固定回执。
同名已完成 target 仍证明其原始有限单元执行，不代表最新 freshness；重新处理使用新的 dedupe_key 或 force 请求。
authority/status 的当前权限检查依然适用，撤销后不能继续查看该受保护回执。
未绑定 query/authority 的 v1 定义与单元不增加序列化字段，保持已有身份；升级需要宿主显式操作。

## 架构与恢复

contracts.py 管纯控制合同，registry.py 管控制登记/证明，service.py 管快照与交付，queue 管固定单元。
composer 继续只负责纯合成，数据库实现继续提供原 UoW/锁/ledger；没有新增调度服务或表迁移。
发布阶段在读取 L0/L1 前重新授权实际候选全集，调用者遗漏 manifest 输入不能绕过权限。

scope 擦除将 query 与 authority 清成单调墓碑；来源擦除保留授权路由墓碑，清除派生正文、manifest 和依赖边。
SQLite backup 与真实 pg_dump 副本的删除日志回放使用同一擦除计划。
旧权限快照额外受宿主外置 authority floor 守卫；本地快照自身不能证明最新权限。
SQLite 同步 BEGIN 的跨连接竞争在独立线程中验证；PostgreSQL 用独立异步连接。未新增异步 SQLite I/O。

## 实际验证

693 项唯一专项及受影响用例通过，0 失败、0 错误、0 跳过；没有执行全量测试。
本轮新增 74 项：SQLite 32、真实 PostgreSQL 17 的 32、纯合同/架构 10。
其中 8 项真实 SIGKILL 覆盖 query/authority 控制事务提交前后，验证控制替换与 outbox 原子性。
另外包含跨连接 CAS/发布竞争、独立 floor/旧备份、grant 重新授权、空覆盖期限、反例、新成员、上下文组合及最终 SDK/MCP 交付。
两包共四个 sdist/wheel 构建通过，170 个 wheel 源码文件与工作区逐字节匹配；安装包专项重复 66 项通过，不累计进唯一通过数。
安装重复排除 8 个 SIGKILL 子进程用例，真实进程验证已在源码专项执行；SDK/MCP 仍使用已安装的原有版本。
示例在源码与安装包上运行通过。原始报告哈希、范围、源码指纹和构建哈希见 JSON 证据。

本次代码审查补上旧备份 authority 回退守卫；测试修复了 manifest 反例夹具、SQLite 同步写入竞争的线程安排和新控制 kind 的擦除枚举。
初次无隔离构建缺少 hatchling，改用声明依赖的标准隔离构建后成功。
这些确定性合成夹具证明协议和权限边界，不替代真实领域 gold、生产抽取质量或远端 ACL 实验。

## 下一步

第十四阶段 B 固定 known_at/valid_at 与定义/政策/上下文历史，验证完整历史 coverage，并在历史读端继续检查当前权限与擦除。
A 的当前 hash/generation 不构成历史政策重建证明；historical=false、derived_parents=false 保持有效。
B 完成后进入 C 的有界传递 processing 图；随后推进 L2 Scenario/版本化页面 full rebuild。
整个第十四阶段与全局 T25–T29/M2 尚未整体验收。
