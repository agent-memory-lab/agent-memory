# 第十四阶段 C：有界传递处理依赖

版本 AM61-ST14-C / 1.0；2026-10-07；设计 v6.1.0；执行台账 revision 23。
方案及任务：[stage-14-plan.md](stage-14-plan.md)、[stage-14-tasks.md](stage-14-tasks.md)。
独立验证：[validation-stage-14c.json](validation-stage-14c.json)。

## 交付范围

新增宿主显式登记的 `locale-parents/1`，组合同一 exact scope、主体、用途和 authority 的当前语言视图。
`parent_facets` 固定配置的完整实际输入集合；新工作单元 `facet-refresh-unit/3` 绑定每个父 facet 的
具体 audit revision、head 摘要及定义摘要。空父视图也绑定其具体空修订，不能仅绑定一个可漂移的名称。
manifest/2 保存直接父绑定和完整传递 header census；生成与发布按存储实际版本核验，不能相信调用者删减后的清单。

renderer 返回 `derived_view` 块，保留父块、争议、有效时间和来源，不从摘要反推新事实。
所有实际父都有 processing 边；只有进入输出的非空父有 support 边。
多条视图引用同一事实不会增加该事实的独立证据数，父版本刷新后即使值相同，也不能悄悄换绑旧子任务。
这一能力是后续 L2/L3 的依赖基础，不等于场景页面或长期画像已经实现。

## 权限与生命周期

每次发布同时生成不可变 `revision_header` 元数据与 head 摘要：含实际 manifest、版本、时间、body 摘要和字节数，不含正文或历史归档。
读取/生成先遍历完整 DAG，核验当前定义读者、用途、authority/floor、所有实际来源 grant、代次及期限，
全部通过之后才读取任何父修订、L0 或 L1 正文。未引用的来源、反例以及空父输入仍参与交集。
子受众只能是全部父/传递来源允许受众的子集；敏感级别取最高、保留类别取最短，期限继承所有父边界。

候选/解释、文档、定义、query、authority、来源 grant 和父发布在原写入 UoW 中保留 dirty 刷新责任。
读取动态复核整个父链，即使子任务尚未重建也立即停止交付。
队列在父失效期间等待，按有效父版本调度子任务；沿用既有有限回执、租约、head CAS 和完成证书。
原已完成回执只证明原固定单元执行过，不证明今天输出仍有效。有效期/授权到期由读取与队列边界检查。

共享擦除计划计算依赖闭包，并保守清理受影响 facet 的所有版本、正文、manifest、header、历史点/区间和反向边。
闭包同时覆盖固定旧 revision 引用及当前配置订阅，包括空或尚未构建的子视图。
SQLite backup / PostgreSQL pg_dump 的权威删除日志回放使用相同计划，不能从旧备份复活派生正文或处理证明。

## 模块与兼容

- `derived/model.py`：显式父合同、可选 v3 单元、边校验及传递擦除计划；旧无父定义与 v1/v2 单元序列化不增加字段。
- `derived/parents.py`：有界 DAG、元数据授权、固定实际版本核验、纯组合和后继 dirty 传播。
- `derived/service.py`：原授权快照、确定性准备、原子发布及最终读取；不引入第二个服务。
- `derived/registry.py`：控制 CAS 与原事务失效；`operations/facet_refresh.py`：父就绪调度、固定目标与租约。
- 两种后端复用 ledger、scope 锁及原反向索引；增加 `processing-graph/1` 能力声明，无 DDL 或强制依赖。

每个 facet 至多 4 个直接父；路径至多 4 个节点（含当前子）；完整图至多 32 个节点（含当前子）。
父输入总容量 256 KiB，在元数据阶段先按 header/body 字节数拒绝，读取后再次核验实际字节。
输出至多 32 KiB；沿用每 facet 128 / scope 4096 修订及现有 ledger 上限，容量不足整体回滚。

启用前升级同一存储的所有写入、删除及恢复进程。后端没有 `derived_parent_contract=processing-graph/1` 时不声明父能力，也不写新 header。
旧发布没有当次 header 证明时要求宿主显式完整重发布；不得读取旧正文后事后编造输入证明。
回退须先停用父 consumer，再由宿主 CAS 登记旧二进制支持的定义；旧二进制不具备新 header 的擦除合同，不能混跑。

SDK/MCP 沿用只读入口，最终 `derived_context` 的第二次读取复核完整处理链。
capabilities 分别声明父 template、合同、容量及 `derived_parent_history=false`。
历史服务不宣称当前父组合能力；条件父、历史父、跨存储范围、任意谓词、模型生成/缓存输入及级别转换继续拒绝或未启用。

## 验证与后续

1082 个唯一新增及受影响用例通过，0 failed、0 skipped；新增 101 项（两种后端各 49 项，纯容量合同 3 项）。
新图包含 8 项实际 SIGKILL；新 wheel 安装后另通过 93 项专项与七个示例，重复不计入唯一总数。
core/PG sdist 与 wheel 的生产源码及类型标记共 173 文件逐字节匹配；本轮新增/修改行无 lint 违规，保留既有 13 处 E501。
报告与构建指纹见本阶段独立 JSON；只运行新增及受影响模块，未执行全量测试。
覆盖双后端固定版本、完整/空父、未引用来源授权、伪造清单、循环/深度/容量、父优先刷新、控制变化、
独立连接发布/撤权竞争、真实 SIGKILL、实际旧备份删除回放和 SDK 最终交付。
SQLite 同步写锁竞争在独立线程/事件循环运行，PostgreSQL 使用独立异步连接；不把事件循环自阻塞当作存储竞争结果。
合成协议夹具不证明真实抽取质量、远端 ACL 或 L3 推断质量。

C01–C03 的本阶段有界当前语言图完成；一般 T25/T28、P3/M2 保持部分实现。
下一步：[第十五阶段方案](stage-15-plan.md) 与 [任务](stage-15-tasks.md)，先实现版本化 L2 页面完整重建。
