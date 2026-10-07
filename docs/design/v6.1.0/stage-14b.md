# 第十四阶段 B.1：已发布检查点的双时态 Observation

版本 AM61-ST14-B.1 / 1.0；2026-10-07；DONE（有界切片）；台账 revision 20。
设计依据保持冻结 v6.1.0；基线 `2cb155b056bb52c9d3d3e688b002c20eff9cefe8`。
开发期间独立文档提交 `c8c62d245f3bcf82bbe1db52ae011c2ae8bc5f02` 已进入 main；本轮不修改其 README/图示，也保留原架构文档的未提交改动。
方案见 [stage-14-plan.md](stage-14-plan.md)，任务见 [stage-14-tasks.md](stage-14-tasks.md)，验收证据见 [validation-stage-14b.json](validation-stage-14b.json)。

## 交付行为

宿主显式设置服务与 FacetDefinition 的 `history_mode="published-point/1"`，并绑定 query/authority。
每次完整发布把当时的 query、定义、政策及候选全集存入不可变修订；发布事务同写覆盖点、head 和完成证书。
认知时间由事务内宿主时钟取得，不信任准备快照中可修改的时间或定义代次/槽元数据。
同一个认知时间下，可以用不同 `valid_at` 重建有效事实；后来收到较早生效的来源不会改写旧认知。
普通 L1 版本更新、解释撤回及政策迁移不会让旧检查点使用新解释；物理擦除仍优先。

该能力只证明已发布的精确 `known_at` 点。查询缺口返回 `derived_history_coverage_unavailable`，
未来时间返回 `derived_history_future`；不会把旧系统的缺失版本伪装成历史，不会回退到当前摘要。
目前只支持非条件 `locale-snapshot/1`；历史资格上下文、连续区间、派生父及任意谓词 renderer 尚未开放。

## 架构与安全

- `derived/contracts.py` 定义固定双时间请求；`derived/history.py` 负责归档、检查点证明和历史读取。
- `derived/service.py` 在既有 scope UoW/租约/head CAS 内原子发布；纯 renderer 不负责存储或授权。
- 两个后端仅复用 ledger 的 `history_point` kind 和共享擦除计划，无新增表或迁移前历史回填。
- 读取先检查当前 authority/floor、当前及过去 audience/purpose、完整来源 grant，再加载历史 L1 正文。
- 来源续授权只影响当前安全，不能扩展旧输出读者；source/atom 物理存在检查仍执行。
- 擦除清除相关 facet 的全部历史归档、manifest 和检查点正文，包括此前的空覆盖；真实备份回放使用同一计划。
- 每 facet 128 个历史点、每 scope 4096 个、每归档 256 KiB；达到上限整笔回滚。规模分页与回收另列任务。

## 接入与验证

SDK 新增 `derived_history_points("language")`，返回实际可读取的 `known_at`；
`derived_read` 与 `derived_context` 同时传入 `known_at`、`valid_at`。
`derived_context` 最终两次读取保留原请求时间，撤权发生在两次之间会拒绝交付。
宿主写接口保持受信使用，模型不能通过 SDK/MCP 开启历史、注册合同、提供上下文或写覆盖证明。
`capabilities` 明示有限历史 mode/template，原未启用服务仍拒绝历史请求并保留 v1 身份。

运行示例：`python examples/derived_history.py`（已安装 core 与 Python SDK）。
测试仅覆盖新增与受影响模块，包含真实 SQLite/PostgreSQL、进程 SIGKILL、跨连接删除竞争、备份/pg_dump 回放及 SDK/MCP；
592 个唯一相关用例通过，其中新增历史用例 75 个（两后端各 36、纯合同 3），另新增架构边界用例 1 个。
4 个历史 SIGKILL 用例已验证，安装 wheel 后另复核 71 个历史用例与三个示例；重复执行不加入唯一总数。
core/PostgreSQL/SDK 的 sdist/wheel 均构建成功，180 个源码/类型标记文件逐字节匹配测试版本。
具体构建指纹与安装验证见机器可读证据。合成协议用例不代表生产抽取质量。

## 后续顺序

1. B.2：候选/解释/控制版本区间、完整成员变化与连续空 coverage；补齐 arbitrary known_at 的证明。
2. B.3：历史 QueryContext、偏序与资格证据，验证历史政策和当前安全的组合矩阵。
3. 完整 B 验收后推进 C：有界传递 processing 图与父版本/许可/擦除传播。
4. 再实现 L2 Scenario/版本化页面 full rebuild，随后按证据决定 delta 与 L3。

全局 T28、完整第十四阶段和 M2 保持 IN_PROGRESS。
