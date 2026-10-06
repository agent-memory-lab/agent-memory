# 第八阶段：资源刷新合并与运行中新工作

编制：2026-10-07；设计保持 v6.1.0，计划/任务台账 revision 12。承接 [stage-07](stage-07.md)。
本轮交付 T16/T18/T22 的受控本地刷新切片，补充 T02/T04/T23/T24/T44 的合同、恢复与说明。
44 项任务仍为 DONE 1、IN_PROGRESS 24、TODO 19；M0/M1 尚未整体验收。

## 交付与责任边界

`ResourceRefreshQueue` 提供宿主绑定、同 scope 的维护调度：多次提交可以合并到同一资源，
每次领取固定自己的处理单元，运行中新工作留给后继领取。仍使用 `BoundedWorker` 执行，
不创建另一个后台循环或模型派发入口。

| 位置 | 责任 |
| --- | --- |
| `operations/resource_refresh.py` | 提交、资源状态机、有限回执、租约代次、事务提交、重试/暂缓/取消与来源守卫 |
| `operations/worker_tasks.py`、`worker_runtime.py` | 共用执行合同；可选 DEFERRED 结果单独计数，旧队列返回 None 的行为不变 |
| `ports.py` | ResourceRefreshUnitOfWork 的存取端口；不依赖存储实现 |
| `operations/sqlite_refresh.py`、PostgreSQL `refresh.py` | 在调用者事务内存取刷新资源/请求；删除时清理依赖、证明、检查点与租约 |
| 宿主 handler | 准备输出、登记真实输入、执行目标对象 CAS、事实资格与输出权限检查；在 commit 回调内持久发布 |

本切片适用于已有持久来源和受控本地 handler。示例通过现有 Block 存储刷新来源清单，
验证真正输出与进度共用一个事务；它不代表 Observation、L2/L3 或模型生成的启用验收。
SDK/MCP 的公开入口和 durable-target/1、/2 不改变；刷新提交与状态查询属于可信宿主 API。

## 三种身份与有限范围

- dedupe_key 标识一次提交，同键不同资源/定义/单元返回冲突；重复提交不扩张回执。
- serialization_key 在 scope + epoch 内绑定资源，定义指纹固定；定义变化显式拒绝。
- generation 标识一次领取，过期重领会增加代次并更换随机 fencing token。

单元使用不可变的宿主工作身份，绑定其全部声明的 retained source IDs。
`requested_through`、`claimed_through`、`completed_through` 在此使用明确的单元集合，
符合设计 §14.7 的集合模式，不把墙钟或最大已见序号冒充连续覆盖。
同一单元的来源集合不可改写；同来源的新处理责任应使用新工作单元身份。
宿主不得省略实际生成输入，队列的来源存活检查不能替代事实支持或动态权限判断。

提交保存 resource-refresh-request/1 回执与合同指纹，status 返回 resource-refresh-readiness/1。
每个请求只按原始单元集合判断 completed/remaining；资源后续新增工作不会延长旧请求目标。
共享事务提交 token 可以覆盖多个请求单元，但回执完成范围仍固定。

## 领取、提交与恢复

同资源有效租约排他，其他资源仍可领取；数据库事务复用既有 scope 锁，网络工作不在锁内执行。
领取后 requested_through 可以增加，claimed_through 不变。失败或过期重试继续原领取集合，
不把新工作加入旧执行；上一集合实际提交后，原子检查剩余范围并切回 pending 或 completed。

`commit(task, write)` 在同一 UoW 中复查 epoch、scope、代次、租约、固定单元/定义和全部来源当前修订，
执行宿主持久写入，核对返回的实际单元集合，最后一起登记完成 token 和进度。
宿主目标 CAS 失败、输出失败、进度写入失败或提交前租约失效均回滚输出与覆盖。
handler 仅返回或保存 checkpoint 不算完成；必须真实调用 commit。重复提交已完成领取不重复写输出。
已有提交的丢确认不改写正在运行的后继任务；旧未提交租约不能发布、保存检查点或续租。

合法 quota/backpressure 暂缓保留 claimed 集合和 checkpoint，不消耗一般故障次数；
BoundedWorker 的 deferred 与 failed 单独报告。无效暂缓按处理失败占用重试预算。
心跳只续租，checkpoint 只保存准备状态，均不推进 last_committed_progress_at。
总处理周期年龄与无提交进度时限独立于租约；新增工作、心跳和暂缓不能无限延长活跃周期。
重试有次数与有界指数退避，终止资源不会因新提交自动复活。

显式 cancel 停止新工作，保留合法已提交范围；旧已完成请求仍 reached，混合请求报告剩余范围并 blocked。
来源擦除/归档和 scope 删除则在同一个删除事务清除相关资源的单元依赖、完成证明、checkpoint 与 fencing。
已删除来源不能凭旧回执再次使用；来源修订同样阻断旧处理。任一依赖失效保守地阻断整个资源，
不声称已实现精细的派生增量重建。目标对象单独删除需宿主同步停止对应资源，尚无通用自动映射。

## 有界存储与升级

每 scope 至多 1000 个资源历史记录、4096 个提交回执，默认 128 个活跃资源（暂缓同样占容量）。
每资源至多 128 个工作单元、每单元 16 个来源、总计 256 个不同来源；单元 JSON 上限 128000 字节，
checkpoint 上限 8192 字节。完成 token 按批保存一次，单元通过 token ID 引用，避免重复存整批证明。
同资源合并在活跃容量已满时仍可接受；需要新资源时拒绝，不能返回已有任务而丢弃新责任。
历史达到容量上限时显式拒绝，历史压缩/归档、跨 scope 总配额与规模优化仍待实现。

SQLite 初始化增加 resource_refresh 表；PostgreSQL 增量 migration 012 增加 agent_memory_resource_refresh。
部署配套升级 core/PostgreSQL 包，并执行既有 initialize 流程。升级保留旧来源和任务，重复初始化幂等。
暂停新刷新 worker 即停止该能力；不要在带活跃刷新任务时回退到不含该删除清理合同的旧版本。
旧 capture、reprocess、索引和普通 worker 不迁移成刷新任务，未注册 handler 不自动开展维护。

运行示例：`examples/resource_refresh.py`。在第一个 handler 运行时提交第二个单元；
第一次提交后旧请求 reached、新请求 processing；第二次提交后 Block version=2，两份请求分别完成。

## 验证与剩余

按用户要求，仅执行新刷新专项、受影响 worker/索引/重处理/来源/删除、迁移、SDK/MCP 和依赖方向回归。
最终相关回归 **267 passed，0 skipped**；新增 64 项行为用例与 1 项架构检查，重复运行不另计数。
core/PostgreSQL wheel 匹配最终源码，migration 012 已入包。源码/构建指纹见 [验证记录](validation-stage-08.json)，不声明全量测试通过。
新增真实 SIGKILL 用例覆盖刷新输出/进度提交前后，SQLite 与 PostgreSQL 17 均执行；不包含数据库断电。
跨连接、重复初始化、CAS、事务回滚、丢确认、来源删除/修订、旧租约、容量、暂缓和独立时限均有专项。

下一步优先补索引终止前缀缺口的显式修复/stream rollover，再补权威删除日志备份回放。
单个 L1 request 多次发布、定义迁移、外部/派生索引和真实领域启用门仍依各自验收推进，
刷新队列本身不等于完成派生知识或外部模型治理。
