# 第九阶段：索引显式修复与流切换

编制：2026-10-07；设计保持 v6.1.0，计划/任务台账 revision 13。承接 [stage-08](stage-08.md)。
本轮交付 T16/T18/T22 的受控本地候选索引恢复切片，补充 T02/T04/T23/T24/T44 的合同与验证。
44 项任务仍为 DONE 1、IN_PROGRESS 24、TODO 19；M0/M1 尚未整体验收。

## 交付与架构责任

终止任务或损坏证明造成连续索引前缀缺口时，可信宿主可实际修复对应投影，
或从当前有效的权威发布清单建立新流。两者均保留来源、处理配置、发布 token 和 capture epoch。

| 位置 | 责任 |
| --- | --- |
| `operations/index_recovery.py` | 流身份、宿主恢复授权边界、权威清单/来源复查、修复 CAS、重建与原子激活、有限审计 |
| `operations/indexing.py` | 普通发布/worker 使用当前流；固定目标按原绑定流验证真实证明和连续覆盖 |
| `operations/readiness.py`、`capture/producer.py` | 冻结新目标的流身份、阶段查询与合同发现；捕获/重处理身份不变 |
| `ports.py` | 恢复存取、真实 SQL 序号与权威处理请求枚举端口 |
| SQLite `sqlite_index.py`/`sqlite_retention.py`、PostgreSQL `index.py`/`retention.py` | 在调用者事务内存取流记录、审计与权威清单；沿用既有 scope 锁与删除清理 |

恢复入口是绑定 exact scope、逻辑通道与 actor 的 `CandidateIndexRecovery`。
SDK/MCP 不增加模型可调用的 repair/rollover 工具；宿主按审计理由显式执行。
索引仍为 opt-in candidate-locator/1，保存身份定位；没有新增全文、向量、外部索引或事实判断。

## 三种身份与旧目标

逻辑通道身份及抽取配置指纹保持稳定；物理索引流由 scope、逻辑通道、capture epoch、generation 确定。
流 0 使用原存储命名空间。generation 大于 0 使用独立物理命名空间，不能与旧流的序号混用。
candidate-index-stream/1 描述符绑定上述身份；序号仅在单个物理流内表示连续覆盖。

旧 durable-target/1、/2 没有 index_stream 字段时固定代表流 0，目标 hash 不改变。
切流后的新冻结目标带新流描述符，并生成自己的 target ID；成员、配置与 processing/publication token 仍固定。
旧目标不自动改绑，也不因切流取得新的完成证明。未完成的退役流目标报告 blocked/index_stream_retired；
合法已完成目标可保持 reached，同时 index_stream_current=false。当前删除与来源修订守卫继续生效。
需要追踪新流的客户端须显式重新冻结目标；后续输入仍不会扩大旧目标。

## 实际修复

1. 宿主 inspect 获取当前流、发布身份、状态、序号与完整 job 的 SHA-256，不返回正文或租约 token。
2. repair 绑定 recovery_id、预期 job hash、流和理由；同键同合同返回原审计，不同合同拒绝。
3. 在同一个事务内复查原发布清单、来源存活/修订、token/请求/事件绑定和真实 SQL 序号。
4. 对 dead、cancelled 或证明/定位损坏的 completed 任务，从权威 dispositions 重新计算并实际写入当前定位条目，
   再一起保存完成证明、任务状态和 candidate-index-repair/1 审计。保留原 token、序号和尝试次数，清除旧租约。

pending/running/retry_wait 任务拒绝修复；正常完成任务返回 repair_not_needed。
来源已删除/修订、任务坐标损坏、预期 hash 变化或退役流不能用修复绕过。
任何定位写入、证明或审计失败都回滚；取消位置不会被直接标作已覆盖。
修复不能解决丢失 ledger/损坏序号或已删除来源留下的永久缺口，此时使用新流重建。

## 从权威发布建立新流

rollover 使用 expected_generation CAS。持有 scope 事务锁后，枚举当前 epoch 的权威处理请求，
纳入同逻辑通道、来源仍存在且修订当前、合法闭合清单中的发布 token。
没有依赖旧索引 ledger 的完整性；已删或旧修订来源不进入基线，适用清单损坏则拒绝切换。

按完成时间、请求和 token 身份排序，为基线分配新流序号 1…N。
逐项真实写入当前候选定位、保存完成证明，再写基线摘要、父流关系、actor/理由和审计回执。
同事务把父流标为 retired，最后激活新 head；失败只保留完整旧状态，不出现半个新流。
空基线也经过显式激活合同，不伪造发布 token 或可用事实。

原流账本/证明及原发布清单不改写。新发布进入当前流，在基线后继续排队；
原流租约不能 apply、complete 或 fail 新流任务。重复操作返回首次回执，即使之后又切流，历史回执也不漂移。
删除路径按来源/候选身份清理所有物理命名空间，旧流定位和证明不能复活已删内容。

## 容量、升级与操作

本地同步切片每次至多枚举 10000 个处理请求，基线最多 256 个发布、4096 个不同候选；
每逻辑通道/epoch 最多 1000 份修复记录，generation 上限 32。超限明确拒绝并回滚。
大规模分页、异步重建、历史压缩、流定义/外部索引迁移仍待专属实施。
head/stream 元数据损坏时阻断操作与相关读取，需要恢复可信元数据；没有自动猜测或回退 head。

SQLite 初始化增加 index_recovery 表；PostgreSQL 增量 migration 013 增加 agent_memory_index_recovery。
配套升级 core/PostgreSQL 包并执行 initialize，已有任务与来源保留，重复初始化幂等。
自定义存储启用宿主恢复须实现新增端口；未实现则返回 index_recovery_unsupported。
尚未切流时流 0 保留旧路径；激活新流后不要直接降级旧 core：旧代码忽略新 head，可能把新发布写入旧流。
停止新写入后保留当前版本和审计，不能靠启动旧二进制宣称完成回滚。

运行 `examples/index_recovery.py`：修复失败任务并实际达到旧目标；擦除另一前缀来源后建立流 1，
新目标可见、旧未完成目标退役阻断，capture epoch 保持不变。

## 验证与后续

仅执行新增专项与受影响的索引、重处理、就绪、来源、删除、worker、迁移、SDK/MCP 和依赖方向回归。
最终 **301 passed，0 skipped**，包含 76 项新增行为用例与 1 项新增架构检查，未运行全量测试。
SQLite 与真实 PostgreSQL 17 均执行；新增 8 项真实 SIGKILL 用例覆盖两个操作提交前后，
另回归 4 项既有索引强杀用例，不代表数据库服务器断电验证。
跨连接/重复初始化、旧租约、CAS/并发、丢确认、事务回滚、损坏/容量与删除两代清理均有专项。
core/PostgreSQL wheel 与最终源码逐字节匹配，migration 013 已入包；指纹见 [验证记录](validation-stage-09.json)。

下一步优先 T23/T24：恢复旧备份后，在开放读写前回放权威最新删除日志，验证日志缺失/回退、
旧离线设备和多连接竞争。单个 L1 request 多次发布、资源定义迁移、具体派生依赖/权限、
真实领域 gold 与外部模型治理仍按各自验收推进，本轮不声明这些能力已启用。
