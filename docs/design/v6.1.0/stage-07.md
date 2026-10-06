# 第七阶段：显式重处理有限目标

设计仍为 v6.1.0，执行计划/台账 revision 11；承接 [stage-06](stage-06.md)。
本轮实现 T02/T16/T20/T22 的一个运行合同切片，补充 T04/T24/T44 的验证与说明。
44 项任务状态仍为 DONE 1、IN_PROGRESS 24、TODO 19；M0/M1 未整体验收。

## 行为与模块边界

已有 L0 来源可以由可信宿主提交新的整来源 primary 重处理请求，使用明确的配置指纹、模式和预期解释代数。
SDK/MCP 现在可以冻结这些已持久化请求，分别查询来源保存、L1 决策和可选本地候选索引可见阶段。
不创建新 L0、接收序号或新的 capture 提交事实；捕获游标保持原值。

- `operations/reprocessing.py` 保有宿主提交、来源所有权/当前修订检查和处理合同指纹。
  `checked_source` 与指纹函数由就绪服务复用，避免两个入口维护不同来源规则。
- `operations/readiness.py` 保有固定目标、阶段投影和 manifest 校验；capture 目标继续使用 `durable-target/1`。
  新重处理目标使用 `durable-target/2`、`target_kind=reprocessing`，绑定 producer、epoch、scope，
  每个成员记录 request_id、source_event_id、configuration_sha256、reprocessing_fingerprint。
- `capture/producer.py` 与 `durable_api.py` 只负责身份绑定、能力发现和操作路由。
  SDK 新增 `durable_freeze_reprocessing_target`；MCP 沿用 `memory_durable` 路由及相同受信上下文。
- `operations/indexing.py` 继续负责实际候选定位条目、完成证明与连续覆盖；不新增调度器或事实授权入口。

重处理配置可以不同于原 ProducerSession 的捕获配置；允许这一差异不等于允许模型提交工作。
公开 durable 入口仍拒绝 `reprocess` 操作，重处理的模式、配置和 CAS 必须先由宿主服务提交。

## 固定目标与阶段语义

一次冻结接受 1–128 个不重复的已有请求 ID，排序后得到确定 target_id。
同一成员集合可幂等冻结；不同目标共享每 scope 1000 项的已有容量限制；写入失败事务回滚。
每次查询都验证目标内容 hash、请求绑定、合同指纹、来源 owner、完整覆盖和当前 SourceRevision。
未知或外部所有者请求不能通过目标查询获得处理信息。

请求持久化时新增类型为 `processing` 的提交 token。新目标返回 `processing_commit_tokens`，
不返回 `capture_commit_tokens`；内部仍保留旧 capture 字段，以兼容既有 manifest 存储校验。
`source_persisted` 表示既有来源仍可用且目标请求已持久化；不表示再次捕获。
`l1_decided` 依赖目标请求自己的闭合 manifest；无输出、全部待决、冲突、处理失败分别报告。
来源删除/修订或 epoch 失效仍优先阻断，不能凭历史 token 绕过当前屏障。

后续同源重处理不替换已冻结的成员、配置、处理 token 或发布 token。
历史请求仍可达到自己的决策阶段，同时 `interpretation_current=false` 明确它已不是当前解释。
有界等待到期只结束客户端等待，不取消 worker，也不创建新请求。

## 索引积压与证明损坏

同源新解释会改变候选版本，已有索引定位条目在新 outbox 尚未消费时可能暂时落后。
当完成证明合法、所有落后候选都有固定目标边界内 pending/running/retry_wait 的索引工作时，
就绪状态为 `processing`；实际条目更新且连续前缀成立后才能报告 `reached`。

缺失或损坏的完成证明仍为 `blocked`；后续任务提到同一候选不能代替该证明。
固定边界之外的新发布不加入目标的修复等待集合。旧目标遇到这样的当前条目落后时仍阻断，
待实际索引更新后再按原目标边界判断；不会自动跟随“最新解释”扩张目标。
取消/终止的索引前缀仍不能跳过，显式修复与 stream rollover 尚未实现。

## 升级与运行

新状态放在已有 request metadata 与 delivery target 中，无新增 SQL DDL 或 migration。
部署应配套升级 core 和 Python SDK；PostgreSQL 存储包沿用第六阶段的 migration 011。
旧 capture 目标 schema、哈希编码和默认索引关闭行为保持兼容。

升级前已持久化且缺少 processing token 的重处理请求，冻结时返回 `readiness_history_unavailable`。
不从现有结果猜测或批量补写历史提交证明；需要新就绪合同的宿主应显式提交新的合法处理请求。
这不禁止旧 worker 按其原合同继续处理。新成员仍要求已有 manifest 与真实 token。

可运行示例：`examples/reprocessing_readiness.py`。先捕获并索引来源，再由宿主提交新配置解释，
通过 SDK 固定目标，分别观察等待超时、L1 决策、索引积压和实际可见；捕获游标始终为 1。

```python
target = await client.durable_freeze_reprocessing_target(session, [receipt.request_id])
status = await client.durable_wait_until(
    session, target["target_id"], stage="l1_decided", timeout=1
)
visible = await client.durable_readiness(
    session, target["target_id"], stage="index_visible"
)
```

## 本轮验证与后续

按用户要求仅执行新增专项和受影响回归：**182 passed，0 skipped**。
新增重处理目标行为用例 58 项，SQLite/真实 PostgreSQL 17 同合同；覆盖 Embedded/MCP、可选索引、
新配置、多请求、所有权、合同/目标损坏、历史缺失、并发冻结、事务回滚、容量、删除/修订与有界等待。
重跑受影响 capture/readiness/index/reprocessing/source revision、SDK/MCP 及三项依赖方向检查。
现有索引进程终止用例作为回归执行，不计为本轮新增故障用例。

示例、Ruff、core/SDK wheel 源码匹配和文档/冻结设计校验见 [验证记录](validation-stage-07.json)。
未运行全量测试；不据此声明完整运行恢复或真实模型抽取质量已验收。

下一切片优先完成通用资源刷新合并与运行中新工作；单请求多发布、索引终止缺口修复/rollover、
权威删除日志备份回放和真实领域质量启用门仍按 [next-steps.md](next-steps.md) 推进。
