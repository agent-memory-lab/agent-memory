# 第一阶段：来源修订与显式重处理

2026-10-06；执行台账 revision 5；设计仍为 v6.1.0。对应后续实施顺序第一步 T15/T16/T20。
本阶段交付整来源、同 scope、单一 primary 解释流的闭环；完整 T20 的局部覆盖和复杂贡献组合仍未完成。

## 模块与状态所有权

| 模块 | 责任 |
| --- | --- |
| `capture/producer.py` / `durable_api.py` / SDK outbox | append 与 revise 共用会话、序号、连续 ack；校验身份；断线重传 |
| `operations/source_revisions.py` | 不可变来源版本、document head CAS、旧版本支持退出和旧任务失效 |
| `operations/reprocessing.py` | 宿主绑定 owner/stream；显式模式、固定配置、覆盖范围、解释 head 与贡献版本快照 |
| `consolidation/interpretation.py` | 独立复核旧贡献；旧新并集对账；保留身份、撤下支持、待决及原子激活 |
| `operations/extraction_worker.py` | 复用原有队列/阶段保存；事务 B 再检查来源、租约、配置、head 和贡献版本 |
| SQLite / PostgreSQL UoW | 共用事务连接；持久 head；CAS；双时态接纳快照 |

来源修订与解释修订是两个不同操作：前者创建新 L0，后者对同一 L0 创建新 request。
旧原文和过去 known_at 不改写。来源修订使旧支持退出当前解释，不据此声称相反事实。

## 运行合同

- `DurableReceiver.revise` / `DurableProducer.revise` 要求新的来源 ID、base_event_id 和 expected_revision。
  来源、旧支持退出、旧任务 superseded、新任务、document head 与 producer ack 在一个事务中提交。
- SDK `durable_revise` 与 outbox `append_revision` 经过共享 `memory_durable` 入口；回执核对包含操作和修订参数。
- `ReprocessingService` 是宿主 API。`snapshot` 取得 generation；`submit` 必须显式指定 additive 或 replace_interpretation、配置与预期 generation。
- additive 保留此前所有有效/待决贡献；相同候选去重。replace 对旧 accepted/pending/contested 并集单独复核。
  新生成零候选不等于旧贡献失效；明确 unsupported 或目标政策不允许才撤下该来源支持。
- 复核不完整禁止激活。未知资格默认 needs_resolution；显式 allow_pending 才可 activated_with_pending。
- 旧贡献保留原 ID 和来源证据；新解释使用 request 命名空间。保留完整新生成审计与旧贡献审查结果。
- additive 与 replace 共享同一个 head CAS；冲突请求不能自动改基线，需新 request。
- 发布前贡献版本改变、删除、修订或租约失效均阻断旧结果；失败事务整体回滚，成功 checkpoint 可重用。
- status 区分历史 l1_decided 与 interpretation_current；来源过时返回 superseded 或 source_current=false。

可执行示例：仓库 `examples/durable_memory.py`，包含初次写入、同源复核保留身份、SDK 来源修订。

## 迁移与启用

PostgreSQL 增量迁移 `008_retention_heads.sql`；SQLite 初始化创建对应表。
旧来源按原 ticket/request 惰性登记 head，不重写 L0。SDK 自动给旧 outbox 增加 operation/revision_json，保留待发正文及序号。
升级时先停止旧 worker，再升级 provider/core/SDK，初始化迁移后恢复消费；禁止新旧 worker 混跑。
回滚前排空或停用新操作；不能以删除 head 表或回退数据库的方式恢复旧来源或绕过删除代次。

仅启用本地受控适配器、整来源、同 scope、单来源普通 replace 贡献。
部分覆盖、shadow stream、跨槽替代、多来源组合贡献、复杂更正/终止的解释替代明确拒绝。
跨来源独立记录可幸存，但多来源 AND/OR 合成属于下一阶段；未声称全套贡献级撤回已完成。

## 验证

- 全量：**1010 passed，5 skipped**；含 SQLite、真实 PostgreSQL 17、SDK Embedded/MCP。5 项为可选 tiktoken 未安装。
- 新增 37 个参数化用例，覆盖零生成、明确不支持、审查缺项/未知、允许待决、独立来源幸存、竞争 CAS、贡献版本变化、事务回滚、删除屏障、修订游标回滚、丢确认重传及旧 outbox 迁移。
- 本地示例运行通过。四个 wheel 构建通过，PostgreSQL wheel 包含 008 迁移；指纹见 validation-stage-01.json。
- 故障测试为事务异常/租约恢复，不代表已测断电或真实进程强杀。合成适配器测试不代表真实领域抽取质量。
