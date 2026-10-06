# 第五阶段：有限 L1 目标与删除序列取消处置

2026-10-06；执行台账 revision 9；设计保持 v6.1.0。本次交付 T17/T22 的下一切片：
固定捕获请求集合、显式 publication manifest、分阶段状态与有界等待，以及 opt-in 序列取消。
T17/T22 保持 IN_PROGRESS；单请求多次物理发布、索引连续覆盖、派生就绪与完整备份回放仍待实现。
本轮按用户要求只运行相关专项/接口回归，不执行全量测试。

## 架构与职责

| 所有者 | 职责 |
| --- | --- |
| `capture/producer.py` | 宿主配置、会话权限、版本合同发现和操作 facade；旧游标与新处置协议显式区分 |
| `capture/dispositions.py` | received/cancelled 处置账本、已接收与已处置前缀、删除证明、幂等和缺口限制 |
| `operations/readiness.py` | capture/publication 令牌、清单建立/关闭与一致性检查、有限目标绑定及阶段状态 |
| `operations/retention.py` / `extraction_worker.py` | 分别在原有事务 A/B 中建立清单、原子发布后关闭清单；不另起发布队列 |
| SQLite `sqlite_delivery.py` / PostgreSQL `delivery.py` | 复用现有事务/锁的最小身份账本，保存 target 与 sequence 控制记录 |
| SDK `durable_dispositions.py` / `durable_readiness.py` | 本地清理后的取消确认、合同校验、固定目标的有界轮询；不持锁等待网络 |

SQLite 增量建立 `retention_delivery`；PostgreSQL 对应 migration `010_delivery_contracts.sql`。
表中仅保存身份/配置与处置，不保存来源正文。Manifest 随已有 request payload 持久化，与 L1 发布共用原事务。

## 删除序列的处置协议

- 宿主注册必须显式设置 `sync_purges=True, sequence_dispositions=True`；SDK 使用
  `DurableOutbox(..., sync_purges=True, settle_purges=True)`。SDK 在上传前检查 `durable_contracts`，缺匹配协议则拒绝投递。
  旧 producer 默认为原有 `acked_through/received_after_gap`，不能在原 producer ID 上静默切换协议。
- 新模式返回 `producer-disposition/1`：`received_through` 只跨过确实接收的连续前缀；
  `settled_through` 可以跨过 received 或有删除依据的 cancelled；分别报告 received_count/cancelled_count 和尚有缺口的处置序列。
  新模式不投影为旧 ack 结构，不把 cancelled 当作已持久接收。
- `cancel_sequence` 只接受当前认证会话，在同事务内检查删除水位已确认、指定来源在对象删除日志中、
  sequence 在允许缺口内及旧身份一致。没有删除证明不能随意取消输入，旧 scope epoch 不获得该权限。
- 取消写处置账本和 producer 前缀共用一个事务；失败一起回滚。重复取消不增加计数；
  已接收后被擦除的来源仍返回 received，其已接收历史不重分类为 cancelled。取消后的序列不可重新写入。
- SDK 本地增量增加 purged/cancel_confirmed。先清理正文与提交 purge cursor，再提交仅含来源身份和序列的取消指令；
  确认丢失可重复确认，不发送旧正文。每次最多处理 128 项，超限留下已完成进度并要求继续调用。
- 例如序列 1 在离线时被删除，2–8 正常接收：received_through=0、settled_through=8、received_count=7、cancelled_count=1。
  缺口窗口按 settled_through 限制，内存中的待处置序列仍有界。账本每 scope 最多 100000 项，超限明确拒绝。
- 旧版本已清理但未记录 purged 的本地序列不自动猜测或回填；旧游标模式仍有原有永久缺口边界。
  本切片不提供自动换 epoch、改身份重放或通用 stream rollover。

## 有限目标与就绪协议

`durable_freeze_target(session, sequences)` 固定 1–128 个已接收到的序列及其 request/configuration 身份；
拒绝重复序列、未接收或删除来源。目标保存在服务端并绑定 scope、producer 和 epoch，同一集合幂等返回相同 target_id。
每 scope 最多 1000 个目标；不是“全库最新”或不断增长的 producer 流。查询只接受已有 target_id，不接受客户端自报水位或发布令牌。

事务 A 建立 `publication-manifest/1`：generation 绑定不可变 request ID、version=0、closed=false、令牌列表为空。
事务 B 原子提交接纳/拒绝/待决结果并关闭清单为 version=1：有处置则记录该事务的 publication token；
没有候选处置时明确 no_outputs=true。有输出但无可索引 Claim 时 no_indexable_outputs=true，例如全部待决。
重处理的旧贡献撤回也计入处置，不因新候选为空而漏掉决策发布。

capture 与 publication token 的 kind、scope、epoch、request generation 和配置分别绑定原接收及发布事务。
它们是服务端账本的稳定提交坐标，不是全局时间顺序，也不是可绕过认证的授权凭证。首次旧回执结构仍保持兼容。
清单内容、closed/version、令牌与输出标记不一致或缺失时，状态返回 blocked，不重建未经实际发布证明的历史完成状态。

`durable_readiness(session, target_id, stage=...)` 与 `durable_wait_until(...)` 使用 `durable-readiness/1`：

| stage / state | 当前合同 |
| --- | --- |
| source_persisted | 目标来源与处理请求仍存在时 reached；不宣称候选已决定 |
| l1_decided | 目标内所有 request 完成且清单闭合才 reached；全部待决也算已有明确处置 |
| processing | 仍有未闭合成员；返回实际令牌、未覆盖 request 和状态版本 |
| failed / blocked | 终止失败或需人工解决；来源删除/修订及缺失历史阻断，不能沿旧状态返回正文或清单 |
| index_visible / observation_covered / view_covered | 当前明确 unsupported；空令牌列表或闭合零输出不会假成功 |
| timed_out | SDK 等待结束，后台工作不取消；保留最后观察状态，未获响应不猜测成功 |

一个目标可以汇总多个请求各自的原子发布令牌：仅完成其中一个时不提前 reached。
后来的 append、修订或独立重处理不扩大旧目标。新解释激活后，旧目标保留原有限处置与令牌，
并报告 interpretation_current=false；发布时的处置计数不是当前事实正确性、有效性或索引可见性的证明。
每次查询重新认证会话、scope/epoch、来源存活及来源修订；来源删除时不返回残留发布清单。

SDK 默认最多等 30 秒，poll_interval 为 0.01–5 秒；timeout=0 做一次即时查询。
轮询请求受剩余时间约束，等待网络时不持数据库锁。实际宿主 ACL/用途政策及外部 dispatch 仍需后续完整合同。

## 迁移、验证与后续

成组更新 core、PostgreSQL provider 和 SDK，并停用不认识新序列协议/清单的旧写入进程。
旧 ProducerSession hash 不变；旧游标模式和旧 receipt/status API 保持原结构。
旧已完成 request 未保存清单时返回 readiness_history_unavailable；实际 worker 发布仍可为存活旧请求建立新清单。
回滚使用匹配旧二进制的部署前备份和权威删除记录，不单独降级后继续处理新协议状态。

运行示例：`python examples/durable_readiness.py`，删除离线序列 1，接收序列 2，等待固定 L1 目标并执行本地确定性 worker。
相关回归 **190 passed，0 skipped**；新增 40 项行为测试和 1 项架构检查。
补充清单强杀断言后复测 14 项恢复测试，补充等待边界后复测 22 项就绪测试，均属于上述集合，不重复计数。
core、PostgreSQL provider、SDK wheel 逐文件匹配最终源码，migration 010 已入包。
测试结果及指纹见 [validation-stage-05.json](validation-stage-05.json)；本轮不提供全量回归通过声明。

下一步是索引 outbox 与指定 publication 集合的完整覆盖；再补单请求多发布、显式重处理目标入口及通用资源刷新。
备份删除回放、真实领域 gold/校准和外部处理守卫继续按 [next-steps.md](next-steps.md) 推进。
