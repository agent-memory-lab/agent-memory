# 第十阶段：权威删除日志备份回放

编制：2026-10-07；设计保持 v6.1.0，计划/任务台账 revision 14。承接 [stage-09](stage-09.md)。
本轮交付 T23/T24 的受控同 scope 离线恢复切片，补充 T02/T04/T16/T17/T18/T22/T44。
44 项任务仍为 DONE 1、IN_PROGRESS 24、TODO 19；M0/M1 尚未整体验收。

## 完成能力与责任边界

恢复旧备份前，宿主须独立保留完整删除日志及其最新检查点；恢复副本在回放和验证前保持离线。
`PurgeRestore` 导出 HMAC 签名的完整、有限 purge 日志，在备份副本上验证独立 pin 后原子回放缺失后缀。
删除后的正文、候选、历史、索引和旧准备任务不能凭旧备份重新成为当前可用记忆。

| 位置 | 责任 |
| --- | --- |
| `operations/purge_restore.py` | exact scope/authority 绑定、签名与独立检查点校验、完整前缀/epoch 检查、回放协调与固定审计回执 |
| `ports.py` | PurgeRestoreUnitOfWork：同事务完整删除、原日志导入与恢复回执存取 |
| SQLite/PostgreSQL repository | 普通删除和恢复复用完整 `_forget_on_connection`；事务/锁仍由调用者管理 |
| SQLite `sqlite_retention.py`、PostgreSQL `retention.py` | 回放仍执行清理和 scope epoch 递增，暂停生成新的重复 purge 条目 |
| SQLite `sqlite_purge.py`、PostgreSQL `purge.py` | 原游标条目/head 导入及有限审计存储；不执行事实判断 |
| 可信宿主 | 日志与密钥独立保管、最新检查点获取、内容备份一致性、离线副本隔离与最终服务提升 |

恢复入口不加入 SDK/MCP 模型工具，不创建调度循环，也不安装自动阻断所有 provider 接口的服务网关。
scope 锁保证数据库内排序，不能替代部署隔离。不得一边向恢复副本开放读取、一边声称恢复屏障已完成。
本轮不把受控回放等同于完整跨 scope 灾备、远端缓存/供应商擦除或外部副本治理。

## 日志完整性与最新性

purge-restore-journal/1 保存原条目：cursor、对象身份、epoch、all_in_scope 和 mode。
它不导出来源正文、租约 token、模型输入或旧备份内容。模式仅接受现有 erase/archive；归档保持既有不可读合同。
checkpoint/1 绑定 authority_id、exact scope、head、scope_epoch 和完整条目 SHA-256；signature 覆盖整个日志和检查点。
密钥至少 32 字节，由宿主保护；scope 或 authority 错误、密钥不匹配、未知/损坏字段和正文混入均拒绝。

HMAC 证明来源及内容完整，不能证明某份已签旧日志是最新。
因此 replay 强制要求 expected_checkpoint，与日志检查点完全匹配；它必须从内容备份之外的可信控制面获取。
不得把同一旧备份内的 checkpoint 再传入，或在最新记录缺失时用旧日志替代。
签名密钥/authority 变更不自动迁移信任关系，需宿主重新验证和保管。

导出在删除共用的 scope 锁和一个事务中分页读取全日志，校验 1…head 没有缺页/跳号，
对象条目不改变 epoch，每条 all_in_scope 增加一次；最终 epoch 必须等于完整历史中的范围操作数。
日志没有自动截断、按最大游标猜测完成或自动压缩。
升级前未记录的删除不能补造；缺少从 epoch 0 开始的完整范围历史时明确拒绝。
本切片要求 journal-era 的一致备份和完整权威历史；pre-migration 009 的对象删除义务仍须宿主另行恢复，不能凭空证明。

## 一个事务内回放

1. 在开始数据库修改前复制并验证 JSON、签名、独立 pin 和完整条目语义。
2. 持有既有 scope 锁，读取副本日志；副本必须是权威完整日志的精确前缀，epoch 不能越过权威记录。
   副本更晚、日志回退、同 cursor 不同内容或缺页均拒绝，不能自动合并分叉。
3. 对缺失后缀逐条执行原模式的完整删除：来源/候选/证据历史、相关 Claim/Block、反馈、准备任务、
   各物理索引流及刷新依赖/证明/租约沿既有删除路径处理。
4. 导入原 cursor 和条目，逐条复查 epoch，最终再验证完整 journal/head/epoch。
5. 一起保存 purge-restore-receipt/1。任何清理、游标、epoch 或回执失败都整体回滚。

不能调用普通独立事务的 forget 再单独推进恢复游标；不能为导入条目重新分配 cursor 或多增一次范围代次。
副本前缀中已提交的范围删除不重复执行，否则会误删该范围擦除后合法新 epoch 的输入。
因此恢复对象须是完整事务边界上的一致物理备份；不支持“保留新日志但拼回旧正文”的混合恢复或任意损坏数据修复。

restore_id 固定一次操作；同 ID/检查点/actor/理由重复返回首次回执，不同合同拒绝。
回执报告原始 from_cursor、实际回放条数和删除影响，不报告事实可见、服务就绪或外部擦除完成。
回放与普通删除跨连接竞争复用 scope 锁：若普通删除先形成不同前缀，恢复显式冲突；若回放先完成，后续删除继续原游标。
宿主必须重新取得当前权威 pin，不能把先前完成回执当作永久服务提升许可。

## 离线操作顺序

1. 维护内容备份之外的删除日志、密钥和单调最新检查点；确定最新权威截止点并停止／协调相应写入。
   本模块提供导出，不自动复制每次删除到外部存储；部署方负责其提交与持久保管合同。
2. 在隔离副本中恢复完整内容、任务/outbox、依赖与日志，执行配套 initialize。
3. 从独立控制面读取最新 pin 和完整签名日志，以绑定的 authority/scope 执行 replay。
4. 校验回执、完整 head/epoch/hash、删除目标与相关守卫；确认权威 pin 在恢复期间没有推进。
   推进则取得更新日志继续回放；缺失/回退/分叉保持离线，不能提升服务。
5. 通过宿主部署检查后开放服务；旧 SDK outbox 仍使用原 purge cursor 清理正文，scope 擦除后旧 session 继续 revoked。

`examples/purge_restore.py` 展示真实 SQLite 旧备份、独立 control-plane 文件、原子回放、重复操作和幸存来源。
PostgreSQL 专项使用真实 pg_dump 一致备份，在独立 schema 中恢复全部表/历史/索引，再执行同合同。

## 容量、升级与回滚

完整有限日志上限 4096 条、签名 JSON 上限 2000000 字节；每 scope 最多 1000 份恢复审计。
超限明确拒绝，不返回已覆盖或部分成功。大规模批次恢复、外部日志存储/单调控制面实现和跨 scope 协调仍待专属实施。

SQLite 初始化增加 retention_purge_restores；PostgreSQL migration 014 增加 agent_memory_retention_purge_restores。
配套升级 core/PostgreSQL 包并执行 initialize，旧 purge 日志和游标原样保留，重复初始化幂等。
自定义存储须实现新增端口；缺端口返回 purge_restore_unsupported。
未改公共 capture/SDK/MCP 协议或现有索引流；恢复后降级不能回滚已生效删除或日志代次。
沿用第九阶段新流已激活时不能直接降级旧 core 的限制；旧版本不具备本轮离线恢复能力。

## 验证与后续

按用户要求，仅新增专项与受影响的删除、来源/贡献/字段证据/双时态历史、索引/刷新、重处理、
SDK/MCP、迁移和依赖方向回归；最终 **508 passed，0 skipped**，未运行全量测试。
包含 64 项新增恢复行为与 1 项新增架构检查；SQLite 和真实 PostgreSQL 17 共同执行。
新增 4 项真实 SIGKILL 覆盖回放提交前后，另回归已有索引、资源刷新与处理进程恢复；不声明数据库断电通过。
完整正文/历史/准备清理、幸存来源、两代索引、原游标、旧离线 outbox、范围代次、CAS/重复/冲突、
131 条分页、跨连接竞争、缺页/回退/分叉/损坏/容量和提交整体回滚均有专项。
core/PostgreSQL wheel 与最终源码逐字节匹配，migration 014 已入包；见 [验证记录](validation-stage-10.json)。

下一步优先 T16/T22：单个 L1 请求多次发布与闭合清单覆盖。
protected journal 部署、规模/远端恢复、具体派生权限依赖、真实领域 gold 与外部模型治理仍按各自验收推进。
