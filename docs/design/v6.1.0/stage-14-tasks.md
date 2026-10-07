# 第十四阶段执行任务

版本 AM61-ST14-TASKS / 1.2；2026-10-07；IN_PROGRESS（A、B.1/B.2 已完成，完整 B 与 C 尚未完成）。
方案：[stage-14-plan.md](stage-14-plan.md)；A 实施：[stage-14a.md](stage-14a.md)；证据：[validation-stage-14a.json](validation-stage-14a.json)。
全局任务状态继续保持，完整 M2 尚未验收。
B.1 实施：[stage-14b.md](stage-14b.md)；证据：[validation-stage-14b.json](validation-stage-14b.json)。
B.2 实施：[stage-14b2.md](stage-14b2.md)；证据：[validation-stage-14b2.json](validation-stage-14b2.json)。

| ID | 交付与验收 | 状态 |
| --- | --- | --- |
| ST14-A01 | 类型化 query、完整候选规则、exact scope、consumer 整体适配和容量 | DONE |
| ST14-A02 | query CAS、代次/hash、原新成员屏障、dirty outbox 与旧任务阻断 | DONE |
| ST14-A03 | 宿主 authority、CAS、期限/撤销、外置当前版本 floor、用途/读者交集和稳定拒绝 | DONE |
| ST14-A04 | 来源 grant 绑定 authority、续期后重新授权、旧宿主禁止降级 | DONE |
| ST14-A05 | 实际查询全集授权先于正文；头版本、manifest、时间与最终交付守卫 | DONE |
| ST14-A06 | authority/context 组合、路由领取、只读 SDK/MCP 和旧 v1 兼容 | DONE |
| ST14-A07 | 双后端跨连接 CAS/发布竞争、真实 SIGKILL 前后原子控制/outbox | DONE |
| ST14-A08 | 范围/对象擦除、新控制墓碑、真实 backup/pg_dump 删除日志回放与旧权限快照阻断 | DONE |
| ST14-A09 | 示例、专项/相关回归、构建安装、版本记录和后续顺序 | DONE |
| ST14-B01 | 固定历史请求身份、known_at/valid_at、定义/政策/上下文版本；B.1 非条件合同已交付，历史 context 待补 | IN_PROGRESS |
| ST14-B02 | 历史候选全集、来源解释版本、连续/空 coverage 与历史重建；B.1/B.2 非条件完整/空检查点及稳定区间已交付，历史资格上下文待补 | IN_PROGRESS |
| ST14-B03 | 历史解释与当前权限/擦除双守卫、授权变化和来源删除竞争；B.1/B.2 已验收，条件扩展继续验收 | IN_PROGRESS |
| ST14-B04 | 双后端相关验收、历史能力 opt-in 和接口合同、独立验证记录；B.1/B.2 已验收，完整 B 未验收 | IN_PROGRESS |
| ST14-C01 | 实际派生父 manifest、固定父版本、processing/support 分离 | TODO |
| ST14-C02 | 传递权限交集、循环/深度/容量和父刷新失效 | TODO |
| ST14-C03 | 传递物理擦除、恢复和实际输入证明，验收后开放派生父 | TODO |

| B.1 子任务 | 验收边界 | 状态 |
| --- | --- | --- |
| ST14-B.1-01 | 发布事务宿主时间、固定双时间合同、精确检查点与缺口拒绝 | DONE |
| ST14-B.1-02 | 非条件 query/definition/policy/L1 版本归档、完整/空覆盖、完成证书与完整性 | DONE |
| ST14-B.1-03 | 当时认知/当前权限分离，撤权/期限/外置 floor、删除与真实备份回放 | DONE |
| ST14-B.1-04 | 双后端原子发布、真实 SIGKILL、容量回滚、只读 SDK/MCP 与最终固定时间交付 | DONE |
| ST14-B.1-05 | 可运行示例、新增/受影响测试、构建安装及独立版本记录 | DONE |

| B.2 子任务 | 验收边界 | 状态 |
| --- | --- | --- |
| ST14-B.2-01 | 显式区间 mode、完整/空全集、半开区间与缺口拒绝；无迁移回填 | DONE |
| ST14-B.2-02 | 候选/解释/文档/query/定义与政策首次变化原 UoW 关闭；输入 head 摘要防止未跟踪变化被封存 | DONE |
| ST14-B.2-03 | 时钟倒退/前沿、当前权限先于历史正文、后端写合同、对象/范围擦除与真实备份回放 | DONE |
| ST14-B.2-04 | 双后端独立连接竞争、真实 SIGKILL、SDK/MCP 固定双时间与最终重检 | DONE |
| ST14-B.2-05 | 818 项相关回归、78 项新增行为、构建/安装/示例及独立版本记录 | DONE |

下一步 B.3 历史资格上下文，完成 B01–B04 整体验收后推进 C。
没有历史政策版本或可证实的完整覆盖时不返回历史 Observation。
