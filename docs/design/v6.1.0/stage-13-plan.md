# 第十三阶段方案：资格保持的语言 facet

版本 AM61-ST13 / 1.0；2026-10-07；IMPLEMENTED；执行台账 revision 18。
依据冻结 v6.1.0、[后续顺序](next-steps.md)及[第十二阶段](stage-12.md)，推进 T11/T25/T26/T27/T29/T44 的有界切片。
任务与验收见 [stage-13-tasks.md](stage-13-tasks.md)，实际结果见 [stage-13.md](stage-13.md)。

## 交付范围

同 exact scope、主体为 scope.user_id、`locale` 字符串；新增显式 opt-in `locale-context/1`。
输入仍为已闭合初次 primary 或已激活解释中的 L1。条件候选保留 PENDING_VERIFICATION，仅在宿主审查、全部必要字段及限定支持完整、可信上下文适用时生成限定 Observation。
不改无条件 Claim，不把语义未知、证据间隙或高优先失效当作无条件事实。

## 合同和流程

1. **可信上下文。** `FacetContext(QueryContext, ProjectionPolicy, expires_at)` 只由宿主构建。
   绑定 exact scope、主体、用途、principal、路由 token、属性 authority 与显式期限。
   首版路由属性仅 project/holiday，另支持可信 timezone；不得承载从其他记忆秘密推导的任意事实。
   valid_at/known_at 相同用于登记当前绑定，实际投影使用当前时钟；不是历史派生入口。期限最多 24 小时。
2. **定义版本。** 服务构造时绑定宿主 `context_token`，定义只能属于此路由。
   全部上下文/政策/期限进入定义 fingerprint 和原工作单元 generation。变更/续期由 register CAS 推进代次。
   读取、领取、准备和提交校验路由；其他路由 worker 不修改本路由任务。
3. **证据资格。** 重用 ContextualMemory 的审查账本、Condition 三值与 FieldSupport AND/OR。
   复核 target hash、审查 principal、政策版本、原限定绑定数量、必要字段、来源原话/定位及内容 hash。
   所有实际 L0/L1 输入包括未引用辅助证据都先获 grant，再读正文；辅助 retained source 也必须当前有效。
4. **投影。** 先按适用域判断条件/例外，再依明确 single_exclusive/ordered_override 合成。
   同域同有效起点异值为争议，不以写入顺序选值。无上下文/证据不足产生不带事实 value 的 context_unknown。
   确定不适用可完成空重建；高优先过期/不完整不自动回退。原无条件 v1 路径继续按原合同运行。
5. **时间和发布。** 保留字段支持交集、OR 间隙、点证据；下一边界包括支持起止、有效期、当地午夜、grant/context 到期。
   点证据仅在精确时间点成立，时钟推进最小精度 1 微秒即阻断旧输出。发布仍复核原 manifest、query 集合、输入、head CAS、lease 与时间覆盖，一次原子提交。
6. **交付/擦除。** SDK/MCP 仍只读，工具不能注入属性或权限；最终 derived_context 再校验一次。
   对象/辅助证据擦除使所有相关派生正文与清单物理消失；scope 擦除额外清除已退休定义中的上下文值。

## 架构与兼容

model.py 保留纯合同；新增 derived/contextual.py 负责纯有界合成；service.py 管授权、版本和交付，FacetRefreshQueue 管有限任务与路由领取。
复用原三种边、事务、双后端和 BoundedWorker；无需新表或迁移。
v1 未设置 context 时不增加序列化字段，保持已有 fingerprint 与读取行为；不得用 v2 代码偷偷接受旧 v1 条件输入。

## 验收与后续边界

测试条件真/假/未知、例外未知、project A/B、AND/OR 时间、点证据、午夜、上下文到期、权限与擦除、上下文 CAS、交付竞争、错误路由、SDK/MCP 和旧 v1。
本阶段使用语义明确且原话落源的合成夹具，不能替代真实领域 gold。
只运行新增及受影响测试。此前用户要求的全量结果独立保存在 [全量补充](stage-12-full-test.md)，不当作新代码的全量证据。

后续依序为通用 query/ACL authority、政策与上下文历史覆盖、传递 processing 图；随后再做 L2 页面及 full rebuild，最后按证据选择 delta/L3。当前不开放跨 scope、派生父输入、历史派生或外部模型。
