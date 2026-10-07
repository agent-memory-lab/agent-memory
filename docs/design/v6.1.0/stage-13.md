# 第十三阶段：资格保持的语言 facet

版本 AM61-ST13-IMPLEMENTATION / 1.0，2026-10-07；台账 revision 18。
本阶段 ST13-01–ST13-08 完成，方案见 [stage-13-plan.md](stage-13-plan.md)，任务见 [stage-13-tasks.md](stage-13-tasks.md)，运行证据见 [validation-stage-13.json](validation-stage-13.json)。
实现以 `655de37` 为基线；该提交另保存第十二阶段 1667 项全量通过结果。新增代码仅声明本轮专项验证，不能混用旧全量成绩。

## 已实现

新增宿主显式启用的 `locale-context/1`。一个或多个来源、同一 exact scope 的语言偏好可以保留不同项目适用域、条件与例外。
例如“项目 A 使用中文，节假日除外”：A/非节假日得到 qualified source_fact；B 或已知节假日完成空重建；项目或节假日状态缺失得到无 value 的 context_unknown。
条件候选一直保留 PENDING_VERIFICATION；原无条件 Claim/普通检索不吸收此输出。

完整字段支持才输出事实，保留源原话定位、条件/例外原文及宿主绑定、字段支持表达式、当前证据 ID 和精确支持范围。
AND 求时间交，OR 保留间隙；点证据在精确时刻生效，时钟推进 1 微秒旧输出即失效。
先按适用域合成，再使用显式偏序；不比较的异值输出冲突，高优先不完整或过期不会自动恢复低优先值。

## 模块职责

| 落点 | 本轮责任 |
| --- | --- |
| derived/model.py | FacetContext 纯合同、受控路由属性/期限、定义序列化与 scope 擦除上下文清理 |
| derived/contextual.py | 纯当前语言投影，复用 Condition、FieldSupport 和域合成；输出资格与时间边界 |
| derived/observation.py | 按明确模板分派，原 locale-snapshot/1 保持拒绝条件输入 |
| derived/service.py | 可信路由检查、输入许可、辅助来源当前性、原子发布与当前/最终交付检查 |
| operations/facet_refresh.py | 按宿主路由领取，其他路由不改任务；固定目标保存原路由 |

保留原 storage/UoW、三种依赖边、BoundedWorker 及 SQLite/PostgreSQL 适配；无需新 DDL。
所有实际辅助来源也必须有 processing grant，未被选中的证据仍进入输入谱系。
对象/辅助来源擦除清除已发布全部相关修订正文、清单和边；OR 独立证据幸存时从获准新快照重建。
scope 擦除还移除已退休定义中的上下文属性值。

## 宿主使用与升级

安装 core + SDK 后运行 `python examples/derived_contextual_observation.py`；示例为合成原话，贯通持久接收/闭合 L1 → 宿主字段/条件审查 → Observation → SDK。
宿主使用 `FacetContext(QueryContext(..., snapshot_token=<route>), ProjectionPolicy(...), expires_at)` 注册 `FacetDefinition(..., template_version="locale-context/1", context=...)`。
对应 `ObservationService(..., context_token=<route>)` 由宿主配置；SDK/MCP 工具参数不能传入属性或覆盖身份。

首版路由属性为 project（字符串）、holiday（布尔值），另允许可信 timezone；不能把任意私有事实放进路由属性绕过输入谱系。
所有绑定期限明确且最多 24 小时。宿主属性/政策改变或续期时，以 register(expected_generation=...) 先推进定义代次，再重建。
一个服务实例对应其绑定路由；多路由宿主分别构造服务和可信接入，不向模型暴露选择路由的权限。
更改构建中的上下文会使旧单元不能提交；更改最终交付边界的上下文会使旧正文不能返回。

旧 v1 定义不新增 context 字段，原 fingerprint、实例构造和读取继续兼容。
降级前关闭 v2 capability；不能用旧代码继续服务 v2 定义。保留账本及删除屏障。

## 验证与剩余范围

本轮 656 个唯一专项/受影响用例通过、0 失败、0 跳过；其中新增 71 项（双真实后端各 33 项，纯合同 5 项）。
按当前收集的用例集合取最新成绩，剔除被新参数化取代的旧测试名称，避免重复累计。
覆盖条件/例外三值、项目分域、必要字段/原话、AND/OR/点/午夜、偏序/冲突、context CAS/过期、错误路由、准备/发布及交付竞争、辅助输入权限/擦除、SDK/MCP 及旧 v1。
原真实 SIGKILL/备份/独立连接竞争属于受影响回归中的第十二阶段用例；不声称新增了同样数量的进程故障实验。
四个发行包 sdist/wheel 成功并核对源码；安装后的条件示例与真实 PostgreSQL 重复初始化/空发布通过；新 71 项在 wheel 安装目录再次验证。

历史派生、通用 query/动态 ACL、派生父输入、跨 scope、跨槽条件贡献更正、L2/L3 与外部模型仍未启用。
本轮的 source-grounded 合成夹具不构成生产抽取 gold。下一步先推进通用查询定义及宿主权限 authority，见 [next-steps.md](next-steps.md)。
