# Agent Memory v3 反馈关联契约

版本：1.0

日期：2026-09-11

## 1. 边界

宿主定义任务结果、评价标准和奖励公式；Agent Memory 校验关联、保存证据、组织记忆并为受控进化提供数据。所有 scope 来自可信宿主上下文，不从模型可控负载提升权限。

## 2. 关联链

```text
run_id
  -> request_id -> bundle_id -> retrieval_trace
  -> decision_id
  -> outcome_id
  -> evaluation_id
  -> reward_id
```

| 标识 | 生成方 | 作用 |
| --- | --- | --- |
| `run_id` | 宿主 | 一次任务或执行轨迹 |
| `request_id` | 宿主或 SDK | 一次检索请求 |
| `bundle_id` | Memory plugin | 一个不可变的返回集合 |
| `decision_id` | 宿主 | 一次实际决策 |
| `outcome_id` | 宿主 | 决策的结果记录 |
| `evaluation_id` | 授权评价器 | 对结果的版本化评价 |
| `reward_id` | 宿主授权公式 | 可选的数值信号 |

记忆引用使用 `(memory_id, version)`，不允许只依赖可变的当前版本。

## 3. 返回与使用

RetrievalTrace 记录候选项、返回项、策略版本、Token 预算和裁剪摘要。Decision 另行记录宿主确认的实际使用项。

`memory_usage` 只有三种值：

- `confirmed`：`used_memory_refs` 由宿主明确提交。
- `none`：宿主确认未使用任何返回记忆。
- `unknown`：宿主没有提供使用信息。

`unknown` 不能转换为全部使用或全部未使用。

## 4. 结果与评价

Outcome 终止状态为 `succeeded | failed | cancelled | timed_out | unknown`。`cancelled`、`timed_out` 和 `unknown` 不自动计入失败率，是否纳入指标由评价规则版本决定。

Evaluation 必须包含 evaluator ID/version、rubric ID/version、指标和证据摘要。Reward 必须引用 Evaluation，并包含 reward definition ID/version。不同 rubric 或 reward definition 的值不直接合并。

## 5. 写入结果

每次写入返回收据：

| 状态 | 含义 |
| --- | --- |
| `accepted` | 引用完整且通过信任校验，可供后续整理 |
| `duplicate` | 同一幂等键与完全相同负载已接受 |
| `pending` | 前置引用尚未到达，在有界期限内等待关联 |
| `rejected` | 越权、不可信来源、版本冲突或无效负载 |
| `superseded` | 该记录已被追加式更正取代 |
| `expired` | 待关联记录超过保留期，不得参与聚合或晋升 |

## 6. 幂等、乱序与更正

- 幂等唯一键为 `(partition_key, record_type, idempotency_key)`。
- 同键同负载返回原收据；同键不同负载返回冲突，不覆盖。
- 乱序 Outcome/Evaluation/Reward 可进入 `pending`，默认最多 1,000 条、保留 24 小时；宿主可收紧上限。
- pending 记录不参与 Episode、Procedure、策略统计或晋升。
- 更正通过 `corrects_id` 追加新记录；旧记录转为 `superseded`，不原地修改审计负载。

## 7. scope 与删除

所有引用的 partition key 必须与写入 scope 一致。不向调用方透露其他 scope 的记录是否存在。来源真删除后，依赖反馈、评价、Episode 和候选转为无效或同步删除敏感负载，不再参与检索、统计和晋升。

## 8. 向后兼容

- 保留现有 `record_decision`/`record_outcome`/`record_reward` 方法名。
- 新字段使用可区分的默认值：旧 Decision 的使用状态为 `unknown`，旧 Outcome 从 `success` 映射为 succeeded/failed，旧 Reward 保留 formula version 但在权威评价中标记 provenance incomplete。
- SQLite 采用可重入 schema migration；PostgreSQL 用新迁移文件，不重写已发布迁移。
- 未声明 feedback capability 的 provider 继续支持基础 ingest/retrieve/state/forget，调用反馈接口时返回明确的不支持错误。
