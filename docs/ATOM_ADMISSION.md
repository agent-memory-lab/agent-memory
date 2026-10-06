# 显式 Atom 接纳接口

本接口实现 [目标架构](AGENT_MEMORY_ARCHITECTURE_PLAN.md) 的首期 L0→L1 流程：宿主显式提供结构化候选和来源权限，系统保存接纳决策、待决记录、证据时间与双时态状态。它与已有 `remember(..., claims=...)` 分开启用，不会自动迁移或重新核验旧 Claim。

目前覆盖单值状态、偏好和约束，值类型为 `string / integer / number / boolean`。事件原子、集合成员、自动外部核验、持久工作队列、L2/L3 派生和质量预算校准尚未在此接口实现。内置存储适配器提供接纳持久化；自定义 provider 需要实现相应能力，不能仅凭旧 Claim 接口宣称支持。

## 1. 宿主与记忆库各负责什么

宿主根据真实身份和领域权限构造 `SourceAuthority`，明确允许该来源为哪些主体、属性提供信息。不要把模型生成的 JSON、用户提交的 `metadata.trust_label` 或消息角色直接转换为权限。`source_id` 是宿主提供的来源标识，本库不替宿主验证其身份。

记忆库检查属性是否注册、值类型是否匹配、来源是否获准、引用是否出现在保存文本中、时间是否合法，以及槽内变化能否发布。`source_quote` **仅用于定位原文**；即使引用完全匹配，也不表示系统验证了抽取的语义或现实事实。首期显式 API 的宿主负责确认主体、值、语气和时间与原文相符。

`self_report` 可用于策略允许的自述；`tool_observation`、`document` 表示宿主认证的工具或文件来源。未知来源性质、`inference`、未经授权主体或属性会保留为待决，不能通过提高置信分数绕过。该接口不接收模型置信分数。

## 2. 写入、查看状态和召回

下面的示例可以作为独立 Python 脚本运行：

```python
import asyncio
from datetime import UTC, datetime

from agent_memory import (
    AdmissionPolicy, AgentMemory, AtomDraft, MemoryScope,
    PredicateSpec, SourceAuthority,
)


async def main() -> None:
    scope = MemoryScope("demo", user_id="alice", session_id="session-1")
    policy = AdmissionPolicy([PredicateSpec("response_language")])
    authority = SourceAuthority(
        "authenticated-user:alice",
        kind="self_report",
        subjects=("alice",),
        predicates=("response_language",),
    )
    observed_at = datetime.now(UTC)

    async with AgentMemory.local("atom-demo.sqlite3", scope=scope) as memory:
        receipt = await memory.remember_atoms(
            "我偏好中文回答。",
            atoms=(AtomDraft(
                subject_id="alice",
                predicate="response_language",
                value="zh-CN",
                text="Alice 自述偏好中文回答。",
                source_quote="我偏好中文回答",
                kind="preference",
            ),),
            authority=authority,
            policy=policy,
            idempotency_key="alice-language-first-report",
            occurred_at=observed_at,
        )
        print(receipt.decisions)
        candidate = receipt.candidate_ids[0]
        print(await memory.atom_status(candidate))
        print(await memory.atom_history(candidate))

        bundle = await memory.recall(
            "Alice 偏好什么回答语言？",
            valid_at=observed_at,
            known_at=datetime.now(UTC),
        )
        print([(claim.text, claim.value) for claim in bundle.current_state])
        print(bundle.retrieval_metadata.get("atom_support", {}))


asyncio.run(main())
```

`AtomDraft` 默认使用 session 范围；需要用户范围时显式设置 `scope_level=ScopeLevel.USER`，并确保宿主允许这种晋升。状态槽由 scope、主体和属性决定，不同主体的同名属性互不覆盖。属性须预先注册，Python 的 `True` 不会作为整数或数字接纳。

回执与旧 `IngestResult` 分开：

| 字段 | 含义 |
| --- | --- |
| `event_id` | 保存的 L0 事件 ID |
| `candidate_ids` | 本次候选身份，用于查询、人工核验和更正目标 |
| `claim_ids` | 本次实际发布的 Claim ID；待决候选不会出现在这里 |
| `decisions` | 每项候选的 `candidate_id / action / reasons` |
| `pending_ids` | 等待核验或有争议的候选 |
| `duplicate` | 是否为相同输入的幂等重试 |

动作包括 `ACCEPT`、`PENDING_VERIFICATION`、`CONTESTED`、`L0_ONLY`；宿主解决待决时也可能产生 `REJECT`。没有 Claim ID 不等于原文写入失败。幂等重试复用首次写入回执；候选后续的最新动作和工作版本请查询 `atom_status`，历史记录请查询 `atom_history`。同一幂等键不得换用不同内容、候选、来源权限或策略配置。省略 `occurred_at` 时重试采用首次入库时间；显式提供时间时必须重用原时间。再次独立运行示例并产生新观察，应更换幂等键或使用空数据库。

## 3. 待决信息由宿主提供新证据后解决

`resolve_atom` 是显式核验入口，不会自行调用业务接口。宿主先读取当前工作版本，再提交有权限的新来源、新原文和判断结果；版本已变化或来源已删除时须重新读取并判断。

下例沿用上面的导入；在已初始化的 `memory` 上调用：

```python
async def verify_order(memory):
    policy = AdmissionPolicy([
        PredicateSpec("payment_received", value_type="boolean", allow_self_report=False),
    ])
    report = SourceAuthority(
        "authenticated-user:alice",
        subjects=("order:42",),
        predicates=("payment_received",),
    )
    pending = await memory.remember_atoms(
        "订单 42 已经付款。",
        atoms=(AtomDraft(
            "order:42", "payment_received", True,
            "来源报告订单 42 已付款。", "订单 42 已经付款",
        ),),
        authority=report,
        policy=policy,
    )
    candidate = pending.pending_ids[0]
    status = await memory.atom_status(candidate)

    # 宿主应先实际查询获准的订单接口，并确认返回值的含义。
    verified_at = datetime.now(UTC)
    tool_source = SourceAuthority(
        "authenticated-order-api",
        kind="tool_observation",
        subjects=("order:42",),
        predicates=("payment_received",),
    )
    return await memory.resolve_atom(
        candidate,
        "订单接口在本次查询时返回：订单 42 已付款。",
        authority=tool_source,
        policy=policy,
        expected_version=status["version"],
        accept=True,
        source_quote="订单 42 已付款",
        occurred_at=verified_at,
    )
```

没有给出 `support_from` 的核验只证明本次观察时点的来源报告，接纳从该观察时点开始；不能因此认定原始报告到核验前的整段时间已经获证实。若来源确实声明一个现实区间，可显式提供 `support_from / support_to`。冲突解决当前要求所给区间与待解决候选的区间完全一致；不相同的重叠争议不会被一并清除。

原先 `PENDING_VERIFICATION` 的候选获得合格证据后，还须通过当前槽的冲突判断。首次核验如果发现互斥值，会将其转为 `CONTESTED`，回执仍无 Claim ID；合格证据本身不会替宿主裁决争议。宿主应读取新的 `atom_status` 版本与现实区间，再以覆盖该完整区间的证据明确调用 `resolve_atom` 选择接受或拒绝。不要把 `accept=True` 的调用成功等同于最终 `ACCEPT`，应检查返回动作。

`accept=False` 同样需要合格来源和匹配引用。接口只解决待核验或争议候选；缺少更正目标、无法找到临时覆盖基础等结构问题，需重新提交完整的 Atom，而非通过核验强行发布。

## 4. 变化、更正与时间

所有时间必须带时区，区间为 `[start, end)`。`valid_from / valid_to` 是宿主从来源中确认的现实时间声明；`occurred_at` 是本次来源观察或事件时间；系统认知版本由服务端为事务统一分配系统时间边界。查询的 `valid_at / known_at` 分别选择现实和认知时间，后收到的更正不会出现在更早的 `known_at` 中，同批发布也不会在历史查询中只出现一部分。

| 输入 | 当前实现的含义 |
| --- | --- |
| 省略 `valid_from` | 从来源观察时点推定状态，证据本身仍是 point；无法支持更早现实时间 |
| 显式 `valid_from / valid_to` | 来源明确声明该区间；宿主不能为方便查询而编造区间 |
| `change_kind="replace"` | 有明确新起点的真实替换；新区间结束后未知，不恢复旧值 |
| `change_kind="temporary_override"` | 必须给出 `valid_to`，且起点有唯一可用基础状态；系统记录基础候选，结束后仅恢复仍有效的基础 |
| `change_kind="correct"` | `corrects_id` 指向**候选 ID**，追加对指定旧断言的解释，保存原认知历史 |
| `modality="planned"` 等非 asserted | 仅保留 L0，不发布为已发生的当前状态 |

这里的 `corrects_id` 使用 `receipt.candidate_ids[...]`，与旧 `remember(..., claims=...)` 使用 Claim ID 的协议不同。更正目标必须同槽且可更正；跨主体、跨属性、找不到目标或同批多项更正同一目标会保持待决。首期不支持直接更正临时覆盖。

同批互斥值，或同现实起点的互斥断言，会形成争议；没有明确新有效起点的不同值也不能只凭后来入库替换旧状态。争议影响的现实区间不返回唯一事实，详情见召回元数据 `conflicts`。真正的变化应由宿主明确给出新的有效起点。

尚未解决的真实替换争议到期后，也不会自动恢复更旧的值；后续独立接纳的明确状态会截断这段不确定时间。因此迟到的旧争议不会覆盖已经确认的更晚状态。

未来生效的候选可以先保存，生效前不会成为当前状态；结束边界也不包含在有效区间中。杭州→上海→杭州是三次状态，不会把两次杭州合并为连续居住。临时覆盖期间若出现新的真实替换，覆盖到期不能复活已被替换的基础状态；基础不唯一或重叠覆盖无法确定时保持待决。

`atom_support` 区分实际适用的来源证据与 `assumed_continuity`（持续性推定）。例如今天的点观察不能加强上个月的自述；查询超出点观察本身、但仍采用持续状态时，要保留推定标签。Claim 的兼容评分字段不是事实真实性或核验强度，宿主应读取接纳和证据信息。

## 5. 删除与首期边界

删除任一 admission 来源时，当前实现会撤下该来源影响的**同 scope、同 slot 整条接纳时间线**，以防残留历史恢复出已经删除的值。这是首期的保守行为，可能连带移除同槽其他来源；不要将其描述为精细的单证据重评估。其他主体、属性和范围的槽不应受影响。

当前接口不负责模型语义抽取，也没有自动外部核验循环、核验任务队列或 L2/L3 自动晋升。旧的 `remember`、自动轨迹抽取和自定义检索插件仍使用各自既有接口；显式 Atom 的最终可见性需经过接纳守卫。不能把使用某个旧插件等同于启用了本接纳协议。

当前 facade 默认每次最多 32 个 Atom，内核硬上限为 64 个。单次可见候选快照上限为 1024 条，超过时明确报错，不静默截断时间线；大规模历史分页与索引优化仍需后续实现。

另外，旧轨迹生成器的模型结果若缺失 `confidence`，不再默认按 `1.0` 接纳；非数字、布尔值、非有限值等无效评分同样跳过。显式 SDK 元数据 Claim 的旧默认行为保持兼容。这项兼容修复并不等于为模型评分提供了真实性保证。

完整目标、后续异步任务、派生依赖及迁移计划见 [Agent Memory 架构提案](AGENT_MEMORY_ARCHITECTURE_PLAN.md)。本页只描述显式 Atom 接口的首期能力。
