# Claim 双时态

原子事实现在分别记录现实有效区间和系统认知区间。SQLite 与 PostgreSQL 使用同一套有效期计算逻辑，数据库适配器负责事务、范围隔离和持久化。

## 时间含义

| 用户提出的时间 | 实现字段 | 含义 |
| --- | --- | --- |
| `t_valid` | `valid_from` | 事实在现实中开始有效的时间 |
| `t_invalid` | `valid_to` | 事实在现实中停止有效的时间，空表示没有声明结束时间 |
| `t_created` | 原始观察的 `observed_at`；Claim 的 `created_at` 保留创建时间 | 系统接收到这项断言的时间，由可信写入端生成 |
| 认知版本开始/结束 | `system_from` / `system_to` | 系统在这一段时间内采用的有效期解释 |

所有区间采用 `[start, end)`。查询参数 `valid_at` 表示“现实中的哪个时刻”，`known_at` 表示“截至系统何时的认知”。时间必须携带时区，持久化有效期统一到 UTC。一个参数省略时取查询时刻；两个都省略时保持普通当前记忆查询。

`created_at` 不能独自承担认知历史：9 月 1 日创建的事实，在 9 月 20 日可能被重新解释为截至 9 月 8 日有效。原始创建时间仍为 9 月 1 日，新解释的 `system_from` 为 9 月 20 日。原解释的 `system_to` 也在此时关闭。

## 普通变化、迟到信息、追溯更正

- 普通变化：同一范围、同一 key 的断言按现实生效时间排列。新断言在其有效区间内优先；同一起点按系统观察顺序选择。
- 迟到信息：晚收到的旧事实可以补充过去的有效区间，不能覆盖生效时间更晚的事实。
- 有限有效期：临时断言结束后，先前没有声明结束时间的事实可再次有效。这是一项明确的状态断言合并策略；需要“之后未知”的业务，应同时结束原断言，不能依赖临时值自动永久覆盖。
- 追溯更正：传入 `corrects_id`，引用同一精确范围、同一 key 的原 Claim。新记录替换该原始断言的解释，同时保留更正前的系统快照。目标不存在、已经被更正或位于其他范围时，整个写入事务回滚。
- 同值确认：没有显式有效期且原 Claim 在本次事件时刻仍然有效时，增加佐证来源并发布新认知版本。历史快照不会因此获得后来才出现的来源。

例如系统先在 2026-09-10 收到“从 2026-09-05 起住杭州”，再在 2026-09-20 收到更正“实际从 2026-09-08 起住杭州”。假设此前住上海：

| `valid_at` | `known_at` | 答案 |
| --- | --- | --- |
| 2026-09-06 | 2026-09-15 | 杭州 |
| 2026-09-06 | 2026-09-25 | 上海 |
| 2026-09-08 | 2026-09-25 | 杭州 |

## 使用

```python
from datetime import datetime
from agent_memory import AgentMemory

async def example():
    async with AgentMemory.local("memory.db") as memory:
        moved = await memory.remember(
            "用户搬到杭州",
            claims=[{
                "key": "home_city",
                "value": "杭州",
                "text": "用户从 2026-09-05 起住杭州",
                "valid_from": "2026-09-05T00:00:00+08:00",
            }],
        )
        await memory.remember(
            "更正搬家日期",
            claims=[{
                "key": "home_city",
                "value": "杭州",
                "text": "实际从 2026-09-08 起住杭州",
                "valid_from": "2026-09-08T00:00:00+08:00",
                "corrects_id": moved.claim_ids[0],
            }],
        )
        bundle = await memory.recall(
            "居住城市",
            valid_at=datetime.fromisoformat("2026-09-06T00:00:00+08:00"),
            known_at=datetime.now().astimezone(),
        )
        return bundle
```

上例实际在运行时写入；`known_at` 应使用真实系统接收时间或返回的版本时间，不能把今天写入的记录伪装成 9 月已知。自动化测试用可控服务端时钟验证上表。

直接使用内核时调用 `get_state_at(scope, valid_at=..., known_at=...)`；`MemoryQuery`、`AgentMemory.recall`、`UnifiedMemory.recall` 均接受两个参数。MCP 的 `memory_retrieve` 和 Python SDK 的 `retrieve` 接受带时区的 ISO-8601 字符串。能力声明为 `bitemporal_claims`，第三方提供者可通过可选的 `BitemporalMemoryRepository` / `BitemporalMemoryProvider` 端口实现，不强制旧插件支持。

## 存储与迁移

SQLite 的 `claim_observations` / `claim_versions` 和 PostgreSQL 的对应 `agent_memory_` 表保存原始输入与认知快照。每次写入在同一事务内关闭旧快照、生成新快照。旧 `claims` 行继续承担写入版本号及兼容接口的职责；当前状态和 Claim 检索由时间快照决定。事实的写入版本号与“当前有效状态”可能不同，例如先写未来生效的事实，再补写迟到的旧事实。

PostgreSQL 新增可重复执行的 `004_claim_history.sql`；SQLite 初始化时自动增加表。协议和事件 schema 仍为 v2，因为本次为可选能力及存储表的追加。

旧数据库只能迁移仍然留存的有效期解释，无法补回已经覆盖的认知。迁移记录一个可查询的认知起点；查询早于该起点、且范围中包含旧数据时抛出 `TemporalHistoryUnavailable`。重复初始化不会重写起点。全新数据库查询写入前的时间返回空结果。

## 明确边界

1. 本次双时态覆盖 **Claim 原子事实**。事件、场景块、Episode、Procedure、Persona 与 ontology 的历史视图没有随之实现。显式时间查询仅返回 Claim，不拼入当前原文、当前摘要或画像，也绕过不支持历史快照的自定义召回管线。
2. 当前查询、历史查询都受现有范围与证据可用性限制。已删除或归档的事实/原文不因回看旧版本恢复；来源删除会同步清理历史载荷的来源列表。删除会牺牲相关认知历史，这是现有遗忘权限的预期结果。
3. 同一 key 是单值状态槽。本次有效期计算不是多方证词裁决器，不自动解决权威性、否定、假设和多值关系。需要这些语义时应在抽取/冲突策略层明确建模。
4. 系统时间由写入端生成，并按 key 保证递增。时间倒退或多个写入拥有同一时刻时，通过微秒递增维持确定顺序；客户端不能通过有效期字段设置认知时间。
5. 每个 key 最多参与 512 条未被更正的原始断言，超限原子失败。历史版本数量会随更新增长；本次没有自动压缩或删除历史版本。该边界约束的是单次有效期计算，不是完整数据库存储预算。

行为验证在 `tests/test_bitemporal_claims.py`，同一测试契约可对 SQLite 和真实 PostgreSQL 执行。
