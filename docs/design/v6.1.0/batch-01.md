# v6.1.0 第一批实施记录：评测基础与采集来源

日期：2026-10-06。执行台账 revision 2。基线为 `7a382d9` 加本批未提交工作区差异。
规范是固定 [设计 v6.1.0](../AGENT_MEMORY_DESIGN_V6.1.0.md)，差距盘点见 [implementation-audit.md](implementation-audit.md)。

## 1. 交付内容

| 切片 | 文件 | 已实现行为 |
| --- | --- | --- |
| T02/T03/T34 基础证据合同 | `src/agent_memory/evaluation/evidence.py` | 不可变 gold 对象、精确来源修订/span/hash、字段覆盖、双时态查询资格、当前 readable 标注、来源族独立性、AND 分支与 OR 替代支持 |
| T05/T39 质量门 | `src/agent_memory/evaluation/acceptance.py` | 冻结数据/配置/基线/profile、样本量、绝对底线、相对退化、答复覆盖、系统失败、成本/延迟；全拒答即使阈值为零也阻断 |
| T03/T38 合成种子 | `tests/fixtures/memory_v61/` | 调研 17 个事件、20 个检查的固定适配；可信宿主角色、来源修订、源文件 hash 和独立检查坐标 |
| T38 可执行评分入口 | `tools/evaluate_memory_evidence.py` | 读取已导出的 evidence observations/独立裁判，生成 JSON 报告；可接固定 profile 与 baseline |
| T06 宿主采集切片 | `capture/profile.py`、`capture/api.py` | 宿主/版本/角色/工具结果/省略声明，capture_complete 三值，来源族与父 revision/receipt；无宿主全集时覆盖率未知 |
| T06 回流接纳限制 | `lifecycle.py`、`kernel.py`、`consolidation/{admission_runtime,atom_extraction}.py` | 已知回流及改写仅作上下文；不触发旧提取器/自动整理，不允许直接 Atom 接纳或调用新提取模型；新的独立确认仍可接纳 |
| T06 来源审计/重试 | 同上 | 有界 capture 注记在 Atom 最终事务保存；来源修订纳入显式/自动接纳幂等指纹；变更来源不能命中旧成功回执 |

评分代码放在现有 evaluation 子系统；capture 合同归 capture；接纳限制复用既有三条入口。
没有新增运行服务、平行队列或数据库迁移；运行时不依赖 evaluation 模块。

## 2. 宿主接入示例

在可信宿主适配器里调用；`profile`、`observation`、`scope` 与 `actor` 不从模型工具参数构造。

```python
from datetime import UTC, datetime
from agent_memory.capture.api import submit_profiled_capture
from agent_memory.capture.profile import CaptureProfile, HostCaptureObservation
from agent_memory.capture.policy import CaptureSanitizer
from agent_memory.capture.sink import DirectCaptureSink
from agent_memory.lifecycle import LifecycleEvent

# kernel、scope 来自已初始化的宿主；生产宿主应提供稳定的源事件 ID。
when = datetime(2026, 10, 6, tzinfo=UTC)
profile = CaptureProfile(
    profile_id="chat-host", version="1", host="my-agent", host_version="1",
    event_types=("message.received",), origins=("user",),
    content_types=("text",), sanitization_policy="builtin-redaction-v1",
)
event = LifecycleEvent(scope, "evt-1", "message.received", "user", when,
                       "run-1", content="以后用中文回复。")
observation = HostCaptureObservation(
    host_event_id="evt-1", message_id="msg-1", source_revision_id="rev-1",
    source_family="source-1", event_type="message.received", origin="user",
    occurred_at=when, content_types=("text",), capture_complete=True,
)
receipt = await submit_profiled_capture(
    event.to_dict(), sink=DirectCaptureSink(kernel, CaptureSanitizer()),
    scope=scope, actor="authenticated-host", profile=profile, observation=observation,
)
```

需要已有持久 outbox 时改用 `QueuedCaptureSink(SQLiteCaptureQueue(...))`。`pending` 表示队列已接收，
`done` 表示本轮 Provider ingest 完成；均不表示新设计的抽取/发布/索引/视图各阶段已就绪。

重要的合同边界：

- 本批支持 text/tool_result。其他内容类型拒绝；有序多块、媒体 locator、请求/响应执行关联在后续 T06/T41。
- `capture_complete=None` 表示无法判断，False 配合明确 omissions；True 只针对宿主声明的合同。
  宿主负责诚实声明清洗策略，sink 仍执行原有清洗/大小限制；profile 名称本身不证明策略被执行。
- `memory_injection=True` 需要 parent revision 与 receipt；已知改写只要携带 parent_revision_ids 也会受到限制。
  父对象存在性、当前权限、递归 processing 依赖和源族规范化仍由后续 T25 实现，本批不自动加信。
- 正文的角色标签、source family 数量或 assistant claim 不能授予 SourceAuthority。
  注记只用于审计/限制；接纳权限仍由宿主单独提供的 SourceAuthority 与 AdmissionPolicy 决定。
- 采用不含正文或个人信息的稳定 opaque IDs；注记和事件仍受清洗和存储大小上限约束。
- 原来的 SDK/MCP capture 不能通过 payload 写入保留 `_agent_memory_capture`。
  新 profile 当前只开放宿主 Python API；SDK/MCP 专用 adapter 合同尚未完成。
- 该切片不改变旧自动抽取的“模型执行后最终保存”边界。不要把 Direct/Queued 入口称为 T16 原子 retain。

## 3. 评分器与冻结门使用

```sh
.venv/bin/python tools/evaluate_memory_evidence.py \
  --dataset tests/fixtures/memory_v61/aurora_dataset.json \
  --observations tests/fixtures/memory_v61/scorer_observations.json \
  --run-configuration tests/fixtures/memory_v61/scorer_configuration.json \
  --output /tmp/agent-memory-v61-scorer.json
```

该命令验证评分器，不执行记忆系统或模型。随库 observations 是人工正确输出，不能据此报告实际准确率。
真实实验需替换 observations，并固定运行配置里的实现版本、模型/裁判版本、提示词/索引/预算和数据分割。
CLI 对配置原始字节做 SHA-256；dataset/report/profile 采用规范 JSON 指纹。修改任一固定输入需要重新评测。

`AcceptanceProfile` 的 baseline_report_sha256、dataset_sha256、candidate_configuration_sha256、
minimum_cases、minimum_answerable_cases、QualityThresholds 和 calibration_reference 必须由评测负责人设定。
初始阈值可以为 None，但门禁必定阻断；本批不编造生产数值。调用时传入此前独立保存的 profile fingerprint：

```sh
# 以下三个路径/指纹由已冻结的真实实验产出提供。
.venv/bin/python tools/evaluate_memory_evidence.py \
  --dataset /path/to/dataset.json --observations /path/to/observations.json \
  --run-configuration /path/to/configuration.json \
  --profile /path/to/profile.json --baseline /path/to/baseline.json \
  --expected-profile-sha256 FROZEN_PROFILE_SHA256 \
  --output /path/to/report.json
```

退出码：0 评分成功且可选质量门通过；1 质量门阻断；2 输入错误。没有 profile 的 0 不表示发布通过。
报告始终 `production_ready=false`；`QualityGateResult.release_outcome()` 仅生成现有发布框架的一项 REPLAY 检查，
不能替代真实 PostgreSQL、运行安全、恢复、构建等其他发布检查。

评分口径：

- `(A AND B) OR C` 返回 C 或 A+B 均完整，只有 A 为半覆盖；不把 OR 展平成所有来源都必需。
- 分支所有证据须在同一 valid_at/known_at 合格；不同区间不会合并填平间隙；来源族最小数量可限制独立佐证。
- 相同文档的其他 span 不算命中；无权/已擦除/不合时点 evidence 不进应召回集合，实际输出则计 forbidden hit。
- `answer_correct` 来自独立裁判；充分证据不自动证明答案正确。未执行问题计 system_error 并保留分母。
- Q20 不计语义准确率，但计成本、未知引用、泄漏和诊断失败。诊断项不能掩盖答复覆盖下降。
- gold 的 readable/scope/fields 是可信离线标注，评分器不执行实时 ACL 或自然语言事实核验。
- quality gate 信任实验产出的 report/observations；hash 防止冻结后意外变更，不是防伪签名或运行证明。

## 4. 数据适配与未覆盖范围

数据版本 `aurora_dataset.json` 的规范指纹：
`5d8052dfa5a49ee9ec003a4469c1cc548b13714fd98721e58d357eafeea76bea`。
17 个事件 / 20 个检查，其中 19 个语义检查、17 个可回答；Q20 单列诊断。

第 15 步成本更正保留旧认知，Q11/Q12 使用不同 known_at；第 16 步撤回原公告，获准转述可存活但仍属相同源族；
第 17 步严格擦除令 Q15 不可回答。以上是 gold 预期，尚未通过真实运行时所有派生/缓存/历史出口端到端验证。
study 明确 `end_to_end_executed=false`。来源许可仅按用户提供的合成教学材料使用，不宣称覆盖真实私有领域数据。

## 5. 验证记录

具体命令、结果及构建指纹写入 [validation-batch-01.json](validation-batch-01.json)。
验证覆盖新评分合同、profile 清洗队列持久化、回流接纳限制、SQLite/真实 PostgreSQL，以及既有核心/各集成包回归。
测试生成器均为确定性桩；没有调用实际模型或声称 Hindsight 比较效果。

环境排错记录：首次临时 PostgreSQL DSN 无 URI authority，被既有测试 fixture 的 urlsplit/urlunsplit 重写成
无效 `postgresql:/...`；改用带 localhost authority、Unix socket host 参数的隔离 DSN 后通过。
首次无隔离 wheel 构建缺 hatchling；采用标准隔离构建成功。均未修改运行时数据库连接逻辑或用户数据库。

## 6. 任务与后续边界

T01 审计 DONE；T02/T03/T04/T05/T06/T34/T38/T39 为 IN_PROGRESS，记录的是已通过切片，完整任务不勾选。
余下任务 TODO。M0 尚缺领域样本/协议/竞态/阈值闭合；M1 尚缺生成前持久化和相关运行安全合同。
下一批按审计结论推进 T02 最小来源/任务合同和 T15/T16，随后接入可靠调度；本批可独立复用的宿主 API 与离线评分器已有测试。

回退本批前停止 profiled/context 事件的接纳与重放；保留已保存 L0，避免旧代码把上下文重新当来源。
纯离线评分器可直接停止使用；本批没有需要逆向执行的数据库迁移。
