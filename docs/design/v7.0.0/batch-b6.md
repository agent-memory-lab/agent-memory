# V7-B6 final engineering verification / 工程验证汇总

## Result and source boundary

The fixed integrated execution source is `4f3e2da77450e5250bbf1ca9551d81cefc1d1a6c` (tree `750636647c4da1567b9dbb5150765b08f5c6e54b`).
The complete repository and all five extension test directories completed with
**3783 passed / 6 skipped / 0 failed** on SQLite and real PostgreSQL 17.11. The six skips are explicitly
PostgreSQL-only assertions instantiated for SQLite; each exact PostgreSQL counterpart
passed. They remain skips in the raw evidence and the strict release gate. No skip
was subtracted, counted as a pass, or silently waived.

This verifies the bounded, opt-in engineering implementation. **Full V7 acceptance,
real-model/domain quality, whole-cost benefit, default enablement and production
readiness remain unaccepted.** Software remains 0.1.0 / Alpha. All 44 inherited
AM61 states and the frozen V7 design bytes are unchanged.

The [validation record](validation-b6.json) binds commands, source/artifact hashes,
all 98 scoped responsibilities, every skip and its actual counterpart. The
[acceptance map](acceptance-map.json) is the current ledger. The older
[evidence plan](evidence-plan-b6.json) and [preparation report](validation-b6-preparation.json)
remain historical planning records; their `not_run` fields are not the final result.
Documentation/evidence commits follow the execution source and do not change runtime
code. Final documentation consistency checks are separately recorded.

## Completed checks

| Check | Actual result and limitation |
| --- | --- |
| Whole repository + all package tests | 3783 passed, 6 explicit skips, 0 failures/errors; 689.702 s |
| Focused process/erase/restore/security integration | 538 passed, 6 explicit skips, 0 failures/errors; 260.637 s |
| Actual pre-V7 source upgrade/rollback/re-upgrade | 2 passed on SQLite and real PostgreSQL; stopped writers, incompatible old-binary probe |
| Build + fresh external install | All six distributions, six sdists and six wheels; offline dependency cache, isolated Python smoke and dependency check |
| Installed/source equivalence | 236 packaged Python files plus 19 separately smoke-verified SQL migrations matched source byte-for-byte |
| Source + all 12 distribution archives | Complete sensitive-information scan, zero findings, no excluded new paths or lowered threshold |
| Resource history | Existing 1800.238 s non-model recovery soak: 32,688 cycles, 8,172 retired generations, 408 restarts, peak RSS 53,571,584 bytes, FD 7 → 7 |

The soak belongs to `f6b6a7a904cbd6c90cb51a700fa655c680a17315`, before later security/page
changes. It is reused only as historical single-host recovery-lifecycle evidence,
not relabeled as a final-tree model/page soak, saturation, distributed throughput or
production scale test. The final dual-provider tests cover the later changed paths.
Build setup initially encountered an unwritable default uv cache and an incomplete
alternate cache; using the existing complete offline cache resolved installation.
No dependency download, model download or paid inference was required.

## Integrated implementation and independent review

- B3 current project registration/full/route/read and B4 deterministic complete-group
  delta, full fallback and immutable generation/current certificate separation.
- B4 cached-body fix rechecks controls around each individual body await. The restart
  clock regression now waits for observed durable coverage rather than assuming a
  PostgreSQL transaction finishes inside 100 ms; artificial 150 ms provider delay and
  twenty independent repetitions preserve the same safety assertions.
- B5 immutable model recipient/configuration, sealed complete input lineage, final
  serialized output binding, exact cache identity and atomic financial identity fences.
  Recording local HTTP fixtures test exact submitted bytes and unknown cost; they do
  not execute Ollama inference or establish actual provider/model quality.
- [Typed page patches](typed-page-patches.md): host-only append/insert/replace/remove,
  page/certificate/block version CAS, mixed original parent generations, exact retained
  block revisions, before/after every body guard, atomic erase and real backup replay.
  Independent review ran 138 compatibility tests and a 26-case expanded adversarial
  suite, including nine distinct per-body permission/context/authority races.
- New question process tests kill independent interpreters before/after full, delta and
  proof publication, then recover fixed responsibilities with no duplicate publication.
  B5 tests independently kill at intent/settlement/recording HTTP receipt boundaries.

## Migration and rollback

Stop old writers, drain or seal obligations using compatible code, then enable the
new opt-in readers/workers. Keep the current authoritative erasure/finance checkpoints.
Question runtime/registration/worker contracts use version 2. Typed pages add internal
`question-page-generation/2` without a new SQL migration; compatible readers must be
upgraded together. Disable the optional page surface before any incompatible rollback.

The actual-source drill uses upstream `7f824dd49b18935c6b47f5eb32aa32efdbd8cf01`
(local exact snapshot `23c66ff0f4858eded2cbf79d3b6e23a50173746b`). Old question reads
reject as `derived_definition_configuration_changed`; old coverage reads reject as
`derived_target_unavailable`; the old worker raises `KeyError('query')` before claiming.
Current code reopens unchanged V7 records and completes the pending responsibility.
This is evidence of incompatibility under a stopped-process probe, not a supported
operational downgrade, online mixed-version write fence or arbitrary rollback guarantee.

## Open external acceptance gates

The actual Ollama Qwen3.5 9B host/base URL, installed model identity/runtime, licensed
held-out project gold, frozen judge/thresholds/sample/group/statistical protocol, and
repeated quality/whole-cost observations have not been supplied. No real model was
executed or downloaded; no paid API was called. Unknown local cost remains unknown.
AM70-T02/T12/T13/T15 and the wider inherited milestones are not marked complete.
Q7-16 remains partial, Q7-30 retains its bounded rollback limitation, Q7-31 lacks the
real workload reconciliation, and Q7-32 is `insufficient_data`. General historical
project pages, free-form model patches, streaming output and declassification stay
unsupported; a test of rejection is not implementation of those capabilities.

## 中文结论

最终固定源码 `4f3e2da` 的完整仓库与五个扩展包运行结果为 3783 通过、
6 明确 skip、零失败/错误；skip 仅为 SQLite 参数下的 PostgreSQL 专属断言，
对应真实 PostgreSQL 断言均通过，未从统计中抹去。另有 538 项运维/擦除/恢复
切片通过和两项真实旧源码升级演练。六包完成构建、仓库外全新环境安装、隔离 smoke、
236 份打包 Python 源码与 19 份 SQL 迁移文件比对及零发现扫描。

B5 模型治理/精确缓存/预算合同与 Q7-23 宿主确定性类型化页面补丁已集成；合成 HTTP
录制端点不是真实 Ollama 推理。30 分钟恢复 soak 沿用明确标注的旧源码证据，未冒充
最终源码长测。旧进程探测确认不兼容，不宣称在线混合版本或安全运行降级。

98 项责任均有当前有界结果与限制；保留 44 项 AM61 原状态及冻结设计。真实 Ollama
9B 配置、许可 gold、裁判/阈值/统计方案和重复全成本对照仍缺失，因此没有完整 V7、
生产质量/收益或默认启用声明。详见 [机器可读验证](validation-b6.json)。
