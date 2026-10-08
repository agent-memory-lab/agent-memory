# V7-B6 closeout engineering verification / 工程验证收尾

## Result and source boundary

The frozen integrated execution/build source is `dc39baf9a0a751a101232f72526c39486d430240` (tree `8348d9364b8c5a165a83bb8bffcc652979017561`).
The unfiltered complete repository and all five extension test directories passed:
**3931 passed / 0 skipped / 0 failed / 0 errors**, on SQLite and real PostgreSQL 17.11,
in 799.576 seconds. Both PostgreSQL DSN entrypoints were configured and strict
ResourceWarning/PytestUnraisableExceptionWarning checks stayed enabled. All six main
imports and child-interpreter imports without PYTHONPATH were verified against this
actual checkout before execution.

Every one of the prior **3783 passing nodes** was executed and passed again; only the
six formerly inapplicable SQLite variants were removed by explicit provider-specific
collection. Their six actual PostgreSQL counterparts remain executed. There are
148 new GC/A9 test cases. The old **3783 passed / 6 skipped** result remains historical
evidence in the [collection audit](validation-b6-zero-skip.json); no skip is subtracted,
counted as a pass, or waived. The complete command has no selector filter.

This completes the declared bounded opt-in engineering scope. **Full V7 acceptance,
real-model/domain quality, whole-cost benefit, default enablement and production
readiness remain unaccepted.** Software remains 0.1.0 / Alpha. All 44 inherited AM61
states, all 15 AM70 entries and frozen design bytes are preserved.

The [validation record](validation-b6.json) binds source/artifact hashes, all 98 scoped
responsibilities, exact node preservation and independent review. The [acceptance
map](acceptance-map.json) is current. Older preparation files retain historical
`not_run` fields. Reporting-only documentation commits follow the tested source;
runtime, tests, SQL and build configuration do not change. Built archives bind the recorded
source and include its documentation snapshot; later reporting summaries are checked
and scanned separately rather than relabeled as archive contents.

## Completed checks

| Check | Actual result and boundary |
| --- | --- |
| Whole repository + all package tests | 3931 passed, zero skips/failures/errors; 799.576 s |
| Prior operational/process/erase/restore slice | All 538 prior passing nodes passed within the final full run; exact-node projection, not an extra run |
| Actual pre-V7 source coordinated cutover | 2 passed on SQLite and real PostgreSQL; old capture/admission/erase/drain, isolated incompatible probe, compatible forward resume and V7 erasure/reopen |
| Build + fresh external install | Six sdists and six wheels; six distributions installed from offline cache; isolated smoke and dependency check passed |
| Installed/source equivalence | 241 packaged Python files plus 19 SQL migrations match the frozen source byte-for-byte |
| Full source + all 12 archives | Complete sensitive-information scan, zero findings; no threshold relaxation or omitted new source paths |
| Independent GC review | Approved source b5bfd24; 62 final GC cases passed on both providers with zero skips |
| Independent A9 review | Approved source ffdf476; 270 unique selected cases including 17 independent probes passed after resolving two missing-SDK-path skips; live PG is independently covered by the final full run |

No network dependency download, model download or paid inference was used.
The prior 1800.238-second non-model recovery soak belongs to source f6b6a7a:
32,688 cycles, 8,172 retired generations, 408 restarts, peak RSS 53,571,584 bytes,
FD 7 → 7. It is historical single-host lifecycle evidence, not a final-tree model/GC
soak, saturation, distributed throughput or production-scale experiment.

## Newly closed implementation work

- [Bounded reference-aware GC](retention-gc.md): explicit trusted-host policy, retention
  holds and atomic provider transactions reclaim only unreachable complete groups.
  Current/original generation, removed-block private lineage, exact receipts, active
  work, model audits and authoritative erasure/backup state remain pinned. Actual
  SQLite backup and PostgreSQL dump/restore replay are covered. No schema migration
  or runtime default changes. T14 is DONE only for the declared current-capability,
  host-retention and coordinated-cutover scope.
- GC does **not eliminate every capacity limit**. Finite receipts have no expiry or
  release contract; original delta ancestry and model audits can remain pinned
  indefinitely. Those workloads still receive bounded backpressure. There is no
  implicit receipt expiry, lineage rewrite or online old-binary support.
- [A9 tooling](a9-experiment-runner.md): frozen gold-free execution inputs, four primary
  arms and isolated ablations run actual SQLite question/refresh/model-cache paths.
  All four templates, qualifiers, expiry, late evidence, membership, revocation and
  erasure have bounded synthetic lifecycle coverage. Every phase, unknown debt,
  paired cluster bootstrap, host-observer/tariff contracts and existing real Ollama
  port binding are implemented. The supplied CLI remains offline synthetic-only;
  actual host telemetry, accepted rates/invoices and real experiment inputs are still
  required. Static hot/cold comparison does not establish learned policy calibration.
- T03–T07 statuses are reconciled to their already delivered current contracts.
  General historical questions, automatic tuning and real economic acceptance are
  not silently added as new prerequisites to those bounded engineering tasks.

Earlier B3/B4/B5/B6 current project full/delta/proof, host-only typed page patches,
immutable provider/input/output identities, exact cache and financial safety remain
covered by the full regression. Actual model calls and quality are not inferred from
recording HTTP fixtures or deterministic synthetic outputs.

## Migration and rollback

Use coordinated stop/drain/forward-resume. Stop old writers, erasers and restore
entrypoints; drain or seal obligations with compatible code; preserve independently
current deletion/finance authority; then enable compatible opt-in readers/workers.
Question runtime/registration/worker contracts use version 2; typed page generation
uses `question-page-generation/2`. The GC extension adds no SQL migration.

The [actual-source drill](validation-b6-cutover.json) uses upstream
7f824dd49b18935c6b47f5eb32aa32efdbd8cf01 (exact local snapshot 23c66ff0f4858eded2cbf79d3b6e23a50173746b).
It verifies old capture/admission/erase/drain before upgrade, unchanged deletion
checkpoint, isolated old read/claim incompatibility, compatible forward completion,
and V7 erasure followed by no-resurrection reopen. The old reader/worker rejection
is a quiescent incompatibility check, **not** a global online old-binary write fence,
a supported operational downgrade, or arbitrary mixed-version compatibility.

## Open external acceptance

AM70-T02/T12/T13/T15 remain IN_PROGRESS. Actual installed Ollama Qwen3.5 9B endpoint,
model/runtime manifest, licensed held-out project gold, independent frozen real judge,
thresholds/sample/group/statistical choices, actual host telemetry/tariffs/invoices,
and repeated quality/whole-cost observations are absent. Q7-16/Q7-30/Q7-31 remain
partial and Q7-32 insufficient_data. Every inherited acceptance state stays bounded.
The strict release report supports **bounded-engineering** after zero-skip checks,
while its aggregate release status remains **blocked** by integration/resource gates.
No deployment or publication is performed by this local verification.

## 中文结论

固定源码 `dc39baf` 的完整仓库及五个扩展包共 **3931 通过、零 skip、零失败/错误**。
先前 3783 项通过节点全部重新通过，新增 148 项 GC/A9 测试；仅移除六个不适用的
SQLite 参数，真实 PostgreSQL 对应断言均保留并执行。原六 skip 仍留作历史证据，
未计为通过或放宽发布门。六包完成构建、仓库外离线全新安装、隔离 smoke、
241 份 Python 与 19 份 SQL 逐字节比对，源码及 12 个归档扫描零发现。

T03–T07 已按现有能力完成状态对账；T14 在当前登记能力、有引用保护 GC 与停机切换
范围内 DONE。精确回执、delta 原始谱系和模型审计可无限保留并继续占满容量；
不支持在线旧二进制、任意混合写入或运行降级。A9 执行器、生命周期与消融、
配对 bootstrap、宿主观测/费率接口及真实 Ollama 接线已实现，但本次只运行合成
离线证据，没有下载模型、付费调用或部署。30 分钟旧源码 soak 未冒充最终长测。

全部 15 项 AM70、44 项 AM61 原状态和冻结设计保持完整。T02/T12/T13/T15 仍等待
许可 gold、实际 Ollama、预冻结裁判/阈值/统计方案、主机实测与费用对账及重复真实
质量/全成本对照。有界工程声明通过，整体发布门继续 blocked，软件仍为 Alpha。
