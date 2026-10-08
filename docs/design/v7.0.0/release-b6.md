# V7-B6 final acceptance runbook / 最终验收手册

This runbook now accompanies the [completed scoped engineering verification](batch-b6.md) and [final evidence](validation-b6.json), **not full V7 acceptance**.
The design remains v7.0.0; the six software distributions remain 0.1.0 / Alpha.
Do not change the 44 inherited AM61 statuses, or promote an unsupported, deferred,
unrun or statistically insufficient scenario to a pass.

## 1. Scope and remaining gates

The initial preparation source was `3dec354ed3879aa38c7dd652779bb7956861b8f1`.
The [preparation validation](validation-b6-preparation.json) binds the later
tested source `612131d833ad8aa1a2c55dfdfd399d2d0fde527e`, which includes B3
`a754ede` and its B2 heartbeat/PG initializer repairs. It records 39 focused
passes, two real-backend version-drill passes, six built/installed distributions
and a complete source/12-archive sensitive scan with zero findings. These are
preparation results, not the final full-repository or B4/B5 acceptance run.
The final closeout runtime execution/build source is `dc39baf9a0a751a101232f72526c39486d430240`; the prior 4f3e2da run and preparation snapshot above remain historical evidence.
The B2 heartbeat/completion-race repair and every later reviewed repair must be
present before final tests; a passing earlier run is not evidence for a later tree.

1. Run the complete root **and all five extension package** test directories with
   a real PostgreSQL service. Many root tests parameterize SQLite and PostgreSQL;
   absent DSN/imports silently skip those variants, so exit code zero is insufficient.
2. Rerun real independent-process crash, competition and backup replay assertions
   after B4/B5 integration. The scheduler's older crash tests alone do not prove
   durability of new model dispatch intents, caches or certificate/delta state.
3. Exercise real prior-source upgrade, stopped-process rollback probing and
   re-upgrade. Unknown-record tests are necessary but are not a real old-binary drill.
4. Build **and install** all six distributions, then run outside the checkout with
   isolated Python. An editable development environment is not installation evidence.
5. Reconcile the 98 scenario responsibilities and each supported capability scope.
   The candidate references in [evidence-plan-b6.json](evidence-plan-b6.json) are
   a review checklist. No reference or same-named test is an automatic scenario pass.
6. Update both README languages, the runtime/API capability boundaries, migration
   instructions, per-batch validation records, task ledger and acceptance map.
7. Real Q7-32 quality/cost acceptance is still blocked. Required inputs include the
   actual Ollama Qwen3.5 9B endpoint and immutable model identity, licensed held-out
   dataset/gold, frozen judge and thresholds, sample/group justification, complete
   repeated workload output, billing/resource rules and grouped paired statistics.
   Tests of an adapter or synthetic evaluator cannot replace this experiment.

## 2. Existing gates to reuse

`agent_memory.evaluation.release` already rejects missing, duplicate, failing,
skipped and unmeasured checks. Its eleven kinds are unit, contract, fault,
integration, replay, resource, SQLite, PostgreSQL, build/install, sensitive
information, and documentation/API. Keep that fail-closed behavior.

The required distribution list is:

- agent-memory
- agent-memory-evolution
- agent-memory-langgraph
- agent-memory-sdk
- agent-memory-mcp
- agent-memory-postgres

The sample release plan in `test_release_acceptance.py` lists only five; a final
release plan must include LangGraph too. That sample is a unit fixture, not the
release configuration. B6 CI now builds and clean-installs all six distributions, runs isolated smoke and scans artifacts. Its live-PG job runs all root/package test directories plus the actual prior-source drill.

Reuse `run_sensitive_information_check`, `run_documentation_consistency_check`,
the PostgreSQL `agent-memory-release-scan` CLI, `tests/test_architecture.py`, and
`tests/test_v7_acceptance_inventory.py`. Production code must continue not to
import evaluation modules. Do not silently weaken strict warning settings.

## 3. Exact final commands

Run from a clean final checkout on Python 3.13. Set both AGENT_MEMORY_TEST_POSTGRES_DSN and AGENT_MEMORY_ONTOLOGY_TEST_DSN to the same disposable PostgreSQL service through
the environment, never put their values in a committed report. The database name must
contain `test`; use only a disposable database. `pg_dump` and `psql` must match the
server major version. The local serialized PostgreSQL wrapper is environment
infrastructure, not a repository dependency or portable release command.

```sh
python3.13 -m venv .venv
. .venv/bin/activate
python -m pip install -e '.[dev]' build hatchling
python -m pip install -e packages/evolution -e packages/langgraph \
  -e packages/python-sdk -e 'packages/mcp-server[dev]' -e packages/postgres
export EVIDENCE_DIR="$(mktemp -d)"
git rev-parse HEAD > "$EVIDENCE_DIR/source-commit.txt"
git diff --exit-code
pg_dump --version
psql --version
python -m pytest -q tests packages/evolution/tests packages/langgraph/tests \
  packages/python-sdk/tests packages/mcp-server/tests packages/postgres/tests \
  --junitxml="$EVIDENCE_DIR/full-dual.xml" -ra
```

Do not unset the DSN and call missing PostgreSQL variants a SQLite acceptance
pass. A SQLite-only diagnostic run is useful, but all missing backend variants
remain explicit unrun/skip evidence. Preserve every JUnit skip's full node ID
and reason. A PostgreSQL-specific concurrency assertion skipped on SQLite may
be removed from future collection only through explicit provider-specific
parameterization that preserves its real PostgreSQL assertion and every previously
passing node. The closeout [collection audit](validation-b6-zero-skip.json) verifies
exactly this change, followed by an unfiltered zero-skip full run. Never subtract
executed skips or relabel them as passes; the strict gate does not waive them.

Keep the operational slice inspectable through exact node IDs in the final full
JUnit. Closeout preserves all 538 prior operational passing nodes in that run;
`operational-subset.json` is an explicit projection, not a separate execution.
The following narrower command is an optional diagnostic, not an extra final gate:

```sh
python -m pytest -q tests/test_durable_process_recovery.py \
  tests/test_refresh_scheduler_process.py tests/test_purge_restore.py \
  tests/test_refresh_schedule_storage.py tests/test_refresh_scheduler_clock_storage.py \
  tests/test_v7_backend_lifecycle.py tests/test_v7_subscriptions.py \
  tests/test_v7_project_indexes.py tests/test_question_runtime_v7.py \
  --junitxml="$EVIDENCE_DIR/fault-restore.xml" -ra
```

Append the final B4/B5 process-kill, actual backup/cache-erasure and uncertainty
tests to that slice after those commits exist. A simulated exception or recreated
Python object cannot be substituted for an independent killed process.
Current B5 commit `0b33d00` supplies
`tests/test_model_budget_recovery_v7.py`: three real SIGKILL phases per backend,
independently connected storage, non-release of unresolved/settled cost, and
real old-backup replay of a separately pinned current money ledger. Also include
`test_governed_models_v7.py`, `test_question_models_v7.py`, and
`test_model_cost_runtime_v7.py` after integration. The synthetic invoice in that
crash contract does not establish actual provider acceptance or Q7-32 quality.

### Actual-source upgrade / rollback probe

Materialize the repository's pinned pre-V7 implementation into an empty temporary
directory. In the preparation checkout, local commit
`23c66ff0f4858eded2cbf79d3b6e23a50173746b` is the exact snapshot of upstream
`7f824dd49b18935c6b47f5eb32aa32efdbd8cf01`. Use the upstream SHA in a normal clone;
record whichever exact source object was actually used. Never substitute HEAD.

```sh
export AGENT_MEMORY_LEGACY_SOURCE="$(mktemp -d)"
git archive 7f824dd49b18935c6b47f5eb32aa32efdbd8cf01 \
  | tar -x -C "$AGENT_MEMORY_LEGACY_SOURCE"
python -m pytest -q tests/operational/v7_upgrade_drill.py \
  --junitxml="$EVIDENCE_DIR/version-drill.xml" -ra
```

This explicitly selected operational file is not part of default collection.
Missing prior source or PostgreSQL fails the drill instead of producing a skip.
It seeds a real old database in an independent process, exercises old capture,
admission and source erasure, drains the old refresh queue, closes it, initializes
the new repository, preserves the old exact receipt, body and deletion checkpoint, publishes a V7
question and coverage receipt, stops current connections, probes the old reader
and worker with another V7 responsibility still pending, then reopens current
code, verifies unchanged V7 records and finishes that pending responsibility.
Compatible current code then erases the new question's source and reopens again;
both old and new erased sources stay absent, deletion checkpoints and scrubbed V7
records remain unchanged, the unrelated old source remains present, and the
question still rejects as erased.

**Observed rollback boundary:** the pinned old reader rejects the V7 question
as `derived_definition_configuration_changed`; the old exact-status reader
rejects the coverage receipt as `derived_target_unavailable`. The old worker
cannot parse the new context and raises `KeyError('query')` before claiming.
This exception is deliberately preserved as incompatibility evidence. It is
not a successful operational downgrade or a stable supported old-worker API.

Do not run old writers against an opted-in V7 database. First stop new entry
points and workers, drain/seal obligations with compatible code, keep current
deletion barriers, and use a compatible forward fix or separately verified
legacy-only environment. This drill does not claim a global old-binary fence,
online mixed-version writing, arbitrary historical rollback, or permission to
restore content without replaying the pinned current deletion journal.

The coordinated stop/drain/forward-resume sequence is an operator requirement:

1. Stop external capture, admission, permission, erase/restore and model-dispatch
   entry points, as well as refresh claims. Drain old active leases with their
   compatible worker or record an explicit responsibility handoff. Confirm all
   old processes have exited and released database connections before enabling
   any new record writer. A drained queue alone does not prove writers are stopped.
2. Pin the current deletion journal/checkpoint and financial obligations outside
   any content backup. Initialize the current schema with entry points still
   stopped; complete guarded index backfill and check its conservative fallback.
3. Enable only compatible readers, writers, erasers, restore operators and workers
   for the opted-in scopes. The deployment supervisor must prevent an old binary
   from targeting those scopes. This codebase does not supply a global binary
   fence; the isolated old read/claim rejection probe is not that fence.
4. If the rollout must stop, close the new entry points, finish or seal finite
   responsibilities with compatible code, retain deletion/clock/finance floors,
   and use a compatible forward fix. Do not resume legacy write, erase or restore
   entry points against the V7 scope. Reopen with compatible code and verify
   receipt continuity, pending-work recovery and erasure before restoring traffic.

The extended drill verifies the sequential old-write/old-erase cutover and the
compatible forward-erasure/reopen boundaries on both real backends. It does not
claim concurrent old/new writer safety, an enforced deployment supervisor, an
operational downgrade, or restoration of a stale backup without purge replay.

### Build and clean artifact installation

```sh
export DIST_DIR="$EVIDENCE_DIR/distributions"
mkdir "$DIST_DIR"
for package in . packages/evolution packages/langgraph packages/python-sdk \
  packages/mcp-server packages/postgres; do
  python -m build --no-isolation --outdir "$DIST_DIR" "$package"
done
export REPOSITORY="$PWD"
export INSTALL_DIR="$EVIDENCE_DIR/installed"
python3.13 -m venv "$INSTALL_DIR"
"$INSTALL_DIR/bin/python" -m pip install "$DIST_DIR"/*.whl
"$INSTALL_DIR/bin/python" -m pip check
cd "$EVIDENCE_DIR"
"$INSTALL_DIR/bin/python" -I "$REPOSITORY/tests/operational/v7_installed_smoke.py" \
  --source-root "$REPOSITORY" > installed-smoke.json
cd "$REPOSITORY"
agent-memory-release-scan --json --fail-on-violation . "$DIST_DIR"
```

The smoke checks all six installed versions and callable entry points, rejects
editable/source-path imports, compares every packaged PostgreSQL SQL migration
byte-for-byte with the source, and runs a real SQLite remember/recall round trip.
The regular build command builds the wheel from the sdist, so missing sdist input
files also fail. Keep artifact SHA-256s and the dependency inventory in evidence.
This smoke is not a substitute for all installed-service integration tests.

Preparation found three scanner false positives in inherited prose/test fixtures:
an ordinary bearer-auth phrase and two deliberately synthetic sensitive
test strings. The prose now says "authorization token"; the test strings are
assembled from adjacent expression operands, preserving their exact runtime
values and rejection assertions. No scanner pattern, threshold or path coverage
is weakened. The positive sensitive-scanner unit tests must also pass, and all
archives must be rebuilt and scanned again after this cleanup.

Optional operational resource and tokenizer runs already exist at
`tests/operational/recovery_soak.py` and `tokenizer_offline_acceptance.py`.
Their scope and measured duration must be stated. Do not relabel a short smoke
as saturation, long-duration soak, actual Qwen tokenization, or a model quality gate.

## 4. Evidence reconciliation rules

For every command retain exact source commit **and dirty-tree/file hashes when
applicable**, Python/provider/tool versions, exact arguments without credentials,
start/end times, exit status, raw JUnit/log artifact hashes, pass/failure/error/skip
counts, and every exception/non-applicable reason. Recheck all affected behavior
after conflict resolution or later edits. Source-path candidates are not evidence.

For each of 98 rows retain its owner, original inherited state and final scoped
result. A pass requires the applicable positive, negative, concurrent/recovery
assertions on actual supported backends. Mark partial when any necessary slice
is absent. Generic historical project questions, unsupported semantic operations,
and absent real-data experiments remain separately visible. No AM61 task becomes
DONE merely because a new bounded AM70 slice passed.

The final machine-readable release report and the scenario map must agree.
An operational/package pass with Q7-32 blocked can support an explicitly bounded,
default-off experimental slice, but cannot be labeled full V7 acceptance,
production benefit, default-enabled quality, or complete AM70-T13/T15.

## 5. 中文操作边界与结论

本文件是 B6 最终执行手册和准备工作，不是 V7 已整体验收的结论。最终必须在
B2 心跳/发布竞争修复、B3、B4、B5 全部整合后的固定提交上，执行完整仓库和五个
扩展包测试；真实 PostgreSQL 缺失造成的 skip 不能算通过。44 项旧任务状态保持不变。

现有测试已提供可复用的真实进程 SIGKILL、双连接/进程竞争、SQLite backup 与
PostgreSQL pg_dump 恢复、删除日志重放、索引回填和未知 schema 拒绝。最终 B6 已对
B4/B5 新证书、缓存、派发 intent、费用未决责任完成相应恢复验证；各次原始计数仍分开记录。

新增版本演练使用真正的旧源码子进程创建旧数据库，再由当前版本迁移和生成问题视图。
所有当前进程关闭后，旧读取器拒绝新问题/coverage 回执；旧 worker 在读取新 context
时抛出 KeyError，未领取任务，当前版本重新打开后新记录不变。这个结果证明不兼容被
阻断，不证明旧版本能够正常降级运行。升级/回滚仍要求停旧写入器、排空或封存责任，
保留删除屏障；不支持在线混合版本写入，也不能通过恢复旧备份复活已删除内容。

六个包必须先构建 sdist/wheel，再在仓库外的新虚拟环境安装。隔离模式 smoke 检查
实际导入位置、版本、所有 entry point、打包迁移文件和 SQLite 写读闭环。editable
安装和只构建不安装均不能替代该证据。正式运行需保存提交/文件指纹、命令、JUnit、
失败与 skip 原因、包文件哈希和依赖版本。中英文 README、能力说明和验收台账同步更新。

Q7-32 仍明确受真实 Ollama Qwen3.5 9B 配置、许可留出 gold、裁判、预冻结阈值与
统计协议阻塞。合成测试、零模型调用或前台调用减少都不能证明真实质量不退化及全成本收益。
98 项候选测试位置见 evidence-plan-b6.json，其中所有“最终验证”仍保留准备阶段的 not_run；最终运行已另写 validation-b6.json 与 batch-b6.md，不能将历史计划字段当成当前未执行或直接改成通过。

## 6. Final execution record

See [batch-b6.md](batch-b6.md) and [validation-b6.json](validation-b6.json) for source-bound final engineering evidence. The frozen preparation map is unchanged; the current [acceptance map](acceptance-map.json) resolves implemented slices without hiding external gaps. The strict no-skip bounded-engineering claim now passes. Overall release acceptance remains blocked by real Q7-32 integration and whole-cost resource requirements. Actual-source cutover is coordinated stop/drain/forward-resume only; bounded host GC does not expire exact receipts or rewrite delta/model-audit lineage.
