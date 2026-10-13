# LongMemEval 检索诊断与快照复放

这是一套本地公开数据集诊断入口，只支持 `--arms agent-memory`。默认评测仍从原文开始抽取，并在任务结束清理隔离数据库。只有显式传入 `--retain-agent-snapshots` 才会保留入库快照。它不是生产数据备份或导出 API。

## 受控实验

第一轮使用完整原始会话、固定抽取模型与参数，只跑指定问题，并保留一次完整入库。以下占位路径需要换成当前的冻结运行环境、新输出目录和官方清理版数据文件：

```sh
PYTHONHASHSEED=42 PYTHONNOUSERSITE=1 PYTHONDONTWRITEBYTECODE=1 \
  /new/run/.venv/bin/python /new/run/runner_snapshot/longmemeval_full_comparison.py \
  --data /benchmark-data/longmemeval_s_cleaned.json \
  --output-dir /new/run \
  --arms agent-memory \
  --question-ids 51a45a95 58bf7951 6ade9755 \
  --retain-agent-snapshots --timeout 900
```

在另一个新输出目录和冻结代码环境里复放同一批快照：

```sh
PYTHONHASHSEED=42 PYTHONNOUSERSITE=1 PYTHONDONTWRITEBYTECODE=1 \
  /new/replay/.venv/bin/python /new/replay/runner_snapshot/longmemeval_full_comparison.py \
  --data /benchmark-data/longmemeval_s_cleaned.json \
  --output-dir /new/replay \
  --arms agent-memory \
  --question-ids 51a45a95 58bf7951 6ade9755 \
  --replay-agent-snapshots /new/run/agent_snapshots --timeout 900
```

复放跳过抽取，仍执行真实检索、相同的共享回答与本地判分。先保持 `ANSWER_PROMPT`、模型、参数、问题及证据预算不变，只更换检索实现。若再修改阅读器提示词，应单独创建另一轮复放；不要把阅读器改进算成记忆检索改进。

## 独立验证阅读协议

默认 `--reader-mode direct` 保留原直接回答协议。验证带引用的结构化阅读时，在新的输出目录使用同一冻结运行环境，并追加：

```sh
--reader-mode structured --allow-contextual-inference \
--reader-thinking --reader-output-tokens 8192 --max-attempts 1
```

这四项阅读选项分别控制输出协议、是否允许有明确标签的上下文推断、是否请求思考，以及生成上限；不是检索算法参数，也不修改入库政策。抽取和裁判仍使用公共配置并关闭思考。`--reader-thinking`、`--reader-output-tokens` 和推断选项只允许用于 structured；未显式启用思考的客户端收到 `think=True` 时会报错，避免清单与实际请求不一致。

输出上限必须为 1–32768 的整数；省略时非思考为 384、思考为 2048。8192 是本轮本地诊断配置，不保证所有模型或问题都需要该额度。`--timeout` 还需容纳实际生成时间。阅读器默认禁止上下文推断；显式允许后，推断答案必须保留该分类、至少两条不同的精确引文及限定措辞，不会回写成权威记忆。精确引文只证明文本存在，不证明推理正确。

`manifest.answer_reader` 绑定 prompt/schema 摘要、策略和请求的思考/输出配置；公共 `settings` 是抽取与裁判基线，不能单独据此判断答案阶段配置。每题 `answer_reader.generation` 只记录停止原因、服务报告 token 数和最终正文是否为空，不记录推理文本。`output_budget_exhausted`、`generation_incomplete`、真正的 `invalid_json`、合法拒答和裁判错误分开记录；失败不自动换提示或退回另一种回答。诊断使用 `--max-attempts 1` 固定每题一次。

Ollama 0.32.14 的 Qwen3.5 `thinking + schema` 路径可能内部生成两次；非流式结果只保留末段 token 计数。账本排空只证明 HTTP 请求均已归档，不证明完整推理成本已被计量。必须同时保留耗时、请求数及该统计限制，不能把末段的低输出 token 数当作思考很便宜的证据。此模式源码依据和实际对照见 [本轮回归摘要](evaluation/LONGMEMEVAL_V15_REGRESSION.md#cost)。

该 CLI 校验整个 500 题原始数据文件的 SHA，再筛选题目；不会按答案或金标会话选择入库内容。快照摘要只绑定 question ID、原始会话与日期，不包含金标答案、金标会话 ID 或问题文本。这样可以在相同入库内容上测试不同查询，而金标仍只进入最终判分。

## 快照与清理语义

- 每次完整入库在 `agent_snapshots/<question-id-hash>/attempt-N/` 生成完整 SQLite 和删除日志的 SQLite backup；WAL 中已提交的数据也会包含。
- 清单记录源数据散列、入库内容散列、宿主作用域、文件散列、源 core/runner 散列与模型配置、入库计数和费用、表行数及 claim 状态/版本计数。
- 同一快照目录禁止覆盖。复放选用每题最新的完整入库 attempt，并把所选路径与清单散列固定到新运行的 manifest；输入、文件或清单发生变化会拒绝继续。
- 快照恢复到新临时目录，始终通过 `UnifiedMemory.recall`；作用域、权限、有效性、待删除日志等治理仍然生效。原快照不被初始化、迁移或修改。
- 原评测 sandbox 的 `clear` 和资源 `close` 仍在 `finally` 执行，快照失败也不会跳过清理。显式保留的快照是独立公开数据集副本，**不会随 sandbox 清理而删除**；诊断结束需要删除对应 `agent_snapshots` 目录。不要把这个功能用于生产用户记忆。
- 复放输出明确标记 `ingestion_mode=snapshot_replay`。本轮 `ingested_events=0`，历史入库统计在 `snapshot_replay.source_ingestion` 中。复放账本只计入本轮检索、回答和判分，不能据此宣称完整总成本下降。

## 可观察结果

每条最终可见证据包含 `id/text/kind/score`、有界 `lineage` 和精确 `citation_spans`。`retrieval_diagnostics` 保留 kernel 的候选、排序、打包计数与有限候选 ID/类型/分数，禁止把任意 provider metadata 或未治理候选正文直接导出。

`retrieval_diagnostics.answer_evidence` 另外记录共享 UTF-8 证据预算下的实际字节数、每项是否完整、被丢弃的项目数。这样可以区分“未进入候选”“候选排序/打包丢失”“下游字节预算截断”“证据已充分但阅读器仍回答错误”。

错题子集是事后回归，不能替代全量 LongMemEval 准确率；本地 Qwen 判分也不是官方判分。旧实验代码和结果保持冻结，不回写新诊断或更改旧分数。
