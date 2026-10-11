# 统一本地记忆框架对比协议

本协议实际调用 Agent Memory、Mem0 OSS、Graphiti 和本机 Ollama，比较共同的原文写入、
检索、更正、历史问答、拒答及整轮删除能力。它是可重复的工程诊断，不是排行榜，也不是
LongMemEval 全集或 v7.2 项目核验、L2/L3 演化的验收。

## 固定的实验条件

| 项目 | 配置 |
|---|---|
| 输入 | `examples/fixtures/unified_memory_pilot_v2.json`，4 条初始原文、2 条更正 |
| 评分 | 初始 5 问、更新后 6 问，每个框架每轮 11 问；另有 6 次删除后检索 |
| 重复 | 默认 3 轮，依次轮换 Agent→Mem0→Graphiti、Mem0→Graphiti→Agent、Graphiti→Agent→Mem0 |
| 生成 | 本地 `qwen3.5:9b`，temperature=0、thinking=false、seed=42、num_ctx=32768 |
| 嵌入 | 本地 `nomic-embed-text`，768 维；当前 Agent 检索使用 query/document 用途前缀及 `truncate=False` |
| 共享回答 | 同一提示、JSON schema、最多 256 个生成 token |
| 回答证据 | 每臂原生检索前 8 条，同一顺序保留及 8192 UTF-8 字节截断规则 |
| Agent 内部检索 | 当前按 `(8192 - 3 × limit) // 4` 换算，8 条时为2042估算 tokens；保留最终共享字节检查 |
| 抽取预算 | Agent 最多 8 条、1536 token；Mem0 原生逻辑、2048 token；Graphiti 最多 8 实体/关系，实体等调用 8192、关系调用 16384 token |
| Graphiti 历史 | 原生图保留全部输入，抽取仅附带最近 2 个 episode |
| 操作截止 | 默认 180 秒；超时后等待原调用真正结束，清理并中止，不继续下一臂 |

字节预算不是精确 token 预算。各框架的原生提示、内部查询、生成上限和存储不同，
因此应称为“共享输入、生成模型和最终回答协议”，不能称为所有内部算法与资源完全一致。
Graphiti 是开源图引擎，Mem0 是 OSS SDK；本结果不能代表 Zep Cloud 或 Mem0 托管平台。

此表描述当前入口。2026-10-10 的冻结短测使用8192估算 token的 Agent 内部预算，
也没有后续 typed embedding 前缀及检索修复。该历史配置、成绩与原清单保持不变；
当前代码执行的是新协议，须使用新输出目录，不能将旧分数作为当前版本的实测结果。

## 数据与隔离

数据只包含合成名字、偏好、数据库版本、搬迁和计划会面。三个适配器都接收相同的
`RawInteraction.body()`，包括时间、角色和原文。写入与检索不接收评分标签，
共享回答器只看问题及各臂检索出的证据。评分别名只在离线判分器使用。

每轮使用新的身份及临时 SQLite、Qdrant、FalkorDB 文件。Mem0 的历史 SQLite 也明确
绑定临时目录；退出时关闭它的历史、向量与模型连接。清理失败单独记录。
Ollama 请求通过仅允许 loopback 上游的记录代理，禁用流式响应；代理等待在途请求
完成入账后才汇总。删除验证检查所有计划问题，不以部分空结果判为通过。
这些检查证明 API 可见状态，不证明磁盘、备份擦除或三个系统具备相同的逐来源删除能力。

## 评分与成本

正向问题要求回答状态为 `answered`，且包含每组必需别名。仅当前值问题出现旧值则失败。
拒答必须明确 `unknown` 并给出短拒答，不能通过在编造信息后附加“未知”得分。
所有计划问题进入分母，失败、超时和缺失不能从准确率分母删除。
字符串规则仍可能误判日期、否定表达或数字，因此应保存原回答并单独人工审阅；
自动分与人工复核结果分开保存，不能看结果后修改评分规则并覆盖原报告。
若复核由本助手执行，应明确标为助手复核，不能冒称人工、双盲或独立模型裁判。

`model_calls.jsonl` 记录每次 HTTP 请求的实验轮次、框架、操作、路径、模型、实际生成配置、
上游状态、耗时及服务器返回的 token 用量，不记录模型请求正文。报告区分 chat、embedding
和元数据请求，进一步按写入、检索、共享回答及清理归属。HTTP 失败和 SDK 重试也入账。
没有返回的 usage 为 `null`，不补零；嵌入输出 token 通常不适用。
服务端报告用量不等于电费、模型总能耗或美元价格，尤其不能由嵌入 API 的零计数推断没有成本。

框架工作流时间包含写入、所有检索、共享回答、删除与清理，**不包含共同预热及后端初始化**。
初始化单独记录，整次实验的 `total_seconds` 包含这些开销。
若发生废弃预检或重跑，应额外保留其调用记录，不能把它们混入有效质量分数，
也不能在实验总开销中悄悄省略。

## 复现

```sh
uv venv --python python3.13 /tmp/unified-memory-benchmark
uv pip install --python /tmp/unified-memory-benchmark/bin/python \
  -r examples/ollama-memory-comparison-requirements.txt -e .
/tmp/unified-memory-benchmark/bin/python examples/unified_memory_benchmark.py \
  --output-dir ../acceptance-reports/unified-memory-new-run --repeats 3
```

输出目录必须是新的，禁止隐式续跑混合协议。`report.json` 包含原始答案、检索证据、操作耗时、
失败、计划分母及模型账本汇总；`fixture.json` 冻结评分数据。
manifest 记录代码提交、执行脚本与提示哈希、数据哈希、所有 Python 包版本、Ollama 版本、
模型 digest、平台和实验配置。应同时保留实际环境的依赖锁文件。
Graphiti SDK 0.30.2 的关系抽取显式传入 16384，而初始化默认配置为 8192；
应以账本中的实际 `generation_settings.num_predict` 分布为准，不能只读初始化参数。
局部测试可运行：

```sh
.venv/bin/python -m pytest tests/test_memory_benchmark_protocol.py \
  tests/test_memory_benchmark_support.py tests/test_comparison_adapters.py -q
```

## 本轮范围与后续扩展

2026-10-10 的有效结果已保存在执行者本地 `.ecc/benchmarks/unified-memory-v72-qwen35-20261010/`，不随仓库发布；
对应代码基线 `6dbb4f8`，执行脚本改动由 manifest 哈希记录。
三次重复有助于发现不稳定性和顺序影响，不能证明总体性能或统计显著性。

本机 Hindsight 尚未部署，标为 `not_deployed`，没有失败分或零分。
加入它需要独立 API、PostgreSQL 向量后端、retain 完成等待、recall 适配器，以及后台整理
调用的完整记录；应采用共享回答器，不能把额外 Reflect 推理混成同等 recall。
部署要求可核对 [Hindsight 固定版本依赖](https://github.com/vectorize-io/hindsight/blob/v0.10.3/hindsight-api-slim/pyproject.toml)
与[官方配置](https://hindsight.vectorize.io/developer/configuration)。

更大规模评测使用 [LongMemEval 官方协议](https://github.com/xiaowu0162/LongMemEval)及本项目
[长会话运行说明](LONGMEMEVAL_COMPARISON.md)。现有六题三方历史试验总耗时约 15.5 小时，
不应将这轮合成短测的成绩冒称为完整 LongMemEval 成绩。

完整500题×三框架的1500项任务使用[完整LongMemEval-S本地运行说明](FULL_LONGMEMEVAL_LOCAL_RUN.md)，
入口为 `examples/longmemeval_full_comparison.py`，使用任务账本恢复并固定计划分母。
本轮合成短测已完成的原始成绩、助手复核、模型用量与废弃预检开销，见本机归档
`.ecc/benchmarks/unified-memory-v72-qwen35-20261010/REPORT.md`；其成绩与完整长测独立。
