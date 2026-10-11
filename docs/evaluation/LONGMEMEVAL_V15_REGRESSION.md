# LongMemEval 三个历史错题回归摘要

版本：1.0。日期：2026-10-12。对应检索实现 v1.5。

同一批入库快照上，默认 direct 回答通过本地裁判 **1/3**，显式启用
structured thinking、上下文推断和 8192 生成上限后为 **3/3**。
两组实际交付证据逐字相同，每题各执行一次，无失败重试。第三题同时回答了
计划中的居家练习，仍有语义精度问题；3/3 只是本地裁判通过数。

这是三个历史错题的事后回归，不是完整 500 题成绩、官方 GPT-4o 判分、独立
事实核验或多框架排名。阅读模式与生成预算同时变化时，不能把改善单独归因于检索或 thinking。

## 协议与来源

使用官方清理版 LongMemEval-S，问题 ID 为 `51a45a95`、`58bf7951`、`6ade9755`。
数据文件 SHA-256 为
`d6f21ea9d60a0d56f34a05b609c79c88a451d2ae03597821ea3d5a9678c3a442`。
入库使用完整原始会话，后续快照复放不重新抽取。金标不进入抽取、检索、
上下文扩展或回答提示，只用于末端裁判与事后分析。

回答及裁判为本地 `qwen3.5:9b`，embedding 为 `nomic-embed-text`，
Ollama 为 0.32.14。v1.5 冻结 core 内容摘要为
`07db3390c8c1ae1749110c926c868c466fca7354801ba08c3ec5d071fe18eff4`。
这是该历史运行的内容摘要，不代表后来提交或工作区的字节摘要。

默认回答保持 direct；新阅读模式必须显式设置：

```sh
--reader-mode structured --reader-thinking --reader-output-tokens 8192 \
--allow-contextual-inference --max-attempts 1
```

仅答案阶段启用 thinking，抽取与裁判仍关闭。运行入口和快照治理见
[复放说明](../LONGMEMEVAL_RETRIEVAL_REPLAY.md)，算法和数据流见
[检索设计](../design/retrieval/native-contextual-retrieval.md)。

<a id="historical-runs"></a>

## 历史结果

| 冻结运行 | 优惠券地点 | 剧目名称 | 瑜伽场馆 | 本地裁判通过 |
|---|---|---|---|---:|
| 旧 core 同快照 | 错 | 错 | 错 | 0/3 |
| v1 direct，生成快照的原入库运行 | 错 | 对 | 对 | 2/3 |
| v1.3 direct 同快照 | 错 | 对 | 错 | 1/3 |
| v1.4 direct 同快照 | 错 | 对 | 错 | 1/3 |
| v1.4 structured，允许推断、未启用 thinking | 错 | 对 | 错 | 1/3 |
| v1.5 direct 同快照 | 错 | 对 | 错 | 1/3 |
| v1.5 structured thinking，8192 上限 | 对 | 对 | 对，含 Home | 3/3 |

各行均完成三题，每题一次。v1.1 单题通过；v1.2、v1.3 单题以及固定 v1.2
证据的 reader-only 诊断仍错误。v1.1/v1.2 的完整三题目录只是预备目录，未执行
完整三题，不能计入成绩。不得选取不同版本的最好答案拼成 3/3。

v1.4 固定证据的独立阅读诊断中，thinking 开启、上限 2048 时三题均无最终正文，
没有进入裁判；它们是机械失败，不能记为语义判错 0/3。原记录未保留停止原因，
不能确证后端故障。中性短问题在三种 thinking/schema 配置下均能返回 JSON。
后续固定同一份 v1.4 证据、thinking 开启、上限 8192 的阅读诊断三题通过，
耗时 569.912 秒；它没有重跑检索，不能替代 v1.5 快照全流程结果。

<a id="diagnostics"></a>

## 错误来源与剩余缺陷

早期测试工具遗漏抽取必填的 `confidence`，后续已修正。之后的三题回归虽记录
1768 次事实接纳，仍为 0/3，且最终证据全是 event；接纳次数不等于独立、有效或
完整的事实数量。缺少中间候选的历史结果也不能证明目标事实已经抽取。

v1.2 首题的只读词法诊断发现：48 项候选中 43 项没有四个查询主题词的字面命中，
24 个 claim 全部没有这些命中。短事实中的 `I`、`where`、`5` 仍可取得高 IDF。
这支持修复查询准入与候选预算，但字面不重合不等于语义无关；新规则也可能漏掉
同义表达，不能把主题词命中当作充分相关性证明。

瑜伽题的关键用户句位于来源字符 `[3929,4101)`。v1 交付 `[1792,4608)` 窗口；
v1.3 的来源广度分配只保留 `[0,3712)` 首窗，互补关系在 embedding 前丢失。
v1.4 的重排后扩展补回原句，两种非 thinking 阅读模式仍答错，失败已经不能只
归因于没有召回。v1.5 进一步修复同源覆盖去重、角色起点与畸形 span 防御。

<a id="answers"></a>

v1.5 structured thinking 的原始答案如下，未将答案改写成金标：

| 问题 | 实际答案 | reader 支持分类 |
|---|---|---|
| 优惠券地点 | `Based on the conversation context, Target` | `contextual_inference` |
| 剧目名称 | `The Glass Menagerie` | `explicit` |
| 瑜伽场馆 | `Home and Serenity Yoga studio` | `explicit` |

每题两条引文，六条均精确存在于交付证据。优惠券原话没有直接说明兑换门店，
Target 来自同一会话的购物上下文，必须保留推断标签，不能升级为已核验事实。
瑜伽题的居家引文上文为 `was thinking of`，把计划混入实际状态，仍有范围与
状态精度问题。本地裁判的 yes 不能消除该缺陷；reader 分类也不是独立证明。
全部保留 `entailment_verified=false`，答案不会写回权威记忆。

<a id="cost"></a>

## 成本与测量限制

| v1.5 三题快照复放 | 耗时 | 模型 API 请求 | embedding 输入 tokens | API 报告输入 / 输出 tokens |
|---|---:|---:|---:|---:|
| direct | 25.056 秒 | 9 | 65262 | 71340 / 90 |
| structured thinking | 433.690 秒 | 9 | 65262 | 86625 / 353 |

每组是三次 embedding、三次回答、三次判分 API 请求，另有两次元数据请求。
不包含历史入库、其他诊断或失败实验开销。embedding 输出用量未知。
旧 core 同快照的 embedding 输入为 22830，当前约为其 2.86 倍。

Ollama v0.32.14 的 thinking + schema 路径可能内部执行两段生成，非流式结果只
保留末段 token metrics。九次模型 API 请求不等于九次底层推理，353 不是完整
内部生成用量。请求账本结清只能证明 HTTP 请求已归档，不能证明全部内部计算
被计量。源码依据是固定版本的[两阶段生成](https://github.com/ollama/ollama/blob/v0.32.14/server/routes.go#L2746)、
[重新构造提示](https://github.com/ollama/ollama/blob/v0.32.14/server/routes.go#L2887)
及[非流式聚合](https://github.com/ollama/ollama/blob/v0.32.14/server/routes.go#L2370)。
没有测量金额或能耗，不能宣称完整成本下降；快照免重抽取是实验节省。

<a id="validation"></a>

## 验证范围

该冻结 v1.5 实验的定向测试覆盖 28 个文件：496 通过、20 个 PostgreSQL 测试因
缺少环境跳过，无失败。修改模块的定向 Ruff 通过，不代表全仓库 lint 或全量测试
通过。此记录是 2026-10-12 运行时的历史结果，提交前新检查应另外记录。

尚需独立样本与原先正确题检查退化、重新入库评估角色污染和重要事实遗漏、
严格区分计划与已发生事实的阅读验收，以及前后台完整成本统计。
独立 dense 召回、ColBERT、IRCoT 未接入默认路径，不能将其论文结果作为本项目成绩。

<a id="archive"></a>

## 原始产物保存方式

本文件是可随 Git 仓库发布的摘要。原始数据、SQLite 快照、冻结环境、逐题输出、
请求账本和测试 XML 留在实验执行者的本地 `acceptance-reports` 归档，不包含在
仓库中；这里不创建指向仓库外目录的链接，也不修改原产物及其摘要。
可按以下运行 ID 核对本地原始清单：

- `longmemeval-agent-memory-contextual-retrieval-20261011`
- `longmemeval-agent-memory-old-core-snapshot-ablation-20261011`
- `longmemeval-agent-memory-contextual-v13-replay-20261011`
- `longmemeval-agent-memory-contextual-v14-direct-20261011`
- `longmemeval-agent-memory-contextual-v14-structured-20261011`
- `longmemeval-agent-memory-contextual-v15-direct-20261012`
- `longmemeval-agent-memory-contextual-v15-structured-thinking-20261012`
- `longmemeval-agent-memory-retrieval-repair-20261012`

获得本仓库并不等于已获得这些原始运行材料；重新验证应使用新目录、固定依赖
和模型，保留新结果，不能覆盖历史归档。
