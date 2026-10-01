# LongMemEval-S 沙箱对比

此流程使用 LongMemEval 官方清理版 `longmemeval_s_cleaned.json`，MIT 许可。数据集有 500 道问题；每道题的历史独立隔离，并按原顺序逐会话写入本地 Agent Memory、Mem0 OSS 和 Graphiti。记忆抽取、回答、评判使用相同的 Ollama `qwen3.5:9b`，向量嵌入使用 `nomic-embed-text`。

三方的 Qwen 调用统一设置 `num_ctx=32768`。Graphiti 的抽取提示包含历史实体，默认 4096 上下文会截断 JSON 输出。更改上下文、提示词或抽取参数后，要使用新的结果和工作目录，避免续跑时混合协议。

Graphiti 适配器在本地 Qwen 上将每个写入片段的实体和关系列表限制为最多 8 条，并只附带最近 2 个原始片段作为抽取上下文。完整历史仍保存在 Graphiti 图中，可供检索。这个资源预算是本地复现实验的配置，应与模型、分块长度一起在报告中披露。

本地 Agent Memory 的对比适配器按问题检索，并从相关记忆、事件和流程记忆中按分数取前 8 条；它不把未排序的全局当前状态塞满回答上下文。三方回答阶段均只看各自检索出的最多 8 条记忆。

评判提示与官方 `evaluate_qa.py` 的任务规则对应，尤其保留知识更新、时间推理、偏好和拒答规则；评判模型换成本地 Qwen，因此分数是 **LongMemEval-S 数据集上的本地复现**，不等于论文中使用 GPT-4o 裁判得到的官方分数。每道题保存单独结果，崩溃后会跳过已完成项并重跑未完成项。模型、提示词、依赖或索引方案变化时，请使用新的输出目录和工作目录。

## 获取数据

从[官方清理数据集](https://huggingface.co/datasets/xiaowu0162/longmemeval-cleaned)下载 `longmemeval_s_cleaned.json`（约 277 MB），不要使用已弃用的原版数据集。当前工作区的文件已放在 `../benchmark-data/longmemeval_s_cleaned.json`，SHA-256 见同目录的 `SOURCE.md`。

```sh
mkdir -p ../benchmark-data
curl -L --fail \
  'https://hf-mirror.com/datasets/xiaowu0162/longmemeval-cleaned/resolve/main/longmemeval_s_cleaned.json' \
  -o ../benchmark-data/longmemeval_s_cleaned.json
shasum -a 256 ../benchmark-data/longmemeval_s_cleaned.json
```

## 完整评测

需要 Python 3.13、Ollama 服务和已安装的 `qwen3.5:9b`、`nomic-embed-text` 模型。在 `agent-memory` 项目目录创建隔离环境并运行：

```sh
python3.13 -m venv /tmp/longmemeval-comparison-venv
/tmp/longmemeval-comparison-venv/bin/pip install -r examples/ollama-memory-comparison-requirements.txt
/tmp/longmemeval-comparison-venv/bin/pip install -e .
/tmp/longmemeval-comparison-venv/bin/python examples/longmemeval_ollama_comparison.py \
  --data ../benchmark-data/longmemeval_s_cleaned.json \
  --output-dir ../acceptance-reports/longmemeval-s-qwen35-9b-ctx32768-g8-p2-rank \
  --work-dir /tmp/longmemeval-s-qwen35-9b-ctx32768-g8-p2-rank
```

用 `--offset N --limit K` 可运行连续的一段题目并保持相同命名空间续跑。完整 500 题为三个系统各自处理数千个长会话，耗时会较长；分段运行时要使用相同的 `--output-dir` 和 `--work-dir`，并确保分段范围不重叠。

2026-09-27 的第 166 题初次诊断中，本地 Agent Memory 用时约 18 分钟，Mem0 OSS 约 11 分钟。Graphiti 在默认 4096 上下文下发生 JSON 截断；改为 32768 后，超过 20 分钟仍未处理到第 10/45 个会话，试跑主动中断。这是旧配置的诊断，不应与新配置的结果混算。

2026-09-29 在本页所示 `ctx32768-g8-p2-rank` 配置下，同一题三方完整完成：Agent Memory 约 19 分钟、Mem0 OSS 约 49 分钟、Graphiti 约 93 分钟。Graphiti 没有再发生 JSON 解析失败；单题裁判分别为错、对、错。完整试跑见 `../acceptance-reports/longmemeval-s-qwen35-9b-ctx32768-g8-p2-rank/REPORT.md`。这一题只验证运行路径，不能作为 500 题基准成绩。

结果目录含每个系统的逐题 JSONL（答案、Qwen 裁判标签、检索上下文与耗时）和汇总 JSON（总准确率、各题型准确率、数据集 SHA-256）。检索提示只包含问题，不使用金标准答案、证据会话 ID 或 `has_answer` 标签；这些标记只用于离线裁判或报告。沙箱中关闭项目默认邮箱遮盖，以让三个后端接收完全相同的公开基准原文。该设置仅适用于公开合成基准，不能用于真实用户数据。

来源：LongMemEval ICLR 2025，[官方代码与协议](https://github.com/xiaowu0162/LongMemEval)，[论文](https://arxiv.org/abs/2410.10813)。
