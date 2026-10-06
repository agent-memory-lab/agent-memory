# v6.1 合成评测种子

这些文件是人工金标准和评分器回归输入，**不是本项目或 Hindsight 的效果测试结果**。

- `aurora_study.json`：用户提供的 17 个事件及 20 项问题的项目适配、来源指纹、宿主身份、修订关系和检查点。
- `aurora_dataset.json`：`evidence-support/1` 精确证据片段、现实/认知坐标、权限资格和充分支持分支。
- `scorer_observations.json`：人工编写的正确输出，用于验证评分器；不能用于宣称运行时记忆效果。
- `scorer_configuration.json`：上述评分器自测的固定配置，不是生产模型配置。

来源：工作区 `Hindsight_全面调研_2026-10-06/学习案例.jsonl` 与 `验收问题.json`。
文件 SHA-256 留在 study 元数据；运行这些夹具无需原调研目录。原始材料为合成教学案例。
检查点时钟是固定测试坐标；原材料缺少删除时间的部分由夹具明确分配，不假装测量过实际接收时间。

适配约定：

1. 宿主身份和工具资格由夹具作者指定，不从材料正文或 `source_type` 提升权限。
2. 第 15 步是同逻辑文档的新修订及明确更正；旧认知保留。答案解释“120 被更正为 12”不能因含有 120 就判错。
3. 第 16 步为撤回原公告；另行采集、仍获准的转述可用，但仍属于 MAINT-01 来源族，不能充当独立佐证。
4. 第 17 步为严格擦除；Q15 不允许历史、缓存、派生文档或诊断恢复端点。当前文件给出 gold，运行时各路径仍待实施验证。
5. Q20 是预算诊断，不进入自然语言准确率分母；其费用、未知引用或泄漏仍要计入检查。
6. 案例中的 `fields` 与 `answer_correct` 是可信标注/独立裁判输出；评分器不会自行判断自然语言语义或实时访问授权。
7. 每个证据 ID 对应固定来源修订及 Unicode code point 区间。返回同一文档的无关片段不算命中。

评分器自测（仓库根目录，已安装本地包）：

```sh
.venv/bin/python tools/evaluate_memory_evidence.py \
  --dataset tests/fixtures/memory_v61/aurora_dataset.json \
  --observations tests/fixtures/memory_v61/scorer_observations.json \
  --run-configuration tests/fixtures/memory_v61/scorer_configuration.json \
  --output /tmp/agent-memory-v61-scorer.json
```

接真实适配器时需另外生成 observations，并记录模型/策略/索引/预算配置、独立答案裁判和调用成本。
缺失问题按系统错误保留在分母中。不得把上述人工 observations 当作真实适配器输出。
