# v7.3 验证记录

日期：2026-10-22（Asia/Shanghai）。
基线：`5849c26`；分支 `codex/evidence-driven-memory-v73`，本记录对应本分支的 v7.3 实现。
环境：Python 3.13.5 / pytest 8.3.4 / SQLite；核心无第三方运行时依赖。
范围：新增能力及受影响模块的局部联合回归，不是全仓库测试或公开基准准确率。

## 1. 最终联合测试

结果：**480 项收集，388 passed，92 skipped，0 failed；7.05 秒，退出码 0。**
跳过包括 87 项未配置 `AGENT_MEMORY_TEST_POSTGRES_DSN` 的真实 PostgreSQL 测试，
以及 5 项未安装可选 `tiktoken` 的 tokenizer 测试；不把跳过计为通过。
早期分组测试存在重叠，本报告不把它们与最终联合结果相加。

在仓库根目录执行：

```bash
PYTHONPATH=src:packages/evolution/src:packages/postgres/src \
  /opt/anaconda3/bin/python3 -m pytest -o addopts='' -q -ra \
  --junitxml=/tmp/agent-memory-v73-targeted.xml \
  tests/test_architecture.py \
  tests/test_qa_optimization.py tests/test_public_qa_acceptance.py \
  tests/test_recoverable_compression.py tests/test_recovery_context_acceptance.py \
  tests/test_recovery_cancel_feedback.py tests/test_strategy_partition_acceptance.py \
  tests/test_recovery_specialized_acceptance.py tests/test_recovery_operations_acceptance.py \
  tests/test_evidence_research.py tests/test_evidence_research_integration.py \
  tests/test_evidence_research_reader.py tests/test_policy_decision_observations.py \
  tests/test_refresh_host_policy.py tests/test_feedback_storage_contract.py \
  tests/test_final_delivery_guard.py tests/test_pipeline_delivery_guard.py \
  tests/test_delivery_evidence_binding.py tests/test_ranked_source_context.py \
  tests/test_governed_recall_integration.py tests/test_native_evidence_delivery.py \
  tests/test_packing_versions.py packages/evolution/tests
```

关键行为覆盖：

- 来源失效、在途激活、撤回失败、重启和旧 schema 迁移；跨包删除及逻辑擦除。
- 独立语义候选无词面门槛、补查预算、无进展停止、超时与取消、最终来源变化。
- 枚举/空集合凭证的新增成员与删除失效，时间精度不足，注册视图拒绝降级。
- Planner 输入实际证据计入预留，失败调用保持未知用量；严格 JSON reader。
- 压缩原文恢复、跨实例状态改变、摘要被替换、策略切换、归档与擦除。
- 决策来源治理、旧反馈幂等兼容、未知成本不当零、冷热策略 CAS 与入队。
- 冻结计划、实际运行 manifest、分组配对、错误弃答、成本与治理验收门控。
- 既有架构依赖边界、最终交付守卫、默认打包协议与恢复流程。

## 2. 交叉检查中修复的问题

1. Planner 传入了证据正文，但原预算仅包含合同与阅读结果：已补全部证据预留，超预算不派发。
2. 决策记录原可绕过来源治理：写入和读取都检查原生来源可用性，拒绝未知、跨范围和已删除引用。
3. 缺少模型调用清点时可能把空费用列表视作零：默认保持未知，显式调用数须绑定所有费用回执。
4. 原文恢复/压缩验证期间另一实例更新状态可能交付旧内容：慢回调后在恢复库快照核对完整摘要和状态版本。
5. 配对运行只绑定计划哈希，无法核对实际模型/评分器/数据：增加运行 manifest，逐字段比对。
6. 删除只停用程序而仍保留敏感载荷：增加持久待擦除责任，停用成功后清除正文、来源片段及评估自由文本。

## 3. 可执行离线示例

以下均实际执行，退出码 0；未调用付费模型。

| 示例 | 本次观察 | 原始结果 |
| --- | --- | --- |
| `examples/evidence_research_sqlite.py` | 实际 SQLite 检索，两轮阅读、5 次内部调用，返回两条受治理证据；语义蕴含仍未独立证明 | [research-demo.json](research-demo.json) |
| `examples/evidence_research_comparison.py` | 同一冻结合成两例：单轮 0/2、有界补查 2/2；计时仅覆盖 answer()；成本与本地资源未知，`accepted=false` | [qa-comparison-offline.json](qa-comparison-offline.json) |
| `packages/evolution/examples/offline_skill_lifecycle.py` | 三步骤配置修复，执行 baseline 与 candidate；走晋升、使用、来源擦除、停用拒用和正文清理 | [skill-lifecycle-demo.json](skill-lifecycle-demo.json) |

```bash
PYTHONPATH=src python examples/evidence_research_sqlite.py
PYTHONPATH=src python examples/evidence_research_comparison.py
PYTHONPATH=src:packages/evolution/src python packages/evolution/examples/offline_skill_lifecycle.py
```

这些是确定性开发夹具。两题配对结果及 Skill 的 30/50/100 次同类配置回放都不代表真实模型、
独立留出集或通用任务收益。配对示例补查增加了调用和延迟，本轮没有证明总成本下降。

## 4. 静态检查与兼容性

- 本轮新增 Python 文件和修改的 evolution Python 文件，共 31 个文件，Ruff 检查通过。
- `git diff --check` 通过。
- 扩大 Ruff 至未修改的 evolution 文件时，`generator.py`、`training.py`、
  `test_training_bridge.py` 仍有 10 条既有导入排序/行长问题；未混入无关格式修改。
- 默认 packing 与旧非恢复压缩入口保持兼容，新研究循环和可恢复压缩显式启用。
- 可选 evolution 包的 `invalidate_sources` 现在必须传 `scope`；外部 registry 实现需补撤回/擦除端口。
- 自定义 RecoveryStore 启用原文恢复需提供 `compression_current`；旧非恢复摘要保留兼容路径。

## 5. 尚未完成的外部验收

- 真实 PostgreSQL 与可选 tokenizer 环境检查仍待执行。
- 独立向量路由是有界 SQLite 参考适配；没有生产持久向量索引或 PostgreSQL 研究循环。
- Census 只证明注册 event_type 已收录行集合，不证明现实世界完整；通用模型语义支持仍可能出错。
- 多库部署和跨控制器并发需要宿主 writer/delivery fence，不宣称跨库原子事务。
- 费用回执和资源测量由可信宿主提供；协议无法证明宿主未遗漏外部调用。
- 完整 LongMemEval-S、STATE-Bench 与真实业务 LIFE-07 未运行，既有 QA-07/08/09 状态保持不变。
- 用户累计 API 上限仍为 20 元，原记录余额 0.0888379 元；本轮付费调用 0，原账本、协议、断点与成绩未重置。
