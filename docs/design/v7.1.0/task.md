# v7.1 本次交付台账

2026-10-10，revision 2；源码起点 `c5424dc`；设计见[运行时补充](../AGENT_MEMORY_DESIGN_V7.1.0.md)。
状态区分代码合同完成和外部效果验收，不把源码存在、合成测试或模型下载当生产通过。

| ID | 任务 | 代码状态 | 效果/边界 |
| --- | --- | --- | --- |
| M1 | 真实抽取/审查与预览 | CODE_DONE | Qwen 9B 已实际生成、审查及接纳合成候选；真实抽取质量待 M5 |
| M2 | 持久领域核验 | CODE_DONE | 普通/项目 supported、refuted 与 unknown；typed 条件资格；真实业务接口及权威数据待输入 |
| M3 | 精确输入 token 预算 | CODE_DONE | 最终 payload、输出预留、撤权先于 tokenizer、零预留溢出拒绝、回执；本地 Transformer 实测。Ollama 仍 byte bound |
| M4 | 可运行宿主 | CODE_DONE | 持久采集、原子授权、生成/核验、native 项目资格与共享问题刷新、停止重启；无长期生产运行背书 |
| M5 | 真实质量/成本验收 | CODE_DONE / AWAITING_INPUT | manifest、权限认证及现有四臂执行接线完成；缺获准语料、独立 gold、业务来源、校准/阈值及费率，真实运行未完成 |
| M6 | 本地真实 logits 重排 | CODE_DONE | 独立可选包、固定工件、官方 yes/no 最后位置 logits、取消/容量边界；真实推理已运行，生产晋升待 M5 |
| M7 | 历史/跨范围/L3/Reflect | CODE_DONE（声明合同） | 历史 published point、同库当前组合、host-reviewed L3 及只读两次 Reflect。通用连续历史、分布式组合/开放工具推理未实现 |

实现和验证说明见 [validation](validation.md)。旧 v7.0/AM61 全目标台账不改变，98 项总验收规格不冒充全部通过。

## 明确仍未完成

1. 真实业务语料、独立 gold、核验接口认证、裁判和费用/阈值依据未提供；不能宣称准确率/总费用降低或开启重排晋升。
2. 任意 known_at 的通用项目历史/连续覆盖，跨数据库组合、持久通用跨范围页面、开放工具 Reflect 仍是扩展目标。
3. 未证明远端 ACL 同步和供应商副本擦除；本版沿用宿主当前本地 authority 合同。
4. 未完成长期真实负载和生产部署验收；队列/画像/历史有明确有限容量，终态任务和历史保留须宿主制定处置策略。

原有本地草稿 `docs/AGENT_MEMORY_ARCHITECTURE_PLAN.md` 保持原样且不纳入提交。
