# v7.1 验证记录

验证区分工程合同、实际模型运行和真实业务效果。源码基线 `c5424dc`；本版新增功能默认关闭或显式宿主装配。
本文件在最终运行完成后登记实际计数和指纹，不累计前序成绩。

## 已取得的证据

- SQLite/PostgreSQL 专项：生成/审查、模型输入授权、溢出零金融预留、跨轮擦除、核验租约与提交原子性、unknown/refuted、条件字段资格、native 项目接入、宿主停止重启、历史点/SDK/MCP 交付、当前跨范围授权、L3 支持/反例/Atom 撤回和只读 Reflect。
- 实际本地 Qwen3.5:9B：授权模型生成、语义审查、预览与 durable host 接纳作者编写的语言偏好。
- 实际本地 Qwen3-Reranker-0.6B：最后位置 yes/no logits、上下文溢出拒绝、实际本地 chat 渲染/生成 token 回执相符；合成文本不代表业务准确率。
- 七个包的 sdist/wheel 构建；最终安装、扫描与全量结果在完成后补登记。

## 运行边界

首次全量曾因测试期间源码改变，冻结实验拒绝后续执行；独立复测 37 项通过。中止过一次尚未完成的全量以补 tokenization 的权限守卫。
这些尝试保留为运行历史，不计为最终全量通过。最终执行已冻结 Python/SQL 源码。

本机 PostgreSQL 未安装 pgvector，其单项实际扩展验证不能计为通过；GitHub CI 使用带 pgvector 的 PostgreSQL 17。
真实获准业务数据、独立 gold、业务核验接口、校准/费率未提供，故没有真实业务质量或总成本收益结论。
发布点历史不覆盖任意 system-time 缺口；当前组合不实现跨数据库事务；Reflect 不开放工具或自动回写。

## 最终结果

| 验证 | 实际结果 |
| --- | --- |
| 仓库核心与既有五个扩展包，SQLite + 实际 PostgreSQL | 4572 passed，0 failed，1 skipped（本机缺 pgvector） |
| 新模块专项，实际双后端 | 63 passed，0 skipped；已包含在全量中，不叠加 |
| 可选本地模型包，实际批准权重 | 4 passed，0 skipped；含真实 logits、溢出拒绝与精确 token 回执 |
| 七包 sdist/wheel | 全部构建成功 |
| 独立 venv 六包完整安装、pip check 和既有 installed smoke | 通过；后者只证明其声明的 SQLite/六包范围 |
| 第七可选包的独立 wheel 安装与惰性 import | 通过；独立 venv 的该包用 no-deps 仅测惰性导入，实际推理依赖在本轮开发 runtime 验证 |
| 当前开发 runtime pip check | 通过 |
| 来源及 build 根扫描 | 749 files、26 archives，0 findings、0 violations，无降低阈值 |

全量命令：

```sh
AGENT_MEMORY_TEST_POSTGRES_DSN=postgresql://memory@127.0.0.1:55671/agent_memory_v71_test \
AGENT_MEMORY_ONTOLOGY_TEST_DSN=postgresql://memory@127.0.0.1:55671/agent_memory_v71_test \
python -m pytest -q tests packages/evolution/tests packages/langgraph/tests \
  packages/python-sdk/tests packages/mcp-server/tests packages/postgres/tests \
  --junitxml=/tmp/agent-memory-v71-full-frozen.xml -ra
```

本轮全量执行 4573 项，跳过不算通过。测试数据库为隔离、可丢弃的 PostgreSQL 17 集群，验证后关闭。
新功能源码逐文件 SHA、JUnit 指纹及独立范围见 [validation.json](validation.json)。
实际模型记录：[Qwen 9B 抽取宿主](model-host-synthetic.json)、[Qwen3 logits](reranker-synthetic.json)。
前者预览及发布共四次有账本的真实模型生成/审查，后者用两个作者编写的片段计算真实 logit margin；
它们不证明真实业务质量、裁判校准或费用下降。没有源码/权重下载被当作效果成绩。

本轮保留的用户草稿 SHA-256：`81095de8aa02db4f59a643d0d6708b4fa9ee415bfb3f230bab4a7abf962fe849`，不提交。
