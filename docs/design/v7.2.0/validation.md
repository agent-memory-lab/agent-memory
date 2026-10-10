# v7.2 验证记录

本轮代码起点 `8fbc49f`。验证覆盖新增功能及受影响合同，不是仓库全量测试。
开发期间模块专项与集成回归有交集，不累计为一个通过总数。

| 验证 | 结果与范围 |
| --- | --- |
| 31 个受影响测试文件，SQLite + 实际 PostgreSQL | **983 passed，0 failed，0 errors，0 skipped**；246.10 秒 |
| 最后有限有效期工具修复，4 文件专项 | **130 passed，0 failed，0 errors，0 skipped**；含新增 12 项双后端案例，与上一行有交集，不叠加 |
| 新增业务政策、项目交接、演化、画像、分类政策、历史、场景和原文执行模块 | 分支计入的合并覆盖率 **83.49%**，各模块均超过 80%；不代表全仓覆盖率 |
| Ruff 新增模块、测试及示例 | 通过 |
| 开发环境 pip check / git diff --check | 通过 |
| 原文项目生命周期演示 | scene=valid、persona=active、historical_scene=ledger_rebuild；author-authored synthetic |
| 最终七包发布门 | 14 个 sdist/wheel 构建、仓库外独立安装、入口与 migration 字节验证、SQLite smoke、扫描均通过；仅 macOS 包门，不冒充模型或跨平台验收 |

最后专项修复了默认本地核验工具缺少 valid_to 字段证明的问题。
精确结束边界由权威记录证明后才能发布；更宽或无限的区间覆盖不能证明候选的精确失效时间。
该工具修复在 31 文件回归后完成，因此另外执行上表 4 文件受影响专项；两个测试范围不能相加。

定向命令、JUnit/源码指纹、包范围与合成示范摘要见 [validation.json](validation.json)。
数据库为本轮独立的、可丢弃的 PostgreSQL 集群；运行结束后关闭。

可运行工程演示：`examples/project_memory_lifecycle.py` 使用 authored finite grammar 和独立本地权威记录，
完成 raw submit、独立核验、自动问题/场景/画像刷新以及 ledger_rebuild 历史读取。
演示零模型调用、零天稳定性设置显式标记为合成示范；费用未测量，真实质量/成本不计为通过。

真实业务验收等待已认证语料、独立 gold、业务来源、模型预算、裁判/阈值和完整费率。
未执行真实业务语料或模型推理；测试中的实际宿主、队列、工具和数据库不改变合成样本类别。
未执行项目不当作支持证据；供应商副本、远端 ACL 和长期生产运行沿用前序验收边界。
