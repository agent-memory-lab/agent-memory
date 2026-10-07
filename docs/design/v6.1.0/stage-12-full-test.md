# 第十二阶段全量测试补充记录

版本 AM61-ST12-FULL-VALIDATION / 1.0，2026-10-07。
被测代码提交：`9d4201cc9303702f463143b381d3c33fbe17a571`。
用户明确要求全量测试，本记录补充此前仅专项测试的 [阶段记录](stage-12.md)，不覆盖旧证据。
机器可读结果与各测试文件计数见 [validation-stage-12-full.json](validation-stage-12-full.json)。

## 结果

| 检查 | 实际结果 |
| --- | --- |
| 全量 pytest：核心库 + 五个扩展包 | 1667 通过，0 失败，0 错误，0 跳过；127 个测试文件，73.52 秒 |
| 核心 / Evolution / LangGraph / SDK / MCP / PostgreSQL 包 | 1578 / 19 / 1 / 13 / 19 / 37 项 |
| 真实存储 | SQLite 与隔离 PostgreSQL 17；包含真实 SIGKILL、备份恢复、独立连接竞争 |
| 六个发行包构建 | 全部 sdist + wheel 成功；211 个源码/迁移/类型标记文件与 wheel 内容逐项一致 |
| 安装产物验证 | 六个 wheel 安装到独立目录；所有模块路径验证通过，89 项扩展包测试全部通过，无跳过 |
| 安装产物运行 | SQLite Observation 示例及 PostgreSQL 重复初始化、空发布、完成证明和读取通过 |
| 恢复循环 | 60 秒、并发 4，2096 次循环、524 次分区退休、26 次重启；文件描述符保持 7 |
| 离线 tokenizer | OS 禁网下安装本地 wheel、两种编码两次新进程、缺依赖和缺缓存拒绝均通过 |
| 依赖一致性 | pip check 通过 |

安装后重复执行的 89 项属于上述全量用例子集，不累计成更多唯一测试。
全量命令：`python -m pytest tests packages -ra --junitxml=<report>`。
设置 `AGENT_MEMORY_TEST_POSTGRES_DSN` 与 `AGENT_MEMORY_ONTOLOGY_TEST_DSN` 指向新建的临时测试数据库，补齐 tiktoken 0.14.0，避免环境缺失造成跳过。

离线验证第一次在联网词表准备环节遇到 requests 下载超时。随后用 curl 与已有缓存取得词表，核对 tiktoken 内置的两个官方 SHA256 后预置缓存；原操作脚本的禁网安装及所有正反例探针通过。此失败发生在准备环境，没有修改业务代码或降低断言。

## 验证边界

本次无需修复业务代码。README 中的两项用户编辑及架构草稿原样保留，未纳入提交。
测试覆盖当前已启用合同；未来条件 facet、通用权限/历史、L2/L3 等未实现能力不因此变成已完成。
单 macOS arm64 / Python 3.13 的结果不能代替全部部署平台；60 秒恢复循环不替代长期压力验证；合成与确定性夹具不构成生产抽取质量认证。
