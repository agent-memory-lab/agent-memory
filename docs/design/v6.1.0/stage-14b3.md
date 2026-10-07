# 第十四阶段 B.3：历史资格上下文

版本 AM61-ST14-B.3 / 1.0；2026-10-07；DONE（有界语言组合）；台账 revision 22。
设计保持冻结 v6.1.0；实现基线 `0d63c4b8bfa8390cb58ccf757765e59760e87688`。
方案见 [stage-14-plan.md](stage-14-plan.md)，任务见 [stage-14-tasks.md](stage-14-tasks.md)，机器证据见 [validation-stage-14b3.json](validation-stage-14b3.json)。
原架构文档的未提交修改保持字节不变，不纳入本轮提交。

## 交付行为

服务与定义显式启用 `published-point/1` 或 `published-interval/1`，绑定 query、authority 和当前宿主 context_token，
可以读取历史 `locale-context/1`。SDK/MCP 请求只提供 known_at/valid_at，不能注入过去的属性、政策或资格。
归档冻结当时的可信 principal/scope/subject/purpose、项目/节假日路由属性、时区、snapshot token、
ProjectionPolicy、完整候选及 contextual-qualification/1 的条件、例外、字段支持和来源证据。
定义代次/摘要和候选行版本进入原不可变发布证明，不读取最新资格去解释过去。

上下文的认知期限约束 known_at；valid_at 独立选择事实有效时间，可以早于上下文登记或晚于其到期。
例如 K1 的项目 A 路由可以投影较早的偏好，K2 换到项目 B 不会改变 K1 的条件结果。
这是按已登记路由参数解释事实，不证明“过去现实中的项目/节假日”状态。
weekday 按 requested valid_at 和冻结时区求值；未知时区、未知属性和例外仍保留三值结果。

旧上下文今天已经到期，不抹去到期前已发布的历史；它也不能用于今天的新处理。
当前调用方仍须匹配当前定义的宿主 token、scope 和用途，并通过今天的 authority/floor 和全部实际来源 grant。
换到新 token 的宿主可以读取受权限保护的旧归档；旧宿主 token 不匹配时在正文之前拒绝。

## 覆盖、政策与字段证据

发布事务复核实际上下文，并保存上下文摘要及认知起止证明；只记录 metadata，不在点证明里复制路由属性。
连续区间右端取首次语义变化与旧上下文到期的最早边界。
没有写入也不能覆盖到期后的 known_at；后续变更或新上下文不能延长旧区间。
到期与下次完整发布之间仍是缺口，不回填迁移前状态，不回退当前摘要。

历史合成使用冻结 AdmissionPolicy 与 ProjectionPolicy，并核对每个资格的 target/source/policy 摘要和字段支持。
政策升级后，旧认知保持旧政策；尚未按新政策核验的候选不能借用旧资格发布新结果。
AND/OR 区间、证据间隙、point 支持及有效时间独立投影；普通来源撤回关闭当前覆盖，保留未擦除的旧认知。
高优先范围的争议、未知或支持失效不能回退成低优先的确定事实。
缺失或损坏历史政策/上下文/资格证明时拒绝，不套用今天的政策。

## 架构与安全边界

- `derived/model.py`：FacetContext 的可信路由、历史双时间与认知期限合同。
- `derived/history.py`：原发布事务内的上下文证明、冻结版本完整性、历史读取及当前授权先行。
- `derived/coverage.py`：在原稳定区间合同中纳入上下文到期上限；保留首次变化、时钟前沿和输入摘要检查。
- `derived/contextual.py` 与 `observation.py`：纯投影，复用同一资格/条件/字段支持算法，不负责存储或授权。
- 原 SQLite/PostgreSQL UoW、scope 锁、租约、CAS、完成证书和共享擦除计划继续使用，无新增表、队列或强制依赖。

撤销未引用的辅助字段来源也会阻断历史读取；授权先于归档 L1 和证据正文。
对象/范围擦除和真实备份回放清除资格归档、manifest、上下文证明及全部受影响点/区间。
独立于备份的当前 authority floor 继续阻断旧权限快照。历史 expiry 不替代当前授权 expiry。
SDK/MCP 最终两次读取保持相同双时间；期间上下文到期或切换可继续读取旧认知，期间撤权则拒绝交付。

## 接入与兼容

新示例：`python examples/derived_context_history.py`（已安装 core 与 SDK）。
原 `derived_contextual_observation.py` 默认仍是当前条件示例；历史行为需要显式 `history=True`。
capabilities 仅在宿主历史/context opt-in 时声明历史 locale-context/1 和 `historical_context=frozen-host-route/1`。
没有新增 SDK/MCP 写接口或请求属性；历史扩展不会自动启用既有定义。
原非条件点/区间序列化保持原义，旧当前条件输出不增加历史投影字段。

部署前升级全部写入/发布进程，沿用 `write-hooks/1` 和统一可信 UTC 域；不支持绕过 UoW 或旧/新写入混跑。
既有 context 定义切换历史需要宿主正常 CAS 重新登记；缺旧发布版本不回填。
旧二进制不支持条件历史，回退先停用相关服务，再由受信宿主显式登记受支持的当前定义，不能借回退绕过权限/擦除。
容量继续每 facet 128 点、scope 4096 点、归档 256 KiB，达到上限原子回滚。

## 验证与后续

981 个唯一新增及受影响用例通过，0 failed、0 skipped；未执行全量测试。
新增历史条件行为 136 项，SQLite/真实 PostgreSQL 17 各 68 项；包含两种 mode 的资格发布和区间上下文切换
提交前后共 12 项真实 SIGKILL，以及独立连接上下文/发布竞争、当前权限、实际撤回和 backup/pg_dump 删除回放。
新 core/PostgreSQL wheel 安装后另通过 124 项专项与六个示例；12 项 SIGKILL 已在源代码真实进程验证，重复成绩不计入唯一总数。
两个包的 sdist/wheel 源码和类型标记共 172 文件逐字节匹配测试版本，所有本轮 Python 修改通过 ruff。
SDK/MCP 无生产代码变更；合成协议用例不代替生产抽取质量或远端 ACL 证据。

B01–B04 的本阶段有界语言组合已验收；完整 T28/M2、一般谓词、跨存储范围、实体政策注册及传递父仍未声明完成。
下一步 C01–C03：固定实际派生父 manifest/版本、传递处理许可交集、循环/深度/容量、父变更失效、物理擦除及恢复。
派生父验收前 capability 保持 false；随后才推进 L2 Scenario/版本化页面 full rebuild。
