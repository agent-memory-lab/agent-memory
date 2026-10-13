# 有界原文上下文召回

版本：1.5。更新：2026-10-12。本文说明 SQLite/native 路径的查询准入、父窗口、重排后上下文扩展、来源机会预算和交付实现；整体排序、独立 reader 与冻结实验记录见 [Native 上下文检索设计](native-contextual-retrieval.md)。v1.5 快照全流程回放已完成：direct 1/3，structured thinking 本地裁判 3/3；第三题仍混入计划中的居家练习，不代表语义全无问题；不含重新入库或全量成绩。

默认检索使用短子片定位，再读取同一原文的连续父窗口，将窗口交给 embedding 重排与证据打包。相比只交付独立的 1,024 字符片段，父窗口保留相邻对话的指代、说话者、地点与限定关系；它不保证任意长文完整召回。

## 定位与读取

索引子片仍为最多 1,024 个 Python 字符，重叠 128 字符，步长 896。v1.3 的 `query_content_anchors()` 先从原查询得到请求期内容词，SQL 对命中子片要求至少一个 anchor，再做 scoped BM25 的 owner 排序与截断；所有原查询词仍参与 BM25 和语义输入。英语功能词、普通纯数字、否定词不能独立准入含主题词的查询；引号专名按词项保护，例如 `"The Who"` 保留 `who`。没有 anchor 的查询保持原检索；有 anchor 时只有全部启用分支汇总为空，才统一去掉 gate，用原查询兜底一次。某个 kind 为空不会单独放宽，不靠补功能词噪声填满预算。

每个 owner 最多保留两个不同词法命中。每个命中读取最多四个连续索引子片：前两个、命中片、后一个。计算起点为 `max(0, matched_start - 2 × 896)`，再覆盖从该位置起最多四个子片的实际边界；在来源开头，缺少前文子片的容量转给后方。最大窗口为 `1024 + 3 × 896 = 3712` 个 Python 字符，来源末尾可更短，校验保护上限仍为 4,096 字符。这替代 v1–v1.2 前后各一片、最多 2,816 字符的父窗，以保留更早出现的指代依据。

每个匹配 owner 最多返回四个不同窗口；这与“一个父窗口最多四个子片”是两个独立上限。组内依次尝试首个词法命中、第二命中、源尾和邻片；相同父窗口坐标只出现一次。v1.1 引入的轮转仍然保留：SQL locator 截断先保留各 owner 的首命中，再分配第二命中；窗口生成也按 owner 惰性轮转，先每个来源一个窗口，有余量才进入下一轮。词法定位最多保留总上限两倍的 locator，仍受 `MAX_CANDIDATES` 限制；owner、正文读取次数和返回窗口数量上限不增加。父窗、源尾与邻片可不直接命中 anchor，但必须来自已合法准入的 owner。源尾探测不读取题目答案或评分标签。

在后续最终候选截断前，EVENT 分支按公开 `item.id` 进行第二次来源轮转，再与其他 kind 轮转共享名额；同源的不同 claim 不合并。两层机会预算分别防止“一个 event 的多个窗口占满名额”和“event 表示占满所有名额”，并不强制最终答案来源或 kind 的比例。代价是较弱来源首窗口可能占用最强来源额外上下文的机会，同源互补窗口只有在剩余预算允许时才能全部进入重排。v1.3 更长的父窗还会增加单候选正文与潜在 embedding 成本；最终预算不变时，可能交付更少窗口或必须缩片，不能只凭窗口更长认定质量或成本改善。

父窗口边界通过 `(source_table, source_id, span_start)` 的邻近索引头计算，在相同读取事务和来源 revision 下读取。SQLite 使用索引保存的 UTF-8 字节边界对 BLOB 切片后还原文本；公开 `source_span` 使用 Python 字符坐标，不能与字节偏移混用。Python 不加载整份长原文，也不把不相邻文本拼成连续引文。单片浏览/索引插件的默认接口不自动启用父窗口；native event 搜索显式启用。

## 重排后再读同源窗口

v1.4 在 `rerank → packing` 之间增加可选 `expand_source_context()`。`source_context.py` 定义厂商无关的 anchor 选择和上限；`sqlite_source_context.py` 负责受治理的 SQLite 定位/读取；kernel 编排并治理新增候选。未实现该可选方法的 provider 保持既有行为。历史查询或未启用 SEMANTIC channel 时不扩展。

从重排序列选最多 `min(4, query.limit)` 个独立公开 EVENT ID，只接受有限 score 和有效 source-span 身份。每个 anchor 的父窗起点分别向前、向后偏移 896 字符，按最多四个索引子片读取父窗，单窗最多 3,712 字符。已有/新增候选身份去重后，合计最多八个新窗口；边界或资格不满足时更少。

扩展使用新的 SQLite 读取事务：先修复脏索引，再复查 scope、未归档状态、source revision、来源长度及起止索引边界，随后在该事务内按 UTF-8 字节切片。重排期间来源发生变更、归档或删除时，旧 anchor 不得授权读取新版本。新增原文还要再次通过 atom 来源治理；原 ranked atoms 已走 AdmissionEngine 时态路径，不再次套用 legacy 来源门禁。

新窗继承 anchor 的融合相关性，不重新算 BM25、不调用 embedding。其 `context_window=reranked-adjacent-parent/1`、`context_only=true`、`context_reason=reranked_adjacent`，并记录 `context_anchor_candidate_id`、`context_anchor_score`、`relevance_origin=inherited_context_anchor`；不伪造自己的 `semantic_score` 或词法命中坐标。邻近关系仅提供补充证据的机会，不证明该窗与查询语义相关。

本次基准原 48 项向量重排不变，packing 输入最多 56 项；通用 kernel 若额外加入原有 atom 候选，应按 `len(ranked)+8` 描述上限。trace 增加 `source_context` 与 `packing_input`，扩展元数据记录状态、anchor/新窗数量及 `semantic_scoring=false`。额外 SQL、正文读取及 packing 有成本；扩展不保证覆盖任意远处关系，也不属于独立 dense 召回。

## 身份、引用与治理

`MemoryItem.id` 保持原始 event ID。不同窗口使用请求内 `candidate_identity=source-span:<lexical_chunk_id>`，其中 `lexical_chunk_id` 根据原 event ID、源 revision 和实际父窗口坐标重新计算；RRF 与 native packing 按该身份去重。身份哈希只是定位校验，不能证明来源授权。

每个窗口保留 `source_event_ids`、`source_owner_id`、`source_span`、`source_revision` 和 `source_chars`，并额外提供：

- `matched_source_span`：用于定位的原始子片坐标。
- `context_window=bounded-parent/2`：四子片父窗口策略标识。
- `context_anchor_chunk_id`、`context_reason`：匹配或上下文探测的来源。
- `query_anchor_policy=content-anchor/1`、`query_anchor_count`、`query_anchor_mode`：查询准入策略、数量和 `required` / `full_query_fallback` / `unrestricted` 模式；不输出原问题或 anchor 字符串。它与指向原文子片的 `context_anchor_chunk_id` 含义不同。

`Citation` 原有字段保持兼容；`retrieval_metadata.citation_spans` 逐窗记录实际交付坐标，并保留打包器跨 kind 的选择顺序。`ordered_bundle_evidence()` 仅据此排列已返回的证据；适配器不再按 `item.score` 撤销去冗余顺序，也不能用该引用增加证据。消费者不能把 items 简单转为按公开 source ID 去重的字典，否则同源互补窗口会再次丢失。

当父窗口满足 token 预算时，优先交付完整窗口。v1.4 为合法索引窗口预计算最多 13 个 variants（含原窗）；需要缩片时，只在窗口内按完整句/行边界选择连续原文，保留最小完整单元供紧预算选择。小数、首字母和常见缩写的点号做保守处理，父窗中途切边不自动算句边界；这仍是启发式，原父窗本身不保证两端句子完整。

没有标点或换行时，fallback 缩片至少 64 个字符，并保留完整 ASCII 单词/数值/标识符；超长不可拆词可以判为不适配，汉字连续窗口不声称语义完整。此下限不排除小于 64 字符的完整短来源。剩余预算放不下任何 variant 时跳过，允许少于 top-k 或剩余预算未用满，不再拼凑几个字的残片。缩片元数据 `passage_boundary` 区分 `sentence_or_line` / `unbroken_min64`。

v1.5 在可见 `user:` / `assistant:` / `tool:` 行首 turn 头存在时，将缩片起点限定到该头，保留角色及前置限定；结尾仍可落于后续完整句/行。它不认证字面角色，不恢复父窗已丢失的上文；保留起点可能导致片段在预算内不可行。另以合法字符 span 合并已选同 event ID、来源集合与 revision 的区间：若并集已连续覆盖待选窗，则选择有新增文本的合法 variant，或跳过。不同 claims、不同 revision 及畸形 span 不据此折叠；同源去重不等同于事实去重。

引用记录最终交付范围，`retrieved_*` 元数据保留原检索窗口身份与坐标。归档或删除公开 source ID 会撤销其全部窗口；窗口不成为新的权威事实或新的删除入口。现有 scope、事实准入、时间轴和来源状态检查仍然生效。

LongMemEval 的 `CommonAgentAdapter` 在检索前把 8,192 UTF-8 字节上限保守换算为 `(8192 - 3 × limit) // 4` 个 native 估计 tokens，`limit=8` 时为 2,042。依据是当前估计器保证可编码正文的 UTF-8 字节数不超过估计值四倍；同时为每条分隔符预留三字节。它防止已打包窗口在回答前被另一预算截断；最终字节检查仍保留。换算不是模型 tokenizer 计数，对非 ASCII 内容可能少用可用字节。上下文长度、实际交付完整性、成本与答案质量需要共同验收。

## 验证边界

定向回归针对内容词准入、数字/否定保留、引号专名、全分支一次兜底、源尾事实、前文指代、起始窗余量、来源/表示轮转、同源互补窗口、UTF-8 边界、跨 kind 选择顺序、字节完整性、跨 scope 与删除。v1.4 还需验证过期 anchor 拒绝、历史/channel 禁用、新窗 atom 治理、八窗上限、无虚构 semantic_score、完整句/行及无标点 fallback。以实际测试报告为准；本地 embedding stub 不检查 gold 答案，契约通过不证明真实模型问答效果。

[v1 三题完整入库实验](../../evaluation/LONGMEMEVAL_V15_REGRESSION.md#historical-runs)以及后续快照复放均保留独立冻结环境，不用当前工作区回填旧结果。完整三题中 v1 为 2/3、旧 core 同快照对照为 0/3，[v1.3 replay](../../evaluation/LONGMEMEVAL_V15_REGRESSION.md#historical-runs)为 1/3。另有独立首题诊断：v1.1 正确，v1.2、v1.3 和固定 v1.2 证据的 reader-only 均错误；v1.1/v1.2 未执行完整三题。以上都不是全量成绩。具体目录与边界见[整体设计](native-contextual-retrieval.md#验证与后续)。目录存在不表示完成，免入库复放也不能代表完整系统成本下降。诊断与公开数据集快照复放见[专门说明](../../LONGMEMEVAL_RETRIEVAL_REPLAY.md)。

[瑜伽题只读诊断](../../evaluation/LONGMEMEVAL_V15_REGRESSION.md#diagnostics)验证了本策略的取舍：v1 交付了同源 `[1792,4608)` 窗口内的关键用户自述，v1.3 的 48 项候选只保留该来源 `[0,3712)` 首窗。场馆名称与含糊 claim 虽仍交付，支持用户行为的互补窗口已在语义重排前丢失；最终无字节截断也不能证明关系证据完整。来源广度、同源互补性和阅读器所问粒度仍需进一步验收，不能把多模块变化单独归因于 embedding 前缀。

v1.4 的重排后扩展已在冻结实验中补回瑜伽题关键用户句，两种回答协议均完整收到 8,165 字节、无下游截断，但仍未答对；此时不能继续把失败归结为该句未交付。三题 direct 与 structured 的证据摘要逐题相同，两组均为 1/3，只第二题正确；[回归摘要](../../evaluation/LONGMEMEVAL_V15_REGRESSION.md#cost)记录历史结果、逐题证据问题及成本边界，原始产物另存本地，后续 v1.5 防御与 packing 修复另列，不回填冻结结果。

结构化阅读器 `examples/memory_evidence_reader.py` 是独立回答协议：区分 `explicit`、`contextual_inference`、`insufficient`，推断默认禁用、启用后带限定措辞；精确引文校验只能证明出自交付证据，`entailment_verified=false`，任何答案都不回写记忆。固定同一 prompt/schema/证据、同时开启 thinking 并将生成上限从 384 改为 2,048 的诊断，三题最终正文均空、无裁判，是机械失败，不能记作语义 0/3；中性短问题 canary 则三种配置均能生成 JSON。随后仅把 thinking 上限增至 8,192 并补停止元数据，同证据三题各运行一次、3/3 通过，约 9.5 分钟；这是固定 v1.4 证据的阅读验证，与 v1.5 真实检索回放分开。Ollama v0.32.14 thinking + schema 的内部两阶段与末段 token 统计限制见[整体设计](native-contextual-retrieval.md#7-独立的结构化证据阅读器)。reader 实验不构成新检索贡献。

这仍是词法召回后的语义重排。query anchor 是英语词项启发式，未实现词性、实体或金额/小数等数值语义；索引 `ascii-han/1` 未变。只要其他来源有 anchor 命中，相关但完全使用同义词的来源就可能被过滤且不触发兜底；距命中窗口很远的关系也仍可能遗漏。结果保持 `coverage=partial`、`world_negative=false`。完整 dense 召回需要宿主显式接入独立向量索引及其权限、版本和删除治理；本轮同快照验证也不覆盖入库角色污染或八条候选上限导致的事实遗漏。

v1.5 已完成并集覆盖去重、角色起点保持及畸形 span 防御。28 个文件的定向测试为 496 passed、20 skipped（PostgreSQL），无失败；本轮修改模块 Ruff 通过，不声称全仓库 lint 通过。[v1.5 direct](../../evaluation/LONGMEMEVAL_V15_REGRESSION.md#historical-runs)为 1/3，[同 core 的 structured thinking 回放](../../evaluation/LONGMEMEVAL_V15_REGRESSION.md#historical-runs)本地裁判为 3/3；均每题一次、无失败重试，从三个相同入库快照实际运行检索、打包、回答及裁判，不重新抽取入库。CLI 显式设置 `--reader-mode structured --reader-thinking --reader-output-tokens 8192 --allow-contextual-inference`，只对 reader 启用 thinking；抽取与裁判默认保持关闭。

两组交付证据逐字相同，所有引文匹配原文。新模式首题标注上下文推断；第三题实际回答 `Home and Serenity Yoga studio`，居家引文上文却是 `was thinking of`，把计划混入实际状态仍有范围与状态精度问题。裁判通过不证明严格单值精确 100%。新模式耗时约 7.23 分钟、9 次模型 API 请求，API 报告 86,625 输入 / 353 输出 tokens，embedding 输出未知且内部 thinking 未完整计量；九次 HTTP 模型 API 请求不等于九次底层推理。三个错题通过不代表全量质量或完整成本达标。
