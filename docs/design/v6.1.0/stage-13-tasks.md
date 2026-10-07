# 第十三阶段任务

版本 AM61-ST13-TASKS / 1.0；2026-10-07；DONE（仅此阶段限定组合）。
方案：[stage-13-plan.md](stage-13-plan.md)；实现：[stage-13.md](stage-13.md)；证据：[validation-stage-13.json](validation-stage-13.json)。
全局 T11/T25/T26/T27/T29/T44 仍为 IN_PROGRESS，不宣称完整 M2。

| ID | 交付 | 状态 |
| --- | --- | --- |
| ST13-01 | FacetContext 类型、路由/期限/范围/用途合同及 v1 fingerprint 兼容 | DONE |
| ST13-02 | locale-context/1 三值条件、例外与必要字段资格合成 | DONE |
| ST13-03 | 同域/跨域、显式偏序、未知/争议和不回退行为 | DONE |
| ST13-04 | AND/OR 支持时间、间隙、点和午夜/到期读端守卫 | DONE |
| ST13-05 | 定义/context CAS、路由独占领取与原子发布竞争 | DONE |
| ST13-06 | 所有实际输入权限、辅助来源、擦除与最终交付检查 | DONE |
| ST13-07 | 只读 SDK/MCP、可运行示例、双后端相关回归与安装验证 | DONE |
| ST13-08 | 更新版本台账、运行边界、验证记录及后续顺序 | DONE |

- [x] 声明的每个功能组合均有新增行为验证；未知时输出无 value 的诊断块。
- [x] 原条件候选不回写无条件 Claim；旧 v1 仍拒绝限定输入。
- [x] 当前安全和时间在发布/读取/最终交付均验证。
- [x] 条件处理和一般权限/历史/画像推断的边界明确分开。
- [x] 专项与受影响回归执行；历史全量成绩单独保存。
