# Agent Memory 资源基线

日期：2026-09-11

## 测量方法

```bash
source .venv/bin/activate
python tools/resource_baseline.py --events 500 --recalls 50
```

负载为单进程、单并发、500 个带一条 Claim 的 Event 写入及 50 次检索。工具同时分别在新子进程导入 core、Evolution 和 PostgreSQL 包，避免用同一进程的累积峰值混淆组件开销。

## 固定门槛

| 指标 | 门槛 |
| --- | ---: |
| 初始化 | 100 ms |
| 500 次写入总时间 | 5,000 ms |
| 50 次检索总时间 | 5,000 ms |
| 进程峰值 RSS | 128 MiB |
| Python heap 峰值 | 16 MiB |
| 500 条负载数据库 | 8 MiB |
| 检索 Token | 不超过调用方预算 |

这些是 alpha 阶段回归门槛，不是对所有硬件的性能承诺。更换 Python、OS、SQLite 版本或负载后必须重新建立基线。

## 2026-09-11 结果

Python 3.13.5，macOS arm64，单并发：

| 指标 | 结果 |
| --- | ---: |
| 初始化 | 4.520 ms |
| 500 次写入 | 633.399 ms |
| 50 次检索 | 2,196.512 ms |
| 检索后进程峰值 | 90,144,768 bytes |
| Python heap 峰值 | 2,865,702 bytes |
| SQLite 数据库 | 1,052,672 bytes |
| 最大 Token 估算 | 648 / 1,200 |

本次固定负载通过门槛。后续在 PostgreSQL 17.11 隔离库完成 worker 队列验收：同一 scope 的 pending/running 容量可配置，重复 job key 不重复计数，容量满时明确拒绝，领取仍为单条租约且使用 `SKIP LOCKED`，不把历史队列常驻 Python 内存。
