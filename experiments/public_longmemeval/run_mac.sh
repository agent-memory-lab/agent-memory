#!/usr/bin/env bash
set -euo pipefail
cd "$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
if command -v python3.13 >/dev/null 2>&1; then
  PYTHON="$(command -v python3.13)"
elif command -v python3 >/dev/null 2>&1 && python3 -c 'import sys; assert sys.version_info >= (3,13)' 2>/dev/null; then
  PYTHON="$(command -v python3)"
else
  printf '%s\n' '需要已安装的 Python 3.13 或更新版本。脚本没有安装任何软件。请先自行安装兼容版本。'
  exit 2
fi
printf '%s\n' '先检查本机 Ollama 和已有 qwen3.5:9b；不下载模型，不开放端口。'
"$PYTHON" mac_smoke.py preflight
RUN="runs/$(date +%Y%m%d-%H%M%S)-$$"
mkdir -p runs
printf '%s\n' '开始 1 道公开数据题：无记忆、BM25、最近片段、agent-memory。模型请求预算 30 分钟/160 次；初始化和收尾另计。'
"$PYTHON" mac_smoke.py run --output "$RUN" --max-seconds 1800 --max-calls 160 2>&1 | tee "$RUN.log"
printf '完成。结果保存在 %s/result.json；日志在 %s.log。请先检查内容，再决定是否分享。\n' "$RUN" "$RUN"
