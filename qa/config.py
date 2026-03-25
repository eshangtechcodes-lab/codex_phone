"""
QA 项目配置 — 统一管理路径、API 和常量

所有脚本通过 from config import * 引用配置，迁移时只需修改此文件。
"""
import platform
from pathlib import Path

_IS_WINDOWS = platform.system() == "Windows"

# ============================================================
# 路径配置
# ============================================================

# QA 项目根目录
QA_DIR = Path(__file__).parent

# 数据库路径（统一使用 AI-Python 主项目的镜像库，避免副本过时）
DB_PATH = str(Path("D:/AISpace/AI-Python/data/dameng_mirror.db") if _IS_WINDOWS else (QA_DIR / "dameng_mirror.db"))

# 报告输出目录
REPORTS_DIR = QA_DIR / "reports"
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

# 历史日志
HISTORY_PATH = str(QA_DIR / "qa_history.jsonl")

# Codex 巡查输出目录
PATROL_OUTPUT_DIR = str(REPORTS_DIR / "patrol_output")

# ============================================================
# API 配置
# ============================================================

API_URL = "https://llm.eshangtech.com/api/agent/"
TIMEOUT = 120  # 秒

# ============================================================
# Codex CLI 配置
# ============================================================

# Codex 工作目录（跑 Codex CLI 时的 cwd）
CODEX_CWD = str(QA_DIR.parent)  # codex_phone 根目录

# Codex 需要访问的外部数据目录
CODEX_DATA_DIR = "D:/AISpace/AI-Python/data" if _IS_WINDOWS else str(QA_DIR)

# ============================================================
# 测试配置
# ============================================================

USER_ID_PREFIX = "qa_auto"
DEFAULT_LLM_MODE = "qwen"
