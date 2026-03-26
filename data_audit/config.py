# -*- coding: utf-8 -*-
"""
数据审核项目 — 统一配置

集中管理路径、API 地址和常量。无 Django 依赖，纯 SQLite + requests。
"""
import platform
from pathlib import Path

_IS_WINDOWS = platform.system() == "Windows"

# ============================================================
# 路径配置
# ============================================================

# 项目根目录（data_audit/）
PROJECT_DIR = Path(__file__).parent

# 数据库路径
DB_PATH = str(PROJECT_DIR / "data" / "dameng_mirror.db")

# ============================================================
# API 配置
# ============================================================

# 远程 DataAPI（达梦数据源）
REMOTE_API_BASE = "http://111.229.213.193:18071/api/dynamic"
REQUEST_TIMEOUT = 20
MAX_RETRIES = 3

# 业务 AI API（用于 QA 测试）
AI_API_URL = "https://llm.eshangtech.com/api/agent/"
AI_API_TIMEOUT = 120

# ============================================================
# 同步配置
# ============================================================

# 默认月份范围起点
DEFAULT_START_MONTH = "202401"

# 行数护栏：新数据不能比旧数据少 50% 以上
ROW_GUARD_RATIO = 0.5
