"""
一键完整同步 + 验证
用法: python data_audit/sync_all.py

流程:
  1. sync_mirror --full-refresh  — 补齐月度表缺口
  2. sync_daily（自动检测日度缺口，补拉到昨天）
  3. data_health_check — 出健康报告
"""

import subprocess
import sys
import sqlite3
from datetime import date, timedelta
from pathlib import Path

# 路径
SCRIPT_DIR = Path(__file__).parent
DATA_DIR = SCRIPT_DIR / "data"
DB_PATH = DATA_DIR / "dameng_mirror.db"
REPORT_DIR = SCRIPT_DIR / "reports"
REPORT_DIR.mkdir(exist_ok=True)

PYTHON = sys.executable


def run(cmd, label):
    """运行子命令，打印标题"""
    print(f"\n{'='*60}")
    print(f"📦 {label}")
    print(f"{'='*60}\n")
    result = subprocess.run(cmd, cwd=str(SCRIPT_DIR))
    return result.returncode


def get_daily_gap_days():
    """检测日度数据缺口天数（从最新日期到昨天）"""
    if not DB_PATH.exists():
        return 30  # 数据库不存在，拉 30 天
    try:
        db = sqlite3.connect(str(DB_PATH))
        row = db.execute("SELECT MAX(STATISTICS_DATE) FROM LOCAL_DAILY_REVENUE").fetchone()
        db.close()
        if not row or not row[0]:
            return 30
        latest = date.fromisoformat(row[0])
        yesterday = date.today() - timedelta(days=1)
        gap = (yesterday - latest).days
        return max(gap, 1)  # 至少拉 1 天
    except Exception:
        return 7  # 出错默认拉 7 天


def main():
    today = date.today().strftime("%Y%m%d")
    print(f"🚀 一键完整同步 — {date.today()}")
    print(f"   数据库: {DB_PATH}")

    # Step 1: 月度表全量补齐
    code = run(
        [PYTHON, str(SCRIPT_DIR / "sync" / "sync_mirror.py"), "--full-refresh", "--yes"],
        "Step 1/3: 月度表全量补齐 (sync_mirror --full-refresh)"
    )
    if code not in (0, 1):  # exit 1 是有 warning 但正常
        print(f"⚠️ sync_mirror 异常退出 (code={code})")

    # Step 2: 日度数据补齐到昨天
    gap = get_daily_gap_days()
    print(f"\n📊 日度缺口检测: 需要补拉 {gap} 天")
    code = run(
        [PYTHON, str(SCRIPT_DIR / "sync" / "sync_daily.py"), "--backfill", str(gap)],
        f"Step 2/3: 日度数据补齐 (sync_daily --backfill {gap})"
    )
    if code not in (0, 1):
        print(f"⚠️ sync_daily 异常退出 (code={code})")

    # Step 3: 健康报告
    month = date.today().strftime("%Y%m")
    report_file = REPORT_DIR / f"health_report_{month}.md"
    code = run(
        [PYTHON, str(SCRIPT_DIR / "audit" / "data_health_check.py"),
         "--month", month, "--output", str(report_file)],
        "Step 3/3: 健康校验 + 出报告"
    )

    print(f"\n{'='*60}")
    if report_file.exists():
        print(f"✅ 全部完成！报告: {report_file}")
    else:
        print(f"⚠️ 报告未生成，请检查日志")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
