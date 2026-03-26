#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
日粒度数据同步脚本

将日营收、日车流和门店明细数据从云端 API 拉取到本地 dameng_mirror.db，
预算入区率、占比、客单价等衍生指标，减少 LLM 在线计算量。

四张本地表：
- LOCAL_DAILY_REVENUE: 日营收（每行 = 1 服务区 × 1 天）
- LOCAL_DAILY_TRAFFIC: 日车流（每行 = 1 服务区 × 1 天，东西区已合并，来自 cloud_direct）
- NEWDAILYCLOSINGUPLOAD: 门店明细（每行 = 1 门店 × 1 天，含业态/营收/客单等）
- NEWSECTIONFLOWLIST: 日车流新版（每行 = 1 服务区 × 1 方位 × 1 天，来自 data_proxy）

同步策略：
- 日营收：遍历所有服务区逐个 GET（API 限制，多 ID 只返合计）
- 日车流：一次性 POST 全省，按服务区+日期分组后存入
- 保留 90 天，超期自动清理

用法:
    # 拉昨天的数据
    python ops/scripts/sync/sync_daily.py

    # 拉指定日期
    python ops/scripts/sync/sync_daily.py --date 2026-03-13

    # 回填最近 N 天
    python ops/scripts/sync/sync_daily.py --backfill 7

    # 只拉营收 / 只拉车流
    python ops/scripts/sync/sync_daily.py --only revenue
    python ops/scripts/sync/sync_daily.py --only traffic

    # 覆盖已存在记录（修复性重跑）
    python ops/scripts/sync/sync_daily.py --date 2026-03-13 --force

    # 详细输出
    python ops/scripts/sync/sync_daily.py --verbose
"""

import argparse
import logging
import sqlite3
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import requests
import urllib3

# 禁用 SSL 警告（云平台 API 证书问题）
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 项目路径（data_audit/sync/sync_daily.py → 上溯 1 级到 data_audit 目录）
DATA_AUDIT_DIR = Path(__file__).resolve().parent.parent
DB_PATH = DATA_AUDIT_DIR / "data" / "dameng_mirror.db"

# 云平台直连基地址（日营收/日车流专用，不走 data_proxy）
CLOUD_BASE = "https://api.eshangtech.com"
REVENUE_PATH = "/EShangApiMain/Revenue/GetRevenueReportByDate"
TRAFFIC_PATH = "/EShangApiMain/BigData/GetSECTIONFLOWList"

REQUEST_TIMEOUT = 20
MAX_RETRIES = 2
RETENTION_DAYS = 90  # 保留天数

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("sync_daily")


# ============================================================
# 数据库操作
# ============================================================

def get_connection() -> sqlite3.Connection:
    """获取 SQLite 连接"""
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def ensure_tables(conn: sqlite3.Connection):
    """确保两张日粒度表存在"""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS LOCAL_DAILY_REVENUE (
            _id INTEGER PRIMARY KEY AUTOINCREMENT,
            STATISTICS_DATE TEXT NOT NULL,
            SERVERPART_ID TEXT NOT NULL,
            SERVERPART_NAME TEXT,
            REVENUE_TOTAL REAL DEFAULT 0,
            TICKET_COUNT INTEGER DEFAULT 0,
            TOTAL_COUNT INTEGER DEFAULT 0,
            CASH_AMOUNT REAL DEFAULT 0,
            MOBILE_AMOUNT REAL DEFAULT 0,
            MOBILE_RATE REAL DEFAULT 0,
            OFF_AMOUNT REAL DEFAULT 0,
            REVENUE_EAST REAL DEFAULT 0,
            REVENUE_WEST REAL DEFAULT 0,
            EAST_RATE REAL DEFAULT 0,
            WEST_RATE REAL DEFAULT 0,
            AVG_TICKET REAL DEFAULT 0,
            UNIQUE(STATISTICS_DATE, SERVERPART_ID)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS LOCAL_DAILY_TRAFFIC (
            _id INTEGER PRIMARY KEY AUTOINCREMENT,
            STATISTICS_DATE TEXT NOT NULL,
            SERVERPART_ID TEXT NOT NULL,
            SERVERPART_NAME TEXT,
            SECTION_TOTAL INTEGER DEFAULT 0,
            ENTRY_TOTAL INTEGER DEFAULT 0,
            ENTRY_RATE REAL DEFAULT 0,
            SECTION_EAST INTEGER DEFAULT 0,
            ENTRY_EAST INTEGER DEFAULT 0,
            ENTRY_RATE_EAST REAL DEFAULT 0,
            SECTION_WEST INTEGER DEFAULT 0,
            ENTRY_WEST INTEGER DEFAULT 0,
            ENTRY_RATE_WEST REAL DEFAULT 0,
            UNIQUE(STATISTICS_DATE, SERVERPART_ID)
        )
    """)
    # 新版日车流表（data_proxy → NEWGetSECTIONFLOWList）
    conn.execute("""
        CREATE TABLE IF NOT EXISTS NEWSECTIONFLOWLIST (
            _id INTEGER PRIMARY KEY AUTOINCREMENT,
            STATISTICS_DATE TEXT NOT NULL,
            SERVERPART_ID INTEGER NOT NULL,
            SERVERPART_NAME TEXT,
            DIRECTION TEXT,
            SECTION_FLOW INTEGER DEFAULT 0,
            ENTRY_FLOW INTEGER DEFAULT 0,
            MALE_COUNT INTEGER DEFAULT 0,
            FEMALE_COUNT INTEGER DEFAULT 0,
            VALID_STATUS INTEGER DEFAULT 1,
            UNIQUE(STATISTICS_DATE, SERVERPART_ID, DIRECTION)
        )
    """)
    conn.commit()
    logger.debug("表结构已就绪")


def get_service_area_ids(conn: sqlite3.Connection) -> List[Dict]:
    """从 NEWGETSERVERPARTLIST 获取所有服务区 ID 和名称"""
    try:
        rows = conn.execute(
            "SELECT 服务区内码, 服务区名称 FROM NEWGETSERVERPARTLIST"
        ).fetchall()
        return [{"id": str(r[0]), "name": r[1]} for r in rows if r[0]]
    except Exception as e:
        logger.warning(f"获取服务区列表失败: {e}")
        return []


def cleanup_old_data(conn: sqlite3.Connection, retention_days: int = RETENTION_DAYS):
    """清理超过 retention_days 天的旧数据"""
    cutoff = (datetime.now() - timedelta(days=retention_days)).strftime("%Y-%m-%d")
    for table in ["LOCAL_DAILY_REVENUE", "LOCAL_DAILY_TRAFFIC", "NEWSECTIONFLOWLIST"]:
        try:
            cnt = conn.execute(
                f'SELECT COUNT(*) FROM {table} WHERE STATISTICS_DATE < ?', (cutoff,)
            ).fetchone()[0]
            if cnt > 0:
                conn.execute(
                    f'DELETE FROM {table} WHERE STATISTICS_DATE < ?', (cutoff,)
                )
                logger.info(f"  🧹 {table}: 清理 {cnt} 条超过 {retention_days} 天的旧数据")
        except Exception:
            pass
    conn.commit()


# ============================================================
# API 调用
# ============================================================

def _safe_float(v, default=0.0) -> float:
    """安全转换为浮点数"""
    try:
        return float(v) if v is not None else default
    except (ValueError, TypeError):
        return default


def _safe_int(v, default=0) -> int:
    """安全转换为整数"""
    try:
        return int(float(v)) if v is not None else default
    except (ValueError, TypeError):
        return default


def _pct(part: float, total: float) -> float:
    """计算百分比，除零返回 0"""
    return round(part / total * 100, 2) if total else 0.0


def api_get(url: str, params: Dict, retries: int = MAX_RETRIES) -> Optional[Dict]:
    """带重试的 GET 请求"""
    for attempt in range(retries):
        try:
            resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT, verify=False)
            if resp.status_code == 200:
                return resp.json()
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(1)
                continue
            logger.warning(f"GET 失败 {url}: {e}")
    return None


def api_post(url: str, body: Dict, retries: int = MAX_RETRIES) -> Optional[Dict]:
    """带重试的 POST 请求"""
    for attempt in range(retries):
        try:
            resp = requests.post(url, json=body, timeout=REQUEST_TIMEOUT, verify=False)
            if resp.status_code == 200:
                return resp.json()
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(1)
                continue
            logger.warning(f"POST 失败 {url}: {e}")
    return None


# ============================================================
# 日营收同步
# ============================================================

def sync_daily_revenue(conn: sqlite3.Connection, date_str: str,
                       service_areas: List[Dict], force: bool = False) -> int:
    """
    拉取指定日期的日营收数据。

    因为 API 限制（传多 ID 只返回合计），需要逐个服务区拉取。
    """
    url = f"{CLOUD_BASE}{REVENUE_PATH}"
    added = 0
    errors = 0

    logger.info(f"  📊 日营收: 拉取 {date_str}，{len(service_areas)} 个服务区...")

    for i, sa in enumerate(service_areas):
        sa_id = sa["id"]
        sa_name = sa["name"]

        # 检查是否已有数据（跳过已拉取的）
        existing = conn.execute(
            "SELECT COUNT(*) FROM LOCAL_DAILY_REVENUE "
            "WHERE STATISTICS_DATE = ? AND SERVERPART_ID = ?",
            (date_str, sa_id)
        ).fetchone()[0]
        if existing > 0 and not force:
            continue

        params = {
            "ServerpartIds": sa_id,
            "StartDate": date_str,
            "EndDate": date_str,
            "DataSourceType": 1,
            "GroupByDaily": "true",
        }

        resp = api_get(url, params)
        if not resp or resp.get("Result_Code") != 100:
            errors += 1
            if errors <= 3:
                logger.debug(f"    ⚠️ {sa_name}({sa_id}): API 返回异常")
            continue

        items = resp.get("Result_Data", {}).get("List", [])
        if not items:
            continue

        # 解析嵌套结构（items[0].node = 合计数据）
        node = items[0].get("node", {})
        total_rev = node.get("TotalRevenue") or {}
        region_a = node.get("RegionARevenue") or {}  # 东区
        region_b = node.get("RegionBRevenue") or {}  # 西区

        revenue_total = _safe_float(total_rev.get("Revenue_Amount"))
        ticket_count = _safe_int(total_rev.get("Ticket_Count"))
        total_count = _safe_int(total_rev.get("Total_Count"))
        cash_amount = _safe_float(total_rev.get("CashPay_Amount"))
        mobile_amount = _safe_float(total_rev.get("MobilePay_Amount"))
        off_amount = _safe_float(total_rev.get("Total_OffAmount"))
        revenue_east = _safe_float(region_a.get("Revenue_Amount"))
        revenue_west = _safe_float(region_b.get("Revenue_Amount"))

        # 预算衍生指标
        mobile_rate = _pct(mobile_amount, revenue_total)
        east_rate = _pct(revenue_east, revenue_total)
        west_rate = _pct(revenue_west, revenue_total)
        avg_ticket = round(revenue_total / total_count, 2) if total_count else 0.0

        conn.execute(
            "INSERT OR REPLACE INTO LOCAL_DAILY_REVENUE "
            "(STATISTICS_DATE, SERVERPART_ID, SERVERPART_NAME, "
            "REVENUE_TOTAL, TICKET_COUNT, TOTAL_COUNT, "
            "CASH_AMOUNT, MOBILE_AMOUNT, MOBILE_RATE, OFF_AMOUNT, "
            "REVENUE_EAST, REVENUE_WEST, EAST_RATE, WEST_RATE, AVG_TICKET) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (date_str, sa_id, sa_name,
             revenue_total, ticket_count, total_count,
             cash_amount, mobile_amount, mobile_rate, off_amount,
             revenue_east, revenue_west, east_rate, west_rate, avg_ticket)
        )
        added += 1

        # 进度日志（每 30 个输出一次）
        if (i + 1) % 30 == 0:
            logger.info(f"    进度: {i+1}/{len(service_areas)}, 已写入 {added} 条")

    conn.commit()

    if errors > 0:
        logger.warning(f"    ⚠️ {errors} 个服务区 API 异常")
    logger.info(f"    ✅ 日营收完成: {added} 条新数据")
    return added


# ============================================================
# 日车流同步
# ============================================================

def sync_daily_traffic(conn: sqlite3.Connection, date_str: str, force: bool = False) -> int:
    """
    拉取指定日期的日车流数据。

    API 支持不传 SERVERPART_IDS 返回全省，一次性拉取后按服务区分组。
    """
    url = f"{CLOUD_BASE}{TRAFFIC_PATH}"

    logger.info(f"  🚗 日车流: 拉取 {date_str}（全省一次性）...")

    body = {
        "SearchParameter": {
            "STATISTICS_DATE_Start": date_str,
            "STATISTICS_DATE_End": date_str,
            "SECTIONFLOW_STATUS": 1,
        },
        "PageIndex": 1,
        "PageSize": 999999,
    }

    resp = api_post(url, body)
    if not resp or resp.get("Result_Code") != 100:
        logger.error(f"    ❌ 日车流 API 失败: {resp.get('Result_Desc', '未知错误') if resp else '无响应'}")
        return 0

    items = resp.get("Result_Data", {}).get("List", [])
    if not items:
        logger.info(f"    {date_str}: 无数据")
        return 0

    logger.info(f"    收到 {len(items)} 条原始记录，按服务区合并东西区...")

    # 按服务区+日期分组（一个服务区可能有东/西两条记录）
    # key = (date, server_id)
    grouped = defaultdict(lambda: {
        "name": "",
        "section_total": 0, "entry_total": 0,
        "section_east": 0, "entry_east": 0,
        "section_west": 0, "entry_west": 0,
    })

    for item in items:
        raw_date = (item.get("STATISTICS_DATE") or "").replace("/", "-")
        # 标准化日期格式（可能有 "2026/3/13 0:00:00" 格式）
        if " " in raw_date:
            raw_date = raw_date.split(" ")[0]
        # 补零：2026-3-13 → 2026-03-13
        parts = raw_date.split("-")
        if len(parts) == 3:
            raw_date = f"{parts[0]}-{int(parts[1]):02d}-{int(parts[2]):02d}"

        sp_id = str(item.get("SERVERPART_ID", item.get("服务区内码", "")))
        sp_name = item.get("SERVERPART_NAME", item.get("服务区名称", ""))
        region = (item.get("SERVERPART_REGION") or "").strip()
        section = _safe_int(item.get("SECTIONFLOW_NUM"))
        entry = _safe_int(item.get("SERVERPART_FLOW"))

        if not sp_id or not raw_date:
            continue

        key = (raw_date, sp_id)
        g = grouped[key]
        g["name"] = sp_name
        g["section_total"] += section
        g["entry_total"] += entry

        # 判断东/西区（常见写法：东区/西区/南区/北区/A区/B区）
        region_lower = region.lower()
        if any(k in region_lower for k in ["东", "a", "南"]):
            g["section_east"] += section
            g["entry_east"] += entry
        else:
            g["section_west"] += section
            g["entry_west"] += entry

    # 写入数据库
    added = 0
    for (dt, sp_id), g in grouped.items():
        # 检查已有数据
        existing = conn.execute(
            "SELECT COUNT(*) FROM LOCAL_DAILY_TRAFFIC "
            "WHERE STATISTICS_DATE = ? AND SERVERPART_ID = ?",
            (dt, sp_id)
        ).fetchone()[0]
        if existing > 0 and not force:
            continue

        conn.execute(
            "INSERT OR REPLACE INTO LOCAL_DAILY_TRAFFIC "
            "(STATISTICS_DATE, SERVERPART_ID, SERVERPART_NAME, "
            "SECTION_TOTAL, ENTRY_TOTAL, ENTRY_RATE, "
            "SECTION_EAST, ENTRY_EAST, ENTRY_RATE_EAST, "
            "SECTION_WEST, ENTRY_WEST, ENTRY_RATE_WEST) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (dt, sp_id, g["name"],
             g["section_total"], g["entry_total"],
             _pct(g["entry_total"], g["section_total"]),
             g["section_east"], g["entry_east"],
             _pct(g["entry_east"], g["section_east"]),
             g["section_west"], g["entry_west"],
             _pct(g["entry_west"], g["section_west"]))
        )
        added += 1

    conn.commit()
    logger.info(f"    ✅ 日车流完成: {added} 条新数据（{len(grouped)} 个服务区×日）")
    return added


# ============================================================
# 验证
# ============================================================

# ============================================================
# 门店明细同步（复用 auto_sync.sync_daily_closing）
# ============================================================

def _sync_shops_for_date(conn: sqlite3.Connection, date_str: str) -> int:
    """
    拉取指定日期的门店明细（NEWDAILYCLOSINGUPLOAD）。

    复用 auto_sync.py 已有的 sync_daily_closing 函数。
    """
    logger.info(f"  🏪 门店明细: 拉取 {date_str}...")
    try:
        # 导入 auto_sync 中的已有函数
        sync_dir = Path(__file__).resolve().parent
        if str(sync_dir) not in sys.path:
            sys.path.insert(0, str(sync_dir))
        from auto_sync import sync_daily_closing, EXTRA_SYNC_TABLE

        entry = EXTRA_SYNC_TABLE[0]  # daily_closing 配置
        added = sync_daily_closing(conn, entry, [date_str])
        logger.info(f"    ✅ 门店明细完成: {added} 条")
        return added
    except Exception as e:
        logger.error(f"    ❌ 门店明细失败: {e}")
        return 0


# ============================================================
# 日车流同步 — 新版（data_proxy → NEWSECTIONFLOWLIST）
# ============================================================

def _sync_section_flow_for_date(conn: sqlite3.Connection, date_str: str,
                                 force: bool = False) -> int:
    """
    拉取指定日期的新版日车流（NEWSECTIONFLOWLIST）。

    复用 auto_sync.py 已有的 sync_section_flow 函数。
    force 时先清理旧数据再拉（sync_section_flow 内部会清理）。
    """
    logger.info(f"  🚗 新版日车流: 拉取 {date_str}...")
    try:
        # 检查是否已有数据（非 force 时跳过）
        if not force:
            existing = conn.execute(
                "SELECT COUNT(*) FROM NEWSECTIONFLOWLIST WHERE STATISTICS_DATE = ?",
                (date_str,)
            ).fetchone()[0]
            if existing > 0:
                logger.info(f"    已有 {existing} 条，跳过（用 --force 覆盖）")
                return 0

        # 导入 auto_sync 中的已有函数
        sync_dir = Path(__file__).resolve().parent
        if str(sync_dir) not in sys.path:
            sys.path.insert(0, str(sync_dir))
        from auto_sync import sync_section_flow, EXTRA_SYNC_TABLE

        # 找到 section_flow 配置
        entry = next(e for e in EXTRA_SYNC_TABLE if e["name"] == "section_flow")
        added = sync_section_flow(conn, entry, [date_str])
        logger.info(f"    ✅ 新版日车流完成: {added} 条")
        return added
    except Exception as e:
        logger.error(f"    ❌ 新版日车流失败: {e}")
        return 0


def verify(conn: sqlite3.Connection, date_str: str):
    """验证指定日期的数据完整性"""
    logger.info(f"\n{'='*50}")
    logger.info(f"📋 验证 {date_str}")
    logger.info(f"{'='*50}")

    for table in ["LOCAL_DAILY_REVENUE", "LOCAL_DAILY_TRAFFIC", "NEWSECTIONFLOWLIST"]:
        try:
            cnt = conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE STATISTICS_DATE = ?",
                (date_str,)
            ).fetchone()[0]

            if table == "LOCAL_DAILY_REVENUE" and cnt > 0:
                row = conn.execute(
                    f"SELECT SUM(REVENUE_TOTAL), COUNT(*), AVG(AVG_TICKET) "
                    f"FROM {table} WHERE STATISTICS_DATE = ?",
                    (date_str,)
                ).fetchone()
                logger.info(
                    f"  {table}: {cnt} 条 | "
                    f"全省总营收: {row[0]:,.0f} | "
                    f"平均客单价: {row[2]:.1f}"
                )
            elif table == "LOCAL_DAILY_TRAFFIC" and cnt > 0:
                row = conn.execute(
                    f"SELECT SUM(SECTION_TOTAL), SUM(ENTRY_TOTAL), COUNT(*) "
                    f"FROM {table} WHERE STATISTICS_DATE = ?",
                    (date_str,)
                ).fetchone()
                total_rate = _pct(row[1], row[0])
                logger.info(
                    f"  {table}: {cnt} 条 | "
                    f"全省断面: {row[0]:,} | "
                    f"入区: {row[1]:,} | "
                    f"入区率: {total_rate}%"
                )
            elif table == "NEWSECTIONFLOWLIST" and cnt > 0:
                row = conn.execute(
                    "SELECT SUM(SECTION_FLOW), SUM(ENTRY_FLOW), "
                    "COUNT(DISTINCT SERVERPART_ID) "
                    "FROM NEWSECTIONFLOWLIST WHERE STATISTICS_DATE = ?",
                    (date_str,)
                ).fetchone()
                total_rate = _pct(row[1], row[0])
                logger.info(
                    f"  {table}: {cnt} 条 | "
                    f"全省断面: {row[0]:,} | "
                    f"入区: {row[1]:,} | "
                    f"入区率: {total_rate}% | "
                    f"服务区数: {row[2]}"
                )
            else:
                logger.warning(f"  {table}: {cnt} 条 {'⚠️ 无数据' if cnt == 0 else ''}")
        except Exception as e:
            logger.error(f"  {table}: 验证失败 - {e}")


# ============================================================
# 主入口
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="日粒度数据同步（日营收 + 日车流 + 门店 + 新版车流）")
    parser.add_argument("--date", help="指定日期 YYYY-MM-DD（默认昨天）")
    parser.add_argument("--backfill", type=int, help="回填最近 N 天")
    parser.add_argument("--only", choices=["revenue", "traffic", "shops", "section_flow"],
                        help="只拉营收、车流、门店或新版车流")
    parser.add_argument("--verbose", "-v", action="store_true", help="详细输出")
    parser.add_argument("--verify-only", action="store_true", help="只验证不拉数据")
    parser.add_argument("--force", action="store_true", help="覆盖已存在记录，用于修复性重跑")
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    start_time = time.time()

    # 确定要拉取的日期
    if args.backfill:
        dates = [
            (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
            for i in range(1, args.backfill + 1)
        ]
        dates.reverse()  # 时间正序
    elif args.date:
        dates = [args.date]
    else:
        dates = [(datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")]

    logger.info(f"🚀 日粒度数据同步")
    logger.info(f"   数据库: {DB_PATH}")
    logger.info(f"   日期范围: {dates[0]} ~ {dates[-1]}（{len(dates)} 天）")
    logger.info(f"   模式: {'仅' + args.only if args.only else '全量（营收+车流+新版车流）'}")
    logger.info(f"   写入策略: {'覆盖已存在记录' if args.force else '仅补充缺失记录'}")

    # 连接数据库
    conn = get_connection()
    ensure_tables(conn)

    if args.verify_only:
        for dt in dates:
            verify(conn, dt)
        conn.close()
        return

    # 获取服务区列表（日营收需要）
    service_areas = []
    if args.only not in ("traffic", "section_flow"):
        service_areas = get_service_area_ids(conn)
        if not service_areas:
            logger.error("❌ 无法获取服务区列表，请确认 NEWGETSERVERPARTLIST 表有数据")
            conn.close()
            sys.exit(1)
        logger.info(f"   服务区数量: {len(service_areas)}")

    # 逐日拉取
    total_revenue = 0
    total_traffic = 0
    total_shops = 0
    total_section_flow = 0

    for date_str in dates:
        logger.info(f"\n{'='*50}")
        logger.info(f"📅 处理日期: {date_str}")
        logger.info(f"{'='*50}")

        if args.only in (None, "revenue"):
            total_revenue += sync_daily_revenue(conn, date_str, service_areas, force=args.force)

        if args.only in (None, "traffic"):
            total_traffic += sync_daily_traffic(conn, date_str, force=args.force)

        if args.only in (None, "shops"):
            total_shops += _sync_shops_for_date(conn, date_str)

        if args.only in (None, "section_flow"):
            total_section_flow += _sync_section_flow_for_date(conn, date_str, force=args.force)

    # 清理旧数据
    cleanup_old_data(conn)

    # 验证最后一天
    verify(conn, dates[-1])

    # 汇总
    elapsed = time.time() - start_time
    logger.info(f"\n{'='*50}")
    logger.info(f"🎉 同步完成 ({elapsed:.1f}s)")
    logger.info(f"   日营收: {total_revenue} 条")
    logger.info(f"   日车流: {total_traffic} 条")
    logger.info(f"   门店明细: {total_shops} 条")
    logger.info(f"   新版车流: {total_section_flow} 条")
    logger.info(f"{'='*50}")

    conn.close()


if __name__ == "__main__":
    main()
