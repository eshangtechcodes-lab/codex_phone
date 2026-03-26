#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
事件驱动数据同步脚本

基于上游 sync-state API 监控表变更，有变动才精准拉取。
替代手动执行 sync_mirror.py 的工作模式。

流程:
  1. GET /api/sync-state/?status=need_sync  查询有变更的表
  2. 按「目标表名」匹配 SYNC_TABLE 配置
  3. 根据「变更月份」精准清理+重拉
  4. 展平联动（revenue_report 更新后自动展平门店表）
  5. 5 维验证
  6. 验证通过 → POST /api/sync-state/{ID}/reset/ 标记已同步
  7. 写入本地 _SYNC_LOG 同步日志

用法:
    # 查看有哪些表需要同步（dry-run，不实际拉数据）
    python ops/scripts/auto_sync.py --dry-run

    # 执行自动同步
    python ops/scripts/auto_sync.py

    # 只处理指定表编码
    python ops/scripts/auto_sync.py --table-code DAILY_CLOSING

    # 查看本地同步日志
    python ops/scripts/auto_sync.py --show-log

依赖: requests（项目已有）
"""

import argparse
try:
    import fcntl
except ImportError:
    fcntl = None  # Windows 不支持 fcntl
import json
import logging
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests

# 项目路径（data_audit/sync/auto_sync.py → 上溯 1 级到 data_audit 目录）
DATA_AUDIT_DIR = Path(__file__).resolve().parent.parent
DB_PATH = DATA_AUDIT_DIR / "data" / "dameng_mirror.db"

# 复用 sync_mirror.py 的核心函数和配置（同目录导入）
sync_dir = str(Path(__file__).resolve().parent)
if sync_dir not in sys.path:
    sys.path.insert(0, sync_dir)
from sync_mirror import (
    SYNC_TABLE,
    REMOTE_BASE,
    get_connection,
    get_service_area_ids,
    get_expected_month,
    sync_type_a,
    sync_type_a_children,
    sync_type_b,
    sync_type_c,
    sync_type_d,
    sync_type_e,
    sync_type_f_inc,
    clean_month_data,
    flatten_revenue_report_shops,
    verify_all,
    _month_range,
    api_get,
    extract_items,
    ensure_columns,
    insert_row,
    get_table_columns,
)

# ============================================================
# 额外表配置（SYNC_TABLE 里没有的新表）
# ============================================================
# DAILY_CLOSING: 日结上传，日粒度拉取，含门店嵌套
# 参数: STATISTICS_MONTH=YYYY-MM-DD（日期格式，不是 YYYYMM）
EXTRA_SYNC_TABLE: List[Dict] = [
    {"name": "daily_closing", "path": "/dailyClosingUpload/",
     "table": "NEWDAILYCLOSINGUPLOAD", "type": "F",
     "month_param": "STATISTICS_MONTH", "has_children": True},
    {"name": "section_flow", "path": "/NEWGetSECTIONFLOWList/",
     "table": "NEWSECTIONFLOWLIST", "type": "F",
     "month_param": "STATISTICS_MONTH", "has_children": False},
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("auto_sync")

# ============================================================
# sync-state API 配置
# ============================================================
SYNC_STATE_BASE = "http://111.229.213.193:18071"
SYNC_STATE_API = f"{SYNC_STATE_BASE}/api/sync-state/"
REQUEST_TIMEOUT = 15

# ============================================================
# 目标表名 → SYNC_TABLE entry 的映射（启动时构建）
# ============================================================
_TABLE_MAP: Dict[str, Dict] = {}
_PATH_MAP: Dict[str, Dict] = {}  # 数据接口路径 → entry

def _build_maps():
    """构建映射表，用于快速查找"""
    for entry in SYNC_TABLE:
        _TABLE_MAP[entry["table"]] = entry
        path = entry["path"]
        _PATH_MAP[path] = entry
    # 额外表也加入映射
    for entry in EXTRA_SYNC_TABLE:
        _TABLE_MAP[entry["table"]] = entry
        _PATH_MAP[entry["path"]] = entry

_build_maps()


# ============================================================
# sync-state API 调用
# ============================================================

def get_need_sync(table_code: Optional[str] = None) -> List[Dict]:
    """查询需要同步的表列表"""
    try:
        params = {"status": "need_sync"}
        resp = requests.get(SYNC_STATE_API, params=params, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()

        if not data.get("success"):
            logger.error(f"sync-state API 返回失败: {data}")
            return []

        items = data.get("data", [])

        # 可选：过滤指定表
        if table_code:
            items = [it for it in items if it.get("表编码") == table_code]

        return items

    except Exception as e:
        logger.error(f"sync-state API 调用失败: {e}")
        return []


def get_all_sync_state() -> List[Dict]:
    """查询所有表的同步状态（用于 dry-run 诊断）"""
    try:
        resp = requests.get(SYNC_STATE_API, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        return data.get("data", []), data.get("stats", {})
    except Exception as e:
        logger.error(f"sync-state API 调用失败: {e}")
        return [], {}


def reset_sync_state(sync_id: int, ack_version: int = None,
                     ack_api: dict = None) -> bool:
    """标记指定表已同步（POST reset）

    优先使用上游 ack_api 字段（含精确版本号），防止同步期间
    上游新变更被误标为已同步。fallback 到旧的空 body POST。
    """
    if ack_api and isinstance(ack_api, dict) and ack_api.get("endpoint"):
        url = f"{SYNC_STATE_BASE}{ack_api['endpoint']}"
        body = ack_api.get("body", {})
    else:
        url = f"{SYNC_STATE_API}{sync_id}/reset/"
        body = {"ack_change_version": ack_version} if ack_version else {}
    try:
        resp = requests.post(url, json=body, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        if data.get("success"):
            ver_info = f" (ack_v={body.get('ack_change_version', '?')})" if body else ""
            logger.info(f"  ✅ reset ID={sync_id} 成功{ver_info}")
            return True
        else:
            logger.warning(f"  ❌ reset ID={sync_id} 失败: {data.get('message')}")
            return False
    except Exception as e:
        logger.error(f"  ❌ reset ID={sync_id} 异常: {e}")
        return False


# ============================================================
# 映射与解析
# ============================================================

def match_entry(item: Dict) -> Optional[Dict]:
    """
    从 sync-state 的一条记录匹配到 SYNC_TABLE 的配置。

    优先用「目标表名」匹配，其次用「数据接口」路径匹配。
    """
    target_table = item.get("目标表名", "")

    # 直接匹配
    if target_table in _TABLE_MAP:
        return _TABLE_MAP[target_table]

    # 特殊映射：ACCOUNT_WARNING 在 SYNC_TABLE 里拆成了两个（D 类和 B 类 _RISK）
    # 上游只有一个 ACCOUNT_WARNING → NEWGETACCOUNTWARNINGLIST
    # 我们的 SYNC_TABLE 有 project_warning（D类）和 project_risk（B类）
    # project_warning 的 table 是 NEWGETACCOUNTWARNINGLIST，能直接匹配
    # project_risk 的 table 是 NEWGETACCOUNTWARNINGLIST_RISK，匹配不到
    # 但 project_risk 有独立的 path（/projectManagementRisk/），不受影响

    # 路径匹配
    api_path = item.get("数据接口", "")
    if api_path:
        # 去掉 /api/dynamic 前缀
        path = api_path.replace("/api/dynamic", "")
        if path in _PATH_MAP:
            return _PATH_MAP[path]

    return None


def parse_change_months(change_month_str: Optional[str]) -> List[str]:
    """
    解析「变更月份」字段为月份列表。

    上游格式：
    - 日期型: "2026-03-13" → ["202603"]
    - 月份型: "2026-03" → ["202603"]
    - 多值: "2026-02,2026-03" → ["202602", "202603"]
    - 空值: None → []
    """
    if not change_month_str:
        return []

    months = set()
    for part in str(change_month_str).split(","):
        part = part.strip()
        if not part:
            continue
        # 去掉短横线
        clean = part.replace("-", "")
        # 取前 6 位作为 YYYYMM
        if len(clean) >= 6:
            months.add(clean[:6])

    return sorted(months)


def get_change_months_smart(item: Dict) -> List[str]:
    """智能提取变更月份：优先 changed_periods，fallback parse_change_months()。

    上游 changed_periods 是结构化数据（含 insert/update 计数），
    比字符串解析更可靠。但 period="未知" 或字段缺失时回退到旧逻辑。

    Returns:
        YYYYMM 格式的月份列表
    """
    periods = item.get("changed_periods")
    if periods and isinstance(periods, list):
        months = set()
        for p in periods:
            period_str = str(p.get("period", ""))
            if period_str == "未知" or not period_str:
                continue
            clean = period_str.replace("-", "")
            if len(clean) >= 6:
                months.add(clean[:6])
        if months:
            return sorted(months)

    # fallback 到旧的字符串解析
    return parse_change_months(item.get("变更月份"))


def parse_change_dates(change_month_str: Optional[str]) -> List[str]:
    """
    解析「变更月份」字段为日期列表（用于日粒度表如 DAILY_CLOSING）。

    上游格式: "2026-03-13" → ["2026-03-13"]
    多值: "2026-03-12,2026-03-13" → ["2026-03-12", "2026-03-13"]
    """
    if not change_month_str:
        return []
    dates = []
    for part in str(change_month_str).split(","):
        part = part.strip()
        if part and len(part) >= 10:  # YYYY-MM-DD 格式
            dates.append(part[:10])
    return sorted(dates)


def get_f_class_gap_dates(conn: sqlite3.Connection, table_name: str) -> List[str]:
    """智能探测本地库中具有日维度的表的断档日期（满射型侦测核心）
    
    自动识别 统计日期 或 STATISTICS_DATE 字段。
    """
    try:
        exists = conn.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?", (table_name,)).fetchone()[0]
        if not exists: return []

        cols = [r[1] for r in conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()]
        date_col = next((c for c in ["统计日期", "STATISTICS_DATE"] if c in cols), None)
        if not date_col:
            return []

        row = conn.execute(f'SELECT MIN("{date_col}") FROM "{table_name}"').fetchone()
        if not row or not row[0]: return []
        
        min_str = str(row[0]).split()[0].replace("/", "-")
        parts = min_str.split("-")
        if len(parts) == 3:
            min_str = f"{parts[0]}-{int(parts[1]):02d}-{int(parts[2]):02d}"
        
        try:
            min_date = datetime.strptime(min_str, "%Y-%m-%d")
        except ValueError:
            min_date = datetime.now() - timedelta(days=30)
            
        min_date = min_date.replace(hour=0, minute=0, second=0, microsecond=0)

        yesterday = datetime.now() - timedelta(days=1)
        yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
        
        # 兜底下限：避免拉取过长历史导致超载，最多回溯 120 天
        limit_date = datetime.now() - timedelta(days=120)
        limit_date = limit_date.replace(hour=0, minute=0, second=0, microsecond=0)
        start_date = max(min_date, limit_date)

        # 1. 构造满射绝对连续日期
        all_dates = set()
        curr = start_date
        while curr <= yesterday:
            all_dates.add(curr.strftime("%Y-%m-%d"))
            curr += timedelta(days=1)
            
        # 2. 拉取库中真实存在的日期
        existing_dates_raw = conn.execute(f'SELECT DISTINCT "{date_col}" FROM "{table_name}"').fetchall()
        existing_dates = set()
        for r in existing_dates_raw:
            if not r or not r[0]: continue
            raw_str = str(r[0]).split()[0].replace("/", "-")
            p = raw_str.split("-")
            if len(p) == 3:
                existing_dates.add(f"{p[0]}-{int(p[1]):02d}-{int(p[2]):02d}")
        
        # 3. 计算缺失的日期差集
        missing_dates = sorted(list(all_dates - existing_dates))
        return missing_dates
    except Exception as e:
        logger.warning(f"智能断档探测失败 {table_name}: {e}")
        return []


# ============================================================
# DAILY_CLOSING 日粒度同步
# ============================================================

def sync_daily_closing(conn: sqlite3.Connection, entry: Dict,
                       dates: List[str]) -> int:
    """
    日粒度同步（F 类）：按日期拉取，展开 children 门店嵌套。

    DAILY_CLOSING 数据结构：
    顶层 = 服务区汇总（含 服务区内码、对客销售 等）
    children = 该服务区下的门店明细（含 门店名称、经营模式、对客销售 等）

    拉取参数: STATISTICS_MONTH=YYYY-MM-DD
    """
    table = entry["table"]
    path = entry["path"]
    added = 0

    def _sanitize_row(row: Dict) -> Dict:
        """清洗行数据的列名：替换双引号为单引号（避免 SQL 语法错误）"""
        cleaned = {}
        for k, v in row.items():
            # 列名含双引号会破坏 SQL，替换为单引号
            safe_key = k.replace('"', "'") if '"' in k else k
            cleaned[safe_key] = v
        return cleaned

    # 确保表存在
    exists = conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
        (table,)
    ).fetchone()[0]
    if not exists:
        conn.execute(f'CREATE TABLE IF NOT EXISTS "{table}" '
                     f'(_id INTEGER PRIMARY KEY AUTOINCREMENT)')
        conn.commit()

    cols = get_table_columns(conn, table)

    for date_str in dates:
        logger.info(f"   拉取日期: {date_str}")
        params = {"STATISTICS_MONTH": date_str}
        resp = api_get(path, params)
        items = extract_items(resp)

        if not items:
            logger.info(f"   {date_str}: 无数据，保留旧数据")
            continue

        # 有新数据了，安全删旧
        if "统计日期" in cols:
            parts = date_str.split("-")
            if len(parts) == 3:
                y, m, d = parts
                no_pad = f"{y}/{int(m)}/{int(d)}"
                with_pad = f"{y}/{m}/{d}"
                conn.execute(
                    f'DELETE FROM "{table}" WHERE 统计日期 LIKE ? OR 统计日期 LIKE ?',
                    (f"%{no_pad}%", f"%{with_pad}%")
                )
            else:
                conn.execute(
                    f'DELETE FROM "{table}" WHERE 统计日期 LIKE ?',
                    (f"%{date_str.replace('-', '/')}%",)
                )

        for sa_row in items:
            # 提取 children 门店数据
            children = sa_row.get("children", [])
            if isinstance(children, str):
                try:
                    children = json.loads(children)
                except (json.JSONDecodeError, TypeError):
                    children = []

            # 存服务区汇总行（去掉 children 以避免存储庞大嵌套 JSON）
            sa_copy = _sanitize_row(sa_row)
            sa_copy.pop("children", None)
            sa_copy["_IS_SUMMARY"] = "1"  # 标记为汇总行
            ensure_columns(conn, table, sa_copy, cols)
            insert_row(conn, table, sa_copy)
            added += 1

            # 存门店明细行
            sa_id = str(sa_row.get("服务区内码", ""))
            sa_name = sa_row.get("服务区名称", "")
            for shop in (children or []):
                if not isinstance(shop, dict):
                    continue
                shop = _sanitize_row(shop)
                shop["_IS_SUMMARY"] = "0"  # 标记为门店明细
                if "服务区内码" not in shop and sa_id:
                    shop["服务区内码"] = sa_id
                if "服务区名称" not in shop and sa_name:
                    shop["服务区名称"] = sa_name
                ensure_columns(conn, table, shop, cols)
                insert_row(conn, table, shop)
                added += 1

        conn.commit()

    return added


def sync_section_flow(conn: sqlite3.Connection, entry: Dict,
                      dates: List[str]) -> int:
    """
    日粒度同步（F 类）：从 data_proxy 拉取 NEWGetSECTIONFLOWList。

    数据结构：扁平列表，每条 = 1 服务区 × 1 方位 × 1 天（无 children）。
    拉取参数: STATISTICS_MONTH=YYYY-MM-DD
    """
    table = entry["table"]
    path = entry["path"]
    added = 0

    # 确保表存在
    exists = conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
        (table,)
    ).fetchone()[0]
    if not exists:
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS "{table}" (
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

    for date_str in dates:
        logger.info(f"   拉取日期: {date_str}")
        params = {"STATISTICS_MONTH": date_str}
        resp = api_get(path, params)
        items = extract_items(resp)

        if not items:
            logger.info(f"   {date_str}: 无数据，保留旧数据")
            continue

        # 有新数据了，安全删旧
        conn.execute(
            f'DELETE FROM "{table}" WHERE STATISTICS_DATE = ?',
            (date_str,)
        )

        def _safe_int(v, default=0):
            try:
                return int(float(v)) if v is not None else default
            except (ValueError, TypeError):
                return default

        for item in items:
            # 标准化日期："2026/03/18" → "2026-03-18"
            raw_date = (item.get("统计日期") or "").replace("/", "-")
            if " " in raw_date:
                raw_date = raw_date.split(" ")[0]
            parts = raw_date.split("-")
            if len(parts) == 3:
                raw_date = f"{parts[0]}-{int(parts[1]):02d}-{int(parts[2]):02d}"

            sp_id = item.get("服务区内码")
            if not sp_id or not raw_date:
                continue

            conn.execute(
                f'INSERT OR REPLACE INTO "{table}" '
                '(STATISTICS_DATE, SERVERPART_ID, SERVERPART_NAME, DIRECTION, '
                'SECTION_FLOW, ENTRY_FLOW, MALE_COUNT, FEMALE_COUNT, VALID_STATUS) '
                'VALUES (?,?,?,?,?,?,?,?,?)',
                (
                    raw_date,
                    int(sp_id),
                    item.get("服务区名称", ""),
                    item.get("服务区方位", ""),
                    _safe_int(item.get("断面流量")),
                    _safe_int(item.get("服务区流量")),
                    _safe_int(item.get("男性数量")),
                    _safe_int(item.get("女性数量")),
                    _safe_int(item.get("有效状态", 1)),
                )
            )
            added += 1

        conn.commit()

    return added


# ============================================================
# 本地同步状态比较表
# ============================================================

def ensure_sync_state_table(conn: sqlite3.Connection):
    """确保 _SYNC_STATE 本地版本比较表存在（含行数校验列）"""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS _SYNC_STATE (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sync_state_id INTEGER UNIQUE,
            table_code TEXT UNIQUE,
            target_table TEXT,
            synced_version INTEGER DEFAULT 0,
            synced_row_count INTEGER DEFAULT 0,
            last_change_months TEXT,
            last_change_summary TEXT,
            last_sync_time TEXT,
            created_at TEXT DEFAULT (datetime('now', 'localtime')),
            updated_at TEXT DEFAULT (datetime('now', 'localtime'))
        )
    """)
    # 兼容旧表：如果缺 synced_row_count 列则补上
    try:
        conn.execute('ALTER TABLE _SYNC_STATE ADD COLUMN synced_row_count INTEGER DEFAULT 0')
    except Exception:
        pass  # 列已存在
    conn.commit()


def get_local_versions(conn: sqlite3.Connection) -> Dict[str, int]:
    """获取本地所有表的已同步版本号，返回 {table_code: synced_version}"""
    try:
        rows = conn.execute(
            'SELECT table_code, synced_version FROM _SYNC_STATE'
        ).fetchall()
        return {code: ver for code, ver in rows}
    except Exception:
        return {}


def update_local_version(conn: sqlite3.Connection, item: Dict):
    """同步成功后，更新本地版本号 + 实际行数（用于下次校验）"""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # 统计同步后表的实际行数
    target_table = item.get("目标表名", "")
    row_count = 0
    if target_table:
        try:
            row_count = conn.execute(
                f'SELECT COUNT(*) FROM "{target_table}"'
            ).fetchone()[0]
        except Exception:
            pass
    conn.execute("""
        INSERT INTO _SYNC_STATE
            (sync_state_id, table_code, target_table, synced_version,
             synced_row_count,
             last_change_months, last_change_summary, last_sync_time, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(table_code) DO UPDATE SET
            synced_version = excluded.synced_version,
            synced_row_count = excluded.synced_row_count,
            last_change_months = excluded.last_change_months,
            last_change_summary = excluded.last_change_summary,
            last_sync_time = excluded.last_sync_time,
            updated_at = excluded.updated_at
    """, (
        int(item.get("ID", 0)),
        item.get("表编码", ""),
        target_table,
        int(item.get("变更版本", 0)),
        row_count,
        item.get("变更月份"),
        item.get("变更摘要"),
        now,
        now,
    ))
    conn.commit()
    logger.info(f"  📊 记录行数: {row_count} 行")


def init_local_versions(conn: sqlite3.Connection):
    """首次运行时，拉取上游全量状态并初始化本地版本表（将所有表的已同步版本设为当前上游版本）"""
    try:
        resp = requests.get(SYNC_STATE_API, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        items = data.get("data", [])
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for item in items:
            conn.execute("""
                INSERT OR IGNORE INTO _SYNC_STATE
                    (sync_state_id, table_code, target_table, synced_version,
                     last_sync_time, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                int(item.get("ID", 0)),
                item.get("表编码", ""),
                item.get("目标表名", ""),
                int(item.get("变更版本", 0)),  # 初始化时假设所有表已同步
                now, now,
            ))
        conn.commit()
        logger.info(f"   初始化本地版本表: {len(items)} 张表")
    except Exception as e:
        logger.error(f"   初始化本地版本表失败: {e}")


def get_need_sync_by_version(conn: sqlite3.Connection,
                              table_code_filter: Optional[str] = None) -> Tuple[List[Dict], bool]:
    """通过对比本地版本号 vs 上游版本号，找出需要同步的表。

    Returns:
        (need_sync_list, upstream_ok): 上游不可达时 upstream_ok=False
    """
    ensure_sync_state_table(conn)
    local_versions = get_local_versions(conn)

    # 如果本地版本表为空，先初始化
    if not local_versions:
        logger.info("   本地版本表为空，正在初始化...")
        init_local_versions(conn)
        local_versions = get_local_versions(conn)

    # 拉取上游全量状态
    try:
        resp = requests.get(SYNC_STATE_API, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        if not data.get("success"):
            return [], False
        all_items = data.get("data", [])
    except Exception as e:
        logger.error(f"sync-state API 调用失败: {e}")
        return [], False

    # 对比版本号 + 行数校验（与上游 本地记录数 对比）
    # 先从 _SYNC_STATE 取缓存的 synced_row_count，避免每次 COUNT(*)
    cached_row_counts = {}
    try:
        rows = conn.execute(
            'SELECT table_code, synced_row_count FROM _SYNC_STATE'
        ).fetchall()
        cached_row_counts = {code: cnt for code, cnt in rows}
    except Exception:
        pass

    need_sync = []
    for item in all_items:
        code = item.get("表编码", "")
        if table_code_filter and code != table_code_filter:
            continue
        upstream_ver = int(item.get("变更版本", 0))
        local_ver = local_versions.get(code, 0)

        # 版本号不一致 → 需要同步
        if upstream_ver > local_ver:
            item["_local_version"] = local_ver
            item["_sync_reason"] = "version"
            need_sync.append(item)
            continue

        # 版本一致 → 用上游行数校验（仅 B 类全量快照表触发同步）
        # C/D/E/F 类表本地行数口径与上游不一致，仅记录不触发
        upstream_count = int(item.get("本地记录数", 0) or 0)
        if upstream_count <= 0:
            continue  # 上游未提供行数，跳过

        # 查该表的 sync_type
        target_table = item.get("目标表名", "")
        matched_entry = _TABLE_MAP.get(target_table)
        if not matched_entry:
            continue
        entry_type = matched_entry.get("type", "")

        # A/A_CHILDREN：省级汇总，行数与上游不可比，跳过
        if entry_type in ("A", "A_CHILDREN"):
            continue

        # C/D/E/F 类：本地行数口径与上游不一致，仅记录不触发同步
        if entry_type in ("C", "D", "E", "F"):
            name = matched_entry.get("name", "")
            if entry_type == "F" and name in ["daily_closing", "section_flow"]:
                gap_dates = get_f_class_gap_dates(conn, target_table)
                if gap_dates:
                    item["_local_version"] = local_ver
                    item["_sync_reason"] = "gap_fill"
                    item["_f_gap_dates"] = gap_dates
                    logger.warning(f"⚠️  {code} F类表发现 {len(gap_dates)} 天断档，触发自愈同步")
                    need_sync.append(item)
                    continue

            # 先用缓存快速判断
            cached_count = cached_row_counts.get(code, -1)
            if cached_count != upstream_count:
                try:
                    actual_count = conn.execute(
                        f'SELECT COUNT(*) FROM "{target_table}"'
                    ).fetchone()[0]
                except Exception:
                    actual_count = 0
                if actual_count != upstream_count:
                    logger.info(
                        f"ℹ️  {code}({entry_type}类) 行数偏差: "
                        f"上游{upstream_count} 本地{actual_count}（仅记录）"
                    )
            continue

        # B 类：全量快照，保留 row_count 校验（版本号可能不变但数据有变化）
        if entry_type == "B":
            # 有 extra_params 的 B 类（如 CURYEAR），本地只存过滤后数据
            # 与上游全量行数口径不一致，仅记录不触发同步
            if matched_entry.get("extra_params"):
                cached_count = cached_row_counts.get(code, -1)
                if cached_count != upstream_count:
                    try:
                        actual_count = conn.execute(
                            f'SELECT COUNT(*) FROM "{target_table}"'
                        ).fetchone()[0]
                    except Exception:
                        actual_count = 0
                    if actual_count != upstream_count:
                        logger.info(
                            f"ℹ️  {code}(B类/有过滤参数) 行数偏差: "
                            f"上游{upstream_count} 本地{actual_count}（仅记录）"
                        )
                continue

            # 检查本机是否只有 1 行（嵌套 root），代表结构性差异
            try:
                local_quick = conn.execute(
                    f'SELECT COUNT(*) FROM "{target_table}"'
                ).fetchone()[0]
            except Exception:
                local_quick = 0
            if local_quick <= 1 and upstream_count > 10:
                continue  # 嵌套 B 类，不比较

        # 先用缓存快速判断：缓存行数 == 上游行数 → 一致，跳过
        cached_count = cached_row_counts.get(code, -1)
        if cached_count == upstream_count:
            continue

        # 缓存不一致，查实际行数确认
        if not target_table:
            continue
        try:
            actual_count = conn.execute(
                f'SELECT COUNT(*) FROM "{target_table}"'
            ).fetchone()[0]
        except Exception:
            actual_count = 0

        if actual_count != upstream_count:
            item["_local_version"] = local_ver
            item["_sync_reason"] = "row_count"
            item["_upstream_rows"] = upstream_count
            item["_actual_rows"] = actual_count
    # 强制扫描所有日颗粒度表（泛化兼容车流、营收以及未来其他日维度表）
    try:
        all_entries = []
        if 'SYNC_TABLE' in globals():
            all_entries.extend(SYNC_TABLE)
        if 'EXTRA_SYNC_TABLE' in globals():
            all_entries.extend(EXTRA_SYNC_TABLE)
            
        for entry in all_entries:
            f_table = entry.get("table")
            if not f_table: continue
            gap_dates = get_f_class_gap_dates(conn, f_table)
            if gap_dates:
                # 检查是否已在 need_sync 中
                already_in = any(i.get("目标表名") == f_table for i in need_sync)
                if not already_in:
                    table_title = entry.get("name", f_table)
                    logger.warning(f"⚠️  {table_title} (全日历接管) 发现 {len(gap_dates)} 天断档，强制创建同步任务")
                    virtual_item = {
                        "表编码": entry.get("name", f_table).upper(),
                        "目标表名": f_table,
                        "数据接口": entry.get("path", ""),
                        "_local_version": 0,
                        "变更版本": 1,
                        "_sync_reason": "gap_fill_forced",
                        "_f_gap_dates": gap_dates
                    }
                    need_sync.append(virtual_item)
    except Exception as e:
        logger.error(f"日维度强接管探测异常: {e}")

    return need_sync, True


# ============================================================
# 同步日志表
# ============================================================

def ensure_sync_log_table(conn: sqlite3.Connection):
    """确保 _SYNC_LOG 表存在"""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS _SYNC_LOG (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sync_state_id INTEGER,
            table_code TEXT,
            target_table TEXT,
            sync_type TEXT,
            change_version INTEGER,
            synced_version INTEGER,
            change_months TEXT,
            change_summary TEXT,
            rows_before INTEGER,
            rows_after INTEGER,
            rows_delta INTEGER,
            status TEXT,
            error_msg TEXT,
            started_at TEXT,
            finished_at TEXT,
            created_at TEXT DEFAULT (datetime('now', 'localtime'))
        )
    """)
    conn.commit()


def log_sync(conn: sqlite3.Connection, item: Dict, entry: Optional[Dict],
             status: str, rows_before: int = 0, rows_after: int = 0,
             error_msg: str = "", started_at: str = ""):
    """写入同步日志"""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn.execute("""
        INSERT INTO _SYNC_LOG
        (sync_state_id, table_code, target_table, sync_type,
         change_version, synced_version, change_months, change_summary,
         rows_before, rows_after, rows_delta, status, error_msg,
         started_at, finished_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        int(item.get("ID", 0)),
        item.get("表编码", ""),
        item.get("目标表名", ""),
        entry.get("type", "?") if entry else "UNMAPPED",
        int(item.get("变更版本", 0)),
        int(item.get("已同步版本", 0)),
        item.get("变更月份"),
        item.get("变更摘要"),
        rows_before,
        rows_after,
        rows_after - rows_before,
        status,
        error_msg,
        started_at,
        now,
    ))
    conn.commit()


# ============================================================
# 核心同步逻辑
# ============================================================

def get_table_row_count(conn: sqlite3.Connection, table: str) -> int:
    """获取表的行数（表不存在返回 0）"""
    try:
        return conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
    except Exception:
        return 0


def sync_one_table(conn: sqlite3.Connection, item: Dict, entry: Dict,
                   dry_run: bool = False) -> bool:
    """
    同步一张表。

    Args:
        conn: SQLite 连接
        item: sync-state API 返回的一条记录
        entry: SYNC_TABLE 中匹配的配置
        dry_run: 如果为 True，只输出诊断不实际执行

    Returns:
        True=同步成功并通过验证, False=失败或跳过
    """
    table_code = item.get("表编码", "")
    target_table = item.get("目标表名", "")
    stype = entry["type"]
    name = entry["name"]
    started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # 解析变更月份
    change_months = get_change_months_smart(item)
    change_summary = item.get("变更摘要", "")
    change_ver = int(item.get("变更版本", 0))
    synced_ver = int(item.get("已同步版本", 0))

    logger.info(f"\n{'='*60}")
    logger.info(f"📦 {table_code} → {name} ({stype}类)")
    sync_reason = item.get("_sync_reason", "version")
    if sync_reason == "row_count":
        logger.info(
            f"   触发原因: 行数不一致 (上游{item.get('_upstream_rows')} "
            f"本地{item.get('_actual_rows')})")
    else:
        logger.info(f"   变更版本: {synced_ver} → {change_ver}")
    logger.info(f"   变更月份: {item.get('变更月份', '无')}")
    logger.info(f"   变更摘要: {change_summary or '无'}")
    # 上游扩展字段信息
    cs = item.get("change_stats")
    if cs:
        logger.info(
            f"   变更统计: +{cs.get('insert_total', 0)} insert, "
            f"~{cs.get('update_total', 0)} update")

    if dry_run:
        logger.info(f"   [DRY-RUN] 跳过实际同步")
        return True

    rows_before = get_table_row_count(conn, target_table)

    try:
        expected = get_expected_month()

        if stype == "F":
            # F 类（日粒度）：DAILY_CLOSING / SECTION_FLOW 等
            dates = parse_change_dates(item.get("变更月份"))
            gap_dates = item.get("_f_gap_dates", [])
            # 即使不是从探测入口进的，常规触发时也动态探测一下防止漏挂
            if not gap_dates and name in ["daily_closing", "section_flow"]:
                gap_dates = get_f_class_gap_dates(conn, target_table)

            all_dates = sorted(list(set((dates or []) + gap_dates)))
            if not all_dates:
                # 兜底：昨日(T-1)，而非今天，避免拉取未完全生成的半截数据
                all_dates = [(datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")]
            
            logger.info(f"   日粒度智能拉取: {all_dates}")
            if name == "section_flow":
                added = sync_section_flow(conn, entry, all_dates)
            elif name == "serverpart_inc":
                # SERVERPART_INC 按节日类型×年份拉取，不需要日期参数
                added = sync_type_f_inc(conn, entry)
            else:
                added = sync_daily_closing(conn, entry, all_dates)
            logger.info(f"   → 新增 {added} 条")

        elif stype in ("A", "A_CHILDREN"):
            # 确定需要同步的月份范围
            if sync_reason == "row_count":
                # 行数不一致 → 按月逐月清理+重拉（不全量清表）
                expected_mo = get_expected_month()
                start_mo = entry.get("start_month", "202401")
                months = _month_range(start_mo, expected_mo)
                logger.info(f"   行数不一致，按月清理+重拉 {len(months)} 个月...")
            else:
                months = change_months or [expected]
            logger.info(f"   拉取月份: {months}")

            if stype == "A":
                added = sync_type_a(conn, entry, months, replace_mode=True)
            else:
                added = sync_type_a_children(conn, entry, months, replace_mode=True)
            logger.info(f"   → 新增 {added} 条")

        elif stype == "B":
            # 全量快照：清表重拉
            logger.info(f"   全量快照重拉...")
            added = sync_type_b(conn, entry, full_refresh=True)
            logger.info(f"   → 新增 {added} 条")

        elif stype == "C":
            # 服务区×月
            sp_ids = get_service_area_ids(conn)
            if not sp_ids:
                logger.warning(f"   ⚠️ 无服务区数据，跳过")
                log_sync(conn, item, entry, "skipped", rows_before, rows_before,
                         "无服务区数据", started_at)
                return False
            if sync_reason == "row_count":
                # 行数不一致 → 按月逐月清理+重拉（不全量清表）
                expected_mo = get_expected_month()
                start_mo = entry.get("start_month", "202401")
                months = _month_range(start_mo, expected_mo)
                logger.info(f"   行数不一致，按月清理+重拉 {len(months)} 月 × {len(sp_ids)} 服务区...")
            else:
                months = change_months or [expected]
            # 按月拉取（replace_mode=True 保证先拉后替换）
            logger.info(f"   拉取: {len(months)} 月 × {len(sp_ids)} 服务区")
            added = sync_type_c(conn, entry, months, sp_ids, replace_mode=True)
            logger.info(f"   → 新增 {added} 条")

        elif stype == "D":
            # 服务区全量
            sp_ids = get_service_area_ids(conn)
            if not sp_ids:
                logger.warning(f"   ⚠️ 无服务区数据，跳过")
                log_sync(conn, item, entry, "skipped", rows_before, rows_before,
                         "无服务区数据", started_at)
                return False
            logger.info(f"   全量重拉 {len(sp_ids)} 服务区...")
            added = sync_type_d(conn, entry, sp_ids, full_refresh=True)
            logger.info(f"   → 新增 {added} 条")

        elif stype == "E":
            # 枚举×月
            if sync_reason == "row_count":
                # 行数不一致 → 按月逐月清理+重拉（不全量清表）
                expected_mo = get_expected_month()
                start_mo = entry.get("start_month", "202401")
                months = _month_range(start_mo, expected_mo)
                logger.info(f"   行数不一致，按月清理+重拉 {len(months)} 个月...")
            else:
                months = change_months or [expected]
            # 按月拉取（replace_mode=True 保证先拉后替换）
            added = sync_type_e(conn, entry, months, replace_mode=True)
            logger.info(f"   → 新增 {added} 条")

        else:
            logger.warning(f"   ⚠️ 未知类型 {stype}，跳过")
            log_sync(conn, item, entry, "skipped", rows_before, rows_before,
                     f"未知类型 {stype}", started_at)
            return False

        # 展平联动：revenue_report_v2 更新后自动展平门店表
        if name == "revenue_report_v2":
            months_to_flatten = change_months or None
            logger.info(f"   展平门店数据...")
            flat_count = flatten_revenue_report_shops(conn, months_to_flatten)
            logger.info(f"   → 展平 {flat_count} 条门店记录")

        rows_after = get_table_row_count(conn, target_table)

        # 验证
        logger.info(f"   验证中...")
        issues = verify_all(conn, expected, [entry])

        if not issues:
            logger.info(f"   ✅ 验证通过 (行数: {rows_before} → {rows_after})")
            log_sync(conn, item, entry, "success", rows_before, rows_after,
                     started_at=started_at)
            return True
        else:
            issue_text = "; ".join(issues)
            # 区分致命 vs 非致命问题
            critical = any(tag in i for i in issues for tag in ["[空表]", "[缺表]"])
            if critical:
                logger.error(
                    f"   🛑 验证发现致命问题: {issue_text}\n"
                    f"   不通知上游 reset，下次会重试同步"
                )
                log_sync(conn, item, entry, "failed_verification",
                         rows_before, rows_after, issue_text, started_at)
                return False  # 不 reset 上游，不更新本地版本号
            else:
                logger.warning(f"   ⚠️ 验证有问题但非致命: {issue_text}")
                log_sync(conn, item, entry, "success_with_warnings",
                         rows_before, rows_after, issue_text, started_at)
                return True

    except Exception as e:
        logger.error(f"   ❌ [CRITICAL] 同步异常: {e}", exc_info=True)
        rows_after = get_table_row_count(conn, target_table)
        log_sync(conn, item, entry, "failed", rows_before, rows_after,
                 str(e), started_at)
        return False


# ============================================================
# 命令行功能
# ============================================================

def cmd_dry_run(table_code: Optional[str] = None):
    """dry-run：通过版本对比查看哪些表需要同步"""
    logger.info("🔍 对比本地 vs 上游版本...\n")

    conn = get_connection()
    ensure_sync_state_table(conn)
    items, upstream_ok = get_need_sync_by_version(conn, table_code)
    conn.close()

    if not upstream_ok:
        logger.warning("⚠️ 上游 sync-state API 不可达，无法判断是否需要同步。")
        return

    if not items:
        logger.info("✅ 所有表版本一致，无需同步。")
        return

    logger.info(f"📋 发现 {len(items)} 张表需要同步:\n")

    for item in items:
        table_code_val = item.get("表编码", "")
        target_table = item.get("目标表名", "")
        entry = match_entry(item)

        matched = f"→ {entry['name']} ({entry['type']}类)" if entry else "⚠️ 未映射"
        change = item.get("变更摘要", "无摘要")
        local_ver = item.get("_local_version", 0)
        upstream_ver = int(item.get("变更版本", 0))

        logger.info(f"  [{table_code_val}] {target_table}")
        logger.info(f"    映射: {matched}")
        logger.info(f"    版本: 本地 v{local_ver} → 上游 v{upstream_ver}")
        logger.info(f"    变更: {change}")
        logger.info(f"    月份: {item.get('变更月份', '无')}")
        # 上游扩展字段
        domain = item.get("business_domain", "")
        grain = item.get("refresh_grain", "")
        if domain or grain:
            logger.info(f"    业务域: {domain} | 粒度: {grain}")
        cs = item.get("change_stats")
        if cs:
            logger.info(
                f"    变更统计: +{cs.get('insert_total', 0)} insert, "
                f"~{cs.get('update_total', 0)} update")
        logger.info("")


def cmd_show_log():
    """显示本地同步日志"""
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT table_code, target_table, sync_type, status,
                   rows_delta, change_summary, finished_at
            FROM _SYNC_LOG
            ORDER BY id DESC
            LIMIT 20
        """).fetchall()
    except Exception:
        logger.info("_SYNC_LOG 表不存在，尚未执行过自动同步。")
        conn.close()
        return

    if not rows:
        logger.info("暂无同步日志。")
        conn.close()
        return

    logger.info(f"最近 {len(rows)} 条同步日志:\n")
    logger.info(f"{'表编码':<25} {'类型':>4} {'状态':<20} {'行数变化':>8} {'时间':<20} {'摘要'}")
    logger.info("-" * 110)
    for r in rows:
        code, table, stype, status, delta, summary, finished = r
        delta_str = f"+{delta}" if (delta or 0) >= 0 else str(delta)
        logger.info(f"{code or '':<25} {stype or '':>4} {status or '':<20} "
                    f"{delta_str:>8} {(finished or ''):<20} {(summary or '')[:40]}")

    conn.close()


def cmd_sync(table_code: Optional[str] = None, dry_run: bool = False):
    """执行自动同步（基于本地 vs 上游版本对比）"""
    # 全局互斥锁：防止 cron/手动/Z巡检 并发写 SQLite
    sync_lock_fd = open(_SYNC_LOCK, "w")
    try:
        fcntl.flock(sync_lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError:
        logger.info("⏭️  另一个 auto_sync 正在运行，跳过本次执行")
        sync_lock_fd.close()
        return

    try:
        _cmd_sync_inner(table_code, dry_run)
    finally:
        fcntl.flock(sync_lock_fd, fcntl.LOCK_UN)
        sync_lock_fd.close()


def _cmd_sync_inner(table_code: Optional[str] = None, dry_run: bool = False):
    """cmd_sync 的实际逻辑（已持有全局锁）"""
    start_time = time.time()

    logger.info("🚀 事件驱动数据同步")
    logger.info(f"   数据库: {DB_PATH}")
    logger.info(f"   sync-state API: {SYNC_STATE_API}")
    logger.info("")

    # 1. 连接数据库
    conn = get_connection()
    ensure_sync_state_table(conn)
    ensure_sync_log_table(conn)

    # 2. 通过版本对比找出需要同步的表
    items, upstream_ok = get_need_sync_by_version(conn, table_code)
    if not upstream_ok:
        logger.warning("⚠️ 上游 sync-state API 不可达，无法判断是否需要同步。")
        conn.close()
        return
    if not items:
        logger.info("✅ 所有表版本一致，无需同步。")
        conn.close()
        return

    logger.info(f"📋 发现 {len(items)} 张表需要同步")

    # 3. 逐表处理
    success_count = 0
    fail_count = 0
    skip_count = 0
    synced_tables = []  # 收集成功同步的表名，用于选择性缓存重建

    for item in items:
        entry = match_entry(item)
        if not entry:
            table_code_val = item.get("表编码", "?")
            logger.info(f"\n⏭️  跳过未映射的表: {table_code_val} ({item.get('目标表名', '')})")
            log_sync(conn, item, None, "skipped", error_msg="未映射",
                     started_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            skip_count += 1
            continue

        ok = sync_one_table(conn, item, entry, dry_run=dry_run)

        if dry_run:
            continue

        if ok:
            # 同步成功，更新本地版本号
            update_local_version(conn, item)
            logger.info(f"  💾 本地版本已更新到 v{int(item.get('变更版本', 0))}")
            # 同时通知上游 reset（携带 ack_change_version 防竞态）
            sync_id = int(item.get("ID", 0))
            if sync_id > 0:
                reset_sync_state(
                    sync_id,
                    ack_version=int(item.get("变更版本", 0)),
                    ack_api=item.get("ack_api"),
                )
            success_count += 1
            synced_tables.append(entry["name"])
        else:
            fail_count += 1

    conn.close()

    # 4. 总结
    elapsed = round(time.time() - start_time, 1)
    logger.info(f"\n{'='*60}")
    logger.info(f"📊 同步完成 ({elapsed}s)")
    logger.info(f"   成功: {success_count}  失败: {fail_count}  跳过: {skip_count}")
    logger.info(f"{'='*60}")

    # 5. 数据有变更 → 按依赖关系选择性重建缓存
    if synced_tables and not dry_run:
        post_sync_rebuild(synced_tables)


# ============================================================
# Post-sync 缓存选择性重建
# ============================================================

# 文件锁路径
_SYNC_LOCK = Path("/tmp/ai_python_auto_sync.lock")        # 全局互斥：防止多个 auto_sync 并发写 SQLite
_REBUILD_LOCK = Path("/tmp/ai_python_cache_rebuild.lock")  # 缓存重建互斥

# 表名 → 需要重建的缓存标签
# 不在此映射中的表（如 daily_closing）= 不触发任何缓存重建
_TABLE_CACHE_DEPS = {
    # 营收类 → 经营诊断 + 基准 + 画像
    "revenue":              {"conclusions", "benchmarks", "insights"},
    "revenue_report":       {"conclusions", "benchmarks", "insights"},
    "revenue_report_v2":    {"conclusions", "benchmarks", "insights"},
    "revenue_qoq":          {"conclusions", "insights"},
    "per_car_value":        {"insights"},
    "revenue_ranking":      {"conclusions", "benchmarks"},
    "revenue_recognition":  {"insights"},
    # 车流类
    "traffic":              {"conclusions", "benchmarks", "insights"},
    "traffic_ranking":      {"conclusions", "benchmarks"},
    "traffic_warning":      {"conclusions"},
    # 商户/风险类 → 风险诊断 + 画像
    "project_risk":         {"conclusions", "insights"},
    "project_warning":      {"conclusions", "insights"},
    "merchant_profitability": {"conclusions", "insights"},
    "merchant_profit_loss": {"conclusions", "insights"},
    "merchant_shops":       {"insights"},
    "business_trade":       {"conclusions", "insights"},
    "business":             {"insights"},  # 门店经营明细（service_area + region 画像）
    # 合同/财务类
    "contract_merchant":    {"conclusions", "insights"},
    "account_reached":      {"conclusions"},
    "bank_payment":         {"insights"},
    "finance":              {"insights"},
    # 品牌/业态类
    "brand_ranking":        {"conclusions"},
    "investment":           {"insights"},
    # 客群类
    "transaction_customer": {"insights"},
    "customer_age_ratio":   {"insights"},
    "customer_sale_ratio":  {"insights"},
    "vehicle_ownership":    {"insights"},
    # SABFI
    "sabfi":                {"conclusions", "insights"},
    # 基础数据
    "basic_info":           {"conclusions", "benchmarks", "insights"},
    "border_service_area":  {"insights"},
    # 仪表盘类 → 基准
    "dashboard_overview":   {"benchmarks"},
    "dashboard_traffic":    {"benchmarks"},
    "dashboard_revenue":    {"benchmarks"},
    # 日结 → 每日简报
    "daily_closing":        {"daily_briefing"},
    # 以下表不触发缓存重建:
    # commodity_sale_summary, asset_efficiency,
    # dashboard_transaction, user_list
}

# 缓存标签 → 重建命令
_CACHE_COMMANDS = {
    "conclusions": {
        "name": "经营诊断+风险缓存",
        "cmd": [sys.executable, "manage.py", "refresh_conclusions", "--domain", "all"],
        "timeout": 120,
    },
    "benchmarks": {
        "name": "全省基准缓存",
        "cmd": [sys.executable, "manage.py", "refresh_benchmarks"],
        "timeout": 60,
    },
    "dimensions": {
        "name": "维度年度画像",
        "cmd": [sys.executable, "manage.py", "refresh_dimensions"],
        "timeout": 300,  # 60→300：实际耗时可达 2-3 分钟
    },
    # insights 已解耦到独立 cron 任务（避免阻塞 auto_sync + database locked）
    # cron 示例: 0 4 * * * cd ~/AI-Python && .../python manage.py generate_insights --workers 1
}


def post_sync_rebuild(synced_tables: list):
    """
    数据同步成功后按依赖关系选择性重建缓存。

    Args:
        synced_tables: 成功同步的表 name 列表（如 ['revenue', 'traffic']）

    逻辑:
        1. 根据 _TABLE_CACHE_DEPS 映射，计算需要重建的缓存标签集合
        2. 只执行受影响的缓存重建命令
        3. 文件锁防并发
    """
    # 计算需要重建的缓存
    needed_caches = set()
    for table_name in synced_tables:
        deps = _TABLE_CACHE_DEPS.get(table_name, set())
        needed_caches.update(deps)

    # dimensions 始终跟着 conclusions 一起（共享数据源）
    if "conclusions" in needed_caches:
        needed_caches.add("dimensions")

    if not needed_caches:
        logger.info(
            f"⏭️  已同步表 {synced_tables} 不影响任何缓存，跳过重建"
        )
        return

    lock_fd = None
    try:
        # 尝试获取文件锁（非阻塞）
        lock_fd = open(_REBUILD_LOCK, "w")
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except IOError:
            logger.info("⏭️  缓存重建已在进行中（文件锁），跳过")
            lock_fd.close()
            return

        logger.info(f"\n{'='*60}")
        logger.info("🔄 Post-sync 缓存选择性重建")
        logger.info(f"   已同步表: {synced_tables}")
        logger.info(f"   需重建: {sorted(needed_caches)}")
        logger.info(f"{'='*60}")

        total_start = time.time()
        results = []

        # 按固定顺序执行（conclusions → benchmarks → dimensions）
        # insights 已解耦到独立 cron 任务
        for cache_key in ["conclusions", "benchmarks", "dimensions"]:
            if cache_key not in needed_caches:
                continue

            item = _CACHE_COMMANDS[cache_key]
            name = item["name"]
            cmd = item["cmd"]
            timeout = item["timeout"]

            logger.info(f"\n  🔨 {name}...")
            start = time.time()

            try:
                proc = subprocess.run(
                    cmd,
                    cwd=str(PROJECT_ROOT),
                    capture_output=True,
                    text=True,
                    timeout=timeout,
                    env={
                        **dict(__import__("os").environ),
                        "DJANGO_SETTINGS_MODULE": "config.settings",
                    },
                )
                elapsed = round(time.time() - start, 1)

                if proc.returncode == 0:
                    logger.info(f"  ✅ {name} 完成 ({elapsed}s)")
                    results.append((name, "success", elapsed))
                else:
                    err = (proc.stderr or "")[-200:]
                    logger.warning(
                        f"  ❌ {name} 失败 (exit={proc.returncode}, "
                        f"{elapsed}s): {err}"
                    )
                    results.append((name, "failed", elapsed))

            except subprocess.TimeoutExpired:
                elapsed = round(time.time() - start, 1)
                logger.warning(f"  ⏰ {name} 超时 ({timeout}s)")
                results.append((name, "timeout", elapsed))

            except Exception as e:
                elapsed = round(time.time() - start, 1)
                logger.error(f"  ❌ {name} 异常: {e}")
                results.append((name, "error", elapsed))

        total_elapsed = round(time.time() - total_start, 1)

        success = sum(1 for _, s, _ in results if s == "success")
        logger.info(f"\n{'='*60}")
        logger.info(
            f"🔄 缓存重建完成: {success}/{len(results)} 成功 "
            f"({total_elapsed}s)"
        )
        for name, status, elapsed in results:
            icon = "✅" if status == "success" else "❌"
            logger.info(f"   {icon} {name}: {status} ({elapsed}s)")
        logger.info(f"{'='*60}")

    except Exception as e:
        logger.error(f"Post-sync 缓存重建异常: {e}")

    finally:
        if lock_fd:
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
                lock_fd.close()
            except Exception:
                pass


# ============================================================
# 主入口
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="事件驱动数据同步（基于 sync-state API）")
    parser.add_argument("--dry-run", action="store_true",
                        help="只查看需要同步的表，不实际拉数据")
    parser.add_argument("--table-code", type=str, default=None,
                        help="只处理指定表编码（如 DAILY_CLOSING）")
    parser.add_argument("--show-log", action="store_true",
                        help="显示本地同步日志")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="详细输出")
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    if args.show_log:
        cmd_show_log()
        return

    if args.dry_run:
        cmd_dry_run(args.table_code)
        return

    cmd_sync(args.table_code)


if __name__ == "__main__":
    main()
