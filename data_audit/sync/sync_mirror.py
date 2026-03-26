#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQLite 镜像数据同步脚本

从远端 DataAPI 拉取数据到本地 dameng_mirror.db，支持增量同步和全量刷新。
所有拉取规则（6 类分组 + 3 个特殊表）都已编码在脚本中，无需手工记忆。

用法:
    # 诊断模式：查看哪些月份缺失，确认后再拉
    python data_audit/sync/sync_mirror.py

    # 拉指定月份（增量，跳过已有数据）
    python data_audit/sync/sync_mirror.py --month 202604

    # 拉指定月份（清理旧数据后重拉，适用于数据有误需要重建）
    python data_audit/sync/sync_mirror.py --month 202604 --clean

    # 只拉指定表（endpoint 名）
    python data_audit/sync/sync_mirror.py --endpoint revenue --month 202604

    # 只跑验证不拉数据
    python data_audit/sync/sync_mirror.py --verify-only

    # 全量刷新（B/D 类全量表会清表重拉）
    python data_audit/sync/sync_mirror.py --full-refresh

    # 自动模式：跳过确认直接执行（给 AI agent 用）
    python data_audit/sync/sync_mirror.py --yes

依赖: requests
"""

import argparse
import json
import logging
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests

# 项目路径（data_audit/sync/sync_mirror.py → 上溯 1 级到 data_audit 目录）
DATA_AUDIT_DIR = Path(__file__).resolve().parent.parent
DB_PATH = DATA_AUDIT_DIR / "data" / "dameng_mirror.db"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("sync_mirror")

REMOTE_BASE = "http://111.229.213.193:18071/api/dynamic"
REQUEST_TIMEOUT = 20
MAX_RETRIES = 3

# ============================================================
# 表分类配置
# ============================================================
# 每个 entry: (endpoint_name, api_path, table_name, sync_type)
# sync_type:
#   A = 省级月度（逐月 GET）
#   B = 全量快照（不带月份，直接 GET）
#   C = 服务区×月（遍历 133 个服务区 × 逐月）
#   D = 服务区全量（遍历 133 个服务区 × 1 次）
#   E = 枚举×月（遍历枚举值 × 逐月）
#   SKIP = 不纳入 SQLite

SYNC_TABLE: List[Dict] = [
    # ---- A 类（省级月度）----
    {"name": "revenue",               "path": "/newGetMonthINCAnalysis/",        "table": "NEWGETMONTHINCANALYSIS",        "type": "A"},
    {"name": "revenue_report",        "path": "/GetMonthlyBusinessAnalysis/",    "table": "NEWGETMONTHLYBUSINESSANALYSIS", "type": "A",
     "aliases": ["business"], "start_month": "202406"},
    {"name": "revenue_qoq",           "path": "/GetRevenueQOQ/",                 "table": "NEWGETREVENUEQOQ",              "type": "A",
     "start_month": "202501"},
    {"name": "revenue_report_v2",     "path": "/GetRevenueReport/",              "table": "NEWGETREVENUEREPORT",           "type": "A",
     "start_month": "202501"},
    {"name": "per_car_value",         "path": "/GetRevenueEstimateList/",        "table": "NEWGETREVENUEESTIMATELIST",     "type": "A",
     "month_param": "StatisticsMonth"},
    {"name": "traffic",               "path": "/GetProvinceVehicleTreeList/",    "table": "NEWGETBAYONETOWNERAHTREELIST",  "type": "A"},
    {"name": "asset_efficiency",      "path": "/GetASSETSPROFITSTreeList/",      "table": "NEWGETASSETSPROFITSTREELIST",   "type": "A"},
    {"name": "sabfi",                 "path": "/GetShopSABFIList/",              "table": "NEWGETSHOPSABFILIST",           "type": "A",
     "start_month": "202407"},
    {"name": "finance",               "path": "/GetContractExcuteAnalysis/",     "table": "NEWGETCONTRACTEXCUTEANALYSIS",  "type": "A",
     "aliases": ["contract_analysis"]},
    {"name": "bank_payment",          "path": "/GetMobilePayRoyaltyReport/",     "table": "NEWGETMOBILEPAYROYALTYREPORT",  "type": "A",
     "start_month": "202501"},
    {"name": "dashboard_overview",    "path": "/ahydDIBData/",                   "table": "NEWGETSUMMARYREVENUEMONTH",     "type": "A",
     "aliases": ["summary_revenue_month"]},
    {"name": "dashboard_traffic",     "path": "/NEWGetProvinceMonthAnalysis/",   "table": "NEWGETPROVINCEMONTHANALYSIS",   "type": "A",
     "aliases": ["province_month"]},
    {"name": "dashboard_transaction", "path": "/NEWGETTRANSACTIONANALYSIS/",     "table": "NEWGETTRANSACTIONANALYSIS",     "type": "A",
     "aliases": ["transaction_analysis"]},
    {"name": "dashboard_revenue",     "path": "/NEWGETSUMMARYREVENUE/",          "table": "NEWGETSUMMARYREVENUE",          "type": "A",
     "aliases": ["summary_revenue"]},
    {"name": "transaction_customer",  "path": "/GetTransactionCustomer/",        "table": "NEWGETTRANSACTIONCUSTOMER",     "type": "A",
     "start_month": "202406"},
    {"name": "commodity_sale_summary","path": "/GetCommoditySaleSummary/",       "table": "NEWGETCOMMODITYSALESUMMARY",    "type": "A",
     "extra_params": {"DATATYPE": "2"}},

    # ---- B 类（全量快照）----
    {"name": "basic_info",            "path": "/GetServerpartList/",             "table": "NEWGETSERVERPARTLIST",          "type": "B",
     "aliases": ["serverpart_list"]},
    {"name": "investment",            "path": "/GetBusinessAnalysisReport/",     "table": "NEWGETBUSINESSANALYSISREPORT",  "type": "B",
     "aliases": ["business_report"]},
    {"name": "merchant_profitability","path": "/NEWMERCHANTPROFITABILITY/",      "table": "NEWMERCHANTPROFITABILITY",      "type": "B"},
    {"name": "business_trade",        "path": "/NEWBUSINESSTRADELIST/",          "table": "NEWBUSINESSTRADELIST",          "type": "B"},
    {"name": "project_risk",          "path": "/projectManagementRisk/",         "table": "NEWGETACCOUNTWARNINGLIST_RISK", "type": "B"},
    {"name": "project_profitability", "path": "/PROJECTPROFITABILITY/",          "table": "NEWPROJECTPROFITABILITY",       "type": "B"},
    {"name": "brand_ranking",         "path": "/NEWBrandRanking/",               "table": "NEWBRANDRANKING",               "type": "B",
     "extra_params": {"CURYEAR": "multi_year"}},
    {"name": "revenue_ranking",       "path": "/NEWREVENUERANKING/",             "table": "NEWREVENUERANKING",             "type": "B",
     "extra_params": {"CURYEAR": "multi_year"}},
    {"name": "traffic_ranking",       "path": "/NEWTRAFFICFLOWRANKING/",         "table": "NEWTRAFFICFLOWRANKING",         "type": "B",
     "extra_params": {"CURYEAR": "multi_year"}},
    {"name": "merchant_shops",        "path": "/NEWGetMerchantSplit/",           "table": "NEWGETMERCHANTSPLIT",           "type": "B"},
    {"name": "user_list",             "path": "/GetUSERList/",                   "table": "NEWGETUSERLIST",                "type": "B"},

    # ---- C 类（服务区×月）----
    {"name": "revenue_recognition",   "path": "/GetRevenueRecognition/",         "table": "NEWGETREVENUERECOGNITION",      "type": "C",
     "start_month": "202501"},
    {"name": "account_reached",       "path": "/GetAccountReached/",             "table": "NEWGETACCOUNTREACHED",          "type": "C",
     "start_month": "202501"},
    {"name": "contract_merchant",     "path": "/GetContractMerchant/",           "table": "NEWGETCONTRACTMERCHANT",        "type": "C",
     "start_month": "202501"},
    {"name": "customer_age_ratio",    "path": "/GetCustomerAgeRatio/",           "table": "GETCUSTOMERAGERATIO",           "type": "C",
     "sp_param": "SERVERPART_ID"},
    {"name": "customer_sale_ratio",   "path": "/GetCustomerSaleRatio/",          "table": "GETCUSTOMERCONSUMERATIO",       "type": "C",
     "sp_param": "SERVERPART_ID"},

    # ---- D 类（服务区全量）----
    {"name": "merchant_profit_loss",  "path": "/GetPeriodWarningList/",          "table": "NEWGETPERIODWARNINGLIST",       "type": "D",
     "aliases": ["period_warning"]},
    {"name": "project_warning",       "path": "/GetAccountWarningList/",         "table": "NEWGETACCOUNTWARNINGLIST",      "type": "D",
     "sp_param": "SERVERPART_ID", "aliases": ["account_warning"]},
    {"name": "border_service_area",   "path": "/NEWBORDERSERVICEPART/",          "table": "NEWBORDERSERVICEPART",          "type": "D",
     "sp_param": "SERVERPART_ID"},

    # ---- E 类（枚举×月）----
    {"name": "traffic_warning",       "path": "/VEHICLEFLOWWARNING/",            "table": "NEWVEHICLEFLOWWARNING",         "type": "E",
     "enum_param": "warningType", "enum_column": "WARNING_TYPE", "enum_values": [1, 2, 3, 4]},

    # ---- F 类（节日枚举×年份，展开 children + 嵌套 dict 展平）----
    {"name": "holiday_detail",        "path": "/NEWGETHOLIDAYANALYSIS/",         "table": "HOLIDAY_DAILY_DETAIL",          "type": "F",
     "holiday_types": [1, 2, 3, 4, 5, 6, 8],  # 跳过7=中秋（上游明细层无数据）
     "years": [2024, 2025, 2026]},
    {"name": "serverpart_inc",        "path": "/GetServerpartINCAnalysis/",      "table": "NEWGETSERVERPARTINCANALYSIS",   "type": "F",
     "holiday_types": [1, 2, 3, 4, 5, 6, 8],  # 跳过7=中秋（上游无数据）
     "years": [2024, 2025, 2026]},

    # ---- 特殊：vehicle_ownership（省级月度，但要展开 children）----
    {"name": "vehicle_ownership",     "path": "/NEWVEHICLEOWNERSHIPLOCATION/",   "table": "NEWVEHICLEOWNERSHIPLOCATION",   "type": "A_CHILDREN",
     "start_month": "202501"},
]


# ============================================================
# 核心函数
# ============================================================

def get_connection() -> sqlite3.Connection:
    """获取 SQLite 连接"""
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def get_service_area_ids(conn: sqlite3.Connection) -> List[str]:
    """从 NEWGETSERVERPARTLIST 获取所有服务区内码（表不存在时安全返回空列表）"""
    try:
        rows = conn.execute("SELECT 服务区内码 FROM NEWGETSERVERPARTLIST").fetchall()
        return [str(r[0]) for r in rows]
    except Exception:
        return []


def get_expected_month() -> str:
    """获取当前应覆盖到的月份（上个自然月）"""
    now = datetime.now()
    y, m = (now.year, now.month - 1) if now.month > 1 else (now.year - 1, 12)
    return f"{y}{m:02d}"


def _month_range(start_month: str, end_month: str) -> List[str]:
    """生成闭区间月份序列（YYYYMM）"""
    months = []
    y, m = int(start_month[:4]), int(start_month[4:])
    end_y, end_m = int(end_month[:4]), int(end_month[4:])
    while y * 100 + m <= end_y * 100 + end_m:
        months.append(f"{y}{m:02d}")
        m += 1
        if m > 12:
            y += 1
            m = 1
    return months


def _normalize_month_value(value) -> Optional[str]:
    """将 SQLite 中的月份值统一规整成 YYYYMM 字符串"""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith(".0"):
        text = text[:-2]
    return text if len(text) == 6 and text.isdigit() else None


def get_missing_months(conn: sqlite3.Connection, entry: Dict, expected: str) -> List[str]:
    """获取某表缺失的月份列表"""
    table = entry["table"]
    try:
        cols = [r[1] for r in conn.execute(f'PRAGMA table_info("{table}")').fetchall()]
        if "STATISTICS_MONTH" not in cols:
            return []  # 全量表无需补月
        existing = set(
            _normalize_month_value(r[0]) for r in conn.execute(
            f'SELECT DISTINCT STATISTICS_MONTH FROM "{table}" WHERE STATISTICS_MONTH IS NOT NULL'
        ).fetchall()
        )
        existing.discard(None)

        min_existing = conn.execute(
            f'SELECT MIN(STATISTICS_MONTH) FROM "{table}" WHERE STATISTICS_MONTH IS NOT NULL'
        ).fetchone()[0]
        start_month = (
            _normalize_month_value(min_existing)
            or entry.get("start_month")
            or "202401"
        )
        all_months = _month_range(start_month, expected)
        return [mo for mo in all_months if mo not in existing]
    except Exception:
        return []


def build_month_params(entry: Dict, month: str) -> Dict[str, str]:
    """构造月份参数，兼容 StatisticsMonth 单参数接口"""
    if entry.get("month_param") == "StatisticsMonth":
        return {"StatisticsMonth": month}
    return {"StatisticsStartMonth": month, "StatisticsEndMonth": month}


def get_server_key(conn: sqlite3.Connection, table: str) -> Optional[str]:
    """返回表中可用的服务区键列名"""
    cols = get_table_columns(conn, table)
    for key in ("_SERVERPART_ID", "服务区内码", "服务区Id", "SERVERPART_ID"):
        if key in cols:
            return key
    return None


def has_month_server_data(conn: sqlite3.Connection, table: str, month: str, sp_id: str) -> bool:
    """判断某表在指定月份/服务区是否已有数据；month 为空时只按服务区判断"""
    key = get_server_key(conn, table)
    if not key:
        return False
    cols = get_table_columns(conn, table)
    if month and "STATISTICS_MONTH" in cols:
        cnt = conn.execute(
            f'SELECT COUNT(*) FROM "{table}" WHERE STATISTICS_MONTH = ? AND "{key}" = ?',
            (month, sp_id)
        ).fetchone()[0]
    else:
        cnt = conn.execute(
            f'SELECT COUNT(*) FROM "{table}" WHERE "{key}" = ?',
            (sp_id,)
        ).fetchone()[0]
    return cnt > 0


def api_get(path: str, params: Dict, retries: int = MAX_RETRIES) -> Optional[Dict]:
    """带重试的 API 调用"""
    url = f"{REMOTE_BASE}{path}"
    for attempt in range(retries):
        try:
            resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200:
                return resp.json()
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(1)
                continue
            logger.warning(f"API 失败 {path} params={params}: {e}")
    return None


def extract_items(resp: Dict) -> List[Dict]:
    """从 API 响应中提取数据列表"""
    if not resp:
        return []
    rd = resp.get("Result_Data", {})
    if rd is None:
        return []
    items = rd.get("List", [])
    return items if isinstance(items, list) else []


def ensure_columns(conn: sqlite3.Connection, table: str, row: Dict, known_cols: set):
    """确保表有所有需要的列"""
    for col in row.keys():
        if col not in known_cols:
            try:
                conn.execute(f'ALTER TABLE "{table}" ADD COLUMN "{col}" TEXT')
                known_cols.add(col)
            except Exception:
                pass


def insert_row(conn: sqlite3.Connection, table: str, row: Dict):
    """插入一行数据"""
    cols = list(row.keys())
    col_str = ", ".join([f'"{c}"' for c in cols])
    ph = ", ".join(["?" for _ in cols])
    vals = []
    for c in cols:
        v = row[c]
        if isinstance(v, (dict, list)):
            vals.append(json.dumps(v, ensure_ascii=False))
        elif v is not None:
            vals.append(str(v))
        else:
            vals.append(None)
    conn.execute(f'INSERT INTO "{table}" ({col_str}) VALUES ({ph})', vals)


def get_table_columns(conn: sqlite3.Connection, table: str) -> set:
    """获取表的列名集合"""
    return set(r[1] for r in conn.execute(f'PRAGMA table_info("{table}")').fetchall())


# ============================================================
# 按类型拉取
# ============================================================

def sync_type_a(conn: sqlite3.Connection, entry: Dict, months: List[str],
               replace_mode: bool = False):
    """A 类：省级月度，逐月 GET

    Args:
        replace_mode: True 时先拉数据，有数据才删旧插新（安全模式）
    """
    table = entry["table"]
    path = entry["path"]
    cols = get_table_columns(conn, table)
    added = 0
    for month in months:
        # 非替换模式下：检查已有数据则跳过
        if not replace_mode and "STATISTICS_MONTH" in cols:
            existing = conn.execute(
                f'SELECT COUNT(*) FROM "{table}" WHERE STATISTICS_MONTH = ?', (month,)
            ).fetchone()[0]
            if existing > 0:
                continue
        params = build_month_params(entry, month)
        params.update(entry.get("extra_params", {}))  # 支持额外固定参数（如 DATATYPE）
        resp = api_get(path, params)
        items = extract_items(resp)
        if not items:
            if replace_mode:
                logger.warning(f"  ⚠️ {month}: API 返回空，保留旧数据")
            continue
        # 有新数据了，安全删旧
        if replace_mode and "STATISTICS_MONTH" in cols:
            conn.execute(
                f'DELETE FROM "{table}" WHERE STATISTICS_MONTH = ?', (month,)
            )
        for row in items:
            row["STATISTICS_MONTH"] = month
            ensure_columns(conn, table, row, cols)
            insert_row(conn, table, row)
        added += len(items)
        conn.commit()
    return added


def sync_type_a_children(conn: sqlite3.Connection, entry: Dict, months: List[str],
                         replace_mode: bool = False):
    """A_CHILDREN 类：省级月度但要展开 children（如 vehicle_ownership）"""
    table = entry["table"]
    path = entry["path"]
    cols = get_table_columns(conn, table)
    added = 0
    for month in months:
        # 非替换模式下：检查已有数据则跳过
        if not replace_mode and "STATISTICS_MONTH" in cols:
            existing = conn.execute(
                f'SELECT COUNT(*) FROM "{table}" WHERE STATISTICS_MONTH = ?', (month,)
            ).fetchone()[0]
            if existing > 0:
                continue
        resp = api_get(path, build_month_params(entry, month))
        items = extract_items(resp)
        if not items:
            if replace_mode:
                logger.warning(f"  ⚠️ {month}: API 返回空，保留旧数据")
            continue
        # 有新数据了，安全删旧
        if replace_mode and "STATISTICS_MONTH" in cols:
            conn.execute(
                f'DELETE FROM "{table}" WHERE STATISTICS_MONTH = ?', (month,)
            )
        # 顶层只有 1 条全省汇总，真数据在 children 里
        top = items[0]
        children = top.get("children", [])

        # 先存省级汇总行
        top_row = dict(top)
        top_row.pop("children", None)
        top_row["STATISTICS_MONTH"] = month
        top_row["_SERVERPART_ID"] = "0"
        ensure_columns(conn, table, top_row, cols)
        insert_row(conn, table, top_row)
        added += 1

        # 再展开服务区 children
        for child in children or []:
            child["STATISTICS_MONTH"] = month
            sp_id = child.get("服务区内码", child.get("servierpart_id", ""))
            if sp_id:
                child["_SERVERPART_ID"] = str(sp_id)
            ensure_columns(conn, table, child, cols)
            insert_row(conn, table, child)
        added += len(children or [])
        conn.commit()
    return added


def sync_type_b(conn: sqlite3.Connection, entry: Dict, full_refresh: bool = False):
    """B 类：全量快照（安全模式：先拉数据，确认有效后再清表）

    支持 extra_params 特殊值：
    - dynamic_year: 自动替换为当前年份（单年拉取）
    - multi_year: 展开为 [去年, 今年] 分别拉取并合并（双年拉取）
    """
    table = entry["table"]
    path = entry["path"]
    if not full_refresh:
        cnt = conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
        if cnt > 0:
            return 0  # 已有数据，不重拉
    cols = get_table_columns(conn, table)
    # 支持额外固定参数（如 CURYEAR）
    params = dict(entry.get("extra_params", {}))
    # 检测是否有 multi_year 参数：需展开为多年分别拉取
    multi_year_keys = [k for k, v in params.items() if v == "multi_year"]
    if multi_year_keys:
        return _sync_type_b_multi_year(conn, entry, full_refresh, multi_year_keys)
    # dynamic_year: 自动替换为当前年份
    for k, v in list(params.items()):
        if v == "dynamic_year":
            params[k] = str(datetime.now().year)
    # 先拉数据，不动旧表
    resp = api_get(path, params)
    items = extract_items(resp)
    if not items:
        logger.warning(f"  ⚠️ API 返回空数据，保留现有 {table} 数据")
        return 0
    # 行数护栏：新数据不能比旧数据少 50% 以上
    if full_refresh:
        old_cnt = conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
        if old_cnt > 10 and len(items) < old_cnt * 0.5:
            logger.error(
                f"  🛑 行数护栏: API 返回 {len(items)} 行，"
                f"当前 {old_cnt} 行，差异过大，跳过同步"
            )
            return 0
        conn.execute(f'DELETE FROM "{table}"')
    # 数据已确认有效，安全写入
    for row in items:
        ensure_columns(conn, table, row, cols)
        insert_row(conn, table, row)
    conn.commit()
    return len(items)


def _sync_type_b_multi_year(conn: sqlite3.Connection, entry: Dict,
                            full_refresh: bool, year_keys: list) -> int:
    """B 类 multi_year 变体：展开为 [去年, 今年] 分别拉取并合并

    确保 full-refresh 不会丢失跨年数据。
    """
    table = entry["table"]
    path = entry["path"]
    cols = get_table_columns(conn, table)
    now_year = datetime.now().year
    years = [now_year - 1, now_year]  # 去年 + 今年

    all_items = []
    for year in years:
        params = dict(entry.get("extra_params", {}))
        for k in year_keys:
            params[k] = str(year)
        # 其余 dynamic_year 参数正常替换
        for k, v in list(params.items()):
            if v == "dynamic_year":
                params[k] = str(now_year)
        resp = api_get(path, params)
        items = extract_items(resp)
        if items:
            # 注入年份标记列，便于区分
            for row in items:
                for k in year_keys:
                    row[k] = str(year)
            all_items.extend(items)
            logger.info(f"  {table} YEAR={year}: {len(items)} 条")
        else:
            logger.warning(f"  ⚠️ {table} YEAR={year}: API 返回空")

    if not all_items:
        logger.warning(f"  ⚠️ 所有年份 API 返回空，保留现有 {table} 数据")
        return 0

    # 行数护栏
    if full_refresh:
        old_cnt = conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
        if old_cnt > 10 and len(all_items) < old_cnt * 0.5:
            logger.error(
                f"  🛑 行数护栏: multi_year 拉取 {len(all_items)} 行，"
                f"当前 {old_cnt} 行，差异过大，跳过同步"
            )
            return 0
        conn.execute(f'DELETE FROM "{table}"')

    for row in all_items:
        ensure_columns(conn, table, row, cols)
        insert_row(conn, table, row)
    conn.commit()
    return len(all_items)


def sync_type_c(conn: sqlite3.Connection, entry: Dict, months: List[str],
                sp_ids: List[str], replace_mode: bool = False):
    """C 类：服务区×月，遍历每个服务区"""
    table = entry["table"]
    path = entry["path"]
    sp_param = entry.get("sp_param", "ServerpartId")
    cols = get_table_columns(conn, table)
    added = 0
    for month in months:
        # 替换模式：先删该月所有数据，再拉取
        # 但需要先确认 API 至少部分可用（先拉一个测试）
        if replace_mode:
            # 先收集该月所有服务区数据到内存
            month_items = []
            for sp_id in sp_ids:
                params = {sp_param: sp_id}
                params.update(build_month_params(entry, month))
                resp = api_get(path, params)
                items = extract_items(resp)
                for row in items:
                    row["STATISTICS_MONTH"] = month
                    row["_SERVERPART_ID"] = sp_id
                month_items.extend(items)
            if not month_items:
                logger.warning(f"  ⚠️ {month}: 所有服务区 API 返回空，保留旧数据")
                continue
            # 有数据了，安全删旧
            key = get_server_key(conn, table)
            if "STATISTICS_MONTH" in cols:
                conn.execute(
                    f'DELETE FROM "{table}" WHERE STATISTICS_MONTH = ?', (month,)
                )
            for row in month_items:
                ensure_columns(conn, table, row, cols)
                insert_row(conn, table, row)
            added += len(month_items)
        else:
            for sp_id in sp_ids:
                # 检查已有
                if has_month_server_data(conn, table, month, sp_id):
                    continue
                params = {sp_param: sp_id}
                params.update(build_month_params(entry, month))
                resp = api_get(path, params)
                items = extract_items(resp)
                for row in items:
                    row["STATISTICS_MONTH"] = month
                    row["_SERVERPART_ID"] = sp_id
                    ensure_columns(conn, table, row, cols)
                    insert_row(conn, table, row)
                added += len(items)
        conn.commit()
        logger.info(f"    {month} 完成 ({len(sp_ids)} 服务区)")
    return added


def sync_type_d(conn: sqlite3.Connection, entry: Dict, sp_ids: List[str],
                full_refresh: bool = False):
    """D 类：服务区全量（安全模式：先收集全部数据，确认有效后再清表）"""
    table = entry["table"]
    path = entry["path"]
    sp_param = entry.get("sp_param", "ServerpartId")
    cols = get_table_columns(conn, table)
    # 先收集所有服务区数据到内存
    all_items = []
    for sp_id in sp_ids:
        if not full_refresh and has_month_server_data(conn, table, "", sp_id):
            continue
        resp = api_get(path, {sp_param: sp_id})
        items = extract_items(resp)
        for row in items:
            row["_SERVERPART_ID"] = sp_id
        all_items.extend(items)
    # 验证：有数据才能替换
    if full_refresh and not all_items:
        logger.warning(f"  ⚠️ 所有服务区 API 返回空，保留现有 {table} 数据")
        return 0
    # 行数护栏
    if full_refresh:
        old_cnt = conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
        if old_cnt > 10 and len(all_items) < old_cnt * 0.5:
            logger.error(
                f"  🛑 行数护栏: API 返回 {len(all_items)} 行，"
                f"当前 {old_cnt} 行，差异过大，跳过同步"
            )
            return 0
        conn.execute(f'DELETE FROM "{table}"')
    # 安全写入
    for row in all_items:
        ensure_columns(conn, table, row, cols)
        insert_row(conn, table, row)
    conn.commit()
    return len(all_items)


def sync_type_e(conn: sqlite3.Connection, entry: Dict, months: List[str],
               replace_mode: bool = False):
    """E 类：枚举×月，展开 children（服务区级明细）

    API 返回结构：顶层 1 条省级汇总 → children[N 个服务区]
    存储：省级汇总行（_SERVERPART_ID=0）+ 展开的服务区 children
    """
    table = entry["table"]
    path = entry["path"]
    enum_param = entry["enum_param"]
    enum_column = entry.get("enum_column", enum_param)
    enum_values = entry["enum_values"]
    cols = get_table_columns(conn, table)
    added = 0
    for month in months:
        for ev in enum_values:
            # 非替换模式下去重检查
            if not replace_mode:
                if "STATISTICS_MONTH" in cols and enum_column in cols:
                    existing = conn.execute(
                        f'SELECT COUNT(*) FROM "{table}" WHERE STATISTICS_MONTH = ? AND "{enum_column}" = ?',
                        (month, str(ev))
                    ).fetchone()[0]
                else:
                    existing = 0
                if existing > 0:
                    continue
            params = {enum_param: ev}
            params.update(build_month_params(entry, month))
            resp = api_get(path, params)
            items = extract_items(resp)
            if not items:
                if replace_mode:
                    logger.warning(f"  ⚠️ {month} enum={ev}: API 返回空，保留旧数据")
                continue

            # 有新数据了，替换模式下安全删旧
            if replace_mode and "STATISTICS_MONTH" in cols and enum_column in cols:
                conn.execute(
                    f'DELETE FROM "{table}" WHERE STATISTICS_MONTH = ? AND "{enum_column}" = ?',
                    (month, str(ev))
                )

            # 顶层是省级汇总，children 是服务区级明细
            for top in items:
                children = top.get("children", [])

                # 存省级汇总行（去掉 children 避免存储庞大 JSON）
                top_row = dict(top)
                top_row.pop("children", None)
                top_row["STATISTICS_MONTH"] = month
                top_row[enum_column] = str(ev)
                top_row["_SERVERPART_ID"] = "0"
                ensure_columns(conn, table, top_row, cols)
                insert_row(conn, table, top_row)
                added += 1

                # 展开服务区 children
                for child in children or []:
                    if not isinstance(child, dict):
                        continue
                    child["STATISTICS_MONTH"] = month
                    child[enum_column] = str(ev)
                    sp_id = child.get("服务区内码", child.get("servierpart_id", ""))
                    if sp_id:
                        child["_SERVERPART_ID"] = str(sp_id)
                    ensure_columns(conn, table, child, cols)
                    insert_row(conn, table, child)
                added += len(children or [])
        conn.commit()
    return added


def sync_type_f(conn: sqlite3.Connection, entry: Dict):
    """F 类：节日枚举×年份，展开 children + 嵌套 dict 展平

    专为 NEWGETHOLIDAYANALYSIS 设计：
    - 按 HolidayType(1-8) × Year(2024-2026) 组合拉取
    - 每次返回顶层 1 条全省汇总 + children[N 个服务区×M 天]
    - 嵌套 dict {'今日金额': xx, '累计金额': xx} 展平为 _日 / _累计 两列
    - 使用 INSERT OR REPLACE 去重
    """
    table = entry["table"]
    path = entry["path"]
    holiday_types = entry.get("holiday_types", [1, 2, 3, 4, 5, 6, 8])
    years = entry.get("years", [2024, 2025, 2026])

    # 建表（带唯一约束，支持 INSERT OR REPLACE 幂等）
    conn.execute(f'''CREATE TABLE IF NOT EXISTS "{table}" (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        holiday_type INTEGER,
        cur_year INTEGER,
        服务区名称 TEXT,
        服务区内码 INTEGER,
        本年日期 TEXT,
        历年日期 TEXT,
        本年对客销售_日 REAL,
        本年对客销售_累计 REAL,
        历年对客销售_日 REAL,
        历年对客销售_累计 REAL,
        本年业主收入_日 REAL,
        本年业主收入_累计 REAL,
        历年业主收入_日 REAL,
        历年业主收入_累计 REAL,
        本年入区车流_日 REAL,
        本年入区车流_累计 REAL,
        历年入区车流_日 REAL,
        历年入区车流_累计 REAL,
        本年自营对客_日 REAL,
        本年自营对客_累计 REAL,
        本年便利店对客_日 REAL,
        本年便利店对客_累计 REAL,
        本年商铺对客_日 REAL,
        本年商铺对客_累计 REAL,
        UNIQUE(holiday_type, cur_year, 服务区内码, 本年日期)
    )''')
    conn.commit()

    def _safe_float(v):
        """安全转 float"""
        if v is None or v == '':
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    def _extract_pair(d, key):
        """从嵌套 dict 提取 (日, 累计) 两个值"""
        val = d.get(key, {})
        if isinstance(val, dict):
            return _safe_float(val.get('今日金额')), _safe_float(val.get('累计金额'))
        # 兼容非 dict 值
        return _safe_float(val), None

    added = 0
    for ht in holiday_types:
        for year in years:
            # 检查是否已有数据（避免重复拉取）
            existing = conn.execute(
                f'SELECT COUNT(*) FROM "{table}" WHERE holiday_type = ? AND cur_year = ?',
                (ht, year)
            ).fetchone()[0]
            if existing > 0:
                logger.debug(f"  跳过 HolidayType={ht} Year={year}: 已有 {existing} 条")
                continue

            # 用 ServerpartIds=416 拉取全省数据
            resp = api_get(path, {
                'ServerpartIds': '416',
                'HolidayType': ht,
                'curYear': str(year),
            })
            items = extract_items(resp)
            if not items:
                logger.debug(f"  HolidayType={ht} Year={year}: 无数据")
                continue

            top = items[0]
            children = top.get('children', [])
            if not children:
                logger.debug(f"  HolidayType={ht} Year={year}: children 为空")
                continue

            # 展平 children 入库
            for child in children:
                if not isinstance(child, dict):
                    continue

                # 提取嵌套 dict 对
                对客_日, 对客_累计 = _extract_pair(child, '本年对客销售')
                历年对客_日, 历年对客_累计 = _extract_pair(child, '历年对客销售')
                业主_日, 业主_累计 = _extract_pair(child, '本年业主营业收入（除税）')
                历年业主_日, 历年业主_累计 = _extract_pair(child, '历年业主营业收入（除税）')
                车流_日, 车流_累计 = _extract_pair(child, '本年入区车流')
                历年车流_日, 历年车流_累计 = _extract_pair(child, '历年入区车流')
                自营_日, 自营_累计 = _extract_pair(child, '本年自营对客销售')
                便利店_日, 便利店_累计 = _extract_pair(child, '本年自营便利店对客销售')
                商铺_日, 商铺_累计 = _extract_pair(child, '本年商铺租赁对客销售')

                conn.execute(
                    f'INSERT OR REPLACE INTO "{table}" ('
                    f'holiday_type, cur_year, 服务区名称, 服务区内码, 本年日期, 历年日期, '
                    f'本年对客销售_日, 本年对客销售_累计, 历年对客销售_日, 历年对客销售_累计, '
                    f'本年业主收入_日, 本年业主收入_累计, 历年业主收入_日, 历年业主收入_累计, '
                    f'本年入区车流_日, 本年入区车流_累计, 历年入区车流_日, 历年入区车流_累计, '
                    f'本年自营对客_日, 本年自营对客_累计, '
                    f'本年便利店对客_日, 本年便利店对客_累计, '
                    f'本年商铺对客_日, 本年商铺对客_累计'
                    f') VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                    (
                        ht, year,
                        child.get('服务区名称', ''),
                        child.get('服务区内码'),
                        child.get('本年日期', ''),
                        child.get('历年日期', ''),
                        对客_日, 对客_累计, 历年对客_日, 历年对客_累计,
                        业主_日, 业主_累计, 历年业主_日, 历年业主_累计,
                        车流_日, 车流_累计, 历年车流_日, 历年车流_累计,
                        自营_日, 自营_累计,
                        便利店_日, 便利店_累计,
                        商铺_日, 商铺_累计,
                    )
                )

            added += len(children)
            conn.commit()
            logger.info(f"  HolidayType={ht} Year={year}: {len(children)} 条")

    return added


def sync_type_f_inc(conn: sqlite3.Connection, entry: Dict):
    """F 类（SERVERPART_INC）：节日枚举×年份，展平嵌套对比 dict

    专为 GetServerpartINCAnalysis 设计：
    - 按 HolidayType(1-8) × Year(2024-2026) 组合拉取
    - 顶层 1 条全省汇总 + 门店数据对比[N 个服务区]
    - 嵌套 dict {本年, 去年, 增长, 增长率} 展平为 _本年/_去年/_增长/_增长率 四列
    - 使用 INSERT OR REPLACE 去重
    """
    table = entry["table"]
    path = entry["path"]
    holiday_types = entry.get("holiday_types", [1, 2, 3, 4, 5, 6, 8])
    years = entry.get("years", [2024, 2025, 2026])

    # 建表（带唯一约束，支持 INSERT OR REPLACE 幂等）
    conn.execute(f'''CREATE TABLE IF NOT EXISTS "{table}" (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        holiday_type INTEGER,
        cur_year INTEGER,
        片区名称 TEXT,
        服务区名称 TEXT,
        服务区内码 INTEGER,
        对客销售_本年 REAL,
        对客销售_去年 REAL,
        对客销售_增长 REAL,
        对客销售_增长率 REAL,
        业主收入_本年 REAL,
        业主收入_去年 REAL,
        业主收入_增长 REAL,
        业主收入_增长率 REAL,
        入区车流_本年 REAL,
        入区车流_去年 REAL,
        入区车流_增长 REAL,
        入区车流_增长率 REAL,
        断面流量_本年 REAL,
        断面流量_去年 REAL,
        断面流量_增长 REAL,
        断面流量_增长率 REAL,
        UNIQUE(holiday_type, cur_year, 服务区内码)
    )''')
    conn.commit()

    # 对比字段映射：API 返回的字段名 → 本地列名前缀
    COMPARE_FIELDS = [
        ("对客销售对比", "对客销售"),
        ("业主营业收入（除税）对比", "业主收入"),
        ("入区车流数据对比", "入区车流"),
        ("断面流量数据对比", "断面流量"),
    ]

    def _safe_float(v):
        """安全转 float"""
        if v is None or v == '':
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    def _extract_compare(row, field_name):
        """从嵌套 dict 提取 (本年, 去年, 增长, 增长率)"""
        val = row.get(field_name, {})
        if isinstance(val, dict):
            return (
                _safe_float(val.get('本年')),
                _safe_float(val.get('去年')),
                _safe_float(val.get('增长')),
                _safe_float(val.get('增长率')),
            )
        return (None, None, None, None)

    def _upsert_row(ht, year, row):
        """展平一行数据并写入"""
        values = [ht, year,
                  row.get('片区名称', ''),
                  row.get('服务区名称', ''),
                  row.get('服务区内码')]
        for api_field, _ in COMPARE_FIELDS:
            values.extend(_extract_compare(row, api_field))
        conn.execute(
            f'INSERT OR REPLACE INTO "{table}" ('
            f'holiday_type, cur_year, 片区名称, 服务区名称, 服务区内码, '
            f'对客销售_本年, 对客销售_去年, 对客销售_增长, 对客销售_增长率, '
            f'业主收入_本年, 业主收入_去年, 业主收入_增长, 业主收入_增长率, '
            f'入区车流_本年, 入区车流_去年, 入区车流_增长, 入区车流_增长率, '
            f'断面流量_本年, 断面流量_去年, 断面流量_增长, 断面流量_增长率'
            f') VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
            tuple(values)
        )

    added = 0
    for ht in holiday_types:
        for year in years:
            # 检查是否已有数据（避免重复拉取）
            existing = conn.execute(
                f'SELECT COUNT(*) FROM "{table}" WHERE holiday_type = ? AND cur_year = ?',
                (ht, year)
            ).fetchone()[0]
            if existing > 0:
                logger.debug(f"  跳过 HolidayType={ht} Year={year}: 已有 {existing} 条")
                continue

            resp = api_get(path, {
                'HolidayType': ht,
                'curYear': str(year),
            })
            items = extract_items(resp)
            if not items:
                logger.debug(f"  HolidayType={ht} Year={year}: 无数据")
                continue

            top = items[0]
            # 存全省汇总行
            _upsert_row(ht, year, top)
            added += 1

            # 展开服务区明细（字段名是「门店数据对比」）
            sa_list = top.get('门店数据对比', [])
            if isinstance(sa_list, str):
                try:
                    sa_list = json.loads(sa_list)
                except (json.JSONDecodeError, TypeError):
                    sa_list = []

            for sa_row in (sa_list or []):
                if not isinstance(sa_row, dict):
                    continue
                _upsert_row(ht, year, sa_row)
                added += 1

            conn.commit()
            logger.info(f"  HolidayType={ht} Year={year}: {len(sa_list or [])+1} 条")

    return added


# ============================================================
# 展平双层嵌套表
# ============================================================

def flatten_revenue_report_shops(conn: sqlite3.Connection,
                                  months: Optional[List[str]] = None) -> int:
    """从 NEWGETREVENUEREPORT 的嵌套 children 展平到 NEWGETREVENUEREPORT_SHOPS

    结构：省级 1 行 → children[~123 服务区] → children[~15 门店]
    展平后每行 = 1 门店 × 1 月，合计项对客销售数据 dict 拍平为独立列。
    """
    target = "NEWGETREVENUEREPORT_SHOPS"
    source = "NEWGETREVENUEREPORT"

    # 确保源表存在
    exists = conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
        (source,)
    ).fetchone()[0]
    if not exists:
        logger.warning(f"跳过展平: {source} 表不存在")
        return 0

    # 建表
    conn.execute(f'''CREATE TABLE IF NOT EXISTS "{target}" (
        _id INTEGER PRIMARY KEY AUTOINCREMENT,
        STATISTICS_MONTH TEXT,
        服务区名称 TEXT,
        服务区内码 TEXT,
        门店名称 TEXT,
        经营模式 TEXT,
        商品业态 TEXT,
        经营商户 TEXT,
        对客销售 REAL,
        客单数量 INTEGER,
        销售数量 REAL,
        现金支付 REAL,
        移动支付 REAL,
        优惠金额 REAL
    )''')

    # 确定要展平的月份
    if months:
        target_months = months
    else:
        src_months = set(
            r[0] for r in conn.execute(
                f'SELECT DISTINCT STATISTICS_MONTH FROM "{source}" '
                f'WHERE children IS NOT NULL'
            ).fetchall()
        )
        try:
            dst_months = set(
                r[0] for r in conn.execute(
                    f'SELECT DISTINCT STATISTICS_MONTH FROM "{target}"'
                ).fetchall()
            )
        except Exception:
            dst_months = set()
        target_months = sorted(src_months - dst_months)

    if not target_months:
        return 0

    total = 0
    for month in target_months:
        conn.execute(
            f'DELETE FROM "{target}" WHERE STATISTICS_MONTH = ?', (month,)
        )
        rows = conn.execute(
            f'SELECT children FROM "{source}" WHERE STATISTICS_MONTH = ?',
            (month,)
        ).fetchall()
        month_count = 0
        for (children_json,) in rows:
            if not children_json:
                continue
            sa_list = json.loads(children_json) if isinstance(
                children_json, str) else children_json
            if not isinstance(sa_list, list):
                continue
            for sa in sa_list:
                sa_name = sa.get("服务区名称", "")
                sa_id = str(sa.get("服务区内码", ""))
                shop_children = sa.get("children", [])
                if isinstance(shop_children, str):
                    shop_children = json.loads(shop_children)
                for shop in (shop_children or []):
                    if not isinstance(shop, dict):
                        continue
                    # 拍平嵌套的销售数据 dict
                    sd = shop.get("合计项对客销售数据", {})
                    if isinstance(sd, str):
                        try:
                            sd = json.loads(sd)
                        except (json.JSONDecodeError, TypeError):
                            sd = {}
                    if not isinstance(sd, dict):
                        sd = {}
                    conn.execute(
                        f'INSERT INTO "{target}" '
                        f'(STATISTICS_MONTH,服务区名称,服务区内码,门店名称,'
                        f'经营模式,商品业态,经营商户,'
                        f'对客销售,客单数量,销售数量,现金支付,移动支付,优惠金额) '
                        f'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)',
                        (month, sa_name, sa_id,
                         shop.get("门店名称", ""),
                         shop.get("经营模式", ""),
                         shop.get("商品业态", ""),
                         shop.get("经营商户", ""),
                         float(sd.get("对客销售", 0) or 0),
                         int(sd.get("客单数量", 0) or 0),
                         float(sd.get("总数量", 0) or 0),
                         float(sd.get("现金支付金额", 0) or 0),
                         float(sd.get("移动支付金额", 0) or 0),
                         float(sd.get("优惠金额", 0) or 0),
                         ))
                    month_count += 1
        conn.commit()
        total += month_count
    logger.info(f"  展平门店数据: {total} 条")
    return total


# 经营模式编码→中文
_MODE_MAP_SABFI = {
    1000: "自营", "1000": "自营",
    2000: "合作经营", "2000": "合作经营",
    3000: "固定租金", "3000": "固定租金",
    4000: "展销", "4000": "展销",
}

# 上游对比 dict 的 key 映射（月度口径重命名）
_SABFI_KEY_MAP = {"本年": "本月", "去年": "去年同月"}


def flatten_sabfi_shops(conn: sqlite3.Connection,
                        months: Optional[List[str]] = None) -> int:
    """从 NEWGETSHOPSABFILIST 的三层嵌套 children 展平到 SABFI_SHOPS

    结构：全省汇总 1 行 → children[~136 服务区] → children[~4 门店]
    展平后每行 = 1 门店 × 1 月，对比 dict 拍平为独立列。
    """
    target = "SABFI_SHOPS"
    source = "NEWGETSHOPSABFILIST"

    exists = conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
        (source,)
    ).fetchone()[0]
    if not exists:
        logger.warning(f"跳过展平: {source} 表不存在")
        return 0

    # 建表
    conn.execute(f'''CREATE TABLE IF NOT EXISTS "{target}" (
        _id INTEGER PRIMARY KEY AUTOINCREMENT,
        STATISTICS_MONTH TEXT,
        片区名称 TEXT,
        服务区名称 TEXT,
        服务区内码 INTEGER,
        门店名称 TEXT,
        品牌名称 TEXT,
        经营业态名称 TEXT,
        经营业态大类 INTEGER,
        经营商户 TEXT,
        经营项目名称 TEXT,
        经营项目内码 INTEGER,
        合同开始日期 TEXT,
        合同结束日期 TEXT,
        经营模式 TEXT,
        结算模式 TEXT,
        对客销售_本月 REAL,
        对客销售_环比 REAL,
        对客销售_环比增长 REAL,
        对客销售_环比增长率 REAL,
        业主营收_本月 REAL,
        业主营收_环比 REAL,
        业主营收_环比增长率 REAL,
        客单数量_本月 INTEGER,
        客单数量_环比增长率 REAL,
        客单均价_本月 REAL,
        客单均价_环比增长率 REAL,
        盈利金额 REAL,
        预估成本 REAL,
        获客成本 REAL,
        租金收益贡献分值 REAL,
        运营盈利能力分值 REAL,
        车流弹性系数分值 REAL,
        商家风险指数分值 REAL,
        基础消费适配度分值 REAL,
        顾客吸引指数分值 REAL,
        SABFI总分 REAL,
        对客销售标准差 REAL,
        对客销售平均值 REAL
    )''')

    # 确定要展平的月份
    if months:
        target_months = months
    else:
        src_months = set(
            r[0] for r in conn.execute(
                f'SELECT DISTINCT STATISTICS_MONTH FROM "{source}" '
                f'WHERE children IS NOT NULL AND STATISTICS_MONTH IS NOT NULL'
            ).fetchall()
        )
        try:
            dst_months = set(
                r[0] for r in conn.execute(
                    f'SELECT DISTINCT STATISTICS_MONTH FROM "{target}"'
                ).fetchall()
            )
        except Exception:
            dst_months = set()
        target_months = sorted(src_months - dst_months)

    if not target_months:
        return 0

    def _get_compare(d, prefix, suffix):
        """从对比 dict 中提取值"""
        comp = d.get(prefix, {})
        if not isinstance(comp, dict):
            return None
        return comp.get(suffix)

    def _safe_float(v):
        try:
            return float(v) if v is not None else None
        except (TypeError, ValueError):
            return None

    total = 0
    for month in target_months:
        # 清理旧数据
        conn.execute(f'DELETE FROM "{target}" WHERE STATISTICS_MONTH = ?', (month,))

        rows = conn.execute(
            f'SELECT children FROM "{source}" WHERE STATISTICS_MONTH = ?',
            (month,)
        ).fetchall()

        month_count = 0
        for (children_json,) in rows:
            if not children_json:
                continue
            sa_list = json.loads(children_json) if isinstance(children_json, str) else children_json
            if not isinstance(sa_list, list):
                continue

            for sa in sa_list:
                # sa 是服务区级，门店在 sa["children"]
                sa_name = sa.get("服务区名称", "")
                sa_id = sa.get("服务区内码", "")
                area_name = sa.get("片区名称", "")
                shop_children = sa.get("children", [])
                if isinstance(shop_children, str):
                    shop_children = json.loads(shop_children)

                for shop in (shop_children or []):
                    if not isinstance(shop, dict):
                        continue

                    # 经营模式编码→中文
                    mode_key = "经营模式(1000：自营，2000：合作经营，3000：固定租金，4000：展销)"
                    mode_raw = shop.get(mode_key, "")
                    mode_cn = _MODE_MAP_SABFI.get(mode_raw, str(mode_raw)) if mode_raw else ""

                    conn.execute(
                        f'INSERT INTO "{target}" ('
                        f'STATISTICS_MONTH, 片区名称, 服务区名称, 服务区内码, '
                        f'门店名称, 品牌名称, 经营业态名称, 经营业态大类, '
                        f'经营商户, 经营项目名称, 经营项目内码, '
                        f'合同开始日期, 合同结束日期, 经营模式, 结算模式, '
                        f'对客销售_本月, 对客销售_环比, 对客销售_环比增长, 对客销售_环比增长率, '
                        f'业主营收_本月, 业主营收_环比, 业主营收_环比增长率, '
                        f'客单数量_本月, 客单数量_环比增长率, '
                        f'客单均价_本月, 客单均价_环比增长率, '
                        f'盈利金额, 预估成本, 获客成本, '
                        f'租金收益贡献分值, 运营盈利能力分值, 车流弹性系数分值, '
                        f'商家风险指数分值, 基础消费适配度分值, 顾客吸引指数分值, '
                        f'SABFI总分, 对客销售标准差, 对客销售平均值'
                        f') VALUES ('
                        f'?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'
                        f'?,?,?,?,?,?,?,?,?,?,?,?,?,?,'
                        f'?,?,?,?,?,?,?,?,?'
                        f')',
                        (
                            month,
                            area_name or shop.get("片区名称", ""),
                            sa_name or shop.get("服务区名称", ""),
                            sa_id or shop.get("服务区内码", ""),
                            shop.get("门店名称", ""),
                            shop.get("品牌名称", ""),
                            shop.get("经营业态名称", ""),
                            shop.get("经营业态大类"),
                            shop.get("经营商户", ""),
                            shop.get("经营项目名称", ""),
                            shop.get("经营项目内码"),
                            shop.get("合同开始日期", ""),
                            shop.get("合同结束日期", ""),
                            mode_cn,
                            str(shop.get("结算模式", "")) if shop.get("结算模式") else "",
                            # 对客销售对比（重命名 本年→本月）
                            _safe_float(_get_compare(shop, "对客销售对比", "本年")),
                            _safe_float(_get_compare(shop, "对客销售对比", "环比")),
                            _safe_float(_get_compare(shop, "对客销售对比", "环比增长")),
                            _safe_float(_get_compare(shop, "对客销售对比", "环比增长率")),
                            # 业主营收对比
                            _safe_float(_get_compare(shop, "业主营业收入（除税）对比", "本年")),
                            _safe_float(_get_compare(shop, "业主营业收入（除税）对比", "环比")),
                            _safe_float(_get_compare(shop, "业主营业收入（除税）对比", "环比增长率")),
                            # 客单数量
                            _safe_float(_get_compare(shop, "客单数量对比", "本年")),
                            _safe_float(_get_compare(shop, "客单数量对比", "环比增长率")),
                            # 客单均价
                            _safe_float(_get_compare(shop, "客单均价对比", "本年")),
                            _safe_float(_get_compare(shop, "客单均价对比", "环比增长率")),
                            # 盈亏
                            _safe_float(shop.get("盈利金额")),
                            _safe_float(shop.get("预估成本")),
                            _safe_float(shop.get("获客成本")),
                            # SABFI 分值
                            _safe_float(shop.get("租金收益贡献分值")),
                            _safe_float(shop.get("运营盈利能力分值")),
                            _safe_float(shop.get("车流弹性系数分值")),
                            _safe_float(shop.get("商家风险指数分值")),
                            _safe_float(shop.get("基础消费适配度分值")),
                            _safe_float(shop.get("顾客吸引指数分值")),
                            _safe_float(shop.get("SABFI总分")),
                            # 统计特征
                            _safe_float(shop.get("对客销售标准差")),
                            _safe_float(shop.get("对客销售平均值")),
                        )
                    )
                    month_count += 1

        conn.commit()
        total += month_count
        logger.info(f"  SABFI_SHOPS 展平 {month}: {month_count} 条门店")

    return total

# ============================================================
# 验证
# ============================================================

def verify_all(conn: sqlite3.Connection, expected_month: str,
               entries: Optional[List[Dict]] = None) -> List[str]:
    """五维度验证，返回问题列表"""
    issues = []
    sp_ids = get_service_area_ids(conn)
    entries = entries or SYNC_TABLE

    for entry in entries:
        table = entry["table"]
        name = entry["name"]
        stype = entry["type"]

        # 检查表是否存在
        exists = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
            (table,)
        ).fetchone()[0]
        if not exists:
            issues.append(f"[缺表] {name}: 表 {table} 不存在")
            continue

        cols = get_table_columns(conn, table)
        total = conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]

        if total == 0:
            issues.append(f"[空表] {name}: 0 行数据")
            continue

        # 维度 2：月份覆盖
        if "STATISTICS_MONTH" in cols and stype not in ("B", "D"):
            max_m = conn.execute(
                f'SELECT MAX(STATISTICS_MONTH) FROM "{table}" WHERE STATISTICS_MONTH IS NOT NULL'
            ).fetchone()[0]
            if max_m and max_m < expected_month:
                issues.append(f"[月份] {name}: 最新 {max_m}，应覆盖到 {expected_month}")

        # 维度 3：服务区覆盖（C 类）
        if stype == "C" and "_SERVERPART_ID" in cols:
            sp_cnt = conn.execute(
                f'SELECT COUNT(DISTINCT _SERVERPART_ID) FROM "{table}" '
                f'WHERE STATISTICS_MONTH = ?', (expected_month,)
            ).fetchone()[0]
            if sp_cnt < len(sp_ids) * 0.3:
                issues.append(
                    f"[覆盖] {name}: {expected_month} 只有 {sp_cnt} 服务区"
                    f"（预期 ≥{int(len(sp_ids)*0.3)}）"
                )

        # 维度 4：内容抽查（C 类用 417 抽查）
        if stype == "C" and not has_month_server_data(conn, table, expected_month, "417"):
            issues.append(f"[抽查] {name}: 417+{expected_month} 无数据")

    if not issues:
        logger.info("✅ 五维度验证全部通过")
    else:
        logger.warning(f"⚠️ 发现 {len(issues)} 个问题:")
        for iss in issues:
            logger.warning(f"  {iss}")

    return issues


def diagnose(conn: sqlite3.Connection, expected: str):
    """诊断模式：输出每张表的当前状态和缺失月份"""
    logger.info("=" * 60)
    logger.info("数据诊断报告")
    logger.info("=" * 60)

    total_missing = 0
    for entry in SYNC_TABLE:
        table = entry["table"]
        name = entry["name"]
        stype = entry["type"]

        exists = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
            (table,)
        ).fetchone()[0]
        if not exists:
            logger.warning(f"  {name:<30} | ❌ 表不存在")
            continue

        total = conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
        cols = get_table_columns(conn, table)

        if "STATISTICS_MONTH" in cols and stype not in ("B", "D"):
            max_m = conn.execute(
                f'SELECT MAX(STATISTICS_MONTH) FROM "{table}" WHERE STATISTICS_MONTH IS NOT NULL'
            ).fetchone()[0] or "无"
            missing = get_missing_months(conn, entry, expected)
            if missing:
                total_missing += len(missing)
                logger.info(f"  {name:<30} | {total:>5} 条 | 最新: {max_m} | ⚠️ 缺 {len(missing)} 月")
            else:
                logger.info(f"  {name:<30} | {total:>5} 条 | 最新: {max_m} | ✅")
        else:
            logger.info(f"  {name:<30} | {total:>5} 条 | 全量表 | ✅")

    logger.info("=" * 60)
    if total_missing > 0:
        logger.info(f"共 {total_missing} 个月份×表 需要补充")
    else:
        logger.info("✅ 所有月度表已覆盖到目标月份")

    return total_missing


def clean_month_data(conn: sqlite3.Connection, entries: List[Dict], month: str):
    """清理指定月份的旧数据"""
    total_deleted = 0
    for entry in entries:
        table = entry["table"]
        stype = entry["type"]
        if stype in ("B", "D"):
            continue  # 全量表不按月清理
        exists = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
            (table,)
        ).fetchone()[0]
        if not exists:
            continue
        cols = get_table_columns(conn, table)
        if "STATISTICS_MONTH" not in cols:
            continue
        cnt = conn.execute(
            f'SELECT COUNT(*) FROM "{table}" WHERE STATISTICS_MONTH = ?', (month,)
        ).fetchone()[0]
        if cnt > 0:
            conn.execute(f'DELETE FROM "{table}" WHERE STATISTICS_MONTH = ?', (month,))
            total_deleted += cnt
            logger.info(f"  清理 {entry['name']}: 删除 {cnt} 条 ({month})")
    conn.commit()
    return total_deleted


def entry_matches(entry: Dict, endpoint_name: str) -> bool:
    """支持按主名或 alias 过滤 endpoint"""
    names = {entry["name"], *entry.get("aliases", [])}
    return endpoint_name in names


# ============================================================
# 主入口
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="SQLite 镜像数据同步")
    parser.add_argument("--month", help="指定要同步的月份（YYYYMM），不传则进入诊断模式")
    parser.add_argument("--endpoint", help="只拉指定 endpoint（name）")
    parser.add_argument("--verify-only", action="store_true", help="只跑验证不拉数据")
    parser.add_argument("--full-refresh", action="store_true", help="全量刷新（B/D 类清表重拉）")
    parser.add_argument("--clean", action="store_true",
                        help="先清理指定月份的旧数据再重拉（需配合 --month 使用）")
    parser.add_argument("--yes", "-y", action="store_true",
                        help="跳过确认直接执行（给 AI agent 或自动化用）")
    parser.add_argument("--verbose", "-v", action="store_true", help="详细输出")
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    if args.clean and not args.month:
        logger.error("--clean 必须配合 --month 使用（指定要清理哪个月份的数据）")
        sys.exit(1)

    conn = get_connection()

    # --verify-only：只跑验证
    if args.verify_only:
        expected = args.month or get_expected_month()
        logger.info(f"验证目标月份: {expected}")
        verify_entries = SYNC_TABLE
        if args.endpoint:
            verify_entries = [e for e in SYNC_TABLE if entry_matches(e, args.endpoint)]
            if not verify_entries:
                logger.error(f"未找到 endpoint: {args.endpoint}")
                conn.close()
                sys.exit(1)
        verify_all(conn, expected, verify_entries)
        conn.close()
        return

    # 不传 --month：诊断模式，输出缺失报告让用户确认
    if not args.month:
        expected = get_expected_month()
        logger.info(f"目标月份: {expected}（上个自然月）")
        logger.info(f"数据库: {DB_PATH}\n")
        total_missing = diagnose(conn, expected)

        if total_missing == 0 and not args.full_refresh:
            conn.close()
            return

        if not args.yes:
            logger.info("")
            logger.info("请确认要补充的月份范围后，使用以下命令执行：")
            logger.info(f"  python scripts/data_update/sync_mirror.py --month YYYYMM")
            logger.info(f"  （如果旧数据有问题需要重拉，加 --clean 参数）")
            conn.close()
            return
        else:
            # --yes 模式：自动执行补缺
            args.month = None  # 让后面的逻辑走 get_missing_months

    logger.info(f"数据库: {DB_PATH}")

    sp_ids = get_service_area_ids(conn)
    logger.info(f"服务区数: {len(sp_ids)}")

    # 过滤 entries
    entries = SYNC_TABLE
    if args.endpoint:
        entries = [e for e in entries if entry_matches(e, args.endpoint)]
        if not entries:
            logger.error(f"未找到 endpoint: {args.endpoint}")
            conn.close()
            sys.exit(1)

    # 空库时按类型排序：B 类优先（含 basic_info 服务区列表），C/D 类靠后
    type_order = {"B": 0, "A": 1, "A_CHILDREN": 1, "E": 1, "F": 1, "D": 2, "C": 3}
    entries = sorted(entries, key=lambda e: type_order.get(e["type"], 9))

    # --clean：先检查并清理旧数据
    if args.clean and args.month:
        logger.info(f"\n检查 {args.month} 旧数据...")
        # 先统计旧数据量
        old_total = 0
        for entry in entries:
            table = entry["table"]
            if entry["type"] in ("B", "D"):
                continue
            try:
                cols = get_table_columns(conn, table)
                if "STATISTICS_MONTH" not in cols:
                    continue
                cnt = conn.execute(
                    f'SELECT COUNT(*) FROM "{table}" WHERE STATISTICS_MONTH = ?',
                    (args.month,)
                ).fetchone()[0]
                if cnt > 0:
                    old_total += cnt
                    logger.info(f"  {entry['name']:<30} | {cnt} 条旧数据")
            except Exception:
                pass

        if old_total > 0:
            if not args.yes:
                logger.warning(f"\n⚠️ {args.month} 共有 {old_total} 条旧数据将被清理后重拉")
                confirm = input("确认清理？(y/N): ").strip().lower()
                if confirm != "y":
                    logger.info("取消操作")
                    conn.close()
                    return
            deleted = clean_month_data(conn, entries, args.month)
            logger.info(f"已清理 {deleted} 条旧数据\n")
        else:
            logger.info(f"  {args.month} 无旧数据，直接拉取\n")

    # 执行同步
    target_month = args.month
    expected = target_month or get_expected_month()
    logger.info(f"开始同步... 目标月份: {expected}\n")

    total_added = 0
    for entry in entries:
        name = entry["name"]
        stype = entry["type"]
        table = entry["table"]

        # 确认表存在，不存在则自动创建空表
        exists = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
            (table,)
        ).fetchone()[0]
        if not exists and stype != "F":
            # 空库场景：自动创建空表，后续 ensure_columns 会动态添加列
            # F 类有自己的结构化建表逻辑，不在这里建
            conn.execute(f'CREATE TABLE IF NOT EXISTS "{table}" (_id INTEGER PRIMARY KEY AUTOINCREMENT)')
            conn.commit()
            logger.info(f"自动建表 {name}: {table}")

        if target_month:
            months = [target_month]
        else:
            months = get_missing_months(conn, entry, expected)
            # --full-refresh 空表场景：get_missing_months 返回空时用全量月份范围
            if not months and args.full_refresh and stype not in ("B", "D"):
                total = conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
                if total == 0:
                    start = entry.get("start_month", "202401")
                    months = _month_range(start, expected)
                    logger.info(f"  空表全量补充: {start} → {expected} ({len(months)} 月)")

        if stype == "A":
            if not months:
                logger.debug(f"跳过 {name}: 无缺失月份")
                continue
            logger.info(f"同步 {name} (A类): {len(months)} 个月")
            added = sync_type_a(conn, entry, months)

        elif stype == "A_CHILDREN":
            if not months:
                logger.debug(f"跳过 {name}: 无缺失月份")
                continue
            logger.info(f"同步 {name} (A_CHILDREN): {len(months)} 个月，展开 children")
            added = sync_type_a_children(conn, entry, months)

        elif stype == "B":
            logger.info(f"同步 {name} (B类): 全量快照")
            added = sync_type_b(conn, entry, args.full_refresh)

        elif stype == "C":
            if not months:
                logger.debug(f"跳过 {name}: 无缺失月份")
                continue
            # 空库场景：C 类需要服务区 ID，此时 B 类已拉完，重新获取
            if not sp_ids:
                sp_ids = get_service_area_ids(conn)
                logger.info(f"重新获取服务区数: {len(sp_ids)}")
            if not sp_ids:
                logger.warning(f"跳过 {name}: 无服务区数据（basic_info 未拉取？）")
                continue
            logger.info(f"同步 {name} (C类): {len(months)} 月 × {len(sp_ids)} 服务区")
            added = sync_type_c(conn, entry, months, sp_ids)

        elif stype == "D":
            # 空库场景：D 类需要服务区 ID
            if not sp_ids:
                sp_ids = get_service_area_ids(conn)
            if not sp_ids:
                logger.warning(f"跳过 {name}: 无服务区数据")
                continue
            logger.info(f"同步 {name} (D类): {len(sp_ids)} 服务区")
            added = sync_type_d(conn, entry, sp_ids, args.full_refresh)

        elif stype == "E":
            if not months:
                logger.debug(f"跳过 {name}: 无缺失月份")
                continue
            vals = entry["enum_values"]
            logger.info(f"同步 {name} (E类): {len(months)} 月 × {len(vals)} 枚举值")
            added = sync_type_e(conn, entry, months)

        elif stype == "F":
            # F 类：节日枚举×年份（不依赖 month 参数）
            ht_list = entry.get('holiday_types', [])
            yr_list = entry.get('years', [])
            logger.info(f"同步 {name} (F类): {len(ht_list)} 节日 × {len(yr_list)} 年")
            added = sync_type_f(conn, entry)

        else:
            logger.warning(f"跳过 {name}: 未知类型 {stype}")
            continue

        if added > 0:
            logger.info(f"  → 新增 {added} 条")
        total_added += added

    logger.info(f"\n同步完成: 共新增 {total_added} 条")

    # 展平双层嵌套表（从已有数据解析，不额外调 API）
    logger.info("\n展平双层嵌套表...")
    flat_month = [target_month] if target_month else None
    flat_added = flatten_revenue_report_shops(conn, flat_month)
    if flat_added > 0:
        logger.info(f"  → 展平新增 {flat_added} 条门店记录")

    sabfi_added = flatten_sabfi_shops(conn, flat_month)
    if sabfi_added > 0:
        logger.info(f"  → SABFI_SHOPS 展平新增 {sabfi_added} 条门店记录")

    # 自动验证
    logger.info("\n开始五维度验证...")
    verify_all(conn, expected, entries)

    conn.close()


if __name__ == "__main__":
    main()
