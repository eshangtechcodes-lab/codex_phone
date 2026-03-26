#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据完整性比对脚本

直接调上游 API，逐月/逐表比对本机 SQLite 行数与 API 返回行数。
不猜测、不用 sync-state，直接比对原始数据。

用法:
    python3 ops/scripts/sync/verify_data.py              # 跑全部 37 张表
    python3 ops/scripts/sync/verify_data.py --type A      # 只跑 A 类
    python3 ops/scripts/sync/verify_data.py --table revenue  # 只跑指定表
    python3 ops/scripts/sync/verify_data.py --sample 5    # C/D 类抽样 5 个服务区（默认 3）
"""

import argparse
import json
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import requests

# 路径（data_audit/sync/verify_data.py → 上溯 1 级到 data_audit 目录）
DATA_AUDIT_DIR = Path(__file__).resolve().parent.parent
DB_PATH = DATA_AUDIT_DIR / "data" / "dameng_mirror.db"

REMOTE_BASE = "http://111.229.213.193:18071/api/dynamic"
REQUEST_TIMEOUT = 20

# ============================================================
# 表配置（直接从 sync_mirror.py 复制，保持一致）
# ============================================================
SYNC_TABLE: List[Dict] = [
    # ---- A 类（省级月度）----
    {"name": "revenue",               "path": "/newGetMonthINCAnalysis/",        "table": "NEWGETMONTHINCANALYSIS",        "type": "A"},
    {"name": "revenue_report",        "path": "/GetMonthlyBusinessAnalysis/",    "table": "NEWGETMONTHLYBUSINESSANALYSIS", "type": "A",
     "start_month": "202406"},
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
    {"name": "finance",               "path": "/GetContractExcuteAnalysis/",     "table": "NEWGETCONTRACTEXCUTEANALYSIS",  "type": "A"},
    {"name": "bank_payment",          "path": "/GetMobilePayRoyaltyReport/",     "table": "NEWGETMOBILEPAYROYALTYREPORT",  "type": "A",
     "start_month": "202501"},
    {"name": "dashboard_overview",    "path": "/ahydDIBData/",                   "table": "NEWGETSUMMARYREVENUEMONTH",     "type": "A"},
    {"name": "dashboard_traffic",     "path": "/NEWGetProvinceMonthAnalysis/",   "table": "NEWGETPROVINCEMONTHANALYSIS",   "type": "A"},
    {"name": "dashboard_transaction", "path": "/NEWGETTRANSACTIONANALYSIS/",     "table": "NEWGETTRANSACTIONANALYSIS",     "type": "A"},
    {"name": "dashboard_revenue",     "path": "/NEWGETSUMMARYREVENUE/",          "table": "NEWGETSUMMARYREVENUE",          "type": "A"},
    {"name": "transaction_customer",  "path": "/GetTransactionCustomer/",        "table": "NEWGETTRANSACTIONCUSTOMER",     "type": "A",
     "start_month": "202406"},
    {"name": "commodity_sale_summary","path": "/GetCommoditySaleSummary/",       "table": "NEWGETCOMMODITYSALESUMMARY",    "type": "A",
     "extra_params": {"DATATYPE": "2"}},

    # ---- B 类（全量快照）----
    {"name": "basic_info",            "path": "/GetServerpartList/",             "table": "NEWGETSERVERPARTLIST",          "type": "B"},
    {"name": "investment",            "path": "/GetBusinessAnalysisReport/",     "table": "NEWGETBUSINESSANALYSISREPORT",  "type": "B"},
    {"name": "merchant_profitability","path": "/NEWMERCHANTPROFITABILITY/",      "table": "NEWMERCHANTPROFITABILITY",      "type": "B"},
    {"name": "business_trade",        "path": "/NEWBUSINESSTRADELIST/",          "table": "NEWBUSINESSTRADELIST",          "type": "B"},
    {"name": "project_risk",          "path": "/projectManagementRisk/",         "table": "NEWGETACCOUNTWARNINGLIST_RISK", "type": "B"},
    {"name": "project_profitability", "path": "/PROJECTPROFITABILITY/",          "table": "NEWPROJECTPROFITABILITY",       "type": "B"},
    {"name": "brand_ranking",         "path": "/NEWBrandRanking/",               "table": "NEWBRANDRANKING",               "type": "B"},
    {"name": "revenue_ranking",       "path": "/NEWREVENUERANKING/",             "table": "NEWREVENUERANKING",             "type": "B"},
    {"name": "traffic_ranking",       "path": "/NEWTRAFFICFLOWRANKING/",         "table": "NEWTRAFFICFLOWRANKING",         "type": "B"},
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
    {"name": "merchant_profit_loss",  "path": "/GetPeriodWarningList/",          "table": "NEWGETPERIODWARNINGLIST",       "type": "D"},
    {"name": "project_warning",       "path": "/GetAccountWarningList/",         "table": "NEWGETACCOUNTWARNINGLIST",      "type": "D",
     "sp_param": "SERVERPART_ID"},
    {"name": "border_service_area",   "path": "/NEWBORDERSERVICEPART/",          "table": "NEWBORDERSERVICEPART",          "type": "D",
     "sp_param": "SERVERPART_ID"},

    # ---- E 类（枚举×月）----
    {"name": "traffic_warning",       "path": "/VEHICLEFLOWWARNING/",            "table": "NEWVEHICLEFLOWWARNING",         "type": "E",
     "enum_param": "warningType", "enum_column": "WARNING_TYPE", "enum_values": [1, 2, 3, 4]},

    # ---- A_CHILDREN（省级月度，展开 children）----
    {"name": "vehicle_ownership",     "path": "/NEWVEHICLEOWNERSHIPLOCATION/",   "table": "NEWVEHICLEOWNERSHIPLOCATION",   "type": "A_CHILDREN",
     "start_month": "202501"},
]


# ============================================================
# 工具函数
# ============================================================

def api_get(path: str, params: Dict) -> Optional[Dict]:
    """调 API，返回 JSON"""
    url = f"{REMOTE_BASE}{path}"
    try:
        resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 200:
            return resp.json()
    except Exception:
        pass
    return None


def extract_items(resp: Dict) -> List[Dict]:
    """从 API 响应中提取 List"""
    if not resp:
        return []
    rd = resp.get("Result_Data", {})
    if rd is None:
        return []
    items = rd.get("List", [])
    return items if isinstance(items, list) else []


def build_month_params(entry: Dict, month: str) -> Dict:
    """构造月份参数"""
    if entry.get("month_param") == "StatisticsMonth":
        return {"StatisticsMonth": month}
    return {"StatisticsStartMonth": month, "StatisticsEndMonth": month}


def get_local_months(conn: sqlite3.Connection, table: str) -> List[str]:
    """获取本机表中所有月份"""
    try:
        cols = [r[1] for r in conn.execute(f'PRAGMA table_info("{table}")').fetchall()]
        if "STATISTICS_MONTH" not in cols:
            return []
        rows = conn.execute(
            f'SELECT DISTINCT STATISTICS_MONTH FROM "{table}" '
            f'WHERE STATISTICS_MONTH IS NOT NULL ORDER BY STATISTICS_MONTH'
        ).fetchall()
        return [str(r[0]).replace(".0", "") for r in rows if r[0]]
    except Exception:
        return []


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    """表是否存在"""
    return conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()[0] > 0


# ============================================================
# 各类型比对逻辑
# ============================================================

def verify_type_a(conn: sqlite3.Connection, entry: Dict) -> List[Dict]:
    """A 类：逐月 1 次 API 调用，比对行数"""
    table = entry["table"]
    results = []

    if not table_exists(conn, table):
        return [{"month": "-", "local": 0, "api": "表不存在", "status": "❌"}]

    months = get_local_months(conn, table)
    if not months:
        # 无月份列或无数据，直接比对总行数
        local_cnt = conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
        resp = api_get(entry["path"], entry.get("extra_params", {}))
        api_cnt = len(extract_items(resp))
        status = "✅" if local_cnt == api_cnt else f"❌ 差{api_cnt - local_cnt}"
        return [{"month": "全量", "local": local_cnt, "api": api_cnt, "status": status}]

    for month in months:
        local_cnt = conn.execute(
            f'SELECT COUNT(*) FROM "{table}" WHERE STATISTICS_MONTH = ?', (month,)
        ).fetchone()[0]

        params = build_month_params(entry, month)
        params.update(entry.get("extra_params", {}))
        resp = api_get(entry["path"], params)
        api_items = extract_items(resp)
        api_cnt = len(api_items)

        status = "✅" if local_cnt == api_cnt else f"❌ 差{api_cnt - local_cnt}"
        results.append({"month": month, "local": local_cnt, "api": api_cnt, "status": status})
        time.sleep(0.1)

    return results


def verify_type_a_children(conn: sqlite3.Connection, entry: Dict) -> List[Dict]:
    """A_CHILDREN 类：逐月调 API，比对展开后的行数（1 省级 + N children）"""
    table = entry["table"]
    results = []

    if not table_exists(conn, table):
        return [{"month": "-", "local": 0, "api": "表不存在", "status": "❌"}]

    months = get_local_months(conn, table)
    for month in months:
        local_cnt = conn.execute(
            f'SELECT COUNT(*) FROM "{table}" WHERE STATISTICS_MONTH = ?', (month,)
        ).fetchone()[0]

        params = build_month_params(entry, month)
        resp = api_get(entry["path"], params)
        api_items = extract_items(resp)
        # API 返回顶层 1 条，children 里是真数据
        api_cnt = 0
        if api_items:
            api_cnt = 1  # 省级汇总行
            children = api_items[0].get("children", [])
            api_cnt += len(children) if children else 0

        status = "✅" if local_cnt == api_cnt else f"❌ 差{api_cnt - local_cnt}"
        results.append({"month": month, "local": local_cnt, "api": api_cnt, "status": status})
        time.sleep(0.1)

    return results


def verify_type_b(conn: sqlite3.Connection, entry: Dict) -> List[Dict]:
    """B 类：1 次 API 调用，比对总行数"""
    table = entry["table"]

    if not table_exists(conn, table):
        return [{"month": "全量", "local": 0, "api": "表不存在", "status": "❌"}]

    local_cnt = conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
    resp = api_get(entry["path"], {})
    api_cnt = len(extract_items(resp))

    status = "✅" if local_cnt == api_cnt else f"❌ 差{api_cnt - local_cnt}"
    return [{"month": "全量", "local": local_cnt, "api": api_cnt, "status": status}]


def verify_type_c(conn: sqlite3.Connection, entry: Dict, sample_sp: List[str],
                  sample_months: List[str]) -> List[Dict]:
    """C 类：抽样 服务区×月，比对行数"""
    table = entry["table"]
    sp_param = entry.get("sp_param", "ServerpartId")
    results = []

    if not table_exists(conn, table):
        return [{"month": "-", "local": 0, "api": "表不存在", "status": "❌"}]

    for month in sample_months:
        for sp_id in sample_sp:
            # 本机行数
            try:
                local_cnt = conn.execute(
                    f'SELECT COUNT(*) FROM "{table}" '
                    f'WHERE STATISTICS_MONTH = ? AND _SERVERPART_ID = ?',
                    (month, sp_id)
                ).fetchone()[0]
            except Exception:
                local_cnt = 0

            # API 行数
            params = {sp_param: sp_id}
            params.update(build_month_params(entry, month))
            resp = api_get(entry["path"], params)
            api_cnt = len(extract_items(resp))

            status = "✅" if local_cnt == api_cnt else f"❌ 差{api_cnt - local_cnt}"
            results.append({
                "month": f"{month}/{sp_id}",
                "local": local_cnt, "api": api_cnt, "status": status
            })
            time.sleep(0.1)

    return results


def verify_type_d(conn: sqlite3.Connection, entry: Dict, sample_sp: List[str]) -> List[Dict]:
    """D 类：抽样服务区，比对行数"""
    table = entry["table"]
    sp_param = entry.get("sp_param", "ServerpartId")
    results = []

    if not table_exists(conn, table):
        return [{"month": "全量", "local": 0, "api": "表不存在", "status": "❌"}]

    for sp_id in sample_sp:
        try:
            local_cnt = conn.execute(
                f'SELECT COUNT(*) FROM "{table}" WHERE _SERVERPART_ID = ?', (sp_id,)
            ).fetchone()[0]
        except Exception:
            local_cnt = 0

        resp = api_get(entry["path"], {sp_param: sp_id})
        api_cnt = len(extract_items(resp))

        status = "✅" if local_cnt == api_cnt else f"❌ 差{api_cnt - local_cnt}"
        results.append({
            "month": f"sp={sp_id}",
            "local": local_cnt, "api": api_cnt, "status": status
        })
        time.sleep(0.1)

    return results


def verify_type_e(conn: sqlite3.Connection, entry: Dict) -> List[Dict]:
    """E 类：逐月×枚举值，比对行数（含 children 展开）"""
    table = entry["table"]
    enum_param = entry["enum_param"]
    enum_column = entry.get("enum_column", enum_param)
    enum_values = entry["enum_values"]
    results = []

    if not table_exists(conn, table):
        return [{"month": "-", "local": 0, "api": "表不存在", "status": "❌"}]

    months = get_local_months(conn, table)
    for month in months:
        for ev in enum_values:
            try:
                local_cnt = conn.execute(
                    f'SELECT COUNT(*) FROM "{table}" '
                    f'WHERE STATISTICS_MONTH = ? AND "{enum_column}" = ?',
                    (month, str(ev))
                ).fetchone()[0]
            except Exception:
                local_cnt = 0

            params = {enum_param: ev}
            params.update(build_month_params(entry, month))
            resp = api_get(entry["path"], params)
            api_items = extract_items(resp)

            # E 类展开后：1 顶层汇总 + N children = 总行数
            api_cnt = 0
            for item in api_items:
                api_cnt += 1  # 顶层汇总行
                ch = item.get("children", [])
                if isinstance(ch, list):
                    api_cnt += len(ch)

            status = "✅" if local_cnt == api_cnt else f"❌ 差{api_cnt - local_cnt}"
            results.append({
                "month": f"{month}/type={ev}",
                "local": local_cnt, "api": api_cnt,
                "status": status
            })
            time.sleep(0.1)

    return results


# ============================================================
# 主流程
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="数据完整性比对")
    parser.add_argument("--type", help="只跑指定类型 (A/B/C/D/E/A_CHILDREN)")
    parser.add_argument("--table", help="只跑指定表 (name)")
    parser.add_argument("--sample", type=int, default=3, help="C/D 类抽样服务区数 (默认 3)")
    args = parser.parse_args()

    if not DB_PATH.exists():
        print(f"❌ 数据库不存在: {DB_PATH}")
        sys.exit(1)

    conn = sqlite3.connect(str(DB_PATH))

    # 获取抽样用的服务区 ID
    try:
        all_sp = [str(r[0]) for r in conn.execute(
            "SELECT 服务区内码 FROM NEWGETSERVERPARTLIST"
        ).fetchall()]
    except Exception:
        all_sp = []

    # 均匀抽样：首、中、尾
    if all_sp and args.sample > 0:
        step = max(1, len(all_sp) // args.sample)
        sample_sp = all_sp[::step][:args.sample]
    else:
        sample_sp = all_sp[:3]

    # C 类抽样月份：最早、最新、中间
    sample_months_c = ["202501", "202602"]  # 取两个有代表性的月份

    # 筛选要跑的表
    tables = SYNC_TABLE
    if args.type:
        tables = [t for t in tables if t["type"] == args.type.upper()]
    if args.table:
        tables = [t for t in tables if t["name"] == args.table]

    if not tables:
        print("没有匹配的表")
        sys.exit(1)

    # 统计
    total_ok = 0
    total_fail = 0
    total_tables = len(tables)
    fail_details = []

    print(f"{'=' * 70}")
    print(f"数据完整性比对报告")
    print(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"数据库: {DB_PATH}")
    print(f"比对表数: {total_tables}")
    if sample_sp:
        print(f"C/D 类抽样服务区: {sample_sp}")
    print(f"{'=' * 70}")

    for i, entry in enumerate(tables, 1):
        name = entry["name"]
        table = entry["table"]
        ttype = entry["type"]

        print(f"\n[{i}/{total_tables}] {name} ({table}) [{ttype}类]")
        print(f"  {'月份/键':<20} {'本机':>8} {'API':>15} {'结果':>10}")
        print(f"  {'-' * 58}")

        try:
            if ttype == "A":
                results = verify_type_a(conn, entry)
            elif ttype == "A_CHILDREN":
                results = verify_type_a_children(conn, entry)
            elif ttype == "B":
                results = verify_type_b(conn, entry)
            elif ttype == "C":
                results = verify_type_c(conn, entry, sample_sp, sample_months_c)
            elif ttype == "D":
                results = verify_type_d(conn, entry, sample_sp)
            elif ttype == "E":
                results = verify_type_e(conn, entry)
            else:
                results = [{"month": "-", "local": 0, "api": "未知类型", "status": "⚠️"}]
        except Exception as e:
            results = [{"month": "-", "local": 0, "api": str(e)[:30], "status": "💥"}]

        table_ok = 0
        table_fail = 0
        for r in results:
            mark = r["status"]
            print(f"  {r['month']:<20} {r['local']:>8} {str(r['api']):>15} {mark:>10}")
            if "✅" in mark:
                table_ok += 1
            else:
                table_fail += 1

        total_ok += table_ok
        total_fail += table_fail

        if table_fail > 0:
            fail_details.append(f"  {name} ({ttype}类): {table_fail} 项不一致")

        # 表级汇总
        if table_fail == 0:
            print(f"  >>> ✅ 全部一致 ({table_ok} 项)")
        else:
            print(f"  >>> ❌ {table_fail}/{table_ok + table_fail} 项不一致")

    # 总汇总
    print(f"\n{'=' * 70}")
    print(f"总计: {total_ok} 项一致, {total_fail} 项不一致")
    if fail_details:
        print(f"\n不一致的表:")
        for d in fail_details:
            print(d)
    else:
        print("🎉 全部数据一致！")
    print(f"{'=' * 70}")

    conn.close()


if __name__ == "__main__":
    main()
