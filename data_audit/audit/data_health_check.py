#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据健康校验脚本

5 层交叉校验：
  层次1：金额级 — MONTHINCA / REVENUEREPORT vs 日度汇总（LOCAL_DAILY_REVENUE）
  层次2：画像一致性 — Redis 结论缓存中的营收 vs 日度真实值
  层次3：排名覆盖率 — REVENUEREPORT / BAYONET children 覆盖的 SA 数 vs 全量 SA
  层次4：口径基线 — 三源（MONTHINCA / REVENUEREPORT / 日度）同月同 SA 金额对比
  层次5：车流交叉 — BAYONET 入区车流 vs MONTHINCA 入区车流数据对比

用法:
    python3 ops/scripts/audit/data_health_check.py               # 默认校验最近1个月
    python3 ops/scripts/audit/data_health_check.py --month 202602 # 指定月份
    python3 ops/scripts/audit/data_health_check.py --all-months   # 校验最近有日度数据的月份
    python3 ops/scripts/audit/data_health_check.py --layer 1      # 只跑指定层次
    python3 ops/scripts/audit/data_health_check.py --output /tmp/report.md  # 输出 Markdown 报告
    python3 ops/scripts/audit/data_health_check.py --output /tmp/report.json # 输出 JSON 报告
"""

import argparse
import calendar
import json
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

# 项目路径（data_audit/audit/data_health_check.py → 上溯 1 级到 data_audit 目录）
DATA_AUDIT_DIR = Path(__file__).resolve().parent.parent
MIRROR_DB = DATA_AUDIT_DIR / "data" / "dameng_mirror.db"

# 差异阈值
REVENUE_DIFF_THRESHOLD = 0.10  # 营收差异 >10% 标红
TRAFFIC_DIFF_THRESHOLD = 0.40  # 车流差异 >40% 标红
COVERAGE_WARN_THRESHOLD = 130  # 排名表覆盖 SA 数低于此值告警
DAILY_COMPLETENESS_THRESHOLD = 0.80  # 日度天数 < 月份总天数的80% 视为不完整


# ============================================================
# 报告收集器（支持 stdout + 文件输出）
# ============================================================

class ReportCollector:
    """收集校验结果，支持 stdout / Markdown / JSON 输出"""

    def __init__(self) -> None:
        self.results: List[Dict[str, Any]] = []
        self._current_month: Optional[Dict[str, Any]] = None
        self._current_layers: List[Dict[str, Any]] = []
        self.freshness: Dict[str, str] = {}

    def start_month(self, month: str, incomplete_days: Optional[int] = None,
                    total_days: Optional[int] = None) -> None:
        """开始一个月份的校验"""
        self._current_month = {
            "month": month,
            "incomplete": incomplete_days is not None,
            "daily_days": incomplete_days,
            "total_days": total_days,
            "layers": [],
        }
        self._current_layers = self._current_month["layers"]
        self.results.append(self._current_month)

    def add_layer(self, layer_num: int, title: str, ok_count: int,
                  issue_count: int, issues: Optional[List[Dict[str, Any]]] = None,
                  notes: Optional[str] = None, reference_only: bool = False) -> None:
        """添加一个层次的校验结果"""
        entry = {
            "layer": layer_num,
            "title": title,
            "ok": ok_count,
            "issues_count": issue_count,
            "issues": issues or [],
            "notes": notes,
            "reference_only": reference_only,
        }
        self._current_layers.append(entry)

    def get_summary(self) -> Tuple[int, int]:
        """汇总统计（排除 reference_only 的）"""
        total_ok = 0
        total_issues = 0
        for m in self.results:
            for layer in m["layers"]:
                if not layer["reference_only"]:
                    total_ok += layer["ok"]
                    total_issues += layer["issues_count"]
        return total_ok, total_issues

    def to_json(self) -> Dict[str, Any]:
        """输出 JSON 结构"""
        ok, issues = self.get_summary()
        result: Dict[str, Any] = {
            "report_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "database": str(MIRROR_DB),
            "summary": {"ok": ok, "issues": issues},
            "months": self.results,
        }
        if hasattr(self, "freshness"):
            result["data_freshness"] = self.freshness
        return result

    def to_markdown(self) -> str:
        """输出 Markdown 报告"""
        lines: List[str] = []
        ok, issues = self.get_summary()
        lines.append(f"# 数据健康校验报告")
        lines.append(f"")
        lines.append(f"- 时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append(f"- 数据库: `{MIRROR_DB.name}`")
        lines.append(f"- 状态: {'✅ 全部健康' if issues == 0 else f'⚠️ {issues} 个待关注项'}")
        lines.append(f"- 一致项: {ok} / 问题项: {issues}")
        if hasattr(self, "freshness"):
            parts = [f"{k}: {v}" for k, v in self.freshness.items()]
            lines.append(f"- 数据新鲜度: {' | '.join(parts)}")
        lines.append("")

        for m in self.results:
            month = m["month"]
            if m["incomplete"]:
                lines.append(f"## {month} ⚠️ 日度不完整({m['daily_days']}/{m['total_days']}天，仅供参考)")
            else:
                lines.append(f"## {month}")
            lines.append("")

            for layer in m["layers"]:
                ref_tag = " (仅供参考)" if layer["reference_only"] else ""
                status = "✅" if layer["issues_count"] == 0 else "❌"
                lines.append(f"### 层次{layer['layer']}：{layer['title']}{ref_tag}")
                lines.append(f"")
                lines.append(f"{status} 一致: {layer['ok']}，问题: {layer['issues_count']}")
                lines.append("")

                if layer.get("notes"):
                    lines.append(f"> {layer['notes']}")
                    lines.append("")

                if layer["issues"]:
                    # 根据层次类型输出不同表头
                    ln = layer["layer"]
                    if ln == 5:
                        lines.append(f"| 服务区 | MONTHINCA | BAYONET | 差异% | 方向 |")
                        lines.append(f"|--------|----------|---------|-------|------|")
                        for i in layer["issues"]:
                            lines.append(
                                f"| {i['name']} | {i.get('monthinca', 0):,.0f} | "
                                f"{i.get('bayonet', 0):,.0f} | {i.get('pct', 0):.1%} | "
                                f"{i.get('direction', '')} |"
                            )
                    elif ln == 2:
                        lines.append(f"| 服务区 | 缓存(万) | 日度(万) | 差异% |")
                        lines.append(f"|--------|---------|---------|-------|")
                        for i in layer["issues"]:
                            lines.append(
                                f"| {i['name']} | {i.get('cache_wan', 0):.2f} | "
                                f"{i.get('daily_wan', 0):.2f} | {i.get('pct', 0):.1%} |"
                            )
                    elif ln == 1:
                        lines.append(f"| 服务区 | MONTHINCA | 日度汇总 | 差额 | 缺失% |")
                        lines.append(f"|--------|----------|---------|------|-------|")
                        for i in layer["issues"]:
                            lines.append(
                                f"| {i['name']} | {i.get('monthinca', 0):,.0f} | "
                                f"{i.get('daily', 0):,.0f} | {i.get('diff', 0):,.0f} | "
                                f"{i.get('pct', 0):.1%} |"
                            )
                    else:
                        # 层次3: 覆盖率
                        if ln == 3:
                            for i in layer["issues"]:
                                lines.append(
                                    f"- {i.get('source', '?')}: 覆盖 "
                                    f"{i.get('count', '?')}/{i.get('total', '?')}，"
                                    f"缺失 {i.get('missing', '?')} 个"
                                )
                        # 层次4: 口径基线三源对比
                        elif ln == 4:
                            lines.append("| 服务区 | MONTHINCA | REVENUEREPORT | 日度汇总 | MI偏差 | RR偏差 |")
                            lines.append("|--------|----------|--------------|---------|-------|-------|")
                            for i in layer["issues"]:
                                lines.append(
                                    f"| {i['name']} | {i.get('monthinca', 0):,.0f} | "
                                    f"{i.get('revreport', 0):,.0f} | "
                                    f"{i.get('daily', 0):,.0f} | "
                                    f"{i.get('mi_pct', 0):.1%} | "
                                    f"{i.get('rr_pct', 0):.1%} |"
                                )
                        else:
                            # 其他层次兖底
                            for i in layer["issues"]:
                                lines.append(f"- {i}")
                    lines.append("")

        return "\n".join(lines)


# ============================================================
# 工具函数
# ============================================================

def get_data_freshness(conn: sqlite3.Connection) -> Dict[str, str]:
    """检查各表最后更新时间，用于评估数据新鲜度"""
    queries = {
        "日度": "SELECT MAX(STATISTICS_DATE) FROM LOCAL_DAILY_REVENUE",
        "MONTHINCA": "SELECT MAX(STATISTICS_MONTH) FROM NEWGETMONTHINCANALYSIS",
        "BAYONET": "SELECT MAX(STATISTICS_MONTH) FROM NEWGETBAYONETOWNERAHTREELIST",
        "REVENUEREPORT": "SELECT MAX(STATISTICS_MONTH) FROM NEWGETREVENUEREPORT",
    }
    result: Dict[str, str] = {}
    for name, sql in queries.items():
        try:
            row = conn.execute(sql).fetchone()
            result[name] = str(row[0]) if row and row[0] else "无数据"
        except Exception:
            result[name] = "查询失败"
    return result

def get_conn() -> sqlite3.Connection:
    """获取 dameng_mirror.db 连接"""
    if not MIRROR_DB.exists():
        print(f"❌ 数据库不存在: {MIRROR_DB}")
        sys.exit(1)
    return sqlite3.connect(str(MIRROR_DB))


def get_available_months(conn: sqlite3.Connection) -> List[str]:
    """获取 MONTHINCA 中所有月份"""
    rows = conn.execute(
        "SELECT DISTINCT STATISTICS_MONTH FROM NEWGETMONTHINCANALYSIS "
        "ORDER BY STATISTICS_MONTH"
    ).fetchall()
    return [r[0] for r in rows if r[0]]


def get_daily_day_count(conn: sqlite3.Connection, month: str) -> int:
    """获取指定月份的日度数据天数"""
    date_prefix = f"{month[:4]}-{month[4:6]}"
    row = conn.execute(
        "SELECT COUNT(DISTINCT STATISTICS_DATE) FROM LOCAL_DAILY_REVENUE "
        "WHERE STATISTICS_DATE LIKE ?",
        (f"{date_prefix}%",)
    ).fetchone()
    return row[0] if row else 0


def get_month_total_days(month: str) -> int:
    """获取月份的总天数"""
    year = int(month[:4])
    mon = int(month[4:6])
    return calendar.monthrange(year, mon)[1]


def get_months_with_daily(conn: sqlite3.Connection) -> List[str]:
    """获取有日度数据的月份列表（格式 YYYYMM）"""
    rows = conn.execute(
        "SELECT DISTINCT SUBSTR(STATISTICS_DATE, 1, 7) FROM LOCAL_DAILY_REVENUE "
        "ORDER BY 1"
    ).fetchall()
    # 转换 "2026-01" → "202601"
    return [r[0].replace("-", "") for r in rows if r[0]]


def get_daily_revenue_by_sa(conn: sqlite3.Connection, month: str) -> Dict[str, Dict[str, Any]]:
    """从 LOCAL_DAILY_REVENUE 按服务区汇总月度营收"""
    date_prefix = f"{month[:4]}-{month[4:6]}"
    rows = conn.execute(
        "SELECT SERVERPART_ID, SERVERPART_NAME, SUM(REVENUE_TOTAL) "
        "FROM LOCAL_DAILY_REVENUE "
        "WHERE STATISTICS_DATE LIKE ? "
        "GROUP BY SERVERPART_ID",
        (f"{date_prefix}%",)
    ).fetchall()
    return {str(r[0]): {"name": r[1], "daily_sum": r[2] or 0} for r in rows}


def get_monthinca_data(conn: sqlite3.Connection, month: str) -> Dict[str, Dict[str, Any]]:
    """从 MONTHINCA children 一次性提取各 SA 的营收和车流（避免重复解析 600KB+ JSON）"""
    row = conn.execute(
        "SELECT children FROM NEWGETMONTHINCANALYSIS "
        "WHERE STATISTICS_MONTH=?",
        (month,)
    ).fetchone()
    if not row or not row[0]:
        return {}
    try:
        children = json.loads(row[0])
    except (json.JSONDecodeError, TypeError):
        return {}
    result: Dict[str, Dict[str, Any]] = {}
    for c in children:
        sid = str(c.get("服务区内码", ""))
        if not sid:
            continue
        name = c.get("服务区名称", "?")
        # 营收
        sales = c.get("对客销售对比", {})
        revenue = float(sales.get("本年", 0) or 0)
        # 车流
        traffic_data = c.get("入区车流数据对比", {})
        traffic = float(traffic_data.get("本年", 0) or 0)
        result[sid] = {"name": name, "monthinca": revenue, "traffic": traffic}
    return result


# 兼容旧调用的便捷包装
def get_monthinca_by_sa(conn: sqlite3.Connection, month: str) -> Dict[str, Dict[str, Any]]:
    """提取营收数据（委托给 get_monthinca_data）"""
    return get_monthinca_data(conn, month)


def get_monthinca_traffic_by_sa(conn: sqlite3.Connection, month: str) -> Dict[str, Dict[str, Any]]:
    """提取车流数据（委托给 get_monthinca_data，过滤 traffic > 0）"""
    data = get_monthinca_data(conn, month)
    return {sid: v for sid, v in data.items() if v.get("traffic", 0) > 0}


def get_bayonet_traffic_by_sa(conn: sqlite3.Connection, month: str) -> Dict[str, Dict[str, Any]]:
    """从 BAYONET 表提取各 SA 的入区车流"""
    rows = conn.execute(
        "SELECT children FROM NEWGETBAYONETOWNERAHTREELIST "
        "WHERE STATISTICS_MONTH=?",
        (month,)
    ).fetchall()
    result = {}
    for (children_json,) in rows:
        if not children_json:
            continue
        try:
            children = json.loads(children_json)
        except (json.JSONDecodeError, TypeError):
            continue
        for child in children:
            sid = child.get("服务区内码")
            if not sid:
                continue
            sid = str(int(sid))
            name = child.get("服务区名称", f"ID:{sid}")
            traffic = float(child.get("入区车流", 0) or 0)
            if traffic > 0:
                result[sid] = {"name": name, "traffic": traffic}
    return result


def get_revenuereport_by_sa(conn: sqlite3.Connection, month: str) -> Dict[str, Dict[str, Any]]:
    """从 REVENUEREPORT children 提取各 SA 的对客销售汇总"""
    rows = conn.execute(
        "SELECT children FROM NEWGETREVENUEREPORT "
        "WHERE STATISTICS_MONTH=?",
        (month,)
    ).fetchall()
    if not rows:
        return {}
    sa_revenue = defaultdict(lambda: {"name": "", "revreport": 0.0})
    for (children_json,) in rows:
        if not children_json:
            continue
        try:
            children = json.loads(children_json)
        except (json.JSONDecodeError, TypeError):
            continue
        if not isinstance(children, list):
            continue
        for child in children:
            sid = child.get("服务区内码")
            if not sid:
                continue
            sid = str(int(sid))
            sales_data = child.get("合计项对客销售数据", {})
            if isinstance(sales_data, str):
                try:
                    sales_data = json.loads(sales_data)
                except (json.JSONDecodeError, TypeError):
                    sales_data = {}
            revenue = float(sales_data.get("对客销售", 0) or 0)
            sa_revenue[sid]["revreport"] += revenue
            sa_revenue[sid]["name"] = child.get("服务区名称", f"ID:{sid}")
    return dict(sa_revenue)


def get_bayonet_sa_count(conn: sqlite3.Connection, month: str) -> Set[str]:
    """从 BAYONET 表统计覆盖的 SA 数"""
    rows = conn.execute(
        "SELECT children FROM NEWGETBAYONETOWNERAHTREELIST "
        "WHERE STATISTICS_MONTH=?",
        (month,)
    ).fetchall()
    sids = set()
    for (children_json,) in rows:
        if not children_json:
            continue
        try:
            children = json.loads(children_json)
        except (json.JSONDecodeError, TypeError):
            continue
        for child in children:
            sid = child.get("服务区内码")
            if sid:
                sids.add(str(int(sid)))
    return sids


# ============================================================
# 层次1：金额级交叉校验
# ============================================================

def check_layer1(conn, month, collector, reference_only=False):
    """MONTHINCA vs 日度汇总，差异 >10% 的 SA 标红"""
    ref_tag = " [仅供参考]" if reference_only else ""
    print(f"\n{'=' * 70}")
    print(f"层次1：金额级交叉校验（{month}）{ref_tag}")
    print(f"  MONTHINCA 对客销售 vs LOCAL_DAILY_REVENUE 日度汇总")
    print(f"{'=' * 70}")

    daily = get_daily_revenue_by_sa(conn, month)
    monthinca = get_monthinca_by_sa(conn, month)

    if not daily:
        print(f"  ⚠️ 无日度数据（{month}），跳过")
        collector.add_layer(1, "金额级交叉校验", 0, 0,
                            notes=f"无日度数据（{month}）",
                            reference_only=reference_only)
        return 0, 0

    issues = []
    ok_count = 0

    for sid, mi in monthinca.items():
        d = daily.get(sid)
        if not d or d["daily_sum"] <= 0:
            continue
        diff = d["daily_sum"] - mi["monthinca"]
        pct = diff / d["daily_sum"]
        if abs(pct) > REVENUE_DIFF_THRESHOLD:
            issues.append({
                "sid": sid,
                "name": mi["name"],
                "monthinca": mi["monthinca"],
                "daily": d["daily_sum"],
                "diff": diff,
                "pct": pct,
            })
        else:
            ok_count += 1

    if issues:
        issues.sort(key=lambda x: -abs(x["diff"]))
        print(f"\n  ❌ {len(issues)} 个 SA 差异 >{REVENUE_DIFF_THRESHOLD*100:.0f}%：")
        print(f"  {'服务区':<14} {'MONTHINCA':>12} {'日度汇总':>12} {'差额':>12} {'缺失%':>6}")
        print(f"  {'-' * 60}")
        for i in issues:
            print(
                f"  {i['name']:<14} "
                f"{i['monthinca']:>12,.0f} "
                f"{i['daily']:>12,.0f} "
                f"{i['diff']:>12,.0f} "
                f"{i['pct']:>5.1%}"
            )
    else:
        print(f"\n  ✅ 全部 SA 差异 <{REVENUE_DIFF_THRESHOLD*100:.0f}%")

    print(f"\n  汇总: {ok_count} 个一致, {len(issues)} 个差异过大")

    collector.add_layer(1, "金额级交叉校验", ok_count, len(issues),
                        issues=issues, reference_only=reference_only)
    return ok_count, len(issues)


# ============================================================
# 层次2：画像一致性（Redis 结论缓存中营收 vs 日度真实值）
# ============================================================

def check_layer2(conn, month, collector, reference_only=False):
    """缓存口径校验：Redis conclusion 缓存（源自 MONTHINCA）vs 日度独立数据源"""
    ref_tag = " [仅供参考]" if reference_only else ""
    print(f"\n{'=' * 70}")
    print(f"层次2：缓存口径校验（{month}）{ref_tag}")
    print(f"  Redis conclusion:revenue vs LOCAL_DAILY_REVENUE（独立数据源交叉验证）")
    print(f"{'=' * 70}")

    try:
        import redis
        r = redis.Redis(host='localhost', port=6379, db=0,
                        decode_responses=True)
        r.ping()
    except Exception as e:
        print(f"  ⚠️ Redis 不可用({e})，跳过层次2")
        collector.add_layer(2, "缓存口径校验(Redis vs 日度)", 0, 0,
                            notes=f"Redis 不可用: {e}",
                            reference_only=reference_only)
        return 0, 0

    daily = get_daily_revenue_by_sa(conn, month)
    if not daily:
        print(f"  ⚠️ 无日度数据（{month}），跳过")
        collector.add_layer(2, "画像一致性(Redis)", 0, 0,
                            notes=f"无日度数据（{month}）",
                            reference_only=reference_only)
        return 0, 0

    # 扫描所有 conclusion:revenue:sa:*:{month} 的 key
    pattern = f"conclusion:revenue:sa:*:{month}"
    keys = list(r.scan_iter(match=pattern, count=5000))

    if not keys:
        print(f"  ⚠️ 无 Redis 结论缓存（pattern={pattern}），跳过")
        collector.add_layer(2, "画像一致性(Redis)", 0, 0,
                            notes=f"无 Redis 结论缓存",
                            reference_only=reference_only)
        return 0, 0

    issues = []
    ok_count = 0

    for key in keys:
        raw = r.get(key)
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            continue

        rev_wan = data.get("data", {}).get("总营收", 0)
        sid = str(data.get("server_id", ""))
        sa_name = data.get("service_area", "?")

        d = daily.get(sid)
        if not d or d["daily_sum"] <= 0:
            continue

        # 结论缓存存的是万元，日度是元
        cache_yuan = rev_wan * 10000
        diff = d["daily_sum"] - cache_yuan
        pct = diff / d["daily_sum"]

        if abs(pct) > REVENUE_DIFF_THRESHOLD:
            issues.append({
                "name": sa_name,
                "cache_wan": rev_wan,
                "daily_wan": round(d["daily_sum"] / 10000, 2),
                "pct": pct,
            })
        else:
            ok_count += 1

    if issues:
        issues.sort(key=lambda x: -abs(x["pct"]))
        print(f"\n  ❌ {len(issues)} 个 SA 结论缓存与日度不一致：")
        print(f"  {'服务区':<14} {'缓存(万)':>10} {'日度(万)':>10} {'差异':>6}")
        print(f"  {'-' * 44}")
        for i in issues:
            print(
                f"  {i['name']:<14} "
                f"{i['cache_wan']:>10.2f} "
                f"{i['daily_wan']:>10.2f} "
                f"{i['pct']:>5.1%}"
            )
    else:
        print(f"\n  ✅ 全部结论缓存与日度一致")

    print(f"\n  汇总: {ok_count} 个一致, {len(issues)} 个差异过大")

    collector.add_layer(2, "缓存口径校验(Redis vs 日度)", ok_count, len(issues),
                        issues=issues, reference_only=reference_only)
    return ok_count, len(issues)


# ============================================================
# 层次3：排名覆盖率
# ============================================================

def check_layer3(conn, month, collector, reference_only=False):
    """REVENUEREPORT / BAYONET 覆盖 SA 数 vs 全量"""
    ref_tag = " [仅供参考]" if reference_only else ""
    print(f"\n{'=' * 70}")
    print(f"层次3：排名覆盖率（{month}）{ref_tag}")
    print(f"{'=' * 70}")

    # 全量 SA 列表（同时取名称用于缺失展示）
    sa_names: Dict[str, str] = {}
    all_sids: Set[str] = set()
    try:
        all_sa_rows = conn.execute(
            "SELECT 服务区内码, 服务区名称 FROM NEWGETSERVERPARTLIST"
        ).fetchall()
        for r in all_sa_rows:
            if r[0]:
                sid = str(int(float(r[0])))
                all_sids.add(sid)
                if r[1]:
                    sa_names[sid] = str(r[1])
    except Exception:
        pass

    total_sa = len(all_sids) if all_sids else "?"
    layer_issues: List[Dict[str, Any]] = []

    # REVENUEREPORT 覆盖
    rev_data = get_revenuereport_by_sa(conn, month)
    rev_sids = set(rev_data.keys())
    rev_count = len(rev_sids)
    missing_rev = all_sids - rev_sids if all_sids else set()

    status = "✅" if rev_count >= COVERAGE_WARN_THRESHOLD else "⚠️"
    print(f"\n  {status} REVENUEREPORT: 覆盖 {rev_count} 个 SA（全量 {total_sa}）")
    if missing_rev:
        missing_info = [f"{sid}({sa_names.get(sid, '?')})" for sid in sorted(missing_rev)[:10]]
        print(f"    缺失 {len(missing_rev)} 个: {', '.join(missing_info)}")
        layer_issues.append({
            "source": "REVENUEREPORT", "count": rev_count,
            "total": total_sa, "missing": len(missing_rev),
        })

    # BAYONET 覆盖
    bay_sids = get_bayonet_sa_count(conn, month)
    bay_count = len(bay_sids)
    missing_bay = all_sids - bay_sids if all_sids else set()

    status = "✅" if bay_count >= COVERAGE_WARN_THRESHOLD else "⚠️"
    print(f"  {status} BAYONET:        覆盖 {bay_count} 个 SA（全量 {total_sa}）")
    if missing_bay:
        missing_info = [f"{sid}({sa_names.get(sid, '?')})" for sid in sorted(missing_bay)[:10]]
        print(f"    缺失 {len(missing_bay)} 个: {', '.join(missing_info)}")
        layer_issues.append({
            "source": "BAYONET", "count": bay_count,
            "total": total_sa, "missing": len(missing_bay),
        })

    issue_count = len(layer_issues)
    collector.add_layer(3, "排名覆盖率", 2 - issue_count, issue_count,
                        issues=layer_issues, reference_only=reference_only)
    return 2 - issue_count, issue_count


# ============================================================
# 层次4：口径基线（三源对比）
# ============================================================

def check_layer4(conn, month, collector, reference_only=False):
    """三源同 SA 同月金额对比（MONTHINCA / REVENUEREPORT / 日度）"""
    ref_tag = " [仅供参考]" if reference_only else ""
    print(f"\n{'=' * 70}")
    print(f"层次4：口径基线（{month}）{ref_tag}")
    print(f"  MONTHINCA vs REVENUEREPORT vs LOCAL_DAILY_REVENUE")
    print(f"{'=' * 70}")

    daily = get_daily_revenue_by_sa(conn, month)
    monthinca = get_monthinca_by_sa(conn, month)
    revreport = get_revenuereport_by_sa(conn, month)

    if not daily:
        print(f"  ⚠️ 无日度数据（{month}），跳过")
        collector.add_layer(4, "口径基线(三源对比)", 0, 0,
                            notes=f"无日度数据（{month}）",
                            reference_only=reference_only)
        return 0, 0

    # 合并所有 SA
    all_sids = set(daily.keys()) | set(monthinca.keys()) | set(revreport.keys())

    print(f"\n  {'服务区':<14} {'MONTHINCA':>12} {'REVENUEREPORT':>14} {'日度汇总':>12} {'MI偏差':>6} {'RR偏差':>6}")
    print(f"  {'-' * 70}")

    # 只打印有差异的（省空间）
    diff_rows = []
    ok_count = 0
    for sid in sorted(all_sids):
        mi_val = monthinca.get(sid, {}).get("monthinca", 0)
        rr_val = revreport.get(sid, {}).get("revreport", 0)
        d_val = daily.get(sid, {}).get("daily_sum", 0)
        name = (
            monthinca.get(sid, {}).get("name") or
            revreport.get(sid, {}).get("name") or
            daily.get(sid, {}).get("name") or
            f"ID:{sid}"
        )

        if d_val <= 0:
            continue

        mi_pct = (d_val - mi_val) / d_val if mi_val else 1.0
        rr_pct = (d_val - rr_val) / d_val if rr_val else 1.0

        if abs(mi_pct) > REVENUE_DIFF_THRESHOLD or abs(rr_pct) > REVENUE_DIFF_THRESHOLD:
            diff_rows.append((name, mi_val, rr_val, d_val, mi_pct, rr_pct))
        else:
            ok_count += 1

    for name, mi_val, rr_val, d_val, mi_pct, rr_pct in diff_rows:
        mi_flag = "❌" if abs(mi_pct) > REVENUE_DIFF_THRESHOLD else "  "
        rr_flag = "❌" if abs(rr_pct) > REVENUE_DIFF_THRESHOLD else "  "
        print(
            f"  {name:<14} "
            f"{mi_val:>12,.0f} "
            f"{rr_val:>14,.0f} "
            f"{d_val:>12,.0f} "
            f"{mi_flag}{mi_pct:>4.1%} "
            f"{rr_flag}{rr_pct:>4.1%}"
        )

    print(f"\n  汇总: {ok_count} 个三源一致, {len(diff_rows)} 个有差异")

    # Fix4: 收集 issues 到 collector（之前这里漏传，导致 Markdown/JSON 报告缺层次4数据）
    layer4_issues: List[Dict[str, Any]] = [
        {
            "name": name, "monthinca": mi_val, "revreport": rr_val,
            "daily": d_val, "mi_pct": mi_pct, "rr_pct": rr_pct,
        }
        for name, mi_val, rr_val, d_val, mi_pct, rr_pct in diff_rows
    ]
    collector.add_layer(4, "口径基线(三源对比)", ok_count, len(diff_rows),
                        issues=layer4_issues, reference_only=reference_only)
    return ok_count, len(diff_rows)


# ============================================================
# 层次5：车流交叉校验（BAYONET vs MONTHINCA）
# ============================================================

def check_layer5(conn, month, collector, reference_only=False):
    """BAYONET 入区车流 vs MONTHINCA 入区车流（仅供参考）

    注意：两数据源的"入区车流"统计口径不同。
    - MONTHINCA: 月度汇总入区车流（来源: newGetMonthINCAnalysis.入区车流数据对比.本年）
    - BAYONET: 车牌识别入区车流（来源: GetProvinceVehicleTreeList.入区车流）
    上游已确认两源的"断面流量"完全一致，但"入区车流"因采集方法不同天然存在差异。
    因此本层次强制标记为 reference_only，不计入问题统计。
    """
    # 口径不同，强制仅供参考
    reference_only = True
    print(f"\n{'=' * 70}")
    print(f"层次5：车流交叉校验（{month}） [仅供参考-口径不同]")
    print(f"  MONTHINCA 入区车流 vs BAYONET 入区车流（两源采集方法不同，差异属正常）")
    print(f"{'=' * 70}")

    mi_traffic = get_monthinca_traffic_by_sa(conn, month)
    bay_traffic = get_bayonet_traffic_by_sa(conn, month)

    if not mi_traffic:
        print(f"  ⚠️ 无 MONTHINCA 车流数据（{month}），跳过")
        collector.add_layer(5, "车流交叉(仅供参考-口径不同)", 0, 0,
                            notes="无 MONTHINCA 车流数据",
                            reference_only=True)
        return 0, 0

    if not bay_traffic:
        print(f"  ⚠️ 无 BAYONET 车流数据（{month}），跳过")
        collector.add_layer(5, "车流交叉(仅供参考-口径不同)", 0, 0,
                            notes="无 BAYONET 车流数据",
                            reference_only=True)
        return 0, 0

    # 只对比两者都有的 SA
    common_sids = set(mi_traffic.keys()) & set(bay_traffic.keys())
    only_mi = set(mi_traffic.keys()) - set(bay_traffic.keys())

    print(f"\n  MONTHINCA 有车流: {len(mi_traffic)} 个 SA")
    print(f"  BAYONET 有车流:   {len(bay_traffic)} 个 SA")
    print(f"  两者都有:         {len(common_sids)} 个 SA")
    if only_mi:
        print(f"  仅 MONTHINCA 有:  {len(only_mi)} 个 SA（BAYONET 未覆盖）")

    issues = []
    ok_count = 0

    for sid in common_sids:
        mi_val = mi_traffic[sid]["traffic"]
        bay_val = bay_traffic[sid]["traffic"]
        if mi_val <= 0:
            continue
        pct = abs(mi_val - bay_val) / max(mi_val, bay_val)
        if pct > TRAFFIC_DIFF_THRESHOLD:
            direction = "BAY偏低" if bay_val < mi_val else "BAY偏高"
            issues.append({
                "sid": sid,
                "name": mi_traffic[sid]["name"],
                "monthinca": mi_val,
                "bayonet": bay_val,
                "pct": pct,
                "direction": direction,
            })
        else:
            ok_count += 1

    if issues:
        issues.sort(key=lambda x: -x["pct"])
        print(f"\n  ℹ️ {len(issues)} 个 SA 差异 >{TRAFFIC_DIFF_THRESHOLD*100:.0f}%（口径不同，仅供参考）：")
        print(f"  {'服务区':<14} {'MONTHINCA':>10} {'BAYONET':>10} {'差异%':>8} {'方向'}")
        print(f"  {'-' * 55}")
        for i in issues:
            print(
                f"  {i['name']:<14} "
                f"{i['monthinca']:>10,.0f} "
                f"{i['bayonet']:>10,.0f} "
                f"{i['pct']:>7.1%} "
                f"{i['direction']}"
            )
    else:
        print(f"\n  ✅ 全部 SA 车流差异 <{TRAFFIC_DIFF_THRESHOLD*100:.0f}%")

    print(f"\n  汇总: {ok_count} 个一致, {len(issues)} 个差异（仅供参考，不计入问题统计）")

    collector.add_layer(5, "车流交叉(仅供参考-口径不同)", ok_count,
                        len(issues), issues=issues, reference_only=True,
                        notes="MONTHINCA与BAYONET入区车流采集方法不同，差异属正常，不计入问题统计")
    return ok_count, len(issues)


# ============================================================
# 主流程
# ============================================================

LAYER_FUNCS = {
    1: check_layer1,
    2: check_layer2,
    3: check_layer3,
    4: check_layer4,
    5: check_layer5,
}


def main():
    parser = argparse.ArgumentParser(description="数据健康校验")
    parser.add_argument("--month", help="指定月份 YYYYMM（默认最近1个月）")
    parser.add_argument("--all-months", action="store_true",
                        help="校验所有有日度数据的月份")
    parser.add_argument("--layer", type=int, help="只跑指定层次 (1/2/3/4/5)")
    parser.add_argument("--output", help="输出报告文件路径（.md 或 .json）")
    args = parser.parse_args()

    conn = get_conn()
    available = get_available_months(conn)
    daily_months = get_months_with_daily(conn)

    if args.all_months:
        # 智能限制：只校验有日度数据的月份（交集）
        months = [m for m in available if m in daily_months]
        if not months:
            months = available[-2:]  # 兜底取最近2个月
    elif args.month:
        months = [args.month]
    else:
        # 默认最近1个月
        months = available[-1:] if available else []

    if not months:
        print("⚠️ 无可校验的月份")
        sys.exit(1)

    collector = ReportCollector()

    # Fix6: 数据新鲜度检查
    freshness = get_data_freshness(conn)
    collector.freshness = freshness  # type: ignore[attr-defined]

    print(f"{'#' * 70}")
    print(f"# 数据健康校验报告")
    print(f"# 时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"# 数据库: {MIRROR_DB}")
    print(f"# 数据新鲜度: " + "  ".join(f"{k}:{v}" for k, v in freshness.items()))
    print(f"# 校验月份: {', '.join(months)}")
    print(f"{'#' * 70}")

    layers = list(LAYER_FUNCS.keys()) if not args.layer else [args.layer]

    for month in months:
        # 日度完整性预检
        daily_days = get_daily_day_count(conn, month)
        total_days = get_month_total_days(month)
        is_incomplete = daily_days < total_days * DAILY_COMPLETENESS_THRESHOLD

        print(f"\n\n{'*' * 70}")
        if is_incomplete and daily_days > 0:
            print(f"* 月份: {month}  ⚠️ 日度不完整({daily_days}/{total_days}天，结果仅供参考)")
        elif daily_days == 0:
            print(f"* 月份: {month}  ⚠️ 无日度数据")
        else:
            print(f"* 月份: {month}  ✅ 日度完整({daily_days}/{total_days}天)")
        print(f"{'*' * 70}")

        # 日度不完整时标记为 reference_only（不计入问题统计）
        reference_only = is_incomplete

        if daily_days > 0:
            collector.start_month(month,
                                  incomplete_days=daily_days if is_incomplete else None,
                                  total_days=total_days if is_incomplete else None)
        else:
            collector.start_month(month, incomplete_days=0, total_days=total_days)

        for layer in layers:
            func = LAYER_FUNCS.get(layer)
            if func:
                func(conn, month, collector, reference_only=reference_only)

    # 总结
    total_ok, total_issues = collector.get_summary()
    print(f"\n\n{'=' * 70}")
    print(f"校验总结")
    print(f"  一致项: {total_ok}")
    print(f"  问题项: {total_issues}")
    if total_issues == 0:
        print(f"  状态: ✅ 全部健康")
    else:
        print(f"  状态: ⚠️ 有 {total_issues} 个待关注项")
    print(f"{'=' * 70}")

    # 输出报告文件
    if args.output:
        output_path = Path(args.output)
        if output_path.suffix == ".json":
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(collector.to_json(), f, ensure_ascii=False, indent=2)
            print(f"\n📄 JSON 报告已输出: {output_path}")
        else:
            # 默认 Markdown
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(collector.to_markdown())
            print(f"\n📄 Markdown 报告已输出: {output_path}")

    conn.close()
    sys.exit(0 if total_issues == 0 else 1)


if __name__ == "__main__":
    main()
