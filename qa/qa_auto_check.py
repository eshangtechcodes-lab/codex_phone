"""
QA 自动核对脚本 — Layer 1: Python 秒级数字核对 + 规则引擎

独立项目版本 — 所有配置从 config.py 读取

输入: qa_runner 生成的 JSON 报告 + dameng_mirror.db
输出: auto_check.json（含每轮数字偏差、分类、工具选择检查结果）

用法:
    python qa/qa_auto_check.py --input reports/report.json
    python qa/qa_auto_check.py --input reports/report.json --db path/to/dameng_mirror.db
"""

import argparse
import json
import re
import sqlite3
import sys
import io
from datetime import datetime
from pathlib import Path

# Windows 控制台 UTF-8 输出
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

# 从 config 读取配置
sys.path.insert(0, str(Path(__file__).parent))
from config import DB_PATH, REPORTS_DIR


# ============================================================
# 1. 数字提取器
# ============================================================

def extract_numbers(text: str) -> dict:
    """从 AI 回答中提取关键数字（增强版：覆盖营收、车流、利润等多维度）"""
    result = {}

    # 提取 "XXX万元" 或 "XXX 万元" 格式的营收数字
    revenue_matches = re.findall(r'[*]*(\d[\d,]*\.?\d*)\s*万元[*]*', text)
    if revenue_matches:
        result['revenue_wan'] = float(revenue_matches[0].replace(',', ''))
        result['all_revenue_wan'] = [float(m.replace(',', '')) for m in revenue_matches]

    # 提取 "XXX元"（不带万，如 "2,590,304.35 元"），转换为万元
    if 'revenue_wan' not in result:
        raw_yuan = re.findall(r'(\d[\d,]*\.?\d*)\s*元(?![/㎡])', text)
        if raw_yuan:
            val = float(raw_yuan[0].replace(',', ''))
            if val > 10000:  # 超过1万才视为营收数字
                result['revenue_wan'] = round(val / 10000, 2)

    # 提取同比百分比: "同比 +XX.XX%" 或 "同比增长XX.XX%" 或 "同比下降XX%"
    yoy_match = re.search(r'同比\s*[增长降下]*\s*([+-]?\s*\d+\.?\d*)\s*%', text)
    if yoy_match:
        yoy_val = float(yoy_match.group(1).replace(' ', ''))
        # 如果含"降"或"下"，取负值
        if re.search(r'同比\s*[降下]', text) and yoy_val > 0:
            yoy_val = -yoy_val
        result['yoy_pct'] = yoy_val

    # 提取环比百分比
    mom_match = re.search(r'环比\s*[增长降下]*\s*([+-]?\s*\d+\.?\d*)\s*%', text)
    if mom_match:
        mom_val = float(mom_match.group(1).replace(' ', ''))
        if re.search(r'环比\s*[降下]', text) and mom_val > 0:
            mom_val = -mom_val
        result['mom_pct'] = mom_val

    # 提取排名: "第X位" 或 "第X名" 或 "排名第X" 或 "排名X"
    rank_match = re.search(r'(?:排名)?第\s*(\d+)\s*[位名]', text)
    if rank_match:
        result['rank'] = int(rank_match.group(1))

    # 提取车流: "XX万辆"
    flow_matches = re.findall(r'(\d[\d,]*\.?\d*)\s*万\s*辆', text)
    if flow_matches:
        result['traffic_wan'] = float(flow_matches[0].replace(',', ''))

    # 提取断面车流: "断面XX万" 或 "断面流量XX万"
    section_match = re.search(r'断面[流量]*[^\d]*(\d[\d,]*\.?\d*)\s*万', text)
    if section_match:
        result['section_flow_wan'] = float(section_match.group(1).replace(',', ''))

    # 提取入区车流: "入区XX万" 或 "入区车流XX万"
    entry_match = re.search(r'入区[车流]*[^\d]*(\d[\d,]*\.?\d*)\s*万', text)
    if entry_match:
        result['entry_flow_wan'] = float(entry_match.group(1).replace(',', ''))

    # 提取入区率
    entry_rate_match = re.search(r'入区率\s*(\d+\.?\d*)\s*%', text)
    if entry_rate_match:
        result['entry_rate'] = float(entry_rate_match.group(1))

    # 提取利润/盈利/亏损: "利润XX万" 或 "盈利XX万" 或 "亏损XX万"
    profit_match = re.search(r'(?:利润|盈利|亏损)[^\d]*(\d[\d,]*\.?\d*)\s*万', text)
    if profit_match:
        result['profit_wan'] = float(profit_match.group(1).replace(',', ''))

    return result


# ============================================================
# 2. SQL 查询生成器
# ============================================================

def identify_query_context(question: str, answer_text: str = '') -> dict:
    """从问题（和回答）中识别查询上下文（服务区名、时间、指标类型）"""
    ctx = {}

    # 识别服务区名
    sa_match = re.search(r'([\u4e00-\u9fa5]{2,8}服务区)', question)
    if sa_match:
        ctx['service_area'] = sa_match.group(1)
    # 回答中也可能提到服务区名（追问场景下问题没有 SA 但回答有）
    if not ctx.get('service_area') and answer_text:
        sa_match2 = re.search(r'([\u4e00-\u9fa5]{2,8}服务区)', answer_text)
        if sa_match2:
            ctx['service_area'] = sa_match2.group(1)
            ctx['sa_from_answer'] = True

    # 识别片区
    region_match = re.search(r'(皖[中南北东西])', question)
    if region_match:
        ctx['region'] = region_match.group(1) + '管理中心'

    # 识别月份（优先从问题中提取）
    month_match = re.search(r'(\d+)月', question)
    if month_match:
        month = int(month_match.group(1))
        ctx['month'] = f"2026{month:02d}"
    elif answer_text:
        # 从回答中提取月份
        ans_month = re.search(r'2026\s*年\s*(\d{1,2})\s*月', answer_text)
        if ans_month:
            month = int(ans_month.group(1))
            ctx['month'] = f"2026{month:02d}"
            ctx['month_from_answer'] = True

    # 识别指标类型（同时检查问题和回答）
    if any(kw in question for kw in ['营收', '对客销售', '营业收入', '赚钱', '经营']):
        ctx['metric'] = 'revenue'
    elif any(kw in question for kw in ['车流', '入区', '断面', '流量']):
        ctx['metric'] = 'traffic'
    elif any(kw in question for kw in ['利润', '亏损', '盈利', '盈亏']):
        ctx['metric'] = 'profit'
    elif any(kw in question for kw in ['业主', '除税']):
        ctx['metric'] = 'owner_revenue'
    elif any(kw in question for kw in ['排名', '前5', '前10', '最高', '最低']):
        ctx['metric'] = 'ranking'
    # 如果问题没有关键词，从回答推断
    elif answer_text:
        if any(kw in answer_text[:300] for kw in ['对客销售', '营收', '营业收入']):
            ctx['metric'] = 'revenue'
        elif any(kw in answer_text[:300] for kw in ['断面', '入区', '车流']):
            ctx['metric'] = 'traffic'

    return ctx


def _parse_monthinca_children(cur, month: str) -> list:
    """从 NEWGETMONTHINCANALYSIS 解析 children 列表（与模型同源）"""
    cur.execute(
        "SELECT children FROM NEWGETMONTHINCANALYSIS "
        "WHERE STATISTICS_MONTH=?", (month,)
    )
    row = cur.fetchone()
    if not row or not row[0]:
        return []
    try:
        children = json.loads(row[0])
        return children if isinstance(children, list) else []
    except (json.JSONDecodeError, TypeError):
        return []


def _get_sales_compare(child: dict) -> dict:
    """从 child 中提取对客销售对比字段"""
    sc = child.get("对客销售对比", {})
    if isinstance(sc, str):
        try:
            sc = json.loads(sc)
        except (json.JSONDecodeError, TypeError):
            sc = {}
    return sc if isinstance(sc, dict) else {}


def query_db_truth(ctx: dict, db_path: str) -> dict:
    """根据上下文查 DB 获取真实值
    
    营收类指标与模型同源：从 NEWGETMONTHINCANALYSIS 的 children 中
    读取 对客销售对比.本年 / 增长率 / 环比增长率，避免接口口径差异导致误报。
    """
    truth = {}
    if not ctx.get('service_area'):
        return truth

    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()

        sa_name = ctx['service_area']
        month = ctx.get('month', '202602')

        if ctx.get('metric') in ('revenue', None):
            # 与模型同源：从 NEWGETMONTHINCANALYSIS children 提取
            children = _parse_monthinca_children(cur, month)
            target = None
            for child in children:
                if child.get("服务区名称") == sa_name:
                    target = child
                    break

            if target:
                sc = _get_sales_compare(target)
                # 营收：对客销售对比.本年（单位：元）→ 转万元
                raw_val = sc.get("本年")
                if raw_val is not None:
                    try:
                        truth['revenue_wan'] = round(float(raw_val) / 10000, 2)
                    except (ValueError, TypeError):
                        pass

                # 同比：直接读增长率字段（模型也是读这个字段）
                yoy_val = sc.get("增长率")
                if yoy_val is not None:
                    try:
                        truth['yoy_pct'] = round(float(yoy_val), 2)
                    except (ValueError, TypeError):
                        pass

                # 环比：直接读环比增长率字段
                mom_val = sc.get("环比增长率")
                if mom_val is not None:
                    try:
                        truth['mom_pct'] = round(float(mom_val), 2)
                    except (ValueError, TypeError):
                        pass

            # 排名：遍历所有 children 按对客销售排序
            if children:
                sa_revenues = []
                for child in children:
                    sc = _get_sales_compare(child)
                    name = child.get("服务区名称", "")
                    raw = sc.get("本年")
                    if name and raw is not None:
                        try:
                            sa_revenues.append((name, float(raw)))
                        except (ValueError, TypeError):
                            pass
                # 按营收降序排名
                sa_revenues.sort(key=lambda x: x[1], reverse=True)
                for rank, (name, _) in enumerate(sa_revenues, 1):
                    if name == sa_name:
                        truth['rank'] = rank
                        break

            # 车流：与模型同源，从 NEWGETMONTHINCANALYSIS 的入区车流数据对比读取
            if children and target:
                tc = target.get("入区车流数据对比", {})
                if isinstance(tc, str):
                    try:
                        tc = json.loads(tc)
                    except (json.JSONDecodeError, TypeError):
                        tc = {}
                if isinstance(tc, dict):
                    # 入区车流（单位：辆次）→ 转万
                    raw_flow = tc.get("本年")
                    if raw_flow is not None:
                        try:
                            flow = float(raw_flow)
                            truth['entry_flow'] = flow
                            truth['entry_flow_wan'] = round(flow / 10000, 2)
                        except (ValueError, TypeError):
                            pass
                    # 入区车流同比
                    flow_yoy = tc.get("增长率")
                    if flow_yoy is not None:
                        try:
                            truth['traffic_yoy_pct'] = round(float(flow_yoy), 2)
                        except (ValueError, TypeError):
                            pass

            # 业主收入：与模型同源
            if children and target:
                oc = target.get("业主营业收入（除税）对比", {})
                if isinstance(oc, str):
                    try:
                        oc = json.loads(oc)
                    except (json.JSONDecodeError, TypeError):
                        oc = {}
                if isinstance(oc, dict):
                    raw_owner = oc.get("本年")
                    if raw_owner is not None:
                        try:
                            truth['owner_revenue_wan'] = round(float(raw_owner) / 10000, 2)
                        except (ValueError, TypeError):
                            pass
                    owner_yoy = oc.get("增长率")
                    if owner_yoy is not None:
                        try:
                            truth['owner_yoy_pct'] = round(float(owner_yoy), 2)
                        except (ValueError, TypeError):
                            pass

        # 车流排名（仍从 NEWTRAFFICFLOWRANKING 补充，修复 TEXT 类型 Bug）
        if ctx.get('metric') in ('traffic', None):
            try:
                cur.execute(
                    "SELECT [断面流量], [入区车流], [入区车流排行（全省）] "
                    "FROM NEWTRAFFICFLOWRANKING "
                    "WHERE [服务区名称]=?",
                    (sa_name,)
                )
                row = cur.fetchone()
                if row:
                    # 修复 TEXT 类型：安全转 float
                    try:
                        sf = float(row[0]) if row[0] else None
                        truth['section_flow'] = sf
                        truth['section_flow_wan'] = round(sf / 10000, 2) if sf else None
                    except (ValueError, TypeError):
                        pass
                    if 'entry_flow' not in truth:
                        try:
                            ef = float(row[1]) if row[1] else None
                            truth['entry_flow'] = ef
                            truth['entry_flow_wan'] = round(ef / 10000, 2) if ef else None
                        except (ValueError, TypeError):
                            pass
                    try:
                        truth['traffic_rank'] = int(float(row[2])) if row[2] else None
                    except (ValueError, TypeError):
                        pass
                    # 入区率
                    sf = truth.get('section_flow')
                    ef = truth.get('entry_flow')
                    if sf and ef and sf > 0:
                        truth['entry_rate'] = round(ef / sf * 100, 2)
            except Exception:
                pass  # 表不存在时静默跳过

        conn.close()
    except Exception as e:
        truth['_error'] = str(e)

    return truth


# ============================================================
# 3. 偏差计算
# ============================================================

def check_deviation(ai_nums: dict, db_truth: dict) -> list:
    """对比 AI 数字和 DB 真实值，返回偏差列表（增强版：支持车流、入区率）"""
    deviations = []

    # 营收对比
    if 'revenue_wan' in ai_nums and 'revenue_wan' in db_truth:
        ai_val = ai_nums['revenue_wan']
        db_val = db_truth['revenue_wan']
        diff = abs(ai_val - db_val)
        pct = round(diff / db_val * 100, 2) if db_val > 0 else 0
        deviations.append({
            'field': 'revenue_wan',
            'ai': ai_val, 'db': db_val,
            'diff': round(diff, 2), 'diff_pct': pct,
            'severity': 'critical' if pct > 5 else ('warning' if pct > 1 else 'ok'),
        })

    # 同比对比
    if 'yoy_pct' in ai_nums and 'yoy_pct' in db_truth:
        ai_val = ai_nums['yoy_pct']
        db_val = db_truth['yoy_pct']
        diff = abs(ai_val - db_val)
        deviations.append({
            'field': 'yoy_pct',
            'ai': ai_val, 'db': db_val,
            'diff': round(diff, 2), 'diff_pct': round(diff, 2),
            'severity': 'critical' if diff > 5 else ('warning' if diff > 1 else 'ok'),
        })

    # 环比对比
    if 'mom_pct' in ai_nums and 'mom_pct' in db_truth:
        ai_val = ai_nums['mom_pct']
        db_val = db_truth['mom_pct']
        diff = abs(ai_val - db_val)
        deviations.append({
            'field': 'mom_pct',
            'ai': ai_val, 'db': db_val,
            'diff': round(diff, 2), 'diff_pct': round(diff, 2),
            'severity': 'critical' if diff > 5 else ('warning' if diff > 1 else 'ok'),
        })

    # 排名对比
    if 'rank' in ai_nums and 'rank' in db_truth:
        ai_val = ai_nums['rank']
        db_val = db_truth['rank']
        diff = abs(ai_val - db_val)
        deviations.append({
            'field': 'rank',
            'ai': ai_val, 'db': db_val, 'diff': diff,
            'severity': 'critical' if diff > 3 else ('warning' if diff > 0 else 'ok'),
        })

    # 断面车流对比（万单位）
    if 'section_flow_wan' in ai_nums and 'section_flow_wan' in db_truth:
        ai_val = ai_nums['section_flow_wan']
        db_val = db_truth['section_flow_wan']
        if db_val:
            diff = abs(ai_val - db_val)
            pct = round(diff / db_val * 100, 2) if db_val > 0 else 0
            deviations.append({
                'field': 'section_flow_wan',
                'ai': ai_val, 'db': db_val,
                'diff': round(diff, 2), 'diff_pct': pct,
                'severity': 'critical' if pct > 10 else ('warning' if pct > 3 else 'ok'),
            })

    # 入区车流对比（万单位）
    if 'entry_flow_wan' in ai_nums and 'entry_flow_wan' in db_truth:
        ai_val = ai_nums['entry_flow_wan']
        db_val = db_truth['entry_flow_wan']
        if db_val:
            diff = abs(ai_val - db_val)
            pct = round(diff / db_val * 100, 2) if db_val > 0 else 0
            deviations.append({
                'field': 'entry_flow_wan',
                'ai': ai_val, 'db': db_val,
                'diff': round(diff, 2), 'diff_pct': pct,
                'severity': 'critical' if pct > 10 else ('warning' if pct > 3 else 'ok'),
            })

    # 入区率对比
    if 'entry_rate' in ai_nums and 'entry_rate' in db_truth:
        ai_val = ai_nums['entry_rate']
        db_val = db_truth['entry_rate']
        diff = abs(ai_val - db_val)
        deviations.append({
            'field': 'entry_rate',
            'ai': ai_val, 'db': db_val, 'diff': round(diff, 2),
            'severity': 'critical' if diff > 5 else ('warning' if diff > 1 else 'ok'),
        })

    return deviations


# ============================================================
# 4. 工具选择规则引擎
# ============================================================

TOOL_RULES = {
    'revenue': {
        'keywords': ['营收', '对客销售', '营业收入', '赚钱', '营收情况'],
        'expected_tools': ['get_revenue', 'query_revenue_report', 'quick_metric',
                          'revenue', 'get_daily_revenue'],
    },
    'traffic': {
        'keywords': ['车流', '入区', '断面', '流量'],
        'expected_tools': ['get_traffic', 'get_daily_traffic', 'traffic',
                          'dashboard_traffic'],
    },
    'profit': {
        'keywords': ['利润', '亏损', '盈利', '盈亏'],
        'expected_tools': ['get_merchant_profit', 'merchant_profit',
                          'get_business_trade_profit'],
    },
    'owner_revenue': {
        'keywords': ['业主营业收入', '业主营收', '除税'],
        'expected_tools': ['get_owner_revenue', 'owner_revenue'],
        'forbidden_tools': ['get_revenue'],
    },
    'contract': {
        'keywords': ['合同', '到期'],
        'expected_tools': ['get_contract', 'contract'],
    },
    'shop': {
        'keywords': ['门店', '店铺', '商户'],
        'expected_tools': ['query_shop_revenue', 'query_shops_full', 'get_merchant'],
    },
}


def check_tool_selection(question: str, diagnostics: dict) -> dict:
    """检查工具选择是否匹配问题意图"""
    result = {'checked': False, 'issues': []}

    tool_calls = diagnostics.get('tool_calls_detail', [])
    if not tool_calls:
        return result

    result['checked'] = True
    actual_tools = []
    for tc in tool_calls:
        name = tc.get('tool_name') or tc.get('name') or tc.get('tool') or tc.get('path', '')
        actual_tools.append(name)
    result['actual_tools'] = actual_tools

    for rule_name, rule in TOOL_RULES.items():
        if any(kw in question for kw in rule['keywords']):
            result['expected_category'] = rule_name
            forbidden = rule.get('forbidden_tools', [])
            for ft in forbidden:
                if any(ft in at for at in actual_tools):
                    result['issues'].append({
                        'type': 'TOOL_WRONG',
                        'detail': f"问题含 '{rule_name}' 关键词，不应使用 {ft}",
                    })
            break

    return result


# ============================================================
# 5. thinking_chain 异常扫描
# ============================================================

def scan_thinking_chain(diagnostics: dict, question: str) -> list:
    """扫描 thinking_chain 中的异常模式"""
    issues = []
    chain = diagnostics.get('thinking_chain', [])
    snapshot = diagnostics.get('semantic_snapshot', {})

    if not chain and not snapshot:
        return issues

    if chain and len(chain) < 2:
        issues.append({
            'type': 'CHAIN_TOO_SHORT',
            'detail': f"thinking_chain 仅 {len(chain)} 步，可能跳过了分析",
        })

    chain_text = ' '.join(str(s) for s in chain)
    if any(kw in chain_text for kw in ['缓存', '直接返回', 'cache', '已有结论']):
        issues.append({
            'type': 'CACHE_STALE_RISK',
            'detail': "thinking_chain 中出现缓存相关描述",
            'chain_excerpt': chain_text[:200],
        })

    if snapshot:
        entities = snapshot.get('entities', [])
        sa_in_question = re.search(r'[\u4e00-\u9fa5]{2,8}服务区', question)
        if sa_in_question and not entities:
            issues.append({
                'type': 'ENTITY_MISMATCH_RISK',
                'detail': f"问题含 '{sa_in_question.group()}' 但 entities 为空",
            })

        if snapshot.get('server_inherited') and sa_in_question:
            issues.append({
                'type': 'CONTEXT_POLLUTION_RISK',
                'detail': "server_inherited=True 但问题中有新服务区名",
            })

    return issues


# ============================================================
# 6. 主流程
# ============================================================

def auto_check(report_path: str, db_path: str) -> dict:
    """执行完整的 Layer 1 自动核对"""
    with open(report_path, 'r', encoding='utf-8') as f:
        report = json.load(f)

    results = []
    stats = {
        'total_turns': 0, 'checked_turns': 0,
        'critical': 0, 'warning': 0, 'ok': 0, 'no_data': 0,
    }

    scenarios = report.get('scenarios', [])
    for si, scenario in enumerate(scenarios):
        scenario_result = {
            'name': scenario['name'],
            'turns': [],
        }

        for turn in scenario.get('turns', []):
            stats['total_turns'] += 1
            text = turn.get('full_response') or turn.get('report_preview', '')
            question = turn.get('question', '')
            diagnostics = turn.get('diagnostics', {})

            turn_check = {
                'turn': turn['turn'],
                'question': question,
                'actual_type': turn.get('actual_type', ''),
                'expected_type': turn.get('expected_type', ''),
                'passed_runner': turn.get('passed', True),
            }

            # 分类检查
            if turn.get('expected_type') and turn.get('actual_type'):
                turn_check['classify_match'] = (
                    turn['expected_type'] == turn['actual_type']
                )

            # 数字提取 + SQL 核对（增强：将回答文本传入上下文识别）
            ctx = identify_query_context(question, text)
            ai_nums = extract_numbers(text) if text else {}
            db_truth = query_db_truth(ctx, db_path) if ctx.get('service_area') else {}

            turn_check['ai_numbers'] = ai_nums
            turn_check['db_truth'] = db_truth

            if ai_nums and db_truth:
                deviations = check_deviation(ai_nums, db_truth)
                turn_check['deviations'] = deviations
                stats['checked_turns'] += 1

                max_sev = 'ok'
                for d in deviations:
                    if d['severity'] == 'critical':
                        max_sev = 'critical'
                        break
                    elif d['severity'] == 'warning':
                        max_sev = 'warning'
                turn_check['max_severity'] = max_sev
                stats[max_sev] += 1
            else:
                turn_check['deviations'] = []
                turn_check['max_severity'] = 'skipped'
                stats['no_data'] += 1

            # 工具选择检查
            if diagnostics:
                turn_check['tool_check'] = check_tool_selection(question, diagnostics)
                turn_check['chain_issues'] = scan_thinking_chain(diagnostics, question)
            else:
                turn_check['tool_check'] = {'checked': False}
                turn_check['chain_issues'] = []

            scenario_result['turns'].append(turn_check)

        results.append(scenario_result)

    return {
        'type': 'auto_check',
        'source': report_path,
        'db': db_path,
        'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'stats': stats,
        'scenarios': results,
    }


def print_summary(check_result: dict):
    """打印核对结果摘要"""
    stats = check_result['stats']
    print(f"\n{'='*60}")
    print(f"Layer 1 自动核对结果")
    print(f"{'='*60}")
    print(f"  总轮次: {stats['total_turns']}")
    print(f"  已核对: {stats['checked_turns']}")
    print(f"  严重偏差: {stats['critical']}")
    print(f"  轻微偏差: {stats['warning']}")
    print(f"  准确: {stats['ok']}")
    print(f"  无法核对: {stats['no_data']}")

    for scenario in check_result['scenarios']:
        has_issues = False
        for turn in scenario['turns']:
            if turn.get('max_severity') in ('critical', 'warning'):
                if not has_issues:
                    print(f"\n--- {scenario['name']} ---")
                    has_issues = True
                sev = '!!!' if turn['max_severity'] == 'critical' else '!'
                print(f"  [{sev}] 轮{turn['turn']}: {turn['question']}")
                for d in turn['deviations']:
                    if d['severity'] != 'ok':
                        print(f"       {d['field']}: AI={d['ai']} DB={d['db']} "
                              f"偏差={d['diff']} ({d['severity']})")

    if stats['critical'] == 0 and stats['warning'] == 0:
        print(f"\n  全部核对通过，无偏差!")


def generate_markdown_report(check_result: dict) -> str:
    """生成 Markdown 格式的核对报告"""
    stats = check_result['stats']
    lines = [
        f"# Layer 1 自动核对报告",
        f"",
        f"> 时间: {check_result['timestamp']} | "
        f"核对: {stats['checked_turns']}/{stats['total_turns']} | "
        f"严重: {stats['critical']} | 警告: {stats['warning']} | 准确: {stats['ok']}",
        f"",
    ]

    lines.append("## 场景汇总\n")
    lines.append("| 场景 | 轮次 | 核对 | 严重 | 警告 | 准确 |")
    lines.append("|------|------|------|------|------|------|")

    for scenario in check_result['scenarios']:
        name = scenario['name'][:20]
        total = len(scenario['turns'])
        checked = sum(1 for t in scenario['turns'] if t.get('deviations'))
        critical = sum(1 for t in scenario['turns'] if t.get('max_severity') == 'critical')
        warning = sum(1 for t in scenario['turns'] if t.get('max_severity') == 'warning')
        ok = sum(1 for t in scenario['turns'] if t.get('max_severity') == 'ok')
        lines.append(f"| {name} | {total} | {checked} | {critical} | {warning} | {ok} |")

    has_dev = False
    for scenario in check_result['scenarios']:
        for turn in scenario['turns']:
            if turn.get('max_severity') in ('critical', 'warning'):
                if not has_dev:
                    lines.append(f"\n## 偏差详情\n")
                    has_dev = True
                sev_icon = "!!!" if turn['max_severity'] == 'critical' else "!"
                lines.append(f"### [{sev_icon}] {scenario['name']} 轮{turn['turn']}: {turn['question']}\n")
                lines.append("| 指标 | AI值 | DB值 | 偏差 | 级别 |")
                lines.append("|------|------|------|------|------|")
                for d in turn['deviations']:
                    lines.append(f"| {d['field']} | {d['ai']} | {d['db']} | {d['diff']} | {d['severity']} |")
                lines.append("")

    if not has_dev:
        lines.append(f"\n## 全部准确 ✅\n")
        lines.append("所有可核对数字均与数据库一致。\n")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="QA Layer 1 自动核对")
    parser.add_argument("--input", required=True, help="qa_runner JSON 报告路径")
    parser.add_argument("--db", default=DB_PATH, help="dameng_mirror.db 路径")
    parser.add_argument("--output", help="JSON 输出路径（默认自动生成）")
    parser.add_argument("--report", help="Markdown 报告路径（默认自动生成）")
    args = parser.parse_args()

    print(f"数据库: {args.db}")
    print(f"报告: {args.input}")

    result = auto_check(args.input, args.db)
    print_summary(result)

    output_path = args.output or args.input.replace('.json', '_autocheck.json')
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"\nJSON: {output_path}")

    report_path = args.report or args.input.replace('.json', '_autocheck.md')
    md = generate_markdown_report(result)
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(md)
    print(f"报告: {report_path}")


if __name__ == "__main__":
    main()
