"""
QA 测试运行器 — 批量发送问题到正式服务器并记录结果

独立项目版本 — 所有配置从 config.py 读取

用法:
    # 跑默认 Golden Set
    python qa/qa_runner.py

    # 跑指定题目
    python qa/qa_runner.py --questions "新桥服务区2月营收" "巢湖服务区车流量"

    # 多轮语义切换测试
    python qa/qa_runner.py --multi-turn --run-id multi_01

    # 指定 LLM 模式
    python qa/qa_runner.py --llm-mode qwen --run-id qwen_01
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import URLError

# Windows 控制台 UTF-8 输出
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# 从 config 读取配置
sys.path.insert(0, str(Path(__file__).parent))
from config import API_URL, TIMEOUT, USER_ID_PREFIX, REPORTS_DIR

DIAGNOSTICS_URL = "https://llm.eshangtech.com/api/admin/diagnostics/"
LLM_MODE = None  # 请求级 LLM 模式（qwen/codex/hybrid），None 表示用服务器默认

# Golden Set — 基线测试题库（43 题）
# 设计原则：服务区分散、工具覆盖全、场景多样化
GOLDEN_SET = [
    # === 基础营收（5 题）===
    {"question": "新桥服务区2月营收情况", "expected_type": "A", "tags": ["营收", "单SA"]},
    {"question": "巢湖服务区1月对客销售", "expected_type": "A", "tags": ["营收", "口径"]},
    {"question": "龙门寺服务区营业收入", "expected_type": "A", "tags": ["营收", "口径"]},
    {"question": "洪林服务区最近3个月营收趋势", "expected_type": "A", "tags": ["营收", "趋势"]},
    {"question": "大墅服务区今年营收怎么样", "expected_type": "A", "tags": ["营收", "模糊时间"]},

    # === 车流（3 题）===
    {"question": "太湖服务区2月车流量", "expected_type": "A", "tags": ["车流", "单SA"]},
    {"question": "仓镇服务区日均车流多少", "expected_type": "A", "tags": ["车流", "日均"]},
    {"question": "全省车流排名前5", "expected_type": "A", "tags": ["车流", "排名"]},

    # === 排名/片区/对比（4 题）===
    {"question": "全省营收排名前5的服务区", "expected_type": "A", "tags": ["排名", "全省"]},
    {"question": "皖南片区哪个服务区营收最高", "expected_type": "A", "tags": ["排名", "片区"]},
    {"question": "皖中管理中心服务区整体表现", "expected_type": "A", "tags": ["片区", "整体"]},
    {"question": "洪林和巢湖服务区营收对比", "expected_type": "A", "tags": ["对比", "双SA"]},

    # === 商户/品牌（4 题）===
    {"question": "肥东服务区有哪些亏损商户", "expected_type": "A", "tags": ["商户", "亏损"]},
    {"question": "麦当劳在哪些服务区有门店", "expected_type": "A", "tags": ["品牌", "分布"]},
    {"question": "香铺服务区门店数量", "expected_type": "A", "tags": ["商户", "数量"]},
    {"question": "梅山服务区自营和联营门店各有多少", "expected_type": "A", "tags": ["商户", "类型"]},

    # === 财务/合同（3 题）===
    {"question": "新桥服务区商户利润排名", "expected_type": "A", "tags": ["财务", "利润"]},
    {"question": "宣城服务区合同到期情况", "expected_type": "A", "tags": ["合同", "到期"]},
    {"question": "方兴大道服务区提成比例多少", "expected_type": "A", "tags": ["财务", "提成"]},

    # === 口径/利润消歧（5 题）===
    {"question": "洪林服务区2月利润多少", "expected_type": "A", "tags": ["口径消歧", "利润≠营收"]},
    {"question": "业主营业收入排名前10", "expected_type": "A", "tags": ["口径消歧", "业主收入"]},
    {"question": "新桥服务区哪个项目最赚钱", "expected_type": "A", "tags": ["口径消歧", "项目≠服务区"]},
    {"question": "太湖服务区自营营收占比", "expected_type": "A", "tags": ["口径消歧", "自营⊂对客"]},
    {"question": "哪个服务区最赚钱", "expected_type": "A", "tags": ["口径消歧", "利润口径"]},

    # === 坪效/客单价（2 题）===
    {"question": "洪林服务区坪效数据", "expected_type": "A", "tags": ["坪效", "效率"]},
    {"question": "太湖服务区客单价多少", "expected_type": "A", "tags": ["客单价", "消费"]},

    # === 实时播报 C 类（4 题）===
    {"question": "新桥服务区现在情况怎么样", "expected_type": None, "tags": ["实时", "综合播报"]},
    {"question": "今天天气如何", "expected_type": "C", "tags": ["实时", "天气"]},
    {"question": "现在哪个服务区最忙", "expected_type": "C", "tags": ["实时", "忙碌排行"]},
    {"question": "滁州服务区充电桩还有空位吗", "expected_type": "C", "tags": ["实时", "充电桩"]},

    # === 复杂组合查询（2 题）===
    {"question": "皖北片区2月营收最低的3个服务区", "expected_type": "A", "tags": ["复杂", "片区+排名"]},
    {"question": "方兴大道服务区2月营收同比和环比变化", "expected_type": None, "tags": ["复杂", "多指标"]},

    # === 知识咨询 B 类（3 题）===
    {"question": "服务区招商流程是什么", "expected_type": "B", "tags": ["知识", "B类"]},
    {"question": "什么是对客销售", "expected_type": "B", "tags": ["知识", "术语"]},
    {"question": "服务区分几个等级", "expected_type": None, "tags": ["知识", "分类"]},

    # === 组织架构（1 题）===
    {"question": "皖东管理中心下面有哪些服务区", "expected_type": "A", "tags": ["架构", "组织"]},

    # === 边界收口（4 题）===
    {"question": "明年服务区营收预测", "expected_type": "A", "tags": ["边界", "未来"]},
    {"question": "2020年新桥营收多少", "expected_type": "A", "tags": ["边界", "远古"]},
    {"question": "全国服务区排名", "expected_type": None, "tags": ["边界", "超范围"]},
    {"question": "你好", "expected_type": "C", "tags": ["边界", "闲聊"]},

    # === 口径一致性对照（3 题一组）===
    {"question": "巢湖服务区2月营收", "expected_type": "A", "tags": ["口径对照", "组1-营收"]},
    {"question": "巢湖服务区2月对客销售", "expected_type": "A", "tags": ["口径对照", "组1-对客"]},
    {"question": "巢湖服务区2月营业收入", "expected_type": "A", "tags": ["口径对照", "组1-营业收入"]},
]


# ============================================================
# 多轮语义切换测试场景
# ============================================================
MULTI_TURN_SCENARIOS = [
    {
        "name": "基础-范围切换：SA→全省→片区→SA",
        "description": "空间维度来回切换",
        "turns": [
            {"question": "新桥服务区2月营收", "expect_contains": ["新桥"], "expect_type": "A"},
            {"question": "全省排名呢", "expect_contains": ["排名"], "expect_type": "A"},
            {"question": "皖中片区呢", "expect_contains": ["皖中"], "expect_type": "A"},
            {"question": "龙门寺呢", "expect_contains": ["龙门寺"], "expect_type": "A"},
        ],
    },
    {
        "name": "基础-时间切换：月→日→趋势→节假日",
        "description": "时间维度来回切换",
        "turns": [
            {"question": "巢湖服务区2月营收", "expect_contains": ["巢湖"], "expect_type": "A"},
            {"question": "昨天呢", "expect_contains": [], "expect_type": None},
            {"question": "最近3个月趋势", "expect_contains": [], "expect_type": "A"},
            {"question": "春节那段怎么样", "expect_contains": [], "expect_type": None},
        ],
    },
    {
        "name": "基础-类型切换：A→C→B→A",
        "description": "数据/实时/知识之间切换",
        "turns": [
            {"question": "洪林服务区2月营收", "expect_contains": ["洪林"], "expect_type": "A"},
            {"question": "现在天气怎么样", "expect_contains": [], "expect_type": "C"},
            {"question": "什么是对客销售", "expect_contains": ["对客销售"], "expect_type": None},
            {"question": "洪林的车流呢", "expect_contains": ["洪林"], "expect_type": "A"},
        ],
    },
    {
        "name": "基础-口径切换：营收→利润→业主→车流",
        "description": "同一 SA 不同指标切换",
        "turns": [
            {"question": "太湖服务区2月营收", "expect_contains": ["太湖"], "expect_type": "A"},
            {"question": "商户利润呢", "expect_contains": [], "expect_type": "A"},
            {"question": "业主营业收入呢", "expect_contains": [], "expect_type": "A"},
            {"question": "车流情况呢", "expect_contains": [], "expect_type": None},
        ],
    },
    {
        "name": "领导视察：全省概况→重点SA→问题深挖→对策→换SA",
        "description": "模拟领导逐层深入了解经营情况，8轮对话",
        "turns": [
            {"question": "全省2月营收整体情况", "expect_contains": [], "expect_type": "A"},
            {"question": "排名前5的服务区是哪些", "expect_contains": [], "expect_type": "A"},
            {"question": "最差的3个呢", "expect_contains": [], "expect_type": "A"},
            {"question": "排最后那个怎么回事", "expect_contains": [], "expect_type": "A"},
            {"question": "它的车流量正常吗", "expect_contains": [], "expect_type": None},
            {"question": "那门店情况呢", "expect_contains": [], "expect_type": "A"},
            {"question": "有没有亏损的商户", "expect_contains": [], "expect_type": "A"},
            {"question": "新桥的情况呢 跟它对比一下", "expect_contains": ["新桥"], "expect_type": "A"},
        ],
    },
    {
        "name": "商务分析：营收→拆原因→看合同→问建议",
        "description": "模拟商务人员做经营分析，从数据到原因到建议",
        "turns": [
            {"question": "方兴大道服务区2月营收同比下降了吗", "expect_contains": ["方兴大道"], "expect_type": "A"},
            {"question": "车流量有下降吗", "expect_contains": [], "expect_type": None},
            {"question": "哪些商户表现差", "expect_contains": [], "expect_type": "A"},
            {"question": "合同快到期的有几个", "expect_contains": [], "expect_type": "A"},
            {"question": "提成比例是多少", "expect_contains": [], "expect_type": None},
            {"question": "同路段其他服务区怎么样", "expect_contains": [], "expect_type": "A"},
        ],
    },
    {
        "name": "跨片区巡检：皖中→皖南→皖北→对比",
        "description": "模拟分管多片区的管理者快速巡查",
        "turns": [
            {"question": "皖中片区2月整体营收", "expect_contains": ["皖中"], "expect_type": "A"},
            {"question": "皖南呢", "expect_contains": ["皖南"], "expect_type": "A"},
            {"question": "皖北呢", "expect_contains": ["皖北"], "expect_type": "A"},
            {"question": "三个片区哪个表现最好", "expect_contains": [], "expect_type": "A"},
            {"question": "皖南最好的那个服务区详细看看", "expect_contains": [], "expect_type": "A"},
            {"question": "它的坪效数据", "expect_contains": [], "expect_type": "A"},
        ],
    },
    {
        "name": "新人连环追问：模糊→澄清→换方向→再换",
        "description": "模拟新用户不太清楚怎么问，思维跳跃大",
        "turns": [
            {"question": "服务区怎么样", "expect_contains": [], "expect_type": None},
            {"question": "就是新桥", "expect_contains": ["新桥"], "expect_type": None},
            {"question": "1月的", "expect_contains": [], "expect_type": None},
            {"question": "对了品牌排行看一下", "expect_contains": [], "expect_type": "A"},
            {"question": "还有客单价", "expect_contains": [], "expect_type": None},
            {"question": "换个 巢湖服务区现在情况", "expect_contains": ["巢湖"], "expect_type": None},
        ],
    },
    {
        "name": "数据验证：同一问题换说法→对比数字",
        "description": "模拟用户反复确认数据是否一致",
        "turns": [
            {"question": "洪林服务区2月营收多少", "expect_contains": ["洪林"], "expect_type": "A"},
            {"question": "对客销售是多少", "expect_contains": [], "expect_type": None},
            {"question": "营业收入呢", "expect_contains": [], "expect_type": None},
            {"question": "这三个数字应该一样吧", "expect_contains": [], "expect_type": None},
        ],
    },
    {
        "name": "实时→历史→实时：C-A-C 快速跳转",
        "description": "模拟用户在实时和历史数据间快速切换",
        "turns": [
            {"question": "新桥现在情况怎么样", "expect_contains": ["新桥"], "expect_type": None},
            {"question": "跟上个月比呢", "expect_contains": [], "expect_type": None},
            {"question": "全省今天哪里最忙", "expect_contains": [], "expect_type": "C"},
            {"question": "那个最忙的服务区2月营收多少", "expect_contains": [], "expect_type": None},
        ],
    },
]


def send_question(question: str, user_id: str, conversation_id: str = None,
                  llm_mode: str = None, api_url: str = None) -> dict:
    """发送单个问题到 API，返回完整响应"""
    url = api_url or API_URL
    data = {"message": question, "user_id": user_id}
    if conversation_id:
        data["conversation_id"] = conversation_id
    # 请求级 LLM 模式（支持 A/B 对比测试）
    actual_llm_mode = llm_mode or LLM_MODE
    if actual_llm_mode:
        data["llm_mode"] = actual_llm_mode
    payload = json.dumps(data, ensure_ascii=False).encode("utf-8")

    req = Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    start = time.time()
    try:
        with urlopen(req, timeout=TIMEOUT) as resp:
            body = json.loads(resp.read().decode("utf-8"))
            elapsed = time.time() - start
            return {
                "success": True,
                "status_code": resp.status,
                "elapsed_seconds": round(elapsed, 2),
                "response": body,
            }
    except URLError as e:
        elapsed = time.time() - start
        return {
            "success": False,
            "error": str(e),
            "elapsed_seconds": round(elapsed, 2),
            "response": None,
        }
    except Exception as e:
        elapsed = time.time() - start
        return {
            "success": False,
            "error": str(e),
            "elapsed_seconds": round(elapsed, 2),
            "response": None,
        }


def evaluate_layer1(question_info: dict, result: dict) -> dict:
    """第一层硬规则评判（零误判）"""
    checks = []
    resp = result.get("response") or {}

    if not result["success"]:
        checks.append({"check": "api_success", "passed": False, "detail": result.get("error")})
        return {"passed": False, "checks": checks}
    checks.append({"check": "api_success", "passed": True})

    expected_type = question_info.get("expected_type")
    actual_type = (resp.get("classification") or {}).get("type")
    if expected_type and actual_type:
        ok = actual_type == expected_type
        checks.append({
            "check": "classify_type", "passed": ok,
            "expected": expected_type, "actual": actual_type,
        })

    if actual_type == "A":
        mode = resp.get("mode", "")
        valid_modes = ("react", "quick_metric", "quick_metric_enriched",
                       "quick_compare_enriched", "governed_refusal",
                       "safe_guard", "chat_data")
        if mode in valid_modes:
            checks.append({"check": "tool_called", "passed": True, "mode": mode})
        else:
            checks.append({"check": "tool_called", "passed": False, "mode": mode})

    report = resp.get("report", "")
    leak_keywords = ["get_revenue", "get_traffic", "server_id=", "tool_result",
                     "api_endpoint", "BASE_URL", "traceback", "Observation:"]
    leaked = [kw for kw in leak_keywords if kw in report]
    if leaked:
        checks.append({"check": "no_leak", "passed": False, "leaked": leaked})
    else:
        checks.append({"check": "no_leak", "passed": True})

    all_passed = all(c["passed"] for c in checks)
    return {"passed": all_passed, "checks": checks}


def evaluate_layer2(question_info: dict, result: dict) -> dict:
    """第二层数据合理性评判"""
    import re
    warnings = []
    resp = result.get("response") or {}
    report = resp.get("report", "")
    tags = question_info.get("tags", [])

    if not report:
        return {"warnings": warnings}

    report_len = len(report)
    if report_len < 20:
        warnings.append({"check": "report_too_short", "length": report_len})
    elif report_len > 3000:
        warnings.append({"check": "report_too_long", "length": report_len})

    if any(t in tags for t in ["营收", "口径", "口径对照"]):
        amounts = re.findall(r'([\d,]+\.?\d*)\s*万元', report)
        for amt_str in amounts:
            amt = float(amt_str.replace(',', ''))
            if amt > 2000:
                warnings.append({"check": "revenue_too_high", "value": amt, "raw": amt_str})

    if "车流" in tags:
        flows = re.findall(r'([\d,]+)\s*(?:辆|万辆)', report)
        for flow_str in flows:
            flow = float(flow_str.replace(',', ''))
            if '万辆' not in report and flow > 2000000:
                warnings.append({"check": "traffic_too_high", "value": flow})

    pcts = re.findall(r'[+-]?\s*([\d.]+)\s*%', report)
    extreme_pcts = [float(p) for p in pcts if float(p) > 500]
    if extreme_pcts:
        warnings.append({"check": "extreme_percentage", "values": extreme_pcts})

    return {"warnings": warnings}


def check_consistency(results: list) -> list:
    """口径一致性检查"""
    import re
    groups = {}
    for r in results:
        for tag in r.get("tags", []):
            if tag.startswith("组1-") or tag.startswith("组2-") or tag.startswith("组3-"):
                group = tag.split("-")[0]
                if group not in groups:
                    groups[group] = []
                groups[group].append(r)

    issues = []
    for group, items in groups.items():
        numbers = {}
        for item in items:
            preview = item.get("report_preview", "")
            match = re.search(r'([\d,]+\.?\d*)\s*万元', preview)
            if match:
                numbers[item["question"]] = float(match.group(1).replace(',', ''))

        if len(numbers) >= 2:
            vals = list(numbers.values())
            max_diff = max(vals) - min(vals)
            if max_diff > 0.1:
                issues.append({
                    "group": group,
                    "questions": list(numbers.keys()),
                    "values": numbers,
                    "max_diff": round(max_diff, 2),
                })

    return issues


def run_test(questions: list, run_id: str, llm_mode: str = None) -> dict:
    """执行一批测试并返回汇总结果"""
    results = []
    total = len(questions)
    mode_label = f" [{llm_mode}]" if llm_mode else ""

    for i, q_info in enumerate(questions, 1):
        question = q_info if isinstance(q_info, str) else q_info["question"]
        user_id = f"{USER_ID_PREFIX}_{run_id}_{i:03d}"

        print(f"  [{i}/{total}]{mode_label} {question[:30]}...", end=" ", flush=True)

        result = send_question(question, user_id, llm_mode=llm_mode)
        layer1 = evaluate_layer1(q_info if isinstance(q_info, dict) else {"question": question}, result)
        layer2 = evaluate_layer2(q_info if isinstance(q_info, dict) else {"question": question}, result)

        resp = result.get("response") or {}
        record = {
            "index": i,
            "question": question,
            "user_id": user_id,
            "tags": q_info.get("tags", []) if isinstance(q_info, dict) else [],
            "success": result["success"],
            "elapsed_seconds": result["elapsed_seconds"],
            "classify_type": (resp.get("classification") or {}).get("type"),
            "complexity": (resp.get("classification") or {}).get("complexity"),
            "mode": resp.get("mode"),
            "report_length": len(resp.get("report", "")),
            "report_preview": (resp.get("report") or "")[:200],
            "log_file": resp.get("log_file"),
            "timing": resp.get("timing"),
            "suggested_questions": resp.get("suggested_questions"),
            "layer1": layer1,
            "layer2": layer2,
            "error": result.get("error"),
        }
        results.append(record)

        status = "✅" if layer1["passed"] else "❌"
        warn = f" ⚠{len(layer2['warnings'])}" if layer2["warnings"] else ""
        t = result["elapsed_seconds"]
        print(f"{status}{warn} {t}s {record['classify_type'] or 'ERR'}")

        if i < total:
            time.sleep(2)

    consistency_issues = check_consistency(results)

    passed = sum(1 for r in results if r["layer1"]["passed"])
    warned = sum(1 for r in results if r["layer2"]["warnings"])
    failed = total - passed
    summary = {
        "run_id": run_id,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "total": total,
        "passed": passed,
        "failed": failed,
        "warned": warned,
        "pass_rate": f"{passed/total*100:.1f}%" if total > 0 else "N/A",
        "avg_elapsed": round(sum(r["elapsed_seconds"] for r in results) / total, 2) if total else 0,
        "consistency_issues": consistency_issues,
        "results": results,
    }
    return summary


def generate_report(summary: dict) -> str:
    """生成 Markdown 报告"""
    lines = [
        f"# QA 测试报告 — {summary['run_id']}",
        f"",
        f"> 时间: {summary['timestamp']} | 总计: {summary['total']} 题 | "
        f"通过: {summary['passed']} | 失败: {summary['failed']} | "
        f"警告: {summary.get('warned', 0)} | "
        f"通过率: {summary['pass_rate']} | 平均耗时: {summary['avg_elapsed']}s",
        f"",
        f"## 详细结果",
        f"",
        f"| # | 问题 | 分类 | 耗时 | L1 | L2 | 备注 |",
        f"|---|------|------|------|-----|-----|------|",
    ]

    for r in summary["results"]:
        status = "✅" if r["layer1"]["passed"] else "❌"
        q = r["question"][:20]
        ct = r["classify_type"] or "ERR"
        t = f"{r['elapsed_seconds']}s"
        w = f"⚠{len(r['layer2']['warnings'])}" if r["layer2"]["warnings"] else "✅"
        fails = [c for c in r["layer1"]["checks"] if not c["passed"]]
        note = ", ".join(c["check"] for c in fails) if fails else ""
        if r["layer2"]["warnings"]:
            warn_names = [w_item["check"] for w_item in r["layer2"]["warnings"]]
            note = (note + " " if note else "") + " ".join(warn_names)
        lines.append(f"| {r['index']} | {q} | {ct} | {t} | {status} | {w} | {note} |")

    ci = summary.get("consistency_issues", [])
    if ci:
        lines.append(f"\n## 口径一致性问题\n")
        for issue in ci:
            lines.append(f"### {issue['group']} — 差异 {issue['max_diff']} 万元")
            for q, v in issue["values"].items():
                lines.append(f"- {q}: **{v}万元**")
            lines.append("")
    else:
        has_caliber = any("口径对照" in t for r in summary["results"] for t in r.get("tags", []))
        if has_caliber:
            lines.append(f"\n## 口径一致性 ✅\n")
            lines.append("口径对照组数据一致，无偏差。\n")

    tag_stats = {}
    for r in summary["results"]:
        for tag in r.get("tags", []):
            if tag.startswith("组"):
                continue
            if tag not in tag_stats:
                tag_stats[tag] = {"total": 0, "passed": 0}
            tag_stats[tag]["total"] += 1
            if r["layer1"]["passed"]:
                tag_stats[tag]["passed"] += 1

    if tag_stats:
        lines.append(f"\n## 按标签通过率\n")
        lines.append(f"| 标签 | 通过/总计 | 通过率 |")
        lines.append(f"|------|----------|--------|")
        for tag, stat in sorted(tag_stats.items(), key=lambda x: x[1]["passed"]/max(x[1]["total"],1)):
            rate = f"{stat['passed']/stat['total']*100:.0f}%" if stat["total"] > 0 else "N/A"
            lines.append(f"| {tag} | {stat['passed']}/{stat['total']} | {rate} |")

    failed = [r for r in summary["results"] if not r["layer1"]["passed"]]
    if failed:
        lines.append(f"\n## 失败详情\n")
        for r in failed:
            lines.append(f"### #{r['index']} {r['question']}")
            for c in r["layer1"]["checks"]:
                if not c["passed"]:
                    lines.append(f"- **{c['check']}**: {json.dumps(c, ensure_ascii=False)}")
            lines.append("")

    return "\n".join(lines)


# ============================================================
# 多轮语义切换测试
# ============================================================

def run_multi_turn(scenarios: list, run_id: str, llm_mode: str = None) -> dict:
    """执行多轮语义切换测试"""
    import re
    results = []
    total_pass = 0
    total_turns = 0
    mode_label = f" [{llm_mode}]" if llm_mode else ""

    for si, scenario in enumerate(scenarios, 1):
        name = scenario["name"]
        turns = scenario["turns"]
        user_id = f"qa_multi_{run_id}_{si}"
        conversation_id = None

        print(f"\n  [{si}/{len(scenarios)}]{mode_label} {name}")
        turn_results = []

        for ti, turn in enumerate(turns, 1):
            q = turn["question"]
            total_turns += 1
            print(f"    轮{ti}: {q}... ", end="", flush=True)

            result = send_question(q, user_id, conversation_id, llm_mode=llm_mode)
            resp = result.get("response") or {}
            elapsed = result.get("elapsed_seconds", 0)

            if ti == 1 and resp.get("conversation_id"):
                conversation_id = resp["conversation_id"]

            passed = True
            issues = []

            if not result["success"]:
                passed = False
                issues.append("API 失败")

            actual_type = ""
            classification = resp.get("classification") or {}
            if isinstance(classification, dict):
                actual_type = classification.get("type", "")
            elif isinstance(classification, str):
                actual_type = classification

            exp_type = turn.get("expect_type")
            if exp_type and actual_type and actual_type != exp_type:
                passed = False
                issues.append(f"分类: 期望{exp_type} 实际{actual_type}")

            report_text = resp.get("report", "") or ""
            for kw in turn.get("expect_contains", []):
                if kw not in report_text:
                    passed = False
                    issues.append(f"缺少关键词: {kw}")

            if result["success"] and len(report_text) < 10:
                issues.append("回答过短(<10字)")

            if passed:
                total_pass += 1
                print(f"✅ {elapsed}s {actual_type}")
            else:
                print(f"❌ {elapsed}s {actual_type} | {'; '.join(issues)}")

            turn_results.append({
                "turn": ti,
                "question": q,
                "elapsed": elapsed,
                "actual_type": actual_type,
                "expected_type": exp_type,
                "passed": passed,
                "issues": issues,
                "report_preview": report_text[:200] if report_text else "",
                "full_response": report_text,
                "conversation_id": conversation_id,
                "diagnostics": {},
            })

        scenario_pass = sum(1 for t in turn_results if t["passed"])
        results.append({
            "name": name,
            "description": scenario["description"],
            "turns": turn_results,
            "passed": scenario_pass,
            "total": len(turns),
            "pass_rate": f"{scenario_pass/len(turns)*100:.0f}%",
        })

    return {
        "run_id": run_id,
        "type": "multi_turn",
        "scenarios": results,
        "total_turns": total_turns,
        "total_pass": total_pass,
        "pass_rate": f"{total_pass/total_turns*100:.1f}%" if total_turns else "0%",
    }


def generate_multi_report(summary: dict) -> str:
    """生成多轮测试报告"""
    lines = [
        f"# 多轮语义切换报告 — {summary['run_id']}\n",
        f"> 总轮次: {summary['total_turns']} | "
        f"通过: {summary['total_pass']} | "
        f"通过率: {summary['pass_rate']}\n",
    ]

    for scenario in summary["scenarios"]:
        status = "✅" if scenario["passed"] == scenario["total"] else "⚠️"
        lines.append(f"## {status} {scenario['name']} ({scenario['pass_rate']})\n")
        lines.append(f"> {scenario['description']}\n")
        lines.append("| 轮 | 问题 | 分类 | 耗时 | 结果 | 问题 |")
        lines.append("|-----|------|------|------|------|------|")

        for t in scenario["turns"]:
            status_icon = "✅" if t["passed"] else "❌"
            issue_str = "; ".join(t["issues"]) if t["issues"] else ""
            lines.append(
                f"| {t['turn']} | {t['question']} | {t['actual_type']} | "
                f"{t['elapsed']}s | {status_icon} | {issue_str} |"
            )
        lines.append("")
        conv_ids = [t["conversation_id"] for t in scenario["turns"]]
        if len(set(conv_ids)) == 1 and conv_ids[0]:
            lines.append(f"📎 会话ID一致: `{conv_ids[0][:8]}...` ✅\n")
        elif None in conv_ids:
            lines.append("⚠️ 部分轮次没有获取到 conversation_id\n")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="QA 测试运行器")
    parser.add_argument("--questions", nargs="+", help="指定测试问题列表")
    parser.add_argument("--questions-file", help="外部题目 JSON 文件（qa_question_gen 生成）")
    parser.add_argument("--output", help="JSON 结果输出路径")
    parser.add_argument("--report", help="Markdown 报告输出路径")
    parser.add_argument("--api-url", default=API_URL, help="API 地址")
    parser.add_argument("--run-id", help="批次 ID（默认自动生成）")
    parser.add_argument("--limit", type=int, help="限制测试题数/场景数")
    parser.add_argument("--multi-turn", action="store_true",
                        help="执行多轮语义切换测试（替代单轮 Golden Set）")
    parser.add_argument("--llm-mode", choices=["qwen", "codex", "hybrid"],
                        help="请求级 LLM 模式（A/B 对比时使用，不传则用服务器默认）")
    args = parser.parse_args()

    # 覆盖全局配置
    global API_URL, LLM_MODE
    if args.api_url != API_URL:
        API_URL = args.api_url
    if args.llm_mode:
        LLM_MODE = args.llm_mode

    run_id = args.run_id or datetime.now().strftime("qa_%Y%m%d_%H%M")

    # 加载外部题目文件
    ext_questions = None
    ext_multi_scenarios = None
    if args.questions_file:
        with open(args.questions_file, 'r', encoding='utf-8') as f:
            qdata = json.load(f)
        ext_questions = qdata.get('single_questions', [])
        ext_multi_scenarios = qdata.get('multi_turn_scenarios', [])
        print(f"   外部题目: {len(ext_questions)} 单轮 + {len(ext_multi_scenarios)} 多轮场景")

    # 多轮模式
    if args.multi_turn:
        scenarios = ext_multi_scenarios if ext_multi_scenarios else MULTI_TURN_SCENARIOS
        if args.limit:
            scenarios = scenarios[:args.limit]
        print(f"\n🔄 多轮语义切换测试开始")
        print(f"   批次: {run_id}")
        print(f"   场景: {len(scenarios)}{'（外部题目）' if ext_multi_scenarios else ''}{f'（限制{args.limit}场景）' if args.limit else ''}")
        print(f"   API:  {API_URL}")
        print(f"{'='*50}")

        summary = run_multi_turn(scenarios, run_id, llm_mode=args.llm_mode)

        print(f"\n{'='*50}")
        print(f"📊 结果: {summary['total_pass']}/{summary['total_turns']} 通过 ({summary['pass_rate']})")

        output_path = args.output or str(REPORTS_DIR / f"qa_multi_{run_id}.json")
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        print(f"💾 JSON: {output_path}")

        report_path = args.report or str(REPORTS_DIR / f"qa_multi_report_{run_id}.md")
        report = generate_multi_report(summary)
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(report)
        print(f"📝 报告: {report_path}")

        if ext_questions:
            print(f"\n🔬 继续跑 {len(ext_questions)} 道单轮题...")
            single_summary = run_test(ext_questions, run_id + "_single", llm_mode=args.llm_mode)
            single_output = output_path.replace('.json', '_single.json')
            with open(single_output, "w", encoding="utf-8") as f:
                json.dump(single_summary, f, ensure_ascii=False, indent=2)
            print(f"💾 单轮 JSON: {single_output}")

        return

    # 单轮模式
    if args.questions:
        questions = [{"question": q, "expected_type": None, "tags": []} for q in args.questions]
    elif ext_questions:
        questions = ext_questions
    else:
        questions = GOLDEN_SET

    if args.limit:
        questions = questions[:args.limit]

    print(f"\n🔬 QA 测试开始")
    print(f"   批次: {run_id}")
    print(f"   题数: {len(questions)}")
    print(f"   API:  {API_URL}")
    print(f"{'='*50}\n")

    summary = run_test(questions, run_id, llm_mode=args.llm_mode)

    print(f"\n{'='*50}")
    print(f"📊 结果: {summary['passed']}/{summary['total']} 通过 ({summary['pass_rate']})")
    print(f"⏱  平均耗时: {summary['avg_elapsed']}s\n")

    output_path = args.output or str(REPORTS_DIR / f"qa_results_{run_id}.json")
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    print(f"💾 JSON: {output_path}")

    report_path = args.report or str(REPORTS_DIR / f"qa_report_{run_id}.md")
    report = generate_report(summary)
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report)
    print(f"📝 报告: {report_path}")


if __name__ == "__main__":
    main()
