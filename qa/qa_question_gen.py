"""
QA 智能出题器 — Step 0: 读 DB/工具/历史 → 生成带目的标注的题目

独立项目版本 — 所有配置从 config.py 读取

用法:
    python qa/qa_question_gen.py --mode rules
    python qa/qa_question_gen.py --mode codex
    python qa/qa_question_gen.py --mode context-only
"""

import argparse
import json
import os
import random
import sqlite3
import subprocess
import sys
import io
import time
from datetime import datetime, timedelta
from pathlib import Path

if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

# 从 config 读取配置
sys.path.insert(0, str(Path(__file__).parent))
from config import DB_PATH, HISTORY_PATH, REPORTS_DIR, CODEX_CWD, CODEX_DATA_DIR, QA_DIR

# 工具描述（系统已有的查询能力）
SYSTEM_TOOLS = [
    {"name": "get_revenue", "desc": "服务区营收/对客销售查询", "params": ["服务区名", "月份"]},
    {"name": "get_daily_revenue", "desc": "日营收明细", "params": ["服务区名", "日期"]},
    {"name": "get_traffic", "desc": "车流/入区流量", "params": ["服务区名"]},
    {"name": "get_daily_traffic", "desc": "日车流明细", "params": ["服务区名", "日期"]},
    {"name": "query_revenue_summary", "desc": "全省/片区营收汇总排名", "params": ["片区", "月份"]},
    {"name": "get_dashboard_revenue", "desc": "大屏营收数据", "params": ["片区"]},
    {"name": "get_merchant_business", "desc": "商户经营分析", "params": ["服务区名"]},
    {"name": "get_brand_ranking", "desc": "品牌排名", "params": ["品牌名/服务区名"]},
    {"name": "get_merchant_profit", "desc": "商户利润/盈亏", "params": ["服务区名"]},
    {"name": "get_contract", "desc": "合同到期/提成比例", "params": ["服务区名"]},
    {"name": "get_asset_efficiency", "desc": "坪效/资产效率", "params": ["服务区名"]},
    {"name": "get_per_car_value", "desc": "客单价/单车消费", "params": ["服务区名"]},
    {"name": "get_org_structure", "desc": "组织架构/服务区列表", "params": ["片区"]},
    {"name": "knowledge_search", "desc": "知识库检索（B类）", "params": ["关键词"]},
    {"name": "service_area_status", "desc": "实时状态（C类）", "params": ["服务区名"]},
]


# ============================================================
# 1. 读 DB 上下文
# ============================================================

def get_db_context(db_path: str) -> dict:
    """从 DB 获取数据范围信息"""
    ctx = {}
    try:
        conn = sqlite3.connect(db_path)

        sas = [r[0] for r in conn.execute(
            "SELECT DISTINCT [服务区名称] FROM NEWGETREVENUEREPORT_SHOPS ORDER BY [服务区名称]"
        ).fetchall()]
        ctx['service_areas'] = sas
        ctx['sa_count'] = len(sas)

        months = [r[0] for r in conn.execute(
            "SELECT DISTINCT [STATISTICS_MONTH] FROM NEWGETREVENUEREPORT_SHOPS ORDER BY [STATISTICS_MONTH]"
        ).fetchall()]
        ctx['months'] = months

        regions = [r[0] for r in conn.execute(
            "SELECT DISTINCT [归属区域名字] FROM NEWGETSERVERPARTLIST WHERE [归属区域名字] IS NOT NULL"
        ).fetchall()]
        ctx['regions'] = regions

        sa_types = conn.execute(
            "SELECT [服务区类型(1000:A类,2000:B类,3000:C类,4000:D类)], COUNT(*) "
            "FROM NEWGETSERVERPARTLIST GROUP BY 1"
        ).fetchall()
        ctx['sa_types'] = {r[0]: r[1] for r in sa_types}

        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE '_%'"
        ).fetchall()]
        ctx['tables'] = tables

        conn.close()
    except Exception as e:
        ctx['_error'] = str(e)

    return ctx


# ============================================================
# 2. 读历史日志
# ============================================================

def get_history(history_path: str, days: int = 7) -> list:
    """读取最近 N 天的历史测试记录"""
    history = []
    if not Path(history_path).exists():
        return history

    with open(history_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
                history.append(record)
            except json.JSONDecodeError:
                continue

    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    history = [r for r in history if r.get('date', '') >= cutoff]
    return history


# ============================================================
# 3. 规则引擎出题
# ============================================================

def generate_by_rules(db_ctx: dict, history: list) -> list:
    """基于规则引擎生成测试题目（增强版：5种多轮模板，5-6轮）"""
    questions = []
    sas = db_ctx.get('service_areas', [])
    months = db_ctx.get('months', [])
    regions = db_ctx.get('regions', [])
    latest_month = months[-1] if months else '202602'
    month_label = f"{int(latest_month)%100}月"

    # 统计历史覆盖
    covered_sas = set()
    known_issues = []
    for record in history:
        covered_sas.update(record.get('sa_covered', []))
        known_issues.extend(record.get('issues', []))

    uncovered = [sa for sa in sas if sa not in covered_sas]

    # --- 1. 覆盖题 (40%) ---
    coverage_sas = random.sample(uncovered, min(8, len(uncovered))) if uncovered else random.sample(sas, 8)
    for sa in coverage_sas[:4]:
        questions.append({
            "question": f"{sa}{month_label}营收",
            "purpose": f"覆盖: {sa} 从未测试过",
            "category": "coverage",
            "expect_type": "A",
        })
    for sa in coverage_sas[4:8]:
        questions.append({
            "question": f"{sa}车流量情况",
            "purpose": f"覆盖: {sa} 车流维度",
            "category": "coverage",
            "expect_type": "A",
        })

    # --- 2. 回归题 (30%) ---
    regression_templates = [
        ("{sa}{month}营收情况", "回归: 验证营收数字准确性"),
        ("{sa}商户有没有亏损的", "回归: 验证商户利润查询"),
        ("{sa}门店经营情况", "回归: 验证商户数据"),
    ]
    regression_sas = random.sample(sas, min(6, len(sas)))
    for i, sa in enumerate(regression_sas):
        tpl, purpose = regression_templates[i % len(regression_templates)]
        questions.append({
            "question": tpl.format(sa=sa, month=month_label),
            "purpose": purpose,
            "category": "regression",
            "expect_type": "A",
        })

    # --- 3. 多轮深挖题 (20%) — 5种场景模板，每组5-6轮 ---
    # 模板设计参考 Codex v3 巡查报告发现的高风险场景
    MULTI_TURN_TEMPLATES = [
        {
            "name_tpl": "领导巡检: {sa}全面复盘",
            "desc": "模拟领导逐层深入：营收→车流→商户→排名→环比→亏损",
            "turns": [
                ("{sa}{month}营收", "A", "多轮: 起始营收查询"),
                ("车流量呢", None, "多轮: 同SA换到车流维度"),
                ("商户利润呢", "A", "多轮: 换到利润维度"),
                ("全省排名呢", "A", "多轮: 扩大范围到全省"),
                ("跟上个月比怎么样", None, "多轮: 时间维度环比"),
                ("有没有亏损的商户", "A", "多轮: 深挖商户亏损"),
            ],
        },
        {
            "name_tpl": "口径验证: {sa}三问对照",
            "desc": "同一SA用不同口径追问，检测数字一致性",
            "turns": [
                ("{sa}{month}营收", "A", "口径: 营收基准"),
                ("对客销售呢", None, "口径: 对客销售=营收?"),
                ("营业收入呢", None, "口径: 营业收入=营收?"),
                ("业主营业收入呢", None, "口径: 业主营收≠对客(高风险)"),
                ("这些数字有什么区别", None, "口径: 概念澄清"),
            ],
        },
        {
            "name_tpl": "实时切换: {sa}今昔对比",
            "desc": "C→A→C快速跳转，测试实时/历史混答风险",
            "turns": [
                ("{sa}现在情况怎么样", None, "实时: C类起点"),
                ("上个月营收呢", "A", "实时→历史: 切到A类月度"),
                ("今天车流怎么样", None, "历史→实时: 切回C类"),
                ("换个 {sa2}现在情况", None, "实时: 换SA(高风险-上下文污染)"),
                ("它{month}营收多少", "A", "承接: 用代词+明确月份"),
            ],
        },
        {
            "name_tpl": "深挖经营: {sa}多维诊断",
            "desc": "模拟商务人员做经营分析：营收→原因→合同→坪效→建议",
            "turns": [
                ("{sa}{month}营收同比变化", "A", "深挖: 同比趋势起点"),
                ("门店经营情况", "A", "深挖: 商户维度"),
                ("合同快到期的有几个", "A", "深挖: 合同维度"),
                ("坪效数据", "A", "深挖: 坪效(高风险-面积底数)"),
                ("客单价多少", None, "深挖: 客单价(高风险-串值)"),
            ],
        },
        {
            "name_tpl": "跨片区: {region}片区对比",
            "desc": "片区级巡检→具体SA→多维对比",
            "turns": [
                ("{region}片区{month}整体营收", "A", "片区: 汇总起点"),
                ("排名前3的服务区", "A", "片区: TOP N"),
                ("最差的呢", "A", "片区: 末位追问"),
                ("排最后那个怎么回事", "A", "片区: 深挖(实体混淆风险)"),
                ("它的车流量正常吗", None, "片区: 代词承接+维度切换"),
            ],
        },
    ]

    multi_turn_sas = random.sample(sas, min(4, len(sas)))
    region_list = regions if regions else ['皖中', '皖南', '皖北']
    for i in range(min(3, len(MULTI_TURN_TEMPLATES))):
        tpl = MULTI_TURN_TEMPLATES[i]
        sa = multi_turn_sas[i % len(multi_turn_sas)]
        sa2 = multi_turn_sas[(i + 1) % len(multi_turn_sas)]
        region = random.choice(region_list)

        first_q = tpl["turns"][0][0].format(sa=sa, sa2=sa2, month=month_label, region=region)
        follow_ups = []
        for turn_q, exp_type, purpose in tpl["turns"][1:]:
            follow_ups.append({
                "question": turn_q.format(sa=sa, sa2=sa2, month=month_label, region=region),
                "purpose": purpose,
                "expect_type": exp_type,
            })

        questions.append({
            "question": first_q,
            "purpose": tpl["desc"],
            "category": "multi_turn_start",
            "expect_type": tpl["turns"][0][1],
            "follow_ups": follow_ups,
            "_scenario_name": tpl["name_tpl"].format(sa=sa, sa2=sa2, region=region),
            "_scenario_desc": tpl["desc"],
        })

    # --- 4. 边界/探索题 (10%) ---
    boundary_questions = [
        {"question": "哪个片区最近表现最差", "purpose": "探索: 模糊问法+片区级对比", "category": "boundary"},
        {"question": f"给我一份{random.choice(sas)}的完整经营报告", "purpose": "探索: 综合报告能力", "category": "boundary"},
        {"question": "对客销售和营业收入有什么区别", "purpose": "探索: B类知识题", "category": "boundary", "expect_type": "B"},
    ]
    questions.extend(boundary_questions)

    return questions


# ============================================================
# 4. Codex 智能出题
# ============================================================

CODEX_PROMPT_TEMPLATE = """你是 QA 出题专家。请根据以下上下文，为高速公路服务区智能助手生成测试题目。

## 系统能力

{tools_desc}

## 数据库范围

- 服务区总数: {sa_count}
- 可用月份: {months}
- 片区: {regions}
- 服务区示例: {sa_sample}

## 历史测试记录（最近 7 天）

{history_summary}

## 出题规则

1. **覆盖题 (40%)**: 从未测试的服务区中挑选，确保两周内覆盖所有 {sa_count} 个
2. **回归题 (30%)**: 对上次发现问题的场景换个问法再测
3. **深挖题 (20%)**: 设计 2 组多轮追问链（4-6 轮），模拟真实使用场景
4. **探索题 (10%)**: 边界/歧义/模糊表述，测试系统的兜底能力

## 每题必须标注

- question: 问题
- purpose: 这道题为什么出（检验什么能力/修复什么 bug）
- category: coverage/regression/multi_turn/boundary
- expect_type: A/B/C（可选）

## 输出格式

将题目输出为 JSON 数组写入 {output_path}
"""


def generate_by_codex(db_ctx: dict, history: list, output_path: str, cwd: str) -> list:
    """调用 Codex 智能出题"""
    tools_desc = "\n".join(
        f"- {t['name']}: {t['desc']} (参数: {', '.join(t['params'])})"
        for t in SYSTEM_TOOLS
    )

    if history:
        history_lines = []
        for r in history[-5:]:
            history_lines.append(
                f"- {r['date']}: {r.get('total', '?')}题, "
                f"通过率 {r.get('pass_rate', '?')}, "
                f"严重偏差 {r.get('critical', 0)}, "
                f"覆盖 {len(r.get('sa_covered', []))} 个SA"
            )
            if r.get('issues'):
                for issue in r['issues'][:3]:
                    history_lines.append(f"  - 问题: {issue}")
        history_summary = "\n".join(history_lines)
    else:
        history_summary = "（无历史记录，这是第一次测试）"

    all_sas = db_ctx.get('service_areas', [])
    sa_sample = ", ".join(random.sample(all_sas, min(20, len(all_sas))))

    prompt = CODEX_PROMPT_TEMPLATE.format(
        tools_desc=tools_desc,
        sa_count=db_ctx.get('sa_count', '?'),
        months=", ".join(db_ctx.get('months', [])),
        regions=", ".join(db_ctx.get('regions', [])),
        sa_sample=sa_sample,
        history_summary=history_summary,
        output_path=output_path,
    )

    cmd = [
        "codex", "exec", "--full-auto",
        "--add-dir", CODEX_DATA_DIR,
        prompt,
    ]
    print(f"  启动 Codex 出题...")
    result = subprocess.run(
        cmd, cwd=cwd,
        capture_output=True, text=True, encoding='utf-8', errors='replace'
    )

    if result.returncode != 0:
        print(f"  Codex 出题失败: {result.stderr[:500]}")
        return []

    if Path(output_path).exists():
        with open(output_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return []


# ============================================================
# 5. 写历史日志
# ============================================================

def append_history(history_path: str, run_id: str, questions: list, results: dict = None):
    """追加一条历史记录到 qa_history.jsonl"""
    import re
    record = {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "time": datetime.now().strftime("%H:%M:%S"),
        "run_id": run_id,
        "total": len(questions),
        "categories": {},
        "sa_covered": [],
        "issues": [],
    }

    for q in questions:
        cat = q.get('category', 'unknown')
        record['categories'][cat] = record['categories'].get(cat, 0) + 1

    for q in questions:
        sa_match = re.search(r'([\u4e00-\u9fa5]{2,8}服务区)', q.get('question', ''))
        if sa_match:
            record['sa_covered'].append(sa_match.group(1))
    record['sa_covered'] = list(set(record['sa_covered']))

    if results:
        record['pass_rate'] = results.get('pass_rate', '?')
        record['critical'] = results.get('critical', 0)
        record['issues'] = results.get('issues', [])

    with open(history_path, 'a', encoding='utf-8') as f:
        f.write(json.dumps(record, ensure_ascii=False) + '\n')

    return record


# ============================================================
# 6. 转换为 qa_runner 格式
# ============================================================

def to_runner_format(questions: list) -> dict:
    """将出题器输出转换为 qa_runner 可用的格式（对齐 MULTI_TURN_SCENARIOS）"""
    single_turns = []
    multi_turns = []

    for q in questions:
        if q.get('follow_ups'):
            first_turn = {
                "question": q['question'],
                "expect_contains": [],
                "expect_type": q.get('expect_type'),
            }
            turns = [first_turn]
            for fu in q['follow_ups']:
                turns.append({
                    "question": fu['question'],
                    "expect_contains": [],
                    "expect_type": fu.get('expect_type'),
                })
            multi_turns.append({
                "name": q.get('_scenario_name', f"自动生成: {q['purpose'][:30]}"),
                "description": q.get('_scenario_desc', q['purpose']),
                "turns": turns,
            })
        else:
            single_turns.append({
                "question": q['question'],
                "expected_type": q.get('expect_type'),
                "tags": [q.get('category', 'unknown')],
                "purpose": q.get('purpose', ''),
            })

    return {
        "single_questions": single_turns,
        "multi_turn_scenarios": multi_turns,
        "metadata": {
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "total_single": len(single_turns),
            "total_multi_scenarios": len(multi_turns),
        }
    }


def main():
    parser = argparse.ArgumentParser(description="QA 智能出题器")
    parser.add_argument("--mode", choices=["rules", "codex", "context-only"],
                        default="rules", help="出题模式")
    parser.add_argument("--db", default=DB_PATH, help="数据库路径")
    parser.add_argument("--output", help="输出路径")
    parser.add_argument("--run-id", help="批次 ID")
    parser.add_argument("--cwd", default=CODEX_CWD, help="Codex 工作目录")
    args = parser.parse_args()

    run_id = args.run_id or datetime.now().strftime("gen_%Y%m%d_%H%M")

    print("读取 DB 上下文...")
    db_ctx = get_db_context(args.db)
    print(f"  服务区: {db_ctx.get('sa_count', 0)} 个")
    print(f"  月份: {db_ctx.get('months', [])}")
    print(f"  片区: {db_ctx.get('regions', [])}")

    print("读取历史日志...")
    history = get_history(HISTORY_PATH)
    print(f"  最近记录: {len(history)} 条")

    if args.mode == "context-only":
        print("\n[context-only] 上下文已读取，不出题")
        return

    if args.mode == "rules":
        print("\n使用规则引擎出题...")
        questions = generate_by_rules(db_ctx, history)
    else:
        output_path = args.output or str(REPORTS_DIR / f"questions_{run_id}.json")
        print("\n使用 Codex 智能出题...")
        questions = generate_by_codex(db_ctx, history, output_path, args.cwd)

    print(f"\n生成 {len(questions)} 道题目:")
    categories = {}
    for q in questions:
        cat = q.get('category', 'unknown')
        categories[cat] = categories.get(cat, 0) + 1
        print(f"  [{cat}] {q['question']}")
        print(f"         目的: {q.get('purpose', '无')}")

    print(f"\n分类统计: {categories}")

    runner_data = to_runner_format(questions)

    output_path = args.output or str(REPORTS_DIR / f"questions_{run_id}.json")
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(runner_data, f, ensure_ascii=False, indent=2)
    print(f"\n题目文件: {output_path}")

    record = append_history(HISTORY_PATH, run_id, questions)
    print(f"历史日志: {HISTORY_PATH}")
    print(f"  覆盖 SA: {len(record['sa_covered'])} 个")


if __name__ == "__main__":
    main()
