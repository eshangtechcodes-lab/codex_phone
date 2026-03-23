# -*- coding: utf-8 -*-
"""
QA 巡查 Diff 对比脚本 — 比较两次巡查结果，跟踪修复进度

用法：
    # 指定两个 JSON 文件
    python qa/qa_diff.py --old qa/reports/golden_qa_20260322.json --new qa/reports/golden_qa_20260323.json

    # 自动找最新两份（按文件名排序）
    python qa/qa_diff.py --latest

    # 指定类型（golden / multi）
    python qa/qa_diff.py --latest --type multi

    # 输出对比报告
    python qa/qa_diff.py --latest --output qa/reports/diff_report.md
"""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

# Windows 控制台 UTF-8
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# 报告目录
REPORTS_DIR = Path("qa/reports")


def load_json(path: str) -> dict:
    """加载 JSON 结果文件"""
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def extract_question_status(data: dict) -> dict:
    """
    从 JSON 结果中提取每个问题的通过/失败状态
    返回: {question_text: {"passed": bool, "type": str, "detail": str, ...}}
    """
    status = {}

    # 单轮结果（Golden Set）
    if "results" in data:
        for r in data["results"]:
            q = r["question"]
            passed = r.get("layer1", {}).get("passed", False)
            warnings = r.get("layer2", {}).get("warnings", [])
            fails = [c for c in r.get("layer1", {}).get("checks", []) if not c["passed"]]
            status[q] = {
                "passed": passed,
                "type": r.get("classify_type", ""),
                "mode": r.get("mode", ""),
                "elapsed": r.get("elapsed_seconds", 0),
                "fail_checks": [c["check"] for c in fails],
                "warnings": [w["check"] for w in warnings],
                "tags": r.get("tags", []),
            }

    # 多轮结果
    if "scenarios" in data:
        for scenario in data["scenarios"]:
            scenario_name = scenario["name"]
            for turn in scenario["turns"]:
                # 多轮用 "场景名::轮N::问题" 作为唯一键
                key = f"{scenario_name}::轮{turn['turn']}::{turn['question']}"
                status[key] = {
                    "passed": turn.get("passed", False),
                    "type": turn.get("actual_type", ""),
                    "elapsed": turn.get("elapsed", 0),
                    "fail_checks": turn.get("issues", []),
                    "scenario": scenario_name,
                    "turn": turn["turn"],
                    "question": turn["question"],
                }

    return status


def diff_results(old_status: dict, new_status: dict) -> dict:
    """
    对比两次结果，分类为 4 种状态：
    - fixed:    老报告失败 → 新报告通过（已修复）
    - regressed: 老报告通过 → 新报告失败（回归问题）
    - persistent: 两次都失败（持续问题）
    - new_issue: 新报告有但老报告没有（新增问题/新增题目）
    """
    all_keys = set(old_status.keys()) | set(new_status.keys())

    result = {
        "fixed": [],       # ✅ 已修复
        "regressed": [],   # ⚠️ 回归问题
        "persistent": [],  # 🔴 持续问题
        "new_issue": [],   # 🆕 新增失败
        "new_pass": [],    # 🆕 新增通过
        "still_pass": [],  # ✅ 持续通过
    }

    for key in sorted(all_keys):
        old = old_status.get(key)
        new = new_status.get(key)

        if old and new:
            # 两次都有
            if old["passed"] and new["passed"]:
                result["still_pass"].append({"question": key, "new": new})
            elif old["passed"] and not new["passed"]:
                result["regressed"].append({"question": key, "old": old, "new": new})
            elif not old["passed"] and new["passed"]:
                result["fixed"].append({"question": key, "old": old, "new": new})
            else:
                result["persistent"].append({"question": key, "old": old, "new": new})
        elif not old and new:
            # 新增题目
            if new["passed"]:
                result["new_pass"].append({"question": key, "new": new})
            else:
                result["new_issue"].append({"question": key, "new": new})
        # old 有但 new 没有 → 删除的题目，不报

    return result


def generate_diff_report(diff: dict, old_path: str, new_path: str) -> str:
    """生成 Markdown 格式的对比报告"""
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    # 统计
    total_old = sum(len(diff[k]) for k in ["fixed", "regressed", "persistent", "still_pass"])
    total_new = sum(len(diff[k]) for k in ["fixed", "regressed", "persistent", "still_pass",
                                             "new_issue", "new_pass"])
    total_fail_new = len(diff["regressed"]) + len(diff["persistent"]) + len(diff["new_issue"])
    total_pass_new = len(diff["fixed"]) + len(diff["still_pass"]) + len(diff["new_pass"])

    lines = [
        f"# QA 巡查 Diff 报告",
        f"",
        f"> 时间: {now}",
        f"> 旧报告: `{Path(old_path).name}`",
        f"> 新报告: `{Path(new_path).name}`",
        f"> 题目数: 旧 {total_old} → 新 {total_new}",
        f"> 新报告通过率: {total_pass_new}/{total_new} "
        f"({total_pass_new/total_new*100:.1f}%)" if total_new > 0 else "",
        f"",
    ]

    # 已修复 ✅
    if diff["fixed"]:
        lines.append(f"## ✅ 已修复 ({len(diff['fixed'])} 个)\n")
        lines.append("| 问题 | 原失败原因 |")
        lines.append("|------|-----------|")
        for item in diff["fixed"]:
            q = _short_question(item["question"])
            reason = ", ".join(item["old"].get("fail_checks", []))
            lines.append(f"| {q} | {reason} |")
        lines.append("")

    # 回归问题 ⚠️
    if diff["regressed"]:
        lines.append(f"## ⚠️ 回归问题 ({len(diff['regressed'])} 个)\n")
        lines.append("| 问题 | 新失败原因 |")
        lines.append("|------|-----------|")
        for item in diff["regressed"]:
            q = _short_question(item["question"])
            reason = ", ".join(item["new"].get("fail_checks", []))
            lines.append(f"| {q} | {reason} |")
        lines.append("")

    # 持续问题 🔴
    if diff["persistent"]:
        lines.append(f"## 🔴 持续问题 ({len(diff['persistent'])} 个)\n")
        lines.append("| 问题 | 失败原因 |")
        lines.append("|------|---------|")
        for item in diff["persistent"]:
            q = _short_question(item["question"])
            reason = ", ".join(item["new"].get("fail_checks", []))
            lines.append(f"| {q} | {reason} |")
        lines.append("")

    # 新增失败 🆕
    if diff["new_issue"]:
        lines.append(f"## 🆕 新增失败 ({len(diff['new_issue'])} 个)\n")
        lines.append("| 问题 | 失败原因 |")
        lines.append("|------|---------|")
        for item in diff["new_issue"]:
            q = _short_question(item["question"])
            reason = ", ".join(item["new"].get("fail_checks", []))
            lines.append(f"| {q} | {reason} |")
        lines.append("")

    # 摘要
    lines.append("## 📊 变化摘要\n")
    lines.append("| 类别 | 数量 | 说明 |")
    lines.append("|------|------|------|")
    lines.append(f"| ✅ 已修复 | {len(diff['fixed'])} | 之前失败 → 现在通过 |")
    lines.append(f"| ⚠️ 回归 | {len(diff['regressed'])} | 之前通过 → 现在失败 |")
    lines.append(f"| 🔴 持续 | {len(diff['persistent'])} | 两次都失败 |")
    lines.append(f"| 🆕 新增失败 | {len(diff['new_issue'])} | 新题目且失败 |")
    lines.append(f"| 🆕 新增通过 | {len(diff['new_pass'])} | 新题目且通过 |")
    lines.append(f"| ✅ 持续通过 | {len(diff['still_pass'])} | 两次都通过 |")
    lines.append("")

    return "\n".join(lines)


def _short_question(key: str) -> str:
    """缩短问题显示（多轮键带场景名，截取问题部分）"""
    if "::" in key:
        parts = key.split("::")
        return f"{parts[0][:10]}…{parts[-1][:15]}"
    return key[:25]


def find_latest_two(report_type: str = "golden") -> tuple:
    """在 qa/reports/ 中找最新两份同类型的 JSON"""
    if not REPORTS_DIR.exists():
        print(f"❌ 报告目录不存在: {REPORTS_DIR}")
        sys.exit(1)

    files = sorted(REPORTS_DIR.glob(f"{report_type}_*.json"))
    if len(files) < 2:
        print(f"❌ {REPORTS_DIR} 中 {report_type}_*.json 不足 2 份（当前 {len(files)} 份）")
        print("   至少需要跑两次巡查才能做 diff 对比")
        sys.exit(1)

    return str(files[-2]), str(files[-1])


def main():
    parser = argparse.ArgumentParser(description="QA 巡查 Diff 对比")
    parser.add_argument("--old", help="旧报告 JSON 路径")
    parser.add_argument("--new", help="新报告 JSON 路径")
    parser.add_argument("--latest", action="store_true",
                        help="自动找最新两份报告对比")
    parser.add_argument("--type", default="golden",
                        choices=["golden", "multi"],
                        help="报告类型（用于 --latest 筛选）")
    parser.add_argument("--output", help="对比报告输出路径（Markdown）")
    args = parser.parse_args()

    if args.latest:
        old_path, new_path = find_latest_two(args.type)
        print(f"📁 自动选取:")
        print(f"   旧: {old_path}")
        print(f"   新: {new_path}")
    elif args.old and args.new:
        old_path, new_path = args.old, args.new
    else:
        print("❌ 请指定 --old 和 --new，或使用 --latest")
        sys.exit(1)

    # 加载
    old_data = load_json(old_path)
    new_data = load_json(new_path)

    # 提取状态
    old_status = extract_question_status(old_data)
    new_status = extract_question_status(new_data)

    print(f"\n📊 旧报告: {len(old_status)} 个问题")
    print(f"📊 新报告: {len(new_status)} 个问题")

    # 对比
    diff = diff_results(old_status, new_status)

    # 终端输出摘要
    print(f"\n{'='*50}")
    print(f"📋 QA Diff 结果")
    print(f"{'='*50}")
    print(f"  ✅ 已修复:   {len(diff['fixed'])}")
    print(f"  ⚠️ 回归:     {len(diff['regressed'])}")
    print(f"  🔴 持续问题: {len(diff['persistent'])}")
    print(f"  🆕 新增失败: {len(diff['new_issue'])}")
    print(f"  🆕 新增通过: {len(diff['new_pass'])}")
    print(f"  ✅ 持续通过: {len(diff['still_pass'])}")
    print(f"{'='*50}")

    # 打印关键变化
    if diff["fixed"]:
        print(f"\n✅ 已修复:")
        for item in diff["fixed"]:
            print(f"  - {_short_question(item['question'])}")

    if diff["regressed"]:
        print(f"\n⚠️ 回归问题（需优先关注）:")
        for item in diff["regressed"]:
            q = _short_question(item["question"])
            reason = ", ".join(item["new"].get("fail_checks", []))
            print(f"  - {q}: {reason}")

    if diff["persistent"]:
        print(f"\n🔴 持续问题:")
        for item in diff["persistent"][:5]:
            print(f"  - {_short_question(item['question'])}")
        if len(diff["persistent"]) > 5:
            print(f"  ... 共 {len(diff['persistent'])} 个")

    # 保存报告
    report = generate_diff_report(diff, old_path, new_path)
    if args.output:
        output_path = args.output
    else:
        output_path = str(REPORTS_DIR / f"diff_{datetime.now().strftime('%Y%m%d_%H%M')}.md")

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(report)
    print(f"\n📝 对比报告: {output_path}")


if __name__ == "__main__":
    main()
