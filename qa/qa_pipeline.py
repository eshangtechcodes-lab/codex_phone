"""
QA Pipeline 一键入口 — 出题→测试→核对→巡查→报告

独立项目版本 — 所有配置从 config.py 读取

用法:
    # 全流程（自动出题 + 多轮测试 + 核对 + Codex 巡查）
    python qa/qa_pipeline.py --auto-generate --run-id e2e_01

    # 跳过 Codex 巡查（省时）
    python qa/qa_pipeline.py --auto-generate --skip-codex

    # 使用已有题目文件
    python qa/qa_pipeline.py --questions-file reports/questions.json

    # 限制场景数（快速验证）
    python qa/qa_pipeline.py --auto-generate --limit-scenarios 2 --skip-codex
"""

import argparse
import json
import os
import subprocess
import sys
import io
import time
from datetime import datetime
from pathlib import Path

if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

# 从 config 读取配置
sys.path.insert(0, str(Path(__file__).parent))
from config import DB_PATH, REPORTS_DIR, QA_DIR


def run_step(name: str, cmd: list, cwd: str) -> tuple:
    """运行一个步骤，返回 (success, elapsed)"""
    print(f"\n{'='*60}")
    print(f"  Step: {name}")
    print(f"  命令: {' '.join(cmd)}")
    print(f"{'='*60}\n")

    start = time.time()
    result = subprocess.run(
        cmd, cwd=cwd,
        capture_output=False,
        text=True, encoding='utf-8', errors='replace',
    )
    elapsed = round(time.time() - start, 1)

    if result.returncode == 0:
        print(f"\n  [OK] {name} 完成 ({elapsed}s)")
    else:
        print(f"\n  [FAIL] {name} 失败 (rc={result.returncode}, {elapsed}s)")

    return result.returncode == 0, elapsed


def main():
    parser = argparse.ArgumentParser(description="QA Pipeline 一键入口")
    parser.add_argument("--auto-generate", action="store_true", help="自动出题（Step 0）")
    parser.add_argument("--questions-file", help="外部题目 JSON 文件")
    parser.add_argument("--llm-mode", default="qwen", help="LLM 模式 (qwen/codex/hybrid)")
    parser.add_argument("--run-id", help="批次 ID（默认自动生成）")
    parser.add_argument("--limit-scenarios", type=int, help="限制场景数")
    parser.add_argument("--skip-codex", action="store_true", help="跳过 Codex 巡查（Layer 2）")
    parser.add_argument("--gen-mode", default="rules", choices=["rules", "codex"],
                        help="出题模式")
    parser.add_argument("--db", default=DB_PATH, help="数据库路径")
    args = parser.parse_args()

    run_id = args.run_id or datetime.now().strftime("e2e_%m%d_%H%M")
    qa_dir = str(QA_DIR)
    python = sys.executable

    print(f"QA Pipeline 启动")
    print(f"  批次: {run_id}")
    print(f"  LLM:  {args.llm_mode}")
    print(f"  DB:   {args.db}")
    print(f"  工作目录: {qa_dir}")

    steps_log = []
    questions_file = args.questions_file

    # ============================================================
    # Step 0: 自动出题
    # ============================================================
    if args.auto_generate and not questions_file:
        questions_file = str(REPORTS_DIR / f"questions_{run_id}.json")
        cmd = [
            python, str(QA_DIR / "qa_question_gen.py"),
            "--mode", args.gen_mode,
            "--db", args.db,
            "--output", questions_file,
            "--run-id", run_id,
        ]
        ok, elapsed = run_step("Step 0: 自动出题", cmd, qa_dir)
        steps_log.append({"step": "出题", "ok": ok, "elapsed": elapsed, "output": questions_file})
        if not ok:
            print("\n[ABORT] 出题失败，中止 Pipeline")
            return

    # ============================================================
    # Step 1: 多轮测试
    # ============================================================
    multi_output = str(REPORTS_DIR / f"qa_multi_{run_id}.json")
    cmd = [
        python, str(QA_DIR / "qa_runner.py"),
        "--multi-turn",
        "--llm-mode", args.llm_mode,
        "--run-id", f"multi_{args.llm_mode}_{run_id.split('_')[-1]}",
        "--output", multi_output,
    ]
    if questions_file:
        cmd.extend(["--questions-file", questions_file])
    if args.limit_scenarios:
        cmd.extend(["--limit", str(args.limit_scenarios)])

    ok, elapsed = run_step("Step 1: 多轮测试", cmd, qa_dir)
    steps_log.append({"step": "测试", "ok": ok, "elapsed": elapsed, "output": multi_output})
    if not ok:
        print("\n[ABORT] 测试执行失败，中止 Pipeline")
        return

    # ============================================================
    # Step 2: 自动核对 (Layer 1)
    # ============================================================
    autocheck_output = multi_output.replace('.json', '_autocheck.json')
    autocheck_report = multi_output.replace('.json', '_autocheck.md')
    cmd = [
        python, str(QA_DIR / "qa_auto_check.py"),
        "--input", multi_output,
        "--db", args.db,
        "--output", autocheck_output,
        "--report", autocheck_report,
    ]
    ok, elapsed = run_step("Step 2: 自动核对 (Layer 1)", cmd, qa_dir)
    steps_log.append({"step": "核对", "ok": ok, "elapsed": elapsed, "output": autocheck_output})

    # ============================================================
    # Step 3: Codex 巡查 (Layer 2)
    # ============================================================
    if not args.skip_codex:
        cmd = [
            python, str(QA_DIR / "qa_codex_dispatch.py"),
            "--input", autocheck_output,
            "--report", multi_output,
        ]
        if args.limit_scenarios:
            cmd.extend(["--limit", str(args.limit_scenarios)])

        ok, elapsed = run_step("Step 3: Codex 巡查 (Layer 2)", cmd, qa_dir)
        steps_log.append({"step": "巡查", "ok": ok, "elapsed": elapsed})
    else:
        print(f"\n  [SKIP] Step 3: Codex 巡查（--skip-codex）")
        steps_log.append({"step": "巡查", "ok": None, "elapsed": 0, "note": "skipped"})

    # ============================================================
    # 汇总报告
    # ============================================================
    print(f"\n{'='*60}")
    print(f"  Pipeline 汇总")
    print(f"{'='*60}")

    total_elapsed = sum(s['elapsed'] for s in steps_log)
    for s in steps_log:
        status = "OK" if s['ok'] else ("SKIP" if s['ok'] is None else "FAIL")
        print(f"  [{status}] {s['step']}: {s['elapsed']}s")
    print(f"  总耗时: {total_elapsed}s ({total_elapsed/60:.1f}min)")

    # 读取核对结果摘要
    if Path(autocheck_output).exists():
        with open(autocheck_output, 'r', encoding='utf-8') as f:
            check = json.load(f)
        stats = check.get('stats', {})
        print(f"\n  Layer 1 核对:")
        print(f"    已核对: {stats.get('checked_turns', 0)}/{stats.get('total_turns', 0)}")
        print(f"    严重: {stats.get('critical', 0)} | 警告: {stats.get('warning', 0)} | 准确: {stats.get('ok', 0)}")

    # 生成 Pipeline 报告
    pipeline_report = str(REPORTS_DIR / f"pipeline_report_{run_id}.md")
    report_lines = [
        f"# Pipeline 报告 — {run_id}\n",
        f"> 时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"> LLM: {args.llm_mode}",
        f"> 总耗时: {total_elapsed}s\n",
        "## 步骤汇总\n",
        "| 步骤 | 状态 | 耗时 |",
        "|------|------|------|",
    ]
    for s in steps_log:
        status = "OK" if s['ok'] else ("SKIP" if s['ok'] is None else "FAIL")
        report_lines.append(f"| {s['step']} | {status} | {s['elapsed']}s |")

    if Path(autocheck_output).exists():
        report_lines.append(f"\n## Layer 1 核对结果\n")
        report_lines.append(f"- 核对覆盖: {stats.get('checked_turns', 0)}/{stats.get('total_turns', 0)}")
        report_lines.append(f"- 严重偏差: {stats.get('critical', 0)}")
        report_lines.append(f"- 警告: {stats.get('warning', 0)}")
        report_lines.append(f"- 准确: {stats.get('ok', 0)}")

    report_lines.append(f"\n## 输出文件\n")
    for s in steps_log:
        if s.get('output'):
            report_lines.append(f"- {s['step']}: `{s['output']}`")
    report_lines.append(f"- Pipeline 报告: `{pipeline_report}`")

    with open(pipeline_report, 'w', encoding='utf-8') as f:
        f.write('\n'.join(report_lines))
    print(f"\n  Pipeline 报告: {pipeline_report}")


if __name__ == "__main__":
    main()
