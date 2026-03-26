"""
Codex 并行巡查调度器 — Layer 2: 场景分割 + 多 Codex 并行巡查

独立项目版本 — 所有配置从 config.py 读取

输入: auto_check.json (Layer 1 结果)
输出: 3 份子巡查报告 → 合并为最终报告

用法:
    python qa/qa_codex_dispatch.py --input reports/autocheck.json --report reports/multi.json
    python qa/qa_codex_dispatch.py --input reports/autocheck.json --report reports/multi.json --dry-run
"""

import argparse
import json
import math
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
from config import CODEX_CWD, CODEX_DATA_DIR, PATROL_OUTPUT_DIR

# Codex CLI 命令模板
CODEX_CMD = [
    "codex", "exec", "--full-auto",
    "--add-dir", CODEX_DATA_DIR,
]

# 巡查任务 Prompt 模板
PATROL_PROMPT_TEMPLATE = """你是 QA 巡查 AI。请对以下场景做幻觉检测和回答质量评分。

## 业务常识（评分时必须遵守）

以下是驿达系统已确认的业务口径规则，评分时不应将符合规则的回答判为错误：

### 口径同义映射
| 用户说法 | 系统含义 | 关系 |
|---------|---------|------|
| 营收/营业收入/营业额/营业金额 | 对客销售 | **完全同义**（数字应完全一致） |
| 业主营业收入/业主收入/业主入账 | 业主营业收入（除税） | **不同于对客销售**（数字不同） |

⚠️ 当用户问"营业收入"而 AI 回答"对客销售"并给出同一数字时，这是**正确的**，不应判为 ENTITY_MISMATCH。
⚠️ 但当用户问"业主营业收入"而 AI 回答"对客销售"数字时，这是**错误的**。

### 实体分类
- 133 对高速服务区：归属五大管理中心（皖中/皖东/皖南/皖西/皖北），参与排名
- 城市店/商城/实验室/餐饮公司等：不参与服务区排名，不混入片区统计

### 行业常识
- 自营门店占比低是**正常**行业规律（联营为高速服务区主流模式），不应判为异常
- 数据中"去年"字段指**去年同期**（同月/同时段），不是去年全年

## 检查规则

对每一轮，评估以下维度并打分 (1-5):
1. **幻觉检测**: 回答中是否有无工具依据的信息？(1=严重幻觉, 5=全有依据)
2. **分析深度**: (1=只给数字, 3=有趋势分析, 5=有原因+建议+可操作)
3. **结论可信度**: (1=数字和结论都错, 3=数字对但结论偏, 5=数字+逻辑都对)
4. **可操作性**: (1=看完没有行动方向, 3=有方向但不具体, 5=领导看完能直接决策)

## Python 自动核对已完成的检查

以下数字偏差已由 Python 层核对（和 dameng_mirror.db SQL 对比），你不需要重复 SQL 查询：
{auto_check_summary}

## 待检查的场景数据

{scenarios_data}

## 输出格式

对每个场景每轮，输出：
```
### 场景 X: [名称]
#### 轮 Y: "问题"
- 幻觉: [1-5] — [简要说明]
- 深度: [1-5] — [说明]
- 可信: [1-5] — [说明]
- 可操作: [1-5] — [说明]
- 根因: [如有严重问题，给根因代码: CACHE_STALE / ENTITY_MISMATCH / TOOL_WRONG / TOOL_RESULT_MANGLED / CONTEXT_POLLUTION / SCOPE_DRIFT / NONE]
```

最后给出该组场景的**平均评分**和**最突出的问题**。

将完整报告写入 {output_path}
"""


def build_auto_check_summary(check_result: dict, scenario_indices: list) -> str:
    """从 auto_check 结果中提取指定场景的核对摘要"""
    lines = []
    for si in scenario_indices:
        if si >= len(check_result.get('scenarios', [])):
            continue
        scenario = check_result['scenarios'][si]
        for turn in scenario['turns']:
            devs = turn.get('deviations', [])
            if devs:
                status_parts = []
                for d in devs:
                    icon = "!!!" if d['severity'] == 'critical' else ("!" if d['severity'] == 'warning' else "OK")
                    status_parts.append(f"{d['field']}: AI={d['ai']} DB={d['db']} [{icon}]")
                lines.append(f"- 场景{si} 轮{turn['turn']} {turn['question']}: {'; '.join(status_parts)}")
            elif turn.get('ai_numbers'):
                lines.append(f"- 场景{si} 轮{turn['turn']} {turn['question']}: 无 DB 数据可核对")

    return "\n".join(lines) if lines else "（本组场景无可核对的数字偏差）"


def build_scenarios_data(report: dict, scenario_indices: list) -> str:
    """构建场景数据文本供 Codex 检查"""
    lines = []
    scenarios = report.get('scenarios', [])
    for si in scenario_indices:
        if si >= len(scenarios):
            continue
        sc = scenarios[si]
        lines.append(f"\n## 场景 {si}: {sc['name']}")
        lines.append(f"> {sc.get('description', '')}")
        for turn in sc['turns']:
            text = turn.get('full_response') or turn.get('report_preview', '')
            preview = text[:500] if text else '(无回答)'
            lines.append(f"\n### 轮 {turn['turn']}: \"{turn['question']}\"")
            lines.append(f"- 分类: {turn.get('actual_type', '?')}")
            lines.append(f"- 回答:\n```\n{preview}\n```")

    return "\n".join(lines)


def split_scenarios(total: int, workers: int) -> list:
    """将场景分成 N 组"""
    chunk = math.ceil(total / workers)
    groups = []
    for i in range(0, total, chunk):
        groups.append(list(range(i, min(i + chunk, total))))
    return groups


def generate_task_file(
    group_id: int,
    scenario_indices: list,
    report: dict,
    check_result: dict,
    output_dir: str,
) -> str:
    """为一组场景生成 Codex 任务 Prompt"""
    auto_summary = build_auto_check_summary(check_result, scenario_indices)
    scenarios_data = build_scenarios_data(report, scenario_indices)
    output_path = f"{output_dir}/patrol_worker_{group_id}.md"

    prompt = PATROL_PROMPT_TEMPLATE.format(
        auto_check_summary=auto_summary,
        scenarios_data=scenarios_data,
        output_path=output_path,
    )
    return prompt, output_path


def launch_codex(prompt: str, cwd: str) -> subprocess.Popen:
    """启动一个 Codex CLI 进程（通过 stdin 传入 prompt 以避免命令行长度限制）"""
    cmd = CODEX_CMD + ["-"]  # "-" 表示从 stdin 读取 prompt
    proc = subprocess.Popen(
        cmd,
        cwd=cwd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding='utf-8',
        errors='replace',
        shell=True,  # Windows 上需要 shell=True 才能找到 npm 全局 .cmd 命令
    )
    # 写入 prompt 到 stdin 并关闭，让 Codex 开始执行
    proc.stdin.write(prompt)
    proc.stdin.close()
    return proc


def merge_reports(output_dir: str, worker_count: int) -> str:
    """合并所有 worker 的巡查报告"""
    merged_lines = [
        f"# QA 巡查报告 — 合并版",
        f"",
        f"> 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"> 合并自 {worker_count} 个并行 Codex worker",
        f"",
    ]

    for i in range(worker_count):
        path = Path(output_dir) / f"patrol_worker_{i}.md"
        if path.exists():
            content = path.read_text(encoding='utf-8')
            merged_lines.append(f"\n---\n## Worker {i}\n")
            merged_lines.append(content)
        else:
            merged_lines.append(f"\n---\n## Worker {i}: 报告未生成\n")

    return "\n".join(merged_lines)


def main():
    parser = argparse.ArgumentParser(description="Codex 并行巡查调度器")
    parser.add_argument("--input", required=True, help="auto_check JSON 路径")
    parser.add_argument("--report", required=True, help="原始 qa_runner JSON 报告路径")
    parser.add_argument("--workers", type=int, default=3, help="并行 Codex 数量")
    parser.add_argument("--output-dir", default=PATROL_OUTPUT_DIR, help="输出目录")
    parser.add_argument("--limit", type=int, help="限制场景数量")
    parser.add_argument("--dry-run", action="store_true", help="只生成任务不启动 Codex")
    parser.add_argument("--cwd", default=CODEX_CWD, help="Codex 工作目录")
    args = parser.parse_args()

    with open(args.input, 'r', encoding='utf-8') as f:
        check_result = json.load(f)
    with open(args.report, 'r', encoding='utf-8') as f:
        report = json.load(f)

    total_scenarios = len(report.get('scenarios', []))
    if args.limit:
        total_scenarios = min(total_scenarios, args.limit)

    output_dir = args.output_dir
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    groups = split_scenarios(total_scenarios, args.workers)
    print(f"场景总数: {total_scenarios}")
    print(f"Worker 数: {len(groups)}")
    for i, g in enumerate(groups):
        print(f"  Worker {i}: 场景 {g}")

    tasks = []
    for i, indices in enumerate(groups):
        prompt, output_path = generate_task_file(
            i, indices, report, check_result, output_dir
        )
        task_file = Path(output_dir) / f"task_worker_{i}.md"
        task_file.write_text(prompt, encoding='utf-8')
        tasks.append({
            'group_id': i, 'indices': indices,
            'prompt': prompt, 'output_path': output_path,
            'task_file': str(task_file),
        })
        print(f"  任务文件: {task_file}")

    if args.dry_run:
        print("\n[dry-run] 任务文件已生成，未启动 Codex")
        return

    print(f"\n启动 {len(tasks)} 个 Codex 进程...")
    procs = []
    for task in tasks:
        proc = launch_codex(task['prompt'], args.cwd)
        procs.append((task['group_id'], proc))
        print(f"  Worker {task['group_id']} PID={proc.pid}")
        time.sleep(2)

    print("\n等待所有 Worker 完成...")
    for gid, proc in procs:
        stdout, _ = proc.communicate()
        rc = proc.returncode
        status = "完成" if rc == 0 else f"失败(rc={rc})"
        print(f"  Worker {gid} {status}")

        log_file = Path(output_dir) / f"codex_log_worker_{gid}.txt"
        log_file.write_text(stdout or '', encoding='utf-8')

    print("\n合并报告...")
    merged = merge_reports(output_dir, len(tasks))
    merged_path = Path(output_dir) / "patrol_merged.md"
    merged_path.write_text(merged, encoding='utf-8')
    print(f"合并报告: {merged_path}")

    meta = {
        'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'workers': len(tasks),
        'scenarios': total_scenarios,
        'groups': [t['indices'] for t in tasks],
        'merged_report': str(merged_path),
    }
    meta_path = Path(output_dir) / "dispatch_meta.json"
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding='utf-8')


if __name__ == "__main__":
    main()
