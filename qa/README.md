# QA 自动巡查系统

> 高速公路服务区智能助手 — 全自动质量检测体系

## 架构概览

```
qa/
├── config.py              ← 配置中心（API/DB/路径）
├── qa_pipeline.py         ← 一键入口
├── qa_question_gen.py     ← 智能出题器
├── qa_runner.py           ← 测试执行引擎
├── qa_auto_check.py       ← Layer 1: Python 数字核对
├── qa_codex_dispatch.py   ← Layer 2: Codex 并行巡查
├── qa_history.jsonl       ← 历史日志
├── dameng_mirror_copy.db  ← 数据镜像（本地副本）
└── reports/               ← 所有报告输出
```

## 三层检测体系

| 层级 | 工具 | 速度 | 能力 |
|------|------|------|------|
| Layer 1 | `qa_auto_check.py` | 秒级 | 正则提取 + SQL 对比，零误判 |
| Layer 2 | `qa_codex_dispatch.py` | 分钟级 | 3x Codex 并行，幻觉检测 + 质量评分 |
| Layer 3 | `qa_pipeline.py` | 全程 | 一键编排 + 报告汇总 |

## 快速开始

### 一键全流程

```bash
# 完整 E2E（出题→测试→核对→Codex 巡查），约 30-40 分钟
python qa/qa_pipeline.py --auto-generate --run-id test01

# 跳过 Codex 巡查，只做 Layer 1 核对（省时）
python qa/qa_pipeline.py --auto-generate --skip-codex --run-id quick01

# 限制场景数快速验证
python qa/qa_pipeline.py --auto-generate --skip-codex --limit-scenarios 2
```

### 单独运行各模块

```bash
# 1. 出题
python qa/qa_question_gen.py --mode rules --output reports/questions.json

# 2. 多轮测试
python qa/qa_runner.py --multi-turn --run-id multi_01

# 3. 自动核对
python qa/qa_auto_check.py --input reports/qa_multi_xxx.json

# 4. Codex 并行巡查
python qa/qa_codex_dispatch.py --input reports/xxx_autocheck.json --report reports/qa_multi_xxx.json

# 5. 单轮 Golden Set 测试
python qa/qa_runner.py --run-id golden_01
```

## 配置说明

所有配置集中在 `config.py`，迁移时只需修改该文件：

| 配置项 | 说明 | 默认值 |
|--------|------|--------|
| `DB_PATH` | 数据库路径 | 优先本地 `dameng_mirror_copy.db` |
| `API_URL` | 服务端 API | `https://llm.eshangtech.com/api/agent/` |
| `REPORTS_DIR` | 报告输出目录 | `qa/reports/` |
| `HISTORY_PATH` | 历史日志 | `qa/qa_history.jsonl` |
| `CODEX_CWD` | Codex 工作目录 | `codex_phone/` |

## 数据依赖

- **`dameng_mirror_copy.db`**: 达梦数据库本地镜像，包含营收/车流/商户等核心表
- **API 服务端**: 通过 `API_URL` 访问，需要网络连通

## 出题器多轮场景

出题器内置 5 种多轮模板（5-6 轮），覆盖 Codex v3 巡查报告发现的高风险场景：

| 模板 | 轮数 | 目标 |
|------|------|------|
| 领导巡检 | 6 | 营收→车流→商户→排名→环比→亏损 |
| 口径验证 | 5 | 营收→对客→营业收入→业主(⚠)→概念澄清 |
| 实时切换 | 5 | C→A→C跳转 + 换SA |
| 深挖经营 | 5 | 同比→商户→合同→坪效→客单价 |
| 跨片区 | 5 | 片区→TOP→末位→深挖→代词承接 |

## 核对能力

Layer 1 自动核对支持以下维度的 SQL 对比：

- **营收**: `XXX万元` / `XXX元`（自动转万）
- **同比/环比**: 自动识别升降，计算百分点偏差
- **排名**: 精确排名核对
- **断面车流**: 万单位对比
- **入区车流**: 万单位对比
- **入区率**: 百分比对比
- **利润/盈利**: 万元对比

## 报告目录

所有报告输出到 `reports/`，命名规则：

```
reports/
├── questions_{run_id}.json          ← 出题器生成的题目
├── qa_multi_{run_id}.json           ← 多轮测试原始结果
├── qa_multi_{run_id}_autocheck.json ← Layer 1 核对结果
├── qa_multi_{run_id}_autocheck.md   ← Layer 1 核对报告
├── qa_report_{run_id}.md            ← 单轮测试报告
├── pipeline_report_{run_id}.md      ← Pipeline 汇总报告
├── patrol_output/                   ← Codex 巡查输出
│   ├── patrol_worker_0.md
│   ├── patrol_worker_1.md
│   ├── patrol_worker_2.md
│   └── patrol_merged.md            ← 合并巡查报告
└── patrol_*.md                      ← 手动巡查报告
```
