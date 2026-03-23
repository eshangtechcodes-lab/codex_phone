# QA 自动巡查体系

AI-Python 项目的自动化质量保障系统。通过 **发题→评判→深度验证→巡查报告→Diff 跟踪** 五个环节，持续监控 AI 回答质量。

## 架构概览

```
┌──────────────────────────────────────────────────────────┐
│                    QA 自动巡查流水线                       │
│                                                          │
│  qa_runner.py ──→ qa_verifier.py ──→ Codex 巡查          │
│  (发题+L1/L2)     (L3 深度验证)      (5维度报告)          │
│       │                │                  │               │
│       ▼                ▼                  ▼               │
│   reports/*.json   L3 验证结果     qa_patrol_*.md         │
│       │                                                   │
│       ▼                                                   │
│   qa_diff.py ──→ diff 对比报告（跟踪修复进度）             │
└──────────────────────────────────────────────────────────┘
```

## 文件说明

| 文件 | 职责 | 行数 |
|------|------|------|
| `qa_runner.py` | 测试运行器：发题到 API、L1/L2 评判、生成报告 | ~870 |
| `qa_verifier.py` | 深度验证器（L3）：用 DB 数据校验 AI 回答真实性 | ~810 |
| `qa_diff.py` | Diff 对比：比较两次巡查结果，跟踪修复进度 | ~240 |
| `codex_patrol_spec.md` | Codex 5 维度巡查规范 | - |
| `codex_patrol_task.md` | Codex 巡查任务模板（给 `codex exec` 的 prompt） | - |
| `qa_patrol_*.md` | 巡查报告（Codex 生成） | - |
| `reports/` | 历史报告归档目录 | - |

## 快速开始

### 环境要求

- Python 3.8+（无外部依赖，只用标准库）
- 网络可访问 `https://llm.eshangtech.com`
- （深度验证）需要 `d:/AISpace/AI-Python/data/` 下的 `db.sqlite3` 和 `dameng_mirror.db`

### 1. 跑单轮 Golden Set（40 题基线测试）

```bash
# 全部 40 题
python qa/qa_runner.py

# 只跑前 10 题
python qa/qa_runner.py --limit 10

# 指定问题
python qa/qa_runner.py --questions "新桥服务区2月营收" "巢湖服务区车流量"
```

输出：
- `qa/reports/golden_{run_id}.json` — 结构化结果
- `qa/reports/golden_{run_id}.md` — Markdown 报告

### 2. 跑多轮语义切换测试（16 组场景）

```bash
python qa/qa_runner.py --multi-turn
```

多轮测试模拟真实用户对话：范围切换、时间切换、口径切换、领导视察、新人追问等 16 种场景，共约 80 轮。

输出：
- `qa/reports/multi_{run_id}.json`
- `qa/reports/multi_{run_id}.md`

### 3. Diff 对比（跟踪修复进度）

```bash
# 自动找最新两份同类报告对比
python qa/qa_diff.py --latest                  # Golden Set
python qa/qa_diff.py --latest --type multi     # 多轮测试

# 手动指定两个文件
python qa/qa_diff.py --old reports/golden_qa_20260322.json --new reports/golden_qa_20260323.json
```

Diff 分类：
- ✅ **已修复** — 旧报告失败 → 新报告通过
- ⚠️ **回归** — 旧报告通过 → 新报告失败
- 🔴 **持续问题** — 两次都失败
- 🆕 **新增** — 新报告独有的问题

### 4. 深度验证（L3）

```bash
# 对已有 JSON 结果做事后 DB 核对
python qa/qa_verifier.py --input qa/reports/multi_xxx.json
```

L3 验证内容：
- 实体存在性（回答中的 SA 是否在 DB 中存在）
- 片区归属（问皖北时回答的 SA 是否真属于皖北）
- 营收数字核对（与 `dameng_mirror.db` 对比）
- 排名顺序验证
- 跨轮一致性（多轮对话中同一 SA 同口径数字是否矛盾）

### 5. Codex 5 维度巡查

```bash
codex exec --full-auto \
  --add-dir d:\AISpace\AI-Python\data \
  -C d:\AISpace\Antigravity_Phone\codex_phone \
  -c model_reasoning_effort="low" \
  "读取 qa/codex_patrol_task.md 并执行"
```

Codex 巡查 5 个维度：
| 维度 | 检查内容 |
|------|---------|
| A. 幻觉检测 | 有数字但没调工具 → 幻觉 |
| B. 数字准确性 | AI 数字 vs DB 真实值 |
| C. 工具选择 | 工具是否匹配问题类型 |
| D. 分类正确性 | A/B/C 分类是否准确 |
| E. 回答质量 | 是否答了问题、逻辑是否合理 |

> **建议用 `low` reasoning effort**，巡查效果和 `xhigh` 一致，但 token 省约 50%。

## 评判分层

```
L1 硬规则（零误判）
├── API 是否成功
├── 分类是否正确（A/B/C）
├── A 类是否调了工具
└── 回答是否泄露内部信息

L2 数据合理性（低误判，WARNING 级别）
├── 回复长度（<20 或 >3000）
├── 营收金额范围
├── 车流量范围
└── 百分比极端值（>500%）

L3 深度验证（qa_verifier.py）
├── 实体存在性
├── 片区归属
├── 营收数字 vs DB
├── 排名顺序
└── 跨轮一致性

L4 Codex 巡查（codex exec）
├── 幻觉检测
├── 数字准确性（主动跑 SQL）
├── 工具选择合理性
├── 分类正确性
└── 回答质量
```

## 测试题库

### Golden Set（40 题）

覆盖 10 个类别：基础营收、车流、排名/片区/对比、商户/品牌、财务/合同、口径消歧、坪效/客单价、实时播报、知识咨询、边界收口，另有 3 题口径一致性对照组。

### 多轮场景（16 组 ~80 轮）

| # | 场景名 | 轮次 | 覆盖 |
|---|--------|------|------|
| 1-4 | 基础切换（范围/时间/类型/口径） | 4×4 | 上下文保持 |
| 5 | 领导视察 | 8 | 逐层深入 |
| 6 | 商务分析 | 6 | 从数据到原因到建议 |
| 7 | 跨片区巡检 | 6 | 多片区快速切换 |
| 8 | 新人连环追问 | 6 | 模糊→澄清→跳跃 |
| 9 | 数据验证 | 4 | 同义问法数字一致性 |
| 10 | 实时↔历史 | 4 | C-A-C 快速跳转 |
| 11 | 🆕 车流量专项 | 5 | get_traffic 全路径 |
| 12 | 🆕 合同查询 | 5 | get_contract / get_finance |
| 13 | 🆕 品牌排名 | 5 | get_brand_ranking / get_merchant |
| 14 | 🆕 跨片区深入对比 | 6 | 片区对比→SA下钻→多工具串联 |
| 15 | 🆕 异常边界 | 6 | 超范围/未来/空结果/闲聊/纠偏 |

## 报告目录结构

```
qa/reports/
├── golden_qa_20260323_2030.json     # 单轮结果
├── golden_qa_20260323_2030.md       # 单轮报告
├── multi_multi_low_0323.json        # 多轮结果
├── multi_multi_low_0323.md          # 多轮报告
└── diff_20260323_2039.md            # Diff 对比报告
```

## 参数说明

### qa_runner.py

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `--questions` | 指定问题列表 | Golden Set |
| `--output` | JSON 输出路径 | `qa/reports/...` |
| `--report` | MD 报告路径 | `qa/reports/...` |
| `--api-url` | API 地址 | `https://llm.eshangtech.com/api/agent/` |
| `--run-id` | 批次 ID | 自动生成 `qa_YYYYMMDD_HHMM` |
| `--limit` | 限制题数 | 全部 |
| `--multi-turn` | 多轮模式 | 关 |

### qa_diff.py

| 参数 | 说明 |
|------|------|
| `--old` | 旧报告 JSON 路径 |
| `--new` | 新报告 JSON 路径 |
| `--latest` | 自动找最新两份 |
| `--type` | 报告类型 `golden`/`multi` |
| `--output` | 对比报告输出路径 |

## 服务器部署

QA 体系全部基于 Python 标准库，无外部依赖，可直接部署到 Ubuntu 服务器。

```bash
# 1. 复制文件
scp -r qa/ ubuntu@llm.eshangtech.com:~/qa/

# 2. 定时任务（每天早 8 晚 8）
crontab -e
# 0 8 * * * cd ~/qa && python3 qa_runner.py --limit 20 >> /var/log/qa.log 2>&1
# 0 20 * * * cd ~/qa && python3 qa_runner.py --multi-turn >> /var/log/qa.log 2>&1

# 3. 服务器上可用本地地址加速
python3 qa_runner.py --api-url http://127.0.0.1:8000/api/agent/
```

> **注意**：Codex 巡查（`codex exec`）需要 Codex CLI 环境，目前只能在本地 Windows 运行。`qa_runner.py` 和 `qa_verifier.py` 可独立在服务器运行。

## 数据库依赖

| 数据库 | 用途 | 使用者 |
|--------|------|--------|
| `db.sqlite3` | 服务区/片区/品牌/节假日基础数据 | qa_verifier.py, Codex |
| `dameng_mirror.db` | 营收真实值（月度/日度/排名） | qa_verifier.py, Codex |

路径配置：
- codex_phone: 绝对路径 `d:/AISpace/AI-Python/data/`
- AI-Python: 相对路径 `../../data/`（从 scripts/testing/）
