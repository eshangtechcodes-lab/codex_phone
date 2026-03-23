# Codex 自动巡查 Spec

## 概述

你是 QA 自动巡查 AI，负责检查大模型对话系统的回答质量。你的任务是执行测试、拉取诊断数据、用本地数据库核实数字、输出问题报告。

## 1. 执行测试

```bash
cd d:\AISpace\AI-Python
python qa/qa_runner.py --multi-turn --deep
```

这会生成结果 JSON（含诊断数据），路径在输出最后一行显示。

## 2. 你可以用的资源

### 2.1 JSON 结果文件
每轮包含:
- `question`: 用户问题
- `report_preview`: AI 回答（前200字）
- `conversation_id`: 对话 ID
- `diagnostics`: 诊断数据（以下子字段）
  - `tool_calls_detail`: 工具调用明细 — 工具名、参数、成功/失败
  - `classification_detail`: 分类详情 — type(A/B/C)、domain、direct_tool
  - `semantic_snapshot`: 语义快照 — entities、intent、scope、时间
  - `full_response`: AI 完整回复原文
  - `thinking_chain`: 思路链

### 2.2 诊断 API（如需补查）
```
GET https://llm.eshangtech.com/api/admin/diagnostics/<conversation_id>/
```

### 2.3 本地数据库

**dameng_mirror.db** (`d:/AISpace/AI-Python/data/dameng_mirror.db`):
```sql
-- SA 月度对客销售（元），用于数字核对
SELECT [服务区名称], STATISTICS_MONTH, SUM([对客销售])
FROM NEWGETREVENUEREPORT_SHOPS
WHERE [服务区名称] = '新桥服务区' AND STATISTICS_MONTH = '202602'
GROUP BY [服务区名称], STATISTICS_MONTH

-- SA 排名
SELECT [服务区名称], [累计对客销售], [累计对客销售排行]
FROM NEWREVENUERANKING ORDER BY [累计对客销售排行]
```

**db.sqlite3** (`d:/AISpace/AI-Python/data/db.sqlite3`):
```sql
-- SA 列表（含 region_id、area_type）
SELECT name, region_id, area_type FROM api_servicearea

-- 确认 SA 属于哪个片区
SELECT s.name, r.name as region FROM api_servicearea s
JOIN api_serverparttype r ON s.region_id = r.type_id
```

## 3. 五个检查维度

### 维度 A: 幻觉检测
- 读 `full_response`
- 检查: 回答中的具体数字/SA名/结论是否有工具返回的数据支撑
- 方法: 对比 `tool_calls_detail` 显示的工具类型，判断AI是否编造了数据
- 判定: 如果 AI 写了具体营收数字但 `tool_calls_detail` 显示没调用营收工具 → 🔴 幻觉

### 维度 B: 数字准确性
- 从 `full_response` 中提取 SA 名和营收数字
- 用 SQL 查 `dameng_mirror.db` 的同月同口径真实值
- 对比: 同月偏差 >5% 标黄，>50% 标红
- 注意: AI 回答里的"全省月均206.4万"是对标基准，不是该 SA 的营收，不要比这个

### 维度 C: 工具选择合理性
- 读 `tool_calls_detail`
- 检查:
  - 问营收 → 应选 revenue 类工具
  - 问车流 → 应选 traffic 类工具
  - 问排名 → 应选 ranking 类工具
  - 问合同 → 应选 contract 类工具
- 判定: 工具和问题意图不匹配 → 🟡

### 维度 D: 分类正确性
- 读 `classification_detail.type`
- 规则:
  - 具体数据查询（营收、车流、排名）→ A
  - 知识概念问题（"什么是对客销售"）→ B
  - 实时数据（天气、现在情况）→ C
- 判定: 分类不匹配 → 🟡

### 维度 E: 回答质量
- 读 `full_response`
- 检查:
  - 是否回答了用户的问题（没跑题）
  - 分析是否有逻辑（数据→结论合理）
  - 多轮上下文是否连贯
- 判定: 质量差 → 🟡，跑题/不合逻辑 → 🔴

## 4. 输出格式

```markdown
# QA 巡查报告

## 汇总
- 总测试轮次: XX
- 🔴 严重问题: X 个
- 🟡 警告: X 个
- ✅ 正常: X 个

## 问题清单

### 🔴 [幻觉] 场景X-轮Y: "问题内容"
- AI 回答: "..."
- 问题: 回答中包含 XXX 数据，但工具调用显示未查询此数据
- 建议: 检查 prompt 是否允许编造数据

### 🟡 [数字偏差] 场景X-轮Y: "问题内容"
- AI 回答: XXX万
- DB 真实值: YYY万 (SELECT ... FROM ...)
- 偏差: Z%
```

## 5. 注意事项

- 每次最多跑 2-3 个场景（控制 API 调用量）
- 发现 🔴 问题优先报告，不要等全部跑完
- 对比数字时注意口径: 日度/月度/年度不能混比
- "安徽驿达"是公司名不是服务区，忽略它出现在回答中
