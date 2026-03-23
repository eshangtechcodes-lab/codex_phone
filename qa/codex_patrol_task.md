# Codex 巡查任务

你是 QA 巡查 AI。请对 `qa/qa_patrol_test.json` 中的测试结果做 5 维度检查。

## 你的检查步骤

### 1. 读取测试结果
```bash
cat qa/qa_patrol_test.json | python -m json.tool | head -100
```

### 2. 逐轮检查以下维度

**维度 A: 幻觉检测**
- 读每轮的 `diagnostics.full_response`
- 读 `diagnostics.tool_calls_detail`
- 如果回答中有具体营收数字，但 tool_calls 没调营收工具 → 🔴 幻觉

**维度 B: 数字准确性**
- 从 `full_response` 中提取 SA 名和营收数字
- 用 sqlite3 查 `d:/AISpace/AI-Python/data/dameng_mirror.db` 获取真实值:
```bash
sqlite3 d:/AISpace/AI-Python/data/dameng_mirror.db "SELECT [服务区名称], SUM([对客销售])/10000 as wan FROM NEWGETREVENUEREPORT_SHOPS WHERE [服务区名称]='新桥服务区' AND STATISTICS_MONTH='202602' GROUP BY [服务区名称]"
```
- 对比 AI 回答的数字和 DB 真实值

**维度 C: 工具选择合理性**
- 问营收应选 revenue 类工具
- 问车流应选 traffic 类工具
- 问排名应选 ranking 类工具

**维度 D: 分类正确性**
- 数据查询 → A
- 知识问答 → B

**维度 E: 回答质量**
- 是否回答了问题
- 分析逻辑是否合理

### 3. 输出报告
将检查结果写入 `qa/qa_patrol_0323.md`，格式:

```markdown
# QA 巡查报告 2026-03-23

## 汇总
- 检查轮次: X
- 🔴 严重: X
- 🟡 警告: X
- ✅ 正常: X

## 问题清单
（逐个列出发现的问题）
```

## 注意
- "安徽驿达"是公司名不是服务区，忽略
- "全省月均206.4万"是对标基准，不是 SA 营收
- 只检查前 2 个场景（共约 11 轮）
