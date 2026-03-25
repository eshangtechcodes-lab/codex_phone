# Codex 巡查任务 — 2026-03-24（前 2 场景试跑）

你是 QA 巡查 AI。请对 `qa/reports/multi_multi_low_0323.json` 中的**前 2 个场景**做 5 维度深度检查。

## 重要：你的检查必须有据可查

每一个判定（通过/问题）都必须附上你的**具体依据**：
- 查了哪个 SQL、返回了什么数字
- 从 full_response 提取到了什么数字
- 对比之后偏差是多少

**禁止**只写"正常"或"没问题"而不给出具体检查过程。

## 你的检查步骤

### 1. 读取测试 JSON

```python
import json
with open('qa/reports/multi_multi_low_0323.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

# 只检查前 2 个场景
for sc_idx in range(2):
    sc = data['scenarios'][sc_idx]
    print(f"\n=== 场景 {sc_idx}: {sc['name']} ===")
    for t in sc['turns']:
        print(f"  轮 {t['turn']}: {t['question']}")
        print(f"    passed={t['passed']}, elapsed={t['elapsed']}")
```

### 2. 逐轮逐维度检查

对前 2 个场景的**每一轮**，执行以下 5 个维度的检查：

**维度 A: 幻觉检测**
- 读 `diagnostics.full_response` — 提取 AI 给出的具体数字
- 读 `diagnostics.tool_calls_detail` — 检查调了哪些工具
- 如果回答有具体营收数字但没调营收工具 → 🔴 幻觉
- **输出**: 列出 AI 提到的关键数字，以及支撑这些数字的工具名

**维度 B: 数字准确性**
- 从 `full_response` 提取 SA 名和营收数字
- 用 Python sqlite3 查 `d:/AISpace/AI-Python/data/dameng_mirror.db` 获取真实值:

```python
import sqlite3
conn = sqlite3.connect(r'd:/AISpace/AI-Python/data/dameng_mirror.db')
cursor = conn.cursor()
# 例如查新桥服务区 2 月营收
cursor.execute("""
    SELECT [服务区名称], SUM([对客销售])/10000.0 as wan
    FROM NEWGETREVENUEREPORT_SHOPS
    WHERE [服务区名称] = '新桥服务区' AND STATISTICS_MONTH = '202602'
    GROUP BY [服务区名称]
""")
print(cursor.fetchall())
```

- 对比 AI 数字和 DB 真实值，计算偏差百分比
- **输出**: `AI说 X 万，DB真实值 Y 万，偏差 Z%`

**维度 C: 工具选择合理性**
- 检查问营收→是否用了 revenue 工具，问车流→traffic 工具，等等
- **输出**: 列出期望工具和实际工具

**维度 D: 分类正确性**
- 读 `diagnostics.classification_detail.type`
- 数据查询应为 A，知识问答应为 B，实时信息应为 C
- **输出**: 期望分类 vs 实际分类

**维度 E: 回答质量**
- 读 full_response，判断是否回答了问题、逻辑是否合理
- **输出**: 简短评价

### 3. 输出报告

将检查结果写入 `qa/reports/patrol_0324_preview.md`，格式:

```markdown
# QA 巡查报告 2026-03-24（前 2 场景试跑）

## 汇总
- 检查场景: 2 / 15
- 检查轮次: X
- 🔴 严重: X
- 🟡 警告: X
- ✅ 正常: X

## 场景 0: 范围切换 SA→全省→片区→SA

### 轮 1: "新桥服务区2月营收"
- **维度 A 幻觉**: ✅/🔴 — [具体依据]
- **维度 B 数字**: ✅/🟡/🔴 — AI 说 X 万，DB 真实 Y 万，偏差 Z%
- **维度 C 工具**: ✅/🟡 — 期望 revenue，实际用了 [工具名]
- **维度 D 分类**: ✅/🟡 — 期望 A，实际 [分类]
- **维度 E 质量**: ✅/🟡/🔴 — [简评]

（后续轮次同理）

## 场景 1: 时间切换 月→日→趋势→节假日
（同上格式）

## 正常轮次汇总
（列出所有 ✅ 的轮次，附简要说明）
```

## 注意
- "安徽驿达"是公司名不是服务区，忽略
- "全省月均206.4万"是对标基准，不是 SA 营收
- 日度数据与月度不可混比
- dameng_mirror.db 日度数据可能不是最新的，超出镜像范围的标注"暂不可复核"
- 你必须真的执行 SQL 查询，不能猜测或编造 DB 数值
