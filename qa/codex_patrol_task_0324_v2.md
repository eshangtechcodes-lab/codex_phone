# Codex 巡查任务 v2 — 2026-03-24（场景 2-4，含根因追踪 + 聚类）

你是 QA 巡查 AI。请对 `qa/reports/multi_multi_low_0323.json` 中的**场景 2、3、4**做 5 维度深度检查。

## 重要：你的检查必须有据可查

每一个判定都必须附上**具体依据**（SQL + 数字对比）。

## 新增要求：根因追踪 + 聚类分析

### 要求 1: 根因追踪

当发现 🔴 严重问题时，你**必须额外做根因分析**：

1. **查 `diagnostics.thinking_chain`** — 读 AI 的思路链，看它在推理过程中哪一步出了错
2. **查 `diagnostics.tool_calls_detail`** — 看工具返回了什么原始数据，是工具返回就错了，还是 AI 二次加工时弄错了
3. **查 `diagnostics.semantic_snapshot`** — 看 AI 识别到的实体、时间、意图是否正确
4. **给出根因分类**，从以下选项中选一个或多个：
   - `CACHE_STALE`: 用了缓存摘要，而不是实时查询
   - `ENTITY_MISMATCH`: 实体识别错误（比如把 A 服务区的数据当成 B 的）
   - `TOOL_WRONG`: 选错了工具（比如该用日度工具却用了月度工具）
   - `TOOL_RESULT_MANGLED`: 工具结果正确但 AI 加工时弄错了（单位换算、截取错行等）
   - `CONTEXT_POLLUTION`: 上下文污染，前一轮的数据串到了这一轮
   - `SCOPE_DRIFT`: 口径漂移（春节→春运、对客销售→营业收入等）
   - `UNKNOWN`: 从现有数据无法判断

**示例输出**：
```
- **根因**: `CONTEXT_POLLUTION` — thinking_chain 显示 AI 在第 3 步仍引用了上一轮的"新桥数据池"，
  导致龙门寺的营收被替换为新桥的数值。semantic_snapshot 中 entities=['龙门寺服务区'] 正确，
  但 tool_calls_detail 的 server_id=416（新桥）而非龙门寺的真实 server_id。
```

### 要求 2: 聚类分析

在报告末尾加一个**聚类汇总**章节，把所有 🔴 问题按根因归类：

```markdown
## 聚类分析

### CACHE_STALE（缓存陈旧）
- 场景 X 轮 Y: ...
- 场景 X 轮 Z: ...
影响范围: 片区级别查询
建议修复: 片区问题禁止走缓存，必须实时聚合

### TOOL_RESULT_MANGLED（工具结果加工错误）
- 场景 X 轮 Y: ...
影响范围: 单站查询
建议修复: 检查 quick_metric 路径的单位换算逻辑
```

## 检查步骤

### 1. 读取场景 2、3、4 的数据

```python
import json
with open('qa/reports/multi_multi_low_0323.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

for sc_idx in range(2, 5):  # 场景 2, 3, 4
    sc = data['scenarios'][sc_idx]
    print(f"\n=== 场景 {sc_idx}: {sc['name']} ===")
    for t in sc['turns']:
        print(f"  轮 {t['turn']}: {t['question']}")
```

### 2. 逐轮 5 维度检查（同 v1）

**维度 A: 幻觉检测** — 对比 full_response 和 tool_calls_detail
**维度 B: 数字准确性** — SQL 查 dameng_mirror.db 对比
**维度 C: 工具选择** — 工具类型是否匹配问题意图
**维度 D: 分类正确性** — classification_detail.type 是否正确
**维度 E: 回答质量** — 是否回答了问题、逻辑是否合理

### 3. 对每个 🔴 做根因追踪

```python
# 读取 thinking_chain
for t in sc['turns']:
    d = t.get('diagnostics', {})
    chain = d.get('thinking_chain', [])
    print(f"轮 {t['turn']} thinking_chain 长度: {len(chain)}")
    for step in chain:
        print(f"  {step}")  # 看每一步推理
    
    # 看 semantic_snapshot
    snap = d.get('semantic_snapshot', {})
    print(f"  entities: {snap.get('entities')}")
    print(f"  intent: {snap.get('intent')}")
    print(f"  scope: {snap.get('scope')}")
```

### 4. 输出报告

将检查结果写入 `qa/reports/patrol_0324_v2.md`，格式:

```markdown
# QA 巡查报告 v2 2026-03-24（场景 2-4，含根因追踪）

## 汇总
- 检查场景: 3 / 15（场景 2、3、4）
- 检查轮次: X
- 🔴 严重: X（含根因追踪）
- 🟡 警告: X
- ✅ 正常: X

## 场景 2: [名称]

### 轮 1: "问题"
- **维度 A-E**: （同 v1 格式）
- **根因**: `ROOT_CAUSE_CODE` — [具体分析，引用 thinking_chain 和 tool_calls_detail 的关键步骤]

## 场景 3: [名称]
（同上）

## 场景 4: [名称]
（同上）

## 聚类分析
### [ROOT_CAUSE_CODE]
- 涉及: 场景 X 轮 Y, 场景 X 轮 Z
- 影响范围: ...
- 根因证据: thinking_chain 第 N 步显示 ...
- 建议修复: ...

## 与 v1 报告的交叉对比
- v1 中场景 0-1 的问题是否与本次场景 2-4 有相同根因？
```

## SQL 查询方法（同 v1）

```python
import sqlite3
conn = sqlite3.connect(r'd:/AISpace/AI-Python/data/dameng_mirror.db')
cursor = conn.cursor()
cursor.execute("SELECT ...", params)
print(cursor.fetchall())
```

## 注意
- "安徽驿达"是公司名不是服务区，忽略
- 日度镜像上限 2026-03-18，超出标注"暂不可复核"
- 你必须真的执行 SQL 查询和读 thinking_chain，不能猜测
- 根因分析必须引用 thinking_chain/tool_calls_detail 的具体内容
