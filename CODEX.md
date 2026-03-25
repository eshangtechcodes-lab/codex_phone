# Codex Phone — 项目上下文

## 项目概述
Codex Phone 是一个面向手机浏览器的 Codex 移动控制面板，用于在手机上与 Codex CLI 交互。

## 技术栈
- 后端：Node.js + Express + WebSocket（JSON-RPC 代理）
- 前端：原生 HTML/CSS/JS（PWA）
- 接口：OpenAI 兼容 REST API（/v1/chat/completions）
- Bot：Telegram Bot（node-telegram-bot-api）
- QA：Python 三层自动巡查系统
- 端口：Web 3002 / Codex app-server WS 4002

## 部署
- 服务器：124.220.229.187（codex.eshangtech.com）
- SSH：root@codex.eshangtech.com（密钥认证）
- Web：https://codex.eshangtech.com（Nginx 反代）

## 代码规范
- 用中文回复和注释
- 代码风格简洁，避免过度设计
- 修改前先理解现有逻辑，做增量修改

## 关键文件
| 文件 | 职责 |
|------|------|
| server.js | Express 服务、WS 代理、REST API、Telegram Bot（/codex /gemini /task /qa /memory /account） |
| patrol.js | 独立版自动巡检脚本（Codex 自主判断 direct/multi 模式） |
| system_prompt.md | AI 人设提示词 |
| .env | 环境变量（TG_TOKEN、TG_PROXY） |
| public/js/app.js | 前端 WebSocket 通信、线程管理、语音、PWA |
| public/index.html | 页面入口 |
| public/css/style.css | 移动端深色主题样式 |
| .patrol/ | 巡检临时文件目录（plan.json、log.txt、prompt.md） |
| qa/ | QA 自动巡查系统（独立子项目，见下方详细说明） |

## QA 自动巡查系统

### 架构
QA 系统是 Python 三层自动巡查流水线，通过 Telegram `/qa` 命令一键触发：

```
/qa → qa_pipeline.py → Step 0: 出题 → Step 1: 测试 → Step 2: 核对 → 推送结果
```

### 流水线步骤
| Step | 脚本 | 功能 |
|------|------|------|
| 0 | qa_question_gen.py | 读 DB + 历史日志，自动生成覆盖/回归/多轮/边界题目 |
| 1 | qa_runner.py | 把题目发到 llm.eshangtech.com API，记录全部响应 |
| 2 | qa_auto_check.py | 从 AI 回答提取数字，与 dameng_mirror_copy.db 对比，标记偏差 |
| 3 | qa_codex_dispatch.py | （可选）并行 Codex 巡查，做幻觉检测和质量评分 |

### QA 文件清单
| 文件 | 职责 |
|------|------|
| qa/config.py | 统一配置（DB 路径、API 地址、目录），自动适配 Windows/Linux |
| qa/qa_pipeline.py | Pipeline 一键入口，串联 Step 0-3 |
| qa/qa_runner.py | 测试执行器，含 43 题 Golden Set 和 10 个多轮场景 |
| qa/qa_question_gen.py | 智能出题器（rules 规则引擎 / codex 智能出题两种模式） |
| qa/qa_auto_check.py | Layer 1 数字核对（营收/同比/环比/排名/车流/入区率） |
| qa/qa_codex_dispatch.py | Layer 2 Codex 并行巡查调度器 |
| qa/qa_verifier.py | 验证和对比工具 |
| qa/qa_diff.py | 两次巡检结果 diff 对比 |
| qa/dameng_mirror_copy.db | 达梦数据库镜像（219MB，用于 SQL 核对） |
| qa/qa_history.jsonl | 历史巡检日志（出题覆盖率追踪） |
| qa/reports/ | 所有报告输出目录 |

### 手动运行 QA
```bash
# 完整流水线（出题+测试+核对，跳过 Codex 巡查）
python3 qa/qa_pipeline.py --auto-generate --skip-codex

# 仅跑 Golden Set 43 题
python3 qa/qa_runner.py --run-id golden_01

# 多轮语义切换测试
python3 qa/qa_runner.py --multi-turn --run-id multi_01

# 仅出题不测试
python3 qa/qa_question_gen.py --mode rules

# 核对已有报告
python3 qa/qa_auto_check.py --input reports/xxx.json
```

### Telegram /qa 命令
- `/qa` — 没在跑：启动完整巡检；正在跑：显示实时进度
- `/qa stop` — 终止巡检
- 进度包含：运行时间、当前步骤、题目进度、通过/失败数

### 查看巡检进度
1. **Telegram**: 发 `/qa` 即可看实时进度
2. **命令行**: 查看 PM2 日志 `pm2 logs codex-phone --lines 50`
3. **报告文件**: `qa/reports/` 目录下的 `.md` 和 `.json` 文件

## 注意事项
- Telegram Token 通过 .env 文件配置（TG_TOKEN）
- codex app-server 由 server.js 自动拉起，无需手动启动
- 前端新建会话时会读取本文件作为 Codex 的上下文
- QA 子项目的配置集中在 qa/config.py，自动适配 Windows/Linux
- QA 测试对象是 llm.eshangtech.com（正式 API），不是本地服务
- dameng_mirror_copy.db 是只读镜像，用于 SQL 核对真实值

## 故障排查手册

遇到问题时，按以下步骤排查：

### 1. 查日志（第一步永远是看日志）
```bash
pm2 logs codex-phone --lines 50 --nostream
```

### 2. Codex app-server 端口冲突（Address in use 4002）
**症状**: 日志出现 `Error: Address in use (os error 98)`，聊天无响应
```bash
# 杀掉残留进程 → 释放端口 → 重启
pm2 stop codex-phone
pkill -f "codex app-server"
fuser -k 4002/tcp
sleep 2
pm2 start codex-phone
pm2 save
```

### 3. Telegram Bot 不回消息
**排查顺序**:
1. 看日志是否有 `Polling error` → 代理问题，Bot 会自动重连（10s 间隔）
2. 看日志是否有 `[TG/Codex]` 开头 → 收到消息了，在等 Codex 回复
3. 看是否有端口冲突 → 按上面第 2 步修
4. 代理彻底不通 → `curl -s --proxy http://127.0.0.1:10809 https://api.telegram.org/bot$TG_TOKEN/getMe`

### 4. QA 巡检卡住或失败
```bash
# 检查 QA 是否在跑
ps aux | grep qa_pipeline
# 看 QA 报告目录
ls -lt qa/reports/ | head -10
# 手动跑测试看报错
cd /home/ubuntu/codex_phone/qa && python3 qa_pipeline.py --auto-generate --skip-codex --limit-scenarios 1
```

### 5. 健康检查
```bash
curl -s http://localhost:3002/health        # Web 服务
curl -s http://localhost:4002/readyz        # Codex app-server
pm2 status                                  # PM2 进程状态
```

### 6. 完整重启（核弹选项）
```bash
pm2 stop codex-phone
pkill -f codex
sleep 3
cd /home/ubuntu/codex_phone && pm2 start server.js --name codex-phone --update-env
pm2 save
```
