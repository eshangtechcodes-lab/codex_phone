# Codex Phone

Codex Phone 是一个面向手机的 Codex 移动控制台。通过 WebSocket 代理本地 `codex app-server`，集成聊天、会话管理、模型切换、Telegram Bot、OpenAI 兼容接口和 QA 自动巡查系统。

## 功能列表

| 功能 | 说明 |
|------|------|
| 📱 手机聊天 | 深色主题 PWA，适配触屏操作 |
| 🤖 Telegram Bot | 通过 `@yskj02_bot` 聊天、切引擎、执行任务 |
| 🔌 OpenAI 兼容 | `/v1/chat/completions`，局域网当 OpenAI 用 |
| 🔮 双引擎 | Codex（GPT-5.4）+ Gemini（CLI 调用） |
| 🧠 记忆系统 | 自动提取对话要点，跨会话记忆 |
| 🔄 多账户 | Codex 账户热切换，不用重新登录 |
| 🔍 QA 自动巡查 | 出题→测试→核对→Codex巡查，三层检测体系 |

## 快速开始

### 环境要求

- Node.js 18+
- 已安装并登录 Codex CLI
- Python 3.8+（QA 模块）

### 安装与启动

```bash
npm install
npm start
```

默认端口：

| 服务 | 端口 | 说明 |
|------|------|------|
| Web | 3002 | 手机浏览器访问 |
| Codex WS | 4002 | app-server 内部通信 |

### 访问方式

- 本地: `http://localhost:3002`
- 局域网: `http://192.168.x.x:3002`
- 公网: `https://codex.eshangtech.com` (Nginx 反向代理)

## 部署信息

| 项目 | 值 |
|------|------|
| 服务器 | `124.220.229.187` (腾讯云) |
| 域名 | `codex.eshangtech.com` / `852727.xyz` |
| SSH | `root@codex.eshangtech.com` (密钥认证) |
| 协议 | HTTPS (Nginx + SSL) |

## Telegram Bot

Bot: `@yskj02_bot`，需要配置 `TG_TOKEN` 环境变量。

### 命令列表

| 命令 | 说明 |
|------|------|
| `/codex` | 切换到 Codex 引擎 |
| `/gemini` | 切换到 Gemini 引擎 |
| `/new` | 新建会话 |
| `/model` | 查看/切换模型 |
| `/quota` | 查看额度和账户 |
| `/memory` | 查看/清空记忆 |
| `/account` | 多账户管理（list/save/切换） |
| `/task 描述` | 后台执行 Codex 任务 |
| `/help` | 帮助信息 |

### 环境变量

```bash
# .env
TG_TOKEN=your_telegram_bot_token
TG_PROXY=http://proxy:port    # 可选，国内访问 Telegram 需要
```

## OpenAI 兼容接口

局域网内可直接当 OpenAI API 使用：

```python
from openai import OpenAI

client = OpenAI(
    base_url="http://YOUR_IP:3002/v1",
    api_key="not-needed",
)

response = client.chat.completions.create(
    model="gpt-5.4-mini",
    messages=[{"role": "user", "content": "hello"}],
)
```

## QA 自动巡查系统

独立的 QA 子项目，位于 `qa/` 目录，详见 [qa/README.md](qa/README.md)。

三层检测体系：

| 层级 | 速度 | 能力 |
|------|------|------|
| Layer 1 | 秒级 | Python 数字提取 + SQL 对比 |
| Layer 2 | 分钟级 | 3x Codex 并行幻觉检测 |
| Layer 3 | 全程 | Pipeline 一键编排 |

```bash
# 快速验证
python qa/qa_pipeline.py --auto-generate --skip-codex --limit-scenarios 2

# 全量 E2E
python qa/qa_pipeline.py --auto-generate --run-id e2e_01
```

## 项目结构

```text
codex_phone/
├── server.js            # 主服务：Express + WS代理 + Telegram Bot
├── patrol.js            # Codex 巡检脚本
├── CODEX.md             # 新会话上下文（Codex 读取）
├── system_prompt.md     # 系统人设提示词
├── .env                 # 环境变量（TG_TOKEN 等）
├── package.json
├── public/              # 前端 PWA
│   ├── index.html
│   ├── css/style.css
│   ├── js/app.js
│   ├── sw.js
│   ├── manifest.json
│   └── icons/
├── .patrol/             # 巡检临时文件
├── reports/             # 巡检报告输出
└── qa/                  # QA 自动巡查系统（独立子项目）
    ├── config.py            # 配置中心
    ├── qa_pipeline.py       # 一键入口
    ├── qa_runner.py         # 测试引擎
    ├── qa_question_gen.py   # 智能出题器
    ├── qa_auto_check.py     # Layer 1 数字核对
    ├── qa_codex_dispatch.py # Layer 2 Codex 巡查
    ├── qa_history.jsonl     # 历史日志
    ├── dameng_mirror_copy.db# 数据镜像
    └── reports/             # QA 报告
```
