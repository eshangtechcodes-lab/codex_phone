# Codex Phone

Codex Phone 是一个面向手机浏览器的 Codex 移动控制台。它通过 WebSocket 连接本地 `codex app-server`，把聊天、会话管理、模型切换和工具调用放到手机上使用，并提供 OpenAI 兼容接口。

## 功能列表

- 手机端聊天界面，适合触屏操作
- 自动启动并代理本地 `codex app-server`
- 会话管理：新建、恢复、切换历史会话
- 模型切换：可在页面顶部直接选择模型
- 语音输入：使用浏览器 Web Speech API
- OpenAI 兼容接口：`/v1/chat/completions` 和 `/v1/models`
- PWA 支持：可安装到主屏幕，并支持离线缓存
- Telegram Bot：支持 `/start`、`/codex`、`/gemini`、`/new`、`/model`、`/quota`

## 快速开始

### 环境要求

- Node.js 18+
- 已安装并登录 Codex CLI
- 手机和运行服务的电脑在同一局域网

### 安装依赖

```bash
npm install
```

### 启动服务

```bash
npm start
```

或：

```bash
npm run dev
```

默认端口：

- Web 服务：`http://localhost:3002`
- Codex WS：`ws://127.0.0.1:4002`

手机浏览器访问电脑局域网 IP，例如 `http://192.168.x.x:3002`。

## 使用说明

### Web 页面

- 点 `+ New` 新建会话
- 点 `历史` 查看并恢复最近会话
- 顶部下拉框切换模型
- 输入框回车发送，`Shift + Enter` 换行
- 点麦克风按钮启用语音输入
- 出现安装提示时，可将页面安装为 PWA

### OpenAI 兼容接口

可直接作为局域网内的 OpenAI 兼容服务使用：

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

print(response.choices[0].message.content)
```

### Telegram Bot

如果启用了 `server.js` 中的 Telegram Bot，可以直接在 Telegram 里发送命令：

- `/start` 查看命令
- `/codex` 切换到 Codex
- `/gemini` 切换到 Gemini
- `/new` 新建对话
- `/model` 查看或切换 Codex 模型
- `/quota` 查看额度

### 额外说明

- 新建会话时，前端会先读取 `CODEX.md` 作为上下文
- 如果要对外访问，建议通过局域网 IP、反向代理或隧道暴露 `3002` 端口

## 项目结构

```text
.
├── CODEX.md          # 新会话默认上下文
├── patrol.js         # 自动巡检/执行脚本
├── package.json
├── server.js         # Web 服务、WS 代理、OpenAI 兼容 API、Telegram Bot
├── public/           # 前端 PWA
│   ├── index.html
│   ├── css/style.css
│   ├── js/app.js
│   ├── sw.js
│   ├── manifest.json
│   └── icons/
└── README.md
```
