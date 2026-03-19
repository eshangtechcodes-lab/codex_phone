# Codex Phone — 项目上下文

## 项目概述
Codex Phone 是一个面向手机浏览器的 Codex 移动控制面板，用于在手机上与 Codex CLI 交互。

## 技术栈
- 后端：Node.js + Express + WebSocket（JSON-RPC 代理）
- 前端：原生 HTML/CSS/JS（PWA）
- 接口：OpenAI 兼容 REST API（/v1/chat/completions）
- Bot：Telegram Bot（node-telegram-bot-api）
- 端口：Web 3002 / Codex app-server WS 4002

## 代码规范
- 用中文回复和注释
- 代码风格简洁，避免过度设计
- 修改前先理解现有逻辑，做增量修改

## 关键文件
| 文件 | 职责 |
|------|------|
| server.js | Express 服务、WS 代理、REST API、Telegram Bot、/patrol 命令 |
| patrol.js | 独立版自动巡检脚本（Codex 自主判断 direct/multi 模式） |
| public/js/app.js | 前端 WebSocket 通信、线程管理、语音、PWA |
| public/index.html | 页面入口 |
| public/css/style.css | 移动端深色主题样式 |
| .patrol/ | 巡检临时文件目录（plan.json、log.txt、prompt.md） |

## 注意事项
- Telegram Token 通过 server.js 硬编码，生产环境应改为环境变量
- codex app-server 由 server.js 自动拉起，无需手动启动
- 前端新建会话时会读取本文件作为 Codex 的上下文
