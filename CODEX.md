# Codex Phone 项目指引

## 项目信息
- 项目名称：Codex Phone（手机控制 Codex 的 Web 服务）
- 技术栈：Node.js + Express + WebSocket + 原生 HTML/CSS/JS
- 端口：Web 3002 / Codex WS 4002

## 用户偏好
- 请用中文回复
- 我叫小明，住在杭州，喜欢吃火锅
- 代码注释用中文

## 项目结构
- server.js — Express 服务 + WS 代理 + OpenAI 兼容 REST API
- public/ — 前端 PWA 聊天界面
- public/js/app.js — 核心前端逻辑
