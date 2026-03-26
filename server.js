/**
 * Codex Phone — 主入口
 *
 * 手机控制 Codex 的 Web 服务，集成：
 * - 静态文件服务（PWA）
 * - WebSocket 代理（手机 ↔ Codex app-server）
 * - OpenAI 兼容 REST API
 * - Telegram Bot（双引擎聊天 + QA 巡检 + 后台任务）
 * - 记忆系统（跨会话持久化）
 *
 * 架构：
 *   server.js（本文件）→ 组装各模块 → 启动服务
 *   src/codex.js    — Codex 进程管理 + WS 通信
 *   src/api.js      — REST API 路由
 *   src/memory.js   — 记忆系统
 *   src/gemini.js   — Gemini CLI 调用
 *   src/telegram/   — Telegram Bot（commands + qa + task）
 */

import 'dotenv/config';

import express from 'express';
import { createServer } from 'http';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

import { startCodexAppServer, setupWsProxy } from './src/codex.js';
import { setupApiRoutes } from './src/api.js';
import { setupTelegramBot } from './src/telegram/index.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// ================================================================
// 配置
// ================================================================
const WEB_PORT = 3002;

// ================================================================
// Express + HTTP Server
// ================================================================
const app = express();
app.use(express.json());
app.use(express.static(join(__dirname, 'public')));
const server = createServer(app);

// ================================================================
// 注册模块
// ================================================================

// 1. REST API 路由
setupApiRoutes(app);

// 2. WebSocket 代理（手机 ↔ Codex）
setupWsProxy(server);

// 3. 启动 Codex app-server 子进程
startCodexAppServer();

// 4. Telegram Bot
const bot = setupTelegramBot();

// ================================================================
// 启动 Web 服务（等待 Codex app-server 就绪）
// ================================================================
setTimeout(() => {
    server.listen(WEB_PORT, '0.0.0.0', () => {
        console.log(`\n🚀 Codex Phone running on http://localhost:${WEB_PORT}`);
        console.log(`📱 Remote: https://codex.852727.xyz\n`);
    });
}, 2000);

// ================================================================
// 优雅退出
// ================================================================
process.on('SIGINT', () => {
    console.log('\nShutting down...');
    if (bot) bot.stopPolling();
    process.exit(0);
});
