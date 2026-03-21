// Codex Phone - 手机控制 Codex 的 Web 服务
// 功能：静态文件服务 + WebSocket 代理到 Codex app-server

import 'dotenv/config';

import express from 'express';
import { createServer } from 'http';
import { WebSocket, WebSocketServer } from 'ws';
import { spawn } from 'child_process';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'fs';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// ================================================================
// Memory System
// ================================================================
const MEMORY_DIR = join(process.env.USERPROFILE || process.env.HOME, '.codex_phone', 'memory');
const MEMORY_CATEGORIES = ['profile', 'projects', 'servers', 'skills', 'notes'];
if (!existsSync(MEMORY_DIR)) { mkdirSync(MEMORY_DIR, { recursive: true }); }

const chatHistory = new Map();
const MAX_HISTORY = 20;

function addHistory(userId, role, content) {
    if (!chatHistory.has(userId)) chatHistory.set(userId, []);
    const h = chatHistory.get(userId);
    h.push({ role, content: content.substring(0, 500) });
    if (h.length > MAX_HISTORY) h.shift();
}

function loadMemory() {
    let memory = '';
    for (const cat of MEMORY_CATEGORIES) {
        const f = join(MEMORY_DIR, `${cat}.md`);
        if (existsSync(f)) {
            const content = readFileSync(f, 'utf-8').trim();
            if (content) memory += `\n### ${cat}\n${content}\n`;
        }
    }
    return memory;
}

const MEMORY_TRIGGERS = ['\u603b\u7ed3\u4e00\u4e0b', '\u8bb0\u4e0b\u6765', '\u8bb0\u4f4f\u8fd9\u4e9b', '\u4fdd\u5b58\u4e00\u4e0b', '\u8bb0\u5f55\u4e00\u4e0b', '\u603b\u7ed3\u5bf9\u8bdd', '\u5e2e\u6211\u8bb0\u4f4f', 'save this', 'remember this'];
function isMemoryTrigger(text) {
    return MEMORY_TRIGGERS.some(t => text.includes(t));
}

async function extractAndSave(userId) {
    const history = chatHistory.get(userId);
    if (!history || history.length < 2) return '\u6ca1\u6709\u8db3\u591f\u7684\u5bf9\u8bdd\u53ef\u4ee5\u603b\u7ed3\u3002';
    const conversation = history.map(h => `${h.role}: ${h.content}`).join('\n');
    const prompt = `\u4f60\u662f\u4fe1\u606f\u63d0\u53d6\u52a9\u624b\u3002\u4ece\u4ee5\u4e0b\u5bf9\u8bdd\u4e2d\u63d0\u53d6\u6709\u4ef7\u503c\u7684\u4fe1\u606f\uff0c\u6309JSON\u683c\u5f0f\u8f93\u51fa\u3002\n\u7c7b\u522b: profile(\u4e2a\u4eba\u4fe1\u606f), projects(\u9879\u76ee), servers(\u670d\u52a1\u5668), skills(\u6280\u80fd\u6d41\u7a0b), notes(\u5176\u4ed6)\n\u53ea\u8f93\u51fa\u6709\u65b0\u4fe1\u606f\u7684\u7c7b\u522b\u3002\u4e25\u683c\u8f93\u51faJSON\uff0c\u65e0\u5176\u4ed6\u6587\u5b57:\n{"profile": "...", "servers": "..."}\n\n\u5bf9\u8bdd:\n${conversation}`;
    try {
        const result = await geminiChat(prompt);
        const jsonMatch = result.match(/\{[\s\S]*\}/);
        if (!jsonMatch) return '\u672a\u63d0\u53d6\u5230\u6709\u4ef7\u503c\u7684\u4fe1\u606f\u3002';
        const extracted = JSON.parse(jsonMatch[0]);
        let saved = [];
        for (const [cat, content] of Object.entries(extracted)) {
            if (!MEMORY_CATEGORIES.includes(cat) || !content || content === '\u65e0') continue;
            const f = join(MEMORY_DIR, `${cat}.md`);
            const existing = existsSync(f) ? readFileSync(f, 'utf-8') : '';
            const ts = new Date().toLocaleString('zh-CN');
            writeFileSync(f, existing + `\n- [${ts}] ${content}`, 'utf-8');
            saved.push(cat);
        }
        if (saved.length === 0) return '\u8fd9\u6bb5\u5bf9\u8bdd\u6ca1\u6709\u9700\u8981\u8bb0\u5fc6\u7684\u65b0\u4fe1\u606f\u3002';
        return `\u2705 \u5df2\u8bb0\u5fc6\uff01\u4fdd\u5b58\u5230: ${saved.map(s => `*${s}*`).join(', ')}`;
    } catch (e) {
        console.log('[Memory] \u63d0\u53d6\u5931\u8d25:', e.message);
        return '\u274c \u8bb0\u5fc6\u63d0\u53d6\u5931\u8d25: ' + e.message;
    }
}

// 配置
const WEB_PORT = 3002;          // Web 服务端口
const CODEX_WS_PORT = 4002;    // Codex app-server WebSocket 端口
const CODEX_WS_URL = `ws://127.0.0.1:${CODEX_WS_PORT}`;

// 启动 Express
const app = express();
app.use(express.static(join(__dirname, 'public')));
const server = createServer(app);

// --- 启动 Codex app-server 子进程 ---
let codexProcess = null;

function startCodexAppServer() {
    console.log(`[CODEX] Starting app-server on ws://127.0.0.1:${CODEX_WS_PORT}...`);
    codexProcess = spawn('codex', ['app-server', '--listen', `ws://127.0.0.1:${CODEX_WS_PORT}`], {
        stdio: ['ignore', 'pipe', 'pipe'],
        shell: true
    });

    codexProcess.stdout.on('data', (data) => {
        console.log(`[CODEX stdout] ${data.toString().trim()}`);
    });

    codexProcess.stderr.on('data', (data) => {
        console.log(`[CODEX stderr] ${data.toString().trim()}`);
    });

    codexProcess.on('close', (code) => {
        console.log(`[CODEX] Process exited with code ${code}`);
        // 自动重启
        if (code !== null) {
            console.log('[CODEX] Restarting in 3s...');
            setTimeout(startCodexAppServer, 3000);
        }
    });

    codexProcess.on('error', (err) => {
        console.error('[CODEX] Failed to start:', err.message);
    });
}

// --- WebSocket 代理：手机 ↔ Codex app-server ---
const wss = new WebSocketServer({ server });

wss.on('connection', (clientWs, req) => {
    console.log(`[WS] Phone connected from ${req.socket.remoteAddress}`);

    // 连接到 Codex app-server
    let codexWs = null;
    let codexReady = false;
    let pendingMessages = [];

    function connectToCodex() {
        codexWs = new WebSocket(CODEX_WS_URL);

        codexWs.on('open', () => {
            console.log('[WS] Connected to Codex app-server');
            codexReady = true;
            // 发送缓存的消息
            pendingMessages.forEach(m => {
                console.log('[WS] Flushing pending →', m.substring(0, 100));
                codexWs.send(m);
            });
            pendingMessages = [];
        });

        codexWs.on('message', (data) => {
            const msg = data.toString();
            console.log('[WS] Codex →', msg.substring(0, 200));
            // 直接转发到手机
            if (clientWs.readyState === WebSocket.OPEN) {
                clientWs.send(msg);
            }
        });

        codexWs.on('close', () => {
            console.log('[WS] Codex connection closed');
            codexReady = false;
        });

        codexWs.on('error', (err) => {
            console.error('[WS] Codex connection error:', err.message);
        });
    }

    connectToCodex();

    // 手机发来的消息 → 转发到 Codex
    clientWs.on('message', (data) => {
        const msg = data.toString();
        console.log('[WS] Phone →', msg.substring(0, 200));

        if (codexWs && codexReady && codexWs.readyState === WebSocket.OPEN) {
            codexWs.send(msg);
        } else {
            console.log('[WS] Codex not ready, buffering message');
            pendingMessages.push(msg);
        }
    });

    clientWs.on('close', () => {
        console.log('[WS] Phone disconnected');
        if (codexWs) codexWs.close();
    });
});

// ================================================================
// OpenAI 兼容 REST API - 局域网其他服务可直接当 OpenAI 用
// POST /v1/chat/completions  （标准 OpenAI 格式）
// GET  /v1/models            （可用模型列表）
// ================================================================
app.use(express.json());

// OpenAI 兼容：chat completions
app.post('/v1/chat/completions', async (req, res) => {
    const { model = 'gpt-5.4', messages = [] } = req.body;

    // 从 messages 中提取最后一条用户消息
    const userMsg = messages.filter(m => m.role === 'user').pop();
    if (!userMsg) {
        return res.status(400).json({
            error: { message: 'At least one user message is required', type: 'invalid_request_error' }
        });
    }

    // 把 system + 历史合成完整 prompt
    const systemMsg = messages.find(m => m.role === 'system');
    let prompt = '';
    if (systemMsg) prompt += `[System: ${systemMsg.content}]\n\n`;

    // 拼接多轮对话上下文
    const history = messages.filter(m => m.role !== 'system');
    if (history.length > 1) {
        // 多轮对话：把前面的都拼进来
        history.slice(0, -1).forEach(m => {
            prompt += `${m.role === 'user' ? 'User' : 'Assistant'}: ${m.content}\n`;
        });
        prompt += '\n';
    }
    prompt += userMsg.content;

    console.log(`[API] /v1/chat/completions model=${model} msg="${userMsg.content.substring(0, 50)}"`);

    try {
        const result = await codexChat(prompt, model);
        const responseId = `chatcmpl-${Date.now()}`;
        res.json({
            id: responseId,
            object: 'chat.completion',
            created: Math.floor(Date.now() / 1000),
            model,
            choices: [{
                index: 0,
                message: { role: 'assistant', content: result.reply },
                finish_reason: 'stop'
            }],
            usage: { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 },
            // 额外字段：方便追踪
            _codex: { threadId: result.threadId }
        });
    } catch (err) {
        console.error('[API] Error:', err.message);
        res.status(500).json({
            error: { message: err.message, type: 'server_error' }
        });
    }
});

// OpenAI 兼容：模型列表
app.get('/v1/models', async (req, res) => {
    const models = [
        { id: 'gpt-5.4', owned_by: 'openai' },
        { id: 'gpt-5.4-mini', owned_by: 'openai' },
        { id: 'gpt-5.3-codex', owned_by: 'openai' },
        { id: 'gpt-5.2-codex', owned_by: 'openai' },
        { id: 'gpt-5.2', owned_by: 'openai' },
        { id: 'gpt-5.1-codex-max', owned_by: 'openai' },
        { id: 'gpt-5.1-codex-mini', owned_by: 'openai' },
    ];
    res.json({
        object: 'list',
        data: models.map(m => ({ ...m, object: 'model', created: 0 }))
    });
});

// 简化版接口（也保留）
app.post('/api/chat', async (req, res) => {
    const { message, model = 'gpt-5.4', threadId: existingThreadId } = req.body;
    if (!message) return res.status(400).json({ error: 'message is required' });
    console.log(`[API] /api/chat "${message.substring(0, 50)}" model=${model}`);
    try {
        const result = await codexChat(message, model, existingThreadId);
        res.json(result);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// 内部函数：通过 WebSocket 与 Codex 完成一轮对话
function codexChat(message, model, existingThreadId) {
    return new Promise((resolve, reject) => {
        const ws = new WebSocket(CODEX_WS_URL);
        let rpcId = 1;
        let threadId = existingThreadId;
        let responseText = '';
        const timeout = setTimeout(() => {
            ws.close();
            reject(new Error('Timeout: Codex did not respond in 120s'));
        }, 120000);

        function send(method, params) {
            const id = rpcId++;
            ws.send(JSON.stringify({ jsonrpc: '2.0', id, method, params }));
            return id;
        }

        ws.on('open', () => {
            // Step 1: Initialize
            send('initialize', {
                clientVersion: '1.0.0',
                protocolVersion: '2.0',
                clientInfo: { name: 'codex-api', version: '1.0.0' },
                capabilities: {}
            });
        });

        let initDone = false;
        let threadStarted = false;

        ws.on('message', (data) => {
            const msg = JSON.parse(data.toString());

            // Initialize 响应
            if (msg.id && !initDone && msg.result) {
                initDone = true;
                if (threadId) {
                    // 恢复已有会话
                    send('thread/resume', { threadId });
                    threadStarted = true;
                    // 发消息
                    send('turn/start', {
                        threadId,
                        input: [{ type: 'text', text: message }]
                    });
                } else {
                    // 新建会话
                    send('thread/start', { model });
                }
                return;
            }

            // thread/start 响应
            if (msg.id && msg.result && !threadStarted) {
                threadId = msg.result.thread?.id || msg.result.threadId;
                threadStarted = true;
                // 发消息
                send('turn/start', {
                    threadId,
                    input: [{ type: 'text', text: message }]
                });
                return;
            }

            // 通知：流式文字
            if (msg.method === 'item/agentMessage/delta' && msg.params?.delta) {
                responseText += msg.params.delta;
            }

            // 通知：turn 完成
            if (msg.method === 'turn/completed') {
                clearTimeout(timeout);
                ws.close();
                resolve({
                    reply: responseText,
                    threadId,
                    model
                });
            }

            // 服务端请求（命令审批等）→ 自动批准
            if (msg.id !== undefined && msg.method) {
                ws.send(JSON.stringify({
                    jsonrpc: '2.0',
                    id: msg.id,
                    result: { approved: true }
                }));
            }
        });

        ws.on('error', (err) => {
            clearTimeout(timeout);
            reject(err);
        });
    });
}

// 健康检查
app.get('/health', (req, res) => {
    res.json({ status: 'ok', codex: codexProcess && !codexProcess.killed });
});

// 启动
startCodexAppServer();

// 等 Codex app-server 启动后再开 Web 服务
setTimeout(() => {
    server.listen(WEB_PORT, '0.0.0.0', () => {
        console.log(`\n🚀 Codex Phone running on http://localhost:${WEB_PORT}`);
        console.log(`📱 Remote: https://codex.852727.xyz (需配置 Tunnel)\n`);
    });
}, 2000);

// ================================================================
// Telegram Bot - 通过 Telegram 聊天使用 Codex
// ================================================================
import TelegramBot from 'node-telegram-bot-api';

const TG_TOKEN = process.env.TG_TOKEN;
if (!TG_TOKEN) { console.warn('[TG] ⚠️ TG_TOKEN 未配置，Telegram Bot 已禁用'); }
let tgModel = 'gpt-5.4-mini'; // Codex 默认模型
let tgGeminiModel = 'gemini-2.5-pro'; // Gemini 默认模型
const tgThreads = new Map();  // userId -> threadId
const tgEngine = new Map();   // userId -> 'codex' | 'gemini'

// 代理配置（国内服务器需要代理访问 Telegram API）
const botOptions = { polling: true };
if (process.env.TG_PROXY) {
    botOptions.request = { proxy: process.env.TG_PROXY };
    console.log(`[TG] 使用代理: ${process.env.TG_PROXY}`);
}

const bot = new TelegramBot(TG_TOKEN, botOptions);

bot.on('polling_error', (err) => console.log('[TG] Polling error:', err.message));

// 注册命令菜单（输入 / 时显示快捷命令）
bot.setMyCommands([
    { command: 'help', description: '📖 帮助信息' },
    { command: 'codex', description: '🤖 切换到 Codex 引擎' },
    { command: 'gemini', description: '🔮 切换到 Gemini 引擎' },
    { command: 'model', description: '🔄 切换模型' },
    { command: 'new', description: '✨ 新建会话' },
    { command: 'quota', description: '📊 查看额度' },
    { command: 'memory', description: '🧠 查看记忆' },
    { command: 'task', description: '🔧 后台执行任务' },
]).then(() => console.log('[TG] 命令菜单已注册')).catch(() => {});

// /start 和 /help 命令
const helpText = (engine) =>
    '🤖 *AI Bot* 已上线！\n\n' +
    `当前引擎: *${engine.toUpperCase()}*\n\n` +
    '💬 *聊天*\n' +
    '/codex — 切到 Codex（能执行代码）\n' +
    '/gemini — 切到 Gemini（多模态）\n' +
    '/new — 新建会话\n' +
    '/model — 切换模型\n' +
    '/quota — 查看额度\n\n' +
    '🔧 *任务*\n' +
    '`/task 任务描述` — 后台执行任务\n' +
    '`/task status` — 查看任务状态\n' +
    '`/task stop` — 停止任务\n\n' +
    '/help — 显示本帮助';

bot.onText(/\/start/, (msg) => {
    const engine = tgEngine.get(msg.from.id) || 'codex';
    bot.sendMessage(msg.chat.id, helpText(engine), { parse_mode: 'Markdown' });
});

bot.onText(/\/help/, (msg) => {
    const engine = tgEngine.get(msg.from.id) || 'codex';
    bot.sendMessage(msg.chat.id, helpText(engine), { parse_mode: 'Markdown' });
});


// /codex 切换引擎
bot.onText(/\/codex/, (msg) => {
    tgEngine.set(msg.from.id, 'codex');
    bot.sendMessage(msg.chat.id, '🟢 已切换到 *Codex* 引擎（GPT-5.4，能执行代码）', { parse_mode: 'Markdown' });
});

// /gemini 切换引擎
bot.onText(/\/gemini/, (msg) => {
    tgEngine.set(msg.from.id, 'gemini');
    bot.sendMessage(msg.chat.id, '🔵 已切换到 *Gemini* 引擎（Gemini 3 Flash，多模态）', { parse_mode: 'Markdown' });
});

// /new 新建会话
bot.onText(/\/new/, (msg) => {
    tgThreads.delete(msg.from.id);
    bot.sendMessage(msg.chat.id, '✅ 已开始新会话');
});

// /model 切换模型（自动识别当前引擎）
bot.onText(/\/model\s*(.*)/, (msg, match) => {
    const engine = tgEngine.get(msg.from.id) || 'codex';
    const input = (match[1] || '').trim();

    if (engine === 'gemini') {
        const geminiModels = ['gemini-2.5-pro', 'gemini-2.5-flash', 'gemini-2.0-flash', 'gemini-3.1-pro-preview'];
        if (input && geminiModels.includes(input)) {
            tgGeminiModel = input;
            bot.sendMessage(msg.chat.id, `✅ Gemini 模型切换为: *${tgGeminiModel}*`, { parse_mode: 'Markdown' });
        } else {
            bot.sendMessage(msg.chat.id,
                `🔮 当前 Gemini 模型: *${tgGeminiModel}*\n\n可选：\n${geminiModels.map(m => `\`/model ${m}\``).join('\n')}`,
                { parse_mode: 'Markdown' }
            );
        }
    } else {
        const codexModels = ['gpt-5.4', 'gpt-5.4-mini', 'gpt-5.3-codex', 'gpt-5.2-codex', 'gpt-5.2', 'gpt-5.1-codex-max', 'gpt-5.1-codex-mini'];
        if (input && codexModels.includes(input)) {
            tgModel = input;
            bot.sendMessage(msg.chat.id, `✅ Codex 模型切换为: *${tgModel}*`, { parse_mode: 'Markdown' });
        } else {
            bot.sendMessage(msg.chat.id,
                `🤖 当前 Codex 模型: *${tgModel}*\n\n可选：\n${codexModels.map(m => `\`/model ${m}\``).join('\n')}`,
                { parse_mode: 'Markdown' }
            );
        }
    }
});

// /quota 查看额度（含账户信息）
bot.onText(/\/quota/, async (msg) => {
    try {
        // 获取额度
        const result = await quickRpc('account/rateLimits/read', {});
        const limits = result.rateLimits || {};
        const p = limits.primary || {};
        const s = limits.secondary || {};
        const resetMin = Math.max(0, Math.round((p.resetsAt * 1000 - Date.now()) / 60000));
        const resetH = Math.floor(resetMin / 60);
        const resetM = resetMin % 60;

        // 读取账户信息
        let email = 'unknown';
        let lastRefresh = '';
        try {
            const authFile = join(process.env.USERPROFILE || process.env.HOME, '.codex', 'auth.json');
            const { default: fs } = await import('fs');
            const auth = JSON.parse(fs.readFileSync(authFile, 'utf-8'));
            lastRefresh = auth.last_refresh ? new Date(auth.last_refresh).toLocaleString('zh-CN') : '';
            // 从 id_token 的 payload 解码邮箱
            const payload = JSON.parse(Buffer.from(auth.tokens?.id_token?.split('.')[1] || '', 'base64').toString());
            email = payload.email || 'unknown';
        } catch {}

        const pRemain = Math.max(0, 100 - (p.usedPercent || 0));
        const sRemain = Math.max(0, 100 - (s.usedPercent || 0));

        bot.sendMessage(msg.chat.id,
            `📊 *额度信息*\n\n` +
            `👤 账户: \`${email}\`\n` +
            `📋 Plan: *${limits.planType || 'unknown'}*\n\n` +
            `⏱ 5h: 剩余 *${pRemain}%* ↻${resetH}h${resetM}m\n` +
            `📅 Week: 剩余 *${sRemain}%*\n` +
            (lastRefresh ? `\n🔄 上次刷新: ${lastRefresh}` : ''),
            { parse_mode: 'Markdown' }
        );
    } catch (e) {
        bot.sendMessage(msg.chat.id, '❌ 查询额度失败: ' + e.message);
    }
});

// --- Gemini 调用（通过 CLI） ---
function geminiChat(message) {
    return new Promise((resolve, reject) => {
        const proc = spawn('gemini', ['-p', message, '--model', tgGeminiModel, '--output-format', 'json'], {
            shell: true,
            timeout: 120000
        });
        let stdout = '';
        let stderr = '';
        proc.stdout.on('data', d => { stdout += d.toString(); });
        proc.stderr.on('data', d => { stderr += d.toString(); });
        proc.on('close', (code) => {
            if (code !== 0) {
                return reject(new Error(stderr || `Gemini exited with code ${code}`));
            }
            try {
                const data = JSON.parse(stdout);
                resolve(data.response || '(empty)');
            } catch {
                // JSON 解析失败，直接返回原始输出
                resolve(stdout.trim() || '(empty)');
            }
        });
        proc.on('error', reject);
    });
}

// /memory 查看和管理记忆
bot.onText(/\/memory\s*(.*)/, async (msg, match) => {
    const arg = (match[1] || '').trim();
    if (arg === 'clear') {
        for (const cat of MEMORY_CATEGORIES) {
            const f = join(MEMORY_DIR, `${cat}.md`);
            if (existsSync(f)) writeFileSync(f, '', 'utf-8');
        }
        bot.sendMessage(msg.chat.id, '🗑️ 所有记忆已清空');
        return;
    }
    const memory = loadMemory();
    if (!memory.trim()) {
        bot.sendMessage(msg.chat.id, '🧠 记忆为空\n\n聊天后说“总结一下”或“记下来”即可保存记忆。');
    } else {
        bot.sendMessage(msg.chat.id, `🧠 *当前记忆*\n${memory}\n\n_发 /memory clear 清空_`, { parse_mode: 'Markdown' })
            .catch(() => bot.sendMessage(msg.chat.id, `🧠 当前记忆\n${memory}\n\n发 /memory clear 清空`));
    }
});

// 图片/语音/文件提示
// /task — 后台执行任务（不阻塞聊天）
let taskProc = null;
bot.onText(/\/task\s*(.*)/, async (msg, match) => {
    const task = match[1]?.trim();
    const chatId = msg.chat.id;

    if (!task || task === 'help') {
        bot.sendMessage(chatId,
            '🤖 *Task — 后台任务执行*\n\n' +
            '用法:\n' +
            '`/task 完善README`\n' +
            '`/task 给项目加.env.example`\n' +
            '`/task stop` — 停止当前任务\n' +
            '`/task status` — 查看状态',
            { parse_mode: 'Markdown' });
        return;
    }

    if (task === 'stop') {
        if (taskProc) { taskProc.kill(); taskProc = null; bot.sendMessage(chatId, '🛑 已停止'); }
        else bot.sendMessage(chatId, 'ℹ️ 没有正在执行的任务');
        return;
    }

    if (task === 'status') {
        bot.sendMessage(chatId, taskProc ? '⏳ 正在执行任务...' : '✅ 空闲');
        return;
    }

    if (taskProc) {
        bot.sendMessage(chatId, '⚠️ 有任务正在执行中，先 `/task stop` 再下新任务', { parse_mode: 'Markdown' });
        return;
    }

    console.log(`[Task] 新任务: "${task}"`);
    bot.sendMessage(chatId, `🚀 *Task 启动*\n📋 ${task}\n\n_后台执行中，你可以继续聊天..._`, { parse_mode: 'Markdown' });

    // 异步执行 codex exec（直接传 prompt，不通过文件）
    taskProc = spawn('codex', ['exec', '-m', 'gpt-5.4-mini', task], {
        cwd: __dirname, shell: true, timeout: 180000
    });

    let output = '';
    taskProc.stdout?.on('data', d => { output += d.toString(); });
    taskProc.stderr?.on('data', d => { output += d.toString(); });

    taskProc.on('error', (err) => {
        console.log('[Task] spawn 错误:', err.message);
        taskProc = null;
        bot.sendMessage(chatId, `❌ Task 执行失败: ${err.message}`);
    });

    taskProc.on('close', (code) => {
        taskProc = null;
        console.log(`[Task] 完成 (code=${code}, output=${output.length} chars)`);

        const status = code === 0 ? '✅' : '⚠️';
        const summary = output.length > 400 ? '...' + output.substring(output.length - 400) : output;
        bot.sendMessage(chatId, `${status} Task 完成\n📋 ${task}\n\n${summary.substring(0, 600)}`)
            .catch(e => console.log('[Task] 发送失败:', e.message));
    });
});

// 不支持的消息类型
bot.on('photo', (msg) => {
    bot.sendMessage(msg.chat.id, '📷 暂不支持图片识别（Gemini CLI 不支持传图）。\n请用文字描述图片内容，我来帮你分析！');
});

bot.on('voice', (msg) => {
    bot.sendMessage(msg.chat.id, '🎤 暂不支持语音识别，请发文字消息。');
});

// 普通消息 → 按引擎路由
bot.on('message', async (msg) => {
    if (!msg.text || msg.text.startsWith('/')) return;

    const userId = msg.from.id;
    const engine = tgEngine.get(userId) || 'codex';

    // 记忆触发检测
    if (isMemoryTrigger(msg.text)) {
        bot.sendChatAction(msg.chat.id, 'typing');
        console.log(`[Memory] 用户触发记忆提取`);
        const result = await extractAndSave(userId);
        bot.sendMessage(msg.chat.id, result, { parse_mode: 'Markdown' })
            .catch(() => bot.sendMessage(msg.chat.id, result));
        return;
    }

    // 记录对话历史
    addHistory(userId, '用户', msg.text);
    bot.sendChatAction(msg.chat.id, 'typing');

    // 加载记忆作为上下文
    const memory = loadMemory();
    const memCtx = memory.trim() ? `[以下是你记住的关于用户的信息，请参考但不要主动提起]\n${memory}\n\n` : '';

    try {
        let reply;
        if (engine === 'gemini') {
            console.log(`[TG/Gemini] "${msg.text.substring(0, 50)}"`);
            reply = await geminiChat(memCtx + msg.text);
        } else {
            const threadId = tgThreads.get(userId) || null;
            console.log(`[TG/Codex] "${msg.text.substring(0, 50)}" model=${tgModel}`);
            // 新会话才注入记忆，已有会话不重复注入
            const text = threadId ? msg.text : memCtx + msg.text;
            const result = await codexChat(text, tgModel, threadId);
            if (result.threadId) tgThreads.set(userId, result.threadId);
            reply = result.reply;
        }
        // 记录 AI 回复
        addHistory(userId, 'AI', reply?.substring(0, 500) || '');
        bot.sendMessage(msg.chat.id, reply || '(empty reply)', { parse_mode: 'Markdown' })
            .catch(() => bot.sendMessage(msg.chat.id, reply || '(empty reply)'));
    } catch (e) {
        bot.sendMessage(msg.chat.id, '❌ Error: ' + e.message);
    }
});

// 辅助：快速 RPC 查询（不走完整对话流程）
function quickRpc(method, params) {
    return new Promise((resolve, reject) => {
        const ws = new WebSocket(CODEX_WS_URL);
        let id = 1;
        ws.on('open', () => {
            ws.send(JSON.stringify({ jsonrpc: '2.0', id: id++, method: 'initialize',
                params: { clientVersion: '1.0.0', protocolVersion: '2.0',
                    clientInfo: { name: 'tg-bot', version: '1.0.0' }, capabilities: {} } }));
        });
        ws.on('message', d => {
            const m = JSON.parse(d.toString());
            if (m.id === 1 && m.result) {
                ws.send(JSON.stringify({ jsonrpc: '2.0', id: id++, method, params }));
            }
            if (m.id === 2) {
                ws.close();
                resolve(m.result);
            }
        });
        ws.on('error', e => { reject(e); });
        setTimeout(() => { ws.close(); reject(new Error('timeout')); }, 5000);
    });
}

console.log('[TG] Bot @yskj02_bot started');

// 优雅退出
process.on('SIGINT', () => {
    console.log('\nShutting down...');
    if (codexProcess) codexProcess.kill();
    bot.stopPolling();
    process.exit(0);
});
