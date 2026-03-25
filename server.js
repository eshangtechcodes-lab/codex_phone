// Codex Phone - 手机控制 Codex 的 Web 服务
// 功能：静态文件服务 + WebSocket 代理到 Codex app-server

import 'dotenv/config';

import express from 'express';
import { createServer } from 'http';
import { WebSocket, WebSocketServer } from 'ws';
import { spawn } from 'child_process';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import { existsSync, mkdirSync, readFileSync, writeFileSync, readdirSync } from 'fs';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// ================================================================
// Memory System
// ================================================================
const MEMORY_DIR = join(process.env.USERPROFILE || process.env.HOME, '.codex_phone', 'memory');
const MEMORY_CATEGORIES = ['profile', 'projects', 'servers', 'skills', 'notes'];
if (!existsSync(MEMORY_DIR)) { mkdirSync(MEMORY_DIR, { recursive: true }); }

// 加载系统提示（人设）
const SYSTEM_PROMPT_FILE = join(__dirname, 'system_prompt.md');
const SYSTEM_PROMPT = existsSync(SYSTEM_PROMPT_FILE)
    ? readFileSync(SYSTEM_PROMPT_FILE, 'utf-8').trim()
    : '';
if (SYSTEM_PROMPT) console.log('[System] 人设已加载');

const chatHistory = new Map();
const MAX_HISTORY = 50;

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

    // 读取已有记忆，传给 AI 做去重/更新
    const existingMemory = loadMemory();
    const existingSection = existingMemory.trim()
        ? `\n\u5df2\u6709\u8bb0\u5fc6\uff08\u8bf7\u5728\u6b64\u57fa\u7840\u4e0a\u66f4\u65b0\uff0c\u4e0d\u8981\u91cd\u590d\uff09:\n${existingMemory}\n`
        : '';

    const prompt = `\u4f60\u662f\u8bb0\u5fc6\u7ba1\u7406\u52a9\u624b\u3002\u4ece\u5bf9\u8bdd\u4e2d\u63d0\u53d6\u6709\u4ef7\u503c\u7684\u4fe1\u606f\uff0c\u5e76\u4e0e\u5df2\u6709\u8bb0\u5fc6\u5408\u5e76\u3002

\u89c4\u5219:
1. \u65b0\u4fe1\u606f: \u76f4\u63a5\u6dfb\u52a0
2. \u91cd\u590d\u4fe1\u606f: \u8df3\u8fc7\uff0c\u4e0d\u91cd\u590d\u8bb0\u5f55
3. \u51b2\u7a81\u4fe1\u606f: \u7528\u65b0\u7684\u8986\u76d6\u65e7\u7684\uff08\u5982\u90ae\u7bb1\u53d8\u4e86\uff09
4. \u6bcf\u6761\u8bb0\u5fc6\u7528\u4e00\u884c\u201c- \u5185\u5bb9\u201d\u683c\u5f0f

\u7c7b\u522b: profile(\u4e2a\u4eba\u4fe1\u606f), projects(\u9879\u76ee), servers(\u670d\u52a1\u5668), skills(\u6280\u80fd\u6d41\u7a0b), notes(\u5176\u4ed6)
${existingSection}
\u5bf9\u8bdd:
${conversation}

\u8f93\u51faJSON\uff0c\u6bcf\u4e2a\u7c7b\u522b\u7684\u503c\u662f\u5408\u5e76\u540e\u7684\u5b8c\u6574\u5185\u5bb9\uff08\u591a\u6761\u7528\\n\u5206\u9694\uff09\uff0c\u6ca1\u6709\u53d8\u5316\u7684\u7c7b\u522b\u4e0d\u8f93\u51fa:
{"profile": "- xxx\\n- yyy", "servers": "- zzz"}`;

    try {
        const result = await geminiChat(prompt);
        const jsonMatch = result.match(/\{[\s\S]*\}/);
        if (!jsonMatch) return '\u672a\u63d0\u53d6\u5230\u6709\u4ef7\u503c\u7684\u4fe1\u606f\u3002';
        const extracted = JSON.parse(jsonMatch[0]);
        let saved = [];
        for (const [cat, content] of Object.entries(extracted)) {
            if (!MEMORY_CATEGORIES.includes(cat) || !content || content === '\u65e0') continue;
            const f = join(MEMORY_DIR, `${cat}.md`);
            // \u8986\u76d6\u5199\u5165\uff08AI \u5df2\u5408\u5e76\u53bb\u91cd\uff09
            writeFileSync(f, content.replace(/\\n/g, '\n'), 'utf-8');
            saved.push(cat);
        }
        if (saved.length === 0) return '\u8fd9\u6bb5\u5bf9\u8bdd\u6ca1\u6709\u9700\u8981\u8bb0\u5fc6\u7684\u65b0\u4fe1\u606f\u3002';
        return `\u2705 \u5df2\u66f4\u65b0\u8bb0\u5fc6\uff01${saved.map(s => `*${s}*`).join(', ')}`;
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
        // 不再设超时限制，让 Codex 自由运行

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
let tgModel = 'gpt-5.4-mini'; // Codex 默认模型（mini 更快）
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

// 代理断连自动重启：连续 3 次 polling 错误 → 自动重连
let pollingErrors = 0;
let isRestarting = false;
bot.on('polling_error', (err) => {
    pollingErrors++;
    console.log(`[TG] Polling error #${pollingErrors}: ${err.message}`);
    if (pollingErrors >= 3 && !isRestarting) {
        isRestarting = true;
        console.log('[TG] 连续失败，10s 后自动重连...');
        bot.stopPolling().then(() => {
            setTimeout(() => {
                bot.startPolling();
                pollingErrors = 0;
                isRestarting = false;
                console.log('[TG] Polling 已重启');
            }, 10000);
        }).catch(() => {
            setTimeout(() => {
                bot.startPolling();
                pollingErrors = 0;
                isRestarting = false;
                console.log('[TG] Polling 已重启(fallback)');
            }, 10000);
        });
    }
});
// polling 成功时重置计数
bot.on('message', () => { pollingErrors = 0; });

// 注册命令菜单（输入 / 时显示快捷命令）
bot.setMyCommands([
    { command: 'help', description: '📖 帮助信息' },
    { command: 'codex', description: '🤖 切换到 Codex 引擎' },
    { command: 'gemini', description: '🔮 切换到 Gemini 引擎' },
    { command: 'model', description: '🔄 切换模型' },
    { command: 'new', description: '✨ 新建会话' },
    { command: 'quota', description: '📊 查看额度' },
    { command: 'memory', description: '🧠 查看记忆' },
    { command: 'account', description: '🔑 切换账户' },
    { command: 'task', description: '🔧 后台执行任务' },
    { command: 'qa', description: '🧪 QA 自动巡检' },
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
// /account 管理多账户
bot.onText(/\/account\s*(.*)/, async (msg, match) => {
    const arg = (match[1] || '').trim();
    const codexDir = join(process.env.USERPROFILE || process.env.HOME, '.codex');
    const authFile = join(codexDir, 'auth.json');

    // 读取当前账户邮箱
    function getEmail(file) {
        try {
            const auth = JSON.parse(readFileSync(file, 'utf-8'));
            const payload = JSON.parse(Buffer.from(auth.tokens?.id_token?.split('.')[1] || '', 'base64').toString());
            return payload.email || 'unknown';
        } catch { return 'unknown'; }
    }

    // 列出所有账户
    if (!arg || arg === 'list') {
        const files = readdirSync(codexDir).filter(f => f.match(/^auth_account\d+\.json$/));
        const currentEmail = getEmail(authFile);
        let list = `🔑 *当前账户:* \`${currentEmail}\`\n\n`;
        if (files.length === 0) {
            list += '暂无备份账户。用 `codex auth` 登录新账户后发 `/account save 2` 保存。';
        } else {
            files.sort().forEach(f => {
                const num = f.match(/\d+/)[0];
                const email = getEmail(join(codexDir, f));
                const isCurrent = email === currentEmail ? ' ← 当前' : '';
                list += `${num}. \`${email}\`${isCurrent}\n`;
            });
            list += '\n切换: `/account 1`\n保存当前: `/account save 3`';
        }
        bot.sendMessage(msg.chat.id, list, { parse_mode: 'Markdown' })
            .catch(() => bot.sendMessage(msg.chat.id, list));
        return;
    }

    // 保存当前账户为指定编号
    if (arg.startsWith('save')) {
        const num = arg.replace('save', '').trim() || '1';
        const target = join(codexDir, `auth_account${num}.json`);
        try {
            const content = readFileSync(authFile, 'utf-8');
            writeFileSync(target, content, 'utf-8');
            const email = getEmail(authFile);
            bot.sendMessage(msg.chat.id, `✅ 已保存当前账户为 *#${num}* (\`${email}\`)`, { parse_mode: 'Markdown' });
        } catch (e) {
            bot.sendMessage(msg.chat.id, '❌ 保存失败: ' + e.message);
        }
        return;
    }

    // 切换账户
    const num = parseInt(arg);
    if (isNaN(num)) {
        bot.sendMessage(msg.chat.id, '用法: `/account 1` 切换, `/account save 2` 保存', { parse_mode: 'Markdown' });
        return;
    }
    const source = join(codexDir, `auth_account${num}.json`);
    if (!existsSync(source)) {
        bot.sendMessage(msg.chat.id, `❌ 账户 #${num} 不存在`);
        return;
    }
    try {
        const content = readFileSync(source, 'utf-8');
        writeFileSync(authFile, content, 'utf-8');
        const email = getEmail(authFile);
        // 重启 Codex 进程让新账户生效
        if (codexProcess) { codexProcess.kill(); }
        bot.sendMessage(msg.chat.id, `✅ 已切换到账户 *#${num}* (\`${email}\`)\n🔄 Codex 重启中...`, { parse_mode: 'Markdown' });
    } catch (e) {
        bot.sendMessage(msg.chat.id, '❌ 切换失败: ' + e.message);
    }
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
            shell: true
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

// /qa — 一键 QA 自动巡检（出题→测试→核对，完成后推送结果）
// 进度状态提升为模块变量，方便随时查询
let qaProc = null;
let qaOutput = '';
let qaStartTime = 0;
let qaChatId = null;

// 解析 QA 输出，提取当前进度摘要
function getQaProgress() {
    const elapsed = Math.round((Date.now() - qaStartTime) / 1000);
    const min = Math.floor(elapsed / 60);
    const sec = elapsed % 60;
    const lines = qaOutput.split('\n');

    // 识别当前步骤
    let currentStep = '准备中...';
    const stepMatches = lines.filter(l => l.includes('Step:'));
    if (stepMatches.length > 0) currentStep = stepMatches[stepMatches.length - 1].replace(/[=\s]+/g, ' ').trim();

    // 统计题目进度 [3/43] 格式
    const progressMatch = [...qaOutput.matchAll(/\[(\d+)\/(\d+)\]/g)];
    let questionProgress = '';
    if (progressMatch.length > 0) {
        const last = progressMatch[progressMatch.length - 1];
        questionProgress = `📊 题目: ${last[1]}/${last[2]}`;
    }

    // 统计通过/失败
    const passed = (qaOutput.match(/✅/g) || []).length;
    const failed = (qaOutput.match(/❌/g) || []).length;

    // 已完成的步骤
    const okSteps = lines.filter(l => l.includes('[OK]')).map(l => l.replace(/.*\[OK\]\s*/, '').trim());
    const failSteps = lines.filter(l => l.includes('[FAIL]')).map(l => l.replace(/.*\[FAIL\]\s*/, '').trim());

    let status = `⏳ *QA 巡检进行中*\n\n`;
    status += `⏱ 已运行: ${min}分${sec}秒\n`;
    status += `📋 ${currentStep}\n`;
    if (questionProgress) status += `${questionProgress}\n`;
    if (passed || failed) status += `✅ ${passed} 通过  ❌ ${failed} 失败\n`;
    if (okSteps.length > 0) status += `\n已完成:\n${okSteps.map(s => `  ✓ ${s}`).join('\n')}\n`;
    if (failSteps.length > 0) status += `${failSteps.map(s => `  ✗ ${s}`).join('\n')}\n`;
    status += `\n_发 /qa stop 可终止_`;
    return status;
}

bot.onText(/\/qa\s*(.*)/, async (msg, match) => {
    const arg = (match[1] || '').trim();
    const chatId = msg.chat.id;

    if (arg === 'stop') {
        if (qaProc) { qaProc.kill(); qaProc = null; qaOutput = ''; bot.sendMessage(chatId, '🛑 QA 已停止'); }
        else bot.sendMessage(chatId, 'ℹ️ 没有正在运行的 QA 任务');
        return;
    }

    // 正在运行 → 显示实时进度
    if (qaProc) {
        const progress = getQaProgress();
        bot.sendMessage(chatId, progress, { parse_mode: 'Markdown' })
            .catch(() => bot.sendMessage(chatId, progress));
        return;
    }

    console.log('[QA] 启动自动巡检');
    qaChatId = chatId;
    qaOutput = '';
    qaStartTime = Date.now();
    bot.sendMessage(chatId,
        '🧪 *QA 自动巡检启动*\n\n' +
        '📋 流程: 自动出题 → 多轮测试 → 数字核对\n' +
        '⏱ 预计 3-5 分钟，完成后自动推送结果\n' +
        '👀 随时发 /qa 查看进度\n\n' +
        '_你可以继续聊天，不影响巡检..._',
        { parse_mode: 'Markdown' });

    // 构建命令：python3 qa/qa_pipeline.py --auto-generate --skip-codex
    const python = process.platform === 'win32' ? 'python' : 'python3';
    const pipelineScript = join(__dirname, 'qa', 'qa_pipeline.py');
    const cmdArgs = [pipelineScript, '--auto-generate', '--skip-codex'];

    qaProc = spawn(python, cmdArgs, {
        cwd: join(__dirname, 'qa'),
        shell: false,
        timeout: 600000,  // 10 分钟超时
        env: { ...process.env, PYTHONIOENCODING: 'utf-8' },
    });

    qaProc.stdout?.on('data', d => { qaOutput += d.toString(); });
    qaProc.stderr?.on('data', d => { qaOutput += d.toString(); });

    qaProc.on('error', (err) => {
        console.log('[QA] spawn 错误:', err.message);
        qaProc = null; qaOutput = '';
        bot.sendMessage(chatId, `❌ QA 启动失败: ${err.message}`);
    });

    qaProc.on('close', (code) => {
        qaProc = null;
        const elapsed = Math.round((Date.now() - qaStartTime) / 1000);
        console.log(`[QA] 完成 (code=${code}, ${elapsed}s, output=${qaOutput.length} chars)`);

        // 提取关键结果
        const lines = qaOutput.split('\n');
        const summaryLines = lines.filter(l =>
            l.includes('Pipeline') || l.includes('[OK]') || l.includes('[FAIL]') ||
            l.includes('核对') || l.includes('严重') || l.includes('通过率') ||
            l.includes('总耗时') || l.includes('报告')
        ).slice(-15);

        const icon = code === 0 ? '✅' : '⚠️';
        const min = Math.floor(elapsed / 60);
        const sec = elapsed % 60;
        const resultText = summaryLines.length > 0
            ? summaryLines.join('\n').substring(0, 1500)
            : qaOutput.substring(Math.max(0, qaOutput.length - 800)).substring(0, 800);

        bot.sendMessage(chatId,
            `${icon} *QA 巡检完成* (${min}分${sec}秒)\n\n\`\`\`\n${resultText}\n\`\`\``,
            { parse_mode: 'Markdown' }
        ).catch(() => bot.sendMessage(chatId, `${icon} QA 巡检完成 (${min}分${sec}秒)\n\n${resultText}`));

        qaOutput = '';
    });
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

    // 构建上下文（人设 + 记忆）
    const memory = loadMemory();
    let ctx = '';
    if (SYSTEM_PROMPT) ctx += `[系统指令]\n${SYSTEM_PROMPT}\n\n`;
    if (memory.trim()) ctx += `[你的记忆]\n${memory}\n\n`;

    try {
        let reply;
        if (engine === 'gemini') {
            console.log(`[TG/Gemini] "${msg.text.substring(0, 50)}"`);
            reply = await geminiChat(ctx + msg.text);
        } else {
            const threadId = tgThreads.get(userId) || null;
            console.log(`[TG/Codex] "${msg.text.substring(0, 50)}" model=${tgModel}`);
            // 新会话才注入上下文，已有会话不重复注入
            const text = threadId ? msg.text : ctx + msg.text;
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
        setTimeout(() => { ws.close(); reject(new Error('timeout')); }, 30000);
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
