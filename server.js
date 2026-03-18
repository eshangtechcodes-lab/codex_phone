// Codex Phone - 手机控制 Codex 的 Web 服务
// 功能：静态文件服务 + WebSocket 代理到 Codex app-server

import express from 'express';
import { createServer } from 'http';
import { WebSocket, WebSocketServer } from 'ws';
import { spawn } from 'child_process';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

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

// 优雅退出
process.on('SIGINT', () => {
    console.log('\nShutting down...');
    if (codexProcess) codexProcess.kill();
    process.exit(0);
});
