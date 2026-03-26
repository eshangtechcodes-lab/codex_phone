/**
 * @module codex
 * @description Codex app-server 进程管理和 WebSocket 通信
 *
 * 职责：
 * - 启动/重启 Codex app-server 子进程
 * - WebSocket 代理：手机浏览器 ↔ Codex app-server
 * - codexChat()：通过 WS 完成一轮 AI 对话
 * - quickRpc()：快速 RPC 查询（如额度查询）
 */

import { WebSocket, WebSocketServer } from 'ws';
import { spawn } from 'child_process';

// 端口配置
const CODEX_WS_PORT = 4002;
const CODEX_WS_URL = `ws://127.0.0.1:${CODEX_WS_PORT}`;

/** @type {import('child_process').ChildProcess|null} Codex 子进程引用 */
let codexProcess = null;

/**
 * 启动 Codex app-server 子进程
 * 进程退出后自动重启（3秒延迟）
 */
export function startCodexAppServer() {
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
        if (code !== null) {
            console.log('[CODEX] Restarting in 3s...');
            setTimeout(startCodexAppServer, 3000);
        }
    });

    codexProcess.on('error', (err) => {
        console.error('[CODEX] Failed to start:', err.message);
    });
}

/**
 * 获取 Codex 进程状态
 * @returns {boolean} 进程是否存活
 */
export function isCodexAlive() {
    return codexProcess && !codexProcess.killed;
}

/**
 * 终止 Codex 进程（用于账户切换后重启）
 */
export function killCodexProcess() {
    if (codexProcess) codexProcess.kill();
}

/**
 * 在 HTTP server 上设置 WebSocket 代理
 * 手机连接 → 代理到 Codex app-server，双向透传消息
 * @param {import('http').Server} server - HTTP 服务实例
 */
export function setupWsProxy(server) {
    const wss = new WebSocketServer({ server });

    wss.on('connection', (clientWs, req) => {
        console.log(`[WS] Phone connected from ${req.socket.remoteAddress}`);

        let codexWs = null;
        let codexReady = false;
        let pendingMessages = [];

        function connectToCodex() {
            codexWs = new WebSocket(CODEX_WS_URL);

            codexWs.on('open', () => {
                console.log('[WS] Connected to Codex app-server');
                codexReady = true;
                pendingMessages.forEach(m => {
                    console.log('[WS] Flushing pending →', m.substring(0, 100));
                    codexWs.send(m);
                });
                pendingMessages = [];
            });

            codexWs.on('message', (data) => {
                const msg = data.toString();
                console.log('[WS] Codex →', msg.substring(0, 200));
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
}

/**
 * 通过 WebSocket 与 Codex 完成一轮对话
 * @param {string} message - 用户消息
 * @param {string} model - 模型名称（如 'gpt-5.4'）
 * @param {string} [existingThreadId] - 已有会话 ID（续聊用）
 * @returns {Promise<{reply: string, threadId: string, model: string}>}
 */
export function codexChat(message, model, existingThreadId) {
    return new Promise((resolve, reject) => {
        const ws = new WebSocket(CODEX_WS_URL);
        let rpcId = 1;
        let threadId = existingThreadId;
        let responseText = '';

        function send(method, params) {
            const id = rpcId++;
            ws.send(JSON.stringify({ jsonrpc: '2.0', id, method, params }));
            return id;
        }

        ws.on('open', () => {
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

            // Initialize 响应 → 创建/恢复会话
            if (msg.id && !initDone && msg.result) {
                initDone = true;
                if (threadId) {
                    send('thread/resume', { threadId });
                    threadStarted = true;
                    send('turn/start', {
                        threadId,
                        input: [{ type: 'text', text: message }]
                    });
                } else {
                    send('thread/start', { model });
                }
                return;
            }

            // thread/start 响应 → 发送消息
            if (msg.id && msg.result && !threadStarted) {
                threadId = msg.result.thread?.id || msg.result.threadId;
                threadStarted = true;
                send('turn/start', {
                    threadId,
                    input: [{ type: 'text', text: message }]
                });
                return;
            }

            // 流式文字增量
            if (msg.method === 'item/agentMessage/delta' && msg.params?.delta) {
                responseText += msg.params.delta;
            }

            // turn 完成 → 返回结果
            if (msg.method === 'turn/completed') {
                ws.close();
                resolve({ reply: responseText, threadId, model });
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

        ws.on('error', (err) => { reject(err); });
    });
}

/**
 * 快速 RPC 查询（不走完整对话流程）
 * 用于查询额度等轻量操作
 * @param {string} method - RPC 方法名
 * @param {object} params - 参数
 * @returns {Promise<object>} RPC 结果
 */
export function quickRpc(method, params) {
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

export { CODEX_WS_PORT };
