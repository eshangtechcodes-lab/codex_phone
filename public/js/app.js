// Codex Phone - 前端控制逻辑
// 通过 WebSocket 与 Codex app-server 通信（JSON-RPC）

// --- Elements ---
const chatContainer = document.getElementById('chatContainer');
const chatContent = document.getElementById('chatContent');
const messageInput = document.getElementById('messageInput');
const sendBtn = document.getElementById('sendBtn');
const statusDot = document.getElementById('statusDot');
const statusText = document.getElementById('statusText');
const newThreadBtn = document.getElementById('newThreadBtn');
const threadList = document.getElementById('threadList');
const modelSelect = document.getElementById('modelSelect');

// --- State ---
let ws = null;
let rpcId = 1;
let currentThreadId = localStorage.getItem('codex_threadId') || null;
let pendingRpc = {};       // id → { resolve, reject, method }
let isStreaming = false;
let currentAssistantEl = null;  // 当前正在输出的助手消息元素
let currentAssistantText = '';  // 当前助手消息的文本累积
let initialized = false;
let selectedModel = localStorage.getItem('codex_model') || 'gpt-5.4';

// --- WebSocket Connection ---
function connect() {
    const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
    ws = new WebSocket(`${protocol}//${location.host}`);

    ws.onopen = () => {
        console.log('[WS] Connected');
        initialized = false;
        // 发送 initialize 握手
        sendRpc('initialize', {
            clientVersion: '1.0.0',
            protocolVersion: '2.0',
            clientInfo: { name: 'codex-phone', version: '1.0.0' },
            capabilities: {}
        }).then(result => {
            console.log('[INIT] Protocol initialized:', result);
            initialized = true;
            updateStatus(true);
            loadModels();
            loadThreads();
            // 恢复上次会话
            if (currentThreadId) {
                resumeThread(currentThreadId);
            }
        }).catch(err => {
            console.error('[INIT] Failed:', err);
            updateStatus(true); // 即使 init 失败也展示连接状态
        });
    };

    ws.onmessage = (event) => {
        handleMessage(event.data);
    };

    ws.onclose = () => {
        console.log('[WS] Disconnected');
        initialized = false;
        updateStatus(false);
        setTimeout(connect, 2000);
    };

    ws.onerror = (err) => {
        console.error('[WS] Error:', err);
    };
}

function updateStatus(connected) {
    if (connected) {
        statusDot.classList.add('connected');
        statusText.textContent = 'Live';
    } else {
        statusDot.classList.remove('connected');
        statusText.textContent = 'Reconnecting...';
    }
}

// --- JSON-RPC Helper ---
function sendRpc(method, params = {}) {
    return new Promise((resolve, reject) => {
        const id = rpcId++;
        const msg = JSON.stringify({
            jsonrpc: '2.0',
            id,
            method,
            params
        });
        pendingRpc[id] = { resolve, reject, method };
        ws.send(msg);
        console.log(`[RPC →] ${method}`, params);
    });
}

// --- Handle Incoming Messages ---
function handleMessage(raw) {
    let msg;
    try {
        msg = JSON.parse(raw);
    } catch (e) {
        console.warn('[WS] Invalid JSON:', raw);
        return;
    }

    // JSON-RPC 响应（有 id，且是我们发出的请求）
    if (msg.id !== undefined && pendingRpc[msg.id]) {
        const { resolve, reject, method } = pendingRpc[msg.id];
        delete pendingRpc[msg.id];

        if (msg.error) {
            console.error(`[RPC ←] ${method} error:`, msg.error);
            reject(msg.error);
        } else {
            console.log(`[RPC ←] ${method} result:`, msg.result);
            resolve(msg.result);
        }
        return;
    }

    // JSON-RPC 通知（无 id，有 method）
    if (msg.method && msg.id === undefined) {
        handleNotification(msg.method, msg.params);
        return;
    }

    // Server → Client 请求（有 id + method，需要我们响应）
    if (msg.id !== undefined && msg.method) {
        handleServerRequest(msg);
    }
}

function handleNotification(method, params) {
    console.log(`[NOTIFY] ${method}`, params);

    switch (method) {
        case 'thread/started':
            console.log('[EVENT] Thread started:', params);
            break;

        case 'turn/started':
            console.log('[EVENT] Turn started');
            isStreaming = true;
            currentAssistantText = '';
            currentAssistantEl = addMessage('assistant', '');
            addThinking();
            break;

        case 'item/agentMessage/delta':
            // 实时文字流
            if (params && params.delta) {
                removeThinking();
                currentAssistantText += params.delta;
                if (currentAssistantEl) {
                    currentAssistantEl.innerHTML = renderMarkdown(currentAssistantText);
                }
                scrollToBottom();
            }
            break;

        case 'item/reasoning/summaryTextDelta':
        case 'item/reasoning/textDelta':
            // 推理过程文字（可选展示）
            break;

        case 'thread/status/changed':
            console.log('[EVENT] Thread status:', params);
            break;

        case 'turn/completed':
            console.log('[EVENT] Turn completed');
            removeThinking();
            isStreaming = false;
            currentAssistantEl = null;
            break;

        case 'item/started':
            // 新 item 开始（工具调用等）
            if (params && params.item) {
                handleItemStarted(params.item);
            }
            break;

        case 'item/completed':
            // item 完成
            if (params && params.item) {
                handleItemCompleted(params.item);
            }
            break;

        case 'item/commandExecution/outputDelta':
            // 命令执行的输出
            if (params && params.delta) {
                appendToolOutput(params.delta);
            }
            break;

        case 'thread/name/updated':
            // 会话名称更新
            if (params && params.name) {
                console.log('[EVENT] Thread renamed:', params.name);
            }
            break;

        default:
            // 其他通知只记录
            break;
    }
}

// 处理服务端请求（如命令审批）
function handleServerRequest(msg) {
    console.log('[SERVER REQUEST]', msg.method, msg.params);

    // 自动批准所有操作（手机端简化版）
    const response = JSON.stringify({
        jsonrpc: '2.0',
        id: msg.id,
        result: { approved: true }
    });
    ws.send(response);
    console.log('[AUTO-APPROVE]', msg.method);
}

// 处理 item 开始
function handleItemStarted(item) {
    if (item.type === 'local_shell_call') {
        const cmd = item.action?.command?.join(' ') || item.action?.command || '';
        if (cmd) addToolCall('Shell', cmd);
    } else if (item.type === 'function_call') {
        addToolCall(item.name || 'Tool', item.arguments || '');
    }
}

// 处理 item 完成
function handleItemCompleted(item) {
    if (item.type === 'message' && item.role === 'assistant') {
        const text = item.content?.map(c => c.text || '').join('') || '';
        // 如果流式没展示过，用完整 item 补上
        if (text && !currentAssistantText) {
            addMessage('assistant', renderMarkdown(text));
        }
    }
    lastToolOutput = null;
}

// --- UI Rendering ---
function addMessage(role, content) {
    removeWelcome();
    const el = document.createElement('div');
    el.className = `message ${role}`;
    el.innerHTML = content;
    chatContent.appendChild(el);
    scrollToBottom();
    return el;
}

function addToolCall(label, cmd) {
    removeWelcome();
    const el = document.createElement('div');
    el.className = 'tool-call';
    el.innerHTML = `
        <div class="tool-label">▶ ${escapeHtml(label)}</div>
        <div class="tool-cmd">${escapeHtml(cmd)}</div>
    `;
    chatContent.appendChild(el);
    scrollToBottom();
    return el;
}

let lastToolOutput = null;

function appendToolOutput(delta) {
    if (!lastToolOutput) {
        lastToolOutput = document.createElement('div');
        lastToolOutput.className = 'tool-call';
        lastToolOutput.style.color = 'var(--text-muted)';
        lastToolOutput.style.fontSize = '11px';
        chatContent.appendChild(lastToolOutput);
    }
    lastToolOutput.textContent += delta;
    scrollToBottom();
}

function addThinking() {
    if (document.querySelector('.thinking')) return;
    const el = document.createElement('div');
    el.className = 'thinking';
    el.innerHTML = `
        <div class="thinking-dots"><span></span><span></span><span></span></div>
        <span>Thinking...</span>
    `;
    chatContent.appendChild(el);
    scrollToBottom();
}

function removeThinking() {
    document.querySelectorAll('.thinking').forEach(el => el.remove());
}

function removeWelcome() {
    const w = document.querySelector('.welcome-state');
    if (w) w.remove();
}

function scrollToBottom() {
    requestAnimationFrame(() => {
        chatContainer.scrollTop = chatContainer.scrollHeight;
    });
}

function clearChat() {
    chatContent.innerHTML = '';
}

// --- Simple Markdown → HTML ---
function renderMarkdown(text) {
    if (!text) return '';
    let html = escapeHtml(text);

    // 代码块
    html = html.replace(/```(\w*)\n([\s\S]*?)```/g, (_, lang, code) => {
        return `<pre><code>${code.trim()}</code></pre>`;
    });

    // 行内代码
    html = html.replace(/`([^`]+)`/g, '<code>$1</code>');

    // 粗体
    html = html.replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>');

    // 斜体
    html = html.replace(/\*(.+?)\*/g, '<em>$1</em>');

    // 换行
    html = html.replace(/\n/g, '<br>');

    return html;
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// --- Actions ---
async function sendMessage() {
    const text = messageInput.value.trim();
    if (!text || !ws || ws.readyState !== WebSocket.OPEN) return;

    messageInput.value = '';
    messageInput.style.height = 'auto';
    lastToolOutput = null;

    // 显示用户消息
    addMessage('user', escapeHtml(text));

    // 如果没有会话，先创建
    if (!currentThreadId) {
        try {
            const result = await sendRpc('thread/start', { model: selectedModel });
            currentThreadId = result.thread?.id || result.threadId;
            localStorage.setItem('codex_threadId', currentThreadId);
            console.log('[THREAD] Auto-created:', currentThreadId);
        } catch (e) {
            addMessage('assistant', `Error starting thread: ${e.message || JSON.stringify(e)}`);
            return;
        }
    }

    // 发送消息 (turn/start)
    try {
        await sendRpc('turn/start', {
            threadId: currentThreadId,
            input: [{ type: 'text', text }]
        });
    } catch (e) {
        addMessage('assistant', `Error: ${e.message || JSON.stringify(e)}`);
    }
}

async function loadModels() {
    try {
        const result = await sendRpc('model/list', {});
        const models = result.data || [];
        modelSelect.innerHTML = '';
        models.filter(m => !m.hidden).forEach(m => {
            const opt = document.createElement('option');
            opt.value = m.id;
            // 简短显示名
            opt.textContent = m.displayName || m.id;
            if (m.id === selectedModel) opt.selected = true;
            if (m.isDefault) opt.textContent += ' ★';
            modelSelect.appendChild(opt);
        });
    } catch (e) {
        console.log('[MODELS] List failed:', e);
    }
}

modelSelect.addEventListener('change', () => {
    selectedModel = modelSelect.value;
    localStorage.setItem('codex_model', selectedModel);
    console.log('[MODEL] Switched to:', selectedModel);
});

async function loadThreads() {
    try {
        const result = await sendRpc('thread/list', {});
        const threads = result.data || result.threads || [];
        threadList.innerHTML = '';
        threads.slice(0, 10).forEach(t => {
            const btn = document.createElement('button');
            btn.className = 'thread-tab';
            btn.textContent = t.preview || t.name || t.title || 'Untitled';
            btn.title = t.id;
            if (t.id === currentThreadId) btn.classList.add('active');
            btn.onclick = () => resumeThread(t.id);
            threadList.appendChild(btn);
        });
    } catch (e) {
        console.log('[THREADS] List failed:', e);
    }
}

async function resumeThread(threadId) {
    try {
        clearChat();
        currentThreadId = threadId;
        localStorage.setItem('codex_threadId', threadId);

        // 用 thread/resume 获取完整历史（thread/read 不返回消息内容）
        const result = await sendRpc('thread/resume', { threadId });
        const turns = result.thread?.turns || [];

        if (turns.length === 0) {
            console.log('[THREAD] No history found');
            return;
        }

        // 遍历所有轮次(turns) → 项(items)
        turns.forEach(turn => {
            (turn.items || []).forEach(item => {
                // 用户消息
                if (item.type === 'userMessage') {
                    const text = item.text || '';
                    if (text) addMessage('user', escapeHtml(text));
                }
                // 助手消息
                else if (item.type === 'agentMessage') {
                    const text = item.text || '';
                    if (text) addMessage('assistant', renderMarkdown(text));
                }
                // 命令执行
                else if (item.type === 'local_shell_call') {
                    const cmd = item.action?.command?.join(' ') || '';
                    if (cmd) addToolCall('Shell', cmd);
                }
            });
        });

        updateThreadTabs();
    } catch (e) {
        console.error('[THREAD] Resume failed:', e);
    }
}

function updateThreadTabs() {
    document.querySelectorAll('.thread-tab').forEach(btn => {
        btn.classList.toggle('active', btn.title === currentThreadId);
    });
    newThreadBtn.classList.toggle('active', !currentThreadId);
}

// --- Event Listeners ---
sendBtn.addEventListener('click', sendMessage);

messageInput.addEventListener('keydown', (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
        e.preventDefault();
        sendMessage();
    }
});

messageInput.addEventListener('input', function () {
    this.style.height = 'auto';
    this.style.height = this.scrollHeight + 'px';
});

newThreadBtn.addEventListener('click', () => {
    currentThreadId = null;
    localStorage.removeItem('codex_threadId');
    clearChat();
    chatContent.innerHTML = `
        <div class="welcome-state">
            <div class="welcome-icon">⌘</div>
            <h2>Codex Phone</h2>
            <p>Send a message to start coding</p>
        </div>
    `;
    updateThreadTabs();
});

// --- Voice Input ---
const voiceBtn = document.getElementById('voiceBtn');
let recognition = null;
let isRecording = false;

function initVoiceInput() {
    const SpeechRecognition = window.SpeechRecognition || window.webkitSpeechRecognition;
    if (!SpeechRecognition) {
        voiceBtn.classList.add('hidden');
        return;
    }

    recognition = new SpeechRecognition();
    recognition.continuous = false;      // 说完自动停止
    recognition.interimResults = true;
    recognition.lang = 'zh-CN';

    let finalTranscript = '';

    recognition.onresult = (event) => {
        let interim = '';
        finalTranscript = '';
        for (let i = 0; i < event.results.length; i++) {
            const t = event.results[i][0].transcript;
            if (event.results[i].isFinal) finalTranscript += t;
            else interim += t;
        }
        messageInput.value = finalTranscript + interim;
        messageInput.style.height = 'auto';
        messageInput.style.height = messageInput.scrollHeight + 'px';
    };

    recognition.onend = () => {
        isRecording = false;
        voiceBtn.classList.remove('recording');
        if (finalTranscript.trim()) {
            messageInput.value = finalTranscript.trim();
        }
        finalTranscript = '';
    };

    recognition.onerror = () => {
        isRecording = false;
        voiceBtn.classList.remove('recording');
        finalTranscript = '';
    };

    voiceBtn.addEventListener('click', () => {
        if (isRecording) {
            recognition.stop();
        } else {
            finalTranscript = '';
            try {
                recognition.start();
                isRecording = true;
                voiceBtn.classList.add('recording');
            } catch (e) { console.warn('[VOICE]', e); }
        }
    });
}

// --- Init ---
initVoiceInput();
connect();
