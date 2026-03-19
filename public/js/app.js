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
            loadQuota();
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

    // 如果没有会话，先创建并让 Codex 读取 CODEX.md
    let isNewThread = false;
    if (!currentThreadId) {
        try {
            const result = await sendRpc('thread/start', { model: selectedModel });
            currentThreadId = result.thread?.id || result.threadId;
            localStorage.setItem('codex_threadId', currentThreadId);
            isNewThread = true;
            console.log('[THREAD] Auto-created:', currentThreadId);
        } catch (e) {
            addMessage('assistant', `Error starting thread: ${e.message || JSON.stringify(e)}`);
            return;
        }
    }

    // 新会话：先让 Codex 读 CODEX.md 作为"记忆"
    const actualInput = isNewThread
        ? `Please first silently read the file CODEX.md in the current directory for context about me and this project. Do not mention reading the file. Then answer my question:\n\n${text}`
        : text;

    // 发送消息 (turn/start)
    try {
        await sendRpc('turn/start', {
            threadId: currentThreadId,
            input: [{ type: 'text', text: actualInput }]
        });
    } catch (e) {
        addMessage('assistant', `Error: ${e.message || JSON.stringify(e)}`);
    }
}

// --- Quota Display ---
const quotaBadge = document.getElementById('quotaBadge');
const quotaFill = document.getElementById('quotaFill');
const quotaText = document.getElementById('quotaText');

async function loadQuota() {
    try {
        const [limitsResult, accountResult] = await Promise.all([
            sendRpc('account/rateLimits/read', {}),
            sendRpc('account/read', {})
        ]);
        const limits = limitsResult.rateLimits || {};
        const account = accountResult.account || {};
        const used = limits.primary?.usedPercent ?? 0;
        const remaining = 100 - used;

        // 计算重置倒计时
        let resetStr = '';
        if (limits.primary?.resetsAt) {
            const mins = Math.max(0, Math.round((limits.primary.resetsAt * 1000 - Date.now()) / 60000));
            if (mins >= 60) {
                resetStr = ` ↻${Math.floor(mins/60)}h${mins%60 > 0 ? mins%60 + 'm' : ''}`;
            } else {
                resetStr = ` ↻${mins}m`;
            }
        }

        // 进度条显示剩余
        quotaFill.style.width = remaining + '%';
        const plan = (limits.planType || 'unknown').toUpperCase();
        quotaText.textContent = remaining + '%' + resetStr;

        // 颜色：绿(>60) → 黄(30-60) → 红(<30)
        if (remaining > 60) {
            quotaFill.style.background = 'var(--success)';
            quotaText.style.color = 'var(--success)';
        } else if (remaining > 30) {
            quotaFill.style.background = 'var(--warning)';
            quotaText.style.color = 'var(--warning)';
        } else {
            quotaFill.style.background = 'var(--error)';
            quotaText.style.color = 'var(--error)';
        }

        // tooltip 显示完整账户信息
        const weekUsed = limits.secondary?.usedPercent ?? 0;
        const email = account.email || 'unknown';
        quotaBadge.title = `📧 ${email}\n📋 Plan: ${plan}\n⏱ 5h: ${remaining}% left${resetStr}\n📅 Week: ${100 - weekUsed}% left`;
    } catch (e) {
        console.log('[QUOTA] Failed:', e);
        quotaText.textContent = '--';
    }
}

// 每分钟自动刷新额度
setInterval(() => { if (initialized) loadQuota(); }, 60000);

// 点击刷新额度
quotaBadge.addEventListener('click', loadQuota);

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
        threads.slice(0, 20).forEach(t => {
            const btn = document.createElement('button');
            btn.className = 'thread-item';
            btn.textContent = t.preview || t.name || t.title || 'Untitled';
            btn.title = t.id;
            if (t.id === currentThreadId) btn.classList.add('active');
            btn.onclick = () => {
                threadDropdown.classList.remove('open');
                resumeThread(t.id);
            };
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
    document.querySelectorAll('.thread-item').forEach(btn => {
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

// --- History dropdown ---
const historyBtn = document.getElementById('historyBtn');
const threadDropdown = document.getElementById('threadDropdown');

historyBtn.addEventListener('click', (e) => {
    e.stopPropagation();
    threadDropdown.classList.toggle('open');
    if (threadDropdown.classList.contains('open')) loadThreads();
});

// 点外部关闭
document.addEventListener('click', (e) => {
    if (!e.target.closest('.thread-dropdown-wrap')) {
        threadDropdown.classList.remove('open');
    }
});

newThreadBtn.addEventListener('click', () => {
    currentThreadId = null;
    localStorage.removeItem('codex_threadId');
    clearChat();
    threadDropdown.classList.remove('open');
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

// ================================================================
// PWA 支持：Service Worker 注册、网络状态、安装提示
// ================================================================

// --- Service Worker 注册 ---
if ('serviceWorker' in navigator) {
    let refreshing = false;

    navigator.serviceWorker.register('/sw.js').then(reg => {
        console.log('[SW] Registered, scope:', reg.scope);

        // 检测新版本
        reg.addEventListener('updatefound', () => {
            const newWorker = reg.installing;
            if (!newWorker) return;

            newWorker.addEventListener('statechange', () => {
                // 新 SW 已安装完毕，等待激活 → 提示用户刷新
                if (newWorker.state === 'installed' && navigator.serviceWorker.controller) {
                    console.log('[SW] New version available');
                    showUpdateBar(newWorker);
                }
            });
        });
    }).catch(err => {
        console.warn('[SW] Registration failed:', err);
    });

    // 监听 controllerchange → 自动刷新页面（防止重复刷新）
    navigator.serviceWorker.addEventListener('controllerchange', () => {
        if (!refreshing) {
            refreshing = true;
            location.reload();
        }
    });
}

// --- 更新提示条 ---
function showUpdateBar(worker) {
    const bar = document.getElementById('updateBar');
    const btn = document.getElementById('updateBtn');
    bar.classList.add('show');

    btn.addEventListener('click', () => {
        // 通知新 SW 立即激活
        worker.postMessage('skipWaiting');
        bar.classList.remove('show');
    });
}

// --- 网络状态监听 ---
const offlineBar = document.getElementById('offlineBar');

function updateOnlineStatus() {
    if (navigator.onLine) {
        offlineBar.classList.remove('show');
    } else {
        offlineBar.classList.add('show');
    }
}

window.addEventListener('online', updateOnlineStatus);
window.addEventListener('offline', updateOnlineStatus);
// 初始检查
updateOnlineStatus();

// --- 安装提示（beforeinstallprompt） ---
let deferredPrompt = null;
const installPrompt = document.getElementById('installPrompt');
const installBtn = document.getElementById('installBtn');
const installDismiss = document.getElementById('installDismiss');

window.addEventListener('beforeinstallprompt', (e) => {
    e.preventDefault();
    deferredPrompt = e;

    // 如果用户之前没有关闭过，延迟 3 秒展示
    const dismissed = localStorage.getItem('codex_install_dismissed');
    if (!dismissed) {
        setTimeout(() => {
            installPrompt.classList.add('show');
        }, 3000);
    }
});

if (installBtn) {
    installBtn.addEventListener('click', async () => {
        if (!deferredPrompt) return;
        installPrompt.classList.remove('show');
        deferredPrompt.prompt();
        const { outcome } = await deferredPrompt.userChoice;
        console.log('[PWA] Install outcome:', outcome);
        deferredPrompt = null;
    });
}

if (installDismiss) {
    installDismiss.addEventListener('click', () => {
        installPrompt.classList.remove('show');
        localStorage.setItem('codex_install_dismissed', '1');
    });
}

// 已安装后隐藏提示
window.addEventListener('appinstalled', () => {
    console.log('[PWA] App installed');
    installPrompt.classList.remove('show');
    deferredPrompt = null;
});

