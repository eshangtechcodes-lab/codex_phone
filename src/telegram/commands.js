/**
 * @module telegram/commands
 * @description Telegram Bot 基础命令集
 *
 * 注册的命令：/help /start /codex /gemini /new /model /account /quota /memory
 */

import { join } from 'path';
import { existsSync, readFileSync, writeFileSync, readdirSync } from 'fs';
import { codexChat, quickRpc, killCodexProcess } from '../codex.js';
import { geminiChat, getGeminiModel, setGeminiModel } from '../gemini.js';
import { loadMemory, clearMemory, MEMORY_DIR, MEMORY_CATEGORIES } from '../memory.js';

// 模型状态（模块级管理）
let tgModel = 'gpt-5.4-mini';
const tgThreads = new Map();   // userId → threadId
const tgEngine = new Map();    // userId → 'codex' | 'gemini'

/**
 * 获取用户当前引擎
 * @param {number} userId
 * @returns {string} 'codex' 或 'gemini'
 */
export function getEngine(userId) {
    return tgEngine.get(userId) || 'codex';
}

/**
 * 获取用户当前 Codex 会话 ID
 * @param {number} userId
 * @returns {string|null}
 */
export function getThreadId(userId) {
    return tgThreads.get(userId) || null;
}

/**
 * 保存用户的 Codex 会话 ID
 * @param {number} userId
 * @param {string} threadId
 */
export function setThreadId(userId, threadId) {
    tgThreads.set(userId, threadId);
}

/**
 * 获取当前 Codex 模型
 * @returns {string}
 */
export function getCodexModel() {
    return tgModel;
}

/**
 * 注册所有基础命令
 * @param {import('node-telegram-bot-api')} bot - Telegram Bot 实例
 */
export function registerBasicCommands(bot) {

    // 帮助文本
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

    // /start /help
    bot.onText(/\/start/, (msg) => {
        bot.sendMessage(msg.chat.id, helpText(getEngine(msg.from.id)), { parse_mode: 'Markdown' });
    });
    bot.onText(/\/help/, (msg) => {
        bot.sendMessage(msg.chat.id, helpText(getEngine(msg.from.id)), { parse_mode: 'Markdown' });
    });

    // /codex — 切换到 Codex 引擎
    bot.onText(/\/codex/, (msg) => {
        tgEngine.set(msg.from.id, 'codex');
        bot.sendMessage(msg.chat.id, '🟢 已切换到 *Codex* 引擎（GPT-5.4，能执行代码）', { parse_mode: 'Markdown' });
    });

    // /gemini — 切换到 Gemini 引擎
    bot.onText(/\/gemini/, (msg) => {
        tgEngine.set(msg.from.id, 'gemini');
        bot.sendMessage(msg.chat.id, '🔵 已切换到 *Gemini* 引擎（Gemini 3 Flash，多模态）', { parse_mode: 'Markdown' });
    });

    // /new — 新建会话
    bot.onText(/\/new/, (msg) => {
        tgThreads.delete(msg.from.id);
        bot.sendMessage(msg.chat.id, '✅ 已开始新会话');
    });

    // /model — 切换模型
    bot.onText(/\/model\s*(.*)/, (msg, match) => {
        const engine = getEngine(msg.from.id);
        const input = (match[1] || '').trim();

        if (engine === 'gemini') {
            const geminiModels = ['gemini-2.5-pro', 'gemini-2.5-flash', 'gemini-2.0-flash', 'gemini-3.1-pro-preview'];
            if (input && geminiModels.includes(input)) {
                setGeminiModel(input);
                bot.sendMessage(msg.chat.id, `✅ Gemini 模型切换为: *${input}*`, { parse_mode: 'Markdown' });
            } else {
                bot.sendMessage(msg.chat.id,
                    `🔮 当前 Gemini 模型: *${getGeminiModel()}*\n\n可选：\n${geminiModels.map(m => `\`/model ${m}\``).join('\n')}`,
                    { parse_mode: 'Markdown' });
            }
        } else {
            const codexModels = ['gpt-5.4', 'gpt-5.4-mini', 'gpt-5.3-codex', 'gpt-5.2-codex', 'gpt-5.2', 'gpt-5.1-codex-max', 'gpt-5.1-codex-mini'];
            if (input && codexModels.includes(input)) {
                tgModel = input;
                bot.sendMessage(msg.chat.id, `✅ Codex 模型切换为: *${tgModel}*`, { parse_mode: 'Markdown' });
            } else {
                bot.sendMessage(msg.chat.id,
                    `🤖 当前 Codex 模型: *${tgModel}*\n\n可选：\n${codexModels.map(m => `\`/model ${m}\``).join('\n')}`,
                    { parse_mode: 'Markdown' });
            }
        }
    });

    // /account — 多账户管理
    bot.onText(/\/account\s*(.*)/, async (msg, match) => {
        const arg = (match[1] || '').trim();
        const codexDir = join(process.env.USERPROFILE || process.env.HOME, '.codex');
        const authFile = join(codexDir, 'auth.json');

        function getEmail(file) {
            try {
                const auth = JSON.parse(readFileSync(file, 'utf-8'));
                const payload = JSON.parse(Buffer.from(auth.tokens?.id_token?.split('.')[1] || '', 'base64').toString());
                return payload.email || 'unknown';
            } catch { return 'unknown'; }
        }

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
            killCodexProcess();
            bot.sendMessage(msg.chat.id, `✅ 已切换到账户 *#${num}* (\`${email}\`)\n🔄 Codex 重启中...`, { parse_mode: 'Markdown' });
        } catch (e) {
            bot.sendMessage(msg.chat.id, '❌ 切换失败: ' + e.message);
        }
    });

    // /quota — 查看额度
    bot.onText(/\/quota/, async (msg) => {
        try {
            const result = await quickRpc('account/rateLimits/read', {});
            const limits = result.rateLimits || {};
            const p = limits.primary || {};
            const s = limits.secondary || {};
            const resetMin = Math.max(0, Math.round((p.resetsAt * 1000 - Date.now()) / 60000));
            const resetH = Math.floor(resetMin / 60);
            const resetM = resetMin % 60;

            let email = 'unknown';
            let lastRefresh = '';
            try {
                const authFile = join(process.env.USERPROFILE || process.env.HOME, '.codex', 'auth.json');
                const auth = JSON.parse(readFileSync(authFile, 'utf-8'));
                lastRefresh = auth.last_refresh ? new Date(auth.last_refresh).toLocaleString('zh-CN') : '';
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
                { parse_mode: 'Markdown' });
        } catch (e) {
            bot.sendMessage(msg.chat.id, '❌ 查询额度失败: ' + e.message);
        }
    });

    // /memory — 查看和管理记忆
    bot.onText(/\/memory\s*(.*)/, async (msg, match) => {
        const arg = (match[1] || '').trim();
        if (arg === 'clear') {
            clearMemory();
            bot.sendMessage(msg.chat.id, '🗑️ 所有记忆已清空');
            return;
        }
        const memory = loadMemory();
        if (!memory.trim()) {
            bot.sendMessage(msg.chat.id, '🧠 记忆为空\n\n聊天后说"总结一下"或"记下来"即可保存记忆。');
        } else {
            bot.sendMessage(msg.chat.id, `🧠 *当前记忆*\n${memory}\n\n_发 /memory clear 清空_`, { parse_mode: 'Markdown' })
                .catch(() => bot.sendMessage(msg.chat.id, `🧠 当前记忆\n${memory}\n\n发 /memory clear 清空`));
        }
    });

    // 注册命令菜单
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
}
