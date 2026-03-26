/**
 * @module telegram/index
 * @description Telegram Bot 初始化、polling 管理和消息路由
 *
 * 职责：
 * - 创建 Bot 实例（含代理配置）
 * - 处理 polling 断连自动重连
 * - 注册所有子模块命令
 * - 路由普通消息到 Codex/Gemini 引擎
 */

import TelegramBot from 'node-telegram-bot-api';
import { existsSync, readFileSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

import { codexChat } from '../codex.js';
import { geminiChat } from '../gemini.js';
import { addHistory, loadMemory, isMemoryTrigger, extractAndSave } from '../memory.js';
import { registerBasicCommands, getEngine, getThreadId, setThreadId, getCodexModel } from './commands.js';
import { registerTaskCommand } from './task.js';
import { registerQaCommand } from './qa.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const PROJECT_ROOT = dirname(dirname(__dirname));

// 加载系统提示（人设）
const SYSTEM_PROMPT_FILE = join(PROJECT_ROOT, 'config', 'system_prompt.md');
// 兼容旧位置（根目录）
const SYSTEM_PROMPT_FILE_LEGACY = join(PROJECT_ROOT, 'system_prompt.md');
const promptFile = existsSync(SYSTEM_PROMPT_FILE) ? SYSTEM_PROMPT_FILE : SYSTEM_PROMPT_FILE_LEGACY;
const SYSTEM_PROMPT = existsSync(promptFile) ? readFileSync(promptFile, 'utf-8').trim() : '';
if (SYSTEM_PROMPT) console.log('[System] 人设已加载');

/**
 * 初始化并启动 Telegram Bot
 * @returns {TelegramBot|null} Bot 实例（未配置 TOKEN 时返回 null）
 */
export function setupTelegramBot() {
    const TG_TOKEN = process.env.TG_TOKEN;
    if (!TG_TOKEN) {
        console.warn('[TG] ⚠️ TG_TOKEN 未配置，Telegram Bot 已禁用');
        return null;
    }

    // 代理配置
    const botOptions = { polling: true };
    if (process.env.TG_PROXY) {
        botOptions.request = { proxy: process.env.TG_PROXY };
        console.log(`[TG] 使用代理: ${process.env.TG_PROXY}`);
    }

    const bot = new TelegramBot(TG_TOKEN, botOptions);

    // --- Polling 断连自动重连 ---
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
    bot.on('message', () => { pollingErrors = 0; });

    // --- 注册各模块命令 ---
    registerBasicCommands(bot);
    registerTaskCommand(bot);
    registerQaCommand(bot);

    // --- 不支持的消息类型 ---
    bot.on('photo', (msg) => {
        bot.sendMessage(msg.chat.id, '📷 暂不支持图片识别（Gemini CLI 不支持传图）。\n请用文字描述图片内容，我来帮你分析！');
    });
    bot.on('voice', (msg) => {
        bot.sendMessage(msg.chat.id, '🎤 暂不支持语音识别，请发文字消息。');
    });

    // --- 普通消息路由 ---
    bot.on('message', async (msg) => {
        if (!msg.text || msg.text.startsWith('/')) return;

        const userId = msg.from.id;
        const engine = getEngine(userId);

        // 记忆触发检测
        if (isMemoryTrigger(msg.text)) {
            bot.sendChatAction(msg.chat.id, 'typing');
            console.log(`[Memory] 用户触发记忆提取`);
            const result = await extractAndSave(userId, geminiChat);
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
                const threadId = getThreadId(userId);
                const model = getCodexModel();
                console.log(`[TG/Codex] "${msg.text.substring(0, 50)}" model=${model}`);
                const text = threadId ? msg.text : ctx + msg.text;
                const result = await codexChat(text, model, threadId);
                if (result.threadId) setThreadId(userId, result.threadId);
                reply = result.reply;
            }
            addHistory(userId, 'AI', reply?.substring(0, 500) || '');
            bot.sendMessage(msg.chat.id, reply || '(empty reply)', { parse_mode: 'Markdown' })
                .catch(() => bot.sendMessage(msg.chat.id, reply || '(empty reply)'));
        } catch (e) {
            bot.sendMessage(msg.chat.id, '❌ Error: ' + e.message);
        }
    });

    console.log('[TG] Bot @yskj02_bot started');
    return bot;
}
