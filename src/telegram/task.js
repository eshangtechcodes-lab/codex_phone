/**
 * @module telegram/task
 * @description /task 后台任务执行
 *
 * 通过 codex exec 在后台异步执行任务，不阻塞聊天。
 */

import { spawn } from 'child_process';
import { fileURLToPath } from 'url';
import { dirname } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const PROJECT_ROOT = dirname(dirname(__dirname)); // src/telegram/ → 项目根

/** @type {import('child_process').ChildProcess|null} */
let taskProc = null;

/**
 * 注册 /task 命令
 * @param {import('node-telegram-bot-api')} bot - Telegram Bot 实例
 */
export function registerTaskCommand(bot) {
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

        taskProc = spawn('codex', ['exec', '-m', 'gpt-5.4-mini', task], {
            cwd: PROJECT_ROOT, shell: true, timeout: 180000
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
}
