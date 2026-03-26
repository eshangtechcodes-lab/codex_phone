/**
 * @module telegram/qa
 * @description /qa 自动巡检集成
 *
 * 启动 Python QA Pipeline，实时追踪进度，完成后推送结果到 Telegram。
 */

import { spawn } from 'child_process';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const PROJECT_ROOT = dirname(dirname(__dirname));

// QA 进程状态（模块级变量，支持进度查询）
let qaProc = null;
let qaOutput = '';
let qaStartTime = 0;
let qaChatId = null;

/**
 * 解析 QA 输出，提取当前进度摘要
 * @returns {string} Markdown 格式的进度文本
 */
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

/**
 * 注册 /qa 命令
 * @param {import('node-telegram-bot-api')} bot - Telegram Bot 实例
 */
export function registerQaCommand(bot) {
    bot.onText(/\/qa\s*(.*)/, async (msg, match) => {
        const arg = (match[1] || '').trim();
        const chatId = msg.chat.id;

        // 停止巡检
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

        // 启动巡检
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

        const python = process.platform === 'win32' ? 'python' : 'python3';
        const pipelineScript = join(PROJECT_ROOT, 'qa', 'qa_pipeline.py');

        qaProc = spawn(python, [pipelineScript, '--auto-generate', '--skip-codex'], {
            cwd: join(PROJECT_ROOT, 'qa'),
            shell: false,
            timeout: 600000,
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
}
