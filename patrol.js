// patrol.js v3 — AI 智能工作流
// 核心逻辑：Codex 自己判断任务复杂度 → 简单的直接干 → 复杂的才拆步骤
// Telegram 实时推送进度
//
// 用法：
//   node patrol.js                            # 默认：完善 README
//   node patrol.js "给项目加上 .env.example"   # 自定义任务

import { execSync } from 'child_process';
import fs from 'fs';
import { dirname, join, basename } from 'path';
import { fileURLToPath } from 'url';
import TelegramBot from 'node-telegram-bot-api';

const __dirname = dirname(fileURLToPath(import.meta.url));

// ===== 配置 =====
const TARGET_DIR = __dirname;
const TG_TOKEN = '8783767689:AAFXHLR_GxnC_RecnWOPGtiizuXr1NGmoOA';
const TG_CHAT_ID = '7350861140';
const PLAN_FILE = join(TARGET_DIR, '.patrol-plan.json');
const LOG_FILE = join(TARGET_DIR, '.patrol-log.txt');

const DEFAULT_TASK = '完善 README.md：加上中文项目简介、功能列表、快速开始、使用说明和项目结构';
const TASK = process.argv[2] || DEFAULT_TASK;

// ===== Telegram =====
const bot = new TelegramBot(TG_TOKEN);
async function tg(msg) {
    try { await bot.sendMessage(TG_CHAT_ID, msg, { parse_mode: 'Markdown' }); }
    catch(e) { console.log('  [TG err]', e.message.substring(0, 60)); }
}

// ===== 日志 =====
function initLog() { fs.writeFileSync(LOG_FILE, `# Patrol v3 — ${new Date().toLocaleString()}\n`, 'utf-8'); }
function writeLog(s) { fs.appendFileSync(LOG_FILE, `[${new Date().toLocaleTimeString()}] ${s}\n`, 'utf-8'); }
function log(emoji, msg) { console.log(`${emoji}  ${msg}`); writeLog(msg); }

// ===== Codex =====
function askCodex(prompt) {
    console.log(`[${new Date().toLocaleTimeString()}] 🤖 Codex 工作中...`);
    writeLog(`PROMPT: ${prompt.substring(0, 150)}...`);
    const pf = join(TARGET_DIR, '.patrol-prompt.md');
    fs.writeFileSync(pf, prompt, 'utf-8');
    try {
        const r = execSync(
            `codex exec -m gpt-5.4-mini "阅读 .patrol-prompt.md 并按要求执行"`,
            { cwd: TARGET_DIR, encoding: 'utf-8', timeout: 180000, maxBuffer: 10*1024*1024 }
        ).trim();
        writeLog(`DONE (${r.length} chars)`);
        return r;
    } catch (e) {
        const o = (e.stdout?.toString() || e.message).trim();
        writeLog(`ERR: ${o.substring(0, 200)}`);
        return o;
    } finally { try { fs.unlinkSync(pf); } catch {} }
}

// ===== 主流程 =====
async function main() {
    const project = basename(TARGET_DIR);
    initLog();

    log('🚀', `任务: ${TASK}`);
    await tg(`🚀 *Patrol 启动*\n📂 \`${project}\`\n📋 ${TASK}`);

    // ===== 第一步：让 Codex 自己判断 =====
    log('🧠', '让 Codex 评估任务...');
    await tg('🧠 Codex 正在评估任务复杂度...');

    askCodex(`你收到一个任务：
"${TASK}"

请先评估这个任务的复杂度，然后写入 .patrol-plan.json：

如果任务简单（可以一次完成），写：
{"mode":"direct","summary":"任务简单，直接执行"}

如果任务复杂（需要分步），写：
{"mode":"multi","summary":"一句话说明","tasks":[{"id":1,"title":"步骤名","description":"简述"}]}

注意：倾向于 direct 模式，除非任务确实涉及多个独立文件或独立功能。`);

    let plan;
    try { plan = JSON.parse(fs.readFileSync(PLAN_FILE, 'utf-8')); } catch {
        log('❌', '评估失败'); await tg('❌ 评估失败'); process.exit(1);
    }

    if (plan.mode === 'direct') {
        // ===== 直接模式：一把搞定 =====
        log('⚡', `直接模式 — ${plan.summary}`);
        await tg(`⚡ *直接模式*\n${plan.summary}\n\n_执行中..._`);

        askCodex(`请直接执行以下任务：\n${TASK}\n\n要求：简洁高效，不要过度设计。`);

        log('✅', '任务完成');
        await tg('✅ 任务已完成！');

    } else {
        // ===== 多步模式 =====
        const n = plan.tasks?.length || 0;
        let planMsg = `📋 *多步模式* (${n} 步)\n${plan.summary}\n\n`;
        plan.tasks?.forEach((t, i) => { planMsg += `${i+1}. ${t.title}\n`; });
        log('📋', `多步模式 — ${n} 个步骤`);
        plan.tasks?.forEach((t, i) => console.log(`  ${i+1}. ${t.title}`));
        await tg(planMsg);

        for (let i = 0; i < plan.tasks.length; i++) {
            const t = plan.tasks[i];
            log('▶️', `[${i+1}/${n}] ${t.title}`);
            await tg(`▶️ [${i+1}/${n}] ${t.title}`);

            askCodex(`执行任务：${t.title}\n${t.description}\n要求：简洁，不要过度设计。`);

            await tg(`✅ [${i+1}/${n}] ${t.title} — 完成`);
        }
    }

    // ===== 提交 =====
    log('📦', '提交修改...');
    try {
        execSync('git add -A', { cwd: TARGET_DIR });
        const diff = execSync('git diff --cached --stat', { cwd: TARGET_DIR, encoding: 'utf-8' }).trim();
        if (diff) {
            execSync(`git commit -m "feat(patrol): ${TASK.substring(0, 50)}"`, { cwd: TARGET_DIR });
            const shortDiff = diff.split('\n').pop();
            log('✅', `已提交: ${shortDiff}`);
            await tg(`📦 已提交: ${shortDiff}`);
        } else {
            log('ℹ️', '无文件变更');
            await tg('ℹ️ 无文件变更');
        }
    } catch {}

    // ===== 汇报 =====
    await tg(`🎉 *Patrol 完成*\n📋 ${TASK}\n📄 日志: \`.patrol-log.txt\``);
    log('🎉', '全部完成！');
    process.exit(0);
}

main();
