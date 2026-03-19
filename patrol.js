// patrol.js — AI 自治巡检 Demo
// 流程：你下令 → Codex 拆任务 → 你确认 → Codex 自己干+自检 → 统一汇报
//
// 用法：
//   node patrol.js                         # 巡检 codex_phone 项目
//   node patrol.js D:\some\project         # 巡检指定项目
//   node patrol.js . "重点看安全问题"       # 带自定义指令

import { execSync } from 'child_process';
import { createInterface } from 'readline';
import fs from 'fs';
import { dirname, join, basename } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));

// ===== 配置 =====
const TARGET_DIR = process.argv[2] || __dirname;
const USER_HINT = process.argv[3] || '';
const PLAN_FILE = join(TARGET_DIR, '.patrol-plan.json');
const REPORT_FILE = join(TARGET_DIR, '.patrol-report.md');

// ===== 工具函数 =====

function askCodex(prompt) {
    const tag = `[${new Date().toLocaleTimeString()}]`;
    console.log(`${tag} 🤖 Codex 工作中...`);
    try {
        return execSync(
            `codex exec -m gpt-5.4-mini "${prompt.replace(/"/g, '\\"')}"`,
            { cwd: TARGET_DIR, encoding: 'utf-8', timeout: 300000, stdio: ['ignore', 'pipe', 'pipe'], maxBuffer: 10 * 1024 * 1024 }
        ).trim();
    } catch (err) {
        return (err.stdout?.toString() || err.stderr?.toString() || err.message).trim();
    }
}

function askUser(prompt) {
    const rl = createInterface({ input: process.stdin, output: process.stdout });
    return new Promise(resolve => rl.question(prompt, a => { rl.close(); resolve(a.trim()); }));
}

function log(emoji, msg) {
    console.log(`\n${emoji}  ${msg}`);
}

function separator(title) {
    console.log(`\n${'═'.repeat(55)}`);
    console.log(`  ${title}`);
    console.log(`${'═'.repeat(55)}`);
}

// ===== 主流程 =====
async function main() {
    const projectName = basename(TARGET_DIR);
    separator(`🔍 AI 自治巡检 — ${projectName}`);
    console.log(`📂 目标: ${TARGET_DIR}`);
    if (USER_HINT) console.log(`💬 指令: ${USER_HINT}`);

    // ==============================
    // 阶段1: Codex 分析 + 拆解任务
    // ==============================
    separator('阶段1: 分析项目 → 拆解任务');
    log('⏳', 'Codex 正在扫描项目并拆解任务...');

    const extraHint = USER_HINT ? `\n用户特别要求: "${USER_HINT}"` : '';

    askCodex(`你是一个资深代码审查专家。请分析当前项目的所有源代码文件，找出问题和改进点。${extraHint}

然后将你的发现拆解成具体的修复任务，写入 .patrol-plan.json 文件，格式如下：
{
  "summary": "项目整体评价（一句话）",
  "tasks": [
    {
      "id": 1,
      "title": "修复xxx问题",
      "file": "涉及的文件",
      "severity": "高/中/低",
      "description": "具体要做什么",
      "verification": "如何验证修复是否成功"
    }
  ]
}

要求：
- 每个任务要足够具体，一个 codex exec 调用就能完成
- 按严重程度从高到低排列
- 只列出真正需要修改的问题，不要列无关紧要的风格建议
- 如果项目没有问题，tasks 数组为空，summary 写"项目健康，无需修改"
`);

    // 读取计划
    let plan;
    try {
        plan = JSON.parse(fs.readFileSync(PLAN_FILE, 'utf-8'));
    } catch {
        log('❌', '未能生成任务计划文件，退出');
        process.exit(1);
    }

    // 展示计划
    separator('任务计划');
    console.log(`\n📊 总评: ${plan.summary}`);
    console.log(`📋 共 ${plan.tasks?.length || 0} 个任务:\n`);

    if (!plan.tasks || plan.tasks.length === 0) {
        log('✅', '项目健康，无需修改。巡检结束！');
        process.exit(0);
    }

    plan.tasks.forEach((t, i) => {
        const icon = t.severity === '高' ? '🔴' : t.severity === '中' ? '🟡' : '🟢';
        console.log(`  ${i + 1}. ${icon} [${t.severity}] ${t.title}`);
        console.log(`     📁 ${t.file}`);
        console.log(`     📝 ${t.description}`);
        console.log();
    });

    // ==============================
    // 阶段2: 用户确认计划
    // ==============================
    console.log('💬 选项:');
    console.log('   回车/确认  → 全部执行');
    console.log('   1,3,5     → 只执行指定编号的任务');
    console.log('   你的意见   → Codex 参考后调整');
    console.log('   停止       → 取消\n');

    const reply = await askUser('👤 你的决定 > ');

    if (reply === '停止' || reply === 'q') {
        log('🛑', '已取消');
        process.exit(0);
    }

    // 解析要执行的任务
    let tasksToRun = plan.tasks;
    if (/^[\d,\s]+$/.test(reply)) {
        const ids = reply.split(',').map(s => parseInt(s.trim())).filter(n => !isNaN(n));
        tasksToRun = plan.tasks.filter(t => ids.includes(t.id));
        log('📌', `将执行 ${tasksToRun.length} 个选中任务`);
    } else if (reply && reply !== '确认' && reply !== 'y' && reply !== '') {
        // 用户有额外意见，让 Codex 重新考虑
        log('🔄', 'Codex 正在根据你的意见调整计划...');
        askCodex(`阅读 .patrol-plan.json 中的任务计划，用户给出了以下反馈：
"${reply}"
请根据用户反馈调整任务计划，更新 .patrol-plan.json 文件。`);
        try {
            plan = JSON.parse(fs.readFileSync(PLAN_FILE, 'utf-8'));
            tasksToRun = plan.tasks || [];
            log('📋', `调整后共 ${tasksToRun.length} 个任务`);
        } catch {
            log('⚠️', '计划更新失败，使用原计划继续');
        }
    }

    // ==============================
    // 阶段3: Codex 自动逐项执行
    // ==============================
    separator('阶段3: 自动执行');
    const results = [];

    for (let i = 0; i < tasksToRun.length; i++) {
        const task = tasksToRun[i];
        log('🔧', `任务 ${i + 1}/${tasksToRun.length}: ${task.title}`);

        // 执行修复
        const output = askCodex(`请执行以下修复任务：
任务: ${task.title}
文件: ${task.file}
要求: ${task.description}

注意：
- 只修改必要的部分
- 确保修改后代码能正常运行
- 不要修改不相关的文件`);

        // 自验证
        log('🔍', `自验证: ${task.verification}`);
        const verify = askCodex(`刚才执行了修复任务"${task.title}"。
请验证修复是否成功：${task.verification}
只回答"通过"或"未通过: 原因"，不要做其他修改。`);

        const passed = verify.includes('通过') && !verify.includes('未通过');
        results.push({ task, output: output.substring(0, 500), passed, verify });

        console.log(`   ${passed ? '✅ 验证通过' : '⚠️ 验证: ' + verify.substring(0, 100)}`);
    }

    // 阶段3.5: 统一 git 提交
    log('📦', '统一提交修改...');
    askCodex(`请检查当前已修改但尚未提交的文件，将所有修改用一条 git commit 提交。
commit message: "fix: patrol auto-fix ${tasksToRun.length} issues"
如果没有未提交的修改则跳过。`);

    // ==============================
    // 阶段4: 生成最终报告
    // ==============================
    separator('阶段4: 最终报告');

    const passed = results.filter(r => r.passed).length;
    const failed = results.filter(r => !r.passed).length;

    // 写入报告文件
    let reportContent = `# 巡检报告 — ${projectName}\n`;
    reportContent += `> ${new Date().toLocaleString()}\n\n`;
    reportContent += `## 总览\n`;
    reportContent += `- 总评: ${plan.summary}\n`;
    reportContent += `- 任务数: ${tasksToRun.length}\n`;
    reportContent += `- ✅ 通过: ${passed}\n`;
    reportContent += `- ⚠️ 待确认: ${failed}\n\n`;
    reportContent += `## 任务详情\n\n`;

    results.forEach((r, i) => {
        reportContent += `### ${i + 1}. ${r.passed ? '✅' : '⚠️'} ${r.task.title}\n`;
        reportContent += `- 文件: ${r.task.file}\n`;
        reportContent += `- 验证: ${r.verify.substring(0, 200)}\n\n`;
    });

    fs.writeFileSync(REPORT_FILE, reportContent, 'utf-8');

    // 在终端展示
    console.log(`\n📊 总评: ${plan.summary}`);
    console.log(`📋 执行: ${tasksToRun.length} 个任务`);
    console.log(`✅ 通过: ${passed}  ⚠️ 待确认: ${failed}\n`);

    results.forEach((r, i) => {
        console.log(`  ${r.passed ? '✅' : '⚠️'} ${i + 1}. ${r.task.title}`);
    });

    console.log(`\n📄 完整报告: ${REPORT_FILE}`);
    separator('巡检完成 🎉');

    process.exit(0);
}

main();
