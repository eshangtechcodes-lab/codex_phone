// patrol.js — AI 巡检循环 Demo
// 用 Codex CLI 对项目进行循环巡检：分析→报告→等你确认→继续
//
// 用法：
//   node patrol.js                    # 巡检当前目录（codex_phone）
//   node patrol.js D:\some\project    # 巡检指定项目

import { execSync } from 'child_process';
import { createInterface } from 'readline';
import fs from 'fs';
import { dirname, join, basename } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));

// ===== 配置 =====
const TARGET_DIR = process.argv[2] || __dirname;
const REPORT_FILE = join(TARGET_DIR, '.patrol-report.md');
const MAX_ROUNDS = 10;

// ===== 工具函数 =====

// 调用 Codex CLI（非交互模式）
function askCodex(prompt) {
    console.log(`\n🤖 Codex 正在工作...\n`);
    try {
        const result = execSync(
            `codex exec -m gpt-5.4-mini "${prompt.replace(/"/g, '\\"')}"`,
            {
                cwd: TARGET_DIR,
                encoding: 'utf-8',
                timeout: 300000,
                stdio: ['ignore', 'pipe', 'pipe'],
                maxBuffer: 10 * 1024 * 1024
            }
        );
        return result.trim();
    } catch (err) {
        return (err.stdout?.toString() || err.stderr?.toString() || err.message).trim();
    }
}

// 读取报告
function readReport() {
    try {
        return fs.readFileSync(REPORT_FILE, 'utf-8').trim();
    } catch {
        return '';
    }
}

// 判断报告中是否还有问题
function hasIssues(report) {
    if (!report) return false;
    const noIssue = ['无问题', '无异常', '没有发现问题', '0 issues', 'no issues', '全部通过', '无需修改', '所有问题都已修复'];
    return !noIssue.some(p => report.toLowerCase().includes(p.toLowerCase()));
}

// 等待用户输入
function askUser(prompt) {
    const rl = createInterface({ input: process.stdin, output: process.stdout });
    return new Promise(resolve => {
        rl.question(prompt, answer => {
            rl.close();
            resolve(answer.trim());
        });
    });
}

// 分隔线
function separator(title) {
    console.log(`\n${'═'.repeat(50)}`);
    console.log(`  ${title}`);
    console.log(`${'═'.repeat(50)}`);
}

// ===== 核心巡检循环 =====
async function runPatrol() {
    const projectName = basename(TARGET_DIR);
    separator(`🔍 AI 巡检 Demo — ${projectName}`);
    console.log(`📂 目标目录: ${TARGET_DIR}`);
    console.log(`📄 报告文件: ${REPORT_FILE}`);
    console.log(`🔄 最大轮数: ${MAX_ROUNDS}`);

    let round = 0;

    while (round < MAX_ROUNDS) {
        round++;
        separator(`📋 第 ${round} 轮巡检`);

        // ===== 阶段1: Codex 扫描项目 =====
        console.log('⏳ 阶段1: Codex 扫描代码...');

        const scanPrompt = round === 1
            ? `你是一个资深代码审查专家。请仔细分析当前项目的所有源代码文件（特别是 server.js, public/js/app.js, public/css/style.css, public/sw.js），找出以下方面的问题：
1. 安全隐患（硬编码密钥、敏感信息暴露、注入风险等）
2. 代码质量（错误处理不当、内存泄漏风险、性能问题）
3. 健壮性（边界条件、异常路径、超时处理）
4. 最佳实践（代码组织、可维护性建议）

请将分析报告写入文件 .patrol-report.md，使用以下格式：
# 第${round}轮巡检报告
## 发现问题
列出所有问题，按严重程度排序（高→低），每个问题包含：
- 问题描述
- 所在文件和位置
- 严重程度（🔴高 / 🟡中 / 🟢低）
- 建议修改方案
## 总结
统计问题数量和整体评价。
如果没有发现任何问题，请在报告中写"无问题，全部通过"。`
            : `请阅读上一轮的巡检报告 .patrol-report.md，检查之前提到的问题是否已被修复。
然后对项目进行新一轮全面分析，找出：
1. 之前的问题是否彻底修复（有没有引入新 bug）
2. 是否有新发现的问题

将新的报告覆盖写入 .patrol-report.md，格式同上。
如果所有问题都已修复且无新问题，请写"无问题，全部通过"。`;

        askCodex(scanPrompt);

        // ===== 阶段2: 读取并展示报告 =====
        const report = readReport();

        if (!report) {
            console.log('\n❌ 未能生成报告文件，巡检中断');
            break;
        }

        console.log('\n' + '─'.repeat(50));
        console.log(report);
        console.log('─'.repeat(50));

        // ===== 阶段3: 判断是否还有问题 =====
        if (!hasIssues(report)) {
            separator('✅ 巡检完成 — 项目健康');
            console.log('所有问题已修复，无新问题。');
            console.log(`共进行了 ${round} 轮巡检 🎉\n`);
            break;
        }

        // ===== 阶段4: 等待用户指令 =====
        console.log('\n💬 选项:');
        console.log('   输入 "继续"    → Codex 自动修复上述问题');
        console.log('   输入 你的意见  → 传递给 Codex 参考后修复');
        console.log('   输入 "停止"    → 结束巡检');
        console.log('   输入 "跳过"    → 不修复，直接进入下一轮扫描\n');

        const userReply = await askUser('👤 你的指令 > ');

        if (userReply === '停止' || userReply === 'stop' || userReply === 'q') {
            separator('🛑 巡检已停止');
            console.log(`共进行了 ${round} 轮\n`);
            break;
        }

        if (userReply === '跳过' || userReply === 'skip') {
            console.log('⏭  跳过修复，进入下一轮...');
            continue;
        }

        // ===== 阶段5: Codex 执行修改 =====
        console.log('\n⏳ 阶段5: Codex 正在修复...');

        const fixPrompt = (userReply === '继续' || userReply === 'y' || userReply === '')
            ? `请阅读 .patrol-report.md 中列出的问题，按照报告中的建议逐个修复代码。
注意事项：
- 只修改有问题的部分，不要大范围重构
- 修复后确保代码仍然能正常运行
- 修复完成后用 git 提交，commit message: "fix: patrol round ${round} - 修复巡检发现的问题"`
            : `请阅读 .patrol-report.md 中的问题，并参考用户的额外意见进行修改：

用户意见: "${userReply}"

修复完成后用 git 提交，commit message: "fix: patrol round ${round} - ${userReply.substring(0, 40)}"`;

        const fixResult = askCodex(fixPrompt);
        console.log('\n📝 修复结果:');
        console.log(fixResult.substring(0, 2000));

        console.log('\n🔄 即将进入下一轮巡检...');
        await new Promise(r => setTimeout(r, 1000));
    }

    if (round >= MAX_ROUNDS) {
        console.log(`\n⚠️ 已达最大轮数 (${MAX_ROUNDS})，巡检结束`);
    }

    // 清理报告文件
    // 保留最终报告不删除，方便查看
    console.log(`\n📄 最终报告: ${REPORT_FILE}`);
    process.exit(0);
}

// ===== 启动 =====
runPatrol();
