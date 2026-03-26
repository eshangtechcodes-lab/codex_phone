/**
 * @module memory
 * @description 记忆系统 — 自动提取对话要点，跨会话持久化存储
 *
 * 职责：
 * - 管理对话历史（按用户隔离）
 * - 加载/保存分类记忆文件（profile/projects/servers/skills/notes）
 * - 检测"记下来""总结一下"等触发词，调 AI 提取并去重合并
 */

import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'fs';
import { join } from 'path';

// 记忆存储目录
const MEMORY_DIR = join(process.env.USERPROFILE || process.env.HOME, '.codex_phone', 'memory');
const MEMORY_CATEGORIES = ['profile', 'projects', 'servers', 'skills', 'notes'];
if (!existsSync(MEMORY_DIR)) { mkdirSync(MEMORY_DIR, { recursive: true }); }

// 对话历史（内存中，按用户 ID 隔离）
const chatHistory = new Map();
const MAX_HISTORY = 50;

/**
 * 添加一条对话记录
 * @param {string|number} userId - 用户 ID
 * @param {string} role - 角色（'用户' 或 'AI'）
 * @param {string} content - 消息内容（截取前 500 字符）
 */
export function addHistory(userId, role, content) {
    if (!chatHistory.has(userId)) chatHistory.set(userId, []);
    const h = chatHistory.get(userId);
    h.push({ role, content: content.substring(0, 500) });
    if (h.length > MAX_HISTORY) h.shift();
}

/**
 * 加载所有类别的记忆，拼成 Markdown 格式
 * @returns {string} 合并后的记忆文本
 */
export function loadMemory() {
    let memory = '';
    for (const cat of MEMORY_CATEGORIES) {
        const f = join(MEMORY_DIR, `${cat}.md`);
        if (existsSync(f)) {
            const content = readFileSync(f, 'utf-8').trim();
            if (content) memory += `\n### ${cat}\n${content}\n`;
        }
    }
    return memory;
}

// 记忆触发词
const MEMORY_TRIGGERS = ['总结一下', '记下来', '记住这些', '保存一下', '记录一下', '总结对话', '帮我记住', 'save this', 'remember this'];

/**
 * 检测文本是否包含记忆触发词
 * @param {string} text - 用户消息
 * @returns {boolean}
 */
export function isMemoryTrigger(text) {
    return MEMORY_TRIGGERS.some(t => text.includes(t));
}

/**
 * 从对话历史中提取有价值的信息并保存到记忆文件
 * 调用 Gemini 做智能去重/合并，覆盖写入各类别文件
 * @param {string|number} userId - 用户 ID
 * @param {Function} aiChat - AI 调用函数（传入 geminiChat）
 * @returns {Promise<string>} 操作结果文本
 */
export async function extractAndSave(userId, aiChat) {
    const history = chatHistory.get(userId);
    if (!history || history.length < 2) return '没有足够的对话可以总结。';
    const conversation = history.map(h => `${h.role}: ${h.content}`).join('\n');

    // 读取已有记忆，传给 AI 做去重/更新
    const existingMemory = loadMemory();
    const existingSection = existingMemory.trim()
        ? `\n已有记忆（请在此基础上更新，不要重复）:\n${existingMemory}\n`
        : '';

    const prompt = `你是记忆管理助手。从对话中提取有价值的信息，并与已有记忆合并。

规则:
1. 新信息: 直接添加
2. 重复信息: 跳过，不重复记录
3. 冲突信息: 用新的覆盖旧的（如邮箱变了）
4. 每条记忆用一行"- 内容"格式

类别: profile(个人信息), projects(项目), servers(服务器), skills(技能流程), notes(其他)
${existingSection}
对话:
${conversation}

输出JSON，每个类别的值是合并后的完整内容（多条用\\n分隔），没有变化的类别不输出:
{"profile": "- xxx\\n- yyy", "servers": "- zzz"}`;

    try {
        const result = await aiChat(prompt);
        const jsonMatch = result.match(/\{[\s\S]*\}/);
        if (!jsonMatch) return '未提取到有价值的信息。';
        const extracted = JSON.parse(jsonMatch[0]);
        let saved = [];
        for (const [cat, content] of Object.entries(extracted)) {
            if (!MEMORY_CATEGORIES.includes(cat) || !content || content === '无') continue;
            const f = join(MEMORY_DIR, `${cat}.md`);
            writeFileSync(f, content.replace(/\\n/g, '\n'), 'utf-8');
            saved.push(cat);
        }
        if (saved.length === 0) return '这段对话没有需要记忆的新信息。';
        return `✅ 已更新记忆！${saved.map(s => `*${s}*`).join(', ')}`;
    } catch (e) {
        console.log('[Memory] 提取失败:', e.message);
        return '❌ 记忆提取失败: ' + e.message;
    }
}

/**
 * 清空所有记忆文件
 */
export function clearMemory() {
    for (const cat of MEMORY_CATEGORIES) {
        const f = join(MEMORY_DIR, `${cat}.md`);
        if (existsSync(f)) writeFileSync(f, '', 'utf-8');
    }
}

export { chatHistory, MEMORY_DIR, MEMORY_CATEGORIES };
