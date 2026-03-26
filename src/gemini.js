/**
 * @module gemini
 * @description Gemini CLI 调用封装
 *
 * 通过 spawn 调用本地 gemini CLI，支持指定模型和 JSON 输出。
 */

import { spawn } from 'child_process';

/** @type {string} 当前 Gemini 模型 */
let geminiModel = 'gemini-2.5-pro';

/**
 * 设置 Gemini 模型
 * @param {string} model - 模型名称
 */
export function setGeminiModel(model) {
    geminiModel = model;
}

/**
 * 获取当前 Gemini 模型
 * @returns {string}
 */
export function getGeminiModel() {
    return geminiModel;
}

/**
 * 调用 Gemini CLI 完成一次对话
 * @param {string} message - 用户消息（含上下文）
 * @returns {Promise<string>} AI 回复文本
 */
export function geminiChat(message) {
    return new Promise((resolve, reject) => {
        const proc = spawn('gemini', ['-p', message, '--model', geminiModel, '--output-format', 'json'], {
            shell: true
        });
        let stdout = '';
        let stderr = '';
        proc.stdout.on('data', d => { stdout += d.toString(); });
        proc.stderr.on('data', d => { stderr += d.toString(); });
        proc.on('close', (code) => {
            if (code !== 0) {
                return reject(new Error(stderr || `Gemini exited with code ${code}`));
            }
            try {
                const data = JSON.parse(stdout);
                resolve(data.response || '(empty)');
            } catch {
                // JSON 解析失败，直接返回原始输出
                resolve(stdout.trim() || '(empty)');
            }
        });
        proc.on('error', reject);
    });
}
