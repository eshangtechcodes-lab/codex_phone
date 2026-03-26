/**
 * @module api
 * @description OpenAI 兼容 REST API 路由
 *
 * 提供：
 * - POST /v1/chat/completions — 标准 OpenAI 聊天接口
 * - GET  /v1/models           — 可用模型列表
 * - POST /api/chat            — 简化版聊天接口
 * - GET  /health              — 健康检查
 */

import { codexChat, isCodexAlive } from './codex.js';

/**
 * 注册所有 API 路由到 Express app
 * @param {import('express').Application} app - Express 实例
 */
export function setupApiRoutes(app) {

    // OpenAI 兼容：chat completions
    app.post('/v1/chat/completions', async (req, res) => {
        const { model = 'gpt-5.4', messages = [] } = req.body;

        const userMsg = messages.filter(m => m.role === 'user').pop();
        if (!userMsg) {
            return res.status(400).json({
                error: { message: 'At least one user message is required', type: 'invalid_request_error' }
            });
        }

        // 合成完整 prompt（system + 多轮历史 + 当前消息）
        const systemMsg = messages.find(m => m.role === 'system');
        let prompt = '';
        if (systemMsg) prompt += `[System: ${systemMsg.content}]\n\n`;

        const history = messages.filter(m => m.role !== 'system');
        if (history.length > 1) {
            history.slice(0, -1).forEach(m => {
                prompt += `${m.role === 'user' ? 'User' : 'Assistant'}: ${m.content}\n`;
            });
            prompt += '\n';
        }
        prompt += userMsg.content;

        console.log(`[API] /v1/chat/completions model=${model} msg="${userMsg.content.substring(0, 50)}"`);

        try {
            const result = await codexChat(prompt, model);
            res.json({
                id: `chatcmpl-${Date.now()}`,
                object: 'chat.completion',
                created: Math.floor(Date.now() / 1000),
                model,
                choices: [{
                    index: 0,
                    message: { role: 'assistant', content: result.reply },
                    finish_reason: 'stop'
                }],
                usage: { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 },
                _codex: { threadId: result.threadId }
            });
        } catch (err) {
            console.error('[API] Error:', err.message);
            res.status(500).json({
                error: { message: err.message, type: 'server_error' }
            });
        }
    });

    // OpenAI 兼容：模型列表
    app.get('/v1/models', (req, res) => {
        const models = [
            { id: 'gpt-5.4', owned_by: 'openai' },
            { id: 'gpt-5.4-mini', owned_by: 'openai' },
            { id: 'gpt-5.3-codex', owned_by: 'openai' },
            { id: 'gpt-5.2-codex', owned_by: 'openai' },
            { id: 'gpt-5.2', owned_by: 'openai' },
            { id: 'gpt-5.1-codex-max', owned_by: 'openai' },
            { id: 'gpt-5.1-codex-mini', owned_by: 'openai' },
        ];
        res.json({
            object: 'list',
            data: models.map(m => ({ ...m, object: 'model', created: 0 }))
        });
    });

    // 简化版聊天接口
    app.post('/api/chat', async (req, res) => {
        const { message, model = 'gpt-5.4', threadId: existingThreadId } = req.body;
        if (!message) return res.status(400).json({ error: 'message is required' });
        console.log(`[API] /api/chat "${message.substring(0, 50)}" model=${model}`);
        try {
            const result = await codexChat(message, model, existingThreadId);
            res.json(result);
        } catch (err) {
            res.status(500).json({ error: err.message });
        }
    });

    // 健康检查
    app.get('/health', (req, res) => {
        res.json({ status: 'ok', codex: isCodexAlive() });
    });
}
