-- NvRouter canonicalizes Enter model IDs without upstream vendor prefixes.
-- Public aliases route each canonical ID to Enter while keeping the provider private.
INSERT INTO model_aliases (alias, target) VALUES
('gpt-5.6-sol','enter-converge/gpt-5.6-sol'),
('gpt-5.6-terra','enter-converge/gpt-5.6-terra'),
('gpt-5.6-luna','enter-converge/gpt-5.6-luna'),
('gpt-5.5','enter-converge/gpt-5.5'),
('gpt-5.4-pro','enter-converge/gpt-5.4-pro'),
('gpt-5.4','enter-converge/gpt-5.4'),
('gpt-5.2-pro','enter-converge/gpt-5.2-pro'),
('claude-opus-4.6','enter-converge/claude-opus-4.6'),
('claude-sonnet-4.5','enter-converge/claude-sonnet-4.5'),
('minimax-m3','enter-converge/minimax-m3'),
('minimax-m2.7','enter-converge/minimax-m2.7'),
('minimax-m2.5','enter-converge/minimax-m2.5'),
('deepseek-v4-pro','enter-converge/deepseek-v4-pro'),
('qwen-3.7-plus','enter-converge/qwen-3.7-plus'),
('qwen-3.7-max','enter-converge/qwen-3.7-max'),
('qwen-3.6-plus','enter-converge/qwen-3.6-plus'),
('qwen-3.6-max-preview','enter-converge/qwen-3.6-max-preview'),
('kimi-k3','enter-converge/kimi-k3'),
('kimi-k2.7-code','enter-converge/kimi-k2.7-code'),
('kimi-k2.6','enter-converge/kimi-k2.6'),
('kimi-k2.5','enter-converge/kimi-k2.5'),
('glm-5.2','enter-converge/glm-5.2'),
('glm-5.1','enter-converge/glm-5.1'),
('glm-5','enter-converge/glm-5')
ON CONFLICT (alias) DO NOTHING;

-- Merge known native Enter cooldowns into canonical IDs, preserving the later expiry.
INSERT INTO model_cooldowns (id, account_id, model, cooldown_until, created_at)
SELECT 'enter-canonical-' || mc.id, mc.account_id,
CASE mc.model
 WHEN 'openai/gpt-5.6-sol' THEN 'gpt-5.6-sol' WHEN 'openai/gpt-5.6-terra' THEN 'gpt-5.6-terra' WHEN 'openai/gpt-5.6-luna' THEN 'gpt-5.6-luna'
 WHEN 'openai/gpt-5.5' THEN 'gpt-5.5' WHEN 'openai/gpt-5.4-pro' THEN 'gpt-5.4-pro' WHEN 'openai/gpt-5.4' THEN 'gpt-5.4' WHEN 'openai/gpt-5.2-pro' THEN 'gpt-5.2-pro'
 WHEN 'anthropic/claude-opus-4.6' THEN 'claude-opus-4.6' WHEN 'anthropic/claude-sonnet-4.5' THEN 'claude-sonnet-4.5'
 WHEN 'minimax/minimax-m3' THEN 'minimax-m3' WHEN 'minimax/minimax-m2.7' THEN 'minimax-m2.7' WHEN 'minimax/minimax-m2.5' THEN 'minimax-m2.5'
 WHEN 'deepseek/deepseek-v4-pro' THEN 'deepseek-v4-pro'
 WHEN 'alibaba/qwen-3.7-plus' THEN 'qwen-3.7-plus' WHEN 'alibaba/qwen-3.7-max' THEN 'qwen-3.7-max' WHEN 'alibaba/qwen-3.6-plus' THEN 'qwen-3.6-plus' WHEN 'alibaba/qwen-3.6-max-preview' THEN 'qwen-3.6-max-preview'
 WHEN 'moonshotai/kimi-k3' THEN 'kimi-k3' WHEN 'moonshotai/kimi-k2.7-code' THEN 'kimi-k2.7-code' WHEN 'moonshotai/kimi-k2.6' THEN 'kimi-k2.6' WHEN 'moonshotai/kimi-k2.5' THEN 'kimi-k2.5'
 WHEN 'z-ai/glm-5.2' THEN 'glm-5.2' WHEN 'z-ai/glm-5.1' THEN 'glm-5.1' WHEN 'z-ai/glm-5' THEN 'glm-5'
END, mc.cooldown_until, mc.created_at
FROM model_cooldowns mc JOIN accounts a ON a.id=mc.account_id
WHERE a.provider='enter-converge' AND mc.model IN (
'openai/gpt-5.6-sol','openai/gpt-5.6-terra','openai/gpt-5.6-luna','openai/gpt-5.5','openai/gpt-5.4-pro','openai/gpt-5.4','openai/gpt-5.2-pro',
'anthropic/claude-opus-4.6','anthropic/claude-sonnet-4.5','minimax/minimax-m3','minimax/minimax-m2.7','minimax/minimax-m2.5','deepseek/deepseek-v4-pro',
'alibaba/qwen-3.7-plus','alibaba/qwen-3.7-max','alibaba/qwen-3.6-plus','alibaba/qwen-3.6-max-preview',
'moonshotai/kimi-k3','moonshotai/kimi-k2.7-code','moonshotai/kimi-k2.6','moonshotai/kimi-k2.5','z-ai/glm-5.2','z-ai/glm-5.1','z-ai/glm-5')
ON CONFLICT (account_id, model) DO UPDATE SET
 cooldown_until=CASE WHEN excluded.cooldown_until > model_cooldowns.cooldown_until THEN excluded.cooldown_until ELSE model_cooldowns.cooldown_until END;

DELETE FROM model_cooldowns
WHERE id IN (
 SELECT mc.id FROM model_cooldowns mc JOIN accounts a ON a.id=mc.account_id
 WHERE a.provider='enter-converge'
 AND mc.model IN ('openai/gpt-5.6-sol','openai/gpt-5.6-terra','openai/gpt-5.6-luna','openai/gpt-5.5','openai/gpt-5.4-pro','openai/gpt-5.4','openai/gpt-5.2-pro','anthropic/claude-opus-4.6','anthropic/claude-sonnet-4.5','minimax/minimax-m3','minimax/minimax-m2.7','minimax/minimax-m2.5','deepseek/deepseek-v4-pro','alibaba/qwen-3.7-plus','alibaba/qwen-3.7-max','alibaba/qwen-3.6-plus','alibaba/qwen-3.6-max-preview','moonshotai/kimi-k3','moonshotai/kimi-k2.7-code','moonshotai/kimi-k2.6','moonshotai/kimi-k2.5','z-ai/glm-5.2','z-ai/glm-5.1','z-ai/glm-5')
);
