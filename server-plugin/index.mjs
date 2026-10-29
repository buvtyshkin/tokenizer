/**
 * Fisher Tokens — server plugin for SillyTavern.
 *
 * SillyTavern counts every Claude model with one frozen local tokenizer
 * (src/tokenizers/claude.json), which has not matched the real tokenizer since
 * Opus 4.7. This plugin asks Anthropic instead, through
 * /v1/messages/count_tokens, so the numbers the UI shows are the ones the model
 * actually sees. The API key never leaves the server.
 *
 * POST /api/plugins/fisher-tokens/count
 *   { model: "claude-opus-5-5", blocks: [{ key: "chat", text: "..." }, ...] }
 * ->{ counts: { chat: 41230, ... }, cached: 3, errors: { persona: "..." } }
 *
 * It also reads the NanoGPT account balance, for the same reason: the key stays
 * on the server and the browser only ever sees the numbers.
 *
 * POST /api/plugins/fisher-tokens/balance/nanogpt
 * ->{ usd: 14.31, spendable: 14.31, pending: 0, pendingStatus: "...", at: 1761734400000 }
 */
import crypto from 'node:crypto';
import { readSecret, SECRET_KEYS } from '../../src/endpoints/secrets.js';

export const info = {
    id: 'fisher-tokens',
    name: 'Fisher Tokens',
    description: 'Exact prompt-section token counts via Anthropic count_tokens, and the NanoGPT balance.',
};

const ENDPOINT = 'https://api.anthropic.com/v1/messages/count_tokens';
const API_VERSION = '2023-06-01';
const CACHE_MAX = 800;
const MAX_BLOCKS = 12;
const MAX_TEXT = 2_000_000;

/** sha256(model + text) -> token count. Insertion-ordered, trimmed from the front. */
const cache = new Map();
/** model -> per-message framing overhead, measured once. */
const overheadByModel = new Map();

function cacheKey(model, text) {
    return crypto.createHash('sha256').update(model).update('\0').update(text).digest('hex');
}

function cacheGet(key) {
    if (!cache.has(key)) return undefined;
    const value = cache.get(key);
    // Refresh recency so the trim below evicts genuinely cold entries.
    cache.delete(key);
    cache.set(key, value);
    return value;
}

function cacheSet(key, value) {
    cache.set(key, value);
    while (cache.size > CACHE_MAX) cache.delete(cache.keys().next().value);
}

const NANOGPT_BALANCE_URL = 'https://nano-gpt.com/api/check-balance';
/** Every open tab polls on its own; one upstream call a minute is plenty. */
const BALANCE_TTL_MS = 60_000;
let nanoBalanceCache = null;   // { at, body }

/** NanoGPT returns money as decimal strings and null for fields it does not know. */
function money(value) {
    if (value == null || value === '') return null;   // Number(null) is 0, not "unknown"
    const n = Number(value);
    return Number.isFinite(n) ? n : null;
}

async function fetchNanoBalance(apiKey) {
    const response = await fetch(NANOGPT_BALANCE_URL, {
        method: 'POST',   // GET answers 405
        headers: { 'x-api-key': apiKey, 'content-type': 'application/json' },
        signal: AbortSignal.timeout(15_000),
    });
    if (!response.ok) {
        const body = await response.text().catch(() => '');
        throw new Error(`check-balance ${response.status}: ${body.slice(0, 300)}`);
    }
    const data = await response.json();
    const usd = money(data?.usd_balance);
    if (usd === null) throw new Error('check-balance returned no usd_balance');
    return {
        usd,
        spendable: money(data?.usd_spendable_balance),
        pending: money(data?.usd_pending_usage),
        pendingStatus: typeof data?.usd_pending_usage_status === 'string' ? data.usd_pending_usage_status : null,
        at: Date.now(),
    };
}

async function callCountTokens(apiKey, model, text) {
    const response = await fetch(ENDPOINT, {
        method: 'POST',
        headers: {
            'content-type': 'application/json',
            'x-api-key': apiKey,
            'anthropic-version': API_VERSION,
        },
        body: JSON.stringify({ model, messages: [{ role: 'user', content: text }] }),
    });

    if (!response.ok) {
        const body = await response.text().catch(() => '');
        throw new Error(`count_tokens ${response.status}: ${body.slice(0, 300)}`);
    }

    const data = await response.json();
    if (typeof data?.input_tokens !== 'number') {
        throw new Error('count_tokens returned no input_tokens');
    }
    return data.input_tokens;
}

/**
 * Every count_tokens call wraps the text in a user message, which costs a few
 * tokens on its own. Measure that once per model against a known single-token
 * body so section counts are the section's own weight and sum up sensibly.
 */
async function getOverhead(apiKey, model) {
    if (overheadByModel.has(model)) return overheadByModel.get(model);
    const probe = await callCountTokens(apiKey, model, 'a');
    const overhead = Math.max(0, probe - 1);
    overheadByModel.set(model, overhead);
    return overhead;
}

async function countBlock(apiKey, model, text) {
    const key = cacheKey(model, text);
    const hit = cacheGet(key);
    if (hit !== undefined) return { tokens: hit, cached: true };

    const [raw, overhead] = await Promise.all([
        callCountTokens(apiKey, model, text),
        getOverhead(apiKey, model),
    ]);
    const tokens = Math.max(0, raw - overhead);
    cacheSet(key, tokens);
    return { tokens, cached: false };
}

export async function init(router) {
    router.post('/count', async (request, response) => {
        try {
            const model = String(request.body?.model || '').trim();
            const blocks = request.body?.blocks;

            if (!model) return response.status(400).json({ error: 'model is required' });
            if (!Array.isArray(blocks) || blocks.length === 0) {
                return response.status(400).json({ error: 'blocks must be a non-empty array' });
            }
            if (blocks.length > MAX_BLOCKS) {
                return response.status(400).json({ error: `at most ${MAX_BLOCKS} blocks per request` });
            }

            const apiKey = readSecret(request.user.directories, SECRET_KEYS.CLAUDE);
            if (!apiKey) {
                return response.status(400).json({ error: 'no Claude API key is stored in SillyTavern' });
            }

            const counts = {};
            const errors = {};
            let cachedCount = 0;

            // Sequential on purpose: a recount is at most a handful of blocks,
            // and firing them in parallel is a fast way to trip a rate limit.
            for (const block of blocks) {
                const key = String(block?.key || '');
                const text = String(block?.text ?? '');
                if (!key) continue;
                if (!text.trim()) { counts[key] = 0; continue; }
                if (text.length > MAX_TEXT) { errors[key] = 'block too large'; continue; }

                try {
                    const { tokens, cached } = await countBlock(apiKey, model, text);
                    counts[key] = tokens;
                    if (cached) cachedCount++;
                } catch (error) {
                    errors[key] = String(error.message || error).slice(0, 300);
                }
            }

            return response.json({ counts, cached: cachedCount, errors });
        } catch (error) {
            console.error('[fisher-tokens]', error);
            return response.status(500).json({ error: String(error.message || error) });
        }
    });

    router.post('/balance/nanogpt', async (request, response) => {
        try {
            const apiKey = readSecret(request.user.directories, SECRET_KEYS.NANOGPT);
            if (!apiKey) {
                return response.status(400).json({ error: 'no NanoGPT API key is stored in SillyTavern' });
            }
            const force = request.body?.force === true;
            if (!force && nanoBalanceCache && Date.now() - nanoBalanceCache.at < BALANCE_TTL_MS) {
                return response.json({ ...nanoBalanceCache.body, cached: true });
            }
            const body = await fetchNanoBalance(apiKey);
            nanoBalanceCache = { at: body.at, body };
            return response.json(body);
        } catch (error) {
            console.error('[fisher-tokens] nanogpt balance', error);
            return response.status(502).json({ error: String(error.message || error).slice(0, 300) });
        }
    });

    console.log('[fisher-tokens] ready — POST /api/plugins/fisher-tokens/count, /balance/nanogpt');
}

export async function exit() {
    cache.clear();
    nanoBalanceCache = null;
    overheadByModel.clear();
}

export default { info, init, exit };
