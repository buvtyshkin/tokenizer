// Fisher Cost — точные токены и стоимость каждого ответа из `usage` API,
// как в fisher-frontend. Перехватывает стрим /api/backends/chat-completions/generate,
// раздваивает его (Таверна получает оригинал), читает usage, привязывает к сообщению,
// фиксирует стоимость в момент генерации и ведёт журнал расходов по чатам.

const MODULE = 'fisher_cost';
const LOG = '[Fisher Cost]';
const GENERATE_URL = '/api/backends/chat-completions/generate';
const EXTRA_KEY = 'fisher_cost';
const RAW_KEEP_MS = 48 * 3600 * 1000;      // сырые записи журнала — двое суток
const DAILY_KEEP_DAYS = 120;               // дневные агрегаты — четыре месяца
const ATTACH_WAIT_MS = 2500;               // сколько ждём MESSAGE_RECEIVED после конца стрима
const REFRESH_MAX_TOKENS = 8;              // non-stream запрос с max_tokens не больше этого — пинг кэш-рефрешера

const DEFAULTS = {
    showUnderMessage: true,
    showTopbar: true,          // строка с разбивкой промпта над чатом
    showMessageTokens: true,   // счётчик токенов у каждого сообщения
    compactLine: 'auto',       // 'auto' — компактно на узких экранах, 'always', 'never'
    injectUsageOption: true,   // Custom-источник: добавлять stream_options.include_usage
    defaultTtl: '1h',          // если API не прислал разбивку cache_creation по TTL
    pricingOverride: '',       // JSON поверх pricing.json
    raw: [],                   // [{t, chat, model, kind, in, out, cr, cw5, cw1, cost, priced}]
    daily: {},                 // { 'YYYY-MM-DD|chat|model': {n, in, out, cr, cw, cost, svc, svcCost, priced} }
};

let ctx = null;
let settings = null;
let pricing = {};
let pendingUsage = null;          // usage, уже прочитанный из стрима, ждёт своё сообщение
let pendingTimer = null;
let awaitingMessageId = null;     // MESSAGE_RECEIVED пришёл раньше, чем стрим дочитан
let currentGenType = null;
let sessionStart = Date.now();
let lastNonStreamWarn = 0;
let lastPromptByChat = {};        // { chatLabel: {model, in, cr, cw} } — размер последнего промпта, для оценки пингов

// ---------- утилиты ----------

function fmtInt(n) {
    return Number(n || 0).toLocaleString('ru-RU');
}

function fmtUsd(v) {
    if (v == null || Number.isNaN(v)) return '—';
    if (v === 0) return '$0';
    if (v < 0.01) return '$' + v.toFixed(4);
    if (v < 1) return '$' + v.toFixed(3);
    return '$' + v.toFixed(2);
}

function dayKey(t) {
    const d = new Date(t);
    const p = (x) => String(x).padStart(2, '0');
    return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())}`;
}

function startOfToday() {
    const d = new Date();
    d.setHours(0, 0, 0, 0);
    return d.getTime();
}

function escapeHtml(s) {
    return String(s ?? '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', '\'': '&#39;' }[c]));
}

function chatLabel() {
    try {
        const c = SillyTavern.getContext();
        if (c.groupId) {
            const g = (c.groups || []).find((x) => x.id === c.groupId);
            return `${g?.name || 'группа'} / ${c.chatId || '?'}`;
        }
        return `${c.name2 || 'персонаж'} / ${c.chatId || '?'}`;
    } catch {
        return '?';
    }
}

// ---------- цены ----------

async function loadPricing() {
    let base = {};
    try {
        const url = new URL('./pricing.json', import.meta.url);
        url.searchParams.set('v', String(Date.now()));
        const r = await fetch(url);
        if (r.ok) base = await r.json();
    } catch (e) {
        console.warn(`${LOG} не удалось загрузить pricing.json`, e);
    }
    let override = {};
    if (settings.pricingOverride?.trim()) {
        try {
            override = JSON.parse(settings.pricingOverride);
        } catch (e) {
            toastr.warning('Fisher Cost: JSON цен не разбирается, использую pricing.json');
        }
    }
    pricing = { ...base, ...override };
    for (const k of Object.keys(pricing)) if (k.startsWith('_')) delete pricing[k];
}

function findPrice(model) {
    if (!model) return null;
    const m = String(model).toLowerCase();
    let best = null;
    for (const key of Object.keys(pricing)) {
        const k = key.toLowerCase();
        if (!m.startsWith(k)) continue;
        // Хвост после совпадения допустим только датированный: claude-opus-4-5
        // покрывает claude-opus-4-5-20251101, но claude-opus-5 не должен молча
        // отдавать свои цены модели claude-opus-5-5. Лучше честное «цена?»,
        // чем правдоподобное, но неверное число.
        const tail = m.slice(k.length);
        if (tail && !/^[-@]\d{6,8}$/.test(tail)) continue;
        if (!best || k.length > best.length) best = k;
    }
    if (!best) return null;
    return pricing[Object.keys(pricing).find((k) => k.toLowerCase() === best)];
}

/** Пиковые часы провайдера (DeepSeek): peak_hours_utc: [[1,4],[6,10]], offpeak_factor: 0.5 */
function isPeakNow(p) {
    if (!p.peak_hours_utc) return true;
    const d = new Date();
    if (p.peak_weekdays_only && (d.getUTCDay() === 0 || d.getUTCDay() === 6)) return false;
    const h = d.getUTCHours() + d.getUTCMinutes() / 60;
    return p.peak_hours_utc.some(([a, b]) => h >= a && h < b);
}

/** Стоимость фиксируется здесь один раз, при генерации, и больше не пересчитывается. */
function computeCost(u) {
    const p = findPrice(u.model);
    if (!p) return { cost: null, priced: false };
    let cw5 = u.cw5, cw1 = u.cw1;
    if (cw5 == null && cw1 == null) {
        if (settings.defaultTtl === '1h') { cw1 = u.cw; cw5 = 0; } else { cw5 = u.cw; cw1 = 0; }
    }
    // длинный контекст (Gemini Pro, OpenAI, xAI): весь запрос по повышенному тарифу
    const promptTotal = (u.in || 0) + (u.cr || 0) + (u.cw || 0);
    const long = p.long_threshold && promptTotal > p.long_threshold;
    const rIn = long ? (p.input_long ?? p.input) : p.input;
    const rOut = long ? (p.output_long ?? p.output) : p.output;
    const rCr = long ? (p.cache_read_long ?? p.cache_read) : p.cache_read;
    const factor = isPeakNow(p) ? 1 : (p.offpeak_factor ?? 1);
    const cost =
        (u.in || 0) * (rIn || 0) +
        (u.out || 0) * (rOut || 0) +
        (u.cr || 0) * (rCr || 0) +
        (cw5 || 0) * (p.cache_write_5m ?? p.cache_write ?? 0) +
        (cw1 || 0) * (p.cache_write_1h ?? p.cache_write ?? 0);
    return { cost: (cost * factor) / 1e6, priced: true, long: !!long, offpeak: factor !== 1 };
}

// ---------- разбор стрима ----------

function newUsage(meta) {
    return { model: meta.model || null, in: 0, out: 0, cr: 0, cw: 0, cw5: null, cw1: null, think: 0, seen: false };
}

/** Один чанк стрима или целый JSON-ответ → поля usage. Общий для стрима и non-stream. */
function applyUsage(u, obj) {
    if (!obj || typeof obj !== 'object') return;
    // Anthropic
    if (obj.type === 'message_start' && obj.message) {
        const mu = obj.message.usage || {};
        if (obj.message.model) u.model = obj.message.model;
        u.in = mu.input_tokens ?? u.in;
        u.cr = mu.cache_read_input_tokens ?? u.cr;
        u.cw = mu.cache_creation_input_tokens ?? u.cw;
        if (mu.cache_creation) {
            u.cw5 = mu.cache_creation.ephemeral_5m_input_tokens ?? 0;
            u.cw1 = mu.cache_creation.ephemeral_1h_input_tokens ?? 0;
        }
        if (mu.output_tokens) u.out = mu.output_tokens;
        u.seen = true;
        return;
    }
    if (obj.type === 'message_delta' && obj.usage) {
        const mu = obj.usage;
        if (mu.output_tokens != null) u.out = mu.output_tokens;
        if (mu.input_tokens != null) u.in = mu.input_tokens;
        if (mu.cache_read_input_tokens != null) u.cr = mu.cache_read_input_tokens;
        if (mu.cache_creation_input_tokens != null) u.cw = mu.cache_creation_input_tokens;
        u.seen = true;
        return;
    }
    // Google AI Studio / Vertex: usageMetadata в каждом чанке, значения накопительные
    if (obj.usageMetadata) {
        const g = obj.usageMetadata;
        if (obj.modelVersion) u.model = obj.modelVersion;
        const cached = g.cachedContentTokenCount || 0;
        if (g.promptTokenCount != null) { u.in = g.promptTokenCount - cached; u.cr = cached; }
        const cand = g.candidatesTokenCount ?? null;
        const think = g.thoughtsTokenCount ?? 0;
        if (cand != null) { u.out = cand + think; u.think = think; }   // thinking у Google оплачивается как выход
        u.seen = true;
        return;
    }
    // OpenAI-совместимые: OpenAI/NanoGPT/DeepSeek/xAI — obj.usage; Moonshot/Kimi — choices[0].usage
    const mu = obj.usage || obj.choices?.[0]?.usage;
    if (mu && (mu.prompt_tokens != null || mu.completion_tokens != null)) {
        // OpenAI: prompt_tokens_details.cached_tokens; Moonshot: cached_tokens; DeepSeek: prompt_cache_hit_tokens
        const cached = mu.prompt_tokens_details?.cached_tokens ?? mu.cached_tokens ?? mu.prompt_cache_hit_tokens ?? 0;
        u.in = (mu.prompt_tokens || 0) - cached;
        u.cr = cached;
        u.cw = mu.prompt_tokens_details?.cache_write_tokens ?? u.cw;
        u.out = mu.completion_tokens || 0;
        const think = mu.completion_tokens_details?.reasoning_tokens;
        if (think) u.think = think;   // уже входит в completion_tokens
        if (obj.model) u.model = obj.model;
        u.seen = true;
    }
}

/** Читает наш экземпляр потока и собирает usage. Не бросает исключений наружу. */
async function consumeStream(stream, meta) {
    const reader = stream.getReader();
    const dec = new TextDecoder();
    let buf = '';
    const u = newUsage(meta);
    const handle = (obj) => applyUsage(u, obj);

    try {
        while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            buf += dec.decode(value, { stream: true });
            let idx;
            while ((idx = buf.indexOf('\n')) >= 0) {
                const line = buf.slice(0, idx).replace(/\r$/, '');
                buf = buf.slice(idx + 1);
                if (!line.startsWith('data:')) continue;
                const payload = line.slice(5).trim();
                if (!payload || payload === '[DONE]') continue;
                try { handle(JSON.parse(payload)); } catch { /* не JSON — пропускаем */ }
            }
        }
    } catch (e) {
        console.debug(`${LOG} стрим прерван`, e);
    }
    return u.seen ? u : null;
}

function installFetchHook() {
    if (window.__fisherCostFetchPatched) return;
    const original = window.fetch;
    window.fetch = async function (input, init) {
        const url = typeof input === 'string' ? input : (input?.url || '');
        if (!url.includes(GENERATE_URL)) return original.apply(this, arguments);

        let meta = { model: null, stream: false, source: null, maxTokens: null };
        try {
            const body = typeof init?.body === 'string' ? JSON.parse(init.body) : null;
            if (body) meta = { model: body.model || null, stream: !!body.stream, source: body.chat_completion_source || null, maxTokens: body.max_tokens ?? null };
        } catch { /* тело не наше дело */ }

        const response = await original.apply(this, arguments);

        if (!meta.stream) {
            if (response.ok) handleNonStream(response.clone(), meta);
            return response;
        }
        if (!response.ok || !response.body) return response;

        try {
            const [forTavern, forUs] = response.body.tee();
            consumeStream(forUs, meta).then((u) => {
                if (u) onUsage(u, meta);
                else warnNoUsage(meta);
            });
            return new Response(forTavern, {
                status: response.status,
                statusText: response.statusText,
                headers: response.headers,
            });
        } catch (e) {
            console.warn(`${LOG} не удалось раздвоить поток, отдаю оригинал`, e);
            return response;
        }
    };
    window.__fisherCostFetchPatched = true;
}

/** Пинг кэш-рефрешера: non-stream запрос с крошечным max_tokens (Cache-Refresh-SillyTavern шлёт 1). */
function isRefreshPing(meta) {
    return !meta.stream && meta.maxTokens != null && meta.maxTokens <= REFRESH_MAX_TOKENS;
}

/**
 * Non-stream ответ. Для Custom/OpenAI-совместимых сервер Таверны отдаёт JSON провайдера как есть —
 * там есть usage, считаем точно. Для Claude и Google сервер разворачивает ответ в {choices,...}
 * и usage выбрасывает: если это пинг рефрешера — оцениваем по последнему промпту, иначе предупреждаем.
 */
async function handleNonStream(cloned, meta) {
    let json = null;
    try { json = await cloned.json(); } catch { /* не JSON */ }
    const u = newUsage(meta);
    if (json) {
        applyUsage(u, json);
        // Google non-stream без обёртки (на всякий случай) — usageMetadata на верхнем уровне
        if (!u.seen && json.usageMetadata) applyUsage(u, { usageMetadata: json.usageMetadata, modelVersion: json.modelVersion });
    }
    if (u.seen) {
        if (isRefreshPing(meta)) recordRefresh(u, meta, false);
        else onUsage(u, meta);
        return;
    }
    if (isRefreshPing(meta)) {
        recordRefresh(estimateRefresh(meta), meta, true);
        return;
    }
    warnNonStream();
}

/**
 * Оценка пинга без usage: промпт тот же, что у последнего ответа в этом чате.
 * Google (неявный кэш): весь промпт по цене чтения кэша. Anthropic и прочие с точками кэша:
 * cr+cw по цене чтения, in (хвост за точкой) по полной. Выход — max_tokens пинга.
 */
function estimateRefresh(meta) {
    const u = newUsage(meta);
    const lp = lastPromptByChat[chatLabel()];
    if (!lp) return u;   // seen=false → запись без цены, только счётчик
    u.model = lp.model || u.model;
    const google = meta.source === 'makersuite' || meta.source === 'vertexai';
    if (google) { u.cr = lp.in + lp.cr + lp.cw; u.in = 0; }
    else { u.cr = lp.cr + lp.cw; u.in = lp.in; }
    u.cw = 0; u.cw5 = 0; u.cw1 = 0;
    u.out = Math.max(1, meta.maxTokens || 1);
    u.seen = true;
    return u;
}

function recordRefresh(u, meta, estimated) {
    const { cost, priced } = u.seen ? computeCost(u) : { cost: null, priced: false };
    const rec = { t: Date.now(), model: u.model || meta.model || '?', in: u.in || 0, out: u.out || 0, cr: u.cr || 0, cw: u.cw || 0, cost, priced, est: !!estimated };
    recordLedger(rec, 'refresh', 'refresh');
    console.log(`${LOG} пинг кэш-рефрешера${estimated ? ' (оценка)' : ''}`, rec);
}

let lastNoUsageWarn = 0;
function warnNoUsage(meta) {
    const now = Date.now();
    if (now - lastNoUsageWarn < 120_000) return;
    lastNoUsageWarn = now;
    const hint = meta.source === 'custom'
        ? 'провайдер не прислал usage в стриме. Включи в настройках Fisher Cost «добавлять stream_options.include_usage» или впиши это в Additional body parameters.'
        : `провайдер (${meta.source || '?'}) не прислал usage в стриме — стоимость не посчитана.`;
    toastr.warning('Fisher Cost: ' + hint, '', { timeOut: 10000 });
}

function warnNonStream() {
    const now = Date.now();
    if (now - lastNonStreamWarn < 60_000) return;
    lastNonStreamWarn = now;
    toastr.info('Fisher Cost: запрос без стриминга — сервер Таверны выбрасывает usage, стоимость не посчитана.', '', { timeOut: 8000 });
}

/** Custom-источник (Moonshot, NanoGPT и т.п.): просим usage в последнем чанке стрима. */
function onSettingsReady(generate_data) {
    if (!settings.injectUsageOption) return;
    if (generate_data?.chat_completion_source !== 'custom' || !generate_data.stream) return;
    const cur = String(generate_data.custom_include_body || '').trim();
    if (/include_usage/.test(cur)) return;
    if (!cur) {
        generate_data.custom_include_body = '{"stream_options":{"include_usage":true}}';
    } else if (cur.startsWith('{') && cur.endsWith('}')) {
        const inner = cur.slice(1, -1).trim();
        generate_data.custom_include_body = '{' + (inner ? inner + ',' : '') + '"stream_options":{"include_usage":true}}';
    } else {
        generate_data.custom_include_body = cur + '\nstream_options:\n  include_usage: true';
    }
}

// ---------- привязка usage к сообщению / журналу ----------

function onUsage(u, meta) {
    const { cost, priced, long, offpeak } = computeCost(u);
    const rec = {
        t: Date.now(),
        model: u.model || meta.model || '?',
        in: u.in || 0,
        out: u.out || 0,
        cr: u.cr || 0,
        cw: u.cw || 0,
        cw5: u.cw5,
        cw1: u.cw1,
        think: u.think || 0,
        long: !!long,
        offpeak: !!offpeak,
        cost,
        priced,
    };

    lastPromptByChat[chatLabel()] = { model: rec.model, in: rec.in, cr: rec.cr, cw: rec.cw };

    if (awaitingMessageId != null) {
        const id = awaitingMessageId;
        awaitingMessageId = null;
        attachToMessage(id, rec);
        return;
    }
    // Ждём MESSAGE_RECEIVED; если не пришёл — это служебный запрос (саммари, копилот и т.п.)
    pendingUsage = rec;
    clearTimeout(pendingTimer);
    pendingTimer = setTimeout(() => {
        if (pendingUsage === rec) {
            pendingUsage = null;
            recordLedger(rec, 'service', currentGenType);
            console.log(`${LOG} служебный запрос`, rec);
        }
    }, ATTACH_WAIT_MS);
}

function onMessageReceived(messageId) {
    const id = Number(messageId);
    if (!Number.isFinite(id)) return;
    if (pendingUsage) {
        const rec = pendingUsage;
        pendingUsage = null;
        clearTimeout(pendingTimer);
        attachToMessage(id, rec);
    } else {
        awaitingMessageId = id;
        // страховка: если стрим так и не отдал usage
        setTimeout(() => { if (awaitingMessageId === id) awaitingMessageId = null; }, ATTACH_WAIT_MS);
    }
}

function attachToMessage(id, rec) {
    const chat = SillyTavern.getContext().chat;
    const mes = chat?.[id];
    if (!mes || mes.is_user) {
        recordLedger(rec, 'service', currentGenType);
        return;
    }
    mes.extra = mes.extra || {};
    const prev = mes.extra[EXTRA_KEY];
    let stored;
    if (currentGenType === 'continue' && prev) {
        // продолжение того же сообщения — стоимость суммируется
        stored = {
            ...rec,
            in: prev.in + rec.in, out: prev.out + rec.out, cr: prev.cr + rec.cr, cw: prev.cw + rec.cw,
            cost: (prev.cost ?? 0) + (rec.cost ?? 0),
            priced: prev.priced && rec.priced,
            parts: (prev.parts || 1) + 1,
        };
    } else {
        stored = { ...rec };
    }
    mes.extra[EXTRA_KEY] = stored;
    // копия для свайпа, чтобы данные пережили переключение свайпов
    if (Array.isArray(mes.swipe_info) && mes.swipe_id != null && mes.swipe_info[mes.swipe_id]) {
        mes.swipe_info[mes.swipe_id].extra = mes.swipe_info[mes.swipe_id].extra || {};
        mes.swipe_info[mes.swipe_id].extra[EXTRA_KEY] = stored;
    }
    recordLedger(rec, 'message', currentGenType);
    renderMessageLine(id);
    try { SillyTavern.getContext().saveChat?.(); } catch { /* сохранится при следующем saveChat Таверны */ }
}

function recordLedger(rec, kind, genType) {
    const chat = chatLabel();
    const entry = { t: rec.t, chat, model: rec.model, kind, gen: genType || null, in: rec.in, out: rec.out, cr: rec.cr, cw: rec.cw, cost: rec.cost, priced: rec.priced, est: !!rec.est };
    settings.raw.push(entry);

    const key = `${dayKey(rec.t)}|${chat}|${rec.model}`;
    const d = settings.daily[key] || (settings.daily[key] = { day: dayKey(rec.t), chat, model: rec.model, n: 0, in: 0, out: 0, cr: 0, cw: 0, cost: 0, svc: 0, svcCost: 0, ref: 0, refCost: 0, unpriced: 0 });
    d.ref = d.ref || 0; d.refCost = d.refCost || 0;   // записи, созданные до появления рефреш-счётчика
    if (kind === 'message') d.n += 1;
    else if (kind === 'refresh') { d.ref += 1; d.refCost += rec.cost || 0; }
    else { d.svc += 1; d.svcCost += rec.cost || 0; }
    d.in += rec.in; d.out += rec.out; d.cr += rec.cr; d.cw += rec.cw;
    d.cost += rec.cost || 0;
    if (!rec.priced) d.unpriced += 1;

    pruneLedger();
    settings.__dirty = true;
    ctx.saveSettingsDebounced();
    refreshChipIfOpen();
}

function pruneLedger() {
    const now = Date.now();
    settings.raw = settings.raw.filter((e) => now - e.t < RAW_KEEP_MS);
    const cutoff = dayKey(now - DAILY_KEEP_DAYS * 86400 * 1000);
    for (const k of Object.keys(settings.daily)) {
        if (settings.daily[k].day < cutoff) delete settings.daily[k];
    }
}

// ---------- строка под сообщением ----------

function fmtShort(n) {
    n = Number(n || 0);
    if (n >= 1_000_000) return (n / 1_000_000).toFixed(2).replace('.', ',') + 'M';
    if (n >= 10_000) return (n / 1000).toFixed(1).replace('.', ',') + 'k';
    return fmtInt(n);
}

function messageLineHtml(u, tok) {
    const mode = settings.compactLine || 'auto';
    const tokFull = tok != null
        ? `<span class="fc-tok" title="Токенов в тексте сообщения, count_tokens">${fmtInt(tok)} тк</span>`
        : '';
    const tokCompact = tok != null ? `<span class="fc-tok">${fmtShort(tok)} тк</span>` : '';
    if (!u) {
        // Своё сообщение или старое, без данных usage — показываем только токены.
        if (tok == null) return '';
        return `<span class="fc-line fc-mode-${mode}">`
            + `<span class="fc-full">${tokFull}</span>`
            + `<span class="fc-compact">${tokCompact}</span>`
            + '</span>';
    }
    const cacheTotal = u.cr + u.cw + u.in;
    const hit = cacheTotal > 0 ? Math.round((u.cr / cacheTotal) * 100) : 0;
    const hitCls = hit >= 85 ? 'fc-hit-good' : hit >= 50 ? 'fc-hit-mid' : 'fc-hit-bad';
    const costTxt = u.priced ? fmtUsd(u.cost) : '<span class="fc-unpriced" title="Модели нет в таблице цен">цена?</span>';
    const parts = u.parts > 1 ? ` <span class="fc-dim">(${u.parts} части)</span>` : '';
    const ttl = (u.cw5 != null || u.cw1 != null) ? `5м ${fmtInt(u.cw5 || 0)} / 1ч ${fmtInt(u.cw1 || 0)}` : '';
    const extra = [
        u.think ? `из них рассуждение ${fmtInt(u.think)}` : '',
        u.long ? 'тариф длинного контекста' : '',
        u.offpeak ? 'непиковый тариф' : '',
    ].filter(Boolean).map((x) => '\n' + x).join('');
    const title = `${u.model}\nвход ${fmtInt(u.in)}\nвыход ${fmtInt(u.out)}\nкэш-чтение ${fmtInt(u.cr)}\nкэш-запись ${fmtInt(u.cw)}${ttl ? ' (' + ttl + ')' : ''}\nкэш-хит ${hit}%${extra}`;

    const full = `<span class="fc-full">`
        + `<span class="fc-model">${escapeHtml(u.model)}</span>`
        + ` <span>вход ${fmtInt(u.in)}</span>`
        + ` <span>выход ${fmtInt(u.out)}</span>`
        + ` <span>кэш ↓${fmtInt(u.cr)} ↑${fmtInt(u.cw)}</span>`
        + ` <span class="fc-hit ${hitCls}">${hit}%</span>`
        + ` <span class="fc-cost">${costTxt}</span>${parts}`
        + (tokFull ? ' ' + tokFull : '')
        + '</span>';

    // компакт: без модели, сокращённые числа, переносится, если всё равно не влезает
    const compact = `<span class="fc-compact">`
        + `<span>in ${fmtShort(u.in)}</span>`
        + ` <span>out ${fmtShort(u.out)}</span>`
        + ` <span>CR ${fmtShort(u.cr)}</span>`
        + (u.cw ? ` <span>CW ${fmtShort(u.cw)}</span>` : '')
        + ` <span class="fc-hit ${hitCls}">${hit}%</span>`
        + ` <span class="fc-cost">${costTxt}</span>`
        + (tokCompact ? ' ' + tokCompact : '')
        + '</span>';

    return `<span class="fc-line fc-mode-${mode}" title="${escapeHtml(title)}">${full}${compact}</span>`;
}

function renderMessageLine(id) {
    const chat = SillyTavern.getContext().chat;
    const mes = chat?.[id];
    const $mes = $(`#chat .mes[mesid="${id}"]`);
    if (!$mes.length) return;
    $mes.find('.fc-line').remove();
    if (!settings.showUnderMessage) return;
    const u = mes?.extra?.[EXTRA_KEY];
    const tok = settings.showMessageTokens ? messageTokens(id) : null;
    if (!u && tok == null) return;
    const $anchor = $mes.find('.mes_text').first();
    if ($anchor.length) $anchor.after(messageLineHtml(u, tok));
}

function renderAllLines() {
    const chat = SillyTavern.getContext().chat || [];
    $('#chat .mes').each(function () {
        const id = Number($(this).attr('mesid'));
        if (Number.isFinite(id) && chat[id]) renderMessageLine(id);
    });
    scheduleMessageCounts();
}

// ---------- отчёт ----------

function sumRaw(since) {
    const acc = { n: 0, svc: 0, ref: 0, in: 0, out: 0, cr: 0, cw: 0, cost: 0, svcCost: 0, refCost: 0, unpriced: 0 };
    for (const e of settings.raw) {
        if (e.t < since) continue;
        if (e.kind === 'message') acc.n++;
        else if (e.kind === 'refresh') { acc.ref++; acc.refCost += e.cost || 0; }
        else { acc.svc++; acc.svcCost += e.cost || 0; }
        acc.in += e.in; acc.out += e.out; acc.cr += e.cr; acc.cw += e.cw; acc.cost += e.cost || 0;
        if (!e.priced) acc.unpriced++;
    }
    return acc;
}

function sumDaily(days) {
    const cutoff = dayKey(Date.now() - (days - 1) * 86400 * 1000);
    const acc = { n: 0, svc: 0, ref: 0, in: 0, out: 0, cr: 0, cw: 0, cost: 0, svcCost: 0, refCost: 0, unpriced: 0 };
    const byChat = {};
    const byModel = {};
    for (const d of Object.values(settings.daily)) {
        if (d.day < cutoff) continue;
        const ref = d.ref || 0, refCost = d.refCost || 0;
        acc.n += d.n; acc.svc += d.svc; acc.ref += ref; acc.in += d.in; acc.out += d.out; acc.cr += d.cr; acc.cw += d.cw;
        acc.cost += d.cost; acc.svcCost += d.svcCost; acc.refCost += refCost; acc.unpriced += d.unpriced;
        const c = byChat[d.chat] || (byChat[d.chat] = { n: 0, svc: 0, ref: 0, cost: 0, refCost: 0, in: 0, out: 0, cr: 0, cw: 0 });
        c.n += d.n; c.svc += d.svc; c.ref += ref; c.cost += d.cost; c.refCost += refCost; c.in += d.in; c.out += d.out; c.cr += d.cr; c.cw += d.cw;
        const m = byModel[d.model] || (byModel[d.model] = { n: 0, cost: 0 });
        m.n += d.n + d.svc + ref; m.cost += d.cost;
    }
    return { acc, byChat, byModel };
}

function sumCurrentChat() {
    const chat = SillyTavern.getContext().chat || [];
    const acc = { n: 0, in: 0, out: 0, cr: 0, cw: 0, cost: 0, unpriced: 0 };
    for (const m of chat) {
        const u = m?.extra?.[EXTRA_KEY];
        if (!u) continue;
        acc.n++; acc.in += u.in; acc.out += u.out; acc.cr += u.cr; acc.cw += u.cw; acc.cost += u.cost || 0;
        if (!u.priced) acc.unpriced++;
    }
    return acc;
}

function periodRow(label, a, note) {
    const hitBase = a.cr + a.cw + a.in;
    const hit = hitBase ? Math.round((a.cr / hitBase) * 100) : 0;
    return `<tr>
        <td class="fc-lbl">${label}</td>
        <td class="fc-num"><b>${fmtUsd(a.cost)}</b>${a.unpriced ? ` <span class="fc-unpriced" title="${a.unpriced} ответов без цены">*</span>` : ''}</td>
        <td class="fc-num">${fmtInt(a.n)}${a.svc ? ` <span class="fc-dim">+${fmtInt(a.svc)} служ. (${fmtUsd(a.svcCost)})</span>` : ''}${a.ref ? ` <span class="fc-dim" title="пинги кэш-рефрешера; для Claude и Google стоимость оценочная — сервер Таверны не отдаёт usage без стриминга">+${fmtInt(a.ref)} рефреш (≈${fmtUsd(a.refCost)})</span>` : ''}</td>
        <td class="fc-num">${fmtInt(a.in)}</td>
        <td class="fc-num">${fmtInt(a.out)}</td>
        <td class="fc-num">${fmtInt(a.cr)}</td>
        <td class="fc-num">${fmtInt(a.cw)}</td>
        <td class="fc-num">${hit}%</td>
        ${note ? `<td class="fc-dim">${note}</td>` : '<td></td>'}
    </tr>`;
}

function buildReportHtml(days) {
    const today = sumRaw(startOfToday());
    const last24 = sumRaw(Date.now() - 86400 * 1000);
    const session = sumRaw(sessionStart);
    const week = sumDaily(7);
    const month = sumDaily(30);
    const cur = sumCurrentChat();
    const sel = sumDaily(days);

    const chats = Object.entries(sel.byChat).sort((a, b) => b[1].cost - a[1].cost);
    const models = Object.entries(sel.byModel).sort((a, b) => b[1].cost - a[1].cost);

    const chatRows = chats.length
        ? chats.map(([name, c]) => `<tr>
            <td class="fc-lbl">${escapeHtml(name)}</td>
            <td class="fc-num"><b>${fmtUsd(c.cost)}</b></td>
            <td class="fc-num">${fmtInt(c.n)}${c.svc ? ` <span class="fc-dim">+${c.svc}</span>` : ''}${c.ref ? ` <span class="fc-dim" title="пинги кэш-рефрешера, ≈${fmtUsd(c.refCost)}">+${c.ref}↻</span>` : ''}</td>
            <td class="fc-num">${c.n ? fmtUsd(c.cost / c.n) : '—'}</td>
            <td class="fc-num">${fmtInt(c.in)}</td>
            <td class="fc-num">${fmtInt(c.out)}</td>
            <td class="fc-num">${fmtInt(c.cr)}</td>
            <td class="fc-num">${fmtInt(c.cw)}</td>
        </tr>`).join('')
        : '<tr><td colspan="8" class="fc-dim">За этот период записей нет. Отправь сообщение — появится первая.</td></tr>';

    const modelRows = models.map(([name, m]) => `<tr><td class="fc-lbl">${escapeHtml(name)}</td><td class="fc-num"><b>${fmtUsd(m.cost)}</b></td><td class="fc-num">${fmtInt(m.n)}</td></tr>`).join('');

    const curHitBase = cur.cr + cur.cw + cur.in;
    const curHit = curHitBase ? Math.round((cur.cr / curHitBase) * 100) : 0;

    return `<div class="fc-report">
        <h3>Расходы</h3>
        <table class="fc-table">
            <thead><tr><th></th><th>стоимость</th><th>ответов</th><th>вход</th><th>выход</th><th>кэш ↓</th><th>кэш ↑</th><th>хит</th><th></th></tr></thead>
            <tbody>
                ${periodRow('Сегодня', today)}
                ${periodRow('24 часа', last24)}
                ${periodRow('7 дней', week.acc)}
                ${periodRow('30 дней', month.acc)}
                ${periodRow('Эта вкладка', session, 'с ' + new Date(sessionStart).toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit' }))}
            </tbody>
        </table>

        <h3>Этот чат <span class="fc-dim">${escapeHtml(chatLabel())}</span></h3>
        <p class="fc-cur">
            <b>${fmtUsd(cur.cost)}</b> за ${fmtInt(cur.n)} ответов
            ${cur.n ? `· в среднем ${fmtUsd(cur.cost / cur.n)}` : ''}
            · кэш-хит ${curHit}%
            ${cur.unpriced ? `<span class="fc-unpriced">· ${cur.unpriced} без цены</span>` : ''}
            <br><span class="fc-dim">Считается по самим сообщениям (сохранены в файле чата), а не по журналу — переживает переустановку расширения. Служебные запросы сюда не входят.</span>
        </p>

        <h3>По чатам
            <select id="fc_period" class="text_pole fc-select">
                <option value="1" ${days === 1 ? 'selected' : ''}>сегодня</option>
                <option value="7" ${days === 7 ? 'selected' : ''}>7 дней</option>
                <option value="30" ${days === 30 ? 'selected' : ''}>30 дней</option>
                <option value="120" ${days === 120 ? 'selected' : ''}>всё (120 дней)</option>
            </select>
        </h3>
        <table class="fc-table">
            <thead><tr><th>чат</th><th>стоимость</th><th>ответов</th><th>за ответ</th><th>вход</th><th>выход</th><th>кэш ↓</th><th>кэш ↑</th></tr></thead>
            <tbody>${chatRows}</tbody>
        </table>

        ${models.length ? `<h3>По моделям</h3><table class="fc-table fc-narrow"><tbody>${modelRows}</tbody></table>` : ''}

        <div class="fc-actions">
            <div class="menu_button" id="fc_export">Скачать журнал (CSV)</div>
            <div class="menu_button" id="fc_reload_prices">Перечитать цены</div>
        </div>
        <p class="fc-dim fc-foot">Стоимость каждого ответа зафиксирована в момент генерации по тогдашним ценам и не пересчитывается. Звёздочка — есть ответы от модели, которой нет в таблице цен.</p>
    </div>`;
}

let reportDays = 7;

function bindReportActions($root) {
    $root.find('#fc_period').on('change', function () {
        reportDays = Number(this.value);
        $root.html(buildReportHtml(reportDays));
        bindReportActions($root);
    });
    $root.find('#fc_export').on('click', exportCsv);
    $root.find('#fc_reload_prices').on('click', async () => {
        await loadPricing();
        toastr.success(`Цены перечитаны: ${Object.keys(pricing).length} моделей`);
    });
}

async function openReport() {
    const html = `<div id="fc_report_root">${buildReportHtml(reportDays)}</div>`;
    const c = SillyTavern.getContext();
    const $dom = $(html);
    bindReportActions($dom);
    if (typeof c.callGenericPopup === 'function' && c.POPUP_TYPE) {
        c.callGenericPopup($dom, c.POPUP_TYPE.TEXT, '', { wide: true, large: true, allowVerticalScrolling: true, okButton: 'Закрыть' });
    } else {
        $('#fc_inline_report').empty().append($dom);
    }
}

function refreshChipIfOpen() {
    const $root = $('#fc_report_root');
    if ($root.length) {
        $root.html(buildReportHtml(reportDays));
        bindReportActions($root);
    }
    updateChip();
}

function updateChip() {
    const today = sumRaw(startOfToday());
    $('#fc_chip_value').text(fmtUsd(today.cost));
}

function exportCsv() {
    const rows = [['day', 'chat', 'model', 'messages', 'service', 'refresh', 'input', 'output', 'cache_read', 'cache_write', 'cost_usd', 'service_cost_usd', 'refresh_cost_usd', 'unpriced']];
    for (const d of Object.values(settings.daily).sort((a, b) => a.day.localeCompare(b.day))) {
        rows.push([d.day, d.chat, d.model, d.n, d.svc, d.ref || 0, d.in, d.out, d.cr, d.cw, d.cost.toFixed(6), d.svcCost.toFixed(6), (d.refCost || 0).toFixed(6), d.unpriced]);
    }
    const csv = rows.map((r) => r.map((v) => `"${String(v).replace(/"/g, '""')}"`).join(',')).join('\n');
    const blob = new Blob(['\ufeff' + csv], { type: 'text/csv;charset=utf-8' });
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = `fisher-cost-${dayKey(Date.now())}.csv`;
    a.click();
    setTimeout(() => URL.revokeObjectURL(a.href), 5000);
}

// ---------- панель настроек ----------

function addSettingsPanel() {
    const html = `
    <div id="fisher_cost_settings">
        <div class="inline-drawer">
            <div class="inline-drawer-toggle inline-drawer-header">
                <b>Fisher Cost</b>
                <div class="inline-drawer-icon fa-solid fa-circle-chevron-down down"></div>
            </div>
            <div class="inline-drawer-content">
                <div class="fc-row">
                    <div class="menu_button" id="fc_open_report">Открыть отчёт</div>
                    <span class="fc-dim">сегодня: <b id="fc_chip_value">$0</b></span>
                </div>
                <label class="checkbox_label"><input type="checkbox" id="fc_show_line"> Строка с токенами и ценой под каждым ответом</label>
                <label class="checkbox_label"><input type="checkbox" id="fc_topbar"> Разбивка промпта над чатом: чат, лорбук, промпт, карточка, персона</label>
                <label class="checkbox_label"><input type="checkbox" id="fc_msgtok"> Токены самого сообщения в строке под ним</label>
                <div class="fc-dim">Считает серверный плагин <code>fisher-tokens</code> через настоящий <code>count_tokens</code>. Чат — без скрытых сообщений. Каждое сообщение считается один раз и запоминается в файле чата.</div>
                <label class="fc-row">Подпись под ответом
                    <select id="fc_compact" class="text_pole fc-select">
                        <option value="auto">полная, на узком экране компактная</option>
                        <option value="always">всегда компактная</option>
                        <option value="never">всегда полная</option>
                    </select>
                </label>
                <label class="checkbox_label"><input type="checkbox" id="fc_inject"> Custom-источник: добавлять <code>stream_options.include_usage</code> (нужно для OpenAI через Custom, NanoGPT и других OpenAI-совместимых)</label>
                <label class="fc-row">Кэш-запись без разбивки по TTL считать как
                    <select id="fc_ttl" class="text_pole fc-select">
                        <option value="1h">1 час</option>
                        <option value="5m">5 минут</option>
                    </select>
                </label>
                <details class="fc-details">
                    <summary>Цены поверх pricing.json</summary>
                    <textarea id="fc_pricing" class="text_pole textarea_compact" rows="6" placeholder='{"claude-opus-5": {"input": 5, "output": 25, "cache_read": 0.5, "cache_write_5m": 6.25, "cache_write_1h": 10}}'></textarea>
                    <div class="fc-dim">Доллары за миллион. Применяется к новым ответам; уже посчитанные не меняются.</div>
                </details>
                <div id="fc_inline_report"></div>
            </div>
        </div>
    </div>`;
    $('#extensions_settings2').append(html);

    $('#fc_show_line').prop('checked', settings.showUnderMessage).on('change', function () {
        settings.showUnderMessage = this.checked;
        ctx.saveSettingsDebounced();
        renderAllLines();
    });
    $('#fc_topbar').prop('checked', settings.showTopbar).on('change', function () {
        settings.showTopbar = this.checked;
        ctx.saveSettingsDebounced();
        renderTopbar();
        if (this.checked) scheduleBreakdown();
    });
    $('#fc_msgtok').prop('checked', settings.showMessageTokens).on('change', function () {
        settings.showMessageTokens = this.checked;
        ctx.saveSettingsDebounced();
        renderAllLines();
    });
    $('#fc_compact').val(settings.compactLine).on('change', function () {
        settings.compactLine = this.value;
        ctx.saveSettingsDebounced();
        renderAllLines();
    });
    $('#fc_inject').prop('checked', settings.injectUsageOption).on('change', function () {
        settings.injectUsageOption = this.checked;
        ctx.saveSettingsDebounced();
    });
    $('#fc_ttl').val(settings.defaultTtl).on('change', function () {
        settings.defaultTtl = this.value;
        ctx.saveSettingsDebounced();
    });
    $('#fc_pricing').val(settings.pricingOverride).on('change', async function () {
        settings.pricingOverride = this.value;
        ctx.saveSettingsDebounced();
        await loadPricing();
    });
    $('#fc_open_report').on('click', openReport);

    // пункт в меню «волшебной палочки» рядом с полем ввода
    const $menu = $('#extensionsMenu');
    if ($menu.length) {
        $menu.append(`<div id="fc_wand" class="list-group-item flex-container flexGap5 interactable" tabindex="0">
            <div class="fa-solid fa-coins extensionsMenuExtensionButton"></div>
            <span>Расходы</span>
        </div>`);
        $('#fc_wand').on('click', openReport);
    }
    updateChip();
}

// ---------- запуск ----------

// ---------- разбивка промпта по секциям ----------
// Таверна считает все модели Claude одним замороженным tokenizers/claude.json,
// который разошёлся с настоящим токенизатором ещё на Opus 4.7. Здесь секции и
// сообщения считает серверный плагин fisher-tokens через count_tokens, то есть
// тем же токенизатором, каким считает сама модель.

/** Контекст пересобирается на каждый вызов: в нём лежит снимок текущих
 *  значений, поэтому кэшировать его нельзя — characterId и прочие примитивы
 *  застынут на моменте загрузки страницы. */
function C() {
    return SillyTavern.getContext();
}

const COUNT_URL = '/api/plugins/fisher-tokens/count';
const TOPBAR_ID = 'fc-topbar';
const MSG_KEY = 'fisher_tokens';
const MSG_BATCH = 6;            // сообщений за один заход очереди
const MSG_TICK_MS = 1200;       // пауза между заходами — бережём лимит запросов
const BREAKDOWN_DEBOUNCE_MS = 800;

const SECTIONS = [
    { key: 'chat', full: 'чат', short: 'чат' },
    { key: 'lorebook', full: 'лорбук', short: 'лор' },
    { key: 'prompt', full: 'промпт', short: 'пр' },
    { key: 'card', full: 'карточка', short: 'крт' },
    { key: 'persona', full: 'персона', short: 'прс' },
];

let wiActivated = [];           // записи лорбука, сработавшие на последней генерации
let breakdown = null;
let breakdownErr = null;
let breakdownBusy = false;
let breakdownTimer = null;
let msgQueue = [];
let msgTimer = null;
let msgDirty = false;

function currentModel() {
    // Контекст отдаёт настройки как chatCompletionSettings, а готовая функция
    // сама разбирается, какое поле модели относится к активному источнику.
    try {
        const m = C().getChatCompletionModel?.();
        if (m) return String(m);
    } catch { /* источник без модели — пробуем настройки напрямую */ }
    const o = C().chatCompletionSettings || {};
    return String(o.claude_model || o.openai_model || o.custom_model || '');
}

async function countBlocks(blocks) {
    const headers = C().getRequestHeaders
        ? C().getRequestHeaders()
        : { 'Content-Type': 'application/json' };
    const res = await fetch(COUNT_URL, {
        method: 'POST',
        headers,
        body: JSON.stringify({ model: currentModel(), blocks }),
    });
    if (res.status === 404) throw new Error('серверный плагин fisher-tokens не установлен или плагины выключены');
    if (!res.ok) throw new Error(`плагин ответил HTTP ${res.status}`);
    const data = await res.json();
    if (data.error) throw new Error(data.error);
    return data;
}

/** Видимая часть чата: /hide помечает сообщения флагом is_system. */
function visibleChatText() {
    return (C().chat || [])
        .filter((m) => m && !m.is_system && typeof m.mes === 'string')
        .map((m) => `${m.name || ''}: ${m.mes}`)
        .join('\n\n');
}

function cardText() {
    const ctxNow = C();
    const c = ctxNow.characters?.[ctxNow.characterId];
    if (!c) return '';
    // Карточки V2/V3 держат поля в data; Таверна дублирует их наверх, но не всегда.
    const pick = (k) => String(c[k] || c.data?.[k] || '');
    return [pick('description'), pick('personality'), pick('scenario'), pick('mes_example')]
        .filter(Boolean)
        .join('\n\n');
}

function personaText() {
    const pu = C().powerUserSettings || {};
    return String(pu.persona_description || '');
}

/** Включённые текстовые блоки пресета. Маркеры пропускаем — их содержимое
 *  (карточка, персона, история) считается отдельными секциями. */
function promptText() {
    const o = C().chatCompletionSettings || {};
    const prompts = Array.isArray(o.prompts) ? o.prompts : [];
    const orders = Array.isArray(o.prompt_order) ? o.prompt_order : [];
    const order = orders.length
        ? (orders.find((x) => Number(x.character_id) === 100001) || orders[orders.length - 1])
        : null;
    const enabled = new Map();
    for (const item of (order?.order || [])) {
        enabled.set(String(item.identifier), item.enabled !== false);
    }
    return prompts
        .filter((p) => p && !p.marker && typeof p.content === 'string' && p.content.trim())
        .filter((p) => enabled.size === 0 || enabled.get(String(p.identifier)) !== false)
        .map((p) => p.content)
        .join('\n\n');
}

function lorebookText() {
    return wiActivated.map((e) => String(e?.content || '')).filter(Boolean).join('\n\n');
}

function scheduleBreakdown() {
    if (!settings.showTopbar) return;
    clearTimeout(breakdownTimer);
    breakdownTimer = setTimeout(refreshBreakdown, BREAKDOWN_DEBOUNCE_MS);
}

async function refreshBreakdown() {
    if (!settings.showTopbar || breakdownBusy) return;
    if (!currentModel()) { breakdownErr = 'модель не выбрана'; renderTopbar(); return; }
    breakdownBusy = true;
    try {
        const { counts, errors } = await countBlocks([
            { key: 'chat', text: visibleChatText() },
            { key: 'lorebook', text: lorebookText() },
            { key: 'prompt', text: promptText() },
            { key: 'card', text: cardText() },
            { key: 'persona', text: personaText() },
        ]);
        breakdown = counts;
        const firstError = Object.values(errors || {})[0];
        breakdownErr = firstError || null;
    } catch (e) {
        breakdownErr = String(e.message || e);
    } finally {
        breakdownBusy = false;
        renderTopbar();
    }
}

function topbarHtml() {
    if (!breakdown && breakdownErr) {
        return `<span class="fc-tb-err" title="${escapeHtml(breakdownErr)}">счётчик недоступен</span>`;
    }
    if (!breakdown) return '<span class="fc-dim">считаю…</span>';

    const total = SECTIONS.reduce((s, x) => s + (breakdown[x.key] || 0), 0);
    const cell = (label, value, extra) =>
        `<span class="fc-tb-cell${extra ? ' ' + extra : ''}">`
        + `<span class="fc-tb-l">${label}</span><span class="fc-tb-v">${value}</span></span>`;

    const full = '<span class="fc-tb-full">'
        + SECTIONS.map((s) => cell(s.full, fmtInt(breakdown[s.key] || 0))).join('')
        + cell('всего', fmtInt(total), 'fc-tb-total')
        + '</span>';
    const compact = '<span class="fc-tb-compact">'
        + SECTIONS.map((s) => cell(s.short, fmtShort(breakdown[s.key] || 0))).join('')
        + cell('Σ', fmtShort(total), 'fc-tb-total')
        + '</span>';

    const warn = breakdownErr
        ? ` <span class="fc-tb-err" title="${escapeHtml(breakdownErr)}">!</span>`
        : '';
    return full + compact + warn;
}

function renderTopbar() {
    if (!settings?.showTopbar) { $('#' + TOPBAR_ID).remove(); return; }
    let $bar = $('#' + TOPBAR_ID);
    if (!$bar.length) {
        const $chat = $('#chat');
        if (!$chat.length) return;
        $chat.before(`<div id="${TOPBAR_ID}" class="fc-topbar fc-mode-${settings.compactLine || 'auto'}" title="Точный подсчёт через count_tokens. Чат — без скрытых сообщений."></div>`);
        $bar = $('#' + TOPBAR_ID);
    }
    $bar.attr('class', `fc-topbar fc-mode-${settings.compactLine || 'auto'}`);
    $bar.html(topbarHtml());
}

// ---------- токены отдельного сообщения ----------

/** Дешёвый устойчивый хеш: пересчитываем сообщение, только если текст изменился. */
function textHash(text) {
    let h = 5381;
    for (let i = 0; i < text.length; i++) h = ((h << 5) + h + text.charCodeAt(i)) | 0;
    return `${text.length}:${h}`;
}

function messageTokens(id) {
    const mes = C().chat?.[id];
    if (!mes || typeof mes.mes !== 'string') return null;
    const rec = mes.extra?.[MSG_KEY];
    return (rec && rec.h === textHash(mes.mes)) ? rec.n : null;
}

/** Сообщения на экране, у которых ещё нет свежего счёта. */
function collectUncounted() {
    if (!settings.showMessageTokens) return [];
    const chat = C().chat || [];
    const ids = [];
    $('#chat .mes').each(function () {
        const id = Number($(this).attr('mesid'));
        if (!Number.isFinite(id)) return;
        const mes = chat[id];
        if (!mes || typeof mes.mes !== 'string' || !mes.mes.trim()) return;
        if (messageTokens(id) === null) ids.push(id);
    });
    return ids;
}

function scheduleMessageCounts() {
    if (!settings.showMessageTokens || msgTimer) return;
    msgTimer = setTimeout(async () => {
        msgTimer = null;
        await processMessageQueue();
    }, 300);
}

async function processMessageQueue() {
    if (!settings.showMessageTokens || !currentModel()) return;
    msgQueue = collectUncounted();
    if (!msgQueue.length) {
        if (msgDirty) { msgDirty = false; try { C().saveChat?.(); } catch { /* сохранится позже */ } }
        return;
    }

    const chat = C().chat || [];
    const batch = msgQueue.slice(0, MSG_BATCH);
    const blocks = batch.map((id) => ({ key: String(id), text: chat[id].mes }));

    try {
        const { counts } = await countBlocks(blocks);
        for (const id of batch) {
            const n = counts[String(id)];
            if (typeof n !== 'number') continue;
            const mes = chat[id];
            if (!mes) continue;
            mes.extra = mes.extra || {};
            mes.extra[MSG_KEY] = { n, h: textHash(mes.mes) };
            msgDirty = true;
            renderMessageLine(id);
        }
    } catch (e) {
        console.warn(`${LOG} подсчёт сообщений: ${e.message || e}`);
        return;   // не долбим сервер в цикле, если он отвечает ошибкой
    }

    if (collectUncounted().length) {
        msgTimer = setTimeout(async () => { msgTimer = null; await processMessageQueue(); }, MSG_TICK_MS);
    } else if (msgDirty) {
        msgDirty = false;
        try { C().saveChat?.(); } catch { /* сохранится позже */ }
    }
}

function init() {
    try {
        ctx = SillyTavern.getContext();
    } catch (e) {
        console.error(`${LOG} нет контекста SillyTavern`, e);
        return;
    }
    ctx.extensionSettings[MODULE] = ctx.extensionSettings[MODULE] || {};
    settings = ctx.extensionSettings[MODULE];
    for (const [k, v] of Object.entries(DEFAULTS)) {
        if (settings[k] === undefined) settings[k] = structuredClone(v);
    }
    delete settings.__dirty;
    pruneLedger();

    installFetchHook();
    loadPricing();

    const et = ctx.eventTypes || ctx.event_types || {};
    const es = ctx.eventSource;
    es.on(et.GENERATION_STARTED || 'generation_started', (type, _params, dryRun) => {
        if (dryRun) return;
        currentGenType = type || 'normal';
    });
    es.on(et.CHAT_COMPLETION_SETTINGS_READY || 'chat_completion_settings_ready', onSettingsReady);
    es.on(et.MESSAGE_RECEIVED || 'message_received', onMessageReceived);
    es.on(et.CHARACTER_MESSAGE_RENDERED || 'character_message_rendered', (id) => renderMessageLine(Number(id)));
    es.on(et.MESSAGE_SWIPED || 'message_swiped', (id) => setTimeout(() => renderMessageLine(Number(id)), 50));
    es.on(et.MESSAGE_UPDATED || 'message_updated', (id) => setTimeout(() => renderMessageLine(Number(id)), 50));
    es.on(et.CHAT_CHANGED || 'chat_id_changed', () => setTimeout(renderAllLines, 100));
    es.on(et.MORE_MESSAGES_LOADED || 'more_messages_loaded', () => setTimeout(renderAllLines, 100));
    es.on(et.WORLD_INFO_ACTIVATED || 'world_info_activated', (entries) => {
        wiActivated = Array.isArray(entries) ? entries : [];
        scheduleBreakdown();
    });
    es.on(et.MESSAGE_SENT || 'message_sent', () => { scheduleBreakdown(); scheduleMessageCounts(); });
    es.on(et.MESSAGE_RECEIVED || 'message_received', () => scheduleBreakdown());
    es.on(et.CHAT_CHANGED || 'chat_id_changed', () => {
        // Лорбук пересобирается на следующей генерации, счёт чата — сразу.
        wiActivated = [];
        breakdown = null;
        setTimeout(() => { renderTopbar(); scheduleBreakdown(); }, 200);
    });

    addSettingsPanel();
    setTimeout(renderAllLines, 500);
    setTimeout(() => { renderTopbar(); scheduleBreakdown(); }, 900);
    console.log(`${LOG} загружен. Стриминг обязателен: usage читается из потока ответа.`);
}

if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
} else {
    init();
}
