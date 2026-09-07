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

const DEFAULTS = {
    showUnderMessage: true,
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
    delete pricing._comment;
}

function findPrice(model) {
    if (!model) return null;
    const m = String(model).toLowerCase();
    let best = null;
    for (const key of Object.keys(pricing)) {
        const k = key.toLowerCase();
        if (m.startsWith(k) && (!best || k.length > best.length)) best = k;
    }
    if (!best) return null;
    return pricing[Object.keys(pricing).find((k) => k.toLowerCase() === best)];
}

/** Стоимость фиксируется здесь один раз, при генерации, и больше не пересчитывается. */
function computeCost(u) {
    const p = findPrice(u.model);
    if (!p) return { cost: null, priced: false };
    let cw5 = u.cw5, cw1 = u.cw1;
    if (cw5 == null && cw1 == null) {
        if (settings.defaultTtl === '1h') { cw1 = u.cw; cw5 = 0; } else { cw5 = u.cw; cw1 = 0; }
    }
    const cost =
        (u.in || 0) * (p.input || 0) +
        (u.out || 0) * (p.output || 0) +
        (u.cr || 0) * (p.cache_read || 0) +
        (cw5 || 0) * (p.cache_write_5m ?? p.cache_write ?? 0) +
        (cw1 || 0) * (p.cache_write_1h ?? p.cache_write ?? 0);
    return { cost: cost / 1e6, priced: true };
}

// ---------- разбор стрима ----------

/** Читает наш экземпляр потока и собирает usage. Не бросает исключений наружу. */
async function consumeStream(stream, meta) {
    const reader = stream.getReader();
    const dec = new TextDecoder();
    let buf = '';
    const u = { model: meta.model || null, in: 0, out: 0, cr: 0, cw: 0, cw5: null, cw1: null, seen: false };

    const handle = (obj) => {
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
        // OpenAI-совместимые (NanoGPT и т.п.) — usage приходит в последнем чанке
        if (obj.usage && (obj.usage.prompt_tokens != null || obj.usage.completion_tokens != null)) {
            const mu = obj.usage;
            const cached = mu.prompt_tokens_details?.cached_tokens ?? mu.prompt_cache_hit_tokens ?? 0;
            u.in = (mu.prompt_tokens || 0) - cached;
            u.cr = cached;
            u.out = mu.completion_tokens || 0;
            if (obj.model) u.model = obj.model;
            u.seen = true;
        }
    };

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

        let meta = { model: null, stream: false, source: null };
        try {
            const body = typeof init?.body === 'string' ? JSON.parse(init.body) : null;
            if (body) meta = { model: body.model || null, stream: !!body.stream, source: body.chat_completion_source || null };
        } catch { /* тело не наше дело */ }

        const response = await original.apply(this, arguments);

        if (!meta.stream) {
            warnNonStream();
            return response;
        }
        if (!response.ok || !response.body) return response;

        try {
            const [forTavern, forUs] = response.body.tee();
            consumeStream(forUs, meta).then((u) => { if (u) onUsage(u, meta); });
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

function warnNonStream() {
    const now = Date.now();
    if (now - lastNonStreamWarn < 60_000) return;
    lastNonStreamWarn = now;
    toastr.info('Fisher Cost: стриминг выключен — сервер Таверны выбрасывает usage, стоимость не посчитана.', '', { timeOut: 8000 });
}

// ---------- привязка usage к сообщению / журналу ----------

function onUsage(u, meta) {
    const { cost, priced } = computeCost(u);
    const rec = {
        t: Date.now(),
        model: u.model || meta.model || '?',
        in: u.in || 0,
        out: u.out || 0,
        cr: u.cr || 0,
        cw: u.cw || 0,
        cw5: u.cw5,
        cw1: u.cw1,
        cost,
        priced,
    };

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
    const entry = { t: rec.t, chat, model: rec.model, kind, gen: genType || null, in: rec.in, out: rec.out, cr: rec.cr, cw: rec.cw, cost: rec.cost, priced: rec.priced };
    settings.raw.push(entry);

    const key = `${dayKey(rec.t)}|${chat}|${rec.model}`;
    const d = settings.daily[key] || (settings.daily[key] = { day: dayKey(rec.t), chat, model: rec.model, n: 0, in: 0, out: 0, cr: 0, cw: 0, cost: 0, svc: 0, svcCost: 0, unpriced: 0 });
    if (kind === 'message') d.n += 1; else { d.svc += 1; d.svcCost += rec.cost || 0; }
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

function messageLineHtml(u) {
    if (!u) return '';
    const cacheTotal = u.cr + u.cw + u.in;
    const hit = cacheTotal > 0 ? Math.round((u.cr / cacheTotal) * 100) : 0;
    const costTxt = u.priced ? fmtUsd(u.cost) : '<span class="fc-unpriced" title="Модели нет в таблице цен">цена?</span>';
    const parts = u.parts > 1 ? ` <span class="fc-dim">(${u.parts} части)</span>` : '';
    const ttl = (u.cw5 != null || u.cw1 != null) ? `5м ${fmtInt(u.cw5 || 0)} / 1ч ${fmtInt(u.cw1 || 0)}` : '';
    const title = `${u.model}\nвход ${fmtInt(u.in)}\nвыход ${fmtInt(u.out)}\nкэш-чтение ${fmtInt(u.cr)}\nкэш-запись ${fmtInt(u.cw)}${ttl ? ' (' + ttl + ')' : ''}\nкэш-хит ${hit}%`;
    return `<span class="fc-line" title="${escapeHtml(title)}">`
        + `<span class="fc-model">${escapeHtml(u.model)}</span>`
        + ` <span>вход ${fmtInt(u.in)}</span>`
        + ` <span>выход ${fmtInt(u.out)}</span>`
        + ` <span>кэш ↓${fmtInt(u.cr)} ↑${fmtInt(u.cw)}</span>`
        + ` <span class="fc-hit ${hit >= 85 ? 'fc-hit-good' : hit >= 50 ? 'fc-hit-mid' : 'fc-hit-bad'}">${hit}%</span>`
        + ` <span class="fc-cost">${costTxt}</span>${parts}`
        + '</span>';
}

function renderMessageLine(id) {
    const chat = SillyTavern.getContext().chat;
    const mes = chat?.[id];
    const $mes = $(`#chat .mes[mesid="${id}"]`);
    if (!$mes.length) return;
    $mes.find('.fc-line').remove();
    if (!settings.showUnderMessage) return;
    const u = mes?.extra?.[EXTRA_KEY];
    if (!u) return;
    const $anchor = $mes.find('.mes_text').first();
    if ($anchor.length) $anchor.after(messageLineHtml(u));
}

function renderAllLines() {
    const chat = SillyTavern.getContext().chat || [];
    $('#chat .mes').each(function () {
        const id = Number($(this).attr('mesid'));
        if (Number.isFinite(id) && chat[id]) renderMessageLine(id);
    });
}

// ---------- отчёт ----------

function sumRaw(since) {
    const acc = { n: 0, svc: 0, in: 0, out: 0, cr: 0, cw: 0, cost: 0, svcCost: 0, unpriced: 0 };
    for (const e of settings.raw) {
        if (e.t < since) continue;
        if (e.kind === 'message') acc.n++; else { acc.svc++; acc.svcCost += e.cost || 0; }
        acc.in += e.in; acc.out += e.out; acc.cr += e.cr; acc.cw += e.cw; acc.cost += e.cost || 0;
        if (!e.priced) acc.unpriced++;
    }
    return acc;
}

function sumDaily(days) {
    const cutoff = dayKey(Date.now() - (days - 1) * 86400 * 1000);
    const acc = { n: 0, svc: 0, in: 0, out: 0, cr: 0, cw: 0, cost: 0, svcCost: 0, unpriced: 0 };
    const byChat = {};
    const byModel = {};
    for (const d of Object.values(settings.daily)) {
        if (d.day < cutoff) continue;
        acc.n += d.n; acc.svc += d.svc; acc.in += d.in; acc.out += d.out; acc.cr += d.cr; acc.cw += d.cw;
        acc.cost += d.cost; acc.svcCost += d.svcCost; acc.unpriced += d.unpriced;
        const c = byChat[d.chat] || (byChat[d.chat] = { n: 0, svc: 0, cost: 0, in: 0, out: 0, cr: 0, cw: 0 });
        c.n += d.n; c.svc += d.svc; c.cost += d.cost; c.in += d.in; c.out += d.out; c.cr += d.cr; c.cw += d.cw;
        const m = byModel[d.model] || (byModel[d.model] = { n: 0, cost: 0 });
        m.n += d.n + d.svc; m.cost += d.cost;
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
        <td class="fc-num">${fmtInt(a.n)}${a.svc ? ` <span class="fc-dim">+${fmtInt(a.svc)} служ. (${fmtUsd(a.svcCost)})</span>` : ''}</td>
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
            <td class="fc-num">${fmtInt(c.n)}${c.svc ? ` <span class="fc-dim">+${c.svc}</span>` : ''}</td>
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
    const rows = [['day', 'chat', 'model', 'messages', 'service', 'input', 'output', 'cache_read', 'cache_write', 'cost_usd', 'service_cost_usd', 'unpriced']];
    for (const d of Object.values(settings.daily).sort((a, b) => a.day.localeCompare(b.day))) {
        rows.push([d.day, d.chat, d.model, d.n, d.svc, d.in, d.out, d.cr, d.cw, d.cost.toFixed(6), d.svcCost.toFixed(6), d.unpriced]);
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
    es.on(et.MESSAGE_RECEIVED || 'message_received', onMessageReceived);
    es.on(et.CHARACTER_MESSAGE_RENDERED || 'character_message_rendered', (id) => renderMessageLine(Number(id)));
    es.on(et.MESSAGE_SWIPED || 'message_swiped', (id) => setTimeout(() => renderMessageLine(Number(id)), 50));
    es.on(et.MESSAGE_UPDATED || 'message_updated', (id) => setTimeout(() => renderMessageLine(Number(id)), 50));
    es.on(et.CHAT_CHANGED || 'chat_id_changed', () => setTimeout(renderAllLines, 100));
    es.on(et.MORE_MESSAGES_LOADED || 'more_messages_loaded', () => setTimeout(renderAllLines, 100));

    addSettingsPanel();
    setTimeout(renderAllLines, 500);
    console.log(`${LOG} загружен. Стриминг обязателен: usage читается из потока ответа.`);
}

if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
} else {
    init();
}
