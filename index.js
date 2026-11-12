const express = require("express");
const crypto = require("crypto");
const { Redis } = require("@upstash/redis");

// ==================================================
// CONFIGURATION
// ==================================================

const TOKEN = process.env.IG_ACCESS_TOKEN;
const IG_USER_ID = process.env.IG_USER_ID;

const API_VERSION = process.env.API_VERSION || "v22.0";
const API_HOST = process.env.API_HOST || "graph.facebook.com";
const BASE_URL = `https://${API_HOST}/${API_VERSION}`;

const MIN_VIEWS = Number(process.env.MIN_VIEWS || 100000);
const PAGE_SIZE = Number(process.env.PAGE_SIZE || 10);
const REQUEST_DELAY_MS = Number(process.env.REQUEST_DELAY_MS || 1500);

// How long one cron call may work before returning (keep under the
// function's maxDuration and your cron site's timeout)
const TIME_BUDGET_MS = Number(process.env.TIME_BUDGET_MS || 20000);

const DELETE_ENABLED = process.env.DELETE_ENABLED === "true";
const CONFIRM_DELETE = process.env.CONFIRM_DELETE === "YES";
const MAX_DELETE = Number(process.env.MAX_DELETE ?? 1);

const LOCK_TTL_SECONDS = 60;
const MAX_CONSECUTIVE_SKIPS = 5;

// ==================================================
// REDIS (state storage; Vercel has no persistent disk)
// ==================================================

const K = {
    collect: "ig:collect_progress",
    scan: "ig:scan_progress",
    reels: "ig:reels",       // sorted set: member = reel id, score = timestamp
    low: "ig:low_view_ids",  // sorted set: oldest first
    deleted: "ig:deleted_ids",
    lock: "ig:lock"
};

let redis = null;

function getRedis() {
    if (redis) return redis;

    const url =
        process.env.UPSTASH_REDIS_REST_URL ||
        process.env.KV_REST_API_URL;
    const token =
        process.env.UPSTASH_REDIS_REST_TOKEN ||
        process.env.KV_REST_API_TOKEN;

    if (!url || !token) {
        throw new Error("Missing Upstash Redis environment variables");
    }

    // Deserialization off: Instagram IDs are huge numbers and must stay strings
    redis = new Redis({ url, token, automaticDeserialization: false });

    return redis;
}

async function getJson(key, fallback) {
    const raw = await getRedis().get(key);
    if (raw === null || raw === undefined) return fallback;
    return typeof raw === "string" ? JSON.parse(raw) : raw;
}

async function setJson(key, value) {
    await getRedis().set(key, JSON.stringify(value));
}

async function withLock(fn) {
    const r = getRedis();

    const acquired = await r.set(K.lock, String(Date.now()), {
        nx: true,
        ex: LOCK_TTL_SECONDS
    });

    if (!acquired) {
        return { busy: true, note: "Another run is still in progress" };
    }

    try {
        return await fn();
    } finally {
        await r.del(K.lock);
    }
}

// ==================================================
// HELPERS
// ==================================================

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

let lastRequestTime = 0;

class MetaApiError extends Error {
    constructor(message, code, subcode, status) {
        super(message);
        this.name = "MetaApiError";
        this.code = code;
        this.subcode = subcode;
        this.status = status;
    }
}

function mustStop(error) {
    const message = String(error.message || "").toLowerCase();
    const stopCodes = [4, 10, 17, 32, 190, 200, 368, 613, 80002];

    return (
        stopCodes.includes(Number(error.code)) ||
        message.includes("too many actions") ||
        message.includes("rate limit") ||
        message.includes("temporarily blocked") ||
        message.includes("request limit") ||
        message.includes("reduce the amount of data")
    );
}

function isObjectGone(error) {
    return (
        error instanceof MetaApiError &&
        error.code === 100 &&
        (Number(error.subcode) === 33 ||
            /does not exist|cannot be loaded/i.test(error.message))
    );
}

function removeToken(url) {
    if (!url) return null;
    const parsed = new URL(url);
    parsed.searchParams.delete("access_token");
    return parsed.toString();
}

function addToken(url) {
    const parsed = new URL(url);
    parsed.searchParams.set("access_token", TOKEN);
    parsed.searchParams.set("limit", String(PAGE_SIZE));
    parsed.searchParams.set(
        "fields",
        "id,media_type,media_product_type,timestamp"
    );
    return parsed.toString();
}

function checkConfig() {
    if (!TOKEN || !IG_USER_ID) {
        throw new Error("Missing IG_ACCESS_TOKEN or IG_USER_ID");
    }

    for (const [name, value] of Object.entries({
        MIN_VIEWS,
        PAGE_SIZE,
        REQUEST_DELAY_MS,
        TIME_BUDGET_MS,
        MAX_DELETE
    })) {
        if (!Number.isFinite(value) || value < 0) {
            throw new Error(`Invalid numeric configuration: ${name}`);
        }
    }
}

// ==================================================
// META API REQUEST
// ==================================================

async function meta(url, options = {}) {
    const wait = REQUEST_DELAY_MS - (Date.now() - lastRequestTime);
    if (wait > 0) await sleep(wait);

    let response;

    try {
        response = await fetch(url, options);
    } catch (error) {
        const networkError = new Error(`Network error: ${error.message}`);
        networkError.fatal = true;
        throw networkError;
    }

    lastRequestTime = Date.now();

    const text = await response.text();

    let data;

    try {
        data = JSON.parse(text);
    } catch {
        const error = new Error(
            `Invalid response: HTTP ${response.status}. ` + text.slice(0, 300)
        );
        error.fatal = true;
        throw error;
    }

    if (!response.ok || data.error) {
        const apiError = data.error || {};

        throw new MetaApiError(
            apiError.message || text.slice(0, 300) || `HTTP ${response.status}`,
            apiError.code,
            apiError.error_subcode,
            response.status
        );
    }

    const usage = response.headers.get("x-business-use-case-usage");
    if (usage) console.log("Meta usage:", usage);

    return data;
}

async function getViews(mediaId) {
    const params = new URLSearchParams({
        metric: "views",
        access_token: TOKEN
    });

    const result = await meta(`${BASE_URL}/${mediaId}/insights?${params}`);

    const metric = result.data?.find(item => item.name === "views");

    if (!metric) throw new Error("Views metric unavailable");

    const value =
        metric.values?.[0]?.value ??
        metric.total_value?.value ??
        metric.value;

    if (value === undefined || value === null) {
        throw new Error("Views value unavailable");
    }

    const views = Number(value);

    if (!Number.isFinite(views)) throw new Error("Invalid views value");

    return views;
}

// ==================================================
// STEP 1: COLLECT REEL IDS (resumable)
// ==================================================

async function collectStep(deadline) {
    const r = getRedis();

    const state = await getJson(K.collect, {
        nextUrl: null,
        completed: false,
        pages: 0
    });

    if (state.completed) {
        return {
            phase: "collect",
            completed: true,
            note: "Already completed",
            reelsTotal: await r.zcard(K.reels)
        };
    }

    let url = state.nextUrl
        ? addToken(state.nextUrl)
        : `${BASE_URL}/${IG_USER_ID}/media?` +
          new URLSearchParams({
              fields: "id,media_type,media_product_type,timestamp",
              limit: String(PAGE_SIZE),
              access_token: TOKEN
          });

    let pages = 0;
    let newReels = 0;

    try {
        while (url && Date.now() < deadline) {
            const result = await meta(url);

            const entries = (result.data || [])
                .filter(
                    post =>
                        post.id &&
                        post.media_type === "VIDEO" &&
                        post.media_product_type === "REELS"
                )
                .map(post => ({
                    score: Date.parse(post.timestamp) || 0,
                    member: String(post.id)
                }));

            if (entries.length) {
                newReels += Number(
                    await r.zadd(K.reels, entries[0], ...entries.slice(1))
                );
            }

            const next = result.paging?.next || null;

            state.nextUrl = removeToken(next);
            state.completed = !next;
            state.pages++;

            await setJson(K.collect, state);

            pages++;
            url = next ? addToken(next) : null;
        }
    } catch (error) {
        return {
            phase: "collect",
            completed: false,
            pages,
            newReels,
            error: error.message
        };
    }

    return {
        phase: "collect",
        completed: state.completed,
        pages,
        newReels,
        reelsTotal: await r.zcard(K.reels)
    };
}

// ==================================================
// STEP 2: CHECK VIEWS (resumable, oldest first)
// ==================================================

async function scanStep(deadline) {
    const r = getRedis();

    const collect = await getJson(K.collect, { completed: false });

    if (!collect.completed) {
        return {
            phase: "scan",
            waiting: "Collection is not finished yet. Run /api/collect first."
        };
    }

    const total = await r.zcard(K.reels);

    const state = await getJson(K.scan, {
        nextIndex: 0,
        checked: 0,
        kept: 0,
        low: 0,
        skipped: 0,
        consecutiveSkips: 0,
        completed: false,
        minViews: MIN_VIEWS
    });

    if (state.completed) {
        return {
            phase: "scan",
            completed: true,
            note: "Already completed. Use /api/reset?target=scan to rescan.",
            lowViewTotal: await r.zcard(K.low)
        };
    }

    if (state.minViews !== MIN_VIEWS && state.nextIndex > 0) {
        return {
            phase: "scan",
            error:
                "MIN_VIEWS changed since this scan started. " +
                "Call /api/reset?target=scan to restart."
        };
    }

    let processed = 0;

    while (state.nextIndex < total && Date.now() < deadline) {
        const batch = await r.zrange(
            K.reels,
            state.nextIndex,
            state.nextIndex + 9,
            { withScores: true }
        );

        if (!batch.length) break;

        for (let i = 0; i < batch.length; i += 2) {
            if (Date.now() >= deadline) break;

            const id = String(batch[i]);
            const score = Number(batch[i + 1]) || 0;

            try {
                const views = await getViews(id);

                if (views < MIN_VIEWS) {
                    state.low += Number(
                        await r.zadd(K.low, { score, member: id })
                    );
                } else {
                    state.kept++;
                }

                state.checked++;
                state.consecutiveSkips = 0;

            } catch (error) {
                console.error("Scan error:", id, error.message);

                // Stop and retry this Reel on the next call
                if (error.fatal || mustStop(error)) {
                    await setJson(K.scan, state);

                    return {
                        phase: "scan",
                        processed,
                        stoppedAt: state.nextIndex + 1,
                        error: error.message
                    };
                }

                state.skipped++;
                state.checked++;
                state.consecutiveSkips = (state.consecutiveSkips || 0) + 1;

                // Many skips in a row means something systemic is wrong
                if (state.consecutiveSkips >= MAX_CONSECUTIVE_SKIPS) {
                    state.nextIndex++;
                    await setJson(K.scan, state);

                    return {
                        phase: "scan",
                        processed,
                        error:
                            `${MAX_CONSECUTIVE_SKIPS} Reels in a row failed. ` +
                            `Last error: ${error.message}`
                    };
                }
            }

            state.nextIndex++;
            processed++;
            await setJson(K.scan, state);
        }
    }

    if (state.nextIndex >= total) {
        state.completed = true;
        await setJson(K.scan, state);
    }

    return {
        phase: "scan",
        completed: state.completed,
        processed,
        progress: `${state.nextIndex}/${total}`,
        low: state.low,
        kept: state.kept,
        skipped: state.skipped
    };
}

// ==================================================
// STEP 3: DELETE (re-checks views right before deleting)
// ==================================================

async function deleteStep(deadline) {
    if (!DELETE_ENABLED || !CONFIRM_DELETE) {
        return {
            phase: "delete",
            blocked:
                "Set DELETE_ENABLED=true and CONFIRM_DELETE=YES to allow deletion."
        };
    }

    const r = getRedis();

    const limit = MAX_DELETE > 0 ? MAX_DELETE : Infinity;

    let deleted = 0;
    let spared = 0;
    let gone = 0;

    try {
        while (deleted < limit && Date.now() < deadline) {
            const next = await r.zrange(K.low, 0, 0);

            if (!next.length) break;

            const id = String(next[0]);

            if (!/^\d+$/.test(id)) {
                await r.zrem(K.low, id);
                continue;
            }

            let views;

            try {
                views = await getViews(id);
            } catch (error) {
                if (isObjectGone(error)) {
                    await r.zrem(K.low, id);
                    gone++;
                    continue;
                }
                throw error;
            }

            if (views >= MIN_VIEWS) {
                await r.zrem(K.low, id);
                spared++;
                continue;
            }

            const params = new URLSearchParams({ access_token: TOKEN });

            const result = await meta(`${BASE_URL}/${id}?${params}`, {
                method: "DELETE"
            });

            if (result.success !== true) {
                throw new Error(
                    "Meta did not confirm deletion: " + JSON.stringify(result)
                );
            }

            await r.zrem(K.low, id);
            await r.zadd(K.deleted, { score: Date.now(), member: id });

            deleted++;
        }
    } catch (error) {
        return {
            phase: "delete",
            deleted,
            spared,
            gone,
            error: error.message,
            remaining: await r.zcard(K.low)
        };
    }

    return {
        phase: "delete",
        deleted,
        spared,
        gone,
        remaining: await r.zcard(K.low)
    };
}

// ==================================================
// EXPRESS APP
// ==================================================

const app = express();

// Vercel rewrites every /api/* request to /api/index?path=<name>.
// Restore the original path so the Express routes below match.
app.use((req, res, next) => {
    const u = new URL(req.url, "http://localhost");

    if (u.pathname === "/api/index" && u.searchParams.get("path")) {
        const original = u.searchParams.get("path");
        u.searchParams.delete("path");
        req.url = "/api/" + original + u.search;
    }

    next();
});

function safeEqual(a, b) {
    const x = Buffer.from(String(a));
    const y = Buffer.from(String(b));
    return x.length === y.length && crypto.timingSafeEqual(x, y);
}

// Accepts: Authorization: Bearer <secret>, x-cron-secret header, or ?key=
function auth(req, res, next) {
    const secret = process.env.CRON_SECRET;

    if (!secret) {
        return res.status(500).json({ error: "CRON_SECRET is not set" });
    }

    const bearer = (req.headers.authorization || "").replace(/^Bearer\s+/i, "");
    const provided = bearer || req.headers["x-cron-secret"] || req.query.key || "";

    if (!provided || !safeEqual(provided, secret)) {
        return res.status(401).json({ error: "Unauthorized" });
    }

    next();
}

function route(handler) {
    return async (req, res) => {
        try {
            checkConfig();

            const deadline = Date.now() + TIME_BUDGET_MS;
            const result = await handler(req, deadline);

            const status = result.blocked ? 403 : result.error ? 500 : 200;

            res.status(status).json(result);
        } catch (error) {
            console.error(error);
            res.status(500).json({ error: error.message });
        }
    };
}

app.get("/", (req, res) => res.json({ ok: true }));

// Collect Reel IDs
app.all("/api/collect", auth, route((req, deadline) =>
    withLock(() => collectStep(deadline))
));

// Check views and build the low-view list
app.all("/api/scan", auth, route((req, deadline) =>
    withLock(() => scanStep(deadline))
));

// Delete from the low-view list
app.all("/api/delete", auth, route((req, deadline) =>
    withLock(() => deleteStep(deadline))
));

// One endpoint for a single cron job: collect -> scan -> delete
app.all("/api/run", auth, route((req, deadline) =>
    withLock(async () => {
        const results = [];

        const collect = await getJson(K.collect, { completed: false });

        if (!collect.completed) {
            const r1 = await collectStep(deadline);
            results.push(r1);
            if (r1.error || !r1.completed) return { steps: results };
        }

        const scan = await getJson(K.scan, { completed: false });

        if (!scan.completed && Date.now() < deadline) {
            const r2 = await scanStep(deadline);
            results.push(r2);
            if (r2.error || !r2.completed) {
                return r2.error ? { steps: results, error: r2.error } : { steps: results };
            }
        }

        if (Date.now() < deadline) {
            const r3 = await deleteStep(deadline);
            results.push(r3);
            if (r3.blocked) return { steps: results, note: r3.blocked };
            if (r3.error) return { steps: results, error: r3.error };
        }

        return { steps: results };
    })
));

app.all("/api/status", auth, route(async () => {
    const r = getRedis();

    const lowPreview = await r.zrange(K.low, 0, 9);

    return {
        minViews: MIN_VIEWS,
        deleteEnabled: DELETE_ENABLED && CONFIRM_DELETE,
        collect: await getJson(K.collect, null),
        scan: await getJson(K.scan, null),
        reelsCollected: await r.zcard(K.reels),
        lowViewPending: await r.zcard(K.low),
        deletedTotal: await r.zcard(K.deleted),
        nextToDelete: lowPreview.map(String)
    };
}));

// ?target=scan | collect | all
app.all("/api/reset", auth, route(async req => {
    const r = getRedis();
    const target = String(req.query.target || "");

    if (target === "scan") {
        await r.del(K.scan);
    } else if (target === "collect") {
        await r.del(K.collect);
    } else if (target === "all") {
        await r.del(K.collect, K.scan, K.reels, K.low);
    } else {
        return { error: "Use ?target=scan, collect or all" };
    }

    return { reset: target };
}));

app.use((req, res) => {
    res.status(404).json({ error: "Not found", path: req.url });
});

module.exports = app;

// Local testing: node api/index.js
if (require.main === module) {
    const port = process.env.PORT || 3000;
    app.listen(port, () => console.log(`Listening on ${port}`));
}
