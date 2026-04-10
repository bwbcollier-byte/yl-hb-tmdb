import { supabase } from './supabase';
import * as dotenv from 'dotenv';
dotenv.config();

const TMDB_BEARER_TOKEN = process.env.TMDB_BEARER_TOKEN!;
const ENDPOINT     = process.env.MINING_ENDPOINT || '/movie/popular';
const MAX_PAGES    = parseInt(process.env.MAX_PAGES || '5');
const MEDIA_TYPE   = process.env.MEDIA_TYPE || (ENDPOINT.includes('/tv') ? 'tv' : 'movie');
const SLEEP_MS     = parseInt(process.env.SLEEP_MS  || '150');
const STALE_DAYS   = parseInt(process.env.STALE_DAYS || '3');
const WORKFLOW_ID  = parseInt(process.env.WORKFLOW_ID || '0');
const UPSERT_CHUNK = 25;

const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

function chunk<T>(arr: T[], n: number): T[][] {
    const out: T[][] = [];
    for (let i = 0; i < arr.length; i += n) out.push(arr.slice(i, i + n));
    return out;
}

async function batchUpsertSocials(rows: any[]): Promise<void> {
    for (const batch of chunk(rows, UPSERT_CHUNK)) {
        let attempts = 0;
        while (attempts < 3) {
            const { error } = await supabase
                .from('hb_socials')
                .upsert(batch, { onConflict: 'type,identifier' });
            if (!error) break;
            attempts++;
            if (attempts >= 3) {
                console.error(`   ⚠️  Social upsert failed after 3 attempts: ${error.message}`);
            } else {
                console.warn(`   ⚠️  Retrying social upsert (${attempts}/3)...`);
                await sleep(1000 * attempts);
            }
        }
    }
}

// ─── TMDB ────────────────────────────────────────────────────────────────────

async function fetchTMDB(endpoint: string): Promise<any> {
    const res = await fetch(`https://api.themoviedb.org/3${endpoint}`, {
        headers: { Authorization: `Bearer ${TMDB_BEARER_TOKEN}`, accept: 'application/json' }
    });
    if (!res.ok) throw new Error(`TMDB ${res.status}: ${res.statusText} — ${endpoint}`);
    return res.json();
}

// ─── SUPABASE HELPERS ────────────────────────────────────────────────────────

// Pre-load recently-mined TMDB media IDs so we skip re-processing fresh entries.
// Uses check_tmdb_mining (dedicated mining timestamp) rather than updated_at
// so unrelated updates to a row don't accidentally reset its staleness clock.
async function loadRecentlyUpdatedMedia(): Promise<Set<string>> {
    const set = new Set<string>();
    const cutoff = new Date(Date.now() - STALE_DAYS * 24 * 60 * 60 * 1000).toISOString();
    const pageSize = 1000;
    let from = 0;
    while (true) {
        const { data, error } = await supabase
            .from('hb_media')
            .select('soc_tmdb_id')
            .gte('check_tmdb_mining', cutoff)
            .not('soc_tmdb_id', 'is', null)
            .range(from, from + pageSize - 1);
        if (error || !data?.length) break;
        for (const row of data) if (row.soc_tmdb_id) set.add(row.soc_tmdb_id);
        if (data.length < pageSize) break;
        from += pageSize;
    }
    return set;
}

// Update the workflows table with run outcome and summary stats
async function updateWorkflowSummary(
    status: 'success' | 'failure',
    summary: Record<string, unknown>,
    durationSecs: number
) {
    if (!WORKFLOW_ID) return;
    const { error } = await supabase.rpc('log_workflow_run', {
        p_workflow_id:   WORKFLOW_ID,
        p_status:        status,
        p_duration_secs: durationSecs,
        p_summary:       summary,
    });
    if (error) console.warn(`   ⚠️  Workflow summary update failed: ${error.message}`);
    else console.log(`   📊 Workflow summary logged to Supabase`);
}

// Load all existing TMDB person IDs in one batch query — avoids N individual lookups
async function loadTmdbPersonMap(): Promise<Map<string, string>> {
    const map = new Map<string, string>();
    const pageSize = 1000;
    let from = 0;
    while (true) {
        const { data, error } = await supabase
            .from('hb_socials')
            .select('identifier, linked_talent')
            .eq('type', 'TMDB')
            .not('linked_talent', 'is', null)
            .range(from, from + pageSize - 1);
        if (error || !data?.length) break;
        for (const row of data) map.set(row.identifier, row.linked_talent);
        if (data.length < pageSize) break;
        from += pageSize;
    }
    return map;
}

// ─── COUNTRIES ───────────────────────────────────────────────────────────────

let COUNTRIES: { name: string; code: string }[] = [];
async function loadCountries() {
    if (COUNTRIES.length) return;
    const { data } = await supabase.from('countries').select('name, country_code');
    if (data) COUNTRIES = data.map(c => ({ name: c.name, code: c.country_code }));
}
function countryCode(placeOfBirth: string | null): string | null {
    if (!placeOfBirth || !COUNTRIES.length) return null;
    const lower = placeOfBirth.toLowerCase();
    return COUNTRIES.find(c => lower.includes(c.name.toLowerCase()))?.code?.toUpperCase() ?? null;
}

// ─── MAIN ────────────────────────────────────────────────────────────────────

async function run() {
    const runStart = Date.now();
    console.log(`🚀 TMDb Media Mining — ${ENDPOINT} (${MEDIA_TYPE}, max ${MAX_PAGES || 'all'} pages, stale after ${STALE_DAYS}d)`);

    await loadCountries();

    // Pre-load recently-updated media IDs — skip any title processed within STALE_DAYS
    const recentMedia = await loadRecentlyUpdatedMedia();
    console.log(`   ⏭️  ${recentMedia.size} titles fresh (updated < ${STALE_DAYS}d ago) — will skip`);

    // Pre-load all known TMDB person IDs → talent UUIDs (1 batch query for the whole run)
    const tmdbPersonMap = await loadTmdbPersonMap();
    console.log(`   ✅ Pre-loaded ${tmdbPersonMap.size} existing TMDB person records\n`);

    // In-run person cache — actors appearing in multiple titles processed once only
    const personCache = new Map<number, string>(); // tmdb_person_id → talent_id
    for (const [id, talentId] of tmdbPersonMap) personCache.set(parseInt(id), talentId);

    let totalMedia = 0, totalTalentNew = 0, totalSkipped = 0;

    // Fetch page 1 to discover total_pages, then cap at MAX_PAGES (0 = all)
    const sep = ENDPOINT.includes('?') ? '&' : '?';
    const firstPage = await fetchTMDB(`${ENDPOINT}${sep}language=en-US&page=1`);
    const discovered = firstPage.total_pages ?? 1;
    const pageLimit  = MAX_PAGES > 0 ? Math.min(MAX_PAGES, discovered) : discovered;
    console.log(`   📋 ${firstPage.total_results ?? '?'} titles across ${pageLimit} page(s) (API total: ${discovered})\n`);

    for (let page = 1; page <= pageLimit; page++) {
        const items: any[] = page === 1
            ? (firstPage.results ?? [])
            : (await fetchTMDB(`${ENDPOINT}${sep}language=en-US&page=${page}`)).results ?? [];
        if (!items.length) break;
        console.log(`📄 Page ${page}/${pageLimit}: ${items.length} titles`);

        for (const item of items) {
            try {
                const label = item.title || item.name;

                // Skip if processed recently
                if (recentMedia.has(String(item.id))) {
                    console.log(`⏭️  ${label} (TMDB: ${item.id}) — skipped (fresh)`);
                    totalSkipped++;
                    continue;
                }

                console.log(`\n🏨 ${label} (TMDB: ${item.id})`);

                // Full detail + credits in one API call
                const detail = await fetchTMDB(
                    `/${MEDIA_TYPE}/${item.id}?append_to_response=credits,external_ids&language=en-US`
                );
                await sleep(SLEEP_MS);

                // ── Upsert hb_media ─────────────────────────────────────────
                const { data: mediaRow, error: mediaErr } = await supabase
                    .from('hb_media')
                    .upsert({
                        name:         detail.title || detail.name,
                        date_release: detail.release_date || detail.first_air_date || null,
                        about:        detail.overview  || null,
                        image:        detail.poster_path ? `https://image.tmdb.org/t/p/w500${detail.poster_path}` : null,
                        genres:       detail.genres?.map((g: any) => g.name) ?? [],
                        soc_tmdb_id:  String(detail.id),
                        soc_tmdb:     `https://www.themoviedb.org/${MEDIA_TYPE}/${detail.id}`,
                        soc_imdb_id:  detail.external_ids?.imdb_id || null,
                        soc_imdb:     detail.external_ids?.imdb_id
                                        ? `https://www.imdb.com/title/${detail.external_ids.imdb_id}/`
                                        : null,
                        media_type:          MEDIA_TYPE,
                        credits:             detail.credits ?? null,
                        check_tmdb_mining:   new Date().toISOString(),
                        updated_at:          new Date().toISOString(),
                    }, { onConflict: 'soc_tmdb_id' })
                    .select('id')
                    .single();

                if (mediaErr) {
                    console.error(`   ❌ hb_media error: ${mediaErr.message}`);
                    continue;
                }
                totalMedia++;

                // ── Process top cast ─────────────────────────────────────────
                const topCast    = detail.credits?.cast?.slice(0, 10) ?? [];
                const talentIds: string[] = [];
                const socialsQueue: any[] = [];

                for (const cast of topCast) {
                    try {
                        // Already processed this run — just link
                        if (personCache.has(cast.id)) {
                            talentIds.push(personCache.get(cast.id)!);
                            continue;
                        }

                        const p = await fetchTMDB(
                            `/person/${cast.id}?append_to_response=external_ids&language=en-US`
                        );
                        await sleep(SLEEP_MS);

                        const talentPayload = {
                            name:           p.name,
                            image:          p.profile_path ? `https://image.tmdb.org/t/p/w500${p.profile_path}` : null,
                            status:         'Ready',
                            act_type:       p.known_for_department || 'Acting',
                            gender:         p.gender === 1 ? 'Female' : p.gender === 2 ? 'Male' : 'Unknown',
                            birth_location: p.place_of_birth   || null,
                            biography:      p.biography        || null,
                            category:       'Film & Television',
                            birth_country:  countryCode(p.place_of_birth),
                            updated_at:     new Date().toISOString(),
                        };

                        let talentId: string | null = null;
                        const existingId = tmdbPersonMap.get(String(p.id));

                        if (existingId) {
                            await supabase.from('hb_talent').update(talentPayload).eq('id', existingId);
                            talentId = existingId;
                        } else {
                            const { data: newT } = await supabase
                                .from('hb_talent')
                                .insert({ ...talentPayload, created_at: new Date().toISOString() })
                                .select('id')
                                .single();
                            talentId = newT?.id ?? null;
                            if (talentId) totalTalentNew++;
                        }

                        if (!talentId) continue;

                        personCache.set(cast.id, talentId);
                        tmdbPersonMap.set(String(p.id), talentId);
                        talentIds.push(talentId);

                        // Queue socials for batch upsert
                        const ext = p.external_ids ?? {};
                        const now = new Date().toISOString();
                        socialsQueue.push(
                            { type: 'TMDB',      identifier: String(p.id),          name: p.name, social_url: `https://www.themoviedb.org/person/${p.id}`, linked_talent: talentId, updated_at: now },
                            ...(ext.imdb_id      ? [{ type: 'IMDB',      identifier: ext.imdb_id,      name: p.name, social_url: `https://www.imdb.com/name/${ext.imdb_id}/`,          linked_talent: talentId, updated_at: now }] : []),
                            ...(ext.instagram_id ? [{ type: 'INSTAGRAM', identifier: ext.instagram_id, name: p.name, social_url: `https://instagram.com/${ext.instagram_id}`,           linked_talent: talentId, updated_at: now }] : []),
                            ...(ext.tiktok_id    ? [{ type: 'TIKTOK',    identifier: ext.tiktok_id,    name: p.name, social_url: `https://tiktok.com/@${ext.tiktok_id}`,                linked_talent: talentId, updated_at: now }] : []),
                            ...(ext.twitter_id   ? [{ type: 'TWITTER',   identifier: ext.twitter_id,   name: p.name, social_url: `https://twitter.com/${ext.twitter_id}`,               linked_talent: talentId, updated_at: now }] : []),
                        );

                    } catch (err: any) {
                        console.error(`   ⚠️  ${cast.name}: ${err.message}`);
                    }
                }

                // ── Batch upsert socials in small chunks ────────────────────
                if (socialsQueue.length > 0) await batchUpsertSocials(socialsQueue);

                // ── Link talent to media ─────────────────────────────────────
                if (talentIds.length > 0) {
                    await supabase
                        .from('hb_media')
                        .update({ linked_talent: talentIds })
                        .eq('id', mediaRow.id);
                }

                console.log(`   ✅ ${talentIds.length} talent linked (${socialsQueue.length} socials queued)`);

            } catch (err: any) {
                console.error(`   ❌ ${item.title || item.name}: ${err.message}`);
            }
        }
    }

    const durationSecs = Math.round((Date.now() - runStart) / 1000);
    const summaryObj = {
        titles_processed: totalMedia,
        titles_skipped:   totalSkipped,
        new_talent_added: totalTalentNew,
        pages_fetched:    pageLimit,
        endpoint:         ENDPOINT,
        run_at:           new Date().toISOString(),
    };
    console.log(`\n🎉 Done! ${totalMedia} processed, ${totalSkipped} skipped, ${totalTalentNew} new talent (${durationSecs}s)`);
    await updateWorkflowSummary('success', summaryObj, durationSecs);
}

run().catch(async err => {
    console.error('🔥 Fatal:', err.message);
    await updateWorkflowSummary('failure', { error: err.message, endpoint: ENDPOINT, run_at: new Date().toISOString() }, 0);
    process.exit(1);
});
