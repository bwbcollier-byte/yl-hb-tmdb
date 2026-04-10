import { supabase } from './supabase';
import * as dotenv from 'dotenv';
dotenv.config();

const TMDB_BEARER_TOKEN = process.env.TMDB_BEARER_TOKEN!;
const MAX_PAGES   = parseInt(process.env.MAX_PAGES   || '10');
const SLEEP_MS    = parseInt(process.env.SLEEP_MS    || '150');
const WORKFLOW_ID = parseInt(process.env.WORKFLOW_ID || '0');

const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

// ─── SUPABASE WORKFLOW SUMMARY ───────────────────────────────────────────────

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

// ─── TMDB ────────────────────────────────────────────────────────────────────

async function fetchTMDB(endpoint: string): Promise<any> {
    const res = await fetch(`https://api.themoviedb.org/3${endpoint}`, {
        headers: { Authorization: `Bearer ${TMDB_BEARER_TOKEN}`, accept: 'application/json' }
    });
    if (!res.ok) throw new Error(`TMDB ${res.status}: ${res.statusText} — ${endpoint}`);
    return res.json();
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

// ─── PRE-LOAD TMDB PERSON MAP ────────────────────────────────────────────────

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

// ─── MAIN ────────────────────────────────────────────────────────────────────

async function run() {
    const runStart = Date.now();
    console.log(`🎬 TMDb Popular Talent Mining — ${MAX_PAGES} pages (${MAX_PAGES * 20} people)\n`);

    await loadCountries();

    // Pre-load all known TMDB person IDs in one bulk query
    const tmdbPersonMap = await loadTmdbPersonMap();
    console.log(`   ✅ Pre-loaded ${tmdbPersonMap.size} existing TMDB person records\n`);

    // ── Fetch all popular people pages ──────────────────────────────────────
    const firstPage = await fetchTMDB(`/person/popular?language=en-US&page=1`);
    const pageLimit = Math.min(MAX_PAGES, firstPage.total_pages ?? 1);
    let allPeople: any[] = firstPage.results ?? [];
    console.log(`   📋 Fetching ${pageLimit} pages (${firstPage.total_results} total available)\n`);

    for (let page = 2; page <= pageLimit; page++) {
        const data = await fetchTMDB(`/person/popular?language=en-US&page=${page}`);
        allPeople = allPeople.concat(data.results ?? []);
        await sleep(SLEEP_MS);
    }

    console.log(`🔍 Processing ${allPeople.length} people...\n`);

    let updatedCount = 0, createdCount = 0, errorCount = 0;

    for (const personSummary of allPeople) {
        try {
            console.log(`👤 ${personSummary.name} (TMDB: ${personSummary.id})`);

            // Full profile — no combined_credits (large payload, never used)
            const p = await fetchTMDB(
                `/person/${personSummary.id}?append_to_response=external_ids&language=en-US`
            );
            await sleep(SLEEP_MS);

            const talentPayload: any = {
                name:           p.name,
                image:          p.profile_path ? `https://image.tmdb.org/t/p/w500${p.profile_path}` : null,
                status:         'Ready',
                act_type:       p.known_for_department || 'Acting',
                gender:         p.gender === 1 ? 'Female' : p.gender === 2 ? 'Male' : 'Unknown',
                birth_location: p.place_of_birth  || null,
                biography:      p.biography       || null,
                category:       'Film & Television',
                birth_country:  countryCode(p.place_of_birth),
                updated_at:     new Date().toISOString(),
            };

            // ── Upsert hb_talent ──────────────────────────────────────────────
            let talentId = tmdbPersonMap.get(String(p.id)) ?? null;

            if (talentId) {
                await supabase.from('hb_talent').update(talentPayload).eq('id', talentId);
                console.log(`   ✅ Updated: ${talentId}`);
                updatedCount++;
            } else {
                const { data: newT, error: insertErr } = await supabase
                    .from('hb_talent')
                    .insert({ ...talentPayload, created_at: new Date().toISOString() })
                    .select('id')
                    .single();
                if (insertErr) throw new Error(`Insert failed: ${insertErr.message}`);
                talentId = newT.id;
                console.log(`   ✨ Created: ${talentId}`);
                createdCount++;
            }

            // ── Batch upsert all socials for this person ──────────────────────
            const ext = p.external_ids ?? {};
            const now = new Date().toISOString();
            const socials = [
                { type: 'TMDB',      identifier: String(p.id),          name: p.name, social_url: `https://www.themoviedb.org/person/${p.id}`,      linked_talent: talentId, updated_at: now },
                ...(ext.imdb_id      ? [{ type: 'IMDB',      identifier: ext.imdb_id,      name: p.name, social_url: `https://imdb.com/name/${ext.imdb_id}/`,            linked_talent: talentId, updated_at: now }] : []),
                ...(ext.instagram_id ? [{ type: 'INSTAGRAM', identifier: ext.instagram_id, name: p.name, social_url: `https://instagram.com/${ext.instagram_id}`,         linked_talent: talentId, updated_at: now }] : []),
                ...(ext.tiktok_id    ? [{ type: 'TIKTOK',    identifier: ext.tiktok_id,    name: p.name, social_url: `https://tiktok.com/@${ext.tiktok_id}`,              linked_talent: talentId, updated_at: now }] : []),
                ...(ext.twitter_id   ? [{ type: 'TWITTER',   identifier: ext.twitter_id,   name: p.name, social_url: `https://twitter.com/${ext.twitter_id}`,             linked_talent: talentId, updated_at: now }] : []),
                ...(ext.facebook_id  ? [{ type: 'FACEBOOK',  identifier: ext.facebook_id,  name: p.name, social_url: `https://facebook.com/${ext.facebook_id}`,           linked_talent: talentId, updated_at: now }] : []),
                ...(ext.wikidata_id  ? [{ type: 'WIKIDATA',  identifier: ext.wikidata_id,  name: p.name, social_url: `https://www.wikidata.org/wiki/${ext.wikidata_id}`,  linked_talent: talentId, updated_at: now }] : []),
            ];

            const { error: socialsErr } = await supabase
                .from('hb_socials')
                .upsert(socials, { onConflict: 'type,identifier' });
            if (socialsErr) console.warn(`   ⚠️  Socials upsert: ${socialsErr.message}`);
            else {
                // Update the person map so later pages benefit from this run
                tmdbPersonMap.set(String(p.id), talentId);
                console.log(`   🔗 ${socials.length} socials upserted`);
            }

        } catch (err: any) {
            console.error(`   ❌ ${personSummary.name}: ${err.message}`);
            errorCount++;
        }
    }

    const durationSecs = Math.round((Date.now() - runStart) / 1000);
    const summaryObj = {
        people_processed: allPeople.length,
        talent_updated:   updatedCount,
        talent_created:   createdCount,
        errors:           errorCount,
        pages_fetched:    pageLimit,
        run_at:           new Date().toISOString(),
    };
    console.log(`\n🎉 Done! ${updatedCount} updated, ${createdCount} new, ${errorCount} errors (${durationSecs}s)`);
    await updateWorkflowSummary('success', summaryObj, durationSecs);
}

run().catch(async (err) => {
    console.error('🔥 Fatal:', err.message);
    await updateWorkflowSummary('failure', { error: err.message, run_at: new Date().toISOString() }, 0);
    process.exit(1);
});
