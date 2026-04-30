import { supabase } from './supabase';
import * as dotenv from 'dotenv';
dotenv.config();

const TMDB_BEARER_TOKEN = process.env.TMDB_BEARER_TOKEN!;
const MAX_PAGES    = parseInt(process.env.MAX_PAGES  || '99'); // 99 = all available pages
const SLEEP_MS     = parseInt(process.env.SLEEP_MS   || '150');
const UPSERT_CHUNK = 5; // small chunks to avoid lock contention on hb_socials

const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

// ─── HELPERS ─────────────────────────────────────────────────────────────────

function chunk<T>(arr: T[], n: number): T[][] {
    const out: T[][] = [];
    for (let i = 0; i < arr.length; i += n) out.push(arr.slice(i, i + n));
    return out;
}

function deduplicateSocials(rows: any[]): any[] {
    const seen = new Set<string>();
    return rows.filter(r => {
        const key = `${r.type}::${r.identifier}`;
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
    });
}

// Upsert in small chunks with retry — prevents statement timeout from lock contention
async function batchUpsertSocials(rows: any[]): Promise<void> {
    const deduped = deduplicateSocials(rows);
    for (const batch of chunk(deduped, UPSERT_CHUNK)) {
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

// ─── TMDB ─────────────────────────────────────────────────────────────────────

async function fetchTMDB(endpoint: string): Promise<any> {
    const res = await fetch(`https://api.themoviedb.org/3${endpoint}`, {
        headers: { Authorization: `Bearer ${TMDB_BEARER_TOKEN}`, accept: 'application/json' }
    });
    if (!res.ok) throw new Error(`TMDB ${res.status}: ${res.statusText} — ${endpoint}`);
    return res.json();
}

// ─── SUPABASE HELPERS ─────────────────────────────────────────────────────────

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

// ─── COUNTRIES ────────────────────────────────────────────────────────────────

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

// ─── MAIN ─────────────────────────────────────────────────────────────────────

async function run() {
    console.log('🎬 TMDb Now Playing Mining — Starting');

    await loadCountries();

    const tmdbPersonMap = await loadTmdbPersonMap();
    console.log(`   ✅ Pre-loaded ${tmdbPersonMap.size} existing TMDB person records`);

    // In-run cache — actors appearing in multiple films are only fetched once
    const personCache = new Map<number, string>();
    for (const [id, talentId] of tmdbPersonMap) personCache.set(parseInt(id), talentId);

    // Fetch page 1 to get total_pages, then fetch remaining pages
    const firstPage  = await fetchTMDB('/movie/now_playing?language=en-US&page=1');
    const totalPages = Math.min(firstPage.total_pages ?? 1, MAX_PAGES);
    console.log(`   📋 ${firstPage.total_results} films across ${totalPages} page(s)\n`);

    const allMovies: any[] = [...(firstPage.results ?? [])];
    for (let page = 2; page <= totalPages; page++) {
        const { results } = await fetchTMDB(`/movie/now_playing?language=en-US&page=${page}`);
        if (results?.length) allMovies.push(...results);
    }

    let totalMedia = 0, totalTalentNew = 0;

    for (const m of allMovies) {
        try {
            console.log(`\n🎞️  ${m.title} (TMDB: ${m.id})`);

            // Full detail + credits in one API call
            const movie = await fetchTMDB(
                `/movie/${m.id}?append_to_response=credits,external_ids&language=en-US`
            );
            await sleep(SLEEP_MS);

            // ── Upsert hb_media ──────────────────────────────────────────────
            const { data: mediaRow, error: mediaErr } = await supabase
                .from('hb_media')
                .upsert({
                    name:         movie.title,
                    date_release: movie.release_date || null,
                    about:        movie.overview     || null,
                    image:        movie.poster_path  ? `https://image.tmdb.org/t/p/w500${movie.poster_path}` : null,
                    genres:       movie.genres?.map((g: any) => g.name) ?? [],
                    soc_tmdb_id:  String(movie.id),
                    soc_tmdb:     `https://www.themoviedb.org/movie/${movie.id}`,
                    soc_imdb_id:  movie.external_ids?.imdb_id || null,
                    soc_imdb:     movie.external_ids?.imdb_id
                                    ? `https://www.imdb.com/title/${movie.external_ids.imdb_id}/`
                                    : null,
                    media_type:   'movie',
                    credits:      movie.credits ?? null,
                    updated_at:   new Date().toISOString(),
                }, { onConflict: 'soc_tmdb_id' })
                .select('id')
                .single();

            if (mediaErr) {
                console.error(`   ❌ hb_media error: ${mediaErr.message}`);
                continue;
            }
            totalMedia++;

            // ── Process top cast ─────────────────────────────────────────────
            const topCast     = movie.credits?.cast?.slice(0, 10) ?? [];
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
                        birth_location: p.place_of_birth || null,
                        biography:      p.biography      || null,
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

                    // Queue socials for chunked batch upsert
                    const ext = p.external_ids ?? {};
                    const now = new Date().toISOString();
                    socialsQueue.push(
                        { type: 'TMDB',      identifier: String(p.id),          name: p.name, social_url: `https://www.themoviedb.org/person/${p.id}`, linked_talent: talentId, updated_at: now },
                        ...(ext.imdb_id      ? [{ type: 'IMDB',      identifier: ext.imdb_id,      name: p.name, social_url: `https://www.imdb.com/name/${ext.imdb_id}/`,        linked_talent: talentId, updated_at: now }] : []),
                        ...(ext.instagram_id ? [{ type: 'INSTAGRAM', identifier: ext.instagram_id, name: p.name, social_url: `https://instagram.com/${ext.instagram_id}`,         linked_talent: talentId, updated_at: now }] : []),
                        ...(ext.tiktok_id    ? [{ type: 'TIKTOK',    identifier: ext.tiktok_id,    name: p.name, social_url: `https://tiktok.com/@${ext.tiktok_id}`,              linked_talent: talentId, updated_at: now }] : []),
                        ...(ext.twitter_id   ? [{ type: 'TWITTER',   identifier: ext.twitter_id,   name: p.name, social_url: `https://twitter.com/${ext.twitter_id}`,             linked_talent: talentId, updated_at: now }] : []),
                    );

                } catch (err: any) {
                    console.error(`   ⚠️  ${cast.name}: ${err.message}`);
                }
            }

            // ── Batch upsert socials in chunks of ${UPSERT_CHUNK} ───────────
            if (socialsQueue.length > 0) await batchUpsertSocials(socialsQueue);

            // ── Link talent to media ─────────────────────────────────────────
            if (talentIds.length > 0) {
                await supabase.from('hb_media').update({ linked_talent: talentIds }).eq('id', mediaRow.id);
            }

            console.log(`   ✅ ${talentIds.length} talent linked, ${socialsQueue.length} socials upserted`);

        } catch (err: any) {
            console.error(`   ❌ ${m.title}: ${err.message}`);
        }
    }

    console.log(`\n🎉 Done! ${totalMedia} films processed, ${totalTalentNew} new talent added`);
}

run().catch(err => {
    console.error('🔥 Fatal:', err.message);
    process.exit(1);
});
