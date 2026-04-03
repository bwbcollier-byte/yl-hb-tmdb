import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
import { updateWorkflowHeartbeat } from './airtable-heartbeat';

dotenv.config();

const SUPABASE_URL = process.env.SUPABASE_URL!;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY!;
const TMDB_BEARER_TOKEN = process.env.TMDB_BEARER_TOKEN!;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

// --- DYNAMIC ENV TARGETS ---
const ENDPOINT = process.env.ENDPOINT || process.env.MINING_ENDPOINT || '/movie/popular'; 
const MAX_PAGES = parseInt(process.env.MAX_PAGES || '5');
const MEDIA_TYPE = process.env.MEDIA_TYPE || (ENDPOINT.includes('/movie') ? 'movie' : 'tv');

// --- HELPERS ---

let COUNTRIES_CACHE: { name: string, code: string }[] = [];

async function loadCountries() {
    if (COUNTRIES_CACHE.length > 0) return;
    const { data } = await supabase.from('countries').select('name, country_code');
    if (data) COUNTRIES_CACHE = data.map(c => ({ name: c.name, code: c.country_code }));
}

function parseCountryCode(placeOfBirth: string | null): string | null {
    if (!placeOfBirth || COUNTRIES_CACHE.length === 0) return null;
    for (const country of COUNTRIES_CACHE) {
        if (placeOfBirth.toLowerCase().includes(country.name.toLowerCase())) {
            return country.code?.toUpperCase() || null;
        }
    }
    return null;
}

async function fetchTMDB(endpoint: string) {
    const url = `https://api.themoviedb.org/3${endpoint}`;
    const response = await fetch(url, {
        headers: { Authorization: `Bearer ${TMDB_BEARER_TOKEN}`, accept: 'application/json' }
    });
    if (!response.ok) throw new Error(`TMDb API Error [${response.status}]: ${response.statusText}`);
    return response.json() as any;
}

async function getOrCreateSocial(type: string, identifier: string, name: string, url: string, talentId: string) {
    const { data, error } = await supabase
        .from('hb_socials')
        .upsert({ type, identifier, name, social_url: url, linked_talent: talentId, updated_at: new Date().toISOString() }, { onConflict: 'type,identifier' })
        .select('id').single();
    return data?.id || null;
}

// --- MASTER RECRUITMENT ENGINE ---

async function run() {
    console.log(`\n🚀 Starting Mega Enrichment Engine: ${ENDPOINT} (${MEDIA_TYPE})`);
    await updateWorkflowHeartbeat('Running', `Scanning ${ENDPOINT} for discovery: ${MAX_PAGES} pages requested.`);
    await loadCountries();

    let totalMediaProcessed = 0;
    let totalTalentRecruited = 0;

    for (let page = 1; page <= MAX_PAGES; page++) {
        console.log(`\n📄 Processing Page ${page}/${MAX_PAGES}...`);
        const { results: items } = await fetchTMDB(`${ENDPOINT}${ENDPOINT.includes('?') ? '&' : '?'}language=en-US&page=${page}`);
        
        if (!items || items.length === 0) break;

        for (const item of items) {
            try {
                const tmdbId = item.id;
                console.log(`\n🏨 PROMOTING MEDIA: ${item.title || item.name} (TMDB: ${tmdbId})`);
                
                // 1. Deep Media Scrape
                const detail = await fetchTMDB(`/${MEDIA_TYPE}/${tmdbId}?append_to_response=credits,external_ids&language=en-US`);
                
                const mediaPayload: any = {
                    name: detail.title || detail.name,
                    date_release: detail.release_date || detail.first_air_date,
                    about: detail.overview,
                    image: detail.poster_path ? `https://image.tmdb.org/t/p/w500${detail.poster_path}` : null,
                    genres: detail.genres?.map((g: any) => g.name) || [],
                    soc_tmdb_id: String(detail.id),
                    media_type: MEDIA_TYPE,
                    tmdb_credits: detail.credits || [],
                    updated_at: new Date().toISOString()
                };

                // 2. Upsert Media Profile
                const { data: mediaRecord, error: mediaError } = await supabase
                    .from('media')
                    .upsert(mediaPayload, { onConflict: 'soc_tmdb_id' })
                    .select('id').single();
                
                if (mediaError) throw new Error(`Media Promotion Error: ${mediaError.message}`);
                const mediaDbId = mediaRecord.id;
                totalMediaProcessed++;

                // 3. RECRUIT TOP CAST (The Recruitment Engine)
                const topCast = detail.credits?.cast?.slice(0, 10) || [];
                const talentUuids: string[] = [];

                console.log(`   🤝 Casting Call: Discovering ${topCast.length} potential talent...`);
                for (const castItem of topCast) {
                    try {
                        // Detailed Person Scrape
                        const p = await fetchTMDB(`/person/${castItem.id}?append_to_response=external_ids,images&language=en-US`);
                        
                        const talentBasePayload: any = {
                            name: p.name,
                            image: p.profile_path ? `https://image.tmdb.org/t/p/w500${p.profile_path}` : null,
                            status: 'Ready',
                            act_type: p.known_for_department || 'Acting',
                            gender: p.gender === 1 ? 'Female' : p.gender === 2 ? 'Male' : 'Unknown',
                            birth_location: p.place_of_birth,
                            biography: p.biography,
                            category: 'Film & Television',
                            birth_country: parseCountryCode(p.place_of_birth),
                            updated_at: new Date().toISOString()
                        };

                        // Talent Matchmaking (By TMDB Social Handle)
                        const { data: existingSocial } = await supabase.from('hb_socials').select('linked_talent').eq('type', 'TMDB').eq('identifier', String(p.id)).maybeSingle();
                        let talentId = existingSocial?.linked_talent;

                        if (talentId) {
                            await supabase.from('hb_talent').update(talentBasePayload).eq('id', talentId);
                        } else {
                            const { data: newTalent } = await supabase.from('hb_talent').insert({ ...talentBasePayload, created_at: new Date().toISOString() }).select('id').single();
                            talentId = newTalent?.id;
                        }

                        if (talentId) {
                            talentUuids.push(talentId);
                            totalTalentRecruited++;
                            
                            // Provision & Link Socials
                            const socialLinker = [
                                { type: 'TMDB', id: String(p.id), url: `https://tmdb.org/person/${p.id}`, field: 'soc_tmdb' },
                                { type: 'INSTAGRAM', id: p.external_ids?.instagram_id, url: p.external_ids?.instagram_id ? `https://instagram.com/${p.external_ids.instagram_id}` : null, field: 'soc_instagram' },
                                { type: 'TIKTOK', id: p.external_ids?.tiktok_id, url: p.external_ids?.tiktok_id ? `https://tiktok.com/@${p.external_ids.tiktok_id}` : null, field: 'soc_tiktok' },
                                { type: 'IMDB', id: p.external_ids?.imdb_id, url: p.external_ids?.imdb_id ? `https://imdb.com/name/${p.external_ids.imdb_id}` : null, field: 'soc_imdb' }
                            ];

                            let linkedSocialIds: any = {};
                            for (const s of socialLinker) {
                                if (s.id && s.url) {
                                    const uuid = await getOrCreateSocial(s.type, s.id, p.name, s.url, talentId);
                                    if (uuid) linkedSocialIds[s.field] = uuid;
                                }
                            }
                            if (Object.keys(linkedSocialIds).length > 0) {
                                await supabase.from('hb_talent').update(linkedSocialIds).eq('id', talentId);
                            }
                        }
                    } catch (talentErr: any) {
                        console.error(`      ⚠️ Talent Scrape Error (${castItem.name}): ${talentErr.message}`);
                    }
                }

                // 4. Final Link: Media -> Talent
                if (talentUuids.length > 0) {
                    await supabase.from('media').update({ linked_talent: talentUuids }).eq('id', mediaDbId);
                    console.log(`   ✅ Film Linked to ${talentUuids.length} Top Talent.`);
                }
            } catch (mediaErr: any) {
                console.error(`   ❌ Record Error: ${mediaErr.message}`);
            }
        }
    }

    console.log(`\n🎉 Done! Processed ${totalMediaProcessed} Media and ${totalTalentRecruited} Talent.`);
    await updateWorkflowHeartbeat('Ready', `Success: Processed ${totalMediaProcessed} media and ${totalTalentRecruited} talent.`);
}

run().catch(async (error) => {
    console.error('🔥 FATAL ERROR:', error);
    await updateWorkflowHeartbeat('Errors', `Fatal Error: ${error.message || String(error)}`);
    process.exit(1);
});
