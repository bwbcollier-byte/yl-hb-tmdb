import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
import { updateWorkflowHeartbeat } from './airtable-heartbeat';

dotenv.config();

const SUPABASE_URL = process.env.SUPABASE_URL!;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY!;
const TMDB_BEARER_TOKEN = process.env.TMDB_BEARER_TOKEN!;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

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

// --- CORE LOGIC ---

async function run() {
    await updateWorkflowHeartbeat('Running', 'Scraping "Now Playing" films and promoting their top cast to hb_talent.');
    await loadCountries();

    console.log('🎬 Starting TMDb Film Now Playing & Talent Recruitment Engine');
    
    // 1. Get Now Playing List
    const { results: movies } = await fetchTMDB('/movie/now_playing?language=en-US&page=1');
    console.log(`🔍 Found ${movies.length} films currently playing in theaters.\n`);

    for (const m of movies) {
        try {
            console.log(`\n🎞️ PROMOTING FILM: ${m.title} (ID: ${m.id})`);
            
            // 2. Deep Scrape Movie Details
            const movie = await fetchTMDB(`/movie/${m.id}?append_to_response=credits,external_ids&language=en-US`);
            
            const mediaPayload: any = {
                name: movie.title,
                date_release: movie.release_date,
                about: movie.overview,
                image: movie.poster_path ? `https://image.tmdb.org/t/p/w500${movie.poster_path}` : null,
                genres: movie.genres?.map((g: any) => g.name) || [],
                soc_tmdb_id: String(movie.id),
                media_type: 'movie',
                tmdb_credits: movie.credits || [],
                updated_at: new Date().toISOString()
            };

            // 3. Upsert to public.media
            const { data: mediaRecord, error: mediaError } = await supabase
                .from('media')
                .upsert(mediaPayload, { onConflict: 'soc_tmdb_id' })
                .select('id').single();
            
            if (mediaError) throw new Error(`Media Promotion Error: ${mediaError.message}`);
            const mediaId = mediaRecord.id;

            // 4. RECRUIT TOP CAST (Discovery Chain)
            const topCast = movie.credits?.cast?.slice(0, 10) || []; // Top 10 actors
            const talentUuids: string[] = [];

            console.log(`   🤝 Discovering Talent: Analyzing top ${topCast.length} cast members...`);
            for (const cast of topCast) {
                try {
                    // Deep Scrape Person Profile
                    const p = await fetchTMDB(`/person/${cast.id}?append_to_response=external_ids,images&language=en-US`);
                    
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

                    // Check for existing connection via TMDB ID
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
                        
                        // Provision Socials
                        const socials = [
                            { type: 'TMDB', id: String(p.id), url: `https://tmdb.org/person/${p.id}`, field: 'soc_tmdb' },
                            { type: 'INSTAGRAM', id: p.external_ids?.instagram_id, url: p.external_ids?.instagram_id ? `https://instagram.com/${p.external_ids.instagram_id}` : null, field: 'soc_instagram' },
                            { type: 'TIKTOK', id: p.external_ids?.tiktok_id, url: p.external_ids?.tiktok_id ? `https://tiktok.com/@${p.external_ids.tiktok_id}` : null, field: 'soc_tiktok' },
                            { type: 'IMDB', id: p.external_ids?.imdb_id, url: p.external_ids?.imdb_id ? `https://imdb.com/name/${p.external_ids.imdb_id}` : null, field: 'soc_imdb' }
                        ];

                        let linkedSocialIds: any = {};
                        for (const s of socials) {
                            if (s.id && s.url) {
                                const uuid = await getOrCreateSocial(s.type, s.id, p.name, s.url, talentId);
                                if (uuid) linkedSocialIds[s.field] = uuid;
                            }
                        }
                        if (Object.keys(linkedSocialIds).length > 0) {
                            await supabase.from('hb_talent').update(linkedSocialIds).eq('id', talentId);
                        }
                    }
                } catch (err: any) {
                    console.error(`      ❌ Talent Scrape Error (${cast.name}): ${err.message}`);
                }
            }

            // 5. Final Media Sync (Link Talent to Film)
            if (talentUuids.length > 0) {
                await supabase.from('media').update({ linked_talent: talentUuids }).eq('id', mediaId);
                console.log(`   ✅ Film Fully Enriched & Linked to ${talentUuids.length} Top Talent.`);
            }

        } catch (err: any) {
            console.error(`   ❌ Film Processing Error: ${err.message}`);
        }
    }

    console.log('\n🎉 Now Playing Induction Complete!');
    await updateWorkflowHeartbeat('Ready', `Success: Imported ${movies.length} films and promoted their lead cast to hb_talent.`);
}

run().catch(async err => {
    console.error('Fatal Error:', err);
    await updateWorkflowHeartbeat('Errors', `Fatal Error: ${err.message}`);
});
