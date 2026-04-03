import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
import { updateWorkflowHeartbeat } from './airtable-heartbeat';

dotenv.config();

const SUPABASE_URL = process.env.SUPABASE_URL!;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY!;
const TMDB_BEARER_TOKEN = process.env.TMDB_BEARER_TOKEN!;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

async function fetchFullProfile(personId: number) {
    const url = `https://api.themoviedb.org/3/person/${personId}?append_to_response=external_ids,images,combined_credits&language=en-US`;
    const response = await fetch(url, {
        headers: {
            Authorization: `Bearer ${TMDB_BEARER_TOKEN}`,
            accept: 'application/json'
        }
    });
    if (!response.ok) throw new Error(`TMDb API Error: ${response.statusText}`);
    return response.json() as any;
}

async function getOrCreateSocial(type: string, identifier: string, name: string, url: string, talentId?: string) {
    const { data, error } = await supabase
        .from('hb_socials')
        .upsert({
            type,
            identifier,
            name,
            social_url: url,
            linked_talent: talentId,
            updated_at: new Date().toISOString()
        }, { onConflict: 'type,identifier' })
        .select('id')
        .single();

    if (error) {
        console.error(`   ❌ Social Error (${type}): ${error.message}`);
        return null;
    }
    return data.id;
}

async function run() {
    // HEARTBEAT: START
    await updateWorkflowHeartbeat('Running', 'Fetching popular talent and performing deep-dive profile scrapes to promote them to hb_talent.');

    console.log('🎬 Starting TMDb Popular Talent Promotion Engine');
    
    // Set a safe limit for deep scrapes to avoid GitHub Action timeouts
    const maxPages = parseInt(process.env.MAX_PAGES || '3');
    let allPeople: any[] = [];

    for (let page = 1; page <= maxPages; page++) {
        const url = `https://api.themoviedb.org/3/person/popular?language=en-US&page=${page}`;
        const response = await fetch(url, {
            headers: {
                Authorization: `Bearer ${TMDB_BEARER_TOKEN}`,
                accept: 'application/json'
            }
        });
        
        if (!response.ok) break;
        const data = await response.json() as any;
        allPeople = allPeople.concat(data.results);
        console.log(`   Fetched page ${page}/${data.total_pages}`);
    }

    console.log(`\n🔍 Found ${allPeople.length} people. Starting Deep Profile Scrapes...\n`);
    let promotedCount = 0;

    for (const personSummary of allPeople) {
        try {
            console.log(`👤 Processing: ${personSummary.name} (ID: ${personSummary.id})`);
            const p = await fetchFullProfile(personSummary.id) as any;
            
            // 2. Prepare Talent Data - Populating ALL requested fields!
            const talentPayload: any = {
                name: p.name,
                image: p.profile_path ? `https://image.tmdb.org/t/p/w500${p.profile_path}` : null,
                status: 'Ready',
                xatid: null,
                spotify_id: null,
                act_type: p.known_for_department,
                gender: p.gender === 1 ? 'Female' : p.gender === 2 ? 'Male' : 'Unknown',
                birth_location: p.place_of_birth,
                biography: p.biography,
                category: 'Film & Television',
                updated_at: new Date().toISOString(),
                // UUID socials are left empty for now until provisioned
                soc_spotify: null,
                soc_instagram: null,
                soc_allmusic: null,
                soc_tiktok: null,
                soc_facebook: null,
                soc_soundcloud: null,
                soc_deezer: null,
                soc_twitter: null,
                soc_songkick: null,
                soc_wikipedia: null,
                soc_imdb: null,
                soc_tmdb: null,
                contacts_updated: null
            };

            // 3. Linkage Check via hb_socials
            const { data: existingSocial } = await supabase
                .from('hb_socials')
                .select('linked_talent')
                .eq('type', 'TMDB')
                .eq('identifier', String(p.id))
                .maybeSingle();

            let talentId = existingSocial?.linked_talent;

            // Fallback check by name
            if (!talentId) {
                const { data: existingByName } = await supabase
                    .from('hb_talent')
                    .select('id')
                    .eq('name', p.name)
                    .eq('category', 'Film & Television')
                    .maybeSingle();
                
                if (existingByName) {
                    talentId = existingByName.id;
                    console.log(`   🕵️‍♂️ Found existing Talent by name: ${talentId}`);
                }
            }

            if (talentId) {
                const { error: updateError } = await supabase.from('hb_talent').update(talentPayload).eq('id', talentId);
                if (updateError) throw new Error(`Talent Update Error: ${updateError.message}`);
                console.log(`   ✅ Updated existing Talent: ${talentId}`);
            } else {
                const { data: newTalent, error: talentError } = await supabase
                    .from('hb_talent')
                    .insert({                 ...talentPayload,
                        created_at: new Date().toISOString()
                    })
                    .select('id')
                    .single();
                
                if (talentError) throw new Error(`Talent Insert Error: ${talentError.message}`);
                talentId = newTalent.id;
                console.log(`   ✨ Created new Talent: ${talentId}`);
            }

            // 4. Provision All Socials and Link them to this Talent ID
            const socialsToProvision = [
                { type: 'TMDB', id: String(p.id), url: `https://www.themoviedb.org/person/${p.id}`, field: 'soc_tmdb' },
                { type: 'INSTAGRAM', id: p.external_ids?.instagram_id, url: p.external_ids?.instagram_id ? `https://instagram.com/${p.external_ids.instagram_id}` : null, field: 'soc_instagram' },
                { type: 'TIKTOK', id: p.external_ids?.tiktok_id, url: p.external_ids?.tiktok_id ? `https://tiktok.com/@${p.external_ids.tiktok_id}` : null, field: 'soc_tiktok' },
                { type: 'FACEBOOK', id: p.external_ids?.facebook_id, url: p.external_ids?.facebook_id ? `https://facebook.com/${p.external_ids.facebook_id}` : null, field: 'soc_facebook' },
                { type: 'TWITTER', id: p.external_ids?.twitter_id, url: p.external_ids?.twitter_id ? `https://twitter.com/${p.external_ids.twitter_id}` : null, field: 'soc_twitter' },
                { type: 'IMDB', id: p.external_ids?.imdb_id, url: p.external_ids?.imdb_id ? `https://imdb.com/name/${p.external_ids.imdb_id}` : null, field: 'soc_imdb' },
                { type: 'WIKIPEDIA', id: p.external_ids?.wikidata_id, url: p.external_ids?.wikidata_id ? `https://www.wikidata.org/wiki/${p.external_ids.wikidata_id}` : null, field: 'soc_wikipedia' }
            ];

            const linkedIds: any = {};
            for (const s of socialsToProvision) {
                if (s.id && s.url && s.id !== "") {
                    const uuid = await getOrCreateSocial(s.type, s.id, p.name, s.url, talentId);
                    if (uuid) linkedIds[s.field] = uuid;
                }
            }

            // 5. Final Connection: Update hb_talent with all the social UUIDs
            if (Object.keys(linkedIds).length > 0) {
                await supabase.from('hb_talent').update(linkedIds).eq('id', talentId);
                console.log(`   🔗 Linked ${Object.keys(linkedIds).length} socials to Talent Profile.`);
            }

            promotedCount++;
        } catch (err: any) {
            console.error(`   ❌ Error processing person: ${err.message}`);
        }
    }

    console.log(`\n🎉 Promotion Complete! ${promotedCount}/${allPeople.length} people processed.\n`);

    // HEARTBEAT: FINISH
    await updateWorkflowHeartbeat('Ready', `Success: ${promotedCount} talent discovered and officially promoted to hb_talent with full deep-dive metadata.`);
}

run().catch(async (err) => {
    console.error('Fatal execution error:', err);
    await updateWorkflowHeartbeat('Errors', `Fatal Error: ${err.message}`);
    process.exit(1);
});
