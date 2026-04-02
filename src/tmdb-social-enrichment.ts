import { supabase } from './supabase';
import { fetchTmdbPerson, sleep, getApiStats, getImageUrl, SLEEP_MS } from './tmdb-api';
import * as dotenv from 'dotenv';
import fetch from 'node-fetch';
dotenv.config();

const LIMIT_ENV = process.env.LIMIT;
const RUN_ALL = !LIMIT_ENV || LIMIT_ENV.trim() === '';
const LIMIT = RUN_ALL ? 1000 : parseInt(LIMIT_ENV as string);
const WORKFLOW_NAME = 'TMDb Talent Social Enrichment';

async function logSystemBug(error: any) {
    const AIRTABLE_PAT = process.env.AIRTABLE_PAT;
    const AIRTABLE_BASE_ID = process.env.AIRTABLE_BASE_ID || 'appXXXXXXXXXXXXX';
    if (!AIRTABLE_PAT) return;

    try {
        await fetch(`https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/tblTphXDvIezGmWae`, {
            method: 'POST',
            headers: {
                'Authorization': `Bearer ${AIRTABLE_PAT}`,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                records: [{
                    fields: {
                        'Name': `Script Failure: ${WORKFLOW_NAME}`,
                        'Details': error.message || String(error),
                        'Status': 'Todo',
                        'Severity': 'High',
                        'Context': 'Automated Script Error'
                    }
                }]
            })
        });
        console.log('🐞 System Bug logged to Airtable.');
    } catch (err) {
        console.error('❌ Failed to log bug to Airtable:', err);
    }
}

async function upsertLinkedSocial(talentId: string, socialType: string, socialId: string, name: string) {
    if (!socialId || socialId.trim() === '') return;

    let socColumn = '';
    let socUrl = '';

    if (socialType === 'Instagram') { socColumn = 'soc_instagram'; socUrl = `https://www.instagram.com/${socialId}/`; }
    else if (socialType === 'TikTok') { socColumn = 'soc_tiktok'; socUrl = `https://www.tiktok.com/@${socialId}`; }
    else if (socialType === 'Twitter') { socColumn = 'soc_twitter'; socUrl = `https://twitter.com/${socialId}`; }
    else if (socialType === 'Facebook') { socColumn = 'soc_facebook'; socUrl = `https://www.facebook.com/${socialId}`; }
    else if (socialType === 'IMDb') { socColumn = 'soc_imdb'; socUrl = `https://www.imdb.com/name/${socialId}/`; }

    if (!socColumn) return;

    const upsertPayload: any = {
        talent_id: talentId,
        type: socialType.toUpperCase(),
        identifier: socialId,
        name: name,
        updated_at: new Date().toISOString()
    };
    
    upsertPayload[socColumn] = socUrl;

    const { error } = await supabase
        .from('hb_socials')
        .upsert(upsertPayload, { onConflict: 'type,identifier' });

    if (error) {
        console.error(`      ⚠️ Failed to upsert linked ${socialType}: ${error.message}`);
    } else {
        console.log(`      🔗 Auto-linked ${socialType}: ${socialId}`);
    }
}

async function processProfiles() {
    console.log(`\n🎬 Starting ${WORKFLOW_NAME}`);
    console.log(`   Limit: ${RUN_ALL ? 'All (cap 1000)' : LIMIT} records\n`);

    const { data: profiles, error } = await supabase
        .from('hb_socials')
        .select('*')
        .eq('type', 'TMDB')
        .order('updated_at', { ascending: true })
        .limit(LIMIT);

    if (error) throw error;
    if (!profiles?.length) {
        console.log('✅ No TMDB social profiles to process.');
        return;
    }

    console.log(`   Found ${profiles.length} TMDB social profiles.\n`);

    let processedCount = 0, successCount = 0, failedCount = 0;

    for (const profile of profiles) {
        processedCount++;
        const personId = profile.identifier;
        console.log(`[${processedCount}/${profiles.length}] Identifier: ${personId} — ${profile.name || profile.id}`);

        if (!personId || !/^\d+$/.test(String(personId))) {
            console.log(`   ⏭️  Skipping invalid identifier: "${personId}"`);
            continue;
        }

        const data = await fetchTmdbPerson(personId);

        if (!data) {
            failedCount++;
            await supabase.from('hb_socials').update({ updated_at: new Date().toISOString() }).eq('id', profile.id);
            continue;
        }

        const extIds = data.external_ids || {};
        
        const updates: any = {
            name: data.name || profile.name,
            image: getImageUrl(data.profile_path),
            soc_instagram: extIds.instagram_id ? `https://instagram.com/${extIds.instagram_id}` : null,
            soc_twitter: extIds.twitter_id ? `https://twitter.com/${extIds.twitter_id}` : null,
            soc_tiktok: extIds.tiktok_id ? `https://tiktok.com/@${extIds.tiktok_id}` : null,
            soc_youtube: extIds.youtube_id ? `https://youtube.com/${extIds.youtube_id}/videos` : null,
            soc_facebook: extIds.facebook_id ? `https://facebook.com/${extIds.facebook_id}` : null,
            soc_wikidata_id: extIds.wikidata_id || null, // ADDED AS TOP LEVEL
            detailed_array: {
                ...(profile.detailed_array || {}),
                biography: data.biography || null,
                birthday: data.birthday || null,
                deathday: data.deathday || null,
                place_of_birth: data.place_of_birth || null,
                known_for_department: data.known_for_department || null,
                imdb_id: extIds.imdb_id || null,
                tmdb_popularity: data.popularity
            },
            updated_at: new Date().toISOString()
        };

        const { error: updateError } = await supabase
            .from('hb_socials')
            .update(updates)
            .eq('id', profile.id);

        if (updateError) {
            console.error(`   ❌ Update error: ${updateError.message}`);
            failedCount++;
        } else {
            successCount++;
            console.log(`   ✅ Enriched: ${data.name}`);

            if (profile.talent_id) {
                if (extIds.instagram_id) await upsertLinkedSocial(profile.talent_id, 'Instagram', extIds.instagram_id, data.name);
                if (extIds.tiktok_id) await upsertLinkedSocial(profile.talent_id, 'TikTok', extIds.tiktok_id, data.name);
                if (extIds.twitter_id) await upsertLinkedSocial(profile.talent_id, 'Twitter', extIds.twitter_id, data.name);
                if (extIds.facebook_id) await upsertLinkedSocial(profile.talent_id, 'Facebook', extIds.facebook_id, data.name);
                if (extIds.imdb_id) await upsertLinkedSocial(profile.talent_id, 'IMDb', extIds.imdb_id, data.name);
            }
        }

        await sleep(SLEEP_MS);
    }

    const stats = getApiStats();
    console.log(`\n🎉 Done! Processed: ${processedCount}, Success: ${successCount}, Failed: ${failedCount}, API Success Rate: ${stats.successRate}%`);
}

processProfiles().catch(async (error) => {
    console.error('🔥 FATAL ERROR:', error);
    await logSystemBug(error);
    process.exit(1);
});
