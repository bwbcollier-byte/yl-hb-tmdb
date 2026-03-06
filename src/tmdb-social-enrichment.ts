import { supabase } from './supabase';
import { fetchTmdbPerson, sleep, getApiStats, getImageUrl, SLEEP_MS } from './tmdb-api';
import * as dotenv from 'dotenv';
dotenv.config();

const LIMIT_ENV = process.env.LIMIT;
const RUN_ALL = !LIMIT_ENV || LIMIT_ENV.trim() === '';
const LIMIT = RUN_ALL ? 999999 : parseInt(LIMIT_ENV as string);
const WORKFLOW_NAME = 'TMDb Social Enrichment';

async function processProfiles() {
    console.log(`\n🎬 Starting ${WORKFLOW_NAME}`);
    console.log(`   Limit: ${RUN_ALL ? 'All' : LIMIT} records\n`);

    // Fetch TMDB social profiles ordered so unprocessed ones come first
    const { data: profiles, error } = await supabase
        .from('social_profiles')
        .select('id, talent_id, social_id, social_url, name')
        .eq('social_type', 'TMDB')
        .order('tmdb_check', { ascending: true, nullsFirst: true })
        .order('last_processed', { ascending: true, nullsFirst: true })
        .limit(LIMIT);

    if (error) { console.error('❌ Fetch error:', error.message); return; }
    if (!profiles?.length) { console.log('✅ No TMDB social profiles to process.'); return; }

    console.log(`   Found ${profiles.length} TMDB social profiles.\n`);

    let processedCount = 0, successCount = 0, failedCount = 0;

    for (const profile of profiles) {
        processedCount++;
        const personId = profile.social_id;
        console.log(`[${processedCount}/${profiles.length}] Person ID: ${personId} — ${profile.name || profile.id}`);

        // Skip missing or non-numeric IDs (e.g. 'not.found' placeholder values)
        if (!personId || !/^\d+$/.test(String(personId))) {
            console.log(`   ⏭️  Skipping invalid social_id: "${personId}"`);
            await supabase
                .from('social_profiles')
                .update({ tmdb_check: 'invalid_id', last_checked: new Date().toISOString() })
                .eq('id', profile.id);
            continue;
        }

        const data = await fetchTmdbPerson(personId);

        if (!data) {
            failedCount++;
            await supabase
                .from('social_profiles')
                .update({ tmdb_check: 'not_found', last_checked: new Date().toISOString() })
                .eq('id', profile.id);
            continue;
        }

        // Sort profile images by vote_average, take top 5
        const profileImages = (data.images?.profiles || [])
            .sort((a: any, b: any) => b.vote_average - a.vote_average)
            .slice(0, 5)
            .map((img: any) => getImageUrl(img.file_path))
            .filter(Boolean);

        const bestImage = getImageUrl(data.profile_path) || profileImages[0] || null;

        const updates: Record<string, any> = {
            name: data.name || profile.name,
            social_about: data.biography || null,
            social_image: bestImage,
            tmdb_check: 'success',
            tmdb_birthday: data.birthday || null,
            tmdb_deathday: data.deathday || null,
            tmdb_gender: data.gender ?? null,
            tmdb_known_for: data.known_for_department || null,
            tmdb_place_of_birth: data.place_of_birth || null,
            tmdb_popularity: data.popularity ?? null,
            tmdb_imdb_id: data.external_ids?.imdb_id || data.imdb_id || null,
            tmdb_instagram_id: data.external_ids?.instagram_id || null,
            tmdb_twitter_id: data.external_ids?.twitter_id || null,
            tmdb_facebook_id: data.external_ids?.facebook_id || null,
            tmdb_tiktok_id: data.external_ids?.tiktok_id || null,
            tmdb_wikidata_id: data.external_ids?.wikidata_id || null,
            tmdb_images: profileImages.length > 0 ? JSON.stringify(profileImages) : null,
            last_processed: new Date().toISOString(),
            last_checked: new Date().toISOString(),
            updated_at: new Date().toISOString(),
            workflow_logs: { last_run: new Date().toISOString(), workflow: WORKFLOW_NAME }
        };

        const { error: updateError } = await supabase
            .from('social_profiles')
            .update(updates)
            .eq('id', profile.id);

        if (updateError) {
            console.error(`   ❌ Update error: ${updateError.message}`);
            failedCount++;
        } else {
            successCount++;
            console.log(`   ✅ Enriched: ${data.name}`);
        }

        await sleep(SLEEP_MS);
    }

    const stats = getApiStats();
    console.log(`\n🎉 Done! Processed: ${processedCount}, Success: ${successCount}, Failed: ${failedCount}, API Success Rate: ${stats.successRate}%`);
}

processProfiles().catch(console.error);
