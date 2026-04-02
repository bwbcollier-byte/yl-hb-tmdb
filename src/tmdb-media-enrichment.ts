import { supabase } from './supabase';
import { fetchTmdbMovie, fetchTmdbTv, sleep, getApiStats, getImageUrl, SLEEP_MS } from './tmdb-api';
import * as dotenv from 'dotenv';
import fetch from 'node-fetch';
dotenv.config();

const LIMIT_ENV = process.env.LIMIT;
const RUN_ALL = !LIMIT_ENV || LIMIT_ENV.trim() === '';
const LIMIT = RUN_ALL ? 1000 : parseInt(LIMIT_ENV as string);
const WORKFLOW_NAME = 'TMDb Media Enrichment';

/**
 * Rule #3: Log System Bug to Airtable on fatal script failure
 */
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

async function processMedia() {
    console.log(`\n🎬 Starting ${WORKFLOW_NAME}`);
    console.log(`   Limit: ${RUN_ALL ? 'All (cap 1000)' : LIMIT} records\n`);

    // Fetch media records that have a tmdb_id, processed first
    const { data: mediaRecords, error } = await supabase
        .from('media')
        .select('*')
        .not('soc_tmdb_id', 'is', null)
        .order('updated_at', { ascending: true })
        .limit(LIMIT);

    if (error) throw error;
    if (!mediaRecords?.length) {
        console.log('✅ No media records with TMDb IDs to process.');
        return;
    }

    console.log(`   Found ${mediaRecords.length} media records to enrich.\n`);

    let processedCount = 0, successCount = 0, failedCount = 0;

    for (const record of mediaRecords) {
        processedCount++;
        const tmdbId = record.soc_tmdb_id;
        // FIXED: Using media_type column
        const mediaType = record.media_type === 'tv' || record.soc_tmdb?.includes('/tv/') ? 'tv' : 'movie';
        
        console.log(`[${processedCount}/${mediaRecords.length}] TMDb ${mediaType} ID: ${tmdbId} — ${record.name}`);

        const data = mediaType === 'tv'
            ? await fetchTmdbTv(tmdbId)
            : await fetchTmdbMovie(tmdbId);

        if (!data) {
            failedCount++;
            await supabase.from('media').update({ updated_at: new Date().toISOString() }).eq('id', record.id);
            continue;
        }

        const isMovie = mediaType !== 'tv';
        const extIds = data.external_ids || {};

        // Cast mapping with Talent IDs
        const rawCast = (data.credits?.cast || []).slice(0, 15);
        const castTmdbIds = rawCast.map((c: any) => String(c.id));
        
        const { data: linkedSocials } = await supabase
            .from('hb_socials')
            .select('talent_id, identifier')
            .eq('type', 'TMDB')
            .in('identifier', castTmdbIds);

        const talentLookup: Record<string, string> = {};
        linkedSocials?.forEach(s => { if (s.identifier) talentLookup[s.identifier] = s.talent_id; });

        const cast = rawCast.map((c: any) => ({
            tmdb_id: c.id,
            talent_id: talentLookup[String(c.id)] || null,
            name: c.name,
            character: c.character,
            profile_path: getImageUrl(c.profile_path)
        }));

        const directors = (data.credits?.crew || [])
            .filter((c: any) => c.job === 'Director' || c.job === 'Series Director')
            .map((c: any) => c.name);

        const updates: any = {
            name: (isMovie ? data.title : data.name) || record.name,
            about: data.overview || record.about,
            date_release: isMovie ? data.release_date : data.first_air_date,
            running_time: isMovie ? `${data.runtime} min` : (data.episode_run_time?.[0] ? `${data.episode_run_time[0]} min` : null),
            genres: (data.genres || []).map((g: any) => g.name),
            image: getImageUrl(record.image || data.poster_path),
            rating: data.vote_average,
            soc_tmdb: `https://www.themoviedb.org/${mediaType}/${tmdbId}`,
            soc_tmdb_id: tmdbId,
            soc_imdb_id: data.imdb_id || extIds.imdb_id || null,
            soc_imdb: (data.imdb_id || extIds.imdb_id) ? `https://www.imdb.com/title/${data.imdb_id || extIds.imdb_id}` : null,
            soc_instagram: extIds.instagram_id ? `https://instagram.com/${extIds.instagram_id}` : null,
            soc_website: data.homepage || null,
            soc_wikidata_id: extIds.wikidata_id || null,
            tmdb_credits: {
                cast: cast,
                directors: directors,
                production_companies: (data.production_companies || []).map((pc: any) => ({
                    id: pc.id,
                    name: pc.name,
                    logo_path: getImageUrl(pc.logo_path)
                }))
            },
            stats_boxoffice_financial: {
                popularity: data.popularity,
                vote_count: data.vote_count,
                status: data.status,
                tagline: data.tagline,
                original_language: data.original_language,
                budget: data.budget || null,
                revenue: data.revenue || null,
                facebook_id: extIds.facebook_id || null,
                twitter_id: extIds.twitter_id || null
            },
            updated_at: new Date().toISOString()
        };

        const { error: updateError } = await supabase
            .from('media')
            .update(updates)
            .eq('id', record.id);

        if (updateError) {
            console.error(`   ❌ Update error: ${updateError.message}`);
            failedCount++;
        } else {
            successCount++;
            console.log(`   ✅ Enriched: ${updates.name}`);
        }

        await sleep(SLEEP_MS);
    }

    const stats = getApiStats();
    console.log(`\n🎉 Done! Processed: ${processedCount}, Success: ${successCount}, Failed: ${failedCount}, API Success Rate: ${stats.successRate}%`);
}

processMedia().catch(async (error) => {
    console.error('🔥 FATAL ERROR:', error);
    await logSystemBug(error);
    process.exit(1);
});
