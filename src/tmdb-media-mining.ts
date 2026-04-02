import { supabase } from './supabase';
import { fetchTmdbMediaMining, getImageUrl, sleep, getApiStats, SLEEP_MS } from './tmdb-api';
import * as dotenv from 'dotenv';
import fetch from 'node-fetch';
dotenv.config();

const PAGES = parseInt(process.env.PAGES || '5');
const ENDPOINT = process.env.ENDPOINT || '/movie/popular'; 
const BATCH_SIZE = 100;
const WORKFLOW_NAME = `TMDb Media Mining (${ENDPOINT})`;

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

async function runMining() {
    console.log(`\n🚀 Starting ${WORKFLOW_NAME}`);
    console.log(`   Pages: ${PAGES}, Batch Size: ${BATCH_SIZE}\n`);

    for (let page = 1; page <= PAGES; page++) {
        console.log(`\n📄 Processing Page ${page}/${PAGES}...`);
        const data = await fetchTmdbMediaMining(ENDPOINT, page);

        if (!data || !data.results?.length) {
            console.warn(`   ⚠️ No data found on page ${page}`);
            break;
        }

        const results = data.results;
        const upsertBatch: any[] = [];

        for (const item of results) {
            const tmdbId = item.id;
            const isMovie = ENDPOINT.includes('/movie') || (item.media_type && item.media_type === 'movie');
            const typeValue = isMovie ? 'movie' : 'tv';
            
            upsertBatch.push({
                name: (isMovie ? item.title : item.name) || 'Unknown',
                media_type: typeValue, // FIXED: Renamed to media_type
                soc_tmdb_id: String(tmdbId),
                soc_tmdb: `https://www.themoviedb.org/${typeValue}/${tmdbId}`,
                image: getImageUrl(item.poster_path),
                about: item.overview || null,
                date_release: isMovie ? item.release_date : item.first_air_date,
                rating: item.vote_average,
                stats_boxoffice_financial: {
                    tmdb_popularity: item.popularity,
                    tmdb_vote_count: item.vote_count
                },
                updated_at: new Date().toISOString()
            });
        }

        if (upsertBatch.length > 0) {
            const { error: upsertError } = await supabase
                .from('media')
                .upsert(upsertBatch, { onConflict: 'soc_tmdb_id' });

            if (upsertError) {
                console.error(`   ❌ Batch Upsert Error (Page ${page}):`, upsertError.message);
            } else {
                console.log(`   ✅ Page ${page} Upserted: ${upsertBatch.length} records.`);
            }
        }

        await sleep(SLEEP_MS);
    }

    const stats = getApiStats();
    console.log(`\n🎉 Done! API Success Rate: ${stats.successRate}% (${stats.totalApiCalls} calls)\n`);
}

runMining().catch(async (error) => {
    console.error('🔥 FATAL ERROR:', error);
    await logSystemBug(error);
    process.exit(1);
});
