import { supabase } from './supabase';
import { fetchTmdbPopularPeople, sleep, getApiStats, getImageUrl, SLEEP_MS } from './tmdb-api';
import { updateWorkflowHeartbeat } from './airtable-heartbeat';
import * as dotenv from 'dotenv';
import fetch from 'node-fetch';
dotenv.config();

const MAX_PAGES = parseInt(process.env.MAX_PAGES || '10');
const WORKFLOW_NAME = 'TMDb Popular Talent Mining';

async function logSystemBug(error: any) {
    const AIRTABLE_PAT = process.env.AIRTABLE_PAT;
    const AIRTABLE_BASE_ID = process.env.AIRTABLE_BASE_ID || 'appCGet4ar0zgtgyj';
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

async function minePopular() {
    console.log(`\n🎬 Starting ${WORKFLOW_NAME}`);
    console.log(`   Fetching up to ${MAX_PAGES} pages of popular people (${MAX_PAGES * 20} max)\n`);

    // HEARTBEAT: START
    await updateWorkflowHeartbeat('Running', `Starting discovery: ${MAX_PAGES} pages requested.`);

    const allPeople: any[] = [];
    let totalUpserted = 0;

    for (let page = 1; page <= MAX_PAGES; page++) {
        try {
            const result = await fetchTmdbPopularPeople(page);
            if (!result?.results?.length) break;

            allPeople.push(...result.results);
            console.log(`   Page ${page}/${result.total_pages}: fetched ${result.results.length} people`);

            if (page >= result.total_pages) break;
            await sleep(SLEEP_MS);
        } catch (err: any) {
            console.error(`   ❌ TMDb API Error on page ${page}: ${err.message}`);
            throw err; 
        }
    }

    if (!allPeople.length) {
        console.log('✅ No popular people returned from TMDb.');
        // HEARTBEAT: FINISH (No data)
        await updateWorkflowHeartbeat('Ready', 'Success: No new records to process.');
        return;
    }

    console.log(`\n   Total popular people to process: ${allPeople.length}`);

    const upsertData = allPeople.map(person => ({
        type: 'TMDB',
        identifier: String(person.id),
        soc_tmdb: `https://www.themoviedb.org/person/${person.id}`,
        name: person.name,
        image: getImageUrl(person.profile_path),
        detailed_array: {
            known_for_department: person.known_for_department || null,
            gender: person.gender ?? null,
            known_for: person.known_for || [],
            tmdb_popularity: person.popularity
        },
        updated_at: new Date().toISOString()
    }));

    console.log(`   Upserting ${upsertData.length} records to hb_socials...`);
    
    const CHUNK_SIZE = 100;
    for (let i = 0; i < upsertData.length; i += CHUNK_SIZE) {
        const chunk = upsertData.slice(i, i + CHUNK_SIZE);
        const { error } = await supabase
            .from('hb_socials')
            .upsert(chunk, { onConflict: 'type,identifier' });

        if (error) {
            console.error(`   ❌ Upsert error: ${error.message}`);
        } else {
            console.log(`   ✅ Chunk ${Math.floor(i/CHUNK_SIZE) + 1} uploaded (${chunk.length} records).`);
            totalUpserted += chunk.length;
        }
    }

    const stats = getApiStats();
    console.log(`\n🎉 Done! API Success Rate: ${stats.successRate}% (${stats.totalApiCalls} calls)\n`);

    // HEARTBEAT: FINISH
    await updateWorkflowHeartbeat('Ready', `Success: ${totalUpserted} records processed. API Success Rate: ${stats.successRate}%`);
}

minePopular().catch(async (error) => {
    console.error('🔥 FATAL ERROR:', error);
    // HEARTBEAT: ERROR
    await updateWorkflowHeartbeat('Errors', `Fatal Error: ${error.message || String(error)}`);
    await logSystemBug(error);
    process.exit(1);
});
