import { supabase } from './supabase';
import { findRecordByExternalId, sleep, getApiStats, SLEEP_MS } from './tmdb-api';
import * as dotenv from 'dotenv';
import fetch from 'node-fetch';
dotenv.config();

const LIMIT_ENV = process.env.LIMIT;
const LIMIT = LIMIT_ENV ? parseInt(LIMIT_ENV) : 50;
const WORKFLOW_NAME = 'TMDb Media Linker (IMDb -> TMDb)';

async function logSystemBug(error: any) {
    const AIRTABLE_PAT = process.env.AIRTABLE_PAT;
    const AIRTABLE_BASE_ID = process.env.AIRTABLE_BASE_ID || 'appXXXXXXXXXXXXX';
    if (!AIRTABLE_PAT) return;

    try {
        await fetch(`https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/System%20Bugs`, {
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
                        'Status': 'New',
                        'Source': 'TMDb Linker Script'
                    }
                }]
            })
        });
    } catch (err) {
        console.error('❌ Failed to log bug to Airtable:', err);
    }
}

async function linkMedia() {
    console.log(`\n🔗 Starting ${WORKFLOW_NAME}`);
    console.log(`   Scanning for media with IMDb IDs but missing TMDb IDs...\n`);

    // Rule #1: Scan for records with IMDb ID but NO TMDb ID
    const { data: records, error } = await supabase
        .from('media')
        .select('id, name, soc_imdb_id, soc_imdb')
        .is('soc_tmdb_id', null)
        .not('soc_imdb_id', 'is', null) // Must have an IMDb ID to match
        .limit(LIMIT);

    if (error) throw error;
    if (!records?.length) {
        console.log('✅ No records found needing TMDb linking.');
        return;
    }

    console.log(`   Found ${records.length} records to link.\n`);

    let linkedCount = 0, failedCount = 0;

    for (const record of records) {
        // Clean IMDb ID: it might be the full URL or just the ID
        let imdbId = record.soc_imdb_id || '';
        if (!imdbId && record.soc_imdb) {
            const match = record.soc_imdb.match(/(tt\d+)/);
            if (match) imdbId = match[1];
        }

        if (!imdbId) {
            console.log(`   ⚠️ Skip: ${record.name} (No valid IMDb ID found)`);
            continue;
        }

        console.log(`   🔍 Searching TMDb for IMDb ID: ${imdbId} (${record.name})`);
        const result = await findRecordByExternalId(imdbId);

        if (!result) {
            failedCount++;
            continue;
        }

        // TMDb /find returns results in arrays by type
        const movie = result.movie_results?.[0];
        const tv = result.tv_results?.[0];
        const found = movie || tv;

        if (found) {
            const tmdbId = found.id;
            const mediaType = movie ? 'movie' : 'tv';

            const { error: updateError } = await supabase
                .from('media')
                .update({
                    soc_tmdb_id: tmdbId,
                    media_type: mediaType,
                    updated_at: new Date().toISOString()
                })
                .eq('id', record.id);

            if (updateError) {
                console.error(`   ❌ Update error for ${record.name}: ${updateError.message}`);
                failedCount++;
            } else {
                linkedCount++;
                console.log(`   ✅ Linked: ${record.name} -> TMDb ID ${tmdbId} (${mediaType})`);
            }
        } else {
            console.log(`   ❌ No match found on TMDb for ${imdbId}`);
            failedCount++;
        }

        await sleep(SLEEP_MS);
    }

    const stats = getApiStats();
    console.log(`\n🎉 Linker Done! Linked: ${linkedCount}, Failed: ${failedCount}, API Success Rate: ${stats.successRate}%`);
    console.log(`   💡 Run tmdb-media-enrichment.ts now to fill in the full details for these ${linkedCount} records!`);
}

linkMedia().catch(async (error) => {
    console.error('🔥 FATAL ERROR:', error);
    await logSystemBug(error);
    process.exit(1);
});
