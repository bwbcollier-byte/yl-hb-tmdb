import { supabase } from './supabase';
import { fetchTmdbMediaMining, getImageUrl, sleep, getApiStats, SLEEP_MS } from './tmdb-api';
import * as dotenv from 'dotenv';
dotenv.config();

const MINING_ENDPOINT = process.env.MINING_ENDPOINT;
const MEDIA_TYPE = process.env.MEDIA_TYPE; // 'movie' or 'tv'
const WORKFLOW_FILE = process.env.WORKFLOW_FILE;

const MAX_PAGES = parseInt(process.env.MAX_PAGES || '5');
const LIMIT = parseInt(process.env.LIMIT || '0');

if (!MINING_ENDPOINT || !MEDIA_TYPE || !WORKFLOW_FILE) {
    console.error("❌ MINING_ENDPOINT, MEDIA_TYPE, and WORKFLOW_FILE environment variables are required.");
    process.exit(1);
}

const GITHUB_ENDPOINT_URL = `https://github.com/bwbcollier-byte/yl-hb-tmdb/actions/workflows/${WORKFLOW_FILE}`;

let processedCount = 0;
let newRecordsCount = 0;
let updatedRecordsCount = 0;
let errorsCount = 0;

async function runMediaMining() {
    console.log(`🏁 Starting TMDb Media Mining`);
    console.log(`   Endpoint: ${MINING_ENDPOINT}`);
    console.log(`   Media Type: ${MEDIA_TYPE}`);
    console.log(`   Max Pages: ${MAX_PAGES}`);
    if (LIMIT > 0) console.log(`   Limit: ${LIMIT} records`);

    // 1. First fetch page 1 to get total_pages
    const firstPageData = await fetchTmdbMediaMining(MINING_ENDPOINT as string, 1);
    if (!firstPageData || !firstPageData.results) {
        console.error("❌ Failed to fetch initial data from TMDb.");
        process.exit(1);
    }

    const totalPagesFromApi = firstPageData.total_pages || 1;
    let totalResultsFromApi = firstPageData.total_results || 0;
    
    // Total to process is bounded by LIMIT and MAX_PAGES
    const targetPages = Math.min(MAX_PAGES, totalPagesFromApi);
    if (LIMIT > 0 && LIMIT < totalResultsFromApi) {
        totalResultsFromApi = LIMIT;
    }

    // Update workflows table to show 'to_process' BEFORE we start looping
    await updateWorkflowProgress(totalResultsFromApi, 0);

    let page = 1;
    let keepGoing = true;

    while (keepGoing && page <= targetPages) {
        console.log(`\n📄 Fetching Page ${page} of ${targetPages}...`);
        
        let data = page === 1 ? firstPageData : await fetchTmdbMediaMining(MINING_ENDPOINT as string, page);
        
        if (!data || !data.results) {
            console.error(`   ❌ Failed to fetch page ${page}. Skipping...`);
            page++;
            await sleep(SLEEP_MS);
            continue;
        }

        const results = data.results;
        
        for (const item of results) {
            // Apply hard limit
            if (LIMIT > 0 && processedCount >= LIMIT) {
                console.log(`\n🛑 Hard limit of ${LIMIT} reached. Stopping.`);
                keepGoing = false;
                break;
            }

            processedCount++;
            
            try {
                await processMediaItem(item);
            } catch (error: any) {
                errorsCount++;
                console.error(`   ❌ Error processing ${item.id}: ${error.message}`);
            }

            // Sync progress every 50 records
            if (processedCount % 50 === 0) {
                await updateWorkflowProgress(totalResultsFromApi, processedCount);
            }
        }

        page++;
        await sleep(SLEEP_MS);
    }

    // Final workflow sync
    await updateWorkflowProgress(totalResultsFromApi, processedCount);

    console.log('\n✅ TMDb Media Mining Complete!');
    console.log(`   Total API Calls: ${getApiStats().totalApiCalls} (${getApiStats().successRate}% success)`);
    console.log(`   Total Processed: ${processedCount}`);
    console.log(`   New Records: ${newRecordsCount}`);
    console.log(`   Updated Records: ${updatedRecordsCount}`);
    console.log(`   Errors: ${errorsCount}`);
    
    process.exit(0);
}

async function processMediaItem(item: any) {
    const tmdbId = item.id;
    const title = item.title || item.name || '';
    
    console.log(`📸 [${processedCount}] Processing: ${title} (TMDb ID: ${tmdbId})`);

    const updatePayload = {
        tmdb_title: title,
        album_name: title, // acts as fallback title
        tmdb_media_type: MEDIA_TYPE,
        media_type: [MEDIA_TYPE],
        tmdb_overview: item.overview || '',
        tmdb_release_date: item.release_date || item.first_air_date || null,
        tmdb_vote_average: item.vote_average || 0,
        tmdb_vote_count: item.vote_count || 0,
        tmdb_poster_path: getImageUrl(item.poster_path) || '',
        tmdb_backdrop_path: getImageUrl(item.backdrop_path) || '',
        tmdb_popularity: item.popularity || 0,
        tmdb_check: new Date().toISOString()
    };

    // Check if exists
    const { data: existingRecords, error: checkError } = await supabase
        .from('media_profiles')
        .select('id')
        .eq('tmdb_id', tmdbId);

    if (checkError) throw checkError;

    if (existingRecords && existingRecords.length > 0) {
        // Update
        const { error: updateError } = await supabase
            .from('media_profiles')
            .update(updatePayload)
            .eq('id', existingRecords[0].id);

        if (updateError) throw updateError;
        updatedRecordsCount++;
    } else {
        // Insert
        const insertPayload = {
            ...updatePayload,
            tmdb_id: tmdbId,
            status: 'active'
        };

        const { error: insertError } = await supabase
            .from('media_profiles')
            .insert(insertPayload);

        if (insertError) throw insertError;
        newRecordsCount++;
        console.log(`   ✨ Created new media_profiles record.`);
    }
}

async function updateWorkflowProgress(toProcess: number, processed: number) {
    try {
        const { error } = await supabase
            .from('workflows')
            .update({
                to_process: toProcess,
                processed: processed,
                last_run_at: new Date().toISOString(),
                updated_at: new Date().toISOString()
            })
            .eq('endpoint_url', GITHUB_ENDPOINT_URL);

        if (error) {
            console.error(`   ⚠️ Failed to update workflow progress: ${error.message}`);
        }
    } catch (e: any) {
         console.error(`   ⚠️ Exception updating workflow progress: ${e.message}`);
    }
}

runMediaMining();
