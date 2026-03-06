import { supabase } from './supabase';
import { fetchTmdbPopularPeople, sleep, getApiStats, getImageUrl, SLEEP_MS } from './tmdb-api';
import * as dotenv from 'dotenv';
dotenv.config();

// Number of popular pages to fetch (20 people per page)
const MAX_PAGES = parseInt(process.env.MAX_PAGES || '10');
const WORKFLOW_NAME = 'TMDb Popular Talent Mining';

async function minePopular() {
    console.log(`\n🎬 Starting ${WORKFLOW_NAME}`);
    console.log(`   Fetching up to ${MAX_PAGES} pages of popular people (${MAX_PAGES * 20} max)\n`);

    const allPeople: any[] = [];

    // Fetch popular people across multiple pages
    for (let page = 1; page <= MAX_PAGES; page++) {
        const result = await fetchTmdbPopularPeople(page);
        if (!result?.results?.length) break;

        allPeople.push(...result.results);
        console.log(`   Page ${page}/${result.total_pages}: fetched ${result.results.length} people`);

        if (page >= result.total_pages) break;
        await sleep(SLEEP_MS);
    }

    if (!allPeople.length) {
        console.log('✅ No popular people returned from TMDb.');
        return;
    }

    console.log(`\n   Total popular people to process: ${allPeople.length}\n`);

    let createdCount = 0, updatedCount = 0, skippedCount = 0;

    for (let i = 0; i < allPeople.length; i++) {
        const person = allPeople[i];
        const tmdbId = String(person.id);
        console.log(`[${i + 1}/${allPeople.length}] ${person.name} (TMDb ID: ${tmdbId}, Popularity: ${person.popularity})`);

        // Check if a TMDB social_profile already exists for this person
        const { data: existing } = await supabase
            .from('social_profiles')
            .select('id, tmdb_popularity')
            .eq('social_type', 'TMDB')
            .eq('social_id', tmdbId)
            .maybeSingle();

        const profileImageUrl = getImageUrl(person.profile_path);
        const tmdbUrl = `https://www.themoviedb.org/person/${tmdbId}`;

        if (existing) {
            // Update popularity and image if already exists
            const { error } = await supabase
                .from('social_profiles')
                .update({
                    tmdb_popularity: person.popularity,
                    social_image: profileImageUrl,
                    last_checked: new Date().toISOString(),
                    updated_at: new Date().toISOString()
                })
                .eq('id', existing.id);

            if (error) {
                console.error(`   ❌ Update error: ${error.message}`);
            } else {
                updatedCount++;
                console.log(`   🔄 Updated existing profile`);
            }
        } else {
            // Create new TMDB social_profile — tmdb_check left null so social enrichment picks it up
            const { error } = await supabase
                .from('social_profiles')
                .insert({
                    social_type: 'TMDB',
                    social_id: tmdbId,
                    social_url: tmdbUrl,
                    name: person.name,
                    social_image: profileImageUrl,
                    tmdb_popularity: person.popularity,
                    tmdb_known_for: person.known_for_department || null,
                    tmdb_gender: person.gender ?? null,
                    status: 'active',
                    last_checked: new Date().toISOString(),
                    updated_at: new Date().toISOString(),
                    workflow_logs: { created_by: WORKFLOW_NAME, created_at: new Date().toISOString() }
                });

            if (error) {
                console.error(`   ❌ Insert error: ${error.message}`);
                skippedCount++;
            } else {
                createdCount++;
                console.log(`   ✨ Created new profile`);
            }
        }
    }

    const stats = getApiStats();
    console.log(`\n🎉 Done! Created: ${createdCount}, Updated: ${updatedCount}, Skipped: ${skippedCount}, API Calls: ${stats.totalApiCalls}`);
}

minePopular().catch(console.error);
