import { supabase } from './supabase';
import { fetchTmdbMovie, fetchTmdbTv, sleep, getApiStats, getImageUrl, SLEEP_MS } from './tmdb-api';
import * as dotenv from 'dotenv';
dotenv.config();

const LIMIT_ENV = process.env.LIMIT;
const RUN_ALL = !LIMIT_ENV || LIMIT_ENV.trim() === '';
const LIMIT = RUN_ALL ? 999999 : parseInt(LIMIT_ENV as string);
const WORKFLOW_NAME = 'TMDb Media Enrichment';

async function processMedia() {
    console.log(`\n🎬 Starting ${WORKFLOW_NAME}`);
    console.log(`   Limit: ${RUN_ALL ? 'All' : LIMIT} records\n`);

    // Fetch media profiles that have a tmdb_id, unprocessed first
    const { data: mediaRecords, error } = await supabase
        .from('media_profiles')
        .select('id, tmdb_id, title, workflow_logs')
        .not('tmdb_id', 'is', null)
        .order('tmdb_check', { ascending: true, nullsFirst: true })
        .order('last_processed', { ascending: true, nullsFirst: true })
        .limit(LIMIT);

    if (error) { console.error('❌ Fetch error:', error.message); return; }
    if (!mediaRecords?.length) { console.log('✅ No media profiles with TMDb IDs to process.'); return; }

    console.log(`   Found ${mediaRecords.length} media records with TMDb IDs.\n`);

    let processedCount = 0, successCount = 0, failedCount = 0;

    for (const record of mediaRecords) {
        processedCount++;
        const tmdbId = record.tmdb_id;
        // Default to 'movie' — set tmdb_media_type = 'tv' in the DB to override per record
        const mediaType: string = (record as any).tmdb_media_type || 'movie';
        console.log(`[${processedCount}/${mediaRecords.length}] TMDb ${mediaType} ID: ${tmdbId} — ${record.title || record.id}`);

        const data = mediaType === 'tv'
            ? await fetchTmdbTv(tmdbId)
            : await fetchTmdbMovie(tmdbId);

        if (!data) {
            failedCount++;
            await supabase
                .from('media_profiles')
                .update({ tmdb_check: 'not_found', last_checked: new Date().toISOString() })
                .eq('id', record.id);
            continue;
        }

        const isMovie = mediaType !== 'tv';

        // Title & release info differ between movies and TV
        const title = isMovie ? data.title : data.name;
        const releaseDate = isMovie ? data.release_date : data.first_air_date;
        const runtime = isMovie ? (data.runtime ?? null) : (data.episode_run_time?.[0] ?? null);

        // Top 10 cast members
        const cast = (data.credits?.cast || []).slice(0, 10).map((c: any) => ({
            id: c.id,
            name: c.name,
            character: c.character,
            order: c.order,
            profile_path: getImageUrl(c.profile_path)
        }));

        // Director(s) from crew
        const directors = (data.credits?.crew || [])
            .filter((c: any) => c.job === 'Director')
            .map((c: any) => c.name);

        // Genres as comma-separated string
        const genres = (data.genres || []).map((g: any) => g.name).join(', ');

        // Top 5 posters and backdrops sorted by vote_average
        const posters = (data.images?.posters || [])
            .sort((a: any, b: any) => b.vote_average - a.vote_average)
            .slice(0, 5)
            .map((img: any) => getImageUrl(img.file_path))
            .filter(Boolean);

        const backdrops = (data.images?.backdrops || [])
            .sort((a: any, b: any) => b.vote_average - a.vote_average)
            .slice(0, 5)
            .map((img: any) => getImageUrl(img.file_path))
            .filter(Boolean);

        const updates: Record<string, any> = {
            tmdb_check: 'success',
            tmdb_title: title || null,
            tmdb_overview: data.overview || null,
            tmdb_tagline: data.tagline || null,
            tmdb_release_date: releaseDate || null,
            tmdb_runtime: runtime,
            tmdb_genres: genres || null,
            tmdb_vote_average: data.vote_average ?? null,
            tmdb_vote_count: data.vote_count ?? null,
            tmdb_poster_path: getImageUrl(data.poster_path),
            tmdb_backdrop_path: getImageUrl(data.backdrop_path),
            tmdb_imdb_id: data.imdb_id || data.external_ids?.imdb_id || null,
            tmdb_status: data.status || null,
            tmdb_popularity: data.popularity ?? null,
            tmdb_cast: cast.length > 0 ? JSON.stringify(cast) : null,
            tmdb_director: directors.length > 0 ? directors.join(', ') : null,
            tmdb_images: JSON.stringify({ posters, backdrops }),
            last_processed: new Date().toISOString(),
            last_checked: new Date().toISOString(),
            updated_at: new Date().toISOString()
        };

        // Movie-only fields
        if (isMovie) {
            updates.tmdb_budget = data.budget ?? null;
            updates.tmdb_revenue = data.revenue ?? null;
        }

        // Merge into existing workflow_logs
        const logs = (record.workflow_logs as Record<string, any>) || {};
        logs['TMDb'] = {
            last_run: new Date().toISOString(),
            status: 'success',
            media_type: mediaType,
            cast_count: cast.length,
            director: directors.join(', ') || null
        };
        updates.workflow_logs = logs;

        const { error: updateError } = await supabase
            .from('media_profiles')
            .update(updates)
            .eq('id', record.id);

        if (updateError) {
            console.error(`   ❌ Update error: ${updateError.message}`);
            failedCount++;
        } else {
            successCount++;
            console.log(`   ✅ Enriched: ${title} (${cast.length} cast, dir: ${directors.join(', ') || 'n/a'})`);
        }

        await sleep(SLEEP_MS);
    }

    const stats = getApiStats();
    console.log(`\n🎉 Done! Processed: ${processedCount}, Success: ${successCount}, Failed: ${failedCount}, API Success Rate: ${stats.successRate}%`);
}

processMedia().catch(console.error);
