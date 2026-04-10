import { supabase } from './supabase';
import { fetchTmdbMovie, fetchTmdbTv, sleep, getApiStats, getImageUrl, SLEEP_MS } from './tmdb-api';
import * as dotenv from 'dotenv';
dotenv.config();

const LIMIT_ENV = process.env.LIMIT;
const RUN_ALL   = !LIMIT_ENV || LIMIT_ENV.trim() === '';
const LIMIT     = RUN_ALL ? 1000 : parseInt(LIMIT_ENV as string);
const WORKFLOW_ID = parseInt(process.env.WORKFLOW_ID || '0');

// ─── SUPABASE WORKFLOW SUMMARY ───────────────────────────────────────────────

async function updateWorkflowSummary(
    status: 'success' | 'failure',
    summary: Record<string, unknown>,
    durationSecs: number,
    lastError?: string
) {
    if (!WORKFLOW_ID) return;
    const { error } = await supabase.rpc('log_workflow_run', {
        p_workflow_id:   WORKFLOW_ID,
        p_status:        status,
        p_duration_secs: durationSecs,
        p_summary:       summary,
    });
    if (error) console.warn(`   ⚠️  Workflow summary update failed: ${error.message}`);

    // Write last_error separately if provided
    if (lastError) {
        await supabase
            .from('workflows')
            .update({ last_error: lastError, updated_at: new Date().toISOString() })
            .eq('github_workflow_id', WORKFLOW_ID);
    }
}

// ─── MAIN ────────────────────────────────────────────────────────────────────

async function processMedia() {
    const runStart = Date.now();
    console.log(`\n🎬 TMDb Media Profile Enrichment`);
    console.log(`   Limit: ${RUN_ALL ? 'All (cap 1000)' : LIMIT} records\n`);

    const { data: mediaRecords, error } = await supabase
        .from('hb_media')
        .select('*')
        .not('soc_tmdb_id', 'is', null)
        .order('updated_at', { ascending: true })
        .limit(LIMIT);

    if (error) throw error;
    if (!mediaRecords?.length) {
        console.log('✅ No media records with TMDb IDs to process.');
        await updateWorkflowSummary('success', { processed: 0, note: 'No records found' }, 0);
        return;
    }

    console.log(`   Found ${mediaRecords.length} media records to enrich.\n`);

    let processedCount = 0, successCount = 0, failedCount = 0;

    for (const record of mediaRecords) {
        processedCount++;
        const tmdbId   = record.soc_tmdb_id;
        const mediaType = record.media_type === 'tv' || record.soc_tmdb?.includes('/tv/') ? 'tv' : 'movie';

        console.log(`[${processedCount}/${mediaRecords.length}] ${mediaType.toUpperCase()} ${tmdbId} — ${record.name}`);

        const data = mediaType === 'tv'
            ? await fetchTmdbTv(tmdbId)
            : await fetchTmdbMovie(tmdbId);

        if (!data) {
            failedCount++;
            // Bump updated_at so it moves to the back of the queue
            await supabase.from('hb_media').update({ updated_at: new Date().toISOString() }).eq('id', record.id);
            continue;
        }

        const isMovie = mediaType !== 'tv';
        const extIds  = data.external_ids || {};

        // ── Cast: look up internal talent IDs via hb_socials ────────────────
        const rawCast     = (data.credits?.cast || []).slice(0, 15);
        const castTmdbIds = rawCast.map((c: any) => String(c.id));

        const { data: linkedSocials } = await supabase
            .from('hb_socials')
            .select('linked_talent, identifier')
            .eq('type', 'TMDB')
            .in('identifier', castTmdbIds);

        const talentLookup: Record<string, string> = {};
        linkedSocials?.forEach(s => { if (s.identifier) talentLookup[s.identifier] = s.linked_talent; });

        const cast = rawCast.map((c: any) => ({
            tmdb_id:      c.id,
            talent_id:    talentLookup[String(c.id)] || null,
            name:         c.name,
            character:    c.character,
            profile_path: getImageUrl(c.profile_path),
        }));

        const directors = (data.credits?.crew || [])
            .filter((c: any) => c.job === 'Director' || c.job === 'Series Director')
            .map((c: any) => c.name);

        const updates: any = {
            name:         (isMovie ? data.title : data.name) || record.name,
            about:        data.overview || record.about,
            date_release: isMovie ? data.release_date : data.first_air_date,
            running_time: isMovie
                ? (data.runtime ? `${data.runtime} min` : null)
                : (data.episode_run_time?.[0] ? `${data.episode_run_time[0]} min` : null),
            genres:       (data.genres || []).map((g: any) => g.name),
            // Always prefer fresh TMDB poster; fall back to existing image
            image:        data.poster_path ? getImageUrl(data.poster_path) : record.image,
            rating:       data.vote_average,
            soc_tmdb:     `https://www.themoviedb.org/${mediaType}/${tmdbId}`,
            soc_tmdb_id:  tmdbId,
            soc_imdb_id:  data.imdb_id || extIds.imdb_id || null,
            soc_imdb:     (data.imdb_id || extIds.imdb_id)
                            ? `https://www.imdb.com/title/${data.imdb_id || extIds.imdb_id}`
                            : null,
            soc_instagram: extIds.instagram_id ? `https://instagram.com/${extIds.instagram_id}` : null,
            soc_website:   data.homepage || null,
            soc_wikidata_id: extIds.wikidata_id || null,
            tmdb_credits: {
                cast,
                directors,
                production_companies: (data.production_companies || []).map((pc: any) => ({
                    id:        pc.id,
                    name:      pc.name,
                    logo_path: getImageUrl(pc.logo_path),
                })),
            },
            stats_boxoffice_financial: {
                popularity:        data.popularity,
                vote_count:        data.vote_count,
                status:            data.status,
                tagline:           data.tagline,
                original_language: data.original_language,
                budget:            data.budget  || null,
                revenue:           data.revenue || null,
                facebook_id:       extIds.facebook_id || null,
                twitter_id:        extIds.twitter_id  || null,
            },
            updated_at: new Date().toISOString(),
        };

        const { error: updateError } = await supabase
            .from('hb_media')
            .update(updates)
            .eq('id', record.id);

        if (updateError) {
            console.error(`   ❌ Update error: ${updateError.message}`);
            failedCount++;
        } else {
            successCount++;
            console.log(`   ✅ ${updates.name}`);
        }

        await sleep(SLEEP_MS);
    }

    const stats       = getApiStats();
    const durationSecs = Math.round((Date.now() - runStart) / 1000);
    const summaryObj  = {
        processed:        processedCount,
        enriched:         successCount,
        failed:           failedCount,
        api_success_rate: `${stats.successRate}%`,
        run_at:           new Date().toISOString(),
    };

    console.log(`\n🎉 Done! ${processedCount} processed — ${successCount} enriched, ${failedCount} failed (${durationSecs}s, API: ${stats.successRate}%)`);
    await updateWorkflowSummary('success', summaryObj, durationSecs);
}

processMedia().catch(async (err) => {
    console.error('🔥 Fatal:', err.message);
    await updateWorkflowSummary(
        'failure',
        { error: err.message, run_at: new Date().toISOString() },
        0,
        err.message
    );
    process.exit(1);
});
