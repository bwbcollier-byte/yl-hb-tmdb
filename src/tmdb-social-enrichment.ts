import { supabase } from './supabase';
import { fetchTmdbPerson, sleep, getImageUrl, SLEEP_MS } from './tmdb-api';
import * as dotenv from 'dotenv';
dotenv.config();

const LIMIT       = parseInt(process.env.LIMIT       || '1000');
const STALE_DAYS  = parseInt(process.env.STALE_DAYS  || '7');
const WORKFLOW_ID = parseInt(process.env.WORKFLOW_ID || '0');

// ─── SUPABASE WORKFLOW SUMMARY ───────────────────────────────────────────────

async function updateWorkflowSummary(
    status: 'success' | 'failure',
    summary: Record<string, unknown>,
    durationSecs: number
) {
    if (!WORKFLOW_ID) return;
    const { error } = await supabase.rpc('log_workflow_run', {
        p_workflow_id:   WORKFLOW_ID,
        p_status:        status,
        p_duration_secs: durationSecs,
        p_summary:       summary,
    });
    if (error) console.warn(`   ⚠️  Workflow summary update failed: ${error.message}`);
    else console.log(`   📊 Workflow summary logged to Supabase`);
}

// ─── MAIN ────────────────────────────────────────────────────────────────────

async function processProfiles() {
    const runStart = Date.now();
    console.log(`🎬 TMDb Social Profile Enrichment — up to ${LIMIT} records (stale > ${STALE_DAYS}d)\n`);

    const staleThreshold = new Date(Date.now() - STALE_DAYS * 24 * 60 * 60 * 1000).toISOString();

    const { data: profiles, error } = await supabase
        .from('hb_socials')
        .select('id, identifier, name, linked_talent, check_tmdb_enrichment')
        .eq('type', 'TMDB')
        .or(`check_tmdb_enrichment.is.null,check_tmdb_enrichment.lt.${staleThreshold}`)
        .order('check_tmdb_enrichment', { ascending: true, nullsFirst: true })
        .limit(LIMIT);

    if (error) throw error;

    if (!profiles?.length) {
        console.log('✅ No stale TMDB social profiles to process.');
        const durationSecs = Math.round((Date.now() - runStart) / 1000);
        await updateWorkflowSummary('success', { profiles_processed: 0, run_at: new Date().toISOString() }, durationSecs);
        return;
    }

    console.log(`   Found ${profiles.length} stale TMDB profiles to enrich.\n`);

    let processedCount = 0, successCount = 0, failedCount = 0;

    for (const profile of profiles) {
        processedCount++;
        const personId = profile.identifier;
        console.log(`[${processedCount}/${profiles.length}] ${profile.name || personId} (TMDB: ${personId})`);

        if (!personId || !/^\d+$/.test(String(personId))) {
            console.log(`   ⏭️  Skipping invalid identifier: "${personId}"`);
            failedCount++;
            continue;
        }

        const data = await fetchTmdbPerson(personId);
        const now = new Date().toISOString();

        if (!data) {
            failedCount++;
            // Stamp check_tmdb_enrichment so we don't hammer 404s every run
            await supabase
                .from('hb_socials')
                .update({ check_tmdb_enrichment: now, updated_at: now })
                .eq('id', profile.id);
            await sleep(SLEEP_MS);
            continue;
        }

        const extIds = data.external_ids || {};

        // ── Update the TMDB hb_socials row ────────────────────────────────
        const tmdbUpdate: Record<string, any> = {
            name: data.name || profile.name,
            detailed_array: {
                biography:            data.biography            || null,
                birthday:             data.birthday             || null,
                deathday:             data.deathday             || null,
                place_of_birth:       data.place_of_birth       || null,
                known_for_department: data.known_for_department || null,
                imdb_id:              extIds.imdb_id            || null,
                wikidata_id:          extIds.wikidata_id        || null,
                tmdb_popularity:      data.popularity           ?? null,
            },
            check_tmdb_enrichment: now,
            updated_at:            now,
        };
        // Only update image if TMDB actually has a profile path
        if (data.profile_path) tmdbUpdate.image = getImageUrl(data.profile_path);

        const { error: updateError } = await supabase
            .from('hb_socials')
            .update(tmdbUpdate)
            .eq('id', profile.id);

        if (updateError) {
            console.error(`   ❌ Update error: ${updateError.message}`);
            failedCount++;
            await sleep(SLEEP_MS);
            continue;
        }

        successCount++;
        console.log(`   ✅ Enriched: ${data.name}`);

        // ── Batch-upsert linked social rows (one per platform) ────────────
        if (profile.linked_talent) {
            const linkedSocials = [
                extIds.imdb_id      && { type: 'IMDB',      identifier: extIds.imdb_id,      name: data.name, social_url: `https://www.imdb.com/name/${extIds.imdb_id}/`,        linked_talent: profile.linked_talent, updated_at: now },
                extIds.instagram_id && { type: 'INSTAGRAM', identifier: extIds.instagram_id, name: data.name, social_url: `https://www.instagram.com/${extIds.instagram_id}/`,    linked_talent: profile.linked_talent, updated_at: now },
                extIds.tiktok_id    && { type: 'TIKTOK',    identifier: extIds.tiktok_id,    name: data.name, social_url: `https://www.tiktok.com/@${extIds.tiktok_id}`,          linked_talent: profile.linked_talent, updated_at: now },
                extIds.twitter_id   && { type: 'TWITTER',   identifier: extIds.twitter_id,   name: data.name, social_url: `https://twitter.com/${extIds.twitter_id}`,             linked_talent: profile.linked_talent, updated_at: now },
                extIds.facebook_id  && { type: 'FACEBOOK',  identifier: extIds.facebook_id,  name: data.name, social_url: `https://www.facebook.com/${extIds.facebook_id}`,       linked_talent: profile.linked_talent, updated_at: now },
                extIds.wikidata_id  && { type: 'WIKIDATA',  identifier: extIds.wikidata_id,  name: data.name, social_url: `https://www.wikidata.org/wiki/${extIds.wikidata_id}`,  linked_talent: profile.linked_talent, updated_at: now },
            ].filter(Boolean) as any[];

            if (linkedSocials.length > 0) {
                const { error: socialsErr } = await supabase
                    .from('hb_socials')
                    .upsert(linkedSocials, { onConflict: 'type,identifier' });
                if (socialsErr) console.warn(`   ⚠️  Linked socials upsert: ${socialsErr.message}`);
                else console.log(`   🔗 ${linkedSocials.length} linked socials upserted`);
            }
        }

        await sleep(SLEEP_MS);
    }

    const durationSecs = Math.round((Date.now() - runStart) / 1000);
    const summaryObj = {
        profiles_processed: processedCount,
        enriched:           successCount,
        failed:             failedCount,
        run_at:             new Date().toISOString(),
    };
    console.log(`\n🎉 Done! ${successCount} enriched, ${failedCount} failed (${durationSecs}s)`);
    await updateWorkflowSummary('success', summaryObj, durationSecs);
}

processProfiles().catch(async (err) => {
    console.error('🔥 Fatal:', err.message);
    await updateWorkflowSummary('failure', { error: err.message, run_at: new Date().toISOString() }, 0);
    process.exit(1);
});
