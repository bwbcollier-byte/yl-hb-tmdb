import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';

dotenv.config();

const SUPABASE_URL = process.env.SUPABASE_URL!;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY!;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

async function heal() {
    console.log('🩹 Starting Database Healing: Reconnecting Orphans and Cleaning Duplicates...');

    // 1. RECONNECT ORPHANED SOCIALS
    // We'll find all socials where linked_talent is null and try to match them by Name
    const { data: orphans } = await supabase
        .from('hb_socials')
        .select('*')
        .is('linked_talent', null)
        .in('type', ['TMDB', 'INSTAGRAM', 'TIKTOK', 'FACEBOOK', 'TWITTER', 'IMDB', 'WIKIPEDIA']);

    if (orphans && orphans.length > 0) {
        console.log(`🔍 Found ${orphans.length} orphaned social records. Attempting to reunite...`);
        for (const social of orphans) {
            // Find talent by name
            const { data: talent } = await supabase
                .from('hb_talent')
                .select('id')
                .eq('name', social.name)
                .eq('category', 'Film & Television')
                .maybeSingle();
            
            if (talent) {
                await supabase.from('hb_socials').update({ linked_talent: talent.id }).eq('id', social.id);
                console.log(`   🔗 Linked Social [${social.type}] for ${social.name} to Talent ${talent.id}`);
            }
        }
    } else {
        console.log('✅ No orphaned socials found.');
    }

    // 2. DEFEAT THE ZOMBIES (Clean up Duplicate hb_talent entries)
    console.log('\n🧠 Scanning for Duplicate Talent Profiles...');
    const { data: allTalent } = await supabase
        .from('hb_talent')
        .select('id, name, created_at')
        .eq('category', 'Film & Television')
        .order('created_at', { ascending: false });

    if (allTalent) {
        const seenNames = new Set<string>();
        const toDelete: string[] = [];

        for (const t of allTalent) {
            if (seenNames.has(t.name)) {
                toDelete.push(t.id);
            } else {
                seenNames.add(t.name);
            }
        }

        if (toDelete.length > 0) {
            console.log(`🧟‍♂️ Found ${toDelete.length} duplicate talent records. Safely removing zombies...`);
            // Delete in chunks of 50
            for (let i = 0; i < toDelete.length; i += 50) {
                const chunk = toDelete.slice(i, i + 50);
                const { error } = await supabase.from('hb_talent').delete().in('id', chunk);
                if (error) console.error(`   ❌ Cleanup Error: ${error.message}`);
                else console.log(`   ✅ Safely removed ${chunk.length} duplicate records.`);
            }
        } else {
            console.log('✅ Your hb_talent table is clean! No duplicates found.');
        }
    }

    console.log('\n🎉 Healing Complete! System is fully synchronized.');
}

heal().catch(console.error);
