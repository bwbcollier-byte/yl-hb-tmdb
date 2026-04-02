import { supabase } from './supabase';

async function test() {
    console.log('🧪 Testing Supabase Connection...');
    const { data, error } = await supabase.from('hb_socials').select('count', { count: 'exact', head: true });
    
    if (error) {
        console.error('❌ Connection Failed:', JSON.stringify(error, null, 2));
    } else {
        console.log('✅ Connection Successful! Total Socials:', data);
    }
}

test();
