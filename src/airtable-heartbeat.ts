import fetch from 'node-fetch';

/**
 * Updates the Airtable Workflow record with the latest execution status.
 */
export async function updateWorkflowHeartbeat(status: 'Running' | 'Ready' | 'Errors', details: string) {
    const AIRTABLE_PAT = process.env.AIRTABLE_PAT;
    const AIRTABLE_BASE_ID = process.env.AIRTABLE_BASE_ID || 'appCGet4ar0zgtgyj';
    const RECORD_ID = process.env.AIRTABLE_RECORD_ID;

    if (!AIRTABLE_PAT || !RECORD_ID) {
        console.warn('⚠️ Airtable heartbeat skipped: AIRTABLE_PAT or AIRTABLE_RECORD_ID missing.');
        return;
    }

    const payload: any = {
        fields: {
            'Run Status': status,
            'Run Details': `${new Date().toISOString().split('T')[1].split('.')[0]} | ${details}`,
            'Last Run': new Date().toISOString().split('T')[0] // YYYY-MM-DD
        }
    };

    try {
        const resp = await fetch(`https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/tblsAqd2HHRHPsl9z/${RECORD_ID}`, {
            method: 'PATCH',
            headers: {
                'Authorization': `Bearer ${AIRTABLE_PAT}`,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(payload)
        });

        if (!resp.ok) {
            const err = await resp.text();
            console.error('❌ Airtable heartbeat update failed:', err);
        } else {
            console.log(`📡 Airtable Heartbeat: ${status}`);
        }
    } catch (err) {
        console.error('❌ Airtable heartbeat error:', err);
    }
}
