import * as dotenv from 'dotenv';
import fetch from 'node-fetch';
dotenv.config();

const AIRTABLE_PAT = process.env.AIRTABLE_PAT;
const AIRTABLE_BASE_ID = process.env.AIRTABLE_BASE_ID;

async function testAirtableBugLog() {
    console.log('🐞 Attempting to send test bug record to Airtable...');
    
    if (!AIRTABLE_PAT || !AIRTABLE_BASE_ID) {
        console.error('❌ Missing Airtable configuration in .env');
        return;
    }

    try {
        const response = await fetch(`https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/tblTphXDvIezGmWae`, {
            method: 'POST',
            headers: {
                'Authorization': `Bearer ${AIRTABLE_PAT}`,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                records: [{
                    fields: {
                        'Name': '🧪 Live Integration Test — Script v2',
                        'Details': 'Testing the fixed automated error logging. Workflow field removed because it requires a Record ID.',
                        'Status': 'Todo',
                        'Severity': 'Low',
                        'Target ID': 'TEST-0002',
                        'Context': 'Local',
                        'Raw JSON': JSON.stringify({ test: true, message: "Fixed field types!" })
                    }
                }]
            })
        });

        if (response.ok) {
            console.log('✅ Success! Check your Airtable table now.');
        } else {
            const errBody = await response.text();
            console.error(`❌ Failed: ${response.status} ${response.statusText}`, errBody);
        }
    } catch (err: any) {
        console.error('🔥 Error during test:', err.message);
    }
}

testAirtableBugLog();
