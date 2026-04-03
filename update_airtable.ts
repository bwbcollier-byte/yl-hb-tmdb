import fetch from 'node-fetch';

const AIRTABLE_PAT = process.env.AIRTABLE_PAT;
const BASE_ID = process.env.AIRTABLE_BASE_ID || 'appCGet4ar0zgtgyj';
const TABLE_ID = 'tblsAqd2HHRHPsl9z';
const URL = `https://api.airtable.com/v0/${BASE_ID}/${TABLE_ID}`;

if (!AIRTABLE_PAT) {
  console.error('❌ AIRTABLE_PAT is missing from environment!');
  process.exit(1);
}

const records = [
  {
    id: "recLx5lEamBWiVS2V",
    fields: {
      Status: "Done",
      Details: "Film Trending Mining ✅ (Node 20 / Airtable-Ready) 🎬",
      "Terminal Command": "cd yl-hb-tmdb && npx ts-node src/tmdb-media-mining.ts",
      Frequency: "Daily @ 9:30 AM UTC"
    }
  },
  {
    id: "rechtdpr1yeAI295k",
    fields: {
      Status: "Done",
      Details: "Film Popular Mining ✅ (Node 20 / Airtable-Ready) 🎬",
      "Terminal Command": "cd yl-hb-tmdb && npx ts-node src/tmdb-media-mining.ts",
      Frequency: "Daily @ 8:00 AM UTC"
    }
  },
  {
    id: "rectWrTUoT0fpMDis",
    fields: {
      Status: "Done",
      Details: "Film Top Rated Mining ✅ (Node 20 / Airtable-Ready) 🎬",
      "Terminal Command": "cd yl-hb-tmdb && npx ts-node src/tmdb-media-mining.ts",
      Frequency: "Daily @ 8:30 AM UTC"
    }
  },
  {
    id: "recTZXyqzIIfE5hMF",
    fields: {
      Status: "Done",
      Details: "Film Now Playing Mining ✅ (Node 20 / Airtable-Ready) 🎬",
      "Terminal Command": "cd yl-hb-tmdb && npx ts-node src/tmdb-media-mining.ts",
      Frequency: "Daily @ 7:30 AM UTC"
    }
  },
  {
    id: "recUy5OrtPeKNZpRk",
    fields: {
      Status: "Done",
      Details: "Film Upcoming Mining ✅ (Node 20 / Airtable-Ready) 🎬",
      "Terminal Command": "cd yl-hb-tmdb && npx ts-node src/tmdb-media-mining.ts",
      Frequency: "Daily @ 9:00 AM UTC"
    }
  },
  {
    id: "reci564vx1vj0DKii",
    fields: {
      Status: "Done",
      Details: "Popular Talent Mining ✅ (Node 20 / Airtable-Ready) 🟢",
      "Terminal Command": "cd yl-hb-tmdb && npx ts-node src/tmdb-popular-mining.ts",
      Frequency: "Daily @ 2:00 AM UTC"
    }
  },
  {
    id: "rec0EWMPdMBuXZ1NN",
    fields: {
      Status: "Done",
      Details: "Trending Talent Mining ✅ (Node 20 / Airtable-Ready) 🟢",
      "Terminal Command": "cd yl-hb-tmdb && npx ts-node src/tmdb-trending-mining.ts",
      Frequency: "Daily @ 1:00 AM UTC"
    }
  },
  {
    id: "recQZMwdc78cUeTFc",
    fields: {
      Status: "Done",
      Details: "TV Trending Mining ✅ (Node 20 / Airtable-Ready) 📺",
      "Terminal Command": "cd yl-hb-tmdb && npx ts-node src/tmdb-media-mining.ts",
      Frequency: "Daily @ 7:00 AM UTC"
    }
  },
  {
    id: "recgn72Vl4VjitJOo",
    fields: {
      Status: "Done",
      Details: "TV On the Air Mining ✅ (Node 20 / Airtable-Ready) 📺",
      "Terminal Command": "cd yl-hb-tmdb && npx ts-node src/tmdb-media-mining.ts",
      Frequency: "Daily @ 5:30 AM UTC"
    }
  },
  {
    id: "recyQTGYYyC7jHTS0",
    fields: {
      Status: "Done",
      Details: "TV Popular Mining ✅ (Node 20 / Airtable-Ready) 📺",
      "Terminal Command": "cd yl-hb-tmdb && npx ts-node src/tmdb-media-mining.ts",
      Frequency: "Daily @ 6:00 AM UTC"
    }
  },
  {
    id: "recJEQGxUIJBgW7Er",
    fields: {
      Status: "Done",
      Details: "TV Top Rated Mining ✅ (Node 20 / Airtable-Ready) 📺",
      "Terminal Command": "cd yl-hb-tmdb && npx ts-node src/tmdb-media-mining.ts",
      Frequency: "Daily @ 6:30 AM UTC"
    }
  },
  {
    id: "recOndXaAhO4vzjRf",
    fields: {
      Status: "Done",
      Details: "TV Airing Today Mining ✅ (Node 20 / Airtable-Ready) 📺",
      "Terminal Command": "cd yl-hb-tmdb && npx ts-node src/tmdb-media-mining.ts",
      Frequency: "Daily @ 5:00 AM UTC"
    }
  },
  {
    id: "rec0I7wtYan0gvRnA",
    fields: {
      Status: "Done",
      Details: "Media Profile Enrichment ✅ (Node 20 / Airtable-Ready) 🟠",
      "Terminal Command": "cd yl-hb-tmdb && npx ts-node src/tmdb-media-enrichment.ts",
      Frequency: "Daily @ 4:00 AM UTC"
    }
  },
  {
    id: "recfzGabmPzhdNURs",
    fields: {
      Status: "Done",
      Details: "Social Enrichment ✅ (Node 20 / Airtable-Ready) 🛡️",
      "Terminal Command": "cd yl-hb-tmdb && npx ts-node src/tmdb-social-enrichment.ts",
      Frequency: "Daily @ 3:00 AM UTC"
    }
  }
];

async function updateAirtable() {
  console.log('🚀 Starting Final Verified Dashboard Sync via Node.js...');

  // Airtable allows 10 records per PATCH
  for (let i = 0; i < records.length; i += 10) {
    const batch = records.slice(i, i + 10);
    console.log(`📊 Processing Batch ${Math.floor(i/10) + 1}...`);
    
    try {
      const resp = await fetch(URL, {
        method: 'PATCH',
        headers: {
          'Authorization': `Bearer ${AIRTABLE_PAT}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({ records: batch })
      });

      if (resp.ok) {
        console.log(`✅ Batch ${Math.floor(i/10) + 1} Succeeded.`);
      } else {
        const err = await resp.text();
        console.error(`❌ Batch ${Math.floor(i/10) + 1} Failed:`, err);
      }
    } catch (e) {
      console.error(`🔥 Fatal Network Error (Batch ${Math.floor(i/10) + 1}):`, e);
    }
  }
  
  console.log('🎉 Done!');
}

updateAirtable();
