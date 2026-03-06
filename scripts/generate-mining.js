const fs = require('fs');
const path = require('path');

const workflows = [
    // TV Set
    { id: 'tv-airing-today', title: 'TMDb TV Airing Today Mining', type: 'tv', endpoint: '/tv/airing_today', cron: '0 5 * * *' },
    { id: 'tv-on-the-air', title: 'TMDb TV On the Air Mining', type: 'tv', endpoint: '/tv/on_the_air', cron: '30 5 * * *' },
    { id: 'tv-popular', title: 'TMDb TV Popular Mining', type: 'tv', endpoint: '/tv/popular', cron: '0 6 * * *' },
    { id: 'tv-top-rated', title: 'TMDb TV Top Rated Mining', type: 'tv', endpoint: '/tv/top_rated', cron: '30 6 * * *' },
    { id: 'tv-trending', title: 'TMDb TV Trending Mining', type: 'tv', endpoint: '/trending/tv/day', cron: '0 7 * * *' },

    // Movie Set
    { id: 'movie-now-playing', title: 'TMDb Film Now Playing Mining', type: 'movie', endpoint: '/movie/now_playing', cron: '30 7 * * *' },
    { id: 'movie-popular', title: 'TMDb Film Popular Mining', type: 'movie', endpoint: '/movie/popular', cron: '0 8 * * *' },
    { id: 'movie-top-rated', title: 'TMDb Film Top Rated Mining', type: 'movie', endpoint: '/movie/top_rated', cron: '30 8 * * *' },
    { id: 'movie-upcoming', title: 'TMDb Film Upcoming Mining', type: 'movie', endpoint: '/movie/upcoming', cron: '0 9 * * *' },
    { id: 'movie-trending', title: 'TMDb Film Trending Mining', type: 'movie', endpoint: '/trending/movie/day', cron: '30 9 * * *' },
];

const githubDir = path.join(__dirname, '..', '.github', 'workflows');
const rootDir = path.join(__dirname, '..');

if (!fs.existsSync(githubDir)) {
    fs.mkdirSync(githubDir, { recursive: true });
}

workflows.forEach(wf => {
    // 1. Write GitHub Action
    const ymlContent = `name: ${wf.title}

on:
  schedule:
    - cron: '${wf.cron}'
  workflow_dispatch:
    inputs:
      max_pages:
        description: "Max pages to fetch"
        required: false
        default: "5"
      limit:
        description: "Max records to process over all pages"
        required: false
        default: "0"

jobs:
  enrich:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout Repository
        uses: actions/checkout@v4

      - name: Setup Node.js
        uses: actions/setup-node@v4
        with:
          node-version: 18

      - name: Install Dependencies
        run: npm install

      - name: Run Mining Script
        env:
          SUPABASE_URL: \${{ secrets.SUPABASE_URL }}
          SUPABASE_SERVICE_KEY: \${{ secrets.SUPABASE_SERVICE_KEY }}
          TMDB_BEARER_TOKEN: \${{ secrets.TMDB_BEARER_TOKEN }}
          MAX_PAGES: \${{ github.event.inputs.max_pages || '5' }}
          LIMIT: \${{ github.event.inputs.limit || '0' }}
          MINING_ENDPOINT: '${wf.endpoint}'
          MEDIA_TYPE: '${wf.type}'
          WORKFLOW_FILE: 'tmdb-${wf.id}-mining.yml'
        run: npx ts-node src/tmdb-media-mining.ts
`;
    fs.writeFileSync(path.join(githubDir, `tmdb-${wf.id}-mining.yml`), ymlContent);

    // 2. Write Overnight shell script
    const shContent = `#!/bin/bash
# Infinite loop script for ${wf.title}
# Designed to run in screen/tmux locally on your server

SLEEP_BETWEEN_ROUNDS=3600
export MAX_PAGES="\${MAX_PAGES:-5}"
export LIMIT="\${LIMIT:-0}"
export MINING_ENDPOINT="${wf.endpoint}"
export MEDIA_TYPE="${wf.type}"
export WORKFLOW_FILE="tmdb-${wf.id}-mining.yml"

echo "=========================================="
echo " Starting ${wf.title} - INFINITE LOOP "
echo " Max Pages: $MAX_PAGES "
echo " Limit: $LIMIT "
echo " Sleep between rounds: $SLEEP_BETWEEN_ROUNDS seconds"
echo "=========================================="
echo ""

while true
do
    echo "▶ \$(date): Starting new mining round..."
    
    # Notice we run the ts-node script
    npx ts-node src/tmdb-media-mining.ts
    
    echo "⏸ \$(date): Round complete. Sleeping for $SLEEP_BETWEEN_ROUNDS seconds..."
    sleep $SLEEP_BETWEEN_ROUNDS
    echo ""
done
`;
    const shPath = path.join(rootDir, `overnight-tmdb-${wf.id}-mining.sh`);
    fs.writeFileSync(shPath, shContent);
    fs.chmodSync(shPath, '755');
});

console.log('✅ Generated 10 GitHub Actions and 10 Overnight scripts successfully.');
