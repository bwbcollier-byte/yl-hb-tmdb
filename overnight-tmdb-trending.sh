#!/bin/bash

# TMDb Trending Talent Mining — Overnight Runner
# Fetches trending people daily and upserts into social_profiles.
#
# Usage:
#   ./overnight-tmdb-trending.sh                  # Default: 3 pages per round
#   MAX_PAGES=5 ./overnight-tmdb-trending.sh      # Fetch more pages
#   nohup ./overnight-tmdb-trending.sh &           # Run in background

# Configuration
MAX_PAGES=${MAX_PAGES:-3}
SLEEP_BETWEEN_ROUNDS=${SLEEP_BETWEEN_ROUNDS:-3600}  # 1 hour default (trending changes daily)

echo "🎬 TMDb Trending Talent Mining — Overnight Mode"
echo "======================================================"
echo "   Pages per round: $MAX_PAGES (~$((MAX_PAGES * 20)) people)"
echo "   Sleep between rounds: ${SLEEP_BETWEEN_ROUNDS}s"
echo "======================================================"

ROUND=0

while true; do
    ROUND=$((ROUND + 1))
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "🔄 Round $ROUND — $(date +'%Y-%m-%d %H:%M:%S')"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    MAX_PAGES=$MAX_PAGES npx ts-node src/tmdb-trending-mining.ts

    echo ""
    echo "✨ Round $ROUND Complete! Sleeping ${SLEEP_BETWEEN_ROUNDS}s..."
    sleep $SLEEP_BETWEEN_ROUNDS
done
