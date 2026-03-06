#!/bin/bash

# TMDb Popular Talent Mining — Overnight Runner
# Fetches popular people and upserts into social_profiles.
#
# Usage:
#   ./overnight-tmdb-popular.sh                   # Default: 10 pages per round
#   MAX_PAGES=20 ./overnight-tmdb-popular.sh      # Fetch more pages
#   nohup ./overnight-tmdb-popular.sh &            # Run in background

# Configuration
MAX_PAGES=${MAX_PAGES:-10}
SLEEP_BETWEEN_ROUNDS=${SLEEP_BETWEEN_ROUNDS:-3600}  # 1 hour default

echo "🎬 TMDb Popular Talent Mining — Overnight Mode"
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

    MAX_PAGES=$MAX_PAGES npx ts-node src/tmdb-popular-mining.ts

    echo ""
    echo "✨ Round $ROUND Complete! Sleeping ${SLEEP_BETWEEN_ROUNDS}s..."
    sleep $SLEEP_BETWEEN_ROUNDS
done
