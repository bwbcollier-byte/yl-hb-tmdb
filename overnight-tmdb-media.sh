#!/bin/bash

# TMDb Media Profile Enrichment — Overnight Runner
# Runs the enrichment in a continuous loop with a sleep between rounds.
#
# Usage:
#   ./overnight-tmdb-media.sh                   # Default: 1000 per round
#   LIMIT=500 ./overnight-tmdb-media.sh         # Custom limit per round
#   nohup ./overnight-tmdb-media.sh &            # Run in background

# Configuration
LIMIT=${LIMIT:-1000}
SLEEP_BETWEEN_ROUNDS=${SLEEP_BETWEEN_ROUNDS:-60}

echo "🎬 TMDb Media Profile Enrichment — Overnight Mode"
echo "======================================================"
echo "   Records per round: $LIMIT"
echo "   Sleep between rounds: ${SLEEP_BETWEEN_ROUNDS}s"
echo "======================================================"

ROUND=0

while true; do
    ROUND=$((ROUND + 1))
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "🔄 Round $ROUND — $(date +'%Y-%m-%d %H:%M:%S')"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    LIMIT=$LIMIT npx ts-node src/tmdb-media-enrichment.ts

    echo ""
    echo "✨ Round $ROUND Complete! Sleeping ${SLEEP_BETWEEN_ROUNDS}s..."
    sleep $SLEEP_BETWEEN_ROUNDS
done
