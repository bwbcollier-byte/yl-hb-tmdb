#!/bin/bash
# Infinite loop script for TMDb TV On the Air Mining
# Designed to run in screen/tmux locally on your server

SLEEP_BETWEEN_ROUNDS=3600
export MAX_PAGES="${MAX_PAGES:-5}"
export LIMIT="${LIMIT:-0}"
export MINING_ENDPOINT="/tv/on_the_air"
export MEDIA_TYPE="tv"
export WORKFLOW_FILE="tmdb-tv-on-the-air-mining.yml"

echo "=========================================="
echo " Starting TMDb TV On the Air Mining - INFINITE LOOP "
echo " Max Pages: $MAX_PAGES "
echo " Limit: $LIMIT "
echo " Sleep between rounds: $SLEEP_BETWEEN_ROUNDS seconds"
echo "=========================================="
echo ""

while true
do
    echo "▶ $(date): Starting new mining round..."
    
    # Notice we run the ts-node script
    npx ts-node src/tmdb-media-mining.ts
    
    echo "⏸ $(date): Round complete. Sleeping for $SLEEP_BETWEEN_ROUNDS seconds..."
    sleep $SLEEP_BETWEEN_ROUNDS
    echo ""
done
