#!/usr/bin/env bash
# Kael Stop — Push current context to Memory API before shutdown
set -euo pipefail

API="http://localhost:8765"
CONTEXT_FILE="$HOME/KAEL_CONTEXT.md"

echo "Kael shutting down — syncing context to Memory API..."

if [ ! -f "$CONTEXT_FILE" ]; then
    echo "WARNING: No context file at ${CONTEXT_FILE} — nothing to sync"
    exit 0
fi

context=$(cat "$CONTEXT_FILE")

# POST context to API
response=$(curl -sf -X POST "${API}/context" \
    -H "Content-Type: application/json" \
    -d "$(python3 -c "
import json, sys
with open('${CONTEXT_FILE}', 'r') as f:
    ctx = f.read()
print(json.dumps({'context': ctx, 'machine': 'ZeroDESK'}))
")" 2>/dev/null) || {
    echo "ERROR: Memory API unreachable — context NOT synced!"
    echo "Context remains at ${CONTEXT_FILE} ($(wc -c < "$CONTEXT_FILE") bytes)"
    exit 1
}

echo "Kael synced"
echo "Response: ${response}"
