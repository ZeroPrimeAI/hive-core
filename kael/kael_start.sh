#!/usr/bin/env bash
# Kael Start — Pull latest context from Memory API and write to ~/KAEL_CONTEXT.md
set -euo pipefail

API="http://localhost:8765"
CONTEXT_FILE="$HOME/KAEL_CONTEXT.md"

echo "Kael starting — pulling context from Memory API..."

response=$(curl -sf "${API}/context" 2>/dev/null) || {
    echo "WARNING: Memory API unreachable at ${API}"
    cat > "$CONTEXT_FILE" <<'FALLBACK'
# Kael Context — FALLBACK
Working blind — Memory API unreachable.
No prior context available. Rebuild state from CLAUDE.md and MEMORY.md.
FALLBACK
    echo "Wrote fallback context to ${CONTEXT_FILE}"
    echo "Kael online (degraded — no memory)"
    exit 0
}

# Extract the context field from JSON
context=$(echo "$response" | python3 -c "
import sys, json
d = json.load(sys.stdin)
ctx = d.get('context')
if ctx:
    print(ctx)
else:
    print('# Kael Context\nNo prior context saved yet. Fresh start.')
" 2>/dev/null) || {
    echo "WARNING: Failed to parse API response"
    echo "# Kael Context — Parse Error" > "$CONTEXT_FILE"
    echo "Kael online (degraded)"
    exit 0
}

echo "$context" > "$CONTEXT_FILE"
echo "Context loaded ($(wc -c < "$CONTEXT_FILE") bytes) -> ${CONTEXT_FILE}"
echo "Kael online"
