#!/bin/bash
# Sync nerve facts from ZeroZI (primary) to ZeroNovo (backup)
ZEROZI="100.105.160.106"
ZERONOVO="100.103.183.91"

# Get latest facts from ZeroZI
FACTS=$(curl -s "http://$ZEROZI:8200/api/facts?limit=100" 2>/dev/null)
if [ -z "$FACTS" ]; then
    echo "[$(date)] ZeroZI nerve unreachable" >> /home/zero/logs/nerve_sync.log
    exit 1
fi

# Push to ZeroNovo
echo "$FACTS" | python3 -c "
import sys, json, urllib.request

data = json.load(sys.stdin)
facts = data.get('facts', [])
if not facts:
    print('No facts to sync')
    sys.exit(0)

bulk = {'facts': [{'category': f['category'], 'key': f['key'], 'value': f['value'], 'source': f.get('source','sync')} for f in facts]}
req = urllib.request.Request('http://$ZERONOVO:8200/api/bulk-add',
    data=json.dumps(bulk).encode(),
    headers={'Content-Type': 'application/json'},
    method='POST')
try:
    resp = urllib.request.urlopen(req, timeout=10)
    result = json.loads(resp.read())
    print(f'Synced: {result}')
except Exception as e:
    print(f'Sync error: {e}')
" >> /home/zero/logs/nerve_sync.log 2>&1
