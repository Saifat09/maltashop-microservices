#!/usr/bin/env python3
"""Follow one request across every service by its correlation ID.

    docker compose logs --no-log-prefix | python scripts/trace.py <correlation-id>

Reads JSON log lines from stdin, keeps those with the given correlation_id,
sorts them by time and prints one compact line per event.
"""

import json
import sys

if len(sys.argv) != 2:
    sys.exit(__doc__)
cid = sys.argv[1]
rows = []
for line in sys.stdin:
    line = line[line.find("{"):].strip()
    try:
        entry = json.loads(line)
    except json.JSONDecodeError:
        continue
    if entry.get("correlation_id") == cid:
        rows.append(entry)

for r in sorted(rows, key=lambda e: e["timestamp"]):
    print(f'{r["timestamp"][11:23]}  {r["service"]:<21} {r["correlation_id"]}  {r["event"]:<21} {r["message"]}')
print(f"-- {len(rows)} log lines, {len({r['service'] for r in rows})} services, correlation_id={cid}")
