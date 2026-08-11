#!/usr/bin/env python3
"""Fast locator for bot_v176_process_control_center.
Usage: python FAST_LOCATE_v176.py safe_edit
"""
from pathlib import Path
import json,sys,re
ROOT=Path(__file__).resolve().parent
MAP=ROOT/'ACTIVE_DEFINITIONS_v176.json'
q=' '.join(sys.argv[1:]).strip()
if not q:
    print('Usage: python FAST_LOCATE_v176.py <function-or-text>'); raise SystemExit(2)
obj=json.loads(MAP.read_text(encoding='utf-8'))
active=obj.get('active',{}); history=obj.get('history',{})
print(f'QUERY: {q}')
for name,row in sorted(active.items()):
    if q.casefold() in name.casefold():
        print(f'ACTIVE {name}: {row["file"]}:{row["line"]} ({row["kind"]})')
        h=history.get(name,[])
        if len(h)>1:
            print('  HISTORY: ' + ' -> '.join(f'{x["file"]}:{x["line"]}' for x in h))
print('\nTEXT OCCURRENCES:')
count=0
for p in sorted(ROOT.glob('*.py')):
    if p.name.startswith('FULL_'): continue
    try: rows=p.read_text(encoding='utf-8').splitlines()
    except Exception: continue
    for i,line in enumerate(rows,1):
        if q.casefold() in line.casefold():
            print(f'{p.name}:{i}: {line.strip()[:180]}'); count+=1
            if count>=80: raise SystemExit
