# v180_total_final_diagnostics
from pathlib import Path
import json,sys
R=Path(__file__).resolve().parent
C=json.loads((R/'CODEMAP_v180.json').read_text(encoding='utf-8'))
q=' '.join(sys.argv[1:]).strip().casefold()
if not q:
    print('Usage: python FAST_LOCATE_v180.py <function|text>'); raise SystemExit(0)
hits=[]
for name,row in (C.get('active_symbols') or {}).items():
    if q in name.casefold(): hits.append((name,row['file'],row['line'],'ACTIVE'))
for term,rows in (C.get('features') or {}).items():
    if q in term.casefold() or term.casefold() in q:
        for r in rows[:30]: hits.append((term,r['file'],r['line'],r.get('text','')))
for h in hits[:80]: print(' | '.join(map(str,h)))
if not hits: print('No CODEMAP hit')
# v180_total_final_diagnostics
