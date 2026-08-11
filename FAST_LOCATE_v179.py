# v179_clean_final
import json,sys
from pathlib import Path
M=json.loads((Path(__file__).parent/"CODEMAP_v179.json").read_text(encoding="utf-8"))
q=" ".join(sys.argv[1:]).strip()
if not q:
    print("usage: python FAST_LOCATE_v179.py <symbol-or-text>"); raise SystemExit(0)
for name,row in M.get("active_symbols",{}).items():
    if q.lower() in name.lower(): print(f"{name}: {row['file']}:{row['line']}")
# v179_clean_final
