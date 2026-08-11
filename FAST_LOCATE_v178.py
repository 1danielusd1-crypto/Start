# v178_global_performance_final
"""Fast source locator for bot_v178_global_performance_final."""
import json, sys
from pathlib import Path
q=" ".join(sys.argv[1:]).casefold()
d=json.loads((Path(__file__).parent/"CODEMAP_v178.json").read_text())
a=json.loads((Path(__file__).parent/"ACTIVE_DEFINITIONS_v178.json").read_text())
for name,row in a.get("active_symbols",{}).items():
    hay=f"{name} {row.get('file','')}".casefold()
    if not q or q in hay:
        print(f"{name} -> {row.get('file')}:{row.get('line')}")
# v178_global_performance_final
