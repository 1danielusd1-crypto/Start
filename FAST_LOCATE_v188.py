# v188_restore_forward_fix_final
import json,sys
from pathlib import Path
root=Path(__file__).resolve().parent
q=(sys.argv[1] if len(sys.argv)>1 else "restore").lower()
for p in root.glob("*.py"):
    if p.name.startswith("FULL_"): continue
    for i,line in enumerate(p.read_text(encoding="utf-8",errors="ignore").splitlines(),1):
        if q in line.lower(): print(f"{p.name}:{i}:{line.strip()}")
# v188_restore_forward_fix_final
