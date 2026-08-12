# v189_main_window_authority_final
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parent
q=" ".join(sys.argv[1:]).strip()
if not q:
    print("usage: python FAST_LOCATE_v189.py <text>")
else:
    for p in sorted(ROOT.glob("*.py")):
        if p.name.startswith("FULL_"):
            continue
        for i,line in enumerate(p.read_text(encoding="utf-8",errors="replace").splitlines(),1):
            if q.casefold() in line.casefold():
                print(f"{p.name}:{i}:{line.strip()}")
# v189_main_window_authority_final
