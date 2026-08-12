# v186_restore_exact_fast
import json,sys
from pathlib import Path
root=Path(__file__).resolve().parent
cm=json.loads((root/"CODEMAP_v186.json").read_text(encoding="utf-8"))
q=" ".join(sys.argv[1:]).casefold()
for rel in cm.get("parts",[]):
    text=(root/rel).read_text(encoding="utf-8",errors="ignore")
    if not q or q in text.casefold(): print(rel)
# v186_restore_exact_fast
