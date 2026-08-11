# v179_clean_final
from pathlib import Path
import ast,hashlib,json,py_compile,sys
R=Path(__file__).resolve().parent
M=json.loads((R/"modules_manifest.json").read_text(encoding="utf-8")); P=M["parts"]
errs=[]
for n in P+["bot.py","FULL_bot_v179_clean_final.py"]:
    try: py_compile.compile(str(R/n),doraise=True)
    except Exception as e: errs.append(f"compile {n}: {e}")
for n in P:
    raw=(R/n).read_bytes(); rows=raw.decode("utf-8").splitlines()
    if hashlib.sha256(raw).hexdigest()!=M["files"].get(n): errs.append(f"hash {n}")
    if not rows or rows[0].strip()!="# v179_clean_final" or rows[-1].strip()!="# v179_clean_final": errs.append(f"marker {n}")
hist={}; cb=0; validators=0; forbidden=[]
for n in P:
    src=(R/n).read_text(encoding="utf-8"); tree=ast.parse(src)
    if "TelegramBotBackupsStart" in src: forbidden.append(n)
    cb += sum(1 for line in src.splitlines() if "bot.callback_query_handler(" in line and not line.lstrip().startswith("#"))
    validators += sum(1 for x in ast.walk(tree) if isinstance(x,(ast.FunctionDef,ast.AsyncFunctionDef)) and x.name=="_v153_validate_restore_gz")
    for x in tree.body:
        if isinstance(x,(ast.FunctionDef,ast.AsyncFunctionDef,ast.ClassDef)): hist.setdefault(x.name,[]).append((n,x.lineno))
dups={k:v for k,v in hist.items() if len(v)>1}
if cb!=1: errs.append(f"callback registrations={cb}")
if validators!=1: errs.append(f"restore validators={validators}")
if dups: errs.append(f"duplicate top-level names={len(dups)}")
if forbidden: errs.append(f"forbidden Start root={forbidden}")
print(f"v179 FAST_TEST: {'PASS' if not errs else 'FAIL'} | parts={len(P)} | callback={cb} | validators={validators} | duplicates={len(dups)}")
if errs:
    print("\n".join(errs)); raise SystemExit(1)
# v179_clean_final
