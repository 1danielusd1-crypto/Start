# v180_total_final_diagnostics
from pathlib import Path
import ast, hashlib, json, py_compile, sys
R=Path(__file__).resolve().parent
M=json.loads((R/'modules_manifest.json').read_text(encoding='utf-8'))
P=M['parts']; errs=[]
if M.get('version')!='bot_v180_total_final_diagnostics': errs.append('manifest version')
for rel in P:
    p=R/rel
    if not p.exists(): errs.append('missing '+rel); continue
    raw=p.read_bytes(); rows=raw.decode('utf-8').splitlines()
    if hashlib.sha256(raw).hexdigest()!=M['files'].get(rel): errs.append('hash '+rel)
    if not rows or rows[0].strip()!='# v180_total_final_diagnostics' or rows[-1].strip()!='# v180_total_final_diagnostics': errs.append('marker '+rel)
    try: py_compile.compile(str(p),doraise=True)
    except Exception as e: errs.append(f'compile {rel}: {e}')
# global definitions
seen={}; dups=[]; cb=[]; restore=[]
for rel in P:
    src=(R/rel).read_text(encoding='utf-8'); tree=ast.parse(src)
    for n in tree.body:
        if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef,ast.ClassDef)):
            if n.name in seen: dups.append((n.name,seen[n.name],(rel,n.lineno)))
            seen[n.name]=(rel,n.lineno)
            if n.name=='_v153_validate_restore_gz': restore.append((rel,n.lineno))
    for i,line in enumerate(src.splitlines(),1):
        if 'bot.callback_query_handler(' in line and not line.lstrip().startswith('#'): cb.append((rel,i))
if dups: errs.append(f'duplicate callables={len(dups)}')
if len(cb)!=1 or cb[0][0]!='89_callback_final.py': errs.append(f'callback handlers={cb}')
if len(restore)!=1 or restore[0][0]!='85_runtime_control.py': errs.append(f'restore validators={restore}')
allsrc='\n'.join((R/r).read_text(encoding='utf-8') for r in P)
checks={
'no_start_root':'/TelegramBotBackupsStart' not in allsrc,
'total_diag':'TOTAL_DIAGNOSTICS_ENABLED' in allsrc,
'profiles_preserve':'code in _V180_DIAGNOSTIC_CODES and profile in {"fast", "minimal"}' in allsrc,
'task_wrappers_retired':'_V172_MESSAGE_WRAPPERS = 0' in allsrc and '_V174_MESSAGE_WRAP = 0' in allsrc,
'pool_perf':'_V180_POOL_PERF' in allsrc,
'sqlite_perf':'_V180_SQLITE_PERF' in allsrc,
'mega_perf':'_V180_MEGA_PERF' in allsrc,
'tg_perf':'telegram_api_result' in allsrc,
}
for k,v in checks.items():
    if not v: errs.append(k)
# FULL exact content
full=R/'FULL_bot_v180_total_final_diagnostics.py'
try: py_compile.compile(str(full),doraise=True)
except Exception as e: errs.append(f'full compile: {e}')
print('v180 FAST_TEST')
print('runtime parts:',len(P))
print('callback handlers:',len(cb))
print('restore validators:',len(restore))
print('duplicate callable defs:',len(dups))
print('diagnostic checks:',checks)
print('PASS' if not errs else 'FAIL')
if errs:
    print('\n'.join(' - '+x for x in errs)); sys.exit(1)
# v180_total_final_diagnostics
