# v191_gz_restore_schema_compat
from pathlib import Path
import hashlib, json, os
MODULAR_VERSION = "bot_v191_gz_restore_schema_compat"
MODULE_FILE_VERSION = "v191_gz_restore_schema_compat"
MODULAR_SOURCE_PARTS = ['00_core.py', '10_mega_runtime.py', '11_data_constitution.py', '15_operation_safety.py', '16_window_diagnostics.py', '17_memory_runtime.py', '20_callback_tokens.py', '30_secret.py', '35_reminders.py', '40_message_router.py', '50_forwarding.py', '60_finance_currency.py', '61_forwarding_ui.py', '62_finance_ui.py', '63_google_sheets.py', '70_fast_ui.py', '80_callback_router.py', '90_commands_exports.py', '91_finance_records_handlers.py', '72_multitenant_runtime.py', '99_web_runtime.py', '73_state_export_runtime.py', '74_ui_reliability_runtime.py', '75_platform_features_runtime.py', '76_tasks_runtime.py', '85_runtime_control.py', '89_callback_final.py']
_MODULAR_ROOT = Path(__file__).resolve().parent
_MODULAR_MERGED_CACHE = None
_MANIFEST_PATH = _MODULAR_ROOT / "modules_manifest.json"

def _sha256_bytes(raw: bytes) -> str: return hashlib.sha256(raw).hexdigest()

def _validate_modular_package() -> None:
    if not _MANIFEST_PATH.exists(): raise RuntimeError("modules_manifest.json is missing")
    manifest=json.loads(_MANIFEST_PATH.read_text(encoding="utf-8"))
    if str(manifest.get("version") or "") != MODULAR_VERSION: raise RuntimeError("modular version mismatch")
    files=manifest.get("files") or {}; markers=manifest.get("file_markers") or {}; problems=[]
    for rel in MODULAR_SOURCE_PARTS:
        path=_MODULAR_ROOT/rel
        if not path.exists(): problems.append(f"missing {rel}"); continue
        raw=path.read_bytes()
        if _sha256_bytes(raw) != str(files.get(rel) or ""): problems.append(f"hash {rel}")
        rows=raw.decode("utf-8").splitlines(); marker="# "+str(markers.get(rel) or MODULE_FILE_VERSION)
        if not rows or rows[0].strip()!=marker or rows[-1].strip()!=marker: problems.append(f"marker {rel}")
    if set(files)!=set(MODULAR_SOURCE_PARTS): problems.append("manifest parts mismatch")
    if problems: raise RuntimeError("MODULAR PACKAGE CHECK FAILED: "+"; ".join(problems[:12]))

def _exec_source_part(relative_path: str) -> None:
    path=_MODULAR_ROOT/relative_path; text=path.read_text(encoding="utf-8"); exec(compile(text,str(path),"exec"),globals(),globals())

def _strip_part_version(text: str) -> str:
    rows=text.splitlines()
    if rows and rows[0].strip().startswith("# v"): rows=rows[1:]
    if rows and rows[-1].strip().startswith("# v"): rows=rows[:-1]
    return "\n".join(rows).rstrip()+"\n"

def _modular_merged_source_path() -> str:
    global _MODULAR_MERGED_CACHE
    tmp_dir=os.getenv("MEGA_LOCAL_TMP_DIR","/tmp").strip() or "/tmp"; os.makedirs(tmp_dir,exist_ok=True)
    target=os.path.join(tmp_dir,f"{MODULAR_VERSION}_full.py")
    chunks=[_strip_part_version((_MODULAR_ROOT/rel).read_text(encoding="utf-8")) for rel in MODULAR_SOURCE_PARTS]
    with open(target,"w",encoding="utf-8") as fh:
        fh.write(f"# {MODULE_FILE_VERSION}\n"); fh.write("\n".join(chunks).rstrip()+"\n"); fh.write('if __name__ == "__main__":\n    main()\n'); fh.write(f"# {MODULE_FILE_VERSION}\n")
    _MODULAR_MERGED_CACHE=target; return target

_validate_modular_package()
for _part in MODULAR_SOURCE_PARTS: _exec_source_part(_part)
if __name__ == "__main__": main()
# v191_gz_restore_schema_compat
