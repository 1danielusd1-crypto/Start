# v130_modular_split
from pathlib import Path
import os

MODULAR_SOURCE_PARTS = ['modules/00_core.py', 'modules/10_mega_runtime.py', 'modules/20_callback_tokens.py', 'modules/30_secret.py', 'modules/40_message_router.py', 'modules/50_forwarding.py', 'modules/60_finance_currency.py', 'modules/61_forwarding_ui.py', 'modules/62_finance_ui.py', 'modules/63_google_sheets.py', 'modules/70_fast_ui.py', 'modules/80_callback_router.py', 'modules/90_commands_exports.py', 'modules/91_finance_records_handlers.py', 'modules/99_web_runtime.py']
_MODULAR_ROOT = Path(__file__).resolve().parent
_MODULAR_MERGED_CACHE = None

def _exec_source_part(relative_path: str) -> None:
    path = _MODULAR_ROOT / relative_path
    text = path.read_text(encoding="utf-8")
    exec(compile(text, str(path), "exec"), globals(), globals())

def _strip_part_version(text: str) -> str:
    rows = text.splitlines()
    if rows and rows[0].strip().startswith("# v130_modular_split"):
        rows = rows[1:]
    if rows and rows[-1].strip().startswith("# v130_modular_split"):
        rows = rows[:-1]
    return "\n".join(rows).rstrip() + "\n"

def _modular_merged_source_path() -> str:
    global _MODULAR_MERGED_CACHE
    tmp_dir = os.getenv("MEGA_LOCAL_TMP_DIR", "/tmp").strip() or "/tmp"
    os.makedirs(tmp_dir, exist_ok=True)
    target = os.path.join(tmp_dir, "bot_v130_modular_split_full.py")
    chunks = []
    for rel in MODULAR_SOURCE_PARTS:
        chunks.append(_strip_part_version((_MODULAR_ROOT / rel).read_text(encoding="utf-8")))
    with open(target, "w", encoding="utf-8") as fh:
        fh.write("# v130_modular_split\n")
        fh.write("\n".join(chunks).rstrip() + "\n")
        fh.write('if __name__ == "__main__":\n    main()\n')
        fh.write("# v130_modular_split\n")
    _MODULAR_MERGED_CACHE = target
    return target

for _part in MODULAR_SOURCE_PARTS:
    _exec_source_part(_part)

if __name__ == "__main__":
    main()
# v130_modular_split
