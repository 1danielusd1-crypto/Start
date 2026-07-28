# v132_anonymous_admin_forward_fix
from pathlib import Path
import hashlib
import json
import os

MODULAR_VERSION = "bot_v132_anonymous_admin_forward_fix"
MODULE_FILE_VERSION = "v132_anonymous_admin_forward_fix"
MODULAR_SOURCE_PARTS = [
    'modules/00_core.py',
    'modules/10_mega_runtime.py',
    'modules/20_callback_tokens.py',
    'modules/30_secret.py',
    'modules/40_message_router.py',
    'modules/50_forwarding.py',
    'modules/60_finance_currency.py',
    'modules/61_forwarding_ui.py',
    'modules/62_finance_ui.py',
    'modules/63_google_sheets.py',
    'modules/70_fast_ui.py',
    'modules/80_callback_router.py',
    'modules/90_commands_exports.py',
    'modules/91_finance_records_handlers.py',
    'modules/99_web_runtime.py',
]
_MODULAR_ROOT = Path(__file__).resolve().parent
_MODULAR_MERGED_CACHE = None
_MANIFEST_PATH = _MODULAR_ROOT / "modules_manifest.json"


def _sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _validate_modular_package() -> None:
    """Fail before Telegram/MEGA starts if GitHub contains a mixed modular version."""
    if not _MANIFEST_PATH.exists():
        raise RuntimeError("modules_manifest.json is missing; upload the complete deploy package")
    try:
        manifest = json.loads(_MANIFEST_PATH.read_text(encoding="utf-8"))
    except Exception as exc:
        raise RuntimeError(f"modules_manifest.json is invalid: {exc}") from exc

    manifest_version = str(manifest.get("version") or "")
    if manifest_version != MODULAR_VERSION:
        raise RuntimeError(
            f"modular version mismatch: bot.py={MODULAR_VERSION}, manifest={manifest_version or 'empty'}"
        )

    files = manifest.get("files") or {}
    problems = []
    for rel in MODULAR_SOURCE_PARTS:
        path = _MODULAR_ROOT / rel
        expected_hash = str(files.get(rel) or "")
        if not path.exists():
            problems.append(f"missing {rel}")
            continue
        raw = path.read_bytes()
        actual_hash = _sha256_bytes(raw)
        if not expected_hash:
            problems.append(f"manifest has no hash for {rel}")
        elif actual_hash != expected_hash:
            problems.append(f"wrong version/content {rel}")
        try:
            rows = raw.decode("utf-8").splitlines()
            file_markers = manifest.get("file_markers") or {}
            expected_file_marker = str(file_markers.get(rel) or manifest.get("file_marker") or MODULE_FILE_VERSION)
            marker = f"# {expected_file_marker}"
            if not rows or rows[0].strip() != marker or rows[-1].strip() != marker:
                problems.append(f"version marker mismatch {rel}: expected {expected_file_marker}")
        except Exception:
            problems.append(f"cannot read version marker {rel}")

    extra_manifest = sorted(set(files) - set(MODULAR_SOURCE_PARTS))
    if extra_manifest:
        problems.append("unexpected manifest entries: " + ", ".join(extra_manifest[:5]))

    if problems:
        raise RuntimeError(
            "MODULAR PACKAGE CHECK FAILED. Do not run mixed files. "
            + "; ".join(problems[:12])
            + ". Upload bot.py + modules/ + modules_manifest.json from the same ZIP."
        )


def _exec_source_part(relative_path: str) -> None:
    path = _MODULAR_ROOT / relative_path
    text = path.read_text(encoding="utf-8")
    exec(compile(text, str(path), "exec"), globals(), globals())


def _strip_part_version(text: str) -> str:
    rows = text.splitlines()
    if rows and rows[0].strip().startswith("# v"):
        rows = rows[1:]
    if rows and rows[-1].strip().startswith("# v"):
        rows = rows[:-1]
    return "\n".join(rows).rstrip() + "\n"


def _modular_merged_source_path() -> str:
    global _MODULAR_MERGED_CACHE
    tmp_dir = os.getenv("MEGA_LOCAL_TMP_DIR", "/tmp").strip() or "/tmp"
    os.makedirs(tmp_dir, exist_ok=True)
    target = os.path.join(tmp_dir, f"{MODULAR_VERSION}_full.py")
    chunks = []
    for rel in MODULAR_SOURCE_PARTS:
        chunks.append(_strip_part_version((_MODULAR_ROOT / rel).read_text(encoding="utf-8")))
    with open(target, "w", encoding="utf-8") as fh:
        fh.write(f"# {MODULE_FILE_VERSION}\n")
        fh.write("\n".join(chunks).rstrip() + "\n")
        fh.write('if __name__ == "__main__":\n    main()\n')
        fh.write(f"# {MODULE_FILE_VERSION}\n")
    _MODULAR_MERGED_CACHE = target
    return target


_validate_modular_package()
for _part in MODULAR_SOURCE_PARTS:
    _exec_source_part(_part)

if __name__ == "__main__":
    main()
# v132_anonymous_admin_forward_fix
