# v175_light_mode
from pathlib import Path
import hashlib
import json
import os

MODULAR_VERSION = "bot_v175_light_mode"
MODULE_FILE_VERSION = "v175_light_mode"
MODULAR_SOURCE_PARTS = [
    '00_core.py',
    '10_mega_runtime.py',
    '15_operation_safety.py',
    '16_window_diagnostics.py',
    '17_memory_runtime.py',
    '20_callback_tokens.py',
    '30_secret.py',
    '35_reminders.py',
    '40_message_router.py',
    '50_forwarding.py',
    '60_finance_currency.py',
    '61_forwarding_ui.py',
    '62_finance_ui.py',
    '63_google_sheets.py',
    '70_fast_ui.py',
    '80_callback_router.py',
    '90_commands_exports.py',
    '91_finance_records_handlers.py',
    '92_v147_diagnostic_hardening.py',
    '93_v148_multitenant_spaces.py',
    '94_v149_tenant_google_merged_reminders.py',
    '99_web_runtime.py',
    '100_v150_excel_reserve_chat_lifecycle.py',
    '101_v151_redo_fixes_5_6_7.py',
    '102_v152_human_journals_chat_rights.py',
    '103_v153_remaining_fixes_11_16.py',
    '104_v154_excel_usd_isolation_date_marks.py',
    '105_v155_button_navigation_audit.py',
    '106_v156_process_status_usd_excel.py',
    '107_v157_process_menu_navigation_repair.py',
    '108_v158_no_process_messages_income_notes.py',
    '109_v159_internal_timers_helper_windows.py',
    '110_v160_stability_parallel_windows_annotations.py',
    '111_v161_button_window_stability.py',
    '112_v162_start_hard_fix.py',
    '113_v163_audit_hardening.py',
    '114_v164_circle_hierarchy_spaces.py',
    '115_v165_owner_first_circle_compat.py',
    '116_v166_fast_parallel_forward_pairs.py',
    '117_v167_excel_formulas_thuwed_google_tz.py',
    '118_v171_all_tz_reliability.py',
    '119_v172_task_dispatcher.py',
    '120_v173_reminder_crosschat_unique_journals.py',
    '121_v174_simplified_task_dispatcher.py',
    '122_v175_light_mode.py',
]
_MODULAR_ROOT = Path(__file__).resolve().parent
_MODULAR_MERGED_CACHE = None
_MANIFEST_PATH = _MODULAR_ROOT / "modules_manifest.json"


def _sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _validate_modular_package() -> None:
    """Останавливаем запуск до Telegram/MEGA, если в корне смешаны файлы разных комплектов."""
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
    markers = manifest.get("file_markers") or {}
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
            expected_marker = str(markers.get(rel) or manifest.get("file_marker") or MODULE_FILE_VERSION)
            marker = f"# {expected_marker}"
            if not rows or rows[0].strip() != marker or rows[-1].strip() != marker:
                problems.append(f"version marker mismatch {rel}: expected {expected_marker}")
        except Exception:
            problems.append(f"cannot read version marker {rel}")

    extra_manifest = sorted(set(files) - set(MODULAR_SOURCE_PARTS))
    if extra_manifest:
        problems.append("unexpected manifest entries: " + ", ".join(extra_manifest[:5]))

    if problems:
        raise RuntimeError(
            "MODULAR PACKAGE CHECK FAILED. Do not run mixed files. "
            + "; ".join(problems[:12])
            + ". Upload bot.py + all numbered .py files + modules_manifest.json from the same ZIP."
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
# v175_light_mode
