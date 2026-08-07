# v158_no_process_messages_income_notes
"""v158: remove auxiliary process messages and add income annotations to every annotated Excel layout."""

VERSION = "bot_v158_no_process_messages_income_notes"

# ---------------------------------------------------------------------------
# 1) Process UI messages are disabled completely.
#    Internal process registry/journal remains untouched; only Telegram helper
#    messages such as "Операция выполняется / telegram_update" are suppressed.
# ---------------------------------------------------------------------------

def process_visual_status_enabled(chat_id: int) -> bool:
    return False


def _v156_process_status_arm(chat_id: int | None, hint: str = "") -> None:
    return None


def _v156_process_status_schedule(chat_id: int, delay: float) -> None:
    return None


def _v156_process_status_tick(chat_id: int) -> None:
    try:
        _v156_process_status_clear(int(chat_id), delete=True)
    except Exception:
        pass


# If this module is ever hot-loaded into a running process, remove any process
# helper message already tracked in RAM. On a normal Render deploy this map is
# empty because the process restarts.
try:
    with _V156_PROCESS_UI_LOCK:
        _v158_existing_process_chats = list((_V156_PROCESS_UI or {}).keys())
except Exception:
    _v158_existing_process_chats = []
for _v158_cid in _v158_existing_process_chats:
    try:
        _v156_process_status_clear(int(_v158_cid), delete=True)
    except Exception:
        pass


# Remove the now-useless process menu/status line from INFO while preserving the
# v157 vertical layout and every other INFO control.
_V158_PREV_BUILD_INFO_TEXT = globals().get("build_info_text")
_V158_PREV_BUILD_INFO_KEYBOARD = globals().get("build_info_keyboard")


def build_info_text(chat_id: int, *args, **kwargs) -> str:
    text = ""
    if callable(_V158_PREV_BUILD_INFO_TEXT):
        try:
            text = str(_V158_PREV_BUILD_INFO_TEXT(int(chat_id), *args, **kwargs) or "")
        except TypeError:
            text = str(_V158_PREV_BUILD_INFO_TEXT(int(chat_id)) or "")
    rows = [
        row for row in text.splitlines()
        if not str(row).strip().casefold().startswith("окно процессов:")
    ]
    # Avoid leaving a double/triple blank block where the status line was.
    cleaned = []
    for row in rows:
        if not str(row).strip() and cleaned and not str(cleaned[-1]).strip():
            continue
        cleaned.append(row)
    return "\n".join(cleaned)[:3900]


def _v158_button_cb(btn) -> str:
    if isinstance(btn, dict):
        return str(btn.get("callback_data") or "")
    return str(getattr(btn, "callback_data", "") or "")


def _v158_button_text(btn) -> str:
    if isinstance(btn, dict):
        return str(btn.get("text") or "")
    return str(getattr(btn, "text", "") or "")


def build_info_keyboard(chat_id: int):
    kb = _V158_PREV_BUILD_INFO_KEYBOARD(int(chat_id)) if callable(_V158_PREV_BUILD_INFO_KEYBOARD) else types.InlineKeyboardMarkup()
    rows = list(getattr(kb, "keyboard", None) or getattr(kb, "inline_keyboard", None) or [])
    blocked = {
        "v156:process_visual_toggle",
        "v157:process_menu",
        "v157:process_owner_toggle",
        "v157:process_others_toggle",
    }
    new_rows = []
    for row in rows:
        kept = []
        for btn in list(row or []):
            cb = _v158_button_cb(btn)
            txt = _v158_button_text(btn).strip().casefold()
            if cb in blocked or txt.startswith("👁 окно процессов") or txt.startswith("👁️ окно процессов"):
                continue
            kept.append(btn)
        if kept:
            new_rows.append(kept)
    try:
        kb.keyboard = new_rows
    except Exception:
        try:
            kb.inline_keyboard = new_rows
        except Exception:
            pass
    return kb


# Old INFO messages can still contain process-menu buttons after deploy. Their
# callbacks are consumed safely and do not open/recreate any process UI.
def _v157_handle_callback(call) -> bool:
    raw = str(getattr(call, "data", "") or "")
    resolved = raw
    try:
        resolver = globals().get("resolve_short_callback")
        if callable(resolver):
            resolved = str(resolver(raw) or raw)
    except Exception:
        pass
    if resolved not in {
        "v156:process_visual_toggle",
        "v157:process_menu",
        "v157:process_owner_toggle",
        "v157:process_others_toggle",
    }:
        return False
    try:
        bot.answer_callback_query(call.id, "Окно процессов отключено: служебные сообщения больше не создаются.")
    except Exception:
        pass
    return True


# ---------------------------------------------------------------------------
# 2) Excel / Google annotations on INCOME cells.
#    Existing expense annotations remain unchanged. We add the operation
#    description as a Note/Comment to the income amount cell in all annotated
#    layouts (simple, category, compact, USD suffix and Google Sheets).
# ---------------------------------------------------------------------------
_V158_PREV_MODERN_SIMPLE = globals().get("_modern_simple_excel_styles_comments")
_V158_PREV_MODERN_CATEGORY = globals().get("_modern_category_excel_styles_comments")
_V158_PREV_CATEGORY_EXPECTED = globals().get("_category_excel_expected_annotations")
_V158_PREV_CATEGORY_COMPACT_ROWS = globals().get("_category_rows_without_description")
_V158_PREV_COMPACT_ROWS = globals().get("_compact_simple_excel_rows_and_annotations")

_V158_SUMMARY_LABELS = {
    "остаток с прошлого раза", "сумма по статьям", "расход", "приход",
    "приход за период", "расход за период", "остаток на руках", "на руках:",
    "гомонковые", "остаток в обороте", "продукты",
    "расход еды на человека в сутки", "расчёт",
}


def _v158_real_operation_description(value) -> str:
    note = str(value or "").strip()
    if not note or note.casefold() in _V158_SUMMARY_LABELS:
        return ""
    return note


def _v158_add_income_annotations_from_description(rows: list[list], comments: dict) -> dict:
    out = dict(comments or {})
    header_seen = False
    for r_idx, raw in enumerate(rows or [], start=1):
        row = list(raw or [])
        first = str(row[0] if row else "").strip().casefold()
        second = str(row[1] if len(row) > 1 else "").strip()
        second_cf = second.casefold()
        is_header = first in {"дата", "date"} and second_cf in {"описание", "description", "приход/выдача", "amount"}
        if is_header:
            header_seen = True
            continue
        if first in {"ars", "usd"} and not second:
            header_seen = False
            continue
        if not header_seen:
            continue
        note = _v158_real_operation_description(second)
        if not note:
            continue
        income = row[2] if len(row) > 2 else ""
        if _excel_nonempty(income):
            out[(r_idx, 3)] = note
    return out


def _modern_simple_excel_styles_comments(rows: list[list]):
    if callable(_V158_PREV_MODERN_SIMPLE):
        styles, comments, header_row, widths = _V158_PREV_MODERN_SIMPLE(rows)
    else:
        max_cols = max((len(r) for r in rows or []), default=4)
        styles, comments, header_row, widths = [[0] * max_cols for _ in rows or []], {}, 1, [13, 38, 15, 15]
    comments = _v158_add_income_annotations_from_description(rows, comments)
    return styles, comments, header_row, widths


def _modern_category_excel_styles_comments(rows: list[list]):
    if callable(_V158_PREV_MODERN_CATEGORY):
        styles, comments, header_row, widths = _V158_PREV_MODERN_CATEGORY(rows)
    else:
        max_cols = max((len(r) for r in rows or []), default=4)
        styles, comments, header_row, widths = [[0] * max_cols for _ in rows or []], {}, 1, [13, 36, 15, 18]
    comments = _v158_add_income_annotations_from_description(rows, comments)
    return styles, comments, header_row, widths


def _category_excel_expected_annotations(rows: list[list]) -> dict[tuple[int, int], str]:
    expected = {}
    if callable(_V158_PREV_CATEGORY_EXPECTED):
        try:
            expected.update(_V158_PREV_CATEGORY_EXPECTED(rows) or {})
        except Exception:
            pass
    return _v158_add_income_annotations_from_description(rows, expected)


def _category_rows_without_description(rows: list[list]) -> tuple[list[list], dict[tuple[int, int], str]]:
    if callable(_V158_PREV_CATEGORY_COMPACT_ROWS):
        out_rows, annotations = _V158_PREV_CATEGORY_COMPACT_ROWS(rows)
    else:
        out_rows, annotations = list(rows or []), {}
    annotations = dict(annotations or {})

    # ARS category rows lose Description: original C (income) becomes B.
    # USD rows intentionally keep Description in v154: income remains C.
    in_usd = False
    for r_idx, raw in enumerate(rows or [], start=1):
        row = list(raw or [])
        first_raw = str(row[0] if row else "").strip()
        if first_raw.upper() == "USD":
            in_usd = True
            continue
        if len(row) < 3:
            continue
        first = first_raw.casefold()
        desc = str(row[1] or "").strip()
        is_header = first in {"дата", "date"} and desc.casefold() in {"описание", "description"}
        if is_header:
            continue
        note = _v158_real_operation_description(desc)
        if not note or not _excel_nonempty(row[2]):
            continue
        annotations[(r_idx, 3 if in_usd else 2)] = note
    return out_rows, annotations


def _compact_simple_excel_rows_and_annotations(raw_rows: list[tuple], opening_balance: float, target_chat_id: int | None = None):
    if callable(_V158_PREV_COMPACT_ROWS):
        rows, annotations = _V158_PREV_COMPACT_ROWS(raw_rows, opening_balance, target_chat_id)
    else:
        rows, annotations = [], {}
    annotations = dict(annotations or {})

    # ARS compact annotations are already generated from the source records by
    # v151. Appended USD remains a four-column table, so add income Notes here.
    in_usd = False
    header_seen = False
    for r_idx, raw in enumerate(rows or [], start=1):
        row = list(raw or [])
        first_raw = str(row[0] if row else "").strip()
        if first_raw.upper() == "USD":
            in_usd = True
            header_seen = False
            continue
        if not in_usd:
            continue
        first = first_raw.casefold()
        desc = str(row[1] if len(row) > 1 else "").strip()
        if first in {"дата", "date"} and desc.casefold() in {"описание", "description"}:
            header_seen = True
            continue
        if not header_seen or len(row) < 3:
            continue
        note = _v158_real_operation_description(desc)
        if note and _excel_nonempty(row[2]):
            annotations[(r_idx, 3)] = note
    return rows, annotations


# v153 restore validator must accept the new release's full-state snapshots.
_V158_PREV_RESTORE_VALIDATE = globals().get("_v153_validate_restore_gz")
try:
    import gzip as _v158_gzip
    import json as _v158_json
    import os as _v158_os
    import shutil as _v158_shutil
    import sqlite3 as _v158_sqlite3
    import tempfile as _v158_tempfile
except Exception:
    pass


def _v153_validate_restore_gz(gz_path: str) -> tuple[dict, str]:
    folder = _v158_tempfile.mkdtemp(prefix="v158_restore_validate_")
    raw = _v158_os.path.join(folder, "restore.sqlite3")
    try:
        with _v158_gzip.open(gz_path, "rb") as fin, open(raw, "wb") as fout:
            _v158_shutil.copyfileobj(fin, fout, 1024 * 1024)
        conn = _v158_sqlite3.connect(raw)
        try:
            integrity = str(conn.execute("PRAGMA integrity_check").fetchone()[0])
            if integrity.lower() != "ok":
                raise RuntimeError(f"SQLite integrity_check: {integrity}")
            row = conn.execute("SELECT v FROM meta WHERE kind='v153_export' AND k='manifest'").fetchone()
            if not row:
                raise RuntimeError("manifest v153 not found")
            manifest = _v158_json.loads(row[0])
        finally:
            conn.close()
        if str(manifest.get("kind")) != "telegram_bot_full_state_v153":
            raise RuntimeError("unknown export kind")
        if int(manifest.get("schema_version") or 0) != int(V153_EXPORT_SCHEMA):
            raise RuntimeError("unsupported export schema")
        export_version = str(manifest.get("bot_version") or "")
        if not export_version.startswith(("bot_v153_", "bot_v154_", "bot_v155_", "bot_v156_", "bot_v157_", "bot_v158_")):
            raise RuntimeError(f"unsupported bot version: {export_version or 'missing'}")
        checksum = _v153_db_logical_checksum(raw)
        if checksum != str(manifest.get("checksum") or ""):
            raise RuntimeError("checksum mismatch")
        return manifest, raw
    except Exception:
        _v158_shutil.rmtree(folder, ignore_errors=True)
        raise


try:
    bot_journal(
        "v158_no_process_messages_income_notes_installed",
        int(OWNER_ID or 0),
        "process_chat_messages=0; process_internal_journal=1; income_excel_annotations=1; info_process_menu=removed",
    )
except Exception:
    pass

# v158_no_process_messages_income_notes
