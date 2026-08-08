# v154_excel_usd_isolation_date_marks
"""v154: expense marks in F111/F114 and strict ARS/USD Excel isolation."""

import calendar as _v154_calendar
import copy as _v154_copy
import os as _v154_os
import shutil as _v154_shutil
import sqlite3 as _v154_sqlite3
import gzip as _v154_gzip
import tempfile as _v154_tempfile
import json as _v154_json

VERSION = "bot_v154_excel_usd_isolation_date_marks"

_V154_BASE_PERIOD_EXCEL_KEYBOARD = globals().get("_period_excel_style_keyboard")
_V154_BASE_CATEGORY_COMPACT = globals().get("_category_rows_without_description")
_V154_BASE_MODERN_COMPACT = globals().get("_modern_compact_excel_styles_comments")
_V154_BASE_MODERN_SIMPLE = globals().get("_modern_simple_excel_styles_comments")
_V154_BASE_MODERN_CATEGORY = globals().get("_modern_category_excel_styles_comments")
_V154_BASE_MODERN_CATEGORY_COMPACT = globals().get("_modern_category_no_description_styles_comments")


def excel_usd_table_enabled(chat_id: int) -> bool:
    """Whether a separate USD operation table is appended to Excel/Google exports."""
    try:
        settings = get_chat_store(int(chat_id)).setdefault("settings", {})
        return bool(settings.get("excel_include_usd_table", True))
    except Exception:
        return True


def set_excel_usd_table_enabled(chat_id: int, enabled: bool) -> bool:
    chat_id = int(chat_id)
    settings = get_chat_store(chat_id).setdefault("settings", {})
    settings["excel_include_usd_table"] = bool(enabled)
    save_data(data, chat_ids=[chat_id])
    try:
        schedule_config_backup_for_chats(chat_id, delay=0.5)
    except Exception:
        pass
    try:
        bot_journal("excel_usd_table_toggle", chat_id, f"enabled={bool(enabled)}")
    except Exception:
        pass
    return bool(enabled)


def toggle_excel_usd_table_enabled(chat_id: int) -> bool:
    return set_excel_usd_table_enabled(int(chat_id), not excel_usd_table_enabled(int(chat_id)))


def _period_excel_style_keyboard(scope: str, target_chat_id: int, mode: str, file_type: str, day_key: str, owner_day_key: str):
    """F179: original controls + an explicit independent USD-table switch."""
    kb = _V154_BASE_PERIOD_EXCEL_KEYBOARD(scope, target_chat_id, mode, file_type, day_key, owner_day_key)
    enabled = excel_usd_table_enabled(int(target_chat_id))
    label = f"💵 USD расходы в таблице: {'ВКЛ' if enabled else 'ВЫКЛ'}"
    row = [IB(label, callback_data=export_callback(
        f"exp_excel_dollar_toggle:{scope}:{int(target_chat_id)}:{mode}:{file_type}:{day_key}:{owner_day_key}"
    ))]
    # Keep navigation at the bottom; insert the switch directly above it.
    try:
        insert_at = max(0, len(kb.keyboard) - 2)
        kb.keyboard.insert(insert_at, row)
    except Exception:
        kb.row(*row)
    return kb


def _v154_day_has_expense(chat_id: int | None, day_key: str) -> bool:
    if chat_id is None:
        return False
    try:
        store = get_chat_store(int(chat_id))
        return bool(expense_anchor_records_for_day(store, str(day_key)))
    except Exception:
        return False


def _export_calendar_start_keyboard(view_year: int, view_month: int, return_day_key: str, chat_id: int | None = None):
    """F111. Mark a date with 📝 only when that day actually has an expense."""
    kb = types.InlineKeyboardMarkup(row_width=7)
    last_day = _v154_calendar.monthrange(int(view_year), int(view_month))[1]
    buttons = []
    for day_num in range(1, last_day + 1):
        day_key = _date_key_from_ymd(view_year, view_month, day_num)
        label = f"📝{day_num}" if _v154_day_has_expense(chat_id, day_key) else str(day_num)
        buttons.append(IB(label, callback_data=export_callback(
            f"exp_pick_set_start:{view_year}:{view_month}:{day_num}:{return_day_key}"
        )))
    for idx in range(0, len(buttons), 7):
        kb.row(*buttons[idx:idx + 7])
    prev_y, prev_m = _shift_month(view_year, view_month, -1)
    next_y, next_m = _shift_month(view_year, view_month, 1)
    kb.row(
        IB("⬅️ Месяц", callback_data=export_callback(f"exp_pick_start:{prev_y}:{prev_m}:{return_day_key}")),
        IB(f"{russian_month_name(view_month)} {view_year}", callback_data="none"),
        IB("Месяц ➡️", callback_data=export_callback(f"exp_pick_start:{next_y}:{next_m}:{return_day_key}")),
    )
    kb.row(
        IB("◀️ Год", callback_data=export_callback(f"exp_pick_start:{view_year-1}:{view_month}:{return_day_key}")),
        IB(str(view_year), callback_data="none"),
        IB("Год ▶️", callback_data=export_callback(f"exp_pick_start:{view_year+1}:{view_month}:{return_day_key}")),
    )
    kb.row(IB("🔙 Назад в CSV / Excel", callback_data=f"d:{return_day_key}:csv_all"))
    return kb


def _export_end_calendar_keyboard(start_key: str, start_rid: int, view_year: int, view_month: int, return_day_key: str, chat_id: int | None = None):
    """F114. Mark selectable dates that contain expenses, without changing range rules."""
    kb = types.InlineKeyboardMarkup(row_width=7)
    last_day = _v154_calendar.monthrange(int(view_year), int(view_month))[1]
    buttons = []
    for day_num in range(1, last_day + 1):
        day_key = _date_key_from_ymd(view_year, view_month, day_num)
        if day_key < start_key:
            buttons.append(IB("·", callback_data="none"))
        else:
            label = f"📝{day_num}" if _v154_day_has_expense(chat_id, day_key) else str(day_num)
            buttons.append(IB(label, callback_data=export_callback(
                f"exp_pick_set_end:{start_key}:{int(start_rid)}:{view_year}:{view_month}:{day_num}:{return_day_key}"
            )))
    for idx in range(0, len(buttons), 7):
        kb.row(*buttons[idx:idx + 7])
    prev_y, prev_m = _shift_month(view_year, view_month, -1)
    next_y, next_m = _shift_month(view_year, view_month, 1)
    nav = []
    if f"{prev_y:04d}-{prev_m:02d}" >= start_key[:7]:
        nav.append(IB("⬅️ Месяц", callback_data=export_callback(
            f"exp_pick_end:{start_key}:{int(start_rid)}:{prev_y}:{prev_m}:{return_day_key}"
        )))
    else:
        nav.append(IB(" ", callback_data="none"))
    nav.append(IB(f"{russian_month_name(view_month)} {view_year}", callback_data="none"))
    nav.append(IB("Месяц ➡️", callback_data=export_callback(
        f"exp_pick_end:{start_key}:{int(start_rid)}:{next_y}:{next_m}:{return_day_key}"
    )))
    kb.row(*nav)
    kb.row(
        IB("◀️ Год", callback_data=export_callback(f"exp_pick_end:{start_key}:{int(start_rid)}:{view_year-1}:{view_month}:{return_day_key}")),
        IB(str(view_year), callback_data="none"),
        IB("Год ▶️", callback_data=export_callback(f"exp_pick_end:{start_key}:{int(start_rid)}:{view_year+1}:{view_month}:{return_day_key}")),
    )
    start_dt = datetime.strptime(start_key, "%Y-%m-%d")
    kb.row(IB("🔙 Изменить начало", callback_data=export_callback(
        f"exp_pick_set_start:{start_dt.year}:{start_dt.month}:{start_dt.day}:{return_day_key}"
    )))
    return kb


# Strict ledger isolation. v151 additionally read embedded usd_amount from ARS records;
# that caused ARS values to leak into / duplicate the USD export table.
def _v151_usd_records(chat_id: int) -> list[dict]:
    store = get_chat_store(int(chat_id))
    active = _v151_sync_currency_snapshots(store)
    source = store.get("records", []) if active == "usd" else store.get("usd_records", [])
    rows = []
    seen = set()
    for rec in source or []:
        if not isinstance(rec, dict):
            continue
        operation_key = str(rec.get("operation_key") or "").strip()
        source_msg_id = int(rec.get("source_msg_id") or 0)
        key = ("op", operation_key) if operation_key else (("msg", source_msg_id) if source_msg_id else (int(rec.get("id") or 0), str(rec.get("timestamp") or ""), _v151_day_key(rec)))
        if key in seen:
            continue
        seen.add(key)
        item = dict(rec)
        item["_v151_amount"] = _v151_float(rec.get("amount"))
        item["_v151_note"] = str(rec.get("note") or "")
        item["_v151_currency"] = "usd"
        rows.append(item)
    try:
        return sorted(rows, key=record_sort_key)
    except Exception:
        return rows


def _v154_join_ars_usd(ars_rows: list[list], usd_rows: list[list], chat_id: int) -> list[list]:
    if not excel_usd_table_enabled(int(chat_id)):
        return list(ars_rows or [])
    # Exactly two blank rows between ARS and USD sections.
    return list(ars_rows or []) + [[], []] + list(usd_rows or [])


def build_exact_category_stats_xlsx_rows(target_chat_id: int, start_key: str, start_rid: int, end_key: str, end_rid: int) -> list[list]:
    """ARS keeps category layout; USD is always a clean four-column operation table."""
    previous = getattr(_V151_EXPORT_LOCAL, "value", None)
    if not previous:
        _V151_EXPORT_LOCAL.value = {
            "kind": "exact", "target_chat_id": int(target_chat_id),
            "start_key": str(start_key)[:10], "start_rid": int(start_rid or 0),
            "end_key": str(end_key)[:10], "end_rid": int(end_rid or 0), "file_type": "xlsxstat",
        }
    try:
        ars_rows = _v151_category_table(int(target_chat_id), "ars")
        usd_rows, _ = _v151_simple_table(int(target_chat_id), "usd", compact=False)
        return _v154_join_ars_usd(ars_rows, usd_rows, int(target_chat_id))
    finally:
        if not previous:
            _V151_EXPORT_LOCAL.value = None


def _xlsx_simple_rows_with_balances(rows: list[list], opening_balance: float, target_chat_id: int | None = None) -> list[list]:
    if target_chat_id is None:
        return globals().get("_V150_BASE_SIMPLE_ROWS", lambda r, o, *_: r)(rows, opening_balance, target_chat_id)
    ars_rows, _ = _v151_simple_table(int(target_chat_id), "ars", compact=False)
    usd_rows, _ = _v151_simple_table(int(target_chat_id), "usd", compact=False)
    return _v154_join_ars_usd(ars_rows, usd_rows, int(target_chat_id))


def _compact_simple_excel_rows_and_annotations(raw_rows: list[tuple], opening_balance: float, target_chat_id: int | None = None) -> tuple[list[list], dict[tuple[int, int], str]]:
    if target_chat_id is None:
        base = globals().get("_V150_BASE_COMPACT_ROWS")
        return base(raw_rows, opening_balance, target_chat_id) if callable(base) else ([], {})
    ars_rows, ars_notes = _v151_simple_table(int(target_chat_id), "ars", compact=True)
    if not excel_usd_table_enabled(int(target_chat_id)):
        return ars_rows, dict(ars_notes)
    # USD must retain Description, so even compact ARS exports append the four-column USD table.
    usd_rows, _ = _v151_simple_table(int(target_chat_id), "usd", compact=False)
    return ars_rows + [[], []] + usd_rows, dict(ars_notes)


def _category_rows_without_description(rows: list[list]) -> tuple[list[list], dict[tuple[int, int], str]]:
    """Compact only the ARS category section; never remove Description from the USD section."""
    usd_index = None
    for idx, row in enumerate(rows or []):
        try:
            if str((row or [""])[0]).strip().upper() == "USD":
                usd_index = idx
                break
        except Exception:
            pass
    if usd_index is None or not callable(_V154_BASE_CATEGORY_COMPACT):
        return _V154_BASE_CATEGORY_COMPACT(rows) if callable(_V154_BASE_CATEGORY_COMPACT) else (rows, {})
    ars_part = list(rows[:usd_index])
    usd_part = list(rows[usd_index:])
    compact_ars, annotations = _V154_BASE_CATEGORY_COMPACT(ars_part)
    return compact_ars + usd_part, annotations



def _v154_find_usd_section(rows: list[list]) -> int | None:
    for idx, row in enumerate(rows or []):
        try:
            if str((row or [""])[0]).strip().upper() == "USD":
                return idx
        except Exception:
            pass
    return None


def _v154_merge_styles(prefix_rows, suffix_rows, prefix_result, suffix_result, keep_suffix_comments=True):
    p_styles, p_comments, p_freeze, p_widths = prefix_result
    s_styles, s_comments, _s_freeze, s_widths = suffix_result
    offset = len(prefix_rows)
    comments = dict(p_comments or {})
    if keep_suffix_comments:
        for (r, c), text in (s_comments or {}).items():
            comments[(int(r) + offset, int(c))] = text
    max_len = max(len(p_widths or []), len(s_widths or []))
    widths = []
    for i in range(max_len):
        widths.append(max((p_widths or [0])[i] if i < len(p_widths or []) else 0, (s_widths or [0])[i] if i < len(s_widths or []) else 0))
    return list(p_styles or []) + list(s_styles or []), comments, p_freeze, widths


def _modern_compact_excel_styles_comments(rows: list[list], annotations: dict[tuple[int, int], str]):
    """Compact ARS may stay 3-column, while appended USD keeps its four-column simple layout."""
    idx = _v154_find_usd_section(rows)
    if idx is None or not callable(_V154_BASE_MODERN_COMPACT) or not callable(_V154_BASE_MODERN_SIMPLE):
        return _V154_BASE_MODERN_COMPACT(rows, annotations)
    prefix, suffix = list(rows[:idx]), list(rows[idx:])
    p = _V154_BASE_MODERN_COMPACT(prefix, annotations)
    s = _V154_BASE_MODERN_SIMPLE(suffix)
    # Compact writer validates only explicit compact annotations; keep USD descriptions in cells,
    # not as additional Notes that could fail the validation map.
    return _v154_merge_styles(prefix, suffix, p, s, keep_suffix_comments=False)


def _modern_category_excel_styles_comments(rows: list[list]):
    idx = _v154_find_usd_section(rows)
    if idx is None or not callable(_V154_BASE_MODERN_CATEGORY) or not callable(_V154_BASE_MODERN_SIMPLE):
        return _V154_BASE_MODERN_CATEGORY(rows)
    prefix, suffix = list(rows[:idx]), list(rows[idx:])
    return _v154_merge_styles(prefix, suffix, _V154_BASE_MODERN_CATEGORY(prefix), _V154_BASE_MODERN_SIMPLE(suffix), keep_suffix_comments=True)


def _modern_category_no_description_styles_comments(rows: list[list], annotations: dict[tuple[int, int], str]):
    idx = _v154_find_usd_section(rows)
    if idx is None or not callable(_V154_BASE_MODERN_CATEGORY_COMPACT) or not callable(_V154_BASE_MODERN_SIMPLE):
        return _V154_BASE_MODERN_CATEGORY_COMPACT(rows, annotations)
    prefix, suffix = list(rows[:idx]), list(rows[idx:])
    p = _V154_BASE_MODERN_CATEGORY_COMPACT(prefix, annotations)
    s = _V154_BASE_MODERN_SIMPLE(suffix)
    return _v154_merge_styles(prefix, suffix, p, s, keep_suffix_comments=False)


# v153 restore must accept snapshots generated by this v154 release too.
def _v153_validate_restore_gz(gz_path: str) -> tuple[dict, str]:
    folder = _v153_tempfile.mkdtemp(prefix="v154_restore_validate_")
    raw = _v154_os.path.join(folder, "restore.sqlite3")
    try:
        with _v154_gzip.open(gz_path, "rb") as fin, open(raw, "wb") as fout:
            _v154_shutil.copyfileobj(fin, fout, 1024 * 1024)
        conn = _v154_sqlite3.connect(raw)
        try:
            integrity = str(conn.execute("PRAGMA integrity_check").fetchone()[0])
            if integrity.lower() != "ok":
                raise RuntimeError(f"SQLite integrity_check: {integrity}")
            row = conn.execute("SELECT v FROM meta WHERE kind='v153_export' AND k='manifest'").fetchone()
            if not row:
                raise RuntimeError("manifest v153 not found")
            manifest = _v154_json.loads(row[0])
        finally:
            conn.close()
        if str(manifest.get("kind")) != "telegram_bot_full_state_v153":
            raise RuntimeError("unknown export kind")
        if int(manifest.get("schema_version") or 0) != V153_EXPORT_SCHEMA:
            raise RuntimeError("unsupported export schema")
        export_version = str(manifest.get("bot_version") or "")
        if not (export_version.startswith("bot_v153_") or export_version.startswith("bot_v154_")):
            raise RuntimeError(f"unsupported bot version: {export_version or 'missing'}")
        checksum = _v153_db_logical_checksum(raw)
        if checksum != str(manifest.get("checksum") or ""):
            raise RuntimeError("checksum mismatch")
        return manifest, raw
    except Exception:
        _v154_shutil.rmtree(folder, ignore_errors=True)
        raise


try:
    WINDOW_MARKER_CONSTANTS["exp_excel_dollar_toggle:*"] = "Ф179"
except Exception:
    pass

try:
    bot_journal("v154_excel_usd_isolation_installed", int(OWNER_ID or 0), "strict_usd_ledger=1; f111_f114_marks=1; f179_usd_toggle=1")
except Exception:
    pass

# v154_excel_usd_isolation_date_marks
