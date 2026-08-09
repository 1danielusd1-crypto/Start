# v168_clean_core_record_identity
"""v167: Excel formulas/formatting, Thu-Wed period, rolling Google tab, TZ lifecycle.

This module deliberately patches only the active public hooks after v166 so older callbacks
remain compatible while the visible semantics become Thursday -> Wednesday.
"""
import copy as _v167_copy
import os as _v167_os
import re as _v167_re
import tempfile as _v167_tempfile
import threading as _v167_threading
import time as _v167_time
import zipfile as _v167_zipfile
import xml.etree.ElementTree as _v167_ET
from datetime import datetime as _v167_datetime, timedelta as _v167_timedelta

VERSION = "bot_v168_clean_core_record_identity"
V167_FILE_MARKER = "v168_clean_core_record_identity"

_V167_BASE_V151_CATEGORIES = globals().get("_v151_categories")
_V167_BASE_V151_SIMPLE_TABLE = globals().get("_v151_simple_table")
_V167_BASE_V151_CATEGORY_TABLE = globals().get("_v151_category_table")
_V167_BASE_V151_CONTEXT_BOUNDS = globals().get("_v151_context_bounds")
_V167_BASE_PERIOD_BOUNDS = globals().get("_period_export_bounds")
_V167_BASE_PERIOD_ROWS = globals().get("_period_export_rows")
_V167_BASE_SEND_CSV_FOR_CHAT = globals().get("send_csv_for_chat_to")
_V167_BASE_SEND_CSV_WEDTHU = globals().get("send_csv_wedthu")
_V167_BASE_WRITE_SIMPLE = globals().get("_write_simple_xlsx")
_V167_BASE_WRITE_TABL = globals().get("_write_tabl_lsx_xlsx")
_V167_BASE_ADD_EXPORT_ROWS = globals().get("_add_export_period_rows")
_V167_BASE_SAVE_TZ = globals().get("_v160_save_tz")
_V167_BASE_EXPORT_TZ = globals().get("_v160_export_text")
_V167_BASE_SPECIAL_CALLBACK = globals().get("_v160_handle_special_callback")
_V167_BASE_AUGMENT_MARKUP = globals().get("_v160_augment_markup")

_V167_GOOGLE_LOCK = _v167_threading.RLock()
_V167_GOOGLE_RUNNING = set()
_V167_GOOGLE_SCHEDULER_STARTED = False
_V167_TZ_ARCHIVE_VERSION = ""


def _v167_cached(value):
    if isinstance(value, dict):
        value = value.get("value", 0)
    try:
        return float(value or 0)
    except Exception:
        return 0.0


def _v167_formula(formula: str, cached):
    val = _v167_cached(cached)
    if abs(val - round(val)) < 1e-9:
        val = int(round(val))
    return {"formula": str(formula).lstrip("="), "value": val}


def _v167_col(index1: int) -> str:
    try:
        return _xlsx_col_name(int(index1))
    except Exception:
        n = max(1, int(index1)); out = ""
        while n:
            n, rem = divmod(n - 1, 26); out = chr(65 + rem) + out
        return out


def _v167_find_label(rows, label: str, preferred_col: int | None = None):
    needle = str(label).strip().casefold()
    for idx, row in enumerate(rows or [], start=1):
        row = list(row or [])
        if preferred_col is not None:
            if preferred_col < len(row) and str(row[preferred_col] or "").strip().casefold() == needle:
                return idx
            continue
        for value in row[:3]:
            if str(value or "").strip().casefold() == needle:
                return idx
    return 0



def _v167_find_label_last(rows, label: str, preferred_col: int | None = None):
    needle = str(label).strip().casefold()
    for idx in range(len(rows or []), 0, -1):
        row = list((rows or [])[idx-1] or [])
        if preferred_col is not None:
            if preferred_col < len(row) and str(row[preferred_col] or "").strip().casefold() == needle:
                return idx
            continue
        for value in row[:3]:
            if str(value or "").strip().casefold() == needle:
                return idx
    return 0

def _v167_thuwed_bounds(day_key: str) -> tuple[str, str]:
    base = _v167_datetime.strptime(str(day_key)[:10], "%Y-%m-%d")
    # Monday=0 ... Thursday=3. Go back to the current period's Thursday.
    start = base - _v167_timedelta(days=((base.weekday() - 3) % 7))
    end = start + _v167_timedelta(days=6)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def _v167_period_title(start_key: str, end_key: str) -> str:
    start = _v167_datetime.strptime(str(start_key)[:10], "%Y-%m-%d")
    end = _v167_datetime.strptime(str(end_key)[:10], "%Y-%m-%d")
    if start.year == end.year and start.month == end.month:
        return f"{start:%d}-{end:%d.%m.%y}"
    if start.year == end.year:
        return f"{start:%d.%m}-{end:%d.%m.%y}"
    return f"{start:%d.%m.%y}-{end:%d.%m.%y}"


# ---------------------------------------------------------------------------
# 1) Thu -> Wed compatibility. Keep legacy callback token `wedthu` so old
# buttons still work, but change its meaning everywhere at the active hooks.
# ---------------------------------------------------------------------------
def _v151_context_bounds(chat_id: int, ctx: dict | None = None) -> tuple[str, str]:
    ctx2 = dict(ctx or (globals().get("_v151_context", lambda: {})() or {}))
    mode = str(ctx2.get("mode") or "all").replace("csv_", "").replace("xlsx_", "")
    if str(ctx2.get("kind") or "period") != "exact" and mode == "wedthu":
        return _v167_thuwed_bounds(str(ctx2.get("day_key") or today_key())[:10])
    if callable(_V167_BASE_V151_CONTEXT_BOUNDS):
        return _V167_BASE_V151_CONTEXT_BOUNDS(chat_id, ctx2)
    return str(ctx2.get("day_key") or today_key())[:10], str(ctx2.get("day_key") or today_key())[:10]


def _period_export_bounds(store: dict, mode: str, day_key: str) -> tuple[str, str]:
    normalized = str(mode or "all").replace("csv_", "").replace("xlsx_", "")
    if normalized == "wedthu":
        return _v167_thuwed_bounds(day_key)
    if callable(_V167_BASE_PERIOD_BOUNDS):
        return _V167_BASE_PERIOD_BOUNDS(store, mode, day_key)
    return day_key, day_key


def _period_export_rows(chat_id: int, mode: str, day_key: str):
    if callable(_V167_BASE_PERIOD_ROWS):
        rows, label = _V167_BASE_PERIOD_ROWS(chat_id, mode, day_key)
    else:
        rows, label = [], "за всё время"
    normalized = str(mode or "all").replace("csv_", "").replace("xlsx_", "")
    if normalized == "wedthu":
        label = ("USD " if str(label).startswith("USD ") else "") + "Чт–Ср"
    return rows, label


def send_csv_for_chat_to(recipient_chat_id: int, target_chat_id: int, mode: str, day_key: str):
    if str(mode or "").replace("csv_", "") == "wedthu":
        # Route through the active period exporter so the corrected 7-day bounds are used.
        return send_export_for_chat_to(int(recipient_chat_id), int(target_chat_id), "wedthu", day_key, "csv")
    if callable(_V167_BASE_SEND_CSV_FOR_CHAT):
        return _V167_BASE_SEND_CSV_FOR_CHAT(recipient_chat_id, target_chat_id, mode, day_key)


def send_csv_wedthu(chat_id: int, day_key: str):
    return send_export_for_chat_to(int(chat_id), int(chat_id), "wedthu", day_key, "csv")


# ---------------------------------------------------------------------------
# 2) Every configured expense article must remain visible, even at zero.
# ---------------------------------------------------------------------------
def _v151_categories(chat_id: int, records: list[dict]) -> list[str]:
    store = get_chat_store(int(chat_id))
    used = []
    for rec in records or []:
        try:
            amount = float(rec.get("_v151_amount") or 0)
        except Exception:
            amount = 0.0
        if amount >= 0:
            continue
        note = str(rec.get("_v151_note") or "")
        try:
            category = resolve_expense_category(note, store)
            override_slug = str(rec.get("category_override_slug") or "").strip()
            if override_slug:
                category = get_category_by_slug(override_slug, store) or category
        except Exception:
            category = "прочие"
        category = str(category or "прочие")
        if category not in used:
            used.append(category)
    try:
        categories = list(get_ordered_category_names(include_all=True, cats={x: 0 for x in used}, store=store) or [])
    except Exception:
        categories = []
    for cat in used:
        if cat not in categories:
            categories.append(cat)
    return categories or ["прочие"]


# ---------------------------------------------------------------------------
# 3) Restore live formulas in simple and category tables. Formula dictionaries
# are understood by both XLSX writer and Google Sheets (`formulaValue`).
# ---------------------------------------------------------------------------
def _v167_formulaize_simple(rows: list[list], compact: bool, chat_id: int, currency: str):
    rows = _v167_copy.deepcopy(rows or [])
    label_col = 0 if compact else 1
    income_col = 2 if compact else 3  # 1-based
    expense_col = 3 if compact else 4
    opening_row = _v167_find_label(rows, "Остаток с прошлого раза", label_col)
    income_row = _v167_find_label(rows, "Приход за период", label_col)
    expense_row = _v167_find_label(rows, "Расход за период", label_col)
    closing_row = _v167_find_label(rows, "Остаток на руках", label_col)
    reserve_row = _v167_find_label(rows, "Гомонковые", label_col)
    turnover_row = _v167_find_label(rows, "Остаток в обороте", label_col)
    if not (opening_row and income_row and expense_row and closing_row):
        return rows
    data_start = opening_row + 2
    data_end = max(data_start, income_row - 2)
    ic, ec = _v167_col(income_col), _v167_col(expense_col)
    rows[income_row-1][income_col-1] = _v167_formula(f"SUM({ic}{data_start}:{ic}{data_end})", rows[income_row-1][income_col-1])
    rows[expense_row-1][expense_col-1] = _v167_formula(f"SUM({ec}{data_start}:{ec}{data_end})", rows[expense_row-1][expense_col-1])
    rows[closing_row-1][income_col-1] = _v167_formula(
        f"{ic}{opening_row}+{ic}{income_row}-{ec}{expense_row}", rows[closing_row-1][income_col-1]
    )
    if reserve_row and turnover_row:
        rows[turnover_row-1][income_col-1] = _v167_formula(
            f"{ic}{closing_row}-{ic}{reserve_row}", rows[turnover_row-1][income_col-1]
        )
    products_row = _v167_find_label_last(rows, "Продукты", label_col)
    metric_row = _v167_find_label(rows, "Расход еды на человека в сутки", label_col)
    if str(currency).lower() == "ars" and products_row and not compact:
        # Simple table has no category columns; recalculate the common "продукт*" rows from Description.
        desc_col = "B"
        rows[products_row-1][income_col-1] = _v167_formula(
            f'SUMIF({desc_col}{data_start}:{desc_col}{data_end},"*продукт*",{ec}{data_start}:{ec}{data_end})',
            rows[products_row-1][income_col-1],
        )
    if str(currency).lower() == "ars" and products_row and metric_row:
        try:
            start_key, end_key = _v151_context_bounds(int(chat_id))
            records = _v151_records_in_context(int(chat_id), "ars", _v151_context())
            _products, _metric, days, rate = _v151_food_metric(int(chat_id), records, start_key, end_key)
            if float(rate or 0) > 0:
                rows[metric_row-1][income_col-1] = _v167_formula(
                    f"{ic}{products_row}/({max(1,int(days))}*5*{float(rate):g})",
                    rows[metric_row-1][income_col-1],
                )
        except Exception:
            pass
    return rows


def _v167_formulaize_category(rows: list[list], chat_id: int, currency: str):
    rows = _v167_copy.deepcopy(rows or [])
    opening_row = _v167_find_label(rows, "Остаток с прошлого раза", 1)
    sums_row = _v167_find_label(rows, "Сумма по статьям", 1)
    expense_row = _v167_find_label(rows, "Расход", 1)
    income_row = _v167_find_label(rows, "Приход", 1)
    closing_row = _v167_find_label(rows, "Остаток на руках", 1)
    reserve_row = _v167_find_label(rows, "Гомонковые", 1)
    turnover_row = _v167_find_label(rows, "Остаток в обороте", 1)
    header_row = 0
    for i, row in enumerate(rows, start=1):
        if len(row or []) >= 3 and str(row[0] or "").strip().casefold() == "дата" and str(row[1] or "").strip().casefold() == "описание":
            header_row = i; break
    if not (opening_row and sums_row and expense_row and income_row and closing_row and header_row):
        return rows
    max_cols = max((len(r or []) for r in rows), default=3)
    data_start = opening_row + 2
    data_end = max(data_start, sums_row - 2)
    # Income total plus every expense article total.
    for col in range(3, max_cols + 1):
        letter = _v167_col(col)
        old = rows[sums_row-1][col-1] if col-1 < len(rows[sums_row-1]) else 0
        while len(rows[sums_row-1]) < max_cols:
            rows[sums_row-1].append("")
        rows[sums_row-1][col-1] = _v167_formula(f"SUM({letter}{data_start}:{letter}{data_end})", old)
    last_letter = _v167_col(max_cols)
    rows[expense_row-1][2] = _v167_formula(f"SUM(D{sums_row}:{last_letter}{sums_row})", rows[expense_row-1][2])
    rows[income_row-1][2] = _v167_formula(f"C{sums_row}", rows[income_row-1][2])
    rows[closing_row-1][2] = _v167_formula(f"C{opening_row}+C{income_row}-C{expense_row}", rows[closing_row-1][2])
    if reserve_row and turnover_row:
        rows[turnover_row-1][2] = _v167_formula(f"C{closing_row}-C{reserve_row}", rows[turnover_row-1][2])
    if str(currency).lower() == "ars":
        products_row = _v167_find_label_last(rows, "Продукты", 1)
        metric_row = _v167_find_label(rows, "Расход еды на человека в сутки", 1)
        product_col = 0
        header = list(rows[header_row-1] or [])
        for idx, value in enumerate(header, start=1):
            if "продукт" in str(value or "").casefold():
                product_col = idx; break
        if products_row and product_col >= 4:
            rows[products_row-1][2] = _v167_formula(f"{_v167_col(product_col)}{sums_row}", rows[products_row-1][2])
        if products_row and metric_row:
            try:
                start_key, end_key = _v151_context_bounds(int(chat_id))
                records = _v151_records_in_context(int(chat_id), "ars", _v151_context())
                _products, _metric, days, rate = _v151_food_metric(int(chat_id), records, start_key, end_key)
                if float(rate or 0) > 0:
                    rows[metric_row-1][2] = _v167_formula(
                        f"C{products_row}/({max(1,int(days))}*5*{float(rate):g})", rows[metric_row-1][2]
                    )
            except Exception:
                pass
    return rows


def _v151_simple_table(chat_id: int, currency: str, compact: bool = False):
    if not callable(_V167_BASE_V151_SIMPLE_TABLE):
        return [], {}
    rows, annotations = _V167_BASE_V151_SIMPLE_TABLE(chat_id, currency, compact=compact)
    return _v167_formulaize_simple(rows, bool(compact), int(chat_id), str(currency)), annotations


def _v151_category_table(chat_id: int, currency: str):
    if not callable(_V167_BASE_V151_CATEGORY_TABLE):
        return []
    rows = _V167_BASE_V151_CATEGORY_TABLE(chat_id, currency)
    return _v167_formulaize_category(rows, int(chat_id), str(currency))


# ---------------------------------------------------------------------------
# 4) XLSX package formatting shared by ALL local Excel writers:
# thousands separator, wrap/contain text, thin borders, wider Description.
# ---------------------------------------------------------------------------
def _v167_patch_xlsx_package(path: str) -> None:
    if not path or not _v167_os.path.exists(path):
        return
    tmp = path + ".v167.tmp"
    ns = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
    _v167_ET.register_namespace("", ns)
    with _v167_zipfile.ZipFile(path, "r") as zin, _v167_zipfile.ZipFile(tmp, "w", _v167_zipfile.ZIP_DEFLATED) as zout:
        for item in zin.infolist():
            raw = zin.read(item.filename)
            if item.filename == "xl/styles.xml":
                try:
                    root = _v167_ET.fromstring(raw)
                    numfmts = root.find(f"{{{ns}}}numFmts")
                    if numfmts is None:
                        numfmts = _v167_ET.Element(f"{{{ns}}}numFmts", {"count": "1"})
                        insert_at = 0
                        root.insert(insert_at, numfmts)
                    existing = None
                    for nf in list(numfmts):
                        if nf.attrib.get("formatCode") in {"#,##0", "#,###"}:
                            existing = nf; break
                    if existing is None:
                        used = {int(x.attrib.get("numFmtId", "0") or 0) for x in list(numfmts)}
                        fmt_id = next((n for n in range(164, 300) if n not in used), 164)
                        _v167_ET.SubElement(numfmts, f"{{{ns}}}numFmt", {"numFmtId": str(fmt_id), "formatCode": "#,##0"})
                    else:
                        fmt_id = int(existing.attrib.get("numFmtId", "164") or 164)
                    numfmts.set("count", str(len(list(numfmts))))
                    borders = root.find(f"{{{ns}}}borders")
                    thin_id = 0
                    if borders is not None:
                        thin_id = len(list(borders))
                        border = _v167_ET.SubElement(borders, f"{{{ns}}}border")
                        for side in ("left", "right", "top", "bottom"):
                            _v167_ET.SubElement(border, f"{{{ns}}}{side}", {"style": "thin"})
                        _v167_ET.SubElement(border, f"{{{ns}}}diagonal")
                        borders.set("count", str(len(list(borders))))
                    cell_xfs = root.find(f"{{{ns}}}cellXfs")
                    if cell_xfs is not None:
                        for xf in list(cell_xfs):
                            xf.set("numFmtId", str(fmt_id)); xf.set("applyNumberFormat", "1")
                            if borders is not None:
                                xf.set("borderId", str(thin_id)); xf.set("applyBorder", "1")
                            align = xf.find(f"{{{ns}}}alignment")
                            if align is None:
                                align = _v167_ET.SubElement(xf, f"{{{ns}}}alignment")
                            align.set("wrapText", "1"); align.set("vertical", "top")
                            xf.set("applyAlignment", "1")
                    raw = _v167_ET.tostring(root, encoding="utf-8", xml_declaration=True)
                except Exception:
                    pass
            elif item.filename == "xl/worksheets/sheet1.xml":
                try:
                    root = _v167_ET.fromstring(raw)
                    cols = root.find(f"{{{ns}}}cols")
                    if cols is not None:
                        for col in list(cols):
                            lo = int(col.attrib.get("min", "0") or 0); hi = int(col.attrib.get("max", "0") or 0)
                            if lo <= 2 <= hi:
                                col.set("width", "42"); col.set("customWidth", "1")
                    raw = _v167_ET.tostring(root, encoding="utf-8", xml_declaration=True)
                except Exception:
                    pass
            zout.writestr(item, raw)
    _v167_os.replace(tmp, path)



def _v167_formulaize_four_week_rows(rows: list[list]) -> list[list]:
    """Restore formulas in the legacy 4-week Thursday-Wednesday workbook too."""
    out = _v167_copy.deepcopy(rows or [])
    i = 0
    while i < len(out):
        row = list(out[i] or [])
        if str(row[0] if row else "").strip().casefold() != "неделя":
            i += 1; continue
        header_idx = i + 1
        opening_idx = i + 2
        if opening_idx >= len(out): break
        total_idx = 0
        j = opening_idx + 1
        while j < len(out):
            first = str((out[j] or [""])[0] if (out[j] or []) else "").strip().casefold()
            if first == "итог:": total_idx = j; break
            if first == "неделя": break
            j += 1
        if not total_idx:
            i += 1; continue
        header = list(out[header_idx] or [])
        max_cols = max(2, len(header))
        data_start = opening_idx + 2  # Excel row after opening row
        data_end = max(data_start, total_idx)  # Excel row immediately before total row
        total_excel = total_idx + 1
        # B = income, D+ = expense article columns.
        while len(out[total_idx]) < max_cols: out[total_idx].append("")
        out[total_idx][1] = _v167_formula(f"SUM(B{data_start}:B{data_end})", out[total_idx][1])
        for col in range(4, max_cols + 1):
            letter = _v167_col(col)
            out[total_idx][col-1] = _v167_formula(f"SUM({letter}{data_start}:{letter}{data_end})", out[total_idx][col-1])
        expense_idx = total_idx + 1
        closing_idx = total_idx + 2
        reserve_idx = total_idx + 3
        turnover_idx = total_idx + 4
        if expense_idx < len(out):
            while len(out[expense_idx]) < 2: out[expense_idx].append("")
            out[expense_idx][1] = _v167_formula(f"SUM(D{total_excel}:{_v167_col(max_cols)}{total_excel})", out[expense_idx][1])
        if closing_idx < len(out):
            while len(out[closing_idx]) < 2: out[closing_idx].append("")
            out[closing_idx][1] = _v167_formula(f"B{opening_idx+1}+B{total_excel}-B{expense_idx+1}", out[closing_idx][1])
        if turnover_idx < len(out) and reserve_idx < len(out):
            while len(out[turnover_idx]) < 2: out[turnover_idx].append("")
            out[turnover_idx][1] = _v167_formula(f"B{closing_idx+1}-B{reserve_idx+1}", out[turnover_idx][1])
        # Food metric: preserve the existing denominator, but reference the Products total formula.
        product_col = 0
        for col_idx, name in enumerate(header, start=1):
            if "продукт" in str(name or "").casefold(): product_col = col_idx; break
        next_week = len(out)
        k = turnover_idx + 1
        while k < len(out):
            first = str((out[k] or [""])[0] if (out[k] or []) else "").strip().casefold()
            if first == "неделя": next_week = k; break
            if first == "расход еды на человека в сутки" and product_col >= 4:
                cached_metric = _v167_cached((out[k] or ["",0])[1] if len(out[k] or []) > 1 else 0)
                cached_product = _v167_cached(out[total_idx][product_col-1])
                if cached_metric > 0 and cached_product > 0:
                    denom = cached_product / cached_metric
                    out[k][1] = _v167_formula(f"{_v167_col(product_col)}{total_excel}/{denom:.12g}", cached_metric)
                break
            k += 1
        i = next_week if next_week > i else i + 1
    return out

def _write_simple_xlsx(path: str, rows: list[list], sheet_name: str = "Данные") -> None:
    if not callable(_V167_BASE_WRITE_SIMPLE):
        raise RuntimeError("XLSX writer is unavailable")
    _V167_BASE_WRITE_SIMPLE(path, rows, sheet_name=sheet_name)
    _v167_patch_xlsx_package(path)


def _write_tabl_lsx_xlsx(path: str, rows: list[list], styles: list[list], sheet_name: str = "4 недели", comments: dict | None = None, freeze_rows: int = 3, widths: list[float] | None = None, annotation_mode: str | None = "notes") -> None:
    if not callable(_V167_BASE_WRITE_TABL):
        raise RuntimeError("Styled XLSX writer is unavailable")
    # Preserve the reference workbook's wide description column rather than the older 34-char cap.
    widths2 = list(widths or [])
    if len(widths2) >= 2:
        widths2[1] = max(42, float(widths2[1] or 0))
    rows2 = _v167_formulaize_four_week_rows(rows) if str(sheet_name or "").strip().casefold() == "4 недели" else rows
    _V167_BASE_WRITE_TABL(path, rows2, styles, sheet_name=sheet_name, comments=comments, freeze_rows=freeze_rows, widths=widths2 or widths, annotation_mode=annotation_mode)
    _v167_patch_xlsx_package(path)


# ---------------------------------------------------------------------------
# 5) F47: visible Thu-Wed label + Google rolling tab controls.
# ---------------------------------------------------------------------------
def _v167_google_schedule_cfg(target_chat_id: int, create: bool = True) -> dict:
    store = get_chat_store(int(target_chat_id))
    cfg = store.get("google_thuwed_v167")
    if not isinstance(cfg, dict):
        if not create:
            return {}
        cfg = {}
        store["google_thuwed_v167"] = cfg
    cfg.setdefault("enabled", True)
    cfg.setdefault("time", "05:01")
    cfg.setdefault("last_run_key", "")
    cfg.setdefault("last_ok_at", "")
    cfg.setdefault("last_error", "")
    cfg.setdefault("schema", 1)
    return cfg


def _v167_persist_schedule(target_chat_id: int):
    try: save_chat_json(int(target_chat_id))
    except Exception: pass
    try: schedule_config_backup_for_chats(int(target_chat_id), delay=0.3)
    except Exception: pass


def _add_export_period_rows(kb, day_key: str, prefix: str, owner_day_key: str | None = None, target_chat_id: int | None = None):
    periods = [
        ("📅 День", "day"), ("🗓 Неделя", "week"), ("📆 Месяц", "month"),
        ("📊 Чт–Ср", "wedthu"), ("📂 Всё время", "all"),
    ]
    scope = "fv" if prefix == "fv" else "d"
    target = int(target_chat_id or (OWNER_ID or 0))
    owner_day = str(owner_day_key or day_key)
    for label, mode in periods:
        if prefix == "fv":
            csv_cb = f"fv:{target}:{day_key}:csv_{mode}:{owner_day}"
        else:
            csv_action = "csv_all_real" if mode == "all" else f"csv_{mode}"
            csv_cb = f"d:{day_key}:{csv_action}"
        xlsx_cb = export_callback(f"exp_style_period:{scope}:{target if prefix == 'fv' else 0}:{mode}:xlsx:{day_key}:{owner_day}")
        xlsxstat_cb = export_callback(f"exp_style_period:{scope}:{target if prefix == 'fv' else 0}:{mode}:xlsxstat:{day_key}:{owner_day}")
        kb.row(IB(label, callback_data="none"), IB("CSV", callback_data=csv_cb), IB("Excel", callback_data=xlsx_cb), IB("Excel статьи", callback_data=xlsxstat_cb))
    if target:
        cfg = _v167_google_schedule_cfg(target)
        enabled = bool(cfg.get("enabled", True)); selected = str(cfg.get("time") or "05:01")
        kb.row(
            IB(("✅ " if enabled else "❌ ") + "Google Чт–Ср авто", callback_data=f"v167:gtoggle:{target}"),
            IB(("✅ " if selected == "00:01" else "") + "00:01", callback_data=f"v167:gtime:{target}:0001"),
            IB(("✅ " if selected == "05:01" else "") + "05:01", callback_data=f"v167:gtime:{target}:0501"),
        )
        kb.row(IB("☁️ Обновить лист Чт–Ср сейчас", callback_data=f"v167:gnow:{target}"))


# ---------------------------------------------------------------------------
# 6) Google Sheets rolling named Thu-Wed tab. Uses tenant Google context.
# ---------------------------------------------------------------------------
def _v167_google_color_format(row, r_idx: int, c_idx: int, max_cols: int, layout: str, annotations: dict):
    value = row[c_idx-1] if c_idx-1 < len(row) else ""
    row_is_blank = not any(_excel_nonempty(v) for v in row)
    first = str(row[0] if row else "").strip().casefold()
    second = str(row[1] if len(row) > 1 else "").strip().casefold()
    fmt = {
        "verticalAlignment": "TOP",
        "wrapStrategy": "CLIP" if c_idx == 2 else "WRAP",
        "borders": {
            side: {"style": "SOLID", "color": {"red": 0.65, "green": 0.65, "blue": 0.65}}
            for side in ("top", "bottom", "left", "right")
        },
    }
    if isinstance(value, (int, float)) or (isinstance(value, dict) and value.get("formula")):
        fmt["numberFormat"] = {"type": "NUMBER", "pattern": "#,##0"}
    if first == "ars":
        fmt.update({"textFormat": {"bold": True}, "backgroundColor": {"red": 0.78, "green": 0.94, "blue": 0.81}})
    elif first == "usd":
        fmt.update({"textFormat": {"bold": True}, "backgroundColor": {"red": 0.72, "green": 0.86, "blue": 1.0}})
    elif first in {"дата", "date"}:
        fmt.update({"textFormat": {"bold": True}, "backgroundColor": _google_category_fill(c_idx - 1)})
    elif row_is_blank:
        fmt["backgroundColor"] = {"red": 1.0, "green": 0.60, "blue": 0.0}
    elif first in {"расход", "сумма по статьям"} or second in {"расход", "сумма по статьям"}:
        fmt.update({"textFormat": {"bold": True}, "backgroundColor": {"red": 1.0, "green": 0.55, "blue": 0.55}})
    elif first in {"приход", "приход за период"} or second in {"приход", "приход за период"}:
        fmt.update({"textFormat": {"bold": True}, "backgroundColor": {"red": 0.55, "green": 0.78, "blue": 1.0}})
    elif first in {"остаток с прошлого раза", "остаток на руках", "гомонковые", "остаток в обороте"} or second in {"остаток с прошлого раза", "остаток на руках", "гомонковые", "остаток в обороте"}:
        fmt.update({"textFormat": {"bold": True}, "backgroundColor": {"red": 0.55, "green": 0.85, "blue": 0.55}})
    elif first == "расход еды на человека в сутки" or second == "расход еды на человека в сутки":
        fmt.update({"textFormat": {"bold": True}, "backgroundColor": {"red": 0.74, "green": 0.82, "blue": 1.0}})
    elif layout == "category" and c_idx >= 4 and _excel_nonempty(value):
        fmt["backgroundColor"] = _google_category_fill(c_idx - 1)
    return fmt


def _v167_google_upsert_named_tab(tab_title: str, rows: list[list], target_chat_id: int, layout: str = "category", annotations_override: dict | None = None) -> str:
    target_chat_id = int(target_chat_id)
    tid = _v149_tenant_id(None, target_chat_id) if callable(globals().get("_v149_tenant_id")) else tenant_id_for_chat(target_chat_id, create=False)
    if callable(globals().get("_v149_chat_belongs_to_tenant")) and not _v149_chat_belongs_to_tenant(target_chat_id, tid):
        raise RuntimeError("Google export blocked: target chat is not connected to this space")
    cfg = tenant_google_config(tid)
    if not bool((cfg.get("export_settings") or {}).get("sheet_enabled", True)):
        raise RuntimeError("Выгрузка в Google Sheets выключена для этого пространства")
    with tenant_google_context(tid):
        token = _google_access_token(); info = _google_service_account_info(); spreadsheet_id = _google_spreadsheet_id()
        service_email = str(info.get("client_email") or "")
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        meta = _google_request_guarded("v167_metadata", requests.get, f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}", headers=headers, params={"fields": "spreadsheetId,properties.title,sheets.properties(sheetId,title,gridProperties)"}, timeout=45, attempts=2)
        if meta.status_code >= 300:
            if meta.status_code in (401,403):
                raise RuntimeError(f"Google Sheets access denied. Добавьте {service_email} как Редактор.")
            raise RuntimeError(f"Google Sheets metadata {meta.status_code}: {meta.text[:500]}")
        payload = meta.json(); sheet_id = None
        for sh in payload.get("sheets") or []:
            props = sh.get("properties") or {}
            if str(props.get("title") or "") == tab_title:
                sheet_id = int(props.get("sheetId")); break
        max_cols = max((len(r or []) for r in rows), default=1); row_count = max(100, len(rows)+20); col_count=max(26,max_cols+3)
        if sheet_id is None:
            add = _google_request_guarded("v167_add_sheet", requests.post, f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}:batchUpdate", headers=headers, json={"requests":[{"addSheet":{"properties":{"title":tab_title,"gridProperties":{"rowCount":row_count,"columnCount":col_count,"frozenRowCount":2}}}}]}, timeout=60, attempts=1)
            if add.status_code >= 300:
                raise RuntimeError(f"Google Sheets add tab {add.status_code}: {add.text[:500]}")
            sheet_id = int(add.json()["replies"][0]["addSheet"]["properties"]["sheetId"])
        annotations = dict(annotations_override or {})
        if not annotations and layout == "category":
            try:
                _styles, annotations, _freeze, _widths = _modern_category_excel_styles_comments(rows)
            except Exception:
                annotations = {}
        cell_rows=[]
        for r_idx,row0 in enumerate(rows,start=1):
            row=list(row0 or []); vals=[]
            for c_idx in range(1,max_cols+1):
                value=row[c_idx-1] if c_idx-1<len(row) else ""
                cell={"userEnteredValue":_google_cell_value(value),"userEnteredFormat":_v167_google_color_format(row,r_idx,c_idx,max_cols,layout,annotations)}
                note=str(annotations.get((r_idx,c_idx)) or "").strip()
                if note: cell["note"]=note
                vals.append(cell)
            cell_rows.append({"values":vals})
        req=[
            {"updateCells":{"range":{"sheetId":sheet_id},"fields":"userEnteredValue,note,userEnteredFormat"}},
            {"updateCells":{"range":{"sheetId":sheet_id,"startRowIndex":0,"startColumnIndex":0},"rows":cell_rows,"fields":"userEnteredValue,note,userEnteredFormat"}},
            {"updateDimensionProperties":{"range":{"sheetId":sheet_id,"dimension":"COLUMNS","startIndex":0,"endIndex":1},"properties":{"pixelSize":95},"fields":"pixelSize"}},
        ]
        if max_cols >= 2:
            req.append({"updateDimensionProperties":{"range":{"sheetId":sheet_id,"dimension":"COLUMNS","startIndex":1,"endIndex":2},"properties":{"pixelSize":320},"fields":"pixelSize"}})
        if max_cols >= 3:
            req.append({"updateDimensionProperties":{"range":{"sheetId":sheet_id,"dimension":"COLUMNS","startIndex":2,"endIndex":max_cols},"properties":{"pixelSize":115},"fields":"pixelSize"}})
        upd=_google_request_guarded("v167_update_named", requests.post, f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}:batchUpdate", headers=headers, json={"requests":req}, timeout=90, attempts=1)
        if upd.status_code >= 300:
            raise RuntimeError(f"Google Sheets update {upd.status_code}: {upd.text[:500]}")
        url=f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit#gid={sheet_id}"
    try:
        tenant_google_history(tid,"thuwed_auto_update",tab_title,ok=True,chat_id=target_chat_id,url=url)
        tenant_google_persist(tid,"tenant_google_update")
    except Exception:
        pass
    return url


def _v167_google_update_target(target_chat_id: int, reason: str = "schedule"):
    target_chat_id=int(target_chat_id)
    with _V167_GOOGLE_LOCK:
        if target_chat_id in _V167_GOOGLE_RUNNING:
            return False
        _V167_GOOGLE_RUNNING.add(target_chat_id)
    try:
        day=today_key(); start_key,end_key=_v167_thuwed_bounds(day); tab=_v167_period_title(start_key,end_key)
        rows=build_exact_category_stats_xlsx_rows(target_chat_id,start_key,0,end_key,0)
        url=_v167_google_upsert_named_tab(tab,rows,target_chat_id,layout="category")
        cfg=_v167_google_schedule_cfg(target_chat_id); cfg["last_ok_at"]=now_local().isoformat(timespec="seconds"); cfg["last_error"]=""; cfg["last_period"]=tab
        _v167_persist_schedule(target_chat_id)
        try: bot_journal("google_thuwed_updated",target_chat_id,f"tab={tab}; reason={reason}; url={url[:120]}")
        except Exception: pass
        return True
    except Exception as exc:
        cfg=_v167_google_schedule_cfg(target_chat_id); cfg["last_error"]=str(exc)[:500]
        _v167_persist_schedule(target_chat_id)
        try: log_error(f"v167 google Thu-Wed update chat={target_chat_id}: {exc}")
        except Exception: pass
        return False
    finally:
        with _V167_GOOGLE_LOCK: _V167_GOOGLE_RUNNING.discard(target_chat_id)


def _v167_known_chat_ids():
    out=set()
    try:
        if OWNER_ID: out.add(int(OWNER_ID))
    except Exception: pass
    try:
        for key in (data.get("chats",{}) or {}).keys():
            try: out.add(int(key))
            except Exception: pass
    except Exception: pass
    return sorted(out)


def _v167_google_scheduler_loop():
    while True:
        try:
            if callable(globals().get("runtime_is_shutting_down")) and runtime_is_shutting_down():
                return
            if callable(globals().get("runtime_is_ready")) and not runtime_is_ready():
                _v167_time.sleep(5); continue
            now=now_local(); hhmm=now.strftime("%H:%M"); date_key=now.strftime("%Y-%m-%d")
            for cid in _v167_known_chat_ids():
                cfg=_v167_google_schedule_cfg(cid,create=False)
                if not cfg or not bool(cfg.get("enabled",True)):
                    continue
                selected=str(cfg.get("time") or "05:01")
                if selected not in {"00:01","05:01"} or hhmm < selected:
                    continue
                # Catch-up is intentional: if Render slept/restarted at the selected minute,
                # the first READY after that time performs today's update exactly once.
                run_key=f"{date_key}@{selected}"
                if str(cfg.get("last_run_key") or "") == run_key:
                    continue
                cfg["last_run_key"]=run_key; _v167_persist_schedule(cid)
                _v167_threading.Thread(target=_v167_google_update_target,args=(cid,"daily"),daemon=True,name=f"v167-gsheet-{cid}").start()
        except Exception as exc:
            try: log_error(f"v167 google scheduler: {exc}")
            except Exception: pass
        _v167_time.sleep(20)


def _v167_start_google_scheduler():
    global _V167_GOOGLE_SCHEDULER_STARTED
    if _V167_GOOGLE_SCHEDULER_STARTED: return
    _V167_GOOGLE_SCHEDULER_STARTED=True
    _v167_threading.Thread(target=_v167_google_scheduler_loop,daemon=True,name="v167-google-thuwed").start()


def _v167_schedule_callback_filter(call):
    try: return str(getattr(call,"data","") or "").startswith("v167:g")
    except Exception: return False


def _v167_schedule_callback(call):
    raw=str(getattr(call,"data","") or ""); parts=raw.split(":")
    try:
        target=int(parts[2]) if len(parts)>2 else int(OWNER_ID or 0)
        uid=int(getattr(getattr(call,"from_user",None),"id",0) or 0)
        if not (tenant_is_platform_owner_user(uid) or tenant_can_manage(uid, chat_id=target)):
            bot.answer_callback_query(call.id,"Недостаточно прав",show_alert=True); return
        cfg=_v167_google_schedule_cfg(target)
        if parts[1]=="gtoggle":
            cfg["enabled"]=not bool(cfg.get("enabled",True)); msg="Автообновление включено" if cfg["enabled"] else "Автообновление выключено"
        elif parts[1]=="gtime":
            code=str(parts[3] if len(parts)>3 else "0501"); cfg["time"]="00:01" if code=="0001" else "05:01"; cfg["enabled"]=True; msg=f"Чт–Ср: ежедневно в {cfg['time']}"
        elif parts[1]=="gnow":
            msg="Обновляю текущий лист Чт–Ср"; _v167_threading.Thread(target=_v167_google_update_target,args=(target,"manual"),daemon=True,name=f"v167-gsheet-now-{target}").start()
        else:
            return
        _v167_persist_schedule(target)
        try: bot.answer_callback_query(call.id,msg)
        except Exception: pass
        # Update only button labels in the same F47 message; no heavy work on callback line.
        try:
            kb=_v167_copy.deepcopy(getattr(getattr(call,"message",None),"reply_markup",None))
            for row in getattr(kb,"keyboard",[]) or []:
                for btn in row:
                    cb=str(getattr(btn,"callback_data","") or "")
                    if cb==f"v167:gtoggle:{target}": btn.text=("✅ " if cfg.get("enabled") else "❌ ")+"Google Чт–Ср авто"
                    elif cb==f"v167:gtime:{target}:0001": btn.text=("✅ " if cfg.get("time")=="00:01" else "")+"00:01"
                    elif cb==f"v167:gtime:{target}:0501": btn.text=("✅ " if cfg.get("time")=="05:01" else "")+"05:01"
            bot.edit_message_reply_markup(call.message.chat.id,call.message.message_id,reply_markup=kb)
        except Exception: pass
    except Exception as exc:
        try: log_error(f"v167 schedule callback {raw}: {exc}")
        except Exception: pass
        try: bot.answer_callback_query(call.id,"Ошибка настройки Google",show_alert=True)
        except Exception: pass


def _v167_install_schedule_callback():
    try:
        bot.callback_query_handler(func=_v167_schedule_callback_filter)(_v167_schedule_callback)
        handlers=getattr(bot,"callback_query_handlers",None)
        if isinstance(handlers,list) and handlers:
            row=handlers.pop(); handlers.insert(0,row)
        return 1
    except Exception:
        return 0


# ---------------------------------------------------------------------------
# 7) TZ lifecycle. A new release starts with an empty active TZ list without
# deleting history: older rows are archived, not falsely marked "fixed".
# ---------------------------------------------------------------------------
def _v168_mark_resolved_tz_rows():
    """Mark only TZ items explicitly fixed by this release; other old rows are merely archived."""
    changed = False
    try:
        _catalog, rows = _v160_annotation_roots()
        for row in rows or []:
            if not isinstance(row, dict) or str(row.get("status") or "open").lower() != "open":
                continue
            marker = str(row.get("marker") or "").upper()
            body = str(row.get("text") or "").casefold()
            resolved = (
                marker == "Ф2" and "переключ" in body and "владель" in body
            ) or (
                marker == "Ф91" and "быстро" in body and "обнов" in body and ("ввода" in body or "пересыл" in body or "редакт" in body)
            )
            if resolved:
                row["status"] = "fixed"
                row["fixed_by_version"] = VERSION
                row["fixed_at"] = now_local().isoformat(timespec="seconds")
                changed = True
        return changed
    except Exception:
        return False


def _v167_archive_old_tz_once(force: bool = False):
    global _V167_TZ_ARCHIVE_VERSION
    current_version = str(globals().get("VERSION") or VERSION)
    if not force and _V167_TZ_ARCHIVE_VERSION == current_version:
        return
    _V167_TZ_ARCHIVE_VERSION = current_version
    try:
        _catalog, rows=_v160_annotation_roots(); changed=False
        for row in rows:
            if not isinstance(row,dict): continue
            row_version=str(row.get("version") or "")
            status=str(row.get("status") or "").lower()
            if row_version != current_version and status not in {"archived","fixed"}:
                row["status"]="archived"; row["archived_by_version"]=current_version; row["archived_at"]=now_local().isoformat(timespec="seconds"); changed=True
            elif not row_version:
                row["version"]="legacy"; changed=True
        if changed:
            try: _v160_persist_annotations(int(OWNER_ID or 0))
            except Exception: pass
    except Exception:
        pass


def _v160_save_tz(chat_id: int, user_id: int, marker: str, body: str, source: dict) -> dict:
    if not callable(_V167_BASE_SAVE_TZ):
        raise RuntimeError("TZ storage unavailable")
    row=_V167_BASE_SAVE_TZ(chat_id,user_id,marker,body,source)
    try:
        row["version"]=VERSION; row["status"]="open"; row["archived_by_version"]=""
        _v160_persist_annotations(chat_id)
    except Exception: pass
    return row


def _v167_tz_export(kind: str):
    catalog,rows=_v160_annotation_roots(); now_s=now_local().strftime("%Y-%m-%d %H:%M:%S")
    archive=(kind=="tz_archive")
    selected=[]
    for row in list(rows or []):
        if not isinstance(row,dict): continue
        status=str(row.get("status") or "open").lower(); ver=str(row.get("version") or "legacy")
        if archive:
            if status in {"archived","fixed"} or ver != VERSION: selected.append(row)
        else:
            if status=="open" and ver==VERSION: selected.append(row)
    title="АРХИВ ТЗ ПО ОКНАМ" if archive else "ТЗ ПО ОКНАМ — ТЕКУЩАЯ ВЕРСИЯ"
    lines=[title,f"Версия выгрузки: {VERSION}",f"Создано: {now_s}",f"Записей: {len(selected)}",""]
    for row in selected:
        src=dict(row.get("source") or {}); marker=str(row.get("marker") or "")
        lines.extend([
            f"[{row.get('at')}] {marker} — {row.get('window_name') or (catalog.get(marker) or {}).get('name') or 'без имени'}",
            f"Статус: {row.get('status') or 'open'}; версия: {row.get('version') or 'legacy'}",
            f"Источник: chat={src.get('chat_id') or '—'} msg={src.get('message_id') or '—'} callback={src.get('callback') or '—'}",
            str(row.get("text") or ""),"","---","",
        ])
    return ("Архив_ТЗ_окон" if archive else "ТЗ_окон_текущая_версия"),"\n".join(lines).rstrip()+"\n"


def _v160_export_text(kind: str):
    if kind in {"tz","tz_archive"}: return _v167_tz_export(kind)
    if callable(_V167_BASE_EXPORT_TZ): return _V167_BASE_EXPORT_TZ(kind)
    return "export",""


def _v167_send_tz_archive(chat_id: int):
    _file_job_progress("формирую архив ТЗ",force=True)
    base,content=_v160_export_text("tz_archive"); folder=_v167_tempfile.mkdtemp(prefix="v167_tz_"); path=_v167_os.path.join(folder,f"{base}_{now_local().strftime('%Y_%m_%d_%H%M%S')}.txt")
    try:
        with open(path,"w",encoding="utf-8") as fh: fh.write(content)
        with open(path,"rb") as fh: bot.send_document(int(chat_id),fh,caption=f"🗃 Архив ТЗ окон · {VERSION}")
        return True
    finally:
        try:
            import shutil as _v167_shutil; _v167_shutil.rmtree(folder,ignore_errors=True)
        except Exception: pass


def _v160_handle_special_callback(call, resolved: str) -> bool:
    if str(resolved)=="v167:export_tz_archive":
        uid=int(getattr(getattr(call,"from_user",None),"id",0) or 0)
        if not _v160_can_annotate(uid):
            try: bot.answer_callback_query(call.id,"Только для владельца платформы",show_alert=True)
            except Exception: pass
            return True
        cid=int(call.message.chat.id); ok,reason=submit_interactive_file_job(cid,"window_tz_archive","Архив ТЗ окон",_v167_send_tz_archive,cid)
        try: bot.answer_callback_query(call.id,"Формирую архив" if ok else "Файл уже формируется")
        except Exception: pass
        if not ok:
            try: send_and_auto_delete(cid,f"⏳ {reason or 'Сейчас уже формируется другой файл.'}",10)
            except Exception: pass
        return True
    if callable(_V167_BASE_SPECIAL_CALLBACK): return bool(_V167_BASE_SPECIAL_CALLBACK(call,resolved))
    return False


def _v160_augment_markup(reply_markup, text: str):
    kb=_V167_BASE_AUGMENT_MARKUP(reply_markup,text) if callable(_V167_BASE_AUGMENT_MARKUP) else reply_markup
    try:
        if _v160_marker_from_text(text)=="Ф89" and isinstance(kb,types.InlineKeyboardMarkup):
            callbacks=_v160_markup_callbacks(kb)
            if "v167:export_tz_archive" not in callbacks:
                kb.row(IB("🗃 Скачать архив ТЗ",callback_data="v167:export_tz_archive"))
    except Exception: pass
    return kb


# Marker constants for the two new logical actions.
try:
    WINDOW_MARKER_CONSTANTS.update({
        "v167:gtoggle:*":"Ф47", "v167:gtime:*":"Ф47", "v167:gnow:*":"Ф47",
        "v167:export_tz_archive":"Ф239",
    })
except Exception:
    pass

# Current owner requested the rolling Google tab as an active feature. Default to 05:01;
# F47 can switch it to 00:01 or disable it without blocking UI.
try:
    if OWNER_ID:
        _v167_google_schedule_cfg(int(OWNER_ID),create=True)
except Exception:
    pass

def _v168_migrate_all_record_uids_once():
    gs = data.setdefault("_global_settings", {})
    if str(gs.get("record_uid_schema") or "") == "v168":
        return 0
    changed = 0
    for cid_s in list((data.get("chats") or {}).keys()):
        try: changed += int(migrate_finance_record_uids(int(cid_s)) or 0)
        except Exception as exc:
            try: log_error(f"v168 startup UID migration {cid_s}: {exc}")
            except Exception: pass
    gs["record_uid_schema"] = "v168"
    if changed:
        # UID backfill changes hashes for old rows. Do not turn the migration itself into one giant delta.
        # The local SQLite is already updated; reset the delta baseline and request a normal full snapshot.
        try: initialize_delta_baseline(data)
        except Exception: pass
        try: _mark_global_snapshot_pending()
        except Exception: pass
    try:
        scheduler = globals().get("V166_CONFIG_IO_SCHEDULER")
        if scheduler is not None:
            scheduler.schedule("v168-record-uid-schema", 0.1, lambda: save_data(data, root_only=True))
        else:
            save_data(data, root_only=True)
    except Exception: pass
    try: bot_journal("record_uid_migration_v168", int(OWNER_ID or 0), f"changed={changed}")
    except Exception: pass
    return changed


try:
    _V168_PREV_SET_WEBHOOK = globals().get("set_webhook")
    if callable(_V168_PREV_SET_WEBHOOK):
        def set_webhook():
            _v168_migrate_all_record_uids_once()
            return _V168_PREV_SET_WEBHOOK()
except Exception:
    pass


_v167_archive_old_tz_once()
_v167_install_schedule_callback()
_v167_start_google_scheduler()

# Run the TZ rollover again at READY. This makes the rule survive future releases loaded
# after v167: VERSION is resolved dynamically after all modules have executed.
try:
    _V167_BASE_RUNTIME_MARK_READY = globals().get("runtime_mark_ready")
    if callable(_V167_BASE_RUNTIME_MARK_READY):
        def runtime_mark_ready(detail: str = ""):
            result = _V167_BASE_RUNTIME_MARK_READY(detail)
            # Restore happens after module import. Mark the two v167 TZ items implemented in v168 as fixed,
            # archive any other old open rows, and leave the v168 current-TZ export clean.
            try:
                if _v168_mark_resolved_tz_rows():
                    _v160_persist_annotations(int(OWNER_ID or 0))
            except Exception: pass
            try: _v167_archive_old_tz_once(force=True)
            except Exception: pass
            try:
                if OWNER_ID:
                    _v167_google_schedule_cfg(int(OWNER_ID), create=True)
                    _v167_persist_schedule(int(OWNER_ID))
            except Exception: pass
            return result
except Exception:
    pass

# Restore validator compatibility: v167 backups use the same schema as v166.
try:
    _V167_PREV_RESTORE_VALIDATOR=globals().get("_v153_validate_restore_gz")
    if callable(_V167_PREV_RESTORE_VALIDATOR):
        def _v153_validate_restore_gz(gz_path: str):
            try:
                return _V167_PREV_RESTORE_VALIDATOR(gz_path)
            except Exception as exc:
                if "unsupported bot version" not in str(exc):
                    raise
                import gzip as _v167_gzip, shutil as _v167_shutil, sqlite3 as _v167_sqlite3, json as _v167_json
                folder=_v167_tempfile.mkdtemp(prefix="v167_restore_validate_")
                raw=_v167_os.path.join(folder,"restore.sqlite3")
                try:
                    with _v167_gzip.open(gz_path,"rb") as fin, open(raw,"wb") as fout:
                        _v167_shutil.copyfileobj(fin,fout,1024*1024)
                    conn=_v167_sqlite3.connect(raw)
                    try:
                        integrity=str(conn.execute("PRAGMA integrity_check").fetchone()[0])
                        if integrity.lower()!="ok": raise RuntimeError(f"SQLite integrity_check: {integrity}")
                        row=conn.execute("SELECT v FROM meta WHERE kind='v153_export' AND k='manifest'").fetchone()
                        if not row: raise RuntimeError("manifest v153 not found")
                        manifest=_v167_json.loads(row[0])
                    finally:
                        conn.close()
                    if str(manifest.get("kind"))!="telegram_bot_full_state_v153": raise RuntimeError("unknown export kind")
                    if int(manifest.get("schema_version") or 0)!=int(V153_EXPORT_SCHEMA): raise RuntimeError("unsupported export schema")
                    export_version=str(manifest.get("bot_version") or "")
                    if not export_version.startswith((
                        "bot_v153_","bot_v154_","bot_v155_","bot_v156_","bot_v157_","bot_v158_",
                        "bot_v159_","bot_v160_","bot_v161_","bot_v162_","bot_v163_","bot_v164_",
                        "bot_v165_","bot_v166_","bot_v167_","bot_v168_",
                    )): raise RuntimeError(f"unsupported bot version: {export_version or 'missing'}")
                    if _v153_db_logical_checksum(raw)!=str(manifest.get("checksum") or ""): raise RuntimeError("checksum mismatch")
                    return manifest,raw
                except Exception:
                    _v167_shutil.rmtree(folder,ignore_errors=True)
                    raise
except Exception:
    pass

try:
    bot_journal("v168_installed", int(OWNER_ID or 0), "clean-core phase 1: immutable record UID + owner-access circles + fast finance UI")
except Exception:
    pass
# v168_clean_core_record_identity
