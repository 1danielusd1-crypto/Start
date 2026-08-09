# v168_clean_core_record_identity

# Правки 5, 6, 7 выполнены заново поверх v150:
# - Excel/Google: ARS + отдельная USD-таблица на том же листе, резервы,
#   оборот и расход еды на человека в сутки с учетом дней периода;
# - безопасный шаблон гомонковых, который никогда не принимает пример за данные;
# - USD «За месяц» меняет только текст текущего окна и сохраняет клавиатуру.

import copy as _v151_copy
import math as _v151_math
import re as _v151_re
import threading as _v151_threading
from datetime import datetime as _v151_datetime, timedelta as _v151_timedelta

VERSION = "bot_v151_redo_fixes_5_6_7"

_V151_EXPORT_LOCAL = _v151_threading.local()
_V151_MONTH_LOCAL = _v151_threading.local()
_V151_REBALANCE_LOCK = _v151_threading.RLock()

_V151_BASE_SEND_EXPORT = globals().get("send_export_for_chat_to")
_V151_BASE_SEND_EXACT_EXPORT = globals().get("send_exact_range_export")
_V151_BASE_PERIOD_ROWS = globals().get("_period_export_rows")
_V151_BASE_EXACT_ROWS = globals().get("_exact_export_rows")
_V151_BASE_SET_WEBHOOK = globals().get("set_webhook")
_V151_BASE_ADD_RECORD = globals().get("_V150_BASE_ADD_RECORD") or globals().get("add_record_to_chat")
_V151_BASE_ADD_LEDGER_RECORD = globals().get("_V150_BASE_ADD_CURRENCY_RECORD") or globals().get("_add_record_to_currency_ledger")


def _v151_float(value, default=0.0) -> float:
    try:
        number = float(value or 0)
        return number if _v151_math.isfinite(number) else float(default)
    except Exception:
        return float(default)


def _v151_num(value):
    number = _v151_float(value)
    return int(number) if float(number).is_integer() else number


def _v151_now() -> str:
    try:
        return now_local().isoformat(timespec="seconds")
    except Exception:
        return _v151_datetime.now().astimezone().isoformat(timespec="seconds")


def _v151_day_key(rec: dict) -> str:
    try:
        return str(_record_day_key(rec))[:10]
    except Exception:
        return str((rec or {}).get("day_key") or "")[:10]


def _v151_parse_day(value: str):
    raw = str(value or "")[:10]
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d.%m.%y"):
        try:
            return _v151_datetime.strptime(raw, fmt)
        except Exception:
            continue
    return None


def _v151_sync_currency_snapshots(store: dict) -> str:
    try:
        active = _ensure_currency_ledgers(store)
        _snapshot_active_currency_ledger(store, active)
        return active
    except Exception:
        return str((store.setdefault("settings", {})).get("_active_currency_ledger") or "ars").lower()


def _v151_ars_records(chat_id: int) -> list[dict]:
    store = get_chat_store(int(chat_id))
    active = _v151_sync_currency_snapshots(store)
    source = store.get("records", []) if active == "ars" else store.get("ars_records", [])
    rows = []
    for rec in source or []:
        if not isinstance(rec, dict) or bool(rec.get("usd_only", False)):
            continue
        item = dict(rec)
        item["_v151_amount"] = _v151_float(rec.get("amount"))
        item["_v151_note"] = str(rec.get("note") or "")
        item["_v151_currency"] = "ars"
        rows.append(item)
    try:
        return sorted(rows, key=record_sort_key)
    except Exception:
        return rows


def _v151_usd_records(chat_id: int) -> list[dict]:
    """Собирает отдельный USD-контур и старые usd_amount без дублей."""
    store = get_chat_store(int(chat_id))
    active = _v151_sync_currency_snapshots(store)
    independent = store.get("records", []) if active == "usd" else store.get("usd_records", [])
    ars_source = store.get("records", []) if active == "ars" else store.get("ars_records", [])

    rows = []
    seen = set()

    def _key(rec: dict, prefix: str = ""):
        operation_key = str(rec.get("operation_key") or "").strip()
        source_msg_id = int(rec.get("source_msg_id") or 0)
        if operation_key:
            return ("op", operation_key)
        if source_msg_id:
            return ("msg", source_msg_id)
        return (prefix, int(rec.get("id") or 0), str(rec.get("timestamp") or ""), _v151_day_key(rec))

    for rec in independent or []:
        if not isinstance(rec, dict):
            continue
        item = dict(rec)
        item["_v151_amount"] = _v151_float(rec.get("amount"))
        item["_v151_note"] = str(rec.get("note") or rec.get("usd_note") or "")
        item["_v151_currency"] = "usd"
        key = _key(item, "usd")
        seen.add(key)
        rows.append(item)

    for rec in ars_source or []:
        if not isinstance(rec, dict):
            continue
        if rec.get("usd_amount") is None or abs(_v151_float(rec.get("usd_amount"))) <= 1e-12:
            continue
        key = _key(rec, "embedded")
        if key in seen:
            continue
        item = dict(rec)
        item["_v151_amount"] = _v151_float(rec.get("usd_amount"))
        item["_v151_note"] = str(rec.get("usd_note") or rec.get("note") or "")
        item["_v151_currency"] = "usd"
        item["_v151_embedded"] = True
        seen.add(key)
        rows.append(item)

    try:
        return sorted(rows, key=record_sort_key)
    except Exception:
        return rows


def _v151_all_records(chat_id: int, currency: str) -> list[dict]:
    return _v151_usd_records(chat_id) if str(currency).lower() == "usd" else _v151_ars_records(chat_id)


def _v151_context() -> dict:
    return dict(getattr(_V151_EXPORT_LOCAL, "value", None) or {})


def _v151_context_bounds(chat_id: int, ctx: dict | None = None) -> tuple[str, str]:
    ctx = dict(ctx or _v151_context())
    day_key = str(ctx.get("day_key") or today_key())[:10]
    kind = str(ctx.get("kind") or "period")
    if kind == "exact":
        start = str(ctx.get("start_key") or day_key)[:10]
        end = str(ctx.get("end_key") or day_key)[:10]
        return (end, start) if end < start else (start, end)

    mode = str(ctx.get("mode") or "all").replace("csv_", "").replace("xlsx_", "")
    base = _v151_parse_day(day_key) or now_local().replace(hour=0, minute=0, second=0, microsecond=0)
    if mode == "day":
        return day_key, day_key
    if mode == "week":
        return (base - _v151_timedelta(days=6)).strftime("%Y-%m-%d"), day_key
    if mode == "month":
        return base.replace(day=1).strftime("%Y-%m-%d"), day_key
    if mode == "wedthu":
        start = base
        while start.weekday() != 2:
            start -= _v151_timedelta(days=1)
        return start.strftime("%Y-%m-%d"), (start + _v151_timedelta(days=1)).strftime("%Y-%m-%d")

    days = [_v151_day_key(r) for r in (_v151_ars_records(chat_id) + _v151_usd_records(chat_id))]
    days = sorted(x for x in days if _v151_parse_day(x))
    return (days[0], days[-1]) if days else (day_key, day_key)


def _v151_period_days(start_key: str, end_key: str) -> int:
    start = _v151_parse_day(start_key)
    end = _v151_parse_day(end_key)
    if not start or not end:
        return 1
    if end < start:
        start, end = end, start
    return max(1, (end.date() - start.date()).days + 1)


def _v151_records_in_context(chat_id: int, currency: str, ctx: dict | None = None) -> list[dict]:
    ctx = dict(ctx or _v151_context())
    start_key, end_key = _v151_context_bounds(chat_id, ctx)
    start_rid = int(ctx.get("start_rid") or 0)
    end_rid = int(ctx.get("end_rid") or 0)
    exact = str(ctx.get("kind") or "") == "exact"
    out = []
    for rec in _v151_all_records(chat_id, currency):
        day = _v151_day_key(rec)
        if not day or day < start_key or day > end_key:
            continue
        # Exact record IDs are meaningful only in the ARS ledger where the user chose them.
        if exact and str(currency).lower() == "ars":
            rid = int(rec.get("id") or 0)
            if day == start_key and start_rid and rid < start_rid:
                continue
            if day == end_key and end_rid and rid > end_rid:
                continue
        out.append(rec)
    try:
        return sorted(out, key=record_sort_key)
    except Exception:
        return out


def _v151_opening_balance(chat_id: int, currency: str, ctx: dict | None = None) -> float:
    ctx = dict(ctx or _v151_context())
    start_key, _end_key = _v151_context_bounds(chat_id, ctx)
    start_rid = int(ctx.get("start_rid") or 0)
    exact = str(ctx.get("kind") or "") == "exact"
    total = 0.0
    for rec in _v151_all_records(chat_id, currency):
        day = _v151_day_key(rec)
        if day < start_key:
            total += _v151_float(rec.get("_v151_amount"))
            continue
        if day > start_key:
            break
        if exact and str(currency).lower() == "ars" and start_rid:
            if int(rec.get("id") or 0) < start_rid:
                total += _v151_float(rec.get("_v151_amount"))
                continue
        break
    return total


def _v151_usd_rate() -> float:
    try:
        row = usd_rate_cached(force=False) or {}
        return max(0.0, _v151_float(row.get("rate") or row.get("venta") or row.get("sell")))
    except Exception:
        return 0.0


def _v151_product_total(chat_id: int, records: list[dict]) -> float:
    store = get_chat_store(int(chat_id))
    total = 0.0
    for rec in records or []:
        amount = _v151_float(rec.get("_v151_amount"))
        if amount >= 0:
            continue
        note = str(rec.get("_v151_note") or "")
        try:
            category = str(resolve_expense_category(note, store) or "").strip().casefold()
        except Exception:
            category = ""
        if category in {"продукты", "продукт", "еда", "food", "products"}:
            total += abs(amount)
    return total


def _v151_food_metric(chat_id: int, records: list[dict], start_key: str, end_key: str) -> tuple[float, float, int, float]:
    products = _v151_product_total(chat_id, records)
    days = _v151_period_days(start_key, end_key)
    rate = _v151_usd_rate()
    metric = products / 5.0 / rate / days if rate > 0 and days > 0 else 0.0
    return products, metric, days, rate


def _v151_reserve(chat_id: int, currency: str) -> float:
    try:
        return max(0.0, _v151_float(gomonk_total(int(chat_id), currency)))
    except Exception:
        return 0.0


def _v151_table_totals(records: list[dict], opening: float) -> tuple[float, float, float]:
    income = sum(max(0.0, _v151_float(r.get("_v151_amount"))) for r in records or [])
    expense = sum(max(0.0, -_v151_float(r.get("_v151_amount"))) for r in records or [])
    return income, expense, opening + income - expense


def _v151_simple_table(chat_id: int, currency: str, compact: bool = False) -> tuple[list[list], dict[tuple[int, int], str]]:
    ctx = _v151_context()
    start_key, end_key = _v151_context_bounds(chat_id, ctx)
    records = _v151_records_in_context(chat_id, currency, ctx)
    opening = _v151_opening_balance(chat_id, currency, ctx)
    income, expense, closing = _v151_table_totals(records, opening)
    reserve = _v151_reserve(chat_id, currency)
    turnover = closing - reserve
    rows = []
    annotations: dict[tuple[int, int], str] = {}
    title = str(currency).upper()

    if compact:
        rows.extend([[title, "", ""], ["Дата", "Приход", "Расход"], ["Остаток с прошлого раза", _v151_num(opening), ""], []])
        prev_day = None
        for rec in records:
            day = _v151_day_key(rec)
            if prev_day is not None and day != prev_day:
                rows.append([])
            prev_day = day
            amount = _v151_float(rec.get("_v151_amount"))
            income_cell, expense_cell = _xlsx_income_expense_values(amount)
            rows.append([fmt_date_table(day), income_cell, expense_cell])
            note = str(rec.get("_v151_note") or "").strip()
            if note:
                annotations[(len(rows), 2 if income_cell != "" else 3)] = note
        rows.extend([
            [],
            ["Приход за период", _v151_num(income), ""],
            ["Расход за период", "", _v151_num(expense)],
            ["Остаток на руках", _v151_num(closing), ""],
            ["Гомонковые", _v151_num(reserve), ""],
            ["Остаток в обороте", _v151_num(turnover), ""],
        ])
        if currency == "ars":
            products, metric, days, rate = _v151_food_metric(chat_id, records, start_key, end_key)
            rows.extend([
                [],
                ["Продукты", _v151_num(products), ""],
                [],
                ["Расход еды на человека в сутки", metric, ""],
                ["Расчёт", f"{days} дн. · 5 чел. · курс {rate:g}" if rate > 0 else f"{days} дн. · 5 чел. · курс не найден", ""],
            ])
        return rows, annotations

    rows.extend([[title, "", "", ""], ["Дата", "Описание", "Приход", "Расход"], ["", "Остаток с прошлого раза", _v151_num(opening), ""], []])
    prev_day = None
    for rec in records:
        day = _v151_day_key(rec)
        if prev_day is not None and day != prev_day:
            rows.append([])
        prev_day = day
        amount = _v151_float(rec.get("_v151_amount"))
        income_cell, expense_cell = _xlsx_income_expense_values(amount)
        rows.append([fmt_date_table(day), str(rec.get("_v151_note") or ""), income_cell, expense_cell])
    rows.extend([
        [],
        ["", "Приход за период", _v151_num(income), ""],
        ["", "Расход за период", "", _v151_num(expense)],
        ["", "Остаток на руках", _v151_num(closing), ""],
        ["", "Гомонковые", _v151_num(reserve), ""],
        ["", "Остаток в обороте", _v151_num(turnover), ""],
    ])
    if currency == "ars":
        products, metric, days, rate = _v151_food_metric(chat_id, records, start_key, end_key)
        rows.extend([
            [],
            ["", "Продукты", _v151_num(products), ""],
            [],
            ["", "Расход еды на человека в сутки", metric, ""],
            ["", "Расчёт", f"{days} дн. · 5 чел. · курс {rate:g}" if rate > 0 else f"{days} дн. · 5 чел. · курс не найден", ""],
        ])
    return rows, annotations


def _v151_categories(chat_id: int, records: list[dict]) -> list[str]:
    store = get_chat_store(int(chat_id))
    totals = {}
    for rec in records:
        amount = _v151_float(rec.get("_v151_amount"))
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
        totals[str(category or "прочие")] = totals.get(str(category or "прочие"), 0.0) + abs(amount)
    try:
        categories = list(get_ordered_category_names(cats=totals, store=store) or [])
    except Exception:
        categories = list(totals)
    return categories or ["прочие"]


def _v151_category_table(chat_id: int, currency: str) -> list[list]:
    ctx = _v151_context()
    start_key, end_key = _v151_context_bounds(chat_id, ctx)
    records = _v151_records_in_context(chat_id, currency, ctx)
    opening = _v151_opening_balance(chat_id, currency, ctx)
    categories = _v151_categories(chat_id, records)
    clean_categories = [_clean_category_display_name(x) for x in categories]
    rows = [[str(currency).upper()] + [""] * (2 + len(categories))]
    rows.append(["Дата", "Описание", "Приход"] + clean_categories)
    rows.append(["", "Остаток с прошлого раза", _v151_num(opening)] + [""] * len(categories))
    rows.append([])
    store = get_chat_store(int(chat_id))
    cat_totals = {cat: 0.0 for cat in categories}
    income = 0.0
    expense = 0.0
    prev_day = None
    for rec in records:
        day = _v151_day_key(rec)
        if prev_day is not None and day != prev_day:
            rows.append([])
        prev_day = day
        note = str(rec.get("_v151_note") or "")
        amount = _v151_float(rec.get("_v151_amount"))
        row = [fmt_date_table(day), note, ""] + [""] * len(categories)
        if amount >= 0:
            income += amount
            row[2] = _v151_num(amount)
        else:
            value = abs(amount)
            expense += value
            try:
                category = resolve_expense_category(note, store)
                override_slug = str(rec.get("category_override_slug") or "").strip()
                if override_slug:
                    category = get_category_by_slug(override_slug, store) or category
            except Exception:
                category = "прочие"
            if category not in cat_totals:
                category = categories[-1]
            idx = categories.index(category)
            cat_totals[category] += value
            row[3 + idx] = _v151_num(value)
        rows.append(row)

    closing = opening + income - expense
    reserve = _v151_reserve(chat_id, currency)
    turnover = closing - reserve
    rows.extend([
        [],
        ["", "Сумма по статьям", _v151_num(income)] + [_v151_num(cat_totals.get(cat, 0.0)) for cat in categories],
        [],
        ["", "Расход", _v151_num(expense)] + [""] * len(categories),
        ["", "Приход", _v151_num(income)] + [""] * len(categories),
        ["", "Остаток на руках", _v151_num(closing)] + [""] * len(categories),
        ["", "Гомонковые", _v151_num(reserve)] + [""] * len(categories),
        ["", "Остаток в обороте", _v151_num(turnover)] + [""] * len(categories),
    ])
    if currency == "ars":
        products, metric, days, rate = _v151_food_metric(chat_id, records, start_key, end_key)
        rows.extend([
            [],
            ["", "Продукты", _v151_num(products)] + [""] * len(categories),
            [],
            ["", "Расход еды на человека в сутки", metric] + [""] * len(categories),
            ["", "Расчёт", f"{days} дн. · 5 чел. · курс {rate:g}" if rate > 0 else f"{days} дн. · 5 чел. · курс не найден"] + [""] * len(categories),
        ])
    return rows


def build_exact_category_stats_xlsx_rows(target_chat_id: int, start_key: str, start_rid: int, end_key: str, end_rid: int) -> list[list]:
    previous = getattr(_V151_EXPORT_LOCAL, "value", None)
    if not previous:
        _V151_EXPORT_LOCAL.value = {
            "kind": "exact", "target_chat_id": int(target_chat_id),
            "start_key": str(start_key)[:10], "start_rid": int(start_rid or 0),
            "end_key": str(end_key)[:10], "end_rid": int(end_rid or 0), "file_type": "xlsxstat",
        }
    try:
        ars_rows = _v151_category_table(int(target_chat_id), "ars")
        usd_rows = _v151_category_table(int(target_chat_id), "usd")
        return ars_rows + [[], []] + usd_rows
    finally:
        if not previous:
            _V151_EXPORT_LOCAL.value = None


def _xlsx_simple_rows_with_balances(rows: list[list], opening_balance: float, target_chat_id: int | None = None) -> list[list]:
    if target_chat_id is None:
        # No chat means there is no safe way to resolve the two isolated ledgers.
        return globals().get("_V150_BASE_SIMPLE_ROWS", lambda r, o, *_: r)(rows, opening_balance, target_chat_id)
    ars_rows, _ = _v151_simple_table(int(target_chat_id), "ars", compact=False)
    usd_rows, _ = _v151_simple_table(int(target_chat_id), "usd", compact=False)
    return ars_rows + [[], []] + usd_rows


def _compact_simple_excel_rows_and_annotations(raw_rows: list[tuple], opening_balance: float, target_chat_id: int | None = None) -> tuple[list[list], dict[tuple[int, int], str]]:
    if target_chat_id is None:
        base = globals().get("_V150_BASE_COMPACT_ROWS")
        return base(raw_rows, opening_balance, target_chat_id) if callable(base) else ([], {})
    ars_rows, ars_notes = _v151_simple_table(int(target_chat_id), "ars", compact=True)
    offset = len(ars_rows) + 2
    usd_rows, usd_notes = _v151_simple_table(int(target_chat_id), "usd", compact=True)
    notes = dict(ars_notes)
    for (row_idx, col_idx), text in usd_notes.items():
        notes[(int(row_idx) + offset, int(col_idx))] = text
    return ars_rows + [[], []] + usd_rows, notes


def _v151_excel_options_for_style(style: str | None, options: dict | None) -> dict | None:
    if isinstance(options, dict):
        return options
    mode = str(style or "").strip().lower()
    if not mode:
        try:
            return normalize_excel_export_options()
        except Exception:
            return {"old_table": True, "comments": False, "notes": False, "description_column": True}
    mapping = {
        "old": {"old_table": True, "comments": False, "notes": False, "description_column": True},
        "new_plain": {"old_table": False, "comments": False, "notes": False, "description_column": True},
        "new_comments": {"old_table": False, "comments": True, "notes": False, "description_column": True},
        "new_notes": {"old_table": False, "comments": False, "notes": True, "description_column": True},
        "google_notes": {"old_table": False, "comments": False, "notes": True, "description_column": True},
    }
    return mapping.get(mode) or normalize_excel_export_options()


def send_export_for_chat_to(recipient_chat_id: int, target_chat_id: int, mode: str, day_key: str, file_type: str = "csv", excel_style_override: str | None = None, excel_options_override: dict | None = None, delivery: str = "chat"):
    previous = getattr(_V151_EXPORT_LOCAL, "value", None)
    _V151_EXPORT_LOCAL.value = {
        "kind": "period", "target_chat_id": int(target_chat_id), "mode": str(mode or "all"),
        "day_key": str(day_key or today_key())[:10], "file_type": str(file_type or "csv").lower(),
    }
    try:
        options = excel_options_override
        # Disable the legacy prebuilt all-time XLSX shortcut; it cannot contain the second USD table.
        if str(file_type or "").lower().lstrip(".") in {"xlsx", "xlsxstat"}:
            effective_style = excel_style_override or excel_table_style(int(target_chat_id))
            options = _v151_excel_options_for_style(effective_style, excel_options_override)
        return _V151_BASE_SEND_EXPORT(
            recipient_chat_id, target_chat_id, mode, day_key, file_type,
            excel_style_override=excel_style_override,
            excel_options_override=options,
            delivery=delivery,
        )
    finally:
        _V151_EXPORT_LOCAL.value = previous


def send_exact_range_export(recipient_chat_id: int, target_chat_id: int, start_key: str, start_rid: int, end_key: str, end_rid: int, file_type: str, excel_style_override: str | None = None, excel_options_override: dict | None = None, delivery: str = "chat"):
    previous = getattr(_V151_EXPORT_LOCAL, "value", None)
    _V151_EXPORT_LOCAL.value = {
        "kind": "exact", "target_chat_id": int(target_chat_id),
        "start_key": str(start_key)[:10], "start_rid": int(start_rid or 0),
        "end_key": str(end_key)[:10], "end_rid": int(end_rid or 0),
        "file_type": str(file_type or "csv").lower(),
    }
    try:
        options = excel_options_override
        if str(file_type or "").lower().lstrip(".") in {"xlsx", "xlsxstat"}:
            effective_style = excel_style_override or excel_table_style(int(target_chat_id))
            options = _v151_excel_options_for_style(effective_style, excel_options_override)
        return _V151_BASE_SEND_EXACT_EXPORT(
            recipient_chat_id, target_chat_id, start_key, start_rid, end_key, end_rid, file_type,
            excel_style_override=excel_style_override,
            excel_options_override=options,
            delivery=delivery,
        )
    finally:
        _V151_EXPORT_LOCAL.value = previous


def _period_export_rows(chat_id: int, mode: str, day_key: str):
    ctx = _v151_context()
    if str(ctx.get("file_type") or "").lower() not in {"xlsx", "xlsxstat"}:
        return _V151_BASE_PERIOD_ROWS(chat_id, mode, day_key)
    records = _v151_records_in_context(int(chat_id), "ars", ctx)
    rows = [(fmt_date_table(_v151_day_key(r)), fmt_csv_amount(r.get("_v151_amount")), r.get("_v151_note", "")) for r in records]
    labels = {"day": "за день", "week": "за неделю", "month": "за месяц", "wedthu": "Ср–Чт", "all": "за всё время"}
    normalized = str(mode or "all").replace("csv_", "").replace("xlsx_", "")
    return rows, labels.get(normalized, "за всё время")


def _exact_export_rows(chat_id: int, start_key: str, start_rid: int, end_key: str, end_rid: int):
    ctx = _v151_context()
    if str(ctx.get("file_type") or "").lower() not in {"xlsx", "xlsxstat"}:
        return _V151_BASE_EXACT_ROWS(chat_id, start_key, start_rid, end_key, end_rid)
    ars = _v151_records_in_context(int(chat_id), "ars", ctx)
    presence = ars or _v151_records_in_context(int(chat_id), "usd", ctx)
    return [(fmt_date_table(_v151_day_key(r)), fmt_csv_amount(r.get("_v151_amount")), r.get("_v151_note", "")) for r in presence]


# ─────────────────────────────────────────────────────────────
# Правка 5: точное однократное покрытие отрицательного оборота.
# ─────────────────────────────────────────────────────────────
def _v151_rebalance_key(chat_id: int, currency: str, rec: dict) -> str:
    operation_key = str((rec or {}).get("operation_key") or "").strip()
    if operation_key:
        return f"{int(chat_id)}:{currency}:{operation_key}"
    source_msg_id = int((rec or {}).get("source_msg_id") or 0)
    if source_msg_id:
        return f"{int(chat_id)}:{currency}:msg:{source_msg_id}"
    return f"{int(chat_id)}:{currency}:rid:{int((rec or {}).get('id') or 0)}:{str((rec or {}).get('timestamp') or '')}"


def _v151_ledger_balance(chat_id: int, currency: str) -> float:
    return sum(_v151_float(r.get("_v151_amount")) for r in _v151_all_records(int(chat_id), currency))


def _v151_reduce_reserve(chat_id: int, currency: str, amount: float) -> list[dict]:
    entries = [dict(x) for x in (gomonk_entries(int(chat_id), currency) or [])]
    left = max(0.0, _v151_float(amount))
    for idx in range(len(entries) - 1, -1, -1):
        if left <= 1e-9:
            break
        current = max(0.0, _v151_float(entries[idx].get("amount")))
        used = min(current, left)
        entries[idx]["amount"] = current - used
        left -= used
    return [x for x in entries if _v151_float(x.get("amount")) > 1e-9]


def _v151_apply_reserve_cover(chat_id: int, currency: str, rec: dict | None, reason: str = "new_finance_operation") -> dict:
    if not isinstance(rec, dict):
        return {}
    currency = "usd" if str(currency).lower() == "usd" else "ars"
    # Respect already-applied v150 receipts during an in-place upgrade.
    old_marker = (rec.get("gomonk_rebalance_v150") or {}).get(currency)
    marker_root = rec.setdefault("gomonk_rebalance_v151", {})
    if marker_root.get(currency):
        return marker_root[currency]
    key = _v151_rebalance_key(int(chat_id), currency, rec)
    receipts = data.setdefault("gomonk_rebalance_receipts_v151", {})
    with _V151_REBALANCE_LOCK:
        existing = receipts.get(key)
        if isinstance(existing, dict) and existing.get("status") == "done":
            marker_root[currency] = dict(existing.get("result") or existing)
            return marker_root[currency]
        if old_marker:
            result = {"key": key, "currency": currency, "status": "migrated_v150", "at": _v151_now(), "legacy": old_marker}
            marker_root[currency] = result
            receipts[key] = {"status": "done", "done_at": _v151_now(), "result": result}
            save_data(data, chat_ids=[int(chat_id)])
            return result

        settings = get_chat_store(int(chat_id)).setdefault("settings", {})
        enabled_key = "usd_gomonk_enabled" if currency == "usd" else "gomonk_enabled"
        balance = _v151_ledger_balance(int(chat_id), currency)
        reserve_before = _v151_reserve(int(chat_id), currency)
        turnover_before = balance - reserve_before
        consume = min(reserve_before, max(0.0, -turnover_before)) if bool(settings.get(enabled_key, False)) else 0.0

        receipts[key] = {
            "status": "pending", "chat_id": int(chat_id), "currency": currency,
            "record_id": rec.get("id"), "source_msg_id": rec.get("source_msg_id"),
            "created_at": _v151_now(), "reason": reason,
        }
        save_data(data, chat_ids=[int(chat_id)])

        if consume > 1e-9:
            _enabled_key, entries_key, _remaining_key = _gomonk_keys(int(chat_id), currency)
            settings[entries_key] = _v151_reduce_reserve(int(chat_id), currency, consume)
        reserve_after = max(0.0, reserve_before - consume)
        turnover_after = balance - reserve_after
        result = {
            "key": key, "currency": currency, "at": _v151_now(), "reason": reason,
            "balance": balance, "reserve_before": reserve_before,
            "turnover_before": turnover_before, "consumed": consume,
            "reserve_after": reserve_after, "turnover_after": turnover_after,
        }
        marker_root[currency] = result
        receipts[key] = {"status": "done", "done_at": _v151_now(), "result": result}
        history = settings.setdefault("gomonk_rebalance_history_v151", [])
        history.append({**result, "record_id": rec.get("id"), "source_msg_id": rec.get("source_msg_id")})
        del history[:-500]
        save_data(data, chat_ids=[int(chat_id)])
        try:
            schedule_config_backup_for_chats(int(chat_id), delay=0.5)
        except Exception:
            pass
        if consume > 1e-9:
            try:
                bot_journal("gomonk_auto_cover_v151", int(chat_id), f"currency={currency}; key={key}; consumed={consume}; reserve_after={reserve_after}; turnover_after={turnover_after}")
            except Exception:
                pass
        return result


def add_record_to_chat(chat_id: int, amount: float, note: str, owner: int, source_msg=None, day_key=None, usd_amount=None, usd_note: str = "", usd_only: bool = False, source_finance_text: str = ""):
    rec = _V151_BASE_ADD_RECORD(
        chat_id, amount, note, owner, source_msg=source_msg, day_key=day_key,
        usd_amount=usd_amount, usd_note=usd_note, usd_only=usd_only,
        source_finance_text=source_finance_text,
    )
    if isinstance(rec, dict):
        try: ensure_finance_record_uid(int(chat_id), rec)
        except Exception: pass
        try: persist_finance_chat_local_fast(int(chat_id))
        except Exception: pass
        try: schedule_financial_window_refresh(int(chat_id), str(rec.get("day_key") or day_key or ""), reason="record_add_fast_v168")
        except Exception: pass
        _v151_apply_reserve_cover(int(chat_id), "ars", rec)
        if usd_amount is not None:
            _v151_apply_reserve_cover(int(chat_id), "usd", rec)
        try: persist_finance_chat_local_fast(int(chat_id))
        except Exception: pass
    return rec


def _add_record_to_currency_ledger(chat_id: int, ledger: str, amount: float, note: str, owner: int, source_msg=None, day_key: str | None = None):
    ledger = "usd" if str(ledger).lower() == "usd" else "ars"
    store_before = get_chat_store(int(chat_id))
    active = str(store_before.setdefault("settings", {}).get("_active_currency_ledger") or "ars")
    records_before = store_before.get("records", []) if active == ledger else store_before.get(f"{ledger}_records", [])
    existing_keys = {_v151_rebalance_key(int(chat_id), ledger, r) for r in records_before or [] if isinstance(r, dict)}
    result = _V151_BASE_ADD_LEDGER_RECORD(chat_id, ledger, amount, note, owner, source_msg=source_msg, day_key=day_key)
    store_after = get_chat_store(int(chat_id))
    active_after = str(store_after.setdefault("settings", {}).get("_active_currency_ledger") or "ars")
    records_after = store_after.get("records", []) if active_after == ledger else store_after.get(f"{ledger}_records", [])
    rec = None
    source_msg_id = int(getattr(source_msg, "message_id", 0) or 0) if source_msg is not None else 0
    if source_msg_id:
        rec = next((r for r in records_after or [] if isinstance(r, dict) and int(r.get("source_msg_id") or 0) == source_msg_id), None)
    if rec is None:
        rec = next((r for r in reversed(records_after or []) if isinstance(r, dict) and _v151_rebalance_key(int(chat_id), ledger, r) not in existing_keys), None)
    if rec is None and isinstance(result, dict):
        rec = result
    if isinstance(rec, dict):
        try: ensure_finance_record_uid(int(chat_id), rec)
        except Exception: pass
        try: persist_finance_chat_local_fast(int(chat_id))
        except Exception: pass
        try: schedule_financial_window_refresh(int(chat_id), str(rec.get("day_key") or day_key or ""), reason="currency_record_add_fast_v168")
        except Exception: pass
        _v151_apply_reserve_cover(int(chat_id), ledger, rec)
        try: persist_finance_chat_local_fast(int(chat_id))
        except Exception: pass
    return result if result is not None else rec


def _v151_repair_pending_rebalances() -> int:
    repaired = 0
    receipts = data.setdefault("gomonk_rebalance_receipts_v151", {})
    for key, receipt in list(receipts.items()):
        if not isinstance(receipt, dict) or receipt.get("status") != "pending":
            continue
        try:
            chat_id = int(receipt.get("chat_id"))
            currency = str(receipt.get("currency") or "ars")
            source_msg_id = int(receipt.get("source_msg_id") or 0)
            record_id = int(receipt.get("record_id") or 0)
            rec = next((r for r in _v151_all_records(chat_id, currency) if (source_msg_id and int(r.get("source_msg_id") or 0) == source_msg_id) or (record_id and int(r.get("id") or 0) == record_id)), None)
            if isinstance(rec, dict):
                _v151_apply_reserve_cover(chat_id, currency, rec, "startup_pending_repair")
                repaired += 1
            else:
                receipt.update({"status": "cancelled_no_record", "closed_at": _v151_now()})
        except Exception as exc:
            try:
                log_error(f"v151 reserve receipt repair {key}: {exc}")
            except Exception:
                pass
    if repaired:
        save_data(data)
    return repaired


# ─────────────────────────────────────────────────────────────
# Правка 6: шаблон и parser гомонковых без учета примера.
# ─────────────────────────────────────────────────────────────
_V151_EXAMPLE_RE = _v151_re.compile(r"^\(?\s*(?:пример\s*:\s*)?имя\s*1\s+1?[ .]?000\s*:\s*имя\s*2\s+5?[ .]?777\s*\)?$", flags=_v151_re.I)


def _v151_gomonk_payload(text: str) -> tuple[str, str | None]:
    raw = str(text or "").replace("\r\n", "\n").replace("\r", "\n")
    token = _v151_re.search(r"\(\s*GOMONKI\s*\|\s*(ARS|USD)\s*\)", raw, flags=_v151_re.I)
    currency = token.group(1).lower() if token else None
    kept = []
    for line in raw.split("\n"):
        stripped = line.strip()
        folded = _v151_re.sub(r"\s+", " ", stripped).casefold()
        if not stripped:
            continue
        if _v151_re.search(r"\(\s*gomonki\s*\|\s*(?:ars|usd)\s*\)", folded, flags=_v151_re.I):
            continue
        if _V151_EXAMPLE_RE.match(folded):
            continue
        kept.append(stripped)
    return "\n".join(kept).strip(), currency


def parse_gomonk_entries(text: str) -> list[dict]:
    payload, _currency = _v151_gomonk_payload(sanitize_telegram_inserted_text(str(text or "")))
    if not payload:
        return []
    parts = [p.strip() for p in _v151_re.split(r"\s*:\s*|\n+", payload) if p.strip()]
    result = []
    number_pattern = r"(?<![A-Za-zА-Яа-яЁё0-9_])[-+]?(?:\d{1,3}(?:[ .]\d{3})+(?:,\d+)?|\d+(?:[.,]\d+)?)"
    for idx, part in enumerate(parts, start=1):
        folded = _v151_re.sub(r"\s+", " ", part).casefold().strip("() ")
        if folded.startswith("пример:") or _v151_re.fullmatch(r"имя\s*[12]\s+(?:1?[ .]?000|5?[ .]?777)", folded, flags=_v151_re.I):
            continue
        matches = list(_v151_re.finditer(number_pattern, part))
        if not matches:
            continue
        match = matches[-1]
        number = match.group(0).replace(" ", "").replace(".", "").replace(",", ".")
        try:
            amount = abs(float(number))
        except Exception:
            continue
        name = (part[:match.start()] + " " + part[match.end():]).strip(" -–—,.;") or f"Сумма {idx}"
        if amount > 0:
            result.append({"name": name[:80], "amount": amount})
    return result[:30]


def _v151_gomonk_template(chat_id: int, currency: str, include_values: bool) -> str:
    username = get_bot_username_cached() or "Good_server_bot"
    lines = [
        f"@{username} (GOMONKI|{currency.upper()})",
        "(Пример: Имя1 1000 : Имя2 5777)",
        "",
    ]
    if include_values:
        values = []
        for item in gomonk_entries(int(chat_id), currency):
            amount = _v151_num(item.get("amount"))
            values.append(f"{str(item.get('name') or 'Сумма').strip()} {amount}")
        lines.append(" : ".join(values))
    return "\n".join(lines)


def build_gomonk_menu_keyboard(chat_id: int, currency: str | None = None):
    currency = _gomonk_currency(chat_id, currency)
    kb = types.InlineKeyboardMarkup(row_width=1)
    kb.row(IB(gomonk_toggle_label(chat_id, currency), callback_data=f"gomonk_toggle:{currency}"))
    can_edit = bool(gomonk_enabled(chat_id, currency) and gomonk_entries(chat_id, currency))
    label = "✏️ Изменить гомонковые" if can_edit else "💰 Ввести гомонковые"
    kb.row(make_copy_or_inline_button(label, _v151_gomonk_template(chat_id, currency, can_edit), viewer_chat_id=chat_id))
    kb.row(IB("🔙 Назад в Инфо", callback_data=f"gomonk_back:{currency}"))
    return kb


def build_gomonk_menu_text(chat_id: int, currency: str | None = None) -> str:
    currency = _gomonk_currency(chat_id, currency)
    entries = gomonk_entries(chat_id, currency)
    fmt = (lambda value: fmt_usd_native(value)) if currency == "usd" else fmt_num
    lines = [
        f"🧳 Гомонковые • {currency.upper()}", "",
        "ARS и USD хранятся отдельно.",
        "Строка с примером является только подсказкой и никогда не сохраняется.", "",
        f"Режим: {'ВКЛ' if gomonk_enabled(chat_id, currency) else 'ВЫКЛ'}",
    ]
    if entries:
        lines.append("Сохранено:")
        for item in entries:
            lines.append(f"• {item['name']}: {fmt(item['amount'])}")
        lines.append(f"Итого: {fmt(gomonk_total(chat_id, currency))}")
    else:
        lines.append("Сохранённых сумм пока нет.")
    return wm_common("\n".join(lines), 9)


def handle_gomonk_insert_message(msg) -> bool:
    if getattr(msg, "content_type", None) != "text" or not _v85_enabled("gomonk_wallets"):
        return False
    raw = str(getattr(msg, "text", "") or "")
    if not _v151_re.search(r"\(\s*GOMONKI\s*\|\s*(?:ARS|USD)\s*\)", raw, flags=_v151_re.I):
        return False
    _durable_note_source_consumed("gomonk_insert_v151")
    chat_id = int(msg.chat.id)
    payload, token_currency = _v151_gomonk_payload(raw)
    currency = token_currency or _gomonk_currency(chat_id)
    entries = parse_gomonk_entries(payload)
    try:
        bot.delete_message(chat_id, msg.message_id)
    except Exception:
        pass
    if not entries:
        send_and_auto_delete(chat_id, "ℹ️ Шаблон получен без реальных значений. Гомонковые не изменены.", 12)
        return True
    set_gomonk_entries(chat_id, entries, currency)
    settings = _gomonk_settings(chat_id, currency)
    enabled_key, _entries_key, _remaining_key = _gomonk_keys(chat_id, currency)
    settings[enabled_key] = True
    save_data(data, chat_ids=[chat_id])
    total = gomonk_total(chat_id, currency)
    shown = fmt_usd_native(total) if currency == "usd" else fmt_num(total)
    try:
        bot_journal("gomonk_values_saved_v151", chat_id, f"currency={currency}; count={len(entries)}; total={total}")
    except Exception:
        pass
    send_and_auto_delete(chat_id, f"✅ Гомонковые {currency.upper()} сохранены: {len(entries)}, сумма {shown}", 10)
    try:
        open_gomonk_window(chat_id, currency=currency)
        finance_changed(chat_id, get_chat_store(chat_id).get("current_view_day") or today_key(), reason="gomonk_update", delay=0.05)
    except Exception:
        pass
    return True


# ─────────────────────────────────────────────────────────────
# Правка 7: «За месяц» — только текст текущего USD-окна.
# ─────────────────────────────────────────────────────────────
def render_usd_month_window(chat_id: int, day_key: str):
    month_key = str(day_key or today_key())[:7]
    try:
        month_dt = _v151_datetime.strptime(month_key + "-01", "%Y-%m-%d")
        month_label = month_dt.strftime("%m.%Y")
    except Exception:
        month_label = month_key
    rows = [r for r in usd_records_for_month(int(chat_id), month_key) if _v151_float(r.get("usd_amount")) < 0]
    total_expense = sum(abs(_v151_float(r.get("usd_amount"))) for r in rows)
    lines = [f"💵 USD расходы за {month_label}", ""]
    if rows:
        for rec in rows:
            amount = abs(_v151_float(rec.get("usd_amount")))
            sid = str(rec.get("usd_short_id") or rec.get("short_id") or f"U{rec.get('id', '')}")
            date_label = fmt_date_ddmmyy(_v151_day_key(rec))
            note = html.escape(str(rec.get("usd_note") or rec.get("note") or ""))
            lines.append(f"{sid} {date_label} -${fmt_num_plain(amount)} {note}".rstrip())
    else:
        lines.append("Нет USD-расходов за этот месяц.")
    balance = usd_balance_for_chat(int(chat_id))
    lines.extend([
        "",
        f"📉 Расход за месяц: -${fmt_num_plain(total_expense)}",
        f"🏦 USD остаток по чату: {('+' if balance >= 0 else '-')}${fmt_num_plain(abs(balance))}",
    ])
    try:
        _V151_MONTH_LOCAL.chat_id = int(chat_id)
    except Exception:
        pass
    return wm_common("\n".join(lines), 1, html_mode=True), -total_expense


def build_usd_month_keyboard(day_key: str):
    """Возвращает ту же клавиатуру, что уже была в USD-окне."""
    chat_id = getattr(_V151_MONTH_LOCAL, "chat_id", None)
    if chat_id is not None:
        try:
            return build_main_keyboard(str(day_key)[:10], int(chat_id))
        except Exception:
            pass
    # Редкий fallback до первого render: не меняем структуру на месячную навигацию.
    try:
        return build_main_keyboard(str(day_key)[:10], None)
    except Exception:
        return types.InlineKeyboardMarkup()




def build_fin_window_usd_month_keyboard(target_chat_id: int, day_key: str, owner_day_key: str):
    """В окне владельца также сохраняется исходное расположение кнопок чата."""
    return build_fin_window_view_keyboard(int(target_chat_id), str(day_key)[:10], str(owner_day_key)[:10])

def set_webhook():
    try:
        repaired = _v151_repair_pending_rebalances()
        if repaired:
            try:
                bot_journal("v151_startup_repair", int(OWNER_ID or 0), f"reserve_receipts={repaired}")
            except Exception:
                pass
    except Exception as exc:
        try:
            log_error(f"v151 startup repair: {exc}")
        except Exception:
            pass
    return _V151_BASE_SET_WEBHOOK()

# v168_clean_core_record_identity
