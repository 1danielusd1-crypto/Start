# v178_global_performance_final

# ─────────────────────────────────────────────────────────────
# v150: f191 chat list, Excel reserve rows, exact-once gomonk
# cover, slash-command audit/durable receipts, stable chat lifecycle.
# ─────────────────────────────────────────────────────────────
import copy as _v150_copy
import json as _v150_json
import math as _v150_math
import re as _v150_re
import threading as _v150_threading
import time as _v150_time
from datetime import datetime as _v150_datetime

VERSION = "bot_v150_excel_reserve_chat_lifecycle"
V150_CHAT_STATUSES = {"active", "unreachable", "bot_removed", "migrated", "archived"}
V150_CHAT_STATUS_LABELS = {
    "active": "🟢 active",
    "unreachable": "🟠 unreachable",
    "bot_removed": "⛔ bot_removed",
    "migrated": "➡️ migrated",
    "archived": "📦 archived",
}
_V150_LOCK = _v150_threading.RLock()
_V150_LAST_ACTIVE_PERSIST = {}

_V150_BASE_REMINDER_MENU_TEXT = globals().get("build_reminder_menu_text")
_V150_BASE_REMINDER_MENU_KEYBOARD = globals().get("build_reminder_menu_keyboard")
_V150_BASE_CATEGORY_ROWS = globals().get("build_exact_category_stats_xlsx_rows")
_V150_BASE_SIMPLE_ROWS = globals().get("_xlsx_simple_rows_with_balances")
_V150_BASE_COMPACT_ROWS = globals().get("_compact_simple_excel_rows_and_annotations")
_V150_BASE_ADD_RECORD = globals().get("add_record_to_chat")
_V150_BASE_ADD_CURRENCY_RECORD = globals().get("_add_record_to_currency_ledger")
_V150_BASE_DURABLE_REQUIRED = globals().get("durable_task_required")
_V150_BASE_DURABLE_EXPECTED = globals().get("_durable_expected_effects")
_V150_BASE_DURABLE_REPORT = globals().get("_durable_effect_report")
_V150_BASE_EXECUTE_PAYLOAD = globals().get("_execute_telegram_payload")
_V150_BASE_SET_WEBHOOK = globals().get("set_webhook")
_V150_BASE_UPDATE_CHAT_MESSAGE = globals().get("update_chat_info_from_message")
_V150_BASE_UPDATE_CHAT_OBJECT = globals().get("update_chat_info_from_chat_object")
_V150_BASE_MIGRATE_CHAT = globals().get("migrate_chat_id_everywhere")
_V150_BASE_TENANT_CHATS_TEXT = globals().get("tenant_chats_text")


def _v150_now() -> str:
    try:
        return now_local().isoformat(timespec="seconds")
    except Exception:
        return _v150_datetime.now().astimezone().isoformat(timespec="seconds")


def _v150_float(value, default=0.0) -> float:
    try:
        number = float(value or 0)
        return number if _v150_math.isfinite(number) else float(default)
    except Exception:
        return float(default)


def _v150_command_from_payload(payload: dict) -> tuple[str, int | None, int | None, int | None]:
    if not isinstance(payload, dict):
        return "", None, None, None
    raw = payload.get("message") or payload.get("edited_message") or payload.get("channel_post") or payload.get("edited_channel_post")
    if not isinstance(raw, dict):
        return "", None, None, None
    text = str(raw.get("text") or raw.get("caption") or "").strip()
    first = text.split(maxsplit=1)[0].lower() if text.startswith("/") else ""
    if "@" in first:
        first = first.split("@", 1)[0]
    try: chat_id = int((raw.get("chat") or {}).get("id"))
    except Exception: chat_id = None
    try: msg_id = int(raw.get("message_id"))
    except Exception: msg_id = None
    try: user_id = int((raw.get("from") or {}).get("id"))
    except Exception: user_id = None
    return first, chat_id, msg_id, user_id


# ─────────────────────────────────────────────────────────────
# f191: show actual chats; keep only “К напоминалкам”.
# ─────────────────────────────────────────────────────────────
def _v150_reminder_chat_lines(cfg: dict) -> list[str]:
    chat_ids = []
    for raw in (cfg or {}).get("chat_ids") or []:
        try:
            cid = int(raw)
        except Exception:
            continue
        if cid not in chat_ids:
            chat_ids.append(cid)
    if not chat_ids:
        return ["💬 Чат: не выбран"]
    if len(chat_ids) == 1:
        return [f"💬 Чат: {get_chat_display_name(chat_ids[0])}"]
    lines = ["💬 Чаты:"]
    for cid in chat_ids:
        lines.append(f"• {get_chat_display_name(cid)}")
    return lines


def build_reminder_menu_text(reminder_id: int) -> str:
    cfg = _reminder_cfg(reminder_id)
    base = _V150_BASE_REMINDER_MENU_TEXT(reminder_id) if callable(_V150_BASE_REMINDER_MENU_TEXT) else ""
    if not isinstance(cfg, dict):
        return base
    lines = str(base or "").splitlines()
    out = []
    inserted = False
    for line in lines:
        # Replace the old count-only line from f191.
        if _v150_re.match(r"^\s*[✅❌]\s*2\.\s*Чаты\s*:", str(line), flags=_v150_re.I):
            out.extend(_v150_reminder_chat_lines(cfg))
            inserted = True
            continue
        out.append(line)
    if not inserted:
        # Insert after reminder text, before period/status.
        insert_at = 2 if len(out) >= 2 else len(out)
        for idx, line in enumerate(out):
            if "Период:" in line or "Расписание:" in line or "Состояние:" in line:
                insert_at = idx
                break
        out[insert_at:insert_at] = _v150_reminder_chat_lines(cfg) + [""]
    return "\n".join(out)[:3900]


def build_reminder_menu_keyboard(reminder_id: int, day_key: str | None = None, page: int = 0, viewer_chat_id: int | None = None):
    kb = _V150_BASE_REMINDER_MENU_KEYBOARD(reminder_id, day_key, page, viewer_chat_id) if callable(_V150_BASE_REMINDER_MENU_KEYBOARD) else types.InlineKeyboardMarkup()
    try:
        filtered = []
        for row in list(getattr(kb, "keyboard", None) or []):
            clean = []
            for button in row:
                text = str(getattr(button, "text", "") or "").strip()
                # f191 already has the explicit destination button. Remove all generic Back variants.
                if "К напоминалкам" not in text and _v150_re.search(r"(^|\s)(⬅️|🔙)?\s*Назад\s*$", text, flags=_v150_re.I):
                    continue
                clean.append(button)
            if clean:
                filtered.append(clean)
        kb.keyboard = filtered
    except Exception as exc:
        try: log_error(f"v150 f191 keyboard cleanup: {exc}")
        except Exception: pass
    return kb


# ─────────────────────────────────────────────────────────────
# Excel rows and food/person/day metric.
# ─────────────────────────────────────────────────────────────
def _v150_export_currency(chat_id: int) -> str:
    try:
        return "usd" if financial_view_is_usd(get_chat_store(int(chat_id))) else "ars"
    except Exception:
        return "ars"


def _v150_export_reserve(chat_id: int) -> float:
    try:
        return float(gomonk_total(int(chat_id), _v150_export_currency(chat_id)) or 0)
    except Exception:
        return 0.0


def _v150_usd_rate() -> float:
    try:
        row = usd_rate_cached(force=False) or {}
        return max(0.0, _v150_float(row.get("rate") or row.get("venta") or row.get("sell")))
    except Exception:
        return 0.0


def _v150_food_per_person(products_total: float) -> float:
    rate = _v150_usd_rate()
    return (max(0.0, _v150_float(products_total)) / 5.0 / rate) if rate > 0 else 0.0


def _v150_is_products_category(name: str) -> bool:
    clean = _v150_re.sub(r"\s+", " ", str(name or "").strip().casefold())
    return clean in {"продукты", "еда", "продукт", "food", "products"}


def _v150_product_total_from_records(chat_id: int, records) -> float:
    store = get_chat_store(int(chat_id))
    total = 0.0
    for item in records or []:
        rec = item[1] if isinstance(item, tuple) and len(item) >= 2 else item
        if not isinstance(rec, dict):
            continue
        amount = financial_view_amount(store, rec)
        if amount >= 0:
            continue
        note = financial_view_note(store, rec)
        try:
            category = resolve_expense_category(note, store)
        except Exception:
            category = ""
        if _v150_is_products_category(category):
            total += abs(float(amount))
    return total


def _v150_append_summary_rows(rows: list[list], chat_id: int | None, closing: float, products_total: float, layout: str) -> list[list]:
    out = [list(x or []) for x in (rows or [])]
    if chat_id is None:
        return out
    reserve = _v150_export_reserve(int(chat_id))
    turnover = float(closing) - reserve
    metric = _v150_food_per_person(products_total)
    if layout == "compact":
        out.append(["Гомонковые", reserve, ""])
        out.append(["Остаток в обороте", turnover, ""])
        out.append([])
        out.append(["Расход еды на человека в сутки", metric, ""])
    else:
        width = max([len(r) for r in out if r] + [4])
        row_res = ["", "Гомонковые", reserve] + [""] * max(0, width - 3)
        row_turn = ["", "Остаток в обороте", turnover] + [""] * max(0, width - 3)
        row_food = ["", "Расход еды на человека в сутки", metric] + [""] * max(0, width - 3)
        out.extend([row_res[:width], row_turn[:width], [], row_food[:width]])
    return out


def _v150_cell_value(value) -> float:
    if isinstance(value, dict):
        return _v150_float(value.get("value"))
    return _v150_float(value)


def _v177_legacy_0175_build_exact_category_stats_xlsx_rows(target_chat_id: int, start_key: str, start_rid: int, end_key: str, end_rid: int) -> list[list]:
    rows = _V150_BASE_CATEGORY_ROWS(target_chat_id, start_key, start_rid, end_key, end_rid)
    closing = 0.0
    for row in reversed(rows or []):
        if len(row) > 2 and str(row[1] if len(row) > 1 else "").strip().casefold() == "остаток на руках":
            closing = _v150_cell_value(row[2])
            break
    try:
        records = exact_record_range(get_chat_store(int(target_chat_id)), start_key, start_rid, end_key, end_rid)
        products_total = _v150_product_total_from_records(int(target_chat_id), records)
    except Exception:
        products_total = 0.0
    return _v150_append_summary_rows(rows, int(target_chat_id), closing, products_total, "wide")
try: _v177_legacy_0175_build_exact_category_stats_xlsx_rows.__name__ = 'build_exact_category_stats_xlsx_rows'
except Exception: pass
build_exact_category_stats_xlsx_rows = _v177_legacy_0175_build_exact_category_stats_xlsx_rows


def _v177_legacy_0091_xlsx_simple_rows_with_balances(rows: list[list], opening_balance: float, target_chat_id: int | None = None) -> list[list]:
    base = _V150_BASE_SIMPLE_ROWS(rows, opening_balance, target_chat_id) if _V150_BASE_SIMPLE_ROWS.__code__.co_argcount >= 3 else _V150_BASE_SIMPLE_ROWS(rows, opening_balance)
    closing = 0.0
    for row in reversed(base or []):
        if len(row) > 2 and str(row[1] if len(row) > 1 else "").strip().casefold() == "остаток на руках":
            closing = _v150_cell_value(row[2]); break
    products_total = 0.0
    if target_chat_id is not None:
        store = get_chat_store(int(target_chat_id))
        for row in (rows or [])[1:]:
            if len(row) < 4:
                continue
            note = str(row[1] or "")
            expense = _v150_float(row[3])
            if expense <= 0:
                continue
            try: category = resolve_expense_category(note, store)
            except Exception: category = ""
            if _v150_is_products_category(category):
                products_total += expense
    return _v150_append_summary_rows(base, target_chat_id, closing, products_total, "wide")
try: _v177_legacy_0091_xlsx_simple_rows_with_balances.__name__ = '_xlsx_simple_rows_with_balances'
except Exception: pass
_xlsx_simple_rows_with_balances = _v177_legacy_0091_xlsx_simple_rows_with_balances


def _v177_legacy_0094_compact_simple_excel_rows_and_annotations(raw_rows: list[tuple], opening_balance: float, target_chat_id: int | None = None) -> tuple[list[list], dict[tuple[int, int], str]]:
    if _V150_BASE_COMPACT_ROWS.__code__.co_argcount >= 3:
        base, notes = _V150_BASE_COMPACT_ROWS(raw_rows, opening_balance, target_chat_id)
    else:
        base, notes = _V150_BASE_COMPACT_ROWS(raw_rows, opening_balance)
    closing = 0.0
    for row in reversed(base or []):
        if row and str(row[0] or "").strip().casefold() == "остаток на руках":
            closing = _v150_cell_value(row[1] if len(row) > 1 else 0); break
    products_total = 0.0
    if target_chat_id is not None:
        store = get_chat_store(int(target_chat_id))
        for _date, amount_raw, note in raw_rows or []:
            try: amount = parse_csv_amount(amount_raw)
            except Exception: amount = _v150_float(amount_raw)
            if amount >= 0:
                continue
            try: category = resolve_expense_category(str(note or ""), store)
            except Exception: category = ""
            if _v150_is_products_category(category):
                products_total += abs(amount)
    return _v150_append_summary_rows(base, target_chat_id, closing, products_total, "compact"), notes
try: _v177_legacy_0094_compact_simple_excel_rows_and_annotations.__name__ = '_compact_simple_excel_rows_and_annotations'
except Exception: pass
_compact_simple_excel_rows_and_annotations = _v177_legacy_0094_compact_simple_excel_rows_and_annotations


# ─────────────────────────────────────────────────────────────
# Exact-once reserve cover after a real new finance operation.
# ─────────────────────────────────────────────────────────────
def _v150_ledger_balance(store: dict, currency: str) -> float:
    currency = "usd" if str(currency).lower() == "usd" else "ars"
    active = str((store.setdefault("settings", {})).get("_active_currency_ledger") or "ars").lower()
    if active == currency:
        return _v150_float(store.get("balance"))
    return _v150_float(store.get(f"{currency}_balance"))


def _v150_reduce_reserve_entries(chat_id: int, currency: str, consume: float) -> list[dict]:
    entries = [dict(x) for x in (gomonk_entries(int(chat_id), currency) or [])]
    left = max(0.0, _v150_float(consume))
    # LIFO: the most recently added reserve item is consumed first.
    for idx in range(len(entries) - 1, -1, -1):
        if left <= 1e-9:
            break
        amount = max(0.0, _v150_float(entries[idx].get("amount")))
        used = min(amount, left)
        entries[idx]["amount"] = amount - used
        left -= used
    return [x for x in entries if _v150_float(x.get("amount")) > 1e-9]


def _v150_rebalance_key(chat_id: int, currency: str, rec: dict) -> str:
    operation_key = str((rec or {}).get("operation_key") or "").strip()
    if operation_key:
        return f"{chat_id}:{currency}:{operation_key}"
    source = int((rec or {}).get("source_msg_id") or 0)
    if source:
        return f"{chat_id}:{currency}:msg:{source}"
    return f"{chat_id}:{currency}:rid:{int((rec or {}).get('id') or 0)}:{str((rec or {}).get('timestamp') or '')}"


def _v150_apply_reserve_cover(chat_id: int, currency: str, rec: dict | None, reason: str = "new_finance_operation") -> dict:
    if not isinstance(rec, dict):
        return {}
    currency = "usd" if str(currency).lower() == "usd" else "ars"
    marker_root = rec.setdefault("gomonk_rebalance_v150", {})
    if marker_root.get(currency):
        return marker_root[currency]
    key = _v150_rebalance_key(int(chat_id), currency, rec)
    store = get_chat_store(int(chat_id))
    settings = store.setdefault("settings", {})
    enabled_key = "usd_gomonk_enabled" if currency == "usd" else "gomonk_enabled"
    balance = _v150_ledger_balance(store, currency)
    reserve_before = float(gomonk_total(int(chat_id), currency) or 0)
    raw_turnover = balance - reserve_before
    consume = min(reserve_before, max(0.0, -raw_turnover)) if bool(settings.get(enabled_key, False)) else 0.0
    if consume > 1e-9:
        entries_after = _v150_reduce_reserve_entries(int(chat_id), currency, consume)
        _enabled_key, entries_key, _remaining_key = _gomonk_keys(int(chat_id), currency)
        settings[entries_key] = entries_after
    reserve_after = max(0.0, reserve_before - consume)
    turnover_after = balance - reserve_after
    result = {
        "key": key,
        "currency": currency,
        "at": _v150_now(),
        "reason": reason,
        "balance": balance,
        "reserve_before": reserve_before,
        "consumed": consume,
        "reserve_after": reserve_after,
        "turnover_after": turnover_after,
    }
    marker_root[currency] = result
    history = settings.setdefault("gomonk_rebalance_history_v150", [])
    history.append({**result, "record_id": rec.get("id"), "source_msg_id": rec.get("source_msg_id")})
    del history[:-300]
    intents = data.setdefault("gomonk_rebalance_intents_v150", {})
    if key in intents:
        intents[key].update({"status": "done", "done_at": _v150_now(), "result": result})
    save_data(data, chat_ids=[int(chat_id)])
    try: schedule_config_backup_for_chats(int(chat_id), delay=0.5)
    except Exception: pass
    if consume > 1e-9:
        try: bot_journal("gomonk_auto_cover", int(chat_id), _v150_json.dumps(result, ensure_ascii=False))
        except Exception: pass
    return result


def _v150_prepare_intent(chat_id: int, currency: str, source_msg, amount: float, note: str) -> str:
    try: source_msg_id = int(getattr(source_msg, "message_id", 0) or 0)
    except Exception: source_msg_id = 0
    if not source_msg_id:
        return ""
    key = f"{int(chat_id)}:{currency}:msg:{source_msg_id}"
    intents = data.setdefault("gomonk_rebalance_intents_v150", {})
    if key not in intents:
        intents[key] = {
            "status": "pending", "chat_id": int(chat_id), "currency": currency,
            "source_msg_id": source_msg_id, "amount": _v150_float(amount), "note": str(note or "")[:180],
            "created_at": _v150_now(),
        }
        # Write-before-effect: recovery can finish reserve cover, but can never create a second finance record.
        save_data(data, chat_ids=[int(chat_id)])
    return key


def _v177_legacy_0228_add_record_to_chat(chat_id: int, amount: float, note: str, owner: int, source_msg=None, day_key=None, usd_amount=None, usd_note: str = "", usd_only: bool = False, source_finance_text: str = ""):
    _v150_prepare_intent(int(chat_id), "ars", source_msg, amount, note)
    if usd_amount is not None:
        _v150_prepare_intent(int(chat_id), "usd", source_msg, usd_amount, usd_note or note)
    rec = _V150_BASE_ADD_RECORD(chat_id, amount, note, owner, source_msg=source_msg, day_key=day_key, usd_amount=usd_amount, usd_note=usd_note, usd_only=usd_only, source_finance_text=source_finance_text)
    if isinstance(rec, dict):
        _v150_apply_reserve_cover(int(chat_id), "ars", rec)
        if usd_amount is not None:
            _v150_apply_reserve_cover(int(chat_id), "usd", rec)
    return rec
try: _v177_legacy_0228_add_record_to_chat.__name__ = 'add_record_to_chat'
except Exception: pass
add_record_to_chat = _v177_legacy_0228_add_record_to_chat


def _v177_legacy_0137_add_record_to_currency_ledger(chat_id: int, ledger: str, amount: float, note: str, owner: int, source_msg=None, day_key: str | None = None):
    ledger = "usd" if str(ledger).lower() == "usd" else "ars"
    _v150_prepare_intent(int(chat_id), ledger, source_msg, amount, note)
    store = get_chat_store(int(chat_id))
    source_msg_id = int(getattr(source_msg, "message_id", 0) or 0) if source_msg is not None else 0
    records_key = "records" if str(store.setdefault("settings", {}).get("_active_currency_ledger") or "ars") == ledger else f"{ledger}_records"
    if source_msg_id:
        existing = next((r for r in store.get(records_key, []) or [] if isinstance(r, dict) and int(r.get("source_msg_id") or 0) == source_msg_id), None)
        if isinstance(existing, dict):
            _v150_apply_reserve_cover(int(chat_id), ledger, existing, "duplicate_retry_repair")
            return existing
    before_ids = {id(r) for r in store.get(records_key, []) or [] if isinstance(r, dict)}
    result = _V150_BASE_ADD_CURRENCY_RECORD(chat_id, ledger, amount, note, owner, source_msg=source_msg, day_key=day_key)
    store = get_chat_store(int(chat_id))
    candidates = [r for r in store.get(records_key, []) or [] if isinstance(r, dict) and id(r) not in before_ids]
    rec = candidates[-1] if candidates else result if isinstance(result, dict) else None
    if isinstance(rec, dict):
        _v150_apply_reserve_cover(int(chat_id), ledger, rec)
    return rec
try: _v177_legacy_0137_add_record_to_currency_ledger.__name__ = '_add_record_to_currency_ledger'
except Exception: pass
_add_record_to_currency_ledger = _v177_legacy_0137_add_record_to_currency_ledger


def _v150_repair_pending_rebalances() -> int:
    repaired = 0
    intents = data.setdefault("gomonk_rebalance_intents_v150", {})
    for key, intent in list(intents.items()):
        if not isinstance(intent, dict) or intent.get("status") != "pending":
            continue
        try:
            chat_id = int(intent.get("chat_id")); currency = str(intent.get("currency") or "ars")
            source_msg_id = int(intent.get("source_msg_id") or 0)
            store = get_chat_store(chat_id)
            active = str(store.setdefault("settings", {}).get("_active_currency_ledger") or "ars")
            records = store.get("records", []) if active == currency else store.get(f"{currency}_records", [])
            rec = next((r for r in records or [] if isinstance(r, dict) and int(r.get("source_msg_id") or 0) == source_msg_id), None)
            if isinstance(rec, dict):
                _v150_apply_reserve_cover(chat_id, currency, rec, "startup_pending_repair")
                repaired += 1
        except Exception as exc:
            try: log_error(f"v150 rebalance repair {key}: {exc}")
            except Exception: pass
    return repaired


# ─────────────────────────────────────────────────────────────
# Chat lifecycle: never delete/hide on a transient Telegram error.
# ─────────────────────────────────────────────────────────────
def _v150_error_class(err) -> tuple[str, str]:
    text = str(err or "").strip()
    low = text.casefold()
    definitive = (
        "bot was kicked", "bot was blocked by the user", "bot is not a member",
        "kicked from the", "bot was blocked", "bot removed",
    )
    if any(x in low for x in definitive):
        return "bot_removed", "telegram_confirmed_bot_removed"
    if "migrate_to_chat_id" in low or "group chat was upgraded" in low:
        return "migrated", "telegram_migration"
    if "chat not found" in low:
        return "unreachable", "telegram_chat_not_found"
    if "not enough rights" in low or "have no rights" in low or "not enough permissions" in low:
        return "unreachable", "telegram_missing_rights"
    if "forbidden" in low:
        return "unreachable", "telegram_forbidden_unconfirmed"
    if "timeout" in low or "timed out" in low or "connection" in low or "temporar" in low or "429" in low or "too many requests" in low:
        return "unreachable", "telegram_temporary_error"
    return "unreachable", "telegram_unknown_error"


def _is_bot_removed_error(err) -> bool:
    return _v150_error_class(err)[0] == "bot_removed"


def _v150_lifecycle(chat_id: int) -> dict:
    store = get_chat_store(int(chat_id))
    row = store.get("chat_lifecycle_v150")
    if not isinstance(row, dict):
        settings = store.setdefault("settings", {})
        old_removed = bool(settings.get("bot_removed", False))
        old_reason = str(settings.get("bot_removed_reason") or "")
        status = "active"
        if old_removed:
            status, _reason_code = _v150_error_class(old_reason)
        _created_at = _v150_now()
        row = {
            "status": status,
            "status_since": _created_at,
            "last_seen_at": "",
            "last_success_at": "",
            "last_error": old_reason,
            "consecutive_failures": 0,
            "migrated_to": None,
            "history": [{
                "at": _created_at, "from": "unknown", "to": status,
                "reason": old_reason[:200] if old_reason else "chat card discovered during v150 migration",
                "source": "v150_migration", "migrated_to": None,
            }],
        }
        store["chat_lifecycle_v150"] = row
    row.setdefault("status", "active")
    row.setdefault("status_since", _v150_now())
    row.setdefault("last_seen_at", "")
    row.setdefault("last_success_at", "")
    row.setdefault("last_error", "")
    row.setdefault("consecutive_failures", 0)
    row.setdefault("migrated_to", None)
    row.setdefault("history", [])
    return row


def set_chat_status_v150(chat_id: int, status: str, reason: str, *, source: str = "runtime", migrated_to: int | None = None, force_history: bool = False) -> dict:
    chat_id = int(chat_id)
    status = str(status or "unreachable").strip().lower()
    if status not in V150_CHAT_STATUSES:
        status = "unreachable"
    with _V150_LOCK:
        row = _v150_lifecycle(chat_id)
        previous = str(row.get("status") or "active")
        now = _v150_now()
        # archived is a deliberate administrative state; routine Telegram success must not
        # silently unarchive it. Only /chat_restore (source=command) may return it to active.
        if previous == "archived" and status == "active" and str(source or "") != "command":
            row["last_seen_at"] = now
            row["last_success_at"] = now
            return row
        changed = previous != status
        # A normal incoming message proves that the chat is active, but persisting that fact
        # on every message would create needless SQLite/MEGA churn. Persist at most once per
        # five minutes unless the status actually changes.
        if status == "active" and previous == "active" and not force_history:
            last_mono = float(_V150_LAST_ACTIVE_PERSIST.get(chat_id, 0.0) or 0.0)
            if (_v150_time.monotonic() - last_mono) < 300.0:
                row["last_seen_at"] = now
                row["last_success_at"] = now
                row["last_error"] = ""
                row["consecutive_failures"] = 0
                return row
        if changed:
            row["status"] = status
            row["status_since"] = now
        if status == "active":
            row["last_seen_at"] = now
            row["last_success_at"] = now
            row["last_error"] = ""
            row["consecutive_failures"] = 0
        elif status == "migrated":
            row["migrated_to"] = int(migrated_to) if migrated_to is not None else row.get("migrated_to")
            row["last_error"] = str(reason or "")[:500]
        else:
            row["last_error"] = str(reason or "")[:500]
            row["consecutive_failures"] = int(row.get("consecutive_failures") or 0) + 1
        settings = get_chat_store(chat_id).setdefault("settings", {})
        settings["bot_removed"] = status == "bot_removed"
        if status == "bot_removed":
            settings["bot_removed_reason"] = str(reason or "")[:300]
            settings["bot_removed_at"] = now
        else:
            settings.pop("bot_removed_reason", None)
            settings.pop("bot_removed_at", None)
        signature = (previous, status, str(reason or "")[:200], str(source or ""))
        last = (row.get("history") or [])[-1] if row.get("history") else {}
        last_signature = (last.get("from"), last.get("to"), last.get("reason"), last.get("source")) if isinstance(last, dict) else None
        if changed or force_history or signature != last_signature:
            row["history"].append({
                "at": now, "from": previous, "to": status,
                "reason": str(reason or "")[:200], "source": str(source or "")[:80],
                "migrated_to": int(migrated_to) if migrated_to is not None else None,
            })
            del row["history"][:-200]
        save_data(data, chat_ids=[chat_id])
        if status == "active":
            _V150_LAST_ACTIVE_PERSIST[chat_id] = _v150_time.monotonic()
        try: schedule_config_backup_for_chats(chat_id, delay=1.0)
        except Exception: pass
        try: bot_journal("chat_lifecycle", chat_id, f"{previous}->{status}; source={source}; reason={str(reason)[:240]}", "WARN" if status != "active" else "INFO")
        except Exception: pass
        return row


def set_chat_bot_removed(chat_id: int, removed: bool = True, reason: str = ""):
    if not removed:
        return set_chat_status_v150(int(chat_id), "active", reason or "telegram api success", source="legacy_bridge")
    status, code = _v150_error_class(reason)
    return set_chat_status_v150(int(chat_id), status, reason or code, source=code)


def is_chat_bot_removed(chat_id: int) -> bool:
    try:
        return str(_v150_lifecycle(int(chat_id)).get("status")) in {"bot_removed", "migrated", "archived"}
    except Exception:
        return False


def chat_button_title(chat_id: int, title: str | None = None) -> str:
    title = str(title or get_chat_display_name(int(chat_id)))
    try: status = str(_v150_lifecycle(int(chat_id)).get("status") or "active")
    except Exception: status = "active"
    icon = {"active": "", "unreachable": "🟠 ", "bot_removed": "⛔ ", "migrated": "➡️ ", "archived": "📦 "}.get(status, "")
    return icon + title


def update_chat_info_from_message(msg):
    result = _V150_BASE_UPDATE_CHAT_MESSAGE(msg) if callable(_V150_BASE_UPDATE_CHAT_MESSAGE) else None
    try:
        chat_id = int(msg.chat.id)
        set_chat_status_v150(chat_id, "active", "message received", source="telegram_update")
    except Exception:
        pass
    return result


def update_chat_info_from_chat_object(chat_obj) -> bool:
    result = bool(_V150_BASE_UPDATE_CHAT_OBJECT(chat_obj)) if callable(_V150_BASE_UPDATE_CHAT_OBJECT) else False
    try:
        set_chat_status_v150(int(chat_obj.id), "active", "getChat success", source="telegram_get_chat")
    except Exception:
        pass
    return result


def probe_bot_in_chat(chat_id: int) -> bool:
    chat_id = int(chat_id)
    try:
        obj = _tg_call_retry(bot.get_chat, chat_id, attempts=2, purpose="probe_get_chat")
        update_chat_info_from_chat_object(obj)
        return True
    except Exception as exc:
        status, code = _v150_error_class(exc)
        set_chat_status_v150(chat_id, status, str(exc)[:500], source=code)
        return False


def migrate_chat_id_everywhere(old_chat_id: int, new_chat_id: int, reason: str = "telegram supergroup migration") -> bool:
    ok = bool(_V150_BASE_MIGRATE_CHAT(old_chat_id, new_chat_id, reason)) if callable(_V150_BASE_MIGRATE_CHAT) else False
    if ok:
        set_chat_status_v150(int(old_chat_id), "migrated", reason, source="telegram_migration", migrated_to=int(new_chat_id), force_history=True)
        set_chat_status_v150(int(new_chat_id), "active", f"migrated from {int(old_chat_id)}", source="telegram_migration", force_history=True)
    return ok


def _v150_status_line(chat_id: int) -> str:
    row = _v150_lifecycle(int(chat_id))
    status = str(row.get("status") or "active")
    reason = str(row.get("last_error") or "").strip()
    label = V150_CHAT_STATUS_LABELS.get(status, status)
    return f"{label}" + (f" · {reason[:90]}" if reason else "")


def _v177_legacy_0259_tenant_chats_text(tenant_id: str) -> str:
    row = tenant_get(tenant_id) or {}
    lines = [f"💬 ЧАТЫ · {row.get('name')}", ""]
    for cid in row.get("chat_ids") or []:
        cid = int(cid)
        marker = "🏠" if cid == int(row.get("root_chat_id") or 0) else "•"
        lines.append(f"{marker} {get_chat_display_name(cid)} · {cid}")
        lines.append(f"   {_v150_status_line(cid)}")
    if len(lines) == 2:
        lines.append("Нет подключённых чатов.")
    return "\n".join(lines)[:3900]
try: _v177_legacy_0259_tenant_chats_text.__name__ = 'tenant_chats_text'
except Exception: pass
tenant_chats_text = _v177_legacy_0259_tenant_chats_text


def _v150_chat_access(user_id: int, chat_id: int) -> bool:
    try:
        tid = tenant_id_for_chat(int(chat_id), create=False)
        if not tid:
            return tenant_is_platform_owner_user(int(user_id))
        return tenant_can_manage(int(user_id), str(tid)) or tenant_is_platform_owner_user(int(user_id))
    except Exception:
        return False


def _v150_status_text(chat_id: int) -> str:
    row = _v150_lifecycle(int(chat_id))
    lines = [
        f"💬 {get_chat_display_name(int(chat_id))}",
        f"ID: {int(chat_id)}",
        f"Статус: {V150_CHAT_STATUS_LABELS.get(str(row.get('status')), str(row.get('status')))}",
        f"С этого времени: {row.get('status_since') or '—'}",
        f"Последний успешный контакт: {row.get('last_success_at') or '—'}",
        f"Последняя ошибка: {row.get('last_error') or '—'}",
        f"Ошибок подряд: {int(row.get('consecutive_failures') or 0)}",
    ]
    if row.get("migrated_to"):
        lines.append(f"Перенесён в: {row.get('migrated_to')}")
    return "\n".join(lines)[:3900]


def _v150_history_text(chat_id: int) -> str:
    row = _v150_lifecycle(int(chat_id))
    lines = [f"🧾 ИСТОРИЯ ЧАТА · {get_chat_display_name(int(chat_id))}", ""]
    for item in reversed((row.get("history") or [])[-30:]):
        if not isinstance(item, dict):
            continue
        lines.append(f"{item.get('at')} · {item.get('from')} → {item.get('to')}")
        lines.append(f"Причина: {item.get('reason') or '—'} · источник: {item.get('source') or '—'}")
    if len(lines) == 2:
        lines.append("История пока пуста.")
    return "\n".join(lines)[:3900]


def _v150_target_chat_from_command(msg) -> int:
    parts = str(getattr(msg, "text", "") or "").split(maxsplit=1)
    if len(parts) > 1:
        try: return int(parts[1].strip())
        except Exception: pass
    return int(msg.chat.id)


@bot.message_handler(commands=["chat_status"])
def v150_cmd_chat_status(msg):
    target = _v150_target_chat_from_command(msg)
    uid = int(getattr(getattr(msg, "from_user", None), "id", 0) or 0)
    if not _v150_chat_access(uid, target):
        bot.reply_to(msg, "⛔ Недостаточно прав для этого чата."); return
    bot.reply_to(msg, _v150_status_text(target))


@bot.message_handler(commands=["chat_history"])
def v150_cmd_chat_history(msg):
    target = _v150_target_chat_from_command(msg)
    uid = int(getattr(getattr(msg, "from_user", None), "id", 0) or 0)
    if not _v150_chat_access(uid, target):
        bot.reply_to(msg, "⛔ Недостаточно прав для этого чата."); return
    bot.reply_to(msg, _v150_history_text(target))


@bot.message_handler(commands=["chat_archive"])
def v150_cmd_chat_archive(msg):
    target = _v150_target_chat_from_command(msg)
    uid = int(getattr(getattr(msg, "from_user", None), "id", 0) or 0)
    if not _v150_chat_access(uid, target):
        bot.reply_to(msg, "⛔ Недостаточно прав для этого чата."); return
    set_chat_status_v150(target, "archived", f"manual archive by {uid}", source="command", force_history=True)
    bot.reply_to(msg, "📦 Чат переведён в статус archived. Данные и история сохранены.")


@bot.message_handler(commands=["chat_restore"])
def v150_cmd_chat_restore(msg):
    target = _v150_target_chat_from_command(msg)
    uid = int(getattr(getattr(msg, "from_user", None), "id", 0) or 0)
    if not _v150_chat_access(uid, target):
        bot.reply_to(msg, "⛔ Недостаточно прав для этого чата."); return
    set_chat_status_v150(target, "active", f"manual restore by {uid}", source="command", force_history=True)
    try: GENERAL_TASK_POOL.submit_unique(f"probe-chat-{target}", probe_bot_in_chat, target)
    except Exception: pass
    bot.reply_to(msg, "✅ Карточка чата восстановлена и поставлена на проверку Telegram.")


# Runtime view of the static audit shipped with the release.
@bot.message_handler(commands=["command_audit"])
def v150_cmd_command_audit(msg):
    uid = int(getattr(getattr(msg, "from_user", None), "id", 0) or 0)
    if not tenant_is_platform_owner_user(uid):
        bot.reply_to(msg, "⛔ Команда доступна только владельцу платформы."); return
    bot.reply_to(msg,
        "✅ Slash-команды v150\n\n"
        "• command-декораторов и алиасов: 91; конфликтов между ними: 0;\n"
        "• в меню Telegram зарегистрировано 96 допустимых латинских команд и алиасов;\n"
        "• обычный catch-all исключает сообщения, начинающиеся с /;\n"
        "• изменяющие состояние команды защищены durable receipt;\n"
        "• /vyapl_ID обрабатывается отдельным цифровым маршрутом и не конфликтует с /vyapl_history.\n\n"
        "Полный аудит находится в COMMAND_AUDIT_v150.txt внутри ZIP."
    )


# ─────────────────────────────────────────────────────────────
# Slash commands: durable mutation receipts + Telegram registration.
# ─────────────────────────────────────────────────────────────
_V150_MUTATION_COMMANDS = {
    "/reset", "/stopforward", "/backup_channel_on", "/backup_channel_off", "/buttons",
    "/restore", "/restore_off", "/secret_bot", "/mega_restore_now", "/mega_backup_now",
    "/knopki", "/кнопки", "/mask", "/maska", "/маска", "/day5", "/fin_day5", "/sutki", "/ost", "/остаток", "/старт",
    "/restore_guard_off", "/restore_guard_on", "/off_on_backup_excel",
    "/space_create", "/tenant_create", "/space_claim", "/tenant_claim",
    "/space_join", "/tenant_join", "/space_chat_link", "/tenant_chat_link",
    "/space_user_link", "/tenant_user_link", "/space_role", "/tenant_role",
    "/space_transfer", "/tenant_transfer", "/space_rename", "/tenant_rename",
    "/space_unlink", "/tenant_unlink", "/google_connect", "/google_sheet",
    "/google_drive", "/google_email", "/chat_archive", "/chat_restore",
}


def _v150_is_mutation_command(command: str) -> bool:
    cmd = str(command or "").lower()
    if cmd in _V150_MUTATION_COMMANDS or bool(_v150_re.fullmatch(r"/vyapl(?:_\d+)?", cmd)):
        return True
    # Hidden owner activation changes access rights and therefore must be durable,
    # although it is intentionally not advertised in Telegram's command menu.
    return bool(_v150_re.fullmatch(r"/(?:владелец|vladelec)(?:1904|-1904|_1904)", cmd, flags=_v150_re.I))


def _v150_is_known_slash_command(text: str) -> bool:
    token = str(text or "").strip().split(maxsplit=1)[0].split("@", 1)[0].casefold()
    if not token.startswith("/"):
        return False
    name = token[1:]
    known = {str(cmd).casefold() for cmd, _desc in _V150_TELEGRAM_COMMANDS} if "_V150_TELEGRAM_COMMANDS" in globals() else set()
    known.update({"старт", "кнопки", "маска", "остаток", "секрет", "sekret", "cekret"})
    return name in known or bool(_v150_re.fullmatch(r"(?:vyapl_\d+|izm_[ru]\d+(?:_u[a-f0-9]{12})?|\d+)", name, flags=_v150_re.I))


def durable_task_required(payload: dict) -> tuple[bool, str]:
    command, _chat_id, _msg_id, _uid = _v150_command_from_payload(payload)
    if str(command or "").lower().startswith("/izm_"):
        return False, "v168:record_edit_open"
    if _v150_is_mutation_command(command):
        try:
            if not mega_tasks_active():
                return False, "mega_tasks_inactive"
        except Exception:
            pass
        return True, "v150:mutation_command"
    # In total-secret mode an unknown slash string is intentionally stored as secret
    # content. It must not be downgraded to a noncritical command by the old classifier.
    if command and _chat_id is not None:
        try:
            if is_total_secret_mode(int(_chat_id)) and not _v150_is_known_slash_command(command):
                return True, "v150:total_secret_slash_content"
        except Exception:
            pass
    return _V150_BASE_DURABLE_REQUIRED(payload)


def _v150_receipt_key(payload: dict) -> str:
    command, chat_id, msg_id, user_id = _v150_command_from_payload(payload)
    update_id = (payload or {}).get("update_id")
    return f"{update_id}:{chat_id}:{msg_id}:{user_id}:{command}"


def _durable_expected_effects(payload: dict) -> dict:
    out = _V150_BASE_DURABLE_EXPECTED(payload)
    command, chat_id, msg_id, user_id = _v150_command_from_payload(payload)
    if _v150_is_mutation_command(command):
        out["command_receipt_v150"] = {
            "key": _v150_receipt_key(payload), "command": command,
            "chat_id": chat_id, "message_id": msg_id, "user_id": user_id,
        }
    elif command and chat_id is not None:
        try:
            if is_total_secret_mode(int(chat_id)) and not _v150_is_known_slash_command(command):
                out["source_secret"] = True
        except Exception:
            pass
    return out


def _v150_has_receipt(key: str) -> bool:
    return any(isinstance(x, dict) and str(x.get("key")) == str(key) for x in (data.get("durable_command_receipts_v150") or []))


def _v150_store_receipt(payload: dict):
    command, chat_id, msg_id, user_id = _v150_command_from_payload(payload)
    if not _v150_is_mutation_command(command):
        return
    key = _v150_receipt_key(payload)
    if _v150_has_receipt(key):
        return
    rows = data.setdefault("durable_command_receipts_v150", [])
    rows.append({
        "key": key, "command": command, "chat_id": chat_id,
        "message_id": msg_id, "user_id": user_id, "update_id": payload.get("update_id"),
        "completed_at": _v150_now(),
    })
    del rows[:-800]
    save_data(data, chat_ids=[chat_id] if chat_id is not None else None)


def _v177_legacy_0073_execute_telegram_payload(payload: dict, update_id=None, update_chat_id=None, update_type: str = "other"):
    result = _V150_BASE_EXECUTE_PAYLOAD(payload, update_id=update_id, update_chat_id=update_chat_id, update_type=update_type)
    _v150_store_receipt(payload)
    return result
try: _v177_legacy_0073_execute_telegram_payload.__name__ = '_execute_telegram_payload'
except Exception: pass
_execute_telegram_payload = _v177_legacy_0073_execute_telegram_payload


def _durable_effect_report(payload: dict, expected: dict | None = None) -> dict:
    report = _V150_BASE_DURABLE_REPORT(payload, expected)
    exp = expected if isinstance(expected, dict) else _durable_expected_effects(payload)
    receipt = exp.get("command_receipt_v150") if isinstance(exp, dict) else None
    if isinstance(receipt, dict) and not _v150_has_receipt(str(receipt.get("key") or "")):
        report["complete"] = False
        missing = report.setdefault("missing", [])
        tag = f"command_receipt:{receipt.get('command')}:{receipt.get('message_id')}"
        if tag not in missing:
            missing.append(tag)
    return report


_V150_TELEGRAM_COMMANDS = [
    ('additional_owners', 'Команда /additional_owners'),
    ('articles', 'Статьи расходов'),
    ('backup_channel_off', 'Команда /backup_channel_off'),
    ('backup_channel_on', 'Команда /backup_channel_on'),
    ('balance', 'Текущий баланс'),
    ('bot_errors', 'Команда /bot_errors'),
    ('buttons', 'Команда /buttons'),
    ('chat_archive', 'Архивировать чат'),
    ('chat_history', 'История статусов чата'),
    ('chat_restore', 'Восстановить карточку чата'),
    ('chat_status', 'Статус чата'),
    ('command_audit', 'Аудит slash-команд'),
    ('csv', 'Скачать CSV'),
    ('db', 'Команда /db'),
    ('delta_status', 'Команда /delta_status'),
    ('diag', 'Команда /diag'),
    ('diagnostics', 'Команда /diagnostics'),
    ('dozvon', 'Команда /dozvon'),
    ('errors', 'Команда /errors'),
    ('excel', 'Настройки Excel'),
    ('google', 'Google пространства'),
    ('google_connect', 'Команда /google_connect'),
    ('google_drive', 'Команда /google_drive'),
    ('google_email', 'Команда /google_email'),
    ('google_sheet', 'Команда /google_sheet'),
    ('google_space', 'Команда /google_space'),
    ('google_tenant', 'Команда /google_tenant'),
    ('help', 'Справка по командам'),
    ('journal', 'Команда /journal'),
    ('json', 'Скачать JSON'),
    ('log', 'Команда /log'),
    ('logs', 'Команда /logs'),
    ('mega_backup_now', 'Команда /mega_backup_now'),
    ('mega_restore_now', 'Команда /mega_restore_now'),
    ('mega_status', 'Команда /mega_status'),
    ('next', 'Команда /next'),
    ('off_on_backup_excel', 'Команда /off_on_backup_excel'),
    ('ok', 'Команда /ok'),
    ('okna', 'Команда /okna'),
    ('owners', 'Команда /owners'),
    ('ping', 'Проверка ответа бота'),
    ('prev', 'Команда /prev'),
    ('queue_status', 'Команда /queue_status'),
    ('queues', 'Команда /queues'),
    ('report', 'Финансовый отчёт'),
    ('reset', 'Команда /reset'),
    ('restore', 'Команда /restore'),
    ('restore_guard', 'Команда /restore_guard'),
    ('restore_guard_off', 'Команда /restore_guard_off'),
    ('restore_guard_on', 'Команда /restore_guard_on'),
    ('restore_off', 'Команда /restore_off'),
    ('runtime_export', 'Команда /runtime_export'),
    ('secret_bot', 'Команда /secret_bot'),
    ('space', 'Текущее пространство'),
    ('space_chat_link', 'Команда /space_chat_link'),
    ('space_chats', 'Команда /space_chats'),
    ('space_claim', 'Команда /space_claim'),
    ('space_create', 'Команда /space_create'),
    ('space_join', 'Команда /space_join'),
    ('space_rename', 'Команда /space_rename'),
    ('space_role', 'Команда /space_role'),
    ('space_transfer', 'Команда /space_transfer'),
    ('space_unlink', 'Команда /space_unlink'),
    ('space_user_link', 'Команда /space_user_link'),
    ('space_users', 'Команда /space_users'),
    ('spaces', 'Список пространств'),
    ('sqlite', 'Команда /sqlite'),
    ('start', 'Открыть главное меню'),
    ('stopforward', 'Команда /stopforward'),
    ('tabl_lsx', 'Excel за четыре недели'),
    ('tenant', 'Команда /tenant'),
    ('tenant_chat_link', 'Команда /tenant_chat_link'),
    ('tenant_chats', 'Команда /tenant_chats'),
    ('tenant_claim', 'Команда /tenant_claim'),
    ('tenant_create', 'Команда /tenant_create'),
    ('tenant_join', 'Команда /tenant_join'),
    ('tenant_rename', 'Команда /tenant_rename'),
    ('tenant_role', 'Команда /tenant_role'),
    ('tenant_transfer', 'Команда /tenant_transfer'),
    ('tenant_unlink', 'Команда /tenant_unlink'),
    ('tenant_user_link', 'Команда /tenant_user_link'),
    ('tenant_users', 'Команда /tenant_users'),
    ('vyapl', 'Выполнить напоминание'),
    ('vyapl_history', 'История выполнения напоминаний'),
    ('windows', 'Команда /windows'),
    ('xlsx', 'Скачать Excel'),    ('knopki', 'Переключить вид кнопок'),
    ('mask', 'Маскировка секретного режима'),
    ('maska', 'Маскировка секретного режима'),
    ('day5', 'Финансовые сутки с 05:00'),
    ('fin_day5', 'Финансовые сутки с 05:00'),
    ('sutki', 'Финансовые сутки с 05:00'),
    ('ost', 'Переключить подпись остатка'),
    ('secret', 'Открыть секретные записи'),
    ('sekret', 'Открыть секретные записи'),
    ('cekret', 'Открыть секретные записи'),

]


def _v150_register_commands():
    try:
        commands = [types.BotCommand(name, description[:256]) for name, description in _V150_TELEGRAM_COMMANDS]
        bot.set_my_commands(commands)
        try: bot_journal("slash_commands_registered", int(OWNER_ID or 0), f"count={len(commands)}")
        except Exception: pass
        return True
    except Exception as exc:
        try: log_error(f"v150 set_my_commands: {exc}")
        except Exception: pass
        return False


def _v150_migrate_chat_lifecycle() -> int:
    count = 0
    for cid, store in list((data.get("chats") or {}).items()):
        if not isinstance(store, dict):
            continue
        try:
            chat_id = int(cid)
            before = store.get("chat_lifecycle_v150")
            _v150_lifecycle(chat_id)
            if not isinstance(before, dict):
                count += 1
        except Exception:
            continue
    if count:
        save_data(data)
    return count


def _v177_legacy_0270_set_webhook():
    global restore_mode
    try:
        _saved_restore_chat = data.get("_restore_mode_chat_v150")
        restore_mode = int(_saved_restore_chat) if _saved_restore_chat is not None else None
        migrated = _v150_migrate_chat_lifecycle()
        repaired = _v150_repair_pending_rebalances()
        try: bot_journal("v150_startup_migration", int(OWNER_ID or 0), f"chat_lifecycle={migrated}; rebalance_repaired={repaired}")
        except Exception: pass
    except Exception as exc:
        try: log_error(f"v150 startup migration: {exc}")
        except Exception: pass
    result = _V150_BASE_SET_WEBHOOK()
    _v150_register_commands()
    return result
try: _v177_legacy_0270_set_webhook.__name__ = 'set_webhook'
except Exception: pass
set_webhook = _v177_legacy_0270_set_webhook

# v178_global_performance_final
