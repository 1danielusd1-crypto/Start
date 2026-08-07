# v156_process_status_usd_excel
"""v156: persistent visual process status + strict USD-only Excel data/description isolation."""

import copy as _v156_copy
import gzip as _v156_gzip
import json as _v156_json
import os as _v156_os
import re as _v156_re
import shutil as _v156_shutil
import sqlite3 as _v156_sqlite3
import tempfile as _v156_tempfile
import threading as _v156_threading
import time as _v156_time

VERSION = "bot_v156_process_status_usd_excel"

# ---------------------------------------------------------------------------
# Visual process status. Telegram callback toasts have a platform-controlled
# lifetime, so long-running work uses one small bot message that is updated
# until the real operation leaves the active process registry.
# ---------------------------------------------------------------------------
_V156_PROCESS_UI_LOCK = _v156_threading.RLock()
_V156_PROCESS_UI = {}
_V156_PROCESS_STATUS_DELAY = 0.8
_V156_PROCESS_STATUS_INTERVAL = 2.0
_V156_PROCESS_STATUS_KEY_PREFIX = "v156-process-status:"


def process_visual_status_enabled(chat_id: int) -> bool:
    try:
        settings = get_chat_store(int(chat_id)).setdefault("settings", {})
        return bool(settings.get("process_visual_status_enabled", True))
    except Exception:
        return True


def set_process_visual_status_enabled(chat_id: int, enabled: bool) -> bool:
    chat_id = int(chat_id)
    settings = get_chat_store(chat_id).setdefault("settings", {})
    settings["process_visual_status_enabled"] = bool(enabled)
    try:
        save_data(data, chat_ids=[chat_id])
        schedule_config_backup_for_chats(chat_id, delay=0.5)
    except Exception:
        pass
    if not enabled:
        _v156_process_status_clear(chat_id, delete=True)
    try:
        bot_journal("process_visual_status_toggle", chat_id, f"enabled={int(bool(enabled))}")
    except Exception:
        pass
    return bool(enabled)


def toggle_process_visual_status(chat_id: int) -> bool:
    return set_process_visual_status_enabled(int(chat_id), not process_visual_status_enabled(int(chat_id)))


def process_visual_status_label(chat_id: int) -> str:
    return f"👁 Окно процессов: {'ВКЛ' if process_visual_status_enabled(int(chat_id)) else 'ВЫКЛ'}"


def _v156_process_status_clear(chat_id: int, delete: bool = False) -> None:
    chat_id = int(chat_id)
    try:
        DELAYED_SCHEDULER.cancel(f"{_V156_PROCESS_STATUS_KEY_PREFIX}{chat_id}")
    except Exception:
        pass
    with _V156_PROCESS_UI_LOCK:
        state = _V156_PROCESS_UI.pop(chat_id, None) or {}
    msg_id = int(state.get("message_id") or 0)
    if delete and msg_id:
        try:
            bot.delete_message(chat_id, msg_id)
        except Exception:
            pass


def _v156_active_process_rows(chat_id: int) -> list[dict]:
    """Only real registered operations for this chat; no unrelated global pools."""
    rows = []
    try:
        lock = globals().get("_PROCESS_CENTER_LOCK")
        runtime = globals().get("_PROCESS_RUNTIME") or {}
        if lock is not None:
            with lock:
                active = list((runtime.get("active") or {}).values())
        else:
            active = list((runtime.get("active") or {}).values())
        for row in active:
            try:
                if int((row or {}).get("chat_id") or 0) != int(chat_id):
                    continue
            except Exception:
                continue
            rows.append(_v156_copy.deepcopy(row))
    except Exception:
        pass
    rows.sort(key=lambda r: float((r or {}).get("started_mono") or 0.0))
    return rows


def _v156_process_status_text(chat_id: int, rows: list[dict], hint: str = "") -> str:
    now_m = _v156_time.monotonic()
    lines = ["⏳ Операция выполняется"]
    if hint:
        lines.append(f"Действие: {str(hint)[:100]}")
    lines.append("")
    for row in rows[:6]:
        started = float((row or {}).get("started_mono") or now_m)
        elapsed = max(0, int(now_m - started))
        label = str((row or {}).get("label") or (row or {}).get("id") or "Операция")
        phase = str((row or {}).get("phase") or "выполняется")
        lines.append(f"• {label}: {phase} · {elapsed}с")
    if len(rows) > 6:
        lines.append(f"…ещё {len(rows) - 6}")
    lines.extend(["", "Окно закроется после завершения процесса."])
    return "\n".join(lines)[:3900]


def _v156_process_status_schedule(chat_id: int, delay: float) -> None:
    try:
        key = f"{_V156_PROCESS_STATUS_KEY_PREFIX}{int(chat_id)}"
        DELAYED_SCHEDULER.cancel(key)
        DELAYED_SCHEDULER.schedule(key, max(0.05, float(delay)), _v156_process_status_tick, int(chat_id))
    except Exception:
        pass


def _v156_process_status_arm(chat_id: int | None, hint: str = "") -> None:
    try:
        chat_id = int(chat_id or 0)
    except Exception:
        return
    if not chat_id or not process_visual_status_enabled(chat_id):
        return
    with _V156_PROCESS_UI_LOCK:
        state = _V156_PROCESS_UI.setdefault(chat_id, {"message_id": 0, "hint": "", "armed_at": _v156_time.monotonic()})
        if hint:
            state["hint"] = str(hint)[:120]
        state["armed_at"] = min(float(state.get("armed_at") or _v156_time.monotonic()), _v156_time.monotonic())
    _v156_process_status_schedule(chat_id, _V156_PROCESS_STATUS_DELAY)


def _v156_process_status_tick(chat_id: int) -> None:
    chat_id = int(chat_id)
    if not process_visual_status_enabled(chat_id):
        _v156_process_status_clear(chat_id, delete=True)
        return
    rows = _v156_active_process_rows(chat_id)
    with _V156_PROCESS_UI_LOCK:
        state = _V156_PROCESS_UI.get(chat_id) or {}
        msg_id = int(state.get("message_id") or 0)
        hint = str(state.get("hint") or "")
    if not rows:
        if msg_id:
            try:
                bot.edit_message_text(
                    f"✅ {hint or 'Операция'}\nВыполнено.",
                    chat_id=chat_id,
                    message_id=msg_id,
                )
                delete_message_later(chat_id, msg_id, 4)
            except Exception:
                pass
        _v156_process_status_clear(chat_id, delete=False)
        return

    text = _v156_process_status_text(chat_id, rows, hint)
    if msg_id:
        try:
            bot.edit_message_text(text, chat_id=chat_id, message_id=msg_id)
        except Exception as exc:
            low = str(exc).casefold()
            if "message is not modified" not in low:
                try:
                    bot_journal("process_visual_status_edit_error", chat_id, str(exc)[:300], "WARN")
                except Exception:
                    pass
    else:
        try:
            sent = bot.send_message(chat_id, text)
            new_id = int(getattr(sent, "message_id", 0) or 0)
            if new_id:
                with _V156_PROCESS_UI_LOCK:
                    if chat_id in _V156_PROCESS_UI:
                        _V156_PROCESS_UI[chat_id]["message_id"] = new_id
        except Exception:
            pass
    _v156_process_status_schedule(chat_id, _V156_PROCESS_STATUS_INTERVAL)


_V156_ORIG_PROCESS_REGISTER = globals().get("process_register")
if callable(_V156_ORIG_PROCESS_REGISTER):
    def process_register(process_id: str, label: str, chat_id=None, phase: str = "ожидает", cancellable: bool = False, meta: dict | None = None):
        result = _V156_ORIG_PROCESS_REGISTER(process_id, label, chat_id, phase=phase, cancellable=cancellable, meta=meta)
        _v156_process_status_arm(chat_id, str(label or "Операция"))
        return result


_V156_ORIG_PROCESS_UPDATE = globals().get("process_update")
if callable(_V156_ORIG_PROCESS_UPDATE):
    def process_update(process_id: str, phase: str | None = None, details: str = ""):
        result = _V156_ORIG_PROCESS_UPDATE(process_id, phase=phase, details=details)
        try:
            lock = globals().get("_PROCESS_CENTER_LOCK")
            runtime = globals().get("_PROCESS_RUNTIME") or {}
            if lock is not None:
                with lock:
                    row = _v156_copy.deepcopy((runtime.get("active") or {}).get(str(process_id)) or {})
            else:
                row = _v156_copy.deepcopy((runtime.get("active") or {}).get(str(process_id)) or {})
            if row:
                _v156_process_status_arm(row.get("chat_id"), row.get("label") or "Операция")
        except Exception:
            pass
        return result


_V156_ORIG_PROCESS_FINISH = globals().get("process_finish")
if callable(_V156_ORIG_PROCESS_FINISH):
    def process_finish(process_id: str, ok: bool | None = True, details: str = ""):
        chat_id = 0
        label = "Операция"
        try:
            lock = globals().get("_PROCESS_CENTER_LOCK")
            runtime = globals().get("_PROCESS_RUNTIME") or {}
            if lock is not None:
                with lock:
                    row = _v156_copy.deepcopy((runtime.get("active") or {}).get(str(process_id)) or {})
            else:
                row = _v156_copy.deepcopy((runtime.get("active") or {}).get(str(process_id)) or {})
            chat_id = int(row.get("chat_id") or 0)
            label = str(row.get("label") or label)
        except Exception:
            pass
        result = _V156_ORIG_PROCESS_FINISH(process_id, ok=ok, details=details)
        if chat_id:
            _v156_process_status_arm(chat_id, label)
            _v156_process_status_schedule(chat_id, 0.08)
        return result


# Interactive files already had a good progress message. Make that message obey
# the same INFO switch instead of always showing it.
def submit_interactive_file_job(chat_id: int, kind: str, label: str, func, *args, **kwargs) -> tuple[bool, str]:
    chat_id = int(chat_id)
    gate = globals().get("memory_heavy_allowed")
    if callable(gate):
        try:
            allowed, reason = gate(str(kind or "export"))
        except Exception:
            allowed, reason = True, ""
        if not allowed:
            try:
                send_and_auto_delete(chat_id, f"🧠 {reason}", 15)
            except Exception:
                pass
            return False, reason or "сервер временно разгружает память"
    key = _INTERACTIVE_FILE_JOB_KEY
    with _FILE_JOB_LOCK:
        existing = _FILE_JOB_STATE.get(key)
        if isinstance(existing, dict):
            return False, build_all_processes_toast(chat_id)
        meta = {
            "key": key,
            "chat_id": chat_id,
            "kind": str(kind),
            "label": str(label),
            "queued_monotonic": _v156_time.monotonic(),
            "started_monotonic": 0.0,
            "phase": "в очереди",
            "status_msg_id": None,
            "last_ui_monotonic": 0.0,
        }
        _FILE_JOB_STATE[key] = meta
    if process_visual_status_enabled(chat_id):
        try:
            status = bot.send_message(chat_id, f"⏳ {label}\nВремя: 0:00\nЭтап: в очереди\nОкно останется до завершения операции.")
            with _FILE_JOB_LOCK:
                if isinstance(_FILE_JOB_STATE.get(key), dict):
                    _FILE_JOB_STATE[key]["status_msg_id"] = int(getattr(status, "message_id", 0) or 0) or None
        except Exception:
            pass
    ok = EXPORT_TASK_POOL.submit_unique(key, _interactive_file_job_runner, dict(meta), func, args, kwargs)
    if not ok:
        with _FILE_JOB_LOCK:
            _FILE_JOB_STATE.pop(key, None)
        return False, build_all_processes_toast(chat_id)
    try:
        DELAYED_SCHEDULER.cancel(f"file-job-tick:{key}")
        DELAYED_SCHEDULER.schedule(f"file-job-tick:{key}", internal_timer_seconds("process_status_refresh", 10.0), _file_job_tick, key)
    except Exception:
        pass
    try:
        bot_journal("file_job_queued", chat_id, f"kind={kind} label={label}; visual={int(process_visual_status_enabled(chat_id))}")
    except Exception:
        pass
    return True, "Запущено"


# INFO switch.
_V156_ORIG_BUILD_INFO_KEYBOARD = globals().get("build_info_keyboard")
if callable(_V156_ORIG_BUILD_INFO_KEYBOARD):
    def build_info_keyboard(chat_id: int):
        kb = _V156_ORIG_BUILD_INFO_KEYBOARD(int(chat_id))
        try:
            button = IB(process_visual_status_label(int(chat_id)), callback_data="v156:process_visual_toggle")
            rows = list(getattr(kb, "keyboard", []) or [])
            insert_at = None
            for idx, row in enumerate(rows):
                if any(str(getattr(x, "callback_data", "") or "") == "process_center" for x in row or []):
                    insert_at = idx + 1
                    break
            if insert_at is None:
                insert_at = max(0, len(rows) - 1)
            rows.insert(insert_at, [button])
            kb.keyboard = rows
        except Exception:
            try:
                kb.row(IB(process_visual_status_label(int(chat_id)), callback_data="v156:process_visual_toggle"))
            except Exception:
                pass
        return kb


_V156_ORIG_BUILD_INFO_TEXT = globals().get("build_info_text")
if callable(_V156_ORIG_BUILD_INFO_TEXT):
    def build_info_text(chat_id: int) -> str:
        text = str(_V156_ORIG_BUILD_INFO_TEXT(int(chat_id)) or "")
        line = f"Окно процессов: {'ВКЛ' if process_visual_status_enabled(int(chat_id)) else 'ВЫКЛ'}"
        rows = text.splitlines()
        try:
            idx = rows.index("Слеш-команды:")
            rows[idx:idx] = [line, ""]
        except Exception:
            rows.extend(["", line])
        return "\n".join(rows)[:3900]


def _v156_handle_process_toggle(call) -> bool:
    raw = str(getattr(call, "data", "") or "")
    resolved = raw
    try:
        resolver = globals().get("resolve_short_callback")
        if callable(resolver):
            resolved = str(resolver(raw) or raw)
    except Exception:
        pass
    if resolved != "v156:process_visual_toggle":
        return False
    try:
        chat_id = int(call.message.chat.id)
        enabled = toggle_process_visual_status(chat_id)
        safe_edit(bot, call, build_info_text(chat_id), reply_markup=build_info_keyboard(chat_id))
        try:
            bot.answer_callback_query(call.id, f"Окно процессов: {'ВКЛ' if enabled else 'ВЫКЛ'}")
        except Exception:
            pass
        return True
    except Exception as exc:
        try:
            log_error(f"v156 process visual toggle: {exc}")
        except Exception:
            pass
        return True


def _v156_install_callback_intercept() -> int:
    count = 0
    for handler in list(getattr(bot, "callback_query_handlers", []) or []):
        if not isinstance(handler, dict):
            continue
        original = handler.get("function")
        if not callable(original) or getattr(original, "_v156_intercept", False):
            continue
        def _wrapped(call, _original=original):
            if _v156_handle_process_toggle(call):
                return None
            return _original(call)
        _wrapped._v156_intercept = True
        _wrapped.__name__ = getattr(original, "__name__", "callback_handler")
        handler["function"] = _wrapped
        count += 1
    return count


try:
    WINDOW_MARKER_CONSTANTS["v156:process_visual_toggle"] = "Ф9"
except Exception:
    pass


# ---------------------------------------------------------------------------
# Strict USD Excel isolation for every XLSX row builder patched in v151/v154.
# Independent USD ledger + explicit USD component are allowed. ARS descriptions
# never become the fallback description of a USD row.
# ---------------------------------------------------------------------------

def _v156_store_ledgers(chat_id: int) -> tuple[dict, str, list[dict], list[dict]]:
    store = get_chat_store(int(chat_id))
    settings = store.setdefault("settings", {})
    active = str(settings.get("_active_currency_ledger") or "").strip().lower()
    if active not in {"ars", "usd"}:
        try:
            active = str(_ensure_currency_ledgers(store) or "ars").lower()
        except Exception:
            active = "ars"
    ars = list((store.get("records") if active == "ars" else store.get("ars_records")) or [])
    usd = list((store.get("records") if active == "usd" else store.get("usd_records")) or [])
    return store, active, ars, usd


def _v156_record_identity(rec: dict) -> tuple:
    op = str((rec or {}).get("operation_key") or "").strip()
    if op:
        return ("op", op)
    try:
        mid = int((rec or {}).get("source_msg_id") or 0)
    except Exception:
        mid = 0
    if mid:
        return ("msg", mid)
    return (
        "fp",
        str((rec or {}).get("day_key") or _v151_day_key(rec))[:10],
        str((rec or {}).get("timestamp") or "")[:32],
        int((rec or {}).get("id") or 0),
    )


def _v156_record_fingerprint(rec: dict) -> tuple:
    return (
        str((rec or {}).get("day_key") or _v151_day_key(rec))[:10],
        round(float((rec or {}).get("amount") or 0.0), 8),
        str((rec or {}).get("note") or "").strip().casefold(),
        int((rec or {}).get("source_msg_id") or 0),
        int((rec or {}).get("source_order_msg_id") or 0),
    )


def _v156_explicit_currency(rec: dict) -> str:
    raw = str((rec or {}).get("currency") or "").strip().casefold()
    if raw in {"usd", "$", "us$", "u$s"}:
        return "usd"
    if raw in {"ars", "peso", "pesos"}:
        return "ars"
    return ""


def _v156_clean_embedded_usd_description(rec: dict) -> str:
    """Extract USD-side description without ever falling back to the ARS note."""
    source = str((rec or {}).get("source_finance_text") or "").strip()
    if source:
        try:
            info = extract_usd_transaction(source)
        except Exception:
            info = None
        if info and info.get("span"):
            try:
                _start, end = info.get("span")
                after = source[int(end):].strip(" \t:;,-–—|/")
                # Text immediately after the USD fragment is the safest association.
                if after:
                    after = _v156_re.sub(r"(?i)\b(?:ars|pesos?|peso)\b", " ", after)
                    after = _v156_re.sub(r"\s+", " ", after).strip(" :;,-–—|/")
                    if after and not _v156_re.fullmatch(r"[\d\s.,+'\-]+", after):
                        return after.lower()[:220]
            except Exception:
                pass
    usd_note = str((rec or {}).get("usd_note") or "").strip().lower()
    ars_note = str((rec or {}).get("note") or "").strip().lower()
    clean = usd_note
    if clean and ars_note:
        if clean == ars_note:
            clean = ""
        elif ars_note in clean:
            clean = clean.replace(ars_note, " ", 1)
    clean = _v156_re.sub(r"(?i)\b(?:ars|pesos?|peso)\b", " ", clean)
    clean = _v156_re.sub(r"(?<!\w)[+\-]?\d[\d\s.,_'’]*(?!\w)", " ", clean)
    clean = _v156_re.sub(r"\s+", " ", clean).strip(" :;,-–—|/")
    return (clean or "USD операция")[:220]


def _v151_usd_records(chat_id: int) -> list[dict]:
    """v156 source of truth for XLSX USD rows: USD-only data, no ARS note fallback."""
    store, active, ars_source, usd_source = _v156_store_ledgers(int(chat_id))
    ars_ids = {_v156_record_identity(r) for r in ars_source if isinstance(r, dict)}
    ars_fps = {_v156_record_fingerprint(r) for r in ars_source if isinstance(r, dict)}
    rows = []
    seen = set()
    filtered = 0

    # 1) Independent USD ledger. Explicit USD tag wins; untagged exact ARS copies are contamination.
    for rec in usd_source:
        if not isinstance(rec, dict):
            continue
        currency = _v156_explicit_currency(rec)
        ident = _v156_record_identity(rec)
        fp = _v156_record_fingerprint(rec)
        if currency == "ars":
            filtered += 1
            continue
        if currency != "usd" and (ident in ars_ids or fp in ars_fps):
            filtered += 1
            continue
        if ident in seen:
            continue
        item = dict(rec)
        item["_v151_amount"] = _v151_float(rec.get("amount"))
        # Independent USD rows use their own USD ledger description only.
        item["_v151_note"] = str(rec.get("note") or rec.get("usd_note") or "USD операция").strip()
        item["_v151_currency"] = "usd"
        item["_v156_source"] = "usd_ledger"
        rows.append(item)
        seen.add(ident)

    # 2) Explicit USD components carried by ARS records. They use usd_amount and a USD-only
    # description extractor; the ARS note is NEVER used as fallback.
    for rec in ars_source:
        if not isinstance(rec, dict) or rec.get("usd_amount") is None:
            continue
        usd_amount = _v151_float(rec.get("usd_amount"))
        if abs(usd_amount) <= 1e-12:
            continue
        ident = _v156_record_identity(rec)
        if ident in seen:
            continue
        item = dict(rec)
        item["_v151_amount"] = usd_amount
        item["_v151_note"] = _v156_clean_embedded_usd_description(rec)
        item["_v151_currency"] = "usd"
        item["_v156_source"] = "explicit_usd_component"
        rows.append(item)
        seen.add(ident)

    try:
        rows = sorted(rows, key=record_sort_key)
    except Exception:
        pass
    if filtered:
        try:
            bot_journal("excel_usd_ars_duplicates_filtered", int(chat_id), f"count={filtered}; active={active}")
        except Exception:
            pass
    return rows


# Tag active ledgers on every future snapshot/load so new data remains unambiguous.
_V156_ORIG_SNAPSHOT_LEDGER = globals().get("_snapshot_active_currency_ledger")
if callable(_V156_ORIG_SNAPSHOT_LEDGER):
    def _snapshot_active_currency_ledger(store: dict, ledger: str | None = None) -> None:
        ledger = str(ledger or (store.setdefault("settings", {}).get("_active_currency_ledger") or "ars")).lower()
        ledger = "usd" if ledger == "usd" else "ars"
        try:
            for rec in store.get("records", []) or []:
                if isinstance(rec, dict):
                    rec["currency"] = ledger.upper()
        except Exception:
            pass
        return _V156_ORIG_SNAPSHOT_LEDGER(store, ledger)


_V156_ORIG_LOAD_LEDGER = globals().get("_load_currency_ledger")
if callable(_V156_ORIG_LOAD_LEDGER):
    def _load_currency_ledger(store: dict, ledger: str) -> None:
        ledger = "usd" if str(ledger).lower() == "usd" else "ars"
        result = _V156_ORIG_LOAD_LEDGER(store, ledger)
        try:
            for rec in store.get("records", []) or []:
                if isinstance(rec, dict):
                    rec["currency"] = ledger.upper()
        except Exception:
            pass
        return result


# v156 full-state exports must be restorable by the same release.
def _v153_validate_restore_gz(gz_path: str) -> tuple[dict, str]:
    folder = _v156_tempfile.mkdtemp(prefix="v156_restore_validate_")
    raw = _v156_os.path.join(folder, "restore.sqlite3")
    try:
        with _v156_gzip.open(gz_path, "rb") as fin, open(raw, "wb") as fout:
            _v156_shutil.copyfileobj(fin, fout, 1024 * 1024)
        conn = _v156_sqlite3.connect(raw)
        try:
            integrity = str(conn.execute("PRAGMA integrity_check").fetchone()[0])
            if integrity.lower() != "ok":
                raise RuntimeError(f"SQLite integrity_check: {integrity}")
            row = conn.execute("SELECT v FROM meta WHERE kind='v153_export' AND k='manifest'").fetchone()
            if not row:
                raise RuntimeError("manifest v153 not found")
            manifest = _v156_json.loads(row[0])
        finally:
            conn.close()
        if str(manifest.get("kind")) != "telegram_bot_full_state_v153":
            raise RuntimeError("unknown export kind")
        if int(manifest.get("schema_version") or 0) != int(V153_EXPORT_SCHEMA):
            raise RuntimeError("unsupported export schema")
        export_version = str(manifest.get("bot_version") or "")
        if not export_version.startswith(("bot_v153_", "bot_v154_", "bot_v155_", "bot_v156_")):
            raise RuntimeError(f"unsupported bot version: {export_version or 'missing'}")
        checksum = _v153_db_logical_checksum(raw)
        if checksum != str(manifest.get("checksum") or ""):
            raise RuntimeError("checksum mismatch")
        return manifest, raw
    except Exception:
        _v156_shutil.rmtree(folder, ignore_errors=True)
        raise


_V156_CALLBACK_INTERCEPTS = _v156_install_callback_intercept()
try:
    bot_journal(
        "v156_process_status_usd_excel_installed",
        int(OWNER_ID or 0),
        f"process_ui=1; callback_intercepts={_V156_CALLBACK_INTERCEPTS}; strict_usd_excel=1; ars_note_fallback=0",
    )
except Exception:
    pass

# v156_process_status_usd_excel
