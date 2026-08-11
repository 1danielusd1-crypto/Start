# v178_global_performance_final
"""v161: deterministic buttons/navigation, parallel-window stability, file-only progress UI, exact window tokens."""

import gzip as _v161_gzip
import json as _v161_json
import os as _v161_os
import re as _v161_re
import secrets as _v161_secrets
import shutil as _v161_shutil
import sqlite3 as _v161_sqlite3
import tempfile as _v161_tempfile
import threading as _v161_threading
import time as _v161_time

VERSION = "bot_v161_button_window_stability"

# 1. Ф232 is forbidden for ordinary telegram_update/background work. Ф233 stays for real file jobs.
try:
    INTERNAL_TIMER_DEFS.pop("helper_process_close", None)
    if "process_status_refresh" in INTERNAL_TIMER_DEFS:
        INTERNAL_TIMER_DEFS["process_status_refresh"]["label"] = "📥📤 Обновление времени / этапа файлов Ф233"
    if "file_status_close" in INTERNAL_TIMER_DEFS:
        INTERNAL_TIMER_DEFS["file_status_close"]["label"] = "📥📤 Закрытие окна файла Ф233 после завершения"
except Exception:
    pass
try:
    if isinstance(WINDOW_MARKER_CLOCK_CODES, set):
        WINDOW_MARKER_CLOCK_CODES.discard("Ф232")
        WINDOW_MARKER_CLOCK_CODES.add("Ф233")
except Exception:
    pass


def process_visual_status_enabled(chat_id: int) -> bool:
    return False


def _v156_process_status_arm(chat_id: int | None, hint: str = "") -> None:
    return None


def _v156_process_status_schedule(chat_id: int, delay: float) -> None:
    try:
        DELAYED_SCHEDULER.cancel(f"{_V156_PROCESS_STATUS_KEY_PREFIX}{int(chat_id)}")
    except Exception:
        pass


def _v156_process_status_tick(chat_id: int) -> None:
    try:
        chat_id = int(chat_id)
    except Exception:
        return
    msg_id = 0
    try:
        with _V156_PROCESS_UI_LOCK:
            row = _V156_PROCESS_UI.pop(chat_id, None) or {}
            msg_id = int(row.get("message_id") or 0)
    except Exception:
        pass
    if msg_id:
        try: bot.delete_message(chat_id, msg_id)
        except Exception: pass
        try: unregister_open_window(chat_id, msg_id)
        except Exception: pass


# 2. Helper/file auto-delete gets up to 3 real attempts and is independent from overloaded delayed queue.
_V161_DELETE_LOCK = _v161_threading.RLock()
_V161_DELETE_STATE = {}


def _v161_delete_already_gone(exc) -> bool:
    low = str(exc or "").casefold()
    return any(x in low for x in (
        "message to delete not found", "message not found", "message_id_invalid",
        "message identifier is not specified",
    ))


def _v161_cleanup_deleted_state(chat_id: int, message_id: int) -> None:
    try: unregister_open_window(int(chat_id), int(message_id))
    except Exception: pass
    try:
        store = get_chat_store(int(chat_id))
        changed = False
        for key in ("_v160_file_status_msg_id", "command_window_id"):
            try:
                if int(store.get(key) or 0) == int(message_id):
                    store[key] = None
                    changed = True
            except Exception:
                pass
        if changed:
            save_data(data, chat_ids=[int(chat_id)])
            try: schedule_quick_backup(int(chat_id), 0.5)
            except Exception: pass
    except Exception:
        pass


def _v161_delete_attempt(chat_id: int, message_id: int, retry_key: str, attempt: int = 1, max_attempts: int = 3) -> None:
    chat_id = int(chat_id); message_id = int(message_id); attempt = int(attempt)
    ok = False; err = ""
    try:
        bot.delete_message(chat_id, message_id)
        ok = True
    except Exception as exc:
        err = str(exc)[:260]
        ok = _v161_delete_already_gone(exc)
    if ok:
        _v161_cleanup_deleted_state(chat_id, message_id)
        with _V161_DELETE_LOCK: _V161_DELETE_STATE.pop(str(retry_key), None)
        try: bot_journal("helper_delete_done", chat_id, f"msg={message_id}; attempt={attempt}")
        except Exception: pass
        return
    if attempt >= int(max_attempts):
        with _V161_DELETE_LOCK: _V161_DELETE_STATE.pop(str(retry_key), None)
        try: bot_journal("helper_delete_failed", chat_id, f"msg={message_id}; attempts={attempt}; error={err}", "WARN")
        except Exception: pass
        return
    wait = 0.8 if attempt == 1 else 2.0
    with _V161_DELETE_LOCK: _V161_DELETE_STATE[str(retry_key)] = attempt + 1
    try:
        _v160_schedule(str(retry_key), wait, _v161_delete_attempt, chat_id, message_id, str(retry_key), attempt + 1, max_attempts)
    except Exception:
        t = _v161_threading.Timer(wait, _v161_delete_attempt, args=(chat_id, message_id, str(retry_key), attempt + 1, max_attempts))
        t.daemon = True; t.start()


def _v160_delete_quiet(chat_id: int, message_id: int) -> None:
    key = f"v161:delete:{int(chat_id)}:{int(message_id)}"
    _v161_delete_attempt(int(chat_id), int(message_id), key, 1, 3)


def _v160_schedule_delete(chat_id: int, message_id: int, delay: float, prefix: str = "delete") -> None:
    key = f"v161:{str(prefix)}:{int(chat_id)}:{int(message_id)}"
    wait = max(0.05, float(delay))
    try:
        _v160_schedule(key, wait, _v161_delete_attempt, int(chat_id), int(message_id), key, 1, 3)
    except Exception:
        t = _v161_threading.Timer(wait, _v161_delete_attempt, args=(int(chat_id), int(message_id), key, 1, 3))
        t.daemon = True; t.start()


# 3. Background finance refresh must not repaint an opened INFO/reminders/menu back to Ф91.
_V161_FORCE_MAIN = _v161_threading.local()
_V161_PREV_BACKUP_WINDOW = globals().get("backup_window_for_owner")
_V161_PREV_UPDATE_OR_SEND = globals().get("update_or_send_day_window")


def _v161_callback_data() -> str:
    try:
        return str((_current_telegram_update_context() or {}).get("callback_data") or "")
    except Exception:
        return ""


def _v161_explicit_main_action() -> bool:
    if bool(getattr(_V161_FORCE_MAIN, "value", False)):
        return True
    raw = _v161_callback_data()
    if raw == "nav_prev" or raw.endswith(":back_main"):
        return True
    if raw.startswith("d:"):
        try: cmd = raw.split(":", 2)[2]
        except Exception: cmd = ""
        if cmd in {"open", "prev", "next", "today"}:
            return True
    return False


def _v161_window_is_auxiliary(chat_id: int, message_id: int) -> bool:
    if not message_id:
        return False
    # v160 records the marker of every successful send/edit even when an old handler
    # forgot to update the open-window registry. Prefer that live marker.
    try:
        with _V160_ANNOTATION_LOCK:
            meta = dict(_V160_LAST_WINDOW_META.get((int(chat_id), int(message_id))) or {})
        marker = str(meta.get("marker") or "").upper()
        if marker:
            return marker != "Ф91"
    except Exception:
        pass
    try: row = get_registered_open_window(int(chat_id), int(message_id)) or {}
    except Exception: row = {}
    if not row:
        return False
    return str(row.get("window_type") or "") != "main_day"


def backup_window_for_owner(chat_id: int, day_key: str, message_id_override: int | None = None):
    chat_id = int(chat_id); day_key = str(day_key)[:10]
    try: mid = int(message_id_override or get_active_window_id(chat_id, day_key) or 0)
    except Exception: mid = 0
    if mid and not _v161_explicit_main_action() and _v161_window_is_auxiliary(chat_id, mid):
        try: bot_journal("main_refresh_deferred_aux_window", chat_id, f"msg={mid}; day={day_key}")
        except Exception: pass
        return False
    if callable(_V161_PREV_BACKUP_WINDOW):
        return _V161_PREV_BACKUP_WINDOW(chat_id, day_key, message_id_override=message_id_override)
    return False


def update_or_send_day_window(chat_id: int, day_key: str):
    chat_id = int(chat_id); day_key = str(day_key)[:10]
    try: mid = int(get_active_window_id(chat_id, day_key) or 0)
    except Exception: mid = 0
    if mid and not _v161_explicit_main_action() and _v161_window_is_auxiliary(chat_id, mid):
        try: bot_journal("day_window_refresh_deferred_aux_window", chat_id, f"msg={mid}; day={day_key}")
        except Exception: pass
        return False
    if callable(_V161_PREV_UPDATE_OR_SEND):
        return _V161_PREV_UPDATE_OR_SEND(chat_id, day_key)
    return False


# 4. General safe edit: a button is successful only after real edit=ok; no silent scheduled/rate-limit success.
def _v177_legacy_0324_v161_edit_retry(chat_id: int, message_id: int, text: str, reply_markup=None, parse_mode=None, purpose: str = "ui") -> str:
    last = "failed"
    for attempt in range(3):
        try:
            result = fast_ui_edit_message_text(int(chat_id), int(message_id), text, reply_markup=reply_markup, parse_mode=parse_mode, purpose=purpose)
        except Exception:
            result = "failed"
        last = str(result or "failed")
        if last == "ok": return "ok"
        if last == "not_found": return "not_found"
        if attempt < 2:
            _v161_time.sleep(0.15 if last != "rate_limited" else (0.35 if attempt == 0 else 0.65))
    return last
try: _v177_legacy_0324_v161_edit_retry.__name__ = '_v161_edit_retry'
except Exception: pass
_v161_edit_retry = _v177_legacy_0324_v161_edit_retry


def _v177_safe_edit_fallback_send(bot_obj, chat_id: int, msg_id: int, raw_action: str, text: str, reply_markup=None, parse_mode=None):
    """Create a replacement window outside the callback thread when the old Telegram message is unusable."""
    started = _v161_time.monotonic()
    try:
        sent = _tg_call_retry(
            bot_obj.send_message, int(chat_id), text, reply_markup=reply_markup,
            parse_mode=parse_mode, attempts=1, purpose="safe_edit_fallback_v177",
        )
        try: _touch_v98_auto_close_for_callback(int(chat_id), int(sent.message_id), raw_action)
        except Exception: pass
        try: _v161_register_from_render(int(chat_id), int(sent.message_id), text)
        except Exception: pass
    except Exception as exc:
        try: log_error(f"safe_edit_v177 async fallback {chat_id}/{msg_id}: {exc}")
        except Exception: pass
    finally:
        try:
            stage = globals().get("v177_perf_stage")
            if callable(stage): stage("telegram_fallback_send", _v161_time.monotonic() - started)
        except Exception:
            pass


def safe_edit(bot_obj, call, text, reply_markup=None, parse_mode=None):
    prep_started = _v161_time.monotonic()
    chat_id = int(call.message.chat.id); msg_id = int(call.message.message_id)
    raw_action = str(getattr(call, "data", "") or "")
    if raw_action != "nav_prev" and not _v161_state_preserving_callback(raw_action):
        try: remember_previous_window(call)
        except Exception: pass
    try:
        code = window_code_for_callback(raw_action, owner_chat=is_owner_chat(chat_id))
        if str(code).endswith("9998"):
            journal_missing_window_marker(raw_action, chat_id, msg_id, text, reply_markup, "safe_edit_v161")
        text = window_mark(text, code, html_mode=(str(parse_mode or "").upper() == "HTML"))
    except Exception:
        pass
    if reply_markup is None:
        try: reply_markup = default_window_nav_keyboard(chat_id)
        except Exception: pass
    try:
        reply_markup = ensure_previous_back_nav_keyboard(reply_markup, chat_id, msg_id)
        reply_markup = ensure_main_back_nav_keyboard(reply_markup, chat_id)
    except Exception:
        pass
    try:
        stage = globals().get("v177_perf_stage")
        if callable(stage): stage("ui_prepare", _v161_time.monotonic() - prep_started)
    except Exception:
        pass
    result = _v161_edit_retry(chat_id, msg_id, text, reply_markup=reply_markup, parse_mode=parse_mode, purpose="safe_edit_v177")
    if result in {"ok", "scheduled"}:
        try: _touch_v98_auto_close_for_callback(chat_id, msg_id, raw_action)
        except Exception: pass
        return result
    # Never block the callback with a second Telegram network call. A missing/uneditable
    # message is recreated asynchronously; the next button press is free immediately.
    try:
        pool = globals().get("GENERAL_TASK_POOL")
        key = f"v177-safe-edit-fallback:{chat_id}:{msg_id}"
        queued = bool(pool and pool.submit_unique(
            key, _v177_safe_edit_fallback_send, bot_obj, chat_id, msg_id, raw_action,
            text, reply_markup, parse_mode,
        ))
        if queued:
            return "scheduled_fallback"
    except Exception:
        pass
    try:
        bot_obj.answer_callback_query(call.id, "Telegram не подтвердил обновление. Нажмите ещё раз.", show_alert=False)
    except Exception:
        pass
    return result


def safe_edit_current_only(bot_obj, call, text, reply_markup=None, parse_mode=None):
    chat_id = int(call.message.chat.id); msg_id = int(call.message.message_id)
    raw_action = str(getattr(call, "data", "") or "")
    if raw_action != "nav_prev" and not _v161_state_preserving_callback(raw_action):
        try: remember_previous_window(call)
        except Exception: pass
    try:
        code = window_code_for_callback(raw_action, owner_chat=is_owner_chat(chat_id))
        text = window_mark(text, code, html_mode=(str(parse_mode or "").upper() == "HTML"))
    except Exception:
        pass
    if reply_markup is None:
        try: reply_markup = default_window_nav_keyboard(chat_id)
        except Exception: pass
    try:
        reply_markup = ensure_previous_back_nav_keyboard(reply_markup, chat_id, msg_id)
        reply_markup = ensure_main_back_nav_keyboard(reply_markup, chat_id)
    except Exception:
        pass
    result = _v161_edit_retry(chat_id, msg_id, text, reply_markup=reply_markup, parse_mode=parse_mode, purpose="safe_edit_current_v177")
    if result not in {"ok", "scheduled"}:
        try: bot_obj.answer_callback_query(call.id, "Это окно устарело. Откройте его заново.", show_alert=False)
        except Exception: pass
    return result


# 5. Back history commits only after actual Telegram edit succeeds.
def _v161_register_from_render(chat_id: int, message_id: int, text: str, day_key: str | None = None, code_hint: str = "") -> None:
    try: marker = _v160_marker_from_text(str(text or ""))
    except Exception: marker = ""
    try:
        day = str(day_key or get_chat_store(int(chat_id)).get("current_view_day") or today_key())
        if marker == "Ф91":
            register_open_window(int(chat_id), int(message_id), "main_day", code="О1", day_key=day, params={"parallel_allowed": True})
            try: set_active_window_id(int(chat_id), day, int(message_id))
            except Exception: pass
        else:
            register_open_window(int(chat_id), int(message_id), "local_fin_view", code=str(code_hint or marker or "view"), day_key=day, params={"parallel_allowed": True})
    except Exception:
        pass


def restore_previous_window(call) -> bool:
    try: chat_id = int(call.message.chat.id); message_id = int(call.message.message_id)
    except Exception: return False
    key = _window_nav_key(chat_id, message_id)
    with _WINDOW_NAV_HISTORY_LOCK:
        stack = list(_WINDOW_NAV_HISTORY.get(key) or [])
        snap = dict(stack[-1]) if stack else None
    if not snap:
        return False
    markup = _deserialize_inline_keyboard(snap.get("markup"))
    try:
        markup = ensure_previous_back_nav_keyboard(markup, chat_id, message_id)
        markup = ensure_main_back_nav_keyboard(markup, chat_id)
    except Exception: pass
    result = _v161_edit_retry(chat_id, message_id, str(snap.get("text") or ""), reply_markup=markup, parse_mode=snap.get("parse_mode"), purpose="nav_prev_restore")
    if result != "ok":
        try: bot_journal("nav_prev_not_committed", chat_id, f"msg={message_id}; result={result}; history_kept=1", "WARN")
        except Exception: pass
        return False
    with _WINDOW_NAV_HISTORY_LOCK:
        live = _WINDOW_NAV_HISTORY.get(key) or []
        if live: live.pop()
        if not live: _WINDOW_NAV_HISTORY.pop(key, None)
    _v161_register_from_render(chat_id, message_id, str(snap.get("text") or ""))
    try: bot_journal("nav_prev_committed", chat_id, f"msg={message_id}; edit=ok")
    except Exception: pass
    return True


def _v161_send_main(chat_id: int, day_key: str) -> int:
    txt, _ = render_day_window(int(chat_id), str(day_key))
    kb = build_main_keyboard(str(day_key), int(chat_id))
    sent = bot.send_message(int(chat_id), txt, reply_markup=kb, parse_mode="HTML")
    mid = int(getattr(sent, "message_id", 0) or 0)
    if mid:
        set_active_window_id(int(chat_id), str(day_key), mid)
        try: register_open_window(int(chat_id), mid, "main_day", code="О1", day_key=str(day_key), params={"parallel_allowed": True})
        except Exception: pass
    try: schedule_balance_panel_refresh(int(chat_id), 0.05)
    except Exception: pass
    return mid


def return_to_main_window_closing_previous(chat_id: int, day_key: str, current_message_id: int | None = None):
    chat_id = int(chat_id); day_key = str(day_key)[:10]
    try: current_mid = int(current_message_id or 0)
    except Exception: current_mid = 0
    try: old_mid = int(get_active_window_id(chat_id, day_key) or 0)
    except Exception: old_mid = 0
    txt, _ = render_day_window(chat_id, day_key); kb = build_main_keyboard(day_key, chat_id)
    if current_mid:
        try:
            cancel_auto_delete_for_message(chat_id, current_mid)
            cancel_fast_ui_edit(chat_id, current_mid)
        except Exception: pass
        result = _v161_edit_retry(chat_id, current_mid, txt, reply_markup=kb, parse_mode="HTML", purpose="back_main_instant")
        try: bot_journal("back_main_v161", chat_id, f"msg={current_mid}; old={old_mid or None}; result={result}; preserve_parallel=1")
        except Exception: pass
        if result == "ok":
            set_active_window_id(chat_id, day_key, current_mid)
            try: register_open_window(chat_id, current_mid, "main_day", code="О1", day_key=day_key, params={"parallel_allowed": True})
            except Exception: pass
            try: schedule_balance_panel_refresh(chat_id, 0.05)
            except Exception: pass
            return True
        if result == "not_found":
            try: unregister_open_window(chat_id, current_mid)
            except Exception: pass
    if old_mid and old_mid != current_mid:
        try:
            row = get_registered_open_window(chat_id, old_mid) or {}
            if str(row.get("window_type") or "") == "main_day":
                _V161_FORCE_MAIN.value = True
                try:
                    if callable(_V161_PREV_BACKUP_WINDOW): _V161_PREV_BACKUP_WINDOW(chat_id, day_key, message_id_override=old_mid)
                finally:
                    _V161_FORCE_MAIN.value = False
                set_active_window_id(chat_id, day_key, old_mid)
                return True
        except Exception:
            try: _V161_FORCE_MAIN.value = False
            except Exception: pass
    try:
        _V161_FORCE_MAIN.value = True
        _v161_send_main(chat_id, day_key)
        return True
    finally:
        _V161_FORCE_MAIN.value = False


# 6. /start gets a priority handler and never trusts one stale active message id.
def _v161_known_main_candidates(chat_id: int, day_key: str) -> list[int]:
    rows = []
    try:
        with _V146_WINDOW_LOCK:
            for item in (_open_window_registry() or {}).values():
                if not isinstance(item, dict): continue
                try:
                    if int(item.get("chat_id") or 0) == int(chat_id) and str(item.get("window_type") or "") == "main_day" and str(item.get("day_key") or "") == str(day_key):
                        rows.append((str(item.get("updated_at") or ""), int(item.get("message_id") or 0)))
                except Exception: continue
    except Exception: pass
    try:
        active = int(get_active_window_id(int(chat_id), str(day_key)) or 0)
        if active: rows.append(("9999", active))
    except Exception: pass
    seen = set(); out = []
    for _, mid in sorted(rows, reverse=True):
        if mid and mid not in seen:
            seen.add(mid); out.append(mid)
    return out


def _v161_cmd_start(msg):
    try: update_chat_info_from_message(msg)
    except Exception: pass
    try: schedule_command_delete(msg)
    except Exception: pass
    try:
        if "tenant_handle_start_payload" in globals() and tenant_handle_start_payload(msg): return
    except Exception as exc:
        try: log_error(f"v161 tenant start payload: {exc}")
        except Exception: pass
    try: chat_id = int(msg.chat.id)
    except Exception: return
    try: set_total_secret_mode(chat_id, False)
    except Exception: pass
    try:
        if is_finance_output_suppressed(chat_id): return
    except Exception: pass
    try: stop_dozvon_for_target(chat_id)
    except Exception: pass
    try:
        if guard_non_owner_finance_for_command(msg, {"ok", "help"}): return
    except Exception: pass
    try:
        if not require_finance(chat_id): return
    except Exception: pass
    try: day_key = finance_today_key() if is_finance_mode(chat_id) else today_key()
    except Exception: day_key = today_key()
    try: get_chat_store(chat_id)["current_view_day"] = day_key
    except Exception: pass
    txt, _ = render_day_window(chat_id, day_key); kb = build_main_keyboard(day_key, chat_id)
    for mid in _v161_known_main_candidates(chat_id, day_key):
        result = _v161_edit_retry(chat_id, mid, txt, reply_markup=kb, parse_mode="HTML", purpose="start_reuse_main")
        if result == "ok":
            set_active_window_id(chat_id, day_key, mid)
            try: register_open_window(chat_id, mid, "main_day", code="О1", day_key=day_key, params={"parallel_allowed": True})
            except Exception: pass
            try: schedule_balance_panel_refresh(chat_id, 0.05)
            except Exception: pass
            try: bot_journal("start_v161_reused", chat_id, f"day={day_key}; msg={mid}")
            except Exception: pass
            return
        if result == "not_found":
            try: unregister_open_window(chat_id, mid)
            except Exception: pass
    try:
        mid = _v161_send_main(chat_id, day_key)
        try: bot_journal("start_v161_created", chat_id, f"day={day_key}; msg={mid}")
        except Exception: pass
    except Exception as exc:
        try:
            log_error(f"/start v161 failed {chat_id}: {exc}")
            send_and_auto_delete(chat_id, "⚠️ Не удалось открыть основное окно. Повторите /start.", 8)
        except Exception: pass


def _v161_install_start_handler() -> int:
    try:
        bot.message_handler(commands=["start"])(_v161_cmd_start)
        handlers = getattr(bot, "message_handlers", None)
        if isinstance(handlers, list) and handlers:
            row = handlers.pop(); handlers.insert(0, row)
        return 1
    except Exception:
        return 0


# 7. Critical navigation callbacks are handled before legacy routing and always target the clicked message.
_V161_SOURCE_CONTEXT = _v161_threading.local()


def _v161_ack(call, text: str | None = None) -> None:
    try: bot.answer_callback_query(call.id, text or None, show_alert=False)
    except Exception: pass


def _v161_permission_ok(call, action: str) -> bool:
    if action == "nav_prev" or action.endswith(":back_main"): return True
    try:
        fn = globals().get("safety_permission_allowed")
        if callable(fn):
            return bool(fn(int(getattr(getattr(call, "from_user", None), "id", 0) or 0), int(call.message.chat.id), action))
    except Exception: return False
    return True


def _v161_open_info(call, day_key: str) -> bool:
    chat_id = int(call.message.chat.id); mid = int(call.message.message_id)
    action = f"d:{day_key}:info"
    if not _v161_permission_ok(call, action):
        try: bot.answer_callback_query(call.id, "Недостаточно прав", show_alert=True)
        except Exception: pass
        return True
    try: remember_previous_window(call)
    except Exception: pass
    text = window_mark(build_info_text(chat_id), "Ф54")
    kb = build_info_keyboard(chat_id)
    result = _v161_edit_retry(chat_id, mid, text, reply_markup=kb, purpose="info_v161")
    if result == "ok":
        try: register_open_window(chat_id, mid, "local_fin_view", code="info", day_key=str(day_key), params={"view_action": "info", "parallel_allowed": True})
        except Exception: pass
        return True
    try:
        sent = bot.send_message(chat_id, text, reply_markup=kb)
        register_open_window(chat_id, int(sent.message_id), "local_fin_view", code="info", day_key=str(day_key), params={"view_action": "info", "parallel_allowed": True})
    except Exception as exc:
        try: log_error(f"info_v161 fallback {chat_id}: {exc}")
        except Exception: pass
    return True


def _v161_critical_callback(call, resolved: str) -> bool:
    raw = str(resolved or "")
    if raw == "nav_prev":
        _v161_ack(call)
        if restore_previous_window(call): return True
        try:
            chat_id = int(call.message.chat.id); mid = int(call.message.message_id)
            day = get_chat_store(chat_id).get("current_view_day") or today_key()
            return_to_main_window_closing_previous(chat_id, day, current_message_id=mid)
            try: bot_journal("nav_prev_v161_fallback_main", chat_id, f"msg={mid}")
            except Exception: pass
        except Exception: pass
        return True
    if raw.startswith("d:"):
        try: _, day_key, cmd = raw.split(":", 2)
        except Exception: return False
        if cmd == "back_main":
            _v161_ack(call)
            try: return_to_main_window_closing_previous(int(call.message.chat.id), str(day_key), current_message_id=int(call.message.message_id))
            except Exception as exc:
                try: log_error(f"back_main v161: {exc}")
                except Exception: pass
            return True
        if cmd == "info":
            _v161_ack(call)
            return _v161_open_info(call, str(day_key))
    return False


def _v161_extract_token(text: str) -> str:
    m = _v161_re.search(r"(?m)^\s*(w[A-Z0-9]{6,12})\s*$", str(text or ""), flags=_v161_re.IGNORECASE)
    return str(m.group(1)).upper() if m else ""


def _v161_install_callback_intercept() -> int:
    count = 0
    for handler in list(getattr(bot, "callback_query_handlers", []) or []):
        if not isinstance(handler, dict): continue
        original = handler.get("function")
        if not callable(original) or getattr(original, "_v161_stability", False): continue
        def _wrapped(call, _original=original):
            raw = str(getattr(call, "data", "") or ""); resolved = raw
            try:
                resolver = globals().get("resolve_short_callback")
                if callable(resolver): resolved = str(resolver(raw) or raw)
            except Exception: pass
            try:
                msg_text = str(getattr(call.message, "text", None) or getattr(call.message, "caption", None) or "")
                _V161_SOURCE_CONTEXT.token = _v161_extract_token(msg_text)
                _V161_SOURCE_CONTEXT.callback = resolved
                _V161_SOURCE_CONTEXT.chat_id = int(call.message.chat.id)
                _V161_SOURCE_CONTEXT.message_id = int(call.message.message_id)
            except Exception: pass
            if _v161_critical_callback(call, resolved): return None
            try:
                clear = globals().get("_v160_clear_legacy_same_button_suppression")
                if callable(clear): clear(call)
            except Exception: pass
            return _original(call)
        _wrapped._v161_stability = True
        _wrapped.__name__ = getattr(original, "__name__", "callback_handler")
        handler["function"] = _wrapped; count += 1
    return count


# 8. Unique token per logical state; switches preserve it, normal transitions rotate it.
_V161_TOKEN_LOCK = _v161_threading.RLock()
_V161_WINDOW_TOKENS = {}


def _v161_new_token() -> str:
    return "w" + _v161_secrets.token_hex(4).upper()


def _v161_state_preserving_callback(raw: str) -> bool:
    low = str(raw or "").casefold()
    if low.startswith("v160:marker_capture") or low.startswith("v160:tz_capture"): return True
    try:
        fn = globals().get("_v160_is_switch_callback")
        if callable(fn) and fn(raw): return True
    except Exception: pass
    return any(x in low for x in ("toggle", "enable", "disable", ":on", ":off"))


def _v161_tokenize_text(text: str, chat_id: int, message_id: int | None = None) -> tuple[str, str]:
    body = str(text or "")
    try: marker = _v160_marker_from_text(body)
    except Exception: marker = ""
    if not marker: return body, ""
    body = _v161_re.sub(r"(?m)^\s*w[A-Z0-9]{6,12}\s*\n?", "", body)
    cb = str(getattr(_V161_SOURCE_CONTEXT, "callback", "") or _v161_callback_data() or "")
    source_token = str(getattr(_V161_SOURCE_CONTEXT, "token", "") or "")
    key = (int(chat_id), int(message_id or 0))
    with _V161_TOKEN_LOCK:
        existing = str(_V161_WINDOW_TOKENS.get(key) or "") if message_id else ""
        if _v161_state_preserving_callback(cb): token = source_token or existing or _v161_new_token()
        elif not cb and existing: token = existing
        else: token = _v161_new_token()
        if message_id: _V161_WINDOW_TOKENS[key] = token
    lines = body.rstrip().splitlines(); insert_at = len(lines)
    for idx in range(len(lines)-1, -1, -1):
        if _v161_re.match(r"^\s*[СФПОВсов]\d{1,6}(?:\s*[⏳⏰])?\s*$", lines[idx], flags=_v161_re.IGNORECASE):
            insert_at = idx; break
    lines.insert(insert_at, token)
    return "\n".join(lines), token


_V161_PREV_SEND = getattr(bot, "send_message", None)
_V161_PREV_EDIT_TEXT = getattr(bot, "edit_message_text", None)
_V161_PREV_EDIT_CAPTION = getattr(bot, "edit_message_caption", None)

if callable(_V161_PREV_SEND):
    def _v161_send_message(chat_id, text, *args, **kwargs):
        decorated, token = _v161_tokenize_text(str(text or ""), int(chat_id), None)
        result = _V161_PREV_SEND(chat_id, decorated, *args, **kwargs)
        try:
            mid = int(getattr(result, "message_id", 0) or 0)
            if token and mid:
                with _V161_TOKEN_LOCK: _V161_WINDOW_TOKENS[(int(chat_id), mid)] = token
        except Exception: pass
        return result
    bot.send_message = _v161_send_message

if callable(_V161_PREV_EDIT_TEXT):
    def _v161_edit_message_text(text, *args, **kwargs):
        chat_id = int(kwargs.get("chat_id") or 0); message_id = int(kwargs.get("message_id") or 0)
        decorated, token = _v161_tokenize_text(str(text or ""), chat_id, message_id)
        result = _V161_PREV_EDIT_TEXT(decorated, *args, **kwargs)
        if token and chat_id and message_id:
            with _V161_TOKEN_LOCK: _V161_WINDOW_TOKENS[(chat_id, message_id)] = token
        return result
    bot.edit_message_text = _v161_edit_message_text

if callable(_V161_PREV_EDIT_CAPTION):
    def _v161_edit_message_caption(*args, **kwargs):
        positional = list(args); caption = kwargs.get("caption")
        if caption is None and positional: caption = positional.pop(0)
        chat_id = int(kwargs.get("chat_id") or 0); message_id = int(kwargs.get("message_id") or 0)
        decorated, token = _v161_tokenize_text(str(caption or ""), chat_id, message_id)
        kwargs["caption"] = decorated
        result = _V161_PREV_EDIT_CAPTION(*positional, **kwargs)
        if token and chat_id and message_id:
            with _V161_TOKEN_LOCK: _V161_WINDOW_TOKENS[(chat_id, message_id)] = token
        return result
    bot.edit_message_caption = _v161_edit_message_caption


_V161_PREV_SOURCE_META = globals().get("_v160_source_meta")
def _v160_source_meta(chat_id: int, message_id: int, marker: str, text: str = "") -> dict:
    try: meta = dict(_V161_PREV_SOURCE_META(chat_id, message_id, marker, text) or {}) if callable(_V161_PREV_SOURCE_META) else {}
    except Exception: meta = {}
    token = _v161_extract_token(text)
    if not token:
        with _V161_TOKEN_LOCK: token = str(_V161_WINDOW_TOKENS.get((int(chat_id), int(message_id))) or "")
    meta["window_token"] = token; meta["version"] = VERSION
    return meta


# 9. Direct /iz-mr Ф91 wXXXX name and /tz Ф91 wXXXX text are supported too.
def _v161_capture_filter(msg) -> bool:
    try:
        text = str(getattr(msg, "text", "") or "").strip(); low = text.casefold()
        chat_id = int(msg.chat.id); user_id = int(getattr(getattr(msg, "from_user", None), "id", 0) or 0)
        if low.startswith("/iz-mr") or low.startswith("/iz_mr") or low.startswith("/tz"): return True
        if low in {"/cancel", "отмена"} and _v160_get_pending(chat_id, user_id): return True
        if text.startswith("/"): return False
        return bool(_v160_get_pending(chat_id, user_id))
    except Exception: return False


def _v161_capture_message(msg):
    chat_id = int(msg.chat.id); user_id = int(getattr(getattr(msg, "from_user", None), "id", 0) or 0)
    if not _v160_can_annotate(user_id): return
    text = str(getattr(msg, "text", "") or "").strip(); low = text.casefold()
    pending = _v160_get_pending(chat_id, user_id, pop=False)
    mode = None; rest = ""
    if low.startswith("/iz-mr"): mode = "marker"; rest = text[len("/iz-mr"):].strip()
    elif low.startswith("/iz_mr"): mode = "marker"; rest = text[len("/iz_mr"):].strip()
    elif low.startswith("/tz"): mode = "tz"; rest = text[len("/tz"):].strip()
    if mode is not None:
        m = _v161_re.match(r"^([СФПОВсов]\d{1,6})(?:\s+(w[A-Z0-9]{6,12}))?\s+(.+)$", rest, flags=_v161_re.IGNORECASE | _v161_re.DOTALL)
        if m:
            marker = str(m.group(1)).upper(); token = str(m.group(2) or "").upper(); body = str(m.group(3)).strip()
            _, reply_marker, source = _v160_reply_source(msg)
            if not source or reply_marker != marker: source = _v160_source_meta(chat_id, int(getattr(msg, "message_id", 0) or 0), marker, "")
            if token: source["window_token"] = token
            if mode == "marker":
                row = _v160_save_marker_name(chat_id, user_id, marker, body, source)
                send_and_auto_delete(chat_id, f"✅ {marker} {token or ''} = {row.get('name')}", 8)
            else:
                _v160_save_tz(chat_id, user_id, marker, body, source)
                send_and_auto_delete(chat_id, f"✅ ТЗ для {marker} {token or ''} сохранено.", 8)
            return
        if not pending:
            _, marker, source = _v160_reply_source(msg)
            if marker:
                _v160_set_pending(chat_id, user_id, mode, marker, source); pending = _v160_get_pending(chat_id, user_id)
            else:
                send_and_auto_delete(chat_id, "ℹ️ Нажмите /iz-mr или /tz под нужным окном.", 10); return
        send_and_auto_delete(chat_id, f"✍️ Теперь пришлите {'название окна' if mode == 'marker' else 'текст ТЗ'} одним сообщением.", 8); return
    pending = _v160_get_pending(chat_id, user_id, pop=True)
    if not pending: return
    if low in {"/cancel", "отмена"}:
        send_and_auto_delete(chat_id, "❌ Ввод отменён.", 6); return
    marker = str(pending.get("marker") or ""); source = dict(pending.get("source") or {})
    if str(pending.get("mode")) == "marker":
        row = _v160_save_marker_name(chat_id, user_id, marker, text, source)
        send_and_auto_delete(chat_id, f"✅ Запомнил: {marker} {source.get('window_token') or ''} = {row.get('name')}", 8)
    else:
        _v160_save_tz(chat_id, user_id, marker, text, source)
        send_and_auto_delete(chat_id, f"✅ ТЗ для {marker} {source.get('window_token') or ''} сохранено.", 8)


def _v161_install_capture() -> int:
    try:
        bot.message_handler(func=_v161_capture_filter, content_types=["text"])(_v161_capture_message)
        handlers = getattr(bot, "message_handlers", None)
        if isinstance(handlers, list) and handlers:
            row = handlers.pop(); handlers.insert(0, row)
        return 1
    except Exception: return 0


# 10. Marker constants and v161 restore compatibility.
try:
    WINDOW_MARKER_CONSTANTS.update({
        "v149:rem:merge:*": "Ф191", "v149:rem:command:*": "Ф191", "v149:rem:done:*": "Ф191", "v149:rem:history": "Ф191",
        "v160:marker_capture": "Ф235", "v160:tz_capture": "Ф236", "v160:export_markers": "Ф237", "v160:export_tz": "Ф238",
    })
except Exception: pass

_V161_PREV_EXPECTED = globals().get("_v155_expected_marker")
def _v155_expected_marker(action: str, chat_id: int) -> str:
    raw = str(action or "")
    if raw == "nav_prev": return ""
    if raw.endswith(":back_main"): return "Ф91"
    if raw.startswith("d:") and raw.endswith(":info"): return "Ф54"
    if raw.startswith("v149:rem:"): return "Ф191"
    if callable(_V161_PREV_EXPECTED):
        try: return str(_V161_PREV_EXPECTED(raw, int(chat_id)) or "")
        except Exception: pass
    return ""


def _v177_legacy_0286_v153_validate_restore_gz(gz_path: str) -> tuple[dict, str]:
    folder = _v161_tempfile.mkdtemp(prefix="v161_restore_validate_"); raw = _v161_os.path.join(folder, "restore.sqlite3")
    try:
        with _v161_gzip.open(gz_path, "rb") as fin, open(raw, "wb") as fout: _v161_shutil.copyfileobj(fin, fout, 1024 * 1024)
        conn = _v161_sqlite3.connect(raw)
        try:
            integrity = str(conn.execute("PRAGMA integrity_check").fetchone()[0])
            if integrity.lower() != "ok": raise RuntimeError(f"SQLite integrity_check: {integrity}")
            row = conn.execute("SELECT v FROM meta WHERE kind='v153_export' AND k='manifest'").fetchone()
            if not row: raise RuntimeError("manifest v153 not found")
            manifest = _v161_json.loads(row[0])
        finally: conn.close()
        if str(manifest.get("kind")) != "telegram_bot_full_state_v153": raise RuntimeError("unknown export kind")
        if int(manifest.get("schema_version") or 0) != int(V153_EXPORT_SCHEMA): raise RuntimeError("unsupported export schema")
        export_version = str(manifest.get("bot_version") or "")
        if not export_version.startswith(("bot_v153_", "bot_v154_", "bot_v155_", "bot_v156_", "bot_v157_", "bot_v158_", "bot_v159_", "bot_v160_", "bot_v161_")):
            raise RuntimeError(f"unsupported bot version: {export_version or 'missing'}")
        if _v153_db_logical_checksum(raw) != str(manifest.get("checksum") or ""): raise RuntimeError("checksum mismatch")
        return manifest, raw
    except Exception:
        _v161_shutil.rmtree(folder, ignore_errors=True); raise
try: _v177_legacy_0286_v153_validate_restore_gz.__name__ = '_v153_validate_restore_gz'
except Exception: pass
_v153_validate_restore_gz = _v177_legacy_0286_v153_validate_restore_gz


_V161_START_HANDLER = _v161_install_start_handler()
_V161_CALLBACK_HANDLERS = _v161_install_callback_intercept()
_V161_CAPTURE_HANDLER = _v161_install_capture()
try: globals()["_V160_FAST_EDIT_MIN_GAP"] = 0.02
except Exception: pass
try:
    bot_journal("v161_button_window_stability_installed", int(OWNER_ID or 0),
                f"start={_V161_START_HANDLER}; callbacks={_V161_CALLBACK_HANDLERS}; capture={_V161_CAPTURE_HANDLER}; F232=off; F233=file-only; delete_retries=3; nav_commit_after_edit=1; parallel=1; token=wXXXXXXXX")
except Exception: pass

# v178_global_performance_final
