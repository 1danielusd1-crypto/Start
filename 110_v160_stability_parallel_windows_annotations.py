# v160_stability_parallel_windows_annotations
"""v160: UI stabilization, parallel-window support, reliable helper timers and exact window/TZ annotations."""

import copy as _v160_copy
import gzip as _v160_gzip
import json as _v160_json
import os as _v160_os
import re as _v160_re
import shutil as _v160_shutil
import sqlite3 as _v160_sqlite3
import tempfile as _v160_tempfile
import threading as _v160_threading
import time as _v160_time
from datetime import timedelta as _v160_timedelta

VERSION = "bot_v160_stability_parallel_windows_annotations"

# ---------------------------------------------------------------------------
# 1) Generic telegram_update/process pop-up is removed again.
#    File download/upload progress (Ф233) stays visible and configurable.
# ---------------------------------------------------------------------------
try:
    INTERNAL_TIMER_DEFS.pop("helper_process_close", None)
    if "process_status_refresh" in INTERNAL_TIMER_DEFS:
        INTERNAL_TIMER_DEFS["process_status_refresh"]["label"] = "📥📤 Обновление времени / этапа файлов Ф233"
    if "file_status_close" in INTERNAL_TIMER_DEFS:
        INTERNAL_TIMER_DEFS["file_status_close"]["label"] = "📥📤 Закрытие окна файла Ф233 после завершения"
    if "helper_message_close" in INTERNAL_TIMER_DEFS:
        INTERNAL_TIMER_DEFS["helper_message_close"]["label"] = "💬 Закрытие малых служебных сообщений Ф234"
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


def _v156_process_status_schedule(chat_id: int, delay: float) -> None:
    try:
        DELAYED_SCHEDULER.cancel(f"{_V156_PROCESS_STATUS_KEY_PREFIX}{int(chat_id)}")
    except Exception:
        pass


def _v156_process_status_arm(chat_id: int | None, hint: str = "") -> None:
    # Intentionally no UI for ordinary telegram_update/background effects.
    return None


def _v156_process_status_tick(chat_id: int) -> None:
    # If a v159 generic helper survived during a hot deploy, retire it quietly.
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
        try:
            bot.delete_message(chat_id, msg_id)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# 2) Dedicated daemon timers for helper/file UI.
#    They do NOT compete with DELAYED_SCHEDULER, whose queue reached 42s in v159.
# ---------------------------------------------------------------------------
_V160_TIMER_LOCK = _v160_threading.RLock()
_V160_TIMERS = {}


def _v160_cancel_timer(key: str) -> None:
    key = str(key)
    timer = None
    with _V160_TIMER_LOCK:
        timer = _V160_TIMERS.pop(key, None)
    if timer is not None:
        try:
            timer.cancel()
        except Exception:
            pass


def _v160_schedule(key: str, delay: float, func, *args, **kwargs):
    key = str(key)
    _v160_cancel_timer(key)
    try:
        wait = max(0.05, float(delay))
    except Exception:
        wait = 0.05

    def _run():
        try:
            func(*args, **kwargs)
        finally:
            with _V160_TIMER_LOCK:
                current = _V160_TIMERS.get(key)
                if current is timer:
                    _V160_TIMERS.pop(key, None)

    timer = _v160_threading.Timer(wait, _run)
    timer.daemon = True
    with _V160_TIMER_LOCK:
        _V160_TIMERS[key] = timer
    timer.start()
    return timer


def _v160_delete_quiet(chat_id: int, message_id: int) -> None:
    chat_id = int(chat_id); message_id = int(message_id)
    try:
        bot.delete_message(chat_id, message_id)
    except Exception:
        pass
    try:
        unregister_open_window(chat_id, message_id)
    except Exception:
        pass
    try:
        store = get_chat_store(chat_id)
        changed = False
        for key in ("_v160_file_status_msg_id", "command_window_id"):
            try:
                if int(store.get(key) or 0) == message_id:
                    store[key] = None; changed = True
            except Exception:
                pass
        if changed:
            save_data(data, chat_ids=[chat_id])
            try: schedule_quick_backup(chat_id, 0.5)
            except Exception: pass
    except Exception:
        pass


def _v160_schedule_delete(chat_id: int, message_id: int, delay: float, prefix: str = "delete") -> None:
    _v160_schedule(f"v160:{prefix}:{int(chat_id)}:{int(message_id)}", delay, _v160_delete_quiet, int(chat_id), int(message_id))


def _file_job_tick(key: str):
    key = str(key)
    with _FILE_JOB_LOCK:
        st = _FILE_JOB_STATE.get(key)
        if not isinstance(st, dict):
            return
        chat_id = int(st.get("chat_id"))
        msg_id = st.get("status_msg_id")
        label = str(st.get("label") or "Файл")
        phase = str(st.get("phase") or "работаю")
        started = float(st.get("started_monotonic") or st.get("queued_monotonic") or _v160_time.monotonic())
        elapsed = _file_job_elapsed_text(_v160_time.monotonic() - started)
        cur = st.get("current")
        tot = st.get("total")
    try:
        if msg_id:
            bot.edit_message_text(
                _v159_file_status_text(label, elapsed, phase, cur, tot),
                chat_id=chat_id, message_id=int(msg_id),
            )
    except Exception:
        pass
    with _FILE_JOB_LOCK:
        alive = isinstance(_FILE_JOB_STATE.get(key), dict)
    if alive:
        _v160_schedule(
            f"v160:file-tick:{key}",
            internal_timer_seconds("process_status_refresh", 10.0),
            _file_job_tick, key,
        )


def _interactive_file_job_runner(job_meta: dict, func, args, kwargs):
    key = str(job_meta.get("key") or _INTERACTIVE_FILE_JOB_KEY)
    previous = getattr(_FILE_JOB_CONTEXT, "value", None)
    _FILE_JOB_CONTEXT.value = {"key": key}
    ok = False
    error_text = ""
    try:
        with _FILE_JOB_LOCK:
            st = _FILE_JOB_STATE.get(key)
            if isinstance(st, dict):
                st["started_monotonic"] = _v160_time.monotonic()
                st["phase"] = "запуск"
        _file_job_progress("запуск", force=True)
        mem_ctx = globals().get("memory_operation")
        if callable(mem_ctx):
            with mem_ctx(
                f"file:{job_meta.get('kind') or 'export'}",
                {"chat_id": job_meta.get("chat_id"), "label": job_meta.get("label")},
                heavy=True,
            ):
                result = func(*args, **kwargs)
        else:
            result = func(*args, **kwargs)
        ok = (result is not False)
        if not ok:
            error_text = "операция завершилась без подтверждения"
    except Exception as exc:
        error_text = str(exc)[:300]
        try:
            log_error(f"INTERACTIVE FILE JOB {job_meta.get('kind')}: {exc}")
        except Exception:
            pass
    finally:
        now_m = _v160_time.monotonic()
        with _FILE_JOB_LOCK:
            st = _FILE_JOB_STATE.get(key)
            if isinstance(st, dict):
                chat_id = int(st.get("chat_id"))
                msg_id = st.get("status_msg_id")
                label = str(st.get("label") or "Файл")
                started = float(st.get("started_monotonic") or st.get("queued_monotonic") or now_m)
                elapsed = _file_job_elapsed_text(now_m - started)
            else:
                chat_id = int(job_meta.get("chat_id") or 0)
                msg_id = None
                label = str(job_meta.get("label") or "Файл")
                elapsed = "0:00"
        try:
            if msg_id:
                close_s = internal_timer_seconds("file_status_close", 15)
                if ok:
                    final = f"✅ {label}\nГотово за {elapsed}.\nОкно закроется через {_format_duration_short(close_s)}."
                else:
                    final = (
                        f"⚠️ {label}\nЗавершено за {elapsed}.\n"
                        f"{error_text or 'Telegram не подтвердил отправку.'}\n"
                        f"Окно закроется через {_format_duration_short(close_s)}."
                    )
                final = _v159_force_marker(final, "Ф233", "⏳")
                bot.edit_message_text(final, chat_id=chat_id, message_id=int(msg_id))
                _v160_schedule_delete(chat_id, int(msg_id), close_s, "file-close")
        except Exception:
            pass
        try:
            bot_journal(
                "file_job_done" if ok else "file_job_uncertain",
                chat_id,
                f"kind={job_meta.get('kind')} elapsed={elapsed} error={error_text}",
            )
        except Exception:
            pass
        _v160_cancel_timer(f"v160:file-tick:{key}")
        try:
            DELAYED_SCHEDULER.cancel(f"file-job-tick:{key}")
        except Exception:
            pass
        with _FILE_JOB_LOCK:
            _FILE_JOB_STATE.pop(key, None)
        if previous is None:
            try:
                delattr(_FILE_JOB_CONTEXT, "value")
            except Exception:
                pass
        else:
            _FILE_JOB_CONTEXT.value = previous


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
                send_and_auto_delete(chat_id, f"🧠 {reason}", internal_timer_seconds("helper_message_close", 25))
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
            "queued_monotonic": _v160_time.monotonic(),
            "started_monotonic": 0.0,
            "phase": "в очереди",
            "status_msg_id": None,
            "last_ui_monotonic": 0.0,
        }
        _FILE_JOB_STATE[key] = meta
    try:
        status = bot.send_message(chat_id, _v159_file_status_text(str(label), "0:00", "в очереди"))
        status_mid = int(getattr(status, "message_id", 0) or 0) or None
        with _FILE_JOB_LOCK:
            if isinstance(_FILE_JOB_STATE.get(key), dict):
                _FILE_JOB_STATE[key]["status_msg_id"] = status_mid
        if status_mid:
            try:
                store = get_chat_store(chat_id)
                store["_v160_file_status_msg_id"] = int(status_mid)
                store["_v160_file_status_created_at"] = now_local().isoformat(timespec="seconds")
                save_data(data, chat_ids=[chat_id])
                schedule_quick_backup(chat_id, 0.2)
            except Exception:
                pass
    except Exception:
        pass
    ok = EXPORT_TASK_POOL.submit_unique(key, _interactive_file_job_runner, dict(meta), func, args, kwargs)
    if not ok:
        with _FILE_JOB_LOCK:
            _FILE_JOB_STATE.pop(key, None)
        return False, build_all_processes_toast(chat_id)
    _v160_schedule(
        f"v160:file-tick:{key}",
        internal_timer_seconds("process_status_refresh", 10.0),
        _file_job_tick, key,
    )
    try:
        bot_journal("file_job_queued", chat_id, f"kind={kind} label={label}; visual=1; marker=Ф233; timer=dedicated")
    except Exception:
        pass
    return True, "Запущено"


# Small helper messages must never occupy/replace command_window_id/F91.
def send_and_auto_delete(chat_id: int, text: str, delay: int = 25):
    if is_finance_output_suppressed(chat_id):
        return
    delay = _v159_helper_delay(delay)
    try:
        msg = bot.send_message(int(chat_id), _v159_helper_mark(text))
        _v160_schedule_delete(int(chat_id), int(msg.message_id), delay, "helper")
    except Exception as exc:
        try:
            log_error(f"send_and_auto_delete v160: {exc}")
        except Exception:
            pass


def send_html_and_auto_delete(chat_id: int, html_text: str, delay: int = 25):
    if is_finance_output_suppressed(chat_id):
        return
    delay = _v159_helper_delay(delay)
    try:
        msg = bot.send_message(int(chat_id), _v159_helper_mark(html_text), parse_mode="HTML")
        _v160_schedule_delete(int(chat_id), int(msg.message_id), delay, "helper-html")
    except Exception as exc:
        try:
            log_error(f"send_html_and_auto_delete v160: {exc}")
        except Exception:
            pass



# Removing old automatic finance windows is UI cleanup, not a financial mutation.
# Clear their runtime ids immediately and delete Telegram messages in maintenance,
# so a toggle does not block the callback on several network delete calls.
def delete_auto_finance_windows_for_chat(chat_id: int, *, persist_now: bool = False) -> int:
    chat_id = int(chat_id)
    store = get_chat_store(chat_id)
    ids = set()
    try:
        ids.update(int(v) for v in (get_or_create_active_windows(chat_id) or {}).values() if v)
    except Exception:
        pass
    try:
        if store.get("balance_panel_id"):
            ids.add(int(store.get("balance_panel_id")))
    except Exception:
        pass

    data.setdefault("active_messages", {})[str(chat_id)] = {}
    store["balance_panel_id"] = None
    store["balance_panel_mode"] = "mini"
    store["main_window_msg_count"] = 0
    store["balance_panel_msg_count"] = 0
    state = _finance_window_state(chat_id)
    state["main_windows"] = {}
    state["balance_panel_id"] = None
    state["balance_panel_mode"] = "mini"
    state["auto_reopen_on_boot"] = False if finance_window_mode(chat_id) == "off" else state.get("auto_reopen_on_boot", True)
    state["updated_at"] = now_local().isoformat(timespec="seconds")
    for mid in sorted(ids):
        try:
            unregister_open_window(chat_id, mid)
        except Exception:
            pass

    try:
        save_data(data, chat_ids=[chat_id])
    except Exception:
        pass
    if persist_now:
        try:
            _persist_finance_window_mode_critical(chat_id)
        except Exception:
            pass
    else:
        try:
            schedule_quick_backup(chat_id, 0.2)
        except Exception:
            pass

    def _delete_batch():
        removed = 0
        for mid in sorted(ids):
            try:
                bot.delete_message(chat_id, int(mid)); removed += 1
            except Exception:
                pass
        try:
            bot_journal("finance_windows_deleted_async", chat_id, f"requested={len(ids)} deleted={removed}")
        except Exception:
            pass

    if ids:
        try:
            if not MAINTENANCE_TASK_POOL.submit(f"v160-fin-window-delete:{chat_id}", _delete_batch):
                _v160_schedule(f"v160-fin-delete-fallback:{chat_id}", 0.05, _delete_batch)
        except Exception:
            _v160_schedule(f"v160-fin-delete-fallback:{chat_id}", 0.05, _delete_batch)
    return len(ids)


# ---------------------------------------------------------------------------
# 3) Button reliability: no shared-delayed edit for an actual click.
#    Exact duplicate callback-query IDs are suppressed; a NEW click is never
#    discarded only because it has the same callback_data as the previous click.
# ---------------------------------------------------------------------------
_V160_FAST_EDIT_LOCKS = defaultdict(_v160_threading.RLock)
_V160_FAST_EDIT_LAST = {}
_V160_FAST_EDIT_MIN_GAP = max(0.03, min(0.20, float(_v160_os.getenv("V160_UI_MIN_GAP_SECONDS", "0.08") or "0.08")))


def _callback_should_debounce(call, data_str: str, min_interval: float = 0.12) -> bool:
    return str(data_str or "") == "none"


def fast_ui_edit_message_text(chat_id: int, message_id: int, text: str, reply_markup=None, parse_mode=None, purpose: str = "fast_ui") -> str:
    chat_id = int(chat_id); message_id = int(message_id)
    try:
        code = (_V159_TIMER_PURPOSE_MARKERS or {}).get(str(purpose or ""))
        if code:
            text = _v159_force_marker(text, code)
    except Exception:
        pass
    try:
        if "secret" not in str(purpose or "").lower():
            reply_markup = ensure_previous_back_nav_keyboard(reply_markup, chat_id, message_id)
            reply_markup = ensure_main_back_nav_keyboard(reply_markup, chat_id)
    except Exception:
        pass
    try:
        text = _ensure_window_marker_for_render(text, reply_markup, chat_id, message_id, purpose)
    except Exception:
        pass
    try:
        augment = globals().get("_v160_augment_markup")
        if callable(augment):
            reply_markup = augment(reply_markup, text)
    except Exception:
        pass
    payload = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
        "reply_markup": reply_markup,
        "parse_mode": parse_mode,
        "purpose": purpose,
    }
    try:
        prepare = globals().get("window_diag_prepare_fast_ui_payload")
        if callable(prepare):
            payload = prepare(payload) or payload
    except Exception:
        pass
    try:
        cancel_fast_ui_edit(chat_id, message_id)
    except Exception:
        pass
    key = (chat_id, message_id)
    with _V160_FAST_EDIT_LOCKS[key]:
        now_m = _v160_time.monotonic()
        last = float(_V160_FAST_EDIT_LAST.get(key, 0.0) or 0.0)
        remain = _V160_FAST_EDIT_MIN_GAP - (now_m - last)
        if remain > 0:
            _v160_time.sleep(remain)
        _V160_FAST_EDIT_LAST[key] = _v160_time.monotonic()
        try:
            apply_fn = globals().get("window_diag_fast_ui_apply")
            if callable(apply_fn):
                apply_fn(payload, delayed=False)
        except Exception:
            pass
        result = _perform_fast_ui_edit(payload)
        if result == "rate_limited":
            # One short local retry; do not put the click into the overloaded delayed heap.
            _v160_time.sleep(0.35)
            _V160_FAST_EDIT_LAST[key] = _v160_time.monotonic()
            result = _perform_fast_ui_edit(payload)
        return result


_V160_CALLBACK_LOCK = _v160_threading.RLock()
_V160_CALLBACK_IDS = {}


def _v160_exact_callback_duplicate(call) -> bool:
    call_id = str(getattr(call, "id", "") or "")
    if not call_id:
        return False
    now_m = _v160_time.monotonic()
    with _V160_CALLBACK_LOCK:
        for old, ts in list(_V160_CALLBACK_IDS.items()):
            if now_m - float(ts) > 600:
                _V160_CALLBACK_IDS.pop(old, None)
        if call_id in _V160_CALLBACK_IDS:
            return True
        _V160_CALLBACK_IDS[call_id] = now_m
    return False


def _v160_clear_legacy_same_button_suppression(call) -> None:
    try:
        store = globals().get("_V153_CALLBACK_SIGNATURES")
        lock = globals().get("_V153_LOCK")
        if not isinstance(store, dict):
            return
        actor_fn = globals().get("_v153_actor_id")
        actor = int(actor_fn(call)) if callable(actor_fn) else int(getattr(getattr(call, "from_user", None), "id", 0) or 0)
        message = getattr(call, "message", None)
        signature = (
            actor,
            int(getattr(getattr(message, "chat", None), "id", 0) or 0),
            int(getattr(message, "message_id", 0) or 0),
            str(getattr(call, "data", "") or ""),
        )
        if lock is not None:
            with lock:
                store.pop(signature, None)
        else:
            store.pop(signature, None)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# 4) Parallel windows are first-class. Same marker may exist in several messages.
# ---------------------------------------------------------------------------
def _window_diag_duplicate_marker(chat_id: int, message_id: int, marker: str) -> dict:
    # A marker identifies window logic, not a unique Telegram message instance.
    return {}


def cleanup_open_window_registry(reason: str = "manual") -> dict:
    now_dt = now_local()
    keep_days = max(7, min(90, int(_v160_os.getenv("V160_PARALLEL_WINDOW_KEEP_DAYS", "30") or "30")))
    cutoff = now_dt - _v160_timedelta(days=keep_days)
    removed = duplicates = normalized = 0
    with _V146_WINDOW_LOCK:
        reg = _open_window_registry()
        grouped = defaultdict(list)
        for key, item in list(reg.items()):
            try:
                cid = int((item or {}).get("chat_id") or 0)
                mid = int((item or {}).get("message_id") or 0)
                if not cid or not mid:
                    reg.pop(key, None); removed += 1; continue
                grouped[(cid, mid)].append((key, item or {}))
            except Exception:
                reg.pop(key, None); removed += 1
        new_reg = {}
        for (chat_id, message_id), rows in grouped.items():
            rows.sort(key=lambda pair: (int((pair[1] or {}).get("epoch") or 0), str((pair[1] or {}).get("updated_at") or "")), reverse=True)
            key, item = rows[0]
            duplicates += max(0, len(rows) - 1)  # same Telegram message only
            try:
                updated = datetime.fromisoformat(str((item or {}).get("updated_at") or ""))
                if updated.tzinfo is None:
                    updated = updated.replace(tzinfo=now_dt.tzinfo)
            except Exception:
                updated = now_dt
            # Do not remove a still-plausible window only because another message is the active/stored one.
            if updated < cutoff:
                removed += 1
                continue
            canonical = _v146_registry_key(chat_id, message_id)
            item = dict(item or {})
            item["epoch"] = max(1, int(item.get("epoch") or 1))
            item["parallel_allowed"] = True
            new_reg[canonical] = item
            if canonical != key:
                normalized += 1
        if duplicates or removed or normalized or len(reg) != len(new_reg):
            reg.clear(); reg.update(new_reg)
            save_data(data, root_only=True)
    result = {
        "reason": reason,
        "kept": len(_open_window_registry() or {}),
        "removed": removed,
        "duplicates_removed": duplicates,
        "parallel_preserved": True,
        "keys_normalized": normalized,
    }
    try:
        bot_journal("window_registry_cleanup", None, _v160_json.dumps(result, ensure_ascii=False))
    except Exception:
        pass
    return result


_V160_PREV_RETURN_TO_MAIN = globals().get("return_to_main_window_closing_previous")


def return_to_main_window_closing_previous(chat_id: int, day_key: str, current_message_id: int | None = None):
    chat_id = int(chat_id); day_key = str(day_key)[:10]
    try:
        current_mid = int(current_message_id or 0)
    except Exception:
        current_mid = 0
    try:
        old_mid = int(get_active_window_id(chat_id, day_key) or 0)
    except Exception:
        old_mid = 0
    if current_mid:
        try:
            cancel_auto_delete_for_message(chat_id, current_mid)
        except Exception:
            pass
        try:
            cancel_fast_ui_edit(chat_id, current_mid)
        except Exception:
            pass
        txt, _ = render_day_window(chat_id, day_key)
        kb = build_main_keyboard(day_key, chat_id)
        result = fast_ui_edit_message_text(
            chat_id, current_mid, txt,
            reply_markup=kb, parse_mode="HTML", purpose="back_main_instant",
        )
        try:
            bot_journal("back_main_fast", chat_id, f"day={day_key} result={result} old={old_mid or None} current={current_mid}; parallel=1")
        except Exception:
            pass
        if result == "ok":
            set_active_window_id(chat_id, day_key, current_mid)
            try:
                register_open_window(chat_id, current_mid, "main_day", code="О1", day_key=day_key, params={"parallel_allowed": True})
            except Exception:
                pass
            if old_mid and old_mid != current_mid:
                try:
                    row = get_registered_open_window(chat_id, old_mid) or {}
                    if row:
                        row["parallel_allowed"] = True
                    bot_journal("parallel_main_preserved", chat_id, f"day={day_key}; primary={current_mid}; preserved={old_mid}")
                except Exception:
                    pass
            schedule_balance_panel_refresh(chat_id, 0.05)
            return
        if result == "not_found":
            try:
                unregister_open_window(chat_id, current_mid)
            except Exception:
                pass
            if old_mid and old_mid != current_mid:
                try:
                    backup_window_for_owner(chat_id, day_key, message_id_override=old_mid)
                    return
                except Exception:
                    pass
    # With no usable clicked message, retain the established fallback behavior.
    if callable(_V160_PREV_RETURN_TO_MAIN):
        return _V160_PREV_RETURN_TO_MAIN(chat_id, day_key, current_message_id=None)
    return None


# ---------------------------------------------------------------------------
# 5) Remaining reminder marker holes found in the v159 journal.
# ---------------------------------------------------------------------------
try:
    WINDOW_MARKER_CONSTANTS.update({
        "v149:rem:merge:*": "Ф191",
        "v149:rem:command:*": "Ф191",
        "v149:rem:done:*": "Ф191",
        "v149:rem:history": "Ф191",
        "v160:marker_capture": "Ф235",
        "v160:tz_capture": "Ф236",
        "v160:export_markers": "Ф237",
        "v160:export_tz": "Ф238",
    })
except Exception:
    pass

_V160_PREV_EXPECTED_MARKER = globals().get("_v155_expected_marker")


def _v155_expected_marker(action: str, chat_id: int) -> str:
    raw = str(action or "")
    if raw.startswith("v149:rem:merge:") or raw.startswith("v149:rem:command:") or raw.startswith("v149:rem:done:") or raw == "v149:rem:history":
        return "Ф191"
    if raw.startswith("v160:"):
        # Annotation/export actions do not replace the source window.
        return ""
    if callable(_V160_PREV_EXPECTED_MARKER):
        try:
            return str(_V160_PREV_EXPECTED_MARKER(raw, int(chat_id)) or "")
        except Exception:
            pass
    return ""


# ---------------------------------------------------------------------------
# 6) Exact window annotation and per-window TZ capture.
#    Telegram cannot programmatically fill the user's compose box. Instead the
#    /iz-mr and /tz inline controls bind the exact source message and open a
#    ForceReply; the next text is saved with marker/message/callback metadata.
# ---------------------------------------------------------------------------
_V160_ANNOTATION_LOCK = _v160_threading.RLock()
_V160_ANNOTATION_PENDING = {}
_V160_LAST_WINDOW_META = {}
_V160_PENDING_TTL = 900.0
_V160_MARKER_ROOT_KEY = "_window_marker_catalog_v160"
_V160_TZ_ROOT_KEY = "_window_tz_v160"


def _v160_marker_from_text(text: str) -> str:
    try:
        fn = globals().get("_window_diag_marker")
        marker = str(fn(str(text or "")) or "") if callable(fn) else ""
        if marker:
            return marker.upper()
    except Exception:
        pass
    try:
        match = _v160_re.search(r"(?:^|\n)\s*([СФПОВсов]\d{1,6})(?:\s*[⏳⏰])?\s*$", str(text or ""), flags=_v160_re.IGNORECASE)
        return str(match.group(1) if match else "").upper()
    except Exception:
        return ""


def _v160_markup_callbacks(reply_markup) -> set[str]:
    out = set()
    try:
        for row in list(getattr(reply_markup, "keyboard", None) or []):
            for button in row or []:
                cb = str(getattr(button, "callback_data", "") or "")
                if cb:
                    out.add(cb)
    except Exception:
        pass
    return out


def _v160_augment_markup(reply_markup, text: str):
    marker = _v160_marker_from_text(text)
    if not marker:
        return reply_markup
    try:
        if isinstance(reply_markup, types.InlineKeyboardMarkup):
            kb = _v160_copy.deepcopy(reply_markup)
        else:
            kb = types.InlineKeyboardMarkup()
    except Exception:
        kb = reply_markup if reply_markup is not None else types.InlineKeyboardMarkup()
    callbacks = _v160_markup_callbacks(kb)
    try:
        if "v160:marker_capture" not in callbacks and "v160:tz_capture" not in callbacks:
            kb.row(IB("/iz-mr", callback_data="v160:marker_capture"), IB("/tz", callback_data="v160:tz_capture"))
        if marker == "Ф89":
            callbacks = _v160_markup_callbacks(kb)
            if "v160:export_markers" not in callbacks:
                kb.row(IB("🏷 Скачать маркировки окон", callback_data="v160:export_markers"))
            if "v160:export_tz" not in callbacks:
                kb.row(IB("📝 Скачать ТЗ окон", callback_data="v160:export_tz"))
    except Exception:
        return reply_markup
    return kb


def _v160_is_switch_callback(raw: str) -> bool:
    low = str(raw or "").casefold()
    switch_tokens = (
        "toggle", "_on", "_off", ":on", ":off", "enable", "disable",
        "itmr_digit:", "itmr_unit:", "itmr_backspace", "itmr_clear", "itmr_apply",
    )
    return any(token in low for token in switch_tokens)


def _v160_recent_diag_for_message(chat_id: int, message_id: int) -> dict:
    try:
        rows = list(window_diagnostic_tail(160)) if callable(globals().get("window_diagnostic_tail")) else []
    except Exception:
        rows = []
    for row in reversed(rows):
        try:
            if int(row.get("chat_id") or 0) != int(chat_id) or int(row.get("message_id") or 0) != int(message_id):
                continue
            cb = str(row.get("callback_data") or "")
            if cb.startswith("v160:") or _v160_is_switch_callback(cb):
                continue
            if row.get("action") in {"window_edit_applied", "window_created", "window_ui_edit_apply", "window_registry_registered", "window_registry_epoch_changed"}:
                return {
                    "action": row.get("action"),
                    "callback": cb,
                    "caller": row.get("caller"),
                    "update_id": row.get("update_id"),
                    "detail": dict(row.get("detail") or {}),
                    "ts": row.get("ts"),
                }
        except Exception:
            continue
    return {}


def _v160_note_window_meta(chat_id: int, message_id: int, text: str, purpose: str = "") -> None:
    marker = _v160_marker_from_text(text)
    if not marker:
        return
    ctx = {}
    try:
        ctx = _current_telegram_update_context()
    except Exception:
        pass
    raw_callback = str((ctx or {}).get("callback_data") or "")
    with _V160_ANNOTATION_LOCK:
        previous_meta = dict(_V160_LAST_WINDOW_META.get((int(chat_id), int(message_id))) or {})
    if _v160_is_switch_callback(raw_callback) and previous_meta:
        previous_meta["seen_at"] = now_local().isoformat(timespec="milliseconds")
        previous_meta["last_switch_callback"] = raw_callback[:240]
        with _V160_ANNOTATION_LOCK:
            _V160_LAST_WINDOW_META[(int(chat_id), int(message_id))] = previous_meta
        return
    rows = [x.strip() for x in str(text or "").splitlines() if x.strip()]
    first_line = rows[0][:220] if rows else ""
    meta = {
        "version": VERSION,
        "marker": marker,
        "chat_id": int(chat_id),
        "message_id": int(message_id),
        "first_line": first_line,
        "purpose": str(purpose or "")[:180],
        "callback": raw_callback[:240],
        "update_id": (ctx or {}).get("update_id"),
        "seen_at": now_local().isoformat(timespec="milliseconds"),
    }
    if not raw_callback:
        meta.update({k: v for k, v in _v160_recent_diag_for_message(chat_id, message_id).items() if v not in (None, "", {})})
    with _V160_ANNOTATION_LOCK:
        _V160_LAST_WINDOW_META[(int(chat_id), int(message_id))] = meta
        if len(_V160_LAST_WINDOW_META) > 1200:
            for stale in list(_V160_LAST_WINDOW_META)[:200]:
                _V160_LAST_WINDOW_META.pop(stale, None)


_V160_ORIG_SEND_MESSAGE = getattr(bot, "send_message", None)
_V160_ORIG_EDIT_MESSAGE_TEXT = getattr(bot, "edit_message_text", None)
_V160_ORIG_EDIT_MESSAGE_CAPTION = getattr(bot, "edit_message_caption", None)

if callable(_V160_ORIG_SEND_MESSAGE):
    def _v160_send_message(chat_id, text, *args, **kwargs):
        try:
            kwargs["reply_markup"] = _v160_augment_markup(kwargs.get("reply_markup"), str(text or ""))
        except Exception:
            pass
        result = _V160_ORIG_SEND_MESSAGE(chat_id, text, *args, **kwargs)
        try:
            _v160_note_window_meta(int(chat_id), int(getattr(result, "message_id", 0) or 0), str(text or ""), "send_message")
        except Exception:
            pass
        return result
    bot.send_message = _v160_send_message

if callable(_V160_ORIG_EDIT_MESSAGE_TEXT):
    def _v160_edit_message_text(text, *args, **kwargs):
        chat_id = kwargs.get("chat_id")
        message_id = kwargs.get("message_id")
        try:
            kwargs["reply_markup"] = _v160_augment_markup(kwargs.get("reply_markup"), str(text or ""))
        except Exception:
            pass
        result = _V160_ORIG_EDIT_MESSAGE_TEXT(text, *args, **kwargs)
        try:
            _v160_note_window_meta(int(chat_id), int(message_id), str(text or ""), "edit_message_text")
        except Exception:
            pass
        return result
    bot.edit_message_text = _v160_edit_message_text

if callable(_V160_ORIG_EDIT_MESSAGE_CAPTION):
    def _v160_edit_message_caption(*args, **kwargs):
        caption = kwargs.get("caption")
        if caption is None and args:
            caption = args[0]
        try:
            kwargs["reply_markup"] = _v160_augment_markup(kwargs.get("reply_markup"), str(caption or ""))
        except Exception:
            pass
        result = _V160_ORIG_EDIT_MESSAGE_CAPTION(*args, **kwargs)
        try:
            _v160_note_window_meta(int(kwargs.get("chat_id")), int(kwargs.get("message_id")), str(caption or ""), "edit_message_caption")
        except Exception:
            pass
        return result
    bot.edit_message_caption = _v160_edit_message_caption


def _v160_annotation_roots():
    gs = data.setdefault("_global_settings", {})
    catalog = gs.setdefault(_V160_MARKER_ROOT_KEY, {})
    tz_rows = gs.setdefault(_V160_TZ_ROOT_KEY, [])
    if not isinstance(catalog, dict):
        catalog = {}; gs[_V160_MARKER_ROOT_KEY] = catalog
    if not isinstance(tz_rows, list):
        tz_rows = []; gs[_V160_TZ_ROOT_KEY] = tz_rows
    return catalog, tz_rows


def _v160_persist_annotations(chat_id: int) -> None:
    try:
        save_data(data, root_only=True)
    except Exception:
        pass
    try:
        schedule_delta_backup(int(OWNER_ID or chat_id), delay=0.2, reason="v160_window_annotations")
    except Exception:
        try:
            schedule_quick_backup(int(OWNER_ID or chat_id), 0.5)
        except Exception:
            pass


def _v160_source_meta(chat_id: int, message_id: int, marker: str, text: str = "") -> dict:
    with _V160_ANNOTATION_LOCK:
        meta = dict(_V160_LAST_WINDOW_META.get((int(chat_id), int(message_id))) or {})
    if not meta:
        meta = _v160_recent_diag_for_message(chat_id, message_id)
    rows = [x.strip() for x in str(text or "").splitlines() if x.strip()]
    meta.update({
        "version": VERSION,
        "marker": str(marker),
        "chat_id": int(chat_id),
        "message_id": int(message_id),
        "first_line": str(meta.get("first_line") or (rows[0] if rows else ""))[:220],
        "captured_at": now_local().isoformat(timespec="milliseconds"),
    })
    return meta


def _v160_save_marker_name(chat_id: int, user_id: int, marker: str, name: str, source: dict) -> dict:
    marker = str(marker or "").upper().strip()
    name = " ".join(str(name or "").strip().split())[:180]
    if not marker or not name:
        raise ValueError("marker/name missing")
    catalog, _ = _v160_annotation_roots()
    now_s = now_local().isoformat(timespec="milliseconds")
    with _V160_ANNOTATION_LOCK:
        row = dict(catalog.get(marker) or {})
        history = list(row.get("name_history") or [])
        previous = str(row.get("name") or "")
        if previous and previous != name:
            history.append({"at": now_s, "name": previous, "changed_by": int(user_id)})
        row.update({
            "marker": marker,
            "name": name,
            "name_history": history[-30:],
            "first_named_at": str(row.get("first_named_at") or now_s),
            "last_named_at": now_s,
            "named_by": int(user_id),
            "source": _v160_copy.deepcopy(source),
        })
        catalog[marker] = row
    _v160_persist_annotations(chat_id)
    try:
        bot_journal("window_marker_named", chat_id, f"marker={marker}; name={name[:100]}; source_msg={source.get('message_id')}")
    except Exception:
        pass
    return row


def _v160_save_tz(chat_id: int, user_id: int, marker: str, body: str, source: dict) -> dict:
    marker = str(marker or "").upper().strip()
    body = str(body or "").strip()[:12000]
    if not marker or not body:
        raise ValueError("marker/tz missing")
    catalog, rows = _v160_annotation_roots()
    now_s = now_local().isoformat(timespec="milliseconds")
    row = {
        "id": f"tz-{int(_v160_time.time()*1000)}-{int(user_id)}",
        "at": now_s,
        "marker": marker,
        "window_name": str((catalog.get(marker) or {}).get("name") or ""),
        "text": body,
        "user_id": int(user_id),
        "source": _v160_copy.deepcopy(source),
    }
    with _V160_ANNOTATION_LOCK:
        rows.append(row)
        if len(rows) > 2000:
            del rows[:-2000]
    _v160_persist_annotations(chat_id)
    try:
        bot_journal("window_tz_saved", chat_id, f"marker={marker}; chars={len(body)}; source_msg={source.get('message_id')}")
    except Exception:
        pass
    return row


def _v160_pending_key(chat_id: int, user_id: int):
    return (int(chat_id), int(user_id))


def _v160_set_pending(chat_id: int, user_id: int, mode: str, marker: str, source: dict) -> None:
    now_m = _v160_time.monotonic()
    with _V160_ANNOTATION_LOCK:
        for key, row in list(_V160_ANNOTATION_PENDING.items()):
            if now_m - float((row or {}).get("created_mono") or 0.0) > _V160_PENDING_TTL:
                _V160_ANNOTATION_PENDING.pop(key, None)
        _V160_ANNOTATION_PENDING[_v160_pending_key(chat_id, user_id)] = {
            "mode": str(mode), "marker": str(marker), "source": _v160_copy.deepcopy(source), "created_mono": now_m,
        }


def _v160_get_pending(chat_id: int, user_id: int, pop: bool = False):
    key = _v160_pending_key(chat_id, user_id)
    with _V160_ANNOTATION_LOCK:
        row = _V160_ANNOTATION_PENDING.get(key)
        if row and _v160_time.monotonic() - float(row.get("created_mono") or 0.0) > _V160_PENDING_TTL:
            _V160_ANNOTATION_PENDING.pop(key, None); return None
        if pop and row:
            return _V160_ANNOTATION_PENDING.pop(key, None)
        return dict(row or {}) if row else None


def _v160_can_annotate(user_id: int) -> bool:
    try:
        fn = globals().get("_v153_platform_owner")
        if callable(fn):
            return bool(fn(int(user_id)))
    except Exception:
        pass
    try:
        return int(user_id) == int(OWNER_ID or 0)
    except Exception:
        return False


def _v160_call_source(call):
    message = getattr(call, "message", None)
    chat_id = int(getattr(getattr(message, "chat", None), "id", 0) or 0)
    message_id = int(getattr(message, "message_id", 0) or 0)
    text = str(getattr(message, "text", None) or getattr(message, "caption", None) or "")
    marker = _v160_marker_from_text(text)
    source = _v160_source_meta(chat_id, message_id, marker, text)
    return chat_id, message_id, marker, source


def _v160_begin_capture(call, mode: str) -> bool:
    chat_id, message_id, marker, source = _v160_call_source(call)
    user_id = int(getattr(getattr(call, "from_user", None), "id", 0) or 0)
    if not _v160_can_annotate(user_id):
        try: bot.answer_callback_query(call.id, "Только для владельца платформы", show_alert=True)
        except Exception: pass
        return True
    if not marker:
        try:
            bot.answer_callback_query(call.id, "Не удалось прочитать маркер этого окна", show_alert=True)
        except Exception:
            pass
        return True
    _v160_set_pending(chat_id, user_id, mode, marker, source)
    catalog, _ = _v160_annotation_roots()
    current_name = str((catalog.get(marker) or {}).get("name") or "")
    if mode == "marker":
        title = f"🏷 Название окна для {marker}"
        detail = f"\nСейчас: {current_name}" if current_name else ""
        prompt = f"{title}{detail}\n\nНапишите постоянное понятное имя этого окна одним сообщением."
        placeholder = "Например: Меню напоминалок"
    else:
        label = f" — {current_name}" if current_name else ""
        prompt = f"📝 ТЗ для окна {marker}{label}\n\nНапишите, что нужно изменить в этом окне или его логике."
        placeholder = "Опишите ТЗ для этого окна"
    try:
        bot.send_message(
            chat_id, prompt,
            reply_to_message_id=message_id,
            allow_sending_without_reply=True,
            reply_markup=types.ForceReply(selective=True, input_field_placeholder=placeholder),
        )
        bot.answer_callback_query(call.id, "Контекст окна зафиксирован")
    except Exception:
        pass
    return True


def _v160_export_text(kind: str) -> tuple[str, str]:
    catalog, tz_rows = _v160_annotation_roots()
    now_s = now_local().strftime("%Y-%m-%d %H:%M:%S")
    if kind == "markers":
        lines = ["МАРКИРОВКИ ОКОН", f"Версия: {VERSION}", f"Создано: {now_s}", ""]
        actions_by_marker = defaultdict(list)
        try:
            for action, marker in (WINDOW_MARKER_CONSTANTS or {}).items():
                marker = str(marker or "").upper().strip()
                if marker and str(action) not in actions_by_marker[marker]:
                    actions_by_marker[marker].append(str(action))
        except Exception:
            pass
        markers = set(actions_by_marker) | {str(x).upper() for x in catalog}
        def _sort_marker(m):
            m = str(m); found = _v160_re.search(r"(\d+)$", m)
            return (m[:1], int(found.group(1)) if found else 999999, m)
        for marker in sorted(markers, key=_sort_marker):
            row = dict(catalog.get(marker) or {})
            src = dict(row.get("source") or {})
            actions = actions_by_marker.get(marker) or []
            action_text = ", ".join(actions[:20]) + (f" … ещё {len(actions)-20}" if len(actions) > 20 else "")
            lines.extend([
                f"{marker} = {row.get('name') or 'без пользовательского имени'}",
                f"  constant_actions: {action_text or '—'}",
                f"  first_line: {src.get('first_line') or '—'}",
                f"  callback_when_named: {src.get('callback') or '—'}",
                f"  purpose/action: {src.get('purpose') or src.get('action') or '—'}",
                f"  caller: {src.get('caller') or '—'}",
                f"  chat_id: {src.get('chat_id') or '—'}; message_id: {src.get('message_id') or '—'}",
                f"  named_at: {row.get('last_named_at') or '—'}",
                "",
            ])
        return "Маркировки_окон", "\n".join(lines).rstrip() + "\n"
    lines = ["ТЗ ПО ОКНАМ", f"Версия: {VERSION}", f"Создано: {now_s}", ""]
    for row in list(tz_rows):
        src = dict((row or {}).get("source") or {})
        lines.extend([
            f"[{row.get('at')}] {row.get('marker')} — {row.get('window_name') or (catalog.get(str(row.get('marker'))) or {}).get('name') or 'без имени'}",
            f"Источник: chat={src.get('chat_id') or '—'} msg={src.get('message_id') or '—'} callback={src.get('callback') or '—'}",
            str(row.get("text") or ""),
            "",
            "---",
            "",
        ])
    return "ТЗ_окон", "\n".join(lines).rstrip() + "\n"


def _v160_send_annotation_export(chat_id: int, kind: str):
    _file_job_progress("формирую файл", force=True)
    base, content = _v160_export_text(kind)
    folder = _v160_tempfile.mkdtemp(prefix="v160_annotations_")
    path = _v160_os.path.join(folder, f"{base}_{now_local().strftime('%Y_%m_%d_%H%M%S')}.txt")
    try:
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(content)
        _file_job_progress("отправляю файл в Telegram", force=True)
        with open(path, "rb") as fh:
            bot.send_document(int(chat_id), fh, caption=f"{'🏷 Маркировки окон' if kind == 'markers' else '📝 ТЗ по окнам'} · {VERSION}")
        return True
    finally:
        _v160_shutil.rmtree(folder, ignore_errors=True)


def _v160_handle_special_callback(call, resolved: str) -> bool:
    if resolved == "v160:marker_capture":
        return _v160_begin_capture(call, "marker")
    if resolved == "v160:tz_capture":
        return _v160_begin_capture(call, "tz")
    if resolved in {"v160:export_markers", "v160:export_tz"}:
        user_id = int(getattr(getattr(call, "from_user", None), "id", 0) or 0)
        if not _v160_can_annotate(user_id):
            try: bot.answer_callback_query(call.id, "Только для владельца платформы", show_alert=True)
            except Exception: pass
            return True
        chat_id = int(call.message.chat.id)
        kind = "markers" if resolved.endswith("markers") else "tz"
        label = "Маркировки окон" if kind == "markers" else "ТЗ по окнам"
        ok, reason = submit_interactive_file_job(chat_id, f"window_{kind}", label, _v160_send_annotation_export, chat_id, kind)
        if not ok:
            send_and_auto_delete(chat_id, f"⏳ {reason or 'Сейчас уже формируется другой файл.'}", 10)
        try:
            bot.answer_callback_query(call.id, "Формирую файл" if ok else "Файл уже формируется")
        except Exception:
            pass
        return True
    return False


def _v160_install_callback_intercept() -> int:
    count = 0
    for handler in list(getattr(bot, "callback_query_handlers", []) or []):
        if not isinstance(handler, dict):
            continue
        original = handler.get("function")
        if not callable(original) or getattr(original, "_v160_stability", False):
            continue
        def _wrapped(call, _original=original):
            raw = str(getattr(call, "data", "") or "")
            resolved = raw
            try:
                resolver = globals().get("resolve_short_callback")
                if callable(resolver):
                    resolved = str(resolver(raw) or raw)
            except Exception:
                pass
            if _v160_exact_callback_duplicate(call):
                try:
                    bot.answer_callback_query(call.id, "Уже принято")
                except Exception:
                    pass
                return None
            if _v160_handle_special_callback(call, resolved):
                return None
            # Defeat the old 1.2s same-data suppression, while exact callback IDs remain protected above.
            _v160_clear_legacy_same_button_suppression(call)
            return _original(call)
        _wrapped._v160_stability = True
        _wrapped.__name__ = getattr(original, "__name__", "callback_handler")
        handler["function"] = _wrapped
        count += 1
    return count


def _v160_capture_filter(msg) -> bool:
    try:
        text = str(getattr(msg, "text", "") or "").strip()
        chat_id = int(msg.chat.id)
        user_id = int(getattr(getattr(msg, "from_user", None), "id", 0) or 0)
        if _v160_get_pending(chat_id, user_id):
            return True
        low = text.casefold()
        return low.startswith("/iz-mr") or low.startswith("/iz_mr") or low.startswith("/tz")
    except Exception:
        return False


def _v160_reply_source(msg):
    reply = getattr(msg, "reply_to_message", None)
    if reply is None:
        return 0, "", {}
    mid = int(getattr(reply, "message_id", 0) or 0)
    text = str(getattr(reply, "text", None) or getattr(reply, "caption", None) or "")
    marker = _v160_marker_from_text(text)
    source = _v160_source_meta(int(msg.chat.id), mid, marker, text) if marker else {}
    return mid, marker, source


def _v160_capture_message(msg):
    chat_id = int(msg.chat.id)
    user_id = int(getattr(getattr(msg, "from_user", None), "id", 0) or 0)
    if not _v160_can_annotate(user_id):
        return
    text = str(getattr(msg, "text", "") or "").strip()
    low = text.casefold()
    pending = _v160_get_pending(chat_id, user_id, pop=False)

    # Manual direct forms are accepted too: /iz-mr Ф191 Имя  or  /tz Ф191 текст ТЗ
    command_mode = None
    rest = ""
    if low.startswith("/iz-mr"):
        command_mode = "marker"; rest = text[len("/iz-mr"):].strip()
    elif low.startswith("/iz_mr"):
        command_mode = "marker"; rest = text[len("/iz_mr"):].strip()
    elif low.startswith("/tz"):
        command_mode = "tz"; rest = text[len("/tz"):].strip()

    if command_mode is not None:
        match = _v160_re.match(r"^([СФПОВсов]\d{1,6})\s+(.+)$", rest, flags=_v160_re.IGNORECASE | _v160_re.DOTALL)
        if match:
            marker = str(match.group(1)).upper(); body = str(match.group(2)).strip()
            _, reply_marker, source = _v160_reply_source(msg)
            if not source or reply_marker != marker:
                source = _v160_source_meta(chat_id, int(getattr(msg, "message_id", 0) or 0), marker, "")
            if command_mode == "marker":
                row = _v160_save_marker_name(chat_id, user_id, marker, body, source)
                send_and_auto_delete(chat_id, f"✅ {marker} = {row.get('name')}", 8)
            else:
                _v160_save_tz(chat_id, user_id, marker, body, source)
                send_and_auto_delete(chat_id, f"✅ ТЗ для {marker} сохранено.", 8)
            return
        if not pending:
            _, marker, source = _v160_reply_source(msg)
            if marker:
                _v160_set_pending(chat_id, user_id, command_mode, marker, source)
                pending = _v160_get_pending(chat_id, user_id)
            else:
                send_and_auto_delete(chat_id, "ℹ️ Используйте кнопки /iz-mr или /tz прямо под нужным окном.", 10)
                return
        # Command itself is not the annotation body.
        send_and_auto_delete(chat_id, f"✍️ Теперь пришлите {'название окна' if command_mode == 'marker' else 'текст ТЗ'} одним сообщением.", 8)
        return

    pending = _v160_get_pending(chat_id, user_id, pop=True)
    if not pending:
        return
    if text.casefold() in {"/cancel", "отмена"}:
        send_and_auto_delete(chat_id, "❌ Ввод отменён.", 6)
        return
    marker = str(pending.get("marker") or "")
    source = dict(pending.get("source") or {})
    if str(pending.get("mode")) == "marker":
        row = _v160_save_marker_name(chat_id, user_id, marker, text, source)
        send_and_auto_delete(chat_id, f"✅ Запомнил: {marker} = {row.get('name')}", 8)
    else:
        _v160_save_tz(chat_id, user_id, marker, text, source)
        send_and_auto_delete(chat_id, f"✅ ТЗ для {marker} сохранено.", 8)


def _v160_install_message_capture() -> int:
    try:
        decorator = bot.message_handler(func=_v160_capture_filter, content_types=["text"])
        decorator(_v160_capture_message)
        handlers = getattr(bot, "message_handlers", None)
        if isinstance(handlers, list) and handlers:
            row = handlers.pop()
            handlers.insert(0, row)
        return 1
    except Exception:
        return 0


_V160_CALLBACK_HANDLERS = _v160_install_callback_intercept()
_V160_MESSAGE_HANDLERS = _v160_install_message_capture()


# ---------------------------------------------------------------------------
# 7) Make v160 full-state export restorable.
# ---------------------------------------------------------------------------
def _v153_validate_restore_gz(gz_path: str) -> tuple[dict, str]:
    folder = _v160_tempfile.mkdtemp(prefix="v160_restore_validate_")
    raw = _v160_os.path.join(folder, "restore.sqlite3")
    try:
        with _v160_gzip.open(gz_path, "rb") as fin, open(raw, "wb") as fout:
            _v160_shutil.copyfileobj(fin, fout, 1024 * 1024)
        conn = _v160_sqlite3.connect(raw)
        try:
            integrity = str(conn.execute("PRAGMA integrity_check").fetchone()[0])
            if integrity.lower() != "ok":
                raise RuntimeError(f"SQLite integrity_check: {integrity}")
            row = conn.execute("SELECT v FROM meta WHERE kind='v153_export' AND k='manifest'").fetchone()
            if not row:
                raise RuntimeError("manifest v153 not found")
            manifest = _v160_json.loads(row[0])
        finally:
            conn.close()
        if str(manifest.get("kind")) != "telegram_bot_full_state_v153":
            raise RuntimeError("unknown export kind")
        if int(manifest.get("schema_version") or 0) != int(V153_EXPORT_SCHEMA):
            raise RuntimeError("unsupported export schema")
        export_version = str(manifest.get("bot_version") or "")
        if not export_version.startswith((
            "bot_v153_", "bot_v154_", "bot_v155_", "bot_v156_", "bot_v157_", "bot_v158_", "bot_v159_", "bot_v160_",
        )):
            raise RuntimeError(f"unsupported bot version: {export_version or 'missing'}")
        checksum = _v153_db_logical_checksum(raw)
        if checksum != str(manifest.get("checksum") or ""):
            raise RuntimeError("checksum mismatch")
        return manifest, raw
    except Exception:
        _v160_shutil.rmtree(folder, ignore_errors=True)
        raise


# Remove transient helper/file windows left by v159 or by an interrupted v160 export.
def _v160_cleanup_legacy_transient_windows() -> None:
    touched = []
    try:
        chats = list((data.get("chats") or {}).items())
    except Exception:
        chats = []
    for cid_s, store in chats:
        try:
            cid = int(cid_s)
        except Exception:
            continue
        if not isinstance(store, dict):
            continue
        mids = set()
        for key in ("command_window_id", "_v160_file_status_msg_id"):
            try:
                mid = int(store.get(key) or 0)
                if mid: mids.add(mid)
            except Exception:
                pass
            if key in store:
                store[key] = None
        if mids:
            touched.append(cid)
            for mid in mids:
                try:
                    bot.delete_message(cid, mid)
                except Exception:
                    pass
                try:
                    unregister_open_window(cid, mid)
                except Exception:
                    pass
    if touched:
        try:
            save_data(data, chat_ids=touched)
        except Exception:
            pass
        for cid in touched:
            try: schedule_quick_backup(cid, 0.5)
            except Exception: pass
    try:
        bot_journal("v160_transient_window_cleanup", None, f"chats={len(touched)}")
    except Exception:
        pass


_V160_PREV_RUNTIME_MARK_READY = globals().get("runtime_mark_ready")
def runtime_mark_ready(detail: str = ""):
    result = _V160_PREV_RUNTIME_MARK_READY(detail) if callable(_V160_PREV_RUNTIME_MARK_READY) else None
    try:
        _v160_schedule("v160-transient-cleanup", 2.0, _v160_cleanup_legacy_transient_windows)
    except Exception:
        pass
    return result


try:
    bot_journal(
        "v160_stability_parallel_windows_annotations_installed",
        int(OWNER_ID or 0),
        f"generic_process_ui=off; file_ui=Ф233 dedicated_timer; helper_ui=Ф234 dedicated_timer; "
        f"parallel_windows=on; callback_handlers={_V160_CALLBACK_HANDLERS}; annotation_handler={_V160_MESSAGE_HANDLERS}",
    )
except Exception:
    pass

# v160_stability_parallel_windows_annotations
