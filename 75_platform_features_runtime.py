# v182_restore_unified
# ---- integrated from 113_v163_audit_hardening.py ----
"""v163: priority /start, per-window navigation lanes, fast callback ACK, export reliability, TZ window fixes."""

import calendar as _v163_calendar
import contextlib as _v163_contextlib
import threading as _v163_threading
import time as _v163_time

VERSION = "bot_v163_audit_hardening"

# ---------------------------------------------------------------------------
# 1) Priority lanes: /start never waits behind ordinary content for the chat.
# Navigation callbacks are serialized per concrete Telegram message/window.
# Unknown/financial callbacks stay on the original per-chat UI lane.
# ---------------------------------------------------------------------------
START_UI_TASK_POOL = KeyedTaskPool(
    "start-ui",
    _env_int("START_UI_WORKERS", 2, 1, 4),
    _env_int("START_UI_MAX_PENDING", 120, 20, 600),
)
# v168 clean-core: v166 has the active per-window pool. Do not keep three dead v163 workers alive.
WINDOW_UI_TASK_POOL = UI_TASK_POOL

# Receipt-level callback ACK: clear Telegram spinner quickly even if the action itself takes longer.
CALLBACK_RECEIPT_ACK_DELAY_SECONDS = 0.15

_V163_WINDOW_EXEC_LOCK_GUARD = _v163_threading.RLock()
_V163_WINDOW_EXEC_LOCKS = {}
_V163_START_EXEC_LOCK_GUARD = _v163_threading.RLock()
_V163_START_EXEC_LOCKS = {}


def _v163_lock_for(table: dict, guard, key):
    with guard:
        lock = table.get(key)
        if lock is None:
            lock = _v163_threading.RLock()
            table[key] = lock
        return lock


def _v163_start_payload(payload: dict) -> bool:
    try:
        msg = (payload or {}).get("message") or {}
        text = str(msg.get("text") or "").strip()
        if not text:
            return False
        cmd = text.split(None, 1)[0].split("@", 1)[0].casefold()
        return cmd in {"/start", "/старт"}
    except Exception:
        return False


def _v163_callback_parts(payload: dict):
    try:
        cq = (payload or {}).get("callback_query") or {}
        msg = cq.get("message") or {}
        chat = msg.get("chat") or {}
        return str(cq.get("data") or ""), int(chat.get("id") or 0), int(msg.get("message_id") or 0)
    except Exception:
        return "", 0, 0


def _v163_is_switch_callback(raw: str) -> bool:
    low = str(raw or "").casefold()
    try:
        fn = globals().get("_v160_is_switch_callback")
        if callable(fn) and fn(raw):
            return True
    except Exception:
        pass
    return any(x in low for x in ("toggle", ":on", ":off", "enable", "disable"))


def _v163_is_navigation_callback(raw: str) -> bool:
    """Only UI/navigation actions are allowed to bypass the chat-wide business lock."""
    raw = str(raw or "")
    low = raw.casefold()
    if _v163_is_switch_callback(raw):
        return False
    if raw in {
        "nav_prev", "info_close", "journal_back", "journal_chats_back", "itmr_back_info",
        "fw_back_src", "process_center", "problem_tasks",
    }:
        return True
    if low.startswith("d:"):
        try:
            cmd = raw.split(":", 2)[2].casefold()
        except Exception:
            cmd = ""
        if cmd in {"back_main", "info"}:
            return True
    # Explicit back/close callbacks from submenus are visual navigation, not business mutation.
    if ("back" in low or low.endswith("_close") or low.startswith("close_")):
        dangerous = ("delete", "remove", "confirm", "save", "apply", "send", "pay", "expense", "income")
        if not any(x in low for x in dangerous):
            return True
    return False


def _v177_legacy_0325_v163_webhook_select_lane(payload: dict, update_type: str, update_key):
    """Called by 99_web_runtime at request time after every module is loaded."""
    if str(update_type) == "message" and _v163_start_payload(payload):
        chat_id = _extract_update_chat_id(payload)
        return START_UI_TASK_POOL, f"start:{chat_id if chat_id is not None else update_key}"
    if str(update_type) == "callback_query":
        raw, chat_id, message_id = _v163_callback_parts(payload)
        if _v163_is_navigation_callback(raw) and chat_id and message_id:
            return WINDOW_UI_TASK_POOL, f"window:{chat_id}:{message_id}"
        return UI_TASK_POOL, f"ui:{chat_id if chat_id else update_key}"
    return WEBHOOK_TASK_POOL, update_key
try: _v177_legacy_0325_v163_webhook_select_lane.__name__ = 'v163_webhook_select_lane'
except Exception: pass
v163_webhook_select_lane = _v177_legacy_0325_v163_webhook_select_lane


# ---------------------------------------------------------------------------
# 2) Execution locking: priority /start and safe navigation must not re-enter
#    the same chat-wide lock used by long finance/forward operations.
# ---------------------------------------------------------------------------
def _v177_legacy_0074_execute_telegram_payload(payload: dict, update_id=None, update_chat_id=None, update_type: str = "other"):
    update = telebot.types.Update.de_json(payload)
    if update_chat_id is None:
        update_chat_id = _extract_update_chat_id(payload) if isinstance(payload, dict) else None
    previous_ctx = getattr(_TELEGRAM_UPDATE_CONTEXT, "value", None)
    critical_callback_target = _durable_callback_target_chat(payload) if isinstance(payload, dict) else None
    callback_data = ""
    source_message_id = None
    source_user_id = None
    try:
        if isinstance(payload, dict):
            callback = payload.get("callback_query") or {}
            if isinstance(callback, dict):
                callback_data = str(callback.get("data") or "")
                source_user_id = ((callback.get("from") or {}).get("id") if isinstance(callback.get("from"), dict) else None)
                callback_message = callback.get("message") or {}
                if isinstance(callback_message, dict):
                    source_message_id = callback_message.get("message_id")
            if source_message_id is None:
                message_payload = payload.get("message") or payload.get("edited_message") or payload.get("channel_post") or payload.get("edited_channel_post") or {}
                if isinstance(message_payload, dict):
                    source_message_id = message_payload.get("message_id")
                    source_user_id = source_user_id or ((message_payload.get("from") or {}).get("id") if isinstance(message_payload.get("from"), dict) else None)
    except Exception:
        callback_data = ""

    _TELEGRAM_UPDATE_CONTEXT.value = {
        "update_id": update_id,
        "chat_id": update_chat_id,
        "update_type": str(update_type or "other"),
        "callback_data": callback_data,
        "message_id": source_message_id,
        "user_id": source_user_id,
        "critical_callback": critical_callback_target is not None,
        "critical_callback_target": critical_callback_target,
        "deferred_quick_chats": set(),
    }
    execution_ctx = {}
    try:
        with state_chat_context(update_chat_id):
            if update_chat_id is None:
                lock_ctx = _v163_contextlib.nullcontext()
            elif str(update_type) == "message" and _v163_start_payload(payload):
                lock_ctx = _v163_lock_for(_V163_START_EXEC_LOCKS, _V163_START_EXEC_LOCK_GUARD, int(update_chat_id))
            elif str(update_type) == "callback_query" and _v163_is_navigation_callback(callback_data) and source_message_id:
                lock_ctx = _v163_lock_for(
                    _V163_WINDOW_EXEC_LOCKS,
                    _V163_WINDOW_EXEC_LOCK_GUARD,
                    (int(update_chat_id), int(source_message_id)),
                )
            else:
                lock_ctx = chat_lock_for(int(update_chat_id))
            with lock_ctx:
                bot.process_new_updates([update])
        execution_ctx = _durable_execution_context_snapshot()
        # Preserve v150 exact-once command receipts after replacing its wrapper.
        try:
            fn = globals().get("_v150_store_receipt")
            if callable(fn):
                fn(payload)
        except Exception as exc:
            try: log_error(f"v163 command receipt: {exc}")
            except Exception: pass
    finally:
        if not execution_ctx:
            execution_ctx = _durable_execution_context_snapshot()
        if previous_ctx is None:
            try: delattr(_TELEGRAM_UPDATE_CONTEXT, "value")
            except Exception: pass
        else:
            _TELEGRAM_UPDATE_CONTEXT.value = previous_ctx
    return execution_ctx
try: _v177_legacy_0074_execute_telegram_payload.__name__ = '_execute_telegram_payload'
except Exception: pass
_execute_telegram_payload = _v177_legacy_0074_execute_telegram_payload


# ---------------------------------------------------------------------------
# 3) F111/F113/F114 exact-export UX from the user's window TZ.
#    Current day uses 📅, not the expense 📝 marker. End calendar has a one-tap
#    "to end of current day" shortcut.
# ---------------------------------------------------------------------------
def _v163_export_day_label(chat_id: int | None, day_key: str, day_num: int) -> str:
    if str(day_key) == str(today_key()):
        return f"📅{int(day_num)}"
    try:
        if _v154_day_has_expense(chat_id, day_key):
            return f"📝{int(day_num)}"
    except Exception:
        pass
    return str(int(day_num))


def _export_calendar_start_keyboard(view_year: int, view_month: int, return_day_key: str, chat_id: int | None = None):
    kb = types.InlineKeyboardMarkup(row_width=7)
    last_day = _v163_calendar.monthrange(int(view_year), int(view_month))[1]
    buttons = []
    for day_num in range(1, last_day + 1):
        day_key = _date_key_from_ymd(view_year, view_month, day_num)
        buttons.append(IB(_v163_export_day_label(chat_id, day_key, day_num), callback_data=export_callback(
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
    kb = types.InlineKeyboardMarkup(row_width=7)
    last_day = _v163_calendar.monthrange(int(view_year), int(view_month))[1]
    buttons = []
    for day_num in range(1, last_day + 1):
        day_key = _date_key_from_ymd(view_year, view_month, day_num)
        if day_key < start_key:
            buttons.append(IB("·", callback_data="none"))
        else:
            buttons.append(IB(_v163_export_day_label(chat_id, day_key, day_num), callback_data=export_callback(
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
    td = str(today_key())
    if td >= str(start_key):
        kb.row(IB(
            f"⏹ До конца текущего дня · {fmt_date_ddmmyy(td)}",
            callback_data=export_callback(f"v163_exp_end_today:{start_key}:{int(start_rid)}:{return_day_key}"),
        ))
    start_dt = datetime.strptime(start_key, "%Y-%m-%d")
    kb.row(IB("🔙 Изменить начало", callback_data=export_callback(
        f"exp_pick_set_start:{start_dt.year}:{start_dt.month}:{start_dt.day}:{return_day_key}"
    )))
    return kb


try:
    WINDOW_ACTION_CODES.update({"v163_exp_end_today:*": "Ф116"})
except Exception:
    pass


def _v163_exact_today_callback(call):
    raw = str(getattr(call, "data", "") or "")
    try:
        resolver = globals().get("resolve_short_callback")
        if callable(resolver):
            raw = str(resolver(raw) or raw)
    except Exception:
        pass
    if not raw.startswith("v163_exp_end_today:"):
        return
    try:
        _, start_key, start_rid, return_day_key = raw.split(":", 3)
        chat_id = int(call.message.chat.id)
        end_key = str(today_key())
        if end_key < str(start_key):
            try: bot.answer_callback_query(call.id, "Текущий день раньше начала периода", show_alert=True)
            except Exception: pass
            return
        try: bot.answer_callback_query(call.id)
        except Exception: pass
        store = get_chat_store(chat_id)
        text = (
            "🎯 Точный период выбран\n\n"
            f"▶️ {exact_boundary_text(store, start_key, int(start_rid), True)}\n"
            f"⏹ {exact_boundary_text(store, end_key, 0, False)}\n\n"
            "Выберите формат файла:"
        )
        safe_edit(
            bot,
            call,
            text,
            reply_markup=_export_format_keyboard(start_key, int(start_rid), end_key, 0, return_day_key),
        )
    except Exception as exc:
        try: log_error(f"v163 exact today: {exc}")
        except Exception: pass


def _v163_exact_today_filter(call) -> bool:
    raw = str(getattr(call, "data", "") or "")
    try:
        resolver = globals().get("resolve_short_callback")
        if callable(resolver):
            raw = str(resolver(raw) or raw)
    except Exception:
        pass
    return raw.startswith("v163_exp_end_today:")


def _v163_install_exact_today_handler():
    return 0  # v179: registration/wrapper retired; final router owns callbacks


# ---------------------------------------------------------------------------
# 4) F52/F53: build the forwarding picker from the complete tenant-scoped chat
#    inventory, not only one cached list.
# ---------------------------------------------------------------------------
def _v163_forward_scope_ids() -> list[int]:
    ids = set()
    try:
        tid = str(tenant_current_id())
    except Exception:
        tid = "platform"
    try:
        for cid in tenant_chat_ids(tid) or []:
            ids.add(int(cid))
    except Exception:
        pass
    try:
        for cid in (data.get("chats", {}) or {}).keys():
            ic = int(cid)
            if str(tenant_id_for_chat(ic, create=False)) == tid:
                ids.add(ic)
    except Exception:
        pass
    try:
        scope = owner_scope_id(current_state_chat_id())
        known = (get_chat_store(int(scope)).get("known_chats") or {})
        for cid in known.keys():
            ic = int(cid)
            if str(tenant_id_for_chat(ic, create=False)) == tid:
                ids.add(ic)
    except Exception:
        pass
    try:
        for src, dsts in (data.get("forward_rules", {}) or {}).items():
            for cid in [src] + list((dsts or {}).keys()):
                ic = int(cid)
                if str(tenant_id_for_chat(ic, create=False)) == tid:
                    ids.add(ic)
    except Exception:
        pass
    try:
        root = int(owner_scope_id(current_state_chat_id()))
        if root:
            ids.add(root)
    except Exception:
        pass
    return sorted(ids, key=lambda cid: get_chat_display_name(cid).casefold())


def _v177_legacy_0181_collect_forward_picker_items(include_owner: bool = True, include_removed: bool = False):
    items = []
    owner_item = None
    try:
        root_id = int(owner_scope_id(current_state_chat_id()))
    except Exception:
        root_id = int(OWNER_ID or 0)
    for cid in _v163_forward_scope_ids():
        try:
            if (not include_removed) and is_chat_bot_removed(int(cid)):
                continue
        except Exception:
            pass
        title = get_chat_display_name(int(cid)) or f"Чат {cid}"
        if root_id and int(cid) == root_id:
            owner_item = (int(cid), title)
        else:
            items.append((int(cid), title))
    if include_owner and root_id and owner_item is None:
        owner_item = (root_id, get_chat_display_name(root_id) or f"Чат {root_id}")
    if not include_owner:
        owner_item = None
    return items, owner_item
try: _v177_legacy_0181_collect_forward_picker_items.__name__ = '_collect_forward_picker_items'
except Exception: pass
_collect_forward_picker_items = _v177_legacy_0181_collect_forward_picker_items


# ---------------------------------------------------------------------------
# 5) F233 reliability: every interactive file job must actually deliver a
#    Telegram document. Silent function return is no longer treated as success.
# ---------------------------------------------------------------------------
_V163_PREV_SEND_DOCUMENT = getattr(bot, "send_document", None)


def _v163_transient_send_error(exc) -> bool:
    low = str(exc or "").casefold()
    return any(x in low for x in (
        "too many requests", "retry after", "internal server error", "bad gateway",
        "service unavailable", "connection reset", "remote disconnected", "temporarily unavailable",
    ))


if callable(_V163_PREV_SEND_DOCUMENT):
    def _v163_send_document(chat_id, document, *args, **kwargs):
        last_exc = None
        for attempt in range(1, 4):
            try:
                result = _V163_PREV_SEND_DOCUMENT(chat_id, document, *args, **kwargs)
                try:
                    ctx = getattr(_FILE_JOB_CONTEXT, "value", None)
                    if isinstance(ctx, dict):
                        key = str(ctx.get("key") or "")
                        with _FILE_JOB_LOCK:
                            st = _FILE_JOB_STATE.get(key)
                            if isinstance(st, dict):
                                st["telegram_documents_sent"] = int(st.get("telegram_documents_sent") or 0) + 1
                                st["telegram_document_message_id"] = int(getattr(result, "message_id", 0) or 0)
                except Exception:
                    pass
                return result
            except Exception as exc:
                last_exc = exc
                if attempt >= 3 or not _v163_transient_send_error(exc):
                    raise
                _v163_time.sleep(0.35 if attempt == 1 else 1.0)
        if last_exc:
            raise last_exc
    bot.send_document = _v163_send_document


_V163_BASE_FILE_RUNNER = globals().get("_interactive_file_job_runner")


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
                st["started_monotonic"] = _v163_time.monotonic()
                st["phase"] = "запуск"
                st["telegram_documents_sent"] = 0
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
        with _FILE_JOB_LOCK:
            st = _FILE_JOB_STATE.get(key)
            sent = int((st or {}).get("telegram_documents_sent") or 0) if isinstance(st, dict) else 0
        ok = (result is not False) and sent > 0
        if not ok:
            error_text = "файл сформирован, но Telegram не подтвердил отправку документа"
    except Exception as exc:
        error_text = str(exc)[:300]
        try: log_error(f"INTERACTIVE FILE JOB v163 {job_meta.get('kind')}: {exc}")
        except Exception: pass
    finally:
        now_m = _v163_time.monotonic()
        with _FILE_JOB_LOCK:
            st = _FILE_JOB_STATE.get(key)
            if isinstance(st, dict):
                chat_id = int(st.get("chat_id"))
                msg_id = st.get("status_msg_id")
                label = str(st.get("label") or "Файл")
                started = float(st.get("started_monotonic") or st.get("queued_monotonic") or now_m)
                sent = int(st.get("telegram_documents_sent") or 0)
                elapsed = _file_job_elapsed_text(now_m - started)
            else:
                chat_id = int(job_meta.get("chat_id") or 0)
                msg_id = None
                label = str(job_meta.get("label") or "Файл")
                sent = 0
                elapsed = "0:00"
        try:
            if msg_id:
                close_s = internal_timer_seconds("file_status_close", 15)
                if ok:
                    final = f"✅ {label}\nОтправлено в чат за {elapsed}.\nОкно закроется через {_format_duration_short(close_s)}."
                else:
                    final = (
                        f"⚠️ {label}\nЗавершено за {elapsed}.\n"
                        f"{error_text or 'Telegram не подтвердил отправку.'}\n"
                        f"Окно закроется через {_format_duration_short(close_s)}."
                    )
                final = _v159_force_marker(final, "Ф233", "⏳")
                bot.edit_message_text(final, chat_id=chat_id, message_id=int(msg_id))
                _v161_schedule_delete(chat_id, int(msg_id), close_s, "file-close")
        except Exception:
            pass
        try:
            bot_journal(
                "file_job_done" if ok else "file_job_send_missing",
                chat_id,
                f"kind={job_meta.get('kind')} elapsed={elapsed} sent_documents={sent} error={error_text}",
                "INFO" if ok else "WARN",
            )
        except Exception:
            pass
        try: _v160_cancel_timer(f"v160:file-tick:{key}")
        except Exception: pass
        try: DELAYED_SCHEDULER.cancel(f"file-job-tick:{key}")
        except Exception: pass
        with _FILE_JOB_LOCK:
            _FILE_JOB_STATE.pop(key, None)
        if previous is None:
            try: delattr(_FILE_JOB_CONTEXT, "value")
            except Exception: pass
        else:
            _FILE_JOB_CONTEXT.value = previous


# Restore accepts this merged release.

_V163_EXACT_TODAY_HANDLERS = 0  # v179 final callback router

try:
    bot_journal(
        "v163_audit_hardening_installed",
        int(OWNER_ID or 0),
        "start_lane=priority; navigation_lane=per_window; ack=0.15; webhook_secret_path=1; "
        "F111_today=calendar; F113_today_shortcut=1; F52_F53_scope_union=1; F233_send_verified=1",
    )
except Exception:
    pass

# ---- integrated from 114_v164_circle_hierarchy_spaces.py ----
"""v164: explicit owner / first-circle / second-circle hierarchy with isolated tenants.

Rules:
- OWNER_ID private owner contour is circle 0 / platform tenant only.
- A chat that appears directly (normal /start/message, not through a first-circle invite) is circle 1
  and gets its own dedicated tenant.
- A chat joined with a chat-link created from a circle-1 chat is circle 2. It also gets its own
  dedicated tenant and stores parent_first_chat_id instead of joining the parent's tenant.
- Platform owner can administer circle lists globally without mixing their data into platform tenant.
- Circle-1 managers can administer their own circle-2 descendants.
"""

import copy as _v164_copy
import gzip as _v164_gzip
import hashlib as _v164_hashlib
import json as _v164_json
import os as _v164_os
import shutil as _v164_shutil
import sqlite3 as _v164_sqlite3
import tempfile as _v164_tempfile
import threading as _v164_threading
import time as _v164_time

VERSION = "bot_v164_circle_hierarchy_spaces"
V164_CIRCLE_SCHEMA = 1

_V164_LOCK = _v164_threading.RLock()
_V164_WINDOW_VIEW_LOCK = _v164_threading.RLock()
_V164_WINDOW_VIEW = {}
_V164_MIGRATING = False

# Preserve the implementation that existed before v164. Calls remain useful for the low-level
# tenant storage mechanics, but v164 decides hierarchy/classification itself.
_V164_PREV_TENANT_ID_FOR_CHAT = globals().get("tenant_id_for_chat")
_V164_PREV_TENANT_NOTE_CHAT_SEEN = globals().get("tenant_note_chat_seen")
_V164_PREV_TENANT_BIND_CHAT = globals().get("tenant_bind_chat")
_V164_PREV_TENANT_CAN_MANAGE = globals().get("tenant_can_manage")
_V164_PREV_TENANT_CREATE_INVITE = globals().get("tenant_create_invite")
_V164_PREV_TENANT_CONSUME_INVITE = globals().get("tenant_consume_invite")
_V164_PREV_TENANT_SAME_SPACE = globals().get("tenant_same_space")
_V164_PREV_ADD_FORWARD_LINK = globals().get("add_forward_link")
_V164_PREV_COLLECT_FORWARD_PAIRS = globals().get("collect_forward_pairs_for_menu")
_V164_PREV_BUILD_FORWARD_NEW_MENU = globals().get("build_forward_new_menu")
_V164_PREV_BUILD_FORWARD_SOURCE_MENU = globals().get("build_forward_source_menu")
_V164_PREV_BUILD_FORWARD_TARGET_MENU = globals().get("build_forward_target_menu")
_V164_PREV_BUILD_QUICK_BALANCE_MODE_MENU = globals().get("build_quick_balance_mode_menu")
_V164_PREV_BUILD_CHAT_DESCRIPTION_MENU = globals().get("build_chat_description_menu")
_V164_PREV_RESTORE_VALIDATE = globals().get("_v153_validate_restore_gz")


def _v164_owner_id() -> int:
    try:
        return int(OWNER_ID or 0)
    except Exception:
        return 0


def _v164_now() -> str:
    try:
        return _tenant_now()
    except Exception:
        return _v164_time.strftime("%Y-%m-%dT%H:%M:%S")


def _v164_root() -> dict:
    gs = data.setdefault("global_settings", {})
    root = gs.get("circle_hierarchy_v164")
    if not isinstance(root, dict):
        root = {}
        gs["circle_hierarchy_v164"] = root
    root.setdefault("schema_version", V164_CIRCLE_SCHEMA)
    root.setdefault("chat_meta", {})
    root.setdefault("migration_runs", 0)
    root.setdefault("global_forward_pairs", {})
    root.setdefault("created_at", _v164_now())
    return root


def _v164_mapping() -> dict:
    try:
        return _tenants_root().setdefault("chat_to_tenant", {})
    except Exception:
        return {}


def _v164_tenant_id_for_root_chat(chat_id: int) -> str:
    seed = _v164_hashlib.sha256(f"chat:{int(chat_id)}".encode("utf-8")).hexdigest()[:12]
    return f"chat_{seed}"


def _v164_meta_raw(chat_id: int) -> dict | None:
    try:
        row = (_v164_root().get("chat_meta") or {}).get(str(int(chat_id)))
        return row if isinstance(row, dict) else None
    except Exception:
        return None


def _v164_set_meta(chat_id: int, circle: int, parent_first_chat_id: int = 0, source: str = "", tenant_id: str = "") -> dict:
    cid = int(chat_id)
    circle = int(circle)
    parent = int(parent_first_chat_id or 0)
    if circle != 2:
        parent = 0
    root = _v164_root()
    meta = root.setdefault("chat_meta", {}).setdefault(str(cid), {})
    old_circle = int(meta.get("circle") or -1)
    old_parent = int(meta.get("parent_first_chat_id") or 0)
    meta.update({
        "chat_id": cid,
        "circle": circle,
        "parent_first_chat_id": parent,
        "tenant_id": str(tenant_id or meta.get("tenant_id") or ""),
        "source": str(source or meta.get("source") or ("owner" if circle == 0 else "direct")),
        "updated_at": _v164_now(),
    })
    meta.setdefault("created_at", _v164_now())
    if old_circle != circle or old_parent != parent:
        meta["classification_changed_at"] = _v164_now()
    return meta


def _v164_circle_from_legacy(chat_id: int) -> tuple[int, int, str]:
    """Infer legacy v148 relation without mutating state."""
    cid = int(chat_id)
    if cid == _v164_owner_id() and cid:
        return 0, 0, TENANT_PLATFORM_ID
    mapping = _v164_mapping()
    tid = str(mapping.get(str(cid)) or "")
    row = tenant_get(tid) if tid else None
    if row:
        root_chat = int(row.get("root_chat_id") or 0)
        if tid == TENANT_PLATFORM_ID:
            # v148 used to absorb chats touched by the platform owner into the platform tenant.
            # In v164 every non-owner chat is an isolated first-circle chat unless a link says otherwise.
            return 1, 0, tid
        if root_chat and cid != root_chat:
            return 2, root_chat, tid
        return 1, 0, tid
    return 1, 0, ""


def _v164_circle_info(chat_id: int, create: bool = False, source: str = "direct") -> dict:
    cid = int(chat_id)
    meta = _v164_meta_raw(cid)
    if meta:
        return meta
    circle, parent, tid = _v164_circle_from_legacy(cid)
    if not create:
        return {
            "chat_id": cid,
            "circle": int(circle),
            "parent_first_chat_id": int(parent or 0),
            "tenant_id": str(tid or ""),
            "source": "legacy_inferred",
        }
    return _v164_ensure_isolated_chat(cid, circle, parent, actor_user_id=0, source=source)


def circle_level_for_chat(chat_id: int) -> int:
    try:
        return int(_v164_circle_info(int(chat_id), create=False).get("circle") or 0)
    except Exception:
        return 0 if int(chat_id or 0) == _v164_owner_id() else 1


def circle_parent_for_chat(chat_id: int) -> int:
    try:
        return int(_v164_circle_info(int(chat_id), create=False).get("parent_first_chat_id") or 0)
    except Exception:
        return 0


def _v164_parent_tenant_row(parent_first_chat_id: int) -> dict:
    parent_tid = str(_v164_mapping().get(str(int(parent_first_chat_id))) or _v164_tenant_id_for_root_chat(int(parent_first_chat_id)))
    return tenant_get(parent_tid) or {}


def _v164_copy_parent_managers_to_child(child_tid: str, parent_first_chat_id: int, consuming_admin: int = 0) -> None:
    parent = _v164_parent_tenant_row(parent_first_chat_id)
    parent_owner = int(parent.get("owner_user_id") or 0)
    if parent_owner:
        try:
            tenant_set_user_role(child_tid, parent_owner, "tenant_owner", changed_by=parent_owner, save=False)
        except Exception:
            pass
    for uid, item in list((parent.get("users") or {}).items()):
        try:
            iuid = int(uid)
        except Exception:
            continue
        role = str((item or {}).get("role") or "viewer")
        if role == "tenant_owner":
            role = "tenant_owner" if iuid == parent_owner else "tenant_admin"
        if role not in {"tenant_owner", "tenant_admin", "operator", "viewer"}:
            continue
        try:
            tenant_set_user_role(child_tid, iuid, role, changed_by=parent_owner, save=False)
        except Exception:
            pass
    if consuming_admin and consuming_admin != parent_owner:
        try:
            tenant_set_user_role(child_tid, int(consuming_admin), "tenant_admin", changed_by=parent_owner or consuming_admin, save=False)
        except Exception:
            pass


def _v164_ensure_isolated_chat(chat_id: int, circle: int, parent_first_chat_id: int = 0, actor_user_id: int = 0, source: str = "direct") -> dict:
    """Ensure one Telegram chat == one tenant. This is the key isolation rule in v164."""
    global _V164_MIGRATING
    cid = int(chat_id)
    circle = int(circle)
    parent = int(parent_first_chat_id or 0)
    owner_id = _v164_owner_id()
    if cid == owner_id and owner_id:
        circle, parent = 0, 0
        tid = TENANT_PLATFORM_ID
        try:
            if callable(_V164_PREV_TENANT_BIND_CHAT):
                _V164_PREV_TENANT_BIND_CHAT(cid, tid, changed_by=int(actor_user_id or owner_id), force=True)
        except Exception:
            pass
        return _v164_set_meta(cid, 0, 0, source="owner", tenant_id=tid)

    if circle not in {1, 2}:
        circle = 1
    if circle == 2 and not parent:
        circle = 1

    tid = _v164_tenant_id_for_root_chat(cid)
    # Publish the intended hierarchy before rebinding. v148 may run its forwarding-isolation
    # cleanup inside tenant_bind_chat; it must already know that parent<->child is a valid family.
    _v164_set_meta(cid, circle, parent, source=source, tenant_id=tid)
    row = tenant_get(tid)
    if not row:
        parent_row = _v164_parent_tenant_row(parent) if circle == 2 and parent else {}
        parent_owner = int(parent_row.get("owner_user_id") or 0)
        actor = int(actor_user_id or 0)
        owner_uid = parent_owner if circle == 2 and parent_owner else (actor if actor and tenant_user_is_chat_admin(cid, actor) else 0)
        # Create a deterministic tenant. The v148 helper is safe because the deterministic id is new here.
        try:
            row_tid = tenant_create(_tenant_default_name(cid), owner_uid, cid, created_by=actor, deterministic_chat_id=cid)
            tid = str(row_tid or tid)
            row = tenant_get(tid)
        except Exception:
            row = tenant_get(tid)
    if not row:
        # Last-resort direct construction; avoids ever falling back to platform ownership.
        root = _tenants_root()
        row = _tenant_normalize(tid, {
            "name": _tenant_default_name(cid), "owner_user_id": 0, "root_chat_id": cid,
            "chat_ids": [cid], "users": {}, "settings": {}, "created_by": int(actor_user_id or 0),
            "created_at": _v164_now(), "updated_at": _v164_now(),
        })
        root.setdefault("tenants", {})[tid] = row

    # A dedicated tenant must contain only this Telegram chat.
    row["root_chat_id"] = cid
    row["chat_ids"] = [cid]
    row["updated_at"] = _v164_now()
    row.setdefault("settings", {})["circle_level"] = circle
    row["settings"]["parent_first_chat_id"] = parent if circle == 2 else 0
    row["settings"]["isolation_v164"] = True

    try:
        if callable(_V164_PREV_TENANT_BIND_CHAT):
            _V164_PREV_TENANT_BIND_CHAT(cid, tid, changed_by=int(actor_user_id or 0), force=True)
        else:
            _v164_mapping()[str(cid)] = tid
    except Exception:
        _v164_mapping()[str(cid)] = tid

    if circle == 2 and parent:
        _v164_copy_parent_managers_to_child(tid, parent, consuming_admin=int(actor_user_id or 0))

    meta = _v164_set_meta(cid, circle, parent, source=source, tenant_id=tid)
    try:
        store = get_chat_store(cid)
        settings = store.setdefault("settings", {})
        settings["tenant_id"] = tid
        settings["owner_scope_id"] = cid
        settings["circle_level"] = circle
        settings["parent_first_chat_id"] = parent if circle == 2 else 0
    except Exception:
        pass
    return meta


def _v164_known_chat_ids() -> set[int]:
    ids = set()
    try:
        for raw in (data.get("chats", {}) or {}).keys():
            ids.add(int(raw))
    except Exception:
        pass
    try:
        for raw in (_v164_mapping() or {}).keys():
            ids.add(int(raw))
    except Exception:
        pass
    try:
        for row in tenant_all() or []:
            for raw in row.get("chat_ids") or []:
                ids.add(int(raw))
    except Exception:
        pass
    if _v164_owner_id():
        ids.add(_v164_owner_id())
    return ids


def _v164_migrate_legacy_hierarchy(force: bool = False) -> bool:
    """Split v148 multi-chat tenants into isolated chat tenants while preserving parent relation."""
    global _V164_MIGRATING
    with _V164_LOCK:
        if _V164_MIGRATING:
            return False
        root = _v164_root()
        # Run again after a remote restore when new legacy mappings appear; this is intentionally idempotent.
        signature_parts = []
        try:
            for tid, row in sorted((_tenants_root().get("tenants") or {}).items()):
                signature_parts.append(f"{tid}:{int((row or {}).get('root_chat_id') or 0)}:{','.join(str(int(x)) for x in ((row or {}).get('chat_ids') or []))}")
        except Exception:
            pass
        sig = _v164_hashlib.sha256("|".join(signature_parts).encode("utf-8")).hexdigest()[:20]
        if not force and str(root.get("legacy_signature") or "") == sig and int(root.get("schema_version") or 0) == V164_CIRCLE_SCHEMA:
            return False
        _V164_MIGRATING = True
        changed = False
        try:
            owner = _v164_owner_id()
            if owner:
                _v164_ensure_isolated_chat(owner, 0, source="owner")
            tenant_rows = list((_tenants_root().get("tenants") or {}).items())
            for tid, raw_row in tenant_rows:
                row = raw_row if isinstance(raw_row, dict) else {}
                root_chat = int(row.get("root_chat_id") or 0)
                chats = []
                for raw in row.get("chat_ids") or []:
                    try:
                        chats.append(int(raw))
                    except Exception:
                        pass
                if str(tid) == str(TENANT_PLATFORM_ID):
                    for cid in list(chats):
                        if cid and cid != owner:
                            _v164_ensure_isolated_chat(cid, 1, actor_user_id=0, source="legacy_platform_split")
                            changed = True
                    continue
                if root_chat:
                    _v164_ensure_isolated_chat(root_chat, 1, actor_user_id=int(row.get("owner_user_id") or 0), source="legacy_first_circle")
                    for cid in list(chats):
                        if cid and cid != root_chat:
                            _v164_ensure_isolated_chat(cid, 2, parent_first_chat_id=root_chat, actor_user_id=int(row.get("owner_user_id") or 0), source="legacy_second_circle")
                            changed = True
            # Any historical chat not represented in the tenant table becomes an isolated first-circle chat.
            for cid in sorted(_v164_known_chat_ids()):
                if cid == owner:
                    continue
                if not _v164_meta_raw(cid):
                    circle, parent, _ = _v164_circle_from_legacy(cid)
                    _v164_ensure_isolated_chat(cid, circle, parent, actor_user_id=0, source="legacy_known_chat")
                    changed = True
            root["schema_version"] = V164_CIRCLE_SCHEMA
            root["legacy_signature"] = sig
            root["migration_runs"] = int(root.get("migration_runs") or 0) + 1
            root["last_migration_at"] = _v164_now()
            if changed:
                try:
                    save_data(data, full=True)
                    schedule_delta_backup(owner or 0, delay=0.5, reason="v164_circle_migration")
                except Exception:
                    pass
                try:
                    bot_journal("v164_circle_migration", owner or 0, f"known={len(_v164_known_chat_ids())}; changed=1")
                except Exception:
                    pass
            return changed
        finally:
            _V164_MIGRATING = False


def tenant_id_for_chat(chat_id: int | None, create: bool = False, actor_user_id: int | None = None) -> str:
    explicit = getattr(_TENANT_CONTEXT, "tenant_id", None)
    if explicit:
        return str(explicit)
    try:
        cid = int(chat_id or 0)
    except Exception:
        cid = 0
    if not cid:
        return TENANT_PLATFORM_ID if not create else ""
    if cid == _v164_owner_id() and cid:
        if create:
            _v164_ensure_isolated_chat(cid, 0, actor_user_id=int(actor_user_id or 0), source="owner")
        return TENANT_PLATFORM_ID
    tid = str(_v164_mapping().get(str(cid)) or "")
    meta = _v164_meta_raw(cid)
    if tid and tenant_get(tid):
        # Lazily repair a legacy shared tenant as soon as it is touched.
        if create:
            circle, parent, _ = _v164_circle_from_legacy(cid) if not meta else (int(meta.get("circle") or 1), int(meta.get("parent_first_chat_id") or 0), tid)
            fixed = _v164_ensure_isolated_chat(cid, circle, parent, actor_user_id=int(actor_user_id or 0), source=str((meta or {}).get("source") or "lazy_repair"))
            return str(fixed.get("tenant_id") or _v164_mapping().get(str(cid)) or tid)
        return tid
    if not create:
        return ""
    fixed = _v164_ensure_isolated_chat(cid, 1, 0, actor_user_id=int(actor_user_id or 0), source="direct")
    return str(fixed.get("tenant_id") or _v164_mapping().get(str(cid)) or "")


def tenant_note_chat_seen(msg) -> None:
    try:
        cid = int(msg.chat.id)
        uid = int(getattr(getattr(msg, "from_user", None), "id", 0) or 0)
    except Exception:
        return
    meta = _v164_meta_raw(cid)
    if not meta:
        # A plain message/start without a v164/legacy link means direct discovery => first circle.
        meta = _v164_ensure_isolated_chat(cid, 0 if cid == _v164_owner_id() else 1, actor_user_id=uid, source="direct_seen")
    else:
        meta = _v164_ensure_isolated_chat(cid, int(meta.get("circle") or 1), int(meta.get("parent_first_chat_id") or 0), actor_user_id=uid, source=str(meta.get("source") or "seen"))
    tid = str(meta.get("tenant_id") or tenant_id_for_chat(cid, create=True, actor_user_id=uid))
    row = tenant_get(tid) or {}
    if not int(row.get("owner_user_id") or 0) and uid and tenant_user_is_chat_admin(cid, uid):
        try:
            tenant_set_user_role(tid, uid, "tenant_owner", changed_by=uid, save=False)
        except Exception:
            pass
    try:
        store = get_chat_store(cid)
        settings = store.setdefault("settings", {})
        settings["tenant_id"] = tid
        settings["owner_scope_id"] = cid
        settings["circle_level"] = int(meta.get("circle") or 1)
        settings["parent_first_chat_id"] = int(meta.get("parent_first_chat_id") or 0)
    except Exception:
        pass


def _v164_circle_parent_root_for_context(chat_id: int) -> int:
    cid = int(chat_id)
    level = circle_level_for_chat(cid)
    if level == 1:
        return cid
    if level == 2:
        return circle_parent_for_chat(cid)
    return 0


def _v164_circle_children(parent_first_chat_id: int) -> list[int]:
    parent = int(parent_first_chat_id or 0)
    out = []
    for raw_cid, meta in list((_v164_root().get("chat_meta") or {}).items()):
        if not isinstance(meta, dict):
            continue
        try:
            cid = int(raw_cid)
        except Exception:
            continue
        if int(meta.get("circle") or 0) == 2 and int(meta.get("parent_first_chat_id") or 0) == parent:
            out.append(cid)
    return sorted(set(out), key=lambda x: get_chat_display_name(x).casefold())


def _v164_all_circle_ids(level: int) -> list[int]:
    _v164_migrate_legacy_hierarchy()
    out = []
    for cid in _v164_known_chat_ids():
        if cid == _v164_owner_id():
            continue
        try:
            info = _v164_circle_info(cid, create=False)
            if int(info.get("circle") or 0) == int(level):
                out.append(int(cid))
        except Exception:
            pass
    return sorted(set(out), key=lambda x: get_chat_display_name(x).casefold())


def _v164_scope_ids(level: int, context_chat_id: int | None = None) -> list[int]:
    _v164_migrate_legacy_hierarchy()
    level = 2 if int(level) == 2 else 1
    try:
        ctx = int(context_chat_id if context_chat_id is not None else current_state_chat_id() or 0)
    except Exception:
        ctx = 0
    if ctx == _v164_owner_id() and ctx:
        return _v164_all_circle_ids(level)
    root_first = _v164_circle_parent_root_for_context(ctx) if ctx else 0
    if not root_first:
        return []
    if level == 1:
        return [root_first]
    return _v164_circle_children(root_first)


def _v164_actor_manages_parent(user_id: int, parent_first_chat_id: int) -> bool:
    parent_tid = str(_v164_mapping().get(str(int(parent_first_chat_id))) or "")
    if not parent_tid:
        return False
    try:
        role = tenant_role_for_user(int(user_id), tenant_id=parent_tid)
        return role in {"platform_owner", "tenant_owner", "tenant_admin"}
    except Exception:
        return False


def tenant_can_manage(user_id: int | None, tenant_id: str | None = None, chat_id: int | None = None, owner_only: bool = False) -> bool:
    try:
        uid = int(user_id or 0)
    except Exception:
        uid = 0
    if tenant_is_platform_owner_user(uid):
        return True
    # Preserve direct tenant membership first.
    try:
        if callable(_V164_PREV_TENANT_CAN_MANAGE) and _V164_PREV_TENANT_CAN_MANAGE(uid, tenant_id, chat_id, owner_only):
            return True
    except Exception:
        pass
    target_chat = 0
    if chat_id:
        try: target_chat = int(chat_id)
        except Exception: target_chat = 0
    if not target_chat and tenant_id:
        try:
            row = tenant_get(str(tenant_id)) or {}
            target_chat = int(row.get("root_chat_id") or 0)
        except Exception:
            target_chat = 0
    if target_chat and circle_level_for_chat(target_chat) == 2:
        parent = circle_parent_for_chat(target_chat)
        return bool(parent and _v164_actor_manages_parent(uid, parent))
    return False


def tenant_same_space(chat_a: int, chat_b: int) -> bool:
    """Isolation is storage-level. Explicit forwarding is allowed inside a first-circle family.

    Platform-owner UI may intentionally connect isolated chats; add_forward_link performs that authorization.
    """
    try:
        a, b = int(chat_a), int(chat_b)
    except Exception:
        return False
    if a == b:
        return True
    ma, mb = _v164_circle_info(a, False), _v164_circle_info(b, False)
    la, lb = int(ma.get("circle") or 0), int(mb.get("circle") or 0)
    pa = a if la == 1 else int(ma.get("parent_first_chat_id") or 0)
    pb = b if lb == 1 else int(mb.get("parent_first_chat_id") or 0)
    if pa and pb and pa == pb:
        return True
    # Platform-owner-created cross-family forwarding links are explicit exceptions.
    pair_key = f"{min(a, b)}:{max(a, b)}"
    if pair_key in (_v164_root().get("global_forward_pairs") or {}):
        return True
    # Same isolated tenant remains valid.
    ta, tb = str(_v164_mapping().get(str(a)) or ""), str(_v164_mapping().get(str(b)) or "")
    return bool(ta and ta == tb)


def _v177_legacy_0143_add_forward_link(src_chat_id: int, dst_chat_id: int, mode: str):
    src, dst = int(src_chat_id), int(dst_chat_id)
    actor = 0
    try:
        actor = int(tenant_current_actor_user_id() or 0)
    except Exception:
        pass
    if not tenant_same_space(src, dst):
        if not tenant_is_platform_owner_user(actor):
            raise PermissionError("Можно связывать только свой 1-й круг и его 2-й круг")
        # Owner is explicitly authorizing a cross-family forwarding pair. Persist that exception so
        # the v148 background isolation cleanup does not remove it later.
        key = f"{min(src, dst)}:{max(src, dst)}"
        _v164_root().setdefault("global_forward_pairs", {})[key] = {
            "src": src, "dst": dst, "created_by": actor, "created_at": _v164_now(),
        }
        try: save_data(data, root_only=True)
        except Exception: pass
    # v148's wrapper would reject cross-isolated tenants, so call the pre-v148 implementation if available.
    base = globals().get("_V148_ORIG_ADD_FORWARD_LINK")
    if callable(base):
        return base(src, dst, mode)
    if callable(_V164_PREV_ADD_FORWARD_LINK):
        return _V164_PREV_ADD_FORWARD_LINK(src, dst, mode)
    raise RuntimeError("add_forward_link is unavailable")
try: _v177_legacy_0143_add_forward_link.__name__ = 'add_forward_link'
except Exception: pass
add_forward_link = _v177_legacy_0143_add_forward_link


def tenant_create_invite(tenant_id: str, kind: str, role: str, created_by: int, max_uses: int = 1, ttl_hours: int = 72) -> str:
    kind = "chat" if str(kind) == "chat" else "user"
    tenant = tenant_get(str(tenant_id)) or {}
    try:
        context_chat = int(current_state_chat_id() or tenant.get("root_chat_id") or 0)
    except Exception:
        context_chat = int(tenant.get("root_chat_id") or 0)
    tenant_root_chat = int(tenant.get("root_chat_id") or 0)
    if kind == "chat" and tenant_root_chat and circle_level_for_chat(tenant_root_chat) == 1:
        root_first = tenant_root_chat
    else:
        root_first = _v164_circle_parent_root_for_context(context_chat) if kind == "chat" else 0
    if kind == "chat" and (not root_first or circle_level_for_chat(root_first) != 1):
        raise PermissionError("Ссылку 2-го круга нужно создавать из чата 1-го круга")
    payload = _V164_PREV_TENANT_CREATE_INVITE(tenant_id, kind, role, created_by, max_uses=max_uses, ttl_hours=ttl_hours)
    if kind != "chat":
        return payload
    row = (_tenants_root().get("invite_tokens") or {}).get(_tenant_token_hash(payload))
    if isinstance(row, dict):
        row["circle_parent_chat_id"] = int(root_first)
        row["circle_parent_tenant_id"] = str(_v164_mapping().get(str(root_first)) or "")
        row["circle_schema"] = V164_CIRCLE_SCHEMA
        row["created_from_chat_id"] = int(context_chat or root_first)
        row["created_at"] = _v164_now()
        try: save_data(data, root_only=True)
        except Exception: pass
    return payload


def tenant_consume_invite(payload: str, user_id: int, chat_id: int, chat_type: str = "") -> tuple[bool, str, str]:
    key = _tenant_token_hash(str(payload or "").strip())
    token = (_tenants_root().get("invite_tokens") or {}).get(key)
    if not isinstance(token, dict) or str(token.get("kind") or "user") != "chat":
        return _V164_PREV_TENANT_CONSUME_INVITE(payload, user_id, chat_id, chat_type)
    if token.get("revoked") or float(token.get("expires_ts") or 0) < _v164_time.time() or int(token.get("uses") or 0) >= int(token.get("max_uses") or 1):
        return False, "Срок действия ссылки закончился.", ""
    cid, uid = int(chat_id), int(user_id or 0)
    if str(chat_type or "") == "private" or cid > 0:
        return False, "Эту ссылку нужно использовать при добавлении бота в группу/канал.", ""
    if not tenant_user_is_chat_admin(cid, uid):
        return False, "Привязать чат может только его администратор.", ""
    parent = int(token.get("circle_parent_chat_id") or 0)
    if not parent:
        legacy_tid = str(token.get("tenant_id") or "")
        legacy_parent = tenant_get(legacy_tid) or {}
        parent = int(legacy_parent.get("root_chat_id") or 0)
    if not parent or circle_level_for_chat(parent) != 1:
        return False, "Ссылка не привязана к чату 1-го круга. Создайте новую ссылку в меню пространства.", ""
    parent_tid = str(_v164_mapping().get(str(parent)) or tenant_id_for_chat(parent, create=True, actor_user_id=uid))
    if not tenant_can_manage(int(token.get("created_by") or uid), parent_tid, parent):
        # Do not trust stale/imported link rows that no longer belong to a manager.
        return False, "Эта ссылка больше не имеет права подключать чат.", ""

    meta = _v164_ensure_isolated_chat(cid, 2, parent_first_chat_id=parent, actor_user_id=uid, source="first_circle_link")
    child_tid = str(meta.get("tenant_id") or tenant_id_for_chat(cid, create=True, actor_user_id=uid))
    _v164_copy_parent_managers_to_child(child_tid, parent, consuming_admin=uid)
    token["uses"] = int(token.get("uses") or 0) + 1
    token["last_used_at"] = _v164_now()
    token["last_used_by"] = uid
    token["child_chat_id"] = cid
    token["child_tenant_id"] = child_tid
    try:
        save_data(data, full=True)
        schedule_delta_backup(parent, delay=0.5, reason="v164_second_circle_join")
    except Exception:
        pass
    try:
        bot_journal("v164_second_circle_join", cid, f"parent={parent}; tenant={child_tid}; by={uid}")
    except Exception:
        pass
    return True, f"✅ Чат подключён как 2-й круг к «{get_chat_display_name(parent)}».\nДанные и настройки этого чата изолированы.", child_tid


# ---------------------------------------------------------------------------
# Simple space menu
# ---------------------------------------------------------------------------
def _v164_circle_label(cid: int, include_parent: bool = False) -> str:
    title = get_chat_display_name(int(cid)) or f"Чат {int(cid)}"
    if include_parent and circle_level_for_chat(cid) == 2:
        parent = circle_parent_for_chat(cid)
        return f"{title} ← {get_chat_display_name(parent)}"
    return title


def tenant_dashboard_text(chat_id: int, user_id: int) -> str:
    _v164_migrate_legacy_hierarchy()
    cid, uid = int(chat_id), int(user_id or 0)
    level = circle_level_for_chat(cid)
    if cid == _v164_owner_id():
        first, second = _v164_all_circle_ids(1), _v164_all_circle_ids(2)
        return (
            "🏠 ПРОСТРАНСТВО ВЛАДЕЛЬЦА\n\n"
            "Здесь находится только ваш собственный контур.\n"
            "Чаты 1-го и 2-го круга имеют отдельные пространства и не смешиваются с ним.\n\n"
            f"1️⃣ Первый круг: {len(first)}\n"
            f"2️⃣ Второй круг: {len(second)}"
        )
    root_first = _v164_circle_parent_root_for_context(cid)
    if level == 1:
        children = _v164_circle_children(cid)
        return (
            "1️⃣ ПРОСТРАНСТВО ПЕРВОГО КРУГА\n\n"
            f"Чат: {get_chat_display_name(cid)}\n"
            f"2-й круг: {len(children)} чат(ов)\n\n"
            "Финансы, настройки, напоминания и Google этого пространства изолированы от владельца бота.\n"
            "По ссылке из этого меню можно подключать только свой 2-й круг."
        )
    parent = circle_parent_for_chat(cid)
    return (
        "2️⃣ ПРОСТРАНСТВО ВТОРОГО КРУГА\n\n"
        f"Чат: {get_chat_display_name(cid)}\n"
        f"Родитель 1-го круга: {get_chat_display_name(parent) if parent else 'не определён'}\n\n"
        "У этого чата собственное изолированное пространство. Он не становится частью пространства владельца бота."
    )


def tenant_dashboard_keyboard(chat_id: int, user_id: int):
    cid, uid = int(chat_id), int(user_id or 0)
    kb = types.InlineKeyboardMarkup(row_width=1)
    level = circle_level_for_chat(cid)
    if cid == _v164_owner_id():
        kb.row(IB(f"1️⃣ Первый круг · {len(_v164_all_circle_ids(1))}", callback_data="v164:space_circle:1"))
        kb.row(IB(f"2️⃣ Второй круг · {len(_v164_all_circle_ids(2))}", callback_data="v164:space_circle:2"))
    elif level == 1:
        if tenant_can_manage(uid, chat_id=cid):
            kb.row(IB(f"2️⃣ Второй круг · {len(_v164_circle_children(cid))}", callback_data="v164:space_circle:2"))
            kb.row(IB("🔗 Подключить чат 2-го круга", callback_data=f"sp:chatlink:{tenant_id_for_chat(cid, create=True, actor_user_id=uid)}"))
            kb.row(IB("👥 Пользователи", callback_data=f"sp:users:{tenant_id_for_chat(cid, create=True, actor_user_id=uid)}"))
    else:
        tid = tenant_id_for_chat(cid, create=True, actor_user_id=uid)
        if tenant_can_manage(uid, tid, cid):
            kb.row(IB("👥 Пользователи", callback_data=f"sp:users:{tid}"))
    kb.row(IB("❌ Закрыть", callback_data="info_close"))
    return kb


def _v164_space_list_text(context_chat_id: int, level: int) -> str:
    ids = _v164_scope_ids(level, context_chat_id)
    title = "1️⃣ ПЕРВЫЙ КРУГ" if int(level) == 1 else "2️⃣ ВТОРОЙ КРУГ"
    lines = [title, ""]
    if not ids:
        lines.append("Нет подключённых чатов.")
    else:
        for cid in ids:
            if int(level) == 2:
                lines.append(f"• {_v164_circle_label(cid, include_parent=True)}")
            else:
                lines.append(f"• {_v164_circle_label(cid)}")
    lines += ["", "Каждый чат хранит собственные настройки и данные."]
    return "\n".join(lines)[:3900]


def _v164_space_list_keyboard(context_chat_id: int, user_id: int, level: int):
    kb = types.InlineKeyboardMarkup(row_width=1)
    ids = _v164_scope_ids(level, context_chat_id)
    for cid in ids[:60]:
        tid = tenant_id_for_chat(cid, create=True, actor_user_id=user_id)
        kb.row(IB(_v164_circle_label(cid, include_parent=(int(level) == 2)), callback_data=f"sp:open:{tid}"))
    if int(level) == 2 and circle_level_for_chat(context_chat_id) == 1 and tenant_can_manage(user_id, chat_id=context_chat_id):
        tid = tenant_id_for_chat(context_chat_id, create=True, actor_user_id=user_id)
        kb.row(IB("🔗 Подключить чат 2-го круга", callback_data=f"sp:chatlink:{tid}"))
    kb.row(IB("🔙 Назад", callback_data="sp:dashboard:x"))
    return kb


def tenant_detail_text(tenant_id: str, viewer_user_id: int) -> str:
    row = tenant_get(tenant_id) or {}
    cid = int(row.get("root_chat_id") or 0)
    if not cid:
        return "❌ Пространство недоступно."
    level = circle_level_for_chat(cid)
    lines = [
        ("1️⃣ ПЕРВЫЙ КРУГ" if level == 1 else ("2️⃣ ВТОРОЙ КРУГ" if level == 2 else "🏠 ПРОСТРАНСТВО ВЛАДЕЛЬЦА")),
        "",
        f"Чат: {get_chat_display_name(cid)}",
        f"ID: {cid}",
        f"Владелец пространства: {security_user_display(int(row.get('owner_user_id') or 0)) if int(row.get('owner_user_id') or 0) else 'не назначен'}",
        "Изоляция: ✅ отдельные данные/настройки",
    ]
    if level == 1:
        lines.append(f"2-й круг: {len(_v164_circle_children(cid))}")
    elif level == 2:
        parent = circle_parent_for_chat(cid)
        lines.append(f"Родитель 1-го круга: {get_chat_display_name(parent) if parent else 'не определён'}")
    return "\n".join(lines)[:3900]


def tenant_chats_text(tenant_id: str) -> str:
    row = tenant_get(tenant_id) or {}
    cid = int(row.get("root_chat_id") or 0)
    if not cid:
        return "💬 ЧАТЫ\n\nНет чатов."
    # v164 guarantees one Telegram chat per tenant.
    return f"💬 ЧАТ ПРОСТРАНСТВА\n\n• {get_chat_display_name(cid)} · {cid}\n\nДругие круги сюда не смешиваются."


def tenant_visible_spaces(user_id: int) -> list[dict]:
    """Keep legacy APIs safe, but do not use this as the v164 owner dashboard.

    The platform owner can still administer all isolated tenants. Non-owner users see direct memberships.
    """
    if tenant_is_platform_owner_user(user_id):
        return tenant_all()
    return tenant_user_spaces(user_id)


def tenant_handle_callback(call, data_str: str) -> bool:
    raw = str(data_str or "")
    if not raw.startswith("sp:"):
        return False
    cid = int(call.message.chat.id)
    uid = int(getattr(call.from_user, "id", 0) or 0)
    parts = raw.split(":")
    action = parts[1] if len(parts) > 1 else ""
    tid = parts[2] if len(parts) > 2 and parts[2] not in {"x", ""} else tenant_id_for_chat(cid, create=True, actor_user_id=uid)
    if action in {"dashboard", "list"}:
        safe_edit(bot, call, tenant_dashboard_text(cid, uid), reply_markup=tenant_dashboard_keyboard(cid, uid))
        return True
    row = tenant_get(tid) or {}
    target_chat = int(row.get("root_chat_id") or 0)
    if not target_chat:
        bot.answer_callback_query(call.id, "Пространство недоступно.", show_alert=True)
        return True
    if not (tenant_is_platform_owner_user(uid) or tenant_can_manage(uid, tid, target_chat) or tenant_role_for_user(uid, tenant_id=tid) in {"operator", "viewer"}):
        bot.answer_callback_query(call.id, "Пространство недоступно.", show_alert=True)
        return True
    if action == "open":
        kb = types.InlineKeyboardMarkup(row_width=1)
        level = circle_level_for_chat(target_chat)
        if tenant_can_manage(uid, tid, target_chat):
            if level == 1:
                kb.row(IB(f"2️⃣ Второй круг · {len(_v164_circle_children(target_chat))}", callback_data="v164:space_circle:2"))
                kb.row(IB("🔗 Подключить чат 2-го круга", callback_data=f"sp:chatlink:{tid}"))
            kb.row(IB("👥 Пользователи", callback_data=f"sp:users:{tid}"))
        kb.row(IB("🔙 Назад", callback_data="sp:dashboard:x"))
        safe_edit(bot, call, tenant_detail_text(tid, uid), reply_markup=kb)
        return True
    if action == "chats":
        kb = types.InlineKeyboardMarkup(); kb.row(IB("🔙 Назад", callback_data=f"sp:open:{tid}"))
        safe_edit(bot, call, tenant_chats_text(tid), reply_markup=kb)
        return True
    if action == "users":
        kb = types.InlineKeyboardMarkup(); kb.row(IB("🔙 Назад", callback_data=f"sp:open:{tid}"))
        safe_edit(bot, call, tenant_users_text(tid), reply_markup=kb)
        return True
    if action == "chatlink":
        if circle_level_for_chat(target_chat) != 1 or not tenant_can_manage(uid, tid, target_chat):
            bot.answer_callback_query(call.id, "Ссылку 2-го круга создаёт только управляющий чата 1-го круга.", show_alert=True)
            return True
        try:
            payload = tenant_create_invite(tid, "chat", "tenant_admin", uid, max_uses=1, ttl_hours=72)
        except Exception as exc:
            bot.answer_callback_query(call.id, str(exc)[:180], show_alert=True)
            return True
        text = (
            "🔗 ПОДКЛЮЧЕНИЕ 2-ГО КРУГА\n\n"
            + tenant_invite_link(payload)
            + f"\n\nКод: {payload}"
            + "\n\nДобавленный по этой ссылке чат получит собственное изолированное пространство и будет привязан к этому 1-му кругу."
        )
        kb = types.InlineKeyboardMarkup(); kb.row(IB("🔙 Назад", callback_data=f"sp:open:{tid}"))
        safe_edit(bot, call, text, reply_markup=kb)
        return True
    if action == "userlink":
        if not tenant_can_manage(uid, tid, target_chat):
            bot.answer_callback_query(call.id, "Недостаточно прав.", show_alert=True); return True
        role = parts[3] if len(parts) > 3 else "operator"
        payload = tenant_create_invite(tid, "user", role, uid, max_uses=20, ttl_hours=72)
        text = f"👤 ССЫЛКА ДЛЯ ПОЛЬЗОВАТЕЛЯ\n\n{tenant_invite_link(payload)}\n\nРоль: {TENANT_ROLE_LABELS.get(role, role)}."
        kb = types.InlineKeyboardMarkup(); kb.row(IB("🔙 Назад", callback_data=f"sp:open:{tid}"))
        safe_edit(bot, call, text, reply_markup=kb)
        return True
    return True


# ---------------------------------------------------------------------------
# Forwarding + finance menus: circle 1 by default, explicit circle 2 button.
# Per-message view state keeps parallel Telegram windows independent.
# ---------------------------------------------------------------------------
def _v164_update_context() -> dict:
    try:
        value = getattr(_TELEGRAM_UPDATE_CONTEXT, "value", None)
        return value if isinstance(value, dict) else {}
    except Exception:
        return {}


def _v164_window_key(kind: str) -> tuple[int, int, str]:
    ctx = _v164_update_context()
    try: cid = int(ctx.get("chat_id") or current_state_chat_id() or 0)
    except Exception: cid = 0
    try: mid = int(ctx.get("message_id") or 0)
    except Exception: mid = 0
    return cid, mid, str(kind)


def _v164_set_window_circle(kind: str, level: int) -> None:
    key = _v164_window_key(kind)
    with _V164_WINDOW_VIEW_LOCK:
        _V164_WINDOW_VIEW[key] = {"circle": 2 if int(level) == 2 else 1, "at": _v164_time.time()}
        # Bounded memory: UI view hints are ephemeral.
        if len(_V164_WINDOW_VIEW) > 600:
            cutoff = _v164_time.time() - 86400
            for k, row in list(_V164_WINDOW_VIEW.items()):
                if float((row or {}).get("at") or 0) < cutoff:
                    _V164_WINDOW_VIEW.pop(k, None)


def _v164_current_window_circle(kind: str, default: int = 1) -> int:
    key = _v164_window_key(kind)
    ctx = _v164_update_context()
    raw = str(ctx.get("callback_data") or "")
    # A fresh entry from the main window always starts at circle 1.
    if str(kind) == "forward" and raw.startswith("d:") and raw.endswith(":forward_menu"):
        _v164_set_window_circle(kind, 1)
        return 1
    if str(kind) == "finmode" and raw.startswith("d:") and raw.endswith(":forward_finmode_menu"):
        # v164 custom config-back never uses this callback, so this is a fresh entry/legacy back.
        _v164_set_window_circle(kind, 1)
        return 1
    with _V164_WINDOW_VIEW_LOCK:
        row = _V164_WINDOW_VIEW.get(key) or {}
    return 2 if int(row.get("circle") or default) == 2 else 1


def _v164_circle_switch_button(kind: str, level: int, selected_a: int = 0):
    other = 1 if int(level) == 2 else 2
    label = "1️⃣ 1-й круг" if other == 1 else "2️⃣ 2-й круг"
    suffix = f":{int(selected_a)}" if selected_a else ""
    return IB(label, callback_data=f"v164:circle:{kind}:{other}{suffix}")


def _v164_scoped_picker_ids(kind: str, level: int | None = None) -> list[int]:
    if level is None:
        level = _v164_current_window_circle(kind, 1)
    return _v164_scope_ids(int(level), current_state_chat_id())


def _v177_legacy_0182_collect_forward_picker_items(include_owner: bool = True, include_removed: bool = False):
    level = _v164_current_window_circle("forward", 1)
    items = []
    for cid in _v164_scoped_picker_ids("forward", level):
        try:
            if (not include_removed) and is_chat_bot_removed(int(cid)):
                continue
        except Exception:
            pass
        items.append((int(cid), get_chat_display_name(int(cid)) or f"Чат {cid}"))
    # OWNER_ID is intentionally not mixed into first/second-circle pickers anymore.
    return items, None
try: _v177_legacy_0182_collect_forward_picker_items.__name__ = '_collect_forward_picker_items'
except Exception: pass
_collect_forward_picker_items = _v177_legacy_0182_collect_forward_picker_items


def _v177_legacy_0191_collect_forward_pairs_for_menu() -> list[tuple[int, int]]:
    rows = _V164_PREV_COLLECT_FORWARD_PAIRS() if callable(_V164_PREV_COLLECT_FORWARD_PAIRS) else []
    allowed = set(_v164_scoped_picker_ids("forward"))
    # Keep pairs whose source belongs to the displayed circle. Target may be the paired parent/child.
    out = []
    for pair in rows or []:
        try:
            a, b = int(pair[0]), int(pair[1])
        except Exception:
            continue
        if a in allowed:
            out.append((a, b))
    return out
try: _v177_legacy_0191_collect_forward_pairs_for_menu.__name__ = 'collect_forward_pairs_for_menu'
except Exception: pass
collect_forward_pairs_for_menu = _v177_legacy_0191_collect_forward_pairs_for_menu


def _v164_insert_before_nav(kb, button) -> None:
    try:
        rows = kb.keyboard
        # Put the circle switch before description/probe/back controls.
        idx = max(0, len(rows) - 3)
        rows.insert(idx, [button])
    except Exception:
        try: kb.row(button)
        except Exception: pass


def build_forward_new_menu(day_key: str | None = None, A: int | None = None, B: int | None = None):
    level = _v164_current_window_circle("forward", circle_level_for_chat(A) if A else 1)
    kb = _V164_PREV_BUILD_FORWARD_NEW_MENU(day_key, A, B)
    if not B:
        _v164_insert_before_nav(kb, _v164_circle_switch_button("forward", level, int(A or 0)))
    return kb


def build_forward_source_menu(day_key: str | None = None):
    # Previous source builder dynamically calls the current build_forward_new_menu in new-style mode.
    if forward_menu_new_style_enabled():
        return build_forward_new_menu(day_key)
    level = _v164_current_window_circle("forward", 1)
    kb = _V164_PREV_BUILD_FORWARD_SOURCE_MENU(day_key)
    _v164_insert_before_nav(kb, _v164_circle_switch_button("forward", level))
    return kb


def build_forward_target_menu(src_id: int):
    src = int(src_id)
    if circle_level_for_chat(src) in {1, 2}:
        # If no explicit circle switch happened, target list follows source circle.
        key = _v164_window_key("forward")
        with _V164_WINDOW_VIEW_LOCK:
            if key not in _V164_WINDOW_VIEW:
                _V164_WINDOW_VIEW[key] = {"circle": circle_level_for_chat(src), "at": _v164_time.time()}
    level = _v164_current_window_circle("forward", circle_level_for_chat(src))
    kb = _V164_PREV_BUILD_FORWARD_TARGET_MENU(src)
    _v164_insert_before_nav(kb, _v164_circle_switch_button("forward", level, src))
    return kb


def build_forward_menu_text_for_current_mode(title: str | None = None, A: int | None = None, B: int | None = None) -> str:
    level = _v164_current_window_circle("forward", circle_level_for_chat(A) if A else 1)
    prefix = "1️⃣ 1-й круг" if level == 1 else "2️⃣ 2-й круг"
    if forward_menu_new_style_enabled():
        body = build_forward_new_text(A, B)
    else:
        body = build_forward_status_text(title or "Пересылка:\nВыберите чат A:")
    return f"{prefix}\n{body}"


def build_forward_menu_keyboard_for_current_mode(day_key: str | None = None, A: int | None = None, B: int | None = None):
    if forward_menu_new_style_enabled():
        return build_forward_new_menu(day_key, A, B)
    if A and B:
        return build_forward_mode_menu(A, B)
    if A:
        return build_forward_target_menu(A)
    return build_forward_source_menu(day_key)


def _v177_legacy_0197_build_finance_toggle_chat_menu(day_key: str):
    level = _v164_current_window_circle("finmode", 1)
    kb = types.InlineKeyboardMarkup(row_width=2)
    buttons = []
    for cid in _v164_scope_ids(level, current_state_chat_id()):
        try:
            if is_chat_bot_removed(cid):
                continue
        except Exception:
            pass
        icon = finance_mode_compact_icon(cid)
        buttons.append(IB(f"{icon} {chat_button_title(cid, get_chat_display_name(cid))}", callback_data=f"d:{day_key}:fw_finmode_pick_{cid}"))
    add_buttons_in_rows(kb, buttons, 2)
    if not buttons:
        kb.row(IB("Нет чатов этого круга", callback_data="none"))
    kb.row(_v164_circle_switch_button("finmode", level))
    kb.row(IB("ℹ️ Описание чатов", callback_data="chat_desc_menu:finmode"))
    kb.row(IB("🔙 Назад", callback_data=f"d:{day_key}:back_main"))
    return kb
try: _v177_legacy_0197_build_finance_toggle_chat_menu.__name__ = 'build_finance_toggle_chat_menu'
except Exception: pass
build_finance_toggle_chat_menu = _v177_legacy_0197_build_finance_toggle_chat_menu


def build_quick_balance_mode_menu(day_key: str, target_chat_id: int):
    kb = _V164_PREV_BUILD_QUICK_BALANCE_MODE_MENU(day_key, target_chat_id)
    level = _v164_current_window_circle("finmode", circle_level_for_chat(int(target_chat_id)))
    try:
        if kb.keyboard:
            # Replace only the last "Назад к чатам" row. All financial action callbacks remain unchanged.
            last = kb.keyboard[-1]
            if last and "Назад" in str(getattr(last[0], "text", "")):
                kb.keyboard[-1] = [IB("🔙 Назад к чатам", callback_data=f"v164:finback:{level}:{day_key}")]
    except Exception:
        pass
    return kb


def build_finance_mode_config_menu(day_key: str, target_chat_id: int):
    return build_quick_balance_mode_menu(day_key, target_chat_id)


def _v177_legacy_0184_build_chat_description_menu(viewer_chat_id: int, origin: str, day_key: str):
    # For forwarding/finmode descriptions, use exactly the circle currently displayed.
    if str(origin) not in {"forward", "finmode"}:
        return _V164_PREV_BUILD_CHAT_DESCRIPTION_MENU(viewer_chat_id, origin, day_key)
    kind = "finmode" if str(origin) == "finmode" else "forward"
    level = _v164_current_window_circle(kind, 1)
    kb = types.InlineKeyboardMarkup(row_width=2)
    buttons = []
    for cid in _v164_scope_ids(level, viewer_chat_id):
        try:
            if is_chat_bot_removed(int(cid)):
                continue
        except Exception:
            pass
        buttons.append(IB(chat_button_title(int(cid)), callback_data=f"chat_desc_open:{origin}:{int(cid)}"))
    add_buttons_in_rows(kb, buttons, 2)
    kb.row(IB("🔙 Назад", callback_data=_chat_description_origin_back(origin, day_key)))
    kb.row(IB("⬅️ Назад осн. окно", callback_data=f"d:{day_key}:back_main"))
    return kb
try: _v177_legacy_0184_build_chat_description_menu.__name__ = 'build_chat_description_menu'
except Exception: pass
build_chat_description_menu = _v177_legacy_0184_build_chat_description_menu


def _v164_circle_callback_filter(call) -> bool:
    try:
        raw = str(getattr(call, "data", "") or "")
        resolver = globals().get("resolve_short_callback")
        if callable(resolver):
            raw = str(resolver(raw) or raw)
        return raw.startswith("v164:")
    except Exception:
        return False


def _v164_current_day_for_ui(chat_id: int) -> str:
    try:
        return str(get_chat_store(int(chat_id)).get("current_view_day") or today_key())
    except Exception:
        return today_key()


def _v164_circle_callback(call):
    raw = str(getattr(call, "data", "") or "")
    try:
        resolver = globals().get("resolve_short_callback")
        if callable(resolver): raw = str(resolver(raw) or raw)
    except Exception:
        pass
    cid = int(call.message.chat.id)
    uid = int(getattr(call.from_user, "id", 0) or 0)
    parts = raw.split(":")
    try:
        if raw.startswith("v164:circle:") and len(parts) >= 4:
            kind = str(parts[2])
            level = 2 if int(parts[3]) == 2 else 1
            selected_a = int(parts[4]) if len(parts) > 4 and str(parts[4]).lstrip("-").isdigit() else 0
            if kind not in {"forward", "finmode"}:
                return
            # Owner can browse globally from owner chat. A first-circle manager browses only its family.
            if cid != _v164_owner_id() and not tenant_can_manage(uid, chat_id=_v164_circle_parent_root_for_context(cid) or cid):
                bot.answer_callback_query(call.id, "Недостаточно прав.", show_alert=True); return
            _v164_set_window_circle(kind, level)
            day = _v164_current_day_for_ui(cid)
            if kind == "forward":
                if selected_a:
                    kb = build_forward_new_menu(day, selected_a) if forward_menu_new_style_enabled() else build_forward_target_menu(selected_a)
                    text = build_forward_menu_text_for_current_mode(f"Источник: {get_chat_display_name(selected_a)}\nВыберите чат B:", A=selected_a)
                else:
                    kb = build_forward_menu_keyboard_for_current_mode(day)
                    text = build_forward_menu_text_for_current_mode("Пересылка:\nВыберите чат A:")
                safe_edit(bot, call, text, reply_markup=kb)
            else:
                safe_edit(bot, call, "💰 Фин режим / В24\n" + ("1️⃣ Первый круг" if level == 1 else "2️⃣ Второй круг") + "\nВыберите чат.", reply_markup=build_finance_toggle_chat_menu(day))
            return
        if raw.startswith("v164:finback:") and len(parts) >= 4:
            level = 2 if int(parts[2]) == 2 else 1
            day = str(parts[3] or _v164_current_day_for_ui(cid))
            _v164_set_window_circle("finmode", level)
            safe_edit(bot, call, "💰 Фин режим / В24\n" + ("1️⃣ Первый круг" if level == 1 else "2️⃣ Второй круг") + "\nВыберите чат.", reply_markup=build_finance_toggle_chat_menu(day))
            return
        if raw.startswith("v164:space_circle:") and len(parts) >= 3:
            level = 2 if int(parts[2]) == 2 else 1
            # Owner-private sees global circle inventory. Other chats see only their family.
            if cid != _v164_owner_id() and not (tenant_can_manage(uid, chat_id=cid) or circle_level_for_chat(cid) == 2):
                bot.answer_callback_query(call.id, "Недостаточно прав.", show_alert=True); return
            safe_edit(bot, call, _v164_space_list_text(cid, level), reply_markup=_v164_space_list_keyboard(cid, uid, level))
            return
    except Exception as exc:
        try: log_error(f"v164 circle callback {raw}: {exc}")
        except Exception: pass
        try: bot.answer_callback_query(call.id, "Не удалось открыть круг.", show_alert=True)
        except Exception: pass


def _v164_install_callback_handler() -> int:
    return 0  # v179: registration/wrapper retired; final router owns callbacks


# A direct command gets a clear answer instead of an uncaught PermissionError from the old handler.
def _v164_space_chat_link_handler(msg):
    try:
        uid, cid = int(getattr(getattr(msg, "from_user", None), "id", 0) or 0), int(msg.chat.id)
        tenant_note_chat_seen(msg)
        if circle_level_for_chat(cid) != 1:
            bot.send_message(cid, "❌ Подключать 2-й круг можно только из чата 1-го круга.")
            return
        tid = tenant_id_for_chat(cid, create=True, actor_user_id=uid)
        if not tenant_can_manage(uid, tid, cid):
            bot.send_message(cid, "❌ Недостаточно прав.")
            return
        payload = tenant_create_invite(tid, "chat", "tenant_admin", uid, max_uses=1, ttl_hours=72)
        bot.send_message(cid, "🔗 Ссылка для подключения чата 2-го круга (72 часа):\n" + tenant_invite_link(payload) + f"\n\nКод: {payload}")
        try: schedule_command_delete(msg)
        except Exception: pass
    except Exception as exc:
        try: bot.send_message(msg.chat.id, f"❌ Не удалось создать ссылку: {str(exc)[:240]}")
        except Exception: pass


def _v164_install_space_command_handler() -> int:
    try:
        bot.message_handler(commands=["space_chat_link", "tenant_chat_link"])(_v164_space_chat_link_handler)
        handlers = getattr(bot, "message_handlers", None)
        if isinstance(handlers, list) and handlers:
            row = handlers.pop()
            handlers.insert(0, row)
        return 1
    except Exception:
        return 0


# Register marker aliases for the circle switches without inventing a new logical window marker.
try:
    WINDOW_MARKER_CONSTANTS.update({
        "v164:circle:forward:*": "Ф53",
        "v164:circle:finmode:*": "Ф52",
        "v164:finback:*": "Ф52",
        "v164:space_circle:*": "Ф239",
    })
except Exception:
    pass


# Restore compatibility with v164 snapshots.


_V164_CALLBACK_HANDLER = 0  # v179 final callback router
_V164_SPACE_COMMAND_HANDLER = _v164_install_space_command_handler()

# Do not force a save during module import. A lazy migration after runtime restore is safer on Render.
try:
    bot_journal(
        "v164_circle_hierarchy_installed",
        _v164_owner_id(),
        "owner=isolated; direct=first_circle; invite=second_circle_isolated; owner_menus=circles; "
        f"callbacks={_V164_CALLBACK_HANDLER}; command={_V164_SPACE_COMMAND_HANDLER}",
    )
except Exception:
    pass

# ---- integrated from 115_v165_owner_first_circle_compat.py ----
"""v165: restore owner row in Forwarding and Finance-mode first-circle pickers.

The owner is visible in the same menus as in v163 and earlier, but remains circle 0 / platform tenant.
Circle 1 continues to mean ordinary direct-connected chats with their own isolated settings.
Circle 2 remains a separate picker and never receives the owner row.
"""

import gzip as _v165_gzip
import json as _v165_json
import os as _v165_os
import shutil as _v165_shutil
import sqlite3 as _v165_sqlite3
import tempfile as _v165_tempfile

VERSION = "bot_v165_owner_first_circle_compat"

_V165_PREV_RESTORE_VALIDATE = globals().get("_v153_validate_restore_gz")


def _v165_is_platform_owner_context() -> bool:
    try:
        return int(current_state_chat_id() or 0) == int(OWNER_ID or 0) and int(OWNER_ID or 0) != 0
    except Exception:
        return False


def _v165_owner_item(include_removed: bool = False):
    try:
        oid = int(OWNER_ID or 0)
    except Exception:
        oid = 0
    if not oid:
        return None
    if not include_removed:
        try:
            # The private owner chat is retained even if an old lifecycle flag was stale.
            if is_chat_bot_removed(oid) and not _v165_is_platform_owner_context():
                return None
        except Exception:
            pass
    return oid, (get_chat_display_name(oid) or f"Чат {oid}")


def _collect_forward_picker_items(include_owner: bool = True, include_removed: bool = False):
    """v165: v163-compatible owner row + v164 circle-scoped ordinary chats."""
    level = _v164_current_window_circle("forward", 1)
    items = []
    owner_item = None
    for cid in _v164_scope_ids(level, current_state_chat_id()):
        try:
            icid = int(cid)
        except Exception:
            continue
        try:
            if (not include_removed) and is_chat_bot_removed(icid):
                continue
        except Exception:
            pass
        items.append((icid, get_chat_display_name(icid) or f"Чат {icid}"))

    # Compatibility rule: only the platform owner, while looking at the 1st-circle page,
    # sees the owner chat as the dedicated owner row. This does not reclassify the owner as circle 1.
    if include_owner and int(level) == 1 and _v165_is_platform_owner_context():
        owner_item = _v165_owner_item(include_removed=include_removed)

    # Defensive de-duplication in case legacy state accidentally exposed OWNER_ID in circle ids.
    if owner_item:
        items = [(cid, title) for cid, title in items if int(cid) != int(owner_item[0])]
    items.sort(key=lambda row: (str(row[1]).casefold(), int(row[0])))
    return items, owner_item


def _v177_legacy_0192_collect_forward_pairs_for_menu() -> list[tuple[int, int]]:
    """Show historical owner pairs again on the 1st-circle page without mixing tenant storage."""
    try:
        rows = _V164_PREV_COLLECT_FORWARD_PAIRS() if callable(_V164_PREV_COLLECT_FORWARD_PAIRS) else []
    except Exception:
        rows = []
    level = _v164_current_window_circle("forward", 1)
    allowed = set(int(x) for x in (_v164_scope_ids(level, current_state_chat_id()) or []))
    if int(level) == 1 and _v165_is_platform_owner_context():
        try:
            allowed.add(int(OWNER_ID))
        except Exception:
            pass
    out = []
    for pair in rows or []:
        try:
            a, b = int(pair[0]), int(pair[1])
        except Exception:
            continue
        if a in allowed:
            out.append((a, b))
    return out
try: _v177_legacy_0192_collect_forward_pairs_for_menu.__name__ = 'collect_forward_pairs_for_menu'
except Exception: pass
collect_forward_pairs_for_menu = _v177_legacy_0192_collect_forward_pairs_for_menu


def build_finance_toggle_chat_menu(day_key: str):
    """Finance-mode picker: owner + ordinary first circle, or second circle only."""
    level = _v164_current_window_circle("finmode", 1)
    kb = types.InlineKeyboardMarkup(row_width=2)
    buttons = []

    # Restore the owner's own row/settings exactly through the existing finance state helpers.
    if int(level) == 1 and _v165_is_platform_owner_context():
        owner_item = _v165_owner_item(include_removed=True)
        if owner_item:
            oid, title = owner_item
            icon = finance_mode_compact_icon(oid)
            kb.row(IB(
                f"{icon} {chat_button_title(oid, title)}",
                callback_data=f"d:{day_key}:fw_finmode_pick_{oid}",
            ))

    for cid in _v164_scope_ids(level, current_state_chat_id()):
        try:
            cid = int(cid)
        except Exception:
            continue
        if OWNER_ID and str(cid) == str(OWNER_ID):
            continue
        try:
            if is_chat_bot_removed(cid):
                continue
        except Exception:
            pass
        icon = finance_mode_compact_icon(cid)
        buttons.append(IB(
            f"{icon} {chat_button_title(cid, get_chat_display_name(cid))}",
            callback_data=f"d:{day_key}:fw_finmode_pick_{cid}",
        ))
    add_buttons_in_rows(kb, buttons, 2)

    if not buttons and not (int(level) == 1 and _v165_is_platform_owner_context()):
        kb.row(IB("Нет чатов этого круга", callback_data="none"))
    kb.row(_v164_circle_switch_button("finmode", level))
    kb.row(IB("ℹ️ Описание чатов", callback_data="chat_desc_menu:finmode"))
    kb.row(IB("🔙 Назад", callback_data=f"d:{day_key}:back_main"))
    return kb


def build_chat_description_menu(viewer_chat_id: int, origin: str, day_key: str):
    """Description picker mirrors the visible owner/first/second-circle selection."""
    if str(origin) not in {"forward", "finmode"}:
        return _V164_PREV_BUILD_CHAT_DESCRIPTION_MENU(viewer_chat_id, origin, day_key)
    kind = "finmode" if str(origin) == "finmode" else "forward"
    level = _v164_current_window_circle(kind, 1)
    kb = types.InlineKeyboardMarkup(row_width=2)
    buttons = []

    try:
        viewer_is_owner = int(viewer_chat_id or 0) == int(OWNER_ID or 0) and int(OWNER_ID or 0) != 0
    except Exception:
        viewer_is_owner = False
    if int(level) == 1 and viewer_is_owner:
        owner_item = _v165_owner_item(include_removed=True)
        if owner_item:
            kb.row(IB(chat_button_title(owner_item[0], owner_item[1]), callback_data=f"chat_desc_open:{origin}:{owner_item[0]}"))

    for cid in _v164_scope_ids(level, viewer_chat_id):
        try:
            cid = int(cid)
        except Exception:
            continue
        if OWNER_ID and str(cid) == str(OWNER_ID):
            continue
        try:
            if is_chat_bot_removed(cid):
                continue
        except Exception:
            pass
        buttons.append(IB(chat_button_title(cid), callback_data=f"chat_desc_open:{origin}:{cid}"))
    add_buttons_in_rows(kb, buttons, 2)
    kb.row(IB("🔙 Назад", callback_data=_chat_description_origin_back(origin, day_key)))
    kb.row(IB("⬅️ Назад осн. окно", callback_data=f"d:{day_key}:back_main"))
    return kb


# v165 backup/restore compatibility.


try:
    bot_journal(
        "v165_owner_first_circle_compat_installed",
        int(OWNER_ID or 0),
        "owner_row=restored_in_forward_and_finmode_first_circle; owner_settings=preserved; circle1=ordinary_isolated_chats; circle2=separate",
    )
except Exception:
    pass

# ---- integrated from 116_v166_fast_parallel_forward_pairs.py ----
"""v166: restore forwarding pairs, fast callbacks, parallel per-window UI and fast finance refresh.

Safety rule: actual finance mutations remain chat-serialized. Independent window UI, forwarding-pair
configuration and post-commit finance-window refreshes are separated into dedicated keyed lanes.
"""

import contextlib as _v166_contextlib
import gzip as _v166_gzip
import json as _v166_json
import os as _v166_os
import shutil as _v166_shutil
import sqlite3 as _v166_sqlite3
import tempfile as _v166_tempfile
import threading as _v166_threading
import time as _v166_time

VERSION = "bot_v166_fast_parallel_forward_pairs"

# ---------------------------------------------------------------------------
# Pools / execution lanes.
# ---------------------------------------------------------------------------
V166_WINDOW_UI_TASK_POOL = UI_TASK_POOL
V166_FORWARD_CONFIG_TASK_POOL = GENERAL_TASK_POOL
V166_FINANCE_UI_TASK_POOL = UI_TASK_POOL
V166_CONFIG_IO_TASK_POOL = GENERAL_TASK_POOL
V166_CONFIG_IO_SCHEDULER = DELAYED_SCHEDULER
V166_FINANCE_DEBOUNCE_TASK_POOL = GENERAL_TASK_POOL
V166_FINANCE_DEBOUNCE_SCHEDULER = DELAYED_SCHEDULER

_V166_PAIR_EXEC_GUARD = _v166_threading.RLock()
_V166_PAIR_EXEC_LOCKS = {}
_V166_FORWARD_STATE_LOCK = _v166_threading.RLock()
_V166_FORWARD_DIRTY_LOCK = _v166_threading.RLock()
_V166_FORWARD_DIRTY_CHATS = set()

_V166_PREV_ACK = globals().get("schedule_callback_receipt_ack")
_V166_PREV_REFRESH_BALANCE = globals().get("refresh_balance_panel_now")
_V166_PREV_REFRESH_TOTAL = globals().get("refresh_total_message_if_any")
_V166_PREV_RESTORE_VALIDATE = globals().get("_v153_validate_restore_gz")


def _v166_callback_raw_parts(payload: dict):
    try:
        cq = (payload or {}).get("callback_query") or {}
        msg = cq.get("message") or {}
        chat = msg.get("chat") or {}
        return str(cq.get("data") or ""), int(chat.get("id") or 0), int(msg.get("message_id") or 0)
    except Exception:
        return "", 0, 0


def _v166_forward_pair_from_callback(raw: str):
    raw = str(raw or "")
    prefixes = (
        "fw_new_mode:", "fw_new_fin:", "fw_new_clear:",
        "fw_mode:", "fw_finpair:", "fw_clear:",
    )
    if not raw.startswith(prefixes):
        return None
    nums = []
    for part in raw.split(":")[1:]:
        try:
            nums.append(int(part))
        except Exception:
            continue
        if len(nums) >= 2:
            break
    if len(nums) < 2:
        return None
    a, b = nums[0], nums[1]
    return (a, b) if a <= b else (b, a)


def _v166_is_finance_business_callback(raw: str) -> bool:
    """Callbacks that can create/delete/edit money stay serialized per chat."""
    low = str(raw or "").casefold()
    if low.startswith(("fw_new_fin:", "fw_new_mode:", "fw_new_clear:", "fw_mode:", "fw_finpair:", "fw_clear:")):
        return False  # dedicated pair lane
    hard_prefixes = (
        "fv:", "fv_", "edit_", "del_", "delete_", "expense_", "income_",
        "rec_", "record_", "usd_edit", "usd_del", "cat_move", "cat_delete",
    )
    if low.startswith(hard_prefixes):
        return True
    dangerous = ("delete_selected", "apply", "save", "confirm", "finance_off", "fin_mode_", "qb_mode_", "qb_hidden_")
    if any(token in low for token in dangerous):
        return True
    if low.startswith("d:"):
        try:
            cmd = low.split(":", 2)[2]
        except Exception:
            cmd = low
        # Menu/view/display toggles are safe per concrete Telegram window.
        safe_tokens = (
            "info", "back_main", "forward_menu", "forward_finmode_menu", "calendar",
            "articles_toggle", "financial_values_toggle", "usd_tx_toggle", "usd_display_toggle",
        )
        if any(token in cmd for token in safe_tokens):
            return False
        if any(token in cmd for token in ("delete", "edit", "save", "apply", "fin_mode_", "qb_mode_", "qb_hidden_")):
            return True
    return False


def _v166_is_safe_window_callback(raw: str) -> bool:
    low = str(raw or "").casefold()
    if not low:
        return False
    if _v166_is_finance_business_callback(raw):
        return False
    # Secret content mutations remain on the old chat lane.
    if low.startswith(("secret", "sec:", "o9:")) and not any(x in low for x in ("back", "close", "menu", "page", "list", "view")):
        return False
    # Forward pair mutations have their own pair lane.
    if _v166_forward_pair_from_callback(raw) is not None:
        return False
    # Known harmless UI preference toggles.
    if low in {
        "forward_menu_style_toggle", "buttons_current_toggle", "icon_buttons_toggle",
        "reminder_ui_mode_toggle", "internal_timers", "process_center", "problem_tasks",
        "journal_open", "journal_back", "keepalive_status", "info_queues", "info_delta_status",
    }:
        return True
    if low.startswith((
        "fw_new_src:", "fw_new_tgt:", "fw_new_pair:", "fw_src:", "fw_tgt:",
        "fw_back", "fw_new_back", "fw_probe", "fw_removed", "chat_desc_", "v164:circle:",
        "rem:list", "rem:open", "rem:completed", "itmr_", "journal_", "version_",
    )):
        return True
    if low == "nav_prev" or "back" in low or low.endswith("_close") or low.startswith("close_"):
        return True
    if low.startswith("d:") and not _v166_is_finance_business_callback(raw):
        return True
    # Generic read/navigation words only; unknown mutations keep old serialization.
    return any(token in low for token in ("menu", "page", "list", "view", "status", "refresh", "open"))


def v163_webhook_select_lane(payload: dict, update_type: str, update_key):
    """v166: /start separate; forwarding pair by pair; safe UI by concrete message; finance serial."""
    if str(update_type) == "message" and _v163_start_payload(payload):
        chat_id = _extract_update_chat_id(payload)
        return START_UI_TASK_POOL, f"start:{chat_id if chat_id is not None else update_key}"
    if str(update_type) == "callback_query":
        raw, chat_id, message_id = _v166_callback_raw_parts(payload)
        pair = _v166_forward_pair_from_callback(raw)
        if pair is not None:
            return V166_FORWARD_CONFIG_TASK_POOL, f"pair:{pair[0]}:{pair[1]}"
        if chat_id and message_id and _v166_is_safe_window_callback(raw):
            return V166_WINDOW_UI_TASK_POOL, f"window:{chat_id}:{message_id}"
        return UI_TASK_POOL, f"ui:{chat_id if chat_id else update_key}"
    return WEBHOOK_TASK_POOL, update_key


def _v166_pair_lock(pair):
    with _V166_PAIR_EXEC_GUARD:
        lock = _V166_PAIR_EXEC_LOCKS.get(pair)
        if lock is None:
            lock = _v166_threading.RLock()
            _V166_PAIR_EXEC_LOCKS[pair] = lock
        return lock


def _execute_telegram_payload(payload: dict, update_id=None, update_chat_id=None, update_type: str = "other"):
    """Match execution locking to the v166 queue lane, so independent windows truly run in parallel."""
    update = telebot.types.Update.de_json(payload)
    if update_chat_id is None:
        update_chat_id = _extract_update_chat_id(payload) if isinstance(payload, dict) else None
    previous_ctx = getattr(_TELEGRAM_UPDATE_CONTEXT, "value", None)
    critical_callback_target = _durable_callback_target_chat(payload) if isinstance(payload, dict) else None
    callback_data = ""
    source_message_id = None
    source_user_id = None
    try:
        if isinstance(payload, dict):
            callback = payload.get("callback_query") or {}
            if isinstance(callback, dict):
                callback_data = str(callback.get("data") or "")
                source_user_id = ((callback.get("from") or {}).get("id") if isinstance(callback.get("from"), dict) else None)
                callback_message = callback.get("message") or {}
                if isinstance(callback_message, dict):
                    source_message_id = callback_message.get("message_id")
            if source_message_id is None:
                message_payload = payload.get("message") or payload.get("edited_message") or payload.get("channel_post") or payload.get("edited_channel_post") or {}
                if isinstance(message_payload, dict):
                    source_message_id = message_payload.get("message_id")
                    source_user_id = source_user_id or ((message_payload.get("from") or {}).get("id") if isinstance(message_payload.get("from"), dict) else None)
    except Exception:
        callback_data = ""

    _TELEGRAM_UPDATE_CONTEXT.value = {
        "update_id": update_id,
        "chat_id": update_chat_id,
        "update_type": str(update_type or "other"),
        "callback_data": callback_data,
        "message_id": source_message_id,
        "user_id": source_user_id,
        "critical_callback": critical_callback_target is not None,
        "critical_callback_target": critical_callback_target,
        "deferred_quick_chats": set(),
    }
    execution_ctx = {}
    try:
        with state_chat_context(update_chat_id):
            pair = _v166_forward_pair_from_callback(callback_data) if str(update_type) == "callback_query" else None
            if update_chat_id is None:
                lock_ctx = _v166_contextlib.nullcontext()
            elif str(update_type) == "message" and _v163_start_payload(payload):
                lock_ctx = _v163_lock_for(_V163_START_EXEC_LOCKS, _V163_START_EXEC_LOCK_GUARD, int(update_chat_id))
            elif pair is not None:
                lock_ctx = _v166_pair_lock(pair)
            elif str(update_type) == "callback_query" and source_message_id and _v166_is_safe_window_callback(callback_data):
                lock_ctx = _v163_lock_for(
                    _V163_WINDOW_EXEC_LOCKS,
                    _V163_WINDOW_EXEC_LOCK_GUARD,
                    (int(update_chat_id), int(source_message_id)),
                )
            else:
                lock_ctx = chat_lock_for(int(update_chat_id))
            with lock_ctx:
                bot.process_new_updates([update])
        execution_ctx = _durable_execution_context_snapshot()
        try:
            fn = globals().get("_v150_store_receipt")
            if callable(fn):
                fn(payload)
        except Exception as exc:
            try: log_error(f"v166 command receipt: {exc}")
            except Exception: pass
    finally:
        if not execution_ctx:
            execution_ctx = _durable_execution_context_snapshot()
        if previous_ctx is None:
            try: delattr(_TELEGRAM_UPDATE_CONTEXT, "value")
            except Exception: pass
        else:
            _TELEGRAM_UPDATE_CONTEXT.value = previous_ctx
    return execution_ctx


# ---------------------------------------------------------------------------
# Callback ACK: 50 ms receipt fallback instead of 150 ms.
# ---------------------------------------------------------------------------
def schedule_callback_receipt_ack(callback_id: str, chat_id=None, delay: float | None = None):
    if callable(_V166_PREV_ACK):
        return _V166_PREV_ACK(callback_id, chat_id, 0.05)


# ---------------------------------------------------------------------------
# Forwarding pairs: build from the actual global rules, not the old v148 same-tenant filter.
# ---------------------------------------------------------------------------
def _v166_pair_key(a: int, b: int):
    a, b = int(a), int(b)
    return (a, b) if a <= b else (b, a)


def _v166_raw_forward_pairs():
    # Snapshot under the common data lock: forwarding workers may read the same maps concurrently.
    with data_lock:
        fr = {str(k): dict(v or {}) for k, v in (data.get("forward_rules", {}) or {}).items()}
        ff = {str(k): dict(v or {}) for k, v in (data.get("forward_finance", {}) or {}).items()}
        order = list(data.get("forward_pair_order", []) or [])
    pairs = []
    seen = set()

    def add(a, b):
        try:
            a, b = int(a), int(b)
        except Exception:
            return
        if a == b:
            return
        key = _v166_pair_key(a, b)
        if key in seen:
            return
        # Display only a real live relation/finance setting.
        ab = str(b) in (fr.get(str(a), {}) or {})
        ba = str(a) in (fr.get(str(b), {}) or {})
        af = bool((ff.get(str(a), {}) or {}).get(str(b), False))
        bf = bool((ff.get(str(b), {}) or {}).get(str(a), False))
        if not (ab or ba or af or bf):
            return
        seen.add(key)
        pairs.append((a, b))

    if isinstance(order, list):
        for raw in order:
            try:
                a_s, b_s = str(raw).split(":", 1)
                add(int(a_s), int(b_s))
            except Exception:
                continue
    for src, dsts in fr.items():
        for dst in (dsts or {}).keys():
            add(src, dst)
    for src, dsts in ff.items():
        for dst, enabled in (dsts or {}).items():
            if enabled:
                add(src, dst)
    return pairs


def _v166_forward_allowed_ids():
    level = _v164_current_window_circle("forward", 1)
    try:
        ctx = int(current_state_chat_id() or 0)
    except Exception:
        ctx = 0
    allowed = set(int(x) for x in (_v164_scope_ids(level, ctx) or []))
    if int(level) == 1 and _v165_is_platform_owner_context():
        try: allowed.add(int(OWNER_ID))
        except Exception: pass
    return int(level), allowed


def collect_forward_pairs_for_menu() -> list[tuple[int, int]]:
    level, allowed = _v166_forward_allowed_ids()
    out = []
    for a, b in _v166_raw_forward_pairs():
        if a in allowed:
            out.append((a, b))
        elif b in allowed:
            # Legacy pair order may have the visible circle on the right. Reorient only for UI.
            out.append((b, a))
    try:
        bot_journal("v166_forward_pairs_menu", current_state_chat_id(), f"circle={level} raw={len(_v166_raw_forward_pairs())} shown={len(out)}")
    except Exception:
        pass
    return out


def build_forward_status_lines() -> list[str]:
    lines = []
    for a, b in collect_forward_pairs_for_menu():
        try:
            arrow, fin, ab_on, ba_on, ab_fin, ba_fin = _forward_pair_icons(a, b)
            if ab_on or ba_on or ab_fin or ba_fin:
                lines.append(f"• {chat_button_title(a)} -({arrow})-({fin})-{chat_button_title(b)}")
        except Exception:
            continue
    return lines


# ---------------------------------------------------------------------------
# Fast forwarding configuration. Network/durable config persistence is moved off the UI callback.
# ---------------------------------------------------------------------------
def _v166_schedule_forward_persist(*chat_ids):
    with _V166_FORWARD_DIRTY_LOCK:
        for cid in chat_ids:
            try: _V166_FORWARD_DIRTY_CHATS.add(int(cid))
            except Exception: pass

    def _fire():
        def _persist():
            with _V166_FORWARD_DIRTY_LOCK:
                ids = sorted(_V166_FORWARD_DIRTY_CHATS)
                _V166_FORWARD_DIRTY_CHATS.clear()
            try:
                save_data(data, full=True)
            except Exception as exc:
                try: log_error(f"v166 forward local persist: {exc}")
                except Exception: pass
            try:
                path = _owner_data_file()
                if path:
                    payload = _load_json(path, {}) or {}
                    if not isinstance(payload, dict):
                        payload = {}
                    payload["forward_rules"] = data.get("forward_rules", {}) or {}
                    payload["forward_finance"] = data.get("forward_finance", {}) or {}
                    payload["forward_pair_order"] = data.get("forward_pair_order", []) or []
                    _save_json(path, payload)
            except Exception as exc:
                try: log_error(f"v166 forward legacy persist: {exc}")
                except Exception: pass
            try:
                if ids:
                    schedule_config_backup_for_chats(*ids, delay=0.3)
                else:
                    schedule_config_backup_for_chats(delay=0.3)
            except Exception:
                pass
        if not V166_CONFIG_IO_TASK_POOL.submit("forward-persist", _persist):
            try: log_error("V166 CONFIG IO QUEUE FULL: forward-persist")
            except Exception: pass

    try:
        V166_CONFIG_IO_SCHEDULER.cancel("v166-forward-persist")
        V166_CONFIG_IO_SCHEDULER.schedule("v166-forward-persist", 0.12, _fire)
    except Exception:
        _fire()


def _v166_authorize_pair(src: int, dst: int):
    src, dst = int(src), int(dst)
    if tenant_same_space(src, dst):
        return
    try:
        actor = int(tenant_current_actor_user_id() or 0)
    except Exception:
        actor = 0
    if not tenant_is_platform_owner_user(actor):
        raise PermissionError("Можно связывать только свой 1-й круг и его 2-й круг")
    try:
        key = f"{min(src, dst)}:{max(src, dst)}"
        _v164_root().setdefault("global_forward_pairs", {})[key] = {
            "src": src, "dst": dst, "created_by": actor, "created_at": _v164_now(),
        }
    except Exception:
        pass


def _v166_cleanup_global_pair(a: int, b: int):
    try:
        arrow, fin, ab_on, ba_on, ab_fin, ba_fin = _forward_pair_icons(a, b)
        if ab_on or ba_on or ab_fin or ba_fin:
            return
        _v164_root().setdefault("global_forward_pairs", {}).pop(f"{min(int(a), int(b))}:{max(int(a), int(b))}", None)
    except Exception:
        pass


def _v166_enable_hidden_finance_memory(dst_chat_id: int):
    dst_chat_id = int(dst_chat_id)
    # The in-memory switch is short but finance-sensitive, so keep it under the destination chat lock.
    # Only SQLite/MEGA persistence is detached from the button path.
    with locked_chat(dst_chat_id):
        store = get_chat_store(dst_chat_id)
        settings = store.setdefault("settings", {})
        was_enabled = bool(store.get("finance_mode", False))
        store["finance_mode"] = True
        try: finance_active_chats.add(dst_chat_id)
        except Exception: pass
        settings["hidden_finance"] = True
        if not was_enabled:
            settings["quick_balance_enabled"] = False
            settings["quick_balance_behavior"] = "normal"
            settings["quick_balance_user_selected"] = True
            state = store.get("finance_window_state")
            if not isinstance(state, dict):
                state = {}
            state.update({
                "mode": "off", "main_windows": {}, "balance_panel_id": None,
                "balance_panel_mode": "mini", "current_view_day": str(store.get("current_view_day") or today_key()),
                "auto_reopen_on_boot": False, "updated_at": now_local().isoformat(timespec="seconds"),
            })
            store["finance_window_state"] = state
    _v166_schedule_forward_persist(dst_chat_id)


def ensure_hidden_finance_for_forward_dst(dst_chat_id: int):
    try:
        _v166_enable_hidden_finance_memory(int(dst_chat_id))
        bot_journal("forward_finance_auto_hidden", int(dst_chat_id), "v166 fast: hidden finance enabled; durable config queued")
    except Exception as exc:
        log_error(f"v166 ensure_hidden_finance_for_forward_dst({dst_chat_id}): {exc}")


def add_forward_link(src_chat_id: int, dst_chat_id: int, mode: str):
    src, dst = int(src_chat_id), int(dst_chat_id)
    _v166_authorize_pair(src, dst)
    with data_lock, _V166_FORWARD_STATE_LOCK:
        data.setdefault("forward_rules", {}).setdefault(str(src), {})[str(dst)] = str(mode)
    _v166_schedule_forward_persist(src, dst)


def remove_forward_link(src_chat_id: int, dst_chat_id: int):
    src, dst = int(src_chat_id), int(dst_chat_id)
    with data_lock, _V166_FORWARD_STATE_LOCK:
        fr = data.setdefault("forward_rules", {})
        ff = data.setdefault("forward_finance", {})
        (fr.get(str(src)) or {}).pop(str(dst), None)
        if str(src) in fr and not fr.get(str(src)):
            fr.pop(str(src), None)
        (ff.get(str(src)) or {}).pop(str(dst), None)
        if str(src) in ff and not ff.get(str(src)):
            ff.pop(str(src), None)
    _v166_cleanup_global_pair(src, dst)
    _v166_schedule_forward_persist(src, dst)


def set_forward_finance(src_chat_id: int, dst_chat_id: int, enabled: bool):
    src, dst = int(src_chat_id), int(dst_chat_id)
    _v166_authorize_pair(src, dst)
    with data_lock, _V166_FORWARD_STATE_LOCK:
        data.setdefault("forward_finance", {}).setdefault(str(src), {})[str(dst)] = bool(enabled)
    if enabled:
        ensure_hidden_finance_for_forward_dst(dst)
    _v166_schedule_forward_persist(src, dst)


def remove_forward_finance(src_chat_id: int, dst_chat_id: int):
    src, dst = int(src_chat_id), int(dst_chat_id)
    with data_lock, _V166_FORWARD_STATE_LOCK:
        ff = data.setdefault("forward_finance", {})
        (ff.get(str(src)) or {}).pop(str(dst), None)
        if str(src) in ff and not ff.get(str(src)):
            ff.pop(str(src), None)
    _v166_cleanup_global_pair(src, dst)
    _v166_schedule_forward_persist(src, dst)


def _remember_forward_pair(A: int, B: int):
    A, B = int(A), int(B)
    if A == B:
        return
    key, rev = f"{A}:{B}", f"{B}:{A}"
    with data_lock, _V166_FORWARD_STATE_LOCK:
        order = data.setdefault("forward_pair_order", [])
        if not isinstance(order, list):
            order = []
            data["forward_pair_order"] = order
        if key not in order and rev not in order:
            order.append(key)
    _v166_schedule_forward_persist(A, B)


def _forget_forward_pair_if_empty(A: int, B: int):
    A, B = int(A), int(B)
    try:
        arrow, fin, ab_on, ba_on, ab_fin, ba_fin = _forward_pair_icons(A, B)
        if ab_on or ba_on or ab_fin or ba_fin:
            return
    except Exception:
        return
    key, rev = f"{A}:{B}", f"{B}:{A}"
    with data_lock, _V166_FORWARD_STATE_LOCK:
        order = data.setdefault("forward_pair_order", [])
        if isinstance(order, list):
            data["forward_pair_order"] = [x for x in order if x not in {key, rev}]
    _v166_cleanup_global_pair(A, B)
    _v166_schedule_forward_persist(A, B)


# UI-only style toggle must repaint first; persistence can follow asynchronously.
def set_forward_menu_new_style_enabled(enabled: bool, chat_id: int | None = None):
    cid = int(chat_id) if chat_id is not None else current_state_chat_id()
    if cid is not None:
        owner_scoped_settings(int(cid))["forward_menu_new_style"] = bool(enabled)
        def _persist():
            try:
                save_data(data, chat_ids=[int(cid)])
                schedule_config_backup_for_chats(int(cid), delay=0.3)
            except Exception as exc:
                try: log_error(f"v166 forward style persist: {exc}")
                except Exception: pass
        V166_CONFIG_IO_TASK_POOL.submit(f"style:{int(cid)}", _persist)
    else:
        data.setdefault("_global_settings", {})["forward_menu_new_style"] = bool(enabled)
        V166_CONFIG_IO_TASK_POOL.submit("style:global", save_data, data)


def toggle_forward_menu_new_style(chat_id: int | None = None) -> bool:
    new_value = not forward_menu_new_style_enabled(chat_id)
    set_forward_menu_new_style_enabled(new_value, chat_id)
    return new_value


# ---------------------------------------------------------------------------
# Finance UI refresh: post-commit Telegram windows are independent per message.
# ---------------------------------------------------------------------------
def _v166_fin_submit(key, fn, *args):
    if not V166_FINANCE_UI_TASK_POOL.submit(str(key), fn, *args):
        try: log_error(f"V166 FINANCE UI QUEUE FULL: {key}")
        except Exception: pass
        return False
    return True


def refresh_balance_panel_now(chat_id: int):
    if callable(_V166_PREV_REFRESH_BALANCE):
        _v166_fin_submit(f"balance:{int(chat_id)}", _V166_PREV_REFRESH_BALANCE, int(chat_id))


def refresh_total_message_if_any(chat_id: int):
    if callable(_V166_PREV_REFRESH_TOTAL):
        _v166_fin_submit(f"total:{int(chat_id)}", _V166_PREV_REFRESH_TOTAL, int(chat_id))


def _v166_refresh_main_one(chat_id: int, day_key: str, mid: int):
    try:
        actual = get_registered_open_window(int(chat_id), int(mid))
        if actual and str(actual.get("window_type") or "") not in {"", "main_day"}:
            return
        text, _ = render_day_window(int(chat_id), str(day_key))
        bot.edit_message_text(
            text, chat_id=int(chat_id), message_id=int(mid),
            reply_markup=build_main_keyboard(str(day_key), int(chat_id)),
        )
        register_open_window(int(chat_id), int(mid), "main_day", code="О1", day_key=str(day_key))
    except Exception as exc:
        if "message is not modified" in str(exc).lower():
            return
        if _message_missing_error(exc):
            try:
                if int(get_active_window_id(int(chat_id), str(day_key)) or 0) == int(mid):
                    clear_active_window_id(int(chat_id), str(day_key))
            except Exception:
                pass
            try: unregister_open_window(int(chat_id), int(mid))
            except Exception: pass
            return
        try: log_error(f"v166 refresh main {chat_id}:{mid}: {exc}")
        except Exception: pass


def _v166_refresh_remaining(chat_id: int, mid: int):
    store = get_chat_store(int(chat_id))
    day_key = store.get("current_view_day") or today_key()
    try:
        bot.edit_message_text(
            build_remaining_text(int(chat_id), day_key), chat_id=int(chat_id), message_id=int(mid),
            reply_markup=build_remaining_keyboard(int(chat_id), day_key), parse_mode="HTML",
        )
        register_open_window(int(chat_id), int(mid), "remaining", code="Ф91", day_key=day_key)
    except Exception as exc:
        if _message_missing_error(exc):
            store["remaining_msg_id"] = None
            try: unregister_open_window(int(chat_id), int(mid))
            except Exception: pass


def _v166_refresh_registry_item(item: dict, target_chat_id: int):
    try:
        wtype = str((item or {}).get("window_type") or "")
        if wtype == "fin_view":
            _refresh_registered_fin_view(item, int(target_chat_id))
        elif wtype == "local_fin_view":
            _refresh_registered_local_fin_view(item, int(target_chat_id))
        elif wtype == "fin_categories_view":
            _refresh_registered_fin_categories_view(item, int(target_chat_id))
        elif wtype == "stored":
            _refresh_registered_stored_window(item, int(target_chat_id))
    except Exception as exc:
        try: log_error(f"v166 finance registry refresh: {exc}")
        except Exception: pass


def _v168_fin_window_is_recent(item: dict, max_age_seconds: float = 900.0) -> bool:
    """Old parallel Telegram windows remain usable, but do not auto-repaint forever on every transaction."""
    try:
        raw = str((item or {}).get("last_interaction_at") or (item or {}).get("updated_at") or "")
        if not raw:
            return False
        dt = datetime.fromisoformat(raw)
        now = now_local()
        if dt.tzinfo is None and getattr(now, "tzinfo", None) is not None:
            dt = dt.replace(tzinfo=now.tzinfo)
        return (now - dt).total_seconds() <= float(max_age_seconds)
    except Exception:
        return False


def refresh_registered_financial_windows(chat_id: int):
    """Fan out current/recent finance windows after a committed finance change.

    Same Telegram message is always serialized by one pool key; different messages in the same
    chat are allowed to refresh concurrently. This is the safe maximum parallelism for UI.
    """
    chat_id = int(chat_id)
    store = get_chat_store(chat_id)
    submitted_messages = set()

    def _submit_main(day_value, message_value):
        try:
            mid_i = int(message_value or 0)
            if not mid_i or mid_i in submitted_messages:
                return
            submitted_messages.add(mid_i)
            _v166_fin_submit(f"msg:{chat_id}:{mid_i}", _v166_refresh_main_one, chat_id, str(day_value or today_key()), mid_i)
        except Exception:
            pass

    # Active pointer(s) always refresh immediately.
    active_mids = set()
    for day_value, mid in list((get_or_create_active_windows(chat_id) or {}).items()):
        try:
            if int(mid or 0): active_mids.add(int(mid))
        except Exception: pass
        _submit_main(day_value, mid)

    # v168: old parallel windows remain valid when clicked, but only recent windows auto-refresh.
    # This prevents one transaction from generating dozens of Telegram edits after many versions/windows.
    registry_snapshot = list((_open_window_registry() or {}).items())
    for _key, item in registry_snapshot:
        try:
            if str((item or {}).get("window_type") or "") != "main_day":
                continue
            host = int((item or {}).get("chat_id") or (item or {}).get("host_chat_id") or 0)
            if host != chat_id:
                continue
            mid = int((item or {}).get("message_id") or 0)
            if mid not in active_mids and not _v168_fin_window_is_recent(item):
                continue
            _submit_main((item or {}).get("day_key") or store.get("current_view_day") or today_key(), mid)
        except Exception:
            continue

    rem_mid = int(store.get("remaining_msg_id") or 0)
    if rem_mid and rem_mid not in submitted_messages:
        submitted_messages.add(rem_mid)
        _v166_fin_submit(f"msg:{chat_id}:{rem_mid}", _v166_refresh_remaining, chat_id, rem_mid)

    # Categories window, if one is actually open, gets the same per-message serialization rule.
    cat_mid = int(store.get("categories_msg_id") or 0)
    if cat_mid:
        _v166_fin_submit(f"msg:{chat_id}:{cat_mid}", _refresh_categories_window_from_state, chat_id)

    # Owner/auxiliary views of this financial chat may live in another Telegram chat. Each concrete
    # host message is independent and therefore can update in parallel with the main windows.
    for _key, item in registry_snapshot:
        try:
            wtype = str((item or {}).get("window_type") or "")
            if wtype not in {"fin_view", "local_fin_view", "fin_categories_view", "stored"}:
                continue
            if not _v168_fin_window_is_recent(item):
                continue
            params = (item or {}).get("params") or {}
            if wtype == "fin_view" and int(params.get("target_chat_id") or 0) != chat_id:
                continue
            if wtype in {"local_fin_view", "fin_categories_view"}:
                target_hint = int(params.get("target_chat_id") or (item or {}).get("chat_id") or 0)
                if target_hint not in {0, chat_id}:
                    continue
            host = int((item or {}).get("chat_id") or (item or {}).get("host_chat_id") or chat_id)
            mid2 = int((item or {}).get("message_id") or 0)
            if not mid2:
                continue
            _v166_fin_submit(f"msg:{host}:{mid2}", _v166_refresh_registry_item, dict(item or {}), chat_id)
        except Exception:
            continue
    return True


def _v177_legacy_0247_schedule_financial_window_refresh(chat_id: int, day_key: str | None = None, reason: str = "finance_changed", delay: float = 0.0):
    """v166: post-commit UI refresh is dispatched immediately to independent per-message lanes.

    finance_changed() already debounces/serializes the business commit. Adding another 120-150 ms
    debounce here only made the visible balance lag, so the second debounce is removed.
    """
    chat_id = int(chat_id)
    day_key = str(day_key or get_chat_store(chat_id).get("current_view_day") or today_key())[:10]

    def _dispatch():
        try:
            # Balance/total helpers are no-ops if the corresponding window is not open.
            refresh_balance_panel_now(chat_id)
            refresh_total_message_if_any(chat_id)
            refresh_registered_financial_windows(chat_id)
            bot_journal("finance_window_refresh_parallel", chat_id, f"day={day_key} reason={reason} v166=1")
        except Exception as exc:
            try: log_error(f"v166 finance window dispatch {chat_id}: {exc}")
            except Exception: pass

    # v168: several layers can report the same mutation (insert -> reserve -> finalize). Coalesce them
    # for a few milliseconds so one transaction does not repaint the same Telegram windows 3-4 times.
    def _fire_visual():
        if not V166_FINANCE_UI_TASK_POOL.submit(f"dispatch:{chat_id}", _dispatch):
            try: log_error(f"V166 FINANCE UI DISPATCH QUEUE FULL: {chat_id}")
            except Exception: pass
    try:
        key = f"finance-visual:{chat_id}"
        V166_FINANCE_DEBOUNCE_SCHEDULER.cancel(key)
        V166_FINANCE_DEBOUNCE_SCHEDULER.schedule(key, max(0.01, min(float(delay or 0.0), 0.05)), _fire_visual)
        return True
    except Exception:
        _fire_visual()
        return True
try: _v177_legacy_0247_schedule_financial_window_refresh.__name__ = 'schedule_financial_window_refresh'
except Exception: pass
schedule_financial_window_refresh = _v177_legacy_0247_schedule_financial_window_refresh


# Faster finance finalization debounce. Actual finance writes still go through FINANCE_TASK_POOL per chat.
def finance_changed(chat_id: int, day_key: str | None = None, reason: str = "change", delay: float = 0.05):
    chat_id = int(chat_id)
    day_key = day_key or get_chat_store(chat_id).get("current_view_day") or today_key()
    try:
        requested = max(0.0, float(delay))
    except Exception:
        requested = 0.05
    # UI-oriented finance changes should settle almost immediately; restore/reset can still request 0.1 s.
    effective = min(requested, 0.10)
    bot_journal("finance_changed_scheduled", chat_id, f"day={day_key} reason={reason} delay={effective} v168=pre_refresh")
    try:
        # Visible windows must not wait for SQLite/MEGA/finalization. Per-message lanes coalesce safely.
        schedule_financial_window_refresh(chat_id, day_key, reason=f"{reason}:precommit_v168")
    except Exception as exc:
        try: log_error(f"v168 immediate finance UI {chat_id}: {exc}")
        except Exception: pass

    def _job():
        if not FINANCE_TASK_POOL.submit(chat_id, _finance_changed_now, chat_id, day_key, reason):
            try: log_error(f"FINANCE QUEUE FULL, RETRY: {chat_id}")
            except Exception: pass
            V166_FINANCE_DEBOUNCE_SCHEDULER.schedule(f"finance-finalize:{chat_id}", 0.25, _fire)

    def _fire():
        with timer_lock:
            _finalize_timers.pop(chat_id, None)
        _job()

    with timer_lock:
        _finalize_timers[chat_id] = _v166_time.time() + effective
    try:
        V166_FINANCE_DEBOUNCE_SCHEDULER.cancel(f"finance-finalize:{chat_id}")
    except Exception:
        pass
    V166_FINANCE_DEBOUNCE_SCHEDULER.schedule(f"finance-finalize:{chat_id}", effective, _fire)


def schedule_finalize(chat_id: int, day_key: str, delay: float = 0.05):
    return finance_changed(int(chat_id), str(day_key), reason="schedule_finalize", delay=min(float(delay or 0.05), 0.10))


# ---------------------------------------------------------------------------
# Restore compatibility.
# ---------------------------------------------------------------------------


try:
    bot_journal(
        "v166_fast_parallel_forward_pairs_installed",
        int(OWNER_ID or 0),
        "pairs=raw_rules_not_v148_filter; callback_ack=0.05s; safe_ui=per_window; forward_config=per_pair; config_io=dedicated; finance_ui=per_message; finance_debounce=dedicated; finance_finalize<=0.10s",
    )
except Exception:
    pass

# ---- integrated from 117_v167_excel_formulas_thuwed_google_tz.py ----
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

VERSION = "bot_v170_clear_journal_names"
V167_FILE_MARKER = "v170_clear_journal_names"

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
    # v169 adds one explicit update mode while keeping the old enabled/time keys as mirrors.
    # Existing v167/v168 settings migrate without changing their daily behaviour.
    if str(cfg.get("mode") or "") not in {"manual", "change", "m15", "h1", "d0001", "d0501"}:
        if not bool(cfg.get("enabled", True)):
            cfg["mode"] = "manual"
        else:
            cfg["mode"] = "d0001" if str(cfg.get("time") or "05:01") == "00:01" else "d0501"
    cfg["enabled"] = str(cfg.get("mode")) != "manual"
    if str(cfg.get("mode")) == "d0001": cfg["time"] = "00:01"
    if str(cfg.get("mode")) == "d0501": cfg["time"] = "05:01"
    cfg.setdefault("last_run_key", "")
    cfg.setdefault("last_ok_at", "")
    cfg.setdefault("last_error", "")
    cfg.setdefault("last_period", "")
    cfg["schema"] = max(2, int(cfg.get("schema", 1) or 1))
    return cfg


def _v167_persist_schedule(target_chat_id: int):
    """Persist only the changed settings on the callback thread.

    v176 measurements exposed that the old implementation rebuilt JSON + CSV +
    optional XLSX for a simple Google schedule toggle.  SQLite is the immediate
    source of truth; external/config backup remains debounced in the background.
    """
    cid = int(target_chat_id)
    started = _v167_time.monotonic()
    try:
        save_data(data, chat_ids=[cid])
    except Exception as exc:
        try: log_error(f"v177 google schedule SQLite persist {cid}: {exc}")
        except Exception: pass
    try:
        schedule_config_backup_for_chats(cid, delay=0.8)
    except Exception:
        pass
    try:
        stage = globals().get("v177_perf_stage")
        if callable(stage): stage("sqlite_google_settings", _v167_time.monotonic() - started)
    except Exception:
        pass


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
        mode = str(cfg.get("mode") or "d0501")
        enabled = mode != "manual"
        selected = str(cfg.get("time") or "05:01") if mode in {"d0001", "d0501"} else ""
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


def _v169_google_mode(cfg: dict | None) -> str:
    cfg = cfg if isinstance(cfg, dict) else {}
    mode = str(cfg.get("mode") or "").strip().lower()
    if mode not in {"manual", "change", "m15", "h1", "d0001", "d0501"}:
        if not bool(cfg.get("enabled", True)):
            mode = "manual"
        else:
            mode = "d0001" if str(cfg.get("time") or "05:01") == "00:01" else "d0501"
    return mode


def _v169_google_mode_label(mode: str) -> str:
    return {
        "manual": "✋ Только вручную",
        "change": "⚡ После изменений",
        "m15": "🕒 Каждые 15 минут",
        "h1": "🕐 Каждый час",
        "d0001": "🌙 Ежедневно 00:01",
        "d0501": "🌅 Ежедневно 05:01",
    }.get(str(mode), "✋ Только вручную")


def _v169_set_google_mode(target_chat_id: int, mode: str) -> dict:
    mode = str(mode or "manual").strip().lower()
    if mode not in {"manual", "change", "m15", "h1", "d0001", "d0501"}:
        mode = "manual"
    cfg = _v167_google_schedule_cfg(int(target_chat_id))
    cfg["mode"] = mode
    cfg["enabled"] = mode != "manual"  # legacy mirror for F47 / old backups
    if mode == "d0001": cfg["time"] = "00:01"
    elif mode == "d0501": cfg["time"] = "05:01"
    cfg["last_run_key"] = ""  # selected mode may run immediately/catch up once
    _v167_persist_schedule(int(target_chat_id))
    return cfg


def _v169_google_settings_text(target_chat_id: int) -> str:
    target_chat_id = int(target_chat_id)
    cfg = _v167_google_schedule_cfg(target_chat_id)
    mode = _v169_google_mode(cfg)
    start_key, end_key = _v167_thuwed_bounds(today_key())
    tab = _v167_period_title(start_key, end_key)
    last_ok = str(cfg.get("last_ok_at") or "—")
    last_error = str(cfg.get("last_error") or "").strip()
    lines = [
        "☁️ GOOGLE ТАБЛИЦА ЧТ–СР",
        "",
        f"Чат: {get_chat_display_name(target_chat_id)}",
        f"Текущий лист: {tab}",
        f"Режим обновления: {_v169_google_mode_label(mode)}",
        f"Последнее успешное: {last_ok}",
    ]
    if last_error:
        lines.append(f"Последняя ошибка: {last_error[:350]}")
    lines += [
        "",
        "Период листа всегда четверг → среда. При наступлении нового четверга создаётся новый лист; внутри периода обновляется тот же лист.",
        "",
        "Выберите способ/период обновления ниже.",
        "",
        "Ф240⏰",
    ]
    return "\n".join(lines)[:3900]


def _v169_google_settings_keyboard(target_chat_id: int, day_key: str | None = None):
    target_chat_id = int(target_chat_id)
    day_key = str(day_key or get_chat_store(target_chat_id).get("current_view_day") or today_key())[:10]
    mode = _v169_google_mode(_v167_google_schedule_cfg(target_chat_id))
    kb = types.InlineKeyboardMarkup(row_width=2)
    def _b(label, value):
        mark = "✅ " if mode == value else ""
        return IB(mark + label, callback_data=f"v169:gmode:{target_chat_id}:{value}")
    kb.row(_b("✋ Вручную", "manual"), _b("⚡ После изменений", "change"))
    kb.row(_b("🕒 15 минут", "m15"), _b("🕐 1 час", "h1"))
    kb.row(_b("🌙 00:01", "d0001"), _b("🌅 05:01", "d0501"))
    kb.row(IB("☁️ Обновить Чт–Ср сейчас", callback_data=f"v169:gnow:{target_chat_id}"))
    kb.row(
        IB("🔙 Назад в Инфо", callback_data=f"d:{day_key}:info"),
        IB("⬅️ Осн. окно", callback_data=f"d:{day_key}:back_main"),
    )
    return kb


def _v169_google_enqueue(target_chat_id: int, reason: str) -> bool:
    target_chat_id = int(target_chat_id)
    key = f"google-thuwed:{target_chat_id}"
    pool = globals().get("EXPORT_TASK_POOL")
    if pool is not None:
        try:
            ok = bool(pool.submit_unique(key, _v167_google_update_target, target_chat_id, str(reason)))
            if not ok:
                try: bot_journal("google_thuwed_coalesced", target_chat_id, f"reason={reason}")
                except Exception: pass
            return ok
        except Exception as exc:
            try: log_error(f"v169 Google queue {target_chat_id}: {exc}")
            except Exception: pass
    # v179: no per-request fallback thread; export pool is a required core lane.
    return False


def _v169_google_change_fire(target_chat_id: int):
    if _v169_google_mode(_v167_google_schedule_cfg(int(target_chat_id), create=False)) != "change":
        return
    if not _v169_google_enqueue(int(target_chat_id), "finance-change"):
        try:
            DELAYED_SCHEDULER.schedule(
                f"google-change-retry:{int(target_chat_id)}", 15.0,
                _v169_google_change_fire, int(target_chat_id)
            )
        except Exception:
            pass


def _v169_schedule_google_after_change(target_chat_id: int, reason: str = "finance_changed") -> None:
    try:
        cfg = _v167_google_schedule_cfg(int(target_chat_id), create=False)
        if not cfg or _v169_google_mode(cfg) != "change":
            return
        low = str(reason or "").casefold()
        if low and not any(x in low for x in ("finance", "record", "forward", "edit", "delete", "add", "currency")):
            return
        # Debounce bursts: multiple records/forwarded copies become one Google API write.
        DELAYED_SCHEDULER.schedule(
            f"google-change:{int(target_chat_id)}", 8.0,
            _v169_google_change_fire, int(target_chat_id)
        )
    except Exception as exc:
        try: log_error(f"v169 schedule Google after change {target_chat_id}: {exc}")
        except Exception: pass


# Late wrapper: every real finance mutation already goes through this active v166 hook.
_V169_BASE_SCHEDULE_FINANCIAL_WINDOW_REFRESH = globals().get("schedule_financial_window_refresh")
def schedule_financial_window_refresh(chat_id: int, day_key: str | None = None, reason: str = "finance_changed", delay: float = 0.0):
    result = None
    if callable(_V169_BASE_SCHEDULE_FINANCIAL_WINDOW_REFRESH):
        result = _V169_BASE_SCHEDULE_FINANCIAL_WINDOW_REFRESH(int(chat_id), day_key, reason=reason, delay=delay)
    _v169_schedule_google_after_change(int(chat_id), reason)
    return result


def _v167_google_scheduler_tick():
    try:
        if callable(globals().get("runtime_is_shutting_down")) and runtime_is_shutting_down():
            return
        if callable(globals().get("runtime_is_ready")) and not runtime_is_ready():
            return
        now = now_local(); hhmm = now.strftime("%H:%M"); date_key = now.strftime("%Y-%m-%d")
        minute = int(now.strftime("%M")); hour = int(now.strftime("%H"))
        for cid in _v167_known_chat_ids():
            cfg = _v167_google_schedule_cfg(cid, create=False)
            if not cfg: continue
            mode = _v169_google_mode(cfg)
            if mode in {"manual", "change"}: continue
            run_key = ""; reason = mode
            if mode == "m15": run_key = f"{date_key}@{hour:02d}:{(minute // 15) * 15:02d}"
            elif mode == "h1": run_key = f"{date_key}@{hour:02d}"
            elif mode in {"d0001", "d0501"}:
                selected = "00:01" if mode == "d0001" else "05:01"
                if hhmm < selected: continue
                run_key = f"{date_key}@{selected}"
            if not run_key or str(cfg.get("last_run_key") or "") == run_key: continue
            cfg["last_run_key"] = run_key
            _v167_persist_schedule(cid)
            _v169_google_enqueue(cid, reason)
    except Exception as exc:
        try: log_error(f"v169 google scheduler: {exc}")
        except Exception: pass
    finally:
        try:
            if not (callable(globals().get("runtime_is_shutting_down")) and runtime_is_shutting_down()):
                DELAYED_SCHEDULER.schedule("google-thuwed-scheduler", 20.0, _v167_google_scheduler_tick)
        except Exception: pass

def _v167_google_scheduler_loop():
    return _v167_google_scheduler_tick()


def _v167_start_google_scheduler():
    global _V167_GOOGLE_SCHEDULER_STARTED
    if _V167_GOOGLE_SCHEDULER_STARTED: return
    _V167_GOOGLE_SCHEDULER_STARTED = True
    DELAYED_SCHEDULER.schedule("google-thuwed-scheduler", 3.0, _v167_google_scheduler_tick)


def _v167_schedule_callback_filter(call):
    try:
        raw = str(getattr(call, "data", "") or "")
        return raw.startswith("v167:g") or raw.startswith("v169:g")
    except Exception:
        return False


def _v167_schedule_callback(call):
    raw = str(getattr(call, "data", "") or ""); parts = raw.split(":")
    try:
        # v169 INFO configuration window.
        if raw.startswith("v169:"):
            action = str(parts[1] if len(parts) > 1 else "")
            target = int(parts[2]) if len(parts) > 2 else int(getattr(getattr(call, "message", None), "chat", None).id)
            uid = int(getattr(getattr(call, "from_user", None), "id", 0) or 0)
            if not (tenant_is_platform_owner_user(uid) or tenant_can_manage(uid, chat_id=target)):
                bot.answer_callback_query(call.id, "Недостаточно прав", show_alert=True); return
            if action == "gmenu":
                try: bot.answer_callback_query(call.id)
                except Exception: pass
                safe_edit(
                    bot, call, _v169_google_settings_text(target),
                    reply_markup=_v169_google_settings_keyboard(target)
                )
                return
            if action == "gmode":
                mode = str(parts[3] if len(parts) > 3 else "manual")
                cfg = _v169_set_google_mode(target, mode)
                try: bot.answer_callback_query(call.id, _v169_google_mode_label(_v169_google_mode(cfg)))
                except Exception: pass
                safe_edit(
                    bot, call, _v169_google_settings_text(target),
                    reply_markup=_v169_google_settings_keyboard(target)
                )
                return
            if action == "gnow":
                queued = _v169_google_enqueue(target, "manual-info")
                try:
                    bot.answer_callback_query(call.id, "Обновление поставлено в очередь" if queued else "Обновление уже выполняется")
                except Exception: pass
                safe_edit(
                    bot, call, _v169_google_settings_text(target),
                    reply_markup=_v169_google_settings_keyboard(target)
                )
                return
            return

        # v167/F47 compatibility controls.
        target = int(parts[2]) if len(parts) > 2 else int(OWNER_ID or 0)
        uid = int(getattr(getattr(call, "from_user", None), "id", 0) or 0)
        if not (tenant_is_platform_owner_user(uid) or tenant_can_manage(uid, chat_id=target)):
            bot.answer_callback_query(call.id, "Недостаточно прав", show_alert=True); return
        cfg = _v167_google_schedule_cfg(target)
        if parts[1] == "gtoggle":
            if _v169_google_mode(cfg) == "manual":
                _v169_set_google_mode(target, "d0001" if str(cfg.get("time") or "05:01") == "00:01" else "d0501")
                msg = "Автообновление включено"
            else:
                _v169_set_google_mode(target, "manual")
                msg = "Автообновление выключено"
            cfg = _v167_google_schedule_cfg(target)
        elif parts[1] == "gtime":
            code = str(parts[3] if len(parts) > 3 else "0501")
            cfg = _v169_set_google_mode(target, "d0001" if code == "0001" else "d0501")
            msg = f"Чт–Ср: ежедневно в {cfg['time']}"
        elif parts[1] == "gnow":
            msg = "Обновляю текущий лист Чт–Ср"
            _v169_google_enqueue(target, "manual-f47")
        else:
            return
        # _v169_set_google_mode() already persisted gtoggle/gtime exactly once.
        # Manual gnow changes no configuration and needs no local file rebuild.
        try: bot.answer_callback_query(call.id, msg)
        except Exception: pass
        # Update only F47 button labels; no Google API work on callback thread.
        try:
            kb = _v167_copy.deepcopy(getattr(getattr(call, "message", None), "reply_markup", None))
            mode = _v169_google_mode(cfg)
            for row in getattr(kb, "keyboard", []) or []:
                for btn in row:
                    cb = str(getattr(btn, "callback_data", "") or "")
                    if cb == f"v167:gtoggle:{target}": btn.text = ("✅ " if mode != "manual" else "❌ ") + "Google Чт–Ср авто"
                    elif cb == f"v167:gtime:{target}:0001": btn.text = ("✅ " if mode == "d0001" else "") + "00:01"
                    elif cb == f"v167:gtime:{target}:0501": btn.text = ("✅ " if mode == "d0501" else "") + "05:01"
            v178_edit_reply_markup_async(
                call.message.chat.id, call.message.message_id, kb,
                "google_schedule_markup_v178",
            )
        except Exception: pass
    except Exception as exc:
        try: log_error(f"v169 schedule callback {raw}: {exc}")
        except Exception: pass
        try: bot.answer_callback_query(call.id, "Ошибка настройки Google", show_alert=True)
        except Exception: pass


def _v167_install_schedule_callback():
    return 0  # v179: registration/wrapper retired; final router owns callbacks


# Diagnostics/window map for the new INFO Google settings window.
try:
    WINDOW_MARKER_CONSTANTS.update({
        "v169:gmenu:*": "Ф240",
        "v169:gmode:*": "Ф240",
        "v169:gnow:*": "Ф240",
    })
except Exception:
    pass


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


def _v177_legacy_0321_v160_save_tz(chat_id: int, user_id: int, marker: str, body: str, source: dict) -> dict:
    if not callable(_V167_BASE_SAVE_TZ):
        raise RuntimeError("TZ storage unavailable")
    row=_V167_BASE_SAVE_TZ(chat_id,user_id,marker,body,source)
    try:
        row["version"]=VERSION; row["status"]="open"; row["archived_by_version"]=""
        _v160_persist_annotations(chat_id)
    except Exception: pass
    return row
try: _v177_legacy_0321_v160_save_tz.__name__ = '_v160_save_tz'
except Exception: pass
_v160_save_tz = _v177_legacy_0321_v160_save_tz


def _v177_legacy_0326_v167_tz_export(kind: str):
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
try: _v177_legacy_0326_v167_tz_export.__name__ = '_v167_tz_export'
except Exception: pass
_v167_tz_export = _v177_legacy_0326_v167_tz_export


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


def _v177_legacy_0318_v160_augment_markup(reply_markup, text: str):
    kb=_V167_BASE_AUGMENT_MARKUP(reply_markup,text) if callable(_V167_BASE_AUGMENT_MARKUP) else reply_markup
    try:
        if _v160_marker_from_text(text)=="Ф89" and isinstance(kb,types.InlineKeyboardMarkup):
            callbacks=_v160_markup_callbacks(kb)
            if "v167:export_tz_archive" not in callbacks:
                kb.row(IB("🗃 Скачать архив ТЗ",callback_data="v167:export_tz_archive"))
    except Exception: pass
    return kb
try: _v177_legacy_0318_v160_augment_markup.__name__ = '_v160_augment_markup'
except Exception: pass
_v160_augment_markup = _v177_legacy_0318_v160_augment_markup


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
_V167_SCHEDULE_CALLBACK = 0  # v179 final callback router
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
    bot_journal("v169_installed", int(OWNER_ID or 0), "main TZ button + immediate finance-forward edit UI + tri-state reminder merge + Google Thu-Wed update modes")
    bot_journal("v170_journal_names_installed", int(OWNER_ID or 0), "distinct human filenames; version words no longer misclassify downloads")
except Exception:
    pass

# ---- integrated from 118_v171_all_tz_reliability.py ----
"""v171: implement all open v169 window TZ items and reliability fixes.

The patch is intentionally loaded last.  It repairs active runtime hooks instead of
editing old historical implementations, so one source of truth wins after module load.
"""
import copy as _v171_copy
import gzip as _v171_gzip
import hashlib as _v171_hashlib
import json as _v171_json
import os as _v171_os
import re as _v171_re
import shutil as _v171_shutil
import sqlite3 as _v171_sqlite3
import tempfile as _v171_tempfile
import threading as _v171_threading
import time as _v171_time

VERSION = "bot_v171_all_tz_reliability"
V171_FILE_MARKER = "v171_all_tz_reliability"

# ---------------------------------------------------------------------------
# Project-wide TZ contract.
# ---------------------------------------------------------------------------
V171_TZ_SCOPE_POLICY = "global_all_contours_if_unspecified"
V171_TZ_SCOPE_TEXT = (
    "ТЗ без явно указанного чата, направления или контура применяется ко всему боту: "
    "ко всем направлениям и всем контурам."
)
try:
    _v171_gs = data.setdefault("_global_settings", {})
    _v171_gs["tz_scope_policy_v171"] = V171_TZ_SCOPE_POLICY
    _v171_gs["tz_scope_policy_text_v171"] = V171_TZ_SCOPE_TEXT
except Exception:
    pass

# ---------------------------------------------------------------------------
# 1) F233 file-status window: v163 calls a name that never existed.
# ---------------------------------------------------------------------------
def _v161_schedule_delete(chat_id: int, message_id: int, delay: float, prefix: str = "delete") -> None:
    fn = globals().get("_v160_schedule_delete")
    if callable(fn):
        fn(int(chat_id), int(message_id), float(delay), str(prefix or "delete"))
        return
    scheduler = globals().get("DELAYED_SCHEDULER")
    if scheduler is not None:
        scheduler.schedule(
            f"v171:{prefix}:{int(chat_id)}:{int(message_id)}",
            max(0.0, float(delay or 0.0)),
            lambda: _v171_delete_message_quiet(int(chat_id), int(message_id)),
        )


def _v171_delete_message_quiet(chat_id: int, message_id: int) -> None:
    try:
        bot.delete_message(int(chat_id), int(message_id))
    except Exception:
        pass
    try:
        unregister_open_window(int(chat_id), int(message_id))
    except Exception:
        pass


# ---------------------------------------------------------------------------
# 2/6) 💰 financial-forward edit mode is genuinely GLOBAL.
# v148 redefined v124's global functions later in load order, which made the
# source chat's tenant decide the initial forwarded-copy UI.
# ---------------------------------------------------------------------------
V171_FORWARD_COPY_EDIT_MODES = ("normal", "button", "slash")


def _v171_forward_mode_root() -> dict:
    try:
        return data.setdefault("_global_settings", {})
    except Exception:
        return {}


def forward_copy_edit_mode(chat_id: int | None = None) -> str:
    gs = _v171_forward_mode_root()
    mode = str(gs.get("forward_copy_edit_mode_global") or "").strip().lower()
    if mode not in V171_FORWARD_COPY_EDIT_MODES:
        # Import the most recent old choice once, then never scope it by source chat again.
        candidates = [str(gs.get("forward_copy_edit_mode") or "").strip().lower()]
        try:
            owner = int(OWNER_ID or 0)
            if owner:
                candidates.append(str(owner_scoped_settings(owner).get("forward_copy_edit_mode") or "").strip().lower())
                candidates.append(str(get_chat_store(owner).setdefault("settings", {}).get("forward_copy_edit_mode") or "").strip().lower())
        except Exception:
            pass
        mode = next((x for x in candidates if x in V171_FORWARD_COPY_EDIT_MODES), "normal")
        gs["forward_copy_edit_mode_global"] = mode
        gs["forward_copy_edit_mode"] = mode
    try:
        if not version_mode_feature("forward_copy_edit"):
            return "normal"
    except Exception:
        pass
    return mode if mode in V171_FORWARD_COPY_EDIT_MODES else "normal"


def set_forward_copy_edit_mode(chat_id: int, mode: str):
    mode = str(mode or "normal").strip().lower()
    if mode not in V171_FORWARD_COPY_EDIT_MODES:
        mode = "normal"
    gs = _v171_forward_mode_root()
    gs["forward_copy_edit_mode_global"] = mode
    gs["forward_copy_edit_mode"] = mode
    # Compatibility mirrors only; runtime reads the global value above.
    try:
        for cid in collect_all_known_chat_ids(include_owner=True):
            try:
                get_chat_store(int(cid)).setdefault("settings", {})["forward_copy_edit_mode"] = mode
            except Exception:
                pass
    except Exception:
        pass
    try:
        scheduler = globals().get("V166_CONFIG_IO_SCHEDULER")
        if scheduler is not None:
            scheduler.schedule("v171-forward-mode", 0.05, lambda: save_data(data, root_only=True))
        else:
            save_data(data, root_only=True)
    except Exception:
        pass
    try:
        schedule_config_backup_for_chats(delay=1.0)
    except Exception:
        pass
    return mode


def cycle_forward_copy_edit_mode(chat_id: int) -> str:
    current = forward_copy_edit_mode(chat_id)
    try:
        idx = V171_FORWARD_COPY_EDIT_MODES.index(current)
    except ValueError:
        idx = 0
    return set_forward_copy_edit_mode(int(chat_id), V171_FORWARD_COPY_EDIT_MODES[(idx + 1) % len(V171_FORWARD_COPY_EDIT_MODES)])


def forward_copy_edit_mode_label(chat_id: int) -> str:
    return {
        "normal": "💰фин.пересылка: обычно",
        "button": "💰фин.пересылка: кнопка",
        "slash": "💰фин.пересылка: слеш",
    }.get(forward_copy_edit_mode(chat_id), "💰фин.пересылка: обычно")


# ---------------------------------------------------------------------------
# 5) Reminder cross-chat delivery + global tri-state merge mode.
# Platform-owner reminders may target any explicitly selected known chat, while
# non-platform spaces remain isolated.
# ---------------------------------------------------------------------------
_V171_PREV_REMINDER_CHAT_ALLOWED = globals().get("_v149_reminder_chat_allowed")


def _v177_legacy_0265_v149_reminder_chat_allowed(cfg: dict, chat_id: int) -> bool:
    cid = int(chat_id)
    try:
        selected = {int(x) for x in ((cfg or {}).get("chat_ids") or [])}
    except Exception:
        selected = set()
    if cid not in selected:
        return False
    try:
        tid = str(_v149_reminder_cfg_tenant(cfg) or TENANT_PLATFORM_ID)
    except Exception:
        tid = str(globals().get("TENANT_PLATFORM_ID") or "platform")
    platform_id = str(globals().get("TENANT_PLATFORM_ID") or "platform")
    if tid == platform_id:
        try:
            known = {int(x) for x in collect_all_known_chat_ids(include_owner=True)}
            return cid in known
        except Exception:
            return False
    try:
        return bool(_v149_chat_belongs_to_tenant(cid, tid))
    except Exception:
        return bool(_V171_PREV_REMINDER_CHAT_ALLOWED(cfg, cid)) if callable(_V171_PREV_REMINDER_CHAT_ALLOWED) else False
try: _v177_legacy_0265_v149_reminder_chat_allowed.__name__ = '_v149_reminder_chat_allowed'
except Exception: pass
_v149_reminder_chat_allowed = _v177_legacy_0265_v149_reminder_chat_allowed


V171_REMINDER_MERGE_MODES = ("off", "smart", "single")


def _v171_reminder_global_mode(migrate: bool = True) -> str:
    gs = data.setdefault("_global_settings", {})
    mode = str(gs.get("reminder_merge_mode_global_v171") or "").strip().lower()
    if mode not in V171_REMINDER_MERGE_MODES and migrate:
        old = "off"
        try:
            owner = int(OWNER_ID or 0)
            if owner:
                settings = _v149_reminder_chat_settings(None, owner)
                old = str(settings.get("merge_mode") or "").strip().lower()
                if old not in V171_REMINDER_MERGE_MODES:
                    old = "smart" if bool(settings.get("merge_enabled", False)) else "off"
        except Exception:
            old = "off"
        mode = old if old in V171_REMINDER_MERGE_MODES else "off"
        gs["reminder_merge_mode_global_v171"] = mode
    return mode if mode in V171_REMINDER_MERGE_MODES else "off"


def reminder_merge_mode(tenant_id: str | None = None, chat_id: int | None = None) -> str:
    return _v171_reminder_global_mode(True)


def reminder_merge_enabled(tenant_id: str | None = None, chat_id: int | None = None) -> bool:
    return reminder_merge_mode(tenant_id, chat_id) != "off"


def reminder_merge_mode_label(tenant_id: str | None = None, chat_id: int | None = None) -> str:
    return {"off": "ВЫКЛ", "smart": "ВКЛ", "single": "1 СООБЩЕНИЕ"}.get(reminder_merge_mode(tenant_id, chat_id), "ВЫКЛ")


def _v171_cycle_reminder_merge() -> str:
    current = _v171_reminder_global_mode(True)
    try:
        idx = V171_REMINDER_MERGE_MODES.index(current)
    except ValueError:
        idx = 0
    mode = V171_REMINDER_MERGE_MODES[(idx + 1) % len(V171_REMINDER_MERGE_MODES)]
    data.setdefault("_global_settings", {})["reminder_merge_mode_global_v171"] = mode
    try:
        save_data(data, root_only=True)
    except Exception:
        pass
    return mode


_V171_PREV_V149_EXTENSION_CALLBACK = globals().get("v149_extension_callback")


def v149_extension_callback(call, data_str: str) -> bool:
    raw = str(data_str or "")
    if raw.startswith("v149:rem:merge:"):
        chat_id = int(call.message.chat.id)
        user_id = int(getattr(getattr(call, "from_user", None), "id", 0) or 0)
        try:
            tid = str(tenant_id_for_chat(chat_id, create=False) or TENANT_PLATFORM_ID)
            if not tenant_can_manage(user_id, tid):
                bot.answer_callback_query(call.id, "Недостаточно прав", show_alert=True)
                return True
        except Exception:
            pass
        mode = _v171_cycle_reminder_merge()
        parts = raw.split(":")
        page = int(parts[3]) if len(parts) > 3 and str(parts[3]).isdigit() else 0
        day_key = parts[4] if len(parts) > 4 else today_key()
        try:
            reminder_text = build_reminder_list_text()
            reminder_keyboard = build_reminder_list_keyboard(day_key, page)
            safe_edit(bot, call, reminder_text, reply_markup=reminder_keyboard)
        except Exception as exc:
            try:
                log_error(f"v171 reminder merge UI: {exc}")
            except Exception:
                pass
        try:
            REMINDER_TASK_POOL.submit_unique("reminder-v149-batch", _v149_reminder_batch_job, None)
        except Exception:
            pass
        try:
            bot.answer_callback_query(call.id, f"Объединять: {reminder_merge_mode_label()}")
        except Exception:
            pass
        return True
    if callable(_V171_PREV_V149_EXTENSION_CALLBACK):
        return bool(_V171_PREV_V149_EXTENSION_CALLBACK(call, raw))
    return False


# ---------------------------------------------------------------------------
# 8/9/10) Every marked menu gets predictable navigation + Description.
# Existing button rows are preserved; missing standard controls are appended.
# ---------------------------------------------------------------------------
try:
    WINDOW_MARKER_CONSTANTS.update({
        "v171:desc": "Ф241",
        "v171:desc_close": "Ф241",
    })
except Exception:
    pass


def _v171_kb_rows(kb):
    return list(getattr(kb, "keyboard", None) or getattr(kb, "inline_keyboard", None) or [])


def _v171_btn_text(btn) -> str:
    if isinstance(btn, dict):
        return str(btn.get("text") or "")
    return str(getattr(btn, "text", "") or "")


def _v171_btn_cb(btn) -> str:
    if isinstance(btn, dict):
        return str(btn.get("callback_data") or "")
    return str(getattr(btn, "callback_data", "") or "")


def _v171_set_kb_rows(kb, rows):
    try:
        kb.keyboard = rows
        return kb
    except Exception:
        pass
    try:
        kb.inline_keyboard = rows
    except Exception:
        pass
    return kb


def _v171_markup_inventory(kb):
    rows = _v171_kb_rows(kb)
    buttons = [btn for row in rows for btn in (row or [])]
    return rows, {_v171_btn_cb(b) for b in buttons if _v171_btn_cb(b)}, [(_v171_btn_text(b), _v171_btn_cb(b)) for b in buttons]


_V171_PREV_AUGMENT_MARKUP = globals().get("_v160_augment_markup")


def _v160_augment_markup(reply_markup, text: str):
    kb = _V171_PREV_AUGMENT_MARKUP(reply_markup, text) if callable(_V171_PREV_AUGMENT_MARKUP) else reply_markup
    marker = ""
    try:
        marker = str(_v160_marker_from_text(text) or "").upper()
    except Exception:
        marker = ""
    if not marker:
        return kb
    try:
        if not isinstance(kb, types.InlineKeyboardMarkup):
            kb = types.InlineKeyboardMarkup()
        # Description helper is already a complete helper window; do not recurse.
        if marker == "Ф241":
            return kb
        rows, callbacks, inventory = _v171_markup_inventory(kb)
        labels_cf = [str(label).strip().casefold() for label, _cb in inventory]
        has_back = any(("назад" in label and "осн" not in label) for label in labels_cf) or "nav_prev" in callbacks
        has_main = any(("назад" in label and ("осн" in label or "глав" in label)) for label in labels_cf) or any(str(cb).endswith(":back_main") for cb in callbacks)
        has_close = any("закры" in label for label in labels_cf) or bool({"info_close", "aux_close", "secclose", "secmclose"} & callbacks)
        has_desc = "v171:desc" in callbacks
        has_marker = "v160:marker_capture" in callbacks
        has_tz = "v160:tz_capture" in callbacks
        day = today_key()
        try:
            # Use the viewed chat's day so Back-to-main is deterministic.
            day = str(get_chat_store(int(getattr(_state_context, "chat_id", 0) or 0)).get("current_view_day") or today_key())
        except Exception:
            day = today_key()
        if not has_desc:
            rows.append([IB("ℹ️ Описание", callback_data="v171:desc")])
        if not has_back:
            rows.append([IB("🔙 Назад", callback_data="nav_prev")])
        if not has_main:
            rows.append([IB("⬅️ Назад осн. окно", callback_data=f"d:{day}:back_main")])
        if not has_close:
            rows.append([IB("❌ Закрыть", callback_data="info_close")])
        if not has_marker or not has_tz:
            add = []
            if not has_marker:
                add.append(IB("/iz-mr", callback_data="v160:marker_capture"))
            if not has_tz:
                add.append(IB("/tz", callback_data="v160:tz_capture"))
            if add:
                rows.append(add)
        return _v171_set_kb_rows(kb, rows)
    except Exception as exc:
        try:
            log_error(f"v171 augment markup {marker}: {exc}")
        except Exception:
            pass
        return kb


def _v171_button_help(label: str, cb: str) -> str:
    low = (str(label) + " " + str(cb)).casefold()
    rules = (
        (("журнал",), "открывает журнал или его настройки"),
        (("фин", "режим"), "управляет финансовым режимом"),
        (("фин.пересыл",), "выбирает оформление финансовой копии: обычно / кнопка / слеш"),
        (("перес",), "открывает или настраивает пересылку"),
        (("google", "чт"), "настраивает обновление Google-таблицы Чт–Ср"),
        (("excel",), "открывает настройки Excel/выгрузки"),
        (("напомин",), "открывает или меняет настройки напоминаний"),
        (("таймер",), "открывает внутренние таймеры окон и задач"),
        (("mega",), "работает с долговечным MEGA-хранилищем"),
        (("guard",), "управляет защитой восстановления/резервирования"),
        (("очеред",), "показывает состояние рабочих очередей"),
        (("render", "сервер"), "показывает состояние Render и runtime"),
        (("проблем",), "показывает задачи, требующие проверки"),
        (("целост",), "проверяет целостность финансовых данных"),
        (("владел",), "открывает управление владельцами/доступом"),
        (("назад осн",), "возвращает в основное окно"),
        (("назад",), "возвращает в предыдущее окно"),
        (("закры",), "закрывает текущее окно"),
        (("/iz-mr",), "позволяет изменить имя/маркер окна"),
        (("/tz",), "добавляет ТЗ именно к этому окну"),
    )
    for needles, text in rules:
        if all(n in low for n in needles):
            return text
    if str(cb) == "none":
        return "разделитель или информационная строка"
    return "выполняет действие этой кнопки в текущем меню"


def _v171_window_description(call) -> str:
    source_text = str(getattr(call.message, "text", None) or getattr(call.message, "caption", None) or "")
    try:
        marker = str(_v160_marker_from_text(source_text) or "без маркера").upper()
    except Exception:
        marker = "без маркера"
    name = ""
    try:
        catalog, _rows = _v160_annotation_roots()
        name = str((catalog.get(marker) or {}).get("name") or "")
    except Exception:
        name = ""
    rows = _v171_kb_rows(getattr(call.message, "reply_markup", None))
    seen = set()
    lines = [f"ℹ️ ОПИСАНИЕ ОКНА {marker}"]
    if name:
        lines.append(f"Название: {name}")
    lines += ["", "Назначение: управление функциями текущего меню.", "", "Кнопки:"]
    for row in rows:
        for btn in row or []:
            label = _v171_btn_text(btn).strip()
            cb = _v171_btn_cb(btn).strip()
            if not label or cb in {"v171:desc", "v171:desc_close"}:
                continue
            key = (label, cb)
            if key in seen:
                continue
            seen.add(key)
            line = f"• {label} — {_v171_button_help(label, cb)}"
            if len("\n".join(lines + [line])) > 3500:
                lines.append("• … остальные кнопки работают по их подписи и назначению меню.")
                break
            lines.append(line)
        if lines and lines[-1].startswith("• …"):
            break
    lines += ["", "Цепочка кнопки: нажатие → отклик → выполнение → результат."]
    try:
        return window_mark("\n".join(lines), "Ф241")
    except Exception:
        return "\n".join(lines) + "\n\nФ241"


def _v171_desc_keyboard(chat_id: int):
    day = today_key()
    try:
        day = str(get_chat_store(int(chat_id)).get("current_view_day") or today_key())
    except Exception:
        pass
    kb = types.InlineKeyboardMarkup()
    kb.row(IB("🔙 Назад", callback_data="v171:desc_close"), IB("⬅️ Назад осн. окно", callback_data=f"d:{day}:back_main"))
    kb.row(IB("❌ Закрыть", callback_data="v171:desc_close"))
    kb.row(IB("/iz-mr", callback_data="v160:marker_capture"), IB("/tz", callback_data="v160:tz_capture"))
    return kb


def _v171_special_callback(call):
    raw = str(getattr(call, "data", "") or "")
    try:
        resolver = globals().get("resolve_short_callback")
        if callable(resolver):
            raw = str(resolver(raw) or raw)
    except Exception:
        pass
    if raw == "none":
        try:
            bot.answer_callback_query(call.id)
        except Exception:
            pass
        return True
    if raw == "v171:desc":
        cid = int(call.message.chat.id)
        try:
            bot.answer_callback_query(call.id)
        except Exception:
            pass
        try:
            bot.send_message(cid, _v171_window_description(call), reply_markup=_v171_desc_keyboard(cid))
        except Exception as exc:
            try:
                bot.send_message(cid, f"❌ Не удалось открыть описание: {str(exc)[:180]}")
            except Exception:
                pass
        return True
    if raw == "v171:desc_close":
        try:
            bot.answer_callback_query(call.id)
        except Exception:
            pass
        _v171_delete_message_quiet(int(call.message.chat.id), int(call.message.message_id))
        return True
    return False


def _v171_register_special_callback() -> int:
    return 0  # v179: registration/wrapper retired; final router owns callbacks


# ---------------------------------------------------------------------------
# 8) F54 logical sections. Existing rows/buttons are never split or reshaped;
# only whole rows are grouped and visually separated by a blank-looking row.
# ---------------------------------------------------------------------------
_V171_PREV_BUILD_INFO_KEYBOARD = globals().get("build_info_keyboard")


def _v171_info_group(row) -> str:
    tokens = " ".join((_v171_btn_text(btn) + " " + _v171_btn_cb(btn)) for btn in (row or [])).casefold()
    if any(x in tokens for x in ("back_main", "info_close", "назад осн", "закрыть")):
        return "nav"
    if any(x in tokens for x in ("forward_copy", "forward_menu", "пересыл", "фин.пересыл", "icon_buttons")):
        return "forward"
    if "reminder" in tokens or "напомин" in tokens:
        return "reminder"
    if any(x in tokens for x in ("restore_guard", "mega_manual", "delta", "backup", "mega_priority")):
        return "storage"
    if any(x in tokens for x in ("journal", "problem_tasks", "integrity", "runtime_watcher", "info_queues", "internal_timers", "keepalive", "safety_profile", "buttons_current")):
        return "diag"
    if any(x in tokens for x in ("finance", "фин", "gomonk", "currency", "usd", "expense", "excel", "google", "article")):
        return "finance"
    if any(x in tokens for x in ("owners", "space", "владел")):
        return "access"
    if any(x in tokens for x in ("instruction", "инструк")):
        return "help"
    return "other"


def _v177_legacy_0221_build_info_keyboard(chat_id: int):
    kb = _V171_PREV_BUILD_INFO_KEYBOARD(int(chat_id)) if callable(_V171_PREV_BUILD_INFO_KEYBOARD) else types.InlineKeyboardMarkup()
    rows = _v171_kb_rows(kb)
    if not rows or not is_owner_chat(int(chat_id)):
        return kb
    order = ("diag", "storage", "finance", "forward", "reminder", "access", "help", "other", "nav")
    buckets = {k: [] for k in order}
    for row in rows:
        buckets.setdefault(_v171_info_group(row), []).append(row)
    out = []
    first = True
    for group in order:
        block = buckets.get(group) or []
        if not block:
            continue
        if not first and group != "nav":
            # Telegram does not allow a truly empty inline-keyboard row. U+3164 renders
            # as an empty visual separator while existing button rows remain unchanged.
            out.append([IB("ㅤ", callback_data="none")])
        out.extend(block)
        first = False
    return _v171_set_kb_rows(kb, out)
try: _v177_legacy_0221_build_info_keyboard.__name__ = 'build_info_keyboard'
except Exception: pass
build_info_keyboard = _v177_legacy_0221_build_info_keyboard


# ---------------------------------------------------------------------------
# 7/11) Contour/button reliability: journal the full button chain, and ACK
# successful v164 contour navigation before network-heavy menu rendering.
# ---------------------------------------------------------------------------
def _v171_contour_preack_allowed(call, raw: str) -> bool:
    if not str(raw).startswith("v164:"):
        return False
    try:
        cid = int(call.message.chat.id)
        uid = int(getattr(getattr(call, "from_user", None), "id", 0) or 0)
        if raw.startswith("v164:circle:"):
            return cid == int(OWNER_ID or 0) or bool(tenant_can_manage(uid, chat_id=_v164_circle_parent_root_for_context(cid) or cid))
        if raw.startswith("v164:space_circle:"):
            return cid == int(OWNER_ID or 0) or bool(tenant_can_manage(uid, chat_id=cid) or circle_level_for_chat(cid) == 2)
        return True
    except Exception:
        return False


def _v171_install_button_chain() -> int:
    return 0  # v179: registration/wrapper retired; final router owns callbacks


# ---------------------------------------------------------------------------
# Journal audit: serialize refresh attempts for the same Telegram window and
# re-check the registry after the first missing-message failure unregisters it.
# ---------------------------------------------------------------------------
def _v171_window_item_key(item: dict):
    try:
        return int(item.get("chat_id") or 0), int(item.get("message_id") or 0)
    except Exception:
        return 0, 0


def _v171_wrap_registered_refresh(name: str):
    previous = globals().get(name)
    if not callable(previous) or getattr(previous, "_v171_refresh_guard", False):
        return
    def guarded(item: dict, changed_chat_id: int, _previous=previous):
        cid, mid = _v171_window_item_key(item or {})
        if not cid or not mid:
            return False
        lock = window_locks[(cid, mid)]
        with lock:
            try:
                if not get_registered_open_window(cid, mid):
                    return False
            except Exception:
                pass
            return _previous(item, changed_chat_id)
    guarded._v171_refresh_guard = True
    guarded.__name__ = name
    globals()[name] = guarded


for _v171_refresh_name in ("_refresh_registered_fin_view", "_refresh_registered_local_fin_view", "_refresh_registered_fin_categories_view"):
    try:
        _v171_wrap_registered_refresh(_v171_refresh_name)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Journal audit: current ~690 KB deltas are below MEGA's practical limits but
# were rejected by our own 512 KB regression guard.  Keep 512 KB as a soft
# diagnostic threshold and hard-block only above 1 MiB, with contributor info.
# ---------------------------------------------------------------------------
try:
    V171_DELTA_SOFT_WARN_BYTES = max(128 * 1024, int(_v171_os.getenv("MEGA_DELTA_SOFT_WARN_BYTES", str(512 * 1024)) or 512 * 1024))
except Exception:
    V171_DELTA_SOFT_WARN_BYTES = 512 * 1024
try:
    V171_DELTA_HARD_MAX_BYTES = max(V171_DELTA_SOFT_WARN_BYTES + 1, int(_v171_os.getenv("MEGA_DELTA_HARD_MAX_BYTES", str(1024 * 1024)) or 1024 * 1024))
except Exception:
    V171_DELTA_HARD_MAX_BYTES = 1024 * 1024
_V171_DELTA_WARN_LOCK = _v171_threading.RLock()
_V171_DELTA_LAST_WARN = 0.0


def _v171_json_size(obj) -> int:
    try:
        return len(_v171_json.dumps(obj, ensure_ascii=False, separators=(",", ":"), default=str).encode("utf-8"))
    except Exception:
        return -1


def _v171_delta_breakdown(payload: dict) -> str:
    pieces = []
    for key in ("chat_changes", "root_patch", "root_map_patches", "root_deletes", "root_map_deletes"):
        size = _v171_json_size((payload or {}).get(key))
        pieces.append(f"{key}={size}")
    try:
        chats = payload.get("chat_changes") or {}
        top = sorted(((str(k), _v171_json_size(v)) for k, v in chats.items()), key=lambda x: x[1], reverse=True)[:3]
        if top:
            pieces.append("top_chats=" + ",".join(f"{k}:{s}" for k, s in top))
    except Exception:
        pass
    return " ".join(pieces)


def _delta_upload_payload(payload: dict) -> tuple[bool, str]:
    global _V171_DELTA_LAST_WARN
    if not payload or not mega_is_configured():
        return False, ""
    encoded_size = _v171_json_size(payload)
    if encoded_size > V171_DELTA_HARD_MAX_BYTES:
        try:
            log_error(
                f"[MEGA DELTA BLOCKED] oversized compact delta: {encoded_size} bytes > hard {V171_DELTA_HARD_MAX_BYTES}; "
                f"{_v171_delta_breakdown(payload)}; full snapshot scheduled"
            )
        except Exception:
            pass
        try:
            _mark_global_snapshot_pending()
        except Exception:
            pass
        return False, ""
    if encoded_size > V171_DELTA_SOFT_WARN_BYTES:
        now_m = _v171_time.monotonic()
        with _V171_DELTA_WARN_LOCK:
            should_log = now_m - float(_V171_DELTA_LAST_WARN or 0.0) >= 60.0
            if should_log:
                _V171_DELTA_LAST_WARN = now_m
        if should_log:
            try:
                bot_journal(
                    "mega_delta_large_allowed_v171", None,
                    f"bytes={encoded_size}; soft={V171_DELTA_SOFT_WARN_BYTES}; hard={V171_DELTA_HARD_MAX_BYTES}; {_v171_delta_breakdown(payload)}",
                    "WARN",
                )
            except Exception:
                pass
    day_dir = mega_delta_remote_day_dir(str(payload.get("created_at") or today_key())[:10])
    _v171_os.makedirs(MEGA_LOCAL_TMP_DIR, exist_ok=True)
    name = f"delta_{payload.get('delta_id')}.json"
    local_path = _v171_os.path.join(MEGA_LOCAL_TMP_DIR, name)
    try:
        _save_json(local_path, payload)
        mega_ensure_remote_path(day_dir)
        _mega_run("mega-put", [local_path, day_dir], check=True, timeout=MEGA_TIMEOUT)
        return True, day_dir.rstrip("/") + "/" + name
    except Exception as exc:
        try:
            log_error(f"[MEGA DELTA ERROR] {exc}")
        except Exception:
            pass
        return False, ""
    finally:
        try:
            if _v171_os.path.exists(local_path):
                _v171_os.remove(local_path)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# TZ rows: persist the global-default scope and make the export self-explanatory.
# Mark all eleven v169 items as fixed by v171 after restore.
# ---------------------------------------------------------------------------
_V171_PREV_SAVE_TZ = globals().get("_v160_save_tz")
_V171_PREV_TZ_EXPORT = globals().get("_v167_tz_export")


def _v160_save_tz(chat_id: int, user_id: int, marker: str, body: str, source: dict) -> dict:
    if not callable(_V171_PREV_SAVE_TZ):
        raise RuntimeError("TZ storage unavailable")
    row = _V171_PREV_SAVE_TZ(chat_id, user_id, marker, body, source)
    try:
        row["scope_policy"] = V171_TZ_SCOPE_POLICY
        row["scope_note"] = V171_TZ_SCOPE_TEXT
        _v160_persist_annotations(int(chat_id))
    except Exception:
        pass
    return row


def _v167_tz_export(kind: str):
    if not callable(_V171_PREV_TZ_EXPORT):
        return "ТЗ_окон", ""
    base, content = _V171_PREV_TZ_EXPORT(kind)
    if str(kind) in {"tz", "tz_archive"}:
        lines = str(content or "").splitlines()
        insert_at = 4 if len(lines) >= 4 else len(lines)
        lines[insert_at:insert_at] = [f"Правило области ТЗ: {V171_TZ_SCOPE_TEXT}", ""]
        content = "\n".join(lines).rstrip() + "\n"
    return base, content


_V171_TZ_FIX_SIGNATURES = (
    ("Ф233", ("таймер", "закрыв")),
    ("Ф91", ("когда даю тз", "всего бота")),
    ("Ф54", ("переименуй", "фин")),
    ("Ф54", ("проверь журнал", "ошиб")),
    ("Ф191", ("напоминал", "другие чаты")),
    ("Ф54", ("перес", "момент пересыл")),
    ("Ф54", ("контур", "все кнопки")),
    ("Ф54", ("отсортируй", "логич")),
    ("Ф91", ("каждом меню", "описание")),
    ("Ф91", ("новое окно", "назад")),
    ("Ф91", ("кнопки не ломались", "нажатие")),
)


def _v171_mark_all_v169_tz_fixed() -> int:
    changed = 0
    try:
        _catalog, rows = _v160_annotation_roots()
    except Exception:
        return 0
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        ver = str(row.get("version") or "")
        if not ver.startswith("bot_v169_"):
            continue
        marker = str(row.get("marker") or "").upper()
        body = str(row.get("text") or "").casefold()
        matched = False
        for want_marker, needles in _V171_TZ_FIX_SIGNATURES:
            if marker == want_marker and all(str(n).casefold() in body for n in needles):
                matched = True
                break
        if not matched:
            continue
        if str(row.get("status") or "").lower() != "fixed" or str(row.get("fixed_by_version") or "") != VERSION:
            row["status"] = "fixed"
            row["fixed_by_version"] = VERSION
            row["fixed_at"] = now_local().isoformat(timespec="seconds")
            row["scope_policy"] = V171_TZ_SCOPE_POLICY
            changed += 1
    if changed:
        try:
            _v160_persist_annotations(int(OWNER_ID or 0))
        except Exception:
            pass
    return changed


# ---------------------------------------------------------------------------
# v171 restore compatibility.
# ---------------------------------------------------------------------------
_V171_PREV_RESTORE_VALIDATOR = globals().get("_v153_validate_restore_gz")


def _v177_legacy_0288_v153_validate_restore_gz(gz_path: str):
    if callable(_V171_PREV_RESTORE_VALIDATOR):
        try:
            return _V171_PREV_RESTORE_VALIDATOR(gz_path)
        except Exception as exc:
            if "unsupported bot version" not in str(exc):
                raise
    folder = _v171_tempfile.mkdtemp(prefix="v171_restore_validate_")
    raw = _v171_os.path.join(folder, "restore.sqlite3")
    try:
        with _v171_gzip.open(gz_path, "rb") as fin, open(raw, "wb") as fout:
            _v171_shutil.copyfileobj(fin, fout, 1024 * 1024)
        conn = _v171_sqlite3.connect(raw)
        try:
            integrity = str(conn.execute("PRAGMA integrity_check").fetchone()[0])
            if integrity.lower() != "ok":
                raise RuntimeError(f"SQLite integrity_check: {integrity}")
            row = conn.execute("SELECT v FROM meta WHERE kind='v153_export' AND k='manifest'").fetchone()
            if not row:
                raise RuntimeError("manifest v153 not found")
            manifest = _v171_json.loads(row[0])
        finally:
            conn.close()
        if str(manifest.get("kind")) != "telegram_bot_full_state_v153":
            raise RuntimeError("unknown export kind")
        if int(manifest.get("schema_version") or 0) != int(V153_EXPORT_SCHEMA):
            raise RuntimeError("unsupported export schema")
        export_version = str(manifest.get("bot_version") or "")
        if not export_version.startswith(tuple(f"bot_v{i}_" for i in range(153, 172))):
            raise RuntimeError(f"unsupported bot version: {export_version or 'missing'}")
        if _v153_db_logical_checksum(raw) != str(manifest.get("checksum") or ""):
            raise RuntimeError("checksum mismatch")
        return manifest, raw
    except Exception:
        _v171_shutil.rmtree(folder, ignore_errors=True)
        raise
try: _v177_legacy_0288_v153_validate_restore_gz.__name__ = '_v153_validate_restore_gz'
except Exception: pass
_v153_validate_restore_gz = _v177_legacy_0288_v153_validate_restore_gz


# ---------------------------------------------------------------------------
# READY-time migration runs after MEGA restore, not just at module import.
# ---------------------------------------------------------------------------
_V171_PREV_RUNTIME_MARK_READY = globals().get("runtime_mark_ready")
if callable(_V171_PREV_RUNTIME_MARK_READY):
    def runtime_mark_ready(detail: str = ""):
        result = _V171_PREV_RUNTIME_MARK_READY(detail)
        try:
            fixed = _v171_mark_all_v169_tz_fixed()
            bot_journal("v171_tz_fixed", int(OWNER_ID or 0), f"fixed={fixed}; policy={V171_TZ_SCOPE_POLICY}")
        except Exception:
            pass
        try:
            _v171_reminder_global_mode(True)
            forward_copy_edit_mode(int(OWNER_ID or 0))
        except Exception:
            pass
        return result


# Install handlers/wrappers last so v164's late contour handler is covered too.
_V171_SPECIAL_HANDLER_COUNT = 0  # v179 final callback router
_V171_BUTTON_CHAIN_COUNT = 0  # v179 final callback router

try:
    bot_journal(
        "v171_installed", int(OWNER_ID or 0),
        f"all_v169_tz=11; special_handlers={_V171_SPECIAL_HANDLER_COUNT}; wrapped_callbacks={_V171_BUTTON_CHAIN_COUNT}; "
        f"delta_soft={V171_DELTA_SOFT_WARN_BYTES}; delta_hard={V171_DELTA_HARD_MAX_BYTES}",
    )
except Exception:
    pass
# v182_restore_unified
