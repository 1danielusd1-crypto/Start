# v178_global_performance_final
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
    try:
        bot.callback_query_handler(func=_v163_exact_today_filter)(_v163_exact_today_callback)
        handlers = getattr(bot, "callback_query_handlers", None)
        if isinstance(handlers, list) and handlers:
            row = handlers.pop()
            handlers.insert(0, row)
        return 1
    except Exception:
        return 0


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
try:
    _V163_PREV_RESTORE_VALIDATE = globals().get("_v153_validate_restore_gz")
    if callable(_V163_PREV_RESTORE_VALIDATE):
        def _v153_validate_restore_gz(gz_path: str):
            try:
                return _V163_PREV_RESTORE_VALIDATE(gz_path)
            except Exception as exc:
                # Current v162 validator rejects future v163 by version prefix only.
                if "unsupported bot version" not in str(exc):
                    raise
                # Delegate to the v162 implementation by temporarily presenting the prefix.
                # A full validator override is intentionally avoided here; current exports are
                # still schema/checksum-validated by the existing v153+ restore code.
                folder = _v162_tempfile.mkdtemp(prefix="v163_restore_validate_")
                raw = _v162_os.path.join(folder, "restore.sqlite3")
                try:
                    with _v162_gzip.open(gz_path, "rb") as fin, open(raw, "wb") as fout:
                        _v162_shutil.copyfileobj(fin, fout, 1024 * 1024)
                    conn = _v162_sqlite3.connect(raw)
                    try:
                        integrity = str(conn.execute("PRAGMA integrity_check").fetchone()[0])
                        if integrity.lower() != "ok": raise RuntimeError(f"SQLite integrity_check: {integrity}")
                        row = conn.execute("SELECT v FROM meta WHERE kind='v153_export' AND k='manifest'").fetchone()
                        if not row: raise RuntimeError("manifest v153 not found")
                        manifest = _v162_json.loads(row[0])
                    finally:
                        conn.close()
                    if str(manifest.get("kind")) != "telegram_bot_full_state_v153": raise RuntimeError("unknown export kind")
                    if int(manifest.get("schema_version") or 0) != int(V153_EXPORT_SCHEMA): raise RuntimeError("unsupported export schema")
                    export_version = str(manifest.get("bot_version") or "")
                    if not export_version.startswith((
                        "bot_v153_", "bot_v154_", "bot_v155_", "bot_v156_", "bot_v157_", "bot_v158_",
                        "bot_v159_", "bot_v160_", "bot_v161_", "bot_v162_", "bot_v163_",
                    )):
                        raise RuntimeError(f"unsupported bot version: {export_version or 'missing'}")
                    if _v153_db_logical_checksum(raw) != str(manifest.get("checksum") or ""):
                        raise RuntimeError("checksum mismatch")
                    return manifest, raw
                except Exception:
                    _v162_shutil.rmtree(folder, ignore_errors=True)
                    raise
except Exception:
    pass

_V163_EXACT_TODAY_HANDLERS = _v163_install_exact_today_handler()

try:
    bot_journal(
        "v163_audit_hardening_installed",
        int(OWNER_ID or 0),
        "start_lane=priority; navigation_lane=per_window; ack=0.15; webhook_secret_path=1; "
        "F111_today=calendar; F113_today_shortcut=1; F52_F53_scope_union=1; F233_send_verified=1",
    )
except Exception:
    pass

# v178_global_performance_final
