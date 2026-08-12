# v196_protected_branches_final
"""v179 single callback middleware for owner, circle 1, circle 2 and all users with feature access."""

def _v179_resolve_callback(call):
    raw = str(getattr(call, "data", "") or "")
    try:
        fn = globals().get("resolve_short_callback")
        resolved = str(fn(raw) or raw) if callable(fn) else raw
    except Exception:
        resolved = raw
    return raw, resolved

def _v179_set_source_context(call, resolved):
    try:
        ctx = globals().get("_V161_SOURCE_CONTEXT")
        if ctx is None:
            return
        msg_text = str(getattr(call.message, "text", None) or getattr(call.message, "caption", None) or "")
        token_fn = globals().get("_v161_extract_token")
        ctx.token = token_fn(msg_text) if callable(token_fn) else ""
        ctx.callback = resolved
        ctx.chat_id = int(call.message.chat.id)
        ctx.message_id = int(call.message.message_id)
    except Exception:
        pass

def _v189_is_finance_window_callback(resolved: str) -> bool:
    """Callbacks that can originate from an old MAIN window. Auxiliary windows keep their own controls."""
    value = str(resolved or "")
    return value.startswith(("d:", "c:", "main_close:", "remaining_open:"))

def _v189_redirect_stale_finance_window(call, resolved: str) -> bool:
    """Close any old finance window and recreate the ONE latest main window at chat bottom."""
    if not _v189_is_finance_window_callback(resolved):
        return False
    try:
        chat_id = int(call.message.chat.id)
        message_id = int(call.message.message_id)
        fn = globals().get("get_primary_main_window")
        if not callable(fn):
            return False
        primary_mid, primary_day = fn(chat_id)
        if not primary_mid or int(primary_mid) == message_id:
            return False
        day_key = str(primary_day or globals().get("today_key", lambda: "")())[:10]
    except Exception:
        return False

    try:
        bot.answer_callback_query(call.id, "Старое окно закрыто → открываю последнее", show_alert=False)
    except Exception:
        pass

    def _redirect():
        try:
            deleter = globals().get("_v189_delete_stale_main_message")
            if callable(deleter): deleter(chat_id, message_id)
            else:
                try: bot.delete_message(chat_id, message_id)
                except Exception: pass
            # Send the canonical main window immediately. set_active_window_id() retires
            # the previous primary asynchronously, so we avoid a blocking delete-before-send.
            force_new = globals().get("force_new_day_window")
            if callable(force_new):
                force_new(chat_id, day_key)
            else:
                recreate = globals().get("recreate_main_window_now")
                if callable(recreate): recreate(chat_id, day_key)
            try: bot_journal("stale_main_redirect_v189", chat_id, f"old={message_id}; primary={primary_mid}; day={day_key}; action={resolved}", "INFO")
            except Exception: pass
        except Exception as exc:
            try: log_error(f"v189 stale main redirect {chat_id}/{message_id}: {exc}")
            except Exception: pass

    try:
        pool = globals().get("UI_TASK_POOL") or globals().get("GENERAL_TASK_POOL")
        if pool is not None:
            submit_unique = getattr(pool, "submit_unique", None)
            if callable(submit_unique):
                submit_unique(f"v189-stale-redirect:{chat_id}", _redirect)
            else:
                pool.submit(f"v189-stale-redirect:{chat_id}", _redirect)
        else:
            _redirect()
    except Exception:
        _redirect()
    return True


def _v179_dispatch_callback(call, raw: str, resolved: str):
    # v189: stale finance windows never execute business logic. They close and redirect
    # to a freshly recreated copy of the single authoritative main window.
    if _v189_redirect_stale_finance_window(call, resolved):
        return True
    # Exact callback-id dedupe only. No historical same-button delay suppression.
    fn = globals().get("_v160_exact_callback_duplicate")
    if callable(fn) and fn(call):
        try: bot.answer_callback_query(call.id, "Уже принято")
        except Exception: pass
        return True
    _v179_set_source_context(call, resolved)

    # Critical navigation/INFO works identically for owner and circles.
    fn = globals().get("_v161_critical_callback")
    if callable(fn) and fn(call, resolved): return True

    # Legacy process buttons remain readable, but no legacy callback handler is registered.
    fn = globals().get("_v157_handle_callback")
    if callable(fn) and fn(call): return True
    fn = globals().get("_v156_handle_process_toggle")
    if callable(fn) and fn(call): return True
    fn = globals().get("_v160_handle_special_callback")
    if callable(fn) and fn(call, resolved): return True

    fn = globals().get("_v176_filter")
    if callable(fn) and fn(call):
        globals()["_v176_callback"](call); return True

    fn = globals().get("_v196_branch_callback_filter")
    if callable(fn) and fn(call):
        globals()["_v196_branch_callback"](call); return True

    if resolved.startswith("rem:"):
        reminder_callback(call); return True
    fn = globals().get("_v163_exact_today_filter")
    if callable(fn) and fn(call): globals()["_v163_exact_today_callback"](call); return True
    fn = globals().get("_v164_circle_callback_filter")
    if callable(fn) and fn(call): globals()["_v164_circle_callback"](call); return True
    fn = globals().get("_v167_schedule_callback_filter")
    if callable(fn) and fn(call): globals()["_v167_schedule_callback"](call); return True
    if resolved.startswith("v171:") or resolved == "none":
        globals()["_v171_special_callback"](call); return True
    fn = globals().get("task_dispatcher_callback_final")
    if callable(fn) and fn(call): return True

    # Remaining business callbacks are handled by the core dispatcher.
    on_callback(call)
    return True

def final_callback_router(call):
    clock = globals().get("_v176_time") or globals().get("time")
    started = clock.monotonic()
    raw, resolved = _v179_resolve_callback(call)
    try: _V177_PERF_LOCAL.action = resolved[:120]
    except Exception: pass
    cid = None; seq_before = 0; err = ""
    try:
        cid = int(call.message.chat.id)
        seq_before = int(globals().get("_WINDOW_DIAG_SEQ", 0) or 0)
    except Exception: pass
    if resolved != "none":
        try:
            if v176_process_enabled("btn_chain"):
                bot_journal("button_chain_press", cid, f"action={resolved}")
        except Exception: pass
    try:
        return _v179_dispatch_callback(call, raw, resolved)
    except Exception as exc:
        err = f"{type(exc).__name__}: {exc}"
        try: log_error(f"FINAL_CALLBACK_ERROR action={resolved} chat={cid}: {exc}")
        except Exception: pass
        try: bot.answer_callback_query(call.id, "Ошибка выполнения кнопки. Записано в журнал.", show_alert=True)
        except Exception: pass
        return None
    finally:
        try:
            elapsed = max(0.0, clock.monotonic() - started)
            if raw != "v176:speed_clear":
                _V176_PERF.append({"ts": clock.time(), "action": resolved[:120], "elapsed": elapsed})
            if resolved != "none" and v176_process_enabled("btn_chain"):
                bot_journal("button_chain_result", cid, f"action={resolved}; ok={int(not bool(err))}; elapsed={elapsed:.3f}s")
        except Exception: pass
        try:
            audit = globals().get("_v155_record_button_outcome")
            if callable(audit) and globals().get("V155_BUTTON_AUDIT_ENABLED", False):
                audit(call, raw, resolved, started, seq_before, err)
        except Exception: pass
        try: _V177_PERF_LOCAL.action = ""
        except Exception: pass

# Exactly one Telegram callback handler in the package.
bot.callback_query_handler(func=lambda c: True)(final_callback_router)
_V179_FINAL_CALLBACK_HANDLERS = 1
# v196_protected_branches_final
