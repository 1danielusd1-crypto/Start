# v186_restore_exact_fast
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

def _v179_dispatch_callback(call, raw: str, resolved: str):
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
# v186_restore_exact_fast
