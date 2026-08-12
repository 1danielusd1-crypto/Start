# v189_main_window_authority_final
# ---- integrated from 92_v147_diagnostic_hardening.py ----
# ─────────────────────────────────────────────────────────────
# v147: защита диагностических секретов, точные reminder-witness и безопасный back-main,
# один durable delta на финансовую партию, подробные failed-задачи,
# ограничение malloc_trim и расширенная диагностика напоминалок.
# ─────────────────────────────────────────────────────────────

_V146_WINDOW_LOCK = threading.RLock()
_V146_WINDOW_DIRTY_TYPES = {
    "main_day", "remaining", "categories", "fin_view", "local_fin_view",
    "fin_categories_view", "stored", "static_view",
}
_V146_WINDOW_REGISTRY_KEEP_DAYS = max(1, min(90, int(os.getenv("WINDOW_REGISTRY_KEEP_DAYS", "7") or "7")))
_V146_DURABLE_FORWARD_TIMEOUT = max(20.0, min(180.0, float(os.getenv("DURABLE_FORWARD_TIMEOUT_SECONDS", "45") or "45")))
_V146_FAILED_DIAG_LIMIT = max(1, min(20, int(os.getenv("FAILED_TASK_DIAG_LIMIT", "10") or "10")))
_V146_FAILED_CACHE_TTL = max(30.0, min(1800.0, float(os.getenv("FAILED_TASK_DIAG_TTL_SECONDS", "300") or "300")))
_V146_FAILED_TASK_DETAILS = []
_V146_FAILED_TASK_DETAILS_AT = 0.0
_V146_FAILED_TASK_RUNTIME_ERRORS = {}
_V146_FAILED_TASK_LOAD_LOCK = threading.Lock()
_V146_LAST_MALLOC_TRIM_MONO = 0.0
_V146_MALLOC_TRIM_COOLDOWN = max(30.0, min(600.0, float(os.getenv("MALLOC_TRIM_COOLDOWN_SECONDS", "180") or "180")))
_V146_MALLOC_TRIM_MIN_MB = max(128.0, min(2048.0, float(os.getenv("MALLOC_TRIM_MIN_MB", "220") or "220")))

# Полный backup объединяем за 5 минут; быстрый immutable delta остаётся немедленным.
try:
    BACKUP_MIN_DELAY_SECONDS = max(300.0, float(os.getenv("BACKUP_MIN_DELAY_SECONDS", "300") or "300"))
except Exception:
    BACKUP_MIN_DELAY_SECONDS = 300.0


def _v146_iso_now() -> str:
    try:
        return now_local().isoformat(timespec="milliseconds")
    except Exception:
        return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _v146_registry_key(chat_id: int, message_id: int) -> str:
    return f"{owner_scope_id(int(chat_id))}:{int(chat_id)}:{int(message_id)}"


def _v146_window_identity(row: dict | None) -> str:
    row = row or {}
    payload = {
        "window_type": str(row.get("window_type") or ""),
        "code": str(row.get("code") or ""),
        "day_key": str(row.get("day_key") or ""),
        "params": row.get("params") or {},
    }
    try:
        return hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")).hexdigest()[:20]
    except Exception:
        return str(payload)


def _v146_registry_rows_for_message(chat_id: int, message_id: int) -> list[tuple[str, dict]]:
    out = []
    for key, item in list((_open_window_registry() or {}).items()):
        try:
            if int((item or {}).get("chat_id") or 0) == int(chat_id) and int((item or {}).get("message_id") or 0) == int(message_id):
                out.append((str(key), item or {}))
        except Exception:
            continue
    return out


def _v179_base_get_registered_open_window(chat_id: int, message_id: int) -> dict | None:
    rows = _v146_registry_rows_for_message(int(chat_id), int(message_id))
    if not rows:
        return None
    rows.sort(key=lambda pair: (
        int((pair[1] or {}).get("epoch") or 0),
        str((pair[1] or {}).get("updated_at") or ""),
    ), reverse=True)
    return dict(rows[0][1])
get_registered_open_window = _v179_base_get_registered_open_window  # v179 compatibility alias; one implementation


def window_registry_epoch(chat_id: int, message_id: int) -> int:
    try:
        return int((get_registered_open_window(int(chat_id), int(message_id)) or {}).get("epoch") or 0)
    except Exception:
        return 0


def _v168_schedule_window_registry_persist():
    """Window registry is diagnostic/UI state; never block a callback on a full root SQLite save."""
    def _job():
        try: save_data(data, root_only=True)
        except Exception as exc:
            try: log_error(f"v168 window registry persist: {exc}")
            except Exception: pass
    try:
        scheduler = globals().get("V166_CONFIG_IO_SCHEDULER")
        if scheduler is not None:
            scheduler.cancel("window-registry-root-v168")
            scheduler.schedule("window-registry-root-v168", 0.20, _job)
            return
    except Exception:
        pass
    try:
        pool = globals().get("V166_CONFIG_IO_TASK_POOL")
        if pool is not None and pool.submit("window-registry-root-v168", _job):
            return
    except Exception:
        pass
    try: DELAYED_SCHEDULER.schedule("window-registry-root-v168", 0.20, _job)
    except Exception: pass


def _v179_base_register_open_window(chat_id: int, message_id: int, window_type: str, code: str = "", day_key: str | None = None, params: dict | None = None):
    chat_id = int(chat_id); message_id = int(message_id)
    params = dict(params or {})
    now_s = _v146_iso_now()
    with _V146_WINDOW_LOCK:
        reg = _open_window_registry()
        rows = _v146_registry_rows_for_message(chat_id, message_id)
        previous = None
        if rows:
            rows.sort(key=lambda pair: (int((pair[1] or {}).get("epoch") or 0), str((pair[1] or {}).get("updated_at") or "")), reverse=True)
            previous = dict(rows[0][1])
            for old_key, _old in rows:
                reg.pop(old_key, None)
        currency_chat_id = chat_id
        try:
            if params.get("target_chat_id") is not None:
                currency_chat_id = int(params.get("target_chat_id"))
        except Exception:
            currency_chat_id = chat_id
        new_row = {
            "owner_id": owner_scope_id(chat_id),
            "chat_id": chat_id,
            "message_id": message_id,
            "window_type": str(window_type or ""),
            "code": str(code or ""),
            "currency_mode": currency_mode(currency_chat_id) if "currency_mode" in globals() else "ars",
            "day_key": day_key,
            "params": params,
        }
        changed_identity = _v146_window_identity(previous) != _v146_window_identity(new_row)
        prev_epoch = int((previous or {}).get("epoch") or 0)
        epoch = max(1, prev_epoch + 1 if changed_identity else prev_epoch)
        new_row.update({
            "epoch": epoch,
            "dirty": False,
            "dirty_reason": "",
            "dirty_at": "",
            "registered_at": str((previous or {}).get("registered_at") or now_s),
            "updated_at": now_s,
            "last_interaction_at": now_s,
        })
        reg[_v146_registry_key(chat_id, message_id)] = new_row
    _v168_schedule_window_registry_persist()
    try:
        _window_diag_emit(
            "window_registry_epoch_changed" if changed_identity and previous else "window_registry_registered",
            chat_id, message_id,
            {
                "epoch_before": int((previous or {}).get("epoch") or 0),
                "epoch_after": epoch,
                "type_before": str((previous or {}).get("window_type") or ""),
                "type_after": str(window_type or ""),
                "code_before": str((previous or {}).get("code") or ""),
                "code_after": str(code or ""),
                "day_before": (previous or {}).get("day_key"),
                "day_after": day_key,
                "duplicates_removed": max(0, len(rows) - 1),
            },
            "WARN" if max(0, len(rows) - 1) else "INFO",
        )
    except Exception:
        pass
    return dict(new_row)
register_open_window = _v179_base_register_open_window  # v179 compatibility alias; one implementation


def unregister_open_window(chat_id: int, message_id: int):
    chat_id = int(chat_id); message_id = int(message_id)
    removed = []
    with _V146_WINDOW_LOCK:
        reg = _open_window_registry()
        for key, item in _v146_registry_rows_for_message(chat_id, message_id):
            removed.append(dict(item or {})); reg.pop(key, None)
        if removed:
            pass
    if removed:
        _v168_schedule_window_registry_persist()
    try:
        cancel_fast_ui_edit(chat_id, message_id)
    except Exception:
        pass
    if removed:
        try:
            _window_diag_emit("window_registry_unregistered", chat_id, message_id, {
                "count": len(removed),
                "epoch": max(int((x or {}).get("epoch") or 0) for x in removed),
            }, "INFO")
        except Exception:
            pass


def _v146_mark_window_dirty(item: dict, reason: str) -> bool:
    if not isinstance(item, dict):
        return False
    if bool(item.get("dirty")) and str(item.get("dirty_reason") or "") == str(reason or ""):
        return False
    item["dirty"] = True
    item["dirty_reason"] = str(reason or "finance_changed")[:180]
    item["dirty_at"] = _v146_iso_now()
    return True


def mark_registered_financial_windows_dirty(changed_chat_id: int, keep_message_ids: set[int] | None = None, reason: str = "finance_changed") -> int:
    changed_chat_id = int(changed_chat_id)
    keep = {int(x) for x in (keep_message_ids or set())}
    changed = 0
    with _V146_WINDOW_LOCK:
        for item in (_open_window_registry() or {}).values():
            if not isinstance(item, dict):
                continue
            try:
                mid = int(item.get("message_id") or 0)
                if mid in keep and int(item.get("chat_id") or 0) == changed_chat_id:
                    continue
                params = item.get("params") or {}
                target = int(params.get("target_chat_id") if params.get("target_chat_id") is not None else item.get("chat_id"))
                depends = target == changed_chat_id or bool(params.get("depends_on_all"))
                if not depends:
                    continue
                if str(item.get("window_type") or "") not in _V146_WINDOW_DIRTY_TYPES:
                    continue
                if _v146_mark_window_dirty(item, reason):
                    changed += 1
            except Exception:
                continue
        if changed:
            save_data(data, root_only=True)
    try:
        bot_journal("window_lazy_dirty_marked", changed_chat_id, f"count={changed} keep={sorted(keep)} reason={reason}")
    except Exception:
        pass
    return changed


def _v177_legacy_0245_cleanup_open_window_registry(reason: str = "manual") -> dict:
    now_dt = now_local()
    cutoff = now_dt - timedelta(days=_V146_WINDOW_REGISTRY_KEEP_DAYS)
    removed = 0; duplicates = 0; f91_removed = 0; normalized = 0
    with _V146_WINDOW_LOCK:
        reg = _open_window_registry()
        grouped = defaultdict(list)
        for key, item in list(reg.items()):
            try:
                grouped[(int(item.get("chat_id") or 0), int(item.get("message_id") or 0))].append((key, item))
            except Exception:
                reg.pop(key, None); removed += 1
        new_reg = {}
        for (chat_id, message_id), rows in grouped.items():
            rows.sort(key=lambda pair: (int((pair[1] or {}).get("epoch") or 0), str((pair[1] or {}).get("updated_at") or "")), reverse=True)
            key, item = rows[0]
            duplicates += max(0, len(rows) - 1)
            keep = True
            store = get_chat_store(chat_id)
            wtype = str((item or {}).get("window_type") or "")
            code = str((item or {}).get("code") or "")
            try:
                updated = datetime.fromisoformat(str((item or {}).get("updated_at") or ""))
                if updated.tzinfo is None:
                    updated = updated.replace(tzinfo=now_dt.tzinfo)
            except Exception:
                updated = now_dt
            if wtype == "remaining" or code.upper() == "Ф91":
                if int(store.get("remaining_msg_id") or 0) != message_id:
                    keep = False; f91_removed += 1
            elif wtype == "main_day":
                day = str((item or {}).get("day_key") or "")
                if int((get_or_create_active_windows(chat_id) or {}).get(day) or 0) != message_id:
                    keep = False
            elif wtype == "stored" and code:
                if int(store.get(code) or 0) != message_id:
                    keep = False
            elif updated < cutoff and wtype in {"static_view", "fin_view", "local_fin_view", "fin_categories_view", "categories"}:
                keep = False
            if keep:
                canonical = _v146_registry_key(chat_id, message_id)
                item["epoch"] = max(1, int(item.get("epoch") or 1))
                new_reg[canonical] = item
                if canonical != key:
                    normalized += 1
            else:
                removed += 1
                try:
                    cancel_fast_ui_edit(chat_id, message_id)
                except Exception:
                    pass
        if duplicates or removed or normalized or len(reg) != len(new_reg):
            reg.clear(); reg.update(new_reg)
            save_data(data, root_only=True)
    result = {
        "reason": reason,
        "kept": len(_open_window_registry() or {}),
        "removed": removed,
        "duplicates_removed": duplicates,
        "f91_removed": f91_removed,
        "keys_normalized": normalized,
    }
    try:
        bot_journal("window_registry_cleanup", None, json.dumps(result, ensure_ascii=False))
    except Exception:
        pass
    return result
try: _v177_legacy_0245_cleanup_open_window_registry.__name__ = 'cleanup_open_window_registry'
except Exception: pass
cleanup_open_window_registry = _v177_legacy_0245_cleanup_open_window_registry


def _v146_refresh_primary_windows(chat_id: int, day_key: str, reason: str = "finance_changed") -> dict:
    chat_id = int(chat_id); day_key = str(day_key or today_key())[:10]
    store = get_chat_store(chat_id)
    refreshed = []; missing = []
    keep = set()
    mid = get_active_window_id(chat_id, day_key)
    if mid:
        keep.add(int(mid))
        actual = get_registered_open_window(chat_id, int(mid))
        if not actual or str(actual.get("window_type") or "") in {"", "main_day"}:
            try:
                text, _ = render_day_window(chat_id, day_key)
                result = fast_ui_edit_message_text(chat_id, int(mid), text, reply_markup=build_main_keyboard(day_key, chat_id), purpose="finance_primary_window_refresh")
                refreshed.append({"message_id": int(mid), "kind": "main_day", "result": result})
            except Exception as exc:
                if _message_missing_error(exc):
                    clear_active_window_id(chat_id, day_key); unregister_open_window(chat_id, int(mid)); missing.append(int(mid))
                else:
                    log_error(f"v146 primary window refresh {chat_id}:{mid}: {exc}")
    rem_mid = int(store.get("remaining_msg_id") or 0)
    if rem_mid:
        keep.add(rem_mid)
        actual = get_registered_open_window(chat_id, rem_mid)
        if not actual or str(actual.get("window_type") or "") in {"", "remaining"}:
            try:
                result = fast_ui_edit_message_text(
                    chat_id, rem_mid, build_remaining_text(chat_id, day_key),
                    reply_markup=build_remaining_keyboard(chat_id, day_key), parse_mode="HTML",
                    purpose="finance_remaining_window_refresh",
                )
                refreshed.append({"message_id": rem_mid, "kind": "remaining", "result": result})
            except Exception as exc:
                if _message_missing_error(exc):
                    store["remaining_msg_id"] = None; unregister_open_window(chat_id, rem_mid); missing.append(rem_mid)
                else:
                    log_error(f"v146 remaining window refresh {chat_id}:{rem_mid}: {exc}")
    dirty = mark_registered_financial_windows_dirty(chat_id, keep, reason)
    result = {"chat_id": chat_id, "day": day_key, "refreshed": refreshed, "dirty": dirty, "missing": missing}
    try:
        bot_journal("finance_window_refresh_detached_done", chat_id, json.dumps(result, ensure_ascii=False, default=str)[:1600])
    except Exception:
        pass
    return result


def _v177_legacy_0246_schedule_financial_window_refresh(chat_id: int, day_key: str | None = None, reason: str = "finance_changed", delay: float = 0.15):
    chat_id = int(chat_id)
    day_key = str(day_key or get_chat_store(chat_id).get("current_view_day") or today_key())[:10]
    scheduler_key = f"v146-fin-window:{chat_id}"
    def _dispatch():
        if not UI_TASK_POOL.submit(scheduler_key, _v146_refresh_primary_windows, chat_id, day_key, reason):
            log_error(f"V146 WINDOW REFRESH QUEUE FULL: {chat_id}")
    DELAYED_SCHEDULER.cancel(scheduler_key)
    DELAYED_SCHEDULER.schedule(scheduler_key, max(0.05, float(delay)), _dispatch)
    try:
        bot_journal("finance_window_refresh_detached", chat_id, f"day={day_key} reason={reason} delay={delay}")
    except Exception:
        pass
def _v177_legacy_0045_refresh_registered_financial_windows(chat_id: int):
    """v146 compatibility: no mass Telegram edits; schedule only primary windows and mark the rest dirty."""
    schedule_financial_window_refresh(int(chat_id), reason="registry_refresh")
    return True
def _finance_changed_now(chat_id: int, day_key: str | None = None, reason: str = "change"):
    chat_id = int(chat_id)
    day_key = str(day_key or get_chat_store(chat_id).get("current_view_day") or today_key())[:10]
    finance_cache_invalidate(chat_id, f"finance_changed:{reason}")
    with locked_chat(chat_id):
        store = get_chat_store(chat_id)
        # v189: business finalization must NEVER move the user's selected/main window day.
        # day_key is the mutation day only; canonical UI day is owned by the last main window.
        _safe_stabilize("normalize_chat_records", lambda: normalize_chat_records(chat_id))
        _safe_stabilize("recalc_balance", lambda: recalc_balance(chat_id))
        _safe_stabilize("rebuild_month_short_ids", lambda: rebuild_month_short_ids(chat_id))
        _safe_stabilize("rebuild_global_records", rebuild_global_records)
        _safe_stabilize("currency_ledger_snapshot", lambda: _snapshot_active_currency_ledger(store, _ensure_currency_ledgers(store)))
        _safe_stabilize("save_data", lambda: save_data(data, chat_ids=[chat_id]))
    # Critical protection first; UI is explicitly detached and cannot delay durable completion.
    _safe_stabilize("delta_queue_early", lambda: schedule_quick_backup(chat_id, MEGA_DELTA_PRIORITY_DELAY_SECONDS if mega_backup_priority_enabled() else MEGA_DELTA_DELAY_SECONDS))
    schedule_financial_window_refresh(chat_id, day_key, reason=reason, delay=0.12)
    _safe_stabilize("full_backup_queue", lambda: schedule_full_backup_only(chat_id, BACKUP_MIN_DELAY_SECONDS))
    try:
        bot_journal("finance_business_complete", chat_id, f"day={day_key} reason={reason}; UI detached=1")
    except Exception:
        pass
    return True


# ── Fast UI: reject a delayed edit when another navigation already changed epoch. ──
_V146_ORIG_WINDOW_DIAG_PREPARE = globals().get("window_diag_prepare_fast_ui_payload")
_V146_ORIG_WINDOW_DIAG_APPLY = globals().get("window_diag_fast_ui_apply")


def window_diag_prepare_fast_ui_payload(payload: dict) -> dict:
    if callable(_V146_ORIG_WINDOW_DIAG_PREPARE):
        payload = _V146_ORIG_WINDOW_DIAG_PREPARE(payload) or payload
    try:
        payload["_window_registry_epoch"] = window_registry_epoch(int(payload.get("chat_id")), int(payload.get("message_id")))
    except Exception:
        payload["_window_registry_epoch"] = 0
    return payload


def _v146_fast_ui_payload_current(payload: dict) -> tuple[bool, dict]:
    chat_id = int(payload.get("chat_id")); message_id = int(payload.get("message_id"))
    expected = int(payload.get("_window_registry_epoch") or 0)
    current = window_registry_epoch(chat_id, message_id)
    # Epoch 0 means the message was not registered yet; preserve legacy behavior.
    ok = expected == 0 or current == expected
    return ok, {"expected_epoch": expected, "current_epoch": current, "purpose": str(payload.get("purpose") or "")[:160]}


def window_diag_fast_ui_apply(payload: dict, delayed: bool = False):
    if callable(_V146_ORIG_WINDOW_DIAG_APPLY):
        try:
            _V146_ORIG_WINDOW_DIAG_APPLY(payload, delayed=delayed)
        except Exception:
            pass
    ok, detail = _v146_fast_ui_payload_current(payload)
    if delayed and not ok:
        try:
            _window_diag_emit("window_stale_update_rejected", payload.get("chat_id"), payload.get("message_id"), detail, "WARN")
        except Exception:
            pass
    return bool(ok or not delayed)


def _run_pending_ui_edit(key):
    with _ui_edit_lock:
        payload = _ui_edit_pending.pop(key, None)
        _ui_edit_timers.pop(key, None)
        if not payload:
            return
        _ui_edit_last_ts[key] = time.time()
    allowed = True
    try:
        allowed = bool(window_diag_fast_ui_apply(payload, delayed=True))
    except Exception:
        allowed = True
    if not allowed:
        try:
            bot_journal("window_stale_update_rejected", payload.get("chat_id"), f"message_id={payload.get('message_id')} purpose={payload.get('purpose')}", "WARN")
        except Exception:
            pass
        return
    _perform_fast_ui_edit(payload)


# ── Financial forwarding: one immutable delta after the whole fan-out batch. ──
_V146_ORIG_PERSIST_FORWARD_FINANCE = globals().get("_persist_forward_finance_delivery_now")


def _persist_forward_finance_delivery_now(src_chat_id: int, src_msg_id: int, dst_chat_id: int, dst_msg_id: int, rec: dict | None = None):
    batch_id = _fin_forward_batch_id(int(src_chat_id), int(src_msg_id)) if "_fin_forward_batch_id" in globals() else ""
    with _FIN_FORWARD_BATCH_LOCK:
        in_batch = batch_id in _FIN_FORWARD_BATCHES
    if not in_batch or not callable(_V146_ORIG_PERSIST_FORWARD_FINANCE):
        return _V146_ORIG_PERSIST_FORWARD_FINANCE(src_chat_id, src_msg_id, dst_chat_id, dst_msg_id, rec) if callable(_V146_ORIG_PERSIST_FORWARD_FINANCE) else False
    try:
        if isinstance(rec, dict):
            tags = {
                "forwarded_by_bot": True,
                "forward_source_chat_id": int(src_chat_id),
                "forward_source_msg_id": int(src_msg_id),
                "forward_dst_chat_id": int(dst_chat_id),
                "forward_dst_msg_id": int(dst_msg_id),
            }
            rec.update(tags)
            store = get_chat_store(int(dst_chat_id)); rid = rec.get("id")
            for arr in (store.get("daily_records", {}) or {}).values():
                for rr in arr or []:
                    if isinstance(rr, dict) and rr.get("id") == rid:
                        rr.update(tags)
        _persist_forward_index_in_data(data)
        save_data(data, chat_ids=[int(dst_chat_id)])
        schedule_quick_backup(int(dst_chat_id), 0.5)
        with _FIN_FORWARD_BATCH_LOCK:
            row = _FIN_FORWARD_BATCHES.get(batch_id)
            if isinstance(row, dict):
                row.setdefault("durable_chats", set()).add(int(dst_chat_id))
        bot_journal("forward_finance_local_committed", int(dst_chat_id), f"batch={batch_id} src={src_chat_id}:{src_msg_id} dst_msg={dst_msg_id}; combined_delta=1")
        return True
    except Exception as exc:
        log_error(f"[V146 FWD FINANCE LOCAL ERROR] {src_chat_id}:{src_msg_id}->{dst_chat_id}:{dst_msg_id}: {exc}")
        return False


def _v146_finish_forward_follow(follow: dict, durable_ok: bool, durable_error: str = ""):
    source_chat_id = int(follow.get("source_chat_id")); source_msg_id = int(follow.get("source_msg_id"))
    normal_targets = list(follow.get("normal_targets") or [])
    if durable_ok:
        if normal_targets:
            if not FORWARD_TASK_POOL.submit(source_chat_id, _forward_normal_stage, source_chat_id, follow.get("msg"), normal_targets):
                _forward_normal_stage(source_chat_id, follow.get("msg"), normal_targets)
        elif source_msg_id:
            _forward_outcome_update(source_chat_id, source_msg_id, state="completed")
    else:
        _forward_outcome_update(source_chat_id, source_msg_id, state="durable_pending", error=str(durable_error or "delta upload failed")[:300])
        def _retry():
            ok = False
            try:
                ok = bool(_run_delta_batch())
            except Exception as exc:
                log_error(f"V146 FIN BATCH DELTA RETRY {source_chat_id}:{source_msg_id}: {exc}")
            if ok:
                _v146_finish_forward_follow(follow, True)
            else:
                DELAYED_SCHEDULER.schedule(f"v146-fin-batch-delta:{source_chat_id}:{source_msg_id}", 5.0, _retry)
        DELAYED_SCHEDULER.schedule(f"v146-fin-batch-delta:{source_chat_id}:{source_msg_id}", 5.0, _retry)


def _fin_forward_batch_finish_target(batch_id: str, dst_chat_id: int, ok: bool, elapsed: float, error: str = "") -> None:
    follow = None
    with _FIN_FORWARD_BATCH_LOCK:
        row = _FIN_FORWARD_BATCHES.get(str(batch_id))
        if not isinstance(row, dict):
            return
        row.setdefault("targets", {})[str(int(dst_chat_id))] = {"ok": bool(ok), "elapsed": round(float(elapsed), 3), "error": str(error or "")[:300]}
        row["remaining"] = max(0, int(row.get("remaining", 0)) - 1)
        if row["remaining"] == 0:
            follow = dict(row)
            if isinstance(row.get("durable_chats"), set):
                follow["durable_chats"] = sorted(row.get("durable_chats"))
            _FIN_FORWARD_BATCHES.pop(str(batch_id), None)
    try:
        bot_journal("finance_forward_target_done", (follow or {}).get("source_chat_id"), f"batch={batch_id} dst={int(dst_chat_id)} ok={bool(ok)} elapsed={elapsed:.3f}s error={str(error or '')[:180]}", "INFO" if ok else "ERROR")
    except Exception:
        pass
    if not follow:
        return
    started = float(follow.get("started_mono") or time.monotonic())
    durable_started = time.monotonic(); durable_ok = False; durable_error = ""
    try:
        durable_ok = bool(_run_delta_batch())
        if not durable_ok:
            durable_error = "combined delta returned false"
    except Exception as exc:
        durable_error = str(exc)
        log_error(f"V146 FIN BATCH COMBINED DELTA {batch_id}: {exc}")
    _v146_finish_forward_follow(follow, durable_ok, durable_error)
    try:
        target_rows = follow.get("targets") or {}; failed = sum(1 for x in target_rows.values() if not bool((x or {}).get("ok")))
        bot_journal("finance_forward_batch_done", int(follow.get("source_chat_id")), f"batch={batch_id} targets={len(target_rows)} failed={failed} durable={int(durable_ok)} delta_elapsed={time.monotonic()-durable_started:.3f}s total_elapsed={time.monotonic()-started:.3f}s")
    except Exception:
        pass


# ── Durable exact wait: realistic deadline and richer diagnostics. ──
def wait_durable_subtasks(chat_id, timeout: float = 20.0, wait_forward: bool = True, payload: dict | None = None, expected: dict | None = None, update_id=None) -> bool:
    if chat_id is None or not wait_forward or not isinstance(payload, dict):
        return True
    raw, source_chat_id, source_msg_id, group_id = _durable_payload_message(payload)
    if not isinstance(raw, dict) or source_chat_id is None or source_msg_id is None:
        return True
    effective_timeout = max(float(timeout or 0), _V146_DURABLE_FORWARD_TIMEOUT)
    deadline = time.monotonic() + effective_timeout
    started = time.monotonic(); last_state = ""; last_targets = {}
    bot_journal("durable_forward_wait_start", chat_id, f"update={update_id} source={source_chat_id}:{source_msg_id} group={group_id or '-'} timeout={effective_timeout:g}")
    while True:
        outcome = _forward_outcome_snapshot(int(source_chat_id), int(source_msg_id))
        last_state = str(outcome.get("state") or ""); last_targets = outcome.get("targets") or {}
        try:
            report = _durable_effect_report(payload, expected if isinstance(expected, dict) else None)
        except Exception as _report_exc:
            report = {"complete": False, "missing": [f"report_error:{str(_report_exc)[:120]}"], "ambiguous": []}
        if bool(report.get("complete")):
            bot_journal("durable_forward_wait_done", chat_id, f"update={update_id} source={source_chat_id}:{source_msg_id} state={last_state or '-'} elapsed={time.monotonic()-started:.3f}s complete=1")
            return True
        pending = _durable_forward_work_still_pending(payload)
        final_state = last_state.startswith("skip:") or last_state in {"completed", "failed", "no_targets"}
        if final_state and not pending:
            bot_journal("durable_forward_wait_done", chat_id, f"update={update_id} source={source_chat_id}:{source_msg_id} state={last_state} elapsed={time.monotonic()-started:.3f}s complete=0 verify_next=1")
            return True
        if time.monotonic() >= deadline:
            qf = FIN_FORWARD_TASK_POOL.stats(); qn = FORWARD_TASK_POOL.stats()
            target_state = {str(k): str((v or {}).get("state") or "") for k, v in list(last_targets.items())[:20]}
            log_error(f"DURABLE EXACT FORWARD TIMEOUT update={update_id} source={source_chat_id}:{source_msg_id} timeout={effective_timeout:g} state={last_state or '-'} targets={target_state} missing={report.get('missing')} ambiguous={report.get('ambiguous')} finQ={qf.get('pending')}/{qf.get('active')} fwdQ={qn.get('pending')}/{qn.get('active')}")
            return False
        time.sleep(0.05)


# ── Failed durable task details. ──
_V146_ORIG_MEGA_TASK_FINISH = globals().get("mega_task_finish")
_V146_ORIG_MEGA_TASK_STATS = globals().get("mega_task_registry_stats")


def mega_task_finish(update_id, success: bool, error: str = "") -> bool:
    result = _V146_ORIG_MEGA_TASK_FINISH(update_id, success, error) if callable(_V146_ORIG_MEGA_TASK_FINISH) else False
    if not success:
        key = _mega_task_id(update_id)
        _V146_FAILED_TASK_RUNTIME_ERRORS[key] = {"error": str(error or "")[:500], "at": _v146_iso_now()}
        try:
            GENERAL_TASK_POOL.submit_unique("v146-failed-task-details", refresh_failed_task_diagnostics, True)
        except Exception:
            pass
    return result


def _v146_load_failed_task_detail(key: str, row: dict) -> dict:
    local = None
    try:
        remote = str((row or {}).get("path") or mega_task_remote_path(key, "failed"))
        local = _mega_download_remote_path(remote)
        if not local:
            raise RuntimeError("failed task file download returned empty")
        with open(local, "r", encoding="utf-8") as fh:
            task = json.load(fh)
        payload = task.get("payload") or {}
        expected = _durable_expected_from_task_or_payload(task, payload)
        report = _durable_effect_report(payload, expected)
        raw, source_chat_id, source_msg_id, group_id = _durable_payload_message(payload)
        runtime_error = _V146_FAILED_TASK_RUNTIME_ERRORS.get(str(key)) or {}
        return {
            "task_id": str(key),
            "update_id": task.get("update_id"),
            "created_at": task.get("created_at"),
            "chat_id": task.get("chat_id"),
            "update_type": task.get("update_type"),
            "reason": task.get("reason"),
            "source_chat_id": source_chat_id,
            "source_message_id": source_msg_id,
            "media_group_id": group_id,
            "content_type": task.get("content_type"),
            "forward_targets": task.get("forward_targets") or [],
            "missing": report.get("missing") or [],
            "ambiguous": report.get("ambiguous") or [],
            "complete_now": bool(report.get("complete")),
            "runtime_error": runtime_error.get("error") or "",
            "safe_replay": False,
            "remote_path": remote,
        }
    except Exception as exc:
        return {"task_id": str(key), "load_error": str(exc)[:500], "remote_path": str((row or {}).get("path") or "")}
    finally:
        try:
            if local:
                shutil.rmtree(os.path.dirname(local), ignore_errors=True)
        except Exception:
            pass


def refresh_failed_task_diagnostics(force: bool = False) -> list[dict]:
    global _V146_FAILED_TASK_DETAILS, _V146_FAILED_TASK_DETAILS_AT
    if not force and time.monotonic() - _V146_FAILED_TASK_DETAILS_AT < _V146_FAILED_CACHE_TTL:
        return list(_V146_FAILED_TASK_DETAILS)
    if not _V146_FAILED_TASK_LOAD_LOCK.acquire(blocking=False):
        return list(_V146_FAILED_TASK_DETAILS)
    try:
        with _MEGA_TASK_LOCK:
            rows = [(str(k), dict(v or {})) for k, v in _mega_task_registry.items() if str((v or {}).get("state") or "") == "failed"][:_V146_FAILED_DIAG_LIMIT]
        details = []
        reconciled = []
        for key, row in rows:
            detail = _v146_load_failed_task_detail(key, row)
            if bool((detail or {}).get("complete_now")) and callable(_V146_ORIG_MEGA_TASK_FINISH):
                try:
                    if _V146_ORIG_MEGA_TASK_FINISH(key, True, "v147 verified existing effects"):
                        reconciled.append(str(key))
                        continue
                except Exception:
                    pass
            details.append(detail)
        _V146_FAILED_TASK_DETAILS = details
        _V146_FAILED_TASK_DETAILS_AT = time.monotonic()
        try:
            bot_journal(
                "failed_task_diagnostics_refreshed", None,
                json.dumps({"count": len(details), "reconciled": reconciled, "tasks": details}, ensure_ascii=False, default=str)[:1800],
                "WARN" if details else "INFO",
            )
        except Exception:
            pass
        return list(details)
    finally:
        _V146_FAILED_TASK_LOAD_LOCK.release()


def _v177_legacy_0065_mega_task_registry_stats() -> dict:
    base = _V146_ORIG_MEGA_TASK_STATS() if callable(_V146_ORIG_MEGA_TASK_STATS) else {}
    try:
        with _MEGA_TASK_LOCK:
            current_failed = [
                str(key) for key, row in _mega_task_registry.items()
                if str((row or {}).get("state") or "") == "failed"
            ][:_V146_FAILED_DIAG_LIMIT]
    except Exception:
        current_failed = []
    current_set = set(current_failed)
    details = [
        row for row in list(_V146_FAILED_TASK_DETAILS)
        if str((row or {}).get("task_id") or "") in current_set
    ]
    detail_set = {str((row or {}).get("task_id") or "") for row in details}
    mismatch = detail_set != current_set
    if mismatch:
        try:
            GENERAL_TASK_POOL.submit_unique("v147-failed-task-details", refresh_failed_task_diagnostics, True)
        except Exception:
            pass
    base["failed_details"] = details
    base["failed_details_at"] = _V146_FAILED_TASK_DETAILS_AT
    base["failed_details_pending"] = bool(current_set and mismatch)
    return base
try: _v177_legacy_0065_mega_task_registry_stats.__name__ = 'mega_task_registry_stats'
except Exception: pass
mega_task_registry_stats = _v177_legacy_0065_mega_task_registry_stats


# ── malloc_trim: no more four calls per minute while RAM is already low. ──
def memory_malloc_trim() -> bool:
    global _V146_LAST_MALLOC_TRIM_MONO
    now_m = time.monotonic()
    if now_m - _V146_LAST_MALLOC_TRIM_MONO < _V146_MALLOC_TRIM_COOLDOWN:
        return False
    try:
        snap = memory_quick_snapshot()
        used = float(snap.get("effective_mb") or 0.0)
        if used < _V146_MALLOC_TRIM_MIN_MB and memory_level(snap) == "normal":
            return False
    except Exception:
        used = 0.0
    try:
        libc = _memory_ctypes.CDLL("libc.so.6")
        result = int(libc.malloc_trim(0))
        _V146_LAST_MALLOC_TRIM_MONO = now_m
        with _MEMORY_LOCK:
            _MEMORY_STATE["malloc_trim_count"] = int(_MEMORY_STATE.get("malloc_trim_count") or 0) + 1
            _MEMORY_STATE["last_malloc_trim_at"] = _v146_iso_now()
            _MEMORY_STATE["last_malloc_trim_used_mb"] = used
        return result == 1
    except Exception:
        return False


# ── Reminder lifecycle diagnostics. ──
_V146_ORIG_REM_DELETE_MAP = globals().get("_reminder_delete_message_map")
_V146_ORIG_REM_COMPLETE = globals().get("_reminder_mark_completed")
_V146_ORIG_REM_GROUP_DELETE = globals().get("_reminder_group_delete_message")


def _reminder_delete_message_map(last_map: dict) -> None:
    for cid_raw, mid_raw in list((last_map or {}).items()):
        try:
            bot.delete_message(int(cid_raw), int(mid_raw))
            bot_journal("reminder_previous_deleted", int(cid_raw), f"message_id={int(mid_raw)}")
        except Exception as exc:
            bot_journal("reminder_previous_delete_failed", int(cid_raw), f"message_id={mid_raw} error={str(exc)[:300]}", "WARN")


def _reminder_mark_completed(reminder_id: int, cfg: dict, reason: str = "time_finished", delete_messages: bool = True) -> None:
    was_completed = _reminder_is_completed(cfg)
    if callable(_V146_ORIG_REM_COMPLETE):
        _V146_ORIG_REM_COMPLETE(reminder_id, cfg, reason, delete_messages)
    if not was_completed and _reminder_is_completed(cfg):
        try:
            bot_journal("reminder_completed", None, f"reminder_id={int(reminder_id)} reason={reason} delete_messages={int(bool(delete_messages))}")
            bot_journal("reminder_archived", None, f"reminder_id={int(reminder_id)} completed_at={cfg.get('completed_at')}")
        except Exception:
            pass


def _reminder_group_delete_message(chat_id: int, message_id: int):
    try:
        result = _V146_ORIG_REM_GROUP_DELETE(chat_id, message_id) if callable(_V146_ORIG_REM_GROUP_DELETE) else bot.delete_message(int(chat_id), int(message_id))
        bot_journal("reminder_group_previous_deleted", int(chat_id), f"message_id={int(message_id)}")
        return result
    except Exception as exc:
        bot_journal("reminder_group_previous_delete_failed", int(chat_id), f"message_id={message_id} error={str(exc)[:300]}", "WARN")
        return None


# ── READY hook: registry cleanup and failed-task details after restore. ──
_V146_ORIG_RUNTIME_MARK_READY = globals().get("runtime_mark_ready")


def _v177_legacy_0083_runtime_mark_ready(detail: str = ""):
    result = _V146_ORIG_RUNTIME_MARK_READY(detail) if callable(_V146_ORIG_RUNTIME_MARK_READY) else None
    try:
        DELAYED_SCHEDULER.schedule("v146-window-registry-cleanup", 2.0, cleanup_open_window_registry, "startup")
    except Exception:
        pass
    try:
        def _v178_start_failed_diagnostics_if_enabled():
            enabled_fn = globals().get("v176_process_enabled")
            if callable(enabled_fn) and not bool(enabled_fn("failed_diag")):
                return []
            fn = globals().get("refresh_failed_task_diagnostics")
            return fn(True) if callable(fn) else []
        DELAYED_SCHEDULER.schedule("v146-failed-task-diagnostics", 8.0, _v178_start_failed_diagnostics_if_enabled)
    except Exception:
        pass
    return result
try: _v177_legacy_0083_runtime_mark_ready.__name__ = 'runtime_mark_ready'
except Exception: pass
runtime_mark_ready = _v177_legacy_0083_runtime_mark_ready


# Marker aliases for callbacks whose full payload has many dynamic segments.
try:
    WINDOW_MARKER_CONSTANTS.setdefault("exp_style_period", "Ф179")
    WINDOW_MARKER_CONSTANTS.setdefault("exp_new_period_send", "Ф180")
except Exception:
    pass

# ---- integrated from 93_v148_multitenant_spaces.py ----
# ─────────────────────────────────────────────────────────────
# v148: независимые пространства (tenant isolation)
# ─────────────────────────────────────────────────────────────
# Код бота общий, но каждый Telegram-чат принадлежит ровно одному пространству.
# Пространство изолирует: список чатов, пользователей/ролей, owner-scoped настройки,
# напоминалки, меню пересылки и допустимые связи. Платформенный владелец видит реестр
# всех пространств, но бизнес-связи между пространствами блокируются.

TENANT_SCHEMA_VERSION = 1
TENANT_PLATFORM_ID = "platform"
TENANT_ROLE_ORDER = ("tenant_owner", "tenant_admin", "operator", "viewer")
TENANT_ROLE_LABELS = {
    "platform_owner": "Владелец платформы",
    "tenant_owner": "Владелец пространства",
    "tenant_admin": "Администратор",
    "operator": "Оператор",
    "viewer": "Только просмотр",
    "standard": "Участник чата",
}
_TENANT_LOCK = threading.RLock()
_TENANT_CONTEXT = threading.local()
_TENANT_BOT_USERNAME = ""


def _tenant_now() -> str:
    return now_local().isoformat(timespec="seconds")


def _tenant_platform_owner_user_id() -> int:
    try:
        return int(OWNER_ID or 0)
    except Exception:
        return 0


def tenant_current_actor_user_id() -> int:
    try:
        ctx = _current_telegram_update_context()
        return int(ctx.get("user_id") or 0)
    except Exception:
        return 0


def tenant_is_platform_owner_user(user_id: int | None) -> bool:
    try:
        return bool(int(user_id or 0) and int(user_id or 0) == _tenant_platform_owner_user_id())
    except Exception:
        return False


def tenant_is_platform_owner_context(chat_id: int | None = None) -> bool:
    uid = tenant_current_actor_user_id()
    if uid:
        return tenant_is_platform_owner_user(uid)
    try:
        cid = int(chat_id if chat_id is not None else (current_state_chat_id() or 0))
    except Exception:
        cid = 0
    return bool(cid and cid == _tenant_platform_owner_user_id())


@contextmanager
def tenant_context(tenant_id: str | None):
    prev = getattr(_TENANT_CONTEXT, "tenant_id", None)
    try:
        _TENANT_CONTEXT.tenant_id = str(tenant_id or "") or None
        yield
    finally:
        _TENANT_CONTEXT.tenant_id = prev


def _tenants_root() -> dict:
    gs = data.setdefault("_global_settings", {})
    with _TENANT_LOCK:
        root = gs.get("tenants_v148")
        if not isinstance(root, dict):
            root = {}
            gs["tenants_v148"] = root
        root.setdefault("schema_version", TENANT_SCHEMA_VERSION)
        root.setdefault("tenants", {})
        root.setdefault("chat_to_tenant", {})
        root.setdefault("invite_tokens", {})
        root.setdefault("legacy_migrated", False)
        root.setdefault("created_at", _tenant_now())
        return root


def _tenant_default_name(chat_id: int | None = None) -> str:
    if chat_id:
        try:
            return str(get_chat_display_name(int(chat_id)) or f"Чат {int(chat_id)}")[:80]
        except Exception:
            pass
    return "Новое пространство"


def _tenant_normalize(tenant_id: str, row: dict | None = None) -> dict:
    row = row if isinstance(row, dict) else {}
    row["id"] = str(tenant_id)
    row.setdefault("name", "Пространство")
    row.setdefault("owner_user_id", 0)
    row.setdefault("root_chat_id", 0)
    row.setdefault("chat_ids", [])
    row.setdefault("users", {})
    row.setdefault("settings", {})
    row.setdefault("status", "active")
    row.setdefault("created_at", _tenant_now())
    row.setdefault("updated_at", _tenant_now())
    clean_chats = []
    for raw in row.get("chat_ids") or []:
        try:
            cid = int(raw)
        except Exception:
            continue
        if cid not in clean_chats:
            clean_chats.append(cid)
    row["chat_ids"] = clean_chats
    users = row.get("users") if isinstance(row.get("users"), dict) else {}
    owner_uid = int(row.get("owner_user_id") or 0)
    if owner_uid:
        users.setdefault(str(owner_uid), {})["role"] = "tenant_owner"
    for uid, item in list(users.items()):
        try:
            int(uid)
        except Exception:
            users.pop(uid, None)
            continue
        if not isinstance(item, dict):
            item = {"role": "viewer"}
            users[str(uid)] = item
        role = str(item.get("role") or "viewer")
        if role not in TENANT_ROLE_ORDER:
            role = "viewer"
        item["role"] = role
        item.setdefault("joined_at", _tenant_now())
        item.setdefault("updated_at", _tenant_now())
    row["users"] = users
    return row


def tenant_get(tenant_id: str | None) -> dict | None:
    if not tenant_id:
        return None
    root = _tenants_root()
    row = (root.get("tenants") or {}).get(str(tenant_id))
    if not isinstance(row, dict):
        return None
    row = _tenant_normalize(str(tenant_id), row)
    root["tenants"][str(tenant_id)] = row
    return row


def tenant_all() -> list[dict]:
    root = _tenants_root()
    rows = []
    for tid, row in list((root.get("tenants") or {}).items()):
        if isinstance(row, dict):
            rows.append(_tenant_normalize(str(tid), row))
    rows.sort(key=lambda r: (0 if str(r.get("id")) == TENANT_PLATFORM_ID else 1, str(r.get("name") or "").casefold()))
    return rows


def _v177_legacy_0248_tenant_id_for_chat(chat_id: int | None, create: bool = False, actor_user_id: int | None = None) -> str:
    explicit = getattr(_TENANT_CONTEXT, "tenant_id", None)
    if explicit:
        return str(explicit)
    try:
        cid = int(chat_id or 0)
    except Exception:
        cid = 0
    root = _tenants_root()
    if cid:
        tid = str((root.get("chat_to_tenant") or {}).get(str(cid)) or "")
        if tid and tenant_get(tid):
            return tid
    if not create:
        return TENANT_PLATFORM_ID
    uid = int(actor_user_id or tenant_current_actor_user_id() or 0)
    if tenant_is_platform_owner_user(uid):
        tenant_bind_chat(cid, TENANT_PLATFORM_ID, changed_by=uid, force=True)
        return TENANT_PLATFORM_ID
    owner_uid = uid if tenant_user_is_chat_admin(cid, uid) else 0
    tid = tenant_create(_tenant_default_name(cid), owner_uid, cid, created_by=uid, deterministic_chat_id=cid)
    return tid
try: _v177_legacy_0248_tenant_id_for_chat.__name__ = 'tenant_id_for_chat'
except Exception: pass
tenant_id_for_chat = _v177_legacy_0248_tenant_id_for_chat


def tenant_current_id(chat_id: int | None = None) -> str:
    explicit = getattr(_TENANT_CONTEXT, "tenant_id", None)
    if explicit:
        return str(explicit)
    if chat_id is None:
        chat_id = current_state_chat_id()
    return tenant_id_for_chat(chat_id, create=False)


def tenant_create(name: str, owner_user_id: int, root_chat_id: int, created_by: int = 0, deterministic_chat_id: int | None = None) -> str:
    root = _tenants_root()
    with _TENANT_LOCK:
        if deterministic_chat_id is not None:
            seed = hashlib.sha256(f"chat:{int(deterministic_chat_id)}".encode("utf-8")).hexdigest()[:12]
            tid = f"chat_{seed}"
        else:
            tid = f"t_{secrets.token_hex(6)}"
        while tid in root["tenants"]:
            if deterministic_chat_id is not None:
                break
            tid = f"t_{secrets.token_hex(6)}"
        row = _tenant_normalize(tid, {
            "name": str(name or _tenant_default_name(root_chat_id))[:80],
            "owner_user_id": int(owner_user_id or 0),
            "root_chat_id": int(root_chat_id or 0),
            "chat_ids": [int(root_chat_id)] if root_chat_id else [],
            "users": {},
            "settings": {},
            "created_by": int(created_by or 0),
            "created_at": _tenant_now(),
            "updated_at": _tenant_now(),
        })
        root["tenants"][tid] = row
        if root_chat_id:
            tenant_bind_chat(int(root_chat_id), tid, changed_by=int(created_by or owner_user_id or 0), force=True)
        if owner_user_id:
            tenant_set_user_role(tid, int(owner_user_id), "tenant_owner", changed_by=created_by, save=False)
        return tid


def tenant_bind_chat(chat_id: int, tenant_id: str, changed_by: int = 0, force: bool = False) -> bool:
    cid = int(chat_id)
    row = tenant_get(tenant_id)
    if not row:
        return False
    root = _tenants_root()
    old_tid = str((root.get("chat_to_tenant") or {}).get(str(cid)) or "")
    # Hot-path idempotence: callbacks repeatedly confirm the same binding. Do nothing if already correct.
    if old_tid == str(tenant_id) and cid in [int(x) for x in (row.get("chat_ids") or [])]:
        try:
            st = get_chat_store(cid).setdefault("settings", {})
            expected_scope = int(row.get("root_chat_id") or cid)
            if str(st.get("tenant_id") or "") == str(tenant_id) and int(st.get("owner_scope_id") or expected_scope) == expected_scope:
                return True
        except Exception:
            pass
    if old_tid and old_tid != str(tenant_id) and not force:
        return False
    if old_tid and old_tid != str(tenant_id):
        old = tenant_get(old_tid)
        if old:
            old["chat_ids"] = [int(x) for x in old.get("chat_ids") or [] if int(x) != cid]
            old["updated_at"] = _tenant_now()
    root["chat_to_tenant"][str(cid)] = str(tenant_id)
    if cid not in row["chat_ids"]:
        row["chat_ids"].append(cid)
    row["updated_at"] = _tenant_now()
    try:
        store = get_chat_store(cid)
        store.setdefault("settings", {})["tenant_id"] = str(tenant_id)
        store["settings"]["owner_scope_id"] = int(row.get("root_chat_id") or cid)
    except Exception:
        pass
    try:
        bot_journal("tenant_chat_bound", cid, f"tenant={tenant_id} old={old_tid or '-'} by={int(changed_by or 0)}")
    except Exception:
        pass
    if old_tid and old_tid != str(tenant_id):
        try:
            enforce = globals().get("tenant_v148_enforce_forward_isolation")
            if callable(enforce):
                enforce()
        except Exception as exc:
            log_error(f"tenant rebind forward cleanup {cid}: {exc}")
    return True


def tenant_unbind_chat(chat_id: int, changed_by: int = 0) -> bool:
    cid = int(chat_id)
    root = _tenants_root()
    tid = str((root.get("chat_to_tenant") or {}).get(str(cid)) or "")
    row = tenant_get(tid)
    if not row or int(row.get("root_chat_id") or 0) == cid:
        return False
    root["chat_to_tenant"].pop(str(cid), None)
    row["chat_ids"] = [int(x) for x in row.get("chat_ids") or [] if int(x) != cid]
    row["updated_at"] = _tenant_now()
    new_tid = tenant_create(_tenant_default_name(cid), 0, cid, created_by=changed_by, deterministic_chat_id=cid)
    try:
        bot_journal("tenant_chat_unbound", cid, f"old={tid} new={new_tid} by={changed_by}")
    except Exception:
        pass
    return True


def tenant_role_for_user(user_id: int | None, tenant_id: str | None = None, chat_id: int | None = None) -> str:
    try:
        uid = int(user_id or 0)
    except Exception:
        uid = 0
    if tenant_is_platform_owner_user(uid):
        return "platform_owner"
    tid = str(tenant_id or tenant_current_id(chat_id))
    row = tenant_get(tid)
    if not row or not uid:
        return "standard"
    item = (row.get("users") or {}).get(str(uid)) or {}
    role = str(item.get("role") or "standard")
    return role if role in TENANT_ROLE_ORDER else "standard"


def tenant_user_spaces(user_id: int) -> list[dict]:
    uid = int(user_id)
    if tenant_is_platform_owner_user(uid):
        return tenant_all()
    out = []
    for row in tenant_all():
        if str(uid) in (row.get("users") or {}):
            out.append(row)
    return out


def tenant_set_user_role(tenant_id: str, user_id: int, role: str, changed_by: int = 0, save: bool = True) -> bool:
    row = tenant_get(tenant_id)
    if not row:
        return False
    uid = int(user_id)
    role = str(role or "viewer")
    if role not in TENANT_ROLE_ORDER:
        return False
    if role == "tenant_owner":
        if str(tenant_id) == TENANT_PLATFORM_ID and uid != _tenant_platform_owner_user_id():
            return False
        old_owner = int(row.get("owner_user_id") or 0)
        if old_owner and old_owner != uid:
            row.setdefault("users", {}).setdefault(str(old_owner), {})["role"] = "tenant_admin"
        row["owner_user_id"] = uid
    item = row.setdefault("users", {}).setdefault(str(uid), {})
    item["role"] = role
    item.setdefault("joined_at", _tenant_now())
    item["updated_at"] = _tenant_now()
    item["changed_by"] = int(changed_by or 0)
    row["updated_at"] = _tenant_now()
    if save:
        save_data(data, full=True)
        try:
            schedule_delta_backup(int(row.get("root_chat_id") or OWNER_ID or 0), delay=0.5, reason="tenant_user_role")
        except Exception:
            pass
    return True


def _v177_legacy_0249_tenant_can_manage(user_id: int | None, tenant_id: str | None = None, chat_id: int | None = None, owner_only: bool = False) -> bool:
    role = tenant_role_for_user(user_id, tenant_id, chat_id)
    if role == "platform_owner":
        return True
    if owner_only:
        return role == "tenant_owner"
    return role in {"tenant_owner", "tenant_admin"}
try: _v177_legacy_0249_tenant_can_manage.__name__ = 'tenant_can_manage'
except Exception: pass
tenant_can_manage = _v177_legacy_0249_tenant_can_manage


def tenant_user_is_chat_admin(chat_id: int, user_id: int) -> bool:
    try:
        cid = int(chat_id); uid = int(user_id)
    except Exception:
        return False
    if not cid or not uid:
        return False
    if tenant_is_platform_owner_user(uid):
        return True
    try:
        store = get_chat_store(cid)
        typ = str((store.get("info") or {}).get("type") or "")
        if typ == "private" or cid > 0:
            return cid == uid
    except Exception:
        if cid > 0:
            return cid == uid
    try:
        member = bot.get_chat_member(cid, uid)
        return str(getattr(member, "status", "") or "") in {"creator", "administrator"}
    except Exception:
        return False


def tenant_chat_ids(tenant_id: str | None = None) -> list[int]:
    row = tenant_get(tenant_id or tenant_current_id())
    if not row:
        return []
    return sorted({int(x) for x in row.get("chat_ids") or []}, key=lambda x: get_chat_display_name(x).casefold())


def _v177_legacy_0250_tenant_same_space(chat_a: int, chat_b: int) -> bool:
    """True only for two explicitly bound chats in the same space.

    Unknown chat IDs must never inherit the platform tenant implicitly: an old/stale
    forwarding rule to an unseen chat is blocked until that chat is registered.
    """
    try:
        a = int(chat_a); b = int(chat_b)
    except Exception:
        return False
    if a == b:
        return True
    mapping = _tenants_root().get("chat_to_tenant") or {}
    ta = str(mapping.get(str(a)) or "")
    tb = str(mapping.get(str(b)) or "")
    return bool(ta and tb and ta == tb and tenant_get(ta))
try: _v177_legacy_0250_tenant_same_space.__name__ = 'tenant_same_space'
except Exception: pass
tenant_same_space = _v177_legacy_0250_tenant_same_space


def _v177_legacy_0251_tenant_note_chat_seen(msg) -> None:
    try:
        chat_id = int(msg.chat.id)
        user_id = int(getattr(getattr(msg, "from_user", None), "id", 0) or 0)
    except Exception:
        return
    tid = tenant_id_for_chat(chat_id, create=True, actor_user_id=user_id)
    row = tenant_get(tid)
    if not row:
        return
    # An unclaimed space can be claimed automatically by the first verified Telegram admin.
    if not int(row.get("owner_user_id") or 0) and user_id and tenant_user_is_chat_admin(chat_id, user_id):
        tenant_set_user_role(tid, user_id, "tenant_owner", changed_by=user_id, save=False)
    try:
        store = get_chat_store(chat_id)
        store.setdefault("settings", {})["tenant_id"] = tid
        store["settings"]["owner_scope_id"] = int(row.get("root_chat_id") or chat_id)
    except Exception:
        pass
try: _v177_legacy_0251_tenant_note_chat_seen.__name__ = 'tenant_note_chat_seen'
except Exception: pass
tenant_note_chat_seen = _v177_legacy_0251_tenant_note_chat_seen


# Preserve v147 chat-info behavior, then attach/refresh the tenant card.
_V148_ORIG_UPDATE_CHAT_INFO = globals().get("update_chat_info_from_message")
def _v177_legacy_0236_update_chat_info_from_message(msg):
    result = None
    if callable(_V148_ORIG_UPDATE_CHAT_INFO):
        result = _V148_ORIG_UPDATE_CHAT_INFO(msg)
    try:
        tenant_note_chat_seen(msg)
    except Exception as exc:
        log_error(f"tenant_note_chat_seen: {exc}")
    return result
try: _v177_legacy_0236_update_chat_info_from_message.__name__ = 'update_chat_info_from_message'
except Exception: pass
update_chat_info_from_message = _v177_legacy_0236_update_chat_info_from_message


# ─────────────────────────────────────────────────────────────
# Owner scope compatibility layer
# ─────────────────────────────────────────────────────────────
def _v168_owner_access_ids() -> set[int]:
    gs = data.setdefault("_global_settings", {})
    raw = gs.get("owner_access_chat_ids_v168")
    if not isinstance(raw, list):
        # v148 legacy values represented user-owner identities, not the chat-access switches used by Ф2.
        raw = []
        gs["owner_access_chat_ids_v168"] = raw
    out = set()
    for value in raw or []:
        try: out.add(int(value))
        except Exception: pass
    return out


def get_additional_owner_ids() -> set[int]:
    # v168: these are chats in which the platform owner explicitly enabled owner/testing access.
    return _v168_owner_access_ids()


def set_additional_owner(chat_id: int, enabled: bool):
    cid = int(chat_id)
    owners = _v168_owner_access_ids()
    if enabled: owners.add(cid)
    else: owners.discard(cid)
    gs = data.setdefault("_global_settings", {})
    gs["owner_access_chat_ids_v168"] = sorted(owners)
    # Do not mirror chat IDs into legacy additional_owner_ids: v148 interprets that old field as user-owner IDs.
    def _persist_owner_access():
        try: save_data(data, root_only=True)
        except Exception as exc:
            try: log_error(f"v168 owner access persist {cid}: {exc}")
            except Exception: pass
    try:
        scheduler = globals().get("V166_CONFIG_IO_SCHEDULER")
        if scheduler is not None:
            scheduler.cancel("owner-access-root-v168")
            scheduler.schedule("owner-access-root-v168", 0.10, _persist_owner_access)
        else:
            _persist_owner_access()
    except Exception:
        _persist_owner_access()
    try: schedule_config_backup_for_chats(cid, delay=0.25)
    except Exception: pass


def is_primary_owner(chat_id: int) -> bool:
    try: cid = int(chat_id)
    except Exception: return False
    owner_uid = _tenant_platform_owner_user_id()
    if cid == owner_uid:
        return True
    actor = tenant_current_actor_user_id()
    return bool(actor == owner_uid and cid in _v168_owner_access_ids())


def is_owner_chat(chat_id: int) -> bool:
    try: cid = int(chat_id)
    except Exception: return False
    owner_uid = _tenant_platform_owner_user_id()
    actor = tenant_current_actor_user_id()
    if cid == owner_uid:
        return True
    # In a selected external chat only the real platform owner gains this extra owner/test access.
    if actor == owner_uid:
        return cid in _v168_owner_access_ids()
    if actor:
        return tenant_can_manage(actor, chat_id=cid)
    row = tenant_get(tenant_id_for_chat(cid, create=False))
    return bool(row and int(row.get("root_chat_id") or 0) == cid)


def owner_scope_id(chat_id: int | None = None) -> int:
    try:
        cid = int(chat_id if chat_id is not None else (current_state_chat_id() or OWNER_ID or 0))
    except Exception:
        cid = _tenant_platform_owner_user_id()
    row = tenant_get(tenant_id_for_chat(cid, create=False))
    return int((row or {}).get("root_chat_id") or cid or _tenant_platform_owner_user_id())


def owner_scoped_settings(chat_id: int | None = None) -> dict:
    tid = tenant_current_id(chat_id)
    row = tenant_get(tid)
    if not row:
        return data.setdefault("_global_settings", {})
    return row.setdefault("settings", {})


def bind_chat_to_owner_scope(chat_id: int, scope_id: int):
    tid = tenant_id_for_chat(int(scope_id), create=True, actor_user_id=tenant_current_actor_user_id())
    ok = tenant_bind_chat(int(chat_id), tid, changed_by=tenant_current_actor_user_id(), force=True)
    if ok:
        save_data(data, chat_ids=[int(chat_id), int(scope_id)], root_only=False)
    return ok


# ─────────────────────────────────────────────────────────────
# Strict chat-list and forwarding isolation
# ─────────────────────────────────────────────────────────────
def collect_forward_menu_chats() -> dict:
    tid = tenant_current_id()
    result = {}
    for cid in tenant_chat_ids(tid):
        try:
            store = get_chat_store(cid)
            info = store.get("info") or {}
            result[str(cid)] = {
                "title": info.get("title") or get_chat_display_name(cid) or f"Чат {cid}",
                "username": info.get("username"),
                "type": info.get("type"),
            }
        except Exception:
            continue
    return result


def collect_all_known_chat_ids(include_owner: bool = True) -> list[int]:
    ids = tenant_chat_ids(tenant_current_id())
    if not include_owner:
        root_id = owner_scope_id(current_state_chat_id())
        ids = [cid for cid in ids if int(cid) != int(root_id)]
    return sorted(set(ids), key=lambda cid: get_chat_display_name(cid).casefold())


_V148_ORIG_RESOLVE_FORWARD_TARGETS = globals().get("resolve_forward_targets")
def resolve_forward_targets(source_chat_id: int):
    rows = _V148_ORIG_RESOLVE_FORWARD_TARGETS(int(source_chat_id)) if callable(_V148_ORIG_RESOLVE_FORWARD_TARGETS) else []
    out = []
    for dst, mode, fin in rows or []:
        if tenant_same_space(int(source_chat_id), int(dst)):
            out.append((int(dst), mode, bool(fin)))
        else:
            try:
                bot_journal("tenant_cross_forward_blocked", int(source_chat_id), f"dst={int(dst)}")
            except Exception:
                pass
    return out


_V148_ORIG_ADD_FORWARD_LINK = globals().get("add_forward_link")
def _v177_legacy_0142_add_forward_link(src_chat_id: int, dst_chat_id: int, mode: str):
    if not tenant_same_space(int(src_chat_id), int(dst_chat_id)):
        raise PermissionError("Нельзя связать пересылкой чаты из разных пространств")
    return _V148_ORIG_ADD_FORWARD_LINK(int(src_chat_id), int(dst_chat_id), mode)
try: _v177_legacy_0142_add_forward_link.__name__ = 'add_forward_link'
except Exception: pass
add_forward_link = _v177_legacy_0142_add_forward_link


def clear_forward_all():
    """v148: очищает связи только текущего пространства, а не чужие контуры."""
    allowed = {str(x) for x in tenant_chat_ids(tenant_current_id())}
    fr = data.setdefault("forward_rules", {})
    ff = data.setdefault("forward_finance", {})
    for src in list(fr.keys()):
        if str(src) in allowed:
            fr.pop(src, None)
            ff.pop(src, None)
            continue
        for dst in list((fr.get(src) or {}).keys()):
            if str(dst) in allowed:
                (fr.get(src) or {}).pop(dst, None)
                (ff.get(src) or {}).pop(dst, None)
    data["forward_pair_order"] = [
        key for key in (data.get("forward_pair_order") or [])
        if not any(part in allowed for part in str(key).split(":", 1))
    ]
    persist_forward_rules_to_owner()
    save_data(data, full=True)


_V148_ORIG_COLLECT_FORWARD_PAIRS = globals().get("collect_forward_pairs_for_menu")
def _v177_legacy_0190_collect_forward_pairs_for_menu() -> list[tuple[int, int]]:
    rows = _V148_ORIG_COLLECT_FORWARD_PAIRS() if callable(_V148_ORIG_COLLECT_FORWARD_PAIRS) else []
    tid = tenant_current_id()
    allowed = set(tenant_chat_ids(tid))
    return [(int(a), int(b)) for a, b in rows if int(a) in allowed and int(b) in allowed]
try: _v177_legacy_0190_collect_forward_pairs_for_menu.__name__ = 'collect_forward_pairs_for_menu'
except Exception: pass
collect_forward_pairs_for_menu = _v177_legacy_0190_collect_forward_pairs_for_menu


_V148_ORIG_GET_CONNECTED = globals().get("get_connected_chat_ids")
def get_connected_chat_ids(chat_id: int):
    rows = _V148_ORIG_GET_CONNECTED(int(chat_id)) if callable(_V148_ORIG_GET_CONNECTED) else []
    return [int(cid) for cid in rows if tenant_same_space(int(chat_id), int(cid))]


# ─────────────────────────────────────────────────────────────
# Tenant-scoped settings which were globally shared in v147
# ─────────────────────────────────────────────────────────────
def _tenant_settings_for_context(chat_id: int | None = None) -> dict:
    return owner_scoped_settings(chat_id)


def _v177_legacy_0147_forward_copy_edit_mode(chat_id: int | None = None) -> str:
    mode = str(_tenant_settings_for_context(chat_id).get("forward_copy_edit_mode") or "normal").lower()
    return mode if mode in FORWARD_COPY_EDIT_MODES and version_mode_feature("forward_copy_edit") else "normal"
try: _v177_legacy_0147_forward_copy_edit_mode.__name__ = 'forward_copy_edit_mode'
except Exception: pass
forward_copy_edit_mode = _v177_legacy_0147_forward_copy_edit_mode


def _v177_legacy_0149_set_forward_copy_edit_mode(chat_id: int, mode: str):
    mode = str(mode or "normal").lower()
    if mode not in FORWARD_COPY_EDIT_MODES:
        mode = "normal"
    _tenant_settings_for_context(chat_id)["forward_copy_edit_mode"] = mode
    save_data(data, root_only=True)
    return mode
try: _v177_legacy_0149_set_forward_copy_edit_mode.__name__ = 'set_forward_copy_edit_mode'
except Exception: pass
set_forward_copy_edit_mode = _v177_legacy_0149_set_forward_copy_edit_mode


def reminder_ui_mode() -> str:
    mode = str(_tenant_settings_for_context().get("reminder_ui_mode_v142") or "new").lower()
    return mode if mode in {"old", "new"} else "new"


def set_reminder_ui_mode(mode: str) -> str:
    mode = "new" if str(mode).lower() == "new" else "old"
    _tenant_settings_for_context()["reminder_ui_mode_v142"] = mode
    save_data(data, root_only=True)
    return mode


def internal_timer_seconds(key: str, fallback=None) -> float:
    spec = INTERNAL_TIMER_DEFS.get(str(key), {})
    default = spec.get("default", fallback if fallback is not None else 0)
    try:
        value = _tenant_settings_for_context().setdefault("internal_timers", {}).get(str(key), default)
        return float(value)
    except Exception:
        return float(default or 0)


def set_internal_timer_seconds(key: str, seconds: int | float) -> float:
    spec = INTERNAL_TIMER_DEFS.get(str(key))
    if not spec:
        raise KeyError(key)
    value = max(float(spec.get("min", 0)), min(float(spec.get("max", 10**9)), float(seconds)))
    _tenant_settings_for_context().setdefault("internal_timers", {})[str(key)] = value
    save_data(data, root_only=True)
    return value


def backup_excel_all_enabled(chat_id: int | None = None) -> bool:
    return bool(_tenant_settings_for_context(chat_id).get("backup_excel_all_enabled", True))


def set_backup_excel_all_enabled(enabled: bool, chat_id: int | None = None):
    _tenant_settings_for_context(chat_id)["backup_excel_all_enabled"] = bool(enabled)
    save_data(data, root_only=True)


def excel_interface_mode(chat_id: int | None = None) -> str:
    mode = str(_tenant_settings_for_context(chat_id).get("excel_interface_mode") or "new").lower()
    return mode if mode in {"old", "new"} else "new"


def set_excel_interface_mode(mode: str) -> str:
    mode = "old" if str(mode).lower() == "old" else "new"
    _tenant_settings_for_context()["excel_interface_mode"] = mode
    save_data(data, root_only=True)
    return mode


def excel_new_export_options() -> dict:
    settings = _tenant_settings_for_context()
    options = settings.get("excel_new_export_options")
    if not isinstance(options, dict):
        options = {"old_table": False, "comments": False, "notes": True, "description_column": False}
        settings["excel_new_export_options"] = options
    return normalize_excel_export_options(options)


def toggle_excel_new_export_option(option: str) -> dict:
    opts = excel_new_export_options()
    option = str(option or "")
    if option in opts:
        opts[option] = not bool(opts.get(option))
    if option == "old_table" and opts.get("old_table"):
        opts["comments"] = opts["notes"] = opts["description_column"] = False
    elif option in {"comments", "notes", "description_column"} and opts.get(option):
        opts["old_table"] = False
        if option in {"comments", "notes"}:
            for other in {"comments", "notes"} - {option}:
                opts[other] = False
    _tenant_settings_for_context()["excel_new_export_options"] = dict(opts)
    save_data(data, root_only=True)
    return opts


def excel_table_style(chat_id: int) -> str:
    mode = _normalize_excel_table_style(_tenant_settings_for_context(chat_id).get("excel_table_style"))
    return mode or "new_notes"


def set_excel_table_style(chat_id: int, mode: str) -> str:
    mode = _normalize_excel_table_style(mode) or "new_notes"
    _tenant_settings_for_context(chat_id)["excel_table_style"] = mode
    try:
        get_chat_store(int(chat_id)).setdefault("settings", {})["excel_table_style"] = mode
    except Exception:
        pass
    save_data(data, chat_ids=[int(chat_id)])
    return mode


# The iPhone expense endpoint remains platform infrastructure in v148.
# Tenant managers do not receive its controls until the HTTP route can resolve a
# tenant-specific token without relying on Telegram thread-local context.


# ─────────────────────────────────────────────────────────────
# Reminder isolation: IDs remain global for scheduler stability, each config has tenant_id.
# UI/callback context sees only current tenant; background scheduler sees all.
# ─────────────────────────────────────────────────────────────
_V148_ORIG_REMINDER_ITEMS = globals().get("_reminder_items")
_V148_ORIG_REMINDER_CFG = globals().get("_reminder_cfg")
_V148_ORIG_REMINDER_CREATE = globals().get("_reminder_create")


def _reminder_context_filter_active() -> bool:
    return bool(getattr(_TENANT_CONTEXT, "tenant_id", None) or current_state_chat_id() is not None)


def _reminder_items(include_completed: bool = False) -> list[tuple[int, dict]]:
    rows = _V148_ORIG_REMINDER_ITEMS(include_completed=include_completed) if callable(_V148_ORIG_REMINDER_ITEMS) else []
    if not _reminder_context_filter_active():
        return rows
    tid = tenant_current_id()
    return [(rid, cfg) for rid, cfg in rows if str((cfg or {}).get("tenant_id") or TENANT_PLATFORM_ID) == tid]


def _reminder_cfg(reminder_id: int | str | None = None, create: bool = False) -> dict | None:
    cfg = _V148_ORIG_REMINDER_CFG(reminder_id, create=create) if callable(_V148_ORIG_REMINDER_CFG) else None
    if not isinstance(cfg, dict):
        return cfg
    if create and not cfg.get("tenant_id"):
        cfg["tenant_id"] = tenant_current_id()
    if _reminder_context_filter_active() and str(cfg.get("tenant_id") or TENANT_PLATFORM_ID) != tenant_current_id():
        return None
    return cfg


def _reminder_create() -> tuple[int, dict]:
    rid, cfg = _V148_ORIG_REMINDER_CREATE()
    cfg["tenant_id"] = tenant_current_id()
    _reminder_save("tenant_reminder_add")
    return rid, cfg


# ─────────────────────────────────────────────────────────────
# Tenant-scoped user permissions
# ─────────────────────────────────────────────────────────────
def security_role_for_user(user_id: int | None) -> str:
    return tenant_role_for_user(user_id, chat_id=current_state_chat_id())


def security_set_role(user_id: int, role: str) -> str:
    tid = tenant_current_id()
    role_map = {
        "finance_admin": "tenant_admin",
        "forward_manager": "operator",
        "secret_manager": "operator",
        "reminder_manager": "operator",
        "expense_input": "operator",
        "view_only": "viewer",
        "standard": "operator",
    }
    tenant_role = role if role in TENANT_ROLE_ORDER else role_map.get(str(role), "viewer")
    if not tenant_can_manage(tenant_current_actor_user_id(), tid):
        raise PermissionError("Недостаточно прав")
    tenant_set_user_role(tid, int(user_id), tenant_role, changed_by=tenant_current_actor_user_id())
    return tenant_role


def security_known_users() -> list[dict]:
    tid = tenant_current_id()
    row = tenant_get(tid) or {}
    allowed = {str(uid) for uid in (row.get("users") or {}).keys()}
    merged = {}
    for cid in tenant_chat_ids(tid):
        try:
            store = get_chat_store(cid)
            for item in (store.get("known_users") or {}).values():
                if not isinstance(item, dict):
                    continue
                uid = str(int(item.get("id") or 0))
                if uid == "0" or (allowed and uid not in allowed):
                    continue
                merged[uid] = dict(item)
        except Exception:
            pass
    for uid, membership in (row.get("users") or {}).items():
        merged.setdefault(str(uid), {"id": int(uid), "first_name": "", "username": "", "last_seen_ts": 0})
        merged[str(uid)]["tenant_role"] = str((membership or {}).get("role") or "viewer")
    return sorted(merged.values(), key=lambda x: (float(x.get("last_seen_ts") or 0), int(x.get("id") or 0)), reverse=True)


def _v177_legacy_0109_security_user_allowed(user_id: int | None, capability: str) -> bool:
    role = tenant_role_for_user(user_id, chat_id=current_state_chat_id())
    if role in {"platform_owner", "tenant_owner", "tenant_admin"}:
        return True
    if role == "operator":
        return str(capability or "view") in {"view", "finance_input", "finance_manage", "export", "forward_manage", "reminder_manage"}
    if role == "viewer":
        return str(capability or "view") == "view"
    # Existing members of a migrated chat retain ordinary input/view behavior.
    return str(capability or "view") in {"view", "finance_input"}
try: _v177_legacy_0109_security_user_allowed.__name__ = 'security_user_allowed'
except Exception: pass
security_user_allowed = _v177_legacy_0109_security_user_allowed


def _tenant_action_target_chat_ids(action: str) -> set[int]:
    raw = str(action or "")
    ids = set()
    for token in re.findall(r"(?<!\d)-?\d{5,16}(?!\d)", raw):
        try:
            ids.add(int(token))
        except Exception:
            pass
    return ids


def _v177_legacy_0111_safety_permission_allowed(user_id: int | None, chat_id: int | None, action: str) -> bool:
    try:
        uid = int(user_id or 0); cid = int(chat_id or 0)
    except Exception:
        return False
    if tenant_is_platform_owner_user(uid):
        return True
    tid = tenant_id_for_chat(cid, create=False)
    # Hard boundary: even old safety profile cannot act on a foreign tenant target.
    for target in _tenant_action_target_chat_ids(action):
        if str(target) in (_tenants_root().get("chat_to_tenant") or {}) and tenant_id_for_chat(target, create=False) != tid:
            return False
    role = tenant_role_for_user(uid, tid)
    normalized = str(action or "").lower()
    if normalized.startswith((
        "mega_", "restore_", "journal_", "runtime_", "problem_tasks",
        "safety_profile", "additional_owners", "addown:", "keepalive_",
        "info_queues", "info_delta_status", "process_center", "integrity_status",
        "expense_",
    )):
        return False
    if normalized.startswith(("sp:", "tenant:")):
        write_actions = ("sp:chatlink", "sp:userlink", "sp:role", "sp:transfer", "sp:rename", "sp:unlink")
        if normalized.startswith(write_actions):
            return role in {"tenant_owner", "tenant_admin"}
        return role in {"tenant_owner", "tenant_admin", "operator", "viewer"}
    if any(x in normalized for x in ("reset", "delete", "del_selected", "fw_new_clear", "security_role")):
        return role in {"tenant_owner", "tenant_admin"}
    if not safety_profile_new_enabled():
        return True
    return security_user_allowed(uid, _security_callback_capability(normalized))
try: _v177_legacy_0111_safety_permission_allowed.__name__ = 'safety_permission_allowed'
except Exception: pass
safety_permission_allowed = _v177_legacy_0111_safety_permission_allowed


# ─────────────────────────────────────────────────────────────
# Invite links and management commands
# ─────────────────────────────────────────────────────────────
def _tenant_bot_username() -> str:
    global _TENANT_BOT_USERNAME
    if _TENANT_BOT_USERNAME:
        return _TENANT_BOT_USERNAME
    try:
        _TENANT_BOT_USERNAME = str(getattr(bot.get_me(), "username", "") or "").lstrip("@")
    except Exception:
        _TENANT_BOT_USERNAME = ""
    return _TENANT_BOT_USERNAME


def _tenant_token_hash(raw: str) -> str:
    return hashlib.sha256(str(raw).encode("utf-8")).hexdigest()


def _tenant_prune_invites() -> None:
    root = _tenants_root(); now_ts = time.time()
    tokens = root.setdefault("invite_tokens", {})
    for key, row in list(tokens.items()):
        if not isinstance(row, dict):
            tokens.pop(key, None); continue
        if bool(row.get("revoked")) or float(row.get("expires_ts") or 0) < now_ts or int(row.get("uses") or 0) >= int(row.get("max_uses") or 1):
            if now_ts - float(row.get("created_ts") or now_ts) > 86400:
                tokens.pop(key, None)
    if len(tokens) > 500:
        ordered = sorted(tokens.items(), key=lambda kv: float((kv[1] or {}).get("created_ts") or 0))
        for key, _ in ordered[:-500]:
            tokens.pop(key, None)


def _v177_legacy_0252_tenant_create_invite(tenant_id: str, kind: str, role: str, created_by: int, max_uses: int = 1, ttl_hours: int = 72) -> str:
    row = tenant_get(tenant_id)
    if not row:
        raise ValueError("Пространство не найдено")
    kind = "chat" if str(kind) == "chat" else "user"
    role = str(role or "operator")
    if role not in TENANT_ROLE_ORDER or role == "tenant_owner":
        role = "operator"
    raw = secrets.token_urlsafe(12).replace("-", "").replace("_", "")[:18]
    prefix = "sc" if kind == "chat" else "su"
    payload = f"{prefix}_{raw}"
    now_ts = time.time()
    _tenant_prune_invites()
    _tenants_root().setdefault("invite_tokens", {})[_tenant_token_hash(payload)] = {
        "tenant_id": str(tenant_id), "kind": kind, "role": role,
        "created_by": int(created_by or 0), "created_at": _tenant_now(), "created_ts": now_ts,
        "expires_ts": now_ts + max(1, int(ttl_hours)) * 3600,
        "max_uses": max(1, int(max_uses)), "uses": 0, "revoked": False,
    }
    save_data(data, root_only=True)
    return payload
try: _v177_legacy_0252_tenant_create_invite.__name__ = 'tenant_create_invite'
except Exception: pass
tenant_create_invite = _v177_legacy_0252_tenant_create_invite


def _v177_legacy_0253_tenant_consume_invite(payload: str, user_id: int, chat_id: int, chat_type: str = "") -> tuple[bool, str, str]:
    key = _tenant_token_hash(str(payload or "").strip())
    row = (_tenants_root().get("invite_tokens") or {}).get(key)
    if not isinstance(row, dict):
        return False, "Ссылка недействительна или уже использована.", ""
    if row.get("revoked") or float(row.get("expires_ts") or 0) < time.time() or int(row.get("uses") or 0) >= int(row.get("max_uses") or 1):
        return False, "Срок действия ссылки закончился.", ""
    tid = str(row.get("tenant_id") or "")
    tenant = tenant_get(tid)
    if not tenant:
        return False, "Пространство не найдено.", ""
    kind = str(row.get("kind") or "user")
    if kind == "chat":
        if str(chat_type or "") == "private" or int(chat_id) > 0:
            return False, "Эту ссылку нужно использовать при добавлении бота в группу/канал.", tid
        if not tenant_user_is_chat_admin(int(chat_id), int(user_id)):
            return False, "Привязать чат может только его администратор.", tid
        old_tid = str((_tenants_root().get("chat_to_tenant") or {}).get(str(int(chat_id))) or "")
        if old_tid and old_tid != tid:
            old = tenant_get(old_tid)
            # Auto-created, empty/unclaimed shell may be replaced by a valid invite.
            if old and int(old.get("owner_user_id") or 0):
                return False, "Этот чат уже принадлежит другому пространству.", tid
        tenant_bind_chat(int(chat_id), tid, changed_by=int(user_id), force=True)
        message = f"✅ Чат подключён к пространству «{tenant.get('name')}»."
    else:
        tenant_set_user_role(tid, int(user_id), str(row.get("role") or "operator"), changed_by=int(row.get("created_by") or 0), save=False)
        message = f"✅ Вы подключены к пространству «{tenant.get('name')}» как {TENANT_ROLE_LABELS.get(str(row.get('role')), str(row.get('role')))}."
    row["uses"] = int(row.get("uses") or 0) + 1
    row["last_used_at"] = _tenant_now(); row["last_used_by"] = int(user_id or 0)
    save_data(data, full=True)
    return True, message, tid
try: _v177_legacy_0253_tenant_consume_invite.__name__ = 'tenant_consume_invite'
except Exception: pass
tenant_consume_invite = _v177_legacy_0253_tenant_consume_invite


def tenant_invite_link(payload: str) -> str:
    username = _tenant_bot_username()
    if not username:
        return payload
    if str(payload).startswith("sc_"):
        return f"https://t.me/{username}?startgroup={payload}"
    return f"https://t.me/{username}?start={payload}"


def tenant_handle_start_payload(msg) -> bool:
    text = str(getattr(msg, "text", "") or "").strip()
    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        return False
    payload = parts[1].strip()
    if not payload.startswith(("su_", "sc_")):
        return False
    try:
        uid = int(getattr(getattr(msg, "from_user", None), "id", 0) or 0)
        cid = int(msg.chat.id)
        typ = str(getattr(msg.chat, "type", "") or "")
        ok, message, _tid = tenant_consume_invite(payload, uid, cid, typ)
        bot.send_message(cid, message)
        try:
            bot_journal("tenant_invite_consumed" if ok else "tenant_invite_rejected", cid, f"user={uid} payload={payload[:5]}***")
        except Exception:
            pass
    except Exception as exc:
        bot.send_message(msg.chat.id, f"❌ Не удалось применить ссылку: {str(exc)[:300]}")
    return True


def _v177_legacy_0254_tenant_visible_spaces(user_id: int) -> list[dict]:
    return tenant_all() if tenant_is_platform_owner_user(user_id) else tenant_user_spaces(user_id)
try: _v177_legacy_0254_tenant_visible_spaces.__name__ = 'tenant_visible_spaces'
except Exception: pass
tenant_visible_spaces = _v177_legacy_0254_tenant_visible_spaces


def _v177_legacy_0255_tenant_dashboard_text(chat_id: int, user_id: int) -> str:
    current_tid = tenant_id_for_chat(chat_id, create=True, actor_user_id=user_id)
    current = tenant_get(current_tid) or {}
    spaces = tenant_visible_spaces(user_id)
    role = tenant_role_for_user(user_id, current_tid)
    return (
        "🏢 ПРОСТРАНСТВА · ИЗОЛИРОВАННЫЙ РЕЖИМ\n\n"
        f"Текущий чат: {get_chat_display_name(chat_id)}\n"
        f"Пространство: {current.get('name') or current_tid}\n"
        f"Роль: {TENANT_ROLE_LABELS.get(role, role)}\n"
        f"Чатов в пространстве: {len(current.get('chat_ids') or [])}\n"
        f"Подключённых пользователей: {len(current.get('users') or {})}\n\n"
        f"Доступно пространств: {len(spaces)}\n"
        "Чужие чаты, настройки, финансы, напоминания и пересылки здесь не отображаются."
    )
try: _v177_legacy_0255_tenant_dashboard_text.__name__ = 'tenant_dashboard_text'
except Exception: pass
tenant_dashboard_text = _v177_legacy_0255_tenant_dashboard_text


def _v177_legacy_0256_tenant_dashboard_keyboard(chat_id: int, user_id: int):
    kb = types.InlineKeyboardMarkup(row_width=1)
    current_tid = tenant_id_for_chat(chat_id, create=True, actor_user_id=user_id)
    for row in tenant_visible_spaces(user_id)[:25]:
        tid = str(row.get("id")); mark = "✅" if tid == current_tid else "▫️"
        kb.row(IB(f"{mark} {row.get('name')} · {len(row.get('chat_ids') or [])} ч.", callback_data=f"sp:open:{tid}"))
    if tenant_can_manage(user_id, current_tid):
        kb.row(IB("💬 Чаты пространства", callback_data=f"sp:chats:{current_tid}"))
        kb.row(IB("👥 Пользователи", callback_data=f"sp:users:{current_tid}"))
        kb.row(IB("🔗 Ссылка для подключения чата", callback_data=f"sp:chatlink:{current_tid}"))
        kb.row(IB("👤 Ссылка для пользователя", callback_data=f"sp:userlink:{current_tid}:operator"))
    kb.row(IB("❌ Закрыть", callback_data="info_close"))
    return kb
try: _v177_legacy_0256_tenant_dashboard_keyboard.__name__ = 'tenant_dashboard_keyboard'
except Exception: pass
tenant_dashboard_keyboard = _v177_legacy_0256_tenant_dashboard_keyboard


def _v177_legacy_0257_tenant_detail_text(tenant_id: str, viewer_user_id: int) -> str:
    row = tenant_get(tenant_id)
    visible_ids = {str(item.get("id")) for item in tenant_visible_spaces(viewer_user_id)}
    if not row or str(tenant_id) not in visible_ids:
        return "❌ Пространство недоступно."
    owner = int(row.get("owner_user_id") or 0)
    lines = [
        f"🏢 {row.get('name')}", "",
        f"ID: {row.get('id')}",
        f"Владелец: {security_user_display(owner) if owner else 'не назначен'}",
        f"Корневой чат: {get_chat_display_name(int(row.get('root_chat_id') or 0))}",
        f"Чатов: {len(row.get('chat_ids') or [])}",
        f"Пользователей: {len(row.get('users') or {})}",
        "", "Чаты:",
    ]
    for cid in row.get("chat_ids") or []:
        lines.append(f"• {get_chat_display_name(int(cid))} · {int(cid)}")
    return "\n".join(lines)[:3900]
try: _v177_legacy_0257_tenant_detail_text.__name__ = 'tenant_detail_text'
except Exception: pass
tenant_detail_text = _v177_legacy_0257_tenant_detail_text


def tenant_users_text(tenant_id: str) -> str:
    row = tenant_get(tenant_id) or {}
    lines = [f"👥 ПОЛЬЗОВАТЕЛИ · {row.get('name')}", ""]
    for uid, item in sorted((row.get("users") or {}).items(), key=lambda kv: (TENANT_ROLE_ORDER.index(str((kv[1] or {}).get("role") or "viewer")), int(kv[0]))):
        role = str((item or {}).get("role") or "viewer")
        lines.append(f"• {security_user_display(int(uid))} · {TENANT_ROLE_LABELS.get(role, role)} · {uid}")
    if len(lines) == 2:
        lines.append("Нет подключённых пользователей.")
    return "\n".join(lines)[:3900]


def _v177_legacy_0258_tenant_chats_text(tenant_id: str) -> str:
    row = tenant_get(tenant_id) or {}
    lines = [f"💬 ЧАТЫ · {row.get('name')}", ""]
    for cid in row.get("chat_ids") or []:
        marker = "🏠" if int(cid) == int(row.get("root_chat_id") or 0) else "•"
        lines.append(f"{marker} {get_chat_display_name(int(cid))} · {int(cid)}")
    return "\n".join(lines)[:3900]
try: _v177_legacy_0258_tenant_chats_text.__name__ = 'tenant_chats_text'
except Exception: pass
tenant_chats_text = _v177_legacy_0258_tenant_chats_text


def _v177_legacy_0260_tenant_handle_callback(call, data_str: str) -> bool:
    raw = str(data_str or "")
    if not raw.startswith("sp:"):
        return False
    chat_id = int(call.message.chat.id)
    user_id = int(getattr(call.from_user, "id", 0) or 0)
    parts = raw.split(":")
    action = parts[1] if len(parts) > 1 else ""
    tid = parts[2] if len(parts) > 2 else tenant_id_for_chat(chat_id, create=True, actor_user_id=user_id)
    if action == "dashboard":
        safe_edit(bot, call, tenant_dashboard_text(chat_id, user_id), reply_markup=tenant_dashboard_keyboard(chat_id, user_id))
        return True
    if not tenant_is_platform_owner_user(user_id) and not any(str(r.get("id")) == tid for r in tenant_visible_spaces(user_id)):
        bot.answer_callback_query(call.id, "Пространство недоступно.", show_alert=True)
        return True
    if action == "open":
        kb = types.InlineKeyboardMarkup(row_width=1)
        if tenant_can_manage(user_id, tid):
            kb.row(IB("💬 Чаты", callback_data=f"sp:chats:{tid}"), IB("👥 Пользователи", callback_data=f"sp:users:{tid}"))
            kb.row(IB("🔗 Подключить чат", callback_data=f"sp:chatlink:{tid}"))
            kb.row(IB("👤 Пригласить оператора", callback_data=f"sp:userlink:{tid}:operator"))
            kb.row(IB("👁 Пригласить зрителя", callback_data=f"sp:userlink:{tid}:viewer"))
        kb.row(IB("🔙 К пространствам", callback_data="sp:list:x"))
        safe_edit(bot, call, tenant_detail_text(tid, user_id), reply_markup=kb)
        return True
    if action == "list":
        safe_edit(bot, call, tenant_dashboard_text(chat_id, user_id), reply_markup=tenant_dashboard_keyboard(chat_id, user_id))
        return True
    if action in {"chatlink", "userlink"} and not tenant_can_manage(user_id, tid):
        bot.answer_callback_query(call.id, "Недостаточно прав.", show_alert=True)
        return True
    if action == "chats":
        kb = types.InlineKeyboardMarkup(); kb.row(IB("🔙 Назад", callback_data=f"sp:open:{tid}"))
        safe_edit(bot, call, tenant_chats_text(tid), reply_markup=kb); return True
    if action == "users":
        kb = types.InlineKeyboardMarkup(); kb.row(IB("🔙 Назад", callback_data=f"sp:open:{tid}"))
        safe_edit(bot, call, tenant_users_text(tid), reply_markup=kb); return True
    if action == "chatlink":
        payload = tenant_create_invite(tid, "chat", "tenant_admin", user_id, max_uses=1, ttl_hours=72)
        text = (
            "🔗 ССЫЛКА ДЛЯ ПОДКЛЮЧЕНИЯ ЧАТА\n\n"
            + tenant_invite_link(payload)
            + f"\n\nКод: {payload}"
            + f"\nВ уже существующем чате можно выполнить: /space_join {payload}"
            + "\n\nОдноразовая, действует 72 часа. Привязку должен подтвердить администратор целевого чата."
        )
        kb = types.InlineKeyboardMarkup(); kb.row(IB("🔙 Назад", callback_data=f"sp:open:{tid}"))
        safe_edit(bot, call, text, reply_markup=kb); return True
    if action == "userlink":
        role = parts[3] if len(parts) > 3 else "operator"
        payload = tenant_create_invite(tid, "user", role, user_id, max_uses=20, ttl_hours=72)
        text = f"👤 ССЫЛКА ДЛЯ ПОЛЬЗОВАТЕЛЯ\n\n{tenant_invite_link(payload)}\n\nРоль: {TENANT_ROLE_LABELS.get(role, role)}. До 20 использований, 72 часа."
        kb = types.InlineKeyboardMarkup(); kb.row(IB("🔙 Назад", callback_data=f"sp:open:{tid}"))
        safe_edit(bot, call, text, reply_markup=kb); return True
    return True
try: _v177_legacy_0260_tenant_handle_callback.__name__ = 'tenant_handle_callback'
except Exception: pass
tenant_handle_callback = _v177_legacy_0260_tenant_handle_callback


def _tenant_command_parts(msg) -> list[str]:
    return str(getattr(msg, "text", "") or "").strip().split()


def _tenant_send_dashboard(msg):
    uid = int(getattr(getattr(msg, "from_user", None), "id", 0) or 0)
    cid = int(msg.chat.id)
    tenant_note_chat_seen(msg)
    bot.send_message(cid, tenant_dashboard_text(cid, uid), reply_markup=tenant_dashboard_keyboard(cid, uid))


@bot.message_handler(commands=["space", "spaces", "tenant", "пространство", "пространства"])
def cmd_tenant_space(msg):
    schedule_command_delete(msg)
    _tenant_send_dashboard(msg)


@bot.message_handler(commands=["space_create", "tenant_create"])
def cmd_tenant_create(msg):
    schedule_command_delete(msg)
    uid = int(getattr(getattr(msg, "from_user", None), "id", 0) or 0); cid = int(msg.chat.id)
    if not tenant_user_is_chat_admin(cid, uid):
        send_and_auto_delete(cid, "❌ Создать пространство может владелец личного чата или администратор группы.", 12); return
    mapped_tid = str((_tenants_root().get("chat_to_tenant") or {}).get(str(cid)) or "")
    current_tid = mapped_tid or ""
    current = tenant_get(current_tid) if current_tid else None
    if current and (int(current.get("owner_user_id") or 0) or current_tid == TENANT_PLATFORM_ID):
        send_and_auto_delete(cid, "❌ Этот чат уже принадлежит пространству. Используйте /space.", 12); return
    parts = _tenant_command_parts(msg); name = " ".join(parts[1:]).strip() or _tenant_default_name(cid)
    if current:
        current["name"] = name[:80]; tenant_set_user_role(current_tid, uid, "tenant_owner", changed_by=uid, save=False); tid = current_tid
    else:
        tid = tenant_create(name, uid, cid, created_by=uid, deterministic_chat_id=cid)
    save_data(data, full=True)
    bot.send_message(cid, f"✅ Создано пространство «{tenant_get(tid).get('name')}».\nОткройте /space для управления.")


@bot.message_handler(commands=["space_claim", "tenant_claim"])
def cmd_tenant_claim(msg):
    schedule_command_delete(msg)
    uid = int(getattr(getattr(msg, "from_user", None), "id", 0) or 0); cid = int(msg.chat.id)
    if not tenant_user_is_chat_admin(cid, uid):
        send_and_auto_delete(cid, "❌ Подтвердить владение может только администратор этого чата.", 12); return
    tid = tenant_id_for_chat(cid, create=True, actor_user_id=uid); row = tenant_get(tid)
    if int(row.get("owner_user_id") or 0) and not tenant_is_platform_owner_user(uid):
        send_and_auto_delete(cid, "❌ У пространства уже есть владелец.", 12); return
    parts = _tenant_command_parts(msg)
    if len(parts) > 1:
        row["name"] = " ".join(parts[1:])[:80]
    tenant_set_user_role(tid, uid, "tenant_owner", changed_by=uid)
    bot.send_message(cid, f"✅ Вы стали владельцем пространства «{row.get('name')}».")


@bot.message_handler(commands=["space_join", "tenant_join"])
def cmd_tenant_join(msg):
    schedule_command_delete(msg)
    parts = _tenant_command_parts(msg)
    if len(parts) < 2:
        send_and_auto_delete(msg.chat.id, "Использование: /space_join КОД_ССЫЛКИ", 12); return
    payload = parts[1].strip()
    uid = int(getattr(getattr(msg, "from_user", None), "id", 0) or 0); cid = int(msg.chat.id)
    ok, text, _ = tenant_consume_invite(payload, uid, cid, str(getattr(msg.chat, "type", "") or ""))
    bot.send_message(cid, text)


@bot.message_handler(commands=["space_chat_link", "tenant_chat_link"])
def cmd_tenant_chat_link(msg):
    schedule_command_delete(msg)
    uid = int(getattr(getattr(msg, "from_user", None), "id", 0) or 0); cid = int(msg.chat.id); tid = tenant_id_for_chat(cid, create=True, actor_user_id=uid)
    if not tenant_can_manage(uid, tid):
        send_and_auto_delete(cid, "❌ Недостаточно прав.", 12); return
    payload = tenant_create_invite(tid, "chat", "tenant_admin", uid, max_uses=1, ttl_hours=72)
    bot.send_message(
        cid,
        "🔗 Ссылка для подключения одного чата (72 часа):\n"
        + tenant_invite_link(payload)
        + f"\n\nКод: {payload}\nВ уже существующем чате: /space_join {payload}",
    )


@bot.message_handler(commands=["space_user_link", "tenant_user_link"])
def cmd_tenant_user_link(msg):
    schedule_command_delete(msg)
    uid = int(getattr(getattr(msg, "from_user", None), "id", 0) or 0); cid = int(msg.chat.id); tid = tenant_id_for_chat(cid, create=True, actor_user_id=uid)
    if not tenant_can_manage(uid, tid):
        send_and_auto_delete(cid, "❌ Недостаточно прав.", 12); return
    parts = _tenant_command_parts(msg); role = parts[1].lower() if len(parts) > 1 else "operator"
    if role not in {"tenant_admin", "operator", "viewer"}:
        role = "operator"
    payload = tenant_create_invite(tid, "user", role, uid, max_uses=20, ttl_hours=72)
    bot.send_message(cid, f"👤 Ссылка для пользователей ({TENANT_ROLE_LABELS.get(role)}):\n{tenant_invite_link(payload)}")


@bot.message_handler(commands=["space_users", "tenant_users"])
def cmd_tenant_users(msg):
    schedule_command_delete(msg)
    uid = int(getattr(getattr(msg, "from_user", None), "id", 0) or 0); cid = int(msg.chat.id); tid = tenant_id_for_chat(cid, create=True, actor_user_id=uid)
    if not tenant_can_manage(uid, tid):
        send_and_auto_delete(cid, "❌ Недостаточно прав.", 12); return
    bot.send_message(cid, tenant_users_text(tid))


@bot.message_handler(commands=["space_chats", "tenant_chats"])
def cmd_tenant_chats(msg):
    schedule_command_delete(msg)
    uid = int(getattr(getattr(msg, "from_user", None), "id", 0) or 0); cid = int(msg.chat.id); tid = tenant_id_for_chat(cid, create=True, actor_user_id=uid)
    if not tenant_can_manage(uid, tid):
        send_and_auto_delete(cid, "❌ Недостаточно прав.", 12); return
    bot.send_message(cid, tenant_chats_text(tid))


@bot.message_handler(commands=["space_role", "tenant_role"])
def cmd_tenant_role(msg):
    schedule_command_delete(msg)
    actor = int(getattr(getattr(msg, "from_user", None), "id", 0) or 0); cid = int(msg.chat.id); tid = tenant_id_for_chat(cid, create=True, actor_user_id=actor)
    if not tenant_can_manage(actor, tid):
        send_and_auto_delete(cid, "❌ Недостаточно прав.", 12); return
    parts = _tenant_command_parts(msg)
    if len(parts) < 3:
        send_and_auto_delete(cid, "Использование: /space_role USER_ID tenant_admin|operator|viewer", 15); return
    try:
        uid = int(parts[1]); role = parts[2].lower()
    except Exception:
        send_and_auto_delete(cid, "❌ Неверный USER_ID.", 12); return
    if role not in {"tenant_admin", "operator", "viewer"}:
        send_and_auto_delete(cid, "❌ Допустимые роли: tenant_admin, operator, viewer.", 12); return
    tenant_set_user_role(tid, uid, role, changed_by=actor)
    bot.send_message(cid, f"✅ Пользователь {uid}: {TENANT_ROLE_LABELS.get(role)}.")


@bot.message_handler(commands=["space_transfer", "tenant_transfer"])
def cmd_tenant_transfer(msg):
    schedule_command_delete(msg)
    actor = int(getattr(getattr(msg, "from_user", None), "id", 0) or 0); cid = int(msg.chat.id); tid = tenant_id_for_chat(cid, create=True, actor_user_id=actor)
    if not tenant_can_manage(actor, tid, owner_only=True):
        send_and_auto_delete(cid, "❌ Передать владение может только владелец пространства.", 12); return
    if tid == TENANT_PLATFORM_ID:
        send_and_auto_delete(cid, "❌ Владение всей платформой закреплено за основным владельцем и не передаётся этой командой.", 15); return
    parts = _tenant_command_parts(msg)
    if len(parts) < 2:
        send_and_auto_delete(cid, "Использование: /space_transfer USER_ID", 12); return
    uid = int(parts[1]); tenant_set_user_role(tid, uid, "tenant_owner", changed_by=actor)
    bot.send_message(cid, f"✅ Владение пространством передано пользователю {uid}.")


@bot.message_handler(commands=["space_rename", "tenant_rename"])
def cmd_tenant_rename(msg):
    schedule_command_delete(msg)
    actor = int(getattr(getattr(msg, "from_user", None), "id", 0) or 0); cid = int(msg.chat.id); tid = tenant_id_for_chat(cid, create=True, actor_user_id=actor)
    if not tenant_can_manage(actor, tid):
        send_and_auto_delete(cid, "❌ Недостаточно прав.", 12); return
    parts = _tenant_command_parts(msg); name = " ".join(parts[1:]).strip()
    if not name:
        send_and_auto_delete(cid, "Использование: /space_rename Новое название", 12); return
    tenant_get(tid)["name"] = name[:80]; tenant_get(tid)["updated_at"] = _tenant_now(); save_data(data, root_only=True)
    bot.send_message(cid, f"✅ Пространство переименовано: {name[:80]}")


@bot.message_handler(commands=["space_unlink", "tenant_unlink"])
def cmd_tenant_unlink(msg):
    schedule_command_delete(msg)
    actor = int(getattr(getattr(msg, "from_user", None), "id", 0) or 0); cid = int(msg.chat.id); tid = tenant_id_for_chat(cid, create=True, actor_user_id=actor)
    if not tenant_can_manage(actor, tid):
        send_and_auto_delete(cid, "❌ Недостаточно прав.", 12); return
    row = tenant_get(tid)
    if int(row.get("root_chat_id") or 0) == cid:
        send_and_auto_delete(cid, "❌ Корневой чат нельзя отсоединить. Можно передать владение или подключить другие чаты.", 15); return
    tenant_unbind_chat(cid, changed_by=actor); save_data(data, full=True)
    bot.send_message(cid, "✅ Чат отсоединён и получил собственное изолированное пространство.")




# ─────────────────────────────────────────────────────────────
# Legacy UI hardening: tenant lists instead of global lists
# ─────────────────────────────────────────────────────────────
def tenant_require_platform_owner(msg, notify: bool = True) -> bool:
    uid = int(getattr(getattr(msg, "from_user", None), "id", 0) or 0)
    ok = tenant_is_platform_owner_user(uid)
    if not ok and notify:
        try:
            send_and_auto_delete(int(msg.chat.id), "Эта команда доступна только владельцу всей платформы.", 10)
        except Exception:
            pass
    return ok


def build_fin_windows_chat_menu(day_key: str):
    kb = types.InlineKeyboardMarkup(row_width=2)
    buttons = []
    for cid in tenant_chat_ids(tenant_current_id()):
        if not is_finance_mode(int(cid)):
            continue
        if is_chat_bot_removed(int(cid)) and not (OWNER_ID and str(int(cid)) == str(OWNER_ID)):
            continue
        buttons.append(IB(chat_button_title(int(cid), get_chat_display_name(int(cid))), callback_data=f"d:{day_key}:finwin_open_{int(cid)}"))
    if buttons:
        add_buttons_in_rows(kb, sorted(buttons, key=lambda b: str(getattr(b, "text", "")).casefold()), 2)
    else:
        kb.row(IB("Нет чатов с финрежимом", callback_data="none"))
    kb.row(IB("🔙 Назад", callback_data=f"d:{day_key}:back_main"))
    return kb


def _v177_legacy_0061_build_forward_status_lines() -> list[str]:
    lines = []
    fr = data.get("forward_rules", {}) or {}
    ff = data.get("forward_finance", {}) or {}
    allowed = set(tenant_chat_ids(tenant_current_id()))
    pairs = set()
    for src, dsts in fr.items():
        try:
            a = int(src)
        except Exception:
            continue
        if a not in allowed:
            continue
        for dst in (dsts or {}).keys():
            try:
                b = int(dst)
            except Exception:
                continue
            if b not in allowed:
                continue
            pairs.add(tuple(sorted((a, b))))
    for a, b in sorted(pairs, key=lambda p: (get_chat_display_name(p[0]).casefold(), get_chat_display_name(p[1]).casefold())):
        ab = str(b) in (fr.get(str(a), {}) or {})
        ba = str(a) in (fr.get(str(b), {}) or {})
        if not (ab or ba):
            continue
        ab_fin = bool((ff.get(str(a), {}) or {}).get(str(b), False))
        ba_fin = bool((ff.get(str(b), {}) or {}).get(str(a), False))
        lines.append(f"• {chat_button_title(a)} -({_forward_arrow_icon(ab, ba)})-({_forward_fin_icon(ab_fin, ba_fin)})-{chat_button_title(b)}")
    return lines or ["• Связи пересылки не настроены"]
try: _v177_legacy_0061_build_forward_status_lines.__name__ = 'build_forward_status_lines'
except Exception: pass
build_forward_status_lines = _v177_legacy_0061_build_forward_status_lines


_V148_ORIG_BUILD_INFO_KEYBOARD = globals().get("build_info_keyboard")
def _v177_legacy_0217_build_info_keyboard(chat_id: int):
    kb = _V148_ORIG_BUILD_INFO_KEYBOARD(int(chat_id))
    platform = tenant_is_platform_owner_context(int(chat_id))
    if not platform:
        blocked_prefixes = (
            "journal_", "restore_guard", "mega_manual_restore", "mega_priority", "keepalive_",
            "process_center", "safety_profile", "problem_tasks", "integrity_status", "info_queues",
            "runtime_watcher", "info_delta_status", "additional_owners", "addown:",
            "expense_",
        )
        clean_rows = []
        for row in list(getattr(kb, "keyboard", []) or []):
            kept = []
            for button in row:
                cb = str(getattr(button, "callback_data", "") or "")
                if cb.startswith(blocked_prefixes):
                    continue
                kept.append(button)
            if kept:
                clean_rows.append(kept)
        kb.keyboard = clean_rows
    actor = tenant_current_actor_user_id()
    role = tenant_role_for_user(actor, chat_id=int(chat_id)) if actor else ("tenant_owner" if is_owner_chat(int(chat_id)) else "standard")
    if role in {"platform_owner", "tenant_owner", "tenant_admin", "operator", "viewer"}:
        if not any(str(getattr(button, "callback_data", "") or "") == "sp:dashboard" for row in getattr(kb, "keyboard", []) for button in row):
            kb.row(IB("🏢 Пространство", callback_data="sp:dashboard"))
    return kb
try: _v177_legacy_0217_build_info_keyboard.__name__ = 'build_info_keyboard'
except Exception: pass
build_info_keyboard = _v177_legacy_0217_build_info_keyboard


_V148_ORIG_BUILD_INFO_TEXT = globals().get("build_info_text")
def _v177_legacy_0055_build_info_text(chat_id: int) -> str:
    text = _V148_ORIG_BUILD_INFO_TEXT(int(chat_id))
    if not tenant_is_platform_owner_context(int(chat_id)):
        forbidden = ("/errors", "/runtime_export", "/mega_", "/queues", "/journal", "/sqlite", "/db", "/restore_guard", "MEGA:")
        text = "\n".join(line for line in str(text).splitlines() if not any(token in line for token in forbidden))
    tid = tenant_id_for_chat(int(chat_id), create=False)
    row = tenant_get(tid) or {}
    suffix = f"\n\n🏢 Пространство: {row.get('name') or tid}\n/space — чаты, пользователи и ссылки подключения"
    return (str(text).rstrip() + suffix)[:3900]
try: _v177_legacy_0055_build_info_text.__name__ = 'build_info_text'
except Exception: pass
build_info_text = _v177_legacy_0055_build_info_text


_V148_ORIG_BUILD_HELP_TEXT = globals().get("build_help_text")
def build_help_text(chat_id: int) -> str:
    text = _V148_ORIG_BUILD_HELP_TEXT(int(chat_id))
    actor = tenant_current_actor_user_id()
    tid = tenant_id_for_chat(int(chat_id), create=False)
    role = tenant_role_for_user(actor, tid) if actor else "standard"
    lines = [str(text).rstrip(), "", "🏢 Изолированное пространство:", "/space — открыть пространство"]
    if role in {"platform_owner", "tenant_owner", "tenant_admin"}:
        lines.extend([
            "/space_chat_link — подключить свой дополнительный чат",
            "/space_user_link operator — пригласить пользователя",
            "/space_users — пользователи и роли",
            "/space_chats — чаты пространства",
        ])
    return "\n".join(lines)[:3900]


# ─────────────────────────────────────────────────────────────
# Startup migration and integrity audit
# ─────────────────────────────────────────────────────────────
def tenant_v148_enforce_forward_isolation() -> int:
    removed = 0
    fr = data.setdefault("forward_rules", {})
    ff = data.setdefault("forward_finance", {})
    for src, dsts in list(fr.items()):
        try:
            src_id = int(src)
        except Exception:
            fr.pop(src, None)
            ff.pop(str(src), None)
            continue
        if not isinstance(dsts, dict):
            fr.pop(src, None)
            ff.pop(str(src_id), None)
            continue
        for dst in list(dsts.keys()):
            try:
                dst_id = int(dst)
            except Exception:
                dsts.pop(dst, None)
                (ff.get(str(src_id)) or {}).pop(str(dst), None)
                removed += 1
                continue
            if not tenant_same_space(src_id, dst_id):
                dsts.pop(dst, None)
                (ff.get(str(src_id)) or {}).pop(str(dst_id), None)
                removed += 1
        if not dsts:
            fr.pop(str(src), None)
            ff.pop(str(src_id), None)
    if removed:
        persist_forward_rules_to_owner()
        try:
            bot_journal("tenant_cross_links_removed", OWNER_ID, f"count={removed}", "WARN")
        except Exception:
            pass
    return removed


def tenant_v148_bootstrap() -> dict:
    root = _tenants_root()
    report = {"created": 0, "bound": 0, "legacy_owners": 0, "reminders_tagged": 0, "cross_links_removed": 0}
    with _TENANT_LOCK:
        if TENANT_PLATFORM_ID not in root["tenants"]:
            root["tenants"][TENANT_PLATFORM_ID] = _tenant_normalize(TENANT_PLATFORM_ID, {
                "name": "Основное пространство владельца",
                "owner_user_id": _tenant_platform_owner_user_id(),
                "root_chat_id": _tenant_platform_owner_user_id(),
                "chat_ids": [], "users": {}, "settings": {},
                "created_by": _tenant_platform_owner_user_id(),
            })
            report["created"] += 1
        platform_row = tenant_get(TENANT_PLATFORM_ID)
        legacy_ids = []
        try:
            legacy_ids = [int(x) for x in data.setdefault("_global_settings", {}).get("additional_owner_ids", [])]
        except Exception:
            legacy_ids = []
        owner_tenants = {}
        for uid in legacy_ids:
            tid = f"owner_{uid}"
            if tid not in root["tenants"]:
                root["tenants"][tid] = _tenant_normalize(tid, {
                    "name": f"Пространство {get_chat_display_name(uid)}",
                    "owner_user_id": uid, "root_chat_id": uid, "chat_ids": [uid], "users": {}, "settings": {},
                    "created_by": _tenant_platform_owner_user_id(),
                })
                report["created"] += 1
            owner_tenants[uid] = tid; report["legacy_owners"] += 1
        if legacy_ids:
            data["_global_settings"]["legacy_additional_owner_ids_v148"] = legacy_ids
            data["_global_settings"]["additional_owner_ids"] = []
        for cid_raw, store in list((data.get("chats") or {}).items()):
            try:
                cid = int(cid_raw)
            except Exception:
                continue
            target_tid = TENANT_PLATFORM_ID
            try:
                old_scope = int(((store or {}).get("settings") or {}).get("owner_scope_id") or 0)
                if old_scope in owner_tenants:
                    target_tid = owner_tenants[old_scope]
            except Exception:
                pass
            if not (root.get("chat_to_tenant") or {}).get(str(cid)):
                tenant_bind_chat(cid, target_tid, changed_by=_tenant_platform_owner_user_id(), force=True)
                report["bound"] += 1
        # Owner personal/root chats are always present.
        for row in tenant_all():
            rid = int(row.get("root_chat_id") or 0)
            if rid and rid not in row["chat_ids"]:
                row["chat_ids"].append(rid)
            if rid:
                root["chat_to_tenant"][str(rid)] = str(row.get("id"))
        # Existing v147 reminders belong to the platform tenant unless already tagged.
        try:
            rem_root = data.setdefault("_global_settings", {}).get("reminders_v2") or {}
            for cfg in (rem_root.get("items") or {}).values():
                if isinstance(cfg, dict) and not cfg.get("tenant_id"):
                    cfg["tenant_id"] = TENANT_PLATFORM_ID; report["reminders_tagged"] += 1
        except Exception:
            pass
        # Import legacy global owner settings only into platform tenant once.
        if not platform_row.get("settings_migrated_v148"):
            gs = data.setdefault("_global_settings", {})
            for key in (
                "buttons_current_window", "forward_menu_new_style", "icon_button_mode", "total_secret_mask_enabled",
                "finance_day_start_5am", "finance_day_start_minute", "mega_backup_priority", "internal_timers",
                "backup_excel_all_enabled", "excel_interface_mode", "excel_new_export_options", "excel_table_style_global",
                "forward_copy_edit_mode_global", "reminder_ui_mode_v142", "expense_shortcut",
            ):
                if key in gs:
                    mapped = key.replace("_global", "") if key in {"excel_table_style_global", "forward_copy_edit_mode_global"} else key
                    platform_row.setdefault("settings", {})[mapped] = copy.deepcopy(gs.get(key))
            platform_row["settings_migrated_v148"] = True
        root["legacy_migrated"] = True
        root["schema_version"] = TENANT_SCHEMA_VERSION
    # Remove already-configured cross-space routes so a stale rule cannot leak after deploy.
    report["cross_links_removed"] += tenant_v148_enforce_forward_isolation()
    save_data(data, full=True)
    try:
        bot_journal("tenant_v148_bootstrap", OWNER_ID, json.dumps(report, ensure_ascii=False))
    except Exception:
        pass
    return report


def tenant_v148_snapshot() -> dict:
    root = _tenants_root()
    return {
        "schema_version": root.get("schema_version"),
        "tenants": len(root.get("tenants") or {}),
        "bound_chats": len(root.get("chat_to_tenant") or {}),
        "active_invites": sum(1 for row in (root.get("invite_tokens") or {}).values() if isinstance(row, dict) and not row.get("revoked") and float(row.get("expires_ts") or 0) >= time.time() and int(row.get("uses") or 0) < int(row.get("max_uses") or 1)),
    }

# ---- integrated from 94_v149_tenant_google_merged_reminders.py ----
# ─────────────────────────────────────────────────────────────
# v149: per-tenant Google + dynamic merged reminders
# ─────────────────────────────────────────────────────────────
import base64 as _v149_base64
import hashlib as _v149_hashlib
import hmac as _v149_hmac
import io as _v149_io
import json as _v149_json
import mimetypes as _v149_mimetypes
import os as _v149_os
import re as _v149_re
import secrets as _v149_secrets
import threading as _v149_threading
import time as _v149_time
from collections import defaultdict as _v149_defaultdict
from contextlib import contextmanager as _v149_contextmanager
from copy import deepcopy as _v149_deepcopy
from datetime import timedelta as _v149_timedelta
from pathlib import Path as _v149_Path

VERSION = "bot_v189_main_window_authority_final"
V149_GOOGLE_SCHEMA_VERSION = 1
V149_REMINDER_SCHEMA_VERSION = 1
_V149_GOOGLE_CONTEXT = _v149_threading.local()
_V149_GOOGLE_TOKEN_LOCK = _v149_threading.RLock()
_V149_GOOGLE_TOKEN_CACHE = {}
_V149_REMINDER_BATCH_LOCK = _v149_threading.RLock()
_V149_COMPLETION_LOCK = _v149_threading.RLock()
_V149_PLATFORM_GOOGLE_JSON = str(globals().get("GOOGLE_SERVICE_ACCOUNT_JSON") or "")
_V149_PLATFORM_GOOGLE_SHEET = str(globals().get("GOOGLE_SHEETS_SPREADSHEET_ID") or "")
_V149_PLATFORM_GOOGLE_SHARE = str(globals().get("GOOGLE_SHEETS_SHARE_EMAIL") or "")
_V149_BASE_GOOGLE_SHEETS_CREATE = globals().get("_google_sheets_create_category_report")
_V149_BASE_REMINDER_LIST_TEXT = globals().get("build_reminder_list_text")
_V149_BASE_REMINDER_MENU_TEXT = globals().get("build_reminder_menu_text")


def _v149_now_iso() -> str:
    return now_local().isoformat(timespec="seconds")


def _v149_actor_id(obj) -> int:
    try:
        return int(getattr(getattr(obj, "from_user", None), "id", 0) or 0)
    except Exception:
        return 0


def _v149_actor_label(obj) -> str:
    user = getattr(obj, "from_user", None)
    if user is None:
        return ""
    full = " ".join(x for x in [str(getattr(user, "first_name", "") or "").strip(), str(getattr(user, "last_name", "") or "").strip()] if x).strip()
    username = str(getattr(user, "username", "") or "").strip()
    if username:
        return f"{full or username} (@{username})"[:120]
    return (full or str(getattr(user, "id", "") or ""))[:120]


def _v149_tenant_id(tenant_id: str | None = None, target_chat_id: int | None = None) -> str:
    if tenant_id:
        return str(tenant_id)
    ctx = str(getattr(_V149_GOOGLE_CONTEXT, "tenant_id", "") or "")
    if ctx:
        return ctx
    if target_chat_id is not None:
        try:
            resolved = tenant_id_for_chat(int(target_chat_id), create=False)
            if resolved:
                return str(resolved)
        except Exception:
            pass
    try:
        return str(tenant_current_id(target_chat_id) or TENANT_PLATFORM_ID)
    except Exception:
        return str(TENANT_PLATFORM_ID)


def _v149_chat_belongs_to_tenant(chat_id: int, tenant_id: str) -> bool:
    """Require an explicit v148 binding; fallback-to-platform is not enough for isolation."""
    try:
        return int(chat_id) in {int(x) for x in tenant_chat_ids(str(tenant_id))}
    except Exception:
        return False


@_v149_contextmanager
def tenant_google_context(tenant_id: str | None = None, target_chat_id: int | None = None):
    previous = getattr(_V149_GOOGLE_CONTEXT, "tenant_id", None)
    _V149_GOOGLE_CONTEXT.tenant_id = _v149_tenant_id(tenant_id, target_chat_id)
    try:
        yield _V149_GOOGLE_CONTEXT.tenant_id
    finally:
        _V149_GOOGLE_CONTEXT.tenant_id = previous


def tenant_google_config(tenant_id: str | None = None, create: bool = True) -> dict:
    tid = _v149_tenant_id(tenant_id)
    row = tenant_get(tid)
    if not isinstance(row, dict):
        if not create:
            return {}
        raise RuntimeError("Пространство Google не найдено")
    cfg = row.get("google_v149")
    if not isinstance(cfg, dict):
        if not create:
            return {}
        cfg = {}
        row["google_v149"] = cfg
    cfg.setdefault("schema_version", V149_GOOGLE_SCHEMA_VERSION)
    cfg.setdefault("credentials_sealed", "")
    cfg.setdefault("credential_fingerprint", "")
    cfg.setdefault("service_account_email", "")
    cfg.setdefault("owner_google_email", "")
    cfg.setdefault("spreadsheet_id", "")
    cfg.setdefault("spreadsheet_title", "")
    cfg.setdefault("drive_folder_id", "")
    cfg.setdefault("drive_folder_name", "")
    cfg.setdefault("export_settings", {
        "sheet_enabled": True,
        "drive_enabled": True,
        "sheet_mode": "new_tab",
        "history_limit": 100,
        "error_limit": 50,
    })
    cfg.setdefault("history", [])
    cfg.setdefault("errors", [])
    cfg.setdefault("input_wait", {})
    cfg.setdefault("connected_at", "")
    cfg.setdefault("connected_by", 0)
    cfg.setdefault("updated_at", _v149_now_iso())
    return cfg


def _v149_google_master_key() -> bytes:
    raw = str(_v149_os.getenv("TENANT_GOOGLE_MASTER_KEY") or _v149_os.getenv("GOOGLE_TENANT_MASTER_KEY") or "").strip()
    if len(raw) < 24:
        raise RuntimeError(
            "Для подключения Google пространств задайте в Render секрет TENANT_GOOGLE_MASTER_KEY длиной не менее 24 символов"
        )
    return _v149_hashlib.sha256(raw.encode("utf-8")).digest()


def _v149_stream_xor(payload: bytes, key: bytes, nonce: bytes) -> bytes:
    out = bytearray(len(payload))
    offset = 0
    counter = 0
    while offset < len(payload):
        block = _v149_hmac.new(key, nonce + counter.to_bytes(8, "big"), _v149_hashlib.sha256).digest()
        take = min(len(block), len(payload) - offset)
        for idx in range(take):
            out[offset + idx] = payload[offset + idx] ^ block[idx]
        offset += take
        counter += 1
    return bytes(out)


def _v149_seal_secret(raw: str) -> str:
    master = _v149_google_master_key()
    nonce = _v149_secrets.token_bytes(16)
    enc_key = _v149_hmac.new(master, b"enc:" + nonce, _v149_hashlib.sha256).digest()
    mac_key = _v149_hmac.new(master, b"mac:" + nonce, _v149_hashlib.sha256).digest()
    cipher = _v149_stream_xor(str(raw).encode("utf-8"), enc_key, nonce)
    tag = _v149_hmac.new(mac_key, b"v1:" + nonce + cipher, _v149_hashlib.sha256).digest()
    return "v1." + _v149_base64.urlsafe_b64encode(nonce + tag + cipher).decode("ascii")


def _v149_open_secret(sealed: str) -> str:
    if not str(sealed or "").startswith("v1."):
        raise RuntimeError("Формат зашифрованного Google-ключа не поддерживается")
    try:
        blob = _v149_base64.urlsafe_b64decode(str(sealed).split(".", 1)[1].encode("ascii"))
        nonce, tag, cipher = blob[:16], blob[16:48], blob[48:]
    except Exception as exc:
        raise RuntimeError(f"Google-ключ повреждён: {exc}")
    master = _v149_google_master_key()
    enc_key = _v149_hmac.new(master, b"enc:" + nonce, _v149_hashlib.sha256).digest()
    mac_key = _v149_hmac.new(master, b"mac:" + nonce, _v149_hashlib.sha256).digest()
    expected = _v149_hmac.new(mac_key, b"v1:" + nonce + cipher, _v149_hashlib.sha256).digest()
    if not _v149_hmac.compare_digest(tag, expected):
        raise RuntimeError("Google-ключ не прошёл проверку целостности")
    try:
        return _v149_stream_xor(cipher, enc_key, nonce).decode("utf-8")
    except Exception as exc:
        raise RuntimeError(f"Google-ключ не расшифрован: {exc}")


def _v149_parse_google_service_json(raw: str) -> dict:
    try:
        info = _v149_json.loads(str(raw))
    except Exception as exc:
        raise RuntimeError(f"JSON Google повреждён: {exc}")
    if not isinstance(info, dict):
        raise RuntimeError("JSON Google должен быть объектом")
    if str(info.get("type") or "") != "service_account":
        raise RuntimeError("Нужен JSON ключ типа service_account")
    for key in ("client_email", "private_key", "token_uri"):
        if not str(info.get(key) or "").strip():
            raise RuntimeError(f"В Google JSON отсутствует {key}")
    return info


def tenant_google_set_credentials(tenant_id: str, raw: str, actor_user_id: int) -> dict:
    tid = _v149_tenant_id(tenant_id)
    info = _v149_parse_google_service_json(raw)
    cfg = tenant_google_config(tenant_id)
    cfg["credentials_sealed"] = _v149_seal_secret(_v149_json.dumps(info, ensure_ascii=False, separators=(",", ":")))
    cfg["credential_fingerprint"] = _v149_hashlib.sha256(str(info.get("client_email") or "").encode("utf-8") + str(info.get("private_key_id") or "").encode("utf-8")).hexdigest()[:20]
    cfg["service_account_email"] = str(info.get("client_email") or "")[:250]
    cfg["connected_at"] = _v149_now_iso()
    cfg["connected_by"] = int(actor_user_id or 0)
    cfg["updated_at"] = _v149_now_iso()
    cfg["input_wait"] = {}
    with _V149_GOOGLE_TOKEN_LOCK:
        for key in list(_V149_GOOGLE_TOKEN_CACHE):
            if str(key).startswith(str(tenant_id) + ":"):
                _V149_GOOGLE_TOKEN_CACHE.pop(key, None)
    tenant_google_history(tenant_id, "account_connected", "Google service account подключён", ok=True)
    tenant_google_persist(tid, "tenant_google_update")
    return info


def tenant_google_persist(tenant_id: str, reason: str = "tenant_google") -> None:
    tid = _v149_tenant_id(tenant_id)
    save_data(data, root_only=True)
    try:
        row = tenant_get(tid) or {}
        scope_chat = int(row.get("root_chat_id") or OWNER_ID or 0)
        if scope_chat:
            schedule_delta_backup(scope_chat, delay=0.35, reason=str(reason or "tenant_google"))
    except Exception as exc:
        try: log_error(f"tenant google delta schedule: {exc}")
        except Exception: pass


def tenant_google_history(tenant_id: str, action: str, detail: str = "", ok: bool = True, **meta) -> None:
    cfg = tenant_google_config(tenant_id)
    row = {
        "at": _v149_now_iso(),
        "action": str(action)[:80],
        "ok": bool(ok),
        "detail": str(detail or "")[:500],
    }
    if meta:
        row["meta"] = {str(k)[:50]: str(v)[:250] for k, v in meta.items() if k not in {"credentials", "private_key", "token"}}
    rows = cfg.setdefault("history", [])
    rows.append(row)
    limit = max(10, min(500, int((cfg.get("export_settings") or {}).get("history_limit", 100) or 100)))
    del rows[:-limit]
    cfg["updated_at"] = _v149_now_iso()


def tenant_google_error(tenant_id: str, action: str, exc) -> None:
    tid = _v149_tenant_id(tenant_id)
    cfg = tenant_google_config(tenant_id)
    message = str(exc or "Ошибка")
    # Never store JWTs/private keys accidentally returned by a library.
    message = _v149_re.sub(r"-----BEGIN [^-]+-----.*?-----END [^-]+-----", "[REDACTED KEY]", message, flags=_v149_re.S)
    message = _v149_re.sub(r"(?i)(access_token|refresh_token|private_key|client_secret)\s*[:=]\s*[^,\s]+", r"\1=[REDACTED]", message)
    rows = cfg.setdefault("errors", [])
    rows.append({"at": _v149_now_iso(), "action": str(action)[:80], "error": message[:1000]})
    limit = max(10, min(200, int((cfg.get("export_settings") or {}).get("error_limit", 50) or 50)))
    del rows[:-limit]
    cfg["updated_at"] = _v149_now_iso()
    try:
        tenant_google_persist(tid, "tenant_google_update")
    except Exception:
        pass


def _google_service_account_info(tenant_id: str | None = None) -> dict:
    tid = _v149_tenant_id(tenant_id)
    cfg = tenant_google_config(tid, create=False)
    sealed = str(cfg.get("credentials_sealed") or "") if cfg else ""
    if sealed:
        return _v149_parse_google_service_json(_v149_open_secret(sealed))
    if tid == str(TENANT_PLATFORM_ID) and _V149_PLATFORM_GOOGLE_JSON:
        raw = _V149_PLATFORM_GOOGLE_JSON
        try:
            if raw.lstrip().startswith("{"):
                return _v149_parse_google_service_json(raw)
            return _v149_parse_google_service_json(_v149_base64.b64decode(raw).decode("utf-8"))
        except Exception as exc:
            raise RuntimeError(f"GOOGLE_SERVICE_ACCOUNT_JSON владельца платформы повреждён: {exc}")
    raise RuntimeError("Google-аккаунт этого пространства не подключён. Откройте /google")


def _v149_google_id(value: str, kind: str) -> str:
    raw = str(value or "").strip()
    if kind == "sheet":
        match = _v149_re.search(r"/spreadsheets/d/([A-Za-z0-9_-]+)", raw)
    else:
        match = _v149_re.search(r"/folders/([A-Za-z0-9_-]+)", raw)
    if match:
        raw = match.group(1)
    raw = raw.split("?")[0].split("#")[0].strip().strip("/")
    if not _v149_re.fullmatch(r"[A-Za-z0-9_-]{10,}", raw):
        raise RuntimeError("Неверная ссылка или ID Google " + ("таблицы" if kind == "sheet" else "папки"))
    return raw


def _google_spreadsheet_id(value: str | None = None, tenant_id: str | None = None) -> str:
    if value is not None and str(value).strip():
        return _v149_google_id(str(value), "sheet")
    tid = _v149_tenant_id(tenant_id)
    cfg = tenant_google_config(tid, create=False)
    raw = str((cfg or {}).get("spreadsheet_id") or "")
    if not raw and tid == str(TENANT_PLATFORM_ID):
        raw = _V149_PLATFORM_GOOGLE_SHEET
    if not raw:
        raise RuntimeError("Для этого пространства не выбрана Google Таблица. Откройте /google")
    return _v149_google_id(raw, "sheet")


def tenant_google_drive_folder_id(tenant_id: str | None = None) -> str:
    tid = _v149_tenant_id(tenant_id)
    raw = str(tenant_google_config(tid, create=False).get("drive_folder_id") or "")
    if not raw:
        raise RuntimeError("Для этого пространства не выбрана папка Google Drive. Откройте /google")
    return _v149_google_id(raw, "folder")


def _google_access_token(tenant_id: str | None = None) -> str:
    tid = _v149_tenant_id(tenant_id)
    info = _google_service_account_info(tid)
    fingerprint = _v149_hashlib.sha256((str(info.get("client_email")) + str(info.get("private_key_id"))).encode("utf-8")).hexdigest()[:20]
    cache_key = f"{tid}:{fingerprint}"
    with _V149_GOOGLE_TOKEN_LOCK:
        now = _v149_time.time()
        cached = _V149_GOOGLE_TOKEN_CACHE.get(cache_key) or {}
        if cached.get("token") and now < float(cached.get("expires_at", 0)) - 120:
            return str(cached["token"])
        header = {"alg": "RS256", "typ": "JWT"}
        claims = {
            "iss": info["client_email"],
            "scope": "https://www.googleapis.com/auth/spreadsheets https://www.googleapis.com/auth/drive",
            "aud": info.get("token_uri") or "https://oauth2.googleapis.com/token",
            "iat": int(now),
            "exp": int(now) + 3600,
        }
        signing_input = (
            _b64url(_v149_json.dumps(header, separators=(",", ":")).encode("utf-8"))
            + "."
            + _b64url(_v149_json.dumps(claims, separators=(",", ":")).encode("utf-8"))
        ).encode("ascii")
        signature = _google_sign_rs256(signing_input, info["private_key"])
        assertion = signing_input.decode("ascii") + "." + _b64url(signature)
        response = _google_request_guarded(
            "oauth", requests.post,
            info.get("token_uri") or "https://oauth2.googleapis.com/token",
            data={"grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer", "assertion": assertion},
            timeout=30, attempts=2,
        )
        if response.status_code >= 300:
            raise RuntimeError(f"Google OAuth {response.status_code}: {response.text[:500]}")
        payload = response.json()
        token = str(payload.get("access_token") or "")
        if not token:
            raise RuntimeError("Google OAuth не вернул access_token")
        _V149_GOOGLE_TOKEN_CACHE[cache_key] = {"token": token, "expires_at": now + int(payload.get("expires_in", 3600) or 3600)}
        return token


def _google_sheets_create_category_report(title: str, rows: list[list], layout: str = "category", annotations_override: dict | None = None, include_annotations: bool = True, tenant_id: str | None = None, target_chat_id: int | None = None) -> str:
    if not callable(_V149_BASE_GOOGLE_SHEETS_CREATE):
        raise RuntimeError("Модуль Google Sheets не загружен")
    tid = _v149_tenant_id(tenant_id, target_chat_id)
    if target_chat_id is not None and not _v149_chat_belongs_to_tenant(int(target_chat_id), tid):
        raise RuntimeError("Google export blocked: target chat is not connected to this space")
    cfg = tenant_google_config(tid)
    if not bool((cfg.get("export_settings") or {}).get("sheet_enabled", True)):
        raise RuntimeError("Выгрузка в Google Sheets выключена для этого пространства")
    try:
        with tenant_google_context(tid):
            url = _V149_BASE_GOOGLE_SHEETS_CREATE(
                title, rows, layout=layout,
                annotations_override=annotations_override,
                include_annotations=include_annotations,
            )
        tenant_google_history(tid, "sheets_export", title, ok=True, chat_id=target_chat_id or 0, url=url)
        tenant_google_persist(tid, "tenant_google_update")
        return url
    except Exception as exc:
        tenant_google_error(tid, "sheets_export", exc)
        raise


def tenant_google_upload_export(local_path: str, display_name: str, target_chat_id: int, mime_type: str | None = None) -> str:
    tid = _v149_tenant_id(target_chat_id=target_chat_id)
    if not _v149_chat_belongs_to_tenant(int(target_chat_id), tid):
        raise RuntimeError("Google Drive export blocked: target chat is not connected to this space")
    cfg = tenant_google_config(tid)
    if not bool((cfg.get("export_settings") or {}).get("drive_enabled", True)):
        raise RuntimeError("Выгрузка в Google Drive выключена для этого пространства")
    folder_id = tenant_google_drive_folder_id(tid)
    token = _google_access_token(tid)
    mime_type = str(mime_type or _v149_mimetypes.guess_type(display_name)[0] or "application/octet-stream")
    headers = {"Authorization": f"Bearer {token}"}
    metadata = {
        "name": str(display_name or _v149_Path(local_path).name)[:240],
        "parents": [folder_id],
        "appProperties": {"tenant_id": tid, "source_chat_id": str(int(target_chat_id))},
    }
    try:
        with open(local_path, "rb") as fh:
            response = _google_request_guarded(
                "drive_upload", requests.post,
                "https://www.googleapis.com/upload/drive/v3/files",
                headers=headers,
                params={"uploadType": "multipart", "fields": "id,name,webViewLink,parents"},
                files={
                    "metadata": (None, _v149_json.dumps(metadata, ensure_ascii=False), "application/json; charset=UTF-8"),
                    "file": (metadata["name"], fh, mime_type),
                },
                timeout=120, attempts=1,
            )
        if response.status_code >= 300:
            raise RuntimeError(f"Google Drive upload {response.status_code}: {response.text[:700]}")
        payload = response.json()
        file_id = str(payload.get("id") or "")
        url = str(payload.get("webViewLink") or (f"https://drive.google.com/file/d/{file_id}/view" if file_id else ""))
        tenant_google_history(tid, "drive_export", metadata["name"], ok=True, chat_id=target_chat_id, file_id=file_id)
        tenant_google_persist(tid, "tenant_google_update")
        return url
    except Exception as exc:
        tenant_google_error(tid, "drive_export", exc)
        raise


def tenant_google_create_spreadsheet(tenant_id: str, title: str = "Финансы бота") -> str:
    tid = _v149_tenant_id(tenant_id)
    token = _google_access_token(tid)
    folder_id = tenant_google_drive_folder_id(tid)
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    metadata = {
        "name": str(title or "Финансы бота")[:200],
        "mimeType": "application/vnd.google-apps.spreadsheet",
        "parents": [folder_id],
        "appProperties": {"tenant_id": tid},
    }
    response = _google_request_guarded(
        "drive_create_sheet", requests.post,
        "https://www.googleapis.com/drive/v3/files",
        headers=headers,
        params={"fields": "id,name,webViewLink,parents"},
        json=metadata,
        timeout=60, attempts=1,
    )
    if response.status_code >= 300:
        exc = RuntimeError(f"Google create spreadsheet {response.status_code}: {response.text[:700]}")
        tenant_google_error(tid, "create_spreadsheet", exc)
        raise exc
    payload = response.json()
    spreadsheet_id = _v149_google_id(str(payload.get("id") or ""), "sheet")
    cfg = tenant_google_config(tid)
    cfg["spreadsheet_id"] = spreadsheet_id
    cfg["spreadsheet_title"] = str(payload.get("name") or title)[:200]
    cfg["updated_at"] = _v149_now_iso()
    tenant_google_history(tid, "create_spreadsheet", cfg["spreadsheet_title"], ok=True, spreadsheet_id=spreadsheet_id)
    tenant_google_persist(tid, "tenant_google_update")
    return str(payload.get("webViewLink") or f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit")


def tenant_google_test(tenant_id: str) -> tuple[bool, str]:
    tid = _v149_tenant_id(tenant_id)
    try:
        token = _google_access_token(tid)
        headers = {"Authorization": f"Bearer {token}"}
        parts = []
        folder_id = str(tenant_google_config(tid).get("drive_folder_id") or "")
        if folder_id:
            response = _google_request_guarded(
                "drive_folder_test", requests.get,
                f"https://www.googleapis.com/drive/v3/files/{_v149_google_id(folder_id, 'folder')}",
                headers=headers,
                params={"fields": "id,name,mimeType,trashed"},
                timeout=30, attempts=2,
            )
            if response.status_code >= 300:
                raise RuntimeError(f"Drive folder {response.status_code}: {response.text[:500]}")
            payload = response.json()
            tenant_google_config(tid)["drive_folder_name"] = str(payload.get("name") or "")[:200]
            parts.append("Drive: доступ есть")
        sheet_raw = str(tenant_google_config(tid).get("spreadsheet_id") or "")
        if not sheet_raw and tid == str(TENANT_PLATFORM_ID):
            sheet_raw = _V149_PLATFORM_GOOGLE_SHEET
        if sheet_raw:
            sid = _v149_google_id(sheet_raw, "sheet")
            response = _google_request_guarded(
                "sheet_test", requests.get,
                f"https://sheets.googleapis.com/v4/spreadsheets/{sid}",
                headers=headers,
                params={"fields": "spreadsheetId,properties.title"},
                timeout=30, attempts=2,
            )
            if response.status_code >= 300:
                raise RuntimeError(f"Sheets {response.status_code}: {response.text[:500]}")
            payload = response.json()
            tenant_google_config(tid)["spreadsheet_title"] = str((payload.get("properties") or {}).get("title") or "")[:200]
            parts.append("Sheets: доступ есть")
        if not parts:
            parts.append("Аккаунт подключён; задайте таблицу и папку")
        tenant_google_history(tid, "connection_test", "; ".join(parts), ok=True)
        tenant_google_persist(tid, "tenant_google_update")
        return True, "✅ " + "; ".join(parts)
    except Exception as exc:
        tenant_google_error(tid, "connection_test", exc)
        return False, "❌ " + str(exc)[:700]


def _v149_mask_id(value: str) -> str:
    raw = str(value or "")
    if len(raw) <= 10:
        return raw or "не задан"
    return raw[:6] + "…" + raw[-4:]


def tenant_google_status_text(tenant_id: str) -> str:
    tid = _v149_tenant_id(tenant_id)
    row = tenant_get(tid) or {}
    cfg = tenant_google_config(tid)
    env_fallback = tid == str(TENANT_PLATFORM_ID) and not cfg.get("credentials_sealed") and bool(_V149_PLATFORM_GOOGLE_JSON)
    account = str(cfg.get("service_account_email") or ("Render Environment" if env_fallback else "не подключён"))
    sheet = str(cfg.get("spreadsheet_title") or "")
    folder = str(cfg.get("drive_folder_name") or "")
    return (
        f"☁️ GOOGLE · {row.get('name') or tid}\n\n"
        f"Аккаунт: {account}\n"
        f"Google владельца: {cfg.get('owner_google_email') or 'не указан'}\n"
        f"Таблица: {sheet or _v149_mask_id(cfg.get('spreadsheet_id') or (_V149_PLATFORM_GOOGLE_SHEET if env_fallback else ''))}\n"
        f"Папка Drive: {folder or _v149_mask_id(cfg.get('drive_folder_id'))}\n"
        f"Выгрузка Sheets: {'включена' if bool((cfg.get('export_settings') or {}).get('sheet_enabled', True)) else 'выключена'}\n"
        f"Выгрузка Drive: {'включена' if bool((cfg.get('export_settings') or {}).get('drive_enabled', True)) else 'выключена'}\n"
        f"История: {len(cfg.get('history') or [])}\n"
        f"Ошибки: {len(cfg.get('errors') or [])}\n\n"
        "Данные, токены, таблица, папка, история и ошибки принадлежат только этому пространству.\n"
        "Для подключения нужен JSON ключ service_account и общий мастер-ключ TENANT_GOOGLE_MASTER_KEY в Render."
    )


def tenant_google_keyboard(tenant_id: str):
    tid = str(tenant_id)
    cfg = tenant_google_config(tid)
    kb = types.InlineKeyboardMarkup(row_width=1)
    kb.row(IB("🔑 Подключить / заменить аккаунт", callback_data="v149:google:connect"))
    kb.row(IB("📊 Указать Google Таблицу", callback_data="v149:google:sheet"))
    kb.row(IB("📁 Указать папку Google Drive", callback_data="v149:google:folder"))
    kb.row(IB("👤 Указать email Google владельца", callback_data="v149:google:owner_email"))
    settings = cfg.get("export_settings") or {}
    kb.row(IB(f"📊 Выгрузка Sheets: {'ВКЛ' if settings.get('sheet_enabled', True) else 'ВЫКЛ'}", callback_data="v149:google:toggle_sheet"))
    kb.row(IB(f"📁 Выгрузка Drive: {'ВКЛ' if settings.get('drive_enabled', True) else 'ВЫКЛ'}", callback_data="v149:google:toggle_drive"))
    kb.row(IB("➕ Создать таблицу в папке", callback_data="v149:google:create_sheet"))
    kb.row(IB("🧪 Проверить подключение", callback_data="v149:google:test"))
    kb.row(
        IB(f"📜 История ({len(cfg.get('history') or [])})", callback_data="v149:google:history"),
        IB(f"⚠️ Ошибки ({len(cfg.get('errors') or [])})", callback_data="v149:google:errors"),
    )
    kb.row(IB("🧹 Отключить Google", callback_data="v149:google:disconnect_confirm"))
    return kb


def _v149_google_wait(tenant_id: str, kind: str, chat_id: int, user_id: int) -> None:
    tid = _v149_tenant_id(tenant_id)
    cfg = tenant_google_config(tid)
    cfg["input_wait"] = {
        "kind": str(kind), "chat_id": int(chat_id), "user_id": int(user_id),
        "expires_at": _v149_time.time() + 900,
    }
    cfg["updated_at"] = _v149_now_iso()
    tenant_google_persist(tid, "tenant_google_update")


def _v149_google_can_manage(chat_id: int, user_id: int, owner_only: bool = True) -> tuple[bool, str]:
    tid = str(tenant_id_for_chat(int(chat_id), create=True, actor_user_id=int(user_id)) or TENANT_PLATFORM_ID)
    return bool(tenant_can_manage(int(user_id), tid, owner_only=owner_only)), tid


def tenant_google_handle_message(msg) -> bool:
    """Called near the top of the common non-command message router."""
    try:
        chat_id = int(msg.chat.id)
        user_id = _v149_actor_id(msg)
        # Do not create/claim a tenant for every ordinary message. Only consume a
        # message when this already registered tenant has an active Google input wait.
        tid = str(tenant_id_for_chat(chat_id, create=False) or "")
        if not tid:
            return False
        cfg = tenant_google_config(tid, create=False)
        wait = (cfg or {}).get("input_wait") or {}
        if not wait:
            return False
        if not tenant_can_manage(user_id, tid, owner_only=True):
            return False
        if not wait or int(wait.get("chat_id") or 0) != chat_id or int(wait.get("user_id") or 0) != user_id:
            return False
        if _v149_time.time() > float(wait.get("expires_at") or 0):
            cfg["input_wait"] = {}
            tenant_google_persist(tid, "tenant_google_update")
            return False
        kind = str(wait.get("kind") or "")
        if kind == "credentials":
            if str(getattr(msg, "content_type", "")) != "document":
                send_and_auto_delete(chat_id, "Пришлите JSON-файл service_account как документ.", 12)
                return True
            document = getattr(msg, "document", None)
            if not document or int(getattr(document, "file_size", 0) or 0) > 250_000:
                send_and_auto_delete(chat_id, "JSON-файл отсутствует или слишком большой.", 12)
                return True
            filename = str(getattr(document, "file_name", "") or "").lower()
            if filename and not filename.endswith(".json"):
                send_and_auto_delete(chat_id, "Нужен файл с расширением .json.", 12)
                return True
            file_info = bot.get_file(document.file_id)
            raw_bytes = bot.download_file(file_info.file_path)
            raw = bytes(raw_bytes).decode("utf-8")
            info = tenant_google_set_credentials(tid, raw, user_id)
            try:
                bot.delete_message(chat_id, msg.message_id)
            except Exception:
                pass
            bot.send_message(chat_id, f"✅ Google-аккаунт подключён: {info.get('client_email')}\n\nТеперь укажите свою таблицу и папку Drive через /google.")
            return True
        if str(getattr(msg, "content_type", "")) != "text":
            send_and_auto_delete(chat_id, "Пришлите ссылку или ID текстом.", 10)
            return True
        value = str(getattr(msg, "text", "") or "").strip()
        if kind == "sheet":
            cfg["spreadsheet_id"] = _v149_google_id(value, "sheet")
            cfg["spreadsheet_title"] = ""
            action = "sheet_configured"
            text = "✅ Google Таблица сохранена."
        elif kind == "folder":
            cfg["drive_folder_id"] = _v149_google_id(value, "folder")
            cfg["drive_folder_name"] = ""
            action = "drive_folder_configured"
            text = "✅ Папка Google Drive сохранена."
        elif kind == "owner_email":
            if not _v149_re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", value):
                raise RuntimeError("Неверный email")
            cfg["owner_google_email"] = value[:250]
            action = "owner_email_configured"
            text = "✅ Email владельца Google сохранён."
        else:
            return False
        cfg["input_wait"] = {}
        cfg["updated_at"] = _v149_now_iso()
        tenant_google_history(tid, action, text, ok=True)
        tenant_google_persist(tid, "tenant_google_update")
        try:
            bot.delete_message(chat_id, msg.message_id)
        except Exception:
            pass
        bot.send_message(chat_id, text, reply_markup=tenant_google_keyboard(tid))
        return True
    except Exception as exc:
        try:
            tid = str(tenant_id_for_chat(int(msg.chat.id), create=False) or TENANT_PLATFORM_ID)
            tenant_google_error(tid, "input", exc)
            send_and_auto_delete(int(msg.chat.id), "❌ " + str(exc)[:700], 20)
        except Exception:
            pass
        return True


def _v149_google_history_text(tenant_id: str, errors: bool = False) -> str:
    cfg = tenant_google_config(tenant_id)
    rows = list(cfg.get("errors" if errors else "history") or [])[-20:]
    title = "⚠️ ОШИБКИ GOOGLE" if errors else "📜 ИСТОРИЯ GOOGLE"
    if not rows:
        return title + "\n\nПока пусто."
    lines = [title, ""]
    for row in reversed(rows):
        if errors:
            lines.append(f"{row.get('at')} · {row.get('action')}\n{row.get('error')}")
        else:
            mark = "✅" if row.get("ok") else "❌"
            lines.append(f"{mark} {row.get('at')} · {row.get('action')}\n{row.get('detail')}")
    return "\n\n".join(lines)[:3900]


@bot.message_handler(commands=["google", "google_space", "google_tenant"])
def cmd_v149_google(msg):
    try:
        schedule_command_delete(msg)
    except Exception:
        pass
    chat_id = int(msg.chat.id); user_id = _v149_actor_id(msg)
    ok, tid = _v149_google_can_manage(chat_id, user_id, owner_only=True)
    if not ok:
        send_and_auto_delete(chat_id, "❌ Google пространства может настраивать только его владелец.", 12)
        return
    bot.send_message(chat_id, tenant_google_status_text(tid), reply_markup=tenant_google_keyboard(tid), disable_web_page_preview=True)


@bot.message_handler(commands=["google_connect"])
def cmd_v149_google_connect(msg):
    try: schedule_command_delete(msg)
    except Exception: pass
    chat_id = int(msg.chat.id); user_id = _v149_actor_id(msg)
    ok, tid = _v149_google_can_manage(chat_id, user_id, owner_only=True)
    if not ok:
        send_and_auto_delete(chat_id, "❌ Недостаточно прав.", 10); return
    _v149_google_wait(tid, "credentials", chat_id, user_id)
    bot.send_message(chat_id, "🔑 Пришлите JSON-ключ Google service_account как документ.\n\nСообщение с файлом бот удалит после чтения. Ключ будет храниться зашифрованно.")


@bot.message_handler(commands=["google_sheet"])
def cmd_v149_google_sheet(msg):
    try: schedule_command_delete(msg)
    except Exception: pass
    chat_id = int(msg.chat.id); user_id = _v149_actor_id(msg)
    ok, tid = _v149_google_can_manage(chat_id, user_id, owner_only=True)
    if not ok: send_and_auto_delete(chat_id, "❌ Недостаточно прав.", 10); return
    parts = str(getattr(msg, "text", "") or "").split(maxsplit=1)
    if len(parts) > 1:
        cfg = tenant_google_config(tid); cfg["spreadsheet_id"] = _v149_google_id(parts[1], "sheet"); cfg["spreadsheet_title"] = ""; cfg["updated_at"] = _v149_now_iso(); tenant_google_history(tid, "sheet_configured", "Google Таблица сохранена", ok=True); tenant_google_persist(tid, "tenant_google_update")
        bot.send_message(chat_id, "✅ Google Таблица сохранена.", reply_markup=tenant_google_keyboard(tid)); return
    _v149_google_wait(tid, "sheet", chat_id, user_id)
    bot.send_message(chat_id, "📊 Пришлите ссылку или ID Google Таблицы. Таблица должна быть открыта вашему service_account как редактору.")


@bot.message_handler(commands=["google_drive"])
def cmd_v149_google_drive(msg):
    try: schedule_command_delete(msg)
    except Exception: pass
    chat_id = int(msg.chat.id); user_id = _v149_actor_id(msg)
    ok, tid = _v149_google_can_manage(chat_id, user_id, owner_only=True)
    if not ok: send_and_auto_delete(chat_id, "❌ Недостаточно прав.", 10); return
    parts = str(getattr(msg, "text", "") or "").split(maxsplit=1)
    if len(parts) > 1:
        cfg = tenant_google_config(tid); cfg["drive_folder_id"] = _v149_google_id(parts[1], "folder"); cfg["drive_folder_name"] = ""; cfg["updated_at"] = _v149_now_iso(); tenant_google_history(tid, "drive_folder_configured", "Папка Drive сохранена", ok=True); tenant_google_persist(tid, "tenant_google_update")
        bot.send_message(chat_id, "✅ Папка Google Drive сохранена.", reply_markup=tenant_google_keyboard(tid)); return
    _v149_google_wait(tid, "folder", chat_id, user_id)
    bot.send_message(chat_id, "📁 Пришлите ссылку или ID папки Google Drive. Папка должна быть открыта вашему service_account как редактору.")


@bot.message_handler(commands=["google_email"])
def cmd_v149_google_email(msg):
    try: schedule_command_delete(msg)
    except Exception: pass
    chat_id = int(msg.chat.id); user_id = _v149_actor_id(msg)
    ok, tid = _v149_google_can_manage(chat_id, user_id, owner_only=True)
    if not ok: send_and_auto_delete(chat_id, "❌ Недостаточно прав.", 10); return
    parts = str(getattr(msg, "text", "") or "").split(maxsplit=1)
    if len(parts) > 1:
        value = parts[1].strip()
        if not _v149_re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", value):
            send_and_auto_delete(chat_id, "❌ Неверный email.", 10); return
        cfg = tenant_google_config(tid); cfg["owner_google_email"] = value[:250]; cfg["updated_at"] = _v149_now_iso()
        tenant_google_history(tid, "owner_email_configured", "Email Google владельца сохранён", ok=True)
        tenant_google_persist(tid, "tenant_google_update")
        bot.send_message(chat_id, "✅ Email Google владельца сохранён.", reply_markup=tenant_google_keyboard(tid)); return
    _v149_google_wait(tid, "owner_email", chat_id, user_id)
    bot.send_message(chat_id, "👤 Пришлите email Google-аккаунта владельца пространства.")


# ─────────────────────────────────────────────────────────────
# Reminder settings, dynamic grouping and /vyapl
# ─────────────────────────────────────────────────────────────
def _v149_reminder_settings(tenant_id: str | None = None) -> dict:
    """Tenant-level reminder metadata (history and per-chat setting map)."""
    tid = _v149_tenant_id(tenant_id)
    row = tenant_get(tid)
    if not isinstance(row, dict):
        return {}
    settings = row.setdefault("settings", {})
    settings.setdefault("reminder_completion_history_v149", [])
    settings.setdefault("reminder_chat_settings_v149", {})
    return settings


def _v149_reminder_chat_settings(tenant_id: str | None = None, chat_id: int | None = None) -> dict:
    tid = _v149_tenant_id(tenant_id, chat_id)
    row = tenant_get(tid) or {}
    if chat_id is None:
        try:
            chat_id = int(current_state_chat_id() or row.get("root_chat_id") or 0)
        except Exception:
            chat_id = int(row.get("root_chat_id") or 0)
    try:
        cid = int(chat_id or 0)
    except Exception:
        cid = 0
    settings = _v149_reminder_settings(tid)
    mapping = settings.setdefault("reminder_chat_settings_v149", {})
    key = str(cid)
    item = mapping.get(key)
    if not isinstance(item, dict):
        # Safely migrate the early tenant-wide draft values as defaults only.
        item = {
            "merge_enabled": bool(settings.get("reminder_merge_enabled_v149", False)),
            "merge_mode": "smart" if bool(settings.get("reminder_merge_enabled_v149", False)) else "off",
            "show_complete_command": bool(settings.get("reminder_show_complete_command_v149", False)),
        }
        mapping[key] = item
    item.setdefault("merge_enabled", False)
    if str(item.get("merge_mode") or "") not in {"off", "smart", "single"}:
        item["merge_mode"] = "smart" if bool(item.get("merge_enabled", False)) else "off"
    item["merge_enabled"] = str(item.get("merge_mode") or "off") != "off"
    item.setdefault("show_complete_command", False)
    item["chat_id"] = cid
    item["tenant_id"] = tid
    return item


REMINDER_MERGE_MODES_V169 = ("off", "smart", "single")


def _v177_legacy_0261_reminder_merge_mode(tenant_id: str | None = None, chat_id: int | None = None) -> str:
    settings = _v149_reminder_chat_settings(tenant_id, chat_id)
    mode = str(settings.get("merge_mode") or "").strip().lower()
    if mode not in REMINDER_MERGE_MODES_V169:
        mode = "smart" if bool(settings.get("merge_enabled", False)) else "off"
    settings["merge_mode"] = mode
    settings["merge_enabled"] = mode != "off"  # legacy mirror
    return mode
try: _v177_legacy_0261_reminder_merge_mode.__name__ = 'reminder_merge_mode'
except Exception: pass
reminder_merge_mode = _v177_legacy_0261_reminder_merge_mode


def _v177_legacy_0262_reminder_merge_enabled(tenant_id: str | None = None, chat_id: int | None = None) -> bool:
    return reminder_merge_mode(tenant_id, chat_id) != "off"
try: _v177_legacy_0262_reminder_merge_enabled.__name__ = 'reminder_merge_enabled'
except Exception: pass
reminder_merge_enabled = _v177_legacy_0262_reminder_merge_enabled


def _v177_legacy_0263_reminder_merge_mode_label(tenant_id: str | None = None, chat_id: int | None = None) -> str:
    return {
        "off": "ВЫКЛ",
        "smart": "ВКЛ",
        "single": "1 СООБЩЕНИЕ",
    }.get(reminder_merge_mode(tenant_id, chat_id), "ВЫКЛ")
try: _v177_legacy_0263_reminder_merge_mode_label.__name__ = 'reminder_merge_mode_label'
except Exception: pass
reminder_merge_mode_label = _v177_legacy_0263_reminder_merge_mode_label


def reminder_show_complete_command(tenant_id: str | None = None, chat_id: int | None = None) -> bool:
    return bool(_v149_reminder_chat_settings(tenant_id, chat_id).get("show_complete_command", False))


def _v149_reminder_all_rows(include_completed: bool = False) -> list[tuple[int, dict]]:
    # Background worker has no Telegram context, therefore v148 returns every tenant.
    return list(_reminder_items(include_completed=include_completed))


def _v149_reminder_chat_ids(cfg: dict) -> list[int]:
    result = []
    for raw in (cfg or {}).get("chat_ids") or []:
        try:
            cid = int(raw)
        except Exception:
            continue
        if cid not in result:
            result.append(cid)
    return result


def _v149_reminder_cfg_tenant(cfg: dict) -> str:
    return str((cfg or {}).get("tenant_id") or TENANT_PLATFORM_ID)


def _v177_legacy_0264_v149_reminder_chat_allowed(cfg: dict, chat_id: int) -> bool:
    tid = _v149_reminder_cfg_tenant(cfg)
    return _v149_chat_belongs_to_tenant(int(chat_id), tid)
try: _v177_legacy_0264_v149_reminder_chat_allowed.__name__ = '_v149_reminder_chat_allowed'
except Exception: pass
_v149_reminder_chat_allowed = _v177_legacy_0264_v149_reminder_chat_allowed


def _v149_reminder_active_now(cfg: dict, now_dt) -> bool:
    return bool(
        cfg and cfg.get("enabled") and not _reminder_is_completed(cfg)
        and str(cfg.get("text") or "").strip()
        and _reminder_date_allowed(now_dt, cfg)
        and _reminder_time_allowed(now_dt, cfg)
    )


def _v149_group_state_root() -> dict:
    root = data.setdefault("_global_settings", {}).setdefault("reminder_groups_v149", {})
    return root if isinstance(root, dict) else {}


def _v149_group_key(chat_id: int) -> str:
    return str(int(chat_id))


def _v149_completion_history(tenant_id: str) -> list:
    return _v149_reminder_settings(tenant_id).setdefault("reminder_completion_history_v149", [])


def _v149_reminder_message_text(reminder_id: int, cfg: dict, chat_id: int, active_count: int = 1) -> str:
    lines = ["НАПОМИНАЛКА🕰️", "", str(cfg.get("text") or "").strip()]
    if reminder_show_complete_command(chat_id=chat_id):
        if active_count <= 1:
            lines += ["", "Выполнить: /vyapl"]
        else:
            lines += ["", f"Выполнить: /vyapl_{int(reminder_id)}"]
    return "\n".join(lines)[:4000]


def _v149_group_message_text(chat_id: int, members: list[tuple[int, dict]]) -> str:
    lines = ["НАПОМИНАЛКА🕰️", ""]
    show = reminder_show_complete_command(chat_id=chat_id)
    budget = 3900
    for idx, (rid, cfg) in enumerate(members, 1):
        text = str(cfg.get("text") or "").strip()
        block = f"{idx}. {text}"
        if show:
            block += f"\n   /vyapl_{int(rid)}"
        if len("\n".join(lines + [block])) > budget:
            lines.append("…")
            break
        lines.append(block)
    if show and len(members) == 1:
        lines += ["", "Можно также: /vyapl"]
    return "\n".join(lines)[:4000]


def _v149_delete_message(chat_id: int, message_id: int) -> None:
    if not message_id:
        return
    try:
        bot.delete_message(int(chat_id), int(message_id))
    except Exception:
        pass


def _v149_send_or_edit_group(chat_id: int, text: str, old_message_id: int = 0) -> tuple[bool, int]:
    if old_message_id:
        try:
            bot.edit_message_text(text, chat_id=int(chat_id), message_id=int(old_message_id))
            return True, int(old_message_id)
        except Exception as exc:
            if "message is not modified" in str(exc).lower():
                return True, int(old_message_id)
    try:
        sent = bot.send_message(int(chat_id), text)
        new_id = int(sent.message_id)
        if old_message_id and old_message_id != new_id:
            _v149_delete_message(chat_id, old_message_id)
        return True, new_id
    except Exception as exc:
        log_error(f"v149 reminder group send {chat_id}: {exc}")
        return False, int(old_message_id or 0)


def _v149_send_individual(chat_id: int, reminder_id: int, cfg: dict, active_count: int) -> tuple[bool, int]:
    old_mid = int((cfg.get("last_message_ids") or {}).get(str(chat_id)) or 0)
    try:
        sent = bot.send_message(int(chat_id), _v149_reminder_message_text(reminder_id, cfg, chat_id, active_count))
        new_mid = int(sent.message_id)
        if old_mid and old_mid != new_mid:
            _v149_delete_message(chat_id, old_mid)
        return True, new_mid
    except Exception as exc:
        log_error(f"v149 reminder {reminder_id} send {chat_id}: {exc}")
        return False, old_mid


def _v149_cleanup_legacy_group_state_once() -> bool:
    """Remove v142 fixed-2h group messages/state without changing reminder intervals again."""
    gs = data.setdefault("_global_settings", {})
    if bool(gs.get("reminder_groups_v149_migrated")):
        return False
    old = gs.pop("reminder_groups_v142", {})
    if isinstance(old, dict):
        for key, row in list(old.items()):
            if not isinstance(row, dict):
                continue
            try:
                cid = int(row.get("target_chat_id") or str(key).rsplit(":", 1)[-1])
                mid = int(row.get("last_message_id") or 0)
            except Exception:
                cid = mid = 0
            if cid and mid:
                _v149_delete_message(cid, mid)
    gs["reminder_groups_v149_migrated"] = True
    return True


def _v149_reminder_batch_job(force_chat_id: int | None = None) -> None:
    """One atomic reminder cycle for all chats.

    Every reminder retains its own interval. A merged chat refreshes whenever any member is due,
    so the visible common message follows the smallest currently active interval. Membership
    changes (completed/outside hours) refresh the same message even when no reminder is due.
    """
    if not _V149_REMINDER_BATCH_LOCK.acquire(blocking=False):
        return
    try:
        legacy_migrated = _v149_cleanup_legacy_group_state_once()
        now_dt = now_local()
        due_ids = set()
        snapshots = {}
        active_by_chat = _v149_defaultdict(list)
        ended_changed = False
        with _REMINDER_CONFIG_LOCK:
            for rid, cfg in _v149_reminder_all_rows(include_completed=False):
                rid = int(rid)
                if _reminder_end_has_passed(cfg, now_dt):
                    _reminder_mark_completed(rid, cfg, "end_date_finished", delete_messages=True)
                    ended_changed = True
                    continue
                if _reminder_due_now(cfg, now_dt):
                    if _reminder_date_allowed(now_dt, cfg) and _reminder_time_allowed(now_dt, cfg):
                        if force_chat_id is None or int(force_chat_id) in _v149_reminder_chat_ids(cfg):
                            due_ids.add(rid)
                    else:
                        next_dt = _reminder_next_valid_start(now_dt, cfg)
                        cfg["next_run_at"] = next_dt.isoformat(timespec="seconds") if next_dt else ""
                        if next_dt is None:
                            _reminder_mark_completed(rid, cfg, "schedule_finished", delete_messages=True)
                        else:
                            _reminder_touch(cfg)
                        ended_changed = True
                if _v149_reminder_active_now(cfg, now_dt):
                    snap = _v149_deepcopy(cfg)
                    snapshots[rid] = snap
                    for cid in _v149_reminder_chat_ids(snap):
                        if force_chat_id is not None and int(cid) != int(force_chat_id):
                            continue
                        if _v149_reminder_chat_allowed(snap, cid):
                            active_by_chat[int(cid)].append((rid, snap))
                        else:
                            try: bot_journal("tenant_reminder_cross_chat_blocked", int(cid), f"reminder_id={rid} tenant={_v149_reminder_cfg_tenant(snap)}", "WARN")
                            except Exception: pass

            state_snapshot = _v149_deepcopy(_v149_group_state_root())

        individual_updates = {}
        group_updates = {}
        group_remove_individual = _v149_defaultdict(list)
        sent_for_rid = _v149_defaultdict(bool)
        chats_to_consider = set(active_by_chat)
        if force_chat_id is None:
            chats_to_consider.update(int(k) for k in state_snapshot.keys() if str(k).lstrip("-").isdigit())
        else:
            chats_to_consider.add(int(force_chat_id))

        for cid in sorted(chats_to_consider):
            members = sorted(active_by_chat.get(cid, []), key=lambda row: row[0])
            state = state_snapshot.get(_v149_group_key(cid), {}) or {}
            old_group_mid = int(state.get("last_message_id") or 0)
            old_member_ids = [int(x) for x in (state.get("member_ids") or []) if str(x).isdigit()]
            current_ids = [rid for rid, _cfg in members]
            due_here = [rid for rid, _cfg in members if rid in due_ids]
            merge_mode = reminder_merge_mode(chat_id=cid)
            # smart = historical v149 behavior; single = force one common message even
            # when only one reminder is currently active in the chat.
            keep_group = bool(
                (merge_mode == "smart" and (len(members) >= 2 or old_group_mid))
                or (merge_mode == "single" and (bool(members) or old_group_mid))
            )
            membership_changed = current_ids != old_member_ids

            if keep_group and not members:
                _v149_delete_message(cid, old_group_mid)
                group_updates[cid] = None
                continue

            if keep_group:
                if due_here or membership_changed or force_chat_id is not None:
                    ok, message_id = _v149_send_or_edit_group(cid, _v149_group_message_text(cid, members), old_group_mid)
                    if ok:
                        for rid, cfg in members:
                            sent_for_rid[rid] = sent_for_rid[rid] or (rid in due_ids)
                            old_individual = int((cfg.get("last_message_ids") or {}).get(str(cid)) or 0)
                            if old_individual:
                                group_remove_individual[rid].append((cid, old_individual))
                        group_updates[cid] = {
                            "last_message_id": message_id,
                            "member_ids": current_ids,
                            "last_sent_at": _v149_now_iso(),
                            "tenant_id": _v149_tenant_id(target_chat_id=cid),
                        }
                continue

            if old_group_mid:
                _v149_delete_message(cid, old_group_mid)
                group_updates[cid] = None

            active_count = len(members)
            for rid, cfg in members:
                if rid not in due_ids:
                    continue
                ok, message_id = _v149_send_individual(cid, rid, cfg, active_count)
                if ok:
                    sent_for_rid[rid] = True
                    individual_updates[(rid, cid)] = message_id

        for rid, pairs in group_remove_individual.items():
            for cid, mid in pairs:
                _v149_delete_message(cid, mid)

        changed = bool(ended_changed or legacy_migrated)
        with _REMINDER_CONFIG_LOCK:
            state_root = _v149_group_state_root()
            for cid, update in group_updates.items():
                key = _v149_group_key(cid)
                if update is None:
                    state_root.pop(key, None)
                else:
                    state_root[key] = update
                changed = True
            for (rid, cid), mid in individual_updates.items():
                cfg = _reminder_cfg(rid)
                if cfg:
                    cfg.setdefault("last_message_ids", {})[str(cid)] = int(mid)
                    changed = True
            for rid, pairs in group_remove_individual.items():
                cfg = _reminder_cfg(rid)
                if cfg:
                    for cid, _mid in pairs:
                        cfg.setdefault("last_message_ids", {}).pop(str(cid), None)
                    changed = True
            for rid in sorted(due_ids):
                cfg = _reminder_cfg(rid)
                if not cfg or _reminder_is_completed(cfg):
                    continue
                if sent_for_rid.get(rid):
                    cfg["last_sent_at"] = now_dt.isoformat(timespec="seconds")
                _reminder_advance_after_send(now_dt, cfg)
                if not cfg.get("next_run_at") or _reminder_end_has_passed(cfg, now_local()):
                    _reminder_mark_completed(rid, cfg, "schedule_finished", delete_messages=True)
                else:
                    _reminder_touch(cfg)
                changed = True

            # Diagnostics: next group refresh is the earliest remaining member schedule.
            for cid, row in list(state_root.items()):
                try:
                    chat_id = int(cid)
                except Exception:
                    continue
                next_rows = []
                member_ids = []
                for rid, cfg in _v149_reminder_all_rows(include_completed=False):
                    if not _v149_reminder_active_now(cfg, now_local()) or chat_id not in _v149_reminder_chat_ids(cfg):
                        continue
                    if not _v149_reminder_chat_allowed(cfg, chat_id):
                        continue
                    member_ids.append(int(rid))
                    dt = _reminder_parse_dt(cfg.get("next_run_at"))
                    if dt is not None:
                        next_rows.append(dt)
                row["member_ids"] = sorted(member_ids)
                row["next_run_at"] = min(next_rows).isoformat(timespec="seconds") if next_rows else ""

        if changed:
            _reminder_save("v149_dynamic_merged_tick")
        if due_ids or group_updates:
            try:
                bot_journal("reminder_v149_batch", None, f"due={len(due_ids)} chats={len(chats_to_consider)} groups={sum(1 for v in group_updates.values() if v)}")
            except Exception:
                pass
    finally:
        _V149_REMINDER_BATCH_LOCK.release()


def _reminder_tick() -> None:
    global _REMINDER_FINANCE_BUSY_SINCE
    finance_busy = _reminder_finance_priority_busy()
    if finance_busy:
        if not _REMINDER_FINANCE_BUSY_SINCE:
            _REMINDER_FINANCE_BUSY_SINCE = _v149_time.monotonic()
        if _v149_time.monotonic() - _REMINDER_FINANCE_BUSY_SINCE < _REMINDER_FINANCE_PRIORITY_GRACE_SECONDS:
            return
    else:
        _REMINDER_FINANCE_BUSY_SINCE = 0.0
    if not REMINDER_TASK_POOL.submit_unique("reminder-v149-batch", _v149_reminder_batch_job, None):
        try: bot_journal("reminder_dispatch_coalesced", None, "v149 batch")
        except Exception: pass


def _reminder_group_send_job(target_chat_id: int, day_key: str | None = None, force: bool = False) -> None:
    _v149_reminder_batch_job(int(target_chat_id))


def build_reminder_list_text() -> str:
    rows = _reminder_items()
    enabled = sum(1 for _rid, cfg in rows if bool(cfg.get("enabled")))
    merge_mode = reminder_merge_mode()
    commands = reminder_show_complete_command()
    merge_desc = {
        "off": "каждая напоминалка отдельным сообщением",
        "smart": "объединение включается для нескольких активных напоминалок",
        "single": "все активные тексты всегда находятся в одном сообщении",
    }.get(merge_mode, "отдельные сообщения")
    return (
        "⏰ НАПОМИНАЛКИ\n\n"
        f"Текущих: {len(rows)} · активных: {enabled}\n"
        f"Объединять: {reminder_merge_mode_label()} — {merge_desc}\n"
        f"Показывать команду выполнения: {'✅ включено' if commands else '❌ выключено'}\n"
        f"Завершённых: {len(_reminder_completed_items())}\n\n"
        "Переключатель «Объединять» тройной: ВЫКЛ → ВКЛ → 1 СООБЩЕНИЕ. "
        "Интервал каждой напоминалки при этом сохраняется."
    )


def build_reminder_list_keyboard(day_key: str | None = None, page: int = 0):
    day_key = str(day_key or today_key())
    rows = _reminder_items()
    pages = max(1, (len(rows) + _REMINDER_LIST_PAGE_SIZE - 1) // _REMINDER_LIST_PAGE_SIZE)
    page = max(0, min(int(page or 0), pages - 1))
    kb = types.InlineKeyboardMarkup(row_width=1)
    kb.row(IB(f"🔗 Объединять: {reminder_merge_mode_label()}", callback_data=f"v149:rem:merge:{page}:{day_key}"))
    kb.row(IB(f"✅ Команда /vyapl: {'ВКЛ' if reminder_show_complete_command() else 'ВЫКЛ'}", callback_data=f"v149:rem:command:{page}:{day_key}"))
    kb.row(IB("+добавить⏰", callback_data=f"rem:add:{page}:{day_key}"))
    start = page * _REMINDER_LIST_PAGE_SIZE
    for idx, (rid, cfg) in enumerate(rows[start:start + _REMINDER_LIST_PAGE_SIZE], start=start + 1):
        kb.row(IB(_reminder_button_label(idx, cfg), callback_data=f"rem:open:{rid}:{page}:{day_key}"))
    if pages > 1:
        nav = []
        if page > 0: nav.append(IB("⬅️", callback_data=f"rem:list:{page-1}:{day_key}"))
        nav.append(IB(f"{page+1}/{pages}", callback_data="none"))
        if page + 1 < pages: nav.append(IB("➡️", callback_data=f"rem:list:{page+1}:{day_key}"))
        kb.row(*nav)
    kb.row(IB(f"✅ Завершённые ({len(_reminder_completed_items())})", callback_data="rem:completed:0:0"))
    kb.row(IB("📜 История выполнений", callback_data="v149:rem:history"))
    kb.row(IB("⬅️ Назад", callback_data=f"d:{day_key}:back_main"))
    return kb


def _v149_reminders_for_completion(chat_id: int) -> list[tuple[int, dict]]:
    rows = []
    current = now_local()
    for rid, cfg in _reminder_items(include_completed=False):
        if not _v149_reminder_active_now(cfg, current):
            continue
        if int(chat_id) not in _v149_reminder_chat_ids(cfg):
            continue
        if not _v149_reminder_chat_allowed(cfg, chat_id):
            continue
        rows.append((int(rid), cfg))
    rows.sort(key=lambda row: row[0])
    return rows


def _v149_complete_reminder(reminder_id: int, chat_id: int, actor_user_id: int, actor_label: str) -> tuple[bool, str]:
    with _V149_COMPLETION_LOCK, _REMINDER_CONFIG_LOCK:
        cfg = _reminder_cfg(int(reminder_id))
        if not cfg or int(chat_id) not in _v149_reminder_chat_ids(cfg) or not _v149_reminder_chat_allowed(cfg, chat_id):
            return False, "Напоминалка не найдена в этом чате."
        if _reminder_is_completed(cfg) or not cfg.get("enabled"):
            return False, "Эта напоминалка уже выполнена или выключена. Повторное выполнение не записано."
        tenant_id = _v149_reminder_cfg_tenant(cfg)
        event_id = _v149_hashlib.sha256(f"{tenant_id}:{int(reminder_id)}:{cfg.get('created_at')}:{cfg.get('updated_at')}".encode("utf-8")).hexdigest()[:24]
        history = _v149_completion_history(tenant_id)
        if any(str(row.get("event_id")) == event_id for row in history if isinstance(row, dict)):
            return False, "Это выполнение уже учтено."
        text = str(cfg.get("text") or "").strip()
        _reminder_mark_completed(int(reminder_id), cfg, "manual_vyapl", delete_messages=True)
        cfg["completed_by_user_id"] = int(actor_user_id or 0)
        cfg["completed_by_label"] = str(actor_label or "")[:120]
        cfg["completed_in_chat_id"] = int(chat_id)
        cfg["completion_event_id"] = event_id
        event = {
            "event_id": event_id,
            "at": str(cfg.get("completed_at") or _v149_now_iso()),
            "tenant_id": tenant_id,
            "reminder_id": int(reminder_id),
            "reminder_text": text[:500],
            "chat_id": int(chat_id),
            "chat_title": str(get_chat_display_name(int(chat_id)) or "")[:150],
            "user_id": int(actor_user_id or 0),
            "user": str(actor_label or "")[:120],
        }
        history.append(event)
        del history[:-500]
        _reminder_save("reminder_manual_vyapl")
    try:
        REMINDER_TASK_POOL.submit_unique("reminder-v149-batch", _v149_reminder_batch_job, int(chat_id))
    except Exception:
        pass
    try:
        bot_journal("reminder_manual_completed", int(chat_id), f"reminder_id={int(reminder_id)} user={int(actor_user_id or 0)} event={event_id}")
    except Exception:
        pass
    return True, f"✅ Выполнено: {text[:300]}"


def _v149_completion_keyboard(chat_id: int, rows: list[tuple[int, dict]]):
    kb = types.InlineKeyboardMarkup(row_width=1)
    for rid, cfg in rows[:30]:
        label = str(cfg.get("text") or f"Напоминалка {rid}").strip().replace("\n", " ")
        if len(label) > 48: label = label[:45] + "…"
        kb.row(IB(f"✅ {label}", callback_data=f"v149:rem:done:{int(rid)}:{int(chat_id)}"))
    return kb


def _v149_completion_history_text(tenant_id: str) -> str:
    rows = list(_v149_completion_history(tenant_id))[-30:]
    if not rows:
        return "📜 ИСТОРИЯ ВЫПОЛНЕНИЙ\n\nПока пусто."
    lines = ["📜 ИСТОРИЯ ВЫПОЛНЕНИЙ", ""]
    for row in reversed(rows):
        lines.append(
            f"✅ {row.get('at')} · №{row.get('reminder_id')}\n"
            f"{row.get('reminder_text')}\n"
            f"Кто: {row.get('user') or row.get('user_id')}\n"
            f"Чат: {row.get('chat_title') or row.get('chat_id')}"
        )
    return "\n\n".join(lines)[:3900]


@bot.message_handler(func=lambda m: bool(_v149_re.match(r"^/vyapl(?:_\d+)?(?:@[A-Za-z0-9_]+)?(?:\s|$)", str(getattr(m, "text", "") or ""), _v149_re.I)), content_types=["text"])
def cmd_v149_vyapl(msg):
    try: schedule_command_delete(msg)
    except Exception: pass
    chat_id = int(msg.chat.id); user_id = _v149_actor_id(msg); label = _v149_actor_label(msg)
    match = _v149_re.match(r"^/vyapl(?:_(\d+))?(?:@[A-Za-z0-9_]+)?", str(msg.text or ""), _v149_re.I)
    rid = int(match.group(1)) if match and match.group(1) else None
    rows = _v149_reminders_for_completion(chat_id)
    if rid is not None:
        ok, text = _v149_complete_reminder(rid, chat_id, user_id, label)
        bot.send_message(chat_id, text)
        return
    if not rows:
        send_and_auto_delete(chat_id, "Нет активных напоминалок для выполнения.", 10)
        return
    if len(rows) == 1:
        ok, text = _v149_complete_reminder(rows[0][0], chat_id, user_id, label)
        bot.send_message(chat_id, text)
        return
    bot.send_message(chat_id, "Какую напоминалку отметить выполненной?", reply_markup=_v149_completion_keyboard(chat_id, rows))


@bot.message_handler(commands=["vyapl_history"])
def cmd_v149_vyapl_history(msg):
    try: schedule_command_delete(msg)
    except Exception: pass
    chat_id = int(msg.chat.id); user_id = _v149_actor_id(msg)
    tid = str(tenant_id_for_chat(chat_id, create=False) or TENANT_PLATFORM_ID)
    if not tenant_can_manage(user_id, tid):
        send_and_auto_delete(chat_id, "❌ История доступна владельцу и администраторам пространства.", 10); return
    bot.send_message(chat_id, _v149_completion_history_text(tid))


def _v177_legacy_0266_v149_extension_callback(call, data_str: str) -> bool:
    data_str = str(data_str or "")
    if not data_str.startswith("v149:"):
        return False
    chat_id = int(call.message.chat.id); user_id = _v149_actor_id(call)
    try:
        if data_str.startswith("v149:google:"):
            ok, tid = _v149_google_can_manage(chat_id, user_id, owner_only=True)
            if not ok:
                bot.answer_callback_query(call.id, "Только владелец пространства", show_alert=True)
                return True
            action = data_str.split(":", 2)[2]
            if action == "connect":
                _v149_google_wait(tid, "credentials", chat_id, user_id)
                bot.send_message(chat_id, "🔑 Пришлите JSON-ключ Google service_account как документ. Сообщение будет удалено после чтения.")
            elif action == "sheet":
                _v149_google_wait(tid, "sheet", chat_id, user_id)
                bot.send_message(chat_id, "📊 Пришлите ссылку или ID своей Google Таблицы.")
            elif action == "folder":
                _v149_google_wait(tid, "folder", chat_id, user_id)
                bot.send_message(chat_id, "📁 Пришлите ссылку или ID своей папки Google Drive.")
            elif action == "owner_email":
                _v149_google_wait(tid, "owner_email", chat_id, user_id)
                bot.send_message(chat_id, "👤 Пришлите email Google-аккаунта владельца пространства.")
            elif action in {"toggle_sheet", "toggle_drive"}:
                cfg = tenant_google_config(tid)
                settings = cfg.setdefault("export_settings", {})
                key = "sheet_enabled" if action == "toggle_sheet" else "drive_enabled"
                settings[key] = not bool(settings.get(key, True))
                cfg["updated_at"] = _v149_now_iso()
                tenant_google_history(tid, action, f"{key}={settings[key]}", ok=True)
                tenant_google_persist(tid, "tenant_google_settings")
                bot.send_message(chat_id, tenant_google_status_text(tid), reply_markup=tenant_google_keyboard(tid))
            elif action == "create_sheet":
                url = tenant_google_create_spreadsheet(tid, f"Финансы · {(tenant_get(tid) or {}).get('name') or tid}")
                bot.send_message(chat_id, f"✅ Таблица создана и закреплена за пространством:\n{url}", disable_web_page_preview=True)
            elif action == "test":
                _ok, text = tenant_google_test(tid)
                bot.send_message(chat_id, text)
            elif action == "history":
                bot.send_message(chat_id, _v149_google_history_text(tid, False))
            elif action == "errors":
                bot.send_message(chat_id, _v149_google_history_text(tid, True))
            elif action == "disconnect_confirm":
                kb = types.InlineKeyboardMarkup(row_width=2)
                kb.row(IB("🧹 Да, отключить", callback_data="v149:google:disconnect"), IB("Отмена", callback_data="v149:google:status"))
                bot.send_message(chat_id, "Отключить Google только у этого пространства? Таблицы и файлы в Google удалены не будут.", reply_markup=kb)
            elif action == "disconnect":
                cfg = tenant_google_config(tid)
                keep_history = list(cfg.get("history") or [])
                keep_errors = list(cfg.get("errors") or [])
                (tenant_get(tid) or {}).pop("google_v149", None)
                fresh = tenant_google_config(tid)
                fresh["history"] = keep_history
                fresh["errors"] = keep_errors
                tenant_google_history(tid, "account_disconnected", "Google отключён", ok=True)
                tenant_google_persist(tid, "tenant_google_disconnect")
                bot.send_message(chat_id, "✅ Google этого пространства отключён.")
            elif action == "status":
                bot.send_message(chat_id, tenant_google_status_text(tid), reply_markup=tenant_google_keyboard(tid))
            try: bot.answer_callback_query(call.id)
            except Exception: pass
            return True

        if data_str.startswith("v149:rem:"):
            parts = data_str.split(":")
            action = parts[2] if len(parts) > 2 else ""
            tid = str(tenant_id_for_chat(chat_id, create=False) or TENANT_PLATFORM_ID)
            if action in {"merge", "command"}:
                if not tenant_can_manage(user_id, tid):
                    bot.answer_callback_query(call.id, "Недостаточно прав", show_alert=True); return True
                settings = _v149_reminder_chat_settings(tid, chat_id)
                if action == "merge":
                    current_mode = reminder_merge_mode(tid, chat_id)
                    try:
                        idx = REMINDER_MERGE_MODES_V169.index(current_mode)
                    except ValueError:
                        idx = 0
                    next_mode = REMINDER_MERGE_MODES_V169[(idx + 1) % len(REMINDER_MERGE_MODES_V169)]
                    settings["merge_mode"] = next_mode
                    settings["merge_enabled"] = next_mode != "off"
                else:
                    key = "show_complete_command"
                    settings[key] = not bool(settings.get(key, False))
                settings["updated_at"] = _v149_now_iso()
                tenant_google_persist(tid, "reminder_chat_settings_v149")
                page = int(parts[3]) if len(parts) > 3 and str(parts[3]).isdigit() else 0
                day_key = parts[4] if len(parts) > 4 else today_key()
                with tenant_context(tid):
                    reminder_text = build_reminder_list_text()
                    reminder_keyboard = build_reminder_list_keyboard(day_key, page)
                safe_edit(bot, call, reminder_text, reply_markup=reminder_keyboard)
                if action == "merge":
                    REMINDER_TASK_POOL.submit_unique("reminder-v149-batch", _v149_reminder_batch_job, None)
                try: bot.answer_callback_query(call.id, "Настройка обновлена")
                except Exception: pass
                return True
            if action == "done":
                rid = int(parts[3]); target_chat_id = int(parts[4])
                if target_chat_id != chat_id:
                    bot.answer_callback_query(call.id, "Кнопка относится к другому чату", show_alert=True); return True
                ok, text = _v149_complete_reminder(rid, chat_id, user_id, _v149_actor_label(call))
                try: bot.answer_callback_query(call.id, text[:180], show_alert=not ok)
                except Exception: pass
                if ok:
                    try:
                        safe_edit(bot, call, text)
                    except Exception:
                        try: bot.send_message(chat_id, text)
                        except Exception: pass
                return True
            if action == "history":
                if not tenant_can_manage(user_id, tid):
                    bot.answer_callback_query(call.id, "Недостаточно прав", show_alert=True); return True
                bot.send_message(chat_id, _v149_completion_history_text(tid))
                try: bot.answer_callback_query(call.id)
                except Exception: pass
                return True
    except Exception as exc:
        try:
            if data_str.startswith("v149:google:"):
                tid = str(tenant_id_for_chat(chat_id, create=False) or TENANT_PLATFORM_ID)
                tenant_google_error(tid, "callback", exc)
            bot.answer_callback_query(call.id, str(exc)[:180], show_alert=True)
        except Exception:
            pass
        return True
    return True
try: _v177_legacy_0266_v149_extension_callback.__name__ = 'v149_extension_callback'
except Exception: pass
v149_extension_callback = _v177_legacy_0266_v149_extension_callback
# v189_main_window_authority_final
