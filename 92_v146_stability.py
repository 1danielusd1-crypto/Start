# v146_window_epoch_lazy_refresh_durable
# ─────────────────────────────────────────────────────────────
# v146: строгий реестр окон + epoch-защита, ленивое обновление,
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
_V146_MALLOC_TRIM_COOLDOWN = max(30.0, min(600.0, float(os.getenv("MALLOC_TRIM_COOLDOWN_SECONDS", "90") or "90")))
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


def get_registered_open_window(chat_id: int, message_id: int) -> dict | None:
    rows = _v146_registry_rows_for_message(int(chat_id), int(message_id))
    if not rows:
        return None
    rows.sort(key=lambda pair: (
        int((pair[1] or {}).get("epoch") or 0),
        str((pair[1] or {}).get("updated_at") or ""),
    ), reverse=True)
    return dict(rows[0][1])


def window_registry_epoch(chat_id: int, message_id: int) -> int:
    try:
        return int((get_registered_open_window(int(chat_id), int(message_id)) or {}).get("epoch") or 0)
    except Exception:
        return 0


def register_open_window(chat_id: int, message_id: int, window_type: str, code: str = "", day_key: str | None = None, params: dict | None = None):
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
        save_data(data, root_only=True)
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
            "WARN" if changed_identity and previous else "INFO",
        )
    except Exception:
        pass
    return dict(new_row)


def unregister_open_window(chat_id: int, message_id: int):
    chat_id = int(chat_id); message_id = int(message_id)
    removed = []
    with _V146_WINDOW_LOCK:
        reg = _open_window_registry()
        for key, item in _v146_registry_rows_for_message(chat_id, message_id):
            removed.append(dict(item or {})); reg.pop(key, None)
        if removed:
            save_data(data, root_only=True)
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


def cleanup_open_window_registry(reason: str = "manual") -> dict:
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


def schedule_financial_window_refresh(chat_id: int, day_key: str | None = None, reason: str = "finance_changed", delay: float = 0.15):
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


def refresh_registered_financial_windows(chat_id: int):
    """v146 compatibility: no mass Telegram edits; schedule only primary windows and mark the rest dirty."""
    schedule_financial_window_refresh(int(chat_id), reason="registry_refresh")
    return True


def _finance_changed_now(chat_id: int, day_key: str | None = None, reason: str = "change"):
    chat_id = int(chat_id)
    day_key = str(day_key or get_chat_store(chat_id).get("current_view_day") or today_key())[:10]
    finance_cache_invalidate(chat_id, f"finance_changed:{reason}")
    with locked_chat(chat_id):
        store = get_chat_store(chat_id)
        store["current_view_day"] = day_key
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
        expected = task.get("expected_effects") or _durable_expected_effects(payload)
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
        details = [_v146_load_failed_task_detail(key, row) for key, row in rows]
        _V146_FAILED_TASK_DETAILS = details
        _V146_FAILED_TASK_DETAILS_AT = time.monotonic()
        try:
            bot_journal("failed_task_diagnostics_refreshed", None, json.dumps({"count": len(details), "tasks": details}, ensure_ascii=False, default=str)[:1800], "WARN" if details else "INFO")
        except Exception:
            pass
        return list(details)
    finally:
        _V146_FAILED_TASK_LOAD_LOCK.release()


def mega_task_registry_stats() -> dict:
    base = _V146_ORIG_MEGA_TASK_STATS() if callable(_V146_ORIG_MEGA_TASK_STATS) else {}
    base["failed_details"] = list(_V146_FAILED_TASK_DETAILS)
    base["failed_details_at"] = _V146_FAILED_TASK_DETAILS_AT
    base["failed_details_pending"] = bool(int(base.get("failed") or 0) and not _V146_FAILED_TASK_DETAILS)
    return base


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


def runtime_mark_ready(detail: str = ""):
    result = _V146_ORIG_RUNTIME_MARK_READY(detail) if callable(_V146_ORIG_RUNTIME_MARK_READY) else None
    try:
        DELAYED_SCHEDULER.schedule("v146-window-registry-cleanup", 2.0, cleanup_open_window_registry, "startup")
    except Exception:
        pass
    try:
        DELAYED_SCHEDULER.schedule("v146-failed-task-diagnostics", 8.0, refresh_failed_task_diagnostics, True)
    except Exception:
        pass
    return result


# Marker aliases for callbacks whose full payload has many dynamic segments.
try:
    WINDOW_MARKER_CONSTANTS.setdefault("exp_style_period", "Ф179")
    WINDOW_MARKER_CONSTANTS.setdefault("exp_new_period_send", "Ф180")
except Exception:
    pass

# v146_window_epoch_lazy_refresh_durable
