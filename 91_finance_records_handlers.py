# v182_restore_unified
# ─────────────────────────────────────────────────────────────
# v27: единая модель финансовых записей
# ─────────────────────────────────────────────────────────────
def _record_day_key(rec: dict) -> str:
    """Безопасно возвращает day_key для записи."""
    dk = rec.get("day_key")
    if dk:
        return str(dk)[:10]
    ts = rec.get("timestamp") or ""
    if isinstance(ts, str) and len(ts) >= 10 and re.match(r"\d{4}-\d{2}-\d{2}", ts[:10]):
        rec["day_key"] = ts[:10]
        return ts[:10]
    rec["day_key"] = today_key()
    return rec["day_key"]


def normalize_chat_records(chat_id: int) -> None:
    """
    v33: records — основной источник, daily_records строится из него.
    Сортировка стабильная: Telegram date + исходный message_id, чтобы 1 2 3 4 не превращалось в 1 2 4 3.
    """
    store = get_chat_store(chat_id)
    records = store.get("records")
    daily = store.get("daily_records") or {}

    if not isinstance(records, list) or not records:
        rebuilt = []
        for dk in sorted(daily.keys()):
            for rec in daily.get(dk, []) or []:
                if isinstance(rec, dict):
                    rec.setdefault("day_key", dk)
                    rebuilt.append(rec)
        records = rebuilt

    clean = []
    for rec in records or []:
        if not isinstance(rec, dict):
            continue
        rec.setdefault("timestamp", now_local().isoformat(timespec="seconds"))
        rec.setdefault("amount", 0)
        rec.setdefault("note", "")
        rec.setdefault("owner", "")
        rec.setdefault("source_order_msg_id", rec.get("source_msg_id") or rec.get("origin_msg_id") or rec.get("msg_id") or rec.get("id") or 0)
        _record_day_key(rec)
        try:
            if "ensure_finance_record_uid" in globals(): ensure_finance_record_uid(int(chat_id), rec)
        except Exception: pass
        clean.append(rec)

    clean.sort(key=record_sort_key)
    store["records"] = clean

    rebuilt_daily = {}
    for rec in clean:
        rebuilt_daily.setdefault(_record_day_key(rec), []).append(rec)
    store["daily_records"] = rebuilt_daily


def recalc_balance(chat_id: int):
    normalize_chat_records(chat_id)
    store = get_chat_store(chat_id)
    store["balance"] = sum(float(r.get("amount", 0) or 0) for r in store.get("records", []))


def rebuild_month_short_ids(chat_id: int):
    """Пересчитывает short_id как месячную нумерацию по стабильной хронологии."""
    normalize_chat_records(chat_id)
    store = get_chat_store(chat_id)
    daily = store.get("daily_records", {}) or {}
    month_counters = {}
    usd_month_counters = {}

    for dk in sorted(daily.keys()):
        month_key = dk[:7]
        month_counters.setdefault(month_key, 1)
        usd_month_counters.setdefault(month_key, 1)
        recs = sorted(daily.get(dk, []) or [], key=record_sort_key)
        daily[dk] = recs
        for r in recs:
            try:
                if "ensure_finance_record_uid" in globals(): ensure_finance_record_uid(int(chat_id), r)
            except Exception: pass
            has_usd = bool(float(r.get("usd_amount", 0) or 0))
            usd_only = bool(r.get("usd_only", False))
            if not usd_only:
                r["short_id"] = f"R{month_counters[month_key]}"
                month_counters[month_key] += 1
            elif has_usd:
                r["short_id"] = f"U{usd_month_counters[month_key]}"
            if has_usd:
                r["usd_short_id"] = f"U{usd_month_counters[month_key]}"
                usd_month_counters[month_key] += 1

    store["records"] = [r for dk in sorted(daily.keys()) for r in daily.get(dk, [])]


def calc_day_balance(store: dict, day_key: str) -> float:
    total = 0.0
    daily = store.get("daily_records", {}) or {}
    for dk in sorted(daily.keys()):
        if dk > day_key:
            break
        for r in daily.get(dk, []) or []:
            total += float(r.get("amount", 0) or 0)
    return total


def rebuild_global_records():
    """Быстрый общий итог без копирования всех записей всех чатов при каждом сообщении."""
    with data_lock:
        total = 0.0
        for _cid, store in (data.get("chats", {}) or {}).items():
            try:
                if "balance" in store:
                    total += float(store.get("balance", 0) or 0)
                else:
                    total += sum(float(r.get("amount", 0) or 0) for r in (store.get("records", []) or []))
            except Exception:
                pass
        # Полные записи уже находятся в chats; дублировать их в root больше не нужно.
        data["records"] = []
        data["overall_balance"] = total

_finalize_timers = {}
_backup_timers = {}
_quick_backup_timers = {}
_balance_panel_refresh_timers = {}
_balance_panel_collapse_timers = {}
_balance_panel_first_timers = {}
_balance_panel_recreate_timers = {}
_total_message_timers = {}
_backup_dirty_chats = set()
_quick_backup_dirty_chats = set()
_global_mega_timer = None

def collect_finance_chat_ids():
    ids = set()
    try:
        for cid, enabled in (data.get("finance_active_chats", {}) or {}).items():
            if enabled:
                ids.add(int(cid))
    except Exception:
        pass
    try:
        for cid in list(finance_active_chats):
            ids.add(int(cid))
    except Exception:
        pass
    try:
        for cid, store in (data.get("chats", {}) or {}).items():
            try:
                int_cid = int(cid)
            except Exception:
                continue
            if store.get("finance_mode") or (OWNER_ID and str(int_cid) == str(OWNER_ID)):
                ids.add(int_cid)
    except Exception:
        pass
    return sorted(ids)


def schedule_startup_main_windows(delay: float = 3.0):
    """v108: restore only automatic finance windows that were actually open before deploy."""
    def _job():
        try:
            for cid in collect_finance_chat_ids():
                try:
                    if is_chat_bot_removed(cid):
                        continue
                    store = get_chat_store(cid)
                    state = _finance_window_state(cid)
                    mode = finance_window_mode(cid)
                    if mode == "off" or not bool(state.get("auto_reopen_on_boot", False)):
                        continue
                    day_key = store.get("current_view_day") or today_key()
                    if mode == "normal":
                        update_or_send_day_window(cid, day_key)
                    elif mode in {"open", "first"}:
                        if store.get("balance_panel_id"):
                            refresh_balance_panel_now(cid)
                        else:
                            send_minimized_balance_panel(cid)
                        if mode == "first":
                            schedule_quick_balance_first_recreate(cid, 60.0)
                    time.sleep(0.20)
                except Exception as e:
                    log_error(f"startup_finance_window({get_chat_display_name(cid)}): {e}")
        except Exception as e:
            log_error(f"schedule_startup_main_windows job: {e}")

    try:
        DELAYED_SCHEDULER.schedule("startup-main-windows", delay, _job)
    except Exception as e:
        log_error(f"schedule_startup_main_windows: {e}")

def schedule_all_finance_backups(delay: float = 10.0):
    for cid in collect_finance_chat_ids():
        schedule_backup_flush(cid, delay=delay)


def _schedule_global_mega_snapshot(delay: float = 30.0):
    """Совместимость старых вызовов: v90 лишь отмечает pending full snapshot.

    Полный global больше не создаётся через 20–30 секунд после каждого чата.
    Его запускает общий quiet/max scheduler.
    """
    _mark_global_snapshot_pending()


def _run_quick_chat_backup(chat_id: int):
    """v90 quick backup = маленький immutable delta, а не полная копия чата/global."""
    chat_id = int(chat_id)
    if RESTORE_GUARD_ACTIVE:
        log_error(f"QUICK DELTA BLOCKED {chat_id}: {RESTORE_GUARD_REASON}")
        return
    with state_chat_context(chat_id):
        try:
            save_data(data, chat_ids=[chat_id])
            with _delta_state_lock:
                _delta_pending_chats.add(chat_id)
                if not _delta_chat_generation.get(chat_id):
                    _delta_chat_generation[chat_id] = int(time.time_ns())
            if not _run_delta_batch():
                schedule_delta_backup(chat_id, BACKUP_BUSY_RETRY_SECONDS, reason="delta_retry")
        finally:
            with timer_lock:
                _quick_backup_dirty_chats.discard(chat_id)


def _run_full_chat_backup(chat_id: int):
    chat_id = int(chat_id)
    if RESTORE_GUARD_ACTIVE:
        log_error(f"FULL BACKUP BLOCKED {chat_id}: {RESTORE_GUARD_REASON}")
        return
    with state_chat_context(chat_id):
        try:
            if not is_finance_mode(chat_id):
                return
            if not is_auto_backup_enabled(chat_id):
                return
            save_data(data, chat_ids=[chat_id])
            save_chat_json(chat_id)

            # Канал/личный чат работают как раньше, но MEGA-файлы теперь заменяются
            # через candidate -> history -> move, без предварительного удаления.
            if is_backup_to_chat_enabled(chat_id) and can_receive_direct_json_backup(chat_id) and not is_finance_output_suppressed(chat_id):
                send_backup_to_chat(chat_id, ensure_files=False)
            if is_backup_to_channel_enabled(chat_id):
                send_backup_to_channel(chat_id, ensure_files=False)
            if is_backup_to_mega_enabled(chat_id):
                mega_upload_chat_backup_bundle(chat_id, current_month_key())
                _mark_global_snapshot_pending()
        except Exception as exc:
            log_error(f"_run_full_chat_backup({chat_id}): {exc}")
        finally:
            with timer_lock:
                _backup_dirty_chats.discard(chat_id)
                _backup_timers.pop(chat_id, None)
            try:
                _lowram_release_chat(chat_id)
            except Exception as _lr_exc:
                log_error(f"LOWRAM full-backup release {chat_id}: {_lr_exc}")


def schedule_quick_backup(chat_id: int, delay: float | None = None):
    """Debounce delta for one chat. Critical toggle callbacks defer async delta until their sync commit.

    Without this tiny guard, an async delta could persist the toggled state a fraction of a second
    before its idempotency marker. A deploy in that microscopic gap could replay the toggle twice.
    """
    chat_id = int(chat_id)
    try:
        ctx = getattr(_TELEGRAM_UPDATE_CONTEXT, "value", None)
        if isinstance(ctx, dict) and ctx.get("critical_callback"):
            ctx.setdefault("deferred_quick_chats", set()).add(chat_id)
            return
    except Exception:
        pass
    if RESTORE_GUARD_ACTIVE:
        return
    if delay is None:
        delay = MEGA_DELTA_PRIORITY_DELAY_SECONDS if mega_backup_priority_enabled() else MEGA_DELTA_DELAY_SECONDS
    due = time.time() + max(0.5, float(delay))
    with timer_lock:
        _quick_backup_dirty_chats.add(chat_id)
        _quick_backup_timers[chat_id] = due
    with _delta_state_lock:
        global _delta_generation
        _delta_generation += 1
        _delta_pending_chats.add(chat_id)
        _delta_chat_generation[chat_id] = _delta_generation

    def _fire():
        # Одна общая задача заберёт изменения всех чатов, накопившиеся к этому моменту.
        def _job():
            if not _run_delta_batch():
                schedule_delta_backup(None, delay=BACKUP_BUSY_RETRY_SECONDS, reason="quick_upload_retry")
        if not DELTA_TASK_POOL.submit("mega-delta-v90", _job):
            log_error(f"QUICK DELTA QUEUE FULL, RETRY: {chat_id}")
            schedule_quick_backup(chat_id, BACKUP_BUSY_RETRY_SECONDS)
    DELAYED_SCHEDULER.cancel("mega-delta-batch-v90")
    DELAYED_SCHEDULER.schedule("mega-delta-batch-v90", max(0.5, float(delay)), _fire)


def schedule_full_backup_only(chat_id: int, delay: float = 3.0):
    """Тяжёлый JSON/канал/MEGA-файл чата — отдельно от быстрого delta."""
    chat_id = int(chat_id)
    if RESTORE_GUARD_ACTIVE:
        log_error(f"FULL BACKUP SCHEDULE BLOCKED {chat_id}: {RESTORE_GUARD_REASON}")
        return
    try:
        delay = max(float(delay or 0), BACKUP_MIN_DELAY_SECONDS)
    except Exception:
        delay = BACKUP_MIN_DELAY_SECONDS
    due = time.time() + delay
    with timer_lock:
        _backup_dirty_chats.add(chat_id)
        _backup_timers[chat_id] = due
    def _fire():
        with timer_lock:
            _backup_timers.pop(chat_id, None)
        if not BACKUP_TASK_POOL.submit(f"full:{chat_id}", _run_full_chat_backup, chat_id):
            log_error(f"FULL BACKUP QUEUE FULL, RETRY: {chat_id}")
            schedule_full_backup_only(chat_id, BACKUP_BUSY_RETRY_SECONDS)
    # Один ключ на чат: серия правок объединяется в один тяжёлый backup.
    DELAYED_SCHEDULER.cancel(f"full-backup:{chat_id}")
    DELAYED_SCHEDULER.schedule(f"full-backup:{chat_id}", delay, _fire)


def schedule_backup_flush(chat_id: int, delay: float = 3.0):
    """SQLite уже сохранена; delta быстро; полный файл чата после 120 сек. тишины."""
    chat_id = int(chat_id)
    if RESTORE_GUARD_ACTIVE:
        log_error(f"BACKUP SCHEDULE BLOCKED {chat_id}: {RESTORE_GUARD_REASON}")
        return
    quick_delay = MEGA_DELTA_PRIORITY_DELAY_SECONDS if mega_backup_priority_enabled() else MEGA_DELTA_DELAY_SECONDS
    schedule_quick_backup(chat_id, quick_delay)
    schedule_full_backup_only(chat_id, delay)

def _safe_stabilize(action_name, func):
    try:
        return func()
    except Exception as e:
        log_error(f"[STABILIZE ERROR] {action_name}: {e}")
        try:
            bot_journal("stabilize_error", None, f"{action_name}: {e}", "ERROR")
        except Exception:
            pass
        return None


def _v177_legacy_0237_finance_changed_now(chat_id: int, day_key: str | None = None, reason: str = "change"):
    """
    Единая точка после фин-изменения.
    Важно: Telegram-отправки/редактирования окон и бэкапы не держат chat_lock,
    чтобы кнопки в этом же чате не висели «Загрузка».
    """
    chat_id = int(chat_id)
    day_key = day_key or get_chat_store(chat_id).get("current_view_day") or today_key()
    try:
        finance_cache_invalidate(chat_id, f"finance_changed:{reason}")
    except Exception:
        pass

    try:
        with locked_chat(chat_id):
            store = get_chat_store(chat_id)
            store["current_view_day"] = day_key

            _safe_stabilize("normalize_chat_records", lambda: normalize_chat_records(chat_id))

            _safe_stabilize("recalc_balance", lambda: recalc_balance(chat_id))

            _safe_stabilize("rebuild_month_short_ids", lambda: rebuild_month_short_ids(chat_id))

            _safe_stabilize("rebuild_global_records", rebuild_global_records)

            _safe_stabilize("currency_ledger_snapshot", lambda: _snapshot_active_currency_ledger(store, _ensure_currency_ledgers(store)))
            _safe_stabilize("save_data", lambda: save_data(data, chat_ids=[chat_id]))

            hidden = is_finance_output_suppressed(chat_id)
            visible_window_mode = finance_window_mode(chat_id)

        # v90: сразу после подтверждённой SQLite ставим маленький delta, ДО Telegram-окон.
        # Поэтому медленное редактирование интерфейса не откладывает аварийную копию.
        _safe_stabilize(
            "delta_queue_early",
            lambda: schedule_quick_backup(
                chat_id,
                MEGA_DELTA_PRIORITY_DELAY_SECONDS if mega_backup_priority_enabled() else MEGA_DELTA_DELAY_SECONDS,
            ),
        )

        # Ниже тяжёлые Telegram-вызовы уже вне chat_lock.
        # v108: hidden finance suppresses ordinary finance chatter, but it no longer disables
        # a separately selected automatic window mode.  The three visible modes remain exclusive.
        if visible_window_mode == "normal":
            # Refresh an existing О1, but do not create an unscheduled one here; the 10-message
            # counter owns creation/recreation for this mode.
            if get_active_window_id(chat_id, day_key):
                if is_owner_chat(chat_id):
                    _safe_stabilize("owner_window", lambda: backup_window_for_owner(chat_id, day_key, None))
                else:
                    _safe_stabilize("day_window", lambda: update_or_send_day_window(chat_id, day_key))
        elif visible_window_mode in {"open", "first"}:
            _safe_stabilize("quick_balance_now", lambda: refresh_balance_panel_now(chat_id))
            _safe_stabilize("quick_balance_schedule", lambda: schedule_balance_panel_refresh(chat_id, BALANCE_PANEL_REFRESH_DELAY))

        if not hidden:
            # Manually opened totals/auxiliary financial views can still refresh when finance is not hidden.
            _safe_stabilize("refresh_total", lambda: refresh_total_message_if_any(chat_id))

        # Реестр обновляем даже для скрытого финрежима: сам скрытый чат может не показывать финансы,
        # но открытое у владельца окно этого чата обязано синхронизироваться.
        _safe_stabilize("open_windows_registry", lambda: refresh_registered_financial_windows(chat_id))

        _safe_stabilize("full_backup_queue", lambda: schedule_full_backup_only(chat_id, BACKUP_MIN_DELAY_SECONDS))

        # Важно: действия в других чатах не должны менять личное окно владельца.
        # Поэтому здесь не вызываем backup_window_for_owner/refresh_owner_after_chat_change.

    except Exception as e:
        raise
try: _v177_legacy_0237_finance_changed_now.__name__ = '_finance_changed_now'
except Exception: pass
_finance_changed_now = _v177_legacy_0237_finance_changed_now


def _v177_legacy_0238_finance_changed(chat_id: int, day_key: str | None = None, reason: str = "change", delay: float = 0.35):
    """Debounced универсальный финальный пересчёт для одного чата."""
    chat_id = int(chat_id)
    bot_journal("finance_changed_scheduled", chat_id, f"day={day_key} reason={reason} delay={delay}")
    day_key = day_key or get_chat_store(chat_id).get("current_view_day") or today_key()

    def _job():
        if not FINANCE_TASK_POOL.submit(chat_id, _finance_changed_now, chat_id, day_key, reason):
            log_error(f"FINANCE QUEUE FULL, RETRY: {chat_id}")
            with timer_lock:
                _finalize_timers[chat_id] = time.time() + 1.0
            DELAYED_SCHEDULER.schedule(f"finance-finalize:{chat_id}", 1.0, _fire_finance)

    with timer_lock:
        _finalize_timers[chat_id] = time.time() + max(0.0, float(delay))
    def _fire_finance():
        with timer_lock:
            _finalize_timers.pop(chat_id, None)
        _job()
    DELAYED_SCHEDULER.schedule(f"finance-finalize:{chat_id}", delay, _fire_finance)
try: _v177_legacy_0238_finance_changed.__name__ = 'finance_changed'
except Exception: pass
finance_changed = _v177_legacy_0238_finance_changed


def _v177_legacy_0239_schedule_finalize(chat_id: int, day_key: str, delay: float = 0.35):
    """Совместимость со старым кодом: теперь всё идёт через finance_changed()."""
    return finance_changed(chat_id, day_key, reason="schedule_finalize", delay=delay)
try: _v177_legacy_0239_schedule_finalize.__name__ = 'schedule_finalize'
except Exception: pass
schedule_finalize = _v177_legacy_0239_schedule_finalize


def _v177_legacy_0240_backup_window_for_owner(chat_id: int, day_key: str, message_id_override: int | None = None):
    """
    Окно дня для владельца без document-caption.
    JSON-бэкапы отправляются отдельно через schedule_backup_flush().
    """
    lock = window_locks[(chat_id, day_key)]

    with lock:
        txt, _ = render_day_window(chat_id, day_key)
        kb = build_main_keyboard(day_key, chat_id)

        if len(txt) > 3900:
            log_error(f"backup_window_for_owner: text too long for {chat_id} {day_key}, len={len(txt)}")

        mid = message_id_override or get_active_window_id(chat_id, day_key)
        if message_id_override:
            try:
                set_active_window_id(chat_id, day_key, message_id_override)
            except Exception:
                pass

        if mid:
            try:
                result = fast_ui_edit_message_text(
                    chat_id, mid, txt, reply_markup=kb, parse_mode="HTML",
                    purpose="main_day_background_v178",
                )
            except Exception:
                result = "failed"
            if str(result or "") in {"ok", "scheduled"}:
                set_active_window_id(chat_id, day_key, mid)
                return result
            if str(result or "") == "not_found":
                try:
                    aw = get_or_create_active_windows(chat_id)
                    if aw.get(day_key) == mid:
                        aw.pop(day_key, None)
                        save_data(data)
                except Exception:
                    pass
                try:
                    delete_async = globals().get("v177_delete_message_async")
                    if callable(delete_async): delete_async(chat_id, mid, "main_day_replace_v178")
                except Exception:
                    pass
            elif str(result or "") not in {"rate_limited", "failed"}:
                return result

        sent = bot.send_message(
            chat_id,
            txt,
            reply_markup=kb,
            parse_mode="HTML"
        )
        set_active_window_id(chat_id, day_key, sent.message_id)
try: _v177_legacy_0240_backup_window_for_owner.__name__ = 'backup_window_for_owner'
except Exception: pass
backup_window_for_owner = _v177_legacy_0240_backup_window_for_owner

def cancel_auto_delete_for_message(chat_id: int, message_id: int):
    """Если окно с автоудалением превращается кнопкой «Назад» в основное — его старый таймер больше не должен удалить О1."""
    chat_id = int(chat_id)
    message_id = int(message_id)
    try:
        _cancel_v98_auto_close(chat_id, message_id)
    except Exception:
        pass
    for key in (
        f"auto-delete:{chat_id}:{message_id}",
        f"auto-delete-html:{chat_id}:{message_id}",
        f"delete-later:{chat_id}:{message_id}",
    ):
        try:
            DELAYED_SCHEDULER.cancel(key)
        except Exception:
            pass
    try:
        store = get_chat_store(chat_id)
        was_total_window = int(store.get("total_msg_id") or 0) == message_id
        # Отменяем только реальные stored-window таймеры этого чата, а не перебираем
        # все числовые поля store (id записи/флаги не должны случайно очищаться).
        for timer_key in list(_aux_window_timers.keys()):
            try:
                timer_chat_id, store_key = timer_key
                if int(timer_chat_id) != chat_id:
                    continue
                if int(store.get(str(store_key)) or 0) != message_id:
                    continue
            except Exception:
                continue
            DELAYED_SCHEDULER.cancel(f"stored-window-delete:{chat_id}:{store_key}")
            _aux_window_timers.pop(timer_key, None)
            store[str(store_key)] = None

        if was_total_window:
            DELAYED_SCHEDULER.cancel(f"owner-total-delete:{chat_id}")
            _total_message_timers.pop(chat_id, None)
            store["total_msg_id"] = None
        # Сохранение сделает уже существующая фоновая очистка back_main; здесь важно
        # не задерживать мгновенное возвращение в основное окно.
    except Exception as e:
        log_error(f"cancel_auto_delete_for_message({chat_id},{message_id}): {e}")


def recreate_main_window_now(chat_id: int, day_key: str):
    """Удаляет старое о1, если возможно, и создаёт новое основное окно."""
    try:
        old_mid = get_active_window_id(chat_id, day_key)
        if old_mid:
            try:
                bot.delete_message(chat_id, int(old_mid))
            except Exception:
                pass
            try:
                clear_active_window_id(chat_id, day_key)
            except Exception:
                pass
    except Exception:
        pass
    force_new_day_window(chat_id, day_key)


def _v177_legacy_0241_force_new_day_window(chat_id: int, day_key: str):
    # v108: hidden accounting no longer forbids an explicitly requested visible main window.
    txt, _ = render_day_window(chat_id, day_key)
    kb = build_main_keyboard(day_key, chat_id)
    sent = bot.send_message(chat_id, txt, reply_markup=kb, parse_mode="HTML")
    set_active_window_id(chat_id, day_key, sent.message_id)
    schedule_balance_panel_refresh(chat_id, 0.5)
try: _v177_legacy_0241_force_new_day_window.__name__ = 'force_new_day_window'
except Exception: pass
force_new_day_window = _v177_legacy_0241_force_new_day_window


def _v177_legacy_0242_return_to_main_window_closing_previous(chat_id: int, day_key: str, current_message_id: int | None = None):
    """Return to О1 without promoting a missing/stale Telegram message to active."""
    chat_id = int(chat_id)
    try:
        current_message_id = int(current_message_id) if current_message_id is not None else None
    except Exception:
        current_message_id = None

    try:
        if current_message_id is not None:
            cancel_auto_delete_for_message(chat_id, current_message_id)
            cancel_fast_ui_edit(chat_id, current_message_id)
    except Exception:
        pass

    try:
        old_mid = get_active_window_id(chat_id, day_key)
        old_mid = int(old_mid) if old_mid else None
    except Exception:
        old_mid = None

    txt, _ = render_day_window(chat_id, day_key)
    kb = build_main_keyboard(day_key, chat_id)

    if current_message_id is not None:
        result = fast_ui_edit_message_text(
            chat_id, current_message_id, txt,
            reply_markup=kb, parse_mode="HTML", purpose="back_main_instant",
        )
        bot_journal("back_main_fast", chat_id, f"day={day_key} result={result} old={old_mid} current={current_message_id}")

        if result == "ok":
            # Only a proven Telegram edit may become the active main window.
            set_active_window_id(chat_id, day_key, current_message_id)
            if old_mid and old_mid != current_message_id:
                def _delete_old():
                    try:
                        _tg_call_retry(bot.delete_message, chat_id, int(old_mid), attempts=1, purpose="back_main_delete_old")
                    except Exception:
                        pass
                    finally:
                        try:
                            unregister_open_window(chat_id, int(old_mid))
                        except Exception:
                            pass
                GENERAL_TASK_POOL.submit(f"back-delete:{chat_id}:{old_mid}", _delete_old)
            schedule_balance_panel_refresh(chat_id, 0.05)
            return

        # A delayed/stale callback can reference a message already closed by its timer.
        # Retire that id, but preserve the valid old main window instead of deleting it.
        if result == "not_found":
            try:
                unregister_open_window(chat_id, current_message_id)
            except Exception:
                pass
            try:
                if get_active_window_id(chat_id, day_key) == current_message_id:
                    clear_active_window_id(chat_id, day_key)
                    old_mid = None
            except Exception:
                pass

        if old_mid and old_mid != current_message_id:
            def _refresh_existing():
                try:
                    backup_window_for_owner(chat_id, day_key, message_id_override=old_mid)
                except Exception as exc:
                    log_error(f"back_main preserve old({chat_id},{day_key},{old_mid}): {exc}")
            GENERAL_TASK_POOL.submit(f"back-preserve:{chat_id}:{old_mid}", _refresh_existing)
            return

    def _send_fallback():
        try:
            update_or_send_day_window(chat_id, day_key)
        except Exception as e:
            log_error(f"return_to_main fallback({chat_id},{day_key}): {e}")
    if not GENERAL_TASK_POOL.submit(f"back-send:{chat_id}", _send_fallback):
        _send_fallback()
try: _v177_legacy_0242_return_to_main_window_closing_previous.__name__ = 'return_to_main_window_closing_previous'
except Exception: pass
return_to_main_window_closing_previous = _v177_legacy_0242_return_to_main_window_closing_previous


def reset_chat_data(chat_id: int):
    """v27: обнуление данных чата без ручного дублирования окон/бэкапов."""
    try:
        with locked_chat(chat_id):
            store = get_chat_store(chat_id)
            cleanup_forward_links(chat_id)
            store["balance"] = 0
            store["records"] = []
            store["daily_records"] = {}
            store["next_id"] = 1
            store["active_windows"] = {}
            clear_edit_wait_state(chat_id, delete_prompt=True)
            store["edit_target"] = None
            store["reset_wait"] = False
            store["reset_time"] = 0
            day_key = store.get("current_view_day", today_key())
            save_data(data)
        finance_changed(chat_id, day_key, reason="reset", delay=0.1)
    except Exception as e:
        log_error(f"reset_chat_data({chat_id}): {e}")


@bot.message_handler(content_types=["document"])
def handle_document(msg):
    global restore_mode, data

    chat_id = msg.chat.id
    update_chat_info_from_message(msg)
    if handle_secret_input_message(msg):
        return
    try:
        if not getattr(getattr(msg, "from_user", None), "is_bot", False):
            bump_quick_balance_recreate_counter(chat_id)
            stop_dozvon_for_target(chat_id)
    except Exception:
        pass

    file = msg.document
    fname = (file.file_name or "").lower()

    log_info(f"[DOC] recv chat={chat_id} restore={restore_mode} fname={fname}")

    # Владелец прислал .json без /restore: спрашиваем, обновлять данные или нет.
    # Если показали вопрос — сам файл дальше не пересылаем и не обрабатываем как обычный документ.
    if restore_mode is None and is_owner_chat(chat_id) and fname.endswith((".json", ".ison")):
        if maybe_prompt_owner_for_json_restore(msg, fname):
            return

    if restore_mode is not None and restore_mode == chat_id:

        if not (fname.endswith(".json") or fname.endswith(".ison") or fname.endswith(".csv") or fname.endswith(".gz")):
            send_and_auto_delete(
                chat_id,
                "⚠️ В режиме восстановления принимаются GZ / JSON / ISON / CSV."
            )
            return

        if fname.endswith(".gz"):
            try:
                prep = globals().get("v182_prepare_gz_restore_document")
                if not callable(prep):
                    raise RuntimeError("GZ restore helper не загружен")
                prep(msg, file)
            except Exception as e:
                send_and_auto_delete(chat_id, f"❌ GZ не подготовлен: {e}", 15)
            return

        tmp_path = f"restore_{chat_id}_{fname}"
        try:
            file_info = bot.get_file(file.file_id)
            stream_fn = globals().get("telegram_download_to_file")
            if callable(stream_fn):
                max_restore = max(1024 * 1024, int(os.getenv("RESTORE_FILE_MAX_BYTES", str(100 * 1024 * 1024)) or str(100 * 1024 * 1024)))
                stream_fn(file_info.file_path, tmp_path, max_bytes=max_restore)
            else:
                raw = bot.download_file(file_info.file_path)
                with open(tmp_path, "wb") as f:
                    f.write(raw)
                raw = None
        except Exception as e:
            try:
                if os.path.exists(tmp_path): os.remove(tmp_path)
            except Exception:
                pass
            send_and_auto_delete(chat_id, f"❌ Ошибка скачивания: {e}")
            return

        backup_dir = ""
        try:
            backup_fn = globals().get("_v153_backup_before_restore")
            if callable(backup_fn):
                backup_dir = backup_fn()
        except Exception as e:
            log_error(f"pre_restore backup before file restore: {e}")

        try:
            if fname in {"data.json", "data.ison"}:
                os.replace(tmp_path, DATA_FILE)
                _import_legacy_global_json_to_db(DATA_FILE, force=True)
                data = load_data()

                finance_active_chats.clear()
                fac = data.get("finance_active_chats") or {}
                for cid, enabled in fac.items():
                    if enabled:
                        try:
                            finance_active_chats.add(int(cid))
                        except Exception:
                            pass

                restore_mode = None
                data.pop("_restore_mode_chat_v150", None)
                save_data(data, chat_ids=[chat_id])
                send_and_auto_delete(chat_id, "🟢 Глобальный JSON импортирован в SQLite!")
                return

            if fname == "csv_meta.json":
                os.replace(tmp_path, CSV_META_FILE)
                _save_csv_meta(_load_json(CSV_META_FILE, {}) or {})
                restore_mode = None
                data.pop("_restore_mode_chat_v150", None)
                save_data(data, chat_ids=[chat_id])
                send_and_auto_delete(chat_id, "🟢 csv_meta.json импортирован в SQLite")
                return

            if fname.endswith((".json", ".ison")):
                payload = _load_json(tmp_path, None)
                if not isinstance(payload, dict):
                    raise RuntimeError("JSON не является объектом")

                if "chats" in payload:
                    os.replace(tmp_path, DATA_FILE)
                    _import_legacy_global_json_to_db(DATA_FILE, force=True)
                    data.clear()
                    data.update(load_data())
                    restore_mode = None
                    data.pop("_restore_mode_chat_v150", None)
                    save_data(data, chat_ids=[chat_id])
                    send_and_auto_delete(chat_id, "🟢 Глобальный JSON импортирован в SQLite")
                    return

                inner_chat_id = payload.get("chat_id")
                if inner_chat_id is None:
                    raise RuntimeError("В JSON нет chat_id")

                if int(inner_chat_id) != int(chat_id):
                    raise RuntimeError(
                        f"JSON относится к чату {get_chat_display_name(int(inner_chat_id))}, а не к текущему {get_chat_display_name(chat_id)}"
                    )

                restore_from_json(chat_id, tmp_path)

                day_key = get_chat_store(chat_id).get(
                    "current_view_day",
                    today_key()
                )
                finance_changed(chat_id, day_key, reason="restore_json", delay=0.1)

                restore_mode = None
                data.pop("_restore_mode_chat_v150", None)
                save_data(data, chat_ids=[chat_id])
                send_and_auto_delete(
                    chat_id,
                    f"🟢 JSON чата {get_chat_display_name(chat_id)} восстановлен"
                )
                return

            if fname.startswith("data_") and fname.endswith(".csv"):
                restore_from_csv(chat_id, tmp_path)

                day_key = get_chat_store(chat_id).get(
                    "current_view_day",
                    today_key()
                )
                finance_changed(chat_id, day_key, reason="restore_csv", delay=0.1)

                restore_mode = None
                data.pop("_restore_mode_chat_v150", None)
                save_data(data, chat_ids=[chat_id])
                send_and_auto_delete(
                    chat_id,
                    f"🟢 CSV чата восстановлен ({fname})"
                )
                return

            send_and_auto_delete(chat_id, f"⚠️ Неизвестный файл: {fname}")

        except Exception as e:
            send_and_auto_delete(chat_id, f"❌ Ошибка восстановления: {e}")

        finally:
            try:
                os.remove(tmp_path)
            except Exception:
                pass
            try:
                if backup_dir:
                    shutil.rmtree(backup_dir, ignore_errors=True)
            except Exception:
                pass

        return

    try:
        schedule_forward_any_message(chat_id, msg)
    except Exception as e:
        log_error(f"handle_document forward failed: {e}")


def cleanup_forward_links(chat_id: int):
    """
    Удаляет все связи пересылки для чата из памяти и из сохранённого индекса.
    """
    _cleanup_forward_storage_for_chat(chat_id)

KEEP_ALIVE_SEND_TO_OWNER = False
KEEP_ALIVE_STATE = {
    "started_at": None,
    "last_attempt_at": None,
    "last_ok_at": None,
    "last_error": "",
    "last_status_code": None,
    "ok_count": 0,
    "fail_count": 0,
    "telegram_ok_at": None,
    # v118 distinguishes the bot's own loopback request from a truly external monitor.
    "self_ping_at": None,
    "external_ping_at": None,
    "external_monitor_at": None,
    "last_keepalive_user_agent": "",
}
_keep_alive_thread = None
_keep_alive_thread_lock = threading.RLock()


def _keep_alive_base_candidates() -> list[str]:
    result = []
    extra = os.getenv("KEEP_ALIVE_URLS", "")
    values = [APP_URL, WEBHOOK_URL, os.getenv("RENDER_EXTERNAL_URL", "").strip(), _RENDER_HOST_URL]
    if extra:
        values.extend(x.strip() for x in extra.split(","))
    for raw in values:
        if not raw:
            continue
        base = str(raw).strip().rstrip("/")
        if base and base not in result:
            result.append(base)
    return result


def keep_alive_task():
    session = requests.Session()
    cycle = 0
    KEEP_ALIVE_STATE["started_at"] = _journal_ts()
    while True:
        cycle_started = time.time()
        try:
            if not KEEP_ALIVE_ENABLED:
                time.sleep(max(20, KEEP_ALIVE_INTERVAL_SECONDS))
                continue

            KEEP_ALIVE_STATE["last_attempt_at"] = _journal_ts()
            bases = _keep_alive_base_candidates()
            ok = False
            last_error = ""
            last_code = None

            for base in bases:
                for path in ("/keepalive", "/healthz", "/"):
                    url = f"{base}{path}?ts={int(time.time() * 1000)}"
                    try:
                        resp = session.get(url, timeout=12, headers={"Cache-Control": "no-cache", "User-Agent": f"{VERSION}-keepalive"})
                        last_code = int(resp.status_code)
                        if 200 <= resp.status_code < 500:
                            ok = True
                            break
                        last_error = f"HTTP {resp.status_code} {url}"
                    except Exception as e:
                        last_error = f"{url}: {e}"
                if ok:
                    break

            cycle += 1
            if cycle % KEEP_ALIVE_TELEGRAM_EVERY == 0:
                try:
                    bot.get_me()
                    KEEP_ALIVE_STATE["telegram_ok_at"] = _journal_ts()
                except Exception as e:
                    last_error = (last_error + " | " if last_error else "") + f"Telegram getMe: {e}"

            KEEP_ALIVE_STATE["last_status_code"] = last_code
            if ok:
                KEEP_ALIVE_STATE["last_ok_at"] = _journal_ts()
                KEEP_ALIVE_STATE["last_error"] = ""
                KEEP_ALIVE_STATE["ok_count"] = int(KEEP_ALIVE_STATE.get("ok_count", 0)) + 1
                if cycle == 1 or cycle % 20 == 0:
                    log_info(f"Keep-alive OK: status={last_code}, bases={len(bases)}")
            else:
                KEEP_ALIVE_STATE["last_error"] = last_error or "APP_URL / WEBHOOK_URL не заданы"
                KEEP_ALIVE_STATE["fail_count"] = int(KEEP_ALIVE_STATE.get("fail_count", 0)) + 1
                log_error(f"Keep-alive failed: {KEEP_ALIVE_STATE['last_error']}")
        except Exception as e:
            KEEP_ALIVE_STATE["last_error"] = str(e)[:500]
            KEEP_ALIVE_STATE["fail_count"] = int(KEEP_ALIVE_STATE.get("fail_count", 0)) + 1
            log_error(f"Keep-alive loop error: {e}")

        elapsed = time.time() - cycle_started
        time.sleep(max(20.0, float(KEEP_ALIVE_INTERVAL_SECONDS) - elapsed))

@bot.channel_post_handler(content_types=[
    "text", "photo", "video", "animation", "audio",
    "voice", "video_note", "document",
    "sticker", "location", "venue", "contact", "dice", "poll",
    # v107: keep forwarding coverage aligned with ordinary messages.
    "game", "story", "paid_media", "invoice"
])
def on_any_channel_post(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception as e:
        log_error(f"channel_post update_chat_info failed: {e}")

    if handle_secret_sequence(msg):
        return
    if handle_secret_input_message(msg):
        return

    try:
        bump_quick_balance_recreate_counter(msg.chat.id)
    except Exception:
        pass

    try:
        stop_dozvon_for_target(msg.chat.id)
    except Exception:
        pass

    try:
        if is_finance_mode(msg.chat.id):
            handle_finance_text(msg)
    except Exception as e:
        log_error(f"channel_post finance failed: {e}")

    try:
        schedule_forward_any_message(msg.chat.id, msg)
    except Exception as e:
        log_error(f"channel_post forward schedule failed: {e}")


@bot.edited_channel_post_handler(content_types=[
    "text", "photo", "video", "animation", "audio",
    "voice", "video_note", "document",
    "sticker", "location", "venue", "contact", "dice", "poll"
])
def on_edited_channel_post(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception as e:
        log_error(f"edited_channel_post update_chat_info failed: {e}")

    if handle_secret_edited_message(msg):
        return

    try:
        if is_finance_mode(msg.chat.id):
            handle_finance_edit(msg)
    except Exception as e:
        log_error(f"edited_channel_post finance edit failed: {e}")

    try:
        schedule_propagate_edited_to_copies(msg)
    except Exception as e:
        log_error(f"edited_channel_post propagate schedule failed: {e}")


def propagate_edited_to_copies(msg):
    source_chat_id = msg.chat.id
    text = _message_text_for_finance(msg)

    links = get_forward_links(source_chat_id, msg.message_id)
    if not links:
        return

    links = sorted(
        list(links),
        key=lambda pair: (0 if get_forward_finance(source_chat_id, int(pair[0])) else 1),
    )
    for dst_chat_id, dst_msg_id in links:
        try:
            finance_enabled = get_forward_finance(source_chat_id, dst_chat_id)
            sync_edited_copy_to_target(source_chat_id, msg, dst_chat_id, dst_msg_id, finance_enabled)
        except Exception as e:
            log_error(f"propagate_edited_to_copies failed {dst_chat_id}:{dst_msg_id}: {e}")


@bot.edited_message_handler(
    content_types=["text", "photo", "video", "animation", "document", "audio", "voice"]
)
def on_edited_message(msg):
    chat_id = msg.chat.id
    try:
        bot_journal("edited_message_received", chat_id, f"msg={getattr(msg, 'message_id', 0)}")
    except Exception:
        pass

    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass

    if handle_secret_edited_message(msg):
        return

    edit_text = _message_text_for_finance(msg)
    if is_forward_delete_command(edit_text):
        try:
            schedule_delete_forward_copies_for_source(chat_id, msg.message_id)
        except Exception as e:
            log_error(f"[EDIT-DEL] schedule failed: {e}")

    try:
        edited = handle_finance_edit(msg)
        if edited:
            store = get_chat_store(chat_id)
            day_key = store.get("current_view_day") or today_key()
            log_info(f"[EDIT-FIN] finalize day_key={day_key}")
            schedule_finalize(chat_id, day_key)
    except Exception as e:
        log_error(f"[EDIT-FIN] failed: {e}")

    try:
        if not is_forward_delete_command(edit_text):
            schedule_propagate_edited_to_copies(msg)
    except Exception as e:
        log_error(f"[EDIT-FWD] schedule failed: {e}")
                                            

@bot.message_handler(commands=["buttons"])
def cmd_buttons(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass
    schedule_command_delete(msg)
    chat_id = msg.chat.id
    if not is_owner_chat(chat_id):
        return
    new_state = toggle_icon_button_mode(chat_id)
    send_and_auto_delete(chat_id, f"✅ Кнопки переключены: {'значки' if new_state else 'текст'}", 30)
    try:
        refresh_registered_financial_windows(chat_id)
    except Exception:
        pass


@bot.message_handler(commands=["restore_guard"])
def cmd_restore_guard(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass
    schedule_command_delete(msg)
    chat_id = msg.chat.id
    if "tenant_require_platform_owner" in globals():
        if not tenant_require_platform_owner(msg):
            return
    elif not is_owner_chat(chat_id if "chat_id" in locals() else msg.chat.id):
        return
    send_and_auto_delete(chat_id, restore_guard_status_text(), 120)


@bot.message_handler(commands=["restore_guard_off"])
def cmd_restore_guard_off(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass
    schedule_command_delete(msg)
    chat_id = msg.chat.id
    if "tenant_require_platform_owner" in globals():
        if not tenant_require_platform_owner(msg):
            return
    elif not is_owner_chat(chat_id if "chat_id" in locals() else msg.chat.id):
        return
    count = disable_restore_guard_and_enable_mega_backups()
    send_and_auto_delete(
        chat_id,
        restore_guard_status_text() + f"\n\n✅ Guard отключён владельцем. MEGA autobackup включён для {count} чатов.",
        120,
    )


@bot.message_handler(commands=["restore_guard_on"])
def cmd_restore_guard_on(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass
    schedule_command_delete(msg)
    chat_id = msg.chat.id
    if "tenant_require_platform_owner" in globals():
        if not tenant_require_platform_owner(msg):
            return
    elif not is_owner_chat(chat_id if "chat_id" in locals() else msg.chat.id):
        return
    set_restore_guard_manual_override(False)
    send_and_auto_delete(chat_id, "🛡 Ручное отключение Restore guard снято. При следующей аварийной проверке guard снова сможет включиться.", 90)


@bot.message_handler(commands=["delta_status"])
def cmd_delta_status(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass
    schedule_command_delete(msg)
    if "tenant_require_platform_owner" in globals():
        if not tenant_require_platform_owner(msg):
            return
    elif not is_owner_chat(chat_id if "chat_id" in locals() else msg.chat.id):
        return
    send_and_auto_delete(msg.chat.id, delta_status_text(), 120)


@bot.message_handler(commands=["mega_status"])
def cmd_mega_status(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass
    schedule_command_delete(msg)
    chat_id = msg.chat.id
    if "tenant_require_platform_owner" in globals():
        if not tenant_require_platform_owner(msg):
            return
    elif not is_owner_chat(chat_id if "chat_id" in locals() else msg.chat.id):
        send_and_auto_delete(chat_id, "Эта команда только для владельца.", HELPER_DELETE_DELAY)
        return
    send_and_auto_delete(chat_id, mega_status_text(), 90)


def run_manual_mega_restore(chat_id: int):
    """Ручное полное восстановление из MEGA тем же движком, что используется при автопроверке после деплоя.

    force=True нужен именно для ручного режима: пользователь осознанно просит перечитать
    лучший полный global snapshot и применить последующие delta, даже если локальная база
    по времени выглядит не хуже облачной.
    """
    chat_id = int(chat_id)
    try:
        send_and_auto_delete(chat_id, "☁️ Ручное восстановление: читаю полный JSON и delta из MEGA…", 30)
        ok, detail = mega_restore_full_from_cloud(force=True)
        if ok:
            try:
                refresh_registered_financial_windows(chat_id)
            except Exception:
                pass
            try:
                schedule_startup_main_windows(delay=0.5)
            except Exception:
                pass
            send_and_auto_delete(chat_id, "✅ MEGA → бот обновлён. " + detail, 180)
        else:
            send_and_auto_delete(chat_id, "❌ MEGA restore: " + detail, 180)
    except Exception as e:
        log_error(f"run_manual_mega_restore: {e}")
        send_and_auto_delete(chat_id, "❌ Ошибка ручного восстановления из MEGA: " + str(e)[:500], 180)


@bot.message_handler(commands=["mega_restore_now"])
def cmd_mega_restore_now(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass
    schedule_command_delete(msg)
    chat_id = msg.chat.id
    if "tenant_require_platform_owner" in globals():
        if not tenant_require_platform_owner(msg):
            return
    elif not is_owner_chat(chat_id if "chat_id" in locals() else msg.chat.id):
        send_and_auto_delete(chat_id, "Эта команда только для владельца.", HELPER_DELETE_DELAY)
        return
    if not GENERAL_TASK_POOL.submit(f"manual-mega-restore:{chat_id}", run_manual_mega_restore, chat_id):
        send_and_auto_delete(chat_id, "⛔ Очередь восстановления переполнена. Попробуйте позже.", 20)


@bot.message_handler(commands=["mega_backup_now"])
def cmd_mega_backup_now(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass
    schedule_command_delete(msg)
    chat_id = msg.chat.id
    if "tenant_require_platform_owner" in globals():
        if not tenant_require_platform_owner(msg):
            return
    elif not is_owner_chat(chat_id if "chat_id" in locals() else msg.chat.id):
        send_and_auto_delete(chat_id, "Эта команда только для владельца.", HELPER_DELETE_DELAY)
        return
    try:
        if RESTORE_GUARD_ACTIVE:
            send_and_auto_delete(chat_id, "🚨 Бэкап заблокирован: " + RESTORE_GUARD_REASON, 120)
            return
        with data_lock:
            export_global_csv(data)
            save_data(data)
        ok = mega_upload_latest_global_backup()
        uploaded = 0
        failed = 0
        for cid in collect_finance_chat_ids():
            try:
                if mega_upload_chat_backup_bundle(cid, current_month_key()):
                    uploaded += 1
                else:
                    failed += 1
            except Exception as e:
                failed += 1
                log_error(f"cmd_mega_backup_now chat {cid}: {e}")
        if ok:
            send_and_auto_delete(chat_id, f"☁️ MEGA backup: ✅ global + чатов: {uploaded}, ошибок: {failed}", 60)
        else:
            send_and_auto_delete(chat_id, f"☁️ MEGA backup: ❌ global не загружен, чатов: {uploaded}, ошибок: {failed}; смотри /errors", 60)
    except Exception as e:
        log_error(f"cmd_mega_backup_now: {e}")
        send_and_auto_delete(chat_id, "☁️ MEGA backup: ❌ ошибка, смотри /errors", 60)


def build_diag_text() -> str:
    chats = data.get("chats", {}) or {}
    finance_ids = collect_finance_chat_ids()
    hidden = []
    quick_on = []
    try:
        for cid in finance_ids:
            if is_hidden_finance_mode(cid):
                hidden.append(cid)
            if is_quick_balance_enabled(cid):
                quick_on.append(cid)
    except Exception:
        pass
    fr = data.get("forward_rules", {}) or {}
    forward_pairs = sum(len(v or {}) for v in fr.values())
    active_windows_count = 0
    try:
        active_windows_count = sum(len(v or {}) for v in (data.get("active_messages", {}) or {}).values())
    except Exception:
        active_windows_count = 0
    dirty_count = 0
    try:
        with timer_lock:
            dirty_count = len(_backup_dirty_chats)
    except Exception:
        pass
    errors = get_recent_errors(5)

    lines = [
        "🧪 Диагностика бота",
        f"Версия: {VERSION}",
        f"SQLite: {DB_FILE}",
        f"Чатов в базе: {len(chats)}",
        f"Фин-чатов: {len(finance_ids)}",
        f"Скрытых фин-чатов: {len(hidden)}",
        f"Быстрый остаток включён: {len(quick_on)}",
        f"Связей пересылки: {forward_pairs}",
        f"Активных окон: {active_windows_count}",
        f"Dirty-бэкапов в очереди: {dirty_count}",
        f"Очередь content: {WEBHOOK_TASK_POOL.stats()['pending']}",
        f"Очередь UI: {UI_TASK_POOL.stats()['pending']}",
        f"Очередь callback ACK: {CALLBACK_ACK_TASK_POOL.stats()['pending']}",
        f"Очередь recovery: {RECOVERY_TASK_POOL.stats()['pending']}",
        f"Очередь напоминалок: {REMINDER_TASK_POOL.stats()['pending']}",
        f"Очередь пересылки: {FORWARD_TASK_POOL.stats()['pending']}",
        f"Очередь финансов: {FINANCE_TASK_POOL.stats()['pending']}",
        f"Очередь delta: {DELTA_TASK_POOL.stats()['pending']}",
        f"Очередь backup: {BACKUP_TASK_POOL.stats()['pending']}",
        f"Очередь maintenance: {MAINTENANCE_TASK_POOL.stats()['pending']}",
        f"BACKUP_CHAT_ID: {'есть' if BACKUP_CHAT_ID else 'нет'}",
        f"Бэкап в канал: {'ВКЛ' if backup_flags.get('channel', True) else 'ВЫКЛ'}",
        f"MEGA: {'ВКЛ' if MEGA_ENABLED else 'ВЫКЛ'} / {'настроено' if mega_is_configured() else 'не настроено'}",
        f"MEGA dir: {MEGA_BACKUP_DIR}",
        f"MEGA delta dir: {mega_delta_remote_root()}",
        f"Delta pending: {len(_delta_pending_chats)} / last events: {_delta_last_event_count}",
        f"Global full pending: {'да' if _global_snapshot_pending else 'нет'}",
        f"Ошибок в журнале: {len(get_recent_errors(80))}",
    ]
    if errors:
        lines.append("")
        lines.append("Последние ошибки:")
        for e in errors:
            lines.append(f"• {e.get('ts','')} — {format_error_for_owner(e.get('msg',''))[:160]}")
    return "\n".join(lines)


@bot.message_handler(commands=["off_on_backup_excel"])
def cmd_off_on_backup_excel(msg):
    update_chat_info_from_message(msg)
    schedule_command_delete(msg)
    chat_id = msg.chat.id
    if "tenant_can_manage" in globals():
        actor = int(getattr(getattr(msg, "from_user", None), "id", 0) or 0)
        if not tenant_can_manage(actor, chat_id=chat_id):
            send_and_auto_delete(chat_id, "Эта команда только для владельца пространства.", HELPER_DELETE_DELAY)
            return
    elif not is_owner_chat(chat_id):
        send_and_auto_delete(chat_id, "Эта команда только для владельца.", HELPER_DELETE_DELAY)
        return
    enabled = toggle_backup_excel_all_enabled()
    if enabled:
        target_ids = tenant_chat_ids(tenant_id_for_chat(chat_id, create=False)) if "tenant_chat_ids" in globals() else collect_finance_chat_ids()
        for cid in target_ids:
            if is_finance_mode(int(cid)):
                schedule_backup_flush(int(cid), BACKUP_MIN_DELAY_SECONDS)
    send_and_auto_delete(chat_id, f"📊 Excel-бэкап чатов пространства: {'ВКЛ' if enabled else 'ВЫКЛ'}", 20)


@bot.message_handler(commands=["queues", "queue_status"])
def cmd_queues(msg):
    update_chat_info_from_message(msg)
    schedule_command_delete(msg)
    chat_id = msg.chat.id
    if "tenant_require_platform_owner" in globals():
        if not tenant_require_platform_owner(msg):
            return
    elif not is_owner_chat(chat_id if "chat_id" in locals() else msg.chat.id):
        send_and_auto_delete(chat_id, "Эта команда только для владельца.", HELPER_DELETE_DELAY)
        return
    send_and_auto_delete(chat_id, build_queue_status_text(), 90)


@bot.message_handler(commands=["diag", "diagnostics"])
def cmd_diag(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass
    schedule_command_delete(msg)
    chat_id = msg.chat.id
    if "tenant_require_platform_owner" in globals():
        if not tenant_require_platform_owner(msg):
            return
    elif not is_owner_chat(chat_id if "chat_id" in locals() else msg.chat.id):
        send_and_auto_delete(chat_id, "Эта команда только для владельца.", HELPER_DELETE_DELAY)
        return
    send_and_auto_delete(chat_id, build_diag_text(), 60)


@bot.message_handler(commands=["errors", "bot_errors"])
def cmd_errors(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass
    schedule_command_delete(msg)
    chat_id = msg.chat.id
    if "tenant_require_platform_owner" in globals():
        if not tenant_require_platform_owner(msg):
            return
    elif not is_owner_chat(chat_id if "chat_id" in locals() else msg.chat.id):
        send_and_auto_delete(chat_id, "Эта команда только для владельца.", HELPER_DELETE_DELAY)
        return
    errors = get_recent_errors(30)
    if not errors:
        send_and_auto_delete(chat_id, "🧯 Ошибок в журнале нет.", 30)
        return

    # v129 / Ф40: identical retries of the same Telegram update used to explode the
    # message beyond Telegram's limit. Keep recent UNIQUE errors and split safely.
    unique = []
    seen = set()
    for e in reversed(errors):
        msg_text = format_error_for_owner(e.get("msg", ""))
        fingerprint = re.sub(r"\s+", " ", msg_text).strip()[:1200]
        if fingerprint in seen:
            continue
        seen.add(fingerprint)
        unique.append((e, msg_text))
        if len(unique) >= 12:
            break
    unique.reverse()

    blocks = ["🧯 Последние ошибки бота:"]
    for e, msg_text in unique:
        blocks.append(f"• {e.get('ts','')}\n{msg_text[:650]}")

    chunks = []
    current = ""
    for block in blocks:
        candidate = block if not current else current + "\n\n" + block
        if len(candidate) > 3400 and current:
            chunks.append(current)
            current = block
        else:
            current = candidate
    if current:
        chunks.append(current)
    for idx, chunk in enumerate(chunks, start=1):
        prefix = f"🧯 Ф40 {idx}/{len(chunks)}\n" if len(chunks) > 1 else ""
        send_and_auto_delete(chat_id, prefix + chunk, 90)




@bot.message_handler(commands=["journal", "log", "logs"])
def cmd_journal(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass
    schedule_command_delete(msg)
    chat_id = msg.chat.id
    bot_journal("command_journal", chat_id, getattr(msg, "text", ""))
    if "tenant_require_platform_owner" in globals():
        if not tenant_require_platform_owner(msg):
            return
    elif not is_owner_chat(chat_id if "chat_id" in locals() else msg.chat.id):
        send_and_auto_delete(chat_id, "Эта команда только для владельца.", HELPER_DELETE_DELAY)
        return
    send_journal_file_to_owner(chat_id, 3000)

def _send_sqlite_dump_job(chat_id: int):
    """Send the working SQLite snapshot through the same no-duplicate file lane."""
    try:
        _file_job_progress("отправляю SQLite в Telegram", force=True)
        with open(DB_FILE, "rb") as f:
            _tg_call_retry(bot.send_document, chat_id, f, caption=f"🗄 SQLite база: {os.path.basename(DB_FILE)}", timeout=120, purpose="manual_sqlite_export")
        return True
    except Exception as e:
        log_error(f"_send_sqlite_dump_job: {e}")
        send_and_auto_delete(chat_id, f"❌ Не удалось отправить SQLite: {e}", HELPER_DELETE_DELAY)
        return False


@bot.message_handler(commands=["sqlite", "db"])
def cmd_sqlite_dump(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass

    schedule_command_delete(msg)
    chat_id = msg.chat.id
    if is_finance_output_suppressed(chat_id):
        return
    stop_dozvon_for_target(chat_id)
    if guard_non_owner_finance_for_command(msg, {"ok", "help"}):
        return
    if "tenant_require_platform_owner" in globals():
        if not tenant_require_platform_owner(msg):
            return
    elif not is_owner_chat(chat_id if "chat_id" in locals() else msg.chat.id):
        send_and_auto_delete(chat_id, "Эта команда только для владельца.", HELPER_DELETE_DELAY)
        return

    ok, info = submit_interactive_file_job(chat_id, "sqlite", "SQLite база", _send_sqlite_dump_job, chat_id)
    if not ok:
        send_and_auto_delete(chat_id, f"⏳ {info}. Новая копия в очередь не добавлена.", 10)


def start_keep_alive_thread():
    global _keep_alive_thread
    with _keep_alive_thread_lock:
        if _keep_alive_thread is not None and _keep_alive_thread.is_alive():
            return _keep_alive_thread
        _keep_alive_thread = threading.Thread(target=keep_alive_task, name="keep-alive-watchdog", daemon=True)
        _keep_alive_thread.start()
        return _keep_alive_thread
# v182_restore_unified
