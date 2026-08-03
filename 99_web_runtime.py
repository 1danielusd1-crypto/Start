# v141_operation_ledger_windows_expense_reminders_safety
@app.route("/", methods=["GET"])
def index():
    return "OK", 200


@app.route("/healthz", methods=["GET"])
def healthz():
    # Liveness only: Render must not restart a healthy process merely because MEGA is temporarily unavailable.
    return {
        "ok": True,
        "version": VERSION,
        "phase": _RUNTIME_STATE.get("phase"),
        "ready": runtime_is_ready(),
        "shutting_down": runtime_is_shutting_down(),
    }, 200


@app.route("/readyz", methods=["GET"])
def readyz():
    code = 200 if runtime_is_ready() else 503
    return {
        "ok": runtime_is_ready(),
        "version": VERSION,
        "phase": _RUNTIME_STATE.get("phase"),
        "task_recovery_remaining": _RUNTIME_STATE.get("task_recovery_remaining", 0),
    }, code


@app.route("/keepalive", methods=["GET", "HEAD"])
def keepalive_endpoint():
    ping_at = _journal_ts()
    user_agent = str(request.headers.get("User-Agent", "") or "")[:240]
    KEEP_ALIVE_STATE["external_ping_at"] = ping_at  # backward-compatible: any HTTP hit to /keepalive
    KEEP_ALIVE_STATE["last_keepalive_user_agent"] = user_agent
    if user_agent == f"{VERSION}-keepalive":
        KEEP_ALIVE_STATE["self_ping_at"] = ping_at
    else:
        KEEP_ALIVE_STATE["external_monitor_at"] = ping_at
    if request.method == "HEAD":
        return "", 200
    return {
        "ok": True,
        "version": VERSION,
        "time": ping_at,
        "profile": active_bot_behavior_profile(),
        "keep_alive": KEEP_ALIVE_ENABLED,
        "ready": runtime_is_ready(),
        "phase": _RUNTIME_STATE.get("phase"),
        "uptime_seconds": round(max(0.0, time.monotonic() - _RUNTIME_STARTED_MONO), 1),
        "external_monitor_seen": bool(KEEP_ALIVE_STATE.get("external_monitor_at")),
    }, 200



@app.route("/expense-ping/<token>", methods=["GET", "POST"])
def expense_ping_endpoint(token: str):
    """Защищённый endpoint для iPhone Shortcuts/Back Tap.

    Событие сначала сохраняется в постоянную очередь бота, поэтому временная ошибка
    Telegram не стирает отметку расхода: доставка будет повторена.
    """
    if not runtime_is_ready() or runtime_is_shutting_down():
        return {"ok": False, "status": "booting"}, 503
    cfg = expense_shortcut_config(False) or {}
    expected = str(cfg.get("token") or "")
    if not expected or not secrets.compare_digest(str(token or ""), expected):
        return {"ok": False}, 404
    if "safety_profile_new_enabled" in globals() and safety_profile_new_enabled():
        try:
            rate_key = f"{request.remote_addr or 'unknown'}:{str(token)[-8:]}"
            now_ts = time.time()
            bucket = _IPHONE_ENDPOINT_RUNTIME[rate_key]
            while bucket and now_ts - float(bucket[0]) > 60.0:
                bucket.popleft()
            if len(bucket) >= 10:
                try:
                    bot_journal("expense_ping_rate_limited", None, f"key={rate_key}", "WARN")
                except Exception:
                    pass
                return {"ok": False, "error": "rate_limited"}, 429
            bucket.append(now_ts)
        except Exception:
            pass
    try:
        event_id, duplicate = enqueue_expense_ping_event("iphone_back_tap", force=False)
        target_chat_id = int(expense_shortcut_config(True).get("target_chat_id") or 0)
        return {
            "ok": True,
            "queued": True,
            "duplicate_suppressed": bool(duplicate),
            "event": event_id,
            "target_chat_id": target_chat_id,
            "time": now_local().isoformat(timespec="seconds"),
        }, 202 if not duplicate else 200
    except Exception as exc:
        log_error(f"expense ping endpoint: {exc}")
        return {"ok": False, "error": "queue_failed"}, 503


def _refresh_callback_window_timers(chat_id: int, message_id: int, raw_callback: str):
    """v135: любой клик в связанном окне начинает ВСЕ его авто-таймеры заново."""
    chat_id = int(chat_id); message_id = int(message_id)
    raw = str(raw_callback or "")
    resolved = resolve_short_callback(raw) or raw
    try:
        _touch_v98_auto_close_for_callback(chat_id, message_id, resolved)
    except Exception:
        pass

    store = get_chat_store(chat_id)
    # Отдельные обычные окна с автовозвратом.
    try:
        for timer_chat_id, store_key in list(_aux_window_timers.keys()):
            if int(timer_chat_id) == chat_id and int(store.get(str(store_key)) or 0) == message_id:
                schedule_stored_window_delete(chat_id, str(store_key), None)
    except Exception:
        pass

    # Любая кнопка внутри окна редактирования/ввода продлевает его ожидание.
    try:
        wait = store.get("edit_wait") or {}
        if int(wait.get("prompt_msg_id") or 0) == message_id:
            schedule_cancel_edit(chat_id, message_id, delay=None)
    except Exception:
        pass
    try:
        wait = store.get("finwin_edit_wait") or {}
        if int(wait.get("prompt_msg_id") or 0) == message_id:
            schedule_cancel_finwin_edit(chat_id, message_id, delay=None)
    except Exception:
        pass
    for field in ("category_add_wait", "category_edit_wait"):
        try:
            wait = store.get(field) or {}
            if int(wait.get("prompt_msg_id") or 0) == message_id:
                schedule_cancel_category_wait(chat_id, field, message_id, delay=None)
        except Exception:
            pass
    try:
        wait = store.get("forward_copy_edit_wait") or {}
        if int(wait.get("prompt_msg_id") or 0) == message_id:
            schedule_forward_copy_edit_wait_cancel(chat_id, message_id, delay=None)
    except Exception:
        pass

    # Секретные окна раньше намеренно исключались из общей защиты. Из-за этого нажатие
    # соседней кнопки могло не продлить 90 секунд и окно закрывалось во время работы.
    try:
        active = store.get("secret_active_window") or {}
        if int(active.get("message_id") or 0) == message_id:
            schedule_secret_calendar_close(chat_id, message_id)
    except Exception:
        pass
    try:
        if (chat_id, message_id) in _secret_media_timer_generation:
            schedule_secret_media_close(chat_id, message_id)
    except Exception:
        pass
    try:
        secret_wait = store.get("secret_wait") or {}
        if int(secret_wait.get("prompt_msg_id") or 0) == message_id:
            schedule_o9_secret_wait_timeout(chat_id, message_id, O9_SECRET_WAIT_SECONDS)
    except Exception:
        pass


def _protect_pending_ui_timers_on_receipt(payload: dict):
    """Продлевает связанные авто-close/auto-cancel/auto-return уже в момент receipt webhook."""
    try:
        if not isinstance(payload, dict):
            return
        msg = payload.get("message") or payload.get("edited_message")
        if isinstance(msg, dict):
            chat = msg.get("chat") or {}
            chat_id = int(chat.get("id"))
            store = get_chat_store(chat_id)
            wait = store.get("edit_wait") or {}
            if isinstance(wait, dict) and wait.get("prompt_msg_id"):
                schedule_cancel_edit(chat_id, int(wait["prompt_msg_id"]), delay=None)
            wait = store.get("finwin_edit_wait") or {}
            if isinstance(wait, dict) and wait.get("prompt_msg_id"):
                schedule_cancel_finwin_edit(chat_id, int(wait["prompt_msg_id"]), delay=None)
            for field in ("category_add_wait", "category_edit_wait"):
                wait = store.get(field) or {}
                if isinstance(wait, dict) and wait.get("prompt_msg_id"):
                    schedule_cancel_category_wait(chat_id, field, int(wait["prompt_msg_id"]), delay=None)
            wait = store.get("forward_copy_edit_wait") or {}
            if isinstance(wait, dict) and wait.get("prompt_msg_id"):
                schedule_forward_copy_edit_wait_cancel(chat_id, int(wait["prompt_msg_id"]), delay=None)
            try:
                secret_wait = store.get("secret_wait") or {}
                if isinstance(secret_wait, dict) and secret_wait.get("prompt_msg_id"):
                    schedule_o9_secret_wait_timeout(chat_id, int(secret_wait["prompt_msg_id"]), O9_SECRET_WAIT_SECONDS)
            except Exception:
                pass
            return

        cq = payload.get("callback_query")
        if isinstance(cq, dict):
            cmsg = cq.get("message") or {}
            chat = cmsg.get("chat") or {}
            chat_id = int(chat.get("id"))
            message_id = int(cmsg.get("message_id") or 0)
            if message_id:
                _refresh_callback_window_timers(chat_id, message_id, str(cq.get("data") or ""))
    except Exception as e:
        try:
            log_error(f"receipt timer protection: {e}")
        except Exception:
            pass


@app.route(f"/{BOT_TOKEN}", methods=["POST"])
def telegram_webhook():
    try:
        payload = request.get_json(force=True, silent=False)
    except Exception as e:
        log_error(f"WEBHOOK: get_json failed: {e}")
        return "BAD REQUEST", 400

    # v108 BOOT/SHUTDOWN gate: do not execute a new Telegram update against partially restored
    # state or while the old Render instance is draining.  503 keeps the update retryable.
    if runtime_is_shutting_down():
        runtime_mark_webhook(payload if isinstance(payload, dict) else None, blocked="shutdown")
        return "SHUTTING DOWN", 503
    if not runtime_is_ready():
        runtime_mark_webhook(payload if isinstance(payload, dict) else None, blocked="boot")
        return "BOOTING", 503
    runtime_mark_webhook(payload if isinstance(payload, dict) else None)

    try:
        if isinstance(payload, dict):
            if "edited_message" in payload:
                log_info("WEBHOOK: получен update с edited_message ✅")
            elif "message" in payload:
                log_info("WEBHOOK: получен update с message")
            elif "callback_query" in payload:
                log_info("WEBHOOK: получен update с callback_query")
            try:
                upd_type = "edited_message" if "edited_message" in payload else "message" if "message" in payload else "callback_query" if "callback_query" in payload else "other"
                bot_journal("webhook_update", _extract_update_chat_id(payload), upd_type)
            except Exception:
                pass

        update = telebot.types.Update.de_json(payload)
        update_chat_id = _extract_update_chat_id(payload) if isinstance(payload, dict) else None
        update_id = getattr(update, "update_id", None)
        if update_id is None:
            update_id = time.time_ns()
        update_key = update_chat_id if update_chat_id is not None else update_id
        update_type = "edited_message" if isinstance(payload, dict) and "edited_message" in payload else "callback_query" if isinstance(payload, dict) and "callback_query" in payload else "message" if isinstance(payload, dict) and "message" in payload else "other"

        # Пользователь уже совершил действие. Не даём рабочему таймеру истечь, пока update
        # стоит за другой задачей этого чата. Секретные таймеры не меняются.
        _protect_pending_ui_timers_on_receipt(payload)
        if update_type == "callback_query":
            try:
                cq_raw = (payload or {}).get("callback_query") or {}
                schedule_callback_receipt_ack(str(cq_raw.get("id") or ""), update_chat_id)
            except Exception as ack_exc:
                log_error(f"CALLBACK RECEIPT ACK SCHEDULE: {ack_exc}")

        # Persisted idempotency marker wins even if Render died after committing state but before HTTP 200.
        if durable_update_processed(update_id):
            with _MEGA_TASK_LOCK:
                _mega_task_counters["skipped_done"] += 1
            return "OK", 200

        # v105/v108: critical content and rare state-mutating F39 callbacks first get a durable card.
        # Only after confirmed pending persistence may the update enter a RAM worker.
        durable_cloud, durable_reason = durable_task_required(payload)
        durable_expected = _durable_expected_effects(payload) if durable_cloud else {}
        if durable_cloud:
            cloud_state = mega_task_known_state(update_id)
            if cloud_state == "done":
                with _MEGA_TASK_LOCK:
                    _mega_task_counters["skipped_done"] += 1
                return "OK", 200
            if cloud_state == "running":
                # После deploy это означает: выполнение уже начиналось. Не запускаем второй worker
                # через webhook; recovery-контур сам сверит/доделает такую задачу.
                schedule_mega_task_recovery(0.2)
                return "TASK RUNNING", 503
            if cloud_state == "failed":
                # v109: failed means manual review. Automatic Telegram retry must not replay
                # a potentially-applied finance operation or toggle.
                return "TASK NEEDS REVIEW", 200
            if cloud_state != "pending":
                task_payload = _build_mega_task_payload(update_id, payload, update_chat_id, update_type, durable_reason)
                durable_expected = _durable_expected_from_task_or_payload(task_payload, payload)
                if not _mega_task_upload_new_pending(update_id, task_payload):
                    # Не исполняем критическую команду без внешнего свидетеля. Telegram повторит update.
                    return "TASK BACKUP UNAVAILABLE", 503

        claim_state, ticket = UPDATE_DISPATCHER.claim(update_id, update_chat_id, update_type)
        if claim_state == "done":
            return "OK", 200

        update_enqueued_at = time.time()

        if claim_state == "new":
            def _process_update():
                started = time.time()
                wait = started - update_enqueued_at
                UPDATE_DISPATCHER.mark_started(update_id)
                bot_journal("update_process_start", update_chat_id, f"update_id={update_id} type={update_type} queue_wait={wait:.3f}s durable={durable_cloud}")
                success = False
                error_text = ""
                durable_started = False
                try:
                    if durable_cloud:
                        durable_started = mega_task_begin(update_id, allow_existing_running=False)
                        if not durable_started:
                            raise RuntimeError("MEGA durable task could not enter running state")
                    execution_ctx = _execute_telegram_payload(payload, update_id, update_chat_id, update_type)
                    success = True
                    if durable_cloud:
                        durable_expected_after = _durable_expected_after_execution(durable_expected, execution_ctx, payload)
                        # v138: forwarding/finance witnesses may need 5–20 s. Verification is moved
                        # to RECOVERY_TASK_POOL; business execution and the content/UI lane are free.
                        queued_finalize = enqueue_durable_finalize_background(
                            update_id, update_chat_id, update_type, payload, durable_expected_after
                        )
                        if not queued_finalize:
                            finalized = finalize_durable_task_after_business(
                                update_id, update_chat_id, update_type,
                                payload=payload, expected_effects=durable_expected_after,
                            )
                            if not finalized:
                                schedule_durable_task_finalize_retry(
                                    update_id, update_chat_id, update_type, 1.0,
                                    payload=payload, expected_effects=durable_expected_after,
                                )
                                log_error(f"MEGA TASK FINALIZE DEFERRED update={update_id}; durable effects still pending")
                except Exception as exc:
                    error_text = str(exc)
                    if durable_cloud and durable_started:
                        mega_task_finish(update_id, False, error_text)
                    log_error(f"WEBHOOK PROCESS FAILED update={update_id} chat={update_chat_id}: {exc}")
                    raise
                finally:
                    UPDATE_DISPATCHER.finish(update_id, success, error_text)
                    bot_journal("update_process_done", update_chat_id, f"update_id={update_id} type={update_type} queue_wait={wait:.3f}s process={time.time()-started:.3f}s total={time.time()-update_enqueued_at:.3f}s success={success} durable={durable_cloud}")
                    # Non-durable updates can release cold history immediately. Durable background
                    # finalizer owns a delayed release after its witness check.
                    if not durable_cloud:
                        try:
                            _lowram_release_chat(update_chat_id)
                        except Exception as _lr_exc:
                            log_error(f"LOWRAM post-update release: {_lr_exc}")

            selected_pool = UI_TASK_POOL if update_type == "callback_query" else WEBHOOK_TASK_POOL
            selected_key = f"ui:{update_key}" if update_type == "callback_query" else update_key
            if not selected_pool.submit(selected_key, _process_update):
                log_error(f"{selected_pool.name.upper()} QUEUE FULL: chat={update_chat_id}")
                UPDATE_DISPATCHER.release_failed_enqueue(update_id, f"{selected_pool.name}_queue_full")
                # Telegram повторит update позже; pending-файл уже сохранён в MEGA.
                return "BUSY", 503

        # v138 safety invariant: callback spinner is cleared by the dedicated ACK lane, but the
        # HTTP webhook receipt is still held until the queued action finishes (or times out).
        # Therefore a Render crash cannot silently lose even a non-cloud UI action: Telegram keeps
        # the update as the external emergency queue and retries after our 503. Critical finance/
        # forwarding/secret content additionally keeps the existing write-before-execute MEGA card.
        state, dispatch_error = UPDATE_DISPATCHER.wait_result(ticket, WEBHOOK_ACK_WAIT_SECONDS)
        if state == "done":
            return "OK", 200
        if state == "failed":
            return "RETRY", 503
        return "PENDING", 503
    except Exception as e:
        log_error(f"WEBHOOK: enqueue/update dispatcher error: {e}")
        return "ERROR", 500
        
def set_webhook():
    if not WEBHOOK_URL:
        log_info("WEBHOOK_URL / APP_URL / RENDER_EXTERNAL_URL не указаны — webhook не установлен.")
        return

    wh_url = WEBHOOK_URL.rstrip("/") + f"/{BOT_TOKEN}"

    bot.remove_webhook()
    time.sleep(0.5)

    try:
        webhook_connections = max(1, min(100, int(os.getenv("WEBHOOK_MAX_CONNECTIONS", "40") or "40")))
    except Exception:
        webhook_connections = 40
    bot.set_webhook(
        url=wh_url,
        max_connections=webhook_connections,
        allowed_updates=[
            "message",
            "edited_message",
            "callback_query",
            "channel_post",
            "edited_channel_post",
            "deleted_business_messages",
        ],
    )
    log_info(f"Webhook установлен: {wh_url} (max_connections={webhook_connections}; отдельные content/UI lanes)")
        
def main():
    global data
    runtime_install_signal_handlers()
    runtime_set_phase("boot_local_load", "восстанавливаю рабочую SQLite из MEGA / локального диска")
    restored = False
    db_restored = False
    if LOWRAM_ENABLED:
        try:
            db_restored, db_detail = mega_restore_sqlite_snapshot_from_cloud()
            runtime_event("boot_sqlite_snapshot", f"ok={db_restored} {db_detail}")
        except Exception as e:
            runtime_event("boot_sqlite_snapshot_error", str(e), "WARN")
    data = load_data()
    if db_restored:
        try:
            delta_count = lowram_apply_deltas_after_db_snapshot()
            restored = True
            runtime_event("boot_sqlite_deltas", f"applied={delta_count}")
        except Exception as e:
            runtime_event("boot_sqlite_deltas_error", str(e), "ERROR")
    runtime_set_phase("boot_mega_restore", "проверяю SQLite snapshot / fallback global + delta в MEGA")
    with _RUNTIME_LOCK:
        _RUNTIME_STATE["restore_attempted"] = True
    try:
        if not db_restored:
            restored = mega_autorestore_if_needed()
            # First v114 boot may have come from legacy latest_global.json. Immediately move
            # the restored history out of RAM into SQLite cold_fields.
            _lowram_prepare_loaded_data(data, migrate_existing=True)
            _lowram_flush_all_hot(evict=True)
        with _RUNTIME_LOCK:
            _RUNTIME_STATE["restore_ok"] = bool(restored or not RESTORE_GUARD_ACTIVE)
            _RUNTIME_STATE["restore_detail"] = ("MEGA SQLite snapshot + deltas" if db_restored else ("MEGA legacy global restore applied" if restored else ("restore guard active" if RESTORE_GUARD_ACTIVE else "local state retained")))
    except Exception as e:
        log_error(f"main mega_autorestore_if_needed: {e}")
        restored = False
        with _RUNTIME_LOCK:
            _RUNTIME_STATE["restore_ok"] = False
            _RUNTIME_STATE["restore_detail"] = str(e)[:500]
        runtime_event("boot_restore_error", str(e), "ERROR")

    # Previous runtime snapshot lives outside Render and survives sleep/redeploy/restart.
    runtime_set_phase("boot_watcher_previous", "читаю предыдущий runtime snapshot")
    try:
        prev_runtime = runtime_load_previous_snapshot()
        prev_reason = runtime_classify_previous(prev_runtime)
        with _RUNTIME_LOCK:
            _RUNTIME_STATE["previous_reason"] = prev_reason
        runtime_event("previous_runtime", prev_reason)
    except Exception as e:
        runtime_event("previous_runtime_error", str(e), "WARN")
    # v115: full durable history remains in MEGA, but BOOT must not spend tens of seconds
    # downloading journal chunks. Recent history is warmed after READY; TXT export streams
    # the complete durable history directly from MEGA.
    runtime_set_phase("boot_journal_deferred", "история журнала в MEGA; прогрев после READY")
    journal_start_durable_loop()

    if not RESTORE_GUARD_ACTIVE:
        migrate_legacy_owner_secrets()
    try:
        gs = data.setdefault("_global_settings", {})
        if not bool(gs.get("journal_default_off_v83_applied", False)):
            gs["bot_journal_enabled"] = False
            for _cid, _store in (data.get("chats", {}) or {}).items():
                if isinstance(_store, dict):
                    _store.setdefault("settings", {})["journal_enabled"] = False
            gs["journal_default_off_v83_applied"] = True
        gs.setdefault("bot_behavior_profile", DEFAULT_BOT_BEHAVIOR_PROFILE)
        # Новая база v90: интерфейсный профиль сохраняется; ядро хранения = delta + редкие snapshots.
        # Явно выбранные старые версии сохраняются без изменений.
        if not bool(gs.get("version_mode_v88_migrated", False)):
            if str(gs.get("bot_behavior_profile") or "") == "v87_current":
                gs["bot_behavior_profile"] = "v88_current"
            gs["version_mode_v88_migrated"] = True
        if not bool(gs.get("version_mode_v90_migrated", False)):
            if str(gs.get("bot_behavior_profile") or "") == "v88_current":
                gs["bot_behavior_profile"] = "v90_current"
            gs["version_mode_v90_migrated"] = True
        if not bool(gs.get("version_mode_v91_migrated", False)):
            if str(gs.get("bot_behavior_profile") or "") == "v90_current":
                gs["bot_behavior_profile"] = "v91_current"
            gs["version_mode_v91_migrated"] = True
        if not bool(gs.get("version_mode_v92_migrated", False)):
            if str(gs.get("bot_behavior_profile") or "") == "v91_current":
                gs["bot_behavior_profile"] = "v92_current"
            gs["version_mode_v92_migrated"] = True
        # Одноразово очищаем сохранённые имена статей от @username бота.
        if not bool(gs.get("category_names_clean_v88_applied", False)):
            for _cid, _store in (data.get("chats", {}) or {}).items():
                if not isinstance(_store, dict):
                    continue
                _custom_category_list(_store)
                _base_category_items(_store)
            gs["category_names_clean_v88_applied"] = True
    except Exception as e:
        log_error(f"v88 defaults migration: {e}")
    try:
        marker_report = audit_window_marker_registry()
        log_info(f"Маркеры окон проверены: {marker_report}")
    except Exception as e:
        log_error(f"audit_window_marker_registry: {e}")
    try:
        threading.Thread(target=_usd_rate_refresh_loop, name="usd-rate-refresh", daemon=True).start()
    except Exception as e:
        log_error(f"usd rate refresh start: {e}")
    for cid in list((data.get("chats", {}) or {}).keys()):
        try:
            store = get_chat_store(int(cid))
            settings = store.setdefault("settings", {})
            settings.setdefault("quick_balance_enabled", False)
            settings.setdefault("quick_balance_behavior", "normal")
            settings.setdefault("quick_balance_user_selected", False)
            settings.setdefault("hidden_finance", False)
            settings.setdefault("auto_backup_enabled", True)
            settings.setdefault("auto_backup_to_mega_enabled", True)
            settings.setdefault("journal_enabled", False)
            settings.setdefault("main_article_buttons_enabled", False)
            settings.setdefault("main_financial_value_buttons_enabled", False)
            settings.setdefault("currency_mode", "ars_usd" if settings.get("usd_display_enabled", False) else "ars")
            settings.setdefault("remaining_show_ost_label", True)
            settings.setdefault("total_secret_mode", False)
            # v114: secret history stays cold in SQLite; numbering is repaired lazily when secret UI is opened.
        except Exception:
            pass
    # После MEGA restore повторно поднимаем индекс пересылки и компактное состояние окон в память.
    # Новые webhook ещё закрыты BOOT-gate до полной проверки durable tasks.
    runtime_set_phase("boot_runtime_state", "восстанавливаю индексы и окна")
    _restore_runtime_state_from_data(data)
    restore_finance_window_runtime_state()
    if not RESTORE_GUARD_ACTIVE:
        save_data(data)
        data["forward_rules"] = load_forward_rules()
    else:
        # Аварийный режим: не создаём/не сохраняем новую пустую SQLite и не запускаем startup-backup.
        data.setdefault("forward_rules", {})
        log_error("v91 emergency mode: local writes and all automatic backups remain blocked")
    # v90: запуск/деплой не планирует бэкап; baseline delta создаётся без загрузки в MEGA.
    # Бэкап ставится только после реального изменения данных.
    try:
        initialize_delta_baseline(data)
    except Exception as e:
        log_error(f"initialize_delta_baseline: {e}")
    # Durable tasks are checked BEFORE READY.  A short synchronous pass recovers most work;
    # if something remains, Flask starts but webhook answers 503 until background recovery finishes.
    runtime_set_phase("boot_task_registry", "читаю MEGA tasks pending/running")
    boot_recovery_remaining = 0
    if mega_tasks_active():
        try:
            task_stats = mega_task_refresh_registry()
            log_info(
                f"[MEGA TASKS STARTUP] pending={task_stats.get('pending', 0)} "
                f"running={task_stats.get('running', 0)} failed={task_stats.get('failed', 0)} "
                f"done={task_stats.get('done', 0)}"
            )
            runtime_recover_tasks_blocking(BOOT_SYNC_RECOVERY_SECONDS)
            boot_recovery_remaining = len(_runtime_pending_recovery_rows())
        except Exception as e:
            log_error(f"mega_task_refresh/recovery startup: {e}")
            runtime_event("boot_task_recovery_error", str(e), "ERROR")
            try:
                boot_recovery_remaining = len(_runtime_pending_recovery_rows())
            except Exception:
                boot_recovery_remaining = 1
    with _RUNTIME_LOCK:
        _RUNTIME_STATE["task_recovery_remaining"] = int(boot_recovery_remaining)
    if OWNER_ID:
        try:
            finance_active_chats.add(int(OWNER_ID))
        except Exception:
            pass
    log_info(f"Данные загружены из SQLite ({DB_FILE}). Версия бота: {VERSION}")
    runtime_set_phase("boot_webhook", "устанавливаю Telegram webhook")
    set_webhook()
    # v140: повторить физические отметки расхода, которые были сохранены до временного сбоя Telegram/deploy.
    try:
        schedule_expense_ping_recovery(1.5)
    except Exception as e:
        log_error(f"expense ping recovery schedule: {e}")
    start_keep_alive_thread()
    try:
        start_reminder_scheduler()
    except Exception as e:
        log_error(f"reminder scheduler start: {e}")
    try:
        start_safety_schedulers()
    except Exception as e:
        log_error(f"safety schedulers start: {e}")

    if boot_recovery_remaining > 0:
        runtime_set_phase("boot_recovery_background", f"осталось {boot_recovery_remaining}; webhook временно 503")
        threading.Thread(target=runtime_continue_boot_recovery_background, name="boot-task-recovery", daemon=True).start()
    else:
        runtime_mark_ready("SQLite/global + delta восстановлены; pending/running durable tasks проверены")
        try:
            journal_flush_to_mega(True)
        except Exception:
            pass
    # v124: keep the exact source of this deploy outside Render's ephemeral filesystem.
    # This is cosmetic/maintenance work and never occupies webhook/finance/forward/delayed workers.
    try:
        if not MAINTENANCE_TASK_POOL.submit("archive-current-bot-source", archive_current_bot_source_to_mega):
            log_error("bot source archive maintenance queue full")
    except Exception as exc:
        log_error(f"bot source archive schedule: {exc}")

    if LOWRAM_ENABLED and not db_restored and not RESTORE_GUARD_ACTIVE:
        def _seed_primary_db_snapshot():
            try:
                time.sleep(2.0)
                mega_upload_latest_database_backup(force=True)
            except Exception as exc:
                log_error(f"LOWRAM initial DB snapshot: {exc}")
        threading.Thread(target=_seed_primary_db_snapshot, name="lowram-db-seed", daemon=True).start()
    # Never replay failed business work automatically. Heal only metadata-only/reclassified-safe gaps.
    try:
        schedule_safe_failed_task_repairs(8.0, 20)
    except Exception as exc:
        log_error(f"safe failed task repair schedule: {exc}")
    owner_id = None
    if OWNER_ID:
        try:
            owner_id = int(OWNER_ID)
        except Exception:
            owner_id = None
        if owner_id:
            try:
                bot.send_message(
                    owner_id,
                    f"{'🚨' if RESTORE_GUARD_ACTIVE else '✅'} {version_animal_badge()} Бот запущен (версия {VERSION}).\n"
                    f"Старт Python: {_RUNTIME_STATE.get('started_at') or '—'}; READY: {_RUNTIME_STATE.get('ready_at') or ('ещё RECOVERY' if not runtime_is_ready() else '—')}\n"
                    f"Причина предыдущего запуска (оценка): {_RUNTIME_STATE.get('previous_reason', '—')}\n"
                    f"Render instance: {str(os.getenv('RENDER_INSTANCE_ID','') or '—')[-28:]}; commit: {str(os.getenv('RENDER_GIT_COMMIT','') or '—')[:12]}\n"
                    f"⚠️ Это время старта Python-процесса, НЕ время начала deploy в Render Events. Процесс также стартует после sleep/restart/maintenance/crash.\n"
                    f"Восстановление: {'OK — SQLite snapshot из MEGA' if db_restored else ('OK — legacy global → SQLite' if restored else ('ОШИБКА — защитный режим' if RESTORE_GUARD_ACTIVE else 'локальная база сохранена'))}\n"
                    f"LOW-RAM: {'ВКЛ — RAM только активное; SQLite рабочее; MEGA постоянное' if LOWRAM_ENABLED else 'ВЫКЛ'}\n"
                    f"Защита бэкапа: {'ВКЛ — ' + RESTORE_GUARD_REASON if RESTORE_GUARD_ACTIVE else 'норма'}\n"
                    f"Индекс старых сообщений: {len(data.get('forward_index', {}) or {})}\n"
                    f"Приоритет: ФИНАНСЫ → ПЕРЕСЫЛКА; forward yield максимум {FORWARD_FINANCE_PRIORITY_MAX_WAIT_SECONDS:g}с\n"
                    f"Журнал: {'ВКЛ' if is_journal_registration_enabled() else 'ВЫКЛ'}; durable MEGA: {'ВКЛ' if BOT_JOURNAL_DURABLE_ENABLED else 'ВЫКЛ'}; keep-alive: {'ВКЛ' if KEEP_ALIVE_ENABLED else 'ВЫКЛ'}\n"
                    f"MEGA-задачи: pending {mega_task_registry_stats().get('pending', 0)}, running {mega_task_registry_stats().get('running', 0)}, failed {mega_task_registry_stats().get('failed', 0)}\n"
                    f"BOOT: {'READY' if runtime_is_ready() else 'RECOVERY'}; Watcher: Инфо → 🖥 Render / Сервер; heartbeat {RUNTIME_WATCHER_HEARTBEAT_SECONDS:g}с; MEGA slots {RUNTIME_WATCHER_SLOT_COUNT}\n"
                    f"Бэкап: delta {MEGA_DELTA_PRIORITY_DELAY_SECONDS if mega_backup_priority_enabled() else MEGA_DELTA_DELAY_SECONDS:g}с; SQLite snapshot после {int(MEGA_GLOBAL_QUIET_SECONDS)}с тишины / максимум {int(MEGA_GLOBAL_MAX_INTERVAL_SECONDS)}с\n"
                    f"/start"
                )
            except Exception as e:
                log_error(f"notify owner on start: {e}")
    try:
        app.run(host="0.0.0.0", port=PORT, threaded=True, use_reloader=False)
    finally:
        # Covers a normal Flask/process exit. SIGTERM/SIGINT already run the same routine first.
        try:
            runtime_graceful_shutdown("APP_EXIT")
        except Exception as e:
            log_error(f"final graceful shutdown: {e}")
# v141_operation_ledger_windows_expense_reminders_safety
