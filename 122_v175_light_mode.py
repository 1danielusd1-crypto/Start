# v178_global_performance_final
"""v175: owner-controlled light mode for isolating UI latency.

Light mode deliberately keeps the business core alive:
- Telegram webhook / UI callbacks
- finance and financial forwarding
- normal forwarding
- reminders
- task dispatcher
- SECRET
- SQLite working persistence
- compact MEGA delta / critical durable witnesses
- keep-alive and memory guard

It pauses only non-essential/background-heavy work:
- window lifecycle diagnostics
- durable journal uploads to MEGA (local journal remains)
- runtime watcher heartbeat uploads
- automatic Google Sheets updates (manual update remains available)
- per-chat full backup generation/channel upload
- automatic global full snapshot scheduling (delta remains active)
- window-registry root persistence and startup diagnostic scans
- source archive / safe-failed metadata repair while testing

The switch is global because these workers are process-wide.  It is intentionally a
runtime diagnostic switch, not a replacement for durable persistence.
"""
import threading as _v175_threading
import time as _v175_time

VERSION = "bot_v175_light_mode"
V175_FILE_MARKER = "v175_light_mode"
V175_HEAVY_SETTING = "heavy_processes_enabled_v175"

# Remember environment/default states so leaving light mode restores the original policy.
_V175_ORIGINAL_WINDOW_DIAGNOSTICS_ENABLED = bool(globals().get("WINDOW_DIAGNOSTICS_ENABLED", True))
_V175_ORIGINAL_JOURNAL_DURABLE_ENABLED = bool(globals().get("BOT_JOURNAL_DURABLE_ENABLED", True))
_V175_MODE_LOCK = _v175_threading.RLock()


def _v177_legacy_0328_heavy_processes_enabled_v175() -> bool:
    try:
        gs = data.setdefault("_global_settings", {})
        return bool(gs.get(V175_HEAVY_SETTING, True))
    except Exception:
        return True
try: _v177_legacy_0328_heavy_processes_enabled_v175.__name__ = 'heavy_processes_enabled_v175'
except Exception: pass
heavy_processes_enabled_v175 = _v177_legacy_0328_heavy_processes_enabled_v175


def _v177_legacy_0329_light_mode_enabled_v175() -> bool:
    return not heavy_processes_enabled_v175()
try: _v177_legacy_0329_light_mode_enabled_v175.__name__ = 'light_mode_enabled_v175'
except Exception: pass
light_mode_enabled_v175 = _v177_legacy_0329_light_mode_enabled_v175


def _v175_known_chat_ids() -> list[int]:
    ids = set()
    try:
        for cid in ((data or {}).get("chats", {}) or {}).keys():
            try:
                value = int(cid)
                if value:
                    ids.add(value)
            except Exception:
                pass
    except Exception:
        pass
    for name in ("_v167_known_chat_ids", "collect_all_known_chat_ids", "collect_finance_chat_ids"):
        fn = globals().get(name)
        if not callable(fn):
            continue
        try:
            for cid in fn() or []:
                try:
                    value = int(cid)
                    if value:
                        ids.add(value)
                except Exception:
                    pass
        except Exception:
            pass
    try:
        if OWNER_ID:
            ids.add(int(OWNER_ID))
    except Exception:
        pass
    return sorted(ids)


def _v175_cancel_heavy_timers() -> None:
    scheduler = globals().get("DELAYED_SCHEDULER")
    if scheduler is None:
        return
    keys = {
        "runtime-heartbeat",
        "journal-warm-tail",
        "mega-global-quiet-v90",
        "mega-global-max-v90",
        "mega-global-retry-v90",
        "window-registry-root-v168",
        "v146-window-registry-cleanup",
        "v146-failed-task-diagnostics",
        "v153-window-reconcile",
    }
    for cid in _v175_known_chat_ids():
        keys.update({
            f"full-backup:{cid}",
            f"google-change:{cid}",
            f"google-change-retry:{cid}",
        })
    for key in keys:
        try:
            scheduler.cancel(str(key))
        except Exception:
            pass


def _v175_persist_mode() -> None:
    """Persist the switch locally immediately; MEGA delta remains allowed in light mode."""
    try:
        SQLITE.save_root(_sqlite_pack_root(data))
    except Exception:
        try:
            save_data(data, root_only=True)
        except Exception:
            pass
    try:
        schedule_delta_backup(int(OWNER_ID or 0) or None, delay=0.5, reason="v175_heavy_process_switch")
    except Exception:
        pass


def _v175_apply_runtime_flags() -> None:
    enabled = heavy_processes_enabled_v175()
    try:
        globals()["WINDOW_DIAGNOSTICS_ENABLED"] = bool(_V175_ORIGINAL_WINDOW_DIAGNOSTICS_ENABLED and enabled)
    except Exception:
        pass
    try:
        globals()["BOT_JOURNAL_DURABLE_ENABLED"] = bool(_V175_ORIGINAL_JOURNAL_DURABLE_ENABLED and enabled)
    except Exception:
        pass
    if not enabled:
        _v175_cancel_heavy_timers()


def set_heavy_processes_enabled_v175(enabled: bool, actor_user_id: int = 0) -> bool:
    enabled = bool(enabled)
    with _V175_MODE_LOCK:
        try:
            data.setdefault("_global_settings", {})[V175_HEAVY_SETTING] = enabled
            data.setdefault("_global_settings", {})["heavy_processes_changed_at_v175"] = now_local().isoformat(timespec="seconds")
            data.setdefault("_global_settings", {})["heavy_processes_changed_by_v175"] = int(actor_user_id or 0)
        except Exception:
            pass
        _v175_apply_runtime_flags()
        _v175_persist_mode()

        if enabled:
            # Resume only one copy of deferred diagnostics/background work.
            try:
                DELAYED_SCHEDULER.schedule("runtime-heartbeat", 2.0, _runtime_heartbeat_job)
            except Exception:
                pass
            try:
                DELAYED_SCHEDULER.schedule("journal-warm-tail", 3.0, _journal_warm_tail_job)
            except Exception:
                pass
            try:
                # A full snapshot was intentionally deferred while light mode was active.
                if bool(globals().get("_global_snapshot_pending", False)):
                    _V175_ORIG_MARK_GLOBAL_SNAPSHOT_PENDING()
            except Exception:
                pass
            try:
                if callable(_V175_ORIG_WINDOW_REGISTRY_PERSIST):
                    _V175_ORIG_WINDOW_REGISTRY_PERSIST()
            except Exception:
                pass
        else:
            _v175_cancel_heavy_timers()

    try:
        bot_journal(
            "v175_heavy_processes",
            int(OWNER_ID or 0) or None,
            f"enabled={int(enabled)} actor={int(actor_user_id or 0)}; core=finance,forward,reminders,tasks,secret,sqlite,delta",
        )
    except Exception:
        pass
    return enabled


def _v177_legacy_0330_heavy_processes_label_v175() -> str:
    return "🧱 Тяжёлые процессы: ВКЛ" if heavy_processes_enabled_v175() else "⚡ Тяжёлые процессы: ВЫКЛ"
try: _v177_legacy_0330_heavy_processes_label_v175.__name__ = 'heavy_processes_label_v175'
except Exception: pass
heavy_processes_label_v175 = _v177_legacy_0330_heavy_processes_label_v175


def _v177_legacy_0331_heavy_processes_status_v175() -> str:
    if heavy_processes_enabled_v175():
        return (
            "🧱 Тяжёлые процессы: ВКЛ\n"
            "Работает полный штатный режим бота."
        )
    return (
        "⚡ Тяжёлые процессы: ВЫКЛ — ЛЁГКИЙ ТЕСТОВЫЙ РЕЖИМ\n"
        "Оставлено: финансы, пересылка, напоминалки, задачи, SECRET, SQLite, "
        "критический MEGA delta, keep-alive и защита памяти.\n"
        "Приостановлено: подробная диагностика окон, MEGA-журнал, watcher heartbeat, "
        "авто-Google, full backup/полные snapshots и служебные фоновые сканы.\n"
        "Уже запущенная тяжёлая задача может один раз завершиться после переключения."
    )
try: _v177_legacy_0331_heavy_processes_status_v175.__name__ = 'heavy_processes_status_v175'
except Exception: pass
heavy_processes_status_v175 = _v177_legacy_0331_heavy_processes_status_v175


# ---------------------------------------------------------------------------
# Dynamic gates. Existing workers resolve these names from the shared global namespace,
# so the switch takes effect without restarting the bot.
# ---------------------------------------------------------------------------
_V175_ORIG_WINDOW_REGISTRY_PERSIST = globals().get("_v168_schedule_window_registry_persist")
if callable(_V175_ORIG_WINDOW_REGISTRY_PERSIST):
    def _v168_schedule_window_registry_persist():
        if light_mode_enabled_v175():
            return None
        return _V175_ORIG_WINDOW_REGISTRY_PERSIST()


_V175_ORIG_MARK_GLOBAL_SNAPSHOT_PENDING = globals().get("_mark_global_snapshot_pending")
if callable(_V175_ORIG_MARK_GLOBAL_SNAPSHOT_PENDING):
    def _mark_global_snapshot_pending():
        global _global_snapshot_pending, _global_snapshot_last_change_monotonic
        if not light_mode_enabled_v175():
            return _V175_ORIG_MARK_GLOBAL_SNAPSHOT_PENDING()
        try:
            if RESTORE_GUARD_ACTIVE or not mega_is_configured():
                return None
            with _delta_state_lock:
                _global_snapshot_pending = True
                _global_snapshot_last_change_monotonic = _v175_time.monotonic()
        except Exception:
            pass
        return None


_V175_ORIG_FULL_BACKUP_ONLY = globals().get("schedule_full_backup_only")
if callable(_V175_ORIG_FULL_BACKUP_ONLY):
    def schedule_full_backup_only(chat_id: int, delay: float = 3.0):
        if light_mode_enabled_v175():
            try:
                with timer_lock:
                    _backup_dirty_chats.add(int(chat_id))
            except Exception:
                pass
            return None
        return _V175_ORIG_FULL_BACKUP_ONLY(int(chat_id), delay)


_V175_ORIG_RUNTIME_UPLOAD = globals().get("runtime_upload_snapshot")
if callable(_V175_ORIG_RUNTIME_UPLOAD):
    def runtime_upload_snapshot(event: str = "snapshot", immutable_event: bool = True) -> bool:
        low = str(event or "").casefold()
        # Keep rare shutdown/fatal forensic evidence even in light mode.
        if light_mode_enabled_v175() and not any(x in low for x in ("shutdown", "fatal")):
            return True
        return bool(_V175_ORIG_RUNTIME_UPLOAD(event, immutable_event))


_V175_ORIG_RUNTIME_HEARTBEAT = globals().get("_runtime_heartbeat_job")
if callable(_V175_ORIG_RUNTIME_HEARTBEAT):
    def _runtime_heartbeat_job():
        if light_mode_enabled_v175():
            return None
        return _V175_ORIG_RUNTIME_HEARTBEAT()


_V175_ORIG_JOURNAL_FLUSH = globals().get("journal_flush_to_mega")
if callable(_V175_ORIG_JOURNAL_FLUSH):
    def journal_flush_to_mega(force: bool = False) -> bool:
        if light_mode_enabled_v175():
            return True
        return bool(_V175_ORIG_JOURNAL_FLUSH(force))


_V175_ORIG_JOURNAL_WARM = globals().get("_journal_warm_tail_job")
if callable(_V175_ORIG_JOURNAL_WARM):
    def _journal_warm_tail_job():
        if light_mode_enabled_v175():
            return None
        return _V175_ORIG_JOURNAL_WARM()


# Automatic Google updates are paused. Explicit "Обновить сейчас" remains available.
_V175_ORIG_GOOGLE_AFTER_CHANGE = globals().get("_v169_schedule_google_after_change")
if callable(_V175_ORIG_GOOGLE_AFTER_CHANGE):
    def _v169_schedule_google_after_change(target_chat_id: int, reason: str = "finance_changed") -> None:
        if light_mode_enabled_v175():
            return None
        return _V175_ORIG_GOOGLE_AFTER_CHANGE(int(target_chat_id), reason)


_V175_ORIG_GOOGLE_ENQUEUE = globals().get("_v169_google_enqueue")
if callable(_V175_ORIG_GOOGLE_ENQUEUE):
    def _v169_google_enqueue(target_chat_id: int, reason: str) -> bool:
        low = str(reason or "").casefold()
        if light_mode_enabled_v175() and not low.startswith("manual"):
            return False
        return bool(_V175_ORIG_GOOGLE_ENQUEUE(int(target_chat_id), reason))


_V175_ORIG_ARCHIVE_SOURCE = globals().get("archive_current_bot_source_to_mega")
if callable(_V175_ORIG_ARCHIVE_SOURCE):
    def archive_current_bot_source_to_mega():
        if light_mode_enabled_v175():
            return True
        return _V175_ORIG_ARCHIVE_SOURCE()


_V175_ORIG_SAFE_FAILED_REPAIRS = globals().get("schedule_safe_failed_task_repairs")
if callable(_V175_ORIG_SAFE_FAILED_REPAIRS):
    def schedule_safe_failed_task_repairs(*args, **kwargs):
        if light_mode_enabled_v175():
            return False
        return _V175_ORIG_SAFE_FAILED_REPAIRS(*args, **kwargs)


_V175_ORIG_FAILED_DIAGNOSTICS = globals().get("refresh_failed_task_diagnostics")
if callable(_V175_ORIG_FAILED_DIAGNOSTICS):
    def refresh_failed_task_diagnostics(*args, **kwargs):
        if light_mode_enabled_v175():
            return []
        return _V175_ORIG_FAILED_DIAGNOSTICS(*args, **kwargs)


_V175_ORIG_WINDOW_CLEANUP = globals().get("cleanup_open_window_registry")
if callable(_V175_ORIG_WINDOW_CLEANUP):
    def cleanup_open_window_registry(*args, **kwargs):
        if light_mode_enabled_v175():
            return {"skipped": "v175_light_mode"}
        return _V175_ORIG_WINDOW_CLEANUP(*args, **kwargs)


# Apply persisted mode as soon as restore has populated data, before READY schedules watcher jobs.
_V175_ORIG_RUNTIME_MARK_READY = globals().get("runtime_mark_ready")
if callable(_V175_ORIG_RUNTIME_MARK_READY):
    def runtime_mark_ready(detail: str = ""):
        _v175_apply_runtime_flags()
        result = _V175_ORIG_RUNTIME_MARK_READY(detail)
        if light_mode_enabled_v175():
            _v175_cancel_heavy_timers()
        return result


# ---------------------------------------------------------------------------
# INFO UI: one owner-only switch, no extra window required.
# ---------------------------------------------------------------------------
_V175_PREV_BUILD_INFO_TEXT = globals().get("build_info_text")
def _v177_legacy_0058_build_info_text(chat_id: int, *args, **kwargs) -> str:
    try:
        base = str(_V175_PREV_BUILD_INFO_TEXT(int(chat_id), *args, **kwargs) if callable(_V175_PREV_BUILD_INFO_TEXT) else "")
    except TypeError:
        base = str(_V175_PREV_BUILD_INFO_TEXT(int(chat_id)) if callable(_V175_PREV_BUILD_INFO_TEXT) else "")
    try:
        if int(chat_id) == int(OWNER_ID or 0):
            line = "🧱 Тяжёлые процессы: ВКЛ" if heavy_processes_enabled_v175() else "⚡ Тяжёлые процессы: ВЫКЛ (лёгкий режим)"
            if line not in base:
                base = (base.rstrip() + "\n\n" + line).strip()
    except Exception:
        pass
    return base[:3900]
try: _v177_legacy_0058_build_info_text.__name__ = 'build_info_text'
except Exception: pass
build_info_text = _v177_legacy_0058_build_info_text


_V175_PREV_BUILD_INFO_KEYBOARD = globals().get("build_info_keyboard")
def _v177_legacy_0222_build_info_keyboard(chat_id: int):
    kb = _V175_PREV_BUILD_INFO_KEYBOARD(int(chat_id)) if callable(_V175_PREV_BUILD_INFO_KEYBOARD) else types.InlineKeyboardMarkup()
    try:
        if int(chat_id) != int(OWNER_ID or 0):
            return kb
        rows = list(getattr(kb, "keyboard", None) or [])
        # Avoid duplicate after hot-load/rebuild.
        for row in rows:
            for btn in row or []:
                if str(getattr(btn, "callback_data", "") or "") == "v175:heavy_toggle":
                    btn.text = heavy_processes_label_v175()
                    return kb
        # Insert before navigation/close rows when possible.
        button_row = [IB(heavy_processes_label_v175(), callback_data="v175:heavy_toggle")]
        insert_at = len(rows)
        for idx, row in enumerate(rows):
            callbacks = {str(getattr(btn, "callback_data", "") or "") for btn in (row or [])}
            labels = " ".join(str(getattr(btn, "text", "") or "") for btn in (row or [])).casefold()
            if "info_close" in callbacks or "back_main" in " ".join(callbacks) or "назад" in labels or "закры" in labels:
                insert_at = idx
                break
        rows.insert(insert_at, button_row)
        kb.keyboard = rows
    except Exception:
        pass
    return kb
try: _v177_legacy_0222_build_info_keyboard.__name__ = 'build_info_keyboard'
except Exception: pass
build_info_keyboard = _v177_legacy_0222_build_info_keyboard


def _v175_callback_filter(call):
    try:
        return str(getattr(call, "data", "") or "").startswith("v175:")
    except Exception:
        return False


def _v175_callback(call):
    raw = str(getattr(call, "data", "") or "")
    if raw != "v175:heavy_toggle":
        return
    try:
        chat_id = int(call.message.chat.id)
        user_id = int(getattr(getattr(call, "from_user", None), "id", 0) or 0)
    except Exception:
        return
    if chat_id != int(OWNER_ID or 0) or user_id != int(OWNER_ID or 0):
        try:
            bot.answer_callback_query(call.id, "Только основной владелец может менять этот режим.", show_alert=True)
        except Exception:
            pass
        return
    new_enabled = not heavy_processes_enabled_v175()
    set_heavy_processes_enabled_v175(new_enabled, user_id)
    try:
        bot.answer_callback_query(
            call.id,
            "Тяжёлые процессы включены" if new_enabled else "Лёгкий режим включён: фоновые тяжёлые процессы остановлены",
            show_alert=False,
        )
    except Exception:
        pass
    try:
        safe_edit(bot, call, build_info_text(chat_id), reply_markup=build_info_keyboard(chat_id))
    except Exception:
        try:
            fast_ui_edit_message_text(
                chat_id, int(call.message.message_id), build_info_text(chat_id),
                reply_markup=build_info_keyboard(chat_id), purpose="light_mode_fallback_v178",
            )
        except Exception:
            pass


def _v175_register_callback() -> int:
    try:
        bot.callback_query_handler(func=_v175_callback_filter)(_v175_callback)
        handlers = getattr(bot, "callback_query_handlers", None)
        if isinstance(handlers, list) and handlers:
            row = handlers.pop()
            handlers.insert(0, row)
        return 1
    except Exception:
        return 0


# Queue/status text should show the experiment state without opening another diagnostics window.
_V175_PREV_QUEUE_STATUS = globals().get("build_queue_status_text")
if callable(_V175_PREV_QUEUE_STATUS):
    def build_queue_status_text() -> str:
        base = str(_V175_PREV_QUEUE_STATUS() or "")
        return (base.rstrip() + "\n\n" + heavy_processes_status_v175())[:3900]


# Restore compatibility for v175 snapshots.
_V175_PREV_RESTORE_VALIDATOR = globals().get("_v153_validate_restore_gz")
def _v177_legacy_0292_v153_validate_restore_gz(gz_path: str):
    try:
        return _V175_PREV_RESTORE_VALIDATOR(gz_path) if callable(_V175_PREV_RESTORE_VALIDATOR) else (None, None)
    except Exception as exc:
        if "unsupported bot version" not in str(exc):
            raise

    import gzip, shutil, sqlite3, tempfile, json
    folder = tempfile.mkdtemp(prefix="v175_restore_validate_")
    raw = os.path.join(folder, "restore.sqlite3")
    try:
        with gzip.open(gz_path, "rb") as fin, open(raw, "wb") as fout:
            shutil.copyfileobj(fin, fout, 1024 * 1024)
        conn = sqlite3.connect(raw)
        try:
            integrity = str(conn.execute("PRAGMA integrity_check").fetchone()[0])
            if integrity.lower() != "ok":
                raise RuntimeError(f"SQLite integrity_check: {integrity}")
            row = conn.execute("SELECT v FROM meta WHERE kind='v153_export' AND k='manifest'").fetchone()
            if not row:
                raise RuntimeError("manifest v153 not found")
            manifest = json.loads(row[0])
        finally:
            conn.close()
        if str(manifest.get("kind")) != "telegram_bot_full_state_v153":
            raise RuntimeError("unknown export kind")
        if int(manifest.get("schema_version") or 0) != int(V153_EXPORT_SCHEMA):
            raise RuntimeError("unsupported export schema")
        export_version = str(manifest.get("bot_version") or "")
        if not export_version.startswith(tuple(f"bot_v{i}_" for i in range(153, 176))):
            raise RuntimeError(f"unsupported bot version: {export_version or 'missing'}")
        if _v153_db_logical_checksum(raw) != str(manifest.get("checksum") or ""):
            raise RuntimeError("checksum mismatch")
        return manifest, raw
    except Exception:
        shutil.rmtree(folder, ignore_errors=True)
        raise
try: _v177_legacy_0292_v153_validate_restore_gz.__name__ = '_v153_validate_restore_gz'
except Exception: pass
_v153_validate_restore_gz = _v177_legacy_0292_v153_validate_restore_gz


_V175_CALLBACK_REGISTERED = _v175_register_callback()
try:
    bot_journal(
        "v175_installed",
        int(OWNER_ID or 0) or None,
        f"light_mode_switch=1 callback={_V175_CALLBACK_REGISTERED}; default_heavy={int(heavy_processes_enabled_v175())}",
    )
except Exception:
    pass
# v178_global_performance_final
