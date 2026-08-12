# v184_full_restore_contract
"""v178 GLOBAL FINAL: process control center + callback latency diagnostics for every contour.

This layer replaces the single v175 heavy-process switch with granular runtime gates.
The bot's mission-critical core remains visible and locked in the menu; optional/background
work can be toggled independently to isolate UI latency without rewriting business logic.
"""
import collections as _v176_collections
import json as _v176_json
import re as _v176_re
import statistics as _v176_statistics
import threading as _v176_threading
import time as _v176_time

VERSION = "bot_v178_global_performance_final"
V176_FILE_MARKER = "v178_global_performance_final"
V176_SETTINGS_KEY = "process_control_v176"
_V176_LOCK = _v176_threading.RLock()
_V176_PERF = _v176_collections.deque(maxlen=240)
_V177_PERF_STAGES = _v176_collections.deque(maxlen=720)
_V177_PERF_LOCAL = _v176_threading.local()

def v177_perf_stage(name: str, elapsed: float) -> None:
    """Record a timed sub-stage inside the currently executing callback."""
    try:
        action = str(getattr(_V177_PERF_LOCAL, "action", "") or "")[:120]
        _V177_PERF_STAGES.append({
            "ts": _v176_time.time(), "action": action,
            "stage": str(name or "stage")[:80], "elapsed": max(0.0, float(elapsed or 0.0)),
        })
    except Exception:
        pass

def v177_perf_clear() -> None:
    try: _V176_PERF.clear()
    except Exception: pass
    try: _V177_PERF_STAGES.clear()
    except Exception: pass

# Capture v175 aggregate state once, then neutralize the broad gate. Granular v176 gates own runtime policy.
# v179 has no v175 runtime layer. Preserve existing granular settings; new installs default to normal mode.
_V176_MIGRATED_HEAVY = True

# code, page, label, default, risk, description
_V176_PROCESS_DEFS = {
    "ui_retry": ("ui", "🔁 Повторы edit Telegram", True, "hot", "Внешние повторы safe_edit при 429/ошибке. Кандидат на задержку кнопок."),
    "win_diag": ("ui", "🩺 Диагностика окон", False, "hot", "Маркировка/диагностика жизненного цикла окон."),
    "win_reg": ("ui", "🗂 Реестр окон → SQLite/MEGA", _V176_MIGRATED_HEAVY, "hot", "Фоновое сохранение реестра открытых окон."),
    "win_rec": ("ui", "🔄 Reconcile окон", _V176_MIGRATED_HEAVY, "hot", "Сверка реестра окон, включая цикл v153 каждые 600 сек."),
    "btn_chain": ("ui", "🧾 Трассировка press/result", False, "hot", "Две дополнительные записи на обработанный callback."),
    "btn_press": ("ui", "📝 Журнал button_pressed", False, "hot", "Запись исходной кнопки в общий журнал."),
    "win_journal": ("ui", "📐 Window-журнал", False, "hot", "Подробные window_* события, которые раньше писались принудительно."),
    "fin_refresh": ("ui", "💹 Автообновление фин. окон", True, "medium", "Автоперерисовка зарегистрированных финансовых окон после изменений."),
    "win_cleanup": ("ui", "🧹 Очистка реестра окон", _V176_MIGRATED_HEAVY, "medium", "Скан/очистка устаревших окон."),

    "delta_auto": ("mega", "☁️ MEGA delta авто", True, "critical", "Маленькие отложенные внешние delta-сохранения изменений."),
    "delta_critical": ("mega", "🛡 MEGA delta критическая", True, "critical", "Синхронный внешний свидетель критичных финансовых операций."),
    "full_chat": ("mega", "📦 Full backup чата", _V176_MIGRATED_HEAVY, "medium", "Формирование полных резервных файлов отдельных чатов."),
    "full_global": ("mega", "🌐 Global full snapshot", _V176_MIGRATED_HEAVY, "medium", "Полный глобальный snapshot после периода тишины/макс. интервала."),
    "journal_mega": ("mega", "📓 MEGA-журнал", _V176_MIGRATED_HEAVY, "medium", "Durable journal buffer и его фоновые выгрузки."),
    "runtime_upload": ("mega", "📡 Runtime watcher upload", _V176_MIGRATED_HEAVY, "medium", "Heartbeat/диагностические snapshots runtime в MEGA."),
    "source_archive": ("mega", "🗃 Архив исходника", _V176_MIGRATED_HEAVY, "low", "Фоновое архивирование текущего исходника в MEGA."),
    "failed_repair": ("mega", "🩹 Repair failed-задач", False, "medium", "Автоматический безопасный ремонт failed durable tasks."),
    "failed_diag": ("mega", "🔬 Диагностика failed-задач", False, "medium", "Фоновый диагностический скан failed tasks."),
    "mega_maint": ("mega", "🧰 MEGA cleanup", True, "medium", "Очистка старых runtime-артефактов внутри /TelegramBotBackups; миграции корня больше нет."),
    "mega_recover": ("mega", "♻️ Startup recovery MEGA-задач", True, "critical", "Восстановление pending/running durable tasks после рестарта."),
    "lease": ("mega", "🔐 Instance lease", True, "critical", "Проверка второго одновременно работающего экземпляра."),

    "google_auto": ("auto", "📊 Google Sheets авто", _V176_MIGRATED_HEAVY, "medium", "Автоматическая синхронизация после финансовых изменений; ручная остаётся."),
    "reminders": ("auto", "⏰ Напоминалки", True, "critical", "Планировщик и отправка напоминаний."),
    "expense_ping": ("auto", "📲 Expense ping", True, "medium", "Автоматическая доставка shortcut-событий расходов и recovery."),
    "lowram": ("system", "🧊 Low-RAM idle sweep", True, "medium", "Выгрузка холодной истории из RAM в SQLite, когда бот свободен."),
    "memory_guard": ("system", "🧠 Memory guard", True, "critical", "Контроль RAM, trim и защитный restart при аварийной памяти."),
}

_V176_PAGE_TITLES = {
    "ui": "⚡ ИНТЕРФЕЙС / КНОПКИ",
    "mega": "☁️ MEGA / BACKUP",
    "auto": "🔄 АВТОМАТИКА",
    "system": "🧠 СИСТЕМА",
    "core": "🔒 ОСНОВА БОТА",
}

_V176_LOCKED_CORE = [
    ("🌐 Telegram webhook / приём update", "Всегда ВКЛ — без него бот не принимает события."),
    ("💰 Финансовый учёт", "Всегда ВКЛ — основная миссия бота."),
    ("💸 Финансовая пересылка", "Всегда ВКЛ — приоритетная бизнес-линия."),
    ("➡️ Обычная пересылка", "Всегда ВКЛ — штатная бизнес-функция."),
    ("🪷 SECRET", "Всегда ВКЛ — штатная бизнес-функция."),
    ("📋 Диспетчер задач", "Всегда ВКЛ — штатная бизнес-функция."),
    ("💾 SQLite", "Всегда ВКЛ — локальное рабочее состояние; не отключается диагностикой."),
    ("❤️ Web keep-alive", "Всегда ВКЛ — жизнеспособность Render/web runtime."),
]


def _v176_root() -> dict:
    try:
        gs = data.setdefault("_global_settings", {})
        root = gs.setdefault(V176_SETTINGS_KEY, {})
    except Exception:
        return {}
    if not root.get("initialized"):
        for code, (_page, _label, default, _risk, _desc) in _V176_PROCESS_DEFS.items():
            root.setdefault(code, bool(default))
        root["initialized"] = True
        root["migrated_from_v175_heavy"] = bool(_V176_MIGRATED_HEAVY)
        try:
            root["created_at"] = now_local().isoformat(timespec="seconds")
        except Exception:
            pass
    return root


def v176_process_enabled(code: str) -> bool:
    row = _V176_PROCESS_DEFS.get(str(code))
    if not row:
        return True
    try:
        return bool(_v176_root().get(str(code), row[2]))
    except Exception:
        return bool(row[2])


def _v176_persist(reason: str = "process_control") -> None:
    try:
        SQLITE.save_root(_sqlite_pack_root(data))
    except Exception:
        try:
            save_data(data, root_only=True)
        except Exception:
            pass
    # Process-control settings themselves are always externally witnessed once, even if auto delta is disabled.
    try:
        if callable(_V176_ORIG_SCHEDULE_DELTA):
            _V176_ORIG_SCHEDULE_DELTA(int(OWNER_ID or 0) or None, delay=0.5, reason=f"v176_{reason}")
    except Exception:
        pass


# Neutralize v175 all-or-nothing light gate after defaults were migrated.
def heavy_processes_enabled_v175() -> bool:
    return True


def light_mode_enabled_v175() -> bool:
    return False


def heavy_processes_label_v175() -> str:
    return "⚙️ Процессы / скорость"


def heavy_processes_status_v175() -> str:
    off = sum(1 for code in _V176_PROCESS_DEFS if not v176_process_enabled(code))
    return f"⚙️ Центр процессов v179: отключено {off} из {len(_V176_PROCESS_DEFS)} управляемых процессов."


# Capture the current active implementations (including v175 compatibility wrappers).
_V176_ORIG_BOT_JOURNAL = globals().get("bot_journal")
_V176_ORIG_WINDOW_REGISTRY = globals().get("_v168_schedule_window_registry_persist")
_V176_ORIG_WINDOW_RECONCILE = globals().get("_v153_reconcile_windows")
_V176_ORIG_WINDOW_CLEANUP = globals().get("cleanup_open_window_registry")
_V176_ORIG_FIN_REFRESH = globals().get("schedule_financial_window_refresh")
_V176_ORIG_SCHEDULE_DELTA = globals().get("schedule_delta_backup")
_V176_ORIG_CRITICAL_DELTA = globals().get("persist_critical_delta_now")
_V176_ORIG_FULL_BACKUP = globals().get("schedule_full_backup_only")
_V176_ORIG_GLOBAL_SNAPSHOT = globals().get("_mark_global_snapshot_pending")
_V176_ORIG_JOURNAL_FLUSH = globals().get("journal_flush_to_mega")
_V176_ORIG_JOURNAL_WARM = globals().get("_journal_warm_tail_job")
_V176_ORIG_RUNTIME_HEARTBEAT = globals().get("_runtime_heartbeat_job")
_V176_ORIG_RUNTIME_UPLOAD = globals().get("runtime_upload_snapshot")
_V176_ORIG_SOURCE_ARCHIVE = globals().get("archive_current_bot_source_to_mega")
_V176_ORIG_FAILED_REPAIR = globals().get("schedule_safe_failed_task_repairs")
_V176_ORIG_FAILED_DIAG = globals().get("refresh_failed_task_diagnostics")
_V176_ORIG_MEGA_MIGRATION = globals().get("_v153_schedule_migration")
_V176_ORIG_RUNTIME_CLEANUP = globals().get("_v153_runtime_cleanup_remote")
_V176_ORIG_INSTANCE_LEASE = globals().get("_v153_instance_lease_check")
_V176_ORIG_MEGA_RECOVERY = globals().get("schedule_mega_task_recovery")
_V176_ORIG_GOOGLE_AFTER = globals().get("_v169_schedule_google_after_change")
_V176_ORIG_GOOGLE_ENQUEUE = globals().get("_v169_google_enqueue")
_V176_ORIG_REMINDER_TICK = globals().get("_reminder_tick")
_V176_ORIG_EXPENSE_ENQUEUE = globals().get("enqueue_expense_ping_event")
_V176_ORIG_EXPENSE_RECOVERY = globals().get("schedule_expense_ping_recovery")
_V176_ORIG_LOWRAM_SWEEP = globals().get("_lowram_idle_sweep_job")
_V176_ORIG_MEMORY_GUARD = globals().get("memory_guard_tick")


# Journal gates: existing callbacks resolve bot_journal dynamically, so no handler rewrite is needed.
def bot_journal(action: str, chat_id=None, detail: str = "", level: str = "INFO"):
    """Final journal gate: one public implementation, no v176→v153→core wrapper chain."""
    action_s = str(action or "")
    level_s = str(level or "INFO")
    if level_s.upper() not in {"ERROR", "CRITICAL"}:
        if action_s.startswith("button_chain_") and not v176_process_enabled("btn_chain"):
            return None
        if action_s == "button_pressed" and not v176_process_enabled("btn_press"):
            return None
        if action_s.startswith("window_") and not v176_process_enabled("win_journal"):
            return None
    base = globals().get("_V153_ORIG_BOT_JOURNAL")
    if callable(base):
        try:
            sanitizer = globals().get("v153_sanitize")
            clean_detail = sanitizer(detail) if callable(sanitizer) else detail
        except Exception:
            clean_detail = detail
        return base(action_s, chat_id, clean_detail, level_s)
    if callable(_V176_ORIG_BOT_JOURNAL):
        return _V176_ORIG_BOT_JOURNAL(action_s, chat_id, detail, level_s)
    return None


def _v177_deferred_ui_retry(chat_id: int, message_id: int, text: str, reply_markup=None, parse_mode=None, purpose: str = "ui") -> None:
    """One non-blocking retry outside the callback thread."""
    try:
        fast_ui_edit_message_text(
            int(chat_id), int(message_id), text, reply_markup=reply_markup,
            parse_mode=parse_mode, purpose=str(purpose or "ui") + "_deferred",
        )
    except Exception:
        pass


def _v161_edit_retry(chat_id: int, message_id: int, text: str, reply_markup=None, parse_mode=None, purpose: str = "ui") -> str:
    """Final UI edit policy: one synchronous Telegram attempt, optional retry in background."""
    started = _v176_time.monotonic()
    try:
        result = str(fast_ui_edit_message_text(
            int(chat_id), int(message_id), text, reply_markup=reply_markup,
            parse_mode=parse_mode, purpose=purpose,
        ) or "failed")
    except Exception:
        result = "failed"
    v177_perf_stage("telegram_edit", _v176_time.monotonic() - started)
    if result in {"rate_limited", "failed"} and v176_process_enabled("ui_retry"):
        try:
            pool = globals().get("GENERAL_TASK_POOL")
            key = f"v177-ui-retry:{int(chat_id)}:{int(message_id)}"
            if pool is not None:
                pool.submit_unique(
                    key, _v177_deferred_ui_retry, int(chat_id), int(message_id), text,
                    reply_markup, parse_mode, purpose,
                )
            return "scheduled"
        except Exception:
            pass
    return result


if callable(_V176_ORIG_WINDOW_REGISTRY):
    def _v168_schedule_window_registry_persist():
        if not v176_process_enabled("win_reg"):
            return None
        return _V176_ORIG_WINDOW_REGISTRY()

if callable(_V176_ORIG_WINDOW_RECONCILE):
    def _v153_reconcile_windows():
        if not v176_process_enabled("win_rec"):
            return {"skipped": "v176_win_rec_off"}
        return _V176_ORIG_WINDOW_RECONCILE()

if callable(_V176_ORIG_WINDOW_CLEANUP):
    def cleanup_open_window_registry(*args, **kwargs):
        if not v176_process_enabled("win_cleanup"):
            return {"skipped": "v176_win_cleanup_off"}
        return _V176_ORIG_WINDOW_CLEANUP(*args, **kwargs)

if callable(_V176_ORIG_FIN_REFRESH):
    def schedule_financial_window_refresh(chat_id: int, day_key=None, reason: str = "finance_changed", delay: float = 0.0):
        """Final finance refresh path: v166 refresh + optional Google trigger, no wrapper cascade."""
        if not v176_process_enabled("fin_refresh"):
            return False
        result = None
        base = globals().get("_V169_BASE_SCHEDULE_FINANCIAL_WINDOW_REFRESH")
        if callable(base):
            result = base(int(chat_id), day_key, reason=reason, delay=delay)
        elif callable(_V176_ORIG_FIN_REFRESH):
            result = _V176_ORIG_FIN_REFRESH(int(chat_id), day_key, reason=reason, delay=delay)
        if v176_process_enabled("google_auto"):
            try:
                hook = globals().get("_v169_schedule_google_after_change")
                if callable(hook): hook(int(chat_id), reason)
            except Exception:
                pass
        return result

if callable(_V176_ORIG_SCHEDULE_DELTA):
    def schedule_delta_backup(chat_id=None, delay=None, reason="change"):
        if not v176_process_enabled("delta_auto"):
            return False
        return _V176_ORIG_SCHEDULE_DELTA(chat_id, delay=delay, reason=reason)

if callable(_V176_ORIG_CRITICAL_DELTA):
    def persist_critical_delta_now(chat_id: int) -> bool:
        if not v176_process_enabled("delta_critical"):
            # Diagnostic bypass: business action is not failed merely because external witness was intentionally disabled.
            return True
        return bool(_V176_ORIG_CRITICAL_DELTA(int(chat_id)))

if callable(_V176_ORIG_FULL_BACKUP):
    def schedule_full_backup_only(chat_id: int, delay: float = 3.0):
        if not v176_process_enabled("full_chat"):
            return None
        return _V176_ORIG_FULL_BACKUP(int(chat_id), delay)

if callable(_V176_ORIG_GLOBAL_SNAPSHOT):
    def _mark_global_snapshot_pending():
        if not v176_process_enabled("full_global"):
            try:
                globals()["_global_snapshot_pending"] = True
                globals()["_global_snapshot_last_change_monotonic"] = _v176_time.monotonic()
            except Exception:
                pass
            return None
        return _V176_ORIG_GLOBAL_SNAPSHOT()

if callable(_V176_ORIG_JOURNAL_FLUSH):
    def journal_flush_to_mega(force: bool = False) -> bool:
        if not v176_process_enabled("journal_mega"):
            return True
        return bool(_V176_ORIG_JOURNAL_FLUSH(force))

if callable(_V176_ORIG_JOURNAL_WARM):
    def _journal_warm_tail_job():
        if not v176_process_enabled("journal_mega"):
            return None
        return _V176_ORIG_JOURNAL_WARM()

V178_RUNTIME_REMOTE_HEARTBEAT_SECONDS = max(120.0, min(900.0, float(os.getenv("RUNTIME_REMOTE_HEARTBEAT_SECONDS", "180") or "180")))

if callable(_V176_ORIG_RUNTIME_HEARTBEAT):
    def _runtime_heartbeat_job():
        """v178: remote diagnostic heartbeat is slow-background, never 30-second MEGA churn."""
        if runtime_is_shutting_down() or not v176_process_enabled("runtime_upload"):
            return None
        next_delay = V178_RUNTIME_REMOTE_HEARTBEAT_SECONDS
        try:
            busy_fn = globals().get("_runtime_watcher_should_yield_to_critical_mega")
            if callable(busy_fn) and busy_fn():
                try: runtime_event("watcher_heartbeat_deferred", "critical MEGA work has priority")
                except Exception: pass
                next_delay = min(90.0, V178_RUNTIME_REMOTE_HEARTBEAT_SECONDS)
            else:
                GENERAL_TASK_POOL.submit_unique("runtime-heartbeat-upload", runtime_upload_snapshot, "heartbeat", False)
        finally:
            try: DELAYED_SCHEDULER.schedule("runtime-heartbeat", next_delay, _runtime_heartbeat_job)
            except Exception: pass
        return True

if callable(_V176_ORIG_RUNTIME_UPLOAD):
    def runtime_upload_snapshot(event: str = "snapshot", immutable_event: bool = True) -> bool:
        low = str(event or "").casefold()
        if not v176_process_enabled("runtime_upload") and not any(x in low for x in ("shutdown", "fatal")):
            return True
        return bool(_V176_ORIG_RUNTIME_UPLOAD(event, immutable_event))

if callable(_V176_ORIG_SOURCE_ARCHIVE):
    def archive_current_bot_source_to_mega():
        if not v176_process_enabled("source_archive"):
            return True
        return _V176_ORIG_SOURCE_ARCHIVE()

if callable(_V176_ORIG_FAILED_REPAIR):
    def schedule_safe_failed_task_repairs(*args, **kwargs):
        if not v176_process_enabled("failed_repair"):
            return False
        return _V176_ORIG_FAILED_REPAIR(*args, **kwargs)

if callable(_V176_ORIG_FAILED_DIAG):
    def refresh_failed_task_diagnostics(*args, **kwargs):
        if not v176_process_enabled("failed_diag"):
            return []
        return _V176_ORIG_FAILED_DIAG(*args, **kwargs)

if callable(_V176_ORIG_MEGA_MIGRATION):
    def _v153_schedule_migration(*args, **kwargs):
        if not v176_process_enabled("mega_maint"):
            return False
        return _V176_ORIG_MEGA_MIGRATION(*args, **kwargs)

if callable(_V176_ORIG_RUNTIME_CLEANUP):
    def _v153_runtime_cleanup_remote(*args, **kwargs):
        if not v176_process_enabled("mega_maint"):
            return {"skipped": "v176_mega_maint_off"}
        return _V176_ORIG_RUNTIME_CLEANUP(*args, **kwargs)

if callable(_V176_ORIG_INSTANCE_LEASE):
    def _v153_instance_lease_check(*args, **kwargs):
        if not v176_process_enabled("lease"):
            return {"skipped": "v176_instance_lease_off"}
        return _V176_ORIG_INSTANCE_LEASE(*args, **kwargs)

if callable(_V176_ORIG_MEGA_RECOVERY):
    def schedule_mega_task_recovery(*args, **kwargs):
        if not v176_process_enabled("mega_recover"):
            return False
        return _V176_ORIG_MEGA_RECOVERY(*args, **kwargs)

if callable(_V176_ORIG_GOOGLE_AFTER):
    def _v169_schedule_google_after_change(target_chat_id: int, reason: str = "finance_changed") -> None:
        if not v176_process_enabled("google_auto"):
            return None
        return _V176_ORIG_GOOGLE_AFTER(int(target_chat_id), reason)

if callable(_V176_ORIG_GOOGLE_ENQUEUE):
    def _v169_google_enqueue(target_chat_id: int, reason: str) -> bool:
        low = str(reason or "").casefold()
        if not v176_process_enabled("google_auto") and not low.startswith("manual"):
            return False
        return bool(_V176_ORIG_GOOGLE_ENQUEUE(int(target_chat_id), reason))

if callable(_V176_ORIG_REMINDER_TICK):
    def _reminder_tick() -> None:
        if not v176_process_enabled("reminders"):
            return None
        return _V176_ORIG_REMINDER_TICK()

if callable(_V176_ORIG_EXPENSE_ENQUEUE):
    def enqueue_expense_ping_event(source: str = "iphone", force: bool = False):
        if not v176_process_enabled("expense_ping"):
            return "", False
        return _V176_ORIG_EXPENSE_ENQUEUE(source, force)

if callable(_V176_ORIG_EXPENSE_RECOVERY):
    def schedule_expense_ping_recovery(*args, **kwargs):
        if not v176_process_enabled("expense_ping"):
            return False
        return _V176_ORIG_EXPENSE_RECOVERY(*args, **kwargs)

if callable(_V176_ORIG_LOWRAM_SWEEP):
    def _lowram_idle_sweep_job():
        if not v176_process_enabled("lowram"):
            return None
        return _V176_ORIG_LOWRAM_SWEEP()

if callable(_V176_ORIG_MEMORY_GUARD):
    def memory_guard_tick():
        if not v176_process_enabled("memory_guard"):
            return None
        return _V176_ORIG_MEMORY_GUARD()



def _v176_cancel_for(code: str) -> None:
    scheduler = globals().get("DELAYED_SCHEDULER")
    if scheduler is None:
        return
    keys = {
        "win_reg": ["window-registry-root-v168"],
        "win_rec": ["v153-window-reconcile"],
        "runtime_upload": ["runtime-heartbeat"],
        "journal_mega": ["journal-warm-tail", "journal-error-flush"],
        "full_global": ["mega-global-quiet-v90", "mega-global-max-v90", "mega-global-retry-v90"],
        "failed_repair": ["mega-task-safe-failed-repair"],
        "failed_diag": ["v146-failed-task-diagnostics"],
        "mega_maint": ["v153-runtime-cleanup"],
        "mega_recover": ["mega-task-startup-recovery"],
        "lowram": ["lowram-idle-sweep"],
        "memory_guard": ["memory-guard"],
        "expense_ping": ["expense-ping-recovery"],
    }.get(code, [])
    for key in keys:
        try:
            scheduler.cancel(key)
        except Exception:
            pass


def _v176_resume_for(code: str) -> None:
    scheduler = globals().get("DELAYED_SCHEDULER")
    if scheduler is None:
        return
    try:
        if code == "win_reg" and callable(globals().get("_v168_schedule_window_registry_persist")):
            globals()["_v168_schedule_window_registry_persist"]()
        elif code == "win_rec" and callable(globals().get("_v153_reconcile_windows")):
            GENERAL_TASK_POOL.submit_unique("v176-window-reconcile", globals()["_v153_reconcile_windows"])
        elif code == "runtime_upload" and callable(globals().get("_runtime_heartbeat_job")):
            scheduler.schedule("runtime-heartbeat", 2.0, globals()["_runtime_heartbeat_job"])
        elif code == "journal_mega" and callable(globals().get("_journal_warm_tail_job")):
            scheduler.schedule("journal-warm-tail", 3.0, globals()["_journal_warm_tail_job"])
        elif code == "full_global" and bool(globals().get("_global_snapshot_pending", False)) and callable(globals().get("_mark_global_snapshot_pending")):
            globals()["_mark_global_snapshot_pending"]()
        elif code == "failed_repair" and callable(globals().get("schedule_safe_failed_task_repairs")):
            globals()["schedule_safe_failed_task_repairs"](2.0)
        elif code == "failed_diag" and callable(globals().get("refresh_failed_task_diagnostics")):
            GENERAL_TASK_POOL.submit_unique("v176-failed-diag", globals()["refresh_failed_task_diagnostics"], True)
        elif code == "mega_maint" and callable(globals().get("_v153_runtime_cleanup_job")):
            scheduler.schedule("v153-runtime-cleanup", 2.0, globals()["_v153_runtime_cleanup_job"])
        elif code == "mega_recover" and callable(globals().get("schedule_mega_task_recovery")):
            globals()["schedule_mega_task_recovery"](0.5)
        elif code == "lowram" and callable(globals().get("_lowram_idle_sweep_job")):
            scheduler.schedule("lowram-idle-sweep", 2.0, globals()["_lowram_idle_sweep_job"])
        elif code == "memory_guard" and callable(globals().get("memory_guard_tick")):
            scheduler.schedule("memory-guard", 2.0, globals()["memory_guard_tick"])
        elif code == "expense_ping" and callable(globals().get("schedule_expense_ping_recovery")):
            globals()["schedule_expense_ping_recovery"](0.5)
    except Exception:
        pass


def _v176_apply_runtime_flags() -> None:
    try:
        globals()["WINDOW_DIAGNOSTICS_ENABLED"] = bool(v176_process_enabled("win_diag"))
    except Exception:
        pass
    try:
        globals()["BOT_JOURNAL_DURABLE_ENABLED"] = bool(v176_process_enabled("journal_mega"))
    except Exception:
        pass
    # Stop already armed one-shot jobs for disabled gates. Self-rescheduling loops use dynamic wrappers.
    for code in _V176_PROCESS_DEFS:
        if not v176_process_enabled(code):
            _v176_cancel_for(code)


def v176_set_process(code: str, enabled: bool, actor: int = 0) -> bool:
    if code not in _V176_PROCESS_DEFS:
        return False
    with _V176_LOCK:
        root = _v176_root()
        root[code] = bool(enabled)
        try:
            root["changed_at"] = now_local().isoformat(timespec="seconds")
            root["changed_by"] = int(actor or 0)
            root["last_changed"] = str(code)
        except Exception:
            pass
        if code == "win_diag":
            globals()["WINDOW_DIAGNOSTICS_ENABLED"] = bool(enabled)
        if code == "journal_mega":
            globals()["BOT_JOURNAL_DURABLE_ENABLED"] = bool(enabled)
        if enabled:
            _v176_resume_for(code)
        else:
            _v176_cancel_for(code)
        _v176_persist(f"toggle_{code}_{int(bool(enabled))}")
    try:
        _V176_ORIG_BOT_JOURNAL("v176_process_toggle", int(OWNER_ID or 0) or None, f"{code}={int(bool(enabled))} actor={int(actor or 0)}")
    except Exception:
        pass
    return bool(enabled)


_V176_FAST_PROFILE_OFF = {
    "ui_retry", "win_diag", "win_reg", "win_rec", "btn_chain", "btn_press", "win_journal",
    "fin_refresh", "win_cleanup", "full_chat", "journal_mega", "runtime_upload", "source_archive",
    "failed_repair", "failed_diag", "mega_maint", "google_auto",
}
_V176_MIN_PROFILE_OFF = _V176_FAST_PROFILE_OFF | {"delta_auto", "full_global", "mega_recover", "expense_ping", "lowram"}


def v176_apply_profile(profile: str, actor: int = 0) -> None:
    profile = str(profile or "")
    root = _v176_root()
    with _V176_LOCK:
        for code in _V176_PROCESS_DEFS:
            if profile == "all":
                value = True
            elif profile == "fast":
                value = code not in _V176_FAST_PROFILE_OFF
            elif profile == "minimal":
                value = code not in _V176_MIN_PROFILE_OFF
            else:
                value = bool(_V176_PROCESS_DEFS[code][2])
            # Safety: critical synchronous witness and RAM guard stay enabled in ready-made profiles.
            if code in {"delta_critical", "memory_guard", "reminders", "lease"}:
                value = True
            root[code] = bool(value)
        try:
            root["profile"] = profile
            root["changed_at"] = now_local().isoformat(timespec="seconds")
            root["changed_by"] = int(actor or 0)
        except Exception:
            pass
        _v176_apply_runtime_flags()
        for code in _V176_PROCESS_DEFS:
            if v176_process_enabled(code):
                _v176_resume_for(code)
        _v176_persist(f"profile_{profile}")



def _v176_status_icon(code: str) -> str:
    return "✅" if v176_process_enabled(code) else "⛔"


def _v176_perf_summary() -> dict:
    rows = list(_V176_PERF)
    if not rows:
        return {"count": 0}
    vals = sorted(float(x.get("elapsed", 0.0)) for x in rows)
    n = len(vals)
    p50 = vals[n // 2]
    p90 = vals[min(n - 1, max(0, int((n - 1) * 0.90)))]
    by = {}
    for row in rows:
        action = str(row.get("action") or "?")[:80]
        by.setdefault(action, []).append(float(row.get("elapsed", 0.0)))
    top = sorted(((sum(v) / len(v), max(v), len(v), k) for k, v in by.items()), reverse=True)[:6]
    return {
        "count": n, "p50": p50, "p90": p90, "max": vals[-1],
        "slow05": sum(1 for x in vals if x >= 0.5), "slow10": sum(1 for x in vals if x >= 1.0),
        "top": top,
    }


def _v176_pool_line(name: str) -> str:
    pool = globals().get(name)
    if pool is None:
        return ""
    try:
        s = pool.stats() or {}
        return f"{name.replace('_TASK_POOL','')}: {int(s.get('active',0))}/{int(s.get('pending',0))}"
    except Exception:
        return ""


def _v176_speed_text() -> str:
    s = _v176_perf_summary()
    lines = ["📊 СКОРОСТЬ КНОПОК v178 GLOBAL FINAL", "", "Полное время callback + отдельный замер тяжёлых внутренних этапов."]
    if not s.get("count"):
        lines += ["", "Пока нет замеров после очистки/запуска v177."]
    else:
        lines += [
            "",
            f"Последних callback: {s['count']}",
            f"Медиана: {s['p50']:.3f} c · P90: {s['p90']:.3f} c · MAX: {s['max']:.3f} c",
            f"≥0.5 c: {s['slow05']} · ≥1.0 c: {s['slow10']}",
            "",
            "Самые медленные действия:",
        ]
        for avg, mx, count, action in s.get("top", []):
            lines.append(f"• {action[:48]} — avg {avg:.3f} c / max {mx:.3f} c / n={count}")
    pools = [x for x in (_v176_pool_line(n) for n in (
        "UI_TASK_POOL", "V166_WINDOW_UI_TASK_POOL", "FINANCE_TASK_POOL", "FORWARD_TASK_POOL",
        "GENERAL_TASK_POOL", "DELTA_TASK_POOL", "BACKUP_TASK_POOL", "JOURNAL_TASK_POOL",
    )) if x]
    if pools:
        lines += ["", "Очереди active/pending:", " · ".join(pools)]
    stages = list(_V177_PERF_STAGES)
    if stages:
        by_stage = {}
        for row in stages:
            by_stage.setdefault(str(row.get("stage") or "stage"), []).append(float(row.get("elapsed", 0.0)))
        top_stages = sorted(((sum(v)/len(v), max(v), len(v), k) for k,v in by_stage.items()), reverse=True)[:6]
        lines += ["", "Самые тяжёлые внутренние этапы:"]
        for avg, mx, count, name in top_stages:
            lines.append(f"• {name[:38]} — avg {avg:.3f} c / max {mx:.3f} c / n={count}")
    lines += ["", "Для чистого теста нажми «🧹 Очистить замер», затем 10–20 раз повтори один и тот же переход."]
    return "\n".join(lines)[:3900]


def _v176_menu_text(page: str = "ui") -> str:
    page = page if page in _V176_PAGE_TITLES else "ui"
    enabled = sum(1 for c in _V176_PROCESS_DEFS if v176_process_enabled(c))
    lines = [
        "⚙️ ЦЕНТР ПРОЦЕССОВ / СКОРОСТЬ v178 GLOBAL",
        f"Управляемых процессов: {len(_V176_PROCESS_DEFS)} · ВКЛ {enabled} · ВЫКЛ {len(_V176_PROCESS_DEFS)-enabled}",
        "",
        _V176_PAGE_TITLES[page],
    ]
    if page == "core":
        lines += ["", "Основа показана здесь специально, чтобы было видно, что миссия бота не исчезает при диагностике:"]
        for label, desc in _V176_LOCKED_CORE:
            lines.append(f"\n🔒 {label}\n{desc}")
        lines += ["", "Эти пункты нельзя случайно выключить из диагностического меню."]
        return "\n".join(lines)[:3900]
    for code, (group, label, _default, risk, desc) in _V176_PROCESS_DEFS.items():
        if group != page:
            continue
        mark = "🔥" if risk == "hot" else ("⚠️" if risk == "critical" else "")
        lines.append(f"\n{_v176_status_icon(code)} {label} {mark}\n{desc}")
    if page == "ui":
        lines += ["", "🔥 = особенно полезно проверить при медленных кнопках."]
    if page == "mega":
        lines += ["", "⚠️ Критические пункты можно отключить вручную для теста, но это снижает устойчивость к deploy/restart."]
    return "\n".join(lines)[:3900]


def _v176_nav_row(page: str):
    order = [("ui", "⚡ UI"), ("mega", "☁️ MEGA"), ("auto", "🔄 Авто"), ("system", "🧠 RAM"), ("core", "🔒 Ядро")]
    return [IB(("• " if p == page else "") + label, callback_data=f"v176:p:{p}") for p, label in order]


def _v176_menu_keyboard(page: str = "ui"):
    kb = types.InlineKeyboardMarkup()
    kb.row(*_v176_nav_row(page))
    if page != "core":
        for code, (group, label, _default, _risk, _desc) in _V176_PROCESS_DEFS.items():
            if group == page:
                kb.row(IB(f"{_v176_status_icon(code)} {label}", callback_data=f"v176:t:{code}"))
    kb.row(IB("⚡ Быстрый тест", callback_data="v176:profile:fast"), IB("🧱 Всё ВКЛ", callback_data="v176:profile:all"))
    kb.row(IB("🧪 Минимум", callback_data="v176:profile:minimal"), IB("📊 Скорость кнопок", callback_data="v176:speed"))
    kb.row(IB("⬅️ Назад в INFO", callback_data="v176:back_info"), IB("✖️ Закрыть", callback_data="info_close"))
    return kb


def _v176_speed_keyboard():
    kb = types.InlineKeyboardMarkup()
    kb.row(IB("🔄 Обновить замер", callback_data="v176:speed"), IB("🧹 Очистить замер", callback_data="v176:speed_clear"))
    kb.row(IB("⬅️ Процессы", callback_data="v176:menu"), IB("✖️ Закрыть", callback_data="info_close"))
    return kb


# INFO integration: v178 GLOBAL FINAL builds INFO from the stable base directly.
# We deliberately do NOT call the v148 -> v152 -> v157 -> v158 -> v171 -> v175
# wrapper chain.  The net behaviour of those layers is applied once below.
_V177_INFO_BASE_TEXT = globals().get("_v177_legacy_0054_build_info_text")
_V177_INFO_BASE_KB = globals().get("_v177_legacy_0216_build_info_keyboard")


def build_info_text(chat_id: int, *args, **kwargs) -> str:
    cid = int(chat_id)
    try:
        base = str(_V177_INFO_BASE_TEXT(cid) if callable(_V177_INFO_BASE_TEXT) else "")
    except Exception:
        base = ""

    # Net v148 tenant isolation behaviour.
    try:
        if not tenant_is_platform_owner_context(cid):
            forbidden = ("/errors", "/runtime_export", "/mega_", "/queues", "/journal", "/sqlite", "/db", "/restore_guard", "MEGA:")
            base = "\n".join(line for line in base.splitlines() if not any(token in line for token in forbidden))
    except Exception:
        pass
    try:
        tid = tenant_id_for_chat(cid, create=False)
        row = tenant_get(tid) or {}
        suffix = f"\n\n🏢 Пространство: {row.get('name') or tid}\n/space — чаты, пользователи и ссылки подключения"
        if tid:
            base = str(base).rstrip() + suffix
    except Exception:
        pass

    # v157 process-status line was subsequently removed by v158, so it is not
    # rebuilt here.  v175 heavy-mode line was replaced by this v177 center line.
    if cid == int(OWNER_ID or 0):
        rows = [r for r in str(base).splitlines() if not r.strip().startswith(("🧱 Тяжёлые процессы:", "⚡ Тяжёлые процессы:", "⚙️ Процессы / скорость:"))]
        off = sum(1 for c in _V176_PROCESS_DEFS if not v176_process_enabled(c))
        rows += ["", f"⚙️ Процессы / скорость: отключено {off}/{len(_V176_PROCESS_DEFS)}"]
        base = "\n".join(rows).strip()
    return str(base)[:3900]


def _v177_info_rows(kb):
    return list(getattr(kb, "keyboard", None) or getattr(kb, "inline_keyboard", None) or [])


def _v177_info_set_rows(kb, rows):
    try:
        kb.keyboard = rows
    except Exception:
        try: kb.inline_keyboard = rows
        except Exception: pass
    return kb


def _v177_info_btn_cb(btn) -> str:
    if isinstance(btn, dict):
        return str(btn.get("callback_data") or "")
    return str(getattr(btn, "callback_data", "") or "")


def _v177_info_btn_text(btn) -> str:
    if isinstance(btn, dict):
        return str(btn.get("text") or "")
    return str(getattr(btn, "text", "") or "")


def build_info_keyboard(chat_id: int):
    cid = int(chat_id)
    try:
        kb = _V177_INFO_BASE_KB(cid) if callable(_V177_INFO_BASE_KB) else types.InlineKeyboardMarkup()
    except Exception:
        kb = types.InlineKeyboardMarkup()
    rows = _v177_info_rows(kb)

    # Net v148 tenant filtering + Space entry.
    try:
        if not tenant_is_platform_owner_context(cid):
            blocked_prefixes = (
                "journal_", "restore_guard", "mega_manual_restore", "mega_priority", "keepalive_",
                "process_center", "safety_profile", "problem_tasks", "integrity_status", "info_queues",
                "runtime_watcher", "info_delta_status", "additional_owners", "addown:", "expense_",
            )
            clean = []
            for row in rows:
                kept = [b for b in (row or []) if not _v177_info_btn_cb(b).startswith(blocked_prefixes)]
                if kept: clean.append(kept)
            rows = clean
        actor = tenant_current_actor_user_id()
        role = tenant_role_for_user(actor, chat_id=cid) if actor else ("tenant_owner" if is_owner_chat(cid) else "standard")
        if role in {"platform_owner", "tenant_owner", "tenant_admin", "operator", "viewer"}:
            if not any(_v177_info_btn_cb(b) == "sp:dashboard" for row in rows for b in (row or [])):
                rows.append([IB("🏢 Пространство", callback_data="sp:dashboard")])
    except Exception:
        pass

    # Net v152 behaviour: the safety button opens its menu instead of toggling.
    for row in rows:
        for btn in row or []:
            try:
                if _v177_info_btn_cb(btn) == "safety_profile_toggle":
                    if isinstance(btn, dict): btn["callback_data"] = "safety_profile_open"
                    else: btn.callback_data = "safety_profile_open"
            except Exception:
                pass

    # Net v157/v158 behaviour: keep the vertical layout from "Перес:" down,
    # but omit the removed process-status controls.
    blocked_process = {"v156:process_visual_toggle", "v157:process_menu", "v157:process_owner_toggle", "v157:process_others_toggle"}
    clean = []
    for row in rows:
        kept = []
        for btn in row or []:
            cb = _v177_info_btn_cb(btn)
            txt = _v177_info_btn_text(btn).strip().casefold()
            if cb in blocked_process or txt.startswith("👁 окно процессов") or txt.startswith("👁️ окно процессов"):
                continue
            kept.append(btn)
        if kept: clean.append(kept)
    rows = clean
    start = None
    for idx, row in enumerate(rows):
        if any("перес:" in _v177_info_btn_text(btn).casefold() for btn in (row or [])):
            start = idx
            break
    if start is not None:
        rows = rows[:start] + [[btn] for row in rows[start:] for btn in (row or [])]

    # Net v171 grouping for owner INFO, applied once instead of through a wrapper.
    if rows and is_owner_chat(cid):
        order = ("diag", "storage", "finance", "forward", "reminder", "access", "help", "other", "nav")
        buckets = {k: [] for k in order}
        for row in rows:
            try: group = _v171_info_group(row)
            except Exception: group = "other"
            buckets.setdefault(group, []).append(row)
        grouped = []
        first = True
        for group in order:
            block = buckets.get(group) or []
            if not block: continue
            if not first and group != "nav":
                grouped.append([IB("ㅤ", callback_data="none")])
            grouped.extend(block)
            first = False
        rows = grouped

    # v178 owner-admin process/speed center.  This replaces only the old v175
    # heavy-toggle control; the older functional "⚙️ Процессы" menu remains.
    if cid == int(OWNER_ID or 0):
        found = False
        for row in rows:
            for btn in row or []:
                cb = _v177_info_btn_cb(btn)
                if cb in {"v175:heavy_toggle", "v176:menu"}:
                    try:
                        if isinstance(btn, dict):
                            btn["text"] = "⚙️ Процессы / скорость"; btn["callback_data"] = "v176:menu"
                        else:
                            btn.text = "⚙️ Процессы / скорость"; btn.callback_data = "v176:menu"
                    except Exception: pass
                    found = True
        if not found:
            insert_at = len(rows)
            for idx, row in enumerate(rows):
                labels = " ".join(_v177_info_btn_text(b) for b in (row or [])).casefold()
                callbacks = " ".join(_v177_info_btn_cb(b) for b in (row or []))
                if "назад" in labels or "закры" in labels or "info_close" in callbacks or "back_main" in callbacks:
                    insert_at = idx
                    break
            rows.insert(insert_at, [IB("⚙️ Процессы / скорость", callback_data="v176:menu")])

    return _v177_info_set_rows(kb, rows)


def _v176_owner_ok(call) -> bool:
    try:
        return int(call.message.chat.id) == int(OWNER_ID or 0) and int(getattr(getattr(call, "from_user", None), "id", 0) or 0) == int(OWNER_ID or 0)
    except Exception:
        return False


def _v176_filter(call):
    raw = str(getattr(call, "data", "") or "")
    return raw.startswith("v176:") or raw == "v175:heavy_toggle"


def _v176_callback(call):
    raw = str(getattr(call, "data", "") or "")
    if not _v176_owner_ok(call):
        try: bot.answer_callback_query(call.id, "Только основной владелец.", show_alert=True)
        except Exception: pass
        return
    chat_id = int(call.message.chat.id)
    if raw in {"v175:heavy_toggle", "v176:menu"}:
        try: bot.answer_callback_query(call.id)
        except Exception: pass
        safe_edit(bot, call, _v176_menu_text("ui"), reply_markup=_v176_menu_keyboard("ui"))
        return
    if raw.startswith("v176:p:"):
        page = raw.split(":", 2)[2]
        try: bot.answer_callback_query(call.id)
        except Exception: pass
        safe_edit(bot, call, _v176_menu_text(page), reply_markup=_v176_menu_keyboard(page))
        return
    if raw.startswith("v176:t:"):
        code = raw.split(":", 2)[2]
        if code not in _V176_PROCESS_DEFS:
            return
        new_state = not v176_process_enabled(code)
        v176_set_process(code, new_state, int(OWNER_ID or 0))
        try: bot.answer_callback_query(call.id, f"{_V176_PROCESS_DEFS[code][1]}: {'ВКЛ' if new_state else 'ВЫКЛ'}")
        except Exception: pass
        page = _V176_PROCESS_DEFS[code][0]
        safe_edit(bot, call, _v176_menu_text(page), reply_markup=_v176_menu_keyboard(page))
        return
    if raw.startswith("v176:profile:"):
        profile = raw.split(":", 2)[2]
        v176_apply_profile(profile, int(OWNER_ID or 0))
        labels = {"all": "Всё включено", "fast": "Быстрый тест", "minimal": "Минимальный диагностический режим"}
        try: bot.answer_callback_query(call.id, labels.get(profile, profile))
        except Exception: pass
        safe_edit(bot, call, _v176_menu_text("ui"), reply_markup=_v176_menu_keyboard("ui"))
        return
    if raw == "v176:speed":
        try: bot.answer_callback_query(call.id)
        except Exception: pass
        safe_edit(bot, call, _v176_speed_text(), reply_markup=_v176_speed_keyboard())
        return
    if raw == "v176:speed_clear":
        v177_perf_clear()
        try: bot.answer_callback_query(call.id, "Замер очищен")
        except Exception: pass
        safe_edit(bot, call, _v176_speed_text(), reply_markup=_v176_speed_keyboard())
        return
    if raw == "v176:back_info":
        try: bot.answer_callback_query(call.id)
        except Exception: pass
        safe_edit(bot, call, build_info_text(chat_id), reply_markup=build_info_keyboard(chat_id))
        return


def _v176_register_callback() -> int:
    return 0  # v179: registration/wrapper retired; final router owns callbacks


# Lightweight independent latency meter. It records every callback handler selected by TeleBot.
def _v176_install_perf_wrappers() -> int:
    return 0  # v179: registration/wrapper retired; final router owns callbacks



# Restore compatibility: v175 validator accepted only through v175; extend the same schema to v176.
_V176_PREV_RESTORE_VALIDATOR = globals().get("_v153_validate_restore_gz")
def _v179_legacy_restore_validator_v178(gz_path: str):
    try:
        return _V176_PREV_RESTORE_VALIDATOR(gz_path) if callable(_V176_PREV_RESTORE_VALIDATOR) else (None, None)
    except Exception as exc:
        if "unsupported bot version" not in str(exc):
            raise
    import gzip as _gz, shutil as _shutil, sqlite3 as _sqlite3, tempfile as _tempfile, json as _json, os as _os
    folder = _tempfile.mkdtemp(prefix="v177_restore_validate_")
    raw = _os.path.join(folder, "restore.sqlite3")
    try:
        with _gz.open(gz_path, "rb") as fin, open(raw, "wb") as fout:
            _shutil.copyfileobj(fin, fout, 1024 * 1024)
        conn = _sqlite3.connect(raw)
        try:
            integrity = str(conn.execute("PRAGMA integrity_check").fetchone()[0])
            if integrity.lower() != "ok":
                raise RuntimeError(f"SQLite integrity_check: {integrity}")
            row = conn.execute("SELECT v FROM meta WHERE kind='v153_export' AND k='manifest'").fetchone()
            if not row:
                raise RuntimeError("manifest v153 not found")
            manifest = _json.loads(row[0])
        finally:
            conn.close()
        if str(manifest.get("kind")) != "telegram_bot_full_state_v153":
            raise RuntimeError("unknown export kind")
        if int(manifest.get("schema_version") or 0) != int(V153_EXPORT_SCHEMA):
            raise RuntimeError("unsupported export schema")
        export_version = str(manifest.get("bot_version") or "")
        if not export_version.startswith(tuple(f"bot_v{i}_" for i in range(153, 179))):
            raise RuntimeError(f"unsupported bot version: {export_version or 'missing'}")
        if _v153_db_logical_checksum(raw) != str(manifest.get("checksum") or ""):
            raise RuntimeError("checksum mismatch")
        return manifest, raw
    except Exception:
        _shutil.rmtree(folder, ignore_errors=True)
        raise

def _v178_migrate_global_speed_defaults() -> bool:
    """One-time v178 migration: disable diagnostic noise globally, keep business durability on."""
    root = _v176_root()
    if bool(root.get("v178_global_speed_defaults_migrated", False)):
        return False
    for code in ("win_diag", "btn_chain", "btn_press", "win_journal", "failed_repair", "failed_diag"):
        root[code] = False
    root["v178_global_speed_defaults_migrated"] = True
    try: root["v178_migrated_at"] = now_local().isoformat(timespec="seconds")
    except Exception: pass
    try: SQLITE.save_root(_sqlite_pack_root(data))
    except Exception: pass
    return True


_V178_SPEED_MIGRATED = _v178_migrate_global_speed_defaults()
_v176_root()
_v176_apply_runtime_flags()
_V176_CALLBACK = 0  # v179 final callback router
_V176_PERF_WRAPPERS = 0  # v179 final callback router owns timing
try:
    _V176_ORIG_BOT_JOURNAL(
        "v179_clean_final_installed", int(OWNER_ID or 0) or None,
        f"managed={len(_V176_PROCESS_DEFS)} callback={_V176_CALLBACK} perf_wrappers={_V176_PERF_WRAPPERS} speed_defaults={int(_V178_SPEED_MIGRATED)}",
    )
except Exception:
    pass

# ==================== v179 CLEAN FINAL OVERRIDES ====================
V179_RUNTIME_REMOTE_HEARTBEAT_SECONDS = max(120.0, min(900.0, float(os.getenv("RUNTIME_REMOTE_HEARTBEAT_SECONDS", "180") or "180")))
_V179_LEASE_LAST = 0.0
_V179_LEASE_LOCK = _v176_threading.RLock()

def _v179_runtime_lease_check(force: bool = False) -> dict:
    """Lease is checked by the runtime watcher, not by a separate 30-second loop."""
    global _V179_LEASE_LAST
    if not v176_process_enabled("lease") or not mega_is_configured():
        return {"active": True, "skipped": True}
    now_m = _v176_time.monotonic()
    with _V179_LEASE_LOCK:
        if not force and now_m - float(_V179_LEASE_LAST or 0.0) < V179_RUNTIME_REMOTE_HEARTBEAT_SECONDS - 5:
            return {"active": True, "cached": True}
        _V179_LEASE_LAST = now_m
    base = globals().get("_v179_base_instance_lease_check") or globals().get("_V176_ORIG_INSTANCE_LEASE")
    if callable(base):
        try: return dict(base() or {})
        except Exception as exc:
            try: runtime_event("lease_check_error", str(exc), "WARN")
            except Exception: pass
    return {"active": True}

def _runtime_heartbeat_job():
    if runtime_is_shutting_down(): return
    next_delay = V179_RUNTIME_REMOTE_HEARTBEAT_SECONDS
    try:
        lease = _v179_runtime_lease_check(False)
        if lease.get("superseded"):
            return
        if v176_process_enabled("runtime_upload"):
            busy_fn = globals().get("_runtime_watcher_should_yield_to_critical_mega")
            if callable(busy_fn) and busy_fn():
                try: runtime_event("watcher_heartbeat_deferred", "critical MEGA work has priority")
                except Exception: pass
                next_delay = min(90.0, V179_RUNTIME_REMOTE_HEARTBEAT_SECONDS)
            else:
                GENERAL_TASK_POOL.submit_unique("runtime-heartbeat-upload", runtime_upload_snapshot, "heartbeat", False)
    finally:
        try: DELAYED_SCHEDULER.schedule("runtime-heartbeat", next_delay, _runtime_heartbeat_job)
        except Exception: pass

def _v153_instance_lease_check(*args, **kwargs):
    # Compatibility/public diagnostic name, but no independent scheduler.
    return _v179_runtime_lease_check(bool(kwargs.get("force", False)))

def _v153_schedule_migration(*args, **kwargs):
    return False

def _v153_reconcile_windows():
    # Periodic reconcile is gone. Explicit diagnostics can request a lazy cleanup.
    fn = globals().get("v179_window_registry_lazy_cleanup")
    return fn(True) if callable(fn) and v176_process_enabled("win_rec") else {"skipped":"v179_lazy_registry"}

# One restore validator. No v153→v154→... exception chain.
def _v153_validate_restore_gz(gz_path: str):
    """FINAL restore validator: accepts both full-state exports and raw working SQLite .gz snapshots."""
    import gzip as _gz, shutil as _shutil, sqlite3 as _sqlite3, tempfile as _tempfile, json as _json, os as _os
    folder = _tempfile.mkdtemp(prefix="v182_restore_validate_")
    raw = _os.path.join(folder, "restore.sqlite3")
    try:
        with _gz.open(gz_path, "rb") as fin, open(raw, "wb") as fout:
            _shutil.copyfileobj(fin, fout, 1024 * 1024)
        conn = _sqlite3.connect(raw)
        try:
            integrity = str(conn.execute("PRAGMA integrity_check").fetchone()[0])
            if integrity.lower() != "ok":
                raise RuntimeError(f"SQLite integrity_check: {integrity}")
            tables = {str(r[0]) for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
            # Format A: v153+ full-state export with embedded manifest/checksum.
            row = None
            if "meta" in tables:
                try:
                    row = conn.execute("SELECT v FROM meta WHERE kind='v153_export' AND k='manifest'").fetchone()
                except Exception:
                    row = None
            if row:
                manifest = _json.loads(row[0])
                if str(manifest.get("kind")) != "telegram_bot_full_state_v153":
                    raise RuntimeError("unknown export kind")
                if int(manifest.get("schema_version") or 0) != int(V153_EXPORT_SCHEMA):
                    raise RuntimeError("unsupported export schema")
                export_version = str(manifest.get("bot_version") or "")
                allowed = tuple(f"bot_v{i}_" for i in range(153, 185))
                if export_version and not export_version.startswith(allowed):
                    raise RuntimeError(f"unsupported bot version: {export_version}")
                if _v153_db_logical_checksum(raw) != str(manifest.get("checksum") or ""):
                    raise RuntimeError("checksum mismatch")
                manifest = dict(manifest)
                manifest["snapshot_format"] = "full_state_export"
                return manifest, raw

            # Format B: normal LOW-RAM working SQLite snapshot stored in MEGA /database/.
            required = {"kv", "chats", "meta"}
            if not required.issubset(tables):
                raise RuntimeError("SQLite не похож на рабочий snapshot бота (нет kv/chats/meta)")
            created_at = ""
            bot_version = ""
            try:
                mrow = conn.execute("SELECT v FROM meta WHERE kind='db_snapshot' AND k='main'").fetchone()
                if mrow:
                    meta = _json.loads(mrow[0]) or {}
                    created_at = str(meta.get("created_at") or "")
                    bot_version = str(meta.get("bot_version") or "")
            except Exception:
                pass
            chat_ids = []
            try:
                chat_ids = [int(r[0]) for r in conn.execute("SELECT chat_id FROM chats ORDER BY chat_id").fetchall()]
            except Exception:
                chat_ids = []
            record_count = 0
            if "cold_fields" in tables:
                try:
                    for (value,) in conn.execute("SELECT v FROM cold_fields WHERE k='records'").fetchall():
                        try:
                            rows = _json.loads(value) if value else []
                            if isinstance(rows, list): record_count += len(rows)
                        except Exception:
                            pass
                except Exception:
                    pass
            if not record_count:
                try:
                    for (value,) in conn.execute("SELECT v FROM chats").fetchall():
                        try:
                            payload = _json.loads(value) if value else {}
                            rows = payload.get("records") if isinstance(payload, dict) else []
                            if isinstance(rows, list): record_count += len(rows)
                        except Exception:
                            pass
                except Exception:
                    pass
            manifest = {
                "kind": "telegram_bot_working_sqlite_snapshot",
                "schema_version": 1,
                "bot_version": bot_version or "working_snapshot",
                "created_at": created_at,
                "scope": "global",
                "tenant_id": "",
                "chat_ids": chat_ids,
                "chat_count": len(chat_ids),
                "record_count": int(record_count),
                "failed_tasks": 0,
                "checksum": "",
                "snapshot_format": "working_sqlite",
            }
            return manifest, raw
        finally:
            conn.close()
    except Exception:
        _shutil.rmtree(folder, ignore_errors=True)
        raise


def _v182_download_restore_document(document) -> tuple[str, str]:
    import os as _os, tempfile as _tempfile
    name = str(getattr(document, "file_name", "") or "backup.sqlite3.gz")
    folder = _tempfile.mkdtemp(prefix="v182_restore_upload_")
    safe_name = _os.path.basename(name).replace("/", "_").replace("\\", "_") or "backup.sqlite3.gz"
    path = _os.path.join(folder, safe_name)
    info = bot.get_file(document.file_id)
    stream_fn = globals().get("telegram_download_to_file")
    if callable(stream_fn):
        max_restore = max(1024 * 1024, int(os.getenv("RESTORE_FILE_MAX_BYTES", str(250 * 1024 * 1024)) or str(250 * 1024 * 1024)))
        stream_fn(info.file_path, path, max_bytes=max_restore)
    else:
        raw = bot.download_file(info.file_path)
        with open(path, "wb") as fh: fh.write(raw)
    return path, folder


def v182_prepare_gz_restore_document(msg, document=None) -> bool:
    """Prepare a .gz restore from a document sent after /restore or replied to by /restore."""
    uid = _v153_actor_id(msg); chat_id = int(msg.chat.id)
    document = document or getattr(getattr(msg, "reply_to_message", None), "document", None)
    if not document:
        raise RuntimeError("Не найден GZ-файл")
    name = str(getattr(document, "file_name", "") or "").lower()
    if not name.endswith(".gz"):
        raise RuntimeError("Нужен файл .gz (обычно .sqlite3.gz)")
    gz = folder = raw = None
    try:
        gz, folder = _v182_download_restore_document(document)
        manifest, raw = _v153_validate_restore_gz(gz)
        scope = str(manifest.get("scope") or "global")
        tenant_id = str(manifest.get("tenant_id") or _v153_tenant_for_chat(chat_id))
        if scope == "global" and not _v153_platform_owner(uid):
            raise RuntimeError("Глобальное восстановление доступно только владельцу платформы")
        if scope == "tenant" and not _v153_can_manage_tenant(uid, tenant_id):
            current = _v153_tenant_for_chat(chat_id)
            if not _v153_can_manage_tenant(uid, current):
                raise RuntimeError("Нельзя восстановить чужое пространство")
            tenant_id = current
        token = _v153_hashlib.sha256(f"v182:{uid}:{chat_id}:{_v153_time.time_ns()}".encode()).hexdigest()[:16]
        with _V153_LOCK:
            _V153_RESTORE_PENDING[token] = {
                "uid": uid, "chat_id": chat_id, "gz": gz, "raw": raw, "manifest": manifest,
                "tenant_id": tenant_id, "created": _v153_time.time(), "upload_folder": folder,
            }
        fmt = str(manifest.get("snapshot_format") or "sqlite")
        text = (
            "🧪 GZ-файл проверен.\n\n"
            f"Формат: {fmt}\n"
            f"Версия: {manifest.get('bot_version') or 'не указана'}\n"
            f"Область: {'весь бот' if scope == 'global' else 'пространство'}\n"
            f"Чатов: {manifest.get('chat_count', 0)}\n"
            f"Финансовых записей: {manifest.get('record_count', 'см. snapshot')}\n"
            f"Создан: {manifest.get('created_at') or 'не указано'}\n\n"
            "Перед применением будет создан pre_restore backup текущей базы."
        )
        bot.reply_to(msg, text, reply_markup=_v153_restore_keyboard(token, scope))
        # Restore mode is one-file-at-a-time, like the historical workflow.
        global restore_mode
        restore_mode = None
        data.pop("_restore_mode_chat_v150", None)
        try: save_data(data, chat_ids=[chat_id])
        except Exception: pass
        return True
    except Exception:
        # _v153_validate_restore_gz owns its extracted raw temp folder on validation errors.
        if folder and (not raw):
            try: _v176_shutil.rmtree(folder, ignore_errors=True)
            except Exception: pass
        raise


def v182_cmd_restore(msg):
    """Unified historical /restore: reply to GZ, or enter upload mode for GZ/JSON/ISON/CSV."""
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass
    try: schedule_command_delete(msg)
    except Exception: pass
    uid = _v153_actor_id(msg); chat_id = int(msg.chat.id)
    if not (_v153_platform_owner(uid) or _v153_can_manage_tenant(uid, _v153_tenant_for_chat(chat_id))):
        bot.reply_to(msg, "⛔ Недостаточно прав для восстановления.")
        return
    replied_doc = getattr(getattr(msg, "reply_to_message", None), "document", None)
    if replied_doc is not None and str(getattr(replied_doc, "file_name", "") or "").lower().endswith(".gz"):
        try:
            v182_prepare_gz_restore_document(msg, replied_doc)
        except Exception as exc:
            bot.reply_to(msg, f"❌ GZ не подготовлен к восстановлению:\n{v153_redact_text(exc)[:700]}")
        return
    global restore_mode
    restore_mode = chat_id
    data["_restore_mode_chat_v150"] = chat_id
    try: save_data(data, chat_ids=[chat_id])
    except Exception: pass
    try: cleanup_forward_links(chat_id)
    except Exception: pass
    send_and_auto_delete(
        chat_id,
        "📥 Режим восстановления включён.\n\n"
        "Теперь отправьте ОДИН файл:\n"
        "• *.sqlite3.gz / *.gz — полный SQLite snapshot\n"
        "• *.json / *.ison — полный JSON/ISON backup (включая chat_<id>.json)\n"
        "• *.csv — CSV чата\n\n"
        "Для следующего файла снова отправьте /restore.\n"
        "Отмена: /restore_off",
        30,
    )


def _v182_install_restore_handler() -> int:
    replaced = 0
    for handler in list(getattr(bot, "message_handlers", []) or []):
        if not isinstance(handler, dict): continue
        filters = handler.get("filters") or {}
        commands = [str(x).lower() for x in (filters.get("commands") or [])]
        if "restore" in commands:
            handler["function"] = v182_cmd_restore; replaced += 1
    if not replaced:
        try: bot.message_handler(commands=["restore"])(v182_cmd_restore); replaced = 1
        except Exception: pass
    return replaced

_V182_RESTORE_HANDLER_COUNT = _v182_install_restore_handler()

# Lazy registry hooks: cleanup at most once/10 min when UI actually touches the registry.
_V179_BASE_REGISTER_OPEN_WINDOW = globals().get("_v179_base_register_open_window") or globals().get("register_open_window")
_V179_BASE_GET_OPEN_WINDOW = globals().get("_v179_base_get_registered_open_window") or globals().get("get_registered_open_window")
def _v179_touch_window_registry():
    try:
        fn=globals().get("v179_window_registry_lazy_cleanup")
        if callable(fn) and v176_process_enabled("win_cleanup"): fn(False)
    except Exception: pass

def register_open_window(chat_id: int, message_id: int, window_type: str, code: str = "", day_key=None, params=None):
    _v179_touch_window_registry()
    return _V179_BASE_REGISTER_OPEN_WINDOW(chat_id, message_id, window_type, code=code, day_key=day_key, params=params)

def get_registered_open_window(chat_id: int, message_id: int):
    _v179_touch_window_registry()
    return _V179_BASE_GET_OPEN_WINDOW(chat_id, message_id)

def runtime_mark_ready(detail: str = ""):
    """One FINAL READY path replacing v153/v160/v167/v171/v172/v175 wrapper chain."""
    with _RUNTIME_LOCK:
        if _RUNTIME_STATE.get("ready"): return
        _RUNTIME_STATE["ready"] = True
        _RUNTIME_STATE["phase"] = "ready"
        _RUNTIME_STATE["ready_at"] = now_local().isoformat(timespec="seconds")
        _RUNTIME_STATE["boot_completed_at"] = _RUNTIME_STATE["ready_at"]
        _RUNTIME_STATE["boot_duration_seconds"] = round(max(0.0, _v176_time.monotonic() - _RUNTIME_STARTED_MONO), 3)
    runtime_event("ready", detail or "BOOT completed")
    # Core runtime state
    try: data.setdefault(V172_TASKS_KEY, {}); data.setdefault(V172_TASK_SETTINGS_KEY, {}); data.setdefault(V172_TASK_SOURCE_INDEX_KEY, {})
    except Exception: pass
    try: _DELTA_ROOT_MAP_KEYS.update({V172_TASKS_KEY, V172_TASK_SETTINGS_KEY, V172_TASK_SOURCE_INDEX_KEY})
    except Exception: pass
    # One lease check + one ready snapshot; then the shared runtime heartbeat owns both.
    try: GENERAL_TASK_POOL.submit_unique("runtime-ready-lease", _v179_runtime_lease_check, True)
    except Exception: pass
    if v176_process_enabled("runtime_upload"):
        try: GENERAL_TASK_POOL.submit_unique("runtime-ready-snapshot", runtime_upload_snapshot, "boot_ready", True)
        except Exception: pass
    try: DELAYED_SCHEDULER.schedule("runtime-heartbeat", V179_RUNTIME_REMOTE_HEARTBEAT_SECONDS, _runtime_heartbeat_job)
    except Exception: pass
    if not RESTORE_GUARD_ACTIVE:
        try: schedule_startup_main_windows(delay=1.0)
        except Exception as exc: runtime_event("startup_windows_error", str(exc), "WARN")
    try: schedule_restored_secret_media_recovery(1.5)
    except Exception: pass
    try: DELAYED_SCHEDULER.schedule("journal-warm-tail", 12.0, _journal_warm_tail_job)
    except Exception: pass
    try: DELAYED_SCHEDULER.schedule("lowram-idle-sweep", 45.0, _lowram_idle_sweep_job)
    except Exception: pass
    try: DELAYED_SCHEDULER.schedule("v160-transient-cleanup", 2.0, _v160_cleanup_legacy_transient_windows)
    except Exception: pass
    # READY-time compatibility migrations that remain meaningful.
    try:
        if _v168_mark_resolved_tz_rows(): _v160_persist_annotations(int(OWNER_ID or 0))
    except Exception: pass
    try: _v167_archive_old_tz_once(force=True)
    except Exception: pass
    try: _v171_mark_all_v169_tz_fixed()
    except Exception: pass
    try: _v171_reminder_global_mode(True); forward_copy_edit_mode(int(OWNER_ID or 0))
    except Exception: pass
    try:
        if OWNER_ID:
            _v167_google_schedule_cfg(int(OWNER_ID), create=True); _v167_persist_schedule(int(OWNER_ID))
    except Exception: pass
    try: bot_journal("v179_ready", int(OWNER_ID or 0) or None, "single_ready_path=1; mega_root=/TelegramBotBackups")
    except Exception: pass
# v179_clean_final


# v179 authoritative runtime version after integrated historical modules.
VERSION = "bot_v184_full_restore_contract"
# v184_full_restore_contract
