# v159_internal_timers_helper_windows
"""v159: correct timer markers, make helper-window lifetime configurable, restore visible file progress."""

import copy as _v159_copy
import gzip as _v159_gzip
import json as _v159_json
import os as _v159_os
import re as _v159_re
import shutil as _v159_shutil
import sqlite3 as _v159_sqlite3
import tempfile as _v159_tempfile
import time as _v159_time

VERSION = "bot_v159_internal_timers_helper_windows"

# ---------------------------------------------------------------------------
# 1) Internal timer labels / new helper timers.
# ---------------------------------------------------------------------------
try:
    if "main_window_refresh" in INTERNAL_TIMER_DEFS:
        INTERNAL_TIMER_DEFS["main_window_refresh"]["label"] = "⏰ Обновление главного окна Ф91"
    if "process_status_refresh" in INTERNAL_TIMER_DEFS:
        INTERNAL_TIMER_DEFS["process_status_refresh"]["label"] = "⚙️ Обновление времени / этапа процессов"
    INTERNAL_TIMER_DEFS.setdefault(
        "helper_process_close",
        {"label": "⏳ Закрытие окна операции после завершения", "default": 4, "min": 1, "max": 3600},
    )
    INTERNAL_TIMER_DEFS.setdefault(
        "file_status_close",
        {"label": "📥📤 Закрытие окна скачивания / загрузки", "default": 15, "min": 1, "max": 3600},
    )
    INTERNAL_TIMER_DEFS.setdefault(
        "helper_message_close",
        {"label": "💬 Закрытие малых служебных сообщений", "default": 25, "min": 1, "max": 3600},
    )
except Exception:
    pass

try:
    if isinstance(WINDOW_MARKER_CLOCK_CODES, set):
        WINDOW_MARKER_CLOCK_CODES.discard("Ф40")
        WINDOW_MARKER_CLOCK_CODES.add("Ф91")
        WINDOW_MARKER_CLOCK_CODES.update({"Ф232", "Ф233"})
except Exception:
    pass

# A back-to-INFO action must expect the INFO page marker, not an action-only code.
try:
    WINDOW_MARKER_CONSTANTS["itmr_back_info"] = "Ф54"
except Exception:
    pass


def _v159_force_marker(text: str, code: str, glyph: str | None = None) -> str:
    body = strip_window_mark(str(text or ""))
    # Some old builders append a marker and then append confirmation text.
    # Remove standalone internal marker lines anywhere in the body before adding
    # the one truthful marker for the resulting page.
    body = _v159_re.sub(r"(?mi)^\s*(?:[СФП]\d{1,6}|[ов]\d{1,3})(?:\s*[⏳⏰])?\s*$", "", body)
    body = _v159_re.sub(r"\n{3,}", "\n\n", body).strip()
    if glyph:
        return f"{body}\n\n{code} {glyph}"
    return window_mark(body, code)


# The original timer builders used legacy_owner:9 => Ф92. Make their direct
# output truthful even before it reaches fast_ui_edit_message_text.
_V159_PREV_BUILD_TIMERS_TEXT = globals().get("build_internal_timers_text")
_V159_PREV_BUILD_TIMER_INPUT_TEXT = globals().get("build_internal_timer_input_text")


def build_internal_timers_text() -> str:
    lines = [
        "⏱ Внутренние таймеры",
        "",
        "Настройки общие для обычных служебных окон и процессов.",
        "Окно процесса остаётся открытым пока операция реально выполняется; выбранное время определяет, сколько оно ещё видно после завершения.",
        "Окна скачивания/загрузки показывают прошедшее время, этап и прогресс до окончания файла.",
        "Главное окно — Ф91.",
        "",
    ]
    for key, cfg in INTERNAL_TIMER_DEFS.items():
        lines.append(f"{cfg['label']}: {_format_duration_short(internal_timer_seconds(key))}")
    lines.extend(["", "Выберите таймер для изменения."])
    return _v159_force_marker("\n".join(lines), "Ф183")


def build_internal_timer_input_text(chat_id: int) -> str:
    if callable(_V159_PREV_BUILD_TIMER_INPUT_TEXT):
        try:
            text = _V159_PREV_BUILD_TIMER_INPUT_TEXT(int(chat_id))
        except Exception:
            text = "⏱ Настройка таймера"
    else:
        text = "⏱ Настройка таймера"
    return _v159_force_marker(text, "Ф184")


# The router has a different semantic marker for every timer action. Force the
# visible result to that marker so button_outcome expected/actual agree.
_V159_PREV_FAST_UI_EDIT = globals().get("fast_ui_edit_message_text")
_V159_TIMER_PURPOSE_MARKERS = {
    "internal_timers": "Ф183",
    "internal_timer_pick": "Ф184",
    "internal_timer_digit": "Ф185",
    "internal_timer_unit": "Ф186",
    "internal_timer_backspace": "Ф187",
    "internal_timer_clear": "Ф188",
    "internal_timer_apply": "Ф189",
    "internal_timer_back_info": "Ф54",
}

if callable(_V159_PREV_FAST_UI_EDIT):
    def fast_ui_edit_message_text(chat_id: int, message_id: int, text: str, reply_markup=None, parse_mode=None, purpose: str = "fast_ui") -> str:
        code = _V159_TIMER_PURPOSE_MARKERS.get(str(purpose or ""))
        if code:
            text = _v159_force_marker(text, code)
        return _V159_PREV_FAST_UI_EDIT(
            int(chat_id), int(message_id), text,
            reply_markup=reply_markup, parse_mode=parse_mode, purpose=purpose,
        )


# ---------------------------------------------------------------------------
# 2) Restore the small process window from v156, but make its refresh and
#    post-completion lifetime controlled by Internal Timers.
# ---------------------------------------------------------------------------
def process_visual_status_enabled(chat_id: int) -> bool:
    # v158 intentionally disabled these messages. v159 restores them by request.
    # There is one reusable process message per chat, not one message per phase.
    return True


def _v156_process_status_schedule(chat_id: int, delay: float) -> None:
    try:
        key = f"{_V156_PROCESS_STATUS_KEY_PREFIX}{int(chat_id)}"
        DELAYED_SCHEDULER.cancel(key)
        DELAYED_SCHEDULER.schedule(key, max(0.05, float(delay)), _v156_process_status_tick, int(chat_id))
    except Exception:
        pass


def _v156_process_status_arm(chat_id: int | None, hint: str = "") -> None:
    try:
        chat_id = int(chat_id or 0)
    except Exception:
        return
    if not chat_id:
        return
    with _V156_PROCESS_UI_LOCK:
        state = _V156_PROCESS_UI.setdefault(chat_id, {"message_id": 0, "hint": "", "armed_at": _v159_time.monotonic()})
        if hint:
            state["hint"] = str(hint)[:120]
        state["armed_at"] = min(float(state.get("armed_at") or _v159_time.monotonic()), _v159_time.monotonic())
    # Do not flash on sub-second work.
    _v156_process_status_schedule(chat_id, 0.8)


def _v159_process_status_text(chat_id: int, rows: list[dict], hint: str = "") -> str:
    now_m = _v159_time.monotonic()
    lines = ["⏳ Операция выполняется"]
    if hint:
        lines.append(f"Действие: {str(hint)[:100]}")
    lines.append("")
    for row in rows[:6]:
        started = float((row or {}).get("started_mono") or now_m)
        elapsed = max(0, int(now_m - started))
        label = str((row or {}).get("label") or (row or {}).get("id") or "Операция")
        phase = str((row or {}).get("phase") or "выполняется")
        lines.append(f"• {label}: {phase} · {elapsed}с")
    if len(rows) > 6:
        lines.append(f"…ещё {len(rows) - 6}")
    close_s = internal_timer_seconds("helper_process_close", 4)
    lines.extend(["", f"После завершения окно закроется через {_format_duration_short(close_s)}."])
    return _v159_force_marker("\n".join(lines)[:3800], "Ф232", "⏰")


def _v156_process_status_tick(chat_id: int) -> None:
    chat_id = int(chat_id)
    rows = _v156_active_process_rows(chat_id)
    with _V156_PROCESS_UI_LOCK:
        state = _V156_PROCESS_UI.get(chat_id) or {}
        msg_id = int(state.get("message_id") or 0)
        hint = str(state.get("hint") or "")
    if not rows:
        if msg_id:
            try:
                close_s = internal_timer_seconds("helper_process_close", 4)
                final = _v159_force_marker(
                    f"✅ {hint or 'Операция'}\nВыполнено.\nОкно закроется через {_format_duration_short(close_s)}.",
                    "Ф232", "⏳",
                )
                bot.edit_message_text(final, chat_id=chat_id, message_id=msg_id)
                delete_message_later(chat_id, msg_id, close_s)
            except Exception:
                pass
        _v156_process_status_clear(chat_id, delete=False)
        return

    text = _v159_process_status_text(chat_id, rows, hint)
    if msg_id:
        try:
            bot.edit_message_text(text, chat_id=chat_id, message_id=msg_id)
        except Exception as exc:
            low = str(exc).casefold()
            if "message is not modified" not in low:
                try:
                    bot_journal("process_visual_status_edit_error", chat_id, str(exc)[:300], "WARN")
                except Exception:
                    pass
            # A deleted helper message is recreated while the process is active.
            if "message_id_invalid" in low or "message to edit not found" in low or "message can't be edited" in low:
                try:
                    sent = bot.send_message(chat_id, text)
                    new_id = int(getattr(sent, "message_id", 0) or 0)
                    if new_id:
                        with _V156_PROCESS_UI_LOCK:
                            if chat_id in _V156_PROCESS_UI:
                                _V156_PROCESS_UI[chat_id]["message_id"] = new_id
                except Exception:
                    pass
    else:
        try:
            sent = bot.send_message(chat_id, text)
            new_id = int(getattr(sent, "message_id", 0) or 0)
            if new_id:
                with _V156_PROCESS_UI_LOCK:
                    if chat_id in _V156_PROCESS_UI:
                        _V156_PROCESS_UI[chat_id]["message_id"] = new_id
        except Exception:
            pass
    _v156_process_status_schedule(chat_id, internal_timer_seconds("process_status_refresh", 10.0))


# ---------------------------------------------------------------------------
# 3) Restore file download/upload progress. One message is updated in-place.
# ---------------------------------------------------------------------------
def _v159_file_phase_prefix(phase: str) -> str:
    low = str(phase or "").casefold()
    if any(x in low for x in ("скачив", "читаю", "получаю")):
        return "📥 Скачивание"
    if any(x in low for x in ("загруж", "отправля", "выгруж", "mega", "drive")):
        return "📤 Загрузка"
    if any(x in low for x in ("zip", "excel", "собир", "формир", "экспорт")):
        return "⚙️ Подготовка"
    return "⏳ Выполнение"


def _v159_file_status_text(label: str, elapsed: str, phase: str, cur=None, tot=None) -> str:
    progress = f"\nПрогресс: {cur}/{tot}" if cur is not None and tot is not None else ""
    close_s = internal_timer_seconds("file_status_close", 15)
    text = (
        f"{_v159_file_phase_prefix(phase)} · {label}\n"
        f"Время: {elapsed}\n"
        f"Этап: {phase}{progress}\n"
        f"После завершения закроется через {_format_duration_short(close_s)}."
    )
    return _v159_force_marker(text, "Ф233", "⏰")


def _file_job_progress(phase: str, current=None, total=None, force: bool = False):
    ctx = _file_job_current()
    if not ctx:
        return
    key = str(ctx.get("key") or _INTERACTIVE_FILE_JOB_KEY)
    now_m = _v159_time.monotonic()
    with _FILE_JOB_LOCK:
        st = _FILE_JOB_STATE.get(key)
        if not isinstance(st, dict):
            return
        st["phase"] = str(phase or "работаю")
        if current is not None:
            st["current"] = current
        if total is not None:
            st["total"] = total
        last = float(st.get("last_ui_monotonic") or 0.0)
        refresh = internal_timer_seconds("process_status_refresh", 10.0)
        if not force and (now_m - last) < refresh:
            return
        st["last_ui_monotonic"] = now_m
        chat_id = int(st.get("chat_id"))
        msg_id = st.get("status_msg_id")
        label = str(st.get("label") or "Файл")
        started = float(st.get("started_monotonic") or st.get("queued_monotonic") or now_m)
        elapsed = _file_job_elapsed_text(now_m - started)
        cur = st.get("current")
        tot = st.get("total")
    if not msg_id:
        return
    try:
        bot.edit_message_text(_v159_file_status_text(label, elapsed, str(phase or "работаю"), cur, tot), chat_id=chat_id, message_id=int(msg_id))
    except Exception:
        pass


def _file_job_tick(key: str):
    key = str(key)
    with _FILE_JOB_LOCK:
        st = _FILE_JOB_STATE.get(key)
        if not isinstance(st, dict):
            return
        chat_id = int(st.get("chat_id"))
        msg_id = st.get("status_msg_id")
        label = str(st.get("label") or "Файл")
        phase = str(st.get("phase") or "работаю")
        started = float(st.get("started_monotonic") or st.get("queued_monotonic") or _v159_time.monotonic())
        elapsed = _file_job_elapsed_text(_v159_time.monotonic() - started)
        cur = st.get("current")
        tot = st.get("total")
    try:
        if msg_id:
            bot.edit_message_text(_v159_file_status_text(label, elapsed, phase, cur, tot), chat_id=chat_id, message_id=int(msg_id))
    except Exception:
        pass
    with _FILE_JOB_LOCK:
        alive = isinstance(_FILE_JOB_STATE.get(key), dict)
    if alive:
        DELAYED_SCHEDULER.schedule(
            f"file-job-tick:{key}", internal_timer_seconds("process_status_refresh", 10.0), _file_job_tick, key
        )


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
                st["started_monotonic"] = _v159_time.monotonic()
                st["phase"] = "запуск"
        _file_job_progress("запуск", force=True)
        mem_ctx = globals().get("memory_operation")
        if callable(mem_ctx):
            with mem_ctx(f"file:{job_meta.get('kind') or 'export'}", {"chat_id": job_meta.get("chat_id"), "label": job_meta.get("label")}, heavy=True):
                result = func(*args, **kwargs)
        else:
            result = func(*args, **kwargs)
        ok = (result is not False)
        if not ok:
            error_text = "операция завершилась без подтверждения"
    except Exception as exc:
        error_text = str(exc)[:300]
        try:
            log_error(f"INTERACTIVE FILE JOB {job_meta.get('kind')}: {exc}")
        except Exception:
            pass
    finally:
        now_m = _v159_time.monotonic()
        with _FILE_JOB_LOCK:
            st = _FILE_JOB_STATE.get(key)
            if isinstance(st, dict):
                chat_id = int(st.get("chat_id"))
                msg_id = st.get("status_msg_id")
                label = str(st.get("label") or "Файл")
                started = float(st.get("started_monotonic") or st.get("queued_monotonic") or now_m)
                elapsed = _file_job_elapsed_text(now_m - started)
            else:
                chat_id = int(job_meta.get("chat_id") or 0)
                msg_id = None
                label = str(job_meta.get("label") or "Файл")
                elapsed = "0:00"
        try:
            if msg_id:
                close_s = internal_timer_seconds("file_status_close", 15)
                if ok:
                    final = f"✅ {label}\nГотово за {elapsed}.\nОкно закроется через {_format_duration_short(close_s)}."
                else:
                    final = f"⚠️ {label}\nЗавершено за {elapsed}.\n{error_text or 'Telegram не подтвердил отправку.'}\nОкно закроется через {_format_duration_short(close_s)}."
                final = _v159_force_marker(final, "Ф233", "⏳")
                bot.edit_message_text(final, chat_id=chat_id, message_id=int(msg_id))
                delete_message_later(chat_id, int(msg_id), close_s)
        except Exception:
            pass
        try:
            bot_journal("file_job_done" if ok else "file_job_uncertain", chat_id, f"kind={job_meta.get('kind')} elapsed={elapsed} error={error_text}")
        except Exception:
            pass
        try:
            DELAYED_SCHEDULER.cancel(f"file-job-tick:{key}")
        except Exception:
            pass
        with _FILE_JOB_LOCK:
            _FILE_JOB_STATE.pop(key, None)
        if previous is None:
            try:
                delattr(_FILE_JOB_CONTEXT, "value")
            except Exception:
                pass
        else:
            _FILE_JOB_CONTEXT.value = previous


def submit_interactive_file_job(chat_id: int, kind: str, label: str, func, *args, **kwargs) -> tuple[bool, str]:
    chat_id = int(chat_id)
    gate = globals().get("memory_heavy_allowed")
    if callable(gate):
        try:
            allowed, reason = gate(str(kind or "export"))
        except Exception:
            allowed, reason = True, ""
        if not allowed:
            try:
                send_and_auto_delete(chat_id, f"🧠 {reason}", internal_timer_seconds("helper_message_close", 25))
            except Exception:
                pass
            return False, reason or "сервер временно разгружает память"
    key = _INTERACTIVE_FILE_JOB_KEY
    with _FILE_JOB_LOCK:
        existing = _FILE_JOB_STATE.get(key)
        if isinstance(existing, dict):
            return False, build_all_processes_toast(chat_id)
        meta = {
            "key": key,
            "chat_id": chat_id,
            "kind": str(kind),
            "label": str(label),
            "queued_monotonic": _v159_time.monotonic(),
            "started_monotonic": 0.0,
            "phase": "в очереди",
            "status_msg_id": None,
            "last_ui_monotonic": 0.0,
        }
        _FILE_JOB_STATE[key] = meta
    try:
        text = _v159_file_status_text(str(label), "0:00", "в очереди")
        status = bot.send_message(chat_id, text)
        with _FILE_JOB_LOCK:
            if isinstance(_FILE_JOB_STATE.get(key), dict):
                _FILE_JOB_STATE[key]["status_msg_id"] = int(getattr(status, "message_id", 0) or 0) or None
    except Exception:
        pass
    ok = EXPORT_TASK_POOL.submit_unique(key, _interactive_file_job_runner, dict(meta), func, args, kwargs)
    if not ok:
        with _FILE_JOB_LOCK:
            _FILE_JOB_STATE.pop(key, None)
        return False, build_all_processes_toast(chat_id)
    try:
        DELAYED_SCHEDULER.cancel(f"file-job-tick:{key}")
        DELAYED_SCHEDULER.schedule(
            f"file-job-tick:{key}", internal_timer_seconds("process_status_refresh", 10.0), _file_job_tick, key
        )
    except Exception:
        pass
    try:
        bot_journal("file_job_queued", chat_id, f"kind={kind} label={label}; visual=1; marker=Ф233")
    except Exception:
        pass
    return True, "Запущено"


# ---------------------------------------------------------------------------
# 4) Standard small auto-delete helper messages use an Internal Timer when the
#    caller uses the historical default helper delay.
# ---------------------------------------------------------------------------
_V159_OLD_HELPER_DELAY = float(globals().get("HELPER_DELETE_DELAY") or 25)


def _v159_helper_delay(delay) -> float:
    try:
        value = float(delay)
    except Exception:
        value = _V159_OLD_HELPER_DELAY
    if abs(value - _V159_OLD_HELPER_DELAY) < 0.001:
        return internal_timer_seconds("helper_message_close", _V159_OLD_HELPER_DELAY)
    return max(0.1, value)


def _v159_helper_mark(text: str) -> str:
    return _v159_force_marker(str(text or ""), "Ф234", "⏳")


def send_and_auto_delete(chat_id: int, text: str, delay: int = 25):
    if is_finance_output_suppressed(chat_id):
        return
    delay = _v159_helper_delay(delay)
    marked = _v159_helper_mark(text)
    if chat_buttons_current_window_enabled(chat_id):
        send_or_edit_stored_window(chat_id, "command_window_id", marked, delay=delay)
        return
    try:
        msg = bot.send_message(chat_id, marked)
        DELAYED_SCHEDULER.schedule(
            f"auto-delete:{chat_id}:{msg.message_id}", delay,
            lambda: _v159_delete_quiet(chat_id, msg.message_id),
        )
    except Exception as e:
        log_error(f"send_and_auto_delete: {e}")


def send_html_and_auto_delete(chat_id: int, html_text: str, delay: int = 25):
    if is_finance_output_suppressed(chat_id):
        return
    delay = _v159_helper_delay(delay)
    marked = _v159_helper_mark(html_text)
    if chat_buttons_current_window_enabled(chat_id):
        send_or_edit_stored_window(chat_id, "command_window_id", marked, parse_mode="HTML", delay=delay)
        return
    try:
        msg = bot.send_message(chat_id, marked, parse_mode="HTML")
        DELAYED_SCHEDULER.schedule(
            f"auto-delete-html:{chat_id}:{msg.message_id}", delay,
            lambda: _v159_delete_quiet(chat_id, msg.message_id),
        )
    except Exception as e:
        log_error(f"send_html_and_auto_delete: {e}")


def _v159_delete_quiet(chat_id: int, message_id: int) -> None:
    try:
        bot.delete_message(int(chat_id), int(message_id))
    except Exception:
        pass


# Explicit helper-window markers.
try:
    WINDOW_MARKER_CONSTANTS.update({
        "v159:process_status": "Ф232",
        "v159:file_status": "Ф233",
        "v159:helper_message": "Ф234",
    })
except Exception:
    pass


# v153 restore validator: permit v159 full-state snapshots as well.
def _v153_validate_restore_gz(gz_path: str) -> tuple[dict, str]:
    folder = _v159_tempfile.mkdtemp(prefix="v159_restore_validate_")
    raw = _v159_os.path.join(folder, "restore.sqlite3")
    try:
        with _v159_gzip.open(gz_path, "rb") as fin, open(raw, "wb") as fout:
            _v159_shutil.copyfileobj(fin, fout, 1024 * 1024)
        conn = _v159_sqlite3.connect(raw)
        try:
            integrity = str(conn.execute("PRAGMA integrity_check").fetchone()[0])
            if integrity.lower() != "ok":
                raise RuntimeError(f"SQLite integrity_check: {integrity}")
            row = conn.execute("SELECT v FROM meta WHERE kind='v153_export' AND k='manifest'").fetchone()
            if not row:
                raise RuntimeError("manifest v153 not found")
            manifest = _v159_json.loads(row[0])
        finally:
            conn.close()
        if str(manifest.get("kind")) != "telegram_bot_full_state_v153":
            raise RuntimeError("unknown export kind")
        if int(manifest.get("schema_version") or 0) != int(V153_EXPORT_SCHEMA):
            raise RuntimeError("unsupported export schema")
        export_version = str(manifest.get("bot_version") or "")
        if not export_version.startswith((
            "bot_v153_", "bot_v154_", "bot_v155_", "bot_v156_", "bot_v157_", "bot_v158_", "bot_v159_",
        )):
            raise RuntimeError(f"unsupported bot version: {export_version or 'missing'}")
        checksum = _v153_db_logical_checksum(raw)
        if checksum != str(manifest.get("checksum") or ""):
            raise RuntimeError("checksum mismatch")
        return manifest, raw
    except Exception:
        _v159_shutil.rmtree(folder, ignore_errors=True)
        raise


try:
    bot_journal(
        "v159_internal_timers_helper_windows_installed",
        int(OWNER_ID or 0),
        "timer_markers=corrected; process_helper=restored; file_progress=restored; helper_close_timers=3; markers=Ф232-Ф234",
    )
except Exception:
    pass

# v159_internal_timers_helper_windows
