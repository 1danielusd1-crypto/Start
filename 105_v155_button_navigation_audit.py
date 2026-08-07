# v155_button_navigation_audit
"""v155: full button/navigation audit hardening and live callback outcome diagnostics."""

import copy as _v155_copy
import gzip as _v155_gzip
import json as _v155_json
import os as _v155_os
import shutil as _v155_shutil
import sqlite3 as _v155_sqlite3
import tempfile as _v155_tempfile
import re as _v155_re
import threading as _v155_threading
import time as _v155_time
from collections import deque as _v155_deque

VERSION = "bot_v155_button_navigation_audit"

V155_BUTTON_AUDIT_ENABLED = str(_v155_os.getenv("BUTTON_OUTCOME_AUDIT", "1") or "1").strip().lower() not in {"0", "false", "off", "no"}
_V155_BUTTON_AUDIT_LOCK = _v155_threading.RLock()
_V155_BUTTON_AUDIT_RECENT = _v155_deque(maxlen=500)
_V155_BUTTON_AUDIT_INSTALLED = 0


def _v155_source_marker(call) -> str:
    try:
        msg = getattr(call, "message", None)
        text = getattr(msg, "text", None) or getattr(msg, "caption", None) or ""
        fn = globals().get("_window_diag_marker")
        if callable(fn):
            return str(fn(text) or "")
        m = _v155_re.search(r"(?:^|\s)([СФПОВсов]\d{1,6})(?:\s*[⏳⏰])?\s*$", str(text or ""), flags=_v155_re.IGNORECASE)
        return str(m.group(1) if m else "").upper()
    except Exception:
        return ""


def _v155_button_label(call, raw_data: str, resolved_data: str) -> str:
    try:
        markup = getattr(getattr(call, "message", None), "reply_markup", None)
        for row in list(getattr(markup, "keyboard", None) or []):
            for button in row or []:
                cb = str(getattr(button, "callback_data", "") or "")
                resolved = cb
                try:
                    resolver = globals().get("resolve_short_callback")
                    if callable(resolver):
                        resolved = resolver(cb) or cb
                except Exception:
                    pass
                if cb == raw_data or str(resolved) == str(resolved_data):
                    return str(getattr(button, "text", "") or "")[:120]
    except Exception:
        pass
    return ""


def _v155_expected_marker(action: str, chat_id: int) -> str:
    try:
        fn = globals().get("window_code_for_callback")
        owner_fn = globals().get("is_owner_chat")
        if callable(fn):
            return str(fn(action, owner_chat=bool(owner_fn(chat_id) if callable(owner_fn) else False)) or "")
    except Exception:
        pass
    return ""


def _v155_window_events_since(seq: int, chat_id: int, message_id: int) -> list[dict]:
    try:
        tail_fn = globals().get("window_diagnostic_tail")
        if not callable(tail_fn):
            return []
        rows = []
        for row in tail_fn(120):
            try:
                if int(row.get("seq") or 0) <= int(seq or 0):
                    continue
                row_chat = row.get("chat_id")
                row_msg = row.get("message_id")
                if row_chat is not None and int(row_chat) != int(chat_id):
                    continue
                if row_msg is not None and int(row_msg) != int(message_id):
                    # A callback may delete an old main window or send a replacement. Keep only
                    # same-chat events, but prefer the source message below in the summary.
                    pass
                rows.append(row)
            except Exception:
                continue
        return rows[-30:]
    except Exception:
        return []


def _v155_summarize_effect(events: list[dict], source_message_id: int) -> tuple[str, str, str]:
    if not events:
        return "handled_no_window_change", "", ""
    names = [str((row or {}).get("action") or "") for row in events]
    failure_order = (
        "window_edit_target_missing", "window_edit_failed", "window_keyboard_edit_failed",
        "window_delete_failed", "window_send_failed", "window_transport_stale_request",
        "window_stale_edit_apply",
    )
    for name in failure_order:
        if name in names:
            row = next((x for x in reversed(events) if str((x or {}).get("action") or "") == name), {})
            return name, str(((row or {}).get("detail") or {}).get("to_marker") or ""), ",".join(names[-8:])
    if "window_edit_applied" in names:
        row = next((x for x in reversed(events) if str((x or {}).get("action") or "") == "window_edit_applied"), {})
        detail = (row or {}).get("detail") or {}
        return "edited", str(detail.get("to_marker") or detail.get("marker") or ""), ",".join(names[-8:])
    if "window_edit_not_modified" in names:
        row = next((x for x in reversed(events) if str((x or {}).get("action") or "") == "window_edit_not_modified"), {})
        detail = (row or {}).get("detail") or {}
        return "already_current", str(detail.get("to_marker") or ""), ",".join(names[-8:])
    if "window_deleted" in names:
        return "deleted", "", ",".join(names[-8:])
    if "window_created" in names:
        row = next((x for x in reversed(events) if str((x or {}).get("action") or "") == "window_created"), {})
        detail = (row or {}).get("detail") or {}
        return "created", str(detail.get("marker") or detail.get("to_marker") or ""), ",".join(names[-8:])
    return "handled", "", ",".join(names[-8:])


def _v155_record_button_outcome(call, raw_data: str, resolved_data: str, started: float, seq_before: int, error: str = "") -> None:
    if not V155_BUTTON_AUDIT_ENABLED:
        return
    try:
        msg = getattr(call, "message", None)
        chat_id = int(getattr(getattr(msg, "chat", None), "id", 0) or 0)
        message_id = int(getattr(msg, "message_id", 0) or 0)
        events = _v155_window_events_since(seq_before, chat_id, message_id)
        result, actual_marker, event_names = _v155_summarize_effect(events, message_id)
        if error:
            result = "exception"
        row = {
            "ts": now_local().isoformat(timespec="milliseconds") if "now_local" in globals() else "",
            "chat_id": chat_id,
            "message_id": message_id,
            "user_id": int(getattr(getattr(call, "from_user", None), "id", 0) or 0),
            "button": _v155_button_label(call, raw_data, resolved_data),
            "callback": str(resolved_data or raw_data)[:220],
            "source_marker": _v155_source_marker(call),
            "expected_marker": _v155_expected_marker(str(resolved_data or raw_data), chat_id),
            "actual_marker": str(actual_marker or ""),
            "result": result,
            "elapsed_ms": int(max(0.0, _v155_time.monotonic() - started) * 1000),
            "events": event_names[:500],
            "error": str(error or "")[:300],
        }
        with _V155_BUTTON_AUDIT_LOCK:
            _V155_BUTTON_AUDIT_RECENT.append(row)
        # Keep this compact: it is durable and intended to explain exactly what each click did.
        if str(resolved_data or "") != "none":
            bot_journal(
                "button_outcome", chat_id,
                _v155_json.dumps(row, ensure_ascii=False, separators=(",", ":"), default=str)[:1750],
                "ERROR" if result in {"exception", "window_edit_failed", "window_keyboard_edit_failed", "window_delete_failed", "window_send_failed", "window_edit_target_missing"} else "INFO",
            )
    except Exception:
        pass


def v155_button_audit_recent(limit: int = 100) -> list[dict]:
    try:
        limit = max(1, min(500, int(limit or 100)))
    except Exception:
        limit = 100
    with _V155_BUTTON_AUDIT_LOCK:
        return [_v155_copy.deepcopy(x) for x in list(_V155_BUTTON_AUDIT_RECENT)[-limit:]]


# ---------------------------------------------------------------------------
# Critical navigation fix: O9 secret gesture must never hijack normal buttons.
# Secret access already has explicit /secret*/секрет* handlers; navigation and
# close buttons are now always ordinary UI actions.
# ---------------------------------------------------------------------------
_V155_ORIG_O9_SECRET_TRIPLE_CLICK = globals().get("handle_o9_secret_triple_click")

def handle_o9_secret_triple_click(call, data_str: str) -> bool:
    return False


def _v155_cancel_o9_click_state(chat_id: int, message_id: int) -> None:
    try:
        lock = globals().get("_o9_secret_click_lock")
        clicks = globals().get("_o9_secret_clicks")
        timers = globals().get("_o9_secret_action_timers")
        cancel = globals().get("_cancel_o9_secret_timer")
        if lock is None or not isinstance(clicks, dict):
            return
        with lock:
            keys = [k for k in list(clicks) if isinstance(k, tuple) and len(k) >= 2 and int(k[0]) == int(chat_id) and int(k[1]) == int(message_id)]
        for key in keys:
            try:
                if callable(cancel):
                    cancel(key)
            except Exception:
                pass
            try:
                with lock:
                    clicks.pop(key, None)
                    if isinstance(timers, dict):
                        timers.pop(key, None)
            except Exception:
                pass
    except Exception:
        pass


def _v155_clear_nav_history(chat_id: int, message_id: int) -> None:
    try:
        lock = globals().get("_WINDOW_NAV_HISTORY_LOCK")
        history = globals().get("_WINDOW_NAV_HISTORY")
        key_fn = globals().get("_window_nav_key")
        if lock is not None and isinstance(history, dict):
            key = key_fn(chat_id, message_id) if callable(key_fn) else (int(chat_id), int(message_id))
            with lock:
                history.pop(key, None)
    except Exception:
        pass


def _v155_clear_window_local_waits(chat_id: int, message_id: int) -> None:
    """Clear only waits attached to the message being turned into the main window."""
    try:
        store = get_chat_store(int(chat_id))
    except Exception:
        return
    try:
        wait = store.get("secret_wait") or {}
        wait_mid = int(wait.get("prompt_msg_id") or wait.get("window_msg_id") or 0)
        if wait_mid == int(message_id):
            fn = globals().get("_clear_secret_wait")
            if callable(fn):
                fn(int(chat_id), delete_prompt=False)
    except Exception:
        pass
    try:
        wait = store.get("forward_copy_edit_wait") or {}
        wait_mid = int(wait.get("prompt_msg_id") or 0)
        if wait_mid == int(message_id):
            fn = globals().get("clear_forward_copy_edit_wait")
            if callable(fn):
                fn(int(chat_id), delete_prompt=False)
    except Exception:
        pass


_V155_ORIG_RETURN_TO_MAIN = globals().get("return_to_main_window_closing_previous")
if callable(_V155_ORIG_RETURN_TO_MAIN):
    def return_to_main_window_closing_previous(chat_id: int, day_key: str, current_message_id: int | None = None):
        try:
            if current_message_id is not None:
                _v155_cancel_o9_click_state(int(chat_id), int(current_message_id))
                _v155_clear_nav_history(int(chat_id), int(current_message_id))
                _v155_clear_window_local_waits(int(chat_id), int(current_message_id))
        except Exception:
            pass
        result = _V155_ORIG_RETURN_TO_MAIN(chat_id, day_key, current_message_id)
        try:
            bot_journal("back_main_clean", int(chat_id), f"day={str(day_key)[:10]}; msg={int(current_message_id or 0)}; secret_gesture=disabled; nav_history=cleared")
        except Exception:
            pass
        return result


# ---------------------------------------------------------------------------
# A real audit mismatch found in v154: this button said "add forgotten expense"
# but invoked expense_shortcut_test, i.e. it created a TEST quick-expense event.
# Do not silently perform a different action: make the label match the callback.
# ---------------------------------------------------------------------------
_V155_ORIG_BUILD_EXPENSE_INBOX_KEYBOARD = globals().get("build_expense_inbox_keyboard")
if callable(_V155_ORIG_BUILD_EXPENSE_INBOX_KEYBOARD):
    def build_expense_inbox_keyboard(chat_id: int):
        kb = _V155_ORIG_BUILD_EXPENSE_INBOX_KEYBOARD(chat_id)
        try:
            for row in list(getattr(kb, "keyboard", None) or []):
                for button in row or []:
                    if str(getattr(button, "callback_data", "") or "") == "expense_shortcut_test" and "забы" in str(getattr(button, "text", "") or "").casefold():
                        button.text = "🧪 Тест быстрой отметки"
        except Exception:
            pass
        return kb



# v155 full-state exports must be restorable by the same release.
def _v153_validate_restore_gz(gz_path: str) -> tuple[dict, str]:
    folder = _v155_tempfile.mkdtemp(prefix="v155_restore_validate_")
    raw = _v155_os.path.join(folder, "restore.sqlite3")
    try:
        with _v155_gzip.open(gz_path, "rb") as fin, open(raw, "wb") as fout:
            _v155_shutil.copyfileobj(fin, fout, 1024 * 1024)
        conn = _v155_sqlite3.connect(raw)
        try:
            integrity = str(conn.execute("PRAGMA integrity_check").fetchone()[0])
            if integrity.lower() != "ok":
                raise RuntimeError(f"SQLite integrity_check: {integrity}")
            row = conn.execute("SELECT v FROM meta WHERE kind='v153_export' AND k='manifest'").fetchone()
            if not row:
                raise RuntimeError("manifest v153 not found")
            manifest = _v155_json.loads(row[0])
        finally:
            conn.close()
        if str(manifest.get("kind")) != "telegram_bot_full_state_v153":
            raise RuntimeError("unknown export kind")
        if int(manifest.get("schema_version") or 0) != int(V153_EXPORT_SCHEMA):
            raise RuntimeError("unsupported export schema")
        export_version = str(manifest.get("bot_version") or "")
        if not export_version.startswith(("bot_v153_", "bot_v154_", "bot_v155_")):
            raise RuntimeError(f"unsupported bot version: {export_version or 'missing'}")
        checksum = _v153_db_logical_checksum(raw)
        if checksum != str(manifest.get("checksum") or ""):
            raise RuntimeError("checksum mismatch")
        return manifest, raw
    except Exception:
        _v155_shutil.rmtree(folder, ignore_errors=True)
        raise

# ---------------------------------------------------------------------------
# Runtime callback outcome audit. It wraps the already-registered handlers after
# v153 dedupe, so it does not change handler ordering or execute any callback twice.
# ---------------------------------------------------------------------------
def _v155_install_callback_outcome_audit() -> int:
    installed = 0
    for handler in list(getattr(bot, "callback_query_handlers", []) or []):
        if not isinstance(handler, dict):
            continue
        original = handler.get("function")
        if not callable(original) or getattr(original, "_v155_button_audit", False):
            continue

        def _wrapped(call, _original=original):
            raw = str(getattr(call, "data", "") or "")
            resolved = raw
            try:
                resolver = globals().get("resolve_short_callback")
                if callable(resolver):
                    resolved = str(resolver(raw) or raw)
            except Exception:
                pass
            started = _v155_time.monotonic()
            try:
                seq_before = int(globals().get("_WINDOW_DIAG_SEQ", 0) or 0)
            except Exception:
                seq_before = 0
            err = ""
            try:
                return _original(call)
            except Exception as exc:
                err = f"{type(exc).__name__}: {exc}"
                raise
            finally:
                _v155_record_button_outcome(call, raw, resolved, started, seq_before, err)

        _wrapped._v155_button_audit = True
        _wrapped.__name__ = getattr(original, "__name__", "callback_handler")
        handler["function"] = _wrapped
        installed += 1
    return installed


def _v155_button_audit_summary_text() -> str:
    rows = v155_button_audit_recent(120)
    counts = {}
    suspicious = []
    for row in rows:
        result = str((row or {}).get("result") or "unknown")
        counts[result] = int(counts.get(result, 0)) + 1
        expected = str((row or {}).get("expected_marker") or "")
        actual = str((row or {}).get("actual_marker") or "")
        if result in {"exception", "window_edit_failed", "window_keyboard_edit_failed", "window_delete_failed", "window_send_failed", "window_edit_target_missing"}:
            suspicious.append(row)
        elif expected and actual and expected not in {"Ф9998", "С9998", "П9998"} and expected != actual:
            suspicious.append(row)
    lines = [
        "🧭 АУДИТ КНОПОК v155",
        "",
        f"Live-аудит: {'ВКЛ' if V155_BUTTON_AUDIT_ENABLED else 'ВЫКЛ'}",
        f"Callback-обработчиков под наблюдением: {_V155_BUTTON_AUDIT_INSTALLED}",
        f"Последних кликов в памяти: {len(rows)}",
        f"Подозрительных результатов: {len(suspicious)}",
        "",
        "Результаты: " + (", ".join(f"{k}={v}" for k, v in sorted(counts.items())) if counts else "пока нет кликов после запуска"),
        "",
        "Обычная кнопка «Назад осн. окно» больше не связана с секретным жестом О9.",
        "Секретный доступ остаётся через существующие slash-команды /secret*/секрет*.",
    ]
    if suspicious:
        lines += ["", "Последние подозрительные:"]
        for row in suspicious[-8:]:
            lines.append(
                f"• {row.get('button') or row.get('callback')} → {row.get('result')} "
                f"({row.get('source_marker') or '-'}→{row.get('actual_marker') or row.get('expected_marker') or '-'})"
            )
    return "\n".join(lines)[:3900]


def v155_cmd_button_audit(msg):
    try:
        uid = int(getattr(getattr(msg, "from_user", None), "id", 0) or 0)
        chat_id = int(getattr(getattr(msg, "chat", None), "id", 0) or 0)
        owner = bool(uid and (uid == int(OWNER_ID or 0) or uid in {int(x) for x in get_additional_owner_ids()}))
        if not owner:
            bot.reply_to(msg, "Команда доступна владельцу.")
            return
        bot.reply_to(msg, _v155_button_audit_summary_text())
    except Exception as exc:
        try:
            bot.reply_to(msg, f"Не удалось собрать аудит кнопок: {exc}")
        except Exception:
            pass


try:
    bot.message_handler(commands=["button_audit"])(v155_cmd_button_audit)
except Exception:
    pass

_V155_BUTTON_AUDIT_INSTALLED = _v155_install_callback_outcome_audit()

try:
    bot_journal(
        "v155_button_navigation_audit_installed", int(OWNER_ID or 0),
        f"callback_handlers={_V155_BUTTON_AUDIT_INSTALLED}; o9_button_gesture=disabled; back_main_clean=1; outcome_audit={int(V155_BUTTON_AUDIT_ENABLED)}"
    )
except Exception:
    pass

# v155_button_navigation_audit
