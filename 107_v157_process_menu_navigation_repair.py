# v157_process_menu_navigation_repair
"""v157: process-window submenu, robust Back navigation, vertical INFO layout and button-log repairs."""

import copy as _v157_copy
import gzip as _v157_gzip
import json as _v157_json
import os as _v157_os
import re as _v157_re
import shutil as _v157_shutil
import sqlite3 as _v157_sqlite3
import tempfile as _v157_tempfile
import threading as _v157_threading
import time as _v157_time

VERSION = "bot_v157_process_menu_navigation_repair"

# ---------------------------------------------------------------------------
# Process window settings: one submenu in INFO, two explicit platform-owner
# switches — for the primary owner chat and for all other chats/users.
# ---------------------------------------------------------------------------
_V157_LEGACY_PROCESS_ENABLED = globals().get("process_visual_status_enabled")
_V157_PROCESS_SETTINGS_KEY = "process_visual_status_v157"


def _v157_is_primary_owner_chat(chat_id: int) -> bool:
    try:
        fn = globals().get("is_primary_owner")
        if callable(fn):
            return bool(fn(int(chat_id)))
    except Exception:
        pass
    try:
        return int(chat_id) == int(OWNER_ID or 0)
    except Exception:
        return False


def _v157_process_settings() -> dict:
    gs = data.setdefault("_global_settings", {})
    root = gs.get(_V157_PROCESS_SETTINGS_KEY)
    if not isinstance(root, dict):
        root = {}
        gs[_V157_PROCESS_SETTINGS_KEY] = root
    if "owner_enabled" not in root:
        default = True
        try:
            if callable(_V157_LEGACY_PROCESS_ENABLED) and OWNER_ID:
                default = bool(_V157_LEGACY_PROCESS_ENABLED(int(OWNER_ID)))
        except Exception:
            default = True
        root["owner_enabled"] = default
    if "others_enabled" not in root:
        root["others_enabled"] = True
    return root


def process_visual_status_enabled(chat_id: int) -> bool:
    try:
        cfg = _v157_process_settings()
        return bool(cfg.get("owner_enabled", True) if _v157_is_primary_owner_chat(int(chat_id)) else cfg.get("others_enabled", True))
    except Exception:
        return True


def _v157_save_process_settings() -> None:
    try:
        save_data(data, root_only=True)
    except Exception:
        pass
    try:
        if OWNER_ID:
            schedule_config_backup_for_chats(int(OWNER_ID), delay=0.5)
    except Exception:
        pass


def _v157_clear_process_scope(scope: str) -> None:
    try:
        with _V156_PROCESS_UI_LOCK:
            chat_ids = list((_V156_PROCESS_UI or {}).keys())
    except Exception:
        chat_ids = []
    for cid in chat_ids:
        try:
            is_owner = _v157_is_primary_owner_chat(int(cid))
            if (scope == "owner" and is_owner) or (scope == "others" and not is_owner):
                _v156_process_status_clear(int(cid), delete=True)
        except Exception:
            pass
    # Heavy file jobs have their own status message. Remove only the visual
    # message; never cancel the actual export/backup operation.
    try:
        with _FILE_JOB_LOCK:
            jobs = [dict(x) for x in (_FILE_JOB_STATE or {}).values() if isinstance(x, dict)]
        for row in jobs:
            cid = int(row.get("chat_id") or 0)
            is_owner = _v157_is_primary_owner_chat(cid)
            if not ((scope == "owner" and is_owner) or (scope == "others" and not is_owner)):
                continue
            mid = int(row.get("status_msg_id") or 0)
            if mid:
                try:
                    bot.delete_message(cid, mid)
                except Exception:
                    pass
                try:
                    with _FILE_JOB_LOCK:
                        for state in (_FILE_JOB_STATE or {}).values():
                            if isinstance(state, dict) and int(state.get("chat_id") or 0) == cid and int(state.get("status_msg_id") or 0) == mid:
                                state["status_msg_id"] = None
                except Exception:
                    pass
    except Exception:
        pass


def _v157_set_process_scope(scope: str, enabled: bool) -> bool:
    cfg = _v157_process_settings()
    key = "owner_enabled" if scope == "owner" else "others_enabled"
    cfg[key] = bool(enabled)
    _v157_save_process_settings()
    if not enabled:
        _v157_clear_process_scope(scope)
    try:
        bot_journal("process_visual_scope_toggle", int(OWNER_ID or 0), f"scope={scope}; enabled={int(bool(enabled))}")
    except Exception:
        pass
    return bool(enabled)


def _v157_process_menu_text() -> str:
    cfg = _v157_process_settings()
    return (
        "👁️ ОКНО ПРОЦЕССОВ\n\n"
        "Показывает одно служебное сообщение, пока операция действительно выполняется. "
        "После завершения сообщение показывает результат и закрывается.\n\n"
        f"👤 Для владельца: {'ВКЛ' if cfg.get('owner_enabled', True) else 'ВЫКЛ'}\n"
        f"👥 Для других чатов и пользователей: {'ВКЛ' if cfg.get('others_enabled', True) else 'ВЫКЛ'}\n\n"
        "Переключатели ниже независимы друг от друга."
    )


def _v157_process_menu_keyboard(chat_id: int):
    cfg = _v157_process_settings()
    kb = types.InlineKeyboardMarkup(row_width=1)
    kb.row(IB(f"👤 Для владельца: {'ВКЛ' if cfg.get('owner_enabled', True) else 'ВЫКЛ'}", callback_data="v157:process_owner_toggle"))
    kb.row(IB(f"👥 Для других чатов и пользователей: {'ВКЛ' if cfg.get('others_enabled', True) else 'ВЫКЛ'}", callback_data="v157:process_others_toggle"))
    day = get_chat_store(int(chat_id)).get("current_view_day") or today_key()
    kb.row(IB("🔙 Назад в Инфо", callback_data=f"d:{day}:info"))
    kb.row(IB("⬅️ Назад в основное окно", callback_data=f"d:{day}:back_main"))
    return kb


# ---------------------------------------------------------------------------
# INFO text/keyboard. v156 narrowed build_info_text to one positional argument,
# while legacy router still legitimately passes (chat_id, day). Keep a backward-
# compatible signature and make all buttons from "Перес:" downward vertical.
# ---------------------------------------------------------------------------
_V157_ORIG_BUILD_INFO_TEXT = globals().get("build_info_text")


def build_info_text(chat_id: int, *args, **kwargs) -> str:
    text = ""
    try:
        if callable(_V157_ORIG_BUILD_INFO_TEXT):
            text = str(_V157_ORIG_BUILD_INFO_TEXT(int(chat_id)) or "")
    except TypeError:
        try:
            text = str(_V157_ORIG_BUILD_INFO_TEXT(int(chat_id), *args, **kwargs) or "")
        except Exception:
            text = ""
    cfg = _v157_process_settings()
    summary = f"Окно процессов: владелец {'ВКЛ' if cfg.get('owner_enabled', True) else 'ВЫКЛ'} · остальные {'ВКЛ' if cfg.get('others_enabled', True) else 'ВЫКЛ'}"
    rows = text.splitlines()
    replaced = False
    for idx, row in enumerate(rows):
        if str(row).strip().startswith("Окно процессов:"):
            rows[idx] = summary
            replaced = True
            break
    if not replaced:
        try:
            idx = rows.index("Слеш-команды:")
            rows[idx:idx] = [summary, ""]
        except Exception:
            rows.extend(["", summary])
    return "\n".join(rows)[:3900]


_V157_ORIG_BUILD_INFO_KEYBOARD = globals().get("build_info_keyboard")


def _v157_kb_rows(kb):
    return list(getattr(kb, "keyboard", None) or getattr(kb, "inline_keyboard", None) or [])


def _v157_btn_text(btn) -> str:
    if isinstance(btn, dict):
        return str(btn.get("text") or "")
    return str(getattr(btn, "text", "") or "")


def _v157_btn_cb(btn) -> str:
    if isinstance(btn, dict):
        return str(btn.get("callback_data") or "")
    return str(getattr(btn, "callback_data", "") or "")


def build_info_keyboard(chat_id: int):
    kb = _V157_ORIG_BUILD_INFO_KEYBOARD(int(chat_id)) if callable(_V157_ORIG_BUILD_INFO_KEYBOARD) else types.InlineKeyboardMarkup()
    rows = _v157_kb_rows(kb)
    # Replace the direct v156 switch with an opening submenu. This also repairs
    # old menu construction without changing unrelated buttons above it.
    found_process = False
    for ridx, row in enumerate(rows):
        for bidx, btn in enumerate(list(row or [])):
            if _v157_btn_cb(btn) == "v156:process_visual_toggle" or _v157_btn_text(btn).strip().startswith("👁 Окно процессов") or _v157_btn_text(btn).strip().startswith("👁️ Окно процессов"):
                rows[ridx][bidx] = IB("👁️ Окно процессов", callback_data="v157:process_menu")
                found_process = True
    if not found_process and is_owner_chat(int(chat_id)):
        insert_at = max(0, len(rows) - 1)
        rows.insert(insert_at, [IB("👁️ Окно процессов", callback_data="v157:process_menu")])

    # User request: from the "Перес:" control and below, every INFO button is a
    # separate vertical row. Preserve the compact top section above that point.
    start = None
    for idx, row in enumerate(rows):
        if any("перес:" in _v157_btn_text(btn).casefold() for btn in (row or [])):
            start = idx
            break
    if start is not None:
        head = rows[:start]
        tail = []
        for row in rows[start:]:
            for btn in row or []:
                tail.append([btn])
        rows = head + tail
    try:
        kb.keyboard = rows
    except Exception:
        try:
            kb.inline_keyboard = rows
        except Exception:
            pass
    return kb


# ---------------------------------------------------------------------------
# Process status robustness: if Telegram says the old status message no longer
# exists, forget its id and recreate one while the operation is still active.
# ---------------------------------------------------------------------------
def _v157_process_message_missing(exc) -> bool:
    low = str(exc or "").casefold()
    return any(x in low for x in (
        "message_id_invalid", "message id invalid", "message to edit not found",
        "message not found", "message to delete not found",
    ))


def _v156_process_status_tick(chat_id: int) -> None:
    chat_id = int(chat_id)
    if not process_visual_status_enabled(chat_id):
        _v156_process_status_clear(chat_id, delete=True)
        return
    rows = _v156_active_process_rows(chat_id)
    with _V156_PROCESS_UI_LOCK:
        state = _V156_PROCESS_UI.get(chat_id) or {}
        msg_id = int(state.get("message_id") or 0)
        hint = str(state.get("hint") or "")
    if not rows:
        if msg_id:
            try:
                bot.edit_message_text(f"✅ {hint or 'Операция'}\nВыполнено.", chat_id=chat_id, message_id=msg_id)
                delete_message_later(chat_id, msg_id, 4)
            except Exception as exc:
                if not _v157_process_message_missing(exc):
                    try:
                        bot_journal("process_visual_status_finish_error", chat_id, str(exc)[:300], "WARN")
                    except Exception:
                        pass
        _v156_process_status_clear(chat_id, delete=False)
        return

    text = _v156_process_status_text(chat_id, rows, hint)
    if msg_id:
        try:
            bot.edit_message_text(text, chat_id=chat_id, message_id=msg_id)
        except Exception as exc:
            low = str(exc).casefold()
            if "message is not modified" in low:
                pass
            elif _v157_process_message_missing(exc):
                with _V156_PROCESS_UI_LOCK:
                    if chat_id in _V156_PROCESS_UI:
                        _V156_PROCESS_UI[chat_id]["message_id"] = 0
                msg_id = 0
            else:
                try:
                    bot_journal("process_visual_status_edit_error", chat_id, str(exc)[:300], "WARN")
                except Exception:
                    pass
    if not msg_id:
        try:
            sent = bot.send_message(chat_id, text, disable_notification=True)
            new_id = int(getattr(sent, "message_id", 0) or 0)
            if new_id:
                with _V156_PROCESS_UI_LOCK:
                    if chat_id in _V156_PROCESS_UI:
                        _V156_PROCESS_UI[chat_id]["message_id"] = new_id
        except Exception:
            pass
    _v156_process_status_schedule(chat_id, _V156_PROCESS_STATUS_INTERVAL)


# ---------------------------------------------------------------------------
# Navigation hardening.
# 1) Main-window action really renders Ф91, not the obsolete Ф40 expectation.
# 2) Generic "Назад" falls back to the main window if history is gone/stale.
# 3) /start reuses an existing main window instead of creating duplicates.
# ---------------------------------------------------------------------------
try:
    WINDOW_MARKER_CONSTANTS["d:*:back_main"] = "Ф91"
    if isinstance(globals().get("WINDOW_MARKER_CLOCK_CODES"), set):
        WINDOW_MARKER_CLOCK_CODES.add("Ф91")
except Exception:
    pass

# Declare marker families added by spaces/rights patches. This removes false
# ERRORs and lets the audit compare the real result instead of Ф9998.
try:
    WINDOW_MARKER_CONSTANTS.update({
        "sp:dashboard": "Ф217",
        "sp:list:*": "Ф217",
        "sp:open:*": "Ф218",
        "sp:users:*": "Ф219",
        "sp:chats:*": "Ф220",
        "sp:chatlink:*": "Ф221",
        "sp:userlink:*": "Ф222",
        "v152:r:l:*": "Ф223",
        "v152:r:c:*": "Ф224",
        "v152:r:t:*": "Ф225",
        "v152:r:g:*": "Ф226",
        "v152:r:i:*": "Ф227",
        "v152:r:p:*": "Ф228",
        "v152:r:x:*": "Ф229",
        "expense_quick_buttons_toggle": "Ф230",
        "v157:process_menu": "Ф231",
        "v157:process_owner_toggle": "Ф231",
        "v157:process_others_toggle": "Ф231",
        # Old messages from v156 open the new menu instead of toggling directly.
        "v156:process_visual_toggle": "Ф231",
    })
except Exception:
    pass

_V157_ORIG_V155_EXPECTED_MARKER = globals().get("_v155_expected_marker")


def _v155_expected_marker(action: str, chat_id: int) -> str:
    raw = str(action or "")
    if raw == "nav_prev":
        # The destination is the actual previous snapshot and therefore dynamic.
        return ""
    if raw.endswith(":back_main"):
        return "Ф91"
    if raw == "expense_quick_buttons_toggle":
        # It may be used both from INFO and from the quick-expense screen.
        return ""
    if callable(_V157_ORIG_V155_EXPECTED_MARKER):
        try:
            return str(_V157_ORIG_V155_EXPECTED_MARKER(raw, int(chat_id)) or "")
        except Exception:
            pass
    return ""


_V157_ORIG_RESTORE_PREVIOUS_WINDOW = globals().get("restore_previous_window")


def restore_previous_window(call) -> bool:
    try:
        chat_id = int(call.message.chat.id)
        message_id = int(call.message.message_id)
    except Exception:
        return False
    try:
        if callable(globals().get("window_has_previous")) and not window_has_previous(chat_id, message_id):
            day = get_chat_store(chat_id).get("current_view_day") or today_key()
            return_to_main_window_closing_previous(chat_id, day, current_message_id=message_id)
            try:
                bot_journal("nav_prev_fallback_main", chat_id, f"msg={message_id}; reason=no_history")
            except Exception:
                pass
            return True
    except Exception:
        pass
    ok = False
    try:
        ok = bool(_V157_ORIG_RESTORE_PREVIOUS_WINDOW(call)) if callable(_V157_ORIG_RESTORE_PREVIOUS_WINDOW) else False
    except Exception as exc:
        try:
            bot_journal("nav_prev_restore_error", chat_id, str(exc)[:300], "WARN")
        except Exception:
            pass
        ok = False
    if ok:
        return True
    day = get_chat_store(chat_id).get("current_view_day") or today_key()
    return_to_main_window_closing_previous(chat_id, day, current_message_id=message_id)
    try:
        bot_journal("nav_prev_fallback_main", chat_id, f"msg={message_id}; reason=restore_failed")
    except Exception:
        pass
    return True


_V157_ORIG_FORCE_NEW_DAY_WINDOW = globals().get("force_new_day_window")


def force_new_day_window(chat_id: int, day_key: str):
    chat_id = int(chat_id)
    day_key = str(day_key)[:10]
    try:
        old_mid = get_active_window_id(chat_id, day_key)
        old_mid = int(old_mid) if old_mid else 0
    except Exception:
        old_mid = 0
    if old_mid:
        try:
            backup_window_for_owner(chat_id, day_key, message_id_override=old_mid)
            # backup_window_for_owner may replace a missing id itself. Either way,
            # do not send an unconditional duplicate /start window afterwards.
            schedule_balance_panel_refresh(chat_id, 0.2)
            bot_journal("start_window_reused", chat_id, f"day={day_key}; previous={old_mid}; active={get_active_window_id(chat_id, day_key)}")
            return
        except Exception as exc:
            try:
                bot_journal("start_window_reuse_failed", chat_id, f"day={day_key}; msg={old_mid}; {str(exc)[:220]}", "WARN")
            except Exception:
                pass
    if callable(_V157_ORIG_FORCE_NEW_DAY_WINDOW):
        return _V157_ORIG_FORCE_NEW_DAY_WINDOW(chat_id, day_key)
    return None


# Stale delayed auto-return should not keep trying to edit a message already
# unregistered after another window became the active main window.
_V157_ORIG_RETURN_TO_MAIN = globals().get("return_to_main_window_closing_previous")


def return_to_main_window_closing_previous(chat_id: int, day_key: str, current_message_id: int | None = None):
    chat_id = int(chat_id)
    day_key = str(day_key)[:10]
    try:
        current_mid = int(current_message_id or 0)
    except Exception:
        current_mid = 0
    try:
        active_mid = int(get_active_window_id(chat_id, day_key) or 0)
    except Exception:
        active_mid = 0
    if current_mid and active_mid and current_mid != active_mid:
        try:
            reg_fn = globals().get("get_registered_open_window")
            registered = reg_fn(chat_id, current_mid) if callable(reg_fn) else None
            if not registered and _v157_threading.current_thread().name.startswith(("delayed", "general")):
                try:
                    backup_window_for_owner(chat_id, day_key, message_id_override=active_mid)
                    bot_journal("back_main_stale_timer_skipped", chat_id, f"stale={current_mid}; active={active_mid}; day={day_key}")
                    return
                except Exception:
                    pass
        except Exception:
            pass
    if callable(_V157_ORIG_RETURN_TO_MAIN):
        return _V157_ORIG_RETURN_TO_MAIN(chat_id, day_key, current_message_id=current_message_id)
    return None


# ---------------------------------------------------------------------------
# Old quick-expense messages: one missing Telegram message must not produce a
# second reply-markup failure forever. Mark it stale and stop refreshing it.
# ---------------------------------------------------------------------------
def migrate_recent_expense_shortcut_events(days: int = 2, refresh_messages: bool = False) -> dict:
    cfg_fn = globals().get("expense_shortcut_config")
    if not callable(cfg_fn):
        return {"imported": 0, "updated": 0, "seen": 0}
    try:
        shortcut = cfg_fn(False) or {}
    except Exception:
        shortcut = {}
    cutoff = now_local() - timedelta(days=max(1, int(days or 2)))
    imported = updated = seen = 0
    duplicate = too_old = missing_message_id = refresh_failed = stale_message = 0
    total_events = len(list(shortcut.get("events") or []))
    changed_events = False
    for event in list(shortcut.get("events") or []):
        if not isinstance(event, dict) or not event.get("id"):
            continue
        dt = _expense_event_dt(event)
        if dt is None or dt < cutoff:
            too_old += 1
            continue
        seen += 1
        event_id = str(event.get("id"))
        target = int(event.get("target_chat_id") or shortcut.get("target_chat_id") or OWNER_ID or 0)
        before = None
        with _EXPENSE_INBOX_LOCK:
            for existing in (_expense_inbox_root().get("items") or {}).values():
                if str((existing or {}).get("source_event_id") or "") == event_id:
                    before = existing
                    break
        draft = expense_draft_for_event(event_id, target, dt.isoformat(timespec="seconds"))
        if before is None:
            imported += 1
        else:
            duplicate += 1
        mid = int(event.get("telegram_message_id") or 0)
        if not mid:
            missing_message_id += 1
        if mid and int((draft or {}).get("telegram_message_id") or 0) != mid:
            with _EXPENSE_INBOX_LOCK:
                draft["telegram_message_id"] = mid
        if refresh_messages and mid and target:
            try:
                text_fn = globals().get("expense_compact_message_text")
                text = text_fn(dt.isoformat(timespec="seconds")) if callable(text_fn) else f"💸 iPhone · {dt.strftime('%H:%M')}"
                markup = expense_draft_message_keyboard(int(draft.get("id") or 0), target)
                bot.edit_message_text(text, chat_id=target, message_id=mid, reply_markup=markup)
                updated += 1
            except Exception as exc:
                low = str(exc).casefold()
                if "message is not modified" in low:
                    continue
                if _v157_process_message_missing(exc):
                    stale_message += 1
                    event["telegram_message_id"] = 0
                    changed_events = True
                    try:
                        with _EXPENSE_INBOX_LOCK:
                            draft["telegram_message_id"] = 0
                    except Exception:
                        pass
                    try:
                        unregister_open_window(target, mid)
                    except Exception:
                        pass
                    continue
                try:
                    bot.edit_message_reply_markup(
                        chat_id=target,
                        message_id=mid,
                        reply_markup=expense_draft_message_keyboard(int(draft.get("id") or 0), target),
                    )
                    updated += 1
                except Exception as exc2:
                    if _v157_process_message_missing(exc2):
                        stale_message += 1
                        event["telegram_message_id"] = 0
                        changed_events = True
                        try:
                            with _EXPENSE_INBOX_LOCK:
                                draft["telegram_message_id"] = 0
                        except Exception:
                            pass
                    else:
                        refresh_failed += 1
    root = _expense_inbox_root()
    root["recent_event_migration_v157_at"] = now_local().isoformat(timespec="seconds")
    _root_save("expense_recent_event_migration_v157")
    if changed_events:
        try:
            save_data(data, root_only=True)
        except Exception:
            pass
    try:
        bot_journal(
            "expense_recent_events_migrated_v157", int(OWNER_ID or 0),
            f"total={total_events} seen_48h={seen} imported={imported} existing={duplicate} updated={updated} "
            f"too_old_or_bad_date={too_old} missing_message_id={missing_message_id} stale_message={stale_message} refresh_failed={refresh_failed}",
        )
    except Exception:
        pass
    return {
        "imported": imported, "updated": updated, "seen": seen, "existing": duplicate,
        "too_old": too_old, "missing_message_id": missing_message_id,
        "stale_message": stale_message, "refresh_failed": refresh_failed,
    }


# ---------------------------------------------------------------------------
# Callback interception for the new process submenu. Existing v156 buttons in
# old Telegram messages are treated as "open menu", never as direct toggles.
# ---------------------------------------------------------------------------
def _v157_primary_actor(call) -> bool:
    try:
        return int(getattr(getattr(call, "from_user", None), "id", 0) or 0) == int(OWNER_ID or 0)
    except Exception:
        return False


def _v157_handle_callback(call) -> bool:
    raw = str(getattr(call, "data", "") or "")
    resolved = raw
    try:
        resolver = globals().get("resolve_short_callback")
        if callable(resolver):
            resolved = str(resolver(raw) or raw)
    except Exception:
        pass
    if resolved not in {
        "v157:process_menu", "v157:process_owner_toggle", "v157:process_others_toggle",
        "v156:process_visual_toggle",
    }:
        return False
    chat_id = int(call.message.chat.id)
    if resolved in {"v157:process_menu", "v156:process_visual_toggle"}:
        safe_edit(bot, call, _v157_process_menu_text(), reply_markup=_v157_process_menu_keyboard(chat_id))
        return True
    if not _v157_primary_actor(call):
        try:
            bot.answer_callback_query(call.id, "Эти переключатели доступны только основному владельцу.", show_alert=True)
        except Exception:
            pass
        return True
    scope = "owner" if resolved == "v157:process_owner_toggle" else "others"
    cfg = _v157_process_settings()
    key = "owner_enabled" if scope == "owner" else "others_enabled"
    enabled = _v157_set_process_scope(scope, not bool(cfg.get(key, True)))
    safe_edit(bot, call, _v157_process_menu_text(), reply_markup=_v157_process_menu_keyboard(chat_id))
    try:
        bot.answer_callback_query(call.id, f"{'Включено' if enabled else 'Выключено'}: {'владелец' if scope == 'owner' else 'другие чаты и пользователи'}")
    except Exception:
        pass
    return True


def _v157_install_callback_intercept() -> int:
    count = 0
    for handler in list(getattr(bot, "callback_query_handlers", []) or []):
        if not isinstance(handler, dict):
            continue
        original = handler.get("function")
        if not callable(original) or getattr(original, "_v157_intercept", False):
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
            if resolved in {
                "v157:process_menu", "v157:process_owner_toggle", "v157:process_others_toggle",
                "v156:process_visual_toggle",
            }:
                started = _v157_time.monotonic()
                try:
                    seq_before = int(globals().get("_WINDOW_DIAG_SEQ", 0) or 0)
                except Exception:
                    seq_before = 0
                try:
                    bot_journal("button_pressed", int(call.message.chat.id), str(resolved)[:500])
                except Exception:
                    pass
                err = ""
                try:
                    _v157_handle_callback(call)
                    return None
                except Exception as exc:
                    err = f"{type(exc).__name__}: {exc}"
                    raise
                finally:
                    try:
                        audit = globals().get("_v155_record_button_outcome")
                        if callable(audit):
                            audit(call, raw, resolved, started, seq_before, err)
                    except Exception:
                        pass
            return _original(call)
        _wrapped._v157_intercept = True
        _wrapped.__name__ = getattr(original, "__name__", "callback_handler")
        handler["function"] = _wrapped
        count += 1
    return count


# v157 full-state exports must remain restorable by this release.
def _v153_validate_restore_gz(gz_path: str) -> tuple[dict, str]:
    folder = _v157_tempfile.mkdtemp(prefix="v157_restore_validate_")
    raw = _v157_os.path.join(folder, "restore.sqlite3")
    try:
        with _v157_gzip.open(gz_path, "rb") as fin, open(raw, "wb") as fout:
            _v157_shutil.copyfileobj(fin, fout, 1024 * 1024)
        conn = _v157_sqlite3.connect(raw)
        try:
            integrity = str(conn.execute("PRAGMA integrity_check").fetchone()[0])
            if integrity.lower() != "ok":
                raise RuntimeError(f"SQLite integrity_check: {integrity}")
            row = conn.execute("SELECT v FROM meta WHERE kind='v153_export' AND k='manifest'").fetchone()
            if not row:
                raise RuntimeError("manifest v153 not found")
            manifest = _v157_json.loads(row[0])
        finally:
            conn.close()
        if str(manifest.get("kind")) != "telegram_bot_full_state_v153":
            raise RuntimeError("unknown export kind")
        if int(manifest.get("schema_version") or 0) != int(V153_EXPORT_SCHEMA):
            raise RuntimeError("unsupported export schema")
        export_version = str(manifest.get("bot_version") or "")
        if not export_version.startswith(("bot_v153_", "bot_v154_", "bot_v155_", "bot_v156_", "bot_v157_")):
            raise RuntimeError(f"unsupported bot version: {export_version or 'missing'}")
        checksum = _v153_db_logical_checksum(raw)
        if checksum != str(manifest.get("checksum") or ""):
            raise RuntimeError("checksum mismatch")
        return manifest, raw
    except Exception:
        _v157_shutil.rmtree(folder, ignore_errors=True)
        raise


_V157_CALLBACK_INTERCEPTS = _v157_install_callback_intercept()
try:
    bot_journal(
        "v157_process_menu_navigation_repair_installed", int(OWNER_ID or 0),
        f"callback_intercepts={_V157_CALLBACK_INTERCEPTS}; process_scopes=2; info_vertical_from_peres=1; "
        "back_main_marker=Ф91; nav_prev_fallback=1; start_window_reuse=1; stale_quick_buttons=1",
    )
except Exception:
    pass

# v157_process_menu_navigation_repair
