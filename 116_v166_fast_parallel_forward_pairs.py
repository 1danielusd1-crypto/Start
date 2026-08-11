# v178_global_performance_final
"""v166: restore forwarding pairs, fast callbacks, parallel per-window UI and fast finance refresh.

Safety rule: actual finance mutations remain chat-serialized. Independent window UI, forwarding-pair
configuration and post-commit finance-window refreshes are separated into dedicated keyed lanes.
"""

import contextlib as _v166_contextlib
import gzip as _v166_gzip
import json as _v166_json
import os as _v166_os
import shutil as _v166_shutil
import sqlite3 as _v166_sqlite3
import tempfile as _v166_tempfile
import threading as _v166_threading
import time as _v166_time

VERSION = "bot_v166_fast_parallel_forward_pairs"

# ---------------------------------------------------------------------------
# Pools / execution lanes.
# ---------------------------------------------------------------------------
V166_WINDOW_UI_TASK_POOL = KeyedTaskPool(
    "window-fast",
    _env_int("V166_WINDOW_UI_WORKERS", 3, 2, 6),
    _env_int("V166_WINDOW_UI_MAX_PENDING", 900, 100, 4000),
)
V166_FORWARD_CONFIG_TASK_POOL = KeyedTaskPool(
    "forward-config",
    _env_int("V166_FORWARD_CONFIG_WORKERS", 1, 1, 3),
    _env_int("V166_FORWARD_CONFIG_MAX_PENDING", 300, 50, 1500),
)
V166_FINANCE_UI_TASK_POOL = KeyedTaskPool(
    "finance-ui",
    _env_int("V166_FINANCE_UI_WORKERS", 2, 2, 4),
    _env_int("V166_FINANCE_UI_MAX_PENDING", 700, 100, 3000),
)
V166_CONFIG_IO_TASK_POOL = KeyedTaskPool(
    "config-io",
    1,
    _env_int("V166_CONFIG_IO_MAX_PENDING", 120, 20, 500),
)
# Debounce timers get their own queues. The global DELAYED worker is often busy with MEGA/recovery
# work; forwarding persistence and finance repaint must not wait behind that unrelated traffic.
V166_CONFIG_IO_SCHEDULER = DelayedTaskScheduler(V166_CONFIG_IO_TASK_POOL)
V166_FINANCE_DEBOUNCE_TASK_POOL = KeyedTaskPool(
    "finance-debounce",
    _env_int("V166_FINANCE_DEBOUNCE_WORKERS", 1, 1, 3),
    _env_int("V166_FINANCE_DEBOUNCE_MAX_PENDING", 300, 50, 1200),
)
V166_FINANCE_DEBOUNCE_SCHEDULER = DelayedTaskScheduler(V166_FINANCE_DEBOUNCE_TASK_POOL)

_V166_PAIR_EXEC_GUARD = _v166_threading.RLock()
_V166_PAIR_EXEC_LOCKS = {}
_V166_FORWARD_STATE_LOCK = _v166_threading.RLock()
_V166_FORWARD_DIRTY_LOCK = _v166_threading.RLock()
_V166_FORWARD_DIRTY_CHATS = set()

_V166_PREV_ACK = globals().get("schedule_callback_receipt_ack")
_V166_PREV_REFRESH_BALANCE = globals().get("refresh_balance_panel_now")
_V166_PREV_REFRESH_TOTAL = globals().get("refresh_total_message_if_any")
_V166_PREV_RESTORE_VALIDATE = globals().get("_v153_validate_restore_gz")


def _v166_callback_raw_parts(payload: dict):
    try:
        cq = (payload or {}).get("callback_query") or {}
        msg = cq.get("message") or {}
        chat = msg.get("chat") or {}
        return str(cq.get("data") or ""), int(chat.get("id") or 0), int(msg.get("message_id") or 0)
    except Exception:
        return "", 0, 0


def _v166_forward_pair_from_callback(raw: str):
    raw = str(raw or "")
    prefixes = (
        "fw_new_mode:", "fw_new_fin:", "fw_new_clear:",
        "fw_mode:", "fw_finpair:", "fw_clear:",
    )
    if not raw.startswith(prefixes):
        return None
    nums = []
    for part in raw.split(":")[1:]:
        try:
            nums.append(int(part))
        except Exception:
            continue
        if len(nums) >= 2:
            break
    if len(nums) < 2:
        return None
    a, b = nums[0], nums[1]
    return (a, b) if a <= b else (b, a)


def _v166_is_finance_business_callback(raw: str) -> bool:
    """Callbacks that can create/delete/edit money stay serialized per chat."""
    low = str(raw or "").casefold()
    if low.startswith(("fw_new_fin:", "fw_new_mode:", "fw_new_clear:", "fw_mode:", "fw_finpair:", "fw_clear:")):
        return False  # dedicated pair lane
    hard_prefixes = (
        "fv:", "fv_", "edit_", "del_", "delete_", "expense_", "income_",
        "rec_", "record_", "usd_edit", "usd_del", "cat_move", "cat_delete",
    )
    if low.startswith(hard_prefixes):
        return True
    dangerous = ("delete_selected", "apply", "save", "confirm", "finance_off", "fin_mode_", "qb_mode_", "qb_hidden_")
    if any(token in low for token in dangerous):
        return True
    if low.startswith("d:"):
        try:
            cmd = low.split(":", 2)[2]
        except Exception:
            cmd = low
        # Menu/view/display toggles are safe per concrete Telegram window.
        safe_tokens = (
            "info", "back_main", "forward_menu", "forward_finmode_menu", "calendar",
            "articles_toggle", "financial_values_toggle", "usd_tx_toggle", "usd_display_toggle",
        )
        if any(token in cmd for token in safe_tokens):
            return False
        if any(token in cmd for token in ("delete", "edit", "save", "apply", "fin_mode_", "qb_mode_", "qb_hidden_")):
            return True
    return False


def _v166_is_safe_window_callback(raw: str) -> bool:
    low = str(raw or "").casefold()
    if not low:
        return False
    if _v166_is_finance_business_callback(raw):
        return False
    # Secret content mutations remain on the old chat lane.
    if low.startswith(("secret", "sec:", "o9:")) and not any(x in low for x in ("back", "close", "menu", "page", "list", "view")):
        return False
    # Forward pair mutations have their own pair lane.
    if _v166_forward_pair_from_callback(raw) is not None:
        return False
    # Known harmless UI preference toggles.
    if low in {
        "forward_menu_style_toggle", "buttons_current_toggle", "icon_buttons_toggle",
        "reminder_ui_mode_toggle", "internal_timers", "process_center", "problem_tasks",
        "journal_open", "journal_back", "keepalive_status", "info_queues", "info_delta_status",
    }:
        return True
    if low.startswith((
        "fw_new_src:", "fw_new_tgt:", "fw_new_pair:", "fw_src:", "fw_tgt:",
        "fw_back", "fw_new_back", "fw_probe", "fw_removed", "chat_desc_", "v164:circle:",
        "rem:list", "rem:open", "rem:completed", "itmr_", "journal_", "version_",
    )):
        return True
    if low == "nav_prev" or "back" in low or low.endswith("_close") or low.startswith("close_"):
        return True
    if low.startswith("d:") and not _v166_is_finance_business_callback(raw):
        return True
    # Generic read/navigation words only; unknown mutations keep old serialization.
    return any(token in low for token in ("menu", "page", "list", "view", "status", "refresh", "open"))


def v163_webhook_select_lane(payload: dict, update_type: str, update_key):
    """v166: /start separate; forwarding pair by pair; safe UI by concrete message; finance serial."""
    if str(update_type) == "message" and _v163_start_payload(payload):
        chat_id = _extract_update_chat_id(payload)
        return START_UI_TASK_POOL, f"start:{chat_id if chat_id is not None else update_key}"
    if str(update_type) == "callback_query":
        raw, chat_id, message_id = _v166_callback_raw_parts(payload)
        pair = _v166_forward_pair_from_callback(raw)
        if pair is not None:
            return V166_FORWARD_CONFIG_TASK_POOL, f"pair:{pair[0]}:{pair[1]}"
        if chat_id and message_id and _v166_is_safe_window_callback(raw):
            return V166_WINDOW_UI_TASK_POOL, f"window:{chat_id}:{message_id}"
        return UI_TASK_POOL, f"ui:{chat_id if chat_id else update_key}"
    return WEBHOOK_TASK_POOL, update_key


def _v166_pair_lock(pair):
    with _V166_PAIR_EXEC_GUARD:
        lock = _V166_PAIR_EXEC_LOCKS.get(pair)
        if lock is None:
            lock = _v166_threading.RLock()
            _V166_PAIR_EXEC_LOCKS[pair] = lock
        return lock


def _execute_telegram_payload(payload: dict, update_id=None, update_chat_id=None, update_type: str = "other"):
    """Match execution locking to the v166 queue lane, so independent windows truly run in parallel."""
    update = telebot.types.Update.de_json(payload)
    if update_chat_id is None:
        update_chat_id = _extract_update_chat_id(payload) if isinstance(payload, dict) else None
    previous_ctx = getattr(_TELEGRAM_UPDATE_CONTEXT, "value", None)
    critical_callback_target = _durable_callback_target_chat(payload) if isinstance(payload, dict) else None
    callback_data = ""
    source_message_id = None
    source_user_id = None
    try:
        if isinstance(payload, dict):
            callback = payload.get("callback_query") or {}
            if isinstance(callback, dict):
                callback_data = str(callback.get("data") or "")
                source_user_id = ((callback.get("from") or {}).get("id") if isinstance(callback.get("from"), dict) else None)
                callback_message = callback.get("message") or {}
                if isinstance(callback_message, dict):
                    source_message_id = callback_message.get("message_id")
            if source_message_id is None:
                message_payload = payload.get("message") or payload.get("edited_message") or payload.get("channel_post") or payload.get("edited_channel_post") or {}
                if isinstance(message_payload, dict):
                    source_message_id = message_payload.get("message_id")
                    source_user_id = source_user_id or ((message_payload.get("from") or {}).get("id") if isinstance(message_payload.get("from"), dict) else None)
    except Exception:
        callback_data = ""

    _TELEGRAM_UPDATE_CONTEXT.value = {
        "update_id": update_id,
        "chat_id": update_chat_id,
        "update_type": str(update_type or "other"),
        "callback_data": callback_data,
        "message_id": source_message_id,
        "user_id": source_user_id,
        "critical_callback": critical_callback_target is not None,
        "critical_callback_target": critical_callback_target,
        "deferred_quick_chats": set(),
    }
    execution_ctx = {}
    try:
        with state_chat_context(update_chat_id):
            pair = _v166_forward_pair_from_callback(callback_data) if str(update_type) == "callback_query" else None
            if update_chat_id is None:
                lock_ctx = _v166_contextlib.nullcontext()
            elif str(update_type) == "message" and _v163_start_payload(payload):
                lock_ctx = _v163_lock_for(_V163_START_EXEC_LOCKS, _V163_START_EXEC_LOCK_GUARD, int(update_chat_id))
            elif pair is not None:
                lock_ctx = _v166_pair_lock(pair)
            elif str(update_type) == "callback_query" and source_message_id and _v166_is_safe_window_callback(callback_data):
                lock_ctx = _v163_lock_for(
                    _V163_WINDOW_EXEC_LOCKS,
                    _V163_WINDOW_EXEC_LOCK_GUARD,
                    (int(update_chat_id), int(source_message_id)),
                )
            else:
                lock_ctx = chat_lock_for(int(update_chat_id))
            with lock_ctx:
                bot.process_new_updates([update])
        execution_ctx = _durable_execution_context_snapshot()
        try:
            fn = globals().get("_v150_store_receipt")
            if callable(fn):
                fn(payload)
        except Exception as exc:
            try: log_error(f"v166 command receipt: {exc}")
            except Exception: pass
    finally:
        if not execution_ctx:
            execution_ctx = _durable_execution_context_snapshot()
        if previous_ctx is None:
            try: delattr(_TELEGRAM_UPDATE_CONTEXT, "value")
            except Exception: pass
        else:
            _TELEGRAM_UPDATE_CONTEXT.value = previous_ctx
    return execution_ctx


# ---------------------------------------------------------------------------
# Callback ACK: 50 ms receipt fallback instead of 150 ms.
# ---------------------------------------------------------------------------
def schedule_callback_receipt_ack(callback_id: str, chat_id=None, delay: float | None = None):
    if callable(_V166_PREV_ACK):
        return _V166_PREV_ACK(callback_id, chat_id, 0.05)


# ---------------------------------------------------------------------------
# Forwarding pairs: build from the actual global rules, not the old v148 same-tenant filter.
# ---------------------------------------------------------------------------
def _v166_pair_key(a: int, b: int):
    a, b = int(a), int(b)
    return (a, b) if a <= b else (b, a)


def _v166_raw_forward_pairs():
    # Snapshot under the common data lock: forwarding workers may read the same maps concurrently.
    with data_lock:
        fr = {str(k): dict(v or {}) for k, v in (data.get("forward_rules", {}) or {}).items()}
        ff = {str(k): dict(v or {}) for k, v in (data.get("forward_finance", {}) or {}).items()}
        order = list(data.get("forward_pair_order", []) or [])
    pairs = []
    seen = set()

    def add(a, b):
        try:
            a, b = int(a), int(b)
        except Exception:
            return
        if a == b:
            return
        key = _v166_pair_key(a, b)
        if key in seen:
            return
        # Display only a real live relation/finance setting.
        ab = str(b) in (fr.get(str(a), {}) or {})
        ba = str(a) in (fr.get(str(b), {}) or {})
        af = bool((ff.get(str(a), {}) or {}).get(str(b), False))
        bf = bool((ff.get(str(b), {}) or {}).get(str(a), False))
        if not (ab or ba or af or bf):
            return
        seen.add(key)
        pairs.append((a, b))

    if isinstance(order, list):
        for raw in order:
            try:
                a_s, b_s = str(raw).split(":", 1)
                add(int(a_s), int(b_s))
            except Exception:
                continue
    for src, dsts in fr.items():
        for dst in (dsts or {}).keys():
            add(src, dst)
    for src, dsts in ff.items():
        for dst, enabled in (dsts or {}).items():
            if enabled:
                add(src, dst)
    return pairs


def _v166_forward_allowed_ids():
    level = _v164_current_window_circle("forward", 1)
    try:
        ctx = int(current_state_chat_id() or 0)
    except Exception:
        ctx = 0
    allowed = set(int(x) for x in (_v164_scope_ids(level, ctx) or []))
    if int(level) == 1 and _v165_is_platform_owner_context():
        try: allowed.add(int(OWNER_ID))
        except Exception: pass
    return int(level), allowed


def collect_forward_pairs_for_menu() -> list[tuple[int, int]]:
    level, allowed = _v166_forward_allowed_ids()
    out = []
    for a, b in _v166_raw_forward_pairs():
        if a in allowed:
            out.append((a, b))
        elif b in allowed:
            # Legacy pair order may have the visible circle on the right. Reorient only for UI.
            out.append((b, a))
    try:
        bot_journal("v166_forward_pairs_menu", current_state_chat_id(), f"circle={level} raw={len(_v166_raw_forward_pairs())} shown={len(out)}")
    except Exception:
        pass
    return out


def build_forward_status_lines() -> list[str]:
    lines = []
    for a, b in collect_forward_pairs_for_menu():
        try:
            arrow, fin, ab_on, ba_on, ab_fin, ba_fin = _forward_pair_icons(a, b)
            if ab_on or ba_on or ab_fin or ba_fin:
                lines.append(f"• {chat_button_title(a)} -({arrow})-({fin})-{chat_button_title(b)}")
        except Exception:
            continue
    return lines


# ---------------------------------------------------------------------------
# Fast forwarding configuration. Network/durable config persistence is moved off the UI callback.
# ---------------------------------------------------------------------------
def _v166_schedule_forward_persist(*chat_ids):
    with _V166_FORWARD_DIRTY_LOCK:
        for cid in chat_ids:
            try: _V166_FORWARD_DIRTY_CHATS.add(int(cid))
            except Exception: pass

    def _fire():
        def _persist():
            with _V166_FORWARD_DIRTY_LOCK:
                ids = sorted(_V166_FORWARD_DIRTY_CHATS)
                _V166_FORWARD_DIRTY_CHATS.clear()
            try:
                save_data(data, full=True)
            except Exception as exc:
                try: log_error(f"v166 forward local persist: {exc}")
                except Exception: pass
            try:
                path = _owner_data_file()
                if path:
                    payload = _load_json(path, {}) or {}
                    if not isinstance(payload, dict):
                        payload = {}
                    payload["forward_rules"] = data.get("forward_rules", {}) or {}
                    payload["forward_finance"] = data.get("forward_finance", {}) or {}
                    payload["forward_pair_order"] = data.get("forward_pair_order", []) or []
                    _save_json(path, payload)
            except Exception as exc:
                try: log_error(f"v166 forward legacy persist: {exc}")
                except Exception: pass
            try:
                if ids:
                    schedule_config_backup_for_chats(*ids, delay=0.3)
                else:
                    schedule_config_backup_for_chats(delay=0.3)
            except Exception:
                pass
        if not V166_CONFIG_IO_TASK_POOL.submit("forward-persist", _persist):
            try: log_error("V166 CONFIG IO QUEUE FULL: forward-persist")
            except Exception: pass

    try:
        V166_CONFIG_IO_SCHEDULER.cancel("v166-forward-persist")
        V166_CONFIG_IO_SCHEDULER.schedule("v166-forward-persist", 0.12, _fire)
    except Exception:
        _fire()


def _v166_authorize_pair(src: int, dst: int):
    src, dst = int(src), int(dst)
    if tenant_same_space(src, dst):
        return
    try:
        actor = int(tenant_current_actor_user_id() or 0)
    except Exception:
        actor = 0
    if not tenant_is_platform_owner_user(actor):
        raise PermissionError("Можно связывать только свой 1-й круг и его 2-й круг")
    try:
        key = f"{min(src, dst)}:{max(src, dst)}"
        _v164_root().setdefault("global_forward_pairs", {})[key] = {
            "src": src, "dst": dst, "created_by": actor, "created_at": _v164_now(),
        }
    except Exception:
        pass


def _v166_cleanup_global_pair(a: int, b: int):
    try:
        arrow, fin, ab_on, ba_on, ab_fin, ba_fin = _forward_pair_icons(a, b)
        if ab_on or ba_on or ab_fin or ba_fin:
            return
        _v164_root().setdefault("global_forward_pairs", {}).pop(f"{min(int(a), int(b))}:{max(int(a), int(b))}", None)
    except Exception:
        pass


def _v166_enable_hidden_finance_memory(dst_chat_id: int):
    dst_chat_id = int(dst_chat_id)
    # The in-memory switch is short but finance-sensitive, so keep it under the destination chat lock.
    # Only SQLite/MEGA persistence is detached from the button path.
    with locked_chat(dst_chat_id):
        store = get_chat_store(dst_chat_id)
        settings = store.setdefault("settings", {})
        was_enabled = bool(store.get("finance_mode", False))
        store["finance_mode"] = True
        try: finance_active_chats.add(dst_chat_id)
        except Exception: pass
        settings["hidden_finance"] = True
        if not was_enabled:
            settings["quick_balance_enabled"] = False
            settings["quick_balance_behavior"] = "normal"
            settings["quick_balance_user_selected"] = True
            state = store.get("finance_window_state")
            if not isinstance(state, dict):
                state = {}
            state.update({
                "mode": "off", "main_windows": {}, "balance_panel_id": None,
                "balance_panel_mode": "mini", "current_view_day": str(store.get("current_view_day") or today_key()),
                "auto_reopen_on_boot": False, "updated_at": now_local().isoformat(timespec="seconds"),
            })
            store["finance_window_state"] = state
    _v166_schedule_forward_persist(dst_chat_id)


def ensure_hidden_finance_for_forward_dst(dst_chat_id: int):
    try:
        _v166_enable_hidden_finance_memory(int(dst_chat_id))
        bot_journal("forward_finance_auto_hidden", int(dst_chat_id), "v166 fast: hidden finance enabled; durable config queued")
    except Exception as exc:
        log_error(f"v166 ensure_hidden_finance_for_forward_dst({dst_chat_id}): {exc}")


def add_forward_link(src_chat_id: int, dst_chat_id: int, mode: str):
    src, dst = int(src_chat_id), int(dst_chat_id)
    _v166_authorize_pair(src, dst)
    with data_lock, _V166_FORWARD_STATE_LOCK:
        data.setdefault("forward_rules", {}).setdefault(str(src), {})[str(dst)] = str(mode)
    _v166_schedule_forward_persist(src, dst)


def remove_forward_link(src_chat_id: int, dst_chat_id: int):
    src, dst = int(src_chat_id), int(dst_chat_id)
    with data_lock, _V166_FORWARD_STATE_LOCK:
        fr = data.setdefault("forward_rules", {})
        ff = data.setdefault("forward_finance", {})
        (fr.get(str(src)) or {}).pop(str(dst), None)
        if str(src) in fr and not fr.get(str(src)):
            fr.pop(str(src), None)
        (ff.get(str(src)) or {}).pop(str(dst), None)
        if str(src) in ff and not ff.get(str(src)):
            ff.pop(str(src), None)
    _v166_cleanup_global_pair(src, dst)
    _v166_schedule_forward_persist(src, dst)


def set_forward_finance(src_chat_id: int, dst_chat_id: int, enabled: bool):
    src, dst = int(src_chat_id), int(dst_chat_id)
    _v166_authorize_pair(src, dst)
    with data_lock, _V166_FORWARD_STATE_LOCK:
        data.setdefault("forward_finance", {}).setdefault(str(src), {})[str(dst)] = bool(enabled)
    if enabled:
        ensure_hidden_finance_for_forward_dst(dst)
    _v166_schedule_forward_persist(src, dst)


def remove_forward_finance(src_chat_id: int, dst_chat_id: int):
    src, dst = int(src_chat_id), int(dst_chat_id)
    with data_lock, _V166_FORWARD_STATE_LOCK:
        ff = data.setdefault("forward_finance", {})
        (ff.get(str(src)) or {}).pop(str(dst), None)
        if str(src) in ff and not ff.get(str(src)):
            ff.pop(str(src), None)
    _v166_cleanup_global_pair(src, dst)
    _v166_schedule_forward_persist(src, dst)


def _remember_forward_pair(A: int, B: int):
    A, B = int(A), int(B)
    if A == B:
        return
    key, rev = f"{A}:{B}", f"{B}:{A}"
    with data_lock, _V166_FORWARD_STATE_LOCK:
        order = data.setdefault("forward_pair_order", [])
        if not isinstance(order, list):
            order = []
            data["forward_pair_order"] = order
        if key not in order and rev not in order:
            order.append(key)
    _v166_schedule_forward_persist(A, B)


def _forget_forward_pair_if_empty(A: int, B: int):
    A, B = int(A), int(B)
    try:
        arrow, fin, ab_on, ba_on, ab_fin, ba_fin = _forward_pair_icons(A, B)
        if ab_on or ba_on or ab_fin or ba_fin:
            return
    except Exception:
        return
    key, rev = f"{A}:{B}", f"{B}:{A}"
    with data_lock, _V166_FORWARD_STATE_LOCK:
        order = data.setdefault("forward_pair_order", [])
        if isinstance(order, list):
            data["forward_pair_order"] = [x for x in order if x not in {key, rev}]
    _v166_cleanup_global_pair(A, B)
    _v166_schedule_forward_persist(A, B)


# UI-only style toggle must repaint first; persistence can follow asynchronously.
def set_forward_menu_new_style_enabled(enabled: bool, chat_id: int | None = None):
    cid = int(chat_id) if chat_id is not None else current_state_chat_id()
    if cid is not None:
        owner_scoped_settings(int(cid))["forward_menu_new_style"] = bool(enabled)
        def _persist():
            try:
                save_data(data, chat_ids=[int(cid)])
                schedule_config_backup_for_chats(int(cid), delay=0.3)
            except Exception as exc:
                try: log_error(f"v166 forward style persist: {exc}")
                except Exception: pass
        V166_CONFIG_IO_TASK_POOL.submit(f"style:{int(cid)}", _persist)
    else:
        data.setdefault("_global_settings", {})["forward_menu_new_style"] = bool(enabled)
        V166_CONFIG_IO_TASK_POOL.submit("style:global", save_data, data)


def toggle_forward_menu_new_style(chat_id: int | None = None) -> bool:
    new_value = not forward_menu_new_style_enabled(chat_id)
    set_forward_menu_new_style_enabled(new_value, chat_id)
    return new_value


# ---------------------------------------------------------------------------
# Finance UI refresh: post-commit Telegram windows are independent per message.
# ---------------------------------------------------------------------------
def _v166_fin_submit(key, fn, *args):
    if not V166_FINANCE_UI_TASK_POOL.submit(str(key), fn, *args):
        try: log_error(f"V166 FINANCE UI QUEUE FULL: {key}")
        except Exception: pass
        return False
    return True


def refresh_balance_panel_now(chat_id: int):
    if callable(_V166_PREV_REFRESH_BALANCE):
        _v166_fin_submit(f"balance:{int(chat_id)}", _V166_PREV_REFRESH_BALANCE, int(chat_id))


def refresh_total_message_if_any(chat_id: int):
    if callable(_V166_PREV_REFRESH_TOTAL):
        _v166_fin_submit(f"total:{int(chat_id)}", _V166_PREV_REFRESH_TOTAL, int(chat_id))


def _v166_refresh_main_one(chat_id: int, day_key: str, mid: int):
    try:
        actual = get_registered_open_window(int(chat_id), int(mid))
        if actual and str(actual.get("window_type") or "") not in {"", "main_day"}:
            return
        text, _ = render_day_window(int(chat_id), str(day_key))
        bot.edit_message_text(
            text, chat_id=int(chat_id), message_id=int(mid),
            reply_markup=build_main_keyboard(str(day_key), int(chat_id)),
        )
        register_open_window(int(chat_id), int(mid), "main_day", code="О1", day_key=str(day_key))
    except Exception as exc:
        if "message is not modified" in str(exc).lower():
            return
        if _message_missing_error(exc):
            try:
                if int(get_active_window_id(int(chat_id), str(day_key)) or 0) == int(mid):
                    clear_active_window_id(int(chat_id), str(day_key))
            except Exception:
                pass
            try: unregister_open_window(int(chat_id), int(mid))
            except Exception: pass
            return
        try: log_error(f"v166 refresh main {chat_id}:{mid}: {exc}")
        except Exception: pass


def _v166_refresh_remaining(chat_id: int, mid: int):
    store = get_chat_store(int(chat_id))
    day_key = store.get("current_view_day") or today_key()
    try:
        bot.edit_message_text(
            build_remaining_text(int(chat_id), day_key), chat_id=int(chat_id), message_id=int(mid),
            reply_markup=build_remaining_keyboard(int(chat_id), day_key), parse_mode="HTML",
        )
        register_open_window(int(chat_id), int(mid), "remaining", code="Ф91", day_key=day_key)
    except Exception as exc:
        if _message_missing_error(exc):
            store["remaining_msg_id"] = None
            try: unregister_open_window(int(chat_id), int(mid))
            except Exception: pass


def _v166_refresh_registry_item(item: dict, target_chat_id: int):
    try:
        wtype = str((item or {}).get("window_type") or "")
        if wtype == "fin_view":
            _refresh_registered_fin_view(item, int(target_chat_id))
        elif wtype == "local_fin_view":
            _refresh_registered_local_fin_view(item, int(target_chat_id))
        elif wtype == "fin_categories_view":
            _refresh_registered_fin_categories_view(item, int(target_chat_id))
        elif wtype == "stored":
            _refresh_registered_stored_window(item, int(target_chat_id))
    except Exception as exc:
        try: log_error(f"v166 finance registry refresh: {exc}")
        except Exception: pass


def _v168_fin_window_is_recent(item: dict, max_age_seconds: float = 900.0) -> bool:
    """Old parallel Telegram windows remain usable, but do not auto-repaint forever on every transaction."""
    try:
        raw = str((item or {}).get("last_interaction_at") or (item or {}).get("updated_at") or "")
        if not raw:
            return False
        dt = datetime.fromisoformat(raw)
        now = now_local()
        if dt.tzinfo is None and getattr(now, "tzinfo", None) is not None:
            dt = dt.replace(tzinfo=now.tzinfo)
        return (now - dt).total_seconds() <= float(max_age_seconds)
    except Exception:
        return False


def refresh_registered_financial_windows(chat_id: int):
    """Fan out current/recent finance windows after a committed finance change.

    Same Telegram message is always serialized by one pool key; different messages in the same
    chat are allowed to refresh concurrently. This is the safe maximum parallelism for UI.
    """
    chat_id = int(chat_id)
    store = get_chat_store(chat_id)
    submitted_messages = set()

    def _submit_main(day_value, message_value):
        try:
            mid_i = int(message_value or 0)
            if not mid_i or mid_i in submitted_messages:
                return
            submitted_messages.add(mid_i)
            _v166_fin_submit(f"msg:{chat_id}:{mid_i}", _v166_refresh_main_one, chat_id, str(day_value or today_key()), mid_i)
        except Exception:
            pass

    # Active pointer(s) always refresh immediately.
    active_mids = set()
    for day_value, mid in list((get_or_create_active_windows(chat_id) or {}).items()):
        try:
            if int(mid or 0): active_mids.add(int(mid))
        except Exception: pass
        _submit_main(day_value, mid)

    # v168: old parallel windows remain valid when clicked, but only recent windows auto-refresh.
    # This prevents one transaction from generating dozens of Telegram edits after many versions/windows.
    registry_snapshot = list((_open_window_registry() or {}).items())
    for _key, item in registry_snapshot:
        try:
            if str((item or {}).get("window_type") or "") != "main_day":
                continue
            host = int((item or {}).get("chat_id") or (item or {}).get("host_chat_id") or 0)
            if host != chat_id:
                continue
            mid = int((item or {}).get("message_id") or 0)
            if mid not in active_mids and not _v168_fin_window_is_recent(item):
                continue
            _submit_main((item or {}).get("day_key") or store.get("current_view_day") or today_key(), mid)
        except Exception:
            continue

    rem_mid = int(store.get("remaining_msg_id") or 0)
    if rem_mid and rem_mid not in submitted_messages:
        submitted_messages.add(rem_mid)
        _v166_fin_submit(f"msg:{chat_id}:{rem_mid}", _v166_refresh_remaining, chat_id, rem_mid)

    # Categories window, if one is actually open, gets the same per-message serialization rule.
    cat_mid = int(store.get("categories_msg_id") or 0)
    if cat_mid:
        _v166_fin_submit(f"msg:{chat_id}:{cat_mid}", _refresh_categories_window_from_state, chat_id)

    # Owner/auxiliary views of this financial chat may live in another Telegram chat. Each concrete
    # host message is independent and therefore can update in parallel with the main windows.
    for _key, item in registry_snapshot:
        try:
            wtype = str((item or {}).get("window_type") or "")
            if wtype not in {"fin_view", "local_fin_view", "fin_categories_view", "stored"}:
                continue
            if not _v168_fin_window_is_recent(item):
                continue
            params = (item or {}).get("params") or {}
            if wtype == "fin_view" and int(params.get("target_chat_id") or 0) != chat_id:
                continue
            if wtype in {"local_fin_view", "fin_categories_view"}:
                target_hint = int(params.get("target_chat_id") or (item or {}).get("chat_id") or 0)
                if target_hint not in {0, chat_id}:
                    continue
            host = int((item or {}).get("chat_id") or (item or {}).get("host_chat_id") or chat_id)
            mid2 = int((item or {}).get("message_id") or 0)
            if not mid2:
                continue
            _v166_fin_submit(f"msg:{host}:{mid2}", _v166_refresh_registry_item, dict(item or {}), chat_id)
        except Exception:
            continue
    return True


def _v177_legacy_0247_schedule_financial_window_refresh(chat_id: int, day_key: str | None = None, reason: str = "finance_changed", delay: float = 0.0):
    """v166: post-commit UI refresh is dispatched immediately to independent per-message lanes.

    finance_changed() already debounces/serializes the business commit. Adding another 120-150 ms
    debounce here only made the visible balance lag, so the second debounce is removed.
    """
    chat_id = int(chat_id)
    day_key = str(day_key or get_chat_store(chat_id).get("current_view_day") or today_key())[:10]

    def _dispatch():
        try:
            # Balance/total helpers are no-ops if the corresponding window is not open.
            refresh_balance_panel_now(chat_id)
            refresh_total_message_if_any(chat_id)
            refresh_registered_financial_windows(chat_id)
            bot_journal("finance_window_refresh_parallel", chat_id, f"day={day_key} reason={reason} v166=1")
        except Exception as exc:
            try: log_error(f"v166 finance window dispatch {chat_id}: {exc}")
            except Exception: pass

    # v168: several layers can report the same mutation (insert -> reserve -> finalize). Coalesce them
    # for a few milliseconds so one transaction does not repaint the same Telegram windows 3-4 times.
    def _fire_visual():
        if not V166_FINANCE_UI_TASK_POOL.submit(f"dispatch:{chat_id}", _dispatch):
            try: log_error(f"V166 FINANCE UI DISPATCH QUEUE FULL: {chat_id}")
            except Exception: pass
    try:
        key = f"finance-visual:{chat_id}"
        V166_FINANCE_DEBOUNCE_SCHEDULER.cancel(key)
        V166_FINANCE_DEBOUNCE_SCHEDULER.schedule(key, max(0.01, min(float(delay or 0.0), 0.05)), _fire_visual)
        return True
    except Exception:
        _fire_visual()
        return True
try: _v177_legacy_0247_schedule_financial_window_refresh.__name__ = 'schedule_financial_window_refresh'
except Exception: pass
schedule_financial_window_refresh = _v177_legacy_0247_schedule_financial_window_refresh


# Faster finance finalization debounce. Actual finance writes still go through FINANCE_TASK_POOL per chat.
def finance_changed(chat_id: int, day_key: str | None = None, reason: str = "change", delay: float = 0.05):
    chat_id = int(chat_id)
    day_key = day_key or get_chat_store(chat_id).get("current_view_day") or today_key()
    try:
        requested = max(0.0, float(delay))
    except Exception:
        requested = 0.05
    # UI-oriented finance changes should settle almost immediately; restore/reset can still request 0.1 s.
    effective = min(requested, 0.10)
    bot_journal("finance_changed_scheduled", chat_id, f"day={day_key} reason={reason} delay={effective} v168=pre_refresh")
    try:
        # Visible windows must not wait for SQLite/MEGA/finalization. Per-message lanes coalesce safely.
        schedule_financial_window_refresh(chat_id, day_key, reason=f"{reason}:precommit_v168")
    except Exception as exc:
        try: log_error(f"v168 immediate finance UI {chat_id}: {exc}")
        except Exception: pass

    def _job():
        if not FINANCE_TASK_POOL.submit(chat_id, _finance_changed_now, chat_id, day_key, reason):
            try: log_error(f"FINANCE QUEUE FULL, RETRY: {chat_id}")
            except Exception: pass
            V166_FINANCE_DEBOUNCE_SCHEDULER.schedule(f"finance-finalize:{chat_id}", 0.25, _fire)

    def _fire():
        with timer_lock:
            _finalize_timers.pop(chat_id, None)
        _job()

    with timer_lock:
        _finalize_timers[chat_id] = _v166_time.time() + effective
    try:
        V166_FINANCE_DEBOUNCE_SCHEDULER.cancel(f"finance-finalize:{chat_id}")
    except Exception:
        pass
    V166_FINANCE_DEBOUNCE_SCHEDULER.schedule(f"finance-finalize:{chat_id}", effective, _fire)


def schedule_finalize(chat_id: int, day_key: str, delay: float = 0.05):
    return finance_changed(int(chat_id), str(day_key), reason="schedule_finalize", delay=min(float(delay or 0.05), 0.10))


# ---------------------------------------------------------------------------
# Restore compatibility.
# ---------------------------------------------------------------------------
if callable(_V166_PREV_RESTORE_VALIDATE):
    def _v153_validate_restore_gz(gz_path: str):
        try:
            return _V166_PREV_RESTORE_VALIDATE(gz_path)
        except Exception as exc:
            if "unsupported bot version" not in str(exc):
                raise
            folder = _v166_tempfile.mkdtemp(prefix="v166_restore_validate_")
            raw = _v166_os.path.join(folder, "restore.sqlite3")
            try:
                with _v166_gzip.open(gz_path, "rb") as fin, open(raw, "wb") as fout:
                    _v166_shutil.copyfileobj(fin, fout, 1024 * 1024)
                conn = _v166_sqlite3.connect(raw)
                try:
                    integrity = str(conn.execute("PRAGMA integrity_check").fetchone()[0])
                    if integrity.lower() != "ok":
                        raise RuntimeError(f"SQLite integrity_check: {integrity}")
                    row = conn.execute("SELECT v FROM meta WHERE kind='v153_export' AND k='manifest'").fetchone()
                    if not row:
                        raise RuntimeError("manifest v153 not found")
                    manifest = _v166_json.loads(row[0])
                finally:
                    conn.close()
                if str(manifest.get("kind")) != "telegram_bot_full_state_v153":
                    raise RuntimeError("unknown export kind")
                if int(manifest.get("schema_version") or 0) != int(V153_EXPORT_SCHEMA):
                    raise RuntimeError("unsupported export schema")
                export_version = str(manifest.get("bot_version") or "")
                if not export_version.startswith((
                    "bot_v153_", "bot_v154_", "bot_v155_", "bot_v156_", "bot_v157_", "bot_v158_",
                    "bot_v159_", "bot_v160_", "bot_v161_", "bot_v162_", "bot_v163_", "bot_v164_",
                    "bot_v165_", "bot_v166_",
                )):
                    raise RuntimeError(f"unsupported bot version: {export_version or 'missing'}")
                if _v153_db_logical_checksum(raw) != str(manifest.get("checksum") or ""):
                    raise RuntimeError("checksum mismatch")
                return manifest, raw
            except Exception:
                _v166_shutil.rmtree(folder, ignore_errors=True)
                raise


try:
    bot_journal(
        "v166_fast_parallel_forward_pairs_installed",
        int(OWNER_ID or 0),
        "pairs=raw_rules_not_v148_filter; callback_ack=0.05s; safe_ui=per_window; forward_config=per_pair; config_io=dedicated; finance_ui=per_message; finance_debounce=dedicated; finance_finalize<=0.10s",
    )
except Exception:
    pass

# v178_global_performance_final
