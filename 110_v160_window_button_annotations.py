# v160_window_button_annotations
import copy as _v160_copy
import json as _v160_json
import os as _v160_os
import re as _v160_re
import secrets as _v160_secrets
import tempfile as _v160_tempfile
import threading as _v160_threading
import time as _v160_time
from datetime import datetime as _v160_datetime

VERSION = "bot_v160_window_button_annotations"

# ---------------------------------------------------------------------------
# 1. Button reliability: v153 cached an *old Telegram result* for 120 seconds
#    using only the requested target state. If the same message changed in the
#    meantime, a later button that wanted the old state was falsely considered
#    already executed. Telegram was never called. Remove that cache layer.
# ---------------------------------------------------------------------------
try:
    if callable(globals().get("_V153_ORIG_EDIT_TEXT")):
        bot.edit_message_text = _V153_ORIG_EDIT_TEXT
    if callable(globals().get("_V153_ORIG_EDIT_MARKUP")):
        bot.edit_message_reply_markup = _V153_ORIG_EDIT_MARKUP
    if isinstance(globals().get("_V153_UI_CACHE"), dict):
        _V153_UI_CACHE.clear()
    bot_journal("v160_ui_result_cache_disabled", OWNER_ID, "v153 120s edit-result cache removed")
except Exception as _v160_exc:
    try: log_error(f"v160 disable edit cache: {_v160_exc}")
    except Exception: pass

# User button callbacks already run in a per-chat serialized UI lane. Do not
# add the background repaint throttle to those direct button edits.
_V160_PREV_EFFECTIVE_UI_INTERVAL = globals().get("effective_ui_edit_interval")
def effective_ui_edit_interval() -> float:
    try:
        if _v160_threading.current_thread().name.startswith("ui-"):
            return 0.0
    except Exception:
        pass
    if callable(_V160_PREV_EFFECTIVE_UI_INTERVAL):
        try: return max(0.0, float(_V160_PREV_EFFECTIVE_UI_INTERVAL()))
        except Exception: pass
    return 0.0

# ---------------------------------------------------------------------------
# 2. Generic process helper Ф232 is disabled. File download/export progress
#    (Ф233) is a separate mechanism and remains enabled.
# ---------------------------------------------------------------------------
def process_visual_status_enabled(chat_id: int) -> bool:
    return False

def _v156_process_status_schedule(chat_id: int, delay: float) -> None:
    try: DELAYED_SCHEDULER.cancel(f"{_V156_PROCESS_STATUS_KEY_PREFIX}{int(chat_id)}")
    except Exception: pass

def _v156_process_status_arm(chat_id: int | None, hint: str = "") -> None:
    try:
        cid = int(chat_id or 0)
        if cid:
            _v156_process_status_clear(cid, delete=True)
    except Exception:
        pass

def _v156_process_status_tick(chat_id: int) -> None:
    try: _v156_process_status_clear(int(chat_id), delete=True)
    except Exception: pass

try:
    for _v160_cid in list((globals().get("_V156_PROCESS_UI") or {}).keys()):
        try: _v156_process_status_clear(int(_v160_cid), delete=True)
        except Exception: pass
except Exception:
    pass

# ---------------------------------------------------------------------------
# 3. Reliable delayed deletion + keep transient "already queued" notifications
#    out of Ф91/stored windows.
# ---------------------------------------------------------------------------
def _v160_delete_with_retry(chat_id: int, message_id: int, attempt: int = 0):
    try:
        bot.delete_message(int(chat_id), int(message_id))
        return True
    except Exception as exc:
        if attempt < 2:
            try:
                DELAYED_SCHEDULER.schedule(
                    f"v160-delete-retry:{int(chat_id)}:{int(message_id)}:{attempt+1}",
                    2.5 + attempt * 3.0, _v160_delete_with_retry,
                    int(chat_id), int(message_id), int(attempt + 1),
                )
            except Exception:
                pass
        else:
            try: bot_journal("v160_delete_failed", int(chat_id), f"msg={message_id}; {str(exc)[:220]}", "WARN")
            except Exception: pass
        return False

def delete_message_later(chat_id: int, message_id: int, delay: float = 10):
    try:
        key = f"delete-later:{int(chat_id)}:{int(message_id)}"
        DELAYED_SCHEDULER.cancel(key)
        DELAYED_SCHEDULER.schedule(key, max(0.05, float(delay)), _v160_delete_with_retry, int(chat_id), int(message_id), 0)
    except Exception:
        pass

_V160_PREV_SEND_AUTO_DELETE = globals().get("send_and_auto_delete")
_V160_PREV_SEND_HTML_AUTO_DELETE = globals().get("send_html_and_auto_delete")

def _v160_ephemeral(chat_id: int, text: str, delay: float = 10, parse_mode=None):
    try:
        msg = bot.send_message(int(chat_id), str(text), parse_mode=parse_mode)
        mid = int(getattr(msg, "message_id", 0) or 0)
        if mid:
            delete_message_later(int(chat_id), mid, delay)
        return msg
    except Exception as exc:
        try: log_error(f"v160 ephemeral: {exc}")
        except Exception: pass
        return None

def _v160_is_queue_duplicate_notice(text: str) -> bool:
    low = str(text or "").casefold()
    return "новая копия в очередь не добавлена" in low or "копия в очередь не добавлена" in low

def send_and_auto_delete(chat_id: int, text: str, delay: int = 25):
    if _v160_is_queue_duplicate_notice(text):
        return _v160_ephemeral(chat_id, text, _v159_helper_delay(delay) if "_v159_helper_delay" in globals() else delay)
    if callable(_V160_PREV_SEND_AUTO_DELETE):
        return _V160_PREV_SEND_AUTO_DELETE(chat_id, text, delay)
    return _v160_ephemeral(chat_id, text, delay)

def send_html_and_auto_delete(chat_id: int, html_text: str, delay: int = 25):
    if _v160_is_queue_duplicate_notice(html_text):
        return _v160_ephemeral(chat_id, html_text, _v159_helper_delay(delay) if "_v159_helper_delay" in globals() else delay, "HTML")
    if callable(_V160_PREV_SEND_HTML_AUTO_DELETE):
        return _V160_PREV_SEND_HTML_AUTO_DELETE(chat_id, html_text, delay)
    return _v160_ephemeral(chat_id, html_text, delay, "HTML")

# ---------------------------------------------------------------------------
# 4. Parallel-window model. Registry cleanup no longer invalidates a perfectly
#    valid Telegram message only because another same-type window became the
#    newest/canonical pointer.
# ---------------------------------------------------------------------------
_V160_PREV_CLEANUP_WINDOWS = globals().get("cleanup_open_window_registry")
def cleanup_open_window_registry(reason: str = "manual") -> dict:
    removed = duplicates = normalized = 0
    try:
        now_dt = now_local()
        cutoff = now_dt - timedelta(days=max(7, int(globals().get("_V146_WINDOW_REGISTRY_KEEP_DAYS") or 7)))
        with _V146_WINDOW_LOCK:
            reg = _open_window_registry()
            grouped = defaultdict(list)
            for key, item in list(reg.items()):
                try:
                    cid = int((item or {}).get("chat_id") or 0); mid = int((item or {}).get("message_id") or 0)
                    if not cid or not mid:
                        reg.pop(key, None); removed += 1; continue
                    grouped[(cid, mid)].append((key, item))
                except Exception:
                    reg.pop(key, None); removed += 1
            new_reg = {}
            for (cid, mid), rows in grouped.items():
                rows.sort(key=lambda pair: (int((pair[1] or {}).get("epoch") or 0), str((pair[1] or {}).get("updated_at") or "")), reverse=True)
                key, item = rows[0]
                duplicates += max(0, len(rows) - 1)
                # Only age out old passive static views. Main/remaining/stored
                # windows are retained so buttons in old parallel messages work.
                keep = True
                wtype = str((item or {}).get("window_type") or "")
                try:
                    upd = _v160_datetime.fromisoformat(str((item or {}).get("updated_at") or ""))
                    if upd.tzinfo is None: upd = upd.replace(tzinfo=now_dt.tzinfo)
                    if upd < cutoff and wtype in {"static_view", "fin_view", "local_fin_view", "fin_categories_view", "categories"}:
                        keep = False
                except Exception:
                    pass
                if keep:
                    canonical = _v146_registry_key(cid, mid)
                    item["epoch"] = max(1, int((item or {}).get("epoch") or 1))
                    new_reg[canonical] = item
                    if canonical != key: normalized += 1
                else:
                    removed += 1
            if duplicates or removed or normalized or len(reg) != len(new_reg):
                reg.clear(); reg.update(new_reg)
                try: save_data(data, root_only=True)
                except Exception: pass
        out = {"reason": reason, "kept": len(_open_window_registry() or {}), "removed": removed,
               "duplicates_removed": duplicates, "f91_removed": 0, "keys_normalized": normalized,
               "parallel_windows": True}
        try: bot_journal("window_registry_cleanup", None, _v160_json.dumps(out, ensure_ascii=False))
        except Exception: pass
        return out
    except Exception as exc:
        try: bot_journal("v160_window_cleanup_fallback", None, str(exc)[:240], "WARN")
        except Exception: pass
        if callable(_V160_PREV_CLEANUP_WINDOWS):
            return _V160_PREV_CLEANUP_WINDOWS(reason)
        return {"reason": reason, "error": str(exc)[:200]}

# Back to main edits the window whose button was pressed and does NOT delete
# another main window. The newest interacted one merely becomes canonical.
def return_to_main_window_closing_previous(chat_id: int, day_key: str, current_message_id: int | None = None):
    chat_id = int(chat_id); day_key = str(day_key)[:10]
    try: current_mid = int(current_message_id or 0)
    except Exception: current_mid = 0
    try:
        if current_mid:
            cancel_auto_delete_for_message(chat_id, current_mid)
            cancel_fast_ui_edit(chat_id, current_mid)
    except Exception: pass
    txt, _ = render_day_window(chat_id, day_key)
    kb = build_main_keyboard(day_key, chat_id)
    if current_mid:
        result = fast_ui_edit_message_text(chat_id, current_mid, txt, reply_markup=kb, parse_mode="HTML", purpose="back_main_instant")
        try: bot_journal("back_main_parallel", chat_id, f"day={day_key}; current={current_mid}; result={result}")
        except Exception: pass
        if result == "ok":
            try: set_active_window_id(chat_id, day_key, current_mid)
            except Exception: pass
            try: register_open_window(chat_id, current_mid, "main_day", "О1", day_key, {})
            except Exception: pass
            try: schedule_balance_panel_refresh(chat_id, 0.08)
            except Exception: pass
            return True
        if result == "not_found":
            try: unregister_open_window(chat_id, current_mid)
            except Exception: pass
    try:
        active = int(get_active_window_id(chat_id, day_key) or 0)
    except Exception:
        active = 0
    if active:
        try:
            result = fast_ui_edit_message_text(chat_id, active, txt, reply_markup=kb, parse_mode="HTML", purpose="back_main_existing")
            if result == "ok": return True
        except Exception: pass
    try:
        sent = bot.send_message(chat_id, txt, reply_markup=kb, parse_mode="HTML")
        mid = int(getattr(sent, "message_id", 0) or 0)
        if mid:
            try: set_active_window_id(chat_id, day_key, mid)
            except Exception: pass
            try: register_open_window(chat_id, mid, "main_day", "О1", day_key, {})
            except Exception: pass
        return True
    except Exception as exc:
        try: log_error(f"v160 back main: {exc}")
        except Exception: pass
        return False

# Generic "Back": do not pop history until Telegram confirms the edit. Never
# report success merely because an edit was queued/rate-limited.
def restore_previous_window(call) -> bool:
    chat_id = int(call.message.chat.id); message_id = int(call.message.message_id)
    key = _window_nav_key(chat_id, message_id)
    with _WINDOW_NAV_HISTORY_LOCK:
        stack = _WINDOW_NAV_HISTORY.get(key) or []
        snap = dict(stack[-1]) if stack else None
    if snap:
        markup = _deserialize_inline_keyboard(snap.get("markup"))
        try:
            markup = ensure_previous_back_nav_keyboard(markup, chat_id, message_id)
            markup = ensure_main_back_nav_keyboard(markup, chat_id)
        except Exception: pass
        result = fast_ui_edit_message_text(chat_id, message_id, str(snap.get("text") or ""), reply_markup=markup,
                                           parse_mode=snap.get("parse_mode"), purpose="nav_prev_restore")
        if result == "ok":
            with _WINDOW_NAV_HISTORY_LOCK:
                stack = _WINDOW_NAV_HISTORY.get(key) or []
                if stack: stack.pop()
                if not stack: _WINDOW_NAV_HISTORY.pop(key, None)
            return True
        if result == "not_found":
            try: unregister_open_window(chat_id, message_id)
            except Exception: pass
    # Lost history after restart/stale message: make the pressed window useful
    # again instead of silently doing nothing.
    try:
        day_key = str((get_registered_open_window(chat_id, message_id) or {}).get("day_key") or today_key())
    except Exception:
        try: day_key = today_key()
        except Exception: day_key = now_local().strftime("%Y-%m-%d")
    return bool(return_to_main_window_closing_previous(chat_id, day_key, current_message_id=message_id))

# Known missing markers seen in v159 logs.
try:
    WINDOW_MARKER_CONSTANTS.update({
        "v149:rem:merge:*": "Ф191",
        "v149:rem:command:*": "Ф191",
        "v149:rem:history": "Ф236",
        "v160:download:markers": "Ф233",
        "v160:download:tz": "Ф233",
    })
except Exception:
    pass

# ---------------------------------------------------------------------------
# 5. Exact window identification /iz-mr + /tz.
# ---------------------------------------------------------------------------
_V160_ANN_LOCK = _v160_threading.RLock()
_V160_TOKEN_BY_MESSAGE = {}
_V160_HELPER_TEXTS = {"/iz-mr", "/tz"}
_V160_MARKER_RE = _v160_re.compile(r"(?mi)^\s*([СФП]\d{1,6}|[ОOВB]\d{1,4})(?:\s*[⏳⏰])?\s*$")

def _v160_ann_root() -> dict:
    with _V160_ANN_LOCK:
        root = data.setdefault("window_annotations_v160", {})
        if not isinstance(root, dict):
            root = {}; data["window_annotations_v160"] = root
        root.setdefault("schema", 1); root.setdefault("windows", {}); root.setdefault("markers", {}); root.setdefault("tz", [])
        return root

def _v160_new_token() -> str:
    return "w" + _v160_secrets.token_hex(5)

def _v160_marker(text: str) -> str:
    rows = _V160_MARKER_RE.findall(str(text or ""))
    return str(rows[-1]).upper() if rows else ""

def _v160_callbacks(markup) -> list:
    out = []
    try:
        for row in list(getattr(markup, "keyboard", None) or []):
            for b in row or []:
                cb = str(getattr(b, "callback_data", "") or "")
                if cb:
                    try: cb = resolve_short_callback(cb) or cb
                    except Exception: pass
                    out.append(cb)
    except Exception: pass
    return out[:80]

def _v160_is_journal_menu(markup) -> bool:
    cbs = _v160_callbacks(markup)
    return any(x in cbs for x in ("journal_file", "journal_current_file", "ops_journal_file", "diagnostic_journal_file"))

def _v160_get_token(chat_id: int | None, message_id: int | None, preferred: str = "") -> str:
    key = None
    try:
        if chat_id is not None and message_id is not None:
            key = (int(chat_id), int(message_id))
    except Exception: key = None
    with _V160_ANN_LOCK:
        if key and _V160_TOKEN_BY_MESSAGE.get(key): return _V160_TOKEN_BY_MESSAGE[key]
        token = str(preferred or "") or _v160_new_token()
        if key: _V160_TOKEN_BY_MESSAGE[key] = token
        return token


def _v160_current_callback() -> str:
    try:
        ctx = _current_telegram_update_context() if callable(globals().get("_current_telegram_update_context")) else {}
        return str((ctx or {}).get("callback_data") or "")
    except Exception:
        return ""

def _v160_toggle_callback(callback: str) -> bool:
    low = str(callback or "").casefold()
    return any(x in low for x in ("toggle", "v149:rem:merge:", "v149:rem:command:", "itmr_digit:", "itmr_unit:"))

def _v160_token_for_edit(chat_id, message_id, marker: str, preferred: str = "") -> str:
    try: key = (int(chat_id), int(message_id))
    except Exception: return _v160_get_token(chat_id, message_id, preferred)
    callback = _v160_current_callback()
    with _V160_ANN_LOCK:
        old_token = _V160_TOKEN_BY_MESSAGE.get(key) or str(preferred or "")
        old_row = ((_v160_ann_root().get("windows") or {}).get(old_token) or {}) if old_token else {}
        old_marker = str(old_row.get("marker") or "")
        old_callback = str(old_row.get("last_callback") or "")
        # A real navigation click creates a new exact logical-window identity.
        # Pure switch/toggle clicks intentionally keep the same identity.
        rotate = False
        if old_token:
            if marker and old_marker and marker != old_marker:
                rotate = not _v160_toggle_callback(callback)
            elif callback and not _v160_toggle_callback(callback) and callback != old_callback:
                rotate = True
        if not old_token or rotate:
            old_token = _v160_new_token()
            _V160_TOKEN_BY_MESSAGE[key] = old_token
        return old_token

def _v160_decorate_markup(markup, chat_id: int | None, marker: str, token: str):
    if not marker:
        return markup
    try:
        kb = _v160_copy.deepcopy(markup) if markup is not None else types.InlineKeyboardMarkup()
    except Exception:
        kb = markup if markup is not None else types.InlineKeyboardMarkup()
    try:
        rows = list(getattr(kb, "keyboard", None) or [])
        clean = []
        for row in rows:
            filtered = [b for b in (row or []) if str(getattr(b, "text", "") or "").strip() not in _V160_HELPER_TEXTS
                        and str(getattr(b, "callback_data", "") or "") not in {"v160:download:markers", "v160:download:tz"}]
            if filtered: clean.append(filtered)
        kb.keyboard = clean
        if _v160_is_journal_menu(markup):
            kb.row(IB("🏷 Скачать маркировки", callback_data="v160:download:markers"),
                   IB("📝 Скачать ТЗ окон", callback_data="v160:download:tz"))
        kb.row(
            make_copy_or_inline_button("/iz-mr", f"/iz-mr {marker} {token} ", viewer_chat_id=chat_id),
            make_copy_or_inline_button("/tz", f"/tz {marker} {token} ", viewer_chat_id=chat_id),
        )
        try: setattr(kb, "_v160_window_token", token)
        except Exception: pass
        return kb
    except Exception:
        return markup

def _v160_now_iso():
    try: return now_local().isoformat(timespec="seconds")
    except Exception: return _v160_datetime.now().isoformat(timespec="seconds")

def _v160_capture(token: str, chat_id, message_id, marker: str, text: str, markup, purpose: str = ""):
    if not token or not marker: return
    try: cid = int(chat_id or 0)
    except Exception: cid = 0
    try: mid = int(message_id or 0)
    except Exception: mid = 0
    reg = None
    try: reg = get_registered_open_window(cid, mid) if cid and mid else None
    except Exception: reg = None
    callbacks = _v160_callbacks(markup)
    try:
        ctx = _current_telegram_update_context() if callable(globals().get("_current_telegram_update_context")) else {}
    except Exception: ctx = {}
    first = ""
    try:
        body = strip_window_mark(str(text or "")).strip()
        first = next((x.strip() for x in body.splitlines() if x.strip()), "")[:240]
    except Exception: pass
    now_s = _v160_now_iso()
    with _V160_ANN_LOCK:
        root = _v160_ann_root(); windows = root["windows"]
        old = windows.get(token) if isinstance(windows.get(token), dict) else {}
        row = dict(old or {})
        row.update({
            "token": token, "marker": marker, "chat_id": cid, "message_id": mid,
            "title": first or str(row.get("title") or ""), "purpose": str(purpose or row.get("purpose") or "")[:160],
            "last_callback": str((ctx or {}).get("callback_data") or (callbacks[0] if callbacks else ""))[:220],
            "callbacks": callbacks, "window_type": str((reg or {}).get("window_type") or row.get("window_type") or ""),
            "code": str((reg or {}).get("code") or row.get("code") or ""),
            "day_key": (reg or {}).get("day_key") or row.get("day_key"),
            "params": (reg or {}).get("params") or row.get("params") or {},
            "version": VERSION, "updated_at": now_s,
        })
        row.setdefault("created_at", now_s); row.setdefault("user_name", "")
        windows[token] = row
        if cid and mid: _V160_TOKEN_BY_MESSAGE[(cid, mid)] = token
        mrow = root["markers"].setdefault(marker, {"marker": marker, "name": "", "first_seen": now_s, "windows": []})
        mrow["last_seen"] = now_s
        ids = list(mrow.get("windows") or [])
        if token not in ids: ids.append(token)
        mrow["windows"] = ids[-80:]
        # Bound persistent diagnostics size.
        if len(windows) > 1200:
            ordered = sorted(windows.items(), key=lambda kv: str((kv[1] or {}).get("updated_at") or ""))
            for old_token, _ in ordered[:len(windows)-1000]: windows.pop(old_token, None)
        if len(root["tz"]) > 800: del root["tz"][:-700]

def _v160_bind_sent(token: str, chat_id, result, marker: str, text: str, markup, purpose="send_message"):
    try: mid = int(getattr(result, "message_id", 0) or 0)
    except Exception: mid = 0
    if mid:
        _v160_capture(token, chat_id, mid, marker, text, markup, purpose)
    return result

def _v160_ensure_marker_for_action(chat_id, text: str, markup=None, parse_mode=None):
    marker = _v160_marker(text)
    if marker:
        return str(text), marker
    code = ""
    callback = _v160_current_callback()
    try:
        if callback and callable(globals().get("window_code_for_callback")):
            code = str(window_code_for_callback(callback, owner_chat=is_owner_chat(int(chat_id))) or "")
    except Exception:
        code = ""
    if not code or code.endswith("9998"):
        try:
            if markup is not None:
                key = _window_key_from_markup(markup)
                code = str(_window_marker_code(key) or "")
        except Exception:
            code = ""
    if code and not code.endswith("9998"):
        try:
            text = window_mark(str(text), code, html_mode=(str(parse_mode or "").upper() == "HTML"))
            marker = _v160_marker(text) or code
        except Exception:
            pass
    return str(text), str(marker or "")

# Save base methods *after* the bad v153 cache layer was removed.
_V160_BASE_SEND_MESSAGE = getattr(bot, "send_message", None)
_V160_BASE_EDIT_TEXT = getattr(bot, "edit_message_text", None)
_V160_BASE_EDIT_MARKUP = getattr(bot, "edit_message_reply_markup", None)

if callable(_V160_BASE_SEND_MESSAGE):
    def _v160_send_message(chat_id, text, *args, **kwargs):
        markup = kwargs.get("reply_markup")
        text, marker = _v160_ensure_marker_for_action(chat_id, text, markup, kwargs.get("parse_mode"))
        if marker:
            token = _v160_get_token(None, None, str(getattr(markup, "_v160_window_token", "") or ""))
            markup = _v160_decorate_markup(markup, int(chat_id), marker, token)
            kwargs["reply_markup"] = markup
            result = _V160_BASE_SEND_MESSAGE(chat_id, text, *args, **kwargs)
            return _v160_bind_sent(token, chat_id, result, marker, text, markup)
        return _V160_BASE_SEND_MESSAGE(chat_id, text, *args, **kwargs)
    bot.send_message = _v160_send_message

if callable(_V160_BASE_EDIT_TEXT):
    def _v160_edit_message_text(text, chat_id=None, message_id=None, *args, **kwargs):
        markup = kwargs.get("reply_markup")
        text, marker = _v160_ensure_marker_for_action(chat_id, text, markup, kwargs.get("parse_mode"))
        if marker:
            token = _v160_token_for_edit(chat_id, message_id, marker, str(getattr(markup, "_v160_window_token", "") or ""))
            markup = _v160_decorate_markup(markup, int(chat_id) if chat_id is not None else None, marker, token)
            kwargs["reply_markup"] = markup
            result = _V160_BASE_EDIT_TEXT(text, chat_id=chat_id, message_id=message_id, *args, **kwargs)
            _v160_capture(token, chat_id, message_id, marker, text, markup, "edit_message_text")
            return result
        return _V160_BASE_EDIT_TEXT(text, chat_id=chat_id, message_id=message_id, *args, **kwargs)
    bot.edit_message_text = _v160_edit_message_text

if callable(_V160_BASE_EDIT_MARKUP):
    def _v160_edit_message_reply_markup(chat_id=None, message_id=None, *args, **kwargs):
        marker = ""; token = ""
        try:
            token = _v160_get_token(chat_id, message_id)
            row = (_v160_ann_root().get("windows") or {}).get(token) or {}
            marker = str(row.get("marker") or "")
            if not marker and callable(globals().get("_window_diag_state_get")):
                marker = str((_window_diag_state_get(int(chat_id), int(message_id)) or {}).get("marker") or "")
        except Exception: pass
        if marker:
            kwargs["reply_markup"] = _v160_decorate_markup(kwargs.get("reply_markup"), int(chat_id), marker, token)
        return _V160_BASE_EDIT_MARKUP(chat_id=chat_id, message_id=message_id, *args, **kwargs)
    bot.edit_message_reply_markup = _v160_edit_message_reply_markup

# Persist user-entered names/TZ immediately; passive captures are included in
# normal data/MEGA persistence without making every button wait for disk/cloud.
def _v160_persist_annotations(reason="window_annotations"):
    try: save_data(data, root_only=True)
    except Exception as exc:
        try: log_error(f"v160 annotations save: {exc}")
        except Exception: pass
    try: schedule_config_backup_for_chats(int(OWNER_ID), delay=0.5, reason=str(reason))
    except Exception: pass

def _v160_actor_allowed(msg) -> bool:
    try:
        uid = int(msg.from_user.id)
        if uid == int(OWNER_ID): return True
        fn = globals().get("get_additional_owner_ids")
        return uid in set(int(x) for x in (fn() if callable(fn) else []))
    except Exception: return False

def _v160_resolve_window(marker: str, token: str, chat_id: int) -> tuple[str, dict]:
    root = _v160_ann_root(); windows = root.get("windows") or {}
    if token and isinstance(windows.get(token), dict): return token, windows[token]
    candidates = [(t, r) for t, r in windows.items() if isinstance(r, dict) and str(r.get("marker") or "").upper() == marker.upper()]
    same = [(t, r) for t, r in candidates if int(r.get("chat_id") or 0) == int(chat_id)]
    pool = same or candidates
    if not pool: return "", {}
    pool.sort(key=lambda x: str((x[1] or {}).get("updated_at") or ""), reverse=True)
    return pool[0]

def _v160_parse_annotation(text: str):
    try:
        text = sanitize_telegram_inserted_text(str(text or ""))
    except Exception: text = str(text or "")
    m = _v160_re.search(r"(?is)(?:^|\s)/(iz-mr|iz_mr|tz)\s+([СФПОOВB]\d{1,6})(?:\s+(w[0-9a-f]{6,24}))?(?:\s+(.*))?$", text.strip())
    if not m: return None
    return m.group(1).lower(), m.group(2).upper(), str(m.group(3) or ""), str(m.group(4) or "").strip()

def _v160_handle_annotation(msg) -> bool:
    parsed = _v160_parse_annotation(getattr(msg, "text", ""))
    if not parsed: return False
    chat_id = int(msg.chat.id)
    if not _v160_actor_allowed(msg):
        _v160_ephemeral(chat_id, "⛔ Команды маркировки доступны владельцу бота.", 8)
        return True
    cmd, marker, token, body = parsed
    token, row = _v160_resolve_window(marker, token, chat_id)
    if not row:
        _v160_ephemeral(chat_id, f"⚠️ Не нашёл точное окно {marker}. Откройте окно заново и нажмите /iz-mr или /tz.", 10)
        return True
    if not body:
        what = "имя окна" if cmd in {"iz-mr", "iz_mr"} else "текст ТЗ"
        _v160_ephemeral(chat_id, f"✍️ Допишите {what} после вставленного маркера и отправьте сообщение.", 10)
        return True
    now_s = _v160_now_iso()
    with _V160_ANN_LOCK:
        root = _v160_ann_root(); current = root["windows"].setdefault(token, row)
        if cmd in {"iz-mr", "iz_mr"}:
            current["user_name"] = body[:500]; current["updated_at"] = now_s
            mrow = root["markers"].setdefault(marker, {"marker": marker, "windows": []})
            mrow["name"] = body[:500]; mrow["last_named_at"] = now_s
        else:
            entry = {"at": now_s, "marker": marker, "token": token, "chat_id": int(current.get("chat_id") or chat_id),
                     "message_id": int(current.get("message_id") or 0), "window_name": str(current.get("user_name") or ""),
                     "title": str(current.get("title") or ""), "purpose": str(current.get("purpose") or ""),
                     "last_callback": str(current.get("last_callback") or ""), "text": body[:12000], "version": VERSION}
            root["tz"].append(entry)
            current["last_tz_at"] = now_s
    _v160_persist_annotations("window_annotation_user_input")
    if cmd in {"iz-mr", "iz_mr"}:
        _v160_ephemeral(chat_id, f"✅ {marker}: имя окна сохранено — {body[:180]}", 8)
    else:
        _v160_ephemeral(chat_id, f"✅ ТЗ привязано к {marker} · {token}.", 8)
    return True

@bot.message_handler(func=lambda m: bool(getattr(m, "text", None)) and bool(_v160_parse_annotation(getattr(m, "text", ""))))
def v160_window_annotation_message(msg):
    _v160_handle_annotation(msg)

# ---------------------------------------------------------------------------
# 6. Exports in Journal menu.
# ---------------------------------------------------------------------------
def _v160_export_path(kind: str) -> str:
    root = _v160_ann_root(); ts = now_local().strftime("%Y%m%d_%H%M%S") if "now_local" in globals() else _v160_datetime.now().strftime("%Y%m%d_%H%M%S")
    base = globals().get("MEGA_LOCAL_TMP_DIR") or _v160_tempfile.gettempdir()
    _v160_os.makedirs(base, exist_ok=True)
    if kind == "markers":
        path = _v160_os.path.join(base, f"window_markers_{ts}.json")
        payload = {"generated_at": _v160_now_iso(), "bot_version": VERSION, "markers": root.get("markers") or {}, "windows": root.get("windows") or {}}
        with open(path, "w", encoding="utf-8") as fh: _v160_json.dump(payload, fh, ensure_ascii=False, indent=2, default=str)
        return path
    path = _v160_os.path.join(base, f"window_tz_{ts}.txt")
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(f"ТЗ ОКОН\nВерсия: {VERSION}\nСоздан: {_v160_now_iso()}\n\n")
        rows = list(root.get("tz") or [])
        if not rows: fh.write("Записей пока нет.\n")
        for i, row in enumerate(rows, 1):
            fh.write(f"=== {i}. {row.get('marker','')} · {row.get('token','')} ===\n")
            fh.write(f"Окно: {row.get('window_name') or row.get('title') or '-'}\n")
            fh.write(f"chat/message: {row.get('chat_id')}/{row.get('message_id')}\n")
            fh.write(f"callback: {row.get('last_callback') or '-'}\n")
            fh.write(f"Дата: {row.get('at') or '-'}\n")
            fh.write(str(row.get("text") or "") + "\n\n")
    return path

def _v160_send_export_sync(chat_id: int, kind: str):
    path = _v160_export_path(kind)
    try:
        with open(path, "rb") as fh:
            _tg_call_retry(bot.send_document, int(chat_id), fh, attempts=2, purpose=f"v160_{kind}_export")
        return True
    finally:
        try: _v160_os.remove(path)
        except Exception: pass

def _v160_submit_export(chat_id: int, kind: str):
    label = "Маркировки окон" if kind == "markers" else "ТЗ окон"
    ok, info = submit_interactive_file_job(int(chat_id), f"v160_{kind}", label, _v160_send_export_sync, int(chat_id), kind)
    if not ok:
        send_and_auto_delete(int(chat_id), f"⏳ {info}. Новая копия в очередь не добавлена.", 10)
    return ok

_V160_PREV_EXTENSION_CALLBACK = globals().get("v149_extension_callback")
def v149_extension_callback(call, data_str: str) -> bool:
    data_str = str(data_str or "")
    if data_str in {"v160:download:markers", "v160:download:tz"}:
        try:
            if int(call.from_user.id) != int(OWNER_ID) and not _v160_actor_allowed(call):
                bot.answer_callback_query(call.id, "Только для владельца", show_alert=True); return True
        except Exception:
            pass
        kind = "markers" if data_str.endswith("markers") else "tz"
        try: bot.answer_callback_query(call.id, "Готовлю файл…", show_alert=False)
        except Exception: pass
        _v160_submit_export(int(call.message.chat.id), kind)
        return True
    if callable(_V160_PREV_EXTENSION_CALLBACK):
        return bool(_V160_PREV_EXTENSION_CALLBACK(call, data_str))
    return False

# The helper that checks permissions accepts any object with from_user.id.
# CallbackQuery has it as well; avoid attribute errors in the common function.
_v160_old_actor_allowed = _v160_actor_allowed
def _v160_actor_allowed(obj) -> bool:
    try:
        uid = int(obj.from_user.id)
        if uid == int(OWNER_ID): return True
        fn = globals().get("get_additional_owner_ids")
        return uid in set(int(x) for x in (fn() if callable(fn) else []))
    except Exception: return False

# ---------------------------------------------------------------------------
# 7. Restore-package validation accepts v160 snapshots as well.
# ---------------------------------------------------------------------------
def _v153_validate_restore_gz(gz_path: str) -> tuple[dict, str]:
    folder = _v159_tempfile.mkdtemp(prefix="v160_restore_validate_")
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
            "bot_v153_", "bot_v154_", "bot_v155_", "bot_v156_", "bot_v157_", "bot_v158_", "bot_v159_", "bot_v160_",
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
    bot_journal("v160_window_button_annotations_installed", OWNER_ID,
                "generic_process=off; file_progress=on; ui_cache=off; parallel_windows=on; helpers=/iz-mr,/tz")
except Exception:
    pass
# v160_window_button_annotations
