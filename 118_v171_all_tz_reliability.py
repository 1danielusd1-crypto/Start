# v178_global_performance_final
"""v171: implement all open v169 window TZ items and reliability fixes.

The patch is intentionally loaded last.  It repairs active runtime hooks instead of
editing old historical implementations, so one source of truth wins after module load.
"""
import copy as _v171_copy
import gzip as _v171_gzip
import hashlib as _v171_hashlib
import json as _v171_json
import os as _v171_os
import re as _v171_re
import shutil as _v171_shutil
import sqlite3 as _v171_sqlite3
import tempfile as _v171_tempfile
import threading as _v171_threading
import time as _v171_time

VERSION = "bot_v171_all_tz_reliability"
V171_FILE_MARKER = "v171_all_tz_reliability"

# ---------------------------------------------------------------------------
# Project-wide TZ contract.
# ---------------------------------------------------------------------------
V171_TZ_SCOPE_POLICY = "global_all_contours_if_unspecified"
V171_TZ_SCOPE_TEXT = (
    "ТЗ без явно указанного чата, направления или контура применяется ко всему боту: "
    "ко всем направлениям и всем контурам."
)
try:
    _v171_gs = data.setdefault("_global_settings", {})
    _v171_gs["tz_scope_policy_v171"] = V171_TZ_SCOPE_POLICY
    _v171_gs["tz_scope_policy_text_v171"] = V171_TZ_SCOPE_TEXT
except Exception:
    pass

# ---------------------------------------------------------------------------
# 1) F233 file-status window: v163 calls a name that never existed.
# ---------------------------------------------------------------------------
def _v161_schedule_delete(chat_id: int, message_id: int, delay: float, prefix: str = "delete") -> None:
    fn = globals().get("_v160_schedule_delete")
    if callable(fn):
        fn(int(chat_id), int(message_id), float(delay), str(prefix or "delete"))
        return
    scheduler = globals().get("DELAYED_SCHEDULER")
    if scheduler is not None:
        scheduler.schedule(
            f"v171:{prefix}:{int(chat_id)}:{int(message_id)}",
            max(0.0, float(delay or 0.0)),
            lambda: _v171_delete_message_quiet(int(chat_id), int(message_id)),
        )


def _v171_delete_message_quiet(chat_id: int, message_id: int) -> None:
    try:
        bot.delete_message(int(chat_id), int(message_id))
    except Exception:
        pass
    try:
        unregister_open_window(int(chat_id), int(message_id))
    except Exception:
        pass


# ---------------------------------------------------------------------------
# 2/6) 💰 financial-forward edit mode is genuinely GLOBAL.
# v148 redefined v124's global functions later in load order, which made the
# source chat's tenant decide the initial forwarded-copy UI.
# ---------------------------------------------------------------------------
V171_FORWARD_COPY_EDIT_MODES = ("normal", "button", "slash")


def _v171_forward_mode_root() -> dict:
    try:
        return data.setdefault("_global_settings", {})
    except Exception:
        return {}


def forward_copy_edit_mode(chat_id: int | None = None) -> str:
    gs = _v171_forward_mode_root()
    mode = str(gs.get("forward_copy_edit_mode_global") or "").strip().lower()
    if mode not in V171_FORWARD_COPY_EDIT_MODES:
        # Import the most recent old choice once, then never scope it by source chat again.
        candidates = [str(gs.get("forward_copy_edit_mode") or "").strip().lower()]
        try:
            owner = int(OWNER_ID or 0)
            if owner:
                candidates.append(str(owner_scoped_settings(owner).get("forward_copy_edit_mode") or "").strip().lower())
                candidates.append(str(get_chat_store(owner).setdefault("settings", {}).get("forward_copy_edit_mode") or "").strip().lower())
        except Exception:
            pass
        mode = next((x for x in candidates if x in V171_FORWARD_COPY_EDIT_MODES), "normal")
        gs["forward_copy_edit_mode_global"] = mode
        gs["forward_copy_edit_mode"] = mode
    try:
        if not version_mode_feature("forward_copy_edit"):
            return "normal"
    except Exception:
        pass
    return mode if mode in V171_FORWARD_COPY_EDIT_MODES else "normal"


def set_forward_copy_edit_mode(chat_id: int, mode: str):
    mode = str(mode or "normal").strip().lower()
    if mode not in V171_FORWARD_COPY_EDIT_MODES:
        mode = "normal"
    gs = _v171_forward_mode_root()
    gs["forward_copy_edit_mode_global"] = mode
    gs["forward_copy_edit_mode"] = mode
    # Compatibility mirrors only; runtime reads the global value above.
    try:
        for cid in collect_all_known_chat_ids(include_owner=True):
            try:
                get_chat_store(int(cid)).setdefault("settings", {})["forward_copy_edit_mode"] = mode
            except Exception:
                pass
    except Exception:
        pass
    try:
        scheduler = globals().get("V166_CONFIG_IO_SCHEDULER")
        if scheduler is not None:
            scheduler.schedule("v171-forward-mode", 0.05, lambda: save_data(data, root_only=True))
        else:
            save_data(data, root_only=True)
    except Exception:
        pass
    try:
        schedule_config_backup_for_chats(delay=1.0)
    except Exception:
        pass
    return mode


def cycle_forward_copy_edit_mode(chat_id: int) -> str:
    current = forward_copy_edit_mode(chat_id)
    try:
        idx = V171_FORWARD_COPY_EDIT_MODES.index(current)
    except ValueError:
        idx = 0
    return set_forward_copy_edit_mode(int(chat_id), V171_FORWARD_COPY_EDIT_MODES[(idx + 1) % len(V171_FORWARD_COPY_EDIT_MODES)])


def forward_copy_edit_mode_label(chat_id: int) -> str:
    return {
        "normal": "💰фин.пересылка: обычно",
        "button": "💰фин.пересылка: кнопка",
        "slash": "💰фин.пересылка: слеш",
    }.get(forward_copy_edit_mode(chat_id), "💰фин.пересылка: обычно")


# ---------------------------------------------------------------------------
# 5) Reminder cross-chat delivery + global tri-state merge mode.
# Platform-owner reminders may target any explicitly selected known chat, while
# non-platform spaces remain isolated.
# ---------------------------------------------------------------------------
_V171_PREV_REMINDER_CHAT_ALLOWED = globals().get("_v149_reminder_chat_allowed")


def _v177_legacy_0265_v149_reminder_chat_allowed(cfg: dict, chat_id: int) -> bool:
    cid = int(chat_id)
    try:
        selected = {int(x) for x in ((cfg or {}).get("chat_ids") or [])}
    except Exception:
        selected = set()
    if cid not in selected:
        return False
    try:
        tid = str(_v149_reminder_cfg_tenant(cfg) or TENANT_PLATFORM_ID)
    except Exception:
        tid = str(globals().get("TENANT_PLATFORM_ID") or "platform")
    platform_id = str(globals().get("TENANT_PLATFORM_ID") or "platform")
    if tid == platform_id:
        try:
            known = {int(x) for x in collect_all_known_chat_ids(include_owner=True)}
            return cid in known
        except Exception:
            return False
    try:
        return bool(_v149_chat_belongs_to_tenant(cid, tid))
    except Exception:
        return bool(_V171_PREV_REMINDER_CHAT_ALLOWED(cfg, cid)) if callable(_V171_PREV_REMINDER_CHAT_ALLOWED) else False
try: _v177_legacy_0265_v149_reminder_chat_allowed.__name__ = '_v149_reminder_chat_allowed'
except Exception: pass
_v149_reminder_chat_allowed = _v177_legacy_0265_v149_reminder_chat_allowed


V171_REMINDER_MERGE_MODES = ("off", "smart", "single")


def _v171_reminder_global_mode(migrate: bool = True) -> str:
    gs = data.setdefault("_global_settings", {})
    mode = str(gs.get("reminder_merge_mode_global_v171") or "").strip().lower()
    if mode not in V171_REMINDER_MERGE_MODES and migrate:
        old = "off"
        try:
            owner = int(OWNER_ID or 0)
            if owner:
                settings = _v149_reminder_chat_settings(None, owner)
                old = str(settings.get("merge_mode") or "").strip().lower()
                if old not in V171_REMINDER_MERGE_MODES:
                    old = "smart" if bool(settings.get("merge_enabled", False)) else "off"
        except Exception:
            old = "off"
        mode = old if old in V171_REMINDER_MERGE_MODES else "off"
        gs["reminder_merge_mode_global_v171"] = mode
    return mode if mode in V171_REMINDER_MERGE_MODES else "off"


def reminder_merge_mode(tenant_id: str | None = None, chat_id: int | None = None) -> str:
    return _v171_reminder_global_mode(True)


def reminder_merge_enabled(tenant_id: str | None = None, chat_id: int | None = None) -> bool:
    return reminder_merge_mode(tenant_id, chat_id) != "off"


def reminder_merge_mode_label(tenant_id: str | None = None, chat_id: int | None = None) -> str:
    return {"off": "ВЫКЛ", "smart": "ВКЛ", "single": "1 СООБЩЕНИЕ"}.get(reminder_merge_mode(tenant_id, chat_id), "ВЫКЛ")


def _v171_cycle_reminder_merge() -> str:
    current = _v171_reminder_global_mode(True)
    try:
        idx = V171_REMINDER_MERGE_MODES.index(current)
    except ValueError:
        idx = 0
    mode = V171_REMINDER_MERGE_MODES[(idx + 1) % len(V171_REMINDER_MERGE_MODES)]
    data.setdefault("_global_settings", {})["reminder_merge_mode_global_v171"] = mode
    try:
        save_data(data, root_only=True)
    except Exception:
        pass
    return mode


_V171_PREV_V149_EXTENSION_CALLBACK = globals().get("v149_extension_callback")


def v149_extension_callback(call, data_str: str) -> bool:
    raw = str(data_str or "")
    if raw.startswith("v149:rem:merge:"):
        chat_id = int(call.message.chat.id)
        user_id = int(getattr(getattr(call, "from_user", None), "id", 0) or 0)
        try:
            tid = str(tenant_id_for_chat(chat_id, create=False) or TENANT_PLATFORM_ID)
            if not tenant_can_manage(user_id, tid):
                bot.answer_callback_query(call.id, "Недостаточно прав", show_alert=True)
                return True
        except Exception:
            pass
        mode = _v171_cycle_reminder_merge()
        parts = raw.split(":")
        page = int(parts[3]) if len(parts) > 3 and str(parts[3]).isdigit() else 0
        day_key = parts[4] if len(parts) > 4 else today_key()
        try:
            reminder_text = build_reminder_list_text()
            reminder_keyboard = build_reminder_list_keyboard(day_key, page)
            safe_edit(bot, call, reminder_text, reply_markup=reminder_keyboard)
        except Exception as exc:
            try:
                log_error(f"v171 reminder merge UI: {exc}")
            except Exception:
                pass
        try:
            REMINDER_TASK_POOL.submit_unique("reminder-v149-batch", _v149_reminder_batch_job, None)
        except Exception:
            pass
        try:
            bot.answer_callback_query(call.id, f"Объединять: {reminder_merge_mode_label()}")
        except Exception:
            pass
        return True
    if callable(_V171_PREV_V149_EXTENSION_CALLBACK):
        return bool(_V171_PREV_V149_EXTENSION_CALLBACK(call, raw))
    return False


# ---------------------------------------------------------------------------
# 8/9/10) Every marked menu gets predictable navigation + Description.
# Existing button rows are preserved; missing standard controls are appended.
# ---------------------------------------------------------------------------
try:
    WINDOW_MARKER_CONSTANTS.update({
        "v171:desc": "Ф241",
        "v171:desc_close": "Ф241",
    })
except Exception:
    pass


def _v171_kb_rows(kb):
    return list(getattr(kb, "keyboard", None) or getattr(kb, "inline_keyboard", None) or [])


def _v171_btn_text(btn) -> str:
    if isinstance(btn, dict):
        return str(btn.get("text") or "")
    return str(getattr(btn, "text", "") or "")


def _v171_btn_cb(btn) -> str:
    if isinstance(btn, dict):
        return str(btn.get("callback_data") or "")
    return str(getattr(btn, "callback_data", "") or "")


def _v171_set_kb_rows(kb, rows):
    try:
        kb.keyboard = rows
        return kb
    except Exception:
        pass
    try:
        kb.inline_keyboard = rows
    except Exception:
        pass
    return kb


def _v171_markup_inventory(kb):
    rows = _v171_kb_rows(kb)
    buttons = [btn for row in rows for btn in (row or [])]
    return rows, {_v171_btn_cb(b) for b in buttons if _v171_btn_cb(b)}, [(_v171_btn_text(b), _v171_btn_cb(b)) for b in buttons]


_V171_PREV_AUGMENT_MARKUP = globals().get("_v160_augment_markup")


def _v160_augment_markup(reply_markup, text: str):
    kb = _V171_PREV_AUGMENT_MARKUP(reply_markup, text) if callable(_V171_PREV_AUGMENT_MARKUP) else reply_markup
    marker = ""
    try:
        marker = str(_v160_marker_from_text(text) or "").upper()
    except Exception:
        marker = ""
    if not marker:
        return kb
    try:
        if not isinstance(kb, types.InlineKeyboardMarkup):
            kb = types.InlineKeyboardMarkup()
        # Description helper is already a complete helper window; do not recurse.
        if marker == "Ф241":
            return kb
        rows, callbacks, inventory = _v171_markup_inventory(kb)
        labels_cf = [str(label).strip().casefold() for label, _cb in inventory]
        has_back = any(("назад" in label and "осн" not in label) for label in labels_cf) or "nav_prev" in callbacks
        has_main = any(("назад" in label and ("осн" in label or "глав" in label)) for label in labels_cf) or any(str(cb).endswith(":back_main") for cb in callbacks)
        has_close = any("закры" in label for label in labels_cf) or bool({"info_close", "aux_close", "secclose", "secmclose"} & callbacks)
        has_desc = "v171:desc" in callbacks
        has_marker = "v160:marker_capture" in callbacks
        has_tz = "v160:tz_capture" in callbacks
        day = today_key()
        try:
            # Use the viewed chat's day so Back-to-main is deterministic.
            day = str(get_chat_store(int(getattr(_state_context, "chat_id", 0) or 0)).get("current_view_day") or today_key())
        except Exception:
            day = today_key()
        if not has_desc:
            rows.append([IB("ℹ️ Описание", callback_data="v171:desc")])
        if not has_back:
            rows.append([IB("🔙 Назад", callback_data="nav_prev")])
        if not has_main:
            rows.append([IB("⬅️ Назад осн. окно", callback_data=f"d:{day}:back_main")])
        if not has_close:
            rows.append([IB("❌ Закрыть", callback_data="info_close")])
        if not has_marker or not has_tz:
            add = []
            if not has_marker:
                add.append(IB("/iz-mr", callback_data="v160:marker_capture"))
            if not has_tz:
                add.append(IB("/tz", callback_data="v160:tz_capture"))
            if add:
                rows.append(add)
        return _v171_set_kb_rows(kb, rows)
    except Exception as exc:
        try:
            log_error(f"v171 augment markup {marker}: {exc}")
        except Exception:
            pass
        return kb


def _v171_button_help(label: str, cb: str) -> str:
    low = (str(label) + " " + str(cb)).casefold()
    rules = (
        (("журнал",), "открывает журнал или его настройки"),
        (("фин", "режим"), "управляет финансовым режимом"),
        (("фин.пересыл",), "выбирает оформление финансовой копии: обычно / кнопка / слеш"),
        (("перес",), "открывает или настраивает пересылку"),
        (("google", "чт"), "настраивает обновление Google-таблицы Чт–Ср"),
        (("excel",), "открывает настройки Excel/выгрузки"),
        (("напомин",), "открывает или меняет настройки напоминаний"),
        (("таймер",), "открывает внутренние таймеры окон и задач"),
        (("mega",), "работает с долговечным MEGA-хранилищем"),
        (("guard",), "управляет защитой восстановления/резервирования"),
        (("очеред",), "показывает состояние рабочих очередей"),
        (("render", "сервер"), "показывает состояние Render и runtime"),
        (("проблем",), "показывает задачи, требующие проверки"),
        (("целост",), "проверяет целостность финансовых данных"),
        (("владел",), "открывает управление владельцами/доступом"),
        (("назад осн",), "возвращает в основное окно"),
        (("назад",), "возвращает в предыдущее окно"),
        (("закры",), "закрывает текущее окно"),
        (("/iz-mr",), "позволяет изменить имя/маркер окна"),
        (("/tz",), "добавляет ТЗ именно к этому окну"),
    )
    for needles, text in rules:
        if all(n in low for n in needles):
            return text
    if str(cb) == "none":
        return "разделитель или информационная строка"
    return "выполняет действие этой кнопки в текущем меню"


def _v171_window_description(call) -> str:
    source_text = str(getattr(call.message, "text", None) or getattr(call.message, "caption", None) or "")
    try:
        marker = str(_v160_marker_from_text(source_text) or "без маркера").upper()
    except Exception:
        marker = "без маркера"
    name = ""
    try:
        catalog, _rows = _v160_annotation_roots()
        name = str((catalog.get(marker) or {}).get("name") or "")
    except Exception:
        name = ""
    rows = _v171_kb_rows(getattr(call.message, "reply_markup", None))
    seen = set()
    lines = [f"ℹ️ ОПИСАНИЕ ОКНА {marker}"]
    if name:
        lines.append(f"Название: {name}")
    lines += ["", "Назначение: управление функциями текущего меню.", "", "Кнопки:"]
    for row in rows:
        for btn in row or []:
            label = _v171_btn_text(btn).strip()
            cb = _v171_btn_cb(btn).strip()
            if not label or cb in {"v171:desc", "v171:desc_close"}:
                continue
            key = (label, cb)
            if key in seen:
                continue
            seen.add(key)
            line = f"• {label} — {_v171_button_help(label, cb)}"
            if len("\n".join(lines + [line])) > 3500:
                lines.append("• … остальные кнопки работают по их подписи и назначению меню.")
                break
            lines.append(line)
        if lines and lines[-1].startswith("• …"):
            break
    lines += ["", "Цепочка кнопки: нажатие → отклик → выполнение → результат."]
    try:
        return window_mark("\n".join(lines), "Ф241")
    except Exception:
        return "\n".join(lines) + "\n\nФ241"


def _v171_desc_keyboard(chat_id: int):
    day = today_key()
    try:
        day = str(get_chat_store(int(chat_id)).get("current_view_day") or today_key())
    except Exception:
        pass
    kb = types.InlineKeyboardMarkup()
    kb.row(IB("🔙 Назад", callback_data="v171:desc_close"), IB("⬅️ Назад осн. окно", callback_data=f"d:{day}:back_main"))
    kb.row(IB("❌ Закрыть", callback_data="v171:desc_close"))
    kb.row(IB("/iz-mr", callback_data="v160:marker_capture"), IB("/tz", callback_data="v160:tz_capture"))
    return kb


def _v171_special_callback(call):
    raw = str(getattr(call, "data", "") or "")
    try:
        resolver = globals().get("resolve_short_callback")
        if callable(resolver):
            raw = str(resolver(raw) or raw)
    except Exception:
        pass
    if raw == "none":
        try:
            bot.answer_callback_query(call.id)
        except Exception:
            pass
        return True
    if raw == "v171:desc":
        cid = int(call.message.chat.id)
        try:
            bot.answer_callback_query(call.id)
        except Exception:
            pass
        try:
            bot.send_message(cid, _v171_window_description(call), reply_markup=_v171_desc_keyboard(cid))
        except Exception as exc:
            try:
                bot.send_message(cid, f"❌ Не удалось открыть описание: {str(exc)[:180]}")
            except Exception:
                pass
        return True
    if raw == "v171:desc_close":
        try:
            bot.answer_callback_query(call.id)
        except Exception:
            pass
        _v171_delete_message_quiet(int(call.message.chat.id), int(call.message.message_id))
        return True
    return False


def _v171_register_special_callback() -> int:
    try:
        bot.callback_query_handler(func=lambda c: (str(getattr(c, "data", "") or "").startswith("v171:") or str(getattr(c, "data", "") or "") == "none"))(_v171_special_callback)
        handlers = getattr(bot, "callback_query_handlers", None)
        if isinstance(handlers, list) and handlers:
            row = handlers.pop()
            handlers.insert(0, row)
        return 1
    except Exception:
        return 0


# ---------------------------------------------------------------------------
# 8) F54 logical sections. Existing rows/buttons are never split or reshaped;
# only whole rows are grouped and visually separated by a blank-looking row.
# ---------------------------------------------------------------------------
_V171_PREV_BUILD_INFO_KEYBOARD = globals().get("build_info_keyboard")


def _v171_info_group(row) -> str:
    tokens = " ".join((_v171_btn_text(btn) + " " + _v171_btn_cb(btn)) for btn in (row or [])).casefold()
    if any(x in tokens for x in ("back_main", "info_close", "назад осн", "закрыть")):
        return "nav"
    if any(x in tokens for x in ("forward_copy", "forward_menu", "пересыл", "фин.пересыл", "icon_buttons")):
        return "forward"
    if "reminder" in tokens or "напомин" in tokens:
        return "reminder"
    if any(x in tokens for x in ("restore_guard", "mega_manual", "delta", "backup", "mega_priority")):
        return "storage"
    if any(x in tokens for x in ("journal", "problem_tasks", "integrity", "runtime_watcher", "info_queues", "internal_timers", "keepalive", "safety_profile", "buttons_current")):
        return "diag"
    if any(x in tokens for x in ("finance", "фин", "gomonk", "currency", "usd", "expense", "excel", "google", "article")):
        return "finance"
    if any(x in tokens for x in ("owners", "space", "владел")):
        return "access"
    if any(x in tokens for x in ("instruction", "инструк")):
        return "help"
    return "other"


def _v177_legacy_0221_build_info_keyboard(chat_id: int):
    kb = _V171_PREV_BUILD_INFO_KEYBOARD(int(chat_id)) if callable(_V171_PREV_BUILD_INFO_KEYBOARD) else types.InlineKeyboardMarkup()
    rows = _v171_kb_rows(kb)
    if not rows or not is_owner_chat(int(chat_id)):
        return kb
    order = ("diag", "storage", "finance", "forward", "reminder", "access", "help", "other", "nav")
    buckets = {k: [] for k in order}
    for row in rows:
        buckets.setdefault(_v171_info_group(row), []).append(row)
    out = []
    first = True
    for group in order:
        block = buckets.get(group) or []
        if not block:
            continue
        if not first and group != "nav":
            # Telegram does not allow a truly empty inline-keyboard row. U+3164 renders
            # as an empty visual separator while existing button rows remain unchanged.
            out.append([IB("ㅤ", callback_data="none")])
        out.extend(block)
        first = False
    return _v171_set_kb_rows(kb, out)
try: _v177_legacy_0221_build_info_keyboard.__name__ = 'build_info_keyboard'
except Exception: pass
build_info_keyboard = _v177_legacy_0221_build_info_keyboard


# ---------------------------------------------------------------------------
# 7/11) Contour/button reliability: journal the full button chain, and ACK
# successful v164 contour navigation before network-heavy menu rendering.
# ---------------------------------------------------------------------------
def _v171_contour_preack_allowed(call, raw: str) -> bool:
    if not str(raw).startswith("v164:"):
        return False
    try:
        cid = int(call.message.chat.id)
        uid = int(getattr(getattr(call, "from_user", None), "id", 0) or 0)
        if raw.startswith("v164:circle:"):
            return cid == int(OWNER_ID or 0) or bool(tenant_can_manage(uid, chat_id=_v164_circle_parent_root_for_context(cid) or cid))
        if raw.startswith("v164:space_circle:"):
            return cid == int(OWNER_ID or 0) or bool(tenant_can_manage(uid, chat_id=cid) or circle_level_for_chat(cid) == 2)
        return True
    except Exception:
        return False


def _v171_install_button_chain() -> int:
    count = 0
    for handler in list(getattr(bot, "callback_query_handlers", []) or []):
        if not isinstance(handler, dict):
            continue
        original = handler.get("function")
        if not callable(original) or getattr(original, "_v171_button_chain", False):
            continue
        def _wrapped(call, _original=original):
            raw = str(getattr(call, "data", "") or "")
            try:
                resolver = globals().get("resolve_short_callback")
                resolved = str(resolver(raw) or raw) if callable(resolver) else raw
            except Exception:
                resolved = raw
            cid = None
            try:
                cid = int(call.message.chat.id)
            except Exception:
                pass
            started = _v171_time.monotonic()
            if resolved != "none":
                try:
                    bot_journal("button_chain_press", cid, f"action={resolved}")
                except Exception:
                    pass
            if _v171_contour_preack_allowed(call, resolved):
                try:
                    bot.answer_callback_query(call.id)
                except Exception:
                    pass
            try:
                result = _original(call)
            except Exception as exc:
                try:
                    log_error(f"BUTTON_CHAIN_ERROR action={resolved} chat={cid}: {exc}")
                    bot_journal("button_chain_error", cid, f"action={resolved}; error={str(exc)[:300]}", "ERROR")
                except Exception:
                    pass
                try:
                    bot.answer_callback_query(call.id, "Ошибка выполнения кнопки. Записано в журнал.", show_alert=True)
                except Exception:
                    pass
                return None
            if resolved != "none":
                try:
                    elapsed = _v171_time.monotonic() - started
                    bot_journal("button_chain_result", cid, f"action={resolved}; ok=1; elapsed={elapsed:.3f}s")
                except Exception:
                    pass
            return result
        _wrapped._v171_button_chain = True
        _wrapped.__name__ = getattr(original, "__name__", "callback_handler")
        handler["function"] = _wrapped
        count += 1
    return count


# ---------------------------------------------------------------------------
# Journal audit: serialize refresh attempts for the same Telegram window and
# re-check the registry after the first missing-message failure unregisters it.
# ---------------------------------------------------------------------------
def _v171_window_item_key(item: dict):
    try:
        return int(item.get("chat_id") or 0), int(item.get("message_id") or 0)
    except Exception:
        return 0, 0


def _v171_wrap_registered_refresh(name: str):
    previous = globals().get(name)
    if not callable(previous) or getattr(previous, "_v171_refresh_guard", False):
        return
    def guarded(item: dict, changed_chat_id: int, _previous=previous):
        cid, mid = _v171_window_item_key(item or {})
        if not cid or not mid:
            return False
        lock = window_locks[(cid, mid)]
        with lock:
            try:
                if not get_registered_open_window(cid, mid):
                    return False
            except Exception:
                pass
            return _previous(item, changed_chat_id)
    guarded._v171_refresh_guard = True
    guarded.__name__ = name
    globals()[name] = guarded


for _v171_refresh_name in ("_refresh_registered_fin_view", "_refresh_registered_local_fin_view", "_refresh_registered_fin_categories_view"):
    try:
        _v171_wrap_registered_refresh(_v171_refresh_name)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Journal audit: current ~690 KB deltas are below MEGA's practical limits but
# were rejected by our own 512 KB regression guard.  Keep 512 KB as a soft
# diagnostic threshold and hard-block only above 1 MiB, with contributor info.
# ---------------------------------------------------------------------------
try:
    V171_DELTA_SOFT_WARN_BYTES = max(128 * 1024, int(_v171_os.getenv("MEGA_DELTA_SOFT_WARN_BYTES", str(512 * 1024)) or 512 * 1024))
except Exception:
    V171_DELTA_SOFT_WARN_BYTES = 512 * 1024
try:
    V171_DELTA_HARD_MAX_BYTES = max(V171_DELTA_SOFT_WARN_BYTES + 1, int(_v171_os.getenv("MEGA_DELTA_HARD_MAX_BYTES", str(1024 * 1024)) or 1024 * 1024))
except Exception:
    V171_DELTA_HARD_MAX_BYTES = 1024 * 1024
_V171_DELTA_WARN_LOCK = _v171_threading.RLock()
_V171_DELTA_LAST_WARN = 0.0


def _v171_json_size(obj) -> int:
    try:
        return len(_v171_json.dumps(obj, ensure_ascii=False, separators=(",", ":"), default=str).encode("utf-8"))
    except Exception:
        return -1


def _v171_delta_breakdown(payload: dict) -> str:
    pieces = []
    for key in ("chat_changes", "root_patch", "root_map_patches", "root_deletes", "root_map_deletes"):
        size = _v171_json_size((payload or {}).get(key))
        pieces.append(f"{key}={size}")
    try:
        chats = payload.get("chat_changes") or {}
        top = sorted(((str(k), _v171_json_size(v)) for k, v in chats.items()), key=lambda x: x[1], reverse=True)[:3]
        if top:
            pieces.append("top_chats=" + ",".join(f"{k}:{s}" for k, s in top))
    except Exception:
        pass
    return " ".join(pieces)


def _delta_upload_payload(payload: dict) -> tuple[bool, str]:
    global _V171_DELTA_LAST_WARN
    if not payload or not mega_is_configured():
        return False, ""
    encoded_size = _v171_json_size(payload)
    if encoded_size > V171_DELTA_HARD_MAX_BYTES:
        try:
            log_error(
                f"[MEGA DELTA BLOCKED] oversized compact delta: {encoded_size} bytes > hard {V171_DELTA_HARD_MAX_BYTES}; "
                f"{_v171_delta_breakdown(payload)}; full snapshot scheduled"
            )
        except Exception:
            pass
        try:
            _mark_global_snapshot_pending()
        except Exception:
            pass
        return False, ""
    if encoded_size > V171_DELTA_SOFT_WARN_BYTES:
        now_m = _v171_time.monotonic()
        with _V171_DELTA_WARN_LOCK:
            should_log = now_m - float(_V171_DELTA_LAST_WARN or 0.0) >= 60.0
            if should_log:
                _V171_DELTA_LAST_WARN = now_m
        if should_log:
            try:
                bot_journal(
                    "mega_delta_large_allowed_v171", None,
                    f"bytes={encoded_size}; soft={V171_DELTA_SOFT_WARN_BYTES}; hard={V171_DELTA_HARD_MAX_BYTES}; {_v171_delta_breakdown(payload)}",
                    "WARN",
                )
            except Exception:
                pass
    day_dir = mega_delta_remote_day_dir(str(payload.get("created_at") or today_key())[:10])
    _v171_os.makedirs(MEGA_LOCAL_TMP_DIR, exist_ok=True)
    name = f"delta_{payload.get('delta_id')}.json"
    local_path = _v171_os.path.join(MEGA_LOCAL_TMP_DIR, name)
    try:
        _save_json(local_path, payload)
        mega_ensure_remote_path(day_dir)
        _mega_run("mega-put", [local_path, day_dir], check=True, timeout=MEGA_TIMEOUT)
        return True, day_dir.rstrip("/") + "/" + name
    except Exception as exc:
        try:
            log_error(f"[MEGA DELTA ERROR] {exc}")
        except Exception:
            pass
        return False, ""
    finally:
        try:
            if _v171_os.path.exists(local_path):
                _v171_os.remove(local_path)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# TZ rows: persist the global-default scope and make the export self-explanatory.
# Mark all eleven v169 items as fixed by v171 after restore.
# ---------------------------------------------------------------------------
_V171_PREV_SAVE_TZ = globals().get("_v160_save_tz")
_V171_PREV_TZ_EXPORT = globals().get("_v167_tz_export")


def _v160_save_tz(chat_id: int, user_id: int, marker: str, body: str, source: dict) -> dict:
    if not callable(_V171_PREV_SAVE_TZ):
        raise RuntimeError("TZ storage unavailable")
    row = _V171_PREV_SAVE_TZ(chat_id, user_id, marker, body, source)
    try:
        row["scope_policy"] = V171_TZ_SCOPE_POLICY
        row["scope_note"] = V171_TZ_SCOPE_TEXT
        _v160_persist_annotations(int(chat_id))
    except Exception:
        pass
    return row


def _v167_tz_export(kind: str):
    if not callable(_V171_PREV_TZ_EXPORT):
        return "ТЗ_окон", ""
    base, content = _V171_PREV_TZ_EXPORT(kind)
    if str(kind) in {"tz", "tz_archive"}:
        lines = str(content or "").splitlines()
        insert_at = 4 if len(lines) >= 4 else len(lines)
        lines[insert_at:insert_at] = [f"Правило области ТЗ: {V171_TZ_SCOPE_TEXT}", ""]
        content = "\n".join(lines).rstrip() + "\n"
    return base, content


_V171_TZ_FIX_SIGNATURES = (
    ("Ф233", ("таймер", "закрыв")),
    ("Ф91", ("когда даю тз", "всего бота")),
    ("Ф54", ("переименуй", "фин")),
    ("Ф54", ("проверь журнал", "ошиб")),
    ("Ф191", ("напоминал", "другие чаты")),
    ("Ф54", ("перес", "момент пересыл")),
    ("Ф54", ("контур", "все кнопки")),
    ("Ф54", ("отсортируй", "логич")),
    ("Ф91", ("каждом меню", "описание")),
    ("Ф91", ("новое окно", "назад")),
    ("Ф91", ("кнопки не ломались", "нажатие")),
)


def _v171_mark_all_v169_tz_fixed() -> int:
    changed = 0
    try:
        _catalog, rows = _v160_annotation_roots()
    except Exception:
        return 0
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        ver = str(row.get("version") or "")
        if not ver.startswith("bot_v169_"):
            continue
        marker = str(row.get("marker") or "").upper()
        body = str(row.get("text") or "").casefold()
        matched = False
        for want_marker, needles in _V171_TZ_FIX_SIGNATURES:
            if marker == want_marker and all(str(n).casefold() in body for n in needles):
                matched = True
                break
        if not matched:
            continue
        if str(row.get("status") or "").lower() != "fixed" or str(row.get("fixed_by_version") or "") != VERSION:
            row["status"] = "fixed"
            row["fixed_by_version"] = VERSION
            row["fixed_at"] = now_local().isoformat(timespec="seconds")
            row["scope_policy"] = V171_TZ_SCOPE_POLICY
            changed += 1
    if changed:
        try:
            _v160_persist_annotations(int(OWNER_ID or 0))
        except Exception:
            pass
    return changed


# ---------------------------------------------------------------------------
# v171 restore compatibility.
# ---------------------------------------------------------------------------
_V171_PREV_RESTORE_VALIDATOR = globals().get("_v153_validate_restore_gz")


def _v177_legacy_0288_v153_validate_restore_gz(gz_path: str):
    if callable(_V171_PREV_RESTORE_VALIDATOR):
        try:
            return _V171_PREV_RESTORE_VALIDATOR(gz_path)
        except Exception as exc:
            if "unsupported bot version" not in str(exc):
                raise
    folder = _v171_tempfile.mkdtemp(prefix="v171_restore_validate_")
    raw = _v171_os.path.join(folder, "restore.sqlite3")
    try:
        with _v171_gzip.open(gz_path, "rb") as fin, open(raw, "wb") as fout:
            _v171_shutil.copyfileobj(fin, fout, 1024 * 1024)
        conn = _v171_sqlite3.connect(raw)
        try:
            integrity = str(conn.execute("PRAGMA integrity_check").fetchone()[0])
            if integrity.lower() != "ok":
                raise RuntimeError(f"SQLite integrity_check: {integrity}")
            row = conn.execute("SELECT v FROM meta WHERE kind='v153_export' AND k='manifest'").fetchone()
            if not row:
                raise RuntimeError("manifest v153 not found")
            manifest = _v171_json.loads(row[0])
        finally:
            conn.close()
        if str(manifest.get("kind")) != "telegram_bot_full_state_v153":
            raise RuntimeError("unknown export kind")
        if int(manifest.get("schema_version") or 0) != int(V153_EXPORT_SCHEMA):
            raise RuntimeError("unsupported export schema")
        export_version = str(manifest.get("bot_version") or "")
        if not export_version.startswith(tuple(f"bot_v{i}_" for i in range(153, 172))):
            raise RuntimeError(f"unsupported bot version: {export_version or 'missing'}")
        if _v153_db_logical_checksum(raw) != str(manifest.get("checksum") or ""):
            raise RuntimeError("checksum mismatch")
        return manifest, raw
    except Exception:
        _v171_shutil.rmtree(folder, ignore_errors=True)
        raise
try: _v177_legacy_0288_v153_validate_restore_gz.__name__ = '_v153_validate_restore_gz'
except Exception: pass
_v153_validate_restore_gz = _v177_legacy_0288_v153_validate_restore_gz


# ---------------------------------------------------------------------------
# READY-time migration runs after MEGA restore, not just at module import.
# ---------------------------------------------------------------------------
_V171_PREV_RUNTIME_MARK_READY = globals().get("runtime_mark_ready")
if callable(_V171_PREV_RUNTIME_MARK_READY):
    def runtime_mark_ready(detail: str = ""):
        result = _V171_PREV_RUNTIME_MARK_READY(detail)
        try:
            fixed = _v171_mark_all_v169_tz_fixed()
            bot_journal("v171_tz_fixed", int(OWNER_ID or 0), f"fixed={fixed}; policy={V171_TZ_SCOPE_POLICY}")
        except Exception:
            pass
        try:
            _v171_reminder_global_mode(True)
            forward_copy_edit_mode(int(OWNER_ID or 0))
        except Exception:
            pass
        return result


# Install handlers/wrappers last so v164's late contour handler is covered too.
_V171_SPECIAL_HANDLER_COUNT = _v171_register_special_callback()
_V171_BUTTON_CHAIN_COUNT = _v171_install_button_chain()

try:
    bot_journal(
        "v171_installed", int(OWNER_ID or 0),
        f"all_v169_tz=11; special_handlers={_V171_SPECIAL_HANDLER_COUNT}; wrapped_callbacks={_V171_BUTTON_CHAIN_COUNT}; "
        f"delta_soft={V171_DELTA_SOFT_WARN_BYTES}; delta_hard={V171_DELTA_HARD_MAX_BYTES}",
    )
except Exception:
    pass
# v178_global_performance_final
