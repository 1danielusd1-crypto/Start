# v152_human_journals_chat_rights

VERSION = "bot_v152_human_journals_chat_rights"

import functools as _v152_functools
import io as _v152_io
import os as _v152_os
import re as _v152_re
import time as _v152_time
from datetime import datetime as _v152_datetime

V152_CHAT_RIGHTS_SCHEMA = 1

V152_PERMISSION_GROUPS = (
    ("finance", "💰 Финансы", (
        ("finance.mode", "Финансовый режим"),
        ("finance.ars", "ARS"),
        ("finance.usd", "USD"),
        ("finance.gomonk", "Гомонковые"),
        ("finance.edit", "Редактирование операций"),
        ("finance.delete", "Удаление операций"),
        ("finance.view_totals", "Просмотр итогов"),
        ("finance.view_month", "Просмотр месяца"),
    )),
    ("exports", "📤 Выгрузки", (
        ("exports.excel_chat", "Excel в чат"),
        ("exports.google_sheets", "Google Sheets"),
        ("exports.google_drive", "Google Drive"),
        ("exports.journals", "Скачивание журналов"),
        ("exports.reports", "Отчёты"),
    )),
    ("reminders", "⏰ Напоминалки", (
        ("reminders.use", "Использование напоминалок"),
        ("reminders.create", "Создание"),
        ("reminders.edit", "Изменение"),
        ("reminders.delete", "Удаление"),
        ("reminders.complete", "Выполнение через /vyapl"),
    )),
    ("forward", "🔁 Пересылка и чаты", (
        ("forward.messages", "Пересылка сообщений"),
        ("forward.media_groups", "Пересылка медиагрупп"),
        ("chats.connect_children", "Подключение дочерних чатов"),
        ("chats.manage_users", "Управление пользователями"),
        ("chats.manage_roles", "Управление ролями"),
    )),
    ("settings", "⚙️ Настройки", (
        ("settings.change", "Изменение настроек"),
        ("settings.windows", "Управление окнами"),
        ("settings.iphone", "Быстрые отметки с iPhone"),
        ("settings.info", "Просмотр INFO"),
        ("settings.diagnostics", "Диагностика"),
        ("settings.backup_recovery", "Backup и recovery"),
        ("settings.audit", "Аудит"),
    )),
)

V152_PERMISSION_ITEMS = tuple(item for _group, _label, items in V152_PERMISSION_GROUPS for item in items)
V152_PERMISSION_KEYS = tuple(key for key, _label in V152_PERMISSION_ITEMS)
V152_PERMISSION_LABELS = dict(V152_PERMISSION_ITEMS)
V152_PERMISSION_INDEX = {key: idx for idx, key in enumerate(V152_PERMISSION_KEYS)}


def _v152_permissions_root() -> dict:
    root = data.setdefault("_global_settings", {}).setdefault("chat_permissions_v152", {})
    if not isinstance(root, dict):
        root = {}
        data.setdefault("_global_settings", {})["chat_permissions_v152"] = root
    root.setdefault("schema_version", V152_CHAT_RIGHTS_SCHEMA)
    root.setdefault("global", {})
    root.setdefault("history", [])
    return root


def _v152_tenant_id_for_chat(chat_id: int) -> str:
    try:
        return str(tenant_id_for_chat(int(chat_id), create=False) or TENANT_PLATFORM_ID)
    except Exception:
        return str(TENANT_PLATFORM_ID)


def _v152_tenant_row(tenant_id: str) -> dict:
    try:
        return tenant_get(str(tenant_id)) or {}
    except Exception:
        return {}


def _v152_tenant_permissions(tenant_id: str) -> dict:
    row = _v152_tenant_row(tenant_id)
    settings = row.setdefault("settings", {})
    permissions = settings.setdefault("chat_permissions_v152_defaults", {})
    return permissions if isinstance(permissions, dict) else {}


def _v152_chat_policy(chat_id: int) -> dict:
    store = get_chat_store(int(chat_id))
    settings = store.setdefault("settings", {})
    policy = settings.setdefault("chat_permissions_v152", {})
    if not isinstance(policy, dict):
        policy = {}
        settings["chat_permissions_v152"] = policy
    policy.setdefault("inherit_tenant", True)
    policy.setdefault("overrides", {})
    policy.setdefault("updated_at", "")
    policy.setdefault("updated_by", 0)
    return policy


def v152_global_permission_allowed(capability: str) -> bool:
    if capability not in V152_PERMISSION_LABELS:
        return True
    return bool((_v152_permissions_root().get("global") or {}).get(capability, True))


def v152_tenant_permission_allowed(tenant_id: str, capability: str) -> bool:
    if not v152_global_permission_allowed(capability):
        return False
    return bool(_v152_tenant_permissions(str(tenant_id)).get(capability, True))


def v152_chat_permission_allowed(chat_id: int, capability: str) -> bool:
    capability = str(capability or "")
    if capability not in V152_PERMISSION_LABELS:
        return True
    tid = _v152_tenant_id_for_chat(int(chat_id))
    if not v152_global_permission_allowed(capability):
        return False
    policy = _v152_chat_policy(int(chat_id))
    if bool(policy.get("inherit_tenant", True)):
        return v152_tenant_permission_allowed(tid, capability)
    overrides = policy.get("overrides") if isinstance(policy.get("overrides"), dict) else {}
    return bool(overrides.get(capability, v152_tenant_permission_allowed(tid, capability)))


def _v152_actor_id(obj=None) -> int:
    try:
        return int(getattr(getattr(obj, "from_user", None), "id", 0) or tenant_current_actor_user_id() or 0)
    except Exception:
        return 0


def _v152_actor_is_platform_owner(user_id: int) -> bool:
    try:
        return bool(tenant_is_platform_owner_user(int(user_id)))
    except Exception:
        return bool(int(user_id or 0) == int(OWNER_ID or 0))


def _v152_actor_can_manage_tenant(user_id: int, tenant_id: str) -> bool:
    if _v152_actor_is_platform_owner(user_id):
        return True
    try:
        return bool(tenant_can_manage(int(user_id), str(tenant_id)))
    except Exception:
        return False


def _v152_actor_can_manage_chat(user_id: int, chat_id: int) -> bool:
    return _v152_actor_can_manage_tenant(int(user_id), _v152_tenant_id_for_chat(int(chat_id)))


def _v152_persist(reason: str, chat_id: int | None = None, tenant_id: str | None = None, actor_id: int = 0) -> None:
    now = now_local().isoformat(timespec="seconds") if "now_local" in globals() else _v152_datetime.now().isoformat(timespec="seconds")
    row = {"at": now, "reason": str(reason), "chat_id": int(chat_id or 0), "tenant_id": str(tenant_id or ""), "actor_id": int(actor_id or 0)}
    history = _v152_permissions_root().setdefault("history", [])
    history.append(row)
    del history[:-500]
    try:
        save_data(data, root_only=True)
    except TypeError:
        save_data(data)
    try:
        root_chat = int((_v152_tenant_row(tenant_id or _v152_tenant_id_for_chat(int(chat_id or 0))).get("root_chat_id") or OWNER_ID or chat_id or 0))
        if root_chat:
            schedule_delta_backup(root_chat, delay=0.35, reason=f"chat_permissions_v152:{reason}")
    except Exception:
        pass
    try:
        bot_journal("chat_permissions_v152_changed", int(chat_id or OWNER_ID or 0), f"reason={reason}; tenant={tenant_id or ''}; actor={int(actor_id or 0)}")
    except Exception:
        pass


def _v152_set_global(capability: str, enabled: bool, actor_id: int) -> bool:
    if not _v152_actor_is_platform_owner(actor_id) or capability not in V152_PERMISSION_LABELS:
        return False
    _v152_permissions_root().setdefault("global", {})[capability] = bool(enabled)
    _v152_persist(f"global:{capability}={int(bool(enabled))}", tenant_id=TENANT_PLATFORM_ID, actor_id=actor_id)
    return True


def _v152_set_tenant(tenant_id: str, capability: str, enabled: bool, actor_id: int) -> bool:
    if not _v152_actor_can_manage_tenant(actor_id, tenant_id) or capability not in V152_PERMISSION_LABELS:
        return False
    if enabled and not v152_global_permission_allowed(capability):
        return False
    _v152_tenant_permissions(tenant_id)[capability] = bool(enabled)
    _v152_persist(f"tenant:{capability}={int(bool(enabled))}", tenant_id=tenant_id, actor_id=actor_id)
    return True


def _v152_set_chat(chat_id: int, capability: str, enabled: bool, actor_id: int) -> bool:
    if not _v152_actor_can_manage_chat(actor_id, chat_id) or capability not in V152_PERMISSION_LABELS:
        return False
    if enabled and not v152_global_permission_allowed(capability):
        return False
    policy = _v152_chat_policy(chat_id)
    if bool(policy.get("inherit_tenant", True)):
        return False
    policy.setdefault("overrides", {})[capability] = bool(enabled)
    policy["updated_at"] = now_local().isoformat(timespec="seconds")
    policy["updated_by"] = int(actor_id)
    _v152_persist(f"chat:{capability}={int(bool(enabled))}", chat_id=chat_id, tenant_id=_v152_tenant_id_for_chat(chat_id), actor_id=actor_id)
    return True


V152_PRESETS = {
    "all": set(V152_PERMISSION_KEYS),
    "none": set(),
    "finance": {key for key in V152_PERMISSION_KEYS if key.startswith("finance.")} | {"exports.excel_chat", "exports.reports", "settings.info"},
    "view": {"finance.view_totals", "finance.view_month", "exports.journals", "exports.reports", "reminders.use", "settings.info"},
    "standard": {
        "finance.mode", "finance.ars", "finance.usd", "finance.gomonk", "finance.view_totals", "finance.view_month",
        "exports.excel_chat", "exports.reports", "reminders.use", "reminders.complete", "forward.messages", "forward.media_groups", "settings.info",
    },
    "locked": {"finance.view_totals", "finance.view_month", "exports.journals", "exports.reports", "reminders.use", "settings.info", "settings.diagnostics"},
}
V152_PRESET_LABELS = {
    "all": "✅ Включить всё",
    "none": "❌ Выключить всё",
    "finance": "💰 Только финансы",
    "view": "👁 Только просмотр",
    "standard": "🧑‍💼 Стандартный чат",
    "locked": "🔒 Заблокировать изменения",
}


def _v152_apply_preset(scope: str, target: str | int, preset: str, actor_id: int) -> bool:
    enabled = V152_PRESETS.get(str(preset))
    if enabled is None:
        return False
    values = {key: bool(key in enabled and v152_global_permission_allowed(key)) for key in V152_PERMISSION_KEYS}
    if scope == "global":
        if not _v152_actor_is_platform_owner(actor_id):
            return False
        _v152_permissions_root()["global"] = {key: key in enabled for key in V152_PERMISSION_KEYS}
        _v152_persist(f"global_preset:{preset}", tenant_id=TENANT_PLATFORM_ID, actor_id=actor_id)
        return True
    if scope == "tenant":
        tenant_id = str(target)
        if not _v152_actor_can_manage_tenant(actor_id, tenant_id):
            return False
        _v152_tenant_row(tenant_id).setdefault("settings", {})["chat_permissions_v152_defaults"] = dict(values)
        _v152_persist(f"tenant_preset:{preset}", tenant_id=tenant_id, actor_id=actor_id)
        return True
    chat_id = int(target)
    if not _v152_actor_can_manage_chat(actor_id, chat_id):
        return False
    policy = _v152_chat_policy(chat_id)
    policy["inherit_tenant"] = False
    policy["overrides"] = dict(values)
    policy["updated_at"] = now_local().isoformat(timespec="seconds")
    policy["updated_by"] = int(actor_id)
    _v152_persist(f"chat_preset:{preset}", chat_id=chat_id, tenant_id=_v152_tenant_id_for_chat(chat_id), actor_id=actor_id)
    return True


def _v152_toggle_chat_inheritance(chat_id: int, actor_id: int) -> bool:
    if not _v152_actor_can_manage_chat(actor_id, chat_id):
        return False
    policy = _v152_chat_policy(chat_id)
    old = bool(policy.get("inherit_tenant", True))
    if old:
        tid = _v152_tenant_id_for_chat(chat_id)
        policy["overrides"] = {key: v152_tenant_permission_allowed(tid, key) for key in V152_PERMISSION_KEYS}
        policy["inherit_tenant"] = False
    else:
        policy["inherit_tenant"] = True
    policy["updated_at"] = now_local().isoformat(timespec="seconds")
    policy["updated_by"] = int(actor_id)
    _v152_persist(f"chat_inherit={int(not old)}", chat_id=chat_id, tenant_id=_v152_tenant_id_for_chat(chat_id), actor_id=actor_id)
    return True


def _v152_accessible_chats(user_id: int, context_chat_id: int) -> list[int]:
    if _v152_actor_is_platform_owner(user_id):
        out = []
        try:
            for tenant in tenant_all():
                for cid in tenant.get("chat_ids") or []:
                    if int(cid) not in out:
                        out.append(int(cid))
        except Exception:
            pass
        return sorted(out, key=lambda cid: str(get_chat_display_name(cid) or cid).casefold())
    tid = _v152_tenant_id_for_chat(context_chat_id)
    if not _v152_actor_can_manage_tenant(user_id, tid):
        return []
    try:
        return list(tenant_chat_ids(tid))
    except Exception:
        return []


def _v152_short(text: str, limit: int = 38) -> str:
    text = " ".join(str(text or "").split())
    return text if len(text) <= limit else text[: limit - 1] + "…"


def build_v152_chat_rights_list_text(user_id: int, context_chat_id: int, page: int = 0) -> str:
    chats = _v152_accessible_chats(user_id, context_chat_id)
    pages = max(1, (len(chats) + 9) // 10)
    page = max(0, min(int(page), pages - 1))
    return (
        "🛡 ПРАВА ЧАТОВ · Ф206\n\n"
        "Права применяются внутри обработчиков команд и callback, а не только скрывают кнопки.\n"
        "Глобальный запрет владельца платформы имеет приоритет над пространством и чатом.\n\n"
        f"Доступных чатов: {len(chats)}\nСтраница: {page + 1}/{pages}"
    )


def build_v152_chat_rights_list_keyboard(user_id: int, context_chat_id: int, page: int = 0):
    chats = _v152_accessible_chats(user_id, context_chat_id)
    pages = max(1, (len(chats) + 9) // 10)
    page = max(0, min(int(page), pages - 1))
    kb = types.InlineKeyboardMarkup(row_width=1)
    if _v152_actor_is_platform_owner(user_id):
        kb.row(IB("🌐 Ограничения платформы", callback_data="v152:r:g:0"))
    tid = _v152_tenant_id_for_chat(context_chat_id)
    if _v152_actor_can_manage_tenant(user_id, tid):
        kb.row(IB("🏢 Права пространства", callback_data=f"v152:r:t:{tid}:0"))
    for cid in chats[page * 10: page * 10 + 10]:
        tenant = _v152_tenant_row(_v152_tenant_id_for_chat(cid))
        root = "⭐ " if int((tenant or {}).get("root_chat_id") or 0) == int(cid) else ""
        kb.row(IB(root + _v152_short(get_chat_display_name(cid) or f"Чат {cid}"), callback_data=f"v152:r:c:{int(cid)}:0"))
    if pages > 1:
        nav = []
        if page > 0:
            nav.append(IB("⬅️", callback_data=f"v152:r:l:{page - 1}"))
        nav.append(IB(f"{page + 1}/{pages}", callback_data="none"))
        if page + 1 < pages:
            nav.append(IB("➡️", callback_data=f"v152:r:l:{page + 1}"))
        kb.row(*nav)
    kb.row(IB("⬅️ К защите", callback_data="safety_profile_open"))
    return kb


def _v152_scope_value(scope: str, target: str | int, capability: str) -> bool:
    if scope == "global":
        return v152_global_permission_allowed(capability)
    if scope == "tenant":
        return v152_tenant_permission_allowed(str(target), capability)
    return v152_chat_permission_allowed(int(target), capability)


def _v152_scope_title(scope: str, target: str | int) -> str:
    if scope == "global":
        return "🌐 ОГРАНИЧЕНИЯ ПЛАТФОРМЫ"
    if scope == "tenant":
        row = _v152_tenant_row(str(target))
        return f"🏢 ПРАВА ПРОСТРАНСТВА\n{row.get('name') or target}"
    chat_id = int(target)
    return f"💬 ПРАВА ЧАТА\n{get_chat_display_name(chat_id) or chat_id}"


def build_v152_permission_text(scope: str, target: str | int) -> str:
    allowed = sum(1 for key in V152_PERMISSION_KEYS if _v152_scope_value(scope, target, key))
    lines = [_v152_scope_title(scope, target), "", f"Разрешено: {allowed}/{len(V152_PERMISSION_KEYS)}"]
    if scope == "chat":
        policy = _v152_chat_policy(int(target))
        lines.append(f"Наследовать настройки пространства: {'✅ включено' if policy.get('inherit_tenant', True) else '❌ выключено'}")
        if policy.get("inherit_tenant", True):
            lines.append("Чтобы менять отдельные функции этого чата, сначала выключите наследование.")
    if scope != "global":
        locked = [V152_PERMISSION_LABELS[key] for key in V152_PERMISSION_KEYS if not v152_global_permission_allowed(key)]
        if locked:
            lines.append(f"Глобально заблокировано: {len(locked)}")
    return "\n".join(lines)


def build_v152_permission_keyboard(scope: str, target: str | int, page: int = 0):
    kb = types.InlineKeyboardMarkup(row_width=1)
    if scope == "chat":
        policy = _v152_chat_policy(int(target))
        kb.row(IB(
            f"Наследовать настройки пространства: {'ВКЛ' if policy.get('inherit_tenant', True) else 'ВЫКЛ'}",
            callback_data=f"v152:r:i:{int(target)}:{int(page)}",
        ))
    for preset in ("all", "none", "finance", "view", "standard", "locked"):
        kb.row(IB(V152_PRESET_LABELS[preset], callback_data=f"v152:r:p:{scope[0]}:{target}:{preset}:{int(page)}"))
    inherited = scope == "chat" and bool(_v152_chat_policy(int(target)).get("inherit_tenant", True))
    for _group, group_label, items in V152_PERMISSION_GROUPS:
        kb.row(IB(group_label, callback_data="none"))
        for capability, label in items:
            enabled = _v152_scope_value(scope, target, capability)
            locked = scope != "global" and not v152_global_permission_allowed(capability)
            prefix = "🔒" if locked else ("✅" if enabled else "❌")
            suffix = " · наследуется" if inherited else ""
            idx = V152_PERMISSION_INDEX[capability]
            kb.row(IB(f"{prefix} {label}{suffix}", callback_data=f"v152:r:x:{scope[0]}:{target}:{idx}:{int(page)}"))
    kb.row(IB("⬅️ К списку чатов", callback_data=f"v152:r:l:{int(page)}"))
    return kb


# INFO: the status button now opens the existing protection menu instead of toggling it.
_V152_ORIG_BUILD_INFO_KEYBOARD = globals().get("build_info_keyboard")
def build_info_keyboard(chat_id: int):
    kb = _V152_ORIG_BUILD_INFO_KEYBOARD(int(chat_id))
    try:
        for row in getattr(kb, "keyboard", None) or getattr(kb, "inline_keyboard", None) or []:
            for button in row:
                if isinstance(button, dict):
                    if button.get("callback_data") == "safety_profile_toggle":
                        button["callback_data"] = "safety_profile_open"
                elif getattr(button, "callback_data", None) == "safety_profile_toggle":
                    button.callback_data = "safety_profile_open"
    except Exception:
        pass
    return kb


_V152_ORIG_BUILD_SAFETY_KEYBOARD = globals().get("build_safety_profile_keyboard")
def build_safety_profile_keyboard(chat_id: int):
    kb = types.InlineKeyboardMarkup(row_width=1)
    kb.row(IB(
        f"🔄 По-старому/по-новому · сейчас {'ПО-НОВОМУ' if safety_profile_new_enabled() else 'ПО-СТАРОМУ'}",
        callback_data="safety_profile_toggle",
    ))
    kb.row(IB("🛡 Права чатов", callback_data="v152:r:l:0"))
    kb.row(IB("👥 Права пользователей", callback_data="security_roles:0"))
    day = get_chat_store(int(chat_id)).get("current_view_day") or today_key()
    kb.row(IB("🔙 Назад в Инфо", callback_data=f"d:{day}:info"))
    return kb


# Callback capability mapping. This is evaluated centrally for every legacy callback.
def v152_callback_capability(action: str) -> str | None:
    raw = str(action or "")
    value = raw.split(":", 2)[2] if raw.startswith("d:") and raw.count(":") >= 2 else raw
    low = value.casefold()
    if low.startswith("v152:r:"):
        return None
    if any(x in low for x in ("journal", "log_file", "errors_file", "failed", "problem_tasks")):
        return "exports.journals"
    if "google" in low:
        if any(x in low for x in ("drive", "folder", "upload_drive")):
            return "exports.google_drive"
        return "exports.google_sheets"
    if any(x in low for x in ("excel", "xlsx", "csv", "tabl_lsx", "download")):
        return "exports.excel_chat"
    if any(x in low for x in ("report", "summary", "itog")):
        return "exports.reports"
    if low.startswith(("rem:add", "reminder_add")):
        return "reminders.create"
    if low.startswith(("rem:delete", "rem:del", "reminder_delete")):
        return "reminders.delete"
    if low.startswith(("rem:edit", "rem:save", "reminder_edit")):
        return "reminders.edit"
    if any(x in low for x in ("v149:rem:done", "vyapl")):
        return "reminders.complete"
    if low.startswith(("rem:", "reminder", "v149:rem:")):
        return "reminders.use"
    if any(x in low for x in ("media_group", "mediagroup", "album")) and low.startswith(("fw", "fwd", "forward")):
        return "forward.media_groups"
    if low.startswith(("fw", "fwd", "forward", "stopforward")):
        return "forward.messages"
    if low.startswith(("sp:chatlink", "tenant_chat_link", "space_chat_link", "sp:unlink")):
        return "chats.connect_children"
    if low.startswith(("sp:userlink", "tenant_user", "space_user")):
        return "chats.manage_users"
    if low.startswith(("sp:role", "sp:transfer", "tenant_role", "space_role")):
        return "chats.manage_roles"
    if "gomonk" in low:
        return "finance.gomonk"
    if any(x in low for x in ("usd_month", "month_usd", "usd:month", "month_view")):
        return "finance.view_month"
    if any(x in low for x in ("usd", "currency_usd")):
        return "finance.usd"
    if any(x in low for x in ("delete", "del_selected", "remove_record")):
        return "finance.delete"
    if any(x in low for x in ("edit", "izm", "record_change")):
        return "finance.edit"
    if any(x in low for x in ("balance", "totals", "ostatok")):
        return "finance.view_totals"
    if any(x in low for x in ("finance_mode", "info_finance", "finmode")):
        return "finance.mode"
    if "expense_shortcut" in low or "iphone" in low:
        return "settings.iphone"
    if any(x in low for x in ("runtime", "diagnostic", "diag", "queues", "delta_status")):
        return "settings.diagnostics"
    if any(x in low for x in ("mega_", "restore", "backup", "sqlite", "db")):
        return "settings.backup_recovery"
    if any(x in low for x in ("audit", "integrity")):
        return "settings.audit"
    if any(x in low for x in ("window", "okna", "buttons_current")):
        return "settings.windows"
    if low in {"info", "info_open"} or low.endswith(":info"):
        return "settings.info"
    if any(x in low for x in ("toggle", "setting", "style", "mode")):
        return "settings.change"
    return None


_V152_ORIG_SAFETY_PERMISSION_ALLOWED = globals().get("safety_permission_allowed")
def safety_permission_allowed(user_id: int | None, chat_id: int | None, action: str) -> bool:
    try:
        uid, cid = int(user_id or 0), int(chat_id or 0)
    except Exception:
        return False
    if _v152_actor_is_platform_owner(uid):
        return True
    capability = v152_callback_capability(action)
    if capability:
        # Tenant boundary and role checks remain mandatory, but the legacy "old/new" switch
        # cannot bypass the explicit per-chat matrix.
        tid = _v152_tenant_id_for_chat(cid)
        role = tenant_role_for_user(uid, tid)
        if role not in {"tenant_owner", "tenant_admin", "operator", "viewer", "standard"}:
            return False
        mutating = capability not in {
            "finance.view_totals", "finance.view_month", "exports.journals", "exports.reports",
            "reminders.use", "settings.info", "settings.diagnostics", "settings.audit",
        }
        if mutating and role in {"viewer", "standard"}:
            return False
        return v152_chat_permission_allowed(cid, capability)
    if callable(_V152_ORIG_SAFETY_PERMISSION_ALLOWED):
        return bool(_V152_ORIG_SAFETY_PERMISSION_ALLOWED(uid, cid, action))
    return True


_V152_ORIG_SECURITY_USER_ALLOWED = globals().get("security_user_allowed")
def security_user_allowed(user_id: int | None, capability: str) -> bool:
    if callable(_V152_ORIG_SECURITY_USER_ALLOWED) and not _V152_ORIG_SECURITY_USER_ALLOWED(user_id, capability):
        return False
    try:
        cid = int(current_state_chat_id() or 0)
    except Exception:
        cid = 0
    if not cid or _v152_actor_is_platform_owner(int(user_id or 0)):
        return True
    fine = {
        "finance_input": "finance.usd" if usd_transactions_view_enabled(cid) else "finance.ars",
        "finance_manage": "finance.edit",
        "export": "exports.excel_chat",
        "forward_manage": "forward.messages",
        "reminder_manage": "reminders.use",
        "view": "settings.info",
    }.get(str(capability or ""))
    return v152_chat_permission_allowed(cid, fine) if fine else True


def v152_command_capability(command: str) -> str | None:
    cmd = str(command or "").strip().casefold().lstrip("/").split("@", 1)[0]
    if _v152_re.fullmatch(r"vyapl(?:_\d+)?", cmd):
        return "reminders.complete"
    maps = {
        "finance.mode": {"buttons"},
        "finance.view_totals": {"balance", "ok", "поехали"},
        "finance.view_month": {"prev", "next"},
        "exports.excel_chat": {"csv", "xlsx", "excel", "tabl_lsx", "json"},
        "exports.journals": {"journal", "log", "logs", "errors", "bot_errors"},
        "exports.reports": {"report"},
        "exports.google_sheets": {"google", "google_space", "google_tenant", "google_connect", "google_sheet", "google_email"},
        "exports.google_drive": {"google_drive"},
        "reminders.complete": {"vyapl_history"},
        "forward.messages": {"stopforward"},
        "chats.connect_children": {"space_chat_link", "tenant_chat_link", "space_join", "tenant_join", "space_unlink", "tenant_unlink", "space_claim", "tenant_claim"},
        "chats.manage_users": {"space_user_link", "tenant_user_link", "space_users", "tenant_users"},
        "chats.manage_roles": {"space_role", "tenant_role", "space_transfer", "tenant_transfer"},
        "settings.change": {"space_rename", "tenant_rename", "space_create", "tenant_create", "off_on_backup_excel"},
        "settings.windows": {"windows", "okna", "окна"},
        "settings.info": {"space", "spaces", "tenant", "пространство", "пространства", "space_chats", "tenant_chats", "help", "start"},
        "settings.diagnostics": {"diag", "diagnostics", "queues", "queue_status", "delta_status", "runtime_export", "mega_status", "chat_status", "chat_history"},
        "settings.backup_recovery": {"backup_channel_on", "backup_channel_off", "mega_backup_now", "mega_restore_now", "restore", "restore_off", "restore_guard", "restore_guard_on", "restore_guard_off", "sqlite", "db", "chat_archive", "chat_restore"},
        "settings.audit": {"command_audit", "articles", "статьи"},
    }
    for capability, commands in maps.items():
        if cmd in commands:
            return capability
    return None


def _v152_command_allowed(msg, capability: str) -> bool:
    uid = _v152_actor_id(msg)
    if _v152_actor_is_platform_owner(uid):
        return True
    try:
        cid = int(msg.chat.id)
    except Exception:
        return False
    role = tenant_role_for_user(uid, _v152_tenant_id_for_chat(cid))
    mutating = capability not in {
        "finance.view_totals", "finance.view_month", "exports.journals", "exports.reports",
        "reminders.use", "settings.info", "settings.diagnostics", "settings.audit",
    }
    if mutating and role in {"viewer", "standard"}:
        return False
    return v152_chat_permission_allowed(cid, capability)


def _v152_install_command_wrappers() -> int:
    wrapped = 0
    handlers = getattr(bot, "message_handlers", None)
    if not isinstance(handlers, list):
        return 0
    for handler in handlers:
        if not isinstance(handler, dict):
            continue
        original = handler.get("function")
        if not callable(original) or getattr(original, "_v152_permission_wrapped", False):
            continue
        @_v152_functools.wraps(original)
        def guarded(message, *args, __original=original, **kwargs):
            text = str(getattr(message, "text", "") or "").strip()
            if text.startswith("/"):
                command = text.split(None, 1)[0]
                capability = v152_command_capability(command)
                if capability and not _v152_command_allowed(message, capability):
                    try:
                        send_and_auto_delete(int(message.chat.id), f"⛔ Для этого чата запрещено: {V152_PERMISSION_LABELS.get(capability, capability)}.", 10)
                        bot_journal("chat_permission_command_denied", int(message.chat.id), f"user={_v152_actor_id(message)} command={command} capability={capability}", "WARN")
                    except Exception:
                        pass
                    return None
            return __original(message, *args, **kwargs)
        guarded._v152_permission_wrapped = True
        handler["function"] = guarded
        wrapped += 1
    return wrapped


# In-handler enforcement for ordinary finance input/edit and forwarding, including non-slash messages.
_V152_ORIG_HANDLE_FINANCE_TEXT = globals().get("handle_finance_text")
def handle_finance_text(msg):
    cid = int(msg.chat.id); uid = _v152_actor_id(msg)
    capability = "finance.usd" if usd_transactions_view_enabled(cid) else "finance.ars"
    if not _v152_actor_is_platform_owner(uid) and (not v152_chat_permission_allowed(cid, "finance.mode") or not v152_chat_permission_allowed(cid, capability)):
        try: send_and_auto_delete(cid, "⛔ Добавление финансовых операций запрещено правами этого чата.", 8)
        except Exception: pass
        return True
    return _V152_ORIG_HANDLE_FINANCE_TEXT(msg) if callable(_V152_ORIG_HANDLE_FINANCE_TEXT) else False


_V152_ORIG_HANDLE_FINANCE_EDIT = globals().get("handle_finance_edit")
def handle_finance_edit(msg):
    cid = int(msg.chat.id); uid = _v152_actor_id(msg)
    if not _v152_actor_is_platform_owner(uid) and not v152_chat_permission_allowed(cid, "finance.edit"):
        try: send_and_auto_delete(cid, "⛔ Редактирование операций запрещено правами этого чата.", 8)
        except Exception: pass
        return False
    return _V152_ORIG_HANDLE_FINANCE_EDIT(msg) if callable(_V152_ORIG_HANDLE_FINANCE_EDIT) else False


_V152_ORIG_HANDLE_GOMONK_INSERT = globals().get("handle_gomonk_insert_message")
def handle_gomonk_insert_message(msg):
    cid = int(msg.chat.id); uid = _v152_actor_id(msg)
    if not _v152_actor_is_platform_owner(uid) and not v152_chat_permission_allowed(cid, "finance.gomonk"):
        try: send_and_auto_delete(cid, "⛔ Изменение гомонковых запрещено правами этого чата.", 8)
        except Exception: pass
        return False
    return _V152_ORIG_HANDLE_GOMONK_INSERT(msg) if callable(_V152_ORIG_HANDLE_GOMONK_INSERT) else False


_V152_ORIG_SCHEDULE_FORWARD = globals().get("schedule_forward_any_message")
def schedule_forward_any_message(chat_id: int, msg):
    cid = int(chat_id); uid = _v152_actor_id(msg)
    capability = "forward.media_groups" if getattr(msg, "media_group_id", None) else "forward.messages"
    if not _v152_actor_is_platform_owner(uid) and not v152_chat_permission_allowed(cid, capability):
        try: bot_journal("chat_permission_forward_blocked", cid, f"user={uid}; capability={capability}", "WARN")
        except Exception: pass
        return None
    return _V152_ORIG_SCHEDULE_FORWARD(cid, msg) if callable(_V152_ORIG_SCHEDULE_FORWARD) else None


# Human-readable filenames for all downloadable journals and operational exports.
class _V152NamedFileProxy:
    def __init__(self, wrapped, name: str):
        self._wrapped = wrapped
        self.name = str(name)
    def __getattr__(self, item):
        return getattr(self._wrapped, item)
    def read(self, *args, **kwargs):
        return self._wrapped.read(*args, **kwargs)
    def seek(self, *args, **kwargs):
        return self._wrapped.seek(*args, **kwargs)
    def tell(self, *args, **kwargs):
        return self._wrapped.tell(*args, **kwargs)
    def __iter__(self):
        return iter(self._wrapped)


def _v152_file_text(document, caption: str = "", purpose: str = "") -> tuple[str, str]:
    name = ""
    try:
        name = str(getattr(document, "name", "") or getattr(document, "file_name", "") or "")
    except Exception:
        pass
    return name, f"{name} {caption or ''} {purpose or ''}".casefold()


def _v152_journal_kind(document, caption: str = "", purpose: str = "") -> str | None:
    _name, low = _v152_file_text(document, caption, purpose)
    if any(x in low for x in ("failed", "problem_tasks", "проблемные задачи")):
        return "Журналы_failed"
    if any(x in low for x in ("runtime", "diagnostic", "diagnostics", "диагност")):
        return "Журнал_диагностики"
    if any(x in low for x in ("error", "ошиб")):
        return "Журнал_ошибок"
    if any(x in low for x in ("recover", "restore", "восстанов")):
        return "Журнал_восстановления"
    if any(x in low for x in ("forward", "fwd", "пересыл")):
        return "Журнал_пересылки"
    if any(x in low for x in ("audit", "integrity", "аудит", "целостност", "command_audit")):
        return "Журнал_аудита"
    if any(x in low for x in ("backup", "snapshot", "sqlite", "бэкап", "резерв")):
        return "Журнал_backup"
    if any(x in low for x in ("finance", "финанс", "csv", "xlsx", "excel", "tabl_lsx", "data_")):
        return "Журнал_финансов"
    if any(x in low for x in ("journal", "log", "журнал")):
        return "Журнал_операций"
    return None


def _v152_filename_component(value: str, fallback: str = "Чат") -> str:
    text = str(value or fallback).strip()
    text = _v152_re.sub(r"[\\/:*?\"<>|]+", "-", text)
    text = _v152_re.sub(r"\s+", "-", text)
    text = _v152_re.sub(r"-+", "-", text).strip("-._")
    return (text or fallback)[:80]


def _v152_scope_name(recipient_chat_id: int, kind: str) -> str:
    try:
        tid = _v152_tenant_id_for_chat(int(recipient_chat_id))
        tenant = _v152_tenant_row(tid)
        if kind in {"Журналы_failed", "Журнал_диагностики", "Журнал_аудита"} and tenant:
            return _v152_filename_component(tenant.get("name") or tid, "Пространство")
    except Exception:
        pass
    try:
        return _v152_filename_component(get_chat_display_name(int(recipient_chat_id)) or f"Чат-{recipient_chat_id}")
    except Exception:
        return _v152_filename_component(f"Чат-{recipient_chat_id}")


def _v152_period_suffix(document, caption: str = "", purpose: str = "") -> str:
    name, _low = _v152_file_text(document, caption, purpose)
    source = f"{name} {caption or ''} {purpose or ''}"
    dates = []
    for y, m, d in _v152_re.findall(r"(?<!\d)(20\d{2})[-_]?([01]\d)[-_]?([0-3]\d)(?!\d)", source):
        value = f"{y}-{m}-{d}"
        if value not in dates:
            dates.append(value)
    if len(dates) >= 2:
        return f"{dates[0]}_{dates[-1]}"
    if len(dates) == 1:
        return dates[0]
    try:
        return now_local().strftime("%Y-%m-%d")
    except Exception:
        return _v152_datetime.now().strftime("%Y-%m-%d")


def v152_human_download_name(recipient_chat_id: int, document, caption: str = "", purpose: str = "") -> str | None:
    kind = _v152_journal_kind(document, caption, purpose)
    if not kind:
        return None
    old_name = str(getattr(document, "name", "") or getattr(document, "file_name", "") or "")
    ext = _v152_os.path.splitext(old_name)[1].lower()
    if ext not in {".txt", ".csv", ".zip", ".json", ".xlsx", ".gz", ".sqlite3"}:
        ext = ".zip" if kind == "Журналы_failed" else ".txt"
    return f"{kind}_{_v152_scope_name(int(recipient_chat_id), kind)}_{_v152_period_suffix(document, caption, purpose)}{ext}"


_V152_ORIG_SEND_DOCUMENT = getattr(bot, "send_document", None)
def _v152_send_document(chat_id, document, *args, **kwargs):
    if not callable(_V152_ORIG_SEND_DOCUMENT):
        raise RuntimeError("send_document unavailable")
    caption = str(kwargs.get("caption") or "")
    purpose = str(kwargs.get("purpose") or "")
    new_name = v152_human_download_name(int(chat_id), document, caption, purpose)
    if new_name:
        try:
            if hasattr(document, "file_name"):
                document.file_name = new_name
            elif hasattr(document, "read"):
                document = _V152NamedFileProxy(document, new_name)
        except Exception:
            pass
    return _V152_ORIG_SEND_DOCUMENT(chat_id, document, *args, **kwargs)

if callable(_V152_ORIG_SEND_DOCUMENT):
    bot.send_document = _v152_send_document


# New callback UI is plugged into the existing early extension hook, before the legacy catch-all.
_V152_ORIG_EXTENSION_CALLBACK = globals().get("v149_extension_callback")
def _v152_answer(call, text: str = "", alert: bool = False):
    try: bot.answer_callback_query(call.id, text or None, show_alert=bool(alert))
    except Exception: pass


def _v152_edit_rights(call, scope: str, target: str | int, page: int = 0):
    safe_edit(bot, call, build_v152_permission_text(scope, target), reply_markup=build_v152_permission_keyboard(scope, target, page))


def _v152_handle_rights_callback(call, data_str: str) -> bool:
    if not str(data_str).startswith("v152:r:"):
        return False
    chat_id = int(call.message.chat.id); user_id = _v152_actor_id(call)
    parts = str(data_str).split(":")
    try:
        action = parts[2]
        if action == "l":
            page = int(parts[3]) if len(parts) > 3 else 0
            if not (_v152_actor_is_platform_owner(user_id) or _v152_actor_can_manage_tenant(user_id, _v152_tenant_id_for_chat(chat_id))):
                _v152_answer(call, "Недостаточно прав", True); return True
            safe_edit(bot, call, build_v152_chat_rights_list_text(user_id, chat_id, page), reply_markup=build_v152_chat_rights_list_keyboard(user_id, chat_id, page))
            return True
        if action == "g":
            if not _v152_actor_is_platform_owner(user_id):
                _v152_answer(call, "Только владелец платформы", True); return True
            _v152_edit_rights(call, "global", "platform", int(parts[3]) if len(parts) > 3 else 0); return True
        if action == "t":
            tenant_id = parts[3]; page = int(parts[4]) if len(parts) > 4 else 0
            if not _v152_actor_can_manage_tenant(user_id, tenant_id):
                _v152_answer(call, "Чужое пространство", True); return True
            _v152_edit_rights(call, "tenant", tenant_id, page); return True
        if action == "c":
            target_chat = int(parts[3]); page = int(parts[4]) if len(parts) > 4 else 0
            if not _v152_actor_can_manage_chat(user_id, target_chat):
                _v152_answer(call, "Чужой чат", True); return True
            _v152_edit_rights(call, "chat", target_chat, page); return True
        if action == "i":
            target_chat = int(parts[3]); page = int(parts[4]) if len(parts) > 4 else 0
            if not _v152_toggle_chat_inheritance(target_chat, user_id):
                _v152_answer(call, "Не удалось изменить наследование", True); return True
            _v152_answer(call, "Наследование изменено")
            _v152_edit_rights(call, "chat", target_chat, page); return True
        if action == "p":
            scope_code, target, preset = parts[3], parts[4], parts[5]
            page = int(parts[6]) if len(parts) > 6 else 0
            scope = {"g": "global", "t": "tenant", "c": "chat"}.get(scope_code)
            real_target = int(target) if scope == "chat" else target
            if not scope or not _v152_apply_preset(scope, real_target, preset, user_id):
                _v152_answer(call, "Пресет недоступен", True); return True
            _v152_answer(call, "Права применены")
            _v152_edit_rights(call, scope, real_target, page); return True
        if action == "x":
            scope_code, target, idx_raw = parts[3], parts[4], parts[5]
            page = int(parts[6]) if len(parts) > 6 else 0
            idx = int(idx_raw)
            if idx < 0 or idx >= len(V152_PERMISSION_KEYS):
                _v152_answer(call, "Неизвестное право", True); return True
            capability = V152_PERMISSION_KEYS[idx]
            scope = {"g": "global", "t": "tenant", "c": "chat"}.get(scope_code)
            real_target = int(target) if scope == "chat" else target
            if scope != "global" and not v152_global_permission_allowed(capability):
                _v152_answer(call, "Функция запрещена владельцем платформы", True); return True
            if scope == "chat" and _v152_chat_policy(int(real_target)).get("inherit_tenant", True):
                _v152_answer(call, "Сначала выключите наследование пространства", True); return True
            current = _v152_scope_value(scope, real_target, capability)
            ok = _v152_set_global(capability, not current, user_id) if scope == "global" else (
                _v152_set_tenant(str(real_target), capability, not current, user_id) if scope == "tenant" else
                _v152_set_chat(int(real_target), capability, not current, user_id)
            )
            if not ok:
                _v152_answer(call, "Недостаточно прав", True); return True
            _v152_answer(call, "Право изменено")
            _v152_edit_rights(call, scope, real_target, page); return True
    except Exception as exc:
        try: log_error(f"v152 rights callback {data_str}: {exc}")
        except Exception: pass
        _v152_answer(call, "Ошибка изменения прав", True)
        return True
    return True


def v149_extension_callback(call, data_str: str) -> bool:
    if _v152_handle_rights_callback(call, data_str):
        return True
    if callable(_V152_ORIG_EXTENSION_CALLBACK):
        return bool(_V152_ORIG_EXTENSION_CALLBACK(call, data_str))
    return False


_V152_WRAPPED_COMMAND_HANDLERS = _v152_install_command_wrappers()
try:
    bot_journal("v152_permissions_installed", int(OWNER_ID or 0), f"command_handlers={_V152_WRAPPED_COMMAND_HANDLERS}; capabilities={len(V152_PERMISSION_KEYS)}")
except Exception:
    pass

# v152_human_journals_chat_rights
