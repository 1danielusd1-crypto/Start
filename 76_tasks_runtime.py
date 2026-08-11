# v180_total_final_diagnostics
# ---- integrated from 119_v172_task_dispatcher.py ----
"""v172: Telegram-native task / purchase dispatcher.

Loaded last on top of v171.  Task state lives in compact root maps so each task is
persisted as an incremental MEGA root-map delta instead of serializing the whole task
registry inside every chat delta.
"""
import re as _v172_re
import secrets as _v172_secrets
import threading as _v172_threading
import time as _v172_time
from datetime import datetime as _v172_datetime, timedelta as _v172_timedelta

VERSION = "bot_v172_task_dispatcher"
V172_FILE_MARKER = "v172_task_dispatcher"

V172_TASKS_KEY = "_tasks_v172"
V172_TASK_SETTINGS_KEY = "_task_settings_v172"
V172_TASK_SOURCE_INDEX_KEY = "_task_source_index_v172"
V172_TASK_SCHEMA = 1
V172_TASK_PAGE_SIZE = 7
V172_HISTORY_KEEP = 100
V172_COMMENT_KEEP = 100

# Root-map delta integration: changing one task produces one root-map patch.
try:
    _DELTA_ROOT_MAP_KEYS.update({V172_TASKS_KEY, V172_TASK_SETTINGS_KEY, V172_TASK_SOURCE_INDEX_KEY})
except Exception:
    pass

try:
    data.setdefault(V172_TASKS_KEY, {})
    data.setdefault(V172_TASK_SETTINGS_KEY, {})
    data.setdefault(V172_TASK_SOURCE_INDEX_KEY, {})
except Exception:
    pass

try:
    WINDOW_MARKER_CONSTANTS.update({
        "v172:task:admin": "Ф242",
        "v172:task:list": "Ф243",
        "v172:task:card": "Ф244",
        "v172:task:history": "Ф245",
        "v172:task:groups": "Ф246",
    })
except Exception:
    pass

_TASK_STATUS = {
    "new": ("🆕", "Новая"),
    "work": ("🔧", "В работе"),
    "wait": ("🟣", "Ждёт"),
    "deferred": ("⏸", "Отложена"),
    "done": ("✅", "Выполнена"),
}
_PURCHASE_STATUS = {
    "need": ("🛒", "Нужно купить"),
    "search": ("🔎", "Ищем"),
    "ordered": ("📦", "Заказано"),
    "bought": ("💰", "Куплено"),
    "received": ("✅", "Получено"),
}
_PRIORITY = {
    "normal": ("🟢", "Обычная"),
    "important": ("🟠", "Важная"),
    "urgent": ("🔴", "Срочная"),
}

_V172_INPUT_LOCK = _v172_threading.RLock()
_V172_INPUT_WAIT = {}
_V172_SEARCH_CACHE = {}


def _v172_now():
    try:
        return now_local()
    except Exception:
        return _v172_datetime.now()


def _v172_iso():
    return _v172_now().isoformat(timespec="seconds")


def _v172_mark(text: str, marker: str) -> str:
    try:
        return window_mark(str(text), str(marker))
    except Exception:
        return str(text).rstrip() + f"\n\n{marker}"


def _v172_user_name(user) -> str:
    if user is None:
        return "неизвестно"
    username = str(getattr(user, "username", "") or "").strip().lstrip("@")
    if username:
        return "@" + username
    first = str(getattr(user, "first_name", "") or "").strip()
    last = str(getattr(user, "last_name", "") or "").strip()
    full = (first + " " + last).strip()
    if full:
        return full
    uid = int(getattr(user, "id", 0) or 0)
    return f"user:{uid}" if uid else "неизвестно"


def _v172_is_manager(user_id: int, chat_id: int) -> bool:
    try:
        uid = int(user_id or 0)
        if uid and uid == int(OWNER_ID or 0):
            return True
        try:
            if uid in {int(x) for x in get_additional_owner_ids()}:
                return True
        except Exception:
            pass
        fn = globals().get("tenant_can_manage")
        if callable(fn):
            for args, kwargs in (((uid,), {"chat_id": int(chat_id)}), ((uid, int(chat_id)), {})):
                try:
                    if fn(*args, **kwargs):
                        return True
                except Exception:
                    continue
    except Exception:
        pass
    return False


def _v172_tasks_root() -> dict:
    return data.setdefault(V172_TASKS_KEY, {})


def _v172_settings_root() -> dict:
    return data.setdefault(V172_TASK_SETTINGS_KEY, {})


def _v172_source_root() -> dict:
    return data.setdefault(V172_TASK_SOURCE_INDEX_KEY, {})


def _v172_chat_settings(chat_id: int) -> dict:
    root = _v172_settings_root()
    key = str(int(chat_id))
    row = root.setdefault(key, {})
    row.setdefault("enabled", False)
    row.setdefault("next_number", 1)
    return row


def task_dispatcher_enabled(chat_id: int) -> bool:
    try:
        return bool(_v172_chat_settings(int(chat_id)).get("enabled", False))
    except Exception:
        return False


def _v172_persist(chat_id: int, reason: str = "task_change") -> None:
    """Persist root state locally immediately and schedule compact MEGA delta."""
    try:
        with data_lock:
            SQLITE.save_root(_sqlite_pack_root(data))
    except Exception as exc:
        try: log_error(f"v172 task SQLite root save: {exc}")
        except Exception: pass
    try:
        schedule_delta_backup(int(chat_id), delay=0.12, reason=f"v172:{reason}")
    except Exception:
        try:
            _mark_global_snapshot_pending()
        except Exception:
            pass


def _v172_source_key(chat_id: int, message_id: int) -> str:
    return f"{int(chat_id)}:{int(message_id)}"


def _v172_new_uid() -> str:
    root = _v172_tasks_root()
    for _ in range(20):
        uid = _v172_secrets.token_hex(5).upper()
        if uid not in root:
            return uid
    return _v172_secrets.token_hex(8).upper()


def _v172_task_for_uid(uid: str):
    row = _v172_tasks_root().get(str(uid or "").upper())
    return row if isinstance(row, dict) else None


def _v172_tasks_for_chat(chat_id: int, include_deleted: bool = False) -> list[dict]:
    cid = int(chat_id)
    out = []
    for row in _v172_tasks_root().values():
        if not isinstance(row, dict) or int(row.get("chat_id", 0) or 0) != cid:
            continue
        if not include_deleted and bool(row.get("deleted", False)):
            continue
        out.append(row)
    return out


def _v172_status_map(task: dict):
    return _PURCHASE_STATUS if str(task.get("type")) == "purchase" else _TASK_STATUS


def _v177_legacy_0327_v172_is_complete(task: dict) -> bool:
    return str(task.get("status")) in ({"received"} if str(task.get("type")) == "purchase" else {"done"})
try: _v177_legacy_0327_v172_is_complete.__name__ = '_v172_is_complete'
except Exception: pass
_v172_is_complete = _v177_legacy_0327_v172_is_complete


def _v172_deadline_dt(task: dict):
    raw = str(task.get("deadline") or "").strip()
    if not raw:
        return None
    try:
        return _v172_datetime.fromisoformat(raw)
    except Exception:
        return None


def _v172_overdue(task: dict) -> bool:
    dt = _v172_deadline_dt(task)
    if not dt or _v172_is_complete(task):
        return False
    try:
        return dt < _v172_now().replace(tzinfo=dt.tzinfo) if dt.tzinfo else dt < _v172_now().replace(tzinfo=None)
    except Exception:
        return False


def _v172_title(text: str) -> str:
    raw = " ".join(str(text or "").strip().split())
    return raw[:110] if raw else "Без названия"


def _v172_history(task: dict, action: str, user_id: int = 0, user_name: str = "", detail: str = "") -> None:
    rows = task.setdefault("history", [])
    rows.append({
        "at": _v172_iso(), "action": str(action), "user_id": int(user_id or 0),
        "user": str(user_name or ""), "detail": str(detail or "")[:600],
    })
    if len(rows) > V172_HISTORY_KEEP:
        del rows[:-V172_HISTORY_KEEP]


def _v172_create_task(chat_id: int, kind: str, text: str, creator, source_msg=None) -> dict:
    cid = int(chat_id)
    kind = "purchase" if str(kind) == "purchase" else "task"
    settings = _v172_chat_settings(cid)
    num = max(1, int(settings.get("next_number", 1) or 1))
    settings["next_number"] = num + 1
    uid = _v172_new_uid()
    source_id = int(getattr(source_msg, "message_id", 0) or 0) if source_msg is not None else 0
    source_user = getattr(source_msg, "from_user", None) if source_msg is not None else None
    source_username = ""
    try:
        source_username = str(getattr(getattr(source_msg, "chat", None), "username", "") or "").lstrip("@")
    except Exception:
        pass
    creator_id = int(getattr(creator, "id", 0) or 0)
    creator_name = _v172_user_name(creator)
    row = {
        "schema": V172_TASK_SCHEMA,
        "uid": uid,
        "number": num,
        "chat_id": cid,
        "type": kind,
        "title": _v172_title(text),
        "description": str(text or "").strip()[:4000],
        "object": "",
        "creator_user_id": creator_id,
        "creator_name": creator_name,
        "source_author_id": int(getattr(source_user, "id", 0) or 0),
        "source_author_name": _v172_user_name(source_user) if source_user else "",
        "source_message_id": source_id,
        "source_chat_username": source_username,
        "assignees": [],
        "status": "need" if kind == "purchase" else "new",
        "priority": "normal",
        "deadline": "",
        "cost": "",
        "comments": [],
        "history": [],
        "created_at": _v172_iso(),
        "updated_at": _v172_iso(),
        "completed_at": "",
        "deleted": False,
    }
    _v172_history(row, "created", creator_id, creator_name, "Покупка" if kind == "purchase" else "Задача")
    _v172_tasks_root()[uid] = row
    if source_id:
        _v172_source_root()[_v172_source_key(cid, source_id)] = uid
    _v172_persist(cid, "task_create")
    try:
        bot_journal("task_v172_created", cid, f"uid={uid} num={num} type={kind} source={source_id}")
    except Exception:
        pass
    return row


def _v172_touch(task: dict, user=None, action: str = "updated", detail: str = "") -> None:
    uid = int(getattr(user, "id", 0) or 0) if user is not None else 0
    name = _v172_user_name(user) if user is not None else "system"
    task["updated_at"] = _v172_iso()
    task["updated_by"] = uid
    _v172_history(task, action, uid, name, detail)
    if _v172_is_complete(task):
        task["completed_at"] = task.get("completed_at") or _v172_iso()
    else:
        task["completed_at"] = ""
    _v172_persist(int(task.get("chat_id", 0) or 0), action)


def _v172_source_url(task: dict) -> str:
    mid = int(task.get("source_message_id", 0) or 0)
    if not mid:
        return ""
    username = str(task.get("source_chat_username") or "").strip().lstrip("@")
    if username:
        return f"https://t.me/{username}/{mid}"
    cid = str(int(task.get("chat_id", 0) or 0))
    if cid.startswith("-100") and len(cid) > 4:
        return f"https://t.me/c/{cid[4:]}/{mid}"
    return ""


def _v172_assignee_text(task: dict) -> str:
    arr = task.get("assignees") or []
    names = [str(x.get("name") or x.get("user_id") or "") for x in arr if isinstance(x, dict)]
    return ", ".join([x for x in names if x]) or "не назначен"


def _v172_deadline_text(task: dict) -> str:
    dt = _v172_deadline_dt(task)
    if not dt:
        return "не указан"
    try:
        value = dt.strftime("%d.%m.%Y %H:%M")
    except Exception:
        value = str(task.get("deadline") or "")
    return ("⏰ ПРОСРОЧЕНО · " if _v172_overdue(task) else "") + value


def _v172_status_text(task: dict) -> str:
    icon, label = _v172_status_map(task).get(str(task.get("status")), ("❔", str(task.get("status") or "—")))
    return f"{icon} {label}"


def _v172_priority_text(task: dict) -> str:
    icon, label = _PRIORITY.get(str(task.get("priority")), _PRIORITY["normal"])
    return f"{icon} {label}"


def _v172_card_text(task: dict) -> str:
    kind = "🛒 ПОКУПКА" if str(task.get("type")) == "purchase" else "📋 ЗАДАЧА"
    lines = [
        f"{kind} №{int(task.get('number', 0) or 0)}",
        "",
        f"📝 {str(task.get('description') or task.get('title') or 'Без названия')[:1800]}",
        "",
        f"🏠 Объект: {str(task.get('object') or 'не указан')}",
        f"👤 Поставил: {str(task.get('creator_name') or task.get('creator_user_id') or '—')}",
        f"👷 Ответственный: {_v172_assignee_text(task)}",
        f"📅 Срок: {_v172_deadline_text(task)}",
        f"⚡ Приоритет: {_v172_priority_text(task)}",
        f"🔧 Статус: {_v172_status_text(task)}",
    ]
    if str(task.get("type")) == "purchase":
        lines.append(f"💵 Стоимость/бюджет: {str(task.get('cost') or '—')}")
    if task.get("source_author_name"):
        lines.append(f"💬 Автор исходного сообщения: {task.get('source_author_name')}")
    if task.get("comments"):
        last = task.get("comments")[-1]
        lines += ["", f"💬 Последний комментарий: {str(last.get('text') or '')[:450]}"]
    lines += ["", f"UID: {task.get('uid')}"]
    return _v172_mark("\n".join(lines), "Ф244")


def _v172_standard_nav(kb, chat_id: int, back_cb: str = "v172:task:list:active:0"):
    day = today_key()
    try:
        day = str(get_chat_store(int(chat_id)).get("current_view_day") or today_key())
    except Exception:
        pass
    kb.row(IB("🔙 Назад", callback_data=back_cb), IB("⬅️ Назад осн. окно", callback_data=f"d:{day}:back_main"))
    kb.row(IB("ℹ️ Описание", callback_data="v171:desc"), IB("❌ Закрыть", callback_data="info_close"))
    kb.row(IB("/iz-mr", callback_data="v160:marker_capture"), IB("/tz", callback_data="v160:tz_capture"))
    return kb


def _v172_card_keyboard(task: dict, viewer_chat_id: int, viewer_user_id: int):
    uid = str(task.get("uid"))
    kb = types.InlineKeyboardMarkup(row_width=2)
    status_map = _v172_status_map(task)
    buttons = []
    for code, (icon, label) in status_map.items():
        prefix = "✅ " if str(task.get("status")) == code else ""
        buttons.append(IB(prefix + icon + " " + label, callback_data=f"v172:task:st:{uid}:{code}"))
    for i in range(0, len(buttons), 2):
        kb.row(*buttons[i:i+2])
    kb.row(
        IB("⚡ Приоритет", callback_data=f"v172:task:prio:{uid}"),
        IB("📅 Срок", callback_data=f"v172:task:input:{uid}:deadline"),
    )
    kb.row(
        IB("👤 Взять себе", callback_data=f"v172:task:take:{uid}"),
        IB("👥 Назначить", callback_data=f"v172:task:input:{uid}:assign"),
    )
    kb.row(
        IB("🏠 Объект", callback_data=f"v172:task:input:{uid}:object"),
        IB("✏️ Текст", callback_data=f"v172:task:input:{uid}:text"),
    )
    if str(task.get("type")) == "purchase":
        kb.row(IB("💰 Стоимость", callback_data=f"v172:task:input:{uid}:cost"))
    kb.row(
        IB("💬 Комментарий", callback_data=f"v172:task:input:{uid}:comment"),
        IB("🕘 История", callback_data=f"v172:task:hist:{uid}"),
    )
    url = _v172_source_url(task)
    if url:
        kb.row(IB("💬 Исходное сообщение", url=url))
    elif int(task.get("source_message_id", 0) or 0):
        kb.row(IB("💬 Исходное сообщение", callback_data=f"v172:task:source:{uid}"))
    if _v172_is_manager(viewer_user_id, int(task.get("chat_id", 0) or 0)) or int(viewer_user_id or 0) == int(task.get("creator_user_id", 0) or 0):
        kb.row(IB("🗑 Удалить", callback_data=f"v172:task:del:{uid}"))
    return _v172_standard_nav(kb, viewer_chat_id, "v172:task:list:active:0")


def _v172_filter_task(task: dict, filt: str, user_id: int = 0) -> bool:
    if bool(task.get("deleted", False)):
        return False
    f = str(filt or "active")
    if f == "active": return not _v172_is_complete(task) and str(task.get("status")) != "deferred"
    if f == "all": return not _v172_is_complete(task)
    if f == "work": return str(task.get("status")) in {"work", "search", "ordered", "bought"}
    if f == "wait": return str(task.get("status")) == "wait"
    if f == "deferred": return str(task.get("status")) == "deferred"
    if f == "urgent": return str(task.get("priority")) == "urgent" and not _v172_is_complete(task)
    if f == "overdue": return _v172_overdue(task)
    if f == "purchase": return str(task.get("type")) == "purchase" and not _v172_is_complete(task)
    if f == "done": return _v172_is_complete(task)
    if f == "mine":
        return any(int(x.get("user_id", 0) or 0) == int(user_id or 0) for x in (task.get("assignees") or []) if isinstance(x, dict)) and not _v172_is_complete(task)
    return True


def _v172_sort_key(task: dict):
    complete = 1 if _v172_is_complete(task) else 0
    overdue = 0 if _v172_overdue(task) else 1
    pri = {"urgent": 0, "important": 1, "normal": 2}.get(str(task.get("priority")), 3)
    deadline = str(task.get("deadline") or "9999-99-99")
    return (complete, overdue, pri, deadline, -int(task.get("number", 0) or 0))


def _v172_list_title(filt: str) -> str:
    return {
        "active": "Все активные", "all": "Невыполненные", "work": "В работе",
        "wait": "Ждут", "deferred": "Отложенные", "urgent": "Срочные",
        "overdue": "Просроченные", "purchase": "Покупки", "done": "Выполненные",
        "mine": "Мои задачи", "search": "Результаты поиска",
    }.get(str(filt), "Задачи")


def _v172_list_rows(chat_id: int, filt: str, user_id: int = 0):
    if str(filt) == "search":
        key = (int(chat_id), int(user_id or 0))
        uids = list((_V172_SEARCH_CACHE.get(key) or {}).get("uids") or [])
        rows = [_v172_task_for_uid(uid) for uid in uids]
        return [x for x in rows if isinstance(x, dict) and not x.get("deleted")]
    rows = [x for x in _v172_tasks_for_chat(chat_id) if _v172_filter_task(x, filt, user_id)]
    rows.sort(key=_v172_sort_key)
    return rows


def _v172_dashboard_counts(chat_id: int) -> dict:
    rows = _v172_tasks_for_chat(chat_id)
    return {
        "active": sum(1 for x in rows if not _v172_is_complete(x) and str(x.get("status")) != "deferred"),
        "urgent": sum(1 for x in rows if str(x.get("priority")) == "urgent" and not _v172_is_complete(x)),
        "overdue": sum(1 for x in rows if _v172_overdue(x)),
        "work": sum(1 for x in rows if str(x.get("status")) in {"work", "search", "ordered", "bought"}),
        "wait": sum(1 for x in rows if str(x.get("status")) == "wait"),
        "deferred": sum(1 for x in rows if str(x.get("status")) == "deferred"),
        "purchase": sum(1 for x in rows if str(x.get("type")) == "purchase" and not _v172_is_complete(x)),
        "done": sum(1 for x in rows if _v172_is_complete(x)),
    }


def _v172_list_text(chat_id: int, filt: str, page: int, user_id: int = 0):
    rows = _v172_list_rows(chat_id, filt, user_id)
    total_pages = max(1, (len(rows) + V172_TASK_PAGE_SIZE - 1) // V172_TASK_PAGE_SIZE)
    page = max(0, min(int(page or 0), total_pages - 1))
    c = _v172_dashboard_counts(chat_id)
    lines = [
        "📋 ДИСПЕТЧЕР ЗАДАЧ",
        f"Чат: {get_chat_display_name(int(chat_id))}",
        "",
        f"Раздел: {_v172_list_title(filt)} · {len(rows)}",
        f"🔴 Срочные {c['urgent']} · ⏰ Просроченные {c['overdue']} · 🔧 В работе {c['work']} · 🛒 Купить {c['purchase']}",
        "",
    ]
    chunk = rows[page * V172_TASK_PAGE_SIZE:(page + 1) * V172_TASK_PAGE_SIZE]
    if not chunk:
        lines.append("Здесь пока нет задач.")
    else:
        for task in chunk:
            icon = "🛒" if str(task.get("type")) == "purchase" else "📋"
            if _v172_overdue(task): icon = "⏰"
            elif str(task.get("priority")) == "urgent": icon = "🔴"
            lines.append(f"{icon} #{task.get('number')} · {_v172_status_text(task)} · {str(task.get('title') or '')[:95]}")
    lines += ["", f"Страница {page + 1}/{total_pages}"]
    return _v172_mark("\n".join(lines), "Ф243"), rows, page, total_pages


def _v172_list_keyboard(chat_id: int, filt: str, page: int, user_id: int = 0):
    text, rows, page, total_pages = _v172_list_text(chat_id, filt, page, user_id)
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.row(IB("➕ Задача", callback_data="v172:task:new:task"), IB("🛒 Покупка", callback_data="v172:task:new:purchase"))
    kb.row(IB("📋 Активные", callback_data="v172:task:list:active:0"), IB("👤 Мои", callback_data="v172:task:list:mine:0"))
    kb.row(IB("🔴 Срочные", callback_data="v172:task:list:urgent:0"), IB("⏰ Просроченные", callback_data="v172:task:list:overdue:0"))
    kb.row(IB("🔧 В работе", callback_data="v172:task:list:work:0"), IB("🟣 Ждут", callback_data="v172:task:list:wait:0"))
    kb.row(IB("⏸ Отложенные", callback_data="v172:task:list:deferred:0"), IB("🛒 Покупки", callback_data="v172:task:list:purchase:0"))
    kb.row(IB("🏠 Объекты", callback_data="v172:task:groups:object:0"), IB("👥 Исполнители", callback_data="v172:task:groups:assignee:0"))
    kb.row(IB("✅ Выполненные", callback_data="v172:task:list:done:0"), IB("🔎 Поиск", callback_data="v172:task:search"))
    chunk = rows[page * V172_TASK_PAGE_SIZE:(page + 1) * V172_TASK_PAGE_SIZE]
    for task in chunk:
        icon = "⏰" if _v172_overdue(task) else ("🔴" if str(task.get("priority")) == "urgent" else ("🛒" if str(task.get("type")) == "purchase" else "📋"))
        kb.row(IB(f"{icon} #{task.get('number')} {str(task.get('title') or '')[:38]}", callback_data=f"v172:task:open:{task.get('uid')}"))
    if total_pages > 1:
        prevp = (page - 1) % total_pages
        nextp = (page + 1) % total_pages
        kb.row(IB("◀️", callback_data=f"v172:task:list:{filt}:{prevp}"), IB(f"{page+1}/{total_pages}", callback_data="none"), IB("▶️", callback_data=f"v172:task:list:{filt}:{nextp}"))
    return text, _v172_standard_nav(kb, chat_id, f"v172:task:list:{filt}:{page}")


def _v172_group_values(chat_id: int, kind: str):
    vals = {}
    for task in _v172_tasks_for_chat(chat_id):
        if _v172_is_complete(task):
            continue
        if kind == "object":
            value = str(task.get("object") or "").strip()
            if value:
                vals[value] = vals.get(value, 0) + 1
        else:
            for a in task.get("assignees") or []:
                if isinstance(a, dict):
                    value = str(a.get("name") or a.get("user_id") or "").strip()
                    if value:
                        vals[value] = vals.get(value, 0) + 1
    return sorted(vals.items(), key=lambda x: (-x[1], x[0].casefold()))


def _v172_value_token(value: str) -> str:
    import hashlib
    return hashlib.sha1(str(value).encode("utf-8")).hexdigest()[:10]


def _v172_groups_text_kb(chat_id: int, kind: str, page: int):
    vals = _v172_group_values(chat_id, kind)
    per = 10
    pages = max(1, (len(vals)+per-1)//per)
    page = max(0, min(int(page or 0), pages-1))
    title = "🏠 ОБЪЕКТЫ" if kind == "object" else "👥 ИСПОЛНИТЕЛИ"
    lines = [title, f"Чат: {get_chat_display_name(chat_id)}", "", "Выберите раздел:"]
    kb = types.InlineKeyboardMarkup()
    for value, count in vals[page*per:(page+1)*per]:
        token = _v172_value_token(value)
        kb.row(IB(f"{value[:44]} · {count}", callback_data=f"v172:task:groupopen:{kind}:{token}:0"))
    if not vals:
        lines.append("Пока нет заполненных данных.")
    if pages > 1:
        kb.row(IB("◀️", callback_data=f"v172:task:groups:{kind}:{(page-1)%pages}"), IB(f"{page+1}/{pages}", callback_data="none"), IB("▶️", callback_data=f"v172:task:groups:{kind}:{(page+1)%pages}"))
    return _v172_mark("\n".join(lines), "Ф246"), _v172_standard_nav(kb, chat_id, "v172:task:list:active:0")


def _v172_group_tasks(chat_id: int, kind: str, token: str):
    value = ""
    for v, _count in _v172_group_values(chat_id, kind):
        if _v172_value_token(v) == token:
            value = v; break
    if not value:
        return "", []
    rows = []
    for task in _v172_tasks_for_chat(chat_id):
        if _v172_is_complete(task): continue
        if kind == "object" and str(task.get("object") or "").strip() == value:
            rows.append(task)
        elif kind == "assignee" and any(str(a.get("name") or a.get("user_id") or "").strip() == value for a in (task.get("assignees") or []) if isinstance(a, dict)):
            rows.append(task)
    rows.sort(key=_v172_sort_key)
    return value, rows


def _v172_history_text(task: dict):
    lines = [f"🕘 ИСТОРИЯ ЗАДАЧИ #{task.get('number')}", ""]
    rows = list(task.get("history") or [])[-30:]
    for h in rows:
        at = str(h.get("at") or "").replace("T", " ")[:16]
        user = str(h.get("user") or h.get("user_id") or "system")
        action = str(h.get("action") or "изменено")
        detail = str(h.get("detail") or "")
        lines.append(f"• {at} · {user} · {action}" + (f" — {detail}" if detail else ""))
    if not rows: lines.append("История пуста.")
    return _v172_mark("\n".join(lines)[:3900], "Ф245")


def _v172_admin_text(page: int = 0):
    all_ids = []
    try:
        all_ids = list(collect_all_known_chat_ids(include_owner=True))
    except Exception:
        all_ids = [int(OWNER_ID)] if str(OWNER_ID or "").lstrip("-").isdigit() else []
    ids = sorted({int(x) for x in all_ids}, key=lambda c: str(get_chat_display_name(c)).casefold())
    per = 12
    pages = max(1, (len(ids)+per-1)//per)
    page = max(0, min(int(page or 0), pages-1))
    enabled = sum(1 for cid in ids if task_dispatcher_enabled(cid))
    text = _v172_mark(
        "📋 ДИСПЕТЧЕР ЗАДАЧ — ЧАТЫ\n\n"
        "Включите диспетчер только там, где нужно фиксировать работы и покупки.\n"
        "Задачи каждого чата хранятся отдельно и не смешиваются с другими чатами/контурами.\n\n"
        f"Чатов: {len(ids)} · включено: {enabled}\nСтраница {page+1}/{pages}",
        "Ф242"
    )
    kb = types.InlineKeyboardMarkup()
    for cid in ids[page*per:(page+1)*per]:
        flag = "✅" if task_dispatcher_enabled(cid) else "❌"
        kb.row(IB(f"{flag} {get_chat_display_name(cid)[:42]}", callback_data=f"v172:task:toggle:{cid}:{page}"))
    if pages > 1:
        kb.row(IB("◀️", callback_data=f"v172:task:admin:{(page-1)%pages}"), IB(f"{page+1}/{pages}", callback_data="none"), IB("▶️", callback_data=f"v172:task:admin:{(page+1)%pages}"))
    owner_chat = int(OWNER_ID or 0)
    return text, _v172_standard_nav(kb, owner_chat, f"d:{today_key()}:back_main")


def _v172_set_input(chat_id: int, user_id: int, action: str, uid: str = "", message_id: int = 0, prompt_id: int = 0):
    with _V172_INPUT_LOCK:
        _V172_INPUT_WAIT[(int(chat_id), int(user_id))] = {
            "action": str(action), "uid": str(uid or "").upper(), "message_id": int(message_id or 0),
            "prompt_id": int(prompt_id or 0), "created": _v172_time.time(),
        }


def _v172_get_input(chat_id: int, user_id: int):
    key = (int(chat_id), int(user_id))
    with _V172_INPUT_LOCK:
        row = _V172_INPUT_WAIT.get(key)
        if row and _v172_time.time() - float(row.get("created", 0) or 0) > 900:
            _V172_INPUT_WAIT.pop(key, None); row = None
        return dict(row) if row else None


def _v172_clear_input(chat_id: int, user_id: int):
    with _V172_INPUT_LOCK:
        return _V172_INPUT_WAIT.pop((int(chat_id), int(user_id)), None)


def _v172_parse_deadline(text: str):
    s = str(text or "").strip().lower()
    if s in {"нет", "убрать", "очистить", "-", "без срока"}:
        return ""
    now = _v172_now()
    m = _v172_re.fullmatch(r"(сегодня|завтра)\s+(\d{1,2}):(\d{2})", s)
    if m:
        base = now if m.group(1) == "сегодня" else now + _v172_timedelta(days=1)
        return base.replace(hour=int(m.group(2)), minute=int(m.group(3)), second=0, microsecond=0).isoformat(timespec="minutes")
    formats = ("%d.%m.%Y %H:%M", "%d.%m.%y %H:%M", "%d.%m %H:%M", "%Y-%m-%d %H:%M")
    for fmt in formats:
        try:
            dt = _v172_datetime.strptime(s, fmt)
            if fmt == "%d.%m %H:%M":
                dt = dt.replace(year=now.year)
                if dt < now.replace(tzinfo=None) - _v172_timedelta(days=2):
                    dt = dt.replace(year=now.year + 1)
            if getattr(now, "tzinfo", None) is not None and dt.tzinfo is None:
                dt = dt.replace(tzinfo=now.tzinfo)
            return dt.isoformat(timespec="minutes")
        except Exception:
            continue
    return None


def _v172_prompt(chat_id: int, user_id: int, action: str, uid: str = "", message_id: int = 0):
    prompts = {
        "new_task": "✍️ Напишите текст новой задачи. Для отмены: отмена",
        "new_purchase": "🛒 Напишите, что нужно купить. Для отмены: отмена",
        "text": "✏️ Напишите новый текст задачи. Для отмены: отмена",
        "object": "🏠 Напишите объект/дом/зону. Например: Дом №2, бассейн, сад. «нет» — очистить.",
        "deadline": "📅 Введите срок: 12.08.2026 18:00, 12.08 18:00, сегодня 18:00, завтра 15:00. «нет» — убрать срок.",
        "assign": "👥 Напишите ответственных через запятую: Daniel, Juan, @maria. «нет» — снять всех.",
        "comment": "💬 Напишите комментарий к задаче. Для отмены: отмена",
        "cost": "💰 Напишите стоимость/бюджет свободным текстом, например: 250 000 ARS или 180 USD. «нет» — очистить.",
        "search": "🔎 Напишите слово или фразу для поиска по тексту, объекту и исполнителям.",
    }
    try:
        sent = bot.send_message(int(chat_id), prompts.get(action, "Введите значение:"))
        _v172_set_input(chat_id, user_id, action, uid, message_id, int(getattr(sent, "message_id", 0) or 0))
    except Exception:
        _v172_set_input(chat_id, user_id, action, uid, message_id, 0)


def _v172_delete_quiet(chat_id: int, message_id: int):
    if not message_id: return
    try: bot.delete_message(int(chat_id), int(message_id))
    except Exception: pass


def _v172_refresh_card(chat_id: int, message_id: int, task: dict, user_id: int):
    try:
        bot.edit_message_text(_v172_card_text(task), chat_id=int(chat_id), message_id=int(message_id), reply_markup=_v172_card_keyboard(task, chat_id, user_id))
    except Exception:
        pass


def _v172_task_message_input(msg) -> bool:
    if getattr(msg, "content_type", "") != "text":
        return False
    cid = int(msg.chat.id)
    uid_user = int(getattr(getattr(msg, "from_user", None), "id", 0) or 0)
    wait = _v172_get_input(cid, uid_user)
    if not wait:
        return False
    text = str(getattr(msg, "text", "") or "").strip()
    if text.lower() in {"отмена", "cancel", "стоп"}:
        old = _v172_clear_input(cid, uid_user) or wait
        _v172_delete_quiet(cid, int(old.get("prompt_id", 0) or 0))
        _v172_delete_quiet(cid, int(getattr(msg, "message_id", 0) or 0))
        try: send_and_auto_delete(cid, "❎ Действие отменено.", 6)
        except Exception: pass
        return True
    action = str(wait.get("action") or "")
    task = _v172_task_for_uid(wait.get("uid")) if wait.get("uid") else None
    if task is not None and int(task.get("chat_id", 0) or 0) != cid:
        _v172_clear_input(cid, uid_user); return True
    user = getattr(msg, "from_user", None)
    ok = True
    notice = "✅ Сохранено."
    if action in {"new_task", "new_purchase"}:
        if not task_dispatcher_enabled(cid):
            notice = "❌ Диспетчер задач в этом чате выключен."; ok = False
        elif not text:
            notice = "❌ Текст пустой."; ok = False
        else:
            task = _v172_create_task(cid, "purchase" if action == "new_purchase" else "task", text, user, source_msg=None)
            notice = f"✅ Создана {'покупка' if action == 'new_purchase' else 'задача'} #{task.get('number')}."
    elif not isinstance(task, dict):
        notice = "❌ Задача не найдена."; ok = False
    elif action == "text":
        if not (_v172_is_manager(uid_user, cid) or uid_user == int(task.get("creator_user_id", 0) or 0)):
            notice = "⛔ Изменять текст может автор или управляющий."; ok = False
        else:
            task["description"] = text[:4000]; task["title"] = _v172_title(text); _v172_touch(task, user, "text_changed", text[:180])
    elif action == "object":
        value = "" if text.lower() in {"нет", "убрать", "очистить", "-"} else text[:180]
        task["object"] = value; _v172_touch(task, user, "object_changed", value or "очищено")
    elif action == "deadline":
        value = _v172_parse_deadline(text)
        if value is None:
            notice = "❌ Срок не распознан. Пример: 12.08 18:00 или завтра 15:00."; ok = False
        else:
            task["deadline"] = value; _v172_touch(task, user, "deadline_changed", value or "срок убран")
    elif action == "assign":
        if not (_v172_is_manager(uid_user, cid) or uid_user == int(task.get("creator_user_id", 0) or 0)):
            notice = "⛔ Назначать других может автор или управляющий."; ok = False
        else:
            if text.lower() in {"нет", "убрать", "очистить", "-"}:
                task["assignees"] = []
            else:
                names = [x.strip() for x in text.split(",") if x.strip()][:10]
                task["assignees"] = [{"user_id": 0, "name": x[:100]} for x in names]
            _v172_touch(task, user, "assignees_changed", _v172_assignee_text(task))
    elif action == "comment":
        arr = task.setdefault("comments", [])
        arr.append({"at": _v172_iso(), "user_id": uid_user, "user": _v172_user_name(user), "text": text[:1200]})
        if len(arr) > V172_COMMENT_KEEP: del arr[:-V172_COMMENT_KEEP]
        _v172_touch(task, user, "comment_added", text[:220])
    elif action == "cost":
        value = "" if text.lower() in {"нет", "убрать", "очистить", "-"} else text[:120]
        task["cost"] = value; _v172_touch(task, user, "cost_changed", value or "очищено")
    elif action == "search":
        q = text.casefold()
        hits = []
        for row in _v172_tasks_for_chat(cid):
            hay = " ".join([
                str(row.get("title") or ""), str(row.get("description") or ""), str(row.get("object") or ""), _v172_assignee_text(row)
            ]).casefold()
            if q in hay: hits.append(row)
        hits.sort(key=_v172_sort_key)
        _V172_SEARCH_CACHE[(cid, uid_user)] = {"query": text[:200], "uids": [x.get("uid") for x in hits], "at": _v172_time.time()}
        notice = f"🔎 Найдено: {len(hits)}"
    old = _v172_clear_input(cid, uid_user) or wait
    _v172_delete_quiet(cid, int(old.get("prompt_id", 0) or 0))
    _v172_delete_quiet(cid, int(getattr(msg, "message_id", 0) or 0))
    if not ok:
        try: send_and_auto_delete(cid, notice, 10)
        except Exception: pass
        # keep deadline input retry only for parse error
        if action == "deadline" and isinstance(task, dict):
            _v172_prompt(cid, uid_user, "deadline", task.get("uid"), int(wait.get("message_id", 0) or 0))
        return True
    mid = int(wait.get("message_id", 0) or 0)
    if action == "search":
        text2, kb2 = _v172_list_keyboard(cid, "search", 0, uid_user)
        if mid:
            try: bot.edit_message_text(text2, chat_id=cid, message_id=mid, reply_markup=kb2)
            except Exception: bot.send_message(cid, text2, reply_markup=kb2)
        else:
            bot.send_message(cid, text2, reply_markup=kb2)
    elif isinstance(task, dict):
        if mid: _v172_refresh_card(cid, mid, task, uid_user)
        elif action in {"new_task", "new_purchase"}:
            bot.send_message(cid, _v172_card_text(task), reply_markup=_v172_card_keyboard(task, cid, uid_user))
    try: send_and_auto_delete(cid, notice, 6)
    except Exception: pass
    return True


# Wrap the already-registered generic non-command message handler so task input wins
# before finance parsing / forwarding.
# v180 retired function removed: _v172_install_message_input_wrapper
def _v172_command_text(msg) -> str:
    raw = str(getattr(msg, "text", "") or "")
    parts = raw.split(None, 1)
    return parts[1].strip() if len(parts) > 1 else ""


def _v172_reply_text(msg) -> str:
    src = getattr(msg, "reply_to_message", None)
    if src is None: return ""
    text = str(getattr(src, "text", "") or getattr(src, "caption", "") or "").strip()
    if text: return text
    ct = str(getattr(src, "content_type", "") or "сообщение")
    return f"{ct}: вложение из исходного сообщения"


def _v172_command_create(msg, kind: str):
    cid = int(msg.chat.id)
    if not task_dispatcher_enabled(cid):
        try: bot.reply_to(msg, "📋 Диспетчер задач в этом чате выключен. Владелец может включить его в основном окне.")
        except Exception: pass
        return
    text = _v172_command_text(msg)
    src = getattr(msg, "reply_to_message", None)
    if not text and src is not None:
        text = _v172_reply_text(msg)
    if not text:
        action = "new_purchase" if kind == "purchase" else "new_task"
        _v172_prompt(cid, int(getattr(msg.from_user, "id", 0) or 0), action)
        return
    if src is not None:
        skey = _v172_source_key(cid, int(getattr(src, "message_id", 0) or 0))
        existing_uid = _v172_source_root().get(skey)
        existing = _v172_task_for_uid(existing_uid) if existing_uid else None
        if isinstance(existing, dict) and not existing.get("deleted"):
            try:
                bot.reply_to(msg, f"ℹ️ Это сообщение уже зафиксировано как #{existing.get('number')}.", reply_markup=_v172_card_keyboard(existing, cid, int(getattr(msg.from_user,'id',0) or 0)))
            except Exception: pass
            return
    task = _v172_create_task(cid, kind, text, msg.from_user, source_msg=src)
    try:
        sent = bot.send_message(cid, _v172_card_text(task), reply_markup=_v172_card_keyboard(task, cid, int(getattr(msg.from_user,'id',0) or 0)), reply_to_message_id=int(getattr(src, "message_id", 0) or 0) or None)
        task["card_message_id"] = int(getattr(sent, "message_id", 0) or 0)
        _v172_persist(cid, "task_card_link")
    except Exception:
        try: bot.send_message(cid, _v172_card_text(task), reply_markup=_v172_card_keyboard(task, cid, int(getattr(msg.from_user,'id',0) or 0)))
        except Exception: pass


@bot.message_handler(commands=["tasks", "задачи"])
def cmd_tasks_v172(msg):
    cid = int(msg.chat.id)
    uid = int(getattr(msg.from_user, "id", 0) or 0)
    if cid == int(OWNER_ID or 0) and not task_dispatcher_enabled(cid):
        text, kb = _v172_admin_text(0)
        bot.send_message(cid, text, reply_markup=kb)
        return
    if not task_dispatcher_enabled(cid):
        bot.reply_to(msg, "📋 Диспетчер задач в этом чате выключен.")
        return
    text, kb = _v172_list_keyboard(cid, "active", 0, uid)
    bot.send_message(cid, text, reply_markup=kb)


@bot.message_handler(commands=["task", "задача"])
def cmd_task_v172(msg):
    _v172_command_create(msg, "task")


@bot.message_handler(commands=["buy", "покупка"])
def cmd_buy_v172(msg):
    _v172_command_create(msg, "purchase")


@bot.message_handler(commands=["task_cancel", "задачи_отмена"])
def cmd_task_cancel_v172(msg):
    row = _v172_clear_input(int(msg.chat.id), int(getattr(msg.from_user, "id", 0) or 0))
    if row:
        _v172_delete_quiet(int(msg.chat.id), int(row.get("prompt_id", 0) or 0))
    try: send_and_auto_delete(int(msg.chat.id), "❎ Ввод задачи отменён.", 6)
    except Exception: pass


def _v172_edit_call(call, text, kb):
    try:
        safe_edit(bot, call, text, reply_markup=kb)
    except Exception:
        try:
            fast_ui_edit_message_text(
                int(call.message.chat.id), int(call.message.message_id), text,
                reply_markup=kb, purpose="task_callback_fallback_v178",
            )
        except Exception:
            pass


def _v172_callback(call):
    raw = str(getattr(call, "data", "") or "")
    try:
        resolver = globals().get("resolve_short_callback")
        if callable(resolver): raw = str(resolver(raw) or raw)
    except Exception: pass
    if not raw.startswith("v172:task:"):
        return False
    viewer_chat = int(call.message.chat.id)
    user = getattr(call, "from_user", None)
    user_id = int(getattr(user, "id", 0) or 0)
    parts = raw.split(":")
    action = parts[2] if len(parts) > 2 else ""
    try: bot.answer_callback_query(call.id)
    except Exception: pass

    if action == "admin":
        if viewer_chat != int(OWNER_ID or 0) and not _v172_is_manager(user_id, viewer_chat):
            return True
        page = int(parts[3]) if len(parts) > 3 and parts[3].lstrip("-").isdigit() else 0
        text, kb = _v172_admin_text(page); _v172_edit_call(call, text, kb); return True

    if action == "toggle":
        if viewer_chat != int(OWNER_ID or 0): return True
        try: target = int(parts[3]); page = int(parts[4]) if len(parts) > 4 else 0
        except Exception: return True
        s = _v172_chat_settings(target); s["enabled"] = not bool(s.get("enabled")); s["updated_at"] = _v172_iso(); s["updated_by"] = user_id
        _v172_persist(target, "dispatcher_toggle")
        try:
            note = "✅ Диспетчер задач включён. Используйте /tasks, /task и /buy." if s["enabled"] else "⏸ Диспетчер задач выключен владельцем. Существующие задачи сохранены."
            bot.send_message(target, note)
        except Exception: pass
        try: schedule_main_window_recreate_after_quiet(target, delay=0.2)
        except Exception: pass
        text, kb = _v172_admin_text(page); _v172_edit_call(call, text, kb); return True

    # Remaining callbacks operate on the current work chat.
    if not task_dispatcher_enabled(viewer_chat):
        try: bot.answer_callback_query(call.id, "Диспетчер в этом чате выключен.", show_alert=True)
        except Exception: pass
        return True

    if action == "list":
        filt = parts[3] if len(parts) > 3 else "active"
        try: page = int(parts[4]) if len(parts) > 4 else 0
        except Exception: page = 0
        text, kb = _v172_list_keyboard(viewer_chat, filt, page, user_id); _v172_edit_call(call, text, kb); return True

    if action == "new":
        kind = parts[3] if len(parts) > 3 else "task"
        _v172_prompt(viewer_chat, user_id, "new_purchase" if kind == "purchase" else "new_task", message_id=int(call.message.message_id)); return True

    if action == "search":
        _v172_prompt(viewer_chat, user_id, "search", message_id=int(call.message.message_id)); return True

    if action == "groups":
        kind = parts[3] if len(parts) > 3 else "object"
        try: page = int(parts[4]) if len(parts)>4 else 0
        except Exception: page = 0
        text, kb = _v172_groups_text_kb(viewer_chat, kind, page); _v172_edit_call(call, text, kb); return True

    if action == "groupopen":
        if len(parts) < 6: return True
        kind, token = parts[3], parts[4]
        try: page = int(parts[5])
        except Exception: page = 0
        value, rows = _v172_group_tasks(viewer_chat, kind, token)
        per = V172_TASK_PAGE_SIZE; pages=max(1,(len(rows)+per-1)//per); page=max(0,min(page,pages-1))
        title = ("🏠 " if kind=="object" else "👤 ") + (value or "Не найдено")
        text = _v172_mark(f"{title}\n\nЗадач: {len(rows)}\nСтраница {page+1}/{pages}", "Ф246")
        kb = types.InlineKeyboardMarkup()
        for task in rows[page*per:(page+1)*per]:
            kb.row(IB(f"#{task.get('number')} {str(task.get('title') or '')[:42]}", callback_data=f"v172:task:open:{task.get('uid')}"))
        if pages>1:
            kb.row(IB("◀️", callback_data=f"v172:task:groupopen:{kind}:{token}:{(page-1)%pages}"), IB(f"{page+1}/{pages}", callback_data="none"), IB("▶️", callback_data=f"v172:task:groupopen:{kind}:{token}:{(page+1)%pages}"))
        _v172_standard_nav(kb, viewer_chat, f"v172:task:groups:{kind}:0")
        _v172_edit_call(call, text, kb); return True

    uid = parts[3].upper() if len(parts) > 3 else ""
    task = _v172_task_for_uid(uid)
    if not isinstance(task, dict) or int(task.get("chat_id", 0) or 0) != viewer_chat or task.get("deleted"):
        try: bot.answer_callback_query(call.id, "Задача не найдена или уже удалена.", show_alert=True)
        except Exception: pass
        return True

    if action == "open":
        _v172_edit_call(call, _v172_card_text(task), _v172_card_keyboard(task, viewer_chat, user_id)); return True

    if action == "st":
        status = parts[4] if len(parts)>4 else ""
        if status in _v172_status_map(task):
            task["status"] = status; _v172_touch(task, user, "status_changed", _v172_status_text(task))
            _v172_edit_call(call, _v172_card_text(task), _v172_card_keyboard(task, viewer_chat, user_id))
        return True

    if action == "prio":
        order = ["normal", "important", "urgent"]
        cur = str(task.get("priority") or "normal")
        task["priority"] = order[(order.index(cur) + 1) % len(order)] if cur in order else "normal"
        _v172_touch(task, user, "priority_changed", _v172_priority_text(task))
        _v172_edit_call(call, _v172_card_text(task), _v172_card_keyboard(task, viewer_chat, user_id)); return True

    if action == "take":
        arr = [x for x in (task.get("assignees") or []) if isinstance(x, dict)]
        existing = next((x for x in arr if int(x.get("user_id",0) or 0) == user_id and user_id), None)
        if existing:
            arr = [x for x in arr if int(x.get("user_id",0) or 0) != user_id]
            detail = "снял себя"
        else:
            arr.append({"user_id": user_id, "name": _v172_user_name(user)})
            detail = "взял себе"
            if str(task.get("type")) == "task" and str(task.get("status")) == "new": task["status"] = "work"
            if str(task.get("type")) == "purchase" and str(task.get("status")) == "need": task["status"] = "search"
        task["assignees"] = arr[:10]; _v172_touch(task, user, "assignee_self", detail)
        _v172_edit_call(call, _v172_card_text(task), _v172_card_keyboard(task, viewer_chat, user_id)); return True

    if action == "input":
        # callback format v172:task:input:UID:field
        field = parts[4] if len(parts) > 4 else ""
        if field in {"text","assign"} and not (_v172_is_manager(user_id, viewer_chat) or user_id == int(task.get("creator_user_id",0) or 0)):
            try: bot.answer_callback_query(call.id, "Это может изменить автор или управляющий.", show_alert=True)
            except Exception: pass
            return True
        if field in {"text","object","deadline","assign","comment","cost"}:
            _v172_prompt(viewer_chat, user_id, field, uid, int(call.message.message_id)); return True

    if action == "hist":
        kb = types.InlineKeyboardMarkup(); _v172_standard_nav(kb, viewer_chat, f"v172:task:open:{uid}")
        _v172_edit_call(call, _v172_history_text(task), kb); return True

    if action == "source":
        try: bot.answer_callback_query(call.id, f"Исходное сообщение ID: {task.get('source_message_id')}. Прямая ссылка для этого типа чата недоступна.", show_alert=True)
        except Exception: pass
        return True

    if action == "del":
        if not (_v172_is_manager(user_id, viewer_chat) or user_id == int(task.get("creator_user_id", 0) or 0)):
            try: bot.answer_callback_query(call.id, "Удалить может автор или управляющий.", show_alert=True)
            except Exception: pass
            return True
        kb = types.InlineKeyboardMarkup(); kb.row(IB("✅ Да, удалить", callback_data=f"v172:task:delok:{uid}"), IB("❌ Нет", callback_data=f"v172:task:open:{uid}")); _v172_standard_nav(kb, viewer_chat, f"v172:task:open:{uid}")
        _v172_edit_call(call, _v172_mark(f"🗑 Удалить задачу #{task.get('number')}?\n\n{task.get('title')}", "Ф244"), kb); return True

    if action == "delok":
        if not (_v172_is_manager(user_id, viewer_chat) or user_id == int(task.get("creator_user_id", 0) or 0)):
            return True
        task["deleted"] = True; _v172_touch(task, user, "deleted", "soft delete")
        text, kb = _v172_list_keyboard(viewer_chat, "active", 0, user_id); _v172_edit_call(call, text, kb); return True

    return True


# v180 retired function removed: _v172_register_callback
_V172_PREV_BUILD_MAIN_KEYBOARD = globals().get("build_main_keyboard")

def _v177_legacy_0166_build_main_keyboard(day_key: str, chat_id=None):
    kb = _V172_PREV_BUILD_MAIN_KEYBOARD(day_key, chat_id) if callable(_V172_PREV_BUILD_MAIN_KEYBOARD) else types.InlineKeyboardMarkup()
    try:
        cid = int(chat_id) if chat_id is not None else 0
        rows = list(getattr(kb, "keyboard", None) or getattr(kb, "inline_keyboard", None) or [])
        callbacks = {str(getattr(b, "callback_data", "") or "") for row in rows for b in (row or [])}
        inserts = []
        if cid and task_dispatcher_enabled(cid) and "v172:task:list:active:0" not in callbacks:
            inserts.append(IB("📋 Задачи", callback_data="v172:task:list:active:0"))
        if cid and cid == int(OWNER_ID or 0) and "v172:task:admin:0" not in callbacks:
            inserts.append(IB("📋 Диспетчер задач", callback_data="v172:task:admin:0"))
        if inserts:
            # Place before the final Close row.
            idx = len(rows)
            for i, row in enumerate(rows):
                if any("закры" in str(getattr(b, "text", "") or "").casefold() for b in (row or [])):
                    idx = i; break
            rows.insert(idx, inserts)
            try: kb.keyboard = rows
            except Exception:
                try: kb.inline_keyboard = rows
                except Exception: pass
    except Exception as exc:
        try: log_error(f"v172 main keyboard: {exc}")
        except Exception: pass
    return kb
try: _v177_legacy_0166_build_main_keyboard.__name__ = 'build_main_keyboard'
except Exception: pass
build_main_keyboard = _v177_legacy_0166_build_main_keyboard


# Restore validator accepts v172 backups too.
# v180 historical restore validator removed: _v177_legacy_0289_v153_validate_restore_gz; one FINAL validator lives in 85_runtime_control.py


# READY-time migration after MEGA restore.
_V172_PREV_RUNTIME_MARK_READY = globals().get("runtime_mark_ready")
if callable(_V172_PREV_RUNTIME_MARK_READY):
    def runtime_mark_ready(detail: str = ""):
        result = _V172_PREV_RUNTIME_MARK_READY(detail)
        try:
            data.setdefault(V172_TASKS_KEY, {}); data.setdefault(V172_TASK_SETTINGS_KEY, {}); data.setdefault(V172_TASK_SOURCE_INDEX_KEY, {})
            _DELTA_ROOT_MAP_KEYS.update({V172_TASKS_KEY, V172_TASK_SETTINGS_KEY, V172_TASK_SOURCE_INDEX_KEY})
            bot_journal("v172_task_dispatcher_ready", int(OWNER_ID or 0), f"tasks={len(_v172_tasks_root())}; enabled={sum(1 for x in _v172_settings_root().values() if isinstance(x,dict) and x.get('enabled'))}")
        except Exception: pass
        return result


_V172_MESSAGE_WRAPPERS = 0  # v180: merged into canonical on_any_message
_V172_CALLBACK_HANDLERS = 0  # v179 merged task dispatcher
try:
    bot_journal("v172_installed", int(OWNER_ID or 0), f"task_dispatcher=1; callback={_V172_CALLBACK_HANDLERS}; message_wrap={_V172_MESSAGE_WRAPPERS}; root_delta_maps=3")
except Exception:
    pass

# ---- integrated from 120_v173_reminder_crosschat_unique_journals.py ----
"""v173: reliable owner cross-chat reminders + unmistakable unique journal filenames.

Loaded after v172.  The platform owner may explicitly select any chat from the reminder
picker; second-circle tenants remain isolated.  Operational journal downloads get a
human type name plus an export timestamp/sequence so Telegram never has to append (1),
(2), etc. to two different downloads from the same day.
"""
import os as _v173_os
import re as _v173_re
import threading as _v173_threading
import time as _v173_time
from datetime import datetime as _v173_datetime

VERSION = "bot_v173_reminder_crosschat_unique_journals"
V173_FILE_MARKER = "v173_reminder_crosschat_unique_journals"

# ---------------------------------------------------------------------------
# 1) Reminder delivery policy.
# Root cause seen in v171 journal: v171 checked an explicitly selected platform-owner
# target against collect_all_known_chat_ids().  v148 had already replaced that helper
# with a *current-tenant-scoped* list, therefore first/second-circle target chats were
# rejected with tenant_reminder_cross_chat_blocked even though the owner selected them.
# ---------------------------------------------------------------------------
_V173_PREV_REMINDER_CHAT_ALLOWED = globals().get("_v149_reminder_chat_allowed")


def _v173_reminder_selected_chat_ids(cfg: dict) -> set[int]:
    out = set()
    for raw in ((cfg or {}).get("chat_ids") or []):
        try:
            out.add(int(raw))
        except Exception:
            continue
    return out


def _v149_reminder_chat_allowed(cfg: dict, chat_id: int) -> bool:
    """Final send-time authority for reminder targets.

    Platform owner reminder:
      explicit selection in cfg.chat_ids is enough; Telegram itself is the final
      reachability check.  Do not re-filter through a tenant-scoped chat picker.

    Non-platform tenant reminder:
      retain strict tenant membership so second-circle spaces cannot notify chats
      belonging to another space merely by injecting an id into stored config.
    """
    try:
        cid = int(chat_id)
    except Exception:
        return False
    if cid not in _v173_reminder_selected_chat_ids(cfg):
        return False

    platform_id = str(globals().get("TENANT_PLATFORM_ID") or "platform")
    try:
        tid = str(_v149_reminder_cfg_tenant(cfg) or platform_id)
    except Exception:
        tid = str((cfg or {}).get("tenant_id") or platform_id)

    if tid == platform_id:
        # The owner selected this exact Telegram chat in the reminder configuration.
        # If the bot has subsequently been removed, send_message will fail explicitly
        # and the delivery journal will show the Telegram error instead of silently
        # hiding the target behind tenant_reminder_cross_chat_blocked.
        return True

    try:
        return bool(_v149_chat_belongs_to_tenant(cid, tid))
    except Exception:
        if callable(_V173_PREV_REMINDER_CHAT_ALLOWED):
            try:
                return bool(_V173_PREV_REMINDER_CHAT_ALLOWED(cfg, cid))
            except Exception:
                pass
        return False


# Per-target delivery witnesses make the next diagnostic journal self-explanatory.
_V173_BASE_REMINDER_SEND_INDIVIDUAL = globals().get("_v149_send_individual")
if callable(_V173_BASE_REMINDER_SEND_INDIVIDUAL):
    def _v149_send_individual(chat_id: int, reminder_id: int, cfg: dict, active_count: int):
        try:
            result = _V173_BASE_REMINDER_SEND_INDIVIDUAL(int(chat_id), int(reminder_id), cfg, int(active_count))
            ok = bool(result[0]) if isinstance(result, tuple) and result else bool(result)
            mid = int(result[1] or 0) if isinstance(result, tuple) and len(result) > 1 else 0
            try:
                bot_journal(
                    "reminder_delivery_v173",
                    int(chat_id),
                    f"reminder_id={int(reminder_id)} mode=individual ok={ok} message_id={mid}",
                    "INFO" if ok else "WARN",
                )
            except Exception:
                pass
            return result
        except Exception as exc:
            try:
                bot_journal("reminder_delivery_v173", int(chat_id), f"reminder_id={int(reminder_id)} mode=individual ok=False error={str(exc)[:300]}", "ERROR")
            except Exception:
                pass
            raise


_V173_BASE_REMINDER_SEND_GROUP = globals().get("_v149_send_or_edit_group")
if callable(_V173_BASE_REMINDER_SEND_GROUP):
    def _v149_send_or_edit_group(chat_id: int, text: str, old_message_id: int = 0):
        try:
            result = _V173_BASE_REMINDER_SEND_GROUP(int(chat_id), text, int(old_message_id or 0))
            ok = bool(result[0]) if isinstance(result, tuple) and result else bool(result)
            mid = int(result[1] or 0) if isinstance(result, tuple) and len(result) > 1 else 0
            try:
                bot_journal(
                    "reminder_delivery_v173",
                    int(chat_id),
                    f"mode=merged ok={ok} message_id={mid} old_message_id={int(old_message_id or 0)}",
                    "INFO" if ok else "WARN",
                )
            except Exception:
                pass
            return result
        except Exception as exc:
            try:
                bot_journal("reminder_delivery_v173", int(chat_id), f"mode=merged ok=False error={str(exc)[:300]}", "ERROR")
            except Exception:
                pass
            raise


# ---------------------------------------------------------------------------
# 2) Journal/export filenames.
# v170 fixed *misclassification*, but two downloads of the same journal on the same
# date still had the same filename.  v173 gives every operational export a clear type
# name plus a unique export timestamp and sequence.
# ---------------------------------------------------------------------------
_V173_DOWNLOAD_NAME_LOCK = _v173_threading.RLock()
_V173_DOWNLOAD_NAME_SEQ = 0
_V173_DOWNLOAD_NAME_LAST_SECOND = ""

V173_DOWNLOAD_KIND_NAMES = {
    "Журнал_текущей_версии": "События_текущей_версии",
    "Журнал_диагностики": "Диагностика_бота",
    "Журнал_FAILED_задач": "FAILED_задачи",
    "Журнал_ошибок": "Ошибки_бота",
    "Журнал_восстановления": "Восстановление_бота",
    "Журнал_пересылки": "Пересылка_сообщений",
    "Журнал_аудита": "Аудит_целостности",
    "Журнал_резервных_копий": "Резервное_копирование",
    "Журнал_финансов": "Финансовые_операции",
    "Журнал_операций": "Действия_бота",
    "Диагностика_Runtime_MEGA": "Диагностика_Runtime_MEGA",
    "ТЗ_окон_текущая_версия": "ТЗ_окон_текущая_версия",
    "ТЗ_окон_архив": "ТЗ_окон_архив",
    "Маркировки_окон": "Маркировки_окон",
    "Исходник_бота": "Исходник_бота",
}


def _v173_export_stamp() -> str:
    global _V173_DOWNLOAD_NAME_SEQ, _V173_DOWNLOAD_NAME_LAST_SECOND
    try:
        now = now_local()
    except Exception:
        now = _v173_datetime.now()
    second = now.strftime("%Y-%m-%d_%H-%M-%S")
    millis = int(getattr(now, "microsecond", 0) // 1000)
    with _V173_DOWNLOAD_NAME_LOCK:
        if second != _V173_DOWNLOAD_NAME_LAST_SECOND:
            _V173_DOWNLOAD_NAME_LAST_SECOND = second
            _V173_DOWNLOAD_NAME_SEQ = 0
        _V173_DOWNLOAD_NAME_SEQ += 1
        seq = _V173_DOWNLOAD_NAME_SEQ
    return f"{second}-{millis:03d}-{seq:02d}"


def _v173_safe_component(value: str, fallback: str = "файл") -> str:
    try:
        fn = globals().get("_v152_filename_component")
        if callable(fn):
            return str(fn(value, fallback))
    except Exception:
        pass
    text = _v173_re.sub(r"[\\/:*?\"<>|]+", "-", str(value or fallback))
    text = _v173_re.sub(r"\s+", "-", text).strip("-._")
    return (text or fallback)[:80]


def v152_human_download_name(recipient_chat_id: int, document, caption: str = "", purpose: str = "") -> str | None:
    """Readable and collision-free name for each operational download."""
    try:
        kind = _v152_download_kind(document, caption, purpose)
    except Exception:
        kind = None
    if not kind:
        return None

    old_name = str(getattr(document, "name", "") or getattr(document, "file_name", "") or "")
    ext = _v173_os.path.splitext(old_name)[1].lower()
    if kind == "Исходник_бота":
        ext = ".py"
    elif ext not in {".txt", ".csv", ".zip", ".json", ".xlsx", ".gz", ".sqlite3", ".py"}:
        ext = ".zip" if kind in {"Журнал_FAILED_задач", "Диагностика_Runtime_MEGA"} else ".txt"

    try:
        scope = _v152_scope_name(int(recipient_chat_id), kind)
    except Exception:
        scope = _v173_safe_component(f"Чат-{recipient_chat_id}", "Чат")
    try:
        period = _v152_period_suffix(document, caption, purpose)
    except Exception:
        period = ""

    visible_kind = V173_DOWNLOAD_KIND_NAMES.get(str(kind), str(kind))
    pieces = [_v173_safe_component(visible_kind, "Выгрузка"), _v173_safe_component(scope, "Чат")]
    if period:
        pieces.append(_v173_safe_component(period, "период"))
    pieces.append(f"выгрузка-{_v173_export_stamp()}")
    return "_".join(pieces) + ext


# ---------------------------------------------------------------------------
# 3) Restore compatibility for v173 snapshots.
# ---------------------------------------------------------------------------

# v180 historical restore validator removed: _v177_legacy_0290_v153_validate_restore_gz; one FINAL validator lives in 85_runtime_control.py


try:
    bot_journal(
        "v173_installed",
        int(OWNER_ID or 0),
        "platform reminder explicit targets bypass tenant-scoped known-chat recheck; unique typed journal filenames enabled",
    )
except Exception:
    pass

# ---- integrated from 121_v174_simplified_task_dispatcher.py ----
"""v174: simplified chat-native task dispatcher with editable keywords and circle inventory.

Design goals:
- owner dispatcher inventory comes from v164 circle-1 + circle-2 registries, never tenant_current_id();
- work chat UI is intentionally compact;
- normal user messages can create tasks/purchases from editable keywords;
- status keywords only change a known task by reply or explicit #number, preventing accidental closes;
- v172 task storage/UID/history/SQLite+MEGA durability are reused without migration.
"""
import re as _v174_re
import threading as _v174_threading
import time as _v174_time

VERSION = "bot_v174_simplified_task_dispatcher"
V174_FILE_MARKER = "v174_simplified_task_dispatcher"

try:
    WINDOW_MARKER_CONSTANTS.update({
        "v174:td:admin": "Ф247",
        "v174:td:menu": "Ф248",
        "v174:td:card": "Ф249",
        "v174:td:keywords": "Ф250",
    })
except Exception:
    pass

# v172 data is kept.  v174 only adds lightweight per-chat settings into the same
# root-map, so existing task UIDs/history remain valid.
_V174_DEFAULT_KEYWORDS = {
    "task": ["задача", "нужно сделать", "надо сделать", "сделать:", "#задача"],
    "purchase": ["купить", "покупка", "нужно купить", "надо купить", "заказать", "#покупка"],
    "done": ["готово", "сделано", "выполнено", "выполнена", "✅"],
    "deferred": ["отложить", "отложено", "позже", "⏸"],
    "cancelled": ["отмена", "отменить", "отменено", "не актуально", "неактуально", "❌"],
    "work": ["в работу", "делаю", "начал", "начинаю", "▶️"],
}
_V174_KEYWORD_LABELS = {
    "task": "📋 Задача",
    "purchase": "🛒 Покупка",
    "done": "✅ Выполнено",
    "deferred": "⏸ Отложено",
    "cancelled": "❌ Отмена",
    "work": "▶️ В работу",
}

_V174_INPUT_LOCK = _v174_threading.RLock()
_V174_INPUT_WAIT = {}

# Keep cancelled as a terminal state and allow deferred purchase without changing
# the durable schema.
try:
    _TASK_STATUS["cancelled"] = ("❌", "Отменена")
    _PURCHASE_STATUS["deferred"] = ("⏸", "Отложена")
    _PURCHASE_STATUS["cancelled"] = ("❌", "Отменена")
except Exception:
    pass

_V174_PREV_IS_COMPLETE = globals().get("_v172_is_complete")
def _v172_is_complete(task: dict) -> bool:
    try:
        status = str((task or {}).get("status") or "")
        if status == "cancelled":
            return True
        if str((task or {}).get("type")) == "purchase":
            return status == "received"
        return status == "done"
    except Exception:
        return bool(_V174_PREV_IS_COMPLETE(task)) if callable(_V174_PREV_IS_COMPLETE) else False


def _v174_settings(chat_id: int) -> dict:
    row = _v172_chat_settings(int(chat_id))
    row.setdefault("auto_capture", True)
    kw = row.setdefault("keywords_v174", {})
    for kind, defaults in _V174_DEFAULT_KEYWORDS.items():
        current = kw.get(kind)
        if not isinstance(current, list):
            kw[kind] = list(defaults)
        else:
            kw[kind] = [str(x).strip() for x in current if str(x).strip()][:30]
    return row


def _v174_keywords(chat_id: int, kind: str) -> list[str]:
    return list((_v174_settings(int(chat_id)).get("keywords_v174") or {}).get(str(kind), []) or [])


def _v174_set_keywords(chat_id: int, kind: str, values) -> None:
    kind = str(kind)
    if kind not in _V174_DEFAULT_KEYWORDS:
        return
    clean = []
    seen = set()
    for raw in values or []:
        value = " ".join(str(raw or "").strip().split())[:80]
        low = value.casefold()
        if value and low not in seen:
            seen.add(low); clean.append(value)
    if not clean:
        clean = list(_V174_DEFAULT_KEYWORDS[kind])
    _v174_settings(int(chat_id)).setdefault("keywords_v174", {})[kind] = clean[:30]
    _v172_persist(int(chat_id), f"keywords_{kind}")


def _v174_normalize(text: str) -> str:
    return " ".join(str(text or "").casefold().replace("ё", "е").split())


def _v174_has_keyword(text: str, keyword: str) -> bool:
    hay = _v174_normalize(text)
    needle = _v174_normalize(keyword)
    if not hay or not needle:
        return False
    if needle.startswith("#") or needle in {"✅", "⏸", "❌", "▶️"}:
        return needle in hay
    try:
        return bool(_v174_re.search(r"(?<!\w)" + _v174_re.escape(needle) + r"(?!\w)", hay, flags=_v174_re.UNICODE))
    except Exception:
        return needle in hay


def _v174_match_kind(chat_id: int, text: str, kinds) -> str:
    # Prefer longest phrase to avoid "купить" winning over "нужно купить" only
    # because of list order.  Category order is supplied by the caller.
    for kind in kinds:
        words = sorted(_v174_keywords(chat_id, kind), key=lambda x: (-len(_v174_normalize(x)), _v174_normalize(x)))
        for word in words:
            if _v174_has_keyword(text, word):
                return str(kind)
    return ""


def _v174_circle_ids(level: int) -> list[int]:
    level = 2 if int(level) == 2 else 1
    ids = []
    fn = globals().get("_v164_all_circle_ids")
    if callable(fn):
        try:
            ids = [int(x) for x in fn(level)]
        except Exception:
            ids = []
    if not ids:
        # Conservative fallback: derive from the full v164 registry rather than the
        # tenant-scoped v148 collect_all_known_chat_ids().
        known_fn = globals().get("_v164_known_chat_ids")
        info_fn = globals().get("circle_level_for_chat")
        if callable(known_fn) and callable(info_fn):
            try:
                ids = [int(x) for x in known_fn() if int(x) and int(info_fn(int(x))) == level]
            except Exception:
                ids = []
    out = []
    owner = int(OWNER_ID or 0)
    for cid in ids:
        if not cid or cid == owner:
            continue
        try:
            removed_fn = globals().get("is_chat_bot_removed")
            if callable(removed_fn) and removed_fn(int(cid)):
                continue
        except Exception:
            pass
        out.append(int(cid))
    return sorted(set(out), key=lambda c: str(get_chat_display_name(c) or f"Чат {c}").casefold())


def _v174_admin_text_kb(level: int = 1, page: int = 0):
    level = 2 if int(level) == 2 else 1
    ids = _v174_circle_ids(level)
    all1, all2 = _v174_circle_ids(1), _v174_circle_ids(2)
    per = 10
    pages = max(1, (len(ids) + per - 1) // per)
    page = max(0, min(int(page or 0), pages - 1))
    enabled = sum(1 for cid in ids if task_dispatcher_enabled(cid))
    text = _v172_mark(
        "📋 ДИСПЕТЧЕР ЗАДАЧ\n\n"
        "Выберите рабочие чаты. После включения бот сможет автоматически фиксировать задачи и покупки по ключевым словам.\n\n"
        f"1️⃣ Первый круг: {len(all1)}\n"
        f"2️⃣ Второй круг: {len(all2)}\n\n"
        f"Сейчас: {'1-й' if level == 1 else '2-й'} круг · включено {enabled}/{len(ids)}\n"
        f"Страница {page + 1}/{pages}",
        "Ф247",
    )
    kb = types.InlineKeyboardMarkup()
    kb.row(
        IB(("✅ " if level == 1 else "") + f"1️⃣ Первый круг ({len(all1)})", callback_data="v174:td:admin:1:0"),
        IB(("✅ " if level == 2 else "") + f"2️⃣ Второй круг ({len(all2)})", callback_data="v174:td:admin:2:0"),
    )
    for cid in ids[page * per:(page + 1) * per]:
        name = str(get_chat_display_name(cid) or f"Чат {cid}")
        if name.casefold() in {"чат 0", "chat 0"}:
            continue
        flag = "✅" if task_dispatcher_enabled(cid) else "⬜️"
        kb.row(IB(f"{flag} {name[:46]}", callback_data=f"v174:td:toggle:{cid}:{level}:{page}"))
    if pages > 1:
        kb.row(
            IB("◀️", callback_data=f"v174:td:admin:{level}:{(page - 1) % pages}"),
            IB(f"{page + 1}/{pages}", callback_data="none"),
            IB("▶️", callback_data=f"v174:td:admin:{level}:{(page + 1) % pages}"),
        )
    try:
        _v172_standard_nav(kb, int(OWNER_ID or 0), f"d:{today_key()}:back_main")
    except Exception:
        pass
    return text, kb


def _v174_counts(chat_id: int, user_id: int = 0) -> dict:
    rows = _v172_tasks_for_chat(int(chat_id))
    out = {"active": 0, "mine": 0, "urgent": 0, "deferred": 0, "done": 0, "cancelled": 0}
    for task in rows:
        status = str(task.get("status") or "")
        cancelled = status == "cancelled"
        complete = _v172_is_complete(task)
        if cancelled:
            out["cancelled"] += 1
        elif complete:
            out["done"] += 1
        elif status == "deferred":
            out["deferred"] += 1
        else:
            out["active"] += 1
        if not complete and str(task.get("priority")) == "urgent":
            out["urgent"] += 1
        if not complete and any(int(a.get("user_id", 0) or 0) == int(user_id or 0) for a in (task.get("assignees") or []) if isinstance(a, dict)):
            out["mine"] += 1
    return out


def _v174_menu_text_kb(chat_id: int, user_id: int = 0):
    cid = int(chat_id); counts = _v174_counts(cid, user_id)
    auto = bool(_v174_settings(cid).get("auto_capture", True))
    text = _v172_mark(
        "📋 ЗАДАЧИ\n\n"
        f"📌 Активные: {counts['active']}   🔴 Срочные: {counts['urgent']}\n"
        f"🙋 Мои: {counts['mine']}   ⏸ Отложенные: {counts['deferred']}\n\n"
        f"🤖 Автоподхват по словам: {'ВКЛ' if auto else 'ВЫКЛ'}\n"
        "Пример: «нужно купить фильтр» → покупка.\n"
        "Ответьте «готово» на исходное сообщение → задача выполнена.",
        "Ф248",
    )
    kb = types.InlineKeyboardMarkup()
    kb.row(IB("➕ Задача", callback_data="v174:td:new:task"), IB("🛒 Покупка", callback_data="v174:td:new:purchase"))
    kb.row(IB(f"📌 Активные {counts['active']}", callback_data="v174:td:list:active:0"), IB(f"🙋 Мои {counts['mine']}", callback_data="v174:td:list:mine:0"))
    kb.row(IB(f"🔴 Срочные {counts['urgent']}", callback_data="v174:td:list:urgent:0"), IB(f"⏸ Отложенные {counts['deferred']}", callback_data="v174:td:list:deferred:0"))
    kb.row(IB("✅ Выполненные", callback_data="v174:td:list:done:0"), IB("❌ Отменённые", callback_data="v174:td:list:cancelled:0"))
    kb.row(IB("⚙️ Ключевые слова", callback_data="v174:td:keywords"))
    try: _v172_standard_nav(kb, cid, f"d:{today_key()}:back_main")
    except Exception: pass
    return text, kb


def _v174_task_by_number(chat_id: int, number: int):
    for task in _v172_tasks_for_chat(int(chat_id)):
        if int(task.get("number", 0) or 0) == int(number) and not task.get("deleted"):
            return task
    return None


def _v174_task_for_reply(chat_id: int, reply_message_id: int):
    cid = int(chat_id); mid = int(reply_message_id or 0)
    if not mid:
        return None
    try:
        uid = _v172_source_root().get(_v172_source_key(cid, mid))
        task = _v172_task_for_uid(uid) if uid else None
        if isinstance(task, dict) and not task.get("deleted"):
            return task
    except Exception:
        pass
    for task in _v172_tasks_for_chat(cid):
        if int(task.get("card_message_id", 0) or 0) == mid and not task.get("deleted"):
            return task
    return None


def _v174_status_label(task: dict) -> str:
    status = str(task.get("status") or "")
    labels = {
        "new": "🆕 Новая", "work": "▶️ В работе", "wait": "🟣 Ждёт", "deferred": "⏸ Отложена",
        "done": "✅ Выполнена", "need": "🛒 Купить", "search": "🔎 Ищем", "ordered": "📦 Заказано",
        "bought": "💰 Куплено", "received": "✅ Куплено", "cancelled": "❌ Отменена",
    }
    return labels.get(status, _v172_status_text(task))


def _v174_compact_text(task: dict) -> str:
    kind = "🛒 ПОКУПКА" if str(task.get("type")) == "purchase" else "📋 ЗАДАЧА"
    pri = "🔴" if str(task.get("priority")) == "urgent" else ("🟠" if str(task.get("priority")) == "important" else "")
    lines = [
        f"{kind} #{int(task.get('number', 0) or 0)} {pri}".rstrip(),
        str(task.get("description") or task.get("title") or "Без названия")[:1500],
        "",
        f"Статус: {_v174_status_label(task)}",
    ]
    deadline = _v172_deadline_text(task)
    if deadline != "не указан":
        lines.append(f"Срок: {deadline}")
    assignee = _v172_assignee_text(task)
    if assignee != "не назначен":
        lines.append(f"Ответственный: {assignee}")
    return _v172_mark("\n".join(lines), "Ф249")


def _v174_compact_kb(task: dict, chat_id: int, user_id: int):
    uid = str(task.get("uid") or "")
    kb = types.InlineKeyboardMarkup()
    status = str(task.get("status") or "")
    if str(task.get("type")) == "purchase":
        kb.row(
            IB(("✅ " if status == "ordered" else "") + "📦 Заказано", callback_data=f"v174:td:status:{uid}:ordered"),
            IB(("✅ " if status == "received" else "") + "✅ Куплено", callback_data=f"v174:td:status:{uid}:received"),
        )
    else:
        kb.row(
            IB(("✅ " if status == "work" else "") + "▶️ В работу", callback_data=f"v174:td:status:{uid}:work"),
            IB(("✅ " if status == "done" else "") + "✅ Выполнено", callback_data=f"v174:td:status:{uid}:done"),
        )
    kb.row(
        IB(("✅ " if status == "deferred" else "") + "⏸ Отложить", callback_data=f"v174:td:status:{uid}:deferred"),
        IB(("✅ " if status == "cancelled" else "") + "❌ Отменить", callback_data=f"v174:td:status:{uid}:cancelled"),
    )
    kb.row(IB("✏️ Изменить", callback_data=f"v174:td:edit:{uid}"), IB("🕘 История", callback_data=f"v174:td:history:{uid}"))
    url = _v172_source_url(task)
    if url:
        kb.row(IB("💬 Исходное сообщение", url=url))
    try: _v172_standard_nav(kb, int(chat_id), "v174:td:menu")
    except Exception: pass
    return kb


def _v174_edit_text_kb(task: dict, chat_id: int, user_id: int):
    uid = str(task.get("uid") or "")
    text = _v172_mark(
        f"✏️ ИЗМЕНИТЬ #{task.get('number')}\n\n"
        f"{str(task.get('description') or task.get('title') or '')[:1200]}\n\n"
        f"⚡ {_v172_priority_text(task)}\n"
        f"📅 {_v172_deadline_text(task)}\n"
        f"👤 {_v172_assignee_text(task)}",
        "Ф249",
    )
    kb = types.InlineKeyboardMarkup()
    kb.row(IB("✏️ Текст", callback_data=f"v174:td:input:{uid}:text"), IB("📅 Срок", callback_data=f"v174:td:input:{uid}:deadline"))
    kb.row(IB("⚡ Срочность", callback_data=f"v174:td:priority:{uid}"), IB("👤 Взять / снять себя", callback_data=f"v174:td:take:{uid}"))
    kb.row(IB("🏠 Объект", callback_data=f"v174:td:input:{uid}:object"), IB("💬 Комментарий", callback_data=f"v174:td:input:{uid}:comment"))
    try: _v172_standard_nav(kb, int(chat_id), f"v174:td:open:{uid}")
    except Exception: pass
    return text, kb


def _v174_filter(task: dict, filt: str, user_id: int) -> bool:
    if task.get("deleted"):
        return False
    status = str(task.get("status") or "")
    complete = _v172_is_complete(task)
    if filt == "active": return not complete and status != "deferred"
    if filt == "mine": return not complete and any(int(a.get("user_id", 0) or 0) == int(user_id or 0) for a in (task.get("assignees") or []) if isinstance(a, dict))
    if filt == "urgent": return not complete and str(task.get("priority")) == "urgent"
    if filt == "deferred": return status == "deferred"
    if filt == "done": return complete and status != "cancelled"
    if filt == "cancelled": return status == "cancelled"
    return True


def _v174_list_text_kb(chat_id: int, filt: str, page: int, user_id: int):
    cid = int(chat_id); filt = str(filt or "active")
    rows = [t for t in _v172_tasks_for_chat(cid) if _v174_filter(t, filt, user_id)]
    rows.sort(key=_v172_sort_key)
    per = 8; pages = max(1, (len(rows) + per - 1) // per); page = max(0, min(int(page or 0), pages - 1))
    title = {"active":"📌 Активные", "mine":"🙋 Мои", "urgent":"🔴 Срочные", "deferred":"⏸ Отложенные", "done":"✅ Выполненные", "cancelled":"❌ Отменённые"}.get(filt, "📋 Задачи")
    text = _v172_mark(f"{title}\n\nНайдено: {len(rows)}\nСтраница {page + 1}/{pages}", "Ф248")
    kb = types.InlineKeyboardMarkup()
    for task in rows[page * per:(page + 1) * per]:
        icon = "🛒" if str(task.get("type")) == "purchase" else "📋"
        if str(task.get("priority")) == "urgent" and not _v172_is_complete(task): icon = "🔴"
        kb.row(IB(f"{icon} #{task.get('number')} {str(task.get('title') or '')[:39]}", callback_data=f"v174:td:open:{task.get('uid')}"))
    if pages > 1:
        kb.row(IB("◀️", callback_data=f"v174:td:list:{filt}:{(page - 1) % pages}"), IB(f"{page + 1}/{pages}", callback_data="none"), IB("▶️", callback_data=f"v174:td:list:{filt}:{(page + 1) % pages}"))
    try: _v172_standard_nav(kb, cid, "v174:td:menu")
    except Exception: pass
    return text, kb


def _v174_keywords_text_kb(chat_id: int):
    cid = int(chat_id); s = _v174_settings(cid); auto = bool(s.get("auto_capture", True))
    lines = [
        "⚙️ КЛЮЧЕВЫЕ СЛОВА", "",
        f"🤖 Автоподхват: {'ВКЛ' if auto else 'ВЫКЛ'}", "",
        "Создание задачи/покупки — если фраза встречается в сообщении.",
        "Статус — только ответом на исходное сообщение/карточку или вместе с #номером.", "",
    ]
    for kind in ("task", "purchase", "done", "deferred", "cancelled", "work"):
        words = ", ".join(_v174_keywords(cid, kind))
        lines.append(f"{_V174_KEYWORD_LABELS[kind]}: {words}")
    text = _v172_mark("\n".join(lines)[:3900], "Ф250")
    kb = types.InlineKeyboardMarkup()
    kb.row(IB(f"🤖 Автоподхват: {'ВКЛ' if auto else 'ВЫКЛ'}", callback_data="v174:td:auto"))
    kb.row(IB("📋 Слова задач", callback_data="v174:td:kw:task"), IB("🛒 Слова покупок", callback_data="v174:td:kw:purchase"))
    kb.row(IB("✅ Выполнено", callback_data="v174:td:kw:done"), IB("▶️ В работу", callback_data="v174:td:kw:work"))
    kb.row(IB("⏸ Отложено", callback_data="v174:td:kw:deferred"), IB("❌ Отмена", callback_data="v174:td:kw:cancelled"))
    kb.row(IB("♻️ Сбросить все слова", callback_data="v174:td:kwreset:all"))
    try: _v172_standard_nav(kb, cid, "v174:td:menu")
    except Exception: pass
    return text, kb


def _v174_keyword_group_text_kb(chat_id: int, kind: str):
    cid = int(chat_id); kind = str(kind)
    words = _v174_keywords(cid, kind)
    text = _v172_mark(
        f"{_V174_KEYWORD_LABELS.get(kind, kind)} — КЛЮЧЕВЫЕ СЛОВА\n\n"
        + ("\n".join(f"• {x}" for x in words) if words else "—")
        + "\n\nНажмите «Заменить список» и отправьте слова через запятую.",
        "Ф250",
    )
    kb = types.InlineKeyboardMarkup()
    kb.row(IB("✏️ Заменить список", callback_data=f"v174:td:kwedit:{kind}"), IB("♻️ По умолчанию", callback_data=f"v174:td:kwreset:{kind}"))
    try: _v172_standard_nav(kb, cid, "v174:td:keywords")
    except Exception: pass
    return text, kb


def _v174_set_input(chat_id: int, user_id: int, action: str, kind: str = "", task_uid: str = "", message_id: int = 0, prompt_id: int = 0):
    with _V174_INPUT_LOCK:
        _V174_INPUT_WAIT[(int(chat_id), int(user_id))] = {
            "action": str(action), "kind": str(kind), "task_uid": str(task_uid or "").upper(),
            "message_id": int(message_id or 0), "prompt_id": int(prompt_id or 0), "created": _v174_time.time(),
        }


def _v174_prompt_input(chat_id: int, user_id: int, action: str, kind: str = "", task_uid: str = "", message_id: int = 0):
    prompts = {
        "new_task": "✍️ Напишите задачу одним сообщением. Для отмены: отмена",
        "new_purchase": "🛒 Напишите, что нужно купить. Для отмены: отмена",
        "text": "✏️ Напишите новый текст. Для отмены: отмена",
        "deadline": "📅 Введите срок: 12.08 18:00, сегодня 18:00, завтра 15:00. «нет» — убрать срок.",
        "object": "🏠 Напишите объект/дом/зону. «нет» — очистить.",
        "comment": "💬 Напишите комментарий. Для отмены: отмена",
        "keywords": f"✏️ Отправьте новый список для «{_V174_KEYWORD_LABELS.get(kind, kind)}» через запятую.\nНапример: задача, нужно сделать, поручение\n\nДля отмены: отмена",
    }
    prompt_id = 0
    try:
        sent = bot.send_message(int(chat_id), prompts.get(action, "Введите значение:"))
        prompt_id = int(getattr(sent, "message_id", 0) or 0)
    except Exception:
        pass
    _v174_set_input(chat_id, user_id, action, kind, task_uid, message_id, prompt_id)


def _v174_pop_input(chat_id: int, user_id: int):
    with _V174_INPUT_LOCK:
        return _V174_INPUT_WAIT.pop((int(chat_id), int(user_id)), None)


def _v174_get_input(chat_id: int, user_id: int):
    with _V174_INPUT_LOCK:
        row = _V174_INPUT_WAIT.get((int(chat_id), int(user_id)))
        if row and _v174_time.time() - float(row.get("created", 0) or 0) > 900:
            _V174_INPUT_WAIT.pop((int(chat_id), int(user_id)), None); return None
        return dict(row) if row else None


def _v174_create_from_message(msg, kind: str, text: str = ""):
    cid = int(msg.chat.id); user_id = int(getattr(getattr(msg, "from_user", None), "id", 0) or 0)
    src = getattr(msg, "reply_to_message", None)
    source = src if src is not None else msg
    body = str(text or "").strip() or _v172_reply_text(msg) or str(getattr(msg, "text", "") or "").strip()
    if not body: return None
    skey = _v172_source_key(cid, int(getattr(source, "message_id", 0) or 0))
    existing_uid = _v172_source_root().get(skey)
    existing = _v172_task_for_uid(existing_uid) if existing_uid else None
    if isinstance(existing, dict) and not existing.get("deleted"):
        return existing
    task = _v172_create_task(cid, "purchase" if kind == "purchase" else "task", body, getattr(msg, "from_user", None), source_msg=source)
    try:
        sent = bot.send_message(cid, _v174_compact_text(task), reply_markup=_v174_compact_kb(task, cid, user_id), reply_to_message_id=int(getattr(source, "message_id", 0) or 0) or None)
        task["card_message_id"] = int(getattr(sent, "message_id", 0) or 0)
        _v172_persist(cid, "v174_compact_card")
    except Exception:
        pass
    return task


def _v174_set_status(task: dict, status: str, user=None, detail: str = ""):
    if not isinstance(task, dict): return False
    kind = str(task.get("type") or "task")
    allowed = {"deferred", "cancelled"}
    allowed |= {"new", "work", "done"} if kind != "purchase" else {"need", "search", "ordered", "received"}
    if status not in allowed: return False
    task["status"] = status
    _v172_touch(task, user, "status_changed", detail or _v174_status_label(task))
    return True


def _v174_refresh_card(task: dict):
    try:
        cid = int(task.get("chat_id", 0) or 0); mid = int(task.get("card_message_id", 0) or 0)
        if cid and mid:
            bot.edit_message_text(_v174_compact_text(task), chat_id=cid, message_id=mid, reply_markup=_v174_compact_kb(task, cid, int(task.get("updated_by", 0) or 0)))
    except Exception:
        pass


def _v174_status_target_from_message(msg):
    cid = int(msg.chat.id); text = str(getattr(msg, "text", "") or "")
    # Explicit #number works without reply.
    m = _v174_re.search(r"(?:#|№)\s*(\d{1,7})", text)
    if m:
        task = _v174_task_by_number(cid, int(m.group(1)))
        if task: return task
    reply = getattr(msg, "reply_to_message", None)
    if reply is not None:
        return _v174_task_for_reply(cid, int(getattr(reply, "message_id", 0) or 0))
    return None


def _v174_auto_process(msg) -> None:
    try:
        if str(getattr(msg, "content_type", "") or "") != "text": return
        cid = int(msg.chat.id)
        if not cid or not task_dispatcher_enabled(cid): return
        text = str(getattr(msg, "text", "") or "").strip()
        if not text or text.startswith("/"): return
        sender = getattr(msg, "from_user", None)
        if sender is not None and bool(getattr(sender, "is_bot", False)): return
        # A text entered for an existing v172 edit prompt must never be auto-captured
        # as a second task before the old prompt consumes it.
        try:
            old_input = globals().get("_v172_get_input")
            if callable(old_input) and old_input(cid, int(getattr(sender, "id", 0) or 0)):
                return
        except Exception:
            pass
        settings = _v174_settings(cid)
        if not bool(settings.get("auto_capture", True)): return

        # 1) Status words need a target.  This prevents a random "готово" from
        # closing whichever task happens to be newest.
        target = _v174_status_target_from_message(msg)
        if target is not None:
            status_kind = _v174_match_kind(cid, text, ("done", "deferred", "cancelled", "work"))
            if status_kind:
                if status_kind == "done": status = "received" if str(target.get("type")) == "purchase" else "done"
                elif status_kind == "work": status = "search" if str(target.get("type")) == "purchase" else "work"
                else: status = status_kind
                if _v174_set_status(target, status, sender, f"по ключевому слову: {status_kind}"):
                    _v174_refresh_card(target)
                    try: bot_journal("v174_task_status_keyword", cid, f"task={target.get('number')}; status={status}; msg={getattr(msg,'message_id',0)}")
                    except Exception: pass
                return

        # 2) Purchase wins over generic task if both categories match.
        create_kind = _v174_match_kind(cid, text, ("purchase", "task"))
        if create_kind:
            skey = _v172_source_key(cid, int(getattr(msg, "message_id", 0) or 0))
            uid = _v172_source_root().get(skey)
            if uid and _v172_task_for_uid(uid): return
            task = _v174_create_from_message(msg, "purchase" if create_kind == "purchase" else "task", text)
            if task:
                try: bot_journal("v174_task_autocaptured", cid, f"task={task.get('number')}; type={task.get('type')}; msg={getattr(msg,'message_id',0)}")
                except Exception: pass
    except Exception as exc:
        try: log_error(f"v174 auto task: {exc}")
        except Exception: pass


def _v174_handle_own_input(msg) -> bool:
    if str(getattr(msg, "content_type", "") or "") != "text": return False
    cid = int(msg.chat.id); uid = int(getattr(getattr(msg, "from_user", None), "id", 0) or 0)
    row = _v174_get_input(cid, uid)
    if not row: return False
    text = str(getattr(msg, "text", "") or "").strip()
    _v174_pop_input(cid, uid)
    _v172_delete_quiet(cid, int(row.get("prompt_id", 0) or 0))
    if text.casefold() in {"отмена", "cancel", "стоп"}:
        try: send_and_auto_delete(cid, "❎ Действие отменено.", 5)
        except Exception: pass
        return True
    action = str(row.get("action") or "")
    user = getattr(msg, "from_user", None)
    if action == "keywords":
        kind = str(row.get("kind") or "")
        values = [x.strip() for x in _v174_re.split(r"[,;\n]+", text) if x.strip()]
        _v174_set_keywords(cid, kind, values)
        try:
            body, kb = _v174_keyword_group_text_kb(cid, kind)
            bot.send_message(cid, body, reply_markup=kb)
        except Exception: pass
        _v172_delete_quiet(cid, int(getattr(msg, "message_id", 0) or 0))
        return True
    if action in {"new_task", "new_purchase"}:
        if not text:
            return True
        task = _v172_create_task(cid, "purchase" if action == "new_purchase" else "task", text, user, source_msg=None)
        try:
            sent = bot.send_message(cid, _v174_compact_text(task), reply_markup=_v174_compact_kb(task, cid, uid))
            task["card_message_id"] = int(getattr(sent, "message_id", 0) or 0)
            _v172_persist(cid, "v174_manual_create")
        except Exception: pass
        _v172_delete_quiet(cid, int(getattr(msg, "message_id", 0) or 0))
        return True
    task = _v172_task_for_uid(row.get("task_uid")) if row.get("task_uid") else None
    if not isinstance(task, dict) or int(task.get("chat_id", 0) or 0) != cid:
        return True
    ok = True
    if action == "text":
        if not (_v172_is_manager(uid, cid) or uid == int(task.get("creator_user_id", 0) or 0)):
            ok = False
        else:
            task["description"] = text[:4000]; task["title"] = _v172_title(text); _v172_touch(task, user, "text_changed", text[:180])
    elif action == "deadline":
        value = _v172_parse_deadline(text)
        if value is None:
            ok = False
            try: send_and_auto_delete(cid, "❌ Срок не распознан. Пример: завтра 15:00", 8)
            except Exception: pass
        else:
            task["deadline"] = value; _v172_touch(task, user, "deadline_changed", value or "срок убран")
    elif action == "object":
        value = "" if text.casefold() in {"нет", "убрать", "очистить", "-"} else text[:180]
        task["object"] = value; _v172_touch(task, user, "object_changed", value or "очищено")
    elif action == "comment":
        arr = task.setdefault("comments", []); arr.append({"at": _v172_iso(), "user_id": uid, "user": _v172_user_name(user), "text": text[:1200]})
        if len(arr) > V172_COMMENT_KEEP: del arr[:-V172_COMMENT_KEEP]
        _v172_touch(task, user, "comment_added", text[:220])
    _v172_delete_quiet(cid, int(getattr(msg, "message_id", 0) or 0))
    if ok:
        _v174_refresh_card(task)
        mid = int(row.get("message_id", 0) or 0)
        if mid:
            try: bot.edit_message_text(_v174_compact_text(task), chat_id=cid, message_id=mid, reply_markup=_v174_compact_kb(task, cid, uid))
            except Exception: pass
    return True


# v180 retired function removed: _v174_install_message_wrapper
def _v174_command_tasks(msg):
    cid = int(msg.chat.id); uid = int(getattr(getattr(msg, "from_user", None), "id", 0) or 0)
    if cid == int(OWNER_ID or 0):
        text, kb = _v174_admin_text_kb(1, 0); bot.send_message(cid, text, reply_markup=kb); return
    if not task_dispatcher_enabled(cid):
        bot.reply_to(msg, "📋 Диспетчер задач в этом чате выключен."); return
    text, kb = _v174_menu_text_kb(cid, uid); bot.send_message(cid, text, reply_markup=kb)


def _v174_command_create(msg, kind: str):
    cid = int(msg.chat.id)
    if not task_dispatcher_enabled(cid):
        bot.reply_to(msg, "📋 Диспетчер задач в этом чате выключен."); return
    text = _v172_command_text(msg)
    src = getattr(msg, "reply_to_message", None)
    if src is not None:
        skey = _v172_source_key(cid, int(getattr(src, "message_id", 0) or 0))
        old_uid = _v172_source_root().get(skey); old = _v172_task_for_uid(old_uid) if old_uid else None
        if isinstance(old, dict) and not old.get("deleted"):
            try: bot.reply_to(msg, f"ℹ️ Уже зафиксировано как #{old.get('number')}.", reply_markup=_v174_compact_kb(old, cid, int(getattr(getattr(msg,'from_user',None),'id',0) or 0)))
            except Exception: pass
            return
    if not text and src is None:
        _v174_prompt_input(cid, int(getattr(getattr(msg, "from_user", None), "id", 0) or 0), "new_purchase" if kind == "purchase" else "new_task")
        return
    _v174_create_from_message(msg, kind, text)


def _v174_replace_command_handlers() -> int:
    count = 0
    for row in list(getattr(bot, "message_handlers", []) or []):
        if not isinstance(row, dict): continue
        fn = row.get("function")
        name = str(getattr(fn, "__name__", ""))
        if name == "cmd_tasks_v172": row["function"] = _v174_command_tasks; count += 1
        elif name == "cmd_task_v172": row["function"] = lambda msg: _v174_command_create(msg, "task"); count += 1
        elif name == "cmd_buy_v172": row["function"] = lambda msg: _v174_command_create(msg, "purchase"); count += 1
    return count


def _v174_edit_call(call, text, kb):
    try: safe_edit(bot, call, text, reply_markup=kb)
    except Exception:
        try:
            fast_ui_edit_message_text(
                int(call.message.chat.id), int(call.message.message_id), text,
                reply_markup=kb, purpose="task_callback_fallback_v178",
            )
        except Exception:
            pass


def _v174_callback(call):
    raw = str(getattr(call, "data", "") or "")
    if not raw.startswith("v174:td:"): return False
    parts = raw.split(":"); action = parts[2] if len(parts) > 2 else ""
    cid = int(call.message.chat.id); user = getattr(call, "from_user", None); uid_user = int(getattr(user, "id", 0) or 0)
    try: bot.answer_callback_query(call.id)
    except Exception: pass

    if action == "admin":
        if cid != int(OWNER_ID or 0): return True
        level = 2 if len(parts) > 3 and str(parts[3]) == "2" else 1
        page = int(parts[4]) if len(parts) > 4 and str(parts[4]).isdigit() else 0
        text, kb = _v174_admin_text_kb(level, page); _v174_edit_call(call, text, kb); return True
    if action == "toggle":
        if cid != int(OWNER_ID or 0): return True
        try: target = int(parts[3]); level = 2 if int(parts[4]) == 2 else 1; page = int(parts[5])
        except Exception: return True
        if target not in set(_v174_circle_ids(level)):
            try: bot.answer_callback_query(call.id, "Чат больше не входит в этот круг.", show_alert=True)
            except Exception: pass
            return True
        s = _v172_chat_settings(target); s["enabled"] = not bool(s.get("enabled")); s["updated_at"] = _v172_iso(); s["updated_by"] = uid_user
        _v174_settings(target)
        _v172_persist(target, "v174_dispatcher_toggle")
        try:
            if s["enabled"]:
                bot.send_message(target, "✅ Диспетчер задач включён. Можно просто писать: «нужно сделать …» или «нужно купить …». Статус меняется кнопками или ответом «готово / отложить / отменить».")
        except Exception: pass
        try: schedule_main_window_recreate_after_quiet(target, delay=0.2)
        except Exception: pass
        text, kb = _v174_admin_text_kb(level, page); _v174_edit_call(call, text, kb); return True

    if not task_dispatcher_enabled(cid):
        try: bot.answer_callback_query(call.id, "Диспетчер в этом чате выключен.", show_alert=True)
        except Exception: pass
        return True

    if action == "menu":
        text, kb = _v174_menu_text_kb(cid, uid_user); _v174_edit_call(call, text, kb); return True
    if action == "new":
        kind = parts[3] if len(parts) > 3 else "task"
        _v174_prompt_input(cid, uid_user, "new_purchase" if kind == "purchase" else "new_task", message_id=int(call.message.message_id)); return True
    if action == "list":
        filt = parts[3] if len(parts) > 3 else "active"; page = int(parts[4]) if len(parts) > 4 and str(parts[4]).isdigit() else 0
        text, kb = _v174_list_text_kb(cid, filt, page, uid_user); _v174_edit_call(call, text, kb); return True
    if action == "keywords":
        text, kb = _v174_keywords_text_kb(cid); _v174_edit_call(call, text, kb); return True
    if action == "auto":
        s = _v174_settings(cid); s["auto_capture"] = not bool(s.get("auto_capture", True)); _v172_persist(cid, "v174_auto_capture")
        text, kb = _v174_keywords_text_kb(cid); _v174_edit_call(call, text, kb); return True
    if action == "kw":
        kind = parts[3] if len(parts) > 3 else "task"
        text, kb = _v174_keyword_group_text_kb(cid, kind); _v174_edit_call(call, text, kb); return True
    if action == "kwedit":
        kind = parts[3] if len(parts) > 3 else "task"
        if not _v172_is_manager(uid_user, cid):
            try: bot.answer_callback_query(call.id, "Ключевые слова меняет управляющий чата.", show_alert=True)
            except Exception: pass
            return True
        _v174_prompt_input(cid, uid_user, "keywords", kind=kind, message_id=int(call.message.message_id)); return True
    if action == "kwreset":
        if not _v172_is_manager(uid_user, cid): return True
        kind = parts[3] if len(parts) > 3 else "all"
        if kind == "all":
            for k, vals in _V174_DEFAULT_KEYWORDS.items(): _v174_set_keywords(cid, k, vals)
            text, kb = _v174_keywords_text_kb(cid)
        else:
            _v174_set_keywords(cid, kind, _V174_DEFAULT_KEYWORDS.get(kind, [])); text, kb = _v174_keyword_group_text_kb(cid, kind)
        _v174_edit_call(call, text, kb); return True

    task_uid = str(parts[3]).upper() if len(parts) > 3 else ""
    task = _v172_task_for_uid(task_uid)
    if not isinstance(task, dict) or int(task.get("chat_id", 0) or 0) != cid or task.get("deleted"):
        try: bot.answer_callback_query(call.id, "Задача не найдена.", show_alert=True)
        except Exception: pass
        return True
    if action == "open":
        _v174_edit_call(call, _v174_compact_text(task), _v174_compact_kb(task, cid, uid_user)); return True
    if action == "status":
        status = parts[4] if len(parts) > 4 else ""
        if _v174_set_status(task, status, user):
            _v174_edit_call(call, _v174_compact_text(task), _v174_compact_kb(task, cid, uid_user))
        return True
    if action == "edit":
        text, kb = _v174_edit_text_kb(task, cid, uid_user); _v174_edit_call(call, text, kb); return True
    if action == "priority":
        order = ["normal", "important", "urgent"]; cur = str(task.get("priority") or "normal")
        task["priority"] = order[(order.index(cur) + 1) % len(order)] if cur in order else "normal"; _v172_touch(task, user, "priority_changed", _v172_priority_text(task))
        text, kb = _v174_edit_text_kb(task, cid, uid_user); _v174_edit_call(call, text, kb); return True
    if action == "take":
        arr = [x for x in (task.get("assignees") or []) if isinstance(x, dict)]; mine = any(int(x.get("user_id", 0) or 0) == uid_user and uid_user for x in arr)
        if mine: arr = [x for x in arr if int(x.get("user_id", 0) or 0) != uid_user]; detail = "снял себя"
        else:
            arr.append({"user_id": uid_user, "name": _v172_user_name(user)}); detail = "взял себе"
            if str(task.get("type")) != "purchase" and str(task.get("status")) == "new": task["status"] = "work"
        task["assignees"] = arr[:10]; _v172_touch(task, user, "assignee_self", detail)
        text, kb = _v174_edit_text_kb(task, cid, uid_user); _v174_edit_call(call, text, kb); return True
    if action == "input":
        field = parts[4] if len(parts) > 4 else ""
        if field in {"text", "deadline", "object", "comment"}: _v174_prompt_input(cid, uid_user, field, task_uid=task_uid, message_id=int(call.message.message_id)); return True
    if action == "history":
        kb = types.InlineKeyboardMarkup()
        try: _v172_standard_nav(kb, cid, f"v174:td:open:{task_uid}")
        except Exception: pass
        _v174_edit_call(call, _v172_history_text(task), kb); return True
    return True


# v180 retired function removed: _v174_register_callback
def _v174_legacy_filter(call):
    raw = str(getattr(call, "data", "") or "")
    return any(raw.startswith(prefix) for prefix in (
        "v172:task:admin", "v172:task:list:", "v172:task:open:", "v172:task:hist:",
        "v172:task:new:", "v172:task:search", "v172:task:groups:",
    ))


def _v174_legacy_callback(call):
    raw = str(getattr(call, "data", "") or "")
    cid = int(call.message.chat.id); user = getattr(call, "from_user", None); uid_user = int(getattr(user, "id", 0) or 0)
    try: bot.answer_callback_query(call.id)
    except Exception: pass
    parts = raw.split(":")
    action = parts[2] if len(parts) > 2 else ""
    if action == "admin":
        if cid == int(OWNER_ID or 0):
            text, kb = _v174_admin_text_kb(1, 0); _v174_edit_call(call, text, kb)
        return
    if not task_dispatcher_enabled(cid): return
    if action == "open" and len(parts) > 3:
        task = _v172_task_for_uid(parts[3])
        if isinstance(task, dict) and int(task.get("chat_id", 0) or 0) == cid:
            _v174_edit_call(call, _v174_compact_text(task), _v174_compact_kb(task, cid, uid_user))
        return
    if action == "hist" and len(parts) > 3:
        task = _v172_task_for_uid(parts[3])
        if isinstance(task, dict) and int(task.get("chat_id", 0) or 0) == cid:
            kb = types.InlineKeyboardMarkup(); _v172_standard_nav(kb, cid, f"v174:td:open:{task.get('uid')}")
            _v174_edit_call(call, _v172_history_text(task), kb)
        return
    if action == "new":
        kind = parts[3] if len(parts) > 3 else "task"; _v174_prompt_input(cid, uid_user, "new_purchase" if kind == "purchase" else "new_task", message_id=int(call.message.message_id)); return
    # Old filters that no longer exist in the simple UI land on the closest useful simple list.
    old_filter = parts[3] if action == "list" and len(parts) > 3 else "active"
    mapping = {"active":"active", "mine":"mine", "urgent":"urgent", "deferred":"deferred", "done":"done"}
    filt = mapping.get(old_filter, "active")
    text, kb = _v174_list_text_kb(cid, filt, 0, uid_user); _v174_edit_call(call, text, kb)


# v180 retired function removed: _v174_register_legacy_callback
# Replace the two v172 buttons after the old builder has already added them.
_V174_PREV_BUILD_MAIN_KEYBOARD = globals().get("build_main_keyboard")
def build_main_keyboard(day_key: str, chat_id=None):
    kb = _V174_PREV_BUILD_MAIN_KEYBOARD(day_key, chat_id) if callable(_V174_PREV_BUILD_MAIN_KEYBOARD) else types.InlineKeyboardMarkup()
    try:
        cid = int(chat_id) if chat_id is not None else 0
        rows = list(getattr(kb, "keyboard", None) or getattr(kb, "inline_keyboard", None) or [])
        cleaned = []
        for row in rows:
            kept = []
            for b in (row or []):
                cb = str(getattr(b, "callback_data", "") or "")
                if cb.startswith("v172:task:") and str(getattr(b, "text", "") or "") in {"📋 Задачи", "📋 Диспетчер задач"}:
                    continue
                kept.append(b)
            if kept: cleaned.append(kept)
        rows = cleaned
        inserts = []
        if cid and cid != int(OWNER_ID or 0) and task_dispatcher_enabled(cid): inserts.append(IB("📋 Задачи", callback_data="v174:td:menu"))
        if cid and cid == int(OWNER_ID or 0): inserts.append(IB("📋 Диспетчер задач", callback_data="v174:td:admin:1:0"))
        if inserts:
            idx = len(rows)
            for i, row in enumerate(rows):
                if any("закры" in str(getattr(b, "text", "") or "").casefold() for b in row): idx = i; break
            rows.insert(idx, inserts)
        try: kb.keyboard = rows
        except Exception:
            try: kb.inline_keyboard = rows
            except Exception: pass
    except Exception as exc:
        try: log_error(f"v174 main keyboard: {exc}")
        except Exception: pass
    return kb


# Restore validator accepts v174 snapshots.
# v180 historical restore validator removed: _v177_legacy_0291_v153_validate_restore_gz; one FINAL validator lives in 85_runtime_control.py


_V174_MESSAGE_WRAP = 0  # v180: merged into canonical on_any_message
_V174_COMMAND_REPLACED = _v174_replace_command_handlers()
_V174_CALLBACK = 0  # v179 final callback router
_V174_LEGACY_CALLBACK = 0  # v179 final callback router
try:
    bot_journal("v174_installed", int(OWNER_ID or 0), f"simple_tasks=1; circles1={len(_v174_circle_ids(1))}; circles2={len(_v174_circle_ids(2))}; message_wrap={_V174_MESSAGE_WRAP}; commands={_V174_COMMAND_REPLACED}; callback={_V174_CALLBACK}; legacy={_V174_LEGACY_CALLBACK}")
except Exception:
    pass

def task_dispatcher_callback_final(call) -> bool:
    """v179 single task callback surface; supports both v174 current and v172 legacy buttons."""
    raw = str(getattr(call, "data", "") or "")
    try:
        resolver = globals().get("resolve_short_callback")
        if callable(resolver): raw = str(resolver(raw) or raw)
    except Exception: pass
    if raw.startswith("v174:td:"):
        _v174_callback(call); return True
    if raw.startswith("v172:task:"):
        if _v174_legacy_filter(call):
            _v174_legacy_callback(call); return True
        # Rare old detail/status callbacks still map to the durable v172 engine.
        _v172_callback(call); return True
    return False
# v180_total_final_diagnostics
