# v174_simplified_task_dispatcher
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


def _v174_install_message_wrapper() -> int:
    for row in list(getattr(bot, "message_handlers", []) or []):
        if not isinstance(row, dict): continue
        fn = row.get("function")
        if not callable(fn) or getattr(fn, "_v174_task_auto", False): continue
        if not (getattr(fn, "_v172_task_input", False) or getattr(fn, "__name__", "") == "on_any_message"):
            continue
        def _wrapped_v174(msg, _original=fn):
            try:
                if _v174_handle_own_input(msg): return
            except Exception as exc:
                try: log_error(f"v174 keyword input: {exc}")
                except Exception: pass
            try: _v174_auto_process(msg)
            except Exception as exc:
                try: log_error(f"v174 auto process: {exc}")
                except Exception: pass
            return _original(msg)
        _wrapped_v174._v174_task_auto = True
        # Preserve the v172 marker so a future layer can still find the catch-all.
        _wrapped_v174._v172_task_input = True
        row["function"] = _wrapped_v174
        return 1
    return 0


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
        try: bot.edit_message_text(text, chat_id=int(call.message.chat.id), message_id=int(call.message.message_id), reply_markup=kb)
        except Exception: pass


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


def _v174_register_callback() -> int:
    try:
        bot.callback_query_handler(func=lambda c: str(getattr(c, "data", "") or "").startswith("v174:td:"))(_v174_callback)
        handlers = getattr(bot, "callback_query_handlers", None)
        if isinstance(handlers, list) and handlers:
            row = handlers.pop(); handlers.insert(0, row)
        return 1
    except Exception:
        return 0


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


def _v174_register_legacy_callback() -> int:
    try:
        bot.callback_query_handler(func=_v174_legacy_filter)(_v174_legacy_callback)
        handlers = getattr(bot, "callback_query_handlers", None)
        if isinstance(handlers, list) and handlers:
            row = handlers.pop(); handlers.insert(0, row)
        return 1
    except Exception:
        return 0


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
_V174_PREV_RESTORE_VALIDATOR = globals().get("_v153_validate_restore_gz")
def _v153_validate_restore_gz(gz_path: str):
    try:
        return _V174_PREV_RESTORE_VALIDATOR(gz_path) if callable(_V174_PREV_RESTORE_VALIDATOR) else (None, None)
    except Exception as exc:
        if "unsupported bot version" not in str(exc): raise
    import gzip, os, shutil, sqlite3, tempfile, json
    folder = tempfile.mkdtemp(prefix="v174_restore_validate_"); raw = os.path.join(folder, "restore.sqlite3")
    try:
        with gzip.open(gz_path, "rb") as fin, open(raw, "wb") as fout: shutil.copyfileobj(fin, fout, 1024 * 1024)
        conn = sqlite3.connect(raw)
        try:
            integrity = str(conn.execute("PRAGMA integrity_check").fetchone()[0])
            if integrity.lower() != "ok": raise RuntimeError(f"SQLite integrity_check: {integrity}")
            row = conn.execute("SELECT v FROM meta WHERE kind='v153_export' AND k='manifest'").fetchone()
            if not row: raise RuntimeError("manifest v153 not found")
            manifest = json.loads(row[0])
        finally: conn.close()
        if str(manifest.get("kind")) != "telegram_bot_full_state_v153": raise RuntimeError("unknown export kind")
        if int(manifest.get("schema_version") or 0) != int(V153_EXPORT_SCHEMA): raise RuntimeError("unsupported export schema")
        export_version = str(manifest.get("bot_version") or "")
        if not export_version.startswith(tuple(f"bot_v{i}_" for i in range(153, 175))): raise RuntimeError(f"unsupported bot version: {export_version or 'missing'}")
        if _v153_db_logical_checksum(raw) != str(manifest.get("checksum") or ""): raise RuntimeError("checksum mismatch")
        return manifest, raw
    except Exception:
        shutil.rmtree(folder, ignore_errors=True); raise


_V174_MESSAGE_WRAP = _v174_install_message_wrapper()
_V174_COMMAND_REPLACED = _v174_replace_command_handlers()
_V174_CALLBACK = _v174_register_callback()
_V174_LEGACY_CALLBACK = _v174_register_legacy_callback()
try:
    bot_journal("v174_installed", int(OWNER_ID or 0), f"simple_tasks=1; circles1={len(_v174_circle_ids(1))}; circles2={len(_v174_circle_ids(2))}; message_wrap={_V174_MESSAGE_WRAP}; commands={_V174_COMMAND_REPLACED}; callback={_V174_CALLBACK}; legacy={_V174_LEGACY_CALLBACK}")
except Exception:
    pass
# v174_simplified_task_dispatcher
