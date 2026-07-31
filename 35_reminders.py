# v138_parallel_lanes_ui_ack

_REMINDER_THREAD_STARTED = False
_REMINDER_THREAD_LOCK = threading.RLock()
_REMINDER_CONFIG_LOCK = threading.RLock()
_REMINDER_CHECK_SECONDS = 15.0
_REMINDER_LIST_PAGE_SIZE = 8
_REMINDER_UI_BINDINGS = {}
_REMINDER_MONTHS_RU = [
    "Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
    "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь",
]


def _reminder_owner_id() -> int | None:
    try:
        return int(OWNER_ID) if OWNER_ID else None
    except Exception:
        return None


def _new_reminder_cfg() -> dict:
    return {
        "enabled": False,
        "text": "",
        "chat_ids": [],
        "interval_minutes": 120,
        "start_hour": 8,
        "end_hour": 22,
        "start_date": today_key(),
        "end_date": "",
        "next_run_at": "",
        "last_sent_at": "",
        "last_message_ids": {},
        "created_at": now_local().isoformat(timespec="seconds"),
        "updated_at": now_local().isoformat(timespec="seconds"),
    }


def _normalize_reminder_cfg(cfg: dict) -> dict:
    if not isinstance(cfg, dict):
        cfg = {}
    cfg.setdefault("enabled", False)
    cfg.setdefault("text", "")
    cfg.setdefault("chat_ids", [])
    cfg.setdefault("interval_minutes", 120)
    cfg.setdefault("start_hour", 8)
    cfg.setdefault("end_hour", 22)
    cfg.setdefault("start_date", today_key())
    cfg.setdefault("end_date", "")
    cfg.setdefault("next_run_at", "")
    cfg.setdefault("last_sent_at", "")
    cfg.setdefault("last_message_ids", {})
    cfg.setdefault("created_at", now_local().isoformat(timespec="seconds"))
    cfg.setdefault("updated_at", now_local().isoformat(timespec="seconds"))
    return cfg


def _reminders_root() -> dict:
    """v135: несколько независимых напоминалок + миграция одиночной v134 в №1."""
    gs = data.setdefault("_global_settings", {})
    with _REMINDER_CONFIG_LOCK:
        root = gs.get("reminders_v2")
        if not isinstance(root, dict):
            root = {"next_id": 1, "items": {}, "migrated_v134": False}
            gs["reminders_v2"] = root
        items = root.setdefault("items", {})
        if not isinstance(items, dict):
            items = {}
            root["items"] = items
        root.setdefault("next_id", 1)
        root.setdefault("migrated_v134", False)

        # Одноразовая безопасная миграция старой одиночной напоминалки.
        if not bool(root.get("migrated_v134")):
            legacy = gs.get("reminder")
            if isinstance(legacy, dict) and any([
                str(legacy.get("text") or "").strip(),
                legacy.get("chat_ids"), legacy.get("last_message_ids"), legacy.get("enabled"),
            ]):
                cfg = _normalize_reminder_cfg(dict(legacy))
                cfg.pop("input_wait", None)
                cfg["updated_at"] = now_local().isoformat(timespec="seconds")
                items.setdefault("1", cfg)
                try:
                    root["next_id"] = max(int(root.get("next_id") or 1), 2)
                except Exception:
                    root["next_id"] = 2
                # Старый контур оставляем как архив, но выключаем, чтобы rollback не дал дубль.
                legacy["enabled"] = False
                legacy["next_run_at"] = ""
                legacy["migrated_to_reminders_v2"] = True
            root["migrated_v134"] = True
        for rid in list(items.keys()):
            if isinstance(items.get(rid), dict):
                items[str(rid)] = _normalize_reminder_cfg(items[rid])
        return root


def _reminder_items() -> list[tuple[int, dict]]:
    root = _reminders_root()
    rows = []
    for rid_raw, cfg in (root.get("items") or {}).items():
        try:
            rid = int(rid_raw)
        except Exception:
            continue
        if isinstance(cfg, dict):
            rows.append((rid, _normalize_reminder_cfg(cfg)))
    rows.sort(key=lambda x: x[0])
    return rows


def _reminder_cfg(reminder_id: int | str | None = None, create: bool = False) -> dict | None:
    if reminder_id is None:
        rows = _reminder_items()
        return rows[0][1] if rows else None
    try:
        rid = int(reminder_id)
    except Exception:
        return None
    root = _reminders_root()
    items = root.setdefault("items", {})
    cfg = items.get(str(rid))
    if cfg is None and create:
        cfg = _new_reminder_cfg()
        items[str(rid)] = cfg
    return _normalize_reminder_cfg(cfg) if isinstance(cfg, dict) else None


def _reminder_create() -> tuple[int, dict]:
    with _REMINDER_CONFIG_LOCK:
        root = _reminders_root()
        try:
            rid = max(1, int(root.get("next_id") or 1))
        except Exception:
            rid = 1
        while str(rid) in (root.get("items") or {}):
            rid += 1
        cfg = _new_reminder_cfg()
        root.setdefault("items", {})[str(rid)] = cfg
        root["next_id"] = rid + 1
    _reminder_save("reminder_add")
    return rid, cfg


def _reminder_position(reminder_id: int) -> int:
    ids = [rid for rid, _cfg in _reminder_items()]
    try:
        return ids.index(int(reminder_id)) + 1
    except Exception:
        return 0


def _reminder_save(reason: str = "reminder") -> None:
    try:
        save_data(data, root_only=True)
    except Exception as exc:
        log_error(f"reminder save root: {exc}")
    owner = _reminder_owner_id()
    if owner:
        try:
            schedule_delta_backup(owner, delay=0.35, reason=reason)
        except Exception as exc:
            log_error(f"reminder delta schedule: {exc}")


def _reminder_touch(cfg: dict) -> None:
    cfg["updated_at"] = now_local().isoformat(timespec="seconds")


def _reminder_parse_date(value: str):
    try:
        return datetime.strptime(str(value or ""), "%Y-%m-%d").date()
    except Exception:
        return None


def _reminder_parse_dt(value: str):
    try:
        dt = datetime.fromisoformat(str(value or ""))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=now_local().tzinfo)
        return dt
    except Exception:
        return None


def _reminder_fmt_date(value: str) -> str:
    d = _reminder_parse_date(value)
    return d.strftime("%d.%m.%Y") if d else "—"


def _reminder_fmt_dt(value: str) -> str:
    dt = _reminder_parse_dt(value)
    return dt.strftime("%d.%m %H:%M") if dt else "—"


def _reminder_interval_label(minutes: int) -> str:
    try:
        minutes = max(1, int(minutes))
    except Exception:
        minutes = 120
    if minutes % 1440 == 0:
        d = minutes // 1440
        return f"{d} д" if d != 1 else "1 день"
    if minutes % 60 == 0:
        return f"{minutes // 60} ч"
    return f"{minutes} мин"


def _reminder_normalize_hours(cfg: dict) -> None:
    try:
        start = max(0, min(23, int(cfg.get("start_hour", 8))))
    except Exception:
        start = 8
    try:
        end = max(0, min(23, int(cfg.get("end_hour", 22))))
    except Exception:
        end = 22
    if start > end:
        end = start
    cfg["start_hour"] = start
    cfg["end_hour"] = end


def _reminder_date_allowed(now_dt: datetime, cfg: dict) -> bool:
    today = now_dt.date()
    start = _reminder_parse_date(cfg.get("start_date"))
    end = _reminder_parse_date(cfg.get("end_date"))
    if start and today < start:
        return False
    if end and today > end:
        return False
    return True


def _reminder_time_allowed(now_dt: datetime, cfg: dict) -> bool:
    _reminder_normalize_hours(cfg)
    return int(cfg["start_hour"]) <= now_dt.hour <= int(cfg["end_hour"])


def _reminder_next_valid_start(now_dt: datetime, cfg: dict):
    _reminder_normalize_hours(cfg)
    start_date = _reminder_parse_date(cfg.get("start_date")) or now_dt.date()
    end_date = _reminder_parse_date(cfg.get("end_date"))
    day = max(now_dt.date(), start_date)
    for _ in range(3700):
        if end_date and day > end_date:
            return None
        candidate = datetime.combine(day, datetime.min.time(), tzinfo=now_dt.tzinfo).replace(
            hour=int(cfg.get("start_hour", 8)), minute=0, second=0, microsecond=0
        )
        end_candidate = candidate.replace(hour=int(cfg.get("end_hour", 22)), minute=59, second=59)
        if day == now_dt.date():
            if now_dt <= candidate:
                return candidate
            if now_dt <= end_candidate:
                return now_dt
        elif candidate >= now_dt:
            return candidate
        day += timedelta(days=1)
    return None


def _reminder_rearm(cfg: dict, immediate_if_valid: bool = True) -> None:
    now_dt = now_local()
    if not bool(cfg.get("enabled")):
        cfg["next_run_at"] = ""
        return
    if immediate_if_valid and _reminder_date_allowed(now_dt, cfg) and _reminder_time_allowed(now_dt, cfg):
        cfg["next_run_at"] = now_dt.isoformat(timespec="seconds")
        return
    next_dt = _reminder_next_valid_start(now_dt, cfg)
    cfg["next_run_at"] = next_dt.isoformat(timespec="seconds") if next_dt else ""
    if next_dt is None:
        cfg["enabled"] = False


def _reminder_advance_after_send(now_dt: datetime, cfg: dict) -> None:
    try:
        minutes = max(1, int(cfg.get("interval_minutes", 120)))
    except Exception:
        minutes = 120
    candidate = now_dt + timedelta(minutes=minutes)
    _reminder_normalize_hours(cfg)
    end_hour = int(cfg.get("end_hour", 22))
    start_hour = int(cfg.get("start_hour", 8))
    if candidate.date() != now_dt.date() or candidate.hour > end_hour:
        day = now_dt.date() + timedelta(days=1)
        candidate = datetime.combine(day, datetime.min.time(), tzinfo=now_dt.tzinfo).replace(hour=start_hour)
    start_date = _reminder_parse_date(cfg.get("start_date"))
    if start_date and candidate.date() < start_date:
        candidate = datetime.combine(start_date, datetime.min.time(), tzinfo=now_dt.tzinfo).replace(hour=start_hour)
    end_date = _reminder_parse_date(cfg.get("end_date"))
    if end_date and candidate.date() > end_date:
        cfg["enabled"] = False
        cfg["next_run_at"] = ""
    else:
        cfg["next_run_at"] = candidate.isoformat(timespec="seconds")


def _reminder_known_chats() -> list[tuple[int, str]]:
    rows, seen = [], set()
    try:
        source = _collect_backup_menu_items()
    except Exception:
        source = []
    for cid, title in source:
        try:
            cid = int(cid)
        except Exception:
            continue
        if cid in seen:
            continue
        seen.add(cid)
        rows.append((cid, str(title or get_chat_display_name(cid))))
    owner = _reminder_owner_id()
    if owner and owner not in seen:
        rows.insert(0, (owner, get_chat_display_name(owner)))
    return rows


def _reminder_button_label(position: int, cfg: dict) -> str:
    text = re.sub(r"\s+", " ", str(cfg.get("text") or "").strip()) or "без текста"
    base = f"{int(position)}. {text}"
    try:
        return pad_button_label_41(base)
    except Exception:
        if len(base) > 41:
            base = base[:40] + "…"
        return base + ("⠀" * max(0, 41 - len(base)))


def build_reminder_list_text() -> str:
    rows = _reminder_items()
    enabled = sum(1 for _rid, cfg in rows if bool(cfg.get("enabled")))
    return (
        "⏰ НАПОМИНАЛКИ\n\n"
        f"Всего: {len(rows)}\n"
        f"Активных: {enabled}\n\n"
        "Нажмите напоминалку для просмотра и настройки."
    )


def build_reminder_list_keyboard(day_key: str | None = None, page: int = 0):
    day_key = day_key or today_key()
    rows = _reminder_items()
    pages = max(1, (len(rows) + _REMINDER_LIST_PAGE_SIZE - 1) // _REMINDER_LIST_PAGE_SIZE)
    page = max(0, min(int(page or 0), pages - 1))
    kb = types.InlineKeyboardMarkup(row_width=1)
    kb.row(IB("+добавить⏰", callback_data=f"rem:add:{page}:{day_key}"))
    start = page * _REMINDER_LIST_PAGE_SIZE
    for idx, (rid, cfg) in enumerate(rows[start:start + _REMINDER_LIST_PAGE_SIZE], start=start + 1):
        kb.row(IB(_reminder_button_label(idx, cfg), callback_data=f"rem:open:{rid}:{page}:{day_key}"))
    if pages > 1:
        nav = []
        if page > 0:
            nav.append(IB("⬅️", callback_data=f"rem:list:{page-1}:{day_key}"))
        nav.append(IB(f"{page+1}/{pages}", callback_data="none"))
        if page + 1 < pages:
            nav.append(IB("➡️", callback_data=f"rem:list:{page+1}:{day_key}"))
        kb.row(*nav)
    kb.row(IB("⬅️ Назад", callback_data=f"d:{day_key}:back_main"))
    return kb


def _reminder_bind_editor(reminder_id: int, chat_id: int, message_id: int, day_key: str, page: int = 0) -> None:
    _REMINDER_UI_BINDINGS[int(reminder_id)] = {
        "chat_id": int(chat_id), "message_id": int(message_id), "day_key": str(day_key),
        "page": int(page), "ts": time.time(),
    }


def _reminder_unbind(reminder_id: int) -> None:
    _REMINDER_UI_BINDINGS.pop(int(reminder_id), None)


def build_reminder_menu_text(reminder_id: int) -> str:
    cfg = _reminder_cfg(reminder_id)
    if not cfg:
        return "⏰ Напоминалка не найдена."
    _reminder_normalize_hours(cfg)
    preview = re.sub(r"\s+", " ", str(cfg.get("text") or "").strip())
    if len(preview) > 160:
        preview = preview[:157] + "..."
    end_date = _reminder_fmt_date(cfg.get("end_date")) if cfg.get("end_date") else "без конца"
    state = "✅ ВКЛ" if cfg.get("enabled") else "⏸ ВЫКЛ"
    chats_count = len([x for x in (cfg.get("chat_ids") or []) if str(x).strip()])
    pos = _reminder_position(int(reminder_id)) or int(reminder_id)
    return (
        f"⏰ НАПОМИНАЛКА №{pos}\n\n"
        f"Состояние: {state}\n"
        f"Текст: {preview or 'не задан'}\n"
        f"Даты: {_reminder_fmt_date(cfg.get('start_date'))} → {end_date}\n"
        f"Время: {int(cfg.get('start_hour', 8)):02d}:00 → {int(cfg.get('end_hour', 22)):02d}:59\n"
        f"Период: каждые {_reminder_interval_label(cfg.get('interval_minutes', 120))}\n"
        f"Чаты: {chats_count}\n"
        f"Следующая: {_reminder_fmt_dt(cfg.get('next_run_at'))}\n"
        f"Последняя: {_reminder_fmt_dt(cfg.get('last_sent_at'))}"
    )


def _reminder_insert_query(token: str, current_text: str = "") -> str:
    service = f"({token} служебное — можно не трогать)"
    max_payload = max(0, 252 - len(service) - 2)
    current = str(current_text or "")
    if len(current) > max_payload:
        current = current[:max_payload]
    return service + "\n\n" + current


def compose_reminder_text_insert_value(reminder_id: int, current_text: str = "") -> str:
    return _reminder_insert_query(f"EDITREM|{int(reminder_id)}|", current_text)


def compose_reminder_interval_insert_value(reminder_id: int, cfg: dict) -> str:
    return _reminder_insert_query(f"EDITREMINT|{int(reminder_id)}|", _reminder_interval_label(cfg.get("interval_minutes", 120)))


def build_reminder_menu_keyboard(reminder_id: int, day_key: str | None = None, page: int = 0, viewer_chat_id: int | None = None):
    day_key = day_key or today_key()
    cfg = _reminder_cfg(reminder_id)
    kb = types.InlineKeyboardMarkup(row_width=2)
    if not cfg:
        kb.row(IB("⬅️ К напоминалкам", callback_data=f"rem:list:{page}:{day_key}"))
        return kb
    kb.row(
        make_copy_or_inline_button("✍️ Текст", compose_reminder_text_insert_value(reminder_id, cfg.get("text") or ""), viewer_chat_id=viewer_chat_id),
        IB(f"💬 Чаты ({len(cfg.get('chat_ids') or [])})", callback_data=f"rem:chats:{reminder_id}:0:{page}:{day_key}"),
    )
    kb.row(
        IB("📅 Даты", callback_data=f"rem:dates:{reminder_id}:{page}:{day_key}"),
        IB(f"🔁 {_reminder_interval_label(cfg.get('interval_minutes', 120))}", callback_data=f"rem:interval:{reminder_id}:{page}:{day_key}"),
    )
    kb.row(
        IB(f"🕗 С {int(cfg.get('start_hour', 8)):02d}:00", callback_data=f"rem:hours:start:{reminder_id}:{page}:{day_key}"),
        IB(f"🕙 До {int(cfg.get('end_hour', 22)):02d}:59", callback_data=f"rem:hours:end:{reminder_id}:{page}:{day_key}"),
    )
    kb.row(
        IB("⏸ Отключить" if cfg.get("enabled") else "✅ Включить", callback_data=f"rem:toggle:{reminder_id}:{page}:{day_key}"),
        IB("Изменить📝", callback_data=f"rem:editmenu:{reminder_id}:{page}:{day_key}"),
    )
    kb.row(IB("⬅️ К напоминалкам", callback_data=f"rem:list:{page}:{day_key}"))
    return kb


def _reminder_edit_menu_keyboard(reminder_id: int, page: int, day_key: str, viewer_chat_id: int):
    cfg = _reminder_cfg(reminder_id) or {}
    kb = types.InlineKeyboardMarkup(row_width=1)
    kb.row(make_copy_or_inline_button(
        "✍️ Вставить текст",
        compose_reminder_text_insert_value(reminder_id, cfg.get("text") or ""),
        viewer_chat_id=viewer_chat_id,
    ))
    kb.row(IB("🗑 Удалить", callback_data=f"rem:delete_confirm:{reminder_id}:{page}:{day_key}"))
    kb.row(
        IB("⬅️ Назад", callback_data=f"rem:open:{reminder_id}:{page}:{day_key}"),
        IB("✖️ Отмена", callback_data=f"rem:open:{reminder_id}:{page}:{day_key}"),
    )
    return kb


def _reminder_dates_text(reminder_id: int) -> str:
    cfg = _reminder_cfg(reminder_id) or {}
    end = _reminder_fmt_date(cfg.get("end_date")) if cfg.get("end_date") else "без конца"
    return f"📅 ДАТЫ НАПОМИНАЛКИ\n\nНачало: {_reminder_fmt_date(cfg.get('start_date'))}\nКонец: {end}"


def _reminder_dates_keyboard(reminder_id: int, page: int, day_key: str):
    kb = types.InlineKeyboardMarkup(row_width=2)
    now_ym = now_local().strftime("%Y-%m")
    kb.row(
        IB("📅 Начало", callback_data=f"rem:calendar:start:{now_ym}:{reminder_id}:{page}:{day_key}"),
        IB("🏁 Конец", callback_data=f"rem:calendar:end:{now_ym}:{reminder_id}:{page}:{day_key}"),
    )
    kb.row(IB("♾ Без конца", callback_data=f"rem:noend:{reminder_id}:{page}:{day_key}"))
    kb.row(IB("⬅️ Назад", callback_data=f"rem:open:{reminder_id}:{page}:{day_key}"))
    return kb


def _reminder_calendar_text(which: str, year: int, month: int) -> str:
    target = "начала" if which == "start" else "окончания"
    return f"📅 Выберите дату {target}\n\n{_REMINDER_MONTHS_RU[month-1]} {year}"


def _reminder_calendar_keyboard(which: str, year: int, month: int, reminder_id: int, page: int, day_key: str):
    kb = types.InlineKeyboardMarkup(row_width=7)
    kb.row(*[IB(x, callback_data="none") for x in ("Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс")])
    cal = calendar.Calendar(firstweekday=0)
    for week in cal.monthdayscalendar(year, month):
        row = []
        for d in week:
            if not d:
                row.append(IB(" ", callback_data="none"))
            else:
                date_key = f"{year:04d}-{month:02d}-{d:02d}"
                row.append(IB(str(d), callback_data=f"rem:date:{which}:{date_key}:{reminder_id}:{page}:{day_key}"))
        kb.row(*row)
    prev_month, prev_year = month - 1, year
    if prev_month < 1:
        prev_month, prev_year = 12, prev_year - 1
    next_month, next_year = month + 1, year
    if next_month > 12:
        next_month, next_year = 1, next_year + 1
    kb.row(
        IB("⬅️", callback_data=f"rem:calendar:{which}:{prev_year:04d}-{prev_month:02d}:{reminder_id}:{page}:{day_key}"),
        IB("📅 Даты", callback_data=f"rem:dates:{reminder_id}:{page}:{day_key}"),
        IB("➡️", callback_data=f"rem:calendar:{which}:{next_year:04d}-{next_month:02d}:{reminder_id}:{page}:{day_key}"),
    )
    return kb


def _reminder_interval_keyboard(reminder_id: int, page: int, day_key: str, viewer_chat_id: int):
    cfg = _reminder_cfg(reminder_id) or {}
    current = int(cfg.get("interval_minutes", 120) or 120)
    kb = types.InlineKeyboardMarkup(row_width=3)
    buttons = []
    for minutes in [30, 60, 120, 180, 240, 360, 720, 1440]:
        mark = "✅ " if minutes == current else ""
        buttons.append(IB(mark + _reminder_interval_label(minutes), callback_data=f"rem:intset:{minutes}:{reminder_id}:{page}:{day_key}"))
    for i in range(0, len(buttons), 3):
        kb.row(*buttons[i:i+3])
    kb.row(make_copy_or_inline_button(
        "✍️ Свой интервал",
        compose_reminder_interval_insert_value(reminder_id, cfg),
        viewer_chat_id=viewer_chat_id,
    ))
    kb.row(IB("⬅️ Назад", callback_data=f"rem:open:{reminder_id}:{page}:{day_key}"))
    return kb


def _reminder_hours_keyboard(which: str, reminder_id: int, page: int, day_key: str):
    cfg = _reminder_cfg(reminder_id) or {}
    current = int(cfg.get("start_hour" if which == "start" else "end_hour", 8 if which == "start" else 22))
    kb = types.InlineKeyboardMarkup(row_width=4)
    buttons = []
    for hour in range(24):
        mark = "✅ " if hour == current else ""
        buttons.append(IB(f"{mark}{hour:02d}:00", callback_data=f"rem:hourset:{which}:{hour}:{reminder_id}:{page}:{day_key}"))
    for i in range(0, 24, 4):
        kb.row(*buttons[i:i+4])
    kb.row(IB("⬅️ Назад", callback_data=f"rem:open:{reminder_id}:{page}:{day_key}"))
    return kb


def _reminder_chats_keyboard(reminder_id: int, chat_page: int, list_page: int, day_key: str):
    cfg = _reminder_cfg(reminder_id) or {}
    selected = {int(x) for x in (cfg.get("chat_ids") or []) if str(x).lstrip("-").isdigit()}
    rows = _reminder_known_chats()
    per_page = 8
    pages = max(1, (len(rows) + per_page - 1) // per_page)
    chat_page = max(0, min(int(chat_page), pages - 1))
    kb = types.InlineKeyboardMarkup(row_width=1)
    for cid, title in rows[chat_page * per_page:(chat_page + 1) * per_page]:
        mark = "✅" if cid in selected else "⬜"
        name = chat_button_title(cid, title) if "chat_button_title" in globals() else str(title)
        kb.row(IB(f"{mark} {name}", callback_data=f"rem:chat:{cid}:{reminder_id}:{chat_page}:{list_page}:{day_key}"))
    nav = []
    if chat_page > 0:
        nav.append(IB("⬅️", callback_data=f"rem:chats:{reminder_id}:{chat_page-1}:{list_page}:{day_key}"))
    nav.append(IB(f"{chat_page+1}/{pages}", callback_data="none"))
    if chat_page + 1 < pages:
        nav.append(IB("➡️", callback_data=f"rem:chats:{reminder_id}:{chat_page+1}:{list_page}:{day_key}"))
    kb.row(*nav)
    kb.row(IB("⬅️ Назад", callback_data=f"rem:open:{reminder_id}:{list_page}:{day_key}"))
    return kb


def _reminder_parse_custom_interval(text: str) -> int | None:
    value = str(text or "").strip().lower().replace(",", ".")
    m = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*(мин|м|min|m|ч|час|часа|часов|h|д|дн|день|дня|дней|d)?\s*", value)
    if not m:
        return None
    num = float(m.group(1)); unit = m.group(2) or "мин"
    if unit in {"ч", "час", "часа", "часов", "h"}:
        minutes = int(round(num * 60))
    elif unit in {"д", "дн", "день", "дня", "дней", "d"}:
        minutes = int(round(num * 1440))
    else:
        minutes = int(round(num))
    return minutes if 5 <= minutes <= 43200 else None


def _reminder_delete_last_messages(cfg: dict) -> None:
    last_map = cfg.get("last_message_ids") or {}
    for cid_raw, mid_raw in list(last_map.items()):
        try:
            bot.delete_message(int(cid_raw), int(mid_raw))
        except Exception:
            pass
    cfg["last_message_ids"] = {}


def _reminder_send_cycle(reminder_id: int, cfg: dict) -> bool:
    text = str(cfg.get("text") or "").strip()
    chat_ids = []
    for raw in cfg.get("chat_ids") or []:
        try:
            chat_ids.append(int(raw))
        except Exception:
            pass
    if not text or not chat_ids:
        return False
    last_map = cfg.setdefault("last_message_ids", {})
    sent_any = False
    for cid in chat_ids:
        old_mid = last_map.get(str(cid))
        try:
            sent = bot.send_message(cid, text)
            last_map[str(cid)] = int(sent.message_id)
            if old_mid and int(old_mid) != int(sent.message_id):
                try:
                    bot.delete_message(cid, int(old_mid))
                except Exception:
                    pass
            sent_any = True
            try:
                bot_journal("reminder_sent", cid, f"reminder_id={int(reminder_id)} message_id={sent.message_id}")
            except Exception:
                pass
        except Exception as exc:
            log_error(f"reminder {reminder_id} send {cid}: {exc}")
            try:
                bot_journal("reminder_send_error", cid, f"reminder_id={int(reminder_id)} {exc}", "ERROR")
            except Exception:
                pass
    return sent_any


def _reminder_tick_one(reminder_id: int, cfg: dict) -> bool:
    if not bool(cfg.get("enabled")):
        return False
    now_dt = now_local()
    if not str(cfg.get("text") or "").strip() or not (cfg.get("chat_ids") or []):
        return False
    due = _reminder_parse_dt(cfg.get("next_run_at"))
    if due is None:
        _reminder_rearm(cfg, immediate_if_valid=True)
        due = _reminder_parse_dt(cfg.get("next_run_at"))
    if due is None or now_dt < due:
        return False
    if not _reminder_date_allowed(now_dt, cfg) or not _reminder_time_allowed(now_dt, cfg):
        next_dt = _reminder_next_valid_start(now_dt, cfg)
        cfg["next_run_at"] = next_dt.isoformat(timespec="seconds") if next_dt else ""
        if next_dt is None:
            cfg["enabled"] = False
        _reminder_touch(cfg)
        return True
    if _reminder_send_cycle(reminder_id, cfg):
        cfg["last_sent_at"] = now_dt.isoformat(timespec="seconds")
    _reminder_advance_after_send(now_dt, cfg)
    _reminder_touch(cfg)
    return True


def _reminder_due_now(cfg: dict, now_dt=None) -> bool:
    if not bool((cfg or {}).get("enabled")):
        return False
    if not str((cfg or {}).get("text") or "").strip() or not ((cfg or {}).get("chat_ids") or []):
        return False
    now_dt = now_dt or now_local()
    due = _reminder_parse_dt((cfg or {}).get("next_run_at"))
    return due is None or now_dt >= due


def _reminder_tick_job(reminder_id: int) -> None:
    """One reminder per keyed worker; network send happens outside the config lock."""
    reminder_id = int(reminder_id)
    snapshot = None
    now_dt = now_local()
    changed_without_send = False
    with _REMINDER_CONFIG_LOCK:
        cfg = _reminder_cfg(reminder_id)
        if not cfg or not _reminder_due_now(cfg, now_dt):
            return
        due = _reminder_parse_dt(cfg.get("next_run_at"))
        if due is None:
            _reminder_rearm(cfg, immediate_if_valid=True)
            due = _reminder_parse_dt(cfg.get("next_run_at"))
        if due is None or now_dt < due:
            return
        if not _reminder_date_allowed(now_dt, cfg) or not _reminder_time_allowed(now_dt, cfg):
            next_dt = _reminder_next_valid_start(now_dt, cfg)
            cfg["next_run_at"] = next_dt.isoformat(timespec="seconds") if next_dt else ""
            if next_dt is None:
                cfg["enabled"] = False
            _reminder_touch(cfg)
            changed_without_send = True
        else:
            snapshot = copy.deepcopy(cfg)
    if changed_without_send:
        _reminder_save("reminders_tick_window")
        return
    if snapshot is None:
        return

    sent_ok = _reminder_send_cycle(reminder_id, snapshot)
    updated = False
    with _REMINDER_CONFIG_LOCK:
        cfg = _reminder_cfg(reminder_id)
        if cfg:
            # Preserve settings changed by the user while the network send was in progress;
            # merge only delivery metadata produced by this cycle.
            cfg["last_message_ids"] = dict(snapshot.get("last_message_ids") or {})
            if sent_ok:
                cfg["last_sent_at"] = now_dt.isoformat(timespec="seconds")
            _reminder_advance_after_send(now_dt, cfg)
            _reminder_touch(cfg)
            updated = True
    if updated:
        _reminder_save("reminders_tick")


def _reminder_tick() -> None:
    now_dt = now_local()
    due_ids = []
    with _REMINDER_CONFIG_LOCK:
        for rid, cfg in _reminder_items():
            try:
                if _reminder_due_now(cfg, now_dt):
                    due_ids.append(int(rid))
            except Exception as exc:
                log_error(f"reminder {rid} due check: {exc}")
    for rid in due_ids:
        if not REMINDER_TASK_POOL.submit_unique(f"reminder:{rid}", _reminder_tick_job, rid):
            try:
                bot_journal("reminder_dispatch_coalesced", None, f"reminder_id={rid}")
            except Exception:
                pass


def _reminder_scheduler_loop() -> None:
    while True:
        try:
            if runtime_is_ready():
                _reminder_tick()
        except Exception as exc:
            log_error(f"reminder scheduler: {exc}")
        time.sleep(_REMINDER_CHECK_SECONDS)


def start_reminder_scheduler() -> None:
    global _REMINDER_THREAD_STARTED
    with _REMINDER_THREAD_LOCK:
        if _REMINDER_THREAD_STARTED:
            return
        _REMINDER_THREAD_STARTED = True
        _reminders_root()  # миграция до первого tick
        try:
            save_data(data, root_only=True)  # миграция v134 должна пережить restart даже до первого send
        except Exception as exc:
            log_error(f"reminder migration save: {exc}")
        threading.Thread(target=_reminder_scheduler_loop, name="reminder-scheduler", daemon=True).start()


def _reminder_direct_input_predicate(msg) -> bool:
    try:
        if getattr(msg, "content_type", None) != "text" or not is_owner_chat(int(msg.chat.id)):
            return False
        text = str(getattr(msg, "text", "") or "")
        return bool(re.search(r"\((?:EDITREM|EDITREMINT)\|\d+\|", text))
    except Exception:
        return False


def _reminder_extract_insert(text: str, kind: str):
    raw = str(text or "")
    m = re.search(rf"\(({re.escape(kind)}\|(\d+)\|)[^)]*\)", raw)
    if not m:
        return None, ""
    try:
        rid = int(m.group(2))
    except Exception:
        return None, ""
    value = (raw[:m.start()] + " " + raw[m.end():]).strip()
    try:
        value = sanitize_telegram_inserted_text(value)
    except Exception:
        value = re.sub(r"(?m)^\s*@[A-Za-z0-9_]{3,}\s+", "", value).strip()
    return rid, value.strip()


def _reminder_refresh_bound_editor(reminder_id: int) -> None:
    binding = _REMINDER_UI_BINDINGS.get(int(reminder_id)) or {}
    try:
        chat_id = int(binding.get("chat_id")); message_id = int(binding.get("message_id"))
    except Exception:
        return
    cfg = _reminder_cfg(reminder_id)
    if not cfg:
        return
    day_key = str(binding.get("day_key") or today_key()); page = int(binding.get("page") or 0)
    try:
        bot.edit_message_text(
            build_reminder_menu_text(reminder_id), chat_id=chat_id, message_id=message_id,
            reply_markup=build_reminder_menu_keyboard(reminder_id, day_key, page, viewer_chat_id=chat_id),
        )
    except Exception as exc:
        if "message is not modified" not in str(exc).lower():
            log_error(f"reminder refresh bound editor {reminder_id}: {exc}")


@bot.message_handler(func=_reminder_direct_input_predicate, content_types=["text"])
def reminder_direct_input_message(msg):
    chat_id = int(msg.chat.id)
    text = str(msg.text or "")
    if "EDITREMINT|" in text:
        rid, value = _reminder_extract_insert(text, "EDITREMINT")
        cfg = _reminder_cfg(rid) if rid is not None else None
        if not cfg:
            send_and_auto_delete(chat_id, "❌ Напоминалка не найдена.", 8)
            return
        minutes = _reminder_parse_custom_interval(value)
        if minutes is None:
            send_and_auto_delete(chat_id, "❌ Пример: 90 мин, 2 ч, 1 день. Минимум 5 минут.", 10)
            return
        _durable_note_source_consumed("reminder_interval_insert")
        cfg["interval_minutes"] = int(minutes)
        if cfg.get("enabled"):
            _reminder_rearm(cfg, immediate_if_valid=True)
        _reminder_touch(cfg); _reminder_save("reminder_interval_insert")
        try:
            bot.delete_message(chat_id, msg.message_id)
        except Exception:
            pass
        _reminder_refresh_bound_editor(rid)
        return

    rid, value = _reminder_extract_insert(text, "EDITREM")
    cfg = _reminder_cfg(rid) if rid is not None else None
    if not cfg:
        send_and_auto_delete(chat_id, "❌ Напоминалка не найдена.", 8)
        return
    if not value:
        send_and_auto_delete(chat_id, "❌ Текст пустой.", 8)
        return
    if len(value) > 4000:
        send_and_auto_delete(chat_id, "❌ Текст слишком длинный. Максимум 4000 символов.", 10)
        return
    _durable_note_source_consumed("reminder_text_insert")
    cfg["text"] = value
    if cfg.get("enabled"):
        _reminder_rearm(cfg, immediate_if_valid=True)
    _reminder_touch(cfg); _reminder_save("reminder_text_insert")
    try:
        bot.delete_message(chat_id, msg.message_id)
    except Exception:
        pass
    _reminder_refresh_bound_editor(rid)


@bot.callback_query_handler(func=lambda c: str(getattr(c, "data", "") or "").startswith("rem:"))
def reminder_callback(call):
    chat_id = int(call.message.chat.id)
    if not is_owner_chat(chat_id):
        try:
            bot.answer_callback_query(call.id, "Напоминалка доступна только владельцу", show_alert=True)
        except Exception:
            pass
        return
    raw = str(call.data or ""); parts = raw.split(":")
    action = parts[1] if len(parts) > 1 else "menu"
    try:
        bot.answer_callback_query(call.id)
    except Exception:
        pass

    # v134 старые кнопки rem:menu:<day> продолжают открывать новый список.
    if action == "menu":
        day_key = parts[2] if len(parts) > 2 else today_key()
        safe_edit(bot, call, build_reminder_list_text(), reply_markup=build_reminder_list_keyboard(day_key, 0))
        return

    # Кнопки старого одиночного окна v134 могут физически остаться в Telegram после deploy.
    # Вместо исключения/битой операции мягко переводим их в новый список.
    legacy_min_parts = {
        "dates": 5, "calendar": 7, "date": 7, "noend": 5, "interval": 5,
        "intset": 6, "hours": 6, "hourset": 7, "chats": 6, "chat": 7,
        "toggle": 5, "delete_confirm": 5, "delete": 5,
    }
    if action in {"text", "intcustom", "cancelinput"} or (action in legacy_min_parts and len(parts) < legacy_min_parts[action]):
        old_day = next((x for x in reversed(parts) if re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(x or ""))), today_key())
        safe_edit(bot, call, build_reminder_list_text(), reply_markup=build_reminder_list_keyboard(old_day, 0))
        return
    if action == "list":
        page = int(parts[2]) if len(parts) > 2 else 0
        day_key = parts[3] if len(parts) > 3 else today_key()
        safe_edit(bot, call, build_reminder_list_text(), reply_markup=build_reminder_list_keyboard(day_key, page))
        return
    if action == "add":
        page = int(parts[2]) if len(parts) > 2 else 0
        day_key = parts[3] if len(parts) > 3 else today_key()
        rid, _cfg = _reminder_create()
        _reminder_bind_editor(rid, chat_id, call.message.message_id, day_key, page)
        safe_edit(bot, call, build_reminder_menu_text(rid), reply_markup=build_reminder_menu_keyboard(rid, day_key, page, viewer_chat_id=chat_id))
        return
    if action == "open":
        rid = int(parts[2]); page = int(parts[3]) if len(parts) > 3 else 0
        day_key = parts[4] if len(parts) > 4 else today_key()
        if not _reminder_cfg(rid):
            safe_edit(bot, call, build_reminder_list_text(), reply_markup=build_reminder_list_keyboard(day_key, page)); return
        _reminder_bind_editor(rid, chat_id, call.message.message_id, day_key, page)
        safe_edit(bot, call, build_reminder_menu_text(rid), reply_markup=build_reminder_menu_keyboard(rid, day_key, page, viewer_chat_id=chat_id))
        return
    if action == "editmenu":
        rid = int(parts[2]); page = int(parts[3]); day_key = parts[4] if len(parts) > 4 else today_key()
        _reminder_bind_editor(rid, chat_id, call.message.message_id, day_key, page)
        safe_edit(bot, call, f"Изменить📝 напоминалку №{_reminder_position(rid) or rid}", reply_markup=_reminder_edit_menu_keyboard(rid, page, day_key, chat_id))
        return
    if action == "dates":
        rid = int(parts[2]); page = int(parts[3]); day_key = parts[4]
        safe_edit(bot, call, _reminder_dates_text(rid), reply_markup=_reminder_dates_keyboard(rid, page, day_key)); return
    if action == "calendar":
        which = parts[2] if len(parts) > 2 else "start"; ym = parts[3]
        rid = int(parts[4]); page = int(parts[5]); day_key = parts[6]
        try:
            year, month = [int(x) for x in ym.split("-", 1)]
        except Exception:
            year, month = now_local().year, now_local().month
        safe_edit(bot, call, _reminder_calendar_text(which, year, month), reply_markup=_reminder_calendar_keyboard(which, year, month, rid, page, day_key)); return
    if action == "date":
        which, date_key = parts[2], parts[3]; rid = int(parts[4]); page = int(parts[5]); day_key = parts[6]
        cfg = _reminder_cfg(rid); selected = _reminder_parse_date(date_key)
        if not cfg or selected is None:
            return
        if which == "start":
            cfg["start_date"] = date_key
            end = _reminder_parse_date(cfg.get("end_date"))
            if end and end < selected:
                cfg["end_date"] = date_key
        else:
            start = _reminder_parse_date(cfg.get("start_date")) or selected
            cfg["end_date"] = date_key if selected >= start else start.strftime("%Y-%m-%d")
        if cfg.get("enabled"):
            _reminder_rearm(cfg, immediate_if_valid=True)
        _reminder_touch(cfg); _reminder_save("reminder_dates")
        safe_edit(bot, call, _reminder_dates_text(rid), reply_markup=_reminder_dates_keyboard(rid, page, day_key)); return
    if action == "noend":
        rid = int(parts[2]); page = int(parts[3]); day_key = parts[4]; cfg = _reminder_cfg(rid)
        if not cfg: return
        cfg["end_date"] = ""
        if cfg.get("enabled"): _reminder_rearm(cfg, immediate_if_valid=True)
        _reminder_touch(cfg); _reminder_save("reminder_noend")
        safe_edit(bot, call, _reminder_dates_text(rid), reply_markup=_reminder_dates_keyboard(rid, page, day_key)); return
    if action == "interval":
        rid = int(parts[2]); page = int(parts[3]); day_key = parts[4]
        safe_edit(bot, call, "🔁 ПЕРИОДИЧНОСТЬ\n\nКак часто присылать напоминание?", reply_markup=_reminder_interval_keyboard(rid, page, day_key, chat_id)); return
    if action == "intset":
        minutes = int(parts[2]); rid = int(parts[3]); page = int(parts[4]); day_key = parts[5]; cfg = _reminder_cfg(rid)
        if not cfg: return
        cfg["interval_minutes"] = max(5, int(minutes))
        if cfg.get("enabled"): _reminder_rearm(cfg, immediate_if_valid=True)
        _reminder_touch(cfg); _reminder_save("reminder_interval")
        safe_edit(bot, call, build_reminder_menu_text(rid), reply_markup=build_reminder_menu_keyboard(rid, day_key, page, viewer_chat_id=chat_id)); return
    if action == "hours":
        which = parts[2]; rid = int(parts[3]); page = int(parts[4]); day_key = parts[5]
        label = "С КАКОГО ЧАСА" if which == "start" else "ДО КАКОГО ЧАСА"
        safe_edit(bot, call, f"🕐 {label}\n\nВыберите час.", reply_markup=_reminder_hours_keyboard(which, rid, page, day_key)); return
    if action == "hourset":
        which = parts[2]; hour = max(0, min(23, int(parts[3]))); rid = int(parts[4]); page = int(parts[5]); day_key = parts[6]
        cfg = _reminder_cfg(rid)
        if not cfg: return
        if which == "start":
            cfg["start_hour"] = hour
            if int(cfg.get("end_hour", 22)) < hour: cfg["end_hour"] = hour
        else:
            cfg["end_hour"] = hour
            if int(cfg.get("start_hour", 8)) > hour: cfg["start_hour"] = hour
        if cfg.get("enabled"): _reminder_rearm(cfg, immediate_if_valid=True)
        _reminder_touch(cfg); _reminder_save("reminder_hours")
        safe_edit(bot, call, build_reminder_menu_text(rid), reply_markup=build_reminder_menu_keyboard(rid, day_key, page, viewer_chat_id=chat_id)); return
    if action == "chats":
        rid = int(parts[2]); chat_page = int(parts[3]); list_page = int(parts[4]); day_key = parts[5]
        safe_edit(bot, call, "💬 ЧАТЫ ДЛЯ НАПОМИНАЛКИ\n\nНажимайте — ✅ выбран / ⬜ не выбран.", reply_markup=_reminder_chats_keyboard(rid, chat_page, list_page, day_key)); return
    if action == "chat":
        target = int(parts[2]); rid = int(parts[3]); chat_page = int(parts[4]); list_page = int(parts[5]); day_key = parts[6]
        cfg = _reminder_cfg(rid)
        if not cfg: return
        selected = {int(x) for x in (cfg.get("chat_ids") or []) if str(x).lstrip("-").isdigit()}
        if target in selected:
            selected.remove(target)
            old_mid = (cfg.get("last_message_ids") or {}).pop(str(target), None)
            if old_mid:
                try: bot.delete_message(target, int(old_mid))
                except Exception: pass
        else:
            selected.add(target)
        cfg["chat_ids"] = sorted(selected)
        if cfg.get("enabled"): _reminder_rearm(cfg, immediate_if_valid=True)
        _reminder_touch(cfg); _reminder_save("reminder_chats")
        safe_edit(bot, call, "💬 ЧАТЫ ДЛЯ НАПОМИНАЛКИ\n\nНажимайте — ✅ выбран / ⬜ не выбран.", reply_markup=_reminder_chats_keyboard(rid, chat_page, list_page, day_key)); return
    if action == "toggle":
        rid = int(parts[2]); page = int(parts[3]); day_key = parts[4]; cfg = _reminder_cfg(rid)
        if not cfg: return
        if not cfg.get("enabled"):
            if not str(cfg.get("text") or "").strip():
                send_and_auto_delete(chat_id, "❌ Сначала задайте текст напоминания.", 8); return
            if not (cfg.get("chat_ids") or []):
                send_and_auto_delete(chat_id, "❌ Сначала выберите хотя бы один чат.", 8); return
            cfg["enabled"] = True; _reminder_rearm(cfg, immediate_if_valid=True)
        else:
            cfg["enabled"] = False; cfg["next_run_at"] = ""
        _reminder_touch(cfg); _reminder_save("reminder_toggle")
        safe_edit(bot, call, build_reminder_menu_text(rid), reply_markup=build_reminder_menu_keyboard(rid, day_key, page, viewer_chat_id=chat_id)); return
    if action == "delete_confirm":
        rid = int(parts[2]); page = int(parts[3]); day_key = parts[4]
        kb = types.InlineKeyboardMarkup(row_width=2)
        kb.row(IB("🗑 Да, удалить", callback_data=f"rem:delete:{rid}:{page}:{day_key}"), IB("✖️ Отмена", callback_data=f"rem:open:{rid}:{page}:{day_key}"))
        safe_edit(bot, call, "🗑 Удалить напоминалку?\n\nПоследние отправленные сообщения этой напоминалки тоже будут удалены из выбранных чатов.", reply_markup=kb); return
    if action == "delete":
        rid = int(parts[2]); page = int(parts[3]); day_key = parts[4]
        root = _reminders_root(); cfg = _reminder_cfg(rid)
        if cfg: _reminder_delete_last_messages(cfg)
        root.setdefault("items", {}).pop(str(rid), None); _reminder_unbind(rid); _reminder_save("reminder_delete")
        rows = _reminder_items(); pages = max(1, (len(rows) + _REMINDER_LIST_PAGE_SIZE - 1) // _REMINDER_LIST_PAGE_SIZE); page = min(page, pages - 1)
        safe_edit(bot, call, build_reminder_list_text(), reply_markup=build_reminder_list_keyboard(day_key, page)); return

# v138_parallel_lanes_ui_ack
