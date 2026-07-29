# v134_flat_reminder

_REMINDER_THREAD_STARTED = False
_REMINDER_THREAD_LOCK = threading.RLock()
_REMINDER_CHECK_SECONDS = 15.0
_REMINDER_MONTHS_RU = [
    "Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
    "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь",
]


def _reminder_owner_id() -> int | None:
    try:
        return int(OWNER_ID) if OWNER_ID else None
    except Exception:
        return None


def _reminder_cfg() -> dict:
    gs = data.setdefault("_global_settings", {})
    cfg = gs.setdefault("reminder", {})
    if not isinstance(cfg, dict):
        cfg = {}
        gs["reminder"] = cfg
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
    cfg.setdefault("input_wait", None)
    return cfg


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
        h = minutes // 60
        return f"{h} ч"
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


def _reminder_rearm(cfg: dict | None = None, immediate_if_valid: bool = True) -> None:
    cfg = cfg or _reminder_cfg()
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
    rows = []
    seen = set()
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


def build_reminder_menu_text() -> str:
    cfg = _reminder_cfg()
    _reminder_normalize_hours(cfg)
    txt = str(cfg.get("text") or "").strip()
    preview = txt.replace("\n", " ")
    if len(preview) > 120:
        preview = preview[:117] + "..."
    end_date = _reminder_fmt_date(cfg.get("end_date")) if cfg.get("end_date") else "без конца"
    state = "✅ ВКЛ" if cfg.get("enabled") else "⏸ ВЫКЛ"
    chats_count = len([x for x in (cfg.get("chat_ids") or []) if str(x).strip()])
    return (
        "⏰ НАПОМИНАЛКА\n\n"
        f"Состояние: {state}\n"
        f"Текст: {preview or 'не задан'}\n"
        f"Даты: {_reminder_fmt_date(cfg.get('start_date'))} → {end_date}\n"
        f"Время: {int(cfg.get('start_hour', 8)):02d}:00 → {int(cfg.get('end_hour', 22)):02d}:59\n"
        f"Период: каждые {_reminder_interval_label(cfg.get('interval_minutes', 120))}\n"
        f"Чаты: {chats_count}\n"
        f"Следующая: {_reminder_fmt_dt(cfg.get('next_run_at'))}\n"
        f"Последняя: {_reminder_fmt_dt(cfg.get('last_sent_at'))}"
    )


def build_reminder_menu_keyboard(day_key: str | None = None):
    day_key = day_key or today_key()
    cfg = _reminder_cfg()
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.row(
        IB("✍️ Текст", callback_data=f"rem:text:{day_key}"),
        IB(f"💬 Чаты ({len(cfg.get('chat_ids') or [])})", callback_data=f"rem:chats:0:{day_key}"),
    )
    kb.row(
        IB("📅 Даты", callback_data=f"rem:dates:{day_key}"),
        IB(f"🔁 {_reminder_interval_label(cfg.get('interval_minutes', 120))}", callback_data=f"rem:interval:{day_key}"),
    )
    kb.row(
        IB(f"🕗 С {int(cfg.get('start_hour', 8)):02d}:00", callback_data=f"rem:hours:start:{day_key}"),
        IB(f"🕙 До {int(cfg.get('end_hour', 22)):02d}:59", callback_data=f"rem:hours:end:{day_key}"),
    )
    kb.row(
        IB("⏸ Отключить" if cfg.get("enabled") else "✅ Включить", callback_data=f"rem:toggle:{day_key}"),
        IB("🗑 Удалить", callback_data=f"rem:delete_confirm:{day_key}"),
    )
    kb.row(IB("⬅️ Назад", callback_data=f"d:{day_key}:back_main"))
    return kb


def _reminder_dates_text() -> str:
    cfg = _reminder_cfg()
    end = _reminder_fmt_date(cfg.get("end_date")) if cfg.get("end_date") else "без конца"
    return f"📅 ДАТЫ НАПОМИНАЛКИ\n\nНачало: {_reminder_fmt_date(cfg.get('start_date'))}\nКонец: {end}"


def _reminder_dates_keyboard(day_key: str):
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.row(
        IB("📅 Начало", callback_data=f"rem:calendar:start:{now_local().strftime('%Y-%m')}:{day_key}"),
        IB("🏁 Конец", callback_data=f"rem:calendar:end:{now_local().strftime('%Y-%m')}:{day_key}"),
    )
    kb.row(IB("♾ Без конца", callback_data=f"rem:noend:{day_key}"))
    kb.row(IB("⬅️ Назад", callback_data=f"rem:menu:{day_key}"))
    return kb


def _reminder_calendar_text(which: str, year: int, month: int) -> str:
    target = "начала" if which == "start" else "окончания"
    return f"📅 Выберите дату {target}\n\n{_REMINDER_MONTHS_RU[month-1]} {year}"


def _reminder_calendar_keyboard(which: str, year: int, month: int, day_key: str):
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
                row.append(IB(str(d), callback_data=f"rem:date:{which}:{date_key}:{day_key}"))
        kb.row(*row)
    prev_month = month - 1
    prev_year = year
    if prev_month < 1:
        prev_month = 12
        prev_year -= 1
    next_month = month + 1
    next_year = year
    if next_month > 12:
        next_month = 1
        next_year += 1
    kb.row(
        IB("⬅️", callback_data=f"rem:calendar:{which}:{prev_year:04d}-{prev_month:02d}:{day_key}"),
        IB("📅 Даты", callback_data=f"rem:dates:{day_key}"),
        IB("➡️", callback_data=f"rem:calendar:{which}:{next_year:04d}-{next_month:02d}:{day_key}"),
    )
    return kb


def _reminder_interval_keyboard(day_key: str):
    cfg = _reminder_cfg()
    current = int(cfg.get("interval_minutes", 120) or 120)
    kb = types.InlineKeyboardMarkup(row_width=3)
    options = [30, 60, 120, 180, 240, 360, 720, 1440]
    buttons = []
    for minutes in options:
        mark = "✅ " if minutes == current else ""
        buttons.append(IB(mark + _reminder_interval_label(minutes), callback_data=f"rem:intset:{minutes}:{day_key}"))
    for i in range(0, len(buttons), 3):
        kb.row(*buttons[i:i+3])
    kb.row(IB("✍️ Свой интервал", callback_data=f"rem:intcustom:{day_key}"))
    kb.row(IB("⬅️ Назад", callback_data=f"rem:menu:{day_key}"))
    return kb


def _reminder_hours_keyboard(which: str, day_key: str):
    cfg = _reminder_cfg()
    current = int(cfg.get("start_hour" if which == "start" else "end_hour", 8 if which == "start" else 22))
    kb = types.InlineKeyboardMarkup(row_width=4)
    buttons = []
    for hour in range(24):
        mark = "✅ " if hour == current else ""
        buttons.append(IB(f"{mark}{hour:02d}:00", callback_data=f"rem:hourset:{which}:{hour}:{day_key}"))
    for i in range(0, 24, 4):
        kb.row(*buttons[i:i+4])
    kb.row(IB("⬅️ Назад", callback_data=f"rem:menu:{day_key}"))
    return kb


def _reminder_chats_keyboard(page: int, day_key: str):
    cfg = _reminder_cfg()
    selected = {int(x) for x in (cfg.get("chat_ids") or []) if str(x).lstrip("-").isdigit()}
    rows = _reminder_known_chats()
    per_page = 8
    pages = max(1, (len(rows) + per_page - 1) // per_page)
    page = max(0, min(int(page), pages - 1))
    kb = types.InlineKeyboardMarkup(row_width=1)
    for cid, title in rows[page * per_page:(page + 1) * per_page]:
        mark = "✅" if cid in selected else "⬜"
        name = chat_button_title(cid, title) if "chat_button_title" in globals() else str(title)
        kb.row(IB(f"{mark} {name}", callback_data=f"rem:chat:{cid}:{page}:{day_key}"))
    nav = []
    if page > 0:
        nav.append(IB("⬅️", callback_data=f"rem:chats:{page-1}:{day_key}"))
    nav.append(IB(f"{page+1}/{pages}", callback_data="none"))
    if page + 1 < pages:
        nav.append(IB("➡️", callback_data=f"rem:chats:{page+1}:{day_key}"))
    kb.row(*nav)
    kb.row(IB("⬅️ Назад", callback_data=f"rem:menu:{day_key}"))
    return kb


def _reminder_wait_active(chat_id: int, wait_type: str | None = None) -> bool:
    owner = _reminder_owner_id()
    if owner is None or int(chat_id) != owner:
        return False
    wait = _reminder_cfg().get("input_wait") or {}
    if not isinstance(wait, dict):
        return False
    try:
        if float(wait.get("expires_at", 0) or 0) < time.time():
            _reminder_cfg()["input_wait"] = None
            _reminder_save("reminder_wait_expired")
            return False
    except Exception:
        pass
    if wait_type is not None and wait.get("type") != wait_type:
        return False
    return bool(wait.get("type"))


def _reminder_set_wait(call, wait_type: str, day_key: str) -> None:
    cfg = _reminder_cfg()
    cfg["input_wait"] = {
        "type": wait_type,
        "day_key": day_key,
        "message_id": int(call.message.message_id),
        "expires_at": time.time() + 180,
    }
    _reminder_save("reminder_input_wait")


def _reminder_parse_custom_interval(text: str) -> int | None:
    value = str(text or "").strip().lower().replace(",", ".")
    m = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*(мин|м|min|m|ч|час|часа|часов|h|д|дн|день|дня|дней|d)?\s*", value)
    if not m:
        return None
    num = float(m.group(1))
    unit = m.group(2) or "мин"
    if unit in {"ч", "час", "часа", "часов", "h"}:
        minutes = int(round(num * 60))
    elif unit in {"д", "дн", "день", "дня", "дней", "d"}:
        minutes = int(round(num * 1440))
    else:
        minutes = int(round(num))
    if minutes < 5 or minutes > 43200:
        return None
    return minutes


def _reminder_edit_back(chat_id: int, message_id: int, day_key: str) -> None:
    try:
        bot.edit_message_text(
            build_reminder_menu_text(),
            chat_id=chat_id,
            message_id=message_id,
            reply_markup=build_reminder_menu_keyboard(day_key),
        )
    except Exception as exc:
        log_error(f"reminder edit back: {exc}")


def _reminder_delete_last_messages(cfg: dict | None = None) -> None:
    cfg = cfg or _reminder_cfg()
    last_map = cfg.get("last_message_ids") or {}
    for cid_raw, mid_raw in list(last_map.items()):
        try:
            bot.delete_message(int(cid_raw), int(mid_raw))
        except Exception:
            pass
    cfg["last_message_ids"] = {}


def _reminder_send_cycle() -> bool:
    cfg = _reminder_cfg()
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
                bot_journal("reminder_sent", cid, f"message_id={sent.message_id}")
            except Exception:
                pass
        except Exception as exc:
            log_error(f"reminder send {cid}: {exc}")
            try:
                bot_journal("reminder_send_error", cid, str(exc), "ERROR")
            except Exception:
                pass
    return sent_any


def _reminder_tick() -> None:
    cfg = _reminder_cfg()
    if not bool(cfg.get("enabled")):
        return
    now_dt = now_local()
    if not str(cfg.get("text") or "").strip() or not (cfg.get("chat_ids") or []):
        return
    due = _reminder_parse_dt(cfg.get("next_run_at"))
    if due is None:
        _reminder_rearm(cfg, immediate_if_valid=True)
        _reminder_save("reminder_rearm")
        due = _reminder_parse_dt(cfg.get("next_run_at"))
    if due is None or now_dt < due:
        return
    if not _reminder_date_allowed(now_dt, cfg) or not _reminder_time_allowed(now_dt, cfg):
        next_dt = _reminder_next_valid_start(now_dt, cfg)
        cfg["next_run_at"] = next_dt.isoformat(timespec="seconds") if next_dt else ""
        if next_dt is None:
            cfg["enabled"] = False
        _reminder_save("reminder_window_shift")
        return
    if _reminder_send_cycle():
        cfg["last_sent_at"] = now_dt.isoformat(timespec="seconds")
    _reminder_advance_after_send(now_dt, cfg)
    _reminder_save("reminder_sent")


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
        threading.Thread(target=_reminder_scheduler_loop, name="reminder-scheduler", daemon=True).start()


@bot.message_handler(func=lambda m: _reminder_wait_active(getattr(getattr(m, "chat", None), "id", 0)), content_types=["text"])
def reminder_input_message(msg):
    chat_id = int(msg.chat.id)
    if not is_owner_chat(chat_id):
        return
    cfg = _reminder_cfg()
    wait = cfg.get("input_wait") or {}
    wait_type = str(wait.get("type") or "")
    day_key = str(wait.get("day_key") or today_key())
    menu_message_id = int(wait.get("message_id") or 0)
    text = str(getattr(msg, "text", "") or "").strip()
    if wait_type == "text":
        if not text:
            send_and_auto_delete(chat_id, "❌ Текст пустой.", 8)
            return
        if len(text) > 4000:
            send_and_auto_delete(chat_id, "❌ Текст слишком длинный. Максимум 4000 символов.", 10)
            return
        cfg["text"] = text
        cfg["input_wait"] = None
        if cfg.get("enabled"):
            _reminder_rearm(cfg, immediate_if_valid=True)
        _reminder_save("reminder_text")
    elif wait_type == "interval":
        minutes = _reminder_parse_custom_interval(text)
        if minutes is None:
            send_and_auto_delete(chat_id, "❌ Пример: 90 мин, 2 ч, 1 день.", 10)
            return
        cfg["interval_minutes"] = int(minutes)
        cfg["input_wait"] = None
        if cfg.get("enabled"):
            _reminder_rearm(cfg, immediate_if_valid=True)
        _reminder_save("reminder_interval")
    else:
        cfg["input_wait"] = None
        _reminder_save("reminder_wait_clear")
        return
    try:
        bot.delete_message(chat_id, msg.message_id)
    except Exception:
        pass
    if menu_message_id:
        _reminder_edit_back(chat_id, menu_message_id, day_key)


@bot.callback_query_handler(func=lambda c: str(getattr(c, "data", "") or "").startswith("rem:"))
def reminder_callback(call):
    chat_id = int(call.message.chat.id)
    if not is_owner_chat(chat_id):
        try:
            bot.answer_callback_query(call.id, "Напоминалка доступна только владельцу", show_alert=True)
        except Exception:
            pass
        return
    raw = str(call.data or "")
    parts = raw.split(":")
    action = parts[1] if len(parts) > 1 else "menu"
    cfg = _reminder_cfg()
    try:
        bot.answer_callback_query(call.id)
    except Exception:
        pass

    if action == "menu":
        day_key = parts[2] if len(parts) > 2 else today_key()
        safe_edit(bot, call, build_reminder_menu_text(), reply_markup=build_reminder_menu_keyboard(day_key))
        return
    if action == "text":
        day_key = parts[2] if len(parts) > 2 else today_key()
        _reminder_set_wait(call, "text", day_key)
        kb = types.InlineKeyboardMarkup()
        kb.row(IB("✖️ Отмена", callback_data=f"rem:cancelinput:{day_key}"))
        safe_edit(bot, call, "✍️ Пришлите одним сообщением текст напоминания.\n\nРежим отменится сам через 3 минуты.", reply_markup=kb)
        return
    if action == "intcustom":
        day_key = parts[2] if len(parts) > 2 else today_key()
        _reminder_set_wait(call, "interval", day_key)
        kb = types.InlineKeyboardMarkup()
        kb.row(IB("✖️ Отмена", callback_data=f"rem:cancelinput:{day_key}"))
        safe_edit(bot, call, "🔁 Напишите период.\n\nПримеры: 90 мин, 2 ч, 1 день.\nМинимум 5 минут.", reply_markup=kb)
        return
    if action == "cancelinput":
        day_key = parts[2] if len(parts) > 2 else today_key()
        cfg["input_wait"] = None
        _reminder_save("reminder_input_cancel")
        safe_edit(bot, call, build_reminder_menu_text(), reply_markup=build_reminder_menu_keyboard(day_key))
        return
    if action == "dates":
        day_key = parts[2] if len(parts) > 2 else today_key()
        safe_edit(bot, call, _reminder_dates_text(), reply_markup=_reminder_dates_keyboard(day_key))
        return
    if action == "calendar":
        which = parts[2] if len(parts) > 2 else "start"
        ym = parts[3] if len(parts) > 3 else now_local().strftime("%Y-%m")
        day_key = parts[4] if len(parts) > 4 else today_key()
        try:
            year, month = [int(x) for x in ym.split("-", 1)]
        except Exception:
            year, month = now_local().year, now_local().month
        safe_edit(bot, call, _reminder_calendar_text(which, year, month), reply_markup=_reminder_calendar_keyboard(which, year, month, day_key))
        return
    if action == "date":
        which = parts[2] if len(parts) > 2 else "start"
        date_key = parts[3] if len(parts) > 3 else today_key()
        day_key = parts[4] if len(parts) > 4 else today_key()
        selected = _reminder_parse_date(date_key)
        if selected is None:
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
        _reminder_save("reminder_dates")
        safe_edit(bot, call, _reminder_dates_text(), reply_markup=_reminder_dates_keyboard(day_key))
        return
    if action == "noend":
        day_key = parts[2] if len(parts) > 2 else today_key()
        cfg["end_date"] = ""
        if cfg.get("enabled"):
            _reminder_rearm(cfg, immediate_if_valid=True)
        _reminder_save("reminder_noend")
        safe_edit(bot, call, _reminder_dates_text(), reply_markup=_reminder_dates_keyboard(day_key))
        return
    if action == "interval":
        day_key = parts[2] if len(parts) > 2 else today_key()
        safe_edit(bot, call, "🔁 ПЕРИОДИЧНОСТЬ\n\nКак часто присылать напоминание?", reply_markup=_reminder_interval_keyboard(day_key))
        return
    if action == "intset":
        minutes = int(parts[2])
        day_key = parts[3] if len(parts) > 3 else today_key()
        cfg["interval_minutes"] = minutes
        if cfg.get("enabled"):
            _reminder_rearm(cfg, immediate_if_valid=True)
        _reminder_save("reminder_interval")
        safe_edit(bot, call, build_reminder_menu_text(), reply_markup=build_reminder_menu_keyboard(day_key))
        return
    if action == "hours":
        which = parts[2] if len(parts) > 2 else "start"
        day_key = parts[3] if len(parts) > 3 else today_key()
        label = "С КАКОГО ЧАСА" if which == "start" else "ДО КАКОГО ЧАСА"
        safe_edit(bot, call, f"🕐 {label}\n\nВыберите час.", reply_markup=_reminder_hours_keyboard(which, day_key))
        return
    if action == "hourset":
        which = parts[2] if len(parts) > 2 else "start"
        hour = max(0, min(23, int(parts[3])))
        day_key = parts[4] if len(parts) > 4 else today_key()
        if which == "start":
            cfg["start_hour"] = hour
            if int(cfg.get("end_hour", 22)) < hour:
                cfg["end_hour"] = hour
        else:
            cfg["end_hour"] = hour
            if int(cfg.get("start_hour", 8)) > hour:
                cfg["start_hour"] = hour
        if cfg.get("enabled"):
            _reminder_rearm(cfg, immediate_if_valid=True)
        _reminder_save("reminder_hours")
        safe_edit(bot, call, build_reminder_menu_text(), reply_markup=build_reminder_menu_keyboard(day_key))
        return
    if action == "chats":
        page = int(parts[2]) if len(parts) > 2 else 0
        day_key = parts[3] if len(parts) > 3 else today_key()
        safe_edit(bot, call, "💬 ЧАТЫ ДЛЯ НАПОМИНАЛКИ\n\nНажимайте — ✅ выбран / ⬜ не выбран.", reply_markup=_reminder_chats_keyboard(page, day_key))
        return
    if action == "chat":
        target = int(parts[2])
        page = int(parts[3]) if len(parts) > 3 else 0
        day_key = parts[4] if len(parts) > 4 else today_key()
        selected = {int(x) for x in (cfg.get("chat_ids") or []) if str(x).lstrip("-").isdigit()}
        if target in selected:
            selected.remove(target)
            old_mid = (cfg.get("last_message_ids") or {}).pop(str(target), None)
            if old_mid:
                try:
                    bot.delete_message(target, int(old_mid))
                except Exception:
                    pass
        else:
            selected.add(target)
        cfg["chat_ids"] = sorted(selected)
        if cfg.get("enabled"):
            _reminder_rearm(cfg, immediate_if_valid=True)
        _reminder_save("reminder_chats")
        safe_edit(bot, call, "💬 ЧАТЫ ДЛЯ НАПОМИНАЛКИ\n\nНажимайте — ✅ выбран / ⬜ не выбран.", reply_markup=_reminder_chats_keyboard(page, day_key))
        return
    if action == "toggle":
        day_key = parts[2] if len(parts) > 2 else today_key()
        if not cfg.get("enabled"):
            if not str(cfg.get("text") or "").strip():
                send_and_auto_delete(chat_id, "❌ Сначала задайте текст напоминания.", 8)
                return
            if not (cfg.get("chat_ids") or []):
                send_and_auto_delete(chat_id, "❌ Сначала выберите хотя бы один чат.", 8)
                return
            cfg["enabled"] = True
            _reminder_rearm(cfg, immediate_if_valid=True)
        else:
            cfg["enabled"] = False
            cfg["next_run_at"] = ""
        _reminder_save("reminder_toggle")
        safe_edit(bot, call, build_reminder_menu_text(), reply_markup=build_reminder_menu_keyboard(day_key))
        return
    if action == "delete_confirm":
        day_key = parts[2] if len(parts) > 2 else today_key()
        kb = types.InlineKeyboardMarkup(row_width=2)
        kb.row(
            IB("🗑 Да, удалить", callback_data=f"rem:delete:{day_key}"),
            IB("✖️ Отмена", callback_data=f"rem:menu:{day_key}"),
        )
        safe_edit(bot, call, "🗑 Удалить напоминалку?\n\nПоследние отправленные сообщения тоже будут удалены из выбранных чатов.", reply_markup=kb)
        return
    if action == "delete":
        day_key = parts[2] if len(parts) > 2 else today_key()
        _reminder_delete_last_messages(cfg)
        cfg.clear()
        _reminder_cfg()
        _reminder_save("reminder_delete")
        safe_edit(bot, call, build_reminder_menu_text(), reply_markup=build_reminder_menu_keyboard(day_key))
        return

# v134_flat_reminder
