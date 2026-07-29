# v134_flat_reminder
# ─────────────────────────────────────────────────────────────
# ⚡ Fast UI edit queue
# ─────────────────────────────────────────────────────────────
# Telegram даёт 429, если слишком часто редактировать одно окно.
# Поэтому кнопки больше не ждут retry_after внутри callback:
# • редактирование одного сообщения ограничено частотой;
# • частые клики собираются в одно последнее обновление;
# • 429 не держит обработчик кнопки, а просто пропускает лишнее обновление.
UI_EDIT_MIN_INTERVAL_SECONDS = float(os.getenv("UI_EDIT_MIN_INTERVAL_SECONDS", "0.20") or "0.20")
_ui_edit_lock = threading.RLock()
_ui_edit_last_ts = {}
_ui_edit_pending = {}
_ui_edit_timers = {}


def _ui_edit_key(chat_id: int, message_id: int):
    return (int(chat_id), int(message_id))


def _perform_fast_ui_edit(payload: dict) -> str:
    chat_id = int(payload.get("chat_id"))
    message_id = int(payload.get("message_id"))
    text = payload.get("text") or ""
    reply_markup = payload.get("reply_markup")
    parse_mode = payload.get("parse_mode")
    purpose = payload.get("purpose") or "fast_ui_edit"
    try:
        _tg_call_retry(
            bot.edit_message_text,
            text,
            chat_id=chat_id,
            message_id=message_id,
            reply_markup=reply_markup,
            parse_mode=parse_mode,
            attempts=1,
            purpose=purpose + "_text",
        )
        return "ok"
    except Exception as e1:
        low = str(e1).lower()
        if "message is not modified" in low:
            return "ok"
        if is_telegram_429(e1):
            try:
                bot_journal("ui_edit_rate_limited", chat_id, f"{purpose}: {str(e1)[:220]}", "WARN")
            except Exception:
                pass
            return "rate_limited"
        if "message to edit not found" in low or "message can't be edited" in low:
            return "not_found"
        try:
            _tg_call_retry(
                bot.edit_message_caption,
                chat_id=chat_id,
                message_id=message_id,
                caption=text,
                reply_markup=reply_markup,
                parse_mode=parse_mode,
                attempts=1,
                purpose=purpose + "_caption",
            )
            return "ok"
        except Exception as e2:
            low2 = str(e2).lower()
            if "message is not modified" in low2:
                return "ok"
            if is_telegram_429(e2):
                try:
                    bot_journal("ui_edit_rate_limited", chat_id, f"{purpose}: {str(e2)[:220]}", "WARN")
                except Exception:
                    pass
                return "rate_limited"
            if "message to edit not found" in low2 or "message can't be edited" in low2:
                return "not_found"
            try:
                bot_journal("ui_edit_failed", chat_id, f"{purpose}: {str(e1)[:180]} / {str(e2)[:180]}", "WARN")
            except Exception:
                pass
            return "failed"


def _run_pending_ui_edit(key):
    with _ui_edit_lock:
        payload = _ui_edit_pending.pop(key, None)
        _ui_edit_timers.pop(key, None)
        if not payload:
            return
        _ui_edit_last_ts[key] = time.time()
    _perform_fast_ui_edit(payload)


def fast_ui_edit_message_text(chat_id: int, message_id: int, text: str, reply_markup=None, parse_mode=None, purpose: str = "fast_ui") -> str:
    key = _ui_edit_key(chat_id, message_id)
    payload = {
        "chat_id": int(chat_id),
        "message_id": int(message_id),
        "text": text,
        "reply_markup": reply_markup,
        "parse_mode": parse_mode,
        "purpose": purpose,
    }
    now_ts = time.time()
    with _ui_edit_lock:
        last_ts = float(_ui_edit_last_ts.get(key, 0) or 0)
        wait = max(0.0, effective_ui_edit_interval() - (now_ts - last_ts))
        if wait > 0:
            _ui_edit_pending[key] = payload
            scheduler_key = f"ui-edit:{int(chat_id)}:{int(message_id)}"
            DELAYED_SCHEDULER.cancel(scheduler_key)
            deadline = DELAYED_SCHEDULER.schedule(
                scheduler_key,
                wait + 0.05,
                _run_pending_ui_edit,
                key,
            )
            _ui_edit_timers[key] = deadline
            return "scheduled"
        _ui_edit_last_ts[key] = now_ts
    return _perform_fast_ui_edit(payload)


def cancel_fast_ui_edit(chat_id: int, message_id: int):
    key = _ui_edit_key(chat_id, message_id)
    with _ui_edit_lock:
        _ui_edit_pending.pop(key, None)
        _ui_edit_timers.pop(key, None)
    DELAYED_SCHEDULER.cancel(f"ui-edit:{int(chat_id)}:{int(message_id)}")

def safe_edit(bot, call, text, reply_markup=None, parse_mode=None):
    """Быстрое обновление окна.
    Не держит callback при Telegram 429 и собирает частые клики в одно последнее обновление.
    """
    chat_id = call.message.chat.id
    msg_id = call.message.message_id
    try:
        text = auto_window_mark(
            text,
            getattr(call, "data", ""),
            owner_chat=is_owner_chat(chat_id),
            html_mode=(str(parse_mode or "").upper() == "HTML")
        )
    except Exception:
        pass
    if reply_markup is None:
        try:
            reply_markup = default_window_nav_keyboard(chat_id)
        except Exception:
            pass
    result = fast_ui_edit_message_text(
        chat_id, msg_id, text,
        reply_markup=reply_markup,
        parse_mode=parse_mode,
        purpose="safe_edit_fast",
    )
    if result in {"ok", "scheduled", "rate_limited"}:
        if result == "rate_limited":
            try:
                bot.answer_callback_query(call.id, "Обновление отложено: Telegram ограничил частые клики.", show_alert=False)
            except Exception:
                pass
        try:
            _touch_v98_auto_close_for_callback(chat_id, msg_id, getattr(call, "data", ""))
        except Exception:
            pass
        return

    # Только если старое сообщение реально потеряно, создаём новое окно.
    try:
        if chat_buttons_current_window_enabled(chat_id):
            try:
                bot.answer_callback_query(call.id, "Текущее окно недоступно, новое не создаю.", show_alert=False)
            except Exception:
                pass
            return
    except Exception:
        pass
    try:
        sent = _tg_call_retry(
            bot.send_message,
            chat_id,
            text,
            reply_markup=reply_markup,
            parse_mode=parse_mode,
            attempts=1,
            purpose="safe_edit_send_fallback",
        )
        try:
            _touch_v98_auto_close_for_callback(chat_id, sent.message_id, getattr(call, "data", ""))
        except Exception:
            pass
    except Exception as e:
        if not is_telegram_429(e):
            log_error(f"safe_edit fallback send {chat_id}: {e}")


def safe_edit_current_only(bot, call, text, reply_markup=None, parse_mode=None):
    """Редактирует только текущее окно, без создания нового и без ожидания retry_after."""
    chat_id = call.message.chat.id
    msg_id = call.message.message_id
    try:
        text = auto_window_mark(
            text,
            getattr(call, "data", ""),
            owner_chat=is_owner_chat(chat_id),
            html_mode=(str(parse_mode or "").upper() == "HTML")
        )
    except Exception:
        pass
    if reply_markup is None:
        try:
            reply_markup = default_window_nav_keyboard(chat_id)
        except Exception:
            pass
    result = fast_ui_edit_message_text(
        chat_id, msg_id, text,
        reply_markup=reply_markup,
        parse_mode=parse_mode,
        purpose="safe_edit_current_only_fast",
    )
    if result == "rate_limited":
        try:
            bot.answer_callback_query(call.id, "Обновление отложено: слишком много кликов.", show_alert=False)
        except Exception:
            pass
    try:
        _touch_v98_auto_close_for_callback(chat_id, msg_id, getattr(call, "data", ""))
    except Exception:
        pass
    return result in {"ok", "scheduled", "rate_limited"}

CATEGORY_PAGE_SAFE_CHARS = 3300


def _split_category_pages(text: str, limit: int = CATEGORY_PAGE_SAFE_CHARS) -> list[str]:
    text = str(text or "").strip()
    if not text:
        return [""]
    pages = []
    current = ""
    for raw_line in text.splitlines():
        line = raw_line
        # Split an abnormally long single line without cutting the whole Telegram message.
        chunks = []
        while len(line) > limit:
            cut = line.rfind(" ", 0, limit)
            if cut < max(200, limit // 3):
                cut = limit
            chunks.append(line[:cut].rstrip())
            line = line[cut:].lstrip()
        chunks.append(line)
        for chunk in chunks:
            candidate = chunk if not current else current + "\n" + chunk
            if len(candidate) > limit and current:
                pages.append(current.rstrip())
                current = chunk
            else:
                current = candidate
    if current or not pages:
        pages.append(current.rstrip())
    return pages


def _serialize_category_markup(markup) -> list[list[dict]]:
    rows = []
    for row in getattr(markup, "keyboard", []) or []:
        out = []
        for btn in row or []:
            item = {"text": str(getattr(btn, "text", "") or "")}
            cb = getattr(btn, "callback_data", None)
            if cb is not None:
                full = None
                try:
                    full = resolve_short_callback(str(cb))
                except Exception:
                    full = None
                item["callback_data"] = str(full or cb)
            url = getattr(btn, "url", None)
            if url:
                item["url"] = str(url)
            out.append(item)
        if out:
            rows.append(out)
    return rows


def _deserialize_category_markup(rows) -> object:
    kb = types.InlineKeyboardMarkup()
    for row in rows or []:
        buttons = []
        for item in row or []:
            text = str((item or {}).get("text") or "")
            cb = (item or {}).get("callback_data")
            url = (item or {}).get("url")
            if cb is not None:
                cb = str(cb)
                if cb.startswith("fvcat_"):
                    cb = fvcat_callback(cb)
                elif cb.startswith("cat_"):
                    cb = cat_callback(cb)
                else:
                    cb = make_short_callback(cb)
                buttons.append(IB(text, callback_data=cb))
            elif url:
                buttons.append(types.InlineKeyboardButton(text=text, url=str(url)))
            else:
                buttons.append(IB(text, callback_data="none"))
        if buttons:
            kb.row(*buttons)
    return kb


def _category_paged_keyboard(state: dict, page_index: int):
    kb = _deserialize_category_markup((state or {}).get("base_markup") or [])
    total = max(1, len((state or {}).get("pages") or []))
    page_index = max(0, min(int(page_index), total - 1))
    row = []
    if page_index > 0:
        row.append(IB("⬅️ Предыдущая", callback_data="cat_page:prev"))
    row.append(IB(f"{page_index + 1}/{total}", callback_data="none"))
    if page_index < total - 1:
        row.append(IB("Следующая ➡️", callback_data="cat_page:next"))
    kb.row(*row)
    return kb


def _category_page_text(state: dict, page_index: int) -> str:
    pages = (state or {}).get("pages") or [""]
    total = len(pages)
    page_index = max(0, min(int(page_index), total - 1))
    body = str(pages[page_index] or "").rstrip() + f"\n\n{page_index + 1}/{total}"
    return window_mark(body, str((state or {}).get("marker_code") or ""), html_mode=str((state or {}).get("parse_mode") or "").upper() == "HTML")


def _show_category_page(chat_id: int, message_id: int, requested) -> bool:
    store = get_chat_store(int(chat_id))
    state = store.get("categories_pagination") or {}
    pages = state.get("pages") or []
    if not pages:
        return False
    current = int(state.get("page", 0) or 0)
    if requested == "next":
        idx = current + 1
    elif requested == "prev":
        idx = current - 1
    else:
        try:
            idx = int(requested)
        except Exception:
            idx = current
    idx = max(0, min(idx, len(pages) - 1))
    state["page"] = idx
    store["categories_pagination"] = state
    text = _category_page_text(state, idx)
    kb = _category_paged_keyboard(state, idx)
    try:
        bot.edit_message_text(
            text, chat_id=int(chat_id), message_id=int(message_id), reply_markup=kb,
            parse_mode=(state.get("parse_mode") or None),
        )
    except Exception as exc:
        if "message is not modified" not in str(exc).lower():
            log_error(f"category page edit failed {chat_id}:{message_id}: {exc}")
            return False
    store["categories_msg_id"] = int(message_id)
    save_data(data, chat_ids=[int(chat_id)])
    return True


def send_or_edit_categories_window(chat_id, text, reply_markup=None, parse_mode=None, preferred_message_id=None, marker_action: str | None = None):
    """One categories window. Long article text is split into Telegram-safe pages instead of truncation."""
    store = get_chat_store(chat_id)
    base_reply_markup = reply_markup
    try:
        marker_key = marker_action or _window_key_from_markup(base_reply_markup)
        marker_code = _window_marker_code(marker_key, "Ф")
        body = strip_window_mark(str(text or ""))
        pages = _split_category_pages(body)
        if len(pages) > 1:
            state = {
                "pages": pages,
                "page": 0,
                "marker_code": marker_code,
                "parse_mode": str(parse_mode or ""),
                "base_markup": _serialize_category_markup(base_reply_markup),
                "marker_action": str(marker_action or ""),
            }
            store["categories_pagination"] = state
            text = _category_page_text(state, 0)
            reply_markup = _category_paged_keyboard(state, 0)
            try:
                bot_journal("categories_window_paginated", chat_id, f"pages={len(pages)} chars={len(body)} marker={marker_code}")
            except Exception:
                pass
        else:
            store.pop("categories_pagination", None)
            text = window_mark(body, marker_code, html_mode=(str(parse_mode or "").upper() == "HTML"))
    except Exception:
        pass

    store["categories_refresh_state"] = {
        "marker_action": marker_action or "",
        "callbacks": _markup_callback_values(base_reply_markup),
    }
    mid = store.get("categories_msg_id")
    candidates = []
    if preferred_message_id is not None:
        try:
            candidates.append(int(preferred_message_id))
        except Exception:
            pass
    if mid:
        try:
            mid_int = int(mid)
            if mid_int not in candidates:
                candidates.append(mid_int)
        except Exception:
            pass

    for target_id in candidates:
        try:
            bot.edit_message_text(text, chat_id=chat_id, message_id=target_id, reply_markup=reply_markup, parse_mode=parse_mode)
            store["categories_msg_id"] = target_id
            register_open_window(chat_id, target_id, "categories", code=marker_action or "")
            save_data(data, chat_ids=[int(chat_id)])
            return target_id
        except Exception as e:
            if "message is not modified" in str(e).lower():
                store["categories_msg_id"] = target_id
                register_open_window(chat_id, target_id, "categories", code=marker_action or "")
                save_data(data, chat_ids=[int(chat_id)])
                return target_id
            log_error(f"send_or_edit_categories_window edit failed {chat_id}:{target_id}: {e}")
            if store.get("categories_msg_id") == target_id:
                unregister_open_window(chat_id, target_id)
                store["categories_msg_id"] = None
                save_data(data, chat_ids=[int(chat_id)])

    sent = bot.send_message(chat_id, text, reply_markup=reply_markup, parse_mode=parse_mode)
    store["categories_msg_id"] = sent.message_id
    register_open_window(chat_id, sent.message_id, "categories", code=marker_action or "")
    save_data(data, chat_ids=[int(chat_id)])
    return sent.message_id

def open_report_window(chat_id: int, month_key: str = None, message_id: int = None):
    """
    Открывает или обновляет отдельное окно отчёта без размножения сообщений.
    """
    text, month_key = build_month_report_text(chat_id, month_key)
    kb = build_report_keyboard(month_key)

    store = get_chat_store(chat_id)
    if message_id and not store.get("report_window_id"):
        store["report_window_id"] = message_id

    final_id = send_or_edit_stored_window(
        chat_id,
        "report_window_id",
        text,
        reply_markup=kb,
        parse_mode="HTML",
        delay=None
    )
    store["report_window_id"] = final_id
    store["report_month"] = month_key
    save_data(data)


def build_owner_instruction_text() -> str:
    return (
        "📘 Инструкция по кнопкам\n\n"
        "🏠 Основное финансовое окно\n"
        "⬅️ День / День ➡️ — перейти на соседний день. 📅 Сегодня — вернуться к текущей дате.\n"
        "📅 Дата — открыть календарь; в календаре можно менять месяц и год.\n"
        "📊 Отчёт — месячный отчёт. 🧮 Итог — общий итог. 🏦 с ост — остаток после каждого расхода.\n"
        "✏️ Изменить / 🗑 Удалить — работа с финансовыми записями. 📦 Статьи — расходы по категориям.\n"
        "📄 CSV — окно Ф47: пять периодов; в каждой строке Период / CSV / Excel / Excel статьи. Точный период открывается отдельной кнопкой ниже.\n\n"
        "💱 Валюта\n"
        "ARS — отдельный учёт в песо. ARS-USD — тот же ARS с эквивалентом по курсу. USD — отдельный долларовый учёт со всеми финансовыми функциями.\n"
        "/ost — включает или выключает подпись «ост:» в окне остатка.\n\n"
        "💰Перес\n"
        "Обычно — бот-копия без кнопки и без /izm_R. Кнопка — под копией появляется ✏️ Изменить. Слеш — в текст копии добавляется /izm_R. При переключении обновляются существующие копии от открытой даты до сегодня.\n\n"
        "🔐 Секрет\n"
        "Секрет у выбранного чата — включает тотальный секрет именно для этого чата. В секрет участвуют и созданные ботом копии. 🪷 Маска — показывает нейтральное сообщение вместо удалённого секретного сообщения.\n\n"
        "📦 Статьи\n"
        "Ф110 — точный диапазон операций; 💵 USD включает долларовое отображение статей. ↕️ Расположение открывает Ф152: сначала выберите статью, затем номер новой позиции.\n"
        "📚 Описание статей — ключевые слова категорий. ➕/✏️/🗑 — добавить, изменить или удалить пользовательскую статью.\n\n"
        "ℹ️ INFO\n"
        "📓 Журнал / 🗂 Журналы чатов — журналы действий. Кнопки в текущем окне — режим обновления интерфейса. Финансы — настройка финансового режима.\n"
        "💵 Доллар — выбор ARS / ARS-USD / USD. 💰Перес — оформление бот-копий. Финансы-кнопки — записи как inline-кнопки.\n"
        "☁️ MEGA — приоритет резервного копирования. ⏱ Внутренние таймеры — единые таймеры обычных режимов. 🚦 Очереди — очереди и диспетчер-свидетель.\n\n"
        "📤 Пересылка\n"
        "Меню пересылки задаёт связанные чаты и финансовую обработку копий. Режим «как у владельца» создаёт отдельный owner scope: настройки такого владельца сохраняются независимо.\n\n"
        "💾 Сохранение\n"
        "После финансового изменения данные сначала сохраняются, затем ставится быстрый backup, после чего обновляются связанные открытые окна и планируется полный backup. В v106 содержательные message/edited_message, включая части Telegram-альбомов, сначала фиксируются маленьким task-файлом в MEGA. Задача пересылки не получает done, пока для исходного сообщения не подтверждены все требуемые направления и, при включённом финучёте, финансовая запись назначения. При deploy pending/running поднимаются из MEGA; потерянный RAM-коллектор альбома ремонтируется по сохранённому update. Callback-кнопки навигации остаются быстрыми."
    )

def build_owner_instruction_keyboard(chat_id: int):
    kb = types.InlineKeyboardMarkup()
    kb.row(IB("🚦 Очереди", callback_data="info_queues"))
    kb.row(IB("🔙 Назад в Инфо", callback_data=f"d:{get_chat_store(chat_id).get('current_view_day', today_key())}:info"))
    kb.row(IB("❌ Закрыть", callback_data="info_close"))
    return kb


def all_task_pool_stats() -> list[dict]:
    return [
        WEBHOOK_TASK_POOL.stats(), FINANCE_TASK_POOL.stats(), FORWARD_TASK_POOL.stats(),
        DELTA_TASK_POOL.stats(), BACKUP_TASK_POOL.stats(), EXPORT_TASK_POOL.stats(), GENERAL_TASK_POOL.stats(),
        MAINTENANCE_TASK_POOL.stats(), JOURNAL_TASK_POOL.stats(), DELAYED_TASK_POOL.stats(), DOZVON_TASK_POOL.stats(),
    ]


def build_queue_status_text() -> str:
    lines = ["🚦 Очереди и нагрузка", ""]
    for st in all_task_pool_stats():
        lines.append(
            f"{st['name']}: {st['active']}/{st['workers']} работают, "
            f"ожидают {st['pending']}, ключей {st['keys']}, "
            f"отказов {st['rejected']}, ошибок {st['failed']}, max ожидание {st['max_wait']}с"
        )
    with timer_lock:
        lines.append("")
        lines.append(f"Таймеров полного бэкапа: {len(_backup_timers)}")
        lines.append(f"Таймеров delta: {len(_quick_backup_timers)}")
        lines.append(f"Dirty чатов: {len(_backup_dirty_chats)}")
    with _delta_state_lock:
        lines.append(f"Delta pending chats: {len(_delta_pending_chats)}")
        lines.append(f"Global full pending: {'да' if _global_snapshot_pending else 'нет'}")
    ds = DELAYED_SCHEDULER.stats()
    lines.append(f"Планировщик: задач {ds['scheduled']}, отменено {ds['cancelled']}, выполнено {ds['executed']}")
    uds = UPDATE_DISPATCHER.stats()
    lines.append(
        f"Диспетчер-свидетель: pending {uds['pending']}, oldest {uds['oldest']}с, "
        f"готово {uds['completed']}, повторы {uds['duplicates']}, timeout {uds['timeouts']}, "
        f"retry {uds['retries']}, ACK ждёт до {uds['ack_wait']}с"
    )
    mts = mega_task_registry_stats() if "mega_task_registry_stats" in globals() else {}
    lines.append(
        f"☁️ MEGA-задачи: pending {mts.get('pending', 0)}, running {mts.get('running', 0)}, "
        f"failed {mts.get('failed', 0)}, done {mts.get('done', 0)}, сейчас {mts.get('processing', 0)}"
    )
    lines.append(
        f"MEGA task: сохранено {mts.get('persisted', 0)}, восстановлено {mts.get('recovered', 0)}, "
        f"уже выполнено {mts.get('skipped_done', 0)}, ошибки записи {mts.get('persist_errors', 0)}, "
        f"ошибки финализации {mts.get('finalize_errors', 0)}"
    )
    if mts.get('last_error'):
        lines.append(f"Последняя ошибка MEGA task: {str(mts.get('last_error'))[:180]}")
    lines.append(f"Excel-бэкап всех чатов: {backup_excel_all_label()}")
    lines.append(f"Telegram общий интервал: {TELEGRAM_GLOBAL_MIN_GAP:.3f}с")
    return "\n".join(lines)


CHAT_JOURNAL_PAGE_SIZE = 20


def _journal_chat_items():
    try:
        return _collect_known_chat_items()
    except Exception:
        items = []
        for cid in (data.get("chats", {}) or {}).keys():
            try:
                items.append((int(cid), get_chat_display_name(int(cid))))
            except Exception:
                pass
        return sorted(items, key=lambda x: str(x[1]).lower())


def build_chat_journal_menu_text(page: int = 0) -> str:
    items = _journal_chat_items()
    pages = max(1, (len(items) + CHAT_JOURNAL_PAGE_SIZE - 1) // CHAT_JOURNAL_PAGE_SIZE)
    page = max(0, min(int(page), pages - 1))
    enabled = sum(1 for cid, _ in items if is_chat_journal_enabled(cid))
    return wm_owner(
        "📓 Журналы по чатам\n\n"
        "Общий журнал по умолчанию выключен. Здесь можно включать запись только для нужных чатов.\n\n"
        f"Включено: {enabled} из {len(items)}\nСтраница: {page + 1}/{pages}",
        9,
    )


def build_chat_journal_menu_keyboard(page: int = 0):
    items = _journal_chat_items()
    pages = max(1, (len(items) + CHAT_JOURNAL_PAGE_SIZE - 1) // CHAT_JOURNAL_PAGE_SIZE)
    page = max(0, min(int(page), pages - 1))
    start = page * CHAT_JOURNAL_PAGE_SIZE
    chunk = items[start:start + CHAT_JOURNAL_PAGE_SIZE]
    kb = types.InlineKeyboardMarkup(row_width=1)
    for cid, title in chunk:
        icon = "✅" if is_chat_journal_enabled(cid) else "❌"
        kb.row(IB(f"{icon} 📓 {chat_button_title(cid, title)}", callback_data=f"journal_chat_toggle:{cid}:{page}"))
    if pages > 1:
        row = []
        if page > 0:
            row.append(IB("⬅️", callback_data=f"journal_chats_open:{page - 1}"))
        row.append(IB(f"{page + 1}/{pages}", callback_data="none"))
        if page + 1 < pages:
            row.append(IB("➡️", callback_data=f"journal_chats_open:{page + 1}"))
        kb.row(*row)
    kb.row(IB("🔙 Назад в Инфо", callback_data="journal_chats_back"))
    return kb


def current_bot_sender_name() -> str:
    """Имя Telegram-бота, который прислал меню. Кэшируем, чтобы не дергать API на каждую кнопку."""
    try:
        global _BOT_DISPLAY_NAME_CACHE
    except Exception:
        pass
    try:
        cached = globals().get("_BOT_DISPLAY_NAME_CACHE")
        if cached:
            return str(cached)
        me = bot.get_me()
        first = str(getattr(me, "first_name", "") or "").strip()
        username = str(getattr(me, "username", "") or "").strip().lstrip("@")
        value = first or (f"@{username}" if username else "Telegram bot")
        if username and first:
            value = f"{first} (@{username})"
        globals()["_BOT_DISPLAY_NAME_CACHE"] = value
        return value
    except Exception:
        username = get_bot_username_cached() if "get_bot_username_cached" in globals() else ""
        return f"@{username}" if username else "Telegram bot"


def bot_file_identity_lines() -> list[str]:
    return [f"🤖 Бот: {current_bot_sender_name()}", f"📄 Файл: {BOT_FILE_NAME}", f"🏷 Версия: {VERSION}"]


VERSION_MENU_PAGE_SIZE = 7


def _version_menu_keys() -> list[str]:
    return list(BOT_BEHAVIOR_PROFILES.keys())


def _version_menu_page(page: int = 0) -> tuple[int, int, list[str]]:
    keys = _version_menu_keys()
    pages = max(1, (len(keys) + VERSION_MENU_PAGE_SIZE - 1) // VERSION_MENU_PAGE_SIZE)
    try:
        page = max(0, min(pages - 1, int(page)))
    except Exception:
        page = 0
    start = page * VERSION_MENU_PAGE_SIZE
    return page, pages, keys[start:start + VERSION_MENU_PAGE_SIZE]


def build_version_menu_text(page: int = 0) -> str:
    active = active_bot_behavior_profile()
    page, pages, keys = _version_menu_page(page)
    active_cfg = BOT_BEHAVIOR_PROFILES.get(active, {})
    lines = [
        "🧩 Переключение версий / режимов",
        *bot_file_identity_lines(),
        "",
        f"Сейчас: ✅ {active_cfg.get('title') or active}",
        f"Страница {page + 1}/{pages}",
        "",
        "Это переключение совместимого профиля внутри текущего безопасного ядра v125. SQLite/MEGA, exact-once и финансовые данные не откатываются.",
        "",
    ]
    for key in keys:
        cfg = BOT_BEHAVIOR_PROFILES[key]
        mark = "✅" if key == active else "▫️"
        lines.append(f"{mark} {cfg['title']}")
        lines.append(f"   {cfg['description']}")
    lines.extend(["", "Выберите версию кнопкой ниже."])
    return wm_owner("\n".join(lines), 9)


def build_version_menu_keyboard(page: int = 0):
    kb = types.InlineKeyboardMarkup(row_width=1)
    active = active_bot_behavior_profile()
    page, pages, keys = _version_menu_page(page)
    for key in keys:
        cfg = BOT_BEHAVIOR_PROFILES[key]
        mark = "✅" if key == active else "▫️"
        kb.row(IB(f"{mark} {cfg['title']}", callback_data=f"version_select:{key}:{page}"))
    nav = []
    if page > 0:
        nav.append(IB("⬅️ Новее", callback_data=f"version_page:{page-1}"))
    if page + 1 < pages:
        nav.append(IB("Старее ➡️", callback_data=f"version_page:{page+1}"))
    if nav:
        kb.row(*nav)
    kb.row(IB("🔙 Назад в Инфо", callback_data="version_back"))
    return kb


def keep_alive_status_text() -> str:
    state = globals().get("KEEP_ALIVE_STATE") or {}
    lines = [
        "💓 Keep-alive / защита от сна",
        "",
        f"Автоматический режим: {'ВКЛ' if KEEP_ALIVE_ENABLED else 'ВЫКЛ'}",
        f"Интервал: {KEEP_ALIVE_INTERVAL_SECONDS} сек.",
        f"APP_URL: {APP_URL or 'не задан'}",
        f"Последний успешный цикл: {state.get('last_ok_at') or 'ещё не было'}",
        f"Self-ping бота: {state.get('self_ping_at') or 'ещё не было'}",
        f"Внешний монитор: {state.get('external_monitor_at') or 'НЕ ОБНАРУЖЕН'}",
        f"Последняя ошибка: {state.get('last_error') or 'нет'}",
        f"Успешных циклов: {state.get('ok_count', 0)}, ошибок: {state.get('fail_count', 0)}",
        "",
        "Для внешнего монитора используйте GET/HEAD /keepalive. v120 отдельно показывает self-ping и реальный внешний запрос.",
    ]
    return wm_owner("\n".join(lines), 9)


def build_info_keyboard(chat_id: int):
    kb = types.InlineKeyboardMarkup()
    layout = version_mode_layout()
    if is_owner_chat(chat_id):
        kb.row(
            IB("📓 Журнал", callback_data="journal_open"),
            IB(journal_toggle_label(), callback_data="journal_toggle"),
        )
        if version_mode_feature("per_chat_journal"):
            kb.row(
                IB("🗂 Журналы чатов", callback_data="journal_chats_open"),
                IB(chat_journal_toggle_label(chat_id), callback_data=f"journal_chat_toggle:{chat_id}:0"),
            )
        kb.row(
            IB(buttons_current_window_label(chat_id), callback_data="buttons_current_toggle"),
            IB(info_finance_toggle_label(chat_id), callback_data="info_finance_off"),
        )
        if version_mode_feature("forward_copy_edit"):
            kb.row(IB(forward_copy_edit_mode_label(chat_id), callback_data="forward_copy_edit_mode_toggle"))
        kb.row(
            IB(forward_menu_style_label(chat_id), callback_data="forward_menu_style_toggle"),
            IB(icon_button_mode_label(chat_id), callback_data="icon_buttons_toggle"),
        )
        kb.row(IB(
            "🛡 Guard: ВКЛ — нажать отключить" if RESTORE_GUARD_ACTIVE else "🛡 Guard: ВЫКЛ — автобэкапы разрешены",
            callback_data="restore_guard_toggle",
        ))
        kb.row(IB(
            "☁️ Обновить JSON из MEGA",
            callback_data="mega_manual_restore",
        ))
        kb.row(
            IB(total_secret_mask_label(chat_id), callback_data="total_secret_mask_toggle"),
            IB(f"🕔 {finance_day_start_label(chat_id)}", callback_data="finance_day5_toggle"),
        )
        if version_mode_feature("mega_priority") and layout in {"v82", "v83"}:
            kb.row(IB(mega_backup_priority_label(chat_id), callback_data="mega_priority_toggle"))
        elif version_mode_feature("mega_priority") and layout in {"v84", "v85", "v86", "v87"}:
            kb.row(
                IB(mega_backup_priority_label(chat_id), callback_data="mega_priority_toggle"),
                IB(main_financial_value_buttons_label(chat_id), callback_data="main_financial_values_toggle"),
            )
        if layout in {"v85", "v86", "v87"}:
            if layout == "v87":
                kb.row(
                    IB(gomonk_info_label(chat_id), callback_data="gomonk_open"),
                    IB(currency_mode_label(chat_id), callback_data="currency_menu"),
                )
            elif layout == "v86":
                kb.row(
                    IB(gomonk_info_label(chat_id), callback_data="gomonk_open"),
                    IB(usd_display_label(chat_id), callback_data="usd_display_toggle"),
                )
            else:
                kb.row(IB(gomonk_info_label(chat_id), callback_data="gomonk_open"))
        if layout == "v83":
            kb.row(IB(main_article_buttons_label(chat_id), callback_data="main_articles_toggle"))
        # Кнопка выбора версии присутствует при любом режиме, включая полный откат v81/v82.
        if version_mode_feature("keepalive_menu"):
            kb.row(
                IB(bot_behavior_profile_label(), callback_data="version_menu"),
                IB("💓 Не спать", callback_data="keepalive_status"),
            )
        else:
            kb.row(IB(bot_behavior_profile_label(), callback_data="version_menu"))
        kb.row(IB("⏱ Внутренние таймеры", callback_data="internal_timers"))
        kb.row(IB(excel_table_style_label(chat_id), callback_data="excel_style_menu"))
        kb.row(
            IB("📘 Инструкция", callback_data="info_instruction"),
            IB("🚦 Очереди", callback_data="info_queues"),
        )
        kb.row(IB("🖥 Render / Сервер", callback_data="runtime_watcher"))
        if active_bot_behavior_profile() in {"v93_current", "v92_current", "v91_current", "v90_current"}:
            kb.row(IB("🧩 Delta / snapshots", callback_data="info_delta_status"))
        if is_primary_owner(chat_id):
            kb.row(IB("👥 /owners", callback_data="additional_owners"))
    else:
        kb.row(IB(info_finance_toggle_label(chat_id), callback_data="info_finance_off"))
        if version_mode_feature("forward_copy_edit"):
            kb.row(IB(forward_copy_edit_mode_label(chat_id), callback_data="forward_copy_edit_mode_toggle"))
        if layout in {"v84", "v85", "v86", "v87"}:
            kb.row(IB(main_financial_value_buttons_label(chat_id), callback_data="main_financial_values_toggle"))
        if layout in {"v85", "v86", "v87"}:
            if layout == "v87":
                kb.row(
                    IB(gomonk_info_label(chat_id), callback_data="gomonk_open"),
                    IB(currency_mode_label(chat_id), callback_data="currency_menu"),
                )
            elif layout == "v86":
                kb.row(
                    IB(gomonk_info_label(chat_id), callback_data="gomonk_open"),
                    IB(usd_display_label(chat_id), callback_data="usd_display_toggle"),
                )
            else:
                kb.row(IB(gomonk_info_label(chat_id), callback_data="gomonk_open"))
        elif layout == "v83":
            kb.row(IB(main_article_buttons_label(chat_id), callback_data="main_articles_toggle"))
    kb.row(
        IB("⬅️ Назад осн. окно", callback_data=f"d:{get_chat_store(chat_id).get('current_view_day', today_key())}:back_main"),
        IB("❌ Закрыть", callback_data="info_close"),
    )
    return kb


def open_info_window(chat_id: int):
    info_text = wm_common(build_info_text(chat_id), 9)
    send_or_edit_stored_window(
        chat_id,
        "info_msg_id",
        info_text,
        reply_markup=build_info_keyboard(chat_id),
        parse_mode=None,
        delay=None
    )



def _expense_anchor_rows(kb, store: dict, day_key: str, callback_builder, empty_text: str = "Нет расходов в этот день"):
    records = expense_anchor_records_for_day(store, day_key)
    if records:
        for rec in records:
            rid = _record_int_id(rec)
            kb.row(IB(expense_anchor_button_label(rec, store), callback_data=callback_builder(rid)))
    else:
        kb.row(IB(empty_text, callback_data="none"))
    return records


def _send_category_pick_start_record(chat_id: int, message_id: int, start_key: str):
    store = get_chat_store(chat_id)
    kb = types.InlineKeyboardMarkup(row_width=1)
    _expense_anchor_rows(
        kb,
        store,
        start_key,
        lambda rid: cat_callback(f"cat_pick_start_record:{start_key}:{rid}"),
    )
    kb.row(IB("➡️ Продолжить с начала дня", callback_data=cat_callback(f"cat_pick_start_record:{start_key}:0")))
    dt = datetime.strptime(start_key, "%Y-%m-%d")
    kb.row(IB("🔙 Назад к календарю", callback_data=cat_callback(f"cat_pick_start:{dt.year}:{dt.month}")))
    text = (
        "🎯 Точное начало периода\n"
        f"📅 День: {fmt_date_ddmmyy(start_key)}\n\n"
        "Выберите расход, с которого начинать расчёт, или продолжите с начала дня."
    )
    send_or_edit_categories_window(
        chat_id,
        text,
        reply_markup=kb,
        preferred_message_id=message_id,
        marker_action="cat_pick_start_record:*",
    )


def _category_end_day_buttons_precise(start_key: str, start_rid: int, view_year: int, view_month: int):
    kb = types.InlineKeyboardMarkup(row_width=7)
    last_day = calendar.monthrange(int(view_year), int(view_month))[1]
    buttons = []
    for dnum in range(1, last_day + 1):
        day_key = _date_key_from_ymd(view_year, view_month, dnum)
        if day_key < start_key:
            buttons.append(IB("·", callback_data="none"))
        else:
            buttons.append(IB(str(dnum), callback_data=cat_callback(f"cat_pick_set_end3:{start_key}:{int(start_rid)}:{view_year}:{view_month}:{dnum}")))
    for idx in range(0, len(buttons), 7):
        kb.row(*buttons[idx:idx + 7])
    return kb


def _send_category_pick_end_precise(chat_id: int, message_id: int, start_key: str, start_rid: int, view_year: int, view_month: int):
    store = get_chat_store(chat_id)
    kb = _category_end_day_buttons_precise(start_key, start_rid, view_year, view_month)
    prev_y, prev_m = _shift_month(view_year, view_month, -1)
    next_y, next_m = _shift_month(view_year, view_month, 1)
    start_month_key = start_key[:7]
    nav = []
    if f"{prev_y:04d}-{prev_m:02d}" >= start_month_key:
        nav.append(IB("⬅️ Месяц", callback_data=cat_callback(f"cat_pick_end3:{start_key}:{int(start_rid)}:{prev_y}:{prev_m}")))
    else:
        nav.append(IB(" ", callback_data="none"))
    nav.append(IB(f"{russian_month_name(view_month)} {view_year}", callback_data="none"))
    nav.append(IB("Месяц ➡️", callback_data=cat_callback(f"cat_pick_end3:{start_key}:{int(start_rid)}:{next_y}:{next_m}")))
    kb.row(*nav)
    kb.row(IB(f"⏹ По сегодняшний день · {fmt_date_ddmmyy(today_key())}", callback_data=cat_callback(f"cat_pick_today_end:{start_key}:{int(start_rid)}")))
    start_dt = datetime.strptime(start_key, "%Y-%m-%d")
    kb.row(IB("🔙 Изменить начало", callback_data=cat_callback(f"cat_pick_set_start:{start_dt.year}:{start_dt.month}:{start_dt.day}")))
    text = (
        "🎯 Точный период расходов\n"
        f"▶️ Начало: {exact_boundary_text(store, start_key, start_rid, True)}\n\n"
        f"Выберите конечный день: {russian_month_name(view_month)} {view_year}"
    )
    send_or_edit_categories_window(
        chat_id,
        text,
        reply_markup=kb,
        preferred_message_id=message_id,
        marker_action="cat_pick_end3:*",
    )


def _send_category_pick_end_record(chat_id: int, message_id: int, start_key: str, start_rid: int, end_key: str):
    store = get_chat_store(chat_id)
    kb = types.InlineKeyboardMarkup(row_width=1)
    records = expense_anchor_records_for_day(store, end_key)
    displayed = 0
    all_recs = sorted_records_for_day(store, end_key)
    pos = {_record_int_id(r): i for i, r in enumerate(all_recs)}
    for rec in records:
        rid = _record_int_id(rec)
        # В тот же день нельзя закончить раньше выбранного начала.
        if end_key == start_key and start_rid:
            if pos.get(rid, -1) < pos.get(int(start_rid), 0):
                continue
        displayed += 1
        kb.row(IB(expense_anchor_button_label(rec, store), callback_data=cat_callback(f"cat_pick_end_record:{start_key}:{int(start_rid)}:{end_key}:{rid}")))
    if not displayed:
        kb.row(IB("Нет подходящих расходов в этот день", callback_data="none"))
    kb.row(IB("✅ Продолжить до конца дня", callback_data=cat_callback(f"cat_pick_end_record:{start_key}:{int(start_rid)}:{end_key}:0")))
    end_dt = datetime.strptime(end_key, "%Y-%m-%d")
    kb.row(IB("🔙 Назад к календарю", callback_data=cat_callback(f"cat_pick_end3:{start_key}:{int(start_rid)}:{end_dt.year}:{end_dt.month}")))
    text = (
        "🎯 Точный конец периода\n"
        f"▶️ Начало: {exact_boundary_text(store, start_key, start_rid, True)}\n"
        f"📅 Конечный день: {fmt_date_ddmmyy(end_key)}\n\n"
        "Выберите последний расход, который включить в расчёт, или продолжите до конца дня."
    )
    send_or_edit_categories_window(
        chat_id,
        text,
        reply_markup=kb,
        preferred_message_id=message_id,
        marker_action="cat_pick_end_record:*",
    )


def build_categories_record_summary_keyboard(start_key: str, start_rid: int, end_key: str, end_rid: int, store: dict):
    kb = types.InlineKeyboardMarkup(row_width=3)
    cats = calc_categories_for_record_range(store, start_key, start_rid, end_key, end_rid)
    buttons = []
    for category in get_ordered_category_names(cats=cats, store=store):
        slug = get_expense_category_slug(category, store)
        if slug:
            buttons.append(IB(_clean_category_display_name(category), callback_data=cat_callback(f"cat_show_records:{start_key}:{int(start_rid)}:{end_key}:{int(end_rid)}:{slug}")))
    add_buttons_in_rows(kb, buttons, 3)
    if _v85_enabled("usd_categories") and not financial_view_is_usd(store):
        usd_on = bool(store.setdefault("settings", {}).get("category_usd_enabled", False))
        kb.row(IB("💵 USD: ВКЛ" if usd_on else "💵 USD: ВЫКЛ", callback_data=cat_callback(f"cat_usd_toggle_records:{start_key}:{int(start_rid)}:{end_key}:{int(end_rid)}")))
    kb.row(IB("↕️ Расположение", callback_data=cat_callback(f"cat_order_open_exact:{start_key}:{int(start_rid)}:{end_key}:{int(end_rid)}")))
    start_dt = datetime.strptime(start_key, "%Y-%m-%d")
    end_dt = datetime.strptime(end_key, "%Y-%m-%d")
    kb.row(
        IB("⬅️ Назад", callback_data=cat_callback(f"cat_pick_set_end3:{start_key}:{int(start_rid)}:{end_dt.year}:{end_dt.month}:{end_dt.day}")),
        IB("🎯 Выбрать заново", callback_data=cat_callback(f"cat_pick_start:{start_dt.year}:{start_dt.month}")),
    )
    kb.row(
        IB("⬅️ Назад осн. окно", callback_data=f"d:{today_key()}:back_main"),
        IB("❌ Закрыть", callback_data=cat_callback("cat_close")),
    )
    return kb


def build_category_record_detail_text(store: dict, start_key: str, start_rid: int, end_key: str, end_rid: int, category: str):
    items = collect_items_for_category_record_range(store, start_key, start_rid, end_key, end_rid, category)
    view_usd = financial_view_is_usd(store)
    mode = currency_mode_from_store(store)
    category_mixed = bool((not view_usd) and store.setdefault("settings", {}).get("category_usd_enabled", False) and _v85_enabled("usd_categories"))
    show_rate = (not view_usd) and (mode != "ars" or category_mixed)
    rate_info = usd_rate_cached() if show_rate else None
    total = sum(amount for _, amount, _ in items)
    clean_category = _clean_category_display_name(category).upper()
    lines = [
        f"📦 {clean_category}",
        f"▶️ {exact_boundary_text(store, start_key, start_rid, True)}",
        f"⏹ {exact_boundary_text(store, end_key, end_rid, False)}",
        "",
        f"Итого: {format_category_view_amount(store, total, category_mixed)}",
    ]
    if show_rate and rate_info:
        lines.append(f"Курс: 1 USD = {fmt_num(rate_info['rate']).lstrip('+')} ARS ({_clean_category_display_name(rate_info.get('source') or 'DolarAPI')})")
    lines.append("")
    if not items:
        lines.append("Нет операций по этой статье.")
    else:
        for day_key, amount, note in items:
            clean_note = _clean_category_display_name(str(note or "").strip())
            lines.append(f"• {fmt_date_ddmmyy(day_key)}: {format_category_view_amount(store, amount, category_mixed)} {clean_note}".rstrip())
    return wm_common("\n".join(lines), 8)

_category_other_sort_state = {}


def _other_sort_key(chat_id: int, start_key: str, start_rid: int, end_key: str, end_rid: int):
    return (int(chat_id), str(start_key), int(start_rid), str(end_key), int(end_rid))


def other_sort_records(store: dict, start_key: str, start_rid: int, end_key: str, end_rid: int) -> list[dict]:
    out = []
    for _day, rec in exact_record_range(store, start_key, start_rid, end_key, end_rid):
        try:
            if financial_view_amount(store, rec) >= 0:
                continue
        except Exception:
            continue
        category = resolve_expense_category_for_record(rec, store)
        if get_expense_category_slug(category, store) == "other":
            out.append(rec)
    return out


def build_other_sort_text(store: dict, start_key: str, start_rid: int, end_key: str, end_rid: int) -> str:
    count = len(other_sort_records(store, start_key, start_rid, end_key, end_rid))
    return wm_common(
        "🔀 Сортировка статьи ПРОЧЕЕ\n\n"
        "Выберите финансовые значения, которые нужно перенести в другую статью. "
        "После выбора нажмите «Выбрать их».\n\n"
        f"Доступно записей: {count}", 8
    )


def build_other_sort_keyboard(chat_id: int, store: dict, start_key: str, start_rid: int, end_key: str, end_rid: int):
    kb = types.InlineKeyboardMarkup(row_width=1)
    key = _other_sort_key(chat_id, start_key, start_rid, end_key, end_rid)
    selected = _category_other_sort_state.setdefault(key, set())
    valid_ids = set()
    for rec in other_sort_records(store, start_key, start_rid, end_key, end_rid):
        rid = _record_int_id(rec)
        valid_ids.add(rid)
        mark = "✅" if rid in selected else "▫️"
        label = f"{mark} {financial_record_button_label(rec, chat_id)}"
        kb.row(IB(label, callback_data=cat_callback(f"cat_other_sort_toggle:{rid}:{start_key}:{int(start_rid)}:{end_key}:{int(end_rid)}")))
    selected.intersection_update(valid_ids)
    if selected:
        kb.row(IB(f"✅ Выбрать их ({len(selected)})", callback_data=cat_callback(f"cat_other_sort_choose:{start_key}:{int(start_rid)}:{end_key}:{int(end_rid)}")))
    else:
        kb.row(IB("Выберите значения выше", callback_data="none"))
    kb.row(IB("⬅️ Назад", callback_data=cat_callback(f"cat_show_records:{start_key}:{int(start_rid)}:{end_key}:{int(end_rid)}:other")))
    kb.row(IB("⬅️ Назад осн. окно", callback_data=f"d:{today_key()}:back_main"), IB("❌ Закрыть", callback_data=cat_callback("cat_close")))
    return kb


def build_other_sort_target_text(chat_id: int, start_key: str, start_rid: int, end_key: str, end_rid: int) -> str:
    key = _other_sort_key(chat_id, start_key, start_rid, end_key, end_rid)
    selected = _category_other_sort_state.get(key, set())
    return wm_common(f"📦 Куда перенести выбранные записи?\n\nВыбрано: {len(selected)}", 8)


def build_other_sort_target_keyboard(chat_id: int, store: dict, start_key: str, start_rid: int, end_key: str, end_rid: int):
    kb = types.InlineKeyboardMarkup(row_width=2)
    buttons = []
    for slug in get_expense_category_order_slugs(store):
        if slug == "other":
            continue
        name = _clean_category_display_name(get_category_by_slug(slug, store) or slug)
        buttons.append(IB(name, callback_data=cat_callback(f"cat_other_sort_target:{slug}:{start_key}:{int(start_rid)}:{end_key}:{int(end_rid)}")))
    add_buttons_in_rows(kb, buttons, 2)
    kb.row(IB("⬅️ Назад к выбору", callback_data=cat_callback(f"cat_other_sort:{start_key}:{int(start_rid)}:{end_key}:{int(end_rid)}")))
    kb.row(IB("⬅️ Назад осн. окно", callback_data=f"d:{today_key()}:back_main"), IB("❌ Закрыть", callback_data=cat_callback("cat_close")))
    return kb


def apply_other_sort_target(store: dict, selected_ids: set[int], target_slug: str) -> int:
    changed = 0
    ids = {int(x) for x in selected_ids}
    for rec in store.get("records", []) or []:
        if _record_int_id(rec) in ids:
            rec["category_override_slug"] = str(target_slug)
            changed += 1
    # daily_records обычно содержит те же dict, но обновляем и отдельные копии после restore.
    for arr in (store.get("daily_records", {}) or {}).values():
        for rec in arr or []:
            if _record_int_id(rec) in ids:
                rec["category_override_slug"] = str(target_slug)
    return changed


def build_category_record_detail_keyboard(start_key: str, start_rid: int, end_key: str, end_rid: int, category: str | None = None, store: dict | None = None):
    kb = types.InlineKeyboardMarkup()
    if category and get_expense_category_slug(category, store) == "other":
        kb.row(IB("🔀 Сортировка", callback_data=cat_callback(f"cat_other_sort:{start_key}:{int(start_rid)}:{end_key}:{int(end_rid)}")))
    kb.row(IB("⬅️ Назад", callback_data=cat_callback(f"cat_back_records:{start_key}:{int(start_rid)}:{end_key}:{int(end_rid)}")))
    kb.row(
        IB("⬅️ Назад осн. окно", callback_data=f"d:{today_key()}:back_main"),
        IB("❌ Закрыть", callback_data=cat_callback("cat_close")),
    )
    return kb


def _category_picker_day_buttons(year: int, month: int, stage: str, start_day: int | None = None, selected_day: int | None = None):
    kb = types.InlineKeyboardMarkup(row_width=7)
    last_day = calendar.monthrange(int(year), int(month))[1]
    buttons = []
    for dnum in range(1, last_day + 1):
        label = f"✅{dnum}" if selected_day == dnum else str(dnum)
        if stage == "start":
            cb = cat_callback(f"cat_pick_set_start:{year}:{month}:{dnum}")
        else:
            cb = cat_callback(f"cat_pick_set_end:{year}:{month}:{int(start_day or 1)}:{dnum}")
        buttons.append(IB(label, callback_data=cb))
    for i in range(0, len(buttons), 7):
        kb.row(*buttons[i:i + 7])
    return kb


def _send_category_pick_start(chat_id: int, message_id: int, year: int, month: int, selected: int | None = None):
    kb = _category_picker_day_buttons(year, month, "start", selected_day=selected)
    if selected:
        kb.row(IB("✅ Выбрать это", callback_data=cat_callback(f"cat_pick_end:{year}:{month}:{selected}")))
    kb.row(IB("🔙 Назад", callback_data=cat_callback(f"cat_m:{year}:{month}")))
    text = f"📅 Выберите начальную дату: {month:02d}.{year}"
    if selected:
        text += f"\n✅ Начало: {selected:02d}.{month:02d}.{year}"
    send_or_edit_categories_window(
        chat_id,
        wm_common(text, 13),
        reply_markup=kb,
        preferred_message_id=message_id,
        marker_action="cat_pick_start:*",
    )


def _shift_month(year: int, month: int, delta: int = 0) -> tuple[int, int]:
    base = datetime(int(year), int(month), 1)
    m0 = (base.year * 12 + base.month - 1) + int(delta or 0)
    y = m0 // 12
    m = m0 % 12 + 1
    return y, m


def _date_key_from_ymd(year: int, month: int, day: int) -> str:
    last_day = calendar.monthrange(int(year), int(month))[1]
    d = max(1, min(int(day), last_day))
    return f"{int(year):04d}-{int(month):02d}-{d:02d}"


def _category_picker_day_buttons_end_any_month(start_key: str, view_year: int, view_month: int, selected_key: str | None = None):
    kb = types.InlineKeyboardMarkup(row_width=7)
    last_day = calendar.monthrange(int(view_year), int(view_month))[1]
    buttons = []
    for dnum in range(1, last_day + 1):
        dk = _date_key_from_ymd(view_year, view_month, dnum)
        label = f"✅{dnum}" if selected_key == dk else str(dnum)
        buttons.append(IB(label, callback_data=cat_callback(f"cat_pick_set_end2:{start_key}:{int(view_year)}:{int(view_month)}:{dnum}")))
    for i in range(0, len(buttons), 7):
        kb.row(*buttons[i:i + 7])
    return kb


def _send_category_pick_end_any_month(chat_id: int, message_id: int, start_key: str, view_year: int, view_month: int, selected_end_key: str | None = None):
    start_dt = datetime.strptime(str(start_key)[:10], "%Y-%m-%d")
    kb = _category_picker_day_buttons_end_any_month(start_key, view_year, view_month, selected_key=selected_end_key)
    prev_y, prev_m = _shift_month(view_year, view_month, -1)
    next_y, next_m = _shift_month(view_year, view_month, 1)
    kb.row(
        IB("⬅️ Месяц", callback_data=cat_callback(f"cat_pick_end2:{start_key}:{prev_y}:{prev_m}")),
        IB(f"{int(view_month):02d}.{int(view_year)}", callback_data="none"),
        IB("Месяц ➡️", callback_data=cat_callback(f"cat_pick_end2:{start_key}:{next_y}:{next_m}")),
    )
    if selected_end_key:
        kb.row(IB("✅ Выбрать конечное", callback_data=cat_callback(f"cat_range_custom2:{start_key}:{selected_end_key}")))
    kb.row(IB("🔙 Назад к началу", callback_data=cat_callback(f"cat_pick_set_start:{start_dt.year}:{start_dt.month}:{start_dt.day}")))
    text = f"📅 Начало: {fmt_date_ddmmyy(start_key)}\nВыберите конечную дату: {int(view_month):02d}.{int(view_year)}"
    if selected_end_key:
        text += f"\n✅ Конец: {fmt_date_ddmmyy(selected_end_key)}"
    send_or_edit_categories_window(chat_id, wm_common(text, 13), reply_markup=kb, preferred_message_id=message_id)


def _send_category_pick_end(chat_id: int, message_id: int, year: int, month: int, start_day: int, selected_end: int | None = None):
    start_key = _date_key_from_ymd(year, month, start_day)
    selected_end_key = _date_key_from_ymd(year, month, selected_end) if selected_end else None
    _send_category_pick_end_any_month(chat_id, message_id, start_key, int(year), int(month), selected_end_key)


def handle_categories_callback(call, data_str: str) -> bool:
    """UI окна расходов по статьям."""
    chat_id = call.message.chat.id
    store = get_chat_store(chat_id)

    if data_str.startswith("cat_page:"):
        requested = data_str.split(":", 1)[1] if ":" in data_str else "0"
        _show_category_page(chat_id, call.message.message_id, requested)
        try:
            bot.answer_callback_query(call.id)
        except Exception:
            pass
        return True

    if data_str == "cat_prompt_back":
        was_edit = bool(store.get("category_edit_wait"))
        clear_category_wait_state(chat_id, "category_add_wait", call.message.message_id, delete_prompt=False)
        clear_category_wait_state(chat_id, "category_edit_wait", call.message.message_id, delete_prompt=False)
        if was_edit:
            send_or_edit_categories_window(
                chat_id,
                wm_common("✏️ Изменить статью\n\nВыберите статью. Б = базовая, С = своя.", 14),
                reply_markup=build_category_edit_keyboard(chat_id),
                preferred_message_id=call.message.message_id,
            )
        else:
            return handle_categories_callback(call, "cat_today")
        return True

    if data_str == "cat_add_cancel":
        clear_category_wait_state(chat_id, "category_add_wait", call.message.message_id, delete_prompt=True)
        clear_category_wait_state(chat_id, "category_edit_wait", call.message.message_id, delete_prompt=True)
        try:
            bot.answer_callback_query(call.id, "Команда отменена")
        except Exception:
            pass
        return True

    if data_str.startswith("cat_main_edit:"):
        try:
            parts = data_str.split(":", 2)
            slug = parts[1]
        except Exception:
            return True
        start_category_edit_wait(chat_id, chat_id, slug)
        try:
            bot.answer_callback_query(call.id, "Редактирование статьи", show_alert=False)
        except Exception:
            pass
        return True

    if data_str == "cat_edit_menu":
        send_or_edit_categories_window(
            chat_id,
            wm_common("✏️ Изменить статью\n\nВыберите статью. Б = базовая, С = своя. Можно менять название и ключевые слова.", 14),
            reply_markup=build_category_edit_keyboard(chat_id),
            preferred_message_id=call.message.message_id
        )
        return True

    if data_str.startswith("cat_edit_pick:"):
        slug = data_str.split(":", 1)[1]
        start_category_edit_wait(chat_id, chat_id, slug)
        try:
            bot.answer_callback_query(call.id, "Напиши новую статью и ключи", show_alert=False)
        except Exception:
            pass
        return True

    if data_str == "cat_del_menu":
        clear_category_wait_state(chat_id, "category_add_wait", delete_prompt=False)
        clear_category_wait_state(chat_id, "category_edit_wait", delete_prompt=False)
        store["category_delete_selection"] = []
        save_data(data)
        send_or_edit_categories_window(
            chat_id,
            wm_common("🗑 Удалить статью\n\nВыберите пользовательские статьи галочками и нажмите «Удалить выбранное». Стандартные статьи не удаляем, чтобы не ломать базовую логику.", 15),
            reply_markup=build_category_delete_keyboard(chat_id),
            preferred_message_id=call.message.message_id
        )
        return True

    if data_str.startswith("cat_del_toggle:"):
        slug = data_str.split(":", 1)[1]
        selected = set(store.get("category_delete_selection") or [])
        if slug in selected:
            selected.remove(slug)
        else:
            selected.add(slug)
        store["category_delete_selection"] = sorted(selected)
        save_data(data)
        send_or_edit_categories_window(
            chat_id,
            wm_common("🗑 Удалить статью\n\nВыберите пользовательские статьи галочками и нажмите «Удалить выбранное».", 15),
            reply_markup=build_category_delete_keyboard(chat_id),
            preferred_message_id=call.message.message_id
        )
        return True

    if data_str == "cat_del_selected":
        selected = set(store.get("category_delete_selection") or [])
        if not selected:
            try:
                bot.answer_callback_query(call.id, "Ничего не выбрано", show_alert=False)
            except Exception:
                pass
            return True
        count = remove_custom_expense_categories(chat_id, selected)
        try:
            bot.answer_callback_query(call.id, f"Удалено статей: {count}", show_alert=False)
        except Exception:
            pass
        return handle_categories_callback(call, "cat_today")

    if data_str == "cat_close":
        mid = store.get("categories_msg_id")
        if mid:
            try:
                bot.delete_message(chat_id, mid)
            except Exception:
                pass
        if mid:
            unregister_open_window(chat_id, int(mid))
        store["categories_msg_id"] = None
        store["categories_refresh_state"] = None
        store.pop("categories_pagination", None)
        save_data(data, chat_ids=[int(chat_id)])
        return True

    if data_str == "cat_today":
        return handle_categories_callback(call, f"cat_wthu:{today_key()}")

    if data_str == "cat_add":
        start_category_add_wait(chat_id, chat_id)
        try:
            bot.answer_callback_query(call.id, "Напиши название и ключи статьи", show_alert=False)
        except Exception:
            pass
        return True

    if data_str == "cat_desc":
        kb = types.InlineKeyboardMarkup()
        kb.row(IB("🔙 Назад к статьям", callback_data=cat_callback(f"cat_wthu:{today_key()}")))
        kb.row(
            IB("⬅️ Назад осн. окно", callback_data=f"d:{today_key()}:back_main"),
            IB("❌ Закрыть статьи", callback_data=cat_callback("cat_close")),
        )
        send_or_edit_categories_window(chat_id, build_articles_description_text(chat_id), reply_markup=kb, preferred_message_id=call.message.message_id)
        return True

    if data_str.startswith("cat_wthu:"):
        ref = data_str.split(":", 1)[1] or today_key()
        start_key = week_start_thursday(ref)
        start, end = week_bounds_thu_wed(start_key)
        label = f"{fmt_date_ddmmyy(start)} — {fmt_date_ddmmyy(end)} (Чт–Ср)"
        text, _ = summarize_categories(store, start, end, label)
        kb = build_categories_summary_keyboard("wthu", start, end, store=store)
        send_or_edit_categories_window(chat_id, text, reply_markup=kb, preferred_message_id=call.message.message_id)
        return True

    if data_str.startswith("cat_wk:"):
        start_key = data_str.split(":", 1)[1].strip() or week_start_monday(today_key())
        start, end = week_bounds_from_start(start_key)
        label = f"{fmt_date_ddmmyy(start)} — {fmt_date_ddmmyy(end)} (Пн–Вс)"
        text, _ = summarize_categories(store, start, end, label)
        kb = build_categories_summary_keyboard("wk", start, end, store=store)
        send_or_edit_categories_window(chat_id, text, reply_markup=kb, preferred_message_id=call.message.message_id)
        return True

    if data_str == "cat_months" or data_str.startswith("cat_months_y:"):
        try:
            year = int(data_str.split(":", 1)[1]) if data_str.startswith("cat_months_y:") else now_local().year
        except Exception:
            year = now_local().year
        kb = types.InlineKeyboardMarkup(row_width=2)
        month_buttons = []
        current_ym = now_local().strftime("%Y-%m")
        for m in range(1, 13):
            ym = f"{year:04d}-{m:02d}"
            label = f"📍 {russian_month_name(m)} ({m}) — текущий" if ym == current_ym else f"{russian_month_name(m)} ({m})"
            month_buttons.append(IB(label, callback_data=cat_callback(f"cat_m:{year}:{m}")))
        for i in range(0, len(month_buttons), 2):
            kb.row(*month_buttons[i:i + 2])
        kb.row(
            IB("⬅️ Год", callback_data=cat_callback(f"cat_months_y:{year - 1}")),
            IB(str(year), callback_data="none"),
            IB("Год ➡️", callback_data=cat_callback(f"cat_months_y:{year + 1}")),
        )
        kb.row(
            IB("📅 Сегодня", callback_data=cat_callback("cat_today")),
            IB("⬅️ Назад осн. окно", callback_data=f"d:{today_key()}:back_main"),
            IB("❌ Закрыть статьи", callback_data=cat_callback("cat_close"))
        )
        send_or_edit_categories_window(
            chat_id,
            wm_common(f"📦 Выберите месяц, год {year}:", 12),
            reply_markup=kb,
            marker_action="markup:plain",
        )
        return True

    if data_str.startswith("cat_m:"):
        try:
            parts = data_str.split(":")
            if len(parts) >= 3:
                year, month = int(parts[1]), int(parts[2])
            else:
                year, month = now_local().year, int(parts[1])
        except Exception:
            return True
        last_day = calendar.monthrange(year, month)[1]
        kb = types.InlineKeyboardMarkup(row_width=7)
        weeks = [(1, 7), (8, 14), (15, 21), (22, last_day)]
        kb.row(*[IB(f"{a:02d}–{b:02d}", callback_data=cat_callback(f"cat_rng:{year}:{month}:{a}:{b}")) for a, b in weeks])
        kb.row(IB("📅 Произвольный период", callback_data=cat_callback(f"cat_pick_start:{year}:{month}")))
        row = []
        if month != now_local().month or year != now_local().year:
            row.append(IB("📅 Сегодня", callback_data=cat_callback("cat_today")))
        row.append(IB("🔙 Назад", callback_data=cat_callback("cat_months")))
        kb.row(*row)
        send_or_edit_categories_window(
            chat_id,
            wm_common(f"📆 Выберите неделю: {russian_month_name(month)} ({month}) {year}", 13),
            reply_markup=kb,
            marker_action="cat_m:*",
        )
        return True

    if data_str.startswith("cat_pick_start:"):
        try:
            _, y, m = data_str.split(":")
            _send_category_pick_start(chat_id, call.message.message_id, int(y), int(m))
        except Exception as e:
            log_error(f"cat_pick_start: {e}")
        return True

    if data_str.startswith("cat_pick_set_start:"):
        try:
            _, y, m, d = data_str.split(":")
            start_key = _date_key_from_ymd(int(y), int(m), int(d))
            _send_category_pick_start_record(chat_id, call.message.message_id, start_key)
        except Exception as e:
            log_error(f"cat_pick_set_start: {e}")
        return True

    if data_str.startswith("cat_pick_start_record:"):
        try:
            _, start_key, start_rid = data_str.split(":")
            start_dt = datetime.strptime(start_key, "%Y-%m-%d")
            _send_category_pick_end_precise(
                chat_id,
                call.message.message_id,
                start_key,
                int(start_rid),
                start_dt.year,
                start_dt.month,
            )
        except Exception as e:
            log_error(f"cat_pick_start_record: {e}")
        return True

    if data_str.startswith("cat_pick_today_end:"):
        try:
            _, start_key, start_rid = data_str.split(":")
            end_key = today_key()
            if end_key < start_key:
                end_key = start_key
            _send_category_pick_end_record(chat_id, call.message.message_id, start_key, int(start_rid), end_key)
        except Exception as e:
            log_error(f"cat_pick_today_end: {e}")
        return True

    if data_str == "cat_pick_today_start":
        try:
            start_key = today_key()
            now_dt = now_local()
            _send_category_pick_end_precise(chat_id, call.message.message_id, start_key, 0, now_dt.year, now_dt.month)
        except Exception as e:
            log_error(f"cat_pick_today_start: {e}")
        return True

    if data_str.startswith("cat_usd_toggle_period:"):
        try:
            _, mode, start, end = data_str.split(":", 3)
            settings = store.setdefault("settings", {})
            settings["category_usd_enabled"] = not bool(settings.get("category_usd_enabled", False))
            save_data(data, chat_ids=[chat_id])
            schedule_config_backup_for_chats(chat_id)
            if settings["category_usd_enabled"]:
                usd_rate_cached(force=False)
            label = f"{fmt_date_ddmmyy(start)} — {fmt_date_ddmmyy(end)}"
            text, _ = summarize_categories(store, start, end, label)
            kb = build_categories_summary_keyboard(mode, start, end, store=store)
            marker = "cat_wthu:*" if mode == "wthu" else ("cat_wk:*" if mode == "wk" else "cat_range_custom2:*")
            send_or_edit_categories_window(chat_id, text, reply_markup=kb, preferred_message_id=call.message.message_id, marker_action=marker)
        except Exception as e:
            log_error(f"cat_usd_toggle_period: {e}")
        return True

    if data_str.startswith("cat_order_open_sum:"):
        try:
            _, mode, start, end = data_str.split(":", 3)
            send_or_edit_categories_window(
                chat_id, build_category_layout_text(store, "sum"),
                reply_markup=build_category_layout_keyboard(store, "sum", (mode, start, end), chat_id=chat_id),
                preferred_message_id=call.message.message_id, marker_action="cat_order_open_sum:*",
            )
        except Exception as e:
            log_error(f"cat_order_open_sum: {e}")
        return True

    if data_str.startswith("cat_order_select_sum:"):
        try:
            _, slug, mode, start, end = data_str.split(":", 4)
            params = ("sum", mode, start, end)
            key = _category_order_selection_key(chat_id, params)
            _category_order_selection[key] = slug
            send_or_edit_categories_window(
                chat_id, build_category_layout_text(store, "sum"),
                reply_markup=build_category_layout_keyboard(store, "sum", (mode, start, end), chat_id=chat_id),
                preferred_message_id=call.message.message_id, marker_action="cat_order_open_sum:*",
            )
        except Exception as e:
            log_error(f"cat_order_select_sum: {e}")
        return True

    if data_str.startswith("cat_order_position_sum:"):
        try:
            _, position, mode, start, end = data_str.split(":", 4)
            params = ("sum", mode, start, end)
            key = _category_order_selection_key(chat_id, params)
            slug = _category_order_selection.get(key)
            if not slug:
                try:
                    bot.answer_callback_query(call.id, "Сначала выберите статью")
                except Exception:
                    pass
                return True
            moved = move_expense_category_to_position(store, slug, int(position))
            _category_order_selection.pop(key, None)
            if moved:
                save_data(data, chat_ids=[chat_id])
                schedule_quick_backup(
                    chat_id,
                    MEGA_DELTA_PRIORITY_DELAY_SECONDS if mega_backup_priority_enabled() else MEGA_DELTA_DELAY_SECONDS,
                )
                schedule_config_backup_for_chats(chat_id, delay=0.4)
                finance_changed(chat_id, store.get("current_view_day") or today_key(), reason="category_order_position_f36", delay=0.03)
            send_or_edit_categories_window(
                chat_id, build_category_layout_text(store, "sum"),
                reply_markup=build_category_layout_keyboard(store, "sum", (mode, start, end), chat_id=chat_id),
                preferred_message_id=call.message.message_id, marker_action="cat_order_open_sum:*",
            )
        except Exception as e:
            log_error(f"cat_order_position_sum: {e}")
        return True

    if data_str.startswith("cat_order_move_sum:"):
        try:
            _, slug, direction, mode, start, end = data_str.split(":", 5)
            if move_expense_category_order(store, slug, direction):
                save_data(data, chat_ids=[chat_id])
                schedule_config_backup_for_chats(chat_id)
            send_or_edit_categories_window(
                chat_id, build_category_layout_text(store, "sum"),
                reply_markup=build_category_layout_keyboard(store, "sum", (mode, start, end), chat_id=chat_id),
                preferred_message_id=call.message.message_id, marker_action="cat_order_move_sum:*",
            )
        except Exception as e:
            log_error(f"cat_order_move_sum: {e}")
        return True

    if data_str.startswith("cat_order_open_exact:"):
        try:
            _, start_key, start_rid, end_key, end_rid = data_str.split(":")
            send_or_edit_categories_window(
                chat_id, build_category_layout_text(store, "exact"),
                reply_markup=build_category_layout_keyboard(store, "exact", (start_key, int(start_rid), end_key, int(end_rid)), chat_id=chat_id),
                preferred_message_id=call.message.message_id, marker_action="cat_order_open_exact:*",
            )
        except Exception as e:
            log_error(f"cat_order_open_exact: {e}")
        return True

    if data_str.startswith("cat_order_select_exact:"):
        try:
            _, slug, start_key, start_rid, end_key, end_rid = data_str.split(":", 5)
            params = (start_key, int(start_rid), end_key, int(end_rid))
            key = _category_order_selection_key(chat_id, params)
            _category_order_selection[key] = slug
            send_or_edit_categories_window(
                chat_id, build_category_layout_text(store, "exact"),
                reply_markup=build_category_layout_keyboard(store, "exact", params, chat_id=chat_id),
                preferred_message_id=call.message.message_id, marker_action="cat_order_open_exact:*",
            )
        except Exception as e:
            log_error(f"cat_order_select_exact: {e}")
        return True

    if data_str.startswith("cat_order_position_exact:"):
        try:
            _, position, start_key, start_rid, end_key, end_rid = data_str.split(":", 5)
            params = (start_key, int(start_rid), end_key, int(end_rid))
            key = _category_order_selection_key(chat_id, params)
            slug = _category_order_selection.get(key)
            if not slug:
                try:
                    bot.answer_callback_query(call.id, "Сначала выберите статью")
                except Exception:
                    pass
                return True
            moved = move_expense_category_to_position(store, slug, int(position))
            _category_order_selection.pop(key, None)
            if moved:
                save_data(data, chat_ids=[chat_id])
                schedule_quick_backup(
                    chat_id,
                    MEGA_DELTA_PRIORITY_DELAY_SECONDS if mega_backup_priority_enabled() else MEGA_DELTA_DELAY_SECONDS,
                )
                schedule_config_backup_for_chats(chat_id, delay=0.4)
                finance_changed(chat_id, store.get("current_view_day") or today_key(), reason="category_order_position", delay=0.03)
            send_or_edit_categories_window(
                chat_id, build_category_layout_text(store, "exact"),
                reply_markup=build_category_layout_keyboard(store, "exact", params, chat_id=chat_id),
                preferred_message_id=call.message.message_id, marker_action="cat_order_open_exact:*",
            )
        except Exception as e:
            log_error(f"cat_order_position_exact: {e}")
        return True

    if data_str.startswith("cat_order_move_exact:"):
        try:
            _, slug, direction, start_key, start_rid, end_key, end_rid = data_str.split(":", 6)
            if move_expense_category_order(store, slug, direction):
                save_data(data, chat_ids=[chat_id])
                schedule_config_backup_for_chats(chat_id)
            send_or_edit_categories_window(
                chat_id, build_category_layout_text(store, "exact"),
                reply_markup=build_category_layout_keyboard(store, "exact", (start_key, int(start_rid), end_key, int(end_rid)), chat_id=chat_id),
                preferred_message_id=call.message.message_id, marker_action="cat_order_move_exact:*",
            )
        except Exception as e:
            log_error(f"cat_order_move_exact: {e}")
        return True

    if data_str.startswith("cat_pick_end3:"):
        try:
            _, start_key, start_rid, y, m = data_str.split(":")
            _send_category_pick_end_precise(chat_id, call.message.message_id, start_key, int(start_rid), int(y), int(m))
        except Exception as e:
            log_error(f"cat_pick_end3: {e}")
        return True

    if data_str.startswith("cat_pick_set_end3:"):
        try:
            _, start_key, start_rid, y, m, d = data_str.split(":")
            end_key = _date_key_from_ymd(int(y), int(m), int(d))
            _send_category_pick_end_record(chat_id, call.message.message_id, start_key, int(start_rid), end_key)
        except Exception as e:
            log_error(f"cat_pick_set_end3: {e}")
        return True

    if data_str.startswith("cat_pick_end_record:"):
        try:
            _, start_key, start_rid, end_key, end_rid = data_str.split(":")
            text, _ = summarize_categories_record_range(store, start_key, int(start_rid), end_key, int(end_rid))
            kb = build_categories_record_summary_keyboard(start_key, int(start_rid), end_key, int(end_rid), store)
            send_or_edit_categories_window(
                chat_id,
                text,
                reply_markup=kb,
                preferred_message_id=call.message.message_id,
                marker_action="cat_range_records:*",
            )
        except Exception as e:
            log_error(f"cat_pick_end_record: {e}")
        return True

    if data_str.startswith("cat_usd_toggle_records:"):
        try:
            _, start_key, start_rid, end_key, end_rid = data_str.split(":")
            settings = store.setdefault("settings", {})
            settings["category_usd_enabled"] = not bool(settings.get("category_usd_enabled", False))
            save_data(data, chat_ids=[chat_id])
            if settings["category_usd_enabled"]:
                usd_rate_cached(force=False)
            text, _ = summarize_categories_record_range(store, start_key, int(start_rid), end_key, int(end_rid))
            kb = build_categories_record_summary_keyboard(start_key, int(start_rid), end_key, int(end_rid), store)
            send_or_edit_categories_window(chat_id, text, reply_markup=kb, preferred_message_id=call.message.message_id, marker_action="cat_range_records:*")
        except Exception as e:
            log_error(f"cat_usd_toggle_records: {e}")
        return True

    if data_str.startswith("cat_range_records:"):
        try:
            _, start_key, start_rid, end_key, end_rid = data_str.split(":")
            text, _ = summarize_categories_record_range(store, start_key, int(start_rid), end_key, int(end_rid))
            kb = build_categories_record_summary_keyboard(start_key, int(start_rid), end_key, int(end_rid), store)
            send_or_edit_categories_window(
                chat_id,
                text,
                reply_markup=kb,
                preferred_message_id=call.message.message_id,
                marker_action="cat_range_records:*",
            )
        except Exception as e:
            log_error(f"cat_range_records: {e}")
        return True

    if data_str.startswith("cat_back_records:"):
        try:
            _, start_key, start_rid, end_key, end_rid = data_str.split(":")
            text, _ = summarize_categories_record_range(store, start_key, int(start_rid), end_key, int(end_rid))
            kb = build_categories_record_summary_keyboard(start_key, int(start_rid), end_key, int(end_rid), store)
            send_or_edit_categories_window(
                chat_id,
                text,
                reply_markup=kb,
                preferred_message_id=call.message.message_id,
                marker_action="cat_range_records:*",
            )
        except Exception as e:
            log_error(f"cat_back_records: {e}")
        return True

    if data_str.startswith("cat_other_sort:"):
        try:
            _, start_key, start_rid, end_key, end_rid = data_str.split(":")
            send_or_edit_categories_window(
                chat_id, build_other_sort_text(store, start_key, int(start_rid), end_key, int(end_rid)),
                reply_markup=build_other_sort_keyboard(chat_id, store, start_key, int(start_rid), end_key, int(end_rid)),
                preferred_message_id=call.message.message_id, marker_action="cat_other_sort:*",
            )
        except Exception as e:
            log_error(f"cat_other_sort: {e}")
        return True

    if data_str.startswith("cat_other_sort_toggle:"):
        try:
            _, rid, start_key, start_rid, end_key, end_rid = data_str.split(":")
            key = _other_sort_key(chat_id, start_key, int(start_rid), end_key, int(end_rid))
            selected = _category_other_sort_state.setdefault(key, set())
            rid_i = int(rid)
            if rid_i in selected:
                selected.remove(rid_i)
            else:
                selected.add(rid_i)
            send_or_edit_categories_window(
                chat_id, build_other_sort_text(store, start_key, int(start_rid), end_key, int(end_rid)),
                reply_markup=build_other_sort_keyboard(chat_id, store, start_key, int(start_rid), end_key, int(end_rid)),
                preferred_message_id=call.message.message_id, marker_action="cat_other_sort_toggle:*",
            )
        except Exception as e:
            log_error(f"cat_other_sort_toggle: {e}")
        return True

    if data_str.startswith("cat_other_sort_choose:"):
        try:
            _, start_key, start_rid, end_key, end_rid = data_str.split(":")
            key = _other_sort_key(chat_id, start_key, int(start_rid), end_key, int(end_rid))
            if not _category_other_sort_state.get(key):
                bot.answer_callback_query(call.id, "Сначала выберите записи", show_alert=False)
                return True
            send_or_edit_categories_window(
                chat_id, build_other_sort_target_text(chat_id, start_key, int(start_rid), end_key, int(end_rid)),
                reply_markup=build_other_sort_target_keyboard(chat_id, store, start_key, int(start_rid), end_key, int(end_rid)),
                preferred_message_id=call.message.message_id, marker_action="cat_other_sort_choose:*",
            )
        except Exception as e:
            log_error(f"cat_other_sort_choose: {e}")
        return True

    if data_str.startswith("cat_other_sort_target:"):
        try:
            _, target_slug, start_key, start_rid, end_key, end_rid = data_str.split(":", 5)
            key = _other_sort_key(chat_id, start_key, int(start_rid), end_key, int(end_rid))
            selected = set(_category_other_sort_state.get(key, set()))
            changed = apply_other_sort_target(store, selected, target_slug)
            _category_other_sort_state.pop(key, None)
            if changed:
                save_data(data, chat_ids=[chat_id])
                finance_changed(chat_id, store.get("current_view_day") or today_key(), reason="category_manual_sort", delay=0.05)
                schedule_config_backup_for_chats(chat_id)
            text, _ = summarize_categories_record_range(store, start_key, int(start_rid), end_key, int(end_rid))
            kb = build_categories_record_summary_keyboard(start_key, int(start_rid), end_key, int(end_rid), store)
            send_or_edit_categories_window(
                chat_id, text, reply_markup=kb, preferred_message_id=call.message.message_id, marker_action="cat_range_records:*",
            )
            try:
                bot.answer_callback_query(call.id, f"Перенесено записей: {changed}", show_alert=False)
            except Exception:
                pass
        except Exception as e:
            log_error(f"cat_other_sort_target: {e}")
        return True

    if data_str.startswith("cat_show_records:"):
        try:
            _, start_key, start_rid, end_key, end_rid, slug = data_str.split(":", 5)
            category = get_category_by_slug(slug, store)
            if not category:
                try:
                    bot.answer_callback_query(call.id, "Статья не найдена", show_alert=False)
                except Exception:
                    pass
                return True
            text = build_category_record_detail_text(store, start_key, int(start_rid), end_key, int(end_rid), category)
            kb = build_category_record_detail_keyboard(start_key, int(start_rid), end_key, int(end_rid), category=category, store=store)
            send_or_edit_categories_window(
                chat_id,
                text,
                reply_markup=kb,
                preferred_message_id=call.message.message_id,
                marker_action="cat_show_records:*",
            )
        except Exception as e:
            log_error(f"cat_show_records: {e}")
        return True

    if data_str.startswith("cat_pick_end:"):
        try:
            _, y, m, start_d = data_str.split(":")
            _send_category_pick_end(chat_id, call.message.message_id, int(y), int(m), int(start_d))
        except Exception as e:
            log_error(f"cat_pick_end: {e}")
        return True

    if data_str.startswith("cat_pick_set_end:"):
        try:
            _, y, m, start_d, end_d = data_str.split(":")
            _send_category_pick_end(chat_id, call.message.message_id, int(y), int(m), int(start_d), int(end_d))
        except Exception as e:
            log_error(f"cat_pick_set_end: {e}")
        return True

    if data_str.startswith("cat_pick_end2:"):
        try:
            _, start_key, y, m = data_str.split(":")
            _send_category_pick_end_any_month(chat_id, call.message.message_id, start_key, int(y), int(m))
        except Exception as e:
            log_error(f"cat_pick_end2: {e}")
        return True

    if data_str.startswith("cat_pick_set_end2:"):
        try:
            _, start_key, y, m, d = data_str.split(":")
            end_key = _date_key_from_ymd(int(y), int(m), int(d))
            _send_category_pick_end_any_month(chat_id, call.message.message_id, start_key, int(y), int(m), end_key)
        except Exception as e:
            log_error(f"cat_pick_set_end2: {e}")
        return True

    if data_str.startswith("cat_range_custom2:"):
        try:
            _, start, end = data_str.split(":", 2)
            if end < start:
                start, end = end, start
            label = f"{fmt_date_ddmmyy(start)} — {fmt_date_ddmmyy(end)}"
            text, _ = summarize_categories(store, start, end, label)
            kb = build_categories_summary_keyboard("rng", start, end, store=store)
            send_or_edit_categories_window(chat_id, text, reply_markup=kb, preferred_message_id=call.message.message_id)
        except Exception as e:
            log_error(f"cat_range_custom2: {e}")
        return True

    if data_str.startswith("cat_range_custom:"):
        try:
            _, y, m, a, b = data_str.split(":")
            y, m, a, b = map(int, (y, m, a, b))
            last_day = calendar.monthrange(y, m)[1]
            a = max(1, min(a, last_day))
            b = max(1, min(b, last_day))
            if b < a:
                a, b = b, a
            start = f"{y}-{m:02d}-{a:02d}"
            end = f"{y}-{m:02d}-{b:02d}"
            label = f"{fmt_date_ddmmyy(start)} — {fmt_date_ddmmyy(end)}"
            text, _ = summarize_categories(store, start, end, label)
            kb = build_categories_summary_keyboard("rng", start, end, store=store)
            send_or_edit_categories_window(chat_id, text, reply_markup=kb, preferred_message_id=call.message.message_id)
        except Exception as e:
            log_error(f"cat_range_custom: {e}")
        return True

    if data_str.startswith("cat_rng:"):
        try:
            _, y, m, a, b = data_str.split(":")
            y, m, a, b = map(int, (y, m, a, b))
        except Exception:
            return True

        if m == 12:
            last_day = (datetime(y + 1, 1, 1) - timedelta(days=1)).day
        else:
            last_day = (datetime(y, m + 1, 1) - timedelta(days=1)).day

        a = max(1, min(a, last_day))
        b = max(1, min(b, last_day))
        if b < a:
            b = a

        start = f"{y}-{m:02d}-{a:02d}"
        end = f"{y}-{m:02d}-{b:02d}"
        label = f"{fmt_date_ddmmyy(start)} — {fmt_date_ddmmyy(end)}"
        text, _ = summarize_categories(store, start, end, label)
        kb = build_categories_summary_keyboard("rng", start, end, store=store)
        send_or_edit_categories_window(chat_id, text, reply_markup=kb, preferred_message_id=call.message.message_id)
        return True

    if data_str.startswith("cat_show_wthu:"):
        _, ref, slug = data_str.split(":", 2)
        category = get_category_by_slug(slug, store)
        if not category:
            return True

        start_key = week_start_thursday(ref or today_key())
        start, end = week_bounds_thu_wed(start_key)
        label = f"{fmt_date_ddmmyy(start)} — {fmt_date_ddmmyy(end)} (Чт–Ср)"
        text = build_category_detail_text(store, start, end, category, label)
        kb = build_category_detail_keyboard(start, end, f"cat_wthu:{start}", mode="wthu", slug=slug, store=store)
        send_or_edit_categories_window(chat_id, text, reply_markup=kb, preferred_message_id=call.message.message_id)
        return True

    if data_str.startswith("cat_show_wk:"):
        _, ref, slug = data_str.split(":", 2)
        category = get_category_by_slug(slug, store)
        if not category:
            return True

        start_key = week_start_monday(ref or today_key())
        start, end = week_bounds_from_start(start_key)
        label = f"{fmt_date_ddmmyy(start)} — {fmt_date_ddmmyy(end)} (Пн–Вс)"
        text = build_category_detail_text(store, start, end, category, label)
        kb = build_category_detail_keyboard(start, end, f"cat_wk:{start}", mode="wk", slug=slug, store=store)
        send_or_edit_categories_window(chat_id, text, reply_markup=kb, preferred_message_id=call.message.message_id)
        return True

    if data_str.startswith("cat_show:"):
        _, start, end, slug = data_str.split(":", 3)
        category = get_category_by_slug(slug, store)
        if not category:
            return True

        start_dt = datetime.strptime(start, "%Y-%m-%d")
        end_dt = datetime.strptime(end, "%Y-%m-%d")
        label = f"{fmt_date_ddmmyy(start)} — {fmt_date_ddmmyy(end)}"

        if (end_dt - start_dt).days == 6 and start == week_start_thursday(start):
            back_callback = f"cat_wthu:{start}"
            label += " (Чт–Ср)"
        elif (end_dt - start_dt).days == 6 and start == week_start_monday(start):
            back_callback = f"cat_wk:{start}"
            label += " (Пн–Вс)"
        else:
            if start_dt.year != end_dt.year or start_dt.month != end_dt.month:
                back_callback = f"cat_range_custom2:{start}:{end}"
            else:
                y, m = start_dt.year, start_dt.month
                back_callback = f"cat_rng:{y}:{m}:{start_dt.day}:{end_dt.day}"

        mode = None
        if (end_dt - start_dt).days == 6 and start == week_start_thursday(start):
            mode = "wthu"
        elif (end_dt - start_dt).days == 6 and start == week_start_monday(start):
            mode = "wk"

        text = build_category_detail_text(store, start, end, category, label)
        kb = build_category_detail_keyboard(start, end, back_callback, mode=mode, slug=slug, store=store)
        send_or_edit_categories_window(chat_id, text, reply_markup=kb, preferred_message_id=call.message.message_id)
        return True

    return False
    

_callback_debounce_state = {}


def _callback_should_debounce(call, data_str: str, min_interval: float = 0.12) -> bool:
    """Защита от частых кликов: Telegram уже получил answer_callback_query, поэтому «Загрузка» не висит."""
    try:
        chat_id = int(call.message.chat.id)
        msg_id = int(call.message.message_id)
        data_str = str(data_str or "")
        if data_str == "none":
            return True
        # Для навигации/экспорта/редактирования достаточно одного клика раз в ~0.45 сек на одно окно.
        hot = False
        if data_str.startswith("d:"):
            parts = data_str.split(":", 2)
            action = parts[2] if len(parts) > 2 else ""
            hot = action in {"prev", "next", "today", "open", "back_main", "calendar", "csv_all"}
        elif data_str.startswith("fv:") or data_str.startswith("c:") or data_str.startswith("fc:"):
            hot = True
        elif data_str.startswith(("secday:", "secview:", "secchatcal:", "secmon:", "secmonthlist:")):
            hot = True
        if not hot:
            return False
        key = (chat_id, msg_id, data_str.split(":", 1)[0], data_str.split(":")[-1])
        now_ts = time.time()
        prev_ts = _callback_debounce_state.get(key, 0)
        _callback_debounce_state[key] = now_ts
        skipped = (now_ts - prev_ts) < float(min_interval)
        if skipped:
            try:
                bot_journal("button_debounced", chat_id, data_str)
            except Exception:
                pass
        return skipped
    except Exception:
        return False


# Debounce перерисовки галочек в секретном редактировании:
# быстрые клики собираются, окно обновляется один раз после последнего клика.
_secret_edit_refresh_lock = threading.RLock()
_secret_edit_refresh_timers = {}


def schedule_secret_edit_refresh_window(viewer_chat_id: int, message_id: int, target_chat_id: int, day_key: str, self_only: bool = False, delay: float = 0.7):
    key = (int(viewer_chat_id), int(message_id))
    generation = time.time_ns()
    scheduler_key = f"secret-edit-refresh:{key[0]}:{key[1]}"

    def _job():
        try:
            with _secret_edit_refresh_lock:
                if _secret_edit_refresh_timers.get(key) != generation:
                    return
            text = build_secret_edit_text(int(target_chat_id), day_key)
            kb = build_secret_edit_keyboard(int(viewer_chat_id), int(target_chat_id), day_key, self_only=bool(self_only))
            try:
                fast_ui_edit_message_text(int(viewer_chat_id), int(message_id), text, reply_markup=kb, purpose="secret_edit_debounce")
            except Exception as e:
                if not is_telegram_429(e) and "message is not modified" not in str(e).lower():
                    log_error(f"secret edit debounce refresh {viewer_chat_id}:{message_id}: {e}")
            register_secret_window(int(viewer_chat_id), int(message_id), int(target_chat_id), "edit", day_key=day_key, self_only=bool(self_only))
            schedule_secret_calendar_close(int(viewer_chat_id), int(message_id))
        finally:
            with _secret_edit_refresh_lock:
                if _secret_edit_refresh_timers.get(key) == generation:
                    _secret_edit_refresh_timers.pop(key, None)

    with _secret_edit_refresh_lock:
        DELAYED_SCHEDULER.cancel(scheduler_key)
        _secret_edit_refresh_timers[key] = generation
        DELAYED_SCHEDULER.schedule(scheduler_key, float(delay), _job)

def _answer_callback_query_quiet(callback_id: str):
    try:
        bot.answer_callback_query(callback_id)
    except Exception:
        pass


def answer_callback_query_background(callback_id: str):
    """Снимает Telegram «Загрузка…» параллельно, не задерживая обработку самой кнопки."""
    key = f"callback-ack:{callback_id}"
    if not GENERAL_TASK_POOL.submit(key, _answer_callback_query_quiet, callback_id):
        # При переполнении не блокируем кнопку сетевым вызовом; последующая UI-операция всё равно выполнится.
        try:
            bot_journal("callback_ack_queue_full", None, str(callback_id), "WARN")
        except Exception:
            pass
# v134_flat_reminder
