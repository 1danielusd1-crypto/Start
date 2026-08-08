# v150_excel_reserve_chat_lifecycle
def send_csv_week(chat_id: int, day_key: str):
    if is_finance_output_suppressed(chat_id):
        return
    if usd_transactions_view_enabled(int(chat_id)):
        return send_export_for_chat_to(chat_id, chat_id, "week", day_key, "csv")
    try:
        store = get_chat_store(chat_id)

        base = datetime.strptime(day_key, "%Y-%m-%d")
        start = base - timedelta(days=6)

        rows = []

        for i in range(7):
            d = (start + timedelta(days=i)).strftime("%Y-%m-%d")
            for r in store.get("daily_records", {}).get(d, []):
                rows.append((fmt_date_table(d), fmt_csv_amount(r["amount"]), r.get("note", "")))

        if not rows:
            send_info(chat_id, "Нет данных за неделю")
            return

        tmp = f"week_{chat_id}.csv"

        with open(tmp, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["date", "amount", "note"])
            rows.sort(key=lambda row: str(row[0]))
            write_csv_rows_with_day_gaps(w, rows, 3)

        with open(tmp, "rb") as f:
            bot.send_document(chat_id, f, caption="🗓 CSV за неделю")

    except Exception as e:
        log_error(f"send_csv_week: {e}")
def send_csv_month(chat_id: int, day_key: str):
    if is_finance_output_suppressed(chat_id):
        return
    if usd_transactions_view_enabled(int(chat_id)):
        return send_export_for_chat_to(chat_id, chat_id, "month", day_key, "csv")
    try:
        store = get_chat_store(chat_id)

        base = datetime.strptime(day_key, "%Y-%m-%d")
        start = base.replace(day=1)

        rows = []

        for d, recs in store.get("daily_records", {}).items():
            dt = datetime.strptime(d, "%Y-%m-%d")
            if dt >= start and dt <= base:
                for r in recs:
                    rows.append((fmt_date_table(d), fmt_csv_amount(r["amount"]), r.get("note", "")))

        if not rows:
            send_info(chat_id, "Нет данных за месяц")
            return

        tmp = f"month_{chat_id}.csv"

        with open(tmp, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["date", "amount", "note"])
            rows.sort(key=lambda row: str(row[0]))
            write_csv_rows_with_day_gaps(w, rows, 3)

        with open(tmp, "rb") as f:
            bot.send_document(chat_id, f, caption="📆 CSV за месяц")

    except Exception as e:
        log_error(f"send_csv_month: {e}")
def send_csv_wedthu(chat_id: int, day_key: str):
    if is_finance_output_suppressed(chat_id):
        return
    if usd_transactions_view_enabled(int(chat_id)):
        return send_export_for_chat_to(chat_id, chat_id, "wedthu", day_key, "csv")
    try:
        store = get_chat_store(chat_id)

        base = datetime.strptime(day_key, "%Y-%m-%d")

        while base.weekday() != 2:
            base -= timedelta(days=1)

        start = base

        rows = []

        for i in range(2):
            d = (start + timedelta(days=i)).strftime("%Y-%m-%d")
            for r in store.get("daily_records", {}).get(d, []):
                rows.append((fmt_date_table(d), fmt_csv_amount(r["amount"]), r.get("note", "")))

        if not rows:
            send_info(chat_id, "Нет данных Ср–Чт")
            return

        tmp = f"wedthu_{chat_id}.csv"

        with open(tmp, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["date", "amount", "note"])
            rows.sort(key=lambda row: str(row[0]))
            write_csv_rows_with_day_gaps(w, rows, 3)

        with open(tmp, "rb") as f:
            bot.send_document(chat_id, f, caption="📊 CSV Ср–Чт")

    except Exception as e:
        log_error(f"send_csv_wedthu: {e}")


def finance_operation_key(chat_id: int, source_msg_id, ledger: str = "main") -> str:
    """Stable idempotency key for a finance effect created from a Telegram message."""
    try:
        mid = int(source_msg_id)
    except Exception:
        return ""
    return f"finance:{int(chat_id)}:{str(ledger or 'main')}:{mid}"


def find_record_by_operation_key(chat_id: int, operation_key: str):
    if not operation_key:
        return None
    try:
        store = get_chat_store(int(chat_id))
        for r in store.get("records", []) or []:
            if isinstance(r, dict) and str(r.get("operation_key") or "") == str(operation_key):
                return r
    except Exception:
        pass
    return None

def add_record_to_chat(
    chat_id: int,
    amount: float,
    note: str,
    owner: int,
    source_msg=None,
    day_key=None,
    usd_amount=None,
    usd_note: str = "",
    usd_only: bool = False,
    source_finance_text: str = "",
):
    bot_journal("record_add_start", chat_id, f"amount={amount} note={note}")
    op_id = operation_begin("finance_add", chat_id, target=str(day_key or "auto"), payload={"amount": amount, "note": note}, critical=True) if "operation_begin" in globals() else ""
    if op_id and "operation_step" in globals(): operation_step(op_id, "saved_locally", "intent recorded", persist=False)
    with locked_chat(chat_id):
        store = get_chat_store(chat_id)
        rid = store.get("next_id", 1)

        if not day_key:
            day_key = day_key_from_message(source_msg)

        source_msg_id = getattr(source_msg, "message_id", None) if source_msg else None
        source_order_msg_id = (
            getattr(source_msg, "source_order_msg_id", None)
            or getattr(source_msg, "forward_source_msg_id", None)
            or source_msg_id
        )

        # v109 exact-once guard. A restored/retried Telegram update with the same source
        # message must return the existing finance effect instead of adding the amount again.
        operation_key = finance_operation_key(chat_id, source_msg_id, "main")
        if source_msg_id is not None:
            for existing in store.get("records", []) or []:
                if not isinstance(existing, dict):
                    continue
                if (operation_key and str(existing.get("operation_key") or "") == operation_key) or int(existing.get("source_msg_id") or 0) == int(source_msg_id):
                    if operation_key and not existing.get("operation_key"):
                        existing["operation_key"] = operation_key
                    bot_journal("finance_duplicate_blocked", chat_id, f"source_msg_id={source_msg_id} operation_key={operation_key}")
                    if op_id and "operation_complete" in globals(): operation_complete(op_id, "duplicate blocked; existing record reused")
                    return existing

        rec = {
            "id": rid,
            "short_id": "",
            "timestamp": message_timestamp_iso(source_msg),
            "amount": amount,
            "note": note,
            "source_msg_id": source_msg_id,
            "source_order_msg_id": source_order_msg_id,
            "owner": owner,
            "msg_id": source_msg_id,
            "origin_msg_id": source_msg_id,
            "day_key": day_key,
            "operation_key": operation_key,
        }
        if usd_amount is not None:
            rec["usd_amount"] = float(usd_amount or 0)
            rec["usd_note"] = str(usd_note or note or "")
            rec["usd_only"] = bool(usd_only)
        if source_finance_text:
            rec["source_finance_text"] = str(source_finance_text)

        store.setdefault("records", []).append(rec)
        normalize_chat_records(chat_id)
        store["next_id"] = max([int(r.get("id", 0) or 0) for r in store.get("records", [])] + [0]) + 1
        store["balance"] = sum(float(r.get("amount", 0) or 0) for r in store.get("records", []))

        rebuild_month_short_ids(chat_id)
        rebuild_global_records()
        try:
            finance_cache_invalidate(chat_id, "finance_add")
            finance_integrity_append(chat_id, "add", rec)
        except Exception as _integrity_exc:
            log_error(f"finance add integrity: {_integrity_exc}")
        if op_id and "operation_complete" in globals(): operation_complete(op_id, f"record={rec.get('id')}")
        return rec

def delete_record_in_chat(chat_id: int, rid: int):
    op_id = operation_begin("finance_delete", chat_id, target=str(rid), payload={"rid": rid}, critical=True) if "operation_begin" in globals() else ""
    with locked_chat(chat_id):
        store = get_chat_store(chat_id)
        deleted_record = next((copy.deepcopy(x) for x in store.get("records", []) if int(x.get("id", -1)) == int(rid)), None)

        store["records"] = [x for x in store["records"] if x["id"] != rid]

        for day, arr in list(store.get("daily_records", {}).items()):
            arr2 = [x for x in arr if x["id"] != rid]
            if arr2:
                store["daily_records"][day] = arr2
            else:
                del store["daily_records"][day]

        renumber_chat_records(chat_id)
        store["balance"] = sum(x["amount"] for x in store["records"])
        rebuild_global_records()
        try:
            finance_cache_invalidate(chat_id, "finance_delete")
            finance_integrity_append(chat_id, "delete", deleted_record or {"id": rid})
        except Exception as _integrity_exc:
            log_error(f"finance delete integrity: {_integrity_exc}")
        if op_id and "operation_complete" in globals(): operation_complete(op_id, f"record={rid}")

def renumber_chat_records(chat_id: int):
    """Перенумеровывает записи по реальной хронологии поступления сообщений."""
    store = get_chat_store(chat_id)
    normalize_chat_records(chat_id)
    all_recs = list(store.get("records", []) or [])
    all_recs.sort(key=record_sort_key)

    for new_id, r in enumerate(all_recs, 1):
        r["id"] = new_id

    store["records"] = all_recs
    rebuilt_daily = {}
    for r in all_recs:
        rebuilt_daily.setdefault(_record_day_key(r), []).append(r)
    store["daily_records"] = rebuilt_daily
    store["next_id"] = len(all_recs) + 1
    rebuild_month_short_ids(chat_id)
    
def get_or_create_active_windows(chat_id: int) -> dict:
    return data.setdefault("active_messages", {}).setdefault(str(chat_id), {})
def set_active_window_id(chat_id: int, day_key: str, message_id: int):
    aw = get_or_create_active_windows(chat_id)
    aw[day_key] = message_id
    register_open_window(chat_id, message_id, "main_day", code="О1", day_key=day_key)
    save_data(data)
    try:
        _finance_window_state(chat_id)["auto_reopen_on_boot"] = True
        _sync_finance_window_state_from_runtime(chat_id, schedule_delta=True)
    except Exception:
        pass
def get_active_window_id(chat_id: int, day_key: str):
    aw = get_or_create_active_windows(chat_id)
    return aw.get(day_key)

def clear_active_window_id(chat_id: int, day_key: str):
    try:
        aw = get_or_create_active_windows(chat_id)
        if str(day_key) in aw:
            old_mid = aw.pop(str(day_key), None)
            if old_mid:
                unregister_open_window(chat_id, old_mid)
            save_data(data)
            try:
                _sync_finance_window_state_from_runtime(chat_id, schedule_delta=True)
            except Exception:
                pass
    except Exception as e:
        log_error(f"clear_active_window_id({chat_id},{day_key}): {e}")

def close_previous_main_window_before_back(chat_id: int, day_key: str, current_message_id: int | None = None):
    """При возврате в основное окно удаляет прежнее О1, чтобы не оставалось дубля."""
    try:
        old_mid = get_active_window_id(chat_id, day_key)
        if not old_mid:
            return
        if current_message_id is not None and int(old_mid) == int(current_message_id):
            return
        try:
            bot.delete_message(int(chat_id), int(old_mid))
        except Exception:
            pass
        clear_active_window_id(chat_id, day_key)
    except Exception as e:
        log_error(f"close_previous_main_window_before_back({chat_id},{day_key}): {e}")
def update_or_send_day_window(chat_id: int, day_key: str):
    # v108: hidden accounting is independent from explicitly selected visible window modes/manual opening.
    if is_owner_chat(chat_id):
        backup_window_for_owner(chat_id, day_key)
        schedule_balance_panel_refresh(chat_id, 0.5)
        return

    lock = window_locks[(chat_id, day_key)]

    with lock:
        txt, _ = render_day_window(chat_id, day_key)
        kb = build_main_keyboard(day_key, chat_id)
        old_mid = get_active_window_id(chat_id, day_key)

        if len(txt) > 3900:
            log_error(f"update_or_send_day_window: text too long for {chat_id} {day_key}, len={len(txt)}")

        if old_mid:
            try:
                bot.edit_message_text(
                    txt,
                    chat_id=chat_id,
                    message_id=old_mid,
                    reply_markup=kb,
                    parse_mode="HTML"
                )
                set_active_window_id(chat_id, day_key, old_mid)
                schedule_balance_panel_refresh(chat_id, 0.5)
                return
            except Exception as e:
                err = str(e).lower()
                if "message is not modified" in err:
                    schedule_balance_panel_refresh(chat_id, 0.5)
                    return
                try:
                    bot.delete_message(chat_id, old_mid)
                except Exception:
                    pass

        sent = bot.send_message(
            chat_id,
            txt,
            reply_markup=kb,
            parse_mode="HTML"
        )
        set_active_window_id(chat_id, day_key, sent.message_id)
    schedule_balance_panel_refresh(chat_id, 0.5)
def is_finance_mode(chat_id):

    store = get_chat_store(chat_id)
    return store.get("finance_mode", False)

def set_finance_mode(chat_id: int, enabled: bool):
    """v108: finance accounting and visible finance windows are separate states.

    A fresh OFF -> ON transition always enables hidden finance and starts with all three
    automatic window modes OFF.  Visible modes are selected independently in F39.
    """
    chat_id = int(chat_id)
    store = get_chat_store(chat_id)
    enabled = bool(enabled)
    was_enabled = bool(store.get("finance_mode", False))
    store["finance_mode"] = enabled
    settings = store.setdefault("settings", {})

    if enabled:
        finance_active_chats.add(chat_id)
        if not was_enabled:
            # User requirement: enabling finance = hidden finance ON, no visible auto window yet.
            settings["hidden_finance"] = True
            settings["quick_balance_enabled"] = False
            settings["quick_balance_behavior"] = "normal"
            settings["quick_balance_user_selected"] = True
            state = store.get("finance_window_state")
            if not isinstance(state, dict):
                state = {}
            state.update({
                "mode": "off",
                "main_windows": {},
                "balance_panel_id": None,
                "balance_panel_mode": "mini",
                "current_view_day": str(store.get("current_view_day") or today_key()),
                "auto_reopen_on_boot": False,
                "updated_at": now_local().isoformat(timespec="seconds"),
            })
            store["finance_window_state"] = state
            # Remove stale automatic windows left from a previous finance session.
            try:
                delete_auto_finance_windows_for_chat(chat_id, persist_now=False)
            except Exception:
                pass
    else:
        finance_active_chats.discard(chat_id)
        settings["hidden_finance"] = False
        settings["quick_balance_enabled"] = False
        settings["quick_balance_behavior"] = "normal"
        settings["quick_balance_user_selected"] = True
        state = store.get("finance_window_state")
        if not isinstance(state, dict):
            state = {}
        state.update({
            "mode": "off",
            "main_windows": {},
            "balance_panel_id": None,
            "balance_panel_mode": "mini",
            "auto_reopen_on_boot": False,
            "updated_at": now_local().isoformat(timespec="seconds"),
        })
        store["finance_window_state"] = state
        try:
            delete_auto_finance_windows_for_chat(chat_id, persist_now=False)
        except Exception:
            pass

    save_data(data, chat_ids=[chat_id])
    try:
        schedule_quick_backup(chat_id, 0.5)
    except Exception:
        pass
    schedule_config_backup_for_chats(chat_id)

def require_finance(chat_id: int) -> bool:
    """
    Проверка: включён ли финансовый режим.
    Если нет — показываем подсказку /поехали.
    """
    if not is_finance_mode(chat_id):
        send_and_auto_delete(chat_id, "⚙️ Финансовый режим выключен.\nАктивируйте командой /ok")
        return False
    return True
def refresh_total_message_if_any(chat_id: int):
    """
    Если в чате есть активное сообщение '💰 Общий итог',
    пересчитывает и обновляет его текст.
    """
    store = get_chat_store(chat_id)
    msg_id = store.get("total_msg_id")
    if not msg_id:
        return
    try:
        chat_bal = store.get("balance", 0)
        if not is_owner_chat(chat_id):
            text = wm_common(f"💰 Общий итог по этому чату: {format_chat_amount(chat_id, chat_bal, True)}", 4)
        else:
            lines = []
            info = store.get("info", {})
            title = get_chat_display_name(chat_id)
            lines.append("💰 Общий итог (для владельца)")
            lines.append("")
            lines.append(f"• Этот чат ({title}): {format_chat_amount(chat_id, chat_bal, True)}")
            all_chats = data.get("chats", {})
            allowed_chat_ids = set(tenant_chat_ids(tenant_id_for_chat(chat_id, create=False))) if "tenant_chat_ids" in globals() else {int(x) for x in all_chats.keys()}
            total_all = 0
            other_lines = []
            for cid, st in all_chats.items():
                try:
                    cid_int = int(cid)
                except Exception:
                    continue
                if cid_int not in allowed_chat_ids:
                    continue
                bal = st.get("balance", 0)
                total_all += bal
                if cid_int == chat_id:
                    continue
                info2 = st.get("info", {})
                title2 = get_chat_display_name(cid_int)
                other_lines.append(f"   • {title2}: {format_chat_amount(chat_id, bal, True)}")
            if other_lines:
                lines.append("")
                lines.append("• Другие чаты:")
                lines.extend(other_lines)
            lines.append("")
            lines.append(f"• Всего по всем чатам: {format_chat_amount(chat_id, total_all, True)}")
            text = "\n".join(lines)
        bot.edit_message_text(
            text,
            chat_id=chat_id,
            message_id=msg_id,
            parse_mode="HTML"
        )
        if is_owner_chat(chat_id):
            schedule_owner_total_window_delete(chat_id, msg_id)
    except Exception as e:
        if "message is not modified" in str(e).lower():
            if is_owner_chat(chat_id):
                schedule_owner_total_window_delete(chat_id, msg_id)
            return
        log_error(f"refresh_total_message_if_any({chat_id}): {e}")
        store["total_msg_id"] = None
        save_data(data)
def refresh_owner_after_chat_change(source_chat_id: int):
    if not OWNER_ID:
        return
    try:
        owner_chat_id = int(OWNER_ID)
    except Exception:
        return
    if int(source_chat_id) == owner_chat_id:
        return

    try:
        owner_store = get_chat_store(owner_chat_id)
        owner_day_key = owner_store.get("current_view_day", today_key())
        backup_window_for_owner(owner_chat_id, owner_day_key, None)
        refresh_balance_panel_now(owner_chat_id)
        refresh_total_message_if_any(owner_chat_id)
    except Exception as e:
        log_error(f"refresh_owner_after_chat_change({source_chat_id}): {e}")


def cancel_pending_window_commands(chat_id: int, delete_prompt: bool = False):
    """Назад в основное окно отменяет режимы ожидания предыдущих окон и их таймеры."""
    try:
        clear_forward_copy_edit_wait(chat_id, delete_prompt=delete_prompt)
    except Exception:
        pass
    try:
        clear_edit_wait_state(chat_id, delete_prompt=delete_prompt)
    except Exception:
        pass
    try:
        clear_finwin_edit_wait_state(chat_id, delete_prompt=delete_prompt)
    except Exception:
        pass
    try:
        clear_category_wait_state(chat_id, "category_add_wait", delete_prompt=delete_prompt)
    except Exception:
        pass
    try:
        clear_category_wait_state(chat_id, "category_edit_wait", delete_prompt=delete_prompt)
    except Exception:
        pass
    try:
        _clear_secret_wait(chat_id, delete_prompt=delete_prompt)
    except Exception:
        pass
    try:
        store = get_chat_store(chat_id)
        if store.get("reset_wait"):
            store["reset_wait"] = False
            store["reset_time"] = 0
            save_data(data)
    except Exception:
        pass


def send_info(chat_id: int, text: str):
    send_and_auto_delete(chat_id, text, HELPER_DELETE_DELAY)


@bot.message_handler(commands=["owners", "additional_owners", "доп_владельцы"])
def cmd_additional_owners(msg):
    schedule_command_delete(msg)
    # v148: глобальные дополнительные владельцы заменены изолированными пространствами.
    if "_tenant_send_dashboard" in globals():
        _tenant_send_dashboard(msg)
        return
    if not is_primary_owner(msg.chat.id):
        return
    bot.send_message(
        msg.chat.id,
        wm_owner("👥 Дополнительные владельцы\n\n✅ — доступ владельца включён\n❌ — доступ выключен", 36),
        reply_markup=build_additional_owners_keyboard(),
    )


@bot.message_handler(commands=["windows", "okna", "окна"])
def cmd_windows_in_current_message(msg):
    schedule_command_delete(msg)
    enabled = toggle_chat_buttons_current_window(msg.chat.id)
    send_and_auto_delete(
        msg.chat.id,
        f"{'✅' if enabled else '❌'} Режим открытия в текущем окне: {'ВКЛ' if enabled else 'ВЫКЛ'}",
        8,
    )
                
@bot.message_handler(commands=["ok", "поехали"])
def cmd_ok(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass

    schedule_command_delete(msg)
    chat_id = msg.chat.id
    set_total_secret_mode(chat_id, False)
    if is_finance_output_suppressed(chat_id):
        return
    stop_dozvon_for_target(chat_id)
    store = get_chat_store(chat_id)

    set_finance_mode(chat_id, True)
    view_day = finance_today_key()
    store["current_view_day"] = view_day
    store.setdefault("settings", {})["auto_add"] = True

    save_data(data)
    schedule_finalize(chat_id, view_day)

    send_and_auto_delete(chat_id, "✅ Финансовый режим включён", HELPER_DELETE_DELAY)
@bot.message_handler(commands=["start"])
def cmd_start(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass

    schedule_command_delete(msg)
    try:
        if "tenant_handle_start_payload" in globals() and tenant_handle_start_payload(msg):
            return
    except Exception as e:
        log_error(f"tenant start payload: {e}")
    chat_id = msg.chat.id
    set_total_secret_mode(chat_id, False)
    if is_finance_output_suppressed(chat_id):
        return
    stop_dozvon_for_target(chat_id)

    if guard_non_owner_finance_for_command(msg, {"ok", "help"}):
        return
    if not require_finance(chat_id):
        return

    day_key = finance_today_key() if is_finance_mode(chat_id) else today_key()
    get_chat_store(chat_id)["current_view_day"] = day_key
    force_new_day_window(chat_id, day_key)
@bot.message_handler(commands=["help"])
def cmd_help(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass
    schedule_command_delete(msg)
    chat_id = msg.chat.id
    if is_finance_output_suppressed(chat_id):
        return
    stop_dozvon_for_target(chat_id)
    help_text = build_help_text(chat_id)
    send_and_auto_delete(chat_id, help_text, HELPER_DELETE_DELAY)

@bot.message_handler(commands=["articles", "статьи"])
def cmd_articles(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass
    schedule_command_delete(msg)
    chat_id = msg.chat.id
    if is_finance_output_suppressed(chat_id):
        return
    send_and_auto_delete(chat_id, build_articles_description_text(chat_id), 40)
    
@bot.message_handler(commands=["restore"])
def cmd_restore(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass

    schedule_command_delete(msg)
    if guard_non_owner_finance_for_command(msg, {"ok", "help"}):
        return
    stop_dozvon_for_target(msg.chat.id)

    global restore_mode
    restore_mode = msg.chat.id  # включаем только для текущего чата
    data["_restore_mode_chat_v150"] = int(msg.chat.id)
    save_data(data, chat_ids=[int(msg.chat.id)])
    cleanup_forward_links(msg.chat.id)
    send_and_auto_delete(
        msg.chat.id,
        "📥 Режим восстановления включён.\n"
        "Отправьте JSON/CSV файл для восстановления."
    )
    
@bot.message_handler(commands=["restore_off"])
def cmd_restore_off(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass

    schedule_command_delete(msg)
    if guard_non_owner_finance_for_command(msg, {"ok", "help"}):
        return
    stop_dozvon_for_target(msg.chat.id)

    global restore_mode
    restore_mode = None  # выключаем
    data.pop("_restore_mode_chat_v150", None)
    save_data(data, chat_ids=[int(msg.chat.id)])
    cleanup_forward_links(msg.chat.id)
    send_and_auto_delete(msg.chat.id, "🔒 Режим восстановления выключен.")
@bot.message_handler(commands=["ping"])
def cmd_ping(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass

    schedule_command_delete(msg)
    if guard_non_owner_finance_for_command(msg, {"ok", "help"}):
        return
    stop_dozvon_for_target(msg.chat.id)
    send_and_auto_delete(msg.chat.id, "PONG — бот работает 🟢", HELPER_DELETE_DELAY)
@bot.message_handler(commands=["prev"])
def cmd_prev(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass

    schedule_command_delete(msg)
    chat_id = msg.chat.id
    if is_finance_output_suppressed(chat_id):
        return
    stop_dozvon_for_target(chat_id)
    if guard_non_owner_finance_for_command(msg, {"ok", "help"}):
        return
    if not require_finance(chat_id):
        return
    d = datetime.strptime(today_key(), "%Y-%m-%d") - timedelta(days=1)
    day_key = d.strftime("%Y-%m-%d")
    get_chat_store(chat_id)["current_view_day"] = day_key
    update_or_send_day_window(chat_id, day_key)
@bot.message_handler(commands=["next"])
def cmd_next(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass

    schedule_command_delete(msg)
    chat_id = msg.chat.id
    if is_finance_output_suppressed(chat_id):
        return
    stop_dozvon_for_target(chat_id)
    if guard_non_owner_finance_for_command(msg, {"ok", "help"}):
        return
    if not require_finance(chat_id):
        return
    d = datetime.strptime(today_key(), "%Y-%m-%d") + timedelta(days=1)
    day_key = d.strftime("%Y-%m-%d")
    get_chat_store(chat_id)["current_view_day"] = day_key
    update_or_send_day_window(chat_id, day_key)
@bot.message_handler(commands=["balance"])
def cmd_balance(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass

    schedule_command_delete(msg)
    chat_id = msg.chat.id
    if is_finance_output_suppressed(chat_id):
        return
    stop_dozvon_for_target(chat_id)
    if guard_non_owner_finance_for_command(msg, {"ok", "help"}):
        return
    if not require_finance(chat_id):
        return
    store = get_chat_store(chat_id)
    bal = store.get("balance", 0)
    send_info(chat_id, f"💰 Баланс: {fmt_num(bal)}")
@bot.message_handler(commands=["report"])
def cmd_report(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass

    schedule_command_delete(msg)
    chat_id = msg.chat.id
    if is_finance_output_suppressed(chat_id):
        return
    stop_dozvon_for_target(chat_id)
    if guard_non_owner_finance_for_command(msg, {"ok", "help"}):
        return
    if not require_finance(chat_id):
        return

    lines = build_day_report_lines(chat_id)
    report_html = "<pre>" + html.escape("\n".join(lines)) + "</pre>"
    send_html_and_auto_delete(chat_id, report_html, 20)
def cmd_csv_all(chat_id: int):
    """
    Общий CSV этого чата (все дни этого чата).
    """
    if is_finance_output_suppressed(chat_id):
        return
    if not require_finance(chat_id):
        return
    try:
        save_chat_json(chat_id)
        path = chat_csv_file(chat_id)
        if not os.path.exists(path):
            send_info(chat_id, "CSV файла ещё нет.")
            return
        with open(path, "rb") as f:
            bot.send_document(
                chat_id,
                f,
                caption=f"📂 Общий CSV: {get_chat_display_name(chat_id)}"
            )
    except Exception as e:
        log_error(f"cmd_csv_all: {e}")
def cmd_csv_day(chat_id: int, day_key: str):
    """CSV только за один день для текущего чата, date DD:MM:YY."""
    if is_finance_output_suppressed(chat_id):
        return
    if not require_finance(chat_id):
        return
    store = get_chat_store(chat_id)
    day_recs = sorted(store.get("daily_records", {}).get(day_key, []) or [], key=record_sort_key)
    if not day_recs:
        send_info(chat_id, "Нет записей за этот день.")
        return
    tmp_name = f"data_{chat_id}_{day_key}.csv"
    try:
        with open(tmp_name, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["date", "chat", "ID", "short_id", "timestamp", "amount", "note", "owner", "day_key"])
            rows = []
            for r in day_recs:
                rows.append((
                    fmt_date_table(day_key),
                    get_chat_display_name(chat_id),
                    r.get("id"),
                    r.get("short_id"),
                    r.get("timestamp"),
                    fmt_csv_amount(r.get("amount")),
                    r.get("note"),
                    r.get("owner"),
                    day_key,
                ))
            write_csv_rows_with_day_gaps(w, rows, 9)
        with open(tmp_name, "rb") as f:
            bot.send_document(chat_id, f, caption=f"📅 CSV за день {fmt_date_table(day_key)}: {get_chat_display_name(chat_id)}")
    except Exception as e:
        log_error(f"cmd_csv_day: {e}")
    finally:
        try:
            os.remove(tmp_name)
        except FileNotFoundError:
            pass

@bot.message_handler(commands=["runtime_export"])
def cmd_runtime_export(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass
    schedule_command_delete(msg)
    chat_id = int(msg.chat.id)
    if "tenant_require_platform_owner" in globals():
        if not tenant_require_platform_owner(msg):
            return
    elif not is_owner_chat(chat_id):
        send_and_auto_delete(chat_id, "Эта команда только для владельца платформы.", 8)
        return
    start_dt, end_dt = _runtime_export_parse_range(getattr(msg, "text", "") or "")
    ok, info = submit_interactive_file_job(chat_id, "runtime", "Runtime / Watcher ZIP", send_runtime_export_zip, chat_id, start_dt, end_dt)
    if not ok:
        send_and_auto_delete(chat_id, f"⏳ {info}. Новая копия в очередь не добавлена.", 12)


@bot.message_handler(commands=["tabl_lsx"])
def cmd_tabl_lsx(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass
    schedule_command_delete(msg)
    chat_id = msg.chat.id
    if is_finance_output_suppressed(chat_id):
        return
    stop_dozvon_for_target(chat_id)
    if guard_non_owner_finance_for_command(msg, {"ok", "help", "tabl_lsx"}):
        return
    if not require_finance(chat_id):
        return
    ok, info = submit_interactive_file_job(chat_id, "tabl_lsx", "Excel /tabl_lsx", send_tabl_lsx_for_chat, chat_id, chat_id)
    if not ok:
        send_and_auto_delete(chat_id, f"⏳ {info}. Новая копия в очередь не добавлена.", 12)


@bot.message_handler(commands=["xlsx", "excel"])
def cmd_xlsx(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass
    schedule_command_delete(msg)
    chat_id = msg.chat.id
    if is_finance_output_suppressed(chat_id):
        return
    stop_dozvon_for_target(chat_id)
    if guard_non_owner_finance_for_command(msg, {"ok", "help"}):
        return
    if not require_finance(chat_id):
        return
    ok, info = submit_interactive_file_job(chat_id, "xlsx", "Excel за всё время", send_export_for_chat_to, chat_id, chat_id, "all", today_key(), "xlsx")
    if not ok:
        send_and_auto_delete(chat_id, f"⏳ {info}. Новая копия в очередь не добавлена.", 10)

def _send_csv_current_chat_job(chat_id: int):
    """Export the current ledger. In 💵 USD operations mode this is a pure USD CSV."""
    try:
        if usd_transactions_view_enabled(int(chat_id)):
            return send_export_for_chat_to(chat_id, chat_id, "all", today_key(), "csv")
        _file_job_progress("обновляю CSV", force=True)
        export_global_csv(data)
        save_chat_json(chat_id)
        per_csv = chat_csv_file(chat_id)
        sent = None
        if os.path.exists(per_csv):
            _file_job_progress("отправляю CSV в Telegram", force=True)
            with open(per_csv, "rb") as f:
                sent = _tg_call_retry(bot.send_document, chat_id, f, caption="📂 CSV этого чата", timeout=120, purpose="manual_csv_export")
        if OWNER_ID and chat_id == int(OWNER_ID):
            meta = _load_csv_meta()
            if sent and getattr(sent, "document", None):
                meta["file_id_csv"] = sent.document.file_id
            meta["message_id_csv"] = getattr(sent, "message_id", meta.get("message_id_csv"))
            _save_csv_meta(meta)
        send_backup_to_channel(chat_id)
        return True
    except Exception as e:
        log_error(f"_send_csv_current_chat_job: {e}")
        return False


@bot.message_handler(commands=["csv"])
def cmd_csv(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass

    """
    Экспортирует CSV текущего чата.
    """
    schedule_command_delete(msg)
    chat_id = msg.chat.id
    if is_finance_output_suppressed(chat_id):
        return
    stop_dozvon_for_target(chat_id)
    if guard_non_owner_finance_for_command(msg, {"ok", "help"}):
        return
    if not require_finance(chat_id):
        return
    ok, info = submit_interactive_file_job(chat_id, "csv", "CSV этого чата", _send_csv_current_chat_job, chat_id)
    if not ok:
        send_and_auto_delete(chat_id, f"⏳ {info}. Новая копия в очередь не добавлена.", 10)
def _send_json_snapshot_job(chat_id: int):
    started = time.time()
    try:
        bot_journal("json_export_start", chat_id, "создание атомарного снимка")
        store = snapshot_chat_store(chat_id)
        payload = build_chat_backup_payload(chat_id, store)
        raw = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        buf = io.BytesIO(raw)
        buf.name = f"{mega_safe_name(get_chat_display_name(chat_id), 'chat')}_{now_local().strftime('%Y%m%d_%H%M%S')}.json"
        _file_job_progress("отправляю JSON в Telegram", force=True)
        sent = _tg_call_retry(bot.send_document, chat_id, buf, caption="🧾 JSON этого чата — последние операции сверху", timeout=120, purpose="manual_json_export")
        elapsed = time.time() - started
        bot_journal("json_export_sent", chat_id, f"bytes={len(raw)} message_id={getattr(sent, 'message_id', '')} elapsed={elapsed:.3f}s")
        return True
    except Exception as e:
        bot_journal("json_export_error", chat_id, f"elapsed={time.time()-started:.3f}s error={e}", "ERROR")
        send_and_auto_delete(chat_id, "❌ Не удалось создать JSON. Ошибка записана в журнал.", 15)
        return False


@bot.message_handler(commands=["json"])
def cmd_json(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass
    schedule_command_delete(msg)
    chat_id = int(msg.chat.id)
    if is_finance_output_suppressed(chat_id):
        return
    stop_dozvon_for_target(chat_id)
    if guard_non_owner_finance_for_command(msg, {"ok", "help"}):
        return
    if not require_finance(chat_id):
        return
    bot_journal("json_command", chat_id, f"message_id={getattr(msg, 'message_id', '')}")
    ok, info = submit_interactive_file_job(chat_id, "json", "JSON снимок чата", _send_json_snapshot_job, chat_id)
    if not ok:
        send_and_auto_delete(chat_id, f"⏳ {info}. Новая копия в очередь не добавлена.", 10)

@bot.message_handler(commands=["reset"])
def cmd_reset(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass

    schedule_command_delete(msg)
    chat_id = msg.chat.id
    if is_finance_output_suppressed(chat_id):
        return
    stop_dozvon_for_target(chat_id)
    if guard_non_owner_finance_for_command(msg, {"ok", "help"}):
        return
    if not require_finance(chat_id):
        return
    store = get_chat_store(chat_id)
    store["reset_wait"] = True
    store["reset_time"] = time.time()
    save_data(data)
    send_and_auto_delete(
        chat_id,
        "⚠️ Вы уверены, что хотите обнулить данные? Напишите ДА в течение 15 секунд.",
        15
    )
    schedule_cancel_wait(chat_id, 15)

@bot.message_handler(commands=["stopforward"])
def cmd_stopforward(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass

    schedule_command_delete(msg)
    chat_id = msg.chat.id
    if is_finance_output_suppressed(chat_id):
        return
    stop_dozvon_for_target(chat_id)
    if guard_non_owner_finance_for_command(msg, {"ok", "help"}):
        return
    if not is_owner_chat(chat_id):
        send_info(chat_id, "Эта команда только для владельца.")
        schedule_command_delete(msg)
        return
    clear_forward_all()
    send_info(chat_id, "Пересылка полностью отключена.")
@bot.message_handler(commands=["backup_channel_on"])
def cmd_on_channel(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass

    schedule_command_delete(msg)
    chat_id = msg.chat.id
    if is_finance_output_suppressed(chat_id):
        return
    stop_dozvon_for_target(chat_id)
    if guard_non_owner_finance_for_command(msg, {"ok", "help"}):
        return
    if "tenant_require_platform_owner" in globals():
        if not tenant_require_platform_owner(msg):
            return
    elif not is_owner_chat(chat_id):
        send_and_auto_delete(chat_id, "Эта команда только для владельца платформы.", HELPER_DELETE_DELAY)
        return
    backup_flags["channel"] = True
    save_data(data)
    send_info(chat_id, "📡 Бэкап в канал включён")
@bot.message_handler(commands=["backup_channel_off"])
def cmd_off_channel(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass

    schedule_command_delete(msg)
    chat_id = msg.chat.id
    if is_finance_output_suppressed(chat_id):
        return
    stop_dozvon_for_target(chat_id)
    if guard_non_owner_finance_for_command(msg, {"ok", "help"}):
        return
    if "tenant_require_platform_owner" in globals():
        if not tenant_require_platform_owner(msg):
            return
    elif not is_owner_chat(chat_id):
        send_and_auto_delete(chat_id, "Эта команда только для владельца платформы.", HELPER_DELETE_DELAY)
        return
    backup_flags["channel"] = False
    save_data(data)
    send_info(chat_id, "📡 Бэкап в канал выключен")
    
@bot.message_handler(commands=["dozvon"])
def cmd_dozvon(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass

    schedule_command_delete(msg)
    chat_id = msg.chat.id
    if is_finance_output_suppressed(chat_id):
        return
    stop_dozvon_for_target(chat_id)
    if guard_non_owner_finance_for_command(msg, {"ok", "help"}):
        return

    connected = get_connected_chat_ids(chat_id)
    if not connected:
        send_and_auto_delete(chat_id, "📞 Нет связанных чатов для дозвона.", HELPER_DELETE_DELAY)
        return

    bot.send_message(
        chat_id,
        "📞 Выберите чат для дозвона:",
        reply_markup=build_dozvon_menu(chat_id)
    )

def send_and_auto_delete(chat_id: int, text: str, delay: int = HELPER_DELETE_DELAY):
    if is_finance_output_suppressed(chat_id):
        return
    if chat_buttons_current_window_enabled(chat_id):
        send_or_edit_stored_window(chat_id, "command_window_id", text, delay=delay)
        return
    try:
        msg = bot.send_message(chat_id, text)
        def _delete():
            try:
                bot.delete_message(chat_id, msg.message_id)
            except Exception:
                pass
        DELAYED_SCHEDULER.schedule(f"auto-delete:{chat_id}:{msg.message_id}", delay, _delete)
    except Exception as e:
        log_error(f"send_and_auto_delete: {e}")


def send_html_and_auto_delete(chat_id: int, html_text: str, delay: int = HELPER_DELETE_DELAY):
    if is_finance_output_suppressed(chat_id):
        return
    if chat_buttons_current_window_enabled(chat_id):
        send_or_edit_stored_window(chat_id, "command_window_id", html_text, parse_mode="HTML", delay=delay)
        return
    try:
        msg = bot.send_message(chat_id, html_text, parse_mode="HTML")
        def _delete():
            try:
                bot.delete_message(chat_id, msg.message_id)
            except Exception:
                pass
        DELAYED_SCHEDULER.schedule(f"auto-delete-html:{chat_id}:{msg.message_id}", delay, _delete)
    except Exception as e:
        log_error(f"send_html_and_auto_delete: {e}")
def delete_message_later(chat_id: int, message_id: int, delay: int = 30):
    """
    Отложенное удаление сообщения пользователя (например, команд).
    """
    try:
        def _job():
            try:
                bot.delete_message(chat_id, message_id)
            except Exception:
                pass
        DELAYED_SCHEDULER.schedule(f"delete-later:{chat_id}:{message_id}", delay, _job)
    except Exception as e:
        log_error(f"delete_message_later: {e}")
_edit_cancel_timers = {}


def clear_edit_wait_state(chat_id: int, expected_prompt_id: int | None = None, delete_prompt: bool = True):
    store = get_chat_store(chat_id)
    edit_wait = store.get("edit_wait") or {}
    prompt_id = edit_wait.get("prompt_msg_id")

    if expected_prompt_id is not None and prompt_id and int(prompt_id) != int(expected_prompt_id):
        return False

    key = (int(chat_id), "edit_wait")
    _edit_cancel_timers.pop(key, None)
    DELAYED_SCHEDULER.cancel(f"edit-wait:{int(chat_id)}")

    store["edit_wait"] = None
    save_data(data)

    if delete_prompt and prompt_id:
        try:
            bot.delete_message(chat_id, int(prompt_id))
        except Exception:
            pass
    return True


def clear_finwin_edit_wait_state(chat_id: int, expected_prompt_id: int | None = None, delete_prompt: bool = True):
    store = get_chat_store(chat_id)
    edit_wait = store.get("finwin_edit_wait") or {}
    prompt_id = edit_wait.get("prompt_msg_id")

    if expected_prompt_id is not None and prompt_id and int(prompt_id) != int(expected_prompt_id):
        return False

    key = (int(chat_id), "finwin_edit_wait")
    _edit_cancel_timers.pop(key, None)
    DELAYED_SCHEDULER.cancel(f"finwin-edit-wait:{int(chat_id)}")

    store["finwin_edit_wait"] = None
    save_data(data)

    if delete_prompt and prompt_id:
        try:
            bot.delete_message(chat_id, int(prompt_id))
        except Exception:
            pass
    return True


def _edit_countdown_text(base_text: str, remaining: int) -> str:
    base = strip_window_mark(str(base_text or "")).rstrip()
    return wm_common(base + f"\n\n⏳ До закрытия: {int(remaining)} сек.", 10)


def schedule_cancel_finwin_edit(chat_id: int, prompt_message_id: int, delay: float | None = None):
    """Единый таймер фин-редактирования; timeout отменяет ввод и возвращает основное окно."""
    key = (int(chat_id), "finwin_edit_wait")
    scheduler_key = f"finwin-edit-wait:{int(chat_id)}"
    if delay is None:
        delay = internal_timer_seconds("input_wait", 40)

    def _job():
        try:
            store = get_chat_store(chat_id)
            wait = store.get("finwin_edit_wait") or {}
            if not wait or int(wait.get("prompt_msg_id") or 0) != int(prompt_message_id):
                return
            cleared = clear_finwin_edit_wait_state(chat_id, prompt_message_id, delete_prompt=False)
            if cleared:
                day_key = store.get("current_view_day") or today_key()
                return_to_main_window_closing_previous(chat_id, day_key, int(prompt_message_id))
                log_info(f"finwin edit_wait auto-cancelled for chat {chat_id}")
        except Exception as e:
            log_error(f"schedule_cancel_finwin_edit({chat_id},{prompt_message_id}): {e}")

    DELAYED_SCHEDULER.cancel(scheduler_key)
    deadline = DELAYED_SCHEDULER.schedule(scheduler_key, float(delay), _job)
    _edit_cancel_timers[key] = deadline


def schedule_cancel_edit(chat_id: int, prompt_message_id: int, delay: float | None = None):
    """Единый таймер обычного редактирования; timeout отменяет ввод и возвращает основное окно."""
    key = (int(chat_id), "edit_wait")
    scheduler_key = f"edit-wait:{int(chat_id)}"
    if delay is None:
        delay = internal_timer_seconds("input_wait", 40)

    def _job():
        try:
            store = get_chat_store(chat_id)
            wait = store.get("edit_wait") or {}
            if not wait or int(wait.get("prompt_msg_id") or 0) != int(prompt_message_id):
                return
            cleared = clear_edit_wait_state(chat_id, prompt_message_id, delete_prompt=False)
            if cleared:
                day_key = store.get("current_view_day") or today_key()
                return_to_main_window_closing_previous(chat_id, day_key, int(prompt_message_id))
        except Exception as e:
            log_error(f"schedule_cancel_edit({chat_id},{prompt_message_id}): {e}")

    DELAYED_SCHEDULER.cancel(scheduler_key)
    deadline = DELAYED_SCHEDULER.schedule(scheduler_key, float(delay), _job)
    _edit_cancel_timers[key] = deadline


def schedule_cancel_wait(chat_id: int, delay: float = 15.0):
    """Через delay секунд сбрасывает reset_wait через общий планировщик."""
    scheduler_key = f"reset-wait:{int(chat_id)}"

    def _job():
        try:
            store = get_chat_store(chat_id)
            changed = False
            if store.get("reset_wait", False):
                store["reset_wait"] = False
                store["reset_time"] = 0
                changed = True
            if changed:
                save_data(data)
        except Exception as e:
            log_error(f"schedule_cancel_wait job: {e}")

    DELAYED_SCHEDULER.cancel(scheduler_key)
    deadline = DELAYED_SCHEDULER.schedule(scheduler_key, float(delay), _job)
    _edit_cancel_timers[int(chat_id)] = deadline



def _remember_known_chat_user(store: dict, msg) -> bool:
    """Кэширует пользователей, которых бот реально видел в чате.

    Telegram Bot API не выдаёт полный список участников группы, поэтому этот кэш
    дополняет список администраторов в окне «Описание чатов».
    """
    try:
        user = getattr(msg, "from_user", None)
        if user is None or not getattr(user, "id", None):
            return False
        uid = str(int(user.id))
        users = store.setdefault("known_users", {})
        old = dict(users.get(uid) or {})
        now_ts = time.time()
        row = {
            "id": int(user.id),
            "first_name": str(getattr(user, "first_name", "") or ""),
            "last_name": str(getattr(user, "last_name", "") or ""),
            "username": str(getattr(user, "username", "") or "").lstrip("@") or None,
            "is_bot": bool(getattr(user, "is_bot", False)),
            "is_premium": bool(getattr(user, "is_premium", False)),
            "language_code": str(getattr(user, "language_code", "") or "") or None,
            "last_seen": old.get("last_seen") or now_local().isoformat(timespec="seconds"),
            "last_seen_ts": float(old.get("last_seen_ts") or 0),
        }
        if now_ts - float(row.get("last_seen_ts") or 0) >= 3600 or not old:
            row["last_seen"] = now_local().isoformat(timespec="seconds")
            row["last_seen_ts"] = now_ts
        changed = old != row
        if changed:
            users[uid] = row
            if len(users) > 500:
                ordered = sorted(users.items(), key=lambda item: float((item[1] or {}).get("last_seen_ts") or 0))
                for old_uid, _ in ordered[:len(users) - 500]:
                    users.pop(old_uid, None)
        return changed
    except Exception:
        return False


def update_chat_info_from_message(msg):
    """
    Обновляет информацию о чате в памяти.
    На диск пишем только если реально что-то изменилось.
    """
    chat_id = msg.chat.id
    was_new_chat = str(chat_id) not in (data.get("chats", {}) if isinstance(data, dict) else {})
    try:
        if not getattr(getattr(msg, "from_user", None), "is_bot", False):
            stop_dozvon_for_target(chat_id)
    except Exception:
        pass
    store = get_chat_store(chat_id)
    try:
        if store.setdefault("settings", {}).get("bot_removed"):
            store["settings"]["bot_removed"] = False
            store["settings"].pop("bot_removed_reason", None)
            store["settings"].pop("bot_removed_at", None)
            save_data(data)
    except Exception:
        pass
    info = store.setdefault("info", {})
    # У каналов/чатов username может отсутствовать. Не обращаемся к ключам напрямую,
    # чтобы не ловить KeyError: 'username' на channel_post / edited_channel_post.
    info.setdefault("title", "")
    info.setdefault("username", None)
    info.setdefault("type", getattr(msg.chat, "type", None))

    changed = False

    # В личных чатах callback приходит от самого бота. Не даём ему затирать имя пользователя названием бота.
    try:
        if getattr(getattr(msg, "from_user", None), "is_bot", False) and not getattr(msg.chat, "title", None):
            return
    except Exception:
        pass

    new_title = _chat_title_from_message(msg, info.get("title") or "")
    new_username = _chat_username_from_message(msg)
    new_type = msg.chat.type

    if _remember_known_chat_user(store, msg):
        changed = True

    if info.get("title") != new_title:
        info["title"] = new_title
        changed = True

    if info.get("username") != new_username:
        info["username"] = new_username
        changed = True

    if info.get("type") != new_type:
        info["type"] = new_type
        changed = True

    if OWNER_ID and str(chat_id) != str(OWNER_ID):
        owner_store = get_chat_store(int(OWNER_ID))
        kc = owner_store.setdefault("known_chats", {})

        new_known = {
            "title": info.get("title") or get_chat_display_name(chat_id),
            "username": info.get("username"),
            "type": info.get("type"),
        }

        # Перед добавлением убираем старые карточки того же чата по username/title, чтобы не плодить дубли.
        new_identity = _chat_identity_key(chat_id, new_known)
        for old_cid, old_info in list(kc.items()):
            try:
                old_id_int = int(old_cid)
            except Exception:
                kc.pop(old_cid, None)
                changed = True
                continue
            if str(old_cid) != str(chat_id) and _chat_identity_key(old_id_int, old_info if isinstance(old_info, dict) else {}) == new_identity:
                kc.pop(old_cid, None)
                changed = True
        if kc.get(str(chat_id)) != new_known:
            kc[str(chat_id)] = new_known
            changed = True

    if changed:
        save_data(data)
        # Если имя/username чата изменились, обновляем карточку памяти/known_chats и бэкапы,
        # чтобы у владельца и в backup-файлах отображалось актуальное название.
        try:
            ids_for_backup = [chat_id]
            if OWNER_ID:
                ids_for_backup.append(int(OWNER_ID))
            schedule_config_backup_for_chats(*ids_for_backup, delay=2.0)
        except Exception as e:
            log_error(f"chat info changed backup schedule {chat_id}: {e}")

    try:
        if was_new_chat and OWNER_ID and str(chat_id) != str(OWNER_ID):
            maybe_prompt_owner_for_new_chat_auto_backup(chat_id)
    except Exception as e:
        log_error(f"new chat auto-backup prompt failed for {get_chat_display_name(chat_id)}: {e}")


def maybe_prompt_owner_for_new_chat_auto_backup(chat_id: int):
    """При первом появлении чата спрашиваем владельца, обновлять ли JSON/CSV бэкапы автоматически."""
    if not OWNER_ID:
        return
    store = get_chat_store(chat_id)
    settings = store.setdefault("settings", {})
    if settings.get("owner_auto_backup_prompted"):
        return
    settings["owner_auto_backup_prompted"] = True
    settings.setdefault("auto_backup_enabled", True)
    save_data(data)

    owner_id = int(OWNER_ID)
    title = get_chat_display_name(chat_id)
    text = (
        "🆕 Новый чат появился в картотеке\n\n"
        f"{title}\n"
        "Автоматически обновлять JSON/CSV бэкапы по этому чату?"
    )
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.row(
        IB("✅ Да", callback_data=f"ncb:{chat_id}:yes"),
        IB("❌ Нет", callback_data=f"ncb:{chat_id}:no"),
    )
    msg = bot.send_message(owner_id, text, reply_markup=kb)
    settings["owner_auto_backup_prompt_msg_id"] = msg.message_id
    save_data(data)
    delete_message_later(owner_id, msg.message_id, 10)



def _safe_tmp_json_name(fname: str) -> str:
    base = os.path.basename(str(fname or "backup.json"))
    base = re.sub(r"[^0-9A-Za-zА-Яа-я_.\-]+", "_", base)
    if not base.lower().endswith(".json"):
        base += ".json"
    return base[:80]


def _extract_chat_id_from_json_filename(fname: str):
    """Пытается вытащить chat_id из имени data_<chat_id>.json."""
    try:
        m = re.search(r"data_(-?\d+)\.json$", str(fname or "").strip().lower())
        if m:
            return int(m.group(1))
    except Exception:
        pass
    return None


def _describe_json_restore_payload(payload, fname: str = ""):
    """Возвращает короткое описание JSON перед подтверждением восстановления."""
    fname_l = str(fname or "").lower()
    if fname_l == "csv_meta.json":
        return "метаданные CSV", None
    if isinstance(payload, dict) and isinstance(payload.get("chats"), dict):
        return f"глобальный data.json, чатов: {len(payload.get('chats') or {})}", None
    if isinstance(payload, dict):
        cid = payload.get("chat_id")
        if cid is None:
            cid = _extract_chat_id_from_json_filename(fname)
        if cid is not None:
            try:
                cid = int(cid)
                rec_count = len(payload.get("records") or []) if isinstance(payload.get("records"), list) else 0
                daily = payload.get("daily_records") or {}
                if isinstance(daily, dict):
                    rec_count = rec_count or sum(len(v or []) for v in daily.values())
                return f"JSON чата {get_chat_display_name(cid)} / ID {cid}, записей: {rec_count}", cid
            except Exception:
                pass
    return "JSON-файл неизвестного формата", None


def _apply_json_restore_from_owner_prompt(owner_chat_id: int, tmp_path: str, fname: str) -> str:
    """
    Восстановление JSON, когда владелец прислал файл без /restore и нажал ✅ Да.
    Поддерживает:
    • глобальный data.json / JSON с ключом chats;
    • csv_meta.json;
    • per-chat JSON data_<chat_id>.json или JSON с chat_id.
    """
    global data, restore_mode

    fname_l = str(fname or "").lower()
    payload = _load_json(tmp_path, None)
    if not isinstance(payload, dict):
        raise RuntimeError("JSON повреждён или не является объектом")

    # csv_meta.json
    if fname_l == "csv_meta.json":
        os.replace(tmp_path, CSV_META_FILE)
        _save_csv_meta(_load_json(CSV_META_FILE, {}) or {})
        restore_mode = None
        return "🟢 csv_meta.json обновлён"

    # Глобальный data.json
    if fname_l == "data.json" or isinstance(payload.get("chats"), dict):
        os.replace(tmp_path, DATA_FILE)
        _import_legacy_global_json_to_db(DATA_FILE, force=True)
        data.clear()
        data.update(load_data())
        rebuild_global_records()
        save_data(data)
        export_global_csv(data)
        restore_mode = None
        return "🟢 Глобальный data.json обновлён"

    # JSON конкретного чата
    target_chat_id = payload.get("chat_id")
    if target_chat_id is None:
        target_chat_id = _extract_chat_id_from_json_filename(fname_l)
    if target_chat_id is None:
        raise RuntimeError("В JSON нет chat_id и его нельзя понять из имени файла")
    target_chat_id = int(target_chat_id)

    # Восстанавливаем именно тот чат, к которому относится файл, даже если файл прислан владельцу.
    restore_from_json(target_chat_id, tmp_path)
    day_key = get_chat_store(target_chat_id).get("current_view_day", today_key())
    finance_changed(target_chat_id, day_key, reason="owner_json_restore", delay=0.1)
    restore_mode = None
    return f"🟢 JSON чата обновлён: {get_chat_display_name(target_chat_id)}"


def _cleanup_owner_json_restore_prompt(key: int, remove_prompt: bool = False):
    try:
        with _owner_json_restore_prompt_lock:
            item = _owner_json_restore_prompts.pop(int(key), None)
        if not item:
            return
        if remove_prompt:
            try:
                bot.delete_message(int(OWNER_ID), int(item.get("prompt_msg_id")))
            except Exception:
                pass
        tmp_path = item.get("tmp_path")
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except Exception:
                pass
    except Exception as e:
        log_error(f"_cleanup_owner_json_restore_prompt({key}): {e}")


def _schedule_owner_json_restore_prompt_cleanup(key: int, delay: int = 12):
    def _job():
        _cleanup_owner_json_restore_prompt(key, remove_prompt=True)
    try:
        DELAYED_SCHEDULER.schedule(f"owner-json-restore-cleanup:{int(key)}", delay, _job)
    except Exception as e:
        log_error(f"_schedule_owner_json_restore_prompt_cleanup({key}): {e}")


def maybe_prompt_owner_for_json_restore(msg, fname: str) -> bool:
    """
    Если владелец прислал .json в личку без /restore — спрашиваем, обновлять данные или нет.
    Кнопки/окно удаляются через 10 секунд в любом случае.
    """
    try:
        if not is_owner_chat(msg.chat.id):
            return False
        if restore_mode is not None:
            return False
        if not str(fname or "").lower().endswith((".json", ".ison")):
            return False

        file_info = bot.get_file(msg.document.file_id)
        tmp_name = f"owner_json_restore_{int(msg.chat.id)}_{int(msg.message_id)}_{_safe_tmp_json_name(fname)}"
        stream_fn = globals().get("telegram_download_to_file")
        if callable(stream_fn):
            max_restore = max(1024 * 1024, int(os.getenv("RESTORE_FILE_MAX_BYTES", str(100 * 1024 * 1024)) or str(100 * 1024 * 1024)))
            stream_fn(file_info.file_path, tmp_name, max_bytes=max_restore)
        else:
            raw = bot.download_file(file_info.file_path)
            with open(tmp_name, "wb") as f:
                f.write(raw)
            raw = None

        payload = _load_json(tmp_name, None)
        if not isinstance(payload, dict):
            try:
                os.remove(tmp_name)
            except Exception:
                pass
            send_and_auto_delete(int(msg.chat.id), f"⚠️ JSON не прочитан или повреждён: {fname}", 10)
            return True

        desc, target_chat_id = _describe_json_restore_payload(payload, fname)
        key = int(msg.message_id)
        text = (
            "🧾 В чате владельца появился JSON-файл без /restore\n\n"
            f"Файл: {fname}\n"
            f"Что внутри: {desc}\n\n"
            "Обновить данные бота из этого JSON?"
        )
        kb = types.InlineKeyboardMarkup(row_width=2)
        kb.row(
            IB("✅ Да", callback_data=f"ojr:{key}:yes"),
            IB("❌ Нет", callback_data=f"ojr:{key}:no"),
        )
        sent = bot.send_message(int(msg.chat.id), text, reply_markup=kb)
        with _owner_json_restore_prompt_lock:
            _owner_json_restore_prompts[key] = {
                "tmp_path": tmp_name,
                "fname": fname,
                "prompt_msg_id": sent.message_id,
                "created_at": time.time(),
                "target_chat_id": target_chat_id,
            }
        delete_message_later(int(msg.chat.id), sent.message_id, 10)
        _schedule_owner_json_restore_prompt_cleanup(key, 12)
        return True
    except Exception as e:
        log_error(f"maybe_prompt_owner_for_json_restore({fname}): {e}")
        return False


def run_owner_json_restore_prompt_job(owner_chat_id: int, item: dict):
    tmp_path = item.get("tmp_path")
    fname = item.get("fname") or "backup.json"
    try:
        # Для глобального файла защищаем data_lock, для per-chat restore_from_json уже сохраняет данные.
        with data_lock:
            result = _apply_json_restore_from_owner_prompt(owner_chat_id, tmp_path, fname)
        send_and_auto_delete(owner_chat_id, result, 10)
    except Exception as e:
        send_and_auto_delete(owner_chat_id, f"❌ JSON не обновлён: {e}", 12)
    finally:
        try:
            if tmp_path and os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass
# v150_excel_reserve_chat_lifecycle
