# v147_multitenant_audit_restore
# v147 consolidated multi-tenant, permissions, audit, export/restore and UI extensions.
import gzip
import io
import sqlite3
import secrets
import ast

V147_SCHEMA_VERSION = 1
V147_PERMISSION_KEYS = [
    "finance", "ars", "usd", "gomonk", "finance_edit", "finance_delete", "reports", "month",
    "excel_chat", "google_sheets", "google_drive", "journals", "reminders", "reminder_create",
    "reminder_edit", "reminder_complete", "reminder_delete", "forward", "media_group",
    "child_chats", "users", "roles", "settings", "windows", "iphone_expense", "info",
    "diagnostics", "backup", "audit",
]


def _v147_root():
    root = data.setdefault("_global_settings", {}).setdefault("v147_multitenant", {})
    root.setdefault("tenants", {})
    root.setdefault("chat_index", {})
    root.setdefault("permissions", {})
    root.setdefault("chat_registry", {})
    root.setdefault("google_profiles", {})
    return root


def platform_owner_id():
    try:
        return int(OWNER_ID or 0)
    except Exception:
        return 0


def ensure_default_tenant():
    root = _v147_root()
    tid = "tenant_main"
    tenant = root["tenants"].setdefault(tid, {
        "tenant_id": tid,
        "name": "Основное пространство владельца",
        "owner_user_id": platform_owner_id(),
        "root_chat_id": platform_owner_id(),
        "chat_ids": [],
        "created_at": now_local().isoformat(timespec="seconds"),
    })
    for raw in list_chat_ids():
        try:
            cid = int(raw)
        except Exception:
            continue
        root["chat_index"].setdefault(str(cid), tid)
        if cid not in tenant["chat_ids"]:
            tenant["chat_ids"].append(cid)
    return tid


def tenant_for_chat(chat_id):
    root = _v147_root()
    key = str(int(chat_id))
    tid = root["chat_index"].get(key)
    if not tid:
        tid = ensure_default_tenant()
        root["chat_index"][key] = tid
        tenant = root["tenants"][tid]
        if int(chat_id) not in tenant["chat_ids"]:
            tenant["chat_ids"].append(int(chat_id))
    return tid


def tenant_record(tenant_id):
    ensure_default_tenant()
    return _v147_root()["tenants"].get(str(tenant_id)) or {}


def is_platform_owner_user(user_id):
    try:
        return int(user_id or 0) == platform_owner_id()
    except Exception:
        return False


def is_tenant_owner(user_id, tenant_id):
    try:
        return is_platform_owner_user(user_id) or int(tenant_record(tenant_id).get("owner_user_id") or 0) == int(user_id or 0)
    except Exception:
        return False


def chat_permission(chat_id, key, user_id=None):
    tid = tenant_for_chat(chat_id)
    row = _v147_root()["permissions"].get(f"{tid}:{int(chat_id)}", {})
    value = row.get("values", {}).get(str(key), row.get(str(key), "inherit"))
    return value is not False


def set_chat_permission(chat_id, key, value, user_id=None):
    tid = tenant_for_chat(chat_id)
    pkey = f"{tid}:{int(chat_id)}"
    row = _v147_root()["permissions"].setdefault(pkey, {"inherit": True, "revision": 0, "values": {}})
    row["values"][str(key)] = bool(value)
    row["revision"] = int(row.get("revision", 0)) + 1
    row["changed_by"] = int(user_id or 0)
    row["changed_at"] = now_local().isoformat(timespec="seconds")
    save_data(data, root_only=True)
    schedule_delta_backup(int(chat_id), delay=0.2, reason="chat_permissions")


def tenant_google_profile(tenant_id):
    root = _v147_root()["google_profiles"]
    return root.setdefault(str(tenant_id), {
        "spreadsheet_id": "",
        "drive_folder_id": "",
        "credential_source": "render_secret_env_or_reauth",
        "updated_at": "",
    })


def reminder_chat_settings(chat_id):
    settings = get_chat_store(int(chat_id)).setdefault("settings", {})
    settings.setdefault("reminder_merge_enabled", True)
    settings.setdefault("reminder_completion_command", False)
    return settings


def reminder_merge_enabled(chat_id):
    return bool(reminder_chat_settings(chat_id).get("reminder_merge_enabled", True))


def reminder_completion_command_enabled(chat_id):
    return bool(reminder_chat_settings(chat_id).get("reminder_completion_command", False))


def toggle_reminder_chat_setting(chat_id, key):
    settings = reminder_chat_settings(chat_id)
    settings[key] = not bool(settings.get(key))
    save_data(data, chat_ids=[int(chat_id)])
    schedule_delta_backup(int(chat_id), delay=0.2, reason="reminder_setting")
    return bool(settings[key])


_V147_ORIG_GROUP_MEMBERS = reminder_group_members

def reminder_group_members(target_chat_id, day_key=None, enabled_only=False):
    if not reminder_merge_enabled(int(target_chat_id)):
        return []
    return _V147_ORIG_GROUP_MEMBERS(target_chat_id, day_key, enabled_only)


_V147_ORIG_REM_LIST_KB = build_reminder_list_keyboard

def build_reminder_list_keyboard(day_key=None, page=0):
    kb = _V147_ORIG_REM_LIST_KB(day_key, page)
    cid = platform_owner_id()
    try:
        kb.row(IB(("✅" if reminder_merge_enabled(cid) else "❌") + " Объединять напоминания", callback_data="v147:rem_merge"))
        kb.row(IB(("✅" if reminder_completion_command_enabled(cid) else "❌") + " Показывать /vyapl", callback_data="v147:rem_vyapl"))
    except Exception:
        pass
    return kb


def _v147_show_chat_list(chat_id, message_id, user_id):
    kb = types.InlineKeyboardMarkup(row_width=1)
    for raw in sorted(list_chat_ids()):
        try:
            target = int(raw)
            tid = tenant_for_chat(target)
            if is_platform_owner_user(user_id) or is_tenant_owner(user_id, tid):
                kb.row(IB(chat_button_title(target), callback_data=f"v147:rights:{target}"))
        except Exception:
            pass
    kb.row(IB("⬅️ Назад", callback_data="nav_prev"))
    fast_ui_edit_message_text(chat_id, message_id, "🛡 ПРАВА ЧАТОВ\n\nВыберите чат:", reply_markup=kb, purpose="v147_rights")


def _v147_show_permissions(chat_id, message_id, target):
    kb = types.InlineKeyboardMarkup(row_width=1)
    labels = {
        "finance": "Финансовый режим", "ars": "ARS операции", "usd": "USD операции",
        "gomonk": "Гомонковые", "finance_edit": "Изменение операций", "finance_delete": "Удаление операций",
        "reports": "Просмотр итогов", "month": "За месяц", "excel_chat": "Excel в чат",
        "google_sheets": "Google Sheets", "google_drive": "Google Drive", "journals": "Скачать журналы",
        "reminders": "Напоминалка", "reminder_create": "Создание напоминаний",
        "reminder_edit": "Изменение напоминаний", "reminder_complete": "Выполнение /vyapl",
        "reminder_delete": "Удаление напоминаний", "forward": "Пересылка сообщений",
        "media_group": "Пересылка media group", "child_chats": "Подключение дочерних чатов",
        "users": "Управление пользователями", "roles": "Управление ролями",
        "settings": "Настройки чата", "windows": "Управление окнами", "iphone_expense": "Быстрый расход iPhone",
        "info": "Просмотр INFO", "diagnostics": "Диагностика", "backup": "Backup / recovery", "audit": "Аудит",
    }
    for key in V147_PERMISSION_KEYS:
        kb.row(IB(("✅ " if chat_permission(target, key) else "❌ ") + labels.get(key, key), callback_data=f"v147:perm:{target}:{key}"))
    kb.row(IB("⬅️ К чатам", callback_data="v147:rights"))
    text = f"🛡 ПРАВА ЧАТА\n\n💬 {get_chat_display_name(target)}\nID: {target}\nПространство: {tenant_for_chat(target)}"
    fast_ui_edit_message_text(chat_id, message_id, text, reply_markup=kb, purpose="v147_permissions")


@bot.callback_query_handler(func=lambda c: str(getattr(c, "data", "")).startswith("v147:"))
def v147_callback(call):
    chat_id = int(call.message.chat.id)
    user_id = int(getattr(call.from_user, "id", 0) or 0)
    raw = str(call.data)
    try:
        if raw == "v147:rem_merge":
            toggle_reminder_chat_setting(chat_id, "reminder_merge_enabled")
        elif raw == "v147:rem_vyapl":
            toggle_reminder_chat_setting(chat_id, "reminder_completion_command")
        elif raw == "v147:rights":
            _v147_show_chat_list(chat_id, call.message.message_id, user_id)
            bot.answer_callback_query(call.id)
            return
        elif raw.startswith("v147:rights:"):
            target = int(raw.rsplit(":", 1)[1])
            _v147_show_permissions(chat_id, call.message.message_id, target)
            bot.answer_callback_query(call.id)
            return
        elif raw.startswith("v147:perm:"):
            _, _, target, key = raw.split(":", 3)
            target = int(target)
            if not (is_platform_owner_user(user_id) or is_tenant_owner(user_id, tenant_for_chat(target))):
                raise PermissionError()
            set_chat_permission(target, key, not chat_permission(target, key, user_id), user_id)
            _v147_show_permissions(chat_id, call.message.message_id, target)
            bot.answer_callback_query(call.id, "Сохранено")
            return
        elif raw.startswith("v147:download_ready:"):
            bot.answer_callback_query(call.id, "Повторите команду скачивания")
            return
        try:
            fast_ui_edit_message_text(chat_id, call.message.message_id, build_reminder_list_text(), reply_markup=build_reminder_list_keyboard(today_key(), 0), purpose="v147_rem_settings")
        except Exception:
            pass
        bot.answer_callback_query(call.id, "Сохранено")
    except PermissionError:
        bot.answer_callback_query(call.id, "Недостаточно прав", show_alert=True)
    except Exception as exc:
        log_error(f"v147 callback: {exc}")
        bot.answer_callback_query(call.id, "Ошибка", show_alert=True)


@bot.message_handler(commands=["vyapl"])
def cmd_vyapl(msg):
    chat_id = int(msg.chat.id)
    user_id = int(getattr(msg.from_user, "id", 0) or 0)
    match = re.search(r"/vyapl_(\d+)", str(msg.text or ""))
    if not match:
        send_and_auto_delete(chat_id, "Выберите конкретную задачу командой /vyapl_ID.", 10)
        return
    reminder_id = int(match.group(1))
    cfg = _reminder_cfg(reminder_id)
    if not cfg or chat_id not in [int(x) for x in cfg.get("chat_ids") or []]:
        send_and_auto_delete(chat_id, "Задача не найдена в этом чате.", 10)
        return
    occurrence_id = str(cfg.get("next_run_at") or cfg.get("last_sent_at") or today_key())
    history = cfg.setdefault("completion_history", [])
    if any(str(x.get("occurrence_id")) == occurrence_id for x in history):
        send_and_auto_delete(chat_id, "ℹ️ Это срабатывание уже отмечено выполненным.", 10)
        return
    history.append({"occurrence_id": occurrence_id, "user_id": user_id, "completed_at": now_local().isoformat(timespec="seconds")})
    history[:] = history[-500:]
    _reminder_save("reminder_completed_command")
    bot_journal("reminder_completed", chat_id, f"reminder_id={reminder_id} occurrence={occurrence_id} user={user_id}")
    send_and_auto_delete(chat_id, f"✅ Задача №{reminder_id} выполнена.", 10)


@bot.message_handler(func=lambda m: str(getattr(m, "text", "") or "").startswith("/vyapl_"), content_types=["text"])
def cmd_vyapl_suffix(msg):
    return cmd_vyapl(msg)


def _v147_sqlite_path():
    for attr in ("db_path", "path", "filename"):
        value = getattr(SQLITE, attr, None)
        if value:
            return str(value)
    return str(globals().get("SQLITE_DB_PATH") or "bot_state.sqlite3")


def _v147_sqlite_backup(scope="global", tenant_id=None):
    work = tempfile.mkdtemp(prefix="v147_full_")
    db_out = os.path.join(work, "latest_bot_state.sqlite3")
    with sqlite3.connect(_v147_sqlite_path()) as source, sqlite3.connect(db_out) as dest:
        source.backup(dest)
    if scope == "tenant" and tenant_id:
        allowed = {str(x) for x in (tenant_record(tenant_id).get("chat_ids") or [])}
        con = sqlite3.connect(db_out)
        try:
            rows = [str(r[0]) for r in con.execute("SELECT chat_id FROM chats").fetchall()]
            for chat_key in rows:
                if chat_key not in allowed:
                    con.execute("DELETE FROM chats WHERE chat_id=?", (chat_key,))
                    con.execute("DELETE FROM cold_fields WHERE chat_id=?", (chat_key,))
            root_row = con.execute("SELECT v FROM kv WHERE k='root'").fetchone()
            if root_row:
                root_obj = json.loads(root_row[0])
                gs = root_obj.setdefault("_global_settings", {})
                mt = gs.setdefault("v147_multitenant", {})
                tenant = tenant_record(tenant_id)
                mt["tenants"] = {str(tenant_id): tenant}
                mt["chat_index"] = {str(cid): str(tenant_id) for cid in allowed}
                mt["permissions"] = {k:v for k,v in (mt.get("permissions") or {}).items() if k.startswith(str(tenant_id)+":")}
                mt["google_profiles"] = {str(tenant_id): (mt.get("google_profiles") or {}).get(str(tenant_id), {})}
                con.execute("UPDATE kv SET v=? WHERE k='root'", (json.dumps(root_obj, ensure_ascii=False, separators=(",", ":")),))
            con.commit()
        finally:
            con.close()
    raw = Path(db_out).read_bytes()
    manifest = {
        "kind": "telegram_bot_full_state", "schema_version": V147_SCHEMA_VERSION,
        "bot_version": VERSION, "created_at": now_local().isoformat(timespec="seconds"),
        "scope": scope, "tenant_id": tenant_id, "sqlite_sha256": hashlib.sha256(raw).hexdigest(),
    }
    gz_path = db_out + ".gz"
    with gzip.open(gz_path, "wb", compresslevel=6) as fh:
        fh.write(raw)
    Path(gz_path + ".manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return gz_path, manifest


@bot.message_handler(commands=["json_full"])
def cmd_json_full(msg):
    chat_id = int(msg.chat.id)
    user_id = int(getattr(msg.from_user, "id", 0) or 0)
    tid = tenant_for_chat(chat_id)
    if not (is_platform_owner_user(user_id) or is_tenant_owner(user_id, tid)):
        send_and_auto_delete(chat_id, "Команда доступна владельцу.", 10)
        return

    def job():
        scope = "global" if is_platform_owner_user(user_id) else "tenant"
        path, manifest = _v147_sqlite_backup(scope, None if scope == "global" else tid)
        payload = Path(path).read_bytes()
        with open(path, "rb") as fh:
            bot.send_document(chat_id, fh, caption=f"📦 Полное состояние · {scope}\nSHA-256: {hashlib.sha256(payload).hexdigest()[:16]}…", visible_file_name="latest_bot_state.sqlite3.gz")
        with open(path + ".manifest.json", "rb") as fh:
            bot.send_document(chat_id, fh, caption="Manifest восстановления", visible_file_name="latest_bot_state.manifest.json")
        return True

    ok, info = submit_interactive_file_job(chat_id, "json_full", "Полный файл состояния", job)
    if not ok:
        send_and_auto_delete(chat_id, f"⏳ Сейчас нельзя скачать: {info}", 15)


_RESTORE_PENDING = {}


@bot.message_handler(commands=["restore"])
def cmd_restore_v147(msg):
    chat_id = int(msg.chat.id)
    user_id = int(getattr(msg.from_user, "id", 0) or 0)
    tid = tenant_for_chat(chat_id)
    if not (is_platform_owner_user(user_id) or is_tenant_owner(user_id, tid)):
        send_and_auto_delete(chat_id, "Команда доступна владельцу.", 10)
        return
    reply = getattr(msg, "reply_to_message", None)
    doc = getattr(reply, "document", None) if reply else None
    if not doc or not str(getattr(doc, "file_name", "")).endswith(".sqlite3.gz"):
        send_and_auto_delete(chat_id, "Ответьте командой /restore на файл latest_bot_state.sqlite3.gz", 15)
        return
    token = secrets.token_urlsafe(8)
    _RESTORE_PENDING[token] = {"chat_id": chat_id, "user_id": user_id, "file_id": doc.file_id, "tenant_id": tid, "created": time.time()}
    kb = types.InlineKeyboardMarkup()
    kb.row(IB("✅ Подтвердить восстановление", callback_data=f"v147restore:{token}"), IB("❌ Отмена", callback_data="none"))
    bot.send_message(chat_id, "⚠️ Перед восстановлением будет создан аварийный backup. Подтвердите действие.", reply_markup=kb)


@bot.callback_query_handler(func=lambda c: str(getattr(c, "data", "")).startswith("v147restore:"))
def cb_restore_v147(call):
    token = str(call.data).split(":", 1)[1]
    req = _RESTORE_PENDING.pop(token, None)
    chat_id = int(call.message.chat.id)
    user_id = int(getattr(call.from_user, "id", 0) or 0)
    if not req or req["user_id"] != user_id or time.time() - req["created"] > 900:
        bot.answer_callback_query(call.id, "Запрос истёк", show_alert=True)
        return
    try:
        fi = bot.get_file(req["file_id"])
        raw = bot.download_file(fi.file_path)
        if len(raw) > 250 * 1024 * 1024:
            raise ValueError("Файл слишком большой")
        unpacked = gzip.decompress(raw)
        if len(unpacked) > 1024 * 1024 * 1024:
            raise ValueError("Распакованный файл слишком большой")
        work = tempfile.mkdtemp(prefix="v147_restore_")
        candidate = os.path.join(work, "candidate.sqlite3")
        Path(candidate).write_bytes(unpacked)
        con = sqlite3.connect(candidate)
        integrity = con.execute("PRAGMA integrity_check").fetchone()[0]
        con.close()
        if integrity != "ok":
            raise ValueError("SQLite integrity_check: " + str(integrity))
        _v147_sqlite_backup("global", None)
        if is_platform_owner_user(user_id):
            replace = getattr(SQLITE, "replace_database", None)
            if not callable(replace):
                raise RuntimeError("Runtime не поддерживает атомарную замену SQLite")
            replace(candidate)
        else:
            allowed = {str(x) for x in (tenant_record(req["tenant_id"]).get("chat_ids") or [])}
            src_con = sqlite3.connect(candidate)
            try:
                chat_rows = src_con.execute("SELECT chat_id,v FROM chats").fetchall()
                cold_rows = src_con.execute("SELECT chat_id,k,v,updated_at FROM cold_fields").fetchall()
            finally:
                src_con.close()
            with SQLITE.lock:
                for chat_key, value in chat_rows:
                    if str(chat_key) in allowed:
                        SQLITE.conn.execute("INSERT INTO chats(chat_id,v) VALUES(?,?) ON CONFLICT(chat_id) DO UPDATE SET v=excluded.v", (str(chat_key), value))
                for chat_key, key, value, updated_at in cold_rows:
                    if str(chat_key) in allowed:
                        SQLITE.conn.execute("INSERT INTO cold_fields(chat_id,k,v,updated_at) VALUES(?,?,?,?) ON CONFLICT(chat_id,k) DO UPDATE SET v=excluded.v,updated_at=excluded.updated_at", (str(chat_key), str(key), value, str(updated_at or "")))
                SQLITE.conn.commit()
        bot_journal("manual_full_restore", chat_id, f"user={user_id} sha256={hashlib.sha256(raw).hexdigest()}")
        bot.edit_message_text("✅ Восстановление завершено. Кэши и таймеры будут перечитаны.", chat_id, call.message.message_id)
        bot.answer_callback_query(call.id, "Готово")
    except Exception as exc:
        log_error(f"v147 restore: {exc}")
        bot.answer_callback_query(call.id, "Восстановление отклонено", show_alert=True)
        bot.edit_message_text(f"⚠️ Восстановление не выполнено: {str(exc)[:300]}", chat_id, call.message.message_id)


def v147_button_slash_audit():
    root = Path(__file__).resolve().parent if "__file__" in globals() else Path(".")
    callbacks, commands, handlers = [], [], []
    for path in root.glob("*.py"):
        if path.name.startswith("FULL_"):
            continue
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                for kw in node.keywords:
                    if kw.arg == "callback_data" and isinstance(kw.value, ast.Constant):
                        callbacks.append((path.name, node.lineno, str(kw.value.value)))
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                for dec in node.decorator_list:
                    text = ast.unparse(dec) if hasattr(ast, "unparse") else ""
                    if "message_handler" in text and "commands" in text:
                        commands.append((path.name, node.lineno, text))
                    if "callback_query_handler" in text:
                        handlers.append((path.name, node.lineno, text))
    duplicates = sorted({x[2] for x in callbacks if sum(1 for y in callbacks if y[2] == x[2]) > 1})
    return {"callbacks": len(callbacks), "slash_handlers": len(commands), "callback_handlers": len(handlers), "duplicate_literal_callbacks": duplicates}


@bot.message_handler(commands=["audit_v146", "audit_buttons"])
def cmd_v147_audit(msg):
    chat_id = int(msg.chat.id)
    user_id = int(getattr(msg.from_user, "id", 0) or 0)
    if not is_platform_owner_user(user_id):
        return
    report = {
        "generated_at": now_local().isoformat(timespec="seconds"), "version": VERSION,
        "button_slash": v147_button_slash_audit(),
        "window": window_diagnostic_stats() if "window_diagnostic_stats" in globals() else {},
        "file_job": _file_job_busy_info(), "chat_registry": _v147_root().get("chat_registry", {}),
    }
    bio = io.BytesIO(json.dumps(report, ensure_ascii=False, indent=2).encode())
    bio.name = "Аудит_бота_v147.json"
    bot.send_document(chat_id, bio, caption="🔎 Аудит кнопок, команд, окон и чатов")


_V147_ORIG_UPDATE_CHAT = globals().get("update_chat_info_from_message")

def update_chat_info_from_message(msg):
    result = _V147_ORIG_UPDATE_CHAT(msg) if callable(_V147_ORIG_UPDATE_CHAT) else None
    try:
        cid = int(msg.chat.id)
        row = _v147_root()["chat_registry"].setdefault(str(cid), {"chat_id": cid, "first_seen": now_local().isoformat(timespec="seconds")})
        row.update({
            "title": getattr(msg.chat, "title", None) or getattr(msg.chat, "first_name", None) or row.get("title") or str(cid),
            "status": "active", "last_seen": now_local().isoformat(timespec="seconds"), "tenant_id": tenant_for_chat(cid),
        })
    except Exception:
        pass
    return result


_V147_ORIG_SUBMIT_FILE = submit_interactive_file_job

def submit_interactive_file_job(chat_id, kind, label, func, *args, **kwargs):
    ok, info = _V147_ORIG_SUBMIT_FILE(chat_id, kind, label, func, *args, **kwargs)
    if not ok:
        try:
            busy = _file_job_busy_info()
            current = str(busy.get("label") or "другой файл")
            msg = bot.send_message(int(chat_id), f"⏳ Сейчас нельзя скачать «{label}»: уже формируется «{current}».\nСообщение обновится после завершения.")

            def ready():
                if _file_job_busy_info():
                    DELAYED_SCHEDULER.schedule(f"v147-file-ready:{chat_id}:{kind}", 5.0, ready)
                    return
                kb = types.InlineKeyboardMarkup()
                kb.row(IB("📥 Скачать сейчас", callback_data=f"v147:download_ready:{kind}"), IB("❌ Закрыть", callback_data="close_window"))
                try:
                    bot.edit_message_text(f"✅ Теперь можно скачать «{label}».", int(chat_id), msg.message_id, reply_markup=kb)
                except Exception:
                    pass

            DELAYED_SCHEDULER.schedule(f"v147-file-ready:{chat_id}:{kind}", 5.0, ready)
        except Exception:
            pass
    return ok, info


_V147_SIMPLE_BAL = _xlsx_simple_rows_with_balances

def _xlsx_simple_rows_with_balances(rows, opening_balance):
    out = _V147_SIMPLE_BAL(rows, opening_balance)
    try:
        cid = int(_file_job_current().get("chat_id") or platform_owner_id())
        currency = "usd" if usd_transactions_view_enabled(cid) else "ars"
        reserve = gomonk_total(cid, currency)
        closing = 0.0
        for row in reversed(out):
            if len(row) > 2 and str(row[1] if len(row) > 1 else "") == "Остаток на руках":
                closing = float((row[2] or {}).get("value", 0) if isinstance(row[2], dict) else row[2] or 0)
                break
        out.append(["", "Гомонковые", reserve, ""])
        out.append(["", "Остаток в обороте", closing - reserve, ""])
    except Exception:
        pass
    return out


ensure_default_tenant()


# f206: keep the mode toggle inside the menu and add chat rights.
_V147_ORIG_SAFETY_KB = build_safety_profile_keyboard
def build_safety_profile_keyboard(chat_id):
    kb = _V147_ORIG_SAFETY_KB(chat_id)
    try:
        kb.row(IB("🛡 Права чатов", callback_data="v147:rights"))
    except Exception:
        pass
    return kb

# f191 text: explicitly show every target chat.
_V147_ORIG_REM_MENU_TEXT = build_reminder_menu_text
def build_reminder_menu_text(reminder_id):
    text = _V147_ORIG_REM_MENU_TEXT(reminder_id)
    cfg = _reminder_cfg(reminder_id) or {}
    names = []
    for raw in cfg.get("chat_ids") or []:
        try:
            cid = int(raw); names.append(f"💬 Чат: {get_chat_display_name(cid)}")
        except Exception:
            names.append(f"💬 Чат: {raw}")
    if names and "💬 Чат:" not in text:
        text += "\n\n" + "\n".join(names)
    return text

# Human-readable journal/download names.
def v147_journal_filename(kind, chat_id, ext="txt", period=None):
    title = get_chat_display_name(int(chat_id)) if chat_id else "Весь_бот"
    safe = re.sub(r"[^0-9A-Za-zА-Яа-яЁё._-]+", "-", str(title)).strip("-")[:60] or "Чат"
    date = str(period or today_key()).replace("/", "-")
    label = re.sub(r"[^0-9A-Za-zА-Яа-яЁё._-]+", "-", str(kind or "операций")).strip("-")
    return f"Журнал_{label}_{safe}_{date}.{str(ext).lstrip('.')}"

# Tenant-scoped Google settings are resolved before global environment fallback.
_V147_ORIG_GOOGLE_SHEET_ID = globals().get("_google_spreadsheet_id")
if callable(_V147_ORIG_GOOGLE_SHEET_ID):
    def _google_spreadsheet_id(value=None):
        if value:
            return _V147_ORIG_GOOGLE_SHEET_ID(value)
        ctx = _file_job_current()
        cid = int(ctx.get("chat_id") or platform_owner_id())
        profile = tenant_google_profile(tenant_for_chat(cid))
        configured = str(profile.get("spreadsheet_id") or "").strip()
        return _V147_ORIG_GOOGLE_SHEET_ID(configured or None)



# Reserve reconciliation is idempotent: reserve may not exceed cash on hand.
def reconcile_gomonk_reserve(chat_id):
    chat_id = int(chat_id)
    store = get_chat_store(chat_id)
    try:
        _snapshot_active_currency_ledger(store, _ensure_currency_ledgers(store))
    except Exception:
        pass
    changed = False
    for currency in ("ars", "usd"):
        entries = gomonk_entries(chat_id, currency)
        total = sum(float(x.get("amount", 0) or 0) for x in entries)
        try:
            balance = float(store.get(f"{currency}_balance", store.get("balance", 0) if currency == "ars" else 0) or 0)
        except Exception:
            balance = 0.0
        target = max(0.0, balance)
        if total <= target + 1e-9 or total <= 0:
            continue
        factor = target / total if total else 0.0
        updated = []
        for item in entries:
            amount = round(float(item.get("amount", 0) or 0) * factor, 6)
            if amount > 0:
                updated.append({"name": str(item.get("name") or "Сумма")[:80], "amount": amount})
        set_gomonk_entries(chat_id, updated, currency)
        bot_journal("gomonk_reserve_used", chat_id, f"currency={currency} before={total} after={target} deficit={total-target}")
        changed = True
    return changed

_V147_ORIG_FINANCE_CHANGED = finance_changed
def finance_changed(chat_id, day_key=None, reason="change", delay=0.35):
    try:
        reconcile_gomonk_reserve(int(chat_id))
    except Exception as exc:
        log_error(f"gomonk reconcile {chat_id}: {exc}")
    return _V147_ORIG_FINANCE_CHANGED(chat_id, day_key, reason, delay)

# Extended category Excel: reserve, turnover, food/person/day and a USD table below ARS.
_V147_CATEGORY_ROWS = build_exact_category_stats_xlsx_rows
def build_exact_category_stats_xlsx_rows(target_chat_id, start_key, start_rid, end_key, end_rid):
    rows = _V147_CATEGORY_ROWS(target_chat_id, start_key, start_rid, end_key, end_rid)
    store = get_chat_store(int(target_chat_id))
    current_currency = "usd" if financial_view_is_usd(store) else "ars"
    reserve = gomonk_total(int(target_chat_id), current_currency)
    closing = 0.0
    for row in reversed(rows):
        if len(row) > 2 and str(row[1] if len(row) > 1 else "") == "Остаток на руках":
            cell = row[2]
            closing = float(cell.get("value", 0) if isinstance(cell, dict) else cell or 0)
            break
    width = max((len(x) for x in rows if isinstance(x, list)), default=4)
    rows.append(["", "Гомонковые", reserve] + [""] * max(0, width - 3))
    rows.append(["", "Остаток в обороте", closing - reserve] + [""] * max(0, width - 3))
    try:
        header = rows[0]
        product_idx = next((i for i, value in enumerate(header) if "продукт" in str(value).casefold()), None)
        product_total = 0.0
        if product_idx is not None:
            for row in rows:
                if len(row) > product_idx and str(row[1] if len(row) > 1 else "") == "Сумма по статьям":
                    cell = row[product_idx]
                    product_total = float(cell.get("value", 0) if isinstance(cell, dict) else cell or 0)
                    break
        start_dt = datetime.strptime(str(start_key)[:10], "%Y-%m-%d")
        end_dt = datetime.strptime(str(end_key)[:10], "%Y-%m-%d")
        days = max(1, (end_dt.date() - start_dt.date()).days + 1)
        rate_info = usd_rate_cached(False) or {}
        rate = float(rate_info.get("rate") or 0)
        per_person_day = (product_total / 5.0 / days / rate) if rate > 0 else 0.0
        rows.append([])
        rows.append(["", "Расход еды на человека в сутки, USD", per_person_day] + [""] * max(0, width - 3))
    except Exception:
        pass
    if current_currency == "ars":
        try:
            records = sorted(store.get("usd_records") or [], key=record_sort_key)
            filtered = [r for r in records if str(start_key)[:10] <= _record_day_key(r) <= str(end_key)[:10]]
            rows.extend([[], ["USD ОПЕРАЦИИ"], ["Дата", "Описание", "Приход USD", "Расход USD"]])
            income = expense = 0.0
            for rec in filtered:
                amount = float(rec.get("amount", rec.get("usd_amount", 0)) or 0)
                note = str(rec.get("note") or rec.get("usd_note") or "")
                if amount >= 0:
                    income += amount; rows.append([fmt_date_table(_record_day_key(rec)), note, amount, ""])
                else:
                    expense += abs(amount); rows.append([fmt_date_table(_record_day_key(rec)), note, "", abs(amount)])
            usd_balance = float(store.get("usd_balance", 0) or 0)
            usd_reserve = gomonk_total(int(target_chat_id), "usd")
            rows.extend([[], ["", "Приход USD", income, ""], ["", "Расход USD", "", expense], ["", "Остаток на руках USD", usd_balance, ""], ["", "Гомонковые USD", usd_reserve, ""], ["", "Остаток в обороте USD", usd_balance-usd_reserve, ""]])
        except Exception as exc:
            log_error(f"USD table append {target_chat_id}: {exc}")
    return rows

# v147_multitenant_audit_restore
