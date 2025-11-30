# ==========================================================
# 🧭 Description: Code_Universal_Finance_Bot
# ==========================================================
# • Единый универсальный финансовый бот (без OWNER_ID)
# • Поддержка пересылки между чатами (всем разрешено)
# • Авто-бэкап в чат и в канал
# • Webhook (Render) + Flask-сервер
# • Не засыпает: периодический авто-пинг
# ==========================================================

import os
import io
import re
import csv
import time
import json
import threading
from datetime import datetime, timedelta
from flask import Flask, request
import telebot
from telebot import types
from telebot.types import InputMediaPhoto, InputMediaVideo, InputMediaDocument, InputMediaAudio

# ==========================================================
# SECTION 1 — Настройки окружения
# ==========================================================

# Токен Telegram-бота и параметры вебхука
BOT_TOKEN = os.getenv("BOT_TOKEN")
WEBHOOK_HOST = os.getenv("RENDER_EXTERNAL_URL", "https://example.onrender.com").rstrip("/")
WEBHOOK_PATH = f"/{BOT_TOKEN}"
WEBHOOK_URL = f"{WEBHOOK_HOST}{WEBHOOK_PATH}"

# ID канала/чата для резервных копий (backup)
BACKUP_CHAT_ID = os.getenv("BACKUP_CHAT_ID", "").strip()

# Имя основных файлов
DATA_FILE = "data.json"
CSV_FILE = "data.csv"
CSV_META_FILE = "csv_meta.json"
CHAT_BACKUP_META_FILE = "chat_backup_meta.json"

# ==========================================================
# SECTION 2 — Инициализация бота и Flask-сервера
# ==========================================================

bot = telebot.TeleBot(BOT_TOKEN)
app = Flask(__name__)

# Флаг режима восстановления (true, если ждём файлы)
restore_mode = False

# Память активных чатов
finance_active_chats = set()

# Служебные флаги резервирования
backup_flags = {
    "drive": True,
    "channel": True
}

# ==========================================================
# SECTION 3 — Загрузка/сохранение data.json
# ==========================================================

def _load_json(path: str, default=None):
    """Безопасная загрузка JSON."""
    if not os.path.exists(path):
        return default
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def _save_json(path: str, obj: dict):
    """Безопасное сохранение JSON."""
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def load_data() -> dict:
    """Загружает основную структуру данных."""
    d = _load_json(DATA_FILE, None)
    if d is None:
        d = default_data()
        _save_json(DATA_FILE, d)
    return d

def save_data(obj: dict):
    """Сохраняет основную структуру данных."""
    try:
        _save_json(DATA_FILE, obj)
    except Exception as e:
        log_error(f"save_data: {e}")

def default_data():
    """Создание структуры по умолчанию."""
    return {
        "overall_balance": 0,
        "records": [],
        "chats": {},
        "active_messages": {},
        "next_id": 1,
        "backup_flags": {"drive": True, "channel": True},
        "finance_active_chats": {},
        "forward_rules": {},
    }

data = load_data()

# ==========================================================
# SECTION 4 — Вспомогательные утилиты
# ==========================================================

def now_local():
    return datetime.now()

def today_key():
    return datetime.now().strftime("%Y-%m-%d")

def fmt_num(n: int | float) -> str:
    try:
        return f"{n:,}".replace(",", " ")
    except Exception:
        return str(n)

def log_info(msg: str):
    print(f"[INFO] {msg}")

def log_error(msg: str):
    print(f"[ERROR] {msg}")

# ==========================================================
# SECTION 5 — Google Drive (сжато)
# ==========================================================

def upload_to_gdrive(path: str):
    """Заглушка Google Drive (сжато в одну строку)."""
    try: log_info(f"[GDRIVE] upload {path}") if backup_flags.get("drive") else None
    except Exception as e: log_error(f"upload_to_gdrive: {e}")

# ==========================================================
# SECTION 6 — Формирование имён файлов
# ==========================================================

def chat_json_file(chat_id: int) -> str:
    return f"data_{chat_id}.json"

def chat_csv_file(chat_id: int) -> str:
    return f"data_{chat_id}.csv"

# ==========================================================
# SECTION 7 — Работа с чатами (store)
# ==========================================================

def get_chat_store(chat_id: int) -> dict:
    """Возвращает или создаёт хранилище данных для чата."""
    chats = data.setdefault("chats", {})
    store = chats.setdefault(chat_id, {
        "records": [],
        "daily_records": {},
        "next_id": 1,
        "balance": 0,
        "info": {},
        "known_chats": {},
        "settings": {"auto_add": False},
        "current_view_day": today_key(),
        "edit_wait": None,
        "reset_wait": False,
        "reset_time": 0,
        "total_msg_id": None,
    })
    return store

# ==========================================================
# SECTION 8 — Форматирование, emoji, имена
# ==========================================================

EMOJI_DIGITS = {"0":"0️⃣","1":"1️⃣","2":"2️⃣","3":"3️⃣","4":"4️⃣","5":"5️⃣","6":"6️⃣","7":"7️⃣","8":"8️⃣","9":"9️⃣"}
backup_channel_notified_chats = set()

def format_chat_id_emoji(chat_id: int) -> str:
    """Возвращает chat_id в виде emoji-цифр."""
    return "".join(EMOJI_DIGITS.get(ch, ch) for ch in str(chat_id))

def _safe_chat_title_for_filename(title) -> str:
    """Создаёт безопасное имя файла из названия чата."""
    if not title: return ""
    title = re.sub(r"[^0-9A-Za-zА-Яа-я_\-]+", "", title.replace(" ", "_"))
    return title[:32]

def _get_chat_title_for_backup(chat_id: int) -> str:
    """Возвращает сохранённое имя чата для backup."""
    try:
        store = get_chat_store(chat_id)
        info = store.get("info", {})
        title = info.get("title")
        if title: return title
    except Exception as e:
        log_error(f"_get_chat_title_for_backup({chat_id}): {e}")
    return f"chat_{chat_id}"

# ==========================================================
# SECTION 9 — Backup в канал (универсальный)
# ==========================================================

def send_backup_to_channel_for_file(base_path: str, meta_key_prefix: str, chat_title: str = None):
    """Отправляет или обновляет файл в BACKUP_CHAT_ID."""
    if not BACKUP_CHAT_ID or not os.path.exists(base_path): return
    try:
        meta = _load_json(CSV_META_FILE, {})
        msg_key = f"msg_{meta_key_prefix}"
        ts_key = f"timestamp_{meta_key_prefix}"
        base_name = os.path.basename(base_path)
        name, dot, ext = base_name.partition(".")
        safe_title = _safe_chat_title_for_filename(chat_title)
        fname = f"{name}_{safe_title}.{ext}" if safe_title else base_name
        caption = f"📦 {fname} — {now_local().strftime('%Y-%m-%d %H:%M')}"
        def _open(): 
            with open(base_path,"rb") as s: b=s.read()
            if not b: return None
            buf=io.BytesIO(b); buf.name=fname; buf.seek(0); return buf
        fobj=_open()
        if not fobj: return
        try:
            if msg_key in meta:
                bot.edit_message_media(int(BACKUP_CHAT_ID),meta[msg_key],telebot.types.InputMediaDocument(fobj,caption=caption))
            else:
                sent=bot.send_document(int(BACKUP_CHAT_ID),fobj,caption=caption)
                meta[msg_key]=sent.message_id
        except Exception as e:
            log_error(f"edit/send {base_name}: {e}")
            sent=bot.send_document(int(BACKUP_CHAT_ID),fobj,caption=caption)
            meta[msg_key]=sent.message_id
        meta[ts_key]=now_local().isoformat(timespec="seconds")
        _save_json(CSV_META_FILE,meta)
    except Exception as e:
        log_error(f"send_backup_to_channel_for_file({base_path}): {e}")

def send_backup_to_channel(chat_id:int):
    """Универсальный бэкап в канал."""
    if not BACKUP_CHAT_ID: return
    try:
        save_chat_json(chat_id)
        chat_title=_get_chat_title_for_backup(chat_id)
        if chat_id not in backup_channel_notified_chats:
            try:
                bot.send_message(int(BACKUP_CHAT_ID),format_chat_id_emoji(chat_id))
                backup_channel_notified_chats.add(chat_id)
            except Exception as e: log_error(f"emoji_id: {e}")
        send_backup_to_channel_for_file(chat_json_file(chat_id),f"json_{chat_id}",chat_title)
        send_backup_to_channel_for_file(chat_csv_file(chat_id),f"csv_{chat_id}",chat_title)
        send_backup_to_channel_for_file(DATA_FILE,"global_data","ALL_CHATS")
        send_backup_to_channel_for_file(CSV_FILE,"global_csv","ALL_CHATS")
    except Exception as e: log_error(f"send_backup_to_channel({chat_id}): {e}")

# ==========================================================
# SECTION 10 — Flask webhook и keep-alive
# ==========================================================

@app.route(WEBHOOK_PATH, methods=["POST"])
def webhook():
    """Приём входящих обновлений от Telegram."""
    if request.headers.get("content-type") == "application/json":
        update = request.get_data().decode("utf-8")
        update = json.loads(update)
        bot.process_new_updates([telebot.types.Update.de_json(update)])
        return "OK", 200
    return "Unsupported", 403

@app.route("/", methods=["GET"])
def index():
    """Главная страница — авто-пинг для Render."""
    return "✅ Бот работает (Kena Olive)"

def set_webhook():
    """Устанавливает webhook при запуске."""
    bot.remove_webhook()
    time.sleep(1)
    bot.set_webhook(url=WEBHOOK_URL)
    log_info(f"Webhook установлен: {WEBHOOK_URL}")

# keep-alive поток
def keep_alive():
    """Периодически пингует самого себя, чтобы Render не уснул."""
    import requests
    def _loop():
        while True:
            try:
                requests.get(WEBHOOK_HOST)
                time.sleep(300)
            except Exception:
                time.sleep(300)
    threading.Thread(target=_loop, daemon=True).start()
    # ==========================================================
# SECTION 11 — Подготовка после запуска (webhook + keepalive)
# ==========================================================

def startup():
    """Запуск вебхука и keep-alive."""
    try:
        set_webhook()
    except Exception as e:
        log_error(f"Ошибка установки webhook: {e}")
    keep_alive()
    log_info("Бот и сервер успешно запущены.")


# ==========================================================
# SECTION 12 — ПОМОЩНИКИ ДЛЯ БЕКАПА В ЧАТ
# ==========================================================

def _load_chat_backup_meta() -> dict:
    """Загружает метаданные для резервных копий в чаты."""
    try:
        return _load_json(CHAT_BACKUP_META_FILE, {})
    except Exception as e:
        log_error(f"_load_chat_backup_meta: {e}")
        return {}

def _save_chat_backup_meta(meta: dict):
    """Сохраняет метаданные резервных копий."""
    try:
        _save_json(CHAT_BACKUP_META_FILE, meta)
    except Exception as e:
        log_error(f"_save_chat_backup_meta: {e}")


def send_backup_to_chat(chat_id: int):
    """
    Создаёт или обновляет резервную копию data_<chat>.json в том же чате.
    Работает одинаково для всех чатов.
    """
    try:
        save_chat_json(chat_id)
        meta = _load_chat_backup_meta()

        msg_key = f"msg_chat_{chat_id}"
        ts_key = f"timestamp_chat_{chat_id}"

        json_path = chat_json_file(chat_id)
        if not os.path.exists(json_path):
            return

        title = _get_chat_title_for_backup(chat_id)
        safe_title = _safe_chat_title_for_filename(title)
        file_name = f"data_{safe_title or chat_id}.json"
        caption = f"📥 {file_name} — {now_local().strftime('%d.%m.%y %H:%M')}"

        with open(json_path, "rb") as f:
            data_bytes = f.read()
        if not data_bytes:
            return

        fobj = io.BytesIO(data_bytes)
        fobj.name = file_name
        fobj.seek(0)

        if msg_key in meta:
            try:
                bot.edit_message_media(
                    chat_id=chat_id,
                    message_id=meta[msg_key],
                    media=InputMediaDocument(fobj, caption=caption)
                )
            except Exception as e:
                log_error(f"edit_message_media chat backup: {e}")
                sent = bot.send_document(chat_id, fobj, caption=caption)
                meta[msg_key] = sent.message_id
        else:
            sent = bot.send_document(chat_id, fobj, caption=caption)
            meta[msg_key] = sent.message_id

        meta[ts_key] = now_local().isoformat(timespec="seconds")
        _save_chat_backup_meta(meta)

    except Exception as e:
        log_error(f"send_backup_to_chat({chat_id}): {e}")


# ==========================================================
# SECTION 13 — Forward (пересылка между чатами, доступно всем)
# ==========================================================

def resolve_forward_targets(source_chat_id: int):
    """Возвращает список (target_chat_id, mode)."""
    fr = data.get("forward_rules", {})
    src = str(source_chat_id)
    if src not in fr:
        return []
    out = []
    for dst, mode in fr[src].items():
        try:
            out.append((int(dst), mode))
        except:
            pass
    return out


def add_forward_link(src_chat: int, dst_chat: int, mode: str):
    """Создаёт/обновляет направление пересылки."""
    fr = data.setdefault("forward_rules", {})
    fr.setdefault(str(src_chat), {})[str(dst_chat)] = mode
    save_data(data)


def remove_forward_link(src_chat: int, dst_chat: int):
    """Удаляет направление пересылки."""
    fr = data.get("forward_rules", {})
    s, d = str(src_chat), str(dst_chat)
    if s in fr and d in fr[s]:
        del fr[s][d]
    if s in fr and not fr[s]:
        del fr[s]
    save_data(data)


def clear_forward_links_between(a: int, b: int):
    """Удаляет пересылку в обе стороны."""
    remove_forward_link(a, b)
    remove_forward_link(b, a)


# ---------------------------- Анонимная пересылка текста ----------------------------

def forward_text_anon(source_chat_id: int, msg, targets: list[tuple[int, str]]):
    """Анонимно пересылает текст."""
    for dst, mode in targets:
        try:
            bot.copy_message(dst, source_chat_id, msg.message_id)
        except Exception as e:
            log_error(f"forward_text_anon to {dst}: {e}")


# ---------------------------- Анонимная пересылка медиа ----------------------------

def forward_media_anon(source_chat_id: int, msg, targets):
    """Пересылает медиа (фото, видео, документ...)."""
    for dst, mode in targets:
        try:
            bot.copy_message(dst, source_chat_id, msg.message_id)
        except Exception as e:
            log_error(f"forward_media_anon to {dst}: {e}")


# ---------------------------- Альбомы (media groups) ----------------------------

_media_group_cache = {}

def collect_media_group(chat_id: int, msg):
    """Собирает сообщения альбома."""
    gid = msg.media_group_id
    if not gid:
        return [msg]

    group = _media_group_cache.setdefault(chat_id, {})
    arr = group.setdefault(gid, [])
    arr.append(msg)

    if len(arr) == 1:
        time.sleep(0.2)

    complete = group.pop(gid, arr)
    return complete


def forward_media_group_anon(source_chat_id: int, messages: list, targets):
    """Пересылка альбома анонимно."""
    if not messages:
        return

    media_list = []
    for msg in messages:
        caption = msg.caption or None

        if msg.content_type == "photo":
            media_list.append(InputMediaPhoto(msg.photo[-1].file_id, caption=caption))
        elif msg.content_type == "video":
            media_list.append(InputMediaVideo(msg.video.file_id, caption=caption))
        elif msg.content_type == "document":
            media_list.append(InputMediaDocument(msg.document.file_id, caption=caption))
        elif msg.content_type == "audio":
            media_list.append(InputMediaAudio(msg.audio.file_id, caption=caption))
        else:
            for dst, mode in targets:
                try:
                    bot.copy_message(dst, source_chat_id, msg.message_id)
                except:
                    pass
            return

    for dst, mode in targets:
        try:
            bot.send_media_group(dst, media_list)
        except Exception as e:
            log_error(f"forward_media_group_anon to {dst}: {e}")


# ==========================================================
# SECTION 14 — Меню пересылки (показывается всем чатом)
# ==========================================================

def build_forward_source_menu(chat_id: int):
    """Меню выбора источника A."""
    kb = types.InlineKeyboardMarkup()
    store = get_chat_store(chat_id)
    known = store.get("known_chats", {})

    if not known:
        kb.row(types.InlineKeyboardButton("Нет известных чатов", callback_data="fw_dummy"))
        return kb

    for cid, info in known.items():
        title = info.get("title") or f"Чат {cid}"
        kb.row(types.InlineKeyboardButton(title, callback_data=f"fw_src:{cid}"))

    kb.row(types.InlineKeyboardButton("🔙 Назад", callback_data="fw_back_root"))
    return kb


def build_forward_target_menu(chat_id: int, src_id: int):
    """Меню выбора получателя B."""
    kb = types.InlineKeyboardMarkup()
    store = get_chat_store(chat_id)
    known = store.get("known_chats", {})

    for cid, info in known.items():
        if int(cid) == src_id:
            continue
        title = info.get("title") or f"Чат {cid}"
        kb.row(types.InlineKeyboardButton(title, callback_data=f"fw_tgt:{src_id}:{cid}"))

    kb.row(types.InlineKeyboardButton("🔙 Назад", callback_data="fw_back_src"))
    return kb


def build_forward_mode_menu(chat_id: int, A: int, B: int):
    """Меню выбора режима пересылки."""
    kb = types.InlineKeyboardMarkup()

    kb.row(types.InlineKeyboardButton(f"➡️ {A} → {B}", callback_data=f"fw_mode:{A}:{B}:to"))
    kb.row(types.InlineKeyboardButton(f"⬅️ {B} → {A}", callback_data=f"fw_mode:{A}:{B}:from"))
    kb.row(types.InlineKeyboardButton(f"↔️ {A} ⇄ {B}", callback_data=f"fw_mode:{A}:{B}:two"))

    kb.row(types.InlineKeyboardButton("❌ Удалить связь", callback_data=f"fw_mode:{A}:{B}:del"))
    kb.row(types.InlineKeyboardButton("🔙 Назад", callback_data=f"fw_back_tgt:{A}"))

    return kb


def apply_forward_mode(A: int, B: int, mode: str):
    """Применяет выбранный режим."""
    if mode == "to":
        add_forward_link(A, B, "oneway_to")
        remove_forward_link(B, A)

    elif mode == "from":
        add_forward_link(B, A, "oneway_to")
        remove_forward_link(A, B)

    elif mode == "two":
        add_forward_link(A, B, "twoway")
        add_forward_link(B, A, "twoway")

    elif mode == "del":
        clear_forward_links_between(A, B)


def build_forward_root(chat_id: int, day_key: str):
    """Корневое меню пересылки."""
    kb = types.InlineKeyboardMarkup()
    kb.row(types.InlineKeyboardButton("🔀 Пары A ↔ B", callback_data="fw_open"))
    kb.row(types.InlineKeyboardButton("🔙 Назад", callback_data=f"d:{day_key}:edit_menu"))
    return kb


# ==========================================================
# SECTION 15 — Кнопки и обработчик callback'ов
# ==========================================================

@bot.callback_query_handler(func=lambda c: True)
def on_callback(call):
    try:
        data_str = call.data or ""
        chat_id = call.message.chat.id

        # -----------------------------------------
        # Блок пересылки (fw_)
        # -----------------------------------------
        if data_str.startswith("fw_"):

            if data_str == "fw_open":
                kb = build_forward_source_menu(chat_id)
                bot.edit_message_text(
                    "Выберите чат A:",
                    chat_id,
                    call.message.message_id,
                    reply_markup=kb
                )
                return

            if data_str == "fw_back_root":
                store = get_chat_store(chat_id)
                dk = store.get("current_view_day", today_key())
                kb = build_edit_menu_keyboard(dk, chat_id)
                bot.edit_message_text(
                    f"Меню редактирования ({dk}):",
                    chat_id,
                    call.message.message_id,
                    reply_markup=kb
                )
                return

            if data_str == "fw_back_src":
                kb = build_forward_source_menu(chat_id)
                bot.edit_message_text(
                    "Выберите чат A:",
                    chat_id,
                    call.message.message_id,
                    reply_markup=kb
                )
                return

            if data_str.startswith("fw_back_tgt:"):
                A = int(data_str.split(":")[1])
                kb = build_forward_target_menu(chat_id, A)
                bot.edit_message_text(
                    f"Источник A: {A}\nВыберите чат B:",
                    chat_id,
                    call.message.message_id,
                    reply_markup=kb
                )
                return

            if data_str.startswith("fw_src:"):
                A = int(data_str.split(":")[1])
                kb = build_forward_target_menu(chat_id, A)
                bot.edit_message_text(
                    f"Источник: {A}\nВыберите чат B:",
                    chat_id,
                    call.message.message_id,
                    reply_markup=kb
                )
                return

            if data_str.startswith("fw_tgt:"):
                _, A, B = data_str.split(":")
                A, B = int(A), int(B)
                kb = build_forward_mode_menu(chat_id, A, B)
                bot.edit_message_text(
                    f"Настройка пересылки: {A} ⇄ {B}",
                    chat_id,
                    call.message.message_id,
                    reply_markup=kb
                )
                return

            if data_str.startswith("fw_mode:"):
                _, A, B, mode = data_str.split(":")
                apply_forward_mode(int(A), int(B), mode)
                kb = build_forward_source_menu(chat_id)
                bot.edit_message_text(
                    "Маршрут обновлён.\nВыберите чат A:",
                    chat_id,
                    call.message.message_id,
                    reply_markup=kb
                )
                return

            return
                    # -----------------------------------------
        # КАЛЕНДАРЬ: листание месяцев
        # -----------------------------------------
        if data_str.startswith("c:"):
            center = data_str[2:]
            try:
                center_dt = datetime.strptime(center, "%Y-%m-%d")
            except ValueError:
                return

            kb = build_calendar_keyboard(center_dt, chat_id)
            try:
                bot.edit_message_reply_markup(
                    chat_id,
                    call.message.message_id,
                    reply_markup=kb
                )
            except Exception:
                pass
            return

        # -----------------------------------------
        # ВСЁ ОСТАЛЬНОЕ: логика d:<day>:cmd
        # -----------------------------------------
        if not data_str.startswith("d:"):
            return

        _, day_key, cmd = data_str.split(":", 2)
        store = get_chat_store(chat_id)

        # ========= Открыть день =========
        if cmd == "open":
            txt, _ = render_day_window(chat_id, day_key)
            kb = build_main_keyboard(day_key, chat_id)
            store["current_view_day"] = day_key

            bot.edit_message_text(
                txt, chat_id, call.message.message_id,
                reply_markup=kb, parse_mode="HTML"
            )
            set_active_window_id(chat_id, day_key, call.message.message_id)
            return

        # ========= Предыдущий день =========
        if cmd == "prev":
            d = datetime.strptime(day_key, "%Y-%m-%d") - timedelta(days=1)
            nd = d.strftime("%Y-%m-%d")
            txt, _ = render_day_window(chat_id, nd)
            kb = build_main_keyboard(nd, chat_id)
            store["current_view_day"] = nd

            bot.edit_message_text(
                txt, chat_id, call.message.message_id,
                reply_markup=kb, parse_mode="HTML"
            )
            set_active_window_id(chat_id, nd, call.message.message_id)
            return

        # ========= Следующий день =========
        if cmd == "next":
            d = datetime.strptime(day_key, "%Y-%m-%d") + timedelta(days=1)
            nd = d.strftime("%Y-%m-%d")
            txt, _ = render_day_window(chat_id, nd)
            kb = build_main_keyboard(nd, chat_id)
            store["current_view_day"] = nd

            bot.edit_message_text(
                txt, chat_id, call.message.message_id,
                reply_markup=kb, parse_mode="HTML"
            )
            set_active_window_id(chat_id, nd, call.message.message_id)
            return

        # ========= Сегодня =========
        if cmd == "today":
            nd = today_key()
            txt, _ = render_day_window(chat_id, nd)
            kb = build_main_keyboard(nd, chat_id)
            store["current_view_day"] = nd

            bot.edit_message_text(
                txt, chat_id, call.message.message_id,
                reply_markup=kb, parse_mode="HTML"
            )
            set_active_window_id(chat_id, nd, call.message.message_id)
            return

        # ========= Календарь =========
        if cmd == "calendar":
            try:
                cdt = datetime.strptime(day_key, "%Y-%m-%d")
            except:
                cdt = now_local()

            kb = build_calendar_keyboard(cdt, chat_id)
            bot.edit_message_reply_markup(
                chat_id, call.message.message_id, reply_markup=kb
            )
            return

        # ========= Отчёт =========
        if cmd == "report":
            lines = ["📊 Отчёт:"]
            for dk, recs in sorted(store.get("daily_records", {}).items()):
                s = sum(r["amount"] for r in recs)
                lines.append(f"{dk}: {fmt_num(s)}")
            bot.send_message(chat_id, "\n".join(lines))
            return

        # ========= Итог =========
        if cmd == "total":
            bal = store.get("balance", 0)
            sent = bot.send_message(
                chat_id,
                f"💰 <b>Итог по чату:</b> {fmt_num(bal)}",
                parse_mode="HTML"
            )
            store["total_msg_id"] = sent.message_id
            save_data(data)
            return

        # ========= Инфо =========
        if cmd == "info":
            bot.send_message(
                chat_id,
                "ℹ️ Бот — универсальная финансовая система\n"
                "/start — окно дня\n"
                "/prev /next — листание\n"
                "/json /csv — экспорт\n"
                "/report — отчёт\n"
                "/restore — восстановление\n"
                "/stopforward — отключить пересылку\n"
                "/autoadd_info — авто-добавление"
            )
            return

        # ========= Меню редактирования =========
        if cmd == "edit_menu":
            store["current_view_day"] = day_key
            kb = build_edit_menu_keyboard(day_key, chat_id)
            bot.edit_message_reply_markup(
                chat_id, call.message.message_id, reply_markup=kb
            )
            return

        # ========= Назад в окно дня =========
        if cmd == "back_main":
            txt, _ = render_day_window(chat_id, day_key)
            kb = build_main_keyboard(day_key, chat_id)
            bot.edit_message_text(
                txt, chat_id, call.message.message_id,
                reply_markup=kb, parse_mode="HTML"
            )
            return

        # ========= CSV (все записи чата) =========
        if cmd == "csv_all":
            cmd_csv_all(chat_id)
            return

        # ========= CSV за день =========
        if cmd == "csv_day":
            cmd_csv_day(chat_id, day_key)
            return

        # ========= Обнуление =========
        if cmd == "reset":
            store["reset_wait"] = True
            store["reset_time"] = time.time()
            save_data(data)
            send_and_auto_delete(chat_id, "⚠️ Напишите ДА для подтверждения (15 секунд).", 15)
            schedule_cancel_wait(chat_id, 15)
            return

        # ========= Добавить запись =========
        if cmd == "add":
            store["edit_wait"] = {"type": "add", "day_key": day_key}
            save_data(data)
            send_and_auto_delete(chat_id, "Введите сумму и текст. Пример: +350 супермаркет", 15)
            schedule_cancel_wait(chat_id, 15)
            return

        # ========= Список записей =========
        if cmd == "edit_list":
            day_recs = store.get("daily_records", {}).get(day_key, [])
            if not day_recs:
                send_and_auto_delete(chat_id, "Нет записей на этот день.")
                return

            kb2 = types.InlineKeyboardMarkup(row_width=3)
            for r in day_recs:
                label = f"{r['short_id']} {fmt_num(r['amount'])} — {r.get('note','')}"
                rid = r["id"]

                kb2.row(
                    types.InlineKeyboardButton(label, callback_data="none"),
                    types.InlineKeyboardButton("✏️", callback_data=f"d:{day_key}:edit_rec_{rid}"),
                    types.InlineKeyboardButton("❌", callback_data=f"d:{day_key}:del_rec_{rid}")
                )

            kb2.row(types.InlineKeyboardButton("🔙 Назад", callback_data=f"d:{day_key}:edit_menu"))
            bot.edit_message_text(
                "Выберите запись:",
                chat_id,
                call.message.message_id,
                reply_markup=kb2
            )
            return

        # ========= Редактировать запись =========
        if cmd.startswith("edit_rec_"):
            rid = int(cmd.split("_")[-1])
            store["edit_wait"] = {
                "type": "edit",
                "day_key": day_key,
                "rid": rid
            }
            save_data(data)

            kb = types.InlineKeyboardMarkup()
            kb.row(types.InlineKeyboardButton("🔙 Назад", callback_data=f"d:{day_key}:edit_list"))

            bot.edit_message_text(
                f"✏️ Введите новую сумму и текст\nдля R{rid} (можно несколько строк):",
                chat_id,
                call.message.message_id,
                reply_markup=kb
            )
            return

        # ========= Удалить запись =========
        if cmd.startswith("del_rec_"):
            rid = int(cmd.split("_")[-1])
            delete_record_in_chat(chat_id, rid)
            update_or_send_day_window(chat_id, day_key)
            refresh_total_message_if_any(chat_id)
            send_and_auto_delete(chat_id, f"Удалено: R{rid}", 10)
            return

        # ========= Меню пересылки =========
        if cmd == "forward_menu":
            kb = build_forward_root(chat_id, day_key)
            bot.edit_message_text(
                "Меню пересылки:",
                chat_id,
                call.message.message_id,
                reply_markup=kb
            )
            return

        # ========= Ввод даты вручную =========
        if cmd == "pick_date":
            bot.send_message(chat_id, "Введите дату в формате YYYY-MM-DD:")
            return

    except Exception as e:
        log_error(f"on_callback error: {e}")

# ==========================================================
# SECTION 16 — Добавление / изменение / удаление записей
# ==========================================================

def add_record_to_chat(chat_id: int, amount: int, note: str, owner):
    """Добавляет новую запись в чат."""
    store = get_chat_store(chat_id)

    rid = store.get("next_id", 1)
    rec = {
        "id": rid,
        "short_id": f"R{rid}",
        "timestamp": now_local().isoformat(timespec="seconds"),
        "amount": amount,
        "note": note,
        "owner": owner,
    }

    store.setdefault("records", []).append(rec)
    store.setdefault("daily_records", {}).setdefault(today_key(), []).append(rec)
    store["next_id"] = rid + 1

    # обновление баланса
    store["balance"] = sum(x["amount"] for x in store["records"])

    # глобальные данные
    data["records"] = []
    for cid, st in data.get("chats", {}).items():
        data["records"].extend(st.get("records", []))
    data["overall_balance"] = sum(x["amount"] for x in data["records"])

    save_data(data)
    save_chat_json(chat_id)
    export_global_csv(data)

    send_backup_to_channel(chat_id)
    send_backup_to_chat(chat_id)

def update_record_in_chat(chat_id: int, rid: int, new_amount: int, new_note: str):
    """Изменяет существующую запись."""
    store = get_chat_store(chat_id)

    target = None
    for r in store.get("records", []):
        if r["id"] == rid:
            target = r
            break

    if not target:
        return

    target["amount"] = new_amount
    target["note"] = new_note

    # обновление в daily_records
    for day, arr in store.get("daily_records", {}).items():
        for r in arr:
            if r["id"] == rid:
                r.update(target)

    # баланс
    store["balance"] = sum(x["amount"] for x in store["records"])

    # глобальные данные
    data["records"] = []
    for cid, st in data.get("chats", {}).items():
        data["records"].extend(st.get("records", []))
    data["overall_balance"] = sum(x["amount"] for x in data["records"])

    save_data(data)
    save_chat_json(chat_id)
    export_global_csv(data)

    send_backup_to_channel(chat_id)
    send_backup_to_chat(chat_id)

def delete_record_in_chat(chat_id: int, rid: int):
    """Удаляет запись из чата."""
    store = get_chat_store(chat_id)

    store["records"] = [x for x in store["records"] if x["id"] != rid]

    for day, arr in list(store.get("daily_records", {}).items()):
        arr2 = [x for x in arr if x["id"] != rid]
        if arr2:
            store["daily_records"][day] = arr2
        else:
            del store["daily_records"][day]

    # сохраняем
    store["balance"] = sum(x["amount"] for x in store["records"])

    data["records"] = []
    for cid, st in data.get("chats", {}).items():
        data["records"].extend(st.get("records", []))
    data["overall_balance"] = sum(x["amount"] for x in data["records"])

    save_data(data)
    save_chat_json(chat_id)
    export_global_csv(data)

    send_backup_to_channel(chat_id)
    send_backup_to_chat(chat_id)
    # ==========================================================
# SECTION 17 — Перенумерация записей
# ==========================================================

def renumber_chat_records(chat_id: int):
    """
    Полная перенумерация:
    - сортировка по timestamp
    - ID = 1,2,3...
    """
    store = get_chat_store(chat_id)
    daily = store.get("daily_records", {})

    all_recs = []

    for dk in sorted(daily.keys()):
        recs = daily[dk]
        recs_sorted = sorted(recs, key=lambda r: r.get("timestamp", ""))
        daily[dk] = recs_sorted
        all_recs.extend(recs_sorted)

    new_id = 1
    for r in all_recs:
        r["id"] = new_id
        r["short_id"] = f"R{new_id}"
        new_id += 1

    store["records"] = all_recs
    store["next_id"] = new_id


# ==========================================================
# SECTION 18 — Active windows (активные окна сообщений)
# ==========================================================

def get_or_create_active_windows(chat_id: int) -> dict:
    """Возвращает структуру активных окон."""
    return data.setdefault("active_messages", {}).setdefault(str(chat_id), {})

def set_active_window_id(chat_id: int, day_key: str, message_id: int):
    """Запоминает message_id окна дня."""
    aw = get_or_create_active_windows(chat_id)
    aw[day_key] = message_id
    save_data(data)

def get_active_window_id(chat_id: int, day_key: str):
    """Возвращает id окна, если есть."""
    aw = get_or_create_active_windows(chat_id)
    return aw.get(day_key)

def delete_active_window_if_exists(chat_id: int, day_key: str):
    """Удаляет старое окно дня (если существует)."""
    mid = get_active_window_id(chat_id, day_key)
    if not mid:
        return
    try:
        bot.delete_message(chat_id, mid)
    except:
        pass

    aw = get_or_create_active_windows(chat_id)
    if day_key in aw:
        del aw[day_key]
    save_data(data)


def update_or_send_day_window(chat_id: int, day_key: str):
    """
    Если окно уже существует — обновляет.
    Если нет — создаёт.
    """
    txt, _ = render_day_window(chat_id, day_key)
    kb = build_main_keyboard(day_key, chat_id)

    mid = get_active_window_id(chat_id, day_key)
    if mid:
        try:
            bot.edit_message_text(
                txt, chat_id, mid, reply_markup=kb, parse_mode="HTML"
            )
            return
        except:
            pass

    sent = bot.send_message(chat_id, txt, reply_markup=kb, parse_mode="HTML")
    set_active_window_id(chat_id, day_key, sent.message_id)


# ==========================================================
# SECTION 19 — Управление финансовым режимом
# ==========================================================

def is_finance_mode(chat_id: int) -> bool:
    return chat_id in finance_active_chats

def set_finance_mode(chat_id: int, enabled: bool):
    if enabled:
        finance_active_chats.add(chat_id)
    else:
        finance_active_chats.discard(chat_id)

def require_finance(chat_id: int) -> bool:
    if not is_finance_mode(chat_id):
        send_and_auto_delete(chat_id, "⚙️ Режим выключен. Напишите /поехали.")
        return False
    return True


# ==========================================================
# SECTION 20 — Обновление сообщения итогов
# ==========================================================

def refresh_total_message_if_any(chat_id: int):
    """Если есть сообщение итогов — обновляет."""
    store = get_chat_store(chat_id)
    msg_id = store.get("total_msg_id")
    if not msg_id:
        return

    try:
        bal = store.get("balance", 0)
        bot.edit_message_text(
            f"💰 <b>Итог по чату:</b> {fmt_num(bal)}",
            chat_id, msg_id, parse_mode="HTML"
        )
    except Exception as e:
        log_error(f"refresh_total_message_if_any({chat_id}): {e}")
        store["total_msg_id"] = None
        save_data(data)


# ==========================================================
# SECTION 21 — Команды (start/help/view/prev/next/balance...)
# ==========================================================

@bot.message_handler(commands=["поехали", "ok"])
def cmd_enable(msg):
    chat_id = msg.chat.id
    delete_message_later(chat_id, msg.message_id, 15)
    set_finance_mode(chat_id, True)
    save_data(data)
    send_and_auto_delete(chat_id, "🚀 Режим включён! /start")
    return

@bot.message_handler(commands=["start"])
def cmd_start(msg):
    chat_id = msg.chat.id
    delete_message_later(chat_id, msg.message_id, 15)

    if not require_finance(chat_id):
        return

    dk = today_key()
    txt, _ = render_day_window(chat_id, dk)
    kb = build_main_keyboard(dk, chat_id)

    sent = bot.send_message(chat_id, txt, reply_markup=kb, parse_mode="HTML")
    set_active_window_id(chat_id, dk, sent.message_id)

@bot.message_handler(commands=["help"])
def cmd_help(msg):
    chat_id = msg.chat.id
    delete_message_later(chat_id, msg.message_id, 15)

    if not is_finance_mode(chat_id):
        send_and_auto_delete(chat_id, "Режим выключен.")
        return

    bot.send_message(
        chat_id,
        "ℹ️ Доступные команды:\n"
        "/start — окно дня\n"
        "/view YYYY-MM-DD — открыть дату\n"
        "/prev /next — листание\n"
        "/report — отчёт\n"
        "/json /csv — экспорт\n"
        "/reset — обнулить\n"
        "/restore — восстановление\n"
        "/stopforward — отключить пересылку\n"
        "/autoadd_info — авто-добавление",
    )

@bot.message_handler(commands=["view"])
def cmd_view(msg):
    chat_id = msg.chat.id
    delete_message_later(chat_id, msg.message_id, 15)
    if not require_finance(chat_id):
        return

    parts = (msg.text or "").split()
    if len(parts) != 2:
        send_and_auto_delete(chat_id, "Использование: /view YYYY-MM-DD")
        return

    day_key = parts[1]
    try:
        datetime.strptime(day_key, "%Y-%m-%d")
    except:
        send_and_auto_delete(chat_id, "Неверная дата.")
        return

    txt, _ = render_day_window(chat_id, day_key)
    kb = build_main_keyboard(day_key, chat_id)
    sent = bot.send_message(chat_id, txt, reply_markup=kb, parse_mode="HTML")
    set_active_window_id(chat_id, day_key, sent.message_id)

@bot.message_handler(commands=["prev"])
def cmd_prev(msg):
    chat_id = msg.chat.id
    delete_message_later(chat_id, msg.message_id, 15)
    if not require_finance(chat_id): return

    d = datetime.strptime(today_key(), "%Y-%m-%d") - timedelta(days=1)
    dk = d.strftime("%Y-%m-%d")
    txt, _ = render_day_window(chat_id, dk)
    kb = build_main_keyboard(dk, chat_id)
    sent = bot.send_message(chat_id, txt, reply_markup=kb, parse_mode="HTML")
    set_active_window_id(chat_id, dk, sent.message_id)

@bot.message_handler(commands=["next"])
def cmd_next(msg):
    chat_id = msg.chat.id
    delete_message_later(chat_id, msg.message_id, 15)
    if not require_finance(chat_id): return

    d = datetime.strptime(today_key(), "%Y-%m-%d") + timedelta(days=1)
    dk = d.strftime("%Y-%m-%d")
    txt, _ = render_day_window(chat_id, dk)
    kb = build_main_keyboard(dk, chat_id)
    sent = bot.send_message(chat_id, txt, reply_markup=kb, parse_mode="HTML")
    set_active_window_id(chat_id, dk, sent.message_id)

@bot.message_handler(commands=["balance"])
def cmd_balance(msg):
    chat_id = msg.chat.id
    delete_message_later(chat_id, msg.message_id, 15)
    if not require_finance(chat_id): return

    bal = get_chat_store(chat_id).get("balance", 0)
    send_and_auto_delete(chat_id, f"💰 Баланс: {fmt_num(bal)}")

@bot.message_handler(commands=["report"])
def cmd_report(msg):
    chat_id = msg.chat.id
    delete_message_later(chat_id, msg.message_id, 15)
    if not require_finance(chat_id): return

    store = get_chat_store(chat_id)
    lines = ["📊 Отчёт:"]
    for dk, recs in sorted(store.get("daily_records", {}).items()):
        s = sum(r["amount"] for r in recs)
        lines.append(f"{dk}: {fmt_num(s)}")
    send_and_auto_delete(chat_id, "\n".join(lines), 20)


# ==========================================================
# SECTION 22 — CSV / JSON команды
# ==========================================================

def cmd_csv_all(chat_id: int):
    if not require_finance(chat_id): return

    save_chat_json(chat_id)
    path = chat_csv_file(chat_id)

    if not os.path.exists(path):
        send_and_auto_delete(chat_id, "CSV ещё не создан.")
        return

    upload_to_gdrive(path)
    with open(path, "rb") as f:
        bot.send_document(chat_id, f, caption="📂 CSV этого чата")

    send_backup_to_channel(chat_id)

def cmd_csv_day(chat_id: int, day_key: str):
    if not require_finance(chat_id): return

    store = get_chat_store(chat_id)
    day_recs = store.get("daily_records", {}).get(day_key, [])
    if not day_recs:
        send_and_auto_delete(chat_id, "Нет записей за этот день.")
        return

    tmp = f"data_{chat_id}_{day_key}.csv"
    try:
        with open(tmp, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["chat_id","ID","short_id","timestamp","amount","note","owner","day_key"])
            for r in day_recs:
                w.writerow([
                    chat_id, r["id"], r["short_id"], r["timestamp"],
                    r["amount"], r["note"], r["owner"], day_key
                ])

        upload_to_gdrive(tmp)
        with open(tmp, "rb") as f:
            bot.send_document(chat_id, f, caption=f"📅 CSV — {day_key}")

    finally:
        try: os.remove(tmp)
        except: pass


@bot.message_handler(commands=["csv"])
def cmd_csv(msg):
    chat_id = msg.chat.id
    delete_message_later(chat_id, msg.message_id, 15)
    if not require_finance(chat_id): return

    save_chat_json(chat_id)
    p = chat_csv_file(chat_id)
    if os.path.exists(p):
        upload_to_gdrive(p)
        with open(p, "rb") as f:
            bot.send_document(chat_id, f, caption="📂 CSV этого чата")
    send_backup_to_channel(chat_id)


@bot.message_handler(commands=["json"])
def cmd_json(msg):
    chat_id = msg.chat.id
    delete_message_later(chat_id, msg.message_id, 15)
    if not require_finance(chat_id): return

    save_chat_json(chat_id)
    p = chat_json_file(chat_id)
    if os.path.exists(p):
        with open(p, "rb") as f:
            bot.send_document(chat_id, f, caption="🧾 JSON этого чата")
    else:
        send_and_auto_delete(chat_id, "JSON ещё не создан.")


# ==========================================================
# SECTION 23 — Reset (обнуление)
# ==========================================================

@bot.message_handler(commands=["reset"])
def cmd_reset(msg):
    chat_id = msg.chat.id
    if not require_finance(chat_id): return

    store = get_chat_store(chat_id)
    store["reset_wait"] = True
    store["reset_time"] = time.time()
    save_data(data)

    send_and_auto_delete(chat_id, "⚠️ Напишите ДА для обнуления (15 секунд).")
    schedule_cancel_wait(chat_id, 15)


@bot.message_handler(commands=["stopforward"])
def cmd_stopforward(msg):
    data["forward_rules"] = {}
    save_data(data)
    send_and_auto_delete(msg.chat.id, "🔕 Пересылка выключена.")
    delete_message_later(msg.chat.id, msg.message_id, 15)
    # ==========================================================
# SECTION 24 — Режим бэкапа Google Drive ON/OFF
# ==========================================================

@bot.message_handler(commands=["backup_gdrive_on"])
def cmd_backup_drive_on(msg):
    backup_flags["drive"] = True
    save_data(data)
    send_and_auto_delete(msg.chat.id, "☁️ Бэкап в Google Drive — ВКЛЮЧЕН.")
    delete_message_later(msg.chat.id, msg.message_id, 15)

@bot.message_handler(commands=["backup_gdrive_off"])
def cmd_backup_drive_off(msg):
    backup_flags["drive"] = False
    save_data(data)
    send_and_auto_delete(msg.chat.id, "☁️ Бэкап в Google Drive — ВЫКЛЮЧЕН.")
    delete_message_later(msg.chat.id, msg.message_id, 15)

@bot.message_handler(commands=["backup_channel_on"])
def cmd_backup_channel_on(msg):
    backup_flags["channel"] = True
    save_data(data)
    send_and_auto_delete(msg.chat.id, "📡 Бэкап в канал — ВКЛЮЧЕН.")
    delete_message_later(msg.chat.id, msg.message_id, 15)

@bot.message_handler(commands=["backup_channel_off"])
def cmd_backup_channel_off(msg):
    backup_flags["channel"] = False
    save_data(data)
    send_and_auto_delete(msg.chat.id, "📡 Бэкап в канал — ВЫКЛЮЧЕН.")
    delete_message_later(msg.chat.id, msg.message_id, 15)


# ==========================================================
# SECTION 25 — AUTOADD (авто-добавление сумм)
# ==========================================================

@bot.message_handler(commands=["autoadd_info", "autoadd.info"])
def cmd_autoadd(msg):
    chat_id = msg.chat.id
    delete_message_later(chat_id, msg.message_id, 15)

    store = get_chat_store(chat_id)
    settings = store.setdefault("settings", {})
    current = settings.get("auto_add", False)

    settings["auto_add"] = not current
    save_chat_json(chat_id)

    send_and_auto_delete(
        chat_id,
        f"⚙️ Авто-добавление: {'ВКЛЮЧЕНО' if not current else 'ВЫКЛЮЧЕНО'}\n"
        "• ВКЛ: ввод суммы автоматически создаёт запись.\n"
        "• ВЫКЛ: запись создаётся только через кнопку «Добавить»."
    )


# ==========================================================
# SECTION 26 — auto-delete helpers (отложенное удаление)
# ==========================================================

def send_and_auto_delete(chat_id: int, text: str, delay: int = 10):
    """Отправляет сообщение и удаляет через delay секунд."""
    try:
        msg = bot.send_message(chat_id, text)
        def _job():
            time.sleep(delay)
            try:
                bot.delete_message(chat_id, msg.message_id)
            except:
                pass
        threading.Thread(target=_job, daemon=True).start()
    except Exception as e:
        log_error(f"send_and_auto_delete: {e}")

def delete_message_later(chat_id: int, msg_id: int, delay: int = 10):
    """Удаляет сообщение пользователя через delay секунд."""
    try:
        def _del():
            time.sleep(delay)
            try:
                bot.delete_message(chat_id, msg_id)
            except:
                pass
        threading.Thread(target=_del, daemon=True).start()
    except Exception as e:
        log_error(f"delete_message_later: {e}")


# ==========================================================
# SECTION 27 — Откладывание сброса (edit_wait/reset_wait)
# ==========================================================

_edit_cancel_timers = {}

def schedule_cancel_wait(chat_id: int, delay: float = 15.0):
    """
    Если пользователь не завершил операцию (add/edit/reset):
    • отменяет edit_wait типа add
    • отменяет reset_wait, если время вышло
    """
    def _job():
        try:
            store = get_chat_store(chat_id)
            changed = False

            wait = store.get("edit_wait")
            if wait and wait.get("type") == "add":
                store["edit_wait"] = None
                changed = True

            if store.get("reset_wait", False):
                store["reset_wait"] = False
                store["reset_time"] = 0
                changed = True

            if changed:
                save_data(data)

        except Exception as e:
            log_error(f"schedule_cancel_wait job: {e}")

    prev = _edit_cancel_timers.get(chat_id)
    if prev and prev.is_alive():
        try: prev.cancel()
        except: pass

    t = threading.Timer(delay, _job)
    _edit_cancel_timers[chat_id] = t
    t.start()


# ==========================================================
# SECTION 28 — Информация о чате (title / username)
# ==========================================================

def update_chat_info_from_message(msg):
    """
    Каждый чат хранит своё info + known_chats.
    Не зависит от владельцев.
    """
    chat_id = msg.chat.id
    store = get_chat_store(chat_id)

    info = store.setdefault("info", {})
    info["title"] = msg.chat.title or info.get("title") or f"Чат {chat_id}"
    info["username"] = msg.chat.username or info.get("username")
    info["type"] = msg.chat.type

    kc = store.setdefault("known_chats", {})
    kc[str(chat_id)] = {
        "title": info["title"],
        "username": info["username"],
        "type": info["type"],
    }

    save_chat_json(chat_id)


# ==========================================================
# SECTION 29 — Debounce / finalize (последняя обработка после затишья)
# ==========================================================

_finalize_timers = {}

def schedule_finalize(chat_id: int, day_key: str, delay: float = 2.0):
    """
    Вызывается после серии сообщений:
    • пересчёт баланса
    • обновление JSON и CSV
    • бэкапы
    • перерисовка окна дня
    """
    def _job():
        try:
            store = get_chat_store(chat_id)

            store["balance"] = sum(r["amount"] for r in store.get("records", []))

            all_recs = []
            for cid, st in data.get("chats", {}).items():
                all_recs.extend(st.get("records", []))

            data["records"] = all_recs
            data["overall_balance"] = sum(r["amount"] for r in all_recs)

            save_chat_json(chat_id)
            save_data(data)
            export_global_csv(data)

            send_backup_to_channel(chat_id)
            send_backup_to_chat(chat_id)

            # новое окно дня
            old_mid = get_active_window_id(chat_id, day_key)
            txt, _ = render_day_window(chat_id, day_key)
            kb = build_main_keyboard(day_key, chat_id)

            new_mid = None
            try:
                m = bot.send_message(chat_id, txt, reply_markup=kb, parse_mode="HTML")
                new_mid = m.message_id
                set_active_window_id(chat_id, day_key, new_mid)
            except Exception as e:
                log_error(f"schedule_finalize send: {e}")
                try:
                    update_or_send_day_window(chat_id, day_key)
                    new_mid = get_active_window_id(chat_id, day_key)
                except Exception as e2:
                    log_error(f"schedule_finalize fallback: {e2}")

            # удаляем старое окно
            if old_mid and new_mid and new_mid != old_mid:
                def _del():
                    time.sleep(1)
                    try:
                        bot.delete_message(chat_id, old_mid)
                    except:
                        pass
                threading.Thread(target=_del, daemon=True).start()

            refresh_total_message_if_any(chat_id)

        except Exception as e:
            log_error(f"schedule_finalize job error: {e}")

    prev = _finalize_timers.get(chat_id)
    if prev and prev.is_alive():
        try: prev.cancel()
        except: pass

    t = threading.Timer(delay, _job)
    _finalize_timers[chat_id] = t
    t.start()


# ==========================================================
# SECTION 30 — TEXT HANDLER (главный обработчик текста)
# ==========================================================

@bot.message_handler(content_types=["text"])
def handle_text(msg):
    try:
        chat_id = msg.chat.id
        text = (msg.text or "").strip()

        update_chat_info_from_message(msg)

        # ---------- Пересылка ----------
        targets = resolve_forward_targets(chat_id)
        if targets:
            forward_text_anon(chat_id, msg, targets)

        store = get_chat_store(chat_id)
        wait = store.get("edit_wait")
        auto_add = store.get("settings", {}).get("auto_add", False)

        # ====================================================
        # ДОБАВЛЕНИЕ ЗАПИСЕЙ (кнопка add + auto_add)
        # ====================================================

        should_add = False

        # кнопка «Добавить»
        if wait and wait.get("type") == "add" and looks_like_amount(text):
            should_add = True
            day_key = wait["day_key"]

        # авто-добавление
        elif auto_add and looks_like_amount(text):
            should_add = True
            day_key = store.get("current_view_day", today_key())

        if should_add:
            lines = [x.strip() for x in text.split("\n") if x.strip()]
            added_any = False

            for line in lines:
                try:
                    amount, note = split_amount_and_note(line)
                except:
                    send_and_auto_delete(chat_id, f"❌ Ошибка суммы: {line}")
                    continue

                rid = store.get("next_id", 1)
                rec = {
                    "id": rid,
                    "short_id": f"R{rid}",
                    "timestamp": now_local().isoformat(timespec="seconds"),
                    "amount": amount,
                    "note": note,
                    "owner": msg.from_user.id,
                }
                store.setdefault("records", []).append(rec)
                store.setdefault("daily_records", {}).setdefault(day_key, []).append(rec)
                store["next_id"] = rid + 1
                added_any = True

            if added_any:
                update_or_send_day_window(chat_id, day_key)
                schedule_finalize(chat_id, day_key)

            store["edit_wait"] = None
            save_chat_json(chat_id)
            save_data(data)
            export_global_csv(data)
            send_backup_to_channel(chat_id)
            send_backup_to_chat(chat_id)
            return

        # ====================================================
        # РЕДАКТИРОВАНИЕ ЗАПИСИ (многострочное)
        # ====================================================
        if wait and wait.get("type") == "edit":
            rid = wait.get("rid")
            day_key = wait.get("day_key", store.get("current_view_day", today_key()))

            old = None
            for r in store.get("records", []):
                if r["id"] == rid:
                    old = r
                    break

            if not old:
                send_and_auto_delete(chat_id, "Запись не найдена.")
                store["edit_wait"] = None
                return

            delete_record_in_chat(chat_id, rid)

            lines = [x.strip() for x in text.split("\n") if x.strip()]
            for ln in lines:
                try:
                    amount, note = split_amount_and_note(ln)
                except:
                    send_and_auto_delete(chat_id, f"Ошибка суммы: {ln}")
                    continue

                nrid = store.get("next_id", 1)
                rec = {
                    "id": nrid,
                    "short_id": f"R{nrid}",
                    "timestamp": now_local().isoformat(timespec="seconds"),
                    "amount": amount,
                    "note": note,
                    "owner": msg.from_user.id,
                }
                store.setdefault("records", []).append(rec)
                store.setdefault("daily_records", {}).setdefault(day_key, []).append(rec)
                store["next_id"] = nrid + 1

            update_or_send_day_window(chat_id, day_key)
            schedule_finalize(chat_id, day_key)

            refresh_total_message_if_any(chat_id)
            store["edit_wait"] = None
            save_data(data)
            return

        # ====================================================
        # ПОДТВЕРЖДЕНИЕ СБРОСА
        # ====================================================
        if text.upper() == "ДА":
            reset_flag = store.get("reset_wait")
            reset_time = store.get("reset_time", 0)

            if reset_flag and (time.time() - reset_time <= 15):
                reset_chat_data(chat_id)
                send_and_auto_delete(chat_id, "🔄 Данные чата обнулены.", 15)
            else:
                send_and_auto_delete(chat_id, "Нет активного запроса.")

            store["reset_wait"] = False
            store["reset_time"] = 0
            save_data(data)
            return

        if store.get("reset_wait", False):
            store["reset_wait"] = False
            store["reset_time"] = 0
            save_data(data)

    except Exception as e:
        log_error(f"handle_text: {e}")


# ==========================================================
# SECTION 31 — MEDIA HANDLER (фото, видео, документы)
# ==========================================================

@bot.message_handler(content_types=[
    "photo", "video", "audio", "document", "voice",
    "video_note", "sticker"
])
def handle_media(msg):
    try:
        chat_id = msg.chat.id

        update_chat_info_from_message(msg)

        targets = resolve_forward_targets(chat_id)
        if targets:
            if msg.media_group_id:
                grp = collect_media_group(chat_id, msg)
                forward_media_group_anon(chat_id, grp, targets)
            else:
                forward_media_anon(chat_id, msg, targets)

        if restore_mode:
            handle_restore_file(msg)
            return

    except Exception as e:
        log_error(f"handle_media: {e}")


# ==========================================================
# SECTION 32 — Restore mode (восстановление файлов)
# ==========================================================

@bot.message_handler(commands=["restore"])
def cmd_restore(msg):
    global restore_mode
    restore_mode = True
    send_and_auto_delete(
        msg.chat.id,
        "📥 Режим восстановления включён.\n"
        "Отправьте один из файлов:\n"
        "• data.json\n"
        "• data_<chat_id>.json\n"
        "• csv_meta.json\n"
        "• data_<chat_id>.csv"
    )

@bot.message_handler(commands=["restore_off"])
def cmd_restore_off(msg):
    global restore_mode
    restore_mode = False
    send_and_auto_delete(msg.chat.id, "🔒 Режим восстановления выключен.")


def handle_restore_file(msg):
    """Обработка документов в режиме восстановления."""
    try:
        chat_id = msg.chat.id
        if not msg.document:
            send_and_auto_delete(chat_id, "Нужен документ-файл JSON/CSV.")
            return

        file_name = msg.document.file_name
        file_id = msg.document.file_id

        info = bot.get_file(file_id)
        raw = bot.download_file(info.file_path)

        tmp = f"restore_tmp_{chat_id}_{file_name}"
        with open(tmp, "wb") as f:
            f.write(raw)

        if file_name == DATA_FILE:
            new = _load_json(tmp, {})
            data.clear()
            data.update(new)
            save_data(data)
            send_and_auto_delete(chat_id, "data.json восстановлен.")

        elif file_name.startswith("data_") and file_name.endswith(".json"):
            m = re.match(r"data_(\d+)\.json", file_name)
            if not m:
                raise ValueError("Неверное имя JSON")

            cid = int(m.group(1))
            new = _load_json(tmp, {})
            store = data.setdefault("chats", {}).setdefault(cid, {})
            store.clear()
            store.update(new)
            save_data(data)
            send_and_auto_delete(chat_id, f"JSON для чата {cid} восстановлен.")

        elif file_name == CSV_META_FILE:
            new = _load_json(tmp, {})
            _save_json(CSV_META_FILE, new)
            send_and_auto_delete(chat_id, "csv_meta.json восстановлен.")

        elif file_name.startswith("data_") and file_name.endswith(".csv"):
            with open(file_name, "wb") as f:
                f.write(raw)
            send_and_auto_delete(chat_id, "CSV восстановлен.")

        else:
            send_and_auto_delete(chat_id, "Неизвестный файл.")

        try: os.remove(tmp)
        except: pass

    except Exception as e:
        log_error(f"handle_restore_file: {e}")


# ==========================================================
# SECTION 33 — RESET DATA (обнуление чата)
# ==========================================================

def reset_chat_data(chat_id: int):
    """Полное удаление данных конкретного чата."""
    try:
        chats = data.setdefault("chats", {})
        if chat_id in chats:
            del chats[chat_id]

        all_recs = []
        for cid, st in chats.items():
            all_recs.extend(st.get("records", []))

        data["records"] = all_recs
        data["overall_balance"] = sum(r["amount"] for r in all_recs)

        save_data(data)
        export_global_csv(data)

        try: os.remove(chat_json_file(chat_id))
        except: pass
        try: os.remove(chat_csv_file(chat_id))
        except: pass

        send_backup_to_channel(chat_id)

    except Exception as e:
        log_error(f"reset_chat_data({chat_id}): {e}")


# ==========================================================
# SECTION 34 — Разбор сумм (парсер)
# ==========================================================

def looks_like_amount(text: str) -> bool:
    """Проверяет, похоже ли сообщение на ввод суммы."""
    text = text.strip()
    if not text:
        return False
    return bool(re.match(r"^[+-]?\d+[.,]?\d*\s+.+", text))

def split_amount_and_note(text: str):
    """
    Разделяет строку:
    "+300 супермаркет" → (300, "супермаркет")
    "-120 такси" → (-120, "такси")
    """
    m = re.match(r"^([+-]?\d+[.,]?\d*)\s*(.*)$", text)
    if not m:
        raise ValueError("Неверный ввод суммы")

    amount = float(m.group(1).replace(",", "."))
    note = m.group(2).strip()

    return int(amount), note


# ==========================================================
# SECTION 35 — Keyboards (основное меню / меню редактирования)
# ==========================================================

def build_main_keyboard(day_key: str, chat_id: int):
    kb = types.InlineKeyboardMarkup()

    kb.row(
        types.InlineKeyboardButton("⬅️", callback_data=f"d:{day_key}:prev"),
        types.InlineKeyboardButton("📅", callback_data=f"d:{day_key}:calendar"),
        types.InlineKeyboardButton("➡️", callback_data=f"d:{day_key}:next")
    )

    kb.row(types.InlineKeyboardButton("➕ Добавить", callback_data=f"d:{day_key}:add"))
    kb.row(types.InlineKeyboardButton("✏️ Редактировать", callback_data=f"d:{day_key}:edit_menu"))
    kb.row(types.InlineKeyboardButton("📊 Отчёт", callback_data=f"d:{day_key}:report"))
    kb.row(types.InlineKeyboardButton("💰 Итог", callback_data=f"d:{day_key}:total"))
    kb.row(types.InlineKeyboardButton("📂 CSV", callback_data=f"d:{day_key}:csv_all"))
    kb.row(types.InlineKeyboardButton("ℹ️ Инфо", callback_data=f"d:{day_key}:info"))

    return kb


def build_edit_menu_keyboard(day_key: str, chat_id: int):
    kb = types.InlineKeyboardMarkup()
    kb.row(types.InlineKeyboardButton("📋 Список записей", callback_data=f"d:{day_key}:edit_list"))
    kb.row(types.InlineKeyboardButton("🗑 Обнулить", callback_data=f"d:{day_key}:reset"))
    kb.row(types.InlineKeyboardButton("🔀 Пересылка", callback_data="forward_menu"))
    kb.row(types.InlineKeyboardButton("🔙 Назад", callback_data=f"d:{day_key}:back_main"))
    return kb


# ==========================================================
# SECTION 36 — Календарь (Keyboard)
# ==========================================================

def build_calendar_keyboard(center_date: datetime, chat_id: int):
    year, month = center_date.year, center_date.month

    first = datetime(year, month, 1)
    start_week = first.weekday()
    pad = 0 if start_week == 6 else start_week + 1

    next_month = (center_date.replace(day=28) + timedelta(days=4)).replace(day=1)
    total_days = (next_month - timedelta(days=1)).day

    grid = []
    row = []

    for _ in range(pad):
        row.append(None)

    for d in range(1, total_days + 1):
        row.append(d)
        if len(row) == 7:
            grid.append(row)
            row = []

    if row:
        while len(row) < 7:
            row.append(None)
        grid.append(row)

    kb = types.InlineKeyboardMarkup()

    kb.row(
        types.InlineKeyboardButton("<<", callback_data=f"c:{(center_date - timedelta(days=31)).strftime('%Y-%m-%d')}"),
        types.InlineKeyboardButton(f"{month:02d}.{year}", callback_data="none"),
        types.InlineKeyboardButton(">>", callback_data=f"c:{(center_date + timedelta(days=31)).strftime('%Y-%m-%d')}")
    )

    for r in grid:
        buttons = []
        for d in r:
            if d:
                dk = f"{year}-{month:02d}-{d:02d}"
                buttons.append(types.InlineKeyboardButton(str(d), callback_data=f"d:{dk}:open"))
            else:
                buttons.append(types.InlineKeyboardButton(" ", callback_data="none"))
        kb.row(*buttons)

    kb.row(types.InlineKeyboardButton("Сегодня", callback_data=f"d:{today_key()}:open"))
    return kb


# ==========================================================
# SECTION 37 — Экспорт CSV глобальный
# ==========================================================

def export_global_csv(data_obj):
    try:
        rows = []
        for cid, st in data_obj.get("chats", {}).items():
            for r in st.get("records", []):
                rows.append([
                    cid,
                    r.get("id"),
                    r.get("short_id"),
                    r.get("timestamp"),
                    r.get("amount"),
                    r.get("note"),
                    r.get("owner"),
                ])

        with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["chat_id", "ID", "short_id", "timestamp", "amount", "note", "owner"])
            w.writerows(rows)

    except Exception as e:
        log_error(f"export_global_csv: {e}")


# ==========================================================
# SECTION 38 — Webhook запуск сервера
# ==========================================================

if __name__ == "__main__":
    print("🚀 Бот запускается через Flask + Webhook...")
    startup()
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))
    
    