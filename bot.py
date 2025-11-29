# ==========================================================
# 🧭 Clean Finance Bot — NO OWNER_ID — Part 1/12
# Полностью переписанная версия без режима владельца
# Пересылка, бэкап, backup в чат/канал, дневные окна — одинаково для всех
# ==========================================================

# ========== SECTION 1 — Imports & basic config ==========
import os
import io
import json
import csv
import re
import html
import logging
import threading
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests
import telebot
from telebot import types
from telebot.types import (
    InputMediaPhoto,
    InputMediaVideo,
    InputMediaDocument,
    InputMediaAudio
)
from flask import Flask, request

# Google Drive
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from googleapiclient.discovery import build
from google.oauth2 import service_account


# ========== SECTION 2 — Environment & globals ==========

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
BACKUP_CHAT_ID = os.getenv("BACKUP_CHAT_ID", "").strip()
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
GDRIVE_FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID", "").strip()
APP_URL = os.getenv("APP_URL", "").strip()
PORT = int(os.getenv("PORT", "8443"))

# OWNER_ID существовал, но теперь не используется:
OWNER_ID = os.getenv("OWNER_ID", "").strip()   # <— переменная сохранена, но ЛОГИКИ НЕТ

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set")

VERSION = "CleanBot_022.9.11 — NO_OWNER_ID"

DEFAULT_TZ = "America/Argentina/Buenos_Aires"
KEEP_ALIVE_INTERVAL_SECONDS = 60

DATA_FILE = "data.json"
CSV_FILE = "data.csv"
CSV_META_FILE = "csv_meta.json"

# Глобальные флаги бэкапов
backup_flags = {
    "drive": True,
    "channel": True,
}

# Режим восстановления файлов
restore_mode = False

# Логирование
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

bot = telebot.TeleBot(BOT_TOKEN, parse_mode=None)
app = Flask(__name__)

# In-memory store
data = {}

# Набор чатов, где включён финансовый режим
finance_active_chats = set()

# Общие meta-файлы
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CHAT_BACKUP_META_FILE = os.path.join(BASE_DIR, "chat_backup_meta.json")
logger.info(f"chat_backup_meta.json PATH = {CHAT_BACKUP_META_FILE}")


# ========== SECTION 3 — Time & logging helpers ==========

def log_info(msg: str):
    logger.info(msg)

def log_error(msg: str):
    logger.error(msg)

def get_tz():
    try:
        return ZoneInfo(DEFAULT_TZ)
    except Exception:
        return timezone(timedelta(hours=-3))

def now_local():
    return datetime.now(get_tz())

def today_key():
    return now_local().strftime("%Y-%m-%d")
    # ==========================================================
# SECTION 4 — JSON / CSV helpers
# ==========================================================

def _load_json(path: str, default):
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        log_error(f"JSON load error {path}: {e}")
        return default


def _save_json(path: str, obj):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log_error(f"JSON save error {path}: {e}")


def _load_csv_meta():
    return _load_json(CSV_META_FILE, {})


def _save_csv_meta(meta: dict):
    try:
        _save_json(CSV_META_FILE, meta)
        log_info("csv_meta.json updated")
    except Exception as e:
        log_error(f"_save_csv_meta: {e}")


# ==========================================================
# Метаданные бэкапов прямо в чаты
# ==========================================================

def _load_chat_backup_meta() -> dict:
    try:
        if not os.path.exists(CHAT_BACKUP_META_FILE):
            return {}
        return _load_json(CHAT_BACKUP_META_FILE, {})
    except Exception as e:
        log_error(f"_load_chat_backup_meta: {e}")
        return {}


def _save_chat_backup_meta(meta: dict) -> None:
    try:
        log_info(f"SAVING META TO: {os.path.abspath(CHAT_BACKUP_META_FILE)}")
        _save_json(CHAT_BACKUP_META_FILE, meta)
    except Exception as e:
        log_error(f"_save_chat_backup_meta: {e}")


# ==========================================================
# UNIVERSAL CHAT BACKUP (NO OWNER LOGIC)
# ==========================================================

def send_backup_to_chat(chat_id: int) -> None:
    """
    Универсальный авто-бэкап JSON прямо в чате.
    • для всех чатов одинаково
    • если msg_id есть — делает edit_message_media
    • если нет — создаёт новое сообщение
    • имя файла = data_<chat>_ChatTitle.json
    """
    try:
        if not chat_id:
            return

        # всегда сохраняем файлы перед бэкапом
        try:
            save_chat_json(chat_id)
        except Exception as e:
            log_error(f"send_backup_to_chat save_chat_json({chat_id}): {e}")

        json_path = chat_json_file(chat_id)
        if not os.path.exists(json_path):
            log_error(f"send_backup_to_chat: {json_path} NOT FOUND")
            return

        meta = _load_chat_backup_meta()
        msg_key = f"msg_chat_{chat_id}"
        ts_key = f"timestamp_chat_{chat_id}"

        chat_title = _get_chat_title_for_backup(chat_id)
        caption = (
            f"🧾 Авто-бэкап JSON чата: {chat_title}\n"
            f"⏱ {now_local().strftime('%Y-%m-%d %H:%M:%S')}"
        )

        def _open_file():
            try:
                with open(json_path, "rb") as f:
                    data_bytes = f.read()
            except Exception as e:
                log_error(f"send_backup_to_chat open({json_path}): {e}")
                return None

            if not data_bytes:
                return None

            safe = _safe_chat_title_for_filename(chat_title)
            base = os.path.basename(json_path)
            name_no_ext, dot, ext = base.partition(".")

            if safe:
                final_name = f"{name_no_ext}_{safe}"
                if ext:
                    final_name += f".{ext}"
            else:
                final_name = base

            buf = io.BytesIO(data_bytes)
            buf.name = final_name
            return buf

        msg_id = meta.get(msg_key)

        # === Попытка обновления старого сообщения
        if msg_id:
            fobj = _open_file()
            if fobj:
                try:
                    bot.edit_message_media(
                        chat_id=chat_id,
                        message_id=msg_id,
                        media=telebot.types.InputMediaDocument(fobj, caption=caption)
                    )
                    # обновляем timestamp
                    meta[ts_key] = now_local().isoformat(timespec="seconds")
                    _save_chat_backup_meta(meta)
                    return
                except Exception as e:
                    log_error(f"send_backup_to_chat edit FAILED {chat_id}: {e}")

        # === Иначе создаём новое сообщение ===
        fobj = _open_file()
        if not fobj:
            return

        sent = bot.send_document(chat_id, fobj, caption=caption)
        meta[msg_key] = sent.message_id
        meta[ts_key] = now_local().isoformat(timespec="seconds")
        _save_chat_backup_meta(meta)

        log_info(f"Chat backup CREATED for {chat_id}")

    except Exception as e:
        log_error(f"send_backup_to_chat error ({chat_id}): {e}")


# ==========================================================
# SECTION 4.1 — Default data + load/save
# ==========================================================

def default_data():
    return {
        "overall_balance": 0,
        "records": [],
        "chats": {},
        "active_messages": {},
        "next_id": 1,

        # флаги бэкапа глобального
        "backup_flags": {"drive": True, "channel": True},

        # пересылка — теперь общая для всех чатов
        "forward_rules": {},
    }


def load_data():
    d = _load_json(DATA_FILE, default_data())

    # дополняем недостающие ключи
    base = default_data()
    for k, v in base.items():
        if k not in d:
            d[k] = v

    # флаги → runtime
    flags = d.get("backup_flags") or {}
    backup_flags["drive"] = bool(flags.get("drive", True))
    backup_flags["channel"] = bool(flags.get("channel", True))

    return d


def save_data(d):
    d["backup_flags"] = {
        "drive": backup_flags.get("drive", True),
        "channel": backup_flags.get("channel", True),
    }
    _save_json(DATA_FILE, d)
    # ==========================================================
# SECTION 5 — Per-chat storage helpers (NO OWNER LOGIC)
# ==========================================================

def chat_json_file(chat_id: int) -> str:
    return f"data_{chat_id}.json"


def chat_csv_file(chat_id: int) -> str:
    return f"data_{chat_id}.csv"


def chat_meta_file(chat_id: int) -> str:
    return f"csv_meta_{chat_id}.json"


def get_chat_store(chat_id: int) -> dict:
    """
    Хранилище данных одного чата.
    Для всех чатов одинаковая структура.
    Никакого режима владельца, никаких known_chats-владельца.
    """
    chats = data.setdefault("chats", {})

    store = chats.setdefault(
        str(chat_id),
        {
            "info": {},                 # название чата / username / тип
            "balance": 0,               # остаток
            "records": [],              # все записи
            "daily_records": {},        # { "2025-01-01": [..] }
            "next_id": 1,               # следующий ID
            "active_windows": {},       # окна дня
            "edit_wait": None,          # ожидание редактирования
            "edit_target": None,
            "current_view_day": today_key(),
            "settings": {"auto_add": False},
        }
    )

    return store


# ==========================================================
# SECTION 5.1 — Save per-chat JSON / CSV / META
# ==========================================================

def save_chat_json(chat_id: int):
    """
    Полное сохранение JSON / CSV / META одного чата.
    Универсально — одинаково для всех, без разделения на владельца.
    """
    try:
        store = data.get("chats", {}).get(str(chat_id))
        if not store:
            store = get_chat_store(chat_id)

        # пути
        path_json = chat_json_file(chat_id)
        path_csv = chat_csv_file(chat_id)
        path_meta = chat_meta_file(chat_id)

        # гарантируем существование файлов
        for p in (path_json, path_csv, path_meta):
            if not os.path.exists(p):
                with open(p, "a", encoding="utf-8"):
                    pass

        # ======================================================
        # JSON-файл
        # ======================================================
        payload = {
            "chat_id": chat_id,
            "balance": store.get("balance", 0),
            "records": store.get("records", []),
            "daily_records": store.get("daily_records", {}),
            "next_id": store.get("next_id", 1),
            "info": store.get("info", {}),
        }

        _save_json(path_json, payload)

        # ======================================================
        # CSV-файл
        # ======================================================
        with open(path_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)

            # Заголовок
            w.writerow([
                "chat_id", "ID", "short_id", "timestamp",
                "amount", "note", "owner", "day_key"
            ])

            # дни по возрастанию
            daily = store.get("daily_records", {})
            for dk in sorted(daily.keys()):
                recs = daily.get(dk, [])
                recs_sorted = sorted(recs, key=lambda r: r.get("timestamp", ""))

                for r in recs_sorted:
                    w.writerow([
                        chat_id,
                        r.get("id"),
                        r.get("short_id"),
                        r.get("timestamp"),
                        r.get("amount"),
                        r.get("note"),
                        r.get("owner"),
                        dk,
                    ])

        # ======================================================
        # META-файл
        # ======================================================
        meta = {
            "last_saved": now_local().isoformat(timespec="seconds"),
            "record_count": sum(
                len(v) for v in store.get("daily_records", {}).values()
            ),
        }
        _save_json(path_meta, meta)

        log_info(f"Per-chat files saved for chat {chat_id}")

    except Exception as e:
        log_error(f"save_chat_json({chat_id}): {e}")
        # ==========================================================
# SECTION 6 — Number formatting & parsing (EU format)
# ==========================================================

def fmt_num(x):
    """
    Европейский формат вывода с обязательным знаком.
    Примеры:
        +1234.56 → ➕ 1.234,56
        -800     → ➖ 800
        0        → ➕ 0
    """

    sign = "+" if x >= 0 else "-"
    x = abs(x)

    # убираем трейлинг нули
    s = f"{x:.12f}".rstrip("0").rstrip(".")

    if "." in s:
        int_part, dec_part = s.split(".")
    else:
        int_part, dec_part = s, ""

    # формат тысяч через точки
    int_part = f"{int(int_part):,}".replace(",", ".")

    if dec_part:
        s = f"{int_part},{dec_part}"
    else:
        s = int_part

    return f"{sign}{s}"


# регулярка для выделения первого числа
num_re = re.compile(r"[+\-–]?\s*\d[\d\s.,_'’]*")


def parse_amount(raw: str) -> float:
    """
    Разбирает любую сумму:
    - 1.234,56
    - 1,234.56
    - 500
    - +500 кафе
    - -200 taxi
    - 2 500,10
    Определяет десятичный знак по последнему разделителю.
    """

    s = raw.strip()

    # знак
    is_negative = s.startswith("-") or s.startswith("–")
    is_positive = s.startswith("+")

    s_clean = s.lstrip("+-–").strip()

    # убираем мусор
    s_clean = (
        s_clean.replace(" ", "")
        .replace("_", "")
        .replace("’", "")
        .replace("'", "")
    )

    # нет разделителей
    if "," not in s_clean and "." not in s_clean:
        value = float(s_clean)
        if not is_positive and not is_negative:
            is_negative = True
        return -value if is_negative else value

    # оба разделителя
    if "." in s_clean and "," in s_clean:
        if s_clean.rfind(",") > s_clean.rfind("."):
            s_clean = s_clean.replace(".", "")
            s_clean = s_clean.replace(",", ".")
        else:
            s_clean = s_clean.replace(",", "")
    else:
        # только один разделитель
        if "," in s_clean:
            pos = s_clean.rfind(",")
            if len(s_clean) - pos - 1 in (1, 2):
                s_clean = s_clean.replace(".", "")
                s_clean = s_clean.replace(",", ".")
            else:
                s_clean = s_clean.replace(",", "")
        elif "." in s_clean:
            pos = s_clean.rfind(".")
            if len(s_clean) - pos - 1 in (1, 2):
                s_clean = s_clean.replace(",", "")
            else:
                s_clean = s_clean.replace(".", "")

    value = float(s_clean)

    # отсутствие знака → расход
    if not is_positive and not is_negative:
        is_negative = True

    return -value if is_negative else value


def split_amount_and_note(text: str):
    """
    Возвращает:
        amount (float)
        note (str)
    """
    m = num_re.search(text)
    if not m:
        raise ValueError("no number found")

    raw_number = m.group(0)
    amount = parse_amount(raw_number)

    note = text.replace(raw_number, " ").strip()
    note = re.sub(r"\s+", " ", note)

    return amount, note


def looks_like_amount(text: str) -> bool:
    """
    Лёгкая проверка: является ли строка суммой.
    """
    try:
        split_amount_and_note(text)
        return True
    except:
        return False
        # ==========================================================
# SECTION 7 — Google Drive helpers (NO OWNER LOGIC)
# ==========================================================

def _get_drive_service():
    """
    Универсальная авторизация Google Drive.
    БЕЗ какого-либо OWNER режима — работает одинаково для всех.
    """
    if not GOOGLE_SERVICE_ACCOUNT_JSON or not GDRIVE_FOLDER_ID:
        return None

    try:
        info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
        creds = service_account.Credentials.from_service_account_info(
            info,
            scopes=["https://www.googleapis.com/auth/drive"]
        )
        return build("drive", "v3", credentials=creds)
    except Exception as e:
        log_error(f"Drive service error: {e}")
        return None


def upload_to_gdrive(path: str, mime_type: str = None, description: str = None):
    """
    Загружает или обновляет файл в GDrive.
    Доступно всем чатам одинаково.
    """
    flags = backup_flags or {}
    if not flags.get("drive", True):
        log_info("upload_to_gdrive: disabled by flag.")
        return

    if not os.path.exists(path):
        log_error(f"upload_to_gdrive: file not found {path}")
        return

    service = _get_drive_service()
    if service is None:
        return

    fname = os.path.basename(path)
    file_metadata = {
        "name": fname,
        "parents": [GDRIVE_FOLDER_ID],
        "description": description or "",
    }
    media = MediaFileUpload(path, mimetype=mime_type, resumable=True)

    try:
        existing = service.files().list(
            q=f"name = '{fname}' and '{GDRIVE_FOLDER_ID}' in parents and trashed = false",
            spaces="drive",
            fields="files(id,name)"
        ).execute()

        items = existing.get("files", [])
        if items:
            file_id = items[0]["id"]
            service.files().update(
                fileId=file_id,
                media_body=media,
                body={"description": description or ""},
            ).execute()
            log_info(f"GDrive: updated {fname}")
        else:
            created = service.files().create(
                body=file_metadata,
                media_body=media,
                fields="id"
            ).execute()
            log_info(f"GDrive: created {fname}, id={created.get('id')}")

    except Exception as e:
        log_error(f"upload_to_gdrive({path}): {e}")


def download_from_gdrive(filename: str, dest_path: str) -> bool:
    """
    Восстанавливает файл filename в dest_path.
    """
    service = _get_drive_service()
    if service is None:
        return False

    try:
        res = service.files().list(
            q=f"name = '{filename}' and '{GDRIVE_FOLDER_ID}' in parents and trashed = false",
            spaces="drive",
            fields="files(id,name,mimeType,size)",
        ).execute()

        items = res.get("files", [])
        if not items:
            return False

        file_id = items[0]["id"]
        req = service.files().get_media(fileId=file_id)
        fh = io.FileIO(dest_path, "wb")
        downloader = MediaIoBaseDownload(fh, req)

        done = False
        while not done:
            status, done = downloader.next_chunk()

        log_info(f"GDrive: downloaded {filename}")
        return True

    except Exception as e:
        log_error(f"download_from_gdrive({filename}): {e}")
        return False


def restore_from_gdrive_if_needed() -> bool:
    """
    Если отсутствуют global JSON / CSV / meta — тянем их из Google Drive.
    """
    restored = False

    if not os.path.exists(DATA_FILE):
        if download_from_gdrive(os.path.basename(DATA_FILE), DATA_FILE):
            restored = True

    if not os.path.exists(CSV_FILE):
        if download_from_gdrive(os.path.basename(CSV_FILE), CSV_FILE):
            restored = True

    if not os.path.exists(CSV_META_FILE):
        if download_from_gdrive(os.path.basename(CSV_META_FILE), CSV_META_FILE):
            restored = True

    return restored


# ==========================================================
# SECTION 8 — Backup to channel (clean version)
# ==========================================================

# emoji-цифры для ID чатов
EMOJI_DIGITS = {
    "0": "0️⃣", "1": "1️⃣", "2": "2️⃣", "3": "3️⃣", "4": "4️⃣",
    "5": "5️⃣", "6": "6️⃣", "7": "7️⃣", "8": "8️⃣", "9": "9️⃣"
}

# чаты, для которых был отправлен emoji-ID в канал
backup_channel_notified_chats = set()


def format_chat_id_emoji(chat_id: int) -> str:
    return "".join(EMOJI_DIGITS.get(ch, ch) for ch in str(chat_id))


def _safe_chat_title_for_filename(title) -> str:
    """Безопасное имя файла по названию чата."""
    if not title:
        return ""
    title = str(title).strip().replace(" ", "_")
    title = re.sub(r"[^0-9A-Za-zА-Яа-я_\-]+", "", title)
    return title[:32]


def _get_chat_title_for_backup(chat_id: int) -> str:
    """
    Возвращает название чата — универсально для всех.
    """
    try:
        store = get_chat_store(chat_id)
        info = store.get("info", {})
        if info.get("title"):
            return info["title"]
    except:
        pass
    return f"chat_{chat_id}"


def send_backup_to_channel_for_file(path: str, meta_key: str, chat_title: str = None):
    """
    Отправляет или обновляет конкретный файл в BACKUP_CHAT_ID.
    NO OWNER LOGIC — одинаково для всех.
    """
    if not BACKUP_CHAT_ID:
        return
    if not os.path.exists(path):
        return

    try:
        meta = _load_csv_meta()
        msg_key = f"msg_{meta_key}"
        ts_key = f"timestamp_{meta_key}"

        base_name = os.path.basename(path)
        title_safe = _safe_chat_title_for_filename(chat_title or "")

        name_no_ext, dot, ext = base_name.partition(".")
        if title_safe:
            file_name = f"{name_no_ext}_{title_safe}"
            if ext:
                file_name += f".{ext}"
        else:
            file_name = base_name

        caption = (
            f"📦 {file_name} — "
            f"{now_local().strftime('%Y-%m-%d %H:%M')}"
        )

        def _open():
            with open(path, "rb") as f:
                d = f.read()
            if not d:
                return None
            buf = io.BytesIO(d)
            buf.name = file_name
            buf.seek(0)
            return buf

        # обновление существующего сообщения
        if meta.get(msg_key):
            fobj = _open()
            if fobj:
                try:
                    bot.edit_message_media(
                        chat_id=int(BACKUP_CHAT_ID),
                        message_id=meta[msg_key],
                        media=telebot.types.InputMediaDocument(
                            fobj, caption=caption
                        )
                    )
                    meta[ts_key] = now_local().isoformat(timespec="seconds")
                    _save_csv_meta(meta)
                    return
                except Exception as e:
                    log_error(f"edit_message_media {path}: {e}")

        # создаём новое сообщение
        fobj = _open()
        if not fobj:
            return

        sent = bot.send_document(int(BACKUP_CHAT_ID), fobj, caption=caption)
        meta[msg_key] = sent.message_id
        meta[ts_key] = now_local().isoformat(timespec="seconds")
        _save_csv_meta(meta)

    except Exception as e:
        log_error(f"send_backup_to_channel_for_file({path}): {e}")


def send_backup_to_channel(chat_id: int):
    """
    Универсальный бэкап:
    • JSON чата
    • CSV чата
    • глобальный data.json
    • глобальный data.csv
    • emoji-ID один раз
    """
    try:
        if not BACKUP_CHAT_ID:
            return
        if not backup_flags.get("channel", True):
            return

        backup_chat_id = int(BACKUP_CHAT_ID)

        # 1) сохраняем файлы
        save_chat_json(chat_id)
        export_global_csv(data)
        save_data(data)

        chat_title = _get_chat_title_for_backup(chat_id)

        # 2) emoji-ID (один раз)
        if chat_id not in backup_channel_notified_chats:
            try:
                emoji = format_chat_id_emoji(chat_id)
                bot.send_message(backup_chat_id, emoji)
                backup_channel_notified_chats.add(chat_id)
            except Exception as e:
                log_error(f"send_backup_to_channel emoji: {e}")

        # 3) per-chat JSON & CSV
        send_backup_to_channel_for_file(
            chat_json_file(chat_id),
            f"json_{chat_id}",
            chat_title
        )

        send_backup_to_channel_for_file(
            chat_csv_file(chat_id),
            f"csv_{chat_id}",
            chat_title
        )

        # 4) global files
        send_backup_to_channel_for_file(DATA_FILE, "global_data", "ALL_CHATS")
        send_backup_to_channel_for_file(CSV_FILE, "global_csv", "ALL_CHATS")

    except Exception as e:
        log_error(f"send_backup_to_channel({chat_id}): {e}")
        # ==========================================================
# SECTION 9 — Restore logic (clean)
# ==========================================================

def try_restore_global_files():
    """
    Если отсутствуют глобальные JSON / CSV — пытаемся взять из Google Drive.
    """
    global restore_mode

    if os.path.exists(DATA_FILE):
        return

    log_info("Global data files missing — trying restore from GDrive...")
    ok = restore_from_gdrive_if_needed()
    if ok:
        restore_mode = True
        log_info("Global restore completed.")
    else:
        log_info("Global restore not available — creating new blank structure.")
        save_data(default_data())


# ==========================================================
# SECTION 10 — Small helpers
# ==========================================================

def safe_html(s: str) -> str:
    return html.escape(str(s), quote=True)


def create_text(s: str) -> str:
    return safe_html(s or "")


# ==========================================================
# SECTION 11 — send_info + auto-delete
# ==========================================================

def send_info(chat_id: int, text: str, delay: int = 0):
    """
    Отправляет сообщение с инфо.
    Если delay > 0 — автоудаляет.
    """
    try:
        msg = bot.send_message(chat_id, text)
        if delay > 0:
            def _del():
                time.sleep(delay)
                try:
                    bot.delete_message(chat_id, msg.message_id)
                except:
                    pass
            threading.Thread(target=_del, daemon=True).start()
        return msg
    except Exception as e:
        log_error(f"send_info({chat_id}): {e}")


def send_and_auto_delete(chat_id: int, text: str, delay: int = 10):
    """
    Укороченная версия.
    """
    try:
        msg = bot.send_message(chat_id, text)
        def _delete():
            time.sleep(delay)
            try:
                bot.delete_message(chat_id, msg.message_id)
            except Exception:
                pass
        threading.Thread(target=_delete, daemon=True).start()
    except Exception as e:
        log_error(f"send_and_auto_delete: {e}")


# ==========================================================
# SECTION 12 — Chat metadata updater
# ==========================================================

def update_chat_info_from_message(msg):
    """
    Универсальный механизм:
    • сохраняем title / username / type
    • используется всеми чатами одинаково
    """

    try:
        chat = msg.chat
        chat_id = chat.id
        store = get_chat_store(chat_id)

        info = store.setdefault("info", {})
        changed = False

        # TITLE
        if chat.title:
            if info.get("title") != chat.title:
                info["title"] = chat.title
                changed = True

        # USERNAME
        if chat.username:
            if info.get("username") != chat.username:
                info["username"] = chat.username
                changed = True

        # TYPE
        if info.get("type") != chat.type:
            info["type"] = chat.type
            changed = True

        if changed:
            save_data(data)

    except Exception as e:
        log_error(f"update_chat_info_from_message: {e}")
        # ==========================================================
# SECTION 13 — FORWARD RULES (UNIVERSAL, NO OWNER LOGIC)
# ==========================================================

def get_forward_rules() -> dict:
    """Возвращает словарь правил пересылки."""
    return data.setdefault("forward_rules", {})


def save_forward_rules(rules: dict):
    """Сохраняет правила."""
    data["forward_rules"] = rules
    save_data(data)


def is_forward_enabled(src_chat: int, dst_chat: int) -> bool:
    """
    Проверяет: есть ли правило src → dst.
    """
    rules = get_forward_rules()
    key = str(src_chat)
    if key not in rules:
        return False

    dsts = rules[key]
    if not isinstance(dsts, list):
        return False

    return dst_chat in dsts


def enable_forward(src_chat: int, dst_chat: int):
    """Включает пересылку src → dst."""
    rules = get_forward_rules()
    arr = rules.setdefault(str(src_chat), [])
    if dst_chat not in arr:
        arr.append(dst_chat)
    save_forward_rules(rules)


def disable_forward(src_chat: int, dst_chat: int):
    """Выключает пересылку src → dst."""
    rules = get_forward_rules()
    arr = rules.setdefault(str(src_chat), [])
    if dst_chat in arr:
        arr.remove(dst_chat)
    save_forward_rules(rules)


def toggle_forward_two_way(chat1: int, chat2: int):
    """
    Переключает двухстороннюю пересылку между чатами:
    - если обе активны → выключить обе
    - если выключено → включить обе
    """

    a = is_forward_enabled(chat1, chat2)
    b = is_forward_enabled(chat2, chat1)

    if a and b:
        disable_forward(chat1, chat2)
        disable_forward(chat2, chat1)
        return "disabled"

    enable_forward(chat1, chat2)
    enable_forward(chat2, chat1)
    return "enabled"


def list_all_chat_ids():
    """
    Возвращает список chat_id, у которых есть данные.
    Используется в меню пересылки.
    """
    chats = data.get("chats", {})
    ids = []
    for cid in chats.keys():
        try:
            ids.append(int(cid))
        except:
            pass
    return sorted(ids)


# ==========================================================
# SECTION 14 — Forwarding engine
# ==========================================================

FORWARDABLE_TYPES = {
    "text",
    "photo",
    "audio",
    "voice",
    "video",
    "document",
    "sticker",
    "animation",
    "video_note",
    "location",
    "venue",
    "contact",
    "poll",
}


def handle_forward_if_needed(msg):
    """
    Основной механизм пересылки.
    Работает одинаково для всех чатов.
    """
    try:
        chat_id = msg.chat.id
        rules = get_forward_rules()

        dsts = rules.get(str(chat_id), [])
        if not dsts:
            return  # нет куда пересылать

        # только если сообщение имеет допустимый тип
        if msg.content_type not in FORWARDABLE_TYPES:
            return

        for dst in dsts:
            try:
                forward_message_clean(chat_id, dst, msg)
            except Exception as e:
                log_error(f"forward {chat_id}->{dst}: {e}")

    except Exception as e:
        log_error(f"handle_forward_if_needed: {e}")


def forward_message_clean(src: int, dst: int, msg):
    """
    Пересылка контента БЕЗ упоминания отправителя.
    (анонимно — сообщение создает бот)
    """

    ct = msg.content_type

    if ct == "text":
        bot.send_message(dst, msg.text)

    elif ct == "photo":
        ph = msg.photo[-1]
        bot.send_photo(dst, ph.file_id, caption=msg.caption or "")

    elif ct == "video":
        bot.send_video(dst, msg.video.file_id, caption=msg.caption or "")

    elif ct == "audio":
        bot.send_audio(dst, msg.audio.file_id, caption=msg.caption or "")

    elif ct == "voice":
        bot.send_voice(dst, msg.voice.file_id)

    elif ct == "document":
        bot.send_document(dst, msg.document.file_id, caption=msg.caption or "")

    elif ct == "sticker":
        bot.send_sticker(dst, msg.sticker.file_id)

    elif ct == "animation":
        bot.send_animation(dst, msg.animation.file_id, caption=msg.caption or "")

    elif ct == "video_note":
        bot.send_video_note(dst, msg.video_note.file_id)

    elif ct == "location":
        bot.send_location(dst, msg.location.latitude, msg.location.longitude)

    elif ct == "venue":
        bot.send_venue(
            dst,
            msg.venue.location.latitude,
            msg.venue.location.longitude,
            msg.venue.title,
            msg.venue.address
        )

    elif ct == "contact":
        bot.send_contact(dst, msg.contact.phone_number, msg.contact.first_name)

    elif ct == "poll":
        bot.send_poll(
            dst,
            msg.poll.question,
            [o.text for o in msg.poll.options],
            is_anonymous=msg.poll.is_anonymous,
            type=msg.poll.type,
        )

    else:
        bot.send_message(dst, f"Unsupported message type: {ct}")
        # ==========================================================
# SECTION 15 — Finance mode
# ==========================================================

def is_finance_mode(chat_id: int) -> bool:
    return chat_id in finance_active_chats


def enable_finance_mode(chat_id: int):
    finance_active_chats.add(chat_id)


def require_finance(chat_id: int) -> bool:
    """
    Проверка: включён ли финансовый режим.
    Если нет — выдаём подсказку.
    """
    if not is_finance_mode(chat_id):
        send_info(
            chat_id,
            "⚙️ Финансовый режим выключен.\n"
            "Активируйте командой /поехали",
            delay=8
        )
        return False
    return True


# ==========================================================
# SECTION 16 — Adding records
# ==========================================================

def add_record(chat_id: int, text: str):
    """
    Основная функция добавления записи.
    Работает одинаково для всех чатов.
    """
    store = get_chat_store(chat_id)

    try:
        amount, note = split_amount_and_note(text)
    except Exception:
        send_and_auto_delete(chat_id, "❌ Ошибка суммы.\nПожалуйста, введите корректно.", 7)
        return

    dk = today_key()
    daily = store.setdefault("daily_records", {})
    arr = daily.setdefault(dk, [])

    rid = store.get("next_id", 1)
    short_id = f"R{rid}"

    rec = {
        "id": rid,
        "short_id": short_id,
        "timestamp": now_local().isoformat(timespec="seconds"),
        "amount": amount,
        "note": note,
        "owner": "",     # без владельца
        "day_key": dk,
    }

    arr.append(rec)

    # обновляем глобальный баланс чата
    store["balance"] = store.get("balance", 0) + amount
    store["next_id"] = rid + 1

    save_data(data)
    save_chat_json(chat_id)

    try:
        send_backup_to_channel(chat_id)
    except Exception as e:
        log_error(f"backup after add_record({chat_id}): {e}")

    # обновляем окно дня
    try:
        update_or_send_day_window(chat_id, dk)
    except Exception as e:
        log_error(f"update day window after add_record({chat_id}): {e}")


# ==========================================================
# SECTION 17 — Editing / deleting records (base logic)
# ==========================================================

def find_record(store: dict, day_key: str, rid: int):
    """
    Ищет запись по ID внутри store.
    """
    daily = store.get("daily_records", {})
    arr = daily.get(day_key, [])
    for r in arr:
        if r.get("id") == rid:
            return r
    return None


def delete_record(chat_id: int, day_key: str, rid: int):
    """
    Удаляет запись.
    """
    store = get_chat_store(chat_id)
    rec = find_record(store, day_key, rid)
    if not rec:
        return

    amount = rec.get("amount", 0)

    daily = store.get("daily_records", {})
    arr = daily.get(day_key, [])
    arr[:] = [r for r in arr if r.get("id") != rid]

    # корректировка баланса
    store["balance"] = store.get("balance", 0) - amount

    save_data(data)
    save_chat_json(chat_id)

    try:
        send_backup_to_channel(chat_id)
    except Exception as e:
        log_error(f"backup after delete({chat_id}): {e}")

    try:
        update_or_send_day_window(chat_id, day_key)
    except Exception as e:
        log_error(f"day window after delete({chat_id}): {e}")


def edit_record(chat_id: int, day_key: str, rid: int, new_text: str):
    """
    Редактирование записи.
    """
    store = get_chat_store(chat_id)
    rec = find_record(store, day_key, rid)
    if not rec:
        return

    try:
        new_amount, new_note = split_amount_and_note(new_text)
    except Exception:
        send_and_auto_delete(chat_id, "❌ Ошибка формата.", 6)
        return

    old_amount = rec.get("amount", 0)

    # обновление записи
    rec["amount"] = new_amount
    rec["note"] = new_note

    # корректировка баланса
    store["balance"] = store.get("balance", 0) + new_amount - old_amount

    save_data(data)
    save_chat_json(chat_id)

    try:
        send_backup_to_channel(chat_id)
    except Exception as e:
        log_error(f"backup after edit({chat_id}): {e}")

    try:
        update_or_send_day_window(chat_id, day_key)
    except Exception as e:
        log_error(f"day window after edit({chat_id}): {e}")
        # ==========================================================
# SECTION 18 — Day window (main UI)
# ==========================================================

def format_day_total(store: dict, dk: str) -> str:
    """
    Итог дня.
    """
    daily = store.get("daily_records", {})
    arr = daily.get(dk, [])
    total = sum(r.get("amount", 0) for r in arr)
    return fmt_num(total)


def render_day_window(chat_id: int, dk: str) -> str:
    """
    Генерирует текст окна дня.
    """
    store = get_chat_store(chat_id)
    daily = store.get("daily_records", {})
    arr = daily.get(dk, [])

    date_str = dk.replace("-", ".")

    lines = [
        f"📅 <b>{date_str}</b>",
        f"💰 Баланс: <b>{fmt_num(store.get('balance', 0))}</b>",
        f"📊 За день: <b>{format_day_total(store, dk)}</b>",
        "",
    ]

    if not arr:
        lines.append("Нет записей.")
        return "\n".join(lines)

    # сортировка по времени
    arr_sorted = sorted(arr, key=lambda r: r.get("timestamp", ""))

    for r in arr_sorted:
        amount = fmt_num(r.get("amount", 0))
        note = r.get("note", "")
        rid = r.get("id")

        ts = r.get("timestamp", "")
        ts_short = ""
        if ts:
            try:
                t = datetime.fromisoformat(ts)
                ts_short = t.strftime("%H:%M")
            except:
                pass

        lines.append(
            f"<b>R{rid}</b> — {amount}  "
            f"{safe_html(note)}  "
            f"<i>{ts_short}</i>"
        )

    return "\n".join(lines)


def make_day_window_keyboard(chat_id: int, dk: str):
    """
    Кнопки: Назад / Вперёд / Изменить / Удалить / Сегодня
    """
    kb = types.InlineKeyboardMarkup()

    # prev/next
    kb.row(
        types.InlineKeyboardButton("⬅️", callback_data=f"prev:{dk}"),
        types.InlineKeyboardButton("➡️", callback_data=f"next:{dk}")
    )

    # изменить / удалить запись
    kb.row(
        types.InlineKeyboardButton("✍️ Редактировать", callback_data=f"edit_menu:{dk}"),
        types.InlineKeyboardButton("⭕ Удалить", callback_data=f"del_menu:{dk}")
    )

    # прыжок на сегодня
    kb.row(
        types.InlineKeyboardButton("📌 Сегодня", callback_data="today")
    )

    return kb


def update_or_send_day_window(chat_id: int, dk: str):
    """
    Обновляет существующее окно дня, если оно есть,
    или создаёт новое.
    """
    store = get_chat_store(chat_id)
    windows = store.setdefault("active_windows", {})
    msg_id = windows.get("day_window")

    text = render_day_window(chat_id, dk)
    kb = make_day_window_keyboard(chat_id, dk)

    if msg_id:
        try:
            bot.edit_message_text(
                text,
                chat_id=chat_id,
                message_id=msg_id,
                reply_markup=kb,
                parse_mode="HTML"
            )
            store["current_view_day"] = dk
            save_data(data)
            return
        except Exception as e:
            log_error(f"edit day window({chat_id}): {e}")

    # если не удалось обновить — создаём новое сообщение
    try:
        sent = bot.send_message(
            chat_id, text, reply_markup=kb, parse_mode="HTML"
        )
        windows["day_window"] = sent.message_id
        store["current_view_day"] = dk
        save_data(data)
    except Exception as e:
        log_error(f"send day window({chat_id}): {e}")


# ==========================================================
# SECTION 19 — Handling day navigation callbacks
# ==========================================================

def shift_day(dk: str, delta: int) -> str:
    """
    Сдвигает дату +- delta дней.
    """
    try:
        d = datetime.strptime(dk, "%Y-%m-%d").date()
        d2 = d + timedelta(days=delta)
        return d2.strftime("%Y-%m-%d")
    except:
        return today_key()


@bot.callback_query_handler(func=lambda c: c.data.startswith("prev:"))
def cb_prev_day(call):
    chat_id = call.message.chat.id
    old = call.data.split(":")[1]
    new = shift_day(old, -1)
    update_or_send_day_window(chat_id, new)
    bot.answer_callback_query(call.id)


@bot.callback_query_handler(func=lambda c: c.data.startswith("next:"))
def cb_next_day(call):
    chat_id = call.message.chat.id
    old = call.data.split(":")[1]
    new = shift_day(old, +1)
    update_or_send_day_window(chat_id, new)
    bot.answer_callback_query(call.id)


@bot.callback_query_handler(func=lambda c: c.data == "today")
def cb_today(call):
    chat_id = call.message.chat.id
    dk = today_key()
    update_or_send_day_window(chat_id, dk)
    bot.answer_callback_query(call.id)
    # ==========================================================
# SECTION 20 — Edit / Delete menus
# ==========================================================

def make_record_select_kb(chat_id: int, dk: str, mode: str):
    """
    Создаёт клавиатуру выбора записи:
    mode = "edit" или "delete"
    """
    store = get_chat_store(chat_id)
    daily = store.get("daily_records", {})
    arr = daily.get(dk, [])
    arr_sorted = sorted(arr, key=lambda r: r.get("timestamp", ""))

    kb = types.InlineKeyboardMarkup()

    if not arr_sorted:
        kb.row(types.InlineKeyboardButton("Назад", callback_data=f"back:{dk}"))
        return kb

    for r in arr_sorted:
        rid = r.get("id")
        amount = fmt_num(r.get("amount", 0))
        note = r.get("note", "")
        label = f"R{rid}: {amount} {note[:20]}"

        kb.row(
            types.InlineKeyboardButton(
                label,
                callback_data=f"{mode}:{dk}:{rid}"
            )
        )

    kb.row(
        types.InlineKeyboardButton("⬅️ Назад", callback_data=f"back:{dk}")
    )

    return kb


# ==========================================================
# Меню редактирования
# ==========================================================

@bot.callback_query_handler(func=lambda c: c.data.startswith("edit_menu:"))
def cb_edit_menu(call):
    chat_id = call.message.chat.id
    dk = call.data.split(":")[1]

    kb = make_record_select_kb(chat_id, dk, "edit")
    bot.edit_message_text(
        f"✍️ Выберите запись для редактирования ({dk.replace('-', '.')})",
        chat_id=chat_id,
        message_id=call.message.message_id,
        reply_markup=kb
    )
    bot.answer_callback_query(call.id)


@bot.callback_query_handler(func=lambda c: c.data.startswith("edit:"))
def cb_edit_record(call):
    chat_id = call.message.chat.id
    _, dk, rid_s = call.data.split(":")
    rid = int(rid_s)

    store = get_chat_store(chat_id)
    store["edit_wait"] = {
        "type": "edit",
        "day_key": dk,
        "rid": rid,
        "origin_msg_id": call.message.message_id
    }

    bot.edit_message_text(
        f"✍️ Введите новое значение для записи R{rid}:",
        chat_id=chat_id,
        message_id=call.message.message_id
    )
    bot.answer_callback_query(call.id)


# ==========================================================
# Меню удаления
# ==========================================================

@bot.callback_query_handler(func=lambda c: c.data.startswith("del_menu:"))
def cb_delete_menu(call):
    chat_id = call.message.chat.id
    dk = call.data.split(":")[1]

    kb = make_record_select_kb(chat_id, dk, "del")
    bot.edit_message_text(
        f"⭕ Выберите запись для удаления ({dk.replace('-', '.')})",
        chat_id=chat_id,
        message_id=call.message.message_id,
        reply_markup=kb
    )
    bot.answer_callback_query(call.id)


@bot.callback_query_handler(func=lambda c: c.data.startswith("del:"))
def cb_delete_record(call):
    chat_id = call.message.chat.id
    _, dk, rid_s = call.data.split(":")
    rid = int(rid_s)

    store = get_chat_store(chat_id)
    store["edit_wait"] = {
        "type": "delete_confirm",
        "day_key": dk,
        "rid": rid,
        "origin_msg_id": call.message.message_id
    }

    kb = types.InlineKeyboardMarkup()
    kb.row(
        types.InlineKeyboardButton("❗ Удалить", callback_data=f"del_yes:{dk}:{rid}"),
        types.InlineKeyboardButton("Отмена", callback_data=f"back:{dk}")
    )

    bot.edit_message_text(
        f"❗ Подтверждаете удаление записи R{rid}?",
        chat_id=chat_id,
        message_id=call.message.message_id,
        reply_markup=kb
    )
    bot.answer_callback_query(call.id)


@bot.callback_query_handler(func=lambda c: c.data.startswith("del_yes:"))
def cb_delete_yes(call):
    chat_id = call.message.chat.id
    _, dk, rid_s = call.data.split(":")
    rid = int(rid_s)

    delete_record(chat_id, dk, rid)
    update_or_send_day_window(chat_id, dk)
    bot.answer_callback_query(call.id)


# ==========================================================
# Кнопка "Назад" в меню выбора записей
# ==========================================================

@bot.callback_query_handler(func=lambda c: c.data.startswith("back:"))
def cb_back(call):
    chat_id = call.message.chat.id
    dk = call.data.split(":")[1]
    update_or_send_day_window(chat_id, dk)
    bot.answer_callback_query(call.id)
    # ==========================================================
# SECTION 21 — Text message handler
# ==========================================================

@bot.message_handler(commands=["start"])
def cmd_start(msg):
    chat_id = msg.chat.id
    update_chat_info_from_message(msg)

    text = (
        "👋 Бот запущен!\n"
        "Чтобы начать вести финансовые записи — отправьте /поехали\n"
        "И просто пишите суммы, например:\n"
        "  500 кафе\n"
        "  -200 такси\n"
        "  1.234,50 покупка"
    )

    bot.send_message(chat_id, text)
    try:
        send_backup_to_chat(chat_id)
    except:
        pass


@bot.message_handler(commands=["поехали"])
def cmd_go(msg):
    chat_id = msg.chat.id
    enable_finance_mode(chat_id)
    update_chat_info_from_message(msg)

    send_info(chat_id, "⚙️ Финансовый режим активирован!", delay=4)

    dk = today_key()
    update_or_send_day_window(chat_id, dk)

    try:
        send_backup_to_chat(chat_id)
    except:
        pass


# ==========================================================
# SECTION 22 — Handling edit_wait (edit mode)
# ==========================================================

def process_edit_wait(chat_id: int, text: str) -> bool:
    """
    Проверяет, есть ли ожидание редактирования / удаления.
    Возвращает True, если обработка выполнена.
    """
    store = get_chat_store(chat_id)
    ew = store.get("edit_wait")
    if not ew:
        return False

    ew_type = ew.get("type")
    dk = ew.get("day_key")
    rid = ew.get("rid")

    # Сбросим ожидание сразу
    store["edit_wait"] = None
    save_data(data)

    if ew_type == "edit":
        edit_record(chat_id, dk, rid, text)
        return True

    elif ew_type == "delete_confirm":
        # если пришёл текст — игнорируем (ждём кнопку)
        send_and_auto_delete(chat_id, "❗ Подтвердите удаление через кнопки.", 5)
        return True

    return False


# ==========================================================
# SECTION 23 — Main message handler
# ==========================================================

@bot.message_handler(func=lambda m: True, content_types=["text"])
def on_text(msg):
    chat_id = msg.chat.id
    text = msg.text.strip()

    update_chat_info_from_message(msg)
    handle_forward_if_needed(msg)

    # 1) проверка edit_wait
    if process_edit_wait(chat_id, text):
        return

    # 2) если в чате НЕ включён финансовый режим
    if not require_finance(chat_id):
        return

    # 3) если похоже на сумму — добавляем запись
    if looks_like_amount(text):
        add_record(chat_id, text)
        return

    # 4) иначе — просто информационное
    send_and_auto_delete(chat_id, "ℹ️ Чтобы добавить запись — напишите сумму.\nПример: 500 кафе", 6)
    # ==========================================================
# SECTION 24 — Media forwarding handler (universal)
# ==========================================================

@bot.message_handler(
    content_types=[
        "photo", "audio", "voice", "video", "document",
        "sticker", "animation", "video_note",
        "location", "venue", "contact", "poll"
    ]
)
def on_media(msg):
    """
    Медиа-сообщения пересылаются по forward_rules.
    """
    update_chat_info_from_message(msg)
    handle_forward_if_needed(msg)


# ==========================================================
# SECTION 25 — Keep-alive
# ==========================================================

def keep_alive():
    """
    Пингует сайт, чтобы Render не засыпал.
    """
    url = APP_URL
    if not url:
        return

    while True:
        try:
            requests.get(url, timeout=10)
        except:
            pass
        time.sleep(KEEP_ALIVE_INTERVAL_SECONDS)


# ==========================================================
# SECTION 26 — Flask webhook
# ==========================================================

@app.route("/" + BOT_TOKEN, methods=["POST"])
def webhook():
    try:
        update = telebot.types.Update.de_json(request.stream.read().decode("utf-8"))
        bot.process_new_updates([update])
    except Exception as e:
        log_error(f"webhook error: {e}")
    return "OK", 200


@app.route("/", methods=["GET"])
def index():
    return f"Bot running. Version: {VERSION}", 200


# ==========================================================
# SECTION 27 — Launch bot
# ==========================================================

def main():
    # 1. Восстановление глобальных файлов при необходимости
    try_restore_global_files()

    # 2. Запуск keep-alive
    if APP_URL:
        th = threading.Thread(target=keep_alive, daemon=True)
        th.start()

    # 3. Устанавливаем webhook
    url = f"{APP_URL}/{BOT_TOKEN}"
    try:
        bot.remove_webhook()
        time.sleep(1)
        bot.set_webhook(url=url)
        log_info(f"Webhook set: {url}")
    except Exception as e:
        log_error(f"set_webhook: {e}")

    # 4. Запуск Flask
    app.run(
        host="0.0.0.0",
        port=PORT,
        debug=False
    )


if __name__ == "__main__":
    main()
    