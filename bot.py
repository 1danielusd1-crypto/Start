# Code_022.6 не доработан доп. пересылка а🔁B
# • доп. пересылка а🔁B
# • ручное востановление
# • пересылка всех типов сообщений
# • вывод значений расширенный
# ==========================================================

# 🧭 Description: Code_022.1
#  • Full finance UI: day window, edit menu, /prev /next /view, 31-day calendar, reports
#  • Per-chat storage: data_<chat_id>.json, data_<chat_id>.csv, csv_meta_<chat_id>.json
#  • Backup & restore via Google Drive + backup Telegram channel
#  • Anonymous message forwarding between chats (forward_rules, owner-configurable)
#  • Finance mode must be enabled per chat via /поехали
#  • Keep-alive, webhook/Flask, daily window scheduler, auto backups
# ==========================================================

#🟠🟠🟠🟠🟠🟠🟠🟠🟠🟠
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

# --- Google Drive ---
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from googleapiclient.discovery import build
from google.oauth2 import service_account

#⚫️⚫️⚫️⚫️⚫️⚫️⚫️⚫️⚫️⚫️
# ========== SECTION 2 — Environment & globals ==========

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
OWNER_ID = os.getenv("OWNER_ID", "").strip()
BACKUP_CHAT_ID = os.getenv("BACKUP_CHAT_ID", "").strip()
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
GDRIVE_FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID", "").strip()
APP_URL = os.getenv("APP_URL", "").strip()
PORT = int(os.getenv("PORT", "8443"))

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set")

VERSION = "Code_022.6 доп. пересылка а🔁B"

DEFAULT_TZ = "America/Argentina/Buenos_Aires"
KEEP_ALIVE_INTERVAL_SECONDS = 60

DATA_FILE = "data.json"
CSV_FILE = "data.csv"
CSV_META_FILE = "csv_meta.json"

# Global flags (runtime, also duplicated into data["backup_flags"])
backup_flags = {
    "drive": True,
    "channel": True,
}

# ==========================================================
# RESTORE MODE FLAG
# ==========================================================

# В этом режиме пересылка документов полностью отключается,
# и бот использует документы ТОЛЬКО для восстановления data.json / data_<chat>.json / csv_meta / CSV.
restore_mode = False

# logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

bot = telebot.TeleBot(BOT_TOKEN, parse_mode=None)
app = Flask(__name__)

# main in-memory store
data = {}

# chats where finance mode is enabled
finance_active_chats = set()

# ==========================================================
# SECTION 3 — Helpers (time, logging)
# ==========================================================

def log_info(msg: str):
    logger.info(msg)


def log_error(msg: str):
    logger.error(msg)


def get_tz():
    """Return local timezone, with fallback to UTC-3."""
    try:
        return ZoneInfo(DEFAULT_TZ)
    except Exception:
        return timezone(timedelta(hours=-3))


def now_local():
    return datetime.now(get_tz())


def today_key() -> str:
    return now_local().strftime("%Y-%m-%d")


# ==========================================================
# SECTION 4 — JSON/CSV helpers
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


def default_data():
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


def load_data():
    d = _load_json(DATA_FILE, default_data())
    base = default_data()
    for k, v in base.items():
        if k not in d:
            d[k] = v
    # sync runtime flags from stored flags
    flags = d.get("backup_flags") or {}
    backup_flags["drive"] = bool(flags.get("drive", True))
    backup_flags["channel"] = bool(flags.get("channel", True))
    # restore finance_active_chats set
    fac = d.get("finance_active_chats") or {}
    finance_active_chats.clear()
    for cid, enabled in fac.items():
        if enabled:
            try:
                finance_active_chats.add(int(cid))
            except Exception:
                pass
    return d


def save_data(d):
    # mirror finance_active_chats set into dict
    fac = {}
    for cid in finance_active_chats:
        fac[str(cid)] = True
    d["finance_active_chats"] = fac
    d["backup_flags"] = {
        "drive": bool(backup_flags.get("drive", True)),
        "channel": bool(backup_flags.get("channel", True)),
    }
    _save_json(DATA_FILE, d)

#🟡🟡🟡🟡🟡🟡🟡🟡
# ==========================================================
# SECTION 5 — Per-chat storage helpers
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
    Добавлено поле "known_chats" для отображения названий/username в меню пересылки.
    """
    chats = data.setdefault("chats", {})

    store = chats.setdefault(
        str(chat_id),
        {
            "info": {},                 # информация о чате (название, username)
            "known_chats": {},          # словарь известных чатов (для владельца)
            "balance": 0,
            "records": [],
            "daily_records": {},
            "next_id": 1,
            "active_windows": {},
            "edit_wait": None,
            "edit_target": None,
            "current_view_day": today_key(),
        }
    )

    # гарантируем наличие known_chats после обновления бота
    if "known_chats" not in store:
        store["known_chats"] = {}

    return store


def save_chat_json(chat_id: int):
    """
    Save per-chat JSON, CSV and META for one chat.
    """
    try:
        store = data.get("chats", {}).get(str(chat_id), {})
        if not store:
            return

        chat_path_json = chat_json_file(chat_id)
        chat_path_csv = chat_csv_file(chat_id)
        chat_path_meta = chat_meta_file(chat_id)

        # ensure files exist
        for p in (chat_path_json, chat_path_csv, chat_path_meta):
            if not os.path.exists(p):
                with open(p, "a", encoding="utf-8"):
                    pass

        payload = {
            "chat_id": chat_id,
            "balance": store.get("balance", 0),
            "records": store.get("records", []),
            "daily_records": store.get("daily_records", {}),
            "next_id": store.get("next_id", 1),
            "info": store.get("info", {}),
            "known_chats": store.get("known_chats", {}),
        }

        _save_json(chat_path_json, payload)

        # CSV
        with open(chat_path_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["chat_id", "ID", "short_id", "timestamp", "amount", "note", "owner", "day_key"])
            for dk, recs in store.get("daily_records", {}).items():
                for r in recs:
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

        meta = {
            "last_saved": now_local().isoformat(timespec="seconds"),
            "record_count": sum(len(v) for v in store.get("daily_records", {}).values()),
        }
        _save_json(chat_path_meta, meta)

        log_info(f"Per-chat files saved for chat {chat_id}")

    except Exception as e:
        log_error(f"save_chat_json({chat_id}): {e}")

#🟣🟣🟣🟣🟣🟣🟣🟣🟣
# ==========================================================
# SECTION 6 — Number formatting & parsing (EU format, decimals)
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

    s = f"{x:.12f}".rstrip("0").rstrip(".")

    if "." in s:
        int_part, dec_part = s.split(".")
    else:
        int_part, dec_part = s, ""

    int_part = f"{int(int_part):,}".replace(",", ".")

    if dec_part:
        s = f"{int_part},{dec_part}"
    else:
        s = int_part

    return f"{sign}{s}"

    

# регулярка на первое число даже внутри слов
num_re = re.compile(r"[+\-–]?\s*\d[\d\s.,_'’]*")


def parse_amount(raw: str) -> float:
    """
    Универсальный парсер:
    - понимает любые разделители
    - смешанные форматы (1.234,56 / 1,234.56)
    - определяет десятичную часть по самому правому разделителю
    - число без знака = расход
    """

    s = raw.strip()

    # Определяем знак
    is_negative = s.startswith("-") or s.startswith("–")
    is_positive = s.startswith("+")

    # Убираем знак для разбора числа
    s_clean = s.lstrip("+-–").strip()

    # Удаляем мусор
    s_clean = (
        s_clean.replace(" ", "")
        .replace("_", "")
        .replace("’", "")
        .replace("'", "")
    )

    # Нет разделителей — просто число
    if "," not in s_clean and "." not in s_clean:
        value = float(s_clean)
        if not is_positive and not is_negative:
            is_negative = True
        return -value if is_negative else value

    # Оба разделителя: "." и ","
    if "." in s_clean and "," in s_clean:
        # самый правый — десятичный знак
        if s_clean.rfind(",") > s_clean.rfind("."):
            # 1.234,56 → запятая = десятичный
            s_clean = s_clean.replace(".", "")
            s_clean = s_clean.replace(",", ".")
        else:
            # 1,234.56 → точка = десятичный
            s_clean = s_clean.replace(",", "")
    else:
        # Один разделитель:
        # если справа 1 или 2 цифры → десятичный
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

    # число без знака → расход
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

    # Описание = весь текст без числа
    note = text.replace(raw_number, " ").strip()
    note = re.sub(r"\s+", " ", note).lower()

    return amount, note

# ==========================================================
# SECTION 7 — Google Drive helpers
# ==========================================================

def _get_drive_service():
    if not GOOGLE_SERVICE_ACCOUNT_JSON or not GDRIVE_FOLDER_ID:
        return None
    try:
        info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
        creds = service_account.Credentials.from_service_account_info(
            info,
            scopes=["https://www.googleapis.com/auth/drive"]
        )
        service = build("drive", "v3", credentials=creds)
        return service
    except Exception as e:
        log_error(f"Drive service error: {e}")
        return None


def upload_to_gdrive(path: str, mime_type: str = None, description: str | None = None):
    flags = backup_flags or {}
    if not flags.get("drive", True):
        log_info("GDrive backup disabled (drive flag = False).")
        return

    service = _get_drive_service()
    if service is None:
        return

    if not os.path.exists(path):
        log_error(f"upload_to_gdrive: file not found {path}")
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
            fields="files(id, name)",
        ).execute()
        items = existing.get("files", [])
        if items:
            file_id = items[0]["id"]
            service.files().update(
                fileId=file_id,
                media_body=media,
                body={"description": description or ""},
            ).execute()
            log_info(f"GDrive: updated {fname}, id={file_id}")
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
    service = _get_drive_service()
    if service is None:
        return False
    try:
        res = service.files().list(
            q=f"name = '{filename}' and '{GDRIVE_FOLDER_ID}' in parents and trashed = false",
            spaces="drive",
            fields="files(id, name, mimeType, size)",
        ).execute()
        items = res.get("files", [])
        if not items:
            log_info(f"GDrive: {filename} not found")
            return False
        file_id = items[0]["id"]
        request = service.files().get_media(fileId=file_id)
        fh = io.FileIO(dest_path, "wb")
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        log_info(f"GDrive: downloaded {filename} -> {dest_path}")
        return True
    except Exception as e:
        log_error(f"download_from_gdrive({filename}): {e}")
        return False


def restore_from_gdrive_if_needed() -> bool:
    """
    If local DATA_FILE/CSV_FILE/CSV_META_FILE are missing,
    try to restore them from Google Drive.
    """
    restored_any = False
    if not os.path.exists(DATA_FILE):
        if download_from_gdrive(os.path.basename(DATA_FILE), DATA_FILE):
            restored_any = True
    if not os.path.exists(CSV_FILE):
        if download_from_gdrive(os.path.basename(CSV_FILE), CSV_FILE):
            restored_any = True
    if not os.path.exists(CSV_META_FILE):
        if download_from_gdrive(os.path.basename(CSV_META_FILE), CSV_META_FILE):
            restored_any = True

    if restored_any:
        log_info("Data restored from Google Drive.")
    else:
        log_info("GDrive restore: nothing to restore.")
    return restored_any
    
    
    
    # ==========================================================
# SECTION 8 — Global CSV export & backup to channel
# ==========================================================

def export_global_csv(d: dict):
    """Legacy global CSV with all chats (for backup channel)."""
    try:
        with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["chat_id", "ID", "short_id", "timestamp", "amount", "note", "owner", "day_key"])
            for cid, cdata in d.get("chats", {}).items():
                for dk, records in cdata.get("daily_records", {}).items():
                    for r in records:
                        w.writerow([
                            cid,
                            r.get("id"),
                            r.get("short_id"),
                            r.get("timestamp"),
                            r.get("amount"),
                            r.get("note"),
                            r.get("owner"),
                            dk,
                        ])
    except Exception as e:
        log_error(f"export_global_csv: {e}")


def send_backup_to_channel_for_file(base_path: str, meta_key_prefix: str):
    """Helper to send or update a file in BACKUP_CHAT_ID with csv_meta tracking."""
    if not BACKUP_CHAT_ID:
        return
    if not os.path.exists(base_path):
        return

    try:
        meta = _load_csv_meta()
        msg_key = f"msg_{meta_key_prefix}"
        ts_key = f"timestamp_{meta_key_prefix}"
        with open(base_path, "rb") as f:
            caption = f"📦 {os.path.basename(base_path)} — {now_local().strftime('%Y-%m-%d %H:%M')}"
            if meta.get(msg_key):
                try:
                    bot.edit_message_media(
                        chat_id=int(BACKUP_CHAT_ID),
                        message_id=meta[msg_key],
                        media=telebot.types.InputMediaDocument(f, caption=caption),
                    )
                    log_info(f"Channel file updated: {base_path}")
                except Exception as e:
                    log_error(f"edit_message_media {base_path}: {e}")
                    sent = bot.send_document(int(BACKUP_CHAT_ID), f, caption=caption)
                    meta[msg_key] = sent.message_id
            else:
                sent = bot.send_document(int(BACKUP_CHAT_ID), f, caption=caption)
                meta[msg_key] = sent.message_id
        meta[ts_key] = now_local().isoformat(timespec="seconds")
        _save_csv_meta(meta)
    except Exception as e:
        log_error(f"send_backup_to_channel_for_file({base_path}): {e}")


def send_backup_to_channel(chat_id: int):
    """
    Send per-chat JSON/CSV and optionally global CSV to BACKUP_CHAT_ID,
    respecting channel backup flag.
    """
    flags = backup_flags or {}
    if not flags.get("channel", True):
        log_info("Channel backup disabled (channel flag = False).")
        return
    if not BACKUP_CHAT_ID:
        log_info("BACKUP_CHAT_ID not set, skipping backup to channel.")
        return

    try:
        # ensure per-chat files are fresh
        save_chat_json(chat_id)
        send_backup_to_channel_for_file(chat_json_file(chat_id), f"json_chat_{chat_id}")
        send_backup_to_channel_for_file(chat_csv_file(chat_id), f"csv_chat_{chat_id}")

        # optional: update global CSV snapshot
        export_global_csv(data)
        send_backup_to_channel_for_file(CSV_FILE, "csv_global")
        if os.path.exists("csv_meta.json"):
            send_backup_to_channel_for_file("csv_meta.json", "csv_meta")

    except Exception as e:
        log_error(f"send_backup_to_channel({chat_id}): {e}")

#🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢🟢
# ==========================================================
# SECTION 9 — Forward rules persistence (owner file)
# ==========================================================

def _owner_data_file() -> str | None:
    """
    Файл владельца, где хранится forward_rules.
    """
    if not OWNER_ID:
        return None
    try:
        return f"data_{int(OWNER_ID)}.json"
    except Exception:
        return None


def load_forward_rules():
    """
    Загружает forward_rules из файла владельца.
    Поддерживает старый формат (списки) и новый (словарь).
    """
    try:
        path = _owner_data_file()
        if not path or not os.path.exists(path):
            return {}

        payload = _load_json(path, {}) or {}
        fr = payload.get("forward_rules", {})

        upgraded = {}

        for src, value in fr.items():
            if isinstance(value, list):
                upgraded[src] = {}
                for dst in value:
                    upgraded[src][dst] = "oneway_to"
            elif isinstance(value, dict):
                upgraded[src] = value
            else:
                continue

        return upgraded
    except Exception as e:
        log_error(f"load_forward_rules: {e}")
        return {}


def persist_forward_rules_to_owner():
    """
    Сохраняет forward_rules (в НОВОМ формате) только в data_OWNER.json.
    """
    try:
        path = _owner_data_file()
        if not path:
            return

        payload = {}
        if os.path.exists(path):
            payload = _load_json(path, {})
            if not isinstance(payload, dict):
                payload = {}

        payload["forward_rules"] = data.get("forward_rules", {})

        _save_json(path, payload)
        log_info(f"forward_rules persisted to {path}")

    except Exception as e:
        log_error(f"persist_forward_rules_to_owner: {e}")

#✅✅✅✅✅✅
# ==========================================================
# SECTION 10 — Работа с forward_rules (логика пересылки)
# ==========================================================
# ==========================================================
# SECTION 10 — Общая логика forward_rules (для обеих систем)
# ==========================================================

def resolve_forward_targets(source_chat_id: int):
    fr = data.get("forward_rules", {})
    src = str(source_chat_id)
    if src not in fr:
        return []
    out = []
    for dst, mode in fr[src].items():
        try:
            out.append((int(dst), mode))
        except:
            continue
    return out


def add_forward_link(src_chat_id: int, dst_chat_id: int, mode: str):
    fr = data.setdefault("forward_rules", {})
    src = str(src_chat_id)
    dst = str(dst_chat_id)
    fr.setdefault(src, {})[dst] = mode
    save_data(data)


def remove_forward_link(src_chat_id: int, dst_chat_id: int):
    fr = data.get("forward_rules", {})
    src = str(src_chat_id)
    dst = str(dst_chat_id)
    if src in fr and dst in fr[src]:
        del fr[src][dst]
    if src in fr and not fr[src]:
        del fr[src]
    save_data(data)

def clear_forward_all():
    """Полностью отключает всю пересылку."""
    data["forward_rules"] = {}
    persist_forward_rules_to_owner()
    save_data(data)


# ----------------------------------------------------------
#   ФУНКЦИИ АНOНИМНОЙ ПЕРЕСЫЛКИ
# ----------------------------------------------------------

def forward_text_anon(source_chat_id: int, msg, targets: list[tuple[int, str]]):
    """Анонимная пересылка текста."""
    for dst, mode in targets:
        try:
            bot.copy_message(dst, source_chat_id, msg.message_id)
        except Exception as e:
            log_error(f"forward_text_anon to {dst}: {e}")


def forward_media_anon(source_chat_id: int, msg, targets: list[tuple[int, str]]):
    """Анонимная пересылка любых медиа."""
    for dst, mode in targets:
        try:
            bot.copy_message(dst, source_chat_id, msg.message_id)
        except Exception as e:
            log_error(f"forward_media_anon to {dst}: {e}")


# ----------------------------------------------------------
#   ПОДДЕРЖКА MEDIA GROUP (альбомов)
# ----------------------------------------------------------

_media_group_cache = {}  # { chat_id : { group_id : [messages...] } }


def collect_media_group(chat_id: int, msg):
    """
    Собирает альбом (media_group) в кэш пока все элементы не пришли.
    """
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


def forward_media_group_anon(source_chat_id: int, messages: list, targets: list[tuple[int, str]]):
    """
    Пересылка собранного альбома анонимно.
    """
    if not messages:
        return

    media_list = []
    for msg in messages:
        if msg.content_type == "photo":
            file_id = msg.photo[-1].file_id
            caption = msg.caption or None
            media_list.append(InputMediaPhoto(file_id, caption=caption))

        elif msg.content_type == "video":
            file_id = msg.video.file_id
            caption = msg.caption or None
            media_list.append(InputMediaVideo(file_id, caption=caption))

        elif msg.content_type == "document":
            file_id = msg.document.file_id
            caption = msg.caption or None
            media_list.append(InputMediaDocument(file_id, caption=caption))

        elif msg.content_type == "audio":
            file_id = msg.audio.file_id
            caption = msg.caption or None
            media_list.append(InputMediaAudio(file_id, caption=caption))

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
# SECTION 11 — Day window renderer (версия код-010)
# ==========================================================

def render_day_window(chat_id: int, day_key: str):
    """
    Рендер окна дня.
    """
    store = get_chat_store(chat_id)
    recs = store.get("daily_records", {}).get(day_key, [])
    lines = []

    lines.append(f"📅 <b>{day_key}</b>")
    lines.append("")

    total = 0

    recs_sorted = sorted(recs, key=lambda x: x.get("timestamp"))

    for r in recs_sorted:
        amt = r["amount"]
        total += amt
        #sign = "+" if amt >= 0 else "-"

        note = html.escape(r.get("note", ""))
        sid = r.get("short_id", f"R{r['id']}")

        lines.append(f"{sid} {fmt_num(amt)} <i>{note}</i>")
        
    if not recs_sorted:
        lines.append("Нет записей за этот день.")

    lines.append("")
    lines.append(f"💰 <b>Итого:{fmt_num(total)}</b>")

    return "\n".join(lines), total

#💠💠💠💠💠💠💠💠
# ==========================================================
# SECTION 12 — Keyboards: main window, calendar, edit menu, forwarding
# ==========================================================

def build_main_keyboard(day_key: str, chat_id=None):
    kb = types.InlineKeyboardMarkup(row_width=2)

    kb.row(
        types.InlineKeyboardButton("➕ Добавить", callback_data=f"d:{day_key}:add"),
        types.InlineKeyboardButton("📝 Редактировать", callback_data=f"d:{day_key}:edit_menu")
    )

    kb.row(
        types.InlineKeyboardButton("⬅️ Вчера", callback_data=f"d:{day_key}:prev"),
        types.InlineKeyboardButton("➡️ Завтра", callback_data=f"d:{day_key}:next")
    )

    kb.row(
        types.InlineKeyboardButton("📅 Календарь", callback_data=f"d:{day_key}:calendar"),
        types.InlineKeyboardButton("📊 Отчёт", callback_data=f"d:{day_key}:report")
    )

    kb.row(
        types.InlineKeyboardButton("ℹ️ Инфо", callback_data=f"d:{day_key}:info"),
        types.InlineKeyboardButton("💰 Общий итог", callback_data=f"d:{day_key}:total")
    )

    return kb


def build_calendar_keyboard(center_day: datetime):
    kb = types.InlineKeyboardMarkup(row_width=4)

    start_day = center_day - timedelta(days=15)
    for week in range(0, 32, 4):
        row = []
        for d in range(4):
            day = start_day + timedelta(days=week + d)
            label = day.strftime("%d.%m")
            key = day.strftime("%Y-%m-%d")
            row.append(types.InlineKeyboardButton(label, callback_data=f"d:{key}:open"))
        kb.row(*row)

    kb.row(
        types.InlineKeyboardButton("⬅️ −31", callback_data=f"c:{(center_day - timedelta(days=31)).strftime('%Y-%m-%d')}"),
        types.InlineKeyboardButton("➡️ +31", callback_data=f"c:{(center_day + timedelta(days=31)).strftime('%Y-%m-%d')}")
    )

    kb.row(
        types.InlineKeyboardButton("📅 Сегодня", callback_data=f"d:{today_key()}:open")
    )

    return kb
    
    
    
    # ==========================================================
# МЕНЮ РЕДАКТИРОВАНИЯ (с кнопкой пересылки)
# ==========================================================

def build_edit_menu_keyboard(day_key: str, chat_id=None):
    kb = types.InlineKeyboardMarkup(row_width=2)

    kb.row(
        types.InlineKeyboardButton("📝 Редактировать запись", callback_data=f"d:{day_key}:edit_list"),
        types.InlineKeyboardButton("📂 Общий CSV", callback_data=f"d:{day_key}:csv_all")
    )

    kb.row(
        types.InlineKeyboardButton("📅 CSV за день", callback_data=f"d:{day_key}:csv_day"),
        types.InlineKeyboardButton("⚙️ Обнулить", callback_data=f"d:{day_key}:reset")
    )

    if OWNER_ID and str(chat_id) == str(OWNER_ID):
        kb.row(
            types.InlineKeyboardButton("🔁 Пересылка ↔️", callback_data=f"d:{day_key}:forward_menu")
        )
    kb.row(
        types.InlineKeyboardButton("🔀 Пересылка A ↔ B", callback_data="fw_open")
    )
    kb.row(
        types.InlineKeyboardButton("📅 Сегодня", callback_data=f"d:{today_key()}:open"),
        types.InlineKeyboardButton("📆 Выбрать день", callback_data=f"d:{day_key}:pick_date")
    )

    kb.row(
        types.InlineKeyboardButton("ℹ️ Инфо", callback_data=f"d:{day_key}:info"),
        types.InlineKeyboardButton("🔙 Назад", callback_data=f"d:{day_key}:back_main")
    )

    return kb

def build_forward_chat_list(day_key: str, chat_id: int):
    """
    Меню выбора чата для пересылки.
    Теперь список берём из known_chats владельца (все чаты, где был бот).
    """
    kb = types.InlineKeyboardMarkup()

    if not OWNER_ID:
        return kb

    # берем ВСЕ чаты, где бот видел сообщения
    owner_store = get_chat_store(int(OWNER_ID))
    known = owner_store.get("known_chats", {})

    rules = data.get("forward_rules", {})

    for cid, info in known.items():
        try:
            int_cid = int(cid)
        except:
            continue

        title = info.get("title") or f"Чат {cid}"
        cur_mode = rules.get(str(chat_id), {}).get(cid)

        if cur_mode == "oneway_to":
            label = f"{title} ➡️"
        elif cur_mode == "oneway_from":
            label = f"{title} ⬅️"
        elif cur_mode == "twoway":
            label = f"{title} ↔️"
        else:
            label = f"{title}"

        kb.row(
            types.InlineKeyboardButton(
                label,
                callback_data=f"d:{day_key}:fw_cfg_{cid}"
            )
        )

    kb.row(
        types.InlineKeyboardButton("🔙 Назад", callback_data=f"d:{day_key}:edit_menu")
    )
    return kb

def build_forward_direction_menu(day_key: str, owner_chat: int, target_chat: int):
    """
    Меню направлений:
        ➡️ owner → target
        ⬅️ target → owner
        ↔️ двусторонняя
        ❌ удалить
        🔙 назад
    """
    kb = types.InlineKeyboardMarkup(row_width=1)

    kb.row(
        types.InlineKeyboardButton(
            f"➡️ В одну сторону (от {owner_chat} → {target_chat})",
            callback_data=f"d:{day_key}:fw_one_{target_chat}"
        )
    )

    kb.row(
        types.InlineKeyboardButton(
            f"⬅️ В обратную ({target_chat} → {owner_chat})",
            callback_data=f"d:{day_key}:fw_rev_{target_chat}"
        )
    )

    kb.row(
        types.InlineKeyboardButton(
            "↔️ Двусторонняя пересылка",
            callback_data=f"d:{day_key}:fw_two_{target_chat}"
        )
    )

    kb.row(
        types.InlineKeyboardButton(
            "❌ Удалить все связи",
            callback_data=f"d:{day_key}:fw_del_{target_chat}"
        )
    )

    kb.row(
        types.InlineKeyboardButton(
            "🔙 Назад",
            callback_data=f"d:{day_key}:forward_menu"
        )
    )

    return kb
# ==========================================================
# SECTION 12.1 — NEW FORWARD SYSTEM (Chat A ↔ B)
# ==========================================================

def build_forward_source_menu():
    """
    Меню выбора чата A (источник пересылки).
    Использует known_chats владельца.
    """
    kb = types.InlineKeyboardMarkup()

    if not OWNER_ID:
        return kb

    owner_store = get_chat_store(int(OWNER_ID))
    known = owner_store.get("known_chats", {})

    for cid, ch in known.items():
        title = ch.get("title") or f"Чат {cid}"
        kb.row(
            types.InlineKeyboardButton(
                title,
                callback_data=f"fw_src:{cid}"
            )
        )

    # Назад → возврат в меню редактирования
    kb.row(
        types.InlineKeyboardButton("🔙 Назад", callback_data="fw_back_root")
    )

    return kb


def build_forward_target_menu(src_id: int):
    """
    Меню выбора чата B (получатель пересылки) для уже выбранного A.
    """
    kb = types.InlineKeyboardMarkup()

    if not OWNER_ID:
        return kb

    owner_store = get_chat_store(int(OWNER_ID))
    known = owner_store.get("known_chats", {})

    for cid, ch in known.items():
        try:
            int_cid = int(cid)
        except Exception:
            continue

        if int_cid == src_id:
            continue

        title = ch.get("title") or f"Чат {cid}"
        kb.row(
            types.InlineKeyboardButton(
                title,
                callback_data=f"fw_tgt:{src_id}:{cid}"
            )
        )

    # Назад → обратно к выбору A
    kb.row(
        types.InlineKeyboardButton("🔙 Назад", callback_data="fw_back_src")
    )

    return kb


def build_forward_mode_menu(A: int, B: int):
    """
    Меню выбора режима пересылки между чатами A и B:
        ➡️ A → B
        ⬅️ B → A
        ↔️ двусторонняя
        ❌ удалить связь
        🔙 назад (к выбору B)
    """
    kb = types.InlineKeyboardMarkup()

    kb.row(
        types.InlineKeyboardButton(
            f"➡️ {A} → {B}",
            callback_data=f"fw_mode:{A}:{B}:to"
        )
    )
    kb.row(
        types.InlineKeyboardButton(
            f"⬅️ {B} → {A}",
            callback_data=f"fw_mode:{A}:{B}:from"
        )
    )
    kb.row(
        types.InlineKeyboardButton(
            f"↔️ {A} ⇄ {B}",
            callback_data=f"fw_mode:{A}:{B}:two"
        )
    )
    kb.row(
        types.InlineKeyboardButton(
            "❌ Удалить связь A-B",
            callback_data=f"fw_mode:{A}:{B}:del"
        )
    )

    # Назад → обратно к выбору B для A
    kb.row(
        types.InlineKeyboardButton(
            "🔙 Назад",
            callback_data=f"fw_back_tgt:{A}"
        )
    )

    return kb


def apply_forward_mode(A: int, B: int, mode: str):
    """
    Применяет выбранный режим пересылки между чатами A и B.
    Использует общие функции add_forward_link / remove_forward_link.
    """
    if mode == "to":
        # только A → B
        add_forward_link(A, B, "oneway_to")
        remove_forward_link(B, A)

    elif mode == "from":
        # только B → A
        add_forward_link(B, A, "oneway_to")
        remove_forward_link(A, B)

    elif mode == "two":
        # двусторонняя пересылка
        add_forward_link(A, B, "twoway")
        add_forward_link(B, A, "twoway")

    elif mode == "del":
        # полностью удалить связь (в обе стороны)
        remove_forward_link(A, B)
        remove_forward_link(B, A)
        
#🟠🟠🟠🟠🟠🟠🟠🟠🟠
#🟠🟠🟠🟠🟠🟠🟠🟠🟠
# ==========================================================
# SECTION 16 — Callback handler
# ==========================================================

@bot.callback_query_handler(func=lambda c: True)
def on_callback(call):
    """
    Универсальный обработчик всех callback_data:
      • fw_*  — новое меню пересылки A ↔ B (только для владельца)
      • c:*   — календарь
      • d:*   — команды окна дня, редактирование, старое меню пересылки
    """
    try:
        data_str = call.data or ""
        chat_id = call.message.chat.id

        # --------------------------------------------------
        # 1) NEW FORWARD SYSTEM — все callback-и fw_*
        # --------------------------------------------------
        if data_str.startswith("fw_"):
            # меню пересылки доступно только владельцу
            if not OWNER_ID or str(chat_id) != str(OWNER_ID):
                try:
                    bot.answer_callback_query(
                        call.id,
                        "Меню пересылки доступно только владельцу.",
                        show_alert=True
                    )
                except Exception:
                    pass
                return

            # открыть выбор чата A
            if data_str == "fw_open":
                kb = build_forward_source_menu()
                bot.edit_message_text(
                    "Выберите чат A:",
                    chat_id=chat_id,
                    message_id=call.message.message_id,
                    reply_markup=kb
                )
                return

            # назад из выбора A → обратно в меню редактирования
            if data_str == "fw_back_root":
                owner_store = get_chat_store(int(OWNER_ID))
                day_key = owner_store.get("current_view_day", today_key())

                kb = build_edit_menu_keyboard(day_key, chat_id)
                try:
                    bot.edit_message_text(
                        f"Меню редактирования для {day_key}:",
                        chat_id=chat_id,
                        message_id=call.message.message_id,
                        reply_markup=kb
                    )
                except Exception:
                    bot.send_message(
                        chat_id,
                        f"Меню редактирования для {day_key}:",
                        reply_markup=kb
                    )
                return

            # назад из выбора B → снова выбор A
            if data_str == "fw_back_src":
                kb = build_forward_source_menu()
                bot.edit_message_text(
                    "Выберите чат A:",
                    chat_id=chat_id,
                    message_id=call.message.message_id,
                    reply_markup=kb
                )
                return

            # назад из выбора режима → снова выбор B для A
            if data_str.startswith("fw_back_tgt:"):
                try:
                    A = int(data_str.split(":", 1)[1])
                except Exception:
                    return
                kb = build_forward_target_menu(A)
                bot.edit_message_text(
                    f"Источник пересылки: {A}\nВыберите чат B:",
                    chat_id=chat_id,
                    message_id=call.message.message_id,
                    reply_markup=kb
                )
                return

            # выбор чата A
            if data_str.startswith("fw_src:"):
                try:
                    A = int(data_str.split(":", 1)[1])
                except Exception:
                    return
                kb = build_forward_target_menu(A)
                bot.edit_message_text(
                    f"Источник пересылки: {A}\nВыберите чат B:",
                    chat_id=chat_id,
                    message_id=call.message.message_id,
                    reply_markup=kb
                )
                return

            # выбор чата B для A
            if data_str.startswith("fw_tgt:"):
                parts = data_str.split(":")
                if len(parts) != 3:
                    return
                _, A_str, B_str = parts
                try:
                    A = int(A_str)
                    B = int(B_str)
                except Exception:
                    return

                kb = build_forward_mode_menu(A, B)
                bot.edit_message_text(
                    f"Настройка пересылки: {A} ⇄ {B}",
                    chat_id=chat_id,
                    message_id=call.message.message_id,
                    reply_markup=kb
                )
                return

            # выбор режима пересылки между A и B
            if data_str.startswith("fw_mode:"):
                parts = data_str.split(":")
                if len(parts) != 4:
                    return
                _, A_str, B_str, mode = parts
                try:
                    A = int(A_str)
                    B = int(B_str)
                except Exception:
                    return

                apply_forward_mode(A, B, mode)
                kb = build_forward_source_menu()
                bot.edit_message_text(
                    "Маршрут обновлён.\nВыберите чат A:",
                    chat_id=chat_id,
                    message_id=call.message.message_id,
                    reply_markup=kb
                )
                return

            # на всякий случай
            return

        # --------------------------------------------------
        # 2) КАЛЕНДАРЬ (c:YYYY-MM-DD)
        # --------------------------------------------------
        if data_str.startswith("c:"):
            center = data_str[2:]
            try:
                center_dt = datetime.strptime(center, "%Y-%m-%d")
            except ValueError:
                return

            kb = build_calendar_keyboard(center_dt)
            try:
                bot.edit_message_reply_markup(
                    chat_id=chat_id,
                    message_id=call.message.message_id,
                    reply_markup=kb
                )
            except Exception:
                pass
            return

        # --------------------------------------------------
        # 3) ОКНО ДНЯ / РЕДАКТИРОВАНИЕ / СТАРОЕ МЕНЮ ПЕРЕСЫЛКИ
        # --------------------------------------------------
        if not data_str.startswith("d:"):
            return

        _, day_key, cmd = data_str.split(":", 2)
        store = get_chat_store(chat_id)

        # открытие конкретного дня
        if cmd == "open":
            txt, _ = render_day_window(chat_id, day_key)
            kb = build_main_keyboard(day_key, chat_id)

            store["current_view_day"] = day_key

            bot.edit_message_text(
                txt,
                chat_id=chat_id,
                message_id=call.message.message_id,
                reply_markup=kb,
                parse_mode="HTML"
            )
            set_active_window_id(chat_id, day_key, call.message.message_id)
            return

        # предыдущий день
        if cmd == "prev":
            d = datetime.strptime(day_key, "%Y-%m-%d") - timedelta(days=1)
            nd = d.strftime("%Y-%m-%d")
            txt, _ = render_day_window(chat_id, nd)
            kb = build_main_keyboard(nd, chat_id)

            store["current_view_day"] = nd

            bot.edit_message_text(
                txt,
                chat_id,
                call.message.message_id,
                reply_markup=kb,
                parse_mode="HTML"
            )
            set_active_window_id(chat_id, nd, call.message.message_id)
            return

        # следующий день
        if cmd == "next":
            d = datetime.strptime(day_key, "%Y-%m-%d") + timedelta(days=1)
            nd = d.strftime("%Y-%m-%d")
            txt, _ = render_day_window(chat_id, nd)
            kb = build_main_keyboard(nd, chat_id)

            store["current_view_day"] = nd

            bot.edit_message_text(
                txt,
                chat_id,
                call.message.message_id,
                reply_markup=kb,
                parse_mode="HTML"
            )
            set_active_window_id(chat_id, nd, call.message.message_id)
            return

        # показать календарь
        if cmd == "calendar":
            try:
                cdt = datetime.strptime(day_key, "%Y-%m-%d")
            except Exception:
                cdt = now_local()

            kb = build_calendar_keyboard(cdt)
            bot.edit_message_reply_markup(
                chat_id=chat_id,
                message_id=call.message.message_id,
                reply_markup=kb
            )
            return

        # отчёт по дням
        if cmd == "report":
            lines = ["📊 Отчёт:"]
            for dk, recs in sorted(store.get("daily_records", {}).items()):
                s = sum(r["amount"] for r in recs)
                lines.append(f"{dk}: {fmt_num(s)}")
            bot.send_message(chat_id, "\n".join(lines))
            return

        # общий итог
        if cmd == "total":
            chat_bal = store.get("balance", 0)
            overall = data.get("overall_balance", 0)
            bot.send_message(
                chat_id,
                f"💰 <b>Общий итог</b>\n\n"
                f"• По этому чату: <b>{fmt_num(chat_bal)}</b>\n"
                f"• По всем чатам: <b>{fmt_num(overall)}</b>",
                parse_mode="HTML"
            )
            return

        # справка
        if cmd == "info":
            try:
                bot.answer_callback_query(call.id)
            except Exception:
                pass

            info_text = (
                f"ℹ️ Финансовый бот — версия {VERSION}\n\n"
                "Команды:\n"
                "/поехали — включить финансовый режим в чате\n"
                "/start — открыть окно дня\n"
                "/view YYYY-MM-DD — открыть день\n"
                "/prev /next — навигация\n"
                "/balance — баланс\n"
                "/report — отчёт\n"
                "/csv — экспорт CSV (Drive+канал)\n"
                "/json — выгрузка JSON\n"
                "/reset — обнулить данные\n"
                "/ping — проверка\n"
                "/backup_gdrive_on / off — включить/выключить GDrive\n"
                "/backup_channel_on / off — включить/выключить бэкап в канал\n"
                "/stopforward — отключить пересылку\n"
                "/restore / /restore_off — режим восстановления\n"
            )
            bot.send_message(chat_id, info_text)
            return

        # меню редактирования
        if cmd == "edit_menu":
            store["current_view_day"] = day_key
            kb = build_edit_menu_keyboard(day_key, chat_id)
            bot.edit_message_reply_markup(
                chat_id=chat_id,
                message_id=call.message.message_id,
                reply_markup=kb
            )
            return

        # назад к основному окну дня
        if cmd == "back_main":
            store["current_view_day"] = day_key
            txt, _ = render_day_window(chat_id, day_key)
            kb = build_main_keyboard(day_key, chat_id)
            bot.edit_message_text(
                txt,
                chat_id=chat_id,
                message_id=call.message.message_id,
                reply_markup=kb,
                parse_mode="HTML"
            )
            return

        # общий CSV
        if cmd == "csv_all":
            cmd_csv_all(chat_id)
            return

        # CSV за день
        if cmd == "csv_day":
            cmd_csv_day(chat_id, day_key)
            return

        # добавление записи
        if cmd == "add":
            store["edit_wait"] = {"type": "add", "day_key": day_key}
            save_data(data)
            bot.send_message(chat_id, "Введите сумму и комментарий: +500 Пример")
            return

        # список записей для редактирования
        if cmd == "edit_list":
            day_recs = store.get("daily_records", {}).get(day_key, [])
            if not day_recs:
                bot.send_message(chat_id, "Нет записей за этот день.")
                return

            kb2 = types.InlineKeyboardMarkup()
            for r in day_recs:
                lbl = f"{r['short_id']}: {fmt_num(r['amount'])} — {r.get('note','')}"
                kb2.row(
                    types.InlineKeyboardButton(
                        lbl,
                        callback_data=f"d:{day_key}:edit_rec_{r['id']}"
                    )
                )

            kb2.row(
                types.InlineKeyboardButton("🔙 Назад", callback_data=f"d:{day_key}:edit_menu")
            )

            bot.send_message(chat_id, "Выберите запись:", reply_markup=kb2)
            return

        # выбор конкретной записи для редактирования
        if cmd.startswith("edit_rec_"):
            rid = int(cmd.split("_")[-1])
            store["edit_wait"] = {"type": "edit", "day_key": day_key, "rid": rid}
            save_data(data)
            bot.send_message(chat_id, f"Введите новую сумму и текст для записи R{rid}:")
            return

        # СТАРОЕ МЕНЮ ПЕРЕСЫЛКИ (на базе day_key)
        if cmd == "forward_menu":
            if not OWNER_ID or str(chat_id) != str(OWNER_ID):
                bot.send_message(chat_id, "Меню доступно только владельцу.")
                return

            kb = build_forward_chat_list(day_key, chat_id)
            bot.edit_message_text(
                "Выберите чат, для которого хотите настроить пересылку:",
                chat_id=chat_id,
                message_id=call.message.message_id,
                reply_markup=kb
            )
            return

        if cmd.startswith("fw_cfg_"):
            tgt = int(cmd.split("_")[-1])
            kb = build_forward_direction_menu(day_key, chat_id, tgt)
            bot.edit_message_text(
                f"Настройка пересылки для чата {tgt}:",
                chat_id=chat_id,
                message_id=call.message.message_id,
                reply_markup=kb
            )
            return

        if cmd.startswith("fw_one_"):
            tgt = int(cmd.split("_")[-1])
            add_forward_link(chat_id, tgt, "oneway_to")
            bot.send_message(chat_id, f"Установлена пересылка ➡️  {chat_id} → {tgt}")
            return

        if cmd.startswith("fw_rev_"):
            tgt = int(cmd.split("_")[-1])
            add_forward_link(tgt, chat_id, "oneway_to")
            add_forward_link(chat_id, tgt, "oneway_from")
            bot.send_message(chat_id, f"Установлена пересылка ⬅️  {tgt} → {chat_id}")
            return

        if cmd.startswith("fw_two_"):
            tgt = int(cmd.split("_")[-1])
            add_forward_link(chat_id, tgt, "twoway")
            add_forward_link(tgt, chat_id, "twoway")
            bot.send_message(chat_id, f"Установлена двусторонняя пересылка ↔️  {chat_id} ⇄ {tgt}")
            return

        if cmd.startswith("fw_del_"):
            tgt = int(cmd.split("_")[-1])
            remove_forward_link(chat_id, tgt)
            remove_forward_link(tgt, chat_id)
            bot.send_message(chat_id, f"Все связи с {tgt} удалены.")
            return

        # выбор даты вручную
        if cmd == "pick_date":
            bot.send_message(chat_id, "Введите дату:\n/view YYYY-MM-DD")
            return

    except Exception as e:
        log_error(f"on_callback error: {e}")
        

# ==========================================================
# SECTION 13 — Add / Update / Delete (версия код-010)
# ==========================================================

def add_record_to_chat(chat_id: int, amount: int, note: str, owner):
    store = get_chat_store(chat_id)

    rid = store.get("next_id", 1)
    rec = {
        "id": rid,
        "short_id": f"R{rid}",
        "timestamp": now_local().isoformat(timespec="seconds"),
        "amount": amount,
        "note": note,
        "owner": owner,
        "msg_id": msg.message_id,   # ← ДОБАВЛЕНО
        "origin_msg_id": msg.message_id,  # FIX VARIANT 3
    }

    data.setdefault("records", []).append(rec)

    store.setdefault("records", []).append(rec)
    store.setdefault("daily_records", {}).setdefault(today_key(), []).append(rec)

    store["balance"] = sum(x["amount"] for x in store["records"])
    data["overall_balance"] = sum(x["amount"] for x in data["records"])
    store["next_id"] = rid + 1

    #update_or_send_day_window(chat_id)
    save_data(data)
    save_chat_json(chat_id)
    export_global_csv(data)

    send_backup_to_channel(chat_id)


def update_record_in_chat(chat_id: int, rid: int, new_amount: int, new_note: str):
    store = get_chat_store(chat_id)
    found = None

    for r in store.get("records", []):
        if r["id"] == rid:
            r["amount"] = new_amount
            r["note"] = new_note
            found = r
            break

    if not found:
        return

    for day, arr in store.get("daily_records", {}).items():
        for r in arr:
            if r["id"] == rid:
                r.update(found)

    store["balance"] = sum(x["amount"] for x in store["records"])

    data["records"] = [x if x["id"] != rid else found for x in data["records"]]
    data["overall_balance"] = sum(x["amount"] for x in data["records"])
    
    #update_or_send_day_window(chat_id)
    save_data(data)
    save_chat_json(chat_id)
    export_global_csv(data)
    send_backup_to_channel(chat_id)


def delete_record_in_chat(chat_id: int, rid: int):
    store = get_chat_store(chat_id)

    store["records"] = [x for x in store["records"] if x["id"] != rid]

    for day, arr in list(store.get("daily_records", {}).items()):
        arr2 = [x for x in arr if x["id"] != rid]
        if arr2:
            store["daily_records"][day] = arr2
        else:
            del store["daily_records"][day]

    store["balance"] = sum(x["amount"] for x in store["records"])

    data["records"] = [x for x in data["records"] if x["id"] != rid]
    data["overall_balance"] = sum(x["amount"] for x in data["records"])

    #update_or_send_day_window(chat_id)
    save_data(data)
    save_chat_json(chat_id)
    export_global_csv(data)
    send_backup_to_channel(chat_id)

# ==========================================================
# SECTION 14 — Active window system (версия код-010)
# ==========================================================

def get_or_create_active_windows(chat_id: int) -> dict:
    return data.setdefault("active_messages", {}).setdefault(str(chat_id), {})


def set_active_window_id(chat_id: int, day_key: str, message_id: int):
    aw = get_or_create_active_windows(chat_id)
    aw[day_key] = message_id
    save_data(data)


def get_active_window_id(chat_id: int, day_key: str):
    aw = get_or_create_active_windows(chat_id)
    return aw.get(day_key)


def delete_active_window_if_exists(chat_id: int, day_key: str):
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
    Если окно дня существует — обновляем через edit.
    Если нет — создаём.
    """
    txt, _ = render_day_window(chat_id, day_key)
    kb = build_main_keyboard(day_key, chat_id)

    mid = get_active_window_id(chat_id, day_key)
    if mid:
        try:
            bot.edit_message_text(
                txt,
                chat_id=chat_id,
                message_id=mid,
                reply_markup=kb,
                parse_mode="HTML"
            )
            return
        except:
            pass

    sent = bot.send_message(chat_id, txt, reply_markup=kb, parse_mode="HTML")
    set_active_window_id(chat_id, day_key, sent.message_id)

# ==========================================================
# SECTION 15 — Управление финансовым режимом
# ==========================================================

def is_finance_mode(chat_id: int) -> bool:
    return chat_id in finance_active_chats


def set_finance_mode(chat_id: int, enabled: bool):
    if enabled:
        finance_active_chats.add(chat_id)
    else:
        finance_active_chats.discard(chat_id)


def require_finance(chat_id: int) -> bool:
    """
    Проверка: включён ли финансовый режим.
    Если нет — показываем подсказку /поехали.
    """
    if not is_finance_mode(chat_id):
        send_info(chat_id, "⚙️ Финансовый режим выключен.\nАктивируйте командой /поехали")
        return False
    return True


        
        
        
        
        # ==========================================================
# SECTION 17 — Команды
# ==========================================================

def send_info(chat_id: int, text: str):
    try:
        bot.send_message(chat_id, text)
    except Exception as e:
        log_error(f"send_info: {e}")


@bot.message_handler(commands=["поехали"])
def cmd_enable_finance(msg):
    chat_id = msg.chat.id
    set_finance_mode(chat_id, True)
    save_data(data)
    send_info(chat_id, "🚀 Финансовый режим включён!\nОтправьте /start")
    return


@bot.message_handler(commands=["start"])
def cmd_start(msg):
    chat_id = msg.chat.id
    if not require_finance(chat_id):
        return

    day_key = today_key()
    txt, _ = render_day_window(chat_id, day_key)
    kb = build_main_keyboard(day_key, chat_id)

    sent = bot.send_message(chat_id, txt, reply_markup=kb, parse_mode="HTML")
    set_active_window_id(chat_id, day_key, sent.message_id)


@bot.message_handler(commands=["help"])
def cmd_help(msg):
    chat_id = msg.chat.id
    if not is_finance_mode(chat_id):
        send_info(chat_id, "ℹ️ Финансовый режим выключен")
        return

    help_text = (
        "📘 Команды финансового бота:\n\n"
        "/поехали — включить финансовый режим\n"
        "/start — окно сегодняшнего дня\n"
        "/view YYYY-MM-DD — открыть конкретный день\n"
        "/prev — предыдущий день\n"
        "/next — следующий день\n"
        "/balance — баланс\n"
        "/report — краткий отчёт\n"
        "/csv — экспорт CSV (Drive+канал+чат)\n"
        "/json — выгрузка JSON\n"
        "/reset — обнулить данные чата\n"
        "/stopforward — отключить пересылку\n"
        "/ping — жив ли бот\n"
        "/backup_gdrive_on / _off — включить/выключить GDrive\n"
        "/backup_channel_on / _off — включить/выключить бэкап в канал\n"
        "/restore / /restore_off — режим восстановления JSON/CSV\n"
        "/help — эта справка\n"
    )
    send_info(chat_id, help_text)

# ==========================================================
# RESTORE MODE COMMANDS
# ==========================================================

@bot.message_handler(commands=["restore"])
def cmd_restore(msg):
    global restore_mode
    restore_mode = True
    bot.send_message(
        msg.chat.id,
        "📥 Режим восстановления включён.\n"
        "Теперь отправьте файл:\n"
        "• data.json\n"
        "• data_<chat_id>.json\n"
        "• csv_meta.json\n"
        "• data_<chat>.csv\n\n"
        "Пересылка документов временно отключена."
    )


@bot.message_handler(commands=["restore_off"])
def cmd_restore_off(msg):
    global restore_mode
    restore_mode = False
    bot.send_message(msg.chat.id, "🔒 Режим восстановления выключен.")


@bot.message_handler(commands=["ping"])
def cmd_ping(msg):
    send_info(msg.chat.id, "PONG — бот работает 🟢")


@bot.message_handler(commands=["view"])
def cmd_view(msg):
    chat_id = msg.chat.id
    if not require_finance(chat_id):
        return

    parts = (msg.text or "").split()
    if len(parts) < 2:
        send_info(chat_id, "Использование: /view YYYY-MM-DD")
        return

    day_key = parts[1]
    try:
        datetime.strptime(day_key, "%Y-%m-%d")
    except ValueError:
        send_info(chat_id, "❌ Неверная дата. Формат: YYYY-MM-DD")
        return

    txt, _ = render_day_window(chat_id, day_key)
    kb = build_main_keyboard(day_key, chat_id)
    sent = bot.send_message(chat_id, txt, reply_markup=kb, parse_mode="HTML")
    set_active_window_id(chat_id, day_key, sent.message_id)


@bot.message_handler(commands=["prev"])
def cmd_prev(msg):
    chat_id = msg.chat.id
    if not require_finance(chat_id):
        return

    d = datetime.strptime(today_key(), "%Y-%m-%d") - timedelta(days=1)
    day_key = d.strftime("%Y-%m-%d")

    txt, _ = render_day_window(chat_id, day_key)
    kb = build_main_keyboard(day_key, chat_id)

    sent = bot.send_message(chat_id, txt, reply_markup=kb, parse_mode="HTML")
    set_active_window_id(chat_id, day_key, sent.message_id)


@bot.message_handler(commands=["next"])
def cmd_next(msg):
    chat_id = msg.chat.id
    if not require_finance(chat_id):
        return

    d = datetime.strptime(today_key(), "%Y-%m-%d") + timedelta(days=1)
    day_key = d.strftime("%Y-%m-%d")

    txt, _ = render_day_window(chat_id, day_key)
    kb = build_main_keyboard(day_key, chat_id)

    sent = bot.send_message(chat_id, txt, reply_markup=kb, parse_mode="HTML")
    set_active_window_id(chat_id, day_key, sent.message_id)


@bot.message_handler(commands=["balance"])
def cmd_balance(msg):
    chat_id = msg.chat.id
    if not require_finance(chat_id):
        return

    store = get_chat_store(chat_id)
    bal = store.get("balance", 0)
    send_info(chat_id, f"💰 Баланс: {fmt_num(bal)}")


@bot.message_handler(commands=["report"])
def cmd_report(msg):
    chat_id = msg.chat.id
    if not require_finance(chat_id):
        return

    store = get_chat_store(chat_id)
    lines = ["📊 Отчёт:"]
    for dk, recs in sorted(store.get("daily_records", {}).items()):
        day_sum = sum(r["amount"] for r in recs)
        lines.append(f"{dk}: {fmt_num(day_sum)}")

    send_info(chat_id, "\n".join(lines))


def cmd_csv_all(chat_id: int):
    """
    Общий CSV по всем чатам (для кнопки в меню редактирования).
    """
    if not require_finance(chat_id):
        return

    try:
        export_global_csv(data)
        if not os.path.exists(CSV_FILE):
            send_info(chat_id, "Файл общего CSV ещё не создан.")
            return

        upload_to_gdrive(CSV_FILE)

        with open(CSV_FILE, "rb") as f:
            bot.send_document(chat_id, f, caption="📂 Общий CSV (все чаты)")
    except Exception as e:
        log_error(f"cmd_csv_all: {e}")


def cmd_csv_day(chat_id: int, day_key: str):
    """
    CSV только за один день для текущего чата.
    """
    if not require_finance(chat_id):
        return

    store = get_chat_store(chat_id)
    day_recs = store.get("daily_records", {}).get(day_key, [])
    if not day_recs:
        send_info(chat_id, "Нет записей за этот день.")
        return

    tmp_name = f"data_{chat_id}_{day_key}.csv"
    try:
        with open(tmp_name, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["chat_id", "ID", "short_id", "timestamp", "amount", "note", "owner", "day_key"])
            for r in day_recs:
                w.writerow([
                    chat_id,
                    r.get("id"),
                    r.get("short_id"),
                    r.get("timestamp"),
                    r.get("amount"),
                    r.get("note"),
                    r.get("owner"),
                    day_key,
                ])

        upload_to_gdrive(tmp_name)

        with open(tmp_name, "rb") as f:
            bot.send_document(chat_id, f, caption=f"📅 CSV за день {day_key}")
    except Exception as e:
        log_error(f"cmd_csv_day: {e}")
    finally:
        try:
            os.remove(tmp_name)
        except FileNotFoundError:
            pass


@bot.message_handler(commands=["csv"])
def cmd_csv(msg):
    """
    Экспортирует CSV текущего чата.
    """
    chat_id = msg.chat.id
    if not require_finance(chat_id):
        return

    export_global_csv(data)
    save_chat_json(chat_id)

    per_csv = chat_csv_file(chat_id)
    sent = None

    if os.path.exists(per_csv):
        upload_to_gdrive(per_csv)

        with open(per_csv, "rb") as f:
            sent = bot.send_document(chat_id, f, caption="📂 CSV этого чата")

    if OWNER_ID and chat_id == int(OWNER_ID):
        meta = _load_csv_meta()
        if sent and getattr(sent, "document", None):
            meta["file_id_csv"] = sent.document.file_id
        meta["message_id_csv"] = getattr(sent, "message_id", meta.get("message_id_csv"))
        _save_csv_meta(meta)

    send_backup_to_channel(chat_id)


@bot.message_handler(commands=["json"])
def cmd_json(msg):
    chat_id = msg.chat.id
    if not require_finance(chat_id):
        return

    save_chat_json(chat_id)
    p = chat_json_file(chat_id)

    if os.path.exists(p):
        with open(p, "rb") as f:
            bot.send_document(chat_id, f, caption="🧾 JSON этого чата")
    else:
        send_info(chat_id, "Файл JSON ещё не создан.")


@bot.message_handler(commands=["reset"])
def cmd_reset(msg):
    chat_id = msg.chat.id
    if not require_finance(chat_id):
        return
    send_info(chat_id, "Вы уверены, что хотите обнулить данные? Напишите ДА.")


@bot.message_handler(commands=["stopforward"])
def cmd_stopforward(msg):
    if str(msg.chat.id) != str(OWNER_ID):
        send_info(msg.chat.id, "Эта команда только для владельца.")
        return
    clear_forward_all()
    send_info(msg.chat.id, "Пересылка полностью отключена.")


@bot.message_handler(commands=["backup_gdrive_on"])
def cmd_on_drive(msg):
    backup_flags["drive"] = True
    save_data(data)
    send_info(msg.chat.id, "☁️ Бэкап в Google Drive включён")


@bot.message_handler(commands=["backup_gdrive_off"])
def cmd_off_drive(msg):
    backup_flags["drive"] = False
    save_data(data)
    send_info(msg.chat.id, "☁️ Бэкап в Google Drive выключен")


@bot.message_handler(commands=["backup_channel_on"])
def cmd_on_channel(msg):
    backup_flags["channel"] = True
    save_data(data)
    send_info(msg.chat.id, "📡 Бэкап в канал включён")


@bot.message_handler(commands=["backup_channel_off"])
def cmd_off_channel(msg):
    backup_flags["channel"] = False
    save_data(data)
    send_info(msg.chat.id, "📡 Бэкап в канал выключен")
    
 # ==========================================================
# SECTION 17 — BACKUP (GDRIVE + CHANNEL)
# ==========================================================

...код...

# ==========================================================
# SECTION 17.5 — ChatID Discovery (my_chat_member handler)
# ==========================================================

@bot.my_chat_member_handler()
def handle_my_chat_member(event):
    """
    Детектор всех чатов, где находится бот.
    Работает даже если никто не писал сообщения.
    """
    try:
        chat = event.chat
        chat_id = chat.id
        chat_title = chat.title or f"Чат {chat_id}"
        chat_type = chat.type

        log_info(f"CHAT_DISCOVERY: бот замечен в чате {chat_id} ({chat_title}), type={chat_type}")

        # --- 1. Обновляем info чата ---
        store = get_chat_store(chat_id)
        info = store.setdefault("info", {})
        info["title"] = chat_title
        info["type"] = chat_type
        info["username"] = getattr(chat, "username", None)
        save_chat_json(chat_id)

        # --- 2. Добавляем в known_chats владельца ---
        if OWNER_ID and str(chat_id) != str(OWNER_ID):
            owner_store = get_chat_store(int(OWNER_ID))
            kc = owner_store.setdefault("known_chats", {})
            kc[str(chat_id)] = {
                "title": chat_title,
                "username": getattr(chat, "username", None),
                "type": chat_type,
            }
            save_chat_json(int(OWNER_ID))

            log_info(f"CHAT_DISCOVERY: добавлен в known_chats владельца {OWNER_ID}")

    except Exception as e:
        log_error(f"handle_my_chat_member error: {e}")


# ==========================================================
# SECTION 18 — Text handler
# ==========================================================
    
    #🔵🔵🔵🔵🔵🔵🔵
# ==========================================================
# SECTION 18 — Text handler (финансы + пересылка + chat_info)
# ==========================================================

def update_chat_info_from_message(msg):
    """
    Обновляет информацию о чате при каждом сообщении.
    Хранится в: store["info"] и store["known_chats"] (для OWNER).
    """
    chat_id = msg.chat.id
    store = get_chat_store(chat_id)

    info = store.setdefault("info", {})
    info["title"] = msg.chat.title or info.get("title") or f"Чат {chat_id}"
    info["username"] = msg.chat.username or info.get("username")
    info["type"] = msg.chat.type

    if OWNER_ID and str(chat_id) != str(OWNER_ID):
        owner_store = get_chat_store(int(OWNER_ID))
        kc = owner_store.setdefault("known_chats", {})
        kc[str(chat_id)] = {
            "title": info["title"],
            "username": info["username"],
            "type": info["type"],
        }
        save_chat_json(int(OWNER_ID))

    save_chat_json(chat_id)


@bot.message_handler(content_types=["text"])
def handle_text(msg):
    try:
        chat_id = msg.chat.id
        text = (msg.text or "").strip()

        update_chat_info_from_message(msg)

        targets = resolve_forward_targets(chat_id)
        if targets:
            forward_text_anon(chat_id, msg, targets)

        store = get_chat_store(chat_id)
        wait = store.get("edit_wait")

        if wait and wait.get("type") == "add":

                day_key = wait.get("day_key")

                lines = text.split("\n")
                added_any = False
                for line in lines:
                        line = line.strip()
                        if not line:
                                continue

                        try:
                                amount, note = split_amount_and_note(line)
                        except Exception:
                                bot.send_message(chat_id, f"❌ Ошибка суммы: {line}")
                                continue

                        # 1) добавляем запись (без сохранений и без бэкапов)
                        store = get_chat_store(chat_id)
                        rid = store.get("next_id", 1)

                        rec = {
                                "id": rid,
                                "short_id": f"R{rid}",
                                "timestamp": now_local().isoformat(timespec="seconds"),
                                "amount": amount,
                                "note": note,
                                "owner": msg.from_user.id,
                                "msg_id": msg.message_id,   # ← ДОБАВЛЕНО
                                "origin_msg_id": msg.message_id,  # FIX VARIANT 3
                        }

                        store.setdefault("records", []).append(rec)
                        store.setdefault("daily_records", {}).setdefault(day_key, []).append(rec)
                        log_info(f"ADD: добавлена запись id={rid} msg_id={msg.message_id} day={day_key} amount={amount} note='{note}'")
                        store["next_id"] = rid + 1
                        added_any = True

                # 2) СНАЧАЛА обновляем окно дня
                # 2) Создаём новое окно и сохраняем как активное
                if added_any:
                        txt, _ = render_day_window(chat_id, day_key)
                        kb = build_main_keyboard(day_key, chat_id)

                        sent = bot.send_message(chat_id, txt, reply_markup=kb, parse_mode="HTML")

                        # Запоминаем msg_id нового окна
                        set_active_window_id(chat_id, day_key, sent.message_id)
                        
                # 3) ПОТОМ выполняем сохранение и бэкап
                store["balance"] = sum(x["amount"] for x in store["records"])

                # Полный пересчёт глобального списка
                data["records"] = []
                for cid, st in data.get("chats", {}).items():
                        data["records"].extend(st.get("records", []))

                data["overall_balance"] = sum(x["amount"] for x in data["records"])

                save_data(data)
                save_chat_json(chat_id)
                export_global_csv(data)
                send_backup_to_channel(chat_id)

                store["edit_wait"] = None
                save_data(data)
                return

        if wait and wait.get("type") == "edit":
            rid = wait.get("rid")

            try:
                amount, note = split_amount_and_note(text)
            except Exception:
                bot.send_message(chat_id, f"❌ Ошибка суммы: {text}")
                return

            update_record_in_chat(chat_id, rid, amount, note)

            store["edit_wait"] = None
            save_data(data)

            day_key = wait.get("day_key")
            txt, _ = render_day_window(chat_id, day_key)
            kb = build_main_keyboard(day_key, chat_id)
            bot.send_message(chat_id, txt, reply_markup=kb, parse_mode="HTML")
            return

        if text.upper() == "ДА":
            reset_chat_data(chat_id)
            bot.send_message(chat_id, "🔄 Данные чата обнулены.")
            return

    except Exception as e:
        log_error(f"handle_text: {e}")

# ==========================================================
# SECTION 18.1 — Reset chat data helper
# ==========================================================

def reset_chat_data(chat_id: int):
    """
    Полный сброс данных чата.
    """
    chats = data.setdefault("chats", {})
    if str(chat_id) in chats:
        chats[str(chat_id)] = {
            "info": {},
            "known_chats": {},
            "balance": 0,
            "records": [],
            "daily_records": {},
            "next_id": 1,
            "active_windows": {},
            "edit_wait": None,
            "edit_target": None,
            "current_view_day": today_key(),
        }

    save_chat_json(chat_id)
    save_data(data)
    export_global_csv(data)
    send_backup_to_channel(chat_id)

# ==========================================================
# SECTION 18.2 — Media forwarding (анонимно + media_group)
# ==========================================================

@bot.message_handler(
    content_types=[
        "photo", "audio", "video", "voice",
        "video_note", "sticker", "animation"
    ]
)
def handle_media_forward(msg):
    try:
        chat_id = msg.chat.id

        update_chat_info_from_message(msg)

        try:
            BOT_ID = bot.get_me().id
        except:
            BOT_ID = None

        if BOT_ID and msg.from_user and msg.from_user.id == BOT_ID:
            return

        targets = resolve_forward_targets(chat_id)
        if not targets:
            return

        group_msgs = collect_media_group(chat_id, msg)
        if not group_msgs:
            return

        if len(group_msgs) > 1:
            forward_media_group_anon(chat_id, group_msgs, targets)
            return

        for dst, mode in targets:
            try:
                bot.copy_message(dst, chat_id, msg.message_id)
            except Exception as e:
                log_error(f"handle_media_forward to {dst}: {e}")

    except Exception as e:
        log_error(f"handle_media_forward error: {e}")

# ==========================================================
# SECTION 18.3 — Forwarding of location / contact / poll / venue
# ==========================================================

@bot.message_handler(content_types=["location", "contact", "poll", "venue"])
def handle_special_forward(msg):
    global restore_mode

    if restore_mode:
        return

    try:
        chat_id = msg.chat.id
        update_chat_info_from_message(msg)

        try:
            BOT_ID = bot.get_me().id
        except:
            BOT_ID = None

        if BOT_ID and msg.from_user and msg.from_user.id == BOT_ID:
            return

        targets = resolve_forward_targets(chat_id)
        if not targets:
            return

        for dst, mode in targets:
            try:
                bot.copy_message(dst, chat_id, msg.message_id)
            except Exception as e:
                log_error(f"handle_special_forward to {dst}: {e}")

    except Exception as e:
        log_error(f"handle_special_forward error: {e}")

# ==========================================================
# SECTION 18.4 — DOCUMENTS: forwarding + restore (единый хендлер)
# ==========================================================

@bot.message_handler(content_types=["document"])
def handle_document(msg):
    """
    Логика обработки документов:
    1) ВСЕ документы обновляют info/known_chats
    2) Если restore_mode == True → используется как файл восстановления
    3) Если restore_mode == False → обычная пересылка документа
    """
    global restore_mode, data

    chat_id = msg.chat.id
    update_chat_info_from_message(msg)

    file = msg.document
    fname = (file.file_name or "").lower()

    # --------- ВЕТКА ВОССТАНОВЛЕНИЯ -----------
    if restore_mode:
        # принимаем только JSON/CSV
        if not (fname.endswith(".json") or fname.endswith(".csv")):
            bot.send_message(chat_id, f"⚠️ Файл '{fname}' не является JSON/CSV.")
            return

        try:
            file_info = bot.get_file(file.file_id)
            raw = bot.download_file(file_info.file_path)
        except Exception as e:
            bot.send_message(chat_id, f"❌ Ошибка скачивания файла: {e}")
            return

        tmp_path = f"restore_{chat_id}_{fname}"

        with open(tmp_path, "wb") as f:
            f.write(raw)

        # 1) Глобальный data.json
        if fname == "data.json":
            try:
                os.replace(tmp_path, "data.json")
                data = load_data()
                restore_mode = False
                bot.send_message(chat_id, "🟢 Глобальный data.json восстановлен!")
            except Exception as e:
                bot.send_message(chat_id, f"❌ Ошибка: {e}")
            return

        # 2) csv_meta.json
        if fname == "csv_meta.json":
            try:
                os.replace(tmp_path, "csv_meta.json")
                restore_mode = False
                bot.send_message(chat_id, "🟢 csv_meta.json восстановлен!")
            except Exception as e:
                bot.send_message(chat_id, f"❌ Ошибка: {e}")
            return

        # 3) per-chat JSON data_<chat>.json
        if fname.startswith("data_") and fname.endswith(".json"):
            try:
                target = int(fname.replace("data_", "").replace(".json", ""))
            except:
                bot.send_message(chat_id, "❌ Невозможно определить chat_id из имени файла.")
                return

            try:
                os.replace(tmp_path, fname)
                store = _load_json(fname, {})
                if not store:
                    bot.send_message(chat_id, "❌ Файл повреждён или пуст.")
                    return

                store["balance"] = sum(r.get("amount", 0) for r in store.get("records", []))

                data.setdefault("chats", {})[str(target)] = store
                finance_active_chats.add(target)

                # пересобираем глобальные records и overall_balance
                all_recs = []
                for cid, s in data.get("chats", {}).items():
                    all_recs.extend(s.get("records", []))
                data["records"] = all_recs
                data["overall_balance"] = sum(r.get("amount", 0) for r in all_recs)

                save_data(data)
                save_chat_json(target)

                update_or_send_day_window(target, today_key())

                restore_mode = False

                bot.send_message(
                    chat_id,
                    f"🟢 Чат {target} восстановлен.\n"
                    f"Записей: {len(store.get('records', []))}\n"
                    f"Баланс: {store['balance']}"
                )
            except Exception as e:
                bot.send_message(chat_id, f"❌ Ошибка: {e}")
            return

        # 4) per-chat CSV
        if fname.startswith("data_") and fname.endswith(".csv"):
            try:
                os.replace(tmp_path, fname)
                restore_mode = False
                bot.send_message(chat_id, f"🟢 CSV восстановлен: {fname}")
            except Exception as e:
                bot.send_message(chat_id, f"❌ Ошибка: {e}")
            return

        bot.send_message(chat_id, f"⚠️ Формат не поддерживается: {fname}")
        return

    # --------- ВЕТКА ПЕРЕСЫЛКИ (restore_mode == False)  -----------

    try:
        try:
            BOT_ID = bot.get_me().id
        except:
            BOT_ID = None

        if BOT_ID and msg.from_user and msg.from_user.id == BOT_ID:
            return

        targets = resolve_forward_targets(chat_id)
        if not targets:
            return

        group_msgs = collect_media_group(chat_id, msg)
        if not group_msgs:
            return

        if len(group_msgs) > 1:
            forward_media_group_anon(chat_id, group_msgs, targets)
            return

        for dst, mode in targets:
            try:
                bot.copy_message(dst, chat_id, msg.message_id)
            except Exception as e:
                log_error(f"handle_document forward to {dst}: {e}")

    except Exception as e:
        log_error(f"handle_document error: {e}")
# ==========================================================
# SECTION 18.5 — Edited messages: direct correction of records
# ==========================================================
@bot.edited_message_handler(content_types=["text"])
def handle_edited_message(msg):
    """
    Редактирование записи через редактирование сообщения.
    """
    chat_id = msg.chat.id
    message_id = msg.message_id
    new_text = (msg.text or "").strip()

    log_info(f"EDITED: пришёл edited_message в чате {chat_id}, msg_id={message_id}, text='{new_text}'")

    # 1) Проверка фин. режима
    if not is_finance_mode(chat_id):
        log_info(f"EDITED: игнор, finance_mode=OFF для чата {chat_id}")
        return

    # 2) Проверка restore_mode
    if restore_mode:
        log_info("EDITED: игнор, restore_mode=True")
        return

    update_chat_info_from_message(msg)

    store = get_chat_store(chat_id)
    day_key = today_key()

    # 3) Ищем запись по msg_id / origin_msg_id
    target = None
    for day, recs in store.get("daily_records", {}).items():
        for r in recs:
            if r.get("msg_id") == message_id or r.get("origin_msg_id") == message_id:
                target = r
                day_key = day
                break
        if target:
            break

    if not target:
        log_info(f"EDITED: запись не найдена по msg_id={message_id} в daily_records чата {chat_id}")
        return

    log_info(f"EDITED: найдена запись ID={target.get('id')} за день {day_key}")

    # 4) Парсим новое содержимое
    try:
        new_amount, new_note = split_amount_and_note(new_text)
    except Exception as e:
        log_error(f"EDITED: ошибка парсинга суммы: {e}")
        bot.send_message(chat_id, "❌ Ошибка: не удалось разобрать сумму.")
        return

    rid = target["id"]
    log_info(f"EDITED: обновляем запись ID={rid}, amount={new_amount}, note='{new_note}'")

    # 5) Обновляем запись
    update_record_in_chat(chat_id, rid, new_amount, new_note)

    # 6) Обновляем окно
    update_or_send_day_window(chat_id, day_key)
    log_info(f"EDITED: окно дня {day_key} обновлено для чата {chat_id}")

 # ==========================================================
# SECTION 19 — Keep-alive
# ==========================================================

KEEP_ALIVE_SEND_TO_OWNER = False

def keep_alive_task():
    while True:
        try:
            if APP_URL:
                try:
                    resp = requests.get(APP_URL, timeout=10)
                    log_info(f"Keep-alive ping -> {resp.status_code}")
                except Exception as e:
                    log_error(f"Keep-alive self error: {e}")

            if KEEP_ALIVE_SEND_TO_OWNER and OWNER_ID:
                try:
                    pass
                except Exception as e:
                    log_error(f"Keep-alive notify error: {e}")

        except Exception as e:
            log_error(f"Keep-alive loop error: {e}")

        time.sleep(max(10, KEEP_ALIVE_INTERVAL_SECONDS))


def start_keep_alive_thread():
    t = threading.Thread(target=keep_alive_task, daemon=True)
    t.start()
# Финансовая логика к документам не относится


# ==========================================================
# SECTION 20 — Webhook / Flask / main()
# ==========================================================

@app.route(f"/{BOT_TOKEN}", methods=["POST"])
def telegram_webhook():
    json_str = request.get_data().decode("utf-8")

    # DEBUG 1: логируем, если прилетел edited_message
    try:
        if '"edited_message"' in json_str:
            log_info("WEBHOOK: получен update с edited_message")
    except Exception as e:
        log_error(f"DEBUG webhook edited check error: {e}")

    update = telebot.types.Update.de_json(json_str)
    bot.process_new_updates([update])
    return "OK", 200


def set_webhook():
    if not APP_URL:
        log_info("APP_URL не указан — работаем в режиме polling.")
        return

    wh_url = APP_URL.rstrip("/") + f"/{BOT_TOKEN}"
    bot.remove_webhook()
    time.sleep(0.5)
    bot.set_webhook(url=wh_url)
    log_info(f"Webhook установлен: {wh_url}")


def main():
    global data

    restored = restore_from_gdrive_if_needed()

    data = load_data()
    data["forward_rules"] = load_forward_rules()
    log_info(f"Данные загружены. Версия бота: {VERSION}")

    set_webhook()
    start_keep_alive_thread()

    if OWNER_ID:
        try:
            bot.send_message(
                int(OWNER_ID),
                f"✅ Бот запущен (версия {VERSION}).\n"
                f"Восстановление: {'OK' if restored else 'пропущено'}"
            )
        except Exception:
            pass

    app.run(host="0.0.0.0", port=PORT)


if __name__ == "__main__":
    main()