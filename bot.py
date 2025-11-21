# Code_022.5-FINAL.2
# • ручное востановление
# •пересылка всех типов сообщений
#  •вывод значений расширенный
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

VERSION = "Code_022.5 final.1"

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
            "known_chats": {},          # <-- ДОБАВЛЕННО: словарь известных чатов
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
    Старая логика сохранена полностью, добавлена поддержка known_chats.
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
            "known_chats": store.get("known_chats", {}),   # <-- ДОБАВЛЕНО
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
    Европейский формат вывода:
        1234.56  → 1.234,56
        1234     → 1.234
        1.2      → 1,2
    """

    negative = x < 0
    x = abs(x)

    # Приводим к нормальному строковому виду:
    s = f"{x:.12f}".rstrip("0").rstrip(".")

    if "." in s:
        int_part, dec_part = s.split(".")
    else:
        int_part, dec_part = s, ""

    # Формируем тысячные (точка)
    int_part = f"{int(int_part):,}".replace(",", ".")

    # Склеиваем
    if dec_part:
        s = f"{int_part},{dec_part}"
    else:
        s = int_part

    #return f"-{s}" if negative else s
    return s


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

    Понимает:
        "Обед200вчера"        → -200, "обед вчера"
        "Нашёл +1 780,678 бонус" → +1780.678, "нашёл бонус"
        "Хлеб 456"            → -456, "хлеб"
        "1.5 сон"             → -1.5, "сон"
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
        # 🔥 ВОТ ЭТА ЧАСТЬ ДОБАВЛЯЕТ БЭКАП csv_meta.json
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

        # ---------------------------
        # Конвертация старого формата:
        # старый формат = { "123": ["555","777"] }
        # новый формат  = {
        #    "123": { "555": "oneway_to" }
        # }
        # ---------------------------
        upgraded = {}

        for src, value in fr.items():
            # если старый формат — value = список
            if isinstance(value, list):
                upgraded[src] = {}
                for dst in value:
                    upgraded[src][dst] = "oneway_to"
            # если новый формат — словарь
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

def resolve_forward_targets(source_chat_id: int) -> list[tuple[int, str]]:
    """
    Возвращает список кортежей:
        (целевой_чат_id, режим)

    forward_rules имеет структуру:
    {
        "123": {
            "555": "oneway_to",
            "777": "oneway_from",
            "999": "twoway"
        }
    }

    Логика:
        • "oneway_to"  означает: source → target
        • "oneway_from" означает: target → source
        • "twoway"      означает: source → target И target → source
        
    Возвращаем только направления "из source".
    """
    fr = data.get("forward_rules", {}) or {}
    out = []

    src = str(source_chat_id)
    if src not in fr:
        return out

    for dst, mode in fr[src].items():
        try:
            dst_id = int(dst)
        except:
            continue

        # Отфильтровываем по режиму:
        if mode == "oneway_to":
            # всегда разрешено: source → dst
            out.append((dst_id, "oneway_to"))

        elif mode == "oneway_from":
            # source является получателем → для исходящего это НЕ подходит
            # то есть source не должен отправлять в dst
            continue

        elif mode == "twoway":
            # двусторонняя пересылка — всегда разрешено
            out.append((dst_id, "twoway"))

    return out


def add_forward_link(src_chat_id: int, dst_chat_id: int, mode: str = "oneway_to"):
    """
    Добавляет пересылку:
        mode ∈ {"oneway_to", "oneway_from", "twoway"}

    Структура:
        forward_rules[src][dst] = mode
    """
    fr = data.setdefault("forward_rules", {})
    src = str(src_chat_id)
    dst = str(dst_chat_id)

    node = fr.setdefault(src, {})
    node[dst] = mode

    persist_forward_rules_to_owner()
    save_data(data)


def remove_forward_link(src_chat_id: int, dst_chat_id: int):
    """Удаляет конкретную связь src→dst (любой режим)."""
    fr = data.get("forward_rules", {}) or {}
    src = str(src_chat_id)
    dst = str(dst_chat_id)

    if src in fr and dst in fr[src]:
        del fr[src][dst]

    # чистим пустые
    if src in fr and not fr[src]:
        del fr[src]

    persist_forward_rules_to_owner()
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
    """
    Анонимная пересылка текста.
    Используем copy_message — он не содержит имя отправителя.
    """
    for dst, mode in targets:
        try:
            bot.copy_message(dst, source_chat_id, msg.message_id)
        except Exception as e:
            log_error(f"forward_text_anon to {dst}: {e}")


def forward_media_anon(source_chat_id: int, msg, targets: list[tuple[int, str]]):
    """
    Анонимная пересылка любых медиа:
        фото, видео, аудио, документ, голос, стикеры, GIF.
    Используем copy_message — сохраняет медиа, но не имя отправителя.
    """
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
    Возвращает:
        None — если альбом ещё не собран
        list[msg] — если альбом завершён и готов к отправке
    """
    gid = msg.media_group_id
    if not gid:
        return [msg]

    group = _media_group_cache.setdefault(chat_id, {})
    arr = group.setdefault(gid, [])
    arr.append(msg)

    # Telegram присылает альбом пакетом за ~50–200ms.
    # Ждём коротко, затем считаем пакет завершённым.
    if len(arr) == 1:
        # первая часть — ждём немного, чтобы собрать остальные
        time.sleep(0.2)

    # после задержки считаем, что пришли все части
    complete = group.pop(gid, arr)
    return complete


def forward_media_group_anon(source_chat_id: int, messages: list, targets: list[tuple[int, str]]):
    """
    Пересылка собранного альбома анонимно.
    Используем InputMediaPhoto/Video/Document.
    """
    if not messages:
        return

    # Формируем InputMedia объекты
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
            # если тип не подходит под альбом — копируем как обычное
            for dst, mode in targets:
                try:
                    bot.copy_message(dst, source_chat_id, msg.message_id)
                except:
                    pass
            return

    # Отправляем альбом
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
    Рендер окна дня:
        • заголовок
        • каждая запись с short_id
        • сортировка по time
        • итог за день
    """
    store = get_chat_store(chat_id)
    recs = store.get("daily_records", {}).get(day_key, [])
    lines = []

    lines.append(f"📅 <b>{day_key}</b>")
    lines.append("")

    total = 0

    # сортируем по timestamp
    recs_sorted = sorted(recs, key=lambda x: x.get("timestamp"))

    for r in recs_sorted:
        amt = r["amount"]
        total += amt
        sign = "➕" if amt >= 0 else "➖"

        note = html.escape(r.get("note", ""))
        sid = r.get("short_id", f"R{r['id']}")

        ts = r.get("timestamp", "")
        ts_show = ts[11:16] if ts else ""
        lines.append(f"{sid} {sign}{fmt_num(amt)} <i>{note}</i>")
        #lines.append(f"{sid} — {sign} {fmt_num(amt)}  ({ts_show})")
        #if note:
            #lines.append(f"      <i>{note}</i>")

    if not recs_sorted:
        lines.append("Нет записей за этот день.")

    lines.append("")
    lines.append(f"💰 <b>Итого: {fmt_num(total)}</b>")

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

    kb.row(types.InlineKeyboardButton("📅 Сегодня", callback_data=f"d:{today_key()}:open"))

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

    # Владелец получает кнопку пересылки
    if OWNER_ID and str(chat_id) == str(OWNER_ID):
        kb.row(
            types.InlineKeyboardButton("🔁 Пересылка ↔️", callback_data=f"d:{day_key}:forward_menu")
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


# ==========================================================
# КЛАВИАТУРА — ВЫБОР ЧАТА ДЛЯ ПЕРЕСЫЛКИ
# ==========================================================

def build_forward_chat_list(day_key: str, chat_id: int):
    """
    Показывает список чатов в виде:

        [Название] (ID)
        ➡️ ↔️ ⬅️ (в зависимости от существующей связи)
    """
    kb = types.InlineKeyboardMarkup()

    chats = data.get("chats", {})
    rules = data.get("forward_rules", {})

    for cid, store in chats.items():
        try:
            int_cid = int(cid)
        except:
            continue

        info = store.get("info", {})
        title = info.get("title") or f"Чат {cid}"

        # определяем состояние стрелок
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


# ==========================================================
# КЛАВИАТУРА — ВЫБОР НАПРАВЛЕНИЯ
# ==========================================================

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
    }

    # глобальное хранилище
    data.setdefault("records", []).append(rec)

    # per-chat
    store.setdefault("records", []).append(rec)
    store.setdefault("daily_records", {}).setdefault(today_key(), []).append(rec)

    store["balance"] = sum(x["amount"] for x in store["records"])
    data["overall_balance"] = sum(x["amount"] for x in data["records"])
    store["next_id"] = rid + 1

    save_data(data)
    save_chat_json(chat_id)
    export_global_csv(data)

    send_backup_to_channel(chat_id)


def update_record_in_chat(chat_id: int, rid: int, new_amount: int, new_note: str):
    store = get_chat_store(chat_id)
    found = None

    # обновляем в store.records
    for r in store.get("records", []):
        if r["id"] == rid:
            r["amount"] = new_amount
            r["note"] = new_note
            found = r
            break

    if not found:
        return

    # обновляем в daily_records
    for day, arr in store.get("daily_records", {}).items():
        for r in arr:
            if r["id"] == rid:
                r.update(found)

    # обновляем баланс чата
    store["balance"] = sum(x["amount"] for x in store["records"])

    # обновляем глобальный список
    data["records"] = [x if x["id"] != rid else found for x in data["records"]]
    data["overall_balance"] = sum(x["amount"] for x in data["records"])

    save_data(data)
    save_chat_json(chat_id)
    export_global_csv(data)
    send_backup_to_channel(chat_id)


def delete_record_in_chat(chat_id: int, rid: int):
    store = get_chat_store(chat_id)

    # удаляем из records
    store["records"] = [x for x in store["records"] if x["id"] != rid]

    # удаляем из daily_records
    for day, arr in list(store.get("daily_records", {}).items()):
        arr2 = [x for x in arr if x["id"] != rid]
        if arr2:
            store["daily_records"][day] = arr2
        else:
            del store["daily_records"][day]

    # обновляем баланс
    store["balance"] = sum(x["amount"] for x in store["records"])

    # глобально
    data["records"] = [x for x in data["records"] if x["id"] != rid]
    data["overall_balance"] = sum(x["amount"] for x in data["records"])

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
            # сообщение удалено → создаём новое
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


#🟠🟠🟠🟠🟠🟠🟠🟠🟠
# ==========================================================
# SECTION 16 — Callback handler (обработчик всех меню)
# ==========================================================

@bot.callback_query_handler(func=lambda c: True)
def on_callback(call):
    """
    Универсальный обработчик всех callback_data.
    Формат callback_data:
        d:<day_key>:<cmd>
        c:<center_day>

    Здесь добавлена:
        • Полная система пересылки ➡️ ↔️ ⬅️
        • Меню forward_menu
        • Выбор чата
        • Выбор направления
        • Кнопки Назад
    """
    try:
        data_str = call.data or ""
        chat_id = call.message.chat.id

        # -------------------------------------------------------
        # КАЛЕНДАРЬ (c:YYYY-MM-DD)
        # -------------------------------------------------------
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
            except:
                pass
            return

        # -------------------------------------------------------
        # d:<day_key>:<cmd>
        # -------------------------------------------------------
        if not data_str.startswith("d:"):
            return

        _, day_key, cmd = data_str.split(":", 2)
        store = get_chat_store(chat_id)

        # -------------------------------------------------------
        # 1) Открытие дня
        # -------------------------------------------------------
        if cmd == "open":
            txt, _ = render_day_window(chat_id, day_key)
            kb = build_main_keyboard(day_key, chat_id)

            bot.edit_message_text(
                txt,
                chat_id=chat_id,
                message_id=call.message.message_id,
                reply_markup=kb,
                parse_mode="HTML"
            )
            set_active_window_id(chat_id, day_key, call.message.message_id)
            return

        # -------------------------------------------------------
        # 2) Навигация /prev /next
        # -------------------------------------------------------
        if cmd == "prev":
            d = datetime.strptime(day_key, "%Y-%m-%d") - timedelta(days=1)
            nd = d.strftime("%Y-%m-%d")
            txt, _ = render_day_window(chat_id, nd)
            kb = build_main_keyboard(nd, chat_id)
            bot.edit_message_text(txt, chat_id, call.message.message_id,
                                  reply_markup=kb, parse_mode="HTML")
            set_active_window_id(chat_id, nd, call.message.message_id)
            return

        if cmd == "next":
            d = datetime.strptime(day_key, "%Y-%m-%d") + timedelta(days=1)
            nd = d.strftime("%Y-%m-%d")
            txt, _ = render_day_window(chat_id, nd)
            kb = build_main_keyboard(nd, chat_id)
            bot.edit_message_text(txt, chat_id, call.message.message_id,
                                  reply_markup=kb, parse_mode="HTML")
            set_active_window_id(chat_id, nd, call.message.message_id)
            return

        # -------------------------------------------------------
        # 3) Календарь
        # -------------------------------------------------------
        if cmd == "calendar":
            try:
                cdt = datetime.strptime(day_key, "%Y-%m-%d")
            except:
                cdt = now_local()

            kb = build_calendar_keyboard(cdt)

            bot.edit_message_reply_markup(
                chat_id=chat_id,
                message_id=call.message.message_id,
                reply_markup=kb
            )
            return

        # -------------------------------------------------------
        # 4) Отчёт
        # -------------------------------------------------------
        if cmd == "report":
            lines = ["📊 Отчёт:"]
            for dk, recs in sorted(store.get("daily_records", {}).items()):
                s = sum(r["amount"] for r in recs)
                lines.append(f"{dk}: {fmt_num(s)}")
            bot.send_message(chat_id, "\n".join(lines))
            return

        # -------------------------------------------------------
        # 5) Общий итог
        # -------------------------------------------------------
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

        # -------------------------------------------------------
        # ℹ️  Инфо
        # -------------------------------------------------------
        if cmd == "info":

            # подтвердить клик (важно для Telegram)
            try:
                bot.answer_callback_query(call.id)
            except:
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
                "/help — помощь\n"
                "/backup_gdrive_on / off — включить/выключить GDrive\n"
                "/backup_channel_on / off — включить/выключить бэкап в канал\n"
                "/stopforward — отключить пересылку\n"
            )

            # главное: не редактировать окно, а послать новое сообщение
            bot.send_message(chat_id, info_text)
            return

        # -------------------------------------------------------
        # 6) Меню редактирования
        # -------------------------------------------------------
        if cmd == "edit_menu":
            kb = build_edit_menu_keyboard(day_key, chat_id)
            bot.edit_message_reply_markup(
                chat_id=chat_id,
                message_id=call.message.message_id,
                reply_markup=kb
            )
            return

        if cmd == "back_main":
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

        # -------------------------------------------------------
        # 7) CSV
        # -------------------------------------------------------
        if cmd == "csv_all":
            cmd_csv_all(chat_id)
            return

        if cmd == "csv_day":
            cmd_csv_day(chat_id, day_key)
            return

        # -------------------------------------------------------
        # 8) Добавление записи
        # -------------------------------------------------------
        if cmd == "add":
            store["edit_wait"] = {"type": "add", "day_key": day_key}
            save_data(data)
            bot.send_message(chat_id, "Введите сумму и комментарий: +500 Пример")
            return
        txt, _ = render_day_window(chat_id, day_key)
        kb = build_main_keyboard(day_key, chat_id)
        bot.edit_message_text(
            txt,
            chat_id=chat_id,
            message_id=call.message.message_id,
            reply_markup=kb,
            parse_mode="HTML"
        )
        # -------------------------------------------------------
        # 9) Список записей
        # -------------------------------------------------------
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

        # -------------------------------------------------------
        # 10) Редактирование записи
        # -------------------------------------------------------
        if cmd.startswith("edit_rec_"):
            rid = int(cmd.split("_")[-1])
            store["edit_wait"] = {"type": "edit", "day_key": day_key, "rid": rid}
            save_data(data)
            bot.send_message(chat_id, f"Введите новую сумму и текст для записи R{rid}:")
            return

        # -------------------------------------------------------
        # 11) Пересылка — ГЛАВНОЕ МЕНЮ
        # -------------------------------------------------------
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

        # -------------------------------------------------------
        # 12) Настройка конкретного чата
        # -------------------------------------------------------
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

        # -------------------------------------------------------
        # 13) Установка направления ➡️ owner → tgt
        # -------------------------------------------------------
        if cmd.startswith("fw_one_"):
            tgt = int(cmd.split("_")[-1])
            add_forward_link(chat_id, tgt, "oneway_to")
            bot.send_message(chat_id, f"Установлена пересылка ➡️  {chat_id} → {tgt}")
            return

        # -------------------------------------------------------
        # 14) Установка направления ⬅️ tgt → owner
        # -------------------------------------------------------
        if cmd.startswith("fw_rev_"):
            tgt = int(cmd.split("_")[-1])
            add_forward_link(tgt, chat_id, "oneway_to")   # обратное направление
            add_forward_link(chat_id, tgt, "oneway_from")
            bot.send_message(chat_id, f"Установлена пересылка ⬅️  {tgt} → {chat_id}")
            return

        # -------------------------------------------------------
        # 15) Установка двустороннего режима ↔️
        # -------------------------------------------------------
        if cmd.startswith("fw_two_"):
            tgt = int(cmd.split("_")[-1])
            add_forward_link(chat_id, tgt, "twoway")
            add_forward_link(tgt, chat_id, "twoway")
            bot.send_message(chat_id, f"Установлена двусторонняя пересылка ↔️  {chat_id} ⇄ {tgt}")
            return

        # -------------------------------------------------------
        # 16) Удаление всех связей
        # -------------------------------------------------------
        if cmd.startswith("fw_del_"):
            tgt = int(cmd.split("_")[-1])
            remove_forward_link(chat_id, tgt)
            remove_forward_link(tgt, chat_id)
            bot.send_message(chat_id, f"Все связи с {tgt} удалены.")
            return

        # -------------------------------------------------------
        # 17) Выбор даты вручную
        # -------------------------------------------------------
        if cmd == "pick_date":
            bot.send_message(chat_id, "Введите дату:\n/view YYYY-MM-DD")
            return

    except Exception as e:
        log_error(f"on_callback error: {e}")

# ==========================================================
# SECTION 17 — Команды (start/help/поехали/view/prev/next/report/balance/etc.)
# ==========================================================

def send_info(chat_id: int, text: str):
    try:
        bot.send_message(chat_id, text)
    except Exception as e:
        log_error(f"send_info: {e}")


# -------------------- /поехали --------------------
@bot.message_handler(commands=["поехали"])
def cmd_enable_finance(msg):
    """
    Включает финансовый режим в ЧАТЕ.
    После включения можно пользоваться всеми функциями.
    """
    chat_id = msg.chat.id
    set_finance_mode(chat_id, True)
    save_data(data)
    send_info(chat_id, "🚀 Финансовый режим включён!\nОтправьте /start")
    return


# -------------------- /start --------------------
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


# -------------------- /help --------------------
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
        "/help — эта справка\n"
    )
    send_info(chat_id, help_text)


# -------------------- /ping --------------------
@bot.message_handler(commands=["ping"])
def cmd_ping(msg):
    send_info(msg.chat.id, "PONG — бот работает 🟢")


# -------------------- /view --------------------
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


# -------------------- /prev --------------------
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


# -------------------- /next --------------------
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


# -------------------- /balance --------------------
@bot.message_handler(commands=["balance"])
def cmd_balance(msg):
    chat_id = msg.chat.id
    if not require_finance(chat_id):
        return

    store = get_chat_store(chat_id)
    bal = store.get("balance", 0)
    send_info(chat_id, f"💰 Баланс: {fmt_num(bal)}")


# -------------------- /report --------------------
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

        # Заливаем в GDrive (если включён)
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


# -------------------- /csv --------------------
@bot.message_handler(commands=["csv"])
def cmd_csv(msg):
    """
    Экспортирует CSV текущего чата:
    - делает per-chat JSON/CSV
    - грузит на Google Drive
    - шлёт в чат
    - шлёт в backup канал
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

    # META для владельца
    if OWNER_ID and chat_id == int(OWNER_ID):
        meta = _load_csv_meta()
        if sent and getattr(sent, "document", None):
            meta["file_id_csv"] = sent.document.file_id
        meta["message_id_csv"] = getattr(sent, "message_id", meta.get("message_id_csv"))
        _save_csv_meta(meta)

    send_backup_to_channel(chat_id)


# -------------------- /json --------------------
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


# -------------------- /reset --------------------
@bot.message_handler(commands=["reset"])
def cmd_reset(msg):
    chat_id = msg.chat.id
    if not require_finance(chat_id):
        return
    send_info(chat_id, "Вы уверены, что хотите обнулить данные? Напишите ДА.")


# -------------------- /stopforward --------------------
@bot.message_handler(commands=["stopforward"])
def cmd_stopforward(msg):
    if str(msg.chat.id) != str(OWNER_ID):
        send_info(msg.chat.id, "Эта команда только для владельца.")
        return
    clear_forward_all()
    send_info(msg.chat.id, "Пересылка полностью отключена.")


# --------------------
# Включение/выключение бэкапов
# --------------------
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

    # Владелец видит все известные чаты → сохраняем их в его store
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
    global data
    try:
        chat_id = msg.chat.id
        text = (msg.text or "").strip()

        # 1) обновляем info и known_chats
        update_chat_info_from_message(msg)

        # 2) пересылка
        targets = resolve_forward_targets(chat_id)
        if targets:
            forward_text_anon(chat_id, msg, targets)

        store = get_chat_store(chat_id)
        wait = store.get("edit_wait")

        # ------------------------------
        # ADD
        # ------------------------------
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
                  }

                  store.setdefault("records", []).append(rec)
                  store.setdefault("daily_records", {}).setdefault(day_key, []).append(rec)
                  store["next_id"] = rid + 1
                  added_any = True

            # 2) СНАЧАЛА обновляем окно дня
            if added_any:
                  txt, _ = render_day_window(chat_id, day_key)
                  kb = build_main_keyboard(day_key, chat_id)
                  bot.send_message(chat_id, txt, reply_markup=kb, parse_mode="HTML")

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
                
        # ------------------------------
        # EDIT
        # ------------------------------
        # ---------------------------------------------------
        # Редактирование записи
        # ---------------------------------------------------
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
                # ------------------------------
        # RESTORE CONFIRMATION (FIXED)
        # ------------------------------
        if wait and wait.get("type") == "restore_confirm":

            expected = f"ВОССТАНОВИТЬ {wait['fname']}"
            if msg.text.strip().upper() != expected.upper():
                bot.send_message(chat_id, "❌ Подтверждение неверное. Восстановление отменено.")
                store["edit_wait"] = None
                save_data(data)
                return

            # СКАЧИВАЕМ ФАЙЛ
            file_info = bot.get_file(wait["file_id"])
            raw = bot.download_file(file_info.file_path)
            tmp_path = f"restore_{chat_id}_{wait['fname']}"

            with open(tmp_path, "wb") as f:
                f.write(raw)

            fname = wait["fname"]

            # -------------------------------
            # ВОССТАНОВЛЕНИЕ data.json
            # -------------------------------
            if fname == "data.json":
                global data
                os.replace(tmp_path, "data.json")
                data = load_data()

                # восстановление finance_active_chats
                finance_active_chats.clear()
                for cid, enabled in data.get("finance_active_chats", {}).items():
                    if enabled:
                        try:
                            finance_active_chats.add(int(cid))
                         except:
                            pass

                bot.send_message(chat_id, "🟢 data.json восстановлен.")
                store["edit_wait"] = None
                save_data(data)
                return

            # -------------------------------
            # ВОССТАНОВЛЕНИЕ csv_meta.json
            # -------------------------------
            if fname == "csv_meta.json":
                os.replace(tmp_path, "csv_meta.json")
                bot.send_message(chat_id, "🟢 csv_meta.json восстановлен.")
                store["edit_wait"] = None
                save_data(data)
                return

            # -------------------------------
            # ВОССТАНОВЛЕНИЕ per-chat JSON
            # -------------------------------
            if fname.startswith("data_") and fname.endswith(".json"):
                global data
                tgt = int(fname.replace("data_", "").replace(".json", ""))
                os.replace(tmp_path, fname)
                store2 = _load_json(fname, {})
                data.setdefault("chats", {})[str(tgt)] = store2
                finance_active_chats.add(tgt)
                save_data(data)
                save_chat_json(tgt)
                update_or_send_day_window(tgt, today_key())

                bot.send_message(chat_id, f"🟢 Чат {tgt} восстановлен.")
                store["edit_wait"] = None
                save_data(data)
                return

            # -------------------------------
            # ВОССТАНОВЛЕНИЕ per-chat CSV
            # -------------------------------
            if fname.startswith("data_") and fname.endswith(".csv"):
                global data
                os.replace(tmp_path, fname)
                #global data
                bot.send_message(chat_id, f"🟢 CSV {fname} восстановлен.")
                store["edit_wait"] = None
                save_data(data)
                return
                
        # ------------------------------
        # RESET CONFIRMATION
        # ------------------------------
        if text.upper() == "ДА":
            reset_chat_data(chat_id)
            bot.send_message(chat_id, "🔄 Данные чата обнулены.")
            return

    except Exception as e:
        log_error(f"handle_text: {e}")


#🟣🟣🟣🟣🟣🟣
# ==========================================================
# SECTION 18.2 — Media forwarding (анонимно + media_group)
# ==========================================================

@bot.message_handler(
    content_types=[
        "photo", "audio", "video", "voice",
        "video_note", "sticker", "animation", "document"
    ]
)
def handle_media_forward(msg):
    """
    Пересылка ВСЕХ медиа:
        • фото, видео, документы, GIF, аудио, голосовые
        • альбомы (media_group)
    НИКАКИХ конфликтов с restore — restore ловит ТОЛЬКО data.json/csv.
    """

    try:
        chat_id = msg.chat.id

        # 1) Обновляем known_chats
        update_chat_info_from_message(msg)

        # 2) Антипетля — не пересылаем сообщения, созданные ботом
        try:
            BOT_ID = bot.get_me().id
        except:
            BOT_ID = None

        if BOT_ID and msg.from_user and msg.from_user.id == BOT_ID:
            return

        # 3) Цели пересылки
        targets = resolve_forward_targets(chat_id)
        if not targets:
            return

        # 4) Альбомы (media_group)
        group_msgs = collect_media_group(chat_id, msg)
        if not group_msgs:
            return

        if len(group_msgs) > 1:
            forward_media_group_anon(chat_id, group_msgs, targets)
            return

        # 5) Одиночное медиа
        for dst, mode in targets:
            try:
                bot.copy_message(dst, chat_id, msg.message_id)
            except Exception as e:
                log_error(f"handle_media_forward to {dst}: {e}")

    except Exception as e:
        log_error(f"handle_media_forward error: {e}")
        
#🔄🔄🔄🔄🔄🔄🔄🔄
# ==========================================================
# SECTION 18.3 — Forwarding of location / contact / poll / venue
# ==========================================================

@bot.message_handler(
    content_types=["location", "contact", "poll", "venue"]
)
def handle_special_forward(msg):
    """
    Пересылка специальных сообщений:
        • Локация
        • Контакт
        • Опросы (Poll)
        • Места (Venue)
    """

    try:
        chat_id = msg.chat.id

        # 1) Обновляем known_chats
        update_chat_info_from_message(msg)

        # 2) Антипетля
        try:
            BOT_ID = bot.get_me().id
        except:
            BOT_ID = None

        if BOT_ID and msg.from_user and msg.from_user.id == BOT_ID:
            return

        # 3) Направления пересылки
        targets = resolve_forward_targets(chat_id)
        if not targets:
            return

        # 4) Пересылка
        for dst, mode in targets:
            try:
                bot.copy_message(dst, chat_id, msg.message_id)
            except Exception as e:
                log_error(f"handle_special_forward to {dst}: {e}")

    except Exception as e:
        log_error(f"handle_special_forward error: {e}")
        
# ==========================================================
# SECTION 18.4 — Restore from uploaded JSON/CSV (SAFE VERSION)
# ==========================================================

@bot.message_handler(content_types=["document"])
def handle_restore_files(msg):
    chat_id = msg.chat.id
    file = msg.document
    fname = file.file_name.lower()

    # ============================
    # 1) Супер-безопасный фильтр
    # ============================
    restore_file = (
        fname == "data.json"
        or fname == "csv_meta.json"
        or (fname.startswith("data_") and fname.endswith(".json"))
        or (fname.startswith("data_") and fname.endswith(".csv"))
    )

    # ❗ НЕ файл восстановления → отправляем в пересылку
    if not restore_file:
        return

    # ============================
    # 2) Подтверждение восстановления
    # ============================
    bot.reply_to(msg,
        f"⚠️ Вы уверены, что хотите восстановить файл {fname}?\n"
        f"Напишите: ВОССТАНОВИТЬ {fname}"
    )

    # Сохраняем ожидание подтверждения
    store = get_chat_store(chat_id)
    store["edit_wait"] = {"type": "restore_confirm", "fname": fname, "file_id": file.file_id}
    save_data(data)
    

# ==========================================================
# SECTION 19 — Keep-alive
# ==========================================================

KEEP_ALIVE_SEND_TO_OWNER = False  # по умолчанию — без уведомлений владельцу

def keep_alive_task():
    while True:
        try:
            # Self ping
            if APP_URL:
                try:
                    resp = requests.get(APP_URL, timeout=10)
                    log_info(f"Keep-alive ping -> {resp.status_code}")
                except Exception as e:
                    log_error(f"Keep-alive self error: {e}")

            # Notify owner (optional)
            if KEEP_ALIVE_SEND_TO_OWNER and OWNER_ID:
                try:
                    pass    # место для send_temp_message()
                except Exception as e:
                    log_error(f"Keep-alive notify error: {e}")

        except Exception as e:
            log_error(f"Keep-alive loop error: {e}")

        time.sleep(max(10, KEEP_ALIVE_INTERVAL_SECONDS))


def start_keep_alive_thread():
    t = threading.Thread(target=keep_alive_task, daemon=True)
    t.start()


# ==========================================================
# SECTION 20 — Webhook / Flask / main()
# ==========================================================

@app.route(f"/{BOT_TOKEN}", methods=["POST"])
def telegram_webhook():
    json_str = request.get_data().decode("utf-8")
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

    # 1. Восстановление данных из GDrive
    restored = restore_from_gdrive_if_needed()

    # 2. Загрузка локальных данных
    data = load_data()
    log_info(f"Данные загружены. Версия бота: {VERSION}")

    # 3. Webhook + планировщик
    set_webhook()
    start_keep_alive_thread()

    # 4. Уведомление владельца
    if OWNER_ID:
        try:
            bot.send_message(
                int(OWNER_ID),
                f"✅ Бот запущен (версия {VERSION}).\n"
                f"Восстановление: {'OK' if restored else 'пропущено'}"
            )
        except Exception:
            pass

    # 5. Запуск Flask
    app.run(host="0.0.0.0", port=PORT)


if __name__ == "__main__":
    main()