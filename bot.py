# Code_022.8 редактирование из сообщения
# • Только новая система пересылки A↔B
# • Владелец присутствует в списке чатов
# • Кнопки "Назад" во всех уровнях меню пересылки
# • Пересылка работает во всех направлениях (A→B, B→A, A↔B)
# • Визуальное отображение выбранных направлений
# ==========================================================

# 🧭 Description: Code_022.7 (на базе Code_022.6)
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

VERSION = "Code_022.8 📝"

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
        "forward_rules": {},   # общие правила пересылки A↔B
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
#2️⃣2️⃣2️⃣2️⃣2️⃣2️⃣2️⃣2️⃣2️⃣2️⃣2️⃣2️⃣2️⃣2️⃣2️⃣2️⃣2️⃣
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
        log_info(f"GDrive: downloaded {filename} → {dest_path}")
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
    """Helper to send/update file in BACKUP_CHAT_ID with csv_meta tracking."""
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
    Send per-chat JSON/CSV and optionally global CSV to BACKUP_CHAT_ID.
    """
    flags = backup_flags or {}
    if not flags.get("channel", True):
        log_info("Channel backup disabled.")
        return
    if not BACKUP_CHAT_ID:
        log_info("BACKUP_CHAT_ID not set.")
        return

    try:
        save_chat_json(chat_id)
        send_backup_to_channel_for_file(chat_json_file(chat_id), f"json_chat_{chat_id}")
        send_backup_to_channel_for_file(chat_csv_file(chat_id), f"csv_chat_{chat_id}")

        export_global_csv(data)
        send_backup_to_channel_for_file(CSV_FILE, "csv_global")

        if os.path.exists("csv_meta.json"):
            send_backup_to_channel_for_file("csv_meta.json", "csv_meta")

    except Exception as e:
        log_error(f"send_backup_to_channel({chat_id}): {e}")


# ==========================================================
# SECTION 9 — Forward rules persistence (owner file) — *A↔B only*
# ==========================================================

def _owner_data_file() -> str | None:
    """Файл владельца, где хранятся forward_rules."""
    if not OWNER_ID:
        return None
    try:
        return f"data_{int(OWNER_ID)}.json"
    except Exception:
        return None


def load_forward_rules():
    """
    Загружает только новую систему A↔B.
    Структура:
        {
            "A_chat_id": {
                "B_chat_id": "oneway_to" | "oneway_from" | "twoway"
            }
        }
    """
    try:
        path = _owner_data_file()
        if not path or not os.path.exists(path):
            return {}

        payload = _load_json(path, {}) or {}
        fr = payload.get("forward_rules", {})

        # корректируем типы
        fixed = {}
        for src, mapping in fr.items():
            if not isinstance(mapping, dict):
                continue
            fixed[str(src)] = {}
            for dst, mode in mapping.items():
                if mode not in ("oneway_to", "oneway_from", "twoway"):
                    continue
                fixed[str(src)][str(dst)] = mode

        return fixed
    except Exception as e:
        log_error(f"load_forward_rules: {e}")
        return {}


def persist_forward_rules_to_owner():
    """
    Сохраняет forward_rules ТОЛЬКО в файл владельца.
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
        
#3️⃣3️⃣3️⃣3️⃣3️⃣3️⃣3️⃣3️⃣3️⃣3️⃣3️⃣3️⃣3️⃣3️⃣3️⃣3️⃣3️⃣3️⃣3️⃣3️⃣3️⃣
#✅✅✅✅✅✅
# ==========================================================
# SECTION 10 — Forward logic (A↔B only)
# ==========================================================

def resolve_forward_targets(source_chat_id: int):
    """
    Возвращает список целей для пересылки из данного чата.
    Структура: [(dst_chat_id, mode), ...]
    mode:
        "oneway_to" — пересылка из source → dst
        "twoway"    — двусторонняя (для UI; по факту source → dst тоже есть)
    """
    fr = data.get("forward_rules", {})
    src = str(source_chat_id)
    if src not in fr:
        return []

    out = []
    for dst, mode in fr[src].items():
        # Для runtime используем только реальные направления source→dst
        if mode in ("oneway_to", "twoway"):
            try:
                out.append((int(dst), mode))
            except Exception:
                continue
    return out


def _ensure_forward_root():
    """Гарантирует наличие словаря forward_rules."""
    if "forward_rules" not in data or not isinstance(data["forward_rules"], dict):
        data["forward_rules"] = {}
    return data["forward_rules"]


def add_forward_link(src_chat_id: int, dst_chat_id: int, mode: str):
    """
    Добавляет/обновляет связь пересылки:
        mode: "oneway_to" | "twoway"
    """
    fr = _ensure_forward_root()
    src = str(src_chat_id)
    dst = str(dst_chat_id)
    fr.setdefault(src, {})[dst] = mode
    save_data(data)
    persist_forward_rules_to_owner()


def remove_forward_link(src_chat_id: int, dst_chat_id: int):
    """
    Удаляет связь пересылки src → dst, если есть.
    """
    fr = _ensure_forward_root()
    src = str(src_chat_id)
    dst = str(dst_chat_id)
    if src in fr and dst in fr[src]:
        del fr[src][dst]
        if not fr[src]:
            del fr[src]
    save_data(data)
    persist_forward_rules_to_owner()


def clear_forward_all():
    """Полностью отключает всю пересылку (все A↔B)."""
    data["forward_rules"] = {}
    save_data(data)
    persist_forward_rules_to_owner()


def get_pair_direction(a_chat_id: int, b_chat_id: int):
    """
    Возвращает (arrow, active) для пары (A, B) относительно всей системы:
        arrow:
            "🔄" — двусторонняя пересылка A↔B
            "➡️" — только A → B
            "⬅️" — только B → A
            ""   — пересылки нет
        active: True/False — есть ли любая активная связь между A и B
    """
    fr = data.get("forward_rules", {}) or {}
    A = str(a_chat_id)
    B = str(b_chat_id)

    a_map = fr.get(A, {})
    b_map = fr.get(B, {})

    # учитываем только реальные направления source→dst
    a_to_b = False
    b_to_a = False

    mode_ab = a_map.get(B)
    mode_ba = b_map.get(A)

    if mode_ab in ("oneway_to", "twoway"):
        a_to_b = True
    if mode_ba in ("oneway_to", "twoway"):
        b_to_a = True

    # На случай старых записей "oneway_from" (обратная логика):
    if mode_ab == "oneway_from":
        b_to_a = True
    if mode_ba == "oneway_from":
        a_to_b = True

    if a_to_b and b_to_a:
        return "🔄", True
    if a_to_b:
        return "➡️", True
    if b_to_a:
        return "⬅️", True
    return "", False


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
    Если это не альбом — возвращает список [msg].
    """
    gid = msg.media_group_id
    if not gid:
        return [msg]

    group = _media_group_cache.setdefault(chat_id, {})
    arr = group.setdefault(gid, [])
    arr.append(msg)

    # небольшая пауза, чтобы собрать остальные элементы
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
            # неизвестный тип в альбоме — пересылаем как есть по одному
            for dst, mode in targets:
                try:
                    bot.copy_message(dst, source_chat_id, msg.message_id)
                except Exception:
                    pass
            return

    for dst, mode in targets:
        try:
            bot.send_media_group(dst, media_list)
        except Exception as e:
            log_error(f"forward_media_group_anon to {dst}: {e}")


# ==========================================================
# SECTION 11 — Day window renderer
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


def build_edit_menu_keyboard(day_key: str, chat_id=None):
    """
    Меню редактирования дня.
    Здесь размещаем кнопку:
        🔀 Пересылка A↔B  — только у владельца.
    """
    kb = types.InlineKeyboardMarkup(row_width=2)

    kb.row(
        types.InlineKeyboardButton("📝 Редактировать запись", callback_data=f"d:{day_key}:edit_list"),
        types.InlineKeyboardButton("📂 Общий CSV", callback_data=f"d:{day_key}:csv_all")
    )

    kb.row(
        types.InlineKeyboardButton("📅 CSV за день", callback_data=f"d:{day_key}:csv_day"),
        types.InlineKeyboardButton("⚙️ Обнулить", callback_data=f"d:{day_key}:reset")
    )

    # 🔀 Пересылка A↔B — только владельцу
    if OWNER_ID and chat_id is not None and str(chat_id) == str(OWNER_ID):
        kb.row(
            types.InlineKeyboardButton("🔀 Пересылка A↔B", callback_data="fw_open")
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

#4️⃣4️⃣4️⃣4️⃣4️⃣4️⃣4️⃣4️⃣4️⃣4️⃣4️⃣4️⃣4️⃣4️⃣4️⃣4️⃣4️⃣4️⃣
# ==========================================================
# SECTION 12.1 — NEW FORWARD SYSTEM (Chat A ↔ B) — FULL UI
# ==========================================================

def _chat_title(cid: int):
    """Возвращаем красивое название чата для меню."""
    s = get_chat_store(cid)
    info = s.get("info", {})
    title = info.get("title")
    if not title:
        title = f"Чат {cid}"
    return title


def build_forward_source_menu():
    """
    Меню выбора чата A.
    В списке:
        • владелец
        • все известные чаты
    Отображение формата:
        <Имя чата>  ➡️/⬅️/🔄/ (или пусто) + 🔴 если активна связь
    """
    kb = types.InlineKeyboardMarkup()

    if not OWNER_ID:
        return kb

    owner_id = int(OWNER_ID)
    owner_store = get_chat_store(owner_id)

    # Список известных чатов
    known = owner_store.get("known_chats", {})

    # Добавляем владельца вручную
    all_chats = {str(owner_id): {"title": _chat_title(owner_id)}}
    all_chats.update(known)

    for cid, info in all_chats.items():
        try:
            int_cid = int(cid)
        except:
            continue

        title = info.get("title") or _chat_title(int_cid)

        # визуализация (с самим собой нет смысла)
        arrow = ""
        active = False
        if int_cid != owner_id:
            arrow, active = get_pair_direction(owner_id, int_cid)

        mark = f" {arrow}" if arrow else ""
        if active:
            mark += " 🔴"

        kb.row(
            types.InlineKeyboardButton(
                f"{title}{mark}",
                callback_data=f"fw_src:{cid}"
            )
        )

    kb.row(types.InlineKeyboardButton("🔙 Назад", callback_data="fw_back_root"))

    return kb



def build_forward_target_menu(A: int):
    """
    Меню выбора чата B для выбранного A.
    Формат:
        <Чат B>  ➡️/⬅️/🔄  + 🔴 если есть связь
    """
    kb = types.InlineKeyboardMarkup()

    owner_id = int(OWNER_ID)
    owner_store = get_chat_store(owner_id)
    known = owner_store.get("known_chats", {})

    # Добавляем владельца в список
    all_chats = {str(owner_id): {"title": _chat_title(owner_id)}}
    all_chats.update(known)

    for cid, info in all_chats.items():
        try:
            int_cid = int(cid)
        except:
            continue

        if int_cid == A:
            continue

        title = info.get("title") or _chat_title(int_cid)

        arrow, active = get_pair_direction(A, int_cid)

        mark = f" {arrow}" if arrow else ""
        if active:
            mark += " 🔴"

        kb.row(
            types.InlineKeyboardButton(
                f"{title}{mark}",
                callback_data=f"fw_tgt:{A}:{cid}"
            )
        )

    kb.row(types.InlineKeyboardButton("🔙 Назад", callback_data="fw_back_src"))

    return kb



def build_forward_mode_menu(A: int, B: int):
    """
    Меню выбора направления пересылки.
    Показывает текущее состояние и имя обоих чатов.
    """
    kb = types.InlineKeyboardMarkup()

    titleA = _chat_title(A)
    titleB = _chat_title(B)

    # текущий режим
    arrow, active = get_pair_direction(A, B)
    cur = f"{titleA} {arrow} {titleB}" if arrow else f"{titleA} — {titleB}"

    kb.row(types.InlineKeyboardButton(f"Текущее: {cur}", callback_data="noop"))

    kb.row(
        types.InlineKeyboardButton(
            f"➡️ {titleA} → {titleB}",
            callback_data=f"fw_mode:{A}:{B}:to"
        )
    )
    kb.row(
        types.InlineKeyboardButton(
            f"⬅️ {titleB} → {titleA}",
            callback_data=f"fw_mode:{A}:{B}:from"
        )
    )
    kb.row(
        types.InlineKeyboardButton(
            f"🔄 {titleA} ⇄ {titleB}",
            callback_data=f"fw_mode:{A}:{B}:two"
        )
    )
    kb.row(
        types.InlineKeyboardButton(
            f"❌ Отключить пересылку",
            callback_data=f"fw_mode:{A}:{B}:del"
        )
    )

    kb.row(
        types.InlineKeyboardButton(
            "🔙 Назад",
            callback_data=f"fw_back_tgt:{A}"
        )
    )

    return kb



def apply_forward_mode(A: int, B: int, mode: str):
    """
    Применяет режим:
        to   → A → B
        from → B → A
        two  → двусторонняя
        del  → очистить связи
    """
    # очистка
    remove_forward_link(A, B)
    remove_forward_link(B, A)

    if mode == "to":
        add_forward_link(A, B, "oneway_to")

    elif mode == "from":
        add_forward_link(B, A, "oneway_to")

    elif mode == "two":
        add_forward_link(A, B, "twoway")
        add_forward_link(B, A, "twoway")

    elif mode == "del":
        # уже удалено выше
        pass
        
#5️⃣5️⃣5️⃣5️⃣5️⃣5️⃣5️⃣5️⃣5️⃣5️⃣5️⃣5️⃣5️⃣5️⃣
# ==========================================================
# SECTION 13 — Add / Update / Delete
# ==========================================================

def add_record_to_chat(chat_id: int, amount: int, note: str, owner):
    """
    Добавляет запись в чат.
    ВНИМАНИЕ: окно дня обновляется СНАЧАЛА, затем делается сохранение + бэкап.
    """
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

    # Добавляем запись без пересчётов
    store.setdefault("records", []).append(rec)
    store.setdefault("daily_records", {}).setdefault(today_key(), []).append(rec)
    store["next_id"] = rid + 1

    # СНАЧАЛА обновляем окно дня
    day_key = today_key()
    txt, _ = render_day_window(chat_id, day_key)
    kb = build_main_keyboard(day_key, chat_id)
    bot.send_message(chat_id, txt, reply_markup=kb, parse_mode="HTML")

    # ПОТОМ делаем пересчёт глобальных данных
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



def update_record_in_chat(chat_id: int, rid: int, new_amount: int, new_note: str):
    """
    Обновляет запись.
    """
    store = get_chat_store(chat_id)
    found = None

    # Обновляем в store["records"]
    for r in store.get("records", []):
        if r["id"] == rid:
            r["amount"] = new_amount
            r["note"] = new_note
            found = r
            break

    if not found:
        return

    # Обновляем во всех daily_records
    for day, arr in store.get("daily_records", {}).items():
        for r in arr:
            if r["id"] == rid:
                r.update(found)

    # Пересчёт баланса
    store["balance"] = sum(x["amount"] for x in store["records"])

    # Пересчёт глобального
    data["records"] = []
    for cid, st in data.get("chats", {}).items():
        data["records"].extend(st.get("records", []))

    data["overall_balance"] = sum(x["amount"] for x in data["records"])

    save_data(data)
    save_chat_json(chat_id)
    export_global_csv(data)
    send_backup_to_channel(chat_id)



def delete_record_in_chat(chat_id: int, rid: int):
    """
    Удаляет запись.
    """
    store = get_chat_store(chat_id)

    store["records"] = [x for x in store["records"] if x["id"] != rid]

    for day, arr in list(store.get("daily_records", {}).items()):
        arr2 = [x for x in arr if x["id"] != rid]
        if arr2:
            store["daily_records"][day] = arr2
        else:
            del store["daily_records"][day]

    store["balance"] = sum(x["amount"] for x in store["records"])

    # Пересборка глобального списка
    data["records"] = []
    for cid, st in data.get("chats", {}).items():
        data["records"].extend(st.get("records", []))

    data["overall_balance"] = sum(x["amount"] for x in data["records"])

    save_data(data)
    save_chat_json(chat_id)
    export_global_csv(data)
    send_backup_to_channel(chat_id)


# ==========================================================
# SECTION 14 — Active window system
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
    Если нет — создаём заново.
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
    Если нет — выдаём подсказку /поехали.
    """
    if not is_finance_mode(chat_id):
        bot.send_message(chat_id, "⚙️ Финансовый режим отключён.\nАктивируйте командой /поехали")
        return False
    return True
    
#6️⃣6️⃣6️⃣6️⃣6️⃣6️⃣6️⃣6️⃣6️⃣6️⃣6️⃣6️⃣6️⃣6️⃣6️⃣6️⃣6️⃣6️⃣6️⃣
#🟠🟠🟠🟠🟠🟠🟠🟠🟠
# ==========================================================
# SECTION 16 — Callback handler (A↔B + calendar + day window)
# ==========================================================

@bot.callback_query_handler(func=lambda c: True)
def on_callback(call):
    """
    Универсальный обработчик всех callback_data:
      • fw_*  — новое меню пересылки A ↔ B (только для владельца)
      • c:*   — календарь
      • d:*   — окно дня, редактирование, CSV, отчёты
    """
    try:
        data_str = call.data or ""
        chat_id = call.message.chat.id

        # --------------------------------------------------
        # 0) техническое "ничего не делать"
        # --------------------------------------------------
        if data_str == "noop":
            try:
                bot.answer_callback_query(call.id)
            except Exception:
                pass
            return

        # --------------------------------------------------
        # 1) NEW FORWARD SYSTEM — все callback-и fw_*
        # --------------------------------------------------
        if data_str.startswith("fw_"):
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

            owner_id = int(OWNER_ID)

            # открыть выбор чата A
            if data_str == "fw_open":
                kb = build_forward_source_menu()
                try:
                    bot.edit_message_text(
                        "Выберите чат A:",
                        chat_id=chat_id,
                        message_id=call.message.message_id,
                        reply_markup=kb
                    )
                except Exception:
                    bot.send_message(chat_id, "Выберите чат A:", reply_markup=kb)
                return

            # назад из выбора A → в меню редактирования текущего дня владельца
            if data_str == "fw_back_root":
                owner_store = get_chat_store(owner_id)
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
                try:
                    bot.edit_message_text(
                        "Выберите чат A:",
                        chat_id=chat_id,
                        message_id=call.message.message_id,
                        reply_markup=kb
                    )
                except Exception:
                    bot.send_message(chat_id, "Выберите чат A:", reply_markup=kb)
                return

            # назад из выбора режима → снова выбор B для A
            if data_str.startswith("fw_back_tgt:"):
                try:
                    A = int(data_str.split(":", 1)[1])
                except Exception:
                    return
                kb = build_forward_target_menu(A)
                titleA = _chat_title(A)
                try:
                    bot.edit_message_text(
                        f"Источник пересылки: {titleA}\nВыберите чат B:",
                        chat_id=chat_id,
                        message_id=call.message.message_id,
                        reply_markup=kb
                    )
                except Exception:
                    bot.send_message(
                        chat_id,
                        f"Источник пересылки: {titleA}\nВыберите чат B:",
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
                titleA = _chat_title(A)
                try:
                    bot.edit_message_text(
                        f"Источник пересылки: {titleA}\nВыберите чат B:",
                        chat_id=chat_id,
                        message_id=call.message.message_id,
                        reply_markup=kb
                    )
                except Exception:
                    bot.send_message(
                        chat_id,
                        f"Источник пересылки: {titleA}\nВыберите чат B:",
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
                try:
                    bot.edit_message_text(
                        "Выберите режим пересылки:",
                        chat_id=chat_id,
                        message_id=call.message.message_id,
                        reply_markup=kb
                    )
                except Exception:
                    bot.send_message(
                        chat_id,
                        "Выберите режим пересылки:",
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

                # после применения показываем список чатов A заново с обновлёнными стрелками
                kb = build_forward_source_menu()
                try:
                    bot.edit_message_text(
                        "Маршрут обновлён.\nВыберите чат A:",
                        chat_id=chat_id,
                        message_id=call.message.message_id,
                        reply_markup=kb
                    )
                except Exception:
                    bot.send_message(
                        chat_id,
                        "Маршрут обновлён.\nВыберите чат A:",
                        reply_markup=kb
                    )
                return

            # любые прочие fw_* игнорируем
            return

        # --------------------------------------------------
        # 2) КАЛЕНДАРЬ (c:YYYY-MM-DD)
        # --------------------------------------------------
        if data_str.startswith("c:"):
            center_raw = data_str[2:]
            try:
                center_dt = datetime.strptime(center_raw, "%Y-%m-%d")
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
        # 3) ОКНО ДНЯ / РЕДАКТИРОВАНИЕ / CSV / ОТЧЁТЫ
        # --------------------------------------------------
        if not data_str.startswith("d:"):
            return

        # Формат: d:<day_key>:<cmd>
        try:
            _, day_key, cmd = data_str.split(":", 2)
        except ValueError:
            return

        store = get_chat_store(chat_id)

        # ---------- ОТКРЫТИЕ ДНЯ ----------
        if cmd == "open":
            store["current_view_day"] = day_key
            txt, _ = render_day_window(chat_id, day_key)
            kb = build_main_keyboard(day_key, chat_id)

            try:
                bot.edit_message_text(
                    txt,
                    chat_id=chat_id,
                    message_id=call.message.message_id,
                    reply_markup=kb,
                    parse_mode="HTML"
                )
            except Exception:
                sent = bot.send_message(
                    chat_id, txt, reply_markup=kb, parse_mode="HTML"
                )
                set_active_window_id(chat_id, day_key, sent.message_id)
            else:
                set_active_window_id(chat_id, day_key, call.message.message_id)
            return

        # ---------- ПРЕДЫДУЩИЙ / СЛЕДУЮЩИЙ ДЕНЬ ----------
        if cmd == "prev":
            try:
                d = datetime.strptime(day_key, "%Y-%m-%d") - timedelta(days=1)
            except ValueError:
                d = now_local() - timedelta(days=1)
            nd = d.strftime("%Y-%m-%d")
            store["current_view_day"] = nd
            txt, _ = render_day_window(chat_id, nd)
            kb = build_main_keyboard(nd, chat_id)

            try:
                bot.edit_message_text(
                    txt,
                    chat_id,
                    call.message.message_id,
                    reply_markup=kb,
                    parse_mode="HTML"
                )
            except Exception:
                sent = bot.send_message(
                    chat_id, txt, reply_markup=kb, parse_mode="HTML"
                )
                set_active_window_id(chat_id, nd, sent.message_id)
            else:
                set_active_window_id(chat_id, nd, call.message.message_id)
            return

        if cmd == "next":
            try:
                d = datetime.strptime(day_key, "%Y-%m-%d") + timedelta(days=1)
            except ValueError:
                d = now_local() + timedelta(days=1)
            nd = d.strftime("%Y-%m-%d")
            store["current_view_day"] = nd
            txt, _ = render_day_window(chat_id, nd)
            kb = build_main_keyboard(nd, chat_id)

            try:
                bot.edit_message_text(
                    txt,
                    chat_id,
                    call.message.message_id,
                    reply_markup=kb,
                    parse_mode="HTML"
                )
            except Exception:
                sent = bot.send_message(
                    chat_id, txt, reply_markup=kb, parse_mode="HTML"
                )
                set_active_window_id(chat_id, nd, sent.message_id)
            else:
                set_active_window_id(chat_id, nd, call.message.message_id)
            return

        # ---------- КАЛЕНДАРЬ К ДНЮ ----------
        if cmd == "calendar":
            try:
                cdt = datetime.strptime(day_key, "%Y-%m-%d")
            except Exception:
                cdt = now_local()

            kb = build_calendar_keyboard(cdt)
            try:
                bot.edit_message_reply_markup(
                    chat_id=chat_id,
                    message_id=call.message.message_id,
                    reply_markup=kb
                )
            except Exception:
                pass
            return

        # ---------- ОТЧЁТ ПО ДНЯМ ----------
        if cmd == "report":
            lines = ["📊 Отчёт:"]
            for dk, recs in sorted(store.get("daily_records", {}).items()):
                s = sum(r["amount"] for r in recs)
                lines.append(f"{dk}: {fmt_num(s)}")
            bot.send_message(chat_id, "\n".join(lines))
            return

        # ---------- ОБЩИЙ ИТОГ ----------
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

        # ---------- ИНФО ----------
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

        # ---------- МЕНЮ РЕДАКТИРОВАНИЯ ----------
        if cmd == "edit_menu":
            store["current_view_day"] = day_key
            kb = build_edit_menu_keyboard(day_key, chat_id)
            try:
                bot.edit_message_reply_markup(
                    chat_id=chat_id,
                    message_id=call.message.message_id,
                    reply_markup=kb
                )
            except Exception:
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

        # ---------- НАЗАД К ОСНОВНОМУ ОКНУ ----------
        if cmd == "back_main":
            store["current_view_day"] = day_key
            txt, _ = render_day_window(chat_id, day_key)
            kb = build_main_keyboard(day_key, chat_id)
            try:
                bot.edit_message_text(
                    txt,
                    chat_id=chat_id,
                    message_id=call.message.message_id,
                    reply_markup=kb,
                    parse_mode="HTML"
                )
            except Exception:
                bot.send_message(chat_id, txt, reply_markup=kb, parse_mode="HTML")
            return

        # ---------- CSV ----------
        if cmd == "csv_all":
            # экспорт общего CSV (все чаты)
            export_global_csv(data)
            bot.send_message(chat_id, "📂 Общий CSV сформирован.")
            return

        if cmd == "csv_day":
            # экспорт CSV за конкретный день текущего чата
            try:
                cmd_csv_day(chat_id, day_key)
            except NameError:
                # будет определён дальше в коде
                pass
            return

        # ---------- ДОБАВЛЕНИЕ ЗАПИСИ ----------
        if cmd == "add":
            store["edit_wait"] = {"type": "add", "day_key": day_key}
            save_data(data)
            bot.send_message(chat_id, "Введите сумму и комментарий, например:\n+500 обед")
            return

        # ---------- СПИСОК ЗАПИСЕЙ ДЛЯ РЕДАКТИРОВАНИЯ ----------
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

        # ---------- ВЫБОР КОНКРЕТНОЙ ЗАПИСИ ДЛЯ РЕДАКТИРОВАНИЯ ----------
        if cmd.startswith("edit_rec_"):
            try:
                rid = int(cmd.split("_")[-1])
            except Exception:
                return
            store["edit_wait"] = {"type": "edit", "day_key": day_key, "rid": rid}
            save_data(data)
            bot.send_message(chat_id, f"Введите новую сумму и текст для записи R{rid}:")
            return

        # ---------- ОБНУЛЕНИЕ ДАННЫХ С ПОДТВЕРЖДЕНИЕМ ----------
        if cmd == "reset":
            store["edit_wait"] = {"type": "reset_confirm", "day_key": day_key}
            save_data(data)
            bot.send_message(
                chat_id,
                "⚠️ Вы уверены, что хотите ОБНУЛИТЬ все данные этого чата?\n\n"
                "Напишите ответом: ДА"
            )
            return

        # ---------- ВЫБОР ДАТЫ ВРУЧНУЮ ----------
        if cmd == "pick_date":
            bot.send_message(chat_id, "Введите дату в формате:\n/view YYYY-MM-DD")
            return

    except Exception as e:
        log_error(f"on_callback error: {e}")
        
#7️⃣7️⃣7️⃣7️⃣7️⃣7️⃣7️⃣7️⃣7️⃣7️⃣7️⃣7️⃣7️⃣7️⃣7️⃣
# ==========================================================
# SECTION 17 — Bot commands
# ==========================================================

@bot.message_handler(commands=["start"])
def cmd_start(msg):
    chat_id = msg.chat.id
    day_key = today_key()

    if not is_finance_mode(chat_id):
        bot.send_message(
            chat_id,
            "👋 Привет! Финансовый режим в этом чате не включён.\n"
            "Включите его командой:\n\n<b>/поехали</b>",
            parse_mode="HTML"
        )
        return

    store = get_chat_store(chat_id)
    store["current_view_day"] = day_key

    update_or_send_day_window(chat_id, day_key)


@bot.message_handler(commands=["поехали"])
def cmd_start_finance(msg):
    chat_id = msg.chat.id
    set_finance_mode(chat_id, True)
    bot.send_message(chat_id, "⚙️ Финансовый режим активирован!")

    # создаём окно дня
    day_key = today_key()
    store = get_chat_store(chat_id)
    store["current_view_day"] = day_key

    update_or_send_day_window(chat_id, day_key)


@bot.message_handler(commands=["balance"])
def cmd_balance(msg):
    chat_id = msg.chat.id
    if not require_finance(chat_id):
        return
    store = get_chat_store(chat_id)
    bal = store.get("balance", 0)
    bot.send_message(chat_id, f"💰 Баланс: {fmt_num(bal)}")


@bot.message_handler(commands=["view"])
def cmd_view(msg):
    chat_id = msg.chat.id
    if not require_finance(chat_id):
        return
    parts = msg.text.strip().split()
    if len(parts) < 2:
        bot.send_message(chat_id, "Использование:\n/view YYYY-MM-DD")
        return
    day_key = parts[1]

    try:
        datetime.strptime(day_key, "%Y-%m-%d")
    except ValueError:
        bot.send_message(chat_id, "Неверный формат. Пример: /view 2025-01-15")
        return

    store = get_chat_store(chat_id)
    store["current_view_day"] = day_key
    update_or_send_day_window(chat_id, day_key)


@bot.message_handler(commands=["prev"])
def cmd_prev(msg):
    chat_id = msg.chat.id
    if not require_finance(chat_id):
        return

    store = get_chat_store(chat_id)
    day_key = store.get("current_view_day", today_key())

    try:
        d = datetime.strptime(day_key, "%Y-%m-%d") - timedelta(days=1)
    except:
        d = now_local() - timedelta(days=1)

    nd = d.strftime("%Y-%m-%d")
    store["current_view_day"] = nd
    update_or_send_day_window(chat_id, nd)


@bot.message_handler(commands=["next"])
def cmd_next(msg):
    chat_id = msg.chat.id
    if not require_finance(chat_id):
        return

    store = get_chat_store(chat_id)
    day_key = store.get("current_view_day", today_key())

    try:
        d = datetime.strptime(day_key, "%Y-%m-%d") + timedelta(days=1)
    except:
        d = now_local() + timedelta(days=1)

    nd = d.strftime("%Y-%m-%d")
    store["current_view_day"] = nd
    update_or_send_day_window(chat_id, nd)


@bot.message_handler(commands=["report"])
def cmd_report(msg):
    chat_id = msg.chat.id
    if not require_finance(chat_id):
        return
    store = get_chat_store(chat_id)

    lines = ["📊 Отчёт:"]
    for dk, recs in sorted(store.get("daily_records", {}).items()):
        s = sum(r["amount"] for r in recs)
        lines.append(f"{dk}: {fmt_num(s)}")

    bot.send_message(chat_id, "\n".join(lines))


@bot.message_handler(commands=["csv"])
def cmd_csv(msg):
    """
    Экспорт общего CSV + бэкап в канал + Drive.
    """
    chat_id = msg.chat.id
    if not require_finance(chat_id):
        return

    export_global_csv(data)
    bot.send_message(chat_id, "📂 Общий CSV сформирован и сохранён.")

    # бэкап
    send_backup_to_channel(chat_id)
    if backup_flags.get("drive", True):
        upload_to_gdrive(CSV_FILE)


@bot.message_handler(commands=["json"])
def cmd_json(msg):
    """
    Выгружает JSON текущего чата.
    """
    chat_id = msg.chat.id
    store = get_chat_store(chat_id)

    path = f"export_{chat_id}.json"
    _save_json(path, store)

    with open(path, "rb") as f:
        bot.send_document(chat_id, f, caption="JSON текущего чата")


@bot.message_handler(commands=["reset"])
def cmd_reset(msg):
    chat_id = msg.chat.id
    if not require_finance(chat_id):
        return

    store = get_chat_store(chat_id)
    store["edit_wait"] = {"type": "reset_confirm", "day_key": today_key()}
    save_data(data)

    bot.send_message(
        chat_id,
        "⚠️ Вы уверены, что хотите ОБНУЛИТЬ все данные этого чата?\n\n"
        "Напишите в ответ: ДА"
    )


@bot.message_handler(commands=["ping"])
def cmd_ping(msg):
    bot.send_message(msg.chat.id, "🏓 Pong!")


@bot.message_handler(commands=["stopforward"])
def cmd_stopforward(msg):
    chat_id = msg.chat.id
    if not OWNER_ID or str(chat_id) != str(OWNER_ID):
        bot.send_message(chat_id, "Эта команда доступна только владельцу.")
        return

    clear_forward_all()
    bot.send_message(chat_id, "🔕 Вся пересылка A↔B отключена.")


# ==========================================================
#  RESTORE MODE (for restoring data via documents)
# ==========================================================

@bot.message_handler(commands=["restore"])
def cmd_restore(msg):
    chat_id = msg.chat.id
    if not OWNER_ID or str(chat_id) != str(OWNER_ID):
        bot.send_message(chat_id, "⛔ Режим восстановления доступен только владельцу.")
        return

    global restore_mode
    restore_mode = True
    bot.send_message(
        chat_id,
        "♻️ РЕЖИМ ВОССТАНОВЛЕНИЯ ВКЛЮЧЁН.\n\n"
        "Теперь отправьте файл data.json, data_<chat>.json, CSV или csv_meta.json.\n"
        "После восстановления отправьте /restore_off."
    )


@bot.message_handler(commands=["restore_off"])
def cmd_restore_off(msg):
    chat_id = msg.chat.id
    if not OWNER_ID or str(chat_id) != str(OWNER_ID):
        bot.send_message(chat_id, "⛔ Доступно только владельцу.")
        return

    global restore_mode
    restore_mode = False
    bot.send_message(chat_id, "🔄 Режим восстановления выключен.")


# ==========================================================
#  BACKUP FLAG SWITCHES
# ==========================================================

@bot.message_handler(commands=["backup_gdrive_on"])
def cmd_gdrive_on(msg):
    chat_id = msg.chat.id
    if not OWNER_ID or str(chat_id) != str(OWNER_ID):
        return
    backup_flags["drive"] = True
    save_data(data)
    bot.send_message(chat_id, "☁️ Google Drive backup: ВКЛ.")


@bot.message_handler(commands=["backup_gdrive_off"])
def cmd_gdrive_off(msg):
    chat_id = msg.chat.id
    if not OWNER_ID or str(chat_id) != str(OWNER_ID):
        return
    backup_flags["drive"] = False
    save_data(data)
    bot.send_message(chat_id, "☁️ Google Drive backup: ВЫКЛ.")


@bot.message_handler(commands=["backup_channel_on"])
def cmd_chan_on(msg):
    chat_id = msg.chat.id
    if not OWNER_ID or str(chat_id) != str(OWNER_ID):
        return
    backup_flags["channel"] = True
    save_data(data)
    bot.send_message(chat_id, "📡 Backup в канал: ВКЛ.")


@bot.message_handler(commands=["backup_channel_off"])
def cmd_chan_off(msg):
    chat_id = msg.chat.id
    if not OWNER_ID or str(chat_id) != str(OWNER_ID):
        return
    backup_flags["channel"] = False
    save_data(data)
    bot.send_message(chat_id, "📡 Backup в канал: ВЫКЛ.")
    
#8️⃣8️⃣8️⃣8️⃣8️⃣8️⃣8️⃣8️⃣8️⃣8️⃣8️⃣8️⃣8️⃣8️⃣8️⃣8️⃣8️⃣8️⃣8️⃣8️⃣
# ==========================================================
# SECTION 18 — TEXT HANDLER (финансы + пересылка + restore)
# ==========================================================

def update_chat_info_from_message(msg):
    """
    Обновляет информацию о чате при КАЖДОМ сообщении.
    Хранится в store["info"] и store["known_chats"] у владельца.
    """
    chat_id = msg.chat.id
    store = get_chat_store(chat_id)
    info = store.setdefault("info", {})

    # Заголовок, username
    info["title"] = msg.chat.title or info.get("title") or f"Чат {chat_id}"
    info["username"] = msg.chat.username or info.get("username")
    info["type"] = msg.chat.type

    # Владелец видит ВСЕ чаты → кладём в его known_chats
    if OWNER_ID and str(chat_id) != str(OWNER_ID):
        owner_store = get_chat_store(int(OWNER_ID))
        kc = owner_store.setdefault("known_chats", {})
        kc[str(chat_id)] = {
            "title": info["title"],
            "username": info["username"],
            "type": info["type"],
        }
        save_chat_json(int(OWNER_ID))



@bot.message_handler(content_types=["text"])
def handle_text(msg):
    """
    Обрабатывает:
      • restore_mode (ДА → восстановить)
      • пересылку текста A↔B
      • финансовые записи (add/edit/delete)
    """
    chat_id = msg.chat.id
    text = msg.text.strip()
    update_chat_info_from_message(msg)

    global restore_mode

    # ================================
    # RESTORE MODE
    # ================================
    if restore_mode:
        # подтверждение "ДА" для восстановления
        store = get_chat_store(chat_id)
        ew = store.get("edit_wait")

        if ew and ew.get("type") == "restore_confirm":
            if text.upper() == "ДА":
                try:
                    bot.send_message(chat_id, "♻️ Восстановление данных…")
                    restore_file_switch(chat_id, ew.get("tmp_path"))
                except Exception as e:
                    bot.send_message(chat_id, f"❌ Ошибка: {e}")
            else:
                bot.send_message(chat_id, "Операция отменена.")

            store["edit_wait"] = None
            save_chat_json(chat_id)
            return

        # иначе restore-mode включён, но нет подтверждений → игнор
        return

    # ================================
    # ПЕРЕСЫЛКА ТЕКСТА A↔B
    # ================================
    targets = resolve_forward_targets(chat_id)
    if targets:
        forward_text_anon(chat_id, msg, targets)

    # ================================
    # ФИНАНСОВЫЙ ФУНКЦИОНАЛ
    # ================================
    if not is_finance_mode(chat_id):
        return

    store = get_chat_store(chat_id)
    ew = store.get("edit_wait")

    # --- Добавление записи ---
    if ew and ew.get("type") == "add":
        try:
            amount, note = split_amount_and_note(text)
        except Exception:
            bot.send_message(chat_id, "Ошибка. Введите сумму и текст, например:\n+500 обед")
            return

        add_record_to_chat(chat_id, amount, note, msg.from_user.id)
        store["edit_wait"] = None
        save_chat_json(chat_id)
        return

    # --- Редактирование записи ---
    if ew and ew.get("type") == "edit":
        rid = ew.get("rid")
        if not rid:
            store["edit_wait"] = None
            save_chat_json(chat_id)
            return

        try:
            amount, note = split_amount_and_note(text)
        except Exception:
            bot.send_message(chat_id, "Ошибка. Введите сумму и текст для обновления.")
            return

        update_record_in_chat(chat_id, rid, amount, note)
        store["edit_wait"] = None
        save_chat_json(chat_id)
        return

    # --- Подтверждение обнуления ---
    if ew and ew.get("type") == "reset_confirm":
        if text.upper() == "ДА":
            bot.send_message(chat_id, "🗑 Данные очищены.")
            # фактическое обнуление
            new_store = {
                "info": store.get("info", {}),
                "known_chats": store.get("known_chats", {}),
                "balance": 0,
                "records": [],
                "daily_records": {},
                "next_id": 1,
                "active_windows": {},
                "edit_wait": None,
                "edit_target": None,
                "current_view_day": today_key(),
            }
            data["chats"][str(chat_id)] = new_store

            save_data(data)
            save_chat_json(chat_id)

            update_or_send_day_window(chat_id, today_key())
        else:
            bot.send_message(chat_id, "Операция отменена.")

        store["edit_wait"] = None
        save_chat_json(chat_id)
        return



# ==========================================================
# SECTION 18.1 — MEDIA HANDLER (photo, video, voice…)
# ==========================================================

@bot.message_handler(content_types=[
    "photo", "video", "audio", "voice", "sticker",
    "animation", "video_note", "location", "contact"
])
def handle_media(msg):
    """
    Обрабатывает ВСЕ сообщения, кроме документов.
    Поддерживает:
       • пересылку A↔B
       • media-group (альбомы) через сбор
    """
    chat_id = msg.chat.id
    update_chat_info_from_message(msg)

    # если restore-mode → пересылка запрещена
    if restore_mode:
        return

    # 1) Собираем альбом, если нужно
    messages = collect_media_group(chat_id, msg)
    if not messages:
        return

    # 2) Пересылка
    targets = resolve_forward_targets(chat_id)
    if targets:
        if len(messages) == 1:
            forward_media_anon(chat_id, msg, targets)
        else:
            forward_media_group_anon(chat_id, messages, targets)

    # 3) Финансовая логика не применяется к media


# ==========================================================
# SECTION 18.2 — DOCUMENT HANDLER (пересылка + restore)
# ==========================================================

@bot.message_handler(content_types=["document"])
def handle_document(msg):
    """
    Документы:
       • если restore_mode → принимаем только JSON/CSV-файлы восстановления
       • иначе → пересылка A↔B
    """
    chat_id = msg.chat.id
    file = msg.document
    fname = (file.file_name or "").lower()

    update_chat_info_from_message(msg)

    global restore_mode

    # ==========================
    # RESTORE MODE
    # ==========================
    if restore_mode:
        # valid restoration files
        if (
            fname == "data.json" or
            fname == "csv_meta.json" or
            (fname.startswith("data_") and fname.endswith(".json")) or
            (fname.startswith("data_") and fname.endswith(".csv"))
        ):
            # сохраняем временно
            try:
                file_info = bot.get_file(file.file_id)
                downloaded = bot.download_file(file_info.file_path)
                tmp_path = f"restore_tmp_{chat_id}.bin"
                with open(tmp_path, "wb") as f:
                    f.write(downloaded)

                store = get_chat_store(chat_id)
                store["edit_wait"] = {
                    "type": "restore_confirm",
                    "tmp_path": tmp_path
                }
                save_chat_json(chat_id)

                bot.send_message(
                    chat_id,
                    "♻️ Найден файл данных.\n"
                    "Для восстановления напишите: ДА"
                )
            except Exception as e:
                bot.send_message(chat_id, f"❌ Ошибка: {e}")

        else:
            bot.send_message(chat_id, "Файл не похож на JSON/CSV данных.")
        return

    # ==========================
    # A↔B ПЕРЕСЫЛКА ДОКУМЕНТОВ
    # ==========================
    targets = resolve_forward_targets(chat_id)
    if targets:
        try:
            bot.copy_message(
                chat_id=targets[0][0],  # copy_message не принимает список → отправляем по одному
                from_chat_id=chat_id,
                message_id=msg.message_id
            )
        except Exception as e:
            log_error(f"forward document error: {e}")

        # отправляем остальным, если более одного назначения
        for dst, mode in targets[1:]:
            try:
                bot.copy_message(dst, chat_id, msg.message_id)
            except Exception:
                pass

    # Финансовая логика к документам не относится
    # Финансовая логика к документам не относится


# ==========================================================
# SECTION 18.3 — EDITED MESSAGE HANDLER (изменение исходных сообщений)
# ==========================================================

@bot.edited_message_handler(content_types=["text"])
def handle_edited_text(msg):
    """
    Позволяет исправлять финансовую запись просто изменив своё сообщение в чате.
    Поиск записи ведётся по message_id.
    """
    chat_id = msg.chat.id
    message_id = msg.message_id
    new_text = msg.text.strip()

    update_chat_info_from_message(msg)

    # restore-mode → правки запрещены
    if restore_mode:
        return

    # финансовый режим выключен → игнорировать
    if not is_finance_mode(chat_id):
        return

    store = get_chat_store(chat_id)
    day_key = today_key()

    # ищем запись в daily_records
    day_recs = store.get("daily_records", {}).get(day_key, [])
    target = None
    for r in day_recs:
        if r.get("msg_id") == message_id:
            target = r
            break

    if not target:
        return  # сообщение не является финансовой записью

    # парсим новое сообщение
    try:
        amount, note = split_amount_and_note(new_text)
    except Exception:
        bot.send_message(chat_id, "Ошибка при обновлении. Введите сумму и текст.")
        return

    # обновляем запись
    rid = target["id"]
    update_record_in_chat(chat_id, rid, amount, note)

    # обновляем окно
    update_or_send_day_window(chat_id, day_key)
    
    #9️⃣9️⃣9️⃣9️⃣9️⃣9️⃣9️⃣9️⃣9️⃣9️⃣9️⃣9️⃣9️⃣9️⃣9️⃣9️⃣
    # ==========================================================
# SECTION 19 — WEBHOOK, KEEP-ALIVE, FLASK SERVER
# ==========================================================

@app.route(f"/{BOT_TOKEN}", methods=["POST"])
def webhook_update():
    """
    Обрабатывает входящие обновления от Telegram.
    """
    try:
        json_str = request.data.decode("utf-8")
        update = telebot.types.Update.de_json(json_str)
        bot.process_new_updates([update])
    except Exception as e:
        log_error(f"webhook_update error: {e}")
    return "OK", 200


@app.route("/")
def index():
    return "FinanceBot A↔B is running.", 200


def set_webhook():
    """
    Устанавливает вебхук. Если APP_URL пустой — бот в режиме polling.
    """
    if not APP_URL:
        log_info("APP_URL пуст — запускаем polling.")
        return False

    wh_url = f"{APP_URL}/{BOT_TOKEN}"
    try:
        bot.remove_webhook()
        time.sleep(1)
        bot.set_webhook(url=wh_url)
        log_info(f"Webhook установлен: {wh_url}")
        return True
    except Exception as e:
        log_error(f"set_webhook error: {e}")
        return False


def keep_alive():
    """
    Периодический пинг вебхука для Render/других хостингов.
    """
    while True:
        try:
            if APP_URL:
                requests.get(APP_URL, timeout=5)
        except:
            pass
        time.sleep(KEEP_ALIVE_INTERVAL_SECONDS)


# ==========================================================
# SECTION 20 — BOT STARTUP (MAIN)
# ==========================================================

def startup():
    """
    Выполняется один раз при старте:
      • восстановление файлов из Google Drive (если нет локальных)
      • загрузка data.json
      • загрузка forward_rules владельца
      • запуск keep-alive
      • установка webhook или polling
    """
    # 1) Попытка восстановления
    restored = restore_from_gdrive_if_needed()
    if restored:
        log_info("Файлы восстановлены из GDrive.")

    # 2) Загружаем основную data.json
    global data
    data = load_data()

    # 3) Загружаем forward_rules владельца
    try:
        fr = load_forward_rules()
        if fr:
            data["forward_rules"] = fr
            log_info("forward_rules загружены из файла владельца.")
    except Exception as e:
        log_error(f"load_forward_rules: {e}")

    # 4) Keep-alive thread
    t = threading.Thread(target=keep_alive, daemon=True)
    t.start()

    # 5) Webhook or polling
    if APP_URL:
        ok = set_webhook()
        if not ok:
            log_error("Webhook не установлен — fallback to polling.")
            bot.infinity_polling(skip_pending=True)
    else:
        log_info("Polling без webhook.")
        bot.infinity_polling(skip_pending=True)
        
    # 6) Уведомление владельца о запуске
    if OWNER_ID:
        try:
            bot.send_message(
                int(OWNER_ID),
                f"🤖 Бот запущен!\nВерсия: {VERSION}\nВремя: {now_local().strftime('%Y-%m-%d %H:%M:%S')}"
            )
        except Exception as e:
            log_error(f"Cannot notify owner on startup: {e}")

if __name__ == "__main__":
    startup()