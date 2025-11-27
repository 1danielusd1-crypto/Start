# Code_022.15
# Финансовый бот:
#  • Окно дня, календарь, отчёты
#  • Per-chat JSON/CSV (data_<chat_id>.json / .csv / csv_meta_<chat_id>.json)
#  • Бэкап в Telegram-канал + JSON-бэкап в сам чат
#  • Анонимная пересылка между чатами (owner-only)
#  • Финансовый режим по команде /ok
#  • Поддержка auto_add, редактирование записей и edited_message
#  • Ручное восстановление JSON/CSV (restore_mode)

#==========================================================
# 🧭 Description: Code_022.15
#==========================================================

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

# ========== SECTION 2 — Environment & globals ==========

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
OWNER_ID = os.getenv("OWNER_ID", "").strip()
BACKUP_CHAT_ID = os.getenv("BACKUP_CHAT_ID", "").strip()
APP_URL = os.getenv("APP_URL", "").strip()
PORT = int(os.getenv("PORT", "8443"))

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set")

VERSION = "Code_022.15"

DEFAULT_TZ = "America/Argentina/Buenos_Aires"
KEEP_ALIVE_INTERVAL_SECONDS = 60

DATA_FILE = "data.json"
CSV_FILE = "data.csv"
CSV_META_FILE = "csv_meta.json"

# Global flags (runtime, also duplicated into data["backup_flags"])
backup_flags = {
    "channel": True,
}

# RESTORE MODE FLAG
# В этом режиме пересылка документов отключается,
# и документы используются только для восстановления.
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
        "backup_flags": {"channel": True},
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
        "channel": bool(backup_flags.get("channel", True)),
    }
    _save_json(DATA_FILE, d)


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
    """
    chats = data.setdefault("chats", {})

    store = chats.setdefault(
        str(chat_id),
        {
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
            "settings": {
                "auto_add": False
            },
        }
    )

    if "known_chats" not in store:
        store["known_chats"] = {}

    return store


def save_chat_json(chat_id: int):
    """
    Save per-chat JSON, CSV and META for one chat.
    Даты и записи в CSV упорядочены по дате и времени.
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

        # CSV — строго по датам и времени
        with open(chat_path_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["chat_id", "ID", "short_id", "timestamp", "amount", "note", "owner", "day_key"])

            daily = store.get("daily_records", {})
            for dk in sorted(daily.keys()):
                recs = sorted(daily[dk], key=lambda r: r.get("timestamp", ""))
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


# ==========================================================
# SECTION 6 — Number formatting & parsing (EU format, decimals)
# ==========================================================

def fmt_num(x):
    """
    Европейский формат вывода с обязательным знаком.
      +1234.56 → +1.234,56
      -800     → -800
      0        → +0
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


def looks_like_amount(text):
    try:
        amount, note = split_amount_and_note(text)
        return True
    except Exception:
        return False


# ==========================================================
# SECTION 8 — Global CSV export & backup to channel
# ==========================================================

def export_global_csv(d: dict):
    """Глобальный CSV со всеми чатами (используется только как бэкап в канал)."""
    try:
        with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["chat_id", "ID", "short_id", "timestamp", "amount", "note", "owner", "day_key"])
            for cid, cdata in d.get("chats", {}).items():
                daily = cdata.get("daily_records", {})
                for dk in sorted(daily.keys()):
                    recs = sorted(daily[dk], key=lambda r: r.get("timestamp", ""))
                    for r in recs:
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
    Бэкап:
      • per-chat JSON/CSV + глобальный CSV → в BACKUP_CHAT_ID (если задан)
      • JSON этого чата → в сам чат
    """
    flags = backup_flags or {}
    if not flags.get("channel", True):
        log_info("Channel backup disabled (channel flag = False).")
        return

    try:
        # всегда сохраняем актуальные файлы
        save_chat_json(chat_id)

        # --- 1. Бэкап в канал (если указан BACKUP_CHAT_ID) ---
        if BACKUP_CHAT_ID:
            send_backup_to_channel_for_file(chat_json_file(chat_id), f"json_chat_{chat_id}")
            send_backup_to_channel_for_file(chat_csv_file(chat_id), f"csv_chat_{chat_id}")

            export_global_csv(data)
            send_backup_to_channel_for_file(CSV_FILE, "csv_global")
            if os.path.exists("csv_meta.json"):
                send_backup_to_channel_for_file("csv_meta.json", "csv_meta")

        # --- 2. Бэкап JSON в тот же чат ---
        json_path = chat_json_file(chat_id)
        if os.path.exists(json_path):
            try:
                with open(json_path, "rb") as f:
                    bot.send_document(
                        chat_id,
                        f,
                        caption="🧾 Актуальный JSON-бэкап этого чата"
                    )
            except Exception as e:
                log_error(f"send_backup_to_channel chat backup {chat_id}: {e}")

    except Exception as e:
        log_error(f"send_backup_to_channel({chat_id}): {e}")

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
                # старый формат: было [1,2,3]
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
        
        # ==========================================================
# SECTION 10 — Общая логика forward_rules (пересылка)
# ==========================================================

def resolve_forward_targets(source_chat_id: int):
    """
    Возвращает список целей пересылки [(dst_chat_id, mode), ...]
    mode ∈ {"oneway_to", "oneway_from", "twoway"}.
    """
    fr = data.get("forward_rules", {})
    src = str(source_chat_id)
    if src not in fr:
        return []
    out = []
    for dst, mode in fr[src].items():
        try:
            out.append((int(dst), mode))
        except Exception:
            continue
    return out


def add_forward_link(src_chat_id: int, dst_chat_id: int, mode: str):
    """
    Добавляет или обновляет правило пересылки:
    mode: "oneway_to", "oneway_from", "twoway"
    """
    fr = data.setdefault("forward_rules", {})
    src = str(src_chat_id)
    dst = str(dst_chat_id)
    fr.setdefault(src, {})[dst] = mode
    save_data(data)


def remove_forward_link(src_chat_id: int, dst_chat_id: int):
    """
    Удаляет правило пересылки src → dst.
    """
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


def forward_text_anon(source_chat_id: int, msg, targets: list[tuple[int, str]]):
    """
    Анонимная пересылка текста:
    • копирует сообщение без упоминания имени  
    """
    for dst, mode in targets:
        try:
            bot.copy_message(dst, source_chat_id, msg.message_id)
        except Exception as e:
            log_error(f"forward_text_anon to {dst}: {e}")


def forward_media_anon(source_chat_id: int, msg, targets: list[tuple[int, str]]):
    """
    Анонимная пересылка фото/видео/документов/аудио.
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
    Собирает альбом (media_group) в кэш, пока не придут все элементы.
    Возвращает полный список сообщений альбома, когда он собран.
    """
    gid = msg.media_group_id
    if not gid:
        return [msg]

    group = _media_group_cache.setdefault(chat_id, {})
    arr = group.setdefault(gid, [])
    arr.append(msg)

    if len(arr) == 1:
        # небольшая задержка, чтобы успели дойти остальные
        time.sleep(0.2)

    complete = group.pop(gid, arr)
    return complete


def forward_media_group_anon(source_chat_id: int, messages: list, targets: list[tuple[int, str]]):
    """
    Пересылка собранного альбома (MediaGroup) анонимно.
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
            # Пересылаем по одной, если формат не поддержан альбомом
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
# SECTION 12 — Keyboards: main window, calendar, edit menu, forwarding
# ==========================================================

def build_main_keyboard(day_key: str, chat_id=None):
    kb = types.InlineKeyboardMarkup(row_width=3)

    kb.row(
        types.InlineKeyboardButton("➕ Добавить", callback_data=f"d:{day_key}:add"),
        types.InlineKeyboardButton("📝 Редактировать", callback_data=f"d:{day_key}:edit_menu")
    )

    # Вчера / Сегодня / Завтра
    kb.row(
        types.InlineKeyboardButton("⬅️ Вчера", callback_data=f"d:{day_key}:prev"),
        types.InlineKeyboardButton("📅 Сегодня", callback_data=f"d:{day_key}:today"),
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
    
    def build_calendar_keyboard(center_day: datetime, chat_id: int | None = None):
    """
    Календарь на 31 день вокруг center_day.
    Дни, где есть транзакции, помечены точкой "• ".
    """
    kb = types.InlineKeyboardMarkup(row_width=4)

    daily = {}
    if chat_id is not None:
        store = get_chat_store(chat_id)
        daily = store.get("daily_records", {})

    start_day = center_day - timedelta(days=15)
    for week in range(0, 32, 4):
        row = []
        for d in range(4):
            day = start_day + timedelta(days=week + d)
            label = day.strftime("%d.%m")
            key = day.strftime("%Y-%m-%d")

            if daily.get(key):
                label = "• " + label

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
    kb = types.InlineKeyboardMarkup(row_width=2)

    kb.row(
        types.InlineKeyboardButton("📝 Редактировать запись", callback_data=f"d:{day_key}:edit_list"),
        types.InlineKeyboardButton("📂 Общий CSV", callback_data=f"d:{day_key}:csv_all")
    )

    kb.row(
        types.InlineKeyboardButton("📅 CSV за день", callback_data=f"d:{day_key}:csv_day"),
        types.InlineKeyboardButton("⚙️ Обнулить", callback_data=f"d:{day_key}:reset")
    )

    # Кнопки пересылки — только для владельца
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
    Список берём из known_chats владельца (все чаты, где был бот).
    """
    kb = types.InlineKeyboardMarkup()

    if not OWNER_ID:
        return kb

    owner_store = get_chat_store(int(OWNER_ID))
    known = owner_store.get("known_chats", {})

    rules = data.get("forward_rules", {})

    for cid, info in known.items():
        try:
            int_cid = int(cid)
        except Exception:
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
    Направления:
        ➡️ owner → target
        ⬅️ target → owner
        ↔️ двусторонняя
        ❌ удалить связь
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

    kb.row(
        types.InlineKeyboardButton("🔙 Назад", callback_data="fw_back_src")
    )

    return kb
    
    def build_forward_mode_menu(A: int, B: int):
    """
    Меню выбора режима пересылки между чатами A и B.
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
    """
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
        remove_forward_link(A, B)
        remove_forward_link(B, A)
        
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
    except Exception:
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
        except Exception:
            pass

    sent = bot.send_message(chat_id, txt, reply_markup=kb, parse_mode="HTML")
    set_active_window_id(chat_id, day_key, sent.message_id)


# ==========================================================
# SECTION 15 — Финансовый режим + перенумерация записей
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
    Если нет — показываем подсказку.
    """
    if not is_finance_mode(chat_id):
        send_info(chat_id, "⚙️ Финансовый режим выключен.\nАктивируйте командой /ok")
        return False
    return True


def renumber_chat_records(chat_id: int):
    """
    Перенумерует записи чата по датам и времени:
      R1, R2, R3... по реальному порядку (день, время).
    """
    store = get_chat_store(chat_id)
    daily = store.get("daily_records", {})
    all_recs = []

    for dk, recs in daily.items():
        for r in recs:
            all_recs.append((dk, r))

    # сортируем по дате и времени
    all_recs.sort(key=lambda t: (t[0], t[1].get("timestamp", "")))

    new_id = 1
    for dk, r in all_recs:
        r["id"] = new_id
        r["short_id"] = f"R{new_id}"
        new_id += 1

    store["next_id"] = new_id

    # store["records"] пересобираем в том же порядке
    store["records"] = [r for dk, r in all_recs]


# ==========================================================
# SECTION 16 — Отложенная финализация (сохранение + бэкап)
# ==========================================================

_finalize_timers = {}


def schedule_finalize(chat_id: int, day_key: str, delay: float = 3.0):
    """
    Планирует мягкую финализацию:
      • перенумерацию записей
      • сохранение JSON/CSV
      • экспорт глобального CSV
      • бэкап в канал + JSON в чат
      • обновление окна дня
    """

    def _job():
        try:
            store = get_chat_store(chat_id)

            # 0. Перенумеруем записи (R1, R2...) по реальному порядку
            renumber_chat_records(chat_id)

            # 1. Сохраняем per-chat JSON/CSV
            save_chat_json(chat_id)

            # 2. Глобальные данные
            save_data(data)
            export_global_csv(data)

            # 3. Бэкап (в канал + JSON в чат)
            send_backup_to_channel(chat_id)

            # 4. Обновляем окно дня
            day = store.get("current_view_day", day_key)
            update_or_send_day_window(chat_id, day)

        except Exception as e:
            log_error(f"schedule_finalize job: {e}")

    key = (chat_id, day_key)
    old_t = _finalize_timers.get(key)
    if old_t and old_t.is_alive():
        try:
            old_t.cancel()
        except Exception:
            pass

    t = threading.Timer(delay, _job)
    _finalize_timers[key] = t
    t.start()


# ==========================================================
# SECTION 17 — Таймеры отмены edit_wait (add / reset / pick_date)
# ==========================================================

_edit_cancel_timers = {}


def schedule_cancel_edit(chat_id: int, delay: float = 15.0):
    """
    Через delay секунд очищает store['edit_wait'], если пользователь не ввёл данные.
    Используется для:
      • add (ввод суммы)
      • reset_confirm (подтверждение обнуления)
      • pick_date (ввод даты)
    """
    def _job():
        try:
            store = get_chat_store(chat_id)
            if store.get("edit_wait"):
                store["edit_wait"] = None
                save_data(data)
        except Exception as e:
            log_error(f"schedule_cancel_edit job: {e}")

    t_prev = _edit_cancel_timers.get(chat_id)
    if t_prev and t_prev.is_alive():
        try:
            t_prev.cancel()
        except Exception:
            pass

    t = threading.Timer(delay, _job)
    _edit_cancel_timers[chat_id] = t
    t.start()


# ==========================================================
# SECTION 18 — Базовые команды (ok, start, help, view, prev/next, report, csv/json)
# ==========================================================

def send_and_auto_delete(chat_id: int, text: str, delay: int = 10):
    try:
        m = bot.send_message(chat_id, text)
        def _delete():
            time.sleep(delay)
            try:
                bot.delete_message(chat_id, m.message_id)
            except Exception:
                pass
        threading.Thread(target=_delete, daemon=True).start()
    except Exception as e:
        log_error(f"send_and_auto_delete: {e}")


def send_info(chat_id: int, text: str):
    send_and_auto_delete(chat_id, text, 10)


@bot.message_handler(commands=["ok"])
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
    store = get_chat_store(chat_id)
    store["current_view_day"] = day_key
    save_data(data)

    txt, _ = render_day_window(chat_id, day_key)
    kb = build_main_keyboard(day_key, chat_id)

    sent = bot.send_message(chat_id, txt, reply_markup=kb, parse_mode="HTML")
    set_active_window_id(chat_id, day_key, sent.message_id)


@bot.message_handler(commands=["help"])
def cmd_help(msg):
    chat_id = msg.chat.id

    lines = [
        f"ℹ️ Финансовый бот — версия {VERSION}",
        "",
        "Основные команды:",
        "/ok — включить финансовый режим в чате",
        "/start — открыть окно сегодняшнего дня",
        "/help — эта справка",
        "",
        "Навигация по дням:",
        "/view YYYY-MM-DD — открыть конкретный день",
        "/prev — предыдущий день",
        "/next — следующий день",
        "",
        "Финансы:",
        "/balance — баланс по этому чату",
        "/report — краткий отчёт по дням этого чата",
        "/csv — CSV этого чата (все дни)",
        "/json — JSON этого чата",
        "/reset — обнулить данные чата (с подтверждением)",
        "",
        "Авто-добавление:",
        "/autoadd_info — включить/выключить авто-добавление сумм",
        "",
        "Служебные:",
        "/ping — проверка, жив ли бот",
        "/backup_channel_on — включить бэкап в канал",
        "/backup_channel_off — выключить бэкап в канал",
        "/restore — режим восстановления JSON/CSV",
        "/restore_off — выход из режима восстановления",
    ]

    if OWNER_ID and str(chat_id) == str(OWNER_ID):
        lines.append("")
        lines.append("Команды владельца:")
        lines.append("/stopforward — полностью отключить пересылку между чатами")

    send_info(chat_id, "\n".join(lines))


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

    store = get_chat_store(chat_id)
    store["current_view_day"] = day_key
    save_data(data)

    txt, _ = render_day_window(chat_id, day_key)
    kb = build_main_keyboard(day_key, chat_id)
    sent = bot.send_message(chat_id, txt, reply_markup=kb, parse_mode="HTML")
    set_active_window_id(chat_id, day_key, sent.message_id)


@bot.message_handler(commands=["prev"])
def cmd_prev(msg):
    chat_id = msg.chat.id
    if not require_finance(chat_id):
        return

    store = get_chat_store(chat_id)
    cur = store.get("current_view_day", today_key())
    d = datetime.strptime(cur, "%Y-%m-%d") - timedelta(days=1)
    day_key = d.strftime("%Y-%m-%d")

    store["current_view_day"] = day_key
    save_data(data)

    txt, _ = render_day_window(chat_id, day_key)
    kb = build_main_keyboard(day_key, chat_id)
    sent = bot.send_message(chat_id, txt, reply_markup=kb, parse_mode="HTML")
    set_active_window_id(chat_id, day_key, sent.message_id)


@bot.message_handler(commands=["next"])
def cmd_next(msg):
    chat_id = msg.chat.id
    if not require_finance(chat_id):
        return

    store = get_chat_store(chat_id)
    cur = store.get("current_view_day", today_key())
    d = datetime.strptime(cur, "%Y-%m-%d") + timedelta(days=1)
    day_key = d.strftime("%Y-%m-%d")

    store["current_view_day"] = day_key
    save_data(data)

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
    Общий CSV только по этому чату (по всем дням).
    """
    if not require_finance(chat_id):
        return

    try:
        save_chat_json(chat_id)
        per_csv = chat_csv_file(chat_id)
        if not os.path.exists(per_csv):
            send_info(chat_id, "Файл CSV ещё не создан.")
            return

        with open(per_csv, "rb") as f:
            bot.send_document(chat_id, f, caption="📂 Общий CSV этого чата (все дни)")
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
    Экспортирует CSV текущего чата и запускает бэкап.
    """
    chat_id = msg.chat.id
    if not require_finance(chat_id):
        return

    cmd_csv_all(chat_id)
    # дополнительно бэкап
    try:
        schedule_finalize(chat_id, get_chat_store(chat_id).get("current_view_day", today_key()), 1.0)
    except Exception as e:
        log_error(f"cmd_csv schedule_finalize: {e}")


@bot.message_handler(commands=["json"])
def cmd_json(msg):
    """
    Отправляет JSON этого чата.
    """
    chat_id = msg.chat.id
    if not require_finance(chat_id):
        return

    try:
        save_chat_json(chat_id)
        path = chat_json_file(chat_id)
        if not os.path.exists(path):
            send_info(chat_id, "JSON ещё не создан.")
            return
        with open(path, "rb") as f:
            bot.send_document(chat_id, f, caption="🧾 JSON этого чата")
    except Exception as e:
        log_error(f"cmd_json: {e}")


# ==========================================================
# SECTION 19 — Reset, backup flags, auto-add, stopforward, restore_mode
# ==========================================================

@bot.message_handler(commands=["reset"])
def cmd_reset(msg):
    chat_id = msg.chat.id
    if not require_finance(chat_id):
        return

    store = get_chat_store(chat_id)
    store["edit_wait"] = {"type": "reset_confirm"}
    save_data(data)

    send_and_auto_delete(
        chat_id,
        "⚠️ Вы уверены, что хотите обнулить данные этого чата?\nНапишите ДА для подтверждения.",
        15
    )
    schedule_cancel_edit(chat_id, 15)


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
            "settings": {
                "auto_add": False
            },
        }

    save_chat_json(chat_id)
    save_data(data)
    export_global_csv(data)
    send_backup_to_channel(chat_id)


@bot.message_handler(commands=["backup_channel_on"])
def cmd_backup_channel_on(msg):
    chat_id = msg.chat.id
    if OWNER_ID and str(chat_id) == str(OWNER_ID):
        backup_flags["channel"] = True
        save_data(data)
        send_info(chat_id, "✅ Бэкап в канал включён.")
    else:
        send_info(chat_id, "Эта команда доступна только владельцу.")


@bot.message_handler(commands=["backup_channel_off"])
def cmd_backup_channel_off(msg):
    chat_id = msg.chat.id
    if OWNER_ID and str(chat_id) == str(OWNER_ID):
        backup_flags["channel"] = False
        save_data(data)
        send_info(chat_id, "⛔ Бэкап в канал выключен.")
    else:
        send_info(chat_id, "Эта команда доступна только владельцу.")


@bot.message_handler(commands=["stopforward"])
def cmd_stop_forward(msg):
    chat_id = msg.chat.id
    if not OWNER_ID or str(chat_id) != str(OWNER_ID):
        send_info(chat_id, "Эта команда доступна только владельцу.")
        return
    clear_forward_all()
    send_info(chat_id, "🔁 Вся пересылка полностью отключена.")


@bot.message_handler(commands=["autoadd_info"])
def cmd_autoadd_info(msg):
    """
    Переключает авто-добавление сумм для чата.
    """
    chat_id = msg.chat.id
    if not require_finance(chat_id):
        return

    store = get_chat_store(chat_id)
    s = store.setdefault("settings", {})
    cur = s.get("auto_add", False)
    s["auto_add"] = not cur
    save_data(data)

    state = "включено" if s["auto_add"] else "выключено"
    send_info(chat_id, f"⚙️ Авто-добавление сумм: {state}.")


@bot.message_handler(commands=["restore"])
def cmd_restore(msg):
    """
    Включает режим восстановления: документы используются для восстановления,
    пересылка документов временно отключается.
    """
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


# ==========================================================
# SECTION 20 — Callback handler (inline-кнопки)
# ==========================================================

@bot.callback_query_handler(func=lambda call: True)
def on_callback(call):
    try:
        data_str = call.data or ""
        chat_id = call.message.chat.id

        # обработка нового FW меню A↔B
        if data_str.startswith("fw_src:") or data_str.startswith("fw_tgt:") or data_str.startswith("fw_mode:") \
                or data_str.startswith("fw_back_"):
            if not OWNER_ID or str(chat_id) != str(OWNER_ID):
                send_info(chat_id, "Меню пересылки доступно только владельцу.")
                return

            if data_str == "fw_back_root":
                kb = build_forward_source_menu()
                bot.edit_message_text(
                    "Выберите исходный чат (A):",
                    chat_id=chat_id,
                    message_id=call.message.message_id,
                    reply_markup=kb
                )
                return

            if data_str.startswith("fw_back_src"):
                kb = build_forward_source_menu()
                bot.edit_message_text(
                    "Выберите исходный чат (A):",
                    chat_id=chat_id,
                    message_id=call.message.message_id,
                    reply_markup=kb
                )
                return

            if data_str.startswith("fw_back_tgt:"):
                _, a_str = data_str.split(":", 1)
                A = int(a_str)
                kb = build_forward_target_menu(A)
                bot.edit_message_text(
                    f"Выберите целевой чат (B) для A={A}:",
                    chat_id=chat_id,
                    message_id=call.message.message_id,
                    reply_markup=kb
                )
                return

            if data_str.startswith("fw_src:"):
                _, cid = data_str.split(":", 1)
                A = int(cid)
                kb = build_forward_target_menu(A)
                bot.edit_message_text(
                    f"Выбран A={A}. Теперь выберите чат B:",
                    chat_id=chat_id,
                    message_id=call.message.message_id,
                    reply_markup=kb
                )
                return

            if data_str.startswith("fw_tgt:"):
                _, rest = data_str.split(":", 1)
                a_str, b_str = rest.split(":")
                A, B = int(a_str), int(b_str)
                kb = build_forward_mode_menu(A, B)
                bot.edit_message_text(
                    f"Настройка пересылки между A={A} и B={B}:",
                    chat_id=chat_id,
                    message_id=call.message.message_id,
                    reply_markup=kb
                )
                return

            if data_str.startswith("fw_mode:"):
                _, rest = data_str.split(":", 1)
                a_str, b_str, mode = rest.split(":")
                A, B = int(a_str), int(b_str)
                apply_forward_mode(A, B, mode)
                persist_forward_rules_to_owner()
                save_data(data)
                send_info(chat_id, f"Режим пересылки A={A}, B={B} обновлён: {mode}")
                return

        # старый формат callback_data: d:<day_key>:<cmd>...
        if data_str.startswith("d:"):
            parts = data_str.split(":")
            if len(parts) < 3:
                return

            _, day_key, cmd = parts[0], parts[1], parts[2]
            store = get_chat_store(chat_id)
            store["current_view_day"] = day_key
            save_data(data)

            # назад в главное окно
            if cmd == "back_main":
                update_or_send_day_window(chat_id, day_key)
                return

            # навигация по дням
            if cmd == "prev":
                d = datetime.strptime(day_key, "%Y-%m-%d") - timedelta(days=1)
                nd = d.strftime("%Y-%m-%d")
                store["current_view_day"] = nd
                save_data(data)
                txt, _ = render_day_window(chat_id, nd)
                kb = build_main_keyboard(nd, chat_id)
                bot.edit_message_text(
                    txt,
                    chat_id=chat_id,
                    message_id=call.message.message_id,
                    reply_markup=kb,
                    parse_mode="HTML"
                )
                set_active_window_id(chat_id, nd, call.message.message_id)
                return

            if cmd == "next":
                d = datetime.strptime(day_key, "%Y-%m-%d") + timedelta(days=1)
                nd = d.strftime("%Y-%m-%d")
                store["current_view_day"] = nd
                save_data(data)
                txt, _ = render_day_window(chat_id, nd)
                kb = build_main_keyboard(nd, chat_id)
                bot.edit_message_text(
                    txt,
                    chat_id=chat_id,
                    message_id=call.message.message_id,
                    reply_markup=kb,
                    parse_mode="HTML"
                )
                set_active_window_id(chat_id, nd, call.message.message_id)
                return

            # открыть конкретный день
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

            # кнопка "Сегодня"
            if cmd == "today":
                nd = today_key()
                store["current_view_day"] = nd
                save_data(data)
                txt, _ = render_day_window(chat_id, nd)
                kb = build_main_keyboard(nd, chat_id)
                bot.edit_message_text(
                    txt,
                    chat_id=chat_id,
                    message_id=call.message.message_id,
                    reply_markup=kb,
                    parse_mode="HTML"
                )
                set_active_window_id(chat_id, nd, call.message.message_id)
                return

            # календарь
            if cmd == "calendar":
                cdt = datetime.strptime(day_key, "%Y-%m-%d")
                kb = build_calendar_keyboard(cdt, chat_id)
                bot.edit_message_text(
                    "📅 Выберите день (• — есть записи):",
                    chat_id=chat_id,
                    message_id=call.message.message_id,
                    reply_markup=kb
                )
                return

            # меню редактирования
            if cmd == "edit_menu":
                kb = build_edit_menu_keyboard(day_key, chat_id)
                bot.edit_message_text(
                    f"📝 Редактирование за {day_key}:",
                    chat_id=chat_id,
                    message_id=call.message.message_id,
                    reply_markup=kb
                )
                return

            # CSV
            if cmd == "csv_all":
                cmd_csv_all(chat_id)
                return

            if cmd == "csv_day":
                cmd_csv_day(chat_id, day_key)
                return

            # Обнулить — через edit_wait
            if cmd == "reset":
                store["edit_wait"] = {"type": "reset_confirm"}
                save_data(data)
                send_and_auto_delete(
                    chat_id,
                    "⚠️ Вы уверены, что хотите обнулить данные этого чата?\nНапишите ДА для подтверждения.",
                    15
                )
                schedule_cancel_edit(chat_id, 15)
                return

            # кнопка "Добавить"
            if cmd == "add":
                store["edit_wait"] = {"type": "add", "day_key": day_key}
                save_data(data)
                send_and_auto_delete(chat_id, "Введите сумму и комментарий: +500 Пример", 15)
                schedule_cancel_edit(chat_id, 15)
                return

            # Выбрать день — ввод даты
            if cmd == "pick_date":
                store["edit_wait"] = {"type": "pick_date"}
                save_data(data)
                send_and_auto_delete(
                    chat_id,
                    "Введите дату в формате YYYY-MM-DD",
                    30
                )
                schedule_cancel_edit(chat_id, 30)
                return

            # Инфо
            if cmd == "info":
                cmd_help(call.message)
                return

            # Общий итог
            if cmd == "total":
                store = get_chat_store(chat_id)
                bal_this = store.get("balance", 0)

                # только владелец видит все чаты
                if OWNER_ID and str(chat_id) == str(OWNER_ID):
                    lines = ["💰 Общий итог (OWNER):"]
                    info_this = store.get("info", {})
                    title_this = info_this.get("title") or f"Чат {chat_id}"
                    lines.append(f"• Этот чат ({title_this}): {fmt_num(bal_this)}")

                    total_all = 0
                    total_all += bal_this

                    for cid, cstore in data.get("chats", {}).items():
                        icid = int(cid)
                        if icid == chat_id:
                            continue
                        bal_c = cstore.get("balance", 0)
                        info_c = cstore.get("info", {})
                        title_c = info_c.get("title") or f"Чат {cid}"
                        lines.append(f"• {title_c}: {fmt_num(bal_c)}")
                        total_all += bal_c

                    lines.append("")
                    lines.append(f"Всего по всем чатам: {fmt_num(total_all)}")

                    send_info(chat_id, "\n".join(lines))
                else:
                    send_info(chat_id, f"💰 Общий остаток по этому чату: {fmt_num(bal_this)}")
                return

            # forward-menu для владельца
            if cmd == "forward_menu":
                if not OWNER_ID or str(chat_id) != str(OWNER_ID):
                    send_info(chat_id, "Меню пересылки доступно только владельцу.")
                    return
                kb = build_forward_chat_list(day_key, chat_id)
                bot.edit_message_text(
                    "🔁 Настройка пересылки:\nВыберите чат для конфигурации:",
                    chat_id=chat_id,
                    message_id=call.message.message_id,
                    reply_markup=kb
                )
                return

            # настройка направления пересылки для конкретного чата (старый формат)
            if cmd.startswith("fw_cfg_"):
                if not OWNER_ID or str(chat_id) != str(OWNER_ID):
                    send_info(chat_id, "Меню пересылки доступно только владельцу.")
                    return
                target_cid = int(cmd.split("_", 2)[2])
                kb = build_forward_direction_menu(day_key, chat_id, target_cid)
                bot.edit_message_text(
                    f"Настройка пересылки для чата {target_cid}:",
                    chat_id=chat_id,
                    message_id=call.message.message_id,
                    reply_markup=kb
                )
                return

            if cmd.startswith("fw_one_"):
                target_cid = int(cmd.split("_", 2)[2])
                add_forward_link(chat_id, target_cid, "oneway_to")
                persist_forward_rules_to_owner()
                save_data(data)
                send_info(chat_id, f"Пересылка: {chat_id} ➡️ {target_cid}")
                return

            if cmd.startswith("fw_rev_"):
                target_cid = int(cmd.split("_", 2)[2])
                add_forward_link(target_cid, chat_id, "oneway_to")
                persist_forward_rules_to_owner()
                save_data(data)
                send_info(chat_id, f"Пересылка: {target_cid} ➡️ {chat_id}")
                return

            if cmd.startswith("fw_two_"):
                target_cid = int(cmd.split("_", 2)[2])
                add_forward_link(chat_id, target_cid, "twoway")
                add_forward_link(target_cid, chat_id, "twoway")
                persist_forward_rules_to_owner()
                save_data(data)
                send_info(chat_id, f"Пересылка: {chat_id} ↔️ {target_cid}")
                return

            if cmd.startswith("fw_del_"):
                target_cid = int(cmd.split("_", 2)[2])
                remove_forward_link(chat_id, target_cid)
                remove_forward_link(target_cid, chat_id)
                persist_forward_rules_to_owner()
                save_data(data)
                send_info(chat_id, f"Пересылка с чатом {target_cid} удалена.")
                return

    except Exception as e:
        log_error(f"on_callback error: {e}")
        
        # ==========================================================
# SECTION 25 — Flask, webhook, keep-alive, main()
# ==========================================================

@app.route("/" + BOT_TOKEN, methods=["POST"])
def webhook():
    try:
        json_str = request.data.decode("utf-8")
        update = telebot.types.Update.de_json(json_str)
        bot.process_new_updates([update])
    except Exception as e:
        log_error(f"webhook error: {e}")
    return "OK", 200


@app.route("/", methods=["GET"])
def index():
    return f"OK — {VERSION}", 200


def set_webhook():
    if not APP_URL:
        log_info("APP_URL не задан — запуск в режиме polling невозможен (используем webhook без URL).")
        return
    url = f"{APP_URL}/{BOT_TOKEN}"
    bot.remove_webhook()
    time.sleep(1)
    bot.set_webhook(url=url)
    log_info(f"Webhook установлен: {url}")


def keep_alive_loop():
    """
    Периодический ping самого себя (чтобы не засыпал хостинг).
    """
    if not APP_URL:
        return
    while True:
        try:
            requests.get(APP_URL, timeout=5)
        except Exception:
            pass
        time.sleep(KEEP_ALIVE_INTERVAL_SECONDS)


def start_keep_alive_thread():
    t = threading.Thread(target=keep_alive_loop, daemon=True)
    t.start()


def main():
    global data

    data = load_data()
    data["forward_rules"] = load_forward_rules()
    log_info(f"Данные загружены. Версия бота: {VERSION}")

    set_webhook()
    start_keep_alive_thread()

    if OWNER_ID:
        try:
            bot.send_message(
                int(OWNER_ID),
                f"✅ Бот запущен (версия {VERSION})."
            )
        except Exception:
            pass

    app.run(host="0.0.0.0", port=PORT)


if __name__ == "__main__":
    main()