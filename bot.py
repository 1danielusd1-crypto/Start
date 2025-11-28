# Code_022.15 исправить
# Финансовый бот:
#  • Окно дня, календарь, отчёты
#  • Per-chat JSON/CSV: data_<chat_id>.json / data_<chat_id>.csv / csv_meta_<chat_id>.json
#  • Бэкап: JSON в чат + JSON/CSV/глобальный CSV в BACKUP_CHAT_ID
#  • Анонимная пересылка между чатами (только для владельца)
#  • Финансовый режим по /ok (и /поехали)

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

# runtime flags (и в data["backup_flags"])
backup_flags = {
    "channel": True,
}

# режим восстановления (JSON/CSV)
restore_mode = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

bot = telebot.TeleBot(BOT_TOKEN, parse_mode=None)
app = Flask(__name__)

# main in-memory store
data = {}

# чаты, где включён финансовый режим
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

    # восстановление флага channel
    flags = d.get("backup_flags") or {}
    backup_flags["channel"] = bool(flags.get("channel", True))

    # восстановление множества finance_active_chats
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
    CSV — даты по порядку, записи по времени.
    """
    try:
        store = data.get("chats", {}).get(str(chat_id), {})
        if not store:
            return

        chat_path_json = chat_json_file(chat_id)
        chat_path_csv = chat_csv_file(chat_id)
        chat_path_meta = chat_meta_file(chat_id)

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
    Европейский формат с явным знаком:
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


# регулярка на первое число даже внутри текста
num_re = re.compile(r"[+\-–]?\s*\d[\d\s.,_'’]*")


def parse_amount(raw: str) -> float:
    """
    Универсальный парсер:
    - любые разделители
    - смешанные форматы (1.234,56 / 1,234.56)
    - десятичный знак — самый правый разделитель
    - без явного знака → расход
    """
    s = raw.strip()

    is_negative = s.startswith("-") or s.startswith("–")
    is_positive = s.startswith("+")

    s_clean = s.lstrip("+-–").strip()
    s_clean = (
        s_clean.replace(" ", "")
        .replace("_", "")
        .replace("’", "")
        .replace("'", "")
    )

    if "," not in s_clean and "." not in s_clean:
        value = float(s_clean)
        if not is_positive and not is_negative:
            is_negative = True
        return -value if is_negative else value

    if "." in s_clean and "," in s_clean:
        if s_clean.rfind(",") > s_clean.rfind("."):
            s_clean = s_clean.replace(".", "")
            s_clean = s_clean.replace(",", ".")
        else:
            s_clean = s_clean.replace(",", "")
    else:
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

    if not is_positive and not is_negative:
        is_negative = True

    return -value if is_negative else value


def split_amount_and_note(text: str):
    """
    Возвращает:
        amount (float), note (str)
    """
    m = num_re.search(text)
    if not m:
        raise ValueError("no number found")

    raw_number = m.group(0)
    amount = parse_amount(raw_number)

    note = text.replace(raw_number, " ").strip()
    note = re.sub(r"\s+", " ", note).lower()

    return amount, note


def looks_like_amount(text):
    try:
        split_amount_and_note(text)
        return True
    except Exception:
        return False
#✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅
# ==========================================================
# SECTION 8 — Global CSV export & backup to channel/chat
# ==========================================================

def export_global_csv(d: dict):
    """
    Глобальный CSV по всем чатам (для BACKUP_CHAT_ID).
    """
    try:
        with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["chat_id", "ID", "short_id", "timestamp", "amount", "note", "owner", "day_key"])
            for cid, cdata in d.get("chats", {}).items():
                daily = cdata.get("daily_records", {})
                for dk in sorted(daily.keys()):
                    for r in sorted(daily[dk], key=lambda x: x.get("timestamp", "")):
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
    """
    Helper для BACKUP_CHAT_ID, с использованием csv_meta.json.
    """
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
      • JSON/CSV чата + глобальный CSV → в BACKUP_CHAT_ID
      • JSON этого чата → в сам чат
    """
    flags = backup_flags or {}
    if not flags.get("channel", True):
        log_info("Channel backup disabled (channel flag = False).")
        return

    try:
        # всегда сохраняем актуальные файлы
        save_chat_json(chat_id)

        # --- 1. Бэкап в канал (если задан BACKUP_CHAT_ID) ---
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

        return upgraded
    except Exception as e:
        log_error(f"load_forward_rules: {e}")
        return {}


def persist_forward_rules_to_owner():
    """
    Сохраняет forward_rules (НОВЫЙ формат) только в data_OWNER.json.
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
# SECTION 10 — Общая логика forward_rules
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
        except Exception:
            continue
    return out


def add_forward_link(src_chat_id: int, dst_chat_id: int, mode: str):
    fr = data.setdefault("forward_rules", {})
    src = str(src_chat_id)
    dst = str(dst_chat_id)
    fr.setdefault(src, {})[dst] = mode
    save_data(data)
    persist_forward_rules_to_owner()


def remove_forward_link(src_chat_id: int, dst_chat_id: int):
    fr = data.get("forward_rules", {})
    src = str(src_chat_id)
    dst = str(dst_chat_id)
    if src in fr and dst in fr[src]:
        del fr[src][dst]
    if src in fr and not fr[src]:
        del fr[src]
    save_data(data)
    persist_forward_rules_to_owner()


def clear_forward_all():
    data["forward_rules"] = {}
    persist_forward_rules_to_owner()
    save_data(data)

# ----------------------------------------------------------
#   ФУНКЦИИ АНOНИМНОЙ ПЕРЕСЫЛКИ
# ----------------------------------------------------------

def forward_text_anon(source_chat_id: int, msg, targets: list[tuple[int, str]]):
    for dst, mode in targets:
        try:
            bot.copy_message(dst, source_chat_id, msg.message_id)
        except Exception as e:
            log_error(f"forward_text_anon to {dst}: {e}")


def forward_media_anon(source_chat_id: int, msg, targets: list[tuple[int, str]]):
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
    Окно дня + сводка:
      • Расход за день
      • Приход за день
      • Итого за день
      • Остаток по чату
    """
    store = get_chat_store(chat_id)
    recs = store.get("daily_records", {}).get(day_key, [])
    lines = []

    # подпись (Сегодня / Вчера / Завтра)
    try:
        day_date = datetime.strptime(day_key, "%Y-%m-%d").date()
    except Exception:
        day_date = now_local().date()

    today = now_local().date()
    suffix = ""
    if day_date == today:
        suffix = " (Сегодня)"
    elif day_date == today - timedelta(days=1):
        suffix = " (Вчера)"
    elif day_date == today + timedelta(days=1):
        suffix = " (Завтра)"

    lines.append(f"📅 <b>{day_key}{suffix}</b>")
    lines.append("")

    recs_sorted = sorted(recs, key=lambda x: x.get("timestamp"))
    total_income = 0.0
    total_expense = 0.0

    for r in recs_sorted:
        amt = r["amount"]
        if amt >= 0:
            total_income += amt
        else:
            total_expense += -amt

        note = html.escape(r.get("note", ""))
        sid = r.get("short_id", f"R{r['id']}")
        lines.append(f"{sid} {fmt_num(amt)} <i>{note}</i>")

    if not recs_sorted:
        lines.append("Нет записей за этот день.")

    lines.append("")

    if recs_sorted:
        lines.append(f"📉 Расход за день: {fmt_num(-total_expense) if total_expense else fmt_num(0)}")
        lines.append(f"📈 Приход за день: {fmt_num(total_income) if total_income else fmt_num(0)}")
        net = total_income - total_expense
        lines.append(f"💰 Итого за день: {fmt_num(net)}")

    bal_chat = store.get("balance", 0)
    lines.append(f"🏦 Остаток по чату: {fmt_num(bal_chat)}")

    return "\n".join(lines), bal_chat

# ==========================================================
# SECTION 12 — Keyboards: main window, calendar, edit menu, forwarding
# ==========================================================

def build_main_keyboard(day_key: str, chat_id=None):
    kb = types.InlineKeyboardMarkup(row_width=3)

    kb.row(
        types.InlineKeyboardButton("➕ Добавить", callback_data=f"d:{day_key}:add"),
        types.InlineKeyboardButton("📝 Редактировать", callback_data=f"d:{day_key}:edit_menu")
    )

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
    Календарь на 31 день.
    Дни с записями помечаются точкой: • 12.03
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

    # кнопки пересылки — только для владельца
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
#✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅
# ==========================================================
# SECTION 12.1 — NEW FORWARD SYSTEM (Chat A ↔ B)
# ==========================================================

def build_forward_source_menu():
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
    Если режим выключен — подсказываем включить /ok (/поехали).
    """
    if not is_finance_mode(chat_id):
        send_info(chat_id, "⚙️ Финансовый режим выключен.\nАктивируйте командой /ok или /поехали")
        return False
    return True

# ==========================================================
# SECTION 16 — Callback handler
# ==========================================================

@bot.callback_query_handler(func=lambda c: True)
def on_callback(call):
    """
    Универсальный обработчик всех callback_data:
      • fw_*  — меню пересылки A ↔ B (только для владельца)
      • c:*   — календарь
      • d:*   — окно дня, редактирование, отчёт, CSV, обнуление
    """
    try:
        data_str = call.data or ""
        chat_id = call.message.chat.id

        # 1) NEW FORWARD SYSTEM — fw_*
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

            if data_str == "fw_open":
                kb = build_forward_source_menu()
                bot.edit_message_text(
                    "Выберите чат A:",
                    chat_id=chat_id,
                    message_id=call.message.message_id,
                    reply_markup=kb
                )
                return

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

            if data_str == "fw_back_src":
                kb = build_forward_source_menu()
                bot.edit_message_text(
                    "Выберите чат A:",
                    chat_id=chat_id,
                    message_id=call.message.message_id,
                    reply_markup=kb
                )
                return

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

            return

        # 2) КАЛЕНДАРЬ (c:YYYY-MM-DD)
        if data_str.startswith("c:"):
            center = data_str[2:]
            try:
                center_dt = datetime.strptime(center, "%Y-%m-%d")
            except ValueError:
                return

            kb = build_calendar_keyboard(center_dt, chat_id)
            try:
                bot.edit_message_reply_markup(
                    chat_id=chat_id,
                    message_id=call.message.message_id,
                    reply_markup=kb
                )
            except Exception:
                pass
            return

        # 3) ОКНО ДНЯ / РЕДАКТИРОВАНИЕ / ПЕРЕСЫЛКА
        if not data_str.startswith("d:"):
            return

        _, day_key, cmd = data_str.split(":", 2)
        store = get_chat_store(chat_id)

        # открыть конкретный день
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

        # сегодняшний день
        if cmd == "today":
            nd = today_key()
            txt, _ = render_day_window(chat_id, nd)
            kb = build_main_keyboard(nd, chat_id)
            store["current_view_day"] = nd
            bot.edit_message_text(
                txt,
                chat_id=chat_id,
                message_id=call.message.message_id,
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

            kb = build_calendar_keyboard(cdt, chat_id)
            bot.edit_message_reply_markup(
                chat_id=chat_id,
                message_id=call.message.message_id,
                reply_markup=kb
            )
            return

        # отчёт по дням (по этому чату)
        if cmd == "report":
            lines = ["📊 Отчёт по дням:"]
            for dk, recs in sorted(store.get("daily_records", {}).items()):
                s = sum(r["amount"] for r in recs)
                lines.append(f"{dk}: {fmt_num(s)}")
            bot.send_message(chat_id, "\n".join(lines))
            return

        # общий итог: логика OWNER / не OWNER
        if cmd == "total":
            chat_bal = store.get("balance", 0)

            # обычные чаты — только свой остаток
            if not OWNER_ID or str(chat_id) != str(OWNER_ID):
                bot.send_message(
                    chat_id,
                    f"💰 <b>Общий итог по этому чату:</b> {fmt_num(chat_bal)}",
                    parse_mode="HTML"
                )
                return

            # OWNER — расширенный вывод
            lines = []
            info = store.get("info", {})
            title = info.get("title") or f"Чат {chat_id}"

            lines.append("💰 <b>Общий итог (для владельца)</b>")
            lines.append("")
            lines.append(f"• Этот чат ({title}): <b>{fmt_num(chat_bal)}</b>")

            all_chats = data.get("chats", {})
            total_all = 0
            other_lines = []

            for cid, st in all_chats.items():
                try:
                    cid_int = int(cid)
                except Exception:
                    continue
                bal = st.get("balance", 0)
                total_all += bal
                if cid_int == chat_id:
                    continue
                info2 = st.get("info", {})
                title2 = info2.get("title") or f"Чат {cid_int}"
                other_lines.append(f"   • {title2}: {fmt_num(bal)}")

            if other_lines:
                lines.append("")
                lines.append("• Другие чаты:")
                lines.extend(other_lines)

            lines.append("")
            lines.append(f"• Всего по всем чатам: <b>{fmt_num(total_all)}</b>")

            bot.send_message(chat_id, "\n".join(lines), parse_mode="HTML")
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
                "/ok, /поехали — включить финансовый режим\n"
                "/start — открыть окно сегодняшнего дня\n"
                "/view YYYY-MM-DD — открыть день\n"
                "/prev /next — вчера/завтра\n"
                "/balance — баланс по чату\n"
                "/report — сводка по дням\n"
                "/csv — CSV этого чата\n"
                "/json — JSON этого чата\n"
                "/reset — обнулить данные (с подтверждением)\n"
                "/csv_all — общий CSV этого чата (все дни)\n"
                "/stopforward — отключить пересылку (OWNER)\n"
                "/backup_channel_on / _off — бэкап в канал\n"
                "/restore / /restore_off — режим восстановления\n"
                "/autoadd_info — авто-добавление по суммам\n"
                "/ping — проверка\n"
                "/help — справка\n"
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

        # список записей
        if cmd == "edit_list":
            store["current_view_day"] = day_key
            day_recs = store.get("daily_records", {}).get(day_key, [])
            if not day_recs:
                bot.answer_callback_query(call.id, "Нет записей за этот день", show_alert=True)
                return

            kb2 = types.InlineKeyboardMarkup(row_width=3)
            for r in day_recs:
                lbl = f"{r['short_id']} {fmt_num(r['amount'])} — {r.get('note','')}"
                rid = r["id"]
                kb2.row(
                    types.InlineKeyboardButton(lbl, callback_data="none"),
                    types.InlineKeyboardButton("✏️", callback_data=f"d:{day_key}:edit_rec_{rid}"),
                    types.InlineKeyboardButton("❌", callback_data=f"d:{day_key}:del_rec_{rid}")
                )

            kb2.row(
                types.InlineKeyboardButton("🔙 Назад", callback_data=f"d:{day_key}:edit_menu")
            )

            bot.edit_message_reply_markup(
                chat_id=chat_id,
                message_id=call.message.message_id,
                reply_markup=kb2
            )
            return

        # назад к основному окну
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

        # общий CSV (ТЕПЕРЬ только для этого чата)
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
            send_and_auto_delete(chat_id, "Введите сумму и комментарий: +500 пример", 15)
            schedule_cancel_edit(chat_id, 15)
            return

        # выбор записи для редактирования
        if cmd.startswith("edit_rec_"):
            rid = int(cmd.split("_")[-1])
            store["edit_wait"] = {"type": "edit", "day_key": day_key, "rid": rid}
            save_data(data)
            send_and_auto_delete(chat_id, f"Введите новую сумму и текст для записи R{rid}:", 30)
            schedule_cancel_edit(chat_id, 30)
            return

        # удаление записи
        if cmd.startswith("del_rec_"):
            rid = int(cmd.split("_")[-1])
            delete_record_in_chat(chat_id, rid)
            update_or_send_day_window(chat_id, day_key)
            send_and_auto_delete(chat_id, f"🗑 Запись R{rid} удалена.", 10)
            return

        # старое меню пересылки — тоже только для владельца
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
            send_and_auto_delete(chat_id, f"Установлена пересылка ➡️  {chat_id} → {tgt}", 10)
            return

        if cmd.startswith("fw_rev_"):
            tgt = int(cmd.split("_")[-1])
            add_forward_link(tgt, chat_id, "oneway_to")
            send_and_auto_delete(chat_id, f"Установлена пересылка ⬅️  {tgt} → {chat_id}", 10)
            return

        if cmd.startswith("fw_two_"):
            tgt = int(cmd.split("_")[-1])
            add_forward_link(chat_id, tgt, "twoway")
            add_forward_link(tgt, chat_id, "twoway")
            send_and_auto_delete(chat_id, f"Установлена двусторонняя пересылка ↔️  {chat_id} ⇄ {tgt}", 10)
            return

        if cmd.startswith("fw_del_"):
            tgt = int(cmd.split("_")[-1])
            remove_forward_link(chat_id, tgt)
            remove_forward_link(tgt, chat_id)
            send_and_auto_delete(chat_id, f"Все связи с {tgt} удалены.", 10)
            return

        # выбор даты вручную
        if cmd == "pick_date":
            store["edit_wait"] = {"type": "pick_date"}
            save_data(data)
            send_and_auto_delete(chat_id, "Введите дату в формате YYYY-MM-DD", 30)
            schedule_cancel_edit(chat_id, 30)
            return

    except Exception as e:
        log_error(f"on_callback error: {e}")

#✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅
# ==========================================================
# SECTION 13 — Add / Update / Delete (продолжение)
# ==========================================================

def add_record_to_chat(chat_id: int, amount: float, note: str, day_key: str):
    """
    Добавление записи в чат:
      • создаём новый ID
      • добавляем в records и daily_records[day_key]
      • обновляем балансы
    """
    store = get_chat_store(chat_id)

    rid = store.get("next_id", 1)
    store["next_id"] = rid + 1

    rec = {
        "id": rid,
        "short_id": f"R{rid}",
        "timestamp": now_local().isoformat(timespec="seconds"),
        "amount": amount,
        "note": note,
        "owner": chat_id,
        "day_key": day_key,
    }

    store.setdefault("records", []).append(rec)
    store.setdefault("daily_records", {}).setdefault(day_key, []).append(rec)

    store["balance"] = sum(x["amount"] for x in store["records"])

    data["records"] = data.get("records", []) + [rec]
    data["overall_balance"] = sum(x["amount"] for x in data["records"])

    save_data(data)
    save_chat_json(chat_id)
    export_global_csv(data)
    send_backup_to_channel(chat_id)


# ==========================================================
# SECTION 14 — Перенумерация записей по реальному порядку
# ==========================================================

def renumber_chat_records(chat_id: int):
    """
    Перенумеровывает записи по датам и времени:
      R1, R2, R3... в порядке day_key + timestamp.
    """
    store = get_chat_store(chat_id)
    daily = store.get("daily_records", {})
    all_recs = []

    for dk, recs in daily.items():
        for r in recs:
            all_recs.append((dk, r))

    all_recs.sort(key=lambda t: (t[0], t[1].get("timestamp", "")))

    new_id = 1
    for dk, r in all_recs:
        r["id"] = new_id
        r["short_id"] = f"R{new_id}"
        new_id += 1

    store["next_id"] = new_id
    store["records"] = [r for dk, r in all_recs]


# ==========================================================
# SECTION 15 — schedule_finalize: финализация после добавления
# ==========================================================

_finalize_timers = {}


def schedule_finalize(chat_id: int, day_key: str, delay: float = 3.0):
    """
    Отложенная финализация:
      • перенумерация
      • сохранение JSON/CSV
      • обновление окна
      • бэкап
    """
    def _job():
        store = get_chat_store(chat_id)

        # 0) перенумеровать
        renumber_chat_records(chat_id)

        # 1) сохранить
        save_chat_json(chat_id)
        save_data(data)
        export_global_csv(data)

        # 2) обновить окно
        update_or_send_day_window(chat_id, day_key)

        # 3) бэкап
        send_backup_to_channel(chat_id)

    t_prev = _finalize_timers.get((chat_id, day_key))
    if t_prev and t_prev.is_alive():
        try:
            t_prev.cancel()
        except:
            pass

    t = threading.Timer(delay, _job)
    _finalize_timers[(chat_id, day_key)] = t
    t.start()


# ==========================================================
# SECTION 16 — Cancel timers for edit_wait (add/reset/pick_date)
# ==========================================================

_edit_cancel_timers = {}


def schedule_cancel_edit(chat_id: int, delay: float = 15.0):
    """
    Через delay секунд сбрасывает store['edit_wait'],
    если пользователь ничего не ввёл.
    """
    def _job():
        store = get_chat_store(chat_id)
        if store.get("edit_wait"):
            store["edit_wait"] = None
            save_data(data)

    prev = _edit_cancel_timers.get(chat_id)
    if prev and prev.is_alive():
        try:
            prev.cancel()
        except:
            pass

    t = threading.Timer(delay, _job)
    _edit_cancel_timers[chat_id] = t
    t.start()


# ==========================================================
# SECTION 17 — send_and_auto_delete
# ==========================================================

def send_and_auto_delete(chat_id: int, text: str, delay: int = 10):
    """
    Отправляет сообщение и удаляет его через delay секунд.
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
# SECTION 18 — Обновление информации о чатах (для OWNER)
# ==========================================================

def update_chat_info_from_message(msg):
    """
    Обновляет данные о чате в store["info"],
    и добавляет чат в known_chats владельца.
    """
    chat_id = msg.chat.id
    store = get_chat_store(chat_id)

    info = store.setdefault("info", {})
    info["title"] = msg.chat.title or info.get("title") or f"Чат {chat_id}"
    info["username"] = msg.chat.username or info.get("username")
    info["type"] = msg.chat.type

    # владелец видит все чаты
    if OWNER_ID and str(chat_id) != str(OWNER_ID):
        try:
            owner_id = int(OWNER_ID)
        except:
            return

        owner_store = get_chat_store(owner_id)
        kc = owner_store.setdefault("known_chats", {})
        kc[str(chat_id)] = {
            "title": info["title"],
            "username": info["username"],
            "type": info["type"],
        }
        save_chat_json(owner_id)
#✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅
# ==========================================================
# SECTION 19 — Message handlers (text, media, documents)
# ==========================================================

@bot.message_handler(content_types=["text"])
def handle_text(msg):
    """
    Основной текстовый обработчик:
      • автообновление info и known_chats
      • пересылка сообщений (анонимно)
      • режимы add/edit/reset_confirm/pick_date
      • авто-добавление сумм (если включено)
    """
    try:
        chat_id = msg.chat.id
        text = (msg.text or "").strip()

        update_chat_info_from_message(msg)

        # ---------------------------
        # Пересылка (Анонизированная)
        # ---------------------------
        targets = resolve_forward_targets(chat_id)
        if targets:
            forward_text_anon(chat_id, msg, targets)

        # ---------------------------
        # Получаем store чата
        # ---------------------------
        store = get_chat_store(chat_id)
        wait = store.get("edit_wait")
        auto_add_enabled = store.get("settings", {}).get("auto_add", False)

        # ======================================================
        # RESET CONFIRM — подтверждение обнуления (ДА)
        # ======================================================
        if wait and wait.get("type") == "reset_confirm":
            if text.upper() == "ДА":
                reset_chat_data(chat_id)
                send_and_auto_delete(chat_id, "🧹 Данные этого чата обнулены.", 10)

                day_key = store.get("current_view_day", today_key())
                update_or_send_day_window(chat_id, day_key)

            else:
                send_and_auto_delete(chat_id, "Отмена обнуления.", 10)

            store["edit_wait"] = None
            save_data(data)
            return

        # ======================================================
        # PICK_DATE — выбор даты вручную
        # ======================================================
        if wait and wait.get("type") == "pick_date":
            try:
                datetime.strptime(text, "%Y-%m-%d")
                store["current_view_day"] = text
                save_data(data)
                update_or_send_day_window(chat_id, text)
            except Exception:
                send_and_auto_delete(chat_id, "Ошибка даты. Формат: YYYY-MM-DD", 10)

            store["edit_wait"] = None
            save_data(data)
            return

        # ======================================================
        # EDIT — изменение записи
        # ======================================================
        if wait and wait.get("type") == "edit":
            day_key = wait["day_key"]
            rid = wait["rid"]

            try:
                amount, note = split_amount_and_note(text)
            except Exception:
                send_and_auto_delete(chat_id, "Ошибка формата. Пример: 100 еда", 10)
                return

            update_record_in_chat(chat_id, rid, amount, note)
            update_or_send_day_window(chat_id, day_key)

            store["edit_wait"] = None
            save_data(data)
            return

        # ======================================================
        # ADD — добавление записи
        # ======================================================
        if wait and wait.get("type") == "add":
            day_key = wait["day_key"]

            try:
                amount, note = split_amount_and_note(text)
            except Exception:
                send_and_auto_delete(chat_id, "Ошибка формата. Пример: +750 кафе", 10)
                return

            add_record_to_chat(chat_id, amount, note, day_key)
            schedule_finalize(chat_id, day_key, 3)

            store["edit_wait"] = None
            save_data(data)
            return

        # ======================================================
        # AUTO_ADD — автоматическое добавление суммы без режима
        # ======================================================
        if auto_add_enabled and looks_like_amount(text):
            day_key = store.get("current_view_day", today_key())

            try:
                amount, note = split_amount_and_note(text)
            except Exception:
                return

            add_record_to_chat(chat_id, amount, note, day_key)
            schedule_finalize(chat_id, day_key, 3)
            return

        # ======================================================
        # Команды (обычный текст внутри handler)
        # ======================================================

        # HELP
        if text.startswith("/help"):
            bot.send_message(
                chat_id,
                "ℹ️ Команды:\n"
                "/ok — включить фин. режим\n"
                "/start — открыть сегодня\n"
                "/view <YYYY-MM-DD>\n"
                "/prev /next\n"
                "/balance\n"
                "/report\n"
                "/csv (CSV этого чата)\n"
                "/json (JSON этого чата)\n"
                "/csv_all — общий CSV этого чата\n"
                "/reset — обнуление\n"
                "/autoadd_info — режим авто-добавления\n"
                "/ping — проверка\n"
                "/stopforward — отключить пересылку\n"
                "/backup_channel_on / _off — бэкап в канал\n"
                "/restore / /restore_off — режим восстановления\n"
            )
            return

        # START → открыть сегодня
        if text.startswith("/start"):
            dk = today_key()
            store["current_view_day"] = dk
            save_data(data)
            update_or_send_day_window(chat_id, dk)
            return

        # OK / ПОЕХАЛИ — включить режим
        if text.startswith("/ok") or text.startswith("/поехали"):
            set_finance_mode(chat_id, True)
            save_data(data)
            send_and_auto_delete(chat_id, "Финансовый режим включён.", 8)
            dk = store.get("current_view_day", today_key())
            update_or_send_day_window(chat_id, dk)
            return

        # VIEW
        if text.startswith("/view"):
            parts = text.split()
            if len(parts) == 2:
                try:
                    datetime.strptime(parts[1], "%Y-%m-%d")
                    store["current_view_day"] = parts[1]
                    save_data(data)
                    update_or_send_day_window(chat_id, parts[1])
                except Exception:
                    send_and_auto_delete(chat_id, "Неверный формат даты", 10)
            return

        # PREV
        if text.startswith("/prev"):
            dk = store.get("current_view_day", today_key())
            d = datetime.strptime(dk, "%Y-%m-%d") - timedelta(days=1)
            nd = d.strftime("%Y-%m-%d")
            store["current_view_day"] = nd
            save_data(data)
            update_or_send_day_window(chat_id, nd)
            return

        # NEXT
        if text.startswith("/next"):
            dk = store.get("current_view_day", today_key())
            d = datetime.strptime(dk, "%Y-%m-%d") + timedelta(days=1)
            nd = d.strftime("%Y-%m-%d")
            store["current_view_day"] = nd
            save_data(data)
            update_or_send_day_window(chat_id, nd)
            return

        # BALANCE
        if text.startswith("/balance"):
            bal = store.get("balance", 0)
            bot.send_message(chat_id, f"💰 Баланс: {fmt_num(bal)}")
            return

        # REPORT
        if text.startswith("/report"):
            lines = ["📊 Отчёт по дням:"]
            for dk, recs in sorted(store.get("daily_records", {}).items()):
                s = sum(r["amount"] for r in recs)
                lines.append(f"{dk}: {fmt_num(s)}")
            bot.send_message(chat_id, "\n".join(lines))
            return

        # CSV (этот чат)
        if text.startswith("/csv"):
            cmd_csv(chat_id)
            return

        # CSV_ALL (все даты этого чата)
        if text.startswith("/csv_all"):
            cmd_csv_all(chat_id)
            return

        # JSON
        if text.startswith("/json"):
            cmd_json(chat_id)
            return

        # RESET
        if text.startswith("/reset"):
            store["edit_wait"] = {"type": "reset_confirm"}
            save_data(data)
            send_and_auto_delete(chat_id, "⚠️ Вы уверены? Напишите ДА", 15)
            schedule_cancel_edit(chat_id, 15)
            return

        # RESTORE ON
        if text.startswith("/restore"):
            global restore_mode
            restore_mode = True
            send_and_auto_delete(chat_id, "🔧 Режим восстановления включён.", 10)
            return

        # RESTORE OFF
        if text.startswith("/restore_off"):
            global restore_mode
            restore_mode = False
            send_and_auto_delete(chat_id, "Режим восстановления выключен.", 10)
            return

        # AUTOADD INFO
        if text.startswith("/autoadd_info"):
            aa = store.get("settings", {}).get("auto_add", False)
            send_and_auto_delete(chat_id, f"Авто-добавление: {'включено' if aa else 'выключено'}", 10)
            return

        # PING
        if text.startswith("/ping"):
            send_and_auto_delete(chat_id, "pong", 5)
            return

        # STOPFORWARD (owner)
        if text.startswith("/stopforward"):
            if OWNER_ID and str(chat_id) == str(OWNER_ID):
                clear_forward_all()
                send_and_auto_delete(chat_id, "Все маршруты пересылки удалены.", 10)
            else:
                send_and_auto_delete(chat_id, "Недоступно.", 10)
            return

    except Exception as e:
        log_error(f"handle_text error: {e}")


# ==========================================================
# SECTION 19.1 — MEDIA HANDLERS
# ==========================================================

@bot.message_handler(content_types=["photo", "video", "audio", "voice", "sticker", "location", "contact", "venue"])
def handle_media(msg):
    """
    Логика пересылки всех типов медиа.
    """
    try:
        chat_id = msg.chat.id
        update_chat_info_from_message(msg)

        targets = resolve_forward_targets(chat_id)
        if not targets:
            return

        if msg.media_group_id:
            group = collect_media_group(chat_id, msg)
            forward_media_group_anon(chat_id, group, targets)
            return

        forward_media_anon(chat_id, msg, targets)

    except Exception as e:
        log_error(f"handle_media: {e}")


# ==========================================================
# SECTION 19.2 — DOCUMENTS: restore + forward
# ==========================================================

@bot.message_handler(content_types=["document"])
def handle_document(msg):
    try:
        chat_id = msg.chat.id
        update_chat_info_from_message(msg)

        # Пересылка документа
        targets = resolve_forward_targets(chat_id)
        if targets:
            forward_media_anon(chat_id, msg, targets)

        # Если режим восстановления выключен — ничего не делаем
        if not restore_mode:
            return

        file = msg.document
        fname = (file.file_name or "").lower()

        # принимаем только json/csv
        if not (fname.endswith(".json") or fname.endswith(".csv")):
            send_and_auto_delete(chat_id, "Этот файл не подходит для восстановления.", 8)
            return

        # скачать файл
        file_info = bot.get_file(file.file_id)
        downloaded = bot.download_file(file_info.file_path)

        temp_path = f"restore_{fname}"
        with open(temp_path, "wb") as f:
            f.write(downloaded)

        # восстановление
        restore_file_switch(chat_id, temp_path, fname)

        # авто-выход из режима
        global restore_mode
        restore_mode = False
        send_and_auto_delete(chat_id, "Режим восстановления выключен.", 8)
        # очистка временного файла
        try:
            os.remove(temp_path)
        except:
            pass

    except Exception as e:
        log_error(f"handle_document: {e}")

# ==========================================================
# SECTION 19.3 — Restore logic
# ==========================================================

def restore_file_switch(chat_id: int, path: str, fname: str):
    """
    Восстановление системы хранения:
      • data.json  — весь проект
      • csv_meta.json
      • data_<cid>.json — JSON одного чата
      • data_<cid>.csv  — CSV одного чата
    """
    try:
        🎈
        # Восстановление глобального data.json
        #if fname == "data.json":
            #new_data = _load_json(path, {})
            #if isinstance(new_data, dict):
              #  _save_json(DATA_FILE, new_data)

                # перезагрузить данные в память
                #global data
                #data = load_data()

                #send_and_auto_delete(chat_id, "✔️ Восстановлен главный data.json", 10)
            #return
#🎈
        if fname == "data.json":
            new_data = _load_json(path, {})
            if isinstance(new_data, dict):
        # сохраняем файл
                _save_json(DATA_FILE, new_data)

        # ПЕРЕЗАГРУЖАЕМ ВСЕ ДАННЫЕ В ОПЕРАТИВНУЮ ПАМЯТЬ
                global data
                data = load_data()

                send_and_auto_delete(chat_id, "✔️ Восстановлен главный data.json", 10)

        # Обновляем окно после восстановления
                try:
                    update_or_send_day_window(chat_id, today_key())
                except:
                    pass

            return
        # csv_meta.json
        if fname == "csv_meta.json":
            meta = _load_json(path, {})
            if isinstance(meta, dict):
                _save_json(CSV_META_FILE, meta)
                send_and_auto_delete(chat_id, "✔️ Восстановлен csv_meta.json", 10)
            return

        # Восстановление JSON одного чата
        if fname.startswith("data_") and fname.endswith(".json"):
            cid = int(fname.split("_", 1)[1].split(".")[0])
            payload = _load_json(path, {})
            if payload:
                _save_json(chat_json_file(cid), payload)
                send_and_auto_delete(chat_id, f"✔️ Восстановлен data_{cid}.json", 10)
            return

        # Восстановление CSV одного чата
        if fname.startswith("data_") and fname.endswith(".csv"):
            cid = int(fname.split("_", 1)[1].split(".")[0])
            with open(chat_csv_file(cid), "wb") as f:
                f.write(open(path, "rb").read())
            send_and_auto_delete(chat_id, f"✔️ Восстановлен data_{cid}.csv", 10)
            return

    except Exception as e:
        log_error(f"restore_file_switch: {e}")
        send_and_auto_delete(chat_id, f"Ошибка восстановления: {e}", 10)
        update_or_send_day_window(chat_id, today_key())
        
# ==========================================================
# SECTION 19.4 — CSV / JSON commands
# ==========================================================

def cmd_csv(chat_id: int):
    """
    CSV конкретного чата.
    """
    save_chat_json(chat_id)

    path = chat_csv_file(chat_id)
    if not os.path.exists(path):
        send_and_auto_delete(chat_id, "CSV файла нет", 10)
        return

    try:
        with open(path, "rb") as f:
            bot.send_document(chat_id, f, caption=f"CSV для чата {chat_id}")
    except Exception as e:
        log_error(f"cmd_csv: {e}")


def cmd_csv_all(chat_id: int):
    """
    Общий CSV этого чата (все дни).
    """
    save_chat_json(chat_id)

    path = chat_csv_file(chat_id)
    if not os.path.exists(path):
        send_and_auto_delete(chat_id, "CSV файла нет", 10)
        return

    try:
        with open(path, "rb") as f:
            bot.send_document(chat_id, f, caption=f"Общий CSV всех операций чата {chat_id}")
    except Exception as e:
        log_error(f"cmd_csv_all: {e}")


def cmd_csv_day(chat_id: int, day_key: str):
    """
    CSV конкретного дня: data_<chat>_YYYY-MM-DD.csv
    """
    try:
        store = get_chat_store(chat_id)
        recs = store.get("daily_records", {}).get(day_key, [])
        if not recs:
            send_and_auto_delete(chat_id, "Нет записей за этот день", 10)
            return

        filename = f"data_{chat_id}_{day_key}.csv"
        with open(filename, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["ID", "short_id", "timestamp", "amount", "note"])
            for r in sorted(recs, key=lambda x: x.get("timestamp", "")):
                w.writerow([r["id"], r["short_id"], r["timestamp"], r["amount"], r["note"]])

        with open(filename, "rb") as f:
            bot.send_document(chat_id, f, caption=f"CSV за {day_key}")

    except Exception as e:
        log_error(f"cmd_csv_day: {e}")


def cmd_json(chat_id: int):
    """
    JSON файла чата.
    """
    save_chat_json(chat_id)
    path = chat_json_file(chat_id)

    if not os.path.exists(path):
        send_and_auto_delete(chat_id, "JSON не найден", 10)
        return

    try:
        with open(path, "rb") as f:
            bot.send_document(chat_id, f, caption=f"JSON чата {chat_id}")
    except Exception as e:
        log_error(f"cmd_json: {e}")
#✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅
# ==========================================================
# SECTION 20 — Reset chat data (обнуление)
# ==========================================================

def reset_chat_data(chat_id: int):
    """
    Полное обнуление данных чата:
      • баланс
      • записи
      • daily_records
      • next_id
      • active_windows
      • сохранение JSON/CSV
      • обновление окна дня
      • бэкап
    """
    try:
        store = get_chat_store(chat_id)

        store["balance"] = 0
        store["records"] = []
        store["daily_records"] = {}
        store["next_id"] = 1
        store["active_windows"] = {}
        store["edit_wait"] = None
        store["edit_target"] = None

        # сохраняем в общий data.json
        save_data(data)

        # сохраняем per-chat JSON/CSV/META
        save_chat_json(chat_id)

        # обновить глобальный CSV
        export_global_csv(data)

        # бэкап в канал + JSON в чат
        send_backup_to_channel(chat_id)

    except Exception as e:
        log_error(f"reset_chat_data({chat_id}): {e}")


# ==========================================================
# SECTION 21 — Keep-alive
# ==========================================================

def keep_alive():
    """
    Самопинг каждые 60 секунд для Render / UptimeRobot.
    """
    try:
        url = f"{APP_URL}/ping"
        requests.get(url, timeout=5)
    except Exception as e:
        log_error(f"keep_alive: {e}")

    threading.Timer(KEEP_ALIVE_INTERVAL_SECONDS, keep_alive).start()


# ==========================================================
# SECTION 22 — Webhook / Flask
# ==========================================================

@app.route(f"/{BOT_TOKEN}", methods=["POST"])
def webhook_update():
    try:
        json_str = request.data.decode("utf-8")
        update = telebot.types.Update.de_json(json_str)
        bot.process_new_updates([update])
    except Exception as e:
        log_error(f"webhook_update: {e}")
    return "OK", 200


@app.route("/ping", methods=["GET"])
def ping_route():
    return "pong", 200


# ==========================================================
# SECTION 23 — MAIN
# ==========================================================

def main():
    global data
    data = load_data()
    log_info("Data loaded. Starting bot...")

    if APP_URL:
        wh_url = f"{APP_URL}/{BOT_TOKEN}"
        try:
            bot.remove_webhook()
            time.sleep(0.5)
            bot.set_webhook(
                url=wh_url,
                drop_pending_updates=True,
            )
            log_info(f"Webhook установлен: {wh_url}")
        except Exception as e:
            log_error(f"Webhook error: {e}")
    else:
        log_info("APP_URL пуст — запускаем polling()")
        bot.remove_webhook()
        bot.infinity_polling(timeout=20, long_polling_timeout=15)

    keep_alive()


if __name__ == "__main__":
    main()
#✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅

#✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅✅