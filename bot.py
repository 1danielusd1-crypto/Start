# Code_022.15 исправить
# Финансовый бот:
#  • Окно дня, календарь, отчёты
#  • Per-chat JSON/CSV: data_<chat>.json / data_<chat>.csv / csv_meta.json
#  • Бэкап в BACKUP_CHAT_ID: per-chat CSV + global CSV
#  • Пересылка между чатами (анонимно, через OWNER)
#  • Авто-добавление сумм, выбор дня, обнуление с подтверждением
#  • Полностью без Google Drive

# ==========================================================
# SECTION 1 — Imports & basic config
# ==========================================================

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

import telebot
from telebot import types

from flask import Flask, request

# ==========================================================
# SECTION 1.1 — ENV & basic constants
# ==========================================================

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
OWNER_ID = os.getenv("OWNER_ID", "").strip()
BACKUP_CHAT_ID = os.getenv("BACKUP_CHAT_ID", "").strip()

if OWNER_ID:
    try:
        OWNER_ID = int(OWNER_ID)
    except Exception:
        OWNER_ID = None
else:
    OWNER_ID = None

if BACKUP_CHAT_ID:
    try:
        BACKUP_CHAT_ID = int(BACKUP_CHAT_ID)
    except Exception:
        BACKUP_CHAT_ID = None
else:
    BACKUP_CHAT_ID = None

DEFAULT_TZ = os.getenv("DEFAULT_TZ", "America/Argentina/Buenos_Aires")

DATA_FILE = "data.json"
CSV_FILE = "data.csv"
CSV_META_FILE = "csv_meta.json"

WEBHOOK_HOST = os.getenv("WEBHOOK_HOST", "")
WEBHOOK_PATH = f"/{BOT_TOKEN}"
WEBHOOK_URL = f"{WEBHOOK_HOST}{WEBHOOK_PATH}" if WEBHOOK_HOST else ""

PORT = int(os.getenv("PORT", "5000"))

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")
app = Flask(__name__)

data_lock = threading.Lock()

data = {
    "chats": {},
    "backup_flags": {
        "channel": True,
    },
}

# режим восстановления (JSON/CSV)
restore_mode = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

logger = logging.getLogger(__name__)


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


def today_key():
    return now_local().strftime("%Y-%m-%d")


# ==========================================================
# SECTION 2 — Data load/save helpers
# ==========================================================

def load_data():
    global data
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            log_error(f"load_data error: {e}")


def save_data(d: dict | None = None):
    global data
    with data_lock:
        if d is not None:
            data = d
        try:
            with open(DATA_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            log_error(f"save_data error: {e}")


def get_chat_store(chat_id: int) -> dict:
    with data_lock:
        chats = data.setdefault("chats", {})
        store = chats.setdefault(str(chat_id), {})
    store.setdefault("daily_records", {})
    store.setdefault("balance", 0)
    store.setdefault("settings", {})
    return store


def chat_json_file(chat_id: int) -> str:
    return f"data_{chat_id}.json"


def chat_csv_file(chat_id: int) -> str:
    return f"data_{chat_id}.csv"


def chat_meta_file(chat_id: int) -> str:
    return f"csv_meta_{chat_id}.json"


def active_windows_file() -> str:
    return "active_windows.json"


def _load_csv_meta() -> dict:
    if os.path.exists(CSV_META_FILE):
        try:
            with open(CSV_META_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            log_error(f"_load_csv_meta error: {e}")
    return {}


def _save_csv_meta(meta: dict):
    try:
        with open(CSV_META_FILE, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log_error(f"_save_csv_meta error: {e}")


def save_chat_json(chat_id: int):
    store = get_chat_store(chat_id)
    path = chat_json_file(chat_id)
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(store, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log_error(f"save_chat_json error: {e}")


def save_chat_csv(chat_id: int):
    store = get_chat_store(chat_id)
    path = chat_csv_file(chat_id)
    daily = store.get("daily_records", {})

    try:
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["ID", "short_id", "timestamp", "amount", "note", "owner", "day_key"])
            for dk in sorted(daily.keys()):
                recs = daily[dk]
                recs_sorted = sorted(recs, key=lambda x: x.get("timestamp", ""))
                for r in recs_sorted:
                    w.writerow([
                        r.get("id"),
                        r.get("short_id"),
                        r.get("timestamp"),
                        r.get("amount"),
                        r.get("note"),
                        r.get("owner"),
                        dk,
                    ])
    except Exception as e:
        log_error(f"save_chat_csv error: {e}")


def save_day_csv(chat_id: int, day_key: str):
    store = get_chat_store(chat_id)
    daily = store.get("daily_records", {})
    recs = daily.get(day_key, [])

    path = f"data_{chat_id}_{day_key}.csv"
    try:
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["ID", "short_id", "timestamp", "amount", "note", "owner", "day_key"])
            for r in sorted(recs, key=lambda x: x.get("timestamp", "")):
                w.writerow([
                    r.get("id"),
                    r.get("short_id"),
                    r.get("timestamp"),
                    r.get("amount"),
                    r.get("note"),
                    r.get("owner"),
                    day_key,
                ])
    except Exception as e:
        log_error(f"save_day_csv error: {e}")


# ==========================================================
# SECTION 3 — Numbers: parse & format
# ==========================================================

def fmt_num(x):
    """
    Европейский формат вывода:
        1234.56  → 1.234,56
        1234     → 1.234
    """
    negative = x < 0
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

    return f"-{s}" if negative else s


num_re = re.compile(r"[+\-–]?\s*\d[\d\s.,_]*")


def parse_amount(text: str) -> float:
    s = text.strip().replace(" ", "").replace("_", "")
    s = s.replace("–", "-")

    if "," in s and "." in s:
        last_comma = s.rfind(",")
        last_dot = s.rfind(".")
        if last_comma > last_dot:
            s = s.replace(".", "")
            s = s.replace(",", ".")
        else:
            s = s.replace(",", "")
    elif "," in s:
        s = s.replace(".", "")
        s = s.replace(",", ".")
    else:
        pass

    return float(s)


def split_amount_and_note(text: str):
    m = num_re.search(text)
    if not m:
        raise ValueError("no number found")

    raw_number = m.group(0)
    amount = parse_amount(raw_number)

    note = text.replace(raw_number, " ").strip()
    note = re.sub(r"\s+", " ", note)

    return amount, note


def looks_like_amount(text: str):
    try:
        amount, note = split_amount_and_note(text)
        return True
    except Exception:
        return False
        
# ==========================================================
# SECTION 4 — ID helpers & balance
# ==========================================================

def next_record_id(chat_id: int) -> int:
    store = get_chat_store(chat_id)
    daily = store.get("daily_records", {})
    max_id = 0
    for dk, recs in daily.items():
        for r in recs:
            rid = int(r.get("id", 0))
            if rid > max_id:
                max_id = rid
    return max_id + 1


def next_short_id(chat_id: int, day_key: str) -> str:
    store = get_chat_store(chat_id)
    daily = store.get("daily_records", {})
    recs = daily.get(day_key, [])
    if not recs:
        return "R1"
    max_n = 0
    for r in recs:
        sid = str(r.get("short_id", ""))
        m = re.match(r"R(\d+)", sid)
        if m:
            num = int(m.group(1))
            if num > max_n:
                max_n = num
    return f"R{max_n + 1}"


def recalc_balance(chat_id: int):
    store = get_chat_store(chat_id)
    daily = store.get("daily_records", {})
    total = 0.0
    for dk in sorted(daily.keys()):
        for r in daily[dk]:
            total += float(r.get("amount", 0))
    store["balance"] = total
    save_data(data)


# ==========================================================
# SECTION 5 — Records operations (add / update / delete / reset)
# ==========================================================

def add_record_to_chat(chat_id: int, amount: float, note: str, day_key: str | None = None):
    store = get_chat_store(chat_id)
    if day_key is None:
        day_key = today_key()

    daily = store.setdefault("daily_records", {})
    recs = daily.setdefault(day_key, [])

    rid = next_record_id(chat_id)
    sid = next_short_id(chat_id, day_key)
    ts = now_local().strftime("%Y-%m-%d %H:%M:%S")

    rec = {
        "id": rid,
        "short_id": sid,
        "timestamp": ts,
        "amount": amount,
        "note": note,
        "owner": chat_id,
    }

    recs.append(rec)
    recs.sort(key=lambda x: x.get("timestamp", ""))
    recalc_balance(chat_id)
    save_data(data)


def update_record_in_chat(chat_id: int, record_id: int, amount: float, note: str):
    store = get_chat_store(chat_id)
    daily = store.get("daily_records", {})

    found = False
    for dk, recs in daily.items():
        for r in recs:
            if int(r.get("id", 0)) == int(record_id):
                r["amount"] = amount
                r["note"] = note
                found = True
                break
        if found:
            recs.sort(key=lambda x: x.get("timestamp", ""))
            break

    if found:
        recalc_balance(chat_id)
        save_data(data)


def delete_record_in_chat(chat_id: int, record_id: int):
    store = get_chat_store(chat_id)
    daily = store.get("daily_records", {})
    changed = False

    for dk, recs in daily.items():
        before = len(recs)
        recs[:] = [r for r in recs if int(r.get("id", 0)) != int(record_id)]
        after = len(recs)
        if after != before:
            changed = True
            recs.sort(key=lambda x: x.get("timestamp", ""))
            break

    if changed:
        recalc_balance(chat_id)
        save_data(data)


def reset_chat_data(chat_id: int):
    store = get_chat_store(chat_id)
    store["daily_records"] = {}
    store["balance"] = 0
    save_data(data)


# ==========================================================
# SECTION 6 — Finance mode flag
# ==========================================================

def is_finance_mode(chat_id: int) -> bool:
    store = get_chat_store(chat_id)
    return bool(store.get("settings", {}).get("finance_mode", False))


def set_finance_mode(chat_id: int, value: bool):
    store = get_chat_store(chat_id)
    store.setdefault("settings", {})["finance_mode"] = bool(value)
    save_data(data)


def require_finance(chat_id: int) -> bool:
    if not is_finance_mode(chat_id):
        send_and_auto_delete(
            chat_id,
            "⚙️ Финансовый режим выключен.\n"
            "Активируйте командой /поехали",
            15,
        )
        return False
    return True


# ==========================================================
# SECTION 7 — CSV / global backup helpers
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
        
        old_msg_id = meta.get(msg_key)

        with open(base_path, "rb") as f:
            file_bytes = f.read()

        if old_msg_id:
            try:
                bot.edit_message_media(
                    chat_id=BACKUP_CHAT_ID,
                    message_id=old_msg_id,
                    media=types.InputMediaDocument(
                        media=file_bytes,
                        caption=f"{base_path} (обновлено)"
                    )
                )
                return
            except Exception as e:
                log_error(f"edit_message_media {base_path}: {e}")

        try:
            sent = bot.send_document(
                BACKUP_CHAT_ID,
                document=file_bytes,
                visible_file_name=os.path.basename(base_path),
                caption=f"{base_path} (новый)"
            )
            meta[msg_key] = sent.message_id
            _save_csv_meta(meta)

        except Exception as e:
            log_error(f"send_backup_to_channel_for_file({base_path}): {e}")

    except Exception as e:
        log_error(f"send_backup_to_channel_for_file error: {e}")


def send_json_backup(chat_id: int):
    """Бэкап per-chat JSON в BACKUP_CHAT_ID."""
    if not BACKUP_CHAT_ID:
        return

    path = chat_json_file(chat_id)
    if not os.path.exists(path):
        return
    try:
        with open(path, "rb") as f:
            b = f.read()
        bot.send_document(
            BACKUP_CHAT_ID,
            document=b,
            visible_file_name=f"data_{chat_id}.json",
            caption=f"JSON чата {chat_id}"
        )
    except Exception as e:
        log_error(f"send_json_backup error: {e}")


# ==========================================================
# SECTION 8 — Anonymized forwarding engine
# ==========================================================

def resolve_forward_targets(chat_id: int):
    """
    Возвращает список chat_id, куда надо пересылать сообщения.
    """
    try:
        chats = data.get("chats", {})
        store = chats.get(str(OWNER_ID), {})
        frules = store.get("forward_rules", {})
    except Exception:
        return []

    return frules.get(str(chat_id), [])


def forward_text_anon(src_chat_id: int, msg, targets: list[int]):
    """Анонимная пересылка текста."""
    forward_text = msg.text or ""
    for t in targets:
        try:
            bot.send_message(t, forward_text)
        except Exception as e:
            log_error(f"forward_text_anon({src_chat_id}->{t}): {e}")


def forward_media_anon(src_chat_id: int, msg, targets: list[int]):
    """Анонимная пересылка одиночного фото/видео/document/audio."""
    for t in targets:
        try:
            if msg.content_type == "photo":
                file_id = msg.photo[-1].file_id
                bot.send_photo(t, file_id)

            elif msg.content_type == "video":
                bot.send_video(t, msg.video.file_id)

            elif msg.content_type == "document":
                bot.send_document(t, msg.document.file_id)

            elif msg.content_type == "audio":
                bot.send_audio(t, msg.audio.file_id)

            elif msg.content_type == "voice":
                bot.send_voice(t, msg.voice.file_id)

            elif msg.content_type == "sticker":
                bot.send_sticker(t, msg.sticker.file_id)

            elif msg.content_type == "location":
                bot.send_location(t, msg.location.latitude, msg.location.longitude)

            elif msg.content_type == "contact":
                bot.send_contact(t, msg.contact.phone_number, msg.contact.first_name)

            elif msg.content_type == "poll":
                # опросы нельзя пересылать напрямую, создадим новый
                q = msg.poll.question
                opts = [o.text for o in msg.poll.options]
                bot.send_poll(t, q, opts)

        except Exception as e:
            log_error(f"forward_media_anon({src_chat_id}->{t}): {e}")


def collect_media_group(msg):
    """
    Возвращает список objects InputMedia* для групповой пересылки.
    """
    try:
        media_group = []

        for m in msg.media_group_id.messages:
            if m.content_type == "photo":
                media_group.append(InputMediaPhoto(m.photo[-1].file_id))
            elif m.content_type == "video":
                media_group.append(InputMediaVideo(m.video.file_id))
            elif m.content_type == "document":
                media_group.append(InputMediaDocument(m.document.file_id))
            elif m.content_type == "audio":
                media_group.append(InputMediaAudio(m.audio.file_id))

        return media_group
    except Exception:
        return None


def forward_media_group_anon(src_chat_id: int, msg, targets: list[int]):
    group = collect_media_group(msg)
    if not group:
        return
    for t in targets:
        try:
            bot.send_media_group(t, group)
        except Exception as e:
            log_error(f"forward_media_group_anon({src_chat_id}->{t}): {e}")


# ==========================================================
# SECTION 9 — Inline keyboards
# ==========================================================

def build_main_keyboard(chat_id: int, day_key: str):
    kb = types.InlineKeyboardMarkup(row_width=4)

    kb.add(
        types.InlineKeyboardButton("⬅️", callback_data="prev"),
        types.InlineKeyboardButton("📅", callback_data="calendar"),
        types.InlineKeyboardButton("➡️", callback_data="next"),
    )

    kb.add(
        types.InlineKeyboardButton("➕ Добавить", callback_data="add"),
        types.InlineKeyboardButton("✏️ Редактировать", callback_data="edit_menu"),
    )

    kb.add(
        types.InlineKeyboardButton("💰 Общий итог", callback_data="balance"),
        types.InlineKeyboardButton("📄 CSV", callback_data=f"csv:{day_key}"),
    )

    if OWNER_ID and chat_id == OWNER_ID:
        kb.add(types.InlineKeyboardButton("🔁 Пересылка", callback_data="fw_menu"))

    return kb
    
def build_calendar_keyboard(chat_id: int):
    kb = types.InlineKeyboardMarkup(row_width=7)

    today = now_local().date()
    days = [(today - timedelta(days=i)) for i in range(31)]
    days = sorted(days)

    row = []
    for d in days:
        label = d.strftime("%d")
        day_key = d.strftime("%Y-%m-%d")
        row.append(types.InlineKeyboardButton(label, callback_data=f"d:{day_key}"))
        if len(row) == 7:
            kb.add(*row)
            row = []

    if row:
        kb.add(*row)

    kb.add(types.InlineKeyboardButton("🔙 Назад", callback_data="back_to_main"))
    return kb


def build_edit_menu_keyboard(chat_id: int, day_key: str):
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        types.InlineKeyboardButton("➕ Добавить", callback_data=f"add:{day_key}"),
        types.InlineKeyboardButton("🗑 Удалить", callback_data=f"del:{day_key}"),
    )
    kb.add(
        types.InlineKeyboardButton("⬅️ Назад", callback_data=f"back:{day_key}"),
    )
    return kb


def build_edit_list_keyboard(chat_id: int, day_key: str):
    store = get_chat_store(chat_id)
    daily = store.get("daily_records", {})
    recs = daily.get(day_key, [])

    kb = types.InlineKeyboardMarkup(row_width=1)
    for r in recs:
        sid = r.get("short_id")
        note = r.get("note", "")
        label = f"{sid}: {note}" if note else sid
        kb.add(types.InlineKeyboardButton(label, callback_data=f"edit:{day_key}:{r.get('id')}"))

    kb.add(types.InlineKeyboardButton("🔙 Назад", callback_data=f"back:{day_key}"))
    return kb


def build_forward_menu(chat_id: int):
    kb = types.inline_keyboard_markup.InlineKeyboardMarkup(row_width=1)
    store = get_chat_store(OWNER_ID)
    kc = store.get("known_chats", {})

    for cid, info in kc.items():
        title = info.get("title") or f"Чат {cid}"
        kb.add(types.InlineKeyboardButton(title, callback_data=f"fw:{cid}"))

    kb.add(types.InlineKeyboardButton("🔙 Назад", callback_data="fw_back"))
    return kb


def build_forward_direction_menu(chat_id: int, target_id: int):
    kb = types.inline_keyboard_markup.InlineKeyboardMarkup(row_width=1)
    kb.add(types.InlineKeyboardButton("➡️ В одну сторону", callback_data=f"fw1:{target_id}"))
    kb.add(types.InlineKeyboardButton("↔️ В обе стороны", callback_data=f"fw2:{target_id}"))
    kb.add(types.InlineKeyboardButton("🚫 Удалить связь", callback_data=f"fw0:{target_id}"))
    kb.add(types.InlineKeyboardButton("🔙 Назад", callback_data="fw_back"))
    return kb


# ==========================================================
# SECTION 10 — Day window renderer
# ==========================================================

def render_day_window(chat_id: int, day_key: str):
    store = get_chat_store(chat_id)
    daily = store.get("daily_records", {})
    recs = daily.get(day_key, [])

    lines = [f"📅 <b>{day_key}</b>"]
    lines.append("")

    income = sum(r.get("amount", 0) for r in recs if r.get("amount", 0) > 0)
    expense = sum(r.get("amount", 0) for r in recs if r.get("amount", 0) < 0)

    lines.append(f"➕ Приход: <b>{fmt_num(income)}</b>")
    lines.append(f"➖ Расход: <b>{fmt_num(expense)}</b>")

    balance = store.get("balance", 0)
    lines.append(f"💰 Общий остаток чата: <b>{fmt_num(balance)}</b>")
    lines.append("")

    for r in sorted(recs, key=lambda x: x.get("timestamp", "")):
        a = r.get("amount", 0)
        s = fmt_num(a)
        note = r.get("note", "")
        sid = r.get("short_id", "")
        ts = r.get("timestamp", "")
        lines.append(f"<b>{sid}</b> {s} — {note} <i>({ts})</i>")

    return "\n".join(lines)


def update_or_send_day_window(chat_id: int, day_key: str):
    fname = active_windows_file()
    windows = {}

    if os.path.exists(fname):
        try:
            with open(fname, "r", encoding="utf-8") as f:
                windows = json.load(f)
        except Exception:
            windows = {}

    msg_id = windows.get(str(chat_id))

    text = render_day_window(chat_id, day_key)
    kb = build_main_keyboard(chat_id, day_key)

    try:
        if msg_id:
            bot.edit_message_text(
                chat_id=chat_id,
                message_id=msg_id,
                text=text,
                reply_markup=kb,
            )
        else:
            m = bot.send_message(chat_id, text, reply_markup=kb)
            windows[str(chat_id)] = m.message_id
    except Exception:
        try:
            m = bot.send_message(chat_id, text, reply_markup=kb)
            windows[str(chat_id)] = m.message_id
        except Exception:
            pass

    try:
        with open(fname, "w", encoding="utf-8") as f:
            json.dump(windows, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log_error(f"update_or_send_day_window save error: {e}")
        
# ==========================================================
# SECTION 11 — Calendar caption (Сегодня / Вчера / Завтра)
# ==========================================================

def render_calendar_caption(day_key: str) -> str:
    """
    Возвращает надпись:
    • Сегодня
    • Вчера
    • Завтра
    • или саму дату
    """
    d = datetime.strptime(day_key, "%Y-%m-%d").date()
    today = now_local().date()

    if d == today:
        return "Сегодня"
    elif d == today - timedelta(days=1):
        return "Вчера"
    elif d == today + timedelta(days=1):
        return "Завтра"
    return day_key


# ==========================================================
# SECTION 12 — Callback handler
# ==========================================================

@bot.callback_query_handler(func=lambda c: True)
def on_callback(call):
    try:
        chat_id = call.message.chat.id
        data_str = call.data

        store = get_chat_store(chat_id)

        # --------------------------------------------------
        # BACK TO MAIN
        # --------------------------------------------------
        if data_str == "back_to_main":
            dk = store.get("current_view_day", today_key())
            update_or_send_day_window(chat_id, dk)
            bot.answer_callback_query(call.id)
            return

        # --------------------------------------------------
        # PREV / NEXT / TODAY
        # --------------------------------------------------

        if data_str == "prev":
            dk = store.get("current_view_day", today_key())
            d = datetime.strptime(dk, "%Y-%m-%d") - timedelta(days=1)
            nd = d.strftime("%Y-%m-%d")
            store["current_view_day"] = nd
            save_data(data)
            update_or_send_day_window(chat_id, nd)
            bot.answer_callback_query(call.id)
            return

        if data_str == "next":
            dk = store.get("current_view_day", today_key())
            d = datetime.strptime(dk, "%Y-%m-%d") + timedelta(days=1)
            nd = d.strftime("%Y-%m-%d")
            store["current_view_day"] = nd
            save_data(data)
            update_or_send_day_window(chat_id, nd)
            bot.answer_callback_query(call.id)
            return

        if data_str == "today":
            dk = today_key()
            store["current_view_day"] = dk
            save_data(data)
            update_or_send_day_window(chat_id, dk)
            bot.answer_callback_query(call.id)
            return

        # --------------------------------------------------
        # CALENDAR
        # --------------------------------------------------
        if data_str == "calendar":
            kb = build_calendar_keyboard(chat_id)
            bot.edit_message_text(
                chat_id=chat_id,
                message_id=call.message.message_id,
                text="📅 Выберите дату:",
                reply_markup=kb,
            )
            bot.answer_callback_query(call.id)
            return

        # --------------------------------------------------
        # DAY SELECTED
        # --------------------------------------------------
        if data_str.startswith("d:"):
            day_key = data_str[2:]
            store["current_view_day"] = day_key
            save_data(data)
            update_or_send_day_window(chat_id, day_key)
            bot.answer_callback_query(call.id)
            return

        # --------------------------------------------------
        # EDIT MENU
        # --------------------------------------------------
        if data_str == "edit_menu":
            dk = store.get("current_view_day", today_key())
            kb = build_edit_menu_keyboard(chat_id, dk)
            bot.edit_message_text(
                chat_id=chat_id,
                message_id=call.message.message_id,
                text=f"✏️ Редактирование дня {dk}",
                reply_markup=kb,
            )
            bot.answer_callback_query(call.id)
            return

        # --------------------------------------------------
        # EDIT LIST
        # --------------------------------------------------
        if data_str.startswith("edit_menu_list:"):
            dk = data_str.split(":", 1)[1]
            kb = build_edit_list_keyboard(chat_id, dk)
            bot.edit_message_text(
                chat_id=chat_id,
                message_id=call.message.message_id,
                text=f"✏️ Выберите запись ({dk})",
                reply_markup=kb,
            )
            bot.answer_callback_query(call.id)
            return

        # --------------------------------------------------
        # ADD
        # --------------------------------------------------
        if data_str.startswith("add"):
            parts = data_str.split(":")
            if len(parts) == 2:
                day_key = parts[1]
            else:
                day_key = store.get("current_view_day", today_key())

            store["edit_wait"] = {"type": "add", "day_key": day_key}
            save_data(data)

            m = bot.send_message(chat_id, "Введите сумму и комментарий. Пример: 150 еда")
            schedule_delete_message(chat_id, m.message_id, 15)
            schedule_cancel_edit(chat_id, 15)

            bot.answer_callback_query(call.id)
            return

        # --------------------------------------------------
        # DELETE MENU
        # --------------------------------------------------
        if data_str.startswith("del:"):
            dk = data_str.split(":", 1)[1]
            kb = build_edit_list_keyboard(chat_id, dk)
            bot.edit_message_text(
                chat_id=chat_id,
                message_id=call.message.message_id,
                text=f"🗑 Удалить запись ({dk})",
                reply_markup=kb,
            )
            bot.answer_callback_query(call.id)
            return

        # --------------------------------------------------
        # DELETE RECORD
        # --------------------------------------------------
        if data_str.startswith("edit:"):
            parts = data_str.split(":")
            if len(parts) == 3:
                day_key = parts[1]
                record_id = int(parts[2])
            else:
                bot.answer_callback_query(call.id)
                return

            store["edit_wait"] = {
                "type": "edit_record",
                "day_key": day_key,
                "record_id": record_id,
            }
            save_data(data)

            bot.edit_message_text(
                chat_id=chat_id,
                message_id=call.message.message_id,
                text=f"✏️ Введите новое значение для записи {record_id}",
            )
            bot.answer_callback_query(call.id)
            return

        # --------------------------------------------------
        # BALANCE BUTTON
        # --------------------------------------------------
        if data_str == "balance":
            store = get_chat_store(chat_id)
            bal = store.get("balance", 0)
            bot.answer_callback_query(call.id, text=f"Баланс: {fmt_num(bal)}", show_alert=True)
            return
            
        # --------------------------------------------------
        # CSV EXPORT
        # --------------------------------------------------
        if data_str.startswith("csv:"):
            day_key = data_str.split(":", 1)[1]
            save_day_csv(chat_id, day_key)

            path = f"data_{chat_id}_{day_key}.csv"
            try:
                with open(path, "rb") as f:
                    bot.send_document(
                        chat_id,
                        document=f,
                        visible_file_name=os.path.basename(path),
                        caption=f"CSV за {day_key}",
                    )
            except Exception as e:
                log_error(f"send CSV error: {e}")

            bot.answer_callback_query(call.id)
            return

        # --------------------------------------------------
        # BACK AFTER EDIT
        # --------------------------------------------------
        if data_str.startswith("back:"):
            dk = data_str.split(":", 1)[1]
            update_or_send_day_window(chat_id, dk)
            bot.answer_callback_query(call.id)
            return

        # --------------------------------------------------
        # FORWARDING MENU (OWNER ONLY)
        # --------------------------------------------------
        if data_str == "fw_menu":
            if OWNER_ID and chat_id == OWNER_ID:
                kb = build_forward_menu(chat_id)
                bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=call.message.message_id,
                    text="🔁 Настройка пересылки\nВыберите чат:",
                    reply_markup=kb,
                )
            else:
                bot.answer_callback_query(call.id, text="Недоступно.", show_alert=True)
            return

        # --------------------------------------------------
        # BACK IN FORWARDING MENU
        # --------------------------------------------------
        if data_str == "fw_back":
            dk = store.get("current_view_day", today_key())
            update_or_send_day_window(chat_id, dk)
            bot.answer_callback_query(call.id)
            return

        # --------------------------------------------------
        # SELECT CHAT IN FORWARD MENU
        # --------------------------------------------------
        if data_str.startswith("fw:"):
            target_id = data_str.split(":", 1)[1]
            kb = build_forward_direction_menu(chat_id, target_id)
            bot.edit_message_text(
                chat_id=chat_id,
                message_id=call.message.message_id,
                text=f"Чат: {target_id}\nВыберите направление пересылки:",
                reply_markup=kb,
            )
            bot.answer_callback_query(call.id)
            return

        # --------------------------------------------------
        # FORWARD RULES SETUP
        # --------------------------------------------------
        if data_str.startswith("fw1:"):
            tgt = data_str.split(":", 1)[1]
            try:
                tgt = int(tgt)
                owner_store = get_chat_store(OWNER_ID)
                fr = owner_store.setdefault("forward_rules", {})
                fr.setdefault(str(chat_id), [])
                if tgt not in fr[str(chat_id)]:
                    fr[str(chat_id)].append(tgt)
                save_data(data)
                bot.answer_callback_query(call.id, text="Сохранено!", show_alert=True)
            except Exception as e:
                log_error(f"fw1 error: {e}")
            return

        if data_str.startswith("fw2:"):
            tgt = data_str.split(":", 1)[1]
            try:
                tgt = int(tgt)
                owner_store = get_chat_store(OWNER_ID)
                fr = owner_store.setdefault("forward_rules", {})
                fr.setdefault(str(chat_id), [])
                fr.setdefault(str(tgt), [])
                if tgt not in fr[str(chat_id)]:
                    fr[str(chat_id)].append(tgt)
                if chat_id not in fr[str(tgt)]:
                    fr[str(tgt)].append(chat_id)
                save_data(data)
                bot.answer_callback_query(call.id, text="Сохранено!", show_alert=True)
            except Exception as e:
                log_error(f"fw2 error: {e}")
            return

        if data_str.startswith("fw0:"):
            tgt = data_str.split(":", 1)[1]
            try:
                tgt = int(tgt)
                owner_store = get_chat_store(OWNER_ID)
                fr = owner_store.setdefault("forward_rules", {})
                if str(chat_id) in fr:
                    fr[str(chat_id)] = [x for x in fr[str(chat_id)] if x != tgt]
                if str(tgt) in fr:
                    fr[str(tgt)] = [x for x in fr[str(tgt)] if x != chat_id]
                save_data(data)
                bot.answer_callback_query(call.id, text="Удалено.", show_alert=True)
            except Exception as e:
                log_error(f"fw0 error: {e}")
            return

        # Unknown callback
        bot.answer_callback_query(call.id)

    except Exception as e:
        log_error(f"on_callback error: {e}")
        
    
# ==========================================================
# SECTION 13 — Add / Edit / Delete handlers (text input)
# ==========================================================

def schedule_delete_message(chat_id: int, msg_id: int, delay: int):
    def _del():
        time.sleep(delay)
        try:
            bot.delete_message(chat_id, msg_id)
        except Exception:
            pass

    threading.Thread(target=_del, daemon=True).start()


def schedule_cancel_edit(chat_id: int, delay: int):
    def _cancel():
        time.sleep(delay)
        store = get_chat_store(chat_id)
        if store.get("edit_wait"):
            store["edit_wait"] = None
            save_data(data)
            try:
                bot.send_message(chat_id, "⌛ Действие отменено.")
            except Exception:
                pass

    threading.Thread(target=_cancel, daemon=True).start()


def schedule_finalize(chat_id: int, day_key: str, delay: int):
    def _run():
        time.sleep(delay)
        update_or_send_day_window(chat_id, day_key)
    threading.Thread(target=_run, daemon=True).start()


# ==========================================================
# SECTION 14 — Text handler
# ==========================================================

@bot.message_handler(content_types=["text"])
def handle_text(msg):
    """
    Основной текстовый обработчик:
      • обновление known_chats
      • пересылка
      • add/edit/reset_confirm
      • авто-добавление
    """
    try:
        chat_id = msg.chat.id
        text = (msg.text or "").strip()

        # ----------------------------------------------
        # Update known chats
        # ----------------------------------------------
        try:
            if OWNER_ID and chat_id != OWNER_ID:
                owner_store = get_chat_store(OWNER_ID)
                kc = owner_store.setdefault("known_chats", {})
                info = kc.setdefault(str(chat_id), {})
                info["title"] = msg.chat.title or info.get("title") or f"Чат {chat_id}"
                info["username"] = msg.chat.username or info.get("username")
                info["type"] = msg.chat.type
                save_data(data)
        except Exception:
            pass

        # ----------------------------------------------
        # Forwarding
        # ----------------------------------------------
        targets = resolve_forward_targets(chat_id)
        if targets:
            if msg.content_type == "text":
                forward_text_anon(chat_id, msg, targets)

        # ----------------------------------------------
        # Get store + edit_wait
        # ----------------------------------------------
        store = get_chat_store(chat_id)
        wait = store.get("edit_wait")
        auto_add_enabled = store.get("settings", {}).get("auto_add", False)

        # ======================================================
        # RESET CONFIRM (ДА)
        # ======================================================
        if wait and wait.get("type") == "reset_confirm":
            if text.upper() == "ДА":
                reset_chat_data(chat_id)
                bot.send_message(chat_id, "🧹 Данные обнулены.")

                day_key = store.get("current_view_day", today_key())
                update_or_send_day_window(chat_id, day_key)
            else:
                bot.send_message(chat_id, "Отмена обнуления.")

            store["edit_wait"] = None
            save_data(data)
            return

        # ======================================================
        # EDIT RECORD
        # ======================================================
        if wait and wait.get("type") == "edit_record":
            day_key = wait["day_key"]
            record_id = wait["record_id"]

            try:
                amount, note = split_amount_and_note(text)
            except Exception:
                bot.send_message(chat_id, "Ошибка формата. Пример: 100 еда")
                return

            update_record_in_chat(chat_id, record_id, amount, note)
            update_or_send_day_window(chat_id, day_key)

            store["edit_wait"] = None
            save_data(data)
            return

        # ======================================================
        # ADD RECORD
        # ======================================================
        if wait and wait.get("type") == "add":
            day_key = wait["day_key"]

            try:
                amount, note = split_amount_and_note(text)
            except Exception:
                bot.send_message(chat_id, "Ошибка формата. Пример: +750 кафе")
                return

            add_record_to_chat(chat_id, amount, note, day_key)
            schedule_finalize(chat_id, day_key, 3)

            store["edit_wait"] = None
            save_data(data)
            return

        # ======================================================
        # AUTO-ADD MODE
        # ======================================================
        if auto_add_enabled and looks_like_amount(text):
            day_key = store.get("current_view_day", today_key())
            try:
                amount, note = split_amount_and_note(text)
                add_record_to_chat(chat_id, amount, note, day_key)
                schedule_finalize(chat_id, day_key, 3)
                return
            except Exception:
                pass

        # ======================================================
        # COMMANDS
        # ======================================================

        if text.startswith("/start"):
            dk = today_key()
            store["current_view_day"] = dk
            save_data(data)
            update_or_send_day_window(chat_id, dk)
            return

        if text.startswith("/поехали") or text.startswith("/ok"):
            set_finance_mode(chat_id, True)
            bot.send_message(chat_id, "Финансовый режим включён.")
            dk = store.get("current_view_day", today_key())
            update_or_send_day_window(chat_id, dk)
            return

        if text.startswith("/view"):
            parts = text.split()
            if len(parts) == 2:
                try:
                    datetime.strptime(parts[1], "%Y-%m-%d")
                    store["current_view_day"] = parts[1]
                    save_data(data)
                    update_or_send_day_window(chat_id, parts[1])
                except Exception:
                    bot.send_message(chat_id, "Неверный формат даты")
            return

        if text.startswith("/prev"):
            dk = store.get("current_view_day", today_key())
            nd = (datetime.strptime(dk, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
            store["current_view_day"] = nd
            save_data(data)
            update_or_send_day_window(chat_id, nd)
            return

        if text.startswith("/next"):
            dk = store.get("current_view_day", today_key())
            nd = (datetime.strptime(dk, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
            store["current_view_day"] = nd
            save_data(data)
            update_or_send_day_window(chat_id, nd)
            return

        if text.startswith("/balance"):
            bal = store.get("balance", 0)
            bot.send_message(chat_id, f"Баланс: {fmt_num(bal)}")
            return
            
        if text.startswith("/report"):
            lines = ["📊 Отчёт по дням:"]
            daily = store.get("daily_records", {})
            for dk in sorted(daily.keys()):
                s = sum(r["amount"] for r in daily.get(dk, []))
                lines.append(f"{dk}: {fmt_num(s)}")
            bot.send_message(chat_id, "\n".join(lines))
            return

        if text.startswith("/csv"):
            try:
                cmd_csv(chat_id)
            except Exception as e:
                log_error(f"/csv error: {e}")
            return

        if text.startswith("/csv_all"):
            try:
                cmd_csv_all(chat_id)
            except Exception as e:
                log_error(f"/csv_all error: {e}")
            return

        if text.startswith("/json"):
            try:
                cmd_json(chat_id)
            except Exception as e:
                log_error(f"/json error: {e}")
            return

        if text.startswith("/reset"):
            store["edit_wait"] = {"type": "reset_confirm"}
            save_data(data)
            m = bot.send_message(chat_id, "⚠️ Вы уверены? Напишите ДА")
            schedule_delete_message(chat_id, m.message_id, 15)
            schedule_cancel_edit(chat_id, 15)
            return

        if text.startswith("/autoadd_info"):
            aa = store.get("settings", {}).get("auto_add", False)
            bot.send_message(chat_id, f"Авто-добавление: {'включено' if aa else 'выключено'}")
            return

        if text.startswith("/ping"):
            bot.send_message(chat_id, "pong")
            return

        if text.startswith("/stopforward"):
            if OWNER_ID and chat_id == OWNER_ID:
                try:
                    owner_store = get_chat_store(OWNER_ID)
                    owner_store["forward_rules"] = {}
                    save_data(data)
                    bot.send_message(chat_id, "Все маршруты пересылки удалены.")
                except Exception as e:
                    log_error(f"/stopforward error: {e}")
            else:
                bot.send_message(chat_id, "Недоступно.")
            return

        # ------------------------------------------------------
        # RESTORE MODE
        # ------------------------------------------------------
        if text.startswith("/restore"):
            global restore_mode
            restore_mode = True
            bot.send_message(chat_id, "🔧 Режим восстановления включён.")
            return

        if text.startswith("/restore_off"):
            global restore_mode
            restore_mode = False
            bot.send_message(chat_id, "Режим восстановления выключен.")
            return

    except Exception as e:
        log_error(f"handle_text error: {e}")
        
# ==========================================================
# SECTION 15 — Handlers for media / documents
# ==========================================================

@bot.message_handler(content_types=[
    "photo", "video", "audio", "voice",
    "document", "sticker", "location",
    "contact", "poll"
])
def handle_media(msg):
    """Обработчик всех медиа, с пересылкой (анонимной)."""
    try:
        chat_id = msg.chat.id

        # обновить known_chats
        try:
            if OWNER_ID and chat_id != OWNER_ID:
                owner_store = get_chat_store(OWNER_ID)
                kc = owner_store.setdefault("known_chats", {})
                info = kc.setdefault(str(chat_id), {})
                info["title"] = msg.chat.title or info.get("title") or f"Чат {chat_id}"
                info["username"] = msg.chat.username or info.get("username")
                info["type"] = msg.chat.type
                save_data(data)
        except Exception:
            pass

        # пересылка
        targets = resolve_forward_targets(chat_id)
        if targets:
            if msg.media_group_id:
                forward_media_group_anon(chat_id, msg, targets)
            else:
                forward_media_anon(chat_id, msg, targets)

        # режим восстановления
        global restore_mode
        if restore_mode and msg.content_type == "document":
            fname = msg.document.file_name.lower()

            if (
                fname == "data.json"
                or fname == "csv_meta.json"
                or (fname.startswith("data_") and fname.endswith(".json"))
                or (fname.startswith("data_") and fname.endswith(".csv"))
            ):
                store = get_chat_store(chat_id)
                store["edit_wait"] = {
                    "type": "restore_confirm",
                    "file_id": msg.document.file_id,
                    "file_name": fname,
                }
                save_data(data)
                bot.send_message(chat_id, "🔧 Напишите: ДА — для восстановления из файла.")
                return

    except Exception as e:
        log_error(f"handle_media error: {e}")


# ==========================================================
# SECTION 16 — Restore from uploaded files
# ==========================================================

def restore_file_switch(chat_id: int, file_id: str, file_name: str):
    """Восстановление JSON/CSV в зависимости от имени файла."""
    try:
        file_info = bot.get_file(file_id)
        downloaded = bot.download_file(file_info.file_path)

        if file_name == "data.json" or file_name.startswith("data_") and file_name.endswith(".json"):
            with open("data.json", "wb") as f:
                f.write(downloaded)
            load_data()
            bot.send_message(chat_id, "✔️ Восстановлено: data.json")

        elif file_name == "csv_meta.json":
            with open("csv_meta.json", "wb") as f:
                f.write(downloaded)
            bot.send_message(chat_id, "✔️ Восстановлено: csv_meta.json")

        elif file_name.startswith("data_") and file_name.endswith(".csv"):
            with open(file_name, "wb") as f:
                f.write(downloaded)
            bot.send_message(chat_id, f"✔️ Восстановлено: {file_name}")

    except Exception as e:
        log_error(f"restore_file_switch error: {e}")


@bot.message_handler(content_types=["text"])
def handle_restore_confirm(msg):
    """Подтверждение восстановления (ДА)."""
    try:
        chat_id = msg.chat.id
        text = msg.text.strip()

        store = get_chat_store(chat_id)
        wait = store.get("edit_wait")

        if wait and wait.get("type") == "restore_confirm":
            if text.upper() == "ДА":
                file_id = wait.get("file_id")
                fname = wait.get("file_name")
                restore_file_switch(chat_id, file_id, fname)
            else:
                bot.send_message(chat_id, "❌ Отменено.")

            store["edit_wait"] = None
            save_data(data)
    except Exception as e:
        log_error(f"handle_restore_confirm error: {e}")
        
        
# ==========================================================
# SECTION 17 — CSV / JSON commands
# ==========================================================

def cmd_csv(chat_id: int):
    save_chat_csv(chat_id)

    path = chat_csv_file(chat_id)
    try:
        with open(path, "rb") as f:
            bot.send_document(
                chat_id,
                document=f,
                visible_file_name=os.path.basename(path),
                caption="CSV за все дни",
            )
    except Exception as e:
        log_error(f"cmd_csv error: {e}")


def cmd_csv_all(chat_id: int):
    """Общий CSV этого чата (daily_records)."""
    store = get_chat_store(chat_id)
    daily = store.get("daily_records", {})

    path = f"data_{chat_id}_all.csv"
    try:
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["ID", "short_id", "timestamp", "amount", "note", "owner", "day_key"])
            for dk in sorted(daily.keys()):
                for r in sorted(daily[dk], key=lambda x: x.get("timestamp", "")):
                    w.writerow([
                        r.get("id"),
                        r.get("short_id"),
                        r.get("timestamp"),
                        r.get("amount"),
                        r.get("note"),
                        r.get("owner"),
                        dk,
                    ])
    except Exception as e:
        log_error(f"cmd_csv_all write error: {e}")

    try:
        with open(path, "rb") as f:
            bot.send_document(
                chat_id,
                document=f,
                visible_file_name=os.path.basename(path),
                caption="Общий CSV этого чата",
            )
    except Exception as e:
        log_error(f"cmd_csv_all send error: {e}")


def cmd_json(chat_id: int):
    save_chat_json(chat_id)
    path = chat_json_file(chat_id)

    try:
        with open(path, "rb") as f:
            bot.send_document(
                chat_id,
                document=f,
                visible_file_name=os.path.basename(path),
                caption="JSON данных этого чата",
            )
    except Exception as e:
        log_error(f"cmd_json error: {e}")


# ==========================================================
# SECTION 18 — Keep-alive / Ping
# ==========================================================

def keep_alive():
    """Периодический self-ping + сообщение владельцу."""
    while True:
        try:
            if OWNER_ID:
                bot.send_message(OWNER_ID, "🤖 Bot alive.")
        except Exception:
            pass

        time.sleep(60)


t = threading.Thread(target=keep_alive, daemon=True)
t.start()


# ==========================================================
# SECTION 19 — Webhook
# ==========================================================

@app.route("/", methods=["GET"])
def index():
    return "Bot running."


@app.route(f"/{BOT_TOKEN}", methods=["POST"])
def webhook():
    try:
        data_json = request.get_data().decode("utf-8")
        update = telebot.types.Update.de_json(data_json)
        bot.process_new_updates([update])
    except Exception as e:
        log_error(f"webhook error: {e}")
    return "OK", 200
    
# ==========================================================
# SECTION 20 — Main (startup, webhook set)
# ==========================================================

def main():
    load_data()

    # Устанавливаем webhook, если требуется
    if WEBHOOK_URL:
        try:
            bot.remove_webhook()
            time.sleep(1)
            bot.set_webhook(url=WEBHOOK_URL)
            log_info(f"Webhook установлен: {WEBHOOK_URL}")
        except Exception as e:
            log_error(f"set_webhook error: {e}")
    else:
        log_info("Запуск без webhook: polling mode")
        bot.infinity_polling(skip_pending=True)


if __name__ == "__main__":
    main()
    
    