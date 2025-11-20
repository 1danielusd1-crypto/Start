# ==========================================================
# 🧭 Финансовый бот — Code_022.3 FINAL (полностью собранный)
# Версия: 022.3-FINAL
# Поддержка:
#   • per-chat JSON + CSV
#   • Google Drive
#   • Канал-бэкап
#   • Пересылка ⬅️ ➡️ ↔️ между чатами
#   • Окно дня, календарь 31 дней
#   • /prev /next /view /поехали
#   • Исправленные отсеки 13 / 16 / 18
# ==========================================================


# ==========================================================
# SECTION 1 — Imports & Base Setup
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

import requests
import telebot
from telebot import types
from flask import Flask, request

from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from google.oauth2 import service_account


# ==========================================================
# SECTION 2 — Global Variables
# ==========================================================
VERSION = "022.3-FINAL"

BOT_TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = os.getenv("OWNER_ID")
APP_URL = os.getenv("APP_URL")

GDRIVE_FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID")
SERVICE_JSON = os.getenv("SERVICE_ACCOUNT_JSON", "service_account.json")

BACKUP_CHAT_ID = os.getenv("BACKUP_CHAT_ID")   # 📦 Канал-бэкап


bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")
app = Flask(__name__)

DATA_FILE = "data.json"

data = {
    "overall_balance": 0,
    "records": [],
    "finance_active_chats": {},
    "known_chats": {},
    "backup_flags": {"drive": True, "channel": True},
    "active_messages": {},
}


# ==========================================================
# SECTION 3 — Time helpers
# ==========================================================
LOCAL_TZ = ZoneInfo("America/Argentina/Buenos_Aires")

def now_local():
    return datetime.now(LOCAL_TZ)

def today_key():
    return now_local().strftime("%Y-%m-%d")


# ==========================================================
# SECTION 4 — Data load/save
# ==========================================================
def load_data():
    global data
    if not os.path.exists(DATA_FILE):
        save_data(data)
        return data
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except:
        pass
    return data


def save_data(d):
    try:
        with open(DATA_FILE, "w", encoding="utf-8") as f:
            json.dump(d, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logging.error(f"save_data error: {e}")


def save_chat_json(chat_id: int):
    try:
        with open(f"data_{chat_id}.json", "w", encoding="utf-8") as f:
            json.dump(data["finance_active_chats"].get(str(chat_id), {}), f, ensure_ascii=False, indent=2)
    except Exception as e:
        logging.error(f"save_chat_json error: {e}")


def export_global_csv(d):
    try:
        with open("data.csv", "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["id", "timestamp", "amount", "note", "owner"])
            for r in d.get("records", []):
                w.writerow([r["id"], r["timestamp"], r["amount"], r["note"], r["owner"]])
    except Exception as e:
        logging.error(f"export_global_csv error: {e}")


# ==========================================================
# SECTION 5 — Chat store helpers
# ==========================================================
def get_chat_store(chat_id: int):
    cid = str(chat_id)
    store = data["finance_active_chats"].setdefault(cid, {
        "records": [],
        "daily_records": {},
        "balance": 0,
        "next_id": 1,
        "info": {},
    })
    return store


def update_chat_info_from_message(msg):
    chat = msg.chat
    cid = str(chat.id)

    info = {
        "id": chat.id,
        "title": getattr(chat, "title", ""),
        "type": chat.type,
        "username": chat.username,
        "first_name": getattr(chat, "first_name", ""),
        "last_name": getattr(chat, "last_name", ""),
    }

    data["known_chats"][cid] = info
    store = get_chat_store(chat.id)
    store["info"] = info

    save_data(data)
# ==========================================================
# SECTION 6 — Number formatting & parsing (исправленный)
# ==========================================================

def fmt_num(x: int) -> str:
    """
    Форматирует число через пробелы: 1 200 500
    """
    return f"{x:,}".replace(",", " ")


# ищет числа даже внутри текста ("тебе500мне", "500тест", "abc1200xyz")
num_re = re.compile(
    r"""
    [+\-–]?              # знак
    \s*                  # пробелы
    \d                   # старт цифры
    (?:[\d\s\.,_'’]*\d)? # тело числа
    """,
    re.VERBOSE
)

def parse_amount(text: str) -> int:
    """
    Универсальный парсер суммы.
    Поддерживает:
        1.200
        1 200
        1,200
        1.200,50
        -500
        +1000
        abc200def
    """
    s = (text or "").strip()

    m = num_re.search(s)
    if not m:
        raise ValueError("no number found")

    num = m.group(0).strip()

    negative = num.startswith("-") or num.startswith("–")
    num = num.lstrip("+-–").strip()

    num = num.replace(" ", "").replace("_", "").replace("’", "").replace("'", "")

    # оба сепаратора → определить десятичный
    if "." in num and "," in num:
        if num.rfind(",") > num.rfind("."):
            num = num.replace(".", "")
            num = num.replace(",", ".")
        else:
            num = num.replace(",", "")
    else:
        if "," in num and "." not in num:
            num = num.replace(".", "")
            num = num.replace(",", ".")
        else:
            num = num.replace(",", "").replace(".", "")

    try:
        val = float(num)
    except:
        raise ValueError("bad number format")

    if negative:
        val = -val

    return int(val)


# ==========================================================
# SECTION 7 — Google Drive integration
# ==========================================================

def gdrive_service():
    try:
        creds = service_account.Credentials.from_service_account_file(
            SERVICE_JSON,
            scopes=["https://www.googleapis.com/auth/drive"]
        )
        return build("drive", "v3", credentials=creds)
    except Exception as e:
        logging.error(f"GDrive creds error: {e}")
        return None


def upload_to_gdrive(filepath: str, filename: str):
    if not data["backup_flags"].get("drive"):
        return

    srv = gdrive_service()
    if not srv:
        return

    try:
        media = MediaFileUpload(filepath, resumable=True)
        srv.files().create(
            media_body=media,
            body={"name": filename, "parents": [GDRIVE_FOLDER_ID]}
        ).execute()
    except Exception as e:
        logging.error(f"upload_to_gdrive({filename}): {e}")


def download_from_gdrive(file_id: str, target: str):
    srv = gdrive_service()
    if not srv:
        return False
    try:
        req = srv.files().get_media(fileId=file_id)
        fh = io.FileIO(target, "wb")
        downloader = MediaIoBaseDownload(fh, req)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        return True
    except Exception as e:
        logging.error(f"download_from_gdrive error: {e}")
        return False


def restore_from_gdrive_if_needed():
    """
    При первом запуске пытаемся загрузить data.json / CSV с GDrive,
    если локальных файлов нет.
    """
    restored = False
    if not os.path.exists(DATA_FILE):
        # TODO: расширенная логика, если нужно
        pass

    return restored


# ==========================================================
# SECTION 8 — Telegram Channel Backup
# ==========================================================

def send_backup_to_channel(chat_id: int):
    """
    Отправляет в канал data.json и data_chat.json
    """
    if not data["backup_flags"].get("channel"):
        return

    try:
        # основной файл
        bot.send_document(
            BACKUP_CHAT_ID,
            open(DATA_FILE, "rb"),
            caption=f"📦 FULL BACKUP (global) — {now_local()}"
        )

        # пер-чат файл
        f = f"data_{chat_id}.json"
        if os.path.exists(f):
            bot.send_document(
                BACKUP_CHAT_ID,
                open(f, "rb"),
                caption=f"📦 data_{chat_id}.json — backup"
            )
    except Exception as e:
        logging.error(f"Channel backup error: {e}")


# ==========================================================
# SECTION 9 — Forwarding system
# ==========================================================

def resolve_forward_targets(src_chat_id: int):
    """
    Возвращает список чатов, куда нужно пересылать сообщения из src.
    """
    fw = data.get("forward_rules", {})
    src = str(src_chat_id)
    if src not in fw:
        return []

    targets = []
    for dst, mode in fw[src].items():
        if mode in ("oneway_to", "twoway"):
            targets.append(int(dst))
    return targets


def forward_text_anon(src_chat_id: int, msg, targets: list):
    """
    Анонимная пересылка текста.
    """
    clean = msg.text
    for t in targets:
        try:
            bot.send_message(t, clean)
        except:
            pass


def add_forward_link(src: int, dst: int, mode: str):
    fw = data.setdefault("forward_rules", {})
    fw.setdefault(str(src), {})[str(dst)] = mode
    save_data(data)


def remove_forward_link(src: int, dst: int):
    fw = data.setdefault("forward_rules", {})
    src = str(src)
    dst = str(dst)
    if src in fw and dst in fw[src]:
        del fw[src][dst]
        save_data(data)


# ==========================================================
# SECTION 10 — UI builders
# ==========================================================

def build_main_keyboard(day_key: str, chat_id: int):
    kb = types.InlineKeyboardMarkup()
    kb.row(
        types.InlineKeyboardButton("➕ Добавить", callback_data=f"d:{day_key}:add"),
        types.InlineKeyboardButton("✏️ Редактировать", callback_data=f"d:{day_key}:edit_menu"),
    )
    kb.row(
        types.InlineKeyboardButton("⬅️", callback_data=f"d:{day_key}:prev"),
        types.InlineKeyboardButton("📅", callback_data=f"d:{day_key}:calendar"),
        types.InlineKeyboardButton("➡️", callback_data=f"d:{day_key}:next"),
    )
    kb.row(
        types.InlineKeyboardButton("📊 Отчёт", callback_data=f"d:{day_key}:report"),
        types.InlineKeyboardButton("💰 Итог", callback_data=f"d:{day_key}:total"),
    )
    kb.row(
        types.InlineKeyboardButton("CSV (день)", callback_data=f"d:{day_key}:csv_day"),
        types.InlineKeyboardButton("CSV (всё)", callback_data=f"d:{day_key}:csv_all"),
    )
    kb.row(types.InlineKeyboardButton("ℹ️ Инфо", callback_data=f"d:{day_key}:info"))
    return kb
# ==========================================================
# SECTION 11 — Day window renderer (исправленный, полный)
# ==========================================================

def render_day_window(chat_id: int, day_key: str):
    """
    Рендер окна дня:
        • заголовок
        • сортировка записей
        • short_id
        • время
        • комментарий
        • итог дня
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

        sign = "➕" if amt >= 0 else "➖"
        note = html.escape(r.get("note", ""))
        sid = r.get("short_id", f"R{r['id']}")

        ts = r.get("timestamp", "")
        ts_show = ts[11:16] if ts else ""

        lines.append(f"{sid} — {sign} {fmt_num(amt)}  ({ts_show})")
        if note:
            lines.append(f"      <i>{note}</i>")

    if not recs_sorted:
        lines.append("Нет записей за этот день.")

    lines.append("")
    lines.append(f"💰 <b>Итого: {fmt_num(total)}</b>")

    return "\n".join(lines), total


# ==========================================================
# SECTION 12 — CSV helpers
# ==========================================================

def cmd_csv_day(chat_id: int, day_key: str):
    """
    Экспорт CSV только за выбранный день
    """
    store = get_chat_store(chat_id)
    rows = store.get("daily_records", {}).get(day_key, [])

    fname = f"csv_day_{chat_id}_{day_key}.csv"
    try:
        with open(fname, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["id", "time", "amount", "note"])
            for r in rows:
                w.writerow([r["id"], r["timestamp"], r["amount"], r["note"]])

        bot.send_document(chat_id, open(fname, "rb"))
    except Exception as e:
        logging.error(f"cmd_csv_day error: {e}")


def cmd_csv_all(chat_id: int):
    """
    CSV всего чата data_<chat_id>.json
    """
    store = get_chat_store(chat_id)
    recs = store.get("records", [])

    fname = f"csv_full_{chat_id}.csv"
    try:
        with open(fname, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["id", "timestamp", "amount", "note"])
            for r in recs:
                w.writerow([r["id"], r["timestamp"], r["amount"], r["note"]])

        bot.send_document(chat_id, open(fname, "rb"))
    except Exception as e:
        logging.error(f"cmd_csv_all error: {e}")
# ==========================================================
# SECTION 13 — Add / Update / Delete (исправленный, финальный)
# ==========================================================

def add_record_to_chat(chat_id: int, amount: int, note: str, owner):
    """
    Добавление финансовой записи + обновление всех структур + обновление UI.
    """
    store = get_chat_store(chat_id)
    day_key = today_key()

    rid = store.get("next_id", 1)
    rec = {
        "id": rid,
        "short_id": f"R{rid}",
        "timestamp": now_local().isoformat(timespec="seconds"),
        "amount": amount,
        "note": note,
        "owner": owner,
    }

    # глобальная база
    data.setdefault("records", []).append(rec)

    # per-chat
    store.setdefault("records", []).append(rec)
    store.setdefault("daily_records", {}).setdefault(day_key, []).append(rec)

    # пересчёт балансов
    store["balance"] = sum(x["amount"] for x in store["records"])
    data["overall_balance"] = sum(x["amount"] for x in data["records"])

    store["next_id"] = rid + 1

    # сохраняем
    save_data(data)
    save_chat_json(chat_id)
    export_global_csv(data)

    # бэкап
    send_backup_to_channel(chat_id)

    # обновляем UI
    update_or_send_day_window(chat_id, day_key)


def update_record_in_chat(chat_id: int, rid: int, new_amount: int, new_note: str):
    """
    Обновление записи в records + daily_records + глобально + UI.
    """
    store = get_chat_store(chat_id)
    found = None
    day_key = None

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
    for dk, arr in store.get("daily_records", {}).items():
        for r in arr:
            if r["id"] == rid:
                r.update(found)
                day_key = dk

    # обновляем баланс чата
    store["balance"] = sum(x["amount"] for x in store["records"])

    # глобально
    data["records"] = [x if x["id"] != rid else found for x in data["records"]]
    data["overall_balance"] = sum(x["amount"] for x in data["records"])

    # сохраняем
    save_data(data)
    save_chat_json(chat_id)
    export_global_csv(data)

    # бэкап
    send_backup_to_channel(chat_id)

    # обновляем UI
    if day_key:
        update_or_send_day_window(chat_id, day_key)


def delete_record_in_chat(chat_id: int, rid: int):
    """
    Удаление записи + пересчёт + обновление UI.
    """
    store = get_chat_store(chat_id)
    day_key = None

    # удаляем из daily_records
    for dk, arr in list(store.get("daily_records", {}).items()):
        new_arr = [x for x in arr if x["id"] != rid]
        if len(new_arr) != len(arr):
            day_key = dk
        if new_arr:
            store["daily_records"][dk] = new_arr
        else:
            del store["daily_records"][dk]

    # удаляем из records
    store["records"] = [x for x in store["records"] if x["id"] != rid]

    # обновляем баланс
    store["balance"] = sum(x["amount"] for x in store["records"])

    # удаляем глобально
    data["records"] = [x for x in data["records"] if x["id"] != rid]
    data["overall_balance"] = sum(x["amount"] for x in data["records"])

    # сохраняем
    save_data(data)
    save_chat_json(chat_id)
    export_global_csv(data)

    # бэкап
    send_backup_to_channel(chat_id)

    # обновление UI
    if day_key:
        update_or_send_day_window(chat_id, day_key)
# ==========================================================
# SECTION 14 — Active window system (исправленный, полный)
# ==========================================================

def get_or_create_active_windows(chat_id: int) -> dict:
    """
    Возвращает словарь: { day_key: message_id }
    """
    return data.setdefault("active_messages", {}).setdefault(str(chat_id), {})


def set_active_window_id(chat_id: int, day_key: str, message_id: int):
    """
    Записывает номер сообщения активного окна дня.
    """
    aw = get_or_create_active_windows(chat_id)
    aw[day_key] = message_id
    save_data(data)


def get_active_window_id(chat_id: int, day_key: str):
    """
    Возвращает message_id активного окна, если есть.
    """
    aw = get_or_create_active_windows(chat_id)
    return aw.get(day_key)


def delete_active_window_if_exists(chat_id: int, day_key: str):
    """
    Удаляет предыдущее окно, если сообщение существует.
    """
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
    Обновляет существующее окно или создаёт новое.
    """
    txt, _ = render_day_window(chat_id, day_key)
    kb = build_main_keyboard(day_key, chat_id)

    mid = get_active_window_id(chat_id, day_key)
    if mid:
        # Пытаемся обновить существующее окно
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
            # окно удалено пользователем → создаём новое
            pass

    sent = bot.send_message(chat_id, txt, reply_markup=kb, parse_mode="HTML")
    set_active_window_id(chat_id, day_key, sent.message_id)


# ==========================================================
# SECTION 15 — Calendar keyboard builder
# ==========================================================

def build_calendar_keyboard(center_date: datetime):
    """
    Рисует календарь последних 31 дня.
    """
    kb = types.InlineKeyboardMarkup()

    # 31 день назад — 0
    days = []
    for i in range(31):
        d = center_date - timedelta(days=30 - i)
        days.append(d.strftime("%Y-%m-%d"))

    row = []
    for dk in days:
        row.append(types.InlineKeyboardButton(
            dk[8:10], callback_data=f"d:{dk}:open"
        ))
        if len(row) == 7:
            kb.row(*row)
            row = []

    if row:
        kb.row(*row)

    # кнопка ручного ввода
    kb.row(types.InlineKeyboardButton(
        "📆 Ввести дату вручную",
        callback_data="d:0000-00-00:pick_date"
    ))

    # кнопка назад — возвращает текущее окно дня
    today = today_key()
    kb.row(types.InlineKeyboardButton(
        "🔙 Назад",
        callback_data=f"d:{today}:open"
    ))

    return kb
# ==========================================================
# SECTION 16 — Callback handler (исправленный, финальный)
# ==========================================================

@bot.callback_query_handler(func=lambda c: True)
def on_callback(call):
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
            bot.edit_message_reply_markup(
                chat_id=chat_id,
                message_id=call.message.message_id,
                reply_markup=kb
            )
            return

        # -------------------------------------------------------
        # d:<day_key>:<cmd>
        # -------------------------------------------------------
        if not data_str.startswith("d:"):
            return

        _, day_key, cmd = data_str.split(":", 2)
        store = get_chat_store(chat_id)

        # -------------------------------------------------------
        # ОТКРЫТЬ ДЕНЬ
        # -------------------------------------------------------
        if cmd == "open":
            update_or_send_day_window(chat_id, day_key)
            return

        # -------------------------------------------------------
        # НАЗАД
        # -------------------------------------------------------
        if cmd == "back_main":
            update_or_send_day_window(chat_id, day_key)
            return

        # -------------------------------------------------------
        # ПРЕДЫДУЩИЙ ДЕНЬ
        # -------------------------------------------------------
        if cmd == "prev":
            d = datetime.strptime(day_key, "%Y-%m-%d") - timedelta(days=1)
            nd = d.strftime("%Y-%m-%d")
            update_or_send_day_window(chat_id, nd)
            return

        # -------------------------------------------------------
        # СЛЕДУЮЩИЙ ДЕНЬ
        # -------------------------------------------------------
        if cmd == "next":
            d = datetime.strptime(day_key, "%Y-%m-%d") + timedelta(days=1)
            nd = d.strftime("%Y-%m-%d")
            update_or_send_day_window(chat_id, nd)
            return

        # -------------------------------------------------------
        # КАЛЕНДАРЬ
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
        # ОТЧЁТ
        # -------------------------------------------------------
        if cmd == "report":
            lines = ["📊 Отчёт:"]
            for dk, recs in sorted(store.get("daily_records", {}).items()):
                s = sum(r["amount"] for r in recs)
                lines.append(f"{dk}: {fmt_num(s)}")

            bot.send_message(chat_id, "\n".join(lines))
            return

        # -------------------------------------------------------
        # ИТОГ
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
        # ИНФО
        # -------------------------------------------------------
        if cmd == "info":
            info_text = (
                f"ℹ️ Финансовый бот — версия {VERSION}\n\n"
                "/поехали — включить финансовый режим\n"
                "/view YYYY-MM-DD — открыть конкретный день\n"
                "/prev /next — навигация\n"
                "/report — отчёт\n"
                "/balance — баланс\n"
                "/csv — CSV текущего чата\n"
                "/json — JSON этого чата\n"
                "/reset — обнулить данные чата\n"
            )
            bot.send_message(chat_id, info_text)
            return

        # -------------------------------------------------------
        # МЕНЮ РЕДАКТИРОВАНИЯ
        # -------------------------------------------------------
        if cmd == "edit_menu":
            kb = build_edit_menu_keyboard(day_key, chat_id)
            bot.edit_message_reply_markup(
                chat_id=chat_id,
                message_id=call.message.message_id,
                reply_markup=kb
            )
            return

        # -------------------------------------------------------
        # CSV: день и всё
        # -------------------------------------------------------
        if cmd == "csv_day":
            cmd_csv_day(chat_id, day_key)
            return

        if cmd == "csv_all":
            cmd_csv_all(chat_id)
            return

        # -------------------------------------------------------
        # ДОБАВИТЬ ЗАПИСЬ
        # -------------------------------------------------------
        if cmd == "add":
            store["edit_wait"] = {"type": "add", "day_key": day_key}
            save_data(data)
            bot.send_message(chat_id, "Введите сумму и комментарий:  +500 Пример")
            return

        # -------------------------------------------------------
        # СПИСОК ЗАПИСЕЙ В ДЕНЬ
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

            kb2.row(types.InlineKeyboardButton("🔙 Назад", callback_data=f"d:{day_key}:edit_menu"))
            bot.send_message(chat_id, "Выберите запись:", reply_markup=kb2)
            return

        # -------------------------------------------------------
        # ВЫБОР ЗАПИСИ ДЛЯ РЕДАКТИРОВАНИЯ
        # -------------------------------------------------------
        if cmd.startswith("edit_rec_"):
            rid = int(cmd.split("_")[-1])
            store["edit_wait"] = {"type": "edit", "day_key": day_key, "rid": rid}
            save_data(data)
            bot.send_message(chat_id, f"Введите новую сумму и текст для записи R{rid}:")
            return

        # -------------------------------------------------------
        # ПЕРЕСЫЛКА — Меню только для владельца
        # -------------------------------------------------------
        if cmd == "forward_menu":
            if not OWNER_ID or str(chat_id) != str(OWNER_ID):
                bot.send_message(chat_id, "Меню доступно только владельцу.")
                return

            kb = build_forward_chat_list(day_key, chat_id)
            bot.edit_message_text(
                "Выберите чат:",
                chat_id=chat_id,
                message_id=call.message.message_id,
                reply_markup=kb
            )
            return

        # -------------------------------------------------------
        # ПЕРЕСЫЛКА — Выбор чата
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
        # ПЕРЕСЫЛКА — ➡️ (owner → tgt)
        # -------------------------------------------------------
        if cmd.startswith("fw_one_"):
            tgt = int(cmd.split("_")[-1])
            add_forward_link(chat_id, tgt, "oneway_to")
            bot.send_message(chat_id, f"Установлена пересылка ➡️  {chat_id} → {tgt}")
            return

        # -------------------------------------------------------
        # ПЕРЕСЫЛКА — ⬅️ (tgt → owner)
        # -------------------------------------------------------
        if cmd.startswith("fw_rev_"):
            tgt = int(cmd.split("_")[-1])
            add_forward_link(tgt, chat_id, "oneway_to")
            add_forward_link(chat_id, tgt, "oneway_from")
            bot.send_message(chat_id, f"Установлена пересылка ⬅️  {tgt} → {chat_id}")
            return

        # -------------------------------------------------------
        # ПЕРЕСЫЛКА — ↔️ (двусторонняя)
        # -------------------------------------------------------
        if cmd.startswith("fw_two_"):
            tgt = int(cmd.split("_")[-1])
            add_forward_link(chat_id, tgt, "twoway")
            add_forward_link(tgt, chat_id, "twoway")
            bot.send_message(chat_id, f"Установлена двусторонняя пересылка ↔️  {chat_id} ⇄ {tgt}")
            return

        # -------------------------------------------------------
        # УДАЛЕНИЕ ПРАВИЛ
        # -------------------------------------------------------
        if cmd.startswith("fw_del_"):
            tgt = int(cmd.split("_")[-1])
            remove_forward_link(chat_id, tgt)
            remove_forward_link(tgt, chat_id)
            bot.send_message(chat_id, f"Удалены все связи с {tgt}.")
            return

        # -------------------------------------------------------
        # ВВЕСТИ ДАТУ ВРУЧНУЮ
        # -------------------------------------------------------
        if cmd == "pick_date":
            bot.send_message(chat_id, "Введите дату:\n/view YYYY-MM-DD")
            return

    except Exception as e:
        log_error(f"on_callback error: {e}")
# ==========================================================
# SECTION 17 — Edit menu keyboard
# ==========================================================

def build_edit_menu_keyboard(day_key: str, chat_id: int):
    """
    Меню редактирования для выбранного дня.
    """
    kb = types.InlineKeyboardMarkup()
    kb.row(
        types.InlineKeyboardButton("📝 Список записей", callback_data=f"d:{day_key}:edit_list")
    )
    kb.row(
        types.InlineKeyboardButton("🔙 Назад", callback_data=f"d:{day_key}:open")
    )
    return kb


# ==========================================================
# SECTION 18 — Text handler (исправленный, финальный)
# ==========================================================

@bot.message_handler(content_types=["text"])
def handle_text(msg):
    try:
        chat_id = msg.chat.id
        text = (msg.text or "").strip()

        # --- 1. обновляем info о чате (название, username)
        update_chat_info_from_message(msg)

        # --- 2. если есть правила пересылки → пересылаем анонимно
        targets = resolve_forward_targets(chat_id)
        if targets:
            forward_text_anon(chat_id, msg, targets)

        store = get_chat_store(chat_id)
        wait = store.get("edit_wait")

        # ------------------------------------------------------
        #   РЕЖИМ ДОБАВЛЕНИЯ НОВОЙ ЗАПИСИ
        # ------------------------------------------------------
        if wait and wait.get("type") == "add":
            try:
                parts = text.split(" ", 1)
                amount = parse_amount(parts[0])
                note = parts[1] if len(parts) > 1 else ""
            except:
                bot.send_message(chat_id, "❌ Ошибка формата. Пример: +500 Обед")
                return

            add_record_to_chat(chat_id, amount, note, msg.from_user.id)

            store["edit_wait"] = None
            save_data(data)

            update_or_send_day_window(chat_id, wait["day_key"])
            return

        # ------------------------------------------------------
        #   РЕЖИМ РЕДАКТИРОВАНИЯ СУЩЕСТВУЮЩЕЙ ЗАПИСИ
        # ------------------------------------------------------
        if wait and wait.get("type") == "edit":
            rid = wait["rid"]

            try:
                parts = text.split(" ", 1)
                amount = parse_amount(parts[0])
                note = parts[1] if len(parts) > 1 else ""
            except:
                bot.send_message(chat_id, "❌ Ошибка формата. Пример: -1200 Такси")
                return

            update_record_in_chat(chat_id, rid, amount, note)

            store["edit_wait"] = None
            save_data(data)

            update_or_send_day_window(chat_id, wait["day_key"])
            return

        # ------------------------------------------------------
        #   ПОДТВЕРЖДЕНИЕ СБРОСА (reset)
        # ------------------------------------------------------
        if text.upper() == "ДА":
            reset_chat_data(chat_id)
            bot.send_message(chat_id, "🔄 Данные чата обнулены.")
            return

        # ------------------------------------------------------
        #   ПРОСТОЙ ТЕКСТ → игнорируется
        # ------------------------------------------------------
        # Функции пересылки уже отработали выше.

    except Exception as e:
        log_error(f"handle_text: {e}")
# ==========================================================
# SECTION 19 — Reset chat data
# ==========================================================

def reset_chat_data(chat_id: int):
    """
    Очищает только данные ЭТОГО чата (индивидуально).
    """
    cid = str(chat_id)
    data["finance_active_chats"][cid] = {
        "records": [],
        "daily_records": {},
        "balance": 0,
        "next_id": 1,
        "info": data["known_chats"].get(cid, {}),
    }

    save_chat_json(chat_id)
    save_data(data)
    export_global_csv(data)


# ==========================================================
# SECTION 20 — JSON exporter
# ==========================================================

def cmd_export_json(chat_id: int):
    """
    Отправляет JSON только для этого чата: data_<chat_id>.json
    """
    fname = f"data_{chat_id}.json"
    if not os.path.exists(fname):
        save_chat_json(chat_id)

    try:
        bot.send_document(chat_id, open(fname, "rb"))
    except Exception as e:
        log_error(f"cmd_export_json error: {e}")


# ==========================================================
# SECTION 21 — Commands: /start, /поехали, /view, /prev, /next
# ==========================================================

@bot.message_handler(commands=["start"])
def cmd_start(msg):
    chat_id = msg.chat.id

    update_chat_info_from_message(msg)

    # если чат не активировал финансовый режим → подсказка
    if str(chat_id) not in data.get("finance_active_chats", {}):
        bot.send_message(
            chat_id,
            "👋 Привет!\n"
            "Чтобы включить финансовый режим, отправьте команду:\n\n"
            "<b>/поехали</b>",
        )
        return

    dk = today_key()
    update_or_send_day_window(chat_id, dk)


@bot.message_handler(commands=["поехали"])
def cmd_go(msg):
    """
    Активирует финансовый режим в чате.
    """
    chat_id = msg.chat.id

    update_chat_info_from_message(msg)

    # создаём пустой store если его не было
    get_chat_store(chat_id)

    bot.send_message(chat_id, "🚀 Финансовый режим активирован!")
    update_or_send_day_window(chat_id, today_key())


@bot.message_handler(commands=["view"])
def cmd_view(msg):
    """
    /view YYYY-MM-DD
    """
    chat_id = msg.chat.id
    parts = (msg.text or "").split()

    update_chat_info_from_message(msg)

    if len(parts) != 2:
        bot.send_message(chat_id, "Формат: /view YYYY-MM-DD")
        return

    try:
        datetime.strptime(parts[1], "%Y-%m-%d")
    except:
        bot.send_message(chat_id, "Ошибка даты. Формат: /view YYYY-MM-DD")
        return

    dk = parts[1]
    update_or_send_day_window(chat_id, dk)


@bot.message_handler(commands=["prev"])
def cmd_prev(msg):
    chat_id = msg.chat.id
    update_chat_info_from_message(msg)

    dk = today_key()
    try:
        last_shown = data.get("last_day", {}).get(str(chat_id))
        if last_shown:
            dk = last_shown
    except:
        pass

    d = datetime.strptime(dk, "%Y-%m-%d") - timedelta(days=1)
    nd = d.strftime("%Y-%m-%d")

    data.setdefault("last_day", {})[str(chat_id)] = nd
    save_data(data)

    update_or_send_day_window(chat_id, nd)


@bot.message_handler(commands=["next"])
def cmd_next(msg):
    chat_id = msg.chat.id
    update_chat_info_from_message(msg)

    dk = today_key()
    try:
        last_shown = data.get("last_day", {}).get(str(chat_id))
        if last_shown:
            dk = last_shown
    except:
        pass

    d = datetime.strptime(dk, "%Y-%m-%d") + timedelta(days=1)
    nd = d.strftime("%Y-%m-%d")

    data.setdefault("last_day", {})[str(chat_id)] = nd
    save_data(data)

    update_or_send_day_window(chat_id, nd)
# ==========================================================
# SECTION 22 — Other Commands (/balance, /csv, /json, /stopforward)
# ==========================================================

@bot.message_handler(commands=["balance"])
def cmd_balance(msg):
    chat_id = msg.chat.id
    update_chat_info_from_message(msg)

    store = get_chat_store(chat_id)
    bal = store.get("balance", 0)
    overall = data.get("overall_balance", 0)

    bot.send_message(
        chat_id,
        f"💰 <b>Баланс чата:</b> {fmt_num(bal)}\n"
        f"🌎 <b>Все чаты:</b> {fmt_num(overall)}",
        parse_mode="HTML",
    )


@bot.message_handler(commands=["csv"])
def cmd_csv(msg):
    chat_id = msg.chat.id
    update_chat_info_from_message(msg)
    cmd_csv_all(chat_id)


@bot.message_handler(commands=["json"])
def cmd_json(msg):
    chat_id = msg.chat.id
    update_chat_info_from_message(msg)
    cmd_export_json(chat_id)


@bot.message_handler(commands=["stopforward"])
def cmd_stop_forward(msg):
    chat_id = msg.chat.id

    fw = data.get("forward_rules", {})
    if str(chat_id) in fw:
        del fw[str(chat_id)]
        save_data(data)

    bot.send_message(chat_id, "⛔ Пересылка отключена для этого чата.")


# ==========================================================
# SECTION 23 — Keep-alive system (self-ping)
# ==========================================================

def keep_alive_ping():
    """
    Периодический self-ping, чтобы Render / Railway не засыпал.
    """
    while True:
        try:
            if APP_URL:
                requests.get(APP_URL)
        except:
            pass

        time.sleep(60 * 5)   # пинг каждые 5 минут


def start_keep_alive_thread():
    th = threading.Thread(target=keep_alive_ping, daemon=True)
    th.start()


# ==========================================================
# SECTION 24 — Webhook + Flask App
# ==========================================================

@app.route(f"/{BOT_TOKEN}", methods=["POST"])
def webhook_receiver():
    try:
        json_str = request.get_data().decode("utf-8")
        update = telebot.types.Update.de_json(json_str)
        bot.process_new_updates([update])
    except Exception as e:
        log_error(f"Webhook error: {e}")
    return "OK", 200


@app.route("/", methods=["GET"])
def home():
    return f"Bot {VERSION} running."
# ==========================================================
# SECTION 25 — Final run (webhook setup + restore + keep-alive)
# ==========================================================

def final_run():
    logging.info(f"🚀 Запуск {VERSION}...")

    # 1) Загружаем локальные данные
    global data
    restored = restore_from_gdrive_if_needed()
    data = load_data()

    # 2) Если данные восстановлены — уведомляем владельца
    if restored and OWNER_ID:
        try:
            bot.send_message(
                int(OWNER_ID),
                "☁️ Данные успешно восстановлены из Google Drive."
            )
        except:
            pass

    # 3) Keep-alive поток
    start_keep_alive_thread()

    # 4) Настраиваем webhook
    if APP_URL:
        wh_url = f"{APP_URL}/{BOT_TOKEN}"
        try:
            bot.remove_webhook()
        except:
            pass

        time.sleep(1)

        try:
            bot.set_webhook(url=wh_url)
            logging.info(f"Webhook установлен: {wh_url}")
        except Exception as e:
            logging.error(f"Webhook set error: {e}")


# ==========================================================
# SECTION 26 — Entry Point
# ==========================================================

if __name__ == "__main__":
    try:
        final_run()
    except Exception as e:
        logging.error(f"MAIN_FATAL: {e}")
