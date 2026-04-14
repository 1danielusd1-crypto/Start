import os
import io
import json
import csv
import re
import html
import logging
import sqlite3
import threading
import time

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests
import telebot
from telebot import types
from telebot.types import InputMediaDocument, InputMediaPhoto, InputMediaVideo, InputMediaAudio, InputMediaAnimation

from flask import Flask, request


from collections import defaultdict

window_locks = defaultdict(threading.Lock)
BOT_TOKEN = os.getenv("B_T", "").strip()
OWNER_ID = os.getenv("ID", "").strip()
APP_URL = os.getenv("APP_URL", "").strip() or os.getenv("RENDER_EXTERNAL_URL", "").strip()
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "").strip() or APP_URL
try:
    PORT = int(os.getenv("PORT", "5000"))
except Exception:
    PORT = 5000
BACKUP_CHAT_ID = os.getenv("BACKUP_CHAT_ID", "").strip()
if not BOT_TOKEN:
    raise RuntimeError("B_T is not set")
VERSION = "Скачать: bot_19_fixed_ready.py"
DEFAULT_TZ = "America/Argentina/Buenos_Aires"
KEEP_ALIVE_INTERVAL_SECONDS = 30
DB_FILE = os.getenv("DB_FILE", "bot_state.sqlite3").strip() or "bot_state.sqlite3"
DATA_FILE = "data.json"
CSV_FILE = "data.csv"
CSV_META_FILE = "csv_meta.json"
forward_map = {}
backup_flags = {
    "channel": True,
}
restore_mode = None
_media_group_cache = {}
_media_group_timers = {}
FORWARD_MEDIA_GROUP_DELAY = 0.8
_forward_state_timer = None
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)
bot = telebot.TeleBot(BOT_TOKEN, parse_mode=None)
app = Flask(__name__)
data = {}
finance_active_chats = set()


class SQLiteState:
    def __init__(self, path: str):
        self.path = path
        self.lock = threading.RLock()
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._init_db()

    def _init_db(self):
        with self.lock:
            cur = self.conn.cursor()
            cur.execute("PRAGMA journal_mode=WAL")
            cur.execute("PRAGMA synchronous=NORMAL")
            cur.execute("PRAGMA temp_store=MEMORY")
            cur.execute("PRAGMA foreign_keys=ON")
            cur.execute(
                "CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v TEXT NOT NULL)"
            )
            cur.execute(
                "CREATE TABLE IF NOT EXISTS chats (chat_id TEXT PRIMARY KEY, v TEXT NOT NULL)"
            )
            cur.execute(
                "CREATE TABLE IF NOT EXISTS meta (kind TEXT NOT NULL, k TEXT NOT NULL, v TEXT NOT NULL, PRIMARY KEY(kind, k))"
            )
            self.conn.commit()

    def _dump(self, obj) -> str:
        return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))

    def _load(self, raw, default=None):
        if raw is None:
            return default
        try:
            return json.loads(raw)
        except Exception:
            return default

    def get_kv(self, key: str, default=None):
        with self.lock:
            row = self.conn.execute("SELECT v FROM kv WHERE k=?", (key,)).fetchone()
        return self._load(row[0], default) if row else default

    def set_kv(self, key: str, obj):
        payload = self._dump(obj)
        with self.lock:
            self.conn.execute(
                "INSERT INTO kv(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
                (key, payload),
            )
            self.conn.commit()

    def load_root(self):
        return self.get_kv("root", None)

    def save_root(self, obj):
        self.set_kv("root", obj)

    def load_chats(self) -> dict:
        with self.lock:
            rows = self.conn.execute("SELECT chat_id, v FROM chats").fetchall()
        out = {}
        for row in rows:
            val = self._load(row[1], {})
            if isinstance(val, dict):
                out[str(row[0])] = val
        return out

    def save_chats(self, chats: dict):
        chats = chats or {}
        with self.lock:
            existing = {str(r[0]) for r in self.conn.execute("SELECT chat_id FROM chats").fetchall()}
            for chat_id, payload in chats.items():
                self.conn.execute(
                    "INSERT INTO chats(chat_id,v) VALUES(?,?) ON CONFLICT(chat_id) DO UPDATE SET v=excluded.v",
                    (str(chat_id), self._dump(payload)),
                )
            for stale in existing - {str(k) for k in chats.keys()}:
                self.conn.execute("DELETE FROM chats WHERE chat_id=?", (stale,))
            self.conn.commit()

    def get_meta(self, kind: str, key: str, default=None):
        with self.lock:
            row = self.conn.execute(
                "SELECT v FROM meta WHERE kind=? AND k=?", (kind, key)
            ).fetchone()
        return self._load(row[0], default) if row else default

    def set_meta(self, kind: str, key: str, obj):
        payload = self._dump(obj)
        with self.lock:
            self.conn.execute(
                "INSERT INTO meta(kind,k,v) VALUES(?,?,?) ON CONFLICT(kind,k) DO UPDATE SET v=excluded.v",
                (kind, key, payload),
            )
            self.conn.commit()


SQLITE = SQLiteState(DB_FILE)


def _sqlite_pack_root(d: dict) -> dict:
    return {k: v for k, v in (d or {}).items() if k != "chats"}


def _sqlite_unpack_data(root: dict | None, chats: dict | None) -> dict:
    d = default_data()
    if isinstance(root, dict):
        for k, v in root.items():
            d[k] = v
    d["chats"] = chats if isinstance(chats, dict) else {}
    return d


def _import_legacy_global_json_to_db(path: str = DATA_FILE, force: bool = False) -> bool:
    root = SQLITE.load_root()
    chats = SQLITE.load_chats()
    if not force and (root is not None or chats):
        return False

    payload = _load_json(path, None)
    if not isinstance(payload, dict):
        return False

    SQLITE.save_root(_sqlite_pack_root(payload))
    SQLITE.save_chats(payload.get("chats", {}) or {})

    legacy_csv_meta = _load_json(CSV_META_FILE, None)
    if isinstance(legacy_csv_meta, dict):
        SQLITE.set_meta("csv_meta", "main", legacy_csv_meta)

    legacy_backup_meta = _load_json(CHAT_BACKUP_META_FILE, None)
    if isinstance(legacy_backup_meta, dict):
        SQLITE.set_meta("chat_backup_meta", "main", legacy_backup_meta)

    return True


def _import_payload_to_db(payload: dict):
    SQLITE.save_root(_sqlite_pack_root(payload))
    SQLITE.save_chats(payload.get("chats", {}) or {})


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

DAY_WINDOW_MAX_RECORDS = 35
DAY_WINDOW_MAX_CHARS = 3500

BALANCE_PANEL_REFRESH_DELAY = 5.0
BALANCE_PANEL_COLLAPSE_DELAY = 60.0
COMMAND_DELETE_DELAY = 30
HELPER_DELETE_DELAY = 25
DOZVON_INTERVAL_SECONDS = 0.5
DOZVON_BURST_SECONDS = 10
DOZVON_PAUSE_SECONDS = 5
OWNER_TOTAL_WINDOW_DELETE_DELAY = 60
AUX_WINDOW_DELETE_DELAY = 120

_dozvon_sessions = {}
_dozvon_target_index = defaultdict(set)


def day_key_from_message(msg=None) -> str:
    try:
        if msg and getattr(msg, "date", None):
            return datetime.fromtimestamp(msg.date, tz=get_tz()).strftime("%Y-%m-%d")
    except Exception:
        pass
    return today_key()

def fmt_num(v):
    try:
        v = float(v)
        if v.is_integer():
            return str(int(v))
        return str(v)
    except:
        return str(v)
    
def fmt_date_ddmmyy(day_key: str) -> str:
    """YYYY-MM-DD -> DD.MM.YY"""
    try:
        d = datetime.strptime(day_key, "%Y-%m-%d")
        return d.strftime("%d.%m.%y")
    except Exception:
        return str(day_key)
def fmt_num_compact(v) -> str:
    """
    Число без .0, с минусом при необходимости.
    """
    try:
        v = float(v)
        if v.is_integer():
            return str(int(v))
        s = f"{v:.2f}".rstrip("0").rstrip(".")
        return s
    except Exception:
        return str(v)


def fmt_csv_amount(v) -> str:
    """CSV-представление суммы без минуса; доход с префиксом «+»."""
    try:
        v = float(v or 0)
    except Exception:
        return str(v)
    body = fmt_num_compact(abs(v))
    if v > 0:
        return f"+ {body}"
    return body


def parse_csv_amount(raw) -> float:
    """Понимает новый CSV-формат и старые +/- значения."""
    s = str(raw or "").strip()
    if not s:
        return 0.0
    low = s.lower()
    if low.startswith("+"):
        num = s[5:].strip()
        return abs(parse_amount("+" + num))
    if s.startswith(("+", "-", "–")):
        return parse_amount(s)
    return -abs(parse_amount(s))

def write_csv_rows_with_day_gaps(writer, rows, width: int | None = None):
    prev_day = None
    for row in rows:
        row = list(row)
        day = str(row[0]) if row else ""
        if prev_day is not None and day != prev_day:
            writer.writerow([""] * (width or len(row)))
        writer.writerow(row)
        prev_day = day


def center_text(text: str, width: int) -> str:
    """
    Центрирование строки в фиксированной ширине.
    Если строка длиннее width — возвращаем как есть.
    """
    text = str(text)
    if len(text) >= width:
        return text
    pad = width - len(text)
    left = pad // 2
    right = pad - left
    return (" " * left) + text + (" " * right)


def report_cell(value, width: int = 7) -> str:
    """Числовая ячейка отчёта фиксированной ширины."""
    s = fmt_num_compact(value)
    return s.rjust(width) if len(s) < width else s


def report_header_cell(label: str, width: int = 7) -> str:
    """Заголовок ячейки отчёта фиксированной ширины."""
    return center_text(label, width)


def get_chat_display_name(chat_id: int) -> str:
    try:
        store = get_chat_store(chat_id)
        info = store.get("info", {}) or {}
        title = (info.get("title") or "").strip()
        username = (info.get("username") or "").strip()
        if title:
            return title
        if username:
            return f"@{username.lstrip('@')}"
    except Exception:
        pass
    return f"Чат {chat_id}"


def format_finance_mode_label(chat_id: int) -> str:
    return "ВКЛ ✅" if is_finance_mode(chat_id) else "ВЫКЛ ❌"


def is_quick_balance_enabled(chat_id: int) -> bool:
    store = get_chat_store(chat_id)
    settings = store.setdefault("settings", {})
    return bool(settings.get("quick_balance_enabled", False))


def get_quick_balance_behavior(chat_id: int) -> str:
    store = get_chat_store(chat_id)
    settings = store.setdefault("settings", {})
    behavior = (settings.get("quick_balance_behavior") or "mini").strip().lower()
    return "open" if behavior == "open" else "mini"


def set_quick_balance_behavior(chat_id: int, behavior: str):
    store = get_chat_store(chat_id)
    settings = store.setdefault("settings", {})
    settings["quick_balance_behavior"] = "open" if str(behavior).lower() == "open" else "mini"


def set_quick_balance_enabled(chat_id: int, enabled: bool):
    chat_id = int(chat_id)
    store = get_chat_store(chat_id)
    settings = store.setdefault("settings", {})
    enabled = bool(enabled)
    settings["quick_balance_enabled"] = enabled
    settings["quick_balance_default_migrated"] = True

    if enabled:
        set_finance_mode(chat_id, True)
        store["balance_panel_mode"] = store.get("balance_panel_mode") or "mini"
        save_data(data)
        schedule_balance_panel_refresh(chat_id, 0.1)
        return

    panel_id = store.get("balance_panel_id")
    if panel_id:
        try:
            bot.delete_message(chat_id, panel_id)
        except Exception:
            pass
    store["balance_panel_id"] = None
    store["balance_panel_mode"] = "mini"
    save_data(data)


def schedule_owner_total_window_delete(chat_id: int, message_id: int, delay: int = OWNER_TOTAL_WINDOW_DELETE_DELAY):
    key = int(chat_id)

    def _job():
        try:
            bot.delete_message(chat_id, message_id)
        except Exception:
            pass
        try:
            store = get_chat_store(chat_id)
            if store.get("total_msg_id") == message_id:
                store["total_msg_id"] = None
                save_data(data)
        except Exception as e:
            log_error(f"schedule_owner_total_window_delete({chat_id}): {e}")

    prev = _total_message_timers.get(key)
    if prev and prev.is_alive():
        try:
            prev.cancel()
        except Exception:
            pass

    t = threading.Timer(delay, _job)
    _total_message_timers[key] = t
    t.start()


_aux_window_timers = {}


def _clear_stored_window(chat_id: int, store_key: str, message_id: int | None = None):
    try:
        store = get_chat_store(chat_id)
        current = store.get(store_key)
        if not current:
            return
        if message_id is not None and int(current) != int(message_id):
            return
        store[store_key] = None
        save_data(data)
    except Exception as e:
        log_error(f"_clear_stored_window({chat_id},{store_key}): {e}")


def schedule_stored_window_delete(chat_id: int, store_key: str, delay: int = AUX_WINDOW_DELETE_DELAY):
    key = (int(chat_id), str(store_key))

    def _job():
        try:
            store = get_chat_store(chat_id)
            message_id = store.get(store_key)
            if not message_id:
                return
            try:
                bot.delete_message(chat_id, message_id)
            except Exception:
                pass
            if store.get(store_key) == message_id:
                store[store_key] = None
                save_data(data)
        except Exception as e:
            log_error(f"schedule_stored_window_delete({chat_id},{store_key}): {e}")

    prev = _aux_window_timers.get(key)
    if prev and prev.is_alive():
        try:
            prev.cancel()
        except Exception:
            pass

    t = threading.Timer(delay, _job)
    _aux_window_timers[key] = t
    t.start()


def send_or_edit_stored_window(chat_id: int, store_key: str, text: str, reply_markup=None, parse_mode=None, delay: int = AUX_WINDOW_DELETE_DELAY):
    store = get_chat_store(chat_id)
    message_id = store.get(store_key)

    if message_id:
        try:
            bot.edit_message_text(
                text,
                chat_id=chat_id,
                message_id=message_id,
                reply_markup=reply_markup,
                parse_mode=parse_mode
            )
            schedule_stored_window_delete(chat_id, store_key, delay)
            return message_id
        except Exception:
            try:
                bot.edit_message_caption(
                    chat_id=chat_id,
                    message_id=message_id,
                    caption=text,
                    reply_markup=reply_markup,
                    parse_mode=parse_mode
                )
                schedule_stored_window_delete(chat_id, store_key, delay)
                return message_id
            except Exception:
                store[store_key] = None
                save_data(data)

    sent = bot.send_message(chat_id, text, reply_markup=reply_markup, parse_mode=parse_mode)
    store[store_key] = sent.message_id
    save_data(data)
    schedule_stored_window_delete(chat_id, store_key, delay)
    return sent.message_id


def delete_stored_window_if_exists(chat_id: int, store_key: str, message_id: int | None = None):
    try:
        store = get_chat_store(chat_id)
        current = store.get(store_key)
        if not current:
            return
        if message_id is not None and int(current) != int(message_id):
            return
        try:
            bot.delete_message(chat_id, int(current))
        except Exception:
            pass
        if store.get(store_key) == current:
            store[store_key] = None
            save_data(data)
    except Exception as e:
        log_error(f"delete_stored_window_if_exists({chat_id},{store_key}): {e}")


def build_toggle_label(prefix: str, title: str, enabled: bool) -> str:
    icon = "✅" if enabled else "❌"
    return f"{prefix} {icon} {title}"

def is_owner_chat(chat_id: int) -> bool:
    return bool(OWNER_ID and str(chat_id) == str(OWNER_ID))


def schedule_command_delete(msg):
    try:
        delete_message_later(msg.chat.id, msg.message_id, COMMAND_DELETE_DELAY)
    except Exception:
        pass


def guard_non_owner_finance_for_command(msg, allowed_commands=None) -> bool:
    allowed = {c.lower().lstrip('/') for c in (allowed_commands or [])}
    chat_id = msg.chat.id
    if is_owner_chat(chat_id):
        return False

    text = (getattr(msg, "text", None) or "").strip().lower()
    cmd = text.split()[0].split('@')[0].lstrip('/') if text else ""
    if cmd in allowed:
        return False

    if not is_finance_mode(chat_id):
        send_and_auto_delete(chat_id, "⚙️ Для этого включите финансовый режим командой /ok", HELPER_DELETE_DELAY)
        return True
    return False


def guard_non_owner_finance_for_callback(chat_id: int, data_str: str) -> bool:
    if is_owner_chat(chat_id):
        return False
    if is_finance_mode(chat_id):
        return False

    if data_str == "info_close":
        return False
    if data_str.startswith("d:") and data_str.endswith(":info"):
        return False

    send_and_auto_delete(chat_id, "⚙️ Для этого включите финансовый режим командой /ok", HELPER_DELETE_DELAY)
    return True


def add_buttons_in_rows(kb, buttons, per_row: int = 3):
    for i in range(0, len(buttons), per_row):
        kb.row(*buttons[i:i + per_row])
    return kb


def build_help_text(chat_id: int) -> str:
    lines = [
        f"ℹ️ Финансовый бот — версия {VERSION}",
        "",
        "Команды:",
        "/ok, /поехали — включить финансовый режим",
        "/start — окно сегодняшнего дня",
        "/view YYYY-MM-DD — открыть конкретный день",
        "/prev — предыдущий день",
        "/next — следующий день",
        "/balance — баланс по этому чату",
        "/report — краткий отчёт по дням",
        "/csv — CSV этого чата",
        "/json — JSON этого чата",
        "/reset — обнулить данные чата (с подтверждением)",
        "/ping — проверка, жив ли бот",
        "/restore / /restore_off — режим восстановления JSON/CSV",
        "/autoadd_info — режим авто-добавления по суммам",
        "/dozvon — окно дозвона по связанным чатам",
    ]
    if is_owner_chat(chat_id):
        lines.extend([
            "/stopforward — отключить пересылку",
            "/backup_channel_on / _off — включить/выключить бэкап в канал",
        ])
    lines.append("/help — эта справка")
    return "\n".join(lines)


def build_info_text(chat_id: int) -> str:
    return build_help_text(chat_id)


def get_connected_chat_ids(chat_id: int):
    connected = set()
    fr = data.get("forward_rules", {}) or {}
    src_key = str(chat_id)

    for dst in (fr.get(src_key, {}) or {}).keys():
        try:
            connected.add(int(dst))
        except Exception:
            pass

    for src, dsts in fr.items():
        if src_key in (dsts or {}):
            try:
                connected.add(int(src))
            except Exception:
                pass

    connected.discard(int(chat_id))
    return sorted(connected, key=lambda cid: get_chat_display_name(cid).lower())


def build_dozvon_menu(chat_id: int):
    kb = types.InlineKeyboardMarkup()
    buttons = []
    for cid in get_connected_chat_ids(chat_id):
        buttons.append(types.InlineKeyboardButton(
            get_chat_display_name(cid),
            callback_data=f"dzv:{cid}"
        ))
    if buttons:
        add_buttons_in_rows(kb, buttons, 3)
    kb.row(types.InlineKeyboardButton("❌ Закрыть", callback_data="dzv:close"))
    return kb


def stop_dozvon_for_target(target_chat_id: int, reason: str = "reply"):
    target_chat_id = int(target_chat_id)
    for session_key in list(_dozvon_target_index.get(target_chat_id, set())):
        sess = _dozvon_sessions.get(session_key)
        if sess:
            sess["stop"] = True
            sess["stop_reason"] = reason


def _cleanup_dozvon_session(session_key):
    sess = _dozvon_sessions.pop(session_key, None)
    if not sess:
        return None
    target_chat_id = int(sess["target_chat_id"])
    idx = _dozvon_target_index.get(target_chat_id)
    if idx and session_key in idx:
        idx.discard(session_key)
        if not idx:
            _dozvon_target_index.pop(target_chat_id, None)
    return sess


def _run_dozvon_session(session_key):
    sess = _dozvon_sessions.get(session_key)
    if not sess:
        return

    source_chat_id = int(sess["source_chat_id"])
    target_chat_id = int(sess["target_chat_id"])
    source_name = get_chat_display_name(source_chat_id)
    ping_text = f"📞 Дозвон от {source_name}"

    try:
        for phase in range(2):
            end_ts = time.time() + DOZVON_BURST_SECONDS
            while time.time() < end_ts:
                if sess.get("stop"):
                    break
                try:
                    sent = bot.send_message(target_chat_id, ping_text)
                    delete_message_later(target_chat_id, sent.message_id, 3)
                except Exception as e:
                    log_error(f"dozvon send to {target_chat_id}: {e}")
                    sess["stop"] = True
                    sess["stop_reason"] = "send_error"
                    break
                time.sleep(DOZVON_INTERVAL_SECONDS)

            if sess.get("stop"):
                break

            if phase == 0:
                pause_until = time.time() + DOZVON_PAUSE_SECONDS
                while time.time() < pause_until:
                    if sess.get("stop"):
                        break
                    time.sleep(0.2)
                if sess.get("stop"):
                    break
    finally:
        sess = _cleanup_dozvon_session(session_key) or {}
        reason = sess.get("stop_reason")
        if reason == "reply":
            send_and_auto_delete(source_chat_id, f"📞 Дозвон остановлен: {get_chat_display_name(target_chat_id)} ответил(а).", HELPER_DELETE_DELAY)
        elif reason == "send_error":
            send_and_auto_delete(source_chat_id, f"⚠️ Дозвон остановлен: не удалось отправить сообщения в {get_chat_display_name(target_chat_id)}.", HELPER_DELETE_DELAY)
        else:
            send_and_auto_delete(source_chat_id, f"📞 Дозвон завершён: {get_chat_display_name(target_chat_id)}.", HELPER_DELETE_DELAY)


def start_dozvon(source_chat_id: int, target_chat_id: int):
    source_chat_id = int(source_chat_id)
    target_chat_id = int(target_chat_id)
    session_key = (source_chat_id, target_chat_id)

    existing = _dozvon_sessions.get(session_key)
    if existing:
        existing["stop"] = True
        existing["stop_reason"] = "restart"
        time.sleep(0.1)

    sess = {
        "source_chat_id": source_chat_id,
        "target_chat_id": target_chat_id,
        "stop": False,
        "stop_reason": None,
    }
    _dozvon_sessions[session_key] = sess
    _dozvon_target_index[target_chat_id].add(session_key)

    send_and_auto_delete(source_chat_id, f"📞 Дозвон запущен: {get_chat_display_name(target_chat_id)}", HELPER_DELETE_DELAY)
    threading.Thread(target=_run_dozvon_session, args=(session_key,), daemon=True).start()



def build_forward_status_lines() -> list[str]:
    lines = []
    fr = data.get("forward_rules", {}) or {}
    ff = data.get("forward_finance", {}) or {}

    seen_pairs = set()

    def _sorted_pair(a: int, b: int):
        name_a = get_chat_display_name(a).lower()
        name_b = get_chat_display_name(b).lower()
        if (name_a, a) <= (name_b, b):
            return a, b
        return b, a

    all_ids = set()
    for src, dsts in fr.items():
        try:
            src_id = int(src)
        except Exception:
            continue
        for dst in (dsts or {}).keys():
            try:
                dst_id = int(dst)
            except Exception:
                continue
            all_ids.add((src_id, dst_id))

    for src_id, dst_id in sorted(all_ids, key=lambda p: (_sorted_pair(p[0], p[1])[0], _sorted_pair(p[0], p[1])[1])):
        left_id, right_id = _sorted_pair(src_id, dst_id)
        pair_key = (left_id, right_id)
        if pair_key in seen_pairs:
            continue
        seen_pairs.add(pair_key)

        ab_on = str(right_id) in (fr.get(str(left_id), {}) or {})
        ba_on = str(left_id) in (fr.get(str(right_id), {}) or {})

        if ab_on and ba_on:
            mail_dir = "↔️"
        elif ab_on:
            mail_dir = "➡️"
        elif ba_on:
            mail_dir = "⬅️"
        else:
            continue

        ab_fin = bool((ff.get(str(left_id), {}) or {}).get(str(right_id), False))
        ba_fin = bool((ff.get(str(right_id), {}) or {}).get(str(left_id), False))
        if ab_fin and ba_fin:
            fin_dir = "↔️"
        elif ab_fin:
            fin_dir = "➡️"
        elif ba_fin:
            fin_dir = "⬅️"
        else:
            fin_dir = "ВЫКЛ"

        fin_mode = "ВКЛ ✅" if (is_finance_mode(left_id) and is_finance_mode(right_id)) else "ВЫКЛ ❌"
        left_name = get_chat_display_name(left_id)
        right_name = get_chat_display_name(right_id)
        lines.append(f"• {left_name}-📨{mail_dir}-💰{fin_mode}-💸{fin_dir}-{right_name}")

    if not lines:
        lines.append("• Связи пересылки не настроены")

    return lines


def build_forward_status_text(title: str | None = None) -> str:
    lines = []
    if title:
        lines.append(title)
        lines.append("")
    lines.append("Текущие связи:")
    lines.extend(build_forward_status_lines())
    return "\n".join(lines)

def _find_forward_origin_by_copied_message(chat_id: int, msg_id: int):
    """
    Ищет origin (source_chat_id, source_msg_id) по копии сообщения в конкретном чате.
    Нужно для правильного reply, когда пользователь отвечает на сообщение,
    которое бот ранее переслал из другого чата.
    """
    try:
        for (src_chat_id, src_msg_id), pairs in forward_map.items():
            for pair_chat_id, pair_msg_id in pairs:
                if int(pair_chat_id) == int(chat_id) and int(pair_msg_id) == int(msg_id):
                    return int(src_chat_id), int(src_msg_id)
    except Exception:
        pass
    return None, None


def resolve_reply_target_message_id(source_chat_id: int, reply_to_message_id: int | None, dst_chat_id: int):
    """
    Возвращает message_id, к которому нужно привязать reply в целевом чате.

    Поддерживает оба сценария:
    1) reply на исходное сообщение текущего чата
    2) reply на сообщение, которое бот переслал сюда из другого чата
    """
    if not reply_to_message_id:
        return None

    source_chat_id = int(source_chat_id)
    dst_chat_id = int(dst_chat_id)
    reply_to_message_id = int(reply_to_message_id)

    # Сценарий 1: отвечают на оригинал в текущем чате.
    # Тогда в целевом чате нужен его mirror/copy.
    try:
        for link_dst_chat_id, link_dst_msg_id in get_forward_links(source_chat_id, reply_to_message_id):
            if int(link_dst_chat_id) == dst_chat_id:
                return int(link_dst_msg_id)
    except Exception:
        pass

    # Сценарий 2: отвечают на бот-копию, пришедшую из другого чата.
    # Тогда надо найти origin и:
    #   • если целевой чат = origin chat → reply на оригинал
    #   • если целевой чат другой → reply на соответствующую копию origin-сообщения
    try:
        origin_chat_id, origin_msg_id = _find_forward_origin_by_copied_message(source_chat_id, reply_to_message_id)
        if origin_chat_id is not None and origin_msg_id is not None:
            if dst_chat_id == int(origin_chat_id):
                return int(origin_msg_id)

            for link_dst_chat_id, link_dst_msg_id in get_forward_links(origin_chat_id, origin_msg_id):
                if int(link_dst_chat_id) == dst_chat_id:
                    return int(link_dst_msg_id)
    except Exception:
        pass

    return None


def _call_with_optional_reply(send_func, *args, reply_to_message_id=None, **kwargs):
    if reply_to_message_id:
        for extra in (
            {"reply_to_message_id": int(reply_to_message_id), "allow_sending_without_reply": True},
            {"reply_to_message_id": int(reply_to_message_id)},
            {},
        ):
            try:
                return send_func(*args, **kwargs, **extra)
            except TypeError:
                continue
    return send_func(*args, **kwargs)


def build_balance_panel_keyboard(chat_id: int):
    kb = types.InlineKeyboardMarkup()
    bal = get_chat_store(chat_id).get("balance", 0)
    kb.row(types.InlineKeyboardButton(
        f"🏦 Остаток по чату: {fmt_num(bal)}",
        callback_data="bp:open"
    ))
    return kb


def render_open_balance_panel_text(chat_id: int) -> str:
    bal = get_chat_store(chat_id).get("balance", 0)
    return f"🏦 Остаток по чату: {fmt_num(bal)}"


def build_balance_panel_open_keyboard(chat_id: int):
    kb = types.InlineKeyboardMarkup()
    kb.row(types.InlineKeyboardButton("🔽 Свернуть", callback_data="bp:collapse"))
    return kb


def _cancel_timer(timer_map: dict, key):
    prev = timer_map.get(key)
    if prev and getattr(prev, "is_alive", lambda: False)():
        try:
            prev.cancel()
        except Exception:
            pass


def clear_chat_active_windows(chat_id: int):
    data.setdefault("active_messages", {})[str(chat_id)] = {}
    save_data(data)


def bind_single_active_window(chat_id: int, day_key: str, message_id: int):
    data.setdefault("active_messages", {})[str(chat_id)] = {}
    set_active_window_id(chat_id, day_key, message_id)


def is_quick_balance_open_mode(chat_id: int) -> bool:
    return get_quick_balance_behavior(chat_id) == "open"


def is_quick_balance_main_open(chat_id: int, message_id: int | None = None) -> bool:
    if not is_quick_balance_enabled(chat_id) or not is_quick_balance_open_mode(chat_id):
        return False
    store = get_chat_store(chat_id)
    panel_id = store.get("balance_panel_id")
    if not panel_id or store.get("balance_panel_mode") != "open_main":
        return False
    if message_id is not None and int(panel_id) != int(message_id):
        return False
    return True


def render_quick_balance_main_window(chat_id: int, day_key: str | None = None):
    store = get_chat_store(chat_id)
    view_day = day_key or store.get("current_view_day", today_key())
    store["current_view_day"] = view_day
    txt, _ = render_day_window(chat_id, view_day)
    kb = build_main_keyboard(view_day, chat_id)
    return txt, kb, view_day


def collapse_balance_panel(chat_id: int):
    store = get_chat_store(chat_id)
    panel_id = store.get("balance_panel_id")
    if not panel_id:
        return

    try:
        bot.edit_message_text(
            "📌 Быстрый остаток",
            chat_id=chat_id,
            message_id=panel_id,
            reply_markup=build_balance_panel_keyboard(chat_id)
        )
        store["balance_panel_mode"] = "mini"
        save_data(data)
    except Exception as e:
        log_error(f"collapse_balance_panel({chat_id}): {e}")


def open_quick_balance_main_window(chat_id: int, message_id: int, day_key: str | None = None):
    if not is_finance_mode(chat_id) or not is_quick_balance_enabled(chat_id):
        return

    store = get_chat_store(chat_id)
    txt, kb, view_day = render_quick_balance_main_window(chat_id, day_key)

    try:
        bot.edit_message_text(
            txt,
            chat_id=chat_id,
            message_id=message_id,
            reply_markup=kb,
            parse_mode="HTML"
        )
        store["balance_panel_id"] = message_id
        store["balance_panel_mode"] = "open_main"
        store["current_view_day"] = view_day
        save_data(data)
        schedule_balance_panel_collapse(chat_id)
    except Exception as e:
        log_error(f"open_quick_balance_main_window({chat_id},{message_id}): {e}")


def schedule_balance_panel_collapse(chat_id: int, delay: float = BALANCE_PANEL_COLLAPSE_DELAY):
    def _job():
        try:
            collapse_balance_panel(chat_id)
        except Exception as e:
            log_error(f"schedule_balance_panel_collapse({chat_id}): {e}")

    store = get_chat_store(chat_id)
    key = store.get("balance_panel_id") or chat_id
    _cancel_timer(_balance_panel_collapse_timers, key)
    t = threading.Timer(delay, _job)
    _balance_panel_collapse_timers[key] = t
    t.start()


def send_minimized_balance_panel(chat_id: int):
    if not is_finance_mode(chat_id) or not is_quick_balance_enabled(chat_id):
        return

    store = get_chat_store(chat_id)
    panel_id = store.get("balance_panel_id")

    if panel_id:
        try:
            bot.edit_message_text(
                "📌 Быстрый остаток",
                chat_id=chat_id,
                message_id=panel_id,
                reply_markup=build_balance_panel_keyboard(chat_id)
            )
            store["balance_panel_mode"] = "mini"
            save_data(data)
            return
        except Exception as e:
            log_error(f"send_minimized_balance_panel edit({chat_id}): {e}")
            try:
                bot.delete_message(chat_id, panel_id)
            except Exception:
                pass
            store["balance_panel_id"] = None

    try:
        sent = bot.send_message(
            chat_id,
            "📌 Быстрый остаток",
            reply_markup=build_balance_panel_keyboard(chat_id)
        )
        store["balance_panel_id"] = sent.message_id
        store["balance_panel_mode"] = "mini"
        save_data(data)
    except Exception as e:
        log_error(f"send_minimized_balance_panel({chat_id}): {e}")


def refresh_balance_panel_now(chat_id: int):
    if not is_finance_mode(chat_id) or not is_quick_balance_enabled(chat_id):
        return

    store = get_chat_store(chat_id)
    panel_id = store.get("balance_panel_id")
    if not panel_id:
        return

    try:
        if is_quick_balance_main_open(chat_id):
            txt, kb, view_day = render_quick_balance_main_window(chat_id)
            bot.edit_message_text(
                txt,
                chat_id=chat_id,
                message_id=panel_id,
                reply_markup=kb,
                parse_mode="HTML"
            )
            store["current_view_day"] = view_day
            save_data(data)
            schedule_balance_panel_collapse(chat_id)
            return
    except Exception as e:
        log_error(f"refresh_balance_panel_now main({chat_id}): {e}")

    try:
        bot.edit_message_text(
            "📌 Быстрый остаток",
            chat_id=chat_id,
            message_id=panel_id,
            reply_markup=build_balance_panel_keyboard(chat_id)
        )
        store["balance_panel_mode"] = "mini"
        save_data(data)
    except Exception as e:
        log_error(f"refresh_balance_panel_now({chat_id}): {e}")
        send_minimized_balance_panel(chat_id)


def schedule_balance_panel_refresh(chat_id: int, delay: float = BALANCE_PANEL_REFRESH_DELAY):
    if not is_finance_mode(chat_id) or not is_quick_balance_enabled(chat_id):
        return

    def _job():
        try:
            store = get_chat_store(chat_id)
            if is_quick_balance_main_open(chat_id):
                refresh_balance_panel_now(chat_id)
            elif store.get("balance_panel_id"):
                refresh_balance_panel_now(chat_id)
            else:
                send_minimized_balance_panel(chat_id)
        except Exception as e:
            log_error(f"schedule_balance_panel_refresh({chat_id}): {e}")

    _cancel_timer(_balance_panel_refresh_timers, chat_id)
    t = threading.Timer(delay, _job)
    _balance_panel_refresh_timers[chat_id] = t
    t.start()


def open_balance_panel_in_message(chat_id: int, message_id: int, day_key: str | None = None):
    if not is_finance_mode(chat_id) or not is_quick_balance_enabled(chat_id):
        return

    if is_quick_balance_open_mode(chat_id):
        open_quick_balance_main_window(chat_id, message_id, day_key)
        return

    try:
        bot.edit_message_text(
            render_open_balance_panel_text(chat_id),
            chat_id=chat_id,
            message_id=message_id,
            reply_markup=build_balance_panel_open_keyboard(chat_id)
        )
        store = get_chat_store(chat_id)
        store["balance_panel_id"] = message_id
        store["balance_panel_mode"] = "open"
        save_data(data)
        schedule_balance_panel_collapse(chat_id)
    except Exception as e:
        log_error(f"open_balance_panel_in_message({chat_id},{message_id}): {e}")

def build_day_report_lines(chat_id: int) -> list[str]:
    """
    Красивый отчёт по дням:
    Дата    | Приход| Расход|Остаток
    Числовые колонки фиксированной ширины 7 символов.
    """
    store = get_chat_store(chat_id)
    daily = store.get("daily_records", {}) or {}

    lines = []
    lines.append("Отчёт:")
    lines.append(
        f"{'Дата':<8}|"
        f"{report_header_cell('Приход', 7)}|"
        f"{report_header_cell('Расход', 7)}|"
        f"{report_header_cell('Остаток', 7)}"
    )

    running_balance = 0.0

    for dk in sorted(daily.keys()):
        recs = daily.get(dk, []) or []

        expense = 0.0
        income = 0.0

        for r in recs:
            amt = float(r.get("amount", 0) or 0)
            if amt < 0:
                expense += abs(amt)
            else:
                income += amt

        running_balance += sum(float(r.get("amount", 0) or 0) for r in recs)

        date_txt = fmt_date_ddmmyy(dk)
        inc_txt = report_cell(income, 7)
        exp_txt = report_cell(expense, 7)
        bal_txt = report_cell(running_balance, 7)

        lines.append(f"{date_txt:<8}|{inc_txt}|{exp_txt}|{bal_txt}")

    return lines
def week_start_monday(day_key: str) -> str:
    """Возвращает YYYY-MM-DD (понедельник недели) для day_key"""
    try:
        d = datetime.strptime(day_key, "%Y-%m-%d").date()
    except Exception:
        d = now_local().date()
    start = d - timedelta(days=d.weekday())
    return start.strftime("%Y-%m-%d")

def week_bounds_from_start(start_key: str):
    """start_key (YYYY-MM-DD, понедельник) -> (start_key, end_key)"""
    try:
        s = datetime.strptime(start_key, "%Y-%m-%d").date()
    except Exception:
        s = now_local().date() - timedelta(days=now_local().date().weekday())
    e = s + timedelta(days=6)
    return s.strftime("%Y-%m-%d"), e.strftime("%Y-%m-%d")
    
def week_start_thursday(day_key: str) -> str:
    """
    Возвращает YYYY-MM-DD (четверг недели ЧТ–СР) для day_key
    """
    try:
        d = datetime.strptime(day_key, "%Y-%m-%d").date()
    except Exception:
        d = now_local().date()

    offset = (d.weekday() - 3) % 7
    start = d - timedelta(days=offset)
    return start.strftime("%Y-%m-%d")


def week_bounds_thu_wed(start_key: str):
    """
    start_key (четверг) -> (четверг, среда)
    """
    try:
        s = datetime.strptime(start_key, "%Y-%m-%d").date()
    except Exception:
        s = now_local().date()
    e = s + timedelta(days=6)
    return s.strftime("%Y-%m-%d"), e.strftime("%Y-%m-%d")
    
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
    meta = SQLITE.get_meta("csv_meta", "main", None)
    if isinstance(meta, dict):
        return meta
    legacy = _load_json(CSV_META_FILE, {})
    if isinstance(legacy, dict) and legacy:
        SQLITE.set_meta("csv_meta", "main", legacy)
    return legacy if isinstance(legacy, dict) else {}

def _save_csv_meta(meta: dict):
    try:
        SQLITE.set_meta("csv_meta", "main", meta or {})
        _save_json(CSV_META_FILE, meta or {})
        log_info("csv_meta updated in sqlite")
    except Exception as e:
        log_error(f"_save_csv_meta: {e}")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CHAT_BACKUP_META_FILE = os.path.join(BASE_DIR, "chat_backup_meta.json")
log_info(f"chat_backup_meta.json PATH = {CHAT_BACKUP_META_FILE}")
def _load_chat_backup_meta() -> dict:
    """Загрузка meta-файла бэкапов для всех чатов."""
    try:
        meta = SQLITE.get_meta("chat_backup_meta", "main", None)
        if isinstance(meta, dict):
            return meta
        if not os.path.exists(CHAT_BACKUP_META_FILE):
            return {}
        legacy = _load_json(CHAT_BACKUP_META_FILE, {})
        if isinstance(legacy, dict) and legacy:
            SQLITE.set_meta("chat_backup_meta", "main", legacy)
        return legacy if isinstance(legacy, dict) else {}
    except Exception as e:
        log_error(f"_load_chat_backup_meta: {e}")
        return {}

def _save_chat_backup_meta(meta: dict) -> None:
    """Сохранение meta-файла и sqlite-копии."""
    try:
        SQLITE.set_meta("chat_backup_meta", "main", meta or {})
        log_info(f"SAVING META TO: {os.path.abspath(CHAT_BACKUP_META_FILE)}")
        _save_json(CHAT_BACKUP_META_FILE, meta or {})
        log_info("chat_backup_meta updated in sqlite")
    except Exception as e:
        log_error(f"_save_chat_backup_meta: {e}")
def send_backup_to_chat(chat_id: int) -> None:
    """
    Универсальный авто-бэкап JSON прямо в чате.
    Работает одинаково для владельца, групп, каналов, всех чатов.
    Логика:
    • гарантируем актуальный data_<chat_id>.json
    • читаем meta-файл chat_backup_meta.json
    • если есть msg_id → edit_message_media()
    • если нет / не найдено → отправляем новое сообщение
    • обновляем meta-файл в рабочей директории (Render-friendly)
    • при смене дня (после 00:00) создаётся НОВОЕ сообщение с файлом
    """
    try:
        if not chat_id:
            return

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

        last_ts = meta.get(ts_key)
        msg_id = meta.get(msg_key)
        if msg_id and last_ts:
            try:
                prev_dt = datetime.fromisoformat(last_ts)
                if prev_dt.date() != now_local().date():
                    msg_id = None
            except Exception as e:
                log_error(f"send_backup_to_chat: bad timestamp for chat {chat_id}: {e}")

        def _open_file() -> io.BytesIO | None:
            """Чтение JSON в BytesIO с правильным именем файла."""
            try:
                with open(json_path, "rb") as f:
                    data_bytes = f.read()
            except Exception as e:
                log_error(f"send_backup_to_chat open({json_path}): {e}")
                return None

            if not data_bytes:
                return None

            base = os.path.basename(json_path)
            name_no_ext, dot, ext = base.partition(".")
            suffix = get_chat_name_for_filename(chat_id)
            file_name = suffix if suffix else name_no_ext
            if dot:
                file_name += f".{ext}"

            buf = io.BytesIO(data_bytes)
            buf.name = file_name
            return buf

        if msg_id:
            fobj = _open_file()
            if not fobj:
                return
            try:
                bot.edit_message_media(
                    chat_id=chat_id,
                    message_id=msg_id,
                    media=types.InputMediaDocument(
                        media=fobj,
                        caption=caption
                    )
                )
                log_info(f"Chat backup UPDATED in chat {chat_id}")
                meta[ts_key] = now_local().isoformat(timespec="seconds")
                _save_chat_backup_meta(meta)
                return
            except Exception as e:
                log_error(f"send_backup_to_chat edit FAILED in {chat_id}: {e}")
                msg_id = None  # упадём в отправку нового

        fobj = _open_file()
        if not fobj:
            return
        sent = bot.send_document(chat_id, fobj, caption=caption)
        meta[msg_key] = sent.message_id
        meta[ts_key] = now_local().isoformat(timespec="seconds")
        _save_chat_backup_meta(meta)
        log_info(f"Chat backup CREATED in chat {chat_id}")

    except Exception as e:
        log_error(f"send_backup_to_chat({chat_id}): {e}")
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
        "forward_finance": {},
        "forward_index": {},
    }
def load_data():
    _import_legacy_global_json_to_db(DATA_FILE, force=False)

    root = SQLITE.load_root()
    chats = SQLITE.load_chats()

    if root is None and not chats:
        d = default_data()
    else:
        d = _sqlite_unpack_data(root or {}, chats or {})

    base = default_data()
    for k, v in base.items():
        if k not in d:
            d[k] = v

    flags = d.get("backup_flags") or {}
    backup_flags["drive"] = bool(flags.get("drive", True))
    backup_flags["channel"] = bool(flags.get("channel", True))

    fac = d.get("finance_active_chats") or {}
    finance_active_chats.clear()
    for cid, enabled in fac.items():
        if enabled:
            try:
                finance_active_chats.add(int(cid))
            except Exception:
                pass

    if OWNER_ID:
        try:
            finance_active_chats.add(int(OWNER_ID))
        except Exception:
            pass

    try:
        _load_forward_index_from_data(d)
    except Exception as e:
        log_error(f"load_data forward_index: {e}")

    return d

def save_data(d):
    fac = {}
    for cid in finance_active_chats:
        fac[str(cid)] = True
    d["finance_active_chats"] = fac
    d["backup_flags"] = {
        "drive": bool(backup_flags.get("drive", True)),
        "channel": bool(backup_flags.get("channel", True)),
    }
    try:
        _persist_forward_index_in_data(d)
    except Exception as e:
        log_error(f"save_data forward_index: {e}")

    SQLITE.save_root(_sqlite_pack_root(d))
    SQLITE.save_chats(d.get("chats", {}) or {})
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
            "finance_mode": False,
            "settings": {
                "auto_add": True,
                "quick_balance_enabled": True,
                "quick_balance_behavior": "mini",
                "quick_balance_default_migrated": True
            },
        }
    )

    settings = store.setdefault("settings", {})
    settings.setdefault("auto_add", True)
    settings.setdefault("quick_balance_enabled", True)
    settings.setdefault("quick_balance_behavior", "mini")
    settings.setdefault("quick_balance_default_migrated", True)
    store.setdefault("finance_mode", False)

    if OWNER_ID and str(chat_id) == str(OWNER_ID):
        store["settings"]["auto_add"] = True
        store["finance_mode"] = True

    if "known_chats" not in store:
        store["known_chats"] = {}

    return store

def migrate_quick_balance_defaults():
    changed = False
    for _cid, store in (data.get("chats", {}) or {}).items():
        settings = store.setdefault("settings", {})
        if not settings.get("quick_balance_default_migrated"):
            settings["quick_balance_enabled"] = True
            settings["quick_balance_behavior"] = settings.get("quick_balance_behavior") or "mini"
            settings["quick_balance_default_migrated"] = True
            changed = True
        if "quick_balance_enabled" not in settings:
            settings["quick_balance_enabled"] = True
            changed = True
        if "quick_balance_behavior" not in settings:
            settings["quick_balance_behavior"] = "mini"
            changed = True
    if changed:
        save_data(data)
    return changed


def collect_forward_menu_chats() -> dict:
    """
    Собирает список чатов для меню пересылки:
    1) из known_chats владельца
    2) из data["chats"] как резерв
    """
    result = {}

    if OWNER_ID:
        try:
            owner_store = get_chat_store(int(OWNER_ID))
            known = owner_store.get("known_chats", {}) or {}
            for cid, info in known.items():
                result[str(cid)] = {
                    "title": info.get("title") or f"Чат {cid}",
                    "username": info.get("username"),
                    "type": info.get("type"),
                }
        except Exception as e:
            log_error(f"collect_forward_menu_chats known_chats: {e}")

    try:
        for cid, store in (data.get("chats", {}) or {}).items():
            if OWNER_ID and str(cid) == str(OWNER_ID):
                continue

            info = store.get("info", {}) or {}
            prev = result.get(str(cid), {})

            result[str(cid)] = {
                "title": info.get("title") or prev.get("title") or f"Чат {cid}",
                "username": info.get("username") or prev.get("username"),
                "type": info.get("type") or prev.get("type"),
            }
    except Exception as e:
        log_error(f"collect_forward_menu_chats data.chats: {e}")

    return result

def save_chat_json(chat_id: int):
    """
    Save per-chat JSON, CSV and META for one chat.
    """
    try:
        store = data.get("chats", {}).get(str(chat_id))
        if not store:
            store = get_chat_store(chat_id)
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
        with open(chat_path_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["date", "amount", "note"])  # Простой заголовок
            daily = store.get("daily_records", {})
            rows = []
            for dk in sorted(daily.keys()):
                recs = daily.get(dk, [])
                recs_sorted = sorted(recs, key=lambda r: r.get("timestamp", ""))
                for r in recs_sorted:
                    rows.append((
                        dk,
                        fmt_csv_amount(r.get("amount")),
                        r.get("note", "")
                    ))
            write_csv_rows_with_day_gaps(w, rows, 3)
        meta = {
            "last_saved": now_local().isoformat(timespec="seconds"),
            "record_count": sum(len(v) for v in store.get("daily_records", {}).values()),
        }
        _save_json(chat_path_meta, meta)
        log_info(f"Per-chat files saved for chat {chat_id}")
    except Exception as e:
        log_error(f"save_chat_json({chat_id}): {e}")
def persist_chat_state(chat_id: int):
    save_data(data)
    save_chat_json(chat_id)
    export_global_csv(data)        
def restore_from_json(chat_id: int, path: str):
    """
    Восстановление из JSON.
    Поддержка:
      1) data.json (глобальный) — если внутри есть ключ "chats"
      2) data_<chat_id>.json (пер-чат) — если внутри есть ключи "records"/"daily_records"
    """
    global data
    payload = _load_json(path, None)
    if not isinstance(payload, dict):
        raise RuntimeError("JSON повреждён или пустой")

    if "chats" in payload and isinstance(payload.get("chats"), dict):
        data = payload
        base = default_data()
        for k, v in base.items():
            if k not in data:
                data[k] = v

        finance_active_chats.clear()
        fac = data.get("finance_active_chats") or {}
        if isinstance(fac, dict):
            for cid, enabled in fac.items():
                if enabled:
                    try:
                        finance_active_chats.add(int(cid))
                    except Exception:
                        pass

        rebuild_global_records()
        save_data(data)

        for cid_str in list(data.get("chats", {}).keys()):
            try:
                save_chat_json(int(cid_str))
            except Exception as e:
                log_error(f"restore_from_json: save_chat_json({cid_str}) failed: {e}")

        export_global_csv(data)
        log_info("restore_from_json: global data restored")
        return

    if "records" in payload or "daily_records" in payload:
        store = get_chat_store(chat_id)

        store["records"] = payload.get("records", []) or []
        store["daily_records"] = payload.get("daily_records", {}) or {}
        store["next_id"] = int(payload.get("next_id", 1) or 1)
        store["info"] = payload.get("info", store.get("info", {})) or store.get("info", {})
        store["known_chats"] = payload.get("known_chats", store.get("known_chats", {})) or store.get("known_chats", {})

        if not store["records"] and store["daily_records"]:
            all_recs = []
            for dk in sorted(store["daily_records"].keys()):
                all_recs.extend(store["daily_records"][dk] or [])
            store["records"] = all_recs

        renumber_chat_records(chat_id)
        recalc_balance(chat_id)
        rebuild_global_records()

        save_data(data)
        save_chat_json(chat_id)
        export_global_csv(data)

        log_info(f"restore_from_json: chat {chat_id} restored from per-chat JSON")
        return

    raise RuntimeError("Неизвестный формат JSON (нет 'chats' и нет 'records/daily_records').")
def restore_from_csv(chat_id: int, path: str):
    """
    Восстановление из CSV (пер-чат).
    Ожидает колонки как у тебя в CSV:
    chat_id,ID,short_id,timestamp,amount,note,owner,day_key
    """
    store = get_chat_store(chat_id)

    daily = {}
    records = []

    with open(path, "r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            try:
                dk = (row.get("day_key") or today_key()).strip()
                amt = parse_csv_amount(row.get("amount") or 0)
                note = (row.get("note") or "").strip()
                owner = row.get("owner") or ""
                ts = (row.get("timestamp") or now_local().isoformat(timespec="seconds")).strip()

                rec = {
                    "id": int(row.get("ID") or 0) or 0,
                    "short_id": row.get("short_id") or "",
                    "timestamp": ts,
                    "amount": amt,
                    "note": note,
                    "owner": owner,
                }
                daily.setdefault(dk, []).append(rec)
                records.append(rec)
            except Exception as e:
                log_error(f"restore_from_csv row skip: {e}")

    store["daily_records"] = daily
    store["records"] = records

    renumber_chat_records(chat_id)
    recalc_balance(chat_id)
    rebuild_global_records()

    save_data(data)
    save_chat_json(chat_id)
    export_global_csv(data)

    log_info(f"restore_from_csv: chat {chat_id} restored from CSV")

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
num_re = re.compile(r"[+\-–]?\s*\d[\d\s.,_'’]*")
def fmt_num_plain(x):
    """
    Формат числа БЕЗ знака (+/-).
    Использовать только для отчётов по статьям расходов.
    """
    try:
        return fmt_num(x).lstrip("+-")
    except Exception:
        return str(x)
def parse_amount(raw: str) -> float:
    """
    Универсальный парсер:
    - понимает любые разделители
    - смешанные форматы (1.234,56 / 1,234.56)
    - определяет десятичную часть по самому правому разделителю
    - число без знака = расход
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
        amount (float)
        note (str)
    """
    m = num_re.search(text)
    if not m:
        raise ValueError("no number found")
    raw_number = m.group(0)
    amount = parse_amount(raw_number)
    note = text.replace(raw_number, " ").strip()
    note = re.sub(r"\s+", " ", note).lower()
    return amount, note


EXPENSE_CATEGORIES = {
    "ПРОДУКТЫ": ["продукты", "шб", "еда"],
    "ОРГТЕХНИКА": ["оргтех", "оргтехника"],
    "СВЯЗЬ": ["тел", "tel", "пополнение"],
    "АВТО": ["авто", "бензин", "билет"],
    "ПЕРЕВОДЫ": ["переводы", "перевод", "переводчик"],
}

EXPENSE_CATEGORY_SLUGS = {
    "ПРОДУКТЫ": "food",
    "ОРГТЕХНИКА": "org",
    "СВЯЗЬ": "link",
    "АВТО": "auto",
    "ПЕРЕВОДЫ": "transfers",
}
CATEGORY_BY_SLUG = {v: k for k, v in EXPENSE_CATEGORY_SLUGS.items()}
EXPENSE_CATEGORY_ORDER = [
    "ПРОДУКТЫ",
    "ОРГТЕХНИКА",
    "СВЯЗЬ",
    "АВТО",
    "ПЕРЕВОДЫ",
]

def resolve_expense_category(note: str):
    if not note:
        return None
    n = str(note).lower()
    for cat in EXPENSE_CATEGORY_ORDER:
        keywords = EXPENSE_CATEGORIES.get(cat, [])
        for kw in keywords:
            if kw in n:
                return cat
    return None

def calc_categories_for_period(store: dict, start: str, end: str) -> dict:
    """Считает суммы расходов по статьям (только отрицательные amount) в диапазоне дат включительно."""
    out = {}
    daily = store.get("daily_records", {}) or {}
    for day, records in daily.items():
        if not (start <= day <= end):
            continue
        for r in (records or []):
            amt = float(r.get("amount", 0) or 0)
            if amt >= 0:
                continue
            cat = resolve_expense_category(r.get("note", ""))
            if not cat:
                continue
            out[cat] = out.get(cat, 0) + (-amt)
    return out


def collect_items_for_category(store: dict, start: str, end: str, category: str):
    """Возвращает список (day, amount, note) для указанной статьи и периода."""
    items = []
    daily = store.get("daily_records", {}) or {}
    for day, records in daily.items():
        if not (start <= day <= end):
            continue
        for r in (records or []):
            amt = float(r.get("amount", 0) or 0)
            if amt >= 0:
                continue
            note = r.get("note", "")
            if resolve_expense_category(note) == category:
                items.append((day, -amt, note))
    return items


def get_ordered_category_names(include_all: bool = False, cats: dict | None = None):
    names = []
    seen = set()
    if include_all:
        for cat in EXPENSE_CATEGORY_ORDER:
            names.append(cat)
            seen.add(cat)
    elif cats:
        for cat in EXPENSE_CATEGORY_ORDER:
            if cat in cats:
                names.append(cat)
                seen.add(cat)
        for cat in sorted(cats.keys()):
            if cat not in seen:
                names.append(cat)
                seen.add(cat)
    return names


def summarize_categories(store: dict, start: str, end: str, label: str):
    cats = calc_categories_for_period(store, start, end)
    lines = [
        "📦 Расходы по статьям",
        f"🗓 {label}",
        ""
    ]
    if not cats:
        lines.append("Нет данных по статьям за этот период.")
    else:
        for cat in get_ordered_category_names(cats=cats):
            lines.append(f"{cat}: {fmt_num_plain(cats.get(cat, 0))}")
    return "\n".join(lines), cats

def build_categories_buttons(start: str, end: str):
    kb = types.InlineKeyboardMarkup(row_width=3)
    buttons = []
    for cat in get_ordered_category_names(include_all=True):
        slug = EXPENSE_CATEGORY_SLUGS.get(cat)
        if not slug:
            continue
        buttons.append(
            types.InlineKeyboardButton(
                cat,
                callback_data=f"cat_show:{start}:{end}:{slug}"
            )
        )

    for i in range(0, len(buttons), 3):
        kb.row(*buttons[i:i + 3])

    return kb


def build_categories_summary_keyboard(mode: str, start: str, end: str):
    kb = build_categories_buttons(start, end)

    if mode == "wthu":
        prev_key = (datetime.strptime(start, "%Y-%m-%d") - timedelta(days=7)).strftime("%Y-%m-%d")
        next_key = (datetime.strptime(start, "%Y-%m-%d") + timedelta(days=7)).strftime("%Y-%m-%d")
        row = [types.InlineKeyboardButton("⬅️ Чт–Ср", callback_data=f"cat_wthu:{prev_key}")]
        if start != week_start_thursday(today_key()):
            row.append(types.InlineKeyboardButton("📅 Сегодня", callback_data="cat_today"))
        row.append(types.InlineKeyboardButton("Чт–Ср ➡️", callback_data=f"cat_wthu:{next_key}"))
        kb.row(*row)
        kb.row(
            types.InlineKeyboardButton(
                "⬜ Пн–Вс",
                callback_data=f"cat_wk:{week_start_monday(start)}"
            ),
            types.InlineKeyboardButton("📆 Выбор недели", callback_data="cat_months")
        )
    elif mode == "wk":
        prev_key = (datetime.strptime(start, "%Y-%m-%d") - timedelta(days=7)).strftime("%Y-%m-%d")
        next_key = (datetime.strptime(start, "%Y-%m-%d") + timedelta(days=7)).strftime("%Y-%m-%d")
        row = [types.InlineKeyboardButton("⬅️ Пн–Вс", callback_data=f"cat_wk:{prev_key}")]
        if start != week_start_monday(today_key()):
            row.append(types.InlineKeyboardButton("📅 Сегодня", callback_data="cat_today"))
        row.append(types.InlineKeyboardButton("Пн–Вс ➡️", callback_data=f"cat_wk:{next_key}"))
        kb.row(*row)
        thu_ref = (datetime.strptime(start, "%Y-%m-%d") + timedelta(days=3)).strftime("%Y-%m-%d")
        kb.row(
            types.InlineKeyboardButton("🟦 Чт–Ср", callback_data=f"cat_wthu:{thu_ref}"),
            types.InlineKeyboardButton("📆 Выбор недели", callback_data="cat_months")
        )
    else:
        kb.row(
            types.InlineKeyboardButton("📅 Сегодня", callback_data="cat_today"),
            types.InlineKeyboardButton("📆 Выбор недели", callback_data="cat_months")
        )

    kb.row(types.InlineKeyboardButton("❌ Закрыть статьи", callback_data="cat_close"))
    return kb


def build_category_detail_text(store: dict, start: str, end: str, category: str, label: str):
    items = collect_items_for_category(store, start, end, category)
    lines = [
        f"📦 {category}",
        f"🗓 {label}",
        ""
    ]

    total = sum(amt for _, amt, _ in items)
    lines.append(f"Итого: {fmt_num_plain(total)}")
    lines.append("")

    if not items:
        lines.append("Нет операций по этой статье.")
    else:
        for day_i, amt_i, note_i in items:
            note_i = (note_i or "").strip()
            lines.append(f"• {fmt_date_ddmmyy(day_i)}: {fmt_num_plain(amt_i)} {note_i}".rstrip())

    return "\n".join(lines)

def build_category_detail_keyboard(start: str, end: str, back_callback: str, mode: str | None = None, slug: str | None = None):
    kb = build_categories_buttons(start, end)

    if mode == "wthu" and slug:
        prev_key = (datetime.strptime(start, "%Y-%m-%d") - timedelta(days=7)).strftime("%Y-%m-%d")
        next_key = (datetime.strptime(start, "%Y-%m-%d") + timedelta(days=7)).strftime("%Y-%m-%d")
        row = [types.InlineKeyboardButton("⬅️ Чт–Ср", callback_data=f"cat_show_wthu:{prev_key}:{slug}")]
        if start != week_start_thursday(today_key()):
            row.append(types.InlineKeyboardButton("📅 Сегодня", callback_data=f"cat_show_wthu:{today_key()}:{slug}"))
        row.append(types.InlineKeyboardButton("Чт–Ср ➡️", callback_data=f"cat_show_wthu:{next_key}:{slug}"))
        kb.row(*row)
    elif mode == "wk" and slug:
        prev_key = (datetime.strptime(start, "%Y-%m-%d") - timedelta(days=7)).strftime("%Y-%m-%d")
        next_key = (datetime.strptime(start, "%Y-%m-%d") + timedelta(days=7)).strftime("%Y-%m-%d")
        row = [types.InlineKeyboardButton("⬅️ Пн–Вс", callback_data=f"cat_show_wk:{prev_key}:{slug}")]
        if start != week_start_monday(today_key()):
            row.append(types.InlineKeyboardButton("📅 Сегодня", callback_data=f"cat_show_wk:{today_key()}:{slug}"))
        row.append(types.InlineKeyboardButton("Пн–Вс ➡️", callback_data=f"cat_show_wk:{next_key}:{slug}"))
        kb.row(*row)

    kb.row(types.InlineKeyboardButton("🔙 Назад", callback_data=back_callback))
    kb.row(types.InlineKeyboardButton("❌ Закрыть статьи", callback_data="cat_close"))
    return kb

def looks_like_amount(text):
    try:
        amount, note = split_amount_and_note(text)
        return True
    except:
        return False
@bot.message_handler(
    func=lambda m: not (m.text and m.text.startswith("/")),
    content_types=[
        "text", "photo", "video", "animation",
        "audio", "voice", "video_note",
        "sticker", "location", "venue", "contact",
        "dice", "poll"
    ]
)
def on_any_message(msg):
    chat_id = msg.chat.id

    if OWNER_ID and str(chat_id) == str(OWNER_ID):
        finance_active_chats.add(chat_id)

    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass

    if msg.content_type == "text":
        try:
            store = get_chat_store(chat_id)
            if store.get("reset_wait"):
                text_up = (msg.text or "").strip().upper()
                if text_up == "ДА":
                    store["reset_wait"] = False
                    store["reset_time"] = 0
                    save_data(data)
                    delete_stored_window_if_exists(chat_id, "reset_prompt_msg_id")
                    cleanup_forward_links(chat_id)
                    reset_chat_data(chat_id)
                    send_and_auto_delete(chat_id, "✅ Данные чата обнулены.", 10)
                    try:
                        bot.delete_message(chat_id, msg.message_id)
                    except Exception:
                        pass
                    return
        except Exception as e:
            log_error(f"reset_wait handler error: {e}")

    if msg.content_type == "text":
        try:
            store = get_chat_store(chat_id)
            wait = store.get("finance_toggle_wait")
            if wait:
                text_up = (msg.text or "").strip().upper()
                if text_up == "ДА":
                    target_chat_id = int(wait.get("target_chat_id"))
                    set_finance_mode(target_chat_id, not is_finance_mode(target_chat_id))
                    store["finance_toggle_wait"] = None
                    save_data(data)
                    send_and_auto_delete(
                        chat_id,
                        f"💰 Финансовый режим для {get_chat_display_name(target_chat_id)}: {format_finance_mode_label(target_chat_id)}",
                        10
                    )
                    try:
                        bot.delete_message(chat_id, msg.message_id)
                    except Exception:
                        pass
                    return
                elif text_up in {"НЕТ", "ОТМЕНА", "CANCEL"}:
                    store["finance_toggle_wait"] = None
                    save_data(data)
                    send_and_auto_delete(chat_id, "❎ Переключение финансового режима отменено.", 8)
                    try:
                        bot.delete_message(chat_id, msg.message_id)
                    except Exception:
                        pass
                    return
        except Exception as e:
            log_error(f"finance_toggle_wait handler error: {e}")

    if restore_mode is not None and restore_mode == chat_id:
        return
    if msg.content_type == "text":
        try:
            store = get_chat_store(chat_id)
            edit_wait = store.get("edit_wait")

            if edit_wait and edit_wait.get("type") == "edit":
                text = (msg.text or "").strip()
                if not text:
                    return

                try:
                    amount, note = split_amount_and_note(text)
                except Exception:
                    send_and_auto_delete(
                        chat_id,
                        "❌ Неверный формат.\nПример: 1500 продукты",
                        10
                    )
                    return

                rid = edit_wait.get("rid")
                day_key = edit_wait.get("day_key") or store.get("current_view_day") or today_key()

                target = next(
                    (r for r in store.get("records", []) if r.get("id") == rid),
                    None
                )

                if not target:
                    store["edit_wait"] = None
                    save_data(data)
                    delete_stored_window_if_exists(chat_id, "edit_prompt_msg_id")
                    send_and_auto_delete(chat_id, "❌ Запись для редактирования не найдена.", 10)
                    return

                target["amount"] = amount
                target["note"] = note

                for dk, arr in store.get("daily_records", {}).items():
                    for r in arr:
                        if r.get("id") == rid:
                            r["amount"] = amount
                            r["note"] = note

                store["balance"] = sum(r["amount"] for r in store.get("records", []))
                store["edit_wait"] = None
                save_data(data)
                delete_stored_window_if_exists(chat_id, "edit_prompt_msg_id")

                update_or_send_day_window(chat_id, day_key)
                send_and_auto_delete(
                    chat_id,
                    f"✅ Запись R{rid} обновлена: {fmt_num(amount)} {note}",
                    10
                )
                try:
                    bot.delete_message(chat_id, msg.message_id)
                except Exception:
                    pass
                return

        except Exception as e:
            log_error(f"edit_wait handler error: {e}")
    if msg.content_type == "text":
        try:
            if is_finance_mode(chat_id):
                handle_finance_text(msg)
        except Exception as e:
            log_error(f"handle_finance_text error: {e}")

    forward_any_message(chat_id, msg)
def handle_finance_text(msg):
    """
    Обработка обычного текстового ввода:
    - авто-добавление
    """

    if msg.content_type != "text":
        return

    chat_id = msg.chat.id
    text = (msg.text or "").strip()
    if not text:
        return
    if not is_finance_mode(chat_id):
        return

    store = get_chat_store(chat_id)
    settings = store.get("settings", {})

    if settings.get("auto_add", True) and looks_like_amount(text):
        try:
            amount, note = split_amount_and_note(text)
        except Exception:
            return

        entry_day = day_key_from_message(msg)

        add_record_to_chat(
            chat_id,
            amount,
            note,
            msg.from_user.id,
            source_msg=msg,
            day_key=entry_day
        )
        schedule_finalize(chat_id, entry_day)
        return

def handle_finance_edit(msg):
    chat_id = msg.chat.id
    text = (msg.text or msg.caption or "").strip()

    store = get_chat_store(chat_id)
    target = None

    for r in store.get("records", []):
        if (
            r.get("source_msg_id") == msg.message_id
            or r.get("origin_msg_id") == msg.message_id
            or r.get("msg_id") == msg.message_id
        ):
            target = r
            break

    if not target:
        log_info(f"[EDIT-FIN] record not found for msg_id={msg.message_id}")
        return False

    if text and looks_like_amount(text):
        try:
            amount, note = split_amount_and_note(text)
        except Exception:
            amount, note = 0, "удалено"
    else:
        amount, note = 0, "удалено"

    target["amount"] = amount
    target["note"] = note

    for day, arr in store.get("daily_records", {}).items():
        for r in arr:
            if r.get("id") == target.get("id"):
                r.update(target)

    store["balance"] = sum(r["amount"] for r in store.get("records", []))

    log_info(
        f"[EDIT-FIN] updated record R{target['id']} "
        f"amount={amount} note={note}"
    )
    day_key = target.get("day_key") or today_key()
    update_or_send_day_window(chat_id, day_key)
    return True
def sync_forwarded_finance_message(dst_chat_id: int, dst_msg_id: int, text: str, owner: int = 0):
    if not is_finance_mode(dst_chat_id):
        return

    store = get_chat_store(dst_chat_id)
    existing = None

    for r in store.get("records", []):
        if (
            r.get("source_msg_id") == dst_msg_id
            or r.get("origin_msg_id") == dst_msg_id
            or r.get("msg_id") == dst_msg_id
        ):
            existing = r
            break

    entry_day = today_key()

    if text and looks_like_amount(text):
        try:
            amount, note = split_amount_and_note(text)
        except Exception:
            return

        if existing:
            existing["amount"] = amount
            existing["note"] = note
            entry_day = existing.get("day_key") or entry_day
            rebuild_month_short_ids(dst_chat_id)
            rebuild_global_records()
            store["balance"] = sum(r.get("amount", 0) for r in store.get("records", []))
        else:
            shadow_msg = type("ForwardShadowMsg", (), {"message_id": dst_msg_id})()
            add_record_to_chat(
                dst_chat_id,
                amount,
                note,
                owner,
                source_msg=shadow_msg,
                day_key=entry_day
            )
    elif existing:
        existing["amount"] = 0
        existing["note"] = "удалено"
        entry_day = existing.get("day_key") or entry_day
        rebuild_month_short_ids(dst_chat_id)
        rebuild_global_records()
        store["balance"] = sum(r.get("amount", 0) for r in store.get("records", []))
    else:
        return

    schedule_finalize(dst_chat_id, entry_day)

def export_global_csv(d: dict):
    """Legacy global CSV with all chats (for backup channel)."""
    try:
        with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["date", "amount", "note"])  # Простой заголовок
            rows = []
            for cid, cdata in d.get("chats", {}).items():
                for dk, records in cdata.get("daily_records", {}).items():
                    for r in records:
                        rows.append((
                            dk,
                            fmt_csv_amount(r.get("amount")),
                            r.get("note", "")
                        ))
            rows.sort(key=lambda row: str(row[0]))
            write_csv_rows_with_day_gaps(w, rows, 3)
    except Exception as e:
        log_error(f"export_global_csv: {e}")
EMOJI_DIGITS = {
    "0": "0️⃣",
    "1": "1️⃣",
    "2": "2️⃣",
    "3": "3️⃣",
    "4": "4️⃣",
    "5": "5️⃣",
    "6": "6️⃣",
    "7": "7️⃣",
    "8": "8️⃣",
    "9": "9️⃣",
}
backup_channel_notified_chats = set()
def format_chat_id_emoji(chat_id: int) -> str:
    """Преобразует числовой chat_id в строку из emoji-цифр."""
    return "".join(EMOJI_DIGITS.get(ch, ch) for ch in str(chat_id))
def _safe_chat_title_for_filename(title) -> str:
    """Делает короткое безопасное имя чата для имени файла."""
    if not title:
        return ""
    title = str(title).strip()
    title = title.replace(" ", "_")
    title = re.sub(r"[^0-9A-Za-zА-Яа-я_\-]+", "", title)
    return title[:32]
def get_chat_name_for_filename(chat_id: int) -> str:
    """
    Выбор имени для файла:
        1) username
        2) title (имя чата)
        3) chat_id
    Всё преобразуется в короткое безопасное имя.
    """
    try:
        store = get_chat_store(chat_id)
        info = store.get("info", {})
        username = info.get("username")
        title = info.get("title")
        if username:
            base = username.lstrip("@")
        elif title:
            base = title
        else:
            base = str(chat_id)
        return _safe_chat_title_for_filename(base)
    except Exception as e:
        log_error(f"get_chat_name_for_filename({chat_id}): {e}")
        return _safe_chat_title_for_filename(str(chat_id))
def _get_chat_title_for_backup(chat_id: int) -> str:
    """Пытается достать название чата из store["info"]["title"]"""
    try:
        store = data.get("chats", {}).get(str(chat_id), {}) if isinstance(data, dict) else {}
        info = store.get("info", {})
        title = info.get("title")
        if title:
            return title
    except Exception as e:
        log_error(f"_get_chat_title_for_backup({chat_id}): {e}")
    return f"chat_{chat_id}"
def _get_chat_title_for_backup(chat_id: int) -> str:
    """
    Берём название чата из store["info"], чтобы подписывать бэкап.
    """
    try:
        store = get_chat_store(chat_id)
        info = store.get("info", {})
        title = info.get("title")
        if title:
            return title
    except Exception as e:
        log_error(f"_get_chat_title_for_backup({chat_id}): {e}")
    return f"chat_{chat_id}"
def send_backup_to_channel_for_file(base_path: str, meta_key_prefix: str, chat_title: str = None):
    """Helper to send or update a file in BACKUP_CHAT_ID with csv_meta tracking.
    Правило:
    • edit → если не удалось → send
    • если сообщение удалено вручную — файл создаётся заново
    """
    if not BACKUP_CHAT_ID:
        return
    if not os.path.exists(base_path):
        log_error(f"send_backup_to_channel_for_file: {base_path} not found")
        return

    try:
        meta = _load_csv_meta()
        msg_key = f"msg_{meta_key_prefix}"
        ts_key = f"timestamp_{meta_key_prefix}"

        base_name = os.path.basename(base_path)
        name_without_ext, dot, ext = base_name.partition(".")
        safe_title = _safe_chat_title_for_filename(chat_title)

        if safe_title:
            file_name = safe_title + (f".{ext}" if dot else "")
        else:
            file_name = base_name

        caption = f"📦 {file_name} — {now_local().strftime('%Y-%m-%d %H:%M')}"

        def _open_for_telegram() -> io.BytesIO | None:
            if not os.path.exists(base_path):
                return None
            with open(base_path, "rb") as src:
                data_bytes = src.read()
            if not data_bytes:
                log_error(f"send_backup_to_channel_for_file: {base_path} is empty, skip")
                return None
            buf = io.BytesIO(data_bytes)
            buf.name = file_name
            buf.seek(0)
            return buf

        sent = False

        if meta.get(msg_key):
            try:
                fobj = _open_for_telegram()
                if not fobj:
                    return
                bot.edit_message_media(
                    chat_id=int(BACKUP_CHAT_ID),
                    message_id=meta[msg_key],
                    media=types.InputMediaDocument(
                        media=fobj,
                        caption=caption
                    )
                )
                sent = True
                log_info(f"[BACKUP] channel file updated: {base_path}")
            except Exception as e:
                log_error(f"[BACKUP] edit failed, will resend: {e}")

        if not sent:
            fobj = _open_for_telegram()
            if not fobj:
                return
            sent_msg = bot.send_document(
                int(BACKUP_CHAT_ID),
                fobj,
                caption=caption
            )
            meta[msg_key] = sent_msg.message_id
            log_info(f"[BACKUP] channel file sent new: {base_path}")

        meta[ts_key] = now_local().isoformat(timespec="seconds")
        _save_csv_meta(meta)

    except Exception as e:
        log_error(f"send_backup_to_channel_for_file({base_path}): {e}")
def send_backup_to_channel(chat_id: int):
    """
    Общий бэкап файлов чата в BACKUP_CHAT_ID.
    Делает:
    • проверку флага backup_flags["channel"]
    • один раз (на первый бэкап чата) отправляет chat_id эмодзи в канал
    • обновляет/создаёт:
        - data_<chat_id>.json
        - data_<chat_id>.csv
    """
    try:
        if not BACKUP_CHAT_ID:
            return
        if not backup_flags.get("channel", True):
            log_info("send_backup_to_channel: channel backup disabled by flag.")
            return
        try:
            backup_chat_id = int(BACKUP_CHAT_ID)
        except Exception:
            log_error("send_backup_to_channel: BACKUP_CHAT_ID не является числом.")
            return
        save_chat_json(chat_id)
        export_global_csv(data)
        save_data(data)
        chat_title = _get_chat_title_for_backup(chat_id)
        if chat_id not in backup_channel_notified_chats:
            try:
                emoji_id = format_chat_id_emoji(chat_id)
                bot.send_message(backup_chat_id, emoji_id)
                backup_channel_notified_chats.add(chat_id)
            except Exception as e:
                log_error(
                    f"send_backup_to_channel: не удалось отправить emoji chat_id "
                    f"в канал: {e}"
                )
        json_path = chat_json_file(chat_id)
        csv_path = chat_csv_file(chat_id)
        send_backup_to_channel_for_file(json_path, f"json_{chat_id}", chat_title)
        send_backup_to_channel_for_file(csv_path, f"csv_{chat_id}", chat_title)
    except Exception as e:
        log_error(f"send_backup_to_channel({chat_id}): {e}")
def _owner_data_file() -> str | None:
    """Legacy JSON snapshot file for owner-compatible backups."""
    if not OWNER_ID:
        return None
    try:
        return f"data_{int(OWNER_ID)}.json"
    except Exception:
        return None

def load_forward_rules():
    """
    Загружает forward_rules/forward_finance из SQLite,
    а если их там ещё нет — пытается импортировать из legacy owner JSON.
    """
    try:
        fr = data.get("forward_rules", {}) or {}
        ff = data.get("forward_finance", {}) or {}
        if fr or ff:
            data["forward_finance"] = ff if isinstance(ff, dict) else {}
            return fr if isinstance(fr, dict) else {}

        path = _owner_data_file()
        if not path or not os.path.exists(path):
            data["forward_finance"] = {}
            return {}

        payload = _load_json(path, {}) or {}
        raw_fr = payload.get("forward_rules", {})
        upgraded = {}

        for src, value in raw_fr.items():
            if isinstance(value, list):
                upgraded[src] = {}
                for dst in value:
                    upgraded[src][dst] = "oneway_to"
            elif isinstance(value, dict):
                upgraded[src] = value

        ff = payload.get("forward_finance", {})
        if not isinstance(ff, dict):
            ff = {}

        data["forward_finance"] = ff
        data["forward_rules"] = upgraded
        save_data(data)
        return upgraded

    except Exception as e:
        log_error(f"load_forward_rules: {e}")
        data["forward_finance"] = {}
        return {}

def persist_forward_rules_to_owner():
    """
    Сохраняет forward_rules/forward_finance в SQLite
    и дополнительно пишет legacy owner JSON-снимок для совместимости.
    """
    try:
        save_data(data)
        path = _owner_data_file()
        if path:
            payload = _load_json(path, {}) or {}
            if not isinstance(payload, dict):
                payload = {}
            payload["forward_rules"] = data.get("forward_rules", {})
            payload["forward_finance"] = data.get("forward_finance", {})
            _save_json(path, payload)
            log_info(f"forward_rules snapshot persisted to {path}")
    except Exception as e:
        log_error(f"persist_forward_rules_to_owner: {e}")
        
def resolve_forward_targets(source_chat_id: int):
    fr = data.get("forward_rules", {})
    ff = data.get("forward_finance", {})
    src = str(source_chat_id)

    if src not in fr:
        return []

    out = []
    for dst, mode in fr[src].items():
        try:
            out.append((
                int(dst),
                mode,
                bool(ff.get(src, {}).get(dst, False))
            ))
        except Exception:
            continue

    return out
def add_forward_link(src_chat_id: int, dst_chat_id: int, mode: str):
    fr = data.setdefault("forward_rules", {})
    src = str(src_chat_id)
    dst = str(dst_chat_id)

    fr.setdefault(src, {})[dst] = mode

    persist_forward_rules_to_owner()
    save_data(data)

def remove_forward_link(src_chat_id: int, dst_chat_id: int):
    fr = data.get("forward_rules", {})
    src = str(src_chat_id)
    dst = str(dst_chat_id)

    if src in fr and dst in fr[src]:
        del fr[src][dst]
    if src in fr and not fr[src]:
        del fr[src]

    remove_forward_finance(src_chat_id, dst_chat_id)
    persist_forward_rules_to_owner()
    save_data(data)
def clear_forward_all():
    """Полностью отключает всю пересылку."""
    data["forward_rules"] = {}
    data["forward_finance"] = {}
    persist_forward_rules_to_owner()
    save_data(data)

def get_forward_finance(src_chat_id: int, dst_chat_id: int) -> bool:
    ff = data.setdefault("forward_finance", {})
    return bool(ff.get(str(src_chat_id), {}).get(str(dst_chat_id), False))

def set_forward_finance(src_chat_id: int, dst_chat_id: int, enabled: bool):
    ff = data.setdefault("forward_finance", {})
    src = str(src_chat_id)
    dst = str(dst_chat_id)

    ff.setdefault(src, {})[dst] = bool(enabled)

    persist_forward_rules_to_owner()
    save_data(data)

def remove_forward_finance(src_chat_id: int, dst_chat_id: int):
    ff = data.setdefault("forward_finance", {})
    src = str(src_chat_id)
    dst = str(dst_chat_id)

    if src in ff and dst in ff[src]:
        del ff[src][dst]
    if src in ff and not ff[src]:
        del ff[src]

    persist_forward_rules_to_owner()
    save_data(data)


def _forward_key(src_chat_id: int, src_msg_id: int) -> str:
    return f"{int(src_chat_id)}:{int(src_msg_id)}"


def _schedule_persist_forward_state(delay: float = 1.2):
    global _forward_state_timer

    def _job():
        try:
            save_data(data)
        except Exception as e:
            log_error(f"_schedule_persist_forward_state: {e}")

    prev = _forward_state_timer
    if prev and prev.is_alive():
        try:
            prev.cancel()
        except Exception:
            pass

    t = threading.Timer(delay, _job)
    _forward_state_timer = t
    t.start()


def _persist_forward_index_in_data(d: dict):
    idx = {}
    for (src_chat_id, src_msg_id), pairs in forward_map.items():
        rows = []
        for pair in pairs:
            if not isinstance(pair, (list, tuple)) or len(pair) < 2:
                continue
            dst_chat_id, dst_msg_id = pair[0], pair[1]
            rows.append({
                "dst_chat_id": int(dst_chat_id),
                "dst_msg_id": int(dst_msg_id),
                "status": "delivered",
            })
        if rows:
            idx[_forward_key(src_chat_id, src_msg_id)] = rows
    d["forward_index"] = idx


def _load_forward_index_from_data(d: dict):
    forward_map.clear()
    idx = d.get("forward_index", {}) or {}
    for key, rows in idx.items():
        try:
            src_chat_id_s, src_msg_id_s = str(key).split(":", 1)
            src_chat_id = int(src_chat_id_s)
            src_msg_id = int(src_msg_id_s)
        except Exception:
            continue

        pairs = []
        for row in rows or []:
            try:
                dst_chat_id = int(row.get("dst_chat_id"))
                dst_msg_id = int(row.get("dst_msg_id"))
                pairs.append((dst_chat_id, dst_msg_id))
            except Exception:
                continue

        if pairs:
            forward_map[(src_chat_id, src_msg_id)] = pairs


def _store_forward_link(src_chat_id: int, src_msg_id: int, dst_chat_id: int, dst_msg_id: int):
    key = (int(src_chat_id), int(src_msg_id))
    pair = (int(dst_chat_id), int(dst_msg_id))
    items = forward_map.setdefault(key, [])
    if pair not in items:
        items.append(pair)
    _schedule_persist_forward_state()


def get_forward_links(src_chat_id: int, src_msg_id: int):
    return list(forward_map.get((int(src_chat_id), int(src_msg_id)), []))


def delete_forward_copies_for_source(src_chat_id: int, src_msg_id: int):
    key = (int(src_chat_id), int(src_msg_id))
    links = list(forward_map.get(key, []))
    for dst_chat_id, dst_msg_id in links:
        try:
            bot.delete_message(dst_chat_id, dst_msg_id)
        except Exception as e:
            log_error(f"delete_forward_copies_for_source {src_chat_id}:{src_msg_id} -> {dst_chat_id}:{dst_msg_id}: {e}")
        try:
            delete_forwarded_finance_record_by_msg_id(dst_chat_id, dst_msg_id)
        except Exception as e:
            log_error(f"delete_forwarded_finance_record_by_msg_id {dst_chat_id}:{dst_msg_id}: {e}")
    if key in forward_map:
        del forward_map[key]
        _schedule_persist_forward_state()


def is_forward_delete_command(text: str) -> bool:
    t = (text or "").strip().lower()
    return t in ("/del", "/дел", "/д")


def find_record_by_message_id(chat_id: int, msg_id: int):
    store = get_chat_store(chat_id)
    for r in store.get("records", []):
        if (
            r.get("source_msg_id") == msg_id
            or r.get("origin_msg_id") == msg_id
            or r.get("msg_id") == msg_id
        ):
            return r
    return None


def delete_forwarded_finance_record_by_msg_id(chat_id: int, msg_id: int) -> bool:
    rec = find_record_by_message_id(chat_id, msg_id)
    if not rec:
        return False
    day_key = rec.get("day_key") or today_key()
    delete_record_in_chat(chat_id, rec["id"])
    schedule_finalize(chat_id, day_key)
    return True


def rebind_forwarded_finance_record(chat_id: int, old_msg_id: int, new_msg_id: int, text: str, owner: int = 0):
    store = get_chat_store(chat_id)
    rec = find_record_by_message_id(chat_id, old_msg_id)
    if rec:
        rec["source_msg_id"] = new_msg_id
        rec["origin_msg_id"] = new_msg_id
        rec["msg_id"] = new_msg_id

        if text and looks_like_amount(text):
            try:
                amount, note = split_amount_and_note(text)
                rec["amount"] = amount
                rec["note"] = note
            except Exception:
                pass

        rec_id = rec.get("id")
        for day, arr in store.get("daily_records", {}).items():
            for item in arr:
                if item.get("id") == rec_id:
                    item.update(rec)

        store["balance"] = sum(r.get("amount", 0) for r in store.get("records", []))
        rebuild_month_short_ids(chat_id)
        rebuild_global_records()
        schedule_finalize(chat_id, rec.get("day_key") or today_key())
        return True

    if text and looks_like_amount(text):
        sync_forwarded_finance_message(chat_id, new_msg_id, text, owner)
        return True

    return False


def _replace_forward_link_pair(src_chat_id: int, src_msg_id: int, old_dst_chat_id: int, old_dst_msg_id: int, new_dst_chat_id: int, new_dst_msg_id: int):
    key = (int(src_chat_id), int(src_msg_id))
    pairs = list(forward_map.get(key, []))
    updated = []
    replaced = False
    for pair in pairs:
        if int(pair[0]) == int(old_dst_chat_id) and int(pair[1]) == int(old_dst_msg_id):
            updated.append((int(new_dst_chat_id), int(new_dst_msg_id)))
            replaced = True
        else:
            updated.append(pair)
    if not replaced:
        updated.append((int(new_dst_chat_id), int(new_dst_msg_id)))
    forward_map[key] = updated
    _schedule_persist_forward_state()


def sync_edited_copy_to_target(source_chat_id: int, msg, dst_chat_id: int, dst_msg_id: int, finance_enabled: bool):
    text = _message_text_for_finance(msg)
    ct = getattr(msg, "content_type", None)
    owner_id = msg.from_user.id if getattr(msg, "from_user", None) else 0

    try:
        if ct == "text":
            try:
                bot.edit_message_text(text, chat_id=dst_chat_id, message_id=dst_msg_id)
            except Exception as e:
                err = str(e).lower()
                if "message is not modified" not in err:
                    raise
        elif ct in ("photo", "video", "document", "audio", "animation"):
            media = _build_input_media_from_message(msg)
            if not media:
                raise RuntimeError(f"Unsupported edited media content_type={ct}")
            try:
                bot.edit_message_media(media=media, chat_id=dst_chat_id, message_id=dst_msg_id)
            except Exception as e:
                err = str(e).lower()
                if "message is not modified" not in err:
                    raise
        elif getattr(msg, "caption", None):
            try:
                bot.edit_message_caption(caption=msg.caption, chat_id=dst_chat_id, message_id=dst_msg_id)
            except Exception as e:
                err = str(e).lower()
                if "message is not modified" not in err:
                    raise
        else:
            raise RuntimeError(f"Edited sync unsupported for content_type={ct}")

        if finance_enabled and text and is_finance_mode(dst_chat_id):
            sync_forwarded_finance_message(dst_chat_id, dst_msg_id, text, owner_id)
        return dst_msg_id

    except Exception as e:
        log_error(f"sync_edited_copy_to_target direct edit failed {dst_chat_id}:{dst_msg_id}: {e}")

    reply_to_target_id = None
    try:
        reply_to_msg = getattr(msg, "reply_to_message", None)
        if reply_to_msg is not None:
            reply_to_target_id = resolve_reply_target_message_id(
                source_chat_id,
                getattr(reply_to_msg, "message_id", None),
                dst_chat_id
            )
    except Exception:
        pass

    try:
        try:
            bot.delete_message(dst_chat_id, dst_msg_id)
        except Exception:
            pass

        sent_msg = _fallback_send_single(dst_chat_id, msg, reply_to_message_id=reply_to_target_id)
        new_dst_msg_id = sent_msg.message_id
        _replace_forward_link_pair(source_chat_id, msg.message_id, dst_chat_id, dst_msg_id, dst_chat_id, new_dst_msg_id)

        if finance_enabled and is_finance_mode(dst_chat_id):
            rebind_forwarded_finance_record(dst_chat_id, dst_msg_id, new_dst_msg_id, text, owner_id)

        return new_dst_msg_id
    except Exception as e:
        _notify_forward_failure(source_chat_id, msg.message_id, dst_chat_id, e)
        return None


def _cleanup_forward_storage_for_chat(chat_id: int):
    chat_id = int(chat_id)
    for key in list(forward_map.keys()):
        src_chat_id, _ = key
        if src_chat_id == chat_id:
            del forward_map[key]
            continue
        pairs = [pair for pair in forward_map.get(key, []) if int(pair[0]) != chat_id]
        if pairs:
            forward_map[key] = pairs
        elif key in forward_map:
            del forward_map[key]
    _schedule_persist_forward_state()


def _notify_forward_failure(source_chat_id: int, msg_id: int, dst_chat_id: int, err: Exception):
    text = (
        f"⚠️ Пересылка не доставлена\n"
        f"из {source_chat_id}:{msg_id}\n"
        f"в {dst_chat_id}\n"
        f"{err}"
    )
    log_error(text)
    if OWNER_ID:
        try:
            bot.send_message(int(OWNER_ID), text)
        except Exception:
            pass


def _message_text_for_finance(msg) -> str:
    return (getattr(msg, "text", None) or getattr(msg, "caption", None) or "").strip()


def _build_input_media_from_message(msg):
    caption = getattr(msg, "caption", None)
    ct = getattr(msg, "content_type", None)
    if ct == "photo" and getattr(msg, "photo", None):
        return InputMediaPhoto(msg.photo[-1].file_id, caption=caption)
    if ct == "video" and getattr(msg, "video", None):
        return InputMediaVideo(msg.video.file_id, caption=caption)
    if ct == "document" and getattr(msg, "document", None):
        return InputMediaDocument(msg.document.file_id, caption=caption)
    if ct == "audio" and getattr(msg, "audio", None):
        return InputMediaAudio(msg.audio.file_id, caption=caption)
    if ct == "animation" and getattr(msg, "animation", None):
        return InputMediaAnimation(msg.animation.file_id, caption=caption)
    return None


def _fallback_send_single(dst_chat_id: int, msg, reply_to_message_id=None):
    ct = getattr(msg, "content_type", None)
    if ct == "text":
        return _call_with_optional_reply(bot.send_message, dst_chat_id, msg.text or "", reply_to_message_id=reply_to_message_id)
    if ct == "photo" and getattr(msg, "photo", None):
        return _call_with_optional_reply(bot.send_photo, dst_chat_id, msg.photo[-1].file_id, caption=getattr(msg, "caption", None), reply_to_message_id=reply_to_message_id)
    if ct == "video" and getattr(msg, "video", None):
        return _call_with_optional_reply(bot.send_video, dst_chat_id, msg.video.file_id, caption=getattr(msg, "caption", None), reply_to_message_id=reply_to_message_id)
    if ct == "audio" and getattr(msg, "audio", None):
        return _call_with_optional_reply(bot.send_audio, dst_chat_id, msg.audio.file_id, caption=getattr(msg, "caption", None), reply_to_message_id=reply_to_message_id)
    if ct == "document" and getattr(msg, "document", None):
        return _call_with_optional_reply(bot.send_document, dst_chat_id, msg.document.file_id, caption=getattr(msg, "caption", None), reply_to_message_id=reply_to_message_id)
    if ct == "voice" and getattr(msg, "voice", None):
        return _call_with_optional_reply(bot.send_voice, dst_chat_id, msg.voice.file_id, caption=getattr(msg, "caption", None), reply_to_message_id=reply_to_message_id)
    if ct == "video_note" and getattr(msg, "video_note", None):
        return _call_with_optional_reply(bot.send_video_note, dst_chat_id, msg.video_note.file_id, reply_to_message_id=reply_to_message_id)
    if ct == "sticker" and getattr(msg, "sticker", None):
        return _call_with_optional_reply(bot.send_sticker, dst_chat_id, msg.sticker.file_id, reply_to_message_id=reply_to_message_id)
    if ct == "animation" and getattr(msg, "animation", None):
        return _call_with_optional_reply(bot.send_animation, dst_chat_id, msg.animation.file_id, caption=getattr(msg, "caption", None), reply_to_message_id=reply_to_message_id)
    if ct == "location" and getattr(msg, "location", None):
        return _call_with_optional_reply(bot.send_location, dst_chat_id, msg.location.latitude, msg.location.longitude, reply_to_message_id=reply_to_message_id)
    if ct == "venue" and getattr(msg, "venue", None):
        return _call_with_optional_reply(bot.send_venue, dst_chat_id, msg.venue.location.latitude, msg.venue.location.longitude, msg.venue.title, msg.venue.address, foursquare_id=getattr(msg.venue, "foursquare_id", None), reply_to_message_id=reply_to_message_id)
    if ct == "contact" and getattr(msg, "contact", None):
        return _call_with_optional_reply(bot.send_contact, dst_chat_id, msg.contact.phone_number, msg.contact.first_name, last_name=getattr(msg.contact, "last_name", None), reply_to_message_id=reply_to_message_id)
    if ct == "dice" and getattr(msg, "dice", None):
        return _call_with_optional_reply(bot.send_dice, dst_chat_id, emoji=getattr(msg.dice, "emoji", None), reply_to_message_id=reply_to_message_id)
    if ct == "poll" and getattr(msg, "poll", None):
        options = [opt.text for opt in getattr(msg.poll, "options", [])]
        return _call_with_optional_reply(bot.send_poll, dst_chat_id, msg.poll.question, options, is_anonymous=getattr(msg.poll, "is_anonymous", True), allows_multiple_answers=getattr(msg.poll, "allows_multiple_answers", False), type=getattr(msg.poll, "type", "regular"), reply_to_message_id=reply_to_message_id)
    raise RuntimeError(f"Unsupported fallback content_type={ct}")


def _forward_single_to_target(source_chat_id: int, msg, dst_chat_id: int, finance_enabled: bool):
    reply_to_target_id = None
    try:
        reply_to_msg = getattr(msg, "reply_to_message", None)
        if reply_to_msg is not None:
            reply_to_target_id = resolve_reply_target_message_id(
                source_chat_id,
                getattr(reply_to_msg, "message_id", None),
                dst_chat_id
            )
    except Exception as e:
        log_error(f"_forward_single_to_target reply resolve {source_chat_id}->{dst_chat_id}: {e}")

    try:
        if reply_to_target_id:
            try:
                sent = bot.copy_message(
                    dst_chat_id,
                    source_chat_id,
                    msg.message_id,
                    reply_to_message_id=reply_to_target_id,
                    allow_sending_without_reply=True
                )
            except TypeError:
                try:
                    sent = bot.copy_message(
                        dst_chat_id,
                        source_chat_id,
                        msg.message_id,
                        reply_to_message_id=reply_to_target_id
                    )
                except TypeError:
                    sent = bot.copy_message(dst_chat_id, source_chat_id, msg.message_id)
        else:
            sent = bot.copy_message(dst_chat_id, source_chat_id, msg.message_id)
        dst_msg_id = sent.message_id
    except Exception:
        try:
            sent_msg = _fallback_send_single(dst_chat_id, msg, reply_to_message_id=reply_to_target_id)
            dst_msg_id = sent_msg.message_id
        except Exception as e_send:
            _notify_forward_failure(source_chat_id, msg.message_id, dst_chat_id, e_send)
            return None

    _store_forward_link(source_chat_id, msg.message_id, dst_chat_id, dst_msg_id)

    text_for_finance = _message_text_for_finance(msg)
    if finance_enabled and text_for_finance and is_finance_mode(dst_chat_id):
        try:
            owner_id = msg.from_user.id if getattr(msg, "from_user", None) else 0
            sync_forwarded_finance_message(dst_chat_id, dst_msg_id, text_for_finance, owner_id)
        except Exception as e:
            log_error(f"_forward_single_to_target finance sync {source_chat_id}->{dst_chat_id}: {e}")

    return dst_msg_id


def _flush_media_group_forward(source_chat_id: int, media_group_id: str):
    cache_key = (int(source_chat_id), str(media_group_id))
    messages = _media_group_cache.pop(cache_key, [])
    timer = _media_group_timers.pop(cache_key, None)
    if timer and timer.is_alive():
        try:
            timer.cancel()
        except Exception:
            pass

    if not messages:
        return

    messages = sorted(messages, key=lambda m: m.message_id)
    targets = resolve_forward_targets(source_chat_id)
    if not targets:
        return

    media = []
    for msg in messages:
        item = _build_input_media_from_message(msg)
        if not item:
            media = []
            break
        media.append(item)

    group_reply_source_id = None
    try:
        first_reply = getattr(messages[0], "reply_to_message", None)
        if first_reply is not None:
            group_reply_source_id = getattr(first_reply, "message_id", None)
    except Exception:
        pass

    for dst_chat_id, mode, finance_enabled in targets:
        sent_ids = []
        reply_to_target_id = resolve_reply_target_message_id(source_chat_id, group_reply_source_id, dst_chat_id) if group_reply_source_id else None
        if media:
            try:
                if reply_to_target_id:
                    try:
                        sent_group = bot.send_media_group(dst_chat_id, media, reply_to_message_id=reply_to_target_id, allow_sending_without_reply=True)
                    except TypeError:
                        try:
                            sent_group = bot.send_media_group(dst_chat_id, media, reply_to_message_id=reply_to_target_id)
                        except TypeError:
                            sent_group = bot.send_media_group(dst_chat_id, media)
                else:
                    sent_group = bot.send_media_group(dst_chat_id, media)
                sent_ids = [m.message_id for m in sent_group]
            except Exception as e:
                log_error(f"_flush_media_group_forward send_media_group failed {source_chat_id}->{dst_chat_id}: {e}")

        if len(sent_ids) == len(messages):
            for src_msg, dst_msg_id in zip(messages, sent_ids):
                _store_forward_link(source_chat_id, src_msg.message_id, dst_chat_id, dst_msg_id)
                text_for_finance = _message_text_for_finance(src_msg)
                if finance_enabled and text_for_finance and is_finance_mode(dst_chat_id):
                    try:
                        owner_id = src_msg.from_user.id if getattr(src_msg, "from_user", None) else 0
                        sync_forwarded_finance_message(dst_chat_id, dst_msg_id, text_for_finance, owner_id)
                    except Exception as e:
                        log_error(f"_flush_media_group_forward finance sync {source_chat_id}->{dst_chat_id}: {e}")
            continue

        for src_msg in messages:
            _forward_single_to_target(source_chat_id, src_msg, dst_chat_id, finance_enabled)


def _collect_media_group_for_forward(source_chat_id: int, msg):
    cache_key = (int(source_chat_id), str(msg.media_group_id))
    bucket = _media_group_cache.setdefault(cache_key, [])
    if not any(m.message_id == msg.message_id for m in bucket):
        bucket.append(msg)

    prev = _media_group_timers.get(cache_key)
    if prev and prev.is_alive():
        try:
            prev.cancel()
        except Exception:
            pass

    t = threading.Timer(0.8, lambda: _flush_media_group_forward(source_chat_id, msg.media_group_id))
    _media_group_timers[cache_key] = t
    t.start()


def forward_any_message(source_chat_id: int, msg):
    try:
        if getattr(getattr(msg, "from_user", None), "is_bot", False):
            return
        if getattr(msg, "edit_date", None):
            return

        targets = resolve_forward_targets(source_chat_id)
        if not targets:
            return

        if getattr(msg, "media_group_id", None) and getattr(msg, "content_type", None) in ("photo", "video", "document", "audio"):
            _collect_media_group_for_forward(source_chat_id, msg)
            return

        for dst_chat_id, mode, finance_enabled in targets:
            _forward_single_to_target(source_chat_id, msg, dst_chat_id, finance_enabled)

    except Exception as e:
        log_error(f"forward_any_message fatal: {e}")

    

def render_day_window(chat_id: int, day_key: str):
    store = get_chat_store(chat_id)
    recs = store.get("daily_records", {}).get(day_key, [])

    d = datetime.strptime(day_key, "%Y-%m-%d")
    wd = ["пн", "вт", "ср", "чт", "пт", "сб", "вс"][d.weekday()]

    t = now_local()
    td = t.strftime("%Y-%m-%d")
    yd = (t - timedelta(days=1)).strftime("%Y-%m-%d")
    tm = (t + timedelta(days=1)).strftime("%Y-%m-%d")

    tag = "сегодня" if day_key == td else "вчера" if day_key == yd else "завтра" if day_key == tm else ""
    dk = fmt_date_ddmmyy(day_key)
    label = f"{dk} ({tag}, {wd})" if tag else f"{dk} ({wd})"

    header = [f"📅 {label}", ""]
    total_income = 0.0
    total_expense = 0.0

    recs_sorted = sorted(recs, key=lambda x: x.get("timestamp"))
    all_record_lines = []

    for r in recs_sorted:
        amt = float(r.get("amount", 0) or 0)
        if amt >= 0:
            total_income += amt
        else:
            total_expense += -amt

        note = html.escape(r.get("note", ""))
        sid = r.get("short_id", f"R{r['id']}")
        all_record_lines.append(f"{sid} {fmt_num(amt)} {note}")

    day_balance = calc_day_balance(store, day_key)
    bal_chat = store.get("balance", 0)

    footer = [""]
    if recs_sorted:
        footer.append(f"📉 Расход за день: {fmt_num(-total_expense) if total_expense else fmt_num(0)}")
        footer.append(f"📈 Приход за день: {fmt_num(total_income) if total_income else fmt_num(0)}")
    footer.append(f"📆 Остаток на конец дня: {fmt_num(day_balance)}")
    footer.append(f"🏦 Остаток по чату: {fmt_num(bal_chat)}")

    total = total_income - total_expense

    if not all_record_lines:
        return "\n".join(header + ["Нет записей за этот день."] + footer), total

    hidden = 0
    visible = list(all_record_lines)

    if len(visible) > DAY_WINDOW_MAX_RECORDS:
        hidden = len(visible) - DAY_WINDOW_MAX_RECORDS
        visible = visible[-DAY_WINDOW_MAX_RECORDS:]

    while True:
        prefix = []
        if hidden > 0:
            prefix = [f"… скрыто ранних записей: {hidden}", ""]

        text = "\n".join(header + prefix + visible + footer)

        if len(text) <= DAY_WINDOW_MAX_CHARS:
            return text, total

        if len(visible) <= 5:
            return text[:DAY_WINDOW_MAX_CHARS], total

        hidden += 1
        visible = visible[1:]

def build_main_keyboard(day_key: str, chat_id=None):
    kb = types.InlineKeyboardMarkup(row_width=3)
    kb.row(
        types.InlineKeyboardButton("📋 Меню", callback_data=f"d:{day_key}:menu")
    )

    nav_row = [
        types.InlineKeyboardButton("⬅️ Вчера", callback_data=f"d:{day_key}:prev")
    ]
    if day_key != today_key():
        nav_row.append(
            types.InlineKeyboardButton("📅 Сегодня", callback_data=f"d:{day_key}:today")
        )
    nav_row.append(
        types.InlineKeyboardButton("➡️ Завтра", callback_data=f"d:{day_key}:next")
    )
    kb.row(*nav_row)

    kb.row(
        types.InlineKeyboardButton("📅 Календарь", callback_data=f"d:{day_key}:calendar"),
        types.InlineKeyboardButton("📊 Отчёт", callback_data=f"d:{day_key}:report")
    )
    kb.row(
        types.InlineKeyboardButton("ℹ️ Инфо", callback_data=f"d:{day_key}:info"),
        types.InlineKeyboardButton("💰 Общий итог", callback_data=f"d:{day_key}:total")
    )
    return kb

def build_report_keyboard(month_key: str):
    """
    month_key: YYYY-MM
    """
    kb = types.InlineKeyboardMarkup(row_width=4)

    try:
        dt = datetime.strptime(month_key + "-01", "%Y-%m-%d")
    except Exception:
        dt = now_local().replace(day=1)
        month_key = dt.strftime("%Y-%m")

    current_month = now_local().strftime("%Y-%m")

    prev_month = (dt.replace(day=1) - timedelta(days=1)).replace(day=1)
    next_month = (dt.replace(day=28) + timedelta(days=4)).replace(day=1)

    row = [
        types.InlineKeyboardButton(
            "⬅️ Пред. месяц",
            callback_data=f"rep:{prev_month.strftime('%Y-%m')}"
        )
    ]

    if month_key != current_month:
        row.append(
            types.InlineKeyboardButton(
                "📅 Сегодня",
                callback_data="rep_today"
            )
        )

    row.append(
        types.InlineKeyboardButton(
            "❌ Закрыть",
            callback_data="rep_close"
        )
    )

    row.append(
        types.InlineKeyboardButton(
            "След. месяц ➡️",
            callback_data=f"rep:{next_month.strftime('%Y-%m')}"
        )
    )

    kb.row(*row)
    return kb

def build_month_report_text(chat_id: int, month_key: str = None):
    """
    Отчёт за месяц в виде:
    Дата    | Приход| Расход|Остаток
    """
    store = get_chat_store(chat_id)
    daily = store.get("daily_records", {})

    if not month_key:
        month_key = now_local().strftime("%Y-%m")

    try:
        month_dt = datetime.strptime(month_key + "-01", "%Y-%m-%d")
    except Exception:
        month_dt = now_local().replace(day=1)
        month_key = month_dt.strftime("%Y-%m")

    year = month_dt.year
    month = month_dt.month

    if month == 12:
        next_month = datetime(year + 1, 1, 1)
    else:
        next_month = datetime(year, month + 1, 1)

    days_in_month = (next_month - timedelta(days=1)).day

    lines = []
    lines.append(f"ОТЧЁТ ЗА {month_dt.strftime('%m.%Y')}")
    lines.append("")
    lines.append(
        f"{'Дата':<8}|"
        f"{report_header_cell('Приход', 7)}|"
        f"{report_header_cell('Расход', 7)}|"
        f"{report_header_cell('Остаток', 7)}"
    )
    lines.append("")

    has_any = False

    for day in range(1, days_in_month + 1):
        day_key = f"{year}-{month:02d}-{day:02d}"
        recs = daily.get(day_key, [])

        total_expense = 0
        total_income = 0

        for r in recs:
            amt = r.get("amount", 0)
            if amt < 0:
                total_expense += -amt
            else:
                total_income += amt

        day_balance = calc_day_balance(store, day_key)
        if recs:
            has_any = True

        date_str = datetime.strptime(day_key, "%Y-%m-%d").strftime("%d.%m.%y")
        lines.append(
            f"{date_str:<8}|"
            f"{report_cell(int(total_income), 7)}|"
            f"{report_cell(int(total_expense), 7)}|"
            f"{report_cell(int(day_balance), 7)}"
        )

    if not has_any:
        lines.append("Нет данных за этот месяц.")

    return "<pre>" + html.escape("\n".join(lines)) + "</pre>", month_key

def build_calendar_keyboard(center_day: datetime, chat_id=None):
    """
    Календарь на 31 день.
    Дни с записями помечаются точкой: 📝 12.03
    """
    kb = types.InlineKeyboardMarkup(row_width=4)
    daily = {}
    back_day_key = today_key()
    if chat_id is not None:
        store = get_chat_store(chat_id)
        daily = store.get("daily_records", {})
        back_day_key = store.get("current_view_day", today_key())

    start_day = center_day.replace(day=1)
    if center_day.month == 12:
        next_month = center_day.replace(year=center_day.year + 1, month=1, day=1)
    else:
        next_month = center_day.replace(month=center_day.month + 1, day=1)

    days_in_month = (next_month - timedelta(days=1)).day
    for week in range(0, days_in_month, 4):
        row = []
        for d in range(4):
            day_index = week + d
            if day_index >= days_in_month:
                continue

            day = start_day + timedelta(days=day_index)
            label = day.strftime("%d.%m")
            key = day.strftime("%Y-%m-%d")
            if daily.get(key):
                label = "📝 " + label
            row.append(
                types.InlineKeyboardButton(
                    label,
                    callback_data=f"d:{key}:open"
                )
            )
        if row:
            kb.row(*row)

    prev_month = (center_day.replace(day=1) - timedelta(days=1)).replace(day=1)
    next_month = (center_day.replace(day=28) + timedelta(days=4)).replace(day=1)

    kb.row(
        types.InlineKeyboardButton(
            "⬅️ Месяц",
            callback_data=f"c:{prev_month.strftime('%Y-%m-%d')}"
        ),
        types.InlineKeyboardButton(
            "➡️ Месяц",
            callback_data=f"c:{next_month.strftime('%Y-%m-%d')}"
        )
    )

    current_month = now_local().strftime("%Y-%m")
    shown_month = center_day.strftime("%Y-%m")
    bottom_row = []
    if shown_month != current_month:
        bottom_row.append(
            types.InlineKeyboardButton(
                "📅 Сегодня",
                callback_data=f"c:{now_local().strftime('%Y-%m-%d')}"
            )
        )
    elif back_day_key != today_key():
        bottom_row.append(
            types.InlineKeyboardButton(
                "📅 Сегодня",
                callback_data=f"d:{today_key()}:open"
            )
        )

    bottom_row.append(
        types.InlineKeyboardButton(
            "🔙 Назад",
            callback_data=f"d:{back_day_key}:back_main"
        )
    )
    kb.row(*bottom_row)
    return kb

def build_csv_menu(day_key: str):
    kb = types.InlineKeyboardMarkup(row_width=3)
    buttons = [
        types.InlineKeyboardButton("📅 За день", callback_data=f"d:{day_key}:csv_day"),
        types.InlineKeyboardButton("🗓 За неделю", callback_data=f"d:{day_key}:csv_week"),
        types.InlineKeyboardButton("📆 За месяц", callback_data=f"d:{day_key}:csv_month"),
        types.InlineKeyboardButton("📊 Ср–Чт", callback_data=f"d:{day_key}:csv_wedthu"),
        types.InlineKeyboardButton("📂 Всё время", callback_data=f"d:{day_key}:csv_all_real"),
    ]
    add_buttons_in_rows(kb, buttons, 3)
    kb.row(types.InlineKeyboardButton("⬅️ Назад", callback_data=f"d:{day_key}:edit_menu"))
    return kb

def build_edit_menu_keyboard(day_key: str, chat_id=None):
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.row(
        types.InlineKeyboardButton("📝 Редактировать запись", callback_data=f"d:{day_key}:edit_list"),
        types.InlineKeyboardButton("📂 Общий CSV", callback_data=f"d:{day_key}:csv_all")
    )
    kb.row(
        types.InlineKeyboardButton("⚙️ Обнулить", callback_data=f"d:{day_key}:reset"),
        types.InlineKeyboardButton("📊 Статьи расходов", callback_data="cat_today")
    )

    if OWNER_ID and str(chat_id) == str(OWNER_ID):
        kb.row(
            types.InlineKeyboardButton("🔁 Пересылка", callback_data=f"d:{day_key}:forward_menu")
        )

    nav_row = []
    if day_key != today_key():
        nav_row.append(
            types.InlineKeyboardButton("📅 Сегодня", callback_data=f"d:{today_key()}:open")
        )
    nav_row.append(
        types.InlineKeyboardButton("📆 Выбрать день", callback_data=f"d:{day_key}:pick_date")
    )
    kb.row(*nav_row)

    kb.row(
        types.InlineKeyboardButton("ℹ️ Инфо", callback_data=f"d:{day_key}:info"),
        types.InlineKeyboardButton("🔙 Назад", callback_data=f"d:{day_key}:back_main")
    )
    return kb

def build_cancel_edit_keyboard(day_key: str):
    kb = types.InlineKeyboardMarkup()
    kb.row(
        types.InlineKeyboardButton(
            "❌ Отмена",
            callback_data=f"d:{day_key}:cancel_edit"
        )
    )
    return kb

def build_forward_chat_list(day_key: str, chat_id: int):
    kb = types.InlineKeyboardMarkup()
    if not OWNER_ID:
        return kb

    known = collect_forward_menu_chats()
    rules = data.get("forward_rules", {})
    buttons = []

    for cid, info in sorted(known.items(), key=lambda x: (x[1].get("title") or "").lower()):
        try:
            int_cid = int(cid)
        except Exception:
            continue

        if int_cid == chat_id:
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
            label = title

        buttons.append(types.InlineKeyboardButton(label, callback_data=f"d:{day_key}:fw_cfg_{cid}"))

    add_buttons_in_rows(kb, buttons, 3)
    kb.row(types.InlineKeyboardButton("🔙 Назад", callback_data=f"d:{day_key}:forward_menu"))
    return kb
def build_forward_direction_menu(day_key: str, owner_chat: int, target_chat: int):
    owner_title = get_chat_display_name(owner_chat)
    target_title = get_chat_display_name(target_chat)

    kb = types.InlineKeyboardMarkup(row_width=1)
    fr = data.get("forward_rules", {}) or {}

    ab_link = str(target_chat) in fr.get(str(owner_chat), {})
    ba_link = str(owner_chat) in fr.get(str(target_chat), {})
    two_on = ab_link and ba_link

    ab_state = "ВКЛ ✅" if ab_link else "ВЫКЛ ❌"
    ba_state = "ВКЛ ✅" if ba_link else "ВЫКЛ ❌"
    two_state = "ВКЛ ✅" if two_on else "ВЫКЛ ❌"

    ab_fin = "ВКЛ ✅" if get_forward_finance(owner_chat, target_chat) else "ВЫКЛ ❌"
    ba_fin = "ВКЛ ✅" if get_forward_finance(target_chat, owner_chat) else "ВЫКЛ ❌"

    kb.row(types.InlineKeyboardButton(
        f"➡️ {ab_state} {owner_title} → {target_title}",
        callback_data=f"d:{day_key}:fw_one_{target_chat}"
    ))
    kb.row(types.InlineKeyboardButton(
        f"⬅️ {ba_state} {target_title} → {owner_title}",
        callback_data=f"d:{day_key}:fw_rev_{target_chat}"
    ))
    kb.row(types.InlineKeyboardButton(
        f"↔️ {two_state} {owner_title} ⇄ {target_title}",
        callback_data=f"d:{day_key}:fw_two_{target_chat}"
    ))
    kb.row(types.InlineKeyboardButton(
        f"💰 {ab_fin} Учёт {owner_title} → {target_title}",
        callback_data=f"d:{day_key}:fw_fin_ab_{target_chat}"
    ))
    kb.row(types.InlineKeyboardButton(
        f"💰 {ba_fin} Учёт {target_title} → {owner_title}",
        callback_data=f"d:{day_key}:fw_fin_ba_{target_chat}"
    ))
    kb.row(types.InlineKeyboardButton(
        "❌ Удалить все связи",
        callback_data=f"d:{day_key}:fw_del_{target_chat}"
    ))
    kb.row(types.InlineKeyboardButton(
        "🔙 Назад",
        callback_data="fw_back_root"
    ))
    return kb
def build_forward_root_menu(day_key: str):
    kb = types.InlineKeyboardMarkup(row_width=2)
    add_buttons_in_rows(kb, [
        types.InlineKeyboardButton("📨 Чаты и пары", callback_data="fw_open"),
        types.InlineKeyboardButton("💰 Фин режим", callback_data=f"d:{day_key}:forward_finmode_menu"),
        types.InlineKeyboardButton("🏦 Быстрый остаток", callback_data=f"d:{day_key}:quick_balance_menu"),
        types.InlineKeyboardButton("🪟 Фин окна чатов", callback_data=f"d:{day_key}:fin_windows_menu"),
    ], 2)
    kb.row(types.InlineKeyboardButton("🔙 Назад", callback_data=f"d:{day_key}:edit_menu"))
    return kb


def _collect_forward_picker_items(include_owner: bool = True):
    known = collect_forward_menu_chats()
    items = []
    owner_item = None

    for cid, ch in sorted(known.items(), key=lambda x: (x[1].get("title") or "").lower()):
        try:
            int_cid = int(cid)
        except Exception:
            continue
        title = ch.get("title") or f"Чат {cid}"
        if OWNER_ID and str(int_cid) == str(OWNER_ID):
            owner_item = (int_cid, title)
        else:
            items.append((int_cid, title))

    if include_owner and OWNER_ID:
        try:
            owner_id = int(OWNER_ID)
            if owner_item is None:
                owner_item = (owner_id, get_chat_display_name(owner_id))
        except Exception:
            owner_item = None

    return items, owner_item


def build_forward_source_menu():
    kb = types.InlineKeyboardMarkup()
    if not OWNER_ID:
        return kb

    items, owner_item = _collect_forward_picker_items(include_owner=True)
    buttons = [
        types.InlineKeyboardButton(title, callback_data=f"fw_src:{cid}")
        for cid, title in items
    ]

    add_buttons_in_rows(kb, buttons, 3)

    if owner_item:
        kb.row(types.InlineKeyboardButton(owner_item[1], callback_data=f"fw_src:{owner_item[0]}"))

    kb.row(types.InlineKeyboardButton("🔙 Назад", callback_data="fw_back_root"))
    return kb


def build_forward_target_menu(src_id: int):
    kb = types.InlineKeyboardMarkup()
    if not OWNER_ID:
        return kb

    items, owner_item = _collect_forward_picker_items(include_owner=True)
    buttons = []

    for int_cid, title in items:
        if int_cid == src_id:
            continue
        buttons.append(types.InlineKeyboardButton(title, callback_data=f"fw_tgt:{src_id}:{int_cid}"))

    add_buttons_in_rows(kb, buttons, 3)

    if owner_item and owner_item[0] != src_id:
        kb.row(types.InlineKeyboardButton(owner_item[1], callback_data=f"fw_tgt:{src_id}:{owner_item[0]}"))

    kb.row(types.InlineKeyboardButton("🔙 Назад", callback_data="fw_back_src"))
    return kb


def build_finance_toggle_chat_menu(day_key: str):
    kb = types.InlineKeyboardMarkup(row_width=2)
    known = collect_forward_menu_chats()

    items = {}
    for cid, ch in known.items():
        try:
            int_cid = int(cid)
        except Exception:
            continue
        items[int_cid] = ch.get("title") or get_chat_display_name(int_cid)

    if OWNER_ID:
        try:
            owner_id = int(OWNER_ID)
            items.setdefault(owner_id, get_chat_display_name(owner_id))
        except Exception:
            pass

    buttons = []
    for int_cid, title in sorted(items.items(), key=lambda x: x[1].lower()):
        enabled = is_finance_mode(int_cid)
        buttons.append(types.InlineKeyboardButton(
            f'{"✅" if enabled else "❌"} {title}',
            callback_data=f"d:{day_key}:fw_finmode_pick_{int_cid}"
        ))

    add_buttons_in_rows(kb, buttons, 2)
    kb.row(types.InlineKeyboardButton("🔙 Назад", callback_data=f"d:{day_key}:forward_menu"))
    return kb


def build_quick_balance_chat_menu(day_key: str):
    kb = types.InlineKeyboardMarkup(row_width=2)
    known = collect_forward_menu_chats()

    items = {}
    for cid, ch in known.items():
        try:
            int_cid = int(cid)
        except Exception:
            continue
        items[int_cid] = ch.get("title") or get_chat_display_name(int_cid)

    owner_item = None
    if OWNER_ID:
        try:
            owner_id = int(OWNER_ID)
            owner_item = (owner_id, get_chat_display_name(owner_id))
            items.setdefault(owner_id, owner_item[1])
        except Exception:
            owner_item = None

    buttons = []
    for int_cid, title in sorted(items.items(), key=lambda x: x[1].lower()):
        if owner_item and int_cid == owner_item[0]:
            continue
        enabled = is_quick_balance_enabled(int_cid)
        icon = "✅" if enabled else "⬜"
        buttons.append(types.InlineKeyboardButton(
            f'{icon} {title}',
            callback_data=f"d:{day_key}:qb_cfg_{int_cid}"
        ))

    add_buttons_in_rows(kb, buttons, 2)

    if owner_item:
        enabled = is_quick_balance_enabled(owner_item[0])
        icon = "✅" if enabled else "⬜"
        kb.row(types.InlineKeyboardButton(
            f'{icon} {owner_item[1]}',
            callback_data=f"d:{day_key}:qb_cfg_{owner_item[0]}"
        ))

    kb.row(types.InlineKeyboardButton("🔙 Назад", callback_data=f"d:{day_key}:forward_menu"))
    return kb


def build_quick_balance_mode_menu(day_key: str, target_chat_id: int):
    kb = types.InlineKeyboardMarkup(row_width=2)
    enabled = is_quick_balance_enabled(target_chat_id)
    behavior = get_quick_balance_behavior(target_chat_id)

    normal_icon = "✅" if enabled and behavior != "open" else "⬜"
    open_icon = "✅" if enabled and behavior == "open" else "⬜"

    kb.row(
        types.InlineKeyboardButton(f"{normal_icon} Как обычно", callback_data=f"d:{day_key}:qb_mode_normal_{target_chat_id}"),
        types.InlineKeyboardButton(f"{open_icon} Быстрый остаток открывался", callback_data=f"d:{day_key}:qb_mode_open_{target_chat_id}")
    )
    kb.row(types.InlineKeyboardButton("🔙 Назад", callback_data=f"d:{day_key}:quick_balance_menu"))
    return kb


def build_fin_windows_chat_menu(day_key: str):
    kb = types.InlineKeyboardMarkup(row_width=2)
    items = []

    for cid, store in (data.get("chats", {}) or {}).items():
        try:
            int_cid = int(cid)
        except Exception:
            continue
        if not is_finance_mode(int_cid):
            continue
        items.append((int_cid, get_chat_display_name(int_cid)))

    buttons = [
        types.InlineKeyboardButton(title, callback_data=f"d:{day_key}:finwin_open_{cid}")
        for cid, title in sorted(items, key=lambda x: x[1].lower())
    ]

    if buttons:
        add_buttons_in_rows(kb, buttons, 2)
    else:
        kb.row(types.InlineKeyboardButton("Нет чатов с финрежимом", callback_data="none"))

    kb.row(types.InlineKeyboardButton("🔙 Назад", callback_data=f"d:{day_key}:forward_menu"))
    return kb


def build_fin_window_view_keyboard(target_chat_id: int, day_key: str, owner_day_key: str):
    kb = types.InlineKeyboardMarkup(row_width=3)
    prev_day = (datetime.strptime(day_key, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
    next_day = (datetime.strptime(day_key, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")

    nav_row = [types.InlineKeyboardButton("⬅️ Вчера", callback_data=f"fv:{target_chat_id}:{prev_day}:open:{owner_day_key}")]
    if day_key != today_key():
        nav_row.append(types.InlineKeyboardButton("📅 Сегодня", callback_data=f"fv:{target_chat_id}:{today_key()}:open:{owner_day_key}"))
    nav_row.append(types.InlineKeyboardButton("➡️ Завтра", callback_data=f"fv:{target_chat_id}:{next_day}:open:{owner_day_key}"))
    kb.row(*nav_row)
    kb.row(types.InlineKeyboardButton("🔙 Назад", callback_data=f"d:{owner_day_key}:fin_windows_menu"))
    return kb


def render_fin_window_text(target_chat_id: int, day_key: str):
    txt, _ = render_day_window(target_chat_id, day_key)
    return f"👁 {html.escape(get_chat_display_name(target_chat_id))}\n\n{txt}"


def build_forward_mode_menu(A: int, B: int):
    """
    Меню выбора режима пересылки между чатами A и B.
    """
    kb = types.InlineKeyboardMarkup()

    name_a = get_chat_display_name(A)
    name_b = get_chat_display_name(B)

    fr = data.get("forward_rules", {}) or {}
    ab_link = str(B) in fr.get(str(A), {})
    ba_link = str(A) in fr.get(str(B), {})
    two_on = ab_link and ba_link

    ab_state = "ВКЛ ✅" if ab_link else "ВЫКЛ ❌"
    ba_state = "ВКЛ ✅" if ba_link else "ВЫКЛ ❌"
    two_state = "ВКЛ ✅" if two_on else "ВЫКЛ ❌"

    ab_fin = "ВКЛ ✅" if get_forward_finance(A, B) else "ВЫКЛ ❌"
    ba_fin = "ВКЛ ✅" if get_forward_finance(B, A) else "ВЫКЛ ❌"

    kb.row(types.InlineKeyboardButton(
        f"➡️ {ab_state} {name_a} → {name_b}",
        callback_data=f"fw_mode:{A}:{B}:to"
    ))
    kb.row(types.InlineKeyboardButton(
        f"⬅️ {ba_state} {name_b} → {name_a}",
        callback_data=f"fw_mode:{A}:{B}:from"
    ))
    kb.row(types.InlineKeyboardButton(
        f"↔️ {two_state} {name_a} ⇄ {name_b}",
        callback_data=f"fw_mode:{A}:{B}:two"
    ))
    kb.row(types.InlineKeyboardButton(
        f"💰 {ab_fin} Учёт {name_a} → {name_b}",
        callback_data=f"fw_finpair:{A}:{B}:ab"
    ))
    kb.row(types.InlineKeyboardButton(
        f"💰 {ba_fin} Учёт {name_b} → {name_a}",
        callback_data=f"fw_finpair:{A}:{B}:ba"
    ))
    kb.row(types.InlineKeyboardButton(
        "❌ Удалить все связи A-B",
        callback_data=f"fw_mode:{A}:{B}:del"
    ))
    kb.row(types.InlineKeyboardButton(
        "🔙 Назад",
        callback_data=f"fw_back_tgt:{A}"
    ))
    return kb
def apply_forward_mode(A: int, B: int, mode: str):
    """
    Применяет выбранный режим пересылки между чатами A и B.
    Использует общие функции add_forward_link / remove_forward_link.
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

def safe_edit(bot, call, text, reply_markup=None, parse_mode=None):
    """Безопасное обновление: edit_text → edit_caption → send_message."""
    chat_id = call.message.chat.id
    msg_id = call.message.message_id
    try:
        bot.edit_message_text(
            text,
            chat_id=chat_id,
            message_id=msg_id,
            reply_markup=reply_markup,
            parse_mode=parse_mode
        )
        return
    except Exception:
        pass
    try:
        bot.edit_message_caption(
            chat_id=chat_id,
            message_id=msg_id,
            caption=text,
            reply_markup=reply_markup,
            parse_mode=parse_mode
        )
        return
    except Exception:
        pass
    bot.send_message(chat_id, text, reply_markup=reply_markup, parse_mode=parse_mode)

def send_or_edit_categories_window(chat_id, text, reply_markup=None, parse_mode=None, preferred_message_id=None):
    """Отдельное окно для отчёта по статьям расходов (одно сообщение на чат)."""
    store = get_chat_store(chat_id)
    mid = store.get("categories_msg_id")

    candidates = []
    if mid:
        try:
            mid_int = int(mid)
            if preferred_message_id is not None:
                try:
                    pref_int = int(preferred_message_id)
                except Exception:
                    pref_int = None
                if pref_int is not None and pref_int == mid_int:
                    candidates.append(pref_int)
            if mid_int not in candidates:
                candidates.append(mid_int)
        except Exception:
            pass

    for target_id in candidates:
        try:
            bot.edit_message_text(
                text,
                chat_id=chat_id,
                message_id=target_id,
                reply_markup=reply_markup,
                parse_mode=parse_mode
            )
            store["categories_msg_id"] = target_id
            save_data(data)
            save_chat_json(chat_id)
            return target_id
        except Exception as e:
            log_error(f"send_or_edit_categories_window edit failed {chat_id}:{target_id}: {e}")
            if store.get("categories_msg_id") == target_id:
                store["categories_msg_id"] = None
                save_data(data)
                save_chat_json(chat_id)

    sent = bot.send_message(chat_id, text, reply_markup=reply_markup, parse_mode=parse_mode)
    store["categories_msg_id"] = sent.message_id
    save_data(data)
    save_chat_json(chat_id)
    return sent.message_id

def build_week_thu_keyboard(start_key: str):
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.row(
        types.InlineKeyboardButton("⬅️", callback_data=f"wthu:{start_key}:prev"),
        types.InlineKeyboardButton("➡️", callback_data=f"wthu:{start_key}:next"),
    )
    return kb
def open_report_window(chat_id: int, month_key: str = None, message_id: int = None):
    """
    Открывает или обновляет отдельное окно отчёта.
    """
    text, month_key = build_month_report_text(chat_id, month_key)
    kb = build_report_keyboard(month_key)

    store = get_chat_store(chat_id)
    target_id = message_id or store.get("report_window_id")

    if target_id:
        try:
            bot.edit_message_text(
                text,
                chat_id=chat_id,
                message_id=target_id,
                reply_markup=kb,
                parse_mode="HTML"
            )
            store["report_window_id"] = target_id
            store["report_month"] = month_key
            save_data(data)
            schedule_stored_window_delete(chat_id, "report_window_id", AUX_WINDOW_DELETE_DELAY)
            return
        except Exception as e:
            log_error(f"open_report_window edit failed: {e}")
            _clear_stored_window(chat_id, "report_window_id", target_id)

    sent = bot.send_message(
        chat_id,
        text,
        reply_markup=kb,
        parse_mode="HTML"
    )
    store["report_window_id"] = sent.message_id
    store["report_month"] = month_key
    save_data(data)
    schedule_stored_window_delete(chat_id, "report_window_id", AUX_WINDOW_DELETE_DELAY)


def open_info_window(chat_id: int):
    info_text = build_info_text(chat_id)
    kb = types.InlineKeyboardMarkup()
    kb.row(types.InlineKeyboardButton("❌ Закрыть", callback_data="info_close"))
    send_or_edit_stored_window(
        chat_id,
        "info_msg_id",
        info_text,
        reply_markup=kb,
        parse_mode=None,
        delay=AUX_WINDOW_DELETE_DELAY
    )
def handle_categories_callback(call, data_str: str) -> bool:
    """UI окна расходов по статьям."""
    chat_id = call.message.chat.id
    store = get_chat_store(chat_id)

    if data_str == "cat_close":
        mid = store.get("categories_msg_id")
        if mid:
            try:
                bot.delete_message(chat_id, mid)
            except Exception:
                pass
        store["categories_msg_id"] = None
        save_data(data)
        save_chat_json(chat_id)
        return True

    if data_str == "cat_today":
        return handle_categories_callback(call, f"cat_wthu:{today_key()}")

    if data_str.startswith("cat_wthu:"):
        ref = data_str.split(":", 1)[1] or today_key()
        start_key = week_start_thursday(ref)
        start, end = week_bounds_thu_wed(start_key)
        label = f"{fmt_date_ddmmyy(start)} — {fmt_date_ddmmyy(end)} (Чт–Ср)"
        text, _ = summarize_categories(store, start, end, label)
        kb = build_categories_summary_keyboard("wthu", start, end)
        send_or_edit_categories_window(chat_id, text, reply_markup=kb, preferred_message_id=call.message.message_id)
        return True

    if data_str.startswith("cat_wk:"):
        start_key = data_str.split(":", 1)[1].strip() or week_start_monday(today_key())
        start, end = week_bounds_from_start(start_key)
        label = f"{fmt_date_ddmmyy(start)} — {fmt_date_ddmmyy(end)} (Пн–Вс)"
        text, _ = summarize_categories(store, start, end, label)
        kb = build_categories_summary_keyboard("wk", start, end)
        send_or_edit_categories_window(chat_id, text, reply_markup=kb, preferred_message_id=call.message.message_id)
        return True

    if data_str == "cat_months":
        kb = types.InlineKeyboardMarkup(row_width=3)
        current_month = now_local().month
        for m in range(1, 13):
            label = datetime(2000, m, 1).strftime("%b")
            kb.add(types.InlineKeyboardButton(label, callback_data=f"cat_m:{m}"))
        kb.row(
            types.InlineKeyboardButton("📅 Сегодня", callback_data="cat_today"),
            types.InlineKeyboardButton("❌ Закрыть статьи", callback_data="cat_close")
        )
        send_or_edit_categories_window(chat_id, "📦 Выберите месяц:", reply_markup=kb)
        return True

    if data_str.startswith("cat_m:"):
        try:
            month = int(data_str.split(":")[1])
        except Exception:
            return True
        year = now_local().year
        kb = types.InlineKeyboardMarkup(row_width=2)
        weeks = [(1, 7), (8, 14), (15, 21), (22, 31)]
        for a, b in weeks:
            kb.add(types.InlineKeyboardButton(
                f"{a:02d}–{b:02d}",
                callback_data=f"cat_rng:{year}:{month}:{a}:{b}"
            ))
        row = []
        if month != now_local().month:
            row.append(types.InlineKeyboardButton("📅 Сегодня", callback_data="cat_today"))
        row.append(types.InlineKeyboardButton("🔙 Назад", callback_data="cat_months"))
        kb.row(*row)
        send_or_edit_categories_window(chat_id, "📆 Выберите неделю:", reply_markup=kb)
        return True

    if data_str.startswith("cat_rng:"):
        try:
            _, y, m, a, b = data_str.split(":")
            y, m, a, b = map(int, (y, m, a, b))
        except Exception:
            return True

        if m == 12:
            last_day = (datetime(y + 1, 1, 1) - timedelta(days=1)).day
        else:
            last_day = (datetime(y, m + 1, 1) - timedelta(days=1)).day

        a = max(1, min(a, last_day))
        b = max(1, min(b, last_day))
        if b < a:
            b = a

        start = f"{y}-{m:02d}-{a:02d}"
        end = f"{y}-{m:02d}-{b:02d}"
        label = f"{fmt_date_ddmmyy(start)} — {fmt_date_ddmmyy(end)}"
        text, _ = summarize_categories(store, start, end, label)
        kb = build_categories_summary_keyboard("rng", start, end)
        send_or_edit_categories_window(chat_id, text, reply_markup=kb, preferred_message_id=call.message.message_id)
        return True

    if data_str.startswith("cat_show_wthu:"):
        _, ref, slug = data_str.split(":", 2)
        category = CATEGORY_BY_SLUG.get(slug)
        if not category:
            return True

        start_key = week_start_thursday(ref or today_key())
        start, end = week_bounds_thu_wed(start_key)
        label = f"{fmt_date_ddmmyy(start)} — {fmt_date_ddmmyy(end)} (Чт–Ср)"
        text = build_category_detail_text(store, start, end, category, label)
        kb = build_category_detail_keyboard(start, end, f"cat_wthu:{start}", mode="wthu", slug=slug)
        send_or_edit_categories_window(chat_id, text, reply_markup=kb, preferred_message_id=call.message.message_id)
        return True

    if data_str.startswith("cat_show_wk:"):
        _, ref, slug = data_str.split(":", 2)
        category = CATEGORY_BY_SLUG.get(slug)
        if not category:
            return True

        start_key = week_start_monday(ref or today_key())
        start, end = week_bounds_from_start(start_key)
        label = f"{fmt_date_ddmmyy(start)} — {fmt_date_ddmmyy(end)} (Пн–Вс)"
        text = build_category_detail_text(store, start, end, category, label)
        kb = build_category_detail_keyboard(start, end, f"cat_wk:{start}", mode="wk", slug=slug)
        send_or_edit_categories_window(chat_id, text, reply_markup=kb, preferred_message_id=call.message.message_id)
        return True

    if data_str.startswith("cat_show:"):
        _, start, end, slug = data_str.split(":", 3)
        category = CATEGORY_BY_SLUG.get(slug)
        if not category:
            return True

        start_dt = datetime.strptime(start, "%Y-%m-%d")
        end_dt = datetime.strptime(end, "%Y-%m-%d")
        label = f"{fmt_date_ddmmyy(start)} — {fmt_date_ddmmyy(end)}"

        if (end_dt - start_dt).days == 6 and start == week_start_thursday(start):
            back_callback = f"cat_wthu:{start}"
            label += " (Чт–Ср)"
        elif (end_dt - start_dt).days == 6 and start == week_start_monday(start):
            back_callback = f"cat_wk:{start}"
            label += " (Пн–Вс)"
        else:
            y, m = start_dt.year, start_dt.month
            back_callback = f"cat_rng:{y}:{m}:{start_dt.day}:{end_dt.day}"

        mode = None
        if (end_dt - start_dt).days == 6 and start == week_start_thursday(start):
            mode = "wthu"
        elif (end_dt - start_dt).days == 6 and start == week_start_monday(start):
            mode = "wk"

        text = build_category_detail_text(store, start, end, category, label)
        kb = build_category_detail_keyboard(start, end, back_callback, mode=mode, slug=slug)
        send_or_edit_categories_window(chat_id, text, reply_markup=kb, preferred_message_id=call.message.message_id)
        return True

    return False
    
def render_week_thu_wed_report(chat_id: int):
    store = get_chat_store(chat_id)

    ref_day = store.get("current_week_thu", today_key())
    start_key = week_start_thursday(ref_day)
    start, end = week_bounds_thu_wed(start_key)

    store["current_week_thu"] = start_key
    save_data(data)

    cats = calc_categories_for_period(store, start, end)

    lines = [
        f"📊 Расходы по статьям",
        f"🗓 {fmt_date_ddmmyy(start)} → {fmt_date_ddmmyy(end)} (Чт–Ср)",
        ""
    ]

    if not cats:
        lines.append("Нет расходов за период.")
    else:
        for cat, amt in sorted(cats.items()):
            lines.append(f"• {cat}: {fmt_num_plain(amt)}")

    return "\n".join(lines), start_key
@bot.callback_query_handler(func=lambda c: True)

def on_callback(call):
    try:
        bot.answer_callback_query(call.id)
    except Exception:
        pass

    try:
        data_str = call.data or ""
        chat_id = call.message.chat.id

        try:
            update_chat_info_from_message(call.message)
        except Exception:
            pass

        if guard_non_owner_finance_for_callback(chat_id, data_str):
            return

        if data_str == "dzv:close":
            try:
                bot.delete_message(chat_id, call.message.message_id)
            except Exception:
                pass
            return
        if data_str.startswith("dzv:"):
            try:
                target_chat_id = int(data_str.split(":", 1)[1])
            except Exception:
                return
            start_dozvon(chat_id, target_chat_id)
            safe_edit(bot, call, f"📞 Дозвон: {get_chat_display_name(target_chat_id)}", reply_markup=build_dozvon_menu(chat_id))
            return

        store = get_chat_store(chat_id)
        if call.message.message_id == store.get("balance_panel_id") and data_str != "bp:open":
            schedule_balance_panel_collapse(chat_id)

        if data_str == "bp:open":
            open_balance_panel_in_message(chat_id, call.message.message_id)
            try:
                bot.answer_callback_query(call.id, f"Остаток: {fmt_num(get_chat_store(chat_id).get('balance', 0))}")
            except Exception:
                pass
            return
        if data_str == "bp:collapse":
            collapse_balance_panel(chat_id)
            return

        if data_str == "rep_today":
            open_report_window(chat_id, now_local().strftime("%Y-%m"), call.message.message_id)
            return
    
        if data_str == "rep_close":
            try:
                bot.delete_message(chat_id, call.message.message_id)
            except Exception as e:
                log_error(f"rep_close delete failed: {e}")
            store = get_chat_store(chat_id)
            if store.get("report_window_id") == call.message.message_id:
                store["report_window_id"] = None
                store["report_month"] = None
                save_data(data)
            return
    
        if data_str.startswith("rep:"):
            month_key = data_str.split(":", 1)[1].strip()
            open_report_window(chat_id, month_key, call.message.message_id)
            return
        if data_str == "cat_months" or data_str.startswith("cat_"):
            if handle_categories_callback(call, data_str):
                return

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
                safe_edit(
                    bot,
                    call,
                    build_forward_status_text("Выберите чат A:"),
                    reply_markup=kb
                )
                return
            if data_str == "fw_back_root":
                owner_store = get_chat_store(int(OWNER_ID))
                day_key = owner_store.get("current_view_day", today_key())
                kb = build_forward_root_menu(day_key)
                safe_edit(
                    bot,
                    call,
                    build_forward_status_text("Меню пересылки:\nВыберите режим:"),
                    reply_markup=kb
                )
                return
            if data_str == "fw_back_src":
                kb = build_forward_source_menu()
                safe_edit(
                    bot,
                    call,
                    build_forward_status_text("Выберите чат A:"),
                    reply_markup=kb
                )
                return
            if data_str.startswith("fw_back_tgt:"):
                try:
                    A = int(data_str.split(":", 1)[1])
                except Exception:
                    return
                kb = build_forward_target_menu(A)
                safe_edit(
                    bot,
                    call,
                    build_forward_status_text(f"Источник: {get_chat_display_name(A)}\nВыберите чат B:"),
                    reply_markup=kb
                )
                return
            if data_str.startswith("fw_src:"):
                try:
                    A = int(data_str.split(":", 1)[1])
                except Exception:
                    return
                kb = build_forward_target_menu(A)
                safe_edit(
                    bot,
                    call,
                    build_forward_status_text(f"Источник: {get_chat_display_name(A)}\nВыберите чат B:"),
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
                safe_edit(
                    bot,
                    call,
                    build_forward_status_text(f"Настройка пересылки: {get_chat_display_name(A)} ⇄ {get_chat_display_name(B)}"),
                    reply_markup=kb
                )
                return
            if data_str.startswith("fw_finpair:"):
                parts = data_str.split(":")
                if len(parts) != 4:
                    return

                _, A_str, B_str, which = parts

                try:
                    A = int(A_str)
                    B = int(B_str)
                except Exception:
                    return

                if which == "ab":
                    set_forward_finance(A, B, not get_forward_finance(A, B))
                elif which == "ba":
                    set_forward_finance(B, A, not get_forward_finance(B, A))

                kb = build_forward_mode_menu(A, B)
                safe_edit(
                    bot,
                    call,
                    build_forward_status_text(f"Настройка пересылки: {get_chat_display_name(A)} ⇄ {get_chat_display_name(B)}"),
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

                if mode == "to":
                    if str(B) in (data.get("forward_rules", {}) or {}).get(str(A), {}):
                        remove_forward_link(A, B)
                    else:
                        add_forward_link(A, B, "oneway_to")
                elif mode == "from":
                    if str(A) in (data.get("forward_rules", {}) or {}).get(str(B), {}):
                        remove_forward_link(B, A)
                    else:
                        add_forward_link(B, A, "oneway_to")
                elif mode == "two":
                    fr = data.get("forward_rules", {}) or {}
                    ab_on = str(B) in fr.get(str(A), {})
                    ba_on = str(A) in fr.get(str(B), {})
                    if ab_on and ba_on:
                        remove_forward_link(A, B)
                        remove_forward_link(B, A)
                    else:
                        add_forward_link(A, B, "twoway")
                        add_forward_link(B, A, "twoway")
                elif mode == "del":
                    remove_forward_link(A, B)
                    remove_forward_link(B, A)

                kb = build_forward_mode_menu(A, B)
                safe_edit(
                    bot,
                    call,
                    build_forward_status_text(f"Настройка пересылки: {get_chat_display_name(A)} ⇄ {get_chat_display_name(B)}"),
                    reply_markup=kb
                )
                return
            return
        if data_str.startswith("c:"):
            center = data_str[2:]
            try:
                center_dt = datetime.strptime(center, "%Y-%m-%d")
            except Exception:
                center_dt = now_local()
        
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
        if data_str == "info_close":
            try:
                bot.delete_message(chat_id, call.message.message_id)
            except Exception as e:
                log_error(f"info_close delete failed: {e}")
            _clear_stored_window(chat_id, "info_msg_id", call.message.message_id)
            return
        if data_str.startswith("fv:"):
            try:
                _, target_s, view_day, action, owner_day_key = data_str.split(":", 4)
                target_chat_id = int(target_s)
            except Exception:
                return
            if action == "open":
                safe_edit(
                    bot,
                    call,
                    render_fin_window_text(target_chat_id, view_day),
                    reply_markup=build_fin_window_view_keyboard(target_chat_id, view_day, owner_day_key),
                    parse_mode="HTML"
                )
            return
        if not data_str.startswith("d:"):
            return
        _, day_key, cmd = data_str.split(":", 2)
        store = get_chat_store(chat_id)
        if cmd == "open":
            store["current_view_day"] = day_key
            if OWNER_ID and str(chat_id) == str(OWNER_ID):
                backup_window_for_owner(chat_id, day_key, None)
            else:
                txt, _ = render_day_window(chat_id, day_key)
                kb = build_main_keyboard(day_key, chat_id)
                safe_edit(bot, call, txt, reply_markup=kb, parse_mode="HTML")
                if is_quick_balance_main_open(chat_id, call.message.message_id):
                    schedule_balance_panel_collapse(chat_id)
                else:
                    set_active_window_id(chat_id, day_key, call.message.message_id)
            return
        if cmd == "prev":
            d = datetime.strptime(day_key, "%Y-%m-%d") - timedelta(days=1)
            nd = d.strftime("%Y-%m-%d")
            store["current_view_day"] = nd
            if OWNER_ID and str(chat_id) == str(OWNER_ID):
                backup_window_for_owner(chat_id, nd, None)
            else:
                txt, _ = render_day_window(chat_id, nd)
                kb = build_main_keyboard(nd, chat_id)
                safe_edit(bot, call, txt, reply_markup=kb, parse_mode="HTML")
                if is_quick_balance_main_open(chat_id, call.message.message_id):
                    schedule_balance_panel_collapse(chat_id)
                else:
                    set_active_window_id(chat_id, nd, call.message.message_id)
            return
        if cmd == "next":
            d = datetime.strptime(day_key, "%Y-%m-%d") + timedelta(days=1)
            nd = d.strftime("%Y-%m-%d")
            store["current_view_day"] = nd
            if OWNER_ID and str(chat_id) == str(OWNER_ID):
                backup_window_for_owner(chat_id, nd, None)
            else:
                txt, _ = render_day_window(chat_id, nd)
                kb = build_main_keyboard(nd, chat_id)
                safe_edit(bot, call, txt, reply_markup=kb, parse_mode="HTML")
                if is_quick_balance_main_open(chat_id, call.message.message_id):
                    schedule_balance_panel_collapse(chat_id)
                else:
                    set_active_window_id(chat_id, nd, call.message.message_id)
            return
        if cmd == "today":
            nd = today_key()
            store["current_view_day"] = nd
            if OWNER_ID and str(chat_id) == str(OWNER_ID):
                backup_window_for_owner(chat_id, nd, None)
            else:
                txt, _ = render_day_window(chat_id, nd)
                kb = build_main_keyboard(nd, chat_id)
                safe_edit(bot, call, txt, reply_markup=kb, parse_mode="HTML")
                if is_quick_balance_main_open(chat_id, call.message.message_id):
                    schedule_balance_panel_collapse(chat_id)
                else:
                    set_active_window_id(chat_id, nd, call.message.message_id)
            return
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
        if cmd == "report":
            try:
                month_key = datetime.strptime(day_key, "%Y-%m-%d").strftime("%Y-%m")
            except Exception:
                month_key = now_local().strftime("%Y-%m")
            open_report_window(chat_id, month_key)
            return
        if cmd == "total":
            open_total_window(chat_id)
            return
        if cmd == "info":
            try:
                bot.answer_callback_query(call.id)
            except Exception:
                pass
            open_info_window(chat_id)
            return
        if cmd in ("edit_menu", "menu"):

            store["current_view_day"] = day_key
            kb = build_edit_menu_keyboard(day_key, chat_id)
            txt, _ = render_day_window(chat_id, day_key)
            safe_edit(bot, call, txt, reply_markup=kb, parse_mode="HTML")
            return
        if cmd == "back_main":
            store["current_view_day"] = day_key
            if OWNER_ID and str(chat_id) == str(OWNER_ID):
                backup_window_for_owner(chat_id, day_key, None)
            else:
                txt, _ = render_day_window(chat_id, day_key)
                kb = build_main_keyboard(day_key, chat_id)
                safe_edit(bot, call, txt, reply_markup=kb, parse_mode="HTML")
                if is_quick_balance_main_open(chat_id, call.message.message_id):
                    schedule_balance_panel_collapse(chat_id)
                else:
                    set_active_window_id(chat_id, day_key, call.message.message_id)
            return
        if cmd == "csv_all":
            kb = build_csv_menu(day_key)
            txt, _ = render_day_window(chat_id, day_key)
            safe_edit(
                bot,
                call,
                txt,
                reply_markup=kb,
                parse_mode="HTML"
            )
            return
        if cmd == "csv_day":
            cmd_csv_day(chat_id, day_key)
            return
        if cmd == "csv_all_real":
            cmd_csv_all(chat_id)
            return

        if cmd == "csv_week":
            send_csv_week(chat_id, day_key)
            return

        if cmd == "csv_month":
            send_csv_month(chat_id, day_key)
            return

        if cmd == "csv_wedthu":
            send_csv_wedthu(chat_id, day_key)
            return
        if cmd == "reset":
            if not require_finance(chat_id):
                return
            store["reset_wait"] = True
            store["reset_time"] = time.time()
            save_data(data)
            send_or_edit_stored_window(
                chat_id,
                "reset_prompt_msg_id",
                "⚠️ Вы уверены, что хотите обнулить данные? Напишите ДА в течение 15 секунд.",
                delay=15
            )
            schedule_cancel_wait(chat_id, 15)
            return

        if cmd == "edit_list":
            day_recs = store.get("daily_records", {}).get(day_key, [])
            if not day_recs:
                send_and_auto_delete(chat_id, "Нет записей за этот день.")
                return
            kb2 = types.InlineKeyboardMarkup(row_width=3)
            for r in day_recs:
                lbl = f" {fmt_num(r['amount'])}"
                rid = r["id"]
                kb2.row(
                    types.InlineKeyboardButton(lbl, callback_data="none"),
                    types.InlineKeyboardButton("✏️", callback_data=f"d:{day_key}:edit_rec_{rid}"),
                    types.InlineKeyboardButton("❌", callback_data=f"d:{day_key}:del_rec_{rid}")
                )
            kb2.row(
                types.InlineKeyboardButton("🔙 Назад", callback_data=f"d:{day_key}:edit_menu")
            )
            txt, _ = render_day_window(chat_id, day_key)
            safe_edit(
                bot,
                call,
                txt,
                reply_markup=kb2,
                parse_mode="HTML"
            )
            return

        if cmd.startswith("edit_rec_"):
            rid = int(cmd.split("_")[-1])

            store = get_chat_store(chat_id)
            rec = next((r for r in store.get("records", []) if r["id"] == rid), None)
            if not rec:
                send_and_auto_delete(chat_id, "❌ Запись не найдена.")
                return

            store["edit_wait"] = {
                "type": "edit",
                "rid": rid,
                "day_key": day_key,
            }
            save_data(data)

            text = (
                f"✏️ Редактирование записи R{rid}\n\n"
                f"Текущие данные:\n"
                f"{fmt_num(rec['amount'])} {rec.get('note','')}\n\n"
                f"✍️ Напишите новые данные.\n\n"
                f"⏳ Это сообщение будет удалено через 40 секунд,\n"
                f"если изменений не будет — редактирование отменится."
            )
            kb = build_cancel_edit_keyboard(day_key)

            prompt_id = send_or_edit_stored_window(
                chat_id,
                "edit_prompt_msg_id",
                text,
                reply_markup=kb,
                delay=40
            )

            schedule_cancel_edit(chat_id, prompt_id, delay=40)

            return
        if cmd.startswith("del_rec_"):
            rid = int(cmd.split("_")[-1])
            delete_record_in_chat(chat_id, rid)
            schedule_finalize(chat_id, day_key)
            send_and_auto_delete(chat_id, f"🗑 Запись R{rid} удалена.", 10)
            return

        if cmd == "forward_menu":
            if not OWNER_ID or str(chat_id) != str(OWNER_ID):
                send_and_auto_delete(chat_id, "Меню доступно только владельцу.", HELPER_DELETE_DELAY)
                return
            kb = build_forward_root_menu(day_key)
            safe_edit(
                bot,
                call,
                build_forward_status_text("Меню пересылки:\nВыберите режим:"),
                reply_markup=kb
            )
            return
        if cmd == "forward_old":
            if not OWNER_ID or str(chat_id) != str(OWNER_ID):
                send_and_auto_delete(chat_id, "Меню доступно только владельцу.", HELPER_DELETE_DELAY)
                return
            kb = build_forward_source_menu()
            safe_edit(
                bot,
                call,
                build_forward_status_text("Выберите чат A:"),
                reply_markup=kb
            )
            return
        if cmd == "forward_finmode_menu":
            kb = build_finance_toggle_chat_menu(day_key)
            safe_edit(
                bot,
                call,
                build_forward_status_text("Выберите чат для переключения финансового режима:"),
                reply_markup=kb
            )
            return
        if cmd == "quick_balance_menu":
            kb = build_quick_balance_chat_menu(day_key)
            safe_edit(
                bot,
                call,
                build_forward_status_text("Быстрый остаток:\nВыберите чат для включения или выключения режима."),
                reply_markup=kb
            )
            return
        if cmd == "fin_windows_menu":
            kb = build_fin_windows_chat_menu(day_key)
            safe_edit(
                bot,
                call,
                "🪟 Фин окна чатов\nВыберите чат для просмотра операций:",
                reply_markup=kb
            )
            return
        if cmd.startswith("finwin_open_"):
            tgt = int(cmd.split("_")[-1])
            target_store = get_chat_store(tgt)
            view_day = target_store.get("current_view_day", today_key())
            safe_edit(
                bot,
                call,
                render_fin_window_text(tgt, view_day),
                reply_markup=build_fin_window_view_keyboard(tgt, view_day, day_key),
                parse_mode="HTML"
            )
            return
        if cmd.startswith("qb_cfg_"):
            tgt = int(cmd.split("_")[-1])
            kb = build_quick_balance_mode_menu(day_key, tgt)
            safe_edit(
                bot,
                call,
                f"Быстрый остаток:\n{get_chat_display_name(tgt)}\n\nВыберите режим:",
                reply_markup=kb
            )
            return
        if cmd.startswith("qb_mode_normal_"):
            tgt = int(cmd.split("_")[-1])
            if is_quick_balance_enabled(tgt) and get_quick_balance_behavior(tgt) != "open":
                set_quick_balance_enabled(tgt, False)
            else:
                set_quick_balance_behavior(tgt, "mini")
                set_quick_balance_enabled(tgt, True)
            if OWNER_ID and str(tgt) != str(OWNER_ID):
                refresh_owner_after_chat_change(tgt)
            kb = build_quick_balance_chat_menu(day_key)
            safe_edit(
                bot,
                call,
                build_forward_status_text("Быстрый остаток:\nВыберите чат для настройки режима."),
                reply_markup=kb
            )
            return
        if cmd.startswith("qb_mode_open_"):
            tgt = int(cmd.split("_")[-1])
            if is_quick_balance_enabled(tgt) and get_quick_balance_behavior(tgt) == "open":
                set_quick_balance_enabled(tgt, False)
            else:
                set_quick_balance_behavior(tgt, "open")
                set_quick_balance_enabled(tgt, True)
            if OWNER_ID and str(tgt) != str(OWNER_ID):
                refresh_owner_after_chat_change(tgt)
            kb = build_quick_balance_chat_menu(day_key)
            safe_edit(
                bot,
                call,
                build_forward_status_text("Быстрый остаток:\nВыберите чат для настройки режима."),
                reply_markup=kb
            )
            return
        if cmd.startswith("fw_finmode_pick_"):
            tgt = int(cmd.split("_")[-1])
            new_state = not is_finance_mode(tgt)
            set_finance_mode(tgt, new_state)
            save_data(data)
            kb = build_finance_toggle_chat_menu(day_key)
            safe_edit(
                bot,
                call,
                build_forward_status_text("Выберите чат для переключения финансового режима:"),
                reply_markup=kb
            )
            return
        if cmd.startswith("fw_cfg_"):
            tgt = int(cmd.split("_")[-1])
            kb = build_forward_direction_menu(day_key, chat_id, tgt)
            safe_edit(
                bot,
                call,
                build_forward_status_text(f"Настройка пересылки для {get_chat_display_name(tgt)}:"),
                reply_markup=kb
            )
            return
        if cmd.startswith("fw_fin_ab_"):
            tgt = int(cmd.split("_")[-1])
            set_forward_finance(chat_id, tgt, not get_forward_finance(chat_id, tgt))
            kb = build_forward_direction_menu(day_key, chat_id, tgt)
            safe_edit(
                bot,
                call,
                build_forward_status_text(f"Настройка пересылки для {get_chat_display_name(tgt)}:"),
                reply_markup=kb
            )
            return

        if cmd.startswith("fw_fin_ba_"):
            tgt = int(cmd.split("_")[-1])
            set_forward_finance(tgt, chat_id, not get_forward_finance(tgt, chat_id))
            kb = build_forward_direction_menu(day_key, chat_id, tgt)
            safe_edit(
                bot,
                call,
                build_forward_status_text(f"Настройка пересылки для {get_chat_display_name(tgt)}:"),
                reply_markup=kb
            )
            return
        if cmd.startswith("fw_one_"):
            tgt = int(cmd.split("_")[-1])
            fr = data.get("forward_rules", {}) or {}
            if str(tgt) in fr.get(str(chat_id), {}):
                remove_forward_link(chat_id, tgt)
            else:
                add_forward_link(chat_id, tgt, "oneway_to")
            kb = build_forward_direction_menu(day_key, chat_id, tgt)
            safe_edit(
                bot,
                call,
                build_forward_status_text(f"Настройка пересылки для {get_chat_display_name(tgt)}:"),
                reply_markup=kb
            )
            return
        if cmd.startswith("fw_rev_"):
            tgt = int(cmd.split("_")[-1])
            fr = data.get("forward_rules", {}) or {}
            if str(chat_id) in fr.get(str(tgt), {}):
                remove_forward_link(tgt, chat_id)
            else:
                add_forward_link(tgt, chat_id, "oneway_to")
            kb = build_forward_direction_menu(day_key, chat_id, tgt)
            safe_edit(
                bot,
                call,
                build_forward_status_text(f"Настройка пересылки для {get_chat_display_name(tgt)}:"),
                reply_markup=kb
            )
            return
        if cmd.startswith("fw_two_"):
            tgt = int(cmd.split("_")[-1])
            fr = data.get("forward_rules", {}) or {}
            ab_on = str(tgt) in fr.get(str(chat_id), {})
            ba_on = str(chat_id) in fr.get(str(tgt), {})
            if ab_on and ba_on:
                remove_forward_link(chat_id, tgt)
                remove_forward_link(tgt, chat_id)
            else:
                add_forward_link(chat_id, tgt, "twoway")
                add_forward_link(tgt, chat_id, "twoway")
            kb = build_forward_direction_menu(day_key, chat_id, tgt)
            safe_edit(
                bot,
                call,
                build_forward_status_text(f"Настройка пересылки для {get_chat_display_name(tgt)}:"),
                reply_markup=kb
            )
            return
        if cmd.startswith("fw_del_"):
            tgt = int(cmd.split("_")[-1])
            remove_forward_link(chat_id, tgt)
            remove_forward_link(tgt, chat_id)
            kb = build_forward_direction_menu(day_key, chat_id, tgt)
            safe_edit(
                bot,
                call,
                build_forward_status_text(f"Настройка пересылки для {get_chat_display_name(tgt)}:"),
                reply_markup=kb
            )
            return
        if cmd == "pick_date":
            send_or_edit_stored_window(
                chat_id,
                "wait_date_msg_id",
                "Введите дату:\n/view YYYY-MM-DD",
                delay=40
            )
            return
        if cmd == "cancel_edit":
            store = get_chat_store(chat_id)
            store["edit_wait"] = None
            save_data(data)
            delete_stored_window_if_exists(chat_id, "edit_prompt_msg_id")
            try:
                bot.delete_message(chat_id, call.message.message_id)
            except Exception:
                pass
            send_and_auto_delete(chat_id, "❎ Редактирование отменено.", 5)
            return
    except Exception as e:
        log_error(f"on_callback error: {e}")
def send_csv_week(chat_id: int, day_key: str):
    try:
        store = get_chat_store(chat_id)

        base = datetime.strptime(day_key, "%Y-%m-%d")
        start = base - timedelta(days=6)

        rows = []

        for i in range(7):
            d = (start + timedelta(days=i)).strftime("%Y-%m-%d")
            for r in store.get("daily_records", {}).get(d, []):
                rows.append((d, fmt_csv_amount(r["amount"]), r.get("note", "")))

        if not rows:
            send_info(chat_id, "Нет данных за неделю")
            return

        tmp = f"week_{chat_id}.csv"

        with open(tmp, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["date", "amount", "note"])
            rows.sort(key=lambda row: str(row[0]))
            write_csv_rows_with_day_gaps(w, rows, 3)

        with open(tmp, "rb") as f:
            bot.send_document(chat_id, f, caption="🗓 CSV за неделю")

    except Exception as e:
        log_error(f"send_csv_week: {e}")
def send_csv_month(chat_id: int, day_key: str):
    try:
        store = get_chat_store(chat_id)

        base = datetime.strptime(day_key, "%Y-%m-%d")
        start = base.replace(day=1)

        rows = []

        for d, recs in store.get("daily_records", {}).items():
            dt = datetime.strptime(d, "%Y-%m-%d")
            if dt >= start and dt <= base:
                for r in recs:
                    rows.append((d, fmt_csv_amount(r["amount"]), r.get("note", "")))

        if not rows:
            send_info(chat_id, "Нет данных за месяц")
            return

        tmp = f"month_{chat_id}.csv"

        with open(tmp, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["date", "amount", "note"])
            rows.sort(key=lambda row: str(row[0]))
            write_csv_rows_with_day_gaps(w, rows, 3)

        with open(tmp, "rb") as f:
            bot.send_document(chat_id, f, caption="📆 CSV за месяц")

    except Exception as e:
        log_error(f"send_csv_month: {e}")
def send_csv_wedthu(chat_id: int, day_key: str):
    try:
        store = get_chat_store(chat_id)

        base = datetime.strptime(day_key, "%Y-%m-%d")

        while base.weekday() != 2:
            base -= timedelta(days=1)

        start = base

        rows = []

        for i in range(2):
            d = (start + timedelta(days=i)).strftime("%Y-%m-%d")
            for r in store.get("daily_records", {}).get(d, []):
                rows.append((d, fmt_csv_amount(r["amount"]), r.get("note", "")))

        if not rows:
            send_info(chat_id, "Нет данных Ср–Чт")
            return

        tmp = f"wedthu_{chat_id}.csv"

        with open(tmp, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["date", "amount", "note"])
            rows.sort(key=lambda row: str(row[0]))
            write_csv_rows_with_day_gaps(w, rows, 3)

        with open(tmp, "rb") as f:
            bot.send_document(chat_id, f, caption="📊 CSV Ср–Чт")

    except Exception as e:
        log_error(f"send_csv_wedthu: {e}")

def add_record_to_chat(
    chat_id: int,
    amount: float,
    note: str,
    owner: int,
    source_msg=None,
    day_key=None
):
    store = get_chat_store(chat_id)
    rid = store.get("next_id", 1)

    if not day_key:
        day_key = day_key_from_message(source_msg)

    rec = {
        "id": rid,
        "short_id": "",
        "timestamp": now_local().isoformat(timespec="seconds"),
        "amount": amount,
        "note": note,
        "source_msg_id": source_msg.message_id if source_msg else None,
        "owner": owner,
        "msg_id": source_msg.message_id if source_msg else None,
        "origin_msg_id": source_msg.message_id if source_msg else None,
        "day_key": day_key,
    }

    store.setdefault("records", []).append(rec)
    store.setdefault("daily_records", {}).setdefault(day_key, []).append(rec)

    store["next_id"] = rid + 1
    store["balance"] = sum(r["amount"] for r in store["records"])

    rebuild_month_short_ids(chat_id)
    rebuild_global_records()

def delete_record_in_chat(chat_id: int, rid: int):
    store = get_chat_store(chat_id)

    store["records"] = [x for x in store["records"] if x["id"] != rid]

    for day, arr in list(store.get("daily_records", {}).items()):
        arr2 = [x for x in arr if x["id"] != rid]
        if arr2:
            store["daily_records"][day] = arr2
        else:
            del store["daily_records"][day]

    renumber_chat_records(chat_id)
    store["balance"] = sum(x["amount"] for x in store["records"])
    rebuild_global_records()

def renumber_chat_records(chat_id: int):
    """
    Перенумеровывает внутренние id по реальному порядку:
      • сортируем по day_key и timestamp
      • id = 1,2,3... по всему чату
      • short_id = R1,R2,... заново в каждом месяце
      • обновляем store["records"] и next_id
    """
    store = get_chat_store(chat_id)
    daily = store.get("daily_records", {}) or {}

    all_recs = []
    for dk in sorted(daily.keys()):
        recs = daily.get(dk, [])
        recs_sorted = sorted(recs, key=lambda r: r.get("timestamp", ""))
        daily[dk] = recs_sorted
        for r in recs_sorted:
            all_recs.append(r)

    new_id = 1
    for r in all_recs:
        r["id"] = new_id
        new_id += 1

    store["records"] = list(all_recs)
    store["next_id"] = new_id

    rebuild_month_short_ids(chat_id)
    
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
    if OWNER_ID and str(chat_id) == str(OWNER_ID):
        backup_window_for_owner(chat_id, day_key)
        return

    lock = window_locks[(chat_id, day_key)]

    with lock:
        txt, _ = render_day_window(chat_id, day_key)
        kb = build_main_keyboard(day_key, chat_id)

        old_mid = get_active_window_id(chat_id, day_key)

        if len(txt) > 3900:
            log_error(f"update_or_send_day_window: text too long for {chat_id} {day_key}, len={len(txt)}")

        if old_mid:
            try:
                bot.edit_message_text(
                    txt,
                    chat_id=chat_id,
                    message_id=old_mid,
                    reply_markup=kb,
                    parse_mode="HTML"
                )
                set_active_window_id(chat_id, day_key, old_mid)
                return
            except Exception as e:
                err = str(e).lower()
                if "message is not modified" in err:
                    return
                try:
                    bot.delete_message(chat_id, old_mid)
                except Exception:
                    pass

        sent = bot.send_message(
            chat_id,
            txt,
            reply_markup=kb,
            parse_mode="HTML"
        )
        set_active_window_id(chat_id, day_key, sent.message_id)
    schedule_balance_panel_refresh(chat_id, 0.5)
def is_finance_mode(chat_id):

    store = get_chat_store(chat_id)

    if str(chat_id) == str(OWNER_ID):
        return True

    return store.get("finance_mode", False)

def set_finance_mode(chat_id: int, enabled: bool):
    chat_id = int(chat_id)
    store = get_chat_store(chat_id)
    enabled = bool(enabled)
    store["finance_mode"] = enabled

    if enabled:
        finance_active_chats.add(chat_id)
    else:
        finance_active_chats.discard(chat_id)
        store.setdefault("settings", {})["quick_balance_enabled"] = False
        panel_id = store.get("balance_panel_id")
        if panel_id:
            try:
                bot.delete_message(chat_id, panel_id)
            except Exception:
                pass
        store["balance_panel_id"] = None
        store["balance_panel_mode"] = "mini"
        clear_chat_active_windows(chat_id)
    save_data(data)

def require_finance(chat_id: int) -> bool:
    """
    Проверка: включён ли финансовый режим.
    Если нет — показываем подсказку /поехали.
    """
    if not is_finance_mode(chat_id):
        send_and_auto_delete(chat_id, "⚙️ Финансовый режим выключен.\nАктивируйте командой /ok")
        return False
    return True
def build_total_window_text(chat_id: int) -> str:
    store = get_chat_store(chat_id)
    chat_bal = store.get("balance", 0)

    if not OWNER_ID or str(chat_id) != str(OWNER_ID):
        return f"💰 Общий итог по этому чату: {fmt_num(chat_bal)}"

    lines = []
    info = store.get("info", {})
    title = info.get("title") or f"Чат {chat_id}"
    lines.append("💰 Общий итог (для владельца)")
    lines.append("")
    lines.append(f"• Этот чат ({title}): {fmt_num(chat_bal)}")

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
    lines.append(f"• Всего по всем чатам: {fmt_num(total_all)}")
    return "\n".join(lines)


def open_total_window(chat_id: int):
    store = get_chat_store(chat_id)
    text = build_total_window_text(chat_id)
    message_id = store.get("total_msg_id")

    if message_id:
        try:
            bot.edit_message_text(
                text,
                chat_id=chat_id,
                message_id=message_id,
                parse_mode="HTML"
            )
            save_data(data)
            if is_owner_chat(chat_id):
                schedule_owner_total_window_delete(chat_id, message_id)
            return message_id
        except Exception as e:
            log_error(f"open_total_window edit failed for chat {chat_id}: {e}")
            _clear_stored_window(chat_id, "total_msg_id", message_id)

    sent = bot.send_message(chat_id, text, parse_mode="HTML")
    store["total_msg_id"] = sent.message_id
    save_data(data)
    if is_owner_chat(chat_id):
        schedule_owner_total_window_delete(chat_id, sent.message_id)
    return sent.message_id


def refresh_total_message_if_any(chat_id: int):
    """
    Если в чате есть активное сообщение '💰 Общий итог',
    пересчитывает и обновляет его текст.
    """
    store = get_chat_store(chat_id)
    msg_id = store.get("total_msg_id")
    if not msg_id:
        return
    try:
        open_total_window(chat_id)
    except Exception as e:
        log_error(f"refresh_total_message_if_any({chat_id}): {e}")
        store["total_msg_id"] = None
        save_data(data)
def refresh_owner_after_chat_change(source_chat_id: int):
    if not OWNER_ID:
        return
    try:
        owner_chat_id = int(OWNER_ID)
    except Exception:
        return
    if int(source_chat_id) == owner_chat_id:
        return

    try:
        owner_store = get_chat_store(owner_chat_id)
        owner_day_key = owner_store.get("current_view_day", today_key())
        if is_quick_balance_enabled(owner_chat_id):
            refresh_balance_panel_now(owner_chat_id)
        else:
            backup_window_for_owner(owner_chat_id, owner_day_key, None)
        refresh_total_message_if_any(owner_chat_id)
    except Exception as e:
        log_error(f"refresh_owner_after_chat_change({source_chat_id}): {e}")


def send_info(chat_id: int, text: str):
    send_and_auto_delete(chat_id, text, HELPER_DELETE_DELAY)
                
@bot.message_handler(commands=["ok", "поехали"])
def cmd_ok(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass

    schedule_command_delete(msg)
    chat_id = msg.chat.id
    stop_dozvon_for_target(chat_id)
    store = get_chat_store(chat_id)

    set_finance_mode(chat_id, True)
    store["current_view_day"] = today_key()
    store.setdefault("settings", {})["auto_add"] = True

    save_data(data)
    schedule_finalize(chat_id, today_key())
    schedule_balance_panel_refresh(chat_id, 0.1)

    send_and_auto_delete(chat_id, "✅ Финансовый режим включён", HELPER_DELETE_DELAY)
@bot.message_handler(commands=["start"])
def cmd_start(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass

    schedule_command_delete(msg)
    chat_id = msg.chat.id
    stop_dozvon_for_target(chat_id)

    store = get_chat_store(chat_id)
    set_finance_mode(chat_id, True)
    store.setdefault("settings", {})["auto_add"] = True
    store["current_view_day"] = today_key()
    save_data(data)

    update_or_send_day_window(chat_id, today_key())
    schedule_balance_panel_refresh(chat_id, 0.1)
@bot.message_handler(commands=["start_new"])
def cmd_start_new(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass

    schedule_command_delete(msg)
    chat_id = msg.chat.id
    stop_dozvon_for_target(chat_id)

    store = get_chat_store(chat_id)
    set_finance_mode(chat_id, True)
    store.setdefault("settings", {})["auto_add"] = True

    day_key = today_key()
    store["current_view_day"] = day_key

    old_mid = get_active_window_id(chat_id, day_key)
    if old_mid:
        try:
            bot.delete_message(chat_id, old_mid)
        except Exception:
            pass

    set_active_window_id(chat_id, day_key, None)

    if OWNER_ID and str(chat_id) == str(OWNER_ID):
        backup_window_for_owner(chat_id, day_key, None)
        schedule_balance_panel_refresh(chat_id, 0.1)
        return

    txt, _ = render_day_window(chat_id, day_key)
    kb = build_main_keyboard(day_key, chat_id)
    sent = bot.send_message(
        chat_id,
        txt,
        reply_markup=kb,
        parse_mode="HTML"
    )

    set_active_window_id(chat_id, day_key, sent.message_id)
    schedule_balance_panel_refresh(chat_id, 0.1)
@bot.message_handler(commands=["help"])
def cmd_help(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass
    schedule_command_delete(msg)
    chat_id = msg.chat.id
    stop_dozvon_for_target(chat_id)
    help_text = build_help_text(chat_id)
    send_and_auto_delete(chat_id, help_text, HELPER_DELETE_DELAY)
    
@bot.message_handler(commands=["restore"])
def cmd_restore(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass

    schedule_command_delete(msg)
    if guard_non_owner_finance_for_command(msg, {"ok", "help"}):
        return
    stop_dozvon_for_target(msg.chat.id)

    global restore_mode
    restore_mode = msg.chat.id  # включаем только для текущего чата
    cleanup_forward_links(msg.chat.id)
    send_and_auto_delete(
        msg.chat.id,
        "📥 Режим восстановления включён.\n"
        "Отправьте JSON/CSV файл для восстановления."
    )
    
@bot.message_handler(commands=["restore_off"])
def cmd_restore_off(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass

    schedule_command_delete(msg)
    if guard_non_owner_finance_for_command(msg, {"ok", "help"}):
        return
    stop_dozvon_for_target(msg.chat.id)

    global restore_mode
    restore_mode = None  # выключаем
    cleanup_forward_links(msg.chat.id)
    send_and_auto_delete(msg.chat.id, "🔒 Режим восстановления выключен.")
@bot.message_handler(commands=["ping"])
def cmd_ping(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass

    schedule_command_delete(msg)
    if guard_non_owner_finance_for_command(msg, {"ok", "help"}):
        return
    stop_dozvon_for_target(msg.chat.id)
    send_and_auto_delete(msg.chat.id, "PONG — бот работает 🟢", HELPER_DELETE_DELAY)
@bot.message_handler(commands=["view"])
def cmd_view(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass

    schedule_command_delete(msg)
    chat_id = msg.chat.id
    stop_dozvon_for_target(chat_id)
    if guard_non_owner_finance_for_command(msg, {"ok", "help"}):
        return

    store = get_chat_store(chat_id)
    msg_id = store.get("wait_date_msg_id")
    if msg_id:
        try:
            bot.delete_message(chat_id, msg_id)
        except Exception:
            pass
        store["wait_date_msg_id"] = None
        save_data(data)

    if not require_finance(chat_id):
        return
    parts = (msg.text or "").split()
    if len(parts) < 2:
        send_info(chat_id, "Использование: /view YYYY-MM-DD")
        schedule_command_delete(msg)
        return
    day_key = parts[1]
    try:
        datetime.strptime(day_key, "%Y-%m-%d")
    except ValueError:
        send_info(chat_id, "❌ Неверная дата. Формат: YYYY-MM-DD")
        return
    store["current_view_day"] = day_key
    update_or_send_day_window(chat_id, day_key)
@bot.message_handler(commands=["prev"])
def cmd_prev(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass

    schedule_command_delete(msg)
    chat_id = msg.chat.id
    stop_dozvon_for_target(chat_id)
    if guard_non_owner_finance_for_command(msg, {"ok", "help"}):
        return
    if not require_finance(chat_id):
        return
    d = datetime.strptime(today_key(), "%Y-%m-%d") - timedelta(days=1)
    day_key = d.strftime("%Y-%m-%d")
    get_chat_store(chat_id)["current_view_day"] = day_key
    update_or_send_day_window(chat_id, day_key)
@bot.message_handler(commands=["next"])
def cmd_next(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass

    schedule_command_delete(msg)
    chat_id = msg.chat.id
    stop_dozvon_for_target(chat_id)
    if guard_non_owner_finance_for_command(msg, {"ok", "help"}):
        return
    if not require_finance(chat_id):
        return
    d = datetime.strptime(today_key(), "%Y-%m-%d") + timedelta(days=1)
    day_key = d.strftime("%Y-%m-%d")
    get_chat_store(chat_id)["current_view_day"] = day_key
    update_or_send_day_window(chat_id, day_key)
@bot.message_handler(commands=["balance"])
def cmd_balance(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass

    schedule_command_delete(msg)
    chat_id = msg.chat.id
    stop_dozvon_for_target(chat_id)
    if guard_non_owner_finance_for_command(msg, {"ok", "help"}):
        return
    if not require_finance(chat_id):
        return
    store = get_chat_store(chat_id)
    bal = store.get("balance", 0)
    send_info(chat_id, f"💰 Баланс: {fmt_num(bal)}")
@bot.message_handler(commands=["report"])
def cmd_report(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass

    schedule_command_delete(msg)
    chat_id = msg.chat.id
    stop_dozvon_for_target(chat_id)
    if guard_non_owner_finance_for_command(msg, {"ok", "help"}):
        return
    if not require_finance(chat_id):
        return

    lines = build_day_report_lines(chat_id)
    report_html = "<pre>" + html.escape("\n".join(lines)) + "</pre>"
    send_html_and_auto_delete(chat_id, report_html, 20)
def cmd_csv_all(chat_id: int):
    """
    Общий CSV этого чата (все дни этого чата).
    """
    if not require_finance(chat_id):
        return
    try:
        save_chat_json(chat_id)
        path = chat_csv_file(chat_id)
        if not os.path.exists(path):
            send_info(chat_id, "CSV файла ещё нет.")
            return
        with open(path, "rb") as f:
            bot.send_document(
                chat_id,
                f,
                caption=f"📂 Общий CSV всех операций чата {chat_id}"
            )
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
            rows = []
            for r in day_recs:
                rows.append((
                    day_key,
                    chat_id,
                    r.get("id"),
                    r.get("short_id"),
                    r.get("timestamp"),
                    fmt_csv_amount(r.get("amount")),
                    r.get("note"),
                    r.get("owner"),
                    day_key,
                ))
            write_csv_rows_with_day_gaps(w, [row[1:] for row in rows], 8)
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
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass

    """
    Экспортирует CSV текущего чата.
    """
    schedule_command_delete(msg)
    chat_id = msg.chat.id
    stop_dozvon_for_target(chat_id)
    if guard_non_owner_finance_for_command(msg, {"ok", "help"}):
        return
    if not require_finance(chat_id):
        return
    export_global_csv(data)
    save_chat_json(chat_id)
    per_csv = chat_csv_file(chat_id)
    sent = None
    if os.path.exists(per_csv):
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
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass

    schedule_command_delete(msg)
    chat_id = msg.chat.id
    stop_dozvon_for_target(chat_id)
    if guard_non_owner_finance_for_command(msg, {"ok", "help"}):
        return
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
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass

    schedule_command_delete(msg)
    chat_id = msg.chat.id
    stop_dozvon_for_target(chat_id)
    if guard_non_owner_finance_for_command(msg, {"ok", "help"}):
        return
    if not require_finance(chat_id):
        return
    store = get_chat_store(chat_id)
    store["reset_wait"] = True
    store["reset_time"] = time.time()
    save_data(data)
    send_or_edit_stored_window(
        chat_id,
        "reset_prompt_msg_id",
        "⚠️ Вы уверены, что хотите обнулить данные? Напишите ДА в течение 15 секунд.",
        delay=15
    )
    schedule_cancel_wait(chat_id, 15)

@bot.message_handler(commands=["stopforward"])
def cmd_stopforward(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass

    schedule_command_delete(msg)
    chat_id = msg.chat.id
    stop_dozvon_for_target(chat_id)
    if guard_non_owner_finance_for_command(msg, {"ok", "help"}):
        return
    if str(chat_id) != str(OWNER_ID):
        send_info(chat_id, "Эта команда только для владельца.")
        schedule_command_delete(msg)
        return
    clear_forward_all()
    send_info(chat_id, "Пересылка полностью отключена.")
@bot.message_handler(commands=["backup_channel_on"])
def cmd_on_channel(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass

    schedule_command_delete(msg)
    chat_id = msg.chat.id
    stop_dozvon_for_target(chat_id)
    if guard_non_owner_finance_for_command(msg, {"ok", "help"}):
        return
    if not is_owner_chat(chat_id):
        send_and_auto_delete(chat_id, "Эта команда только для владельца.", HELPER_DELETE_DELAY)
        return
    backup_flags["channel"] = True
    save_data(data)
    send_info(chat_id, "📡 Бэкап в канал включён")
@bot.message_handler(commands=["backup_channel_off"])
def cmd_off_channel(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass

    schedule_command_delete(msg)
    chat_id = msg.chat.id
    stop_dozvon_for_target(chat_id)
    if guard_non_owner_finance_for_command(msg, {"ok", "help"}):
        return
    if not is_owner_chat(chat_id):
        send_and_auto_delete(chat_id, "Эта команда только для владельца.", HELPER_DELETE_DELAY)
        return
    backup_flags["channel"] = False
    save_data(data)
    send_info(chat_id, "📡 Бэкап в канал выключен")
    
@bot.message_handler(commands=["dozvon"])
def cmd_dozvon(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass

    schedule_command_delete(msg)
    chat_id = msg.chat.id
    stop_dozvon_for_target(chat_id)
    if guard_non_owner_finance_for_command(msg, {"ok", "help"}):
        return

    connected = get_connected_chat_ids(chat_id)
    if not connected:
        send_and_auto_delete(chat_id, "📞 Нет связанных чатов для дозвона.", HELPER_DELETE_DELAY)
        return

    bot.send_message(
        chat_id,
        "📞 Выберите чат для дозвона:",
        reply_markup=build_dozvon_menu(chat_id)
    )

@bot.message_handler(commands=["autoadd_info", "autoadd.info"])
def cmd_autoadd_info(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass

    schedule_command_delete(msg)
    chat_id = msg.chat.id
    stop_dozvon_for_target(chat_id)
    if guard_non_owner_finance_for_command(msg, {"ok", "help"}):
        return

    if OWNER_ID and str(chat_id) == str(OWNER_ID):
        send_and_auto_delete(
            chat_id,
            "⚙️ Авто-добавление у владельца ВСЕГДА включено.\n\n"
            "Сообщения с суммами автоматически записываются.",
            10
        )
        return

    store = get_chat_store(chat_id)
    settings = store.setdefault("settings", {})

    current = settings.get("auto_add", True)
    new_state = not current
    settings["auto_add"] = new_state

    save_data(data)
    save_chat_json(chat_id)

    send_and_auto_delete(
        chat_id,
        f"⚙️ Авто-добавление сообщений: "
        f"{'ВКЛЮЧЕНО ✅' if new_state else 'ВЫКЛЮЧЕНО ❌'}\n\n"
        "Использование:\n"
        "• ВКЛ → каждое сообщение с суммой записывается автоматически\n"
        "• ВЫКЛ → сообщения с суммами не записываются",
        12
    )
def send_and_auto_delete(chat_id: int, text: str, delay: int = HELPER_DELETE_DELAY):
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


def send_html_and_auto_delete(chat_id: int, html_text: str, delay: int = HELPER_DELETE_DELAY):
    try:
        msg = bot.send_message(chat_id, html_text, parse_mode="HTML")
        def _delete():
            time.sleep(delay)
            try:
                bot.delete_message(chat_id, msg.message_id)
            except Exception:
                pass
        threading.Thread(target=_delete, daemon=True).start()
    except Exception as e:
        log_error(f"send_html_and_auto_delete: {e}")
def delete_message_later(chat_id: int, message_id: int, delay: int = 30):
    """
    Отложенное удаление сообщения пользователя (например, команд).
    """
    try:
        def _job():
            time.sleep(delay)
            try:
                bot.delete_message(chat_id, message_id)
            except Exception:
                pass
        threading.Thread(target=_job, daemon=True).start()
    except Exception as e:
        log_error(f"delete_message_later: {e}")
_edit_cancel_timers = {}
        
def schedule_cancel_wait(chat_id: int, delay: float = 15.0):
    """
    Через delay секунд сбрасывает флаг reset_wait,
    если он всё ещё активен.
    """
    def _job():
        try:
            store = get_chat_store(chat_id)
            changed = False

            if store.get("reset_wait", False):
                store["reset_wait"] = False
                store["reset_time"] = 0
                changed = True

            if changed:
                save_data(data)
            delete_stored_window_if_exists(chat_id, "reset_prompt_msg_id")
        except Exception as e:
            log_error(f"schedule_cancel_wait job: {e}")

    prev = _edit_cancel_timers.get((chat_id, "reset"))
    if prev and prev.is_alive():
        try:
            prev.cancel()
        except Exception:
            pass

    t = threading.Timer(delay, _job)
    _edit_cancel_timers[(chat_id, "reset")] = t
    t.start()


def schedule_cancel_edit(chat_id: int, prompt_message_id: int | None = None, delay: float = 40.0):
    def _job():
        try:
            store = get_chat_store(chat_id)
            if store.get("edit_wait"):
                store["edit_wait"] = None
                save_data(data)
            delete_stored_window_if_exists(chat_id, "edit_prompt_msg_id", prompt_message_id)
        except Exception as e:
            log_error(f"schedule_cancel_edit({chat_id}): {e}")

    prev = _edit_cancel_timers.get((chat_id, "edit"))
    if prev and prev.is_alive():
        try:
            prev.cancel()
        except Exception:
            pass

    t = threading.Timer(delay, _job)
    _edit_cancel_timers[(chat_id, "edit")] = t
    t.start()


def update_chat_info_from_message(msg):
    """
    Обновляет информацию о чате в памяти.
    На диск пишем только если реально что-то изменилось.
    """
    chat_id = msg.chat.id
    try:
        if not getattr(getattr(msg, "from_user", None), "is_bot", False):
            stop_dozvon_for_target(chat_id)
    except Exception:
        pass
    store = get_chat_store(chat_id)
    info = store.setdefault("info", {})

    changed = False

    new_title = msg.chat.title or info.get("title") or f"Чат {chat_id}"
    new_username = msg.chat.username or info.get("username")
    new_type = msg.chat.type

    if info.get("title") != new_title:
        info["title"] = new_title
        changed = True

    if info.get("username") != new_username:
        info["username"] = new_username
        changed = True

    if info.get("type") != new_type:
        info["type"] = new_type
        changed = True

    if OWNER_ID and str(chat_id) != str(OWNER_ID):
        owner_store = get_chat_store(int(OWNER_ID))
        kc = owner_store.setdefault("known_chats", {})

        new_known = {
            "title": info["title"],
            "username": info["username"],
            "type": info["type"],
        }

        if kc.get(str(chat_id)) != new_known:
            kc[str(chat_id)] = new_known
            changed = True

    if changed:
        save_data(data)
_finalize_timers = {}
_backup_timers = {}
_balance_panel_refresh_timers = {}
_balance_panel_collapse_timers = {}
_total_message_timers = {}

def schedule_backup_flush(chat_id: int, delay: float = 8.0):
    def _job():
        try:
            save_chat_json(chat_id)
            export_global_csv(data)

            send_backup_to_chat(chat_id)
            send_backup_to_channel(chat_id)
        except Exception as e:
            log_error(f"schedule_backup_flush({chat_id}): {e}")

    prev = _backup_timers.get(chat_id)
    if prev and prev.is_alive():
        try:
            prev.cancel()
        except Exception:
            pass

    t = threading.Timer(delay, _job)
    _backup_timers[chat_id] = t
    t.start()
    
def schedule_finalize(chat_id: int, day_key: str, delay: float = 0.8):
    def _safe(action_name, func):
        try:
            return func()
        except Exception as e:
            log_error(f"[FINALIZE ERROR] {action_name}: {e}")
            return None

    def _job():
        _safe("recalc_balance", lambda: recalc_balance(chat_id))
        _safe("rebuild_global_records", rebuild_global_records)

        if OWNER_ID and str(chat_id) == str(OWNER_ID):
            _safe(
                "owner_backup_window",
                lambda: backup_window_for_owner(chat_id, day_key, None)
            )
        else:
            _safe(
                "update_day_window",
                lambda: update_or_send_day_window(chat_id, day_key)
            )

        _safe(
            "refresh_total_chat",
            lambda: refresh_total_message_if_any(chat_id)
        )

        if OWNER_ID and str(chat_id) != str(OWNER_ID):
            _safe(
                "refresh_total_owner",
                lambda: refresh_total_message_if_any(int(OWNER_ID))
            )
            _safe(
                "refresh_owner_window",
                lambda: refresh_owner_after_chat_change(chat_id)
            )

        _safe("save_data", lambda: save_data(data))
        _safe("refresh_balance_panel_now", lambda: refresh_balance_panel_now(chat_id))
        _safe("schedule_balance_panel_refresh", lambda: schedule_balance_panel_refresh(chat_id, BALANCE_PANEL_REFRESH_DELAY))

        _safe(
            "schedule_backup_flush",
            lambda: schedule_backup_flush(chat_id, 8.0)
        )

    t_prev = _finalize_timers.get(chat_id)
    if t_prev and t_prev.is_alive():
        try:
            t_prev.cancel()
        except Exception:
            pass

    t = threading.Timer(delay, _job)
    _finalize_timers[chat_id] = t
    t.start()
    
def recalc_balance(chat_id: int):
    store = get_chat_store(chat_id)
    store["balance"] = sum(r.get("amount", 0) for r in store.get("records", []))
def rebuild_month_short_ids(chat_id: int):
    """
    Пересчитывает short_id как месячную нумерацию:
    в каждом месяце заново R1, R2, R3...
    Внутренний id НЕ трогаем.
    """
    store = get_chat_store(chat_id)
    daily = store.get("daily_records", {}) or {}

    month_counters = {}

    for dk in sorted(daily.keys()):
        month_key = dk[:7]  # YYYY-MM
        if month_key not in month_counters:
            month_counters[month_key] = 1

        recs = sorted(daily.get(dk, []), key=lambda r: r.get("timestamp", ""))
        daily[dk] = recs

        for r in recs:
            r["short_id"] = f"R{month_counters[month_key]}"
            month_counters[month_key] += 1

    by_id = {}
    for dk in daily:
        for r in daily[dk]:
            by_id[r["id"]] = r

    store["records"] = [by_id[r["id"]] for r in sorted(by_id.values(), key=lambda x: x["id"])]
def calc_day_balance(store: dict, day_key: str) -> float:
    """
    Остаток на конец указанного дня.
    Сумма всех операций <= day_key.
    """
    total = 0.0
    daily = store.get("daily_records", {}) or {}

    for dk in sorted(daily.keys()):
        if dk > day_key:
            break
        for r in daily.get(dk, []):
            total += float(r.get("amount", 0) or 0)

    return total
def rebuild_global_records():
    all_recs = []
    for cid, st in data.get("chats", {}).items():
        all_recs.extend(st.get("records", []))
    data["records"] = all_recs
    data["overall_balance"] = sum(r.get("amount", 0) for r in all_recs)
def force_backup_to_chat(chat_id: int):
    try:
        save_chat_json(chat_id)
        json_path = chat_json_file(chat_id)
        if not os.path.exists(json_path):
            log_error(f"force_backup_to_chat: {json_path} missing")
            return

        meta = _load_chat_backup_meta()
        msg_key = f"msg_chat_{chat_id}"
        ts_key = f"timestamp_chat_{chat_id}"

        chat_title = _get_chat_title_for_backup(chat_id)
        caption = (
            f"🧾 Авто-бэкап JSON чата: {chat_title}\n"
            f"⏱ {now_local().strftime('%Y-%m-%d %H:%M:%S')}"
        )

        with open(json_path, "rb") as f:
            data_bytes = f.read()
            if not data_bytes:
                log_error("force_backup_to_chat: empty JSON")
                return

        base = os.path.basename(json_path)
        name_no_ext, dot, ext = base.partition(".")
        suffix = get_chat_name_for_filename(chat_id)
        file_name = suffix if suffix else name_no_ext
        if dot:
            file_name += f".{ext}"

        buf = io.BytesIO(data_bytes)
        buf.name = file_name

        sent = bot.send_document(chat_id, buf, caption=caption)
        meta[msg_key] = sent.message_id
        meta[ts_key] = now_local().isoformat(timespec="seconds")
        _save_chat_backup_meta(meta)

    except Exception as e:
        log_error(f"force_backup_to_chat({chat_id}): {e}")

def backup_window_for_owner(chat_id: int, day_key: str, message_id_override: int | None = None):
    """
    Окно дня для владельца без document-caption.
    JSON-бэкапы отправляются отдельно через schedule_backup_flush().
    """
    lock = window_locks[(chat_id, day_key)]

    with lock:
        txt, _ = render_day_window(chat_id, day_key)
        kb = build_main_keyboard(day_key, chat_id)

        if len(txt) > 3900:
            log_error(f"backup_window_for_owner: text too long for {chat_id} {day_key}, len={len(txt)}")

        mid = message_id_override or get_active_window_id(chat_id, day_key)
        if message_id_override:
            try:
                set_active_window_id(chat_id, day_key, message_id_override)
            except Exception:
                pass

        if mid:
            try:
                bot.edit_message_text(
                    txt,
                    chat_id=chat_id,
                    message_id=mid,
                    reply_markup=kb,
                    parse_mode="HTML"
                )
                set_active_window_id(chat_id, day_key, mid)
                return
            except Exception as e:
                err = str(e).lower()
                if "message is not modified" in err:
                    return
                log_error(f"backup_window_for_owner edit failed: {e}")
                try:
                    bot.delete_message(chat_id, mid)
                except Exception:
                    pass

        sent = bot.send_message(
            chat_id,
            txt,
            reply_markup=kb,
            parse_mode="HTML"
        )
        set_active_window_id(chat_id, day_key, sent.message_id)

def force_new_day_window(chat_id: int, day_key: str):
    if OWNER_ID and str(chat_id) == str(OWNER_ID):
        backup_window_for_owner(chat_id, day_key)
        return

    old_mid = get_active_window_id(chat_id, day_key)
    txt, _ = render_day_window(chat_id, day_key)
    kb = build_main_keyboard(day_key, chat_id)
    sent = bot.send_message(chat_id, txt, reply_markup=kb, parse_mode="HTML")
    set_active_window_id(chat_id, day_key, sent.message_id)
    if old_mid:
        try:
            bot.delete_message(chat_id, old_mid)
        except Exception:
            pass
def reset_chat_data(chat_id: int):
    """
    Полное обнуление данных чата:
      • баланс
      • записи / daily_records
      • next_id
      • active_windows
      • edit_wait / edit_target
      • обновление окна дня
      • бэкап
    """
    try:
        store = get_chat_store(chat_id)
        cleanup_forward_links(chat_id)
        store["balance"] = 0
        store["records"] = []
        store["daily_records"] = {}
        store["next_id"] = 1
        store["active_windows"] = {}
        store["edit_wait"] = None
        store["edit_target"] = None
        store["reset_wait"] = False
        store["reset_time"] = 0
        store["wait_date_msg_id"] = None
        store["reset_prompt_msg_id"] = None
        store["edit_prompt_msg_id"] = None
        save_data(data)
        save_chat_json(chat_id)
        export_global_csv(data)
        send_backup_to_channel(chat_id)
        send_backup_to_chat(chat_id)
        day_key = store.get("current_view_day", today_key())
        update_or_send_day_window(chat_id, day_key)
        try:
            day_key = get_chat_store(chat_id).get("current_view_day", today_key())
            update_or_send_day_window(chat_id, day_key)
        except Exception:
            pass
        refresh_total_message_if_any(chat_id)
        schedule_balance_panel_refresh(chat_id, 1.0)
        if OWNER_ID and str(chat_id) != str(OWNER_ID):
            try:
                refresh_total_message_if_any(int(OWNER_ID))
            except Exception:
                pass
            try:
                refresh_owner_after_chat_change(chat_id)
            except Exception:
                pass
    except Exception as e:
        log_error(f"reset_chat_data({chat_id}): {e}")

@bot.message_handler(content_types=["document"])
def handle_document(msg):
    global restore_mode, data

    chat_id = msg.chat.id
    update_chat_info_from_message(msg)
    try:
        if not getattr(getattr(msg, "from_user", None), "is_bot", False):
            stop_dozvon_for_target(chat_id)
    except Exception:
        pass

    file = msg.document
    fname = (file.file_name or "").lower()

    log_info(f"[DOC] recv chat={chat_id} restore={restore_mode} fname={fname}")

    if restore_mode is not None and restore_mode == chat_id:

        if not (fname.endswith(".json") or fname.endswith(".csv")):
            send_and_auto_delete(
                chat_id,
                "⚠️ В режиме восстановления принимаются только JSON / CSV."
            )
            return

        try:
            file_info = bot.get_file(file.file_id)
            raw = bot.download_file(file_info.file_path)
        except Exception as e:
            send_and_auto_delete(chat_id, f"❌ Ошибка скачивания: {e}")
            return

        tmp_path = f"restore_{chat_id}_{fname}"
        with open(tmp_path, "wb") as f:
            f.write(raw)

        try:
            if fname == "data.json":
                os.replace(tmp_path, DATA_FILE)
                _import_legacy_global_json_to_db(DATA_FILE, force=True)
                data = load_data()

                finance_active_chats.clear()
                fac = data.get("finance_active_chats") or {}
                for cid, enabled in fac.items():
                    if enabled:
                        try:
                            finance_active_chats.add(int(cid))
                        except Exception:
                            pass

                restore_mode = None
                send_and_auto_delete(chat_id, "🟢 Глобальный data.json импортирован в SQLite!")
                return

            if fname == "csv_meta.json":
                os.replace(tmp_path, CSV_META_FILE)
                _save_csv_meta(_load_json(CSV_META_FILE, {}) or {})
                restore_mode = None
                send_and_auto_delete(chat_id, "🟢 csv_meta.json импортирован в SQLite")
                return

            if fname.endswith(".json"):
                payload = _load_json(tmp_path, None)
                if not isinstance(payload, dict):
                    raise RuntimeError("JSON не является объектом")

                if "chats" in payload:
                    os.replace(tmp_path, DATA_FILE)
                    _import_legacy_global_json_to_db(DATA_FILE, force=True)
                    data.clear()
                    data.update(load_data())
                    restore_mode = None
                    send_and_auto_delete(chat_id, "🟢 Глобальный data.json импортирован в SQLite")
                    return

                inner_chat_id = payload.get("chat_id")
                if inner_chat_id is None:
                    raise RuntimeError("В JSON нет chat_id")

                if int(inner_chat_id) != int(chat_id):
                    raise RuntimeError(
                        f"JSON относится к чату {inner_chat_id}, а не к текущему {chat_id}"
                    )

                restore_from_json(chat_id, tmp_path)

                day_key = get_chat_store(chat_id).get(
                    "current_view_day",
                    today_key()
                )
                update_or_send_day_window(chat_id, day_key)
                send_backup_to_chat(chat_id)
                send_backup_to_channel(chat_id)

                restore_mode = None
                send_and_auto_delete(
                    chat_id,
                    f"🟢 JSON чата {chat_id} восстановлен"
                )
                return

            if fname.startswith("data_") and fname.endswith(".csv"):
                restore_from_csv(chat_id, tmp_path)

                day_key = get_chat_store(chat_id).get(
                    "current_view_day",
                    today_key()
                )
                update_or_send_day_window(chat_id, day_key)
                send_backup_to_chat(chat_id)
                send_backup_to_channel(chat_id)

                restore_mode = None
                send_and_auto_delete(
                    chat_id,
                    f"🟢 CSV чата восстановлен ({fname})"
                )
                return

            send_and_auto_delete(chat_id, f"⚠️ Неизвестный файл: {fname}")

        except Exception as e:
            send_and_auto_delete(chat_id, f"❌ Ошибка восстановления: {e}")

        finally:
            try:
                os.remove(tmp_path)
            except Exception:
                pass

        return

    try:
        forward_any_message(chat_id, msg)
    except Exception as e:
        log_error(f"handle_document forward failed: {e}")


def cleanup_forward_links(chat_id: int):
    """
    Удаляет все связи пересылки для чата из памяти и из сохранённого индекса.
    """
    _cleanup_forward_storage_for_chat(chat_id)

KEEP_ALIVE_SEND_TO_OWNER = False
def keep_alive_task():
    while True:
        try:
            base_candidates = []
            for raw in (APP_URL, WEBHOOK_URL, os.getenv("RENDER_EXTERNAL_URL", "").strip()):
                if not raw:
                    continue
                base = raw.rstrip("/")
                if base not in base_candidates:
                    base_candidates.append(base)

            if base_candidates:
                ok = False
                for base in base_candidates:
                    for url in (f"{base}/healthz", f"{base}/?ts={int(time.time())}"):
                        try:
                            resp = requests.get(
                                url,
                                timeout=10,
                                headers={"Cache-Control": "no-cache"}
                            )
                            log_info(f"Keep-alive ping {url} -> {resp.status_code}")
                            ok = True
                            break
                        except Exception as e:
                            log_error(f"Keep-alive self error for {url}: {e}")
                    if ok:
                        break
                if not ok:
                    log_error("Keep-alive: all self-ping attempts failed")
            else:
                log_error("Keep-alive skipped: APP_URL / WEBHOOK_URL / RENDER_EXTERNAL_URL are empty")
            if KEEP_ALIVE_SEND_TO_OWNER and OWNER_ID:
                try:
                    pass
                except Exception as e:
                    log_error(f"Keep-alive notify error: {e}")
        except Exception as e:
            log_error(f"Keep-alive loop error: {e}")
        time.sleep(max(10, KEEP_ALIVE_INTERVAL_SECONDS))

@bot.channel_post_handler(content_types=[
    "text", "photo", "video", "animation", "audio",
    "voice", "video_note", "document",
    "sticker", "location", "venue", "contact", "dice", "poll"
])
def on_any_channel_post(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception as e:
        log_error(f"channel_post update_chat_info failed: {e}")

    try:
        stop_dozvon_for_target(msg.chat.id)
    except Exception:
        pass

    try:
        forward_any_message(msg.chat.id, msg)
    except Exception as e:
        log_error(f"channel_post forward failed: {e}")


@bot.edited_channel_post_handler(content_types=[
    "text", "photo", "video", "animation", "audio",
    "voice", "video_note", "document",
    "sticker", "location", "venue", "contact", "dice", "poll"
])
def on_edited_channel_post(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception as e:
        log_error(f"edited_channel_post update_chat_info failed: {e}")

    try:
        propagate_edited_to_copies(msg)
    except Exception as e:
        log_error(f"edited_channel_post propagate failed: {e}")


def propagate_edited_to_copies(msg):
    source_chat_id = msg.chat.id
    text = _message_text_for_finance(msg)

    links = get_forward_links(source_chat_id, msg.message_id)
    if not links:
        return

    for dst_chat_id, dst_msg_id in links:
        try:
            finance_enabled = get_forward_finance(source_chat_id, dst_chat_id)
            sync_edited_copy_to_target(source_chat_id, msg, dst_chat_id, dst_msg_id, finance_enabled)
        except Exception as e:
            log_error(f"propagate_edited_to_copies failed {dst_chat_id}:{dst_msg_id}: {e}")


@bot.edited_message_handler(
    content_types=["text", "photo", "video", "animation", "document", "audio", "voice"]
)
def on_edited_message(msg):
    chat_id = msg.chat.id

    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass

    edit_text = _message_text_for_finance(msg)
    if is_forward_delete_command(edit_text):
        try:
            delete_forward_copies_for_source(chat_id, msg.message_id)
        except Exception as e:
            log_error(f"[EDIT-DEL] failed: {e}")

    try:
        edited = handle_finance_edit(msg)
        if edited:
            store = get_chat_store(chat_id)
            day_key = store.get("current_view_day") or today_key()
            log_info(f"[EDIT-FIN] finalize day_key={day_key}")
            schedule_finalize(chat_id, day_key)
    except Exception as e:
        log_error(f"[EDIT-FIN] failed: {e}")

    try:
        if not is_forward_delete_command(edit_text):
            propagate_edited_to_copies(msg)
    except Exception as e:
        log_error(f"[EDIT-FWD] failed: {e}")
                                            
@bot.message_handler(commands=["sqlite", "db"])
def cmd_sqlite_dump(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass

    schedule_command_delete(msg)
    chat_id = msg.chat.id
    stop_dozvon_for_target(chat_id)
    if guard_non_owner_finance_for_command(msg, {"ok", "help"}):
        return
    if not is_owner_chat(chat_id):
        send_and_auto_delete(chat_id, "Эта команда только для владельца.", HELPER_DELETE_DELAY)
        return

    try:
        with open(DB_FILE, "rb") as f:
            bot.send_document(chat_id, f, caption=f"🗄 SQLite база: {os.path.basename(DB_FILE)}")
    except Exception as e:
        log_error(f"cmd_sqlite_dump: {e}")
        send_and_auto_delete(chat_id, f"❌ Не удалось отправить SQLite: {e}", HELPER_DELETE_DELAY)


def start_keep_alive_thread():
    t = threading.Thread(target=keep_alive_task, daemon=True)
    t.start()
@app.route("/", methods=["GET"])
def index():
    return "OK", 200


@app.route("/healthz", methods=["GET"])
def healthz():
    return "OK", 200


@app.route(f"/{BOT_TOKEN}", methods=["POST"])
def telegram_webhook():
    try:
        payload = request.get_json(force=True, silent=False)
    except Exception as e:
        log_error(f"WEBHOOK: get_json failed: {e}")
        return "BAD REQUEST", 400

    try:
        if isinstance(payload, dict):
            if "edited_message" in payload:
                log_info("WEBHOOK: получен update с edited_message ✅")
            elif "message" in payload:
                log_info("WEBHOOK: получен update с message")
            elif "callback_query" in payload:
                log_info("WEBHOOK: получен update с callback_query")

        update = telebot.types.Update.de_json(payload)
        bot.process_new_updates([update])
    except Exception as e:
        log_error(f"WEBHOOK: process update error: {e}")
        return "ERROR", 500

    return "OK", 200
        
def set_webhook():
    if not WEBHOOK_URL:
        log_info("WEBHOOK_URL / APP_URL / RENDER_EXTERNAL_URL не указаны — webhook не установлен.")
        return

    wh_url = WEBHOOK_URL.rstrip("/") + f"/{BOT_TOKEN}"

    bot.remove_webhook()
    time.sleep(0.5)

    bot.set_webhook(
        url=wh_url,
        allowed_updates=[
            "message",
            "edited_message",
            "callback_query",
            "channel_post",
            "edited_channel_post",
            "deleted_business_messages",
        ],
    )
    log_info(f"Webhook установлен: {wh_url} (allowed_updates включает edited_message)")
        
def main():
    global data
    restored = False
    data = load_data()
    data["forward_rules"] = load_forward_rules()
    migrate_quick_balance_defaults()
    if OWNER_ID:
        try:
            finance_active_chats.add(int(OWNER_ID))
        except Exception:
            pass
    log_info(f"Данные загружены из SQLite ({DB_FILE}). Версия бота: {VERSION}")
    set_webhook()
    start_keep_alive_thread()
    owner_id = None
    if OWNER_ID:
        try:
            owner_id = int(OWNER_ID)
        except Exception:
            owner_id = None
        if owner_id:
            try:
                bot.send_message(
                    owner_id,
                    f"✅ Бот запущен (версия {VERSION}).\n"
                    f"Восстановление: {'OK' if restored else 'пропущено'}"
                )
            except Exception as e:
                log_error(f"notify owner on start: {e}")
    app.run(host="0.0.0.0", port=PORT, threaded=True, use_reloader=False)
if __name__ == "__main__":
    main()
