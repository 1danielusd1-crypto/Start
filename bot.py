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
import zipfile
import subprocess
import shutil
import tempfile
import calendar

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests
import telebot
from telebot import types
from telebot.types import InputMediaDocument, InputMediaPhoto, InputMediaVideo, InputMediaAudio, InputMediaAnimation

from flask import Flask, request


from collections import defaultdict, deque
from contextlib import contextmanager

window_locks = defaultdict(threading.Lock)

# ─────────────────────────────────────────────────────────────
# Потокобезопасность / очереди по чатам
# ─────────────────────────────────────────────────────────────
# Flask webhook работает в threaded=True, поэтому разные апдейты Telegram
# могут приходить одновременно. Эти замки делают обработку стабильной:
# • один и тот же чат обрабатывается строго по очереди;
# • data/save_data защищены отдельным глобальным замком;
# • forward_map защищён отдельным замком;
# • пересылка вынесена из основного lock чата и выполняется через общий
#   forward_delivery_lock, чтобы не было deadlock при связках A ⇄ B.
chat_locks = defaultdict(threading.RLock)
data_lock = threading.RLock()
forward_map_lock = threading.RLock()
forward_delivery_lock = threading.RLock()
timer_lock = threading.RLock()


def chat_lock_for(chat_id: int):
    return chat_locks[int(chat_id)]


@contextmanager
def locked_chat(chat_id: int):
    with chat_lock_for(int(chat_id)):
        yield


def _extract_update_chat_id(payload: dict):
    """Достаёт chat_id из сырого Telegram update до передачи в telebot."""
    try:
        for key in ("message", "edited_message", "channel_post", "edited_channel_post"):
            item = payload.get(key)
            if isinstance(item, dict):
                chat = item.get("chat") or {}
                if "id" in chat:
                    return int(chat["id"])

        cq = payload.get("callback_query")
        if isinstance(cq, dict):
            msg = cq.get("message") or {}
            chat = msg.get("chat") or {}
            if "id" in chat:
                return int(chat["id"])
    except Exception:
        pass
    return None


def schedule_forward_any_message(source_chat_id: int, msg):
    """Запускает пересылку после выхода из lock исходного чата."""
    def _job():
        try:
            with forward_delivery_lock:
                forward_any_message(source_chat_id, msg)
        except Exception as e:
            log_error(f"schedule_forward_any_message({source_chat_id}): {e}")

    threading.Thread(target=_job, daemon=True).start()


def schedule_propagate_edited_to_copies(msg):
    """Синхронизация правок копий вынесена из lock исходного чата."""
    def _job():
        try:
            with forward_delivery_lock:
                propagate_edited_to_copies(msg)
        except Exception as e:
            log_error(f"schedule_propagate_edited_to_copies: {e}")

    threading.Thread(target=_job, daemon=True).start()


def schedule_delete_forward_copies_for_source(source_chat_id: int, source_msg_id: int):
    """Удаление копий вынесено из lock исходного чата."""
    def _job():
        try:
            with forward_delivery_lock:
                delete_forward_copies_for_source(source_chat_id, source_msg_id)
        except Exception as e:
            log_error(f"schedule_delete_forward_copies_for_source: {e}")

    threading.Thread(target=_job, daemon=True).start()
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
VERSION = "bot_v73_secret_tabl_lsx"
DEFAULT_TZ = "America/Argentina/Buenos_Aires"
KEEP_ALIVE_INTERVAL_SECONDS = 30
DB_FILE = os.getenv("DB_FILE", "bot_state.sqlite3").strip() or "bot_state.sqlite3"
DATA_FILE = "data.json"
CSV_FILE = "data.csv"
CSV_META_FILE = "csv_meta.json"

# ─────────────────────────────────────────────────────────────
# MEGA.nz / MEGAcmd backup + autorestore
# ─────────────────────────────────────────────────────────────
def _env_bool(name: str, default: str = "0") -> bool:
    return str(os.getenv(name, default)).strip().lower() in {"1", "true", "yes", "y", "on", "да"}

MEGA_ENABLED = _env_bool("MEGA_ENABLED", "0")
MEGA_AUTORESTORE = _env_bool("MEGA_AUTORESTORE", "1")
MEGA_EMAIL = os.getenv("MEGA_EMAIL", "").strip()
MEGA_PASSWORD = os.getenv("MEGA_PASSWORD", "").strip()
MEGA_BACKUP_DIR = os.getenv("MEGA_BACKUP_DIR", "/TelegramBotBackups").strip() or "/TelegramBotBackups"
try:
    MEGA_TIMEOUT = int(os.getenv("MEGA_TIMEOUT", "120"))
except Exception:
    MEGA_TIMEOUT = 120
MEGA_LATEST_GLOBAL_NAME = os.getenv("MEGA_LATEST_GLOBAL_NAME", "latest_global.json").strip() or "latest_global.json"
MEGA_LOCAL_TMP_DIR = os.getenv("MEGA_LOCAL_TMP_DIR", "/tmp").strip() or "/tmp"
MEGA_CHAT_BACKUP_DIR = os.getenv("MEGA_CHAT_BACKUP_DIR", "chats").strip().strip("/") or "chats"
MEGA_MONTHLY_BACKUP_DIR = os.getenv("MEGA_MONTHLY_BACKUP_DIR", "monthly").strip().strip("/") or "monthly"
forward_map = {}
backup_flags = {
    "channel": True,
}
restore_mode = None
_media_group_cache = {}
_media_group_timers = {}
FORWARD_MEDIA_GROUP_DELAY = 0.8
_forward_state_timer = None
_owner_json_restore_prompts = {}
_owner_json_restore_prompt_lock = threading.RLock()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)
BOT_ERROR_LOG = deque(maxlen=200)
error_log_lock = threading.RLock()
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


def log_info(msg: str):
    logger.info(msg)
def log_error(msg: str):
    logger.error(msg)
    try:
        if 'bot_journal' in globals():
            bot_journal("error", None, str(msg), "ERROR")
    except Exception:
        pass
    try:
        with error_log_lock:
            BOT_ERROR_LOG.append({
                "ts": now_local().strftime("%Y-%m-%d %H:%M:%S") if "now_local" in globals() else time.strftime("%Y-%m-%d %H:%M:%S"),
                "msg": str(msg)[:900],
            })
    except Exception:
        pass

def get_recent_errors(limit: int = 20):
    try:
        with error_log_lock:
            return list(BOT_ERROR_LOG)[-int(limit):]
    except Exception:
        return []


# ─────────────────────────────────────────────────────────────
# 📓 Журнал действий всего бота
# ─────────────────────────────────────────────────────────────
BOT_JOURNAL_MAX = int(os.getenv("BOT_JOURNAL_MAX", "5000") or "5000")
BOT_JOURNAL_FILE = os.getenv("BOT_JOURNAL_FILE", "bot_journal.jsonl").strip() or "bot_journal.jsonl"
BOT_ACTION_LOG = deque(maxlen=BOT_JOURNAL_MAX)
bot_journal_lock = threading.RLock()


def _journal_ts() -> str:
    try:
        return now_local().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    except Exception:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def is_journal_registration_enabled() -> bool:
    """Глобальный переключатель регистрации 📓 журнала. По умолчанию включён."""
    try:
        d = globals().get("data")
        if isinstance(d, dict):
            gs = d.setdefault("_global_settings", {})
            return bool(gs.get("bot_journal_enabled", True))
    except Exception:
        pass
    return True


def set_journal_registration_enabled(enabled: bool):
    try:
        d = globals().get("data")
        if isinstance(d, dict):
            d.setdefault("_global_settings", {})["bot_journal_enabled"] = bool(enabled)
            if "save_data" in globals():
                save_data(d)
    except Exception:
        pass


def toggle_journal_registration() -> bool:
    new_value = not is_journal_registration_enabled()
    set_journal_registration_enabled(new_value)
    return new_value


def journal_toggle_label() -> str:
    return ("✅ Журнал ВКЛ" if is_journal_registration_enabled() else "❌ Журнал ВЫКЛ")

def buttons_current_window_enabled() -> bool:
    """Глобальный режим владельца: кнопочные переходы стараются открываться в текущем окне."""
    try:
        gs = (data or {}).setdefault("_global_settings", {})
        return bool(gs.get("buttons_current_window", False))
    except Exception:
        return False


def chat_buttons_current_window_enabled(chat_id: int) -> bool:
    try:
        store = get_chat_store(int(chat_id))
        local = bool(store.setdefault("settings", {}).get("buttons_current_window", False))
        return local or (is_owner_chat(chat_id) and buttons_current_window_enabled())
    except Exception:
        return False


def toggle_chat_buttons_current_window(chat_id: int) -> bool:
    store = get_chat_store(int(chat_id))
    settings = store.setdefault("settings", {})
    new_value = not bool(settings.get("buttons_current_window", False))
    settings["buttons_current_window"] = new_value
    save_data(data)
    return new_value


def set_buttons_current_window_enabled(enabled: bool):
    try:
        data.setdefault("_global_settings", {})["buttons_current_window"] = bool(enabled)
        save_data(data)
    except Exception as e:
        log_error(f"set_buttons_current_window_enabled: {e}")


def toggle_buttons_current_window() -> bool:
    new_value = not buttons_current_window_enabled()
    set_buttons_current_window_enabled(new_value)
    return new_value


def buttons_current_window_label() -> str:
    return "✅ В текущем окне" if buttons_current_window_enabled() else "❌ В текущем окне"




def forward_menu_new_style_enabled() -> bool:
    """Глобальный режим владельца для В22: старое меню или новое визуальное меню пары A/B."""
    try:
        gs = (data or {}).setdefault("_global_settings", {})
        return bool(gs.get("forward_menu_new_style", False))
    except Exception:
        return False


def set_forward_menu_new_style_enabled(enabled: bool):
    try:
        data.setdefault("_global_settings", {})["forward_menu_new_style"] = bool(enabled)
        save_data(data)
    except Exception as e:
        log_error(f"set_forward_menu_new_style_enabled: {e}")


def toggle_forward_menu_new_style() -> bool:
    new_value = not forward_menu_new_style_enabled()
    set_forward_menu_new_style_enabled(new_value)
    return new_value


def forward_menu_style_label() -> str:
    return "🧩 Пересылка: по-новому" if forward_menu_new_style_enabled() else "🔁 Пересылка: обычно"


# ─────────────────────────────────────────────────────────────
# Короткие подписи inline-кнопок: режим переключается владельцем командой /buttons.
# Имена чатов не трогаем: сокращаются только известные служебные подписи.
# ─────────────────────────────────────────────────────────────
def icon_button_mode_enabled() -> bool:
    try:
        gs = (data or {}).setdefault("_global_settings", {})
        return bool(gs.get("icon_button_mode", True))
    except Exception:
        return False


def set_icon_button_mode_enabled(enabled: bool):
    try:
        data.setdefault("_global_settings", {})["icon_button_mode"] = bool(enabled)
        save_data(data)
    except Exception as e:
        log_error(f"set_icon_button_mode_enabled: {e}")


def toggle_icon_button_mode() -> bool:
    new_value = not icon_button_mode_enabled()
    set_icon_button_mode_enabled(new_value)
    return new_value


def icon_button_mode_label() -> str:
    return "🔣 Кнопки: значки" if icon_button_mode_enabled() else "🔤 Кнопки: текст"


def total_secret_mask_enabled() -> bool:
    try:
        gs = (data or {}).setdefault("_global_settings", {})
        return bool(gs.get("total_secret_mask_enabled", False))
    except Exception:
        return False


def set_total_secret_mask_enabled(enabled: bool):
    try:
        data.setdefault("_global_settings", {})["total_secret_mask_enabled"] = bool(enabled)
        save_data(data)
    except Exception as e:
        log_error(f"set_total_secret_mask_enabled: {e}")


def toggle_total_secret_mask() -> bool:
    new_value = not total_secret_mask_enabled()
    set_total_secret_mask_enabled(new_value)
    return new_value


def total_secret_mask_label() -> str:
    return "🪷 Маска: ВКЛ" if total_secret_mask_enabled() else "🪷 Маска: ВЫКЛ"

def bot_journal(action: str, chat_id=None, detail: str = "", level: str = "INFO"):
    """Пишет действие в общий журнал: команды, кнопки, функции, Telegram API, backup, ошибки."""
    try:
        # Если регистрация выключена — не пишем обычные действия. Ошибки остаются в /errors.
        if str(action or "") not in {"journal_toggle", "journal_export_requested"} and str(level or "INFO").upper() != "ERROR":
            if not is_journal_registration_enabled():
                return None
        row = {
            "ts": _journal_ts(),
            "level": str(level or "INFO"),
            "action": str(action or "")[:160],
            "chat_id": str(chat_id) if chat_id is not None else "",
            "chat_name": "",
            "detail": str(detail or "")[:1800],
        }
        try:
            if chat_id is not None:
                row["chat_name"] = get_chat_display_name(int(chat_id))
        except Exception:
            pass
        with bot_journal_lock:
            BOT_ACTION_LOG.append(row)
            try:
                with open(BOT_JOURNAL_FILE, "a", encoding="utf-8") as jf:
                    jf.write(json.dumps(row, ensure_ascii=False) + "\n")
            except Exception:
                pass
        return row
    except Exception:
        return None


def get_recent_journal(limit: int = 200):
    try:
        with bot_journal_lock:
            return list(BOT_ACTION_LOG)[-int(limit):]
    except Exception:
        return []


def format_journal_text(limit: int = 120) -> str:
    rows = get_recent_journal(limit)
    if not rows:
        return "📓 Журнал пока пуст."
    lines = [f"📓 Журнал действий бота, последние {len(rows)} записей:"]
    for r in rows:
        chat = r.get("chat_name") or r.get("chat_id") or "-"
        detail = r.get("detail") or ""
        if len(detail) > 500:
            detail = detail[:500] + "…"
        lines.append(f"\n• {r.get('ts','')} [{r.get('level','')}] {r.get('action','')}\n  чат: {chat}\n  {detail}".rstrip())
    text = wm_owner("\n".join(lines), 9)
    return text[-3900:] if len(text) > 3900 else text


def send_journal_file_to_owner(chat_id: int, limit: int = 2000):
    if not is_owner_chat(chat_id):
        send_and_auto_delete(chat_id, "📓 Журнал доступен только владельцу.", HELPER_DELETE_DELAY)
        return
    bot_journal("journal_export_requested", chat_id, f"limit={limit}")
    rows = get_recent_journal(limit)
    if not rows and os.path.exists(BOT_JOURNAL_FILE):
        try:
            with open(BOT_JOURNAL_FILE, "r", encoding="utf-8") as f:
                raw = f.readlines()[-int(limit):]
            rows = [json.loads(x) for x in raw if x.strip()]
        except Exception:
            rows = []
    text_lines = ["📓 Журнал действий бота", f"Создан: {_journal_ts()}", ""]
    for r in rows:
        text_lines.append(
            f"{r.get('ts','')} | {r.get('level','')} | {r.get('action','')} | "
            f"chat={r.get('chat_name') or r.get('chat_id')} | {r.get('detail','')}"
        )
    payload = "\n".join(text_lines).encode("utf-8")
    buf = io.BytesIO(payload)
    buf.name = f"bot_journal_{now_local().strftime('%Y%m%d_%H%M%S')}.txt" if 'now_local' in globals() else "bot_journal.txt"
    _tg_call_retry(bot.send_document, chat_id, buf, caption="📓 Журнал действий бота", purpose="journal_send_document")


# ─────────────────────────────────────────────────────────────
# 🔐 Секретные заметки владельца через О9
# Telegram Bot API не умеет отличать долгое удержание inline-кнопки,
# поэтому используется рабочая замена: 3 быстрых нажатия за 3 секунды.
# Пока по ТЗ хранение обычным текстом: data + plain JSON в MEGA, если MEGA настроена.
# ─────────────────────────────────────────────────────────────
O9_SECRET_CLICK_WINDOW_SECONDS = 3.0
O9_SECRET_WAIT_SECONDS = 90
O9_SECRET_WAIT_COUNTDOWN_STEP_SECONDS = 5
_o9_secret_clicks = {}
_o9_secret_click_lock = threading.RLock()
_o9_secret_action_timers = {}
_o9_secret_wait_timers = {}


def _secret_notes_list() -> list:
    try:
        arr = data.setdefault("_secret_notes", [])
        if not isinstance(arr, list):
            data["_secret_notes"] = []
            arr = data["_secret_notes"]
        return arr
    except Exception:
        return []


def _secret_notes_local_path() -> str:
    try:
        os.makedirs(MEGA_LOCAL_TMP_DIR, exist_ok=True)
        return os.path.join(MEGA_LOCAL_TMP_DIR, "secret_notes_owner.json")
    except Exception:
        return "secret_notes_owner.json"


def _save_secret_notes_plain_to_file() -> str | None:
    try:
        payload = {
            "kind": "owner_secret_notes_plain_text",
            "version": VERSION,
            "created_at": now_local().isoformat(timespec="seconds"),
            "warning": "plain text, not encrypted",
            "notes": _secret_notes_list(),
        }
        path = _secret_notes_local_path()
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        return path
    except Exception as e:
        log_error(f"_save_secret_notes_plain_to_file: {e}")
        return None


def upload_secret_notes_to_mega() -> bool:
    """Совместимость: секреты О9 теперь идут в единый файл чата владельца."""
    try:
        return bool(OWNER_ID and upload_chat_secrets_to_mega(int(OWNER_ID)))
    except Exception as e:
        log_error(f"upload_secret_notes_to_mega: {e}")
        return False


def _is_o9_owner_call(call) -> bool:
    try:
        chat_id = int(call.message.chat.id)
        if not is_owner_chat(chat_id):
            return False
        msg_id = int(call.message.message_id)
        store = get_chat_store(chat_id)
        if int(store.get("info_msg_id") or 0) == msg_id:
            return True
        text = (getattr(call.message, "text", None) or getattr(call.message, "caption", None) or "")
        return bool(re.search(r"(?:^|\s)о9\s*$", str(text or "")[-160:]))
    except Exception:
        return False


def _cancel_o9_secret_timer(key):
    try:
        t = _o9_secret_action_timers.pop(key, None)
        if t and getattr(t, "is_alive", lambda: False)():
            t.cancel()
    except Exception:
        pass


def _format_mmss(seconds: int) -> str:
    seconds = max(0, int(seconds))
    return f"{seconds // 60:02d}:{seconds % 60:02d}"


def _secret_wait_keyboard(chat_id: int, remaining: int = O9_SECRET_WAIT_SECONDS):
    kb = types.InlineKeyboardMarkup()
    store = get_chat_store(chat_id)
    kb.row(
        IB(f"❌ Закрыть {_format_mmss(remaining)}", callback_data="secret_cancel"),
        IB("⬅️ Назад осн. окно", callback_data=f"d:{store.get('current_view_day', today_key())}:back_main"),
    )
    return kb


def _secret_wait_prompt_text(remaining: int | None = None) -> str:
    tail = ""
    if remaining is not None:
        tail = f"\n\n⏳ Осталось: {_format_mmss(remaining)}"
    return wm_common(
        "🔐 Секретные данные\n\n"
        "Отправь одним сообщением текст, который нужно сохранить.\n"
        "Бот удалит твоё сообщение после сохранения.\n\n"
        "Важно: сейчас хранение обычным текстом, без шифрования."
        + tail,
        9,
    )


def _cancel_o9_secret_wait_timer(chat_id: int):
    key = int(chat_id)
    with _o9_secret_click_lock:
        item = _o9_secret_wait_timers.get(key)
        if isinstance(item, dict):
            item["cancelled"] = True
        _o9_secret_wait_timers.pop(key, None)


def schedule_o9_secret_wait_timeout(chat_id: int, prompt_message_id: int, delay: int = O9_SECRET_WAIT_SECONDS):
    key = int(chat_id)
    with _o9_secret_click_lock:
        prev = _o9_secret_wait_timers.get(key)
        if isinstance(prev, dict):
            prev["cancelled"] = True
        generation = int(time.time() * 1000)
        token = {"generation": generation, "cancelled": False}
        _o9_secret_wait_timers[key] = token

    def _job():
        try:
            remaining = int(delay)
            while remaining > 0:
                with _o9_secret_click_lock:
                    current = _o9_secret_wait_timers.get(key)
                    if current is not token or token.get("cancelled"):
                        return
                try:
                    _tg_call_retry(
                        bot.edit_message_text,
                        _secret_wait_prompt_text(remaining),
                        chat_id=chat_id,
                        message_id=int(prompt_message_id),
                        reply_markup=_secret_wait_keyboard(chat_id, remaining),
                        purpose="o9_secret_wait_countdown",
                    )
                except Exception as e:
                    if "message is not modified" not in str(e).lower():
                        log_error(f"o9 secret wait countdown {chat_id}:{prompt_message_id}: {e}")
                time.sleep(O9_SECRET_WAIT_COUNTDOWN_STEP_SECONDS)
                remaining -= O9_SECRET_WAIT_COUNTDOWN_STEP_SECONDS
            with _o9_secret_click_lock:
                current = _o9_secret_wait_timers.get(key)
                if current is not token or token.get("cancelled"):
                    return
                _o9_secret_wait_timers.pop(key, None)
            _clear_secret_wait(chat_id, delete_prompt=True)
            send_and_auto_delete(chat_id, "⌛ Время принятия секретных данных истекло.", 8)
        except Exception as e:
            log_error(f"schedule_o9_secret_wait_timeout({chat_id},{prompt_message_id}): {e}")

    threading.Thread(target=_job, daemon=True).start()



def _o9_delayed_close(chat_id: int, message_id: int, key):
    try:
        with _o9_secret_click_lock:
            item = _o9_secret_clicks.get(key) or {}
            # Если за время ожидания случился третий клик, обычное закрытие не делаем.
            if int(item.get("count", 0) or 0) >= 3:
                return
            _o9_secret_clicks.pop(key, None)
            _o9_secret_action_timers.pop(key, None)
        try:
            bot.delete_message(chat_id, message_id)
        except Exception:
            pass
        try:
            _clear_secret_wait(chat_id, delete_prompt=False)
        except Exception:
            pass
        try:
            _clear_stored_window(chat_id, "info_msg_id", message_id)
        except Exception:
            pass
    except Exception as e:
        log_error(f"_o9_delayed_close: {e}")


def _o9_delayed_back_main(chat_id: int, message_id: int, day_key: str, key):
    try:
        with _o9_secret_click_lock:
            item = _o9_secret_clicks.get(key) or {}
            if int(item.get("count", 0) or 0) >= 3:
                return
            _o9_secret_clicks.pop(key, None)
            _o9_secret_action_timers.pop(key, None)
        try:
            cancel_pending_window_commands(chat_id, delete_prompt=False)
        except Exception:
            pass
        try:
            day_key = day_key or get_chat_store(chat_id).get("current_view_day") or today_key()
            txt, _ = render_day_window(chat_id, day_key)
            bot.edit_message_text(
                txt,
                chat_id=chat_id,
                message_id=message_id,
                reply_markup=build_main_keyboard(day_key, chat_id),
                parse_mode="HTML",
            )
            try:
                set_active_window_id(chat_id, day_key, message_id)
            except Exception:
                pass
            try:
                _clear_stored_window(chat_id, "info_msg_id", message_id)
            except Exception:
                pass
        except Exception as e:
            log_error(f"_o9_delayed_back_main edit failed: {e}")
            try:
                txt, _ = render_day_window(chat_id, day_key)
                sent = _tg_call_retry(bot.send_message, chat_id, txt, reply_markup=build_main_keyboard(day_key, chat_id), parse_mode="HTML", purpose="o9_secret_back_send_main")
                try:
                    set_active_window_id(chat_id, day_key, sent.message_id)
                except Exception:
                    pass
            except Exception as e2:
                log_error(f"_o9_delayed_back_main send main failed: {e2}")
    except Exception as e:
        log_error(f"_o9_delayed_back_main: {e}")


def _start_secret_wait(chat_id: int, message_id: int | None = None):
    try:
        store = get_chat_store(chat_id)
        store["secret_wait"] = {
            "type": "secret_note_add",
            "started_at": now_local().isoformat(timespec="seconds"),
            "window_msg_id": int(message_id or 0),
        }
        save_data(data)

        kb = _secret_wait_keyboard(chat_id, O9_SECRET_WAIT_SECONDS)
        text = _secret_wait_prompt_text(O9_SECRET_WAIT_SECONDS)
        if message_id:
            try:
                bot.edit_message_text(text, chat_id=chat_id, message_id=message_id, reply_markup=kb)
                store["secret_wait"]["prompt_msg_id"] = int(message_id)
                save_data(data)
                schedule_o9_secret_wait_timeout(chat_id, int(message_id), O9_SECRET_WAIT_SECONDS)
                return
            except Exception:
                pass
        sent = _tg_call_retry(bot.send_message, chat_id, text, reply_markup=kb, purpose="secret_prompt")
        store["secret_wait"]["prompt_msg_id"] = sent.message_id
        save_data(data)
        schedule_o9_secret_wait_timeout(chat_id, sent.message_id, O9_SECRET_WAIT_SECONDS)
    except Exception as e:
        log_error(f"_start_secret_wait({chat_id}): {e}")

def _format_secret_notes_text() -> str:
    notes = _secret_notes_list()
    if not notes:
        return "🔐 Секретные данные\n\nПока пусто."
    lines = ["🔐 Секретные данные", ""]
    for i, item in enumerate(notes, start=1):
        ts = str((item or {}).get("ts") or "")
        body = str((item or {}).get("text") or "")
        lines.append(f"{i}. {ts}\n{body}")
        lines.append("")
    text = "\n".join(lines).strip()
    if len(text) > 3900:
        text = text[-3900:]
        text = "🔐 Секретные данные (последняя часть)\n\n" + text
    return text


def _send_secret_notes_to_owner(chat_id: int, message_id: int | None = None):
    try:
        open_secret_day_window(chat_id, chat_id, message_id=message_id)
    except Exception as e:
        log_error(f"_send_secret_notes_to_owner({chat_id}): {e}")


def _clear_secret_wait(chat_id: int, delete_prompt: bool = False):
    try:
        _cancel_o9_secret_wait_timer(chat_id)
        store = get_chat_store(chat_id)
        wait = store.get("secret_wait") or {}
        msg_id = int(wait.get("prompt_msg_id") or wait.get("window_msg_id") or 0)
        store["secret_wait"] = None
        save_data(data)
        if delete_prompt and msg_id:
            try:
                bot.delete_message(chat_id, msg_id)
            except Exception:
                pass
            try:
                _clear_stored_window(chat_id, "info_msg_id", msg_id)
            except Exception:
                pass
    except Exception as e:
        log_error(f"_clear_secret_wait({chat_id}): {e}")


def handle_secret_note_message(msg) -> bool:
    """Сохраняет секретное сообщение владельца и удаляет исходный текст."""
    try:
        if getattr(msg, "content_type", None) != "text":
            return False
        chat_id = int(msg.chat.id)
        if not is_owner_chat(chat_id):
            return False
        store = get_chat_store(chat_id)
        wait = store.get("secret_wait")
        if not wait or wait.get("type") != "secret_note_add":
            return False
        text = (msg.text or "").strip()
        if not text:
            return True
        save_secret_message(chat_id, msg, cleaned_text=text)
        delete_secret_source_message(msg)
        _clear_secret_wait(chat_id, delete_prompt=True)
        status = "✅ Секрет сохранён в единый файл чата и поставлен в очередь MEGA."
        sent = _tg_call_retry(bot.send_message, chat_id, status, purpose="secret_saved_notice")
        try:
            delete_message_later(chat_id, sent.message_id, 12)
        except Exception:
            pass
        return True
    except Exception as e:
        log_error(f"handle_secret_note_message: {e}")
        return True


def handle_o9_secret_triple_click(call, data_str: str) -> bool:
    """Перехватывает О9: Закрыть ×3 = ввод секрета, Назад ×3 = показать секреты."""
    try:
        if not _is_o9_owner_call(call):
            return False
        chat_id = int(call.message.chat.id)
        msg_id = int(call.message.message_id)
        kind = None
        day_key = get_chat_store(chat_id).get("current_view_day", today_key())
        if data_str == "info_close":
            kind = "close"
        elif str(data_str or "").startswith("d:"):
            parts = str(data_str).split(":", 2)
            action = parts[2] if len(parts) >= 3 else ""
            if action == "back_main":
                kind = "back"
                day_key = parts[1] or day_key
        if not kind:
            return False

        key = (chat_id, msg_id, kind)
        now_ts = time.time()
        with _o9_secret_click_lock:
            item = _o9_secret_clicks.get(key) or {"count": 0, "ts": 0}
            if now_ts - float(item.get("ts", 0) or 0) > O9_SECRET_CLICK_WINDOW_SECONDS:
                item = {"count": 0, "ts": 0}
            item["count"] = int(item.get("count", 0) or 0) + 1
            item["ts"] = now_ts
            _o9_secret_clicks[key] = item
            _cancel_o9_secret_timer(key)
            count = int(item["count"])

            if count < 3:
                if kind == "close":
                    t = threading.Timer(O9_SECRET_CLICK_WINDOW_SECONDS + 0.2, _o9_delayed_close, args=(chat_id, msg_id, key))
                else:
                    t = threading.Timer(O9_SECRET_CLICK_WINDOW_SECONDS + 0.2, _o9_delayed_back_main, args=(chat_id, msg_id, day_key, key))
                _o9_secret_action_timers[key] = t
                t.start()

        if count >= 3:
            _cancel_o9_secret_timer(key)
            with _o9_secret_click_lock:
                _o9_secret_clicks.pop(key, None)
            if kind == "close":
                _start_secret_wait(chat_id, msg_id)
                try:
                    bot.answer_callback_query(call.id, "🔐 Секретные данные")
                except Exception:
                    pass
            else:
                _send_secret_notes_to_owner(chat_id, msg_id)
                try:
                    bot.answer_callback_query(call.id, "🔐 Отправил секретные данные")
                except Exception:
                    pass
            return True

        try:
            bot.answer_callback_query(call.id, f"Секрет: {count}/3", show_alert=False)
        except Exception:
            pass
        return True
    except Exception as e:
        log_error(f"handle_o9_secret_triple_click: {e}")
        return False



def _process_trace_enabled() -> bool:
    """Глобальный рубильник PROCESS через Render env. По чатам всё равно по умолчанию выключено."""
    return _env_bool("FIN_PROCESS_TRACE", "1")


def _trace_timestamp() -> str:
    try:
        return now_local().strftime("%H:%M:%S.%f")[:-3]
    except Exception:
        return datetime.now().strftime("%H:%M:%S.%f")[:-3]


def _trace_delete_delay() -> int:
    try:
        return int(os.getenv("FIN_PROCESS_TRACE_DELETE_SECONDS", "120"))
    except Exception:
        return 120


class ProcessTrace:
    """PROCESS-трейс: одно сообщение, которое редактируется и пополняется строками по мере старта этапов."""

    def __init__(self, chat_id: int, title: str):
        self.chat_id = int(chat_id)
        self.title = str(title or "Процесс")
        self.lines = []
        self.message_id = None
        self.enabled = False
        self._last_text = ""
        self._last_edit_ts = 0.0
        try:
            self.enabled = bool(_process_trace_enabled() and is_process_trace_enabled(self.chat_id))
        except Exception:
            self.enabled = False

    def start(self):
        self.lines.append(f"{len(self.lines) + 1}. {_trace_timestamp()} — старт")
        try:
            bot_journal("process_start", self.chat_id, self.title)
        except Exception:
            pass
        if not self.enabled:
            return self
        try:
            text = self._render(running=True)
            if "_tg_call_retry" in globals():
                sent = _tg_call_retry(bot.send_message, self.chat_id, text, purpose="process_trace_start")
            else:
                sent = bot.send_message(self.chat_id, text)
            self.message_id = sent.message_id
            self._last_text = text
            self._last_edit_ts = time.time()
        except Exception as e:
            log_error(f"ProcessTrace start({self.chat_id}): {e}")
            self.enabled = False
        return self

    def step(self, label: str):
        self.lines.append(f"{len(self.lines) + 1}. {_trace_timestamp()} — {label}")
        try:
            bot_journal("process_step", self.chat_id, f"{self.title}: {label}")
        except Exception:
            pass
        self._update_message(running=True)
        return self

    def _render(self, running: bool = False) -> str:
        head = "⏳" if running else "✅"
        # Telegram лимит ~4096. Держим одно сообщение, но если строк очень много — оставляем хвост и пометку.
        lines = list(self.lines)
        hidden = 0
        while lines and len(head + " " + self.title + "\n" + "\n".join(lines)) > 3900:
            lines.pop(0)
            hidden += 1
        if hidden:
            lines.insert(0, f"… скрыто ранних этапов: {hidden}")
        return head + " " + self.title + "\n" + "\n".join(lines)

    def _update_message(self, running: bool = True, force: bool = False):
        if not self.enabled or not self.message_id:
            return
        text = self._render(running=running)
        if text == self._last_text and not force:
            return
        # Редактируем это же сообщение на каждом этапе, чтобы было видно, где именно бот сейчас занят.
        try:
            if "_tg_call_retry" in globals():
                _tg_call_retry(bot.edit_message_text, text, chat_id=self.chat_id, message_id=self.message_id, purpose="process_trace_edit")
            else:
                bot.edit_message_text(text, chat_id=self.chat_id, message_id=self.message_id)
            self._last_text = text
            self._last_edit_ts = time.time()
        except Exception as e:
            err = str(e).lower()
            if "message is not modified" not in err:
                log_error(f"ProcessTrace update({self.chat_id}): {e}")

    def finish(self, final_label: str = "завершено"):
        self.lines.append(f"{len(self.lines) + 1}. {_trace_timestamp()} — {final_label}")
        try:
            bot_journal("process_finish", self.chat_id, f"{self.title}: {final_label}")
        except Exception:
            pass
        if not self.enabled:
            return
        try:
            if self.message_id:
                self._update_message(running=False, force=True)
                delete_message_later(self.chat_id, self.message_id, _trace_delete_delay())
            else:
                send_and_auto_delete(self.chat_id, self._render(running=False), _trace_delete_delay())
        except Exception as e:
            log_error(f"ProcessTrace finish({self.chat_id}): {e}")

    def fail(self, err: Exception):
        self.lines.append(f"{len(self.lines) + 1}. {_trace_timestamp()} — ошибка: {str(err)[:160]}")
        self.finish("остановлено")

def format_error_for_owner(raw) -> str:
    """Для /errors: по возможности заменяет известные chat_id на имена чатов/пользователей."""
    text = str(raw or "")
    try:
        ids = set()
        for cid in (data.get("chats", {}) or {}).keys():
            try:
                ids.add(str(int(cid)))
            except Exception:
                pass
        if OWNER_ID:
            try:
                ids.add(str(int(OWNER_ID)))
            except Exception:
                pass
        if BACKUP_CHAT_ID:
            try:
                ids.add(str(int(BACKUP_CHAT_ID)))
            except Exception:
                pass
        for cid_s in sorted(ids, key=len, reverse=True):
            try:
                name = get_chat_display_name(int(cid_s))
            except Exception:
                continue
            if not name or name == f"Чат {cid_s}":
                continue
            text = re.sub(rf"(?<!\\d){re.escape(cid_s)}(?!\\d)", name, text)
    except Exception:
        pass
    return text

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


# ─────────────────────────────────────────────────────────────
# Метки окон для ориентира при отладке
# о1/о2/... — общие окна, доступные обычным чатам и владельцу.
# в1/в2/... — окна/меню только владельца.
# Метка добавляется внизу справа обычным текстом и не меняет callback-логику.
# ─────────────────────────────────────────────────────────────
WINDOW_MARK_RE = re.compile(r"(?:^|\s)([ов]\d{1,3})\s*$")


def has_window_mark(text: str) -> bool:
    """True, если внизу окна уже есть метка о1/в1 и т.п."""
    try:
        tail = str(text or "")[-120:]
        tail = re.sub(r"<[^>]+>", "", tail)
        return bool(WINDOW_MARK_RE.search(tail))
    except Exception:
        return False


def strip_window_mark(text: str) -> str:
    """Убирает старую метку окна в самом конце, чтобы при смене кнопок окно получило новое имя."""
    try:
        text = str(text or "")
        text = re.sub(r"\n\s*<i>[ов]\d{1,3}</i>\s*$", "", text)
        text = re.sub(r"\n\s*[ов]\d{1,3}\s*$", "", text)
        return text.rstrip()
    except Exception:
        return str(text or "")


def window_mark(text: str, code: str, html_mode: bool = False) -> str:
    try:
        text = str(text or "")
        code = str(code or "").strip()
        if not code:
            return text
        # У каждого перехода/набора кнопок должна быть своя метка.
        # Поэтому старую метку в конце всегда снимаем и ставим новую.
        if has_window_mark(text):
            text = strip_window_mark(text)
        pad = " " * 26
        if html_mode:
            return text + "\n\n" + pad + f"<i>{html.escape(code)}</i>"
        return text + "\n\n" + pad + code
    except Exception:
        return str(text or "")


def wm_common(text: str, n: int, html_mode: bool = False) -> str:
    return window_mark(text, f"о{int(n)}", html_mode=html_mode)


def wm_owner(text: str, n: int, html_mode: bool = False) -> str:
    return window_mark(text, f"в{int(n)}", html_mode=html_mode)


def window_code_for_callback(data_str: str, owner_chat: bool = False) -> str:
    """Запасная маркировка для любых переходных окон, которые забыли пометить вручную."""
    d = str(data_str or "")
    try:
        d = resolve_short_callback(d) or d
    except Exception:
        pass
    # Общие окна.
    if d.startswith("cat_") or d.startswith("catx:"):
        if "cat_show" in d:
            return "о8"
        if "cat_edit" in d:
            return "о14"
        if "cat_del" in d:
            return "о15"
        if "cat_add" in d:
            return "о11"
        if "cat_desc" in d:
            return "о13"
        return "о7"
    if d.startswith("d:"):
        action = d.split(":", 2)[2] if d.count(":") >= 2 else ""
        if action in {"open", "prev", "next", "today", "back_main", "edit_menu", "menu"}:
            return "о1"
        if action == "calendar":
            return "о2"
        if action == "report":
            return "о3"
        if action == "total":
            return "о4"
        if action.startswith(("csv", "xlsx", "bk_")):
            return "о5"
        if action.startswith("edit") or action.startswith("del_") or action in {"cancel_edit"}:
            return "о6"
        if action == "info":
            return "о9"
        if action.startswith("process"):
            return "в20" if owner_chat else "о20"
        if action.startswith("backup"):
            return "в21" if owner_chat else "о21"
        if action == "forward_finmode_menu":
            return "в24" if owner_chat else "о24"
        if action.startswith("fw_finmode_pick_"):
            return "в25" if owner_chat else "о25"
        if action.startswith("qb_finwin_open_") or action.startswith("finwin"):
            return "в26" if owner_chat else "о26"
        if action.startswith("qb_hidden_toggle_"):
            return "в31" if owner_chat else "о31"
        if action.startswith("qb_mode_normal_"):
            return "в32" if owner_chat else "о32"
        if action.startswith("qb_mode_open_"):
            return "в33" if owner_chat else "о33"
        if action.startswith("qb_mode_first_"):
            return "в34" if owner_chat else "о34"
        if action.startswith("fin_mode_toggle_") or action.startswith("fin_mode_off_"):
            return "в35" if owner_chat else "о35"
        if action.startswith(("fin_mode_", "qb_")) or action == "quick_balance_menu":
            return "в25" if owner_chat else "о25"
        if action == "forward_menu":
            return "в22" if owner_chat else "о22"
        if action.startswith("forward") or action.startswith("fw_"):
            return "в22" if owner_chat else "о22"
        if action.startswith("hf_") or action == "hidden_finance_menu":
            return "в25" if owner_chat else "о25"
        if action.startswith("finwin"):
            return "в26" if owner_chat else "о26"
    # Окна владельца.
    if d.startswith("fvcat_") or d.startswith("fvcatx:"):
        if "fvcat_show" in d:
            return "в10"
        return "в9"
    if d.startswith("fv:"):
        parts = d.split(":")
        action = parts[3] if len(parts) > 3 else ""
        if action == "open":
            return "в6"
        if action in {"csv_menu"} or action.startswith(("csv_", "xlsx_", "bk_")):
            return "в11"
        if action == "calendar":
            return "в12"
        if action == "report":
            return "в13"
        if action == "total":
            return "в14"
        if action.startswith("edit") or action.startswith("del_"):
            return "в15"
        if action == "info":
            return "в16"
        return "в6"
    if d.startswith("rep"):
        return "о3"
    if d == "forward_menu_style_toggle":
        return "о9"
    if d.startswith("fw_new_pair:") or d.startswith("fw_new_tgt:") or d.startswith("fw_new_mode:") or d.startswith("fw_new_fin:") or d.startswith("fw_new_clear:"):
        return "в27" if owner_chat else "о27"
    if d.startswith("fw_new_src:"):
        return "в23" if owner_chat else "о23"
    if d == "fw_new_back_src":
        return "в22" if owner_chat else "о22"
    if d == "fw_probe_all":
        return "в29" if owner_chat else "о29"
    if d == "fw_removed_list" or d.startswith("fw_probe_one:"):
        return "в28" if owner_chat else "о28"
    if d in {"fw_open", "fw_back_root", "fw_back_src"}:
        return "в22" if owner_chat else "о22"
    if d.startswith("fw_src:") or d.startswith("fw_back_tgt:"):
        return "в23" if owner_chat else "о23"
    if d.startswith("fw_tgt:") or d.startswith("fw_mode:") or d.startswith("fw_finpair:"):
        return "в27" if owner_chat else "о27"
    if d.startswith("fw_"):
        return "в22" if owner_chat else "о22"
    if d.startswith("journal_"):
        return "в30" if owner_chat else "о30"
    if d.startswith("articles_desc"):
        return "о13"
    return "в98" if owner_chat else "о98"


def auto_window_mark(text: str, data_str: str = "", owner_chat: bool = False, html_mode: bool = False) -> str:
    code = window_code_for_callback(data_str, owner_chat=owner_chat)
    # Если то же текстовое окно открыто с другими кнопками, старую метку заменяем на новую.
    # Это даёт индивидуальное имя каждому переходу/клавиатуре, как в ТЗ.
    if has_window_mark(text):
        text = strip_window_mark(text)
    return window_mark(text, code, html_mode=html_mode)


_v98_auto_close_timers = {}
_v98_auto_close_lock = threading.RLock()


def _cancel_v98_auto_close(chat_id: int, message_id: int):
    key = (int(chat_id), int(message_id))
    with _v98_auto_close_lock:
        t = _v98_auto_close_timers.pop(key, None)
    if t and getattr(t, "is_alive", lambda: False)():
        try:
            t.cancel()
        except Exception:
            pass


def _schedule_v98_auto_close(chat_id: int, message_id: int, delay: int = 60):
    """Окна о98/в98 закрываются через минуту бездействия. Любая новая кнопка на этом же окне сбрасывает таймер."""
    chat_id = int(chat_id)
    message_id = int(message_id)
    _cancel_v98_auto_close(chat_id, message_id)

    def _job():
        with _v98_auto_close_lock:
            _v98_auto_close_timers.pop((chat_id, message_id), None)
        try:
            bot.delete_message(chat_id, message_id)
        except Exception:
            pass

    t = threading.Timer(int(delay), _job)
    with _v98_auto_close_lock:
        _v98_auto_close_timers[(chat_id, message_id)] = t
    t.start()


def _touch_v98_auto_close_for_callback(chat_id: int, message_id: int, data_str: str):
    try:
        code = window_code_for_callback(data_str, owner_chat=is_owner_chat(chat_id))
        if code in {"о98", "в98"}:
            _schedule_v98_auto_close(chat_id, message_id, 60)
        else:
            _cancel_v98_auto_close(chat_id, message_id)
    except Exception:
        pass

DAY_WINDOW_MAX_RECORDS = 35
DAY_WINDOW_MAX_CHARS = 3500

BALANCE_PANEL_REFRESH_DELAY = 5.0
BALANCE_PANEL_COLLAPSE_DELAY = 90.0
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

    
def fmt_date_ddmmyy(day_key: str) -> str:
    """YYYY-MM-DD -> DD.MM.YY"""
    try:
        d = datetime.strptime(day_key, "%Y-%m-%d")
        return d.strftime("%d.%m.%y")
    except Exception:
        return str(day_key)

def fmt_date_backup(day_key: str) -> str:
    """Формат даты для backup-файлов: DD:MM:YY. Внутренний day_key YYYY-MM-DD сохраняем отдельно."""
    try:
        d = datetime.strptime(str(day_key)[:10], "%Y-%m-%d")
        return d.strftime("%d:%m:%y")
    except Exception:
        return str(day_key)


def backup_record_copy(rec: dict) -> dict:
    """Копия записи для JSON-бэкапа: добавляем date в формате DD:MM:YY, не ломая day_key для восстановления."""
    try:
        rr = json.loads(json.dumps(rec or {}, ensure_ascii=False, default=str))
    except Exception:
        rr = dict(rec or {})
    dk = rr.get("day_key") or _record_day_key(rr) if isinstance(rr, dict) else today_key()
    rr["date"] = fmt_date_backup(dk)
    return rr


def backup_records_list(records) -> list:
    return [backup_record_copy(r) for r in (records or []) if isinstance(r, dict)]


def backup_daily_records(daily: dict) -> dict:
    """JSON-friendly daily_records с прежними ключами YYYY-MM-DD и дополнительными date в записях."""
    out = {}
    for dk in sorted((daily or {}).keys()):
        out[str(dk)] = backup_records_list((daily or {}).get(dk, []))
    return out


def message_timestamp_iso(source_msg=None) -> str:
    """Для хронологии берём Telegram msg.date, а не время обработки потока."""
    try:
        msg_date = getattr(source_msg, "date", None)
        if msg_date:
            return datetime.fromtimestamp(int(msg_date), tz=get_tz()).isoformat(timespec="seconds")
    except Exception:
        pass
    return now_local().isoformat(timespec="seconds")


def record_sort_key(rec: dict):
    """Устойчивая сортировка: дата → время Telegram → исходный message_id → внутренний id."""
    try:
        order_msg = int(rec.get("source_order_msg_id") or rec.get("source_msg_id") or rec.get("origin_msg_id") or rec.get("msg_id") or 0)
    except Exception:
        order_msg = 0
    try:
        rid = int(rec.get("id", 0) or 0)
    except Exception:
        rid = 0
    return (str(rec.get("day_key", "")), str(rec.get("timestamp", "")), order_msg, rid)


def compose_edit_input_value(amount, note: str = "") -> str:
    """Готовая строка для ручного редактирования записи."""
    try:
        amount = float(amount or 0)
    except Exception:
        amount = 0.0
    note = (note or "").strip()
    if amount > 0:
        base = "+" + fmt_num_compact(amount)
    elif amount < 0:
        # Для расхода можно отправить без минуса: парсер всё равно считает это расходом.
        base = fmt_num_compact(abs(amount))
    else:
        base = "0"
    return (base + (" " + note if note else "")).strip()

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
    """Понимает новый CSV-формат и старые +/- значения.

    ВАЖНО: fmt_csv_amount() пишет приход как "+ 123".
    Раньше здесь было s[5:], из-за чего для "+ 123" получалась пустая строка
    и Excel-экспорт по периодам падал с ошибкой: could not convert string to float: ''.
    """
    s = str(raw or "").strip()
    if not s:
        return 0.0

    # Поддержка возможных визуальных плюсов/минусов из старых выгрузок.
    s = s.replace("➕", "+").replace("➖", "-").strip()

    if s.startswith("+"):
        num = s[1:].strip()
        if not num:
            return 0.0
        return abs(parse_amount("+" + num))

    if s.startswith(("-", "–")):
        return parse_amount(s)

    # Если вдруг в CSV пришло число без знака — это расход, как и в обычном вводе.
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
        if is_primary_owner(chat_id):
            return "🏀"
        store = get_chat_store(chat_id)
        info = store.get("info", {}) or {}
        title = (info.get("title") or "").strip()
        username = (info.get("username") or "").strip()
        if title and title != f"Чат {chat_id}":
            return title
        if username:
            return f"@{username.lstrip('@')}"
        if title:
            return title
    except Exception:
        pass
    return f"Чат {chat_id}"


def _chat_title_from_message(msg, previous_title: str = "") -> str:
    """Название для меню: у владельца 🏀, в личке — имя/username, в группе — title."""
    try:
        chat_id = msg.chat.id
        if is_primary_owner(chat_id):
            return "🏀"

        chat_title = getattr(msg.chat, "title", None)
        if chat_title:
            return str(chat_title).strip()

        user = getattr(msg, "from_user", None)
        if user is not None:
            if getattr(user, "is_bot", False):
                if previous_title and not str(previous_title).startswith("Чат "):
                    return previous_title
            else:
                first = (getattr(user, "first_name", None) or "").strip()
                last = (getattr(user, "last_name", None) or "").strip()
                full = (first + " " + last).strip()
                if full:
                    return full
                username = (getattr(user, "username", None) or "").strip()
                if username:
                    return f"@{username.lstrip('@')}"

        if previous_title and not str(previous_title).startswith("Чат "):
            return previous_title
    except Exception:
        pass
    return f"Чат {getattr(getattr(msg, 'chat', None), 'id', '')}".strip()


def _chat_username_from_message(msg):
    try:
        username = getattr(msg.chat, "username", None)
        if username:
            return str(username).lstrip("@")
        user = getattr(msg, "from_user", None)
        if user is not None and not getattr(user, "is_bot", False) and getattr(user, "username", None):
            return str(user.username).lstrip("@")
    except Exception:
        pass
    return None


def format_finance_mode_label(chat_id: int) -> str:
    return "ВКЛ ✅" if is_finance_mode(chat_id) else "ВЫКЛ ❌"


def info_finance_toggle_label(chat_id: int) -> str:
    return "✅ Фин режим ВКЛ" if is_finance_mode(chat_id) else "❌ Фин режим ВЫКЛ"


def is_quick_balance_enabled(chat_id: int) -> bool:
    store = get_chat_store(chat_id)
    settings = store.setdefault("settings", {})
    return bool(settings.get("quick_balance_enabled", False))


def get_quick_balance_behavior(chat_id: int) -> str:
    store = get_chat_store(chat_id)
    settings = store.setdefault("settings", {})
    behavior = (settings.get("quick_balance_behavior") or "normal").strip().lower()
    if behavior in {"normal", "mini", "open", "first"}:
        return behavior
    return "normal"


def set_quick_balance_behavior(chat_id: int, behavior: str):
    store = get_chat_store(chat_id)
    settings = store.setdefault("settings", {})
    behavior = str(behavior or "normal").strip().lower()
    if behavior not in {"normal", "mini", "open", "first"}:
        behavior = "normal"
    settings["quick_balance_behavior"] = behavior
    settings["quick_balance_user_selected"] = True
    save_data(data)
    schedule_config_backup_for_chats(chat_id)
    if behavior == "first":
        schedule_quick_balance_first_recreate(chat_id)


def set_quick_balance_enabled(chat_id: int, enabled: bool):
    chat_id = int(chat_id)
    store = get_chat_store(chat_id)
    settings = store.setdefault("settings", {})
    enabled = bool(enabled)
    # Скрытый режим независим от быстрого остатка: не выключаем hidden_finance здесь.
    settings["quick_balance_enabled"] = enabled

    if enabled:
        set_finance_mode(chat_id, True)
        if store.get("balance_panel_mode") not in {"mini", "open"}:
            store["balance_panel_mode"] = "mini"
        save_data(data)
        schedule_config_backup_for_chats(chat_id)
        schedule_balance_panel_refresh(chat_id, 0.1)
        return

    panel_id = store.get("balance_panel_id")
    if panel_id:
        try:
            bot.delete_message(chat_id, panel_id)
        except Exception:
            pass
    store["balance_panel_id"] = None
    store["balance_panel_mode"] = "normal"
    settings["quick_balance_behavior"] = "normal"
    save_data(data)
    schedule_config_backup_for_chats(chat_id)


def is_hidden_finance_mode(chat_id: int) -> bool:
    try:
        store = get_chat_store(chat_id)
        return bool(store.setdefault("settings", {}).get("hidden_finance", False))
    except Exception:
        return False


def is_finance_output_suppressed(chat_id: int) -> bool:
    """Скрытый финрежим: учёт остаётся, но в самом чате ничего финансового не выводим."""
    try:
        return bool(is_hidden_finance_mode(chat_id) and not is_owner_chat(chat_id))
    except Exception:
        return False


def _backup_target_setting_key(target: str) -> str:
    target = str(target or "").strip().lower()
    if target in {"chat", "owner", "self"}:
        return "auto_backup_to_chat_enabled"
    if target in {"channel", "backup_channel"}:
        return "auto_backup_to_channel_enabled"
    if target in {"mega", "cloud"}:
        return "auto_backup_to_mega_enabled"
    return "auto_backup_enabled"


def _ensure_backup_settings(chat_id: int) -> dict:
    store = get_chat_store(chat_id)
    settings = store.setdefault("settings", {})
    legacy = bool(settings.get("auto_backup_enabled", True))
    settings.setdefault("auto_backup_enabled", legacy)
    settings.setdefault("auto_backup_to_chat_enabled", legacy)
    settings.setdefault("auto_backup_to_channel_enabled", legacy)
    settings.setdefault("auto_backup_to_mega_enabled", legacy)
    return settings


def is_backup_target_enabled(chat_id: int, target: str) -> bool:
    try:
        settings = _ensure_backup_settings(chat_id)
        return bool(settings.get(_backup_target_setting_key(target), True))
    except Exception:
        return True


def is_backup_to_chat_enabled(chat_id: int) -> bool:
    return is_backup_target_enabled(chat_id, "chat")


def is_backup_to_channel_enabled(chat_id: int) -> bool:
    return is_backup_target_enabled(chat_id, "channel")


def is_backup_to_mega_enabled(chat_id: int) -> bool:
    return is_backup_target_enabled(chat_id, "mega")


def is_auto_backup_enabled(chat_id: int) -> bool:
    """Legacy master: True если включён хотя бы один тип авто-бэкапа."""
    try:
        return any((
            is_backup_to_chat_enabled(chat_id),
            is_backup_to_channel_enabled(chat_id),
            is_backup_to_mega_enabled(chat_id),
        ))
    except Exception:
        return True


def set_backup_target_enabled(chat_id: int, target: str, enabled: bool):
    settings = _ensure_backup_settings(chat_id)
    settings[_backup_target_setting_key(target)] = bool(enabled)
    settings["auto_backup_enabled"] = any((
        bool(settings.get("auto_backup_to_chat_enabled", True)),
        bool(settings.get("auto_backup_to_channel_enabled", True)),
        bool(settings.get("auto_backup_to_mega_enabled", True)),
    ))
    save_data(data)
    schedule_config_backup_for_chats(chat_id)


def set_auto_backup_enabled(chat_id: int, enabled: bool):
    """Совместимость: старое включение/выключение теперь меняет все три бэкапа сразу."""
    settings = _ensure_backup_settings(chat_id)
    enabled = bool(enabled)
    settings["auto_backup_enabled"] = enabled
    settings["auto_backup_to_chat_enabled"] = enabled
    settings["auto_backup_to_channel_enabled"] = enabled
    settings["auto_backup_to_mega_enabled"] = enabled
    save_data(data)
    schedule_config_backup_for_chats(chat_id)


def _ensure_process_settings(chat_id: int) -> dict:
    """Настройка PROCESS по чатам. По умолчанию выключено, но сохраняется в JSON/SQLite."""
    store = get_chat_store(chat_id)
    settings = store.setdefault("settings", {})
    settings.setdefault("process_trace_enabled", False)
    return settings


def is_process_trace_enabled(chat_id: int) -> bool:
    try:
        settings = _ensure_process_settings(chat_id)
        return bool(settings.get("process_trace_enabled", False))
    except Exception:
        return False


def set_process_trace_enabled(chat_id: int, enabled: bool):
    settings = _ensure_process_settings(chat_id)
    settings["process_trace_enabled"] = bool(enabled)
    save_data(data)
    # Настройка тоже должна уехать в JSON-бэкап, но сам PROCESS при этом не блокирует основной поток.
    schedule_config_backup_for_chats(chat_id)


def toggle_process_trace(chat_id: int) -> bool:
    new_value = not is_process_trace_enabled(chat_id)
    set_process_trace_enabled(chat_id, new_value)
    return new_value


def _is_bot_removed_error(err) -> bool:
    text = str(err or "").lower()
    needles = (
        "bot was kicked",
        "bot was blocked",
        "user is deactivated",
        "chat not found",
        "forbidden",
        "not enough rights",
        "have no rights",
    )
    return any(n in text for n in needles)


def set_chat_bot_removed(chat_id: int, removed: bool = True, reason: str = ""):
    try:
        store = get_chat_store(int(chat_id))
        settings = store.setdefault("settings", {})
        if bool(settings.get("bot_removed", False)) == bool(removed) and not reason:
            return
        settings["bot_removed"] = bool(removed)
        if removed:
            settings["bot_removed_reason"] = str(reason or "bot removed")[:300]
            settings["bot_removed_at"] = now_local().isoformat(timespec="seconds")
        else:
            settings.pop("bot_removed_reason", None)
            settings.pop("bot_removed_at", None)
        save_data(data)
        try:
            ids_for_backup = [int(chat_id)]
            if OWNER_ID and str(chat_id) != str(OWNER_ID):
                ids_for_backup.append(int(OWNER_ID))
            schedule_config_backup_for_chats(*ids_for_backup, delay=1.0)
        except Exception:
            pass
        try:
            bot_journal("bot_removed_state", int(chat_id), f"removed={removed} {reason}")
        except Exception:
            pass
    except Exception as e:
        log_error(f"set_chat_bot_removed({chat_id}): {e}")


def is_chat_bot_removed(chat_id: int) -> bool:
    try:
        store = get_chat_store(int(chat_id))
        return bool(store.setdefault("settings", {}).get("bot_removed", False))
    except Exception:
        return False


def chat_button_title(chat_id: int, title: str | None = None) -> str:
    title = title or get_chat_display_name(chat_id)
    return ("➖ " if is_chat_bot_removed(chat_id) else "") + str(title)


def answer_removed_chat(call, target_chat_id: int) -> bool:
    if not is_chat_bot_removed(target_chat_id):
        return False
    txt = f"➖ Бот удалён из чата: {get_chat_display_name(target_chat_id)}"
    try:
        bot.answer_callback_query(call.id, txt, show_alert=True)
    except Exception:
        pass
    try:
        send_and_auto_delete(call.message.chat.id, txt, 12)
    except Exception:
        pass
    return True


def collect_all_known_chat_ids(include_owner: bool = True) -> list[int]:
    """Все известные чаты из памяти/пересылок/финрежима для проверки наличия бота."""
    ids = set()
    try:
        for cid in (data.get("chats", {}) or {}).keys():
            ids.add(int(cid))
    except Exception:
        pass
    try:
        for cid in (collect_forward_menu_chats() or {}).keys():
            ids.add(int(cid))
    except Exception:
        pass
    try:
        fr = data.get("forward_rules", {}) or {}
        for src, dsts in fr.items():
            ids.add(int(src))
            for dst in (dsts or {}).keys():
                ids.add(int(dst))
    except Exception:
        pass
    if OWNER_ID and include_owner:
        try:
            ids.add(int(OWNER_ID))
        except Exception:
            pass
    return sorted(ids, key=lambda cid: get_chat_display_name(cid).lower())


def update_chat_info_from_chat_object(chat_obj) -> bool:
    """Обновляет карточку чата по результату Telegram getChat: title/username/type."""
    try:
        chat_id = int(getattr(chat_obj, "id"))
    except Exception:
        return False
    store = get_chat_store(chat_id)
    info = store.setdefault("info", {})
    prev_title = info.get("title") or ""
    chat_type = getattr(chat_obj, "type", None)
    title = (getattr(chat_obj, "title", None) or "").strip()
    username = (getattr(chat_obj, "username", None) or "").strip().lstrip("@") or None
    if not title:
        first = (getattr(chat_obj, "first_name", None) or "").strip()
        last = (getattr(chat_obj, "last_name", None) or "").strip()
        title = (first + " " + last).strip() or (f"@{username}" if username else prev_title or f"Чат {chat_id}")

    changed = False
    if info.get("title") != title:
        info["title"] = title
        changed = True
    if info.get("username") != username:
        info["username"] = username
        changed = True
    if info.get("type") != chat_type:
        info["type"] = chat_type
        changed = True

    if OWNER_ID and str(chat_id) != str(OWNER_ID):
        owner_store = get_chat_store(int(OWNER_ID))
        kc = owner_store.setdefault("known_chats", {})
        new_known = {"title": title, "username": username, "type": chat_type}
        if kc.get(str(chat_id)) != new_known:
            kc[str(chat_id)] = new_known
            changed = True

    if changed:
        save_data(data)
        try:
            ids_for_backup = [chat_id]
            if OWNER_ID:
                ids_for_backup.append(int(OWNER_ID))
            schedule_config_backup_for_chats(*ids_for_backup, delay=2.0)
        except Exception as e:
            log_error(f"update_chat_info_from_chat_object backup {chat_id}: {e}")
    return changed

def probe_bot_in_chat(chat_id: int) -> bool:
    """Проверяет, видит ли бот чат. При успехе обновляет имя/username, при ошибке помечает как удалённый."""
    try:
        chat_obj = _tg_call_retry(bot.get_chat, int(chat_id), attempts=2, purpose="probe_get_chat")
        update_chat_info_from_chat_object(chat_obj)
        set_chat_bot_removed(int(chat_id), False, "probe ok")
        return True
    except Exception as e:
        if _is_bot_removed_error(e):
            set_chat_bot_removed(int(chat_id), True, str(e)[:240])
        else:
            log_error(f"probe_bot_in_chat({get_chat_display_name(chat_id)}): {e}")
        return False


def probe_all_known_chats() -> tuple[int, int]:
    ok = 0
    bad = 0
    for cid in collect_all_known_chat_ids(include_owner=False):
        if probe_bot_in_chat(cid):
            ok += 1
        elif is_chat_bot_removed(cid):
            bad += 1
    save_data(data)
    schedule_config_backup_for_chats()
    return ok, bad


def build_removed_chats_menu(day_key: str | None = None):
    kb = types.InlineKeyboardMarkup(row_width=2)
    removed = [cid for cid in collect_all_known_chat_ids(include_owner=False) if is_chat_bot_removed(cid)]
    if removed:
        buttons = [IB(chat_button_title(cid, get_chat_display_name(cid)), callback_data=f"fw_probe_one:{cid}") for cid in removed]
        add_buttons_in_rows(kb, buttons, 2)
    else:
        kb.row(IB("Удалённых нет", callback_data="none"))
    kb.row(IB("📡 Проверить все", callback_data="fw_probe_all"))
    kb.row(IB("🔙 Назад", callback_data="fw_back_src" if day_key is None else f"d:{day_key}:forward_menu"))
    return kb


def set_hidden_finance_mode(chat_id: int, enabled: bool):
    chat_id = int(chat_id)
    store = get_chat_store(chat_id)
    settings = store.setdefault("settings", {})
    settings["hidden_finance"] = bool(enabled)

    if enabled:
        set_finance_mode(chat_id, True)
        # Скрытый режим независим от меню быстрого остатка: не меняем выбранный режим.
        panel_id = store.get("balance_panel_id")
        if panel_id:
            try:
                bot.delete_message(chat_id, panel_id)
            except Exception:
                pass
        store["balance_panel_id"] = None
        store["balance_panel_mode"] = "mini"
        # Убираем сохранённые активные окна, чтобы скрытый чат больше не размножал фин-окна.
        try:
            data.setdefault("active_messages", {})[str(chat_id)] = {}
        except Exception:
            pass
    save_data(data)
    schedule_config_backup_for_chats(chat_id)


def force_recreate_balance_panel(chat_id: int):
    """Пересоздаёт быстрый остаток, чтобы он снова стал последним окном в чате."""
    if is_hidden_finance_mode(chat_id):
        return
    if not is_finance_mode(chat_id) or not is_quick_balance_enabled(chat_id):
        return
    store = get_chat_store(chat_id)
    panel_id = store.get("balance_panel_id")
    if panel_id:
        try:
            bot.delete_message(chat_id, int(panel_id))
        except Exception:
            pass
    store["balance_panel_id"] = None
    store["balance_panel_mode"] = "mini"
    store["balance_panel_msg_count"] = 0
    save_data(data)
    send_minimized_balance_panel(chat_id)


def is_normal_finance_window_mode(chat_id: int) -> bool:
    """Как обычно: финокно чата, без быстрого остатка, пересоздаётся после 10 сообщений."""
    try:
        return (not is_quick_balance_enabled(chat_id)) or get_quick_balance_behavior(chat_id) == "normal"
    except Exception:
        return True


def schedule_main_window_recreate_after_quiet(chat_id: int, delay: float = 4.0):
    try:
        chat_id = int(chat_id)
    except Exception:
        return
    if is_hidden_finance_mode(chat_id):
        return
    if not is_finance_mode(chat_id):
        return

    def _job():
        try:
            with locked_chat(chat_id):
                store = get_chat_store(chat_id)
                if int(store.get("main_window_msg_count", 0) or 0) < 10:
                    return
                store["main_window_msg_count"] = 0
                day_key = store.get("current_view_day") or today_key()
                save_data(data)
            # Режим «как обычно»: после 10 сообщений нужно именно ПЕРЕСОЗДАТЬ О1,
            # а не просто отредактировать старое окно. Так окно снова становится последним/видимым.
            recreate_main_window_now(chat_id, day_key)
        except Exception as e:
            log_error(f"schedule_main_window_recreate_after_quiet({get_chat_display_name(chat_id)}): {e}")

    with timer_lock:
        prev = _balance_panel_recreate_timers.get(("main", chat_id))
        if prev and prev.is_alive():
            try:
                prev.cancel()
            except Exception:
                pass
        t = threading.Timer(delay, _job)
        _balance_panel_recreate_timers[("main", chat_id)] = t
        t.start()


def bump_quick_balance_recreate_counter(chat_id: int, count: int = 1):
    """Сообщения после ввода: обычное окно через 10 сообщений или быстрый остаток по выбранному режиму."""
    try:
        if is_hidden_finance_mode(chat_id):
            return
        if not is_finance_mode(chat_id):
            return

        if is_normal_finance_window_mode(chat_id):
            store = get_chat_store(chat_id)
            cur = int(store.get("main_window_msg_count", 0) or 0) + int(count or 1)
            store["main_window_msg_count"] = cur
            save_data(data)
            if cur >= 10:
                schedule_main_window_recreate_after_quiet(chat_id, delay=4.0)
            return

        if not is_quick_balance_enabled(chat_id):
            return

        # Режим «всегда первым»: отдельный минутный таймер после последнего сообщения.
        if get_quick_balance_behavior(chat_id) == "first":
            schedule_quick_balance_first_recreate(chat_id)

        store = get_chat_store(chat_id)
        cur = int(store.get("balance_panel_msg_count", 0) or 0) + int(count or 1)
        store["balance_panel_msg_count"] = cur
        save_data(data)

        # Если сообщений уже 3 или больше — ставим debounce, а не удаляем/создаём в шквале.
        if cur >= 3:
            schedule_quick_balance_recreate_after_quiet(chat_id, delay=4.0)
    except Exception as e:
        log_error(f"bump_quick_balance_recreate_counter({get_chat_display_name(chat_id)}): {e}")


def schedule_quick_balance_first_recreate(chat_id: int, delay: float = 60.0):
    """Режим «всегда быть первым»: если минуту нет новых сообщений, пересоздаём быстрый остаток."""
    try:
        chat_id = int(chat_id)
    except Exception:
        return
    if is_hidden_finance_mode(chat_id):
        return
    if not is_finance_mode(chat_id) or not is_quick_balance_enabled(chat_id):
        return
    if get_quick_balance_behavior(chat_id) != "first":
        return

    def _job():
        try:
            with locked_chat(chat_id):
                if is_hidden_finance_mode(chat_id):
                    return
                if not is_finance_mode(chat_id) or not is_quick_balance_enabled(chat_id):
                    return
                if get_quick_balance_behavior(chat_id) != "first":
                    return
                force_recreate_balance_panel(chat_id)
        except Exception as e:
            log_error(f"schedule_quick_balance_first_recreate({chat_id}): {e}")

    with timer_lock:
        prev = _balance_panel_first_timers.get(chat_id)
        if prev and prev.is_alive():
            try:
                prev.cancel()
            except Exception:
                pass
        t = threading.Timer(delay, _job)
        _balance_panel_first_timers[chat_id] = t
        t.start()



def schedule_quick_balance_recreate_after_quiet(chat_id: int, delay: float = 4.0):
    """Debounce для быстрого остатка: пересоздать только когда поток сообщений стих."""
    try:
        chat_id = int(chat_id)
    except Exception:
        return
    if is_hidden_finance_mode(chat_id):
        return
    if not is_finance_mode(chat_id) or not is_quick_balance_enabled(chat_id):
        return

    def _job():
        try:
            with locked_chat(chat_id):
                store = get_chat_store(chat_id)
                if int(store.get("balance_panel_msg_count", 0) or 0) < 3:
                    return
                store["balance_panel_msg_count"] = 0
                save_data(data)
                force_recreate_balance_panel(chat_id)
        except Exception as e:
            log_error(f"schedule_quick_balance_recreate_after_quiet({get_chat_display_name(chat_id)}): {e}")

    with timer_lock:
        prev = _balance_panel_recreate_timers.get(chat_id)
        if prev and prev.is_alive():
            try:
                prev.cancel()
            except Exception:
                pass
        t = threading.Timer(delay, _job)
        _balance_panel_recreate_timers[chat_id] = t
        t.start()


def _set_panel_open_state(chat_id: int, message_id: int):
    store = get_chat_store(chat_id)
    store["balance_panel_id"] = message_id
    store["balance_panel_mode"] = "open"
    store["balance_panel_msg_count"] = 0
    save_data(data)
    schedule_balance_panel_collapse(chat_id)

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


def default_window_nav_keyboard(chat_id: int):
    """Кнопки для окон, где раньше не было кнопок: закрыть + назад в основное окно."""
    kb = types.InlineKeyboardMarkup()
    day = get_chat_store(chat_id).get("current_view_day") or today_key()
    kb.row(
        IB("⬅️ Назад осн. окно", callback_data=f"d:{day}:back_main"),
        IB("❌ Закрыть", callback_data="aux_close"),
    )
    return kb


def send_or_edit_stored_window(chat_id: int, store_key: str, text: str, reply_markup=None, parse_mode=None, delay: int = AUX_WINDOW_DELETE_DELAY):
    store = get_chat_store(chat_id)
    if reply_markup is None:
        try:
            reply_markup = default_window_nav_keyboard(chat_id)
        except Exception:
            pass
    try:
        mark_map = {
            "report_window_id": 3,
            "total_msg_id": 4,
            "info_msg_id": 9,
            "categories_msg_id": 7,
            "calendar_msg_id": 2,
        }
        if str(store_key) in mark_map:
            text = wm_common(text, mark_map[str(store_key)], html_mode=(str(parse_mode or "").upper() == "HTML"))
        else:
            text = auto_window_mark(text, str(store_key), owner_chat=is_owner_chat(chat_id), html_mode=(str(parse_mode or "").upper() == "HTML"))
    except Exception:
        pass
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
        except Exception as e:
            if "message is not modified" in str(e).lower():
                schedule_stored_window_delete(chat_id, store_key, delay)
                return message_id
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
            except Exception as e2:
                if "message is not modified" in str(e2).lower():
                    schedule_stored_window_delete(chat_id, store_key, delay)
                    return message_id
                store[store_key] = None
                save_data(data)

    sent = bot.send_message(chat_id, text, reply_markup=reply_markup, parse_mode=parse_mode)
    store[store_key] = sent.message_id
    save_data(data)
    schedule_stored_window_delete(chat_id, store_key, delay)
    return sent.message_id


def is_primary_owner(chat_id: int) -> bool:
    return bool(OWNER_ID and str(chat_id) == str(OWNER_ID))


def get_additional_owner_ids() -> set[int]:
    try:
        raw = data.setdefault("_global_settings", {}).setdefault("additional_owner_ids", [])
        return {int(x) for x in raw}
    except Exception:
        return set()


def set_additional_owner(user_id: int, enabled: bool):
    user_id = int(user_id)
    owners = get_additional_owner_ids()
    if enabled:
        owners.add(user_id)
        finance_active_chats.add(user_id)
        get_chat_store(user_id)
    else:
        owners.discard(user_id)
    data.setdefault("_global_settings", {})["additional_owner_ids"] = sorted(owners)
    save_data(data)
    schedule_config_backup_for_chats(user_id)


def is_owner_chat(chat_id: int) -> bool:
    try:
        return is_primary_owner(chat_id) or int(chat_id) in get_additional_owner_ids()
    except Exception:
        return is_primary_owner(chat_id)


def is_backup_channel_chat(chat_id: int) -> bool:
    """True только для служебного backup-канала, если он задан."""
    return bool(BACKUP_CHAT_ID and str(chat_id) == str(BACKUP_CHAT_ID))


def can_receive_direct_json_backup(chat_id: int) -> bool:
    """JSON прямо в чат отправляем только владельцу или в backup-канал."""
    return is_owner_chat(chat_id) or is_backup_channel_chat(chat_id)


def schedule_command_delete(msg):
    try:
        bot_journal("command_received", msg.chat.id, getattr(msg, "text", ""))
    except Exception:
        pass
    try:
        delete_message_later(msg.chat.id, msg.message_id, COMMAND_DELETE_DELAY)
    except Exception:
        pass


def guard_non_owner_finance_for_command(msg, allowed_commands=None) -> bool:
    allowed = {c.lower().lstrip('/') for c in (allowed_commands or [])}
    chat_id = msg.chat.id
    if is_owner_chat(chat_id):
        return False
    if is_finance_output_suppressed(chat_id):
        return True

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
    if is_finance_output_suppressed(chat_id):
        return True
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
        "/ok — включить финансовый режим",
        "/start — окно сегодняшнего дня",
        "/prev — предыдущий день",
        "/next — следующий день",
        "/balance — баланс по этому чату",
        "/report — краткий отчёт по дням",
        "/csv — CSV этого чата",
        "/xlsx — Excel этого чата",
        "/tabl_lsx — таблица за последние 4 недели Чт–Ср",
        "/json — JSON этого чата",
        "/reset — обнулить данные чата (с подтверждением)",
        "/ping — проверка, жив ли бот",
        "/restore / /restore_off — режим восстановления JSON/CSV",
        "/dozvon — окно дозвона по связанным чатам",
    ]
    if is_owner_chat(chat_id):
        lines.extend([
            "/stopforward — отключить пересылку",
            "/backup_channel_on / _off — включить/выключить бэкап в канал",
            "/diag — диагностика бота",
            "/errors — последние ошибки",
            "/journal — скачать журнал действий бота",
            "/articles — описание статей: статья = ключевые слова",
            "/mega_status — статус MEGA/MEGAcmd",
            "/mega_backup_now — сразу загрузить latest_global.json в MEGA",
            "/buttons — переключить кнопки: text/icons",
            "/mask — переключить маскировку тотального секрета",
        ])
    lines.append("/help — эта справка")
    return "\n".join(lines)


def build_info_text(chat_id: int) -> str:
    state = "ВКЛ" if chat_buttons_current_window_enabled(chat_id) else "ВЫКЛ"
    text = build_help_text(chat_id) + f"\n/windows — открывать действия в текущем окне: {state}"
    if is_owner_chat(chat_id):
        text += f"\n/buttons — сейчас: {'значки' if icon_button_mode_enabled() else 'текст'}"
        text += f"\n/mask — сейчас: {'ВКЛ' if total_secret_mask_enabled() else 'ВЫКЛ'}"
    return text


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
        buttons.append(IB(
            chat_button_title(cid, get_chat_display_name(cid)),
            callback_data=f"dzv:{cid}"
        ))
    if buttons:
        add_buttons_in_rows(kb, buttons, 3)
    kb.row(
        IB("⬅️ Назад осн. окно", callback_data=f"d:{get_chat_store(chat_id).get('current_view_day', today_key())}:back_main"),
        IB("❌ Закрыть", callback_data="dzv:close"),
    )
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


def _direction_state_label(enabled: bool, left: str, arrow: str, right: str) -> str:
    icon = "✅" if enabled else "❌"
    return f"{icon} {left} {arrow} {right}"


def _forward_arrow_icon(ab_on: bool, ba_on: bool) -> str:
    if ab_on and ba_on:
        return "🔄"
    if ab_on:
        return "⏩️"
    if ba_on:
        return "⏪️"
    return "❌"


def _forward_fin_icon(ab_fin: bool, ba_fin: bool) -> str:
    if ab_fin and ba_fin:
        return "💰🔄"
    if ab_fin:
        return "💰▶️"
    if ba_fin:
        return "💰◀️"
    return "❌"


def build_forward_status_lines() -> list[str]:
    """Статус В22: короткая схема связей.
    Всегда показываем Чат A первым:
    Чат A -(⏩️/⏪️/🔄)-(💰▶️/💰◀️/💰🔄/❌)-Чат B
    """
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

    all_pairs = set()
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
            all_pairs.add(_sorted_pair(src_id, dst_id))

    for a_id, b_id in sorted(all_pairs, key=lambda p: (get_chat_display_name(p[0]).lower(), get_chat_display_name(p[1]).lower())):
        pair_key = (a_id, b_id)
        if pair_key in seen_pairs:
            continue
        seen_pairs.add(pair_key)

        ab_on = str(b_id) in (fr.get(str(a_id), {}) or {})
        ba_on = str(a_id) in (fr.get(str(b_id), {}) or {})
        if not (ab_on or ba_on):
            continue

        ab_fin = bool((ff.get(str(a_id), {}) or {}).get(str(b_id), False))
        ba_fin = bool((ff.get(str(b_id), {}) or {}).get(str(a_id), False))
        name_a = chat_button_title(a_id)
        name_b = chat_button_title(b_id)
        lines.append(f"• {name_a} -({_forward_arrow_icon(ab_on, ba_on)})-({_forward_fin_icon(ab_fin, ba_fin)})-{name_b}")

    if not lines:
        lines.append("• Связи пересылки не настроены")
    return lines

def build_forward_status_text(title: str | None = None) -> str:
    lines = []
    if title:
        lines.append(title)
        lines.append("")
    # Короткая подсказка для окна В22, чтобы установка пересылки была понятнее.
    if title and "Пересылка" in str(title):
        lines.append("Шаги: 1) выберите чат A → 2) выберите чат B → 3) включите 📨 пересылку и 💰 финучёт пересылки по нужным направлениям.")
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


_telegram_send_last_ts = {}
_telegram_send_rate_lock = threading.RLock()


def _telegram_retry_after_seconds(err: Exception):
    """Достаёт retry_after из Telegram 429: Too Many Requests."""
    try:
        result_json = getattr(err, "result_json", None) or {}
        params = result_json.get("parameters") or {}
        if "retry_after" in params:
            return int(params.get("retry_after") or 0)
    except Exception:
        pass
    text = str(err or "")
    m = re.search(r"retry after\s+(\d+)", text, re.I)
    if m:
        try:
            return int(m.group(1))
        except Exception:
            pass
    return None


def _telegram_rate_limit_chat(chat_id, min_gap: float = 0.35):
    """Мягкий лимит отправки в один чат, чтобы реже получать 429 при шквале пересылок."""
    try:
        cid = int(chat_id)
    except Exception:
        return
    with _telegram_send_rate_lock:
        now_ts = time.time()
        prev_ts = float(_telegram_send_last_ts.get(cid, 0) or 0)
        wait = float(min_gap) - (now_ts - prev_ts)
        if wait > 0:
            time.sleep(wait)
        _telegram_send_last_ts[cid] = time.time()


def _tg_first_chat_id(args, kwargs):
    if "chat_id" in kwargs:
        return kwargs.get("chat_id")
    if args:
        return args[0]
    return None


def _tg_call_retry(func, *args, attempts: int = 7, purpose: str = "telegram", **kwargs):
    """
    Telegram API wrapper: если Telegram вернул 429, ждём retry_after и повторяем.
    Это нужно, чтобы пересылка не терялась, а доставлялась позже.
    """
    last_err = None
    for attempt in range(1, int(attempts) + 1):
        try:
            chat_id = _tg_first_chat_id(args, kwargs)
            if chat_id is not None:
                _telegram_rate_limit_chat(chat_id)
            try:
                bot_journal("telegram_api_call", chat_id, f"{purpose}: {getattr(func, '__name__', str(func))} attempt={attempt}/{attempts}")
            except Exception:
                pass
            # Важно для send_document/edit_media: при повторной попытке после 429
            # файловый объект может уже быть прочитан. Возвращаем указатель в начало.
            try:
                for _obj in list(args) + list(kwargs.values()):
                    if hasattr(_obj, "seek"):
                        try:
                            _obj.seek(0)
                        except Exception:
                            pass
            except Exception:
                pass
            _res = func(*args, **kwargs)
            try:
                if chat_id is not None and is_chat_bot_removed(int(chat_id)):
                    set_chat_bot_removed(int(chat_id), False, "telegram api success")
            except Exception:
                pass
            return _res
        except TypeError:
            raise
        except Exception as e:
            last_err = e
            retry_after = _telegram_retry_after_seconds(e)
            if retry_after is None:
                try:
                    chat_id_for_mark = _tg_first_chat_id(args, kwargs)
                    if chat_id_for_mark is not None and _is_bot_removed_error(e):
                        set_chat_bot_removed(int(chat_id_for_mark), True, str(e)[:240])
                except Exception:
                    pass
                raise
            wait = max(1, int(retry_after)) + 1
            log_info(f"[TG 429 RETRY] {purpose}: attempt={attempt}/{attempts}, wait={wait}s, error={str(e)[:220]}")
            try:
                bot_journal("telegram_429_retry", chat_id if 'chat_id' in locals() else None, f"{purpose}: attempt={attempt}/{attempts}, wait={wait}s, error={str(e)[:220]}", "WARN")
            except Exception:
                pass
            if attempt >= int(attempts):
                break
            time.sleep(wait)
    raise last_err


def _call_with_optional_reply(send_func, *args, reply_to_message_id=None, **kwargs):
    if reply_to_message_id:
        for extra in (
            {"reply_to_message_id": int(reply_to_message_id), "allow_sending_without_reply": True},
            {"reply_to_message_id": int(reply_to_message_id)},
            {},
        ):
            try:
                return _tg_call_retry(send_func, *args, purpose="send_with_reply", **kwargs, **extra)
            except TypeError:
                continue
    return _tg_call_retry(send_func, *args, purpose="send", **kwargs)


def build_balance_panel_keyboard(chat_id: int):
    kb = types.InlineKeyboardMarkup()
    bal = get_chat_store(chat_id).get("balance", 0)
    kb.row(IB(
        f"🏦 Остаток: {fmt_num(bal)}",
        callback_data="bp:open"
    ))
    return kb


def _cancel_timer(timer_map: dict, key):
    prev = timer_map.get(key)
    if prev and getattr(prev, "is_alive", lambda: False)():
        try:
            prev.cancel()
        except Exception:
            pass


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
        err = str(e).lower()
        if "message is not modified" not in err:
            log_error(f"collapse_balance_panel({chat_id}): {e}")


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
    if is_hidden_finance_mode(chat_id):
        return
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
            err = str(e).lower()
            if "message is not modified" in err:
                store["balance_panel_mode"] = "mini"
                save_data(data)
                return
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
    if is_hidden_finance_mode(chat_id):
        return
    if not is_finance_mode(chat_id) or not is_quick_balance_enabled(chat_id):
        return

    store = get_chat_store(chat_id)
    panel_id = store.get("balance_panel_id")
    if not panel_id:
        send_minimized_balance_panel(chat_id)
        return

    mode = store.get("balance_panel_mode") or "mini"
    try:
        if mode == "open":
            day_key = store.get("current_view_day", today_key())
            txt, _ = render_day_window(chat_id, day_key)
            bot.edit_message_text(
                txt,
                chat_id=chat_id,
                message_id=panel_id,
                reply_markup=build_main_keyboard(day_key, chat_id),
                parse_mode="HTML"
            )
            _set_panel_open_state(chat_id, panel_id)
        else:
            bot.edit_message_text(
                "📌 Быстрый остаток",
                chat_id=chat_id,
                message_id=panel_id,
                reply_markup=build_balance_panel_keyboard(chat_id)
            )
    except Exception as e:
        err = str(e).lower()
        if "message is not modified" in err:
            if mode == "open":
                schedule_balance_panel_collapse(chat_id)
            return
        log_error(f"refresh_balance_panel_now({chat_id}): {e}")
        # Если старый panel_id стал недоступен — удаляем ссылку и создаём один новый быстрый остаток.
        store["balance_panel_id"] = None
        store["balance_panel_mode"] = "mini"
        save_data(data)
        send_minimized_balance_panel(chat_id)


def schedule_balance_panel_refresh(chat_id: int, delay: float = BALANCE_PANEL_REFRESH_DELAY):
    if is_hidden_finance_mode(chat_id):
        return
    if not is_finance_mode(chat_id) or not is_quick_balance_enabled(chat_id):
        return

    def _job():
        try:
            store = get_chat_store(chat_id)
            if store.get("balance_panel_id"):
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
    if is_hidden_finance_mode(chat_id):
        return
    if not is_finance_mode(chat_id) or not is_quick_balance_enabled(chat_id):
        return

    store = get_chat_store(chat_id)
    day_key = day_key or store.get("current_view_day", today_key())
    store["current_view_day"] = day_key

    try:
        txt, _ = render_day_window(chat_id, day_key)
        bot.edit_message_text(
            txt,
            chat_id=chat_id,
            message_id=message_id,
            reply_markup=build_main_keyboard(day_key, chat_id),
            parse_mode="HTML"
        )
        set_active_window_id(chat_id, day_key, message_id)
        _set_panel_open_state(chat_id, message_id)
    except Exception as e:
        err = str(e).lower()
        if "message is not modified" in err:
            set_active_window_id(chat_id, day_key, message_id)
            _set_panel_open_state(chat_id, message_id)
            return
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


# ─────────────────────────────────────────────────────────────
# MEGA.nz helpers. Работает через официальный MEGAcmd:
# mega-login / mega-mkdir / mega-put / mega-get / mega-whoami.
# ─────────────────────────────────────────────────────────────
def mega_is_configured() -> bool:
    return bool(MEGA_ENABLED and MEGA_EMAIL and MEGA_PASSWORD)


def mega_remote_file_path(filename: str = None) -> str:
    filename = filename or MEGA_LATEST_GLOBAL_NAME
    return MEGA_BACKUP_DIR.rstrip("/") + "/" + filename


def _mega_required_commands():
    return ["mega-login", "mega-whoami", "mega-mkdir", "mega-put", "mega-get", "mega-rm"]


def mega_missing_commands():
    return [cmd for cmd in _mega_required_commands() if shutil.which(cmd) is None]


def _mega_run(cmd: str, args=None, timeout: int | None = None, check: bool = True):
    args = list(args or [])
    exe = shutil.which(cmd)
    if not exe:
        raise RuntimeError(f"MEGAcmd command not found: {cmd}")
    try:
        res = subprocess.run(
            [exe] + args,
            capture_output=True,
            text=True,
            timeout=timeout or MEGA_TIMEOUT,
        )
    except subprocess.TimeoutExpired:
        raise RuntimeError(f"{cmd} timeout after {timeout or MEGA_TIMEOUT}s")
    if check and res.returncode != 0:
        out = (res.stdout or "").strip()
        err = (res.stderr or "").strip()
        msg = (err or out or f"returncode={res.returncode}")[:800]
        # Не печатаем пароль/логин-команду в лог.
        raise RuntimeError(f"{cmd} failed: {msg}")
    return res


def mega_login_if_needed() -> bool:
    if not mega_is_configured():
        return False
    missing = mega_missing_commands()
    if missing:
        raise RuntimeError("MEGAcmd не установлен или команды не в PATH: " + ", ".join(missing))

    try:
        res = _mega_run("mega-whoami", [], check=False, timeout=30)
        text = ((res.stdout or "") + "\n" + (res.stderr or "")).lower()
        if res.returncode == 0 and (MEGA_EMAIL.lower() in text or "account e-mail" in text or "email" in text):
            return True
    except Exception:
        pass

    # Если сессии нет — логинимся. Ошибку не раскрываем с паролем.
    res = _mega_run("mega-login", [MEGA_EMAIL, MEGA_PASSWORD], check=False, timeout=MEGA_TIMEOUT)
    if res.returncode != 0:
        msg = ((res.stderr or "") or (res.stdout or "") or "login failed")[:500]
        raise RuntimeError(f"mega-login failed: {msg}")
    return True


def mega_ensure_remote_dir() -> bool:
    if not mega_login_if_needed():
        return False
    parts = [p for p in MEGA_BACKUP_DIR.strip("/").split("/") if p]
    current = ""
    for part in parts:
        current += "/" + part
        # Если папка уже есть, mega-mkdir может вернуть ошибку — это нормально.
        _mega_run("mega-mkdir", [current], check=False, timeout=30)
    return True


def mega_ensure_remote_path(remote_dir: str) -> bool:
    """Создаёт любую папку в MEGA по полному пути /base/sub/sub2."""
    if not mega_login_if_needed():
        return False
    remote_dir = (remote_dir or MEGA_BACKUP_DIR).strip() or MEGA_BACKUP_DIR
    parts = [p for p in remote_dir.strip("/").split("/") if p]
    current = ""
    for part in parts:
        current += "/" + part
        _mega_run("mega-mkdir", [current], check=False, timeout=30)
    return True


def mega_safe_name(value, fallback: str = "chat") -> str:
    """Безопасное имя файла/папки для MEGA: имя чата + без мусора."""
    try:
        value = str(value or "").strip()
    except Exception:
        value = ""
    if not value:
        value = fallback
    value = value.replace(" ", "_")
    value = re.sub(r"[^0-9A-Za-zА-Яа-я_@.\-]+", "", value)
    value = value.strip("._-")
    return (value or fallback)[:80]


def mega_chat_slug(chat_id: int) -> str:
    try:
        name = get_chat_display_name(chat_id)
    except Exception:
        name = f"chat_{chat_id}"
    safe = mega_safe_name(name, f"chat_{chat_id}")
    # Добавляем chat_id в хвост, чтобы одинаковые названия не перетирали друг друга.
    return f"{safe}_{chat_id}"


def mega_remote_chat_dir(chat_id: int) -> str:
    return f"{MEGA_BACKUP_DIR.rstrip('/')}/{MEGA_CHAT_BACKUP_DIR}/{mega_chat_slug(chat_id)}"


def mega_remote_month_dir(month_key: str) -> str:
    return f"{MEGA_BACKUP_DIR.rstrip('/')}/{MEGA_MONTHLY_BACKUP_DIR}/{month_key}"


def _copy_file_for_mega(src_path: str, dst_name: str) -> str | None:
    """Копия во временный файл с красивым именем, потому что mega-put имя не переименовывает."""
    try:
        if not src_path or not os.path.exists(src_path):
            return None
        os.makedirs(MEGA_LOCAL_TMP_DIR, exist_ok=True)
        dst_path = os.path.join(MEGA_LOCAL_TMP_DIR, dst_name)
        with open(src_path, "rb") as src, open(dst_path, "wb") as dst:
            dst.write(src.read())
        return dst_path
    except Exception as e:
        log_error(f"_copy_file_for_mega({src_path},{dst_name}): {e}")
        return None


def mega_put_replace(local_path: str, remote_dir: str, remote_name: str | None = None) -> bool:
    """Загрузить файл в MEGA с заменой файла того же имени."""
    if not mega_is_configured():
        return False
    if not local_path or not os.path.exists(local_path):
        return False
    try:
        mega_ensure_remote_path(remote_dir)
        upload_path = local_path
        if remote_name and os.path.basename(local_path) != remote_name:
            copied = _copy_file_for_mega(local_path, remote_name)
            if copied:
                upload_path = copied
        final_name = remote_name or os.path.basename(upload_path)
        remote_file = remote_dir.rstrip("/") + "/" + final_name
        _mega_run("mega-rm", [remote_file], check=False, timeout=30)
        _mega_run("mega-put", [upload_path, remote_dir], check=True, timeout=MEGA_TIMEOUT)
        return True
    except Exception as e:
        log_error(f"[MEGA PUT ERROR] {local_path} -> {remote_dir}: {e}")
        return False


def current_month_key() -> str:
    return now_local().strftime("%Y-%m")


def _record_day_key(rec: dict) -> str:
    dk = str((rec or {}).get("day_key") or "").strip()
    if dk:
        return dk[:10]
    ts = str((rec or {}).get("timestamp") or "")
    return ts[:10] if len(ts) >= 10 else today_key()


def calc_opening_balance_for_month(store: dict, month_key: str) -> float:
    """Остаток на начало месяца: сумма всех записей до YYYY-MM-01."""
    start = f"{month_key}-01"
    total = 0.0
    for r in (store.get("records", []) or []):
        try:
            if _record_day_key(r) < start:
                total += float(r.get("amount", 0) or 0)
        except Exception:
            pass
    return total


def month_records_for_chat(store: dict, month_key: str) -> list[dict]:
    out = []
    prefix = month_key + "-"
    for r in (store.get("records", []) or []):
        try:
            if _record_day_key(r).startswith(prefix):
                out.append(r)
        except Exception:
            pass
    return sorted(out, key=lambda r: (_record_day_key(r), str(r.get("timestamp", ""))))


def build_chat_settings_backup_payload(chat_id: int, store: dict | None = None) -> dict:
    """Полная настройка чата для JSON-бэкапа: финрежим, скрытый режим, пересылки, быстрый остаток."""
    store = store or get_chat_store(chat_id)
    cid = str(chat_id)
    with data_lock:
        fr = json.loads(json.dumps(data.get("forward_rules", {}) or {}, ensure_ascii=False, default=str))
        ff = json.loads(json.dumps(data.get("forward_finance", {}) or {}, ensure_ascii=False, default=str))
        fac = json.loads(json.dumps(data.get("finance_active_chats", {}) or {}, ensure_ascii=False, default=str))
        flags = json.loads(json.dumps(data.get("backup_flags", {}) or {}, ensure_ascii=False, default=str))
    incoming_rules = {src: (dsts or {}).get(cid) for src, dsts in fr.items() if cid in (dsts or {})}
    outgoing_rules = fr.get(cid, {}) or {}
    incoming_finance = {src: (dsts or {}).get(cid) for src, dsts in ff.items() if cid in (dsts or {})}
    outgoing_finance = ff.get(cid, {}) or {}
    return {
        "chat_id": int(chat_id),
        "chat_name": get_chat_display_name(chat_id),
        "finance_mode": bool(store.get("finance_mode") or is_finance_mode(chat_id)),
        "settings": store.get("settings", {}) or {},
        "balance_panel_id": store.get("balance_panel_id"),
        "balance_panel_mode": store.get("balance_panel_mode"),
        "current_view_day": store.get("current_view_day"),
        "auto_backup_enabled": is_auto_backup_enabled(chat_id),
        "hidden_finance": is_hidden_finance_mode(chat_id),
        "quick_balance_enabled": is_quick_balance_enabled(chat_id),
        "quick_balance_behavior": get_quick_balance_behavior(chat_id),
        "process_trace_enabled": is_process_trace_enabled(chat_id),
        "forward_rules_outgoing": outgoing_rules,
        "forward_rules_incoming": incoming_rules,
        "forward_finance_outgoing": outgoing_finance,
        "forward_finance_incoming": incoming_finance,
        "global_forward_rules": fr,
        "global_forward_finance": ff,
        "finance_active_chats": fac,
        "backup_flags": flags,
    }


def build_chat_monthly_backup_payload(chat_id: int, month_key: str | None = None) -> dict:
    month_key = month_key or current_month_key()
    store = get_chat_store(chat_id)
    opening = calc_opening_balance_for_month(store, month_key)
    recs = sorted(month_records_for_chat(store, month_key), key=record_sort_key)
    total_income = 0.0
    total_expense = 0.0
    clean_recs = []
    for r in recs:
        rr = backup_record_copy(r)
        amt = float(r.get("amount", 0) or 0)
        if amt >= 0:
            total_income += amt
        else:
            total_expense += -amt
        rr["day_key"] = _record_day_key(r)
        rr["date"] = fmt_date_backup(rr["day_key"])
        clean_recs.append(rr)
    closing = opening + total_income - total_expense
    return {
        "kind": "chat_monthly_backup",
        "version": VERSION,
        "created_at": now_local().isoformat(timespec="seconds"),
        "date_format": "DD:MM:YY",
        "month": month_key,
        "chat_id": int(chat_id),
        "chat_name": get_chat_display_name(chat_id),
        "opening_balance": opening,
        "total_income": total_income,
        "total_expense": total_expense,
        "closing_balance": closing,
        "record_count": len(clean_recs),
        "settings_backup": build_chat_settings_backup_payload(chat_id, store),
        "records": clean_recs,
    }


def save_chat_monthly_backup_files(chat_id: int, month_key: str | None = None) -> dict:
    """Создаёт месячные JSON/CSV/XLSX с остатком на начало месяца и закрытием месяца."""
    month_key = month_key or current_month_key()
    slug = mega_chat_slug(chat_id)
    base = f"{month_key}_{slug}"
    os.makedirs(MEGA_LOCAL_TMP_DIR, exist_ok=True)
    json_path = os.path.join(MEGA_LOCAL_TMP_DIR, base + ".json")
    csv_path = os.path.join(MEGA_LOCAL_TMP_DIR, base + ".csv")
    xlsx_path = os.path.join(MEGA_LOCAL_TMP_DIR, base + ".xlsx")
    payload = build_chat_monthly_backup_payload(chat_id, month_key)

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["month", month_key])
        w.writerow(["chat", payload.get("chat_name")])
        w.writerow(["opening_balance", payload.get("opening_balance")])
        w.writerow(["total_income", payload.get("total_income")])
        w.writerow(["total_expense", payload.get("total_expense")])
        w.writerow(["closing_balance", payload.get("closing_balance")])
        w.writerow([])
        w.writerow(["date", "amount", "note", "id", "short_id", "timestamp", "owner"])
        for r in payload.get("records", []):
            w.writerow([
                r.get("date") or fmt_date_backup(r.get("day_key")),
                r.get("amount"),
                r.get("note", ""),
                r.get("id", ""),
                r.get("short_id", ""),
                r.get("timestamp", ""),
                r.get("owner", ""),
            ])

    rows = [
        ["month", month_key],
        ["chat", payload.get("chat_name")],
        ["opening_balance", payload.get("opening_balance")],
        ["total_income", payload.get("total_income")],
        ["total_expense", payload.get("total_expense")],
        ["closing_balance", payload.get("closing_balance")],
        [],
        ["Дата", "Описание", "Приход", "Расход", "ID", "Номер", "Время", "Автор"],
    ]
    for r in payload.get("records", []):
        base_row = _xlsx_record_row(r.get("date") or fmt_date_backup(r.get("day_key")), r.get("amount"), r.get("note", ""))
        rows.append(base_row + [
            r.get("id", ""),
            r.get("short_id", ""),
            r.get("timestamp", ""),
            r.get("owner", ""),
        ])
    _write_simple_xlsx(xlsx_path, rows, sheet_name="Месяц")

    return {"json": json_path, "csv": csv_path, "xlsx": xlsx_path}


def mega_upload_chat_backup_bundle(chat_id: int, month_key: str | None = None) -> bool:
    """MEGA-бэкап одного чата: только JSON (latest + месячный JSON)."""
    if not mega_is_configured():
        return False
    if not is_backup_to_mega_enabled(chat_id):
        return False
    try:
        save_chat_json(chat_id)
        slug = mega_chat_slug(chat_id)
        remote_chat_dir = mega_remote_chat_dir(chat_id)
        ok = True

        # В MEGA больше не грузим CSV/XLSX — только JSON.
        ok = mega_put_replace(
            chat_json_file(chat_id),
            remote_chat_dir,
            f"latest_{slug}.json"
        ) and ok

        month_key = month_key or current_month_key()
        month_files = save_chat_monthly_backup_files(chat_id, month_key)
        remote_month_dir = mega_remote_month_dir(month_key)
        json_month_path = month_files.get("json")
        if json_month_path:
            ok = mega_put_replace(json_month_path, remote_month_dir, os.path.basename(json_month_path)) and ok

        if ok:
            log_info(f"[MEGA] JSON-only chat backup uploaded: {get_chat_display_name(chat_id)} / {month_key}")
        return ok
    except Exception as e:
        log_error(f"[MEGA CHAT BACKUP ERROR] {chat_id}: {e}")
        return False


def schedule_config_backup_for_chats(*chat_ids, delay: float = 3.0):
    """После изменения настроек/пересылки тоже обновляем JSON/канал/MEGA."""
    ids = set()
    for cid in chat_ids:
        try:
            if cid is not None:
                ids.add(int(cid))
        except Exception:
            pass
    if not ids:
        try:
            ids.update(collect_finance_chat_ids())
        except Exception:
            pass
    for cid in ids:
        try:
            schedule_backup_flush(cid, delay=delay)
        except Exception:
            pass


def make_global_backup_payload() -> dict:
    """Глобальный JSON для восстановления всего бота."""
    with data_lock:
        payload = json.loads(json.dumps(data or {}, ensure_ascii=False, default=str))
    payload.setdefault("chats", {})
    payload.setdefault("forward_rules", data.get("forward_rules", {}) if isinstance(data, dict) else {})
    payload.setdefault("forward_finance", data.get("forward_finance", {}) if isinstance(data, dict) else {})
    try:
        for _cid, _store in (payload.get("chats", {}) or {}).items():
            if isinstance(_store, dict):
                _store["records"] = backup_records_list(_store.get("records", []))
                _store["daily_records_by_date"] = {fmt_date_backup(k): backup_records_list(v) for k, v in (_store.get("daily_records", {}) or {}).items()}
    except Exception as e:
        log_error(f"make_global_backup_payload date annotate: {e}")
    payload["_backup_meta"] = {
        "kind": "mega_latest_global",
        "version": VERSION,
        "created_at": now_local().isoformat(timespec="seconds"),
        "chat_count": len(payload.get("chats", {}) or {}),
        "finance_active_chats": payload.get("finance_active_chats", {}),
        "forward_rules_count": sum(len(v or {}) for v in (payload.get("forward_rules", {}) or {}).values()),
        "forward_finance_count": sum(len(v or {}) for v in (payload.get("forward_finance", {}) or {}).values()),
        "note": "Полный JSON: чаты, финрежимы, скрытые режимы, быстрый остаток, пересылка, фин-учёт пересылки.",
    }
    return payload


def save_global_backup_snapshot(path: str) -> str:
    payload = make_global_backup_payload()
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return path


def mega_upload_latest_global_backup() -> bool:
    """Загружает latest_global.json в MEGA. Не ломает основной бот при ошибке."""
    if not mega_is_configured():
        return False
    try:
        os.makedirs(MEGA_LOCAL_TMP_DIR, exist_ok=True)
        local_path = os.path.join(MEGA_LOCAL_TMP_DIR, MEGA_LATEST_GLOBAL_NAME)
        save_global_backup_snapshot(local_path)
        mega_ensure_remote_dir()
        remote_file = mega_remote_file_path(MEGA_LATEST_GLOBAL_NAME)
        # Удаляем старый файл, чтобы в MEGA не плодились дубли latest_global.json.
        _mega_run("mega-rm", [remote_file], check=False, timeout=30)
        _mega_run("mega-put", [local_path, MEGA_BACKUP_DIR], check=True, timeout=MEGA_TIMEOUT)
        log_info(f"[MEGA] latest backup uploaded: {remote_file}")
        return True
    except Exception as e:
        log_error(f"[MEGA BACKUP ERROR] {e}")
        return False


def mega_download_latest_global_backup() -> str | None:
    if not mega_is_configured():
        return None
    try:
        mega_login_if_needed()
        restore_dir = tempfile.mkdtemp(prefix="mega_restore_")
        remote_file = mega_remote_file_path(MEGA_LATEST_GLOBAL_NAME)
        _mega_run("mega-get", [remote_file, restore_dir], check=True, timeout=MEGA_TIMEOUT)
        local_path = os.path.join(restore_dir, MEGA_LATEST_GLOBAL_NAME)
        if not os.path.exists(local_path):
            # На случай если MEGAcmd сохранил с другим именем — ищем первый JSON.
            for name in os.listdir(restore_dir):
                if name.lower().endswith(".json"):
                    local_path = os.path.join(restore_dir, name)
                    break
        if not os.path.exists(local_path):
            raise RuntimeError("download finished, but latest_global.json not found locally")
        log_info(f"[MEGA] latest backup downloaded: {local_path}")
        return local_path
    except Exception as e:
        log_error(f"[MEGA RESTORE DOWNLOAD ERROR] {e}")
        return None


def is_data_effectively_empty_for_restore(d: dict) -> bool:
    """True, если база похожа на пустую после нового deploy Render."""
    if not isinstance(d, dict):
        return True
    if d.get("forward_rules") or d.get("forward_finance"):
        return False
    chats = d.get("chats", {}) or {}
    if not chats:
        return True
    for _, store in chats.items():
        if not isinstance(store, dict):
            continue
        if store.get("records"):
            return False
        daily = store.get("daily_records") or {}
        if any((daily.get(day) or []) for day in daily):
            return False
    return True


def mega_autorestore_if_needed() -> bool:
    """При старте: если SQLite/data пустые, пробуем восстановиться из MEGA latest_global.json."""
    global data
    if not MEGA_AUTORESTORE or not mega_is_configured():
        return False
    if not is_data_effectively_empty_for_restore(data):
        return False

    local_path = mega_download_latest_global_backup()
    if not local_path:
        return False

    try:
        # restore_from_json уже умеет глобальный JSON с ключом chats.
        restore_chat_id = int(OWNER_ID) if OWNER_ID else 0
        restore_from_json(restore_chat_id, local_path)
        log_info("[MEGA] autorestore completed")
        return True
    except Exception as e:
        log_error(f"[MEGA AUTORESTORE ERROR] {e}")
        return False


def mega_status_text() -> str:
    lines = ["☁️ MEGA.nz / MEGAcmd"]
    lines.append(f"MEGA_ENABLED: {'ВКЛ' if MEGA_ENABLED else 'ВЫКЛ'}")
    lines.append(f"MEGA_AUTORESTORE: {'ВКЛ' if MEGA_AUTORESTORE else 'ВЫКЛ'}")
    lines.append(f"MEGA_EMAIL: {'есть' if MEGA_EMAIL else 'нет'}")
    lines.append(f"MEGA_BACKUP_DIR: {MEGA_BACKUP_DIR}")
    lines.append(f"MEGA_CHAT_BACKUP_DIR: {MEGA_CHAT_BACKUP_DIR}")
    lines.append(f"MEGA_MONTHLY_BACKUP_DIR: {MEGA_MONTHLY_BACKUP_DIR}")
    missing = mega_missing_commands()
    lines.append(f"MEGAcmd: {'OK' if not missing else 'нет команд: ' + ', '.join(missing)}")
    if mega_is_configured() and not missing:
        try:
            mega_login_if_needed()
            res = _mega_run("mega-whoami", [], check=False, timeout=30)
            txt = ((res.stdout or "") + (res.stderr or "")).strip()
            if txt:
                lines.append("whoami: " + txt[:300])
            else:
                lines.append("whoami: OK")
        except Exception as e:
            lines.append("whoami/login: ERROR — " + str(e)[:300])
    return "\n".join(lines)
def _load_csv_meta():
    # Сначала берём meta из data: она попадает в latest_global.json и переживает deploy/autorestore.
    try:
        meta_from_data = (data or {}).get("csv_meta")
        if isinstance(meta_from_data, dict) and meta_from_data:
            return meta_from_data
    except Exception:
        pass
    meta = SQLITE.get_meta("csv_meta", "main", None)
    if isinstance(meta, dict) and meta:
        try:
            data["csv_meta"] = meta
        except Exception:
            pass
        return meta
    legacy = _load_json(CSV_META_FILE, {})
    if isinstance(legacy, dict) and legacy:
        SQLITE.set_meta("csv_meta", "main", legacy)
        try:
            data["csv_meta"] = legacy
        except Exception:
            pass
    return legacy if isinstance(legacy, dict) else {}

def _save_csv_meta(meta: dict):
    try:
        meta = meta or {}
        SQLITE.set_meta("csv_meta", "main", meta)
        try:
            data["csv_meta"] = meta
            save_data(data)
        except Exception:
            pass
        _save_json(CSV_META_FILE, meta)
        log_info("csv_meta updated in sqlite/data")
    except Exception as e:
        log_error(f"_save_csv_meta: {e}")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CHAT_BACKUP_META_FILE = os.path.join(BASE_DIR, "chat_backup_meta.json")
log_info(f"chat_backup_meta.json PATH = {CHAT_BACKUP_META_FILE}")
def _load_chat_backup_meta() -> dict:
    """Загрузка meta-файла бэкапов для всех чатов."""
    try:
        meta_from_data = (data or {}).get("chat_backup_meta")
        if isinstance(meta_from_data, dict) and meta_from_data:
            return meta_from_data
        meta = SQLITE.get_meta("chat_backup_meta", "main", None)
        if isinstance(meta, dict) and meta:
            try:
                data["chat_backup_meta"] = meta
            except Exception:
                pass
            return meta
        if not os.path.exists(CHAT_BACKUP_META_FILE):
            return {}
        legacy = _load_json(CHAT_BACKUP_META_FILE, {})
        if isinstance(legacy, dict) and legacy:
            SQLITE.set_meta("chat_backup_meta", "main", legacy)
            try:
                data["chat_backup_meta"] = legacy
            except Exception:
                pass
        return legacy if isinstance(legacy, dict) else {}
    except Exception as e:
        log_error(f"_load_chat_backup_meta: {e}")
        return {}

def _save_chat_backup_meta(meta: dict) -> None:
    """Сохранение meta-файла, sqlite-копии и data-копии для MEGA autorestore."""
    try:
        meta = meta or {}
        SQLITE.set_meta("chat_backup_meta", "main", meta)
        try:
            data["chat_backup_meta"] = meta
            save_data(data)
        except Exception:
            pass
        log_info(f"SAVING META TO: {os.path.abspath(CHAT_BACKUP_META_FILE)}")
        _save_json(CHAT_BACKUP_META_FILE, meta)
        log_info("chat_backup_meta updated in sqlite/data")
    except Exception as e:
        log_error(f"_save_chat_backup_meta: {e}")
def send_backup_to_chat(chat_id: int) -> None:
    # JSON-бэкап прямо в чат больше не рассылаем пользователям/группам.
    # Разрешено только владельцу и, если эта функция будет вызвана напрямую, backup-каналу.
    if not can_receive_direct_json_backup(chat_id):
        return
    if is_finance_output_suppressed(chat_id) or not is_backup_to_chat_enabled(chat_id):
        return
    """
    Авто-бэкап JSON прямо в чат.
    Работает только для владельца и служебного backup-канала.
    Логика:
    • гарантируем актуальный data_<chat_id>.json
    • читаем meta-файл chat_backup_meta.json
    • если есть msg_id → edit_message_media()
    • если нет / не найдено → отправляем новое сообщение
    • обновляем meta-файл в рабочей директории (Render-friendly)
    • старое сообщение обновляется всегда; новое создаётся только если старое удалено/недоступно
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

        # Важно: не создаём новый документ каждый день/после deploy.
        # Если msg_id есть — всегда пытаемся обновить старое сообщение.
        # Новый документ создаётся только если старое сообщение удалено или Telegram не дал его отредактировать.
        msg_id = meta.get(msg_key)

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
                _tg_call_retry(
                    bot.edit_message_media,
                    chat_id=chat_id,
                    message_id=msg_id,
                    media=types.InputMediaDocument(
                        media=fobj,
                        caption=caption
                    ),
                    purpose="backup_edit_message_media"
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
        sent = _tg_call_retry(bot.send_document, chat_id, fobj, caption=caption, purpose="backup_send_document")
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
        "bot_errors": [],
        "csv_meta": {},
        "chat_backup_meta": {},
        "_global_settings": {"bot_journal_enabled": True, "buttons_current_window": False, "forward_menu_new_style": False, "icon_button_mode": True, "total_secret_mask_enabled": False},
    }

# InlineKeyboardButton wrapper for optional compact mode. It is intentionally
# exact/pattern based, so chat names remain untouched.
_ORIGINAL_INLINE_KEYBOARD_BUTTON = types.InlineKeyboardButton


def _compact_button_label(text) -> str:
    label = str(text or "")
    if not icon_button_mode_enabled():
        return label
    exact = {
        "⬅️ Назад осн. окно": "⬅️",
        "🔙 Назад": "↩️",
        "🔙 Назад в Инфо": "↩️ ℹ️",
        "⏪ Назад к статьям": "↩️ 📊",
        "❌ Закрыть": "✖️",
        "❌ Закрыть статьи": "✖️ 📊",
        "🗑 Удалить статью": "🗑",
        "🗑 Удалить выбранное": "🗑 ✅",
        "🗑 Удалить секреты": "🗑🔐",
        "🗑 День": "🗑 Д",
        "🗑 Неделя": "🗑 Н",
        "🗑 Месяц": "🗑 М",
        "🗑 Всё": "🗑 Всё",
        "➕ Добавить": "➕",
        "➕ Добавить статью": "➕",
        "✏️ Изменить": "✏️",
        "📅 Сегодня": "📅",
        "📅 Календарь": "📆",
        "📆 Выбор недели": "📆",
        "📚 Описание статей": "📚",
        "📓 Журнал": "📓",
        "📄 Скачать TXT": "📄",
        "📡 Проверить все": "📡",
        "⬅️ День": "⬅️",
        "День ➡️": "➡️",
        "⬅️ Месяц": "⬅️",
        "Месяц ➡️": "➡️",
        "⬅️ Чт–Ср": "⬅️ Чт",
        "Чт–Ср ➡️": "Чт ➡️",
        "⬅️ Пн–Вс": "⬅️ Пн",
        "Пн–Вс ➡️": "Пн ➡️",
        "⬜ Пн–Вс": "⬜ Пн",
        "🟦 Чт–Ср": "🟦 Чт",
        "👥 /owners": "👥 /own",
        "Нет доступных чатов": "Нет чатов",
        "Нет данных для изменения": "Нет данных",
        "Нет пользовательских статей": "Нет статей",
        "Удалённых нет": "Нет",
    }
    if label in exact:
        return exact[label]
    # Dynamic/service labels.
    close_match = re.fullmatch(r"❌ Закрыть (\d{2}:\d{2})", label)
    if close_match:
        return f"✖️ {close_match.group(1)}"
    if re.fullmatch(r"[✅❌] Фин режим (?:ВКЛ|ВЫКЛ)", label):
        return ("✅" if label.startswith("✅") else "❌") + " Фин"
    if re.fullmatch(r"[✅❌] Журнал (?:ВКЛ|ВЫКЛ)", label):
        return ("✅" if label.startswith("✅") else "❌") + " 📓"
    if label.startswith("✅ В текущем окне"):
        return "✅ 🪟"
    if label.startswith("❌ В текущем окне"):
        return "❌ 🪟"
    if label.startswith("🧩 Пересылка:") or label.startswith("🔁 Пересылка:"):
        return "🔁/🧩"
    if label.startswith("🔣 Кнопки:") or label.startswith("🔤 Кнопки:"):
        return "🔣/🔤"
    if label.startswith("🪷 Маска:"):
        return "🪷" + ("✅" if "ВКЛ" in label else "❌")
    if re.fullmatch(r"[✅❌] Секрет", label):
        return ("✅" if label.startswith("✅") else "❌") + "🔐"
    if label.startswith("🏦 Остаток:"):
        return label.replace("🏦 Остаток:", "🏦", 1)
    if label.startswith("✏️ ") and len(label) > 18:
        return "✏️ " + label[3:24].strip()
    if label.startswith("☑️ ") or label.startswith("⬛ "):
        return label[:2] + " " + label[3:24].strip()
    return label


def IB(text, *args, **kwargs):
    return _ORIGINAL_INLINE_KEYBOARD_BUTTON(_compact_button_label(text), *args, **kwargs)

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
    """Потокобезопасное сохранение общего состояния."""
    with data_lock:
        fac = {}
        for cid in list(finance_active_chats):
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
def chat_xlsx_file(chat_id: int) -> str:
    return f"data_{chat_id}.xlsx"
def chat_meta_file(chat_id: int) -> str:
    return f"csv_meta_{chat_id}.json"
    
def get_chat_store(chat_id: int) -> dict:
    """
    Хранилище данных одного чата.
    Добавлено поле "known_chats" для отображения названий/username в меню пересылки.
    """
    with data_lock:

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
                    "quick_balance_enabled": False,
                    "quick_balance_behavior": "normal",
                    "quick_balance_user_selected": False,
                    "hidden_finance": False,
                    "auto_backup_enabled": True,
                    "auto_backup_to_chat_enabled": True,
                    "auto_backup_to_channel_enabled": True,
                    "auto_backup_to_mega_enabled": True
                },
            }
        )

        store.setdefault("settings", {}).setdefault("auto_add", True)
        store.setdefault("settings", {}).setdefault("quick_balance_enabled", False)
        store.setdefault("settings", {}).setdefault("quick_balance_behavior", "normal")
        store.setdefault("settings", {}).setdefault("quick_balance_user_selected", False)
        store.setdefault("settings", {}).setdefault("hidden_finance", False)
        store.setdefault("settings", {}).setdefault("auto_backup_enabled", True)
        legacy_backup_enabled = bool(store.setdefault("settings", {}).get("auto_backup_enabled", True))
        store.setdefault("settings", {}).setdefault("auto_backup_to_chat_enabled", legacy_backup_enabled)
        store.setdefault("settings", {}).setdefault("auto_backup_to_channel_enabled", legacy_backup_enabled)
        store.setdefault("settings", {}).setdefault("auto_backup_to_mega_enabled", legacy_backup_enabled)
        store.setdefault("finance_mode", False)

        if is_owner_chat(chat_id):
            store["settings"]["auto_add"] = True

        if "known_chats" not in store:
            store["known_chats"] = {}

        return store

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


def _xlsx_col_name(n: int) -> str:
    """1 -> A, 27 -> AA."""
    out = ""
    n = int(n)
    while n > 0:
        n, rem = divmod(n - 1, 26)
        out = chr(65 + rem) + out
    return out or "A"


def _xlsx_xml_escape(value) -> str:
    text = "" if value is None else str(value)
    return (
        text.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
            .replace("'", "&apos;")
    )


def _xlsx_cell_xml(row_idx: int, col_idx: int, value, style: int | None = None) -> str:
    ref = f"{_xlsx_col_name(col_idx)}{row_idx}"
    s_attr = f' s="{int(style)}"' if style is not None else ""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return f'<c r="{ref}"{s_attr}><v>{value}</v></c>'
    return f'<c r="{ref}" t="inlineStr"{s_attr}><is><t>{_xlsx_xml_escape(value)}</t></is></c>'


def _write_simple_xlsx(path: str, rows: list[list], sheet_name: str = "Данные") -> None:
    """Минимальный XLSX без внешних библиотек: дата / сумма / заметка."""
    rows = rows or [["date", "amount", "note"]]
    sheet_rows = []
    for r_idx, row in enumerate(rows, start=1):
        cells = []
        for c_idx, value in enumerate(row, start=1):
            cells.append(_xlsx_cell_xml(r_idx, c_idx, value, style=1 if r_idx == 1 else None))
        sheet_rows.append(f'<row r="{r_idx}">' + "".join(cells) + '</row>')

    sheet_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
<sheetViews><sheetView workbookViewId="0"><pane ySplit="1" topLeftCell="A2" activePane="bottomLeft" state="frozen"/></sheetView></sheetViews>
<cols><col min="1" max="1" width="13" customWidth="1"/><col min="2" max="2" width="42" customWidth="1"/><col min="3" max="3" width="14" customWidth="1"/><col min="4" max="4" width="14" customWidth="1"/><col min="5" max="10" width="18" customWidth="1"/></cols>
<sheetData>""" + "".join(sheet_rows) + """</sheetData>
</worksheet>"""

    workbook_xml = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
<sheets><sheet name="{_xlsx_xml_escape(sheet_name)[:31]}" sheetId="1" r:id="rId1"/></sheets>
</workbook>"""

    rels_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
</Relationships>"""

    workbook_rels_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
<Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>
</Relationships>"""

    content_types_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
<Default Extension="xml" ContentType="application/xml"/>
<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
<Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
<Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>
</Types>"""

    styles_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
<fonts count="2"><font><sz val="11"/><name val="Calibri"/></font><font><b/><sz val="11"/><name val="Calibri"/></font></fonts>
<fills count="1"><fill><patternFill patternType="none"/></fill></fills>
<borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders>
<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>
<cellXfs count="2"><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/><xf numFmtId="0" fontId="1" fillId="0" borderId="0" xfId="0" applyFont="1"/></cellXfs>
<cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles>
</styleSheet>"""

    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("[Content_Types].xml", content_types_xml)
        z.writestr("_rels/.rels", rels_xml)
        z.writestr("xl/workbook.xml", workbook_xml)
        z.writestr("xl/_rels/workbook.xml.rels", workbook_rels_xml)
        z.writestr("xl/worksheets/sheet1.xml", sheet_xml)
        z.writestr("xl/styles.xml", styles_xml)


def _xlsx_income_expense_values(amount):
    """Возвращает (приход, расход) для Excel: сумма разбита по двум колонкам."""
    try:
        v = float(amount or 0)
    except Exception:
        v = 0.0
    if v >= 0:
        income = int(v) if float(v).is_integer() else v
        return income, ""
    expense = abs(v)
    expense = int(expense) if float(expense).is_integer() else expense
    return "", expense


def _xlsx_record_row(date_value, amount, note):
    income, expense = _xlsx_income_expense_values(amount)
    return [date_value, note or "", income, expense]



TABL_LSX_CATEGORIES = [
    "Продукты",
    "Хоз общ",
    "Авто и (бус)",
    "прочие",
    "орг. техника",
    "Еда доп и ШБ",
    "Связь",
    "переводы",
    "Проживание",
    "Хоз за ашр",
    "аптечка",
]


def _tabl_lsx_category(note: str) -> str:
    text = str(note or "").casefold()
    checks = [
        ("Еда доп и ШБ", ("шб", "шамп", "мыло", "зуб", "паста", "гигиен")),
        ("Продукты", ("продукт", "еда", "хлеб", "мол", "фрукт", "овощ", "банан", "лук", "масло", "йогурт", "кофе", "чай", "курица", "мясо")),
        ("Хоз общ", ("хоз", "салф", "порош", "клей", "краск", "саморез", "инструмент", "батарей", "розет", "шнур", "пульт", "ключ")),
        ("Авто и (бус)", ("авто", "бенз", "соляр", "заправ", "машин", "шина", "масло авто", "пикап", "бус")),
        ("орг. техника", ("орг", "двд", "dvd", "переходник", "блок питание", "провод", "кабель", "монитор", "паяль", "заряд", "науш", "мыш", "принтер")),
        ("Связь", ("тел", "связ", "пополнение", "сим", "интернет")),
        ("переводы", ("перевод", "вестерн", "western", "банковский", "mercado", "меркадо")),
        ("Проживание", ("прож", "аренд", "квар", "отель", "дом")),
        ("Хоз за ашр", ("ашр", "ашрам")),
        ("аптечка", ("аптеч", "аптек", "лекар", "ибуп", "витамин", "стоматолог")),
    ]
    for name, words in checks:
        if any(w in text for w in words):
            return name
    return "прочие"


def _tabl_lsx_weeks(reference_day: str | None = None, count: int = 4) -> list[tuple[str, str]]:
    ref = reference_day or today_key()
    start_key = week_start_thursday(ref)
    start = datetime.strptime(start_key, "%Y-%m-%d").date()
    weeks = []
    first = start - timedelta(days=7 * (int(count) - 1))
    for i in range(int(count)):
        s = first + timedelta(days=7 * i)
        e = s + timedelta(days=6)
        weeks.append((s.strftime("%Y-%m-%d"), e.strftime("%Y-%m-%d")))
    return weeks


def _tabl_lsx_opening_balance(store: dict, start_key: str) -> float:
    total = 0.0
    for r in (store.get("records", []) or []):
        try:
            if _record_day_key(r) < start_key:
                total += float(r.get("amount", 0) or 0)
        except Exception:
            pass
    return total


def _xlsx_cell_xml2(row_idx: int, col_idx: int, value, style: int = 0) -> str:
    if value is None:
        value = ""
    ref = f"{_xlsx_col_name(col_idx)}{row_idx}"
    s_attr = f' s="{int(style)}"' if int(style or 0) else ""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return f'<c r="{ref}"{s_attr}><v>{float(value):.2f}</v></c>'
    text = str(value)
    return f'<c r="{ref}" t="inlineStr"{s_attr}><is><t>{_xlsx_xml_escape(text)}</t></is></c>'


def _write_tabl_lsx_xlsx(path: str, rows: list[list], styles: list[list], sheet_name: str = "4 недели") -> None:
    max_cols = max((len(r) for r in rows), default=1)
    widths = [13, 16, 28] + [20] * max(0, max_cols - 3)
    sheet_rows = []
    for r_idx, row in enumerate(rows, start=1):
        cells = []
        st_row = styles[r_idx - 1] if r_idx - 1 < len(styles) else []
        for c_idx in range(1, max_cols + 1):
            value = row[c_idx - 1] if c_idx - 1 < len(row) else ""
            style = st_row[c_idx - 1] if c_idx - 1 < len(st_row) else 0
            cells.append(_xlsx_cell_xml2(r_idx, c_idx, value, style=style))
        height = ' ht="22" customHeight="1"' if r_idx <= 2 else ""
        sheet_rows.append(f'<row r="{r_idx}"{height}>' + "".join(cells) + '</row>')
    cols_xml = "".join(
        f'<col min="{i}" max="{i}" width="{min(widths[i-1] if i-1 < len(widths) else 18, 34)}" customWidth="1"/>'
        for i in range(1, max_cols + 1)
    )
    sheet_xml = f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
<sheetViews><sheetView workbookViewId="0"><pane ySplit="3" topLeftCell="A4" activePane="bottomLeft" state="frozen"/></sheetView></sheetViews>
<cols>{cols_xml}</cols>
<sheetData>{''.join(sheet_rows)}</sheetData>
</worksheet>'''
    workbook_xml = f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
<sheets><sheet name="{_xlsx_xml_escape(sheet_name)[:31]}" sheetId="1" r:id="rId1"/></sheets>
</workbook>'''
    rels_xml = '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
</Relationships>'''
    workbook_rels_xml = '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
<Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>
</Relationships>'''
    content_types_xml = '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
<Default Extension="xml" ContentType="application/xml"/>
<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
<Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
<Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>
</Types>'''
    styles_xml = '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
<fonts count="3"><font><sz val="10"/><name val="Calibri"/></font><font><b/><sz val="10"/><name val="Calibri"/></font><font><b/><sz val="14"/><name val="Calibri"/></font></fonts>
<fills count="7"><fill><patternFill patternType="none"/></fill><fill><patternFill patternType="gray125"/></fill><fill><patternFill patternType="solid"><fgColor rgb="FF00E000"/></patternFill></fill><fill><patternFill patternType="solid"><fgColor rgb="FFFFC000"/></patternFill></fill><fill><patternFill patternType="solid"><fgColor rgb="FFFF9999"/></patternFill></fill><fill><patternFill patternType="solid"><fgColor rgb="FFE2F0D9"/></patternFill></fill><fill><patternFill patternType="solid"><fgColor rgb="FFD9EAD3"/></patternFill></fill></fills>
<borders count="2"><border><left/><right/><top/><bottom/><diagonal/></border><border><left style="thin"/><right style="thin"/><top style="thin"/><bottom style="thin"/><diagonal/></border></borders>
<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>
<cellXfs count="8"><xf numFmtId="0" fontId="0" fillId="0" borderId="1" xfId="0"/><xf numFmtId="0" fontId="2" fillId="0" borderId="0" xfId="0"/><xf numFmtId="0" fontId="1" fillId="2" borderId="1" xfId="0" applyFill="1" applyFont="1"/><xf numFmtId="0" fontId="1" fillId="3" borderId="1" xfId="0" applyFill="1" applyFont="1"/><xf numFmtId="0" fontId="0" fillId="0" borderId="1" xfId="0"/><xf numFmtId="0" fontId="1" fillId="4" borderId="1" xfId="0" applyFill="1" applyFont="1"/><xf numFmtId="0" fontId="1" fillId="5" borderId="1" xfId="0" applyFill="1" applyFont="1"/><xf numFmtId="0" fontId="1" fillId="6" borderId="1" xfId="0" applyFill="1" applyFont="1"/></cellXfs>
<cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles>
</styleSheet>'''
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("[Content_Types].xml", content_types_xml)
        z.writestr("_rels/.rels", rels_xml)
        z.writestr("xl/workbook.xml", workbook_xml)
        z.writestr("xl/_rels/workbook.xml.rels", workbook_rels_xml)
        z.writestr("xl/worksheets/sheet1.xml", sheet_xml)
        z.writestr("xl/styles.xml", styles_xml)


def create_tabl_lsx_file(chat_id: int, reference_day: str | None = None) -> str:
    chat_id = int(chat_id)
    store = get_chat_store(chat_id)
    weeks = _tabl_lsx_weeks(reference_day or today_key(), 4)
    cols = ["Дата", "Приход/выдача", "Откуда/кому"] + TABL_LSX_CATEGORIES
    rows, styles = [], []
    title = f"сегодня {fmt_date_ddmmyy(today_key())} — {get_chat_display_name(chat_id)}"
    rows.append([title]); styles.append([1] + [0] * (len(cols) - 1))
    rows.append(["Таблица за последние 4 недели: четверг–среда"]); styles.append([1] + [0] * (len(cols) - 1))
    rows.append([]); styles.append([])
    daily = store.get("daily_records", {}) or {}
    for start_key, end_key in weeks:
        rows.append(["Неделя", f"{fmt_date_ddmmyy(start_key)} — {fmt_date_ddmmyy(end_key)}"])
        styles.append([3, 3] + [3] * (len(cols) - 2))
        rows.append(cols); styles.append([2] * len(cols))
        opening = _tabl_lsx_opening_balance(store, start_key)
        rows.append([fmt_date_ddmmyy(start_key), int(round(opening)), "остаток с прошлой недели"] + [""] * len(TABL_LSX_CATEGORIES))
        styles.append([7, 7, 7] + [4] * len(TABL_LSX_CATEGORIES))
        income_total = 0.0
        expense_total = 0.0
        cat_totals = {cat: 0.0 for cat in TABL_LSX_CATEGORIES}
        start_dt = datetime.strptime(start_key, "%Y-%m-%d").date()
        for offset in range(7):
            dk = (start_dt + timedelta(days=offset)).strftime("%Y-%m-%d")
            recs = sorted(daily.get(dk, []) or [], key=record_sort_key)
            if not recs:
                rows.append([fmt_date_ddmmyy(dk)] + [""] * (len(cols) - 1))
                styles.append([3] + [4] * (len(cols) - 1))
                continue
            first_for_day = True
            for rec in recs:
                try:
                    amount = float(rec.get("amount", 0) or 0)
                except Exception:
                    amount = 0.0
                note = str(rec.get("note") or "").strip()
                row = [fmt_date_ddmmyy(dk) if first_for_day else "", "", ""] + [""] * len(TABL_LSX_CATEGORIES)
                first_for_day = False
                if amount >= 0:
                    income_total += amount
                    row[1] = int(round(amount))
                    row[2] = note
                else:
                    value = abs(amount)
                    expense_total += value
                    cat = _tabl_lsx_category(note)
                    cat_totals[cat] = cat_totals.get(cat, 0.0) + value
                    col_idx = 3 + TABL_LSX_CATEGORIES.index(cat)
                    shown = fmt_num_plain(value)
                    row[col_idx] = (shown + (" " + note if note else "")).strip()
                rows.append(row)
                styles.append([3 if row[0] else 4, 4, 4] + [4] * len(TABL_LSX_CATEGORIES))
        total_row = ["Итог:", int(round(income_total)), ""] + [int(round(cat_totals.get(cat, 0))) if cat_totals.get(cat, 0) else "" for cat in TABL_LSX_CATEGORIES]
        rows.append(total_row); styles.append([5] * len(cols))
        rows.append(["расход:", int(round(expense_total))] + [""] * (len(cols) - 2)); styles.append([5] * len(cols))
        rows.append(["на руках:", int(round(opening + income_total - expense_total))] + [""] * (len(cols) - 2)); styles.append([6] * len(cols))
        rows.append([]); styles.append([])
    os.makedirs(MEGA_LOCAL_TMP_DIR, exist_ok=True)
    start_all, end_all = weeks[0][0], weeks[-1][1]
    fname = f"tabl_lsx_{mega_safe_name(get_chat_display_name(chat_id), 'chat')}_{start_all}_{end_all}.xlsx"
    path = os.path.join(MEGA_LOCAL_TMP_DIR, fname)
    _write_tabl_lsx_xlsx(path, rows, styles, sheet_name="4 недели")
    return path


def send_tabl_lsx_for_chat(recipient_chat_id: int, target_chat_id: int):
    trace = ProcessTrace(recipient_chat_id, f"Таблица LSX: {get_chat_display_name(target_chat_id)}").start()
    path = None
    try:
        trace.step("собирает последние 4 недели Чт–Ср")
        path = create_tabl_lsx_file(target_chat_id, today_key())
        trace.step("отправляет Excel")
        display = os.path.basename(path)
        fobj = file_bytesio_named(path, display)
        if fobj:
            _tg_call_retry(
                bot.send_document,
                recipient_chat_id,
                fobj,
                caption=f"📊 Таблица LSX за последние 4 недели Чт–Ср: {get_chat_display_name(target_chat_id)}",
                purpose="tabl_lsx_send_document",
            )
        trace.finish("таблица готова")
    except Exception as e:
        log_error(f"send_tabl_lsx_for_chat({target_chat_id}): {e}")
        send_and_auto_delete(recipient_chat_id, "❌ Не удалось создать /tabl_lsx.", 15)
        try:
            trace.fail(e)
        except Exception:
            pass
    finally:
        if path:
            try:
                os.remove(path)
            except Exception:
                pass

def save_chat_xlsx(chat_id: int, path: str | None = None, store: dict | None = None) -> str | None:
    """Создаёт Excel .xlsx для чата; date в формате DD:MM:YY."""
    try:
        store = store or data.get("chats", {}).get(str(chat_id)) or get_chat_store(chat_id)
        path = path or chat_xlsx_file(chat_id)
        rows = [["Дата", "Описание", "Приход", "Расход"]]
        daily = store.get("daily_records", {}) or {}
        for dk in sorted(daily.keys()):
            recs_sorted = sorted(daily.get(dk, []) or [], key=record_sort_key)
            for r in recs_sorted:
                rows.append(_xlsx_record_row(fmt_date_backup(dk), r.get("amount", 0), r.get("note", "")))
        _write_simple_xlsx(path, rows, sheet_name="Данные")
        return path
    except Exception as e:
        log_error(f"save_chat_xlsx({get_chat_display_name(chat_id)}): {e}")
        return None

def save_chat_json(chat_id: int):
    """Save per-chat JSON, CSV, XLSX and META for one chat."""
    try:
        store = data.get("chats", {}).get(str(chat_id))
        if not store:
            store = get_chat_store(chat_id)
        normalize_chat_records(chat_id)
        chat_path_json = chat_json_file(chat_id)
        chat_path_csv = chat_csv_file(chat_id)
        chat_path_xlsx = chat_xlsx_file(chat_id)
        chat_path_meta = chat_meta_file(chat_id)
        for p in (chat_path_json, chat_path_csv, chat_path_xlsx, chat_path_meta):
            if not os.path.exists(p):
                with open(p, "a", encoding="utf-8"):
                    pass
        payload = {
            "kind": "chat_full_backup",
            "version": VERSION,
            "created_at": now_local().isoformat(timespec="seconds"),
            "date_format": "DD:MM:YY",
            "chat_id": chat_id,
            "chat_name": get_chat_display_name(chat_id),
            "balance": store.get("balance", 0),
            "records": backup_records_list(store.get("records", [])),
            "daily_records": backup_daily_records(store.get("daily_records", {})),
            "daily_records_by_date": {fmt_date_backup(k): backup_records_list(v) for k, v in (store.get("daily_records", {}) or {}).items()},
            "next_id": store.get("next_id", 1),
            "info": store.get("info", {}),
            "known_chats": store.get("known_chats", {}),
            "settings_backup": build_chat_settings_backup_payload(chat_id, store),
        }
        _save_json(chat_path_json, payload)
        with open(chat_path_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["date", "amount", "note"])
            daily = store.get("daily_records", {}) or {}
            rows = []
            for dk in sorted(daily.keys()):
                recs_sorted = sorted(daily.get(dk, []) or [], key=record_sort_key)
                for r in recs_sorted:
                    rows.append((fmt_date_backup(dk), fmt_csv_amount(r.get("amount")), r.get("note", "")))
            write_csv_rows_with_day_gaps(w, rows, 3)
        save_chat_xlsx(chat_id, chat_path_xlsx, store)
        meta = {
            "last_saved": now_local().isoformat(timespec="seconds"),
            "date_format": "DD:MM:YY",
            "record_count": sum(len(v) for v in store.get("daily_records", {}).values()),
        }
        _save_json(chat_path_meta, meta)
        log_info(f"Per-chat files saved for chat {get_chat_display_name(chat_id)}")
    except Exception as e:
        log_error(f"save_chat_json({get_chat_display_name(chat_id)}): {e}")
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
        schedule_all_finance_backups(delay=0.5)
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
        finance_changed(chat_id, get_chat_store(chat_id).get("current_view_day", today_key()), reason="restore_json_core", delay=0.1)

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
    finance_changed(chat_id, get_chat_store(chat_id).get("current_view_day", today_key()), reason="restore_csv_core", delay=0.1)

    log_info(f"restore_from_csv: chat {chat_id} restored from CSV")

def fmt_num(x):
    """
    Европейский формат вывода с обязательным знаком.
    В фин-окнах округляем до целого, чтобы не появлялись хвосты float вроде
    +2.683.012,399999999907.
    Примеры:
        +1234.56 → +1.235
        -800     → -800
        0        → +0
    """
    try:
        x = float(x or 0)
    except Exception:
        try:
            x = float(str(x).replace(" ", "").replace(".", "").replace(",", "."))
        except Exception:
            x = 0.0
    sign = "+" if x >= 0 else "-"
    whole = int(round(abs(x)))
    s = f"{whole:,}".replace(",", ".")
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

    # Приход без знака "+": слова "обмен" и "возврат" считаем приходом.
    # Примеры: "1000 возврат", "500 обмен", "возврат 300".
    income_words = ("обмен", "возврат")
    if amount < 0 and any(w in note for w in income_words):
        amount = abs(amount)

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


def _custom_category_list(store: dict | None) -> list:
    if not isinstance(store, dict):
        return []
    settings = store.setdefault("settings", {})
    raw = settings.setdefault("expense_categories_custom", [])
    if isinstance(raw, dict):
        raw = list(raw.values())
        settings["expense_categories_custom"] = raw
    if not isinstance(raw, list):
        settings["expense_categories_custom"] = []
        return []
    out = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip().upper()
        keywords = [str(x).strip().lower() for x in (item.get("keywords") or []) if str(x).strip()]
        slug = str(item.get("slug") or "").strip()
        if not name or not keywords:
            continue
        if not slug:
            slug = make_custom_category_slug(name, raw)
            item["slug"] = slug
        out.append({"name": name, "slug": slug, "keywords": keywords})
    return out


def make_custom_category_slug(name: str, existing=None) -> str:
    base = re.sub(r"[^0-9a-zA-Zа-яА-Я]+", "_", str(name or "").lower()).strip("_")[:32] or "cat"
    slug = "custom_" + base
    used = set(EXPENSE_CATEGORY_SLUGS.values())
    for item in existing or []:
        if isinstance(item, dict) and item.get("slug"):
            used.add(str(item.get("slug")))
    if slug not in used:
        return slug
    i = 2
    while f"{slug}_{i}" in used:
        i += 1
    return f"{slug}_{i}"


def get_expense_category_order(store: dict | None = None) -> list[str]:
    names = list(EXPENSE_CATEGORY_ORDER)
    for item in _custom_category_list(store):
        if item["name"] not in names:
            names.append(item["name"])
    return names


def get_expense_category_slug(category: str, store: dict | None = None) -> str | None:
    category = str(category or "").strip().upper()
    if category in EXPENSE_CATEGORY_SLUGS:
        return EXPENSE_CATEGORY_SLUGS.get(category)
    for item in _custom_category_list(store):
        if item["name"] == category:
            return item["slug"]
    return None


def get_category_by_slug(slug: str, store: dict | None = None) -> str | None:
    slug = str(slug or "").strip()
    if slug in CATEGORY_BY_SLUG:
        return CATEGORY_BY_SLUG.get(slug)
    for item in _custom_category_list(store):
        if item["slug"] == slug:
            return item["name"]
    return None


def parse_category_definition(text: str):
    raw = str(text or "").strip()
    if not raw:
        raise ValueError("empty")
    if raw.lower() in {"отмена", "cancel", "/cancel"}:
        return None, None
    sep = ":" if ":" in raw else "|" if "|" in raw else "-" if " - " in raw else None
    if not sep:
        raise ValueError("format")
    name, keys = raw.split(sep, 1)
    name = re.sub(r"\s+", " ", name.strip()).upper()
    keywords = [re.sub(r"\s+", " ", x.strip().lower()) for x in re.split(r"[,;]", keys) if x.strip()]
    if not name or not keywords:
        raise ValueError("format")
    return name, keywords


def add_custom_expense_category(chat_id: int, name: str, keywords: list[str]) -> dict:
    store = get_chat_store(chat_id)
    settings = store.setdefault("settings", {})
    custom = settings.setdefault("expense_categories_custom", [])
    if not isinstance(custom, list):
        custom = []
        settings["expense_categories_custom"] = custom
    name = str(name or "").strip().upper()
    keywords = [str(x).strip().lower() for x in (keywords or []) if str(x).strip()]
    for item in custom:
        if isinstance(item, dict) and str(item.get("name", "")).strip().upper() == name:
            item["keywords"] = sorted(set((item.get("keywords") or []) + keywords))
            save_data(data)
            schedule_config_backup_for_chats(chat_id)
            bot_journal("category_updated", chat_id, f"{name}: {', '.join(item['keywords'])}")
            return item
    item = {"name": name, "slug": make_custom_category_slug(name, custom), "keywords": sorted(set(keywords))}
    custom.append(item)
    save_data(data)
    schedule_config_backup_for_chats(chat_id)
    bot_journal("category_added", chat_id, f"{name}: {', '.join(item['keywords'])}")
    return item


def expense_keyword_matches(note: str, keyword: str) -> bool:
    """Match a configured word/phrase without matching it inside another word."""
    note = re.sub(r"\s+", " ", str(note or "").casefold()).strip()
    keyword = re.sub(r"\s+", " ", str(keyword or "").casefold()).strip()
    if not note or not keyword:
        return False
    pattern = r"(?<![\w])" + re.escape(keyword).replace(r"\ ", r"\s+") + r"(?![\w])"
    return bool(re.search(pattern, note, flags=re.UNICODE))


def resolve_expense_category(note: str, store: dict | None = None):
    if not note:
        return None
    # Сначала пользовательские статьи: они важнее стандартных, если ключ совпал.
    for item in _custom_category_list(store):
        for kw in item.get("keywords", []):
            if expense_keyword_matches(note, kw):
                return item.get("name")
    for cat in EXPENSE_CATEGORY_ORDER:
        keywords = EXPENSE_CATEGORIES.get(cat, [])
        for kw in keywords:
            if expense_keyword_matches(note, kw):
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
            cat = resolve_expense_category(r.get("note", ""), store)
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
            if resolve_expense_category(note, store) == category:
                items.append((day, -amt, note))
    return items


def get_ordered_category_names(include_all: bool = False, cats: dict | None = None, store: dict | None = None):
    names = []
    seen = set()
    order = get_expense_category_order(store)
    if include_all:
        for cat in order:
            if cat not in seen:
                names.append(cat)
                seen.add(cat)
    elif cats:
        for cat in order:
            if cat in cats and cat not in seen:
                names.append(cat)
                seen.add(cat)
        for cat in sorted(cats.keys()):
            if cat not in seen:
                names.append(cat)
                seen.add(cat)
    return names


def build_articles_description_text(chat_id: int | None = None) -> str:
    """Описание статей: статья = ключевые слова. Для владельца показывает стандартные + пользовательские по выбранному чату."""
    try:
        store = get_chat_store(chat_id) if chat_id is not None else None
    except Exception:
        store = None
    lines = ["📚 Описание статей расходов", ""]
    for cat in EXPENSE_CATEGORY_ORDER:
        keys = EXPENSE_CATEGORIES.get(cat, []) or []
        lines.append(f"{cat}: {', '.join(keys) if keys else '—'}")
    custom = _custom_category_list(store)
    if custom:
        lines.append("")
        lines.append("Пользовательские статьи:")
        for item in custom:
            lines.append(f"{item.get('name')}: {', '.join(item.get('keywords') or [])}")
    lines.append("")
    lines.append("Добавить новую статью можно в окне 📊 Статьи → ➕ Добавить статью.")
    return wm_common("\n".join(lines), 7)


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
        for cat in get_ordered_category_names(cats=cats, store=store):
            lines.append(f"{cat}: {fmt_num_plain(cats.get(cat, 0))}")
    return wm_common("\n".join(lines), 7), cats



# ─────────────────────────────────────────────────────────────
# Короткие callback-и для меню статей
# Telegram ограничивает callback_data 64 байтами. В статьях есть даты,
# chat_id и пользовательские slug-и, поэтому длинные callback-и могут
# приводить к BUTTON_DATA_INVALID и меню «не открывается».
# Здесь длинная команда кладётся во временную карту, а в кнопку идёт короткий токен.
# ─────────────────────────────────────────────────────────────
_short_callback_lock = threading.RLock()
_short_callback_store = {}
_short_callback_counter = 0
SHORT_CALLBACK_TTL_SECONDS = 6 * 60 * 60


def base36(num: int) -> str:
    try:
        num = int(num)
    except Exception:
        num = 0
    alphabet = "0123456789abcdefghijklmnopqrstuvwxyz"
    if num == 0:
        return "0"
    neg = num < 0
    num = abs(num)
    out = ""
    while num:
        num, rem = divmod(num, 36)
        out = alphabet[rem] + out
    return ("-" if neg else "") + out


def make_short_callback(data_str: str, prefix: str | None = None) -> str:
    global _short_callback_counter
    data_str = str(data_str or "")
    try:
        if len(data_str.encode("utf-8")) <= 54:
            return data_str
    except Exception:
        pass
    if not prefix:
        if data_str.startswith("fvcat_"):
            prefix = "fvcatx"
        elif data_str.startswith("cat_"):
            prefix = "catx"
        else:
            prefix = "cbx"
    with _short_callback_lock:
        _short_callback_counter += 1
        token = base36(_short_callback_counter) + base36(int(time.time() * 1000) % 46656)
        _short_callback_store[token] = {
            "data": data_str,
            "ts": time.time(),
        }
        # Лёгкая чистка старых токенов, чтобы память не росла бесконечно.
        if len(_short_callback_store) > 2000:
            cutoff = time.time() - SHORT_CALLBACK_TTL_SECONDS
            for k in list(_short_callback_store.keys())[:500]:
                if _short_callback_store.get(k, {}).get("ts", 0) < cutoff:
                    _short_callback_store.pop(k, None)
    return f"{prefix}:{token}"


def resolve_short_callback(data_str: str) -> str | None:
    data_str = str(data_str or "")
    if not (data_str.startswith("catx:") or data_str.startswith("fvcatx:") or data_str.startswith("cbx:")):
        return data_str
    token = data_str.split(":", 1)[1]
    with _short_callback_lock:
        item = _short_callback_store.get(token)
    if not item:
        return None
    return str(item.get("data") or "")


def cat_callback(data_str: str) -> str:
    return make_short_callback(data_str, "catx")


def fvcat_callback(data_str: str) -> str:
    return make_short_callback(data_str, "fvcatx")

def build_categories_buttons(start: str, end: str, store: dict | None = None):
    kb = types.InlineKeyboardMarkup(row_width=3)
    buttons = []
    for cat in get_ordered_category_names(include_all=True, store=store):
        slug = get_expense_category_slug(cat, store)
        if not slug:
            continue
        buttons.append(
            IB(
                cat,
                callback_data=cat_callback(f"cat_show:{start}:{end}:{slug}")
            )
        )

    for i in range(0, len(buttons), 3):
        kb.row(*buttons[i:i + 3])

    return kb


def build_categories_summary_keyboard(mode: str, start: str, end: str, store: dict | None = None):
    kb = build_categories_buttons(start, end, store=store)

    if mode == "wthu":
        prev_key = (datetime.strptime(start, "%Y-%m-%d") - timedelta(days=7)).strftime("%Y-%m-%d")
        next_key = (datetime.strptime(start, "%Y-%m-%d") + timedelta(days=7)).strftime("%Y-%m-%d")
        row = [IB("⬅️ Чт–Ср", callback_data=cat_callback(f"cat_wthu:{prev_key}"))]
        if start != week_start_thursday(today_key()):
            row.append(IB("📅 Сегодня", callback_data=cat_callback("cat_today")))
        row.append(IB("Чт–Ср ➡️", callback_data=cat_callback(f"cat_wthu:{next_key}")))
        kb.row(*row)
        kb.row(
            IB(
                "⬜ Пн–Вс",
                callback_data=cat_callback(f"cat_wk:{week_start_monday(start)}")
            ),
            IB("📆 Выбор недели", callback_data=cat_callback("cat_months"))
        )
    elif mode == "wk":
        prev_key = (datetime.strptime(start, "%Y-%m-%d") - timedelta(days=7)).strftime("%Y-%m-%d")
        next_key = (datetime.strptime(start, "%Y-%m-%d") + timedelta(days=7)).strftime("%Y-%m-%d")
        row = [IB("⬅️ Пн–Вс", callback_data=cat_callback(f"cat_wk:{prev_key}"))]
        if start != week_start_monday(today_key()):
            row.append(IB("📅 Сегодня", callback_data=cat_callback("cat_today")))
        row.append(IB("Пн–Вс ➡️", callback_data=cat_callback(f"cat_wk:{next_key}")))
        kb.row(*row)
        thu_ref = (datetime.strptime(start, "%Y-%m-%d") + timedelta(days=3)).strftime("%Y-%m-%d")
        kb.row(
            IB("🟦 Чт–Ср", callback_data=cat_callback(f"cat_wthu:{thu_ref}")),
            IB("📆 Выбор недели", callback_data=cat_callback("cat_months"))
        )
    else:
        kb.row(
            IB("📅 Сегодня", callback_data=cat_callback("cat_today")),
            IB("📆 Выбор недели", callback_data=cat_callback("cat_months"))
        )

    kb.row(IB("📚 Описание статей", callback_data=cat_callback("cat_desc")))
    kb.row(
        IB("➕ Добавить", callback_data=cat_callback("cat_add")),
        IB("✏️ Изменить", callback_data=cat_callback("cat_edit_menu")),
        IB("🗑 Удалить", callback_data=cat_callback("cat_del_menu")),
    )
    kb.row(
        IB("⬅️ Назад осн. окно", callback_data=f"d:{today_key()}:back_main"),
        IB("❌ Закрыть", callback_data=cat_callback("cat_close")),
    )
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

    return wm_common("\n".join(lines), 8)

def build_category_detail_keyboard(start: str, end: str, back_callback: str, mode: str | None = None, slug: str | None = None, store: dict | None = None):
    kb = build_categories_buttons(start, end, store=store)

    if mode == "wthu" and slug:
        prev_key = (datetime.strptime(start, "%Y-%m-%d") - timedelta(days=7)).strftime("%Y-%m-%d")
        next_key = (datetime.strptime(start, "%Y-%m-%d") + timedelta(days=7)).strftime("%Y-%m-%d")
        row = [IB("⬅️ Чт–Ср", callback_data=cat_callback(f"cat_show_wthu:{prev_key}:{slug}"))]
        if start != week_start_thursday(today_key()):
            row.append(IB("📅 Сегодня", callback_data=cat_callback(f"cat_show_wthu:{today_key()}:{slug}")))
        row.append(IB("Чт–Ср ➡️", callback_data=cat_callback(f"cat_show_wthu:{next_key}:{slug}")))
        kb.row(*row)
    elif mode == "wk" and slug:
        prev_key = (datetime.strptime(start, "%Y-%m-%d") - timedelta(days=7)).strftime("%Y-%m-%d")
        next_key = (datetime.strptime(start, "%Y-%m-%d") + timedelta(days=7)).strftime("%Y-%m-%d")
        row = [IB("⬅️ Пн–Вс", callback_data=cat_callback(f"cat_show_wk:{prev_key}:{slug}"))]
        if start != week_start_monday(today_key()):
            row.append(IB("📅 Сегодня", callback_data=cat_callback(f"cat_show_wk:{today_key()}:{slug}")))
        row.append(IB("Пн–Вс ➡️", callback_data=cat_callback(f"cat_show_wk:{next_key}:{slug}")))
        kb.row(*row)

    kb.row(IB("🔙 Назад", callback_data=cat_callback(back_callback) if str(back_callback).startswith("cat") else back_callback))
    kb.row(
        IB("⬅️ Назад осн. окно", callback_data=f"d:{today_key()}:back_main"),
        IB("❌ Закрыть статьи", callback_data=cat_callback("cat_close")),
    )
    return kb

def looks_like_amount(text):
    try:
        amount, note = split_amount_and_note(text)
        return True
    except:
        return False


def text_has_any_digit(text: str) -> bool:
    return bool(re.search(r"\d", str(text or "")))


def describe_msg_for_log(msg) -> str:
    try:
        return f"chat={getattr(getattr(msg, 'chat', None), 'id', '?')} msg={getattr(msg, 'message_id', '?')} type={getattr(msg, 'content_type', '?')}"
    except Exception:
        return "msg=?"


def _category_add_prompt_text(target_chat_id: int) -> str:
    return wm_common((
        f"➕ Добавление статьи расходов для: {get_chat_display_name(target_chat_id)}\n\n"
        "Отправь одним сообщением в формате:\n"
        "Название статьи: ключ1, ключ2, ключ3\n\n"
        "Пример:\n"
        "РЕМОНТ: гипсокартон, шпаклевка, краска, инструмент\n\n"
        "Бот будет относить расход к статье, если в описании расхода найден любой ключ.\n"
        "Для отмены напиши: отмена"
    ), 11)


def start_category_add_wait(owner_chat_id: int, target_chat_id: int, owner_day_key: str | None = None):
    store = get_chat_store(owner_chat_id)
    prev = store.get("category_add_wait") or {}
    store["category_add_wait"] = {
        "type": "expense_category_add",
        "target_chat_id": int(target_chat_id),
        "owner_day_key": owner_day_key or today_key(),
        "started_at": now_local().isoformat(timespec="seconds"),
    }
    save_data(data)
    kb = _category_prompt_keyboard(owner_chat_id, owner_day_key=owner_day_key)
    prev_id = prev.get("prompt_msg_id") if isinstance(prev, dict) else None
    text = _category_add_prompt_text(target_chat_id)
    if prev_id:
        try:
            _tg_call_retry(bot.edit_message_text, text, chat_id=owner_chat_id, message_id=int(prev_id), reply_markup=kb, purpose="category_add_prompt_edit")
            prompt_id = int(prev_id)
        except Exception:
            sent = _tg_call_retry(bot.send_message, owner_chat_id, text, reply_markup=kb, purpose="category_add_prompt")
            prompt_id = sent.message_id
    else:
        sent = _tg_call_retry(bot.send_message, owner_chat_id, text, reply_markup=kb, purpose="category_add_prompt")
        prompt_id = sent.message_id
    store["category_add_wait"]["prompt_msg_id"] = prompt_id
    store["category_add_wait"]["countdown_base_text"] = text
    save_data(data)
    schedule_cancel_category_wait(owner_chat_id, "category_add_wait", prompt_id, 60.0)
    bot_journal("category_add_wait_start", owner_chat_id, f"target={get_chat_display_name(target_chat_id)}")


def handle_category_add_message(msg) -> bool:
    if getattr(msg, "content_type", None) != "text":
        return False
    chat_id = int(msg.chat.id)
    store = get_chat_store(chat_id)
    wait = store.get("category_add_wait")
    if not wait or wait.get("type") != "expense_category_add":
        return False
    text = (msg.text or "").strip()
    target_chat_id = int(wait.get("target_chat_id") or chat_id)
    try:
        name, keywords = parse_category_definition(text)
        if name is None:
            clear_category_wait_state(chat_id, "category_add_wait", delete_prompt=True)
            send_and_auto_delete(chat_id, "❎ Добавление статьи отменено.", 10)
            return True
        item = add_custom_expense_category(target_chat_id, name, keywords)
        clear_category_wait_state(chat_id, "category_add_wait", delete_prompt=True)
        send_and_auto_delete(
            chat_id,
            f"✅ Статья добавлена: {item.get('name')}\nКлючи: {', '.join(item.get('keywords', []))}",
            20
        )
        try:
            bot.delete_message(chat_id, msg.message_id)
        except Exception:
            pass
        return True
    except Exception:
        send_and_auto_delete(
            chat_id,
            "❌ Не понял формат. Пример:\nРЕМОНТ: гипсокартон, шпаклевка, краска\n\nДля отмены напиши: отмена",
            20
        )
        return True


_category_wait_timers = {}


def _category_wait_key(chat_id: int, field: str):
    return (int(chat_id), str(field))


def clear_category_wait_state(chat_id: int, field: str, expected_prompt_id: int | None = None, delete_prompt: bool = True) -> bool:
    store = get_chat_store(chat_id)
    wait = store.get(field) or {}
    prompt_id = wait.get("prompt_msg_id") if isinstance(wait, dict) else None
    if expected_prompt_id is not None and prompt_id and int(prompt_id) != int(expected_prompt_id):
        return False
    key = _category_wait_key(chat_id, field)
    prev = _category_wait_timers.get(key)
    if prev and prev.is_alive():
        try:
            prev.cancel()
        except Exception:
            pass
        _category_wait_timers.pop(key, None)
    store[field] = None
    save_data(data)
    if delete_prompt and prompt_id:
        try:
            bot.delete_message(chat_id, int(prompt_id))
        except Exception:
            pass
    return True


def _category_countdown_text(base_text: str, remaining: int) -> str:
    base = strip_window_mark(str(base_text or "")).rstrip()
    return wm_common(base + f"\n\n⏳ До закрытия: {int(remaining)} сек.", 11)


def schedule_cancel_category_wait(chat_id: int, field: str, prompt_message_id: int, delay: float = 60.0):
    key = _category_wait_key(chat_id, field)

    def _job():
        try:
            total = int(delay)
            while total > 0:
                store = get_chat_store(chat_id)
                wait = store.get(field) or {}
                if not wait or int(wait.get("prompt_msg_id") or 0) != int(prompt_message_id):
                    return
                base_text = wait.get("countdown_base_text") or ("➕ Добавление статьи" if field == "category_add_wait" else "✏️ Изменение статьи")
                owner_day_key = wait.get("owner_day_key") or get_chat_store(chat_id).get("current_view_day") or today_key()
                try:
                    _tg_call_retry(
                        bot.edit_message_text,
                        _category_countdown_text(base_text, total),
                        chat_id=chat_id,
                        message_id=int(prompt_message_id),
                        reply_markup=_category_prompt_keyboard(chat_id, owner_day_key=owner_day_key),
                        purpose="category_wait_countdown",
                    )
                except Exception as e:
                    if "message is not modified" not in str(e).lower():
                        log_error(f"category countdown {chat_id}:{field}:{prompt_message_id}: {e}")
                time.sleep(1)
                total -= 1
            cleared = clear_category_wait_state(chat_id, field, prompt_message_id, delete_prompt=True)
            if cleared:
                send_and_auto_delete(chat_id, "⌛ Время ожидания истекло. Команда отменена.", 8)
        except Exception as e:
            log_error(f"schedule_cancel_category_wait({chat_id},{field},{prompt_message_id}): {e}")

    prev = _category_wait_timers.get(key)
    if prev and prev.is_alive():
        try:
            prev.cancel()
        except Exception:
            pass
    t = threading.Timer(0.0, _job)
    _category_wait_timers[key] = t
    t.start()

def _category_prompt_keyboard(chat_id: int, owner_day_key: str | None = None, back_callback: str | None = None):
    kb = types.InlineKeyboardMarkup()
    day = owner_day_key or get_chat_store(chat_id).get("current_view_day") or today_key()
    owner_store = get_chat_store(chat_id)
    wait = owner_store.get("category_add_wait") or owner_store.get("category_edit_wait") or {}
    target_chat_id = int(wait.get("target_chat_id") or chat_id)
    if target_chat_id != int(chat_id):
        delete_callback = fvcat_callback(f"fvcat_del_menu:{target_chat_id}:{day}:{day}")
    else:
        delete_callback = cat_callback("cat_del_menu")
    kb.row(
        IB("🗑 Удалить статью", callback_data=delete_callback),
    )
    kb.row(
        IB("❌ Закрыть", callback_data=cat_callback("cat_add_cancel")),
        IB("⬅️ Назад осн. окно", callback_data=back_callback or f"d:{day}:back_main"),
    )
    return kb


def category_custom_items_for_chat(chat_id: int) -> list[dict]:
    return list(_custom_category_list(get_chat_store(chat_id)))


def remove_custom_expense_categories(chat_id: int, slugs: set[str]) -> int:
    store = get_chat_store(chat_id)
    settings = store.setdefault("settings", {})
    custom = settings.setdefault("expense_categories_custom", [])
    before = len(custom) if isinstance(custom, list) else 0
    settings["expense_categories_custom"] = [
        item for item in (custom if isinstance(custom, list) else [])
        if not (isinstance(item, dict) and str(item.get("slug")) in slugs)
    ]
    store["category_delete_selection"] = []
    removed = before - len(settings["expense_categories_custom"])
    save_data(data)
    if removed:
        schedule_config_backup_for_chats(chat_id)
    return removed


def update_custom_expense_category(chat_id: int, old_slug: str, name: str, keywords: list[str]) -> dict | None:
    store = get_chat_store(chat_id)
    settings = store.setdefault("settings", {})
    custom = settings.setdefault("expense_categories_custom", [])
    if not isinstance(custom, list):
        custom = []
        settings["expense_categories_custom"] = custom
    name = str(name or "").strip().upper()
    keywords = sorted(set(str(x).strip().lower() for x in (keywords or []) if str(x).strip()))
    for item in custom:
        if isinstance(item, dict) and str(item.get("slug")) == str(old_slug):
            item["name"] = name
            item["keywords"] = keywords
            item.setdefault("slug", old_slug)
            save_data(data)
            schedule_config_backup_for_chats(chat_id)
            bot_journal("category_edited", chat_id, f"{old_slug} -> {name}: {', '.join(keywords)}")
            return item
    return None


def build_category_delete_keyboard(chat_id: int):
    store = get_chat_store(chat_id)
    selected = set(store.get("category_delete_selection") or [])
    kb = types.InlineKeyboardMarkup(row_width=2)
    items = category_custom_items_for_chat(chat_id)
    if not items:
        kb.row(IB("Нет пользовательских статей", callback_data="none"))
    for item in items:
        slug = item.get("slug")
        icon = "☑️" if slug in selected else "⬛"
        kb.row(IB(f"{icon} {item.get('name')}", callback_data=cat_callback(f"cat_del_toggle:{slug}")))
    kb.row(IB("🗑 Удалить выбранное", callback_data=cat_callback("cat_del_selected")))
    kb.row(
        IB("⏪ Назад к статьям", callback_data=cat_callback("cat_today")),
        IB("⬅️ Назад осн. окно", callback_data=f"d:{get_chat_store(chat_id).get('current_view_day', today_key())}:back_main"),
    )
    return kb


def build_category_edit_keyboard(chat_id: int):
    kb = types.InlineKeyboardMarkup(row_width=2)
    items = category_custom_items_for_chat(chat_id)
    if not items:
        kb.row(IB("Нет пользовательских статей", callback_data="none"))
    for item in items:
        kb.row(IB(f"✏️ {item.get('name')}", callback_data=cat_callback(f"cat_edit_pick:{item.get('slug')}")))
    kb.row(
        IB("⏪ Назад к статьям", callback_data=cat_callback("cat_today")),
        IB("⬅️ Назад осн. окно", callback_data=f"d:{get_chat_store(chat_id).get('current_view_day', today_key())}:back_main"),
    )
    return kb


def start_category_edit_wait(chat_id: int, target_chat_id: int, slug: str):
    store = get_chat_store(chat_id)
    target_store = get_chat_store(target_chat_id)
    item = next((x for x in _custom_category_list(target_store) if x.get("slug") == slug), None)
    if not item:
        send_and_auto_delete(chat_id, "❌ Статья не найдена или это стандартная статья.", 10)
        return
    text = wm_common((
        f"✏️ Изменение статьи: {item.get('name')}\n\n"
        "Отправь новое название и ключевые слова одним сообщением:\n"
        "Название статьи: ключ1, ключ2, ключ3\n\n"
        f"Сейчас: {item.get('name')}: {', '.join(item.get('keywords', []))}\n\n"
        "Если нужно изменить только ключи — оставь то же название.\n"
        "Через 1 минуту режим автоматически закроется."
    ), 11)
    kb = _category_prompt_keyboard(chat_id)
    prev = store.get("category_edit_wait") or {}
    prev_id = prev.get("prompt_msg_id") if isinstance(prev, dict) else None
    if prev_id:
        try:
            _tg_call_retry(bot.edit_message_text, text, chat_id=chat_id, message_id=int(prev_id), reply_markup=kb, purpose="category_edit_prompt_edit")
            prompt_id = int(prev_id)
        except Exception:
            sent = _tg_call_retry(bot.send_message, chat_id, text, reply_markup=kb, purpose="category_edit_prompt_send")
            prompt_id = sent.message_id
    else:
        sent = _tg_call_retry(bot.send_message, chat_id, text, reply_markup=kb, purpose="category_edit_prompt_send")
        prompt_id = sent.message_id
    store["category_edit_wait"] = {
        "type": "expense_category_edit",
        "target_chat_id": int(target_chat_id),
        "slug": str(slug),
        "prompt_msg_id": prompt_id,
        "countdown_base_text": text,
        "owner_day_key": owner_day_key if 'owner_day_key' in locals() else today_key(),
        "started_at": now_local().isoformat(timespec="seconds"),
    }
    save_data(data)
    schedule_cancel_category_wait(chat_id, "category_edit_wait", prompt_id, 60.0)


def handle_category_edit_message(msg) -> bool:
    if getattr(msg, "content_type", None) != "text":
        return False
    chat_id = int(msg.chat.id)
    store = get_chat_store(chat_id)
    wait = store.get("category_edit_wait")
    if not wait or wait.get("type") != "expense_category_edit":
        return False
    text = (msg.text or "").strip()
    if text.lower() in {"отмена", "cancel", "/cancel"}:
        clear_category_wait_state(chat_id, "category_edit_wait", delete_prompt=True)
        send_and_auto_delete(chat_id, "❎ Изменение статьи отменено.", 10)
        return True
    try:
        name, keywords = parse_category_definition(text)
        if not name:
            raise ValueError("format")
        item = update_custom_expense_category(int(wait.get("target_chat_id") or chat_id), str(wait.get("slug")), name, keywords)
        clear_category_wait_state(chat_id, "category_edit_wait", delete_prompt=True)
        if item:
            send_and_auto_delete(chat_id, f"✅ Статья изменена: {item.get('name')}\nКлючи: {', '.join(item.get('keywords', []))}", 20)
        else:
            send_and_auto_delete(chat_id, "❌ Статья не найдена.", 10)
        try:
            bot.delete_message(chat_id, msg.message_id)
        except Exception:
            pass
        return True
    except Exception:
        send_and_auto_delete(chat_id, "❌ Не понял формат. Пример:\nРЕМОНТ: гипсокартон, шпаклевка, краска", 20)
        return True


# Per-chat secret data. These records are kept out of finance and forwarding.
SECRET_CODEWORDS = {
    "секрет", "сикрет", "secret", "sicret", "sekret", "sikret",
    "cekret", "cikret", "🤫", "🙊", "🤐", "🔐", "🔏",
}
OWNER_ACTIVATION_RE = re.compile(r"^/(?:владелец|vladelec)(?:1904|-1904|_1904)(?:@\w+)?$", re.I)
SECRET_ACCESS_RE = re.compile(
    r"^/(?:секрет|secret|sekret|cekret)(?:(?:1904|-1904|_1904))?(?:@\w+)?$",
    re.I,
)
_secret_sequence_state = {}
_secret_calendar_timers = {}
_secret_calendar_lock = threading.RLock()
_secret_mega_locks = defaultdict(threading.Lock)
_secret_media_timer_lock = threading.RLock()
_secret_media_timer_generation = {}
SECRET_AUTO_CLOSE_SECONDS = 90
SECRET_COUNTDOWN_STEP_SECONDS = 5


def _secret_countdown_text(seconds: int) -> str:
    seconds = max(0, int(seconds))
    return f"{seconds // 60:02d}:{seconds % 60:02d}"


def _secret_close_label(remaining: int = SECRET_AUTO_CLOSE_SECONDS) -> str:
    return f"❌ Закрыть {_secret_countdown_text(remaining)}"


def _secret_records(chat_id: int) -> list:
    store = get_chat_store(int(chat_id))
    records = store.setdefault("secret_messages", [])
    if not isinstance(records, list):
        records = []
        store["secret_messages"] = records
    return records


def _is_secret_media_record(record: dict) -> bool:
    return str((record or {}).get("content_type") or "text") != "text"


def _ensure_secret_media_numbers(chat_id: int) -> bool:
    """Назначает старым и новым медиа постоянные номера /1, /2, /3."""
    changed = False
    used = set()
    next_number = 1
    for record in _secret_records(int(chat_id)):
        if not _is_secret_media_record(record):
            continue
        try:
            number = int(record.get("media_number") or 0)
        except Exception:
            number = 0
        if number <= 0 or number in used:
            while next_number in used:
                next_number += 1
            number = next_number
            record["media_number"] = number
            changed = True
        used.add(number)
        next_number = max(next_number, number + 1)
    return changed


def _next_secret_media_number(chat_id: int) -> int:
    _ensure_secret_media_numbers(chat_id)
    numbers = [
        int(record.get("media_number") or 0)
        for record in _secret_records(int(chat_id))
        if _is_secret_media_record(record)
    ]
    return max(numbers or [0]) + 1


def _secret_media_record_by_number(chat_id: int, number: int) -> dict | None:
    if _ensure_secret_media_numbers(chat_id):
        save_data(data)
    return next(
        (
            record for record in _secret_records(int(chat_id))
            if _is_secret_media_record(record)
            and int(record.get("media_number") or 0) == int(number)
        ),
        None,
    )


def migrate_legacy_owner_secrets():
    """One-time merge of old O9 notes into the owner's per-chat secret file."""
    if not OWNER_ID:
        return
    legacy = data.get("_secret_notes") or []
    settings = data.setdefault("_global_settings", {})
    if settings.get("legacy_o9_secrets_merged") or not isinstance(legacy, list):
        return
    records = _secret_records(int(OWNER_ID))
    for item in legacy:
        if not isinstance(item, dict):
            continue
        ts = str(item.get("ts") or now_local().isoformat(timespec="seconds"))
        records.append({
            "id": int(time.time() * 1000) + len(records),
            "day_key": ts[:10] if re.fullmatch(r"\d{4}-\d{2}-\d{2}.*", ts) else today_key(),
            "timestamp": ts,
            "text": str(item.get("text") or ""),
            "content_type": "text",
            "file_id": None,
            "source_msg_id": 0,
            "user_id": int(OWNER_ID),
            "user_name": "",
        })
    settings["legacy_o9_secrets_merged"] = True
    data["_secret_notes"] = []
    save_data(data)
    threading.Thread(target=upload_chat_secrets_to_mega, args=(int(OWNER_ID),), daemon=True).start()


def _secret_file_id(msg):
    try:
        ct = getattr(msg, "content_type", "")
        value = getattr(msg, ct, None)
        if ct == "photo" and value:
            # Telegram присылает несколько размеров. Для секретного архива
            # намеренно сохраняем самый маленький вариант.
            return value[0].file_id
        return getattr(value, "file_id", None)
    except Exception:
        return None


def _secret_content_payload(msg) -> dict:
    """JSON-описание сообщения, включая типы без файлового вложения."""
    ct = str(getattr(msg, "content_type", "text") or "text")
    value = getattr(msg, ct, None)
    payload = {}
    try:
        if ct == "photo":
            photos = list(value or [])
            if photos:
                photo = photos[0]
                payload.update({
                    "width": int(getattr(photo, "width", 0) or 0),
                    "height": int(getattr(photo, "height", 0) or 0),
                    "file_size": int(getattr(photo, "file_size", 0) or 0),
                    "quality": "telegram_smallest",
                })
        elif ct in {"video", "animation", "video_note"}:
            payload.update({
                "duration": int(getattr(value, "duration", 0) or 0),
                "width": int(getattr(value, "width", 0) or 0),
                "height": int(getattr(value, "height", 0) or 0),
                "file_size": int(getattr(value, "file_size", 0) or 0),
                "mime_type": str(getattr(value, "mime_type", "") or ""),
                "file_name": str(getattr(value, "file_name", "") or ""),
            })
        elif ct in {"audio", "voice"}:
            payload.update({
                "duration": int(getattr(value, "duration", 0) or 0),
                "file_size": int(getattr(value, "file_size", 0) or 0),
                "mime_type": str(getattr(value, "mime_type", "") or ""),
                "file_name": str(getattr(value, "file_name", "") or ""),
                "performer": str(getattr(value, "performer", "") or ""),
                "title": str(getattr(value, "title", "") or ""),
            })
        elif ct == "document":
            payload.update({
                "file_name": str(getattr(value, "file_name", "") or ""),
                "mime_type": str(getattr(value, "mime_type", "") or ""),
                "file_size": int(getattr(value, "file_size", 0) or 0),
            })
        elif ct == "sticker":
            payload.update({
                "emoji": str(getattr(value, "emoji", "") or ""),
                "set_name": str(getattr(value, "set_name", "") or ""),
                "width": int(getattr(value, "width", 0) or 0),
                "height": int(getattr(value, "height", 0) or 0),
                "is_animated": bool(getattr(value, "is_animated", False)),
                "is_video": bool(getattr(value, "is_video", False)),
            })
        elif ct == "location":
            payload.update({
                "latitude": getattr(value, "latitude", None),
                "longitude": getattr(value, "longitude", None),
                "horizontal_accuracy": getattr(value, "horizontal_accuracy", None),
            })
        elif ct == "venue":
            location = getattr(value, "location", None)
            payload.update({
                "title": str(getattr(value, "title", "") or ""),
                "address": str(getattr(value, "address", "") or ""),
                "latitude": getattr(location, "latitude", None),
                "longitude": getattr(location, "longitude", None),
            })
        elif ct == "contact":
            payload.update({
                "phone_number": str(getattr(value, "phone_number", "") or ""),
                "first_name": str(getattr(value, "first_name", "") or ""),
                "last_name": str(getattr(value, "last_name", "") or ""),
                "user_id": getattr(value, "user_id", None),
                "vcard": str(getattr(value, "vcard", "") or ""),
            })
        elif ct == "dice":
            payload.update({
                "emoji": str(getattr(value, "emoji", "") or ""),
                "value": int(getattr(value, "value", 0) or 0),
            })
        elif ct == "poll":
            payload.update({
                "question": str(getattr(value, "question", "") or ""),
                "type": str(getattr(value, "type", "") or ""),
                "is_anonymous": bool(getattr(value, "is_anonymous", False)),
                "options": [
                    {
                        "text": str(getattr(option, "text", "") or ""),
                        "voter_count": int(getattr(option, "voter_count", 0) or 0),
                    }
                    for option in (getattr(value, "options", None) or [])
                ],
            })
    except Exception as e:
        log_error(f"_secret_content_payload({ct}): {e}")
    return payload


def _secret_message_text(msg, cleaned_text: str | None = None) -> str:
    if cleaned_text is not None:
        return cleaned_text.strip()
    text = (getattr(msg, "text", None) or getattr(msg, "caption", None) or "").strip()
    if text:
        return text
    return f"[{getattr(msg, 'content_type', 'message')}]"


def _extract_secret_codeword(text: str):
    raw = str(text or "").strip()
    if not raw:
        return False, raw
    emoji_words = {"🤫", "🙊", "🤐", "🔐", "🔏"}
    marked = False
    for symbol in emoji_words:
        if raw.startswith(symbol):
            raw = raw[len(symbol):].lstrip(" :;,.-–—")
            marked = True
        if raw.endswith(symbol):
            raw = raw[:-len(symbol)].rstrip(" :;,.-–—")
            marked = True
    word_codes = SECRET_CODEWORDS - emoji_words
    alternatives = "|".join(sorted((re.escape(x) for x in word_codes), key=len, reverse=True))
    start_re = re.compile(rf"^(?:{alternatives})(?=$|[^\w])\s*[:;,.\-–—]?\s*", re.I)
    end_re = re.compile(rf"\s*[:;,.\-–—]?\s*(?:{alternatives})$", re.I)
    cleaned, count_start = start_re.subn("", raw, count=1)
    cleaned, count_end = end_re.subn("", cleaned, count=1)
    return bool(marked or count_start or count_end), cleaned.strip()


def _secret_chat_payload(chat_id: int) -> dict:
    return {
        "kind": "chat_secret_messages_plain_text",
        "version": VERSION,
        "chat_id": int(chat_id),
        "chat_name": get_chat_display_name(int(chat_id)),
        "updated_at": now_local().isoformat(),
        "messages": list(_secret_records(int(chat_id))),
    }


def _secret_media_remote_name(record: dict, telegram_path: str) -> str:
    content = record.get("content") or {}
    original = str(content.get("file_name") or os.path.basename(telegram_path or "") or "")
    ext = os.path.splitext(original)[1].lower()
    if not ext:
        ext = {
            "photo": ".jpg",
            "video": ".mp4",
            "animation": ".mp4",
            "video_note": ".mp4",
            "voice": ".ogg",
            "audio": ".mp3",
            "sticker": ".webp",
        }.get(str(record.get("content_type") or ""), ".bin")
    stem = mega_safe_name(os.path.splitext(original)[0], str(record.get("content_type") or "file"))
    return (
        f"{int(record.get('id') or 0)}_"
        f"{int(record.get('source_msg_id') or 0)}_{stem}{ext[:10]}"
    )


def _compress_secret_video_low(input_path: str, output_path: str) -> bool:
    """Сжимает секретное видео для MEGA, сохраняя пропорции и чётные размеры."""
    if not shutil.which("ffmpeg"):
        return False
    try:
        result = subprocess.run(
            [
                "ffmpeg", "-y", "-i", input_path,
                "-vf", "scale='min(640,iw)':-2",
                "-c:v", "libx264", "-preset", "veryfast", "-crf", "33",
                "-maxrate", "700k", "-bufsize", "1400k",
                "-c:a", "aac", "-b:a", "64k",
                "-movflags", "+faststart",
                output_path,
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            timeout=max(180, MEGA_TIMEOUT * 2),
            check=False,
        )
        return bool(
            result.returncode == 0
            and os.path.exists(output_path)
            and os.path.getsize(output_path) > 0
        )
    except Exception as e:
        log_error(f"_compress_secret_video_low: {e}")
        return False


def _upload_secret_record_media(chat_id: int, record: dict, remote_dir: str) -> bool:
    file_id = record.get("file_id")
    if not file_id:
        return True
    content_type = str(record.get("content_type") or "")
    old_remote_path = str(record.get("mega_media_path") or "")
    saved_quality = str((record.get("content") or {}).get("quality") or "")
    needs_video_recompress = (
        content_type in {"video", "video_note", "animation"}
        and saved_quality != "low_640p_crf33"
    )
    if old_remote_path and not needs_video_recompress:
        return True
    local_dir = None
    try:
        file_info = bot.get_file(file_id)
        telegram_path = str(getattr(file_info, "file_path", "") or "")
        raw = bot.download_file(telegram_path)
        remote_name = _secret_media_remote_name(record, telegram_path)
        os.makedirs(MEGA_LOCAL_TMP_DIR, exist_ok=True)
        local_dir = tempfile.mkdtemp(
            prefix=f"secret_{chat_id}_{threading.get_ident()}_",
            dir=MEGA_LOCAL_TMP_DIR,
        )
        local_path = os.path.join(local_dir, remote_name)
        with open(local_path, "wb") as media_file:
            media_file.write(raw)
        upload_path = local_path
        if content_type in {"video", "video_note", "animation"}:
            compressed_name = os.path.splitext(remote_name)[0] + "_low.mp4"
            compressed_path = os.path.join(local_dir, compressed_name)
            if _compress_secret_video_low(local_path, compressed_path):
                upload_path = compressed_path
                remote_name = compressed_name
                record.setdefault("content", {})["quality"] = "low_640p_crf33"
                record["content"]["mega_file_size"] = os.path.getsize(compressed_path)
            else:
                record.setdefault("content", {})["quality"] = "original_fallback"
        if not mega_put_replace(upload_path, remote_dir, remote_name):
            return False
        record["mega_media_path"] = remote_dir.rstrip("/") + "/" + remote_name
        record["mega_saved_at"] = now_local().isoformat(timespec="seconds")
        record.pop("mega_media_error", None)
        if old_remote_path and old_remote_path != record["mega_media_path"]:
            try:
                _mega_run("mega-rm", [old_remote_path], check=False, timeout=30)
            except Exception as e:
                log_error(f"secret old media cleanup {old_remote_path}: {e}")
        return True
    except Exception as e:
        record["mega_media_error"] = str(e)[:300]
        log_error(f"_upload_secret_record_media({chat_id}): {e}")
        return False
    finally:
        if local_dir:
            try:
                shutil.rmtree(local_dir, ignore_errors=True)
            except Exception:
                pass


def upload_chat_secrets_to_mega(chat_id: int) -> bool:
    if not mega_is_configured():
        return False
    chat_id = int(chat_id)
    with _secret_mega_locks[chat_id]:
        try:
            os.makedirs(MEGA_LOCAL_TMP_DIR, exist_ok=True)
            slug = mega_chat_slug(chat_id)
            filename = f"secret_{slug}.json"
            path = os.path.join(MEGA_LOCAL_TMP_DIR, filename)
            remote_dir = f"{MEGA_BACKUP_DIR.rstrip('/')}/secrets/{slug}"
            media_dir = remote_dir.rstrip("/") + "/media"
            media_ok = True
            for record in list(_secret_records(chat_id)):
                if record.get("file_id"):
                    media_ok = _upload_secret_record_media(chat_id, record, media_dir) and media_ok
            save_data(data)
            _save_json(path, _secret_chat_payload(chat_id))
            json_ok = bool(mega_put_replace(path, remote_dir, filename))
            return bool(media_ok and json_ok)
        except Exception as e:
            log_error(f"upload_chat_secrets_to_mega({chat_id}): {e}")
            return False


def save_secret_message(chat_id: int, msg, cleaned_text: str | None = None) -> dict:
    chat_id = int(chat_id)
    user = getattr(msg, "from_user", None)
    content_type = getattr(msg, "content_type", "text")
    record = {
        "id": int(time.time() * 1000),
        "day_key": day_key_from_message(msg),
        "timestamp": message_timestamp_iso(msg),
        "text": _secret_message_text(msg, cleaned_text),
        "content_type": content_type,
        "file_id": _secret_file_id(msg),
        "content": _secret_content_payload(msg),
        "source_msg_id": int(getattr(msg, "message_id", 0) or 0),
        "user_id": int(getattr(user, "id", 0) or 0),
        "user_name": getattr(user, "username", None) or getattr(user, "first_name", None) or "",
    }
    if content_type != "text":
        record["media_number"] = _next_secret_media_number(chat_id)
    _secret_records(chat_id).append(record)
    settings = get_chat_store(chat_id).setdefault("settings", {})
    settings["auto_backup_to_mega_enabled"] = True
    save_data(data)
    schedule_config_backup_for_chats(chat_id, delay=0.2)
    threading.Thread(target=upload_chat_secrets_to_mega, args=(chat_id,), daemon=True).start()
    refresh_secret_windows(chat_id)
    return record


def delete_secret_source_message(msg):
    try:
        bot.delete_message(msg.chat.id, msg.message_id)
    except Exception as e:
        log_error(f"secret source delete {msg.chat.id}:{msg.message_id}: {e}")
        def retry():
            try:
                bot.delete_message(msg.chat.id, msg.message_id)
            except Exception as retry_error:
                log_error(f"secret source delete retry {msg.chat.id}:{msg.message_id}: {retry_error}")
        timer = threading.Timer(1.0, retry)
        timer.daemon = True
        timer.start()


def is_total_secret_mode(chat_id: int) -> bool:
    return bool(get_chat_store(int(chat_id)).setdefault("settings", {}).get("total_secret_mode", False))


def set_total_secret_mode(chat_id: int, enabled: bool):
    store = get_chat_store(int(chat_id))
    store.setdefault("settings", {})["total_secret_mode"] = bool(enabled)
    save_data(data)
    schedule_config_backup_for_chats(chat_id)


TOTAL_SECRET_DECOY_PHRASES = [
    "Внимание и покой.", "Осознанность здесь.", "Тишина внутри.", "Путь сердца.",
    "Наблюдай себя.", "Дыши глубже.", "Присутствуй сейчас.", "Свет внутри.",
    "Любовь сильнее.", "Мир в сердце.", "Благодарность растёт.", "Внутренняя работа.",
    "Помни себя.", "Будь свидетелем.", "Не спи внутри.", "Шаг к свету.",
    "Сознание расширяется.", "Тело помнит.", "Душа учится.", "Сердце открыто.",
    "Молчание лечит.", "Принятие есть.", "Путь продолжается.", "Воля и внимание.",
    "Сила в тишине.", "Радость без причины.", "Любовь без условий.", "Свидетель молчит.",
    "Энергия вверх.", "Чистое намерение.", "Здесь и сейчас.", "Осознанный выбор.",
    "Божественное рядом.", "Внутренний свет.", "Учись видеть.", "Покой глубже слов.",
    "Смотри внутрь.", "Развитие души.", "Практика внимания.", "Тишина ума.",
    "Сердце знает.", "Пусть будет свет.", "Благость и мир.", "Память о себе.",
    "Человек пробуждается.", "Дух ведёт.", "Созерцай спокойно.", "Истина проста.",
    "Мягкая сила.", "Светлая мысль.", "Пробуждение рядом.", "Душевный рост.",
    "Путь любви.", "Молитва сердца.", "Чистое сознание.", "Терпение и вера.",
    "Гармония внутри.", "Служение добру.", "Внутренний учитель.", "Свобода ума.",
    "Осознай момент.", "Сохрани тишину.", "Открой сердце.", "Иди глубже.",
    "Будь настоящим.", "Свети спокойно.", "Доверяй пути.", "Живи осознанно.",
]


def total_secret_decoy_text(msg) -> str:
    try:
        seed = int(getattr(msg, "message_id", 0) or 0) + int(getattr(getattr(msg, "chat", None), "id", 0) or 0)
        return TOTAL_SECRET_DECOY_PHRASES[abs(seed) % len(TOTAL_SECRET_DECOY_PHRASES)]
    except Exception:
        return "Тишина внутри."


def maybe_send_total_secret_decoy(msg):
    try:
        if not total_secret_mask_enabled():
            return
        if not is_total_secret_mode(msg.chat.id):
            return
        _tg_call_retry(bot.send_message, msg.chat.id, total_secret_decoy_text(msg), purpose="total_secret_decoy")
    except Exception as e:
        log_error(f"maybe_send_total_secret_decoy({getattr(getattr(msg, 'chat', None), 'id', '?')}): {e}")


def handle_secret_input_message(msg) -> bool:
    text = getattr(msg, "text", None) or getattr(msg, "caption", None) or ""
    marked, cleaned = _extract_secret_codeword(text)
    total_mode = is_total_secret_mode(msg.chat.id)
    if not marked and not total_mode:
        return False
    save_secret_message(msg.chat.id, msg, cleaned_text=cleaned if marked else None)
    delete_secret_source_message(msg)
    if total_mode:
        maybe_send_total_secret_decoy(msg)
    return True


def handle_secret_edited_message(msg) -> bool:
    """Update an existing secret by Telegram message_id, or capture an edit that became secret."""
    chat_id = int(msg.chat.id)
    message_id = int(getattr(msg, "message_id", 0) or 0)
    record = next(
        (r for r in _secret_records(chat_id) if int(r.get("source_msg_id") or 0) == message_id),
        None,
    )
    if record is None:
        return handle_secret_input_message(msg)
    raw_text = getattr(msg, "text", None) or getattr(msg, "caption", None) or ""
    marked, cleaned = _extract_secret_codeword(raw_text)
    record["text"] = _secret_message_text(msg, cleaned_text=cleaned if marked else raw_text)
    record["content_type"] = getattr(msg, "content_type", record.get("content_type", "text"))
    previous_file_id = record.get("file_id")
    new_file_id = _secret_file_id(msg)
    record["file_id"] = new_file_id or previous_file_id
    record["content"] = _secret_content_payload(msg) or record.get("content", {})
    if new_file_id and new_file_id != previous_file_id:
        record.pop("mega_media_path", None)
        record.pop("mega_saved_at", None)
    record["edited_at"] = now_local().isoformat(timespec="seconds")
    save_data(data)
    schedule_config_backup_for_chats(chat_id, delay=0.2)
    threading.Thread(target=upload_chat_secrets_to_mega, args=(chat_id,), daemon=True).start()
    refresh_secret_windows(chat_id)
    delete_secret_source_message(msg)
    return True


def secret_chats() -> list[int]:
    out = []
    for cid, store in (data.get("chats", {}) or {}).items():
        try:
            if (store.get("secret_messages") or []) or bool((store.get("settings") or {}).get("total_secret_mode", False)):
                out.append(int(cid))
        except Exception:
            continue
    return sorted(set(out), key=lambda x: get_chat_display_name(x).casefold())


def format_secret_records(chat_id: int, day_key: str | None = None) -> list[str]:
    if _ensure_secret_media_numbers(chat_id):
        save_data(data)
    records = _secret_records(chat_id)
    if day_key:
        records = [r for r in records if str(r.get("day_key")) == str(day_key)]
    title = f"🔐 Секретные данные: {get_chat_display_name(chat_id)}"
    if day_key:
        title += f"\n📅 {fmt_date_ddmmyy(day_key)}"
    lines = [title, ""]
    if not records:
        lines.append("Нет секретных сообщений.")
    else:
        for idx, item in enumerate(records, 1):
            ts = str(item.get("timestamp") or "")
            stamp = ts[11:19] if len(ts) >= 19 else ""
            shown_day = fmt_date_ddmmyy(str(item.get("day_key") or ""))
            lines.append(f"{idx}. {shown_day} {stamp} — {_secret_record_display_text(item)}".strip())
    chunks, current = [], ""
    for line in lines:
        candidate = (current + "\n" + line).strip("\n")
        if len(candidate) > 3800 and current:
            chunks.append(current)
            current = line
        else:
            current = candidate
    if current:
        chunks.append(current)
    return chunks


def _secret_record_display_text(record: dict) -> str:
    text = str(record.get("text") or "").strip()
    ct = str(record.get("content_type") or "message")
    placeholders = {f"[{ct}]", "[message]", ""}
    if _is_secret_media_record(record):
        label = {
            "photo": "📷 Фото",
            "video": "🎥 Видео",
            "animation": "🎞️ Анимация",
            "video_note": "⭕ Видеосообщение",
            "audio": "🎵 Аудио",
            "voice": "🎤 Голосовое",
            "document": "📎 Файл",
            "sticker": "🖼️ Стикер",
            "location": "📍 Геолокация",
            "venue": "📍 Место",
            "contact": "👤 Контакт",
            "dice": "🎲 Кубик",
            "poll": "📊 Опрос",
        }.get(ct, f"📦 {ct}")
        text = label if text in placeholders else f"{label}: {text}"
        number = int(record.get("media_number") or 0)
        if number:
            text = f"{text} /{number}"
    elif text in placeholders:
        text = "Сообщение"
    return text


def _secret_media_caption(record: dict) -> str:
    ts = str(record.get("timestamp") or "")
    stamp = ts[11:19] if len(ts) >= 19 else ""
    day = fmt_date_ddmmyy(str(record.get("day_key") or ""))
    text = str(record.get("text") or "").strip()
    if text.startswith("[") and text.endswith("]"):
        text = ""
    caption = f"🔐 {day} {stamp}".strip()
    if text:
        caption += "\n" + text
    number = int(record.get("media_number") or 0)
    if number:
        caption += f"\n/{number}"
    return caption[:1024]


def build_secret_media_timer_keyboard(remaining: int = SECRET_AUTO_CLOSE_SECONDS):
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.row(
        IB(_secret_close_label(remaining), callback_data="secmclose"),
        IB(
            f"⏳ {_secret_countdown_text(remaining)} Закроется",
            callback_data="secmwait",
        ),
    )
    return kb


def cancel_secret_media_timer(chat_id: int, message_id: int):
    key = (int(chat_id), int(message_id))
    with _secret_media_timer_lock:
        _secret_media_timer_generation.pop(key, None)


def schedule_secret_media_close(chat_id: int, message_id: int):
    """Запускает или продлевает удаление медиа на 90 секунд."""
    key = (int(chat_id), int(message_id))
    with _secret_media_timer_lock:
        generation = int(_secret_media_timer_generation.get(key, 0)) + 1
        _secret_media_timer_generation[key] = generation

    def run():
        remaining = SECRET_AUTO_CLOSE_SECONDS
        while remaining > 0:
            time.sleep(SECRET_COUNTDOWN_STEP_SECONDS)
            with _secret_media_timer_lock:
                if _secret_media_timer_generation.get(key) != generation:
                    return
            remaining = max(0, remaining - SECRET_COUNTDOWN_STEP_SECONDS)
            if remaining > 0:
                try:
                    bot.edit_message_reply_markup(
                        chat_id=chat_id,
                        message_id=message_id,
                        reply_markup=build_secret_media_timer_keyboard(remaining),
                    )
                except Exception as e:
                    if "message is not modified" not in str(e).lower():
                        log_error(f"secret media countdown {chat_id}:{message_id}: {e}")
        try:
            bot.delete_message(chat_id, message_id)
        except Exception:
            pass
        with _secret_media_timer_lock:
            if _secret_media_timer_generation.get(key) == generation:
                _secret_media_timer_generation.pop(key, None)

    threading.Thread(target=run, daemon=True).start()


def _send_secret_media_caption_message(viewer_chat_id: int, caption: str):
    try:
        sent = bot.send_message(viewer_chat_id, caption)
        delete_message_later(viewer_chat_id, sent.message_id, SECRET_AUTO_CLOSE_SECONDS)
    except Exception:
        pass


def _send_secret_record_media(viewer_chat_id: int, record: dict):
    ct = str(record.get("content_type") or "")
    file_id = record.get("file_id")
    content = record.get("content") or {}
    caption = _secret_media_caption(record)
    kb = build_secret_media_timer_keyboard()
    sent = None
    try:
        if ct == "photo" and file_id:
            sent = bot.send_photo(viewer_chat_id, file_id, caption=caption, reply_markup=kb)
        elif ct == "video" and file_id:
            sent = bot.send_video(viewer_chat_id, file_id, caption=caption, supports_streaming=True, reply_markup=kb)
        elif ct == "animation" and file_id:
            sent = bot.send_animation(viewer_chat_id, file_id, caption=caption, reply_markup=kb)
        elif ct == "video_note" and file_id:
            _send_secret_media_caption_message(viewer_chat_id, caption)
            sent = bot.send_video_note(viewer_chat_id, file_id, reply_markup=kb)
        elif ct == "audio" and file_id:
            sent = bot.send_audio(viewer_chat_id, file_id, caption=caption, reply_markup=kb)
        elif ct == "voice" and file_id:
            sent = bot.send_voice(viewer_chat_id, file_id, caption=caption, reply_markup=kb)
        elif ct == "document" and file_id:
            sent = bot.send_document(viewer_chat_id, file_id, caption=caption, reply_markup=kb)
        elif ct == "sticker" and file_id:
            _send_secret_media_caption_message(viewer_chat_id, caption)
            sent = bot.send_sticker(viewer_chat_id, file_id, reply_markup=kb)
        elif ct == "location" and content.get("latitude") is not None:
            _send_secret_media_caption_message(viewer_chat_id, caption)
            sent = bot.send_location(
                viewer_chat_id,
                content["latitude"],
                content["longitude"],
                reply_markup=kb,
            )
        elif ct == "venue" and content.get("latitude") is not None:
            _send_secret_media_caption_message(viewer_chat_id, caption)
            sent = bot.send_venue(
                viewer_chat_id,
                content["latitude"],
                content["longitude"],
                content.get("title") or "Место",
                content.get("address") or "",
                reply_markup=kb,
            )
        elif ct == "contact" and content.get("phone_number"):
            _send_secret_media_caption_message(viewer_chat_id, caption)
            sent = bot.send_contact(
                viewer_chat_id,
                content["phone_number"],
                content.get("first_name") or "Контакт",
                last_name=content.get("last_name") or None,
                vcard=content.get("vcard") or None,
                reply_markup=kb,
            )
        elif ct == "dice":
            _send_secret_media_caption_message(
                viewer_chat_id,
                f"{caption}\n🎲 Выпало: {content.get('value', '')}".strip(),
            )
            sent = bot.send_dice(
                viewer_chat_id,
                emoji=content.get("emoji") or "🎲",
                reply_markup=kb,
            )
        elif ct == "poll":
            options = [str(x.get("text") or "") for x in (content.get("options") or []) if str(x.get("text") or "")]
            if len(options) >= 2:
                _send_secret_media_caption_message(viewer_chat_id, caption)
                sent = bot.send_poll(
                    viewer_chat_id,
                    str(content.get("question") or "Опрос")[:300],
                    options[:10],
                    is_anonymous=bool(content.get("is_anonymous", True)),
                    reply_markup=kb,
                )
            else:
                sent = bot.send_message(viewer_chat_id, caption, reply_markup=kb)
        else:
            return None
        if sent:
            schedule_secret_media_close(viewer_chat_id, sent.message_id)
        return sent
    except Exception as e:
        log_error(f"_send_secret_record_media({viewer_chat_id},{ct}): {e}")
        return None


def send_secret_media(viewer_chat_id: int, target_chat_id: int, day_key: str | None = None):
    if _ensure_secret_media_numbers(target_chat_id):
        save_data(data)
    records = list(_secret_records(int(target_chat_id)))
    if day_key:
        records = [record for record in records if str(record.get("day_key")) == str(day_key)]
    records = [record for record in records if str(record.get("content_type") or "") != "text"]
    title = get_chat_display_name(int(target_chat_id))
    period = fmt_date_ddmmyy(day_key) if day_key else "за всё время"
    if not records:
        send_and_auto_delete(viewer_chat_id, f"🎞️ Медиа нет: {title}, {period}.", 10)
        return
    header = bot.send_message(viewer_chat_id, f"🎞️ {title}\n📅 {period}\nФайлов: {len(records)}")
    delete_message_later(viewer_chat_id, header.message_id, SECRET_AUTO_CLOSE_SECONDS)
    sent = 0
    for record in records:
        if _send_secret_record_media(viewer_chat_id, record):
            sent += 1
        time.sleep(0.12)
    if sent != len(records):
        send_and_auto_delete(
            viewer_chat_id,
            f"🎞️ Отправлено: {sent}/{len(records)}. Некоторые старые записи не содержат файла.",
            15,
        )


def send_secret_records(chat_id: int, target_chat_id: int, day_key: str | None = None):
    for chunk in format_secret_records(int(target_chat_id), day_key):
        bot.send_message(int(chat_id), chunk)


SECRET_EDIT_TOKEN = "EDITSECRET"


def _secret_day_records(target_chat_id: int, day_key: str) -> list[dict]:
    return [r for r in _secret_records(target_chat_id) if str(r.get("day_key")) == str(day_key)]


def _default_secret_day(target_chat_id: int) -> str:
    days = sorted({str(r.get("day_key")) for r in _secret_records(target_chat_id) if r.get("day_key")})
    return days[-1] if days else today_key()


def build_secret_day_text(target_chat_id: int, day_key: str) -> str:
    if _ensure_secret_media_numbers(target_chat_id):
        save_data(data)
    lines = [
        f"🔐 Секретные данные: {get_chat_display_name(target_chat_id)}",
        f"📅 {fmt_date_ddmmyy(day_key)}",
        "",
    ]
    records = _secret_day_records(target_chat_id, day_key)
    if not records:
        lines.append("Нет секретных сообщений.")
    for idx, item in enumerate(records, 1):
        ts = str(item.get("timestamp") or "")
        stamp = ts[11:19] if len(ts) >= 19 else ""
        lines.append(f"{idx}. {stamp} — {_secret_record_display_text(item)}".rstrip())
    text = "\n".join(lines)
    return text if len(text) <= 3900 else text[:3890] + "\n…"


SECRET_DELETE_MODES = ("day", "week", "month", "all")


def can_manage_secret_target(viewer_chat_id: int, target_chat_id: int) -> bool:
    try:
        return bool(is_owner_chat(int(viewer_chat_id)) or int(viewer_chat_id) == int(target_chat_id))
    except Exception:
        return False


def _renumber_secret_media_numbers(chat_id: int) -> None:
    number = 1
    for record in _secret_records(int(chat_id)):
        if _is_secret_media_record(record):
            record["media_number"] = number
            number += 1


def _secret_delete_period_bounds(mode: str, day_key: str):
    mode = str(mode or "day")
    try:
        base = datetime.strptime(str(day_key)[:10], "%Y-%m-%d").date()
    except Exception:
        base = now_local().date()
    if mode == "all":
        return None, None
    if mode == "week":
        start = datetime.strptime(week_start_monday(base.strftime("%Y-%m-%d")), "%Y-%m-%d").date()
        end = start + timedelta(days=6)
        return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")
    if mode == "month":
        start = base.replace(day=1)
        last_day = calendar.monthrange(base.year, base.month)[1]
        end = base.replace(day=last_day)
        return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")
    return base.strftime("%Y-%m-%d"), base.strftime("%Y-%m-%d")


def _secret_delete_period_label(mode: str, day_key: str) -> str:
    start, end = _secret_delete_period_bounds(mode, day_key)
    if mode == "all":
        return "Всё"
    if mode == "month":
        try:
            return datetime.strptime(str(day_key)[:10], "%Y-%m-%d").strftime("%m.%y")
        except Exception:
            return str(day_key)[:7]
    if start == end:
        return fmt_date_ddmmyy(start)
    return f"{fmt_date_ddmmyy(start)}–{fmt_date_ddmmyy(end)}"


def _secret_record_matches_delete_mode(record: dict, mode: str, day_key: str) -> bool:
    if mode == "all":
        return True
    rk = str((record or {}).get("day_key") or "")[:10]
    if not rk:
        return False
    start, end = _secret_delete_period_bounds(mode, day_key)
    return bool(start <= rk <= end)


def _secret_delete_count(target_chat_id: int, mode: str, day_key: str) -> int:
    return sum(1 for r in _secret_records(int(target_chat_id)) if _secret_record_matches_delete_mode(r, mode, day_key))


def _secret_delete_selection(viewer_chat_id: int, target_chat_id: int, day_key: str) -> set[str]:
    store = get_chat_store(int(viewer_chat_id))
    item = store.get("secret_delete_selection") or {}
    if int(item.get("target_chat_id") or 0) != int(target_chat_id) or str(item.get("day_key") or "") != str(day_key):
        item = {"target_chat_id": int(target_chat_id), "day_key": str(day_key), "modes": []}
        store["secret_delete_selection"] = item
        save_data(data)
    return {m for m in (item.get("modes") or []) if m in SECRET_DELETE_MODES}


def set_secret_delete_selection(viewer_chat_id: int, target_chat_id: int, day_key: str, selected: set[str]):
    store = get_chat_store(int(viewer_chat_id))
    store["secret_delete_selection"] = {
        "target_chat_id": int(target_chat_id),
        "day_key": str(day_key),
        "modes": [m for m in SECRET_DELETE_MODES if m in set(selected or set())],
    }
    save_data(data)


def toggle_secret_delete_selection(viewer_chat_id: int, target_chat_id: int, day_key: str, mode: str) -> set[str]:
    selected = _secret_delete_selection(viewer_chat_id, target_chat_id, day_key)
    if mode in selected:
        selected.discard(mode)
    elif mode in SECRET_DELETE_MODES:
        # Если выбрано "всё", остальные галочки не нужны. И наоборот.
        if mode == "all":
            selected = {"all"}
        else:
            selected.discard("all")
            selected.add(mode)
    set_secret_delete_selection(viewer_chat_id, target_chat_id, day_key, selected)
    return selected


def build_secret_delete_text(viewer_chat_id: int, target_chat_id: int, day_key: str) -> str:
    selected = _secret_delete_selection(viewer_chat_id, target_chat_id, day_key)
    lines = [
        f"🗑 Удаление секретных данных: {get_chat_display_name(target_chat_id)}",
        f"📅 Точка отсчёта: {fmt_date_ddmmyy(day_key)}",
        "",
        "Выбери период галочкой и нажми «Удалить выбранное».",
        "Удаляются текст, фото, видео, документы и другие секретные записи.",
        "",
    ]
    for mode in SECRET_DELETE_MODES:
        mark = "☑️" if mode in selected else "⬛"
        title = {"day": "День", "week": "Неделя", "month": "Месяц", "all": "Всё"}.get(mode, mode)
        lines.append(f"{mark} {title}: {_secret_delete_period_label(mode, day_key)} — {_secret_delete_count(target_chat_id, mode, day_key)}")
    return "\n".join(lines)


def build_secret_delete_keyboard(
    viewer_chat_id: int,
    target_chat_id: int,
    day_key: str,
    self_only: bool = False,
    remaining: int = SECRET_AUTO_CLOSE_SECONDS,
):
    selected = _secret_delete_selection(viewer_chat_id, target_chat_id, day_key)
    kb = types.InlineKeyboardMarkup(row_width=2)
    for mode in SECRET_DELETE_MODES:
        mark = "☑️" if mode in selected else "⬛"
        title = {"day": "🗑 День", "week": "🗑 Неделя", "month": "🗑 Месяц", "all": "🗑 Всё"}.get(mode, mode)
        count = _secret_delete_count(target_chat_id, mode, day_key)
        kb.row(IB(f"{mark} {title} ({count})", callback_data=f"secdelt:{target_chat_id}:{day_key}:{mode}"))
    kb.row(IB("🗑 Удалить выбранное", callback_data=f"secdelgo:{target_chat_id}:{day_key}"))
    kb.row(
        IB("🔙 Назад", callback_data=f"secchatcal:{target_chat_id}:{day_key[:7]}"),
        IB(_secret_close_label(remaining), callback_data="secclose"),
    )
    return kb


def _delete_secret_mega_media_paths(paths: list[str]):
    if not paths or not mega_is_configured():
        return
    for remote_path in sorted(set(str(p) for p in paths if p)):
        try:
            _mega_run("mega-rm", [remote_path], check=False, timeout=30)
        except Exception as e:
            log_error(f"delete secret mega media {remote_path}: {e}")


def delete_secret_records_by_modes(target_chat_id: int, modes: set[str], day_key: str) -> int:
    target_chat_id = int(target_chat_id)
    modes = {m for m in (modes or set()) if m in SECRET_DELETE_MODES}
    if not modes:
        return 0
    records = _secret_records(target_chat_id)
    kept = []
    deleted = []
    for record in records:
        if any(_secret_record_matches_delete_mode(record, mode, day_key) for mode in modes):
            deleted.append(record)
        else:
            kept.append(record)
    if not deleted:
        return 0
    media_paths = [str(r.get("mega_media_path") or "") for r in deleted if r.get("mega_media_path")]
    records[:] = kept
    _renumber_secret_media_numbers(target_chat_id)
    save_data(data)
    schedule_config_backup_for_chats(target_chat_id, delay=0.2)
    if media_paths:
        threading.Thread(target=_delete_secret_mega_media_paths, args=(media_paths,), daemon=True).start()
    threading.Thread(target=upload_chat_secrets_to_mega, args=(target_chat_id,), daemon=True).start()
    refresh_secret_windows(target_chat_id)
    return len(deleted)


def delete_secret_records_by_ids(target_chat_id: int, record_ids: set[int]) -> int:
    target_chat_id = int(target_chat_id)
    record_ids = {int(x) for x in (record_ids or set())}
    if not record_ids:
        return 0
    records = _secret_records(target_chat_id)
    deleted = [record for record in records if int(record.get("id") or 0) in record_ids]
    if not deleted:
        return 0
    records[:] = [record for record in records if int(record.get("id") or 0) not in record_ids]
    media_paths = [str(record.get("mega_media_path") or "") for record in deleted if record.get("mega_media_path")]
    _renumber_secret_media_numbers(target_chat_id)
    save_data(data)
    schedule_config_backup_for_chats(target_chat_id, delay=0.2)
    if media_paths:
        threading.Thread(target=_delete_secret_mega_media_paths, args=(media_paths,), daemon=True).start()
    threading.Thread(target=upload_chat_secrets_to_mega, args=(target_chat_id,), daemon=True).start()
    refresh_secret_windows(target_chat_id)
    return len(deleted)


def build_secret_day_keyboard(
    target_chat_id: int,
    day_key: str,
    self_only: bool = False,
    remaining: int = SECRET_AUTO_CLOSE_SECONDS,
):
    base = datetime.strptime(day_key, "%Y-%m-%d")
    prev_day = (base - timedelta(days=1)).strftime("%Y-%m-%d")
    next_day = (base + timedelta(days=1)).strftime("%Y-%m-%d")
    kb = types.InlineKeyboardMarkup(row_width=3)
    kb.row(
        IB("⬅️ День", callback_data=f"secview:{target_chat_id}:{prev_day}"),
        IB("📅 Сегодня", callback_data=f"secview:{target_chat_id}:{today_key()}"),
        IB("День ➡️", callback_data=f"secview:{target_chat_id}:{next_day}"),
    )
    kb.row(
        IB("📅 Календарь", callback_data=f"secchatcal:{target_chat_id}:{day_key[:7]}"),
        IB("🎞️", callback_data=f"secmedia:{target_chat_id}:{day_key}"),
        IB("✏️ Изменить", callback_data=f"secedit:{target_chat_id}:{day_key}"),
    )
    if self_only:
        kb.row(IB(_secret_close_label(remaining), callback_data="secclose"))
    else:
        kb.row(
            IB("🔙 Назад", callback_data="secbacklist"),
            IB(_secret_close_label(remaining), callback_data="secclose"),
        )
    return kb


def register_secret_window(
    viewer_chat_id: int,
    message_id: int,
    target_chat_id: int,
    kind: str,
    day_key: str | None = None,
    month_key: str | None = None,
    self_only: bool = False,
):
    store = get_chat_store(int(viewer_chat_id))
    store["secret_active_window"] = {
        "message_id": int(message_id),
        "target_chat_id": int(target_chat_id),
        "kind": str(kind),
        "day_key": day_key,
        "month_key": month_key,
        "self_only": bool(self_only),
    }
    store["secret_last_target_chat_id"] = int(target_chat_id)
    store["secret_last_self_only"] = bool(self_only)
    save_data(data)


def secret_window_self_only(viewer_chat_id: int, message_id: int | None = None) -> bool:
    active = get_chat_store(int(viewer_chat_id)).get("secret_active_window") or {}
    if message_id is not None and int(active.get("message_id") or 0) != int(message_id):
        return False
    return bool(active.get("self_only", False))


def clear_secret_window(viewer_chat_id: int, message_id: int | None = None):
    store = get_chat_store(int(viewer_chat_id))
    active = store.get("secret_active_window") or {}
    if message_id is None or int(active.get("message_id") or 0) == int(message_id):
        try:
            if message_id is not None:
                _cancel_secret_calendar_timer(int(viewer_chat_id), int(message_id))
        except Exception:
            pass
        store["secret_active_window"] = None
        save_data(data)


def register_secret_list_window(viewer_chat_id: int, message_id: int):
    store = get_chat_store(int(viewer_chat_id))
    target_chat_id = int(store.get("secret_last_target_chat_id") or viewer_chat_id)
    register_secret_window(
        viewer_chat_id,
        message_id,
        target_chat_id,
        "list",
        self_only=False,
    )
    schedule_secret_calendar_close(viewer_chat_id, message_id)


def refresh_secret_windows(target_chat_id: int):
    target_chat_id = int(target_chat_id)
    for viewer_s, viewer_store in list((data.get("chats", {}) or {}).items()):
        active = (viewer_store or {}).get("secret_active_window") or {}
        if int(active.get("target_chat_id") or 0) != target_chat_id:
            continue
        try:
            viewer_id = int(viewer_s)
            message_id = int(active.get("message_id") or 0)
            kind = active.get("kind")
            self_only = bool(active.get("self_only", False))
            updated = False
            if not message_id:
                continue
            if kind == "day":
                day_key = active.get("day_key") or _default_secret_day(target_chat_id)
                bot.edit_message_text(
                    build_secret_day_text(target_chat_id, day_key),
                    chat_id=viewer_id,
                    message_id=message_id,
                    reply_markup=build_secret_day_keyboard(target_chat_id, day_key, self_only=self_only),
                )
                updated = True
            elif kind == "edit":
                day_key = active.get("day_key") or _default_secret_day(target_chat_id)
                bot.edit_message_text(
                    build_secret_edit_text(target_chat_id, day_key),
                    chat_id=viewer_id,
                    message_id=message_id,
                    reply_markup=build_secret_edit_keyboard(viewer_id, target_chat_id, day_key, self_only=self_only),
                )
                updated = True
            elif kind == "delete":
                day_key = active.get("day_key") or _default_secret_day(target_chat_id)
                bot.edit_message_text(
                    build_secret_delete_text(viewer_id, target_chat_id, day_key),
                    chat_id=viewer_id,
                    message_id=message_id,
                    reply_markup=build_secret_delete_keyboard(viewer_id, target_chat_id, day_key, self_only=self_only),
                )
                updated = True
            elif kind == "calendar":
                month_key = active.get("month_key") or now_local().strftime("%Y-%m")
                bot.edit_message_text(
                    f"🔐 Секретные сообщения\n{get_chat_display_name(target_chat_id)}\n📅 {month_key}",
                    chat_id=viewer_id,
                    message_id=message_id,
                    reply_markup=build_secret_calendar_keyboard(target_chat_id, month_key, self_only=self_only),
                )
                updated = True
            elif kind == "month_list":
                month_key = active.get("month_key") or now_local().strftime("%Y-%m")
                bot.edit_message_text(
                    build_secret_month_summary_text(target_chat_id, month_key),
                    chat_id=viewer_id,
                    message_id=message_id,
                    reply_markup=build_secret_month_summary_keyboard(target_chat_id, month_key, self_only=self_only),
                )
                updated = True
            if updated:
                schedule_secret_calendar_close(viewer_id, message_id)
        except Exception as e:
            if "message is not modified" not in str(e).lower():
                log_error(f"refresh_secret_windows({target_chat_id}): {e}")


def open_secret_day_window(
    chat_id: int,
    target_chat_id: int,
    day_key: str | None = None,
    message_id: int | None = None,
    self_only: bool = False,
):
    day_key = day_key or _default_secret_day(target_chat_id)
    text = build_secret_day_text(target_chat_id, day_key)
    kb = build_secret_day_keyboard(target_chat_id, day_key, self_only=self_only)
    if message_id:
        bot.edit_message_text(text, chat_id=chat_id, message_id=message_id, reply_markup=kb)
        register_secret_window(chat_id, message_id, target_chat_id, "day", day_key=day_key, self_only=self_only)
        schedule_secret_calendar_close(chat_id, message_id)
        return message_id
    sent = bot.send_message(chat_id, text, reply_markup=kb)
    register_secret_window(chat_id, sent.message_id, target_chat_id, "day", day_key=day_key, self_only=self_only)
    schedule_secret_calendar_close(chat_id, sent.message_id)
    return sent.message_id


def compose_secret_edit_insert(target_chat_id: int, record: dict) -> str:
    meta = f"{SECRET_EDIT_TOKEN}|{int(target_chat_id)}|{int(record.get('id') or 0)}|"
    return f"({meta} служебное — можно не трогать)\n\n{record.get('text', '')}"


def _secret_edit_delete_selection(viewer_chat_id: int, target_chat_id: int, day_key: str) -> set[int]:
    store = get_chat_store(int(viewer_chat_id))
    item = store.get("secret_edit_delete_selection") or {}
    if int(item.get("target_chat_id") or 0) != int(target_chat_id) or str(item.get("day_key") or "") != str(day_key):
        item = {"target_chat_id": int(target_chat_id), "day_key": str(day_key), "ids": []}
        store["secret_edit_delete_selection"] = item
        save_data(data)
    return {int(x) for x in (item.get("ids") or []) if str(x).lstrip("-").isdigit()}


def set_secret_edit_delete_selection(viewer_chat_id: int, target_chat_id: int, day_key: str, selected: set[int]):
    store = get_chat_store(int(viewer_chat_id))
    store["secret_edit_delete_selection"] = {
        "target_chat_id": int(target_chat_id),
        "day_key": str(day_key),
        "ids": sorted(int(x) for x in (selected or set())),
    }
    save_data(data)


def toggle_secret_edit_delete_selection(viewer_chat_id: int, target_chat_id: int, day_key: str, record_id: int) -> set[int]:
    selected = _secret_edit_delete_selection(viewer_chat_id, target_chat_id, day_key)
    record_id = int(record_id)
    if record_id in selected:
        selected.remove(record_id)
    else:
        selected.add(record_id)
    set_secret_edit_delete_selection(viewer_chat_id, target_chat_id, day_key, selected)
    return selected


def build_secret_edit_text(target_chat_id: int, day_key: str) -> str:
    lines = [
        "✏️ Изменить секретные данные",
        get_chat_display_name(target_chat_id),
        f"📅 {fmt_date_ddmmyy(day_key)}",
        "",
    ]
    records = _secret_day_records(target_chat_id, day_key)
    if not records:
        lines.append("Нет данных для изменения.")
    for idx, item in enumerate(records, 1):
        ts = str(item.get("timestamp") or "")
        stamp = ts[11:19] if len(ts) >= 19 else ""
        body = re.sub(r"\s+", " ", _secret_record_display_text(item)).strip()
        if len(body) > 68:
            body = body[:68].rstrip()
        lines.append(f"{idx}. {stamp} — {body}...")
    text = "\n".join(lines)
    return text if len(text) <= 3900 else text[:3890] + "\n…"


def build_secret_edit_keyboard(
    viewer_chat_id: int,
    target_chat_id: int,
    day_key: str,
    self_only: bool = False,
    remaining: int = SECRET_AUTO_CLOSE_SECONDS,
):
    kb = types.InlineKeyboardMarkup(row_width=2)
    selected = _secret_edit_delete_selection(viewer_chat_id, target_chat_id, day_key)
    for idx, item in enumerate(_secret_day_records(target_chat_id, day_key), 1):
        ts = str(item.get("timestamp") or "")
        stamp = ts[11:19] if len(ts) >= 19 else ""
        record_id = int(item.get("id") or 0)
        label = f"{idx}. {fmt_date_ddmmyy(day_key)} {stamp} ✏️"
        delete_label = "☑️ Удалить" if record_id in selected else "⬛ Удалить"
        kb.row(
            IB(label, switch_inline_query_current_chat=compose_secret_edit_insert(target_chat_id, item)[:256]),
            IB(delete_label, callback_data=f"secedtoggle:{target_chat_id}:{day_key}:{record_id}"),
        )
    if not _secret_day_records(target_chat_id, day_key):
        kb.row(IB("Нет данных для изменения", callback_data="none"))
    if selected:
        kb.row(IB("🗑 Удалить выбранное", callback_data=f"secedselected:{target_chat_id}:{day_key}"))
    kb.row(IB("🔙 Назад", callback_data=f"secview:{target_chat_id}:{day_key}"))
    kb.row(IB(_secret_close_label(remaining), callback_data="secclose"))
    return kb


def handle_secret_edit_insert_message(msg) -> bool:
    if getattr(msg, "content_type", None) != "text":
        return False
    text = (msg.text or "").strip()
    if SECRET_EDIT_TOKEN + "|" not in text:
        return False
    try:
        # Удаляем служебное сообщение редактирования сразу: даже если текст пустой
        # или запись не найдена, хвост вида EDITSECRET|... не должен висеть в чате.
        delete_secret_source_message(msg)
    except Exception:
        pass
    try:
        match = re.search(r"\((%s\|[^)]*)\)" % re.escape(SECRET_EDIT_TOKEN), text)
        if not match:
            return False
        parts = match.group(1).split("|", 3)
        target_chat_id = int(parts[1])
        record_id = int(parts[2])
        if not is_owner_chat(msg.chat.id) and int(msg.chat.id) != target_chat_id:
            return True
        new_text = sanitize_telegram_inserted_text((text[:match.start()] + " " + text[match.end():]).strip())
        target = next((r for r in _secret_records(target_chat_id) if int(r.get("id") or 0) == record_id), None)
        if not target or not new_text:
            send_and_auto_delete(msg.chat.id, "❌ Секретная запись не найдена или текст пуст.", 8)
            return True
        target["text"] = new_text
        target["edited_at"] = now_local().isoformat(timespec="seconds")
        save_data(data)
        schedule_config_backup_for_chats(target_chat_id, delay=0.2)
        threading.Thread(target=upload_chat_secrets_to_mega, args=(target_chat_id,), daemon=True).start()
        refresh_secret_windows(target_chat_id)
        send_and_auto_delete(msg.chat.id, "✅ Секретные данные изменены.", 8)
        return True
    except Exception as e:
        log_error(f"handle_secret_edit_insert_message: {e}")
        return True


def build_secret_chat_list_keyboard(remaining: int = SECRET_AUTO_CLOSE_SECONDS):
    kb = types.InlineKeyboardMarkup(row_width=3)
    chats = collect_all_known_chat_ids(include_owner=True)
    for cid in chats:
        mode = "✅" if is_total_secret_mode(cid) else "❌"
        kb.row(
            IB(get_chat_display_name(cid)[:28], callback_data=f"seclist:{cid}"),
            IB(f"{mode} Секрет", callback_data=f"sectoggle:{cid}"),
            IB("📅", callback_data=f"secchatcal:{cid}"),
        )
    if not chats:
        kb.row(IB("Нет чатов с секретами", callback_data="none"))
    kb.row(
        IB("🔙 Назад осн. окно", callback_data=f"d:{today_key()}:back_main"),
        IB(_secret_close_label(remaining), callback_data="secclose"),
    )
    return kb


def _cancel_secret_calendar_timer(chat_id: int, message_id: int):
    key = (int(chat_id), int(message_id))
    with _secret_calendar_lock:
        token = _secret_calendar_timers.pop(key, None)
        if isinstance(token, dict):
            token["cancelled"] = True


def _build_secret_active_keyboard(viewer_chat_id: int, active: dict, remaining: int):
    target_chat_id = int(active.get("target_chat_id") or viewer_chat_id)
    kind = str(active.get("kind") or "")
    self_only = bool(active.get("self_only", False))
    if kind == "list":
        return build_secret_chat_list_keyboard(remaining=remaining)
    if kind == "day":
        day_key = active.get("day_key") or _default_secret_day(target_chat_id)
        return build_secret_day_keyboard(target_chat_id, day_key, self_only=self_only, remaining=remaining)
    if kind == "edit":
        day_key = active.get("day_key") or _default_secret_day(target_chat_id)
        return build_secret_edit_keyboard(
            viewer_chat_id, target_chat_id, day_key,
            self_only=self_only, remaining=remaining,
        )
    if kind == "delete":
        day_key = active.get("day_key") or _default_secret_day(target_chat_id)
        return build_secret_delete_keyboard(
            viewer_chat_id, target_chat_id, day_key,
            self_only=self_only, remaining=remaining,
        )
    if kind == "calendar":
        month_key = active.get("month_key") or now_local().strftime("%Y-%m")
        return build_secret_calendar_keyboard(
            target_chat_id, month_key,
            self_only=self_only, remaining=remaining,
        )
    if kind == "month_list":
        month_key = active.get("month_key") or now_local().strftime("%Y-%m")
        return build_secret_month_summary_keyboard(
            target_chat_id, month_key,
            self_only=self_only, remaining=remaining,
        )
    return None


def _update_secret_window_countdown(chat_id: int, message_id: int, remaining: int) -> bool:
    active = get_chat_store(int(chat_id)).get("secret_active_window") or {}
    if int(active.get("message_id") or 0) != int(message_id):
        return False
    kb = _build_secret_active_keyboard(chat_id, active, remaining)
    if kb is None:
        return False
    try:
        bot.edit_message_reply_markup(
            chat_id=chat_id,
            message_id=message_id,
            reply_markup=kb,
        )
        return True
    except Exception as e:
        if "message is not modified" not in str(e).lower():
            log_error(f"secret window countdown {chat_id}:{message_id}: {e}")
        return True


def schedule_secret_calendar_close(chat_id: int, message_id: int):
    _cancel_secret_calendar_timer(chat_id, message_id)
    key = (int(chat_id), int(message_id))
    token = {"cancelled": False, "generation": time.time_ns()}
    with _secret_calendar_lock:
        _secret_calendar_timers[key] = token

    try:
        _update_secret_window_countdown(chat_id, message_id, SECRET_AUTO_CLOSE_SECONDS)
    except Exception:
        pass

    def close():
        remaining = int(SECRET_AUTO_CLOSE_SECONDS)
        while remaining > 0:
            time.sleep(SECRET_COUNTDOWN_STEP_SECONDS)
            with _secret_calendar_lock:
                if _secret_calendar_timers.get(key) is not token or token.get("cancelled"):
                    return
            remaining = max(0, remaining - SECRET_COUNTDOWN_STEP_SECONDS)
            if remaining > 0:
                if not _update_secret_window_countdown(chat_id, message_id, remaining):
                    return
        try:
            bot.delete_message(chat_id, message_id)
        except Exception:
            pass
        clear_secret_window(chat_id, message_id)
        with _secret_calendar_lock:
            if _secret_calendar_timers.get(key) is token:
                _secret_calendar_timers.pop(key, None)
    threading.Thread(target=close, daemon=True).start()


def _secret_month_records(target_chat_id: int, month_key: str) -> list[dict]:
    prefix = str(month_key or now_local().strftime("%Y-%m"))[:7] + "-"
    return [r for r in _secret_records(int(target_chat_id)) if str(r.get("day_key") or "").startswith(prefix)]


def build_secret_month_summary_text(target_chat_id: int, month_key: str) -> str:
    if _ensure_secret_media_numbers(target_chat_id):
        save_data(data)
    records = _secret_month_records(target_chat_id, month_key)
    lines = [
        f"🪬 Секреты за месяц: {get_chat_display_name(target_chat_id)}",
        f"📅 {month_key}",
        "",
    ]
    if not records:
        lines.append("Нет секретных сообщений за этот месяц.")
    for idx, item in enumerate(records, 1):
        day = fmt_date_ddmmyy(str(item.get("day_key") or ""))
        ts = str(item.get("timestamp") or "")
        stamp = ts[11:16] if len(ts) >= 16 else ""
        body = re.sub(r"\s+", " ", _secret_record_display_text(item)).strip()
        if len(body) > 74:
            body = body[:74].rstrip()
        lines.append(f"{idx}. {day} {stamp} — {body}...")
    text = "\n".join(lines)
    return text if len(text) <= 3900 else text[:3890] + "\n…"


def build_secret_month_summary_keyboard(
    target_chat_id: int,
    month_key: str,
    self_only: bool = False,
    remaining: int = SECRET_AUTO_CLOSE_SECONDS,
):
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.row(
        IB("📅 Календарь", callback_data=f"secchatcal:{target_chat_id}:{month_key}"),
        IB("🗑 Удалить секреты", callback_data=f"secdel:{target_chat_id}:{month_key}-01"),
    )
    if self_only:
        kb.row(IB(_secret_close_label(remaining), callback_data="secclose"))
    else:
        kb.row(
            IB("🔙 Назад", callback_data="secbacklist"),
            IB(_secret_close_label(remaining), callback_data="secclose"),
        )
    return kb


def open_secret_month_summary(
    chat_id: int,
    target_chat_id: int,
    month_key: str | None = None,
    message_id: int | None = None,
    self_only: bool = False,
):
    month_key = month_key or now_local().strftime("%Y-%m")
    text = build_secret_month_summary_text(target_chat_id, month_key)
    kb = build_secret_month_summary_keyboard(target_chat_id, month_key, self_only=self_only)
    if message_id:
        bot.edit_message_text(text, chat_id=chat_id, message_id=message_id, reply_markup=kb)
        register_secret_window(
            chat_id, message_id, target_chat_id, "month_list",
            month_key=month_key, self_only=self_only,
        )
        schedule_secret_calendar_close(chat_id, message_id)
        return message_id
    sent = bot.send_message(chat_id, text, reply_markup=kb)
    register_secret_window(
        chat_id, sent.message_id, target_chat_id, "month_list",
        month_key=month_key, self_only=self_only,
    )
    schedule_secret_calendar_close(chat_id, sent.message_id)
    return sent.message_id




def touch_secret_window_timer_for_callback(chat_id: int, message_id: int, data_str: str | None = None) -> bool:
    """Продлевает автозакрытие любого активного секретного окна при любом нажатии."""
    try:
        active = get_chat_store(int(chat_id)).get("secret_active_window") or {}
        if int(active.get("message_id") or 0) == int(message_id):
            schedule_secret_calendar_close(int(chat_id), int(message_id))
            return True
    except Exception as e:
        log_error(f"touch_secret_window_timer_for_callback({chat_id},{message_id},{data_str}): {e}")
    return False


def build_secret_calendar_keyboard(
    target_chat_id: int,
    month_key: str,
    self_only: bool = False,
    remaining: int = SECRET_AUTO_CLOSE_SECONDS,
):
    year, month = (int(x) for x in month_key.split("-", 1))
    marked = {str(r.get("day_key")) for r in _secret_records(target_chat_id)}
    kb = types.InlineKeyboardMarkup(row_width=7)
    kb.row(*[IB(x, callback_data="none") for x in ("Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс")])
    for week in calendar.Calendar(firstweekday=0).monthdayscalendar(year, month):
        row = []
        for day in week:
            if not day:
                row.append(IB(" ", callback_data="none"))
                continue
            day_key = f"{year:04d}-{month:02d}-{day:02d}"
            label = f"🔐{day}" if day_key in marked else str(day)
            row.append(IB(label, callback_data=f"secday:{target_chat_id}:{day_key}" if day_key in marked else "none"))
        kb.row(*row)
    first = datetime(year, month, 1)
    prev = (first - timedelta(days=1)).strftime("%Y-%m")
    nxt = (first.replace(day=28) + timedelta(days=4)).replace(day=1).strftime("%Y-%m")
    kb.row(
        IB("⬅️ Месяц", callback_data=f"secmon:{target_chat_id}:{prev}"),
        IB("📅 Сегодня", callback_data=f"secview:{target_chat_id}:{today_key()}"),
        IB("Месяц ➡️", callback_data=f"secmon:{target_chat_id}:{nxt}"),
    )
    anchor_day = today_key() if month_key == now_local().strftime("%Y-%m") else f"{month_key}-01"
    kb.row(
        IB("🪬", callback_data=f"secmonthlist:{target_chat_id}:{month_key}"),
        IB("🗑 Удалить секреты", callback_data=f"secdel:{target_chat_id}:{anchor_day}"),
    )
    if self_only:
        kb.row(IB(_secret_close_label(remaining), callback_data="secclose"))
    else:
        kb.row(
            IB("🔙 Назад", callback_data="secbacklist"),
            IB(_secret_close_label(remaining), callback_data="secclose"),
        )
    return kb


def open_secret_calendar(
    chat_id: int,
    target_chat_id: int,
    month_key: str | None = None,
    message_id: int | None = None,
    self_only: bool = False,
):
    month_key = month_key or now_local().strftime("%Y-%m")
    text = f"🔐 Секретные сообщения\n{get_chat_display_name(target_chat_id)}\n📅 {month_key}"
    kb = build_secret_calendar_keyboard(target_chat_id, month_key, self_only=self_only)
    if message_id:
        bot.edit_message_text(text, chat_id=chat_id, message_id=message_id, reply_markup=kb)
        register_secret_window(
            chat_id, message_id, target_chat_id, "calendar",
            month_key=month_key, self_only=self_only,
        )
        schedule_secret_calendar_close(chat_id, message_id)
        return message_id
    sent = bot.send_message(chat_id, text, reply_markup=kb)
    register_secret_window(
        chat_id, sent.message_id, target_chat_id, "calendar",
        month_key=month_key, self_only=self_only,
    )
    schedule_secret_calendar_close(chat_id, sent.message_id)
    return sent.message_id


def handle_secret_sequence(msg) -> bool:
    text = (getattr(msg, "text", None) or "").strip()
    if text not in {"11", "22", "33"}:
        return False
    user_id = int(getattr(getattr(msg, "from_user", None), "id", 0) or 0)
    key = (int(msg.chat.id), user_id)
    now_ts = time.time()
    item = _secret_sequence_state.get(key, {"step": 0, "ts": 0.0, "message_ids": []})
    if now_ts - float(item.get("ts", 0)) > 10:
        item = {"step": 0, "ts": 0.0, "message_ids": []}
    expected = ("11", "22", "33")[int(item.get("step", 0))]
    if text != expected:
        _secret_sequence_state.pop(key, None)
        return False
    step = int(item.get("step", 0)) + 1
    message_ids = list(item.get("message_ids") or []) + [int(msg.message_id)]
    if step == 3:
        _secret_sequence_state.pop(key, None)
        open_secret_calendar(msg.chat.id, msg.chat.id, self_only=True)
        for message_id in message_ids:
            try:
                bot.delete_message(msg.chat.id, message_id)
            except Exception as e:
                log_error(f"secret sequence delete {msg.chat.id}:{message_id}: {e}")
                delete_message_later(msg.chat.id, message_id, 1)
        return True
    _secret_sequence_state[key] = {"step": step, "ts": now_ts, "message_ids": message_ids}
    return True


def build_additional_owners_keyboard():
    kb = types.InlineKeyboardMarkup(row_width=2)
    owners = get_additional_owner_ids()
    buttons = []
    for cid in collect_all_known_chat_ids(include_owner=False):
        if is_primary_owner(cid):
            continue
        icon = "✅" if int(cid) in owners else "❌"
        buttons.append(IB(f"{icon} {get_chat_display_name(cid)[:32]}", callback_data=f"addown:{cid}"))
    for i in range(0, len(buttons), 2):
        kb.row(*buttons[i:i + 2])
    if not buttons:
        kb.row(IB("Нет доступных чатов", callback_data="none"))
    kb.row(IB("🔙 Назад в Инфо", callback_data="journal_back"))
    return kb


@bot.message_handler(func=lambda m: bool(getattr(m, "text", None) and OWNER_ACTIVATION_RE.fullmatch(m.text.strip())))
def cmd_hidden_owner_activation(msg):
    schedule_command_delete(msg)
    user_id = int(getattr(getattr(msg, "from_user", None), "id", 0) or 0)
    if user_id:
        set_additional_owner(user_id, True)
        send_and_auto_delete(msg.chat.id, "✅ Доступ владельца активирован.", 8)


@bot.message_handler(func=lambda m: bool(getattr(m, "text", None) and SECRET_ACCESS_RE.fullmatch(m.text.strip())))
def cmd_secret_access(msg):
    schedule_command_delete(msg)
    if getattr(msg.chat, "type", "") != "private":
        send_and_auto_delete(msg.chat.id, "🔐 Список секретов доступен только в личке с ботом.", 8)
        return
    sent = bot.send_message(
        msg.chat.id,
        "🔐 Выберите чат с секретными данными:",
        reply_markup=build_secret_chat_list_keyboard(),
    )
    register_secret_list_window(msg.chat.id, sent.message_id)


@bot.message_handler(commands=["secret_bot"])
def cmd_total_secret(msg):
    try:
        bot.delete_message(msg.chat.id, msg.message_id)
    except Exception as e:
        log_error(f"secret_bot immediate delete {msg.chat.id}:{msg.message_id}: {e}")
    set_total_secret_mode(msg.chat.id, True)
    send_and_auto_delete(msg.chat.id, "🔐 Тотальный секрет включён. Все следующие сообщения сохраняются как секретные.", 10)


def _secret_media_command_target(viewer_chat_id: int) -> int:
    store = get_chat_store(int(viewer_chat_id))
    active = store.get("secret_active_window") or {}
    target = active.get("target_chat_id") or store.get("secret_last_target_chat_id") or viewer_chat_id
    try:
        target = int(target)
    except Exception:
        target = int(viewer_chat_id)
    if not is_owner_chat(viewer_chat_id) and target != int(viewer_chat_id):
        target = int(viewer_chat_id)
    return target


@bot.message_handler(func=lambda m: bool(
    getattr(m, "text", None)
    and re.fullmatch(r"/\d+(?:@\w+)?", m.text.strip())
))
def cmd_secret_media_number(msg):
    try:
        number = int(msg.text.strip().split("@", 1)[0][1:])
    except Exception:
        return
    try:
        bot.delete_message(msg.chat.id, msg.message_id)
    except Exception:
        pass
    target_chat_id = _secret_media_command_target(msg.chat.id)
    record = _secret_media_record_by_number(target_chat_id, number)
    if not record:
        send_and_auto_delete(
            msg.chat.id,
            f"❌ Медиа /{number} не найдено в чате {get_chat_display_name(target_chat_id)}.",
            10,
        )
        return
    get_chat_store(msg.chat.id)["secret_last_target_chat_id"] = int(target_chat_id)
    save_data(data)
    if not _send_secret_record_media(msg.chat.id, record):
        send_and_auto_delete(msg.chat.id, f"❌ Не удалось открыть медиа /{number}.", 10)


@bot.message_handler(func=lambda m: bool(getattr(m, "text", None) and re.fullmatch(r"/старт(?:@\w+)?", m.text.strip(), re.I)))
def cmd_start_ru(msg):
    set_total_secret_mode(msg.chat.id, False)
    cmd_start(msg)


@bot.message_handler(func=lambda m: bool(getattr(m, "text", None) and re.fullmatch(r"/(?:buttons|knopki|кнопки)(?:@\w+)?", m.text.strip(), re.I)))
def cmd_toggle_icon_buttons(msg):
    schedule_command_delete(msg)
    if not is_owner_chat(msg.chat.id):
        send_and_auto_delete(msg.chat.id, "Эта команда только для владельца.", 8)
        return
    new_state = toggle_icon_button_mode()
    send_and_auto_delete(msg.chat.id, "🔣 Кнопки: значки" if new_state else "🔤 Кнопки: текст", 10)
    try:
        open_info_window(msg.chat.id)
    except Exception:
        pass


@bot.message_handler(func=lambda m: bool(getattr(m, "text", None) and re.fullmatch(r"/(?:mask|maska|маска)(?:@\w+)?", m.text.strip(), re.I)))
def cmd_toggle_total_secret_mask(msg):
    schedule_command_delete(msg)
    if not is_owner_chat(msg.chat.id):
        send_and_auto_delete(msg.chat.id, "Эта команда только для владельца.", 8)
        return
    new_state = toggle_total_secret_mask()
    send_and_auto_delete(msg.chat.id, "🪷 Маскировка тотального секрета ВКЛ" if new_state else "🪷 Маскировка тотального секрета ВЫКЛ", 10)
    try:
        open_info_window(msg.chat.id)
    except Exception:
        pass


@bot.message_handler(func=lambda m: bool(
    getattr(m, "text", None)
    and m.text.startswith("/")
    and is_total_secret_mode(m.chat.id)
    and m.text.split()[0].split("@")[0].casefold() not in {"/ok", "/start", "/старт", "/secret_bot", "/кнопки", "/buttons", "/knopki", "/маска", "/mask", "/maska", "/windows", "/okna", "/owners", "/additional_owners", "/доп_владельцы", "/tabl_lsx"}
))
def cmd_total_secret_capture(msg):
    save_secret_message(msg.chat.id, msg)
    delete_secret_source_message(msg)
    maybe_send_total_secret_decoy(msg)


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

    if is_owner_chat(chat_id):
        finance_active_chats.add(chat_id)

    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass

    try:
        bot_journal("message_received", chat_id, describe_msg_for_log(msg))
    except Exception:
        pass

    if handle_secret_sequence(msg):
        return

    if handle_secret_edit_insert_message(msg):
        return

    if handle_secret_input_message(msg):
        return

    try:
        if not getattr(getattr(msg, "from_user", None), "is_bot", False):
            bump_quick_balance_recreate_counter(chat_id)
    except Exception:
        pass

    if msg.content_type == "text":
        try:
            if handle_secret_note_message(msg):
                return
            if handle_direct_edit_insert_message(msg):
                return
            if handle_category_edit_message(msg):
                return
            if handle_category_add_message(msg):
                return
        except Exception as e:
            log_error(f"secret/category_add/edit/direct-edit message handler error: {e}")

    if msg.content_type == "text":
        try:
            store = get_chat_store(chat_id)
            if store.get("reset_wait"):
                text_up = (msg.text or "").strip().upper()
                if text_up == "ДА":
                    store["reset_wait"] = False
                    store["reset_time"] = 0
                    save_data(data)
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
            finwin_reset_wait = store.get("finwin_reset_wait")
            if finwin_reset_wait and finwin_reset_wait.get("type") == "finwin_reset":
                text_up = (msg.text or "").strip().upper()
                target_chat_id = int(finwin_reset_wait.get("target_chat_id"))
                fin_window_msg_id = finwin_reset_wait.get("fin_window_msg_id")
                owner_day_key = finwin_reset_wait.get("owner_day_key") or today_key()
                if text_up == "ДА":
                    store["finwin_reset_wait"] = None
                    save_data(data)
                    cleanup_forward_links(target_chat_id)
                    reset_chat_data(target_chat_id)
                    send_and_auto_delete(chat_id, f"✅ Данные чата {get_chat_display_name(target_chat_id)} обнулены.", 10)
                    try:
                        bot.delete_message(chat_id, msg.message_id)
                    except Exception:
                        pass
                    if fin_window_msg_id:
                        try:
                            safe_txt = render_fin_window_text(target_chat_id, today_key())
                            bot.edit_message_text(
                                safe_txt,
                                chat_id=chat_id,
                                message_id=int(fin_window_msg_id),
                                reply_markup=build_fin_window_view_keyboard(target_chat_id, today_key(), owner_day_key),
                                parse_mode="HTML"
                            )
                        except Exception as e:
                            log_error(f"finwin reset refresh failed: {e}")
                    return
                elif text_up in {"НЕТ", "ОТМЕНА", "CANCEL"}:
                    store["finwin_reset_wait"] = None
                    save_data(data)
                    send_and_auto_delete(chat_id, "❎ Обнуление отменено.", 8)
                    try:
                        bot.delete_message(chat_id, msg.message_id)
                    except Exception:
                        pass
                    return
        except Exception as e:
            log_error(f"finwin_reset_wait handler error: {e}")

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
            finwin_wait = store.get("finwin_edit_wait")
            if finwin_wait and finwin_wait.get("type") == "finwin_edit":
                text = sanitize_telegram_inserted_text((msg.text or "").strip())
                try:
                    amount, note = split_amount_and_note(text)
                except Exception:
                    send_and_auto_delete(chat_id, "❌ Неверный формат. Пример: 1500 продукты", 10)
                    return

                target_chat_id = int(finwin_wait.get("target_chat_id"))
                rid = int(finwin_wait.get("rid"))
                day_key = finwin_wait.get("day_key") or today_key()
                owner_day_key = finwin_wait.get("owner_day_key") or today_key()
                fin_window_msg_id = finwin_wait.get("fin_window_msg_id")

                with locked_chat(target_chat_id):
                    ok = update_record_in_chat(target_chat_id, rid, amount, note)

                clear_finwin_edit_wait_state(chat_id, delete_prompt=True)
                try:
                    bot.delete_message(chat_id, msg.message_id)
                except Exception:
                    pass

                if not ok:
                    send_and_auto_delete(chat_id, "❌ Запись для редактирования не найдена.", 10)
                    return

                if fin_window_msg_id:
                    try:
                        bot.edit_message_text(
                            render_fin_window_text(target_chat_id, day_key),
                            chat_id=chat_id,
                            message_id=int(fin_window_msg_id),
                            reply_markup=build_edit_records_keyboard(day_key, target_chat_id, prefix="fv", owner_day_key=owner_day_key),
                            parse_mode="HTML"
                        )
                    except Exception as e:
                        log_error(f"finwin edit refresh failed: {e}")

                schedule_finalize(target_chat_id, day_key, delay=0.1)
                send_and_auto_delete(chat_id, f"✅ Запись обновлена: {fmt_num(amount)} {note}", 8)
                return
        except Exception as e:
            log_error(f"finwin_edit_wait handler error: {e}")
    if msg.content_type == "text":
        try:
            store = get_chat_store(chat_id)
            edit_wait = store.get("edit_wait")

            if edit_wait and edit_wait.get("type") == "edit":
                text = sanitize_telegram_inserted_text((msg.text or "").strip())
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
                clear_edit_wait_state(chat_id)
                save_data(data)
                finance_changed(chat_id, day_key, reason="edit_wait", delay=0.1)

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

    schedule_forward_any_message(chat_id, msg)
def handle_finance_text(msg):
    """
    Обработка обычного ввода для финучёта.
    Теперь принимает сумму не только из text, но и из caption
    у фото/видео/документов/аудио и т.п.
    """

    chat_id = msg.chat.id
    bot_journal("finance_text_start", chat_id, describe_msg_for_log(msg))
    text = _message_text_for_finance(msg)
    if not text:
        return False
    if not is_finance_mode(chat_id):
        return False

    store = get_chat_store(chat_id)
    settings = store.get("settings", {})
    if not settings.get("auto_add", True):
        return False

    if not looks_like_amount(text):
        # Не считаем обычный текст ошибкой, но если в сообщении есть цифры,
        # это полезно видеть в /errors: возможно, формат суммы не распознан.
        if text_has_any_digit(text):
            log_error(f"[FINANCE SKIP] amount not recognized: {describe_msg_for_log(msg)} text={text[:220]!r}")
        return False

    try:
        amount, note = split_amount_and_note(text)
    except Exception as e:
        log_error(f"[FINANCE PARSE ERROR] {describe_msg_for_log(msg)} text={text[:220]!r}: {e}")
        return False

    entry_day = day_key_from_message(msg)
    store["current_view_day"] = entry_day

    try:
        add_record_to_chat(
            chat_id,
            amount,
            note,
            getattr(getattr(msg, "from_user", None), "id", 0),
            source_msg=msg,
            day_key=entry_day
        )
        schedule_finalize(chat_id, entry_day)
        return True
    except Exception as e:
        log_error(f"[FINANCE ADD ERROR] {describe_msg_for_log(msg)} amount={amount} note={note!r}: {e}")
        return False

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
    save_data(data)
    return True
def sync_forwarded_finance_message(dst_chat_id: int, dst_msg_id: int, text: str, owner: int = 0, source_msg=None):
    with locked_chat(dst_chat_id):
        if not is_finance_mode(dst_chat_id):
            if text_has_any_digit(text):
                log_error(f"[FWD FINANCE SKIP] finance mode off: dst={get_chat_display_name(dst_chat_id)} msg={dst_msg_id} text={str(text)[:220]!r}")
            return False

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

        entry_day = day_key_from_message(source_msg) if source_msg is not None else today_key()
        store["current_view_day"] = entry_day

        if text and looks_like_amount(text):
            try:
                amount, note = split_amount_and_note(text)
            except Exception as e:
                log_error(f"[FWD FINANCE PARSE ERROR] dst={get_chat_display_name(dst_chat_id)} msg={dst_msg_id} text={str(text)[:220]!r}: {e}")
                return False

            try:
                if existing:
                    existing["amount"] = amount
                    existing["note"] = note
                    existing["timestamp"] = message_timestamp_iso(source_msg)
                    if source_msg is not None:
                        existing["source_order_msg_id"] = getattr(source_msg, "message_id", existing.get("source_order_msg_id", 0))
                    entry_day = existing.get("day_key") or entry_day
                    rebuild_month_short_ids(dst_chat_id)
                    rebuild_global_records()
                    store["balance"] = sum(float(r.get("amount", 0) or 0) for r in store.get("records", []))
                else:
                    shadow_msg = type("ForwardShadowMsg", (), {
                        "message_id": int(dst_msg_id),
                        "date": getattr(source_msg, "date", int(time.time())) if source_msg is not None else int(time.time()),
                        "forward_source_msg_id": getattr(source_msg, "message_id", int(dst_msg_id)) if source_msg is not None else int(dst_msg_id),
                    })()
                    add_record_to_chat(
                        dst_chat_id,
                        amount,
                        note,
                        owner,
                        source_msg=shadow_msg,
                        day_key=entry_day
                    )
            except Exception as e:
                log_error(f"[FWD FINANCE ADD ERROR] dst={get_chat_display_name(dst_chat_id)} msg={dst_msg_id} amount={amount} note={note!r}: {e}")
                return False

        elif existing:
            existing["amount"] = 0
            existing["note"] = "удалено"
            entry_day = existing.get("day_key") or entry_day
            rebuild_month_short_ids(dst_chat_id)
            rebuild_global_records()
            store["balance"] = sum(float(r.get("amount", 0) or 0) for r in store.get("records", []))
        else:
            if text_has_any_digit(text):
                log_error(f"[FWD FINANCE SKIP] amount not recognized: dst={get_chat_display_name(dst_chat_id)} msg={dst_msg_id} text={str(text)[:220]!r}")
            return False

    schedule_finalize(dst_chat_id, entry_day)
    return True

def export_global_csv(d: dict):
    """Legacy global CSV with all chats (for backup channel), date DD:MM:YY."""
    try:
        with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["date", "amount", "note"])
            rows = []
            for cid, cdata in d.get("chats", {}).items():
                for dk, records in (cdata.get("daily_records", {}) or {}).items():
                    for r in records or []:
                        rows.append((fmt_date_backup(dk), fmt_csv_amount(r.get("amount")), r.get("note", "")))
            # Сортируем по исходной дате, если можем восстановить из DD:MM:YY.
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
    """Преобразует chat_id в строку из emoji-цифр; владельца показываем как 🏀."""
    if is_owner_chat(chat_id):
        return "🏀"
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
        if title:
            base = title
        elif username:
            base = username.lstrip("@")
        else:
            base = str(chat_id)
        return _safe_chat_title_for_filename(base)
    except Exception as e:
        log_error(f"get_chat_name_for_filename({chat_id}): {e}")
        return _safe_chat_title_for_filename(str(chat_id))

def _safe_export_name_part(value, fallback: str = "chat") -> str:
    try:
        value = str(value or "").strip()
    except Exception:
        value = ""
    if not value:
        value = fallback
    value = value.replace(" ", "_")
    value = re.sub(r"[^0-9A-Za-zА-Яа-я_@.\-]+", "", value)
    value = value.strip("._-")
    return (value or fallback)[:70]


def export_period_date_label(mode: str, day_key: str) -> str:
    """Дата/период для имени экспортируемого файла: _(03.06.26-04.06.26)."""
    mode = str(mode or "all").replace("csv_", "").replace("xlsx_", "")
    if mode == "all_real":
        mode = "all"

    def _d(dk: str) -> str:
        return fmt_date_backup(dk).replace(":", ".")

    try:
        if mode == "day":
            return f"({_d(day_key)})"
        if mode == "week":
            base = datetime.strptime(day_key, "%Y-%m-%d")
            start = base - timedelta(days=6)
            return f"({_d(start.strftime('%Y-%m-%d'))}-{_d(day_key)})"
        if mode == "month":
            base = datetime.strptime(day_key, "%Y-%m-%d")
            start = base.replace(day=1)
            return f"({_d(start.strftime('%Y-%m-%d'))}-{_d(day_key)})"
        if mode == "wedthu":
            base = datetime.strptime(day_key, "%Y-%m-%d")
            while base.weekday() != 2:
                base -= timedelta(days=1)
            end = base + timedelta(days=1)
            return f"({_d(base.strftime('%Y-%m-%d'))}-{_d(end.strftime('%Y-%m-%d'))})"
    except Exception:
        pass
    return "(all)"

def export_display_filename(chat_id: int, mode: str, day_key: str, ext: str) -> str:
    """Имя файла для CSV/Excel: имя_чата + дата/период файла."""
    chat_name = _safe_export_name_part(get_chat_name_for_filename(chat_id) or get_chat_display_name(chat_id), f"chat_{chat_id}")
    date_part = export_period_date_label(mode, day_key)
    ext = str(ext or "csv").lower().lstrip(".")
    return f"{chat_name}_{date_part}.{ext}"


def file_bytesio_named(path: str, file_name: str) -> io.BytesIO | None:
    try:
        with open(path, "rb") as f:
            payload = f.read()
        if not payload:
            return None
        buf = io.BytesIO(payload)
        buf.name = file_name
        buf.seek(0)
        return buf
    except Exception as e:
        log_error(f"file_bytesio_named({path}): {e}")
        return None

def _get_chat_title_for_backup(chat_id: int) -> str:
    """Always derive the Telegram filename from the chat's current stored name."""
    try:
        current_name = get_chat_name_for_filename(chat_id)
        if current_name:
            return current_name
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
                _tg_call_retry(
                    bot.edit_message_media,
                    chat_id=int(BACKUP_CHAT_ID),
                    message_id=meta[msg_key],
                    media=types.InputMediaDocument(
                        media=fobj,
                        caption=caption
                    ),
                    purpose="backup_channel_edit_message_media"
                )
                sent = True
                log_info(f"[BACKUP] channel file updated: {base_path}")
            except Exception as e:
                log_error(f"[BACKUP] edit failed, will resend: {e}")

        if not sent:
            fobj = _open_for_telegram()
            if not fobj:
                return
            sent_msg = _tg_call_retry(
                bot.send_document,
                int(BACKUP_CHAT_ID),
                fobj,
                caption=caption,
                purpose="backup_channel_send_document"
            )
            meta[msg_key] = sent_msg.message_id
            log_info(f"[BACKUP] channel file sent new: {base_path}")

        meta[ts_key] = now_local().isoformat(timespec="seconds")
        _save_csv_meta(meta)

    except Exception as e:
        log_error(f"send_backup_to_channel_for_file({base_path}): {e}")
def send_backup_to_channel(chat_id: int):
    bot_journal("backup_to_channel_start", chat_id, "send_backup_to_channel")
    if not is_backup_to_channel_enabled(chat_id):
        return
    """
    Общий бэкап файлов чата в BACKUP_CHAT_ID.
    Делает:
    • проверку флага backup_flags["channel"]
    • один раз (на первый бэкап чата) отправляет chat_id эмодзи в канал
    • обновляет/создаёт в канале только:
        - data_<chat_id>.json
        - data_<chat_id>.xlsx
      CSV в backup-канал больше не отправляется.
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
        # Файлы чата уже сохраняются в flush-очереди; здесь на всякий случай
        # обновляем только файлы конкретного чата, без тяжёлого глобального экспорта.
        save_chat_json(chat_id)
        chat_title = _get_chat_title_for_backup(chat_id)
        meta = _load_csv_meta()
        notify_key = f"emoji_notified_{chat_id}"
        if not meta.get(notify_key):
            try:
                emoji_id = format_chat_id_emoji(chat_id)
                _tg_call_retry(bot.send_message, backup_chat_id, emoji_id, purpose="backup_channel_send_chat_marker")
                backup_channel_notified_chats.add(chat_id)
                meta[notify_key] = True
                _save_csv_meta(meta)
            except Exception as e:
                log_error(
                    f"send_backup_to_channel: не удалось отправить emoji chat_id "
                    f"в канал: {e}"
                )
        json_path = chat_json_file(chat_id)
        xlsx_path = chat_xlsx_file(chat_id)
        # В backup-канал отправляем только JSON и Excel. CSV убран по требованию.
        send_backup_to_channel_for_file(json_path, f"json_{chat_id}", chat_title)
        send_backup_to_channel_for_file(xlsx_path, f"xlsx_{chat_id}", chat_title)
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
    with data_lock:
        fr = data.get("forward_rules", {})
        ff = data.get("forward_finance", {})
        src = str(source_chat_id)

        if src not in fr:
            return []

        out = []
        for dst, mode in list(fr[src].items()):
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
    schedule_config_backup_for_chats(src_chat_id, dst_chat_id)

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
    schedule_config_backup_for_chats(src_chat_id, dst_chat_id)
def clear_forward_all():
    """Полностью отключает всю пересылку."""
    data["forward_rules"] = {}
    data["forward_finance"] = {}
    persist_forward_rules_to_owner()
    save_data(data)
    schedule_config_backup_for_chats()

def get_forward_finance(src_chat_id: int, dst_chat_id: int) -> bool:
    ff = data.setdefault("forward_finance", {})
    return bool(ff.get(str(src_chat_id), {}).get(str(dst_chat_id), False))

def _has_visible_fin_mode_selected(chat_id: int) -> bool:
    """True если включён один из трёх видимых режимов В24: как обычно / 3️⃣ / 🥇."""
    try:
        if not is_finance_mode(chat_id) or is_hidden_finance_mode(chat_id):
            return False
        if is_quick_balance_enabled(chat_id):
            return get_quick_balance_behavior(chat_id) in {"open", "first"}
        return get_quick_balance_behavior(chat_id) == "normal" or not is_quick_balance_enabled(chat_id)
    except Exception:
        return False


def ensure_hidden_finance_for_forward_dst(dst_chat_id: int):
    """Если включили 💰учёт пересылки в чат, этот чат должен принимать финзаписи скрыто."""
    try:
        dst_chat_id = int(dst_chat_id)
        if is_hidden_finance_mode(dst_chat_id):
            return
        set_finance_mode(dst_chat_id, True)
        set_quick_balance_behavior(dst_chat_id, "normal")
        set_quick_balance_enabled(dst_chat_id, False)
        set_hidden_finance_mode(dst_chat_id, True)
        bot_journal("forward_finance_auto_hidden", dst_chat_id, "💰 учёт пересылки включил скрытые финансы")
    except Exception as e:
        log_error(f"ensure_hidden_finance_for_forward_dst({dst_chat_id}): {e}")


def set_forward_finance(src_chat_id: int, dst_chat_id: int, enabled: bool):
    ff = data.setdefault("forward_finance", {})
    src = str(src_chat_id)
    dst = str(dst_chat_id)

    ff.setdefault(src, {})[dst] = bool(enabled)

    # 💰 учёт пересылки записывает финоперацию в принимающий чат.
    # Чтобы там не плодились окна, автоматически включаем скрытые финансы.
    if bool(enabled):
        ensure_hidden_finance_for_forward_dst(int(dst_chat_id))

    persist_forward_rules_to_owner()
    save_data(data)
    schedule_config_backup_for_chats(src_chat_id, dst_chat_id)

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
    schedule_config_backup_for_chats(src_chat_id, dst_chat_id)


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
    with forward_map_lock:
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
    with forward_map_lock:
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
    with forward_map_lock:
        key = (int(src_chat_id), int(src_msg_id))
        pair = (int(dst_chat_id), int(dst_msg_id))
        items = forward_map.setdefault(key, [])
        if pair not in items:
            items.append(pair)
    _schedule_persist_forward_state()


def get_forward_links(src_chat_id: int, src_msg_id: int):
    with forward_map_lock:
        return list(forward_map.get((int(src_chat_id), int(src_msg_id)), []))


def delete_forward_copies_for_source(src_chat_id: int, src_msg_id: int):
    key = (int(src_chat_id), int(src_msg_id))
    with forward_map_lock:
        links = list(forward_map.get(key, []))
    for dst_chat_id, dst_msg_id in links:
        try:
            bot.delete_message(dst_chat_id, dst_msg_id)
        except Exception as e:
            log_error(f"delete_forward_copies_for_source {src_chat_id}:{src_msg_id} -> {dst_chat_id}:{dst_msg_id}: {e}")
        try:
            with locked_chat(dst_chat_id):
                delete_forwarded_finance_record_by_msg_id(dst_chat_id, dst_msg_id)
        except Exception as e:
            log_error(f"delete_forwarded_finance_record_by_msg_id {dst_chat_id}:{dst_msg_id}: {e}")
    with forward_map_lock:
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
    with locked_chat(chat_id):
        rec = find_record_by_message_id(chat_id, msg_id)
        if not rec:
            return False
        day_key = rec.get("day_key") or today_key()
        delete_record_in_chat(chat_id, rec["id"])
        schedule_finalize(chat_id, day_key)
        return True

def rebind_forwarded_finance_record(chat_id: int, old_msg_id: int, new_msg_id: int, text: str, owner: int = 0):
    with locked_chat(chat_id):
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
    with forward_map_lock:
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
            sync_forwarded_finance_message(dst_chat_id, dst_msg_id, text, owner_id, source_msg=msg)
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
    with forward_map_lock:
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
    if _is_bot_removed_error(err):
        set_chat_bot_removed(dst_chat_id, True, str(err)[:240])
    src_name = get_chat_display_name(source_chat_id)
    dst_name = get_chat_display_name(dst_chat_id)
    text = (
        f"⚠️ Пересылка не доставлена\n"
        f"из: {src_name}\n"
        f"сообщение: {msg_id}\n"
        f"в: {dst_name}\n"
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
    trace_chat_id = int(source_chat_id) if is_process_trace_enabled(source_chat_id) else (int(dst_chat_id) if is_process_trace_enabled(dst_chat_id) else int(source_chat_id))
    trace = ProcessTrace(trace_chat_id, f"Пересылка: {get_chat_display_name(source_chat_id)} → {get_chat_display_name(dst_chat_id)}").start()
    trace.step(f"получено сообщение {getattr(msg, 'message_id', '?')} type={getattr(msg, 'content_type', '?')}")
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
            trace.step(f"ищет reply-связь: target_reply={reply_to_target_id}")
            try:
                sent = _tg_call_retry(
                    bot.copy_message,
                    dst_chat_id,
                    source_chat_id,
                    msg.message_id,
                    reply_to_message_id=reply_to_target_id,
                    allow_sending_without_reply=True,
                    purpose="forward_copy_message"
                )
            except TypeError:
                try:
                    sent = _tg_call_retry(
                        bot.copy_message,
                        dst_chat_id,
                        source_chat_id,
                        msg.message_id,
                        reply_to_message_id=reply_to_target_id,
                        purpose="forward_copy_message"
                    )
                except TypeError:
                    sent = _tg_call_retry(bot.copy_message, dst_chat_id, source_chat_id, msg.message_id, purpose="forward_copy_message")
        else:
            trace.step("копирует сообщение через Telegram copy_message")
            sent = _tg_call_retry(bot.copy_message, dst_chat_id, source_chat_id, msg.message_id, purpose="forward_copy_message")
        dst_msg_id = sent.message_id
        trace.step(f"доставлено в целевой чат message_id={dst_msg_id}")
    except Exception:
        trace.step("copy_message не сработал — пробует fallback send")
        try:
            sent_msg = _fallback_send_single(dst_chat_id, msg, reply_to_message_id=reply_to_target_id)
            dst_msg_id = sent_msg.message_id
            trace.step(f"fallback-доставка успешна message_id={dst_msg_id}")
        except Exception as e_send:
            trace.fail(e_send)
            _notify_forward_failure(source_chat_id, msg.message_id, dst_chat_id, e_send)
            return None

    trace.step("сохраняет связь оригинал → копия")
    _store_forward_link(source_chat_id, msg.message_id, dst_chat_id, dst_msg_id)
    trace.step("обновляет счётчик быстрого остатка целевого чата")
    bump_quick_balance_recreate_counter(dst_chat_id)

    text_for_finance = _message_text_for_finance(msg)
    if finance_enabled and text_for_finance:
        trace.step("включён финучёт пересылки — синхронизирует сумму")
        try:
            owner_id = msg.from_user.id if getattr(msg, "from_user", None) else 0
            ok_fin = sync_forwarded_finance_message(dst_chat_id, dst_msg_id, text_for_finance, owner_id, source_msg=msg)
            if not ok_fin and text_has_any_digit(text_for_finance):
                log_error(f"[FWD FINANCE NOT RECORDED] {get_chat_display_name(source_chat_id)}:{msg.message_id} -> {get_chat_display_name(dst_chat_id)}:{dst_msg_id} text={text_for_finance[:220]!r}")
        except Exception as e:
            log_error(f"_forward_single_to_target finance sync {get_chat_display_name(source_chat_id)}->{get_chat_display_name(dst_chat_id)}: {e}")

    trace.finish("пересылка завершена")
    return dst_msg_id


def _flush_media_group_forward(source_chat_id: int, media_group_id: str):
    with forward_delivery_lock:
        return _flush_media_group_forward_locked(source_chat_id, media_group_id)


def _flush_media_group_forward_locked(source_chat_id: int, media_group_id: str):
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
                        sent_group = _tg_call_retry(bot.send_media_group, dst_chat_id, media, reply_to_message_id=reply_to_target_id, allow_sending_without_reply=True, purpose="forward_media_group")
                    except TypeError:
                        try:
                            sent_group = _tg_call_retry(bot.send_media_group, dst_chat_id, media, reply_to_message_id=reply_to_target_id, purpose="forward_media_group")
                        except TypeError:
                            sent_group = _tg_call_retry(bot.send_media_group, dst_chat_id, media, purpose="forward_media_group")
                else:
                    sent_group = _tg_call_retry(bot.send_media_group, dst_chat_id, media, purpose="forward_media_group")
                sent_ids = [m.message_id for m in sent_group]
            except Exception as e:
                log_error(f"_flush_media_group_forward send_media_group failed {get_chat_display_name(source_chat_id)}->{get_chat_display_name(dst_chat_id)}: {e}")

        if len(sent_ids) == len(messages):
            for src_msg, dst_msg_id in zip(messages, sent_ids):
                _store_forward_link(source_chat_id, src_msg.message_id, dst_chat_id, dst_msg_id)
                bump_quick_balance_recreate_counter(dst_chat_id)
                text_for_finance = _message_text_for_finance(src_msg)
                if finance_enabled and text_for_finance:
                    try:
                        owner_id = src_msg.from_user.id if getattr(src_msg, "from_user", None) else 0
                        ok_fin = sync_forwarded_finance_message(dst_chat_id, dst_msg_id, text_for_finance, owner_id, source_msg=src_msg)
                        if not ok_fin and text_has_any_digit(text_for_finance):
                            log_error(f"[FWD MEDIA FINANCE NOT RECORDED] {get_chat_display_name(source_chat_id)}:{src_msg.message_id} -> {get_chat_display_name(dst_chat_id)}:{dst_msg_id} text={text_for_finance[:220]!r}")
                    except Exception as e:
                        log_error(f"_flush_media_group_forward finance sync {get_chat_display_name(source_chat_id)}->{get_chat_display_name(dst_chat_id)}: {e}")
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
        return wm_common("\n".join(header + ["Нет записей за этот день."] + footer), 1, html_mode=True), total

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
            return wm_common(text, 1, html_mode=True), total

        if len(visible) <= 5:
            return wm_common(text[:DAY_WINDOW_MAX_CHARS], 1, html_mode=True), total

        hidden += 1
        visible = visible[1:]


def _collect_process_menu_items():
    """Чаты для меню PROCESS: известные чаты + владелец, без дублей."""
    items = {}
    try:
        known = collect_forward_menu_chats()
        for cid, ch in (known or {}).items():
            try:
                int_cid = int(cid)
            except Exception:
                continue
            title = (ch or {}).get("title") or get_chat_display_name(int_cid)
            items[int_cid] = title
    except Exception as e:
        log_error(f"_collect_process_menu_items known: {e}")

    try:
        for cid in (data.get("chats", {}) or {}).keys():
            try:
                int_cid = int(cid)
            except Exception:
                continue
            items.setdefault(int_cid, get_chat_display_name(int_cid))
    except Exception as e:
        log_error(f"_collect_process_menu_items data: {e}")

    if OWNER_ID:
        try:
            owner_id = int(OWNER_ID)
            items.setdefault(owner_id, get_chat_display_name(owner_id))
        except Exception:
            pass

    return sorted(items.items(), key=lambda x: (x[1] or "").lower())


def build_process_menu(day_key: str):
    kb = types.InlineKeyboardMarkup(row_width=2)
    buttons = []
    for cid, title in _collect_process_menu_items():
        icon = "✅" if is_process_trace_enabled(cid) else "❌"
        buttons.append(IB(
            f"{icon} {chat_button_title(cid, title)}",
            callback_data=f"d:{day_key}:process_toggle_{cid}"
        ))
    if buttons:
        add_buttons_in_rows(kb, buttons, 2)
    else:
        kb.row(IB("Нет чатов", callback_data="none"))
    kb.row(IB("🔙 Назад", callback_data=f"d:{day_key}:back_main"))
    return kb


def build_process_menu_text() -> str:
    return wm_owner((
        "🧪 PROCESS\n"
        "Включает диагностическое сообщение процесса для выбранного чата.\n"
        "По умолчанию у всех чатов выключено. Когда включено, бот пишет одно сообщение и редактирует его, добавляя этапы по времени ЧЧ:ММ:СС.мс."
    ), 8)


def _collect_backup_menu_items():
    """Чаты для меню BACKUP: известные чаты + владелец, без дублей."""
    return _collect_process_menu_items()


def build_backup_owner_menu(day_key: str):
    """Меню владельца BACKUP: каждая строка = чат | в чат | канал | MEGA."""
    kb = types.InlineKeyboardMarkup(row_width=4)
    owner_id = int(OWNER_ID) if OWNER_ID else None
    for cid, title in _collect_backup_menu_items():
        # Если бот удалён из чата, название остаётся с ➖ и само нажатие на название
        # показывает владельцу понятное сообщение, а не молчит через callback_data="none".
        title_cb = f"d:{day_key}:removed_{cid}" if is_chat_bot_removed(cid) else "none"
        chat_btn = IB(f"💬 {chat_button_title(cid, title)}", callback_data=title_cb)
        chat_label = _backup_toggle_label(cid, "chat", "чат") if (owner_id is not None and int(cid) == owner_id) else "➖ чат"
        chat_cb = f"d:{day_key}:backup_toggle_chat_{cid}" if (owner_id is not None and int(cid) == owner_id) else (f"d:{day_key}:removed_{cid}" if is_chat_bot_removed(cid) else "none")
        kb.row(
            chat_btn,
            IB(chat_label, callback_data=chat_cb),
            IB(_backup_toggle_label(cid, "channel", "канал"), callback_data=f"d:{day_key}:backup_toggle_channel_{cid}"),
            IB(_backup_toggle_label(cid, "mega", "MEGA"), callback_data=f"d:{day_key}:backup_toggle_mega_{cid}"),
        )
    kb.row(IB("🔙 Назад", callback_data=f"d:{day_key}:back_main"))
    return kb


def build_backup_owner_menu_text() -> str:
    return wm_owner((
        "💾 BACKUP\n"
        "Настройка авто-бэкапов по чатам. По умолчанию все бэкапы включены.\n"
        "Канал = JSON + Excel. MEGA = только JSON. В чат = только для владельца, чтобы JSON не уходил пользователям."
    ), 7)


def build_main_keyboard(day_key: str, chat_id=None):
    """Главное окно без отдельной кнопки «Меню»: все основные функции сразу на виду."""
    kb = types.InlineKeyboardMarkup(row_width=3)

    nav_row = [
        IB("⬅️ Вчера", callback_data=f"d:{day_key}:prev")
    ]
    if day_key != today_key():
        nav_row.append(IB("📅 Сегодня", callback_data=f"d:{day_key}:today"))
    nav_row.append(IB("➡️ Завтра", callback_data=f"d:{day_key}:next"))
    kb.row(*nav_row)

    kb.row(
        IB("📅 Календарь", callback_data=f"d:{day_key}:calendar"),
        IB("📊 Отчёт", callback_data=f"d:{day_key}:report"),
        IB("💰 Общий итог", callback_data=f"d:{day_key}:total"),
    )
    kb.row(
        IB("📝 Редактировать", callback_data=f"d:{day_key}:edit_list"),
        IB("📂 CSV", callback_data=f"d:{day_key}:csv_all"),
        IB("📊 Статьи", callback_data=cat_callback("cat_today")),
    )
    # Обнуление убрано из основного окна о1 по ТЗ. Оставлена команда /reset в окне ℹ️ Инфо.
    kb.row(
        IB("ℹ️ Инфо", callback_data=f"d:{day_key}:info"),
    )

    if is_owner_chat(chat_id):
        kb.row(
            IB("🔁 Пересылка", callback_data=f"d:{day_key}:forward_menu"),
            IB("💰 Фин режим", callback_data=f"d:{day_key}:forward_finmode_menu"),
        )
        # Скрытые финансы и Фин окно перенесены внутрь: Фин режим → выбор чата → настройки чата.
        kb.row(
            IB("🧪 PROCESS", callback_data=f"d:{day_key}:process_menu"),
            IB("💾 BACKUP", callback_data=f"d:{day_key}:backup_menu"),
        )

    return kb
def build_report_keyboard(month_key: str):
    """
    month_key: YYYY-MM. В о3 навигация по месяцам — первый ряд, назад/закрыть — второй ряд.
    """
    kb = types.InlineKeyboardMarkup(row_width=3)

    try:
        dt = datetime.strptime(month_key + "-01", "%Y-%m-%d")
    except Exception:
        dt = now_local().replace(day=1)
        month_key = dt.strftime("%Y-%m")

    current_month = now_local().strftime("%Y-%m")
    prev_month = (dt.replace(day=1) - timedelta(days=1)).replace(day=1)
    next_month = (dt.replace(day=28) + timedelta(days=4)).replace(day=1)

    nav_row = [IB("⬅️ Пред. месяц", callback_data=f"rep:{prev_month.strftime('%Y-%m')}")]
    if month_key != current_month:
        nav_row.append(IB("📅 Сегодня", callback_data="rep_today"))
    nav_row.append(IB("След. месяц ➡️", callback_data=f"rep:{next_month.strftime('%Y-%m')}"))
    kb.row(*nav_row)
    kb.row(
        IB("⬅️ Назад осн. окно", callback_data=f"d:{today_key()}:back_main"),
        IB("❌ Закрыть", callback_data="rep_close"),
    )
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

    return wm_common("<pre>" + html.escape("\n".join(lines)) + "</pre>", 3, html_mode=True), month_key

def build_calendar_keyboard(center_day: datetime, chat_id=None):
    """Месячный финансовый календарь с привычной сеткой Пн–Вс."""
    kb = types.InlineKeyboardMarkup(row_width=7)
    daily = {}
    back_day_key = today_key()
    if chat_id is not None:
        store = get_chat_store(chat_id)
        daily = store.get("daily_records", {})
        back_day_key = store.get("current_view_day", today_key())

    kb.row(*[IB(x, callback_data="none") for x in ("Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс")])
    for week in calendar.Calendar(firstweekday=0).monthdayscalendar(center_day.year, center_day.month):
        row = []
        for day_num in week:
            if not day_num:
                row.append(IB(" ", callback_data="none"))
                continue
            key = f"{center_day.year:04d}-{center_day.month:02d}-{day_num:02d}"
            label = f"📝{day_num}" if daily.get(key) else str(day_num)
            if daily.get(key):
                label = f"📝{day_num}"
            row.append(
                IB(
                    label,
                    callback_data=f"d:{key}:open"
                )
            )
        kb.row(*row)

    prev_month = (center_day.replace(day=1) - timedelta(days=1)).replace(day=1)
    next_month = (center_day.replace(day=28) + timedelta(days=4)).replace(day=1)

    kb.row(
        IB(
            "⬅️ Месяц",
            callback_data=f"c:{prev_month.strftime('%Y-%m-%d')}"
        ),
        IB(
            "➡️ Месяц",
            callback_data=f"c:{next_month.strftime('%Y-%m-%d')}"
        )
    )

    current_month = now_local().strftime("%Y-%m")
    shown_month = center_day.strftime("%Y-%m")
    bottom_row = []
    if shown_month != current_month:
        bottom_row.append(
            IB(
                "📅 Сегодня",
                callback_data=f"c:{now_local().strftime('%Y-%m-%d')}"
            )
        )
    elif back_day_key != today_key():
        bottom_row.append(
            IB(
                "📅 Сегодня",
                callback_data=f"d:{today_key()}:open"
            )
        )

    bottom_row.append(
        IB(
            "🔙 Назад",
            callback_data=f"d:{back_day_key}:back_main"
        )
    )
    kb.row(*bottom_row)
    return kb

def _backup_toggle_label(chat_id: int, target: str, label: str) -> str:
    icon = "✅" if is_backup_target_enabled(chat_id, target) else "❌"
    return f"{icon} {label}"


def _add_export_period_rows(kb, day_key: str, prefix: str, owner_day_key: str | None = None, target_chat_id: int | None = None):
    """Ряды: слева период, справа CSV и Excel."""
    periods = [
        ("📅 День", "day"),
        ("🗓 Неделя", "week"),
        ("📆 Месяц", "month"),
        ("📊 Ср–Чт", "wedthu"),
        ("📂 Всё время", "all"),
    ]
    for label, mode in periods:
        if prefix == "fv":
            csv_cb = f"fv:{target_chat_id}:{day_key}:csv_{mode}:{owner_day_key}"
            xlsx_cb = f"fv:{target_chat_id}:{day_key}:xlsx_{mode}:{owner_day_key}"
        else:
            csv_action = "csv_all_real" if mode == "all" else f"csv_{mode}"
            xlsx_action = "xlsx_all" if mode == "all" else f"xlsx_{mode}"
            csv_cb = f"d:{day_key}:{csv_action}"
            xlsx_cb = f"d:{day_key}:{xlsx_action}"
        kb.row(
            IB(label, callback_data="none"),
            IB("CSV", callback_data=csv_cb),
            IB("Excel", callback_data=xlsx_cb),
        )


def build_csv_menu(day_key: str, chat_id: int | None = None):
    kb = types.InlineKeyboardMarkup(row_width=3)
    _add_export_period_rows(kb, day_key, "d")
    # Выбор вида/направления бэкапа оставлен только владельцу.
    if chat_id is not None and is_owner_chat(chat_id):
        kb.row(
            IB(_backup_toggle_label(chat_id, "chat", "Бэкап в чат"), callback_data=f"d:{day_key}:bk_chat"),
            IB(_backup_toggle_label(chat_id, "channel", "в канал"), callback_data=f"d:{day_key}:bk_channel"),
            IB(_backup_toggle_label(chat_id, "mega", "в MEGA"), callback_data=f"d:{day_key}:bk_mega"),
        )
    kb.row(IB("⬅️ Назад", callback_data=f"d:{day_key}:edit_menu"))
    return kb


def build_edit_menu_keyboard(day_key: str, chat_id=None):
    """Совместимость со старыми callback: отдельного подменю больше нет."""
    return build_main_keyboard(day_key, chat_id)
def make_copy_or_inline_button(label: str, text: str):
    """Кнопка-вставка в поле ввода через inline current chat.
    Если Telegram добавит @имя_бота, обработчики редактирования очищают его перед сохранением.
    """
    return IB(label, switch_inline_query_current_chat=str(text)[:256])




_BOT_USERNAME_CACHE = None

def get_bot_username_cached() -> str:
    """Имя бота нужно только для очистки текста, вставленного через inline-поле Telegram."""
    global _BOT_USERNAME_CACHE
    if _BOT_USERNAME_CACHE is not None:
        return _BOT_USERNAME_CACHE
    try:
        me = bot.get_me()
        _BOT_USERNAME_CACHE = (getattr(me, "username", "") or "").lstrip("@")
    except Exception:
        _BOT_USERNAME_CACHE = ""
    return _BOT_USERNAME_CACHE

def sanitize_telegram_inserted_text(text: str) -> str:
    """Убирает @имя_бота, которое Telegram может добавить при inline-вставке."""
    s = str(text or "").strip()
    username = get_bot_username_cached()
    if username:
        s = re.sub(rf"(?im)^\s*@{re.escape(username)}\b[:\s,]*", "", s)
        s = re.sub(rf"(?i)\s*@{re.escape(username)}\b", "", s)
    # Запасной вариант: если Telegram поставил любое @имя в самое начало перед суммой/служебной скобкой.
    s = re.sub(r"(?m)^\s*@[A-Za-z0-9_]{3,}\s+(?=(?:\(|[+\-–]?\s*\d))", "", s)
    return re.sub(r"[ \t]+", " ", s).strip()

DIRECT_EDIT_TOKEN = "EDITREC"


def compose_direct_edit_insert_value(target_chat_id: int, rid: int, day_key: str, amount, note: str = "") -> str:
    """Текст для быстрой вставки редактирования записи через inline-поле Telegram.
    Метаданные спрятаны в скобках. Пользователь меняет только строку суммы ниже.
    После отправки бот удалит служебную строку/сообщение и обновит запись.
    """
    value = compose_edit_input_value(amount, note)
    meta = f"{DIRECT_EDIT_TOKEN}|{int(target_chat_id)}|{int(rid)}|{str(day_key)[:10]}|"
    return f"({meta} служебное — можно не трогать)\n\n{value}"


def make_direct_edit_insert_button(label: str, insert_text: str):
    """Кнопка, которая сразу открывает поле ввода Telegram с подготовленным текстом.
    В Bot API это возможно только через inline-query текущего чата; обычный callback не умеет
    принудительно вставлять текст в поле ввода.
    """
    return IB(label, switch_inline_query_current_chat=str(insert_text)[:256])


def handle_direct_edit_insert_message(msg) -> bool:
    """Обрабатывает отправленный пользователем текст, который был вставлен кнопкой ✏️ из О6.
    Формат: EDITREC|chat_id|rid|day_key| сумма описание
    """
    try:
        if getattr(msg, "content_type", None) != "text":
            return False
        chat_id = int(msg.chat.id)
        text = (msg.text or "").strip()
        if DIRECT_EDIT_TOKEN + "|" not in text:
            return False

        # Новый удобный формат: (EDITREC|chat|rid|day| служебное...) + ниже обычный текст суммы.
        # Всё, что в скобках, используется как адрес записи и удаляется из текста перед разбором суммы.
        m = re.search(r"\((%s\|[^)]*)\)" % re.escape(DIRECT_EDIT_TOKEN), text)
        if m:
            meta_text = m.group(1)
            parts = meta_text.split("|", 4)
            if len(parts) < 4:
                return False
            _, target_s, rid_s, day_key = parts[:4]
            value_text = (text[:m.start()] + " " + text[m.end():]).strip()
        else:
            # Старый формат для совместимости: EDITREC|chat|rid|day| сумма описание
            text = text[text.find(DIRECT_EDIT_TOKEN + "|"):]
            parts = text.split("|", 4)
            if len(parts) < 5:
                return False
            _, target_s, rid_s, day_key, value_text = parts
            value_text = (value_text or "").strip()

        target_chat_id = int(target_s)
        rid = int(rid_s)
        day_key = (day_key or today_key())[:10]
        value_text = sanitize_telegram_inserted_text(value_text)
        if not value_text:
            send_and_auto_delete(chat_id, "❌ Нет нового значения для редактирования.", 10)
            return True

        # Обычный пользователь может редактировать только запись своего чата.
        # Владелец может редактировать любой просматриваемый чат.
        if not is_owner_chat(chat_id) and int(chat_id) != int(target_chat_id):
            send_and_auto_delete(chat_id, "⛔ Нельзя редактировать запись другого чата.", 10)
            return True

        amount, note = split_amount_and_note(value_text)
        with locked_chat(target_chat_id):
            ok = update_record_in_chat(target_chat_id, rid, amount, note)
        if not ok:
            send_and_auto_delete(chat_id, "❌ Запись для редактирования не найдена.", 10)
            return True

        try:
            bot.delete_message(chat_id, msg.message_id)
        except Exception:
            pass
        finance_changed(target_chat_id, day_key, reason="direct_edit_insert", delay=0.1)
        send_and_auto_delete(chat_id, f"✅ Запись обновлена: {fmt_num(amount)} {note}", 8)
        return True
    except Exception as e:
        log_error(f"handle_direct_edit_insert_message: {e}")
        try:
            send_and_auto_delete(msg.chat.id, "❌ Не удалось применить вставленное редактирование.", 10)
        except Exception:
            pass
        return True

def build_cancel_edit_keyboard(day_key: str, insert_text: str | None = None):
    kb = types.InlineKeyboardMarkup()
    # Кнопка открывает поле ввода через inline current chat; возможное @имя_бота очищается обработчиком перед сохранением.
    if insert_text:
        kb.row(make_copy_or_inline_button("✍️ Вставить текст", str(insert_text)))
    kb.row(
        IB("❌ Закрыть", callback_data=f"d:{day_key}:cancel_edit"),
        IB("⬅️ Назад осн. окно", callback_data=f"d:{day_key}:back_main"),
    )
    return kb


def build_finwin_cancel_edit_keyboard(target_chat_id: int, day_key: str, owner_day_key: str, insert_text: str | None = None):
    kb = types.InlineKeyboardMarkup()
    if insert_text:
        kb.row(make_copy_or_inline_button("✍️ Вставить текст", str(insert_text)))
    kb.row(
        IB("❌ Закрыть", callback_data=f"fv:{target_chat_id}:{day_key}:cancel_edit:{owner_day_key}"),
        IB("⬅️ Назад осн. окно", callback_data=f"fv:{target_chat_id}:{day_key}:open:{owner_day_key}"),
    )
    return kb


def send_or_edit_edit_prompt(chat_id: int, store_key: str, text: str, reply_markup=None, parse_mode=None):
    """Окно редактирования записи не плодится: старое сообщение редактируется, новое создаётся только если старое недоступно."""
    store = get_chat_store(chat_id)
    prev = store.get(store_key) or {}
    prev_id = prev.get("prompt_msg_id") if isinstance(prev, dict) else None
    if prev_id:
        try:
            _tg_call_retry(
                bot.edit_message_text,
                text,
                chat_id=chat_id,
                message_id=int(prev_id),
                reply_markup=reply_markup,
                parse_mode=parse_mode,
                purpose="edit_prompt_edit_message"
            )
            return int(prev_id)
        except Exception as e:
            err = str(e).lower()
            if "message is not modified" in err:
                return int(prev_id)
            try:
                bot.delete_message(chat_id, int(prev_id))
            except Exception:
                pass
    sent = _tg_call_retry(bot.send_message, chat_id, text, reply_markup=reply_markup, parse_mode=parse_mode, purpose="edit_prompt_send_message")
    return sent.message_id


def build_forward_root_menu(day_key: str):
    """Корневое меню пересылки: старый режим или новый визуальный режим пары A/B."""
    if forward_menu_new_style_enabled():
        return build_forward_new_menu(day_key)
    return build_forward_source_menu(day_key)
def _collect_forward_picker_items(include_owner: bool = True, include_removed: bool = False):
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
            # Удалённые чаты не держим в общем списке — они показываются только в меню «Удалённые».
            if (not include_removed) and is_chat_bot_removed(int_cid):
                continue
            items.append((int_cid, title))

    if include_owner and OWNER_ID:
        try:
            owner_id = int(OWNER_ID)
            if owner_item is None:
                owner_item = (owner_id, get_chat_display_name(owner_id))
        except Exception:
            owner_item = None

    return items, owner_item



def build_forward_source_menu(day_key: str | None = None):
    if forward_menu_new_style_enabled():
        return build_forward_new_menu(day_key)
    kb = types.InlineKeyboardMarkup(row_width=3)
    if not OWNER_ID:
        return kb

    items, owner_item = _collect_forward_picker_items(include_owner=True)
    buttons = [
        IB(chat_button_title(cid, title), callback_data=f"fw_src:{cid}")
        for cid, title in items
    ]
    add_buttons_in_rows(kb, buttons, 2)

    if owner_item:
        kb.row(IB(chat_button_title(owner_item[0], owner_item[1]), callback_data=f"fw_src:{owner_item[0]}"))

    kb.row(
        IB("📡 Проверить чаты", callback_data="fw_probe_all"),
        IB("🗑 Удалённые", callback_data="fw_removed_list"),
    )

    if day_key:
        kb.row(IB("🔙 Назад", callback_data=f"d:{day_key}:back_main"))
    else:
        kb.row(IB("🔙 Назад", callback_data="fw_back_root"))
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
        buttons.append(IB(chat_button_title(int_cid, title), callback_data=f"fw_tgt:{src_id}:{int_cid}"))

    add_buttons_in_rows(kb, buttons, 2)

    if owner_item and owner_item[0] != src_id:
        kb.row(IB(chat_button_title(owner_item[0], owner_item[1]), callback_data=f"fw_tgt:{src_id}:{owner_item[0]}"))

    kb.row(IB("🔙 Назад", callback_data="fw_back_src"))
    return kb




def _forward_pair_key(A: int, B: int) -> str:
    # В новом В22 порядок важен: выбранный Чат A остаётся слева, Чат B справа.
    return f"{int(A)}:{int(B)}"


def _forward_pair_undirected_key(A: int, B: int) -> tuple[int, int]:
    A = int(A); B = int(B)
    return (A, B) if A <= B else (B, A)


def _remember_forward_pair(A: int, B: int):
    """Сохраняет порядок создания пар для нового В22. Старую логику пересылки не трогает."""
    try:
        A, B = int(A), int(B)
        if A == B:
            return
        key = _forward_pair_key(A, B)
        rev = _forward_pair_key(B, A)
        order = data.setdefault("forward_pair_order", [])
        if not isinstance(order, list):
            order = []
            data["forward_pair_order"] = order
        if key not in order and rev not in order:
            order.append(key)
            save_data(data)
    except Exception as e:
        log_error(f"_remember_forward_pair({A},{B}): {e}")


def _forget_forward_pair_if_empty(A: int, B: int):
    """Убирает пару из порядка, только если уже нет ни пересылки, ни 💰 финучёта в обе стороны."""
    try:
        A, B = int(A), int(B)
        arrow, fin, ab_on, ba_on, ab_fin, ba_fin = _forward_pair_icons(A, B)
        if ab_on or ba_on or ab_fin or ba_fin:
            return
        key = _forward_pair_key(A, B)
        rev = _forward_pair_key(B, A)
        order = data.setdefault("forward_pair_order", [])
        if isinstance(order, list) and (key in order or rev in order):
            data["forward_pair_order"] = [x for x in order if x not in {key, rev}]
            save_data(data)
    except Exception as e:
        log_error(f"_forget_forward_pair_if_empty({A},{B}): {e}")


def _forward_pair_sort_key(pair):
    try:
        order = data.get("forward_pair_order", []) or []
        key = _forward_pair_key(pair[0], pair[1])
        rev = _forward_pair_key(pair[1], pair[0])
        if key in order:
            return (0, order.index(key))
        if rev in order:
            return (0, order.index(rev))
        a, b = pair
        return (1, get_chat_display_name(int(a)).lower(), get_chat_display_name(int(b)).lower(), int(a), int(b))
    except Exception:
        return (9, str(pair))


def _sorted_forward_pair(a: int, b: int):
    """Старый helper оставлен для совместимости. Новый В22 порядок выбора не сортирует."""
    a = int(a); b = int(b)
    ka = (get_chat_display_name(a).lower(), a)
    kb = (get_chat_display_name(b).lower(), b)
    return (a, b) if ka <= kb else (b, a)


def collect_forward_pairs_for_menu() -> list[tuple[int, int]]:
    """Все пары, где есть пересылка или 💰 финучёт пересылки. Порядок пары берём из создания/первого обнаружения."""
    relation_pairs = []
    seen = set()
    fr = data.get("forward_rules", {}) or {}
    ff = data.get("forward_finance", {}) or {}

    def _add_pair(a, b):
        try:
            a = int(a); b = int(b)
        except Exception:
            return
        if a == b:
            return
        uk = _forward_pair_undirected_key(a, b)
        if uk in seen:
            return
        seen.add(uk)
        relation_pairs.append((a, b))

    # Сначала порядок, который создал владелец в новом В22.
    order = data.get("forward_pair_order", []) or []
    if isinstance(order, list):
        for key in order:
            try:
                a_s, b_s = str(key).split(":", 1)
                a, b = int(a_s), int(b_s)
            except Exception:
                continue
            arrow, fin, ab_on, ba_on, ab_fin, ba_fin = _forward_pair_icons(a, b)
            if ab_on or ba_on or ab_fin or ba_fin:
                _add_pair(a, b)

    # Потом старые/найденные связи — в порядке словарей, не ломая старую базу.
    for src, dsts in fr.items():
        for dst in (dsts or {}).keys():
            _add_pair(src, dst)
    for src, dsts in ff.items():
        for dst, enabled in (dsts or {}).items():
            if enabled:
                _add_pair(src, dst)

    # Дополняем forward_pair_order, чтобы следующий раз порядок был стабильным.
    try:
        order = data.setdefault("forward_pair_order", [])
        if not isinstance(order, list):
            order = []
            data["forward_pair_order"] = order
        changed = False
        for A, B in relation_pairs:
            key = _forward_pair_key(A, B)
            rev = _forward_pair_key(B, A)
            if key not in order and rev not in order:
                order.append(key)
                changed = True
        if changed:
            save_data(data)
    except Exception:
        pass

    return sorted(relation_pairs, key=_forward_pair_sort_key)


def _forward_pair_icons(A: int, B: int):
    fr = data.get("forward_rules", {}) or {}
    ff = data.get("forward_finance", {}) or {}
    ab_on = str(B) in (fr.get(str(A), {}) or {})
    ba_on = str(A) in (fr.get(str(B), {}) or {})
    ab_fin = bool((ff.get(str(A), {}) or {}).get(str(B), False))
    ba_fin = bool((ff.get(str(B), {}) or {}).get(str(A), False))
    return _forward_arrow_icon(ab_on, ba_on), _forward_fin_icon(ab_fin, ba_fin), ab_on, ba_on, ab_fin, ba_fin


def _forward_new_pair_buttons(A: int, B: int):
    """Две кнопки пары сверху в новом В22.

    По уточнённому ТЗ:
    • кнопка Чата A сверху остаётся выбором этого чата как нового Чата A;
    • кнопка Чата B сверху открывает настройки именно этой пары и помечается 🛠️ перед именем;
    • ниже разделителя Чаты A из готовых пар не дублируются, чтобы список не захламлялся.
    """
    arrow, fin, *_ = _forward_pair_icons(A, B)
    return (
        IB(f"{chat_button_title(A)} ({arrow})", callback_data=f"fw_new_src:{A}"),
        IB(f"({fin}) 🛠️ {chat_button_title(B)}", callback_data=f"fw_new_pair:{A}:{B}"),
    )


def _forward_new_toggle_label(enabled: bool, icon: str) -> str:
    return ("✅" if enabled else "❌") + icon


def _visible_forward_items_for_new_menu(include_owner: bool = True):
    items, owner_item = _collect_forward_picker_items(include_owner=include_owner)
    all_items = list(items)
    if owner_item:
        all_items.append(owner_item)
    visible = []
    for cid, title in all_items:
        try:
            if is_chat_bot_removed(int(cid)):
                continue
        except Exception:
            pass
        visible.append((int(cid), title))
    return visible


def build_forward_new_text(A: int | None = None, B: int | None = None) -> str:
    """В22 новый режим: пары сверху, выбор A/B и настройка шести кнопок."""
    lines = ["🔁 Пересылка / В22", "Режим: по-новому", ""]
    if A and B:
        arrow, fin, *_ = _forward_pair_icons(A, B)
        lines.append(f"Чат А: {get_chat_display_name(A)} ({arrow})")
        lines.append(f"Чат Б: ({fin}) {get_chat_display_name(B)}")
        lines.append("Ниже выбери направление пересылки и 💰 финучёт.")
    elif A:
        lines.append(f"Чат А выбран: {get_chat_display_name(A)}")
        lines.append("Теперь выбери Чат Б. Остальные чаты остаются ниже по 2 кнопки в ряд.")
    else:
        lines.append("Сверху пары со связями. Ниже — все доступные чаты. Любой чат можно снова выбрать как Чат А.")
    return "\n".join(lines)


def build_forward_new_menu(day_key: str | None = None, A: int | None = None, B: int | None = None):
    """
    Новый В22 по уточнённому ТЗ:
    • старт: пары сверху по 2 кнопки (A слева, B справа), потом пустой разделитель, потом свободные чаты по 2 кнопки;
    • выбран A: кнопка Чат А сверху, остальные чаты остаются ниже по 2 кнопки;
    • выбран B: сверху Чат А / Чат Б, ниже 6 кнопок режимов, ниже кнопка возврата к выбору чатов.
    """
    kb = types.InlineKeyboardMarkup(row_width=2)
    if not OWNER_ID:
        return kb

    visible_items = _visible_forward_items_for_new_menu(include_owner=True)
    pair_rows = collect_forward_pairs_for_menu()

    if A and B:
        A, B = int(A), int(B)
        arrow, fin, ab_on, ba_on, ab_fin, ba_fin = _forward_pair_icons(A, B)
        kb.row(
            IB(f"Чат А: {chat_button_title(A)}", callback_data=f"fw_new_pair:{A}:{B}"),
            IB(f"Чат Б: {chat_button_title(B)}", callback_data=f"fw_new_pair:{A}:{B}"),
        )
        kb.row(
            IB(_forward_new_toggle_label(ba_on, "⏪️"), callback_data=f"fw_new_mode:{A}:{B}:from"),
            IB(_forward_new_toggle_label(ab_on, "⏩️"), callback_data=f"fw_new_mode:{A}:{B}:to"),
            IB(_forward_new_toggle_label(ab_on and ba_on, "🔄"), callback_data=f"fw_new_mode:{A}:{B}:two"),
            IB(_forward_new_toggle_label(ba_fin, "◀️"), callback_data=f"fw_new_fin:{A}:{B}:ba"),
            IB(_forward_new_toggle_label(ab_fin, "▶️"), callback_data=f"fw_new_fin:{A}:{B}:ab"),
            IB("❌", callback_data=f"fw_new_clear:{A}:{B}"),
        )
        kb.row(IB("🔙 Назад в окно выбора чатов", callback_data="fw_new_back_src"))
        return kb

    if A:
        A = int(A)
        kb.row(IB(f"Чат А: {chat_button_title(A)}", callback_data=f"fw_new_src:{A}"))
        buttons = []
        for cid, title in visible_items:
            if int(cid) == int(A):
                continue
            buttons.append(IB(f"Чат Б: {chat_button_title(cid, title)}", callback_data=f"fw_new_tgt:{A}:{int(cid)}"))
        if buttons:
            add_buttons_in_rows(kb, buttons, 2)
        else:
            kb.row(IB("Нет чатов для выбора Чата Б", callback_data="none"))
        kb.row(IB("🔙 Назад в окно выбора чатов", callback_data="fw_new_back_src"))
        return kb

    shown_pairs = 0
    top_pair_a_ids = set()
    for A0, B0 in pair_rows:
        try:
            if is_chat_bot_removed(A0) or is_chat_bot_removed(B0):
                continue
        except Exception:
            pass
        top_pair_a_ids.add(int(A0))
        left_btn, right_btn = _forward_new_pair_buttons(A0, B0)
        kb.row(left_btn, right_btn)
        shown_pairs += 1

    # Ниже после разделителя показываем доступные чаты, но убираем только те,
    # которые уже стоят сверху как Чат A. Чаты B остаются доступными для выбора,
    # а их верхняя кнопка с 🛠 открывает настройки существующей пары.
    chat_buttons = []
    for cid, title in visible_items:
        if int(cid) in top_pair_a_ids:
            continue
        chat_buttons.append(IB(chat_button_title(cid, title), callback_data=f"fw_new_src:{cid}"))

    if shown_pairs and chat_buttons:
        kb.row(IB("⠀", callback_data="none"))

    if chat_buttons:
        add_buttons_in_rows(kb, chat_buttons, 2)
    elif not shown_pairs:
        kb.row(IB("Нет доступных чатов", callback_data="none"))

    kb.row(
        IB("📡 Проверить чаты", callback_data="fw_probe_all"),
        IB("🗑 Удалённые", callback_data="fw_removed_list"),
    )
    if day_key:
        kb.row(IB("🔙 Назад", callback_data=f"d:{day_key}:back_main"))
    else:
        kb.row(IB("🔙 Назад", callback_data="fw_back_root"))
    return kb

def build_forward_menu_text_for_current_mode(title: str | None = None, A: int | None = None, B: int | None = None) -> str:
    if forward_menu_new_style_enabled():
        return build_forward_new_text(A, B)
    return build_forward_status_text(title or "Пересылка:\nВыберите чат A:")


def build_forward_menu_keyboard_for_current_mode(day_key: str | None = None, A: int | None = None, B: int | None = None):
    if forward_menu_new_style_enabled():
        return build_forward_new_menu(day_key, A, B)
    if A and B:
        return build_forward_mode_menu(A, B)
    if A:
        return build_forward_target_menu(A)
    return build_forward_source_menu(day_key)



def finance_mode_compact_icon(chat_id: int) -> str:
    """Короткий знак режима для В24 в списке чатов. Скрытый режим независим и дописывается обезьянкой."""
    try:
        if not is_finance_mode(chat_id):
            return "❌"
        hidden_prefix = "🙈" if is_hidden_finance_mode(chat_id) else ""
        if is_quick_balance_enabled(chat_id):
            behavior = get_quick_balance_behavior(chat_id)
            if behavior == "first":
                return hidden_prefix + "✅🥇"
            if behavior == "open":
                return hidden_prefix + "✅3️⃣"
        return hidden_prefix + "✅🔟"
    except Exception:
        return "❌"


def finance_mode_state_lines(chat_id: int) -> list[str]:
    """Строки В25: скрытые финансы независимы от трёх видимых режимов."""
    fin_on = is_finance_mode(chat_id)
    hidden_on = bool(fin_on and is_hidden_finance_mode(chat_id))
    qb_on = bool(fin_on and is_quick_balance_enabled(chat_id))
    behavior = get_quick_balance_behavior(chat_id) if fin_on else "normal"
    return [
        f"Чат: {chat_button_title(chat_id)}",
        "",
        f"{'✅' if fin_on else '❌'} Фин режим",
        f"{'🙈' if hidden_on else '❌'} Скрытые финансы",
        f"{'✅🔟' if (fin_on and (not qb_on or behavior == 'normal')) else '❌'} Как обычно — окно через 10 сообщений",
        f"{'✅3️⃣' if (fin_on and qb_on and behavior == 'open') else '❌'} Быстрый остаток — открывать окно",
        f"{'✅🥇' if (fin_on and qb_on and behavior == 'first') else '❌'} Быстрый остаток — всегда первым",
    ]


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
        if is_chat_bot_removed(int_cid) and not (OWNER_ID and str(int_cid) == str(OWNER_ID)):
            continue
        icon = finance_mode_compact_icon(int_cid)
        buttons.append(IB(
            f'{icon} {chat_button_title(int_cid, title)}',
            callback_data=f"d:{day_key}:fw_finmode_pick_{int_cid}"
        ))

    add_buttons_in_rows(kb, buttons, 2)
    kb.row(IB("🔙 Назад", callback_data=f"d:{day_key}:back_main"))
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
        visible_qb = bool(is_finance_mode(int_cid) and is_quick_balance_enabled(int_cid))
        behavior = get_quick_balance_behavior(int_cid) if visible_qb else "normal"
        icon = "✅🥇" if visible_qb and behavior == "first" else ("✅3️⃣" if visible_qb and behavior == "open" else ("✅" if visible_qb else "❌"))
        buttons.append(IB(
            f'{icon} {chat_button_title(int_cid, title)}',
            callback_data=f"d:{day_key}:qb_cfg_{int_cid}"
        ))

    add_buttons_in_rows(kb, buttons, 2)

    if owner_item:
        visible_qb = bool(is_finance_mode(owner_item[0]) and is_quick_balance_enabled(owner_item[0]))
        behavior = get_quick_balance_behavior(owner_item[0]) if visible_qb else "normal"
        icon = "✅🥇" if visible_qb and behavior == "first" else ("✅3️⃣" if visible_qb and behavior == "open" else ("✅" if visible_qb else "❌"))
        kb.row(IB(
            f'{icon} {chat_button_title(owner_item[0], owner_item[1])}',
            callback_data=f"d:{day_key}:qb_cfg_{owner_item[0]}"
        ))

    kb.row(IB("🔙 Назад", callback_data=f"d:{day_key}:back_main"))
    return kb

def build_quick_balance_mode_menu(day_key: str, target_chat_id: int):
    kb = types.InlineKeyboardMarkup(row_width=1)
    fin_on = is_finance_mode(target_chat_id)
    hidden_on = bool(fin_on and is_hidden_finance_mode(target_chat_id))
    enabled = bool(fin_on and is_quick_balance_enabled(target_chat_id))
    behavior = get_quick_balance_behavior(target_chat_id) if fin_on else "normal"

    fin_icon = "✅" if fin_on else "❌"
    normal_icon = "✅🔟" if (fin_on and (not enabled or behavior == "normal")) else "❌"
    open_icon = "✅3️⃣" if (fin_on and enabled and behavior == "open") else "❌"
    first_icon = "✅🥇" if (fin_on and enabled and behavior == "first") else "❌"

    hidden_icon = "🙈" if hidden_on else "❌"
    finwin_icon = "🪟✅" if fin_on else "🪟❌"

    kb.row(IB(f"{fin_icon} Фин режим ВКЛ/ВЫКЛ", callback_data=f"d:{day_key}:fin_mode_toggle_{target_chat_id}"))
    kb.row(IB(f"{normal_icon} Как обычно — фин окно через 10 сообщений", callback_data=f"d:{day_key}:qb_mode_normal_{target_chat_id}"))
    kb.row(IB(f"{open_icon} Фин режим + быстрый остаток: открывать окно", callback_data=f"d:{day_key}:qb_mode_open_{target_chat_id}"))
    kb.row(IB(f"{first_icon} Фин режим + быстрый остаток: всегда первым", callback_data=f"d:{day_key}:qb_mode_first_{target_chat_id}"))
    kb.row(
        IB(f"{hidden_icon} Скрытые финансы", callback_data=f"d:{day_key}:qb_hidden_toggle_{target_chat_id}"),
        IB(f"{finwin_icon} Фин окно", callback_data=f"d:{day_key}:qb_finwin_open_{target_chat_id}"),
    )
    kb.row(IB("🔙 Назад к чатам", callback_data=f"d:{day_key}:forward_finmode_menu"))
    return kb

def build_finance_mode_config_menu(day_key: str, target_chat_id: int):
    """Подменю после: Фин режим → выбор чата. Объединяет финрежим и старый быстрый остаток."""
    return build_quick_balance_mode_menu(day_key, target_chat_id)


def build_finance_mode_config_text(target_chat_id: int) -> str:
    return "💰 Фин режим / В24\n" + "\n".join(finance_mode_state_lines(target_chat_id))

def build_hidden_finance_chat_menu(day_key: str):
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
        if is_chat_bot_removed(int_cid) and not (OWNER_ID and str(int_cid) == str(OWNER_ID)):
            continue
        enabled = is_hidden_finance_mode(int_cid)
        icon = "🙈" if enabled else "❌"
        buttons.append(IB(
            f"{icon} {chat_button_title(int_cid, title)}",
            callback_data=f"d:{day_key}:hf_pick_{int_cid}"
        ))

    add_buttons_in_rows(kb, buttons, 2)
    kb.row(IB("🔙 Назад", callback_data=f"d:{day_key}:back_main"))
    return kb

def build_edit_records_keyboard(day_key: str, chat_id: int, prefix: str = "d", owner_day_key: str | None = None):
    store = get_chat_store(chat_id)
    selected = set(int(x) for x in (store.get("edit_delete_selected", {}) or {}).get(day_key, []))
    kb = types.InlineKeyboardMarkup(row_width=3)
    day_recs = store.get("daily_records", {}).get(day_key, [])
    for r in day_recs:
        rid = int(r["id"])
        lbl = f" {fmt_num(r['amount'])}"
        del_icon = "☑️" if rid in selected else "❌"
        if prefix == "fv":
            # Кнопка ✏️ сразу вставляет подготовленный текст в поле ввода владельца.
            # Старый callback edit_rec_* оставлен ниже в обработчике для совместимости со старыми окнами.
            del_cb = f"fv:{chat_id}:{day_key}:del_toggle_{rid}:{owner_day_key or today_key()}"
        else:
            del_cb = f"d:{day_key}:del_toggle_{rid}"
        insert_text = compose_direct_edit_insert_value(chat_id, rid, day_key, r.get("amount", 0), r.get("note", ""))
        kb.row(
            IB(lbl, callback_data="none"),
            make_direct_edit_insert_button("✏️", insert_text),
            IB(del_icon, callback_data=del_cb)
        )

    if selected:
        if prefix == "fv":
            kb.row(IB("🗑 Удалить выбранное", callback_data=f"fv:{chat_id}:{day_key}:del_selected:{owner_day_key or today_key()}"))
        else:
            kb.row(IB("🗑 Удалить выбранное", callback_data=f"d:{day_key}:del_selected"))

    if prefix == "fv":
        kb.row(IB("🔙 Назад", callback_data=f"fv:{chat_id}:{day_key}:clear_delete_back:{owner_day_key or today_key()}"))
    else:
        kb.row(IB("🔙 Назад", callback_data=f"d:{day_key}:back_main"))
    return kb
def toggle_edit_delete_selection(chat_id: int, day_key: str, rid: int):
    store = get_chat_store(chat_id)
    all_sel = store.setdefault("edit_delete_selected", {})
    selected = set(int(x) for x in all_sel.get(day_key, []))
    rid = int(rid)
    if rid in selected:
        selected.remove(rid)
    else:
        selected.add(rid)
    if selected:
        all_sel[day_key] = sorted(selected)
    else:
        all_sel.pop(day_key, None)
    save_data(data)


def clear_edit_delete_selection(chat_id: int, day_key: str | None = None):
    store = get_chat_store(chat_id)
    all_sel = store.setdefault("edit_delete_selected", {})
    if day_key is None:
        all_sel.clear()
    else:
        all_sel.pop(day_key, None)
    save_data(data)


def update_record_in_chat(chat_id: int, rid: int, amount: float, note: str) -> bool:
    """v27: только меняет данные. Окна/бэкапы делает finance_changed()."""
    bot_journal("record_update_start", chat_id, f"rid={rid} amount={amount} note={note}")
    store = get_chat_store(chat_id)
    target = next((r for r in store.get("records", []) if int(r.get("id", -1)) == int(rid)), None)
    if not target:
        return False
    target["amount"] = amount
    target["note"] = note
    for dk, arr in (store.get("daily_records", {}) or {}).items():
        for r in arr:
            if int(r.get("id", -1)) == int(rid):
                r["amount"] = amount
                r["note"] = note
    recalc_balance(chat_id)
    rebuild_month_short_ids(chat_id)
    rebuild_global_records()
    save_data(data)
    return True


def delete_selected_records(chat_id: int, day_key: str) -> int:
    with locked_chat(chat_id):
        """Удаляет все отмеченные ☑️ записи одним проходом, без ошибки из-за перенумерации id."""
        store = get_chat_store(chat_id)
        all_sel = store.setdefault("edit_delete_selected", {})
        selected = {int(x) for x in all_sel.get(day_key, [])}
        if not selected:
            return 0

        before = len(store.get("records", []) or [])
        store["records"] = [r for r in (store.get("records", []) or []) if int(r.get("id", -1)) not in selected]

        daily = store.get("daily_records", {}) or {}
        for dk in list(daily.keys()):
            arr = daily.get(dk, []) or []
            arr2 = [r for r in arr if int(r.get("id", -1)) not in selected]
            if arr2:
                daily[dk] = arr2
            else:
                daily.pop(dk, None)

        deleted = before - len(store.get("records", []) or [])
        all_sel.pop(day_key, None)

        renumber_chat_records(chat_id)
        recalc_balance(chat_id)
        rebuild_global_records()
        save_data(data)
        finance_changed(chat_id, day_key, reason="delete_selected", delay=0.1)
        return deleted


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
        if is_chat_bot_removed(int_cid) and not (OWNER_ID and str(int_cid) == str(OWNER_ID)):
            continue
        items.append((int_cid, get_chat_display_name(int_cid)))

    buttons = [
        IB(chat_button_title(cid, title), callback_data=f"d:{day_key}:finwin_open_{cid}")
        for cid, title in sorted(items, key=lambda x: x[1].lower())
    ]

    if buttons:
        add_buttons_in_rows(kb, buttons, 2)
    else:
        kb.row(IB("Нет чатов с финрежимом", callback_data="none"))

    kb.row(IB("🔙 Назад", callback_data=f"d:{day_key}:back_main"))
    return kb

def build_fin_window_view_keyboard(target_chat_id: int, day_key: str, owner_day_key: str):
    kb = types.InlineKeyboardMarkup(row_width=3)

    prev_day = (datetime.strptime(day_key, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
    next_day = (datetime.strptime(day_key, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    nav_row = [IB("⬅️ Вчера", callback_data=f"fv:{target_chat_id}:{prev_day}:open:{owner_day_key}")]
    if day_key != today_key():
        nav_row.append(IB("📅 Сегодня", callback_data=f"fv:{target_chat_id}:{today_key()}:open:{owner_day_key}"))
    nav_row.append(IB("➡️ Завтра", callback_data=f"fv:{target_chat_id}:{next_day}:open:{owner_day_key}"))
    kb.row(*nav_row)

    kb.row(
        IB("📝 Редактировать", callback_data=f"fv:{target_chat_id}:{day_key}:edit_list:{owner_day_key}"),
        IB("📂 CSV", callback_data=f"fv:{target_chat_id}:{day_key}:csv_menu:{owner_day_key}"),
        IB("📊 Статьи", callback_data=fvcat_callback(f"fvcat_today:{target_chat_id}:{owner_day_key}")),
    )
    kb.row(
        IB("📅 Календарь", callback_data=f"fv:{target_chat_id}:{day_key}:calendar:{owner_day_key}"),
        IB("📊 Отчёт", callback_data=f"fv:{target_chat_id}:{day_key}:report:{owner_day_key}"),
        IB("💰 Общий итог", callback_data=f"fv:{target_chat_id}:{day_key}:total:{owner_day_key}"),
    )
    kb.row(
        IB("⚙️ Обнулить", callback_data=f"fv:{target_chat_id}:{day_key}:reset:{owner_day_key}"),
        IB("ℹ️ Инфо", callback_data=f"fv:{target_chat_id}:{day_key}:info:{owner_day_key}"),
        IB("🔙 Назад к списку", callback_data=f"d:{owner_day_key}:fin_windows_menu"),
    )
    return kb

def build_fin_window_menu_keyboard(target_chat_id: int, day_key: str, owner_day_key: str):
    """Совместимость: отдельного меню больше нет."""
    return build_fin_window_view_keyboard(target_chat_id, day_key, owner_day_key)

def build_fin_window_csv_menu(target_chat_id: int, day_key: str, owner_day_key: str):
    kb = types.InlineKeyboardMarkup(row_width=3)
    _add_export_period_rows(kb, day_key, "fv", owner_day_key=owner_day_key, target_chat_id=target_chat_id)
    kb.row(
        IB(_backup_toggle_label(target_chat_id, "chat", "Бэкап в чат"), callback_data=f"fv:{target_chat_id}:{day_key}:bk_chat:{owner_day_key}"),
        IB(_backup_toggle_label(target_chat_id, "channel", "в канал"), callback_data=f"fv:{target_chat_id}:{day_key}:bk_channel:{owner_day_key}"),
        IB(_backup_toggle_label(target_chat_id, "mega", "в MEGA"), callback_data=f"fv:{target_chat_id}:{day_key}:bk_mega:{owner_day_key}"),
    )
    kb.row(IB("🔙 Назад", callback_data=f"fv:{target_chat_id}:{day_key}:open:{owner_day_key}"))
    return kb


def send_csv_for_chat_to(recipient_chat_id: int, target_chat_id: int, mode: str, day_key: str):
    """Отправляет CSV владельцу, но данные берёт из target_chat_id."""
    try:
        store = get_chat_store(target_chat_id)
        rows = []
        caption = f"📂 CSV: {get_chat_display_name(target_chat_id)}"
        if mode == "all":
            save_chat_json(target_chat_id)
            path = chat_csv_file(target_chat_id)
            if os.path.exists(path):
                fobj = file_bytesio_named(path, export_display_filename(target_chat_id, mode, day_key, "csv"))
                if fobj:
                    _tg_call_retry(bot.send_document, recipient_chat_id, fobj, caption=caption, purpose="send_csv_for_chat_to")
                return
        elif mode == "day":
            for r in store.get("daily_records", {}).get(day_key, []) or []:
                rows.append((fmt_date_backup(day_key), fmt_csv_amount(r.get("amount")), r.get("note", "")))
            caption = f"📅 CSV за день {fmt_date_backup(day_key)}: {get_chat_display_name(target_chat_id)}"
        elif mode == "week":
            base = datetime.strptime(day_key, "%Y-%m-%d")
            start = base - timedelta(days=6)
            for i in range(7):
                dk = (start + timedelta(days=i)).strftime("%Y-%m-%d")
                for r in store.get("daily_records", {}).get(dk, []) or []:
                    rows.append((fmt_date_backup(dk), fmt_csv_amount(r.get("amount")), r.get("note", "")))
            caption = f"🗓 CSV за неделю: {get_chat_display_name(target_chat_id)}"
        elif mode == "month":
            base = datetime.strptime(day_key, "%Y-%m-%d")
            start = base.replace(day=1)
            for dk, recs in (store.get("daily_records", {}) or {}).items():
                try:
                    dt = datetime.strptime(dk, "%Y-%m-%d")
                except Exception:
                    continue
                if start <= dt <= base:
                    for r in recs or []:
                        rows.append((fmt_date_backup(dk), fmt_csv_amount(r.get("amount")), r.get("note", "")))
            caption = f"📆 CSV за месяц: {get_chat_display_name(target_chat_id)}"
        elif mode == "wedthu":
            base = datetime.strptime(day_key, "%Y-%m-%d")
            while base.weekday() != 2:
                base -= timedelta(days=1)
            for i in range(2):
                dk = (base + timedelta(days=i)).strftime("%Y-%m-%d")
                for r in store.get("daily_records", {}).get(dk, []) or []:
                    rows.append((fmt_date_backup(dk), fmt_csv_amount(r.get("amount")), r.get("note", "")))
            caption = f"📊 CSV Ср–Чт: {get_chat_display_name(target_chat_id)}"

        if not rows:
            send_and_auto_delete(recipient_chat_id, "Нет данных для CSV.", 8)
            return
        tmp_name = os.path.join(MEGA_LOCAL_TMP_DIR, f"fv_csv_{target_chat_id}_{mode}_{int(time.time())}.csv")
        with open(tmp_name, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["date", "amount", "note"])
            write_csv_rows_with_day_gaps(w, rows, 3)
        fobj = file_bytesio_named(tmp_name, export_display_filename(target_chat_id, mode, day_key, "csv"))
        if fobj:
            _tg_call_retry(bot.send_document, recipient_chat_id, fobj, caption=caption, purpose="send_csv_for_chat_to")
        try:
            os.remove(tmp_name)
        except Exception:
            pass
    except Exception as e:
        log_error(f"send_csv_for_chat_to({get_chat_display_name(target_chat_id)}): {e}")




def _period_export_rows(chat_id: int, mode: str, day_key: str):
    """Возвращает rows для CSV/XLSX по периоду: date, amount, note."""
    store = get_chat_store(chat_id)
    mode = str(mode or "all").replace("csv_", "").replace("xlsx_", "")
    if mode == "all_real":
        mode = "all"
    rows = []

    def _append_day(dk: str):
        for r in sorted(store.get("daily_records", {}).get(dk, []) or [], key=record_sort_key):
            rows.append((fmt_date_backup(dk), fmt_csv_amount(r.get("amount")), r.get("note", "")))

    if mode == "day":
        _append_day(day_key)
        label = f"за день {fmt_date_backup(day_key)}"
    elif mode == "week":
        base = datetime.strptime(day_key, "%Y-%m-%d")
        start = base - timedelta(days=6)
        for i in range(7):
            _append_day((start + timedelta(days=i)).strftime("%Y-%m-%d"))
        label = "за неделю"
    elif mode == "month":
        base = datetime.strptime(day_key, "%Y-%m-%d")
        start = base.replace(day=1)
        for dk in sorted((store.get("daily_records", {}) or {}).keys()):
            try:
                dt = datetime.strptime(dk, "%Y-%m-%d")
            except Exception:
                continue
            if start <= dt <= base:
                _append_day(dk)
        label = "за месяц"
    elif mode == "wedthu":
        base = datetime.strptime(day_key, "%Y-%m-%d")
        while base.weekday() != 2:
            base -= timedelta(days=1)
        for i in range(2):
            _append_day((base + timedelta(days=i)).strftime("%Y-%m-%d"))
        label = "Ср–Чт"
    else:
        for dk in sorted((store.get("daily_records", {}) or {}).keys()):
            _append_day(dk)
        label = "за всё время"

    return rows, label


def send_export_for_chat_to(recipient_chat_id: int, target_chat_id: int, mode: str, day_key: str, file_type: str = "csv"):
    """Отправка CSV или Excel по выбранному периоду. Работает для обычного чата и для меню владельца по чужому чату."""
    trace = ProcessTrace(recipient_chat_id, f"Экспорт {str(file_type).upper()}: {get_chat_display_name(target_chat_id)}").start()
    try:
        trace.step("читает режим периода")
        file_type = str(file_type or "csv").lower().lstrip(".")
        mode = str(mode or "all").replace("csv_", "").replace("xlsx_", "")
        if mode == "all_real":
            mode = "all"

        if mode == "all":
            trace.step("экспорт за всё время — обновляет локальные файлы")
            save_chat_json(target_chat_id)
            path = chat_xlsx_file(target_chat_id) if file_type == "xlsx" else chat_csv_file(target_chat_id)
            label = "за всё время"
            if os.path.exists(path):
                trace.step("отправляет готовый файл в Telegram")
                fobj = file_bytesio_named(path, export_display_filename(target_chat_id, mode, day_key, "xlsx" if file_type == "xlsx" else "csv"))
                if fobj:
                    _tg_call_retry(
                        bot.send_document,
                        recipient_chat_id,
                        fobj,
                        caption=f"📂 {'Excel' if file_type == 'xlsx' else 'CSV'} {label}: {get_chat_display_name(target_chat_id)}",
                        purpose="export_send_document"
                    )
                trace.finish("экспорт завершён")
                return

        trace.step("собирает строки за выбранный период")
        rows, label = _period_export_rows(target_chat_id, mode, day_key)
        ext = "xlsx" if file_type == "xlsx" else "csv"
        if not rows and ext != "xlsx":
            trace.step("строк нет — отправляет уведомление")
            send_info(recipient_chat_id, f"Нет данных {label}.")
            trace.finish("экспорт завершён без данных")
            return
        if not rows and ext == "xlsx":
            trace.step("строк нет — создаёт пустой Excel с заголовками")
        tmp_name = os.path.join(MEGA_LOCAL_TMP_DIR, f"export_{target_chat_id}_{mode}_{int(time.time() * 1000)}.{ext}")
        display_name = export_display_filename(target_chat_id, mode, day_key, ext)
        if ext == "xlsx":
            trace.step("создаёт временный Excel файл")
            xlsx_rows = [["Дата", "Описание", "Приход", "Расход"]]
            for date_v, amount_v, note_v in rows:
                try:
                    parsed_amount = parse_csv_amount(amount_v)
                except Exception as e_amount:
                    log_error(f"xlsx export amount parse skip: chat={get_chat_display_name(target_chat_id)} amount={amount_v!r} note={note_v!r}: {e_amount}")
                    parsed_amount = 0.0
                xlsx_rows.append(_xlsx_record_row(date_v, parsed_amount, note_v))
            _write_simple_xlsx(tmp_name, xlsx_rows, sheet_name="Экспорт")
        else:
            trace.step("создаёт временный CSV файл")
            with open(tmp_name, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(["date", "amount", "note"])
                write_csv_rows_with_day_gaps(w, rows, 3)

        trace.step("отправляет файл в Telegram")
        fobj = file_bytesio_named(tmp_name, display_name)
        if fobj:
            _tg_call_retry(
                bot.send_document,
                recipient_chat_id,
                fobj,
                caption=f"📂 {'Excel' if ext == 'xlsx' else 'CSV'} {label}: {get_chat_display_name(target_chat_id)}",
                purpose="export_send_document"
            )
        trace.step("удаляет временный файл")
        try:
            os.remove(tmp_name)
        except Exception:
            pass
        trace.finish("экспорт завершён")
    except Exception as e:
        trace.fail(e)
        log_error(f"send_export_for_chat_to({get_chat_display_name(target_chat_id)}): {e}")

def build_fin_categories_summary_keyboard(target_chat_id: int, mode: str, start: str, end: str, owner_day_key: str):
    store = get_chat_store(target_chat_id)
    kb = types.InlineKeyboardMarkup(row_width=3)
    buttons = []
    for cat in get_ordered_category_names(include_all=True, store=store):
        slug = get_expense_category_slug(cat, store)
        if slug:
            buttons.append(IB(cat, callback_data=fvcat_callback(f"fvcat_show:{target_chat_id}:{start}:{end}:{slug}:{owner_day_key}")))
    add_buttons_in_rows(kb, buttons, 3)
    if mode == "wthu":
        prev_key = (datetime.strptime(start, "%Y-%m-%d") - timedelta(days=7)).strftime("%Y-%m-%d")
        next_key = (datetime.strptime(start, "%Y-%m-%d") + timedelta(days=7)).strftime("%Y-%m-%d")
        kb.row(
            IB("⬅️ Чт–Ср", callback_data=fvcat_callback(f"fvcat_wthu:{target_chat_id}:{prev_key}:{owner_day_key}")),
            IB("📅 Сегодня", callback_data=fvcat_callback(f"fvcat_today:{target_chat_id}:{owner_day_key}")),
            IB("Чт–Ср ➡️", callback_data=fvcat_callback(f"fvcat_wthu:{target_chat_id}:{next_key}:{owner_day_key}")),
        )
    kb.row(IB("📚 Описание статей", callback_data=fvcat_callback(f"fvcat_desc:{target_chat_id}:{start}:{owner_day_key}")))
    kb.row(
        IB("➕ Добавить статью", callback_data=fvcat_callback(f"fvcat_add:{target_chat_id}:{start}:{owner_day_key}")),
        IB("✏️ Изменить статью", callback_data=fvcat_callback(f"fvcat_edit_menu:{target_chat_id}:{start}:{owner_day_key}")),
    )
    kb.row(IB("🗑 Удалить статью", callback_data=fvcat_callback(f"fvcat_del_menu:{target_chat_id}:{start}:{owner_day_key}")))
    kb.row(
        IB("⏪ Назад осн. окно", callback_data=f"fv:{target_chat_id}:{start}:open:{owner_day_key}"),
        IB("❌ Закрыть статьи", callback_data=f"fv:{target_chat_id}:{start}:open:{owner_day_key}"),
    )
    return kb


def build_fin_category_edit_keyboard(target_chat_id: int, ref: str, owner_day_key: str):
    kb = types.InlineKeyboardMarkup(row_width=2)
    items = category_custom_items_for_chat(target_chat_id)
    if not items:
        kb.row(IB("Нет пользовательских статей", callback_data="none"))
    for item in items:
        kb.row(IB(f"✏️ {item.get('name')}", callback_data=fvcat_callback(f"fvcat_edit_pick:{target_chat_id}:{item.get('slug')}:{owner_day_key}")))
    kb.row(IB("🔙 Назад к статьям", callback_data=fvcat_callback(f"fvcat_wthu:{target_chat_id}:{ref}:{owner_day_key}")))
    kb.row(IB("⬅️ Назад осн. окно", callback_data=f"fv:{target_chat_id}:{ref}:open:{owner_day_key}"))
    return kb


def build_fin_category_delete_keyboard(target_chat_id: int, ref: str, owner_day_key: str):
    store = get_chat_store(target_chat_id)
    selected = set(store.get("category_delete_selection") or [])
    kb = types.InlineKeyboardMarkup(row_width=2)
    items = category_custom_items_for_chat(target_chat_id)
    if not items:
        kb.row(IB("Нет пользовательских статей", callback_data="none"))
    for item in items:
        slug = item.get("slug")
        icon = "☑️" if slug in selected else "⬛"
        kb.row(IB(f"{icon} {item.get('name')}", callback_data=fvcat_callback(f"fvcat_del_toggle:{target_chat_id}:{slug}:{ref}:{owner_day_key}")))
    kb.row(IB("🗑 Удалить выбранное", callback_data=fvcat_callback(f"fvcat_del_selected:{target_chat_id}:{ref}:{owner_day_key}")))
    kb.row(IB("🔙 Назад к статьям", callback_data=fvcat_callback(f"fvcat_wthu:{target_chat_id}:{ref}:{owner_day_key}")))
    kb.row(IB("⬅️ Назад осн. окно", callback_data=f"fv:{target_chat_id}:{ref}:open:{owner_day_key}"))
    return kb

def handle_finwindow_categories_callback(call, data_str: str) -> bool:
    if not data_str.startswith("fvcat_"):
        return False
    owner_chat_id = call.message.chat.id
    if not is_owner_chat(owner_chat_id):
        return True
    try:
        parts = data_str.split(":")
        action = parts[0]
        target_chat_id = int(parts[1])
    except Exception:
        return True
    store = get_chat_store(target_chat_id)
    if action == "fvcat_today":
        owner_day_key = parts[2] if len(parts) > 2 else today_key()
        return handle_finwindow_categories_callback(call, f"fvcat_wthu:{target_chat_id}:{today_key()}:{owner_day_key}")
    if action == "fvcat_desc":
        ref = parts[2] if len(parts) > 2 else today_key()
        owner_day_key = parts[3] if len(parts) > 3 else today_key()
        kb = types.InlineKeyboardMarkup()
        kb.row(IB("🔙 Назад к статьям", callback_data=fvcat_callback(f"fvcat_wthu:{target_chat_id}:{ref}:{owner_day_key}")))
        kb.row(IB("🔙 К окну чата", callback_data=f"fv:{target_chat_id}:{ref}:open:{owner_day_key}"))
        safe_edit(bot, call, f"👁 {get_chat_display_name(target_chat_id)}\n" + build_articles_description_text(target_chat_id), reply_markup=kb, parse_mode=None)
        return True

    if action == "fvcat_add":
        try:
            owner_day_key = parts[3] if len(parts) > 3 else today_key()
        except Exception:
            owner_day_key = today_key()
        start_category_add_wait(owner_chat_id, target_chat_id, owner_day_key=owner_day_key)
        try:
            bot.answer_callback_query(call.id, "Напиши название и ключи статьи", show_alert=False)
        except Exception:
            pass
        return True

    if action == "fvcat_edit_menu":
        ref = parts[2] if len(parts) > 2 else today_key()
        owner_day_key = parts[3] if len(parts) > 3 else today_key()
        safe_edit(
            bot, call,
            wm_owner(f"✏️ Изменить статью\n👁 {get_chat_display_name(target_chat_id)}\n\nВыберите пользовательскую статью. Стандартные статьи не меняем.", 18),
            reply_markup=build_fin_category_edit_keyboard(target_chat_id, ref, owner_day_key)
        )
        return True

    if action == "fvcat_edit_pick":
        try:
            target_chat_id = int(parts[1])
            slug = parts[2]
            owner_day_key = parts[3] if len(parts) > 3 else today_key()
        except Exception:
            return True
        start_category_edit_wait(owner_chat_id, target_chat_id, slug)
        try:
            bot.answer_callback_query(call.id, "Напиши новую статью и ключи", show_alert=False)
        except Exception:
            pass
        return True

    if action == "fvcat_del_menu":
        clear_category_wait_state(owner_chat_id, "category_add_wait", delete_prompt=False)
        clear_category_wait_state(owner_chat_id, "category_edit_wait", delete_prompt=False)
        ref = parts[2] if len(parts) > 2 else today_key()
        owner_day_key = parts[3] if len(parts) > 3 else today_key()
        get_chat_store(target_chat_id)["category_delete_selection"] = []
        save_data(data)
        safe_edit(
            bot, call,
            wm_owner(f"🗑 Удалить статью\n👁 {get_chat_display_name(target_chat_id)}\n\nВыберите пользовательские статьи галочками.", 19),
            reply_markup=build_fin_category_delete_keyboard(target_chat_id, ref, owner_day_key)
        )
        return True

    if action == "fvcat_del_toggle":
        try:
            target_chat_id = int(parts[1])
            slug = parts[2]
            ref = parts[3] if len(parts) > 3 else today_key()
            owner_day_key = parts[4] if len(parts) > 4 else today_key()
        except Exception:
            return True
        tstore = get_chat_store(target_chat_id)
        selected = set(tstore.get("category_delete_selection") or [])
        if slug in selected:
            selected.remove(slug)
        else:
            selected.add(slug)
        tstore["category_delete_selection"] = sorted(selected)
        save_data(data)
        safe_edit(
            bot, call,
            wm_owner(f"🗑 Удалить статью\n👁 {get_chat_display_name(target_chat_id)}\n\nВыберите пользовательские статьи галочками.", 19),
            reply_markup=build_fin_category_delete_keyboard(target_chat_id, ref, owner_day_key)
        )
        return True

    if action == "fvcat_del_selected":
        try:
            target_chat_id = int(parts[1])
            ref = parts[2] if len(parts) > 2 else today_key()
            owner_day_key = parts[3] if len(parts) > 3 else today_key()
        except Exception:
            return True
        selected = set(get_chat_store(target_chat_id).get("category_delete_selection") or [])
        if not selected:
            try:
                bot.answer_callback_query(call.id, "Ничего не выбрано", show_alert=False)
            except Exception:
                pass
            return True
        count = remove_custom_expense_categories(target_chat_id, selected)
        try:
            bot.answer_callback_query(call.id, f"Удалено статей: {count}", show_alert=False)
        except Exception:
            pass
        return handle_finwindow_categories_callback(call, f"fvcat_wthu:{target_chat_id}:{ref}:{owner_day_key}")

    if action == "fvcat_wthu":
        ref = parts[2] if len(parts) > 2 else today_key()
        owner_day_key = parts[3] if len(parts) > 3 else today_key()
        start_key = week_start_thursday(ref)
        start, end = week_bounds_thu_wed(start_key)
        label = f"{fmt_date_ddmmyy(start)} — {fmt_date_ddmmyy(end)} (Чт–Ср)"
        text, _ = summarize_categories(store, start, end, label)
        text = f"👁 {get_chat_display_name(target_chat_id)}\n" + text
        safe_edit(bot, call, text, reply_markup=build_fin_categories_summary_keyboard(target_chat_id, "wthu", start, end, owner_day_key), parse_mode=None)
        return True
    if action == "fvcat_show":
        try:
            _, target_s, start, end, slug, owner_day_key = data_str.split(":", 5)
            target_chat_id = int(target_s)
        except Exception:
            return True
        category = get_category_by_slug(slug, store)
        if not category:
            return True
        label = f"{fmt_date_ddmmyy(start)} — {fmt_date_ddmmyy(end)}"
        text = f"👁 {get_chat_display_name(target_chat_id)}\n" + build_category_detail_text(store, start, end, category, label)
        kb = build_fin_categories_summary_keyboard(target_chat_id, "detail", start, end, owner_day_key)
        kb.row(IB("🔙 Назад", callback_data=fvcat_callback(f"fvcat_wthu:{target_chat_id}:{start}:{owner_day_key}")))
        kb.row(IB("🔙 К окну чата", callback_data=f"fv:{target_chat_id}:{start}:open:{owner_day_key}"))
        safe_edit(bot, call, text, reply_markup=kb, parse_mode=None)
        return True
    return True


def render_fin_window_text(target_chat_id: int, day_key: str):
    txt, _ = render_day_window(target_chat_id, day_key)
    return wm_owner(f"👁 {html.escape(get_chat_display_name(target_chat_id))}\n\n{txt}", 6, html_mode=True)


def build_fin_calendar_keyboard(target_chat_id: int, center_day: datetime, owner_day_key: str):
    kb = types.InlineKeyboardMarkup(row_width=7)
    store = get_chat_store(target_chat_id)
    daily = store.get("daily_records", {})

    kb.row(*[IB(x, callback_data="none") for x in ("Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс")])
    for week in calendar.Calendar(firstweekday=0).monthdayscalendar(center_day.year, center_day.month):
        row = []
        for day_num in week:
            if not day_num:
                row.append(IB(" ", callback_data="none"))
                continue
            key = f"{center_day.year:04d}-{center_day.month:02d}-{day_num:02d}"
            label = f"📝{day_num}" if daily.get(key) else str(day_num)
            row.append(IB(label, callback_data=f"fv:{target_chat_id}:{key}:open:{owner_day_key}"))
        kb.row(*row)

    prev_month = (center_day.replace(day=1) - timedelta(days=1)).replace(day=1)
    next_month = (center_day.replace(day=28) + timedelta(days=4)).replace(day=1)
    kb.row(
        IB("⬅️ Месяц", callback_data=f"fc:{target_chat_id}:{prev_month.strftime('%Y-%m-%d')}:{owner_day_key}"),
        IB("➡️ Месяц", callback_data=f"fc:{target_chat_id}:{next_month.strftime('%Y-%m-%d')}:{owner_day_key}")
    )
    row = []
    if center_day.strftime("%Y-%m") != now_local().strftime("%Y-%m"):
        row.append(IB("📅 Сегодня", callback_data=f"fc:{target_chat_id}:{today_key()}:{owner_day_key}"))
    row.append(IB("🔙 Назад", callback_data=f"fv:{target_chat_id}:{store.get('current_view_day', today_key())}:open:{owner_day_key}"))
    kb.row(*row)
    return kb


def build_forward_mode_menu(A: int, B: int):
    """
    Меню выбора режима пересылки между чатами A и B.
    """
    kb = types.InlineKeyboardMarkup()

    name_a = chat_button_title(A)
    name_b = chat_button_title(B)

    fr = data.get("forward_rules", {}) or {}
    ab_link = str(B) in fr.get(str(A), {})
    ba_link = str(A) in fr.get(str(B), {})
    two_on = ab_link and ba_link

    ab_state = "ВКЛ ✅" if ab_link else "ВЫКЛ ❌"
    ba_state = "ВКЛ ✅" if ba_link else "ВЫКЛ ❌"
    two_state = "ВКЛ ✅" if two_on else "ВЫКЛ ❌"

    ab_fin = "ВКЛ ✅" if get_forward_finance(A, B) else "ВЫКЛ ❌"
    ba_fin = "ВКЛ ✅" if get_forward_finance(B, A) else "ВЫКЛ ❌"

    kb.row(IB(
        f"➡️ {ab_state} {name_a} → {name_b}",
        callback_data=f"fw_mode:{A}:{B}:to"
    ))
    kb.row(IB(
        f"⬅️ {ba_state} {name_b} → {name_a}",
        callback_data=f"fw_mode:{A}:{B}:from"
    ))
    kb.row(IB(
        f"↔️ {two_state} {name_a} ⇄ {name_b}",
        callback_data=f"fw_mode:{A}:{B}:two"
    ))
    kb.row(IB(
        f"💰 {ab_fin} Учёт {name_a} → {name_b}",
        callback_data=f"fw_finpair:{A}:{B}:ab"
    ))
    kb.row(IB(
        f"💰 {ba_fin} Учёт {name_b} → {name_a}",
        callback_data=f"fw_finpair:{A}:{B}:ba"
    ))
    kb.row(IB(
        "❌ Удалить все связи A-B",
        callback_data=f"fw_mode:{A}:{B}:del"
    ))
    kb.row(IB(
        "🔙 Назад",
        callback_data=f"fw_back_tgt:{A}"
    ))
    return kb


def _one_button_keyboard(label: str, callback_data: str):
    kb = types.InlineKeyboardMarkup()
    kb.row(IB(label, callback_data=callback_data))
    return kb

def safe_edit(bot, call, text, reply_markup=None, parse_mode=None):
    """Безопасное обновление: edit_text → edit_caption → send_message.
    Все окна, которые открываются кнопками, получают метку о*/в*, если она ещё не проставлена вручную.
    """
    chat_id = call.message.chat.id
    msg_id = call.message.message_id
    try:
        text = auto_window_mark(
            text,
            getattr(call, "data", ""),
            owner_chat=is_owner_chat(chat_id),
            html_mode=(str(parse_mode or "").upper() == "HTML")
        )
    except Exception:
        pass
    if reply_markup is None:
        try:
            reply_markup = default_window_nav_keyboard(chat_id)
        except Exception:
            pass
    try:
        _tg_call_retry(
            bot.edit_message_text,
            text,
            chat_id=chat_id,
            message_id=msg_id,
            reply_markup=reply_markup,
            parse_mode=parse_mode,
            purpose="safe_edit_text"
        )
        _touch_v98_auto_close_for_callback(chat_id, msg_id, getattr(call, "data", ""))
        return
    except Exception:
        pass
    try:
        _tg_call_retry(
            bot.edit_message_caption,
            chat_id=chat_id,
            message_id=msg_id,
            caption=text,
            reply_markup=reply_markup,
            parse_mode=parse_mode,
            purpose="safe_edit_caption"
        )
        _touch_v98_auto_close_for_callback(chat_id, msg_id, getattr(call, "data", ""))
        return
    except Exception:
        pass
    try:
        if chat_buttons_current_window_enabled(chat_id):
            try:
                bot.answer_callback_query(call.id, "Режим текущего окна включён: новое окно не создаю.", show_alert=False)
            except Exception:
                pass
            return
    except Exception:
        pass
    sent = _tg_call_retry(bot.send_message, chat_id, text, reply_markup=reply_markup, parse_mode=parse_mode, purpose="safe_edit_send_fallback")
    try:
        _touch_v98_auto_close_for_callback(chat_id, sent.message_id, getattr(call, "data", ""))
    except Exception:
        pass


def safe_edit_current_only(bot, call, text, reply_markup=None, parse_mode=None):
    """Редактирует только текущее окно.
    Используется для долгих действий вроде «Проверить чаты», чтобы при ошибке edit
    не плодить новое окно через send_message.
    """
    chat_id = call.message.chat.id
    msg_id = call.message.message_id
    try:
        text = auto_window_mark(
            text,
            getattr(call, "data", ""),
            owner_chat=is_owner_chat(chat_id),
            html_mode=(str(parse_mode or "").upper() == "HTML")
        )
    except Exception:
        pass
    if reply_markup is None:
        try:
            reply_markup = default_window_nav_keyboard(chat_id)
        except Exception:
            pass
    try:
        _tg_call_retry(
            bot.edit_message_text,
            text,
            chat_id=chat_id,
            message_id=msg_id,
            reply_markup=reply_markup,
            parse_mode=parse_mode,
            purpose="safe_edit_current_only_text"
        )
        _touch_v98_auto_close_for_callback(chat_id, msg_id, getattr(call, "data", ""))
        return True
    except Exception as e1:
        if "message is not modified" in str(e1).lower():
            return True
    try:
        _tg_call_retry(
            bot.edit_message_caption,
            chat_id=chat_id,
            message_id=msg_id,
            caption=text,
            reply_markup=reply_markup,
            parse_mode=parse_mode,
            purpose="safe_edit_current_only_caption"
        )
        _touch_v98_auto_close_for_callback(chat_id, msg_id, getattr(call, "data", ""))
        return True
    except Exception as e2:
        if "message is not modified" in str(e2).lower():
            return True
        try:
            bot.answer_callback_query(call.id, "Окно не удалось обновить, но новое окно не создаю.", show_alert=False)
        except Exception:
            pass
        return False

def send_or_edit_categories_window(chat_id, text, reply_markup=None, parse_mode=None, preferred_message_id=None):
    """Отдельное окно для отчёта по статьям расходов (одно сообщение на чат)."""
    try:
        text = window_mark(text, "о7", html_mode=(str(parse_mode or "").upper() == "HTML"))
    except Exception:
        pass
    store = get_chat_store(chat_id)
    mid = store.get("categories_msg_id")

    candidates = []
    # Сначала пробуем текущее сообщение, из которого нажали кнопку «Статьи».
    # Так окно открывается явно в том же месте и не теряется среди сообщений.
    if preferred_message_id is not None:
        try:
            pref_int = int(preferred_message_id)
            candidates.append(pref_int)
        except Exception:
            pass
    if mid:
        try:
            mid_int = int(mid)
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
            return target_id
        except Exception as e:
            if "message is not modified" in str(e).lower():
                store["categories_msg_id"] = target_id
                save_data(data)
                return target_id
            log_error(f"send_or_edit_categories_window edit failed {chat_id}:{target_id}: {e}")
            if store.get("categories_msg_id") == target_id:
                store["categories_msg_id"] = None
                save_data(data)

    sent = bot.send_message(chat_id, text, reply_markup=reply_markup, parse_mode=parse_mode)
    store["categories_msg_id"] = sent.message_id
    save_data(data)
    return sent.message_id

def open_report_window(chat_id: int, month_key: str = None, message_id: int = None):
    """
    Открывает или обновляет отдельное окно отчёта без размножения сообщений.
    """
    text, month_key = build_month_report_text(chat_id, month_key)
    kb = build_report_keyboard(month_key)

    store = get_chat_store(chat_id)
    if message_id and not store.get("report_window_id"):
        store["report_window_id"] = message_id

    final_id = send_or_edit_stored_window(
        chat_id,
        "report_window_id",
        text,
        reply_markup=kb,
        parse_mode="HTML",
        delay=AUX_WINDOW_DELETE_DELAY
    )
    store["report_window_id"] = final_id
    store["report_month"] = month_key
    save_data(data)


def build_info_keyboard(chat_id: int):
    kb = types.InlineKeyboardMarkup()
    if is_owner_chat(chat_id):
        kb.row(
            IB("📓 Журнал", callback_data="journal_open"),
            IB(journal_toggle_label(), callback_data="journal_toggle"),
        )
        kb.row(
            IB(buttons_current_window_label(), callback_data="buttons_current_toggle"),
            IB(info_finance_toggle_label(chat_id), callback_data="info_finance_off"),
        )
        kb.row(
            IB(forward_menu_style_label(), callback_data="forward_menu_style_toggle"),
            IB(icon_button_mode_label(), callback_data="icon_buttons_toggle"),
        )
        kb.row(IB(total_secret_mask_label(), callback_data="total_secret_mask_toggle"))
        if is_primary_owner(chat_id):
            kb.row(IB("👥 /owners", callback_data="additional_owners"))
    else:
        kb.row(IB(info_finance_toggle_label(chat_id), callback_data="info_finance_off"))
    kb.row(
        IB("⬅️ Назад осн. окно", callback_data=f"d:{get_chat_store(chat_id).get('current_view_day', today_key())}:back_main"),
        IB("❌ Закрыть", callback_data="info_close"),
    )
    return kb


def open_info_window(chat_id: int):
    info_text = wm_common(build_info_text(chat_id), 9)
    send_or_edit_stored_window(
        chat_id,
        "info_msg_id",
        info_text,
        reply_markup=build_info_keyboard(chat_id),
        parse_mode=None,
        delay=AUX_WINDOW_DELETE_DELAY
    )
def handle_categories_callback(call, data_str: str) -> bool:
    """UI окна расходов по статьям."""
    chat_id = call.message.chat.id
    store = get_chat_store(chat_id)

    if data_str == "cat_add_cancel":
        clear_category_wait_state(chat_id, "category_add_wait", call.message.message_id, delete_prompt=True)
        clear_category_wait_state(chat_id, "category_edit_wait", call.message.message_id, delete_prompt=True)
        try:
            bot.answer_callback_query(call.id, "Команда отменена")
        except Exception:
            pass
        return True

    if data_str == "cat_edit_menu":
        send_or_edit_categories_window(
            chat_id,
            wm_common("✏️ Изменить статью\n\nВыберите пользовательскую статью. Стандартные статьи не меняем, чтобы не ломать базовую логику.", 14),
            reply_markup=build_category_edit_keyboard(chat_id),
            preferred_message_id=call.message.message_id
        )
        return True

    if data_str.startswith("cat_edit_pick:"):
        slug = data_str.split(":", 1)[1]
        start_category_edit_wait(chat_id, chat_id, slug)
        try:
            bot.answer_callback_query(call.id, "Напиши новую статью и ключи", show_alert=False)
        except Exception:
            pass
        return True

    if data_str == "cat_del_menu":
        clear_category_wait_state(chat_id, "category_add_wait", delete_prompt=False)
        clear_category_wait_state(chat_id, "category_edit_wait", delete_prompt=False)
        store["category_delete_selection"] = []
        save_data(data)
        send_or_edit_categories_window(
            chat_id,
            wm_common("🗑 Удалить статью\n\nВыберите пользовательские статьи галочками и нажмите «Удалить выбранное». Стандартные статьи не удаляем, чтобы не ломать базовую логику.", 15),
            reply_markup=build_category_delete_keyboard(chat_id),
            preferred_message_id=call.message.message_id
        )
        return True

    if data_str.startswith("cat_del_toggle:"):
        slug = data_str.split(":", 1)[1]
        selected = set(store.get("category_delete_selection") or [])
        if slug in selected:
            selected.remove(slug)
        else:
            selected.add(slug)
        store["category_delete_selection"] = sorted(selected)
        save_data(data)
        send_or_edit_categories_window(
            chat_id,
            wm_common("🗑 Удалить статью\n\nВыберите пользовательские статьи галочками и нажмите «Удалить выбранное».", 15),
            reply_markup=build_category_delete_keyboard(chat_id),
            preferred_message_id=call.message.message_id
        )
        return True

    if data_str == "cat_del_selected":
        selected = set(store.get("category_delete_selection") or [])
        if not selected:
            try:
                bot.answer_callback_query(call.id, "Ничего не выбрано", show_alert=False)
            except Exception:
                pass
            return True
        count = remove_custom_expense_categories(chat_id, selected)
        try:
            bot.answer_callback_query(call.id, f"Удалено статей: {count}", show_alert=False)
        except Exception:
            pass
        return handle_categories_callback(call, "cat_today")

    if data_str == "cat_close":
        mid = store.get("categories_msg_id")
        if mid:
            try:
                bot.delete_message(chat_id, mid)
            except Exception:
                pass
        store["categories_msg_id"] = None
        save_data(data)
        return True

    if data_str == "cat_today":
        return handle_categories_callback(call, f"cat_wthu:{today_key()}")

    if data_str == "cat_add":
        start_category_add_wait(chat_id, chat_id)
        try:
            bot.answer_callback_query(call.id, "Напиши название и ключи статьи", show_alert=False)
        except Exception:
            pass
        return True

    if data_str == "cat_desc":
        kb = types.InlineKeyboardMarkup()
        kb.row(IB("🔙 Назад к статьям", callback_data=cat_callback(f"cat_wthu:{today_key()}")))
        kb.row(
            IB("⬅️ Назад осн. окно", callback_data=f"d:{today_key()}:back_main"),
            IB("❌ Закрыть статьи", callback_data=cat_callback("cat_close")),
        )
        send_or_edit_categories_window(chat_id, build_articles_description_text(chat_id), reply_markup=kb, preferred_message_id=call.message.message_id)
        return True

    if data_str.startswith("cat_wthu:"):
        ref = data_str.split(":", 1)[1] or today_key()
        start_key = week_start_thursday(ref)
        start, end = week_bounds_thu_wed(start_key)
        label = f"{fmt_date_ddmmyy(start)} — {fmt_date_ddmmyy(end)} (Чт–Ср)"
        text, _ = summarize_categories(store, start, end, label)
        kb = build_categories_summary_keyboard("wthu", start, end, store=store)
        send_or_edit_categories_window(chat_id, text, reply_markup=kb, preferred_message_id=call.message.message_id)
        return True

    if data_str.startswith("cat_wk:"):
        start_key = data_str.split(":", 1)[1].strip() or week_start_monday(today_key())
        start, end = week_bounds_from_start(start_key)
        label = f"{fmt_date_ddmmyy(start)} — {fmt_date_ddmmyy(end)} (Пн–Вс)"
        text, _ = summarize_categories(store, start, end, label)
        kb = build_categories_summary_keyboard("wk", start, end, store=store)
        send_or_edit_categories_window(chat_id, text, reply_markup=kb, preferred_message_id=call.message.message_id)
        return True

    if data_str == "cat_months":
        kb = types.InlineKeyboardMarkup(row_width=3)
        current_month = now_local().month
        for m in range(1, 13):
            label = datetime(2000, m, 1).strftime("%b")
            kb.add(IB(label, callback_data=cat_callback(f"cat_m:{m}")))
        kb.row(
            IB("📅 Сегодня", callback_data=cat_callback("cat_today")),
            IB("⬅️ Назад осн. окно", callback_data=f"d:{today_key()}:back_main"),
            IB("❌ Закрыть статьи", callback_data=cat_callback("cat_close"))
        )
        send_or_edit_categories_window(chat_id, wm_common("📦 Выберите месяц:", 12), reply_markup=kb)
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
            kb.add(IB(
                f"{a:02d}–{b:02d}",
                callback_data=cat_callback(f"cat_rng:{year}:{month}:{a}:{b}")
            ))
        row = []
        if month != now_local().month:
            row.append(IB("📅 Сегодня", callback_data=cat_callback("cat_today")))
        row.append(IB("🔙 Назад", callback_data=cat_callback("cat_months")))
        kb.row(*row)
        send_or_edit_categories_window(chat_id, wm_common("📆 Выберите неделю:", 13), reply_markup=kb)
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
        kb = build_categories_summary_keyboard("rng", start, end, store=store)
        send_or_edit_categories_window(chat_id, text, reply_markup=kb, preferred_message_id=call.message.message_id)
        return True

    if data_str.startswith("cat_show_wthu:"):
        _, ref, slug = data_str.split(":", 2)
        category = get_category_by_slug(slug, store)
        if not category:
            return True

        start_key = week_start_thursday(ref or today_key())
        start, end = week_bounds_thu_wed(start_key)
        label = f"{fmt_date_ddmmyy(start)} — {fmt_date_ddmmyy(end)} (Чт–Ср)"
        text = build_category_detail_text(store, start, end, category, label)
        kb = build_category_detail_keyboard(start, end, f"cat_wthu:{start}", mode="wthu", slug=slug, store=store)
        send_or_edit_categories_window(chat_id, text, reply_markup=kb, preferred_message_id=call.message.message_id)
        return True

    if data_str.startswith("cat_show_wk:"):
        _, ref, slug = data_str.split(":", 2)
        category = get_category_by_slug(slug, store)
        if not category:
            return True

        start_key = week_start_monday(ref or today_key())
        start, end = week_bounds_from_start(start_key)
        label = f"{fmt_date_ddmmyy(start)} — {fmt_date_ddmmyy(end)} (Пн–Вс)"
        text = build_category_detail_text(store, start, end, category, label)
        kb = build_category_detail_keyboard(start, end, f"cat_wk:{start}", mode="wk", slug=slug, store=store)
        send_or_edit_categories_window(chat_id, text, reply_markup=kb, preferred_message_id=call.message.message_id)
        return True

    if data_str.startswith("cat_show:"):
        _, start, end, slug = data_str.split(":", 3)
        category = get_category_by_slug(slug, store)
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
        kb = build_category_detail_keyboard(start, end, back_callback, mode=mode, slug=slug, store=store)
        send_or_edit_categories_window(chat_id, text, reply_markup=kb, preferred_message_id=call.message.message_id)
        return True

    return False
    

_callback_debounce_state = {}


def _callback_should_debounce(call, data_str: str, min_interval: float = 0.45) -> bool:
    """Защита от частых кликов: Telegram уже получил answer_callback_query, поэтому «Загрузка» не висит."""
    try:
        chat_id = int(call.message.chat.id)
        msg_id = int(call.message.message_id)
        data_str = str(data_str or "")
        if data_str == "none":
            return True
        # Для навигации/экспорта/редактирования достаточно одного клика раз в ~0.45 сек на одно окно.
        hot = False
        if data_str.startswith("d:"):
            parts = data_str.split(":", 2)
            action = parts[2] if len(parts) > 2 else ""
            hot = action in {"prev", "next", "today", "open", "back_main", "calendar", "csv_all"}
        elif data_str.startswith("fv:") or data_str.startswith("c:") or data_str.startswith("fc:"):
            hot = True
        if not hot:
            return False
        key = (chat_id, msg_id, data_str.split(":", 1)[0], data_str.split(":")[-1])
        now_ts = time.time()
        prev_ts = _callback_debounce_state.get(key, 0)
        _callback_debounce_state[key] = now_ts
        skipped = (now_ts - prev_ts) < float(min_interval)
        if skipped:
            try:
                bot_journal("button_debounced", chat_id, data_str)
            except Exception:
                pass
        return skipped
    except Exception:
        return False

@bot.callback_query_handler(func=lambda c: True)

def on_callback(call):
    try:
        bot.answer_callback_query(call.id)
    except Exception:
        pass

    try:
        raw_data_str = call.data or ""
        data_str = resolve_short_callback(raw_data_str)
        chat_id = call.message.chat.id
        if data_str is None:
            try:
                bot.answer_callback_query(call.id, "Кнопка устарела. Открой меню заново.", show_alert=True)
            except Exception:
                pass
            return
        try:
            if raw_data_str != data_str:
                bot_journal("button_pressed", chat_id, f"{raw_data_str} -> {str(data_str)[:500]}")
            else:
                bot_journal("button_pressed", chat_id, str(data_str)[:500])
        except Exception:
            pass

        try:
            # Любая кнопка в любом окне секретного режима означает, что пользователь
            # ещё работает с окном: перезапускаем отсчёт автозакрытия с 01:30.
            touch_secret_window_timer_for_callback(chat_id, call.message.message_id, data_str)
        except Exception:
            pass

        if _callback_should_debounce(call, data_str):
            return

        try:
            update_chat_info_from_message(call.message)
        except Exception:
            pass

        if data_str.startswith("ojr:"):
            if not is_owner_chat(chat_id):
                return
            try:
                _, key_s, answer = data_str.split(":", 2)
                key = int(key_s)
            except Exception:
                return

            with _owner_json_restore_prompt_lock:
                item = _owner_json_restore_prompts.pop(key, None)

            try:
                bot.delete_message(chat_id, call.message.message_id)
            except Exception:
                pass

            if not item:
                try:
                    bot.answer_callback_query(call.id, "Срок кнопки истёк", show_alert=True)
                except Exception:
                    pass
                return

            if answer != "yes":
                tmp_path = item.get("tmp_path")
                if tmp_path and os.path.exists(tmp_path):
                    try:
                        os.remove(tmp_path)
                    except Exception:
                        pass
                try:
                    bot.answer_callback_query(call.id, "Обновление JSON отменено")
                except Exception:
                    pass
                return

            try:
                bot.answer_callback_query(call.id, "Принято, обновляю JSON…")
            except Exception:
                pass
            threading.Thread(
                target=run_owner_json_restore_prompt_job,
                args=(chat_id, item),
                daemon=True,
            ).start()
            return

        if data_str.startswith("ncb:"):
            if not is_owner_chat(chat_id):
                return
            try:
                _, target_s, answer = data_str.split(":", 2)
                target_chat_id = int(target_s)
            except Exception:
                return
            set_auto_backup_enabled(target_chat_id, answer == "yes")
            try:
                bot.delete_message(chat_id, call.message.message_id)
            except Exception:
                pass
            try:
                bot.answer_callback_query(call.id, "Автообновление бэкапов включено" if answer == "yes" else "Автообновление бэкапов выключено")
            except Exception:
                pass
            return

        # Секретные окна доступны по своей скрытой команде и работают независимо
        # от финансового/скрытого режима.
        if data_str == "secmclose":
            cancel_secret_media_timer(chat_id, call.message.message_id)
            try:
                bot.delete_message(chat_id, call.message.message_id)
            except Exception:
                pass
            return
        if data_str == "secmwait":
            schedule_secret_media_close(chat_id, call.message.message_id)
            try:
                bot.edit_message_reply_markup(
                    chat_id=chat_id,
                    message_id=call.message.message_id,
                    reply_markup=build_secret_media_timer_keyboard(),
                )
                bot.answer_callback_query(call.id, "Продлено на 1 мин 30 сек")
            except Exception:
                pass
            return
        if data_str == "secclose":
            _cancel_secret_calendar_timer(chat_id, call.message.message_id)
            clear_secret_window(chat_id, call.message.message_id)
            try:
                bot.delete_message(chat_id, call.message.message_id)
            except Exception:
                pass
            return
        if data_str == "secbacklist":
            if secret_window_self_only(chat_id, call.message.message_id):
                _cancel_secret_calendar_timer(chat_id, call.message.message_id)
                clear_secret_window(chat_id, call.message.message_id)
                try:
                    bot.delete_message(chat_id, call.message.message_id)
                except Exception:
                    pass
                return
            _cancel_secret_calendar_timer(chat_id, call.message.message_id)
            clear_secret_window(chat_id, call.message.message_id)
            safe_edit_current_only(bot, call, "🔐 Выберите чат с секретными данными:", reply_markup=build_secret_chat_list_keyboard())
            register_secret_list_window(chat_id, call.message.message_id)
            return
        if data_str.startswith("seclist:"):
            try:
                target_chat_id = int(data_str.split(":", 1)[1])
                open_secret_day_window(
                    chat_id, target_chat_id,
                    message_id=call.message.message_id,
                    self_only=False,
                )
            except Exception as e:
                log_error(f"secret list callback: {e}")
            return
        if data_str.startswith("sectoggle:"):
            try:
                target_chat_id = int(data_str.split(":", 1)[1])
                set_total_secret_mode(target_chat_id, not is_total_secret_mode(target_chat_id))
                safe_edit_current_only(bot, call, "🔐 Выберите чат с секретными данными:", reply_markup=build_secret_chat_list_keyboard())
            except Exception as e:
                log_error(f"secret mode toggle callback: {e}")
            return
        if data_str.startswith("secdel:"):
            try:
                _, target_s, day_key = data_str.split(":", 2)
                target_chat_id = int(target_s)
                if not can_manage_secret_target(chat_id, target_chat_id):
                    bot.answer_callback_query(call.id, "Нет доступа", show_alert=True)
                    return
                self_only = secret_window_self_only(chat_id, call.message.message_id)
                set_secret_delete_selection(chat_id, target_chat_id, day_key, set())
                safe_edit_current_only(
                    bot,
                    call,
                    build_secret_delete_text(chat_id, target_chat_id, day_key),
                    reply_markup=build_secret_delete_keyboard(chat_id, target_chat_id, day_key, self_only=self_only),
                )
                register_secret_window(
                    chat_id, call.message.message_id, target_chat_id, "delete",
                    day_key=day_key, self_only=self_only,
                )
                schedule_secret_calendar_close(chat_id, call.message.message_id)
            except Exception as e:
                log_error(f"secret delete menu callback: {e}")
            return
        if data_str.startswith("secdelt:"):
            try:
                _, target_s, day_key, mode = data_str.split(":", 3)
                target_chat_id = int(target_s)
                if not can_manage_secret_target(chat_id, target_chat_id):
                    bot.answer_callback_query(call.id, "Нет доступа", show_alert=True)
                    return
                self_only = secret_window_self_only(chat_id, call.message.message_id)
                toggle_secret_delete_selection(chat_id, target_chat_id, day_key, mode)
                safe_edit_current_only(
                    bot,
                    call,
                    build_secret_delete_text(chat_id, target_chat_id, day_key),
                    reply_markup=build_secret_delete_keyboard(chat_id, target_chat_id, day_key, self_only=self_only),
                )
                register_secret_window(
                    chat_id, call.message.message_id, target_chat_id, "delete",
                    day_key=day_key, self_only=self_only,
                )
                schedule_secret_calendar_close(chat_id, call.message.message_id)
            except Exception as e:
                log_error(f"secret delete toggle callback: {e}")
            return
        if data_str.startswith("secdelgo:"):
            try:
                _, target_s, day_key = data_str.split(":", 2)
                target_chat_id = int(target_s)
                if not can_manage_secret_target(chat_id, target_chat_id):
                    bot.answer_callback_query(call.id, "Нет доступа", show_alert=True)
                    return
                selected = _secret_delete_selection(chat_id, target_chat_id, day_key)
                if not selected:
                    bot.answer_callback_query(call.id, "Сначала поставь галочку", show_alert=True)
                    return
                count = delete_secret_records_by_modes(target_chat_id, selected, day_key)
                set_secret_delete_selection(chat_id, target_chat_id, day_key, set())
                self_only = secret_window_self_only(chat_id, call.message.message_id)
                try:
                    bot.answer_callback_query(call.id, f"Удалено: {count}", show_alert=False)
                except Exception:
                    pass
                open_secret_calendar(
                    chat_id, target_chat_id, day_key[:7],
                    message_id=call.message.message_id, self_only=self_only,
                )
            except Exception as e:
                log_error(f"secret delete selected callback: {e}")
            return
        if data_str.startswith("secmedia:"):
            try:
                _, target_s, period = data_str.split(":", 2)
                target_chat_id = int(target_s)
                day_key = None if period == "all" else period
                try:
                    bot.answer_callback_query(call.id, "Отправляю медиа…")
                except Exception:
                    pass
                threading.Thread(
                    target=send_secret_media,
                    args=(chat_id, target_chat_id, day_key),
                    daemon=True,
                ).start()
            except Exception as e:
                log_error(f"secret media callback: {e}")
            return
        if data_str.startswith("secmonthlist:"):
            try:
                _, target_s, month_key = data_str.split(":", 2)
                target_chat_id = int(target_s)
                self_only = secret_window_self_only(chat_id, call.message.message_id)
                open_secret_month_summary(
                    chat_id, target_chat_id, month_key,
                    message_id=call.message.message_id, self_only=self_only,
                )
            except Exception as e:
                log_error(f"secret month summary callback: {e}")
            return
        if data_str.startswith("secchatcal:"):
            try:
                parts = data_str.split(":", 2)
                target_chat_id = int(parts[1])
                month_key = parts[2] if len(parts) > 2 and parts[2] else now_local().strftime("%Y-%m")
                self_only = secret_window_self_only(chat_id, call.message.message_id)
                open_secret_calendar(
                    chat_id, target_chat_id, month_key,
                    call.message.message_id, self_only=self_only,
                )
            except Exception as e:
                log_error(f"secret chat calendar callback: {e}")
            return
        if data_str.startswith("secview:"):
            try:
                _, target_s, day_key = data_str.split(":", 2)
                self_only = secret_window_self_only(chat_id, call.message.message_id)
                open_secret_day_window(
                    chat_id, int(target_s), day_key,
                    call.message.message_id, self_only=self_only,
                )
            except Exception as e:
                log_error(f"secret day view callback: {e}")
            return
        if data_str.startswith("secedtoggle:"):
            try:
                _, target_s, day_key, record_s = data_str.split(":", 3)
                target_chat_id = int(target_s)
                record_id = int(record_s)
                if not can_manage_secret_target(chat_id, target_chat_id):
                    bot.answer_callback_query(call.id, "Нет доступа", show_alert=True)
                    return
                self_only = secret_window_self_only(chat_id, call.message.message_id)
                toggle_secret_edit_delete_selection(chat_id, target_chat_id, day_key, record_id)
                safe_edit_current_only(
                    bot,
                    call,
                    build_secret_edit_text(target_chat_id, day_key),
                    reply_markup=build_secret_edit_keyboard(
                        chat_id, target_chat_id, day_key, self_only=self_only,
                    ),
                )
                register_secret_window(
                    chat_id, call.message.message_id, target_chat_id, "edit",
                    day_key=day_key, self_only=self_only,
                )
                schedule_secret_calendar_close(chat_id, call.message.message_id)
            except Exception as e:
                log_error(f"secret edit delete toggle callback: {e}")
            return
        if data_str.startswith("secedselected:"):
            try:
                _, target_s, day_key = data_str.split(":", 2)
                target_chat_id = int(target_s)
                if not can_manage_secret_target(chat_id, target_chat_id):
                    bot.answer_callback_query(call.id, "Нет доступа", show_alert=True)
                    return
                selected = _secret_edit_delete_selection(chat_id, target_chat_id, day_key)
                if not selected:
                    bot.answer_callback_query(call.id, "Сначала выбери записи", show_alert=True)
                    return
                count = delete_secret_records_by_ids(target_chat_id, selected)
                set_secret_edit_delete_selection(chat_id, target_chat_id, day_key, set())
                self_only = secret_window_self_only(chat_id, call.message.message_id)
                try:
                    bot.answer_callback_query(call.id, f"Удалено: {count}", show_alert=False)
                except Exception:
                    pass
                safe_edit_current_only(
                    bot,
                    call,
                    build_secret_edit_text(target_chat_id, day_key),
                    reply_markup=build_secret_edit_keyboard(
                        chat_id, target_chat_id, day_key, self_only=self_only,
                    ),
                )
                register_secret_window(
                    chat_id, call.message.message_id, target_chat_id, "edit",
                    day_key=day_key, self_only=self_only,
                )
                schedule_secret_calendar_close(chat_id, call.message.message_id)
            except Exception as e:
                log_error(f"secret edit delete selected callback: {e}")
            return
        if data_str.startswith("secedit:"):
            try:
                _, target_s, day_key = data_str.split(":", 2)
                target_chat_id = int(target_s)
                self_only = secret_window_self_only(chat_id, call.message.message_id)
                set_secret_edit_delete_selection(chat_id, target_chat_id, day_key, set())
                safe_edit_current_only(
                    bot,
                    call,
                    build_secret_edit_text(target_chat_id, day_key),
                    reply_markup=build_secret_edit_keyboard(chat_id, target_chat_id, day_key, self_only=self_only),
                )
                register_secret_window(
                    chat_id, call.message.message_id, target_chat_id, "edit",
                    day_key=day_key, self_only=self_only,
                )
                schedule_secret_calendar_close(chat_id, call.message.message_id)
            except Exception as e:
                log_error(f"secret edit menu callback: {e}")
            return
        if data_str.startswith("secmon:"):
            try:
                _, target_s, month_key = data_str.split(":", 2)
                self_only = secret_window_self_only(chat_id, call.message.message_id)
                open_secret_calendar(
                    chat_id, int(target_s), month_key,
                    call.message.message_id, self_only=self_only,
                )
            except Exception as e:
                log_error(f"secret month callback: {e}")
            return
        if data_str.startswith("secday:"):
            try:
                _, target_s, day_key = data_str.split(":", 2)
                self_only = secret_window_self_only(chat_id, call.message.message_id)
                open_secret_day_window(
                    chat_id, int(target_s), day_key,
                    call.message.message_id, self_only=self_only,
                )
            except Exception as e:
                log_error(f"secret day callback: {e}")
            return
        if data_str.startswith("addown:"):
            if not is_primary_owner(chat_id):
                return
            try:
                target_id = int(data_str.split(":", 1)[1])
                set_additional_owner(target_id, target_id not in get_additional_owner_ids())
                safe_edit(
                    bot,
                    call,
                    wm_owner("👥 Дополнительные владельцы\n\n✅ — доступ владельца включён\n❌ — доступ выключен", 36),
                    reply_markup=build_additional_owners_keyboard(),
                )
            except Exception as e:
                log_error(f"additional owner callback: {e}")
            return
        if data_str == "additional_owners":
            if not is_primary_owner(chat_id):
                return
            safe_edit(
                bot,
                call,
                wm_owner("👥 Дополнительные владельцы\n\n✅ — доступ владельца включён\n❌ — доступ выключен", 36),
                reply_markup=build_additional_owners_keyboard(),
            )
            return

        # Статьи должны работать во всех режимах: обычное окно, быстрый остаток,
        # скрытый финрежим и просмотр владельцем чужих фин-окон. Поэтому обрабатываем
        # их ДО guard_non_owner_finance_for_callback, который может скрывать фин-вывод.
        if data_str.startswith("fvcat_"):
            if handle_finwindow_categories_callback(call, data_str):
                return
        if data_str == "cat_months" or data_str.startswith("cat_"):
            if handle_categories_callback(call, data_str):
                return

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

        if data_str == "secret_cancel":
            _clear_secret_wait(chat_id, delete_prompt=False)
            try:
                bot.delete_message(chat_id, call.message.message_id)
            except Exception:
                pass
            return

        try:
            wait = store.get("secret_wait") or {}
            wait_msg_id = int(wait.get("prompt_msg_id") or wait.get("window_msg_id") or 0)
            if wait_msg_id == int(call.message.message_id) and str(data_str).startswith("d:") and str(data_str).endswith(":back_main"):
                _clear_secret_wait(chat_id, delete_prompt=False)
        except Exception:
            pass

        if handle_o9_secret_triple_click(call, data_str):
            return

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

        if data_str in {"aux_close", "info_close"}:
            cancel_pending_window_commands(chat_id, delete_prompt=False)
            try:
                bot.delete_message(chat_id, call.message.message_id)
            except Exception:
                pass
            return
    
        if data_str.startswith("rep:"):
            month_key = data_str.split(":", 1)[1].strip()
            open_report_window(chat_id, month_key, call.message.message_id)
            return
        if data_str.startswith("fvcat_"):
            if handle_finwindow_categories_callback(call, data_str):
                return
        if data_str == "cat_months" or data_str.startswith("cat_"):
            if handle_categories_callback(call, data_str):
                return

        if data_str.startswith("fw_"):
            if not is_owner_chat(chat_id):
                try:
                    bot.answer_callback_query(
                        call.id,
                        "Меню пересылки доступно только владельцу.",
                        show_alert=True
                    )
                except Exception:
                    pass
                return
            if data_str == "fw_new_back_src":
                owner_store = get_chat_store(int(OWNER_ID))
                owner_day_key = owner_store.get("current_view_day", today_key())
                safe_edit(
                    bot,
                    call,
                    build_forward_new_text(),
                    reply_markup=build_forward_new_menu(owner_day_key)
                )
                return
            if data_str.startswith("fw_new_pair:"):
                parts = data_str.split(":")
                if len(parts) != 3:
                    return
                try:
                    A = int(parts[1]); B = int(parts[2])
                except Exception:
                    return
                if answer_removed_chat(call, A) or answer_removed_chat(call, B):
                    return
                safe_edit(bot, call, build_forward_new_text(A, B), reply_markup=build_forward_new_menu(None, A, B))
                return
            if data_str.startswith("fw_new_src:"):
                try:
                    A = int(data_str.split(":", 1)[1])
                except Exception:
                    return
                if answer_removed_chat(call, A):
                    return
                safe_edit(bot, call, build_forward_new_text(A, None), reply_markup=build_forward_new_menu(None, A, None))
                return
            if data_str.startswith("fw_new_tgt:"):
                parts = data_str.split(":")
                if len(parts) != 3:
                    return
                try:
                    A = int(parts[1]); B = int(parts[2])
                except Exception:
                    return
                if answer_removed_chat(call, A) or answer_removed_chat(call, B):
                    return
                safe_edit(bot, call, build_forward_new_text(A, B), reply_markup=build_forward_new_menu(None, A, B))
                return
            if data_str.startswith("fw_new_fin:"):
                parts = data_str.split(":")
                if len(parts) != 4:
                    return
                try:
                    A = int(parts[1]); B = int(parts[2]); which = parts[3]
                except Exception:
                    return
                if answer_removed_chat(call, A) or answer_removed_chat(call, B):
                    return
                if which == "ab":
                    new_val = not get_forward_finance(A, B)
                    set_forward_finance(A, B, new_val)
                    if new_val:
                        _remember_forward_pair(A, B)
                elif which == "ba":
                    new_val = not get_forward_finance(B, A)
                    set_forward_finance(B, A, new_val)
                    if new_val:
                        _remember_forward_pair(A, B)
                _forget_forward_pair_if_empty(A, B)
                safe_edit(bot, call, build_forward_new_text(A, B), reply_markup=build_forward_new_menu(None, A, B))
                return
            if data_str.startswith("fw_new_mode:"):
                parts = data_str.split(":")
                if len(parts) != 4:
                    return
                try:
                    A = int(parts[1]); B = int(parts[2]); mode = parts[3]
                except Exception:
                    return
                if answer_removed_chat(call, A) or answer_removed_chat(call, B):
                    return
                fr = data.get("forward_rules", {}) or {}
                if mode == "to":
                    if str(B) in (fr.get(str(A), {}) or {}):
                        remove_forward_link(A, B)
                    else:
                        add_forward_link(A, B, "oneway_to")
                        _remember_forward_pair(A, B)
                elif mode == "from":
                    if str(A) in (fr.get(str(B), {}) or {}):
                        remove_forward_link(B, A)
                    else:
                        add_forward_link(B, A, "oneway_to")
                        _remember_forward_pair(A, B)
                elif mode == "two":
                    ab_on = str(B) in (fr.get(str(A), {}) or {})
                    ba_on = str(A) in (fr.get(str(B), {}) or {})
                    if ab_on and ba_on:
                        remove_forward_link(A, B)
                        remove_forward_link(B, A)
                    else:
                        add_forward_link(A, B, "twoway")
                        add_forward_link(B, A, "twoway")
                        _remember_forward_pair(A, B)
                _forget_forward_pair_if_empty(A, B)
                safe_edit(bot, call, build_forward_new_text(A, B), reply_markup=build_forward_new_menu(None, A, B))
                return
            if data_str.startswith("fw_new_clear:"):
                parts = data_str.split(":")
                if len(parts) != 3:
                    return
                try:
                    A = int(parts[1]); B = int(parts[2])
                except Exception:
                    return
                remove_forward_link(A, B)
                remove_forward_link(B, A)
                remove_forward_finance(A, B)
                remove_forward_finance(B, A)
                _forget_forward_pair_if_empty(A, B)
                safe_edit(bot, call, build_forward_new_text(A, B), reply_markup=build_forward_new_menu(None, A, B))
                return
            if data_str == "fw_probe_all":
                owner_store = get_chat_store(int(OWNER_ID))
                owner_day_key = owner_store.get("current_view_day", today_key())
                # Сначала обновляем это же окно, но не создаём новое окно даже если edit не получится.
                safe_edit_current_only(
                    bot,
                    call,
                    build_forward_status_text("📡 Проверяю чаты...\nОкно не будет плодиться, результат появится здесь же."),
                    reply_markup=build_forward_source_menu(owner_day_key)
                )
                ok, bad = probe_all_known_chats()
                owner_store = get_chat_store(int(OWNER_ID))
                owner_day_key = owner_store.get("current_view_day", today_key())
                kb = build_forward_source_menu(owner_day_key)
                safe_edit_current_only(
                    bot,
                    call,
                    build_forward_status_text(f"📡 Проверка чатов завершена. Доступно: {ok}. Удалено/нет доступа: {bad}.\n\nПересылка:\nВыберите чат A:"),
                    reply_markup=kb
                )
                return
            if data_str == "fw_removed_list":
                owner_store = get_chat_store(int(OWNER_ID))
                owner_day_key = owner_store.get("current_view_day", today_key())
                safe_edit(
                    bot,
                    call,
                    "🗑 Удалённые чаты\nНажмите чат, чтобы перепроверить наличие бота.",
                    reply_markup=build_removed_chats_menu(owner_day_key)
                )
                return
            if data_str.startswith("fw_probe_one:"):
                try:
                    cid = int(data_str.split(":", 1)[1])
                except Exception:
                    return
                ok = probe_bot_in_chat(cid)
                status = "✅ бот снова доступен" if ok else "➖ бот удалён/нет доступа"
                owner_store = get_chat_store(int(OWNER_ID))
                owner_day_key = owner_store.get("current_view_day", today_key())
                safe_edit_current_only(
                    bot,
                    call,
                    f"🗑 Удалённые чаты\n{get_chat_display_name(cid)}: {status}",
                    reply_markup=build_removed_chats_menu(owner_day_key)
                )
                return
            if data_str == "fw_open":
                owner_store = get_chat_store(int(OWNER_ID))
                owner_day_key = owner_store.get("current_view_day", today_key())
                kb = build_forward_menu_keyboard_for_current_mode(owner_day_key)
                safe_edit(
                    bot,
                    call,
                    build_forward_menu_text_for_current_mode("Пересылка:\nВыберите чат A:"),
                    reply_markup=kb
                )
                return
            if data_str == "fw_back_root":
                owner_store = get_chat_store(int(OWNER_ID))
                day_key = owner_store.get("current_view_day", today_key())
                txt, _ = render_day_window(chat_id, day_key)
                safe_edit(bot, call, txt, reply_markup=build_main_keyboard(day_key, chat_id), parse_mode="HTML")
                return
            if data_str == "fw_back_src":
                owner_store = get_chat_store(int(OWNER_ID))
                owner_day_key = owner_store.get("current_view_day", today_key())
                kb = build_forward_menu_keyboard_for_current_mode(owner_day_key)
                safe_edit(
                    bot,
                    call,
                    build_forward_menu_text_for_current_mode("Пересылка:\nВыберите чат A:"),
                    reply_markup=kb
                )
                return
            if data_str.startswith("fw_back_tgt:"):
                try:
                    A = int(data_str.split(":", 1)[1])
                except Exception:
                    return
                if answer_removed_chat(call, A):
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
                if answer_removed_chat(call, A):
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
                if answer_removed_chat(call, A) or answer_removed_chat(call, B):
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
                if answer_removed_chat(call, A) or answer_removed_chat(call, B):
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
                if answer_removed_chat(call, A) or answer_removed_chat(call, B):
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
            safe_edit(bot, call, wm_common("📅 Выберите день:", 2), reply_markup=kb)
            return
        if data_str.startswith("fc:"):
            if not is_owner_chat(chat_id):
                return
            try:
                _, target_s, center_s, owner_day_key = data_str.split(":", 3)
                target_chat_id = int(target_s)
                center_dt = datetime.strptime(center_s, "%Y-%m-%d")
            except Exception:
                return
            safe_edit(
                bot,
                call,
                f"📅 Календарь: {html.escape(get_chat_display_name(target_chat_id))}",
                reply_markup=build_fin_calendar_keyboard(target_chat_id, center_dt, owner_day_key),
                parse_mode="HTML"
            )
            return
        if data_str == "articles_desc":
            if not is_owner_chat(chat_id):
                return
            kb = types.InlineKeyboardMarkup()
            kb.row(IB("🔙 Назад", callback_data="journal_back"))
            safe_edit(bot, call, build_articles_description_text(chat_id), reply_markup=kb)
            return

        if data_str == "journal_open":
            if not is_owner_chat(chat_id):
                return
            kb = types.InlineKeyboardMarkup()
            kb.row(IB("📄 Скачать TXT", callback_data="journal_file"))
            kb.row(
                IB("🔙 Назад", callback_data="journal_back"),
                IB("⬅️ Назад осн. окно", callback_data=f"d:{get_chat_store(chat_id).get('current_view_day', today_key())}:back_main"),
                IB("❌ Закрыть", callback_data="info_close"),
            )
            safe_edit(bot, call, format_journal_text(120), reply_markup=kb)
            return
        if data_str == "journal_file":
            if not is_owner_chat(chat_id):
                return
            send_journal_file_to_owner(chat_id, 3000)
            return
        if data_str == "journal_toggle":
            if not is_owner_chat(chat_id):
                return
            new_state = toggle_journal_registration()
            bot_journal("journal_toggle", chat_id, f"enabled={new_state}")
            safe_edit(bot, call, build_info_text(chat_id), reply_markup=build_info_keyboard(chat_id))
            return
        if data_str == "journal_back":
            if not is_owner_chat(chat_id):
                return
            safe_edit(bot, call, build_info_text(chat_id), reply_markup=build_info_keyboard(chat_id))
            return
        if data_str == "forward_menu_style_toggle":
            if not is_owner_chat(chat_id):
                return
            new_state = toggle_forward_menu_new_style()
            safe_edit(bot, call, build_info_text(chat_id) + f"\n\nМеню пересылки: {'по-новому' if new_state else 'как обычно'}", reply_markup=build_info_keyboard(chat_id))
            return
        if data_str == "buttons_current_toggle":
            if not is_owner_chat(chat_id):
                return
            new_state = toggle_buttons_current_window()
            safe_edit(bot, call, build_info_text(chat_id) + f"\n\nРежим кнопок в текущем окне: {'ВКЛ' if new_state else 'ВЫКЛ'}", reply_markup=build_info_keyboard(chat_id))
            return
        if data_str == "icon_buttons_toggle":
            if not is_owner_chat(chat_id):
                return
            new_state = toggle_icon_button_mode()
            safe_edit(bot, call, build_info_text(chat_id) + f"\n\nКнопки: {'значки' if new_state else 'текст'}", reply_markup=build_info_keyboard(chat_id))
            return
        if data_str == "total_secret_mask_toggle":
            if not is_owner_chat(chat_id):
                return
            new_state = toggle_total_secret_mask()
            safe_edit(bot, call, build_info_text(chat_id) + f"\n\nМаскировка тотального секрета: {'ВКЛ' if new_state else 'ВЫКЛ'}", reply_markup=build_info_keyboard(chat_id))
            return
        if data_str == "info_finance_off":
            try:
                if is_finance_mode(chat_id):
                    set_hidden_finance_mode(chat_id, False)
                    set_quick_balance_behavior(chat_id, "normal")
                    set_quick_balance_enabled(chat_id, False)
                    set_finance_mode(chat_id, False)
                    state_text = "выключен"
                else:
                    set_finance_mode(chat_id, True)
                    state_text = "включён"
                open_info_window(chat_id)
                bot.answer_callback_query(call.id, f"Фин режим {state_text}", show_alert=False)
            except Exception as e:
                log_error(f"info_finance_off({chat_id}): {e}")
            return

        if data_str == "info_close":
            try:
                bot.delete_message(chat_id, call.message.message_id)
            except Exception as e:
                log_error(f"info_close delete failed: {e}")
            _clear_stored_window(chat_id, "info_msg_id", call.message.message_id)
            return
        if data_str.startswith("fv:"):
            if not is_owner_chat(chat_id):
                return
            try:
                _, target_s, view_day, action, owner_day_key = data_str.split(":", 4)
                target_chat_id = int(target_s)
            except Exception:
                return
            target_store = get_chat_store(target_chat_id)
            target_store["current_view_day"] = view_day

            if action == "clear_delete_back":
                clear_edit_delete_selection(target_chat_id, view_day)
                safe_edit(
                    bot,
                    call,
                    render_fin_window_text(target_chat_id, view_day),
                    reply_markup=build_fin_window_view_keyboard(target_chat_id, view_day, owner_day_key),
                    parse_mode="HTML"
                )
                return

            if action in {"open", "back_main", "menu"}:
                clear_edit_delete_selection(target_chat_id, view_day)
                safe_edit(
                    bot,
                    call,
                    render_fin_window_text(target_chat_id, view_day),
                    reply_markup=build_fin_window_view_keyboard(target_chat_id, view_day, owner_day_key),
                    parse_mode="HTML"
                )
                return
            if action == "menu":
                clear_edit_delete_selection(target_chat_id, view_day)
                safe_edit(
                    bot,
                    call,
                    render_fin_window_text(target_chat_id, view_day),
                    reply_markup=build_fin_window_menu_keyboard(target_chat_id, view_day, owner_day_key),
                    parse_mode="HTML"
                )
                return
            if action == "calendar":
                try:
                    cdt = datetime.strptime(view_day, "%Y-%m-%d")
                except Exception:
                    cdt = now_local()
                safe_edit(
                    bot,
                    call,
                    f"📅 Календарь: {html.escape(get_chat_display_name(target_chat_id))}",
                    reply_markup=build_fin_calendar_keyboard(target_chat_id, cdt, owner_day_key),
                    parse_mode="HTML"
                )
                return
            if action == "report":
                try:
                    month_key = datetime.strptime(view_day, "%Y-%m-%d").strftime("%Y-%m")
                except Exception:
                    month_key = now_local().strftime("%Y-%m")
                report_html, _ = build_month_report_text(target_chat_id, month_key)
                safe_edit(
                    bot,
                    call,
                    f"👁 {html.escape(get_chat_display_name(target_chat_id))}\n" + report_html,
                    reply_markup=_one_button_keyboard("🔙 Назад", f"fv:{target_chat_id}:{view_day}:open:{owner_day_key}"),
                    parse_mode="HTML"
                )
                return
            if action == "total":
                text = f"👁 {html.escape(get_chat_display_name(target_chat_id))}\n\n💰 Общий итог по чату: {fmt_num(target_store.get('balance', 0))}"
                safe_edit(bot, call, text, reply_markup=build_fin_window_view_keyboard(target_chat_id, view_day, owner_day_key), parse_mode="HTML")
                return
            if action == "info":
                kb_info = build_fin_window_view_keyboard(target_chat_id, view_day, owner_day_key)
                safe_edit(bot, call, build_info_text(target_chat_id) + "\n\n" + build_articles_description_text(target_chat_id), reply_markup=kb_info)
                return
            if action == "reset":
                owner_store = get_chat_store(chat_id)
                owner_store["finwin_reset_wait"] = {
                    "type": "finwin_reset",
                    "target_chat_id": target_chat_id,
                    "owner_day_key": owner_day_key,
                    "fin_window_msg_id": call.message.message_id,
                    "expires_at": time.time() + 20,
                }
                save_data(data)
                send_and_auto_delete(
                    chat_id,
                    f"⚠️ Обнулить данные чата {get_chat_display_name(target_chat_id)}? Напишите ДА в течение 20 секунд или ОТМЕНА.",
                    20
                )
                return
            if action == "cancel_edit":
                clear_finwin_edit_wait_state(chat_id, call.message.message_id, delete_prompt=True)
                try:
                    bot.answer_callback_query(call.id, "Редактирование отменено")
                except Exception:
                    pass
                return
            if action == "edit_list":
                day_recs = target_store.get("daily_records", {}).get(view_day, [])
                if not day_recs:
                    send_and_auto_delete(chat_id, "Нет записей за этот день.", 8)
                    return
                safe_edit(
                    bot,
                    call,
                    render_fin_window_text(target_chat_id, view_day),
                    reply_markup=build_edit_records_keyboard(view_day, target_chat_id, prefix="fv", owner_day_key=owner_day_key),
                    parse_mode="HTML"
                )
                return
            if action.startswith("del_toggle_"):
                rid = int(action.split("_")[-1])
                toggle_edit_delete_selection(target_chat_id, view_day, rid)
                safe_edit(bot, call, render_fin_window_text(target_chat_id, view_day), reply_markup=build_edit_records_keyboard(view_day, target_chat_id, prefix="fv", owner_day_key=owner_day_key), parse_mode="HTML")
                return
            if action == "del_selected":
                count = delete_selected_records(target_chat_id, view_day)
                safe_edit(bot, call, render_fin_window_text(target_chat_id, view_day), reply_markup=build_edit_records_keyboard(view_day, target_chat_id, prefix="fv", owner_day_key=owner_day_key), parse_mode="HTML")
                send_and_auto_delete(chat_id, f"🗑 Удалено записей: {count}", 8)
                return
            if action.startswith("edit_rec_"):
                rid = int(action.split("_")[-1])
                rec = next((r for r in target_store.get("records", []) if int(r.get("id", -1)) == rid), None)
                if not rec:
                    send_and_auto_delete(chat_id, "❌ Запись не найдена.", 8)
                    return

                insert_value = compose_edit_input_value(rec.get("amount"), rec.get("note", ""))
                prompt_text = wm_owner((
                    f"✏️ Редактирование записи {rec.get('short_id') or 'R' + str(rid)}\n"
                    f"👁 Чат: {get_chat_display_name(target_chat_id)}\n\n"
                    f"Текущие данные:\n{fmt_num(rec['amount'])} {rec.get('note','')}\n\n"
                    f"✍️ Напишите новые данные или нажмите «Вставить текущее значение».\n"
                    f"⏳ Это сообщение и режим редактирования будут автоматически отменены через 40 секунд."
                ), 17)
                owner_store = get_chat_store(chat_id)
                prompt_id = send_or_edit_edit_prompt(
                    chat_id,
                    "finwin_edit_wait",
                    prompt_text,
                    reply_markup=build_finwin_cancel_edit_keyboard(target_chat_id, view_day, owner_day_key, insert_text=insert_value)
                )
                owner_store["finwin_edit_wait"] = {
                    "type": "finwin_edit",
                    "target_chat_id": target_chat_id,
                    "rid": rid,
                    "day_key": view_day,
                    "owner_day_key": owner_day_key,
                    "prompt_msg_id": prompt_id,
                    "fin_window_msg_id": call.message.message_id,
                    "insert_text": insert_value,
                    "countdown_base_text": prompt_text,
                    "expires_at": time.time() + 40,
                }
                save_data(data)
                schedule_cancel_finwin_edit(chat_id, prompt_id, delay=40)
                return
            if action == "csv_menu":
                safe_edit(
                    bot,
                    call,
                    wm_common(f"📂 CSV / Excel: {html.escape(get_chat_display_name(target_chat_id))}\nВыберите период:", 5),
                    reply_markup=build_fin_window_csv_menu(target_chat_id, view_day, owner_day_key),
                    parse_mode="HTML"
                )
                return
            if action in {"bk_chat", "bk_channel", "bk_mega"}:
                target = action.replace("bk_", "")
                set_backup_target_enabled(target_chat_id, target, not is_backup_target_enabled(target_chat_id, target))
                safe_edit(
                    bot,
                    call,
                    wm_common(f"📂 CSV / Excel: {html.escape(get_chat_display_name(target_chat_id))}\nВыберите период:", 5),
                    reply_markup=build_fin_window_csv_menu(target_chat_id, view_day, owner_day_key),
                    parse_mode="HTML"
                )
                return
            if action in {"csv_all", "csv_day", "csv_week", "csv_month", "csv_wedthu", "xlsx_all", "xlsx_day", "xlsx_week", "xlsx_month", "xlsx_wedthu"}:
                file_type = "xlsx" if action.startswith("xlsx_") else "csv"
                mode = action.replace("csv_", "").replace("xlsx_", "")
                send_export_for_chat_to(chat_id, target_chat_id, mode, view_day, file_type)
                return
            return
        if not data_str.startswith("d:"):
            return
        _, day_key, cmd = data_str.split(":", 2)
        store = get_chat_store(chat_id)
        if cmd.startswith("removed_"):
            try:
                removed_chat_id = int(cmd.rsplit("_", 1)[1])
            except Exception:
                return
            answer_removed_chat(call, removed_chat_id)
            return
        if cmd == "open":
            clear_edit_delete_selection(chat_id, day_key)
            store["current_view_day"] = day_key
            if is_owner_chat(chat_id):
                backup_window_for_owner(chat_id, day_key, call.message.message_id)
            else:
                txt, _ = render_day_window(chat_id, day_key)
                kb = build_main_keyboard(day_key, chat_id)
                safe_edit(bot, call, txt, reply_markup=kb, parse_mode="HTML")
                set_active_window_id(chat_id, day_key, call.message.message_id)
                schedule_balance_panel_refresh(chat_id, 0.1)
            return
        if cmd == "prev":
            base_day_key = store.get("current_view_day") or day_key
            d = datetime.strptime(base_day_key, "%Y-%m-%d") - timedelta(days=1)
            nd = d.strftime("%Y-%m-%d")
            store["current_view_day"] = nd
            if is_owner_chat(chat_id):
                backup_window_for_owner(chat_id, nd, call.message.message_id)
            else:
                txt, _ = render_day_window(chat_id, nd)
                kb = build_main_keyboard(nd, chat_id)
                safe_edit(bot, call, txt, reply_markup=kb, parse_mode="HTML")
                set_active_window_id(chat_id, nd, call.message.message_id)
                schedule_balance_panel_refresh(chat_id, 0.1)
            return
        if cmd == "next":
            base_day_key = store.get("current_view_day") or day_key
            d = datetime.strptime(base_day_key, "%Y-%m-%d") + timedelta(days=1)
            nd = d.strftime("%Y-%m-%d")
            store["current_view_day"] = nd
            if is_owner_chat(chat_id):
                backup_window_for_owner(chat_id, nd, call.message.message_id)
            else:
                txt, _ = render_day_window(chat_id, nd)
                kb = build_main_keyboard(nd, chat_id)
                safe_edit(bot, call, txt, reply_markup=kb, parse_mode="HTML")
                set_active_window_id(chat_id, nd, call.message.message_id)
                schedule_balance_panel_refresh(chat_id, 0.1)
            return
        if cmd == "today":
            nd = today_key()
            store["current_view_day"] = nd
            if is_owner_chat(chat_id):
                backup_window_for_owner(chat_id, nd, call.message.message_id)
            else:
                txt, _ = render_day_window(chat_id, nd)
                kb = build_main_keyboard(nd, chat_id)
                safe_edit(bot, call, txt, reply_markup=kb, parse_mode="HTML")
                set_active_window_id(chat_id, nd, call.message.message_id)
                schedule_balance_panel_refresh(chat_id, 0.1)
            return
        if cmd == "calendar":
            try:
                cdt = datetime.strptime(day_key, "%Y-%m-%d")
            except Exception:
                cdt = now_local()
            kb = build_calendar_keyboard(cdt, chat_id)
            safe_edit(bot, call, wm_common("📅 Выберите день:", 2), reply_markup=kb)
            return
        if cmd == "report":
            try:
                month_key = datetime.strptime(day_key, "%Y-%m-%d").strftime("%Y-%m")
            except Exception:
                month_key = now_local().strftime("%Y-%m")
            if chat_buttons_current_window_enabled(chat_id):
                report_html, _ = build_month_report_text(chat_id, month_key)
                safe_edit(bot, call, report_html, reply_markup=build_report_keyboard(month_key), parse_mode="HTML")
            else:
                open_report_window(chat_id, month_key)
            return
        if cmd == "total":
            chat_bal = store.get("balance", 0)

            if not is_owner_chat(chat_id):
                text = wm_common(f"💰 Общий итог по этому чату: {fmt_num(chat_bal)}", 4)
                if chat_buttons_current_window_enabled(chat_id):
                    safe_edit(bot, call, text, parse_mode="HTML")
                    return
                final_id = send_or_edit_stored_window(
                    chat_id,
                    "total_msg_id",
                    text,
                    parse_mode="HTML",
                    delay=AUX_WINDOW_DELETE_DELAY
                )
                store["total_msg_id"] = final_id
                save_data(data)
                return

            lines = []
            info = store.get("info", {})
            title = get_chat_display_name(chat_id)
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
                title2 = get_chat_display_name(cid_int)
                other_lines.append(f"   • {title2}: {fmt_num(bal)}")
            if other_lines:
                lines.append("")
                lines.append("• Другие чаты:")
                lines.extend(other_lines)
            lines.append("")
            lines.append(f"• Всего по всем чатам: {fmt_num(total_all)}")

            text = "\n".join(lines)
            if chat_buttons_current_window_enabled(chat_id):
                safe_edit(bot, call, wm_common(text, 4), parse_mode="HTML")
                return
            final_id = send_or_edit_stored_window(
                chat_id,
                "total_msg_id",
                text,
                parse_mode="HTML",
                delay=OWNER_TOTAL_WINDOW_DELETE_DELAY
            )
            store["total_msg_id"] = final_id
            save_data(data)
            schedule_owner_total_window_delete(chat_id, final_id)
            return
        if cmd == "info":
            try:
                bot.answer_callback_query(call.id)
            except Exception:
                pass
            if chat_buttons_current_window_enabled(chat_id):
                kb_info = types.InlineKeyboardMarkup()
                kb_info.row(
                    IB("📓 Журнал", callback_data="journal_open"),
                    IB(journal_toggle_label(), callback_data="journal_toggle"),
                )
                kb_info.row(
                    IB(buttons_current_window_label(), callback_data="buttons_current_toggle"),
            IB(info_finance_toggle_label(chat_id), callback_data="info_finance_off"),
                )
                kb_info.row(
                    IB("⬅️ Назад осн. окно", callback_data=f"d:{get_chat_store(chat_id).get('current_view_day', today_key())}:back_main"),
                    IB("❌ Закрыть", callback_data="info_close"),
                )
                safe_edit(bot, call, wm_common(build_info_text(chat_id), 9), reply_markup=kb_info)
            else:
                open_info_window(chat_id)
            return
        if cmd == "process_menu":
            if not is_owner_chat(chat_id):
                try:
                    bot.answer_callback_query(call.id, "PROCESS доступен только владельцу", show_alert=True)
                except Exception:
                    pass
                return
            safe_edit(bot, call, build_process_menu_text(), reply_markup=build_process_menu(day_key))
            return
        if cmd.startswith("process_toggle_"):
            if not is_owner_chat(chat_id):
                try:
                    bot.answer_callback_query(call.id, "PROCESS доступен только владельцу", show_alert=True)
                except Exception:
                    pass
                return
            try:
                target_chat_id = int(cmd.rsplit("_", 1)[1])
            except Exception:
                return
            if answer_removed_chat(call, target_chat_id):
                return
            enabled = toggle_process_trace(target_chat_id)
            try:
                bot.answer_callback_query(call.id, "PROCESS включён" if enabled else "PROCESS выключен")
            except Exception:
                pass
            safe_edit(bot, call, build_process_menu_text(), reply_markup=build_process_menu(day_key))
            return
        if cmd == "backup_menu":
            if not is_owner_chat(chat_id):
                try:
                    bot.answer_callback_query(call.id, "BACKUP доступен только владельцу", show_alert=True)
                except Exception:
                    pass
                return
            safe_edit(bot, call, build_backup_owner_menu_text(), reply_markup=build_backup_owner_menu(day_key))
            return
        if cmd.startswith("backup_toggle_"):
            if not is_owner_chat(chat_id):
                try:
                    bot.answer_callback_query(call.id, "BACKUP доступен только владельцу", show_alert=True)
                except Exception:
                    pass
                return
            try:
                tail = cmd[len("backup_toggle_"):]
                target, cid_s = tail.rsplit("_", 1)
                target_chat_id = int(cid_s)
            except Exception:
                return
            if answer_removed_chat(call, target_chat_id):
                return
            if target == "chat" and not is_owner_chat(target_chat_id):
                try:
                    bot.answer_callback_query(call.id, "Бэкап в сам чат разрешён только владельцу", show_alert=True)
                except Exception:
                    pass
                return
            set_backup_target_enabled(target_chat_id, target, not is_backup_target_enabled(target_chat_id, target))
            try:
                bot.answer_callback_query(call.id, "Бэкап включён" if is_backup_target_enabled(target_chat_id, target) else "Бэкап выключен")
            except Exception:
                pass
            safe_edit(bot, call, build_backup_owner_menu_text(), reply_markup=build_backup_owner_menu(day_key))
            return
        if cmd in ("edit_menu", "menu"):
            clear_edit_delete_selection(chat_id, day_key)
            store["current_view_day"] = day_key
            txt, _ = render_day_window(chat_id, day_key)
            safe_edit(bot, call, txt, reply_markup=build_main_keyboard(day_key, chat_id), parse_mode="HTML")
            set_active_window_id(chat_id, day_key, call.message.message_id)
            return
        if cmd == "back_main":
            cancel_pending_window_commands(chat_id, delete_prompt=False)
            clear_edit_delete_selection(chat_id, day_key)
            store["current_view_day"] = day_key
            return_to_main_window_closing_previous(chat_id, day_key, call.message.message_id)
            return
        if cmd == "csv_all":
            kb = build_csv_menu(day_key, chat_id)
            txt, _ = render_day_window(chat_id, day_key)
            safe_edit(
                bot,
                call,
                txt,
                reply_markup=kb,
                parse_mode="HTML"
            )
            return
        if cmd in {"bk_chat", "bk_channel", "bk_mega"}:
            if not is_owner_chat(chat_id):
                try:
                    bot.answer_callback_query(call.id, "Настройка бэкапа доступна только владельцу", show_alert=True)
                except Exception:
                    pass
                return
            target = cmd.replace("bk_", "")
            set_backup_target_enabled(chat_id, target, not is_backup_target_enabled(chat_id, target))
            kb = build_csv_menu(day_key, chat_id)
            txt, _ = render_day_window(chat_id, day_key)
            safe_edit(bot, call, txt, reply_markup=kb, parse_mode="HTML")
            return
        if cmd in {"csv_day", "csv_week", "csv_month", "csv_wedthu", "csv_all_real", "xlsx_day", "xlsx_week", "xlsx_month", "xlsx_wedthu", "xlsx_all"}:
            file_type = "xlsx" if cmd.startswith("xlsx_") else "csv"
            mode = cmd.replace("csv_", "").replace("xlsx_", "")
            if mode == "all_real":
                mode = "all"
            send_export_for_chat_to(chat_id, chat_id, mode, day_key, file_type)
            return
        if cmd == "reset":
            # Кнопка обнуления убрана из о1. Старые/зависшие кнопки не запускают reset;
            # рабочий путь оставлен только через команду /reset из окна ℹ️ Инфо.
            send_and_auto_delete(chat_id, "⚙️ Обнуление доступно только командой /reset из окна ℹ️ Инфо.", 12)
            return

        if cmd == "edit_list":
            day_recs = store.get("daily_records", {}).get(day_key, [])
            if not day_recs:
                send_and_auto_delete(chat_id, "Нет записей за этот день.")
                return
            txt, _ = render_day_window(chat_id, day_key)
            safe_edit(
                bot,
                call,
                txt,
                reply_markup=build_edit_records_keyboard(day_key, chat_id),
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

            text = (
                f"✏️ Редактирование записи R{rid}\n\n"
                f"Текущие данные:\n"
                f"{fmt_num(rec['amount'])} {rec.get('note','')}\n\n"
                f"✍️ Напишите новые данные.\n\n"
                f"⏳ Это сообщение и режим редактирования будут автоматически отменены через 40 секунд."
            )

            insert_value = compose_edit_input_value(rec.get("amount"), rec.get("note", ""))
            text = wm_common(text, 10)
            kb = build_cancel_edit_keyboard(day_key, insert_text=insert_value)

            prompt_id = send_or_edit_edit_prompt(chat_id, "edit_wait", text, reply_markup=kb)

            store["edit_wait"] = {
                "type": "edit",
                "rid": rid,
                "day_key": day_key,
                "prompt_msg_id": prompt_id,
                "insert_text": insert_value,
                "countdown_base_text": text,
                "expires_at": time.time() + 40,
            }
            save_data(data)

            schedule_cancel_edit(chat_id, prompt_id, delay=40)

            return
        if cmd.startswith("del_toggle_"):
            rid = int(cmd.split("_")[-1])
            toggle_edit_delete_selection(chat_id, day_key, rid)
            txt, _ = render_day_window(chat_id, day_key)
            safe_edit(bot, call, txt, reply_markup=build_edit_records_keyboard(day_key, chat_id), parse_mode="HTML")
            return
        if cmd == "del_selected":
            count = delete_selected_records(chat_id, day_key)
            txt, _ = render_day_window(chat_id, day_key)
            safe_edit(bot, call, txt, reply_markup=build_edit_records_keyboard(day_key, chat_id), parse_mode="HTML")
            send_and_auto_delete(chat_id, f"🗑 Удалено записей: {count}", 8)
            return

        if cmd == "forward_menu":
            if not is_owner_chat(chat_id):
                send_and_auto_delete(chat_id, "Меню доступно только владельцу.", HELPER_DELETE_DELAY)
                return
            kb = build_forward_menu_keyboard_for_current_mode(day_key)
            safe_edit(
                bot,
                call,
                build_forward_menu_text_for_current_mode("Пересылка:\nВыберите чат A:"),
                reply_markup=kb
            )
            return
        if cmd == "forward_finmode_menu":
            kb = build_finance_toggle_chat_menu(day_key)
            safe_edit(
                bot,
                call,
                "💰 Фин режим / В24\nВыберите чат. Значок рядом с чатом показывает текущий режим:\n❌ выкл | 🙈 скрыто | ✅🔟 как обычно | ✅3️⃣ открыть окно | ✅🥇 всегда первым",
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
        if cmd == "hidden_finance_menu":
            kb = build_hidden_finance_chat_menu(day_key)
            safe_edit(
                bot,
                call,
                build_forward_status_text("Скрытые финансы:\nВыберите чат. Финансовый учёт и бэкапы работают, окна в чате не выводятся."),
                reply_markup=kb
            )
            return
        if cmd.startswith("hf_pick_"):
            tgt = int(cmd.split("_")[-1])
            if answer_removed_chat(call, tgt):
                return
            set_hidden_finance_mode(tgt, not is_hidden_finance_mode(tgt))
            kb = build_hidden_finance_chat_menu(day_key)
            safe_edit(
                bot,
                call,
                build_forward_status_text("Скрытые финансы:\nВыберите чат."),
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
            if answer_removed_chat(call, tgt):
                return
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
            if answer_removed_chat(call, tgt):
                return
            kb = build_quick_balance_mode_menu(day_key, tgt)
            safe_edit(
                bot,
                call,
                build_finance_mode_config_text(tgt),
                reply_markup=kb
            )
            return
        if cmd.startswith("qb_mode_normal_"):
            tgt = int(cmd.split("_")[-1])
            if answer_removed_chat(call, tgt):
                return
            set_hidden_finance_mode(tgt, False)
            set_finance_mode(tgt, True)
            set_quick_balance_behavior(tgt, "normal")
            set_quick_balance_enabled(tgt, False)
            try:
                get_chat_store(tgt)["main_window_msg_count"] = 0
                save_data(data)
            except Exception:
                pass
            try:
                if is_finance_mode(tgt) and not is_hidden_finance_mode(tgt):
                    recreate_main_window_now(tgt, get_chat_store(tgt).get("current_view_day") or today_key())
            except Exception as e:
                log_error(f"qb_mode_normal recreate main window {get_chat_display_name(tgt)}: {e}")
            # Не трогаем личное окно владельца при изменении настроек другого чата.
            safe_edit(
                bot,
                call,
                build_finance_mode_config_text(tgt),
                reply_markup=build_finance_mode_config_menu(day_key, tgt)
            )
            return
        if cmd.startswith("qb_mode_open_"):
            tgt = int(cmd.split("_")[-1])
            if answer_removed_chat(call, tgt):
                return
            set_hidden_finance_mode(tgt, False)
            set_quick_balance_behavior(tgt, "open")
            set_quick_balance_enabled(tgt, True)
            # Не трогаем личное окно владельца при изменении настроек другого чата.
            safe_edit(
                bot,
                call,
                build_finance_mode_config_text(tgt),
                reply_markup=build_finance_mode_config_menu(day_key, tgt)
            )
            return
        if cmd.startswith("qb_mode_first_"):
            tgt = int(cmd.split("_")[-1])
            if answer_removed_chat(call, tgt):
                return
            set_hidden_finance_mode(tgt, False)
            set_quick_balance_behavior(tgt, "first")
            set_quick_balance_enabled(tgt, True)
            schedule_quick_balance_first_recreate(tgt, 60.0)
            # Не трогаем личное окно владельца при изменении настроек другого чата.
            safe_edit(
                bot,
                call,
                build_finance_mode_config_text(tgt),
                reply_markup=build_finance_mode_config_menu(day_key, tgt)
            )
            return
        if cmd.startswith("qb_hidden_toggle_"):
            tgt = int(cmd.split("_")[-1])
            if answer_removed_chat(call, tgt):
                return
            new_hidden = not is_hidden_finance_mode(tgt)
            if new_hidden:
                # Скрытые финансы независимы от трёх режимов: не сбрасываем quick_balance/normal.
                set_finance_mode(tgt, True)
                set_hidden_finance_mode(tgt, True)
            else:
                # Выключаем только скрытый режим. Остальные выбранные режимы остаются как были.
                set_hidden_finance_mode(tgt, False)
            safe_edit(
                bot,
                call,
                build_finance_mode_config_text(tgt),
                reply_markup=build_finance_mode_config_menu(day_key, tgt)
            )
            return
        if cmd.startswith("qb_finwin_open_"):
            tgt = int(cmd.split("_")[-1])
            if answer_removed_chat(call, tgt):
                return
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
        if cmd.startswith("fw_finmode_pick_"):
            tgt = int(cmd.split("_")[-1])
            if answer_removed_chat(call, tgt):
                return
            safe_edit(
                bot,
                call,
                build_finance_mode_config_text(tgt),
                reply_markup=build_finance_mode_config_menu(day_key, tgt)
            )
            return
        if cmd.startswith("fin_mode_toggle_"):
            tgt = int(cmd.split("_")[-1])
            if answer_removed_chat(call, tgt):
                return
            if is_finance_mode(tgt):
                set_hidden_finance_mode(tgt, False)
                set_quick_balance_behavior(tgt, "normal")
                set_quick_balance_enabled(tgt, False)
                set_finance_mode(tgt, False)
            else:
                set_finance_mode(tgt, True)
                set_quick_balance_behavior(tgt, "normal")
                set_quick_balance_enabled(tgt, False)
                # По ТЗ при простом включении финрежима включаем скрытый режим, остальные режимы остаются ❌.
                set_hidden_finance_mode(tgt, True)
            safe_edit(
                bot,
                call,
                build_finance_mode_config_text(tgt),
                reply_markup=build_finance_mode_config_menu(day_key, tgt)
            )
            return
        if cmd.startswith("fin_mode_off_"):
            tgt = int(cmd.split("_")[-1])
            if answer_removed_chat(call, tgt):
                return
            set_hidden_finance_mode(tgt, False)
            set_finance_mode(tgt, False)
            set_quick_balance_behavior(tgt, "normal")
            set_quick_balance_enabled(tgt, False)
            save_data(data)
            safe_edit(
                bot,
                call,
                build_finance_mode_config_text(tgt),
                reply_markup=build_finance_mode_config_menu(day_key, tgt)
            )
            return
        if cmd == "pick_date":
            try:
                cdt = datetime.strptime(day_key, "%Y-%m-%d")
            except Exception:
                cdt = now_local()
            safe_edit(bot, call, "📅 Выберите день:", reply_markup=build_calendar_keyboard(cdt, chat_id))
            return
        if cmd == "cancel_edit":
            clear_edit_wait_state(chat_id, call.message.message_id, delete_prompt=True)
            try:
                bot.answer_callback_query(call.id, "Редактирование отменено")
            except Exception:
                pass
            return
    except Exception as e:
        log_error(f"on_callback error: {e}")
def send_csv_week(chat_id: int, day_key: str):
    if is_finance_output_suppressed(chat_id):
        return
    try:
        store = get_chat_store(chat_id)

        base = datetime.strptime(day_key, "%Y-%m-%d")
        start = base - timedelta(days=6)

        rows = []

        for i in range(7):
            d = (start + timedelta(days=i)).strftime("%Y-%m-%d")
            for r in store.get("daily_records", {}).get(d, []):
                rows.append((fmt_date_backup(d), fmt_csv_amount(r["amount"]), r.get("note", "")))

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
    if is_finance_output_suppressed(chat_id):
        return
    try:
        store = get_chat_store(chat_id)

        base = datetime.strptime(day_key, "%Y-%m-%d")
        start = base.replace(day=1)

        rows = []

        for d, recs in store.get("daily_records", {}).items():
            dt = datetime.strptime(d, "%Y-%m-%d")
            if dt >= start and dt <= base:
                for r in recs:
                    rows.append((fmt_date_backup(d), fmt_csv_amount(r["amount"]), r.get("note", "")))

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
    if is_finance_output_suppressed(chat_id):
        return
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
                rows.append((fmt_date_backup(d), fmt_csv_amount(r["amount"]), r.get("note", "")))

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
    bot_journal("record_add_start", chat_id, f"amount={amount} note={note}")
    with locked_chat(chat_id):
        store = get_chat_store(chat_id)
        rid = store.get("next_id", 1)

        if not day_key:
            day_key = day_key_from_message(source_msg)

        source_msg_id = getattr(source_msg, "message_id", None) if source_msg else None
        source_order_msg_id = (
            getattr(source_msg, "source_order_msg_id", None)
            or getattr(source_msg, "forward_source_msg_id", None)
            or source_msg_id
        )

        rec = {
            "id": rid,
            "short_id": "",
            "timestamp": message_timestamp_iso(source_msg),
            "amount": amount,
            "note": note,
            "source_msg_id": source_msg_id,
            "source_order_msg_id": source_order_msg_id,
            "owner": owner,
            "msg_id": source_msg_id,
            "origin_msg_id": source_msg_id,
            "day_key": day_key,
        }

        store.setdefault("records", []).append(rec)
        normalize_chat_records(chat_id)
        store["next_id"] = max([int(r.get("id", 0) or 0) for r in store.get("records", [])] + [0]) + 1
        store["balance"] = sum(float(r.get("amount", 0) or 0) for r in store.get("records", []))

        rebuild_month_short_ids(chat_id)
        rebuild_global_records()

def delete_record_in_chat(chat_id: int, rid: int):
    with locked_chat(chat_id):
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
    """Перенумеровывает записи по реальной хронологии поступления сообщений."""
    store = get_chat_store(chat_id)
    normalize_chat_records(chat_id)
    all_recs = list(store.get("records", []) or [])
    all_recs.sort(key=record_sort_key)

    for new_id, r in enumerate(all_recs, 1):
        r["id"] = new_id

    store["records"] = all_recs
    rebuilt_daily = {}
    for r in all_recs:
        rebuilt_daily.setdefault(_record_day_key(r), []).append(r)
    store["daily_records"] = rebuilt_daily
    store["next_id"] = len(all_recs) + 1
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

def clear_active_window_id(chat_id: int, day_key: str):
    try:
        aw = get_or_create_active_windows(chat_id)
        if str(day_key) in aw:
            aw.pop(str(day_key), None)
            save_data(data)
    except Exception as e:
        log_error(f"clear_active_window_id({chat_id},{day_key}): {e}")

def close_previous_main_window_before_back(chat_id: int, day_key: str, current_message_id: int | None = None):
    """При возврате в основное окно удаляет прежнее О1, чтобы не оставалось дубля."""
    try:
        old_mid = get_active_window_id(chat_id, day_key)
        if not old_mid:
            return
        if current_message_id is not None and int(old_mid) == int(current_message_id):
            return
        try:
            bot.delete_message(int(chat_id), int(old_mid))
        except Exception:
            pass
        clear_active_window_id(chat_id, day_key)
    except Exception as e:
        log_error(f"close_previous_main_window_before_back({chat_id},{day_key}): {e}")
def update_or_send_day_window(chat_id: int, day_key: str):
    if is_hidden_finance_mode(chat_id) and not is_owner_chat(chat_id):
        return
    if is_owner_chat(chat_id):
        backup_window_for_owner(chat_id, day_key)
        schedule_balance_panel_refresh(chat_id, 0.5)
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
                schedule_balance_panel_refresh(chat_id, 0.5)
                return
            except Exception as e:
                err = str(e).lower()
                if "message is not modified" in err:
                    schedule_balance_panel_refresh(chat_id, 0.5)
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
    return store.get("finance_mode", False)

def set_finance_mode(chat_id: int, enabled: bool):
    chat_id = int(chat_id)
    store = get_chat_store(chat_id)
    enabled = bool(enabled)
    store["finance_mode"] = enabled

    if enabled:
        finance_active_chats.add(chat_id)
        # Фин-режим сам по себе = "как обычно": основное окно через 10 сообщений,
        # без быстрого остатка. Быстрый остаток включается только если владелец
        # явно выбрал режим в меню быстрого остатка (quick_balance_user_selected=True).
        settings = store.setdefault("settings", {})
        settings.setdefault("quick_balance_user_selected", False)
        if not settings.get("quick_balance_user_selected", False):
            settings["quick_balance_enabled"] = False
            settings["quick_balance_behavior"] = "normal"
            panel_id = store.get("balance_panel_id")
            if panel_id:
                try:
                    bot.delete_message(chat_id, panel_id)
                except Exception:
                    pass
            store["balance_panel_id"] = None
            store["balance_panel_mode"] = "normal"
    else:
        finance_active_chats.discard(chat_id)
        panel_id = store.get("balance_panel_id")
        if panel_id:
            try:
                bot.delete_message(chat_id, panel_id)
            except Exception:
                pass
        store["balance_panel_id"] = None
        store["balance_panel_mode"] = "mini"
    save_data(data)
    schedule_config_backup_for_chats(chat_id)

def require_finance(chat_id: int) -> bool:
    """
    Проверка: включён ли финансовый режим.
    Если нет — показываем подсказку /поехали.
    """
    if not is_finance_mode(chat_id):
        send_and_auto_delete(chat_id, "⚙️ Финансовый режим выключен.\nАктивируйте командой /ok")
        return False
    return True
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
        chat_bal = store.get("balance", 0)
        if not is_owner_chat(chat_id):
            text = wm_common(f"💰 Общий итог по этому чату: {fmt_num(chat_bal)}", 4)
        else:
            lines = []
            info = store.get("info", {})
            title = get_chat_display_name(chat_id)
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
                title2 = get_chat_display_name(cid_int)
                other_lines.append(f"   • {title2}: {fmt_num(bal)}")
            if other_lines:
                lines.append("")
                lines.append("• Другие чаты:")
                lines.extend(other_lines)
            lines.append("")
            lines.append(f"• Всего по всем чатам: {fmt_num(total_all)}")
            text = "\n".join(lines)
        bot.edit_message_text(
            text,
            chat_id=chat_id,
            message_id=msg_id,
            parse_mode="HTML"
        )
        if is_owner_chat(chat_id):
            schedule_owner_total_window_delete(chat_id, msg_id)
    except Exception as e:
        if "message is not modified" in str(e).lower():
            if is_owner_chat(chat_id):
                schedule_owner_total_window_delete(chat_id, msg_id)
            return
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
        backup_window_for_owner(owner_chat_id, owner_day_key, None)
        refresh_balance_panel_now(owner_chat_id)
        refresh_total_message_if_any(owner_chat_id)
    except Exception as e:
        log_error(f"refresh_owner_after_chat_change({source_chat_id}): {e}")


def cancel_pending_window_commands(chat_id: int, delete_prompt: bool = False):
    """Назад в основное окно отменяет режимы ожидания предыдущих окон и их таймеры."""
    try:
        clear_edit_wait_state(chat_id, delete_prompt=delete_prompt)
    except Exception:
        pass
    try:
        clear_finwin_edit_wait_state(chat_id, delete_prompt=delete_prompt)
    except Exception:
        pass
    try:
        clear_category_wait_state(chat_id, "category_add_wait", delete_prompt=delete_prompt)
    except Exception:
        pass
    try:
        clear_category_wait_state(chat_id, "category_edit_wait", delete_prompt=delete_prompt)
    except Exception:
        pass
    try:
        _clear_secret_wait(chat_id, delete_prompt=delete_prompt)
    except Exception:
        pass
    try:
        store = get_chat_store(chat_id)
        if store.get("reset_wait"):
            store["reset_wait"] = False
            store["reset_time"] = 0
            save_data(data)
    except Exception:
        pass


def send_info(chat_id: int, text: str):
    send_and_auto_delete(chat_id, text, HELPER_DELETE_DELAY)


@bot.message_handler(commands=["owners", "additional_owners", "доп_владельцы"])
def cmd_additional_owners(msg):
    schedule_command_delete(msg)
    if not is_primary_owner(msg.chat.id):
        return
    bot.send_message(
        msg.chat.id,
        wm_owner("👥 Дополнительные владельцы\n\n✅ — доступ владельца включён\n❌ — доступ выключен", 36),
        reply_markup=build_additional_owners_keyboard(),
    )


@bot.message_handler(commands=["windows", "okna", "окна"])
def cmd_windows_in_current_message(msg):
    schedule_command_delete(msg)
    enabled = toggle_chat_buttons_current_window(msg.chat.id)
    send_and_auto_delete(
        msg.chat.id,
        f"{'✅' if enabled else '❌'} Режим открытия в текущем окне: {'ВКЛ' if enabled else 'ВЫКЛ'}",
        8,
    )
                
@bot.message_handler(commands=["ok", "поехали"])
def cmd_ok(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass

    schedule_command_delete(msg)
    chat_id = msg.chat.id
    set_total_secret_mode(chat_id, False)
    if is_finance_output_suppressed(chat_id):
        return
    stop_dozvon_for_target(chat_id)
    store = get_chat_store(chat_id)

    set_finance_mode(chat_id, True)
    store["current_view_day"] = today_key()
    store.setdefault("settings", {})["auto_add"] = True

    save_data(data)
    schedule_finalize(chat_id, today_key())

    send_and_auto_delete(chat_id, "✅ Финансовый режим включён", HELPER_DELETE_DELAY)
@bot.message_handler(commands=["start"])
def cmd_start(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass

    schedule_command_delete(msg)
    chat_id = msg.chat.id
    set_total_secret_mode(chat_id, False)
    if is_finance_output_suppressed(chat_id):
        return
    stop_dozvon_for_target(chat_id)

    if guard_non_owner_finance_for_command(msg, {"ok", "help"}):
        return
    if not require_finance(chat_id):
        return

    day_key = today_key()
    get_chat_store(chat_id)["current_view_day"] = day_key
    force_new_day_window(chat_id, day_key)
@bot.message_handler(commands=["help"])
def cmd_help(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass
    schedule_command_delete(msg)
    chat_id = msg.chat.id
    if is_finance_output_suppressed(chat_id):
        return
    stop_dozvon_for_target(chat_id)
    help_text = build_help_text(chat_id)
    send_and_auto_delete(chat_id, help_text, HELPER_DELETE_DELAY)

@bot.message_handler(commands=["articles", "статьи"])
def cmd_articles(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass
    schedule_command_delete(msg)
    chat_id = msg.chat.id
    if is_finance_output_suppressed(chat_id):
        return
    send_and_auto_delete(chat_id, build_articles_description_text(chat_id), 40)
    
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
@bot.message_handler(commands=["prev"])
def cmd_prev(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass

    schedule_command_delete(msg)
    chat_id = msg.chat.id
    if is_finance_output_suppressed(chat_id):
        return
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
    if is_finance_output_suppressed(chat_id):
        return
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
    if is_finance_output_suppressed(chat_id):
        return
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
    if is_finance_output_suppressed(chat_id):
        return
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
    if is_finance_output_suppressed(chat_id):
        return
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
                caption=f"📂 Общий CSV: {get_chat_display_name(chat_id)}"
            )
    except Exception as e:
        log_error(f"cmd_csv_all: {e}")
def cmd_csv_day(chat_id: int, day_key: str):
    """CSV только за один день для текущего чата, date DD:MM:YY."""
    if is_finance_output_suppressed(chat_id):
        return
    if not require_finance(chat_id):
        return
    store = get_chat_store(chat_id)
    day_recs = sorted(store.get("daily_records", {}).get(day_key, []) or [], key=record_sort_key)
    if not day_recs:
        send_info(chat_id, "Нет записей за этот день.")
        return
    tmp_name = f"data_{chat_id}_{day_key}.csv"
    try:
        with open(tmp_name, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["date", "chat", "ID", "short_id", "timestamp", "amount", "note", "owner", "day_key"])
            rows = []
            for r in day_recs:
                rows.append((
                    fmt_date_backup(day_key),
                    get_chat_display_name(chat_id),
                    r.get("id"),
                    r.get("short_id"),
                    r.get("timestamp"),
                    fmt_csv_amount(r.get("amount")),
                    r.get("note"),
                    r.get("owner"),
                    day_key,
                ))
            write_csv_rows_with_day_gaps(w, rows, 9)
        with open(tmp_name, "rb") as f:
            bot.send_document(chat_id, f, caption=f"📅 CSV за день {fmt_date_backup(day_key)}: {get_chat_display_name(chat_id)}")
    except Exception as e:
        log_error(f"cmd_csv_day: {e}")
    finally:
        try:
            os.remove(tmp_name)
        except FileNotFoundError:
            pass

@bot.message_handler(commands=["tabl_lsx"])
def cmd_tabl_lsx(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass
    schedule_command_delete(msg)
    chat_id = msg.chat.id
    if is_finance_output_suppressed(chat_id):
        return
    stop_dozvon_for_target(chat_id)
    if guard_non_owner_finance_for_command(msg, {"ok", "help", "tabl_lsx"}):
        return
    if not require_finance(chat_id):
        return
    send_tabl_lsx_for_chat(chat_id, chat_id)


@bot.message_handler(commands=["xlsx", "excel"])
def cmd_xlsx(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass
    schedule_command_delete(msg)
    chat_id = msg.chat.id
    if is_finance_output_suppressed(chat_id):
        return
    stop_dozvon_for_target(chat_id)
    if guard_non_owner_finance_for_command(msg, {"ok", "help"}):
        return
    if not require_finance(chat_id):
        return
    send_export_for_chat_to(chat_id, chat_id, "all", today_key(), "xlsx")

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
    if is_finance_output_suppressed(chat_id):
        return
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
    if is_finance_output_suppressed(chat_id):
        return
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
    if is_finance_output_suppressed(chat_id):
        return
    stop_dozvon_for_target(chat_id)
    if guard_non_owner_finance_for_command(msg, {"ok", "help"}):
        return
    if not require_finance(chat_id):
        return
    store = get_chat_store(chat_id)
    store["reset_wait"] = True
    store["reset_time"] = time.time()
    save_data(data)
    send_and_auto_delete(
        chat_id,
        "⚠️ Вы уверены, что хотите обнулить данные? Напишите ДА в течение 15 секунд.",
        15
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
    if is_finance_output_suppressed(chat_id):
        return
    stop_dozvon_for_target(chat_id)
    if guard_non_owner_finance_for_command(msg, {"ok", "help"}):
        return
    if not is_owner_chat(chat_id):
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
    if is_finance_output_suppressed(chat_id):
        return
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
    if is_finance_output_suppressed(chat_id):
        return
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
    if is_finance_output_suppressed(chat_id):
        return
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

def send_and_auto_delete(chat_id: int, text: str, delay: int = HELPER_DELETE_DELAY):
    if is_finance_output_suppressed(chat_id):
        return
    if chat_buttons_current_window_enabled(chat_id):
        send_or_edit_stored_window(chat_id, "command_window_id", text, delay=delay)
        return
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
    if is_finance_output_suppressed(chat_id):
        return
    if chat_buttons_current_window_enabled(chat_id):
        send_or_edit_stored_window(chat_id, "command_window_id", html_text, parse_mode="HTML", delay=delay)
        return
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


def clear_edit_wait_state(chat_id: int, expected_prompt_id: int | None = None, delete_prompt: bool = True):
    store = get_chat_store(chat_id)
    edit_wait = store.get("edit_wait") or {}
    prompt_id = edit_wait.get("prompt_msg_id")

    if expected_prompt_id is not None and prompt_id and int(prompt_id) != int(expected_prompt_id):
        return False

    key = (int(chat_id), "edit_wait")
    prev = _edit_cancel_timers.get(key)
    if prev and prev.is_alive():
        try:
            prev.cancel()
        except Exception:
            pass
        _edit_cancel_timers.pop(key, None)

    store["edit_wait"] = None
    save_data(data)

    if delete_prompt and prompt_id:
        try:
            bot.delete_message(chat_id, int(prompt_id))
        except Exception:
            pass
    return True


def clear_finwin_edit_wait_state(chat_id: int, expected_prompt_id: int | None = None, delete_prompt: bool = True):
    store = get_chat_store(chat_id)
    edit_wait = store.get("finwin_edit_wait") or {}
    prompt_id = edit_wait.get("prompt_msg_id")

    if expected_prompt_id is not None and prompt_id and int(prompt_id) != int(expected_prompt_id):
        return False

    key = (int(chat_id), "finwin_edit_wait")
    prev = _edit_cancel_timers.get(key)
    if prev and prev.is_alive():
        try:
            prev.cancel()
        except Exception:
            pass
        _edit_cancel_timers.pop(key, None)

    store["finwin_edit_wait"] = None
    save_data(data)

    if delete_prompt and prompt_id:
        try:
            bot.delete_message(chat_id, int(prompt_id))
        except Exception:
            pass
    return True


def _edit_countdown_text(base_text: str, remaining: int) -> str:
    base = strip_window_mark(str(base_text or "")).rstrip()
    return wm_common(base + f"\n\n⏳ До закрытия: {int(remaining)} сек.", 10)


def schedule_cancel_finwin_edit(chat_id: int, prompt_message_id: int, delay: float = 40.0):
    key = (int(chat_id), "finwin_edit_wait")

    def _job():
        try:
            total = int(delay)
            while total > 0:
                store = get_chat_store(chat_id)
                wait = store.get("finwin_edit_wait") or {}
                if not wait or int(wait.get("prompt_msg_id") or 0) != int(prompt_message_id):
                    return
                base_text = wait.get("countdown_base_text") or "✏️ Редактирование записи"
                target_chat_id = int(wait.get("target_chat_id") or chat_id)
                day_key = wait.get("day_key") or today_key()
                owner_day_key = wait.get("owner_day_key") or today_key()
                insert_text = wait.get("insert_text") or ""
                try:
                    _tg_call_retry(
                        bot.edit_message_text,
                        _edit_countdown_text(base_text, total),
                        chat_id=chat_id,
                        message_id=int(prompt_message_id),
                        reply_markup=build_finwin_cancel_edit_keyboard(target_chat_id, day_key, owner_day_key, insert_text=insert_text),
                        purpose="finwin_edit_countdown",
                    )
                except Exception as e:
                    if "message is not modified" not in str(e).lower():
                        log_error(f"finwin edit countdown {chat_id}:{prompt_message_id}: {e}")
                time.sleep(1)
                total -= 1
            cleared = clear_finwin_edit_wait_state(chat_id, prompt_message_id, delete_prompt=True)
            if cleared:
                log_info(f"finwin edit_wait auto-cancelled for chat {chat_id}")
        except Exception as e:
            log_error(f"schedule_cancel_finwin_edit({chat_id},{prompt_message_id}): {e}")

    prev = _edit_cancel_timers.get(key)
    if prev and prev.is_alive():
        try:
            prev.cancel()
        except Exception:
            pass
    t = threading.Timer(0.0, _job)
    _edit_cancel_timers[key] = t
    t.start()


def schedule_cancel_edit(chat_id: int, prompt_message_id: int, delay: float = 40.0):
    key = (int(chat_id), "edit_wait")

    def _job():
        try:
            total = int(delay)
            while total > 0:
                store = get_chat_store(chat_id)
                wait = store.get("edit_wait") or {}
                if not wait or int(wait.get("prompt_msg_id") or 0) != int(prompt_message_id):
                    return
                base_text = wait.get("countdown_base_text") or "✏️ Редактирование записи"
                day_key = wait.get("day_key") or today_key()
                insert_text = wait.get("insert_text") or ""
                try:
                    _tg_call_retry(
                        bot.edit_message_text,
                        _edit_countdown_text(base_text, total),
                        chat_id=chat_id,
                        message_id=int(prompt_message_id),
                        reply_markup=build_cancel_edit_keyboard(day_key, insert_text=insert_text),
                        purpose="edit_countdown",
                    )
                except Exception as e:
                    if "message is not modified" not in str(e).lower():
                        log_error(f"edit countdown {chat_id}:{prompt_message_id}: {e}")
                time.sleep(1)
                total -= 1
            cleared = clear_edit_wait_state(chat_id, prompt_message_id, delete_prompt=True)
            if cleared:
                send_and_auto_delete(chat_id, "⌛ Время редактирования истекло. Режим редактирования отменён.", 8)
        except Exception as e:
            log_error(f"schedule_cancel_edit({chat_id},{prompt_message_id}): {e}")

    prev = _edit_cancel_timers.get(key)
    if prev and prev.is_alive():
        try:
            prev.cancel()
        except Exception:
            pass
    t = threading.Timer(0.0, _job)
    _edit_cancel_timers[key] = t
    t.start()

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
        except Exception as e:
            log_error(f"schedule_cancel_wait job: {e}")

    prev = _edit_cancel_timers.get(chat_id)
    if prev and prev.is_alive():
        try:
            prev.cancel()
        except Exception:
            pass

    t = threading.Timer(delay, _job)
    _edit_cancel_timers[chat_id] = t
    t.start()
    
def update_chat_info_from_message(msg):
    """
    Обновляет информацию о чате в памяти.
    На диск пишем только если реально что-то изменилось.
    """
    chat_id = msg.chat.id
    was_new_chat = str(chat_id) not in (data.get("chats", {}) if isinstance(data, dict) else {})
    try:
        if not getattr(getattr(msg, "from_user", None), "is_bot", False):
            stop_dozvon_for_target(chat_id)
    except Exception:
        pass
    store = get_chat_store(chat_id)
    try:
        if store.setdefault("settings", {}).get("bot_removed"):
            store["settings"]["bot_removed"] = False
            store["settings"].pop("bot_removed_reason", None)
            store["settings"].pop("bot_removed_at", None)
            save_data(data)
    except Exception:
        pass
    info = store.setdefault("info", {})
    # У каналов/чатов username может отсутствовать. Не обращаемся к ключам напрямую,
    # чтобы не ловить KeyError: 'username' на channel_post / edited_channel_post.
    info.setdefault("title", "")
    info.setdefault("username", None)
    info.setdefault("type", getattr(msg.chat, "type", None))

    changed = False

    # В личных чатах callback приходит от самого бота. Не даём ему затирать имя пользователя названием бота.
    try:
        if getattr(getattr(msg, "from_user", None), "is_bot", False) and not getattr(msg.chat, "title", None):
            return
    except Exception:
        pass

    new_title = _chat_title_from_message(msg, info.get("title") or "")
    new_username = _chat_username_from_message(msg)
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
            "title": info.get("title") or get_chat_display_name(chat_id),
            "username": info.get("username"),
            "type": info.get("type"),
        }

        if kc.get(str(chat_id)) != new_known:
            kc[str(chat_id)] = new_known
            changed = True

    if changed:
        save_data(data)
        # Если имя/username чата изменились, обновляем карточку памяти/known_chats и бэкапы,
        # чтобы у владельца и в backup-файлах отображалось актуальное название.
        try:
            ids_for_backup = [chat_id]
            if OWNER_ID:
                ids_for_backup.append(int(OWNER_ID))
            schedule_config_backup_for_chats(*ids_for_backup, delay=2.0)
        except Exception as e:
            log_error(f"chat info changed backup schedule {chat_id}: {e}")

    try:
        if was_new_chat and OWNER_ID and str(chat_id) != str(OWNER_ID):
            maybe_prompt_owner_for_new_chat_auto_backup(chat_id)
    except Exception as e:
        log_error(f"new chat auto-backup prompt failed for {get_chat_display_name(chat_id)}: {e}")


def maybe_prompt_owner_for_new_chat_auto_backup(chat_id: int):
    """При первом появлении чата спрашиваем владельца, обновлять ли JSON/CSV бэкапы автоматически."""
    if not OWNER_ID:
        return
    store = get_chat_store(chat_id)
    settings = store.setdefault("settings", {})
    if settings.get("owner_auto_backup_prompted"):
        return
    settings["owner_auto_backup_prompted"] = True
    settings.setdefault("auto_backup_enabled", True)
    save_data(data)

    owner_id = int(OWNER_ID)
    title = get_chat_display_name(chat_id)
    text = (
        "🆕 Новый чат появился в картотеке\n\n"
        f"{title}\n"
        "Автоматически обновлять JSON/CSV бэкапы по этому чату?"
    )
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.row(
        IB("✅ Да", callback_data=f"ncb:{chat_id}:yes"),
        IB("❌ Нет", callback_data=f"ncb:{chat_id}:no"),
    )
    msg = bot.send_message(owner_id, text, reply_markup=kb)
    settings["owner_auto_backup_prompt_msg_id"] = msg.message_id
    save_data(data)
    delete_message_later(owner_id, msg.message_id, 10)



def _safe_tmp_json_name(fname: str) -> str:
    base = os.path.basename(str(fname or "backup.json"))
    base = re.sub(r"[^0-9A-Za-zА-Яа-я_.\-]+", "_", base)
    if not base.lower().endswith(".json"):
        base += ".json"
    return base[:80]


def _extract_chat_id_from_json_filename(fname: str):
    """Пытается вытащить chat_id из имени data_<chat_id>.json."""
    try:
        m = re.search(r"data_(-?\d+)\.json$", str(fname or "").strip().lower())
        if m:
            return int(m.group(1))
    except Exception:
        pass
    return None


def _describe_json_restore_payload(payload, fname: str = ""):
    """Возвращает короткое описание JSON перед подтверждением восстановления."""
    fname_l = str(fname or "").lower()
    if fname_l == "csv_meta.json":
        return "метаданные CSV", None
    if isinstance(payload, dict) and isinstance(payload.get("chats"), dict):
        return f"глобальный data.json, чатов: {len(payload.get('chats') or {})}", None
    if isinstance(payload, dict):
        cid = payload.get("chat_id")
        if cid is None:
            cid = _extract_chat_id_from_json_filename(fname)
        if cid is not None:
            try:
                cid = int(cid)
                rec_count = len(payload.get("records") or []) if isinstance(payload.get("records"), list) else 0
                daily = payload.get("daily_records") or {}
                if isinstance(daily, dict):
                    rec_count = rec_count or sum(len(v or []) for v in daily.values())
                return f"JSON чата {get_chat_display_name(cid)} / ID {cid}, записей: {rec_count}", cid
            except Exception:
                pass
    return "JSON-файл неизвестного формата", None


def _apply_json_restore_from_owner_prompt(owner_chat_id: int, tmp_path: str, fname: str) -> str:
    """
    Восстановление JSON, когда владелец прислал файл без /restore и нажал ✅ Да.
    Поддерживает:
    • глобальный data.json / JSON с ключом chats;
    • csv_meta.json;
    • per-chat JSON data_<chat_id>.json или JSON с chat_id.
    """
    global data, restore_mode

    fname_l = str(fname or "").lower()
    payload = _load_json(tmp_path, None)
    if not isinstance(payload, dict):
        raise RuntimeError("JSON повреждён или не является объектом")

    # csv_meta.json
    if fname_l == "csv_meta.json":
        os.replace(tmp_path, CSV_META_FILE)
        _save_csv_meta(_load_json(CSV_META_FILE, {}) or {})
        restore_mode = None
        return "🟢 csv_meta.json обновлён"

    # Глобальный data.json
    if fname_l == "data.json" or isinstance(payload.get("chats"), dict):
        os.replace(tmp_path, DATA_FILE)
        _import_legacy_global_json_to_db(DATA_FILE, force=True)
        data.clear()
        data.update(load_data())
        rebuild_global_records()
        save_data(data)
        export_global_csv(data)
        restore_mode = None
        return "🟢 Глобальный data.json обновлён"

    # JSON конкретного чата
    target_chat_id = payload.get("chat_id")
    if target_chat_id is None:
        target_chat_id = _extract_chat_id_from_json_filename(fname_l)
    if target_chat_id is None:
        raise RuntimeError("В JSON нет chat_id и его нельзя понять из имени файла")
    target_chat_id = int(target_chat_id)

    # Восстанавливаем именно тот чат, к которому относится файл, даже если файл прислан владельцу.
    restore_from_json(target_chat_id, tmp_path)
    day_key = get_chat_store(target_chat_id).get("current_view_day", today_key())
    finance_changed(target_chat_id, day_key, reason="owner_json_restore", delay=0.1)
    restore_mode = None
    return f"🟢 JSON чата обновлён: {get_chat_display_name(target_chat_id)}"


def _cleanup_owner_json_restore_prompt(key: int, remove_prompt: bool = False):
    try:
        with _owner_json_restore_prompt_lock:
            item = _owner_json_restore_prompts.pop(int(key), None)
        if not item:
            return
        if remove_prompt:
            try:
                bot.delete_message(int(OWNER_ID), int(item.get("prompt_msg_id")))
            except Exception:
                pass
        tmp_path = item.get("tmp_path")
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except Exception:
                pass
    except Exception as e:
        log_error(f"_cleanup_owner_json_restore_prompt({key}): {e}")


def _schedule_owner_json_restore_prompt_cleanup(key: int, delay: int = 12):
    def _job():
        _cleanup_owner_json_restore_prompt(key, remove_prompt=True)
    try:
        threading.Timer(delay, _job).start()
    except Exception as e:
        log_error(f"_schedule_owner_json_restore_prompt_cleanup({key}): {e}")


def maybe_prompt_owner_for_json_restore(msg, fname: str) -> bool:
    """
    Если владелец прислал .json в личку без /restore — спрашиваем, обновлять данные или нет.
    Кнопки/окно удаляются через 10 секунд в любом случае.
    """
    try:
        if not is_owner_chat(msg.chat.id):
            return False
        if restore_mode is not None:
            return False
        if not str(fname or "").lower().endswith((".json", ".ison")):
            return False

        file_info = bot.get_file(msg.document.file_id)
        raw = bot.download_file(file_info.file_path)

        tmp_name = f"owner_json_restore_{int(msg.chat.id)}_{int(msg.message_id)}_{_safe_tmp_json_name(fname)}"
        with open(tmp_name, "wb") as f:
            f.write(raw)

        payload = _load_json(tmp_name, None)
        if not isinstance(payload, dict):
            try:
                os.remove(tmp_name)
            except Exception:
                pass
            send_and_auto_delete(int(msg.chat.id), f"⚠️ JSON не прочитан или повреждён: {fname}", 10)
            return True

        desc, target_chat_id = _describe_json_restore_payload(payload, fname)
        key = int(msg.message_id)
        text = (
            "🧾 В чате владельца появился JSON-файл без /restore\n\n"
            f"Файл: {fname}\n"
            f"Что внутри: {desc}\n\n"
            "Обновить данные бота из этого JSON?"
        )
        kb = types.InlineKeyboardMarkup(row_width=2)
        kb.row(
            IB("✅ Да", callback_data=f"ojr:{key}:yes"),
            IB("❌ Нет", callback_data=f"ojr:{key}:no"),
        )
        sent = bot.send_message(int(msg.chat.id), text, reply_markup=kb)
        with _owner_json_restore_prompt_lock:
            _owner_json_restore_prompts[key] = {
                "tmp_path": tmp_name,
                "fname": fname,
                "prompt_msg_id": sent.message_id,
                "created_at": time.time(),
                "target_chat_id": target_chat_id,
            }
        delete_message_later(int(msg.chat.id), sent.message_id, 10)
        _schedule_owner_json_restore_prompt_cleanup(key, 12)
        return True
    except Exception as e:
        log_error(f"maybe_prompt_owner_for_json_restore({fname}): {e}")
        return False


def run_owner_json_restore_prompt_job(owner_chat_id: int, item: dict):
    tmp_path = item.get("tmp_path")
    fname = item.get("fname") or "backup.json"
    try:
        # Для глобального файла защищаем data_lock, для per-chat restore_from_json уже сохраняет данные.
        with data_lock:
            result = _apply_json_restore_from_owner_prompt(owner_chat_id, tmp_path, fname)
        send_and_auto_delete(owner_chat_id, result, 10)
    except Exception as e:
        send_and_auto_delete(owner_chat_id, f"❌ JSON не обновлён: {e}", 12)
    finally:
        try:
            if tmp_path and os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass



# ─────────────────────────────────────────────────────────────
# v27: единая модель финансовых записей
# ─────────────────────────────────────────────────────────────
def _record_day_key(rec: dict) -> str:
    """Безопасно возвращает day_key для записи."""
    dk = rec.get("day_key")
    if dk:
        return str(dk)[:10]
    ts = rec.get("timestamp") or ""
    if isinstance(ts, str) and len(ts) >= 10 and re.match(r"\d{4}-\d{2}-\d{2}", ts[:10]):
        rec["day_key"] = ts[:10]
        return ts[:10]
    rec["day_key"] = today_key()
    return rec["day_key"]


def normalize_chat_records(chat_id: int) -> None:
    """
    v33: records — основной источник, daily_records строится из него.
    Сортировка стабильная: Telegram date + исходный message_id, чтобы 1 2 3 4 не превращалось в 1 2 4 3.
    """
    store = get_chat_store(chat_id)
    records = store.get("records")
    daily = store.get("daily_records") or {}

    if not isinstance(records, list) or not records:
        rebuilt = []
        for dk in sorted(daily.keys()):
            for rec in daily.get(dk, []) or []:
                if isinstance(rec, dict):
                    rec.setdefault("day_key", dk)
                    rebuilt.append(rec)
        records = rebuilt

    clean = []
    for rec in records or []:
        if not isinstance(rec, dict):
            continue
        rec.setdefault("timestamp", now_local().isoformat(timespec="seconds"))
        rec.setdefault("amount", 0)
        rec.setdefault("note", "")
        rec.setdefault("owner", "")
        rec.setdefault("source_order_msg_id", rec.get("source_msg_id") or rec.get("origin_msg_id") or rec.get("msg_id") or rec.get("id") or 0)
        _record_day_key(rec)
        clean.append(rec)

    clean.sort(key=record_sort_key)
    store["records"] = clean

    rebuilt_daily = {}
    for rec in clean:
        rebuilt_daily.setdefault(_record_day_key(rec), []).append(rec)
    store["daily_records"] = rebuilt_daily


def recalc_balance(chat_id: int):
    normalize_chat_records(chat_id)
    store = get_chat_store(chat_id)
    store["balance"] = sum(float(r.get("amount", 0) or 0) for r in store.get("records", []))


def rebuild_month_short_ids(chat_id: int):
    """Пересчитывает short_id как месячную нумерацию по стабильной хронологии."""
    normalize_chat_records(chat_id)
    store = get_chat_store(chat_id)
    daily = store.get("daily_records", {}) or {}
    month_counters = {}

    for dk in sorted(daily.keys()):
        month_key = dk[:7]
        month_counters.setdefault(month_key, 1)
        recs = sorted(daily.get(dk, []) or [], key=record_sort_key)
        daily[dk] = recs
        for r in recs:
            r["short_id"] = f"R{month_counters[month_key]}"
            month_counters[month_key] += 1

    store["records"] = [r for dk in sorted(daily.keys()) for r in daily.get(dk, [])]


def calc_day_balance(store: dict, day_key: str) -> float:
    total = 0.0
    daily = store.get("daily_records", {}) or {}
    for dk in sorted(daily.keys()):
        if dk > day_key:
            break
        for r in daily.get(dk, []) or []:
            total += float(r.get("amount", 0) or 0)
    return total


def rebuild_global_records():
    with data_lock:
        all_recs = []
        for cid, st in list((data.get("chats", {}) or {}).items()):
            try:
                normalize_chat_records(int(cid))
            except Exception:
                pass
            all_recs.extend(st.get("records", []) or [])
        data["records"] = all_recs
        data["overall_balance"] = sum(float(r.get("amount", 0) or 0) for r in all_recs)

_finalize_timers = {}
_backup_timers = {}
_balance_panel_refresh_timers = {}
_balance_panel_collapse_timers = {}
_balance_panel_first_timers = {}
_balance_panel_recreate_timers = {}
_total_message_timers = {}
_backup_dirty_chats = set()
_backup_global_timer = None
_backup_run_lock = threading.Lock()

def collect_finance_chat_ids():
    ids = set()
    try:
        for cid, enabled in (data.get("finance_active_chats", {}) or {}).items():
            if enabled:
                ids.add(int(cid))
    except Exception:
        pass
    try:
        for cid in list(finance_active_chats):
            ids.add(int(cid))
    except Exception:
        pass
    try:
        for cid, store in (data.get("chats", {}) or {}).items():
            try:
                int_cid = int(cid)
            except Exception:
                continue
            if store.get("finance_mode") or (OWNER_ID and str(int_cid) == str(OWNER_ID)):
                ids.add(int_cid)
    except Exception:
        pass
    return sorted(ids)


def schedule_startup_main_windows(delay: float = 3.0):
    """После deploy/рестарта создаёт или обновляет основное окно у владельца и финчатов, кроме скрытых."""
    def _job():
        try:
            for cid in collect_finance_chat_ids():
                try:
                    if is_hidden_finance_mode(cid) and not is_owner_chat(cid):
                        continue
                    if is_chat_bot_removed(cid):
                        continue
                    store = get_chat_store(cid)
                    day_key = store.get("current_view_day") or today_key()
                    update_or_send_day_window(cid, day_key)
                    time.sleep(0.25)
                except Exception as e:
                    log_error(f"startup_main_window({get_chat_display_name(cid)}): {e}")
        except Exception as e:
            log_error(f"schedule_startup_main_windows job: {e}")

    try:
        threading.Timer(delay, _job).start()
    except Exception as e:
        log_error(f"schedule_startup_main_windows: {e}")

def schedule_all_finance_backups(delay: float = 10.0):
    for cid in collect_finance_chat_ids():
        schedule_backup_flush(cid, delay=delay)

def _flush_dirty_backups():
    global _backup_global_timer
    if not _backup_run_lock.acquire(blocking=False):
        # Если предыдущий бэкап ещё идёт, не блокируем бота: переносим пачку немного позже.
        with timer_lock:
            if _backup_dirty_chats:
                t = threading.Timer(5.0, _flush_dirty_backups)
                _backup_global_timer = t
                t.start()
        return

    try:
        with timer_lock:
            dirty = sorted(int(x) for x in _backup_dirty_chats)
            _backup_dirty_chats.clear()
            _backup_global_timer = None

        if not dirty:
            return

        # Локальное сохранение/глобальный снимок — без chat_lock и без сетевых вызовов.
        try:
            export_global_csv(data)
            save_data(data)
        except Exception as e:
            log_error(f"_flush_dirty_backups global export/save: {e}")

        any_mega = any(is_backup_to_mega_enabled(cid) for cid in dirty)
        if any_mega:
            try:
                mega_upload_latest_global_backup()
            except Exception as e:
                log_error(f"_flush_dirty_backups mega global upload: {e}")

        month_key_for_mega = current_month_key()

        for cid in dirty:
            trace = ProcessTrace(cid, f"Бэкап: {get_chat_display_name(cid)}").start()
            try:
                if not is_finance_mode(cid):
                    trace.step("финрежим выключен — пропуск")
                    trace.finish("бэкап завершён")
                    continue
                if not is_auto_backup_enabled(cid):
                    trace.step("все авто-бэкапы выключены — пропуск")
                    trace.finish("бэкап завершён")
                    continue

                trace.step("проверяет настройки бэкапов чата")
                trace.step("создаёт локальный JSON")
                trace.step("создаёт локальный CSV")
                trace.step("создаёт локальный Excel")
                save_chat_json(cid)
                trace.step("локальные файлы готовы")

                if is_backup_to_chat_enabled(cid) and can_receive_direct_json_backup(cid) and not is_finance_output_suppressed(cid):
                    trace.step("обновляет прямой JSON-бэкап в чат")
                    send_backup_to_chat(cid)
                else:
                    trace.step("прямой бэкап в чат выключен/не разрешён — пропуск")

                if is_backup_to_channel_enabled(cid):
                    trace.step("обновляет backup-канал JSON+Excel")
                    send_backup_to_channel(cid)
                else:
                    trace.step("бэкап в канал выключен — пропуск")

                if is_backup_to_mega_enabled(cid):
                    trace.step("грузит JSON в MEGA")
                    try:
                        mega_upload_chat_backup_bundle(cid, month_key_for_mega)
                    except Exception as e:
                        log_error(f"_flush_dirty_backups mega chat {cid}: {e}")
                else:
                    trace.step("бэкап в MEGA выключен — пропуск")

                trace.finish("бэкап завершён")
            except Exception as e:
                log_error(f"_flush_dirty_backups chat {cid}: {e}")
                trace.fail(e)
    finally:
        try:
            _backup_run_lock.release()
        except Exception:
            pass


def schedule_backup_flush(chat_id: int, delay: float = 3.0):
    """Debounced backup queue: много операций за короткое время = один flush."""
    global _backup_global_timer
    try:
        if chat_id is not None:
            with timer_lock:
                _backup_dirty_chats.add(int(chat_id))
    except Exception:
        pass

    with timer_lock:
        prev = _backup_global_timer
        if prev and getattr(prev, "is_alive", lambda: False)():
            try:
                prev.cancel()
            except Exception:
                pass
        t = threading.Timer(delay, _flush_dirty_backups)
        _backup_global_timer = t
        t.start()
    
def _safe_stabilize(action_name, func):
    try:
        try:
            bot_journal("process_call_start", None, str(action_name))
        except Exception:
            pass
        res = func()
        try:
            bot_journal("process_call_done", None, str(action_name))
        except Exception:
            pass
        return res
    except Exception as e:
        log_error(f"[STABILIZE ERROR] {action_name}: {e}")
        try:
            bot_journal("process_call_error", None, f"{action_name}: {e}", "ERROR")
        except Exception:
            pass
        return None


def _finance_changed_now(chat_id: int, day_key: str | None = None, reason: str = "change"):
    """
    Единая точка после фин-изменения.
    Важно: Telegram-отправки/редактирования окон и бэкапы не держат chat_lock,
    чтобы кнопки в этом же чате не висели «Загрузка».
    """
    chat_id = int(chat_id)
    day_key = day_key or get_chat_store(chat_id).get("current_view_day") or today_key()
    trace = ProcessTrace(chat_id, f"Фин-процесс: {get_chat_display_name(chat_id)} / {reason}").start()

    try:
        with locked_chat(chat_id):
            store = get_chat_store(chat_id)
            store["current_view_day"] = day_key

            trace.step("получает хранилище чата")
            trace.step("фиксирует текущую дату окна")
            trace.step("нормализует записи чата")
            _safe_stabilize("normalize_chat_records", lambda: normalize_chat_records(chat_id))

            trace.step("вычисляет остатки")
            _safe_stabilize("recalc_balance", lambda: recalc_balance(chat_id))

            trace.step("пересчитывает месячные номера R")
            _safe_stabilize("rebuild_month_short_ids", lambda: rebuild_month_short_ids(chat_id))

            trace.step("пересобирает общие записи")
            _safe_stabilize("rebuild_global_records", rebuild_global_records)

            trace.step("записывает финрежимы в общий словарь")
            trace.step("сохраняет SQLite/data")
            _safe_stabilize("save_data", lambda: save_data(data))

            trace.step("проверяет скрытый финрежим")
            hidden = is_finance_output_suppressed(chat_id)

        # Ниже тяжёлые Telegram-вызовы уже вне chat_lock.
        if not hidden:
            if is_owner_chat(chat_id):
                trace.step("обновляет окно владельца")
                _safe_stabilize("owner_window", lambda: backup_window_for_owner(chat_id, day_key, None))
            else:
                trace.step("обновляет окно дня")
                _safe_stabilize("day_window", lambda: update_or_send_day_window(chat_id, day_key))

            trace.step("обновляет общий итог")
            _safe_stabilize("refresh_total", lambda: refresh_total_message_if_any(chat_id))

            trace.step("обновляет быстрый остаток")
            _safe_stabilize("quick_balance_now", lambda: refresh_balance_panel_now(chat_id))
            _safe_stabilize("quick_balance_schedule", lambda: schedule_balance_panel_refresh(chat_id, BALANCE_PANEL_REFRESH_DELAY))

        trace.step("ставит бэкап в отдельную очередь")
        _safe_stabilize("backup_queue", lambda: schedule_backup_flush(chat_id, 3.0))

        # Важно: действия в других чатах не должны менять личное окно владельца.
        # Поэтому здесь не вызываем backup_window_for_owner/refresh_owner_after_chat_change.

        trace.finish("финобработка завершена")
    except Exception as e:
        trace.fail(e)
        raise


def finance_changed(chat_id: int, day_key: str | None = None, reason: str = "change", delay: float = 0.35):
    """Debounced универсальный финальный пересчёт для одного чата."""
    chat_id = int(chat_id)
    bot_journal("finance_changed_scheduled", chat_id, f"day={day_key} reason={reason} delay={delay}")
    day_key = day_key or get_chat_store(chat_id).get("current_view_day") or today_key()

    def _job():
        _finance_changed_now(chat_id, day_key, reason)

    with timer_lock:
        t_prev = _finalize_timers.get(chat_id)
        if t_prev and getattr(t_prev, "is_alive", lambda: False)():
            try:
                t_prev.cancel()
            except Exception:
                pass
        t = threading.Timer(delay, _job)
        _finalize_timers[chat_id] = t
        t.start()


def schedule_finalize(chat_id: int, day_key: str, delay: float = 0.35):
    """Совместимость со старым кодом: теперь всё идёт через finance_changed()."""
    return finance_changed(chat_id, day_key, reason="schedule_finalize", delay=delay)


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
                # Старое окно могли удалить руками или Telegram уже не даёт его редактировать.
                # Это не критическая ошибка: очищаем сохранённый id и создаём новое окно.
                if any(x in err for x in ("message to edit not found", "message_id_invalid", "message not found")):
                    try:
                        aw = get_or_create_active_windows(chat_id)
                        if aw.get(day_key) == mid:
                            aw.pop(day_key, None)
                            save_data(data)
                    except Exception:
                        pass
                else:
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

def recreate_main_window_now(chat_id: int, day_key: str):
    """Удаляет старое о1, если возможно, и создаёт новое основное окно."""
    try:
        old_mid = get_active_window_id(chat_id, day_key)
        if old_mid:
            try:
                bot.delete_message(chat_id, int(old_mid))
            except Exception:
                pass
            try:
                clear_active_window_id(chat_id, day_key)
            except Exception:
                pass
    except Exception:
        pass
    force_new_day_window(chat_id, day_key)


def force_new_day_window(chat_id: int, day_key: str):
    if is_hidden_finance_mode(chat_id) and not is_owner_chat(chat_id):
        return
    txt, _ = render_day_window(chat_id, day_key)
    kb = build_main_keyboard(day_key, chat_id)
    sent = bot.send_message(chat_id, txt, reply_markup=kb, parse_mode="HTML")
    set_active_window_id(chat_id, day_key, sent.message_id)
    schedule_balance_panel_refresh(chat_id, 0.5)


def return_to_main_window_closing_previous(chat_id: int, day_key: str, current_message_id: int | None = None):
    """Назад в основное окно.
    • если нажали из самого О1 — просто обновляем/перетекаем в О1 без удаления текущего сообщения;
    • если нажали из переходного окна — старое сохранённое О1 удаляем, а текущее окно превращаем в новое О1.
    """
    try:
        if current_message_id is not None:
            _cancel_v98_auto_close(int(chat_id), int(current_message_id))
    except Exception:
        pass

    try:
        old_mid = get_active_window_id(chat_id, day_key)
    except Exception:
        old_mid = None

    # Если текущее окно не является сохранённым О1 — удаляем прежний О1, чтобы не было дублей.
    try:
        if old_mid and current_message_id is not None and int(old_mid) != int(current_message_id):
            try:
                bot.delete_message(int(chat_id), int(old_mid))
            except Exception:
                pass
            clear_active_window_id(chat_id, day_key)
    except Exception as e:
        log_error(f"return_to_main delete old O1({chat_id},{day_key}): {e}")

    # Текущее окно становится О1: без удаления, обычным edit_message_text.
    if current_message_id is not None:
        try:
            set_active_window_id(chat_id, day_key, int(current_message_id))
            if is_owner_chat(chat_id):
                backup_window_for_owner(chat_id, day_key, message_id_override=int(current_message_id))
            else:
                update_or_send_day_window(chat_id, day_key)
            return
        except Exception as e:
            log_error(f"return_to_main edit current to O1({chat_id},{day_key}): {e}")

    # Запасной вариант, если нет текущего сообщения.
    update_or_send_day_window(chat_id, day_key)


def reset_chat_data(chat_id: int):
    """v27: обнуление данных чата без ручного дублирования окон/бэкапов."""
    try:
        with locked_chat(chat_id):
            store = get_chat_store(chat_id)
            cleanup_forward_links(chat_id)
            store["balance"] = 0
            store["records"] = []
            store["daily_records"] = {}
            store["next_id"] = 1
            store["active_windows"] = {}
            clear_edit_wait_state(chat_id, delete_prompt=True)
            store["edit_target"] = None
            store["reset_wait"] = False
            store["reset_time"] = 0
            day_key = store.get("current_view_day", today_key())
            save_data(data)
        finance_changed(chat_id, day_key, reason="reset", delay=0.1)
    except Exception as e:
        log_error(f"reset_chat_data({chat_id}): {e}")


@bot.message_handler(content_types=["document"])
def handle_document(msg):
    global restore_mode, data

    chat_id = msg.chat.id
    update_chat_info_from_message(msg)
    if handle_secret_input_message(msg):
        return
    try:
        if not getattr(getattr(msg, "from_user", None), "is_bot", False):
            bump_quick_balance_recreate_counter(chat_id)
            stop_dozvon_for_target(chat_id)
    except Exception:
        pass

    file = msg.document
    fname = (file.file_name or "").lower()

    log_info(f"[DOC] recv chat={chat_id} restore={restore_mode} fname={fname}")

    # Владелец прислал .json без /restore: спрашиваем, обновлять данные или нет.
    # Если показали вопрос — сам файл дальше не пересылаем и не обрабатываем как обычный документ.
    if restore_mode is None and is_owner_chat(chat_id) and fname.endswith((".json", ".ison")):
        if maybe_prompt_owner_for_json_restore(msg, fname):
            return

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
                        f"JSON относится к чату {get_chat_display_name(int(inner_chat_id))}, а не к текущему {get_chat_display_name(chat_id)}"
                    )

                restore_from_json(chat_id, tmp_path)

                day_key = get_chat_store(chat_id).get(
                    "current_view_day",
                    today_key()
                )
                finance_changed(chat_id, day_key, reason="restore_json", delay=0.1)

                restore_mode = None
                send_and_auto_delete(
                    chat_id,
                    f"🟢 JSON чата {get_chat_display_name(chat_id)} восстановлен"
                )
                return

            if fname.startswith("data_") and fname.endswith(".csv"):
                restore_from_csv(chat_id, tmp_path)

                day_key = get_chat_store(chat_id).get(
                    "current_view_day",
                    today_key()
                )
                finance_changed(chat_id, day_key, reason="restore_csv", delay=0.1)

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
        schedule_forward_any_message(chat_id, msg)
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

    if handle_secret_sequence(msg):
        return
    if handle_secret_input_message(msg):
        return

    try:
        bump_quick_balance_recreate_counter(msg.chat.id)
    except Exception:
        pass

    try:
        stop_dozvon_for_target(msg.chat.id)
    except Exception:
        pass

    try:
        if is_finance_mode(msg.chat.id):
            handle_finance_text(msg)
    except Exception as e:
        log_error(f"channel_post finance failed: {e}")

    try:
        schedule_forward_any_message(msg.chat.id, msg)
    except Exception as e:
        log_error(f"channel_post forward schedule failed: {e}")


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

    if handle_secret_edited_message(msg):
        return

    try:
        if is_finance_mode(msg.chat.id):
            handle_finance_edit(msg)
    except Exception as e:
        log_error(f"edited_channel_post finance edit failed: {e}")

    try:
        schedule_propagate_edited_to_copies(msg)
    except Exception as e:
        log_error(f"edited_channel_post propagate schedule failed: {e}")


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

    if handle_secret_edited_message(msg):
        return

    edit_text = _message_text_for_finance(msg)
    if is_forward_delete_command(edit_text):
        try:
            schedule_delete_forward_copies_for_source(chat_id, msg.message_id)
        except Exception as e:
            log_error(f"[EDIT-DEL] schedule failed: {e}")

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
            schedule_propagate_edited_to_copies(msg)
    except Exception as e:
        log_error(f"[EDIT-FWD] schedule failed: {e}")
                                            

@bot.message_handler(commands=["mega_status"])
def cmd_mega_status(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass
    schedule_command_delete(msg)
    chat_id = msg.chat.id
    if not is_owner_chat(chat_id):
        send_and_auto_delete(chat_id, "Эта команда только для владельца.", HELPER_DELETE_DELAY)
        return
    send_and_auto_delete(chat_id, mega_status_text(), 90)


@bot.message_handler(commands=["mega_backup_now"])
def cmd_mega_backup_now(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass
    schedule_command_delete(msg)
    chat_id = msg.chat.id
    if not is_owner_chat(chat_id):
        send_and_auto_delete(chat_id, "Эта команда только для владельца.", HELPER_DELETE_DELAY)
        return
    try:
        with data_lock:
            export_global_csv(data)
            save_data(data)
        ok = mega_upload_latest_global_backup()
        uploaded = 0
        failed = 0
        for cid in collect_finance_chat_ids():
            try:
                if mega_upload_chat_backup_bundle(cid, current_month_key()):
                    uploaded += 1
                else:
                    failed += 1
            except Exception as e:
                failed += 1
                log_error(f"cmd_mega_backup_now chat {cid}: {e}")
        if ok:
            send_and_auto_delete(chat_id, f"☁️ MEGA backup: ✅ global + чатов: {uploaded}, ошибок: {failed}", 60)
        else:
            send_and_auto_delete(chat_id, f"☁️ MEGA backup: ❌ global не загружен, чатов: {uploaded}, ошибок: {failed}; смотри /errors", 60)
    except Exception as e:
        log_error(f"cmd_mega_backup_now: {e}")
        send_and_auto_delete(chat_id, "☁️ MEGA backup: ❌ ошибка, смотри /errors", 60)


def build_diag_text() -> str:
    chats = data.get("chats", {}) or {}
    finance_ids = collect_finance_chat_ids()
    hidden = []
    quick_on = []
    try:
        for cid in finance_ids:
            if is_hidden_finance_mode(cid):
                hidden.append(cid)
            if is_quick_balance_enabled(cid):
                quick_on.append(cid)
    except Exception:
        pass
    fr = data.get("forward_rules", {}) or {}
    forward_pairs = sum(len(v or {}) for v in fr.values())
    active_windows_count = 0
    try:
        active_windows_count = sum(len(v or {}) for v in (data.get("active_messages", {}) or {}).values())
    except Exception:
        active_windows_count = 0
    dirty_count = 0
    try:
        with timer_lock:
            dirty_count = len(_backup_dirty_chats)
    except Exception:
        pass
    errors = get_recent_errors(5)

    lines = [
        "🧪 Диагностика бота",
        f"Версия: {VERSION}",
        f"SQLite: {DB_FILE}",
        f"Чатов в базе: {len(chats)}",
        f"Фин-чатов: {len(finance_ids)}",
        f"Скрытых фин-чатов: {len(hidden)}",
        f"Быстрый остаток включён: {len(quick_on)}",
        f"Связей пересылки: {forward_pairs}",
        f"Активных окон: {active_windows_count}",
        f"Dirty-бэкапов в очереди: {dirty_count}",
        f"BACKUP_CHAT_ID: {'есть' if BACKUP_CHAT_ID else 'нет'}",
        f"Бэкап в канал: {'ВКЛ' if backup_flags.get('channel', True) else 'ВЫКЛ'}",
        f"MEGA: {'ВКЛ' if MEGA_ENABLED else 'ВЫКЛ'} / {'настроено' if mega_is_configured() else 'не настроено'}",
        f"MEGA dir: {MEGA_BACKUP_DIR}",
        f"Ошибок в журнале: {len(get_recent_errors(80))}",
    ]
    if errors:
        lines.append("")
        lines.append("Последние ошибки:")
        for e in errors:
            lines.append(f"• {e.get('ts','')} — {format_error_for_owner(e.get('msg',''))[:160]}")
    return "\n".join(lines)


@bot.message_handler(commands=["diag", "diagnostics"])
def cmd_diag(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass
    schedule_command_delete(msg)
    chat_id = msg.chat.id
    if not is_owner_chat(chat_id):
        send_and_auto_delete(chat_id, "Эта команда только для владельца.", HELPER_DELETE_DELAY)
        return
    send_and_auto_delete(chat_id, build_diag_text(), 60)


@bot.message_handler(commands=["errors", "bot_errors"])
def cmd_errors(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass
    schedule_command_delete(msg)
    chat_id = msg.chat.id
    if not is_owner_chat(chat_id):
        send_and_auto_delete(chat_id, "Эта команда только для владельца.", HELPER_DELETE_DELAY)
        return
    errors = get_recent_errors(20)
    if not errors:
        send_and_auto_delete(chat_id, "🧯 Ошибок в журнале нет.", 30)
        return
    lines = ["🧯 Последние ошибки бота:"]
    for e in errors:
        lines.append(f"\n• {e.get('ts','')}\n{format_error_for_owner(e.get('msg',''))[:700]}")
    send_and_auto_delete(chat_id, "\n".join(lines), 90)




@bot.message_handler(commands=["journal", "log", "logs"])
def cmd_journal(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass
    schedule_command_delete(msg)
    chat_id = msg.chat.id
    bot_journal("command_journal", chat_id, getattr(msg, "text", ""))
    if not is_owner_chat(chat_id):
        send_and_auto_delete(chat_id, "Эта команда только для владельца.", HELPER_DELETE_DELAY)
        return
    send_journal_file_to_owner(chat_id, 3000)

@bot.message_handler(commands=["sqlite", "db"])
def cmd_sqlite_dump(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass

    schedule_command_delete(msg)
    chat_id = msg.chat.id
    if is_finance_output_suppressed(chat_id):
        return
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
            try:
                upd_type = "edited_message" if "edited_message" in payload else "message" if "message" in payload else "callback_query" if "callback_query" in payload else "other"
                bot_journal("webhook_update", _extract_update_chat_id(payload), upd_type)
            except Exception:
                pass

        update = telebot.types.Update.de_json(payload)
        update_chat_id = _extract_update_chat_id(payload) if isinstance(payload, dict) else None
        if update_chat_id is None:
            bot.process_new_updates([update])
        else:
            # Главная очередь: один и тот же чат всегда обрабатывается строго последовательно.
            with locked_chat(update_chat_id):
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
    try:
        restored = mega_autorestore_if_needed()
    except Exception as e:
        log_error(f"main mega_autorestore_if_needed: {e}")
        restored = False
    migrate_legacy_owner_secrets()
    for cid in list((data.get("chats", {}) or {}).keys()):
        try:
            store = get_chat_store(int(cid))
            settings = store.setdefault("settings", {})
            settings.setdefault("quick_balance_enabled", False)
            settings.setdefault("quick_balance_behavior", "normal")
            settings.setdefault("quick_balance_user_selected", False)
            settings.setdefault("hidden_finance", False)
            settings.setdefault("auto_backup_enabled", True)
            settings["auto_backup_to_mega_enabled"] = True
            settings.setdefault("total_secret_mode", False)
            store.setdefault("secret_messages", [])
            _ensure_secret_media_numbers(int(cid))
        except Exception:
            pass
    save_data(data)
    data["forward_rules"] = load_forward_rules()
    schedule_all_finance_backups(delay=20.0)
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
                    f"✅ 🦷Бот запущен (версия {VERSION}).\n"
                    f"Восстановление: {'OK' if restored else 'пропущено'}"
                )
            except Exception as e:
                log_error(f"notify owner on start: {e}")
    schedule_startup_main_windows(delay=3.0)
    app.run(host="0.0.0.0", port=PORT, threaded=True, use_reloader=False)
if __name__ == "__main__":
    main()
