# bot_v98 usdok_transactions_forward_edit
import os
import io
import json
import csv
import copy
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
import hashlib
import queue
import heapq

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
# Ограниченные очереди с сохранением порядка внутри одного чата
# ─────────────────────────────────────────────────────────────
# KeyedTaskPool выполняет задачи разных чатов параллельно, но задачи одного
# chat_id/source_chat_id всегда идут строго по порядку. Это не даёт 100 активным
# чатам создавать сотни бесконтрольных потоков.
class KeyedTaskPool:
    def __init__(self, name: str, workers: int = 4, max_pending: int = 1000):
        self.name = str(name)
        self.workers = max(1, int(workers))
        self.max_pending = max(10, int(max_pending))
        self._ready = queue.Queue()
        self._lock = threading.RLock()
        self._by_key = defaultdict(deque)
        self._active_keys = set()
        self._pending = 0
        self._active_workers = 0
        self._submitted = 0
        self._completed = 0
        self._failed = 0
        self._rejected = 0
        self._max_wait = 0.0
        self._last_error = ""
        for idx in range(self.workers):
            t = threading.Thread(target=self._worker, name=f"{self.name}-{idx+1}", daemon=True)
            t.start()

    def submit(self, key, func, *args, **kwargs) -> bool:
        key = str(key)
        with self._lock:
            if self._pending >= self.max_pending:
                self._rejected += 1
                return False
            self._by_key[key].append((func, args, kwargs, time.time()))
            self._pending += 1
            self._submitted += 1
            if key not in self._active_keys:
                self._active_keys.add(key)
                self._ready.put(key)
        return True

    def _worker(self):
        while True:
            key = self._ready.get()
            task = None
            with self._lock:
                q = self._by_key.get(key)
                if q:
                    task = q.popleft()
                    self._active_workers += 1
                else:
                    self._active_keys.discard(key)
                    self._by_key.pop(key, None)
            if task is None:
                self._ready.task_done()
                continue
            func, args, kwargs, enqueued_at = task
            wait = max(0.0, time.time() - enqueued_at)
            with self._lock:
                self._max_wait = max(self._max_wait, wait)
            try:
                func(*args, **kwargs)
                with self._lock:
                    self._completed += 1
            except Exception as exc:
                with self._lock:
                    self._failed += 1
                    self._last_error = str(exc)[:300]
                try:
                    log_error(f"POOL {self.name}: {exc}")
                except Exception:
                    logging.exception("POOL %s", self.name)
            finally:
                with self._lock:
                    self._pending = max(0, self._pending - 1)
                    self._active_workers = max(0, self._active_workers - 1)
                    q = self._by_key.get(key)
                    if q:
                        self._ready.put(key)
                    else:
                        self._by_key.pop(key, None)
                        self._active_keys.discard(key)
                self._ready.task_done()

    def stats(self) -> dict:
        with self._lock:
            return {
                "name": self.name,
                "workers": self.workers,
                "active": self._active_workers,
                "pending": self._pending,
                "keys": len(self._active_keys),
                "submitted": self._submitted,
                "completed": self._completed,
                "failed": self._failed,
                "rejected": self._rejected,
                "max_wait": round(self._max_wait, 3),
                "last_error": self._last_error,
            }


class DelayedTaskScheduler:
    """Один поток хранит все логические таймеры без сотен threading.Timer."""
    def __init__(self, executor_pool: KeyedTaskPool):
        self.executor_pool = executor_pool
        self._cv = threading.Condition(threading.RLock())
        self._heap = []
        self._versions = {}
        self._deadlines = {}
        self._seq = 0
        self._submitted = 0
        self._executed = 0
        self._cancelled = 0
        self._failed_dispatch = 0
        threading.Thread(target=self._worker, name="delayed-scheduler", daemon=True).start()

    def schedule(self, key, delay: float, func, *args, **kwargs):
        key = str(key)
        run_at = time.time() + max(0.0, float(delay or 0))
        with self._cv:
            self._seq += 1
            version = int(self._versions.get(key, 0)) + 1
            self._versions[key] = version
            self._deadlines[key] = run_at
            heapq.heappush(self._heap, (run_at, self._seq, key, version, func, args, kwargs))
            self._submitted += 1
            self._cv.notify_all()
        return run_at

    def cancel(self, key):
        key = str(key)
        with self._cv:
            self._versions[key] = int(self._versions.get(key, 0)) + 1
            if key in self._deadlines:
                self._deadlines.pop(key, None)
                self._cancelled += 1
            self._cv.notify_all()

    def deadline(self, key):
        with self._cv:
            return self._deadlines.get(str(key))

    def stats(self):
        with self._cv:
            return {
                "scheduled": len(self._deadlines),
                "heap": len(self._heap),
                "submitted": self._submitted,
                "executed": self._executed,
                "cancelled": self._cancelled,
                "dispatch_failed": self._failed_dispatch,
            }

    def _worker(self):
        while True:
            with self._cv:
                while not self._heap:
                    self._cv.wait()
                run_at, seq, key, version, func, args, kwargs = self._heap[0]
                wait = run_at - time.time()
                if wait > 0:
                    self._cv.wait(timeout=wait)
                    continue
                heapq.heappop(self._heap)
                if int(self._versions.get(key, 0)) != int(version):
                    continue
                self._deadlines.pop(key, None)
            dispatch_key = f"delay:{key}:{seq}"
            ok = self.executor_pool.submit(dispatch_key, self._execute, func, args, kwargs)
            if not ok:
                # Не теряем таймер при кратком всплеске: возвращаем его в heap и пробуем позже.
                with self._cv:
                    self._failed_dispatch += 1
                    if int(self._versions.get(key, 0)) == int(version):
                        retry_at = time.time() + 0.5
                        self._seq += 1
                        self._deadlines[key] = retry_at
                        heapq.heappush(self._heap, (retry_at, self._seq, key, version, func, args, kwargs))
                        self._cv.notify_all()
                try:
                    log_error(f"DELAYED QUEUE FULL, RETRY: {key}")
                except Exception:
                    pass

    def _execute(self, func, args, kwargs):
        try:
            func(*args, **kwargs)
        finally:
            with self._cv:
                self._executed += 1


def _env_int(name: str, default: int, minimum: int = 1, maximum: int = 128) -> int:
    try:
        return max(minimum, min(maximum, int(os.getenv(name, str(default)) or default)))
    except Exception:
        return int(default)


WEBHOOK_TASK_POOL = KeyedTaskPool(
    "webhook",
    _env_int("WEBHOOK_WORKERS", 6, 2, 16),
    _env_int("WEBHOOK_MAX_PENDING", 2000, 100, 10000),
)
FINANCE_TASK_POOL = KeyedTaskPool(
    "finance",
    _env_int("FINANCE_WORKERS", 4, 2, 12),
    _env_int("FINANCE_MAX_PENDING", 1000, 100, 5000),
)
FORWARD_TASK_POOL = KeyedTaskPool(
    "forward",
    _env_int("FORWARD_WORKERS", 4, 2, 12),
    _env_int("FORWARD_MAX_PENDING", 1500, 100, 5000),
)
BACKUP_TASK_POOL = KeyedTaskPool(
    "backup",
    _env_int("BACKUP_WORKERS", 1, 1, 2),
    _env_int("BACKUP_MAX_PENDING", 300, 50, 1500),
)
# v90: маленькие аварийные delta не ждут Excel/канал/полный файл чата в backup queue.
DELTA_TASK_POOL = KeyedTaskPool(
    "delta",
    _env_int("DELTA_WORKERS", 1, 1, 2),
    _env_int("DELTA_MAX_PENDING", 500, 50, 2000),
)
EXPORT_TASK_POOL = KeyedTaskPool(
    "export",
    _env_int("EXPORT_WORKERS", 1, 1, 2),
    _env_int("EXPORT_MAX_PENDING", 300, 20, 2000),
)
GENERAL_TASK_POOL = KeyedTaskPool(
    "general",
    _env_int("GENERAL_WORKERS", 2, 1, 6),
    _env_int("GENERAL_MAX_PENDING", 500, 50, 2000),
)
JOURNAL_TASK_POOL = KeyedTaskPool(
    "journal",
    _env_int("JOURNAL_WORKERS", 1, 1, 2),
    _env_int("JOURNAL_MAX_PENDING", 3000, 500, 10000),
)
DELAYED_TASK_POOL = KeyedTaskPool(
    "delayed",
    _env_int("DELAYED_WORKERS", 2, 1, 6),
    _env_int("DELAYED_MAX_PENDING", 1000, 100, 5000),
)
DOZVON_TASK_POOL = KeyedTaskPool(
    "dozvon",
    _env_int("DOZVON_WORKERS", 1, 1, 2),
    _env_int("DOZVON_MAX_PENDING", 100, 10, 500),
)
DELAYED_SCHEDULER = DelayedTaskScheduler(DELAYED_TASK_POOL)

chat_locks = defaultdict(threading.RLock)
data_lock = threading.RLock()
forward_map_lock = threading.RLock()
timer_lock = threading.RLock()
_state_context = threading.local()


def chat_lock_for(chat_id: int):
    return chat_locks[int(chat_id)]


@contextmanager
def locked_chat(chat_id: int):
    with chat_lock_for(int(chat_id)):
        yield


@contextmanager
def state_chat_context(chat_id):
    prev = getattr(_state_context, "chat_id", None)
    try:
        _state_context.chat_id = int(chat_id) if chat_id is not None else None
        yield
    finally:
        _state_context.chat_id = prev


def current_state_chat_id():
    return getattr(_state_context, "chat_id", None)


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
    """Пересылка: порядок сохраняется по исходному чату, разные чаты идут параллельно."""
    if not FORWARD_TASK_POOL.submit(int(source_chat_id), forward_any_message, source_chat_id, msg):
        # При редком переполнении не теряем пересылку: выполняем в текущем ограниченном webhook-worker.
        log_error(f"FORWARD QUEUE FULL, INLINE FALLBACK: {source_chat_id}")
        forward_any_message(source_chat_id, msg)


def schedule_propagate_edited_to_copies(msg):
    source_chat_id = int(getattr(getattr(msg, "chat", None), "id", 0) or 0)
    if not FORWARD_TASK_POOL.submit(source_chat_id, propagate_edited_to_copies, msg):
        log_error(f"FORWARD EDIT QUEUE FULL, INLINE FALLBACK: {source_chat_id}")
        propagate_edited_to_copies(msg)


def schedule_delete_forward_copies_for_source(source_chat_id: int, source_msg_id: int):
    if not FORWARD_TASK_POOL.submit(int(source_chat_id), delete_forward_copies_for_source, source_chat_id, source_msg_id):
        log_error(f"FORWARD DELETE QUEUE FULL, INLINE FALLBACK: {source_chat_id}")
        delete_forward_copies_for_source(source_chat_id, source_msg_id)
BOT_TOKEN = os.getenv("B_T", "").strip()
OWNER_ID = os.getenv("ID", "").strip()
RENDER_EXTERNAL_HOSTNAME = os.getenv("RENDER_EXTERNAL_HOSTNAME", "").strip()
_RENDER_HOST_URL = f"https://{RENDER_EXTERNAL_HOSTNAME}" if RENDER_EXTERNAL_HOSTNAME else ""
APP_URL = os.getenv("APP_URL", "").strip() or os.getenv("RENDER_EXTERNAL_URL", "").strip() or _RENDER_HOST_URL
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "").strip() or APP_URL
try:
    PORT = int(os.getenv("PORT", "5000"))
except Exception:
    PORT = 5000
BACKUP_CHAT_ID = os.getenv("BACKUP_CHAT_ID", "").strip()
if not BOT_TOKEN:
    raise RuntimeError("B_T is not set")
VERSION = "bot_v97_usd_transactions_forward_edit"


def version_animal_badge(version: str | None = None) -> str:
    """Для каждой новой версии — свой зверь и номер."""
    raw = str(version or VERSION)
    m = re.search(r"(?:^|_)v(\d+)", raw, re.I)
    number = int(m.group(1)) if m else 0
    animals = ["🐺", "🦊", "🐯", "🐲", "🦅", "🐘", "🦉", "🐆", "🦈", "🦄", "🐻", "🦁", "🐼", "🐸", "🐙", "🦚", "🐬", "🦬", "🦏", "🐊"]
    animal = animals[(number - 81) % len(animals)] if number else "🤖"
    return f"{animal}{number}" if number else animal
DEFAULT_TZ = "America/Argentina/Buenos_Aires"
try:
    KEEP_ALIVE_INTERVAL_SECONDS = max(20, int(os.getenv("KEEP_ALIVE_INTERVAL_SECONDS", "45") or "45"))
except Exception:
    KEEP_ALIVE_INTERVAL_SECONDS = 45
KEEP_ALIVE_ENABLED = str(os.getenv("KEEP_ALIVE_ENABLED", "1")).strip().lower() in {"1", "true", "yes", "y", "on", "да"}
try:
    KEEP_ALIVE_TELEGRAM_EVERY = max(1, int(os.getenv("KEEP_ALIVE_TELEGRAM_EVERY", "4") or "4"))
except Exception:
    KEEP_ALIVE_TELEGRAM_EVERY = 4
DB_FILE = os.getenv("DB_FILE", "bot_state.sqlite3").strip() or "bot_state.sqlite3"
DATA_FILE = "data.json"
CSV_FILE = "data.csv"
CSV_META_FILE = "csv_meta.json"

# Стабильный логический формат полного бэкапа между версиями бота.
UNIVERSAL_BACKUP_KIND = "telegram_finance_bot_universal"
UNIVERSAL_BACKUP_SCHEMA_VERSION = 10

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
MEGA_HISTORY_BACKUP_DIR = os.getenv("MEGA_HISTORY_BACKUP_DIR", "history").strip().strip("/") or "history"
# v90: небольшие неизменяемые delta-файлы вместо полного global после каждой операции.
MEGA_DELTA_BACKUP_DIR = os.getenv("MEGA_DELTA_BACKUP_DIR", "deltas").strip().strip("/") or "deltas"
try:
    MEGA_DELTA_DELAY_SECONDS = max(1.0, float(os.getenv("MEGA_DELTA_DELAY_SECONDS", "8") or "8"))
except Exception:
    MEGA_DELTA_DELAY_SECONDS = 8.0
try:
    MEGA_DELTA_PRIORITY_DELAY_SECONDS = max(0.5, float(os.getenv("MEGA_DELTA_PRIORITY_DELAY_SECONDS", "1") or "1"))
except Exception:
    MEGA_DELTA_PRIORITY_DELAY_SECONDS = 1.0
try:
    MEGA_GLOBAL_QUIET_SECONDS = max(60.0, float(os.getenv("MEGA_GLOBAL_QUIET_SECONDS", "180") or "180"))
except Exception:
    MEGA_GLOBAL_QUIET_SECONDS = 180.0
try:
    MEGA_GLOBAL_MAX_INTERVAL_SECONDS = max(300.0, float(os.getenv("MEGA_GLOBAL_MAX_INTERVAL_SECONDS", "900") or "900"))
except Exception:
    MEGA_GLOBAL_MAX_INTERVAL_SECONDS = 900.0
try:
    MEGA_GLOBAL_HISTORY_KEEP = min(2, max(1, int(os.getenv("MEGA_GLOBAL_HISTORY_KEEP", "2") or "2")))
except Exception:
    MEGA_GLOBAL_HISTORY_KEEP = 2
try:
    MEGA_FILE_HISTORY_KEEP = min(2, max(1, int(os.getenv("MEGA_FILE_HISTORY_KEEP", "2") or "2")))
except Exception:
    MEGA_FILE_HISTORY_KEEP = 2
try:
    MEGA_DELTA_KEEP_FILES = max(50, int(os.getenv("MEGA_DELTA_KEEP_FILES", "500") or "500"))
except Exception:
    MEGA_DELTA_KEEP_FILES = 500
try:
    MEGA_DELTA_RESTORE_LIMIT = max(50, int(os.getenv("MEGA_DELTA_RESTORE_LIMIT", "1000") or "1000"))
except Exception:
    MEGA_DELTA_RESTORE_LIMIT = 1000
try:
    MEGA_GLOBAL_MIN_SAFE_BYTES = max(2048, int(os.getenv("MEGA_GLOBAL_MIN_SAFE_BYTES", "8192") or "8192"))
except Exception:
    MEGA_GLOBAL_MIN_SAFE_BYTES = 8192
try:
    MEGA_GLOBAL_MAX_RECORD_DROP = min(0.95, max(0.05, float(os.getenv("MEGA_GLOBAL_MAX_RECORD_DROP", "0.30") or "0.30")))
except Exception:
    MEGA_GLOBAL_MAX_RECORD_DROP = 0.30
ALLOW_EMPTY_MEGA_RESTORE = _env_bool("ALLOW_EMPTY_MEGA_RESTORE", "0")
RESTORE_GUARD_ACTIVE = False
RESTORE_GUARD_REASON = ""
MEGA_GLOBAL_BACKUP_LOCK = threading.RLock()
MEGA_COMMAND_LOCK = threading.RLock()
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

    def save_chat(self, chat_id, payload: dict):
        """Точечно сохраняет только один изменившийся чат."""
        with self.lock:
            self.conn.execute(
                "INSERT INTO chats(chat_id,v) VALUES(?,?) ON CONFLICT(chat_id) DO UPDATE SET v=excluded.v",
                (str(chat_id), self._dump(payload or {})),
            )
            self.conn.commit()

    def delete_chat(self, chat_id):
        with self.lock:
            self.conn.execute("DELETE FROM chats WHERE chat_id=?", (str(chat_id),))
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
BOT_JOURNAL_MAX = int(os.getenv("BOT_JOURNAL_MAX", "1200") or "1200")
BOT_JOURNAL_FILE = os.getenv("BOT_JOURNAL_FILE", "bot_journal.jsonl").strip() or "bot_journal.jsonl"
BOT_ACTION_LOG = deque(maxlen=BOT_JOURNAL_MAX)
bot_journal_lock = threading.RLock()


def _journal_ts() -> str:
    try:
        return now_local().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    except Exception:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def is_journal_registration_enabled() -> bool:
    """Глобальный журнал. В v83 по умолчанию выключен."""
    try:
        d = globals().get("data")
        if isinstance(d, dict):
            gs = d.setdefault("_global_settings", {})
            return bool(gs.get("bot_journal_enabled", False))
    except Exception:
        pass
    return False


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
    return ("✅ Общий журнал ВКЛ" if is_journal_registration_enabled() else "❌ Общий журнал ВЫКЛ")


def is_chat_journal_enabled(chat_id: int) -> bool:
    try:
        store = get_chat_store(int(chat_id))
        return bool(store.setdefault("settings", {}).get("journal_enabled", False))
    except Exception:
        return False


def set_chat_journal_enabled(chat_id: int, enabled: bool):
    store = get_chat_store(int(chat_id))
    store.setdefault("settings", {})["journal_enabled"] = bool(enabled)
    save_data(data, chat_ids=[int(chat_id)])
    schedule_config_backup_for_chats(int(chat_id))


def toggle_chat_journal(chat_id: int) -> bool:
    new_value = not is_chat_journal_enabled(int(chat_id))
    set_chat_journal_enabled(int(chat_id), new_value)
    return new_value


def chat_journal_toggle_label(chat_id: int, short: bool = False) -> str:
    enabled = is_chat_journal_enabled(int(chat_id))
    if short:
        return ("✅ 📓" if enabled else "❌ 📓")
    return ("✅ Журнал чата ВКЛ" if enabled else "❌ Журнал чата ВЫКЛ")


def journal_should_record(chat_id=None) -> bool:
    if is_journal_registration_enabled():
        return True
    if chat_id is None:
        return False
    return is_chat_journal_enabled(int(chat_id))


BOT_BEHAVIOR_PROFILES = {
    "v97_current": {
        "title": "v97 Все правки чата / USD v93 сохранён",
        "ui_edit_interval": 0.03,
        "fast_tg_gap": 0.01,
        "info_layout": "v87",
        "per_chat_journal": True,
        "mega_priority": True,
        "keepalive_menu": True,
        "article_buttons": False,
        "financial_value_buttons": True,
        "financial_buttons_per_row": 1,
        "gomonk_wallets": True,
        "remaining_window": True,
        "usd_categories": True,
        "daily_usd": True,
        "forward_copy_edit": True,
        "usd_transactions": True,
        "description": "v93 USD-транзакции + все исправления из текущего чата до отдельной команды восстановления USD-кнопки.",
    },
    "v93_current": {
        "title": "v93 USD / 💰Перес редактирование",
        "ui_edit_interval": 0.03,
        "fast_tg_gap": 0.01,
        "info_layout": "v87",
        "per_chat_journal": True,
        "mega_priority": True,
        "keepalive_menu": True,
        "article_buttons": False,
        "financial_value_buttons": True,
        "financial_buttons_per_row": 1,
        "gomonk_wallets": True,
        "remaining_window": True,
        "usd_categories": True,
        "daily_usd": True,
        "forward_copy_edit": True,
        "usd_transactions": True,
        "description": "v92 + безопасный отдельный учёт USD-транзакций и улучшенное окно редактирования бот-копии.",
    },
    "v92_current": {
        "title": "v92 💰Перес / редактирование копий",
        "ui_edit_interval": 0.03,
        "fast_tg_gap": 0.01,
        "info_layout": "v87",
        "per_chat_journal": True,
        "mega_priority": True,
        "keepalive_menu": True,
        "article_buttons": False,
        "financial_value_buttons": True,
        "financial_buttons_per_row": 1,
        "gomonk_wallets": True,
        "remaining_window": True,
        "usd_categories": True,
        "daily_usd": True,
        "forward_copy_edit": True,
        "usd_transactions": True,
        "description": "v91 + режим 💰Перес: обычно / кнопка / слеш для редактирования бот-копии и связанной финансовой записи.",
    },
    "v91_current": {
        "title": "v91 Статьи / Excel стат",
        "ui_edit_interval": 0.03,
        "fast_tg_gap": 0.01,
        "info_layout": "v87",
        "per_chat_journal": True,
        "mega_priority": True,
        "keepalive_menu": True,
        "article_buttons": False,
        "financial_value_buttons": True,
        "financial_buttons_per_row": 1,
        "gomonk_wallets": True,
        "remaining_window": True,
        "usd_categories": True,
        "daily_usd": True,
        "description": "Текущая версия: v90 delta/snapshots + порядок статей, сортировка ПРОЧЕЕ, Excel стат и компактная история MEGA.",
    },
    "v90_current": {
        "title": "v90 Delta / snapshots",
        "ui_edit_interval": 0.03,
        "fast_tg_gap": 0.01,
        "info_layout": "v87",
        "per_chat_journal": True,
        "mega_priority": True,
        "keepalive_menu": True,
        "article_buttons": False,
        "financial_value_buttons": True,
        "financial_buttons_per_row": 1,
        "gomonk_wallets": True,
        "remaining_window": True,
        "usd_categories": True,
        "daily_usd": True,
        "description": "Текущая версия: быстрые immutable delta, редкие full snapshots, безопасные файлы чатов и восстановление global + delta.",
    },
    "v88_current": {
        "title": "v88 Чистые статьи / полная валюта",
        "ui_edit_interval": 0.03,
        "fast_tg_gap": 0.01,
        "info_layout": "v87",
        "per_chat_journal": True,
        "mega_priority": True,
        "keepalive_menu": True,
        "article_buttons": False,
        "financial_value_buttons": True,
        "financial_buttons_per_row": 1,
        "gomonk_wallets": True,
        "remaining_window": True,
        "usd_categories": True,
        "daily_usd": True,
        "description": "Текущая версия: статьи без @имени бота и полноценные ARS / ARS-USD / USD во всех окнах статей.",
    },
    "v87_current": {
        "title": "v87 Валюты / быстрый возврат",
        "ui_edit_interval": 0.03,
        "fast_tg_gap": 0.01,
        "info_layout": "v87",
        "per_chat_journal": True,
        "mega_priority": True,
        "keepalive_menu": True,
        "article_buttons": False,
        "financial_value_buttons": True,
        "financial_buttons_per_row": 1,
        "gomonk_wallets": True,
        "remaining_window": True,
        "usd_categories": True,
        "daily_usd": True,
        "description": "Текущая версия: ARS / ARS-USD / USD, быстрый возврат в основное окно и навигация Ф91.",
    },
    "v86_current": {
        "title": "v86 Левые фин-кнопки / USD",
        "ui_edit_interval": 0.05,
        "fast_tg_gap": 0.015,
        "info_layout": "v86",
        "per_chat_journal": True,
        "mega_priority": True,
        "keepalive_menu": True,
        "article_buttons": False,
        "financial_value_buttons": True,
        "financial_buttons_per_row": 1,
        "gomonk_wallets": True,
        "remaining_window": True,
        "usd_categories": True,
        "daily_usd": True,
        "description": "Текущая версия: фин-кнопки по одной строке со сдвигом влево, гомонки и USD в окне дня.",
    },
    "v85_current": {
        "title": "v85 Гомонки / USD",
        "ui_edit_interval": 0.05,
        "fast_tg_gap": 0.015,
        "info_layout": "v85",
        "per_chat_journal": True,
        "mega_priority": True,
        "keepalive_menu": True,
        "article_buttons": False,
        "financial_value_buttons": True,
        "financial_buttons_per_row": 1,
        "gomonk_wallets": True,
        "remaining_window": True,
        "usd_categories": True,
        "daily_usd": False,
        "description": "Прежняя v85: быстрые кнопки, финансы по одной в ряд, гомонки, остатки после расходов и USD.",
    },
    "v84_current": {
        "title": "v84 Фин-кнопки",
        "ui_edit_interval": 0.20,
        "fast_tg_gap": 0.05,
        "info_layout": "v84",
        "per_chat_journal": True,
        "mega_priority": True,
        "keepalive_menu": True,
        "article_buttons": False,
        "financial_value_buttons": True,
        "financial_buttons_per_row": 2,
        "gomonk_wallets": False,
        "remaining_window": False,
        "usd_categories": False,
        "daily_usd": False,
        "description": "Прежняя v84: финансовые записи-кнопки по две в ряд.",
    },
    "v83_flexible": {
        "title": "v83 Гибкая",
        "ui_edit_interval": 0.20,
        "fast_tg_gap": 0.05,
        "info_layout": "v83",
        "per_chat_journal": True,
        "mega_priority": True,
        "keepalive_menu": True,
        "article_buttons": True,
        "financial_value_buttons": False,
        "financial_buttons_per_row": 0,
        "gomonk_wallets": False,
        "remaining_window": False,
        "usd_categories": False,
        "daily_usd": False,
        "description": "Поведение прежней v83: индивидуальные журналы, keep-alive и исторический режим статей-кнопок.",
    },
    "v82_stable": {
        "title": "v82 Стабильная",
        "ui_edit_interval": 0.35,
        "fast_tg_gap": 0.08,
        "info_layout": "v82",
        "per_chat_journal": False,
        "mega_priority": True,
        "keepalive_menu": False,
        "article_buttons": False,
        "financial_value_buttons": False,
        "financial_buttons_per_row": 0,
        "gomonk_wallets": False,
        "remaining_window": False,
        "usd_categories": False,
        "daily_usd": False,
        "description": "Интерфейс и набор кнопок v82: универсальный MEGA-бэкап без функций v83/v84.",
    },
    "v81_compatible": {
        "title": "v81 Совместимость",
        "ui_edit_interval": 1.15,
        "fast_tg_gap": 0.20,
        "info_layout": "v81",
        "per_chat_journal": False,
        "mega_priority": False,
        "keepalive_menu": False,
        "article_buttons": False,
        "financial_value_buttons": False,
        "financial_buttons_per_row": 0,
        "gomonk_wallets": False,
        "remaining_window": False,
        "usd_categories": False,
        "daily_usd": False,
        "description": "Интерфейс и осторожное поведение v81 без новых кнопок; выбор версии остаётся доступен.",
    },
}
DEFAULT_BOT_BEHAVIOR_PROFILE = "v97_current"


def active_bot_behavior_profile() -> str:
    try:
        key = str((data or {}).setdefault("_global_settings", {}).get("bot_behavior_profile") or DEFAULT_BOT_BEHAVIOR_PROFILE)
    except Exception:
        key = DEFAULT_BOT_BEHAVIOR_PROFILE
    return key if key in BOT_BEHAVIOR_PROFILES else DEFAULT_BOT_BEHAVIOR_PROFILE


def active_bot_behavior_profile_info() -> dict:
    return BOT_BEHAVIOR_PROFILES.get(active_bot_behavior_profile(), BOT_BEHAVIOR_PROFILES[DEFAULT_BOT_BEHAVIOR_PROFILE])


def _version_mode_snapshot_fields() -> tuple[tuple[str, ...], tuple[str, ...]]:
    # Только настройки интерфейса/поведения. Финансовые записи, остатки, пересылки,
    # владельцы и backup-данные при выборе версии никогда не откатываются.
    global_fields = (
        "buttons_current_window", "forward_menu_new_style", "icon_button_mode",
        "total_secret_mask_enabled", "finance_day_start_5am", "mega_backup_priority",
    )
    chat_fields = (
        "buttons_current_window", "journal_enabled", "main_article_buttons_enabled",
        "main_financial_value_buttons_enabled", "gomonk_enabled", "gomonk_entries",
        "remaining_with_gomonk", "usd_display_enabled", "currency_mode", "remaining_show_ost_label", "quick_balance_enabled",
        "category_usd_enabled", "expense_category_order_slugs",
        "quick_balance_behavior", "quick_balance_user_selected", "hidden_finance",
        "process_trace_enabled", "forward_copy_edit_mode", "usd_transactions_view",
    )
    return global_fields, chat_fields


def save_version_mode_snapshot(profile_key: str | None = None):
    try:
        key = str(profile_key or active_bot_behavior_profile())
        if key not in BOT_BEHAVIOR_PROFILES:
            return
        gs = data.setdefault("_global_settings", {})
        snapshots = gs.setdefault("version_mode_snapshots", {})
        global_fields, chat_fields = _version_mode_snapshot_fields()
        snap = {
            "global": {name: gs.get(name) for name in global_fields if name in gs},
            "chats": {},
            "saved_at": now_local().isoformat(timespec="seconds"),
        }
        for cid, store in (data.get("chats", {}) or {}).items():
            if not isinstance(store, dict):
                continue
            settings = store.setdefault("settings", {})
            snap["chats"][str(cid)] = {name: settings.get(name) for name in chat_fields if name in settings}
        snapshots[key] = snap
    except Exception as e:
        log_error(f"save_version_mode_snapshot: {e}")


def restore_version_mode_snapshot(profile_key: str):
    try:
        gs = data.setdefault("_global_settings", {})
        snap = (gs.setdefault("version_mode_snapshots", {}) or {}).get(str(profile_key)) or {}
        global_fields, chat_fields = _version_mode_snapshot_fields()
        global_values = snap.get("global") if isinstance(snap, dict) else {}
        if isinstance(global_values, dict):
            for name in global_fields:
                if name in global_values:
                    gs[name] = global_values[name]
        chat_values = snap.get("chats") if isinstance(snap, dict) else {}
        if isinstance(chat_values, dict):
            for cid, values in chat_values.items():
                if not isinstance(values, dict):
                    continue
                store = get_chat_store(int(cid))
                settings = store.setdefault("settings", {})
                for name in chat_fields:
                    if name in values:
                        settings[name] = values[name]
    except Exception as e:
        log_error(f"restore_version_mode_snapshot({profile_key}): {e}")


def version_mode_feature(name: str) -> bool:
    try:
        return bool(active_bot_behavior_profile_info().get(str(name), False))
    except Exception:
        return False


def version_mode_layout() -> str:
    try:
        return str(active_bot_behavior_profile_info().get("info_layout") or "v87")
    except Exception:
        return "v87"


def set_bot_behavior_profile(profile_key: str) -> str:
    profile_key = str(profile_key or "").strip()
    if profile_key not in BOT_BEHAVIOR_PROFILES:
        profile_key = DEFAULT_BOT_BEHAVIOR_PROFILE
    previous = active_bot_behavior_profile()
    if previous != profile_key:
        save_version_mode_snapshot(previous)
    data.setdefault("_global_settings", {})["bot_behavior_profile"] = profile_key
    if previous != profile_key:
        restore_version_mode_snapshot(profile_key)
    save_data(data, full=True)
    try:
        with _ui_edit_lock:
            _ui_edit_last_ts.clear()
            _ui_edit_pending.clear()
    except Exception:
        pass
    try:
        schedule_config_backup_for_chats(delay=1.0)
    except Exception:
        pass
    return profile_key


def bot_behavior_profile_label() -> str:
    return "🧩 " + str(active_bot_behavior_profile_info().get("title") or active_bot_behavior_profile())


def effective_ui_edit_interval() -> float:
    raw = os.getenv("UI_EDIT_MIN_INTERVAL_SECONDS")
    if raw not in (None, ""):
        try:
            return max(0.05, float(raw))
        except Exception:
            pass
    return float(active_bot_behavior_profile_info().get("ui_edit_interval", 0.20))


def effective_fast_telegram_gap() -> float:
    return float(active_bot_behavior_profile_info().get("fast_tg_gap", 0.05))


def main_article_buttons_enabled(chat_id: int) -> bool:
    try:
        return bool(get_chat_store(int(chat_id)).setdefault("settings", {}).get("main_article_buttons_enabled", False))
    except Exception:
        return False


def set_main_article_buttons_enabled(chat_id: int, enabled: bool):
    store = get_chat_store(int(chat_id))
    store.setdefault("settings", {})["main_article_buttons_enabled"] = bool(enabled)
    save_data(data, chat_ids=[int(chat_id)])
    schedule_config_backup_for_chats(int(chat_id))


def toggle_main_article_buttons(chat_id: int) -> bool:
    new_value = not main_article_buttons_enabled(int(chat_id))
    set_main_article_buttons_enabled(int(chat_id), new_value)
    return new_value


def main_article_buttons_label(chat_id: int) -> str:
    return "✅ Статьи-кнопки ВКЛ" if main_article_buttons_enabled(int(chat_id)) else "❌ Статьи-кнопки ВЫКЛ"


def main_financial_value_buttons_enabled(chat_id: int) -> bool:
    try:
        return bool(get_chat_store(int(chat_id)).setdefault("settings", {}).get("main_financial_value_buttons_enabled", False))
    except Exception:
        return False


def effective_main_article_buttons_enabled(chat_id: int) -> bool:
    return bool(version_mode_feature("article_buttons") and main_article_buttons_enabled(int(chat_id)))


def effective_main_financial_value_buttons_enabled(chat_id: int) -> bool:
    return bool(version_mode_feature("financial_value_buttons") and main_financial_value_buttons_enabled(int(chat_id)))


def set_main_financial_value_buttons_enabled(chat_id: int, enabled: bool):
    store = get_chat_store(int(chat_id))
    store.setdefault("settings", {})["main_financial_value_buttons_enabled"] = bool(enabled)
    save_data(data, chat_ids=[int(chat_id)])
    schedule_config_backup_for_chats(int(chat_id))


def toggle_main_financial_value_buttons(chat_id: int) -> bool:
    new_value = not main_financial_value_buttons_enabled(int(chat_id))
    set_main_financial_value_buttons_enabled(int(chat_id), new_value)
    return new_value


def main_financial_value_buttons_label(chat_id: int) -> str:
    return "✅ Финансы-кнопки ВКЛ" if main_financial_value_buttons_enabled(int(chat_id)) else "❌ Финансы-кнопки ВЫКЛ"


FIN_BUTTON_RIGHT_PAD = max(0, min(18, int(os.getenv("FIN_BUTTON_RIGHT_PAD", "10") or "10")))
FIN_BUTTON_PAD_CHAR = "⠀"  # U+2800: Telegram сохраняет символ, визуально сдвигая подпись влево.


def financial_record_button_label(rec: dict, chat_id: int | None = None) -> str:
    try:
        amount = float((rec or {}).get("amount", 0) or 0)
    except Exception:
        amount = 0.0
    sid = str((rec or {}).get("short_id") or f"R{(rec or {}).get('id', '')}")
    note = re.sub(r"\s+", " ", str((rec or {}).get("note") or "").strip())
    if len(note) > 31:
        note = note[:30] + "…"
    if chat_id is not None and version_mode_feature("daily_usd"):
        amount_text = format_chat_amount(int(chat_id), amount, mixed_space=False)
    else:
        amount_text = fmt_num(amount)
    label = f"{sid} {amount_text}"
    if note:
        label += f" {note}"
    if active_bot_behavior_profile() in {"v92_current", "v91_current", "v90_current", "v88_current", "v87_current", "v86_current"} and FIN_BUTTON_RIGHT_PAD:
        label += FIN_BUTTON_PAD_CHAR * FIN_BUTTON_RIGHT_PAD
    return label

def financial_value_records_for_day(chat_id: int, day_key: str) -> list[dict]:
    try:
        recs = get_chat_store(int(chat_id)).get("daily_records", {}).get(str(day_key), []) or []
        return sorted((r for r in recs if isinstance(r, dict) and not bool(r.get("usd_only", False))), key=record_sort_key)
    except Exception:
        return []


def _owner_setting_value(key: str, default=False, chat_id: int | None = None):
    """Настройка owner scope; для старых данных сохраняет fallback на глобальное значение."""
    try:
        cid = int(chat_id) if chat_id is not None else current_state_chat_id()
        if cid is not None:
            scoped = owner_scoped_settings(cid)
            if key in scoped:
                return scoped.get(key)
        return (data or {}).setdefault("_global_settings", {}).get(key, default)
    except Exception:
        return default


def _set_owner_setting_value(key: str, value, chat_id: int | None = None):
    cid = int(chat_id) if chat_id is not None else current_state_chat_id()
    if cid is not None:
        owner_scoped_settings(cid)[key] = value
        save_data(data, chat_ids=[cid])
        schedule_config_backup_for_chats(cid, delay=0.3)
    else:
        data.setdefault("_global_settings", {})[key] = value
        save_data(data)


def buttons_current_window_enabled(chat_id: int | None = None) -> bool:
    return bool(_owner_setting_value("buttons_current_window", False, chat_id))


def chat_buttons_current_window_enabled(chat_id: int) -> bool:
    try:
        store = get_chat_store(int(chat_id))
        local = bool(store.setdefault("settings", {}).get("buttons_current_window", False))
        return local or buttons_current_window_enabled(chat_id)
    except Exception:
        return False


def toggle_chat_buttons_current_window(chat_id: int) -> bool:
    store = get_chat_store(int(chat_id))
    settings = store.setdefault("settings", {})
    new_value = not bool(settings.get("buttons_current_window", False))
    settings["buttons_current_window"] = new_value
    save_data(data, chat_ids=[int(chat_id)])
    return new_value


def set_buttons_current_window_enabled(enabled: bool, chat_id: int | None = None):
    try:
        _set_owner_setting_value("buttons_current_window", bool(enabled), chat_id)
    except Exception as e:
        log_error(f"set_buttons_current_window_enabled: {e}")


def toggle_buttons_current_window(chat_id: int | None = None) -> bool:
    new_value = not buttons_current_window_enabled(chat_id)
    set_buttons_current_window_enabled(new_value, chat_id)
    return new_value


def buttons_current_window_label(chat_id: int | None = None) -> str:
    return "✅ В текущем окне" if buttons_current_window_enabled(chat_id) else "❌ В текущем окне"


def forward_menu_new_style_enabled(chat_id: int | None = None) -> bool:
    return bool(_owner_setting_value("forward_menu_new_style", False, chat_id))


def set_forward_menu_new_style_enabled(enabled: bool, chat_id: int | None = None):
    try:
        _set_owner_setting_value("forward_menu_new_style", bool(enabled), chat_id)
    except Exception as e:
        log_error(f"set_forward_menu_new_style_enabled: {e}")


def toggle_forward_menu_new_style(chat_id: int | None = None) -> bool:
    new_value = not forward_menu_new_style_enabled(chat_id)
    set_forward_menu_new_style_enabled(new_value, chat_id)
    return new_value


def forward_menu_style_label(chat_id: int | None = None) -> str:
    return "🧩 Пересылка: по-новому" if forward_menu_new_style_enabled(chat_id) else "🔁 Пересылка: обычно"


def icon_button_mode_enabled(chat_id: int | None = None) -> bool:
    return bool(_owner_setting_value("icon_button_mode", True, chat_id))


def set_icon_button_mode_enabled(enabled: bool, chat_id: int | None = None):
    try:
        _set_owner_setting_value("icon_button_mode", bool(enabled), chat_id)
    except Exception as e:
        log_error(f"set_icon_button_mode_enabled: {e}")


def toggle_icon_button_mode(chat_id: int | None = None) -> bool:
    new_value = not icon_button_mode_enabled(chat_id)
    set_icon_button_mode_enabled(new_value, chat_id)
    return new_value


def icon_button_mode_label(chat_id: int | None = None) -> str:
    return "🔣 Кнопки: значки" if icon_button_mode_enabled(chat_id) else "🔤 Кнопки: текст"

def total_secret_mask_enabled(chat_id: int | None = None) -> bool:
    try:
        if chat_id is not None:
            scoped = owner_scoped_settings(int(chat_id))
            if "total_secret_mask_enabled" in scoped:
                return bool(scoped.get("total_secret_mask_enabled"))
        gs = (data or {}).setdefault("_global_settings", {})
        return bool(gs.get("total_secret_mask_enabled", False))
    except Exception:
        return False


def set_total_secret_mask_enabled(enabled: bool, chat_id: int | None = None):
    try:
        if chat_id is not None:
            owner_scoped_settings(int(chat_id))["total_secret_mask_enabled"] = bool(enabled)
            save_data(data, chat_ids=[int(chat_id)])
            schedule_config_backup_for_chats(int(chat_id), delay=0.3)
        else:
            data.setdefault("_global_settings", {})["total_secret_mask_enabled"] = bool(enabled)
            save_data(data)
    except Exception as e:
        log_error(f"set_total_secret_mask_enabled: {e}")


def toggle_total_secret_mask(chat_id: int | None = None) -> bool:
    new_value = not total_secret_mask_enabled(chat_id)
    set_total_secret_mask_enabled(new_value, chat_id)
    return new_value


def total_secret_mask_label(chat_id: int | None = None) -> str:
    return "🪷 Маска: ВКЛ" if total_secret_mask_enabled(chat_id) else "🪷 Маска: ВЫКЛ"

def verbose_process_journal_enabled() -> bool:
    """Подробный PROCESS-журнал нужен только для диагностики. По умолчанию выключен, чтобы не тормозить бот."""
    try:
        if _env_bool("BOT_JOURNAL_VERBOSE_PROCESS", "0"):
            return True
    except Exception:
        pass
    try:
        return bool((data or {}).setdefault("_global_settings", {}).get("bot_journal_verbose_process", False))
    except Exception:
        return False


def verbose_telegram_journal_enabled() -> bool:
    """Успешные Telegram API-вызовы сильно раздувают журнал. Включать только для диагностики."""
    try:
        if _env_bool("BOT_JOURNAL_VERBOSE_TELEGRAM", "0"):
            return True
    except Exception:
        pass
    try:
        return bool((data or {}).setdefault("_global_settings", {}).get("bot_journal_verbose_telegram", False))
    except Exception:
        return False


def _journal_write_row(row: dict):
    try:
        with open(BOT_JOURNAL_FILE, "a", encoding="utf-8") as jf:
            jf.write(json.dumps(row, ensure_ascii=False) + "\n")
    except Exception:
        pass


def bot_journal(action: str, chat_id=None, detail: str = "", level: str = "INFO"):
    """Пишет действие в общий журнал: команды, кнопки, функции, Telegram API, backup, ошибки."""
    try:
        # Если регистрация выключена — не пишем обычные действия. Ошибки остаются в /errors.
        if str(action or "") not in {"journal_toggle", "journal_chat_toggle", "journal_export_requested"} and str(level or "INFO").upper() != "ERROR":
            if not journal_should_record(chat_id):
                return None
        row = {
            "ts": _journal_ts(),
            "level": str(level or "INFO"),
            "action": str(action or "")[:160],
            "chat_id": str(chat_id) if chat_id is not None else "",
            "chat_name": "",
            "detail": str(detail or "")[:3000],
            "thread": threading.current_thread().name,
            "profile": active_bot_behavior_profile() if "data" in globals() and isinstance(data, dict) else "startup",
            "webhook_pending": WEBHOOK_TASK_POOL.stats().get("pending", 0),
            "general_pending": GENERAL_TASK_POOL.stats().get("pending", 0),
            "backup_pending": BACKUP_TASK_POOL.stats().get("pending", 0),
        }
        try:
            if chat_id is not None:
                row["chat_name"] = get_chat_display_name(int(chat_id))
        except Exception:
            pass
        with bot_journal_lock:
            BOT_ACTION_LOG.append(row)
        if not JOURNAL_TASK_POOL.submit("journal-file", _journal_write_row, dict(row)):
            _journal_write_row(row)
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
O9_SECRET_WAIT_COUNTDOWN_STEP_SECONDS = 30
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


def _o9_action_scheduler_key(key) -> str:
    try:
        return f"o9-action:{int(key[0])}:{int(key[1])}:{str(key[2])}"
    except Exception:
        return f"o9-action:{str(key)}"


def _cancel_o9_secret_timer(key):
    try:
        _o9_secret_action_timers.pop(key, None)
        DELAYED_SCHEDULER.cancel(_o9_action_scheduler_key(key))
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
    try:
        DELAYED_SCHEDULER.cancel(f"o9-secret-wait:{key}")
    except Exception:
        pass


def schedule_o9_secret_wait_timeout(chat_id: int, prompt_message_id: int, delay: int = O9_SECRET_WAIT_SECONDS):
    """Автоотмена ожидания секрета без частого редактирования таймера."""
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
            with _o9_secret_click_lock:
                current = _o9_secret_wait_timers.get(key)
                if current is not token or token.get("cancelled"):
                    return
                _o9_secret_wait_timers.pop(key, None)
            _clear_secret_wait(chat_id, delete_prompt=True)
            send_and_auto_delete(chat_id, "⌛ Время принятия секретных данных истекло.", 8)
        except Exception as e:
            log_error(f"schedule_o9_secret_wait_timeout({chat_id},{prompt_message_id}): {e}")

    DELAYED_SCHEDULER.schedule(f"o9-secret-wait:{key}", int(delay), _job)


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
                scheduler_key = _o9_action_scheduler_key(key)
                if kind == "close":
                    deadline = DELAYED_SCHEDULER.schedule(
                        scheduler_key,
                        O9_SECRET_CLICK_WINDOW_SECONDS + 0.2,
                        _o9_delayed_close,
                        chat_id,
                        msg_id,
                        key,
                    )
                else:
                    deadline = DELAYED_SCHEDULER.schedule(
                        scheduler_key,
                        O9_SECRET_CLICK_WINDOW_SECONDS + 0.2,
                        _o9_delayed_back_main,
                        chat_id,
                        msg_id,
                        day_key,
                        key,
                    )
                _o9_secret_action_timers[key] = deadline

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
            if self.enabled or verbose_process_journal_enabled():
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
            if self.enabled or verbose_process_journal_enabled():
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
            if self.enabled or verbose_process_journal_enabled():
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
# Уникальные метки окон для точного ориентира при отладке
# С1/С2/... — секретный режим.
# Ф1/Ф2/... — финансовый режим и общие служебные окна бота.
# П1/П2/... — пересылка.
#
# ВАЖНО:
# - один нормализованный переход/нажатая кнопка всегда получает одну и ту же метку;
# - разные переходы не получают один и тот же номер внутри своей группы;
# - реальный chat_id, дата, message_id и другие переменные параметры не создают
#   новые имена: метка указывает именно на участок логики/кнопку;
# - все маркеры заранее прописаны в WINDOW_MARKER_CONSTANTS;
# - при нажатиях новые номера не создаются и данные бота на нумерацию не влияют.
# ─────────────────────────────────────────────────────────────
WINDOW_MARK_RE = re.compile(r"(?:^|\s)([СФП]\d{1,6}|[ов]\d{1,3})\s*$", re.IGNORECASE)
_WINDOW_MARK_GROUPS = ("С", "Ф", "П")

# ФИКСИРОВАННЫЕ МАРКЕРЫ ОКОН.
# Все имена назначены заранее в коде и не создаются при нажатии кнопок.
# Не менять существующие номера: по ним пользователь даёт точные ориентиры.
WINDOW_MARKER_CONSTANTS = {
    'forward_menu_style_toggle': 'П1',
    'fw_': 'П2',
    'fw_back_root': 'П3',
    'fw_back_src': 'П4',
    'fw_back_tgt:*': 'П5',
    'fw_finpair:*': 'П6',
    'fw_finpair:*:ab': 'П7',
    'fw_finpair:*:ba': 'П8',
    'fw_mode:*': 'П9',
    'fw_mode:*:del': 'П10',
    'fw_mode:*:from': 'П11',
    'fw_mode:*:to': 'П12',
    'fw_mode:*:two': 'П13',
    'fw_new_back_src': 'П14',
    'fw_new_clear:*': 'П15',
    'fw_new_fin:*': 'П16',
    'fw_new_fin:*:ab': 'П17',
    'fw_new_fin:*:ba': 'П18',
    'fw_new_mode:*': 'П19',
    'fw_new_mode:*:from': 'П20',
    'fw_new_mode:*:to': 'П21',
    'fw_new_mode:*:two': 'П22',
    'fw_new_pair:*': 'П23',
    'fw_new_src:*': 'П24',
    'fw_new_tgt:*': 'П25',
    'fw_open': 'П26',
    'fw_probe_all': 'П27',
    'fw_probe_one:*': 'П28',
    'fw_removed_list': 'П29',
    'fw_src:*': 'П30',
    'fw_tgt:*': 'П31',
    'secbacklist': 'С1',
    'secchatcal:*': 'С2',
    'secclose': 'С3',
    'secday:*': 'С4',
    'secdel:*': 'С5',
    'secdelgo:*': 'С6',
    'secdelt:*': 'С7',
    'secedit:*': 'С8',
    'secedselected:*': 'С9',
    'secedtoggle:*': 'С10',
    'seclist:*': 'С11',
    'secmclose': 'С12',
    'secmedia:*': 'С13',
    'secmon:*': 'С14',
    'secmonthlist:*': 'С15',
    'secmwait': 'С16',
    'secret_cancel': 'С17',
    'sectoggle:*': 'С18',
    'secview:*': 'С19',
    'total_secret_mask_toggle': 'С20',
    'additional_owners': 'Ф1',
    'addown:*': 'Ф2',
    'articles_desc': 'Ф3',
    'aux_close': 'Ф4',
    'bp:collapse': 'Ф5',
    'bp:open': 'Ф6',
    'buttons_current_toggle': 'Ф7',
    'c:*': 'Ф8',
    'cat_': 'Ф9',
    'cat_add': 'Ф10',
    'cat_add_cancel': 'Ф11',
    'cat_close': 'Ф12',
    'cat_del_menu': 'Ф13',
    'cat_del_selected': 'Ф14',
    'cat_del_toggle:*': 'Ф15',
    'cat_desc': 'Ф16',
    'cat_edit_menu': 'Ф17',
    'cat_edit_pick:*': 'Ф18',
    'cat_m:*': 'Ф19',
    'cat_months': 'Ф20',
    'cat_months_y:*': 'Ф21',
    'cat_pick_end2:*': 'Ф22',
    'cat_pick_end:*': 'Ф23',
    'cat_pick_set_end2:*': 'Ф24',
    'cat_pick_set_end:*': 'Ф25',
    'cat_pick_set_start:*': 'Ф26',
    'cat_pick_start:*': 'Ф27',
    'cat_range_custom2:*': 'Ф28',
    'cat_range_custom:*': 'Ф29',
    'cat_rng:*': 'Ф30',
    'cat_show:*': 'Ф31',
    'cat_show_wk:*': 'Ф32',
    'cat_show_wthu:*': 'Ф33',
    'cat_today': 'Ф34',
    'cat_wk:*': 'Ф35',
    'cat_wthu:*': 'Ф36',
    'catx:*': 'Ф37',
    'cbx:*': 'Ф38',
    'd:*': 'Ф39',
    'd:*:back_main': 'Ф40',
    'd:*:backup_menu': 'Ф41',
    'd:*:bk_channel': 'Ф42',
    'd:*:bk_chat': 'Ф43',
    'd:*:bk_mega': 'Ф44',
    'd:*:calendar': 'Ф45',
    'd:*:cancel_edit': 'Ф46',
    'd:*:csv_all': 'Ф47',
    'd:*:del_selected': 'Ф48',
    'd:*:edit_list': 'Ф49',
    'd:*:edit_menu': 'Ф50',
    'd:*:fin_windows_menu': 'Ф51',
    'd:*:forward_finmode_menu': 'Ф52',
    'd:*:forward_menu': 'Ф53',
    'd:*:info': 'Ф54',
    'd:*:next': 'Ф55',
    'd:*:open': 'Ф56',
    'd:*:prev': 'Ф57',
    'd:*:process_menu': 'Ф58',
    'd:*:report': 'Ф59',
    'd:*:today': 'Ф60',
    'd:*:total': 'Ф61',
    'dzv:*': 'Ф62',
    'dzv:close': 'Ф63',
    'fc:*': 'Ф64',
    'finance:plain_window': 'Ф65',
    'finance_day5_toggle': 'Ф66',
    'fv:*': 'Ф67',
    'fv:*:bk_channel:*': 'Ф68',
    'fv:*:bk_chat:*': 'Ф69',
    'fv:*:bk_mega:*': 'Ф70',
    'fv:*:calendar:*': 'Ф71',
    'fv:*:cancel_edit:*': 'Ф72',
    'fv:*:clear_delete_back:*': 'Ф73',
    'fv:*:csv_menu:*': 'Ф74',
    'fv:*:del_selected:*': 'Ф75',
    'fv:*:edit_list:*': 'Ф76',
    'fv:*:info:*': 'Ф77',
    'fv:*:open:*': 'Ф78',
    'fv:*:report:*': 'Ф79',
    'fv:*:reset:*': 'Ф80',
    'fv:*:total:*': 'Ф81',
    'fvcat_': 'Ф82',
    'fvcatx:*': 'Ф83',
    'icon_buttons_toggle': 'Ф84',
    'info_close': 'Ф85',
    'info_finance_off': 'Ф86',
    'journal_back': 'Ф87',
    'journal_file': 'Ф88',
    'journal_open': 'Ф89',
    'journal_toggle': 'Ф90',
    'legacy_common:*': 'Ф91',
    'legacy_owner:*': 'Ф92',
    'markup:plain': 'Ф93',
    'ncb:*': 'Ф94',
    'ncb:*:no': 'Ф95',
    'ncb:*:yes': 'Ф96',
    'none': 'Ф97',
    'ojr:*': 'Ф98',
    'ojr:*:no': 'Ф99',
    'ojr:*:yes': 'Ф100',
    'rep:*': 'Ф101',
    'rep_close': 'Ф102',
    'rep_today': 'Ф103',
    'cat_pick_start_record:*': 'Ф104',
    'cat_pick_end3:*': 'Ф105',
    'cat_pick_set_end3:*': 'Ф106',
    'cat_pick_end_record:*': 'Ф107',
    'cat_range_records:*': 'Ф110',
    'cat_show_records:*': 'Ф109',
    'cat_back_records:*': 'Ф149',
    'exp_pick_start:*': 'Ф111',
    'exp_pick_set_start:*': 'Ф112',
    'exp_pick_start_record:*': 'Ф113',
    'exp_pick_end:*': 'Ф114',
    'exp_pick_set_end:*': 'Ф115',
    'exp_pick_end_record:*': 'Ф116',
    'exp_send:*:csv:*': 'Ф117',
    'exp_send:*:xlsx:*': 'Ф118',
    'd:*:backup_mass_chat': 'Ф119',
    'd:*:backup_mass_channel': 'Ф120',
    'd:*:backup_mass_mega': 'Ф121',
    'cat_prompt_back': 'Ф122',
    'info_instruction': 'Ф123',
    'info_queues': 'Ф124',
    'mega_priority_toggle': 'Ф125',
    'journal_chats_open': 'Ф126',
    'journal_chats_open:*': 'Ф127',
    'journal_chat_toggle:*': 'Ф128',
    'journal_chats_back': 'Ф129',
    'main_articles_toggle': 'Ф130',
    'cat_main_edit:*': 'Ф131',
    'version_menu': 'Ф132',
    'version_select:*': 'Ф133',
    'version_back': 'Ф134',
    'main_financial_values_toggle': 'Ф135',
    'keepalive_status': 'Ф136',
    'gomonk_open': 'Ф137',
    'gomonk_toggle': 'Ф138',
    'gomonk_back': 'Ф139',
    'remaining_open:*': 'Ф140',
    'remaining_toggle:*': 'Ф141',
    'cat_pick_today_start': 'Ф142',
    'cat_usd_toggle_records:*': 'Ф143',
    'usd_display_toggle': 'Ф144',
    'currency_menu': 'Ф145',
    'currency_select:*': 'Ф146',
    'currency_back': 'Ф147',
    'info_delta_status': 'Ф148',
    'cat_usd_toggle_period:*': 'Ф150',
    'cat_order_open_sum:*': 'Ф151',
    'cat_order_open_exact:*': 'Ф152',
    'cat_order_move_sum:*': 'Ф153',
    'cat_order_move_exact:*': 'Ф154',
    'cat_other_sort:*': 'Ф155',
    'cat_other_sort_toggle:*': 'Ф156',
    'cat_other_sort_choose:*': 'Ф157',
    'cat_other_sort_target:*': 'Ф158',
    'cat_pick_today_end:*': 'Ф159',
    'exp_send:*:xlsxstat:*': 'Ф160',
    'forward_copy_edit_mode_toggle': 'Ф161',
    'fwdcopy_edit': 'Ф162',
    'fwdcopy_edit_cancel': 'Ф163',
    'd:*:usd_tx_toggle': 'Ф164',
}

WINDOW_MARKER_UNKNOWN = {"С": "С9998", "Ф": "Ф9998", "П": "П9998"}


def has_window_mark(text: str) -> bool:
    try:
        tail = str(text or "")[-160:]
        tail = re.sub(r"<[^>]+>", "", tail)
        return bool(WINDOW_MARK_RE.search(tail))
    except Exception:
        return False


def strip_window_mark(text: str) -> str:
    try:
        text = str(text or "")
        text = re.sub(r"\n\s*<i>(?:[СФП]\d{1,6}|[ов]\d{1,3})</i>\s*$", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\n\s*(?:[СФП]\d{1,6}|[ов]\d{1,3})\s*$", "", text, flags=re.IGNORECASE)
        return text.rstrip()
    except Exception:
        return str(text or "")


def window_mark(text: str, code: str, html_mode: bool = False) -> str:
    try:
        text = strip_window_mark(str(text or ""))
        code = str(code or "").strip()
        if not code:
            return text
        pad = " " * 26
        if html_mode:
            return text + "\n\n" + pad + f"<i>{html.escape(code)}</i>"
        return text + "\n\n" + pad + code
    except Exception:
        return str(text or "")


def _normalize_window_action(data_str: str) -> str:
    d = str(data_str or "").strip()
    try:
        d = resolve_short_callback(d) or d
    except Exception:
        pass
    if not d:
        return "finance:unknown"
    d = d.replace(" ", "_")
    parts = d.split(":")
    norm = []
    for idx, part in enumerate(parts):
        low = str(part or "").strip().casefold()
        if idx == 0:
            norm.append(low or "unknown")
            continue
        if re.fullmatch(r"[a-zа-яё_][a-zа-яё0-9_\-]{0,48}", low, flags=re.IGNORECASE):
            norm.append(low)
        else:
            norm.append("*")
    compact = []
    for item in norm:
        if item == "*" and compact and compact[-1] == "*":
            continue
        compact.append(item)
    return ":".join(compact)


def _window_group_for_action(action_key: str) -> str:
    head = str(action_key or "").casefold().split(":", 1)[0]
    if head.startswith("sec") or head.startswith("secret") or head.startswith("total_secret"):
        return "С"
    if head.startswith("fw") or head.startswith("forward"):
        return "П"
    return "Ф"


def _marker_constant_pattern_matches(pattern: str, key: str) -> bool:
    """Сопоставляет статический ключ с константным шаблоном.

    Звёздочка внутри шаблона соответствует одному сегменту callback, а
    последняя звёздочка — всему оставшемуся хвосту. Это не создаёт маркеры
    динамически: номера по-прежнему берутся только из таблицы констант.
    """
    p_parts = str(pattern or "").split(":")
    k_parts = str(key or "").split(":")
    for idx, part in enumerate(p_parts):
        if idx >= len(k_parts):
            return False
        if part == "*":
            if idx == len(p_parts) - 1:
                return True
            continue
        if part != k_parts[idx]:
            return False
    return len(k_parts) == len(p_parts)


def _window_marker_code(action_key: str, forced_group: str | None = None) -> str:
    key = _normalize_window_action(action_key)
    code = WINDOW_MARKER_CONSTANTS.get(key)
    if code:
        return code
    # Более конкретные шаблоны проверяются первыми. Все номера всё равно
    # заранее записаны в WINDOW_MARKER_CONSTANTS.
    candidates = sorted(
        WINDOW_MARKER_CONSTANTS.items(),
        key=lambda item: (item[0].count("*"), -len(item[0])),
    )
    for pattern, marker in candidates:
        if "*" in pattern and _marker_constant_pattern_matches(pattern, key):
            return marker
    group = str(forced_group or _window_group_for_action(key)).upper()
    if group not in _WINDOW_MARK_GROUPS:
        group = "Ф"
    try:
        log_error(f"WINDOW_MARKER_NOT_DECLARED: {key}")
    except Exception:
        pass
    return WINDOW_MARKER_UNKNOWN[group]


def window_code_for_callback(data_str: str, owner_chat: bool = False) -> str:
    return _window_marker_code(str(data_str or ""))


def _window_key_from_markup(reply_markup) -> str:
    """Определяет фиксированный маркер окна по его кнопкам.

    Ф93 оставлен только за окном выбора месяцев. Остальные окна получают
    собственный заранее объявленный маркер по первой содержательной кнопке,
    поэтому один и тот же Ф93 больше не повторяется во всех окнах статей.
    """
    try:
        rows = getattr(reply_markup, "keyboard", None) or []
        values = []
        for row in rows:
            for btn in row:
                cb = getattr(btn, "callback_data", None)
                if cb:
                    values.append(_normalize_window_action(str(cb)))
        if values:
            # Ф93 — конкретно окно 2×6 с месяцами и переключением года.
            if any(v.startswith("cat_m:") for v in values) and any(v.startswith("cat_months_y:") for v in values):
                return "markup:plain"
            # Берём первую содержательную кнопку, для которой маркер объявлен константой.
            for value in values:
                if value == "none":
                    continue
                if value in WINDOW_MARKER_CONSTANTS:
                    return value
            for value in values:
                if value != "none":
                    return value
    except Exception:
        pass
    return "finance:plain_window"


def auto_window_mark(text: str, data_str: str = "", owner_chat: bool = False, html_mode: bool = False) -> str:
    return window_mark(text, window_code_for_callback(data_str, owner_chat=owner_chat), html_mode=html_mode)


def wm_common(text: str, n: int, html_mode: bool = False) -> str:
    body = strip_window_mark(str(text or ""))
    return window_mark(body, _window_marker_code(f"legacy_common:{int(n)}", "Ф"), html_mode=html_mode)


def wm_owner(text: str, n: int, html_mode: bool = False) -> str:
    body = strip_window_mark(str(text or ""))
    return window_mark(body, _window_marker_code(f"legacy_owner:{int(n)}", "Ф"), html_mode=html_mode)


def audit_window_marker_registry() -> dict:
    """Проверяет статическую таблицу констант на повторы."""
    values = list(WINDOW_MARKER_CONSTANTS.values())
    duplicates = sorted({v for v in values if values.count(v) > 1})
    return {
        "fixed": 0,
        "duplicates": duplicates,
        "groups": {g: sum(1 for v in values if v.startswith(g)) for g in _WINDOW_MARK_GROUPS},
        "constant": True,
    }

_v98_auto_close_timers = {}
_v98_auto_close_lock = threading.RLock()


def _v98_scheduler_key(chat_id: int, message_id: int) -> str:
    return f"v98-close:{int(chat_id)}:{int(message_id)}"


def _cancel_v98_auto_close(chat_id: int, message_id: int):
    key = (int(chat_id), int(message_id))
    with _v98_auto_close_lock:
        _v98_auto_close_timers.pop(key, None)
    DELAYED_SCHEDULER.cancel(_v98_scheduler_key(chat_id, message_id))


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

    deadline = DELAYED_SCHEDULER.schedule(
        _v98_scheduler_key(chat_id, message_id),
        int(delay),
        _job,
    )
    with _v98_auto_close_lock:
        _v98_auto_close_timers[(chat_id, message_id)] = deadline


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
try:
    BACKUP_MIN_DELAY_SECONDS = max(30.0, float(os.getenv("BACKUP_MIN_DELAY_SECONDS", "120") or "120"))
except Exception:
    BACKUP_MIN_DELAY_SECONDS = 120.0
try:
    BACKUP_BUSY_RETRY_SECONDS = max(15.0, float(os.getenv("BACKUP_BUSY_RETRY_SECONDS", "60") or "60"))
except Exception:
    BACKUP_BUSY_RETRY_SECONDS = 60.0

_dozvon_sessions = {}
_dozvon_target_index = defaultdict(set)


def day_key_from_message(msg=None) -> str:
    try:
        if msg and getattr(msg, "date", None):
            return datetime.fromtimestamp(msg.date, tz=get_tz()).strftime("%Y-%m-%d")
    except Exception:
        pass
    return today_key()


def finance_day_start_5am_enabled(chat_id: int | None = None) -> bool:
    """Режим финансовых суток хранится отдельно в owner scope."""
    return bool(_owner_setting_value("finance_day_start_5am", False, chat_id))


def toggle_finance_day_start_5am(chat_id: int | None = None) -> bool:
    new_value = not finance_day_start_5am_enabled(chat_id)
    _set_owner_setting_value("finance_day_start_5am", new_value, chat_id)
    return new_value


def finance_day_key_from_datetime(dt: datetime, chat_id: int | None = None) -> str:
    try:
        if finance_day_start_5am_enabled(chat_id):
            dt = dt - timedelta(hours=5)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return today_key()


def finance_day_key_from_message(msg=None) -> str:
    try:
        if msg and getattr(msg, "date", None):
            dt = datetime.fromtimestamp(int(msg.date), tz=get_tz())
        else:
            dt = now_local()
        cid = getattr(getattr(msg, "chat", None), "id", None) if msg is not None else current_state_chat_id()
        return finance_day_key_from_datetime(dt, cid)
    except Exception:
        return day_key_from_message(msg)


def finance_today_key(chat_id: int | None = None) -> str:
    return finance_day_key_from_datetime(now_local(), chat_id if chat_id is not None else current_state_chat_id())


def finance_day_start_label(chat_id: int | None = None) -> str:
    return "05:00" if finance_day_start_5am_enabled(chat_id) else "00:00"


RU_MONTH_NAMES = (
    "Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
    "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь",
)


def russian_month_name(month: int) -> str:
    try:
        return RU_MONTH_NAMES[int(month) - 1]
    except Exception:
        return str(month)


def calendar_window_text(center_day: datetime, marker: bool = True) -> str:
    text = f"📅 Выберите день:\n{russian_month_name(center_day.month)} {center_day.year}"
    return wm_common(text, 2) if marker else text


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

def fmt_date_table(day_key: str) -> str:
    """Формат дат в пользовательских CSV/Excel: DD.MM.YY."""
    try:
        d = datetime.strptime(str(day_key)[:10], "%Y-%m-%d")
        return d.strftime("%d.%m.%y")
    except Exception:
        raw = str(day_key or "")
        return raw.replace(":", ".")


def insert_blank_rows_between_days(rows: list[list], header_rows: int = 1, date_col: int = 0) -> list[list]:
    """Добавляет пустую строку между разными днями в Excel-таблицах."""
    rows = list(rows or [])
    head = rows[:max(0, int(header_rows))]
    body = rows[max(0, int(header_rows)):]
    out = list(head)
    prev_day = None
    for row in body:
        row = list(row or [])
        day = str(row[date_col]).strip() if len(row) > date_col else ""
        if day and prev_day is not None and day != prev_day:
            out.append([])
        out.append(row)
        if day:
            prev_day = day
    return out


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


def mega_backup_priority_enabled(chat_id: int | None = None) -> bool:
    """Приоритет MEGA — настройка owner scope; без контекста сохраняется legacy fallback."""
    return bool(_owner_setting_value("mega_backup_priority", False, chat_id))


def set_mega_backup_priority_enabled(enabled: bool, chat_id: int | None = None):
    _set_owner_setting_value("mega_backup_priority", bool(enabled), chat_id)
    if mega_is_configured():
        _schedule_global_mega_snapshot(1.0)


def toggle_mega_backup_priority(chat_id: int | None = None) -> bool:
    new_value = not mega_backup_priority_enabled(chat_id)
    set_mega_backup_priority_enabled(new_value, chat_id)
    return new_value


def mega_backup_priority_label(chat_id: int | None = None) -> str:
    return "☁️ Сразу в MEGA" if mega_backup_priority_enabled(chat_id) else "🕓 MEGA как обычно"

def backup_excel_all_enabled() -> bool:
    try:
        return bool((data or {}).setdefault("_global_settings", {}).get("backup_excel_all_enabled", True))
    except Exception:
        return True


def set_backup_excel_all_enabled(enabled: bool):
    data.setdefault("_global_settings", {})["backup_excel_all_enabled"] = bool(enabled)
    save_data(data, full=True)


def toggle_backup_excel_all_enabled() -> bool:
    new_value = not backup_excel_all_enabled()
    set_backup_excel_all_enabled(new_value)
    return new_value


def backup_excel_all_label() -> str:
    return "ВКЛ" if backup_excel_all_enabled() else "ВЫКЛ"


def _backup_target_all_state(target: str) -> tuple[int, int]:
    ids = [int(cid) for cid, _ in _collect_backup_menu_items()]
    if target == "chat":
        ids = [cid for cid in ids if is_owner_chat(cid)]
    enabled = sum(1 for cid in ids if is_backup_target_enabled(cid, target))
    return enabled, len(ids)


def set_backup_target_for_all(target: str, enabled: bool) -> int:
    count = 0
    for cid, _title in _collect_backup_menu_items():
        cid = int(cid)
        if target == "chat" and not is_owner_chat(cid):
            continue
        settings = _ensure_backup_settings(cid)
        settings[_backup_target_setting_key(target)] = bool(enabled)
        settings["auto_backup_enabled"] = any((
            bool(settings.get("auto_backup_to_chat_enabled", True)),
            bool(settings.get("auto_backup_to_channel_enabled", True)),
            bool(settings.get("auto_backup_to_mega_enabled", True)),
        ))
        count += 1
    save_data(data, full=True)
    for cid, _title in _collect_backup_menu_items():
        schedule_backup_flush(int(cid), BACKUP_MIN_DELAY_SECONDS)
    return count


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
        # Перед добавлением убираем старые карточки того же чата по username/title, чтобы не плодить дубли.
        new_identity = _chat_identity_key(chat_id, new_known)
        for old_cid, old_info in list(kc.items()):
            try:
                old_id_int = int(old_cid)
            except Exception:
                kc.pop(old_cid, None)
                changed = True
                continue
            if str(old_cid) != str(chat_id) and _chat_identity_key(old_id_int, old_info if isinstance(old_info, dict) else {}) == new_identity:
                kc.pop(old_cid, None)
                changed = True
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
    try:
        normalize_known_chats_for_owner()
    except Exception:
        pass
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

    scheduler_key = f"main-window-recreate:{chat_id}"
    with timer_lock:
        DELAYED_SCHEDULER.cancel(scheduler_key)
        deadline = DELAYED_SCHEDULER.schedule(scheduler_key, delay, _job)
        _balance_panel_recreate_timers[("main", chat_id)] = deadline


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

    scheduler_key = f"quick-balance-first:{chat_id}"
    with timer_lock:
        DELAYED_SCHEDULER.cancel(scheduler_key)
        deadline = DELAYED_SCHEDULER.schedule(scheduler_key, delay, _job)
        _balance_panel_first_timers[chat_id] = deadline



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

    scheduler_key = f"quick-balance-recreate:{chat_id}"
    with timer_lock:
        DELAYED_SCHEDULER.cancel(scheduler_key)
        deadline = DELAYED_SCHEDULER.schedule(scheduler_key, delay, _job)
        _balance_panel_recreate_timers[chat_id] = deadline


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

    scheduler_key = f"owner-total-delete:{key}"
    DELAYED_SCHEDULER.cancel(scheduler_key)
    deadline = DELAYED_SCHEDULER.schedule(scheduler_key, delay, _job)
    _total_message_timers[key] = deadline


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
        if current:
            unregister_open_window(chat_id, int(current))
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
                unregister_open_window(chat_id, int(message_id))
                save_data(data)
        except Exception as e:
            log_error(f"schedule_stored_window_delete({chat_id},{store_key}): {e}")

    scheduler_key = f"stored-window-delete:{int(chat_id)}:{str(store_key)}"
    DELAYED_SCHEDULER.cancel(scheduler_key)
    deadline = DELAYED_SCHEDULER.schedule(scheduler_key, delay, _job)
    _aux_window_timers[key] = deadline


def default_window_nav_keyboard(chat_id: int):
    """Кнопки для окон, где раньше не было кнопок: закрыть + назад в основное окно."""
    kb = types.InlineKeyboardMarkup()
    day = get_chat_store(chat_id).get("current_view_day") or today_key()
    kb.row(
        IB("⬅️ Назад осн. окно", callback_data=f"d:{day}:back_main"),
        IB("❌ Закрыть", callback_data="aux_close"),
    )
    return kb


def _open_window_registry() -> dict:
    return data.setdefault("open_window_registry", {})


def register_open_window(chat_id: int, message_id: int, window_type: str, code: str = "", day_key: str | None = None, params: dict | None = None):
    try:
        chat_id = int(chat_id)
        message_id = int(message_id)
        key = f"{owner_scope_id(chat_id)}:{chat_id}:{message_id}"
        params = params or {}
        currency_chat_id = chat_id
        try:
            if params.get("target_chat_id") is not None:
                currency_chat_id = int(params.get("target_chat_id"))
        except Exception:
            currency_chat_id = chat_id
        _open_window_registry()[key] = {
            "owner_id": owner_scope_id(chat_id),
            "chat_id": chat_id,
            "message_id": message_id,
            "window_type": str(window_type or ""),
            "code": str(code or ""),
            "currency_mode": currency_mode(currency_chat_id) if "currency_mode" in globals() else "ars",
            "day_key": day_key,
            "params": params,
            "updated_at": now_local().isoformat(timespec="seconds"),
        }
        # Реестр должен переживать перезапуск, поэтому фиксируем root SQLite сразу.
        save_data(data, root_only=True)
    except Exception as e:
        log_error(f"register_open_window: {e}")


def unregister_open_window(chat_id: int, message_id: int):
    try:
        chat_id = int(chat_id); message_id = int(message_id)
        reg = _open_window_registry()
        changed = False
        for key, item in list(reg.items()):
            if int(item.get("chat_id", 0) or 0) == chat_id and int(item.get("message_id", 0) or 0) == message_id:
                reg.pop(key, None)
                changed = True
        if changed:
            save_data(data, root_only=True)
    except Exception:
        pass


def get_registered_open_window(chat_id: int, message_id: int) -> dict | None:
    """Возвращает фактическое последнее состояние конкретного Telegram-сообщения."""
    try:
        chat_id = int(chat_id); message_id = int(message_id)
        best = None
        for item in (_open_window_registry() or {}).values():
            if int((item or {}).get("chat_id", 0) or 0) != chat_id:
                continue
            if int((item or {}).get("message_id", 0) or 0) != message_id:
                continue
            best = item
        return best
    except Exception:
        return None


def register_static_open_view(chat_id: int, message_id: int, code: str = "", day_key: str | None = None, params: dict | None = None):
    """Помечает открытое меню как фактически открытое, чтобы фин-синхронизация не превращала его обратно в О1."""
    register_open_window(chat_id, message_id, "static_view", code=code, day_key=day_key, params=params or {})


def _message_missing_error(exc) -> bool:
    text = str(exc or "").lower()
    return any(x in text for x in (
        "message to edit not found", "message not found", "message_id_invalid",
        "message can't be edited", "chat not found", "bot was blocked", "forbidden",
    ))


def _markup_callback_values(reply_markup) -> list[str]:
    out = []
    try:
        for row in getattr(reply_markup, "keyboard", None) or getattr(reply_markup, "inline_keyboard", None) or []:
            for btn in row:
                cb = getattr(btn, "callback_data", None)
                if cb:
                    out.append(str(cb))
    except Exception:
        pass
    return out


def _refresh_categories_window_from_state(chat_id: int) -> bool:
    """Перерисовывает основные зависимые окна статей по сохранённому состоянию."""
    store = get_chat_store(chat_id)
    mid = store.get("categories_msg_id")
    state = store.get("categories_refresh_state") or {}
    if not mid or not state:
        return False
    marker = str(state.get("marker_action") or "")
    callbacks = [str(x) for x in (state.get("callbacks") or [])]
    try:
        if marker.startswith("cat_range_records"):
            cb = next((x for x in callbacks if x.startswith("cat_show_records:")), None)
            if cb:
                _, start_key, start_rid, end_key, end_rid, _slug = cb.split(":", 5)
                text, _ = summarize_categories_record_range(store, start_key, int(start_rid), end_key, int(end_rid))
                kb = build_categories_record_summary_keyboard(start_key, int(start_rid), end_key, int(end_rid), store)
                send_or_edit_categories_window(chat_id, text, reply_markup=kb, preferred_message_id=int(mid), marker_action="cat_range_records:*")
                return True
        if marker.startswith(("cat_order_open_sum", "cat_order_move_sum", "cat_order_select_sum", "cat_order_position_sum")):
            cb = next((x for x in callbacks if x.startswith("cat_order_select_sum:")), None)
            if cb:
                _, _slug, mode, start, end = cb.split(":", 4)
                send_or_edit_categories_window(
                    chat_id, build_category_layout_text(store, "sum"),
                    reply_markup=build_category_layout_keyboard(store, "sum", (mode, start, end), chat_id=chat_id),
                    preferred_message_id=int(mid), marker_action="cat_order_open_sum:*",
                )
                return True
        if marker.startswith(("cat_order_open_exact", "cat_order_move_exact", "cat_order_select_exact", "cat_order_position_exact")):
            cb = next((x for x in callbacks if x.startswith("cat_order_select_exact:")), None)
            if cb:
                _, _slug, start_key, start_rid, end_key, end_rid = cb.split(":", 5)
                params = (start_key, int(start_rid), end_key, int(end_rid))
                send_or_edit_categories_window(
                    chat_id, build_category_layout_text(store, "exact"),
                    reply_markup=build_category_layout_keyboard(store, "exact", params, chat_id=chat_id),
                    preferred_message_id=int(mid), marker_action="cat_order_open_exact:*",
                )
                return True
    except Exception as e:
        if _message_missing_error(e):
            unregister_open_window(chat_id, int(mid))
            store["categories_msg_id"] = None
            store["categories_refresh_state"] = None
        else:
            log_error(f"_refresh_categories_window_from_state({chat_id}): {e}")
    return False


def _refresh_registered_fin_view(item: dict, changed_chat_id: int) -> bool:
    """Перерисовывает окно владельца, которое показывает финансы другого чата."""
    params = item.get("params") or {}
    try:
        target_chat_id = int(params.get("target_chat_id") or 0)
        host_chat_id = int(item.get("chat_id") or 0)
        message_id = int(item.get("message_id") or 0)
    except Exception:
        return False
    if target_chat_id != int(changed_chat_id) or not host_chat_id or not message_id:
        return False
    view_day = str(item.get("day_key") or params.get("view_day") or get_chat_store(target_chat_id).get("current_view_day") or today_key())
    owner_day_key = str(params.get("owner_day_key") or get_chat_store(host_chat_id).get("current_view_day") or today_key())
    action = str(params.get("view_action") or "open")
    target_store = get_chat_store(target_chat_id)
    try:
        if action in {"open", "back_main", "menu", "clear_delete_back"}:
            text = render_fin_window_text(target_chat_id, view_day)
            kb = build_fin_window_view_keyboard(target_chat_id, view_day, owner_day_key)
            bot.edit_message_text(text, chat_id=host_chat_id, message_id=message_id, reply_markup=kb, parse_mode="HTML")
        elif action in {"edit_list", "del_toggle"}:
            text = render_fin_window_text(target_chat_id, view_day)
            kb = build_edit_records_keyboard(view_day, target_chat_id, prefix="fv", owner_day_key=owner_day_key)
            bot.edit_message_text(text, chat_id=host_chat_id, message_id=message_id, reply_markup=kb, parse_mode="HTML")
        elif action == "calendar":
            try:
                cdt = datetime.strptime(str(params.get("center_day") or view_day), "%Y-%m-%d")
            except Exception:
                cdt = now_local()
            bot.edit_message_text(
                f"📅 Календарь: {html.escape(get_chat_display_name(target_chat_id))}",
                chat_id=host_chat_id, message_id=message_id,
                reply_markup=build_fin_calendar_keyboard(target_chat_id, cdt, owner_day_key), parse_mode="HTML",
            )
        elif action == "report":
            try:
                month_key = datetime.strptime(view_day, "%Y-%m-%d").strftime("%Y-%m")
            except Exception:
                month_key = now_local().strftime("%Y-%m")
            report_html, _ = build_month_report_text(target_chat_id, month_key)
            bot.edit_message_text(
                f"👁 {html.escape(get_chat_display_name(target_chat_id))}\n" + report_html,
                chat_id=host_chat_id, message_id=message_id,
                reply_markup=_one_button_keyboard("🔙 Назад", f"fv:{target_chat_id}:{view_day}:open:{owner_day_key}"),
                parse_mode="HTML",
            )
        elif action == "total":
            text = f"👁 {html.escape(get_chat_display_name(target_chat_id))}\n\n💰 Общий итог по чату: {format_chat_amount(target_chat_id, target_store.get('balance', 0), True)}"
            bot.edit_message_text(text, chat_id=host_chat_id, message_id=message_id, reply_markup=build_fin_window_view_keyboard(target_chat_id, view_day, owner_day_key), parse_mode="HTML")
        elif action == "info":
            text = build_info_text(target_chat_id) + "\n\n" + build_articles_description_text(target_chat_id)
            bot.edit_message_text(text, chat_id=host_chat_id, message_id=message_id, reply_markup=build_fin_window_view_keyboard(target_chat_id, view_day, owner_day_key))
        elif action == "csv_menu":
            text = wm_common(f"📂 CSV / Excel: {html.escape(get_chat_display_name(target_chat_id))}\nВыберите период:", 5)
            bot.edit_message_text(text, chat_id=host_chat_id, message_id=message_id, reply_markup=build_fin_window_csv_menu(target_chat_id, view_day, owner_day_key), parse_mode="HTML")
        else:
            return False
        register_open_window(
            host_chat_id, message_id, "fin_view", code=f"fv:{action}", day_key=view_day,
            params={"target_chat_id": target_chat_id, "owner_day_key": owner_day_key, "view_action": action},
        )
        return True
    except Exception as e:
        if "message is not modified" in str(e).lower():
            return True
        if _message_missing_error(e):
            unregister_open_window(host_chat_id, message_id)
            return False
        log_error(f"_refresh_registered_fin_view({host_chat_id},{message_id}->{target_chat_id}): {e}")
        return False


def _build_total_window_text_for_registry(chat_id: int) -> str:
    """Тот же итог, что показывает кнопка «💰 Общий итог», но пригодный для автообновления реестра."""
    chat_id = int(chat_id)
    store = get_chat_store(chat_id)
    chat_bal = store.get("balance", 0)
    if not is_owner_chat(chat_id):
        return wm_common(f"💰 Общий итог по этому чату: {format_chat_amount(chat_id, chat_bal, True)}", 4)
    lines = [
        "💰 Общий итог (для владельца)",
        "",
        f"• Этот чат ({get_chat_display_name(chat_id)}): {format_chat_amount(chat_id, chat_bal, True)}",
    ]
    total_all = 0
    other_lines = []
    for cid, st in (data.get("chats", {}) or {}).items():
        try:
            cid_int = int(cid)
        except Exception:
            continue
        bal = st.get("balance", 0)
        total_all += bal
        if cid_int == chat_id:
            continue
        other_lines.append(f"   • {get_chat_display_name(cid_int)}: {format_chat_amount(chat_id, bal, True)}")
    if other_lines:
        lines.extend(["", "• Другие чаты:"])
        lines.extend(other_lines)
    lines.extend(["", f"• Всего по всем чатам: {format_chat_amount(chat_id, total_all, True)}"])
    return wm_common("\n".join(lines), 4)


def _refresh_registered_local_fin_view(item: dict, changed_chat_id: int) -> bool:
    """Сохраняет фактически открытый локальный финансовый экран, а не возвращает сообщение принудительно в О1."""
    params = item.get("params") or {}
    try:
        host_chat_id = int(item.get("chat_id") or 0)
        message_id = int(item.get("message_id") or 0)
    except Exception:
        return False
    if not host_chat_id or not message_id:
        return False
    action = str(params.get("view_action") or item.get("code") or "")
    depends_on_all = bool(params.get("depends_on_all"))
    if host_chat_id != int(changed_chat_id) and not depends_on_all:
        return False
    view_day = str(item.get("day_key") or params.get("view_day") or get_chat_store(host_chat_id).get("current_view_day") or today_key())
    try:
        if action == "calendar":
            center_s = str(params.get("center_day") or view_day)
            try:
                center_dt = datetime.strptime(center_s, "%Y-%m-%d")
            except Exception:
                center_dt = now_local()
            bot.edit_message_text(
                calendar_window_text(center_dt), chat_id=host_chat_id, message_id=message_id,
                reply_markup=build_calendar_keyboard(center_dt, host_chat_id),
            )
        elif action == "report":
            month_key = str(params.get("month_key") or view_day[:7])
            report_html, _ = build_month_report_text(host_chat_id, month_key)
            bot.edit_message_text(
                report_html, chat_id=host_chat_id, message_id=message_id,
                reply_markup=build_report_keyboard(month_key), parse_mode="HTML",
            )
        elif action == "total":
            bot.edit_message_text(
                _build_total_window_text_for_registry(host_chat_id),
                chat_id=host_chat_id, message_id=message_id, parse_mode="HTML",
            )
        elif action == "info":
            bot.edit_message_text(
                wm_common(build_info_text(host_chat_id), 9),
                chat_id=host_chat_id, message_id=message_id,
                reply_markup=build_info_keyboard(host_chat_id),
            )
        elif action == "csv_menu":
            txt, _ = render_day_window(host_chat_id, view_day)
            bot.edit_message_text(
                txt, chat_id=host_chat_id, message_id=message_id,
                reply_markup=build_csv_menu(view_day, host_chat_id), parse_mode="HTML",
            )
        elif action == "edit_list":
            txt, _ = render_day_window(host_chat_id, view_day)
            bot.edit_message_text(
                txt, chat_id=host_chat_id, message_id=message_id,
                reply_markup=build_edit_records_keyboard(view_day, host_chat_id), parse_mode="HTML",
            )
        else:
            return False
        register_open_window(
            host_chat_id, message_id, "local_fin_view", code=action, day_key=view_day,
            params={**params, "view_action": action},
        )
        return True
    except Exception as e:
        if "message is not modified" in str(e).lower():
            return True
        if _message_missing_error(e):
            unregister_open_window(host_chat_id, message_id)
            return False
        log_error(f"_refresh_registered_local_fin_view({host_chat_id},{message_id},{action}): {e}")
        return False


def _refresh_registered_fin_categories_view(item: dict, changed_chat_id: int) -> bool:
    """Автообновление открытых у владельца окон статей чужого/связанного чата."""
    params = item.get("params") or {}
    try:
        host_chat_id = int(item.get("chat_id") or 0)
        message_id = int(item.get("message_id") or 0)
        target_chat_id = int(params.get("target_chat_id") or 0)
    except Exception:
        return False
    if target_chat_id != int(changed_chat_id) or not host_chat_id or not message_id:
        return False
    action = str(params.get("view_action") or "")
    owner_day_key = str(params.get("owner_day_key") or today_key())
    store = get_chat_store(target_chat_id)
    try:
        if action == "wthu":
            ref = str(params.get("ref") or today_key())
            start_key = week_start_thursday(ref)
            start, end = week_bounds_thu_wed(start_key)
            label = f"{fmt_date_ddmmyy(start)} — {fmt_date_ddmmyy(end)} (Чт–Ср)"
            text, _ = summarize_categories(store, start, end, label)
            text = f"👁 {get_chat_display_name(target_chat_id)}\n" + text
            kb = build_fin_categories_summary_keyboard(target_chat_id, "wthu", start, end, owner_day_key)
            bot.edit_message_text(text, chat_id=host_chat_id, message_id=message_id, reply_markup=kb)
        elif action == "show":
            start = str(params.get("start") or today_key())
            end = str(params.get("end") or start)
            slug = str(params.get("slug") or "")
            category = get_category_by_slug(slug, store)
            if not category:
                return False
            label = f"{fmt_date_ddmmyy(start)} — {fmt_date_ddmmyy(end)}"
            text = f"👁 {get_chat_display_name(target_chat_id)}\n" + build_category_detail_text(store, start, end, category, label)
            kb = build_fin_categories_summary_keyboard(target_chat_id, "detail", start, end, owner_day_key)
            kb.row(IB("🔙 Назад", callback_data=fvcat_callback(f"fvcat_wthu:{target_chat_id}:{start}:{owner_day_key}")))
            kb.row(IB("🔙 К окну чата", callback_data=f"fv:{target_chat_id}:{start}:open:{owner_day_key}"))
            bot.edit_message_text(text, chat_id=host_chat_id, message_id=message_id, reply_markup=kb)
        else:
            return False
        register_open_window(
            host_chat_id, message_id, "fin_categories_view", code=f"fvcat:{action}", day_key=item.get("day_key"),
            params=params,
        )
        return True
    except Exception as e:
        if "message is not modified" in str(e).lower():
            return True
        if _message_missing_error(e):
            unregister_open_window(host_chat_id, message_id)
            return False
        log_error(f"_refresh_registered_fin_categories_view({host_chat_id},{message_id}->{target_chat_id},{action}): {e}")
        return False


def _refresh_registered_stored_window(item: dict, changed_chat_id: int) -> bool:
    """Перерисовывает известные отдельные окна текущего чата, зависящие от финансов/настроек."""
    try:
        host_chat_id = int(item.get("chat_id") or 0)
        message_id = int(item.get("message_id") or 0)
    except Exception:
        return False
    if host_chat_id != int(changed_chat_id) or not message_id:
        return False
    code = str(item.get("code") or "")
    store = get_chat_store(host_chat_id)
    try:
        if code == "info_msg_id":
            bot.edit_message_text(build_info_text(host_chat_id), chat_id=host_chat_id, message_id=message_id, reply_markup=build_info_keyboard(host_chat_id))
            return True
        if code == "report_window_id":
            month_key = str(store.get("report_month") or now_local().strftime("%Y-%m"))
            text, _ = build_month_report_text(host_chat_id, month_key)
            bot.edit_message_text(text, chat_id=host_chat_id, message_id=message_id, reply_markup=build_report_keyboard(month_key), parse_mode="HTML")
            return True
        # remaining_msg_id обновляется отдельным специализированным блоком ниже.
    except Exception as e:
        if "message is not modified" in str(e).lower():
            return True
        if _message_missing_error(e):
            unregister_open_window(host_chat_id, message_id)
            if store.get(code) == message_id:
                store[code] = None
                save_data(data, chat_ids=[host_chat_id])
            return False
        log_error(f"_refresh_registered_stored_window({host_chat_id},{message_id},{code}): {e}")
    return False


def refresh_registered_financial_windows(chat_id: int):
    """Обновляет известные открытые окна текущего owner scope после изменения финансов."""
    chat_id = int(chat_id)
    store = get_chat_store(chat_id)
    # Все фактически известные основные окна по дням, а не только current_view_day.
    for day_key, mid in list((get_or_create_active_windows(chat_id) or {}).items()):
        try:
            # Одно и то же Telegram-сообщение может быть превращено кнопками из О1
            # в Ф47/календарь/редактирование/другое меню. Не возвращаем его насильно в О1.
            actual = get_registered_open_window(chat_id, int(mid))
            if actual and str(actual.get("window_type") or "") not in {"", "main_day"}:
                continue
            text, _ = render_day_window(chat_id, day_key)
            bot.edit_message_text(text, chat_id=chat_id, message_id=int(mid), reply_markup=build_main_keyboard(day_key, chat_id))
            register_open_window(chat_id, int(mid), "main_day", code="О1", day_key=day_key)
        except Exception as e:
            if "message is not modified" in str(e).lower():
                continue
            if _message_missing_error(e):
                clear_active_window_id(chat_id, day_key)
                unregister_open_window(chat_id, int(mid))
    # Окно «с ост».
    mid = store.get("remaining_msg_id")
    if mid:
        day_key = store.get("current_view_day") or today_key()
        try:
            bot.edit_message_text(
                build_remaining_text(chat_id, day_key), chat_id=chat_id, message_id=int(mid),
                reply_markup=build_remaining_keyboard(chat_id, day_key), parse_mode="HTML",
            )
            register_open_window(chat_id, int(mid), "remaining", code="Ф91", day_key=day_key)
        except Exception as e:
            if _message_missing_error(e):
                store["remaining_msg_id"] = None
                unregister_open_window(chat_id, int(mid))
    _refresh_categories_window_from_state(chat_id)

    # Полный реестр: окна могут физически находиться в другом чате владельца,
    # но показывать данные изменившегося target_chat_id (Ф110/фин-окна).
    for _key, item in list((_open_window_registry() or {}).items()):
        try:
            wtype = str((item or {}).get("window_type") or "")
            if wtype == "fin_view":
                _refresh_registered_fin_view(item, chat_id)
            elif wtype == "local_fin_view":
                _refresh_registered_local_fin_view(item, chat_id)
            elif wtype == "fin_categories_view":
                _refresh_registered_fin_categories_view(item, chat_id)
            elif wtype == "stored":
                _refresh_registered_stored_window(item, chat_id)
        except Exception as e:
            log_error(f"refresh_registered_financial_windows registry item: {e}")


def send_or_edit_stored_window(chat_id: int, store_key: str, text: str, reply_markup=None, parse_mode=None, delay: int = AUX_WINDOW_DELETE_DELAY):
    store = get_chat_store(chat_id)
    if reply_markup is None:
        try:
            reply_markup = default_window_nav_keyboard(chat_id)
        except Exception:
            pass
    try:
        marker_key = f"stored:{store_key}:" + _window_key_from_markup(reply_markup)
        text = window_mark(
            text,
            _window_marker_code(marker_key),
            html_mode=(str(parse_mode or "").upper() == "HTML"),
        )
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
            register_open_window(chat_id, message_id, "stored", code=store_key, day_key=store.get("current_view_day"))
            schedule_stored_window_delete(chat_id, store_key, delay)
            return message_id
        except Exception as e:
            if "message is not modified" in str(e).lower():
                register_open_window(chat_id, message_id, "stored", code=store_key, day_key=store.get("current_view_day"))
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
                register_open_window(chat_id, message_id, "stored", code=store_key, day_key=store.get("current_view_day"))
                schedule_stored_window_delete(chat_id, store_key, delay)
                return message_id
            except Exception as e2:
                if "message is not modified" in str(e2).lower():
                    schedule_stored_window_delete(chat_id, store_key, delay)
                    return message_id
                unregister_open_window(chat_id, message_id)
                store[store_key] = None
                save_data(data)

    sent = bot.send_message(chat_id, text, reply_markup=reply_markup, parse_mode=parse_mode)
    store[store_key] = sent.message_id
    register_open_window(chat_id, sent.message_id, "stored", code=store_key, day_key=store.get("current_view_day"))
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
        store = get_chat_store(user_id)
        store.setdefault("settings", {})["owner_scope_id"] = int(user_id)
        store.setdefault("settings", {}).setdefault("owner_scope_settings", {})
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


def owner_scope_id(chat_id: int | None = None) -> int:
    """Logical owner namespace. Each additional owner keeps an independent settings world."""
    try:
        cid = int(chat_id) if chat_id is not None else int(OWNER_ID or 0)
    except Exception:
        cid = int(OWNER_ID or 0)
    if cid and is_owner_chat(cid):
        return cid
    try:
        store = get_chat_store(cid) if cid else {}
        scoped = int((store.get("settings") or {}).get("owner_scope_id") or 0)
        if scoped and is_owner_chat(scoped):
            return scoped
    except Exception:
        pass
    return int(OWNER_ID or cid or 0)


def owner_scoped_settings(chat_id: int | None = None) -> dict:
    scope = owner_scope_id(chat_id)
    if not scope:
        return data.setdefault("_global_settings", {})
    store = get_chat_store(scope)
    settings = store.setdefault("settings", {})
    return settings.setdefault("owner_scope_settings", {})


def bind_chat_to_owner_scope(chat_id: int, scope_id: int):
    try:
        get_chat_store(int(chat_id)).setdefault("settings", {})["owner_scope_id"] = int(scope_id)
        save_data(data, chat_ids=[int(chat_id)])
    except Exception as e:
        log_error(f"bind_chat_to_owner_scope({chat_id},{scope_id}): {e}")


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

    if data_str in {"info_close", "main_articles_toggle", "main_financial_values_toggle"}:
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
        "/ost — слово «ост:» в Ф91 ВКЛ/ВЫКЛ",
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
            "/mega_backup_now — безопасно загрузить latest_global.json в MEGA",
            "/mega_restore_now — принудительно полностью восстановить данные из MEGA",
            "/restore_guard — статус аварийной защиты восстановления",
            "/buttons — переключить кнопки: text/icons",
            "/mask — переключить маскировку тотального секрета",
            "/day5 — финсутки: 00:00 / 05:00",
            "/off_on_backup_excel — Excel-бэкап всех чатов ВКЛ/ВЫКЛ",
            "/queues — состояние очередей и нагрузки",
        ])
    lines.append("/help — эта справка")
    return "\n".join(lines)


def build_info_text(chat_id: int) -> str:
    """Компактный INFO: одна функция показывается один раз, без дублей команд и кнопок."""
    layout = version_mode_layout()
    lines = [
        "ℹ️ INFO",
        "",
        f"Финансы: {'ВКЛ' if is_finance_mode(chat_id) else 'ВЫКЛ'}",
        f"Текущее окно: {'ВКЛ' if chat_buttons_current_window_enabled(chat_id) else 'ВЫКЛ'}",
        f"Журнал чата: {'ВКЛ' if is_chat_journal_enabled(chat_id) else 'ВЫКЛ'}",
    ]
    if layout in {"v84", "v85", "v86", "v87"}:
        lines.append(f"Финансы-кнопки: {'ВКЛ' if main_financial_value_buttons_enabled(chat_id) else 'ВЫКЛ'}")
    if layout in {"v86", "v87"}:
        lines.append(f"Валюта: {currency_mode(chat_id).upper().replace('_', '-')}")
        lines.append(f"Подпись «ост:»: {'ВКЛ' if remaining_ost_label_enabled(chat_id) else 'ВЫКЛ'}")
    if version_mode_feature("forward_copy_edit"):
        lines.append(f"💰Перес: {forward_copy_edit_mode(chat_id).replace('normal', 'обычно').replace('button', 'кнопка').replace('slash', 'слеш')}")
    if is_owner_chat(chat_id):
        lines.extend([
            f"Кнопки интерфейса: {'значки' if icon_button_mode_enabled(chat_id) else 'текст'}",
            f"Маска секрета: {'ВКЛ' if total_secret_mask_enabled(chat_id) else 'ВЫКЛ'}",
            f"Финансовые сутки: с {finance_day_start_label(chat_id)}",
        ])
        if version_mode_feature("mega_priority"):
            lines.append(f"MEGA: {'приоритетный' if mega_backup_priority_enabled(chat_id) else 'обычный'} режим")
        lines.append(f"Версия: {active_bot_behavior_profile_info().get('title')}")
    lines.extend(["", "Слеш-команды:"])
    commands = [
        "/ok — включить финансовый режим",
        "/start — открыть окно сегодняшнего дня",
        "/prev — предыдущий день",
        "/next — следующий день",
        "/balance — баланс по текущему чату",
        "/report — краткий отчёт",
        "/csv — CSV текущего чата",
        "/xlsx — Excel текущего чата",
        "/tabl_lsx — Excel-таблица по периоду Чт–Ср",
        "/json — JSON текущего чата",
        "/ost — включить/выключить подпись «ост:»",
        "/restore — включить режим восстановления",
        "/restore_off — выключить режим восстановления",
        "/dozvon — открыть дозвон по связанным чатам",
        "/reset — обнулить данные чата с подтверждением",
        "/ping — проверить работу бота",
        "/help — полная справка",
    ]
    if is_owner_chat(chat_id):
        commands.extend([
            "/stopforward — полностью отключить пересылку",
            "/backup_channel_on — включить бэкап в канал",
            "/backup_channel_off — выключить бэкап в канал",
            "/diag — диагностика бота",
            "/errors — последние ошибки",
            "/journal — скачать журнал действий",
            "/articles — описание статей и ключевых слов",
            "/mega_status — статус MEGA",
            "/mega_backup_now — запустить безопасный бэкап MEGA",
            "/restore_guard — статус защиты восстановления",
            "/buttons — переключить вид кнопок",
            "/mask — переключить маскировку тотального секрета",
            "/day5 — начало финансовых суток 00:00 / 05:00",
            "/off_on_backup_excel — Excel-бэкап всех чатов ВКЛ/ВЫКЛ",
            "/queues — состояние очередей и нагрузки",
        ])
    # Защита от случайных дублей: команда (до первого пробела) выводится только один раз.
    seen_commands = set()
    for row in commands:
        cmd = row.split(" — ", 1)[0].strip().casefold()
        if cmd in seen_commands:
            continue
        seen_commands.add(cmd)
        lines.append(row)
    lines.extend(["", "Нажмите нужную кнопку ниже. Полное описание — «📘 Инструкция»."])
    return "\n".join(lines)

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
    if not DOZVON_TASK_POOL.submit(f"{source_chat_id}:{target_chat_id}", _run_dozvon_session, session_key):
        send_and_auto_delete(source_chat_id, "⛔ Очередь дозвона переполнена.", 12)


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
_telegram_global_rate_lock = threading.RLock()
_telegram_global_last_ts = 0.0
try:
    TELEGRAM_GLOBAL_MIN_GAP = max(0.01, float(os.getenv("TELEGRAM_GLOBAL_MIN_GAP", "0.04") or "0.04"))
except Exception:
    TELEGRAM_GLOBAL_MIN_GAP = 0.04


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


def is_telegram_429(err: Exception) -> bool:
    """True если Telegram ограничил частоту. Для UI такие ошибки нельзя держать sleep-ом."""
    try:
        return _telegram_retry_after_seconds(err) is not None
    except Exception:
        return "too many requests" in str(err or "").lower()


def _is_fast_ui_purpose(purpose: str) -> bool:
    p = str(purpose or "").lower()
    fast_marks = (
        "safe_edit", "countdown", "secret_window", "secret media",
        "secret_edit_debounce", "category_wait_countdown", "process_trace_edit",
        "o9_secret_wait_countdown",
    )
    return any(x in p for x in fast_marks)


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


def _telegram_rate_limit_global():
    """Общий лимитер Telegram API для всех чатов, чтобы не ловить шквал 429."""
    global _telegram_global_last_ts
    with _telegram_global_rate_lock:
        now_ts = time.time()
        wait = TELEGRAM_GLOBAL_MIN_GAP - (now_ts - _telegram_global_last_ts)
        if wait > 0:
            time.sleep(wait)
        _telegram_global_last_ts = time.time()


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
            _telegram_rate_limit_global()
            if chat_id is not None:
                # UI-кнопки уже имеют собственный debounce. Не добавляем к ним ещё 0.35 с ожидания.
                ui_gap = effective_fast_telegram_gap() if _is_fast_ui_purpose(purpose) else 0.35
                _telegram_rate_limit_chat(chat_id, min_gap=ui_gap)
            try:
                if verbose_telegram_journal_enabled():
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
            # UI-операции не должны держать кнопку 20–30 секунд.
            # Для них пропускаем редактирование и отдаём управление сразу.
            if _is_fast_ui_purpose(purpose):
                raise e
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
        f"🏦 Остаток: {format_chat_amount(chat_id, bal, True)}",
        callback_data="bp:open"
    ))
    return kb


def _cancel_timer(timer_map: dict, key, scheduler_key: str | None = None):
    timer_map.pop(key, None)
    if scheduler_key:
        try:
            DELAYED_SCHEDULER.cancel(scheduler_key)
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
    scheduler_key = f"balance-panel-collapse:{int(chat_id)}:{int(key)}"
    _cancel_timer(_balance_panel_collapse_timers, key, scheduler_key)
    deadline = DELAYED_SCHEDULER.schedule(scheduler_key, delay, _job)
    _balance_panel_collapse_timers[key] = deadline


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

    scheduler_key = f"balance-panel-refresh:{int(chat_id)}"
    _cancel_timer(_balance_panel_refresh_timers, chat_id, scheduler_key)
    deadline = DELAYED_SCHEDULER.schedule(scheduler_key, delay, _job)
    _balance_panel_refresh_timers[chat_id] = deadline


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
    store = get_chat_store(chat_id)
    daily = store.get("daily_records", {}) or {}
    mode = currency_mode(chat_id)
    if mode != "ars":
        lines = ["Отчёт:"]
        running_balance = 0.0
        for dk in sorted(daily.keys()):
            recs = daily.get(dk, []) or []
            expense = sum(abs(float(r.get("amount", 0) or 0)) for r in recs if float(r.get("amount", 0) or 0) < 0)
            income = sum(float(r.get("amount", 0) or 0) for r in recs if float(r.get("amount", 0) or 0) >= 0)
            running_balance += sum(float(r.get("amount", 0) or 0) for r in recs)
            lines.append(
                f"{fmt_date_ddmmyy(dk)} | приход {format_chat_amount(chat_id, income, True)} | "
                f"расход {format_chat_amount(chat_id, -expense, True)} | ост {format_chat_amount(chat_id, running_balance, True)}"
            )
        return lines

    lines = ["Отчёт:"]
    lines.append(
        f"{'Дата':<8}|"
        f"{report_header_cell('Приход', 7)}|"
        f"{report_header_cell('Расход', 7)}|"
        f"{report_header_cell('Остаток', 7)}"
    )
    running_balance = 0.0
    for dk in sorted(daily.keys()):
        recs = daily.get(dk, []) or []
        expense = sum(abs(float(r.get("amount", 0) or 0)) for r in recs if float(r.get("amount", 0) or 0) < 0)
        income = sum(float(r.get("amount", 0) or 0) for r in recs if float(r.get("amount", 0) or 0) >= 0)
        running_balance += sum(float(r.get("amount", 0) or 0) for r in recs)
        lines.append(
            f"{fmt_date_ddmmyy(dk):<8}|{report_cell(income, 7)}|{report_cell(expense, 7)}|{report_cell(running_balance, 7)}"
        )
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
    """Атомарная запись: читатель никогда не видит половину JSON."""
    tmp_path = str(path) + f".tmp.{threading.get_ident()}.{time.time_ns()}"
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)
            f.flush()
            try:
                os.fsync(f.fileno())
            except Exception:
                pass
        os.replace(tmp_path, path)
    except Exception as e:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass
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
    return ["mega-login", "mega-whoami", "mega-mkdir", "mega-put", "mega-get", "mega-rm", "mega-mv", "mega-find"]


def mega_missing_commands():
    return [cmd for cmd in _mega_required_commands() if shutil.which(cmd) is None]


def _mega_run(cmd: str, args=None, timeout: int | None = None, check: bool = True):
    """Один MEGAcmd вызов за раз: на Render 512MB параллельные mega-* давали пики памяти."""
    args = list(args or [])
    exe = shutil.which(cmd)
    if not exe:
        raise RuntimeError(f"MEGAcmd command not found: {cmd}")
    with MEGA_COMMAND_LOCK:
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


def _mega_remote_missing_error(raw: str) -> bool:
    txt = str(raw or "").casefold()
    return any(x in txt for x in ("couldn't find", "not found", "no such file", "does not exist"))


def _mega_find_remote_files(remote_dir: str, pattern: str, limit: int | None = None) -> list[str]:
    """Список удалённых файлов MEGA. Имена v90 содержат sortable timestamp."""
    if not mega_is_configured() or shutil.which("mega-find") is None:
        return []
    try:
        res = _mega_run(
            "mega-find",
            [str(remote_dir), f"--pattern={pattern}", "--type=f"],
            check=False,
            timeout=60,
        )
        rows = sorted({x.strip() for x in (res.stdout or "").splitlines() if x.strip()}, reverse=True)
        return rows[: int(limit)] if limit else rows
    except Exception as e:
        log_error(f"_mega_find_remote_files({remote_dir},{pattern}): {e}")
        return []


def _mega_prune_remote_history(remote_dir: str, pattern: str, keep: int) -> int:
    """Удаляет только лишние СТАРЫЕ исторические копии. Активный файл не затрагивается."""
    rows = _mega_find_remote_files(remote_dir, pattern)
    removed = 0
    for remote_path in rows[max(1, int(keep)):]:
        try:
            res = _mega_run("mega-rm", [remote_path], check=False, timeout=30)
            if res.returncode == 0:
                removed += 1
        except Exception:
            pass
    return removed


def mega_put_replace(local_path: str, remote_dir: str, remote_name: str | None = None) -> bool:
    """Безопасно обновляет файл в MEGA без схемы rm->put.

    1) новый файл целиком загружается как уникальный candidate;
    2) прежний активный файл переносится в history;
    3) candidate одним move становится активным;
    4) хранится ограниченное число предыдущих версий.

    Если процесс упадёт на шаге 1, старый файл не тронут. Если на шаге 2/3 —
    старый уже находится в history, а candidate остаётся в MEGA.
    """
    if not mega_is_configured() or not local_path or not os.path.exists(local_path):
        return False
    candidate_local = None
    try:
        mega_ensure_remote_path(remote_dir)
        final_name = str(remote_name or os.path.basename(local_path))
        stem, ext = os.path.splitext(final_name)
        stamp = now_local().strftime("%Y%m%d_%H%M%S_%f")
        candidate_name = f"candidate_{mega_safe_name(stem, 'file')}_{stamp}{ext or '.json'}"
        candidate_local = _copy_file_for_mega(local_path, candidate_name)
        if not candidate_local:
            return False

        # Сначала candidate. Активный remote_file пока существует без изменений.
        _mega_run("mega-put", [candidate_local, remote_dir], check=True, timeout=MEGA_TIMEOUT)
        remote_candidate = remote_dir.rstrip("/") + "/" + candidate_name
        remote_file = remote_dir.rstrip("/") + "/" + final_name

        history_dir = remote_dir.rstrip("/") + "/history"
        mega_ensure_remote_path(history_dir)
        archive_name = f"{mega_safe_name(stem, 'file')}__{stamp}{ext or '.json'}"
        remote_archive = history_dir.rstrip("/") + "/" + archive_name

        # Отсутствие старого файла нормально. Любая другая ошибка оставляет старый файл на месте
        # и не пытается насильно его удалить.
        mv_old = _mega_run("mega-mv", [remote_file, remote_archive], check=False, timeout=60)
        if mv_old.returncode != 0:
            err = (mv_old.stderr or mv_old.stdout or "")[:500]
            if not _mega_remote_missing_error(err):
                log_error(f"[MEGA SAFE REPLACE] archive blocked for {remote_file}: {err}")
                return False

        _mega_run("mega-mv", [remote_candidate, remote_file], check=True, timeout=60)
        try:
            _mega_prune_remote_history(history_dir, f"{mega_safe_name(stem, 'file')}__*{ext or '.json'}", MEGA_FILE_HISTORY_KEEP)
        except Exception:
            pass
        return True
    except Exception as e:
        log_error(f"[MEGA SAFE REPLACE ERROR] {local_path} -> {remote_dir}: {e}")
        return False
    finally:
        try:
            if candidate_local and os.path.exists(candidate_local):
                os.remove(candidate_local)
        except Exception:
            pass

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
        prev_day = None
        for r in payload.get("records", []):
            day_key = str(r.get("day_key") or "")[:10]
            if prev_day is not None and day_key and day_key != prev_day:
                w.writerow([])
            w.writerow([
                fmt_date_table(day_key),
                r.get("amount"),
                r.get("note", ""),
                r.get("id", ""),
                r.get("short_id", ""),
                r.get("timestamp", ""),
                r.get("owner", ""),
            ])
            if day_key:
                prev_day = day_key

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
    prev_day = None
    for r in payload.get("records", []):
        day_key = str(r.get("day_key") or "")[:10]
        if prev_day is not None and day_key and day_key != prev_day:
            rows.append([])
        base_row = _xlsx_record_row(fmt_date_table(day_key), r.get("amount"), r.get("note", ""))
        rows.append(base_row + [
            r.get("id", ""),
            r.get("short_id", ""),
            r.get("timestamp", ""),
            r.get("owner", ""),
        ])
        if day_key:
            prev_day = day_key
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


def mega_upload_chat_latest_json_only(chat_id: int) -> bool:
    """Быстрый MEGA JSON без Excel/CSV и месячного пакета."""
    if not is_backup_to_mega_enabled(chat_id) or not mega_is_configured():
        return False
    try:
        local_path = chat_json_file(chat_id)
        if not os.path.exists(local_path):
            local_path = save_chat_json_only(chat_id)
        if not local_path:
            return False
        slug = mega_chat_slug(chat_id)
        remote_chat_dir = mega_remote_chat_dir(chat_id)
        return bool(mega_put_replace(local_path, remote_chat_dir, f"latest_{slug}.json"))
    except Exception as e:
        log_error(f"mega_upload_chat_latest_json_only({chat_id}): {e}")
        return False


def schedule_config_backup_for_chats(*chat_ids, delay: float = 3.0):
    """После изменения настроек/пересылки обновляем JSON/канал/MEGA с мягким debounce.

    Не ставим мгновенный бэкап после каждого клика/секрета: это разгружает Telegram API
    и не влияет на сохранность, потому что операции всё равно уже записаны в SQLite/data.
    """
    try:
        delay = max(float(delay or 0), BACKUP_MIN_DELAY_SECONDS)
    except Exception:
        delay = BACKUP_MIN_DELAY_SECONDS
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



# ─────────────────────────────────────────────────────────────
# v90: append-only delta journal + редкие full snapshots
# ─────────────────────────────────────────────────────────────
_delta_state_lock = threading.RLock()
_delta_record_baseline: dict[int, dict[str, str]] = {}
_delta_meta_baseline: dict[int, dict[str, str]] = {}
_delta_root_baseline: dict[str, str] = {}
_delta_pending_chats: set[int] = set()
_delta_chat_generation: dict[int, int] = defaultdict(int)
_delta_generation = 0
_delta_batch_timer = None
_delta_last_success_at = ""
_delta_last_file = ""
_delta_last_event_count = 0
_delta_last_error = ""
_global_snapshot_pending = False
_global_snapshot_last_success_monotonic = time.monotonic()
_global_snapshot_last_success_at = ""
_global_snapshot_last_change_monotonic = 0.0
_global_snapshot_capture_generation = 0

_DELTA_VOLATILE_CHAT_KEYS = {
    "active_windows", "edit_wait", "edit_target", "categories_msg_id", "report_window_id",
    "info_msg_id", "command_window_id", "total_msg_id", "balance_panel_id", "secret_wait",
    "main_window_msg_count", "balance_panel_msg_count", "current_view_day",
}
_DELTA_VOLATILE_ROOT_KEYS = {"chats", "records", "active_messages", "bot_errors", "_state_meta"}
_DELTA_ROOT_MAP_KEYS = {"forward_index", "forward_rules", "forward_finance", "finance_active_chats", "_global_settings", "csv_meta", "chat_backup_meta", "backup_flags"}


def mega_delta_remote_root() -> str:
    return f"{MEGA_BACKUP_DIR.rstrip('/')}/{MEGA_DELTA_BACKUP_DIR}"


def mega_delta_remote_day_dir(day_key: str | None = None) -> str:
    return mega_delta_remote_root().rstrip("/") + "/" + str(day_key or today_key())


def _delta_json_clone(value):
    return json.loads(json.dumps(value, ensure_ascii=False, default=str))


def _delta_hash(value) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _delta_record_key(rec: dict) -> str:
    if not isinstance(rec, dict):
        return "invalid:" + _delta_hash(rec)[:16]
    for key in ("id", "record_id"):
        value = rec.get(key)
        if value not in (None, ""):
            return f"id:{value}"
    return "src:%s:%s:%s" % (
        rec.get("source_chat_id") or rec.get("chat_id") or "",
        rec.get("source_msg_id") or rec.get("origin_msg_id") or rec.get("msg_id") or "",
        rec.get("timestamp") or rec.get("day_key") or "",
    )


def _delta_chat_meta(store: dict) -> dict:
    return {
        str(k): _delta_json_clone(v)
        for k, v in (store or {}).items()
        if k not in _DELTA_VOLATILE_CHAT_KEYS and k not in {"records", "daily_records", "daily_records_by_date"}
    }


def _delta_root_patch(payload: dict) -> dict:
    return {
        str(k): _delta_json_clone(v)
        for k, v in (payload or {}).items()
        if k not in _DELTA_VOLATILE_ROOT_KEYS
        and k not in {"_universal_backup", "_backup_meta", "_runtime_snapshot", "_delta_restore_meta"}
    }


def _delta_root_signature_state(root: dict) -> dict:
    out = {}
    for key, value in (root or {}).items():
        if key in _DELTA_ROOT_MAP_KEYS and isinstance(value, dict):
            out[str(key)] = {
                "kind": "map",
                "entries": {str(entry): _delta_hash(entry_value) for entry, entry_value in value.items()},
            }
        else:
            out[str(key)] = {"kind": "value", "hash": _delta_hash(value)}
    return out


def _delta_baseline_from_payload(payload: dict) -> tuple[dict[int, dict[str, str]], dict[int, dict[str, str]], dict[str, str]]:
    rec_baseline: dict[int, dict[str, str]] = {}
    meta_baseline: dict[int, dict[str, str]] = {}
    for cid_s, store in ((payload or {}).get("chats", {}) or {}).items():
        try:
            cid = int(cid_s)
        except Exception:
            continue
        if not isinstance(store, dict):
            continue
        rec_baseline[cid] = {
            _delta_record_key(rec): _delta_hash(rec)
            for rec in (store.get("records", []) or [])
            if isinstance(rec, dict)
        }
        meta = _delta_chat_meta(store)
        meta_baseline[cid] = {str(key): _delta_hash(value) for key, value in meta.items()}
    root = _delta_root_patch(payload or {})
    root_baseline = _delta_root_signature_state(root)
    return rec_baseline, meta_baseline, root_baseline

def initialize_delta_baseline(payload: dict | None = None):
    """Начальная точка delta. Ничего не загружает и не создаёт бэкап."""
    global _delta_record_baseline, _delta_meta_baseline, _delta_root_baseline
    snapshot = payload
    if snapshot is None:
        with data_lock:
            _persist_forward_index_in_data(data)
            snapshot = _delta_json_clone(data or {})
    recs, metas, root_sig = _delta_baseline_from_payload(snapshot or {})
    with _delta_state_lock:
        _delta_record_baseline = recs
        _delta_meta_baseline = metas
        _delta_root_baseline = dict(root_sig or {})


def _build_delta_payload(chat_ids: list[int], generation_map: dict[int, int]) -> tuple[dict | None, dict]:
    """Строит только изменившиеся записи и поля настроек относительно подтверждённого delta/full."""
    requested_ids = sorted({int(x) for x in chat_ids})
    with data_lock:
        _persist_forward_index_in_data(data)
        # Не копируем все чаты для маленького delta: только root и реально изменившиеся чаты.
        state = {
            str(key): _delta_json_clone(value)
            for key, value in (data or {}).items()
            if key != "chats"
        }
        all_chats = (data or {}).get("chats", {}) or {}
        state["chats"] = {
            str(cid): _delta_json_clone(all_chats.get(str(cid), {}) or {})
            for cid in requested_ids
        }

    with _delta_state_lock:
        old_records = {int(cid): dict(sigs or {}) for cid, sigs in _delta_record_baseline.items()}
        old_meta = {int(cid): dict(sigs or {}) for cid, sigs in _delta_meta_baseline.items()}
        old_root = dict(_delta_root_baseline or {})

    chat_changes = {}
    next_record_sigs = {}
    next_meta_sigs = {}
    event_count = 0
    chats = state.get("chats", {}) or {}
    for cid in requested_ids:
        store = chats.get(str(cid)) or {}
        if not isinstance(store, dict):
            continue
        current_records = {
            _delta_record_key(rec): rec
            for rec in (store.get("records", []) or [])
            if isinstance(rec, dict)
        }
        current_sigs = {key: _delta_hash(rec) for key, rec in current_records.items()}
        previous_sigs = old_records.get(cid, {}) or {}
        upsert_keys = [key for key, sig in current_sigs.items() if previous_sigs.get(key) != sig]
        delete_keys = [key for key in previous_sigs if key not in current_sigs]

        meta = _delta_chat_meta(store)
        current_meta_sigs = {str(key): _delta_hash(value) for key, value in meta.items()}
        previous_meta_sigs = old_meta.get(cid, {}) or {}
        changed_meta_keys = [key for key, sig in current_meta_sigs.items() if previous_meta_sigs.get(key) != sig]
        deleted_meta_keys = [key for key in previous_meta_sigs if key not in current_meta_sigs]

        if upsert_keys or delete_keys or changed_meta_keys or deleted_meta_keys:
            row = {
                "chat_id": cid,
                "upserts": [{"key": key, "record": current_records[key]} for key in upsert_keys],
                "deletes": delete_keys,
                "chat_meta_patch": {key: meta[key] for key in changed_meta_keys},
                "chat_meta_deletes": deleted_meta_keys,
            }
            chat_changes[str(cid)] = row
            event_count += len(upsert_keys) + len(delete_keys) + len(changed_meta_keys) + len(deleted_meta_keys)
        next_record_sigs[cid] = current_sigs
        next_meta_sigs[cid] = current_meta_sigs

    root = _delta_root_patch(state)
    current_root_sigs = _delta_root_signature_state(root)
    root_patch = {}
    root_deletes = []
    root_map_patches = {}
    root_map_deletes = {}
    for key, sig_state in current_root_sigs.items():
        old_state = old_root.get(key) or {}
        if sig_state.get("kind") == "map":
            current_entries = sig_state.get("entries") or {}
            old_entries = old_state.get("entries") or {} if old_state.get("kind") == "map" else {}
            changed_entries = [entry for entry, sig in current_entries.items() if old_entries.get(entry) != sig]
            deleted_entries = [entry for entry in old_entries if entry not in current_entries]
            if changed_entries:
                root_map_patches[key] = {entry: root[key][entry] for entry in changed_entries}
            if deleted_entries:
                root_map_deletes[key] = deleted_entries
            event_count += len(changed_entries) + len(deleted_entries)
        elif old_state != sig_state:
            root_patch[key] = root[key]
            event_count += 1
    for key in old_root:
        if key not in current_root_sigs:
            root_deletes.append(key)
            event_count += 1

    baseline = {
        "record_sigs": next_record_sigs,
        "meta_sigs": next_meta_sigs,
        "root_sigs": current_root_sigs,
        "generation_map": generation_map,
    }
    if event_count <= 0:
        return None, baseline

    created_at = now_local().isoformat(timespec="microseconds")
    seq = time.time_ns()
    payload = {
        "kind": "telegram_finance_bot_delta",
        "schema_version": 1,
        "bot_version": VERSION,
        "created_at": created_at,
        "delta_id": f"{now_local().strftime('%Y%m%d_%H%M%S_%f')}_{seq}",
        "chat_changes": chat_changes,
        "root_patch": root_patch,
        "root_deletes": root_deletes,
        "root_map_patches": root_map_patches,
        "root_map_deletes": root_map_deletes,
        "event_count": event_count,
        "chat_count": len(chat_changes),
    }
    return payload, baseline

def _commit_delta_baseline(baseline: dict):
    global _delta_root_baseline
    with _delta_state_lock:
        for cid, sigs in (baseline.get("record_sigs") or {}).items():
            _delta_record_baseline[int(cid)] = dict(sigs or {})
        for cid, sigs in (baseline.get("meta_sigs") or {}).items():
            _delta_meta_baseline[int(cid)] = dict(sigs or {})
        if "root_sigs" in baseline:
            _delta_root_baseline = dict(baseline.get("root_sigs") or {})

def _delta_upload_payload(payload: dict) -> tuple[bool, str]:
    if not payload or not mega_is_configured():
        return False, ""
    day_dir = mega_delta_remote_day_dir(str(payload.get("created_at") or today_key())[:10])
    os.makedirs(MEGA_LOCAL_TMP_DIR, exist_ok=True)
    name = f"delta_{payload.get('delta_id')}.json"
    local_path = os.path.join(MEGA_LOCAL_TMP_DIR, name)
    try:
        _save_json(local_path, payload)
        mega_ensure_remote_path(day_dir)
        # Delta immutable: уникальное имя, старые файлы не удаляем и не заменяем.
        _mega_run("mega-put", [local_path, day_dir], check=True, timeout=MEGA_TIMEOUT)
        return True, day_dir.rstrip("/") + "/" + name
    except Exception as e:
        log_error(f"[MEGA DELTA ERROR] {e}")
        return False, ""
    finally:
        try:
            if os.path.exists(local_path):
                os.remove(local_path)
        except Exception:
            pass


def _mark_global_snapshot_pending():
    """Full global: после 3 минут тишины, но максимум через 15 минут непрерывной работы."""
    global _global_snapshot_pending, _global_snapshot_last_change_monotonic
    if RESTORE_GUARD_ACTIVE or not mega_is_configured():
        return
    now_mono = time.monotonic()
    with _delta_state_lock:
        _global_snapshot_pending = True
        _global_snapshot_last_change_monotonic = now_mono
        elapsed = max(0.0, now_mono - _global_snapshot_last_success_monotonic)
        max_wait = max(5.0, MEGA_GLOBAL_MAX_INTERVAL_SECONDS - elapsed)

    def _quiet_fire():
        with _delta_state_lock:
            quiet_for = time.monotonic() - _global_snapshot_last_change_monotonic
            pending = _global_snapshot_pending
        if not pending:
            return
        if quiet_for + 0.5 < MEGA_GLOBAL_QUIET_SECONDS:
            DELAYED_SCHEDULER.schedule("mega-global-quiet-v90", MEGA_GLOBAL_QUIET_SECONDS - quiet_for, _quiet_fire)
            return
        _submit_global_snapshot_v90("quiet")

    def _max_fire():
        with _delta_state_lock:
            pending = _global_snapshot_pending
        if pending:
            _submit_global_snapshot_v90("max_interval")

    DELAYED_SCHEDULER.cancel("mega-global-quiet-v90")
    DELAYED_SCHEDULER.schedule("mega-global-quiet-v90", MEGA_GLOBAL_QUIET_SECONDS, _quiet_fire)
    if DELAYED_SCHEDULER.deadline("mega-global-max-v90") is None:
        DELAYED_SCHEDULER.schedule("mega-global-max-v90", max_wait, _max_fire)


def _submit_global_snapshot_v90(reason: str):
    if RESTORE_GUARD_ACTIVE:
        return
    def _job():
        ok = mega_upload_latest_global_backup()
        if not ok:
            DELAYED_SCHEDULER.schedule("mega-global-retry-v90", BACKUP_BUSY_RETRY_SECONDS, _submit_global_snapshot_v90, "retry")
    if not BACKUP_TASK_POOL.submit("mega-global-v90", _job):
        log_error(f"GLOBAL v90 QUEUE FULL ({reason}), RETRY")
        DELAYED_SCHEDULER.schedule("mega-global-retry-v90", BACKUP_BUSY_RETRY_SECONDS, _submit_global_snapshot_v90, "queue_retry")


def _run_delta_batch():
    global _delta_last_success_at, _delta_last_file, _delta_last_event_count, _delta_last_error
    with _delta_state_lock:
        chat_ids = sorted(_delta_pending_chats)
        generation_map = {cid: int(_delta_chat_generation.get(cid, 0)) for cid in chat_ids}
    if not chat_ids:
        return True
    payload, baseline = _build_delta_payload(chat_ids, generation_map)

    if payload is not None:
        ok, remote_path = _delta_upload_payload(payload)
        if not ok:
            _delta_last_error = "delta upload failed"
            return False
        _delta_last_success_at = str(payload.get("created_at") or "")
        _delta_last_file = remote_path
        _delta_last_event_count = int(payload.get("event_count", 0) or 0)
        _delta_last_error = ""
        log_info(f"[MEGA DELTA] uploaded {remote_path}; events={_delta_last_event_count}; chats={payload.get('chat_count')}")
        _mark_global_snapshot_pending()

    _commit_delta_baseline(baseline)
    with _delta_state_lock:
        for cid, gen in generation_map.items():
            if int(_delta_chat_generation.get(cid, 0)) == int(gen):
                _delta_pending_chats.discard(cid)
        more_pending = bool(_delta_pending_chats)
    with timer_lock:
        for cid in generation_map:
            _quick_backup_timers.pop(int(cid), None)
            _quick_backup_dirty_chats.discard(int(cid))
    if more_pending:
        schedule_delta_backup(None, delay=1.0, reason="changes_during_upload")
    return True

def schedule_delta_backup(chat_id: int | None, delay: float | None = None, reason: str = "change"):
    """Общий debounce разных чатов: несколько изменений попадают в один маленький delta."""
    global _delta_generation, _delta_batch_timer
    if RESTORE_GUARD_ACTIVE or not mega_is_configured():
        return False
    with _delta_state_lock:
        _delta_generation += 1
        if chat_id is not None:
            cid = int(chat_id)
            _delta_pending_chats.add(cid)
            _delta_chat_generation[cid] = _delta_generation
        elif not _delta_pending_chats:
            return False
    if delay is None:
        delay = MEGA_DELTA_PRIORITY_DELAY_SECONDS if mega_backup_priority_enabled() else MEGA_DELTA_DELAY_SECONDS
    delay = max(0.5, float(delay))

    def _fire():
        def _job():
            if not _run_delta_batch():
                schedule_delta_backup(None, delay=BACKUP_BUSY_RETRY_SECONDS, reason="upload_retry")
        if not DELTA_TASK_POOL.submit("mega-delta-v90", _job):
            log_error("DELTA QUEUE FULL, RETRY")
            schedule_delta_backup(None, delay=BACKUP_BUSY_RETRY_SECONDS, reason="queue_retry")

    DELAYED_SCHEDULER.cancel("mega-delta-batch-v90")
    _delta_batch_timer = DELAYED_SCHEDULER.schedule("mega-delta-batch-v90", delay, _fire)
    return True


def _apply_delta_payload_to_state(state: dict, delta: dict) -> dict:
    if not isinstance(state, dict) or not isinstance(delta, dict):
        return state
    root_patch = delta.get("root_patch") or {}
    for key in (delta.get("root_deletes") or []):
        if key not in _DELTA_VOLATILE_ROOT_KEYS:
            state.pop(str(key), None)
    for key, value in root_patch.items():
        if key not in _DELTA_VOLATILE_ROOT_KEYS:
            state[key] = _delta_json_clone(value)
    for key, entries in (delta.get("root_map_patches") or {}).items():
        target = state.setdefault(str(key), {})
        if not isinstance(target, dict):
            target = {}
            state[str(key)] = target
        for entry, value in (entries or {}).items():
            target[str(entry)] = _delta_json_clone(value)
    for key, entries in (delta.get("root_map_deletes") or {}).items():
        target = state.get(str(key))
        if isinstance(target, dict):
            for entry in entries or []:
                target.pop(str(entry), None)
    chats = state.setdefault("chats", {})
    for cid_s, change in (delta.get("chat_changes") or {}).items():
        if not isinstance(change, dict):
            continue
        store = chats.setdefault(str(cid_s), {})
        # Поддержка первых тестовых delta с chat_meta и основной field-patch формат v90.
        meta = change.get("chat_meta")
        if isinstance(meta, dict):
            for key, value in meta.items():
                store[key] = _delta_json_clone(value)
        for key in (change.get("chat_meta_deletes") or []):
            store.pop(str(key), None)
        for key, value in (change.get("chat_meta_patch") or {}).items():
            store[str(key)] = _delta_json_clone(value)
        current = {
            _delta_record_key(rec): rec
            for rec in (store.get("records", []) or [])
            if isinstance(rec, dict)
        }
        for key in (change.get("deletes") or []):
            current.pop(str(key), None)
        for item in (change.get("upserts") or []):
            if not isinstance(item, dict) or not isinstance(item.get("record"), dict):
                continue
            current[str(item.get("key") or _delta_record_key(item["record"]))] = _delta_json_clone(item["record"])
        records = sorted(current.values(), key=record_sort_key)
        daily = defaultdict(list)
        for rec in records:
            dk = _record_day_key(rec)
            rec["day_key"] = dk
            daily[dk].append(rec)
        store["records"] = records
        store["daily_records"] = {dk: sorted(rows, key=record_sort_key) for dk, rows in sorted(daily.items())}
        store["balance"] = sum(float(rec.get("amount", 0) or 0) for rec in records)
    state["overall_balance"] = sum(float((s or {}).get("balance", 0) or 0) for s in chats.values() if isinstance(s, dict))
    state["records"] = []
    state["_delta_restore_meta"] = {
        "last_delta_id": delta.get("delta_id"),
        "last_delta_created_at": delta.get("created_at"),
        "last_delta_event_count": delta.get("event_count"),
    }
    return state


def _delta_remote_candidates_after(created_at: str, limit: int | None = None) -> list[str]:
    rows = _mega_find_remote_files(mega_delta_remote_root(), "delta_*.json")
    base_ts = _parse_iso_timestamp(created_at)
    selected = []
    for path in sorted(rows):
        name = os.path.basename(path)
        match = re.search(r"delta_(\d{8})_(\d{6})_(\d{6})_", name)
        if match:
            try:
                dt = datetime.strptime("".join(match.groups()), "%Y%m%d%H%M%S%f").replace(tzinfo=get_tz())
                if dt.timestamp() <= base_ts:
                    continue
            except Exception:
                pass
        selected.append(path)
    return selected[: int(limit or MEGA_DELTA_RESTORE_LIMIT)]


def merge_global_snapshot_with_mega_deltas(local_global_path: str) -> tuple[str, int]:
    """Скачивает и применяет immutable delta, созданные после full snapshot."""
    base = _load_json(local_global_path, {}) or {}
    if not _global_payload_is_structurally_valid(base):
        return local_global_path, 0
    created_at = str((base.get("_universal_backup") or {}).get("created_at") or (base.get("_backup_meta") or {}).get("created_at") or "")
    remote_rows = _delta_remote_candidates_after(created_at)
    applied = 0
    for remote_path in remote_rows:
        local_delta = _mega_download_remote_path(remote_path)
        if not local_delta:
            continue
        delta = _load_json(local_delta, {}) or {}
        if delta.get("kind") != "telegram_finance_bot_delta":
            continue
        if _parse_iso_timestamp(delta.get("created_at")) <= _parse_iso_timestamp(created_at):
            continue
        _apply_delta_payload_to_state(base, delta)
        applied += 1
    if not applied:
        return local_global_path, 0
    merged = os.path.join(MEGA_LOCAL_TMP_DIR, f"merged_global_with_{applied}_deltas.json")
    _save_json(merged, base)
    log_info(f"[MEGA RESTORE] merged full snapshot + {applied} delta files")
    return merged, applied


def _prune_delta_files_after_full_snapshot():
    try:
        rows = _mega_find_remote_files(mega_delta_remote_root(), "delta_*.json")
        for remote_path in rows[MEGA_DELTA_KEEP_FILES:]:
            _mega_run("mega-rm", [remote_path], check=False, timeout=30)
    except Exception as e:
        log_error(f"_prune_delta_files_after_full_snapshot: {e}")


def delta_status_text() -> str:
    with _delta_state_lock:
        pending = len(_delta_pending_chats)
        global_pending = _global_snapshot_pending
        since_full = max(0, int(time.monotonic() - _global_snapshot_last_success_monotonic))
    return (
        "🧩 Delta / snapshots v91\n"
        f"Ожидают чаты: {pending}\n"
        f"Последний delta: {_delta_last_success_at or '-'}\n"
        f"Событий в нём: {_delta_last_event_count}\n"
        f"Файл: {_delta_last_file or '-'}\n"
        f"Ошибка: {_delta_last_error or '-'}\n"
        f"Full global ожидается: {'да' if global_pending else 'нет'}\n"
        f"После последнего full: {since_full} сек.\n"
        f"Тишина для full: {int(MEGA_GLOBAL_QUIET_SECONDS)} сек.; максимум: {int(MEGA_GLOBAL_MAX_INTERVAL_SECONDS)} сек."
    )


def _snapshot_runtime_state_for_backup(payload: dict) -> dict:
    """Минимальный стабильный runtime-слой, необходимый для восстановления между версиями."""
    return {
        "backup_flags": json.loads(json.dumps(payload.get("backup_flags", {}) or {}, ensure_ascii=False, default=str)),
        "finance_active_chats": json.loads(json.dumps(payload.get("finance_active_chats", {}) or {}, ensure_ascii=False, default=str)),
        "forward_index": json.loads(json.dumps(payload.get("forward_index", {}) or {}, ensure_ascii=False, default=str)),
        "global_settings": json.loads(json.dumps(payload.get("_global_settings", {}) or {}, ensure_ascii=False, default=str)),
        "csv_meta": json.loads(json.dumps(payload.get("csv_meta", {}) or {}, ensure_ascii=False, default=str)),
        "chat_backup_meta": json.loads(json.dumps(payload.get("chat_backup_meta", {}) or {}, ensure_ascii=False, default=str)),
    }


def make_global_backup_payload() -> dict:
    """Универсальный полный JSON: данные, настройки и индекс старых пересланных сообщений."""
    with data_lock:
        # Важно снять актуальный forward_map ДО копирования. Иначе свежие связи старых/новых
        # сообщений могли отсутствовать в latest_global.json до срабатывания debounce.
        _persist_forward_index_in_data(data)
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

    created_at = now_local().isoformat(timespec="seconds")
    payload["_universal_backup"] = {
        "kind": UNIVERSAL_BACKUP_KIND,
        "schema_version": UNIVERSAL_BACKUP_SCHEMA_VERSION,
        "bot_version": VERSION,
        "created_at": created_at,
        "restore_mode": "replace_full_state",
        "contains": [
            "all_chats", "records", "settings", "global_settings", "forward_rules",
            "forward_finance", "forward_index", "secret_messages", "backup_metadata"
        ],
    }
    payload["_runtime_snapshot"] = _snapshot_runtime_state_for_backup(payload)
    payload["_backup_meta"] = {
        "kind": "mega_latest_global",
        "version": VERSION,
        "schema_version": UNIVERSAL_BACKUP_SCHEMA_VERSION,
        "created_at": created_at,
        "chat_count": len(payload.get("chats", {}) or {}),
        "finance_active_chats": payload.get("finance_active_chats", {}),
        "forward_rules_count": sum(len(v or {}) for v in (payload.get("forward_rules", {}) or {}).values()),
        "forward_finance_count": sum(len(v or {}) for v in (payload.get("forward_finance", {}) or {}).values()),
        "forward_index_count": len(payload.get("forward_index", {}) or {}),
        "note": "Универсальный полный JSON: все чаты, записи, настройки, секреты, пересылка и индекс сообщений.",
    }
    return payload


def save_global_backup_snapshot(path: str) -> str:
    """Атомарно создаёт локальный universal snapshot."""
    payload = make_global_backup_payload()
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        f.flush()
        try:
            os.fsync(f.fileno())
        except Exception:
            pass
    os.replace(tmp, path)
    return path


def _global_payload_stats(payload: dict, path: str | None = None) -> dict:
    chats = payload.get("chats", {}) if isinstance(payload, dict) else {}
    if not isinstance(chats, dict):
        chats = {}
    record_count = 0
    nonempty_chats = 0
    for store in chats.values():
        if not isinstance(store, dict):
            continue
        recs = store.get("records") or []
        if isinstance(recs, list):
            record_count += len(recs)
            if recs:
                nonempty_chats += 1
    try:
        size_bytes = os.path.getsize(path) if path and os.path.exists(path) else len(json.dumps(payload, ensure_ascii=False))
    except Exception:
        size_bytes = 0
    universal = payload.get("_universal_backup") or {}
    return {
        "size_bytes": int(size_bytes),
        "chat_count": len(chats),
        "nonempty_chats": nonempty_chats,
        "record_count": int(record_count),
        "schema_version": int(universal.get("schema_version") or 0),
        "created_at": str(universal.get("created_at") or (payload.get("_backup_meta") or {}).get("created_at") or ""),
        "is_universal": universal.get("kind") == UNIVERSAL_BACKUP_KIND,
    }


def _global_payload_is_structurally_valid(payload: dict) -> bool:
    if not isinstance(payload, dict):
        return False
    if not isinstance(payload.get("chats"), dict):
        return False
    universal = payload.get("_universal_backup") or {}
    return universal.get("kind") == UNIVERSAL_BACKUP_KIND or "_backup_meta" in payload


def _global_candidate_rejection(candidate: dict, current: dict | None = None) -> str:
    """Возвращает причину отказа, если кандидат похож на обнулённую/обрезанную базу."""
    if not candidate.get("is_universal"):
        return "candidate is not universal"
    if candidate.get("size_bytes", 0) < MEGA_GLOBAL_MIN_SAFE_BYTES and candidate.get("record_count", 0) == 0:
        return f"candidate too small/empty: {candidate.get('size_bytes')} bytes"
    if current:
        old_records = int(current.get("record_count", 0) or 0)
        new_records = int(candidate.get("record_count", 0) or 0)
        old_size = int(current.get("size_bytes", 0) or 0)
        new_size = int(candidate.get("size_bytes", 0) or 0)
        old_chats = int(current.get("chat_count", 0) or 0)
        new_chats = int(candidate.get("chat_count", 0) or 0)
        if old_records >= 10 and new_records < old_records * (1.0 - MEGA_GLOBAL_MAX_RECORD_DROP):
            return f"record drop blocked: {old_records} -> {new_records}"
        if old_size >= 100_000 and new_size < old_size * 0.50:
            return f"size drop blocked: {old_size} -> {new_size}"
        if old_chats >= 2 and new_chats < max(1, int(old_chats * 0.50)):
            return f"chat drop blocked: {old_chats} -> {new_chats}"
    return ""


def _set_restore_guard(reason: str):
    global RESTORE_GUARD_ACTIVE, RESTORE_GUARD_REASON
    RESTORE_GUARD_ACTIVE = True
    RESTORE_GUARD_REASON = str(reason or "restore not confirmed")[:1000]
    log_error(f"[RESTORE GUARD ON] {RESTORE_GUARD_REASON}")


def _clear_restore_guard():
    global RESTORE_GUARD_ACTIVE, RESTORE_GUARD_REASON
    RESTORE_GUARD_ACTIVE = False
    RESTORE_GUARD_REASON = ""


def mega_history_remote_dir() -> str:
    return f"{MEGA_BACKUP_DIR.rstrip('/')}/{MEGA_HISTORY_BACKUP_DIR}"


def mega_download_global_named(remote_name: str) -> str | None:
    if not mega_is_configured():
        return None
    try:
        mega_login_if_needed()
        restore_dir = tempfile.mkdtemp(prefix="mega_restore_")
        remote_file = mega_remote_file_path(remote_name)
        _mega_run("mega-get", [remote_file, restore_dir], check=True, timeout=MEGA_TIMEOUT)
        local_path = os.path.join(restore_dir, os.path.basename(remote_name))
        if not os.path.exists(local_path):
            for name in os.listdir(restore_dir):
                if name.lower().endswith(".json"):
                    local_path = os.path.join(restore_dir, name)
                    break
        return local_path if os.path.exists(local_path) else None
    except Exception as e:
        log_error(f"[MEGA RESTORE DOWNLOAD ERROR] {remote_name}: {e}")
        return None


def mega_download_latest_global_backup() -> str | None:
    return mega_download_global_named(MEGA_LATEST_GLOBAL_NAME)


def _mega_history_candidates(limit: int = 20) -> list[str]:
    """Возвращает последние immutable global snapshots из MEGA history."""
    exe = shutil.which("mega-find")
    if not exe or not mega_is_configured():
        return []
    try:
        mega_ensure_remote_path(mega_history_remote_dir())
        res = _mega_run(
            "mega-find",
            [mega_history_remote_dir(), "--pattern=global_*.json", "--type=f"],
            check=False,
            timeout=60,
        )
        rows = [x.strip() for x in (res.stdout or "").splitlines() if x.strip().lower().endswith(".json")]
        return sorted(set(rows), reverse=True)[:max(1, int(limit))]
    except Exception as e:
        log_error(f"_mega_history_candidates: {e}")
        return []


def _mega_download_remote_path(remote_path: str) -> str | None:
    try:
        restore_dir = tempfile.mkdtemp(prefix="mega_history_restore_")
        _mega_run("mega-get", [remote_path, restore_dir], check=True, timeout=MEGA_TIMEOUT)
        base = os.path.basename(remote_path.rstrip("/"))
        local = os.path.join(restore_dir, base)
        if os.path.exists(local):
            return local
        for name in os.listdir(restore_dir):
            if name.lower().endswith(".json"):
                return os.path.join(restore_dir, name)
    except Exception as e:
        log_error(f"_mega_download_remote_path({remote_path}): {e}")
    return None


def mega_upload_latest_global_backup(force: bool = False) -> bool:
    """Безопасный latest_global: проверка усечения, история и замена без предварительного удаления."""
    if not mega_is_configured():
        return False
    if RESTORE_GUARD_ACTIVE and not force:
        log_error(f"[MEGA BACKUP BLOCKED BY RESTORE GUARD] {RESTORE_GUARD_REASON}")
        return False
    with MEGA_GLOBAL_BACKUP_LOCK:
        candidate_path = None
        try:
            with _delta_state_lock:
                snapshot_capture_generation = int(_delta_generation)
            os.makedirs(MEGA_LOCAL_TMP_DIR, exist_ok=True)
            stamp = now_local().strftime("%Y%m%d_%H%M%S_%f")
            candidate_name = f"candidate_global_{stamp}.json"
            candidate_path = os.path.join(MEGA_LOCAL_TMP_DIR, candidate_name)
            save_global_backup_snapshot(candidate_path)
            candidate_payload = _load_json(candidate_path, {}) or {}
            candidate_stats = _global_payload_stats(candidate_payload, candidate_path)

            current_path = mega_download_latest_global_backup()
            current_payload = _load_json(current_path, {}) if current_path else {}
            current_stats = _global_payload_stats(current_payload, current_path) if _global_payload_is_structurally_valid(current_payload) else None

            rejection = "" if force else _global_candidate_rejection(candidate_stats, current_stats)
            if rejection:
                _set_restore_guard("dangerous MEGA overwrite prevented: " + rejection)
                log_error(f"[MEGA GLOBAL REJECTED] candidate={candidate_stats} current={current_stats}")
                return False

            mega_ensure_remote_path(MEGA_BACKUP_DIR)
            mega_ensure_remote_path(mega_history_remote_dir())

            # Сначала загружаем кандидат под уникальным временным именем.
            _mega_run("mega-put", [candidate_path, MEGA_BACKUP_DIR], check=True, timeout=MEGA_TIMEOUT)
            remote_candidate = MEGA_BACKUP_DIR.rstrip("/") + "/" + candidate_name
            remote_latest = mega_remote_file_path(MEGA_LATEST_GLOBAL_NAME)

            # Старый latest не удаляем: переносим в историю. Даже если следующий шаг упадёт,
            # предыдущий полный файл останется доступен для autorestore.
            if current_path and current_stats:
                old_stamp = re.sub(r"[^0-9]", "", current_stats.get("created_at", ""))[:14] or stamp
                archived = mega_history_remote_dir().rstrip("/") + f"/global_{old_stamp}_{current_stats.get('record_count',0)}r_{stamp}.json"
                mv = _mega_run("mega-mv", [remote_latest, archived], check=False, timeout=60)
                if mv.returncode != 0:
                    log_error(f"[MEGA] could not archive previous latest: {(mv.stderr or mv.stdout or '')[:300]}")

            # Активируем новый latest одним move, без окна delete->put.
            _mega_run("mega-mv", [remote_candidate, remote_latest], check=True, timeout=60)
            # Полный снимок успешно активирован: фиксируем baseline именно из candidate,
            # а не из более нового live-state, который мог измениться во время загрузки.
            initialize_delta_baseline(candidate_payload)
            global _global_snapshot_pending, _global_snapshot_last_success_monotonic, _global_snapshot_last_success_at
            with _delta_state_lock:
                newer_changes_exist = int(_delta_generation) > int(snapshot_capture_generation)
                _global_snapshot_pending = bool(newer_changes_exist)
                _global_snapshot_last_success_monotonic = time.monotonic()
                _global_snapshot_last_success_at = now_local().isoformat(timespec="seconds")
            DELAYED_SCHEDULER.cancel("mega-global-max-v90")
            DELAYED_SCHEDULER.cancel("mega-global-quiet-v90")
            if newer_changes_exist:
                _mark_global_snapshot_pending()
            try:
                _mega_prune_remote_history(mega_history_remote_dir(), "global_*.json", MEGA_GLOBAL_HISTORY_KEEP)
                _prune_delta_files_after_full_snapshot()
            except Exception:
                pass
            log_info(f"[MEGA] guarded latest uploaded: {remote_latest}; stats={candidate_stats}")
            return True
        except Exception as e:
            log_error(f"[MEGA BACKUP ERROR] {e}")
            return False
        finally:
            try:
                import gc
                gc.collect()
            except Exception:
                pass


def is_data_effectively_empty_for_restore(d: dict) -> bool:
    """True, если база похожа на пустую после нового deploy/restart Render."""
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
        if store.get("secret_messages"):
            return False
    return True


def _parse_iso_timestamp(value: str | None) -> float:
    try:
        return datetime.fromisoformat(str(value or "").replace("Z", "+00:00")).timestamp()
    except Exception:
        return 0.0


def _local_restore_stats(d: dict) -> dict:
    """Статистика локальной базы без создания нового backup timestamp."""
    chats = (d or {}).get("chats", {}) if isinstance(d, dict) else {}
    if not isinstance(chats, dict):
        chats = {}
    records = 0
    nonempty = 0
    secrets = 0
    for store in chats.values():
        if not isinstance(store, dict):
            continue
        recs = store.get("records") or []
        if isinstance(recs, list):
            records += len(recs)
            if recs:
                nonempty += 1
        secrets += len(store.get("secret_messages") or []) if isinstance(store.get("secret_messages") or [], list) else 0
    return {
        "chat_count": len(chats),
        "nonempty_chats": nonempty,
        "record_count": records,
        "secret_count": secrets,
        "forward_rules_count": sum(len(v or {}) for v in ((d or {}).get("forward_rules", {}) or {}).values()),
        "forward_index_count": len((d or {}).get("forward_index", {}) or {}),
        "last_saved_at": str(((d or {}).get("_state_meta") or {}).get("last_saved_at") or ""),
    }


def _mega_discover_global_candidates(limit: int = 60) -> list[str]:
    """Ищет полноценные global JSON во всём каталоге MEGA, а не только exact latest/history.

    Это восстанавливает ситуацию, когда latest_global.json был временно перемещён в history,
    остался candidate_global_*.json после прерванной ротации или файл лежит глубже в каталоге.
    """
    if not mega_is_configured() or shutil.which("mega-find") is None:
        return []
    rows = []
    try:
        mega_login_if_needed()
        for pattern in (MEGA_LATEST_GLOBAL_NAME, "global_*.json", "candidate_global_*.json", "*global*.json"):
            res = _mega_run(
                "mega-find",
                [MEGA_BACKUP_DIR, f"--pattern={pattern}", "--type=f"],
                check=False,
                timeout=90,
            )
            rows.extend(x.strip() for x in (res.stdout or "").splitlines() if x.strip().lower().endswith(".json"))
    except Exception as e:
        log_error(f"_mega_discover_global_candidates: {e}")
    # latest первым, затем более новые имена; дубли убираем.
    latest_path = mega_remote_file_path(MEGA_LATEST_GLOBAL_NAME)
    uniq = []
    seen = set()
    for path in [latest_path] + sorted(set(rows), reverse=True):
        if path and path not in seen:
            seen.add(path)
            uniq.append(path)
    return uniq[:max(1, int(limit))]


def _mega_select_best_global_candidate(limit: int = 60) -> tuple[str | None, dict, str]:
    """Скачивает доступные global snapshots и выбирает лучший валидный полный снимок."""
    best_path = None
    best_stats = {}
    best_label = ""
    candidates = _mega_discover_global_candidates(limit=limit)
    # На старых установках mega-find может не вернуть exact latest — пробуем его напрямую.
    direct_latest = mega_download_latest_global_backup()
    local_candidates = []
    if direct_latest:
        local_candidates.append(("latest", direct_latest))
    for remote_path in candidates:
        if remote_path == mega_remote_file_path(MEGA_LATEST_GLOBAL_NAME) and direct_latest:
            continue
        local_candidates.append((remote_path, None))

    for label, local_path in local_candidates:
        try:
            if local_path is None:
                local_path = _mega_download_remote_path(label)
            if not local_path:
                continue
            payload = _load_json(local_path, {}) or {}
            if not _global_payload_is_structurally_valid(payload):
                log_error(f"[MEGA RESTORE] invalid global candidate: {label}")
                continue
            stats = _global_payload_stats(payload, local_path)
            if stats.get("record_count", 0) == 0 and not ALLOW_EMPTY_MEGA_RESTORE:
                continue
            score = (
                _parse_iso_timestamp(stats.get("created_at")),
                int(stats.get("record_count", 0) or 0),
                int(stats.get("chat_count", 0) or 0),
                int(stats.get("size_bytes", 0) or 0),
            )
            best_score = (
                _parse_iso_timestamp(best_stats.get("created_at")),
                int(best_stats.get("record_count", 0) or 0),
                int(best_stats.get("chat_count", 0) or 0),
                int(best_stats.get("size_bytes", 0) or 0),
            ) if best_stats else (-1, -1, -1, -1)
            if score > best_score:
                best_path, best_stats, best_label = local_path, stats, label
        except Exception as e:
            log_error(f"[MEGA RESTORE] candidate scan error {label}: {e}")
    return best_path, best_stats, best_label


def mega_restore_full_from_cloud(force: bool = False) -> tuple[bool, str]:
    """Полное восстановление из лучшего global snapshot + всех последующих delta.

    Восстанавливает весь state целиком: chats, records, settings, owners, forwarding,
    forward_index, secret_messages и прочие поля универсального backup.
    """
    global data
    if not mega_is_configured():
        return False, "MEGA не настроена"

    local_empty = is_data_effectively_empty_for_restore(data)
    local_stats = _local_restore_stats(data)
    base_path, base_stats, label = _mega_select_best_global_candidate(limit=80)
    if not base_path:
        if local_empty:
            _set_restore_guard("local database is empty; no valid full global snapshot found in MEGA")
        return False, "В MEGA не найден валидный полный global JSON"

    try:
        merged_path, applied_delta_count = merge_global_snapshot_with_mega_deltas(base_path)
        remote_payload = _load_json(merged_path, {}) or {}
        if not _global_payload_is_structurally_valid(remote_payload):
            return False, "Найденный global JSON повреждён после объединения delta"
        remote_stats = _global_payload_stats(remote_payload, merged_path)
        remote_stats["applied_deltas"] = applied_delta_count

        remote_created = str(remote_stats.get("created_at") or "")
        local_saved = str(local_stats.get("last_saved_at") or "")
        remote_newer = _parse_iso_timestamp(remote_created) > _parse_iso_timestamp(local_saved) + 1
        materially_richer = (
            int(remote_stats.get("record_count", 0) or 0) > int(local_stats.get("record_count", 0) or 0)
            or int(remote_stats.get("chat_count", 0) or 0) > int(local_stats.get("chat_count", 0) or 0)
        )
        local_suspicious = (
            local_empty
            or (int(local_stats.get("record_count", 0) or 0) == 0 and int(remote_stats.get("record_count", 0) or 0) > 0)
            or (int(local_stats.get("chat_count", 0) or 0) <= 1 and int(remote_stats.get("chat_count", 0) or 0) > 1)
        )

        if not force and not (local_suspicious or remote_newer or materially_richer):
            _clear_restore_guard()
            return False, f"Локальная база не хуже MEGA; восстановление не требуется. local={local_stats}, mega={remote_stats}"

        restore_chat_id = int(OWNER_ID) if OWNER_ID else 0
        restore_from_json(restore_chat_id, merged_path)
        _restore_runtime_state_from_data(data)
        initialize_delta_baseline(data)
        _clear_restore_guard()
        msg = (
            f"Полное восстановление OK из {label or 'MEGA'}: "
            f"чатов={remote_stats.get('chat_count', 0)}, записей={remote_stats.get('record_count', 0)}, "
            f"delta={applied_delta_count}"
        )
        log_info("[MEGA RESTORE FULL] " + msg)
        return True, msg
    except Exception as e:
        log_error(f"[MEGA RESTORE FULL ERROR] {e}")
        if local_empty:
            _set_restore_guard("MEGA full restore failed: " + str(e)[:500])
        return False, "Ошибка полного восстановления: " + str(e)[:500]


def mega_autorestore_if_needed() -> bool:
    """Надёжное авто-восстановление: всегда проверяет MEGA и умеет восстановить частичную/старую SQLite."""
    global data
    if not MEGA_AUTORESTORE or not mega_is_configured():
        if is_data_effectively_empty_for_restore(data):
            _set_restore_guard("local database is empty and MEGA autorestore is unavailable")
        return False
    ok, detail = mega_restore_full_from_cloud(force=False)
    log_info(f"[MEGA AUTORESTORE] ok={ok}; {detail}")
    return bool(ok)

def mega_status_text() -> str:
    lines = ["☁️ MEGA.nz / MEGAcmd"]
    lines.append(f"MEGA_ENABLED: {'ВКЛ' if MEGA_ENABLED else 'ВЫКЛ'}")
    lines.append(f"MEGA_AUTORESTORE: {'ВКЛ' if MEGA_AUTORESTORE else 'ВЫКЛ'}")
    lines.append(f"RESTORE_GUARD: {'ВКЛ — ' + RESTORE_GUARD_REASON if RESTORE_GUARD_ACTIVE else 'ВЫКЛ'}")
    lines.append(f"MEGA_HISTORY_DIR: {mega_history_remote_dir()}")
    lines.append(f"MEGA_DELTA_DIR: {mega_delta_remote_root()}")
    lines.append(f"Delta delay: {MEGA_DELTA_PRIORITY_DELAY_SECONDS if mega_backup_priority_enabled() else MEGA_DELTA_DELAY_SECONDS:g} сек")
    lines.append(f"Global full: после {int(MEGA_GLOBAL_QUIET_SECONDS)} сек. тишины / максимум {int(MEGA_GLOBAL_MAX_INTERVAL_SECONDS)} сек.")
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
def send_backup_to_chat(chat_id: int, ensure_files: bool = True) -> None:
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

        if ensure_files:
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
        "_global_settings": {"bot_journal_enabled": False, "bot_journal_verbose_process": False, "bot_journal_verbose_telegram": False, "buttons_current_window": False, "forward_menu_new_style": False, "icon_button_mode": True, "total_secret_mask_enabled": False, "finance_day_start_5am": False, "backup_excel_all_enabled": True, "mega_backup_priority": False, "bot_behavior_profile": "v87_current", "journal_default_off_v83_applied": True},
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
    if re.fullmatch(r"[✅❌] (?:Общий )?журнал(?: чата)? (?:ВКЛ|ВЫКЛ)", label, flags=re.IGNORECASE):
        return ("✅" if label.startswith("✅") else "❌") + " 📓"
    if re.fullmatch(r"[✅❌] Статьи-кнопки (?:ВКЛ|ВЫКЛ)", label):
        return ("✅" if label.startswith("✅") else "❌") + " 📚"
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
    if label in {"☁️ Сразу в MEGA", "🕓 MEGA как обычно"}:
        return "☁️⚡" if "Сразу" in label else "☁️🕓"
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

def save_data(d, chat_ids=None, full: bool = False, root_only: bool = False):
    """Потокобезопасное сохранение.

    В обработчике конкретного чата SQLite обновляет только этот чат. Полный
    проход по всем чатам выполняется при старте, восстановлении и глобальном
    бэкапе. Это убирает квадратичную нагрузку при 100 активных чатах.
    """
    with data_lock:
        d.setdefault("_state_meta", {})["last_saved_at"] = now_local().isoformat(timespec="seconds")
        d["_state_meta"]["bot_version"] = VERSION
        fac = {str(cid): True for cid in list(finance_active_chats)}
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
        if root_only:
            return

        ids = set()
        if chat_ids is not None:
            if isinstance(chat_ids, (list, tuple, set)):
                source_ids = chat_ids
            else:
                source_ids = [chat_ids]
            for cid in source_ids:
                try:
                    ids.add(int(cid))
                except Exception:
                    pass
        elif not full:
            cid = current_state_chat_id()
            if cid is not None:
                try:
                    ids.add(int(cid))
                except Exception:
                    pass

        chats = d.get("chats", {}) or {}
        if ids and not full:
            for cid in ids:
                payload = chats.get(str(cid))
                if isinstance(payload, dict):
                    SQLITE.save_chat(cid, payload)
        else:
            SQLITE.save_chats(chats)
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
                    "auto_backup_to_mega_enabled": True,
                    "journal_enabled": False,
                    "main_article_buttons_enabled": False,
                    "main_financial_value_buttons_enabled": False,
                    "gomonk_enabled": False,
                    "gomonk_entries": [],
                    "remaining_with_gomonk": True,
                    "usd_display_enabled": False,
                    "currency_mode": "ars",
                    "remaining_show_ost_label": True
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
        store.setdefault("settings", {}).setdefault("journal_enabled", False)
        store.setdefault("settings", {}).setdefault("main_article_buttons_enabled", False)
        store.setdefault("settings", {}).setdefault("main_financial_value_buttons_enabled", False)
        store.setdefault("settings", {}).setdefault("gomonk_enabled", False)
        store.setdefault("settings", {}).setdefault("gomonk_entries", [])
        store.setdefault("settings", {}).setdefault("remaining_with_gomonk", True)
        store.setdefault("settings", {}).setdefault("usd_display_enabled", False)
        store.setdefault("settings", {}).setdefault("currency_mode", "ars_usd" if store.setdefault("settings", {}).get("usd_display_enabled", False) else "ars")
        store.setdefault("settings", {}).setdefault("remaining_show_ost_label", True)
        store.setdefault("settings", {}).setdefault("category_usd_enabled", False)
        store.setdefault("finance_mode", False)

        if is_owner_chat(chat_id):
            store["settings"]["auto_add"] = True

        if "known_chats" not in store:
            store["known_chats"] = {}

        return store



def _chat_identity_key(cid: int, info: dict | None = None) -> str:
    """Безопасный ключ дубля: username можно считать одним чатом, а одинаковый title — только подозрение, не удаление."""
    info = info or {}
    username = str(info.get("username") or "").strip().lower().lstrip("@")
    if username:
        return "u:" + username
    return "id:" + str(int(cid))


def _chat_title_suspect_key(cid: int, info: dict | None = None) -> str:
    info = info or {}
    title = re.sub(r"\s+", " ", str(info.get("title") or get_chat_display_name(cid) or "").strip().casefold())
    typ = str(info.get("type") or "")
    return f"t:{typ}:{title}" if title else f"id:{cid}"


def normalize_known_chats_for_owner() -> int:
    """
    Убирает только безопасные дубли карточек чатов у владельца:
    • одинаковый chat_id невозможен в dict, но битые ключи чистим;
    • одинаковый username считаем дублем;
    • одинаковые названия НЕ удаляем, а складываем в suspected_duplicate_titles.
    """
    if not OWNER_ID:
        return 0
    try:
        owner_store = get_chat_store(int(OWNER_ID))
        known = owner_store.setdefault("known_chats", {})
        if not isinstance(known, dict):
            owner_store["known_chats"] = {}
            return 0
        keep = {}
        removed = 0
        rows = []
        for cid_s, info in known.items():
            try:
                cid = int(cid_s)
            except Exception:
                removed += 1
                continue
            rows.append((cid, info if isinstance(info, dict) else {}, str(cid_s) in (data.get("chats", {}) or {})))
        rows.sort(key=lambda x: (not x[2], str(x[0])))

        seen_identity = set()
        title_map = defaultdict(list)
        for cid, info, exists in rows:
            key = _chat_identity_key(cid, info)
            if key in seen_identity:
                removed += 1
                continue
            seen_identity.add(key)
            keep[str(cid)] = info
            title_map[_chat_title_suspect_key(cid, info)].append(str(cid))

        suspects = {k: v for k, v in title_map.items() if len(v) > 1 and k and not k.startswith("id:")}
        if keep != known or owner_store.get("suspected_duplicate_titles") != suspects:
            owner_store["known_chats"] = keep
            owner_store["suspected_duplicate_titles"] = suspects
            save_data(data)
        return removed
    except Exception as e:
        log_error(f"normalize_known_chats_for_owner: {e}")
        return 0

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

    deduped = {}
    seen = set()
    for cid, info in sorted(result.items(), key=lambda kv: (str((kv[1] or {}).get("title") or "").lower(), str(kv[0]))):
        try:
            key = _chat_identity_key(int(cid), info if isinstance(info, dict) else {})
        except Exception:
            key = "id:" + str(cid)
        if key in seen:
            continue
        seen.add(key)
        deduped[str(cid)] = info
    return deduped


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
    if isinstance(value, dict) and value.get("formula"):
        formula = _xlsx_xml_escape(str(value.get("formula") or "").lstrip("="))
        cached = value.get("value", 0)
        try:
            cached = float(cached)
            if cached.is_integer():
                cached = int(cached)
        except Exception:
            cached = 0
        return f'<c r="{ref}"{s_attr}><f>{formula}</f><v>{cached}</v></c>'
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
<calcPr calcId="191029" fullCalcOnLoad="1" forceFullCalc="1"/>
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
                rows.append(_xlsx_record_row(fmt_date_table(dk), r.get("amount", 0), r.get("note", "")))
        _write_simple_xlsx(path, insert_blank_rows_between_days(rows, header_rows=1), sheet_name="Данные")
        return path
    except Exception as e:
        log_error(f"save_chat_xlsx({get_chat_display_name(chat_id)}): {e}")
        return None

def snapshot_chat_store(chat_id: int) -> dict:
    """Стабильный снимок одного чата для файлового бэкапа."""
    with locked_chat(int(chat_id)):
        normalize_chat_records(int(chat_id))
        store = data.get("chats", {}).get(str(chat_id)) or get_chat_store(chat_id)
        return json.loads(json.dumps(store, ensure_ascii=False, default=str))


def build_chat_backup_payload(chat_id: int, store: dict | None = None) -> dict:
    """JSON для чтения: последние операции и даты находятся сверху."""
    store = store or snapshot_chat_store(chat_id)
    records_desc = sorted((store.get("records", []) or []), key=record_sort_key, reverse=True)
    daily_src = store.get("daily_records", {}) or {}
    daily_desc = {}
    daily_by_date_desc = {}
    for day_key in sorted(daily_src.keys(), reverse=True):
        day_records = sorted((daily_src.get(day_key, []) or []), key=record_sort_key, reverse=True)
        daily_desc[str(day_key)] = backup_records_list(day_records)
        daily_by_date_desc[fmt_date_backup(day_key)] = backup_records_list(day_records)
    return {
        "kind": "chat_full_backup",
        "version": VERSION,
        "created_at": now_local().isoformat(timespec="seconds"),
        "date_format": "DD:MM:YY",
        "sort_order": "newest_first",
        "chat_id": chat_id,
        "chat_name": get_chat_display_name(chat_id),
        "balance": store.get("balance", 0),
        "records": backup_records_list(records_desc),
        "daily_records": daily_desc,
        "daily_records_by_date": daily_by_date_desc,
        "next_id": store.get("next_id", 1),
        "info": store.get("info", {}),
        "known_chats": store.get("known_chats", {}),
        "settings_backup": build_chat_settings_backup_payload(chat_id, store),
    }


def save_chat_json_only(chat_id: int) -> str | None:
    """Быстрый лёгкий JSON без CSV/Excel."""
    try:
        store = snapshot_chat_store(chat_id)
        payload = build_chat_backup_payload(chat_id, store)
        path = chat_json_file(chat_id)
        _save_json(path, payload)
        return path
    except Exception as e:
        log_error(f"save_chat_json_only({get_chat_display_name(chat_id)}): {e}")
        return None


def save_chat_json(chat_id: int):
    """Полный локальный пакет чата: JSON + CSV + опциональный Excel + META."""
    try:
        store = snapshot_chat_store(chat_id)
        payload = build_chat_backup_payload(chat_id, store)
        chat_path_json = chat_json_file(chat_id)
        _save_json(chat_path_json, payload)
        chat_path_csv = chat_csv_file(chat_id)
        chat_path_xlsx = chat_xlsx_file(chat_id)
        chat_path_meta = chat_meta_file(chat_id)
        with open(chat_path_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["date", "amount", "note"])
            daily = store.get("daily_records", {}) or {}
            rows = []
            for dk in sorted(daily.keys()):
                for r in sorted(daily.get(dk, []) or [], key=record_sort_key):
                    rows.append((fmt_date_table(dk), fmt_csv_amount(r.get("amount")), r.get("note", "")))
            write_csv_rows_with_day_gaps(w, rows, 3)
        if backup_excel_all_enabled():
            save_chat_xlsx(chat_id, chat_path_xlsx, store)
        meta = {
            "last_saved": now_local().isoformat(timespec="seconds"),
            "date_format": "DD.MM.YY",
            "record_count": sum(len(v) for v in store.get("daily_records", {}).values()),
            "excel_enabled": backup_excel_all_enabled(),
        }
        _save_json(chat_path_meta, meta)
        log_info(f"Per-chat files saved for chat {get_chat_display_name(chat_id)}")
        return chat_path_json
    except Exception as e:
        log_error(f"save_chat_json({get_chat_display_name(chat_id)}): {e}")
        return None
def _extract_universal_state(payload: dict) -> dict:
    """Поддерживает текущий плоский формат и будущий envelope со state/bot_state."""
    if not isinstance(payload, dict):
        return {}
    for key in ("state", "bot_state", "data"):
        candidate = payload.get(key)
        if isinstance(candidate, dict) and isinstance(candidate.get("chats"), dict):
            state = json.loads(json.dumps(candidate, ensure_ascii=False, default=str))
            # Runtime-слой envelope может дополнять старое состояние.
            runtime = payload.get("runtime") or payload.get("_runtime_snapshot") or {}
            if isinstance(runtime, dict):
                state.setdefault("forward_index", runtime.get("forward_index", {}))
                state.setdefault("finance_active_chats", runtime.get("finance_active_chats", {}))
                state.setdefault("backup_flags", runtime.get("backup_flags", {}))
                state.setdefault("_global_settings", runtime.get("global_settings", {}))
            return state
    return json.loads(json.dumps(payload, ensure_ascii=False, default=str))


def _migrate_full_state(restored: dict) -> dict:
    """Мягкая миграция старых JSON: неизвестные поля сохраняются, отсутствующие добавляются."""
    base = default_data()
    for key, value in base.items():
        if key not in restored:
            restored[key] = json.loads(json.dumps(value, ensure_ascii=False, default=str))
    restored.setdefault("chats", {})
    restored.setdefault("forward_rules", {})
    restored.setdefault("forward_finance", {})
    restored.setdefault("forward_index", {})

    default_globals = default_data().get("_global_settings", {})
    globals_state = restored.setdefault("_global_settings", {})
    if not isinstance(globals_state, dict):
        globals_state = {}
        restored["_global_settings"] = globals_state
    for key, value in default_globals.items():
        globals_state.setdefault(key, value)

    runtime = restored.get("_runtime_snapshot") or {}
    if isinstance(runtime, dict):
        if not restored.get("forward_index") and isinstance(runtime.get("forward_index"), dict):
            restored["forward_index"] = runtime.get("forward_index") or {}
        if not restored.get("finance_active_chats") and runtime.get("finance_active_chats"):
            restored["finance_active_chats"] = runtime.get("finance_active_chats")
        if not restored.get("backup_flags") and runtime.get("backup_flags"):
            restored["backup_flags"] = runtime.get("backup_flags")
        for key, value in (runtime.get("global_settings") or {}).items():
            globals_state.setdefault(key, value)
    return restored


def _restore_runtime_state_from_data(restored: dict):
    """Загружает логическое состояние в оперативные структуры ДО первого save_data()."""
    finance_active_chats.clear()
    fac = restored.get("finance_active_chats") or {}
    if isinstance(fac, dict):
        items = fac.items()
    elif isinstance(fac, (list, tuple, set)):
        items = ((x, True) for x in fac)
    else:
        items = ()
    for cid, enabled in items:
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

    flags = restored.get("backup_flags") or {}
    backup_flags["drive"] = bool(flags.get("drive", True))
    backup_flags["channel"] = bool(flags.get("channel", True))

    # Критически важно: сначала восстановить forward_map. Иначе save_data() запишет
    # пустой индекс поверх загруженного JSON, и правки старых сообщений не найдут копии.
    _load_forward_index_from_data(restored)


def restore_from_json(chat_id: int, path: str):
    """Восстановление глобального универсального или старого per-chat JSON."""
    global data
    raw_payload = _load_json(path, None)
    if not isinstance(raw_payload, dict):
        raise RuntimeError("JSON повреждён или пустой")

    payload = _extract_universal_state(raw_payload)
    if "chats" in payload and isinstance(payload.get("chats"), dict):
        data = _migrate_full_state(payload)
        _restore_runtime_state_from_data(data)

        rebuild_global_records()
        save_data(data, full=True)
        if not is_data_effectively_empty_for_restore(data):
            _clear_restore_guard()
        # v90: после restore не запускаем overwrite; baseline+delta начнутся только после нового изменения.
        log_info(
            "restore_from_json: universal global state restored "
            f"schema={(raw_payload.get('_universal_backup') or {}).get('schema_version', 'legacy')} "
            f"forward_index={len(data.get('forward_index', {}) or {})}"
        )
        return

    if "records" in payload or "daily_records" in payload:
        store = get_chat_store(chat_id)

        store["records"] = payload.get("records", []) or []
        store["daily_records"] = payload.get("daily_records", {}) or {}
        store["next_id"] = int(payload.get("next_id", 1) or 1)
        store["info"] = payload.get("info", store.get("info", {})) or store.get("info", {})
        store["known_chats"] = payload.get("known_chats", store.get("known_chats", {})) or store.get("known_chats", {})
        if isinstance(payload.get("settings"), dict):
            store["settings"].update(payload.get("settings") or {})

        if not store["records"] and store["daily_records"]:
            all_recs = []
            for dk in sorted(store["daily_records"].keys()):
                all_recs.extend(store["daily_records"][dk] or [])
            store["records"] = all_recs

        renumber_chat_records(chat_id)
        recalc_balance(chat_id)
        rebuild_global_records()

        save_data(data, chat_ids=[chat_id])
        if store.get("records") or any(store.get("daily_records", {}).values()):
            _clear_restore_guard()
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
def note_has_income_marker(note: str) -> bool:
    """True, если текст явно говорит о приходе денег.
    Учитывает «приход» в любом регистре, но не срабатывает на «не приход», «без прихода», «нет прихода».
    """
    t = re.sub(r"\s+", " ", str(note or "").casefold()).strip()
    if not t:
        return False
    negative_patterns = (
        r"(?:^|\s)не\s+приход",
        r"(?:^|\s)без\s+приход",
        r"(?:^|\s)нет\s+приход",
        r"(?:^|\s)ne\s+prihod",
        r"(?:^|\s)bez\s+prihod",
        r"(?:^|\s)net\s+prihod",
    )
    if any(re.search(pat, t, re.I) for pat in negative_patterns):
        return False
    income_patterns = (
        r"приход",
        r"prihod",
        r"prixod",
        r"обмен",
        r"возврат",
        r"сдача",
    )
    return any(re.search(pat, t, re.I) for pat in income_patterns)


USD_EXPLICIT_AFTER_RE = re.compile(
    r"(?P<sign>[+\-–]?)\s*(?P<num>\d[\d\s.,_'’]*?)(?P<mult>[kк])?\s*(?P<cur>usd|usд|усд|\$)",
    re.I,
)
USD_EXPLICIT_PREFIX_RE = re.compile(
    r"\$\s*(?P<sign>[+\-–]?)\s*(?P<num>\d[\d\s.,_'’]*?)(?P<mult>[kк])?(?=\s|$)",
    re.I,
)
USD_COMPACT_K_RE = re.compile(
    r"(?P<sign>[+\-–]?)\s*(?P<num>\d+(?:[.,]\d+)?)\s*(?P<plus_after>\+)?\s*[kк]\b",
    re.I,
)
USD_EXCHANGE_RE = re.compile(r"обмен|exchange|change", re.I)
USD_PESO_RE = re.compile(r"песс?о|peso|ars|арс", re.I)


def _parse_usd_number_parts(sign: str, num: str, mult: str = "") -> float:
    raw = str(num or "").strip()
    if not raw:
        raise ValueError("empty usd amount")
    # parse_amount без явного плюса считает число расходом, поэтому здесь берём модуль.
    value = abs(float(parse_amount("+" + raw)))
    if str(mult or "").strip().casefold() in {"k", "к"}:
        value *= 1000.0
    if str(sign or "").strip() in {"-", "–"}:
        return -value
    if str(sign or "").strip() == "+":
        return value
    return -value


def extract_usd_transaction(text: str) -> dict | None:
    """Извлекает отдельное движение USD из пользовательской строки.

    Правила v93:
    - явные USD/УСД/$: плюс = приход, минус/без знака = расход;
    - «обмен ... песо/ARS» с компактным 1к/2к = расход USD;
    - «+1к от ...» = приход USD;
    - «И 5+к» = приход 5000 USD.
    Возвращает span USD-фрагмента, чтобы ARS-часть можно было разобрать отдельно.
    """
    raw = str(text or "")
    if not raw.strip():
        return None

    candidates = []
    for rx in (USD_EXPLICIT_AFTER_RE, USD_EXPLICIT_PREFIX_RE):
        for m in rx.finditer(raw):
            try:
                amount = _parse_usd_number_parts(m.group("sign"), m.group("num"), m.group("mult"))
            except Exception:
                continue
            # Если знака нет, слово «приход» может явно задать приход; «обмен» для USD остаётся расходом.
            if not str(m.group("sign") or "").strip():
                low = raw.casefold()
                if ("приход" in low or "prihod" in low) and not USD_EXCHANGE_RE.search(low):
                    amount = abs(amount)
            candidates.append({
                "amount": float(amount),
                "span": m.span(),
                "explicit": True,
                "token": m.group(0),
            })
    if candidates:
        # Берём первое явное USD-значение по порядку текста.
        candidates.sort(key=lambda x: x["span"][0])
        info = candidates[0]
        info["note"] = re.sub(r"\s+", " ", raw).strip().lower()
        return info

    low = raw.casefold()
    k_matches = list(USD_COMPACT_K_RE.finditer(raw))
    if not k_matches:
        return None

    # «И 5+к» — специальная пользовательская запись прихода USD.
    for m in k_matches:
        before = low[max(0, m.start() - 6):m.start()]
        if m.group("plus_after") and re.search(r"(?:^|\s)и\s*$", before):
            value = abs(_parse_usd_number_parts("+", m.group("num"), "к"))
            return {"amount": value, "span": m.span(), "explicit": False, "token": m.group(0), "note": re.sub(r"\s+", " ", raw).strip().lower()}

    # Обмен USD -> песо/ARS: компактная сумма с «к» считается расходом USD.
    if USD_EXCHANGE_RE.search(low) and (USD_PESO_RE.search(low) or len(num_re.findall(raw)) >= 2):
        exchange_pos = USD_EXCHANGE_RE.search(low).start()
        m = min(k_matches, key=lambda x: abs(x.start() - exchange_pos))
        value = abs(_parse_usd_number_parts("+", m.group("num"), "к"))
        return {"amount": -value, "span": m.span(), "explicit": False, "token": m.group(0), "note": re.sub(r"\s+", " ", raw).strip().lower()}

    # «+1к от ...» / «+1к приход» — приход USD без явного USD.
    for m in k_matches:
        if str(m.group("sign") or "").strip() == "+" and (re.search(r"\bот\b", low) or "приход" in low):
            value = abs(_parse_usd_number_parts("+", m.group("num"), "к"))
            return {"amount": value, "span": m.span(), "explicit": False, "token": m.group(0), "note": re.sub(r"\s+", " ", raw).strip().lower()}

    return None


def _remove_usd_fragment_for_ars(text: str, usd_info: dict | None) -> str:
    raw = str(text or "")
    if not usd_info or not usd_info.get("span"):
        return raw
    try:
        start, end = usd_info["span"]
        rest = (raw[:int(start)] + " " + raw[int(end):]).strip()
    except Exception:
        rest = raw
    # В «1к обмен на песо по 1500» число после «по» — курс, а не движение ARS.
    rest = re.sub(r"(?i)\bпо\s*[+\-–]?\s*\d[\d\s.,_'’]*", " ", rest)
    return re.sub(r"\s+", " ", rest).strip()


def parse_financial_components(text: str) -> dict:
    """Разбирает одну строку одновременно на ARS и отдельное движение USD."""
    raw = str(text or "").strip()
    usd = extract_usd_transaction(raw)
    if usd is None:
        amount, note = split_amount_and_note(raw)
        return {
            "amount": float(amount), "note": note,
            "usd_amount": None, "usd_note": "", "usd_only": False,
            "source_finance_text": raw,
        }

    ars_text = _remove_usd_fragment_for_ars(raw, usd)
    ars_amount = None
    ars_note = ""
    if num_re.search(ars_text or ""):
        try:
            ars_amount, ars_note = split_amount_and_note(ars_text)
        except Exception:
            ars_amount, ars_note = None, ""

    usd_only = ars_amount is None
    try:
        _us, _ue = usd.get("span") or (0, 0)
        usd_note = re.sub(r"\s+", " ", (raw[:int(_us)] + " " + raw[int(_ue):])).strip().lower()
    except Exception:
        usd_note = ""
    return {
        "amount": float(ars_amount or 0.0),
        "note": ars_note if ars_amount is not None else re.sub(r"\s+", " ", raw).strip().lower(),
        "usd_amount": float(usd.get("amount", 0.0) or 0.0),
        "usd_note": usd_note,
        "usd_only": bool(usd_only),
        "source_finance_text": raw,
    }


def parse_usd_edit_value(text: str):
    """Редактирование уже найденной USD-записи: число без знака = расход, + = приход."""
    m = num_re.search(str(text or ""))
    if not m:
        raise ValueError("no usd number")
    amount = parse_amount(m.group(0))
    note = (str(text or "")[:m.start()] + " " + str(text or "")[m.end():]).strip()
    note = re.sub(r"\s+", " ", note).lower()
    return float(amount), note


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

    # Приход без знака "+": «приход/обмен/возврат/сдача» считаем поступлением,
    # но «не приход / без прихода / нет прихода» не переворачивает сумму.
    if amount < 0 and note_has_income_marker(note):
        amount = abs(amount)

    return amount, note


EXPENSE_CATEGORIES = {
    "ПРОДУКТЫ": ["продукты", "шб", "еда"],
    "ОРГТЕХНИКА": ["оргтех", "оргтехника"],
    "СВЯЗЬ": ["тел", "tel", "пополнение"],
    "АВТО": ["авто", "бензин", "билет"],
    "ПЕРЕВОДЫ": ["переводы", "перевод", "переводчик"],
    "ПРОЧЕЕ": [],
}

EXPENSE_CATEGORY_SLUGS = {
    "ПРОДУКТЫ": "food",
    "ОРГТЕХНИКА": "org",
    "СВЯЗЬ": "link",
    "АВТО": "auto",
    "ПЕРЕВОДЫ": "transfers",
    "ПРОЧЕЕ": "other",
}
CATEGORY_BY_SLUG = {v: k for k, v in EXPENSE_CATEGORY_SLUGS.items()}
EXPENSE_CATEGORY_ORDER = [
    "ПРОДУКТЫ",
    "ОРГТЕХНИКА",
    "СВЯЗЬ",
    "АВТО",
    "ПЕРЕВОДЫ",
    "ПРОЧЕЕ",
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
        raw_name = str(item.get("name") or "").strip()
        name = _clean_category_display_name(raw_name).upper()
        if name and raw_name != name:
            item["name"] = name
        keywords = [str(x).strip().lower() for x in (item.get("keywords") or []) if str(x).strip()]
        slug = str(item.get("slug") or "").strip()
        if not name or not keywords:
            continue
        if not slug:
            slug = make_custom_category_slug(name, raw)
            item["slug"] = slug
        out.append({"name": name, "slug": slug, "keywords": keywords})
    return out




def _base_category_overrides(store: dict | None) -> dict:
    if not isinstance(store, dict):
        return {}
    settings = store.setdefault("settings", {})
    raw = settings.setdefault("expense_categories_base_overrides", {})
    if not isinstance(raw, dict):
        raw = {}
        settings["expense_categories_base_overrides"] = raw
    return raw


def _base_category_items(store: dict | None = None) -> list[dict]:
    overrides = _base_category_overrides(store)
    items = []
    for default_name in EXPENSE_CATEGORY_ORDER:
        slug = EXPENSE_CATEGORY_SLUGS.get(default_name)
        ov = overrides.get(slug) if isinstance(overrides, dict) else None
        if not isinstance(ov, dict):
            ov = {}
        raw_name = str(ov.get("name") or default_name).strip()
        name = _clean_category_display_name(raw_name).upper()
        if ov and name and raw_name != name:
            ov["name"] = name
        keywords = ov.get("keywords") if isinstance(ov.get("keywords"), list) else EXPENSE_CATEGORIES.get(default_name, [])
        keywords = [str(x).strip().lower() for x in (keywords or []) if str(x).strip()]
        items.append({"name": name, "slug": slug, "keywords": keywords, "base": True, "default_name": default_name})
    return items


def _base_category_item_by_slug(store: dict | None, slug: str) -> dict | None:
    slug = str(slug or "")
    for item in _base_category_items(store):
        if item.get("slug") == slug:
            return item
    return None

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


def get_expense_category_order_slugs(store: dict | None = None) -> list[str]:
    """Стабильный порядок статей сверху вниз; пользователь может менять его в v91."""
    items = list(_base_category_items(store)) + list(_custom_category_list(store))
    available = [str(item.get("slug") or "") for item in items if str(item.get("slug") or "")]
    try:
        settings = (store or {}).setdefault("settings", {})
        saved = [str(x) for x in (settings.get("expense_category_order_slugs") or []) if str(x)]
    except Exception:
        saved = []
    result = [slug for slug in saved if slug in available]
    result.extend(slug for slug in available if slug not in result)
    return result


def get_expense_category_order(store: dict | None = None) -> list[str]:
    by_slug = {}
    for item in list(_base_category_items(store)) + list(_custom_category_list(store)):
        slug = str(item.get("slug") or "")
        if slug:
            by_slug[slug] = item.get("name")
    return [by_slug[slug] for slug in get_expense_category_order_slugs(store) if slug in by_slug]


def move_expense_category_order(store: dict, slug: str, direction: str) -> bool:
    order = get_expense_category_order_slugs(store)
    slug = str(slug or "")
    if slug not in order:
        return False
    idx = order.index(slug)
    new_idx = idx - 1 if str(direction).lower() == "up" else idx + 1
    if new_idx < 0 or new_idx >= len(order):
        return False
    order[idx], order[new_idx] = order[new_idx], order[idx]
    store.setdefault("settings", {})["expense_category_order_slugs"] = order
    return True


_category_order_selection = {}


def _category_order_selection_key(chat_id: int, params: tuple) -> tuple:
    return (int(chat_id),) + tuple(str(x) for x in params)


def move_expense_category_to_position(store: dict, slug: str, position: int) -> bool:
    """Вставка статьи в новую позицию со сдвигом промежуточных статей."""
    order = get_expense_category_order_slugs(store)
    slug = str(slug or "")
    if slug not in order or not order:
        return False
    try:
        target_idx = max(0, min(len(order) - 1, int(position) - 1))
    except Exception:
        return False
    old_idx = order.index(slug)
    if old_idx == target_idx:
        return True
    order.pop(old_idx)
    order.insert(target_idx, slug)
    store.setdefault("settings", {})["expense_category_order_slugs"] = order
    return True


def get_expense_category_slug(category: str, store: dict | None = None) -> str | None:
    category = _clean_category_display_name(str(category or "")).upper()
    for item in _base_category_items(store):
        if category in {str(item.get("name") or "").upper(), str(item.get("default_name") or "").upper()}:
            return item.get("slug")
    for item in _custom_category_list(store):
        if item["name"] == category:
            return item["slug"]
    return None


def get_category_by_slug(slug: str, store: dict | None = None) -> str | None:
    slug = str(slug or "").strip()
    base = _base_category_item_by_slug(store, slug)
    if base:
        return base.get("name")
    for item in _custom_category_list(store):
        if item["slug"] == slug:
            return item["name"]
    return None


def parse_category_definition(text: str):
    raw = _clean_category_display_name(str(text or "").strip())
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
    """Определяет статью расхода; всё без совпавших ключей попадает в ПРОЧЕЕ."""
    # Сначала пользовательские статьи: они важнее стандартных, если ключ совпал.
    for item in _custom_category_list(store):
        for kw in item.get("keywords", []):
            if expense_keyword_matches(note, kw):
                return item.get("name")
    for item in _base_category_items(store):
        if item.get("slug") == "other":
            continue
        for kw in item.get("keywords", []):
            if expense_keyword_matches(note, kw):
                return item.get("name")
    other = _base_category_item_by_slug(store, "other")
    return (other or {}).get("name") or "ПРОЧЕЕ"

def resolve_expense_category_for_record(rec: dict, store: dict | None = None):
    """Учитывает ручной перенос записи из ПРОЧЕЕ в выбранную статью."""
    try:
        override_slug = str((rec or {}).get("category_override_slug") or "").strip()
        if override_slug:
            category = get_category_by_slug(override_slug, store)
            if category:
                return category
    except Exception:
        pass
    return resolve_expense_category((rec or {}).get("note", ""), store)

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
            cat = resolve_expense_category_for_record(r, store)
            if not cat:
                continue
            out[cat] = out.get(cat, 0) + (-amt)
    return out



def _record_int_id(rec: dict) -> int:
    try:
        return int((rec or {}).get("id", 0) or 0)
    except Exception:
        return 0


def sorted_records_for_day(store: dict, day_key: str) -> list:
    return sorted((store.get("daily_records", {}) or {}).get(str(day_key), []) or [], key=record_sort_key)


def expense_anchor_records_for_day(store: dict, day_key: str) -> list:
    """Расходные записи дня, которые можно выбрать как точную границу периода."""
    out = []
    for rec in sorted_records_for_day(store, day_key):
        try:
            if float(rec.get("amount", 0) or 0) < 0:
                out.append(rec)
        except Exception:
            continue
    return out


def expense_anchor_button_label(rec: dict, store: dict | None = None) -> str:
    """Короткая, но понятная подпись кнопки точного расхода."""
    try:
        raw_amount = float(rec.get("amount", 0) or 0)
        amount = format_store_amount(store or {}, raw_amount, mixed_space=False, ars_plain=False)
    except Exception:
        amount = str(rec.get("amount", ""))
    note = _clean_category_display_name(re.sub(r"\s+", " ", str(rec.get("note", "") or "")).strip())
    category = _clean_category_display_name(resolve_expense_category_for_record(rec, store) or "")
    rec_code = str(rec.get("short_id") or f"R{_record_int_id(rec)}")
    parts = [rec_code, amount]
    if note:
        parts.append(note[:30])
    if category and category.casefold() not in note.casefold():
        parts.append(f"[{category[:16]}]")
    return " • ".join(parts)[:62]


def exact_record_range(store: dict, start_day: str, start_rid: int | None, end_day: str, end_rid: int | None):
    """Записи между двумя точными границами включительно.

    start_rid=0/None означает начало стартового дня.
    end_rid=0/None означает конец конечного дня.
    Граница выбирается по расходу, но в экспорт попадают все записи между
    выбранными позициями: и расходы, и приходы.
    """
    start_day = str(start_day)[:10]
    end_day = str(end_day)[:10]
    try:
        start_rid = int(start_rid or 0)
    except Exception:
        start_rid = 0
    try:
        end_rid = int(end_rid or 0)
    except Exception:
        end_rid = 0

    if end_day < start_day:
        start_day, end_day = end_day, start_day
        start_rid, end_rid = end_rid, start_rid

    rows = []
    daily = store.get("daily_records", {}) or {}
    for day_key in sorted(daily.keys()):
        if not (start_day <= day_key <= end_day):
            continue
        recs = sorted_records_for_day(store, day_key)
        if not recs:
            continue

        lo = 0
        hi = len(recs) - 1
        if day_key == start_day and start_rid:
            found = next((idx for idx, rec in enumerate(recs) if _record_int_id(rec) == start_rid), None)
            if found is not None:
                lo = found
        if day_key == end_day and end_rid:
            found = next((idx for idx, rec in enumerate(recs) if _record_int_id(rec) == end_rid), None)
            if found is not None:
                hi = found
        if lo > hi:
            continue
        for rec in recs[lo:hi + 1]:
            rows.append((day_key, rec))
    return rows


def exact_boundary_text(store: dict, day_key: str, rid: int | None, is_start: bool) -> str:
    rid = int(rid or 0)
    if not rid:
        return f"{fmt_date_ddmmyy(day_key)} — {'с начала дня' if is_start else 'до конца дня'}"
    rec = next((_r for _r in sorted_records_for_day(store, day_key) if _record_int_id(_r) == rid), None)
    if not rec:
        return f"{fmt_date_ddmmyy(day_key)} — {'с начала дня' if is_start else 'до конца дня'}"
    return f"{fmt_date_ddmmyy(day_key)} — {expense_anchor_button_label(rec, store)}"


def calc_categories_for_record_range(store: dict, start_day: str, start_rid: int, end_day: str, end_rid: int) -> dict:
    out = {}
    for _day, rec in exact_record_range(store, start_day, start_rid, end_day, end_rid):
        try:
            amt = float(rec.get("amount", 0) or 0)
        except Exception:
            continue
        if amt >= 0:
            continue
        category = resolve_expense_category_for_record(rec, store)
        if not category:
            continue
        out[category] = out.get(category, 0) + (-amt)
    return out


def collect_items_for_category_record_range(store: dict, start_day: str, start_rid: int, end_day: str, end_rid: int, category: str):
    items = []
    for day_key, rec in exact_record_range(store, start_day, start_rid, end_day, end_rid):
        try:
            amt = float(rec.get("amount", 0) or 0)
        except Exception:
            continue
        if amt >= 0:
            continue
        note = rec.get("note", "")
        if resolve_expense_category_for_record(rec, store) == category:
            items.append((day_key, -amt, note))
    return items


def summarize_categories_record_range(store: dict, start_day: str, start_rid: int, end_day: str, end_rid: int):
    cats = calc_categories_for_record_range(store, start_day, start_rid, end_day, end_rid)
    mode = currency_mode_from_store(store)
    category_mixed = bool(store.setdefault("settings", {}).get("category_usd_enabled", False) and _v85_enabled("usd_categories"))
    show_rate = mode != "ars" or category_mixed
    rate_info = usd_rate_cached() if show_rate else None
    lines = [
        "📦 Расходы по статьям — точный период",
        f"▶️ {exact_boundary_text(store, start_day, start_rid, True)}",
        f"⏹ {exact_boundary_text(store, end_day, end_rid, False)}",
        "",
    ]
    if show_rate:
        if rate_info:
            lines.append(f"💵 Курс: 1 USD = {fmt_num(rate_info['rate']).lstrip('+')} ARS ({_clean_category_display_name(rate_info.get('source') or 'DolarAPI')})")
        else:
            lines.append("💵 Курс USD временно недоступен")
        lines.append("")
    if not cats:
        lines.append("Нет данных по статьям в выбранных границах.")
    else:
        for category in get_ordered_category_names(cats=cats, store=store):
            clean_name = _clean_category_display_name(category).upper()
            amount = cats.get(category, 0)
            lines.append(f"{clean_name}: {format_category_amount(store, amount, category_mixed)}")
    return wm_common("\n".join(lines), 7), cats

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
            if resolve_expense_category_for_record(r, store) == category:
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
    for item in _base_category_items(store):
        keys = item.get("keywords", []) or []
        clean_name = _clean_category_display_name(item.get("name") or "")
        lines.append(f"{clean_name}: {', '.join(keys) if keys else '—'}")
    custom = _custom_category_list(store)
    if custom:
        lines.append("")
        lines.append("Пользовательские статьи:")
        for item in custom:
            clean_name = _clean_category_display_name(item.get("name") or "")
            lines.append(f"{clean_name}: {', '.join(item.get('keywords') or [])}")
    lines.append("")
    lines.append("Добавить новую статью можно в окне 📊 Статьи → ➕ Добавить статью.")
    return wm_common("\n".join(lines), 7)


def summarize_categories(store: dict, start: str, end: str, label: str):
    """Сводка статей с тем же режимом валюты, что и основное финансовое окно."""
    cats = calc_categories_for_period(store, start, end)
    mode = currency_mode_from_store(store)
    category_mixed = bool(
        mode == "ars"
        and store.setdefault("settings", {}).get("category_usd_enabled", False)
        and _v85_enabled("usd_categories")
    )
    show_rate = mode != "ars" or category_mixed
    rate_info = usd_rate_cached(force=False) if show_rate else None
    lines = [
        "📦 Расходы по статьям",
        f"🗓 {label}",
        ""
    ]
    if show_rate:
        if rate_info and rate_info.get("rate"):
            lines.append(
                f"💵 Курс: 1 USD = {fmt_num(rate_info['rate']).lstrip('+')} ARS "
                f"({_clean_category_display_name(rate_info.get('source') or 'DolarAPI')})"
            )
        else:
            lines.append("💵 Курс USD временно недоступен")
        lines.append("")
    if not cats:
        lines.append("Нет данных по статьям за этот период.")
    else:
        for cat in get_ordered_category_names(cats=cats, store=store):
            clean_name = _clean_category_display_name(cat).upper()
            lines.append(f"{clean_name}: {format_category_amount(store, cats.get(cat, 0), category_mixed)}")
    lines.extend(["", "✏️ Изменить: название статьи и/или её ключевые слова."])
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


def export_callback(data_str: str) -> str:
    return make_short_callback(data_str, "cbx")


def build_categories_buttons(start: str, end: str, store: dict | None = None):
    kb = types.InlineKeyboardMarkup(row_width=3)
    buttons = []
    for cat in get_ordered_category_names(include_all=True, store=store):
        slug = get_expense_category_slug(cat, store)
        if not slug:
            continue
        buttons.append(
            IB(
                _clean_category_display_name(cat),
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

    if _v85_enabled("usd_categories") and currency_mode_from_store(store or {}) == "ars":
        usd_on = bool((store or {}).setdefault("settings", {}).get("category_usd_enabled", False))
        kb.row(IB("💵 USD ВЫКЛ" if usd_on else "💵 USD ВКЛ", callback_data=cat_callback(f"cat_usd_toggle_period:{mode}:{start}:{end}")))
    if mode == "wthu":
        kb.row(IB("↕️ Расположение", callback_data=cat_callback(f"cat_order_open_sum:{mode}:{start}:{end}")))
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


def build_category_layout_text(store: dict, context: str = "exact") -> str:
    if context == "exact":
        lines = [
            "↕️ Расположение статей",
            "",
            "Слева выберите статью — возле неё появится ✅. Затем справа нажмите номер новой позиции.",
            "Статья будет вставлена в выбранное место, остальные автоматически сдвинутся.",
            "",
        ]
    else:
        lines = [
            "↕️ Расположение статей",
            "",
            "Слева выберите статью — возле неё появится ✅. Затем справа нажмите номер новой позиции.",
            "Статья будет вставлена в выбранное место, остальные автоматически сдвинутся.",
            "",
        ]
    for idx, name in enumerate(get_expense_category_order(store), 1):
        lines.append(f"{idx}. {_clean_category_display_name(name)}")
    return wm_common("\n".join(lines), 7)


def build_category_layout_keyboard(store: dict, context: str, params: tuple, chat_id: int | None = None) -> object:
    slugs = get_expense_category_order_slugs(store)
    if context == "exact":
        kb = types.InlineKeyboardMarkup(row_width=2)
        start_key, start_rid, end_key, end_rid = params
        selection_key = _category_order_selection_key(int(chat_id or 0), params)
        selected = _category_order_selection.get(selection_key)
        for idx, slug in enumerate(slugs, 1):
            name = _clean_category_display_name(get_category_by_slug(slug, store) or slug)
            left = f"✅ {name}" if slug == selected else name
            select_cb = cat_callback(f"cat_order_select_exact:{slug}:{start_key}:{int(start_rid)}:{end_key}:{int(end_rid)}")
            pos_cb = cat_callback(f"cat_order_position_exact:{idx}:{start_key}:{int(start_rid)}:{end_key}:{int(end_rid)}")
            kb.row(IB(left[:36], callback_data=select_cb), IB(str(idx), callback_data=pos_cb))
        back_cb = cat_callback(f"cat_range_records:{start_key}:{int(start_rid)}:{end_key}:{int(end_rid)}")
        kb.row(IB("⬅️ Назад", callback_data=back_cb), IB("❌ Закрыть", callback_data=cat_callback("cat_close")))
        return kb

    kb = types.InlineKeyboardMarkup(row_width=2)
    mode, start, end = params
    selection_key = _category_order_selection_key(int(chat_id or 0), ("sum", mode, start, end))
    selected = _category_order_selection.get(selection_key)
    for idx, slug in enumerate(slugs, 1):
        name = _clean_category_display_name(get_category_by_slug(slug, store) or slug)
        left = f"✅ {name}" if slug == selected else name
        select_cb = cat_callback(f"cat_order_select_sum:{slug}:{mode}:{start}:{end}")
        pos_cb = cat_callback(f"cat_order_position_sum:{idx}:{mode}:{start}:{end}")
        kb.row(IB(left[:36], callback_data=select_cb), IB(str(idx), callback_data=pos_cb))
    if mode == "wthu":
        back_cb = cat_callback(f"cat_wthu:{start}")
    elif mode == "wk":
        back_cb = cat_callback(f"cat_wk:{start}")
    else:
        back_cb = cat_callback(f"cat_range_custom2:{start}:{end}")
    kb.row(IB("⬅️ Назад", callback_data=back_cb), IB("❌ Закрыть", callback_data=cat_callback("cat_close")))
    return kb

def build_category_detail_text(store: dict, start: str, end: str, category: str, label: str):
    """Детализация статьи в режимах ARS / ARS-USD / USD."""
    items = collect_items_for_category(store, start, end, category)
    mode = currency_mode_from_store(store)
    category_mixed = bool(
        mode == "ars"
        and store.setdefault("settings", {}).get("category_usd_enabled", False)
        and _v85_enabled("usd_categories")
    )
    show_rate = mode != "ars" or category_mixed
    rate_info = usd_rate_cached(force=False) if show_rate else None
    clean_category = _clean_category_display_name(category).upper()
    lines = [
        f"📦 {clean_category}",
        f"🗓 {label}",
        ""
    ]

    total = sum(amt for _, amt, _ in items)
    lines.append(f"Итого: {format_category_amount(store, total, category_mixed)}")
    if show_rate and rate_info and rate_info.get("rate"):
        lines.append(
            f"Курс: 1 USD = {fmt_num(rate_info['rate']).lstrip('+')} ARS "
            f"({_clean_category_display_name(rate_info.get('source') or 'DolarAPI')})"
        )
    lines.append("")

    if not items:
        lines.append("Нет операций по этой статье.")
    else:
        for day_i, amt_i, note_i in items:
            clean_note = _clean_category_display_name((note_i or "").strip())
            amount_text = format_category_amount(store, amt_i, category_mixed)
            lines.append(f"• {fmt_date_ddmmyy(day_i)}: {amount_text} {clean_note}".rstrip())

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
    _category_wait_timers.pop(key, None)
    DELAYED_SCHEDULER.cancel(f"category-wait:{int(chat_id)}:{str(field)}")
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
    """Автоотмена ожидания статьи без ежесекундного редактирования окна."""
    key = _category_wait_key(chat_id, field)

    def _job():
        try:
            store = get_chat_store(chat_id)
            wait = store.get(field) or {}
            if not wait or int(wait.get("prompt_msg_id") or 0) != int(prompt_message_id):
                return
            cleared = clear_category_wait_state(chat_id, field, prompt_message_id, delete_prompt=True)
            if cleared:
                send_and_auto_delete(chat_id, "⌛ Время ожидания истекло. Команда отменена.", 8)
        except Exception as e:
            log_error(f"schedule_cancel_category_wait({chat_id},{field},{prompt_message_id}): {e}")

    scheduler_key = f"category-wait:{int(chat_id)}:{str(field)}"
    DELAYED_SCHEDULER.cancel(scheduler_key)
    deadline = DELAYED_SCHEDULER.schedule(scheduler_key, float(delay), _job)
    _category_wait_timers[key] = deadline

def _category_prompt_keyboard(chat_id: int, owner_day_key: str | None = None, back_callback: str | None = None, insert_text: str | None = None):
    kb = types.InlineKeyboardMarkup()
    day = owner_day_key or get_chat_store(chat_id).get("current_view_day") or today_key()
    owner_store = get_chat_store(chat_id)
    wait = owner_store.get("category_add_wait") or owner_store.get("category_edit_wait") or {}
    target_chat_id = int(wait.get("target_chat_id") or chat_id)
    if target_chat_id != int(chat_id):
        delete_callback = fvcat_callback(f"fvcat_del_menu:{target_chat_id}:{day}:{day}")
    else:
        delete_callback = cat_callback("cat_del_menu")
    if insert_text:
        kb.row(make_copy_or_inline_button("✏️ Изменить значение", str(insert_text)))
    kb.row(IB("🗑 Удалить статью", callback_data=delete_callback))
    kb.row(
        IB("⬅️ Назад", callback_data=cat_callback("cat_prompt_back")),
        IB("❌ Закрыть", callback_data=cat_callback("cat_add_cancel")),
        IB("⬅️ Осн. окно", callback_data=back_callback or f"d:{day}:back_main"),
    )
    return kb


def category_custom_items_for_chat(chat_id: int) -> list[dict]:
    return list(_custom_category_list(get_chat_store(chat_id)))


def category_edit_items_for_chat(chat_id: int) -> list[dict]:
    store = get_chat_store(chat_id)
    return list(_base_category_items(store)) + list(_custom_category_list(store))


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
    name = str(name or "").strip().upper()
    keywords = sorted(set(str(x).strip().lower() for x in (keywords or []) if str(x).strip()))

    if str(old_slug) in CATEGORY_BY_SLUG:
        overrides = settings.setdefault("expense_categories_base_overrides", {})
        if not isinstance(overrides, dict):
            overrides = {}
            settings["expense_categories_base_overrides"] = overrides
        overrides[str(old_slug)] = {"name": name, "keywords": keywords}
        save_data(data)
        schedule_config_backup_for_chats(chat_id)
        bot_journal("base_category_edited", chat_id, f"{old_slug} -> {name}: {', '.join(keywords)}")
        return {"name": name, "slug": str(old_slug), "keywords": keywords, "base": True}

    custom = settings.setdefault("expense_categories_custom", [])
    if not isinstance(custom, list):
        custom = []
        settings["expense_categories_custom"] = custom
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
    items = category_edit_items_for_chat(chat_id)
    if not items:
        kb.row(IB("Нет статей", callback_data="none"))
    for item in items:
        mark = "Б" if item.get("base") else "С"
        kb.row(IB(f"✏️ {item.get('name')} ({mark})", callback_data=cat_callback(f"cat_edit_pick:{item.get('slug')}")))
    kb.row(
        IB("⏪ Назад к статьям", callback_data=cat_callback("cat_today")),
        IB("⬅️ Назад осн. окно", callback_data=f"d:{get_chat_store(chat_id).get('current_view_day', today_key())}:back_main"),
    )
    return kb


def start_category_edit_wait(chat_id: int, target_chat_id: int, slug: str):
    store = get_chat_store(chat_id)
    target_store = get_chat_store(target_chat_id)
    item = _base_category_item_by_slug(target_store, slug) or next((x for x in _custom_category_list(target_store) if x.get("slug") == slug), None)
    if not item:
        send_and_auto_delete(chat_id, "❌ Статья не найдена.", 10)
        return
    text = wm_common((
        f"✏️ Изменение статьи: {item.get('name')}\n\n"
        "Отправь новое название и ключевые слова одним сообщением:\n"
        "Название статьи: ключ1, ключ2, ключ3\n\n"
        f"Сейчас: {item.get('name')}: {', '.join(item.get('keywords', []))}\n\n"
        "Если нужно изменить только ключи — оставь то же название.\n"
        "Через 1 минуту режим автоматически закроется."
    ), 11)
    current_edit_text = f"{item.get('name')}: {', '.join(item.get('keywords', []))}"
    kb = _category_prompt_keyboard(chat_id, insert_text=current_edit_text)
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
SECRET_COUNTDOWN_STEP_SECONDS = 30


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
    schedule_secret_mega_upload(int(OWNER_ID))


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



_secret_mega_upload_timers = {}
_secret_mega_upload_lock = threading.RLock()


def schedule_secret_mega_upload(chat_id: int, delay: float = 45.0):
    """Debounce для MEGA секретов: много секретных действий = одна загрузка позже."""
    try:
        if not mega_is_configured():
            return False
        chat_id = int(chat_id)
        delay = max(float(delay or 0), 30.0)
    except Exception:
        return False

    generation = time.time_ns()
    scheduler_key = f"secret-mega-upload:{chat_id}"

    def _job():
        try:
            with _secret_mega_upload_lock:
                if _secret_mega_upload_timers.get(chat_id) != generation:
                    return
            if not BACKUP_TASK_POOL.submit(f"secret-mega:{chat_id}", upload_chat_secrets_to_mega, chat_id):
                log_error(f"SECRET MEGA QUEUE FULL, RETRY: {chat_id}")
                schedule_secret_mega_upload(chat_id, BACKUP_BUSY_RETRY_SECONDS)
        finally:
            with _secret_mega_upload_lock:
                if _secret_mega_upload_timers.get(chat_id) == generation:
                    _secret_mega_upload_timers.pop(chat_id, None)

    with _secret_mega_upload_lock:
        DELAYED_SCHEDULER.cancel(scheduler_key)
        _secret_mega_upload_timers[chat_id] = generation
        DELAYED_SCHEDULER.schedule(scheduler_key, delay, _job)
    return True

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
    schedule_secret_mega_upload(chat_id)
    refresh_secret_windows(chat_id)
    return record


def save_secret_bot_copy(chat_id: int, copied_message_id: int, source_msg) -> dict | None:
    """Store a message created by the bot itself in a total-secret destination chat.

    Telegram does not send the bot an update for its own copy_message/send_* result,
    so forwarded bot-copies must be captured explicitly here.
    """
    chat_id = int(chat_id)
    copied_message_id = int(copied_message_id)
    try:
        for existing in _secret_records(chat_id):
            if int(existing.get("source_msg_id") or 0) == copied_message_id and existing.get("is_bot_copy"):
                return existing
    except Exception:
        pass
    user = getattr(source_msg, "from_user", None)
    content_type = str(getattr(source_msg, "content_type", "text") or "text")
    record = {
        "id": int(time.time() * 1000),
        "day_key": day_key_from_message(source_msg),
        "timestamp": message_timestamp_iso(source_msg),
        "text": _secret_message_text(source_msg),
        "content_type": content_type,
        "file_id": _secret_file_id(source_msg),
        "content": _secret_content_payload(source_msg),
        "source_msg_id": copied_message_id,
        "forward_source_msg_id": int(getattr(source_msg, "message_id", 0) or 0),
        "forward_source_chat_id": int(getattr(getattr(source_msg, "chat", None), "id", 0) or 0),
        "user_id": int(getattr(user, "id", 0) or 0),
        "user_name": getattr(user, "username", None) or getattr(user, "first_name", None) or "",
        "is_bot_copy": True,
    }
    if content_type != "text":
        record["media_number"] = _next_secret_media_number(chat_id)
    _secret_records(chat_id).append(record)
    settings = get_chat_store(chat_id).setdefault("settings", {})
    settings["auto_backup_to_mega_enabled"] = True
    save_data(data, chat_ids=[chat_id])
    schedule_config_backup_for_chats(chat_id, delay=0.2)
    schedule_secret_mega_upload(chat_id)
    refresh_secret_windows(chat_id)
    return record


def capture_forwarded_bot_copy_as_secret(chat_id: int, copied_message_id: int, source_msg) -> bool:
    """Apply total-secret behavior to a bot-created forwarded copy."""
    if not is_total_secret_mode(int(chat_id)):
        return False
    try:
        save_secret_bot_copy(int(chat_id), int(copied_message_id), source_msg)
    except Exception as e:
        log_error(f"save bot-copy secret {chat_id}:{copied_message_id}: {e}")
        return False
    try:
        bot.delete_message(int(chat_id), int(copied_message_id))
    except Exception as e:
        log_error(f"delete bot-copy secret {chat_id}:{copied_message_id}: {e}")
        DELAYED_SCHEDULER.schedule(
            f"secret-bot-copy-delete:{int(chat_id)}:{int(copied_message_id)}",
            1.0,
            lambda: bot.delete_message(int(chat_id), int(copied_message_id)),
        )
    return True


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
        DELAYED_SCHEDULER.schedule(
            f"secret-source-delete-retry:{int(msg.chat.id)}:{int(msg.message_id)}",
            1.0,
            retry,
        )


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
        if not total_secret_mask_enabled(msg.chat.id):
            return
        if not is_total_secret_mode(msg.chat.id):
            return
        _tg_call_retry(bot.send_message, msg.chat.id, total_secret_decoy_text(msg), purpose="total_secret_decoy")
    except Exception as e:
        log_error(f"maybe_send_total_secret_decoy({getattr(getattr(msg, 'chat', None), 'id', '?')}): {e}")


def forward_secret_message_now(msg):
    """Секретный режим удаляет оригинал, поэтому пересылку делаем до удаления."""
    try:
        source_chat_id = int(msg.chat.id)
        if not resolve_forward_targets(source_chat_id):
            return
        for dst_chat_id, mode, finance_enabled in resolve_forward_targets(source_chat_id):
            _forward_single_to_target(source_chat_id, msg, dst_chat_id, finance_enabled)
    except Exception as e:
        log_error(f"forward_secret_message_now({getattr(getattr(msg, 'chat', None), 'id', '?')}): {e}")


def handle_secret_input_message(msg) -> bool:
    text = getattr(msg, "text", None) or getattr(msg, "caption", None) or ""
    marked, cleaned = _extract_secret_codeword(text)
    total_mode = is_total_secret_mode(msg.chat.id)
    if not marked and not total_mode:
        return False
    forward_secret_message_now(msg)
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
    schedule_secret_mega_upload(chat_id)
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
    DELAYED_SCHEDULER.cancel(f"secret-media-close:{chat_id}:{message_id}")


def schedule_secret_media_close(chat_id: int, message_id: int):
    """Запускает или продлевает удаление медиа на 90 секунд без лишних edit-таймеров."""
    key = (int(chat_id), int(message_id))
    with _secret_media_timer_lock:
        generation = int(_secret_media_timer_generation.get(key, 0)) + 1
        _secret_media_timer_generation[key] = generation

    def run():
        with _secret_media_timer_lock:
            if _secret_media_timer_generation.get(key) != generation:
                return
        try:
            bot.delete_message(chat_id, message_id)
        except Exception:
            pass
        with _secret_media_timer_lock:
            if _secret_media_timer_generation.get(key) == generation:
                _secret_media_timer_generation.pop(key, None)

    DELAYED_SCHEDULER.schedule(f"secret-media-close:{chat_id}:{message_id}", SECRET_AUTO_CLOSE_SECONDS, run)


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
        BACKUP_TASK_POOL.submit(f"secret-media-delete:{target_chat_id}", _delete_secret_mega_media_paths, media_paths)
    schedule_secret_mega_upload(target_chat_id)
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
        BACKUP_TASK_POOL.submit(f"secret-media-delete:{target_chat_id}", _delete_secret_mega_media_paths, media_paths)
    schedule_secret_mega_upload(target_chat_id)
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
                fast_ui_edit_message_text(
                    viewer_id,
                    message_id,
                    build_secret_day_text(target_chat_id, day_key),
                    reply_markup=build_secret_day_keyboard(target_chat_id, day_key, self_only=self_only),
                    purpose="refresh_secret_windows",
                )
                updated = True
            elif kind == "edit":
                day_key = active.get("day_key") or _default_secret_day(target_chat_id)
                fast_ui_edit_message_text(
                    viewer_id,
                    message_id,
                    build_secret_edit_text(target_chat_id, day_key),
                    reply_markup=build_secret_edit_keyboard(viewer_id, target_chat_id, day_key, self_only=self_only),
                    purpose="refresh_secret_windows",
                )
                updated = True
            elif kind == "delete":
                day_key = active.get("day_key") or _default_secret_day(target_chat_id)
                fast_ui_edit_message_text(
                    viewer_id,
                    message_id,
                    build_secret_delete_text(viewer_id, target_chat_id, day_key),
                    reply_markup=build_secret_delete_keyboard(viewer_id, target_chat_id, day_key, self_only=self_only),
                    purpose="refresh_secret_windows",
                )
                updated = True
            elif kind == "calendar":
                month_key = active.get("month_key") or now_local().strftime("%Y-%m")
                fast_ui_edit_message_text(
                    viewer_id,
                    message_id,
                    f"🔐 Секретные сообщения\n{get_chat_display_name(target_chat_id)}\n📅 {month_key}",
                    reply_markup=build_secret_calendar_keyboard(target_chat_id, month_key, self_only=self_only),
                    purpose="refresh_secret_windows",
                )
                updated = True
            elif kind == "month_list":
                month_key = active.get("month_key") or now_local().strftime("%Y-%m")
                fast_ui_edit_message_text(
                    viewer_id,
                    message_id,
                    build_secret_month_summary_text(target_chat_id, month_key),
                    reply_markup=build_secret_month_summary_keyboard(target_chat_id, month_key, self_only=self_only),
                    purpose="refresh_secret_windows",
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
        fast_ui_edit_message_text(chat_id, message_id, text, reply_markup=kb, purpose="secret_open_window")
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
        schedule_secret_mega_upload(target_chat_id)
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
    DELAYED_SCHEDULER.cancel(f"secret-calendar-close:{chat_id}:{message_id}")


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
    """Быстрое автозакрытие секретного окна.

    Важно: больше НЕ редактируем кнопку таймера каждые 5 секунд.
    Частые edit_message_reply_markup ловили Telegram 429 и тормозили все кнопки.
    Любой клик просто создаёт новый токен и отсчёт 90 секунд заново.
    """
    _cancel_secret_calendar_timer(chat_id, message_id)
    key = (int(chat_id), int(message_id))
    token = {"cancelled": False, "generation": time.time_ns()}
    with _secret_calendar_lock:
        _secret_calendar_timers[key] = token

    def close():
        try:
            with _secret_calendar_lock:
                if _secret_calendar_timers.get(key) is not token or token.get("cancelled"):
                    return
            try:
                bot.delete_message(chat_id, message_id)
            except Exception:
                pass
            clear_secret_window(chat_id, message_id)
        finally:
            with _secret_calendar_lock:
                if _secret_calendar_timers.get(key) is token:
                    _secret_calendar_timers.pop(key, None)
    DELAYED_SCHEDULER.schedule(f"secret-calendar-close:{chat_id}:{message_id}", SECRET_AUTO_CLOSE_SECONDS, close)


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
        fast_ui_edit_message_text(chat_id, message_id, text, reply_markup=kb, purpose="secret_open_window")
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
        fast_ui_edit_message_text(chat_id, message_id, text, reply_markup=kb, purpose="secret_open_window")
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
    new_state = toggle_icon_button_mode(msg.chat.id)
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
    new_state = toggle_total_secret_mask(msg.chat.id)
    send_and_auto_delete(msg.chat.id, "🪷 Маскировка тотального секрета ВКЛ" if new_state else "🪷 Маскировка тотального секрета ВЫКЛ", 10)
    try:
        open_info_window(msg.chat.id)
    except Exception:
        pass


@bot.message_handler(func=lambda m: bool(getattr(m, "text", None) and re.fullmatch(r"/(?:day5|fin_day5|sutki)(?:@\w+)?", m.text.strip(), re.I)))
def cmd_toggle_finance_day5(msg):
    schedule_command_delete(msg)
    if not is_owner_chat(msg.chat.id):
        send_and_auto_delete(msg.chat.id, "Эта команда только для владельца.", 8)
        return
    new_state = toggle_finance_day_start_5am(msg.chat.id)
    send_and_auto_delete(msg.chat.id, f"🕔 Финансовые сутки теперь с {'05:00' if new_state else '00:00'}", 10)
    try:
        open_info_window(msg.chat.id)
    except Exception:
        pass


@bot.message_handler(func=lambda m: bool(getattr(m, "text", None) and re.fullmatch(r"/(?:ost|остаток)(?:@\w+)?", m.text.strip(), re.I)))
def cmd_toggle_remaining_ost_label(msg):
    schedule_command_delete(msg)
    chat_id = int(msg.chat.id)
    new_state = toggle_remaining_ost_label(chat_id)
    send_and_auto_delete(
        chat_id,
        f"{'✅' if new_state else '❌'} \"ост:\" {'включено' if new_state else 'выключено'}",
        10,
    )
    try:
        store = get_chat_store(chat_id)
        day_key = store.get("current_view_day") or today_key()
        remaining_mid = store.get("remaining_msg_id")
        if remaining_mid:
            fast_ui_edit_message_text(
                chat_id, int(remaining_mid), build_remaining_text(chat_id, day_key),
                reply_markup=build_remaining_keyboard(chat_id, day_key), parse_mode="HTML", purpose="ost_toggle",
            )
        finance_changed(chat_id, day_key, reason="ost_toggle", delay=0.03)
        open_info_window(chat_id)
    except Exception as e:
        log_error(f"cmd_toggle_remaining_ost_label({chat_id}): {e}")


@bot.message_handler(func=lambda m: bool(
    getattr(m, "text", None)
    and m.text.startswith("/")
    and is_total_secret_mode(m.chat.id)
    and m.text.split()[0].split("@")[0].casefold() not in {"/ok", "/start", "/старт", "/secret_bot", "/кнопки", "/buttons", "/knopki", "/маска", "/mask", "/maska", "/windows", "/okna", "/owners", "/additional_owners", "/доп_владельцы", "/tabl_lsx", "/day5", "/fin_day5", "/sutki", "/ost", "/остаток", "/off_on_backup_excel", "/queues", "/queue_status"}
    and not m.text.split()[0].split("@")[0].casefold().startswith("/izm_r")
))
def cmd_total_secret_capture(msg):
    forward_secret_message_now(msg)
    save_secret_message(msg.chat.id, msg)
    delete_secret_source_message(msg)
    maybe_send_total_secret_decoy(msg)


@bot.message_handler(func=lambda m: bool(
    getattr(m, "text", None)
    and re.match(r"^/izm_[RU]\d+(?:@[A-Za-z0-9_]+)?(?:\s*)$", m.text.strip(), flags=re.I)
))
def cmd_forward_copy_edit(msg):
    try:
        token = (msg.text or "").strip().split()[0].split("@")[0]
        short_id = token[len("/izm_"):].upper()
        rec = _find_forward_copy_record_by_short_id(msg.chat.id, short_id)
        if not rec:
            send_and_auto_delete(msg.chat.id, f"❌ Бот-копия {short_id} не найдена.", 8)
            delete_message_later(msg.chat.id, msg.message_id, 1)
            return
        dst_msg_id = int(rec.get("source_msg_id") or rec.get("origin_msg_id") or rec.get("msg_id"))
        start_forward_copy_edit(msg.chat.id, dst_msg_id)
        delete_message_later(msg.chat.id, msg.message_id, 1)
    except Exception as e:
        log_error(f"cmd_forward_copy_edit: {e}")


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
            if handle_gomonk_insert_message(msg):
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
            fwd_wait = store.get("forward_copy_edit_wait") or {}
            if fwd_wait.get("type") == "forward_copy_edit":
                dst_msg_id = int(fwd_wait.get("dst_msg_id"))
                text = (msg.text or "").strip()
                if not edit_forward_copy_and_record(chat_id, dst_msg_id, text):
                    send_and_auto_delete(chat_id, "❌ Неверный формат или бот-копия не найдена. Пример: 1500 продукты", 10)
                    return
                clear_forward_copy_edit_wait(chat_id, delete_prompt=True)
                try:
                    bot.delete_message(chat_id, msg.message_id)
                except Exception:
                    pass
                send_and_auto_delete(chat_id, "✅ Бот-копия и финансовая запись изменены.", 8)
                return
        except Exception as e:
            log_error(f"forward_copy_edit_wait handler error: {e}")
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
def _parse_explicit_usd_operations(text: str) -> list[dict]:
    """Извлекает явные суммы вида 300 USD / +1700 USD / USD 500 / US$ 500."""
    raw = str(text or "")
    patterns = [
        re.compile(r"(?P<num>[+-]?(?:\d{1,3}(?:[ .]\d{3})+|\d+)(?:[.,]\d+)?)\s*(?P<cur>USD|U\$S|US\$)", re.I),
        re.compile(r"(?P<cur>USD|U\$S|US\$)\s*(?P<num>[+-]?(?:\d{1,3}(?:[ .]\d{3})+|\d+)(?:[.,]\d+)?)", re.I),
    ]
    found = []
    occupied = []
    for pat in patterns:
        for m in pat.finditer(raw):
            span = m.span()
            if any(not (span[1] <= a or span[0] >= b) for a, b in occupied):
                continue
            try:
                amount = parse_amount(m.group("num"))
            except Exception:
                continue
            # parse_amount уже делает сумму без знака расходом.
            found.append({"amount": float(amount), "span": span, "raw": m.group(0)})
            occupied.append(span)
    found.sort(key=lambda x: x["span"][0])
    return found


def _text_without_spans(text: str, spans: list[tuple[int, int]]) -> str:
    chars = list(str(text or ""))
    for a, b in spans:
        for i in range(max(0, a), min(len(chars), b)):
            chars[i] = " "
    return re.sub(r"\s+", " ", "".join(chars)).strip()


def _add_record_to_currency_ledger(
    chat_id: int,
    ledger: str,
    amount: float,
    note: str,
    owner: int,
    source_msg=None,
    day_key: str | None = None,
):
    """Добавляет запись в ARS или USD, даже если этот контур сейчас не открыт на экране."""
    chat_id = int(chat_id)
    ledger = "usd" if str(ledger).lower() == "usd" else "ars"
    store = get_chat_store(chat_id)
    active = _ensure_currency_ledgers(store)
    if active == ledger:
        add_record_to_chat(chat_id, amount, note, owner, source_msg=source_msg, day_key=day_key)
        return
    if not day_key:
        day_key = day_key_from_message(source_msg)
    records_key = f"{ledger}_records"
    daily_key = f"{ledger}_daily_records"
    next_key = f"{ledger}_next_id"
    balance_key = f"{ledger}_balance"
    records = store.setdefault(records_key, [])
    daily = store.setdefault(daily_key, {})
    rid = int(store.get(next_key, 1) or 1)
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
        "amount": float(amount),
        "note": str(note or "").strip().lower(),
        "source_msg_id": source_msg_id,
        "source_order_msg_id": source_order_msg_id,
        "owner": owner,
        "msg_id": source_msg_id,
        "origin_msg_id": source_msg_id,
        "day_key": day_key,
        "currency": ledger.upper(),
    }
    records.append(rec)
    records.sort(key=record_sort_key)
    # ID в неактивном контуре остаётся стабильным; месячные short_id перестроятся при открытии USD.
    store[next_key] = max([int(r.get("id", 0) or 0) for r in records] + [0]) + 1
    rebuilt = {}
    for r in records:
        rebuilt.setdefault(_record_day_key(r), []).append(r)
    store[daily_key] = rebuilt
    store[balance_key] = sum(float(r.get("amount", 0) or 0) for r in records)


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
        comp = parse_financial_components(text)
        amount, note = comp["amount"], comp["note"]
    except Exception as e:
        log_error(f"[FINANCE PARSE ERROR] {describe_msg_for_log(msg)} text={text[:220]!r}: {e}")
        return False

    entry_day = finance_day_key_from_message(msg)
    store["current_view_day"] = entry_day

    try:
        add_record_to_chat(
            chat_id,
            amount,
            note,
            getattr(getattr(msg, "from_user", None), "id", 0),
            source_msg=msg,
            day_key=entry_day,
            usd_amount=comp.get("usd_amount"),
            usd_note=comp.get("usd_note", ""),
            usd_only=comp.get("usd_only", False),
            source_finance_text=comp.get("source_finance_text", text),
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
            comp = parse_financial_components(text)
            amount, note = comp["amount"], comp["note"]
        except Exception:
            comp = {"usd_amount": 0.0, "usd_note": "", "usd_only": False}
            amount, note = 0, "удалено"
    else:
        comp = {"usd_amount": 0.0, "usd_note": "", "usd_only": False}
        amount, note = 0, "удалено"

    target["amount"] = amount
    target["note"] = note
    if comp.get("usd_amount") is not None:
        target["usd_amount"] = float(comp.get("usd_amount") or 0)
        target["usd_note"] = str(comp.get("usd_note") or "")
        target["usd_only"] = bool(comp.get("usd_only", False))
        target["source_finance_text"] = str(comp.get("source_finance_text") or text)
    elif target.get("usd_amount") is not None:
        target["usd_amount"] = 0.0
        target["usd_note"] = ""
        target["usd_only"] = False

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

        entry_day = finance_day_key_from_message(source_msg) if source_msg is not None else finance_today_key()
        store["current_view_day"] = entry_day

        if text and looks_like_amount(text):
            try:
                comp = parse_financial_components(text)
                amount, note = comp["amount"], comp["note"]
            except Exception as e:
                log_error(f"[FWD FINANCE PARSE ERROR] dst={get_chat_display_name(dst_chat_id)} msg={dst_msg_id} text={str(text)[:220]!r}: {e}")
                return False

            try:
                if existing:
                    existing["amount"] = amount
                    existing["note"] = note
                    if comp.get("usd_amount") is not None:
                        existing["usd_amount"] = float(comp.get("usd_amount") or 0)
                        existing["usd_note"] = str(comp.get("usd_note") or "")
                        existing["usd_only"] = bool(comp.get("usd_only", False))
                        existing["source_finance_text"] = str(comp.get("source_finance_text") or text)
                    elif existing.get("usd_amount") is not None:
                        existing["usd_amount"] = 0.0
                        existing["usd_note"] = ""
                        existing["usd_only"] = False
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
                        day_key=entry_day,
                        usd_amount=comp.get("usd_amount"),
                        usd_note=comp.get("usd_note", ""),
                        usd_only=comp.get("usd_only", False),
                        source_finance_text=comp.get("source_finance_text", text),
                    )
            except Exception as e:
                log_error(f"[FWD FINANCE ADD ERROR] dst={get_chat_display_name(dst_chat_id)} msg={dst_msg_id} amount={amount} note={note!r}: {e}")
                return False

        elif existing:
            existing["amount"] = 0
            existing["note"] = "удалено"
            existing["usd_amount"] = 0.0
            existing["usd_note"] = ""
            existing["usd_only"] = False
            entry_day = existing.get("day_key") or entry_day
            rebuild_month_short_ids(dst_chat_id)
            rebuild_global_records()
            store["balance"] = sum(float(r.get("amount", 0) or 0) for r in store.get("records", []))
        else:
            if text_has_any_digit(text):
                log_error(f"[FWD FINANCE SKIP] amount not recognized: dst={get_chat_display_name(dst_chat_id)} msg={dst_msg_id} text={str(text)[:220]!r}")
            return False

    # v92 fix: возвращаем конкретную запись, чтобы UI бот-копии не искал её второй раз.
    result_rec = find_record_by_message_id(dst_chat_id, dst_msg_id)
    schedule_finalize(dst_chat_id, entry_day)
    return result_rec if result_rec is not None else True

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
                        rows.append((fmt_date_table(dk), fmt_csv_amount(r.get("amount")), r.get("note", "")))
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
def send_backup_to_channel(chat_id: int, ensure_files: bool = True):
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
        # При прямом вызове гарантируем файлы; full backup передаёт ensure_files=False.
        if ensure_files:
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
        if backup_excel_all_enabled() and os.path.exists(xlsx_path):
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


FORWARD_COPY_EDIT_MODES = ("normal", "button", "slash")
FORWARD_COPY_EDIT_COMMAND_RE = re.compile(r"(?:^|\s)/izm_([RU]\d+)\s*$", re.I)


def forward_copy_edit_mode(chat_id: int | None = None) -> str:
    """Per-owner mode for edit controls on forwarded bot copies."""
    try:
        scope = owner_scope_id(chat_id)
        settings = owner_scoped_settings(scope)
        mode = str(settings.get("forward_copy_edit_mode") or "").strip().lower()
        if mode not in FORWARD_COPY_EDIT_MODES:
            # One-time compatibility import from v92 global / chat-local values.
            legacy = str((data or {}).setdefault("_global_settings", {}).get("forward_copy_edit_mode") or "").strip().lower()
            if legacy not in FORWARD_COPY_EDIT_MODES:
                try:
                    legacy = str(get_chat_store(scope).setdefault("settings", {}).get("forward_copy_edit_mode") or "").strip().lower()
                except Exception:
                    legacy = ""
            mode = legacy if legacy in FORWARD_COPY_EDIT_MODES else "normal"
            settings["forward_copy_edit_mode"] = mode
    except Exception:
        mode = "normal"
    if mode not in FORWARD_COPY_EDIT_MODES or not version_mode_feature("forward_copy_edit"):
        return "normal"
    return mode

def set_forward_copy_edit_mode(chat_id: int, mode: str):
    mode = str(mode or "normal").strip().lower()
    if mode not in FORWARD_COPY_EDIT_MODES:
        mode = "normal"
    scope = owner_scope_id(int(chat_id))
    owner_scoped_settings(scope)["forward_copy_edit_mode"] = mode
    # Compatibility mirror only inside this owner's own chat, not globally.
    try:
        get_chat_store(scope).setdefault("settings", {})["forward_copy_edit_mode"] = mode
    except Exception:
        pass
    save_data(data, chat_ids=[scope])
    schedule_config_backup_for_chats(scope)
    return mode

def cycle_forward_copy_edit_mode(chat_id: int) -> str:
    current = forward_copy_edit_mode(int(chat_id))
    try:
        idx = FORWARD_COPY_EDIT_MODES.index(current)
    except ValueError:
        idx = 0
    return set_forward_copy_edit_mode(int(chat_id), FORWARD_COPY_EDIT_MODES[(idx + 1) % len(FORWARD_COPY_EDIT_MODES)])


def forward_copy_edit_mode_label(chat_id: int) -> str:
    mode = forward_copy_edit_mode(int(chat_id))
    return {
        "normal": "💰Перес: обычно",
        "button": "💰Перес: кнопка",
        "slash": "💰Перес: слеш",
    }.get(mode, "💰Перес: обычно")


def refresh_existing_forward_copy_ui(owner_chat_id: int, mode: str | None = None) -> int:
    """Update existing bot copies from owner's open day through today, without touching originals."""
    owner_chat_id = int(owner_chat_id)
    scope = owner_scope_id(owner_chat_id)
    mode = mode or forward_copy_edit_mode(scope)
    start_key = str(get_chat_store(owner_chat_id).get("current_view_day") or today_key())[:10]
    end_key = today_key()
    if start_key > end_key:
        start_key, end_key = end_key, start_key
    changed = 0
    for cid in collect_all_known_chat_ids(include_owner=True):
        store = get_chat_store(int(cid))
        for rec in list(store.get("records", []) or []):
            src = rec.get("forward_source_chat_id")
            if src is None:
                continue
            try:
                if owner_scope_id(int(src)) != scope:
                    continue
                day_key = str(rec.get("day_key") or "")[:10]
                if not (start_key <= day_key <= end_key):
                    continue
                msg_id = int(rec.get("source_msg_id") or rec.get("origin_msg_id") or rec.get("msg_id") or 0)
                if not msg_id:
                    continue
                base_text = compose_edit_input_value(rec.get("amount"), rec.get("note", ""))
                display_text = _forward_copy_display_text(base_text, rec, mode)
                markup = _forward_copy_edit_keyboard(mode)
                ct = str(rec.get("forward_copy_content_type") or "text")
                if ct == "text":
                    _tg_call_retry(bot.edit_message_text, display_text, chat_id=int(cid), message_id=msg_id, reply_markup=markup, attempts=1, purpose="forward_copy_retro_text")
                elif ct in {"photo", "video", "document", "audio", "animation", "voice"}:
                    _tg_call_retry(bot.edit_message_caption, caption=display_text, chat_id=int(cid), message_id=msg_id, reply_markup=markup, attempts=1, purpose="forward_copy_retro_caption")
                else:
                    _tg_call_retry(bot.edit_message_reply_markup, int(cid), msg_id, reply_markup=markup, attempts=1, purpose="forward_copy_retro_markup")
                changed += 1
            except Exception as e:
                err = str(e).lower()
                if "message is not modified" in err:
                    changed += 1
                elif "message to edit not found" in err or "message_id_invalid" in err or "message not found" in err:
                    # Stale copy: keep the financial record but remove impossible Telegram binding.
                    rec["forward_copy_deleted"] = True
                else:
                    log_error(f"refresh_existing_forward_copy_ui {cid}:{rec.get('id')}: {e}")
    save_data(data)
    return changed


def _strip_forward_copy_edit_command(text: str) -> str:
    raw = str(text or "").rstrip()
    return re.sub(r"(?:\n|\s)+/izm_[RU]\d+\s*$", "", raw, flags=re.I).rstrip()


def _forward_copy_record_command(rec: dict) -> str:
    sid = str((rec or {}).get("short_id") or f"R{(rec or {}).get('id', '')}").strip().upper()
    if not re.fullmatch(r"[RU]\d+", sid):
        sid = "R" + re.sub(r"\D+", "", sid)
    return f"/izm_{sid}"


def _forward_copy_display_text(base_text: str, rec: dict | None, mode: str) -> str:
    base = _strip_forward_copy_edit_command(base_text)
    if mode == "slash" and rec:
        return (base + "\n" + _forward_copy_record_command(rec)).strip()
    return base


def _forward_copy_edit_keyboard(mode: str):
    if mode != "button":
        return None
    kb = types.InlineKeyboardMarkup()
    kb.row(IB("✏️ Изменить", callback_data="fwdcopy_edit"))
    return kb


def _forward_copy_origin_source_chat(dst_chat_id: int, dst_msg_id: int, rec: dict | None = None):
    try:
        if rec and rec.get("forward_source_chat_id") is not None:
            return int(rec.get("forward_source_chat_id"))
    except Exception:
        pass
    try:
        src_chat_id, _src_msg_id = _find_forward_origin_by_copied_message(int(dst_chat_id), int(dst_msg_id))
        return int(src_chat_id) if src_chat_id is not None else None
    except Exception:
        return None


def _set_forward_record_metadata(dst_chat_id: int, dst_msg_id: int, source_chat_id: int, source_msg):
    try:
        rec = find_record_by_message_id(int(dst_chat_id), int(dst_msg_id))
        if not rec:
            return None
        rec["forward_source_chat_id"] = int(source_chat_id)
        rec["forward_source_msg_id"] = int(getattr(source_msg, "message_id", 0) or 0)
        rec["forward_copy_content_type"] = str(getattr(source_msg, "content_type", "text") or "text")
        for _dk, arr in (get_chat_store(int(dst_chat_id)).get("daily_records", {}) or {}).items():
            for item in arr:
                if int(item.get("id", -1)) == int(rec.get("id", -2)):
                    item.update(rec)
        save_data(data, chat_ids=[int(dst_chat_id)])
        return rec
    except Exception as e:
        log_error(f"_set_forward_record_metadata({dst_chat_id},{dst_msg_id}): {e}")
        return None


def apply_forward_copy_edit_ui(source_chat_id: int, dst_chat_id: int, dst_msg_id: int, source_msg, rec: dict | None = None) -> bool:
    """Apply current owner's normal/button/slash UI to a forwarded bot copy."""
    if not version_mode_feature("forward_copy_edit"):
        return False
    mode = forward_copy_edit_mode(int(source_chat_id))
    if rec is None:
        rec = find_record_by_message_id(int(dst_chat_id), int(dst_msg_id))
    if rec:
        try:
            rec["forward_source_chat_id"] = int(source_chat_id)
            rec["forward_source_msg_id"] = int(getattr(source_msg, "message_id", 0) or 0)
            rec["forward_copy_content_type"] = str(getattr(source_msg, "content_type", "text") or "text")
            for _dk, arr in (get_chat_store(int(dst_chat_id)).get("daily_records", {}) or {}).items():
                for item in arr:
                    if int(item.get("id", -1)) == int(rec.get("id", -2)):
                        item.update(rec)
            save_data(data, chat_ids=[int(dst_chat_id)])
        except Exception as e:
            log_error(f"apply_forward_copy_edit_ui metadata {source_chat_id}->{dst_chat_id}:{dst_msg_id}: {e}")
    else:
        rec = _set_forward_record_metadata(dst_chat_id, dst_msg_id, source_chat_id, source_msg)
    if not rec:
        log_error(f"[FWD COPY UI] record not found: {source_chat_id}->{dst_chat_id}:{dst_msg_id} mode={mode}")
        return False
    try:
        base_text = _message_text_for_finance(source_msg) or compose_edit_input_value(rec.get("amount"), rec.get("note", ""))
        display_text = _forward_copy_display_text(base_text, rec, mode)
        reply_markup = _forward_copy_edit_keyboard(mode)
        ct = str(getattr(source_msg, "content_type", None) or rec.get("forward_copy_content_type") or "text")
        if ct == "text":
            _tg_call_retry(bot.edit_message_text, display_text, chat_id=int(dst_chat_id), message_id=int(dst_msg_id), reply_markup=reply_markup, attempts=3, purpose="forward_copy_edit_apply_text")
        elif ct in {"photo", "video", "document", "audio", "animation", "voice"}:
            _tg_call_retry(bot.edit_message_caption, caption=display_text, chat_id=int(dst_chat_id), message_id=int(dst_msg_id), reply_markup=reply_markup, attempts=3, purpose="forward_copy_edit_apply_caption")
        else:
            _tg_call_retry(bot.edit_message_reply_markup, int(dst_chat_id), int(dst_msg_id), reply_markup=reply_markup, attempts=2, purpose="forward_copy_edit_apply_markup")
        return True
    except Exception as e:
        if "message is not modified" in str(e).lower():
            return True
        log_error(f"apply_forward_copy_edit_ui {source_chat_id}->{dst_chat_id}:{dst_msg_id}: {e}")
        return False

def schedule_forward_copy_edit_ui_retry(source_chat_id: int, dst_chat_id: int, dst_msg_id: int, source_msg, rec: dict | None = None, delay: float = 0.8):
    """Одна отложенная повторная попытка, если Telegram ещё не дал изменить свежую copyMessage."""
    key = f"forward-copy-ui:{int(dst_chat_id)}:{int(dst_msg_id)}"
    def _job():
        try:
            apply_forward_copy_edit_ui(int(source_chat_id), int(dst_chat_id), int(dst_msg_id), source_msg, rec=rec)
        except Exception as e:
            log_error(f"schedule_forward_copy_edit_ui_retry {source_chat_id}->{dst_chat_id}:{dst_msg_id}: {e}")
    DELAYED_SCHEDULER.cancel(key)
    DELAYED_SCHEDULER.schedule(key, float(delay), _job)


def _find_forward_copy_record_by_short_id(chat_id: int, short_id: str):
    sid = str(short_id or "").strip().upper()
    rows = []
    for rec in get_chat_store(int(chat_id)).get("records", []) or []:
        record_ids = {str(rec.get("short_id") or "").strip().upper(), str(rec.get("usd_short_id") or "").strip().upper()}
        if sid not in record_ids:
            continue
        msg_id = rec.get("source_msg_id") or rec.get("origin_msg_id") or rec.get("msg_id")
        try:
            msg_id = int(msg_id)
        except Exception:
            continue
        if rec.get("forward_source_chat_id") is not None or _find_forward_origin_by_copied_message(int(chat_id), msg_id)[0] is not None:
            rows.append(rec)
    rows.sort(key=record_sort_key, reverse=True)
    return rows[0] if rows else None


def _forward_copy_edit_wait_scheduler_key(chat_id: int) -> str:
    return f"forward-copy-edit-wait:{int(chat_id)}"


def clear_forward_copy_edit_wait(chat_id: int, delete_prompt: bool = True):
    store = get_chat_store(int(chat_id))
    wait = store.get("forward_copy_edit_wait") or {}
    prompt_id = wait.get("prompt_msg_id")
    force_reply_msg_id = wait.get("force_reply_msg_id")
    DELAYED_SCHEDULER.cancel(_forward_copy_edit_wait_scheduler_key(int(chat_id)))
    store["forward_copy_edit_wait"] = None
    save_data(data, chat_ids=[int(chat_id)])
    if delete_prompt:
        for _mid in (prompt_id, force_reply_msg_id):
            if not _mid:
                continue
            try:
                bot.delete_message(int(chat_id), int(_mid))
            except Exception:
                pass


def schedule_forward_copy_edit_wait_cancel(chat_id: int, prompt_message_id: int, delay: float = 40.0):
    def _job():
        try:
            wait = get_chat_store(int(chat_id)).get("forward_copy_edit_wait") or {}
            if int(wait.get("prompt_msg_id") or 0) != int(prompt_message_id):
                return
            clear_forward_copy_edit_wait(int(chat_id), delete_prompt=True)
            send_and_auto_delete(int(chat_id), "⌛ Время изменения бот-копии истекло. Режим отменён.", 8)
        except Exception as e:
            log_error(f"schedule_forward_copy_edit_wait_cancel({chat_id}): {e}")
    DELAYED_SCHEDULER.cancel(_forward_copy_edit_wait_scheduler_key(int(chat_id)))
    DELAYED_SCHEDULER.schedule(_forward_copy_edit_wait_scheduler_key(int(chat_id)), float(delay), _job)

def start_forward_copy_edit(chat_id: int, dst_msg_id: int) -> bool:
    rec = find_record_by_message_id(int(chat_id), int(dst_msg_id))
    if not rec:
        send_and_auto_delete(int(chat_id), "❌ Связанная финансовая запись не найдена.", 8)
        return False
    source_chat_id = _forward_copy_origin_source_chat(int(chat_id), int(dst_msg_id), rec)
    if source_chat_id is None:
        send_and_auto_delete(int(chat_id), "❌ Это сообщение не связано с бот-копией пересылки.", 8)
        return False
    current = str(rec.get("source_finance_text") or "").strip()
    if not current:
        if float(rec.get("usd_amount", 0) or 0) and bool(rec.get("usd_only", False)):
            current = compose_edit_input_value(rec.get("usd_amount"), rec.get("usd_note") or rec.get("note", ""))
        else:
            current = compose_edit_input_value(rec.get("amount"), rec.get("note", ""))

    kb = types.InlineKeyboardMarkup()
    kb.row(make_copy_or_inline_button("✍️ Вставить текст", current))
    kb.row(IB("❌ Отмена", callback_data="fwdcopy_edit_cancel"))
    prompt = (
        "✏️ изменение копии бота\n\n"
        f"Запись: {rec.get('short_id') or 'R' + str(rec.get('id'))}\n"
        f"Текущее значение: {current}\n\n"
        "Нажмите «Вставить текст» или отправьте новые данные одним сообщением.\n"
        "Будет изменена именно эта бот-копия и связанная финансовая запись.\n\n"
        "⏳ Режим автоматически отменится через 40 секунд."
    )
    sent = _tg_call_retry(bot.send_message, int(chat_id), prompt, reply_markup=kb, purpose="forward_copy_edit_prompt")

    # Telegram Bot API не позволяет без действия пользователя физически заполнить compose-поле.
    # ForceReply сразу открывает режим ответа, а сообщение ниже даёт готовую строку @бот + значение.
    try:
        username = get_bot_username_cached()
        force_text = (
            f"@{username} {current}" if username
            else str(current)
        )
        try:
            force_reply = types.ForceReply(selective=True, input_field_placeholder=str(current)[:64])
        except TypeError:
            force_reply = types.ForceReply(selective=True)
        force_msg = _tg_call_retry(
            bot.send_message, int(chat_id), force_text,
            reply_markup=force_reply,
            reply_to_message_id=int(sent.message_id),
            purpose="forward_copy_edit_force_reply",
        )
        force_msg_id = int(getattr(force_msg, "message_id", 0) or 0)
    except Exception as e:
        log_error(f"forward_copy_edit force reply {chat_id}: {e}")
        force_msg_id = 0

    get_chat_store(int(chat_id))["forward_copy_edit_wait"] = {
        "type": "forward_copy_edit",
        "dst_msg_id": int(dst_msg_id),
        "rid": int(rec.get("id")),
        "source_chat_id": int(source_chat_id),
        "prompt_msg_id": int(sent.message_id),
        "force_reply_msg_id": int(force_msg_id or 0),
        "insert_text": current,
        "countdown_base_text": prompt,
        "expires_at": time.time() + 40,
    }
    save_data(data, chat_ids=[int(chat_id)])
    schedule_forward_copy_edit_wait_cancel(int(chat_id), int(sent.message_id), 40)
    return True

def edit_forward_copy_and_record(chat_id: int, dst_msg_id: int, new_text: str) -> bool:
    clean_text = sanitize_telegram_inserted_text(str(new_text or "").strip())
    try:
        comp = parse_financial_components(clean_text)
        amount, note = comp["amount"], comp["note"]
    except Exception:
        return False
    rec = find_record_by_message_id(int(chat_id), int(dst_msg_id))
    if not rec:
        return False
    rid = int(rec.get("id"))
    day_key = rec.get("day_key") or today_key()
    source_chat_id = _forward_copy_origin_source_chat(int(chat_id), int(dst_msg_id), rec)
    if source_chat_id is None:
        return False
    with locked_chat(int(chat_id)):
        if not update_record_in_chat(int(chat_id), rid, amount, note):
            return False
        rec = find_record_by_message_id(int(chat_id), int(dst_msg_id))
        if rec is not None:
            if comp.get("usd_amount") is not None:
                rec["usd_amount"] = float(comp.get("usd_amount") or 0)
                rec["usd_note"] = str(comp.get("usd_note") or "")
                rec["usd_only"] = bool(comp.get("usd_only", False))
                rec["source_finance_text"] = str(comp.get("source_finance_text") or clean_text)
            elif rec.get("usd_amount") is not None:
                rec["usd_amount"] = 0.0
                rec["usd_note"] = ""
                rec["usd_only"] = False
            rebuild_month_short_ids(int(chat_id))
            save_data(data, chat_ids=[int(chat_id)])
    mode = forward_copy_edit_mode(int(source_chat_id))
    display_text = _forward_copy_display_text(clean_text, rec, mode)
    reply_markup = _forward_copy_edit_keyboard(mode)
    ct = str((rec or {}).get("forward_copy_content_type") or "text")
    try:
        if ct == "text":
            _tg_call_retry(
                bot.edit_message_text,
                display_text,
                chat_id=int(chat_id),
                message_id=int(dst_msg_id),
                reply_markup=reply_markup,
                purpose="forward_copy_manual_edit",
            )
        elif ct in {"photo", "video", "document", "audio", "animation", "voice"}:
            _tg_call_retry(
                bot.edit_message_caption,
                caption=display_text,
                chat_id=int(chat_id),
                message_id=int(dst_msg_id),
                reply_markup=reply_markup,
                purpose="forward_copy_manual_edit",
            )
        else:
            return False
    except Exception as e:
        if "message is not modified" not in str(e).lower():
            log_error(f"edit_forward_copy_and_record({chat_id},{dst_msg_id}): {e}")
            return False
    finance_changed(int(chat_id), day_key, reason="forward_copy_manual_edit", delay=0.1)
    return True

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


def _schedule_persist_forward_state(delay: float = 0.25):
    global _forward_state_timer

    def _job():
        try:
            # Индекс пересылки хранится в root SQLite; чаты повторно не переписываем.
            save_data(data, root_only=True)
        except Exception as e:
            log_error(f"_schedule_persist_forward_state: {e}")

    scheduler_key = "forward-state-save"
    DELAYED_SCHEDULER.cancel(scheduler_key)
    _forward_state_timer = DELAYED_SCHEDULER.schedule(scheduler_key, delay, _job)


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
    rec = find_record_by_message_id(dst_chat_id, dst_msg_id) if finance_enabled else None
    edit_mode = forward_copy_edit_mode(source_chat_id) if finance_enabled else "normal"
    display_text = _forward_copy_display_text(text, rec, edit_mode) if rec else text
    edit_markup = _forward_copy_edit_keyboard(edit_mode) if finance_enabled else None

    try:
        if ct == "text":
            try:
                bot.edit_message_text(display_text, chat_id=dst_chat_id, message_id=dst_msg_id, reply_markup=edit_markup)
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
                bot.edit_message_caption(caption=display_text, chat_id=dst_chat_id, message_id=dst_msg_id, reply_markup=edit_markup)
            except Exception as e:
                err = str(e).lower()
                if "message is not modified" not in err:
                    raise
        else:
            raise RuntimeError(f"Edited sync unsupported for content_type={ct}")

        if finance_enabled and text and is_finance_mode(dst_chat_id):
            sync_forwarded_finance_message(dst_chat_id, dst_msg_id, text, owner_id, source_msg=msg)
            apply_forward_copy_edit_ui(source_chat_id, dst_chat_id, dst_msg_id, msg)
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

    # v92 fix2: режим 💰Перес глобальный. В режиме «кнопка» прикрепляем
    # inline-кнопку прямо к copyMessage, чтобы она появилась вместе с копией
    # и не зависела от последующего edit_message_reply_markup.
    pre_copy_markup = None
    try:
        if finance_enabled and forward_copy_edit_mode(source_chat_id) == "button":
            pre_copy_markup = _forward_copy_edit_keyboard("button")
    except Exception:
        pre_copy_markup = None

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
                    reply_markup=pre_copy_markup,
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
                        reply_markup=pre_copy_markup,
                        purpose="forward_copy_message"
                    )
                except TypeError:
                    sent = _tg_call_retry(bot.copy_message, dst_chat_id, source_chat_id, msg.message_id, reply_markup=pre_copy_markup, purpose="forward_copy_message")
        else:
            trace.step("копирует сообщение через Telegram copy_message")
            sent = _tg_call_retry(bot.copy_message, dst_chat_id, source_chat_id, msg.message_id, reply_markup=pre_copy_markup, purpose="forward_copy_message")
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
            if ok_fin:
                _rec = ok_fin if isinstance(ok_fin, dict) else None
                _ui_ok = apply_forward_copy_edit_ui(source_chat_id, dst_chat_id, dst_msg_id, msg, rec=_rec)
                if not _ui_ok and forward_copy_edit_mode(source_chat_id) != "normal":
                    schedule_forward_copy_edit_ui_retry(source_chat_id, dst_chat_id, dst_msg_id, msg, rec=_rec, delay=0.8)
            elif text_has_any_digit(text_for_finance):
                log_error(f"[FWD FINANCE NOT RECORDED] {get_chat_display_name(source_chat_id)}:{msg.message_id} -> {get_chat_display_name(dst_chat_id)}:{dst_msg_id} text={text_for_finance[:220]!r}")
        except Exception as e:
            log_error(f"_forward_single_to_target finance sync {get_chat_display_name(source_chat_id)}->{get_chat_display_name(dst_chat_id)}: {e}")

    # Bot-created copies do not generate incoming Telegram updates; capture them explicitly.
    try:
        capture_forwarded_bot_copy_as_secret(dst_chat_id, dst_msg_id, msg)
    except Exception as e:
        log_error(f"forward secret capture {source_chat_id}->{dst_chat_id}:{dst_msg_id}: {e}")

    trace.finish("пересылка завершена")
    return dst_msg_id


def _flush_media_group_forward(source_chat_id: int, media_group_id: str):
    if not FORWARD_TASK_POOL.submit(int(source_chat_id), _flush_media_group_forward_locked, source_chat_id, media_group_id):
        log_error(f"MEDIA GROUP FORWARD QUEUE FULL: {source_chat_id}")


def _flush_media_group_forward_locked(source_chat_id: int, media_group_id: str):
    cache_key = (int(source_chat_id), str(media_group_id))
    messages = _media_group_cache.pop(cache_key, [])
    _media_group_timers.pop(cache_key, None)
    DELAYED_SCHEDULER.cancel(f"media-group:{int(source_chat_id)}:{str(media_group_id)}")

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
                        if ok_fin:
                            _rec = ok_fin if isinstance(ok_fin, dict) else None
                            _ui_ok = apply_forward_copy_edit_ui(source_chat_id, dst_chat_id, dst_msg_id, src_msg, rec=_rec)
                            if not _ui_ok and forward_copy_edit_mode(source_chat_id) != "normal":
                                schedule_forward_copy_edit_ui_retry(source_chat_id, dst_chat_id, dst_msg_id, src_msg, rec=_rec, delay=0.8)
                        elif text_has_any_digit(text_for_finance):
                            log_error(f"[FWD MEDIA FINANCE NOT RECORDED] {get_chat_display_name(source_chat_id)}:{src_msg.message_id} -> {get_chat_display_name(dst_chat_id)}:{dst_msg_id} text={text_for_finance[:220]!r}")
                    except Exception as e:
                        log_error(f"_flush_media_group_forward finance sync {get_chat_display_name(source_chat_id)}->{get_chat_display_name(dst_chat_id)}: {e}")
                try:
                    capture_forwarded_bot_copy_as_secret(dst_chat_id, dst_msg_id, src_msg)
                except Exception as e:
                    log_error(f"media-group secret capture {source_chat_id}->{dst_chat_id}:{dst_msg_id}: {e}")
            continue

        for src_msg in messages:
            _forward_single_to_target(source_chat_id, src_msg, dst_chat_id, finance_enabled)


def _collect_media_group_for_forward(source_chat_id: int, msg):
    cache_key = (int(source_chat_id), str(msg.media_group_id))
    bucket = _media_group_cache.setdefault(cache_key, [])
    if not any(m.message_id == msg.message_id for m in bucket):
        bucket.append(msg)

    scheduler_key = f"media-group:{int(source_chat_id)}:{str(msg.media_group_id)}"
    DELAYED_SCHEDULER.cancel(scheduler_key)
    deadline = DELAYED_SCHEDULER.schedule(
        scheduler_key,
        0.8,
        _flush_media_group_forward,
        source_chat_id,
        msg.media_group_id,
    )
    _media_group_timers[cache_key] = deadline


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

    

# ─────────────────────────────────────────────────────────────
# v86: гомонковые резервы, остаток после расходов и USD
# ─────────────────────────────────────────────────────────────
GOMONKI_INSERT_TOKEN = "GOMONKI"
USD_RATE_URL = os.getenv("USD_RATE_URL", "https://dolarapi.com/v1/dolares/blue").strip()
USD_RATE_CACHE_SECONDS = max(300, int(os.getenv("USD_RATE_CACHE_SECONDS", "1800") or "1800"))


def _v85_enabled(feature: str) -> bool:
    return bool(active_bot_behavior_profile() in {"v92_current", "v91_current", "v90_current", "v88_current", "v87_current", "v86_current", "v85_current"} and version_mode_feature(feature))


def _gomonk_settings(chat_id: int) -> dict:
    settings = get_chat_store(int(chat_id)).setdefault("settings", {})
    settings.setdefault("gomonk_enabled", False)
    settings.setdefault("gomonk_entries", [])
    settings.setdefault("remaining_with_gomonk", True)
    return settings


def gomonk_enabled(chat_id: int) -> bool:
    return bool(_gomonk_settings(chat_id).get("gomonk_enabled", False))


def gomonk_entries(chat_id: int) -> list[dict]:
    out = []
    for item in (_gomonk_settings(chat_id).get("gomonk_entries") or []):
        if not isinstance(item, dict):
            continue
        try:
            amount = abs(float(item.get("amount", 0) or 0))
        except Exception:
            continue
        name = str(item.get("name") or "Сумма").strip() or "Сумма"
        if amount:
            out.append({"name": name[:80], "amount": amount})
    return out


def gomonk_total(chat_id: int) -> float:
    return sum(float(x.get("amount", 0) or 0) for x in gomonk_entries(chat_id))


def toggle_gomonk_enabled(chat_id: int) -> bool:
    settings = _gomonk_settings(chat_id)
    settings["gomonk_enabled"] = not bool(settings.get("gomonk_enabled", False))
    save_data(data, chat_ids=[int(chat_id)])
    schedule_config_backup_for_chats(int(chat_id))
    return bool(settings["gomonk_enabled"])


def parse_gomonk_entries(text: str) -> list[dict]:
    raw = sanitize_telegram_inserted_text(str(text or ""))
    raw = re.sub(r"(?is)^\s*\(?\s*GOMONKI\s*\)?\s*[:|\-]*\s*", "", raw).strip()
    parts = [p.strip() for p in raw.split(":") if p.strip()]
    result = []
    for idx, part in enumerate(parts, start=1):
        number_pattern = r"(?<![A-Za-zА-Яа-яЁё0-9_])[-+]?(?:\d{1,3}(?:[ .]\d{3})+(?:,\d+)?|\d+(?:[.,]\d+)?)"
        matches = list(re.finditer(number_pattern, part))
        if not matches:
            continue
        match = matches[-1]
        num_text = match.group(0).replace(" ", "").replace(".", "").replace(",", ".")
        try:
            amount = abs(float(num_text))
        except Exception:
            continue
        name = (part[:match.start()] + " " + part[match.end():]).strip(" -–—,.;")
        if not name:
            name = f"Сумма {idx}"
        if amount:
            result.append({"name": name[:80], "amount": amount})
    return result


def set_gomonk_entries(chat_id: int, entries: list[dict]):
    settings = _gomonk_settings(chat_id)
    settings["gomonk_entries"] = list(entries or [])[:30]
    save_data(data, chat_ids=[int(chat_id)])
    schedule_config_backup_for_chats(int(chat_id), delay=1.0)


def gomonk_toggle_label(chat_id: int) -> str:
    return "✅ Гомонковые ВКЛ" if gomonk_enabled(chat_id) else "❌ Гомонковые ВЫКЛ"


def gomonk_info_label(chat_id: int) -> str:
    return "🧳 Гомонковые ВКЛ" if gomonk_enabled(chat_id) else "🧳 Гомонковые ВЫКЛ"


def _ensure_currency_ledgers(store: dict) -> str:
    """Инициализирует независимые ARS/USD контуры без потери старых данных."""
    settings = store.setdefault("settings", {})
    active = str(settings.get("_active_currency_ledger") or "").lower()
    if active not in {"ars", "usd"}:
        # Все старые версии хранили основной учёт в ARS.
        active = "ars"
        settings["_active_currency_ledger"] = active
        store.setdefault("ars_records", copy.deepcopy(store.get("records", []) or []))
        store.setdefault("ars_daily_records", copy.deepcopy(store.get("daily_records", {}) or {}))
        store.setdefault("ars_balance", float(store.get("balance", 0) or 0))
        store.setdefault("ars_next_id", int(store.get("next_id", 1) or 1))
    store.setdefault("usd_records", [])
    store.setdefault("usd_daily_records", {})
    store.setdefault("usd_balance", 0.0)
    store.setdefault("usd_next_id", 1)
    return active


def _snapshot_active_currency_ledger(store: dict, ledger: str | None = None) -> None:
    ledger = ledger or _ensure_currency_ledgers(store)
    if ledger not in {"ars", "usd"}:
        return
    store[f"{ledger}_records"] = copy.deepcopy(store.get("records", []) or [])
    store[f"{ledger}_daily_records"] = copy.deepcopy(store.get("daily_records", {}) or {})
    store[f"{ledger}_balance"] = float(store.get("balance", 0) or 0)
    store[f"{ledger}_next_id"] = int(store.get("next_id", 1) or 1)


def _load_currency_ledger(store: dict, ledger: str) -> None:
    ledger = "usd" if str(ledger).lower() == "usd" else "ars"
    store["records"] = copy.deepcopy(store.get(f"{ledger}_records", []) or [])
    store["daily_records"] = copy.deepcopy(store.get(f"{ledger}_daily_records", {}) or {})
    store["balance"] = float(store.get(f"{ledger}_balance", 0) or 0)
    store["next_id"] = int(store.get(f"{ledger}_next_id", 1) or 1)
    store.setdefault("settings", {})["_active_currency_ledger"] = ledger


def active_currency_ledger_from_store(store: dict | None) -> str:
    try:
        return _ensure_currency_ledgers(store or {})
    except Exception:
        return "ars"


def active_currency_ledger(chat_id: int) -> str:
    return active_currency_ledger_from_store(get_chat_store(int(chat_id)))


def _switch_currency_ledger(chat_id: int, target: str) -> bool:
    """Переключает основной рабочий набор records/daily_records на выбранную валюту."""
    store = get_chat_store(int(chat_id))
    current = _ensure_currency_ledgers(store)
    target = "usd" if str(target).lower() == "usd" else "ars"
    if current == target:
        return False
    _snapshot_active_currency_ledger(store, current)
    _load_currency_ledger(store, target)
    return True


def currency_mode(chat_id: int) -> str:
    """Режим финансовых окон: ARS, ARS-USD (ARS с эквивалентом), либо отдельный USD-контур."""
    try:
        store = get_chat_store(int(chat_id))
        settings = store.setdefault("settings", {})
        mode = str(settings.get("currency_mode") or "").strip().lower()
        if mode not in {"ars", "ars_usd", "usd"}:
            mode = "ars_usd" if bool(settings.get("usd_display_enabled", False)) else "ars"
            settings["currency_mode"] = mode
        # На старте после рестарта рабочий набор уже соответствует сохранённому active ledger.
        _ensure_currency_ledgers(store)
        return mode
    except Exception:
        return "ars"


def currency_mode_from_store(store: dict | None) -> str:
    try:
        settings = (store or {}).setdefault("settings", {})
        mode = str(settings.get("currency_mode") or "").strip().lower()
        if mode not in {"ars", "ars_usd", "usd"}:
            mode = "ars_usd" if bool(settings.get("usd_display_enabled", False)) else "ars"
        return mode
    except Exception:
        return "ars"


def set_currency_mode(chat_id: int, mode: str):
    mode = str(mode or "ars").strip().lower()
    if mode not in {"ars", "ars_usd", "usd"}:
        mode = "ars"
    chat_id = int(chat_id)
    store = get_chat_store(chat_id)
    # ARS и ARS-USD используют один песовый реестр; USD — полностью отдельный.
    target_ledger = "usd" if mode == "usd" else "ars"
    _switch_currency_ledger(chat_id, target_ledger)
    settings = store.setdefault("settings", {})
    settings["currency_mode"] = mode
    settings["usd_display_enabled"] = mode != "ars"
    # Снимок активного контура нужен, чтобы backup всегда содержал актуальную валюту и после рестарта.
    _snapshot_active_currency_ledger(store, target_ledger)
    save_data(data, chat_ids=[chat_id])
    schedule_config_backup_for_chats(chat_id)
    if mode == "ars_usd":
        GENERAL_TASK_POOL.submit("usd-rate-refresh", usd_rate_cached, False)

def currency_mode_label(chat_id: int) -> str:
    labels = {"ars": "ARS", "ars_usd": "ARS-USD", "usd": "USD"}
    return f"💵 Доллар: {labels.get(currency_mode(chat_id), 'ARS')}"


def currency_menu_text(chat_id: int) -> str:
    mode = currency_mode(chat_id)
    labels = {"ars": "ARS — только песо", "ars_usd": "ARS-USD — песо и доллар в скобках", "usd": "USD — все суммы только в долларах"}
    rate_info = usd_rate_cached(force=False) if mode != "ars" else None
    lines = [
        "💱 Валюта финансовых окон",
        "",
        f"Текущий режим: {labels.get(mode, labels['ars'])}",
        "",
        "ARS — все значения в аргентинских песо.",
        "ARS-USD — основная сумма в песо, рядом эквивалент в долларах.",
        "USD — финансовые значения выводятся только в долларах.",
    ]
    if rate_info and rate_info.get("rate"):
        lines.extend(["", f"Курс: 1 USD = {fmt_num(rate_info.get('rate')).lstrip('+')} ARS"] )
    return wm_common("\n".join(lines), 9)


def build_currency_menu_keyboard(chat_id: int):
    current = currency_mode(chat_id)
    kb = types.InlineKeyboardMarkup(row_width=1)
    for mode, label in (("ars", "ARS"), ("ars_usd", "ARS-USD"), ("usd", "USD")):
        mark = "✅" if current == mode else "▫️"
        kb.row(IB(f"{mark} {label}", callback_data=f"currency_select:{mode}"))
    kb.row(IB("⏪", callback_data="currency_back"))
    return kb


def usd_display_enabled(chat_id: int) -> bool:
    """Совместимость со старым v86: True для ARS-USD и USD."""
    return currency_mode(int(chat_id)) != "ars"


def set_usd_display_enabled(chat_id: int, enabled: bool):
    set_currency_mode(int(chat_id), "ars_usd" if enabled else "ars")


def toggle_usd_display(chat_id: int) -> bool:
    new_mode = "ars" if currency_mode(int(chat_id)) != "ars" else "ars_usd"
    set_currency_mode(int(chat_id), new_mode)
    return new_mode != "ars"


def usd_display_label(chat_id: int) -> str:
    return currency_mode_label(chat_id)


def remaining_ost_label_enabled(chat_id: int) -> bool:
    try:
        return bool(get_chat_store(int(chat_id)).setdefault("settings", {}).get("remaining_show_ost_label", True))
    except Exception:
        return True


def toggle_remaining_ost_label(chat_id: int) -> bool:
    store = get_chat_store(int(chat_id))
    settings = store.setdefault("settings", {})
    new_value = not bool(settings.get("remaining_show_ost_label", True))
    settings["remaining_show_ost_label"] = new_value
    save_data(data, chat_ids=[int(chat_id)])
    schedule_config_backup_for_chats(int(chat_id))
    return new_value


def fmt_usd_compact(amount: float, rate_info: dict | None, signed: bool = True, absolute: bool = False) -> str:
    """Конвертация ARS→USD для режима ARS-USD."""
    if not rate_info or not rate_info.get("rate"):
        return "$—"
    amount = float(amount or 0)
    value = int(round(abs(amount) / float(rate_info["rate"])))
    if absolute or not signed:
        sign = ""
    else:
        sign = "+" if amount >= 0 else "-"
    return f"{sign}${value:,}".replace(",", " ")


def fmt_usd_native(amount: float, signed: bool = True, absolute: bool = False) -> str:
    """Формат суммы, которая уже хранится в отдельном USD-контуре."""
    amount = float(amount or 0)
    value = abs(amount)
    if abs(value - round(value)) < 1e-9:
        body = f"{int(round(value)):,}".replace(",", " ")
    else:
        body = f"{value:,.2f}".replace(",", " ").rstrip("0").rstrip(".")
    sign = "" if (absolute or not signed) else ("+" if amount >= 0 else "-")
    return f"{sign}${body}"


def format_chat_amount(chat_id: int, amount: float, mixed_space: bool = False) -> str:
    """Единый формат: ARS, ARS-USD либо нативные суммы отдельного USD-контура."""
    mode = currency_mode(int(chat_id))
    if mode == "ars":
        return fmt_num(amount)
    if mode == "usd":
        return fmt_usd_native(amount, signed=True)
    rate_info = usd_rate_cached(force=False)
    spacer = " " if mixed_space else ""
    return f"{fmt_num(amount)}{spacer}({fmt_usd_compact(amount, rate_info, signed=False, absolute=True)})"


def format_store_amount(store: dict, amount: float, mixed_space: bool = False, ars_plain: bool = False) -> str:
    mode = currency_mode_from_store(store)
    if mode == "ars":
        return fmt_num_plain(amount) if ars_plain else fmt_num(amount)
    if mode == "usd":
        return fmt_usd_native(amount, signed=not ars_plain, absolute=ars_plain)
    rate_info = usd_rate_cached(force=False)
    ars = fmt_num_plain(amount) if ars_plain else fmt_num(amount)
    spacer = " " if mixed_space else ""
    return f"{ars}{spacer}({fmt_usd_compact(amount, rate_info, signed=False, absolute=True)})"


def format_category_amount(store: dict, amount: float, category_mixed: bool = False) -> str:
    mode = currency_mode_from_store(store)
    if mode == "usd":
        return fmt_usd_native(amount, signed=False, absolute=True)
    rate_info = usd_rate_cached(force=False) if (mode == "ars_usd" or category_mixed) else None
    ars = fmt_num_plain(amount)
    if mode == "ars_usd" or category_mixed:
        return f"{ars} ({fmt_usd_compact(amount, rate_info, signed=False, absolute=True)})"
    return ars

def gomonk_summary_lines(chat_id: int) -> list[str]:
    if not (_v85_enabled("gomonk_wallets") and gomonk_enabled(chat_id)):
        return []
    entries = gomonk_entries(chat_id)
    if not entries:
        return ["", f"🧮 Сумма гомонковых: {format_chat_amount(chat_id, 0, mixed_space=True)}"]
    balance = float(get_chat_store(chat_id).get("balance", 0) or 0)
    total = gomonk_total(chat_id)
    return [
        "",
        f"🧮 Сумма гомонковых: {format_chat_amount(chat_id, total, mixed_space=True)}",
        f"🏦 Остаток без гомонковых: {format_chat_amount(chat_id, balance - total, mixed_space=True)}",
    ]


def build_gomonk_menu_text(chat_id: int) -> str:
    entries = gomonk_entries(chat_id)
    lines = [
        "🧳 Гомонковые",
        "",
        "Это суммы, которые резервируются отдельно и вычитаются из остатка по чату.",
        "Формат нескольких сумм через двоеточие:",
        "Имя1 1000 : Имя2 5777 : 3000",
        "",
        f"Режим: {'ВКЛ' if gomonk_enabled(chat_id) else 'ВЫКЛ'}",
    ]
    if entries:
        lines.append("Сохранено:")
        for item in entries:
            lines.append(f"• {item['name']}: {fmt_num(item['amount'])}")
        lines.append(f"Итого: {fmt_num(gomonk_total(chat_id))}")
    else:
        lines.append("Сохранённых сумм пока нет.")
    return wm_common("\n".join(lines), 9)


def build_gomonk_menu_keyboard(chat_id: int):
    kb = types.InlineKeyboardMarkup(row_width=1)
    kb.row(IB(gomonk_toggle_label(chat_id), callback_data="gomonk_toggle"))
    template = f"({GOMONKI_INSERT_TOKEN})\nИмя1 1000 : Имя2 5777"
    kb.row(make_copy_or_inline_button("💰 Сумма", template))
    kb.row(IB("🔙 Назад в Инфо", callback_data="gomonk_back"))
    return kb


def handle_gomonk_insert_message(msg) -> bool:
    if getattr(msg, "content_type", None) != "text" or not _v85_enabled("gomonk_wallets"):
        return False
    cleaned = sanitize_telegram_inserted_text(getattr(msg, "text", "") or "")
    if GOMONKI_INSERT_TOKEN not in cleaned.upper():
        return False
    chat_id = int(msg.chat.id)
    entries = parse_gomonk_entries(cleaned)
    try:
        bot.delete_message(chat_id, msg.message_id)
    except Exception:
        pass
    if not entries:
        send_and_auto_delete(chat_id, "❌ Не нашёл сумм. Пример: Имя1 1000 : Имя2 5777", 12)
        return True
    set_gomonk_entries(chat_id, entries)
    _gomonk_settings(chat_id)["gomonk_enabled"] = True
    save_data(data, chat_ids=[chat_id])
    bot_journal("gomonk_values_saved", chat_id, f"count={len(entries)} total={gomonk_total(chat_id)}")
    send_and_auto_delete(chat_id, f"✅ Гомонковые сохранены: {len(entries)}, сумма {fmt_num(gomonk_total(chat_id))}", 10)
    try:
        open_gomonk_window(chat_id)
        finance_changed(chat_id, get_chat_store(chat_id).get("current_view_day") or today_key(), reason="gomonk_update", delay=0.05)
    except Exception:
        pass
    return True


def open_gomonk_window(chat_id: int, message_id: int | None = None):
    if message_id:
        fast_ui_edit_message_text(chat_id, message_id, build_gomonk_menu_text(chat_id), reply_markup=build_gomonk_menu_keyboard(chat_id), purpose="gomonk_window")
    else:
        send_or_edit_stored_window(chat_id, "info_msg_id", build_gomonk_menu_text(chat_id), reply_markup=build_gomonk_menu_keyboard(chat_id), delay=AUX_WINDOW_DELETE_DELAY)


def _opening_balance_before_day(store: dict, day_key: str) -> float:
    total = 0.0
    for rec in (store.get("records", []) or []):
        try:
            if _record_day_key(rec) < day_key:
                total += float(rec.get("amount", 0) or 0)
        except Exception:
            pass
    return total


def build_remaining_text(chat_id: int, day_key: str, with_gomonk: bool | None = None) -> str:
    store = get_chat_store(chat_id)
    settings = _gomonk_settings(chat_id)
    if with_gomonk is None:
        with_gomonk = bool(settings.get("remaining_with_gomonk", True))
    reserve = gomonk_total(chat_id) if (with_gomonk and gomonk_enabled(chat_id)) else 0.0
    running = _opening_balance_before_day(store, day_key)
    lines = [
        "🧮 Остаток после каждого расхода",
        f"📅 {fmt_date_ddmmyy(day_key)}",
        f"Режим: {'с гомонковыми' if reserve else 'без гомонковых'}",
        "",
    ]
    mode = currency_mode(chat_id)
    show_ost = remaining_ost_label_enabled(chat_id)
    shown = 0
    for rec in sorted((store.get("daily_records", {}) or {}).get(day_key, []) or [], key=record_sort_key):
        try:
            amount = float(rec.get("amount", 0) or 0)
        except Exception:
            continue
        running += amount
        if amount >= 0:
            continue
        shown += 1
        rid = rec.get("short_id") or f"R{rec.get('id', '')}"
        note = html.escape(str(rec.get("note") or "").strip())
        after = running - reserve
        amount_text = format_chat_amount(chat_id, amount, mixed_space=False)
        after_text = format_chat_amount(chat_id, after, mixed_space=False) if mode == "usd" else fmt_num(after)
        label = "ост:" if show_ost else ""
        lines.append(f"{rid} {amount_text} {note} ({label}{after_text})".rstrip())
    if not shown:
        lines.append("За этот день расходов нет.")
    current_remaining = float(store.get("balance", 0) or 0) - reserve
    lines.extend(["", f"🏦 Текущий остаток по чату: {format_chat_amount(chat_id, current_remaining, mixed_space=True)}"])
    if reserve:
        lines.append(f"🧳 Вычтено гомонковых: {format_chat_amount(chat_id, reserve, mixed_space=True)}")
    return wm_common("\n".join(lines), 9, html_mode=True)


def build_remaining_keyboard(chat_id: int, day_key: str):
    settings = _gomonk_settings(chat_id)
    with_g = bool(settings.get("remaining_with_gomonk", True))
    try:
        dt = datetime.strptime(day_key, "%Y-%m-%d")
    except Exception:
        dt = now_local()
        day_key = dt.strftime("%Y-%m-%d")
    prev_key = (dt - timedelta(days=1)).strftime("%Y-%m-%d")
    next_key = (dt + timedelta(days=1)).strftime("%Y-%m-%d")
    kb = types.InlineKeyboardMarkup(row_width=3)
    # v91: если включён режим «Финансы-кнопки», записи идут первыми, сверху окна Ф91.
    if effective_main_financial_value_buttons_enabled(chat_id):
        for rec in financial_value_records_for_day(chat_id, day_key)[:84]:
            try:
                rid = int(rec.get("id"))
            except Exception:
                continue
            kb.row(IB(financial_record_button_label(rec, chat_id), callback_data=f"d:{day_key}:value_rec_{rid}"))
    nav = [IB("⬅️ День", callback_data=f"remaining_open:{prev_key}")]
    if day_key != today_key():
        nav.append(IB("📅 Сегодня", callback_data=f"remaining_open:{today_key()}"))
    nav.append(IB("День ➡️", callback_data=f"remaining_open:{next_key}"))
    kb.row(*nav)
    kb.row(IB("Без гомонковых" if with_g else "С гомонковыми", callback_data=f"remaining_toggle:{day_key}"))
    kb.row(IB("⬅️ Назад осн. окно", callback_data=f"d:{day_key}:back_main"), IB("❌ Закрыть", callback_data="aux_close"))
    return kb

def open_remaining_window(chat_id: int, day_key: str, message_id: int | None = None):
    text = build_remaining_text(chat_id, day_key)
    kb = build_remaining_keyboard(chat_id, day_key)
    if message_id:
        fast_ui_edit_message_text(chat_id, message_id, text, reply_markup=kb, parse_mode="HTML", purpose="remaining_window")
    else:
        send_or_edit_stored_window(chat_id, "remaining_msg_id", text, reply_markup=kb, parse_mode="HTML", delay=AUX_WINDOW_DELETE_DELAY)


def _clean_category_display_name(value: str) -> str:
    s = str(value or "").strip()
    s = re.sub(r"(?i)@[A-Za-z0-9_]{3,}\s*", "", s)
    return re.sub(r"\s+", " ", s).strip(" :,-")


def usd_rate_cached(force: bool = False) -> dict | None:
    gs = data.setdefault("_global_settings", {})
    cache = gs.get("usd_rate_cache") if isinstance(gs.get("usd_rate_cache"), dict) else {}
    age = time.time() - float(cache.get("fetched_ts", 0) or 0)
    if not force and cache.get("rate") and age < USD_RATE_CACHE_SECONDS:
        return cache
    # Никогда не ждём внешний сайт внутри callback/webhook: отдаём старый курс и обновляем фоном.
    if threading.current_thread().name.startswith("webhook"):
        GENERAL_TASK_POOL.submit("usd-rate-refresh", usd_rate_cached, True)
        return cache if cache.get("rate") else None
    try:
        resp = requests.get(USD_RATE_URL, timeout=8)
        resp.raise_for_status()
        payload = resp.json()
        rate = float(payload.get("venta") or payload.get("promedio") or payload.get("compra") or 0)
        if rate <= 0:
            raise ValueError("курс venta отсутствует")
        cache = {
            "rate": rate,
            "source": str(payload.get("nombre") or payload.get("casa") or "DolarAPI dólar blue"),
            "fetched_at": str(payload.get("fechaActualizacion") or now_local().isoformat(timespec="seconds")),
            "fetched_ts": time.time(),
            "url": USD_RATE_URL,
        }
        gs["usd_rate_cache"] = cache
        save_data(data, root_only=True)
        bot_journal("usd_rate_updated", None, f"rate={rate} source={cache['source']}")
        return cache
    except Exception as e:
        bot_journal("usd_rate_error", None, str(e), "WARN")
        return cache if cache.get("rate") else None


def _usd_rate_refresh_loop():
    while True:
        try:
            usd_rate_cached(force=True)
        except Exception:
            pass
        time.sleep(USD_RATE_CACHE_SECONDS)


def fmt_usd_from_ars(amount: float, rate_info: dict | None) -> str:
    """Совместимый короткий USD-формат для старых окон."""
    return fmt_usd_compact(amount, rate_info, signed=False, absolute=True)


def usd_transactions_view_enabled(chat_id: int) -> bool:
    try:
        return bool(get_chat_store(int(chat_id)).setdefault("settings", {}).get("usd_transactions_view", False))
    except Exception:
        return False


def set_usd_transactions_view(chat_id: int, enabled: bool):
    store = get_chat_store(int(chat_id))
    store.setdefault("settings", {})["usd_transactions_view"] = bool(enabled)
    save_data(data, chat_ids=[int(chat_id)])
    schedule_config_backup_for_chats(int(chat_id))


def toggle_usd_transactions_view(chat_id: int) -> bool:
    new_value = not usd_transactions_view_enabled(int(chat_id))
    set_usd_transactions_view(int(chat_id), new_value)
    return new_value


def usd_transactions_toggle_label(chat_id: int) -> str:
    return "🇦🇷 ARS операции" if usd_transactions_view_enabled(int(chat_id)) else "💵 USD операции"


def ensure_usd_migration_for_chat(chat_id: int) -> int:
    """Однократно подхватывает USD из старых v92-записей текущей базы.

    Новые v93-записи сразу имеют usd_amount. Для старых реконструируем исходную строку
    из amount + note и применяем тот же парсер. Миграция не создаёт дублей.
    """
    store = get_chat_store(int(chat_id))
    settings = store.setdefault("settings", {})
    if settings.get("usd_transactions_migrated_v93"):
        return 0
    changed = 0
    with locked_chat(int(chat_id)):
        for rec in store.get("records", []) or []:
            if rec.get("usd_amount") is not None:
                continue
            note = str(rec.get("note") or "").strip()
            low = note.casefold()
            likely = bool(
                re.search(r"usd|усд|\$", low)
                or (("к" in low or re.search(r"\bk\b", low)) and (USD_EXCHANGE_RE.search(low) or re.search(r"\bот\b", low) or "+к" in low or "+k" in low))
            )
            if not likely:
                continue
            try:
                old_amount = float(rec.get("amount", 0) or 0)
            except Exception:
                old_amount = 0.0
            # Отдельный старый формат «И 5+к»: после старого парсера число 5 было amount, а в note осталось «и +к».
            if re.search(r"(?i)(?:^|\s)и\s*\+[kк]\b", low):
                rec["usd_amount"] = abs(old_amount) * 1000.0
                rec["usd_note"] = ""
                rec["usd_only"] = True
                rec["source_finance_text"] = f"И {fmt_num_compact(abs(old_amount))}+к"
                rec["amount"] = 0.0
                changed += 1
                continue

            sign = "+" if old_amount > 0 else ""
            amount_text = fmt_num_compact(abs(old_amount))
            # Если в старой note остался USD-маркер без собственной суммы, значит старый парсер
            # вырезал именно первое USD-число. Возвращаем число на прежнее место перед USD/УСД.
            explicit_note_usd = extract_usd_transaction(note)
            if explicit_note_usd is None and re.search(r"(?i)(?:usd|усд|\$)", note):
                if USD_EXCHANGE_RE.search(low):
                    sign = ""  # «обмен» раньше мог искусственно перевернуть расход в плюс
                insert_value = sign + amount_text
                # «2к усд» после старого парсера превращалось в amount=2, note="к усд ...".
                mcur = re.search(r"(?i)(?P<mult>[kк]\s*)?(?P<cur>usd|усд|\$)", note)
                if mcur:
                    replacement = insert_value + (mcur.group("mult") or "") + mcur.group("cur")
                    reconstructed = (note[:mcur.start()] + replacement + note[mcur.end():]).strip()
                else:
                    reconstructed = f"{insert_value} {note}".strip()
            else:
                reconstructed = f"{sign}{amount_text} {note}".strip()
            try:
                comp = parse_financial_components(reconstructed)
            except Exception:
                continue
            if comp.get("usd_amount") is None:
                continue
            rec["usd_amount"] = float(comp.get("usd_amount") or 0)
            rec["usd_note"] = str(comp.get("usd_note") or "")
            rec["usd_only"] = bool(comp.get("usd_only", False))
            rec["source_finance_text"] = reconstructed
            # Если строка была только USD или парсер нашёл корректную отдельную ARS-часть, исправляем старое ARS-значение.
            rec["amount"] = float(comp.get("amount", 0) or 0)
            rec["note"] = str(comp.get("note") or rec.get("note") or "")
            changed += 1

        settings["usd_transactions_migrated_v93"] = True
        if changed:
            normalize_chat_records(int(chat_id))
            recalc_balance(int(chat_id))
            rebuild_month_short_ids(int(chat_id))
            rebuild_global_records()
        save_data(data, chat_ids=[int(chat_id)])
    if changed:
        try:
            bot_journal("usd_v93_migration", int(chat_id), f"records={changed}")
        except Exception:
            pass
    return changed


def usd_records_for_month(chat_id: int, month_key: str) -> list[dict]:
    ensure_usd_migration_for_chat(int(chat_id))
    rows = []
    for rec in get_chat_store(int(chat_id)).get("records", []) or []:
        try:
            if not _record_day_key(rec).startswith(str(month_key)[:7]):
                continue
            usd_amount = float(rec.get("usd_amount", 0) or 0)
            if not usd_amount:
                continue
            rows.append(rec)
        except Exception:
            continue
    return sorted(rows, key=record_sort_key)


def usd_balance_for_chat(chat_id: int) -> float:
    ensure_usd_migration_for_chat(int(chat_id))
    total = 0.0
    for rec in get_chat_store(int(chat_id)).get("records", []) or []:
        try:
            total += float(rec.get("usd_amount", 0) or 0)
        except Exception:
            pass
    return total


def render_usd_month_window(chat_id: int, day_key: str):
    month_key = str(day_key or today_key())[:7]
    try:
        month_dt = datetime.strptime(month_key + "-01", "%Y-%m-%d")
        month_label = month_dt.strftime("%m.%Y")
    except Exception:
        month_label = month_key
    rows = usd_records_for_month(int(chat_id), month_key)
    income = sum(float(r.get("usd_amount", 0) or 0) for r in rows if float(r.get("usd_amount", 0) or 0) > 0)
    expense = sum(abs(float(r.get("usd_amount", 0) or 0)) for r in rows if float(r.get("usd_amount", 0) or 0) < 0)
    lines = [f"💵 USD операции за {month_label}", ""]
    if rows:
        for rec in rows:
            amt = float(rec.get("usd_amount", 0) or 0)
            sid = str(rec.get("usd_short_id") or rec.get("short_id") or f"U{rec.get('id','')}")
            dk = fmt_date_ddmmyy(_record_day_key(rec))
            note = html.escape(str(rec.get("usd_note") or rec.get("note") or ""))
            sign = "+" if amt >= 0 else "-"
            val = fmt_num_plain(abs(amt))
            lines.append(f"{sid} {dk} {sign}${val} {note}".rstrip())
    else:
        lines.append("Нет USD-транзакций за этот месяц.")
    lines.extend([
        "",
        f"📉 Расход за месяц: -${fmt_num_plain(expense)}",
        f"📈 Приход за месяц: +${fmt_num_plain(income)}",
        f"💵 Итог месяца: {('+' if income-expense >= 0 else '-')}${fmt_num_plain(abs(income-expense))}",
        f"🏦 USD остаток по чату: {('+' if usd_balance_for_chat(chat_id) >= 0 else '-')}${fmt_num_plain(abs(usd_balance_for_chat(chat_id)))}",
    ])
    return wm_common("\n".join(lines), 1, html_mode=True), income - expense


def render_day_window(chat_id: int, day_key: str):
    if version_mode_feature("usd_transactions") and usd_transactions_view_enabled(int(chat_id)):
        return render_usd_month_window(int(chat_id), day_key)
    store = get_chat_store(chat_id)
    recs = [r for r in (store.get("daily_records", {}).get(day_key, []) or []) if not bool(r.get("usd_only", False))]

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
    mode = currency_mode(chat_id) if version_mode_feature("daily_usd") else "ars"
    rate_info = usd_rate_cached(force=False) if mode != "ars" else None
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
        all_record_lines.append(f"{sid} {format_chat_amount(chat_id, amt, mixed_space=False)} {note}".rstrip())

    day_balance = calc_day_balance(store, day_key)
    bal_chat = store.get("balance", 0)

    footer = [""]
    if recs_sorted:
        expense_value = -total_expense if total_expense else 0.0
        income_value = total_income if total_income else 0.0
        footer.append(f"📉 Расход за день: {format_chat_amount(chat_id, expense_value, mixed_space=True)}")
        footer.append(f"📈 Приход за день: {format_chat_amount(chat_id, income_value, mixed_space=True)}")
    footer.append(f"📆 Остаток на конец дня: {format_chat_amount(chat_id, day_balance, mixed_space=True)}")
    footer.append(f"🏦 Остаток по чату: {format_chat_amount(chat_id, bal_chat, mixed_space=True)}")
    if mode != "ars" and rate_info:
        footer.append(f"💵 Курс: 1 USD = {fmt_num(rate_info.get('rate')).lstrip('+')} ARS")
    footer.extend(gomonk_summary_lines(chat_id))

    total = total_income - total_expense

    if not all_record_lines:
        return wm_common("\n".join(header + ["Нет записей за этот день."] + footer), 1, html_mode=True), total

    if effective_main_financial_value_buttons_enabled(chat_id):
        hint = [f"💳 Записей за день: {len(recs_sorted)}", "Нажмите сумму-кнопку ниже, чтобы изменить запись."]
        return wm_common("\n".join(header + hint + footer), 1, html_mode=True), total

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
    """Ф41: верхняя строка массово включает/выключает три вида бэкапа."""
    kb = types.InlineKeyboardMarkup(row_width=4)
    owner_id = int(OWNER_ID) if OWNER_ID else None
    headers = []
    for target, label in (("chat", "чат"), ("channel", "канал"), ("mega", "MEGA")):
        enabled, total = _backup_target_all_state(target)
        all_on = bool(total and enabled == total)
        headers.append(IB(("✅" if all_on else "❌") + f" все {label}", callback_data=f"d:{day_key}:backup_mass_{target}"))
    kb.row(IB("Чаты", callback_data="none"), *headers)
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
        f"Канал = JSON + Excel (Excel сейчас {backup_excel_all_label()}). MEGA = только JSON. В чат = только для владельца. Верхняя строка переключает сразу все чаты."
    ), 7)


def build_main_keyboard(day_key: str, chat_id=None):
    """Главное окно без отдельной кнопки «Меню»: все основные функции сразу на виду."""
    kb = types.InlineKeyboardMarkup(row_width=3)

    if chat_id is not None and usd_transactions_view_enabled(int(chat_id)):
        nav_row = [IB("⬅️ Пред. месяц", callback_data=f"d:{day_key}:prev")]
        if str(day_key)[:7] != today_key()[:7]:
            nav_row.append(IB("📅 Этот месяц", callback_data=f"d:{day_key}:today"))
        nav_row.append(IB("След. месяц ➡️", callback_data=f"d:{day_key}:next"))
    else:
        nav_row = [IB("⬅️ Вчера", callback_data=f"d:{day_key}:prev")]
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
    if chat_id is not None and version_mode_feature("usd_transactions"):
        kb.row(IB(usd_transactions_toggle_label(int(chat_id)), callback_data=f"d:{day_key}:usd_tx_toggle"))

    # v84: каждая финансовая запись дня становится кнопкой. Нажатие сразу
    # открывает штатный 40-секундный режим редактирования этой записи.
    if chat_id is not None and effective_main_financial_value_buttons_enabled(int(chat_id)) and not usd_transactions_view_enabled(int(chat_id)):
        value_buttons = []
        for rec in financial_value_records_for_day(int(chat_id), day_key):
            try:
                rid = int(rec.get("id"))
            except Exception:
                continue
            value_buttons.append(IB(financial_record_button_label(rec, int(chat_id)), callback_data=f"d:{day_key}:value_rec_{rid}"))
        per_row = max(1, int(active_bot_behavior_profile_info().get("financial_buttons_per_row", 2) or 2))
        add_buttons_in_rows(kb, value_buttons[:84], per_row)
        if len(value_buttons) > 84:
            kb.row(IB(f"Ещё записей: {len(value_buttons) - 84}", callback_data=f"d:{day_key}:edit_list"))

    # Исторический режим v83 сохранён для точного сравнения поведения версии.
    if chat_id is not None and effective_main_article_buttons_enabled(int(chat_id)):
        article_buttons = []
        for item in category_edit_items_for_chat(int(chat_id)):
            slug = str(item.get("slug") or "").strip()
            name = _clean_category_display_name(str(item.get("name") or slug or "Статья").strip())
            if not slug:
                continue
            article_buttons.append(IB(f"✏️ {name}", callback_data=cat_callback(f"cat_main_edit:{slug}:{day_key}")))
        add_buttons_in_rows(kb, article_buttons[:84], 2)

    # Обнуление убрано из основного окна о1 по ТЗ. Оставлена команда /reset в окне ℹ️ Инфо.
    if chat_id is not None and _v85_enabled("remaining_window"):
        kb.row(
            IB("ℹ️ Инфо", callback_data=f"d:{day_key}:info"),
            IB("с ост", callback_data=f"remaining_open:{day_key}"),
        )
    else:
        kb.row(IB("ℹ️ Инфо", callback_data=f"d:{day_key}:info"))

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


def start_record_edit_prompt(chat_id: int, day_key: str, rid: int) -> bool:
    try:
        chat_id = int(chat_id)
        rid = int(rid)
        store = get_chat_store(chat_id)
        rec = next((r for r in store.get("records", []) if int(r.get("id", 0) or 0) == rid), None)
        if not rec:
            send_and_auto_delete(chat_id, "❌ Запись не найдена.")
            return False
        text = (
            f"✏️ Редактирование записи R{rid}\n\n"
            f"Текущие данные:\n"
            f"{fmt_num(rec.get('amount', 0))} {rec.get('note','')}\n\n"
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
        save_data(data, chat_ids=[chat_id])
        schedule_cancel_edit(chat_id, prompt_id, delay=40)
        return True
    except Exception as e:
        log_error(f"start_record_edit_prompt({chat_id},{day_key},{rid}): {e}")
        return False


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
    store = get_chat_store(chat_id)
    daily = store.get("daily_records", {})
    if not month_key:
        month_key = now_local().strftime("%Y-%m")
    try:
        month_dt = datetime.strptime(month_key + "-01", "%Y-%m-%d")
    except Exception:
        month_dt = now_local().replace(day=1)
        month_key = month_dt.strftime("%Y-%m")
    year, month = month_dt.year, month_dt.month
    next_month = datetime(year + 1, 1, 1) if month == 12 else datetime(year, month + 1, 1)
    days_in_month = (next_month - timedelta(days=1)).day
    mode = currency_mode(chat_id)
    lines = [f"ОТЧЁТ ЗА {month_dt.strftime('%m.%Y')}", ""]
    if mode == "ars":
        lines.extend([
            f"{'Дата':<8}|{report_header_cell('Приход', 7)}|{report_header_cell('Расход', 7)}|{report_header_cell('Остаток', 7)}",
            "",
        ])
    has_any = False
    for day in range(1, days_in_month + 1):
        day_key = f"{year}-{month:02d}-{day:02d}"
        recs = [r for r in (daily.get(day_key, []) or []) if not bool(r.get("usd_only", False))]
        total_expense = sum(-float(r.get("amount", 0) or 0) for r in recs if float(r.get("amount", 0) or 0) < 0)
        total_income = sum(float(r.get("amount", 0) or 0) for r in recs if float(r.get("amount", 0) or 0) >= 0)
        day_balance = calc_day_balance(store, day_key)
        has_any = has_any or bool(recs)
        date_str = datetime.strptime(day_key, "%Y-%m-%d").strftime("%d.%m.%y")
        if mode == "ars":
            lines.append(f"{date_str:<8}|{report_cell(int(total_income), 7)}|{report_cell(int(total_expense), 7)}|{report_cell(int(day_balance), 7)}")
        else:
            lines.append(
                f"{date_str} | приход {format_chat_amount(chat_id, total_income, True)} | "
                f"расход {format_chat_amount(chat_id, -total_expense, True)} | ост {format_chat_amount(chat_id, day_balance, True)}"
            )
    if not has_any:
        lines.append("Нет данных за этот месяц.")
    return wm_common("<pre>" + html.escape("\n".join(lines)) + "</pre>", 3, html_mode=True), month_key

def build_calendar_keyboard(center_day: datetime, chat_id=None):
    """Monthly financial calendar with explicit month/year and separate year navigation."""
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
            row.append(IB(label, callback_data=f"d:{key}:open"))
        kb.row(*row)

    prev_month = (center_day.replace(day=1) - timedelta(days=1)).replace(day=1)
    next_month = (center_day.replace(day=28) + timedelta(days=4)).replace(day=1)
    kb.row(
        IB("⬅️ Месяц", callback_data=f"c:{prev_month.strftime('%Y-%m-%d')}"),
        IB(f"{russian_month_name(center_day.month)} {center_day.year}", callback_data="none"),
        IB("Месяц ➡️", callback_data=f"c:{next_month.strftime('%Y-%m-%d')}"),
    )
    try:
        prev_year = center_day.replace(year=center_day.year - 1, day=1)
    except ValueError:
        prev_year = center_day.replace(year=center_day.year - 1, month=2, day=28)
    try:
        next_year = center_day.replace(year=center_day.year + 1, day=1)
    except ValueError:
        next_year = center_day.replace(year=center_day.year + 1, month=2, day=28)
    kb.row(
        IB("◀️ Год", callback_data=f"c:{prev_year.strftime('%Y-%m-%d')}"),
        IB(str(center_day.year), callback_data="none"),
        IB("Год ▶️", callback_data=f"c:{next_year.strftime('%Y-%m-%d')}"),
    )

    current_month = now_local().strftime("%Y-%m")
    shown_month = center_day.strftime("%Y-%m")
    bottom_row = []
    if shown_month != current_month:
        bottom_row.append(IB("📅 Сегодня", callback_data=f"c:{now_local().strftime('%Y-%m-%d')}"))
    elif back_day_key != today_key():
        bottom_row.append(IB("📅 Сегодня", callback_data=f"d:{today_key()}:open"))
    bottom_row.append(IB("🔙 Назад", callback_data=f"d:{back_day_key}:back_main"))
    kb.row(*bottom_row)
    return kb

def _backup_toggle_label(chat_id: int, target: str, label: str) -> str:
    icon = "✅" if is_backup_target_enabled(chat_id, target) else "❌"
    return f"{icon} {label}"


def _add_export_period_rows(kb, day_key: str, prefix: str, owner_day_key: str | None = None, target_chat_id: int | None = None):
    """Ф47: пять строк периодов и четыре колонки: период / CSV / Excel / Excel статьи."""
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
            xlsxstat_cb = f"fv:{target_chat_id}:{day_key}:xlsxstat_{mode}:{owner_day_key}"
        else:
            csv_action = "csv_all_real" if mode == "all" else f"csv_{mode}"
            xlsx_action = "xlsx_all" if mode == "all" else f"xlsx_{mode}"
            xlsxstat_action = f"xlsxstat_{mode}"
            csv_cb = f"d:{day_key}:{csv_action}"
            xlsx_cb = f"d:{day_key}:{xlsx_action}"
            xlsxstat_cb = f"d:{day_key}:{xlsxstat_action}"
        kb.row(
            IB(label, callback_data="none"),
            IB("CSV", callback_data=csv_cb),
            IB("Excel", callback_data=xlsx_cb),
            IB("Excel статьи", callback_data=xlsxstat_cb),
        )



def _export_calendar_start_keyboard(view_year: int, view_month: int, return_day_key: str):
    kb = types.InlineKeyboardMarkup(row_width=7)
    last_day = calendar.monthrange(int(view_year), int(view_month))[1]
    buttons = [
        IB(str(day_num), callback_data=export_callback(f"exp_pick_set_start:{view_year}:{view_month}:{day_num}:{return_day_key}"))
        for day_num in range(1, last_day + 1)
    ]
    for idx in range(0, len(buttons), 7):
        kb.row(*buttons[idx:idx + 7])
    prev_y, prev_m = _shift_month(view_year, view_month, -1)
    next_y, next_m = _shift_month(view_year, view_month, 1)
    kb.row(
        IB("⬅️ Месяц", callback_data=export_callback(f"exp_pick_start:{prev_y}:{prev_m}:{return_day_key}")),
        IB(f"{russian_month_name(view_month)} {view_year}", callback_data="none"),
        IB("Месяц ➡️", callback_data=export_callback(f"exp_pick_start:{next_y}:{next_m}:{return_day_key}")),
    )
    kb.row(
        IB("◀️ Год", callback_data=export_callback(f"exp_pick_start:{view_year-1}:{view_month}:{return_day_key}")),
        IB(str(view_year), callback_data="none"),
        IB("Год ▶️", callback_data=export_callback(f"exp_pick_start:{view_year+1}:{view_month}:{return_day_key}")),
    )
    kb.row(IB("🔙 Назад в CSV / Excel", callback_data=f"d:{return_day_key}:csv_all"))
    return kb


def _export_start_record_keyboard(chat_id: int, start_key: str, return_day_key: str):
    store = get_chat_store(chat_id)
    kb = types.InlineKeyboardMarkup(row_width=1)
    _expense_anchor_rows(
        kb,
        store,
        start_key,
        lambda rid: export_callback(f"exp_pick_start_record:{start_key}:{rid}:{return_day_key}"),
    )
    kb.row(IB("➡️ Продолжить с начала дня", callback_data=export_callback(f"exp_pick_start_record:{start_key}:0:{return_day_key}")))
    dt = datetime.strptime(start_key, "%Y-%m-%d")
    kb.row(IB("🔙 Назад к календарю", callback_data=export_callback(f"exp_pick_start:{dt.year}:{dt.month}:{return_day_key}")))
    return kb


def _export_end_calendar_keyboard(start_key: str, start_rid: int, view_year: int, view_month: int, return_day_key: str):
    kb = types.InlineKeyboardMarkup(row_width=7)
    last_day = calendar.monthrange(int(view_year), int(view_month))[1]
    buttons = []
    for day_num in range(1, last_day + 1):
        day_key = _date_key_from_ymd(view_year, view_month, day_num)
        if day_key < start_key:
            buttons.append(IB("·", callback_data="none"))
        else:
            buttons.append(IB(str(day_num), callback_data=export_callback(
                f"exp_pick_set_end:{start_key}:{int(start_rid)}:{view_year}:{view_month}:{day_num}:{return_day_key}"
            )))
    for idx in range(0, len(buttons), 7):
        kb.row(*buttons[idx:idx + 7])
    prev_y, prev_m = _shift_month(view_year, view_month, -1)
    next_y, next_m = _shift_month(view_year, view_month, 1)
    nav = []
    if f"{prev_y:04d}-{prev_m:02d}" >= start_key[:7]:
        nav.append(IB("⬅️ Месяц", callback_data=export_callback(
            f"exp_pick_end:{start_key}:{int(start_rid)}:{prev_y}:{prev_m}:{return_day_key}"
        )))
    else:
        nav.append(IB(" ", callback_data="none"))
    nav.append(IB(f"{russian_month_name(view_month)} {view_year}", callback_data="none"))
    nav.append(IB("Месяц ➡️", callback_data=export_callback(
        f"exp_pick_end:{start_key}:{int(start_rid)}:{next_y}:{next_m}:{return_day_key}"
    )))
    kb.row(*nav)
    kb.row(
        IB("◀️ Год", callback_data=export_callback(f"exp_pick_end:{start_key}:{int(start_rid)}:{view_year-1}:{view_month}:{return_day_key}")),
        IB(str(view_year), callback_data="none"),
        IB("Год ▶️", callback_data=export_callback(f"exp_pick_end:{start_key}:{int(start_rid)}:{view_year+1}:{view_month}:{return_day_key}")),
    )
    kb.row(IB("🔙 Изменить начало", callback_data=export_callback(
        f"exp_pick_set_start:{datetime.strptime(start_key, '%Y-%m-%d').year}:{datetime.strptime(start_key, '%Y-%m-%d').month}:{datetime.strptime(start_key, '%Y-%m-%d').day}:{return_day_key}"
    )))
    return kb


def _export_end_record_keyboard(chat_id: int, start_key: str, start_rid: int, end_key: str, return_day_key: str):
    store = get_chat_store(chat_id)
    kb = types.InlineKeyboardMarkup(row_width=1)
    all_recs = sorted_records_for_day(store, end_key)
    positions = {_record_int_id(rec): idx for idx, rec in enumerate(all_recs)}
    displayed = 0
    for rec in expense_anchor_records_for_day(store, end_key):
        rid = _record_int_id(rec)
        if end_key == start_key and start_rid and positions.get(rid, -1) < positions.get(int(start_rid), 0):
            continue
        displayed += 1
        kb.row(IB(expense_anchor_button_label(rec, store), callback_data=export_callback(
            f"exp_pick_end_record:{start_key}:{int(start_rid)}:{end_key}:{rid}:{return_day_key}"
        )))
    if not displayed:
        kb.row(IB("Нет подходящих расходов в этот день", callback_data="none"))
    kb.row(IB("✅ Продолжить до конца дня", callback_data=export_callback(
        f"exp_pick_end_record:{start_key}:{int(start_rid)}:{end_key}:0:{return_day_key}"
    )))
    end_dt = datetime.strptime(end_key, "%Y-%m-%d")
    kb.row(IB("🔙 Назад к календарю", callback_data=export_callback(
        f"exp_pick_end:{start_key}:{int(start_rid)}:{end_dt.year}:{end_dt.month}:{return_day_key}"
    )))
    return kb


def _export_format_keyboard(start_key: str, start_rid: int, end_key: str, end_rid: int, return_day_key: str):
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.row(
        IB("📄 CSV", callback_data=export_callback(
            f"exp_send:{start_key}:{int(start_rid)}:{end_key}:{int(end_rid)}:csv:{return_day_key}"
        )),
        IB("📊 Excel", callback_data=export_callback(
            f"exp_send:{start_key}:{int(start_rid)}:{end_key}:{int(end_rid)}:xlsx:{return_day_key}"
        )),
    )
    kb.row(IB("📊 Excel стат", callback_data=export_callback(
        f"exp_send:{start_key}:{int(start_rid)}:{end_key}:{int(end_rid)}:xlsxstat:{return_day_key}"
    )))
    end_dt = datetime.strptime(end_key, "%Y-%m-%d")
    kb.row(IB("🔙 Изменить конец", callback_data=export_callback(
        f"exp_pick_set_end:{start_key}:{int(start_rid)}:{end_dt.year}:{end_dt.month}:{end_dt.day}:{return_day_key}"
    )))
    kb.row(IB("❌ Вернуться в CSV / Excel", callback_data=f"d:{return_day_key}:csv_all"))
    return kb


def _exact_export_rows(chat_id: int, start_key: str, start_rid: int, end_key: str, end_rid: int):
    store = get_chat_store(chat_id)
    rows = []
    for day_key, rec in exact_record_range(store, start_key, start_rid, end_key, end_rid):
        rows.append((fmt_date_table(day_key), fmt_csv_amount(rec.get("amount")), rec.get("note", "")))
    return rows


def build_exact_category_stats_xlsx_rows(target_chat_id: int, start_key: str, start_rid: int, end_key: str, end_rid: int) -> list[list]:
    """Excel стат по пользовательскому образцу с настоящими Excel-формулами."""
    store = get_chat_store(target_chat_id)
    records = exact_record_range(store, start_key, start_rid, end_key, end_rid)
    cats_map = calc_categories_for_record_range(store, start_key, start_rid, end_key, end_rid)
    categories = get_ordered_category_names(cats=cats_map, store=store)
    clean_categories = [_clean_category_display_name(x) for x in categories]
    headers = ["Дата", "Описание", "Приход"] + clean_categories
    rows = [headers]
    income_total = 0.0
    expense_total = 0.0
    cat_totals = {cat: 0.0 for cat in categories}
    prev_day = None
    for day_key, rec in records:
        try:
            amount = float(rec.get("amount", 0) or 0)
        except Exception:
            amount = 0.0
        if prev_day is not None and day_key != prev_day:
            rows.append([])
        prev_day = day_key
        row = [fmt_date_table(day_key), str(rec.get("note") or ""), ""] + [""] * len(categories)
        if amount >= 0:
            income_total += amount
            row[2] = int(round(amount)) if float(amount).is_integer() else amount
        else:
            value = abs(amount)
            expense_total += value
            category = resolve_expense_category_for_record(rec, store)
            if category in cat_totals:
                cat_totals[category] += value
                idx = categories.index(category)
                row[3 + idx] = int(round(value)) if float(value).is_integer() else value
        rows.append(row)

    # Data ends before the separator preceding totals. Blank day separators are intentionally included in SUM ranges.
    data_last_row = max(2, len(rows))
    rows.append([])
    sum_row_num = len(rows) + 1
    sum_row = ["", "Сумма по статьям", {"formula": f"SUM(C2:C{data_last_row})", "value": income_total}]
    for idx, cat in enumerate(categories, start=4):
        col = _xlsx_col_name(idx)
        sum_row.append({"formula": f"SUM({col}2:{col}{data_last_row})", "value": cat_totals.get(cat, 0.0)})
    rows.append(sum_row)
    rows.append([])

    expense_row_num = len(rows) + 1
    if categories:
        first_cat = _xlsx_col_name(4)
        last_cat = _xlsx_col_name(3 + len(categories))
        expense_formula = f"SUM({first_cat}{sum_row_num}:{last_cat}{sum_row_num})"
    else:
        expense_formula = "0"
    rows.append(["", "Расход", {"formula": expense_formula, "value": expense_total}] + [""] * len(categories))
    income_row_num = len(rows) + 1
    rows.append(["", "Приход", {"formula": f"C{sum_row_num}", "value": income_total}] + [""] * len(categories))
    balance_row_num = len(rows) + 1
    rows.append(["", "Остаток на руках", {"formula": f"C{income_row_num}-C{expense_row_num}", "value": income_total - expense_total}] + [""] * len(categories))
    return rows

def send_exact_range_export(recipient_chat_id: int, target_chat_id: int, start_key: str, start_rid: int, end_key: str, end_rid: int, file_type: str):
    """Фоновый экспорт между двумя точными границами включительно."""
    trace = ProcessTrace(recipient_chat_id, f"Точный экспорт {str(file_type).upper()}: {get_chat_display_name(target_chat_id)}").start()
    tmp_name = None
    try:
        file_type = str(file_type or "csv").lower()
        if file_type not in {"csv", "xlsx", "xlsxstat"}:
            file_type = "csv"
        rows = _exact_export_rows(target_chat_id, start_key, int(start_rid), end_key, int(end_rid))
        if not rows:
            send_and_auto_delete(recipient_chat_id, "Нет записей в выбранном точном диапазоне.", 10)
            trace.finish("экспорт завершён без данных")
            return
        ext = "xlsx" if file_type in {"xlsx", "xlsxstat"} else "csv"
        tmp_name = os.path.join(
            MEGA_LOCAL_TMP_DIR,
            f"exact_export_{target_chat_id}_{int(time.time() * 1000)}.{ext}",
        )
        if file_type == "xlsxstat":
            xlsx_rows = build_exact_category_stats_xlsx_rows(target_chat_id, start_key, int(start_rid), end_key, int(end_rid))
            _write_simple_xlsx(tmp_name, xlsx_rows, sheet_name="Excel стат")
        elif ext == "xlsx":
            xlsx_rows = [["Дата", "Описание", "Приход", "Расход"]]
            for date_v, amount_v, note_v in rows:
                try:
                    parsed_amount = parse_csv_amount(amount_v)
                except Exception:
                    parsed_amount = 0.0
                xlsx_rows.append(_xlsx_record_row(date_v, parsed_amount, note_v))
            _write_simple_xlsx(tmp_name, insert_blank_rows_between_days(xlsx_rows, header_rows=1), sheet_name="Точный период")
        else:
            with open(tmp_name, "w", newline="", encoding="utf-8") as fh:
                writer = csv.writer(fh)
                writer.writerow(["date", "amount", "note"])
                write_csv_rows_with_day_gaps(writer, rows, 3)

        chat_name = _safe_export_name_part(
            get_chat_name_for_filename(target_chat_id) or get_chat_display_name(target_chat_id),
            f"chat_{target_chat_id}",
        )
        start_label = fmt_date_backup(start_key).replace(":", ".")
        end_label = fmt_date_backup(end_key).replace(":", ".")
        display_name = f"{chat_name}_({start_label}-{end_label})_{'excel_стат' if file_type == 'xlsxstat' else 'точный'}.{ext}"
        store = get_chat_store(target_chat_id)
        caption = (
            f"🎯 {'Excel стат' if file_type == 'xlsxstat' else ('Excel' if ext == 'xlsx' else 'CSV')} — точный период\n"
            f"▶️ {exact_boundary_text(store, start_key, start_rid, True)}\n"
            f"⏹ {exact_boundary_text(store, end_key, end_rid, False)}"
        )
        fobj = file_bytesio_named(tmp_name, display_name)
        if fobj:
            _tg_call_retry(
                bot.send_document,
                recipient_chat_id,
                fobj,
                caption=caption,
                purpose="exact_export_send_document",
            )
        trace.finish("точный экспорт завершён")
    except Exception as exc:
        trace.fail(exc)
        log_error(f"send_exact_range_export({target_chat_id}): {exc}")
    finally:
        if tmp_name:
            try:
                os.remove(tmp_name)
            except Exception:
                pass


def build_csv_menu(day_key: str, chat_id: int | None = None):
    kb = types.InlineKeyboardMarkup(row_width=4)
    _add_export_period_rows(kb, day_key, "d")
    try:
        ref_dt = datetime.strptime(day_key, "%Y-%m-%d")
    except Exception:
        ref_dt = now_local()
    kb.row(IB(
        "🎯 Произвольный точный период",
        callback_data=export_callback(f"exp_pick_start:{ref_dt.year}:{ref_dt.month}:{day_key}"),
    ))
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
USD_DIRECT_EDIT_TOKEN = "EDITUSD"


def compose_direct_edit_insert_value(target_chat_id: int, rid: int, day_key: str, amount, note: str = "") -> str:
    """Текст для быстрой вставки редактирования записи через inline-поле Telegram.
    Метаданные спрятаны в скобках. Пользователь меняет только строку суммы ниже.
    После отправки бот удалит служебную строку/сообщение и обновит запись.
    """
    value = compose_edit_input_value(amount, note)
    meta = f"{DIRECT_EDIT_TOKEN}|{int(target_chat_id)}|{int(rid)}|{str(day_key)[:10]}|"
    return f"({meta} служебное — можно не трогать)\n\n{value}"


def compose_usd_edit_insert_value(target_chat_id: int, rid: int, day_key: str, amount, note: str = "") -> str:
    value = compose_edit_input_value(amount, note)
    meta = f"{USD_DIRECT_EDIT_TOKEN}|{int(target_chat_id)}|{int(rid)}|{str(day_key)[:10]}|"
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
        token_kind = USD_DIRECT_EDIT_TOKEN if USD_DIRECT_EDIT_TOKEN + "|" in text else DIRECT_EDIT_TOKEN if DIRECT_EDIT_TOKEN + "|" in text else None
        if not token_kind:
            return False

        # Формат: (EDITREC/EDITUSD|chat|rid|day| служебное...) + ниже обычный текст суммы.
        m = re.search(r"\((%s\|[^)]*)\)" % re.escape(token_kind), text)
        if m:
            meta_text = m.group(1)
            parts = meta_text.split("|", 4)
            if len(parts) < 4:
                return False
            _, target_s, rid_s, day_key = parts[:4]
            value_text = (text[:m.start()] + " " + text[m.end():]).strip()
        else:
            # Старый формат для совместимости: EDITREC|chat|rid|day| сумма описание
            text = text[text.find(token_kind + "|"):]
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

        if token_kind == USD_DIRECT_EDIT_TOKEN:
            amount, note = parse_usd_edit_value(value_text)
            with locked_chat(target_chat_id):
                rec = next((r for r in get_chat_store(target_chat_id).get("records", []) if int(r.get("id", -1)) == int(rid)), None)
                ok = rec is not None
                if rec is not None:
                    rec["usd_amount"] = float(amount)
                    rec["usd_note"] = str(note or rec.get("usd_note") or rec.get("note") or "")
                    rec["usd_only"] = bool(rec.get("usd_only", False) and not float(rec.get("amount", 0) or 0))
                    rebuild_month_short_ids(target_chat_id)
                    save_data(data, chat_ids=[target_chat_id])
        else:
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
        if token_kind == USD_DIRECT_EDIT_TOKEN:
            send_and_auto_delete(chat_id, f"✅ USD-запись обновлена: {fmt_num(amount)} USD {note}", 8)
        else:
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
        # v91: Telegram показывает @имя_бота, а сам текст начинается с новой строки, не сплошняком.
        kb.row(IB("✍️ Вставить текст", switch_inline_query_current_chat=("\n" + str(insert_text))[:256]))
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
def build_usd_edit_records_keyboard(day_key: str, chat_id: int):
    month_key = str(day_key)[:7]
    kb = types.InlineKeyboardMarkup(row_width=2)
    rows = usd_records_for_month(int(chat_id), month_key)
    for rec in rows:
        rid = int(rec.get("id"))
        amt = float(rec.get("usd_amount", 0) or 0)
        sid = str(rec.get("usd_short_id") or rec.get("short_id") or f"U{rid}")
        label = f"{sid} {('+' if amt >= 0 else '-')}${fmt_num_plain(abs(amt))}"
        insert_text = compose_usd_edit_insert_value(chat_id, rid, _record_day_key(rec), amt, rec.get("usd_note") or rec.get("note", ""))
        kb.row(IB(label, callback_data="none"), make_direct_edit_insert_button("✏️", insert_text))
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
                rows.append((fmt_date_table(day_key), fmt_csv_amount(r.get("amount")), r.get("note", "")))
            caption = f"📅 CSV за день {fmt_date_table(day_key)}: {get_chat_display_name(target_chat_id)}"
        elif mode == "week":
            base = datetime.strptime(day_key, "%Y-%m-%d")
            start = base - timedelta(days=6)
            for i in range(7):
                dk = (start + timedelta(days=i)).strftime("%Y-%m-%d")
                for r in store.get("daily_records", {}).get(dk, []) or []:
                    rows.append((fmt_date_table(dk), fmt_csv_amount(r.get("amount")), r.get("note", "")))
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
                        rows.append((fmt_date_table(dk), fmt_csv_amount(r.get("amount")), r.get("note", "")))
            caption = f"📆 CSV за месяц: {get_chat_display_name(target_chat_id)}"
        elif mode == "wedthu":
            base = datetime.strptime(day_key, "%Y-%m-%d")
            while base.weekday() != 2:
                base -= timedelta(days=1)
            for i in range(2):
                dk = (base + timedelta(days=i)).strftime("%Y-%m-%d")
                for r in store.get("daily_records", {}).get(dk, []) or []:
                    rows.append((fmt_date_table(dk), fmt_csv_amount(r.get("amount")), r.get("note", "")))
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
            rows.append((fmt_date_table(dk), fmt_csv_amount(r.get("amount")), r.get("note", "")))

    if mode == "day":
        _append_day(day_key)
        label = f"за день {fmt_date_table(day_key)}"
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
        raw_mode = str(mode or "all")
        if raw_mode.startswith("xlsxstat_"):
            raw_mode = raw_mode[len("xlsxstat_"):]
        mode = raw_mode.replace("csv_", "").replace("xlsx_", "")
        if mode == "all_real":
            mode = "all"

        if mode == "all" and file_type in {"csv", "xlsx"}:
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
        ext = "xlsx" if file_type in {"xlsx", "xlsxstat"} else "csv"
        if not rows and ext != "xlsx":
            trace.step("строк нет — отправляет уведомление")
            send_info(recipient_chat_id, f"Нет данных {label}.")
            trace.finish("экспорт завершён без данных")
            return
        if not rows and ext == "xlsx":
            trace.step("строк нет — создаёт пустой Excel с заголовками")
        tmp_name = os.path.join(MEGA_LOCAL_TMP_DIR, f"export_{target_chat_id}_{mode}_{int(time.time() * 1000)}.{ext}")
        if file_type == "xlsxstat":
            safe_chat = mega_safe_name(get_chat_display_name(target_chat_id), "chat")
            display_name = f"{safe_chat}_{mode}_{day_key}_excel_статьи.xlsx"
        else:
            display_name = export_display_filename(target_chat_id, mode, day_key, ext)
        if file_type == "xlsxstat":
            trace.step("создаёт Excel по статьям с формулами")
            store = get_chat_store(target_chat_id)
            base = datetime.strptime(day_key, "%Y-%m-%d")
            if mode == "day":
                start_key = end_key = day_key
            elif mode == "week":
                start_key = (base - timedelta(days=6)).strftime("%Y-%m-%d")
                end_key = day_key
            elif mode == "month":
                start_key = base.replace(day=1).strftime("%Y-%m-%d")
                end_key = day_key
            elif mode == "wedthu":
                start_dt = base
                while start_dt.weekday() != 2:
                    start_dt -= timedelta(days=1)
                start_key = start_dt.strftime("%Y-%m-%d")
                end_key = (start_dt + timedelta(days=1)).strftime("%Y-%m-%d")
            else:
                keys = sorted((store.get("daily_records", {}) or {}).keys())
                start_key = keys[0] if keys else day_key
                end_key = keys[-1] if keys else day_key
            xlsx_rows = build_exact_category_stats_xlsx_rows(target_chat_id, start_key, 0, end_key, 0)
            _write_simple_xlsx(tmp_name, xlsx_rows, sheet_name="Статьи")
        elif ext == "xlsx":
            trace.step("создаёт временный Excel файл")
            xlsx_rows = [["Дата", "Описание", "Приход", "Расход"]]
            for date_v, amount_v, note_v in rows:
                try:
                    parsed_amount = parse_csv_amount(amount_v)
                except Exception as e_amount:
                    log_error(f"xlsx export amount parse skip: chat={get_chat_display_name(target_chat_id)} amount={amount_v!r} note={note_v!r}: {e_amount}")
                    parsed_amount = 0.0
                xlsx_rows.append(_xlsx_record_row(date_v, parsed_amount, note_v))
            _write_simple_xlsx(tmp_name, insert_blank_rows_between_days(xlsx_rows, header_rows=1), sheet_name="Экспорт")
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
                caption=f"📂 {'Excel статьи' if file_type == 'xlsxstat' else ('Excel' if ext == 'xlsx' else 'CSV')} {label}: {get_chat_display_name(target_chat_id)}",
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
    items = category_edit_items_for_chat(target_chat_id)
    if not items:
        kb.row(IB("Нет статей", callback_data="none"))
    for item in items:
        mark = "Б" if item.get("base") else "С"
        kb.row(IB(f"✏️ {item.get('name')} ({mark})", callback_data=fvcat_callback(f"fvcat_edit_pick:{target_chat_id}:{item.get('slug')}:{owner_day_key}")))
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
    # Любой экран статей заменяет на этом message_id прежнее фин-окно. Сразу фиксируем это,
    # чтобы реестр не перерисовал его обратно; финансово-зависимые wthu/show ниже станут динамическими.
    try:
        register_static_open_view(
            owner_chat_id, call.message.message_id, code=action,
            day_key=parts[2] if len(parts) > 2 else None,
            params={"target_chat_id": target_chat_id, "view_action": action},
        )
    except Exception:
        pass
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
            wm_owner(f"✏️ Изменить статью\n👁 {get_chat_display_name(target_chat_id)}\n\nВыберите статью. Б = базовая, С = своя.", 18),
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
        register_open_window(
            owner_chat_id, call.message.message_id, "fin_categories_view", code="fvcat:wthu", day_key=ref,
            params={"target_chat_id": target_chat_id, "owner_day_key": owner_day_key, "view_action": "wthu", "ref": ref},
        )
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
        register_open_window(
            owner_chat_id, call.message.message_id, "fin_categories_view", code="fvcat:show", day_key=start,
            params={"target_chat_id": target_chat_id, "owner_day_key": owner_day_key, "view_action": "show", "start": start, "end": end, "slug": slug},
        )
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
        IB(f"{russian_month_name(center_day.month)} {center_day.year}", callback_data="none"),
        IB("Месяц ➡️", callback_data=f"fc:{target_chat_id}:{next_month.strftime('%Y-%m-%d')}:{owner_day_key}"),
    )
    prev_year = center_day.replace(year=center_day.year - 1, day=1)
    next_year = center_day.replace(year=center_day.year + 1, day=1)
    kb.row(
        IB("◀️ Год", callback_data=f"fc:{target_chat_id}:{prev_year.strftime('%Y-%m-%d')}:{owner_day_key}"),
        IB(str(center_day.year), callback_data="none"),
        IB("Год ▶️", callback_data=f"fc:{target_chat_id}:{next_year.strftime('%Y-%m-%d')}:{owner_day_key}"),
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


# ─────────────────────────────────────────────────────────────
# ⚡ Fast UI edit queue
# ─────────────────────────────────────────────────────────────
# Telegram даёт 429, если слишком часто редактировать одно окно.
# Поэтому кнопки больше не ждут retry_after внутри callback:
# • редактирование одного сообщения ограничено частотой;
# • частые клики собираются в одно последнее обновление;
# • 429 не держит обработчик кнопки, а просто пропускает лишнее обновление.
UI_EDIT_MIN_INTERVAL_SECONDS = float(os.getenv("UI_EDIT_MIN_INTERVAL_SECONDS", "0.20") or "0.20")
_ui_edit_lock = threading.RLock()
_ui_edit_last_ts = {}
_ui_edit_pending = {}
_ui_edit_timers = {}


def _ui_edit_key(chat_id: int, message_id: int):
    return (int(chat_id), int(message_id))


def _perform_fast_ui_edit(payload: dict) -> str:
    chat_id = int(payload.get("chat_id"))
    message_id = int(payload.get("message_id"))
    text = payload.get("text") or ""
    reply_markup = payload.get("reply_markup")
    parse_mode = payload.get("parse_mode")
    purpose = payload.get("purpose") or "fast_ui_edit"
    try:
        _tg_call_retry(
            bot.edit_message_text,
            text,
            chat_id=chat_id,
            message_id=message_id,
            reply_markup=reply_markup,
            parse_mode=parse_mode,
            attempts=1,
            purpose=purpose + "_text",
        )
        return "ok"
    except Exception as e1:
        low = str(e1).lower()
        if "message is not modified" in low:
            return "ok"
        if is_telegram_429(e1):
            try:
                bot_journal("ui_edit_rate_limited", chat_id, f"{purpose}: {str(e1)[:220]}", "WARN")
            except Exception:
                pass
            return "rate_limited"
        if "message to edit not found" in low or "message can't be edited" in low:
            return "not_found"
        try:
            _tg_call_retry(
                bot.edit_message_caption,
                chat_id=chat_id,
                message_id=message_id,
                caption=text,
                reply_markup=reply_markup,
                parse_mode=parse_mode,
                attempts=1,
                purpose=purpose + "_caption",
            )
            return "ok"
        except Exception as e2:
            low2 = str(e2).lower()
            if "message is not modified" in low2:
                return "ok"
            if is_telegram_429(e2):
                try:
                    bot_journal("ui_edit_rate_limited", chat_id, f"{purpose}: {str(e2)[:220]}", "WARN")
                except Exception:
                    pass
                return "rate_limited"
            if "message to edit not found" in low2 or "message can't be edited" in low2:
                return "not_found"
            try:
                bot_journal("ui_edit_failed", chat_id, f"{purpose}: {str(e1)[:180]} / {str(e2)[:180]}", "WARN")
            except Exception:
                pass
            return "failed"


def _run_pending_ui_edit(key):
    with _ui_edit_lock:
        payload = _ui_edit_pending.pop(key, None)
        _ui_edit_timers.pop(key, None)
        if not payload:
            return
        _ui_edit_last_ts[key] = time.time()
    _perform_fast_ui_edit(payload)


def fast_ui_edit_message_text(chat_id: int, message_id: int, text: str, reply_markup=None, parse_mode=None, purpose: str = "fast_ui") -> str:
    key = _ui_edit_key(chat_id, message_id)
    payload = {
        "chat_id": int(chat_id),
        "message_id": int(message_id),
        "text": text,
        "reply_markup": reply_markup,
        "parse_mode": parse_mode,
        "purpose": purpose,
    }
    now_ts = time.time()
    with _ui_edit_lock:
        last_ts = float(_ui_edit_last_ts.get(key, 0) or 0)
        wait = max(0.0, effective_ui_edit_interval() - (now_ts - last_ts))
        if wait > 0:
            _ui_edit_pending[key] = payload
            scheduler_key = f"ui-edit:{int(chat_id)}:{int(message_id)}"
            DELAYED_SCHEDULER.cancel(scheduler_key)
            deadline = DELAYED_SCHEDULER.schedule(
                scheduler_key,
                wait + 0.05,
                _run_pending_ui_edit,
                key,
            )
            _ui_edit_timers[key] = deadline
            return "scheduled"
        _ui_edit_last_ts[key] = now_ts
    return _perform_fast_ui_edit(payload)


def cancel_fast_ui_edit(chat_id: int, message_id: int):
    key = _ui_edit_key(chat_id, message_id)
    with _ui_edit_lock:
        _ui_edit_pending.pop(key, None)
        _ui_edit_timers.pop(key, None)
    DELAYED_SCHEDULER.cancel(f"ui-edit:{int(chat_id)}:{int(message_id)}")

def safe_edit(bot, call, text, reply_markup=None, parse_mode=None):
    """Быстрое обновление окна.
    Не держит callback при Telegram 429 и собирает частые клики в одно последнее обновление.
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
    result = fast_ui_edit_message_text(
        chat_id, msg_id, text,
        reply_markup=reply_markup,
        parse_mode=parse_mode,
        purpose="safe_edit_fast",
    )
    if result in {"ok", "scheduled", "rate_limited"}:
        if result == "rate_limited":
            try:
                bot.answer_callback_query(call.id, "Обновление отложено: Telegram ограничил частые клики.", show_alert=False)
            except Exception:
                pass
        try:
            _touch_v98_auto_close_for_callback(chat_id, msg_id, getattr(call, "data", ""))
        except Exception:
            pass
        return

    # Только если старое сообщение реально потеряно, создаём новое окно.
    try:
        if chat_buttons_current_window_enabled(chat_id):
            try:
                bot.answer_callback_query(call.id, "Текущее окно недоступно, новое не создаю.", show_alert=False)
            except Exception:
                pass
            return
    except Exception:
        pass
    try:
        sent = _tg_call_retry(
            bot.send_message,
            chat_id,
            text,
            reply_markup=reply_markup,
            parse_mode=parse_mode,
            attempts=1,
            purpose="safe_edit_send_fallback",
        )
        try:
            _touch_v98_auto_close_for_callback(chat_id, sent.message_id, getattr(call, "data", ""))
        except Exception:
            pass
    except Exception as e:
        if not is_telegram_429(e):
            log_error(f"safe_edit fallback send {chat_id}: {e}")


def safe_edit_current_only(bot, call, text, reply_markup=None, parse_mode=None):
    """Редактирует только текущее окно, без создания нового и без ожидания retry_after."""
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
    result = fast_ui_edit_message_text(
        chat_id, msg_id, text,
        reply_markup=reply_markup,
        parse_mode=parse_mode,
        purpose="safe_edit_current_only_fast",
    )
    if result == "rate_limited":
        try:
            bot.answer_callback_query(call.id, "Обновление отложено: слишком много кликов.", show_alert=False)
        except Exception:
            pass
    try:
        _touch_v98_auto_close_for_callback(chat_id, msg_id, getattr(call, "data", ""))
    except Exception:
        pass
    return result in {"ok", "scheduled", "rate_limited"}

def send_or_edit_categories_window(chat_id, text, reply_markup=None, parse_mode=None, preferred_message_id=None, marker_action: str | None = None):
    """Отдельное окно для отчёта по статьям расходов (одно сообщение на чат).

    marker_action позволяет заранее и однозначно закрепить константный маркер
    за конкретным окном, независимо от порядка кнопок.
    """
    try:
        marker_key = marker_action or _window_key_from_markup(reply_markup)
        text = window_mark(text, _window_marker_code(marker_key, "Ф"), html_mode=(str(parse_mode or "").upper() == "HTML"))
    except Exception:
        pass
    store = get_chat_store(chat_id)
    store["categories_refresh_state"] = {
        "marker_action": marker_action or "",
        "callbacks": _markup_callback_values(reply_markup),
    }
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
            register_open_window(chat_id, target_id, "categories", code=marker_action or "")
            save_data(data)
            return target_id
        except Exception as e:
            if "message is not modified" in str(e).lower():
                store["categories_msg_id"] = target_id
                register_open_window(chat_id, target_id, "categories", code=marker_action or "")
                save_data(data)
                return target_id
            log_error(f"send_or_edit_categories_window edit failed {chat_id}:{target_id}: {e}")
            if store.get("categories_msg_id") == target_id:
                unregister_open_window(chat_id, target_id)
                store["categories_msg_id"] = None
                save_data(data)

    sent = bot.send_message(chat_id, text, reply_markup=reply_markup, parse_mode=parse_mode)
    store["categories_msg_id"] = sent.message_id
    register_open_window(chat_id, sent.message_id, "categories", code=marker_action or "")
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


def build_owner_instruction_text() -> str:
    return (
        "📘 Инструкция по кнопкам\n\n"
        "🏠 Основное финансовое окно\n"
        "⬅️ День / День ➡️ — перейти на соседний день. 📅 Сегодня — вернуться к текущей дате.\n"
        "📅 Дата — открыть календарь; в календаре можно менять месяц и год.\n"
        "📊 Отчёт — месячный отчёт. 🧮 Итог — общий итог. 🏦 с ост — остаток после каждого расхода.\n"
        "✏️ Изменить / 🗑 Удалить — работа с финансовыми записями. 📦 Статьи — расходы по категориям.\n"
        "📄 CSV — окно Ф47: пять периодов; в каждой строке Период / CSV / Excel / Excel статьи. Точный период открывается отдельной кнопкой ниже.\n\n"
        "💱 Валюта\n"
        "ARS — отдельный учёт в песо. ARS-USD — тот же ARS с эквивалентом по курсу. USD — отдельный долларовый учёт со всеми финансовыми функциями.\n"
        "/ost — включает или выключает подпись «ост:» в окне остатка.\n\n"
        "💰Перес\n"
        "Обычно — бот-копия без кнопки и без /izm_R. Кнопка — под копией появляется ✏️ Изменить. Слеш — в текст копии добавляется /izm_R. При переключении обновляются существующие копии от открытой даты до сегодня.\n\n"
        "🔐 Секрет\n"
        "Секрет у выбранного чата — включает тотальный секрет именно для этого чата. В секрет участвуют и созданные ботом копии. 🪷 Маска — показывает нейтральное сообщение вместо удалённого секретного сообщения.\n\n"
        "📦 Статьи\n"
        "Ф110 — точный диапазон операций; 💵 USD включает долларовое отображение статей. ↕️ Расположение открывает Ф152: сначала выберите статью, затем номер новой позиции.\n"
        "📚 Описание статей — ключевые слова категорий. ➕/✏️/🗑 — добавить, изменить или удалить пользовательскую статью.\n\n"
        "ℹ️ INFO\n"
        "📓 Журнал / 🗂 Журналы чатов — журналы действий. Кнопки в текущем окне — режим обновления интерфейса. Финансы — настройка финансового режима.\n"
        "💵 Доллар — выбор ARS / ARS-USD / USD. 💰Перес — оформление бот-копий. Финансы-кнопки — записи как inline-кнопки.\n"
        "☁️ MEGA — приоритет резервного копирования. 📘 Инструкция — это окно. 🚦 Очереди — состояние рабочих очередей.\n\n"
        "📤 Пересылка\n"
        "Меню пересылки задаёт связанные чаты и финансовую обработку копий. Режим «как у владельца» создаёт отдельный owner scope: настройки такого владельца сохраняются независимо.\n\n"
        "💾 Сохранение\n"
        "После финансового изменения данные сначала сохраняются, затем ставится быстрый backup, после чего обновляются связанные открытые окна и планируется полный backup."
    )

def build_owner_instruction_keyboard(chat_id: int):
    kb = types.InlineKeyboardMarkup()
    kb.row(IB("🚦 Очереди", callback_data="info_queues"))
    kb.row(IB("🔙 Назад в Инфо", callback_data=f"d:{get_chat_store(chat_id).get('current_view_day', today_key())}:info"))
    kb.row(IB("❌ Закрыть", callback_data="info_close"))
    return kb


def all_task_pool_stats() -> list[dict]:
    return [
        WEBHOOK_TASK_POOL.stats(), FINANCE_TASK_POOL.stats(), FORWARD_TASK_POOL.stats(),
        DELTA_TASK_POOL.stats(), BACKUP_TASK_POOL.stats(), EXPORT_TASK_POOL.stats(), GENERAL_TASK_POOL.stats(),
        JOURNAL_TASK_POOL.stats(), DELAYED_TASK_POOL.stats(), DOZVON_TASK_POOL.stats(),
    ]


def build_queue_status_text() -> str:
    lines = ["🚦 Очереди и нагрузка", ""]
    for st in all_task_pool_stats():
        lines.append(
            f"{st['name']}: {st['active']}/{st['workers']} работают, "
            f"ожидают {st['pending']}, ключей {st['keys']}, "
            f"отказов {st['rejected']}, ошибок {st['failed']}, max ожидание {st['max_wait']}с"
        )
    with timer_lock:
        lines.append("")
        lines.append(f"Таймеров полного бэкапа: {len(_backup_timers)}")
        lines.append(f"Таймеров delta: {len(_quick_backup_timers)}")
        lines.append(f"Dirty чатов: {len(_backup_dirty_chats)}")
    with _delta_state_lock:
        lines.append(f"Delta pending chats: {len(_delta_pending_chats)}")
        lines.append(f"Global full pending: {'да' if _global_snapshot_pending else 'нет'}")
    ds = DELAYED_SCHEDULER.stats()
    lines.append(f"Планировщик: задач {ds['scheduled']}, отменено {ds['cancelled']}, выполнено {ds['executed']}")
    lines.append(f"Excel-бэкап всех чатов: {backup_excel_all_label()}")
    lines.append(f"Telegram общий интервал: {TELEGRAM_GLOBAL_MIN_GAP:.3f}с")
    return "\n".join(lines)


CHAT_JOURNAL_PAGE_SIZE = 20


def _journal_chat_items():
    try:
        return _collect_process_menu_items()
    except Exception:
        items = []
        for cid in (data.get("chats", {}) or {}).keys():
            try:
                items.append((int(cid), get_chat_display_name(int(cid))))
            except Exception:
                pass
        return sorted(items, key=lambda x: str(x[1]).lower())


def build_chat_journal_menu_text(page: int = 0) -> str:
    items = _journal_chat_items()
    pages = max(1, (len(items) + CHAT_JOURNAL_PAGE_SIZE - 1) // CHAT_JOURNAL_PAGE_SIZE)
    page = max(0, min(int(page), pages - 1))
    enabled = sum(1 for cid, _ in items if is_chat_journal_enabled(cid))
    return wm_owner(
        "📓 Журналы по чатам\n\n"
        "Общий журнал по умолчанию выключен. Здесь можно включать запись только для нужных чатов.\n\n"
        f"Включено: {enabled} из {len(items)}\nСтраница: {page + 1}/{pages}",
        9,
    )


def build_chat_journal_menu_keyboard(page: int = 0):
    items = _journal_chat_items()
    pages = max(1, (len(items) + CHAT_JOURNAL_PAGE_SIZE - 1) // CHAT_JOURNAL_PAGE_SIZE)
    page = max(0, min(int(page), pages - 1))
    start = page * CHAT_JOURNAL_PAGE_SIZE
    chunk = items[start:start + CHAT_JOURNAL_PAGE_SIZE]
    kb = types.InlineKeyboardMarkup(row_width=1)
    for cid, title in chunk:
        icon = "✅" if is_chat_journal_enabled(cid) else "❌"
        kb.row(IB(f"{icon} 📓 {chat_button_title(cid, title)}", callback_data=f"journal_chat_toggle:{cid}:{page}"))
    if pages > 1:
        row = []
        if page > 0:
            row.append(IB("⬅️", callback_data=f"journal_chats_open:{page - 1}"))
        row.append(IB(f"{page + 1}/{pages}", callback_data="none"))
        if page + 1 < pages:
            row.append(IB("➡️", callback_data=f"journal_chats_open:{page + 1}"))
        kb.row(*row)
    kb.row(IB("🔙 Назад в Инфо", callback_data="journal_chats_back"))
    return kb


def build_version_menu_text() -> str:
    active = active_bot_behavior_profile()
    lines = [
        "🧩 Полное переключение версий",
        "",
        "Выбор меняет структуру меню, доступные кнопки, интервалы интерфейса и совместимое поведение выбранной версии. Финансовые записи, остатки, пересылки и бэкапы остаются общими и не удаляются.",
        "",
        "Кнопка выбора версии всегда остаётся в ИНФО, даже в режиме v81.",
        "",
    ]
    for key, cfg in BOT_BEHAVIOR_PROFILES.items():
        mark = "✅" if key == active else "▫️"
        lines.append(f"{mark} {cfg['title']} — {cfg['description']}")
        lines.append(f"   Интервал UI: {cfg['ui_edit_interval']:.2f} сек.; меню: {cfg.get('info_layout', key)}")
    return wm_owner("\n".join(lines), 9)


def build_version_menu_keyboard():
    kb = types.InlineKeyboardMarkup(row_width=1)
    active = active_bot_behavior_profile()
    for key, cfg in BOT_BEHAVIOR_PROFILES.items():
        mark = "✅" if key == active else "▫️"
        kb.row(IB(f"{mark} {cfg['title']}", callback_data=f"version_select:{key}"))
    kb.row(IB("🔙 Назад в Инфо", callback_data="version_back"))
    return kb


def keep_alive_status_text() -> str:
    state = globals().get("KEEP_ALIVE_STATE") or {}
    lines = [
        "💓 Keep-alive / защита от сна",
        "",
        f"Автоматический режим: {'ВКЛ' if KEEP_ALIVE_ENABLED else 'ВЫКЛ'}",
        f"Интервал: {KEEP_ALIVE_INTERVAL_SECONDS} сек.",
        f"APP_URL: {APP_URL or 'не задан'}",
        f"Последний успешный ping: {state.get('last_ok_at') or 'ещё не было'}",
        f"Последняя ошибка: {state.get('last_error') or 'нет'}",
        f"Успешных циклов: {state.get('ok_count', 0)}, ошибок: {state.get('fail_count', 0)}",
        "",
        "Важно: внутренний self-ping поддерживает активность процесса, пока он запущен. Для тарифа хостинга с принудительным сном нужен внешний HTTP-монитор, который обращается к /keepalive.",
    ]
    return wm_owner("\n".join(lines), 9)


def build_info_keyboard(chat_id: int):
    kb = types.InlineKeyboardMarkup()
    layout = version_mode_layout()
    if is_owner_chat(chat_id):
        kb.row(
            IB("📓 Журнал", callback_data="journal_open"),
            IB(journal_toggle_label(), callback_data="journal_toggle"),
        )
        if version_mode_feature("per_chat_journal"):
            kb.row(
                IB("🗂 Журналы чатов", callback_data="journal_chats_open"),
                IB(chat_journal_toggle_label(chat_id), callback_data=f"journal_chat_toggle:{chat_id}:0"),
            )
        kb.row(
            IB(buttons_current_window_label(chat_id), callback_data="buttons_current_toggle"),
            IB(info_finance_toggle_label(chat_id), callback_data="info_finance_off"),
        )
        if version_mode_feature("forward_copy_edit"):
            kb.row(IB(forward_copy_edit_mode_label(chat_id), callback_data="forward_copy_edit_mode_toggle"))
        kb.row(
            IB(forward_menu_style_label(chat_id), callback_data="forward_menu_style_toggle"),
            IB(icon_button_mode_label(chat_id), callback_data="icon_buttons_toggle"),
        )
        kb.row(
            IB(total_secret_mask_label(chat_id), callback_data="total_secret_mask_toggle"),
            IB(f"🕔 {finance_day_start_label(chat_id)}", callback_data="finance_day5_toggle"),
        )
        if version_mode_feature("mega_priority") and layout in {"v82", "v83"}:
            kb.row(IB(mega_backup_priority_label(chat_id), callback_data="mega_priority_toggle"))
        elif version_mode_feature("mega_priority") and layout in {"v84", "v85", "v86", "v87"}:
            kb.row(
                IB(mega_backup_priority_label(chat_id), callback_data="mega_priority_toggle"),
                IB(main_financial_value_buttons_label(chat_id), callback_data="main_financial_values_toggle"),
            )
        if layout in {"v85", "v86", "v87"}:
            if layout == "v87":
                kb.row(
                    IB(gomonk_info_label(chat_id), callback_data="gomonk_open"),
                    IB(currency_mode_label(chat_id), callback_data="currency_menu"),
                )
            elif layout == "v86":
                kb.row(
                    IB(gomonk_info_label(chat_id), callback_data="gomonk_open"),
                    IB(usd_display_label(chat_id), callback_data="usd_display_toggle"),
                )
            else:
                kb.row(IB(gomonk_info_label(chat_id), callback_data="gomonk_open"))
        if layout == "v83":
            kb.row(IB(main_article_buttons_label(chat_id), callback_data="main_articles_toggle"))
        # Кнопка выбора версии присутствует при любом режиме, включая полный откат v81/v82.
        if version_mode_feature("keepalive_menu"):
            kb.row(
                IB(bot_behavior_profile_label(), callback_data="version_menu"),
                IB("💓 Не спать", callback_data="keepalive_status"),
            )
        else:
            kb.row(IB(bot_behavior_profile_label(), callback_data="version_menu"))
        kb.row(
            IB("📘 Инструкция", callback_data="info_instruction"),
            IB("🚦 Очереди", callback_data="info_queues"),
        )
        if active_bot_behavior_profile() in {"v93_current", "v92_current", "v91_current", "v90_current"}:
            kb.row(IB("🧩 Delta / snapshots", callback_data="info_delta_status"))
        if is_primary_owner(chat_id):
            kb.row(IB("👥 /owners", callback_data="additional_owners"))
    else:
        kb.row(IB(info_finance_toggle_label(chat_id), callback_data="info_finance_off"))
        if version_mode_feature("forward_copy_edit"):
            kb.row(IB(forward_copy_edit_mode_label(chat_id), callback_data="forward_copy_edit_mode_toggle"))
        if layout in {"v84", "v85", "v86", "v87"}:
            kb.row(IB(main_financial_value_buttons_label(chat_id), callback_data="main_financial_values_toggle"))
        if layout in {"v85", "v86", "v87"}:
            if layout == "v87":
                kb.row(
                    IB(gomonk_info_label(chat_id), callback_data="gomonk_open"),
                    IB(currency_mode_label(chat_id), callback_data="currency_menu"),
                )
            elif layout == "v86":
                kb.row(
                    IB(gomonk_info_label(chat_id), callback_data="gomonk_open"),
                    IB(usd_display_label(chat_id), callback_data="usd_display_toggle"),
                )
            else:
                kb.row(IB(gomonk_info_label(chat_id), callback_data="gomonk_open"))
        elif layout == "v83":
            kb.row(IB(main_article_buttons_label(chat_id), callback_data="main_articles_toggle"))
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



def _expense_anchor_rows(kb, store: dict, day_key: str, callback_builder, empty_text: str = "Нет расходов в этот день"):
    records = expense_anchor_records_for_day(store, day_key)
    if records:
        for rec in records:
            rid = _record_int_id(rec)
            kb.row(IB(expense_anchor_button_label(rec, store), callback_data=callback_builder(rid)))
    else:
        kb.row(IB(empty_text, callback_data="none"))
    return records


def _send_category_pick_start_record(chat_id: int, message_id: int, start_key: str):
    store = get_chat_store(chat_id)
    kb = types.InlineKeyboardMarkup(row_width=1)
    _expense_anchor_rows(
        kb,
        store,
        start_key,
        lambda rid: cat_callback(f"cat_pick_start_record:{start_key}:{rid}"),
    )
    kb.row(IB("➡️ Продолжить с начала дня", callback_data=cat_callback(f"cat_pick_start_record:{start_key}:0")))
    dt = datetime.strptime(start_key, "%Y-%m-%d")
    kb.row(IB("🔙 Назад к календарю", callback_data=cat_callback(f"cat_pick_start:{dt.year}:{dt.month}")))
    text = (
        "🎯 Точное начало периода\n"
        f"📅 День: {fmt_date_ddmmyy(start_key)}\n\n"
        "Выберите расход, с которого начинать расчёт, или продолжите с начала дня."
    )
    send_or_edit_categories_window(
        chat_id,
        text,
        reply_markup=kb,
        preferred_message_id=message_id,
        marker_action="cat_pick_start_record:*",
    )


def _category_end_day_buttons_precise(start_key: str, start_rid: int, view_year: int, view_month: int):
    kb = types.InlineKeyboardMarkup(row_width=7)
    last_day = calendar.monthrange(int(view_year), int(view_month))[1]
    buttons = []
    for dnum in range(1, last_day + 1):
        day_key = _date_key_from_ymd(view_year, view_month, dnum)
        if day_key < start_key:
            buttons.append(IB("·", callback_data="none"))
        else:
            buttons.append(IB(str(dnum), callback_data=cat_callback(f"cat_pick_set_end3:{start_key}:{int(start_rid)}:{view_year}:{view_month}:{dnum}")))
    for idx in range(0, len(buttons), 7):
        kb.row(*buttons[idx:idx + 7])
    return kb


def _send_category_pick_end_precise(chat_id: int, message_id: int, start_key: str, start_rid: int, view_year: int, view_month: int):
    store = get_chat_store(chat_id)
    kb = _category_end_day_buttons_precise(start_key, start_rid, view_year, view_month)
    prev_y, prev_m = _shift_month(view_year, view_month, -1)
    next_y, next_m = _shift_month(view_year, view_month, 1)
    start_month_key = start_key[:7]
    nav = []
    if f"{prev_y:04d}-{prev_m:02d}" >= start_month_key:
        nav.append(IB("⬅️ Месяц", callback_data=cat_callback(f"cat_pick_end3:{start_key}:{int(start_rid)}:{prev_y}:{prev_m}")))
    else:
        nav.append(IB(" ", callback_data="none"))
    nav.append(IB(f"{russian_month_name(view_month)} {view_year}", callback_data="none"))
    nav.append(IB("Месяц ➡️", callback_data=cat_callback(f"cat_pick_end3:{start_key}:{int(start_rid)}:{next_y}:{next_m}")))
    kb.row(*nav)
    kb.row(IB(f"⏹ По сегодняшний день · {fmt_date_ddmmyy(today_key())}", callback_data=cat_callback(f"cat_pick_today_end:{start_key}:{int(start_rid)}")))
    start_dt = datetime.strptime(start_key, "%Y-%m-%d")
    kb.row(IB("🔙 Изменить начало", callback_data=cat_callback(f"cat_pick_set_start:{start_dt.year}:{start_dt.month}:{start_dt.day}")))
    text = (
        "🎯 Точный период расходов\n"
        f"▶️ Начало: {exact_boundary_text(store, start_key, start_rid, True)}\n\n"
        f"Выберите конечный день: {russian_month_name(view_month)} {view_year}"
    )
    send_or_edit_categories_window(
        chat_id,
        text,
        reply_markup=kb,
        preferred_message_id=message_id,
        marker_action="cat_pick_end3:*",
    )


def _send_category_pick_end_record(chat_id: int, message_id: int, start_key: str, start_rid: int, end_key: str):
    store = get_chat_store(chat_id)
    kb = types.InlineKeyboardMarkup(row_width=1)
    records = expense_anchor_records_for_day(store, end_key)
    displayed = 0
    all_recs = sorted_records_for_day(store, end_key)
    pos = {_record_int_id(r): i for i, r in enumerate(all_recs)}
    for rec in records:
        rid = _record_int_id(rec)
        # В тот же день нельзя закончить раньше выбранного начала.
        if end_key == start_key and start_rid:
            if pos.get(rid, -1) < pos.get(int(start_rid), 0):
                continue
        displayed += 1
        kb.row(IB(expense_anchor_button_label(rec, store), callback_data=cat_callback(f"cat_pick_end_record:{start_key}:{int(start_rid)}:{end_key}:{rid}")))
    if not displayed:
        kb.row(IB("Нет подходящих расходов в этот день", callback_data="none"))
    kb.row(IB("✅ Продолжить до конца дня", callback_data=cat_callback(f"cat_pick_end_record:{start_key}:{int(start_rid)}:{end_key}:0")))
    end_dt = datetime.strptime(end_key, "%Y-%m-%d")
    kb.row(IB("🔙 Назад к календарю", callback_data=cat_callback(f"cat_pick_end3:{start_key}:{int(start_rid)}:{end_dt.year}:{end_dt.month}")))
    text = (
        "🎯 Точный конец периода\n"
        f"▶️ Начало: {exact_boundary_text(store, start_key, start_rid, True)}\n"
        f"📅 Конечный день: {fmt_date_ddmmyy(end_key)}\n\n"
        "Выберите последний расход, который включить в расчёт, или продолжите до конца дня."
    )
    send_or_edit_categories_window(
        chat_id,
        text,
        reply_markup=kb,
        preferred_message_id=message_id,
        marker_action="cat_pick_end_record:*",
    )


def build_categories_record_summary_keyboard(start_key: str, start_rid: int, end_key: str, end_rid: int, store: dict):
    kb = types.InlineKeyboardMarkup(row_width=3)
    cats = calc_categories_for_record_range(store, start_key, start_rid, end_key, end_rid)
    buttons = []
    for category in get_ordered_category_names(cats=cats, store=store):
        slug = get_expense_category_slug(category, store)
        if slug:
            buttons.append(IB(_clean_category_display_name(category), callback_data=cat_callback(f"cat_show_records:{start_key}:{int(start_rid)}:{end_key}:{int(end_rid)}:{slug}")))
    add_buttons_in_rows(kb, buttons, 3)
    if _v85_enabled("usd_categories"):
        usd_on = bool(store.setdefault("settings", {}).get("category_usd_enabled", False))
        kb.row(IB("💵 USD: ВКЛ" if usd_on else "💵 USD: ВЫКЛ", callback_data=cat_callback(f"cat_usd_toggle_records:{start_key}:{int(start_rid)}:{end_key}:{int(end_rid)}")))
    kb.row(IB("↕️ Расположение", callback_data=cat_callback(f"cat_order_open_exact:{start_key}:{int(start_rid)}:{end_key}:{int(end_rid)}")))
    start_dt = datetime.strptime(start_key, "%Y-%m-%d")
    end_dt = datetime.strptime(end_key, "%Y-%m-%d")
    kb.row(
        IB("⬅️ Назад", callback_data=cat_callback(f"cat_pick_set_end3:{start_key}:{int(start_rid)}:{end_dt.year}:{end_dt.month}:{end_dt.day}")),
        IB("🎯 Выбрать заново", callback_data=cat_callback(f"cat_pick_start:{start_dt.year}:{start_dt.month}")),
    )
    kb.row(
        IB("⬅️ Назад осн. окно", callback_data=f"d:{today_key()}:back_main"),
        IB("❌ Закрыть", callback_data=cat_callback("cat_close")),
    )
    return kb


def build_category_record_detail_text(store: dict, start_key: str, start_rid: int, end_key: str, end_rid: int, category: str):
    items = collect_items_for_category_record_range(store, start_key, start_rid, end_key, end_rid, category)
    mode = currency_mode_from_store(store)
    category_mixed = bool(store.setdefault("settings", {}).get("category_usd_enabled", False) and _v85_enabled("usd_categories"))
    show_rate = mode != "ars" or category_mixed
    rate_info = usd_rate_cached() if show_rate else None
    total = sum(amount for _, amount, _ in items)
    clean_category = _clean_category_display_name(category).upper()
    lines = [
        f"📦 {clean_category}",
        f"▶️ {exact_boundary_text(store, start_key, start_rid, True)}",
        f"⏹ {exact_boundary_text(store, end_key, end_rid, False)}",
        "",
        f"Итого: {format_category_amount(store, total, category_mixed)}",
    ]
    if show_rate and rate_info:
        lines.append(f"Курс: 1 USD = {fmt_num(rate_info['rate']).lstrip('+')} ARS ({_clean_category_display_name(rate_info.get('source') or 'DolarAPI')})")
    lines.append("")
    if not items:
        lines.append("Нет операций по этой статье.")
    else:
        for day_key, amount, note in items:
            clean_note = _clean_category_display_name(str(note or "").strip())
            lines.append(f"• {fmt_date_ddmmyy(day_key)}: {format_category_amount(store, amount, category_mixed)} {clean_note}".rstrip())
    return wm_common("\n".join(lines), 8)

_category_other_sort_state = {}


def _other_sort_key(chat_id: int, start_key: str, start_rid: int, end_key: str, end_rid: int):
    return (int(chat_id), str(start_key), int(start_rid), str(end_key), int(end_rid))


def other_sort_records(store: dict, start_key: str, start_rid: int, end_key: str, end_rid: int) -> list[dict]:
    out = []
    for _day, rec in exact_record_range(store, start_key, start_rid, end_key, end_rid):
        try:
            if float(rec.get("amount", 0) or 0) >= 0:
                continue
        except Exception:
            continue
        category = resolve_expense_category_for_record(rec, store)
        if get_expense_category_slug(category, store) == "other":
            out.append(rec)
    return out


def build_other_sort_text(store: dict, start_key: str, start_rid: int, end_key: str, end_rid: int) -> str:
    count = len(other_sort_records(store, start_key, start_rid, end_key, end_rid))
    return wm_common(
        "🔀 Сортировка статьи ПРОЧЕЕ\n\n"
        "Выберите финансовые значения, которые нужно перенести в другую статью. "
        "После выбора нажмите «Выбрать их».\n\n"
        f"Доступно записей: {count}", 8
    )


def build_other_sort_keyboard(chat_id: int, store: dict, start_key: str, start_rid: int, end_key: str, end_rid: int):
    kb = types.InlineKeyboardMarkup(row_width=1)
    key = _other_sort_key(chat_id, start_key, start_rid, end_key, end_rid)
    selected = _category_other_sort_state.setdefault(key, set())
    valid_ids = set()
    for rec in other_sort_records(store, start_key, start_rid, end_key, end_rid):
        rid = _record_int_id(rec)
        valid_ids.add(rid)
        mark = "✅" if rid in selected else "▫️"
        label = f"{mark} {financial_record_button_label(rec, chat_id)}"
        kb.row(IB(label, callback_data=cat_callback(f"cat_other_sort_toggle:{rid}:{start_key}:{int(start_rid)}:{end_key}:{int(end_rid)}")))
    selected.intersection_update(valid_ids)
    if selected:
        kb.row(IB(f"✅ Выбрать их ({len(selected)})", callback_data=cat_callback(f"cat_other_sort_choose:{start_key}:{int(start_rid)}:{end_key}:{int(end_rid)}")))
    else:
        kb.row(IB("Выберите значения выше", callback_data="none"))
    kb.row(IB("⬅️ Назад", callback_data=cat_callback(f"cat_show_records:{start_key}:{int(start_rid)}:{end_key}:{int(end_rid)}:other")))
    kb.row(IB("⬅️ Назад осн. окно", callback_data=f"d:{today_key()}:back_main"), IB("❌ Закрыть", callback_data=cat_callback("cat_close")))
    return kb


def build_other_sort_target_text(chat_id: int, start_key: str, start_rid: int, end_key: str, end_rid: int) -> str:
    key = _other_sort_key(chat_id, start_key, start_rid, end_key, end_rid)
    selected = _category_other_sort_state.get(key, set())
    return wm_common(f"📦 Куда перенести выбранные записи?\n\nВыбрано: {len(selected)}", 8)


def build_other_sort_target_keyboard(chat_id: int, store: dict, start_key: str, start_rid: int, end_key: str, end_rid: int):
    kb = types.InlineKeyboardMarkup(row_width=2)
    buttons = []
    for slug in get_expense_category_order_slugs(store):
        if slug == "other":
            continue
        name = _clean_category_display_name(get_category_by_slug(slug, store) or slug)
        buttons.append(IB(name, callback_data=cat_callback(f"cat_other_sort_target:{slug}:{start_key}:{int(start_rid)}:{end_key}:{int(end_rid)}")))
    add_buttons_in_rows(kb, buttons, 2)
    kb.row(IB("⬅️ Назад к выбору", callback_data=cat_callback(f"cat_other_sort:{start_key}:{int(start_rid)}:{end_key}:{int(end_rid)}")))
    kb.row(IB("⬅️ Назад осн. окно", callback_data=f"d:{today_key()}:back_main"), IB("❌ Закрыть", callback_data=cat_callback("cat_close")))
    return kb


def apply_other_sort_target(store: dict, selected_ids: set[int], target_slug: str) -> int:
    changed = 0
    ids = {int(x) for x in selected_ids}
    for rec in store.get("records", []) or []:
        if _record_int_id(rec) in ids:
            rec["category_override_slug"] = str(target_slug)
            changed += 1
    # daily_records обычно содержит те же dict, но обновляем и отдельные копии после restore.
    for arr in (store.get("daily_records", {}) or {}).values():
        for rec in arr or []:
            if _record_int_id(rec) in ids:
                rec["category_override_slug"] = str(target_slug)
    return changed


def build_category_record_detail_keyboard(start_key: str, start_rid: int, end_key: str, end_rid: int, category: str | None = None, store: dict | None = None):
    kb = types.InlineKeyboardMarkup()
    if category and get_expense_category_slug(category, store) == "other":
        kb.row(IB("🔀 Сортировка", callback_data=cat_callback(f"cat_other_sort:{start_key}:{int(start_rid)}:{end_key}:{int(end_rid)}")))
    kb.row(IB("⬅️ Назад", callback_data=cat_callback(f"cat_back_records:{start_key}:{int(start_rid)}:{end_key}:{int(end_rid)}")))
    kb.row(
        IB("⬅️ Назад осн. окно", callback_data=f"d:{today_key()}:back_main"),
        IB("❌ Закрыть", callback_data=cat_callback("cat_close")),
    )
    return kb


def _category_picker_day_buttons(year: int, month: int, stage: str, start_day: int | None = None, selected_day: int | None = None):
    kb = types.InlineKeyboardMarkup(row_width=7)
    last_day = calendar.monthrange(int(year), int(month))[1]
    buttons = []
    for dnum in range(1, last_day + 1):
        label = f"✅{dnum}" if selected_day == dnum else str(dnum)
        if stage == "start":
            cb = cat_callback(f"cat_pick_set_start:{year}:{month}:{dnum}")
        else:
            cb = cat_callback(f"cat_pick_set_end:{year}:{month}:{int(start_day or 1)}:{dnum}")
        buttons.append(IB(label, callback_data=cb))
    for i in range(0, len(buttons), 7):
        kb.row(*buttons[i:i + 7])
    return kb


def _send_category_pick_start(chat_id: int, message_id: int, year: int, month: int, selected: int | None = None):
    kb = _category_picker_day_buttons(year, month, "start", selected_day=selected)
    if selected:
        kb.row(IB("✅ Выбрать это", callback_data=cat_callback(f"cat_pick_end:{year}:{month}:{selected}")))
    kb.row(IB("🔙 Назад", callback_data=cat_callback(f"cat_m:{year}:{month}")))
    text = f"📅 Выберите начальную дату: {month:02d}.{year}"
    if selected:
        text += f"\n✅ Начало: {selected:02d}.{month:02d}.{year}"
    send_or_edit_categories_window(
        chat_id,
        wm_common(text, 13),
        reply_markup=kb,
        preferred_message_id=message_id,
        marker_action="cat_pick_start:*",
    )


def _shift_month(year: int, month: int, delta: int = 0) -> tuple[int, int]:
    base = datetime(int(year), int(month), 1)
    m0 = (base.year * 12 + base.month - 1) + int(delta or 0)
    y = m0 // 12
    m = m0 % 12 + 1
    return y, m


def _date_key_from_ymd(year: int, month: int, day: int) -> str:
    last_day = calendar.monthrange(int(year), int(month))[1]
    d = max(1, min(int(day), last_day))
    return f"{int(year):04d}-{int(month):02d}-{d:02d}"


def _category_picker_day_buttons_end_any_month(start_key: str, view_year: int, view_month: int, selected_key: str | None = None):
    kb = types.InlineKeyboardMarkup(row_width=7)
    last_day = calendar.monthrange(int(view_year), int(view_month))[1]
    buttons = []
    for dnum in range(1, last_day + 1):
        dk = _date_key_from_ymd(view_year, view_month, dnum)
        label = f"✅{dnum}" if selected_key == dk else str(dnum)
        buttons.append(IB(label, callback_data=cat_callback(f"cat_pick_set_end2:{start_key}:{int(view_year)}:{int(view_month)}:{dnum}")))
    for i in range(0, len(buttons), 7):
        kb.row(*buttons[i:i + 7])
    return kb


def _send_category_pick_end_any_month(chat_id: int, message_id: int, start_key: str, view_year: int, view_month: int, selected_end_key: str | None = None):
    start_dt = datetime.strptime(str(start_key)[:10], "%Y-%m-%d")
    kb = _category_picker_day_buttons_end_any_month(start_key, view_year, view_month, selected_key=selected_end_key)
    prev_y, prev_m = _shift_month(view_year, view_month, -1)
    next_y, next_m = _shift_month(view_year, view_month, 1)
    kb.row(
        IB("⬅️ Месяц", callback_data=cat_callback(f"cat_pick_end2:{start_key}:{prev_y}:{prev_m}")),
        IB(f"{int(view_month):02d}.{int(view_year)}", callback_data="none"),
        IB("Месяц ➡️", callback_data=cat_callback(f"cat_pick_end2:{start_key}:{next_y}:{next_m}")),
    )
    if selected_end_key:
        kb.row(IB("✅ Выбрать конечное", callback_data=cat_callback(f"cat_range_custom2:{start_key}:{selected_end_key}")))
    kb.row(IB("🔙 Назад к началу", callback_data=cat_callback(f"cat_pick_set_start:{start_dt.year}:{start_dt.month}:{start_dt.day}")))
    text = f"📅 Начало: {fmt_date_ddmmyy(start_key)}\nВыберите конечную дату: {int(view_month):02d}.{int(view_year)}"
    if selected_end_key:
        text += f"\n✅ Конец: {fmt_date_ddmmyy(selected_end_key)}"
    send_or_edit_categories_window(chat_id, wm_common(text, 13), reply_markup=kb, preferred_message_id=message_id)


def _send_category_pick_end(chat_id: int, message_id: int, year: int, month: int, start_day: int, selected_end: int | None = None):
    start_key = _date_key_from_ymd(year, month, start_day)
    selected_end_key = _date_key_from_ymd(year, month, selected_end) if selected_end else None
    _send_category_pick_end_any_month(chat_id, message_id, start_key, int(year), int(month), selected_end_key)


def handle_categories_callback(call, data_str: str) -> bool:
    """UI окна расходов по статьям."""
    chat_id = call.message.chat.id
    store = get_chat_store(chat_id)

    if data_str == "cat_prompt_back":
        was_edit = bool(store.get("category_edit_wait"))
        clear_category_wait_state(chat_id, "category_add_wait", call.message.message_id, delete_prompt=False)
        clear_category_wait_state(chat_id, "category_edit_wait", call.message.message_id, delete_prompt=False)
        if was_edit:
            send_or_edit_categories_window(
                chat_id,
                wm_common("✏️ Изменить статью\n\nВыберите статью. Б = базовая, С = своя.", 14),
                reply_markup=build_category_edit_keyboard(chat_id),
                preferred_message_id=call.message.message_id,
            )
        else:
            return handle_categories_callback(call, "cat_today")
        return True

    if data_str == "cat_add_cancel":
        clear_category_wait_state(chat_id, "category_add_wait", call.message.message_id, delete_prompt=True)
        clear_category_wait_state(chat_id, "category_edit_wait", call.message.message_id, delete_prompt=True)
        try:
            bot.answer_callback_query(call.id, "Команда отменена")
        except Exception:
            pass
        return True

    if data_str.startswith("cat_main_edit:"):
        try:
            parts = data_str.split(":", 2)
            slug = parts[1]
        except Exception:
            return True
        start_category_edit_wait(chat_id, chat_id, slug)
        try:
            bot.answer_callback_query(call.id, "Редактирование статьи", show_alert=False)
        except Exception:
            pass
        return True

    if data_str == "cat_edit_menu":
        send_or_edit_categories_window(
            chat_id,
            wm_common("✏️ Изменить статью\n\nВыберите статью. Б = базовая, С = своя. Можно менять название и ключевые слова.", 14),
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
        if mid:
            unregister_open_window(chat_id, int(mid))
        store["categories_msg_id"] = None
        store["categories_refresh_state"] = None
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

    if data_str == "cat_months" or data_str.startswith("cat_months_y:"):
        try:
            year = int(data_str.split(":", 1)[1]) if data_str.startswith("cat_months_y:") else now_local().year
        except Exception:
            year = now_local().year
        kb = types.InlineKeyboardMarkup(row_width=2)
        month_buttons = []
        current_ym = now_local().strftime("%Y-%m")
        for m in range(1, 13):
            ym = f"{year:04d}-{m:02d}"
            label = f"📍 {russian_month_name(m)} ({m}) — текущий" if ym == current_ym else f"{russian_month_name(m)} ({m})"
            month_buttons.append(IB(label, callback_data=cat_callback(f"cat_m:{year}:{m}")))
        for i in range(0, len(month_buttons), 2):
            kb.row(*month_buttons[i:i + 2])
        kb.row(
            IB("⬅️ Год", callback_data=cat_callback(f"cat_months_y:{year - 1}")),
            IB(str(year), callback_data="none"),
            IB("Год ➡️", callback_data=cat_callback(f"cat_months_y:{year + 1}")),
        )
        kb.row(
            IB("📅 Сегодня", callback_data=cat_callback("cat_today")),
            IB("⬅️ Назад осн. окно", callback_data=f"d:{today_key()}:back_main"),
            IB("❌ Закрыть статьи", callback_data=cat_callback("cat_close"))
        )
        send_or_edit_categories_window(
            chat_id,
            wm_common(f"📦 Выберите месяц, год {year}:", 12),
            reply_markup=kb,
            marker_action="markup:plain",
        )
        return True

    if data_str.startswith("cat_m:"):
        try:
            parts = data_str.split(":")
            if len(parts) >= 3:
                year, month = int(parts[1]), int(parts[2])
            else:
                year, month = now_local().year, int(parts[1])
        except Exception:
            return True
        last_day = calendar.monthrange(year, month)[1]
        kb = types.InlineKeyboardMarkup(row_width=7)
        weeks = [(1, 7), (8, 14), (15, 21), (22, last_day)]
        kb.row(*[IB(f"{a:02d}–{b:02d}", callback_data=cat_callback(f"cat_rng:{year}:{month}:{a}:{b}")) for a, b in weeks])
        kb.row(IB("📅 Произвольный период", callback_data=cat_callback(f"cat_pick_start:{year}:{month}")))
        row = []
        if month != now_local().month or year != now_local().year:
            row.append(IB("📅 Сегодня", callback_data=cat_callback("cat_today")))
        row.append(IB("🔙 Назад", callback_data=cat_callback("cat_months")))
        kb.row(*row)
        send_or_edit_categories_window(
            chat_id,
            wm_common(f"📆 Выберите неделю: {russian_month_name(month)} ({month}) {year}", 13),
            reply_markup=kb,
            marker_action="cat_m:*",
        )
        return True

    if data_str.startswith("cat_pick_start:"):
        try:
            _, y, m = data_str.split(":")
            _send_category_pick_start(chat_id, call.message.message_id, int(y), int(m))
        except Exception as e:
            log_error(f"cat_pick_start: {e}")
        return True

    if data_str.startswith("cat_pick_set_start:"):
        try:
            _, y, m, d = data_str.split(":")
            start_key = _date_key_from_ymd(int(y), int(m), int(d))
            _send_category_pick_start_record(chat_id, call.message.message_id, start_key)
        except Exception as e:
            log_error(f"cat_pick_set_start: {e}")
        return True

    if data_str.startswith("cat_pick_start_record:"):
        try:
            _, start_key, start_rid = data_str.split(":")
            start_dt = datetime.strptime(start_key, "%Y-%m-%d")
            _send_category_pick_end_precise(
                chat_id,
                call.message.message_id,
                start_key,
                int(start_rid),
                start_dt.year,
                start_dt.month,
            )
        except Exception as e:
            log_error(f"cat_pick_start_record: {e}")
        return True

    if data_str.startswith("cat_pick_today_end:"):
        try:
            _, start_key, start_rid = data_str.split(":")
            end_key = today_key()
            if end_key < start_key:
                end_key = start_key
            _send_category_pick_end_record(chat_id, call.message.message_id, start_key, int(start_rid), end_key)
        except Exception as e:
            log_error(f"cat_pick_today_end: {e}")
        return True

    if data_str == "cat_pick_today_start":
        try:
            start_key = today_key()
            now_dt = now_local()
            _send_category_pick_end_precise(chat_id, call.message.message_id, start_key, 0, now_dt.year, now_dt.month)
        except Exception as e:
            log_error(f"cat_pick_today_start: {e}")
        return True

    if data_str.startswith("cat_usd_toggle_period:"):
        try:
            _, mode, start, end = data_str.split(":", 3)
            settings = store.setdefault("settings", {})
            settings["category_usd_enabled"] = not bool(settings.get("category_usd_enabled", False))
            save_data(data, chat_ids=[chat_id])
            schedule_config_backup_for_chats(chat_id)
            if settings["category_usd_enabled"]:
                usd_rate_cached(force=False)
            label = f"{fmt_date_ddmmyy(start)} — {fmt_date_ddmmyy(end)}"
            text, _ = summarize_categories(store, start, end, label)
            kb = build_categories_summary_keyboard(mode, start, end, store=store)
            marker = "cat_wthu:*" if mode == "wthu" else ("cat_wk:*" if mode == "wk" else "cat_range_custom2:*")
            send_or_edit_categories_window(chat_id, text, reply_markup=kb, preferred_message_id=call.message.message_id, marker_action=marker)
        except Exception as e:
            log_error(f"cat_usd_toggle_period: {e}")
        return True

    if data_str.startswith("cat_order_open_sum:"):
        try:
            _, mode, start, end = data_str.split(":", 3)
            send_or_edit_categories_window(
                chat_id, build_category_layout_text(store, "sum"),
                reply_markup=build_category_layout_keyboard(store, "sum", (mode, start, end), chat_id=chat_id),
                preferred_message_id=call.message.message_id, marker_action="cat_order_open_sum:*",
            )
        except Exception as e:
            log_error(f"cat_order_open_sum: {e}")
        return True

    if data_str.startswith("cat_order_select_sum:"):
        try:
            _, slug, mode, start, end = data_str.split(":", 4)
            params = ("sum", mode, start, end)
            key = _category_order_selection_key(chat_id, params)
            _category_order_selection[key] = slug
            send_or_edit_categories_window(
                chat_id, build_category_layout_text(store, "sum"),
                reply_markup=build_category_layout_keyboard(store, "sum", (mode, start, end), chat_id=chat_id),
                preferred_message_id=call.message.message_id, marker_action="cat_order_open_sum:*",
            )
        except Exception as e:
            log_error(f"cat_order_select_sum: {e}")
        return True

    if data_str.startswith("cat_order_position_sum:"):
        try:
            _, position, mode, start, end = data_str.split(":", 4)
            params = ("sum", mode, start, end)
            key = _category_order_selection_key(chat_id, params)
            slug = _category_order_selection.get(key)
            if not slug:
                try:
                    bot.answer_callback_query(call.id, "Сначала выберите статью")
                except Exception:
                    pass
                return True
            moved = move_expense_category_to_position(store, slug, int(position))
            _category_order_selection.pop(key, None)
            if moved:
                save_data(data, chat_ids=[chat_id])
                schedule_quick_backup(
                    chat_id,
                    MEGA_DELTA_PRIORITY_DELAY_SECONDS if mega_backup_priority_enabled() else MEGA_DELTA_DELAY_SECONDS,
                )
                schedule_config_backup_for_chats(chat_id, delay=0.4)
                finance_changed(chat_id, store.get("current_view_day") or today_key(), reason="category_order_position_f36", delay=0.03)
            send_or_edit_categories_window(
                chat_id, build_category_layout_text(store, "sum"),
                reply_markup=build_category_layout_keyboard(store, "sum", (mode, start, end), chat_id=chat_id),
                preferred_message_id=call.message.message_id, marker_action="cat_order_open_sum:*",
            )
        except Exception as e:
            log_error(f"cat_order_position_sum: {e}")
        return True

    if data_str.startswith("cat_order_move_sum:"):
        try:
            _, slug, direction, mode, start, end = data_str.split(":", 5)
            if move_expense_category_order(store, slug, direction):
                save_data(data, chat_ids=[chat_id])
                schedule_config_backup_for_chats(chat_id)
            send_or_edit_categories_window(
                chat_id, build_category_layout_text(store, "sum"),
                reply_markup=build_category_layout_keyboard(store, "sum", (mode, start, end), chat_id=chat_id),
                preferred_message_id=call.message.message_id, marker_action="cat_order_move_sum:*",
            )
        except Exception as e:
            log_error(f"cat_order_move_sum: {e}")
        return True

    if data_str.startswith("cat_order_open_exact:"):
        try:
            _, start_key, start_rid, end_key, end_rid = data_str.split(":")
            send_or_edit_categories_window(
                chat_id, build_category_layout_text(store, "exact"),
                reply_markup=build_category_layout_keyboard(store, "exact", (start_key, int(start_rid), end_key, int(end_rid)), chat_id=chat_id),
                preferred_message_id=call.message.message_id, marker_action="cat_order_open_exact:*",
            )
        except Exception as e:
            log_error(f"cat_order_open_exact: {e}")
        return True

    if data_str.startswith("cat_order_select_exact:"):
        try:
            _, slug, start_key, start_rid, end_key, end_rid = data_str.split(":", 5)
            params = (start_key, int(start_rid), end_key, int(end_rid))
            key = _category_order_selection_key(chat_id, params)
            _category_order_selection[key] = slug
            send_or_edit_categories_window(
                chat_id, build_category_layout_text(store, "exact"),
                reply_markup=build_category_layout_keyboard(store, "exact", params, chat_id=chat_id),
                preferred_message_id=call.message.message_id, marker_action="cat_order_open_exact:*",
            )
        except Exception as e:
            log_error(f"cat_order_select_exact: {e}")
        return True

    if data_str.startswith("cat_order_position_exact:"):
        try:
            _, position, start_key, start_rid, end_key, end_rid = data_str.split(":", 5)
            params = (start_key, int(start_rid), end_key, int(end_rid))
            key = _category_order_selection_key(chat_id, params)
            slug = _category_order_selection.get(key)
            if not slug:
                try:
                    bot.answer_callback_query(call.id, "Сначала выберите статью")
                except Exception:
                    pass
                return True
            moved = move_expense_category_to_position(store, slug, int(position))
            _category_order_selection.pop(key, None)
            if moved:
                save_data(data, chat_ids=[chat_id])
                schedule_quick_backup(
                    chat_id,
                    MEGA_DELTA_PRIORITY_DELAY_SECONDS if mega_backup_priority_enabled() else MEGA_DELTA_DELAY_SECONDS,
                )
                schedule_config_backup_for_chats(chat_id, delay=0.4)
                finance_changed(chat_id, store.get("current_view_day") or today_key(), reason="category_order_position", delay=0.03)
            send_or_edit_categories_window(
                chat_id, build_category_layout_text(store, "exact"),
                reply_markup=build_category_layout_keyboard(store, "exact", params, chat_id=chat_id),
                preferred_message_id=call.message.message_id, marker_action="cat_order_open_exact:*",
            )
        except Exception as e:
            log_error(f"cat_order_position_exact: {e}")
        return True

    if data_str.startswith("cat_order_move_exact:"):
        try:
            _, slug, direction, start_key, start_rid, end_key, end_rid = data_str.split(":", 6)
            if move_expense_category_order(store, slug, direction):
                save_data(data, chat_ids=[chat_id])
                schedule_config_backup_for_chats(chat_id)
            send_or_edit_categories_window(
                chat_id, build_category_layout_text(store, "exact"),
                reply_markup=build_category_layout_keyboard(store, "exact", (start_key, int(start_rid), end_key, int(end_rid)), chat_id=chat_id),
                preferred_message_id=call.message.message_id, marker_action="cat_order_move_exact:*",
            )
        except Exception as e:
            log_error(f"cat_order_move_exact: {e}")
        return True

    if data_str.startswith("cat_pick_end3:"):
        try:
            _, start_key, start_rid, y, m = data_str.split(":")
            _send_category_pick_end_precise(chat_id, call.message.message_id, start_key, int(start_rid), int(y), int(m))
        except Exception as e:
            log_error(f"cat_pick_end3: {e}")
        return True

    if data_str.startswith("cat_pick_set_end3:"):
        try:
            _, start_key, start_rid, y, m, d = data_str.split(":")
            end_key = _date_key_from_ymd(int(y), int(m), int(d))
            _send_category_pick_end_record(chat_id, call.message.message_id, start_key, int(start_rid), end_key)
        except Exception as e:
            log_error(f"cat_pick_set_end3: {e}")
        return True

    if data_str.startswith("cat_pick_end_record:"):
        try:
            _, start_key, start_rid, end_key, end_rid = data_str.split(":")
            text, _ = summarize_categories_record_range(store, start_key, int(start_rid), end_key, int(end_rid))
            kb = build_categories_record_summary_keyboard(start_key, int(start_rid), end_key, int(end_rid), store)
            send_or_edit_categories_window(
                chat_id,
                text,
                reply_markup=kb,
                preferred_message_id=call.message.message_id,
                marker_action="cat_range_records:*",
            )
        except Exception as e:
            log_error(f"cat_pick_end_record: {e}")
        return True

    if data_str.startswith("cat_usd_toggle_records:"):
        try:
            _, start_key, start_rid, end_key, end_rid = data_str.split(":")
            settings = store.setdefault("settings", {})
            settings["category_usd_enabled"] = not bool(settings.get("category_usd_enabled", False))
            save_data(data, chat_ids=[chat_id])
            if settings["category_usd_enabled"]:
                usd_rate_cached(force=False)
            text, _ = summarize_categories_record_range(store, start_key, int(start_rid), end_key, int(end_rid))
            kb = build_categories_record_summary_keyboard(start_key, int(start_rid), end_key, int(end_rid), store)
            send_or_edit_categories_window(chat_id, text, reply_markup=kb, preferred_message_id=call.message.message_id, marker_action="cat_range_records:*")
        except Exception as e:
            log_error(f"cat_usd_toggle_records: {e}")
        return True

    if data_str.startswith("cat_range_records:"):
        try:
            _, start_key, start_rid, end_key, end_rid = data_str.split(":")
            text, _ = summarize_categories_record_range(store, start_key, int(start_rid), end_key, int(end_rid))
            kb = build_categories_record_summary_keyboard(start_key, int(start_rid), end_key, int(end_rid), store)
            send_or_edit_categories_window(
                chat_id,
                text,
                reply_markup=kb,
                preferred_message_id=call.message.message_id,
                marker_action="cat_range_records:*",
            )
        except Exception as e:
            log_error(f"cat_range_records: {e}")
        return True

    if data_str.startswith("cat_back_records:"):
        try:
            _, start_key, start_rid, end_key, end_rid = data_str.split(":")
            text, _ = summarize_categories_record_range(store, start_key, int(start_rid), end_key, int(end_rid))
            kb = build_categories_record_summary_keyboard(start_key, int(start_rid), end_key, int(end_rid), store)
            send_or_edit_categories_window(
                chat_id,
                text,
                reply_markup=kb,
                preferred_message_id=call.message.message_id,
                marker_action="cat_range_records:*",
            )
        except Exception as e:
            log_error(f"cat_back_records: {e}")
        return True

    if data_str.startswith("cat_other_sort:"):
        try:
            _, start_key, start_rid, end_key, end_rid = data_str.split(":")
            send_or_edit_categories_window(
                chat_id, build_other_sort_text(store, start_key, int(start_rid), end_key, int(end_rid)),
                reply_markup=build_other_sort_keyboard(chat_id, store, start_key, int(start_rid), end_key, int(end_rid)),
                preferred_message_id=call.message.message_id, marker_action="cat_other_sort:*",
            )
        except Exception as e:
            log_error(f"cat_other_sort: {e}")
        return True

    if data_str.startswith("cat_other_sort_toggle:"):
        try:
            _, rid, start_key, start_rid, end_key, end_rid = data_str.split(":")
            key = _other_sort_key(chat_id, start_key, int(start_rid), end_key, int(end_rid))
            selected = _category_other_sort_state.setdefault(key, set())
            rid_i = int(rid)
            if rid_i in selected:
                selected.remove(rid_i)
            else:
                selected.add(rid_i)
            send_or_edit_categories_window(
                chat_id, build_other_sort_text(store, start_key, int(start_rid), end_key, int(end_rid)),
                reply_markup=build_other_sort_keyboard(chat_id, store, start_key, int(start_rid), end_key, int(end_rid)),
                preferred_message_id=call.message.message_id, marker_action="cat_other_sort_toggle:*",
            )
        except Exception as e:
            log_error(f"cat_other_sort_toggle: {e}")
        return True

    if data_str.startswith("cat_other_sort_choose:"):
        try:
            _, start_key, start_rid, end_key, end_rid = data_str.split(":")
            key = _other_sort_key(chat_id, start_key, int(start_rid), end_key, int(end_rid))
            if not _category_other_sort_state.get(key):
                bot.answer_callback_query(call.id, "Сначала выберите записи", show_alert=False)
                return True
            send_or_edit_categories_window(
                chat_id, build_other_sort_target_text(chat_id, start_key, int(start_rid), end_key, int(end_rid)),
                reply_markup=build_other_sort_target_keyboard(chat_id, store, start_key, int(start_rid), end_key, int(end_rid)),
                preferred_message_id=call.message.message_id, marker_action="cat_other_sort_choose:*",
            )
        except Exception as e:
            log_error(f"cat_other_sort_choose: {e}")
        return True

    if data_str.startswith("cat_other_sort_target:"):
        try:
            _, target_slug, start_key, start_rid, end_key, end_rid = data_str.split(":", 5)
            key = _other_sort_key(chat_id, start_key, int(start_rid), end_key, int(end_rid))
            selected = set(_category_other_sort_state.get(key, set()))
            changed = apply_other_sort_target(store, selected, target_slug)
            _category_other_sort_state.pop(key, None)
            if changed:
                save_data(data, chat_ids=[chat_id])
                finance_changed(chat_id, store.get("current_view_day") or today_key(), reason="category_manual_sort", delay=0.05)
                schedule_config_backup_for_chats(chat_id)
            text, _ = summarize_categories_record_range(store, start_key, int(start_rid), end_key, int(end_rid))
            kb = build_categories_record_summary_keyboard(start_key, int(start_rid), end_key, int(end_rid), store)
            send_or_edit_categories_window(
                chat_id, text, reply_markup=kb, preferred_message_id=call.message.message_id, marker_action="cat_range_records:*",
            )
            try:
                bot.answer_callback_query(call.id, f"Перенесено записей: {changed}", show_alert=False)
            except Exception:
                pass
        except Exception as e:
            log_error(f"cat_other_sort_target: {e}")
        return True

    if data_str.startswith("cat_show_records:"):
        try:
            _, start_key, start_rid, end_key, end_rid, slug = data_str.split(":", 5)
            category = get_category_by_slug(slug, store)
            if not category:
                try:
                    bot.answer_callback_query(call.id, "Статья не найдена", show_alert=False)
                except Exception:
                    pass
                return True
            text = build_category_record_detail_text(store, start_key, int(start_rid), end_key, int(end_rid), category)
            kb = build_category_record_detail_keyboard(start_key, int(start_rid), end_key, int(end_rid), category=category, store=store)
            send_or_edit_categories_window(
                chat_id,
                text,
                reply_markup=kb,
                preferred_message_id=call.message.message_id,
                marker_action="cat_show_records:*",
            )
        except Exception as e:
            log_error(f"cat_show_records: {e}")
        return True

    if data_str.startswith("cat_pick_end:"):
        try:
            _, y, m, start_d = data_str.split(":")
            _send_category_pick_end(chat_id, call.message.message_id, int(y), int(m), int(start_d))
        except Exception as e:
            log_error(f"cat_pick_end: {e}")
        return True

    if data_str.startswith("cat_pick_set_end:"):
        try:
            _, y, m, start_d, end_d = data_str.split(":")
            _send_category_pick_end(chat_id, call.message.message_id, int(y), int(m), int(start_d), int(end_d))
        except Exception as e:
            log_error(f"cat_pick_set_end: {e}")
        return True

    if data_str.startswith("cat_pick_end2:"):
        try:
            _, start_key, y, m = data_str.split(":")
            _send_category_pick_end_any_month(chat_id, call.message.message_id, start_key, int(y), int(m))
        except Exception as e:
            log_error(f"cat_pick_end2: {e}")
        return True

    if data_str.startswith("cat_pick_set_end2:"):
        try:
            _, start_key, y, m, d = data_str.split(":")
            end_key = _date_key_from_ymd(int(y), int(m), int(d))
            _send_category_pick_end_any_month(chat_id, call.message.message_id, start_key, int(y), int(m), end_key)
        except Exception as e:
            log_error(f"cat_pick_set_end2: {e}")
        return True

    if data_str.startswith("cat_range_custom2:"):
        try:
            _, start, end = data_str.split(":", 2)
            if end < start:
                start, end = end, start
            label = f"{fmt_date_ddmmyy(start)} — {fmt_date_ddmmyy(end)}"
            text, _ = summarize_categories(store, start, end, label)
            kb = build_categories_summary_keyboard("rng", start, end, store=store)
            send_or_edit_categories_window(chat_id, text, reply_markup=kb, preferred_message_id=call.message.message_id)
        except Exception as e:
            log_error(f"cat_range_custom2: {e}")
        return True

    if data_str.startswith("cat_range_custom:"):
        try:
            _, y, m, a, b = data_str.split(":")
            y, m, a, b = map(int, (y, m, a, b))
            last_day = calendar.monthrange(y, m)[1]
            a = max(1, min(a, last_day))
            b = max(1, min(b, last_day))
            if b < a:
                a, b = b, a
            start = f"{y}-{m:02d}-{a:02d}"
            end = f"{y}-{m:02d}-{b:02d}"
            label = f"{fmt_date_ddmmyy(start)} — {fmt_date_ddmmyy(end)}"
            text, _ = summarize_categories(store, start, end, label)
            kb = build_categories_summary_keyboard("rng", start, end, store=store)
            send_or_edit_categories_window(chat_id, text, reply_markup=kb, preferred_message_id=call.message.message_id)
        except Exception as e:
            log_error(f"cat_range_custom: {e}")
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
            if start_dt.year != end_dt.year or start_dt.month != end_dt.month:
                back_callback = f"cat_range_custom2:{start}:{end}"
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


def _callback_should_debounce(call, data_str: str, min_interval: float = 0.12) -> bool:
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
        elif data_str.startswith(("secday:", "secview:", "secchatcal:", "secmon:", "secmonthlist:")):
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


# Debounce перерисовки галочек в секретном редактировании:
# быстрые клики собираются, окно обновляется один раз после последнего клика.
_secret_edit_refresh_lock = threading.RLock()
_secret_edit_refresh_timers = {}


def schedule_secret_edit_refresh_window(viewer_chat_id: int, message_id: int, target_chat_id: int, day_key: str, self_only: bool = False, delay: float = 0.7):
    key = (int(viewer_chat_id), int(message_id))
    generation = time.time_ns()
    scheduler_key = f"secret-edit-refresh:{key[0]}:{key[1]}"

    def _job():
        try:
            with _secret_edit_refresh_lock:
                if _secret_edit_refresh_timers.get(key) != generation:
                    return
            text = build_secret_edit_text(int(target_chat_id), day_key)
            kb = build_secret_edit_keyboard(int(viewer_chat_id), int(target_chat_id), day_key, self_only=bool(self_only))
            try:
                fast_ui_edit_message_text(int(viewer_chat_id), int(message_id), text, reply_markup=kb, purpose="secret_edit_debounce")
            except Exception as e:
                if not is_telegram_429(e) and "message is not modified" not in str(e).lower():
                    log_error(f"secret edit debounce refresh {viewer_chat_id}:{message_id}: {e}")
            register_secret_window(int(viewer_chat_id), int(message_id), int(target_chat_id), "edit", day_key=day_key, self_only=bool(self_only))
            schedule_secret_calendar_close(int(viewer_chat_id), int(message_id))
        finally:
            with _secret_edit_refresh_lock:
                if _secret_edit_refresh_timers.get(key) == generation:
                    _secret_edit_refresh_timers.pop(key, None)

    with _secret_edit_refresh_lock:
        DELAYED_SCHEDULER.cancel(scheduler_key)
        _secret_edit_refresh_timers[key] = generation
        DELAYED_SCHEDULER.schedule(scheduler_key, float(delay), _job)

def _answer_callback_query_quiet(callback_id: str):
    try:
        bot.answer_callback_query(callback_id)
    except Exception:
        pass


def answer_callback_query_background(callback_id: str):
    """Снимает Telegram «Загрузка…» параллельно, не задерживая обработку самой кнопки."""
    key = f"callback-ack:{callback_id}"
    if not GENERAL_TASK_POOL.submit(key, _answer_callback_query_quiet, callback_id):
        # При переполнении не блокируем кнопку сетевым вызовом; последующая UI-операция всё равно выполнится.
        try:
            bot_journal("callback_ack_queue_full", None, str(callback_id), "WARN")
        except Exception:
            pass


@bot.callback_query_handler(func=lambda c: True)

def on_callback(call):
    # Раньше этот сетевой вызов выполнялся синхронно и давал 0.3–1.0 с задержки
    # до начала обработки кнопки. Теперь подтверждение и действие идут параллельно.
    answer_callback_query_background(call.id)

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
            if not GENERAL_TASK_POOL.submit(f"restore:{chat_id}", run_owner_json_restore_prompt_job, chat_id, item):
                send_and_auto_delete(chat_id, "⛔ Очередь восстановления переполнена.", 15)
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
                if not EXPORT_TASK_POOL.submit(f"secret-media:{chat_id}", send_secret_media, chat_id, target_chat_id, day_key):
                    send_and_auto_delete(chat_id, "⛔ Очередь медиа переполнена.", 12)
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
                try:
                    bot.answer_callback_query(call.id, "✅", show_alert=False)
                except Exception:
                    pass
                schedule_secret_edit_refresh_window(
                    chat_id, call.message.message_id, target_chat_id, day_key,
                    self_only=self_only, delay=0.7,
                )
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

        # 💰Перес работает и в скрытом финрежиме принимающего чата, поэтому до guard.
        if data_str == "fwdcopy_edit":
            start_forward_copy_edit(chat_id, call.message.message_id)
            return
        if data_str == "fwdcopy_edit_cancel":
            clear_forward_copy_edit_wait(chat_id, delete_prompt=True)
            return
        if data_str == "forward_copy_edit_mode_toggle":
            if not version_mode_feature("forward_copy_edit"):
                return
            new_mode = cycle_forward_copy_edit_mode(chat_id)
            try:
                bot.answer_callback_query(call.id, forward_copy_edit_mode_label(chat_id), show_alert=False)
            except Exception:
                pass
            safe_edit(bot, call, build_info_text(chat_id), reply_markup=build_info_keyboard(chat_id))
            if not GENERAL_TASK_POOL.submit(f"fwdcopy-retro:{owner_scope_id(chat_id)}", refresh_existing_forward_copy_ui, chat_id, new_mode):
                refresh_existing_forward_copy_ui(chat_id, new_mode)
            bot_journal("forward_copy_edit_mode", chat_id, f"mode={new_mode}")
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
                bot.answer_callback_query(call.id, f"Остаток: {format_chat_amount(chat_id, get_chat_store(chat_id).get('balance', 0), True)}")
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
            unregister_open_window(chat_id, call.message.message_id)
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
            safe_edit(bot, call, calendar_window_text(center_dt), reply_markup=kb)
            register_open_window(
                chat_id, call.message.message_id, "local_fin_view", code="calendar", day_key=center_dt.strftime("%Y-%m-%d"),
                params={"view_action": "calendar", "center_day": center_dt.strftime("%Y-%m-%d")},
            )
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
                f"📅 Выберите день: {html.escape(get_chat_display_name(target_chat_id))}\n{russian_month_name(center_dt.month)} {center_dt.year}",
                reply_markup=build_fin_calendar_keyboard(target_chat_id, center_dt, owner_day_key),
                parse_mode="HTML"
            )
            register_open_window(
                chat_id, call.message.message_id, "fin_view", code="fv:calendar", day_key=center_dt.strftime("%Y-%m-%d"),
                params={"target_chat_id": target_chat_id, "owner_day_key": owner_day_key, "view_action": "calendar", "center_day": center_dt.strftime("%Y-%m-%d")},
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
        if data_str == "journal_chats_open" or data_str.startswith("journal_chats_open:"):
            if not is_owner_chat(chat_id):
                return
            try:
                page = int(data_str.split(":", 1)[1]) if ":" in data_str else 0
            except Exception:
                page = 0
            safe_edit(bot, call, build_chat_journal_menu_text(page), reply_markup=build_chat_journal_menu_keyboard(page))
            return
        if data_str.startswith("journal_chat_toggle:"):
            if not is_owner_chat(chat_id):
                return
            try:
                parts = data_str.split(":")
                target_chat_id = int(parts[1])
                page = int(parts[2]) if len(parts) > 2 else 0
            except Exception:
                return
            new_state = toggle_chat_journal(target_chat_id)
            bot_journal("journal_chat_toggle", target_chat_id, f"enabled={new_state}")
            # Если переключили текущий чат прямо в ИНФО, остаёмся в ИНФО; иначе обновляем список.
            if int(target_chat_id) == int(chat_id) and not str(getattr(call.message, 'text', '') or '').startswith("📓 Журналы по чатам"):
                safe_edit(bot, call, build_info_text(chat_id), reply_markup=build_info_keyboard(chat_id))
            else:
                safe_edit(bot, call, build_chat_journal_menu_text(page), reply_markup=build_chat_journal_menu_keyboard(page))
            return
        if data_str == "journal_chats_back":
            if not is_owner_chat(chat_id):
                return
            safe_edit(bot, call, build_info_text(chat_id), reply_markup=build_info_keyboard(chat_id))
            return
        if data_str == "currency_menu":
            if version_mode_layout() != "v87":
                return
            fast_ui_edit_message_text(
                chat_id, call.message.message_id, currency_menu_text(chat_id),
                reply_markup=build_currency_menu_keyboard(chat_id),
                purpose="currency_menu",
            )
            return
        if data_str.startswith("currency_select:"):
            if version_mode_layout() != "v87":
                return
            mode = data_str.split(":", 1)[1]
            set_currency_mode(chat_id, mode)
            bot_journal("currency_mode_changed", chat_id, f"mode={mode}")
            fast_ui_edit_message_text(
                chat_id, call.message.message_id, currency_menu_text(chat_id),
                reply_markup=build_currency_menu_keyboard(chat_id),
                purpose="currency_select",
            )
            try:
                day_key = get_chat_store(chat_id).get("current_view_day") or today_key()
                finance_changed(chat_id, day_key, reason="currency_mode_changed", delay=0.03)
            except Exception:
                pass
            return
        if data_str == "currency_back":
            fast_ui_edit_message_text(
                chat_id, call.message.message_id, build_info_text(chat_id),
                reply_markup=build_info_keyboard(chat_id),
                purpose="currency_back",
            )
            return
        if data_str == "usd_display_toggle":
            if not version_mode_feature("daily_usd"):
                return
            new_state = toggle_usd_display(chat_id)
            bot_journal("usd_display_toggle", chat_id, f"enabled={new_state}")
            try:
                bot.answer_callback_query(call.id, "Доллар включён" if new_state else "Доллар выключен", show_alert=False)
            except Exception:
                pass
            safe_edit(bot, call, build_info_text(chat_id), reply_markup=build_info_keyboard(chat_id))
            try:
                day_key = get_chat_store(chat_id).get("current_view_day") or today_key()
                finance_changed(chat_id, day_key, reason="usd_display_toggle", delay=0.05)
            except Exception:
                pass
            return
        if data_str == "gomonk_open":
            if not _v85_enabled("gomonk_wallets"):
                return
            open_gomonk_window(chat_id, call.message.message_id)
            return
        if data_str == "gomonk_toggle":
            if not _v85_enabled("gomonk_wallets"):
                return
            new_state = toggle_gomonk_enabled(chat_id)
            bot_journal("gomonk_toggle", chat_id, f"enabled={new_state}")
            open_gomonk_window(chat_id, call.message.message_id)
            return
        if data_str == "gomonk_back":
            fast_ui_edit_message_text(chat_id, call.message.message_id, build_info_text(chat_id), reply_markup=build_info_keyboard(chat_id), purpose="gomonk_back")
            return
        if data_str.startswith("remaining_open:"):
            if not _v85_enabled("remaining_window"):
                return
            day_key = data_str.split(":", 1)[1] or today_key()
            open_remaining_window(chat_id, day_key, call.message.message_id)
            return
        if data_str.startswith("remaining_toggle:"):
            if not _v85_enabled("remaining_window"):
                return
            day_key = data_str.split(":", 1)[1] or today_key()
            settings = _gomonk_settings(chat_id)
            settings["remaining_with_gomonk"] = not bool(settings.get("remaining_with_gomonk", True))
            save_data(data, chat_ids=[chat_id])
            open_remaining_window(chat_id, day_key, call.message.message_id)
            return
        if data_str == "main_articles_toggle":
            if not version_mode_feature("article_buttons"):
                return
            new_state = toggle_main_article_buttons(chat_id)
            try:
                bot.answer_callback_query(call.id, "Статьи-кнопки включены" if new_state else "Статьи-кнопки выключены", show_alert=False)
            except Exception:
                pass
            safe_edit(bot, call, build_info_text(chat_id), reply_markup=build_info_keyboard(chat_id))
            try:
                finance_changed(chat_id, get_chat_store(chat_id).get("current_view_day") or today_key(), reason="main_articles_toggle", delay=0.03)
            except Exception:
                pass
            return
        if data_str == "main_financial_values_toggle":
            if not version_mode_feature("financial_value_buttons"):
                return
            new_state = toggle_main_financial_value_buttons(chat_id)
            try:
                bot.answer_callback_query(call.id, "Финансовые значения теперь кнопками" if new_state else "Финансовые кнопки выключены", show_alert=False)
            except Exception:
                pass
            safe_edit(bot, call, build_info_text(chat_id), reply_markup=build_info_keyboard(chat_id))
            try:
                finance_changed(chat_id, get_chat_store(chat_id).get("current_view_day") or today_key(), reason="main_financial_values_toggle", delay=0.03)
            except Exception:
                pass
            return
        if data_str == "version_menu":
            if not is_owner_chat(chat_id):
                return
            safe_edit(bot, call, build_version_menu_text(), reply_markup=build_version_menu_keyboard())
            return
        if data_str.startswith("version_select:"):
            if not is_owner_chat(chat_id):
                return
            profile_key = data_str.split(":", 1)[1]
            set_bot_behavior_profile(profile_key)
            safe_edit(bot, call, build_version_menu_text(), reply_markup=build_version_menu_keyboard())
            return
        if data_str == "version_back":
            if not is_owner_chat(chat_id):
                return
            safe_edit(bot, call, build_info_text(chat_id), reply_markup=build_info_keyboard(chat_id))
            return
        if data_str == "keepalive_status":
            if not is_owner_chat(chat_id):
                return
            kb = types.InlineKeyboardMarkup()
            kb.row(IB("🔙 Назад в Инфо", callback_data="version_back"))
            safe_edit(bot, call, keep_alive_status_text(), reply_markup=kb)
            return
        if data_str == "journal_back":
            if not is_owner_chat(chat_id):
                return
            safe_edit(bot, call, build_info_text(chat_id), reply_markup=build_info_keyboard(chat_id))
            return
        if data_str == "forward_menu_style_toggle":
            if not is_owner_chat(chat_id):
                return
            new_state = toggle_forward_menu_new_style(chat_id)
            safe_edit(bot, call, build_info_text(chat_id) + f"\n\nМеню пересылки: {'по-новому' if new_state else 'как обычно'}", reply_markup=build_info_keyboard(chat_id))
            return
        if data_str == "buttons_current_toggle":
            if not is_owner_chat(chat_id):
                return
            new_state = toggle_buttons_current_window(chat_id)
            safe_edit(bot, call, build_info_text(chat_id) + f"\n\nРежим кнопок в текущем окне: {'ВКЛ' if new_state else 'ВЫКЛ'}", reply_markup=build_info_keyboard(chat_id))
            return
        if data_str == "icon_buttons_toggle":
            if not is_owner_chat(chat_id):
                return
            new_state = toggle_icon_button_mode(chat_id)
            safe_edit(bot, call, build_info_text(chat_id) + f"\n\nКнопки: {'значки' if new_state else 'текст'}", reply_markup=build_info_keyboard(chat_id))
            return
        if data_str == "total_secret_mask_toggle":
            if not is_owner_chat(chat_id):
                return
            new_state = toggle_total_secret_mask(chat_id)
            safe_edit(bot, call, build_info_text(chat_id) + f"\n\nМаскировка тотального секрета: {'ВКЛ' if new_state else 'ВЫКЛ'}", reply_markup=build_info_keyboard(chat_id))
            return
        if data_str == "finance_day5_toggle":
            if not is_owner_chat(chat_id):
                return
            new_state = toggle_finance_day_start_5am(chat_id)
            safe_edit(bot, call, build_info_text(chat_id) + f"\n\nФинансовые сутки: с {'05:00' if new_state else '00:00'}", reply_markup=build_info_keyboard(chat_id))
            return
        if data_str == "mega_priority_toggle":
            if not is_owner_chat(chat_id):
                return
            new_state = toggle_mega_backup_priority(chat_id)
            mode_text = "сначала и сразу в MEGA" if new_state else "как обычно"
            bot_journal("mega_priority_toggle", chat_id, f"enabled={new_state}")
            safe_edit(bot, call, build_info_text(chat_id) + f"\n\nБэкап MEGA: {mode_text}", reply_markup=build_info_keyboard(chat_id))
            return
        if data_str == "info_instruction":
            if not is_owner_chat(chat_id):
                try:
                    bot.answer_callback_query(call.id, "Только для владельца", show_alert=True)
                except Exception:
                    pass
                return
            safe_edit(bot, call, build_owner_instruction_text(), reply_markup=build_owner_instruction_keyboard(chat_id))
            return
        if data_str == "info_delta_status":
            if not is_owner_chat(chat_id):
                return
            kbd = types.InlineKeyboardMarkup()
            kbd.row(IB("🔄 Обновить", callback_data="info_delta_status"))
            kbd.row(IB("🔙 Назад в Инфо", callback_data=f"d:{get_chat_store(chat_id).get('current_view_day', today_key())}:info"))
            safe_edit(bot, call, delta_status_text(), reply_markup=kbd)
            return
        if data_str == "info_queues":
            if not is_owner_chat(chat_id):
                try:
                    bot.answer_callback_query(call.id, "Только для владельца", show_alert=True)
                except Exception:
                    pass
                return
            kbq = types.InlineKeyboardMarkup()
            kbq.row(IB("🔄 Обновить", callback_data="info_queues"))
            kbq.row(IB("🔙 Назад в Инфо", callback_data=f"d:{get_chat_store(chat_id).get('current_view_day', today_key())}:info"))
            safe_edit(bot, call, build_queue_status_text(), reply_markup=kbq)
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

            # Фактически открытое окно владельца регистрируем как зависимое от target_chat_id.
            # При следующем изменении данных target-чата оно будет автоматически перерисовано.
            registry_action = action
            if action == "clear_delete_back":
                registry_action = "open"
            elif action.startswith("del_toggle_") or action == "del_selected":
                registry_action = "edit_list"
            if registry_action in {"open", "back_main", "menu", "calendar", "report", "total", "info", "edit_list", "csv_menu"}:
                register_open_window(
                    chat_id, call.message.message_id, "fin_view", code=f"fv:{registry_action}", day_key=view_day,
                    params={"target_chat_id": target_chat_id, "owner_day_key": owner_day_key, "view_action": registry_action},
                )

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
                text = f"👁 {html.escape(get_chat_display_name(target_chat_id))}\n\n💰 Общий итог по чату: {format_chat_amount(target_chat_id, target_store.get('balance', 0), True)}"
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
            if action in {"csv_all", "csv_day", "csv_week", "csv_month", "csv_wedthu", "xlsx_all", "xlsx_day", "xlsx_week", "xlsx_month", "xlsx_wedthu", "xlsxstat_all", "xlsxstat_day", "xlsxstat_week", "xlsxstat_month", "xlsxstat_wedthu"}:
                if action.startswith("xlsxstat_"):
                    file_type = "xlsxstat"
                    mode = action.replace("xlsxstat_", "", 1)
                else:
                    file_type = "xlsx" if action.startswith("xlsx_") else "csv"
                    mode = action.replace("csv_", "").replace("xlsx_", "")
                send_export_for_chat_to(chat_id, target_chat_id, mode, view_day, file_type)
                return
            return

        if data_str.startswith("exp_"):
            # Точный экспорт временно превращает Ф47 в последовательность экранов выбора.
            # Помечаем фактическое состояние, чтобы автообновление финансов не вернуло окно в О1/Ф47 посреди выбора.
            try:
                register_static_open_view(
                    chat_id, call.message.message_id, code=data_str.split(":", 1)[0],
                    day_key=get_chat_store(chat_id).get("current_view_day") or today_key(),
                    params={"source": "exact_export"},
                )
            except Exception:
                pass

        if data_str.startswith("exp_pick_start:"):
            try:
                _, y, m, return_day_key = data_str.split(":")
                y, m = int(y), int(m)
                safe_edit(
                    bot,
                    call,
                    f"🎯 Точный CSV / Excel\nВыберите начальную дату: {russian_month_name(m)} {y}",
                    reply_markup=_export_calendar_start_keyboard(y, m, return_day_key),
                )
            except Exception as e:
                log_error(f"exp_pick_start: {e}")
            return

        if data_str.startswith("exp_pick_set_start:"):
            try:
                _, y, m, d, return_day_key = data_str.split(":")
                start_key = _date_key_from_ymd(int(y), int(m), int(d))
                store = get_chat_store(chat_id)
                safe_edit(
                    bot,
                    call,
                    "🎯 Точное начало экспорта\n"
                    f"📅 День: {fmt_date_ddmmyy(start_key)}\n\n"
                    "Выберите расход, с которого начинать файл, или продолжите с начала дня.",
                    reply_markup=_export_start_record_keyboard(chat_id, start_key, return_day_key),
                )
            except Exception as e:
                log_error(f"exp_pick_set_start: {e}")
            return

        if data_str.startswith("exp_pick_start_record:"):
            try:
                _, start_key, start_rid, return_day_key = data_str.split(":")
                start_dt = datetime.strptime(start_key, "%Y-%m-%d")
                store = get_chat_store(chat_id)
                safe_edit(
                    bot,
                    call,
                    "🎯 Точный CSV / Excel\n"
                    f"▶️ Начало: {exact_boundary_text(store, start_key, int(start_rid), True)}\n\n"
                    "Выберите конечную дату:",
                    reply_markup=_export_end_calendar_keyboard(
                        start_key,
                        int(start_rid),
                        start_dt.year,
                        start_dt.month,
                        return_day_key,
                    ),
                )
            except Exception as e:
                log_error(f"exp_pick_start_record: {e}")
            return

        if data_str.startswith("exp_pick_end:"):
            try:
                _, start_key, start_rid, y, m, return_day_key = data_str.split(":")
                store = get_chat_store(chat_id)
                safe_edit(
                    bot,
                    call,
                    "🎯 Точный CSV / Excel\n"
                    f"▶️ Начало: {exact_boundary_text(store, start_key, int(start_rid), True)}\n\n"
                    f"Выберите конечную дату: {russian_month_name(int(m))} {int(y)}",
                    reply_markup=_export_end_calendar_keyboard(
                        start_key,
                        int(start_rid),
                        int(y),
                        int(m),
                        return_day_key,
                    ),
                )
            except Exception as e:
                log_error(f"exp_pick_end: {e}")
            return

        if data_str.startswith("exp_pick_set_end:"):
            try:
                _, start_key, start_rid, y, m, d, return_day_key = data_str.split(":")
                end_key = _date_key_from_ymd(int(y), int(m), int(d))
                store = get_chat_store(chat_id)
                safe_edit(
                    bot,
                    call,
                    "🎯 Точный конец экспорта\n"
                    f"▶️ Начало: {exact_boundary_text(store, start_key, int(start_rid), True)}\n"
                    f"📅 Конечный день: {fmt_date_ddmmyy(end_key)}\n\n"
                    "Выберите последний расход, который включить в файл, или продолжите до конца дня.",
                    reply_markup=_export_end_record_keyboard(
                        chat_id,
                        start_key,
                        int(start_rid),
                        end_key,
                        return_day_key,
                    ),
                )
            except Exception as e:
                log_error(f"exp_pick_set_end: {e}")
            return

        if data_str.startswith("exp_pick_end_record:"):
            try:
                _, start_key, start_rid, end_key, end_rid, return_day_key = data_str.split(":")
                store = get_chat_store(chat_id)
                text = (
                    "🎯 Точный период выбран\n\n"
                    f"▶️ {exact_boundary_text(store, start_key, int(start_rid), True)}\n"
                    f"⏹ {exact_boundary_text(store, end_key, int(end_rid), False)}\n\n"
                    "Выберите формат файла:"
                )
                safe_edit(
                    bot,
                    call,
                    text,
                    reply_markup=_export_format_keyboard(
                        start_key,
                        int(start_rid),
                        end_key,
                        int(end_rid),
                        return_day_key,
                    ),
                )
            except Exception as e:
                log_error(f"exp_pick_end_record: {e}")
            return

        if data_str.startswith("exp_send:"):
            try:
                _, start_key, start_rid, end_key, end_rid, file_type, return_day_key = data_str.split(":")
                try:
                    bot.answer_callback_query(call.id, "Готовлю файл…", show_alert=False)
                except Exception:
                    pass
                try:
                    send_and_auto_delete(chat_id, "⏳ Готовлю точный экспорт в фоне…", 12)
                except Exception:
                    pass
                if not EXPORT_TASK_POOL.submit(
                    f"export:{chat_id}", send_exact_range_export,
                    chat_id, chat_id, start_key, int(start_rid), end_key, int(end_rid), file_type,
                ):
                    send_and_auto_delete(chat_id, "⛔ Очередь экспортов переполнена.", 12)
            except Exception as e:
                log_error(f"exp_send: {e}")
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
            if usd_transactions_view_enabled(chat_id):
                _base = datetime.strptime(base_day_key, "%Y-%m-%d").replace(day=1)
                d = (_base - timedelta(days=1)).replace(day=1)
            else:
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
            if usd_transactions_view_enabled(chat_id):
                _base = datetime.strptime(base_day_key, "%Y-%m-%d").replace(day=28) + timedelta(days=4)
                d = _base.replace(day=1)
            else:
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
        if cmd == "usd_tx_toggle":
            enabled = toggle_usd_transactions_view(chat_id)
            store["current_view_day"] = day_key
            txt, _ = render_day_window(chat_id, day_key)
            safe_edit(bot, call, txt, reply_markup=build_main_keyboard(day_key, chat_id), parse_mode="HTML")
            try:
                bot.answer_callback_query(call.id, "USD операции" if enabled else "ARS операции", show_alert=False)
            except Exception:
                pass
            return
        if cmd == "calendar":
            try:
                cdt = datetime.strptime(day_key, "%Y-%m-%d")
            except Exception:
                cdt = now_local()
            kb = build_calendar_keyboard(cdt, chat_id)
            safe_edit(bot, call, calendar_window_text(cdt), reply_markup=kb)
            register_open_window(
                chat_id, call.message.message_id, "local_fin_view", code="calendar", day_key=day_key,
                params={"view_action": "calendar", "center_day": cdt.strftime("%Y-%m-%d")},
            )
            return
        if cmd == "report":
            if usd_transactions_view_enabled(chat_id):
                txt, _ = render_usd_month_window(chat_id, day_key)
                safe_edit(bot, call, txt, reply_markup=build_main_keyboard(day_key, chat_id), parse_mode="HTML")
                return
            try:
                month_key = datetime.strptime(day_key, "%Y-%m-%d").strftime("%Y-%m")
            except Exception:
                month_key = now_local().strftime("%Y-%m")
            if chat_buttons_current_window_enabled(chat_id):
                report_html, _ = build_month_report_text(chat_id, month_key)
                safe_edit(bot, call, report_html, reply_markup=build_report_keyboard(month_key), parse_mode="HTML")
                register_open_window(
                    chat_id, call.message.message_id, "local_fin_view", code="report", day_key=day_key,
                    params={"view_action": "report", "month_key": month_key},
                )
            else:
                open_report_window(chat_id, month_key)
            return
        if cmd == "total":
            if usd_transactions_view_enabled(chat_id):
                month_key = str(day_key)[:7]
                rows = usd_records_for_month(chat_id, month_key)
                inc = sum(float(r.get("usd_amount", 0) or 0) for r in rows if float(r.get("usd_amount", 0) or 0) > 0)
                exp = sum(abs(float(r.get("usd_amount", 0) or 0)) for r in rows if float(r.get("usd_amount", 0) or 0) < 0)
                bal = usd_balance_for_chat(chat_id)
                text = wm_common(
                    f"💵 USD итог\n\nПриход за месяц: +${fmt_num_plain(inc)}\nРасход за месяц: -${fmt_num_plain(exp)}\nИтог месяца: {('+' if inc-exp >= 0 else '-')}${fmt_num_plain(abs(inc-exp))}\nUSD остаток по чату: {('+' if bal >= 0 else '-')}${fmt_num_plain(abs(bal))}",
                    4,
                )
                safe_edit(bot, call, text, reply_markup=build_main_keyboard(day_key, chat_id))
                return
            chat_bal = store.get("balance", 0)

            if not is_owner_chat(chat_id):
                text = wm_common(f"💰 Общий итог по этому чату: {format_chat_amount(chat_id, chat_bal, True)}", 4)
                if chat_buttons_current_window_enabled(chat_id):
                    safe_edit(bot, call, text, parse_mode="HTML")
                    register_open_window(
                        chat_id, call.message.message_id, "local_fin_view", code="total", day_key=day_key,
                        params={"view_action": "total", "depends_on_all": False},
                    )
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
            lines.append(f"• Этот чат ({title}): {format_chat_amount(chat_id, chat_bal, True)}")

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
                other_lines.append(f"   • {title2}: {format_chat_amount(chat_id, bal, True)}")
            if other_lines:
                lines.append("")
                lines.append("• Другие чаты:")
                lines.extend(other_lines)
            lines.append("")
            lines.append(f"• Всего по всем чатам: {format_chat_amount(chat_id, total_all, True)}")

            text = "\n".join(lines)
            if chat_buttons_current_window_enabled(chat_id):
                safe_edit(bot, call, wm_common(text, 4), parse_mode="HTML")
                register_open_window(
                    chat_id, call.message.message_id, "local_fin_view", code="total", day_key=day_key,
                    params={"view_action": "total", "depends_on_all": True},
                )
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
            if chat_buttons_current_window_enabled(chat_id):
                safe_edit(
                    bot,
                    call,
                    wm_common(build_info_text(chat_id), 9),
                    reply_markup=build_info_keyboard(chat_id),
                )
                register_open_window(
                    chat_id, call.message.message_id, "local_fin_view", code="info", day_key=day_key,
                    params={"view_action": "info"},
                )
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
        if cmd.startswith("backup_mass_"):
            if not is_owner_chat(chat_id):
                try:
                    bot.answer_callback_query(call.id, "BACKUP доступен только владельцу", show_alert=True)
                except Exception:
                    pass
                return
            target = cmd.replace("backup_mass_", "", 1)
            enabled_count, total_count = _backup_target_all_state(target)
            new_value = not bool(total_count and enabled_count == total_count)
            count = set_backup_target_for_all(target, new_value)
            try:
                bot.answer_callback_query(call.id, f"{'Включено' if new_value else 'Выключено'} для чатов: {count}")
            except Exception:
                pass
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
            # Сначала мгновенно возвращаем интерфейс. Очистку старых режимов и SQLite
            # выполняем после показа основного окна, чтобы кнопка не висела несколько секунд.
            store["current_view_day"] = day_key
            return_to_main_window_closing_previous(chat_id, day_key, call.message.message_id)

            def _cleanup_after_fast_back():
                try:
                    cancel_pending_window_commands(chat_id, delete_prompt=False)
                except Exception:
                    pass
                try:
                    clear_edit_delete_selection(chat_id, day_key)
                except Exception:
                    pass
                try:
                    save_data(data, chat_ids=[chat_id])
                except Exception:
                    pass

            if not GENERAL_TASK_POOL.submit(f"back-cleanup:{chat_id}", _cleanup_after_fast_back):
                _cleanup_after_fast_back()
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
            register_open_window(
                chat_id, call.message.message_id, "local_fin_view", code="csv_menu", day_key=day_key,
                params={"view_action": "csv_menu"},
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
        if cmd in {"csv_day", "csv_week", "csv_month", "csv_wedthu", "csv_all_real", "xlsx_day", "xlsx_week", "xlsx_month", "xlsx_wedthu", "xlsx_all", "xlsxstat_day", "xlsxstat_week", "xlsxstat_month", "xlsxstat_wedthu", "xlsxstat_all"}:
            if cmd.startswith("xlsxstat_"):
                file_type = "xlsxstat"
                mode = cmd.replace("xlsxstat_", "", 1)
            else:
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
            if usd_transactions_view_enabled(chat_id):
                rows = usd_records_for_month(chat_id, str(day_key)[:7])
                if not rows:
                    send_and_auto_delete(chat_id, "Нет USD-записей за этот месяц.")
                    return
                txt, _ = render_usd_month_window(chat_id, day_key)
                safe_edit(bot, call, txt, reply_markup=build_usd_edit_records_keyboard(day_key, chat_id), parse_mode="HTML")
                return
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
            register_open_window(
                chat_id, call.message.message_id, "local_fin_view", code="edit_list", day_key=day_key,
                params={"view_action": "edit_list"},
            )
            return

        if cmd.startswith("value_rec_"):
            if not effective_main_financial_value_buttons_enabled(chat_id):
                send_and_auto_delete(chat_id, "Этот режим финансовых кнопок сейчас выключен.", 8)
                return
            rid = int(cmd.split("_")[-1])
            start_record_edit_prompt(chat_id, day_key, rid)
            return

        if cmd.startswith("edit_rec_"):
            rid = int(cmd.split("_")[-1])
            start_record_edit_prompt(chat_id, day_key, rid)
            return
        if cmd.startswith("del_toggle_"):
            rid = int(cmd.split("_")[-1])
            toggle_edit_delete_selection(chat_id, day_key, rid)
            txt, _ = render_day_window(chat_id, day_key)
            safe_edit(bot, call, txt, reply_markup=build_edit_records_keyboard(day_key, chat_id), parse_mode="HTML")
            register_open_window(chat_id, call.message.message_id, "local_fin_view", code="edit_list", day_key=day_key, params={"view_action": "edit_list"})
            return
        if cmd == "del_selected":
            count = delete_selected_records(chat_id, day_key)
            txt, _ = render_day_window(chat_id, day_key)
            safe_edit(bot, call, txt, reply_markup=build_edit_records_keyboard(day_key, chat_id), parse_mode="HTML")
            register_open_window(chat_id, call.message.message_id, "local_fin_view", code="edit_list", day_key=day_key, params={"view_action": "edit_list"})
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
            register_open_window(
                chat_id, call.message.message_id, "fin_view", code="fv:open", day_key=view_day,
                params={"target_chat_id": tgt, "owner_day_key": day_key, "view_action": "open"},
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
            register_open_window(
                chat_id, call.message.message_id, "fin_view", code="fv:open", day_key=view_day,
                params={"target_chat_id": tgt, "owner_day_key": day_key, "view_action": "open"},
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
            safe_edit(bot, call, calendar_window_text(cdt, marker=False), reply_markup=build_calendar_keyboard(cdt, chat_id))
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
                rows.append((fmt_date_table(d), fmt_csv_amount(r["amount"]), r.get("note", "")))

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
                    rows.append((fmt_date_table(d), fmt_csv_amount(r["amount"]), r.get("note", "")))

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
                rows.append((fmt_date_table(d), fmt_csv_amount(r["amount"]), r.get("note", "")))

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
    day_key=None,
    usd_amount=None,
    usd_note: str = "",
    usd_only: bool = False,
    source_finance_text: str = "",
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
        if usd_amount is not None:
            rec["usd_amount"] = float(usd_amount or 0)
            rec["usd_note"] = str(usd_note or note or "")
            rec["usd_only"] = bool(usd_only)
        if source_finance_text:
            rec["source_finance_text"] = str(source_finance_text)

        store.setdefault("records", []).append(rec)
        normalize_chat_records(chat_id)
        store["next_id"] = max([int(r.get("id", 0) or 0) for r in store.get("records", [])] + [0]) + 1
        store["balance"] = sum(float(r.get("amount", 0) or 0) for r in store.get("records", []))

        rebuild_month_short_ids(chat_id)
        rebuild_global_records()
        return rec

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
    register_open_window(chat_id, message_id, "main_day", code="О1", day_key=day_key)
    save_data(data)
def get_active_window_id(chat_id: int, day_key: str):
    aw = get_or_create_active_windows(chat_id)
    return aw.get(day_key)

def clear_active_window_id(chat_id: int, day_key: str):
    try:
        aw = get_or_create_active_windows(chat_id)
        if str(day_key) in aw:
            old_mid = aw.pop(str(day_key), None)
            if old_mid:
                unregister_open_window(chat_id, old_mid)
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
            text = wm_common(f"💰 Общий итог по этому чату: {format_chat_amount(chat_id, chat_bal, True)}", 4)
        else:
            lines = []
            info = store.get("info", {})
            title = get_chat_display_name(chat_id)
            lines.append("💰 Общий итог (для владельца)")
            lines.append("")
            lines.append(f"• Этот чат ({title}): {format_chat_amount(chat_id, chat_bal, True)}")
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
                other_lines.append(f"   • {title2}: {format_chat_amount(chat_id, bal, True)}")
            if other_lines:
                lines.append("")
                lines.append("• Другие чаты:")
                lines.extend(other_lines)
            lines.append("")
            lines.append(f"• Всего по всем чатам: {format_chat_amount(chat_id, total_all, True)}")
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
        clear_forward_copy_edit_wait(chat_id, delete_prompt=delete_prompt)
    except Exception:
        pass
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
    view_day = finance_today_key()
    store["current_view_day"] = view_day
    store.setdefault("settings", {})["auto_add"] = True

    save_data(data)
    schedule_finalize(chat_id, view_day)

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

    day_key = finance_today_key() if is_finance_mode(chat_id) else today_key()
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
                    fmt_date_table(day_key),
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
            bot.send_document(chat_id, f, caption=f"📅 CSV за день {fmt_date_table(day_key)}: {get_chat_display_name(chat_id)}")
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
    try:
        notice = bot.send_message(chat_id, "⏳ Готовлю /tabl_lsx в фоне…")
        delete_message_later(chat_id, notice.message_id, 20)
    except Exception:
        pass
    if not EXPORT_TASK_POOL.submit(f"tabl-lsx:{chat_id}", send_tabl_lsx_for_chat, chat_id, chat_id):
        send_and_auto_delete(chat_id, "⛔ Очередь Excel переполнена.", 12)


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
def _send_json_snapshot_job(chat_id: int):
    started = time.time()
    try:
        bot_journal("json_export_start", chat_id, "создание атомарного снимка")
        store = snapshot_chat_store(chat_id)
        payload = build_chat_backup_payload(chat_id, store)
        raw = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        buf = io.BytesIO(raw)
        buf.name = f"{mega_safe_name(get_chat_display_name(chat_id), 'chat')}_{now_local().strftime('%Y%m%d_%H%M%S')}.json"
        sent = _tg_call_retry(bot.send_document, chat_id, buf, caption="🧾 JSON этого чата — последние операции сверху", purpose="manual_json_export")
        elapsed = time.time() - started
        bot_journal("json_export_sent", chat_id, f"bytes={len(raw)} message_id={getattr(sent, 'message_id', '')} elapsed={elapsed:.3f}s")
    except Exception as e:
        bot_journal("json_export_error", chat_id, f"elapsed={time.time()-started:.3f}s error={e}", "ERROR")
        send_and_auto_delete(chat_id, "❌ Не удалось создать JSON. Ошибка записана в журнал.", 15)


@bot.message_handler(commands=["json"])
def cmd_json(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass
    schedule_command_delete(msg)
    chat_id = int(msg.chat.id)
    if is_finance_output_suppressed(chat_id):
        return
    stop_dozvon_for_target(chat_id)
    if guard_non_owner_finance_for_command(msg, {"ok", "help"}):
        return
    if not require_finance(chat_id):
        return
    bot_journal("json_command", chat_id, f"message_id={getattr(msg, 'message_id', '')}")
    if not EXPORT_TASK_POOL.submit(f"json:{chat_id}", _send_json_snapshot_job, chat_id):
        send_and_auto_delete(chat_id, "⛔ Очередь JSON занята. Повторите команду.", 10)

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
            try:
                bot.delete_message(chat_id, msg.message_id)
            except Exception:
                pass
        DELAYED_SCHEDULER.schedule(f"auto-delete:{chat_id}:{msg.message_id}", delay, _delete)
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
            try:
                bot.delete_message(chat_id, msg.message_id)
            except Exception:
                pass
        DELAYED_SCHEDULER.schedule(f"auto-delete-html:{chat_id}:{msg.message_id}", delay, _delete)
    except Exception as e:
        log_error(f"send_html_and_auto_delete: {e}")
def delete_message_later(chat_id: int, message_id: int, delay: int = 30):
    """
    Отложенное удаление сообщения пользователя (например, команд).
    """
    try:
        def _job():
            try:
                bot.delete_message(chat_id, message_id)
            except Exception:
                pass
        DELAYED_SCHEDULER.schedule(f"delete-later:{chat_id}:{message_id}", delay, _job)
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
    _edit_cancel_timers.pop(key, None)
    DELAYED_SCHEDULER.cancel(f"edit-wait:{int(chat_id)}")

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
    _edit_cancel_timers.pop(key, None)
    DELAYED_SCHEDULER.cancel(f"finwin-edit-wait:{int(chat_id)}")

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
    """Автоотмена фин-редактирования без отдельного потока на каждое окно."""
    key = (int(chat_id), "finwin_edit_wait")
    scheduler_key = f"finwin-edit-wait:{int(chat_id)}"

    def _job():
        try:
            store = get_chat_store(chat_id)
            wait = store.get("finwin_edit_wait") or {}
            if not wait or int(wait.get("prompt_msg_id") or 0) != int(prompt_message_id):
                return
            cleared = clear_finwin_edit_wait_state(chat_id, prompt_message_id, delete_prompt=True)
            if cleared:
                log_info(f"finwin edit_wait auto-cancelled for chat {chat_id}")
        except Exception as e:
            log_error(f"schedule_cancel_finwin_edit({chat_id},{prompt_message_id}): {e}")

    DELAYED_SCHEDULER.cancel(scheduler_key)
    deadline = DELAYED_SCHEDULER.schedule(scheduler_key, float(delay), _job)
    _edit_cancel_timers[key] = deadline


def schedule_cancel_edit(chat_id: int, prompt_message_id: int, delay: float = 40.0):
    """Автоотмена редактирования без отдельного потока на каждое окно."""
    key = (int(chat_id), "edit_wait")
    scheduler_key = f"edit-wait:{int(chat_id)}"

    def _job():
        try:
            store = get_chat_store(chat_id)
            wait = store.get("edit_wait") or {}
            if not wait or int(wait.get("prompt_msg_id") or 0) != int(prompt_message_id):
                return
            cleared = clear_edit_wait_state(chat_id, prompt_message_id, delete_prompt=True)
            if cleared:
                send_and_auto_delete(chat_id, "⌛ Время редактирования истекло. Режим редактирования отменён.", 8)
        except Exception as e:
            log_error(f"schedule_cancel_edit({chat_id},{prompt_message_id}): {e}")

    DELAYED_SCHEDULER.cancel(scheduler_key)
    deadline = DELAYED_SCHEDULER.schedule(scheduler_key, float(delay), _job)
    _edit_cancel_timers[key] = deadline


def schedule_cancel_wait(chat_id: int, delay: float = 15.0):
    """Через delay секунд сбрасывает reset_wait через общий планировщик."""
    scheduler_key = f"reset-wait:{int(chat_id)}"

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

    DELAYED_SCHEDULER.cancel(scheduler_key)
    deadline = DELAYED_SCHEDULER.schedule(scheduler_key, float(delay), _job)
    _edit_cancel_timers[int(chat_id)] = deadline


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

        # Перед добавлением убираем старые карточки того же чата по username/title, чтобы не плодить дубли.
        new_identity = _chat_identity_key(chat_id, new_known)
        for old_cid, old_info in list(kc.items()):
            try:
                old_id_int = int(old_cid)
            except Exception:
                kc.pop(old_cid, None)
                changed = True
                continue
            if str(old_cid) != str(chat_id) and _chat_identity_key(old_id_int, old_info if isinstance(old_info, dict) else {}) == new_identity:
                kc.pop(old_cid, None)
                changed = True
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
        DELAYED_SCHEDULER.schedule(f"owner-json-restore-cleanup:{int(key)}", delay, _job)
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
    usd_month_counters = {}

    for dk in sorted(daily.keys()):
        month_key = dk[:7]
        month_counters.setdefault(month_key, 1)
        usd_month_counters.setdefault(month_key, 1)
        recs = sorted(daily.get(dk, []) or [], key=record_sort_key)
        daily[dk] = recs
        for r in recs:
            has_usd = bool(float(r.get("usd_amount", 0) or 0))
            usd_only = bool(r.get("usd_only", False))
            if not usd_only:
                r["short_id"] = f"R{month_counters[month_key]}"
                month_counters[month_key] += 1
            elif has_usd:
                r["short_id"] = f"U{usd_month_counters[month_key]}"
            if has_usd:
                r["usd_short_id"] = f"U{usd_month_counters[month_key]}"
                usd_month_counters[month_key] += 1

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
    """Быстрый общий итог без копирования всех записей всех чатов при каждом сообщении."""
    with data_lock:
        total = 0.0
        for _cid, store in (data.get("chats", {}) or {}).items():
            try:
                if "balance" in store:
                    total += float(store.get("balance", 0) or 0)
                else:
                    total += sum(float(r.get("amount", 0) or 0) for r in (store.get("records", []) or []))
            except Exception:
                pass
        # Полные записи уже находятся в chats; дублировать их в root больше не нужно.
        data["records"] = []
        data["overall_balance"] = total

_finalize_timers = {}
_backup_timers = {}
_quick_backup_timers = {}
_balance_panel_refresh_timers = {}
_balance_panel_collapse_timers = {}
_balance_panel_first_timers = {}
_balance_panel_recreate_timers = {}
_total_message_timers = {}
_backup_dirty_chats = set()
_quick_backup_dirty_chats = set()
_global_mega_timer = None

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
        DELAYED_SCHEDULER.schedule("startup-main-windows", delay, _job)
    except Exception as e:
        log_error(f"schedule_startup_main_windows: {e}")

def schedule_all_finance_backups(delay: float = 10.0):
    for cid in collect_finance_chat_ids():
        schedule_backup_flush(cid, delay=delay)


def _schedule_global_mega_snapshot(delay: float = 30.0):
    """Совместимость старых вызовов: v90 лишь отмечает pending full snapshot.

    Полный global больше не создаётся через 20–30 секунд после каждого чата.
    Его запускает общий quiet/max scheduler.
    """
    _mark_global_snapshot_pending()


def _run_quick_chat_backup(chat_id: int):
    """v90 quick backup = маленький immutable delta, а не полная копия чата/global."""
    chat_id = int(chat_id)
    if RESTORE_GUARD_ACTIVE:
        log_error(f"QUICK DELTA BLOCKED {chat_id}: {RESTORE_GUARD_REASON}")
        return
    with state_chat_context(chat_id):
        try:
            save_data(data, chat_ids=[chat_id])
            with _delta_state_lock:
                _delta_pending_chats.add(chat_id)
                if not _delta_chat_generation.get(chat_id):
                    _delta_chat_generation[chat_id] = int(time.time_ns())
            if not _run_delta_batch():
                schedule_delta_backup(chat_id, BACKUP_BUSY_RETRY_SECONDS, reason="delta_retry")
        finally:
            with timer_lock:
                _quick_backup_dirty_chats.discard(chat_id)


def _run_full_chat_backup(chat_id: int):
    chat_id = int(chat_id)
    if RESTORE_GUARD_ACTIVE:
        log_error(f"FULL BACKUP BLOCKED {chat_id}: {RESTORE_GUARD_REASON}")
        return
    trace = ProcessTrace(chat_id, f"Бэкап: {get_chat_display_name(chat_id)}").start()
    with state_chat_context(chat_id):
        try:
            if not is_finance_mode(chat_id):
                trace.step("финрежим выключен — пропуск")
                trace.finish("бэкап завершён")
                return
            if not is_auto_backup_enabled(chat_id):
                trace.step("все авто-бэкапы выключены — пропуск")
                trace.finish("бэкап завершён")
                return
            save_data(data, chat_ids=[chat_id])
            trace.step("создаёт JSON/CSV" + ("/Excel" if backup_excel_all_enabled() else ""))
            save_chat_json(chat_id)

            # Канал/личный чат работают как раньше, но MEGA-файлы теперь заменяются
            # через candidate -> history -> move, без предварительного удаления.
            if is_backup_to_chat_enabled(chat_id) and can_receive_direct_json_backup(chat_id) and not is_finance_output_suppressed(chat_id):
                send_backup_to_chat(chat_id, ensure_files=False)
            if is_backup_to_channel_enabled(chat_id):
                send_backup_to_channel(chat_id, ensure_files=False)
            if is_backup_to_mega_enabled(chat_id):
                trace.step("безопасно обновляет JSON чата и месячный JSON в MEGA")
                mega_upload_chat_backup_bundle(chat_id, current_month_key())
                _mark_global_snapshot_pending()
            trace.finish("бэкап завершён")
        except Exception as exc:
            trace.fail(exc)
            log_error(f"_run_full_chat_backup({chat_id}): {exc}")
        finally:
            with timer_lock:
                _backup_dirty_chats.discard(chat_id)
                _backup_timers.pop(chat_id, None)


def schedule_quick_backup(chat_id: int, delay: float | None = None):
    """Debounce delta для конкретного чата; разные чаты объединяются общим delta batch."""
    chat_id = int(chat_id)
    if RESTORE_GUARD_ACTIVE:
        return
    if delay is None:
        delay = MEGA_DELTA_PRIORITY_DELAY_SECONDS if mega_backup_priority_enabled() else MEGA_DELTA_DELAY_SECONDS
    due = time.time() + max(0.5, float(delay))
    with timer_lock:
        _quick_backup_dirty_chats.add(chat_id)
        _quick_backup_timers[chat_id] = due
    with _delta_state_lock:
        global _delta_generation
        _delta_generation += 1
        _delta_pending_chats.add(chat_id)
        _delta_chat_generation[chat_id] = _delta_generation

    def _fire():
        # Одна общая задача заберёт изменения всех чатов, накопившиеся к этому моменту.
        def _job():
            if not _run_delta_batch():
                schedule_delta_backup(None, delay=BACKUP_BUSY_RETRY_SECONDS, reason="quick_upload_retry")
        if not DELTA_TASK_POOL.submit("mega-delta-v90", _job):
            log_error(f"QUICK DELTA QUEUE FULL, RETRY: {chat_id}")
            schedule_quick_backup(chat_id, BACKUP_BUSY_RETRY_SECONDS)
    DELAYED_SCHEDULER.cancel("mega-delta-batch-v90")
    DELAYED_SCHEDULER.schedule("mega-delta-batch-v90", max(0.5, float(delay)), _fire)


def schedule_full_backup_only(chat_id: int, delay: float = 3.0):
    """Тяжёлый JSON/канал/MEGA-файл чата — отдельно от быстрого delta."""
    chat_id = int(chat_id)
    if RESTORE_GUARD_ACTIVE:
        log_error(f"FULL BACKUP SCHEDULE BLOCKED {chat_id}: {RESTORE_GUARD_REASON}")
        return
    try:
        delay = max(float(delay or 0), BACKUP_MIN_DELAY_SECONDS)
    except Exception:
        delay = BACKUP_MIN_DELAY_SECONDS
    due = time.time() + delay
    with timer_lock:
        _backup_dirty_chats.add(chat_id)
        _backup_timers[chat_id] = due
    def _fire():
        with timer_lock:
            _backup_timers.pop(chat_id, None)
        if not BACKUP_TASK_POOL.submit(f"full:{chat_id}", _run_full_chat_backup, chat_id):
            log_error(f"FULL BACKUP QUEUE FULL, RETRY: {chat_id}")
            schedule_full_backup_only(chat_id, BACKUP_BUSY_RETRY_SECONDS)
    DELAYED_SCHEDULER.schedule(f"full-backup:{chat_id}", delay, _fire)


def schedule_backup_flush(chat_id: int, delay: float = 3.0):
    """SQLite уже сохранена; delta быстро; полный файл чата после 120 сек. тишины."""
    chat_id = int(chat_id)
    if RESTORE_GUARD_ACTIVE:
        log_error(f"BACKUP SCHEDULE BLOCKED {chat_id}: {RESTORE_GUARD_REASON}")
        return
    quick_delay = MEGA_DELTA_PRIORITY_DELAY_SECONDS if mega_backup_priority_enabled() else MEGA_DELTA_DELAY_SECONDS
    schedule_quick_backup(chat_id, quick_delay)
    schedule_full_backup_only(chat_id, delay)

def _safe_stabilize(action_name, func):
    try:
        try:
            if verbose_process_journal_enabled():
                bot_journal("process_call_start", None, str(action_name))
        except Exception:
            pass
        res = func()
        try:
            if verbose_process_journal_enabled():
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
            _safe_stabilize("currency_ledger_snapshot", lambda: _snapshot_active_currency_ledger(store, _ensure_currency_ledgers(store)))
            _safe_stabilize("save_data", lambda: save_data(data, chat_ids=[chat_id]))

            trace.step("проверяет скрытый финрежим")
            hidden = is_finance_output_suppressed(chat_id)

        # v90: сразу после подтверждённой SQLite ставим маленький delta, ДО Telegram-окон.
        # Поэтому медленное редактирование интерфейса не откладывает аварийную копию.
        trace.step("ставит быстрый delta до обновления окон")
        _safe_stabilize(
            "delta_queue_early",
            lambda: schedule_quick_backup(
                chat_id,
                MEGA_DELTA_PRIORITY_DELAY_SECONDS if mega_backup_priority_enabled() else MEGA_DELTA_DELAY_SECONDS,
            ),
        )

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

        # Реестр обновляем даже для скрытого финрежима: сам скрытый чат может не показывать финансы,
        # но открытое у владельца окно этого чата обязано синхронизироваться.
        trace.step("обновляет зарегистрированные открытые окна")
        _safe_stabilize("open_windows_registry", lambda: refresh_registered_financial_windows(chat_id))

        trace.step("ставит бэкап в отдельную очередь")
        _safe_stabilize("full_backup_queue", lambda: schedule_full_backup_only(chat_id, BACKUP_MIN_DELAY_SECONDS))

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
        if not FINANCE_TASK_POOL.submit(chat_id, _finance_changed_now, chat_id, day_key, reason):
            log_error(f"FINANCE QUEUE FULL, RETRY: {chat_id}")
            with timer_lock:
                _finalize_timers[chat_id] = time.time() + 1.0
            DELAYED_SCHEDULER.schedule(f"finance-finalize:{chat_id}", 1.0, _fire_finance)

    with timer_lock:
        _finalize_timers[chat_id] = time.time() + max(0.0, float(delay))
    def _fire_finance():
        with timer_lock:
            _finalize_timers.pop(chat_id, None)
        _job()
    DELAYED_SCHEDULER.schedule(f"finance-finalize:{chat_id}", delay, _fire_finance)


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

def cancel_auto_delete_for_message(chat_id: int, message_id: int):
    """Если окно с автоудалением превращается кнопкой «Назад» в основное — его старый таймер больше не должен удалить О1."""
    chat_id = int(chat_id)
    message_id = int(message_id)
    try:
        _cancel_v98_auto_close(chat_id, message_id)
    except Exception:
        pass
    for key in (
        f"auto-delete:{chat_id}:{message_id}",
        f"auto-delete-html:{chat_id}:{message_id}",
        f"delete-later:{chat_id}:{message_id}",
    ):
        try:
            DELAYED_SCHEDULER.cancel(key)
        except Exception:
            pass
    try:
        store = get_chat_store(chat_id)
        was_total_window = int(store.get("total_msg_id") or 0) == message_id
        # Отменяем только реальные stored-window таймеры этого чата, а не перебираем
        # все числовые поля store (id записи/флаги не должны случайно очищаться).
        for timer_key in list(_aux_window_timers.keys()):
            try:
                timer_chat_id, store_key = timer_key
                if int(timer_chat_id) != chat_id:
                    continue
                if int(store.get(str(store_key)) or 0) != message_id:
                    continue
            except Exception:
                continue
            DELAYED_SCHEDULER.cancel(f"stored-window-delete:{chat_id}:{store_key}")
            _aux_window_timers.pop(timer_key, None)
            store[str(store_key)] = None

        if was_total_window:
            DELAYED_SCHEDULER.cancel(f"owner-total-delete:{chat_id}")
            _total_message_timers.pop(chat_id, None)
            store["total_msg_id"] = None
        # Сохранение сделает уже существующая фоновая очистка back_main; здесь важно
        # не задерживать мгновенное возвращение в основное окно.
    except Exception as e:
        log_error(f"cancel_auto_delete_for_message({chat_id},{message_id}): {e}")


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
    """Мгновенный возврат в О1 без синхронного удаления и тяжёлого backup_window_for_owner."""
    chat_id = int(chat_id)
    try:
        current_message_id = int(current_message_id) if current_message_id is not None else None
    except Exception:
        current_message_id = None

    try:
        if current_message_id is not None:
            cancel_auto_delete_for_message(chat_id, current_message_id)
            cancel_fast_ui_edit(chat_id, current_message_id)
    except Exception:
        pass

    try:
        old_mid = get_active_window_id(chat_id, day_key)
    except Exception:
        old_mid = None

    txt, _ = render_day_window(chat_id, day_key)
    kb = build_main_keyboard(day_key, chat_id)

    if current_message_id is not None:
        set_active_window_id(chat_id, day_key, current_message_id)
        result = fast_ui_edit_message_text(
            chat_id, current_message_id, txt,
            reply_markup=kb, parse_mode="HTML", purpose="back_main_instant",
        )
        bot_journal("back_main_fast", chat_id, f"day={day_key} result={result} old={old_mid} current={current_message_id}")

        if old_mid and int(old_mid) != current_message_id:
            def _delete_old():
                try:
                    _tg_call_retry(bot.delete_message, chat_id, int(old_mid), attempts=1, purpose="back_main_delete_old")
                except Exception:
                    pass
            GENERAL_TASK_POOL.submit(f"back-delete:{chat_id}:{old_mid}", _delete_old)
        schedule_balance_panel_refresh(chat_id, 0.05)
        return

    def _send_fallback():
        try:
            update_or_send_day_window(chat_id, day_key)
        except Exception as e:
            log_error(f"return_to_main fallback({chat_id},{day_key}): {e}")
    if not GENERAL_TASK_POOL.submit(f"back-send:{chat_id}", _send_fallback):
        _send_fallback()

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
KEEP_ALIVE_STATE = {
    "started_at": None,
    "last_attempt_at": None,
    "last_ok_at": None,
    "last_error": "",
    "last_status_code": None,
    "ok_count": 0,
    "fail_count": 0,
    "telegram_ok_at": None,
}
_keep_alive_thread = None
_keep_alive_thread_lock = threading.RLock()


def _keep_alive_base_candidates() -> list[str]:
    result = []
    extra = os.getenv("KEEP_ALIVE_URLS", "")
    values = [APP_URL, WEBHOOK_URL, os.getenv("RENDER_EXTERNAL_URL", "").strip(), _RENDER_HOST_URL]
    if extra:
        values.extend(x.strip() for x in extra.split(","))
    for raw in values:
        if not raw:
            continue
        base = str(raw).strip().rstrip("/")
        if base and base not in result:
            result.append(base)
    return result


def keep_alive_task():
    session = requests.Session()
    cycle = 0
    KEEP_ALIVE_STATE["started_at"] = _journal_ts()
    while True:
        cycle_started = time.time()
        try:
            if not KEEP_ALIVE_ENABLED:
                time.sleep(max(20, KEEP_ALIVE_INTERVAL_SECONDS))
                continue

            KEEP_ALIVE_STATE["last_attempt_at"] = _journal_ts()
            bases = _keep_alive_base_candidates()
            ok = False
            last_error = ""
            last_code = None

            for base in bases:
                for path in ("/keepalive", "/healthz", "/"):
                    url = f"{base}{path}?ts={int(time.time() * 1000)}"
                    try:
                        resp = session.get(url, timeout=12, headers={"Cache-Control": "no-cache", "User-Agent": f"{VERSION}-keepalive"})
                        last_code = int(resp.status_code)
                        if 200 <= resp.status_code < 500:
                            ok = True
                            break
                        last_error = f"HTTP {resp.status_code} {url}"
                    except Exception as e:
                        last_error = f"{url}: {e}"
                if ok:
                    break

            cycle += 1
            if cycle % KEEP_ALIVE_TELEGRAM_EVERY == 0:
                try:
                    bot.get_me()
                    KEEP_ALIVE_STATE["telegram_ok_at"] = _journal_ts()
                except Exception as e:
                    last_error = (last_error + " | " if last_error else "") + f"Telegram getMe: {e}"

            KEEP_ALIVE_STATE["last_status_code"] = last_code
            if ok:
                KEEP_ALIVE_STATE["last_ok_at"] = _journal_ts()
                KEEP_ALIVE_STATE["last_error"] = ""
                KEEP_ALIVE_STATE["ok_count"] = int(KEEP_ALIVE_STATE.get("ok_count", 0)) + 1
                if cycle == 1 or cycle % 20 == 0:
                    log_info(f"Keep-alive OK: status={last_code}, bases={len(bases)}")
            else:
                KEEP_ALIVE_STATE["last_error"] = last_error or "APP_URL / WEBHOOK_URL не заданы"
                KEEP_ALIVE_STATE["fail_count"] = int(KEEP_ALIVE_STATE.get("fail_count", 0)) + 1
                log_error(f"Keep-alive failed: {KEEP_ALIVE_STATE['last_error']}")
        except Exception as e:
            KEEP_ALIVE_STATE["last_error"] = str(e)[:500]
            KEEP_ALIVE_STATE["fail_count"] = int(KEEP_ALIVE_STATE.get("fail_count", 0)) + 1
            log_error(f"Keep-alive loop error: {e}")

        elapsed = time.time() - cycle_started
        time.sleep(max(20.0, float(KEEP_ALIVE_INTERVAL_SECONDS) - elapsed))

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
        bot_journal("edited_message_received", chat_id, f"msg={getattr(msg, 'message_id', 0)}")
    except Exception:
        pass

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
                                            

@bot.message_handler(commands=["restore_guard"])
def cmd_restore_guard(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass
    schedule_command_delete(msg)
    chat_id = msg.chat.id
    if not is_owner_chat(chat_id):
        return
    state = "ВКЛ" if RESTORE_GUARD_ACTIVE else "ВЫКЛ"
    send_and_auto_delete(
        chat_id,
        f"🛡 Restore guard: {state}\nПричина: {RESTORE_GUARD_REASON or '-'}\n"
        f"Автобэкапы: {'заблокированы' if RESTORE_GUARD_ACTIVE else 'разрешены'}",
        120,
    )


@bot.message_handler(commands=["delta_status"])
def cmd_delta_status(msg):
    try:
        update_chat_info_from_message(msg)
    except Exception:
        pass
    schedule_command_delete(msg)
    if not is_owner_chat(msg.chat.id):
        return
    send_and_auto_delete(msg.chat.id, delta_status_text(), 120)


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


@bot.message_handler(commands=["mega_restore_now"])
def cmd_mega_restore_now(msg):
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
        send_and_auto_delete(chat_id, "☁️ Запускаю полное восстановление из MEGA…", 20)
        ok, detail = mega_restore_full_from_cloud(force=True)
        if ok:
            try:
                refresh_registered_financial_windows(chat_id)
            except Exception:
                pass
            try:
                schedule_startup_main_windows(delay=0.5)
            except Exception:
                pass
            send_and_auto_delete(chat_id, "✅ " + detail, 120)
        else:
            send_and_auto_delete(chat_id, "❌ " + detail, 120)
    except Exception as e:
        log_error(f"cmd_mega_restore_now: {e}")
        send_and_auto_delete(chat_id, "❌ Ошибка восстановления из MEGA: " + str(e)[:500], 120)


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
        if RESTORE_GUARD_ACTIVE:
            send_and_auto_delete(chat_id, "🚨 Бэкап заблокирован: " + RESTORE_GUARD_REASON, 120)
            return
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
        f"Очередь webhook: {WEBHOOK_TASK_POOL.stats()['pending']}",
        f"Очередь пересылки: {FORWARD_TASK_POOL.stats()['pending']}",
        f"Очередь финансов: {FINANCE_TASK_POOL.stats()['pending']}",
        f"Очередь delta: {DELTA_TASK_POOL.stats()['pending']}",
        f"Очередь backup: {BACKUP_TASK_POOL.stats()['pending']}",
        f"BACKUP_CHAT_ID: {'есть' if BACKUP_CHAT_ID else 'нет'}",
        f"Бэкап в канал: {'ВКЛ' if backup_flags.get('channel', True) else 'ВЫКЛ'}",
        f"MEGA: {'ВКЛ' if MEGA_ENABLED else 'ВЫКЛ'} / {'настроено' if mega_is_configured() else 'не настроено'}",
        f"MEGA dir: {MEGA_BACKUP_DIR}",
        f"MEGA delta dir: {mega_delta_remote_root()}",
        f"Delta pending: {len(_delta_pending_chats)} / last events: {_delta_last_event_count}",
        f"Global full pending: {'да' if _global_snapshot_pending else 'нет'}",
        f"Ошибок в журнале: {len(get_recent_errors(80))}",
    ]
    if errors:
        lines.append("")
        lines.append("Последние ошибки:")
        for e in errors:
            lines.append(f"• {e.get('ts','')} — {format_error_for_owner(e.get('msg',''))[:160]}")
    return "\n".join(lines)


@bot.message_handler(commands=["off_on_backup_excel"])
def cmd_off_on_backup_excel(msg):
    update_chat_info_from_message(msg)
    schedule_command_delete(msg)
    chat_id = msg.chat.id
    if not is_owner_chat(chat_id):
        send_and_auto_delete(chat_id, "Эта команда только для владельца.", HELPER_DELETE_DELAY)
        return
    enabled = toggle_backup_excel_all_enabled()
    if enabled:
        for cid in collect_finance_chat_ids():
            schedule_backup_flush(cid, BACKUP_MIN_DELAY_SECONDS)
    send_and_auto_delete(chat_id, f"📊 Excel-бэкап всех чатов: {'ВКЛ' if enabled else 'ВЫКЛ'}", 20)


@bot.message_handler(commands=["queues", "queue_status"])
def cmd_queues(msg):
    update_chat_info_from_message(msg)
    schedule_command_delete(msg)
    chat_id = msg.chat.id
    if not is_owner_chat(chat_id):
        send_and_auto_delete(chat_id, "Эта команда только для владельца.", HELPER_DELETE_DELAY)
        return
    send_and_auto_delete(chat_id, build_queue_status_text(), 90)


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
    global _keep_alive_thread
    with _keep_alive_thread_lock:
        if _keep_alive_thread is not None and _keep_alive_thread.is_alive():
            return _keep_alive_thread
        _keep_alive_thread = threading.Thread(target=keep_alive_task, name="keep-alive-watchdog", daemon=True)
        _keep_alive_thread.start()
        return _keep_alive_thread
@app.route("/", methods=["GET"])
def index():
    return "OK", 200


@app.route("/healthz", methods=["GET"])
def healthz():
    return "OK", 200


@app.route("/keepalive", methods=["GET", "HEAD"])
def keepalive_endpoint():
    KEEP_ALIVE_STATE["external_ping_at"] = _journal_ts()
    if request.method == "HEAD":
        return "", 200
    return {
        "ok": True,
        "version": VERSION,
        "time": _journal_ts(),
        "profile": active_bot_behavior_profile(),
        "keep_alive": KEEP_ALIVE_ENABLED,
    }, 200


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
        update_key = update_chat_id if update_chat_id is not None else getattr(update, "update_id", time.time_ns())

        update_enqueued_at = time.time()
        update_id = getattr(update, "update_id", None)
        update_type = "edited_message" if isinstance(payload, dict) and "edited_message" in payload else "callback_query" if isinstance(payload, dict) and "callback_query" in payload else "message" if isinstance(payload, dict) and "message" in payload else "other"

        def _process_update():
            started = time.time()
            wait = started - update_enqueued_at
            bot_journal("update_process_start", update_chat_id, f"update_id={update_id} type={update_type} queue_wait={wait:.3f}s")
            try:
                with state_chat_context(update_chat_id):
                    if update_chat_id is None:
                        bot.process_new_updates([update])
                    else:
                        with locked_chat(update_chat_id):
                            bot.process_new_updates([update])
            finally:
                bot_journal("update_process_done", update_chat_id, f"update_id={update_id} type={update_type} queue_wait={wait:.3f}s process={time.time()-started:.3f}s total={time.time()-update_enqueued_at:.3f}s")

        if not WEBHOOK_TASK_POOL.submit(update_key, _process_update):
            log_error(f"WEBHOOK QUEUE FULL: chat={update_chat_id}")
            # Telegram повторит update позже; данные не теряются молча.
            return "BUSY", 503
    except Exception as e:
        log_error(f"WEBHOOK: enqueue update error: {e}")
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
    if not RESTORE_GUARD_ACTIVE:
        migrate_legacy_owner_secrets()
    try:
        gs = data.setdefault("_global_settings", {})
        if not bool(gs.get("journal_default_off_v83_applied", False)):
            gs["bot_journal_enabled"] = False
            for _cid, _store in (data.get("chats", {}) or {}).items():
                if isinstance(_store, dict):
                    _store.setdefault("settings", {})["journal_enabled"] = False
            gs["journal_default_off_v83_applied"] = True
        gs.setdefault("bot_behavior_profile", DEFAULT_BOT_BEHAVIOR_PROFILE)
        # Новая база v90: интерфейсный профиль сохраняется; ядро хранения = delta + редкие snapshots.
        # Явно выбранные старые версии сохраняются без изменений.
        if not bool(gs.get("version_mode_v88_migrated", False)):
            if str(gs.get("bot_behavior_profile") or "") == "v87_current":
                gs["bot_behavior_profile"] = "v88_current"
            gs["version_mode_v88_migrated"] = True
        if not bool(gs.get("version_mode_v90_migrated", False)):
            if str(gs.get("bot_behavior_profile") or "") == "v88_current":
                gs["bot_behavior_profile"] = "v90_current"
            gs["version_mode_v90_migrated"] = True
        if not bool(gs.get("version_mode_v91_migrated", False)):
            if str(gs.get("bot_behavior_profile") or "") == "v90_current":
                gs["bot_behavior_profile"] = "v91_current"
            gs["version_mode_v91_migrated"] = True
        if not bool(gs.get("version_mode_v92_migrated", False)):
            if str(gs.get("bot_behavior_profile") or "") == "v91_current":
                gs["bot_behavior_profile"] = "v92_current"
            gs["version_mode_v92_migrated"] = True
        # Одноразово очищаем сохранённые имена статей от @username бота.
        if not bool(gs.get("category_names_clean_v88_applied", False)):
            for _cid, _store in (data.get("chats", {}) or {}).items():
                if not isinstance(_store, dict):
                    continue
                _custom_category_list(_store)
                _base_category_items(_store)
            gs["category_names_clean_v88_applied"] = True
    except Exception as e:
        log_error(f"v88 defaults migration: {e}")
    try:
        marker_report = audit_window_marker_registry()
        log_info(f"Маркеры окон проверены: {marker_report}")
    except Exception as e:
        log_error(f"audit_window_marker_registry: {e}")
    try:
        threading.Thread(target=_usd_rate_refresh_loop, name="usd-rate-refresh", daemon=True).start()
    except Exception as e:
        log_error(f"usd rate refresh start: {e}")
    for cid in list((data.get("chats", {}) or {}).keys()):
        try:
            store = get_chat_store(int(cid))
            settings = store.setdefault("settings", {})
            settings.setdefault("quick_balance_enabled", False)
            settings.setdefault("quick_balance_behavior", "normal")
            settings.setdefault("quick_balance_user_selected", False)
            settings.setdefault("hidden_finance", False)
            settings.setdefault("auto_backup_enabled", True)
            settings.setdefault("auto_backup_to_mega_enabled", True)
            settings.setdefault("journal_enabled", False)
            settings.setdefault("main_article_buttons_enabled", False)
            settings.setdefault("main_financial_value_buttons_enabled", False)
            settings.setdefault("currency_mode", "ars_usd" if settings.get("usd_display_enabled", False) else "ars")
            settings.setdefault("remaining_show_ost_label", True)
            settings.setdefault("total_secret_mode", False)
            store.setdefault("secret_messages", [])
            _ensure_secret_media_numbers(int(cid))
        except Exception:
            pass
    # После MEGA restore повторно поднимаем индекс пересылки в память.
    # Это позволяет редактировать старые сообщения сразу после деплоя.
    _restore_runtime_state_from_data(data)
    if not RESTORE_GUARD_ACTIVE:
        save_data(data)
        data["forward_rules"] = load_forward_rules()
    else:
        # Аварийный режим: не создаём/не сохраняем новую пустую SQLite и не запускаем startup-backup.
        data.setdefault("forward_rules", {})
        log_error("v91 emergency mode: local writes and all automatic backups remain blocked")
    # v90: запуск/деплой не планирует бэкап; baseline delta создаётся без загрузки в MEGA.
    # Бэкап ставится только после реального изменения данных.
    try:
        initialize_delta_baseline(data)
    except Exception as e:
        log_error(f"initialize_delta_baseline: {e}")
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
                    f"{'🚨' if RESTORE_GUARD_ACTIVE else '✅'} {version_animal_badge()} Бот запущен (версия {VERSION}).\n"
                    f"Восстановление: {'OK — полный универсальный снимок' if restored else ('ОШИБКА — защитный режим' if RESTORE_GUARD_ACTIVE else 'локальная база сохранена')}\n"
                    f"Защита бэкапа: {'ВКЛ — ' + RESTORE_GUARD_REASON if RESTORE_GUARD_ACTIVE else 'норма'}\n"
                    f"Индекс старых сообщений: {len(data.get('forward_index', {}) or {})}\n"
                    f"Активная версия: {active_bot_behavior_profile_info().get('title')}\n"
                    f"Журнал: {'ВКЛ' if is_journal_registration_enabled() else 'ВЫКЛ'}; keep-alive: {'ВКЛ' if KEEP_ALIVE_ENABLED else 'ВЫКЛ'}\n"
                    f"Бэкап v91: delta {MEGA_DELTA_PRIORITY_DELAY_SECONDS if mega_backup_priority_enabled() else MEGA_DELTA_DELAY_SECONDS:g}с; full после {int(MEGA_GLOBAL_QUIET_SECONDS)}с тишины / максимум {int(MEGA_GLOBAL_MAX_INTERVAL_SECONDS)}с"
                )
            except Exception as e:
                log_error(f"notify owner on start: {e}")
    if not RESTORE_GUARD_ACTIVE:
        schedule_startup_main_windows(delay=3.0)
    app.run(host="0.0.0.0", port=PORT, threaded=True, use_reloader=False)
if __name__ == "__main__":
    main()
# bot_v97_usd_transactions_forward_edit
