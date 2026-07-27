# v130_modular_split
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
import gzip
import subprocess
import shutil
import tempfile
import calendar
import hashlib
import queue
import heapq
import signal
import socket
import sys
import platform

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests
import telebot
from telebot import types
from telebot.types import InputMediaDocument, InputMediaPhoto, InputMediaVideo, InputMediaAudio, InputMediaAnimation

from flask import Flask, request


from collections import defaultdict, deque
from contextlib import contextmanager
from pathlib import Path

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

    def submit_unique(self, key, func, *args, **kwargs) -> bool:
        """Submit only when this logical key has no active/queued task.

        Used for heavy interactive file exports: repeated button presses must coalesce
        instead of building a long queue of the same ZIP/XLSX/journal job. Existing
        submit() semantics remain unchanged for finance/forward/business queues.
        """
        key = str(key)
        with self._lock:
            if key in self._active_keys or bool(self._by_key.get(key)):
                return False
            if self._pending >= self.max_pending:
                self._rejected += 1
                return False
            self._by_key[key].append((func, args, kwargs, time.time()))
            self._pending += 1
            self._submitted += 1
            self._active_keys.add(key)
            self._ready.put(key)
        return True

    def key_status(self, key) -> dict:
        """Small introspection helper for UI status; no queue mutation."""
        key = str(key)
        with self._lock:
            q = self._by_key.get(key)
            return {
                "active": key in self._active_keys,
                "queued": len(q) if q else 0,
            }

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

    def wait_key_idle(self, key, timeout: float = 15.0) -> bool:
        """Wait until all already-submitted tasks for one logical key finish.

        Used only by the durable MEGA witness before it declares a content update complete.
        It does not create new workers and does not affect unrelated chat keys.
        """
        key = str(key)
        deadline = time.monotonic() + max(0.0, float(timeout))
        while True:
            with self._lock:
                active = key in self._active_keys
                queued = bool(self._by_key.get(key))
            if not active and not queued:
                return True
            if time.monotonic() >= deadline:
                return False
            time.sleep(0.02)

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
    _env_int("WEBHOOK_WORKERS", 3, 2, 16),
    _env_int("WEBHOOK_MAX_PENDING", 2000, 100, 10000),
)
FINANCE_TASK_POOL = KeyedTaskPool(
    "finance",
    _env_int("FINANCE_WORKERS", 2, 2, 12),
    _env_int("FINANCE_MAX_PENDING", 1000, 100, 5000),
)
FORWARD_TASK_POOL = KeyedTaskPool(
    "forward",
    _env_int("FORWARD_WORKERS", 2, 2, 12),
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
    _env_int("GENERAL_WORKERS", 1, 1, 6),
    _env_int("GENERAL_MAX_PENDING", 500, 50, 2000),
)
# v117: slow cosmetic retro-updates must never block business callbacks/general work.
# One low-priority worker is intentionally isolated from finance/forward/webhook/export.
MAINTENANCE_TASK_POOL = KeyedTaskPool(
    "maintenance",
    _env_int("MAINTENANCE_WORKERS", 1, 1, 1),
    _env_int("MAINTENANCE_MAX_PENDING", 100, 10, 500),
)
JOURNAL_TASK_POOL = KeyedTaskPool(
    "journal",
    _env_int("JOURNAL_WORKERS", 1, 1, 2),
    _env_int("JOURNAL_MAX_PENDING", 3000, 500, 10000),
)
DELAYED_TASK_POOL = KeyedTaskPool(
    "delayed",
    _env_int("DELAYED_WORKERS", 1, 1, 6),
    _env_int("DELAYED_MAX_PENDING", 1000, 100, 5000),
)
DOZVON_TASK_POOL = KeyedTaskPool(
    "dozvon",
    _env_int("DOZVON_WORKERS", 1, 1, 2),
    _env_int("DOZVON_MAX_PENDING", 100, 10, 500),
)
DELAYED_SCHEDULER = DelayedTaskScheduler(DELAYED_TASK_POOL)


# ─────────────────────────────────────────────────────────────
# v104: Диспетчер-свидетель входящих Telegram update
# ─────────────────────────────────────────────────────────────
# Важно для Render: pending-задачи НЕ считаются сохранёнными в локальной SQLite.
# Надёжность здесь обеспечивает сам Telegram: webhook получает 2xx только после того,
# как update реально прошёл через обработчик. Если задача ещё ждёт/процесс перезапустился,
# возвращаем 503 и Telegram повторит update. Это не требует постоянной локальной очереди.
try:
    WEBHOOK_ACK_WAIT_SECONDS = max(2.0, min(25.0, float(os.getenv("WEBHOOK_ACK_WAIT_SECONDS", "8") or "8")))
except Exception:
    WEBHOOK_ACK_WAIT_SECONDS = 8.0
try:
    WEBHOOK_STUCK_WARN_SECONDS = max(5.0, min(300.0, float(os.getenv("WEBHOOK_STUCK_WARN_SECONDS", "20") or "20")))
except Exception:
    WEBHOOK_STUCK_WARN_SECONDS = 20.0
try:
    WEBHOOK_DONE_TTL_SECONDS = max(60.0, min(3600.0, float(os.getenv("WEBHOOK_DONE_TTL_SECONDS", "600") or "600")))
except Exception:
    WEBHOOK_DONE_TTL_SECONDS = 600.0


class DurableUpdateDispatcher:
    """
    Независимый наблюдатель за входящими update.

    Он не выполняет бизнес-логику и не нарушает порядок операций одного чата.
    Его задача — не позволить webhook молча подтвердить Telegram update, который
    ещё только лежит в RAM-очереди. При timeout Telegram остаётся внешней
    долговечной очередью и повторяет update после рестарта/deploy.
    """
    def __init__(self):
        self._lock = threading.RLock()
        self._tickets = {}
        self._received = 0
        self._duplicates = 0
        self._completed = 0
        self._failed = 0
        self._timeouts = 0
        self._retries = 0
        self._last_error = ""
        self._last_warn = {}
        threading.Thread(target=self._watchdog, name="update-dispatcher-watchdog", daemon=True).start()

    def claim(self, update_id, chat_id=None, update_type="other"):
        key = str(update_id)
        now = time.time()
        with self._lock:
            self._received += 1
            item = self._tickets.get(key)
            if item:
                state = item.get("state")
                if state == "done":
                    self._duplicates += 1
                    return "done", item
                if state in {"queued", "running"}:
                    self._duplicates += 1
                    return "pending", item
                # failed update разрешаем Telegram повторить как новую попытку
                self._retries += 1
                attempts = int(item.get("attempts", 1)) + 1
            else:
                attempts = 1
            event = threading.Event()
            item = {
                "update_id": key,
                "chat_id": chat_id,
                "type": str(update_type or "other"),
                "state": "queued",
                "created_at": now,
                "started_at": None,
                "finished_at": None,
                "attempts": attempts,
                "event": event,
                "error": "",
            }
            self._tickets[key] = item
            return "new", item

    def mark_started(self, update_id):
        with self._lock:
            item = self._tickets.get(str(update_id))
            if item:
                item["state"] = "running"
                item["started_at"] = time.time()

    def finish(self, update_id, success=True, error=""):
        with self._lock:
            item = self._tickets.get(str(update_id))
            if not item:
                return
            item["state"] = "done" if success else "failed"
            item["finished_at"] = time.time()
            item["error"] = str(error or "")[:500]
            if success:
                self._completed += 1
            else:
                self._failed += 1
                self._last_error = item["error"]
            event = item.get("event")
        if event:
            event.set()

    def release_failed_enqueue(self, update_id, error="queue_full"):
        self.finish(update_id, False, error)

    def wait_result(self, item, timeout):
        event = item.get("event")
        if event and not event.wait(max(0.1, float(timeout))):
            with self._lock:
                self._timeouts += 1
            return "timeout", ""
        with self._lock:
            state = str(item.get("state") or "")
            return state, str(item.get("error") or "")

    def stats(self):
        now = time.time()
        with self._lock:
            pending = [x for x in self._tickets.values() if x.get("state") in {"queued", "running"}]
            oldest = max([now - float(x.get("created_at", now)) for x in pending] or [0.0])
            return {
                "pending": len(pending),
                "oldest": round(oldest, 2),
                "received": self._received,
                "duplicates": self._duplicates,
                "completed": self._completed,
                "failed": self._failed,
                "timeouts": self._timeouts,
                "retries": self._retries,
                "last_error": self._last_error,
                "ack_wait": WEBHOOK_ACK_WAIT_SECONDS,
            }

    def _watchdog(self):
        while True:
            try:
                time.sleep(2.0)
                now = time.time()
                stale_keys = []
                warnings = []
                with self._lock:
                    for key, item in list(self._tickets.items()):
                        state = item.get("state")
                        age = now - float(item.get("created_at", now))
                        if state in {"done", "failed"}:
                            finished = float(item.get("finished_at") or item.get("created_at") or now)
                            if now - finished > WEBHOOK_DONE_TTL_SECONDS:
                                stale_keys.append(key)
                            continue
                        if age >= WEBHOOK_STUCK_WARN_SECONDS:
                            last = float(self._last_warn.get(key, 0) or 0)
                            if now - last >= WEBHOOK_STUCK_WARN_SECONDS:
                                self._last_warn[key] = now
                                warnings.append((key, item.get("chat_id"), item.get("type"), age, state))
                    for key in stale_keys:
                        self._tickets.pop(key, None)
                        self._last_warn.pop(key, None)
                for key, chat_id, typ, age, state in warnings:
                    try:
                        log_error(f"DISPATCHER STUCK: update={key} chat={chat_id} type={typ} state={state} age={age:.1f}s; Telegram will retry until 2xx")
                    except Exception:
                        pass
            except Exception:
                time.sleep(2.0)


UPDATE_DISPATCHER = DurableUpdateDispatcher()


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


try:
    FORWARD_FINANCE_PRIORITY_MAX_WAIT_SECONDS = max(0.0, min(10.0, float(os.getenv("FORWARD_FINANCE_PRIORITY_MAX_WAIT_SECONDS", "2.0") or "2.0")))
except Exception:
    FORWARD_FINANCE_PRIORITY_MAX_WAIT_SECONDS = 2.0


def _wait_for_finance_priority_before_forward(kind: str = "forward") -> float:
    """
    v110: финансовая очередь имеет приоритет над пересылкой.
    Пересылка не блокирует finance-worker: она только коротко уступает CPU, пока
    в FINANCE_TASK_POOL есть pending/active работа. Есть жёсткий потолок ожидания,
    чтобы длинный поток финансов не мог навсегда остановить пересылку.
    """
    started = time.monotonic()
    limit = float(FORWARD_FINANCE_PRIORITY_MAX_WAIT_SECONDS or 0.0)
    if limit <= 0:
        return 0.0
    while True:
        try:
            fs = FINANCE_TASK_POOL.stats()
            busy = int(fs.get("pending", 0) or 0) > 0 or int(fs.get("active", 0) or 0) > 0
        except Exception:
            busy = False
        if not busy:
            break
        elapsed = time.monotonic() - started
        if elapsed >= limit:
            try:
                bot_journal("forward_priority_timeout", None, f"kind={kind} waited={elapsed:.3f}s finance still busy", "WARN")
            except Exception:
                pass
            break
        time.sleep(0.02)
    waited = time.monotonic() - started
    if waited >= 0.05:
        try:
            bot_journal("forward_yielded_to_finance", None, f"kind={kind} waited={waited:.3f}s")
        except Exception:
            pass
    return waited


def _forward_with_finance_priority(source_chat_id: int, msg):
    _wait_for_finance_priority_before_forward("message")
    return forward_any_message(source_chat_id, msg)


def _forward_edit_with_finance_priority(msg):
    _wait_for_finance_priority_before_forward("edit")
    return propagate_edited_to_copies(msg)


def _forward_delete_with_finance_priority(source_chat_id: int, source_msg_id: int):
    _wait_for_finance_priority_before_forward("delete")
    return delete_forward_copies_for_source(source_chat_id, source_msg_id)


def schedule_forward_any_message(source_chat_id: int, msg):
    """Пересылка: порядок по исходному чату сохраняется; finance имеет приоритет.

    v121 keeps an explicit live outcome for the asynchronous worker. This prevents a
    successful handler from becoming MEGA/failed merely because forwarding was skipped
    by design or an album was still waiting for its delayed media-group flush.
    """
    try:
        if getattr(getattr(msg, "from_user", None), "is_bot", False):
            _forward_outcome_skip(source_chat_id, msg, "bot_sender")
            return
        if getattr(msg, "edit_date", None):
            _forward_outcome_skip(source_chat_id, msg, "edited_source")
            return
    except Exception:
        pass
    _durable_note_forward_decision(int(source_chat_id), direct=False)
    try:
        mid = int(getattr(msg, "message_id", 0) or 0)
        if mid:
            _forward_outcome_update(source_chat_id, mid, state="scheduled")
    except Exception:
        pass
    if not FORWARD_TASK_POOL.submit(int(source_chat_id), _forward_with_finance_priority, source_chat_id, msg):
        log_error(f"FORWARD QUEUE FULL, INLINE FALLBACK: {source_chat_id}")
        _forward_with_finance_priority(source_chat_id, msg)


def schedule_propagate_edited_to_copies(msg):
    source_chat_id = int(getattr(getattr(msg, "chat", None), "id", 0) or 0)
    if not FORWARD_TASK_POOL.submit(source_chat_id, _forward_edit_with_finance_priority, msg):
        log_error(f"FORWARD EDIT QUEUE FULL, INLINE FALLBACK: {source_chat_id}")
        _forward_edit_with_finance_priority(msg)


def schedule_delete_forward_copies_for_source(source_chat_id: int, source_msg_id: int):
    if not FORWARD_TASK_POOL.submit(int(source_chat_id), _forward_delete_with_finance_priority, source_chat_id, source_msg_id):
        log_error(f"FORWARD DELETE QUEUE FULL, INLINE FALLBACK: {source_chat_id}")
        _forward_delete_with_finance_priority(source_chat_id, source_msg_id)
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
VERSION = "bot_v130_modular_split"
BOT_FILE_NAME = os.path.basename(__file__) if "__file__" in globals() else "bot_v130_modular_split.py"
BOT_DISPLAY_NAME = os.getenv("BOT_DISPLAY_NAME", "Финансовый бот").strip() or "Финансовый бот"


def _current_source_path() -> str:
    """Single-file path in legacy mode; reconstructed full source in modular mode."""
    helper = globals().get("_modular_merged_source_path")
    if callable(helper):
        try:
            return str(helper())
        except Exception:
            pass
    return os.path.abspath(__file__)


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
# v105: отдельный облачный журнал критических входящих задач.
# Он НЕ заменяет Telegram webhook и НЕ хранится только в локальной SQLite Render.
MEGA_TASKS_ENABLED = _env_bool("MEGA_TASKS_ENABLED", "1")
MEGA_TASK_BACKUP_DIR = os.getenv("MEGA_TASK_BACKUP_DIR", "tasks").strip().strip("/") or "tasks"
try:
    MEGA_TASK_DONE_KEEP = max(20, min(1000, int(os.getenv("MEGA_TASK_DONE_KEEP", "200") or "200")))
except Exception:
    MEGA_TASK_DONE_KEEP = 200
try:
    MEGA_TASK_RECOVERY_LIMIT = max(20, min(2000, int(os.getenv("MEGA_TASK_RECOVERY_LIMIT", "500") or "500")))
except Exception:
    MEGA_TASK_RECOVERY_LIMIT = 500
try:
    MEGA_TASK_RECOVERY_DELAY_SECONDS = max(0.5, min(30.0, float(os.getenv("MEGA_TASK_RECOVERY_DELAY_SECONDS", "2") or "2")))
except Exception:
    MEGA_TASK_RECOVERY_DELAY_SECONDS = 2.0
try:
    MEGA_TASK_FINALIZE_RETRIES = max(1, min(5, int(os.getenv("MEGA_TASK_FINALIZE_RETRIES", "3") or "3")))
except Exception:
    MEGA_TASK_FINALIZE_RETRIES = 3
try:
    MEGA_TASK_PROCESSED_KEEP = max(100, min(5000, int(os.getenv("MEGA_TASK_PROCESSED_KEEP", "500") or "500")))
except Exception:
    MEGA_TASK_PROCESSED_KEEP = 500
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
try:
    MEGA_RESTORE_DISCOVERY_RETRIES = max(1, min(6, int(os.getenv("MEGA_RESTORE_DISCOVERY_RETRIES", "3") or "3")))
except Exception:
    MEGA_RESTORE_DISCOVERY_RETRIES = 3
try:
    MEGA_RESTORE_DISCOVERY_RETRY_SECONDS = max(0.5, float(os.getenv("MEGA_RESTORE_DISCOVERY_RETRY_SECONDS", "3") or "3"))
except Exception:
    MEGA_RESTORE_DISCOVERY_RETRY_SECONDS = 3.0
RESTORE_GUARD_ACTIVE = False
RESTORE_GUARD_REASON = ""
MEGA_GLOBAL_BACKUP_LOCK = threading.RLock()
MEGA_COMMAND_LOCK = threading.RLock()
CRITICAL_DELTA_LOCK = threading.RLock()
forward_map = {}
backup_flags = {
    "channel": True,
}
restore_mode = None
_media_group_cache = {}
_media_group_timers = {}
FORWARD_MEDIA_GROUP_DELAY = 0.8

# v121: live exact-once witness for asynchronous forwarding.
# Durable tasks are created before the forward worker runs; therefore a completed handler
# must distinguish "not forwarded by design", "still pending", "delivered", and "failed".
_FORWARD_OUTCOME_LOCK = threading.RLock()
_FORWARD_OUTCOMES = {}
_FORWARD_OUTCOME_MAX = 2500

def _forward_outcome_key(source_chat_id: int, source_msg_id: int):
    return (int(source_chat_id), int(source_msg_id))

def _forward_outcome_prune_locked():
    if len(_FORWARD_OUTCOMES) <= _FORWARD_OUTCOME_MAX:
        return
    ordered = sorted(
        _FORWARD_OUTCOMES.items(),
        key=lambda kv: float((kv[1] or {}).get("updated_at", 0.0) or 0.0),
    )
    for key, _item in ordered[: max(1, len(ordered) - _FORWARD_OUTCOME_MAX)]:
        _FORWARD_OUTCOMES.pop(key, None)

def _forward_outcome_update(source_chat_id: int, source_msg_id: int, state: str | None = None, dst_chat_id: int | None = None, dst_state: str | None = None, dst_msg_id: int | None = None, error: str = ""):
    try:
        key = _forward_outcome_key(source_chat_id, source_msg_id)
        with _FORWARD_OUTCOME_LOCK:
            item = _FORWARD_OUTCOMES.setdefault(key, {"state": "", "targets": {}, "updated_at": time.time()})
            if state:
                item["state"] = str(state)
            if dst_chat_id is not None:
                dst = int(dst_chat_id)
                target = item.setdefault("targets", {}).setdefault(dst, {})
                if dst_state:
                    target["state"] = str(dst_state)
                if dst_msg_id:
                    target["dst_msg_id"] = int(dst_msg_id)
                if error:
                    target["error"] = str(error)[:500]
            item["updated_at"] = time.time()
            _forward_outcome_prune_locked()
    except Exception:
        pass

def _forward_outcome_snapshot(source_chat_id: int, source_msg_id: int) -> dict:
    try:
        key = _forward_outcome_key(source_chat_id, source_msg_id)
        with _FORWARD_OUTCOME_LOCK:
            return copy.deepcopy(_FORWARD_OUTCOMES.get(key) or {})
    except Exception:
        return {}

def _forward_outcome_skip(source_chat_id: int, msg, reason: str):
    try:
        mid = int(getattr(msg, "message_id", 0) or 0)
        if mid:
            _forward_outcome_update(source_chat_id, mid, state=f"skip:{reason}")
            bot_journal("forward_not_expected", source_chat_id, f"msg={mid} reason={reason}")
    except Exception:
        pass
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
bot = telebot.TeleBot(BOT_TOKEN, parse_mode=None, threaded=False)  # v104: handlers run inside our durable dispatcher/task pools
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
            cur.execute("PRAGMA temp_store=FILE")
            cur.execute("PRAGMA cache_size=-4096")  # ~4 MiB page cache; history belongs on disk, not Python/SQLite RAM
            cur.execute("PRAGMA mmap_size=0")
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
            # v114 LOW-RAM: large per-chat history is stored on disk and loaded only on demand.
            cur.execute(
                "CREATE TABLE IF NOT EXISTS cold_fields (chat_id TEXT NOT NULL, k TEXT NOT NULL, v TEXT NOT NULL, updated_at TEXT NOT NULL DEFAULT '', PRIMARY KEY(chat_id, k))"
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

    # v114 LOW-RAM cold storage -------------------------------------------------
    def get_cold(self, chat_id, key: str, default=None):
        with self.lock:
            row = self.conn.execute(
                "SELECT v FROM cold_fields WHERE chat_id=? AND k=?",
                (str(chat_id), str(key)),
            ).fetchone()
        return self._load(row[0], default) if row else default

    def set_cold(self, chat_id, key: str, obj):
        payload = self._dump(obj)
        stamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
        with self.lock:
            self.conn.execute(
                "INSERT INTO cold_fields(chat_id,k,v,updated_at) VALUES(?,?,?,?) "
                "ON CONFLICT(chat_id,k) DO UPDATE SET v=excluded.v,updated_at=excluded.updated_at",
                (str(chat_id), str(key), payload, stamp),
            )
            self.conn.commit()

    def delete_cold(self, chat_id, key: str):
        with self.lock:
            self.conn.execute("DELETE FROM cold_fields WHERE chat_id=? AND k=?", (str(chat_id), str(key)))
            self.conn.commit()

    def cold_count(self, chat_id=None, key: str | None = None) -> int:
        sql = "SELECT COUNT(*) FROM cold_fields"
        params = []
        where = []
        if chat_id is not None:
            where.append("chat_id=?"); params.append(str(chat_id))
        if key is not None:
            where.append("k=?"); params.append(str(key))
        if where:
            sql += " WHERE " + " AND ".join(where)
        with self.lock:
            row = self.conn.execute(sql, tuple(params)).fetchone()
        return int(row[0] or 0) if row else 0

    def cold_keys_for_chat(self, chat_id) -> list[str]:
        with self.lock:
            rows = self.conn.execute("SELECT k FROM cold_fields WHERE chat_id=?", (str(chat_id),)).fetchall()
        return [str(r[0]) for r in rows]

    def backup_to(self, target_path: str):
        """Consistent on-disk SQLite snapshot without materializing bot state in Python RAM."""
        target_path = str(target_path)
        os.makedirs(os.path.dirname(target_path) or ".", exist_ok=True)
        with self.lock:
            try:
                self.conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
            except Exception:
                pass
            dest = sqlite3.connect(target_path)
            try:
                self.conn.backup(dest, pages=128, sleep=0.01)
                dest.commit()
            finally:
                dest.close()
        return target_path

    def replace_database(self, source_path: str):
        """Replace ephemeral working DB with a restored MEGA snapshot and reopen connection."""
        source_path = str(source_path)
        if not os.path.exists(source_path):
            raise FileNotFoundError(source_path)
        with self.lock:
            try:
                self.conn.close()
            except Exception:
                pass
            for suffix in ("", "-wal", "-shm"):
                try:
                    if os.path.exists(self.path + suffix):
                        os.remove(self.path + suffix)
                except Exception:
                    pass
            shutil.copy2(source_path, self.path)
            self.conn = sqlite3.connect(self.path, check_same_thread=False)
            self.conn.row_factory = sqlite3.Row
            self._init_db()


SQLITE = SQLiteState(DB_FILE)


# ─────────────────────────────────────────────────────────────
# v114 LOW-RAM CORE
# RAM = active working set; SQLite = disposable working state; MEGA = durable state.
# Large chat histories are lazy-loaded from SQLite and released after each Telegram update.
# ─────────────────────────────────────────────────────────────
LOWRAM_ENABLED = _env_bool("LOWRAM_ENABLED", "1")
LOWRAM_COLD_KEYS = {
    "records", "daily_records", "daily_records_by_date",
    "ars_records", "ars_daily_records", "ars_daily_records_by_date",
    "usd_records", "usd_daily_records", "usd_daily_records_by_date",
    "secret_messages",
}
LOWRAM_LIST_KEYS = {"records", "ars_records", "usd_records", "secret_messages"}
LOWRAM_DB_REMOTE_DIR_NAME = "database"
LOWRAM_DB_LATEST_NAME = "latest_bot_state.sqlite3.gz"
LOWRAM_DB_HISTORY_KEEP = max(2, min(30, int(os.getenv("LOWRAM_DB_HISTORY_KEEP", "6") or "6")))
LOWRAM_LEGACY_GLOBAL_JSON = _env_bool("LOWRAM_LEGACY_GLOBAL_JSON", "0")
_LOWRAM_DB_RESTORED_THIS_BOOT = False
_LOWRAM_DB_RESTORE_DETAIL = ""
_LOWRAM_LOCK = threading.RLock()
_LOWRAM_STATS = {
    "cold_loads": 0, "cold_saves": 0, "cold_evictions": 0,
    "db_snapshots": 0, "db_snapshot_errors": 0, "db_restores": 0,
    "last_snapshot_at": "", "last_restore_at": "", "last_error": "",
}

def _lowram_default_for_key(key: str):
    return [] if str(key) in LOWRAM_LIST_KEYS else {}

def _lowram_touch(chat_id: int, key: str):
    # Lightweight diagnostic only; no growing per-message cache.
    try:
        _LOWRAM_STATS["last_access"] = now_local().isoformat(timespec="seconds")
        _LOWRAM_STATS["last_chat"] = int(chat_id)
        _LOWRAM_STATS["last_key"] = str(key)
    except Exception:
        pass

class ColdChatStore(dict):
    """dict-compatible chat state with lazy large fields backed by SQLite."""
    def __init__(self, chat_id: int, initial=None):
        super().__init__(initial or {})
        self._chat_id = int(chat_id)
        self._cold_loaded = {k for k in LOWRAM_COLD_KEYS if dict.__contains__(self, k)}

    def _ensure_cold(self, key: str):
        key = str(key)
        if not LOWRAM_ENABLED or key not in LOWRAM_COLD_KEYS:
            return
        if dict.__contains__(self, key):
            _lowram_touch(self._chat_id, key)
            return
        value = SQLITE.get_cold(self._chat_id, key, _lowram_default_for_key(key))
        if value is None:
            value = _lowram_default_for_key(key)
        dict.__setitem__(self, key, value)
        self._cold_loaded.add(key)
        with _LOWRAM_LOCK:
            _LOWRAM_STATS["cold_loads"] += 1
        _lowram_touch(self._chat_id, key)

    def __getitem__(self, key):
        self._ensure_cold(key)
        return dict.__getitem__(self, key)

    def get(self, key, default=None):
        self._ensure_cold(key)
        if dict.__contains__(self, key):
            return dict.get(self, key)
        return default

    def setdefault(self, key, default=None):
        self._ensure_cold(key)
        if dict.__contains__(self, key):
            return dict.__getitem__(self, key)
        if default is None and str(key) in LOWRAM_COLD_KEYS:
            default = _lowram_default_for_key(str(key))
        dict.__setitem__(self, key, default)
        if str(key) in LOWRAM_COLD_KEYS:
            self._cold_loaded.add(str(key)); _lowram_touch(self._chat_id, str(key))
        return default

    def __setitem__(self, key, value):
        dict.__setitem__(self, key, value)
        if str(key) in LOWRAM_COLD_KEYS:
            self._cold_loaded.add(str(key)); _lowram_touch(self._chat_id, str(key))

def _lowram_wrap_store(chat_id, store):
    if isinstance(store, ColdChatStore):
        return store
    return ColdChatStore(int(chat_id), store if isinstance(store, dict) else {})

def _lowram_store_meta_payload(store: dict) -> dict:
    # dict.items() intentionally avoids triggering ColdChatStore lazy loads.
    return {str(k): v for k, v in dict.items(store) if str(k) not in LOWRAM_COLD_KEYS}

def _lowram_rebuild_daily(records):
    daily = {}
    for rec in records or []:
        if not isinstance(rec, dict):
            continue
        try:
            dk = _record_day_key(rec) if "_record_day_key" in globals() else str(rec.get("day_key") or "")
        except Exception:
            dk = str(rec.get("day_key") or "")
        if dk:
            rec["day_key"] = dk
            daily.setdefault(str(dk), []).append(rec)
    return daily

def _lowram_flush_chat(chat_id: int, store: dict | None = None, evict: bool = False):
    if not LOWRAM_ENABLED:
        return
    try:
        cid = int(chat_id)
    except Exception:
        return
    store = store if isinstance(store, dict) else ((data.get("chats", {}) or {}).get(str(cid)) if isinstance(data, dict) else None)
    if not isinstance(store, dict):
        return
    # Keep records/daily consistent without loading a field that was never touched.
    for rec_key, daily_key in (("records","daily_records"),("ars_records","ars_daily_records"),("usd_records","usd_daily_records")):
        if dict.__contains__(store, rec_key):
            records = dict.__getitem__(store, rec_key) or []
            daily = _lowram_rebuild_daily(records)
            dict.__setitem__(store, daily_key, daily)
            if isinstance(store, ColdChatStore): store._cold_loaded.add(daily_key)
    for key in LOWRAM_COLD_KEYS:
        if dict.__contains__(store, key):
            SQLITE.set_cold(cid, key, dict.__getitem__(store, key))
            with _LOWRAM_LOCK:
                _LOWRAM_STATS["cold_saves"] += 1
    if evict:
        removed = 0
        for key in list(LOWRAM_COLD_KEYS):
            if dict.__contains__(store, key):
                dict.pop(store, key, None); removed += 1
        if isinstance(store, ColdChatStore):
            store._cold_loaded.clear()
        if removed:
            with _LOWRAM_LOCK:
                _LOWRAM_STATS["cold_evictions"] += 1

def _lowram_memory_snapshot() -> dict:
    """Small, dependency-safe RAM snapshot used by LOW-RAM cleanup.

    The runtime watcher helper is defined later in the file, so resolve it dynamically.
    This avoids the v114/v115 NameError that disabled post-update GC and flooded the journal.
    """
    for name in ("_runtime_memory_stats", "_memory_usage_snapshot"):
        fn = globals().get(name)
        if callable(fn):
            try:
                snap = fn()
                if isinstance(snap, dict):
                    return snap
            except Exception:
                pass
    return {}


def _lowram_release_chat(chat_id):
    """Called only after the update/finalizer finished, so temporary history can leave RAM."""
    if not LOWRAM_ENABLED or chat_id is None:
        return
    try:
        with data_lock:
            store = (data.get("chats", {}) or {}).get(str(int(chat_id)))
            if isinstance(store, dict):
                _lowram_flush_chat(int(chat_id), store, evict=True)
                SQLITE.save_chat(int(chat_id), _lowram_store_meta_payload(store))
        # Prompt Python to return unreachable JSON/list objects before the next heavy MEGA operation.
        if _lowram_memory_snapshot().get("rss_mb", 0) >= 320:
            import gc; gc.collect()
    except Exception as exc:
        with _LOWRAM_LOCK:
            _LOWRAM_STATS["last_error"] = str(exc)[:300]
        log_error(f"LOWRAM release chat={chat_id}: {exc}")

def _lowram_prepare_loaded_data(d: dict, migrate_existing: bool = True) -> dict:
    if not LOWRAM_ENABLED or not isinstance(d, dict):
        return d
    chats = d.setdefault("chats", {})
    for cid_s, raw in list(chats.items()):
        try:
            cid = int(cid_s)
        except Exception:
            continue
        raw = raw if isinstance(raw, dict) else {}
        # One-time migration from legacy JSON/chat blobs to disk cold fields.
        if migrate_existing:
            for key in LOWRAM_COLD_KEYS:
                if key in raw:
                    SQLITE.set_cold(cid, key, raw.get(key))
                    raw.pop(key, None)
        chats[str(cid)] = _lowram_wrap_store(cid, raw)
    return d

def _lowram_materialize_chat_snapshot(chat_id: int, store: dict | None = None) -> dict:
    """Plain JSON-ready chat snapshot. Loads only one chat's cold fields at a time."""
    cid = int(chat_id)
    store = store if isinstance(store, dict) else ((data.get("chats", {}) or {}).get(str(cid)) or {})
    snap = _lowram_store_meta_payload(store)
    for key in LOWRAM_COLD_KEYS:
        if dict.__contains__(store, key):
            value = dict.__getitem__(store, key)
        else:
            value = SQLITE.get_cold(cid, key, _lowram_default_for_key(key))
        if value not in (None, [], {}):
            snap[key] = value
    return snap

def _lowram_flush_all_hot(evict: bool = False):
    if not LOWRAM_ENABLED or not isinstance(data, dict):
        return
    chats = data.get("chats", {}) or {}
    with data_lock:
        meta_chats = {}
        for cid_s, store in list(chats.items()):
            try: cid = int(cid_s)
            except Exception: continue
            if isinstance(store, dict):
                _lowram_flush_chat(cid, store, evict=evict)
                meta_chats[str(cid)] = _lowram_store_meta_payload(store)
        if meta_chats:
            SQLITE.save_chats(meta_chats)
        SQLITE.save_root(_sqlite_pack_root(data))

def lowram_status_text() -> str:
    mem = _lowram_memory_snapshot()
    with _LOWRAM_LOCK:
        st = dict(_LOWRAM_STATS)
    loaded = 0
    try:
        for store in ((data or {}).get("chats", {}) or {}).values():
            if isinstance(store, dict):
                loaded += sum(1 for k in LOWRAM_COLD_KEYS if dict.__contains__(store, k))
    except Exception:
        pass
    return (
        f"LOW-RAM: {'ВКЛ' if LOWRAM_ENABLED else 'ВЫКЛ'} | RAM {mem.get('rss_mb','?')} MB\n"
        f"Cold fields loaded now: {loaded}; loads={st.get('cold_loads',0)} saves={st.get('cold_saves',0)} evictions={st.get('cold_evictions',0)}\n"
        f"SQLite cold rows: {SQLITE.cold_count()} | DB snapshots={st.get('db_snapshots',0)} restores={st.get('db_restores',0)}\n"
        f"Последний DB snapshot: {st.get('last_snapshot_at') or '—'}; restore: {st.get('last_restore_at') or '—'}\n"
        f"Ошибка: {st.get('last_error') or 'нет'}"
    )

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
BOT_JOURNAL_MAX = int(os.getenv("BOT_JOURNAL_MAX", "400") or "400")
BOT_JOURNAL_FILE = os.getenv("BOT_JOURNAL_FILE", "bot_journal.jsonl").strip() or "bot_journal.jsonl"
BOT_ACTION_LOG = deque(maxlen=BOT_JOURNAL_MAX)
bot_journal_lock = threading.RLock()

# v111: локальный journal на Render ephemeral, поэтому сохраняем его append-only чанками в MEGA.
# Это НЕ синхронная запись на каждую строку: обычная работа бота не должна ждать сеть.
BOT_JOURNAL_DURABLE_ENABLED = str(os.getenv("BOT_JOURNAL_DURABLE_ENABLED", "1") or "1").strip().lower() not in {"0", "false", "no", "off"}
try:
    BOT_JOURNAL_DURABLE_FLUSH_SECONDS = max(10.0, min(300.0, float(os.getenv("BOT_JOURNAL_DURABLE_FLUSH_SECONDS", "30") or "30")))
except Exception:
    BOT_JOURNAL_DURABLE_FLUSH_SECONDS = 30.0
try:
    BOT_JOURNAL_DURABLE_FLUSH_ROWS = max(10, min(500, int(os.getenv("BOT_JOURNAL_DURABLE_FLUSH_ROWS", "50") or "50")))
except Exception:
    BOT_JOURNAL_DURABLE_FLUSH_ROWS = 50
try:
    BOT_JOURNAL_DURABLE_REMOTE_KEEP = max(200, min(10000, int(os.getenv("BOT_JOURNAL_DURABLE_REMOTE_KEEP", "3000") or "3000")))
except Exception:
    BOT_JOURNAL_DURABLE_REMOTE_KEEP = 3000
try:
    BOT_JOURNAL_DURABLE_RESTORE_FILES = max(10, min(200, int(os.getenv("BOT_JOURNAL_DURABLE_RESTORE_FILES", "80") or "80")))
except Exception:
    BOT_JOURNAL_DURABLE_RESTORE_FILES = 80

_JOURNAL_DURABLE_LOCK = threading.RLock()
_JOURNAL_DURABLE_BUFFER = []
_JOURNAL_DURABLE_SEQ = 0
_JOURNAL_DURABLE_THREAD_STARTED = False
_JOURNAL_DURABLE_STATS = {
    "uploaded_chunks": 0, "uploaded_rows": 0, "upload_errors": 0,
    "restored_chunks": 0, "restored_rows": 0, "last_upload_at": "",
    "last_upload_file": "", "last_error": "",
}


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

# v120: Ф132 показывает все версии этого проекта, которые реально были собраны в чате.
# Для v98+ это совместимые runtime-профили внутри текущего безопасного ядра: выбор
# меняет профиль/настройки интерфейса, но НЕ откатывает SQLite/MEGA схему и exact-once защиту.
def _modern_behavior_profile(title: str, description: str) -> dict:
    cfg = dict(BOT_BEHAVIOR_PROFILES["v97_current"])
    cfg.update({"title": str(title), "description": str(description), "info_layout": "v87"})
    return cfg

_MODERN_BEHAVIOR_PROFILES = {
    "v130_current": _modern_behavior_profile("v130 Modular split / v129 behavior", "Физически разделён на модули без изменения бизнес-логики v129; Google existing Sheet / Notes / stability сохранены."),
    "v129_current": _modern_behavior_profile("v129 Google existing Sheet / Notes / stability", "Google Sheets экспорт пишет в заранее расшаренную таблицу владельца, создавая отдельную вкладку с native Notes; Ф40 защищён от слишком длинных сообщений."),
    "v128_current": _modern_behavior_profile("v128 Google Sheets Notes / Gomonk fix", "Нативные примечания Google Sheets для Excel статей и исправление кнопки Гомонковые во всех современных профилях."),
    "v127_current": _modern_behavior_profile("v127 Excel Notes exact validation", "Исправление ложной проверки expected/actual в Excel статьи и проверка текста каждого Примечания внутри XLSX."),
    "v126_current": _modern_behavior_profile("v126 Кнопки / Excel Примечания", "Аудит callback-кнопок, channel-safe вставка без Telegram 400 и Excel статьи с примечаниями без автора/современных комментариев."),
    "v125_current": _modern_behavior_profile("v125 Быстрый 💰Перес / Excel Примечания", "💰Перес обновляет только свежие копии за 3 дня без длинной очереди; режим Excel глобальный, Примечания отделены от Комментариев."),
    "v124_current": _modern_behavior_profile("v124 Global 💰Перес / версии / файлы", "Глобальный 💰Перес с ретро-обновлением старых копий, постраничный Ф132 и загрузки исходника/журнала в Ф89."),
    "v123_current": _modern_behavior_profile("v123 Edit consistency / 💰Перес safe", "Единая логика редактирования, exact edit witnesses и безопасное завершение 40-секундного окна 💰Перес."),
    "v122_current": _modern_behavior_profile("v122 Excel notes / balances / finance witness", "Примечания Excel, остатки и расширенные доказательства финансового редактирования."),
    "v121_current": _modern_behavior_profile("v121 Forward outcome / Excel exports", "Уточнение результата пересылки и сохранение всех вариантов Excel/экспорта."),
    "v120_current": _modern_behavior_profile("v120 Single-flight exports / forward witness", "Повторные нажатия экспорта не копятся в очереди; видимое время формирования; исправление ложного ambiguous forward для worker-skip."),
    "v119_current": _modern_behavior_profile("v119 Excel / runtime export / exact edit", "Новый Excel с заливками и примечаниями, экспорт runtime из MEGA, исправление ложного source_finance при редактировании."),
    "v118_current": _modern_behavior_profile("v118 Runtime slots / restart forensics", "Rotating runtime slots, корректный watcher_mega_ok и диагностика рестартов Render."),
    "v117_current": _modern_behavior_profile("v117 Secret routes / Telegram maintenance", "Исправление secret-route witness и отдельная throttled maintenance-очередь Telegram edits."),
    "v116_current": _modern_behavior_profile("v116 Stable LOW-RAM / exact effects", "LOW-RAM cleanup, exact-effects forwarding и журнал через отдельный EXPORT pool."),
    "v115_current": _modern_behavior_profile("v115 Stable LOW-RAM core", "Стабилизация LOW-RAM, SQLite snapshots и fallback runtime recovery."),
    "v114_current": _modern_behavior_profile("v114 LOW-RAM SQLite / MEGA core", "Cold history в SQLite и уменьшение RAM без удаления пользовательских функций."),
    "v113_current": _modern_behavior_profile("v113 Memory guard stability", "Контроль памяти Render и аварийная очистка диагностических данных."),
    "v112_current": _modern_behavior_profile("v112 Runtime forensics stability", "Durable runtime heartbeat, exception hooks и исправленный atomic JSON dump."),
    "v111_current": _modern_behavior_profile("v111 Durable journal / Render history", "Append-only журнал действий в MEGA и история между restart/deploy."),
    "v110_current": _modern_behavior_profile("v110 Finance priority / max diagnostics", "FINANCE → FORWARD приоритет и расширенная диагностика задержек."),
    "v109_current": _modern_behavior_profile("v109 Exact-once finance safe recovery", "Operation keys, no blind replay running-задач и защита от дублей финансов."),
    "v108_current": _modern_behavior_profile("v108 BOOT/SHUTDOWN / fin windows", "BOOT/READY/SHUTDOWN watcher и восстановление финансовых окон."),
    "v107_current": _modern_behavior_profile("v107 All forwarding durable", "Durable witness для всех пересылаемых типов контента."),
    "v106_current": _modern_behavior_profile("v106 Deploy-safe all directions", "Deploy-safe пересылка и восстановление направлений без потери сообщений."),
    "v105_current": _modern_behavior_profile("v105 MEGA durable tasks", "Внешние карточки критических Telegram update в MEGA до выполнения."),
    "v104_current": _modern_behavior_profile("v104 Durable dispatcher / timers", "Диспетчер update, bounded queues и устойчивые внутренние таймеры."),
    "v103_current": _modern_behavior_profile("v103 Compact MEGA delta safe", "Компактные delta без раздувания полной истории и safe full snapshot fallback."),
    "v102_current": _modern_behavior_profile("v102 Supergroup migration / forward retry", "Автомиграция group→supergroup и одноразовый безопасный retry пересылки."),
    "v101_current": _modern_behavior_profile("v101 MEGA restore / durable forward finance", "Повторный discovery restore и немедленное durable сохранение финансовой пересылки."),
    "v100_current": _modern_behavior_profile("v100 Factory defaults / file identity", "Заводские настройки, имя файла/версии и улучшения Ф9998."),
    "v99_current": _modern_behavior_profile("v99 Manual MEGA restore menu", "Ручное полное обновление состояния из MEGA через INFO."),
    "v98_current": _modern_behavior_profile("v98 Buttons / Restore guard", "Рабочий /buttons и постоянный ручной override Restore guard."),
}
# Новые версии показываем первыми, затем исторические v97..v81.
BOT_BEHAVIOR_PROFILES = {**_MODERN_BEHAVIOR_PROFILES, **BOT_BEHAVIOR_PROFILES}
DEFAULT_BOT_BEHAVIOR_PROFILE = "v130_current"


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
        "process_trace_enabled", "usd_transactions_view",
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
    # IB() не получает chat_id. Вне state_chat_context используем primary owner scope,
    # чтобы /buttons и кнопка INFO реально меняли подписи создаваемых inline-кнопок.
    if chat_id is None:
        chat_id = current_state_chat_id()
        if chat_id is None and OWNER_ID:
            try:
                chat_id = int(OWNER_ID)
            except Exception:
                chat_id = None
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
        _ws = WEBHOOK_TASK_POOL.stats()
        _fs = FINANCE_TASK_POOL.stats()
        _fws = FORWARD_TASK_POOL.stats()
        _ds = DELTA_TASK_POOL.stats() if "DELTA_TASK_POOL" in globals() else {}
        row = {
            "ts": _journal_ts(),
            "level": str(level or "INFO"),
            "action": str(action or "")[:160],
            "chat_id": str(chat_id) if chat_id is not None else "",
            "chat_name": "",
            "detail": str(detail or "")[:3000],
            "thread": threading.current_thread().name,
            "profile": active_bot_behavior_profile() if "data" in globals() and isinstance(data, dict) else "startup",
            "bot_version": str(globals().get("VERSION") or "startup"),
            "render_commit": str(os.getenv("RENDER_GIT_COMMIT", "") or ""),
            "webhook_pending": _ws.get("pending", 0),
            "webhook_active": _ws.get("active", 0),
            "finance_pending": _fs.get("pending", 0),
            "finance_active": _fs.get("active", 0),
            "forward_pending": _fws.get("pending", 0),
            "forward_active": _fws.get("active", 0),
            "delta_pending": _ds.get("pending", 0),
            "general_pending": GENERAL_TASK_POOL.stats().get("pending", 0),
            "backup_pending": BACKUP_TASK_POOL.stats().get("pending", 0),
            "runtime_phase": str((globals().get("_RUNTIME_STATE") or {}).get("phase") or ""),
            "runtime_ready": bool((globals().get("_RUNTIME_STATE") or {}).get("ready", False)),
        }
        try:
            if chat_id is not None:
                row["chat_name"] = get_chat_display_name(int(chat_id))
        except Exception:
            pass
        with bot_journal_lock:
            BOT_ACTION_LOG.append(row)
        # v111: копия строки уходит в независимый MEGA journal-spool. Сеть здесь НЕ ждём.
        if BOT_JOURNAL_DURABLE_ENABLED:
            try:
                with _JOURNAL_DURABLE_LOCK:
                    _JOURNAL_DURABLE_BUFFER.append(dict(row))
            except Exception:
                pass
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


def _safe_diag_call(name: str, func, default=None):
    try:
        return func()
    except Exception as e:
        return {"error": f"{name}: {e}"} if default is None else default


def _journal_read_file_rows(limit: int = 20000) -> list[dict]:
    """Читает только хвост локального журнала, не загружая весь файл в RAM."""
    if not os.path.exists(BOT_JOURNAL_FILE):
        return []
    rows = []
    try:
        max_rows = max(1, int(limit))
        with open(BOT_JOURNAL_FILE, "r", encoding="utf-8") as f:
            raw = deque(f, maxlen=max_rows)
        for line in raw:
            try:
                item = json.loads(line)
                if isinstance(item, dict):
                    rows.append(item)
            except Exception:
                continue
    except Exception:
        return []
    return rows



def _atomic_json_dump(path: str, payload) -> None:
    """Atomically write JSON on the local ephemeral disk before a MEGA upload.

    v111 referenced this helper before it existed, so both runtime_latest and the
    durable journal failed to persist.  Keep it tiny and dependency-free.
    """
    target = os.path.abspath(str(path))
    parent = os.path.dirname(target) or "."
    os.makedirs(parent, exist_ok=True)
    tmp = f"{target}.tmp.{os.getpid()}.{threading.get_ident()}.{time.time_ns()}"
    try:
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, separators=(",", ":"))
            fh.flush()
            try:
                os.fsync(fh.fileno())
            except Exception:
                pass
        os.replace(tmp, target)
    finally:
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass


def _journal_durable_remote_dir() -> str:
    base = str(globals().get("MEGA_BACKUP_DIR") or "/TelegramBotBackups").rstrip("/")
    return f"{base}/runtime/journal"


def _journal_row_key(row: dict):
    return (
        str(row.get("ts") or ""), str(row.get("action") or ""),
        str(row.get("chat_id") or ""), str(row.get("detail") or ""),
        str(row.get("thread") or ""),
    )


def _journal_merge_rows(*groups, limit: int = 20000) -> list[dict]:
    seen = set()
    out = []
    for group in groups:
        for row in (group or []):
            if not isinstance(row, dict):
                continue
            key = _journal_row_key(row)
            if key in seen:
                continue
            seen.add(key)
            out.append(row)
    # ISO timestamps sort correctly for our journal format. Keep stable order for equal timestamps.
    try:
        out.sort(key=lambda r: str(r.get("ts") or ""))
    except Exception:
        pass
    return out[-max(1, int(limit)):]


def journal_flush_to_mega(force: bool = False) -> bool:
    """Сбрасывает накопленный journal chunk в MEGA. Не перезаписывает старые чанки."""
    global _JOURNAL_DURABLE_SEQ
    if not BOT_JOURNAL_DURABLE_ENABLED:
        return False
    if not globals().get("mega_is_configured") or not mega_is_configured():
        return False
    with _JOURNAL_DURABLE_LOCK:
        if not _JOURNAL_DURABLE_BUFFER:
            return True
        if not force and len(_JOURNAL_DURABLE_BUFFER) < BOT_JOURNAL_DURABLE_FLUSH_ROWS:
            return False
        rows = list(_JOURNAL_DURABLE_BUFFER)
        _JOURNAL_DURABLE_BUFFER.clear()
        _JOURNAL_DURABLE_SEQ += 1
        seq = _JOURNAL_DURABLE_SEQ
    tmp = None
    try:
        remote_dir = _journal_durable_remote_dir()
        mega_ensure_remote_path(remote_dir)
        os.makedirs(MEGA_LOCAL_TMP_DIR, exist_ok=True)
        stamp = now_local().strftime("%Y%m%d_%H%M%S_%f") if "now_local" in globals() else datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        inst = mega_safe_name(str(os.getenv("RENDER_INSTANCE_ID", "local") or "local")[-18:], "instance")
        name = f"journal_{stamp}_{inst}_{seq:06d}.json"
        tmp = os.path.join(MEGA_LOCAL_TMP_DIR, name)
        payload = {
            "kind": "telegram_bot_journal_chunk", "schema_version": 1,
            "bot_version": globals().get("VERSION", ""), "created_at": _journal_ts(),
            "render_instance_id": str(os.getenv("RENDER_INSTANCE_ID", "") or ""),
            "render_git_commit": str(os.getenv("RENDER_GIT_COMMIT", "") or ""),
            "row_count": len(rows), "rows": rows,
        }
        _atomic_json_dump(tmp, payload)
        _mega_run("mega-put", [tmp, remote_dir], check=True, timeout=MEGA_TIMEOUT)
        _JOURNAL_DURABLE_STATS["uploaded_chunks"] += 1
        _JOURNAL_DURABLE_STATS["uploaded_rows"] += len(rows)
        _JOURNAL_DURABLE_STATS["last_upload_at"] = _journal_ts()
        _JOURNAL_DURABLE_STATS["last_upload_file"] = name
        _JOURNAL_DURABLE_STATS["last_error"] = ""
        # Prune rarely; listing thousands of files on each flush would itself load Render/MEGA.
        if (_JOURNAL_DURABLE_STATS["uploaded_chunks"] % 25) == 0:
            try:
                _mega_prune_remote_history(remote_dir, "journal_*.json", BOT_JOURNAL_DURABLE_REMOTE_KEEP)
            except Exception:
                pass
        return True
    except Exception as e:
        _JOURNAL_DURABLE_STATS["upload_errors"] += 1
        _JOURNAL_DURABLE_STATS["last_error"] = str(e)[:500]
        # Не теряем строки при временной ошибке MEGA: возвращаем в голову буфера.
        with _JOURNAL_DURABLE_LOCK:
            _JOURNAL_DURABLE_BUFFER[0:0] = rows
            # ограничиваем аварийный RAM spool, чтобы MEGA outage не съел всю память Render
            if len(_JOURNAL_DURABLE_BUFFER) > 1000:
                del _JOURNAL_DURABLE_BUFFER[:-1000]
        return False
    finally:
        try:
            if tmp and os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass


def _journal_durable_loop():
    # Периодический flush только когда есть строки. Никаких MEGA вызовов в простое.
    while True:
        try:
            time.sleep(BOT_JOURNAL_DURABLE_FLUSH_SECONDS)
            with _JOURNAL_DURABLE_LOCK:
                has_rows = bool(_JOURNAL_DURABLE_BUFFER)
            if has_rows:
                journal_flush_to_mega(True)
        except Exception as e:
            _JOURNAL_DURABLE_STATS["last_error"] = str(e)[:500]
            time.sleep(5.0)


def journal_start_durable_loop():
    global _JOURNAL_DURABLE_THREAD_STARTED
    if not BOT_JOURNAL_DURABLE_ENABLED or _JOURNAL_DURABLE_THREAD_STARTED:
        return
    _JOURNAL_DURABLE_THREAD_STARTED = True
    threading.Thread(target=_journal_durable_loop, name="journal-mega-spool", daemon=True).start()


def _journal_read_mega_rows(limit: int = 20000) -> list[dict]:
    """Читает последние durable journal chunks из MEGA, newest-first files -> chronological rows."""
    if not BOT_JOURNAL_DURABLE_ENABLED or not globals().get("mega_is_configured") or not mega_is_configured():
        return []
    remote_dir = _journal_durable_remote_dir()
    try:
        # BOOT/recent-tail reads should not enumerate dozens of MEGA files.
        # Full export has its own streaming reader and may use a larger window.
        estimated_files = max(3, int(max(1, int(limit)) / max(1, BOT_JOURNAL_DURABLE_FLUSH_ROWS)) + 3)
        files = _mega_find_remote_files(
            remote_dir, "journal_*.json", min(BOT_JOURNAL_DURABLE_RESTORE_FILES, estimated_files)
        )
    except Exception:
        return []
    chunks = []
    total = 0
    # mega-find returns reverse sorted. Read newest until enough rows, then merge chronologically.
    for remote in files:
        local = None
        try:
            local = _mega_download_remote_path(remote)
            doc = _load_json(local, {}) if local else {}
            rows = doc.get("rows") if isinstance(doc, dict) else []
            if isinstance(rows, list) and rows:
                chunks.append(rows)
                total += len(rows)
                if total >= int(limit):
                    break
        except Exception:
            continue
        finally:
            try:
                if local:
                    shutil.rmtree(os.path.dirname(local), ignore_errors=True)
            except Exception:
                pass
    merged = []
    for rows in reversed(chunks):
        merged.extend(rows)
    return _journal_merge_rows(merged, limit=limit)


def journal_restore_from_mega(limit: int = 200) -> dict:
    """BOOT: restores only a small recent tail. Full history stays in MEGA and is streamed on export."""
    remote_rows = _journal_read_mega_rows(limit)
    local_rows = _journal_read_file_rows(limit)
    merged = _journal_merge_rows(remote_rows, local_rows, limit=limit)
    if remote_rows:
        try:
            with bot_journal_lock:
                BOT_ACTION_LOG.clear()
                BOT_ACTION_LOG.extend(merged[-BOT_JOURNAL_MAX:])
            # Локальный файл теперь тоже содержит историю до рестарта + новый BOOT хвост.
            with open(BOT_JOURNAL_FILE, "w", encoding="utf-8") as f:
                for row in merged:
                    f.write(json.dumps(row, ensure_ascii=False) + "\n")
        except Exception as e:
            _JOURNAL_DURABLE_STATS["last_error"] = str(e)[:500]
    _JOURNAL_DURABLE_STATS["restored_rows"] = len(remote_rows)
    # approximate chunk count is exposed by read limit files; exact count not worth a second mega-find
    _JOURNAL_DURABLE_STATS["restored_chunks"] = 0 if not remote_rows else 1
    return {"remote_rows": len(remote_rows), "merged_rows": len(merged)}


def journal_durable_stats() -> dict:
    with _JOURNAL_DURABLE_LOCK:
        out = dict(_JOURNAL_DURABLE_STATS)
        out["buffer_rows"] = len(_JOURNAL_DURABLE_BUFFER)
    out.update({
        "enabled": BOT_JOURNAL_DURABLE_ENABLED,
        "flush_seconds": BOT_JOURNAL_DURABLE_FLUSH_SECONDS,
        "flush_rows": BOT_JOURNAL_DURABLE_FLUSH_ROWS,
        "remote_dir": _journal_durable_remote_dir(),
    })
    return out


def _journal_warm_tail_job():
    """Post-READY warm-up only; never delays BOOT or Telegram availability."""
    try:
        if runtime_is_shutting_down():
            return
        if _runtime_watcher_should_yield_to_critical_mega():
            DELAYED_SCHEDULER.schedule("journal-warm-tail", 30.0, _journal_warm_tail_job)
            return
        pressure = _runtime_memory_pressure()
        if str(pressure.get("level")) in {"high", "critical"}:
            DELAYED_SCHEDULER.schedule("journal-warm-tail", 60.0, _journal_warm_tail_job)
            return
        jr = journal_restore_from_mega(40)
        runtime_event("journal_warm_tail", f"remote_rows={jr.get('remote_rows',0)} merged_rows={jr.get('merged_rows',0)}")
    except Exception as e:
        runtime_event("journal_warm_tail_error", str(e), "WARN")



def _journal_render_safe_env() -> dict:
    # Только системные Render-поля. Пользовательские env, токены и пароли сюда намеренно не попадают.
    keys = (
        "RENDER", "RENDER_CPU_COUNT", "RENDER_INSTANCE_ID", "RENDER_GIT_COMMIT",
        "RENDER_GIT_BRANCH", "RENDER_GIT_REPO_SLUG", "RENDER_SERVICE_ID",
        "RENDER_SERVICE_NAME", "RENDER_SERVICE_TYPE", "RENDER_REGION",
        "RENDER_EXTERNAL_HOSTNAME", "RENDER_EXTERNAL_URL", "IS_PULL_REQUEST",
    )
    return {k: str(os.getenv(k, "") or "") for k in keys}


def _journal_diagnostic_snapshot() -> dict:
    snap = runtime_snapshot({"source": "journal_export"}) if "runtime_snapshot" in globals() else {}
    dispatcher = UPDATE_DISPATCHER.stats() if "UPDATE_DISPATCHER" in globals() else {}
    thread_rows = []
    try:
        for t in threading.enumerate():
            thread_rows.append({"name": t.name, "ident": t.ident, "daemon": bool(t.daemon), "alive": bool(t.is_alive())})
    except Exception:
        pass
    os_diag = {}
    try:
        os_diag["loadavg"] = list(os.getloadavg()) if hasattr(os, "getloadavg") else None
    except Exception:
        os_diag["loadavg"] = None
    try:
        os_diag["cpu_count"] = os.cpu_count()
    except Exception:
        pass
    try:
        os_diag["open_fd_count"] = len(os.listdir("/proc/self/fd")) if os.path.isdir("/proc/self/fd") else None
    except Exception:
        os_diag["open_fd_count"] = None
    try:
        os_diag["python_gc_count"] = list(__import__("gc").get_count())
    except Exception:
        pass
    return {
        "generated_at": _journal_ts(),
        "version": VERSION,
        "behavior_profile": active_bot_behavior_profile() if "active_bot_behavior_profile" in globals() else "",
        "render_safe_env": _journal_render_safe_env(),
        "runtime": snap,
        "dispatcher": dispatcher,
        "keep_alive": dict(KEEP_ALIVE_STATE) if "KEEP_ALIVE_STATE" in globals() else {},
        "delayed_scheduler": DELAYED_SCHEDULER.stats() if "DELAYED_SCHEDULER" in globals() else {},
        "mega_tasks": mega_task_registry_stats() if "mega_task_registry_stats" in globals() else {},
        "durable_journal": journal_durable_stats() if "journal_durable_stats" in globals() else {},
        "priority": {
            "order": ["finance", "forward", "other"],
            "forward_finance_priority_max_wait_seconds": globals().get("FORWARD_FINANCE_PRIORITY_MAX_WAIT_SECONDS"),
        },
        "threads": thread_rows,
        "os": os_diag,
    }


def _journal_write_export_row(fh, r: dict):
    q = (
        f"Q wh={r.get('webhook_pending',0)}/{r.get('webhook_active',0)} "
        f"fin={r.get('finance_pending',0)}/{r.get('finance_active',0)} "
        f"fwd={r.get('forward_pending',0)}/{r.get('forward_active',0)} "
        f"delta={r.get('delta_pending',0)} backup={r.get('backup_pending',0)}"
    )
    fh.write(
        f"{r.get('ts','')} | {r.get('level','')} | {r.get('action','')} | "
        f"chat={r.get('chat_name') or r.get('chat_id')} | thread={r.get('thread','')} | "
        f"phase={r.get('runtime_phase','')} ready={r.get('runtime_ready','')} | {q} | {r.get('detail','')}\n"
    )


def _journal_stream_mega_rows_to_file(fh, limit: int = 3000, bot_version_filter: str | None = None, since_ts: str | None = None) -> int:
    """Stream durable journal chunks without loading the whole history into RAM.

    bot_version_filter is exact for v124+ rows. since_ts is a compatibility fallback for
    older rows that did not yet carry bot_version. Existing full-journal callers use no filter.
    """
    if not BOT_JOURNAL_DURABLE_ENABLED or not mega_is_configured():
        return 0
    remote_dir = _journal_durable_remote_dir()
    max_files = min(BOT_JOURNAL_DURABLE_RESTORE_FILES, max(8, int(max(1, limit) / max(1, BOT_JOURNAL_DURABLE_FLUSH_ROWS)) + 16))
    try:
        files = _mega_find_remote_files(remote_dir, "journal_*.json", max_files)
    except Exception:
        return 0
    count = 0
    seen = set()
    wanted_version = str(bot_version_filter or "").strip()
    since_ts = str(since_ts or "").strip()
    for remote in reversed(files):
        local = None
        try:
            local = _mega_download_remote_path(remote)
            doc = _load_json(local, {}) if local else {}
            rows = doc.get("rows") if isinstance(doc, dict) else []
            if not isinstance(rows, list):
                continue
            for r in rows:
                if not isinstance(r, dict):
                    continue
                if wanted_version:
                    row_version = str(r.get("bot_version") or "").strip()
                    if row_version:
                        if row_version != wanted_version:
                            continue
                    elif since_ts and str(r.get("ts") or "") < since_ts:
                        continue
                key = _journal_row_key(r)
                if key in seen:
                    continue
                seen.add(key)
                _journal_write_export_row(fh, r)
                count += 1
                if count >= int(limit):
                    return count
        except Exception as e:
            fh.write(f"[journal chunk read error] {remote}: {e}\n")
        finally:
            try:
                if local:
                    shutil.rmtree(os.path.dirname(local), ignore_errors=True)
            except Exception:
                pass
    return count



# ─────────────────────────────────────────────────────────────
# v120: single-flight interactive file exports + visible elapsed time
# ─────────────────────────────────────────────────────────────
# EXPORT_TASK_POOL has one worker by default. Previously every button press created
# another full ZIP/journal job, so 3 taps could mean 3 expensive MEGA scans in a row.
# The gate below allows only one owner-requested file build/send job at a time and
# coalesces repeated taps without touching finance/forward/business queues.
_INTERACTIVE_FILE_JOB_KEY = "interactive-file-global"
_FILE_JOB_LOCK = threading.RLock()
_FILE_JOB_STATE = {}
_FILE_JOB_CONTEXT = threading.local()


def _file_job_elapsed_text(seconds: float) -> str:
    try:
        total = max(0, int(seconds or 0))
    except Exception:
        total = 0
    h, rem = divmod(total, 3600)
    m, sec = divmod(rem, 60)
    return f"{h:d}:{m:02d}:{sec:02d}" if h else f"{m:d}:{sec:02d}"


def _file_job_current() -> dict:
    try:
        return getattr(_FILE_JOB_CONTEXT, "value", None) or {}
    except Exception:
        return {}


def _file_job_busy_info() -> dict:
    with _FILE_JOB_LOCK:
        st = dict(_FILE_JOB_STATE.get(_INTERACTIVE_FILE_JOB_KEY) or {})
    if not st:
        return {}
    started = float(st.get("started_monotonic") or st.get("queued_monotonic") or time.monotonic())
    st["elapsed"] = max(0.0, time.monotonic() - started)
    return st


def _file_job_progress(phase: str, current=None, total=None, force: bool = False):
    """Update one temporary Telegram status message at a throttled rate."""
    ctx = _file_job_current()
    if not ctx:
        return
    key = str(ctx.get("key") or _INTERACTIVE_FILE_JOB_KEY)
    now_m = time.monotonic()
    with _FILE_JOB_LOCK:
        st = _FILE_JOB_STATE.get(key)
        if not isinstance(st, dict):
            return
        st["phase"] = str(phase or "работаю")
        if current is not None:
            st["current"] = current
        if total is not None:
            st["total"] = total
        last = float(st.get("last_ui_monotonic") or 0.0)
        if not force and (now_m - last) < 8.0:
            return
        st["last_ui_monotonic"] = now_m
        chat_id = int(st.get("chat_id"))
        msg_id = st.get("status_msg_id")
        label = str(st.get("label") or "Файл")
        started = float(st.get("started_monotonic") or st.get("queued_monotonic") or now_m)
        elapsed = _file_job_elapsed_text(now_m - started)
        cur = st.get("current")
        tot = st.get("total")
    progress = ""
    if cur is not None and tot is not None:
        progress = f"\nПрогресс: {cur}/{tot}"
    text = f"⏳ {label}\nВремя: {elapsed}\nЭтап: {phase}{progress}\nПовторные нажатия не ставятся в очередь."
    try:
        if msg_id:
            bot.edit_message_text(text, chat_id=chat_id, message_id=int(msg_id))
    except Exception:
        pass



def _file_job_tick(key: str):
    """Keep elapsed time moving even when the builder is inside one long blocking call."""
    key = str(key)
    with _FILE_JOB_LOCK:
        st = _FILE_JOB_STATE.get(key)
        if not isinstance(st, dict):
            return
        chat_id = int(st.get("chat_id"))
        msg_id = st.get("status_msg_id")
        label = str(st.get("label") or "Файл")
        phase = str(st.get("phase") or "работаю")
        started = float(st.get("started_monotonic") or st.get("queued_monotonic") or time.monotonic())
        elapsed = _file_job_elapsed_text(time.monotonic() - started)
        cur = st.get("current")
        tot = st.get("total")
    progress = f"\nПрогресс: {cur}/{tot}" if cur is not None and tot is not None else ""
    try:
        if msg_id:
            bot.edit_message_text(
                f"⏳ {label}\nВремя: {elapsed}\nЭтап: {phase}{progress}\nПовторные нажатия не ставятся в очередь.",
                chat_id=chat_id,
                message_id=int(msg_id),
            )
    except Exception:
        pass
    with _FILE_JOB_LOCK:
        alive = isinstance(_FILE_JOB_STATE.get(key), dict)
    if alive:
        DELAYED_SCHEDULER.schedule(f"file-job-tick:{key}", 10.0, _file_job_tick, key)


def _interactive_file_job_runner(job_meta: dict, func, args, kwargs):
    key = str(job_meta.get("key") or _INTERACTIVE_FILE_JOB_KEY)
    previous = getattr(_FILE_JOB_CONTEXT, "value", None)
    _FILE_JOB_CONTEXT.value = {"key": key}
    ok = False
    error_text = ""
    try:
        with _FILE_JOB_LOCK:
            st = _FILE_JOB_STATE.get(key)
            if isinstance(st, dict):
                st["started_monotonic"] = time.monotonic()
                st["phase"] = "запуск"
        _file_job_progress("запуск", force=True)
        result = func(*args, **kwargs)
        ok = (result is not False)
        if not ok:
            error_text = "операция завершилась без подтверждения"
    except Exception as exc:
        error_text = str(exc)[:300]
        try:
            log_error(f"INTERACTIVE FILE JOB {job_meta.get('kind')}: {exc}")
        except Exception:
            pass
    finally:
        now_m = time.monotonic()
        with _FILE_JOB_LOCK:
            st = _FILE_JOB_STATE.get(key)
            if isinstance(st, dict):
                chat_id = int(st.get("chat_id"))
                msg_id = st.get("status_msg_id")
                label = str(st.get("label") or "Файл")
                started = float(st.get("started_monotonic") or st.get("queued_monotonic") or now_m)
                elapsed = _file_job_elapsed_text(now_m - started)
            else:
                chat_id = int(job_meta.get("chat_id") or 0)
                msg_id = None
                label = str(job_meta.get("label") or "Файл")
                elapsed = "0:00"
        try:
            if msg_id:
                final = (f"✅ {label}\nГотово за {elapsed}." if ok else f"⚠️ {label}\nЗавершено за {elapsed}.\n{error_text or 'Telegram не подтвердил отправку.'}")
                bot.edit_message_text(final, chat_id=chat_id, message_id=int(msg_id))
                delete_message_later(chat_id, int(msg_id), 90 if ok else 180)
        except Exception:
            pass
        try:
            bot_journal("file_job_done" if ok else "file_job_uncertain", chat_id, f"kind={job_meta.get('kind')} elapsed={elapsed} error={error_text}")
        except Exception:
            pass
        try:
            DELAYED_SCHEDULER.cancel(f"file-job-tick:{key}")
        except Exception:
            pass
        with _FILE_JOB_LOCK:
            _FILE_JOB_STATE.pop(key, None)
        if previous is None:
            try:
                delattr(_FILE_JOB_CONTEXT, "value")
            except Exception:
                pass
        else:
            _FILE_JOB_CONTEXT.value = previous


def submit_interactive_file_job(chat_id: int, kind: str, label: str, func, *args, **kwargs) -> tuple[bool, str]:
    """Start one heavy user-requested file job; duplicate taps are coalesced."""
    chat_id = int(chat_id)
    key = _INTERACTIVE_FILE_JOB_KEY
    with _FILE_JOB_LOCK:
        existing = _FILE_JOB_STATE.get(key)
        if isinstance(existing, dict):
            started = float(existing.get("started_monotonic") or existing.get("queued_monotonic") or time.monotonic())
            elapsed = _file_job_elapsed_text(time.monotonic() - started)
            return False, f"Уже выполняется: {existing.get('label','файл')} · {elapsed}"
        meta = {
            "key": key,
            "chat_id": chat_id,
            "kind": str(kind),
            "label": str(label),
            "queued_monotonic": time.monotonic(),
            "started_monotonic": 0.0,
            "phase": "в очереди",
            "status_msg_id": None,
            "last_ui_monotonic": 0.0,
        }
        _FILE_JOB_STATE[key] = meta
    try:
        status = bot.send_message(chat_id, f"⏳ {label}\nВремя: 0:00\nЭтап: в очереди\nПовторные нажатия не ставятся в очередь.")
        with _FILE_JOB_LOCK:
            if isinstance(_FILE_JOB_STATE.get(key), dict):
                _FILE_JOB_STATE[key]["status_msg_id"] = int(getattr(status, "message_id", 0) or 0) or None
    except Exception:
        pass
    ok = EXPORT_TASK_POOL.submit_unique(key, _interactive_file_job_runner, dict(meta), func, args, kwargs)
    if not ok:
        with _FILE_JOB_LOCK:
            _FILE_JOB_STATE.pop(key, None)
        return False, "Экспорт уже занят"
    try:
        DELAYED_SCHEDULER.cancel(f"file-job-tick:{key}")
        DELAYED_SCHEDULER.schedule(f"file-job-tick:{key}", 10.0, _file_job_tick, key)
    except Exception:
        pass
    try:
        bot_journal("file_job_queued", chat_id, f"kind={kind} label={label}")
    except Exception:
        pass
    return True, "Запущено"


def _send_journal_file_to_owner_sync(chat_id: int, limit: int = 3000):
    """Build/send diagnostics inside EXPORT_TASK_POOL; never block Telegram webhook workers."""
    if not is_owner_chat(chat_id):
        send_and_auto_delete(chat_id, "📓 Журнал доступен только владельцу.", HELPER_DELETE_DELAY)
        return
    bot_journal("journal_export_requested", chat_id, f"limit={limit}; streaming=1; memory_guard=1")
    _file_job_progress("фиксирую свежий журнал в MEGA", force=True)
    try:
        journal_flush_to_mega(True)
    except Exception:
        pass

    pressure = _runtime_memory_pressure()
    if str(pressure.get("level")) in {"high", "critical"}:
        _runtime_emergency_trim("journal_export")

    diag = _journal_diagnostic_snapshot()
    tmp_path = None
    try:
        fd, tmp_path = tempfile.mkstemp(prefix="bot_diagnostic_", suffix=".txt")
        os.close(fd)
        with open(tmp_path, "w", encoding="utf-8") as fh:
            fh.write("📓 МАКСИМАЛЬНЫЙ ДИАГНОСТИЧЕСКИЙ ЖУРНАЛ БОТА\n")
            fh.write(f"Создан: {_journal_ts()}\nВерсия: {VERSION}\n")
            fh.write("ВАЖНО: время старта Python != время начала Render deploy.\n")
            fh.write("v123: edit consistency + 💰Перес clean insert + exact edit witnesses; LOW-RAM remains active.\n\n")
            fh.write("==================== CURRENT DIAGNOSTIC SNAPSHOT (JSON) ====================\n")
            json.dump(diag, fh, ensure_ascii=False, indent=2, default=str)
            fh.write("\n\n==================== DURABLE JOURNAL ====================\n")
            json.dump(journal_durable_stats(), fh, ensure_ascii=False, indent=2, default=str)
            fh.write("\n\n==================== RUNTIME EVENTS ====================\n")
            try:
                with _RUNTIME_LOCK:
                    runtime_rows = list(_RUNTIME_EVENTS)
                for r in runtime_rows:
                    fh.write(f"{r.get('ts','')} | {r.get('level','')} | {r.get('event','')} | {r.get('detail','')}\n")
            except Exception as e:
                fh.write(f"runtime events unavailable: {e}\n")
            fh.write("\n==================== ACTION JOURNAL (MEGA STREAM) ====================\n")
            _file_job_progress("читаю журнал из MEGA", force=True)
            remote_count = _journal_stream_mega_rows_to_file(fh, int(limit))
            fh.write(f"\n[MEGA rows streamed: {remote_count}]\n")
            fh.write("\n==================== CURRENT PROCESS TAIL ====================\n")
            seen_tail = set()
            for r in _journal_merge_rows(_journal_read_file_rows(300), get_recent_journal(300), limit=600):
                key = _journal_row_key(r)
                if key in seen_tail:
                    continue
                seen_tail.add(key)
                _journal_write_export_row(fh, r)
            fh.write("\n==================== INTERPRETATION KEYS ====================\n")
            fh.write("deploy_new_commit_*: Git commit changed — strong deploy evidence.\n")
            fh.write("planned_restart_same_commit: same commit + graceful SIGTERM.\n")
            fh.write("probable_render_idle_*: probable sleep/wake estimate.\n")
            fh.write("process_restart_or_unknown + high RAM/no SIGTERM: suspect OOM/hard kill.\n")
            fh.write("v125: fast 3-day 💰Перес repaint, global Excel mode and verified Notes/Comments separation.\n")
        _file_job_progress("отправляю файл в Telegram", force=True)
        with open(tmp_path, "rb") as fh:
            _tg_call_retry(
                bot.send_document,
                chat_id,
                fh,
                caption="📓 Максимальный журнал: Render + бот + очереди + MEGA (single-flight v120)",
                timeout=120,
                purpose="journal_send_document",
            )
        return True
    finally:
        try:
            if tmp_path and os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass


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



def send_journal_file_to_owner(chat_id: int, limit: int = 3000):
    """Queue one maximum journal export; repeated taps are coalesced."""
    if not is_owner_chat(chat_id):
        send_and_auto_delete(chat_id, "📓 Журнал доступен только владельцу.", HELPER_DELETE_DELAY)
        return False
    ok, info = submit_interactive_file_job(int(chat_id), "journal", "Диагностический журнал", _send_journal_file_to_owner_sync, int(chat_id), int(limit))
    if not ok:
        try:
            send_and_auto_delete(chat_id, f"⏳ {info}. Новая копия в очередь не добавлена.", 10)
        except Exception:
            pass
        return False
    return True


BOT_SOURCE_ARCHIVE_DIR = os.getenv("MEGA_BOT_SOURCE_ARCHIVE_DIR", "/TelegramBotBackups/runtime/bot_versions").strip() or "/TelegramBotBackups/runtime/bot_versions"


def _current_version_journal_since_ts() -> str:
    """Compatibility lower bound for v123-and-older rows without a bot_version field."""
    try:
        return str((globals().get("_RUNTIME_STATE") or {}).get("started_at") or "")[:23].replace("T", " ")
    except Exception:
        return ""


def _send_current_version_journal_to_owner_sync(chat_id: int, limit: int = 5000):
    tmp_path = None
    try:
        _file_job_progress("фиксирую последние строки журнала", force=True)
        try:
            journal_flush_to_mega(True)
        except Exception:
            pass
        stamp = now_local().strftime("%Y%m%d_%H%M%S")
        safe_ver = re.sub(r"[^0-9A-Za-z_\-]+", "_", str(VERSION))[:90]
        tmp_path = os.path.join(MEGA_LOCAL_TMP_DIR, f"journal_{safe_ver}_{stamp}.txt")
        with open(tmp_path, "w", encoding="utf-8") as fh:
            fh.write("📓 ЖУРНАЛ ТЕКУЩЕЙ ВЕРСИИ БОТА\n")
            fh.write(f"Версия: {VERSION}\n")
            fh.write(f"Commit: {str(os.getenv('RENDER_GIT_COMMIT','') or '—')}\n")
            fh.write(f"Текущий Python start: {str((globals().get('_RUNTIME_STATE') or {}).get('started_at') or '—')}\n")
            fh.write(f"Создан: {now_local().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}\n")
            fh.write("=" * 78 + "\n")
            _file_job_progress("читаю журнал этой версии из MEGA", force=True)
            count = _journal_stream_mega_rows_to_file(
                fh,
                int(limit),
                bot_version_filter=str(VERSION),
                since_ts=_current_version_journal_since_ts(),
            )
            fh.write("=" * 78 + "\n")
            fh.write(f"Строк: {count}\n")
            fh.write("\nRUNTIME EVENTS CURRENT PROCESS\n")
            for ev in list(globals().get("_RUNTIME_EVENTS") or []):
                if isinstance(ev, dict):
                    fh.write(f"{ev.get('ts','')} | {ev.get('level','')} | {ev.get('event','')} | {ev.get('detail','')}\n")
        _file_job_progress("отправляю журнал текущей версии", force=True)
        with open(tmp_path, "rb") as fh:
            _tg_call_retry(
                bot.send_document,
                int(chat_id),
                fh,
                caption=f"📓 Журнал текущей версии: {VERSION}",
                timeout=120,
                purpose="current_version_journal_send",
            )
        return True
    finally:
        try:
            if tmp_path and os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass


def send_current_version_journal_to_owner(chat_id: int, limit: int = 5000):
    if not is_owner_chat(chat_id):
        return False
    ok, info = submit_interactive_file_job(
        int(chat_id), "journal_current", "Журнал текущей версии",
        _send_current_version_journal_to_owner_sync, int(chat_id), int(limit),
    )
    if not ok:
        send_and_auto_delete(int(chat_id), f"⏳ {info}. Новая копия в очередь не добавлена.", 10)
    return bool(ok)


def _send_current_bot_source_sync(chat_id: int):
    path = _current_source_path()
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    _file_job_progress("читаю исходник текущего деплоя", force=True)
    fobj = file_bytesio_named(path, f"{VERSION}.py")
    if not fobj:
        raise RuntimeError("Не удалось открыть исходник текущего деплоя")
    _file_job_progress("отправляю исходник текущего деплоя", force=True)
    _tg_call_retry(
        bot.send_document,
        int(chat_id),
        fobj,
        caption=f"🤖 Исходник текущего деплоя\n{VERSION}\ncommit {str(os.getenv('RENDER_GIT_COMMIT','') or '—')[:12]}",
        timeout=120,
        purpose="current_bot_source_send",
    )
    return True


def send_current_bot_source_to_owner(chat_id: int):
    if not is_owner_chat(chat_id):
        return False
    ok, info = submit_interactive_file_job(
        int(chat_id), "bot_source", "Исходник текущего деплоя",
        _send_current_bot_source_sync, int(chat_id),
    )
    if not ok:
        send_and_auto_delete(int(chat_id), f"⏳ {info}. Новая копия в очередь не добавлена.", 10)
    return bool(ok)


def archive_current_bot_source_to_mega() -> bool:
    """One immutable name per VERSION+commit, so the running source survives later deploys."""
    if not mega_is_configured():
        return False
    path = _current_source_path()
    if not os.path.exists(path):
        return False
    commit = re.sub(r"[^0-9A-Za-z]+", "", str(os.getenv("RENDER_GIT_COMMIT", "") or ""))[:16] or "no_commit"
    safe_ver = re.sub(r"[^0-9A-Za-z_\-]+", "_", str(VERSION))[:90]
    remote_name = f"{safe_ver}__{commit}.py"
    ok = mega_put_replace(path, BOT_SOURCE_ARCHIVE_DIR, remote_name, archive_previous=False)
    try:
        bot_journal("bot_source_archive", None, f"ok={ok} file={remote_name}")
    except Exception:
        pass
    return bool(ok)

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
    'restore_guard_toggle': 'Ф165',
    'mega_manual_restore': 'Ф166',
    'main_close:*': 'Ф167',
    'runtime_watcher': 'Ф168',
    'runtime_events': 'Ф169',
    'runtime_snapshot_now': 'Ф170',
    'excel_style_toggle': 'Ф171',
    'excel_style_menu': 'Ф171',
    'excel_style_set:*': 'Ф171',
    'runtime_export': 'Ф172',
    'info_close': 'Ф85',
    'info_finance_off': 'Ф86',
    'journal_back': 'Ф87',
    'journal_file': 'Ф88',
    'journal_current_file': 'Ф173',
    'journal_bot_source': 'Ф174',
    'fwdcopy_edit_copy': 'Ф175',
    'itxt:*': 'Ф176',
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
    'version_page:*': 'Ф132',
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
    """Добавляет чистый служебный маркер окна без декоративных пробелов/HTML-знаков."""
    try:
        text = strip_window_mark(str(text or ""))
        code = str(code or "").strip()
        if not code:
            return text
        # Маркер показываем ровно как «Ф110», «С4», «П22» — без отступов,
        # курсива, скобок и других лишних знаков. Это одинаково для HTML/plain.
        return text + "\n\n" + code
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
    # v119: stored windows inherit the marker of their real navigation callback.
    # Example: stored:command_window_id:d:*:back_main -> Ф40 instead of noisy Ф9998.
    if key.startswith("stored:"):
        parts = key.split(":", 2)
        if len(parts) == 3 and parts[2]:
            inner_key = parts[2]
            direct = WINDOW_MARKER_CONSTANTS.get(inner_key)
            if direct:
                return direct
            for pattern, marker in sorted(WINDOW_MARKER_CONSTANTS.items(), key=lambda item: (item[0].count("*"), -len(item[0]))):
                if "*" in pattern and _marker_constant_pattern_matches(pattern, inner_key):
                    return marker
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


def _schedule_v98_auto_close(chat_id: int, message_id: int, delay: int | float | None = None):
    """Обычные o98/v98 окна по таймеру возвращаются в основное окно; секретные режимы сюда не входят."""
    chat_id = int(chat_id)
    message_id = int(message_id)
    if delay is None:
        delay = internal_timer_seconds("window_auto_return", 120)
    _cancel_v98_auto_close(chat_id, message_id)

    def _job():
        with _v98_auto_close_lock:
            _v98_auto_close_timers.pop((chat_id, message_id), None)
        try:
            day_key = get_chat_store(chat_id).get("current_view_day") or today_key()
            return_to_main_window_closing_previous(chat_id, day_key, message_id)
        except Exception as e:
            log_error(f"v98 auto return({chat_id},{message_id}): {e}")

    deadline = DELAYED_SCHEDULER.schedule(
        _v98_scheduler_key(chat_id, message_id),
        float(delay),
        _job,
    )
    with _v98_auto_close_lock:
        _v98_auto_close_timers[(chat_id, message_id)] = deadline


def _touch_v98_auto_close_for_callback(chat_id: int, message_id: int, data_str: str):
    try:
        code = window_code_for_callback(data_str, owner_chat=is_owner_chat(chat_id))
        if code in {"о98", "в98", "Ф9998"}:
            _schedule_v98_auto_close(chat_id, message_id, None)
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

# v104: единые пользовательские таймеры для обычных (НЕ секретных) режимов.
# Значения глобальные: изменение одного таймера действует во всех окнах/режимах,
# где используется соответствующая функция. Секретные таймеры намеренно отдельные.
INTERNAL_TIMER_DEFS = {
    "input_wait": {"label": "✏️ Ожидание ввода / редактирования", "default": 40, "min": 5, "max": 3600},
    "window_auto_return": {"label": "🪟 Автовозврат обычных окон", "default": 120, "min": 5, "max": 7200},
    "command_cleanup": {"label": "🧹 Удаление команд пользователя", "default": 30, "min": 1, "max": 3600},
    "balance_collapse": {"label": "🏦 Сворачивание быстрого остатка", "default": 90, "min": 5, "max": 3600},
}
_timer_input_sessions = {}
_timer_input_lock = threading.RLock()


def _format_duration_short(seconds: int | float) -> str:
    seconds = max(0, int(round(float(seconds or 0))))
    minutes, sec = divmod(seconds, 60)
    if minutes and sec:
        return f"{minutes}м {sec}с"
    if minutes:
        return f"{minutes}м"
    return f"{sec}с"


def internal_timer_seconds(key: str, fallback=None) -> float:
    cfg = INTERNAL_TIMER_DEFS.get(str(key)) or {}
    default = float(cfg.get("default", fallback if fallback is not None else 30) or 30)
    try:
        gs = data.setdefault("_global_settings", {})
        values = gs.setdefault("internal_timers", {})
        value = float(values.get(str(key), default) or default)
    except Exception:
        value = default
    low = float(cfg.get("min", 1) or 1)
    high = float(cfg.get("max", 86400) or 86400)
    return max(low, min(high, value))


def set_internal_timer_seconds(key: str, seconds: int | float) -> float:
    key = str(key)
    cfg = INTERNAL_TIMER_DEFS.get(key)
    if not cfg:
        raise KeyError(key)
    value = max(float(cfg.get("min", 1)), min(float(cfg.get("max", 86400)), float(seconds)))
    data.setdefault("_global_settings", {}).setdefault("internal_timers", {})[key] = value
    save_data(data, root_only=True)
    # Настройка должна пережить Render deploy: маленькая root-delta отправляется быстро в MEGA.
    try:
        if OWNER_ID:
            schedule_quick_backup(int(OWNER_ID), 0.5)
        _mark_global_snapshot_pending()
    except Exception as e:
        try:
            log_error(f"set_internal_timer_seconds backup: {e}")
        except Exception:
            pass
    return value


def build_internal_timers_text() -> str:
    lines = [
        "⏱ Внутренние таймеры",
        "",
        "Настройки общие для всех обычных режимов бота.",
        "Секретный режим имеет собственные таймеры и здесь не меняется.",
        "",
    ]
    for key, cfg in INTERNAL_TIMER_DEFS.items():
        lines.append(f"{cfg['label']}: {_format_duration_short(internal_timer_seconds(key))}")
    lines.extend(["", "Выберите таймер для изменения."])
    return wm_owner("\n".join(lines), 9)


def build_internal_timers_keyboard(chat_id: int):
    kb = types.InlineKeyboardMarkup(row_width=1)
    for key, cfg in INTERNAL_TIMER_DEFS.items():
        kb.row(IB(f"{cfg['label']} — {_format_duration_short(internal_timer_seconds(key))}", callback_data=f"itmr_pick:{key}"))
    day = get_chat_store(chat_id).get("current_view_day") or today_key()
    kb.row(IB("🔙 Назад в Инфо", callback_data="itmr_back_info"))
    kb.row(IB("⬅️ Назад осн. окно", callback_data=f"d:{day}:back_main"), IB("❌ Закрыть", callback_data="info_close"))
    return kb


def _timer_input_session(chat_id: int):
    with _timer_input_lock:
        return _timer_input_sessions.setdefault(int(chat_id), {"key": None, "buffer": "", "minutes": None, "seconds": None})


def _reset_timer_input_session(chat_id: int, key: str | None = None):
    with _timer_input_lock:
        _timer_input_sessions[int(chat_id)] = {"key": key, "buffer": "", "minutes": None, "seconds": None}
        return _timer_input_sessions[int(chat_id)]


def _timer_input_total_preview(session: dict) -> int:
    minutes = int(session.get("minutes") or 0)
    seconds = int(session.get("seconds") or 0)
    buf = str(session.get("buffer") or "")
    if buf:
        # После выбранных минут оставшийся буфер трактуем как секунды; без единицы — секунды.
        if session.get("minutes") is not None:
            seconds = int(buf)
        elif session.get("seconds") is not None:
            seconds = int(buf)
        else:
            seconds = int(buf)
    return minutes * 60 + seconds


def build_internal_timer_input_text(chat_id: int) -> str:
    session = _timer_input_session(chat_id)
    key = session.get("key")
    cfg = INTERNAL_TIMER_DEFS.get(str(key)) or {"label": "Таймер"}
    buf = str(session.get("buffer") or "") or "—"
    mins = "—" if session.get("minutes") is None else str(session.get("minutes"))
    secs = "—" if session.get("seconds") is None else str(session.get("seconds"))
    preview = _timer_input_total_preview(session)
    return wm_owner(
        f"⏱ {cfg.get('label')}\n\n"
        f"Сейчас: {_format_duration_short(internal_timer_seconds(str(key)))}\n"
        f"Минуты: {mins}\nСекунды: {secs}\nНабор: {buf}\n"
        f"Итого сейчас: {_format_duration_short(preview)}\n\n"
        "Наберите число и нажмите «м» или «с». Можно задать, например: 1 → м → 30 → с. "
        "Если единицу не нажимать, число считается секундами. Затем нажмите «✅ Выбрать».",
        9,
    )


def build_internal_timer_input_keyboard(chat_id: int):
    kb = types.InlineKeyboardMarkup(row_width=3)
    for row in (("1","2","3"),("4","5","6"),("7","8","9"),("м","0","с")):
        buttons = []
        for value in row:
            if value == "м":
                buttons.append(IB("м", callback_data="itmr_unit:m"))
            elif value == "с":
                buttons.append(IB("с", callback_data="itmr_unit:s"))
            else:
                buttons.append(IB(value, callback_data=f"itmr_digit:{value}"))
        kb.row(*buttons)
    kb.row(IB("⌫", callback_data="itmr_backspace"), IB("🧹 Очистить", callback_data="itmr_clear"))
    kb.row(IB("✅ Выбрать", callback_data="itmr_apply"))
    day = get_chat_store(chat_id).get("current_view_day") or today_key()
    kb.row(IB("🔙 К списку таймеров", callback_data="internal_timers"))
    kb.row(IB("ℹ️ Инфо", callback_data="itmr_back_info"), IB("⬅️ Осн. окно", callback_data=f"d:{day}:back_main"), IB("❌ Закрыть", callback_data="info_close"))
    return kb

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
        else:
            # Заводская граница суток: 00:05. События 00:00–00:04 относятся к предыдущему финансовому дню.
            try:
                minute = int(_owner_setting_value("finance_day_start_minute", 5, chat_id) or 5)
            except Exception:
                minute = 5
            dt = dt - timedelta(minutes=max(0, min(59, minute)))
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
    if finance_day_start_5am_enabled(chat_id):
        return "05:00"
    try:
        minute = int(_owner_setting_value("finance_day_start_minute", 5, chat_id) or 5)
    except Exception:
        minute = 5
    return f"00:{max(0, min(59, minute)):02d}"


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



def _infer_legacy_finance_window_mode(chat_id: int) -> str:
    """Migration from v107: hidden-only means no visible auto-window; otherwise preserve old visible mode."""
    try:
        if not is_finance_mode(chat_id):
            return "off"
        if is_quick_balance_enabled(chat_id):
            behavior = get_quick_balance_behavior(chat_id)
            if behavior in {"open", "first"}:
                return behavior
        # In v107 simple "finance ON" enabled hidden finance and intentionally showed no window.
        if is_hidden_finance_mode(chat_id):
            return "off"
        return "normal"
    except Exception:
        return "off"


def _finance_window_state(chat_id: int) -> dict:
    chat_id = int(chat_id)
    store = get_chat_store(chat_id)
    state = store.get("finance_window_state")
    if not isinstance(state, dict):
        try:
            active = dict((data.get("active_messages", {}) or {}).get(str(chat_id), {}) or {})
        except Exception:
            active = {}
        mode = _infer_legacy_finance_window_mode(chat_id)
        state = {
            "mode": mode,
            "main_windows": {str(k): int(v) for k, v in active.items() if v},
            "balance_panel_id": int(store.get("balance_panel_id")) if store.get("balance_panel_id") else None,
            "balance_panel_mode": str(store.get("balance_panel_mode") or "mini"),
            "current_view_day": str(store.get("current_view_day") or today_key()),
            "auto_reopen_on_boot": bool(mode != "off" and (active or store.get("balance_panel_id") or not is_hidden_finance_mode(chat_id))),
            "updated_at": now_local().isoformat(timespec="seconds"),
        }
        store["finance_window_state"] = state
    state.setdefault("mode", _infer_legacy_finance_window_mode(chat_id))
    if state.get("mode") not in {"off", "normal", "open", "first"}:
        state["mode"] = "off"
    state.setdefault("main_windows", {})
    state.setdefault("balance_panel_id", None)
    state.setdefault("balance_panel_mode", "mini")
    state.setdefault("current_view_day", str(store.get("current_view_day") or today_key()))
    state.setdefault("auto_reopen_on_boot", bool(state.get("mode") != "off"))
    state.setdefault("updated_at", now_local().isoformat(timespec="seconds"))
    return state


def finance_window_mode(chat_id: int) -> str:
    if not is_finance_mode(chat_id):
        return "off"
    try:
        return str(_finance_window_state(chat_id).get("mode") or "off")
    except Exception:
        return "off"


def finance_window_mode_enabled(chat_id: int, mode: str | None = None) -> bool:
    current = finance_window_mode(chat_id)
    if mode is None:
        return current in {"normal", "open", "first"}
    return current == str(mode)


def _sync_finance_window_state_from_runtime(chat_id: int, *, schedule_delta: bool = False):
    """Compact UI state intentionally survives deploy without putting full open_window_registry into delta."""
    try:
        chat_id = int(chat_id)
        store = get_chat_store(chat_id)
        state = _finance_window_state(chat_id)
        try:
            active = dict((data.get("active_messages", {}) or {}).get(str(chat_id), {}) or {})
        except Exception:
            active = {}
        state["main_windows"] = {str(k): int(v) for k, v in active.items() if v}
        state["balance_panel_id"] = int(store.get("balance_panel_id")) if store.get("balance_panel_id") else None
        state["balance_panel_mode"] = str(store.get("balance_panel_mode") or state.get("balance_panel_mode") or "mini")
        state["current_view_day"] = str(store.get("current_view_day") or state.get("current_view_day") or today_key())
        state["updated_at"] = now_local().isoformat(timespec="seconds")
        store["finance_window_state"] = state
        save_data(data, chat_ids=[chat_id])
        if schedule_delta and mega_is_configured() and not RESTORE_GUARD_ACTIVE:
            schedule_quick_backup(chat_id, 0.5)
    except Exception as e:
        log_error(f"_sync_finance_window_state_from_runtime({chat_id}): {e}")


def restore_finance_window_runtime_state():
    """Rehydrate volatile Telegram message ids from compact chat metadata after MEGA/global+delta restore."""
    try:
        for cid_s, store in (data.get("chats", {}) or {}).items():
            try:
                cid = int(cid_s)
            except Exception:
                continue
            state = store.get("finance_window_state")
            if not isinstance(state, dict):
                # Create migration state, but do not generate Telegram windows here.
                _finance_window_state(cid)
                state = store.get("finance_window_state") or {}
            mode = str(state.get("mode") or "off")
            settings = store.setdefault("settings", {})
            if mode == "normal":
                settings["quick_balance_enabled"] = False
                settings["quick_balance_behavior"] = "normal"
                settings["quick_balance_user_selected"] = True
            elif mode in {"open", "first"}:
                settings["quick_balance_enabled"] = True
                settings["quick_balance_behavior"] = mode
                settings["quick_balance_user_selected"] = True
            else:
                settings["quick_balance_enabled"] = False
                settings["quick_balance_behavior"] = "normal"
                settings["quick_balance_user_selected"] = True
            main_windows = state.get("main_windows") or {}
            data.setdefault("active_messages", {})[str(cid)] = {
                str(k): int(v) for k, v in main_windows.items() if v
            }
            store["balance_panel_id"] = int(state.get("balance_panel_id")) if state.get("balance_panel_id") else None
            store["balance_panel_mode"] = str(state.get("balance_panel_mode") or "mini")
            if state.get("current_view_day"):
                store["current_view_day"] = str(state.get("current_view_day"))
    except Exception as e:
        log_error(f"restore_finance_window_runtime_state: {e}")


def _persist_finance_window_mode_critical(chat_id: int) -> bool:
    """Persist window choice + callback idempotency marker before a critical callback may be acknowledged."""
    try:
        chat_id = int(chat_id)
        _sync_finance_window_state_from_runtime(chat_id, schedule_delta=False)
        if mega_is_configured() and not RESTORE_GUARD_ACTIVE:
            ctx = _current_telegram_update_context()
            update_id = ctx.get("update_id")
            if update_id is not None and str(ctx.get("update_type") or "") == "callback_query":
                # Marker and chat state are captured by the same following compact delta.
                mark_durable_update_processed(update_id, chat_id, "callback_query")
            return bool(persist_critical_delta_now(chat_id))
    except Exception as e:
        log_error(f"_persist_finance_window_mode_critical({chat_id}): {e}")
    return False


def set_finance_window_mode(chat_id: int, mode: str, *, persist_now: bool = False):
    chat_id = int(chat_id)
    mode = str(mode or "off").lower().strip()
    if mode not in {"off", "normal", "open", "first"}:
        mode = "off"
    store = get_chat_store(chat_id)
    settings = store.setdefault("settings", {})
    state = _finance_window_state(chat_id)
    state["mode"] = mode
    state["auto_reopen_on_boot"] = bool(mode != "off")
    state["updated_at"] = now_local().isoformat(timespec="seconds")
    if mode == "normal":
        settings["quick_balance_enabled"] = False
        settings["quick_balance_behavior"] = "normal"
        settings["quick_balance_user_selected"] = True
    elif mode in {"open", "first"}:
        settings["quick_balance_enabled"] = True
        settings["quick_balance_behavior"] = mode
        settings["quick_balance_user_selected"] = True
    else:
        settings["quick_balance_enabled"] = False
        settings["quick_balance_behavior"] = "normal"
        settings["quick_balance_user_selected"] = True
    store["finance_window_state"] = state
    save_data(data, chat_ids=[chat_id])
    if persist_now:
        _persist_finance_window_mode_critical(chat_id)
    else:
        schedule_config_backup_for_chats(chat_id)


def delete_auto_finance_windows_for_chat(chat_id: int, *, persist_now: bool = False) -> int:
    """Delete only automatic finance windows controlled by the three F39 modes, not manual reports/F91/category views."""
    chat_id = int(chat_id)
    store = get_chat_store(chat_id)
    ids = set()
    try:
        ids.update(int(v) for v in (get_or_create_active_windows(chat_id) or {}).values() if v)
    except Exception:
        pass
    try:
        if store.get("balance_panel_id"):
            ids.add(int(store.get("balance_panel_id")))
    except Exception:
        pass
    removed = 0
    for mid in sorted(ids):
        try:
            bot.delete_message(chat_id, mid)
            removed += 1
        except Exception:
            pass
        try:
            unregister_open_window(chat_id, mid)
        except Exception:
            pass
    data.setdefault("active_messages", {})[str(chat_id)] = {}
    store["balance_panel_id"] = None
    store["balance_panel_mode"] = "mini"
    store["main_window_msg_count"] = 0
    store["balance_panel_msg_count"] = 0
    state = _finance_window_state(chat_id)
    state["main_windows"] = {}
    state["balance_panel_id"] = None
    state["balance_panel_mode"] = "mini"
    state["auto_reopen_on_boot"] = False if finance_window_mode(chat_id) == "off" else state.get("auto_reopen_on_boot", True)
    state["updated_at"] = now_local().isoformat(timespec="seconds")
    save_data(data, chat_ids=[chat_id])
    if persist_now:
        _persist_finance_window_mode_critical(chat_id)
    else:
        try:
            schedule_quick_backup(chat_id, 0.5)
        except Exception:
            pass
    return removed


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


def _normalize_excel_table_style(value) -> str:
    raw = str(value or "").strip().lower()
    aliases = {
        "new": "new_notes",
        "notes": "new_notes",
        "note": "new_notes",
        "comments": "new_comments",
        "comment": "new_comments",
        "google": "google_notes",
        "sheets": "google_notes",
        "google_sheets": "google_notes",
        "google_notes": "google_notes",
    }
    mode = aliases.get(raw, raw)
    return mode if mode in {"old", "new_comments", "new_notes", "google_notes"} else ""


def excel_table_style(chat_id: int) -> str:
    """Глобальный формат: old / new_comments / new_notes / google_notes.

    v124 хранил выбор отдельно в каждом чате. Из-за этого владелец мог выбрать
    «Примечания» в INFO, а экспорт другого целевого чата всё ещё создавался в
    его старом режиме «Комментарии». v125 хранит единственный глобальный выбор.
    """
    gs = data.setdefault("_global_settings", {})
    mode = _normalize_excel_table_style(gs.get("excel_table_style_global"))
    if not mode:
        # One-time migration: owner/global choice wins over a stale per-target chat setting.
        candidates = [gs.get("excel_table_style")]
        try:
            if OWNER_ID:
                candidates.append(get_chat_store(int(OWNER_ID)).setdefault("settings", {}).get("excel_table_style"))
        except Exception:
            pass
        try:
            candidates.append(get_chat_store(int(chat_id)).setdefault("settings", {}).get("excel_table_style"))
        except Exception:
            pass
        mode = next((_normalize_excel_table_style(v) for v in candidates if _normalize_excel_table_style(v)), "new_notes")
        gs["excel_table_style_global"] = mode
        gs["excel_table_style"] = mode
    return mode


def set_excel_table_style(chat_id: int, mode: str) -> str:
    chat_id = int(chat_id)
    mode = _normalize_excel_table_style(mode) or "new_notes"
    gs = data.setdefault("_global_settings", {})
    gs["excel_table_style_global"] = mode
    gs["excel_table_style"] = mode  # rollback compatibility mirror

    # Current v125 reads only the global switch. Mirror just the owner/control chats
    # for rollback compatibility; do not rewrite every finance chat merely to change UI.
    touched = []
    for cid in (chat_id, int(OWNER_ID or 0)):
        if not cid or cid in touched:
            continue
        try:
            get_chat_store(cid).setdefault("settings", {})["excel_table_style"] = mode
            touched.append(cid)
        except Exception:
            pass
    save_data(data, chat_ids=touched or None, root_only=not bool(touched))
    try:
        schedule_config_backup_for_chats(*(touched or [chat_id]), delay=1.0)
    except Exception:
        pass
    return mode


def toggle_excel_table_style(chat_id: int) -> str:
    """Compatibility helper: cycle OLD -> Comments -> Notes -> OLD."""
    order = ["old", "new_comments", "new_notes", "google_notes"]
    current = excel_table_style(chat_id)
    try:
        next_mode = order[(order.index(current) + 1) % len(order)]
    except Exception:
        next_mode = "new_notes"
    return set_excel_table_style(chat_id, next_mode)


def excel_table_style_caption(chat_id: int) -> str:
    mode = excel_table_style(chat_id)
    if mode == "old":
        return "СТАРОЕ"
    if mode == "new_comments":
        return "НОВОЕ • КОММЕНТАРИИ"
    if mode == "google_notes":
        return "GOOGLE SHEETS • ПРИМЕЧАНИЯ"
    return "НОВОЕ • ПРИМЕЧАНИЯ"


def excel_annotation_mode(chat_id: int) -> str | None:
    mode = excel_table_style(chat_id)
    if mode == "new_comments":
        return "comments"
    if mode in {"new_notes", "google_notes"}:
        return "notes"
    return None


def excel_table_style_label(chat_id: int) -> str:
    return "📊 Excel"


def build_excel_style_text(chat_id: int) -> str:
    return wm_owner(
        "📊 Excel\n\n"
        "Формат единый для ВСЕХ XLSX-файлов и всех чатов.\n"
        "• Старая — прежняя простая таблица.\n"
        "• Новая в комментариях — цветная таблица + современные Excel Comments.\n"
        "• Новая в примечаниях — цветная XLSX + классические Excel Notes.\n"
        "• Google Sheets в примечаниях — Excel статьи создаётся прямо в Google Sheets через API; описание записывается в нативное поле Примечание.\n\n"
        f"Сейчас: {excel_table_style_caption(chat_id)}",
        9,
    )


def build_excel_style_keyboard(chat_id: int):
    mode = excel_table_style(chat_id)
    kb = types.InlineKeyboardMarkup(row_width=1)
    choices = [
        ("old", "Старая"),
        ("new_comments", "Новая в комментариях"),
        ("new_notes", "Новая в примечаниях (XLSX)"),
        ("google_notes", "Google Sheets в примечаниях"),
    ]
    for value, label in choices:
        mark = "✅" if mode == value else "⬜"
        kb.row(IB(f"{mark} {label}", callback_data=f"excel_style_set:{value}"))
    kb.row(IB("🔙 Назад в Инфо", callback_data="journal_back"))
    return kb


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
    """v108: hidden finance is independent from the three automatic finance-window modes."""
    chat_id = int(chat_id)
    store = get_chat_store(chat_id)
    settings = store.setdefault("settings", {})
    settings["hidden_finance"] = bool(enabled)
    if enabled:
        # Hidden means silent accounting/confirmations. It must NOT erase or disable a separately selected window mode.
        set_finance_mode(chat_id, True)
    save_data(data, chat_ids=[chat_id])
    schedule_config_backup_for_chats(chat_id)


def force_recreate_balance_panel(chat_id: int):
    """Пересоздаёт быстрый остаток, чтобы он снова стал последним окном в чате."""
    if finance_window_mode(chat_id) not in {"open", "first"}:
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
    """Как обычно: отдельный выбранный режим; hidden finance does not disable it."""
    try:
        return bool(is_finance_mode(chat_id) and finance_window_mode(chat_id) == "normal")
    except Exception:
        return False


def schedule_main_window_recreate_after_quiet(chat_id: int, delay: float = 4.0):
    try:
        chat_id = int(chat_id)
    except Exception:
        return
    if not is_finance_mode(chat_id) or finance_window_mode(chat_id) != "normal":
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
        if not is_finance_mode(chat_id) or finance_window_mode(chat_id) == "off":
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
    if finance_window_mode(chat_id) != "first":
        return
    if not is_finance_mode(chat_id) or not is_quick_balance_enabled(chat_id):
        return
    if get_quick_balance_behavior(chat_id) != "first":
        return

    def _job():
        try:
            with locked_chat(chat_id):
                if finance_window_mode(chat_id) != "first":
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
    if finance_window_mode(chat_id) not in {"open", "first"}:
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
    _sync_finance_window_state_from_runtime(chat_id, schedule_delta=True)
    schedule_balance_panel_collapse(chat_id)

def schedule_owner_total_window_delete(chat_id: int, message_id: int, delay: int | float | None = None):
    """
    v104 compatibility shim. Окно «Общий итог» больше не удаляется по отдельному
    таймеру: как и остальные обычные окна, по единому глобальному таймеру оно
    возвращается в основное окно. Секретные режимы этой логикой не затрагиваются.
    """
    key = int(chat_id)
    if delay is None:
        delay = internal_timer_seconds("window_auto_return", 120)
    try:
        # Старый отдельный scheduler-key мог остаться в памяти после обновления кода.
        DELAYED_SCHEDULER.cancel(f"owner-total-delete:{key}")
    except Exception:
        pass
    schedule_stored_window_delete(chat_id, "total_msg_id", float(delay))
    _total_message_timers[key] = _aux_window_timers.get((key, "total_msg_id"))


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


def schedule_stored_window_delete(chat_id: int, store_key: str, delay: int | float | None = None):
    key = (int(chat_id), str(store_key))
    if delay is None:
        delay = internal_timer_seconds("window_auto_return", 120)

    def _job():
        try:
            store = get_chat_store(chat_id)
            message_id = store.get(store_key)
            if not message_id:
                return
            if store.get(store_key) == message_id:
                store[store_key] = None
                unregister_open_window(chat_id, int(message_id))
                _aux_window_timers.pop(key, None)
                save_data(data)
            day_key = store.get("current_view_day") or today_key()
            return_to_main_window_closing_previous(chat_id, day_key, int(message_id))
        except Exception as e:
            log_error(f"schedule_stored_window_delete({chat_id},{store_key}): {e}")

    scheduler_key = f"stored-window-delete:{int(chat_id)}:{str(store_key)}"
    DELAYED_SCHEDULER.cancel(scheduler_key)
    deadline = DELAYED_SCHEDULER.schedule(scheduler_key, float(delay), _job)
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


def send_or_edit_stored_window(chat_id: int, store_key: str, text: str, reply_markup=None, parse_mode=None, delay: int | float | None = None):
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
        delete_message_later(msg.chat.id, msg.message_id, internal_timer_seconds("command_cleanup", COMMAND_DELETE_DELAY))
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
        # v108: hidden accounting no longer disables a deliberately selected visible finance window.
        # Its buttons (including Ф91 Close / quick-balance open-collapse / edit/report) must remain usable.
        if finance_window_mode(chat_id) in {"normal", "open", "first"}:
            return False
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
            "/runtime_export — скачать Runtime/Watcher файлы из MEGA одним ZIP",
            "/articles — описание статей: статья = ключевые слова",
            "/mega_status — статус MEGA/MEGAcmd",
            "/mega_backup_now — безопасно загрузить latest_global.json в MEGA",
            "/mega_restore_now — принудительно полностью восстановить данные из MEGA",
            "/restore_guard — статус аварийной защиты восстановления",
            "/restore_guard_off — отключить guard и разрешить MEGA автобэкап",
            "/restore_guard_on — вернуть автоматическую защиту guard",
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
    identity = f"🤖 {BOT_DISPLAY_NAME} | {version_animal_badge()} | {VERSION}"
    lines = [
        identity,
        "ℹ️ INFO",
        "",
        f"Финансы: {'ВКЛ' if is_finance_mode(chat_id) else 'ВЫКЛ'}",
        f"Текущее окно: {'ВКЛ' if chat_buttons_current_window_enabled(chat_id) else 'ВЫКЛ'}",
        f"Журнал чата: {'ВКЛ' if is_chat_journal_enabled(chat_id) else 'ВЫКЛ'}",
    ]
    if layout in {"v84", "v85", "v86", "v87"}:
        lines.append(f"Финансы-кнопки: {'ВКЛ' if main_financial_value_buttons_enabled(chat_id) else 'ВЫКЛ'}")
    if _v85_enabled("gomonk_wallets"):
        lines.append(f"Гомонковые: {'ВКЛ' if gomonk_enabled(chat_id) else 'ВЫКЛ'}")
    if layout in {"v86", "v87"}:
        lines.append(f"Валюта: {currency_mode(chat_id).upper().replace('_', '-')}")
        lines.append(f"Подпись «ост:»: {'ВКЛ' if remaining_ost_label_enabled(chat_id) else 'ВЫКЛ'}")
    if version_mode_feature("forward_copy_edit"):
        lines.append(f"💰Перес: {forward_copy_edit_mode(chat_id).replace('normal', 'обычно').replace('button', 'кнопка').replace('slash', 'слеш')}")
    if is_owner_chat(chat_id):
        lines.extend([
            f"Кнопки интерфейса: {'значки' if icon_button_mode_enabled(chat_id) else 'текст'}",
            f"Restore guard: {'ВКЛ' if RESTORE_GUARD_ACTIVE else 'ВЫКЛ'}",
            f"Guard override: {'ВКЛ' if restore_guard_manual_override_enabled() else 'ВЫКЛ'}",
            f"Автобэкап MEGA: {'РАЗРЕШЁН' if not RESTORE_GUARD_ACTIVE else 'ЗАБЛОКИРОВАН'}",
            f"Маска секрета: {'ВКЛ' if total_secret_mask_enabled(chat_id) else 'ВЫКЛ'}",
            f"Финансовые сутки: с {finance_day_start_label(chat_id)}",
            f"Диспетчер: pending {UPDATE_DISPATCHER.stats().get('pending', 0)}",
            f"Таймер ввода: {_format_duration_short(internal_timer_seconds('input_wait'))}; окна: {_format_duration_short(internal_timer_seconds('window_auto_return'))}",
            f"Excel все файлы: {excel_table_style_caption(chat_id)}",
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
            "/mega_restore_now — вручную полностью обновить данные из MEGA",
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
    lines.extend(["", "Нажмите нужную кнопку ниже. Полное описание — «📘 Инструкция».", "", identity])
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
        _sync_finance_window_state_from_runtime(chat_id, schedule_delta=True)
    except Exception as e:
        err = str(e).lower()
        if "message is not modified" not in err:
            log_error(f"collapse_balance_panel({chat_id}): {e}")


def schedule_balance_panel_collapse(chat_id: int, delay: float | None = None):
    if delay is None:
        delay = internal_timer_seconds("balance_collapse", BALANCE_PANEL_COLLAPSE_DELAY)
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
    if finance_window_mode(chat_id) not in {"open", "first"}:
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
            _sync_finance_window_state_from_runtime(chat_id, schedule_delta=True)
            return
        except Exception as e:
            err = str(e).lower()
            if "message is not modified" in err:
                store["balance_panel_mode"] = "mini"
                save_data(data)
                _sync_finance_window_state_from_runtime(chat_id, schedule_delta=True)
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
        _finance_window_state(chat_id)["auto_reopen_on_boot"] = True
        _sync_finance_window_state_from_runtime(chat_id, schedule_delta=True)
    except Exception as e:
        log_error(f"send_minimized_balance_panel({chat_id}): {e}")


def refresh_balance_panel_now(chat_id: int):
    if finance_window_mode(chat_id) not in {"open", "first"}:
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
        _sync_finance_window_state_from_runtime(chat_id, schedule_delta=True)
        send_minimized_balance_panel(chat_id)


def schedule_balance_panel_refresh(chat_id: int, delay: float = BALANCE_PANEL_REFRESH_DELAY):
    if finance_window_mode(chat_id) not in {"open", "first"}:
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
    if finance_window_mode(chat_id) not in {"open", "first"}:
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
# v130_modular_split
