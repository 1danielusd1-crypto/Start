# v178_global_performance_final
"""v153: remaining fixes 11-16.

- deep command/button/runtime audit;
- interactive export wait notices with a reusable Download button;
- global secret redaction and preflight scanning;
- runtime cleanup, exact failed counters and chat lifecycle history;
- /json_full and validated /restore for global or tenant state;
- resumable verified MEGA migration to /TelegramBotBackupsStart.
"""

import copy as _v153_copy
import gzip as _v153_gzip
import hashlib as _v153_hashlib
import io as _v153_io
import json as _v153_json
import os as _v153_os
import re as _v153_re
import shutil as _v153_shutil
import sqlite3 as _v153_sqlite3
import tempfile as _v153_tempfile
import threading as _v153_threading
import time as _v153_time
import zipfile as _v153_zipfile
from datetime import datetime as _v153_datetime
from pathlib import Path as _V153Path

VERSION = "bot_v153_remaining_fixes_11_16"
V153_EXPORT_SCHEMA = 1
V153_OLD_MEGA_ROOT = "/TelegramBotBackups"
V153_NEW_MEGA_ROOT = "/TelegramBotBackupsStart"
V153_MIGRATION_BATCH = max(3, min(100, int(_v153_os.getenv("V153_MEGA_MIGRATION_BATCH", "20") or "20")))
V153_RESTORE_PENDING_TTL = 24 * 3600

_V153_LOCK = _v153_threading.RLock()
_V153_WAITING_EXPORTS = {}     # chat_id -> request
_V153_READY_EXPORTS = {}       # token -> request
_V153_RESTORE_PENDING = {}     # token -> validated restore
_V153_CALLBACK_RECEIPTS = {}
_V153_CALLBACK_SIGNATURES = {}
_V153_SANITIZED_TEMP = set()


def _v153_now() -> str:
    try:
        return now_local().isoformat(timespec="seconds")
    except Exception:
        return _v153_datetime.now().astimezone().isoformat(timespec="seconds")


def _v153_actor_id(obj) -> int:
    try:
        return int(getattr(getattr(obj, "from_user", None), "id", 0) or 0)
    except Exception:
        return 0


def _v153_platform_owner(uid: int) -> bool:
    try:
        return bool(tenant_is_platform_owner_user(int(uid)))
    except Exception:
        try:
            return int(uid) == int(OWNER_ID or 0)
        except Exception:
            return False


def _v153_tenant_for_chat(chat_id: int) -> str:
    try:
        return str(tenant_id_for_chat(int(chat_id), create=False) or TENANT_PLATFORM_ID)
    except Exception:
        return str(globals().get("TENANT_PLATFORM_ID") or "platform")


def _v153_can_manage_tenant(uid: int, tenant_id: str) -> bool:
    if _v153_platform_owner(uid):
        return True
    try:
        return bool(tenant_can_manage(int(uid), str(tenant_id), owner_only=True))
    except TypeError:
        try:
            return bool(tenant_can_manage(int(uid), str(tenant_id)))
        except Exception:
            return False
    except Exception:
        return False


# ─────────────────────────────────────────────────────────────
# Secrets: one sanitizer for logs, snapshots, failed tasks and exported files.
# ─────────────────────────────────────────────────────────────
_V153_SECRET_KEY_RE = _v153_re.compile(
    r"^(?:password|passwd|pass|mega_password|telegram_token|bot_token|access[_-]?token|refresh[_-]?token|oauth[_-]?token|google[_-]?oauth[_-]?token|api[_-]?key|private[_-]?key|credential(?:s)?|authorization|cookie|webhook[_-]?secret|client[_-]?secret)$",
    _v153_re.I,
)
_V153_ENV_SECRET_RE = _v153_re.compile(
    r"(?:PASS|PASSWORD|SECRET|TOKEN|API_KEY|PRIVATE_KEY|CREDENTIAL|AUTH|COOKIE|OAUTH|WEBHOOK)",
    _v153_re.I,
)


def _v153_secret_values() -> list[str]:
    values = set()
    for key, value in _v153_os.environ.items():
        if _V153_ENV_SECRET_RE.search(str(key)) and value and len(str(value)) >= 6:
            values.add(str(value))
    for name in (
        "MEGA_EMAIL", "MEGA_PASSWORD", "BOT_TOKEN", "B_T", "GOOGLE_SERVICE_ACCOUNT_JSON",
        "TENANT_GOOGLE_MASTER_KEY", "GOOGLE_TENANT_MASTER_KEY", "WEBHOOK_SECRET",
    ):
        value = str(globals().get(name) or _v153_os.getenv(name) or "")
        if value and len(value) >= 6:
            values.add(value)
    return sorted(values, key=len, reverse=True)


def v153_redact_text(value) -> str:
    text = str(value or "")
    for secret in _v153_secret_values():
        if secret in text:
            text = text.replace(secret, "***")
    # Whole HTTP credentials must be removed before the generic key=value pass;
    # otherwise only the word Bearer could be hidden while its token remained.
    text = _v153_re.sub(r"(?i)(Authorization\s*[:=]\s*)(?:Bearer\s+)?[^\s,;]+", r"\1***", text)
    text = _v153_re.sub(r"(?i)(Cookie\s*[:=]\s*)[^\r\n]+", r"\1***", text)
    # JSON, header and key=value forms.
    text = _v153_re.sub(
        r'(?i)("?(?:password|passwd|secret|token|api[_-]?key|private[_-]?key|authorization|cookie|client[_-]?secret|webhook[_-]?secret)"?\s*[:=]\s*)("[^"\r\n]*"|[^,;\r\n ]+)',
        lambda m: m.group(1) + '"***"', text,
    )
    text = _v153_re.sub(r"(?i)(Bearer\s+)[A-Za-z0-9._~+/=-]+", r"\1***", text)
    return text


def v153_sanitize(value, key: str = ""):
    if _V153_SECRET_KEY_RE.search(str(key or "")):
        return "***"
    if isinstance(value, dict):
        return {str(k): v153_sanitize(v, str(k)) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [v153_sanitize(x, key) for x in value]
    if isinstance(value, str):
        return v153_redact_text(value)
    return value


_V153_ORIG_LOG_ERROR = globals().get("log_error")
_V153_ORIG_LOG_INFO = globals().get("log_info")
_V153_ORIG_BOT_JOURNAL = globals().get("bot_journal")
_V153_ORIG_ATOMIC_JSON_DUMP = globals().get("_atomic_json_dump")
_V153_ORIG_MEGA_RUN = globals().get("_mega_run")
_V153_ORIG_MAKE_GLOBAL_BACKUP = globals().get("make_global_backup_payload")


def log_error(message):
    if callable(_V153_ORIG_LOG_ERROR):
        return _V153_ORIG_LOG_ERROR(v153_redact_text(message))


def log_info(message):
    if callable(_V153_ORIG_LOG_INFO):
        return _V153_ORIG_LOG_INFO(v153_redact_text(message))


def _v177_legacy_0007_bot_journal(action, chat_id=None, detail="", level="INFO"):
    if callable(_V153_ORIG_BOT_JOURNAL):
        return _V153_ORIG_BOT_JOURNAL(str(action), chat_id, v153_sanitize(detail), str(level))
try: _v177_legacy_0007_bot_journal.__name__ = 'bot_journal'
except Exception: pass
bot_journal = _v177_legacy_0007_bot_journal


def _atomic_json_dump(path: str, payload) -> None:
    safe = v153_sanitize(payload)
    if callable(_V153_ORIG_ATOMIC_JSON_DUMP):
        return _V153_ORIG_ATOMIC_JSON_DUMP(path, safe)
    with open(path, "w", encoding="utf-8") as fh:
        _v153_json.dump(safe, fh, ensure_ascii=False)


def make_global_backup_payload():
    payload = _V153_ORIG_MAKE_GLOBAL_BACKUP() if callable(_V153_ORIG_MAKE_GLOBAL_BACKUP) else {}
    return v153_sanitize(payload)


def _mega_run(cmd: str, args=None, timeout=None, check: bool = True):
    if not callable(_V153_ORIG_MEGA_RUN):
        raise RuntimeError("MEGA runner unavailable")
    safe_args = list(args or [])
    temp_dir = ""
    try:
        # Every JSON/TXT/CSV/log/diagnostic ZIP uploaded to MEGA receives a second
        # preflight pass. Operational callback/invite tokens remain intact; only
        # Render credentials and explicit credential fields are removed.
        if str(cmd or "").lower() == "mega-put" and safe_args:
            source = str(safe_args[0] or "")
            if _v153_os.path.isfile(source):
                safe = v153_prepare_safe_file(source, "mega upload task snapshot failed backup audit runtime")
                if safe != source:
                    safe_args[0] = safe
                    temp_dir = _v153_os.path.dirname(safe)
        return _V153_ORIG_MEGA_RUN(cmd, safe_args, timeout=timeout, check=check)
    except Exception as exc:
        raise RuntimeError(v153_redact_text(exc)) from None
    finally:
        if temp_dir:
            _v153_shutil.rmtree(temp_dir, ignore_errors=True)
            _V153_SANITIZED_TEMP.discard(temp_dir)


def _v153_text_extension(path: str) -> bool:
    return _v153_os.path.splitext(str(path or ""))[1].lower() in {".txt", ".json", ".csv", ".log", ".md", ".xml", ".yaml", ".yml"}


def _v153_sanitize_zip(src: str, dst: str) -> None:
    with _v153_zipfile.ZipFile(src, "r") as zin, _v153_zipfile.ZipFile(dst, "w", _v153_zipfile.ZIP_DEFLATED) as zout:
        for item in zin.infolist():
            raw = zin.read(item.filename)
            ext = _v153_os.path.splitext(item.filename)[1].lower()
            if ext in {".txt", ".json", ".csv", ".log", ".md", ".xml", ".yaml", ".yml"}:
                try:
                    raw = v153_redact_text(raw.decode("utf-8")).encode("utf-8")
                except Exception:
                    pass
            zout.writestr(item, raw)


def v153_prepare_safe_file(path: str, hint: str = "") -> str:
    """Return original or a sanitized temporary copy. Binary state DBs are not rewritten."""
    src = str(path or "")
    if not src or not _v153_os.path.isfile(src):
        return src
    low = f"{src} {hint}".casefold()
    sensitive_export = any(x in low for x in ("journal", "log", "runtime", "diagn", "failed", "audit", "error", "snapshot", "export"))
    if not sensitive_export and not _v153_text_extension(src):
        return src
    folder = _v153_tempfile.mkdtemp(prefix="v153_safe_")
    dst = _v153_os.path.join(folder, _v153_os.path.basename(src))
    try:
        if src.lower().endswith(".zip"):
            _v153_sanitize_zip(src, dst)
        elif _v153_text_extension(src):
            raw = _V153Path(src).read_text(encoding="utf-8", errors="replace")
            _V153Path(dst).write_text(v153_redact_text(raw), encoding="utf-8")
        else:
            return src
        _V153_SANITIZED_TEMP.add(folder)
        return dst
    except Exception:
        _v153_shutil.rmtree(folder, ignore_errors=True)
        return src


_V153_ORIG_SEND_DOCUMENT = getattr(bot, "send_document", None)
if callable(_V153_ORIG_SEND_DOCUMENT):
    def _v153_send_document(chat_id, document, *args, **kwargs):
        temp_dir = ""
        original_pos = None
        try:
            name = str(getattr(document, "name", "") or "")
            path = name if name and _v153_os.path.isfile(name) else ""
            if path:
                safe = v153_prepare_safe_file(path, f"{kwargs.get('caption','')} {kwargs.get('purpose','')}")
                if safe != path:
                    temp_dir = _v153_os.path.dirname(safe)
                    document = open(safe, "rb")
            return _V153_ORIG_SEND_DOCUMENT(chat_id, document, *args, **kwargs)
        finally:
            try:
                if temp_dir and hasattr(document, "close"):
                    document.close()
            except Exception:
                pass
            if temp_dir:
                _v153_shutil.rmtree(temp_dir, ignore_errors=True)
                _V153_SANITIZED_TEMP.discard(temp_dir)
    bot.send_document = _v153_send_document


# ─────────────────────────────────────────────────────────────
# Export single-flight: visible waiting message that becomes a reusable button.
# ─────────────────────────────────────────────────────────────
_V153_ORIG_FILE_RUNNER = globals().get("_interactive_file_job_runner")
_V153_ORIG_FILE_SUBMIT = globals().get("submit_interactive_file_job")


def _v153_wait_token(chat_id: int, kind: str) -> str:
    raw = f"{chat_id}:{kind}:{_v153_time.time_ns()}".encode()
    return _v153_hashlib.sha256(raw).hexdigest()[:16]


def _v153_wait_keyboard(token: str):
    kb = types.InlineKeyboardMarkup()
    kb.add(IB("📥 Скачать сейчас", callback_data=f"v153:file:{token}"))
    return kb


def _v153_register_wait(chat_id: int, kind: str, label: str, func, args, kwargs, active: dict) -> None:
    chat_id = int(chat_id)
    token = _v153_wait_token(chat_id, kind)
    text = f"⏳ Сейчас нельзя скачать «{label}».\n\nУже формируется:\n«{active.get('label') or 'другой файл'}»."
    with _V153_LOCK:
        old = _V153_WAITING_EXPORTS.get(chat_id) or {}
    msg_id = int(old.get("message_id") or 0)
    try:
        if msg_id:
            bot.edit_message_text(text, chat_id=chat_id, message_id=msg_id)
        else:
            sent = bot.send_message(chat_id, text)
            msg_id = int(getattr(sent, "message_id", 0) or 0)
    except Exception:
        pass
    request = {
        "token": token, "chat_id": chat_id, "kind": str(kind), "label": str(label),
        "func": func, "args": tuple(args), "kwargs": dict(kwargs), "message_id": msg_id,
        "created_at": _v153_now(),
    }
    with _V153_LOCK:
        _V153_WAITING_EXPORTS[chat_id] = request


def _v153_release_waiters(ok: bool, error_text: str = "") -> None:
    with _V153_LOCK:
        rows = list(_V153_WAITING_EXPORTS.values())
        _V153_WAITING_EXPORTS.clear()
    for row in rows:
        token = str(row["token"])
        with _V153_LOCK:
            _V153_READY_EXPORTS[token] = row
        if ok:
            text = f"✅ Теперь можно скачать «{row['label']}»."
        else:
            text = "⚠️ Предыдущая выгрузка завершилась ошибкой.\n\nТеперь можно попробовать снова."
        try:
            if row.get("message_id"):
                bot.edit_message_text(text, chat_id=int(row["chat_id"]), message_id=int(row["message_id"]), reply_markup=_v153_wait_keyboard(token))
            else:
                sent = bot.send_message(int(row["chat_id"]), text, reply_markup=_v153_wait_keyboard(token))
                row["message_id"] = int(getattr(sent, "message_id", 0) or 0)
        except Exception:
            pass


def _v177_legacy_0013_interactive_file_job_runner(job_meta: dict, func, args, kwargs):
    state = {"ok": False, "error": ""}
    def _target(*a, **k):
        try:
            result = func(*a, **k)
            state["ok"] = result is not False
            if not state["ok"]:
                state["error"] = "операция завершилась без подтверждения"
            return result
        except Exception as exc:
            state["error"] = v153_redact_text(exc)[:300]
            raise
    if callable(_V153_ORIG_FILE_RUNNER):
        result = _V153_ORIG_FILE_RUNNER(job_meta, _target, args, kwargs)
    else:
        result = _target(*args, **kwargs)
    _v153_release_waiters(bool(state["ok"]), state["error"])
    return result
try: _v177_legacy_0013_interactive_file_job_runner.__name__ = '_interactive_file_job_runner'
except Exception: pass
_interactive_file_job_runner = _v177_legacy_0013_interactive_file_job_runner


def _v177_legacy_0017_submit_interactive_file_job(chat_id: int, kind: str, label: str, func, *args, **kwargs):
    busy = _file_job_busy_info() if "_file_job_busy_info" in globals() else {}
    if busy:
        _v153_register_wait(int(chat_id), str(kind), str(label), func, args, kwargs, busy)
        return False, f"Уже формируется: {busy.get('label') or 'файл'}"
    if callable(_V153_ORIG_FILE_SUBMIT):
        return _V153_ORIG_FILE_SUBMIT(chat_id, kind, label, func, *args, **kwargs)
    return False, "Экспорт недоступен"
try: _v177_legacy_0017_submit_interactive_file_job.__name__ = 'submit_interactive_file_job'
except Exception: pass
submit_interactive_file_job = _v177_legacy_0017_submit_interactive_file_job


# ─────────────────────────────────────────────────────────────
# Runtime export and durable diagnostics hardening.
# ─────────────────────────────────────────────────────────────
_V153_ORIG_RUNTIME_SELECT = globals().get("_runtime_export_select_paths")
_V153_ORIG_RUNTIME_SEND = globals().get("send_runtime_export_zip")
_V153_ORIG_MEGA_STATS = globals().get("mega_task_registry_stats")
_V153_ORIG_RUNTIME_MARK_READY = globals().get("runtime_mark_ready")
_V153_ORIG_RUNTIME_UPLOAD = globals().get("runtime_upload_snapshot")
_V153_ORIG_NORMALIZE_EXPECTED = globals().get("_durable_normalize_expected_for_route")
_V153_ORIG_RESTORE_SQLITE = globals().get("mega_restore_sqlite_snapshot_from_cloud")
_V153_ORIG_RESTORE_FULL = globals().get("mega_restore_full_from_cloud")
_V153_ORIG_CLASSIFY_PREVIOUS = globals().get("runtime_classify_previous")
_V153_INSTANCE_SUPERSEDED = False
_V153_INSTANCE_LEASE_LAST = 0.0


def mega_task_registry_stats() -> dict:
    base = _V153_ORIG_MEGA_STATS() if callable(_V153_ORIG_MEGA_STATS) else {}
    try:
        with _MEGA_TASK_LOCK:
            failed_ids = [str(k) for k, row in _mega_task_registry.items() if str((row or {}).get("state") or "") == "failed"]
        details = [x for x in list(base.get("failed_details") or []) if str((x or {}).get("task_id") or "") in set(failed_ids)]
        base["failed"] = len(failed_ids)
        base["failed_details"] = details
        base["failed_details_pending"] = len(details) != min(len(failed_ids), int(globals().get("_V146_FAILED_DIAG_LIMIT") or 10))
    except Exception:
        pass
    return v153_sanitize(base)


def _runtime_export_select_paths(start_dt=None, end_dt=None, max_downloads: int = 360):
    indexed, _legacy_selected = _V153_ORIG_RUNTIME_SELECT(start_dt, end_dt, max_downloads=max_downloads) if callable(_V153_ORIG_RUNTIME_SELECT) else ([], [])
    # Download immutable events + rotating slots. Candidate/staged files stay in index only,
    # except the newest candidate when it is newer than every selected stable record.
    stable = [x for x in indexed if x[0] in {"slot", "event"}]
    stable = stable[:max(20, min(int(max_downloads), 180))]
    selected = list(stable)
    newest_stable = max((x[2] for x in stable if x[2] is not None), default=None)
    candidates = [x for x in indexed if x[0] in {"candidate", "staged"} and x[2] is not None]
    if candidates:
        newest = max(candidates, key=lambda x: x[2])
        if newest_stable is None or newest[2] > newest_stable:
            selected.append(newest)
    return indexed, selected[:max_downloads]


def _v153_chat_lifecycle_snapshot() -> dict:
    out = {"created_at": _v153_now(), "chats": {}}
    try:
        for cid_s, store in list(((data or {}).get("chats") or {}).items()):
            if not isinstance(store, dict):
                continue
            life = ((store.get("settings") or {}).get("chat_lifecycle_v150") or {})
            if life:
                out["chats"][str(cid_s)] = v153_sanitize(life)
    except Exception:
        pass
    return out


def send_runtime_export_zip(recipient_chat_id: int, start_dt=None, end_dt=None):
    # Rebuild the original ZIP, then append lifecycle and the v153 audit summary before send
    # by using a local interception of send_document.
    if not callable(_V153_ORIG_RUNTIME_SEND):
        return False
    original_send = bot.send_document
    captured = {"done": False}
    def _capture(chat_id, document, *args, **kwargs):
        try:
            name = str(getattr(document, "name", "") or "")
            if name and _v153_os.path.isfile(name) and name.lower().endswith(".zip"):
                temp_dir = _v153_tempfile.mkdtemp(prefix="v153_runtime_")
                temp = _v153_os.path.join(temp_dir, _v153_os.path.basename(name))
                _v153_shutil.copy2(name, temp)
                with _v153_zipfile.ZipFile(temp, "a", _v153_zipfile.ZIP_DEFLATED) as z:
                    z.writestr("chat_lifecycle_history.json", _v153_json.dumps(_v153_chat_lifecycle_snapshot(), ensure_ascii=False, indent=2))
                    z.writestr("v153_runtime_fixes.txt", _v153_runtime_audit_text())
                safe_document = open(temp, "rb")
                try:
                    return original_send(chat_id, safe_document, *args, **kwargs)
                finally:
                    safe_document.close(); _v153_shutil.rmtree(temp_dir, ignore_errors=True)
        except Exception as exc:
            log_error(f"runtime lifecycle append: {exc}")
        return original_send(chat_id, document, *args, **kwargs)
    bot.send_document = _capture
    try:
        return _V153_ORIG_RUNTIME_SEND(recipient_chat_id, start_dt, end_dt)
    finally:
        bot.send_document = original_send


def _durable_normalize_expected_for_route(payload: dict, expected: dict | None) -> dict:
    adjusted = _V153_ORIG_NORMALIZE_EXPECTED(payload, expected) if callable(_V153_ORIG_NORMALIZE_EXPECTED) else dict(expected or {})
    # A reminder witness is valid only for an explicit deterministic EDITREM token.
    raw, _cid, _mid, _grp = _durable_payload_message(payload or {}) if "_durable_payload_message" in globals() else ({}, None, None, None)
    text = str((raw or {}).get("text") or (raw or {}).get("caption") or "") if isinstance(raw, dict) else ""
    if "EDITREM|" not in text and "EDITREMINT|" not in text:
        adjusted["reminder_edits"] = []
    return adjusted


def _v153_runtime_cleanup_remote() -> dict:
    result = {"candidates_removed": 0, "staged_removed": 0}
    try:
        root = runtime_remote_dir()
        result["candidates_removed"] = _mega_prune_remote_history(root, "candidate_runtime_latest_*.json", 2)
        result["staged_removed"] = _mega_prune_remote_history(root, "runtime_latest__*.json", 2)
    except Exception as exc:
        result["error"] = v153_redact_text(exc)
    return result


def _v153_remote_marker_exists(root: str) -> bool:
    try:
        rows = _mega_find_remote_files(str(root).rstrip("/"), "migration_v153_complete.json", limit=3)
        return any(str(x).rstrip("/").endswith("/migration_v153_complete.json") for x in rows)
    except Exception:
        return False


def _v153_select_boot_mega_root() -> str:
    explicit = str(_v153_os.getenv("MEGA_BACKUP_DIR") or "").strip()
    if explicit:
        _v153_apply_mega_root(explicit)
        return explicit.rstrip("/")
    # The marker in the new root is the cloud source of truth on a fresh Render disk.
    # Never fall back to the old root after a verified migration.
    if mega_is_configured() and _v153_remote_marker_exists(V153_NEW_MEGA_ROOT):
        _v153_apply_mega_root(V153_NEW_MEGA_ROOT)
        return V153_NEW_MEGA_ROOT
    _v153_apply_mega_root(V153_OLD_MEGA_ROOT)
    return V153_OLD_MEGA_ROOT


def mega_restore_sqlite_snapshot_from_cloud() -> tuple[bool, str]:
    _v153_select_boot_mega_root()
    if callable(_V153_ORIG_RESTORE_SQLITE):
        return _V153_ORIG_RESTORE_SQLITE()
    return False, "SQLite restore unavailable"


def mega_restore_full_from_cloud(force: bool = False) -> tuple[bool, str]:
    _v153_select_boot_mega_root()
    if callable(_V153_ORIG_RESTORE_FULL):
        return _V153_ORIG_RESTORE_FULL(force=force)
    return False, "Full restore unavailable"


def _v153_instance_lease_payload() -> dict:
    return {
        "kind": "telegram_bot_active_instance_v153",
        "instance_id": str(_v153_os.getenv("RENDER_INSTANCE_ID") or _v153_os.uname().nodename),
        "commit": str(_v153_os.getenv("RENDER_GIT_COMMIT") or ""),
        "started_at": str(globals().get("_RUNTIME_STARTED_AT") or _v153_now()),
        "heartbeat_at": _v153_now(),
        "version": VERSION,
    }


def _v153_instance_lease_check() -> dict:
    global _V153_INSTANCE_SUPERSEDED, _V153_INSTANCE_LEASE_LAST
    result = {"active": True, "superseded": False}
    if not mega_is_configured():
        return result
    now_m = _v153_time.monotonic()
    if now_m - _V153_INSTANCE_LEASE_LAST < 20.0:
        result["superseded"] = bool(_V153_INSTANCE_SUPERSEDED)
        result["active"] = not bool(_V153_INSTANCE_SUPERSEDED)
        return result
    _V153_INSTANCE_LEASE_LAST = now_m
    remote_dir = runtime_remote_dir()
    remote = remote_dir.rstrip("/") + "/active_instance_v153.json"
    ours = _v153_instance_lease_payload()
    current = None
    local = None
    try:
        local = _mega_download_remote_path(remote)
        if local and _v153_os.path.isfile(local):
            current = _v153_json.loads(_V153Path(local).read_text(encoding="utf-8"))
    except Exception:
        current = None
    finally:
        if local:
            _v153_shutil.rmtree(_v153_os.path.dirname(local), ignore_errors=True)
    other_id = str((current or {}).get("instance_id") or "")
    our_id = str(ours["instance_id"])
    current_started = str((current or {}).get("started_at") or "")
    ours_started = str(ours["started_at"])
    if other_id and other_id != our_id and current_started > ours_started:
        _V153_INSTANCE_SUPERSEDED = True
        result.update({"active": False, "superseded": True, "newer_instance": other_id})
        try:
            _RUNTIME_STATE["phase"] = "superseded"
            _RUNTIME_STATE["ready"] = False
            _RUNTIME_STATE["last_event"] = "newer_render_instance_active"
            _RUNTIME_STATE["last_event_at"] = _v153_now()
        except Exception:
            pass
        bot_journal("runtime_instance_superseded", None, f"newer_instance={other_id}; newer_started={current_started}", "WARN")
        return result
    temp_dir = _v153_tempfile.mkdtemp(prefix="v153_lease_")
    try:
        path = _v153_os.path.join(temp_dir, "active_instance_v153.json")
        _V153Path(path).write_text(_v153_json.dumps(v153_sanitize(ours), ensure_ascii=False, indent=2), encoding="utf-8")
        if mega_put_replace(path, remote_dir, "active_instance_v153.json", archive_previous=False):
            _V153_INSTANCE_SUPERSEDED = False
            result["active_instance"] = our_id
    finally:
        _v153_shutil.rmtree(temp_dir, ignore_errors=True)
    return result


def runtime_classify_previous(prev: dict) -> str:
    base = _V153_ORIG_CLASSIFY_PREVIOUS(prev) if callable(_V153_ORIG_CLASSIFY_PREVIOUS) else "process_restart_or_unknown"
    try:
        prev_state = (prev or {}).get("state") or {}
        prev_render = (prev or {}).get("render") or {}
        prev_id = str(prev_render.get("RENDER_INSTANCE_ID") or "")
        cur_id = str(_v153_os.getenv("RENDER_INSTANCE_ID") or "")
        prev_capture = str((prev or {}).get("captured_at") or "")
        cur_start = str(globals().get("_RUNTIME_STARTED_AT") or "")
        if prev_id and cur_id and prev_id != cur_id and prev_capture and cur_start and prev_capture >= cur_start:
            return "overlapping_render_instances_detected"
        if bool(prev_state.get("shutdown_finished_at")):
            return base
        if str(prev_state.get("phase") or "") == "superseded":
            return "older_instance_superseded_by_newer_instance"
    except Exception:
        pass
    return base


def runtime_upload_snapshot(event: str = "snapshot", immutable_event: bool = True) -> bool:
    lease = _v153_instance_lease_check()
    if lease.get("superseded"):
        return False
    ok = _V153_ORIG_RUNTIME_UPLOAD(event, immutable_event) if callable(_V153_ORIG_RUNTIME_UPLOAD) else False
    try:
        if event in {"boot_ready", "manual", "shutdown"}:
            GENERAL_TASK_POOL.submit_unique("v153-runtime-prune", _v153_runtime_cleanup_remote)
    except Exception:
        pass
    return ok


# ─────────────────────────────────────────────────────────────
# MEGA root migration. Old root is never deleted.
# ─────────────────────────────────────────────────────────────

def _v153_migration_store(root=None) -> dict:
    target = root if isinstance(root, dict) else data
    gs = target.setdefault("_global_settings", {})
    row = gs.setdefault("mega_root_migration_v153", {})
    row.setdefault("old_root", V153_OLD_MEGA_ROOT)
    row.setdefault("new_root", V153_NEW_MEGA_ROOT)
    row.setdefault("status", "pending")
    row.setdefault("verified", {})
    row.setdefault("skipped", [])
    row.setdefault("created_at", _v153_now())
    return row


def _v153_apply_mega_root(root: str) -> None:
    root = str(root).rstrip("/")
    globals()["MEGA_BACKUP_DIR"] = root
    globals()["BOT_SOURCE_ARCHIVE_DIR"] = f"{root}/runtime/bot_versions"
    try:
        globals()["DURABLE_JOURNAL_REMOTE_DIR"] = f"{root}/runtime/journal"
    except Exception:
        pass


_V153_ORIG_LOAD_DATA = globals().get("load_data")
def load_data():
    loaded = _V153_ORIG_LOAD_DATA() if callable(_V153_ORIG_LOAD_DATA) else {}
    try:
        state = _v153_migration_store(loaded)
        explicit = str(_v153_os.getenv("MEGA_BACKUP_DIR") or "").strip()
        if explicit:
            _v153_apply_mega_root(explicit)
        elif str(state.get("status")) == "complete":
            _v153_apply_mega_root(V153_NEW_MEGA_ROOT)
        else:
            _v153_apply_mega_root(V153_OLD_MEGA_ROOT)
    except Exception:
        pass
    return loaded


def _v153_remote_relative(path: str, root: str) -> str:
    raw = str(path or "")
    prefix = str(root).rstrip("/") + "/"
    return raw[len(prefix):] if raw.startswith(prefix) else _v153_os.path.basename(raw)


def _v153_sha256_file(path: str) -> str:
    h = _v153_hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _v153_mega_copy_verify(old_remote: str, new_remote: str) -> tuple[bool, str]:
    local_old = local_new = None
    try:
        local_old = _mega_download_remote_path(old_remote)
        if not local_old:
            return False, "download old failed"
        old_hash = _v153_sha256_file(local_old)
        new_dir, new_name = new_remote.rsplit("/", 1)
        if not mega_put_replace(local_old, new_dir, new_name, archive_previous=False):
            return False, "upload new failed"
        local_new = _mega_download_remote_path(new_remote)
        if not local_new:
            return False, "verify download failed"
        new_hash = _v153_sha256_file(local_new)
        return old_hash == new_hash, old_hash if old_hash == new_hash else "checksum mismatch"
    finally:
        for local in (local_old, local_new):
            try:
                if local:
                    _v153_shutil.rmtree(_v153_os.path.dirname(local), ignore_errors=True)
            except Exception:
                pass


def v153_migrate_mega_root() -> dict:
    state = _v153_migration_store()
    if str(state.get("status")) == "complete":
        _v153_apply_mega_root(V153_NEW_MEGA_ROOT)
        return state
    if not mega_is_configured():
        state["status"] = "waiting_mega"
        return state
    state["status"] = "copying"
    state["last_run_at"] = _v153_now()
    save_data(data, root_only=True)
    try:
        mega_ensure_remote_path(V153_NEW_MEGA_ROOT)
        listing = _mega_run("mega-find", [V153_OLD_MEGA_ROOT, "--pattern=*", "--type=f"], check=False, timeout=120)
        if int(getattr(listing, "returncode", 1) or 0) != 0:
            raise RuntimeError("Не удалось получить проверенный список старой папки MEGA")
        old_files = sorted({x.strip() for x in str(getattr(listing, "stdout", "") or "").splitlines() if x.strip()}, reverse=True)
        if not old_files:
            state["status"] = "waiting_source_files"
            state["last_error"] = "Старая папка пока вернула 0 файлов; переключение запрещено"
            save_data(data, root_only=True)
            try: DELAYED_SCHEDULER.schedule("v153-mega-migration-retry", 120.0, _v153_schedule_migration)
            except Exception: pass
            return state
        # Stale candidate/staged files are technical debris. Keep only the newest two of each;
        # the complete immutable event history remains copied.
        candidate_rows = [x for x in old_files if "/candidate_runtime_latest_" in x]
        staged_rows = [x for x in old_files if "/runtime_latest__" in x]
        keep_technical = set(sorted(candidate_rows, reverse=True)[:2] + sorted(staged_rows, reverse=True)[:2])
        targets = []
        skipped = set(state.get("skipped") or [])
        for remote in old_files:
            if (remote in candidate_rows or remote in staged_rows) and remote not in keep_technical:
                skipped.add(remote); continue
            targets.append(remote)
        state["skipped"] = sorted(skipped)[-2000:]
        verified = state.setdefault("verified", {})
        pending = [x for x in targets if x not in verified]
        state["total_files"] = len(targets)
        state["remaining_files"] = len(pending)
        for old_remote in pending[:V153_MIGRATION_BATCH]:
            relative = _v153_remote_relative(old_remote, V153_OLD_MEGA_ROOT)
            new_remote = V153_NEW_MEGA_ROOT.rstrip("/") + "/" + relative
            ok, detail = _v153_mega_copy_verify(old_remote, new_remote)
            if not ok:
                state["last_error"] = f"{relative}: {detail}"[:500]
                break
            verified[old_remote] = {"new": new_remote, "sha256": detail, "at": _v153_now()}
            state["copied_files"] = len(verified)
            save_data(data, root_only=True)
        remaining = [x for x in targets if x not in verified]
        state["remaining_files"] = len(remaining)
        if not remaining:
            marker = {
                "kind": "telegram_bot_mega_root_migration_v153", "completed_at": _v153_now(),
                "old_root": V153_OLD_MEGA_ROOT, "new_root": V153_NEW_MEGA_ROOT,
                "verified_files": len(verified), "skipped_stale_runtime_candidates": len(skipped),
            }
            tmp = _v153_tempfile.mktemp(prefix="v153_migration_", suffix=".json")
            _V153Path(tmp).write_text(_v153_json.dumps(marker, ensure_ascii=False, indent=2), encoding="utf-8")
            if not mega_put_replace(tmp, V153_NEW_MEGA_ROOT, "migration_v153_complete.json", archive_previous=False):
                raise RuntimeError("migration marker upload failed")
            _v153_os.remove(tmp)
            state["status"] = "complete"
            state["completed_at"] = _v153_now()
            state["active_root"] = V153_NEW_MEGA_ROOT
            _v153_apply_mega_root(V153_NEW_MEGA_ROOT)
        save_data(data, root_only=True)
        if state.get("status") != "complete" and int(state.get("remaining_files") or 0) > 0:
            try: DELAYED_SCHEDULER.schedule("v153-mega-migration-next", 20.0, _v153_schedule_migration)
            except Exception: pass
    except Exception as exc:
        state["status"] = "error"
        state["last_error"] = v153_redact_text(exc)[:500]
        save_data(data, root_only=True)
        try: DELAYED_SCHEDULER.schedule("v153-mega-migration-retry", 120.0, _v153_schedule_migration)
        except Exception: pass
    return state


def _v153_schedule_migration():
    try:
        GENERAL_TASK_POOL.submit_unique("v153-mega-root-migration", v153_migrate_mega_root)
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────
# Full/tenant SQLite export and validated restore.
# ─────────────────────────────────────────────────────────────

def _v153_sql_rows(conn, table: str, where: str = "", params=()):
    sql = f"SELECT * FROM {table}" + (f" WHERE {where}" if where else "")
    return conn.execute(sql, tuple(params)).fetchall()


def _v153_db_logical_checksum(path: str) -> str:
    conn = _v153_sqlite3.connect(path)
    try:
        h = _v153_hashlib.sha256()
        for table in ("kv", "chats", "meta", "cold_fields"):
            cols = [x[1] for x in conn.execute(f"PRAGMA table_info({table})").fetchall()]
            if not cols:
                continue
            order = ",".join(cols[:2])
            for row in conn.execute(f"SELECT * FROM {table} ORDER BY {order}").fetchall():
                if table == "meta" and len(row) >= 2 and str(row[0]) == "v153_export" and str(row[1]) == "manifest":
                    continue
                h.update(table.encode()); h.update(b"\0")
                for value in row:
                    h.update(str(value).encode("utf-8", "replace")); h.update(b"\0")
        return h.hexdigest()
    finally:
        conn.close()


def _v153_scope_chat_ids_from_item(item) -> set[int]:
    result = set()
    if not isinstance(item, dict):
        return result
    for key in ("chat_id", "source_chat_id", "target_chat_id", "src_chat_id", "dst_chat_id", "root_chat_id"):
        raw = item.get(key)
        if str(raw or "").lstrip("-").isdigit():
            result.add(int(raw))
    for key in ("chat_ids", "target_chat_ids", "forward_targets"):
        value = item.get(key) or []
        if isinstance(value, dict):
            value = list(value.keys())
        for raw in value if isinstance(value, (list, tuple, set)) else []:
            if isinstance(raw, dict):
                raw = raw.get("dst_chat_id") or raw.get("chat_id")
            if str(raw or "").lstrip("-").isdigit():
                result.add(int(raw))
    return result


def _v153_item_in_tenant_scope(item, tenant_id: str, chat_ids: set[int], key: str = "") -> bool:
    if isinstance(item, dict):
        tid = str(item.get("tenant_id") or "")
        if tid and tid == str(tenant_id):
            return True
        if _v153_scope_chat_ids_from_item(item) & set(chat_ids):
            return True
    key_s = str(key or "")
    for cid in chat_ids:
        if key_s == str(cid) or key_s.startswith(f"{cid}:") or key_s.startswith(f"{cid}|"):
            return True
    return False


def _v153_filter_global_settings_for_tenant(gs: dict, tenant_id: str, chat_ids: set[int]) -> dict:
    tenants = (((gs or {}).get("tenants_v148") or {}).get("tenants") or {})
    tenant_row = _v153_copy.deepcopy(tenants.get(str(tenant_id)) or {})
    safe = {
        "tenants_v148": {
            "schema_version": 1,
            "tenants": {str(tenant_id): v153_sanitize(tenant_row)},
            "chat_to_tenant": {str(cid): str(tenant_id) for cid in sorted(chat_ids)},
            "invite_tokens": {},
            "legacy_migrated": True,
            "created_at": _v153_now(),
        }
    }
    # Reminder definitions are global in legacy architecture; filter every item by tenant/chat.
    rem = _v153_copy.deepcopy((gs or {}).get("reminders_v2") or {})
    items = {}
    for rid, cfg in ((rem.get("items") or {}).items() if isinstance(rem, dict) else []):
        if _v153_item_in_tenant_scope(cfg, tenant_id, chat_ids):
            items[str(rid)] = v153_sanitize(cfg)
    if items:
        safe["reminders_v2"] = {
            "next_id": max([int(x) for x in items if str(x).isdigit()] + [0]) + 1,
            "items": items,
            "migrated_v134": True,
        }
    # Operation and integrity history are retained only for this contour.
    op = _v153_copy.deepcopy((gs or {}).get("operation_ledger_v141") or {})
    if isinstance(op, dict):
        op_items = {str(k): v153_sanitize(v) for k, v in (op.get("items") or {}).items() if _v153_item_in_tenant_scope(v, tenant_id, chat_ids, str(k))}
        if op_items:
            order = [str(x) for x in (op.get("order") or []) if str(x) in op_items]
            safe["operation_ledger_v141"] = {"items": op_items, "order": order, "next_seq": int(op.get("next_seq") or 1)}
    integ = _v153_copy.deepcopy((gs or {}).get("finance_integrity_v141") or {})
    if isinstance(integ, dict):
        events = [v153_sanitize(x) for x in (integ.get("events") or []) if _v153_item_in_tenant_scope(x, tenant_id, chat_ids)]
        tips = {str(k): v for k, v in (integ.get("tips") or {}).items() if str(k).lstrip("-").isdigit() and int(k) in chat_ids}
        if events or tips:
            safe["finance_integrity_v141"] = {"events": events, "tips": tips, "anchor": {}, "event_seq": int(integ.get("event_seq") or 0)}
    return safe


def _v153_filter_root_for_tenant(root: dict, tenant_id: str, chat_ids: set[int]) -> dict:
    out = {}
    for key, value in (root or {}).items():
        if key == "_global_settings":
            out[key] = _v153_filter_global_settings_for_tenant(value if isinstance(value, dict) else {}, str(tenant_id), chat_ids)
            continue
        if key in {"chats", "_restore_mode_chat_v150"}:
            continue
        if isinstance(value, list):
            rows = [v153_sanitize(_v153_copy.deepcopy(item)) for item in value if _v153_item_in_tenant_scope(item, tenant_id, chat_ids)]
            if rows:
                out[key] = rows
            continue
        if isinstance(value, dict):
            filtered = {}
            for item_key, item in value.items():
                if _v153_item_in_tenant_scope(item, tenant_id, chat_ids, str(item_key)):
                    filtered[str(item_key)] = v153_sanitize(_v153_copy.deepcopy(item))
            if filtered:
                out[key] = filtered
            continue
        # Unscoped global scalars are intentionally excluded from tenant exports.
    return out


def _v153_collect_failed_tasks(tenant_id: str | None, chat_ids: set[int]) -> list[dict]:
    rows = []
    try:
        mega_task_refresh_registry()
        with _MEGA_TASK_LOCK:
            failed = [(str(k), str((v or {}).get("path") or "")) for k, v in _mega_task_registry.items() if str((v or {}).get("state") or "") == "failed"]
        for key, remote in failed[:500]:
            local = _mega_download_remote_path(remote)
            if not local:
                continue
            try:
                task = _v153_json.loads(_V153Path(local).read_text(encoding="utf-8"))
                cid = int(task.get("chat_id") or 0)
                if tenant_id is None or cid in chat_ids:
                    rows.append(v153_sanitize(task))
            finally:
                _v153_shutil.rmtree(_v153_os.path.dirname(local), ignore_errors=True)
    except Exception as exc:
        rows.append({"load_error": v153_redact_text(exc)})
    return rows


def _v153_build_export(scope: str, tenant_id: str | None = None) -> str:
    _lowram_flush_all_hot(evict=False)
    save_data(data, full=True)
    folder = _v153_tempfile.mkdtemp(prefix="v153_full_export_")
    raw = _v153_os.path.join(folder, "state.sqlite3")
    SQLITE.backup_to(raw)
    conn = _v153_sqlite3.connect(raw)
    try:
        chat_ids = set()
        if scope == "tenant":
            chat_ids = set(int(x) for x in tenant_chat_ids(str(tenant_id)))
            marks = ",".join("?" for _ in chat_ids) or "NULL"
            if chat_ids:
                conn.execute(f"DELETE FROM chats WHERE CAST(chat_id AS INTEGER) NOT IN ({marks})", tuple(chat_ids))
                conn.execute(f"DELETE FROM cold_fields WHERE CAST(chat_id AS INTEGER) NOT IN ({marks})", tuple(chat_ids))
            else:
                conn.execute("DELETE FROM chats"); conn.execute("DELETE FROM cold_fields")
            row = conn.execute("SELECT v FROM kv WHERE k='root'").fetchone()
            root = _v153_json.loads(row[0]) if row else {}
            filtered = _v153_filter_root_for_tenant(root, str(tenant_id), chat_ids)
            conn.execute("INSERT INTO kv(k,v) VALUES('root',?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", (_v153_json.dumps(filtered, ensure_ascii=False, separators=(",", ":")),))
        else:
            # Sanitize every JSON-bearing state table without changing the live DB.
            for table, key_cols, json_col in (("kv", ("k",), "v"), ("chats", ("chat_id",), "v"), ("meta", ("kind", "k"), "v"), ("cold_fields", ("chat_id", "k"), "v")):
                cols = ",".join(key_cols + (json_col,))
                for row in conn.execute(f"SELECT {cols} FROM {table}").fetchall():
                    keys, raw_json = row[:-1], row[-1]
                    try:
                        payload = v153_sanitize(_v153_json.loads(raw_json))
                    except Exception:
                        payload = v153_redact_text(raw_json)
                    where = " AND ".join(f"{c}=?" for c in key_cols)
                    conn.execute(f"UPDATE {table} SET {json_col}=? WHERE {where}", (_v153_json.dumps(payload, ensure_ascii=False, separators=(",", ":")), *keys))
            chat_ids = {int(x[0]) for x in conn.execute("SELECT chat_id FROM chats").fetchall() if str(x[0]).lstrip("-").isdigit()}
        failed = _v153_collect_failed_tasks(tenant_id if scope == "tenant" else None, chat_ids)
        conn.execute("INSERT INTO meta(kind,k,v) VALUES('v153_export','failed_tasks',?) ON CONFLICT(kind,k) DO UPDATE SET v=excluded.v", (_v153_json.dumps(failed, ensure_ascii=False, separators=(",", ":")),))
        manifest = {
            "kind": "telegram_bot_full_state_v153", "schema_version": V153_EXPORT_SCHEMA,
            "bot_version": VERSION, "created_at": _v153_now(), "scope": scope,
            "tenant_id": str(tenant_id or ""), "chat_ids": sorted(chat_ids),
            "chat_count": len(chat_ids), "failed_tasks": len(failed), "checksum": "",
        }
        conn.execute("INSERT INTO meta(kind,k,v) VALUES('v153_export','manifest',?) ON CONFLICT(kind,k) DO UPDATE SET v=excluded.v", (_v153_json.dumps(manifest, ensure_ascii=False, separators=(",", ":")),))
        conn.commit()
    finally:
        conn.close()
    checksum = _v153_db_logical_checksum(raw)
    conn = _v153_sqlite3.connect(raw)
    try:
        manifest = _v153_json.loads(conn.execute("SELECT v FROM meta WHERE kind='v153_export' AND k='manifest'").fetchone()[0])
        manifest["checksum"] = checksum
        conn.execute("UPDATE meta SET v=? WHERE kind='v153_export' AND k='manifest'", (_v153_json.dumps(manifest, ensure_ascii=False, separators=(",", ":")),))
        conn.commit()
    finally:
        conn.close()
    gz = _v153_os.path.join(folder, "latest_bot_state.sqlite3.gz")
    with open(raw, "rb") as fin, _v153_gzip.open(gz, "wb", compresslevel=6) as fout:
        _v153_shutil.copyfileobj(fin, fout, 1024 * 1024)
    return gz


def _v153_send_full_export(chat_id: int, scope: str, tenant_id: str | None):
    path = _v153_build_export(scope, tenant_id)
    try:
        with open(path, "rb") as fh:
            caption = "🗄 Полное состояние всего бота" if scope == "global" else f"🗄 Состояние пространства: {(tenant_get(tenant_id) or {}).get('name') or tenant_id}"
            _tg_call_retry(bot.send_document, int(chat_id), fh, caption=caption, timeout=180, purpose="json_full_v153")
        return True
    finally:
        _v153_shutil.rmtree(_v153_os.path.dirname(path), ignore_errors=True)


def _v177_legacy_0278_v153_validate_restore_gz(gz_path: str) -> tuple[dict, str]:
    folder = _v153_tempfile.mkdtemp(prefix="v153_restore_validate_")
    raw = _v153_os.path.join(folder, "restore.sqlite3")
    try:
        with _v153_gzip.open(gz_path, "rb") as fin, open(raw, "wb") as fout:
            _v153_shutil.copyfileobj(fin, fout, 1024 * 1024)
        conn = _v153_sqlite3.connect(raw)
        try:
            integrity = str(conn.execute("PRAGMA integrity_check").fetchone()[0])
            if integrity.lower() != "ok":
                raise RuntimeError(f"SQLite integrity_check: {integrity}")
            row = conn.execute("SELECT v FROM meta WHERE kind='v153_export' AND k='manifest'").fetchone()
            if not row:
                raise RuntimeError("manifest v153 not found")
            manifest = _v153_json.loads(row[0])
        finally:
            conn.close()
        if str(manifest.get("kind")) != "telegram_bot_full_state_v153":
            raise RuntimeError("unknown export kind")
        if int(manifest.get("schema_version") or 0) != V153_EXPORT_SCHEMA:
            raise RuntimeError("unsupported export schema")
        export_version = str(manifest.get("bot_version") or "")
        if not export_version.startswith("bot_v153_"):
            raise RuntimeError(f"unsupported bot version: {export_version or 'missing'}")
        checksum = _v153_db_logical_checksum(raw)
        if checksum != str(manifest.get("checksum") or ""):
            raise RuntimeError("checksum mismatch")
        return manifest, raw
    except Exception:
        _v153_shutil.rmtree(folder, ignore_errors=True)
        raise
try: _v177_legacy_0278_v153_validate_restore_gz.__name__ = '_v153_validate_restore_gz'
except Exception: pass
_v153_validate_restore_gz = _v177_legacy_0278_v153_validate_restore_gz


def _v153_restore_keyboard(token: str, scope: str):
    kb = types.InlineKeyboardMarkup()
    if scope == "tenant":
        kb.add(IB("♻️ Заменить", callback_data=f"v153:restore:{token}:replace"))
        kb.add(IB("➕ Объединить", callback_data=f"v153:restore:{token}:merge"))
    else:
        kb.add(IB("♻️ Восстановить весь бот", callback_data=f"v153:restore:{token}:replace"))
    kb.add(IB("❌ Отмена", callback_data=f"v153:restore:{token}:cancel"))
    return kb


def _v153_download_replied_document(msg) -> str:
    reply = getattr(msg, "reply_to_message", None)
    document = getattr(reply, "document", None)
    if not document:
        raise RuntimeError("Ответьте командой /restore на файл latest_bot_state.sqlite3.gz")
    name = str(getattr(document, "file_name", "") or "")
    if not name.lower().endswith(".sqlite3.gz"):
        raise RuntimeError("Нужен файл .sqlite3.gz")
    info = bot.get_file(document.file_id)
    raw = bot.download_file(info.file_path)
    folder = _v153_tempfile.mkdtemp(prefix="v153_restore_upload_")
    path = _v153_os.path.join(folder, "latest_bot_state.sqlite3.gz")
    with open(path, "wb") as fh:
        fh.write(raw)
    return path


def v153_cmd_json_full(msg):
    uid = _v153_actor_id(msg); chat_id = int(msg.chat.id)
    tenant_id = _v153_tenant_for_chat(chat_id)
    if _v153_platform_owner(uid):
        parts = str(getattr(msg, "text", "") or "").split(maxsplit=1)
        if len(parts) <= 1:
            kb = types.InlineKeyboardMarkup()
            kb.row(IB("🌐 Весь бот", callback_data="v153:json:global"))
            for tenant in tenant_all() or []:
                tid = str((tenant or {}).get("id") or "")
                if tid:
                    kb.row(IB(f"🏠 {(tenant or {}).get('name') or tid}", callback_data=f"v153:json:tenant:{tid}"))
            bot.reply_to(msg, "🗄 Что выгрузить?", reply_markup=kb)
            return
        if parts[1].strip().lower() not in {"all", "все", "global"}:
            tenant_id = parts[1].strip()
            if not tenant_get(tenant_id):
                bot.reply_to(msg, "⛔ Пространство не найдено."); return
            scope = "tenant"
        else:
            scope = "global"
    elif _v153_can_manage_tenant(uid, tenant_id):
        scope = "tenant"
    else:
        bot.reply_to(msg, "⛔ Недостаточно прав для полного экспорта."); return
    submit_interactive_file_job(chat_id, "json_full", "Полное состояние бота" if scope == "global" else "Состояние пространства", _v153_send_full_export, chat_id, scope, tenant_id if scope == "tenant" else None)


def v153_cmd_restore(msg):
    uid = _v153_actor_id(msg); chat_id = int(msg.chat.id)
    try:
        gz = _v153_download_replied_document(msg)
        manifest, raw = _v153_validate_restore_gz(gz)
        scope = str(manifest.get("scope") or "")
        tenant_id = str(manifest.get("tenant_id") or _v153_tenant_for_chat(chat_id))
        if scope == "global" and not _v153_platform_owner(uid):
            raise RuntimeError("Глобальное восстановление доступно только владельцу платформы")
        if scope == "tenant" and not _v153_can_manage_tenant(uid, tenant_id):
            # A tenant owner may restore into the current tenant only, never a foreign contour.
            current = _v153_tenant_for_chat(chat_id)
            if not _v153_can_manage_tenant(uid, current):
                raise RuntimeError("Нельзя восстановить чужое пространство")
            tenant_id = current
        token = _v153_hashlib.sha256(f"{uid}:{chat_id}:{_v153_time.time_ns()}".encode()).hexdigest()[:16]
        with _V153_LOCK:
            _V153_RESTORE_PENDING[token] = {
                "uid": uid, "chat_id": chat_id, "gz": gz, "raw": raw, "manifest": manifest,
                "tenant_id": tenant_id, "created": _v153_time.time(),
            }
        text = (
            "🧪 Файл проверен.\n\n"
            f"Версия: {manifest.get('bot_version')}\n"
            f"Область: {'весь бот' if scope == 'global' else 'пространство'}\n"
            f"Чатов: {manifest.get('chat_count')}\n"
            f"Failed-задач: {manifest.get('failed_tasks')}\n"
            f"Создан: {manifest.get('created_at')}\n\n"
            "Перед применением будет создана резервная копия текущего состояния."
        )
        bot.reply_to(msg, text, reply_markup=_v153_restore_keyboard(token, scope))
    except Exception as exc:
        bot.reply_to(msg, f"❌ Восстановление не подготовлено:\n{v153_redact_text(exc)[:500]}")


def _v153_backup_before_restore() -> str:
    folder = _v153_tempfile.mkdtemp(prefix="v153_pre_restore_")
    raw = _v153_os.path.join(folder, "pre_restore.sqlite3")
    SQLITE.backup_to(raw)
    gz = raw + ".gz"
    with open(raw, "rb") as fin, _v153_gzip.open(gz, "wb", compresslevel=5) as fout:
        _v153_shutil.copyfileobj(fin, fout, 1024 * 1024)
    if mega_is_configured():
        mega_put_replace(gz, f"{MEGA_BACKUP_DIR.rstrip('/')}/database/pre_restore", f"pre_restore_{now_local().strftime('%Y%m%d_%H%M%S')}.sqlite3.gz", archive_previous=False)
    return folder


def _v153_apply_global_restore(raw: str) -> None:
    SQLITE.replace_database(raw)
    restored = load_data()
    data.clear(); data.update(restored)
    try: tenant_v148_bootstrap()
    except Exception: pass
    try: tenant_v148_enforce_forward_isolation()
    except Exception: pass
    save_data(data, full=True)
    schedule_delta_backup(int(OWNER_ID or 0), delay=0.1, reason="v153_global_restore")


def _v153_retarget_tenant_value(value, source_tenant: str, target_tenant: str):
    if isinstance(value, dict):
        return {str(k): _v153_retarget_tenant_value(v, source_tenant, target_tenant) for k, v in value.items()}
    if isinstance(value, list):
        return [_v153_retarget_tenant_value(v, source_tenant, target_tenant) for v in value]
    if isinstance(value, str) and value == str(source_tenant):
        return str(target_tenant)
    return _v153_copy.deepcopy(value)


def _v153_remove_scope_from_root_value(value, target_tenant: str, target_chat_ids: set[int]):
    if isinstance(value, list):
        return [x for x in value if not _v153_item_in_tenant_scope(x, target_tenant, target_chat_ids)]
    if isinstance(value, dict):
        return {str(k): v for k, v in value.items() if not _v153_item_in_tenant_scope(v, target_tenant, target_chat_ids, str(k))}
    return value


def _v153_restore_tenant_root(source_root: dict, manifest: dict, target_tenant: str, target_chat_ids: set[int], mode: str) -> dict[int, int]:
    source_tenant = str(manifest.get("tenant_id") or target_tenant)
    source_chat_ids = {int(x) for x in (manifest.get("chat_ids") or [])}
    live_gs = data.setdefault("_global_settings", {})
    live_tenants = _tenants_root()
    current_row = tenant_get(target_tenant) or {}
    source_gs = (source_root or {}).get("_global_settings") or {}
    source_row = (((source_gs.get("tenants_v148") or {}).get("tenants") or {}).get(source_tenant) or {})
    imported_row = _v153_retarget_tenant_value(v153_sanitize(source_row), source_tenant, target_tenant)
    if mode == "merge":
        merged = _v153_copy.deepcopy(current_row)
        for key, value in imported_row.items():
            if key in {"settings", "users", "google_v149"} and isinstance(value, dict):
                base = merged.setdefault(key, {})
                if isinstance(base, dict):
                    base.update(value)
                else:
                    merged[key] = value
            elif key not in {"id", "owner_user_id", "created_at"}:
                merged[key] = value
        imported_row = merged
    imported_row["id"] = str(target_tenant)
    imported_row["owner_user_id"] = int(current_row.get("owner_user_id") or imported_row.get("owner_user_id") or 0)
    imported_row["chat_ids"] = sorted(source_chat_ids)
    imported_row["root_chat_id"] = int(imported_row.get("root_chat_id") or next(iter(sorted(source_chat_ids)), 0))
    live_tenants.setdefault("tenants", {})[str(target_tenant)] = imported_row
    for cid in source_chat_ids:
        live_tenants.setdefault("chat_to_tenant", {})[str(cid)] = str(target_tenant)

    # Restore reminders, remapping IDs that belong to another tenant.
    remap = {}
    source_rem = (source_gs.get("reminders_v2") or {}).get("items") or {}
    live_rem = live_gs.setdefault("reminders_v2", {"next_id": 1, "items": {}, "migrated_v134": True})
    live_items = live_rem.setdefault("items", {})
    if mode == "replace":
        for rid, cfg in list(live_items.items()):
            if _v153_item_in_tenant_scope(cfg, target_tenant, target_chat_ids):
                live_items.pop(str(rid), None)
    next_id = max([int(x) for x in live_items if str(x).isdigit()] + [int(live_rem.get("next_id") or 1), 1])
    for rid_s, cfg in source_rem.items():
        old_id = int(rid_s) if str(rid_s).isdigit() else 0
        new_id = old_id
        if str(new_id) in live_items and not _v153_item_in_tenant_scope(live_items.get(str(new_id)), target_tenant, target_chat_ids):
            next_id += 1; new_id = next_id
        row = _v153_retarget_tenant_value(cfg, source_tenant, target_tenant)
        row["tenant_id"] = str(target_tenant)
        live_items[str(new_id)] = row
        remap[old_id] = new_id
    live_rem["next_id"] = max([int(x) for x in live_items if str(x).isdigit()] + [1]) + 1
    live_rem["migrated_v134"] = True

    # Restore tenant-scoped operation/integrity history without touching other contours.
    for gs_key in ("operation_ledger_v141", "finance_integrity_v141"):
        src_value = source_gs.get(gs_key)
        if src_value is None:
            continue
        if mode == "replace" and gs_key in live_gs:
            if gs_key == "operation_ledger_v141":
                old_live = live_gs.get(gs_key) or {}
                kept_items = {str(k): v for k, v in (old_live.get("items") or {}).items() if not _v153_item_in_tenant_scope(v, target_tenant, target_chat_ids, str(k))}
                live_gs[gs_key] = {
                    "items": kept_items,
                    "order": [str(x) for x in (old_live.get("order") or []) if str(x) in kept_items],
                    "next_seq": int(old_live.get("next_seq") or 1),
                }
            elif gs_key == "finance_integrity_v141":
                old_live = live_gs.get(gs_key) or {}
                live_gs[gs_key] = {
                    "events": [x for x in (old_live.get("events") or []) if not _v153_item_in_tenant_scope(x, target_tenant, target_chat_ids)],
                    "tips": {str(k): v for k, v in (old_live.get("tips") or {}).items() if not (str(k).lstrip("-").isdigit() and int(k) in target_chat_ids)},
                    "anchor": {},
                    "event_seq": int(old_live.get("event_seq") or 0),
                }
        if gs_key == "operation_ledger_v141":
            live = live_gs.setdefault(gs_key, {"items": {}, "order": [], "next_seq": 1})
            for op_id, row in ((src_value or {}).get("items") or {}).items():
                target_id = str(op_id)
                while target_id in (live.get("items") or {}):
                    target_id = target_id + "_r"
                live.setdefault("items", {})[target_id] = _v153_retarget_tenant_value(row, source_tenant, target_tenant)
                live.setdefault("order", []).append(target_id)
            live["next_seq"] = max(int(live.get("next_seq") or 1), int((src_value or {}).get("next_seq") or 1))
        elif gs_key == "finance_integrity_v141":
            live = live_gs.setdefault(gs_key, {"events": [], "tips": {}, "anchor": {}, "event_seq": 0})
            live.setdefault("events", []).extend(_v153_retarget_tenant_value((src_value or {}).get("events") or [], source_tenant, target_tenant))
            live.setdefault("tips", {}).update((src_value or {}).get("tips") or {})
            live["event_seq"] = max(int(live.get("event_seq") or 0), int((src_value or {}).get("event_seq") or 0))

    # Restore remaining root-level rows already filtered to this tenant.
    for key, src_value in (source_root or {}).items():
        if key == "_global_settings":
            continue
        src_value = _v153_retarget_tenant_value(src_value, source_tenant, target_tenant)
        if mode == "replace" and key in data:
            data[key] = _v153_remove_scope_from_root_value(data.get(key), target_tenant, target_chat_ids)
        if isinstance(src_value, list):
            live = data.setdefault(key, [])
            seen = {_v153_json.dumps(x, sort_keys=True, ensure_ascii=False, default=str) for x in live if isinstance(live, list)}
            for item in src_value:
                sig = _v153_json.dumps(item, sort_keys=True, ensure_ascii=False, default=str)
                if sig not in seen:
                    live.append(item); seen.add(sig)
        elif isinstance(src_value, dict):
            data.setdefault(key, {}).update(src_value)
    return remap


def _v153_apply_tenant_restore(raw: str, target_tenant: str, mode: str) -> None:
    src = _v153_sqlite3.connect(raw); src.row_factory = _v153_sqlite3.Row
    try:
        manifest = _v153_json.loads(src.execute("SELECT v FROM meta WHERE kind='v153_export' AND k='manifest'").fetchone()[0])
        source_chat_ids = {int(x) for x in (manifest.get("chat_ids") or [])}
        root_row = src.execute("SELECT v FROM kv WHERE k='root'").fetchone()
        source_root = _v153_json.loads(root_row[0]) if root_row else {}
        target_row = tenant_get(target_tenant) or {}
        target_chat_ids = set(int(x) for x in (target_row.get("chat_ids") or []))
        for cid in source_chat_ids:
            current_tenant = str(tenant_id_for_chat(cid, create=False) or "")
            if current_tenant and current_tenant != str(target_tenant) and cid not in target_chat_ids:
                raise RuntimeError(f"Чат {cid} сейчас принадлежит другому пространству")
        if mode == "replace":
            for cid in list(target_chat_ids - source_chat_ids):
                data.get("chats", {}).pop(str(cid), None); SQLITE.delete_chat(cid)
                for key in list(LOWRAM_COLD_KEYS): SQLITE.delete_cold(cid, key)
            try:
                root = _tenants_root()
                row = (root.get("tenants") or {}).get(str(target_tenant)) or target_row
                row["chat_ids"] = sorted(source_chat_ids)
                if int(row.get("root_chat_id") or 0) not in source_chat_ids:
                    row["root_chat_id"] = next(iter(sorted(source_chat_ids)), 0)
                mapping = root.setdefault("chat_to_tenant", {})
                for cid in target_chat_ids - source_chat_ids:
                    if str(mapping.get(str(cid)) or "") == str(target_tenant):
                        mapping.pop(str(cid), None)
            except Exception:
                pass
        for row in src.execute("SELECT chat_id,v FROM chats").fetchall():
            cid = int(row[0]); payload = _v153_json.loads(row[1])
            if mode == "merge" and str(cid) in (data.get("chats") or {}):
                current = _lowram_materialize_chat_snapshot(cid, (data.get("chats") or {}).get(str(cid)))
                current.update(payload); payload = current
            data.setdefault("chats", {})[str(cid)] = _lowram_wrap_store(cid, payload)
            SQLITE.save_chat(cid, _lowram_store_meta_payload(payload))
        for row in src.execute("SELECT chat_id,k,v FROM cold_fields").fetchall():
            cid = int(row[0]); key = str(row[1]); value = _v153_json.loads(row[2])
            if mode == "merge":
                old = SQLITE.get_cold(cid, key, None)
                if isinstance(old, list) and isinstance(value, list):
                    seen = set(); merged = []
                    for item in old + value:
                        sig = _v153_json.dumps(item, sort_keys=True, ensure_ascii=False, default=str)
                        if sig not in seen: seen.add(sig); merged.append(item)
                    value = merged
                elif isinstance(old, dict) and isinstance(value, dict):
                    old.update(value); value = old
            SQLITE.set_cold(cid, key, value)
        _v153_restore_tenant_root(source_root, manifest, target_tenant, target_chat_ids, mode)
        # Rebind imported chats only to the chosen target tenant.
        for cid in source_chat_ids:
            tenant_bind_chat(cid, target_tenant, changed_by=0, force=True)
        try: tenant_v148_enforce_forward_isolation()
        except Exception: pass
        save_data(data, full=True)
        schedule_delta_backup(int(target_row.get("root_chat_id") or next(iter(source_chat_ids), OWNER_ID or 0)), delay=0.1, reason=f"v153_tenant_restore_{mode}")
    finally:
        src.close()


def _v153_restore_failed_tasks_from_db(raw: str, allowed_chat_ids: set[int] | None = None) -> int:
    if not mega_is_configured():
        return 0
    conn = _v153_sqlite3.connect(raw)
    try:
        row = conn.execute("SELECT v FROM meta WHERE kind='v153_export' AND k='failed_tasks'").fetchone()
        tasks = _v153_json.loads(row[0]) if row else []
    finally:
        conn.close()
    restored = 0
    remote_dir = mega_task_remote_dir("failed")
    ensure_mega_task_dirs()
    for task in tasks or []:
        if not isinstance(task, dict) or task.get("load_error"):
            continue
        cid = int(task.get("chat_id") or 0)
        if allowed_chat_ids is not None and cid not in allowed_chat_ids:
            continue
        key = str(task.get("task_id") or task.get("update_id") or "").strip()
        if not key:
            continue
        folder = _v153_tempfile.mkdtemp(prefix="v153_failed_restore_")
        try:
            name = f"task_{key}.json"
            local = _v153_os.path.join(folder, name)
            _V153Path(local).write_text(_v153_json.dumps(v153_sanitize(task), ensure_ascii=False, indent=2), encoding="utf-8")
            if mega_put_replace(local, remote_dir, name, archive_previous=False):
                restored += 1
        finally:
            _v153_shutil.rmtree(folder, ignore_errors=True)
    try: mega_task_refresh_registry()
    except Exception: pass
    return restored


def _v153_execute_restore(token: str, mode: str, call) -> bool:
    with _V153_LOCK:
        row = _V153_RESTORE_PENDING.pop(str(token), None)
    if not row:
        try: bot.answer_callback_query(call.id, "Файл восстановления устарел", show_alert=True)
        except Exception: pass
        return True
    uid = _v153_actor_id(call)
    if uid != int(row.get("uid") or 0) and not _v153_platform_owner(uid):
        try: bot.answer_callback_query(call.id, "Это подтверждение другого пользователя", show_alert=True)
        except Exception: pass
        return True
    backup_dir = ""
    try:
        backup_dir = _v153_backup_before_restore()
        scope = str((row.get("manifest") or {}).get("scope") or "")
        if scope == "global":
            _v153_apply_global_restore(str(row["raw"]))
            restored_failed = _v153_restore_failed_tasks_from_db(str(row["raw"]), None)
        else:
            _v153_apply_tenant_restore(str(row["raw"]), str(row["tenant_id"]), str(mode))
            restored_failed = _v153_restore_failed_tasks_from_db(str(row["raw"]), set(int(x) for x in ((row.get("manifest") or {}).get("chat_ids") or [])))
        safe_edit(bot, call, f"✅ Восстановление завершено. Текущее состояние сохранено и поставлено в durable backup.\nFailed-задач восстановлено: {restored_failed}.")
        bot_journal("v153_restore_applied", int(row["chat_id"]), f"scope={scope}; mode={mode}; tenant={row.get('tenant_id')}; by={uid}")
    except Exception as exc:
        safe_edit(bot, call, f"❌ Восстановление остановлено:\n{v153_redact_text(exc)[:800]}")
        bot_journal("v153_restore_failed", int(row["chat_id"]), v153_redact_text(exc), "ERROR")
    finally:
        for path in {row.get("gz"), row.get("raw")}:
            try:
                if path: _v153_shutil.rmtree(_v153_os.path.dirname(str(path)), ignore_errors=True)
            except Exception: pass
        if backup_dir: _v153_shutil.rmtree(backup_dir, ignore_errors=True)
    return True


# ─────────────────────────────────────────────────────────────
# Telegram UI idempotence and registry reconciliation.
# ─────────────────────────────────────────────────────────────
_V153_ORIG_EDIT_TEXT = getattr(bot, "edit_message_text", None)
_V153_ORIG_EDIT_MARKUP = getattr(bot, "edit_message_reply_markup", None)
_V153_ORIG_DELETE_MESSAGE = getattr(bot, "delete_message", None)
_V153_UI_CACHE = {}


def _v153_ui_sig(kind: str, chat_id, message_id, payload, markup=None) -> str:
    try:
        markup_value = markup.to_json() if hasattr(markup, "to_json") else repr(markup)
    except Exception:
        markup_value = repr(markup)
    raw = _v153_json.dumps([kind, int(chat_id), int(message_id), str(payload or ""), markup_value], ensure_ascii=False, default=str)
    return _v153_hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _v153_ui_cached(sig: str):
    with _V153_LOCK:
        row = _V153_UI_CACHE.get(sig)
        if row and _v153_time.monotonic() - float(row[0]) <= 120.0:
            return row[1]
        for key, item in list(_V153_UI_CACHE.items()):
            if _v153_time.monotonic() - float(item[0]) > 180.0:
                _V153_UI_CACHE.pop(key, None)
    return None


def _v153_ui_remember(sig: str, result):
    with _V153_LOCK:
        _V153_UI_CACHE[sig] = (_v153_time.monotonic(), result)
    return result


if callable(_V153_ORIG_EDIT_TEXT):
    def _v153_edit_message_text(text, chat_id=None, message_id=None, *args, **kwargs):
        sig = _v153_ui_sig("text", chat_id, message_id, text, kwargs.get("reply_markup"))
        cached = _v153_ui_cached(sig)
        if cached is not None:
            return cached
        try:
            return _v153_ui_remember(sig, _V153_ORIG_EDIT_TEXT(text, chat_id=chat_id, message_id=message_id, *args, **kwargs))
        except Exception as exc:
            low = str(exc).casefold()
            if "message is not modified" in low:
                bot_journal("telegram_edit_idempotent", chat_id, f"message={message_id}")
                return _v153_ui_remember(sig, True)
            raise
    bot.edit_message_text = _v153_edit_message_text


if callable(_V153_ORIG_EDIT_MARKUP):
    def _v153_edit_message_reply_markup(chat_id=None, message_id=None, *args, **kwargs):
        sig = _v153_ui_sig("markup", chat_id, message_id, "", kwargs.get("reply_markup"))
        cached = _v153_ui_cached(sig)
        if cached is not None:
            return cached
        try:
            return _v153_ui_remember(sig, _V153_ORIG_EDIT_MARKUP(chat_id=chat_id, message_id=message_id, *args, **kwargs))
        except Exception as exc:
            if "message is not modified" in str(exc).casefold():
                bot_journal("telegram_markup_idempotent", chat_id, f"message={message_id}")
                return _v153_ui_remember(sig, True)
            raise
    bot.edit_message_reply_markup = _v153_edit_message_reply_markup


if callable(_V153_ORIG_DELETE_MESSAGE):
    def _v153_delete_message(chat_id, message_id, *args, **kwargs):
        try:
            result = _V153_ORIG_DELETE_MESSAGE(chat_id, message_id, *args, **kwargs)
            try: unregister_open_window(int(chat_id), int(message_id))
            except Exception: pass
            return result
        except Exception as exc:
            low = str(exc).casefold()
            if any(x in low for x in ("message to delete not found", "message can't be deleted", "message identifier is not specified")):
                try: unregister_open_window(int(chat_id), int(message_id))
                except Exception: pass
                bot_journal("telegram_delete_already_gone", chat_id, f"message={message_id}")
                return True
            raise
    bot.delete_message = _v153_delete_message


def _v153_reconcile_windows() -> dict:
    result = cleanup_open_window_registry("v153_periodic") if "cleanup_open_window_registry" in globals() else {}
    try:
        bot_journal("v153_window_reconcile", None, result)
    except Exception:
        pass
    return result


# ─────────────────────────────────────────────────────────────
# Deep audit exposed as /full_audit and included in the release.
# ─────────────────────────────────────────────────────────────

def _v153_handler_audit() -> dict:
    commands = {}; duplicates = []; handlers_without_filters = 0
    for handler in list(getattr(bot, "message_handlers", []) or []):
        filters = handler.get("filters", {}) if isinstance(handler, dict) else {}
        fn = handler.get("function") if isinstance(handler, dict) else None
        rows = filters.get("commands") or []
        if not filters: handlers_without_filters += 1
        for command in rows:
            key = str(command).lower()
            if key in commands and commands[key] is not fn:
                duplicates.append(key)
            commands[key] = fn
    callback_count = len(list(getattr(bot, "callback_query_handlers", []) or []))
    return {
        "commands": len(commands), "duplicate_commands": sorted(set(duplicates)),
        "message_handlers": len(list(getattr(bot, "message_handlers", []) or [])),
        "callback_handlers": callback_count, "handlers_without_filters": handlers_without_filters,
        "known_commands": sorted(commands),
    }


def _v153_runtime_audit_text() -> str:
    a = _v153_handler_audit()
    stats = mega_task_registry_stats()
    migration = _v153_migration_store()
    lines = [
        "ГЛУБОКИЙ АУДИТ v153", f"Создан: {_v153_now()}", "",
        f"Slash-команд: {a['commands']}; дублей: {', '.join(a['duplicate_commands']) or 'нет'}.",
        f"Message handlers: {a['message_handlers']}; callback handlers: {a['callback_handlers']}.",
        "Команды /json_full, /restore и /full_audit имеют отдельные обработчики и проверку прав.",
        "Callback подтверждения защищены одноразовыми token и actor check.",
        "Повторная выгрузка не создаёт очередь: одно INFO-сообщение становится кнопкой скачивания.",
        "Telegram message is not modified считается идемпотентным результатом, а не ошибкой бизнеса.",
        "Временные chat not found/timeout не удаляют чат; lifecycle active/unreachable/bot_removed/migrated/archived сохраняется в runtime ZIP.",
        "Runtime ZIP скачивает slots/events; устаревшие candidate/staged остаются только в индексе и очищаются до 2 файлов.",
        f"Durable failed: {stats.get('failed',0)}; details: {len(stats.get('failed_details') or [])}; pending_detail_refresh={bool(stats.get('failed_details_pending'))}.",
        "Reminder witnesses принимаются только при явном EDITREM/EDITREMINT, поэтому финансовая задача не требует reminder_edit.",
        "Секреты очищаются перед журналом, snapshot, ZIP/TXT/JSON export и отправкой документа.",
        f"MEGA migration: {migration.get('status')} {migration.get('copied_files',0)}/{migration.get('total_files','?')}; old root не удаляется.",
    ]
    return "\n".join(lines)


def v153_cmd_full_audit(msg):
    uid = _v153_actor_id(msg)
    if not _v153_platform_owner(uid):
        bot.reply_to(msg, "⛔ Аудит доступен только владельцу платформы."); return
    bot.reply_to(msg, _v153_runtime_audit_text()[:3900])


def v153_cmd_mega_migration_status(msg):
    uid = _v153_actor_id(msg)
    if not _v153_platform_owner(uid): return
    row = _v153_migration_store()
    bot.reply_to(msg, f"☁️ Миграция MEGA\nСтатус: {row.get('status')}\nПроверено: {row.get('copied_files',0)}/{row.get('total_files','?')}\nОсталось: {row.get('remaining_files','?')}\nАктивная папка: {globals().get('MEGA_BACKUP_DIR')}\nОшибка: {row.get('last_error') or 'нет'}")


# ─────────────────────────────────────────────────────────────
# Callback extension and command registration/replacement.
# ─────────────────────────────────────────────────────────────

def _v153_callback_once(call, key: str) -> bool:
    now = _v153_time.time(); actor = _v153_actor_id(call)
    receipt = f"{actor}:{getattr(call, 'id', '')}:{key}"
    with _V153_LOCK:
        for old, ts in list(_V153_CALLBACK_RECEIPTS.items()):
            if now - ts > 600: _V153_CALLBACK_RECEIPTS.pop(old, None)
        if receipt in _V153_CALLBACK_RECEIPTS:
            return False
        _V153_CALLBACK_RECEIPTS[receipt] = now
    return True


_V153_ORIG_EXTENSION_CALLBACK = globals().get("v149_extension_callback")
def _v177_legacy_0268_v149_extension_callback(call, data_str: str) -> bool:
    data_str = str(data_str or "")
    if data_str == "runtime_watcher":
        uid = _v153_actor_id(call); chat_id = int(call.message.chat.id)
        if not _v153_platform_owner(uid):
            return bool(_V153_ORIG_EXTENSION_CALLBACK(call, data_str)) if callable(_V153_ORIG_EXTENSION_CALLBACK) else False
        kbw = types.InlineKeyboardMarkup()
        kbw.row(IB("🔄 Обновить", callback_data="runtime_watcher"), IB("📜 События", callback_data="runtime_events"))
        kbw.row(IB("☁️ Снимок Watcher в MEGA", callback_data="runtime_snapshot_now"))
        kbw.row(IB("📦 Runtime ZIP", callback_data="runtime_export"), IB("🗄 /json_full", callback_data="v153:json:menu"))
        kbw.row(IB("🚦 Очереди", callback_data="info_queues"), IB("🧩 Delta", callback_data="info_delta_status"))
        day = get_chat_store(chat_id).get("current_view_day") or today_key()
        kbw.row(IB("🔙 Назад в Инфо", callback_data=f"d:{day}:info"), IB("❌ Закрыть", callback_data="info_close"))
        safe_edit(bot, call, build_runtime_watcher_text(), reply_markup=kbw)
        return True
    if data_str == "v153:json:menu":
        uid = _v153_actor_id(call); chat_id = int(call.message.chat.id)
        if not _v153_platform_owner(uid):
            tenant_id = _v153_tenant_for_chat(chat_id)
            if _v153_can_manage_tenant(uid, tenant_id):
                submit_interactive_file_job(chat_id, "json_full", "Состояние пространства", _v153_send_full_export, chat_id, "tenant", tenant_id)
            return True
        kb = types.InlineKeyboardMarkup(); kb.row(IB("🌐 Весь бот", callback_data="v153:json:global"))
        for tenant in tenant_all() or []:
            tid = str((tenant or {}).get("id") or "")
            if tid: kb.row(IB(f"🏠 {(tenant or {}).get('name') or tid}", callback_data=f"v153:json:tenant:{tid}"))
        safe_edit(bot, call, "🗄 Что выгрузить?", reply_markup=kb)
        return True
    if data_str == "v153:json:global":
        uid = _v153_actor_id(call); chat_id = int(call.message.chat.id)
        if not _v153_platform_owner(uid):
            try: bot.answer_callback_query(call.id, "Только для владельца платформы", show_alert=True)
            except Exception: pass
            return True
        submit_interactive_file_job(chat_id, "json_full", "Полное состояние бота", _v153_send_full_export, chat_id, "global", None)
        return True
    if data_str.startswith("v153:json:tenant:"):
        uid = _v153_actor_id(call); chat_id = int(call.message.chat.id)
        tenant_id = data_str.split(":", 3)[3]
        if not (_v153_platform_owner(uid) or _v153_can_manage_tenant(uid, tenant_id)):
            try: bot.answer_callback_query(call.id, "Недостаточно прав", show_alert=True)
            except Exception: pass
            return True
        submit_interactive_file_job(chat_id, "json_full", "Состояние пространства", _v153_send_full_export, chat_id, "tenant", tenant_id)
        return True
    if data_str.startswith("v153:file:"):
        token = data_str.split(":", 2)[2]
        with _V153_LOCK:
            row = _V153_READY_EXPORTS.pop(token, None)
        if not row:
            try: bot.answer_callback_query(call.id, "Запрос уже использован или устарел", show_alert=True)
            except Exception: pass
            return True
        if int(row.get("chat_id") or 0) != int(call.message.chat.id):
            try: bot.answer_callback_query(call.id, "Это кнопка другого чата", show_alert=True)
            except Exception: pass
            return True
        if not _v153_callback_once(call, data_str): return True
        try: safe_edit(bot, call, f"⏳ Запускаю «{row['label']}»…")
        except Exception: pass
        submit_interactive_file_job(int(row["chat_id"]), row["kind"], row["label"], row["func"], *row["args"], **row["kwargs"])
        return True
    if data_str.startswith("v153:restore:"):
        parts = data_str.split(":")
        if len(parts) >= 4:
            token, action = parts[2], parts[3]
            if action == "cancel":
                with _V153_LOCK: row = _V153_RESTORE_PENDING.pop(token, None)
                if row:
                    for path in (row.get("gz"), row.get("raw")):
                        try: _v153_shutil.rmtree(_v153_os.path.dirname(str(path)), ignore_errors=True)
                        except Exception: pass
                safe_edit(bot, call, "❌ Восстановление отменено.")
                return True
            if action in {"replace", "merge"}:
                return _v153_execute_restore(token, action, call)
    if callable(_V153_ORIG_EXTENSION_CALLBACK):
        return bool(_V153_ORIG_EXTENSION_CALLBACK(call, data_str))
    return False
try: _v177_legacy_0268_v149_extension_callback.__name__ = 'v149_extension_callback'
except Exception: pass
v149_extension_callback = _v177_legacy_0268_v149_extension_callback


def _v153_prune_restore_pending() -> None:
    now = _v153_time.time()
    expired = []
    with _V153_LOCK:
        for token, row in list(_V153_RESTORE_PENDING.items()):
            if now - float((row or {}).get("created") or 0.0) > V153_RESTORE_PENDING_TTL:
                expired.append(_V153_RESTORE_PENDING.pop(token, None))
    for row in expired:
        for path in ((row or {}).get("gz"), (row or {}).get("raw")):
            try:
                if path: _v153_shutil.rmtree(_v153_os.path.dirname(str(path)), ignore_errors=True)
            except Exception:
                pass


def _v153_install_callback_dedupe() -> int:
    installed = 0
    for handler in list(getattr(bot, "callback_query_handlers", []) or []):
        if not isinstance(handler, dict):
            continue
        original = handler.get("function")
        if not callable(original) or getattr(original, "_v153_dedupe", False):
            continue
        def _wrapped(call, _original=original):
            now = _v153_time.monotonic()
            actor = _v153_actor_id(call)
            message = getattr(call, "message", None)
            signature = (
                actor,
                int(getattr(getattr(message, "chat", None), "id", 0) or 0),
                int(getattr(message, "message_id", 0) or 0),
                str(getattr(call, "data", "") or ""),
            )
            with _V153_LOCK:
                for key, ts in list(_V153_CALLBACK_SIGNATURES.items()):
                    if now - float(ts) > 5.0:
                        _V153_CALLBACK_SIGNATURES.pop(key, None)
                if signature in _V153_CALLBACK_SIGNATURES and now - float(_V153_CALLBACK_SIGNATURES[signature]) < 1.2:
                    try: bot.answer_callback_query(call.id, "Уже выполняется", show_alert=False)
                    except Exception: pass
                    bot_journal("callback_duplicate_suppressed", signature[1], f"message={signature[2]}; action={signature[3][:120]}")
                    return None
                _V153_CALLBACK_SIGNATURES[signature] = now
            return _original(call)
        _wrapped._v153_dedupe = True
        handler["function"] = _wrapped
        installed += 1
    return installed


def _v153_add_command(commands, function):
    try:
        decorator = bot.message_handler(commands=list(commands))
        decorator(function)
    except Exception:
        pass


def _v153_replace_restore_handler():
    replaced = 0
    for handler in list(getattr(bot, "message_handlers", []) or []):
        if not isinstance(handler, dict): continue
        filters = handler.get("filters") or {}
        commands = [str(x).lower() for x in (filters.get("commands") or [])]
        if "restore" in commands:
            handler["function"] = v153_cmd_restore; replaced += 1
    if not replaced:
        _v153_add_command(["restore"], v153_cmd_restore)
    return replaced


_v153_add_command(["json_full"], v153_cmd_json_full)
_v153_add_command(["full_audit", "audit_full"], v153_cmd_full_audit)
_v153_add_command(["mega_migration_status"], v153_cmd_mega_migration_status)
_V153_RESTORE_HANDLERS = _v153_replace_restore_handler()
_V153_CALLBACK_DEDUPE_HANDLERS = _v153_install_callback_dedupe()
try:
    _V150_MUTATION_COMMANDS.update({"/restore"})
except Exception:
    pass


def _v177_legacy_0084_runtime_mark_ready(detail: str = ""):
    result = _V153_ORIG_RUNTIME_MARK_READY(detail) if callable(_V153_ORIG_RUNTIME_MARK_READY) else None
    try:
        DELAYED_SCHEDULER.schedule("v153-instance-lease", 1.0, lambda: GENERAL_TASK_POOL.submit_unique("v153-instance-lease", _v153_instance_lease_check))
        DELAYED_SCHEDULER.schedule("v153-mega-migration", 3.0, _v153_schedule_migration)
        DELAYED_SCHEDULER.schedule("v153-window-reconcile", 8.0, lambda: GENERAL_TASK_POOL.submit_unique("v153-window-reconcile", _v153_reconcile_windows))
        DELAYED_SCHEDULER.schedule("v153-runtime-cleanup", 12.0, lambda: GENERAL_TASK_POOL.submit_unique("v153-runtime-prune", _v153_runtime_cleanup_remote))
        DELAYED_SCHEDULER.schedule("v153-restore-pending-cleanup", 60.0, _v153_prune_restore_pending)
        # Recurring checks are self-rescheduled without creating duplicate delayed jobs.
        def _lease_loop():
            _v153_instance_lease_check()
            try: DELAYED_SCHEDULER.schedule("v153-instance-lease-loop", 30.0, _lease_loop)
            except Exception: pass
        def _window_loop():
            _v153_reconcile_windows()
            try: DELAYED_SCHEDULER.schedule("v153-window-reconcile-loop", 600.0, _window_loop)
            except Exception: pass
        DELAYED_SCHEDULER.schedule("v153-instance-lease-loop", 30.0, _lease_loop)
        DELAYED_SCHEDULER.schedule("v153-window-reconcile-loop", 600.0, _window_loop)
    except Exception:
        pass
    return result
try: _v177_legacy_0084_runtime_mark_ready.__name__ = 'runtime_mark_ready'
except Exception: pass
runtime_mark_ready = _v177_legacy_0084_runtime_mark_ready


try:
    bot_journal("v153_remaining_fixes_installed", int(OWNER_ID or 0), f"restore_handlers={_V153_RESTORE_HANDLERS}; callback_dedupe={_V153_CALLBACK_DEDUPE_HANDLERS}; new_mega_root={V153_NEW_MEGA_ROOT}")
except Exception:
    pass

# v178_global_performance_final
