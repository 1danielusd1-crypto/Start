# v178_global_performance_final
"""v173: reliable owner cross-chat reminders + unmistakable unique journal filenames.

Loaded after v172.  The platform owner may explicitly select any chat from the reminder
picker; second-circle tenants remain isolated.  Operational journal downloads get a
human type name plus an export timestamp/sequence so Telegram never has to append (1),
(2), etc. to two different downloads from the same day.
"""
import os as _v173_os
import re as _v173_re
import threading as _v173_threading
import time as _v173_time
from datetime import datetime as _v173_datetime

VERSION = "bot_v173_reminder_crosschat_unique_journals"
V173_FILE_MARKER = "v173_reminder_crosschat_unique_journals"

# ---------------------------------------------------------------------------
# 1) Reminder delivery policy.
# Root cause seen in v171 journal: v171 checked an explicitly selected platform-owner
# target against collect_all_known_chat_ids().  v148 had already replaced that helper
# with a *current-tenant-scoped* list, therefore first/second-circle target chats were
# rejected with tenant_reminder_cross_chat_blocked even though the owner selected them.
# ---------------------------------------------------------------------------
_V173_PREV_REMINDER_CHAT_ALLOWED = globals().get("_v149_reminder_chat_allowed")


def _v173_reminder_selected_chat_ids(cfg: dict) -> set[int]:
    out = set()
    for raw in ((cfg or {}).get("chat_ids") or []):
        try:
            out.add(int(raw))
        except Exception:
            continue
    return out


def _v149_reminder_chat_allowed(cfg: dict, chat_id: int) -> bool:
    """Final send-time authority for reminder targets.

    Platform owner reminder:
      explicit selection in cfg.chat_ids is enough; Telegram itself is the final
      reachability check.  Do not re-filter through a tenant-scoped chat picker.

    Non-platform tenant reminder:
      retain strict tenant membership so second-circle spaces cannot notify chats
      belonging to another space merely by injecting an id into stored config.
    """
    try:
        cid = int(chat_id)
    except Exception:
        return False
    if cid not in _v173_reminder_selected_chat_ids(cfg):
        return False

    platform_id = str(globals().get("TENANT_PLATFORM_ID") or "platform")
    try:
        tid = str(_v149_reminder_cfg_tenant(cfg) or platform_id)
    except Exception:
        tid = str((cfg or {}).get("tenant_id") or platform_id)

    if tid == platform_id:
        # The owner selected this exact Telegram chat in the reminder configuration.
        # If the bot has subsequently been removed, send_message will fail explicitly
        # and the delivery journal will show the Telegram error instead of silently
        # hiding the target behind tenant_reminder_cross_chat_blocked.
        return True

    try:
        return bool(_v149_chat_belongs_to_tenant(cid, tid))
    except Exception:
        if callable(_V173_PREV_REMINDER_CHAT_ALLOWED):
            try:
                return bool(_V173_PREV_REMINDER_CHAT_ALLOWED(cfg, cid))
            except Exception:
                pass
        return False


# Per-target delivery witnesses make the next diagnostic journal self-explanatory.
_V173_BASE_REMINDER_SEND_INDIVIDUAL = globals().get("_v149_send_individual")
if callable(_V173_BASE_REMINDER_SEND_INDIVIDUAL):
    def _v149_send_individual(chat_id: int, reminder_id: int, cfg: dict, active_count: int):
        try:
            result = _V173_BASE_REMINDER_SEND_INDIVIDUAL(int(chat_id), int(reminder_id), cfg, int(active_count))
            ok = bool(result[0]) if isinstance(result, tuple) and result else bool(result)
            mid = int(result[1] or 0) if isinstance(result, tuple) and len(result) > 1 else 0
            try:
                bot_journal(
                    "reminder_delivery_v173",
                    int(chat_id),
                    f"reminder_id={int(reminder_id)} mode=individual ok={ok} message_id={mid}",
                    "INFO" if ok else "WARN",
                )
            except Exception:
                pass
            return result
        except Exception as exc:
            try:
                bot_journal("reminder_delivery_v173", int(chat_id), f"reminder_id={int(reminder_id)} mode=individual ok=False error={str(exc)[:300]}", "ERROR")
            except Exception:
                pass
            raise


_V173_BASE_REMINDER_SEND_GROUP = globals().get("_v149_send_or_edit_group")
if callable(_V173_BASE_REMINDER_SEND_GROUP):
    def _v149_send_or_edit_group(chat_id: int, text: str, old_message_id: int = 0):
        try:
            result = _V173_BASE_REMINDER_SEND_GROUP(int(chat_id), text, int(old_message_id or 0))
            ok = bool(result[0]) if isinstance(result, tuple) and result else bool(result)
            mid = int(result[1] or 0) if isinstance(result, tuple) and len(result) > 1 else 0
            try:
                bot_journal(
                    "reminder_delivery_v173",
                    int(chat_id),
                    f"mode=merged ok={ok} message_id={mid} old_message_id={int(old_message_id or 0)}",
                    "INFO" if ok else "WARN",
                )
            except Exception:
                pass
            return result
        except Exception as exc:
            try:
                bot_journal("reminder_delivery_v173", int(chat_id), f"mode=merged ok=False error={str(exc)[:300]}", "ERROR")
            except Exception:
                pass
            raise


# ---------------------------------------------------------------------------
# 2) Journal/export filenames.
# v170 fixed *misclassification*, but two downloads of the same journal on the same
# date still had the same filename.  v173 gives every operational export a clear type
# name plus a unique export timestamp and sequence.
# ---------------------------------------------------------------------------
_V173_DOWNLOAD_NAME_LOCK = _v173_threading.RLock()
_V173_DOWNLOAD_NAME_SEQ = 0
_V173_DOWNLOAD_NAME_LAST_SECOND = ""

V173_DOWNLOAD_KIND_NAMES = {
    "Журнал_текущей_версии": "События_текущей_версии",
    "Журнал_диагностики": "Диагностика_бота",
    "Журнал_FAILED_задач": "FAILED_задачи",
    "Журнал_ошибок": "Ошибки_бота",
    "Журнал_восстановления": "Восстановление_бота",
    "Журнал_пересылки": "Пересылка_сообщений",
    "Журнал_аудита": "Аудит_целостности",
    "Журнал_резервных_копий": "Резервное_копирование",
    "Журнал_финансов": "Финансовые_операции",
    "Журнал_операций": "Действия_бота",
    "Диагностика_Runtime_MEGA": "Диагностика_Runtime_MEGA",
    "ТЗ_окон_текущая_версия": "ТЗ_окон_текущая_версия",
    "ТЗ_окон_архив": "ТЗ_окон_архив",
    "Маркировки_окон": "Маркировки_окон",
    "Исходник_бота": "Исходник_бота",
}


def _v173_export_stamp() -> str:
    global _V173_DOWNLOAD_NAME_SEQ, _V173_DOWNLOAD_NAME_LAST_SECOND
    try:
        now = now_local()
    except Exception:
        now = _v173_datetime.now()
    second = now.strftime("%Y-%m-%d_%H-%M-%S")
    millis = int(getattr(now, "microsecond", 0) // 1000)
    with _V173_DOWNLOAD_NAME_LOCK:
        if second != _V173_DOWNLOAD_NAME_LAST_SECOND:
            _V173_DOWNLOAD_NAME_LAST_SECOND = second
            _V173_DOWNLOAD_NAME_SEQ = 0
        _V173_DOWNLOAD_NAME_SEQ += 1
        seq = _V173_DOWNLOAD_NAME_SEQ
    return f"{second}-{millis:03d}-{seq:02d}"


def _v173_safe_component(value: str, fallback: str = "файл") -> str:
    try:
        fn = globals().get("_v152_filename_component")
        if callable(fn):
            return str(fn(value, fallback))
    except Exception:
        pass
    text = _v173_re.sub(r"[\\/:*?\"<>|]+", "-", str(value or fallback))
    text = _v173_re.sub(r"\s+", "-", text).strip("-._")
    return (text or fallback)[:80]


def v152_human_download_name(recipient_chat_id: int, document, caption: str = "", purpose: str = "") -> str | None:
    """Readable and collision-free name for each operational download."""
    try:
        kind = _v152_download_kind(document, caption, purpose)
    except Exception:
        kind = None
    if not kind:
        return None

    old_name = str(getattr(document, "name", "") or getattr(document, "file_name", "") or "")
    ext = _v173_os.path.splitext(old_name)[1].lower()
    if kind == "Исходник_бота":
        ext = ".py"
    elif ext not in {".txt", ".csv", ".zip", ".json", ".xlsx", ".gz", ".sqlite3", ".py"}:
        ext = ".zip" if kind in {"Журнал_FAILED_задач", "Диагностика_Runtime_MEGA"} else ".txt"

    try:
        scope = _v152_scope_name(int(recipient_chat_id), kind)
    except Exception:
        scope = _v173_safe_component(f"Чат-{recipient_chat_id}", "Чат")
    try:
        period = _v152_period_suffix(document, caption, purpose)
    except Exception:
        period = ""

    visible_kind = V173_DOWNLOAD_KIND_NAMES.get(str(kind), str(kind))
    pieces = [_v173_safe_component(visible_kind, "Выгрузка"), _v173_safe_component(scope, "Чат")]
    if period:
        pieces.append(_v173_safe_component(period, "период"))
    pieces.append(f"выгрузка-{_v173_export_stamp()}")
    return "_".join(pieces) + ext


# ---------------------------------------------------------------------------
# 3) Restore compatibility for v173 snapshots.
# ---------------------------------------------------------------------------
_V173_PREV_RESTORE_VALIDATOR = globals().get("_v153_validate_restore_gz")

def _v177_legacy_0290_v153_validate_restore_gz(gz_path: str):
    try:
        return _V173_PREV_RESTORE_VALIDATOR(gz_path) if callable(_V173_PREV_RESTORE_VALIDATOR) else (None, None)
    except Exception as exc:
        if "unsupported bot version" not in str(exc):
            raise

    import gzip, shutil, sqlite3, tempfile, json
    folder = tempfile.mkdtemp(prefix="v173_restore_validate_")
    raw = _v173_os.path.join(folder, "restore.sqlite3")
    try:
        with gzip.open(gz_path, "rb") as fin, open(raw, "wb") as fout:
            shutil.copyfileobj(fin, fout, 1024 * 1024)
        conn = sqlite3.connect(raw)
        try:
            integrity = str(conn.execute("PRAGMA integrity_check").fetchone()[0])
            if integrity.lower() != "ok":
                raise RuntimeError(f"SQLite integrity_check: {integrity}")
            row = conn.execute("SELECT v FROM meta WHERE kind='v153_export' AND k='manifest'").fetchone()
            if not row:
                raise RuntimeError("manifest v153 not found")
            manifest = json.loads(row[0])
        finally:
            conn.close()
        if str(manifest.get("kind")) != "telegram_bot_full_state_v153":
            raise RuntimeError("unknown export kind")
        if int(manifest.get("schema_version") or 0) != int(V153_EXPORT_SCHEMA):
            raise RuntimeError("unsupported export schema")
        export_version = str(manifest.get("bot_version") or "")
        if not export_version.startswith(tuple(f"bot_v{i}_" for i in range(153, 174))):
            raise RuntimeError(f"unsupported bot version: {export_version or 'missing'}")
        if _v153_db_logical_checksum(raw) != str(manifest.get("checksum") or ""):
            raise RuntimeError("checksum mismatch")
        return manifest, raw
    except Exception:
        shutil.rmtree(folder, ignore_errors=True)
        raise
try: _v177_legacy_0290_v153_validate_restore_gz.__name__ = '_v153_validate_restore_gz'
except Exception: pass
_v153_validate_restore_gz = _v177_legacy_0290_v153_validate_restore_gz


try:
    bot_journal(
        "v173_installed",
        int(OWNER_ID or 0),
        "platform reminder explicit targets bypass tenant-scoped known-chat recheck; unique typed journal filenames enabled",
    )
except Exception:
    pass
# v178_global_performance_final
