# v162_start_hard_fix
"""v162: hard /start path. Bare /start bypasses legacy message-handler routing and always gives a visible result."""

import gzip as _v162_gzip
import json as _v162_json
import os as _v162_os
import shutil as _v162_shutil
import sqlite3 as _v162_sqlite3
import tempfile as _v162_tempfile
import threading as _v162_threading

VERSION = "bot_v162_start_hard_fix"

_V162_START_LOCK_GUARD = _v162_threading.RLock()
_V162_START_LOCKS = {}


def _v162_start_lock(chat_id: int):
    cid = int(chat_id)
    with _V162_START_LOCK_GUARD:
        lock = _V162_START_LOCKS.get(cid)
        if lock is None:
            lock = _v162_threading.RLock()
            _V162_START_LOCKS[cid] = lock
        return lock


def _v162_is_start_message(msg) -> bool:
    try:
        text = str(getattr(msg, "text", "") or "").strip()
        if not text:
            return False
        cmd = text.split(None, 1)[0].split("@", 1)[0].casefold()
        return cmd in {"/start", "/старт"}
    except Exception:
        return False


def _v162_start_payload_present(msg) -> bool:
    try:
        text = str(getattr(msg, "text", "") or "").strip()
        return len(text.split(None, 1)) > 1
    except Exception:
        return False


def _v162_force_start(msg) -> bool:
    """Always produce a visible /start result. Never reuse/edit an old main window."""
    try:
        chat_id = int(msg.chat.id)
    except Exception:
        return True

    with _v162_start_lock(chat_id):
        try:
            update_chat_info_from_message(msg)
        except Exception:
            pass

        # Deep-link /start payloads keep their tenant/invite semantics.
        if _v162_start_payload_present(msg):
            try:
                fn = globals().get("tenant_handle_start_payload")
                if callable(fn) and fn(msg):
                    try:
                        schedule_command_delete(msg)
                    except Exception:
                        pass
                    try:
                        bot_journal("start_v162_payload", chat_id, "handled=tenant_payload")
                    except Exception:
                        pass
                    return True
            except Exception as exc:
                try:
                    log_error(f"v162 tenant /start payload {chat_id}: {exc}")
                except Exception:
                    pass

        try:
            set_total_secret_mode(chat_id, False)
        except Exception:
            pass
        try:
            stop_dozvon_for_target(chat_id)
        except Exception:
            pass

        # For a non-owner hidden/suppressed chat do not expose finance data, but never stay silent.
        try:
            if is_finance_output_suppressed(chat_id) and not is_owner_chat(chat_id):
                bot.send_message(chat_id, "ℹ️ Основное финансовое окно скрыто настройками этого чата.")
                try:
                    schedule_command_delete(msg)
                except Exception:
                    pass
                try:
                    bot_journal("start_v162_visible_block", chat_id, "reason=finance_output_suppressed")
                except Exception:
                    pass
                return True
        except Exception:
            pass

        # Finance-disabled chats also receive a visible answer instead of a silent return.
        try:
            if not is_finance_mode(chat_id):
                bot.send_message(chat_id, "⚙️ Финансовый режим выключен.\nАктивируйте командой /ok")
                try:
                    schedule_command_delete(msg)
                except Exception:
                    pass
                try:
                    bot_journal("start_v162_visible_block", chat_id, "reason=finance_mode_off")
                except Exception:
                    pass
                return True
        except Exception:
            pass

        try:
            day_key = finance_today_key()
        except Exception:
            try:
                day_key = today_key()
            except Exception:
                day_key = ""

        try:
            store = get_chat_store(chat_id)
            store["current_view_day"] = day_key
        except Exception:
            pass

        try:
            # Deliberately CREATE a fresh Ф91. /start is a recovery command and must be visible.
            mid = int(_v161_send_main(chat_id, day_key) or 0)
            if not mid:
                raise RuntimeError("send_message returned no message_id")
            try:
                schedule_command_delete(msg)
            except Exception:
                pass
            try:
                bot_journal("start_v162_created", chat_id, f"day={day_key}; msg={mid}; always_new=1")
            except Exception:
                pass
            return True
        except Exception as exc:
            try:
                log_error(f"/start v162 hard path failed {chat_id}: {exc}")
            except Exception:
                pass
            # Last-resort visible acknowledgement. Do not delete the user's /start on failure.
            try:
                bot.send_message(chat_id, "⚠️ Команда /start получена, но Telegram не дал открыть Ф91. Ошибка записана в журнал.")
            except Exception:
                pass
            return True


# Intercept /start before TeleBot's legacy handler chain. This avoids handler-order/filter conflicts.
_V162_ORIGINAL_PROCESS_NEW_UPDATES = getattr(bot, "process_new_updates", None)


def _v162_process_new_updates(updates):
    remaining = []
    for update in list(updates or []):
        msg = getattr(update, "message", None)
        if msg is not None and _v162_is_start_message(msg):
            try:
                _v162_force_start(msg)
            except Exception as exc:
                try:
                    log_error(f"v162 /start interceptor: {exc}")
                except Exception:
                    pass
            continue
        remaining.append(update)
    if remaining and callable(_V162_ORIGINAL_PROCESS_NEW_UPDATES):
        return _V162_ORIGINAL_PROCESS_NEW_UPDATES(remaining)
    return None


if callable(_V162_ORIGINAL_PROCESS_NEW_UPDATES):
    bot.process_new_updates = _v162_process_new_updates


# Keep full-state restore forward-compatible with this release.
def _v153_validate_restore_gz(gz_path: str) -> tuple[dict, str]:
    folder = _v162_tempfile.mkdtemp(prefix="v162_restore_validate_")
    raw = _v162_os.path.join(folder, "restore.sqlite3")
    try:
        with _v162_gzip.open(gz_path, "rb") as fin, open(raw, "wb") as fout:
            _v162_shutil.copyfileobj(fin, fout, 1024 * 1024)
        conn = _v162_sqlite3.connect(raw)
        try:
            integrity = str(conn.execute("PRAGMA integrity_check").fetchone()[0])
            if integrity.lower() != "ok":
                raise RuntimeError(f"SQLite integrity_check: {integrity}")
            row = conn.execute("SELECT v FROM meta WHERE kind='v153_export' AND k='manifest'").fetchone()
            if not row:
                raise RuntimeError("manifest v153 not found")
            manifest = _v162_json.loads(row[0])
        finally:
            conn.close()
        if str(manifest.get("kind")) != "telegram_bot_full_state_v153":
            raise RuntimeError("unknown export kind")
        if int(manifest.get("schema_version") or 0) != int(V153_EXPORT_SCHEMA):
            raise RuntimeError("unsupported export schema")
        export_version = str(manifest.get("bot_version") or "")
        if not export_version.startswith((
            "bot_v153_", "bot_v154_", "bot_v155_", "bot_v156_", "bot_v157_",
            "bot_v158_", "bot_v159_", "bot_v160_", "bot_v161_", "bot_v162_",
        )):
            raise RuntimeError(f"unsupported bot version: {export_version or 'missing'}")
        if _v153_db_logical_checksum(raw) != str(manifest.get("checksum") or ""):
            raise RuntimeError("checksum mismatch")
        return manifest, raw
    except Exception:
        _v162_shutil.rmtree(folder, ignore_errors=True)
        raise


try:
    bot_journal("v162_start_hard_fix_installed", int(OWNER_ID or 0), "process_new_updates_intercept=1; start_always_new_f91=1; silent_returns=0")
except Exception:
    pass

# v162_start_hard_fix
