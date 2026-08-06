# v149_tenant_google_merged_reminders

# ─────────────────────────────────────────────────────────────
# v149: per-tenant Google + dynamic merged reminders
# ─────────────────────────────────────────────────────────────
import base64 as _v149_base64
import hashlib as _v149_hashlib
import hmac as _v149_hmac
import io as _v149_io
import json as _v149_json
import mimetypes as _v149_mimetypes
import os as _v149_os
import re as _v149_re
import secrets as _v149_secrets
import threading as _v149_threading
import time as _v149_time
from collections import defaultdict as _v149_defaultdict
from contextlib import contextmanager as _v149_contextmanager
from copy import deepcopy as _v149_deepcopy
from datetime import timedelta as _v149_timedelta
from pathlib import Path as _v149_Path

VERSION = "bot_v149_tenant_google_merged_reminders"
V149_GOOGLE_SCHEMA_VERSION = 1
V149_REMINDER_SCHEMA_VERSION = 1
_V149_GOOGLE_CONTEXT = _v149_threading.local()
_V149_GOOGLE_TOKEN_LOCK = _v149_threading.RLock()
_V149_GOOGLE_TOKEN_CACHE = {}
_V149_REMINDER_BATCH_LOCK = _v149_threading.RLock()
_V149_COMPLETION_LOCK = _v149_threading.RLock()
_V149_PLATFORM_GOOGLE_JSON = str(globals().get("GOOGLE_SERVICE_ACCOUNT_JSON") or "")
_V149_PLATFORM_GOOGLE_SHEET = str(globals().get("GOOGLE_SHEETS_SPREADSHEET_ID") or "")
_V149_PLATFORM_GOOGLE_SHARE = str(globals().get("GOOGLE_SHEETS_SHARE_EMAIL") or "")
_V149_BASE_GOOGLE_SHEETS_CREATE = globals().get("_google_sheets_create_category_report")
_V149_BASE_REMINDER_LIST_TEXT = globals().get("build_reminder_list_text")
_V149_BASE_REMINDER_MENU_TEXT = globals().get("build_reminder_menu_text")


def _v149_now_iso() -> str:
    return now_local().isoformat(timespec="seconds")


def _v149_actor_id(obj) -> int:
    try:
        return int(getattr(getattr(obj, "from_user", None), "id", 0) or 0)
    except Exception:
        return 0


def _v149_actor_label(obj) -> str:
    user = getattr(obj, "from_user", None)
    if user is None:
        return ""
    full = " ".join(x for x in [str(getattr(user, "first_name", "") or "").strip(), str(getattr(user, "last_name", "") or "").strip()] if x).strip()
    username = str(getattr(user, "username", "") or "").strip()
    if username:
        return f"{full or username} (@{username})"[:120]
    return (full or str(getattr(user, "id", "") or ""))[:120]


def _v149_tenant_id(tenant_id: str | None = None, target_chat_id: int | None = None) -> str:
    if tenant_id:
        return str(tenant_id)
    ctx = str(getattr(_V149_GOOGLE_CONTEXT, "tenant_id", "") or "")
    if ctx:
        return ctx
    if target_chat_id is not None:
        try:
            resolved = tenant_id_for_chat(int(target_chat_id), create=False)
            if resolved:
                return str(resolved)
        except Exception:
            pass
    try:
        return str(tenant_current_id(target_chat_id) or TENANT_PLATFORM_ID)
    except Exception:
        return str(TENANT_PLATFORM_ID)


def _v149_chat_belongs_to_tenant(chat_id: int, tenant_id: str) -> bool:
    """Require an explicit v148 binding; fallback-to-platform is not enough for isolation."""
    try:
        return int(chat_id) in {int(x) for x in tenant_chat_ids(str(tenant_id))}
    except Exception:
        return False


@_v149_contextmanager
def tenant_google_context(tenant_id: str | None = None, target_chat_id: int | None = None):
    previous = getattr(_V149_GOOGLE_CONTEXT, "tenant_id", None)
    _V149_GOOGLE_CONTEXT.tenant_id = _v149_tenant_id(tenant_id, target_chat_id)
    try:
        yield _V149_GOOGLE_CONTEXT.tenant_id
    finally:
        _V149_GOOGLE_CONTEXT.tenant_id = previous


def tenant_google_config(tenant_id: str | None = None, create: bool = True) -> dict:
    tid = _v149_tenant_id(tenant_id)
    row = tenant_get(tid)
    if not isinstance(row, dict):
        if not create:
            return {}
        raise RuntimeError("Пространство Google не найдено")
    cfg = row.get("google_v149")
    if not isinstance(cfg, dict):
        if not create:
            return {}
        cfg = {}
        row["google_v149"] = cfg
    cfg.setdefault("schema_version", V149_GOOGLE_SCHEMA_VERSION)
    cfg.setdefault("credentials_sealed", "")
    cfg.setdefault("credential_fingerprint", "")
    cfg.setdefault("service_account_email", "")
    cfg.setdefault("owner_google_email", "")
    cfg.setdefault("spreadsheet_id", "")
    cfg.setdefault("spreadsheet_title", "")
    cfg.setdefault("drive_folder_id", "")
    cfg.setdefault("drive_folder_name", "")
    cfg.setdefault("export_settings", {
        "sheet_enabled": True,
        "drive_enabled": True,
        "sheet_mode": "new_tab",
        "history_limit": 100,
        "error_limit": 50,
    })
    cfg.setdefault("history", [])
    cfg.setdefault("errors", [])
    cfg.setdefault("input_wait", {})
    cfg.setdefault("connected_at", "")
    cfg.setdefault("connected_by", 0)
    cfg.setdefault("updated_at", _v149_now_iso())
    return cfg


def _v149_google_master_key() -> bytes:
    raw = str(_v149_os.getenv("TENANT_GOOGLE_MASTER_KEY") or _v149_os.getenv("GOOGLE_TENANT_MASTER_KEY") or "").strip()
    if len(raw) < 24:
        raise RuntimeError(
            "Для подключения Google пространств задайте в Render секрет TENANT_GOOGLE_MASTER_KEY длиной не менее 24 символов"
        )
    return _v149_hashlib.sha256(raw.encode("utf-8")).digest()


def _v149_stream_xor(payload: bytes, key: bytes, nonce: bytes) -> bytes:
    out = bytearray(len(payload))
    offset = 0
    counter = 0
    while offset < len(payload):
        block = _v149_hmac.new(key, nonce + counter.to_bytes(8, "big"), _v149_hashlib.sha256).digest()
        take = min(len(block), len(payload) - offset)
        for idx in range(take):
            out[offset + idx] = payload[offset + idx] ^ block[idx]
        offset += take
        counter += 1
    return bytes(out)


def _v149_seal_secret(raw: str) -> str:
    master = _v149_google_master_key()
    nonce = _v149_secrets.token_bytes(16)
    enc_key = _v149_hmac.new(master, b"enc:" + nonce, _v149_hashlib.sha256).digest()
    mac_key = _v149_hmac.new(master, b"mac:" + nonce, _v149_hashlib.sha256).digest()
    cipher = _v149_stream_xor(str(raw).encode("utf-8"), enc_key, nonce)
    tag = _v149_hmac.new(mac_key, b"v1:" + nonce + cipher, _v149_hashlib.sha256).digest()
    return "v1." + _v149_base64.urlsafe_b64encode(nonce + tag + cipher).decode("ascii")


def _v149_open_secret(sealed: str) -> str:
    if not str(sealed or "").startswith("v1."):
        raise RuntimeError("Формат зашифрованного Google-ключа не поддерживается")
    try:
        blob = _v149_base64.urlsafe_b64decode(str(sealed).split(".", 1)[1].encode("ascii"))
        nonce, tag, cipher = blob[:16], blob[16:48], blob[48:]
    except Exception as exc:
        raise RuntimeError(f"Google-ключ повреждён: {exc}")
    master = _v149_google_master_key()
    enc_key = _v149_hmac.new(master, b"enc:" + nonce, _v149_hashlib.sha256).digest()
    mac_key = _v149_hmac.new(master, b"mac:" + nonce, _v149_hashlib.sha256).digest()
    expected = _v149_hmac.new(mac_key, b"v1:" + nonce + cipher, _v149_hashlib.sha256).digest()
    if not _v149_hmac.compare_digest(tag, expected):
        raise RuntimeError("Google-ключ не прошёл проверку целостности")
    try:
        return _v149_stream_xor(cipher, enc_key, nonce).decode("utf-8")
    except Exception as exc:
        raise RuntimeError(f"Google-ключ не расшифрован: {exc}")


def _v149_parse_google_service_json(raw: str) -> dict:
    try:
        info = _v149_json.loads(str(raw))
    except Exception as exc:
        raise RuntimeError(f"JSON Google повреждён: {exc}")
    if not isinstance(info, dict):
        raise RuntimeError("JSON Google должен быть объектом")
    if str(info.get("type") or "") != "service_account":
        raise RuntimeError("Нужен JSON ключ типа service_account")
    for key in ("client_email", "private_key", "token_uri"):
        if not str(info.get(key) or "").strip():
            raise RuntimeError(f"В Google JSON отсутствует {key}")
    return info


def tenant_google_set_credentials(tenant_id: str, raw: str, actor_user_id: int) -> dict:
    tid = _v149_tenant_id(tenant_id)
    info = _v149_parse_google_service_json(raw)
    cfg = tenant_google_config(tenant_id)
    cfg["credentials_sealed"] = _v149_seal_secret(_v149_json.dumps(info, ensure_ascii=False, separators=(",", ":")))
    cfg["credential_fingerprint"] = _v149_hashlib.sha256(str(info.get("client_email") or "").encode("utf-8") + str(info.get("private_key_id") or "").encode("utf-8")).hexdigest()[:20]
    cfg["service_account_email"] = str(info.get("client_email") or "")[:250]
    cfg["connected_at"] = _v149_now_iso()
    cfg["connected_by"] = int(actor_user_id or 0)
    cfg["updated_at"] = _v149_now_iso()
    cfg["input_wait"] = {}
    with _V149_GOOGLE_TOKEN_LOCK:
        for key in list(_V149_GOOGLE_TOKEN_CACHE):
            if str(key).startswith(str(tenant_id) + ":"):
                _V149_GOOGLE_TOKEN_CACHE.pop(key, None)
    tenant_google_history(tenant_id, "account_connected", "Google service account подключён", ok=True)
    tenant_google_persist(tid, "tenant_google_update")
    return info


def tenant_google_persist(tenant_id: str, reason: str = "tenant_google") -> None:
    tid = _v149_tenant_id(tenant_id)
    save_data(data, root_only=True)
    try:
        row = tenant_get(tid) or {}
        scope_chat = int(row.get("root_chat_id") or OWNER_ID or 0)
        if scope_chat:
            schedule_delta_backup(scope_chat, delay=0.35, reason=str(reason or "tenant_google"))
    except Exception as exc:
        try: log_error(f"tenant google delta schedule: {exc}")
        except Exception: pass


def tenant_google_history(tenant_id: str, action: str, detail: str = "", ok: bool = True, **meta) -> None:
    cfg = tenant_google_config(tenant_id)
    row = {
        "at": _v149_now_iso(),
        "action": str(action)[:80],
        "ok": bool(ok),
        "detail": str(detail or "")[:500],
    }
    if meta:
        row["meta"] = {str(k)[:50]: str(v)[:250] for k, v in meta.items() if k not in {"credentials", "private_key", "token"}}
    rows = cfg.setdefault("history", [])
    rows.append(row)
    limit = max(10, min(500, int((cfg.get("export_settings") or {}).get("history_limit", 100) or 100)))
    del rows[:-limit]
    cfg["updated_at"] = _v149_now_iso()


def tenant_google_error(tenant_id: str, action: str, exc) -> None:
    tid = _v149_tenant_id(tenant_id)
    cfg = tenant_google_config(tenant_id)
    message = str(exc or "Ошибка")
    # Never store JWTs/private keys accidentally returned by a library.
    message = _v149_re.sub(r"-----BEGIN [^-]+-----.*?-----END [^-]+-----", "[REDACTED KEY]", message, flags=_v149_re.S)
    message = _v149_re.sub(r"(?i)(access_token|refresh_token|private_key|client_secret)\s*[:=]\s*[^,\s]+", r"\1=[REDACTED]", message)
    rows = cfg.setdefault("errors", [])
    rows.append({"at": _v149_now_iso(), "action": str(action)[:80], "error": message[:1000]})
    limit = max(10, min(200, int((cfg.get("export_settings") or {}).get("error_limit", 50) or 50)))
    del rows[:-limit]
    cfg["updated_at"] = _v149_now_iso()
    try:
        tenant_google_persist(tid, "tenant_google_update")
    except Exception:
        pass


def _google_service_account_info(tenant_id: str | None = None) -> dict:
    tid = _v149_tenant_id(tenant_id)
    cfg = tenant_google_config(tid, create=False)
    sealed = str(cfg.get("credentials_sealed") or "") if cfg else ""
    if sealed:
        return _v149_parse_google_service_json(_v149_open_secret(sealed))
    if tid == str(TENANT_PLATFORM_ID) and _V149_PLATFORM_GOOGLE_JSON:
        raw = _V149_PLATFORM_GOOGLE_JSON
        try:
            if raw.lstrip().startswith("{"):
                return _v149_parse_google_service_json(raw)
            return _v149_parse_google_service_json(_v149_base64.b64decode(raw).decode("utf-8"))
        except Exception as exc:
            raise RuntimeError(f"GOOGLE_SERVICE_ACCOUNT_JSON владельца платформы повреждён: {exc}")
    raise RuntimeError("Google-аккаунт этого пространства не подключён. Откройте /google")


def _v149_google_id(value: str, kind: str) -> str:
    raw = str(value or "").strip()
    if kind == "sheet":
        match = _v149_re.search(r"/spreadsheets/d/([A-Za-z0-9_-]+)", raw)
    else:
        match = _v149_re.search(r"/folders/([A-Za-z0-9_-]+)", raw)
    if match:
        raw = match.group(1)
    raw = raw.split("?")[0].split("#")[0].strip().strip("/")
    if not _v149_re.fullmatch(r"[A-Za-z0-9_-]{10,}", raw):
        raise RuntimeError("Неверная ссылка или ID Google " + ("таблицы" if kind == "sheet" else "папки"))
    return raw


def _google_spreadsheet_id(value: str | None = None, tenant_id: str | None = None) -> str:
    if value is not None and str(value).strip():
        return _v149_google_id(str(value), "sheet")
    tid = _v149_tenant_id(tenant_id)
    cfg = tenant_google_config(tid, create=False)
    raw = str((cfg or {}).get("spreadsheet_id") or "")
    if not raw and tid == str(TENANT_PLATFORM_ID):
        raw = _V149_PLATFORM_GOOGLE_SHEET
    if not raw:
        raise RuntimeError("Для этого пространства не выбрана Google Таблица. Откройте /google")
    return _v149_google_id(raw, "sheet")


def tenant_google_drive_folder_id(tenant_id: str | None = None) -> str:
    tid = _v149_tenant_id(tenant_id)
    raw = str(tenant_google_config(tid, create=False).get("drive_folder_id") or "")
    if not raw:
        raise RuntimeError("Для этого пространства не выбрана папка Google Drive. Откройте /google")
    return _v149_google_id(raw, "folder")


def _google_access_token(tenant_id: str | None = None) -> str:
    tid = _v149_tenant_id(tenant_id)
    info = _google_service_account_info(tid)
    fingerprint = _v149_hashlib.sha256((str(info.get("client_email")) + str(info.get("private_key_id"))).encode("utf-8")).hexdigest()[:20]
    cache_key = f"{tid}:{fingerprint}"
    with _V149_GOOGLE_TOKEN_LOCK:
        now = _v149_time.time()
        cached = _V149_GOOGLE_TOKEN_CACHE.get(cache_key) or {}
        if cached.get("token") and now < float(cached.get("expires_at", 0)) - 120:
            return str(cached["token"])
        header = {"alg": "RS256", "typ": "JWT"}
        claims = {
            "iss": info["client_email"],
            "scope": "https://www.googleapis.com/auth/spreadsheets https://www.googleapis.com/auth/drive",
            "aud": info.get("token_uri") or "https://oauth2.googleapis.com/token",
            "iat": int(now),
            "exp": int(now) + 3600,
        }
        signing_input = (
            _b64url(_v149_json.dumps(header, separators=(",", ":")).encode("utf-8"))
            + "."
            + _b64url(_v149_json.dumps(claims, separators=(",", ":")).encode("utf-8"))
        ).encode("ascii")
        signature = _google_sign_rs256(signing_input, info["private_key"])
        assertion = signing_input.decode("ascii") + "." + _b64url(signature)
        response = _google_request_guarded(
            "oauth", requests.post,
            info.get("token_uri") or "https://oauth2.googleapis.com/token",
            data={"grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer", "assertion": assertion},
            timeout=30, attempts=2,
        )
        if response.status_code >= 300:
            raise RuntimeError(f"Google OAuth {response.status_code}: {response.text[:500]}")
        payload = response.json()
        token = str(payload.get("access_token") or "")
        if not token:
            raise RuntimeError("Google OAuth не вернул access_token")
        _V149_GOOGLE_TOKEN_CACHE[cache_key] = {"token": token, "expires_at": now + int(payload.get("expires_in", 3600) or 3600)}
        return token


def _google_sheets_create_category_report(title: str, rows: list[list], layout: str = "category", annotations_override: dict | None = None, include_annotations: bool = True, tenant_id: str | None = None, target_chat_id: int | None = None) -> str:
    if not callable(_V149_BASE_GOOGLE_SHEETS_CREATE):
        raise RuntimeError("Модуль Google Sheets не загружен")
    tid = _v149_tenant_id(tenant_id, target_chat_id)
    if target_chat_id is not None and not _v149_chat_belongs_to_tenant(int(target_chat_id), tid):
        raise RuntimeError("Google export blocked: target chat is not connected to this space")
    cfg = tenant_google_config(tid)
    if not bool((cfg.get("export_settings") or {}).get("sheet_enabled", True)):
        raise RuntimeError("Выгрузка в Google Sheets выключена для этого пространства")
    try:
        with tenant_google_context(tid):
            url = _V149_BASE_GOOGLE_SHEETS_CREATE(
                title, rows, layout=layout,
                annotations_override=annotations_override,
                include_annotations=include_annotations,
            )
        tenant_google_history(tid, "sheets_export", title, ok=True, chat_id=target_chat_id or 0, url=url)
        tenant_google_persist(tid, "tenant_google_update")
        return url
    except Exception as exc:
        tenant_google_error(tid, "sheets_export", exc)
        raise


def tenant_google_upload_export(local_path: str, display_name: str, target_chat_id: int, mime_type: str | None = None) -> str:
    tid = _v149_tenant_id(target_chat_id=target_chat_id)
    if not _v149_chat_belongs_to_tenant(int(target_chat_id), tid):
        raise RuntimeError("Google Drive export blocked: target chat is not connected to this space")
    cfg = tenant_google_config(tid)
    if not bool((cfg.get("export_settings") or {}).get("drive_enabled", True)):
        raise RuntimeError("Выгрузка в Google Drive выключена для этого пространства")
    folder_id = tenant_google_drive_folder_id(tid)
    token = _google_access_token(tid)
    mime_type = str(mime_type or _v149_mimetypes.guess_type(display_name)[0] or "application/octet-stream")
    headers = {"Authorization": f"Bearer {token}"}
    metadata = {
        "name": str(display_name or _v149_Path(local_path).name)[:240],
        "parents": [folder_id],
        "appProperties": {"tenant_id": tid, "source_chat_id": str(int(target_chat_id))},
    }
    try:
        with open(local_path, "rb") as fh:
            response = _google_request_guarded(
                "drive_upload", requests.post,
                "https://www.googleapis.com/upload/drive/v3/files",
                headers=headers,
                params={"uploadType": "multipart", "fields": "id,name,webViewLink,parents"},
                files={
                    "metadata": (None, _v149_json.dumps(metadata, ensure_ascii=False), "application/json; charset=UTF-8"),
                    "file": (metadata["name"], fh, mime_type),
                },
                timeout=120, attempts=1,
            )
        if response.status_code >= 300:
            raise RuntimeError(f"Google Drive upload {response.status_code}: {response.text[:700]}")
        payload = response.json()
        file_id = str(payload.get("id") or "")
        url = str(payload.get("webViewLink") or (f"https://drive.google.com/file/d/{file_id}/view" if file_id else ""))
        tenant_google_history(tid, "drive_export", metadata["name"], ok=True, chat_id=target_chat_id, file_id=file_id)
        tenant_google_persist(tid, "tenant_google_update")
        return url
    except Exception as exc:
        tenant_google_error(tid, "drive_export", exc)
        raise


def tenant_google_create_spreadsheet(tenant_id: str, title: str = "Финансы бота") -> str:
    tid = _v149_tenant_id(tenant_id)
    token = _google_access_token(tid)
    folder_id = tenant_google_drive_folder_id(tid)
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    metadata = {
        "name": str(title or "Финансы бота")[:200],
        "mimeType": "application/vnd.google-apps.spreadsheet",
        "parents": [folder_id],
        "appProperties": {"tenant_id": tid},
    }
    response = _google_request_guarded(
        "drive_create_sheet", requests.post,
        "https://www.googleapis.com/drive/v3/files",
        headers=headers,
        params={"fields": "id,name,webViewLink,parents"},
        json=metadata,
        timeout=60, attempts=1,
    )
    if response.status_code >= 300:
        exc = RuntimeError(f"Google create spreadsheet {response.status_code}: {response.text[:700]}")
        tenant_google_error(tid, "create_spreadsheet", exc)
        raise exc
    payload = response.json()
    spreadsheet_id = _v149_google_id(str(payload.get("id") or ""), "sheet")
    cfg = tenant_google_config(tid)
    cfg["spreadsheet_id"] = spreadsheet_id
    cfg["spreadsheet_title"] = str(payload.get("name") or title)[:200]
    cfg["updated_at"] = _v149_now_iso()
    tenant_google_history(tid, "create_spreadsheet", cfg["spreadsheet_title"], ok=True, spreadsheet_id=spreadsheet_id)
    tenant_google_persist(tid, "tenant_google_update")
    return str(payload.get("webViewLink") or f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit")


def tenant_google_test(tenant_id: str) -> tuple[bool, str]:
    tid = _v149_tenant_id(tenant_id)
    try:
        token = _google_access_token(tid)
        headers = {"Authorization": f"Bearer {token}"}
        parts = []
        folder_id = str(tenant_google_config(tid).get("drive_folder_id") or "")
        if folder_id:
            response = _google_request_guarded(
                "drive_folder_test", requests.get,
                f"https://www.googleapis.com/drive/v3/files/{_v149_google_id(folder_id, 'folder')}",
                headers=headers,
                params={"fields": "id,name,mimeType,trashed"},
                timeout=30, attempts=2,
            )
            if response.status_code >= 300:
                raise RuntimeError(f"Drive folder {response.status_code}: {response.text[:500]}")
            payload = response.json()
            tenant_google_config(tid)["drive_folder_name"] = str(payload.get("name") or "")[:200]
            parts.append("Drive: доступ есть")
        sheet_raw = str(tenant_google_config(tid).get("spreadsheet_id") or "")
        if not sheet_raw and tid == str(TENANT_PLATFORM_ID):
            sheet_raw = _V149_PLATFORM_GOOGLE_SHEET
        if sheet_raw:
            sid = _v149_google_id(sheet_raw, "sheet")
            response = _google_request_guarded(
                "sheet_test", requests.get,
                f"https://sheets.googleapis.com/v4/spreadsheets/{sid}",
                headers=headers,
                params={"fields": "spreadsheetId,properties.title"},
                timeout=30, attempts=2,
            )
            if response.status_code >= 300:
                raise RuntimeError(f"Sheets {response.status_code}: {response.text[:500]}")
            payload = response.json()
            tenant_google_config(tid)["spreadsheet_title"] = str((payload.get("properties") or {}).get("title") or "")[:200]
            parts.append("Sheets: доступ есть")
        if not parts:
            parts.append("Аккаунт подключён; задайте таблицу и папку")
        tenant_google_history(tid, "connection_test", "; ".join(parts), ok=True)
        tenant_google_persist(tid, "tenant_google_update")
        return True, "✅ " + "; ".join(parts)
    except Exception as exc:
        tenant_google_error(tid, "connection_test", exc)
        return False, "❌ " + str(exc)[:700]


def _v149_mask_id(value: str) -> str:
    raw = str(value or "")
    if len(raw) <= 10:
        return raw or "не задан"
    return raw[:6] + "…" + raw[-4:]


def tenant_google_status_text(tenant_id: str) -> str:
    tid = _v149_tenant_id(tenant_id)
    row = tenant_get(tid) or {}
    cfg = tenant_google_config(tid)
    env_fallback = tid == str(TENANT_PLATFORM_ID) and not cfg.get("credentials_sealed") and bool(_V149_PLATFORM_GOOGLE_JSON)
    account = str(cfg.get("service_account_email") or ("Render Environment" if env_fallback else "не подключён"))
    sheet = str(cfg.get("spreadsheet_title") or "")
    folder = str(cfg.get("drive_folder_name") or "")
    return (
        f"☁️ GOOGLE · {row.get('name') or tid}\n\n"
        f"Аккаунт: {account}\n"
        f"Google владельца: {cfg.get('owner_google_email') or 'не указан'}\n"
        f"Таблица: {sheet or _v149_mask_id(cfg.get('spreadsheet_id') or (_V149_PLATFORM_GOOGLE_SHEET if env_fallback else ''))}\n"
        f"Папка Drive: {folder or _v149_mask_id(cfg.get('drive_folder_id'))}\n"
        f"Выгрузка Sheets: {'включена' if bool((cfg.get('export_settings') or {}).get('sheet_enabled', True)) else 'выключена'}\n"
        f"Выгрузка Drive: {'включена' if bool((cfg.get('export_settings') or {}).get('drive_enabled', True)) else 'выключена'}\n"
        f"История: {len(cfg.get('history') or [])}\n"
        f"Ошибки: {len(cfg.get('errors') or [])}\n\n"
        "Данные, токены, таблица, папка, история и ошибки принадлежат только этому пространству.\n"
        "Для подключения нужен JSON ключ service_account и общий мастер-ключ TENANT_GOOGLE_MASTER_KEY в Render."
    )


def tenant_google_keyboard(tenant_id: str):
    tid = str(tenant_id)
    cfg = tenant_google_config(tid)
    kb = types.InlineKeyboardMarkup(row_width=1)
    kb.row(IB("🔑 Подключить / заменить аккаунт", callback_data="v149:google:connect"))
    kb.row(IB("📊 Указать Google Таблицу", callback_data="v149:google:sheet"))
    kb.row(IB("📁 Указать папку Google Drive", callback_data="v149:google:folder"))
    kb.row(IB("👤 Указать email Google владельца", callback_data="v149:google:owner_email"))
    settings = cfg.get("export_settings") or {}
    kb.row(IB(f"📊 Выгрузка Sheets: {'ВКЛ' if settings.get('sheet_enabled', True) else 'ВЫКЛ'}", callback_data="v149:google:toggle_sheet"))
    kb.row(IB(f"📁 Выгрузка Drive: {'ВКЛ' if settings.get('drive_enabled', True) else 'ВЫКЛ'}", callback_data="v149:google:toggle_drive"))
    kb.row(IB("➕ Создать таблицу в папке", callback_data="v149:google:create_sheet"))
    kb.row(IB("🧪 Проверить подключение", callback_data="v149:google:test"))
    kb.row(
        IB(f"📜 История ({len(cfg.get('history') or [])})", callback_data="v149:google:history"),
        IB(f"⚠️ Ошибки ({len(cfg.get('errors') or [])})", callback_data="v149:google:errors"),
    )
    kb.row(IB("🧹 Отключить Google", callback_data="v149:google:disconnect_confirm"))
    return kb


def _v149_google_wait(tenant_id: str, kind: str, chat_id: int, user_id: int) -> None:
    tid = _v149_tenant_id(tenant_id)
    cfg = tenant_google_config(tid)
    cfg["input_wait"] = {
        "kind": str(kind), "chat_id": int(chat_id), "user_id": int(user_id),
        "expires_at": _v149_time.time() + 900,
    }
    cfg["updated_at"] = _v149_now_iso()
    tenant_google_persist(tid, "tenant_google_update")


def _v149_google_can_manage(chat_id: int, user_id: int, owner_only: bool = True) -> tuple[bool, str]:
    tid = str(tenant_id_for_chat(int(chat_id), create=True, actor_user_id=int(user_id)) or TENANT_PLATFORM_ID)
    return bool(tenant_can_manage(int(user_id), tid, owner_only=owner_only)), tid


def tenant_google_handle_message(msg) -> bool:
    """Called near the top of the common non-command message router."""
    try:
        chat_id = int(msg.chat.id)
        user_id = _v149_actor_id(msg)
        # Do not create/claim a tenant for every ordinary message. Only consume a
        # message when this already registered tenant has an active Google input wait.
        tid = str(tenant_id_for_chat(chat_id, create=False) or "")
        if not tid:
            return False
        cfg = tenant_google_config(tid, create=False)
        wait = (cfg or {}).get("input_wait") or {}
        if not wait:
            return False
        if not tenant_can_manage(user_id, tid, owner_only=True):
            return False
        if not wait or int(wait.get("chat_id") or 0) != chat_id or int(wait.get("user_id") or 0) != user_id:
            return False
        if _v149_time.time() > float(wait.get("expires_at") or 0):
            cfg["input_wait"] = {}
            tenant_google_persist(tid, "tenant_google_update")
            return False
        kind = str(wait.get("kind") or "")
        if kind == "credentials":
            if str(getattr(msg, "content_type", "")) != "document":
                send_and_auto_delete(chat_id, "Пришлите JSON-файл service_account как документ.", 12)
                return True
            document = getattr(msg, "document", None)
            if not document or int(getattr(document, "file_size", 0) or 0) > 250_000:
                send_and_auto_delete(chat_id, "JSON-файл отсутствует или слишком большой.", 12)
                return True
            filename = str(getattr(document, "file_name", "") or "").lower()
            if filename and not filename.endswith(".json"):
                send_and_auto_delete(chat_id, "Нужен файл с расширением .json.", 12)
                return True
            file_info = bot.get_file(document.file_id)
            raw_bytes = bot.download_file(file_info.file_path)
            raw = bytes(raw_bytes).decode("utf-8")
            info = tenant_google_set_credentials(tid, raw, user_id)
            try:
                bot.delete_message(chat_id, msg.message_id)
            except Exception:
                pass
            bot.send_message(chat_id, f"✅ Google-аккаунт подключён: {info.get('client_email')}\n\nТеперь укажите свою таблицу и папку Drive через /google.")
            return True
        if str(getattr(msg, "content_type", "")) != "text":
            send_and_auto_delete(chat_id, "Пришлите ссылку или ID текстом.", 10)
            return True
        value = str(getattr(msg, "text", "") or "").strip()
        if kind == "sheet":
            cfg["spreadsheet_id"] = _v149_google_id(value, "sheet")
            cfg["spreadsheet_title"] = ""
            action = "sheet_configured"
            text = "✅ Google Таблица сохранена."
        elif kind == "folder":
            cfg["drive_folder_id"] = _v149_google_id(value, "folder")
            cfg["drive_folder_name"] = ""
            action = "drive_folder_configured"
            text = "✅ Папка Google Drive сохранена."
        elif kind == "owner_email":
            if not _v149_re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", value):
                raise RuntimeError("Неверный email")
            cfg["owner_google_email"] = value[:250]
            action = "owner_email_configured"
            text = "✅ Email владельца Google сохранён."
        else:
            return False
        cfg["input_wait"] = {}
        cfg["updated_at"] = _v149_now_iso()
        tenant_google_history(tid, action, text, ok=True)
        tenant_google_persist(tid, "tenant_google_update")
        try:
            bot.delete_message(chat_id, msg.message_id)
        except Exception:
            pass
        bot.send_message(chat_id, text, reply_markup=tenant_google_keyboard(tid))
        return True
    except Exception as exc:
        try:
            tid = str(tenant_id_for_chat(int(msg.chat.id), create=False) or TENANT_PLATFORM_ID)
            tenant_google_error(tid, "input", exc)
            send_and_auto_delete(int(msg.chat.id), "❌ " + str(exc)[:700], 20)
        except Exception:
            pass
        return True


def _v149_google_history_text(tenant_id: str, errors: bool = False) -> str:
    cfg = tenant_google_config(tenant_id)
    rows = list(cfg.get("errors" if errors else "history") or [])[-20:]
    title = "⚠️ ОШИБКИ GOOGLE" if errors else "📜 ИСТОРИЯ GOOGLE"
    if not rows:
        return title + "\n\nПока пусто."
    lines = [title, ""]
    for row in reversed(rows):
        if errors:
            lines.append(f"{row.get('at')} · {row.get('action')}\n{row.get('error')}")
        else:
            mark = "✅" if row.get("ok") else "❌"
            lines.append(f"{mark} {row.get('at')} · {row.get('action')}\n{row.get('detail')}")
    return "\n\n".join(lines)[:3900]


@bot.message_handler(commands=["google", "google_space", "google_tenant"])
def cmd_v149_google(msg):
    try:
        schedule_command_delete(msg)
    except Exception:
        pass
    chat_id = int(msg.chat.id); user_id = _v149_actor_id(msg)
    ok, tid = _v149_google_can_manage(chat_id, user_id, owner_only=True)
    if not ok:
        send_and_auto_delete(chat_id, "❌ Google пространства может настраивать только его владелец.", 12)
        return
    bot.send_message(chat_id, tenant_google_status_text(tid), reply_markup=tenant_google_keyboard(tid), disable_web_page_preview=True)


@bot.message_handler(commands=["google_connect"])
def cmd_v149_google_connect(msg):
    try: schedule_command_delete(msg)
    except Exception: pass
    chat_id = int(msg.chat.id); user_id = _v149_actor_id(msg)
    ok, tid = _v149_google_can_manage(chat_id, user_id, owner_only=True)
    if not ok:
        send_and_auto_delete(chat_id, "❌ Недостаточно прав.", 10); return
    _v149_google_wait(tid, "credentials", chat_id, user_id)
    bot.send_message(chat_id, "🔑 Пришлите JSON-ключ Google service_account как документ.\n\nСообщение с файлом бот удалит после чтения. Ключ будет храниться зашифрованно.")


@bot.message_handler(commands=["google_sheet"])
def cmd_v149_google_sheet(msg):
    try: schedule_command_delete(msg)
    except Exception: pass
    chat_id = int(msg.chat.id); user_id = _v149_actor_id(msg)
    ok, tid = _v149_google_can_manage(chat_id, user_id, owner_only=True)
    if not ok: send_and_auto_delete(chat_id, "❌ Недостаточно прав.", 10); return
    parts = str(getattr(msg, "text", "") or "").split(maxsplit=1)
    if len(parts) > 1:
        cfg = tenant_google_config(tid); cfg["spreadsheet_id"] = _v149_google_id(parts[1], "sheet"); cfg["spreadsheet_title"] = ""; cfg["updated_at"] = _v149_now_iso(); tenant_google_history(tid, "sheet_configured", "Google Таблица сохранена", ok=True); tenant_google_persist(tid, "tenant_google_update")
        bot.send_message(chat_id, "✅ Google Таблица сохранена.", reply_markup=tenant_google_keyboard(tid)); return
    _v149_google_wait(tid, "sheet", chat_id, user_id)
    bot.send_message(chat_id, "📊 Пришлите ссылку или ID Google Таблицы. Таблица должна быть открыта вашему service_account как редактору.")


@bot.message_handler(commands=["google_drive"])
def cmd_v149_google_drive(msg):
    try: schedule_command_delete(msg)
    except Exception: pass
    chat_id = int(msg.chat.id); user_id = _v149_actor_id(msg)
    ok, tid = _v149_google_can_manage(chat_id, user_id, owner_only=True)
    if not ok: send_and_auto_delete(chat_id, "❌ Недостаточно прав.", 10); return
    parts = str(getattr(msg, "text", "") or "").split(maxsplit=1)
    if len(parts) > 1:
        cfg = tenant_google_config(tid); cfg["drive_folder_id"] = _v149_google_id(parts[1], "folder"); cfg["drive_folder_name"] = ""; cfg["updated_at"] = _v149_now_iso(); tenant_google_history(tid, "drive_folder_configured", "Папка Drive сохранена", ok=True); tenant_google_persist(tid, "tenant_google_update")
        bot.send_message(chat_id, "✅ Папка Google Drive сохранена.", reply_markup=tenant_google_keyboard(tid)); return
    _v149_google_wait(tid, "folder", chat_id, user_id)
    bot.send_message(chat_id, "📁 Пришлите ссылку или ID папки Google Drive. Папка должна быть открыта вашему service_account как редактору.")


@bot.message_handler(commands=["google_email"])
def cmd_v149_google_email(msg):
    try: schedule_command_delete(msg)
    except Exception: pass
    chat_id = int(msg.chat.id); user_id = _v149_actor_id(msg)
    ok, tid = _v149_google_can_manage(chat_id, user_id, owner_only=True)
    if not ok: send_and_auto_delete(chat_id, "❌ Недостаточно прав.", 10); return
    parts = str(getattr(msg, "text", "") or "").split(maxsplit=1)
    if len(parts) > 1:
        value = parts[1].strip()
        if not _v149_re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", value):
            send_and_auto_delete(chat_id, "❌ Неверный email.", 10); return
        cfg = tenant_google_config(tid); cfg["owner_google_email"] = value[:250]; cfg["updated_at"] = _v149_now_iso()
        tenant_google_history(tid, "owner_email_configured", "Email Google владельца сохранён", ok=True)
        tenant_google_persist(tid, "tenant_google_update")
        bot.send_message(chat_id, "✅ Email Google владельца сохранён.", reply_markup=tenant_google_keyboard(tid)); return
    _v149_google_wait(tid, "owner_email", chat_id, user_id)
    bot.send_message(chat_id, "👤 Пришлите email Google-аккаунта владельца пространства.")


# ─────────────────────────────────────────────────────────────
# Reminder settings, dynamic grouping and /vyapl
# ─────────────────────────────────────────────────────────────
def _v149_reminder_settings(tenant_id: str | None = None) -> dict:
    """Tenant-level reminder metadata (history and per-chat setting map)."""
    tid = _v149_tenant_id(tenant_id)
    row = tenant_get(tid)
    if not isinstance(row, dict):
        return {}
    settings = row.setdefault("settings", {})
    settings.setdefault("reminder_completion_history_v149", [])
    settings.setdefault("reminder_chat_settings_v149", {})
    return settings


def _v149_reminder_chat_settings(tenant_id: str | None = None, chat_id: int | None = None) -> dict:
    tid = _v149_tenant_id(tenant_id, chat_id)
    row = tenant_get(tid) or {}
    if chat_id is None:
        try:
            chat_id = int(current_state_chat_id() or row.get("root_chat_id") or 0)
        except Exception:
            chat_id = int(row.get("root_chat_id") or 0)
    try:
        cid = int(chat_id or 0)
    except Exception:
        cid = 0
    settings = _v149_reminder_settings(tid)
    mapping = settings.setdefault("reminder_chat_settings_v149", {})
    key = str(cid)
    item = mapping.get(key)
    if not isinstance(item, dict):
        # Safely migrate the early tenant-wide draft values as defaults only.
        item = {
            "merge_enabled": bool(settings.get("reminder_merge_enabled_v149", False)),
            "show_complete_command": bool(settings.get("reminder_show_complete_command_v149", False)),
        }
        mapping[key] = item
    item.setdefault("merge_enabled", False)
    item.setdefault("show_complete_command", False)
    item["chat_id"] = cid
    item["tenant_id"] = tid
    return item


def reminder_merge_enabled(tenant_id: str | None = None, chat_id: int | None = None) -> bool:
    return bool(_v149_reminder_chat_settings(tenant_id, chat_id).get("merge_enabled", False))


def reminder_show_complete_command(tenant_id: str | None = None, chat_id: int | None = None) -> bool:
    return bool(_v149_reminder_chat_settings(tenant_id, chat_id).get("show_complete_command", False))


def _v149_reminder_all_rows(include_completed: bool = False) -> list[tuple[int, dict]]:
    # Background worker has no Telegram context, therefore v148 returns every tenant.
    return list(_reminder_items(include_completed=include_completed))


def _v149_reminder_chat_ids(cfg: dict) -> list[int]:
    result = []
    for raw in (cfg or {}).get("chat_ids") or []:
        try:
            cid = int(raw)
        except Exception:
            continue
        if cid not in result:
            result.append(cid)
    return result


def _v149_reminder_cfg_tenant(cfg: dict) -> str:
    return str((cfg or {}).get("tenant_id") or TENANT_PLATFORM_ID)


def _v149_reminder_chat_allowed(cfg: dict, chat_id: int) -> bool:
    tid = _v149_reminder_cfg_tenant(cfg)
    return _v149_chat_belongs_to_tenant(int(chat_id), tid)


def _v149_reminder_active_now(cfg: dict, now_dt) -> bool:
    return bool(
        cfg and cfg.get("enabled") and not _reminder_is_completed(cfg)
        and str(cfg.get("text") or "").strip()
        and _reminder_date_allowed(now_dt, cfg)
        and _reminder_time_allowed(now_dt, cfg)
    )


def _v149_group_state_root() -> dict:
    root = data.setdefault("_global_settings", {}).setdefault("reminder_groups_v149", {})
    return root if isinstance(root, dict) else {}


def _v149_group_key(chat_id: int) -> str:
    return str(int(chat_id))


def _v149_completion_history(tenant_id: str) -> list:
    return _v149_reminder_settings(tenant_id).setdefault("reminder_completion_history_v149", [])


def _v149_reminder_message_text(reminder_id: int, cfg: dict, chat_id: int, active_count: int = 1) -> str:
    lines = ["НАПОМИНАЛКА🕰️", "", str(cfg.get("text") or "").strip()]
    if reminder_show_complete_command(chat_id=chat_id):
        if active_count <= 1:
            lines += ["", "Выполнить: /vyapl"]
        else:
            lines += ["", f"Выполнить: /vyapl_{int(reminder_id)}"]
    return "\n".join(lines)[:4000]


def _v149_group_message_text(chat_id: int, members: list[tuple[int, dict]]) -> str:
    lines = ["НАПОМИНАЛКА🕰️", ""]
    show = reminder_show_complete_command(chat_id=chat_id)
    budget = 3900
    for idx, (rid, cfg) in enumerate(members, 1):
        text = str(cfg.get("text") or "").strip()
        block = f"{idx}. {text}"
        if show:
            block += f"\n   /vyapl_{int(rid)}"
        if len("\n".join(lines + [block])) > budget:
            lines.append("…")
            break
        lines.append(block)
    if show and len(members) == 1:
        lines += ["", "Можно также: /vyapl"]
    return "\n".join(lines)[:4000]


def _v149_delete_message(chat_id: int, message_id: int) -> None:
    if not message_id:
        return
    try:
        bot.delete_message(int(chat_id), int(message_id))
    except Exception:
        pass


def _v149_send_or_edit_group(chat_id: int, text: str, old_message_id: int = 0) -> tuple[bool, int]:
    if old_message_id:
        try:
            bot.edit_message_text(text, chat_id=int(chat_id), message_id=int(old_message_id))
            return True, int(old_message_id)
        except Exception as exc:
            if "message is not modified" in str(exc).lower():
                return True, int(old_message_id)
    try:
        sent = bot.send_message(int(chat_id), text)
        new_id = int(sent.message_id)
        if old_message_id and old_message_id != new_id:
            _v149_delete_message(chat_id, old_message_id)
        return True, new_id
    except Exception as exc:
        log_error(f"v149 reminder group send {chat_id}: {exc}")
        return False, int(old_message_id or 0)


def _v149_send_individual(chat_id: int, reminder_id: int, cfg: dict, active_count: int) -> tuple[bool, int]:
    old_mid = int((cfg.get("last_message_ids") or {}).get(str(chat_id)) or 0)
    try:
        sent = bot.send_message(int(chat_id), _v149_reminder_message_text(reminder_id, cfg, chat_id, active_count))
        new_mid = int(sent.message_id)
        if old_mid and old_mid != new_mid:
            _v149_delete_message(chat_id, old_mid)
        return True, new_mid
    except Exception as exc:
        log_error(f"v149 reminder {reminder_id} send {chat_id}: {exc}")
        return False, old_mid


def _v149_cleanup_legacy_group_state_once() -> bool:
    """Remove v142 fixed-2h group messages/state without changing reminder intervals again."""
    gs = data.setdefault("_global_settings", {})
    if bool(gs.get("reminder_groups_v149_migrated")):
        return False
    old = gs.pop("reminder_groups_v142", {})
    if isinstance(old, dict):
        for key, row in list(old.items()):
            if not isinstance(row, dict):
                continue
            try:
                cid = int(row.get("target_chat_id") or str(key).rsplit(":", 1)[-1])
                mid = int(row.get("last_message_id") or 0)
            except Exception:
                cid = mid = 0
            if cid and mid:
                _v149_delete_message(cid, mid)
    gs["reminder_groups_v149_migrated"] = True
    return True


def _v149_reminder_batch_job(force_chat_id: int | None = None) -> None:
    """One atomic reminder cycle for all chats.

    Every reminder retains its own interval. A merged chat refreshes whenever any member is due,
    so the visible common message follows the smallest currently active interval. Membership
    changes (completed/outside hours) refresh the same message even when no reminder is due.
    """
    if not _V149_REMINDER_BATCH_LOCK.acquire(blocking=False):
        return
    try:
        legacy_migrated = _v149_cleanup_legacy_group_state_once()
        now_dt = now_local()
        due_ids = set()
        snapshots = {}
        active_by_chat = _v149_defaultdict(list)
        ended_changed = False
        with _REMINDER_CONFIG_LOCK:
            for rid, cfg in _v149_reminder_all_rows(include_completed=False):
                rid = int(rid)
                if _reminder_end_has_passed(cfg, now_dt):
                    _reminder_mark_completed(rid, cfg, "end_date_finished", delete_messages=True)
                    ended_changed = True
                    continue
                if _reminder_due_now(cfg, now_dt):
                    if _reminder_date_allowed(now_dt, cfg) and _reminder_time_allowed(now_dt, cfg):
                        if force_chat_id is None or int(force_chat_id) in _v149_reminder_chat_ids(cfg):
                            due_ids.add(rid)
                    else:
                        next_dt = _reminder_next_valid_start(now_dt, cfg)
                        cfg["next_run_at"] = next_dt.isoformat(timespec="seconds") if next_dt else ""
                        if next_dt is None:
                            _reminder_mark_completed(rid, cfg, "schedule_finished", delete_messages=True)
                        else:
                            _reminder_touch(cfg)
                        ended_changed = True
                if _v149_reminder_active_now(cfg, now_dt):
                    snap = _v149_deepcopy(cfg)
                    snapshots[rid] = snap
                    for cid in _v149_reminder_chat_ids(snap):
                        if force_chat_id is not None and int(cid) != int(force_chat_id):
                            continue
                        if _v149_reminder_chat_allowed(snap, cid):
                            active_by_chat[int(cid)].append((rid, snap))
                        else:
                            try: bot_journal("tenant_reminder_cross_chat_blocked", int(cid), f"reminder_id={rid} tenant={_v149_reminder_cfg_tenant(snap)}", "WARN")
                            except Exception: pass

            state_snapshot = _v149_deepcopy(_v149_group_state_root())

        individual_updates = {}
        group_updates = {}
        group_remove_individual = _v149_defaultdict(list)
        sent_for_rid = _v149_defaultdict(bool)
        chats_to_consider = set(active_by_chat)
        if force_chat_id is None:
            chats_to_consider.update(int(k) for k in state_snapshot.keys() if str(k).lstrip("-").isdigit())
        else:
            chats_to_consider.add(int(force_chat_id))

        for cid in sorted(chats_to_consider):
            members = sorted(active_by_chat.get(cid, []), key=lambda row: row[0])
            state = state_snapshot.get(_v149_group_key(cid), {}) or {}
            old_group_mid = int(state.get("last_message_id") or 0)
            old_member_ids = [int(x) for x in (state.get("member_ids") or []) if str(x).isdigit()]
            current_ids = [rid for rid, _cfg in members]
            due_here = [rid for rid, _cfg in members if rid in due_ids]
            merge = reminder_merge_enabled(chat_id=cid)
            keep_group = bool(merge and (len(members) >= 2 or old_group_mid))
            membership_changed = current_ids != old_member_ids

            if keep_group and not members:
                _v149_delete_message(cid, old_group_mid)
                group_updates[cid] = None
                continue

            if keep_group:
                if due_here or membership_changed or force_chat_id is not None:
                    ok, message_id = _v149_send_or_edit_group(cid, _v149_group_message_text(cid, members), old_group_mid)
                    if ok:
                        for rid, cfg in members:
                            sent_for_rid[rid] = sent_for_rid[rid] or (rid in due_ids)
                            old_individual = int((cfg.get("last_message_ids") or {}).get(str(cid)) or 0)
                            if old_individual:
                                group_remove_individual[rid].append((cid, old_individual))
                        group_updates[cid] = {
                            "last_message_id": message_id,
                            "member_ids": current_ids,
                            "last_sent_at": _v149_now_iso(),
                            "tenant_id": _v149_tenant_id(target_chat_id=cid),
                        }
                continue

            if old_group_mid:
                _v149_delete_message(cid, old_group_mid)
                group_updates[cid] = None

            active_count = len(members)
            for rid, cfg in members:
                if rid not in due_ids:
                    continue
                ok, message_id = _v149_send_individual(cid, rid, cfg, active_count)
                if ok:
                    sent_for_rid[rid] = True
                    individual_updates[(rid, cid)] = message_id

        for rid, pairs in group_remove_individual.items():
            for cid, mid in pairs:
                _v149_delete_message(cid, mid)

        changed = bool(ended_changed or legacy_migrated)
        with _REMINDER_CONFIG_LOCK:
            state_root = _v149_group_state_root()
            for cid, update in group_updates.items():
                key = _v149_group_key(cid)
                if update is None:
                    state_root.pop(key, None)
                else:
                    state_root[key] = update
                changed = True
            for (rid, cid), mid in individual_updates.items():
                cfg = _reminder_cfg(rid)
                if cfg:
                    cfg.setdefault("last_message_ids", {})[str(cid)] = int(mid)
                    changed = True
            for rid, pairs in group_remove_individual.items():
                cfg = _reminder_cfg(rid)
                if cfg:
                    for cid, _mid in pairs:
                        cfg.setdefault("last_message_ids", {}).pop(str(cid), None)
                    changed = True
            for rid in sorted(due_ids):
                cfg = _reminder_cfg(rid)
                if not cfg or _reminder_is_completed(cfg):
                    continue
                if sent_for_rid.get(rid):
                    cfg["last_sent_at"] = now_dt.isoformat(timespec="seconds")
                _reminder_advance_after_send(now_dt, cfg)
                if not cfg.get("next_run_at") or _reminder_end_has_passed(cfg, now_local()):
                    _reminder_mark_completed(rid, cfg, "schedule_finished", delete_messages=True)
                else:
                    _reminder_touch(cfg)
                changed = True

            # Diagnostics: next group refresh is the earliest remaining member schedule.
            for cid, row in list(state_root.items()):
                try:
                    chat_id = int(cid)
                except Exception:
                    continue
                next_rows = []
                member_ids = []
                for rid, cfg in _v149_reminder_all_rows(include_completed=False):
                    if not _v149_reminder_active_now(cfg, now_local()) or chat_id not in _v149_reminder_chat_ids(cfg):
                        continue
                    if not _v149_reminder_chat_allowed(cfg, chat_id):
                        continue
                    member_ids.append(int(rid))
                    dt = _reminder_parse_dt(cfg.get("next_run_at"))
                    if dt is not None:
                        next_rows.append(dt)
                row["member_ids"] = sorted(member_ids)
                row["next_run_at"] = min(next_rows).isoformat(timespec="seconds") if next_rows else ""

        if changed:
            _reminder_save("v149_dynamic_merged_tick")
        if due_ids or group_updates:
            try:
                bot_journal("reminder_v149_batch", None, f"due={len(due_ids)} chats={len(chats_to_consider)} groups={sum(1 for v in group_updates.values() if v)}")
            except Exception:
                pass
    finally:
        _V149_REMINDER_BATCH_LOCK.release()


def _reminder_tick() -> None:
    global _REMINDER_FINANCE_BUSY_SINCE
    finance_busy = _reminder_finance_priority_busy()
    if finance_busy:
        if not _REMINDER_FINANCE_BUSY_SINCE:
            _REMINDER_FINANCE_BUSY_SINCE = _v149_time.monotonic()
        if _v149_time.monotonic() - _REMINDER_FINANCE_BUSY_SINCE < _REMINDER_FINANCE_PRIORITY_GRACE_SECONDS:
            return
    else:
        _REMINDER_FINANCE_BUSY_SINCE = 0.0
    if not REMINDER_TASK_POOL.submit_unique("reminder-v149-batch", _v149_reminder_batch_job, None):
        try: bot_journal("reminder_dispatch_coalesced", None, "v149 batch")
        except Exception: pass


def _reminder_group_send_job(target_chat_id: int, day_key: str | None = None, force: bool = False) -> None:
    _v149_reminder_batch_job(int(target_chat_id))


def build_reminder_list_text() -> str:
    rows = _reminder_items()
    enabled = sum(1 for _rid, cfg in rows if bool(cfg.get("enabled")))
    merged = reminder_merge_enabled()
    commands = reminder_show_complete_command()
    return (
        "⏰ НАПОМИНАЛКИ\n\n"
        f"Текущих: {len(rows)} · активных: {enabled}\n"
        f"Объединять напоминания: {'✅ включено' if merged else '❌ выключено'}\n"
        f"Показывать команду выполнения: {'✅ включено' if commands else '❌ выключено'}\n"
        f"Завершённых: {len(_reminder_completed_items())}\n\n"
        "При объединении каждый интервал сохраняется. Общее сообщение обновляется по ближайшей отправке — фактически по самому маленькому активному интервалу."
    )


def build_reminder_list_keyboard(day_key: str | None = None, page: int = 0):
    day_key = str(day_key or today_key())
    rows = _reminder_items()
    pages = max(1, (len(rows) + _REMINDER_LIST_PAGE_SIZE - 1) // _REMINDER_LIST_PAGE_SIZE)
    page = max(0, min(int(page or 0), pages - 1))
    kb = types.InlineKeyboardMarkup(row_width=1)
    kb.row(IB(f"🔗 Объединять: {'ВКЛ' if reminder_merge_enabled() else 'ВЫКЛ'}", callback_data=f"v149:rem:merge:{page}:{day_key}"))
    kb.row(IB(f"✅ Команда /vyapl: {'ВКЛ' if reminder_show_complete_command() else 'ВЫКЛ'}", callback_data=f"v149:rem:command:{page}:{day_key}"))
    kb.row(IB("+добавить⏰", callback_data=f"rem:add:{page}:{day_key}"))
    start = page * _REMINDER_LIST_PAGE_SIZE
    for idx, (rid, cfg) in enumerate(rows[start:start + _REMINDER_LIST_PAGE_SIZE], start=start + 1):
        kb.row(IB(_reminder_button_label(idx, cfg), callback_data=f"rem:open:{rid}:{page}:{day_key}"))
    if pages > 1:
        nav = []
        if page > 0: nav.append(IB("⬅️", callback_data=f"rem:list:{page-1}:{day_key}"))
        nav.append(IB(f"{page+1}/{pages}", callback_data="none"))
        if page + 1 < pages: nav.append(IB("➡️", callback_data=f"rem:list:{page+1}:{day_key}"))
        kb.row(*nav)
    kb.row(IB(f"✅ Завершённые ({len(_reminder_completed_items())})", callback_data="rem:completed:0:0"))
    kb.row(IB("📜 История выполнений", callback_data="v149:rem:history"))
    kb.row(IB("⬅️ Назад", callback_data=f"d:{day_key}:back_main"))
    return kb


def _v149_reminders_for_completion(chat_id: int) -> list[tuple[int, dict]]:
    rows = []
    current = now_local()
    for rid, cfg in _reminder_items(include_completed=False):
        if not _v149_reminder_active_now(cfg, current):
            continue
        if int(chat_id) not in _v149_reminder_chat_ids(cfg):
            continue
        if not _v149_reminder_chat_allowed(cfg, chat_id):
            continue
        rows.append((int(rid), cfg))
    rows.sort(key=lambda row: row[0])
    return rows


def _v149_complete_reminder(reminder_id: int, chat_id: int, actor_user_id: int, actor_label: str) -> tuple[bool, str]:
    with _V149_COMPLETION_LOCK, _REMINDER_CONFIG_LOCK:
        cfg = _reminder_cfg(int(reminder_id))
        if not cfg or int(chat_id) not in _v149_reminder_chat_ids(cfg) or not _v149_reminder_chat_allowed(cfg, chat_id):
            return False, "Напоминалка не найдена в этом чате."
        if _reminder_is_completed(cfg) or not cfg.get("enabled"):
            return False, "Эта напоминалка уже выполнена или выключена. Повторное выполнение не записано."
        tenant_id = _v149_reminder_cfg_tenant(cfg)
        event_id = _v149_hashlib.sha256(f"{tenant_id}:{int(reminder_id)}:{cfg.get('created_at')}:{cfg.get('updated_at')}".encode("utf-8")).hexdigest()[:24]
        history = _v149_completion_history(tenant_id)
        if any(str(row.get("event_id")) == event_id for row in history if isinstance(row, dict)):
            return False, "Это выполнение уже учтено."
        text = str(cfg.get("text") or "").strip()
        _reminder_mark_completed(int(reminder_id), cfg, "manual_vyapl", delete_messages=True)
        cfg["completed_by_user_id"] = int(actor_user_id or 0)
        cfg["completed_by_label"] = str(actor_label or "")[:120]
        cfg["completed_in_chat_id"] = int(chat_id)
        cfg["completion_event_id"] = event_id
        event = {
            "event_id": event_id,
            "at": str(cfg.get("completed_at") or _v149_now_iso()),
            "tenant_id": tenant_id,
            "reminder_id": int(reminder_id),
            "reminder_text": text[:500],
            "chat_id": int(chat_id),
            "chat_title": str(get_chat_display_name(int(chat_id)) or "")[:150],
            "user_id": int(actor_user_id or 0),
            "user": str(actor_label or "")[:120],
        }
        history.append(event)
        del history[:-500]
        _reminder_save("reminder_manual_vyapl")
    try:
        REMINDER_TASK_POOL.submit_unique("reminder-v149-batch", _v149_reminder_batch_job, int(chat_id))
    except Exception:
        pass
    try:
        bot_journal("reminder_manual_completed", int(chat_id), f"reminder_id={int(reminder_id)} user={int(actor_user_id or 0)} event={event_id}")
    except Exception:
        pass
    return True, f"✅ Выполнено: {text[:300]}"


def _v149_completion_keyboard(chat_id: int, rows: list[tuple[int, dict]]):
    kb = types.InlineKeyboardMarkup(row_width=1)
    for rid, cfg in rows[:30]:
        label = str(cfg.get("text") or f"Напоминалка {rid}").strip().replace("\n", " ")
        if len(label) > 48: label = label[:45] + "…"
        kb.row(IB(f"✅ {label}", callback_data=f"v149:rem:done:{int(rid)}:{int(chat_id)}"))
    return kb


def _v149_completion_history_text(tenant_id: str) -> str:
    rows = list(_v149_completion_history(tenant_id))[-30:]
    if not rows:
        return "📜 ИСТОРИЯ ВЫПОЛНЕНИЙ\n\nПока пусто."
    lines = ["📜 ИСТОРИЯ ВЫПОЛНЕНИЙ", ""]
    for row in reversed(rows):
        lines.append(
            f"✅ {row.get('at')} · №{row.get('reminder_id')}\n"
            f"{row.get('reminder_text')}\n"
            f"Кто: {row.get('user') or row.get('user_id')}\n"
            f"Чат: {row.get('chat_title') or row.get('chat_id')}"
        )
    return "\n\n".join(lines)[:3900]


@bot.message_handler(func=lambda m: bool(_v149_re.match(r"^/vyapl(?:_\d+)?(?:@[A-Za-z0-9_]+)?(?:\s|$)", str(getattr(m, "text", "") or ""), _v149_re.I)), content_types=["text"])
def cmd_v149_vyapl(msg):
    try: schedule_command_delete(msg)
    except Exception: pass
    chat_id = int(msg.chat.id); user_id = _v149_actor_id(msg); label = _v149_actor_label(msg)
    match = _v149_re.match(r"^/vyapl(?:_(\d+))?(?:@[A-Za-z0-9_]+)?", str(msg.text or ""), _v149_re.I)
    rid = int(match.group(1)) if match and match.group(1) else None
    rows = _v149_reminders_for_completion(chat_id)
    if rid is not None:
        ok, text = _v149_complete_reminder(rid, chat_id, user_id, label)
        bot.send_message(chat_id, text)
        return
    if not rows:
        send_and_auto_delete(chat_id, "Нет активных напоминалок для выполнения.", 10)
        return
    if len(rows) == 1:
        ok, text = _v149_complete_reminder(rows[0][0], chat_id, user_id, label)
        bot.send_message(chat_id, text)
        return
    bot.send_message(chat_id, "Какую напоминалку отметить выполненной?", reply_markup=_v149_completion_keyboard(chat_id, rows))


@bot.message_handler(commands=["vyapl_history"])
def cmd_v149_vyapl_history(msg):
    try: schedule_command_delete(msg)
    except Exception: pass
    chat_id = int(msg.chat.id); user_id = _v149_actor_id(msg)
    tid = str(tenant_id_for_chat(chat_id, create=False) or TENANT_PLATFORM_ID)
    if not tenant_can_manage(user_id, tid):
        send_and_auto_delete(chat_id, "❌ История доступна владельцу и администраторам пространства.", 10); return
    bot.send_message(chat_id, _v149_completion_history_text(tid))


def v149_extension_callback(call, data_str: str) -> bool:
    data_str = str(data_str or "")
    if not data_str.startswith("v149:"):
        return False
    chat_id = int(call.message.chat.id); user_id = _v149_actor_id(call)
    try:
        if data_str.startswith("v149:google:"):
            ok, tid = _v149_google_can_manage(chat_id, user_id, owner_only=True)
            if not ok:
                bot.answer_callback_query(call.id, "Только владелец пространства", show_alert=True)
                return True
            action = data_str.split(":", 2)[2]
            if action == "connect":
                _v149_google_wait(tid, "credentials", chat_id, user_id)
                bot.send_message(chat_id, "🔑 Пришлите JSON-ключ Google service_account как документ. Сообщение будет удалено после чтения.")
            elif action == "sheet":
                _v149_google_wait(tid, "sheet", chat_id, user_id)
                bot.send_message(chat_id, "📊 Пришлите ссылку или ID своей Google Таблицы.")
            elif action == "folder":
                _v149_google_wait(tid, "folder", chat_id, user_id)
                bot.send_message(chat_id, "📁 Пришлите ссылку или ID своей папки Google Drive.")
            elif action == "owner_email":
                _v149_google_wait(tid, "owner_email", chat_id, user_id)
                bot.send_message(chat_id, "👤 Пришлите email Google-аккаунта владельца пространства.")
            elif action in {"toggle_sheet", "toggle_drive"}:
                cfg = tenant_google_config(tid)
                settings = cfg.setdefault("export_settings", {})
                key = "sheet_enabled" if action == "toggle_sheet" else "drive_enabled"
                settings[key] = not bool(settings.get(key, True))
                cfg["updated_at"] = _v149_now_iso()
                tenant_google_history(tid, action, f"{key}={settings[key]}", ok=True)
                tenant_google_persist(tid, "tenant_google_settings")
                bot.send_message(chat_id, tenant_google_status_text(tid), reply_markup=tenant_google_keyboard(tid))
            elif action == "create_sheet":
                url = tenant_google_create_spreadsheet(tid, f"Финансы · {(tenant_get(tid) or {}).get('name') or tid}")
                bot.send_message(chat_id, f"✅ Таблица создана и закреплена за пространством:\n{url}", disable_web_page_preview=True)
            elif action == "test":
                _ok, text = tenant_google_test(tid)
                bot.send_message(chat_id, text)
            elif action == "history":
                bot.send_message(chat_id, _v149_google_history_text(tid, False))
            elif action == "errors":
                bot.send_message(chat_id, _v149_google_history_text(tid, True))
            elif action == "disconnect_confirm":
                kb = types.InlineKeyboardMarkup(row_width=2)
                kb.row(IB("🧹 Да, отключить", callback_data="v149:google:disconnect"), IB("Отмена", callback_data="v149:google:status"))
                bot.send_message(chat_id, "Отключить Google только у этого пространства? Таблицы и файлы в Google удалены не будут.", reply_markup=kb)
            elif action == "disconnect":
                cfg = tenant_google_config(tid)
                keep_history = list(cfg.get("history") or [])
                keep_errors = list(cfg.get("errors") or [])
                (tenant_get(tid) or {}).pop("google_v149", None)
                fresh = tenant_google_config(tid)
                fresh["history"] = keep_history
                fresh["errors"] = keep_errors
                tenant_google_history(tid, "account_disconnected", "Google отключён", ok=True)
                tenant_google_persist(tid, "tenant_google_disconnect")
                bot.send_message(chat_id, "✅ Google этого пространства отключён.")
            elif action == "status":
                bot.send_message(chat_id, tenant_google_status_text(tid), reply_markup=tenant_google_keyboard(tid))
            try: bot.answer_callback_query(call.id)
            except Exception: pass
            return True

        if data_str.startswith("v149:rem:"):
            parts = data_str.split(":")
            action = parts[2] if len(parts) > 2 else ""
            tid = str(tenant_id_for_chat(chat_id, create=False) or TENANT_PLATFORM_ID)
            if action in {"merge", "command"}:
                if not tenant_can_manage(user_id, tid):
                    bot.answer_callback_query(call.id, "Недостаточно прав", show_alert=True); return True
                settings = _v149_reminder_chat_settings(tid, chat_id)
                key = "merge_enabled" if action == "merge" else "show_complete_command"
                settings[key] = not bool(settings.get(key, False))
                settings["updated_at"] = _v149_now_iso()
                tenant_google_persist(tid, "reminder_chat_settings_v149")
                page = int(parts[3]) if len(parts) > 3 and str(parts[3]).isdigit() else 0
                day_key = parts[4] if len(parts) > 4 else today_key()
                with tenant_context(tid):
                    reminder_text = build_reminder_list_text()
                    reminder_keyboard = build_reminder_list_keyboard(day_key, page)
                safe_edit(bot, call, reminder_text, reply_markup=reminder_keyboard)
                if action == "merge":
                    REMINDER_TASK_POOL.submit_unique("reminder-v149-batch", _v149_reminder_batch_job, None)
                try: bot.answer_callback_query(call.id, "Настройка обновлена")
                except Exception: pass
                return True
            if action == "done":
                rid = int(parts[3]); target_chat_id = int(parts[4])
                if target_chat_id != chat_id:
                    bot.answer_callback_query(call.id, "Кнопка относится к другому чату", show_alert=True); return True
                ok, text = _v149_complete_reminder(rid, chat_id, user_id, _v149_actor_label(call))
                try: bot.answer_callback_query(call.id, text[:180], show_alert=not ok)
                except Exception: pass
                if ok:
                    try: bot.edit_message_text(text, chat_id=chat_id, message_id=call.message.message_id)
                    except Exception: bot.send_message(chat_id, text)
                return True
            if action == "history":
                if not tenant_can_manage(user_id, tid):
                    bot.answer_callback_query(call.id, "Недостаточно прав", show_alert=True); return True
                bot.send_message(chat_id, _v149_completion_history_text(tid))
                try: bot.answer_callback_query(call.id)
                except Exception: pass
                return True
    except Exception as exc:
        try:
            if data_str.startswith("v149:google:"):
                tid = str(tenant_id_for_chat(chat_id, create=False) or TENANT_PLATFORM_ID)
                tenant_google_error(tid, "callback", exc)
            bot.answer_callback_query(call.id, str(exc)[:180], show_alert=True)
        except Exception:
            pass
        return True
    return True

# v149_tenant_google_merged_reminders
