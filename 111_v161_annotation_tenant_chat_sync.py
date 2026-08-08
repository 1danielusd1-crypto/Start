# v161_annotation_tenant_chat_sync

# v161: fixes annotation command priority + exact first-circle chat lists.
# Loaded after v160 so it only patches the shared routing/UI layer.

import html as _v161_html
import re as _v161_re
import threading as _v161_threading
import copy as _v161_copy

VERSION = "bot_v161_annotation_tenant_chat_sync"

# ---------------------------------------------------------------------------
# 1) /iz-mr and /tz must be intercepted BEFORE generic finance/forward routing.
# ---------------------------------------------------------------------------
_V161_ANNOTATION_RE = _v161_re.compile(r"(?is)(?:^|\s)/(iz-mr|iz_mr|tz)\b")
_V161_MARKER_TOKEN_RE = _v161_re.compile(
    r"(?is)/(iz-mr|iz_mr|tz)\s+([СФПОOВB]\s*\d{1,6})(?:\s+(w[0-9a-f]{6,24}))?(?:\s+(.*))?$"
)
_V161_TOKEN_ONLY_RE = _v161_re.compile(
    r"(?is)/(iz-mr|iz_mr|tz)\s+(?:[СФПОOВB]\s*)?(w[0-9a-f]{6,24})(?:\s+(.*))?$"
)


def _v161_clean_inserted_text(text: str) -> str:
    s = str(text or "").strip()
    try:
        s = sanitize_telegram_inserted_text(s)
    except Exception:
        pass
    # Telegram switch_inline_query_current_chat commonly produces:
    #   @bot_username /tz Ф89 wabc... text
    # The legacy sanitizer only removed @username before amounts, not before '/'.
    s = _v161_re.sub(r"(?is)^\s*@[A-Za-z0-9_]{3,}\s+(?=/)", "", s)
    return _v161_re.sub(r"[ \t]+", " ", s).strip()


def _v161_parse_annotation(text: str):
    s = _v161_clean_inserted_text(text)
    m = _V161_MARKER_TOKEN_RE.search(s)
    if m:
        marker = _v161_re.sub(r"\s+", "", str(m.group(2) or "")).upper()
        return str(m.group(1) or "").lower(), marker, str(m.group(3) or ""), str(m.group(4) or "").strip()

    # Recovery path: if Telegram/client malformed the marker but preserved the unique
    # v160 token, the token itself is enough to recover the exact window marker.
    m = _V161_TOKEN_ONLY_RE.search(s)
    if not m:
        return None
    cmd = str(m.group(1) or "").lower()
    token = str(m.group(2) or "")
    body = str(m.group(3) or "").strip()
    marker = ""
    try:
        row = ((_v160_ann_root().get("windows") or {}).get(token) or {})
        marker = str(row.get("marker") or "").upper()
    except Exception:
        marker = ""
    if not marker:
        return None
    return cmd, marker, token, body


# Replace v160 parser globally. The already-registered v160 handler also starts using it.
_v160_parse_annotation = _v161_parse_annotation


# ---------------------------------------------------------------------------
# 2) Keep enough RAM-only render state to rename the exact visible window.
#    Persistent state still stores only compact annotation metadata.
# ---------------------------------------------------------------------------
_V161_RENDER_LOCK = _v161_threading.RLock()
_V161_RENDER_BY_TOKEN = {}
_V161_RENDER_MAX = 320
_V161_REAL_SEND = globals().get("_V160_BASE_SEND_MESSAGE")
_V161_REAL_EDIT = globals().get("_V160_BASE_EDIT_TEXT")


def _v161_markup_token(markup, chat_id=None, message_id=None) -> str:
    try:
        token = str(getattr(markup, "_v160_window_token", "") or "")
        if token:
            return token
    except Exception:
        pass
    try:
        return str(_V160_TOKEN_BY_MESSAGE.get((int(chat_id), int(message_id))) or "")
    except Exception:
        return ""


def _v161_annotation_name(token: str) -> str:
    if not token:
        return ""
    try:
        return str((((_v160_ann_root().get("windows") or {}).get(token) or {}).get("user_name") or "")).strip()
    except Exception:
        return ""


def _v161_name_safe(name: str, parse_mode=None) -> str:
    s = str(name or "").replace("\r", " ").replace("\n", " ").strip()[:240]
    mode = str(parse_mode or "").upper()
    if mode == "HTML":
        return _v161_html.escape(s, quote=False)
    if mode in {"MARKDOWNV2", "MARKDOWN_V2"}:
        return _v161_re.sub(r"([_\*\[\]\(\)~`>#+\-=|{}.!\\])", r"\\\1", s)
    return s


def _v161_apply_window_name(text: str, name: str, parse_mode=None) -> str:
    body = str(text or "")
    # Never stack an older visible alias when a window is renamed.
    rows = [row for row in body.splitlines() if not _v161_re.match(r"^\s*🏷\s*Имя окна\s*:", row)]
    if not name:
        return "\n".join(rows)
    marker_re = _v161_re.compile(r"^\s*(?:[СФП]\d{1,6}|[ОOВB]\d{1,4})(?:\s*[⏳⏰☑️])?\s*$", _v161_re.I)
    idx = -1
    for pos, row in enumerate(rows):
        if marker_re.match(row):
            idx = pos
    if idx < 0:
        return "\n".join(rows)
    rows.insert(idx, f"🏷 Имя окна: {_v161_name_safe(name, parse_mode)}")
    return "\n".join(rows)


def _v161_cache_render(token: str, chat_id, message_id, text: str, markup, parse_mode=None):
    if not token:
        return
    try:
        cid = int(chat_id or 0); mid = int(message_id or 0)
    except Exception:
        return
    try:
        markup_copy = _v161_copy.deepcopy(markup)
    except Exception:
        markup_copy = markup
    with _V161_RENDER_LOCK:
        _V161_RENDER_BY_TOKEN[token] = {
            "chat_id": cid,
            "message_id": mid,
            "text": str(text or ""),
            "reply_markup": markup_copy,
            "parse_mode": parse_mode,
        }
        if len(_V161_RENDER_BY_TOKEN) > _V161_RENDER_MAX:
            for old in list(_V161_RENDER_BY_TOKEN.keys())[: len(_V161_RENDER_BY_TOKEN) - _V161_RENDER_MAX]:
                _V161_RENDER_BY_TOKEN.pop(old, None)


def _v161_refresh_named_window(token: str) -> bool:
    name = _v161_annotation_name(token)
    if not token or not name or not callable(_V161_REAL_EDIT):
        return False
    with _V161_RENDER_LOCK:
        item = dict(_V161_RENDER_BY_TOKEN.get(token) or {})
    if not item or not int(item.get("chat_id") or 0) or not int(item.get("message_id") or 0):
        return False
    actual_text = _v161_apply_window_name(item.get("text") or "", name, item.get("parse_mode"))
    kwargs = {"reply_markup": item.get("reply_markup")}
    if item.get("parse_mode"):
        kwargs["parse_mode"] = item.get("parse_mode")
    try:
        _V161_REAL_EDIT(
            actual_text,
            chat_id=int(item["chat_id"]),
            message_id=int(item["message_id"]),
            **kwargs,
        )
        with _V161_RENDER_LOCK:
            if token in _V161_RENDER_BY_TOKEN:
                _V161_RENDER_BY_TOKEN[token]["text"] = actual_text
        return True
    except Exception as exc:
        try:
            # "message is not modified" is already the desired state.
            if "not modified" in str(exc).lower():
                return True
        except Exception:
            pass
        try: log_error(f"v161 rename visible window {token}: {exc}")
        except Exception: pass
        return False


if callable(_V161_REAL_SEND):
    def _v161_base_send(chat_id, text, *args, **kwargs):
        markup = kwargs.get("reply_markup")
        token = _v161_markup_token(markup)
        name = _v161_annotation_name(token)
        actual_text = _v161_apply_window_name(text, name, kwargs.get("parse_mode")) if name else str(text)
        result = _V161_REAL_SEND(chat_id, actual_text, *args, **kwargs)
        try:
            mid = int(getattr(result, "message_id", 0) or 0)
            _v161_cache_render(token, chat_id, mid, actual_text, markup, kwargs.get("parse_mode"))
        except Exception:
            pass
        return result
    _V160_BASE_SEND_MESSAGE = _v161_base_send


if callable(_V161_REAL_EDIT):
    def _v161_base_edit(text, chat_id=None, message_id=None, *args, **kwargs):
        markup = kwargs.get("reply_markup")
        token = _v161_markup_token(markup, chat_id, message_id)
        name = _v161_annotation_name(token)
        actual_text = _v161_apply_window_name(text, name, kwargs.get("parse_mode")) if name else str(text)
        result = _V161_REAL_EDIT(actual_text, chat_id=chat_id, message_id=message_id, *args, **kwargs)
        _v161_cache_render(token, chat_id, message_id, actual_text, markup, kwargs.get("parse_mode"))
        return result
    _V160_BASE_EDIT_TEXT = _v161_base_edit


# Wrap v160 saver so successful inputs are explicit in the downloadable journal,
# and /iz-mr immediately updates the visible exact window when render state exists.
_V161_PREV_HANDLE_ANNOTATION = globals().get("_v160_handle_annotation")


def _v161_handle_annotation(msg) -> bool:
    parsed = _v161_parse_annotation(getattr(msg, "text", ""))
    if not parsed:
        return False
    cmd, marker, token, body = parsed
    resolved_token = token
    try:
        resolved_token, _row = _v160_resolve_window(marker, token, int(msg.chat.id))
    except Exception:
        pass
    handled = bool(_V161_PREV_HANDLE_ANNOTATION(msg)) if callable(_V161_PREV_HANDLE_ANNOTATION) else False
    if not handled:
        return False
    if body and resolved_token:
        event = "window_annotation_saved" if cmd in {"iz-mr", "iz_mr"} else "window_tz_saved"
        try:
            bot_journal(event, int(msg.chat.id), f"marker={marker}; token={resolved_token}; chars={len(body)}")
        except Exception:
            pass
        if cmd in {"iz-mr", "iz_mr"}:
            visible = _v161_refresh_named_window(resolved_token)
            try:
                bot_journal("window_annotation_visible_name", int(msg.chat.id), f"marker={marker}; token={resolved_token}; visible={int(bool(visible))}")
            except Exception:
                pass
    return True


_v160_handle_annotation = _v161_handle_annotation


def _v161_priority_filter(msg) -> bool:
    try:
        return bool(getattr(msg, "text", None)) and bool(_V161_ANNOTATION_RE.search(str(msg.text or ""))) and bool(_v161_parse_annotation(msg.text))
    except Exception:
        return False


def v161_annotation_priority_message(msg):
    _v160_handle_annotation(msg)


# Register normally, then move this exact handler to index 0 so it wins over the
# older catch-all non-command text handler in 40_message_router.py.
try:
    bot.message_handler(func=_v161_priority_filter, content_types=["text"])(v161_annotation_priority_message)
    handlers = getattr(bot, "message_handlers", None)
    if isinstance(handlers, list):
        for idx in range(len(handlers) - 1, -1, -1):
            row = handlers[idx]
            fn = row.get("function") if isinstance(row, dict) else None
            if fn is v161_annotation_priority_message:
                handlers.insert(0, handlers.pop(idx))
                break
except Exception as exc:
    try: log_error(f"v161 annotation priority install: {exc}")
    except Exception: pass


# ---------------------------------------------------------------------------
# 3) One canonical first-circle list for Space / Finance / Forwarding.
#    No hidden bot_removed filtering: removed chats stay visible with the existing
#    '➖' marker and existing callbacks may still reject actions requiring access.
# ---------------------------------------------------------------------------

def _v161_context_chat_id() -> int:
    try:
        cid = current_state_chat_id()
        if cid is not None:
            return int(cid)
    except Exception:
        pass
    try: return int(OWNER_ID or 0)
    except Exception: return 0


def _v161_current_tenant_id() -> str:
    cid = _v161_context_chat_id()
    try:
        tid = str(tenant_id_for_chat(cid, create=False) or "") if cid else ""
    except Exception:
        tid = ""
    if not tid:
        try: tid = str(TENANT_PLATFORM_ID or "")
        except Exception: tid = ""
    return tid


def _v161_first_circle_chat_ids() -> list[int]:
    tid = _v161_current_tenant_id()
    ids = []
    row = {}
    try: row = tenant_get(tid) or {}
    except Exception: row = {}
    try: ids.extend(int(x) for x in (row.get("chat_ids") or []))
    except Exception: pass
    try:
        root_id = int(row.get("root_chat_id") or 0)
        if root_id:
            ids.append(root_id)
    except Exception:
        pass
    if not ids:
        cid = _v161_context_chat_id()
        if cid: ids.append(cid)
    return sorted(set(ids), key=lambda cid: str(get_chat_display_name(int(cid)) or cid).casefold())


def _v161_first_circle_root() -> int:
    tid = _v161_current_tenant_id()
    try:
        row = tenant_get(tid) or {}
        rid = int(row.get("root_chat_id") or 0)
        if rid:
            return rid
    except Exception:
        pass
    ids = _v161_first_circle_chat_ids()
    return int(ids[0]) if ids else 0


def collect_forward_menu_chats() -> dict:
    result = {}
    for cid in _v161_first_circle_chat_ids():
        try:
            store = get_chat_store(int(cid))
            info = store.get("info") or {}
        except Exception:
            info = {}
        result[str(int(cid))] = {
            "title": info.get("title") or get_chat_display_name(int(cid)) or f"Чат {int(cid)}",
            "username": info.get("username"),
            "type": info.get("type"),
        }
    return result


def collect_all_known_chat_ids(include_owner: bool = True) -> list[int]:
    ids = list(_v161_first_circle_chat_ids())
    if not include_owner:
        root = _v161_first_circle_root()
        ids = [cid for cid in ids if int(cid) != int(root)]
    return ids


def _collect_forward_picker_items(include_owner: bool = True, include_removed: bool = False):
    root = _v161_first_circle_root()
    items = []
    owner_item = None
    for cid in _v161_first_circle_chat_ids():
        title = get_chat_display_name(int(cid)) or f"Чат {int(cid)}"
        if root and int(cid) == int(root):
            owner_item = (int(cid), title)
        else:
            # v161 intentionally keeps bot_removed chats visible in the first-circle list.
            items.append((int(cid), title))
    items.sort(key=lambda row: str(row[1]).casefold())
    if include_owner and root and owner_item is None:
        owner_item = (int(root), get_chat_display_name(int(root)))
    if not include_owner:
        owner_item = None
    return items, owner_item


def _visible_forward_items_for_new_menu(include_owner: bool = True):
    items, owner_item = _collect_forward_picker_items(include_owner=include_owner, include_removed=True)
    rows = list(items)
    if owner_item:
        rows.append(owner_item)
    return rows


def _v161_finance_chat_buttons(day_key: str, callback_prefix: str, icon_fn):
    kb = types.InlineKeyboardMarkup(row_width=2)
    buttons = []
    for cid in _v161_first_circle_chat_ids():
        title = get_chat_display_name(int(cid)) or f"Чат {int(cid)}"
        buttons.append(IB(
            f"{icon_fn(int(cid))} {chat_button_title(int(cid), title)}",
            callback_data=f"d:{day_key}:{callback_prefix}{int(cid)}",
        ))
    add_buttons_in_rows(kb, buttons, 2)
    return kb


def build_finance_toggle_chat_menu(day_key: str):
    kb = _v161_finance_chat_buttons(day_key, "fw_finmode_pick_", finance_mode_compact_icon)
    kb.row(IB("ℹ️ Описание чатов", callback_data="chat_desc_menu:finmode"))
    kb.row(IB("🔙 Назад", callback_data=f"d:{day_key}:back_main"))
    try: bot_journal("v161_first_circle_fin_menu", _v161_context_chat_id(), f"count={len(_v161_first_circle_chat_ids())}")
    except Exception: pass
    return kb


def build_quick_balance_chat_menu(day_key: str):
    def _icon(cid):
        mode = finance_window_mode(cid) if is_finance_mode(cid) else "off"
        return "✅🥇" if mode == "first" else ("✅3️⃣" if mode == "open" else ("✅🔟" if mode == "normal" else "❌"))
    kb = _v161_finance_chat_buttons(day_key, "qb_cfg_", _icon)
    kb.row(IB("🔙 Назад", callback_data=f"d:{day_key}:back_main"))
    return kb


def build_hidden_finance_chat_menu(day_key: str):
    def _icon(cid): return "🙈" if is_hidden_finance_mode(cid) else "❌"
    kb = _v161_finance_chat_buttons(day_key, "hf_pick_", _icon)
    kb.row(IB("🔙 Назад", callback_data=f"d:{day_key}:back_main"))
    return kb


try:
    bot_journal(
        "v161_annotation_tenant_chat_sync_installed",
        int(OWNER_ID or 0),
        "annotation_priority=first; visible_window_names=on; space_fin_forward=tenant_chat_ids; removed_chats=visible",
    )
except Exception:
    pass


# ---------------------------------------------------------------------------
# 4) Full-state restore must accept snapshots created by v161 itself.
# ---------------------------------------------------------------------------
def _v153_validate_restore_gz(gz_path: str) -> tuple[dict, str]:
    folder = _v159_tempfile.mkdtemp(prefix="v161_restore_validate_")
    raw = _v159_os.path.join(folder, "restore.sqlite3")
    try:
        with _v159_gzip.open(gz_path, "rb") as fin, open(raw, "wb") as fout:
            _v159_shutil.copyfileobj(fin, fout, 1024 * 1024)
        conn = _v159_sqlite3.connect(raw)
        try:
            integrity = str(conn.execute("PRAGMA integrity_check").fetchone()[0])
            if integrity.lower() != "ok":
                raise RuntimeError(f"SQLite integrity_check: {integrity}")
            row = conn.execute("SELECT v FROM meta WHERE kind='v153_export' AND k='manifest'").fetchone()
            if not row:
                raise RuntimeError("manifest v153 not found")
            manifest = _v159_json.loads(row[0])
        finally:
            conn.close()
        if str(manifest.get("kind")) != "telegram_bot_full_state_v153":
            raise RuntimeError("unknown export kind")
        if int(manifest.get("schema_version") or 0) != int(V153_EXPORT_SCHEMA):
            raise RuntimeError("unsupported export schema")
        export_version = str(manifest.get("bot_version") or "")
        if not export_version.startswith((
            "bot_v153_", "bot_v154_", "bot_v155_", "bot_v156_", "bot_v157_", "bot_v158_", "bot_v159_", "bot_v160_", "bot_v161_",
        )):
            raise RuntimeError(f"unsupported bot version: {export_version or 'missing'}")
        checksum = _v153_db_logical_checksum(raw)
        if checksum != str(manifest.get("checksum") or ""):
            raise RuntimeError("checksum mismatch")
        return manifest, raw
    except Exception:
        _v159_shutil.rmtree(folder, ignore_errors=True)
        raise

# v161_annotation_tenant_chat_sync
