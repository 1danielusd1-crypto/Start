# v178_global_performance_final

# ─────────────────────────────────────────────────────────────
# v148: независимые пространства (tenant isolation)
# ─────────────────────────────────────────────────────────────
# Код бота общий, но каждый Telegram-чат принадлежит ровно одному пространству.
# Пространство изолирует: список чатов, пользователей/ролей, owner-scoped настройки,
# напоминалки, меню пересылки и допустимые связи. Платформенный владелец видит реестр
# всех пространств, но бизнес-связи между пространствами блокируются.

TENANT_SCHEMA_VERSION = 1
TENANT_PLATFORM_ID = "platform"
TENANT_ROLE_ORDER = ("tenant_owner", "tenant_admin", "operator", "viewer")
TENANT_ROLE_LABELS = {
    "platform_owner": "Владелец платформы",
    "tenant_owner": "Владелец пространства",
    "tenant_admin": "Администратор",
    "operator": "Оператор",
    "viewer": "Только просмотр",
    "standard": "Участник чата",
}
_TENANT_LOCK = threading.RLock()
_TENANT_CONTEXT = threading.local()
_TENANT_BOT_USERNAME = ""


def _tenant_now() -> str:
    return now_local().isoformat(timespec="seconds")


def _tenant_platform_owner_user_id() -> int:
    try:
        return int(OWNER_ID or 0)
    except Exception:
        return 0


def tenant_current_actor_user_id() -> int:
    try:
        ctx = _current_telegram_update_context()
        return int(ctx.get("user_id") or 0)
    except Exception:
        return 0


def tenant_is_platform_owner_user(user_id: int | None) -> bool:
    try:
        return bool(int(user_id or 0) and int(user_id or 0) == _tenant_platform_owner_user_id())
    except Exception:
        return False


def tenant_is_platform_owner_context(chat_id: int | None = None) -> bool:
    uid = tenant_current_actor_user_id()
    if uid:
        return tenant_is_platform_owner_user(uid)
    try:
        cid = int(chat_id if chat_id is not None else (current_state_chat_id() or 0))
    except Exception:
        cid = 0
    return bool(cid and cid == _tenant_platform_owner_user_id())


@contextmanager
def tenant_context(tenant_id: str | None):
    prev = getattr(_TENANT_CONTEXT, "tenant_id", None)
    try:
        _TENANT_CONTEXT.tenant_id = str(tenant_id or "") or None
        yield
    finally:
        _TENANT_CONTEXT.tenant_id = prev


def _tenants_root() -> dict:
    gs = data.setdefault("_global_settings", {})
    with _TENANT_LOCK:
        root = gs.get("tenants_v148")
        if not isinstance(root, dict):
            root = {}
            gs["tenants_v148"] = root
        root.setdefault("schema_version", TENANT_SCHEMA_VERSION)
        root.setdefault("tenants", {})
        root.setdefault("chat_to_tenant", {})
        root.setdefault("invite_tokens", {})
        root.setdefault("legacy_migrated", False)
        root.setdefault("created_at", _tenant_now())
        return root


def _tenant_default_name(chat_id: int | None = None) -> str:
    if chat_id:
        try:
            return str(get_chat_display_name(int(chat_id)) or f"Чат {int(chat_id)}")[:80]
        except Exception:
            pass
    return "Новое пространство"


def _tenant_normalize(tenant_id: str, row: dict | None = None) -> dict:
    row = row if isinstance(row, dict) else {}
    row["id"] = str(tenant_id)
    row.setdefault("name", "Пространство")
    row.setdefault("owner_user_id", 0)
    row.setdefault("root_chat_id", 0)
    row.setdefault("chat_ids", [])
    row.setdefault("users", {})
    row.setdefault("settings", {})
    row.setdefault("status", "active")
    row.setdefault("created_at", _tenant_now())
    row.setdefault("updated_at", _tenant_now())
    clean_chats = []
    for raw in row.get("chat_ids") or []:
        try:
            cid = int(raw)
        except Exception:
            continue
        if cid not in clean_chats:
            clean_chats.append(cid)
    row["chat_ids"] = clean_chats
    users = row.get("users") if isinstance(row.get("users"), dict) else {}
    owner_uid = int(row.get("owner_user_id") or 0)
    if owner_uid:
        users.setdefault(str(owner_uid), {})["role"] = "tenant_owner"
    for uid, item in list(users.items()):
        try:
            int(uid)
        except Exception:
            users.pop(uid, None)
            continue
        if not isinstance(item, dict):
            item = {"role": "viewer"}
            users[str(uid)] = item
        role = str(item.get("role") or "viewer")
        if role not in TENANT_ROLE_ORDER:
            role = "viewer"
        item["role"] = role
        item.setdefault("joined_at", _tenant_now())
        item.setdefault("updated_at", _tenant_now())
    row["users"] = users
    return row


def tenant_get(tenant_id: str | None) -> dict | None:
    if not tenant_id:
        return None
    root = _tenants_root()
    row = (root.get("tenants") or {}).get(str(tenant_id))
    if not isinstance(row, dict):
        return None
    row = _tenant_normalize(str(tenant_id), row)
    root["tenants"][str(tenant_id)] = row
    return row


def tenant_all() -> list[dict]:
    root = _tenants_root()
    rows = []
    for tid, row in list((root.get("tenants") or {}).items()):
        if isinstance(row, dict):
            rows.append(_tenant_normalize(str(tid), row))
    rows.sort(key=lambda r: (0 if str(r.get("id")) == TENANT_PLATFORM_ID else 1, str(r.get("name") or "").casefold()))
    return rows


def _v177_legacy_0248_tenant_id_for_chat(chat_id: int | None, create: bool = False, actor_user_id: int | None = None) -> str:
    explicit = getattr(_TENANT_CONTEXT, "tenant_id", None)
    if explicit:
        return str(explicit)
    try:
        cid = int(chat_id or 0)
    except Exception:
        cid = 0
    root = _tenants_root()
    if cid:
        tid = str((root.get("chat_to_tenant") or {}).get(str(cid)) or "")
        if tid and tenant_get(tid):
            return tid
    if not create:
        return TENANT_PLATFORM_ID
    uid = int(actor_user_id or tenant_current_actor_user_id() or 0)
    if tenant_is_platform_owner_user(uid):
        tenant_bind_chat(cid, TENANT_PLATFORM_ID, changed_by=uid, force=True)
        return TENANT_PLATFORM_ID
    owner_uid = uid if tenant_user_is_chat_admin(cid, uid) else 0
    tid = tenant_create(_tenant_default_name(cid), owner_uid, cid, created_by=uid, deterministic_chat_id=cid)
    return tid
try: _v177_legacy_0248_tenant_id_for_chat.__name__ = 'tenant_id_for_chat'
except Exception: pass
tenant_id_for_chat = _v177_legacy_0248_tenant_id_for_chat


def tenant_current_id(chat_id: int | None = None) -> str:
    explicit = getattr(_TENANT_CONTEXT, "tenant_id", None)
    if explicit:
        return str(explicit)
    if chat_id is None:
        chat_id = current_state_chat_id()
    return tenant_id_for_chat(chat_id, create=False)


def tenant_create(name: str, owner_user_id: int, root_chat_id: int, created_by: int = 0, deterministic_chat_id: int | None = None) -> str:
    root = _tenants_root()
    with _TENANT_LOCK:
        if deterministic_chat_id is not None:
            seed = hashlib.sha256(f"chat:{int(deterministic_chat_id)}".encode("utf-8")).hexdigest()[:12]
            tid = f"chat_{seed}"
        else:
            tid = f"t_{secrets.token_hex(6)}"
        while tid in root["tenants"]:
            if deterministic_chat_id is not None:
                break
            tid = f"t_{secrets.token_hex(6)}"
        row = _tenant_normalize(tid, {
            "name": str(name or _tenant_default_name(root_chat_id))[:80],
            "owner_user_id": int(owner_user_id or 0),
            "root_chat_id": int(root_chat_id or 0),
            "chat_ids": [int(root_chat_id)] if root_chat_id else [],
            "users": {},
            "settings": {},
            "created_by": int(created_by or 0),
            "created_at": _tenant_now(),
            "updated_at": _tenant_now(),
        })
        root["tenants"][tid] = row
        if root_chat_id:
            tenant_bind_chat(int(root_chat_id), tid, changed_by=int(created_by or owner_user_id or 0), force=True)
        if owner_user_id:
            tenant_set_user_role(tid, int(owner_user_id), "tenant_owner", changed_by=created_by, save=False)
        return tid


def tenant_bind_chat(chat_id: int, tenant_id: str, changed_by: int = 0, force: bool = False) -> bool:
    cid = int(chat_id)
    row = tenant_get(tenant_id)
    if not row:
        return False
    root = _tenants_root()
    old_tid = str((root.get("chat_to_tenant") or {}).get(str(cid)) or "")
    if old_tid and old_tid != str(tenant_id) and not force:
        return False
    if old_tid and old_tid != str(tenant_id):
        old = tenant_get(old_tid)
        if old:
            old["chat_ids"] = [int(x) for x in old.get("chat_ids") or [] if int(x) != cid]
            old["updated_at"] = _tenant_now()
    root["chat_to_tenant"][str(cid)] = str(tenant_id)
    if cid not in row["chat_ids"]:
        row["chat_ids"].append(cid)
    row["updated_at"] = _tenant_now()
    try:
        store = get_chat_store(cid)
        store.setdefault("settings", {})["tenant_id"] = str(tenant_id)
        store["settings"]["owner_scope_id"] = int(row.get("root_chat_id") or cid)
    except Exception:
        pass
    try:
        bot_journal("tenant_chat_bound", cid, f"tenant={tenant_id} old={old_tid or '-'} by={int(changed_by or 0)}")
    except Exception:
        pass
    if old_tid and old_tid != str(tenant_id):
        try:
            enforce = globals().get("tenant_v148_enforce_forward_isolation")
            if callable(enforce):
                enforce()
        except Exception as exc:
            log_error(f"tenant rebind forward cleanup {cid}: {exc}")
    return True


def tenant_unbind_chat(chat_id: int, changed_by: int = 0) -> bool:
    cid = int(chat_id)
    root = _tenants_root()
    tid = str((root.get("chat_to_tenant") or {}).get(str(cid)) or "")
    row = tenant_get(tid)
    if not row or int(row.get("root_chat_id") or 0) == cid:
        return False
    root["chat_to_tenant"].pop(str(cid), None)
    row["chat_ids"] = [int(x) for x in row.get("chat_ids") or [] if int(x) != cid]
    row["updated_at"] = _tenant_now()
    new_tid = tenant_create(_tenant_default_name(cid), 0, cid, created_by=changed_by, deterministic_chat_id=cid)
    try:
        bot_journal("tenant_chat_unbound", cid, f"old={tid} new={new_tid} by={changed_by}")
    except Exception:
        pass
    return True


def tenant_role_for_user(user_id: int | None, tenant_id: str | None = None, chat_id: int | None = None) -> str:
    try:
        uid = int(user_id or 0)
    except Exception:
        uid = 0
    if tenant_is_platform_owner_user(uid):
        return "platform_owner"
    tid = str(tenant_id or tenant_current_id(chat_id))
    row = tenant_get(tid)
    if not row or not uid:
        return "standard"
    item = (row.get("users") or {}).get(str(uid)) or {}
    role = str(item.get("role") or "standard")
    return role if role in TENANT_ROLE_ORDER else "standard"


def tenant_user_spaces(user_id: int) -> list[dict]:
    uid = int(user_id)
    if tenant_is_platform_owner_user(uid):
        return tenant_all()
    out = []
    for row in tenant_all():
        if str(uid) in (row.get("users") or {}):
            out.append(row)
    return out


def tenant_set_user_role(tenant_id: str, user_id: int, role: str, changed_by: int = 0, save: bool = True) -> bool:
    row = tenant_get(tenant_id)
    if not row:
        return False
    uid = int(user_id)
    role = str(role or "viewer")
    if role not in TENANT_ROLE_ORDER:
        return False
    if role == "tenant_owner":
        if str(tenant_id) == TENANT_PLATFORM_ID and uid != _tenant_platform_owner_user_id():
            return False
        old_owner = int(row.get("owner_user_id") or 0)
        if old_owner and old_owner != uid:
            row.setdefault("users", {}).setdefault(str(old_owner), {})["role"] = "tenant_admin"
        row["owner_user_id"] = uid
    item = row.setdefault("users", {}).setdefault(str(uid), {})
    item["role"] = role
    item.setdefault("joined_at", _tenant_now())
    item["updated_at"] = _tenant_now()
    item["changed_by"] = int(changed_by or 0)
    row["updated_at"] = _tenant_now()
    if save:
        save_data(data, full=True)
        try:
            schedule_delta_backup(int(row.get("root_chat_id") or OWNER_ID or 0), delay=0.5, reason="tenant_user_role")
        except Exception:
            pass
    return True


def _v177_legacy_0249_tenant_can_manage(user_id: int | None, tenant_id: str | None = None, chat_id: int | None = None, owner_only: bool = False) -> bool:
    role = tenant_role_for_user(user_id, tenant_id, chat_id)
    if role == "platform_owner":
        return True
    if owner_only:
        return role == "tenant_owner"
    return role in {"tenant_owner", "tenant_admin"}
try: _v177_legacy_0249_tenant_can_manage.__name__ = 'tenant_can_manage'
except Exception: pass
tenant_can_manage = _v177_legacy_0249_tenant_can_manage


def tenant_user_is_chat_admin(chat_id: int, user_id: int) -> bool:
    try:
        cid = int(chat_id); uid = int(user_id)
    except Exception:
        return False
    if not cid or not uid:
        return False
    if tenant_is_platform_owner_user(uid):
        return True
    try:
        store = get_chat_store(cid)
        typ = str((store.get("info") or {}).get("type") or "")
        if typ == "private" or cid > 0:
            return cid == uid
    except Exception:
        if cid > 0:
            return cid == uid
    try:
        member = bot.get_chat_member(cid, uid)
        return str(getattr(member, "status", "") or "") in {"creator", "administrator"}
    except Exception:
        return False


def tenant_chat_ids(tenant_id: str | None = None) -> list[int]:
    row = tenant_get(tenant_id or tenant_current_id())
    if not row:
        return []
    return sorted({int(x) for x in row.get("chat_ids") or []}, key=lambda x: get_chat_display_name(x).casefold())


def _v177_legacy_0250_tenant_same_space(chat_a: int, chat_b: int) -> bool:
    """True only for two explicitly bound chats in the same space.

    Unknown chat IDs must never inherit the platform tenant implicitly: an old/stale
    forwarding rule to an unseen chat is blocked until that chat is registered.
    """
    try:
        a = int(chat_a); b = int(chat_b)
    except Exception:
        return False
    if a == b:
        return True
    mapping = _tenants_root().get("chat_to_tenant") or {}
    ta = str(mapping.get(str(a)) or "")
    tb = str(mapping.get(str(b)) or "")
    return bool(ta and tb and ta == tb and tenant_get(ta))
try: _v177_legacy_0250_tenant_same_space.__name__ = 'tenant_same_space'
except Exception: pass
tenant_same_space = _v177_legacy_0250_tenant_same_space


def _v177_legacy_0251_tenant_note_chat_seen(msg) -> None:
    try:
        chat_id = int(msg.chat.id)
        user_id = int(getattr(getattr(msg, "from_user", None), "id", 0) or 0)
    except Exception:
        return
    tid = tenant_id_for_chat(chat_id, create=True, actor_user_id=user_id)
    row = tenant_get(tid)
    if not row:
        return
    # An unclaimed space can be claimed automatically by the first verified Telegram admin.
    if not int(row.get("owner_user_id") or 0) and user_id and tenant_user_is_chat_admin(chat_id, user_id):
        tenant_set_user_role(tid, user_id, "tenant_owner", changed_by=user_id, save=False)
    try:
        store = get_chat_store(chat_id)
        store.setdefault("settings", {})["tenant_id"] = tid
        store["settings"]["owner_scope_id"] = int(row.get("root_chat_id") or chat_id)
    except Exception:
        pass
try: _v177_legacy_0251_tenant_note_chat_seen.__name__ = 'tenant_note_chat_seen'
except Exception: pass
tenant_note_chat_seen = _v177_legacy_0251_tenant_note_chat_seen


# Preserve v147 chat-info behavior, then attach/refresh the tenant card.
_V148_ORIG_UPDATE_CHAT_INFO = globals().get("update_chat_info_from_message")
def _v177_legacy_0236_update_chat_info_from_message(msg):
    result = None
    if callable(_V148_ORIG_UPDATE_CHAT_INFO):
        result = _V148_ORIG_UPDATE_CHAT_INFO(msg)
    try:
        tenant_note_chat_seen(msg)
    except Exception as exc:
        log_error(f"tenant_note_chat_seen: {exc}")
    return result
try: _v177_legacy_0236_update_chat_info_from_message.__name__ = 'update_chat_info_from_message'
except Exception: pass
update_chat_info_from_message = _v177_legacy_0236_update_chat_info_from_message


# ─────────────────────────────────────────────────────────────
# Owner scope compatibility layer
# ─────────────────────────────────────────────────────────────
def _v168_owner_access_ids() -> set[int]:
    gs = data.setdefault("_global_settings", {})
    raw = gs.get("owner_access_chat_ids_v168")
    if not isinstance(raw, list):
        # v148 legacy values represented user-owner identities, not the chat-access switches used by Ф2.
        raw = []
        gs["owner_access_chat_ids_v168"] = raw
    out = set()
    for value in raw or []:
        try: out.add(int(value))
        except Exception: pass
    return out


def get_additional_owner_ids() -> set[int]:
    # v168: these are chats in which the platform owner explicitly enabled owner/testing access.
    return _v168_owner_access_ids()


def set_additional_owner(chat_id: int, enabled: bool):
    cid = int(chat_id)
    owners = _v168_owner_access_ids()
    if enabled: owners.add(cid)
    else: owners.discard(cid)
    gs = data.setdefault("_global_settings", {})
    gs["owner_access_chat_ids_v168"] = sorted(owners)
    # Do not mirror chat IDs into legacy additional_owner_ids: v148 interprets that old field as user-owner IDs.
    def _persist_owner_access():
        try: save_data(data, root_only=True)
        except Exception as exc:
            try: log_error(f"v168 owner access persist {cid}: {exc}")
            except Exception: pass
    try:
        scheduler = globals().get("V166_CONFIG_IO_SCHEDULER")
        if scheduler is not None:
            scheduler.cancel("owner-access-root-v168")
            scheduler.schedule("owner-access-root-v168", 0.10, _persist_owner_access)
        else:
            _persist_owner_access()
    except Exception:
        _persist_owner_access()
    try: schedule_config_backup_for_chats(cid, delay=0.25)
    except Exception: pass


def is_primary_owner(chat_id: int) -> bool:
    try: cid = int(chat_id)
    except Exception: return False
    owner_uid = _tenant_platform_owner_user_id()
    if cid == owner_uid:
        return True
    actor = tenant_current_actor_user_id()
    return bool(actor == owner_uid and cid in _v168_owner_access_ids())


def is_owner_chat(chat_id: int) -> bool:
    try: cid = int(chat_id)
    except Exception: return False
    owner_uid = _tenant_platform_owner_user_id()
    actor = tenant_current_actor_user_id()
    if cid == owner_uid:
        return True
    # In a selected external chat only the real platform owner gains this extra owner/test access.
    if actor == owner_uid:
        return cid in _v168_owner_access_ids()
    if actor:
        return tenant_can_manage(actor, chat_id=cid)
    row = tenant_get(tenant_id_for_chat(cid, create=False))
    return bool(row and int(row.get("root_chat_id") or 0) == cid)


def owner_scope_id(chat_id: int | None = None) -> int:
    try:
        cid = int(chat_id if chat_id is not None else (current_state_chat_id() or OWNER_ID or 0))
    except Exception:
        cid = _tenant_platform_owner_user_id()
    row = tenant_get(tenant_id_for_chat(cid, create=False))
    return int((row or {}).get("root_chat_id") or cid or _tenant_platform_owner_user_id())


def owner_scoped_settings(chat_id: int | None = None) -> dict:
    tid = tenant_current_id(chat_id)
    row = tenant_get(tid)
    if not row:
        return data.setdefault("_global_settings", {})
    return row.setdefault("settings", {})


def bind_chat_to_owner_scope(chat_id: int, scope_id: int):
    tid = tenant_id_for_chat(int(scope_id), create=True, actor_user_id=tenant_current_actor_user_id())
    ok = tenant_bind_chat(int(chat_id), tid, changed_by=tenant_current_actor_user_id(), force=True)
    if ok:
        save_data(data, chat_ids=[int(chat_id), int(scope_id)], root_only=False)
    return ok


# ─────────────────────────────────────────────────────────────
# Strict chat-list and forwarding isolation
# ─────────────────────────────────────────────────────────────
def collect_forward_menu_chats() -> dict:
    tid = tenant_current_id()
    result = {}
    for cid in tenant_chat_ids(tid):
        try:
            store = get_chat_store(cid)
            info = store.get("info") or {}
            result[str(cid)] = {
                "title": info.get("title") or get_chat_display_name(cid) or f"Чат {cid}",
                "username": info.get("username"),
                "type": info.get("type"),
            }
        except Exception:
            continue
    return result


def collect_all_known_chat_ids(include_owner: bool = True) -> list[int]:
    ids = tenant_chat_ids(tenant_current_id())
    if not include_owner:
        root_id = owner_scope_id(current_state_chat_id())
        ids = [cid for cid in ids if int(cid) != int(root_id)]
    return sorted(set(ids), key=lambda cid: get_chat_display_name(cid).casefold())


_V148_ORIG_RESOLVE_FORWARD_TARGETS = globals().get("resolve_forward_targets")
def resolve_forward_targets(source_chat_id: int):
    rows = _V148_ORIG_RESOLVE_FORWARD_TARGETS(int(source_chat_id)) if callable(_V148_ORIG_RESOLVE_FORWARD_TARGETS) else []
    out = []
    for dst, mode, fin in rows or []:
        if tenant_same_space(int(source_chat_id), int(dst)):
            out.append((int(dst), mode, bool(fin)))
        else:
            try:
                bot_journal("tenant_cross_forward_blocked", int(source_chat_id), f"dst={int(dst)}")
            except Exception:
                pass
    return out


_V148_ORIG_ADD_FORWARD_LINK = globals().get("add_forward_link")
def _v177_legacy_0142_add_forward_link(src_chat_id: int, dst_chat_id: int, mode: str):
    if not tenant_same_space(int(src_chat_id), int(dst_chat_id)):
        raise PermissionError("Нельзя связать пересылкой чаты из разных пространств")
    return _V148_ORIG_ADD_FORWARD_LINK(int(src_chat_id), int(dst_chat_id), mode)
try: _v177_legacy_0142_add_forward_link.__name__ = 'add_forward_link'
except Exception: pass
add_forward_link = _v177_legacy_0142_add_forward_link


def clear_forward_all():
    """v148: очищает связи только текущего пространства, а не чужие контуры."""
    allowed = {str(x) for x in tenant_chat_ids(tenant_current_id())}
    fr = data.setdefault("forward_rules", {})
    ff = data.setdefault("forward_finance", {})
    for src in list(fr.keys()):
        if str(src) in allowed:
            fr.pop(src, None)
            ff.pop(src, None)
            continue
        for dst in list((fr.get(src) or {}).keys()):
            if str(dst) in allowed:
                (fr.get(src) or {}).pop(dst, None)
                (ff.get(src) or {}).pop(dst, None)
    data["forward_pair_order"] = [
        key for key in (data.get("forward_pair_order") or [])
        if not any(part in allowed for part in str(key).split(":", 1))
    ]
    persist_forward_rules_to_owner()
    save_data(data, full=True)


_V148_ORIG_COLLECT_FORWARD_PAIRS = globals().get("collect_forward_pairs_for_menu")
def _v177_legacy_0190_collect_forward_pairs_for_menu() -> list[tuple[int, int]]:
    rows = _V148_ORIG_COLLECT_FORWARD_PAIRS() if callable(_V148_ORIG_COLLECT_FORWARD_PAIRS) else []
    tid = tenant_current_id()
    allowed = set(tenant_chat_ids(tid))
    return [(int(a), int(b)) for a, b in rows if int(a) in allowed and int(b) in allowed]
try: _v177_legacy_0190_collect_forward_pairs_for_menu.__name__ = 'collect_forward_pairs_for_menu'
except Exception: pass
collect_forward_pairs_for_menu = _v177_legacy_0190_collect_forward_pairs_for_menu


_V148_ORIG_GET_CONNECTED = globals().get("get_connected_chat_ids")
def get_connected_chat_ids(chat_id: int):
    rows = _V148_ORIG_GET_CONNECTED(int(chat_id)) if callable(_V148_ORIG_GET_CONNECTED) else []
    return [int(cid) for cid in rows if tenant_same_space(int(chat_id), int(cid))]


# ─────────────────────────────────────────────────────────────
# Tenant-scoped settings which were globally shared in v147
# ─────────────────────────────────────────────────────────────
def _tenant_settings_for_context(chat_id: int | None = None) -> dict:
    return owner_scoped_settings(chat_id)


def _v177_legacy_0147_forward_copy_edit_mode(chat_id: int | None = None) -> str:
    mode = str(_tenant_settings_for_context(chat_id).get("forward_copy_edit_mode") or "normal").lower()
    return mode if mode in FORWARD_COPY_EDIT_MODES and version_mode_feature("forward_copy_edit") else "normal"
try: _v177_legacy_0147_forward_copy_edit_mode.__name__ = 'forward_copy_edit_mode'
except Exception: pass
forward_copy_edit_mode = _v177_legacy_0147_forward_copy_edit_mode


def _v177_legacy_0149_set_forward_copy_edit_mode(chat_id: int, mode: str):
    mode = str(mode or "normal").lower()
    if mode not in FORWARD_COPY_EDIT_MODES:
        mode = "normal"
    _tenant_settings_for_context(chat_id)["forward_copy_edit_mode"] = mode
    save_data(data, root_only=True)
    return mode
try: _v177_legacy_0149_set_forward_copy_edit_mode.__name__ = 'set_forward_copy_edit_mode'
except Exception: pass
set_forward_copy_edit_mode = _v177_legacy_0149_set_forward_copy_edit_mode


def reminder_ui_mode() -> str:
    mode = str(_tenant_settings_for_context().get("reminder_ui_mode_v142") or "new").lower()
    return mode if mode in {"old", "new"} else "new"


def set_reminder_ui_mode(mode: str) -> str:
    mode = "new" if str(mode).lower() == "new" else "old"
    _tenant_settings_for_context()["reminder_ui_mode_v142"] = mode
    save_data(data, root_only=True)
    return mode


def internal_timer_seconds(key: str, fallback=None) -> float:
    spec = INTERNAL_TIMER_DEFS.get(str(key), {})
    default = spec.get("default", fallback if fallback is not None else 0)
    try:
        value = _tenant_settings_for_context().setdefault("internal_timers", {}).get(str(key), default)
        return float(value)
    except Exception:
        return float(default or 0)


def set_internal_timer_seconds(key: str, seconds: int | float) -> float:
    spec = INTERNAL_TIMER_DEFS.get(str(key))
    if not spec:
        raise KeyError(key)
    value = max(float(spec.get("min", 0)), min(float(spec.get("max", 10**9)), float(seconds)))
    _tenant_settings_for_context().setdefault("internal_timers", {})[str(key)] = value
    save_data(data, root_only=True)
    return value


def backup_excel_all_enabled(chat_id: int | None = None) -> bool:
    return bool(_tenant_settings_for_context(chat_id).get("backup_excel_all_enabled", True))


def set_backup_excel_all_enabled(enabled: bool, chat_id: int | None = None):
    _tenant_settings_for_context(chat_id)["backup_excel_all_enabled"] = bool(enabled)
    save_data(data, root_only=True)


def excel_interface_mode(chat_id: int | None = None) -> str:
    mode = str(_tenant_settings_for_context(chat_id).get("excel_interface_mode") or "new").lower()
    return mode if mode in {"old", "new"} else "new"


def set_excel_interface_mode(mode: str) -> str:
    mode = "old" if str(mode).lower() == "old" else "new"
    _tenant_settings_for_context()["excel_interface_mode"] = mode
    save_data(data, root_only=True)
    return mode


def excel_new_export_options() -> dict:
    settings = _tenant_settings_for_context()
    options = settings.get("excel_new_export_options")
    if not isinstance(options, dict):
        options = {"old_table": False, "comments": False, "notes": True, "description_column": False}
        settings["excel_new_export_options"] = options
    return normalize_excel_export_options(options)


def toggle_excel_new_export_option(option: str) -> dict:
    opts = excel_new_export_options()
    option = str(option or "")
    if option in opts:
        opts[option] = not bool(opts.get(option))
    if option == "old_table" and opts.get("old_table"):
        opts["comments"] = opts["notes"] = opts["description_column"] = False
    elif option in {"comments", "notes", "description_column"} and opts.get(option):
        opts["old_table"] = False
        if option in {"comments", "notes"}:
            for other in {"comments", "notes"} - {option}:
                opts[other] = False
    _tenant_settings_for_context()["excel_new_export_options"] = dict(opts)
    save_data(data, root_only=True)
    return opts


def excel_table_style(chat_id: int) -> str:
    mode = _normalize_excel_table_style(_tenant_settings_for_context(chat_id).get("excel_table_style"))
    return mode or "new_notes"


def set_excel_table_style(chat_id: int, mode: str) -> str:
    mode = _normalize_excel_table_style(mode) or "new_notes"
    _tenant_settings_for_context(chat_id)["excel_table_style"] = mode
    try:
        get_chat_store(int(chat_id)).setdefault("settings", {})["excel_table_style"] = mode
    except Exception:
        pass
    save_data(data, chat_ids=[int(chat_id)])
    return mode


# The iPhone expense endpoint remains platform infrastructure in v148.
# Tenant managers do not receive its controls until the HTTP route can resolve a
# tenant-specific token without relying on Telegram thread-local context.


# ─────────────────────────────────────────────────────────────
# Reminder isolation: IDs remain global for scheduler stability, each config has tenant_id.
# UI/callback context sees only current tenant; background scheduler sees all.
# ─────────────────────────────────────────────────────────────
_V148_ORIG_REMINDER_ITEMS = globals().get("_reminder_items")
_V148_ORIG_REMINDER_CFG = globals().get("_reminder_cfg")
_V148_ORIG_REMINDER_CREATE = globals().get("_reminder_create")


def _reminder_context_filter_active() -> bool:
    return bool(getattr(_TENANT_CONTEXT, "tenant_id", None) or current_state_chat_id() is not None)


def _reminder_items(include_completed: bool = False) -> list[tuple[int, dict]]:
    rows = _V148_ORIG_REMINDER_ITEMS(include_completed=include_completed) if callable(_V148_ORIG_REMINDER_ITEMS) else []
    if not _reminder_context_filter_active():
        return rows
    tid = tenant_current_id()
    return [(rid, cfg) for rid, cfg in rows if str((cfg or {}).get("tenant_id") or TENANT_PLATFORM_ID) == tid]


def _reminder_cfg(reminder_id: int | str | None = None, create: bool = False) -> dict | None:
    cfg = _V148_ORIG_REMINDER_CFG(reminder_id, create=create) if callable(_V148_ORIG_REMINDER_CFG) else None
    if not isinstance(cfg, dict):
        return cfg
    if create and not cfg.get("tenant_id"):
        cfg["tenant_id"] = tenant_current_id()
    if _reminder_context_filter_active() and str(cfg.get("tenant_id") or TENANT_PLATFORM_ID) != tenant_current_id():
        return None
    return cfg


def _reminder_create() -> tuple[int, dict]:
    rid, cfg = _V148_ORIG_REMINDER_CREATE()
    cfg["tenant_id"] = tenant_current_id()
    _reminder_save("tenant_reminder_add")
    return rid, cfg


# ─────────────────────────────────────────────────────────────
# Tenant-scoped user permissions
# ─────────────────────────────────────────────────────────────
def security_role_for_user(user_id: int | None) -> str:
    return tenant_role_for_user(user_id, chat_id=current_state_chat_id())


def security_set_role(user_id: int, role: str) -> str:
    tid = tenant_current_id()
    role_map = {
        "finance_admin": "tenant_admin",
        "forward_manager": "operator",
        "secret_manager": "operator",
        "reminder_manager": "operator",
        "expense_input": "operator",
        "view_only": "viewer",
        "standard": "operator",
    }
    tenant_role = role if role in TENANT_ROLE_ORDER else role_map.get(str(role), "viewer")
    if not tenant_can_manage(tenant_current_actor_user_id(), tid):
        raise PermissionError("Недостаточно прав")
    tenant_set_user_role(tid, int(user_id), tenant_role, changed_by=tenant_current_actor_user_id())
    return tenant_role


def security_known_users() -> list[dict]:
    tid = tenant_current_id()
    row = tenant_get(tid) or {}
    allowed = {str(uid) for uid in (row.get("users") or {}).keys()}
    merged = {}
    for cid in tenant_chat_ids(tid):
        try:
            store = get_chat_store(cid)
            for item in (store.get("known_users") or {}).values():
                if not isinstance(item, dict):
                    continue
                uid = str(int(item.get("id") or 0))
                if uid == "0" or (allowed and uid not in allowed):
                    continue
                merged[uid] = dict(item)
        except Exception:
            pass
    for uid, membership in (row.get("users") or {}).items():
        merged.setdefault(str(uid), {"id": int(uid), "first_name": "", "username": "", "last_seen_ts": 0})
        merged[str(uid)]["tenant_role"] = str((membership or {}).get("role") or "viewer")
    return sorted(merged.values(), key=lambda x: (float(x.get("last_seen_ts") or 0), int(x.get("id") or 0)), reverse=True)


def _v177_legacy_0109_security_user_allowed(user_id: int | None, capability: str) -> bool:
    role = tenant_role_for_user(user_id, chat_id=current_state_chat_id())
    if role in {"platform_owner", "tenant_owner", "tenant_admin"}:
        return True
    if role == "operator":
        return str(capability or "view") in {"view", "finance_input", "finance_manage", "export", "forward_manage", "reminder_manage"}
    if role == "viewer":
        return str(capability or "view") == "view"
    # Existing members of a migrated chat retain ordinary input/view behavior.
    return str(capability or "view") in {"view", "finance_input"}
try: _v177_legacy_0109_security_user_allowed.__name__ = 'security_user_allowed'
except Exception: pass
security_user_allowed = _v177_legacy_0109_security_user_allowed


def _tenant_action_target_chat_ids(action: str) -> set[int]:
    raw = str(action or "")
    ids = set()
    for token in re.findall(r"(?<!\d)-?\d{5,16}(?!\d)", raw):
        try:
            ids.add(int(token))
        except Exception:
            pass
    return ids


def _v177_legacy_0111_safety_permission_allowed(user_id: int | None, chat_id: int | None, action: str) -> bool:
    try:
        uid = int(user_id or 0); cid = int(chat_id or 0)
    except Exception:
        return False
    if tenant_is_platform_owner_user(uid):
        return True
    tid = tenant_id_for_chat(cid, create=False)
    # Hard boundary: even old safety profile cannot act on a foreign tenant target.
    for target in _tenant_action_target_chat_ids(action):
        if str(target) in (_tenants_root().get("chat_to_tenant") or {}) and tenant_id_for_chat(target, create=False) != tid:
            return False
    role = tenant_role_for_user(uid, tid)
    normalized = str(action or "").lower()
    if normalized.startswith((
        "mega_", "restore_", "journal_", "runtime_", "problem_tasks",
        "safety_profile", "additional_owners", "addown:", "keepalive_",
        "info_queues", "info_delta_status", "process_center", "integrity_status",
        "expense_",
    )):
        return False
    if normalized.startswith(("sp:", "tenant:")):
        write_actions = ("sp:chatlink", "sp:userlink", "sp:role", "sp:transfer", "sp:rename", "sp:unlink")
        if normalized.startswith(write_actions):
            return role in {"tenant_owner", "tenant_admin"}
        return role in {"tenant_owner", "tenant_admin", "operator", "viewer"}
    if any(x in normalized for x in ("reset", "delete", "del_selected", "fw_new_clear", "security_role")):
        return role in {"tenant_owner", "tenant_admin"}
    if not safety_profile_new_enabled():
        return True
    return security_user_allowed(uid, _security_callback_capability(normalized))
try: _v177_legacy_0111_safety_permission_allowed.__name__ = 'safety_permission_allowed'
except Exception: pass
safety_permission_allowed = _v177_legacy_0111_safety_permission_allowed


# ─────────────────────────────────────────────────────────────
# Invite links and management commands
# ─────────────────────────────────────────────────────────────
def _tenant_bot_username() -> str:
    global _TENANT_BOT_USERNAME
    if _TENANT_BOT_USERNAME:
        return _TENANT_BOT_USERNAME
    try:
        _TENANT_BOT_USERNAME = str(getattr(bot.get_me(), "username", "") or "").lstrip("@")
    except Exception:
        _TENANT_BOT_USERNAME = ""
    return _TENANT_BOT_USERNAME


def _tenant_token_hash(raw: str) -> str:
    return hashlib.sha256(str(raw).encode("utf-8")).hexdigest()


def _tenant_prune_invites() -> None:
    root = _tenants_root(); now_ts = time.time()
    tokens = root.setdefault("invite_tokens", {})
    for key, row in list(tokens.items()):
        if not isinstance(row, dict):
            tokens.pop(key, None); continue
        if bool(row.get("revoked")) or float(row.get("expires_ts") or 0) < now_ts or int(row.get("uses") or 0) >= int(row.get("max_uses") or 1):
            if now_ts - float(row.get("created_ts") or now_ts) > 86400:
                tokens.pop(key, None)
    if len(tokens) > 500:
        ordered = sorted(tokens.items(), key=lambda kv: float((kv[1] or {}).get("created_ts") or 0))
        for key, _ in ordered[:-500]:
            tokens.pop(key, None)


def _v177_legacy_0252_tenant_create_invite(tenant_id: str, kind: str, role: str, created_by: int, max_uses: int = 1, ttl_hours: int = 72) -> str:
    row = tenant_get(tenant_id)
    if not row:
        raise ValueError("Пространство не найдено")
    kind = "chat" if str(kind) == "chat" else "user"
    role = str(role or "operator")
    if role not in TENANT_ROLE_ORDER or role == "tenant_owner":
        role = "operator"
    raw = secrets.token_urlsafe(12).replace("-", "").replace("_", "")[:18]
    prefix = "sc" if kind == "chat" else "su"
    payload = f"{prefix}_{raw}"
    now_ts = time.time()
    _tenant_prune_invites()
    _tenants_root().setdefault("invite_tokens", {})[_tenant_token_hash(payload)] = {
        "tenant_id": str(tenant_id), "kind": kind, "role": role,
        "created_by": int(created_by or 0), "created_at": _tenant_now(), "created_ts": now_ts,
        "expires_ts": now_ts + max(1, int(ttl_hours)) * 3600,
        "max_uses": max(1, int(max_uses)), "uses": 0, "revoked": False,
    }
    save_data(data, root_only=True)
    return payload
try: _v177_legacy_0252_tenant_create_invite.__name__ = 'tenant_create_invite'
except Exception: pass
tenant_create_invite = _v177_legacy_0252_tenant_create_invite


def _v177_legacy_0253_tenant_consume_invite(payload: str, user_id: int, chat_id: int, chat_type: str = "") -> tuple[bool, str, str]:
    key = _tenant_token_hash(str(payload or "").strip())
    row = (_tenants_root().get("invite_tokens") or {}).get(key)
    if not isinstance(row, dict):
        return False, "Ссылка недействительна или уже использована.", ""
    if row.get("revoked") or float(row.get("expires_ts") or 0) < time.time() or int(row.get("uses") or 0) >= int(row.get("max_uses") or 1):
        return False, "Срок действия ссылки закончился.", ""
    tid = str(row.get("tenant_id") or "")
    tenant = tenant_get(tid)
    if not tenant:
        return False, "Пространство не найдено.", ""
    kind = str(row.get("kind") or "user")
    if kind == "chat":
        if str(chat_type or "") == "private" or int(chat_id) > 0:
            return False, "Эту ссылку нужно использовать при добавлении бота в группу/канал.", tid
        if not tenant_user_is_chat_admin(int(chat_id), int(user_id)):
            return False, "Привязать чат может только его администратор.", tid
        old_tid = str((_tenants_root().get("chat_to_tenant") or {}).get(str(int(chat_id))) or "")
        if old_tid and old_tid != tid:
            old = tenant_get(old_tid)
            # Auto-created, empty/unclaimed shell may be replaced by a valid invite.
            if old and int(old.get("owner_user_id") or 0):
                return False, "Этот чат уже принадлежит другому пространству.", tid
        tenant_bind_chat(int(chat_id), tid, changed_by=int(user_id), force=True)
        message = f"✅ Чат подключён к пространству «{tenant.get('name')}»."
    else:
        tenant_set_user_role(tid, int(user_id), str(row.get("role") or "operator"), changed_by=int(row.get("created_by") or 0), save=False)
        message = f"✅ Вы подключены к пространству «{tenant.get('name')}» как {TENANT_ROLE_LABELS.get(str(row.get('role')), str(row.get('role')))}."
    row["uses"] = int(row.get("uses") or 0) + 1
    row["last_used_at"] = _tenant_now(); row["last_used_by"] = int(user_id or 0)
    save_data(data, full=True)
    return True, message, tid
try: _v177_legacy_0253_tenant_consume_invite.__name__ = 'tenant_consume_invite'
except Exception: pass
tenant_consume_invite = _v177_legacy_0253_tenant_consume_invite


def tenant_invite_link(payload: str) -> str:
    username = _tenant_bot_username()
    if not username:
        return payload
    if str(payload).startswith("sc_"):
        return f"https://t.me/{username}?startgroup={payload}"
    return f"https://t.me/{username}?start={payload}"


def tenant_handle_start_payload(msg) -> bool:
    text = str(getattr(msg, "text", "") or "").strip()
    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        return False
    payload = parts[1].strip()
    if not payload.startswith(("su_", "sc_")):
        return False
    try:
        uid = int(getattr(getattr(msg, "from_user", None), "id", 0) or 0)
        cid = int(msg.chat.id)
        typ = str(getattr(msg.chat, "type", "") or "")
        ok, message, _tid = tenant_consume_invite(payload, uid, cid, typ)
        bot.send_message(cid, message)
        try:
            bot_journal("tenant_invite_consumed" if ok else "tenant_invite_rejected", cid, f"user={uid} payload={payload[:5]}***")
        except Exception:
            pass
    except Exception as exc:
        bot.send_message(msg.chat.id, f"❌ Не удалось применить ссылку: {str(exc)[:300]}")
    return True


def _v177_legacy_0254_tenant_visible_spaces(user_id: int) -> list[dict]:
    return tenant_all() if tenant_is_platform_owner_user(user_id) else tenant_user_spaces(user_id)
try: _v177_legacy_0254_tenant_visible_spaces.__name__ = 'tenant_visible_spaces'
except Exception: pass
tenant_visible_spaces = _v177_legacy_0254_tenant_visible_spaces


def _v177_legacy_0255_tenant_dashboard_text(chat_id: int, user_id: int) -> str:
    current_tid = tenant_id_for_chat(chat_id, create=True, actor_user_id=user_id)
    current = tenant_get(current_tid) or {}
    spaces = tenant_visible_spaces(user_id)
    role = tenant_role_for_user(user_id, current_tid)
    return (
        "🏢 ПРОСТРАНСТВА · ИЗОЛИРОВАННЫЙ РЕЖИМ\n\n"
        f"Текущий чат: {get_chat_display_name(chat_id)}\n"
        f"Пространство: {current.get('name') or current_tid}\n"
        f"Роль: {TENANT_ROLE_LABELS.get(role, role)}\n"
        f"Чатов в пространстве: {len(current.get('chat_ids') or [])}\n"
        f"Подключённых пользователей: {len(current.get('users') or {})}\n\n"
        f"Доступно пространств: {len(spaces)}\n"
        "Чужие чаты, настройки, финансы, напоминания и пересылки здесь не отображаются."
    )
try: _v177_legacy_0255_tenant_dashboard_text.__name__ = 'tenant_dashboard_text'
except Exception: pass
tenant_dashboard_text = _v177_legacy_0255_tenant_dashboard_text


def _v177_legacy_0256_tenant_dashboard_keyboard(chat_id: int, user_id: int):
    kb = types.InlineKeyboardMarkup(row_width=1)
    current_tid = tenant_id_for_chat(chat_id, create=True, actor_user_id=user_id)
    for row in tenant_visible_spaces(user_id)[:25]:
        tid = str(row.get("id")); mark = "✅" if tid == current_tid else "▫️"
        kb.row(IB(f"{mark} {row.get('name')} · {len(row.get('chat_ids') or [])} ч.", callback_data=f"sp:open:{tid}"))
    if tenant_can_manage(user_id, current_tid):
        kb.row(IB("💬 Чаты пространства", callback_data=f"sp:chats:{current_tid}"))
        kb.row(IB("👥 Пользователи", callback_data=f"sp:users:{current_tid}"))
        kb.row(IB("🔗 Ссылка для подключения чата", callback_data=f"sp:chatlink:{current_tid}"))
        kb.row(IB("👤 Ссылка для пользователя", callback_data=f"sp:userlink:{current_tid}:operator"))
    kb.row(IB("❌ Закрыть", callback_data="info_close"))
    return kb
try: _v177_legacy_0256_tenant_dashboard_keyboard.__name__ = 'tenant_dashboard_keyboard'
except Exception: pass
tenant_dashboard_keyboard = _v177_legacy_0256_tenant_dashboard_keyboard


def _v177_legacy_0257_tenant_detail_text(tenant_id: str, viewer_user_id: int) -> str:
    row = tenant_get(tenant_id)
    visible_ids = {str(item.get("id")) for item in tenant_visible_spaces(viewer_user_id)}
    if not row or str(tenant_id) not in visible_ids:
        return "❌ Пространство недоступно."
    owner = int(row.get("owner_user_id") or 0)
    lines = [
        f"🏢 {row.get('name')}", "",
        f"ID: {row.get('id')}",
        f"Владелец: {security_user_display(owner) if owner else 'не назначен'}",
        f"Корневой чат: {get_chat_display_name(int(row.get('root_chat_id') or 0))}",
        f"Чатов: {len(row.get('chat_ids') or [])}",
        f"Пользователей: {len(row.get('users') or {})}",
        "", "Чаты:",
    ]
    for cid in row.get("chat_ids") or []:
        lines.append(f"• {get_chat_display_name(int(cid))} · {int(cid)}")
    return "\n".join(lines)[:3900]
try: _v177_legacy_0257_tenant_detail_text.__name__ = 'tenant_detail_text'
except Exception: pass
tenant_detail_text = _v177_legacy_0257_tenant_detail_text


def tenant_users_text(tenant_id: str) -> str:
    row = tenant_get(tenant_id) or {}
    lines = [f"👥 ПОЛЬЗОВАТЕЛИ · {row.get('name')}", ""]
    for uid, item in sorted((row.get("users") or {}).items(), key=lambda kv: (TENANT_ROLE_ORDER.index(str((kv[1] or {}).get("role") or "viewer")), int(kv[0]))):
        role = str((item or {}).get("role") or "viewer")
        lines.append(f"• {security_user_display(int(uid))} · {TENANT_ROLE_LABELS.get(role, role)} · {uid}")
    if len(lines) == 2:
        lines.append("Нет подключённых пользователей.")
    return "\n".join(lines)[:3900]


def _v177_legacy_0258_tenant_chats_text(tenant_id: str) -> str:
    row = tenant_get(tenant_id) or {}
    lines = [f"💬 ЧАТЫ · {row.get('name')}", ""]
    for cid in row.get("chat_ids") or []:
        marker = "🏠" if int(cid) == int(row.get("root_chat_id") or 0) else "•"
        lines.append(f"{marker} {get_chat_display_name(int(cid))} · {int(cid)}")
    return "\n".join(lines)[:3900]
try: _v177_legacy_0258_tenant_chats_text.__name__ = 'tenant_chats_text'
except Exception: pass
tenant_chats_text = _v177_legacy_0258_tenant_chats_text


def _v177_legacy_0260_tenant_handle_callback(call, data_str: str) -> bool:
    raw = str(data_str or "")
    if not raw.startswith("sp:"):
        return False
    chat_id = int(call.message.chat.id)
    user_id = int(getattr(call.from_user, "id", 0) or 0)
    parts = raw.split(":")
    action = parts[1] if len(parts) > 1 else ""
    tid = parts[2] if len(parts) > 2 else tenant_id_for_chat(chat_id, create=True, actor_user_id=user_id)
    if action == "dashboard":
        safe_edit(bot, call, tenant_dashboard_text(chat_id, user_id), reply_markup=tenant_dashboard_keyboard(chat_id, user_id))
        return True
    if not tenant_is_platform_owner_user(user_id) and not any(str(r.get("id")) == tid for r in tenant_visible_spaces(user_id)):
        bot.answer_callback_query(call.id, "Пространство недоступно.", show_alert=True)
        return True
    if action == "open":
        kb = types.InlineKeyboardMarkup(row_width=1)
        if tenant_can_manage(user_id, tid):
            kb.row(IB("💬 Чаты", callback_data=f"sp:chats:{tid}"), IB("👥 Пользователи", callback_data=f"sp:users:{tid}"))
            kb.row(IB("🔗 Подключить чат", callback_data=f"sp:chatlink:{tid}"))
            kb.row(IB("👤 Пригласить оператора", callback_data=f"sp:userlink:{tid}:operator"))
            kb.row(IB("👁 Пригласить зрителя", callback_data=f"sp:userlink:{tid}:viewer"))
        kb.row(IB("🔙 К пространствам", callback_data="sp:list:x"))
        safe_edit(bot, call, tenant_detail_text(tid, user_id), reply_markup=kb)
        return True
    if action == "list":
        safe_edit(bot, call, tenant_dashboard_text(chat_id, user_id), reply_markup=tenant_dashboard_keyboard(chat_id, user_id))
        return True
    if action in {"chatlink", "userlink"} and not tenant_can_manage(user_id, tid):
        bot.answer_callback_query(call.id, "Недостаточно прав.", show_alert=True)
        return True
    if action == "chats":
        kb = types.InlineKeyboardMarkup(); kb.row(IB("🔙 Назад", callback_data=f"sp:open:{tid}"))
        safe_edit(bot, call, tenant_chats_text(tid), reply_markup=kb); return True
    if action == "users":
        kb = types.InlineKeyboardMarkup(); kb.row(IB("🔙 Назад", callback_data=f"sp:open:{tid}"))
        safe_edit(bot, call, tenant_users_text(tid), reply_markup=kb); return True
    if action == "chatlink":
        payload = tenant_create_invite(tid, "chat", "tenant_admin", user_id, max_uses=1, ttl_hours=72)
        text = (
            "🔗 ССЫЛКА ДЛЯ ПОДКЛЮЧЕНИЯ ЧАТА\n\n"
            + tenant_invite_link(payload)
            + f"\n\nКод: {payload}"
            + f"\nВ уже существующем чате можно выполнить: /space_join {payload}"
            + "\n\nОдноразовая, действует 72 часа. Привязку должен подтвердить администратор целевого чата."
        )
        kb = types.InlineKeyboardMarkup(); kb.row(IB("🔙 Назад", callback_data=f"sp:open:{tid}"))
        safe_edit(bot, call, text, reply_markup=kb); return True
    if action == "userlink":
        role = parts[3] if len(parts) > 3 else "operator"
        payload = tenant_create_invite(tid, "user", role, user_id, max_uses=20, ttl_hours=72)
        text = f"👤 ССЫЛКА ДЛЯ ПОЛЬЗОВАТЕЛЯ\n\n{tenant_invite_link(payload)}\n\nРоль: {TENANT_ROLE_LABELS.get(role, role)}. До 20 использований, 72 часа."
        kb = types.InlineKeyboardMarkup(); kb.row(IB("🔙 Назад", callback_data=f"sp:open:{tid}"))
        safe_edit(bot, call, text, reply_markup=kb); return True
    return True
try: _v177_legacy_0260_tenant_handle_callback.__name__ = 'tenant_handle_callback'
except Exception: pass
tenant_handle_callback = _v177_legacy_0260_tenant_handle_callback


def _tenant_command_parts(msg) -> list[str]:
    return str(getattr(msg, "text", "") or "").strip().split()


def _tenant_send_dashboard(msg):
    uid = int(getattr(getattr(msg, "from_user", None), "id", 0) or 0)
    cid = int(msg.chat.id)
    tenant_note_chat_seen(msg)
    bot.send_message(cid, tenant_dashboard_text(cid, uid), reply_markup=tenant_dashboard_keyboard(cid, uid))


@bot.message_handler(commands=["space", "spaces", "tenant", "пространство", "пространства"])
def cmd_tenant_space(msg):
    schedule_command_delete(msg)
    _tenant_send_dashboard(msg)


@bot.message_handler(commands=["space_create", "tenant_create"])
def cmd_tenant_create(msg):
    schedule_command_delete(msg)
    uid = int(getattr(getattr(msg, "from_user", None), "id", 0) or 0); cid = int(msg.chat.id)
    if not tenant_user_is_chat_admin(cid, uid):
        send_and_auto_delete(cid, "❌ Создать пространство может владелец личного чата или администратор группы.", 12); return
    mapped_tid = str((_tenants_root().get("chat_to_tenant") or {}).get(str(cid)) or "")
    current_tid = mapped_tid or ""
    current = tenant_get(current_tid) if current_tid else None
    if current and (int(current.get("owner_user_id") or 0) or current_tid == TENANT_PLATFORM_ID):
        send_and_auto_delete(cid, "❌ Этот чат уже принадлежит пространству. Используйте /space.", 12); return
    parts = _tenant_command_parts(msg); name = " ".join(parts[1:]).strip() or _tenant_default_name(cid)
    if current:
        current["name"] = name[:80]; tenant_set_user_role(current_tid, uid, "tenant_owner", changed_by=uid, save=False); tid = current_tid
    else:
        tid = tenant_create(name, uid, cid, created_by=uid, deterministic_chat_id=cid)
    save_data(data, full=True)
    bot.send_message(cid, f"✅ Создано пространство «{tenant_get(tid).get('name')}».\nОткройте /space для управления.")


@bot.message_handler(commands=["space_claim", "tenant_claim"])
def cmd_tenant_claim(msg):
    schedule_command_delete(msg)
    uid = int(getattr(getattr(msg, "from_user", None), "id", 0) or 0); cid = int(msg.chat.id)
    if not tenant_user_is_chat_admin(cid, uid):
        send_and_auto_delete(cid, "❌ Подтвердить владение может только администратор этого чата.", 12); return
    tid = tenant_id_for_chat(cid, create=True, actor_user_id=uid); row = tenant_get(tid)
    if int(row.get("owner_user_id") or 0) and not tenant_is_platform_owner_user(uid):
        send_and_auto_delete(cid, "❌ У пространства уже есть владелец.", 12); return
    parts = _tenant_command_parts(msg)
    if len(parts) > 1:
        row["name"] = " ".join(parts[1:])[:80]
    tenant_set_user_role(tid, uid, "tenant_owner", changed_by=uid)
    bot.send_message(cid, f"✅ Вы стали владельцем пространства «{row.get('name')}».")


@bot.message_handler(commands=["space_join", "tenant_join"])
def cmd_tenant_join(msg):
    schedule_command_delete(msg)
    parts = _tenant_command_parts(msg)
    if len(parts) < 2:
        send_and_auto_delete(msg.chat.id, "Использование: /space_join КОД_ССЫЛКИ", 12); return
    payload = parts[1].strip()
    uid = int(getattr(getattr(msg, "from_user", None), "id", 0) or 0); cid = int(msg.chat.id)
    ok, text, _ = tenant_consume_invite(payload, uid, cid, str(getattr(msg.chat, "type", "") or ""))
    bot.send_message(cid, text)


@bot.message_handler(commands=["space_chat_link", "tenant_chat_link"])
def cmd_tenant_chat_link(msg):
    schedule_command_delete(msg)
    uid = int(getattr(getattr(msg, "from_user", None), "id", 0) or 0); cid = int(msg.chat.id); tid = tenant_id_for_chat(cid, create=True, actor_user_id=uid)
    if not tenant_can_manage(uid, tid):
        send_and_auto_delete(cid, "❌ Недостаточно прав.", 12); return
    payload = tenant_create_invite(tid, "chat", "tenant_admin", uid, max_uses=1, ttl_hours=72)
    bot.send_message(
        cid,
        "🔗 Ссылка для подключения одного чата (72 часа):\n"
        + tenant_invite_link(payload)
        + f"\n\nКод: {payload}\nВ уже существующем чате: /space_join {payload}",
    )


@bot.message_handler(commands=["space_user_link", "tenant_user_link"])
def cmd_tenant_user_link(msg):
    schedule_command_delete(msg)
    uid = int(getattr(getattr(msg, "from_user", None), "id", 0) or 0); cid = int(msg.chat.id); tid = tenant_id_for_chat(cid, create=True, actor_user_id=uid)
    if not tenant_can_manage(uid, tid):
        send_and_auto_delete(cid, "❌ Недостаточно прав.", 12); return
    parts = _tenant_command_parts(msg); role = parts[1].lower() if len(parts) > 1 else "operator"
    if role not in {"tenant_admin", "operator", "viewer"}:
        role = "operator"
    payload = tenant_create_invite(tid, "user", role, uid, max_uses=20, ttl_hours=72)
    bot.send_message(cid, f"👤 Ссылка для пользователей ({TENANT_ROLE_LABELS.get(role)}):\n{tenant_invite_link(payload)}")


@bot.message_handler(commands=["space_users", "tenant_users"])
def cmd_tenant_users(msg):
    schedule_command_delete(msg)
    uid = int(getattr(getattr(msg, "from_user", None), "id", 0) or 0); cid = int(msg.chat.id); tid = tenant_id_for_chat(cid, create=True, actor_user_id=uid)
    if not tenant_can_manage(uid, tid):
        send_and_auto_delete(cid, "❌ Недостаточно прав.", 12); return
    bot.send_message(cid, tenant_users_text(tid))


@bot.message_handler(commands=["space_chats", "tenant_chats"])
def cmd_tenant_chats(msg):
    schedule_command_delete(msg)
    uid = int(getattr(getattr(msg, "from_user", None), "id", 0) or 0); cid = int(msg.chat.id); tid = tenant_id_for_chat(cid, create=True, actor_user_id=uid)
    if not tenant_can_manage(uid, tid):
        send_and_auto_delete(cid, "❌ Недостаточно прав.", 12); return
    bot.send_message(cid, tenant_chats_text(tid))


@bot.message_handler(commands=["space_role", "tenant_role"])
def cmd_tenant_role(msg):
    schedule_command_delete(msg)
    actor = int(getattr(getattr(msg, "from_user", None), "id", 0) or 0); cid = int(msg.chat.id); tid = tenant_id_for_chat(cid, create=True, actor_user_id=actor)
    if not tenant_can_manage(actor, tid):
        send_and_auto_delete(cid, "❌ Недостаточно прав.", 12); return
    parts = _tenant_command_parts(msg)
    if len(parts) < 3:
        send_and_auto_delete(cid, "Использование: /space_role USER_ID tenant_admin|operator|viewer", 15); return
    try:
        uid = int(parts[1]); role = parts[2].lower()
    except Exception:
        send_and_auto_delete(cid, "❌ Неверный USER_ID.", 12); return
    if role not in {"tenant_admin", "operator", "viewer"}:
        send_and_auto_delete(cid, "❌ Допустимые роли: tenant_admin, operator, viewer.", 12); return
    tenant_set_user_role(tid, uid, role, changed_by=actor)
    bot.send_message(cid, f"✅ Пользователь {uid}: {TENANT_ROLE_LABELS.get(role)}.")


@bot.message_handler(commands=["space_transfer", "tenant_transfer"])
def cmd_tenant_transfer(msg):
    schedule_command_delete(msg)
    actor = int(getattr(getattr(msg, "from_user", None), "id", 0) or 0); cid = int(msg.chat.id); tid = tenant_id_for_chat(cid, create=True, actor_user_id=actor)
    if not tenant_can_manage(actor, tid, owner_only=True):
        send_and_auto_delete(cid, "❌ Передать владение может только владелец пространства.", 12); return
    if tid == TENANT_PLATFORM_ID:
        send_and_auto_delete(cid, "❌ Владение всей платформой закреплено за основным владельцем и не передаётся этой командой.", 15); return
    parts = _tenant_command_parts(msg)
    if len(parts) < 2:
        send_and_auto_delete(cid, "Использование: /space_transfer USER_ID", 12); return
    uid = int(parts[1]); tenant_set_user_role(tid, uid, "tenant_owner", changed_by=actor)
    bot.send_message(cid, f"✅ Владение пространством передано пользователю {uid}.")


@bot.message_handler(commands=["space_rename", "tenant_rename"])
def cmd_tenant_rename(msg):
    schedule_command_delete(msg)
    actor = int(getattr(getattr(msg, "from_user", None), "id", 0) or 0); cid = int(msg.chat.id); tid = tenant_id_for_chat(cid, create=True, actor_user_id=actor)
    if not tenant_can_manage(actor, tid):
        send_and_auto_delete(cid, "❌ Недостаточно прав.", 12); return
    parts = _tenant_command_parts(msg); name = " ".join(parts[1:]).strip()
    if not name:
        send_and_auto_delete(cid, "Использование: /space_rename Новое название", 12); return
    tenant_get(tid)["name"] = name[:80]; tenant_get(tid)["updated_at"] = _tenant_now(); save_data(data, root_only=True)
    bot.send_message(cid, f"✅ Пространство переименовано: {name[:80]}")


@bot.message_handler(commands=["space_unlink", "tenant_unlink"])
def cmd_tenant_unlink(msg):
    schedule_command_delete(msg)
    actor = int(getattr(getattr(msg, "from_user", None), "id", 0) or 0); cid = int(msg.chat.id); tid = tenant_id_for_chat(cid, create=True, actor_user_id=actor)
    if not tenant_can_manage(actor, tid):
        send_and_auto_delete(cid, "❌ Недостаточно прав.", 12); return
    row = tenant_get(tid)
    if int(row.get("root_chat_id") or 0) == cid:
        send_and_auto_delete(cid, "❌ Корневой чат нельзя отсоединить. Можно передать владение или подключить другие чаты.", 15); return
    tenant_unbind_chat(cid, changed_by=actor); save_data(data, full=True)
    bot.send_message(cid, "✅ Чат отсоединён и получил собственное изолированное пространство.")




# ─────────────────────────────────────────────────────────────
# Legacy UI hardening: tenant lists instead of global lists
# ─────────────────────────────────────────────────────────────
def tenant_require_platform_owner(msg, notify: bool = True) -> bool:
    uid = int(getattr(getattr(msg, "from_user", None), "id", 0) or 0)
    ok = tenant_is_platform_owner_user(uid)
    if not ok and notify:
        try:
            send_and_auto_delete(int(msg.chat.id), "Эта команда доступна только владельцу всей платформы.", 10)
        except Exception:
            pass
    return ok


def build_fin_windows_chat_menu(day_key: str):
    kb = types.InlineKeyboardMarkup(row_width=2)
    buttons = []
    for cid in tenant_chat_ids(tenant_current_id()):
        if not is_finance_mode(int(cid)):
            continue
        if is_chat_bot_removed(int(cid)) and not (OWNER_ID and str(int(cid)) == str(OWNER_ID)):
            continue
        buttons.append(IB(chat_button_title(int(cid), get_chat_display_name(int(cid))), callback_data=f"d:{day_key}:finwin_open_{int(cid)}"))
    if buttons:
        add_buttons_in_rows(kb, sorted(buttons, key=lambda b: str(getattr(b, "text", "")).casefold()), 2)
    else:
        kb.row(IB("Нет чатов с финрежимом", callback_data="none"))
    kb.row(IB("🔙 Назад", callback_data=f"d:{day_key}:back_main"))
    return kb


def _v177_legacy_0061_build_forward_status_lines() -> list[str]:
    lines = []
    fr = data.get("forward_rules", {}) or {}
    ff = data.get("forward_finance", {}) or {}
    allowed = set(tenant_chat_ids(tenant_current_id()))
    pairs = set()
    for src, dsts in fr.items():
        try:
            a = int(src)
        except Exception:
            continue
        if a not in allowed:
            continue
        for dst in (dsts or {}).keys():
            try:
                b = int(dst)
            except Exception:
                continue
            if b not in allowed:
                continue
            pairs.add(tuple(sorted((a, b))))
    for a, b in sorted(pairs, key=lambda p: (get_chat_display_name(p[0]).casefold(), get_chat_display_name(p[1]).casefold())):
        ab = str(b) in (fr.get(str(a), {}) or {})
        ba = str(a) in (fr.get(str(b), {}) or {})
        if not (ab or ba):
            continue
        ab_fin = bool((ff.get(str(a), {}) or {}).get(str(b), False))
        ba_fin = bool((ff.get(str(b), {}) or {}).get(str(a), False))
        lines.append(f"• {chat_button_title(a)} -({_forward_arrow_icon(ab, ba)})-({_forward_fin_icon(ab_fin, ba_fin)})-{chat_button_title(b)}")
    return lines or ["• Связи пересылки не настроены"]
try: _v177_legacy_0061_build_forward_status_lines.__name__ = 'build_forward_status_lines'
except Exception: pass
build_forward_status_lines = _v177_legacy_0061_build_forward_status_lines


_V148_ORIG_BUILD_INFO_KEYBOARD = globals().get("build_info_keyboard")
def _v177_legacy_0217_build_info_keyboard(chat_id: int):
    kb = _V148_ORIG_BUILD_INFO_KEYBOARD(int(chat_id))
    platform = tenant_is_platform_owner_context(int(chat_id))
    if not platform:
        blocked_prefixes = (
            "journal_", "restore_guard", "mega_manual_restore", "mega_priority", "keepalive_",
            "process_center", "safety_profile", "problem_tasks", "integrity_status", "info_queues",
            "runtime_watcher", "info_delta_status", "additional_owners", "addown:",
            "expense_",
        )
        clean_rows = []
        for row in list(getattr(kb, "keyboard", []) or []):
            kept = []
            for button in row:
                cb = str(getattr(button, "callback_data", "") or "")
                if cb.startswith(blocked_prefixes):
                    continue
                kept.append(button)
            if kept:
                clean_rows.append(kept)
        kb.keyboard = clean_rows
    actor = tenant_current_actor_user_id()
    role = tenant_role_for_user(actor, chat_id=int(chat_id)) if actor else ("tenant_owner" if is_owner_chat(int(chat_id)) else "standard")
    if role in {"platform_owner", "tenant_owner", "tenant_admin", "operator", "viewer"}:
        if not any(str(getattr(button, "callback_data", "") or "") == "sp:dashboard" for row in getattr(kb, "keyboard", []) for button in row):
            kb.row(IB("🏢 Пространство", callback_data="sp:dashboard"))
    return kb
try: _v177_legacy_0217_build_info_keyboard.__name__ = 'build_info_keyboard'
except Exception: pass
build_info_keyboard = _v177_legacy_0217_build_info_keyboard


_V148_ORIG_BUILD_INFO_TEXT = globals().get("build_info_text")
def _v177_legacy_0055_build_info_text(chat_id: int) -> str:
    text = _V148_ORIG_BUILD_INFO_TEXT(int(chat_id))
    if not tenant_is_platform_owner_context(int(chat_id)):
        forbidden = ("/errors", "/runtime_export", "/mega_", "/queues", "/journal", "/sqlite", "/db", "/restore_guard", "MEGA:")
        text = "\n".join(line for line in str(text).splitlines() if not any(token in line for token in forbidden))
    tid = tenant_id_for_chat(int(chat_id), create=False)
    row = tenant_get(tid) or {}
    suffix = f"\n\n🏢 Пространство: {row.get('name') or tid}\n/space — чаты, пользователи и ссылки подключения"
    return (str(text).rstrip() + suffix)[:3900]
try: _v177_legacy_0055_build_info_text.__name__ = 'build_info_text'
except Exception: pass
build_info_text = _v177_legacy_0055_build_info_text


_V148_ORIG_BUILD_HELP_TEXT = globals().get("build_help_text")
def build_help_text(chat_id: int) -> str:
    text = _V148_ORIG_BUILD_HELP_TEXT(int(chat_id))
    actor = tenant_current_actor_user_id()
    tid = tenant_id_for_chat(int(chat_id), create=False)
    role = tenant_role_for_user(actor, tid) if actor else "standard"
    lines = [str(text).rstrip(), "", "🏢 Изолированное пространство:", "/space — открыть пространство"]
    if role in {"platform_owner", "tenant_owner", "tenant_admin"}:
        lines.extend([
            "/space_chat_link — подключить свой дополнительный чат",
            "/space_user_link operator — пригласить пользователя",
            "/space_users — пользователи и роли",
            "/space_chats — чаты пространства",
        ])
    return "\n".join(lines)[:3900]


# ─────────────────────────────────────────────────────────────
# Startup migration and integrity audit
# ─────────────────────────────────────────────────────────────
def tenant_v148_enforce_forward_isolation() -> int:
    removed = 0
    fr = data.setdefault("forward_rules", {})
    ff = data.setdefault("forward_finance", {})
    for src, dsts in list(fr.items()):
        try:
            src_id = int(src)
        except Exception:
            fr.pop(src, None)
            ff.pop(str(src), None)
            continue
        if not isinstance(dsts, dict):
            fr.pop(src, None)
            ff.pop(str(src_id), None)
            continue
        for dst in list(dsts.keys()):
            try:
                dst_id = int(dst)
            except Exception:
                dsts.pop(dst, None)
                (ff.get(str(src_id)) or {}).pop(str(dst), None)
                removed += 1
                continue
            if not tenant_same_space(src_id, dst_id):
                dsts.pop(dst, None)
                (ff.get(str(src_id)) or {}).pop(str(dst_id), None)
                removed += 1
        if not dsts:
            fr.pop(str(src), None)
            ff.pop(str(src_id), None)
    if removed:
        persist_forward_rules_to_owner()
        try:
            bot_journal("tenant_cross_links_removed", OWNER_ID, f"count={removed}", "WARN")
        except Exception:
            pass
    return removed


def tenant_v148_bootstrap() -> dict:
    root = _tenants_root()
    report = {"created": 0, "bound": 0, "legacy_owners": 0, "reminders_tagged": 0, "cross_links_removed": 0}
    with _TENANT_LOCK:
        if TENANT_PLATFORM_ID not in root["tenants"]:
            root["tenants"][TENANT_PLATFORM_ID] = _tenant_normalize(TENANT_PLATFORM_ID, {
                "name": "Основное пространство владельца",
                "owner_user_id": _tenant_platform_owner_user_id(),
                "root_chat_id": _tenant_platform_owner_user_id(),
                "chat_ids": [], "users": {}, "settings": {},
                "created_by": _tenant_platform_owner_user_id(),
            })
            report["created"] += 1
        platform_row = tenant_get(TENANT_PLATFORM_ID)
        legacy_ids = []
        try:
            legacy_ids = [int(x) for x in data.setdefault("_global_settings", {}).get("additional_owner_ids", [])]
        except Exception:
            legacy_ids = []
        owner_tenants = {}
        for uid in legacy_ids:
            tid = f"owner_{uid}"
            if tid not in root["tenants"]:
                root["tenants"][tid] = _tenant_normalize(tid, {
                    "name": f"Пространство {get_chat_display_name(uid)}",
                    "owner_user_id": uid, "root_chat_id": uid, "chat_ids": [uid], "users": {}, "settings": {},
                    "created_by": _tenant_platform_owner_user_id(),
                })
                report["created"] += 1
            owner_tenants[uid] = tid; report["legacy_owners"] += 1
        if legacy_ids:
            data["_global_settings"]["legacy_additional_owner_ids_v148"] = legacy_ids
            data["_global_settings"]["additional_owner_ids"] = []
        for cid_raw, store in list((data.get("chats") or {}).items()):
            try:
                cid = int(cid_raw)
            except Exception:
                continue
            target_tid = TENANT_PLATFORM_ID
            try:
                old_scope = int(((store or {}).get("settings") or {}).get("owner_scope_id") or 0)
                if old_scope in owner_tenants:
                    target_tid = owner_tenants[old_scope]
            except Exception:
                pass
            if not (root.get("chat_to_tenant") or {}).get(str(cid)):
                tenant_bind_chat(cid, target_tid, changed_by=_tenant_platform_owner_user_id(), force=True)
                report["bound"] += 1
        # Owner personal/root chats are always present.
        for row in tenant_all():
            rid = int(row.get("root_chat_id") or 0)
            if rid and rid not in row["chat_ids"]:
                row["chat_ids"].append(rid)
            if rid:
                root["chat_to_tenant"][str(rid)] = str(row.get("id"))
        # Existing v147 reminders belong to the platform tenant unless already tagged.
        try:
            rem_root = data.setdefault("_global_settings", {}).get("reminders_v2") or {}
            for cfg in (rem_root.get("items") or {}).values():
                if isinstance(cfg, dict) and not cfg.get("tenant_id"):
                    cfg["tenant_id"] = TENANT_PLATFORM_ID; report["reminders_tagged"] += 1
        except Exception:
            pass
        # Import legacy global owner settings only into platform tenant once.
        if not platform_row.get("settings_migrated_v148"):
            gs = data.setdefault("_global_settings", {})
            for key in (
                "buttons_current_window", "forward_menu_new_style", "icon_button_mode", "total_secret_mask_enabled",
                "finance_day_start_5am", "finance_day_start_minute", "mega_backup_priority", "internal_timers",
                "backup_excel_all_enabled", "excel_interface_mode", "excel_new_export_options", "excel_table_style_global",
                "forward_copy_edit_mode_global", "reminder_ui_mode_v142", "expense_shortcut",
            ):
                if key in gs:
                    mapped = key.replace("_global", "") if key in {"excel_table_style_global", "forward_copy_edit_mode_global"} else key
                    platform_row.setdefault("settings", {})[mapped] = copy.deepcopy(gs.get(key))
            platform_row["settings_migrated_v148"] = True
        root["legacy_migrated"] = True
        root["schema_version"] = TENANT_SCHEMA_VERSION
    # Remove already-configured cross-space routes so a stale rule cannot leak after deploy.
    report["cross_links_removed"] += tenant_v148_enforce_forward_isolation()
    save_data(data, full=True)
    try:
        bot_journal("tenant_v148_bootstrap", OWNER_ID, json.dumps(report, ensure_ascii=False))
    except Exception:
        pass
    return report


def tenant_v148_snapshot() -> dict:
    root = _tenants_root()
    return {
        "schema_version": root.get("schema_version"),
        "tenants": len(root.get("tenants") or {}),
        "bound_chats": len(root.get("chat_to_tenant") or {}),
        "active_invites": sum(1 for row in (root.get("invite_tokens") or {}).values() if isinstance(row, dict) and not row.get("revoked") and float(row.get("expires_ts") or 0) >= time.time() and int(row.get("uses") or 0) < int(row.get("max_uses") or 1)),
    }

# v178_global_performance_final
