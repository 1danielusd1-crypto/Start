# v164_circle_hierarchy_spaces
"""v164: explicit owner / first-circle / second-circle hierarchy with isolated tenants.

Rules:
- OWNER_ID private owner contour is circle 0 / platform tenant only.
- A chat that appears directly (normal /start/message, not through a first-circle invite) is circle 1
  and gets its own dedicated tenant.
- A chat joined with a chat-link created from a circle-1 chat is circle 2. It also gets its own
  dedicated tenant and stores parent_first_chat_id instead of joining the parent's tenant.
- Platform owner can administer circle lists globally without mixing their data into platform tenant.
- Circle-1 managers can administer their own circle-2 descendants.
"""

import copy as _v164_copy
import gzip as _v164_gzip
import hashlib as _v164_hashlib
import json as _v164_json
import os as _v164_os
import shutil as _v164_shutil
import sqlite3 as _v164_sqlite3
import tempfile as _v164_tempfile
import threading as _v164_threading
import time as _v164_time

VERSION = "bot_v164_circle_hierarchy_spaces"
V164_CIRCLE_SCHEMA = 1

_V164_LOCK = _v164_threading.RLock()
_V164_WINDOW_VIEW_LOCK = _v164_threading.RLock()
_V164_WINDOW_VIEW = {}
_V164_MIGRATING = False

# Preserve the implementation that existed before v164. Calls remain useful for the low-level
# tenant storage mechanics, but v164 decides hierarchy/classification itself.
_V164_PREV_TENANT_ID_FOR_CHAT = globals().get("tenant_id_for_chat")
_V164_PREV_TENANT_NOTE_CHAT_SEEN = globals().get("tenant_note_chat_seen")
_V164_PREV_TENANT_BIND_CHAT = globals().get("tenant_bind_chat")
_V164_PREV_TENANT_CAN_MANAGE = globals().get("tenant_can_manage")
_V164_PREV_TENANT_CREATE_INVITE = globals().get("tenant_create_invite")
_V164_PREV_TENANT_CONSUME_INVITE = globals().get("tenant_consume_invite")
_V164_PREV_TENANT_SAME_SPACE = globals().get("tenant_same_space")
_V164_PREV_ADD_FORWARD_LINK = globals().get("add_forward_link")
_V164_PREV_COLLECT_FORWARD_PAIRS = globals().get("collect_forward_pairs_for_menu")
_V164_PREV_BUILD_FORWARD_NEW_MENU = globals().get("build_forward_new_menu")
_V164_PREV_BUILD_FORWARD_SOURCE_MENU = globals().get("build_forward_source_menu")
_V164_PREV_BUILD_FORWARD_TARGET_MENU = globals().get("build_forward_target_menu")
_V164_PREV_BUILD_QUICK_BALANCE_MODE_MENU = globals().get("build_quick_balance_mode_menu")
_V164_PREV_BUILD_CHAT_DESCRIPTION_MENU = globals().get("build_chat_description_menu")
_V164_PREV_RESTORE_VALIDATE = globals().get("_v153_validate_restore_gz")


def _v164_owner_id() -> int:
    try:
        return int(OWNER_ID or 0)
    except Exception:
        return 0


def _v164_now() -> str:
    try:
        return _tenant_now()
    except Exception:
        return _v164_time.strftime("%Y-%m-%dT%H:%M:%S")


def _v164_root() -> dict:
    gs = data.setdefault("global_settings", {})
    root = gs.get("circle_hierarchy_v164")
    if not isinstance(root, dict):
        root = {}
        gs["circle_hierarchy_v164"] = root
    root.setdefault("schema_version", V164_CIRCLE_SCHEMA)
    root.setdefault("chat_meta", {})
    root.setdefault("migration_runs", 0)
    root.setdefault("global_forward_pairs", {})
    root.setdefault("created_at", _v164_now())
    return root


def _v164_mapping() -> dict:
    try:
        return _tenants_root().setdefault("chat_to_tenant", {})
    except Exception:
        return {}


def _v164_tenant_id_for_root_chat(chat_id: int) -> str:
    seed = _v164_hashlib.sha256(f"chat:{int(chat_id)}".encode("utf-8")).hexdigest()[:12]
    return f"chat_{seed}"


def _v164_meta_raw(chat_id: int) -> dict | None:
    try:
        row = (_v164_root().get("chat_meta") or {}).get(str(int(chat_id)))
        return row if isinstance(row, dict) else None
    except Exception:
        return None


def _v164_set_meta(chat_id: int, circle: int, parent_first_chat_id: int = 0, source: str = "", tenant_id: str = "") -> dict:
    cid = int(chat_id)
    circle = int(circle)
    parent = int(parent_first_chat_id or 0)
    if circle != 2:
        parent = 0
    root = _v164_root()
    meta = root.setdefault("chat_meta", {}).setdefault(str(cid), {})
    old_circle = int(meta.get("circle") or -1)
    old_parent = int(meta.get("parent_first_chat_id") or 0)
    meta.update({
        "chat_id": cid,
        "circle": circle,
        "parent_first_chat_id": parent,
        "tenant_id": str(tenant_id or meta.get("tenant_id") or ""),
        "source": str(source or meta.get("source") or ("owner" if circle == 0 else "direct")),
        "updated_at": _v164_now(),
    })
    meta.setdefault("created_at", _v164_now())
    if old_circle != circle or old_parent != parent:
        meta["classification_changed_at"] = _v164_now()
    return meta


def _v164_circle_from_legacy(chat_id: int) -> tuple[int, int, str]:
    """Infer legacy v148 relation without mutating state."""
    cid = int(chat_id)
    if cid == _v164_owner_id() and cid:
        return 0, 0, TENANT_PLATFORM_ID
    mapping = _v164_mapping()
    tid = str(mapping.get(str(cid)) or "")
    row = tenant_get(tid) if tid else None
    if row:
        root_chat = int(row.get("root_chat_id") or 0)
        if tid == TENANT_PLATFORM_ID:
            # v148 used to absorb chats touched by the platform owner into the platform tenant.
            # In v164 every non-owner chat is an isolated first-circle chat unless a link says otherwise.
            return 1, 0, tid
        if root_chat and cid != root_chat:
            return 2, root_chat, tid
        return 1, 0, tid
    return 1, 0, ""


def _v164_circle_info(chat_id: int, create: bool = False, source: str = "direct") -> dict:
    cid = int(chat_id)
    meta = _v164_meta_raw(cid)
    if meta:
        return meta
    circle, parent, tid = _v164_circle_from_legacy(cid)
    if not create:
        return {
            "chat_id": cid,
            "circle": int(circle),
            "parent_first_chat_id": int(parent or 0),
            "tenant_id": str(tid or ""),
            "source": "legacy_inferred",
        }
    return _v164_ensure_isolated_chat(cid, circle, parent, actor_user_id=0, source=source)


def circle_level_for_chat(chat_id: int) -> int:
    try:
        return int(_v164_circle_info(int(chat_id), create=False).get("circle") or 0)
    except Exception:
        return 0 if int(chat_id or 0) == _v164_owner_id() else 1


def circle_parent_for_chat(chat_id: int) -> int:
    try:
        return int(_v164_circle_info(int(chat_id), create=False).get("parent_first_chat_id") or 0)
    except Exception:
        return 0


def _v164_parent_tenant_row(parent_first_chat_id: int) -> dict:
    parent_tid = str(_v164_mapping().get(str(int(parent_first_chat_id))) or _v164_tenant_id_for_root_chat(int(parent_first_chat_id)))
    return tenant_get(parent_tid) or {}


def _v164_copy_parent_managers_to_child(child_tid: str, parent_first_chat_id: int, consuming_admin: int = 0) -> None:
    parent = _v164_parent_tenant_row(parent_first_chat_id)
    parent_owner = int(parent.get("owner_user_id") or 0)
    if parent_owner:
        try:
            tenant_set_user_role(child_tid, parent_owner, "tenant_owner", changed_by=parent_owner, save=False)
        except Exception:
            pass
    for uid, item in list((parent.get("users") or {}).items()):
        try:
            iuid = int(uid)
        except Exception:
            continue
        role = str((item or {}).get("role") or "viewer")
        if role == "tenant_owner":
            role = "tenant_owner" if iuid == parent_owner else "tenant_admin"
        if role not in {"tenant_owner", "tenant_admin", "operator", "viewer"}:
            continue
        try:
            tenant_set_user_role(child_tid, iuid, role, changed_by=parent_owner, save=False)
        except Exception:
            pass
    if consuming_admin and consuming_admin != parent_owner:
        try:
            tenant_set_user_role(child_tid, int(consuming_admin), "tenant_admin", changed_by=parent_owner or consuming_admin, save=False)
        except Exception:
            pass


def _v164_ensure_isolated_chat(chat_id: int, circle: int, parent_first_chat_id: int = 0, actor_user_id: int = 0, source: str = "direct") -> dict:
    """Ensure one Telegram chat == one tenant. This is the key isolation rule in v164."""
    global _V164_MIGRATING
    cid = int(chat_id)
    circle = int(circle)
    parent = int(parent_first_chat_id or 0)
    owner_id = _v164_owner_id()
    if cid == owner_id and owner_id:
        circle, parent = 0, 0
        tid = TENANT_PLATFORM_ID
        try:
            if callable(_V164_PREV_TENANT_BIND_CHAT):
                _V164_PREV_TENANT_BIND_CHAT(cid, tid, changed_by=int(actor_user_id or owner_id), force=True)
        except Exception:
            pass
        return _v164_set_meta(cid, 0, 0, source="owner", tenant_id=tid)

    if circle not in {1, 2}:
        circle = 1
    if circle == 2 and not parent:
        circle = 1

    tid = _v164_tenant_id_for_root_chat(cid)
    # Publish the intended hierarchy before rebinding. v148 may run its forwarding-isolation
    # cleanup inside tenant_bind_chat; it must already know that parent<->child is a valid family.
    _v164_set_meta(cid, circle, parent, source=source, tenant_id=tid)
    row = tenant_get(tid)
    if not row:
        parent_row = _v164_parent_tenant_row(parent) if circle == 2 and parent else {}
        parent_owner = int(parent_row.get("owner_user_id") or 0)
        actor = int(actor_user_id or 0)
        owner_uid = parent_owner if circle == 2 and parent_owner else (actor if actor and tenant_user_is_chat_admin(cid, actor) else 0)
        # Create a deterministic tenant. The v148 helper is safe because the deterministic id is new here.
        try:
            row_tid = tenant_create(_tenant_default_name(cid), owner_uid, cid, created_by=actor, deterministic_chat_id=cid)
            tid = str(row_tid or tid)
            row = tenant_get(tid)
        except Exception:
            row = tenant_get(tid)
    if not row:
        # Last-resort direct construction; avoids ever falling back to platform ownership.
        root = _tenants_root()
        row = _tenant_normalize(tid, {
            "name": _tenant_default_name(cid), "owner_user_id": 0, "root_chat_id": cid,
            "chat_ids": [cid], "users": {}, "settings": {}, "created_by": int(actor_user_id or 0),
            "created_at": _v164_now(), "updated_at": _v164_now(),
        })
        root.setdefault("tenants", {})[tid] = row

    # A dedicated tenant must contain only this Telegram chat.
    row["root_chat_id"] = cid
    row["chat_ids"] = [cid]
    row["updated_at"] = _v164_now()
    row.setdefault("settings", {})["circle_level"] = circle
    row["settings"]["parent_first_chat_id"] = parent if circle == 2 else 0
    row["settings"]["isolation_v164"] = True

    try:
        if callable(_V164_PREV_TENANT_BIND_CHAT):
            _V164_PREV_TENANT_BIND_CHAT(cid, tid, changed_by=int(actor_user_id or 0), force=True)
        else:
            _v164_mapping()[str(cid)] = tid
    except Exception:
        _v164_mapping()[str(cid)] = tid

    if circle == 2 and parent:
        _v164_copy_parent_managers_to_child(tid, parent, consuming_admin=int(actor_user_id or 0))

    meta = _v164_set_meta(cid, circle, parent, source=source, tenant_id=tid)
    try:
        store = get_chat_store(cid)
        settings = store.setdefault("settings", {})
        settings["tenant_id"] = tid
        settings["owner_scope_id"] = cid
        settings["circle_level"] = circle
        settings["parent_first_chat_id"] = parent if circle == 2 else 0
    except Exception:
        pass
    return meta


def _v164_known_chat_ids() -> set[int]:
    ids = set()
    try:
        for raw in (data.get("chats", {}) or {}).keys():
            ids.add(int(raw))
    except Exception:
        pass
    try:
        for raw in (_v164_mapping() or {}).keys():
            ids.add(int(raw))
    except Exception:
        pass
    try:
        for row in tenant_all() or []:
            for raw in row.get("chat_ids") or []:
                ids.add(int(raw))
    except Exception:
        pass
    if _v164_owner_id():
        ids.add(_v164_owner_id())
    return ids


def _v164_migrate_legacy_hierarchy(force: bool = False) -> bool:
    """Split v148 multi-chat tenants into isolated chat tenants while preserving parent relation."""
    global _V164_MIGRATING
    with _V164_LOCK:
        if _V164_MIGRATING:
            return False
        root = _v164_root()
        # Run again after a remote restore when new legacy mappings appear; this is intentionally idempotent.
        signature_parts = []
        try:
            for tid, row in sorted((_tenants_root().get("tenants") or {}).items()):
                signature_parts.append(f"{tid}:{int((row or {}).get('root_chat_id') or 0)}:{','.join(str(int(x)) for x in ((row or {}).get('chat_ids') or []))}")
        except Exception:
            pass
        sig = _v164_hashlib.sha256("|".join(signature_parts).encode("utf-8")).hexdigest()[:20]
        if not force and str(root.get("legacy_signature") or "") == sig and int(root.get("schema_version") or 0) == V164_CIRCLE_SCHEMA:
            return False
        _V164_MIGRATING = True
        changed = False
        try:
            owner = _v164_owner_id()
            if owner:
                _v164_ensure_isolated_chat(owner, 0, source="owner")
            tenant_rows = list((_tenants_root().get("tenants") or {}).items())
            for tid, raw_row in tenant_rows:
                row = raw_row if isinstance(raw_row, dict) else {}
                root_chat = int(row.get("root_chat_id") or 0)
                chats = []
                for raw in row.get("chat_ids") or []:
                    try:
                        chats.append(int(raw))
                    except Exception:
                        pass
                if str(tid) == str(TENANT_PLATFORM_ID):
                    for cid in list(chats):
                        if cid and cid != owner:
                            _v164_ensure_isolated_chat(cid, 1, actor_user_id=0, source="legacy_platform_split")
                            changed = True
                    continue
                if root_chat:
                    _v164_ensure_isolated_chat(root_chat, 1, actor_user_id=int(row.get("owner_user_id") or 0), source="legacy_first_circle")
                    for cid in list(chats):
                        if cid and cid != root_chat:
                            _v164_ensure_isolated_chat(cid, 2, parent_first_chat_id=root_chat, actor_user_id=int(row.get("owner_user_id") or 0), source="legacy_second_circle")
                            changed = True
            # Any historical chat not represented in the tenant table becomes an isolated first-circle chat.
            for cid in sorted(_v164_known_chat_ids()):
                if cid == owner:
                    continue
                if not _v164_meta_raw(cid):
                    circle, parent, _ = _v164_circle_from_legacy(cid)
                    _v164_ensure_isolated_chat(cid, circle, parent, actor_user_id=0, source="legacy_known_chat")
                    changed = True
            root["schema_version"] = V164_CIRCLE_SCHEMA
            root["legacy_signature"] = sig
            root["migration_runs"] = int(root.get("migration_runs") or 0) + 1
            root["last_migration_at"] = _v164_now()
            if changed:
                try:
                    save_data(data, full=True)
                    schedule_delta_backup(owner or 0, delay=0.5, reason="v164_circle_migration")
                except Exception:
                    pass
                try:
                    bot_journal("v164_circle_migration", owner or 0, f"known={len(_v164_known_chat_ids())}; changed=1")
                except Exception:
                    pass
            return changed
        finally:
            _V164_MIGRATING = False


def tenant_id_for_chat(chat_id: int | None, create: bool = False, actor_user_id: int | None = None) -> str:
    explicit = getattr(_TENANT_CONTEXT, "tenant_id", None)
    if explicit:
        return str(explicit)
    try:
        cid = int(chat_id or 0)
    except Exception:
        cid = 0
    if not cid:
        return TENANT_PLATFORM_ID if not create else ""
    if cid == _v164_owner_id() and cid:
        if create:
            _v164_ensure_isolated_chat(cid, 0, actor_user_id=int(actor_user_id or 0), source="owner")
        return TENANT_PLATFORM_ID
    tid = str(_v164_mapping().get(str(cid)) or "")
    meta = _v164_meta_raw(cid)
    if tid and tenant_get(tid):
        # Lazily repair a legacy shared tenant as soon as it is touched.
        if create:
            circle, parent, _ = _v164_circle_from_legacy(cid) if not meta else (int(meta.get("circle") or 1), int(meta.get("parent_first_chat_id") or 0), tid)
            fixed = _v164_ensure_isolated_chat(cid, circle, parent, actor_user_id=int(actor_user_id or 0), source=str((meta or {}).get("source") or "lazy_repair"))
            return str(fixed.get("tenant_id") or _v164_mapping().get(str(cid)) or tid)
        return tid
    if not create:
        return ""
    fixed = _v164_ensure_isolated_chat(cid, 1, 0, actor_user_id=int(actor_user_id or 0), source="direct")
    return str(fixed.get("tenant_id") or _v164_mapping().get(str(cid)) or "")


def tenant_note_chat_seen(msg) -> None:
    try:
        cid = int(msg.chat.id)
        uid = int(getattr(getattr(msg, "from_user", None), "id", 0) or 0)
    except Exception:
        return
    meta = _v164_meta_raw(cid)
    if not meta:
        # A plain message/start without a v164/legacy link means direct discovery => first circle.
        meta = _v164_ensure_isolated_chat(cid, 0 if cid == _v164_owner_id() else 1, actor_user_id=uid, source="direct_seen")
    else:
        meta = _v164_ensure_isolated_chat(cid, int(meta.get("circle") or 1), int(meta.get("parent_first_chat_id") or 0), actor_user_id=uid, source=str(meta.get("source") or "seen"))
    tid = str(meta.get("tenant_id") or tenant_id_for_chat(cid, create=True, actor_user_id=uid))
    row = tenant_get(tid) or {}
    if not int(row.get("owner_user_id") or 0) and uid and tenant_user_is_chat_admin(cid, uid):
        try:
            tenant_set_user_role(tid, uid, "tenant_owner", changed_by=uid, save=False)
        except Exception:
            pass
    try:
        store = get_chat_store(cid)
        settings = store.setdefault("settings", {})
        settings["tenant_id"] = tid
        settings["owner_scope_id"] = cid
        settings["circle_level"] = int(meta.get("circle") or 1)
        settings["parent_first_chat_id"] = int(meta.get("parent_first_chat_id") or 0)
    except Exception:
        pass


def _v164_circle_parent_root_for_context(chat_id: int) -> int:
    cid = int(chat_id)
    level = circle_level_for_chat(cid)
    if level == 1:
        return cid
    if level == 2:
        return circle_parent_for_chat(cid)
    return 0


def _v164_circle_children(parent_first_chat_id: int) -> list[int]:
    parent = int(parent_first_chat_id or 0)
    out = []
    for raw_cid, meta in list((_v164_root().get("chat_meta") or {}).items()):
        if not isinstance(meta, dict):
            continue
        try:
            cid = int(raw_cid)
        except Exception:
            continue
        if int(meta.get("circle") or 0) == 2 and int(meta.get("parent_first_chat_id") or 0) == parent:
            out.append(cid)
    return sorted(set(out), key=lambda x: get_chat_display_name(x).casefold())


def _v164_all_circle_ids(level: int) -> list[int]:
    _v164_migrate_legacy_hierarchy()
    out = []
    for cid in _v164_known_chat_ids():
        if cid == _v164_owner_id():
            continue
        try:
            info = _v164_circle_info(cid, create=False)
            if int(info.get("circle") or 0) == int(level):
                out.append(int(cid))
        except Exception:
            pass
    return sorted(set(out), key=lambda x: get_chat_display_name(x).casefold())


def _v164_scope_ids(level: int, context_chat_id: int | None = None) -> list[int]:
    _v164_migrate_legacy_hierarchy()
    level = 2 if int(level) == 2 else 1
    try:
        ctx = int(context_chat_id if context_chat_id is not None else current_state_chat_id() or 0)
    except Exception:
        ctx = 0
    if ctx == _v164_owner_id() and ctx:
        return _v164_all_circle_ids(level)
    root_first = _v164_circle_parent_root_for_context(ctx) if ctx else 0
    if not root_first:
        return []
    if level == 1:
        return [root_first]
    return _v164_circle_children(root_first)


def _v164_actor_manages_parent(user_id: int, parent_first_chat_id: int) -> bool:
    parent_tid = str(_v164_mapping().get(str(int(parent_first_chat_id))) or "")
    if not parent_tid:
        return False
    try:
        role = tenant_role_for_user(int(user_id), tenant_id=parent_tid)
        return role in {"platform_owner", "tenant_owner", "tenant_admin"}
    except Exception:
        return False


def tenant_can_manage(user_id: int | None, tenant_id: str | None = None, chat_id: int | None = None, owner_only: bool = False) -> bool:
    try:
        uid = int(user_id or 0)
    except Exception:
        uid = 0
    if tenant_is_platform_owner_user(uid):
        return True
    # Preserve direct tenant membership first.
    try:
        if callable(_V164_PREV_TENANT_CAN_MANAGE) and _V164_PREV_TENANT_CAN_MANAGE(uid, tenant_id, chat_id, owner_only):
            return True
    except Exception:
        pass
    target_chat = 0
    if chat_id:
        try: target_chat = int(chat_id)
        except Exception: target_chat = 0
    if not target_chat and tenant_id:
        try:
            row = tenant_get(str(tenant_id)) or {}
            target_chat = int(row.get("root_chat_id") or 0)
        except Exception:
            target_chat = 0
    if target_chat and circle_level_for_chat(target_chat) == 2:
        parent = circle_parent_for_chat(target_chat)
        return bool(parent and _v164_actor_manages_parent(uid, parent))
    return False


def tenant_same_space(chat_a: int, chat_b: int) -> bool:
    """Isolation is storage-level. Explicit forwarding is allowed inside a first-circle family.

    Platform-owner UI may intentionally connect isolated chats; add_forward_link performs that authorization.
    """
    try:
        a, b = int(chat_a), int(chat_b)
    except Exception:
        return False
    if a == b:
        return True
    ma, mb = _v164_circle_info(a, False), _v164_circle_info(b, False)
    la, lb = int(ma.get("circle") or 0), int(mb.get("circle") or 0)
    pa = a if la == 1 else int(ma.get("parent_first_chat_id") or 0)
    pb = b if lb == 1 else int(mb.get("parent_first_chat_id") or 0)
    if pa and pb and pa == pb:
        return True
    # Platform-owner-created cross-family forwarding links are explicit exceptions.
    pair_key = f"{min(a, b)}:{max(a, b)}"
    if pair_key in (_v164_root().get("global_forward_pairs") or {}):
        return True
    # Same isolated tenant remains valid.
    ta, tb = str(_v164_mapping().get(str(a)) or ""), str(_v164_mapping().get(str(b)) or "")
    return bool(ta and ta == tb)


def add_forward_link(src_chat_id: int, dst_chat_id: int, mode: str):
    src, dst = int(src_chat_id), int(dst_chat_id)
    actor = 0
    try:
        actor = int(tenant_current_actor_user_id() or 0)
    except Exception:
        pass
    if not tenant_same_space(src, dst):
        if not tenant_is_platform_owner_user(actor):
            raise PermissionError("Можно связывать только свой 1-й круг и его 2-й круг")
        # Owner is explicitly authorizing a cross-family forwarding pair. Persist that exception so
        # the v148 background isolation cleanup does not remove it later.
        key = f"{min(src, dst)}:{max(src, dst)}"
        _v164_root().setdefault("global_forward_pairs", {})[key] = {
            "src": src, "dst": dst, "created_by": actor, "created_at": _v164_now(),
        }
        try: save_data(data, root_only=True)
        except Exception: pass
    # v148's wrapper would reject cross-isolated tenants, so call the pre-v148 implementation if available.
    base = globals().get("_V148_ORIG_ADD_FORWARD_LINK")
    if callable(base):
        return base(src, dst, mode)
    if callable(_V164_PREV_ADD_FORWARD_LINK):
        return _V164_PREV_ADD_FORWARD_LINK(src, dst, mode)
    raise RuntimeError("add_forward_link is unavailable")


def tenant_create_invite(tenant_id: str, kind: str, role: str, created_by: int, max_uses: int = 1, ttl_hours: int = 72) -> str:
    kind = "chat" if str(kind) == "chat" else "user"
    tenant = tenant_get(str(tenant_id)) or {}
    try:
        context_chat = int(current_state_chat_id() or tenant.get("root_chat_id") or 0)
    except Exception:
        context_chat = int(tenant.get("root_chat_id") or 0)
    tenant_root_chat = int(tenant.get("root_chat_id") or 0)
    if kind == "chat" and tenant_root_chat and circle_level_for_chat(tenant_root_chat) == 1:
        root_first = tenant_root_chat
    else:
        root_first = _v164_circle_parent_root_for_context(context_chat) if kind == "chat" else 0
    if kind == "chat" and (not root_first or circle_level_for_chat(root_first) != 1):
        raise PermissionError("Ссылку 2-го круга нужно создавать из чата 1-го круга")
    payload = _V164_PREV_TENANT_CREATE_INVITE(tenant_id, kind, role, created_by, max_uses=max_uses, ttl_hours=ttl_hours)
    if kind != "chat":
        return payload
    row = (_tenants_root().get("invite_tokens") or {}).get(_tenant_token_hash(payload))
    if isinstance(row, dict):
        row["circle_parent_chat_id"] = int(root_first)
        row["circle_parent_tenant_id"] = str(_v164_mapping().get(str(root_first)) or "")
        row["circle_schema"] = V164_CIRCLE_SCHEMA
        row["created_from_chat_id"] = int(context_chat or root_first)
        row["created_at"] = _v164_now()
        try: save_data(data, root_only=True)
        except Exception: pass
    return payload


def tenant_consume_invite(payload: str, user_id: int, chat_id: int, chat_type: str = "") -> tuple[bool, str, str]:
    key = _tenant_token_hash(str(payload or "").strip())
    token = (_tenants_root().get("invite_tokens") or {}).get(key)
    if not isinstance(token, dict) or str(token.get("kind") or "user") != "chat":
        return _V164_PREV_TENANT_CONSUME_INVITE(payload, user_id, chat_id, chat_type)
    if token.get("revoked") or float(token.get("expires_ts") or 0) < _v164_time.time() or int(token.get("uses") or 0) >= int(token.get("max_uses") or 1):
        return False, "Срок действия ссылки закончился.", ""
    cid, uid = int(chat_id), int(user_id or 0)
    if str(chat_type or "") == "private" or cid > 0:
        return False, "Эту ссылку нужно использовать при добавлении бота в группу/канал.", ""
    if not tenant_user_is_chat_admin(cid, uid):
        return False, "Привязать чат может только его администратор.", ""
    parent = int(token.get("circle_parent_chat_id") or 0)
    if not parent:
        legacy_tid = str(token.get("tenant_id") or "")
        legacy_parent = tenant_get(legacy_tid) or {}
        parent = int(legacy_parent.get("root_chat_id") or 0)
    if not parent or circle_level_for_chat(parent) != 1:
        return False, "Ссылка не привязана к чату 1-го круга. Создайте новую ссылку в меню пространства.", ""
    parent_tid = str(_v164_mapping().get(str(parent)) or tenant_id_for_chat(parent, create=True, actor_user_id=uid))
    if not tenant_can_manage(int(token.get("created_by") or uid), parent_tid, parent):
        # Do not trust stale/imported link rows that no longer belong to a manager.
        return False, "Эта ссылка больше не имеет права подключать чат.", ""

    meta = _v164_ensure_isolated_chat(cid, 2, parent_first_chat_id=parent, actor_user_id=uid, source="first_circle_link")
    child_tid = str(meta.get("tenant_id") or tenant_id_for_chat(cid, create=True, actor_user_id=uid))
    _v164_copy_parent_managers_to_child(child_tid, parent, consuming_admin=uid)
    token["uses"] = int(token.get("uses") or 0) + 1
    token["last_used_at"] = _v164_now()
    token["last_used_by"] = uid
    token["child_chat_id"] = cid
    token["child_tenant_id"] = child_tid
    try:
        save_data(data, full=True)
        schedule_delta_backup(parent, delay=0.5, reason="v164_second_circle_join")
    except Exception:
        pass
    try:
        bot_journal("v164_second_circle_join", cid, f"parent={parent}; tenant={child_tid}; by={uid}")
    except Exception:
        pass
    return True, f"✅ Чат подключён как 2-й круг к «{get_chat_display_name(parent)}».\nДанные и настройки этого чата изолированы.", child_tid


# ---------------------------------------------------------------------------
# Simple space menu
# ---------------------------------------------------------------------------
def _v164_circle_label(cid: int, include_parent: bool = False) -> str:
    title = get_chat_display_name(int(cid)) or f"Чат {int(cid)}"
    if include_parent and circle_level_for_chat(cid) == 2:
        parent = circle_parent_for_chat(cid)
        return f"{title} ← {get_chat_display_name(parent)}"
    return title


def tenant_dashboard_text(chat_id: int, user_id: int) -> str:
    _v164_migrate_legacy_hierarchy()
    cid, uid = int(chat_id), int(user_id or 0)
    level = circle_level_for_chat(cid)
    if cid == _v164_owner_id():
        first, second = _v164_all_circle_ids(1), _v164_all_circle_ids(2)
        return (
            "🏠 ПРОСТРАНСТВО ВЛАДЕЛЬЦА\n\n"
            "Здесь находится только ваш собственный контур.\n"
            "Чаты 1-го и 2-го круга имеют отдельные пространства и не смешиваются с ним.\n\n"
            f"1️⃣ Первый круг: {len(first)}\n"
            f"2️⃣ Второй круг: {len(second)}"
        )
    root_first = _v164_circle_parent_root_for_context(cid)
    if level == 1:
        children = _v164_circle_children(cid)
        return (
            "1️⃣ ПРОСТРАНСТВО ПЕРВОГО КРУГА\n\n"
            f"Чат: {get_chat_display_name(cid)}\n"
            f"2-й круг: {len(children)} чат(ов)\n\n"
            "Финансы, настройки, напоминания и Google этого пространства изолированы от владельца бота.\n"
            "По ссылке из этого меню можно подключать только свой 2-й круг."
        )
    parent = circle_parent_for_chat(cid)
    return (
        "2️⃣ ПРОСТРАНСТВО ВТОРОГО КРУГА\n\n"
        f"Чат: {get_chat_display_name(cid)}\n"
        f"Родитель 1-го круга: {get_chat_display_name(parent) if parent else 'не определён'}\n\n"
        "У этого чата собственное изолированное пространство. Он не становится частью пространства владельца бота."
    )


def tenant_dashboard_keyboard(chat_id: int, user_id: int):
    cid, uid = int(chat_id), int(user_id or 0)
    kb = types.InlineKeyboardMarkup(row_width=1)
    level = circle_level_for_chat(cid)
    if cid == _v164_owner_id():
        kb.row(IB(f"1️⃣ Первый круг · {len(_v164_all_circle_ids(1))}", callback_data="v164:space_circle:1"))
        kb.row(IB(f"2️⃣ Второй круг · {len(_v164_all_circle_ids(2))}", callback_data="v164:space_circle:2"))
    elif level == 1:
        if tenant_can_manage(uid, chat_id=cid):
            kb.row(IB(f"2️⃣ Второй круг · {len(_v164_circle_children(cid))}", callback_data="v164:space_circle:2"))
            kb.row(IB("🔗 Подключить чат 2-го круга", callback_data=f"sp:chatlink:{tenant_id_for_chat(cid, create=True, actor_user_id=uid)}"))
            kb.row(IB("👥 Пользователи", callback_data=f"sp:users:{tenant_id_for_chat(cid, create=True, actor_user_id=uid)}"))
    else:
        tid = tenant_id_for_chat(cid, create=True, actor_user_id=uid)
        if tenant_can_manage(uid, tid, cid):
            kb.row(IB("👥 Пользователи", callback_data=f"sp:users:{tid}"))
    kb.row(IB("❌ Закрыть", callback_data="info_close"))
    return kb


def _v164_space_list_text(context_chat_id: int, level: int) -> str:
    ids = _v164_scope_ids(level, context_chat_id)
    title = "1️⃣ ПЕРВЫЙ КРУГ" if int(level) == 1 else "2️⃣ ВТОРОЙ КРУГ"
    lines = [title, ""]
    if not ids:
        lines.append("Нет подключённых чатов.")
    else:
        for cid in ids:
            if int(level) == 2:
                lines.append(f"• {_v164_circle_label(cid, include_parent=True)}")
            else:
                lines.append(f"• {_v164_circle_label(cid)}")
    lines += ["", "Каждый чат хранит собственные настройки и данные."]
    return "\n".join(lines)[:3900]


def _v164_space_list_keyboard(context_chat_id: int, user_id: int, level: int):
    kb = types.InlineKeyboardMarkup(row_width=1)
    ids = _v164_scope_ids(level, context_chat_id)
    for cid in ids[:60]:
        tid = tenant_id_for_chat(cid, create=True, actor_user_id=user_id)
        kb.row(IB(_v164_circle_label(cid, include_parent=(int(level) == 2)), callback_data=f"sp:open:{tid}"))
    if int(level) == 2 and circle_level_for_chat(context_chat_id) == 1 and tenant_can_manage(user_id, chat_id=context_chat_id):
        tid = tenant_id_for_chat(context_chat_id, create=True, actor_user_id=user_id)
        kb.row(IB("🔗 Подключить чат 2-го круга", callback_data=f"sp:chatlink:{tid}"))
    kb.row(IB("🔙 Назад", callback_data="sp:dashboard:x"))
    return kb


def tenant_detail_text(tenant_id: str, viewer_user_id: int) -> str:
    row = tenant_get(tenant_id) or {}
    cid = int(row.get("root_chat_id") or 0)
    if not cid:
        return "❌ Пространство недоступно."
    level = circle_level_for_chat(cid)
    lines = [
        ("1️⃣ ПЕРВЫЙ КРУГ" if level == 1 else ("2️⃣ ВТОРОЙ КРУГ" if level == 2 else "🏠 ПРОСТРАНСТВО ВЛАДЕЛЬЦА")),
        "",
        f"Чат: {get_chat_display_name(cid)}",
        f"ID: {cid}",
        f"Владелец пространства: {security_user_display(int(row.get('owner_user_id') or 0)) if int(row.get('owner_user_id') or 0) else 'не назначен'}",
        "Изоляция: ✅ отдельные данные/настройки",
    ]
    if level == 1:
        lines.append(f"2-й круг: {len(_v164_circle_children(cid))}")
    elif level == 2:
        parent = circle_parent_for_chat(cid)
        lines.append(f"Родитель 1-го круга: {get_chat_display_name(parent) if parent else 'не определён'}")
    return "\n".join(lines)[:3900]


def tenant_chats_text(tenant_id: str) -> str:
    row = tenant_get(tenant_id) or {}
    cid = int(row.get("root_chat_id") or 0)
    if not cid:
        return "💬 ЧАТЫ\n\nНет чатов."
    # v164 guarantees one Telegram chat per tenant.
    return f"💬 ЧАТ ПРОСТРАНСТВА\n\n• {get_chat_display_name(cid)} · {cid}\n\nДругие круги сюда не смешиваются."


def tenant_visible_spaces(user_id: int) -> list[dict]:
    """Keep legacy APIs safe, but do not use this as the v164 owner dashboard.

    The platform owner can still administer all isolated tenants. Non-owner users see direct memberships.
    """
    if tenant_is_platform_owner_user(user_id):
        return tenant_all()
    return tenant_user_spaces(user_id)


def tenant_handle_callback(call, data_str: str) -> bool:
    raw = str(data_str or "")
    if not raw.startswith("sp:"):
        return False
    cid = int(call.message.chat.id)
    uid = int(getattr(call.from_user, "id", 0) or 0)
    parts = raw.split(":")
    action = parts[1] if len(parts) > 1 else ""
    tid = parts[2] if len(parts) > 2 and parts[2] not in {"x", ""} else tenant_id_for_chat(cid, create=True, actor_user_id=uid)
    if action in {"dashboard", "list"}:
        safe_edit(bot, call, tenant_dashboard_text(cid, uid), reply_markup=tenant_dashboard_keyboard(cid, uid))
        return True
    row = tenant_get(tid) or {}
    target_chat = int(row.get("root_chat_id") or 0)
    if not target_chat:
        bot.answer_callback_query(call.id, "Пространство недоступно.", show_alert=True)
        return True
    if not (tenant_is_platform_owner_user(uid) or tenant_can_manage(uid, tid, target_chat) or tenant_role_for_user(uid, tenant_id=tid) in {"operator", "viewer"}):
        bot.answer_callback_query(call.id, "Пространство недоступно.", show_alert=True)
        return True
    if action == "open":
        kb = types.InlineKeyboardMarkup(row_width=1)
        level = circle_level_for_chat(target_chat)
        if tenant_can_manage(uid, tid, target_chat):
            if level == 1:
                kb.row(IB(f"2️⃣ Второй круг · {len(_v164_circle_children(target_chat))}", callback_data="v164:space_circle:2"))
                kb.row(IB("🔗 Подключить чат 2-го круга", callback_data=f"sp:chatlink:{tid}"))
            kb.row(IB("👥 Пользователи", callback_data=f"sp:users:{tid}"))
        kb.row(IB("🔙 Назад", callback_data="sp:dashboard:x"))
        safe_edit(bot, call, tenant_detail_text(tid, uid), reply_markup=kb)
        return True
    if action == "chats":
        kb = types.InlineKeyboardMarkup(); kb.row(IB("🔙 Назад", callback_data=f"sp:open:{tid}"))
        safe_edit(bot, call, tenant_chats_text(tid), reply_markup=kb)
        return True
    if action == "users":
        kb = types.InlineKeyboardMarkup(); kb.row(IB("🔙 Назад", callback_data=f"sp:open:{tid}"))
        safe_edit(bot, call, tenant_users_text(tid), reply_markup=kb)
        return True
    if action == "chatlink":
        if circle_level_for_chat(target_chat) != 1 or not tenant_can_manage(uid, tid, target_chat):
            bot.answer_callback_query(call.id, "Ссылку 2-го круга создаёт только управляющий чата 1-го круга.", show_alert=True)
            return True
        try:
            payload = tenant_create_invite(tid, "chat", "tenant_admin", uid, max_uses=1, ttl_hours=72)
        except Exception as exc:
            bot.answer_callback_query(call.id, str(exc)[:180], show_alert=True)
            return True
        text = (
            "🔗 ПОДКЛЮЧЕНИЕ 2-ГО КРУГА\n\n"
            + tenant_invite_link(payload)
            + f"\n\nКод: {payload}"
            + "\n\nДобавленный по этой ссылке чат получит собственное изолированное пространство и будет привязан к этому 1-му кругу."
        )
        kb = types.InlineKeyboardMarkup(); kb.row(IB("🔙 Назад", callback_data=f"sp:open:{tid}"))
        safe_edit(bot, call, text, reply_markup=kb)
        return True
    if action == "userlink":
        if not tenant_can_manage(uid, tid, target_chat):
            bot.answer_callback_query(call.id, "Недостаточно прав.", show_alert=True); return True
        role = parts[3] if len(parts) > 3 else "operator"
        payload = tenant_create_invite(tid, "user", role, uid, max_uses=20, ttl_hours=72)
        text = f"👤 ССЫЛКА ДЛЯ ПОЛЬЗОВАТЕЛЯ\n\n{tenant_invite_link(payload)}\n\nРоль: {TENANT_ROLE_LABELS.get(role, role)}."
        kb = types.InlineKeyboardMarkup(); kb.row(IB("🔙 Назад", callback_data=f"sp:open:{tid}"))
        safe_edit(bot, call, text, reply_markup=kb)
        return True
    return True


# ---------------------------------------------------------------------------
# Forwarding + finance menus: circle 1 by default, explicit circle 2 button.
# Per-message view state keeps parallel Telegram windows independent.
# ---------------------------------------------------------------------------
def _v164_update_context() -> dict:
    try:
        value = getattr(_TELEGRAM_UPDATE_CONTEXT, "value", None)
        return value if isinstance(value, dict) else {}
    except Exception:
        return {}


def _v164_window_key(kind: str) -> tuple[int, int, str]:
    ctx = _v164_update_context()
    try: cid = int(ctx.get("chat_id") or current_state_chat_id() or 0)
    except Exception: cid = 0
    try: mid = int(ctx.get("message_id") or 0)
    except Exception: mid = 0
    return cid, mid, str(kind)


def _v164_set_window_circle(kind: str, level: int) -> None:
    key = _v164_window_key(kind)
    with _V164_WINDOW_VIEW_LOCK:
        _V164_WINDOW_VIEW[key] = {"circle": 2 if int(level) == 2 else 1, "at": _v164_time.time()}
        # Bounded memory: UI view hints are ephemeral.
        if len(_V164_WINDOW_VIEW) > 600:
            cutoff = _v164_time.time() - 86400
            for k, row in list(_V164_WINDOW_VIEW.items()):
                if float((row or {}).get("at") or 0) < cutoff:
                    _V164_WINDOW_VIEW.pop(k, None)


def _v164_current_window_circle(kind: str, default: int = 1) -> int:
    key = _v164_window_key(kind)
    ctx = _v164_update_context()
    raw = str(ctx.get("callback_data") or "")
    # A fresh entry from the main window always starts at circle 1.
    if str(kind) == "forward" and raw.startswith("d:") and raw.endswith(":forward_menu"):
        _v164_set_window_circle(kind, 1)
        return 1
    if str(kind) == "finmode" and raw.startswith("d:") and raw.endswith(":forward_finmode_menu"):
        # v164 custom config-back never uses this callback, so this is a fresh entry/legacy back.
        _v164_set_window_circle(kind, 1)
        return 1
    with _V164_WINDOW_VIEW_LOCK:
        row = _V164_WINDOW_VIEW.get(key) or {}
    return 2 if int(row.get("circle") or default) == 2 else 1


def _v164_circle_switch_button(kind: str, level: int, selected_a: int = 0):
    other = 1 if int(level) == 2 else 2
    label = "1️⃣ 1-й круг" if other == 1 else "2️⃣ 2-й круг"
    suffix = f":{int(selected_a)}" if selected_a else ""
    return IB(label, callback_data=f"v164:circle:{kind}:{other}{suffix}")


def _v164_scoped_picker_ids(kind: str, level: int | None = None) -> list[int]:
    if level is None:
        level = _v164_current_window_circle(kind, 1)
    return _v164_scope_ids(int(level), current_state_chat_id())


def _collect_forward_picker_items(include_owner: bool = True, include_removed: bool = False):
    level = _v164_current_window_circle("forward", 1)
    items = []
    for cid in _v164_scoped_picker_ids("forward", level):
        try:
            if (not include_removed) and is_chat_bot_removed(int(cid)):
                continue
        except Exception:
            pass
        items.append((int(cid), get_chat_display_name(int(cid)) or f"Чат {cid}"))
    # OWNER_ID is intentionally not mixed into first/second-circle pickers anymore.
    return items, None


def collect_forward_pairs_for_menu() -> list[tuple[int, int]]:
    rows = _V164_PREV_COLLECT_FORWARD_PAIRS() if callable(_V164_PREV_COLLECT_FORWARD_PAIRS) else []
    allowed = set(_v164_scoped_picker_ids("forward"))
    # Keep pairs whose source belongs to the displayed circle. Target may be the paired parent/child.
    out = []
    for pair in rows or []:
        try:
            a, b = int(pair[0]), int(pair[1])
        except Exception:
            continue
        if a in allowed:
            out.append((a, b))
    return out


def _v164_insert_before_nav(kb, button) -> None:
    try:
        rows = kb.keyboard
        # Put the circle switch before description/probe/back controls.
        idx = max(0, len(rows) - 3)
        rows.insert(idx, [button])
    except Exception:
        try: kb.row(button)
        except Exception: pass


def build_forward_new_menu(day_key: str | None = None, A: int | None = None, B: int | None = None):
    level = _v164_current_window_circle("forward", circle_level_for_chat(A) if A else 1)
    kb = _V164_PREV_BUILD_FORWARD_NEW_MENU(day_key, A, B)
    if not B:
        _v164_insert_before_nav(kb, _v164_circle_switch_button("forward", level, int(A or 0)))
    return kb


def build_forward_source_menu(day_key: str | None = None):
    # Previous source builder dynamically calls the current build_forward_new_menu in new-style mode.
    if forward_menu_new_style_enabled():
        return build_forward_new_menu(day_key)
    level = _v164_current_window_circle("forward", 1)
    kb = _V164_PREV_BUILD_FORWARD_SOURCE_MENU(day_key)
    _v164_insert_before_nav(kb, _v164_circle_switch_button("forward", level))
    return kb


def build_forward_target_menu(src_id: int):
    src = int(src_id)
    if circle_level_for_chat(src) in {1, 2}:
        # If no explicit circle switch happened, target list follows source circle.
        key = _v164_window_key("forward")
        with _V164_WINDOW_VIEW_LOCK:
            if key not in _V164_WINDOW_VIEW:
                _V164_WINDOW_VIEW[key] = {"circle": circle_level_for_chat(src), "at": _v164_time.time()}
    level = _v164_current_window_circle("forward", circle_level_for_chat(src))
    kb = _V164_PREV_BUILD_FORWARD_TARGET_MENU(src)
    _v164_insert_before_nav(kb, _v164_circle_switch_button("forward", level, src))
    return kb


def build_forward_menu_text_for_current_mode(title: str | None = None, A: int | None = None, B: int | None = None) -> str:
    level = _v164_current_window_circle("forward", circle_level_for_chat(A) if A else 1)
    prefix = "1️⃣ 1-й круг" if level == 1 else "2️⃣ 2-й круг"
    if forward_menu_new_style_enabled():
        body = build_forward_new_text(A, B)
    else:
        body = build_forward_status_text(title or "Пересылка:\nВыберите чат A:")
    return f"{prefix}\n{body}"


def build_forward_menu_keyboard_for_current_mode(day_key: str | None = None, A: int | None = None, B: int | None = None):
    if forward_menu_new_style_enabled():
        return build_forward_new_menu(day_key, A, B)
    if A and B:
        return build_forward_mode_menu(A, B)
    if A:
        return build_forward_target_menu(A)
    return build_forward_source_menu(day_key)


def build_finance_toggle_chat_menu(day_key: str):
    level = _v164_current_window_circle("finmode", 1)
    kb = types.InlineKeyboardMarkup(row_width=2)
    buttons = []
    for cid in _v164_scope_ids(level, current_state_chat_id()):
        try:
            if is_chat_bot_removed(cid):
                continue
        except Exception:
            pass
        icon = finance_mode_compact_icon(cid)
        buttons.append(IB(f"{icon} {chat_button_title(cid, get_chat_display_name(cid))}", callback_data=f"d:{day_key}:fw_finmode_pick_{cid}"))
    add_buttons_in_rows(kb, buttons, 2)
    if not buttons:
        kb.row(IB("Нет чатов этого круга", callback_data="none"))
    kb.row(_v164_circle_switch_button("finmode", level))
    kb.row(IB("ℹ️ Описание чатов", callback_data="chat_desc_menu:finmode"))
    kb.row(IB("🔙 Назад", callback_data=f"d:{day_key}:back_main"))
    return kb


def build_quick_balance_mode_menu(day_key: str, target_chat_id: int):
    kb = _V164_PREV_BUILD_QUICK_BALANCE_MODE_MENU(day_key, target_chat_id)
    level = _v164_current_window_circle("finmode", circle_level_for_chat(int(target_chat_id)))
    try:
        if kb.keyboard:
            # Replace only the last "Назад к чатам" row. All financial action callbacks remain unchanged.
            last = kb.keyboard[-1]
            if last and "Назад" in str(getattr(last[0], "text", "")):
                kb.keyboard[-1] = [IB("🔙 Назад к чатам", callback_data=f"v164:finback:{level}:{day_key}")]
    except Exception:
        pass
    return kb


def build_finance_mode_config_menu(day_key: str, target_chat_id: int):
    return build_quick_balance_mode_menu(day_key, target_chat_id)


def build_chat_description_menu(viewer_chat_id: int, origin: str, day_key: str):
    # For forwarding/finmode descriptions, use exactly the circle currently displayed.
    if str(origin) not in {"forward", "finmode"}:
        return _V164_PREV_BUILD_CHAT_DESCRIPTION_MENU(viewer_chat_id, origin, day_key)
    kind = "finmode" if str(origin) == "finmode" else "forward"
    level = _v164_current_window_circle(kind, 1)
    kb = types.InlineKeyboardMarkup(row_width=2)
    buttons = []
    for cid in _v164_scope_ids(level, viewer_chat_id):
        try:
            if is_chat_bot_removed(int(cid)):
                continue
        except Exception:
            pass
        buttons.append(IB(chat_button_title(int(cid)), callback_data=f"chat_desc_open:{origin}:{int(cid)}"))
    add_buttons_in_rows(kb, buttons, 2)
    kb.row(IB("🔙 Назад", callback_data=_chat_description_origin_back(origin, day_key)))
    kb.row(IB("⬅️ Назад осн. окно", callback_data=f"d:{day_key}:back_main"))
    return kb


def _v164_circle_callback_filter(call) -> bool:
    try:
        raw = str(getattr(call, "data", "") or "")
        resolver = globals().get("resolve_short_callback")
        if callable(resolver):
            raw = str(resolver(raw) or raw)
        return raw.startswith("v164:")
    except Exception:
        return False


def _v164_current_day_for_ui(chat_id: int) -> str:
    try:
        return str(get_chat_store(int(chat_id)).get("current_view_day") or today_key())
    except Exception:
        return today_key()


def _v164_circle_callback(call):
    raw = str(getattr(call, "data", "") or "")
    try:
        resolver = globals().get("resolve_short_callback")
        if callable(resolver): raw = str(resolver(raw) or raw)
    except Exception:
        pass
    cid = int(call.message.chat.id)
    uid = int(getattr(call.from_user, "id", 0) or 0)
    parts = raw.split(":")
    try:
        if raw.startswith("v164:circle:") and len(parts) >= 4:
            kind = str(parts[2])
            level = 2 if int(parts[3]) == 2 else 1
            selected_a = int(parts[4]) if len(parts) > 4 and str(parts[4]).lstrip("-").isdigit() else 0
            if kind not in {"forward", "finmode"}:
                return
            # Owner can browse globally from owner chat. A first-circle manager browses only its family.
            if cid != _v164_owner_id() and not tenant_can_manage(uid, chat_id=_v164_circle_parent_root_for_context(cid) or cid):
                bot.answer_callback_query(call.id, "Недостаточно прав.", show_alert=True); return
            _v164_set_window_circle(kind, level)
            day = _v164_current_day_for_ui(cid)
            if kind == "forward":
                if selected_a:
                    kb = build_forward_new_menu(day, selected_a) if forward_menu_new_style_enabled() else build_forward_target_menu(selected_a)
                    text = build_forward_menu_text_for_current_mode(f"Источник: {get_chat_display_name(selected_a)}\nВыберите чат B:", A=selected_a)
                else:
                    kb = build_forward_menu_keyboard_for_current_mode(day)
                    text = build_forward_menu_text_for_current_mode("Пересылка:\nВыберите чат A:")
                safe_edit(bot, call, text, reply_markup=kb)
            else:
                safe_edit(bot, call, "💰 Фин режим / В24\n" + ("1️⃣ Первый круг" if level == 1 else "2️⃣ Второй круг") + "\nВыберите чат.", reply_markup=build_finance_toggle_chat_menu(day))
            return
        if raw.startswith("v164:finback:") and len(parts) >= 4:
            level = 2 if int(parts[2]) == 2 else 1
            day = str(parts[3] or _v164_current_day_for_ui(cid))
            _v164_set_window_circle("finmode", level)
            safe_edit(bot, call, "💰 Фин режим / В24\n" + ("1️⃣ Первый круг" if level == 1 else "2️⃣ Второй круг") + "\nВыберите чат.", reply_markup=build_finance_toggle_chat_menu(day))
            return
        if raw.startswith("v164:space_circle:") and len(parts) >= 3:
            level = 2 if int(parts[2]) == 2 else 1
            # Owner-private sees global circle inventory. Other chats see only their family.
            if cid != _v164_owner_id() and not (tenant_can_manage(uid, chat_id=cid) or circle_level_for_chat(cid) == 2):
                bot.answer_callback_query(call.id, "Недостаточно прав.", show_alert=True); return
            safe_edit(bot, call, _v164_space_list_text(cid, level), reply_markup=_v164_space_list_keyboard(cid, uid, level))
            return
    except Exception as exc:
        try: log_error(f"v164 circle callback {raw}: {exc}")
        except Exception: pass
        try: bot.answer_callback_query(call.id, "Не удалось открыть круг.", show_alert=True)
        except Exception: pass


def _v164_install_callback_handler() -> int:
    try:
        bot.callback_query_handler(func=_v164_circle_callback_filter)(_v164_circle_callback)
        handlers = getattr(bot, "callback_query_handlers", None)
        if isinstance(handlers, list) and handlers:
            row = handlers.pop()
            handlers.insert(0, row)
        return 1
    except Exception:
        return 0


# A direct command gets a clear answer instead of an uncaught PermissionError from the old handler.
def _v164_space_chat_link_handler(msg):
    try:
        uid, cid = int(getattr(getattr(msg, "from_user", None), "id", 0) or 0), int(msg.chat.id)
        tenant_note_chat_seen(msg)
        if circle_level_for_chat(cid) != 1:
            bot.send_message(cid, "❌ Подключать 2-й круг можно только из чата 1-го круга.")
            return
        tid = tenant_id_for_chat(cid, create=True, actor_user_id=uid)
        if not tenant_can_manage(uid, tid, cid):
            bot.send_message(cid, "❌ Недостаточно прав.")
            return
        payload = tenant_create_invite(tid, "chat", "tenant_admin", uid, max_uses=1, ttl_hours=72)
        bot.send_message(cid, "🔗 Ссылка для подключения чата 2-го круга (72 часа):\n" + tenant_invite_link(payload) + f"\n\nКод: {payload}")
        try: schedule_command_delete(msg)
        except Exception: pass
    except Exception as exc:
        try: bot.send_message(msg.chat.id, f"❌ Не удалось создать ссылку: {str(exc)[:240]}")
        except Exception: pass


def _v164_install_space_command_handler() -> int:
    try:
        bot.message_handler(commands=["space_chat_link", "tenant_chat_link"])(_v164_space_chat_link_handler)
        handlers = getattr(bot, "message_handlers", None)
        if isinstance(handlers, list) and handlers:
            row = handlers.pop()
            handlers.insert(0, row)
        return 1
    except Exception:
        return 0


# Register marker aliases for the circle switches without inventing a new logical window marker.
try:
    WINDOW_MARKER_CONSTANTS.update({
        "v164:circle:forward:*": "Ф53",
        "v164:circle:finmode:*": "Ф52",
        "v164:finback:*": "Ф52",
        "v164:space_circle:*": "Ф239",
    })
except Exception:
    pass


# Restore compatibility with v164 snapshots.
if callable(_V164_PREV_RESTORE_VALIDATE):
    def _v153_validate_restore_gz(gz_path: str):
        try:
            return _V164_PREV_RESTORE_VALIDATE(gz_path)
        except Exception as exc:
            if "unsupported bot version" not in str(exc):
                raise
            folder = _v164_tempfile.mkdtemp(prefix="v164_restore_validate_")
            raw = _v164_os.path.join(folder, "restore.sqlite3")
            try:
                with _v164_gzip.open(gz_path, "rb") as fin, open(raw, "wb") as fout:
                    _v164_shutil.copyfileobj(fin, fout, 1024 * 1024)
                conn = _v164_sqlite3.connect(raw)
                try:
                    integrity = str(conn.execute("PRAGMA integrity_check").fetchone()[0])
                    if integrity.lower() != "ok": raise RuntimeError(f"SQLite integrity_check: {integrity}")
                    row = conn.execute("SELECT v FROM meta WHERE kind='v153_export' AND k='manifest'").fetchone()
                    if not row: raise RuntimeError("manifest v153 not found")
                    manifest = _v164_json.loads(row[0])
                finally:
                    conn.close()
                if str(manifest.get("kind")) != "telegram_bot_full_state_v153": raise RuntimeError("unknown export kind")
                if int(manifest.get("schema_version") or 0) != int(V153_EXPORT_SCHEMA): raise RuntimeError("unsupported export schema")
                export_version = str(manifest.get("bot_version") or "")
                if not export_version.startswith((
                    "bot_v153_", "bot_v154_", "bot_v155_", "bot_v156_", "bot_v157_", "bot_v158_",
                    "bot_v159_", "bot_v160_", "bot_v161_", "bot_v162_", "bot_v163_", "bot_v164_",
                )):
                    raise RuntimeError(f"unsupported bot version: {export_version or 'missing'}")
                if _v153_db_logical_checksum(raw) != str(manifest.get("checksum") or ""):
                    raise RuntimeError("checksum mismatch")
                return manifest, raw
            except Exception:
                _v164_shutil.rmtree(folder, ignore_errors=True)
                raise


_V164_CALLBACK_HANDLER = _v164_install_callback_handler()
_V164_SPACE_COMMAND_HANDLER = _v164_install_space_command_handler()

# Do not force a save during module import. A lazy migration after runtime restore is safer on Render.
try:
    bot_journal(
        "v164_circle_hierarchy_installed",
        _v164_owner_id(),
        "owner=isolated; direct=first_circle; invite=second_circle_isolated; owner_menus=circles; "
        f"callbacks={_V164_CALLBACK_HANDLER}; command={_V164_SPACE_COMMAND_HANDLER}",
    )
except Exception:
    pass

# v164_circle_hierarchy_spaces
