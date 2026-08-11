# v178_global_performance_final
"""v165: restore owner row in Forwarding and Finance-mode first-circle pickers.

The owner is visible in the same menus as in v163 and earlier, but remains circle 0 / platform tenant.
Circle 1 continues to mean ordinary direct-connected chats with their own isolated settings.
Circle 2 remains a separate picker and never receives the owner row.
"""

import gzip as _v165_gzip
import json as _v165_json
import os as _v165_os
import shutil as _v165_shutil
import sqlite3 as _v165_sqlite3
import tempfile as _v165_tempfile

VERSION = "bot_v165_owner_first_circle_compat"

_V165_PREV_RESTORE_VALIDATE = globals().get("_v153_validate_restore_gz")


def _v165_is_platform_owner_context() -> bool:
    try:
        return int(current_state_chat_id() or 0) == int(OWNER_ID or 0) and int(OWNER_ID or 0) != 0
    except Exception:
        return False


def _v165_owner_item(include_removed: bool = False):
    try:
        oid = int(OWNER_ID or 0)
    except Exception:
        oid = 0
    if not oid:
        return None
    if not include_removed:
        try:
            # The private owner chat is retained even if an old lifecycle flag was stale.
            if is_chat_bot_removed(oid) and not _v165_is_platform_owner_context():
                return None
        except Exception:
            pass
    return oid, (get_chat_display_name(oid) or f"Чат {oid}")


def _collect_forward_picker_items(include_owner: bool = True, include_removed: bool = False):
    """v165: v163-compatible owner row + v164 circle-scoped ordinary chats."""
    level = _v164_current_window_circle("forward", 1)
    items = []
    owner_item = None
    for cid in _v164_scope_ids(level, current_state_chat_id()):
        try:
            icid = int(cid)
        except Exception:
            continue
        try:
            if (not include_removed) and is_chat_bot_removed(icid):
                continue
        except Exception:
            pass
        items.append((icid, get_chat_display_name(icid) or f"Чат {icid}"))

    # Compatibility rule: only the platform owner, while looking at the 1st-circle page,
    # sees the owner chat as the dedicated owner row. This does not reclassify the owner as circle 1.
    if include_owner and int(level) == 1 and _v165_is_platform_owner_context():
        owner_item = _v165_owner_item(include_removed=include_removed)

    # Defensive de-duplication in case legacy state accidentally exposed OWNER_ID in circle ids.
    if owner_item:
        items = [(cid, title) for cid, title in items if int(cid) != int(owner_item[0])]
    items.sort(key=lambda row: (str(row[1]).casefold(), int(row[0])))
    return items, owner_item


def _v177_legacy_0192_collect_forward_pairs_for_menu() -> list[tuple[int, int]]:
    """Show historical owner pairs again on the 1st-circle page without mixing tenant storage."""
    try:
        rows = _V164_PREV_COLLECT_FORWARD_PAIRS() if callable(_V164_PREV_COLLECT_FORWARD_PAIRS) else []
    except Exception:
        rows = []
    level = _v164_current_window_circle("forward", 1)
    allowed = set(int(x) for x in (_v164_scope_ids(level, current_state_chat_id()) or []))
    if int(level) == 1 and _v165_is_platform_owner_context():
        try:
            allowed.add(int(OWNER_ID))
        except Exception:
            pass
    out = []
    for pair in rows or []:
        try:
            a, b = int(pair[0]), int(pair[1])
        except Exception:
            continue
        if a in allowed:
            out.append((a, b))
    return out
try: _v177_legacy_0192_collect_forward_pairs_for_menu.__name__ = 'collect_forward_pairs_for_menu'
except Exception: pass
collect_forward_pairs_for_menu = _v177_legacy_0192_collect_forward_pairs_for_menu


def build_finance_toggle_chat_menu(day_key: str):
    """Finance-mode picker: owner + ordinary first circle, or second circle only."""
    level = _v164_current_window_circle("finmode", 1)
    kb = types.InlineKeyboardMarkup(row_width=2)
    buttons = []

    # Restore the owner's own row/settings exactly through the existing finance state helpers.
    if int(level) == 1 and _v165_is_platform_owner_context():
        owner_item = _v165_owner_item(include_removed=True)
        if owner_item:
            oid, title = owner_item
            icon = finance_mode_compact_icon(oid)
            kb.row(IB(
                f"{icon} {chat_button_title(oid, title)}",
                callback_data=f"d:{day_key}:fw_finmode_pick_{oid}",
            ))

    for cid in _v164_scope_ids(level, current_state_chat_id()):
        try:
            cid = int(cid)
        except Exception:
            continue
        if OWNER_ID and str(cid) == str(OWNER_ID):
            continue
        try:
            if is_chat_bot_removed(cid):
                continue
        except Exception:
            pass
        icon = finance_mode_compact_icon(cid)
        buttons.append(IB(
            f"{icon} {chat_button_title(cid, get_chat_display_name(cid))}",
            callback_data=f"d:{day_key}:fw_finmode_pick_{cid}",
        ))
    add_buttons_in_rows(kb, buttons, 2)

    if not buttons and not (int(level) == 1 and _v165_is_platform_owner_context()):
        kb.row(IB("Нет чатов этого круга", callback_data="none"))
    kb.row(_v164_circle_switch_button("finmode", level))
    kb.row(IB("ℹ️ Описание чатов", callback_data="chat_desc_menu:finmode"))
    kb.row(IB("🔙 Назад", callback_data=f"d:{day_key}:back_main"))
    return kb


def build_chat_description_menu(viewer_chat_id: int, origin: str, day_key: str):
    """Description picker mirrors the visible owner/first/second-circle selection."""
    if str(origin) not in {"forward", "finmode"}:
        return _V164_PREV_BUILD_CHAT_DESCRIPTION_MENU(viewer_chat_id, origin, day_key)
    kind = "finmode" if str(origin) == "finmode" else "forward"
    level = _v164_current_window_circle(kind, 1)
    kb = types.InlineKeyboardMarkup(row_width=2)
    buttons = []

    try:
        viewer_is_owner = int(viewer_chat_id or 0) == int(OWNER_ID or 0) and int(OWNER_ID or 0) != 0
    except Exception:
        viewer_is_owner = False
    if int(level) == 1 and viewer_is_owner:
        owner_item = _v165_owner_item(include_removed=True)
        if owner_item:
            kb.row(IB(chat_button_title(owner_item[0], owner_item[1]), callback_data=f"chat_desc_open:{origin}:{owner_item[0]}"))

    for cid in _v164_scope_ids(level, viewer_chat_id):
        try:
            cid = int(cid)
        except Exception:
            continue
        if OWNER_ID and str(cid) == str(OWNER_ID):
            continue
        try:
            if is_chat_bot_removed(cid):
                continue
        except Exception:
            pass
        buttons.append(IB(chat_button_title(cid), callback_data=f"chat_desc_open:{origin}:{cid}"))
    add_buttons_in_rows(kb, buttons, 2)
    kb.row(IB("🔙 Назад", callback_data=_chat_description_origin_back(origin, day_key)))
    kb.row(IB("⬅️ Назад осн. окно", callback_data=f"d:{day_key}:back_main"))
    return kb


# v165 backup/restore compatibility.
if callable(_V165_PREV_RESTORE_VALIDATE):
    def _v153_validate_restore_gz(gz_path: str):
        try:
            return _V165_PREV_RESTORE_VALIDATE(gz_path)
        except Exception as exc:
            if "unsupported bot version" not in str(exc):
                raise
            folder = _v165_tempfile.mkdtemp(prefix="v165_restore_validate_")
            raw = _v165_os.path.join(folder, "restore.sqlite3")
            try:
                with _v165_gzip.open(gz_path, "rb") as fin, open(raw, "wb") as fout:
                    _v165_shutil.copyfileobj(fin, fout, 1024 * 1024)
                conn = _v165_sqlite3.connect(raw)
                try:
                    integrity = str(conn.execute("PRAGMA integrity_check").fetchone()[0])
                    if integrity.lower() != "ok":
                        raise RuntimeError(f"SQLite integrity_check: {integrity}")
                    row = conn.execute("SELECT v FROM meta WHERE kind='v153_export' AND k='manifest'").fetchone()
                    if not row:
                        raise RuntimeError("manifest v153 not found")
                    manifest = _v165_json.loads(row[0])
                finally:
                    conn.close()
                if str(manifest.get("kind")) != "telegram_bot_full_state_v153":
                    raise RuntimeError("unknown export kind")
                if int(manifest.get("schema_version") or 0) != int(V153_EXPORT_SCHEMA):
                    raise RuntimeError("unsupported export schema")
                export_version = str(manifest.get("bot_version") or "")
                if not export_version.startswith((
                    "bot_v153_", "bot_v154_", "bot_v155_", "bot_v156_", "bot_v157_", "bot_v158_",
                    "bot_v159_", "bot_v160_", "bot_v161_", "bot_v162_", "bot_v163_", "bot_v164_", "bot_v165_",
                )):
                    raise RuntimeError(f"unsupported bot version: {export_version or 'missing'}")
                if _v153_db_logical_checksum(raw) != str(manifest.get("checksum") or ""):
                    raise RuntimeError("checksum mismatch")
                return manifest, raw
            except Exception:
                _v165_shutil.rmtree(folder, ignore_errors=True)
                raise


try:
    bot_journal(
        "v165_owner_first_circle_compat_installed",
        int(OWNER_ID or 0),
        "owner_row=restored_in_forward_and_finmode_first_circle; owner_settings=preserved; circle1=ordinary_isolated_chats; circle2=separate",
    )
except Exception:
    pass

# v178_global_performance_final
