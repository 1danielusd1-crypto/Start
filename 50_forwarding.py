# v186_restore_exact_fast
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
        
def _v177_legacy_0140_resolve_forward_targets(source_chat_id: int):
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
try: _v177_legacy_0140_resolve_forward_targets.__name__ = 'resolve_forward_targets'
except Exception: pass
resolve_forward_targets = _v177_legacy_0140_resolve_forward_targets
def _v177_legacy_0141_add_forward_link(src_chat_id: int, dst_chat_id: int, mode: str):
    fr = data.setdefault("forward_rules", {})
    src = str(src_chat_id)
    dst = str(dst_chat_id)

    fr.setdefault(src, {})[dst] = mode

    persist_forward_rules_to_owner()
    save_data(data)
    schedule_config_backup_for_chats(src_chat_id, dst_chat_id)
try: _v177_legacy_0141_add_forward_link.__name__ = 'add_forward_link'
except Exception: pass
add_forward_link = _v177_legacy_0141_add_forward_link

def _v177_legacy_0144_remove_forward_link(src_chat_id: int, dst_chat_id: int):
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
try: _v177_legacy_0144_remove_forward_link.__name__ = 'remove_forward_link'
except Exception: pass
remove_forward_link = _v177_legacy_0144_remove_forward_link
def _v177_legacy_0145_clear_forward_all():
    """Полностью отключает всю пересылку."""
    data["forward_rules"] = {}
    data["forward_finance"] = {}
    persist_forward_rules_to_owner()
    save_data(data)
    schedule_config_backup_for_chats()
try: _v177_legacy_0145_clear_forward_all.__name__ = 'clear_forward_all'
except Exception: pass
clear_forward_all = _v177_legacy_0145_clear_forward_all

def get_forward_finance(src_chat_id: int, dst_chat_id: int) -> bool:
    ff = data.setdefault("forward_finance", {})
    return bool(ff.get(str(src_chat_id), {}).get(str(dst_chat_id), False))


FORWARD_COPY_EDIT_MODES = ("normal", "button", "slash")
def _v177_legacy_0146_forward_copy_edit_mode(chat_id: int | None = None) -> str:
    """One GLOBAL 💰Перес mode for every chat/copy.

    v92-v123 stored this in owner/chat scopes. v124 imports that legacy value once and then
    keeps a single root value, so changing the mode in INFO changes all bot copies globally.
    """
    mode = ""
    try:
        gs = (data or {}).setdefault("_global_settings", {})
        mode = str(gs.get("forward_copy_edit_mode_global") or "").strip().lower()
        if mode not in FORWARD_COPY_EDIT_MODES:
            # Compatibility import: old root -> owner scope -> owner chat.
            legacy = str(gs.get("forward_copy_edit_mode") or "").strip().lower()
            if legacy not in FORWARD_COPY_EDIT_MODES:
                try:
                    scope = owner_scope_id(chat_id)
                    legacy = str(owner_scoped_settings(scope).get("forward_copy_edit_mode") or "").strip().lower()
                except Exception:
                    legacy = ""
            if legacy not in FORWARD_COPY_EDIT_MODES:
                try:
                    scope = owner_scope_id(chat_id)
                    legacy = str(get_chat_store(scope).setdefault("settings", {}).get("forward_copy_edit_mode") or "").strip().lower()
                except Exception:
                    legacy = ""
            mode = legacy if legacy in FORWARD_COPY_EDIT_MODES else "normal"
            gs["forward_copy_edit_mode_global"] = mode
            gs["forward_copy_edit_mode"] = mode  # legacy mirror for older code/backups
    except Exception:
        mode = "normal"
    if mode not in FORWARD_COPY_EDIT_MODES or not version_mode_feature("forward_copy_edit"):
        return "normal"
    return mode
try: _v177_legacy_0146_forward_copy_edit_mode.__name__ = 'forward_copy_edit_mode'
except Exception: pass
forward_copy_edit_mode = _v177_legacy_0146_forward_copy_edit_mode


def _v177_legacy_0148_set_forward_copy_edit_mode(chat_id: int, mode: str):
    mode = str(mode or "normal").strip().lower()
    if mode not in FORWARD_COPY_EDIT_MODES:
        mode = "normal"
    gs = data.setdefault("_global_settings", {})
    gs["forward_copy_edit_mode_global"] = mode
    gs["forward_copy_edit_mode"] = mode  # compatibility mirror
    # Mirror to owner scopes/chats so a rollback to an older code version reads the same choice.
    scope_ids = []
    try:
        for scope in [int(OWNER_ID or 0)] + list(get_additional_owner_ids()):
            if not scope:
                continue
            try:
                scope = int(scope)
                owner_scoped_settings(scope)["forward_copy_edit_mode"] = mode
                get_chat_store(scope).setdefault("settings", {})["forward_copy_edit_mode"] = mode
                scope_ids.append(scope)
            except Exception:
                pass
    except Exception:
        pass
    save_data(data, chat_ids=scope_ids or None, root_only=not bool(scope_ids))
    schedule_config_backup_for_chats(*(scope_ids or []), delay=1.0)
    return mode
try: _v177_legacy_0148_set_forward_copy_edit_mode.__name__ = 'set_forward_copy_edit_mode'
except Exception: pass
set_forward_copy_edit_mode = _v177_legacy_0148_set_forward_copy_edit_mode


def _v177_legacy_0150_cycle_forward_copy_edit_mode(chat_id: int) -> str:
    current = forward_copy_edit_mode(int(chat_id))
    try:
        idx = FORWARD_COPY_EDIT_MODES.index(current)
    except ValueError:
        idx = 0
    return set_forward_copy_edit_mode(int(chat_id), FORWARD_COPY_EDIT_MODES[(idx + 1) % len(FORWARD_COPY_EDIT_MODES)])
try: _v177_legacy_0150_cycle_forward_copy_edit_mode.__name__ = 'cycle_forward_copy_edit_mode'
except Exception: pass
cycle_forward_copy_edit_mode = _v177_legacy_0150_cycle_forward_copy_edit_mode


def _v177_legacy_0151_forward_copy_edit_mode_label(chat_id: int) -> str:
    mode = forward_copy_edit_mode(int(chat_id))
    return {
        "normal": "💰Перес: обычно",
        "button": "💰Перес: кнопка",
        "slash": "💰Перес: слеш",
    }.get(mode, "💰Перес: обычно")
try: _v177_legacy_0151_forward_copy_edit_mode_label.__name__ = 'forward_copy_edit_mode_label'
except Exception: pass
forward_copy_edit_mode_label = _v177_legacy_0151_forward_copy_edit_mode_label


try:
    # v125: cosmetic retro refresh is intentionally bounded. New copies always use the
    # selected mode immediately; only recent history is repainted in the background.
    FORWARD_COPY_RETRO_MIN_GAP_SECONDS = max(0.04, min(2.0, float(os.getenv("FORWARD_COPY_RETRO_MIN_GAP_SECONDS", "0.10") or "0.10")))
except Exception:
    FORWARD_COPY_RETRO_MIN_GAP_SECONDS = 0.10
try:
    FORWARD_COPY_RETRO_DAYS = max(1, min(7, int(os.getenv("FORWARD_COPY_RETRO_DAYS", "3") or "3")))
except Exception:
    FORWARD_COPY_RETRO_DAYS = 3
try:
    FORWARD_COPY_RETRO_MAX_PER_CHAT = max(3, min(30, int(os.getenv("FORWARD_COPY_RETRO_MAX_PER_CHAT", "12") or "12")))
except Exception:
    FORWARD_COPY_RETRO_MAX_PER_CHAT = 12
_FORWARD_COPY_RETRO_LOCK = threading.RLock()
_FORWARD_COPY_RETRO_GENERATION = {}


def _begin_forward_copy_retro_refresh(owner_chat_id: int) -> int:
    # Global generation: a newer toggle cancels stale work even when another owner pressed it.
    scope = 0
    with _FORWARD_COPY_RETRO_LOCK:
        generation = int(_FORWARD_COPY_RETRO_GENERATION.get(scope, 0) or 0) + 1
        _FORWARD_COPY_RETRO_GENERATION[scope] = generation
        return generation


def _forward_copy_retro_is_stale(owner_chat_id: int, generation: int | None) -> bool:
    if generation is None:
        return False
    with _FORWARD_COPY_RETRO_LOCK:
        return int(_FORWARD_COPY_RETRO_GENERATION.get(0, 0) or 0) != int(generation)


def _forward_copy_record_identity(chat_id: int, rec: dict):
    """Return (is_forward_copy, msg_id, src_chat_id, src_msg_id), including v92-era rows."""
    if not isinstance(rec, dict):
        return False, 0, None, None
    try:
        msg_id = int(rec.get("forward_dst_msg_id") or rec.get("source_msg_id") or rec.get("origin_msg_id") or rec.get("msg_id") or 0)
    except Exception:
        msg_id = 0
    if not msg_id:
        return False, 0, None, None
    src_chat_id = rec.get("forward_source_chat_id")
    src_msg_id = rec.get("forward_source_msg_id")
    try:
        src_chat_id = int(src_chat_id) if src_chat_id is not None else None
    except Exception:
        src_chat_id = None
    try:
        src_msg_id = int(src_msg_id) if src_msg_id is not None else None
    except Exception:
        src_msg_id = None
    if src_chat_id is None or src_msg_id is None:
        try:
            rev_chat, rev_msg = _find_forward_origin_by_copied_message(int(chat_id), int(msg_id))
            if rev_chat is not None:
                src_chat_id = int(rev_chat)
            if rev_msg is not None:
                src_msg_id = int(rev_msg)
        except Exception:
            pass
    legacy_flag = bool(rec.get("forwarded_by_bot") or rec.get("forwarded_finance") or rec.get("forward_source_chat_id") is not None)
    is_copy = bool(legacy_flag or src_chat_id is not None)
    return is_copy, msg_id, src_chat_id, src_msg_id


def _hydrate_legacy_forward_copy_metadata(chat_id: int, rec: dict, msg_id: int, src_chat_id=None, src_msg_id=None) -> bool:
    """Persist modern metadata when an old pre-deploy finance row is recognized as a copy."""
    changed = False
    try:
        if not rec.get("forwarded_by_bot"):
            rec["forwarded_by_bot"] = True; changed = True
        if rec.get("forward_dst_chat_id") is None:
            rec["forward_dst_chat_id"] = int(chat_id); changed = True
        if rec.get("forward_dst_msg_id") is None:
            rec["forward_dst_msg_id"] = int(msg_id); changed = True
        if src_chat_id is not None and rec.get("forward_source_chat_id") is None:
            rec["forward_source_chat_id"] = int(src_chat_id); changed = True
        if src_msg_id is not None and rec.get("forward_source_msg_id") is None:
            rec["forward_source_msg_id"] = int(src_msg_id); changed = True
        if not rec.get("forward_copy_content_type"):
            rec["forward_copy_content_type"] = "text"; changed = True
        if changed:
            rid = rec.get("id")
            store = get_chat_store(int(chat_id))
            for daily_key in ("daily_records", "ars_daily_records", "usd_daily_records"):
                for arr in (store.get(daily_key, {}) or {}).values():
                    for rr in arr or []:
                        if not isinstance(rr, dict) or rr.get("id") != rid:
                            continue
                        # R-ids can collide between ARS/USD. Prefer exact bot-copy message id.
                        if _record_has_message_id(rr, int(msg_id)):
                            rr.update(rec)
    except Exception:
        pass
    return changed


def _forward_copy_retro_record_is_recent(rec: dict, cutoff_key: str) -> bool:
    """True only for records in the bounded recent-history repaint window."""
    try:
        day = str((rec or {}).get("day_key") or "")[:10]
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", day):
            return day >= cutoff_key
    except Exception:
        pass
    # Legacy rows normally carry an ISO timestamp even when day_key was not persisted.
    for key in ("timestamp", "created_at", "updated_at"):
        try:
            raw = str((rec or {}).get(key) or "")
            m = re.search(r"(20\d{2}-\d{2}-\d{2})", raw)
            if m:
                return m.group(1) >= cutoff_key
        except Exception:
            pass
    return False


def refresh_existing_forward_copy_ui(owner_chat_id: int, mode: str | None = None, generation: int | None = None) -> int:
    """Quick repaint of only the newest bot copies.

    v124 could walk 600+ historical messages and Telegram answered 429/retry_after≈40s,
    making a cosmetic mode switch look frozen. v125 repaints at most the newest
    FORWARD_COPY_RETRO_MAX_PER_CHAT copies per chat from the last
    FORWARD_COPY_RETRO_DAYS calendar days, and interleaves chats round-robin.
    Old history is left untouched; newly created copies always use the selected mode.
    """
    owner_chat_id = int(owner_chat_id)
    mode = mode or forward_copy_edit_mode(owner_chat_id)
    try:
        today_dt = datetime.strptime(today_key(), "%Y-%m-%d").date()
        cutoff_key = (today_dt - timedelta(days=max(0, FORWARD_COPY_RETRO_DAYS - 1))).strftime("%Y-%m-%d")
    except Exception:
        cutoff_key = today_key()

    changed = 0
    attempted = 0
    hydrated = 0
    stopped_stale = False
    metadata_changed_chats = set()
    rate_limited_chats = set()
    candidates_by_chat = []

    try:
        bot_journal(
            "forward_copy_retro_start", owner_chat_id,
            f"GLOBAL mode={mode} generation={generation} days={FORWARD_COPY_RETRO_DAYS} "
            f"max_per_chat={FORWARD_COPY_RETRO_MAX_PER_CHAT} gap={FORWARD_COPY_RETRO_MIN_GAP_SECONDS}s",
        )
    except Exception:
        pass

    # Candidate collection is local/SQLite memory work only; no Telegram API calls here.
    for cid in collect_all_known_chat_ids(include_owner=True):
        if _forward_copy_retro_is_stale(owner_chat_id, generation):
            stopped_stale = True
            break
        try:
            store = get_chat_store(int(cid))
            rows = [rec for _ledger_key, rec in _finance_record_lists(store) if _forward_copy_retro_record_is_recent(rec, cutoff_key)]
            rows.sort(key=record_sort_key, reverse=True)
            seen = set()
            picked = []
            for rec in rows:
                is_copy, msg_id, src_chat_id, src_msg_id = _forward_copy_record_identity(int(cid), rec)
                if not is_copy or not msg_id or rec.get("forward_copy_deleted") or int(msg_id) in seen:
                    continue
                seen.add(int(msg_id))
                picked.append((rec, int(msg_id), src_chat_id, src_msg_id))
                if len(picked) >= FORWARD_COPY_RETRO_MAX_PER_CHAT:
                    break
            if picked:
                candidates_by_chat.append((int(cid), picked))
        except Exception as e:
            log_error(f"refresh_existing_forward_copy_ui collect {cid}: {e}")

    # Round-robin keeps edits to one Telegram chat spread out instead of bursting 100+
    # calls into the same channel/group and triggering a 40-second retry_after.
    max_depth = max((len(rows) for _cid, rows in candidates_by_chat), default=0)
    last_edit_mono = 0.0
    for depth in range(max_depth):
        for cid, picked in candidates_by_chat:
            if depth >= len(picked) or cid in rate_limited_chats:
                continue
            if _forward_copy_retro_is_stale(owner_chat_id, generation):
                stopped_stale = True
                break
            rec, msg_id, src_chat_id, src_msg_id = picked[depth]
            if _hydrate_legacy_forward_copy_metadata(cid, rec, msg_id, src_chat_id, src_msg_id):
                metadata_changed_chats.add(cid)
                hydrated += 1
            try:
                if last_edit_mono > 0:
                    wait = FORWARD_COPY_RETRO_MIN_GAP_SECONDS - (time.monotonic() - last_edit_mono)
                    if wait > 0:
                        time.sleep(wait)
                if _forward_copy_retro_is_stale(owner_chat_id, generation):
                    stopped_stale = True
                    break
                base_text = str(rec.get("source_finance_text") or "").strip() or compose_edit_input_value(rec.get("amount"), rec.get("note", ""))
                display_text = _forward_copy_display_text(base_text, rec, mode)
                markup = _forward_copy_edit_keyboard(mode)
                ct = str(rec.get("forward_copy_content_type") or "text")
                attempted += 1
                last_edit_mono = time.monotonic()
                # Cosmetic repaint must NEVER sleep 40s on Telegram 429. attempts=1 means
                # a rate-limited old copy is simply skipped; live finance/forward stays free.
                if ct == "text":
                    _tg_call_retry(bot.edit_message_text, display_text, chat_id=cid, message_id=msg_id, reply_markup=markup, attempts=1, purpose="forward_copy_retro_text_fast")
                elif ct in {"photo", "video", "document", "audio", "animation", "voice"}:
                    _tg_call_retry(bot.edit_message_caption, caption=display_text, chat_id=cid, message_id=msg_id, reply_markup=markup, attempts=1, purpose="forward_copy_retro_caption_fast")
                else:
                    _tg_call_retry(bot.edit_message_reply_markup, cid, msg_id, reply_markup=markup, attempts=1, purpose="forward_copy_retro_markup_fast")
                changed += 1
            except Exception as e:
                err = str(e).lower()
                if "message is not modified" in err:
                    changed += 1
                elif "message to edit not found" in err or "message_id_invalid" in err or "message not found" in err:
                    rec["forward_copy_deleted"] = True
                    metadata_changed_chats.add(cid)
                elif is_telegram_429(e):
                    # This is cosmetic history only. Do not retry/sleep and do not mark deleted.
                    # Stop repainting this chat for the current toggle so we do not keep
                    # hammering Telegram during its retry_after window.
                    rate_limited_chats.add(cid)
                    try:
                        bot_journal("forward_copy_retro_rate_skip", cid, f"msg={msg_id} mode={mode}; chat paused after 429", "WARN")
                    except Exception:
                        pass
                else:
                    log_error(f"refresh_existing_forward_copy_ui {cid}:{rec.get('id')}: {e}")
        if stopped_stale:
            break

    for cid in metadata_changed_chats:
        try:
            store = get_chat_store(cid)
            _snapshot_active_currency_ledger(store, _ensure_currency_ledgers(store))
            save_data(data, chat_ids=[cid])
        except Exception:
            pass
    try:
        _persist_forward_index_in_data(data)
        save_data(data, root_only=True)
    except Exception:
        pass
    try:
        total_candidates = sum(len(rows) for _cid, rows in candidates_by_chat)
        bot_journal(
            "forward_copy_retro_done", owner_chat_id,
            f"GLOBAL mode={mode} generation={generation} cutoff={cutoff_key} candidates={total_candidates} "
            f"attempted={attempted} changed={changed} hydrated={hydrated} rate_limited_chats={len(rate_limited_chats)} stale={stopped_stale}",
        )
    except Exception:
        pass
    return changed


def _v168_record_uid_seed(chat_id: int, rec: dict) -> str:
    """Stable seed that deliberately excludes mutable amount/note/short_id fields."""
    payload = {
        "chat_id": int(chat_id or 0),
        "operation_key": str((rec or {}).get("operation_key") or ""),
        "source_msg_id": int((rec or {}).get("source_msg_id") or 0),
        "origin_msg_id": int((rec or {}).get("origin_msg_id") or 0),
        "msg_id": int((rec or {}).get("msg_id") or 0),
        "forward_source_chat_id": int((rec or {}).get("forward_source_chat_id") or 0),
        "forward_source_msg_id": int((rec or {}).get("forward_source_msg_id") or 0),
        "source_order_msg_id": int((rec or {}).get("source_order_msg_id") or 0),
        "timestamp": str((rec or {}).get("timestamp") or ""),
        "day_key": str((rec or {}).get("day_key") or ""),
        "id": int((rec or {}).get("id") or 0),
        "currency": "usd" if bool((rec or {}).get("usd_only")) else "ars",
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8", errors="replace")).hexdigest()[:12].upper()


def ensure_finance_record_uid(chat_id: int, rec: dict) -> str:
    if not isinstance(rec, dict):
        return ""
    current = str(rec.get("record_uid") or "").strip().upper()
    if re.fullmatch(r"[A-F0-9]{12}", current):
        return current
    current = _v168_record_uid_seed(int(chat_id), rec)
    rec["record_uid"] = current
    return current


def find_finance_record_by_uid(chat_id: int, record_uid: str):
    uid = str(record_uid or "").strip().upper()
    if not re.fullmatch(r"[A-F0-9]{12}", uid):
        return None
    try:
        for _key, rec in _finance_record_lists(get_chat_store(int(chat_id))):
            if isinstance(rec, dict) and ensure_finance_record_uid(int(chat_id), rec) == uid:
                return rec
    except Exception:
        pass
    return None


def persist_finance_chat_local_fast(chat_id: int) -> bool:
    """Persist one finance chat without rebuilding/saving the global root/forward index."""
    try:
        cid = int(chat_id)
        store = get_chat_store(cid)
        with data_lock:
            if LOWRAM_ENABLED:
                _lowram_flush_chat(cid, store, evict=False)
                SQLITE.save_chat(cid, _lowram_store_meta_payload(store))
            else:
                SQLITE.save_chat(cid, store)
        return True
    except Exception as exc:
        try: log_error(f"v168 local finance persist {chat_id}: {exc}")
        except Exception: pass
        return False


def migrate_finance_record_uids(chat_id: int) -> int:
    changed = 0
    seen = set()
    try:
        for _key, rec in _finance_record_lists(get_chat_store(int(chat_id))):
            if not isinstance(rec, dict) or id(rec) in seen:
                continue
            seen.add(id(rec))
            before = str(rec.get("record_uid") or "")
            after = ensure_finance_record_uid(int(chat_id), rec)
            if after and after != before:
                changed += 1
        if changed:
            persist_finance_chat_local_fast(int(chat_id))
    except Exception as exc:
        try: log_error(f"v168 UID migration chat={chat_id}: {exc}")
        except Exception: pass
    return changed


def _strip_forward_copy_edit_command(text: str) -> str:
    raw = str(text or "").rstrip()
    return re.sub(r"(?:\n|\s)+/izm_[RU]\d+(?:_u[A-F0-9]{12})?\s*$", "", raw, flags=re.I).rstrip()


def _forward_copy_record_command(rec: dict) -> str:
    sid = str((rec or {}).get("short_id") or f"R{(rec or {}).get('id', '')}").strip().upper()
    if not re.fullmatch(r"[RU]\d+", sid):
        sid = "R" + re.sub(r"\D+", "", sid)
    uid = str((rec or {}).get("record_uid") or "").strip().upper()
    return f"/izm_{sid}_u{uid}" if re.fullmatch(r"[A-F0-9]{12}", uid) else f"/izm_{sid}"


def _v169_forward_uid_for_copy(dst_chat_id: int, source_msg) -> str:
    """Deterministic UID known before Telegram creates the destination copy."""
    try:
        src_chat_id = int(getattr(getattr(source_msg, "chat", None), "id", 0) or 0)
        src_msg_id = int(getattr(source_msg, "message_id", 0) or 0)
        raw = f"forward-copy:{src_chat_id}:{src_msg_id}:{int(dst_chat_id)}"
        return hashlib.sha256(raw.encode("utf-8", errors="replace")).hexdigest()[:12].upper()
    except Exception:
        return ""


def _predict_forward_copy_record_command(dst_chat_id: int, source_msg, text: str) -> str | None:
    """Predict the final R/U + stable UID while caller holds locked_chat(dst_chat_id).

    The destination chat lock stays held through Telegram send + finance-row creation in
    _forward_single_to_target, so another finance insert cannot steal the predicted monthly
    position.  The UID is derived only from the immutable forwarding edge and is assigned to
    the concrete row immediately after it is created.
    """
    try:
        raw = str(text or "").strip()
        if not raw or not looks_like_amount(raw) or not is_finance_mode(int(dst_chat_id)):
            return None
        comp = parse_financial_components(raw)
        store = get_chat_store(int(dst_chat_id))
        normalize_chat_records(int(dst_chat_id))
        day_key = finance_day_key_from_message(source_msg) if source_msg is not None else finance_today_key()
        month_key = str(day_key)[:7]
        temp = {
            "id": int(store.get("next_id", 1) or 1),
            "day_key": str(day_key),
            "timestamp": message_timestamp_iso(source_msg),
            "source_order_msg_id": int(getattr(source_msg, "message_id", 0) or 0),
            "source_msg_id": 0,
            "usd_only": bool(comp.get("usd_only", False)),
            "usd_amount": float(comp.get("usd_amount", 0) or 0),
        }
        temp_key = record_sort_key(temp)
        rows = []
        for dk, arr in (store.get("daily_records", {}) or {}).items():
            if str(dk)[:7] != month_key:
                continue
            for rec in arr or []:
                if isinstance(rec, dict):
                    rows.append(rec)
        if bool(temp.get("usd_only")):
            # U numbering is shared by *all* records that contain a USD component,
            # including mixed ARS+USD rows; mirror rebuild_month_short_ids exactly.
            relevant = [r for r in rows if bool(float(r.get("usd_amount", 0) or 0))]
            prefix = "U"
        else:
            relevant = [r for r in rows if not bool(r.get("usd_only", False))]
            prefix = "R"
        position = 1 + sum(1 for r in relevant if record_sort_key(r) < temp_key)
        uid = _v169_forward_uid_for_copy(int(dst_chat_id), source_msg)
        return f"/izm_{prefix}{position}_u{uid}" if uid else None
    except Exception as e:
        try: log_error(f"v169 predict forward copy command dst={dst_chat_id}: {e}")
        except Exception: pass
        return None


def _v169_apply_predicted_record_uid(dst_chat_id: int, rec: dict | None, command: str | None) -> dict | None:
    if not isinstance(rec, dict) or not command:
        return rec
    try:
        match = re.search(r"_u([A-F0-9]{12})$", str(command), flags=re.I)
        if not match:
            return rec
        uid = str(match.group(1)).upper()
        rec["record_uid"] = uid
        store = get_chat_store(int(dst_chat_id))
        rid = int(rec.get("id", -1) or -1)
        for _key, item in _finance_record_lists(store):
            try:
                if int(item.get("id", -2) or -2) == rid:
                    item["record_uid"] = uid
            except Exception:
                pass
        for arr in (store.get("daily_records", {}) or {}).values():
            for item in arr or []:
                try:
                    if int(item.get("id", -2) or -2) == rid:
                        item["record_uid"] = uid
                except Exception:
                    pass
        persist_finance_chat_local_fast(int(dst_chat_id))
    except Exception as exc:
        try: log_error(f"v169 apply predicted UID {dst_chat_id}: {exc}")
        except Exception: pass
    return rec


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
        ensure_finance_record_uid(int(dst_chat_id), rec)
        for _key, item in _finance_record_lists(get_chat_store(int(dst_chat_id))):
            if int(item.get("id", -1)) == int(rec.get("id", -2)):
                item.update(rec)
                ensure_finance_record_uid(int(dst_chat_id), item)
        if not persist_finance_chat_local_fast(int(dst_chat_id)):
            return None
        return rec
    except Exception as e:
        log_error(f"_set_forward_record_metadata({dst_chat_id},{dst_msg_id}): {e}")
        return None


def apply_forward_copy_edit_ui(source_chat_id: int, dst_chat_id: int, dst_msg_id: int, source_msg, rec: dict | None = None) -> bool:
    """Apply edit UI only after the linked finance record is safely present in local SQLite."""
    if not version_mode_feature("forward_copy_edit"):
        return False
    mode = forward_copy_edit_mode(int(source_chat_id))
    if rec is None:
        rec = find_record_by_message_id(int(dst_chat_id), int(dst_msg_id))
    if not rec:
        rec = _set_forward_record_metadata(dst_chat_id, dst_msg_id, source_chat_id, source_msg)
    if not rec:
        log_error(f"[FWD COPY UI] record not found: {source_chat_id}->{dst_chat_id}:{dst_msg_id} mode={mode}")
        return False
    try:
        rec["forward_source_chat_id"] = int(source_chat_id)
        rec["forward_source_msg_id"] = int(getattr(source_msg, "message_id", 0) or 0)
        rec["forward_copy_content_type"] = str(getattr(source_msg, "content_type", "text") or "text")
        ensure_finance_record_uid(int(dst_chat_id), rec)
        for _key, item in _finance_record_lists(get_chat_store(int(dst_chat_id))):
            if not isinstance(item, dict):
                continue
            same_id = int(item.get("id", -1)) == int(rec.get("id", -2))
            same_msg = int(item.get("source_msg_id") or item.get("origin_msg_id") or item.get("msg_id") or 0) == int(dst_msg_id)
            if same_id or same_msg:
                item.update(rec)
                ensure_finance_record_uid(int(dst_chat_id), item)
        if not persist_finance_chat_local_fast(int(dst_chat_id)):
            log_error(f"[FWD COPY UI] local finance persist failed: {source_chat_id}->{dst_chat_id}:{dst_msg_id}")
            return False
    except Exception as exc:
        log_error(f"apply_forward_copy_edit_ui metadata {source_chat_id}->{dst_chat_id}:{dst_msg_id}: {exc}")
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
            _tg_call_retry(bot.edit_message_reply_markup, chat_id=int(dst_chat_id), message_id=int(dst_msg_id), reply_markup=reply_markup, attempts=3, purpose="forward_copy_edit_apply_markup")
        try: schedule_config_backup_for_chats(int(dst_chat_id), delay=0.5)
        except Exception: pass
        return True
    except Exception as exc:
        if "message is not modified" in str(exc).lower():
            return True
        log_error(f"apply_forward_copy_edit_ui Telegram {source_chat_id}->{dst_chat_id}:{dst_msg_id}: {exc}")
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



def _forward_copy_edit_wait_scheduler_key(chat_id: int) -> str:
    return f"forward-copy-edit-wait:{int(chat_id)}"


def _forward_copy_clean_copy_button(text: str):
    """Compatibility helper kept for old code paths; v125 uses the main edit insert UX."""
    return make_copy_or_inline_button("✍️ Вставить текст", "\n" + str(text or ""), viewer_chat_id=None)


def _forward_copy_edit_prompt_text(rec: dict, current: str) -> str:
    sid = str(rec.get("short_id") or ("R" + str(rec.get("id"))))
    return wm_common(
        f"✏️ Редактирование записи {sid}\n\n"
        f"Текущие данные:\n{current}\n\n"
        f"✍️ Напишите новые данные.\n"
        f"Будет изменена эта бот-копия и связанная финансовая запись.\n\n"
        f"⏳ Это сообщение и режим редактирования будут автоматически отменены через 40 секунд.",
        10,
    )


def _forward_copy_edit_prompt_keyboard(current: str, day_key: str | None = None, chat_id: int | None = None):
    kb = types.InlineKeyboardMarkup()
    day_key = str(day_key or today_key())[:10]
    chat_type = ""
    try:
        if chat_id is not None:
            chat_type = str((get_chat_store(int(chat_id)).get("info") or {}).get("type") or "").lower()
    except Exception:
        chat_type = ""
    if current:
        kb.row(make_copy_or_inline_button("✍️ Вставить текст", "\n" + str(current), viewer_chat_id=chat_id))
    kb.row(
        IB("❌ Закрыть", callback_data="fwdcopy_edit_cancel"),
        IB("⬅️ Назад осн. окно", callback_data=f"d:{day_key}:back_main"),
    )
    return kb


def refresh_active_forward_copy_edit_prompt(chat_id: int, dst_msg_id: int, rec: dict | None = None) -> bool:
    """Keep an already-open 💰Перес edit window synchronized with auto-edited bot copies."""
    try:
        chat_id = int(chat_id); dst_msg_id = int(dst_msg_id)
        store = get_chat_store(chat_id)
        wait = store.get("forward_copy_edit_wait") or {}
        if wait.get("type") != "forward_copy_edit" or int(wait.get("dst_msg_id") or 0) != dst_msg_id:
            return False
        rec = rec or find_record_by_message_id(chat_id, dst_msg_id)
        if not isinstance(rec, dict):
            return False
        current = str(rec.get("source_finance_text") or "").strip()
        if not current:
            current = compose_edit_input_value(rec.get("amount"), rec.get("note", ""))
        wait["insert_text"] = current
        prompt = _forward_copy_edit_prompt_text(rec, current)
        wait["countdown_base_text"] = prompt
        store["forward_copy_edit_wait"] = wait
        # v168: 40-second edit-wait state is ephemeral; never block UI on SQLite/root persistence.
        prompt_id = int(wait.get("prompt_msg_id") or 0)
        if prompt_id:
            try:
                _tg_call_retry(
                    bot.edit_message_text, prompt,
                    chat_id=chat_id, message_id=prompt_id,
                    reply_markup=_forward_copy_edit_prompt_keyboard(current, rec.get("day_key"), chat_id=chat_id),
                    purpose="forward_copy_edit_prompt_refresh",
                )
            except Exception as e:
                if "message is not modified" not in str(e).lower():
                    log_error(f"refresh_active_forward_copy_edit_prompt({chat_id},{dst_msg_id}): {e}")
        return True
    except Exception as e:
        log_error(f"refresh_active_forward_copy_edit_prompt({chat_id},{dst_msg_id}): {e}")
        return False


def clear_forward_copy_edit_wait(chat_id: int, delete_prompt: bool = True):
    store = get_chat_store(int(chat_id))
    wait = store.get("forward_copy_edit_wait") or {}
    prompt_id = wait.get("prompt_msg_id")
    force_reply_msg_id = wait.get("force_reply_msg_id")
    DELAYED_SCHEDULER.cancel(_forward_copy_edit_wait_scheduler_key(int(chat_id)))
    store["forward_copy_edit_wait"] = None
    # v168: ephemeral wait cancellation is RAM-only.
    if delete_prompt:
        for _mid in (prompt_id, force_reply_msg_id):
            if not _mid:
                continue
            try:
                bot.delete_message(int(chat_id), int(_mid))
            except Exception:
                pass


def schedule_forward_copy_edit_wait_cancel(chat_id: int, prompt_message_id: int, delay: float | None = None):
    if delay is None:
        delay = internal_timer_seconds("input_wait", 40)
    def _job():
        try:
            store = get_chat_store(int(chat_id))
            wait = store.get("forward_copy_edit_wait") or {}
            if int(wait.get("prompt_msg_id") or 0) != int(prompt_message_id):
                return
            clear_forward_copy_edit_wait(int(chat_id), delete_prompt=True)
            bot_journal("forward_copy_edit_timeout", int(chat_id), f"prompt={prompt_message_id}; window deleted")
        except Exception as e:
            log_error(f"schedule_forward_copy_edit_wait_cancel({chat_id}): {e}")
    DELAYED_SCHEDULER.cancel(_forward_copy_edit_wait_scheduler_key(int(chat_id)))
    DELAYED_SCHEDULER.schedule(_forward_copy_edit_wait_scheduler_key(int(chat_id)), float(delay), _job)

def start_forward_copy_edit(chat_id: int, dst_msg_id: int) -> bool:
    rec = find_record_by_message_id(int(chat_id), int(dst_msg_id))
    if not rec:
        send_and_auto_delete(int(chat_id), "❌ Связанная финансовая запись не найдена.", 8)
        return False
    is_copy, _msg_id, source_chat_id, source_msg_id = _forward_copy_record_identity(int(chat_id), rec)
    if not is_copy:
        send_and_auto_delete(int(chat_id), "❌ Это сообщение не связано с бот-копией пересылки.", 8)
        return False
    _hydrate_legacy_forward_copy_metadata(int(chat_id), rec, int(dst_msg_id), source_chat_id, source_msg_id)
    current = str(rec.get("source_finance_text") or "").strip()
    if not current:
        if float(rec.get("usd_amount", 0) or 0) and bool(rec.get("usd_only", False)):
            current = compose_edit_input_value(rec.get("usd_amount"), rec.get("usd_note") or rec.get("note", ""))
        else:
            current = compose_edit_input_value(rec.get("amount"), rec.get("note", ""))

    prompt = _forward_copy_edit_prompt_text(rec, current)
    sent = _tg_call_retry(
        bot.send_message, int(chat_id), prompt,
        reply_markup=_forward_copy_edit_prompt_keyboard(current, rec.get("day_key"), chat_id=int(chat_id)),
        purpose="forward_copy_edit_prompt",
    )
    # v125 uses the same insert/edit flow as the main edit window. Any Telegram @bot
    # prefix is removed by sanitize_telegram_inserted_text before finance parsing.
    force_msg_id = 0

    get_chat_store(int(chat_id))["forward_copy_edit_wait"] = {
        "type": "forward_copy_edit",
        "dst_msg_id": int(dst_msg_id),
        "rid": int(rec.get("id")),
        "record_uid": ensure_finance_record_uid(int(chat_id), rec),
        "source_chat_id": int(source_chat_id or 0),
        "prompt_msg_id": int(sent.message_id),
        "force_reply_msg_id": int(force_msg_id or 0),
        "insert_text": current,
        "countdown_base_text": prompt,
        "expires_at": time.time() + 40,
    }
    # v168: edit-wait state is intentionally RAM-only and expires in seconds.
    schedule_forward_copy_edit_wait_cancel(int(chat_id), int(sent.message_id), None)
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
    is_copy, _msg_id, source_chat_id, source_msg_id = _forward_copy_record_identity(int(chat_id), rec)
    if not is_copy:
        return False
    _hydrate_legacy_forward_copy_metadata(int(chat_id), rec, int(dst_msg_id), source_chat_id, source_msg_id)
    with locked_chat(int(chat_id)):
        if not update_record_in_chat(int(chat_id), rid, amount, note, source_finance_text=str(comp.get("source_finance_text") or clean_text), source_msg_id=int(dst_msg_id)):
            return False
        rec = find_record_by_message_id(int(chat_id), int(dst_msg_id))
        if rec is not None:
            rec["source_finance_text"] = str(comp.get("source_finance_text") or clean_text)
            if comp.get("usd_amount") is not None:
                rec["usd_amount"] = float(comp.get("usd_amount") or 0)
                rec["usd_note"] = str(comp.get("usd_note") or "")
                rec["usd_only"] = bool(comp.get("usd_only", False))
            elif rec.get("usd_amount") is not None:
                rec["usd_amount"] = 0.0
                rec["usd_note"] = ""
                rec["usd_only"] = False
            rebuild_month_short_ids(int(chat_id))
            try: ensure_finance_record_uid(int(chat_id), rec)
            except Exception: pass
            if not persist_finance_chat_local_fast(int(chat_id)):
                return False
            try: schedule_financial_window_refresh(int(chat_id), str(day_key), reason="forward_copy_edit_immediate_v168")
            except Exception: pass
    mode = forward_copy_edit_mode(int(chat_id))
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
    _durable_note_source_consumed("forward_copy_manual_edit")
    if isinstance(rec, dict):
        _durable_note_record_edit_witness(_durable_record_edit_witness(
            int(chat_id), rid, amount=rec.get("amount", 0), note=rec.get("note", ""),
            source_finance_text=rec.get("source_finance_text", ""),
            usd_amount=rec.get("usd_amount") if rec.get("usd_amount") is not None else None,
            usd_note=rec.get("usd_note") if rec.get("usd_amount") is not None else None,
            kind="forward_copy_edit",
        ))
    finance_changed(int(chat_id), day_key, reason="forward_copy_manual_edit", delay=0.1)
    return True

def _has_visible_fin_mode_selected(chat_id: int) -> bool:
    """v108: visible auto-window selection is independent from hidden accounting."""
    try:
        return bool(is_finance_mode(chat_id) and finance_window_mode(chat_id) in {"normal", "open", "first"})
    except Exception:
        return False


def _v177_legacy_0152_ensure_hidden_finance_for_forward_dst(dst_chat_id: int):
    """💰 forwarding enables hidden accounting without touching an already selected visible window mode."""
    try:
        dst_chat_id = int(dst_chat_id)
        was_finance = is_finance_mode(dst_chat_id)
        if not was_finance:
            set_finance_mode(dst_chat_id, True)
            set_finance_window_mode(dst_chat_id, "off", persist_now=False)
        if not is_hidden_finance_mode(dst_chat_id):
            set_hidden_finance_mode(dst_chat_id, True)
        _persist_finance_window_mode_critical(dst_chat_id)
        bot_journal("forward_finance_auto_hidden", dst_chat_id, "💰 учёт пересылки включил скрытые финансы; оконный режим сохранён")
    except Exception as e:
        log_error(f"ensure_hidden_finance_for_forward_dst({dst_chat_id}): {e}")
try: _v177_legacy_0152_ensure_hidden_finance_for_forward_dst.__name__ = 'ensure_hidden_finance_for_forward_dst'
except Exception: pass
ensure_hidden_finance_for_forward_dst = _v177_legacy_0152_ensure_hidden_finance_for_forward_dst


def _v177_legacy_0153_set_forward_finance(src_chat_id: int, dst_chat_id: int, enabled: bool):
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
try: _v177_legacy_0153_set_forward_finance.__name__ = 'set_forward_finance'
except Exception: pass
set_forward_finance = _v177_legacy_0153_set_forward_finance

def _v177_legacy_0154_remove_forward_finance(src_chat_id: int, dst_chat_id: int):
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
try: _v177_legacy_0154_remove_forward_finance.__name__ = 'remove_forward_finance'
except Exception: pass
remove_forward_finance = _v177_legacy_0154_remove_forward_finance


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


def _v177_legacy_0155_persist_forward_finance_delivery_now(src_chat_id: int, src_msg_id: int, dst_chat_id: int, dst_msg_id: int, rec: dict | None = None):
    """Сразу фиксирует Telegram-копию + финансовую запись в SQLite и forward_index."""
    try:
        if isinstance(rec, dict):
            tags = {
                "forwarded_by_bot": True,
                "forward_source_chat_id": int(src_chat_id),
                "forward_source_msg_id": int(src_msg_id),
                "forward_dst_chat_id": int(dst_chat_id),
                "forward_dst_msg_id": int(dst_msg_id),
            }
            rec.update(tags)
            store = get_chat_store(int(dst_chat_id))
            rid = rec.get("id")
            for arr in (store.get("daily_records", {}) or {}).values():
                for rr in arr or []:
                    if isinstance(rr, dict) and rr.get("id") == rid:
                        rr.update(tags)
        _persist_forward_index_in_data(data)
        save_data(data, chat_ids=[int(dst_chat_id)])
        # Для пересланной финансовой записи недостаточно debounce: deploy мог начаться сразу
        # после появления Telegram-сообщения. Ждём подтверждения immutable delta в MEGA.
        mega_ok = False
        try:
            mega_ok = persist_critical_delta_now(int(dst_chat_id))
        except Exception as e:
            log_error(f"[FWD FINANCE DURABLE] critical delta: {e}")
        if not mega_ok:
            # Не теряем обычный retry-механизм, если синхронная загрузка временно не прошла.
            try:
                schedule_quick_backup(int(dst_chat_id), MEGA_DELTA_PRIORITY_DELAY_SECONDS)
            except Exception as e:
                log_error(f"[FWD FINANCE DURABLE] delta retry schedule: {e}")
        log_info(f"[FWD FINANCE DURABLE] persisted local+mega={mega_ok} {src_chat_id}:{src_msg_id} -> {dst_chat_id}:{dst_msg_id}")
        return True
    except Exception as e:
        log_error(f"[FWD FINANCE DURABLE ERROR] {src_chat_id}:{src_msg_id} -> {dst_chat_id}:{dst_msg_id}: {e}")
        return False
try: _v177_legacy_0155_persist_forward_finance_delivery_now.__name__ = '_persist_forward_finance_delivery_now'
except Exception: pass
_persist_forward_finance_delivery_now = _v177_legacy_0155_persist_forward_finance_delivery_now


def _rebuild_forward_index_from_finance_records(d: dict) -> int:
    """После restore восстанавливает forward_index, сканируя по одному чату без удержания всей истории в RAM."""
    added = 0
    try:
        with forward_map_lock:
            for cid, store in ((d or {}).get("chats", {}) or {}).items():
                if not isinstance(store, dict):
                    continue
                try:
                    cid_i = int(cid)
                except Exception:
                    continue
                if LOWRAM_ENABLED and not dict.__contains__(store, "records"):
                    rows = SQLITE.get_cold(cid_i, "records", []) or []
                else:
                    rows = store.get("records", []) or []
                for rec in rows:
                    if not isinstance(rec, dict) or not rec.get("forwarded_by_bot"):
                        continue
                    try:
                        src_chat = int(rec.get("forward_source_chat_id"))
                        src_msg = int(rec.get("forward_source_msg_id"))
                        dst_chat = int(rec.get("forward_dst_chat_id") or cid_i)
                        dst_msg = int(rec.get("forward_dst_msg_id") or rec.get("source_msg_id") or rec.get("msg_id"))
                    except Exception:
                        continue
                    key = (src_chat, src_msg); pair = (dst_chat, dst_msg)
                    rows_map = forward_map.setdefault(key, [])
                    if pair not in rows_map:
                        rows_map.append(pair); added += 1
                # rows becomes unreachable before the next chat.
                rows = None
            if added:
                _persist_forward_index_in_data(d)
        if added:
            log_info(f"[FORWARD INDEX RECOVERY] rebuilt {added} links from SQLite finance records")
        return added
    except Exception as e:
        log_error(f"_rebuild_forward_index_from_finance_records: {e}")
        return 0


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


def _finance_record_lists(store: dict):
    """Active + persistent currency ledgers, active first; duplicate objects are skipped."""
    seen = set()
    for key in ("records", "ars_records", "usd_records"):
        arr = store.get(key, []) or []
        if not isinstance(arr, list):
            continue
        for rec in arr:
            if not isinstance(rec, dict):
                continue
            oid = id(rec)
            if oid in seen:
                continue
            seen.add(oid)
            yield key, rec


def _record_has_message_id(rec: dict, msg_id: int) -> bool:
    try:
        mid = int(msg_id)
    except Exception:
        return False
    for key in ("forward_dst_msg_id", "source_msg_id", "origin_msg_id", "msg_id"):
        try:
            if rec.get(key) is not None and int(rec.get(key)) == mid:
                return True
        except Exception:
            pass
    return False


def find_record_by_message_id(chat_id: int, msg_id: int):
    # v124: old records can be parked in ars_records/usd_records after a currency switch or
    # restored deploy.  Search all persistent ledgers, while keeping the active list first.
    store = get_chat_store(chat_id)
    for _key, r in _finance_record_lists(store):
        if _record_has_message_id(r, msg_id):
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

            rec["source_finance_text"] = str(text or "").strip()
            if text and looks_like_amount(text):
                try:
                    comp = parse_financial_components(text)
                    rec["amount"] = comp.get("amount", 0.0)
                    rec["note"] = comp.get("note", "")
                    if comp.get("usd_amount") is not None:
                        rec["usd_amount"] = float(comp.get("usd_amount") or 0)
                        rec["usd_note"] = str(comp.get("usd_note") or "")
                        rec["usd_only"] = bool(comp.get("usd_only", False))
                    elif rec.get("usd_amount") is not None:
                        rec["usd_amount"] = 0.0
                        rec["usd_note"] = ""
                        rec["usd_only"] = False
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

    # v135 TOTAL SECRET: исходная Telegram-копия уже удалена после скрытого сохранения.
    # Нельзя падать в обычный fallback_send_single — он создаст ВИДИМОЕ сообщение и раскроет секрет.
    try:
        if is_total_secret_mode(int(dst_chat_id)):
            hidden_ok = sync_forwarded_secret_bot_copy_edit(
                int(dst_chat_id), int(dst_msg_id), int(source_chat_id), msg
            )
            if hidden_ok:
                if finance_enabled and text and is_finance_mode(int(dst_chat_id)):
                    sync_forwarded_finance_message(int(dst_chat_id), int(dst_msg_id), text, owner_id, source_msg=msg)
                return int(dst_msg_id)
            # Даже если скрытый индекс повреждён, секретный target никогда не получает visible fallback.
            raise RuntimeError(f"TOTAL SECRET edit could not be stored safely for {dst_chat_id}:{dst_msg_id}")
    except Exception as secret_exc:
        try:
            if is_total_secret_mode(int(dst_chat_id)):
                log_error(f"sync_edited_copy_to_target secret-safe failed {dst_chat_id}:{dst_msg_id}: {secret_exc}")
                _notify_forward_failure(source_chat_id, msg.message_id, dst_chat_id, secret_exc)
                return None
        except Exception:
            pass

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




def _telegram_migrate_to_chat_id(err: Exception):
    """Возвращает новый chat_id, когда Telegram сообщает migration group -> supergroup."""
    try:
        result_json = getattr(err, "result_json", None) or {}
        params = result_json.get("parameters") or {}
        value = params.get("migrate_to_chat_id")
        if value is not None:
            return int(value)
    except Exception:
        pass
    text = str(err or "")
    # Некоторые версии библиотеки не пробрасывают parameters, но текст/JSON может их содержать.
    for pat in (
        r'"migrate_to_chat_id"\s*:\s*(-?\d+)',
        r"migrate_to_chat_id\s*[=:]\s*(-?\d+)",
    ):
        m = re.search(pat, text, re.I)
        if m:
            try:
                return int(m.group(1))
            except Exception:
                pass
    return None


def _merge_chat_store_for_migration(old_id: int, new_id: int):
    chats = data.setdefault("chats", {})
    old_key, new_key = str(int(old_id)), str(int(new_id))
    old_store = chats.get(old_key)
    new_store = chats.get(new_key)
    if not isinstance(old_store, dict):
        return
    if not isinstance(new_store, dict):
        chats[new_key] = old_store
    else:
        # Новый store приоритетнее только там, где уже есть реальные данные.
        for k, v in old_store.items():
            if k not in new_store or new_store.get(k) in (None, "", [], {}):
                new_store[k] = v
        # Финансовые записи объединяем по id/source_msg_id, чтобы миграция не удаляла историю.
        for list_key in ("records", "ars_records", "usd_records"):
            old_rows = old_store.get(list_key) or []
            new_rows = new_store.setdefault(list_key, [])
            seen = {(r.get("id"), r.get("source_msg_id"), r.get("origin_msg_id")) for r in new_rows if isinstance(r, dict)}
            for r in old_rows:
                if not isinstance(r, dict):
                    continue
                sig = (r.get("id"), r.get("source_msg_id"), r.get("origin_msg_id"))
                if sig not in seen:
                    new_rows.append(r)
                    seen.add(sig)
        for daily_key in ("daily_records", "ars_daily_records", "usd_daily_records"):
            od = old_store.get(daily_key) or {}
            nd = new_store.setdefault(daily_key, {})
            for day, rows in od.items():
                dest = nd.setdefault(day, [])
                seen = {(r.get("id"), r.get("source_msg_id"), r.get("origin_msg_id")) for r in dest if isinstance(r, dict)}
                for r in rows or []:
                    if not isinstance(r, dict):
                        continue
                    sig=(r.get("id"), r.get("source_msg_id"), r.get("origin_msg_id"))
                    if sig not in seen:
                        dest.append(r); seen.add(sig)
    chats.pop(old_key, None)


def _v177_legacy_0156_migrate_chat_id_everywhere(old_chat_id: int, new_chat_id: int, reason: str = "telegram supergroup migration") -> bool:
    """Атомарно переносит известный chat_id старой group на новый supergroup chat_id."""
    old_chat_id, new_chat_id = int(old_chat_id), int(new_chat_id)
    if old_chat_id == new_chat_id:
        return True
    try:
        with data_lock:
            _merge_chat_store_for_migration(old_chat_id, new_chat_id)

            # forward_rules / forward_finance: переносим и source-ключ, и destination-ключи.
            for root_key in ("forward_rules", "forward_finance"):
                root = data.setdefault(root_key, {})
                oldk, newk = str(old_chat_id), str(new_chat_id)
                if oldk in root:
                    src_payload = root.pop(oldk) or {}
                    dst_payload = root.setdefault(newk, {})
                    if isinstance(src_payload, dict) and isinstance(dst_payload, dict):
                        dst_payload.update(src_payload)
                    elif src_payload:
                        root[newk] = src_payload
                for src, dsts in list(root.items()):
                    if not isinstance(dsts, dict):
                        continue
                    if oldk in dsts:
                        val = dsts.pop(oldk)
                        # Не затираем уже существующую более новую связь.
                        if newk not in dsts:
                            dsts[newk] = val

            # owner known_chats.
            try:
                for _cid, st in (data.get("chats", {}) or {}).items():
                    if not isinstance(st, dict):
                        continue
                    kc = st.get("known_chats")
                    if isinstance(kc, dict) and str(old_chat_id) in kc:
                        info = kc.pop(str(old_chat_id))
                        kc.setdefault(str(new_chat_id), info)
            except Exception:
                pass

            # Некоторые root-карты индексированы по chat_id.
            for root_key in ("active_messages",):
                root = data.get(root_key)
                if isinstance(root, dict) and str(old_chat_id) in root:
                    oldv = root.pop(str(old_chat_id))
                    root.setdefault(str(new_chat_id), oldv)

            # Реестр окон: host chat и target chat.
            reg = data.get("open_window_registry") or {}
            if isinstance(reg, dict):
                for item in reg.values():
                    if not isinstance(item, dict):
                        continue
                    if int(item.get("chat_id", 0) or 0) == old_chat_id:
                        item["chat_id"] = new_chat_id
                    params = item.get("params") or {}
                    if isinstance(params, dict):
                        for k in ("target_chat_id", "source_chat_id", "dst_chat_id"):
                            try:
                                if int(params.get(k, 0) or 0) == old_chat_id:
                                    params[k] = new_chat_id
                            except Exception:
                                pass

            # Финансовые metadata, которые запомнили source/destination chat id.
            for _cid, st in (data.get("chats", {}) or {}).items():
                if not isinstance(st, dict):
                    continue
                pools = [st.get("records") or [], st.get("ars_records") or [], st.get("usd_records") or []]
                for daily_key in ("daily_records", "ars_daily_records", "usd_daily_records"):
                    for rows in (st.get(daily_key) or {}).values():
                        pools.append(rows or [])
                for rows in pools:
                    for rec in rows:
                        if not isinstance(rec, dict):
                            continue
                        for k in ("forward_source_chat_id", "forward_dst_chat_id"):
                            try:
                                if int(rec.get(k, 0) or 0) == old_chat_id:
                                    rec[k] = new_chat_id
                            except Exception:
                                pass

            # finance_active_chats runtime + persisted list/set if present.
            try:
                if old_chat_id in finance_active_chats:
                    finance_active_chats.discard(old_chat_id)
                    finance_active_chats.add(new_chat_id)
            except Exception:
                pass
            fac = data.get("finance_active_chats")
            if isinstance(fac, list):
                data["finance_active_chats"] = [new_chat_id if int(x)==old_chat_id else x for x in fac]

            # forward_map runtime: source keys и destination pairs.
            with forward_map_lock:
                rebuilt = {}
                for (src, mid), pairs in list(forward_map.items()):
                    nsrc = new_chat_id if int(src) == old_chat_id else int(src)
                    npairs = []
                    for dcid, dmid in pairs:
                        ndcid = new_chat_id if int(dcid) == old_chat_id else int(dcid)
                        pair = (ndcid, int(dmid))
                        if pair not in npairs:
                            npairs.append(pair)
                    key = (nsrc, int(mid))
                    rebuilt.setdefault(key, [])
                    for pair in npairs:
                        if pair not in rebuilt[key]:
                            rebuilt[key].append(pair)
                forward_map.clear(); forward_map.update(rebuilt)
                _persist_forward_index_in_data(data)

            # Сохраняем миграцию синхронно: следующий update уже должен видеть новый ID.
            save_data(data, full=True)
            persist_forward_rules_to_owner()
            try:
                schedule_config_backup_for_chats(new_chat_id, delay=0.1)
                if OWNER_ID:
                    schedule_config_backup_for_chats(int(OWNER_ID), delay=0.1)
            except Exception:
                pass
            try:
                schedule_delta_backup(new_chat_id, delay=0.5, reason="chat_id_migration")
            except Exception:
                pass

        log_info(f"[CHAT MIGRATION] {old_chat_id} -> {new_chat_id}: {reason}")
        try:
            bot_journal("chat_id_migration", new_chat_id, f"{old_chat_id} -> {new_chat_id}; {reason}")
        except Exception:
            pass
        return True
    except Exception as e:
        log_error(f"migrate_chat_id_everywhere({old_chat_id}->{new_chat_id}): {e}")
        return False
try: _v177_legacy_0156_migrate_chat_id_everywhere.__name__ = 'migrate_chat_id_everywhere'
except Exception: pass
migrate_chat_id_everywhere = _v177_legacy_0156_migrate_chat_id_everywhere


def _handle_supergroup_migration_error(old_chat_id: int, err: Exception):
    new_id = _telegram_migrate_to_chat_id(err)
    if new_id is None:
        return None
    if migrate_chat_id_everywhere(int(old_chat_id), int(new_id), reason=str(err)[:300]):
        return int(new_id)
    return None

def _note_forward_target_migrated(source_chat_id: int, source_msg_id: int, old_chat_id: int, new_chat_id: int):
    """Mark old target as migrated so live/durable witnesses do not wait on it forever."""
    try:
        _forward_outcome_update(
            int(source_chat_id), int(source_msg_id),
            dst_chat_id=int(old_chat_id), dst_state="migrated",
        )
    except Exception:
        pass
    try:
        fn = globals().get("_durable_note_forward_target_migration")
        if callable(fn):
            fn(int(source_chat_id), int(old_chat_id), int(new_chat_id))
    except Exception as e:
        try:
            log_error(f"forward migration witness {old_chat_id}->{new_chat_id}: {e}")
        except Exception:
            pass


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


def _forward_single_to_target(source_chat_id: int, msg, dst_chat_id: int, finance_enabled: bool, _migration_retry: bool = False):
    try:
        _forward_outcome_update(source_chat_id, int(getattr(msg, "message_id", 0) or 0), state="dispatching", dst_chat_id=int(dst_chat_id), dst_state="attempted")
    except Exception:
        pass
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

    # v131: 💰Перес должен выглядеть правильно уже в момент появления копии.
    # Кнопка и раньше прикреплялась прямо к copyMessage. Для режима «слеш» текстовые
    # финансовые копии теперь отправляются сразу с будущим /izm_R... или /izm_U...,
    # а запись создаётся под lock того же целевого чата, поэтому номер не успевает сдвинуться.
    pre_copy_markup = None
    initial_slash_command = None
    initial_slash_synced_rec = None
    initial_slash_sent = False
    text_for_finance = _message_text_for_finance(msg)
    copy_edit_mode = "normal"
    try:
        if finance_enabled:
            copy_edit_mode = forward_copy_edit_mode(source_chat_id)
            if copy_edit_mode == "button":
                pre_copy_markup = _forward_copy_edit_keyboard("button")
    except Exception:
        pre_copy_markup = None
        copy_edit_mode = "normal"

    try:
        use_initial_slash = False
        try:
            use_initial_slash = bool(
                finance_enabled
                and copy_edit_mode == "slash"
                and str(getattr(msg, "content_type", "") or "") == "text"
                and text_for_finance
                and is_finance_mode(int(dst_chat_id))
                and looks_like_amount(text_for_finance)
            )
        except Exception:
            use_initial_slash = False

        if use_initial_slash:
            with locked_chat(int(dst_chat_id)):
                initial_slash_command = _predict_forward_copy_record_command(int(dst_chat_id), msg, text_for_finance)
                if initial_slash_command:
                    display_text = (_strip_forward_copy_edit_command(text_for_finance) + "\n" + initial_slash_command).strip()
                    send_kwargs = {}
                    entities = getattr(msg, "entities", None)
                    if entities:
                        send_kwargs["entities"] = entities
                    if reply_to_target_id:
                        send_kwargs["reply_to_message_id"] = int(reply_to_target_id)
                        send_kwargs["allow_sending_without_reply"] = True
                    try:
                        sent = _tg_call_retry(
                            bot.send_message, int(dst_chat_id), display_text,
                            purpose="forward_send_text_initial_slash", **send_kwargs
                        )
                    except TypeError:
                        send_kwargs.pop("allow_sending_without_reply", None)
                        sent = _tg_call_retry(
                            bot.send_message, int(dst_chat_id), display_text,
                            purpose="forward_send_text_initial_slash", **send_kwargs
                        )
                    dst_msg_id = int(sent.message_id)
                    initial_slash_sent = True
                    # Persist the Telegram link before finance work, matching the old safety order.
                    _store_forward_link(source_chat_id, msg.message_id, dst_chat_id, dst_msg_id)
                    _forward_outcome_update(source_chat_id, int(msg.message_id), dst_chat_id=int(dst_chat_id), dst_state="delivered", dst_msg_id=int(dst_msg_id))
                    try:
                        _persist_forward_index_in_data(data)
                        save_data(data, root_only=True)
                    except Exception as e:
                        log_error(f"[FORWARD LINK DURABLE initial slash] {source_chat_id}:{msg.message_id}->{dst_chat_id}:{dst_msg_id}: {e}")
                    owner_id = msg.from_user.id if getattr(msg, "from_user", None) else 0
                    initial_slash_synced_rec = sync_forwarded_finance_message(
                        int(dst_chat_id), int(dst_msg_id), text_for_finance, owner_id, source_msg=msg
                    )
                    if isinstance(initial_slash_synced_rec, dict):
                        initial_slash_synced_rec = _v169_apply_predicted_record_uid(
                            int(dst_chat_id), initial_slash_synced_rec, initial_slash_command
                        )

        if not initial_slash_sent:
            if reply_to_target_id:
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
                sent = _tg_call_retry(bot.copy_message, dst_chat_id, source_chat_id, msg.message_id, reply_markup=pre_copy_markup, purpose="forward_copy_message")
            dst_msg_id = sent.message_id
    except Exception as e_copy:
        # If Telegram changed a basic group into a supergroup, do not lose the migration hint
        # behind a later unsupported manual fallback.
        migrated_id = None if _migration_retry else _handle_supergroup_migration_error(dst_chat_id, e_copy)
        if migrated_id is not None:
            _note_forward_target_migrated(source_chat_id, int(getattr(msg, "message_id", 0) or 0), dst_chat_id, migrated_id)
            return _forward_single_to_target(source_chat_id, msg, migrated_id, finance_enabled, _migration_retry=True)

        # v107: forward_message is a broad Bot API fallback for content classes for which
        # copy_message or our hand-written send_* fallback may not exist yet.  It may show
        # Telegram's original-forward attribution, but it is preferable to silently losing data.
        try:
            sent_forward = _tg_call_retry(
                bot.forward_message,
                dst_chat_id,
                source_chat_id,
                msg.message_id,
                purpose="forward_message_fallback"
            )
            dst_msg_id = sent_forward.message_id
        except Exception as e_forward:
            migrated_id = None if _migration_retry else _handle_supergroup_migration_error(dst_chat_id, e_forward)
            if migrated_id is not None:
                _note_forward_target_migrated(source_chat_id, int(getattr(msg, "message_id", 0) or 0), dst_chat_id, migrated_id)
                return _forward_single_to_target(source_chat_id, msg, migrated_id, finance_enabled, _migration_retry=True)

            try:
                sent_msg = _fallback_send_single(dst_chat_id, msg, reply_to_message_id=reply_to_target_id)
                dst_msg_id = sent_msg.message_id
            except Exception as e_send:
                migrated_id = None if _migration_retry else _handle_supergroup_migration_error(dst_chat_id, e_send)
                if migrated_id is not None:
                    _note_forward_target_migrated(source_chat_id, int(getattr(msg, "message_id", 0) or 0), dst_chat_id, migrated_id)
                    return _forward_single_to_target(source_chat_id, msg, migrated_id, finance_enabled, _migration_retry=True)
                # Preserve the most useful Telegram error in diagnostics when manual fallback merely
                # says "unsupported content_type".
                final_error = e_forward if "Unsupported fallback content_type" in str(e_send) else e_send
                _forward_outcome_update(source_chat_id, int(getattr(msg, "message_id", 0) or 0), dst_chat_id=int(dst_chat_id), dst_state="failed", error=str(final_error))
                _notify_forward_failure(source_chat_id, msg.message_id, dst_chat_id, final_error)
                return None

    _store_forward_link(source_chat_id, msg.message_id, dst_chat_id, dst_msg_id)
    _forward_outcome_update(source_chat_id, int(msg.message_id), dst_chat_id=int(dst_chat_id), dst_state="delivered", dst_msg_id=int(dst_msg_id))
    # После успешной Telegram-доставки индекс фиксируем сразу, не ждём debounce 0.25с.
    try:
        _persist_forward_index_in_data(data)
        save_data(data, root_only=True)
    except Exception as e:
        log_error(f"[FORWARD LINK DURABLE] {source_chat_id}:{msg.message_id}->{dst_chat_id}:{dst_msg_id}: {e}")
    bump_quick_balance_recreate_counter(dst_chat_id)

    if finance_enabled and text_for_finance:
        try:
            owner_id = msg.from_user.id if getattr(msg, "from_user", None) else 0
            ok_fin = initial_slash_synced_rec or sync_forwarded_finance_message(dst_chat_id, dst_msg_id, text_for_finance, owner_id, source_msg=msg)
            if ok_fin:
                _rec = ok_fin if isinstance(ok_fin, dict) else find_record_by_message_id(dst_chat_id, dst_msg_id)
                if isinstance(_rec, dict) and initial_slash_sent:
                    _rec["forward_copy_content_type"] = "text"
                # Сначала durable SQLite + индекс, только затем косметика.
                _persist_forward_finance_delivery_now(source_chat_id, msg.message_id, dst_chat_id, dst_msg_id, _rec)
                if initial_slash_sent and isinstance(_rec, dict):
                    actual_command = _forward_copy_record_command(_rec)
                    if actual_command == initial_slash_command:
                        _ui_ok = True
                        try:
                            bot_journal(
                                "forward_copy_initial_slash", int(dst_chat_id),
                                f"src={source_chat_id}:{msg.message_id} dst_msg={dst_msg_id} command={actual_command}",
                            )
                        except Exception:
                            pass
                    else:
                        # Rare out-of-order history case: correct the command once rather than leave a wrong /izm_.
                        _ui_ok = apply_forward_copy_edit_ui(source_chat_id, dst_chat_id, dst_msg_id, msg, rec=_rec)
                else:
                    _ui_ok = apply_forward_copy_edit_ui(source_chat_id, dst_chat_id, dst_msg_id, msg, rec=_rec)
                if not _ui_ok and forward_copy_edit_mode(source_chat_id) != "normal":
                    schedule_forward_copy_edit_ui_retry(source_chat_id, dst_chat_id, dst_msg_id, msg, rec=_rec, delay=0.8)
            elif text_has_any_digit(text_for_finance):
                try:
                    _dst_fin_mode = bool(is_finance_mode(dst_chat_id))
                except Exception:
                    _dst_fin_mode = False
                if _dst_fin_mode:
                    log_error(f"[FWD FINANCE NOT RECORDED] {get_chat_display_name(source_chat_id)}:{msg.message_id} -> {get_chat_display_name(dst_chat_id)}:{dst_msg_id} text={text_for_finance[:220]!r}")
                else:
                    bot_journal("forward_finance_not_expected", dst_chat_id, f"src={source_chat_id}:{msg.message_id} dst_msg={dst_msg_id}")
        except Exception as e:
            log_error(f"_forward_single_to_target finance sync {get_chat_display_name(source_chat_id)}->{get_chat_display_name(dst_chat_id)}: {e}")

    # Bot-created copies do not generate incoming Telegram updates; capture them explicitly.
    try:
        capture_forwarded_bot_copy_as_secret(dst_chat_id, dst_msg_id, msg)
    except Exception as e:
        log_error(f"forward secret capture {source_chat_id}->{dst_chat_id}:{dst_msg_id}: {e}")

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
    targets = sorted(
        list(resolve_forward_targets(source_chat_id) or []),
        key=lambda row: (0 if bool(row[2]) else 1),
    )
    if not targets:
        for src_msg in messages:
            try:
                _forward_outcome_update(source_chat_id, int(src_msg.message_id), state="no_targets")
            except Exception:
                pass
        return

    # Keep every album item explicitly pending until ALL destination loops are finished.
    # A destination may finish much earlier than the next one; marking the whole message
    # completed after the first target used to let the durable verifier race the worker.
    for src_msg in messages:
        try:
            _forward_outcome_update(source_chat_id, int(src_msg.message_id), state="dispatching")
        except Exception:
            pass

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
        for src_msg in messages:
            try:
                _forward_outcome_update(source_chat_id, int(src_msg.message_id), dst_chat_id=int(dst_chat_id), dst_state="attempted")
            except Exception:
                pass
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
                migrated_id = _handle_supergroup_migration_error(dst_chat_id, e)
                if migrated_id is not None:
                    log_info(f"[MEDIA GROUP MIGRATION RETRY] {dst_chat_id} -> {migrated_id}")
                    for src_msg in messages:
                        _note_forward_target_migrated(source_chat_id, int(getattr(src_msg, "message_id", 0) or 0), dst_chat_id, migrated_id)
                        _forward_single_to_target(source_chat_id, src_msg, migrated_id, finance_enabled, _migration_retry=True)
                    continue
                log_error(f"_flush_media_group_forward send_media_group failed {get_chat_display_name(source_chat_id)}->{get_chat_display_name(dst_chat_id)}: {e}")

        if len(sent_ids) == len(messages):
            for src_msg, dst_msg_id in zip(messages, sent_ids):
                _store_forward_link(source_chat_id, src_msg.message_id, dst_chat_id, dst_msg_id)
                _forward_outcome_update(source_chat_id, int(src_msg.message_id), dst_chat_id=int(dst_chat_id), dst_state="delivered", dst_msg_id=int(dst_msg_id))
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

    # Only now all destination loops have returned. Individual target states still retain
    # delivered/failed evidence, so "completed" does not hide a failed destination.
    for src_msg in messages:
        try:
            _forward_outcome_update(source_chat_id, int(src_msg.message_id), state="completed")
        except Exception:
            pass


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



# v143: one finance source message may fan out to several financial destinations.
# Destinations run in parallel, while key source:destination keeps strict order for that pair.
_FIN_FORWARD_BATCH_LOCK = threading.RLock()
_FIN_FORWARD_BATCHES = {}

def _fin_forward_batch_id(source_chat_id: int, source_msg_id: int) -> str:
    return f"{int(source_chat_id)}:{int(source_msg_id)}"

def _v177_legacy_0157_fin_forward_batch_finish_target(batch_id: str, dst_chat_id: int, ok: bool, elapsed: float, error: str = "") -> None:
    follow = None
    with _FIN_FORWARD_BATCH_LOCK:
        row = _FIN_FORWARD_BATCHES.get(str(batch_id))
        if not isinstance(row, dict):
            return
        row.setdefault("targets", {})[str(int(dst_chat_id))] = {
            "ok": bool(ok), "elapsed": round(float(elapsed), 3), "error": str(error or "")[:300],
        }
        row["remaining"] = max(0, int(row.get("remaining", 0)) - 1)
        if row["remaining"] == 0:
            follow = dict(row)
            _FIN_FORWARD_BATCHES.pop(str(batch_id), None)
    try:
        bot_journal("finance_forward_target_done", follow.get("source_chat_id") if follow else None,
                    f"batch={batch_id} dst={int(dst_chat_id)} ok={bool(ok)} elapsed={elapsed:.3f}s error={str(error or '')[:180]}",
                    "INFO" if ok else "ERROR")
    except Exception:
        pass
    if not follow:
        return
    source_chat_id = int(follow.get("source_chat_id"))
    source_msg_id = int(follow.get("source_msg_id"))
    normal_targets = list(follow.get("normal_targets") or [])
    started = float(follow.get("started_mono") or time.monotonic())
    if normal_targets:
        if not FORWARD_TASK_POOL.submit(source_chat_id, _forward_normal_stage, source_chat_id, follow.get("msg"), normal_targets):
            log_error(f"FORWARD QUEUE FULL AFTER FIN BATCH, INLINE FALLBACK: {source_chat_id}")
            _forward_normal_stage(source_chat_id, follow.get("msg"), normal_targets)
    elif source_msg_id:
        _forward_outcome_update(source_chat_id, source_msg_id, state="completed")
    try:
        target_rows = follow.get("targets") or {}
        failed = sum(1 for x in target_rows.values() if not bool((x or {}).get("ok")))
        bot_journal("finance_forward_batch_done", source_chat_id,
                    f"batch={batch_id} targets={len(target_rows)} failed={failed} elapsed={time.monotonic()-started:.3f}s")
    except Exception:
        pass
try: _v177_legacy_0157_fin_forward_batch_finish_target.__name__ = '_fin_forward_batch_finish_target'
except Exception: pass
_fin_forward_batch_finish_target = _v177_legacy_0157_fin_forward_batch_finish_target

def _fin_forward_target_job(batch_id: str, source_chat_id: int, msg, dst_chat_id: int, finance_enabled: bool) -> None:
    started = time.monotonic(); ok = False; err = ""
    try:
        ok = bool(_forward_single_to_target(int(source_chat_id), msg, int(dst_chat_id), bool(finance_enabled)))
    except Exception as exc:
        err = str(exc)
        log_error(f"FIN-FORWARD TARGET {source_chat_id}->{dst_chat_id}: {exc}")
    finally:
        _fin_forward_batch_finish_target(batch_id, int(dst_chat_id), ok, time.monotonic()-started, err)

def _start_financial_forward_batch(source_chat_id: int, msg, finance_targets: list, normal_targets: list) -> None:
    source_chat_id = int(source_chat_id)
    source_msg_id = int(getattr(msg, "message_id", 0) or 0)
    batch_id = _fin_forward_batch_id(source_chat_id, source_msg_id)
    with _FIN_FORWARD_BATCH_LOCK:
        _FIN_FORWARD_BATCHES[batch_id] = {
            "source_chat_id": source_chat_id, "source_msg_id": source_msg_id, "msg": msg,
            "normal_targets": list(normal_targets or []), "remaining": len(finance_targets or []),
            "targets": {}, "started_mono": time.monotonic(),
        }
    try:
        bot_journal("finance_forward_batch_started", source_chat_id,
                    f"batch={batch_id} finance_targets={len(finance_targets or [])} normal_targets={len(normal_targets or [])}")
    except Exception:
        pass
    for dst_chat_id, _mode, finance_enabled in list(finance_targets or []):
        task_key = f"{source_chat_id}:{int(dst_chat_id)}"
        if not FIN_FORWARD_TASK_POOL.submit(task_key, _fin_forward_target_job, batch_id, source_chat_id, msg, int(dst_chat_id), bool(finance_enabled)):
            log_error(f"FIN-FORWARD TARGET QUEUE FULL, INLINE FALLBACK: {task_key}")
            _fin_forward_target_job(batch_id, source_chat_id, msg, int(dst_chat_id), bool(finance_enabled))


def _forward_targets_stage(source_chat_id: int, msg, targets: list, final_stage: bool = False) -> None:
    """Выполняет уже выбранную часть направлений, не смешивая очереди."""
    source_chat_id = int(source_chat_id)
    source_msg_id = int(getattr(msg, "message_id", 0) or 0)
    try:
        for dst_chat_id, _mode, finance_enabled in list(targets or []):
            _forward_single_to_target(source_chat_id, msg, int(dst_chat_id), bool(finance_enabled))
    finally:
        if final_stage and source_msg_id:
            _forward_outcome_update(source_chat_id, source_msg_id, state="completed")


def _forward_normal_stage(source_chat_id: int, msg, targets: list) -> None:
    _forward_targets_stage(source_chat_id, msg, targets, final_stage=True)


def _forward_financial_stage(source_chat_id: int, msg, finance_targets: list, normal_targets: list) -> None:
    """Сначала финансовые копии и записи, затем обычные/секретные назначения."""
    source_chat_id = int(source_chat_id)
    source_msg_id = int(getattr(msg, "message_id", 0) or 0)
    try:
        _forward_targets_stage(source_chat_id, msg, finance_targets, final_stage=False)
    finally:
        if normal_targets:
            if not FORWARD_TASK_POOL.submit(source_chat_id, _forward_normal_stage, source_chat_id, msg, list(normal_targets)):
                log_error(f"FORWARD QUEUE FULL AFTER FIN STAGE, INLINE FALLBACK: {source_chat_id}")
                _forward_normal_stage(source_chat_id, msg, list(normal_targets))
        elif source_msg_id:
            _forward_outcome_update(source_chat_id, source_msg_id, state="completed")


def schedule_financial_forward_pipeline(source_chat_id: int, msg) -> None:
    """Финансовые назначения идут раньше всех остальных, без потери exact-once защиты."""
    source_chat_id = int(source_chat_id)
    source_msg_id = int(getattr(msg, "message_id", 0) or 0)
    try:
        # Альбом должен собраться целиком; внутри его назначения тоже сортируются ФИН первыми.
        if getattr(msg, "media_group_id", None) and getattr(msg, "content_type", None) in ("photo", "video", "document", "audio"):
            if not FORWARD_TASK_POOL.submit(source_chat_id, _forward_with_finance_priority, source_chat_id, msg):
                log_error(f"MEDIA FORWARD QUEUE FULL, INLINE FALLBACK: {source_chat_id}")
                _forward_with_finance_priority(source_chat_id, msg)
            return
        targets = list(resolve_forward_targets(source_chat_id) or [])
        if not targets:
            if source_msg_id:
                _forward_outcome_update(source_chat_id, source_msg_id, state="no_targets")
            return
        finance_targets = [row for row in targets if bool(row[2])]
        normal_targets = [row for row in targets if not bool(row[2])]
        if source_msg_id:
            _forward_outcome_update(source_chat_id, source_msg_id, state="dispatching")
        if finance_targets:
            _start_financial_forward_batch(source_chat_id, msg, list(finance_targets), list(normal_targets))
        else:
            ok = FORWARD_TASK_POOL.submit(source_chat_id, _forward_normal_stage, source_chat_id, msg, list(normal_targets))
            if not ok:
                log_error(f"FORWARD QUEUE FULL, INLINE FALLBACK: {source_chat_id}")
                _forward_normal_stage(source_chat_id, msg, list(normal_targets))
    except Exception as exc:
        log_error(f"schedule_financial_forward_pipeline {source_chat_id}: {exc}")
        if source_msg_id:
            _forward_outcome_update(source_chat_id, source_msg_id, state="failed")


def forward_any_message(source_chat_id: int, msg):
    try:
        source_msg_id = int(getattr(msg, "message_id", 0) or 0)
        sender_skip_reason = _forward_sender_skip_reason(msg)
        if sender_skip_reason:
            _forward_outcome_skip(source_chat_id, msg, sender_skip_reason)
            return
        if getattr(msg, "edit_date", None):
            _forward_outcome_skip(source_chat_id, msg, "edited_source")
            return

        targets = sorted(
            list(resolve_forward_targets(source_chat_id) or []),
            key=lambda row: (0 if bool(row[2]) else 1),
        )
        if not targets:
            if source_msg_id:
                _forward_outcome_update(source_chat_id, source_msg_id, state="no_targets")
            return

        if getattr(msg, "media_group_id", None) and getattr(msg, "content_type", None) in ("photo", "video", "document", "audio"):
            if source_msg_id:
                _forward_outcome_update(source_chat_id, source_msg_id, state="media_group_pending")
            _collect_media_group_for_forward(source_chat_id, msg)
            return

        if source_msg_id:
            _forward_outcome_update(source_chat_id, source_msg_id, state="dispatching")
        for dst_chat_id, mode, finance_enabled in targets:
            _forward_single_to_target(source_chat_id, msg, dst_chat_id, finance_enabled)
        if source_msg_id:
            _forward_outcome_update(source_chat_id, source_msg_id, state="completed")

    except Exception as e:
        log_error(f"forward_any_message fatal: {e}")
# v186_restore_exact_fast
