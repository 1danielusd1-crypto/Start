# v188_restore_forward_fix_final
"""v185 DATA CONSTITUTION.

Immutable storage contract. UI/performance modules must not redefine these functions.
Canonical MEGA root remains /TelegramBotBackups (or MEGA_BACKUP_DIR secret).
"""

DATA_CONSTITUTION_SCHEMA = 1
DATA_CONSTITUTION_QUARANTINE = False
DATA_CONSTITUTION_REASON = ""
DATA_CONSTITUTION_LAST_VERIFY = {}
DATA_CONSTITUTION_LOCK = threading.RLock()
DATA_CONSTITUTION_LEDGER_LOCK = threading.RLock()
DATA_CONSTITUTION_MANIFEST_CACHE = {"at": 0.0, "value": None}
DATA_CONSTITUTION_GENERATION_KEEP_MIN = 24
try:
    LOWRAM_DB_HISTORY_KEEP = max(int(LOWRAM_DB_HISTORY_KEEP), DATA_CONSTITUTION_GENERATION_KEEP_MIN)
except Exception:
    LOWRAM_DB_HISTORY_KEEP = DATA_CONSTITUTION_GENERATION_KEEP_MIN


def constitution_root() -> str:
    return str(MEGA_BACKUP_DIR or "/TelegramBotBackups").rstrip("/")


def constitution_database_dir() -> str:
    return constitution_root() + "/database"


def constitution_generations_dir() -> str:
    return constitution_database_dir() + "/generations"


def constitution_manifests_dir() -> str:
    return constitution_database_dir() + "/manifests"


def constitution_manifest_history_dir() -> str:
    return constitution_database_dir() + "/manifest_history"


def constitution_current_manifest_remote() -> str:
    return constitution_database_dir() + "/current_manifest.json"


def constitution_ledger_root() -> str:
    return constitution_root() + "/ledger/finance"


def constitution_quarantine_active() -> bool:
    return bool(DATA_CONSTITUTION_QUARANTINE or globals().get("RESTORE_GUARD_ACTIVE", False))


def constitution_set_quarantine(reason: str) -> None:
    global DATA_CONSTITUTION_QUARANTINE, DATA_CONSTITUTION_REASON
    reason = str(reason or "DATA CONSTITUTION violation")[:800]
    with DATA_CONSTITUTION_LOCK:
        DATA_CONSTITUTION_QUARANTINE = True
        DATA_CONSTITUTION_REASON = reason
    try:
        _set_restore_guard("DATA CONSTITUTION: " + reason)
    except Exception:
        pass
    try:
        SQLITE.set_meta("data_constitution", "quarantine", {"active": True, "reason": reason, "at": now_local().isoformat(timespec="seconds")})
    except Exception:
        pass
    try:
        runtime_event("data_constitution_quarantine", reason, "ERROR")
    except Exception:
        pass


def constitution_clear_quarantine(reason: str = "verified") -> None:
    global DATA_CONSTITUTION_QUARANTINE, DATA_CONSTITUTION_REASON
    with DATA_CONSTITUTION_LOCK:
        DATA_CONSTITUTION_QUARANTINE = False
        DATA_CONSTITUTION_REASON = ""
    try:
        SQLITE.set_meta("data_constitution", "quarantine", {"active": False, "reason": str(reason), "at": now_local().isoformat(timespec="seconds")})
    except Exception:
        pass


def _constitution_json_clone(value):
    try:
        return json.loads(json.dumps(value, ensure_ascii=False, default=str))
    except Exception:
        return copy.deepcopy(value)


def _constitution_record_key(rec: dict, index: int = 0) -> str:
    if not isinstance(rec, dict):
        return f"bad:{index}"
    for key in ("operation_key", "record_uid", "uid"):
        value = str(rec.get(key) or "").strip()
        if value:
            return key + ":" + value
    source = rec.get("source_msg_id") or rec.get("msg_id") or rec.get("origin_msg_id")
    if source not in (None, "", 0, "0"):
        return "msg:" + str(source)
    return "row:" + hashlib.sha256(json.dumps(rec, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")).hexdigest()[:24]


def _constitution_record_day(rec: dict) -> str:
    day = str((rec or {}).get("day_key") or (rec or {}).get("date") or "").strip()
    if re.fullmatch(r"\d{2}:\d{2}:\d{2}", day):
        try:
            return datetime.strptime(day, "%d:%m:%y").strftime("%Y-%m-%d")
        except Exception:
            pass
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", day):
        return day
    ts = str((rec or {}).get("timestamp") or "")[:10]
    return ts if re.fullmatch(r"\d{4}-\d{2}-\d{2}", ts) else ""


def _constitution_collect_unique_records(store: dict) -> list[dict]:
    seen = {}
    for field in ("records", "ars_records", "usd_records"):
        try:
            rows = store.get(field, []) or []
        except Exception:
            rows = []
        for idx, rec in enumerate(rows):
            if not isinstance(rec, dict):
                continue
            key = _constitution_record_key(rec, idx)
            # Prefer the richer version of the same logical record.
            old = seen.get(key)
            if old is None or len(rec) > len(old):
                seen[key] = rec
    return list(seen.values())


def _constitution_chat_semantics(chat_id, store: dict) -> dict:
    rows = _constitution_collect_unique_records(store or {})
    days = sorted({d for d in (_constitution_record_day(r) for r in rows) if d})
    ars_sum = 0.0
    usd_sum = 0.0
    usd_events = 0
    keys = []
    for idx, rec in enumerate(rows):
        keys.append(_constitution_record_key(rec, idx))
        try:
            ars_sum += float(rec.get("amount", 0) or 0)
        except Exception:
            pass
        if "usd_amount" in rec:
            try:
                u = float(rec.get("usd_amount", 0) or 0)
                usd_sum += u
                if abs(u) > 0 or bool(rec.get("usd_only")):
                    usd_events += 1
            except Exception:
                pass
        elif str(rec.get("currency") or "").upper() == "USD":
            try:
                usd_sum += float(rec.get("amount", 0) or 0); usd_events += 1
            except Exception:
                pass
    keys.sort()
    return {
        "chat_id": int(chat_id),
        "record_count": len(rows),
        "usd_event_count": int(usd_events),
        "earliest_day": days[0] if days else "",
        "latest_day": days[-1] if days else "",
        "ars_sum": round(ars_sum, 6),
        "usd_sum": round(usd_sum, 6),
        "record_keys_hash": hashlib.sha256("\n".join(keys).encode("utf-8")).hexdigest(),
    }


def _constitution_integrity_state() -> tuple[int, str]:
    try:
        root = (_root_settings().get("finance_integrity_v141") or {}) if "_root_settings" in globals() else {}
        return int(root.get("event_seq") or 0), str((root.get("anchor") or {}).get("hash") or "")
    except Exception:
        return 0, ""


def _constitution_ledger_highwater() -> dict:
    try:
        root_value = ((_root_settings().get("data_constitution_ledger_highwater") or {}) if "_root_settings" in globals() else {})
        if isinstance(root_value, dict) and int(root_value.get("seq") or 0) > 0:
            return root_value
    except Exception:
        pass
    try:
        value = SQLITE.get_meta("data_constitution", "ledger_highwater", {}) or {}
        return value if isinstance(value, dict) else {}
    except Exception:
        return {}


def constitution_semantic_manifest_from_live() -> dict:
    chats_out = {}
    chat_ids = set()
    try:
        chat_ids.update(int(x) for x in (SQLITE.load_chats() or {}).keys())
    except Exception:
        pass
    try:
        chat_ids.update(int(x) for x in (data.get("chats", {}) or {}).keys())
    except Exception:
        pass
    total = 0
    for cid in sorted(chat_ids):
        try:
            store = get_chat_store(int(cid))
            row = _constitution_chat_semantics(cid, store)
            if row["record_count"] or is_finance_mode(int(cid)):
                chats_out[str(cid)] = row
                total += int(row["record_count"])
        except Exception as exc:
            try: log_error(f"constitution manifest chat {cid}: {exc}")
            except Exception: pass
    seq, anchor = _constitution_integrity_state()
    high = _constitution_ledger_highwater()
    manifest = {
        "kind": "telegram_bot_data_constitution_manifest",
        "schema_version": DATA_CONSTITUTION_SCHEMA,
        "bot_version": VERSION,
        "created_at": now_local().isoformat(timespec="microseconds"),
        "total_records": int(total),
        "finance_chat_count": len(chats_out),
        "chats": chats_out,
        "integrity_seq": int(seq),
        "integrity_anchor": anchor,
        "ledger_highwater_seq": int(high.get("seq") or 0),
        "ledger_highwater_hash": str(high.get("hash") or ""),
    }
    manifest["semantic_hash"] = hashlib.sha256(json.dumps(manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")).hexdigest()
    return manifest


def constitution_semantic_manifest_from_sqlite(path: str) -> dict:
    conn = sqlite3.connect(str(path))
    try:
        qc = conn.execute("PRAGMA quick_check").fetchone()
        if not qc or str(qc[0]).lower() != "ok":
            raise RuntimeError(f"SQLite quick_check failed: {qc}")
        chats_meta = {}
        for cid, raw in conn.execute("SELECT chat_id,v FROM chats").fetchall():
            try: chats_meta[str(cid)] = json.loads(raw) if raw else {}
            except Exception: chats_meta[str(cid)] = {}
        cold = defaultdict(dict)
        for cid, key, raw in conn.execute("SELECT chat_id,k,v FROM cold_fields").fetchall():
            if str(key) not in {"records", "ars_records", "usd_records"}:
                continue
            try: cold[str(cid)][str(key)] = json.loads(raw) if raw else []
            except Exception: cold[str(cid)][str(key)] = []
        chats_out = {}; total = 0
        for cid in sorted(set(chats_meta) | set(cold), key=lambda x: int(x) if str(x).lstrip("-").isdigit() else str(x)):
            store = dict(chats_meta.get(cid) or {})
            store.update(cold.get(cid) or {})
            try: row = _constitution_chat_semantics(int(cid), store)
            except Exception: continue
            if row["record_count"] or bool((store.get("settings") or {}).get("finance_mode")):
                chats_out[str(cid)] = row; total += int(row["record_count"])
        integrity_seq = 0; ledger_seq = 0; ledger_hash = ""; root = {}
        try:
            root_raw = conn.execute("SELECT v FROM kv WHERE k='root'").fetchone()
            root = json.loads(root_raw[0]) if root_raw and root_raw[0] else {}
            integ = ((root.get("_global_settings") or {}).get("finance_integrity_v141") or {})
            integrity_seq = int(integ.get("event_seq") or 0)
            root_high = ((root.get("_global_settings") or {}).get("data_constitution_ledger_highwater") or {})
            ledger_seq = int(root_high.get("seq") or 0); ledger_hash = str(root_high.get("hash") or "")
        except Exception:
            pass
        try:
            hrow = conn.execute("SELECT v FROM meta WHERE kind='data_constitution' AND k='ledger_highwater'").fetchone()
            high = json.loads(hrow[0]) if hrow and hrow[0] else {}
            if int(high.get("seq") or 0) > ledger_seq:
                ledger_seq = int(high.get("seq") or 0); ledger_hash = str(high.get("hash") or "")
        except Exception:
            pass
        manifest = {
            "kind": "telegram_bot_data_constitution_manifest",
            "schema_version": DATA_CONSTITUTION_SCHEMA,
            "bot_version": VERSION,
            "created_at": now_local().isoformat(timespec="microseconds"),
            "total_records": int(total),
            "finance_chat_count": len(chats_out),
            "chats": chats_out,
            "integrity_seq": int(integrity_seq),
            "ledger_highwater_seq": int(ledger_seq),
            "ledger_highwater_hash": ledger_hash,
        }
        manifest["semantic_hash"] = hashlib.sha256(json.dumps(manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")).hexdigest()
        return manifest
    finally:
        conn.close()


def _constitution_authorized_deletes_since(old_seq: int) -> dict:
    out = defaultdict(int)
    try:
        events = list((_integrity_root().get("events") or []))
    except Exception:
        events = []
    for ev in events:
        try:
            if int(ev.get("seq") or 0) <= int(old_seq):
                continue
            cid = str(int(ev.get("chat_id")))
            action = str(ev.get("action") or "")
            if action == "delete": out[cid] += 1
            elif action == "bulk_delete":
                details = ev.get("details") or {}
                rows = details.get("records") if isinstance(details, dict) else []
                out[cid] += len(rows or []) or len(((ev.get("record") or {}).get("ids") or []))
        except Exception:
            continue
    return dict(out)


def constitution_bootstrap_ledger_genesis() -> dict:
    """Anchor all pre-v185 history once, then immutable ledger covers mutations after that point."""
    current = constitution_load_active_manifest_remote(force=True)
    if current:
        return {"ok": True, "existing": True}
    live = constitution_semantic_manifest_from_live()
    seq = int(live.get("integrity_seq") or 0)
    event = {
        "kind": "telegram_bot_finance_ledger_genesis",
        "schema_version": DATA_CONSTITUTION_SCHEMA,
        "bot_version": VERSION,
        "at": now_local().isoformat(timespec="microseconds"),
        "integrity_seq": seq,
        "semantic_manifest": live,
    }
    event["event_hash"] = hashlib.sha256(json.dumps(event, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")).hexdigest()
    if mega_is_configured():
        workdir = tempfile.mkdtemp(prefix="constitution_genesis_")
        try:
            remote_dir = constitution_ledger_root()+"/genesis"; mega_ensure_remote_path(remote_dir)
            name = f"ledger_genesis_{re.sub(r'[^0-9]','',event['at'])[:20]}_{event['event_hash'][:16]}.json"
            local = os.path.join(workdir, name); _save_json(local, event)
            _mega_run("mega-put", [local, remote_dir], check=True, timeout=MEGA_TIMEOUT)
            high = {"seq": seq, "hash": event["event_hash"], "integrity_hash": str(live.get("integrity_anchor") or ""), "at": event["at"], "remote": remote_dir+"/"+name, "genesis": True}
            SQLITE.set_meta("data_constitution", "ledger_highwater", high)
            try:
                _root_settings()["data_constitution_ledger_highwater"] = copy.deepcopy(high)
                _root_save_coalesced("constitution_ledger_genesis", 0.1)
            except Exception: pass
            return {"ok": True, "existing": False, "seq": seq, "remote": high["remote"]}
        finally:
            shutil.rmtree(workdir, ignore_errors=True)
    raise RuntimeError("cannot bootstrap DATA CONSTITUTION ledger without MEGA")


def constitution_snapshot_rejection(candidate: dict, current: dict | None) -> str:
    if not isinstance(candidate, dict) or candidate.get("kind") != "telegram_bot_data_constitution_manifest":
        return "candidate manifest invalid"
    if int(candidate.get("ledger_highwater_seq") or 0) < int(candidate.get("integrity_seq") or 0):
        return f"ledger is behind finance integrity: {candidate.get('ledger_highwater_seq')} < {candidate.get('integrity_seq')}"
    if not current or not isinstance(current, dict):
        return ""
    old_total = int(current.get("total_records") or 0); new_total = int(candidate.get("total_records") or 0)
    old_seq = int(current.get("integrity_seq") or 0)
    deletes = _constitution_authorized_deletes_since(old_seq)
    authorized_total = sum(int(x or 0) for x in deletes.values())
    if new_total < old_total and (old_total - new_total) > authorized_total:
        return f"unauthorized total record loss: {old_total}->{new_total}; authorized deletes={authorized_total}"
    old_chats = current.get("chats") or {}; new_chats = candidate.get("chats") or {}
    for cid, old in old_chats.items():
        old_n = int((old or {}).get("record_count") or 0); new_n = int((new_chats.get(str(cid)) or {}).get("record_count") or 0)
        allowed = int(deletes.get(str(cid), 0) or 0)
        if new_n < old_n and (old_n - new_n) > allowed:
            return f"chat {cid} record loss: {old_n}->{new_n}; authorized deletes={allowed}"
        if old_n >= 10 and new_n > 0:
            old_first = str((old or {}).get("earliest_day") or ""); new_first = str((new_chats.get(str(cid)) or {}).get("earliest_day") or "")
            if old_first and new_first and new_first > old_first and (old_n - new_n) > allowed:
                return f"chat {cid} history starts later: {old_first}->{new_first}"
    return ""


def constitution_load_active_manifest_remote(force: bool = False) -> dict | None:
    now_mono = time.monotonic()
    with DATA_CONSTITUTION_LOCK:
        if not force and DATA_CONSTITUTION_MANIFEST_CACHE.get("value") is not None and now_mono - float(DATA_CONSTITUTION_MANIFEST_CACHE.get("at") or 0) < 120:
            return copy.deepcopy(DATA_CONSTITUTION_MANIFEST_CACHE.get("value"))
    if not mega_is_configured():
        return None
    workdir = tempfile.mkdtemp(prefix="constitution_manifest_")
    try:
        res = _mega_run("mega-get", [constitution_current_manifest_remote(), workdir], check=False, timeout=MEGA_TIMEOUT)
        if res.returncode != 0:
            return None
        candidates = list(Path(workdir).rglob("current_manifest.json")) + list(Path(workdir).rglob("*.json"))
        if not candidates:
            return None
        payload = _load_json(str(candidates[0]), None)
        if not isinstance(payload, dict):
            return None
        with DATA_CONSTITUTION_LOCK:
            DATA_CONSTITUTION_MANIFEST_CACHE["at"] = now_mono; DATA_CONSTITUTION_MANIFEST_CACHE["value"] = copy.deepcopy(payload)
        return payload
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


def constitution_publish_sqlite_generation(raw_sqlite: str, gz_path: str, created_at: str, *, allow_destructive: bool = False) -> dict:
    """Publish immutable generation first, then atomically move only the active pointer.

    The legacy latest_bot_state.sqlite3.gz is updated only after the generation is accepted.
    """
    if not mega_is_configured():
        raise RuntimeError("MEGA unavailable")
    candidate = constitution_semantic_manifest_from_sqlite(raw_sqlite)
    current = constitution_load_active_manifest_remote(force=True)
    rejection = "" if bool(allow_destructive) else constitution_snapshot_rejection(candidate, current)
    if rejection:
        constitution_set_quarantine("snapshot rejected: " + rejection)
        raise RuntimeError(rejection)
    stamp = re.sub(r"[^0-9]", "", str(created_at or now_local().isoformat(timespec="seconds")))[:20] or str(int(time.time()))
    digest = str(candidate.get("semantic_hash") or "")[:12]
    generation_name = f"generation_{stamp}_{digest}.sqlite3.gz"
    manifest_name = f"generation_{stamp}_{digest}.json"
    workdir = tempfile.mkdtemp(prefix="constitution_publish_")
    try:
        manifest_local = os.path.join(workdir, manifest_name)
        candidate.update({"generation": generation_name, "remote_generation": constitution_generations_dir()+"/"+generation_name})
        _save_json(manifest_local, candidate)
        mega_ensure_remote_path(constitution_generations_dir())
        mega_ensure_remote_path(constitution_manifests_dir())
        mega_ensure_remote_path(constitution_manifest_history_dir())
        # Immutable generation and its manifest: unique names, never overwritten.
        upload_gz = os.path.join(workdir, generation_name); shutil.copy2(gz_path, upload_gz)
        _mega_run("mega-put", [upload_gz, constitution_generations_dir()], check=True, timeout=MEGA_TIMEOUT)
        _mega_run("mega-put", [manifest_local, constitution_manifests_dir()], check=True, timeout=MEGA_TIMEOUT)
        # Active pointer is the only mutable constitution object.
        pointer_candidate_name = f"candidate_current_manifest_{stamp}_{digest}.json"
        pointer_local = os.path.join(workdir, pointer_candidate_name); _save_json(pointer_local, candidate)
        mega_ensure_remote_path(constitution_database_dir())
        _mega_run("mega-put", [pointer_local, constitution_database_dir()], check=True, timeout=MEGA_TIMEOUT)
        remote_pointer_candidate = constitution_database_dir()+"/"+pointer_candidate_name
        archive_name = f"current_manifest_{stamp}_{digest}.json"
        if not _mega_promote_remote_candidate(remote_pointer_candidate, constitution_current_manifest_remote(), history_dir=constitution_manifest_history_dir(), archive_name=archive_name):
            raise RuntimeError("cannot activate constitution current_manifest")
        # Legacy compatibility mirror, only after semantic acceptance and active pointer commit.
        legacy_candidate_name = f"candidate_bot_state_{stamp}_{digest}.sqlite3.gz"
        legacy_local = os.path.join(workdir, legacy_candidate_name); shutil.copy2(gz_path, legacy_local)
        _mega_run("mega-put", [legacy_local, constitution_database_dir()], check=True, timeout=MEGA_TIMEOUT)
        remote_legacy_candidate = constitution_database_dir()+"/"+legacy_candidate_name
        history = constitution_database_dir()+"/history"; mega_ensure_remote_path(history)
        archive_legacy = f"bot_state_{stamp[:14]}.sqlite3.gz"
        if not _mega_promote_remote_candidate(remote_legacy_candidate, lowram_database_remote_latest(), history_dir=history, archive_name=archive_legacy):
            log_error("[DATA CONSTITUTION] generation active, but legacy latest mirror update failed")
        with DATA_CONSTITUTION_LOCK:
            DATA_CONSTITUTION_MANIFEST_CACHE["at"] = time.monotonic(); DATA_CONSTITUTION_MANIFEST_CACHE["value"] = copy.deepcopy(candidate)
        try:
            SQLITE.set_meta("data_constitution", "active_manifest", candidate)
        except Exception:
            pass
        return candidate
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


def constitution_reanchor_after_manual_restore(reason: str = "manual_restore") -> dict:
    """Explicit owner restore checkpoint. pre_restore must already exist before this is called."""
    live = constitution_semantic_manifest_from_live()
    seq = int(live.get("integrity_seq") or 0)
    checkpoint = {
        "kind": "telegram_bot_data_constitution_restore_checkpoint",
        "schema_version": DATA_CONSTITUTION_SCHEMA,
        "bot_version": VERSION,
        "reason": str(reason),
        "at": now_local().isoformat(timespec="microseconds"),
        "integrity_seq": seq,
        "semantic_manifest": live,
    }
    checkpoint["event_hash"] = hashlib.sha256(json.dumps(checkpoint, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")).hexdigest()
    workdir = tempfile.mkdtemp(prefix="constitution_restore_checkpoint_")
    try:
        remote_dir = constitution_ledger_root()+"/checkpoints"; mega_ensure_remote_path(remote_dir)
        name = f"restore_checkpoint_{re.sub(r'[^0-9]','',checkpoint['at'])[:20]}_{checkpoint['event_hash'][:16]}.json"
        local = os.path.join(workdir, name); _save_json(local, checkpoint)
        _mega_run("mega-put", [local, remote_dir], check=True, timeout=MEGA_TIMEOUT)
        high = {"seq": seq, "hash": checkpoint["event_hash"], "integrity_hash": str(live.get("integrity_anchor") or ""), "at": checkpoint["at"], "remote": remote_dir+"/"+name, "restore_checkpoint": True}
        SQLITE.set_meta("data_constitution", "ledger_highwater", high)
        try:
            _root_settings()["data_constitution_ledger_highwater"] = copy.deepcopy(high)
        except Exception: pass
        # Persist the new highwater inside the SQLite generation.
        try: save_data(data, full=True)
        except Exception: pass
        raw = os.path.join(workdir, "restored_state.sqlite3")
        gz = raw + ".gz"
        SQLITE.backup_to(raw); _lowram_gzip_file(raw, gz)
        active = constitution_publish_sqlite_generation(raw, gz, checkpoint["at"], allow_destructive=True)
        constitution_clear_quarantine("manual restore checkpoint verified")
        try: _clear_restore_guard()
        except Exception: pass
        return {"checkpoint": checkpoint, "active": active}
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


def constitution_download_active_generation(workdir: str) -> tuple[str | None, dict | None, str]:
    manifest = constitution_load_active_manifest_remote(force=True)
    if not manifest:
        return None, None, "no constitution manifest"
    remote = str(manifest.get("remote_generation") or "")
    if not remote:
        return None, manifest, "manifest has no remote_generation"
    res = _mega_run("mega-get", [remote, workdir], check=False, timeout=max(float(MEGA_TIMEOUT), 180.0))
    if res.returncode != 0:
        return None, manifest, "active generation download failed"
    candidates = list(Path(workdir).rglob(os.path.basename(remote))) or list(Path(workdir).rglob("generation_*.sqlite3.gz"))
    return (str(candidates[0]) if candidates else None), manifest, "constitution generation"


def constitution_boot_verify_after_restore() -> dict:
    global DATA_CONSTITUTION_LAST_VERIFY
    current = constitution_load_active_manifest_remote(force=True)
    live = constitution_semantic_manifest_from_live()
    if not current:
        # First constitution boot: immutable GENESIS anchors all pre-v185 history.
        try:
            genesis = constitution_bootstrap_ledger_genesis()
            live = constitution_semantic_manifest_from_live()
            snapshot_ok = bool(mega_upload_latest_database_backup(force=True))
            active_now = constitution_load_active_manifest_remote(force=True) if snapshot_ok else None
            if not snapshot_ok or not active_now:
                raise RuntimeError("genesis created but first immutable generation was not activated")
            result = {"ok": True, "mode": "bootstrap", "live": live, "genesis": genesis, "active": active_now, "reason": "constitution genesis + first generation created"}
        except Exception as exc:
            constitution_set_quarantine(f"constitution genesis failed: {exc}")
            result = {"ok": False, "mode": "bootstrap", "live": live, "reason": str(exc)}
        DATA_CONSTITUTION_LAST_VERIFY = result
        return result
    rejection = constitution_snapshot_rejection(live, current)
    ok = not bool(rejection)
    result = {"ok": ok, "mode": "verify", "live": live, "current": current, "reason": rejection}
    DATA_CONSTITUTION_LAST_VERIFY = result
    if not ok:
        constitution_set_quarantine("BOOT semantic verification failed: " + rejection)
    else:
        # Only clear the constitution quarantine; manual restore guard remains under existing owner controls.
        with DATA_CONSTITUTION_LOCK:
            globals()["DATA_CONSTITUTION_QUARANTINE"] = False; globals()["DATA_CONSTITUTION_REASON"] = ""
    try:
        runtime_event("data_constitution_boot_verify", f"ok={ok}; records={live.get('total_records')}; active={current.get('total_records')}; {rejection}", "INFO" if ok else "ERROR")
    except Exception:
        pass
    return result


def constitution_ledger_append(chat_id: int, action: str, record: dict | None, details: dict | None, integrity_hash: str, seq: int) -> bool:
    """Synchronous immutable financial event in MEGA.

    A failed ledger write quarantines future mutations/backups. The already committed mutation
    remains recoverable through the existing durable update/delta path.
    """
    if not mega_is_configured():
        constitution_set_quarantine("finance ledger unavailable: MEGA is not configured")
        return False
    event = {
        "kind": "telegram_bot_finance_immutable_ledger",
        "schema_version": DATA_CONSTITUTION_SCHEMA,
        "bot_version": VERSION,
        "seq": int(seq),
        "chat_id": int(chat_id),
        "action": str(action),
        "record": _constitution_json_clone(record or {}),
        "details": _constitution_json_clone(details or {}),
        "integrity_hash": str(integrity_hash or ""),
        "at": now_local().isoformat(timespec="microseconds"),
    }
    event["event_hash"] = hashlib.sha256(json.dumps(event, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")).hexdigest()
    day = event["at"][:10].replace("-", "/")
    remote_dir = constitution_ledger_root()+"/"+day
    name = f"ledger_{int(seq):010d}_{int(chat_id)}_{event['event_hash'][:16]}.json"
    workdir = tempfile.mkdtemp(prefix="constitution_ledger_")
    local = os.path.join(workdir, name)
    try:
        SQLITE.set_meta("data_constitution_pending", name, event)
        _save_json(local, event)
        with DATA_CONSTITUTION_LEDGER_LOCK:
            mega_ensure_remote_path(remote_dir)
            _mega_run("mega-put", [local, remote_dir], check=True, timeout=MEGA_TIMEOUT)
        high = {"seq": int(seq), "hash": event["event_hash"], "integrity_hash": str(integrity_hash or ""), "at": event["at"], "remote": remote_dir+"/"+name}
        SQLITE.set_meta("data_constitution", "ledger_highwater", high)
        try:
            _root_settings()["data_constitution_ledger_highwater"] = copy.deepcopy(high)
            _root_save_coalesced("constitution_ledger_highwater", 0.1)
        except Exception: pass
        SQLITE.set_meta("data_constitution_pending", name, {"done": True, "at": event["at"]})
        return True
    except Exception as exc:
        constitution_set_quarantine(f"immutable finance ledger write failed seq={seq}: {exc}")
        try: log_error(f"[DATA CONSTITUTION LEDGER] {exc}")
        except Exception: pass
        return False
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


def constitution_status_text() -> str:
    active = constitution_load_active_manifest_remote(force=False) or {}
    live = constitution_semantic_manifest_from_live()
    return (
        "🏛 КОНСТИТУЦИЯ ДАННЫХ v185\n"
        f"Статус: {'🚨 КАРАНТИН' if constitution_quarantine_active() else '✅ НОРМА'}\n"
        f"Причина: {DATA_CONSTITUTION_REASON or globals().get('RESTORE_GUARD_REASON','') or '—'}\n"
        f"Live finance records: {live.get('total_records',0)}\n"
        f"Active generation records: {active.get('total_records','—')}\n"
        f"Integrity seq: {live.get('integrity_seq',0)}\n"
        f"Ledger highwater: {live.get('ledger_highwater_seq',0)}\n"
        f"Active generation: {active.get('generation','—')}\n"
        f"MEGA root: {constitution_root()}"
    )


@bot.message_handler(commands=["data_constitution", "constitution"])
def cmd_data_constitution(msg):
    try:
        uid = int(getattr(getattr(msg, "from_user", None), "id", 0) or 0)
        if uid != int(OWNER_ID or 0):
            return
    except Exception:
        return
    try:
        send_and_auto_delete(int(msg.chat.id), constitution_status_text(), 180)
    except Exception as exc:
        try: bot.send_message(int(msg.chat.id), f"❌ DATA CONSTITUTION status: {exc}")
        except Exception: pass


# Storage-core invariant registry. Later modules are verified at package build and runtime boot.
DATA_CONSTITUTION_PROTECTED_SYMBOLS = (
    "constitution_snapshot_rejection",
    "constitution_publish_sqlite_generation",
    "constitution_download_active_generation",
    "constitution_boot_verify_after_restore",
    "constitution_ledger_append",
    "constitution_semantic_manifest_from_live",
    "constitution_semantic_manifest_from_sqlite",
)
DATA_CONSTITUTION_PROTECTED_IDS = {name: id(globals().get(name)) for name in DATA_CONSTITUTION_PROTECTED_SYMBOLS}


def constitution_verify_protected_symbols() -> tuple[bool, str]:
    changed = [name for name, ident in DATA_CONSTITUTION_PROTECTED_IDS.items() if id(globals().get(name)) != ident]
    if changed:
        constitution_set_quarantine("storage-core symbol redefined: " + ", ".join(changed))
        return False, ", ".join(changed)
    return True, "ok"
# v188_restore_forward_fix_final
