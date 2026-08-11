# v181_recovery_readonly
"""Emergency read-only MEGA recovery scanner.

This build MUST NOT mutate MEGA.  It inventories both historical roots and inspects
SQLite/global snapshots so the owner can choose a known-good source before any restore.
"""
import os as _rec_os
import re as _rec_re
import io as _rec_io
import json as _rec_json
import gzip as _rec_gzip
import time as _rec_time
import shutil as _rec_shutil
import sqlite3 as _rec_sqlite3
import tempfile as _rec_tempfile
import subprocess as _rec_subprocess
from pathlib import Path as _RecPath
from datetime import datetime as _rec_datetime

RECOVERY_READONLY_MODE = True
_RECOVERY_WRITE_COMMANDS = {"mega-put", "mega-mv", "mega-rm", "mega-mkdir"}
_RECOVERY_ORIG_MEGA_RUN = globals().get("_mega_run")
_RECOVERY_LAST_REPORT = ""
_RECOVERY_LAST_SCAN = {}
_RECOVERY_SCAN_RUNNING = False


def _recovery_is_owner(msg) -> bool:
    try:
        uid = int(getattr(getattr(msg, "from_user", None), "id", 0) or 0)
        cid = int(getattr(getattr(msg, "chat", None), "id", 0) or 0)
        fn = globals().get("tenant_is_platform_owner_user")
        if callable(fn) and fn(uid):
            return True
        return bool(globals().get("is_owner_chat") and is_owner_chat(cid))
    except Exception:
        return False


def _recovery_mega_run(cmd, args, *, check=True, timeout=None, **kwargs):
    """Hard write barrier: every MEGA mutating command is blocked."""
    name = str(cmd or "").strip().lower()
    if name in _RECOVERY_WRITE_COMMANDS:
        try:
            log_info(f"[RECOVERY READONLY] blocked {name} args={list(args or [])[:3]}")
        except Exception:
            pass
        cp = _rec_subprocess.CompletedProcess([name] + list(args or []), 77, stdout="", stderr="RECOVERY_READONLY_BLOCKED")
        if check:
            raise RuntimeError(f"RECOVERY_READONLY_BLOCKED:{name}")
        return cp
    if callable(_RECOVERY_ORIG_MEGA_RUN):
        return _RECOVERY_ORIG_MEGA_RUN(cmd, args, check=check, timeout=timeout, **kwargs)
    raise RuntimeError("MEGA runner unavailable")


# Install the write barrier before main() starts.  All previously defined functions resolve
# _mega_run dynamically from the shared module namespace.
if callable(_RECOVERY_ORIG_MEGA_RUN):
    globals()["_mega_run"] = _recovery_mega_run

# Also force the existing restore guard on so high-level automatic snapshot functions stop
# before they even reach the MEGA command barrier where possible.
try:
    RESTORE_GUARD_ACTIVE = True
    RESTORE_GUARD_REASON = "v181 emergency recovery read-only: cloud writes disabled"
except Exception:
    pass


def _recovery_roots() -> list[str]:
    rows = []
    explicit = str(_rec_os.getenv("MEGA_BACKUP_DIR") or "").strip().rstrip("/")
    for root in (explicit, "/TelegramBotBackups", "/TelegramBotBackupsStart"):
        root = str(root or "").strip().rstrip("/")
        if root and root not in rows:
            rows.append(root)
    return rows


def _recovery_find(remote_dir: str, pattern: str, limit: int = 30) -> list[str]:
    try:
        fn = globals().get("_mega_find_remote_files")
        return list(fn(remote_dir, pattern, limit=limit) or []) if callable(fn) else []
    except Exception:
        return []


def _recovery_download(remote: str) -> tuple[str | None, str | None]:
    """Download any remote file and return (file, temp_dir)."""
    folder = _rec_tempfile.mkdtemp(prefix="v181_recovery_scan_")
    try:
        res = _recovery_mega_run("mega-get", [remote, folder], check=False, timeout=max(float(globals().get("MEGA_TIMEOUT", 90) or 90), 120.0))
        if int(getattr(res, "returncode", 1) or 1) != 0:
            _rec_shutil.rmtree(folder, ignore_errors=True)
            return None, None
        base = _rec_os.path.basename(str(remote).rstrip("/"))
        exact = _rec_os.path.join(folder, base)
        if _rec_os.path.isfile(exact):
            return exact, folder
        for p in _RecPath(folder).rglob("*"):
            if p.is_file() and (p.name == base or p.name.endswith((".sqlite3.gz", ".json", ".gz"))):
                return str(p), folder
    except Exception:
        pass
    _rec_shutil.rmtree(folder, ignore_errors=True)
    return None, None


def _recovery_date_tokens(obj) -> list[str]:
    vals = []
    if not isinstance(obj, dict):
        return vals
    for key in ("day_key", "date", "created_at", "updated_at", "timestamp", "time", "datetime"):
        raw = obj.get(key)
        if raw is not None:
            vals.append(str(raw))
    return vals


def _recovery_date_norm(raw: str) -> str:
    s = str(raw or "").strip()
    m = _rec_re.search(r"(20\d{2})[-/.](\d{1,2})[-/.](\d{1,2})", s)
    if m:
        return f"{int(m.group(1)):04d}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    m = _rec_re.search(r"(\d{1,2})[.:/\-](\d{1,2})[.:/\-](20\d{2}|\d{2})", s)
    if m:
        y = int(m.group(3)); y = y + 2000 if y < 100 else y
        return f"{y:04d}-{int(m.group(2)):02d}-{int(m.group(1)):02d}"
    return ""


def _recovery_record_key(rec: dict) -> str:
    if not isinstance(rec, dict):
        return repr(rec)
    for k in ("id", "record_id", "uid", "uuid", "message_id", "source_message_id"):
        if rec.get(k) not in (None, ""):
            return f"{k}:{rec.get(k)}"
    try:
        return _rec_json.dumps(rec, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    except Exception:
        return repr(rec)


def _recovery_inspect_sqlite_gz(remote: str) -> dict:
    row = {"remote": remote, "kind": "sqlite", "valid": False, "error": "", "created_at": "", "chats": 0,
           "records": 0, "ars_records": 0, "usd_records": 0, "unique_records": 0, "min_date": "", "max_date": "", "chat_counts": {}}
    local = folder = raw = None
    try:
        local, folder = _recovery_download(remote)
        if not local:
            row["error"] = "download failed"; return row
        raw = _rec_os.path.join(folder, "inspect.sqlite3")
        with _rec_gzip.open(local, "rb") as src, open(raw, "wb") as dst:
            _rec_shutil.copyfileobj(src, dst, length=1024 * 1024)
        con = _rec_sqlite3.connect(f"file:{raw}?mode=ro", uri=True)
        try:
            chk = con.execute("PRAGMA quick_check").fetchone()
            if not chk or str(chk[0]).lower() != "ok":
                row["error"] = f"quick_check={chk}"; return row
            row["valid"] = True
            try:
                m = con.execute("SELECT v FROM meta WHERE kind='db_snapshot' AND k='main'").fetchone()
                if m:
                    row["created_at"] = str((_rec_json.loads(m[0]) or {}).get("created_at") or "")
            except Exception:
                pass
            try:
                row["chats"] = int(con.execute("SELECT COUNT(*) FROM chats").fetchone()[0] or 0)
            except Exception:
                pass
            unique = set(); dates = []
            try:
                fields = con.execute("SELECT chat_id,k,v FROM cold_fields WHERE k IN ('records','ars_records','usd_records')").fetchall()
            except Exception:
                fields = []
            for chat_id, key, payload in fields:
                try: arr = _rec_json.loads(payload) or []
                except Exception: arr = []
                if not isinstance(arr, list):
                    continue
                row[key] = int(row.get(key, 0) or 0) + len(arr)
                c = row["chat_counts"].setdefault(str(chat_id), {"records": 0, "ars_records": 0, "usd_records": 0, "unique": 0})
                c[key] = int(c.get(key, 0) or 0) + len(arr)
                local_unique = set()
                for rec in arr:
                    rk = _recovery_record_key(rec)
                    unique.add((str(chat_id), rk)); local_unique.add(rk)
                    for token in _recovery_date_tokens(rec):
                        d = _recovery_date_norm(token)
                        if d: dates.append(d)
                c["unique"] = max(int(c.get("unique", 0) or 0), len(local_unique))
            row["unique_records"] = len(unique)
            if dates:
                row["min_date"] = min(dates); row["max_date"] = max(dates)
        finally:
            con.close()
    except Exception as exc:
        row["error"] = f"{type(exc).__name__}: {str(exc)[:240]}"
    finally:
        if folder:
            _rec_shutil.rmtree(folder, ignore_errors=True)
    return row


def _recovery_inspect_global_json(remote: str) -> dict:
    row = {"remote": remote, "kind": "global_json", "valid": False, "error": "", "created_at": "", "chats": 0,
           "records": 0, "ars_records": 0, "usd_records": 0, "unique_records": 0, "min_date": "", "max_date": ""}
    local = folder = None
    try:
        local, folder = _recovery_download(remote)
        if not local:
            row["error"] = "download failed"; return row
        payload = _rec_json.loads(_RecPath(local).read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            row["error"] = "not a dict"; return row
        row["valid"] = True
        row["created_at"] = str(payload.get("created_at") or payload.get("saved_at") or (payload.get("_state_meta") or {}).get("last_saved_at") or "")
        chats = payload.get("chats") or {}
        row["chats"] = len(chats) if isinstance(chats, dict) else 0
        unique=set(); dates=[]
        if isinstance(chats, dict):
            for cid, store in chats.items():
                if not isinstance(store, dict): continue
                for key in ("records", "ars_records", "usd_records"):
                    arr=store.get(key) or []
                    if not isinstance(arr, list): continue
                    row[key] += len(arr)
                    for rec in arr:
                        unique.add((str(cid), _recovery_record_key(rec)))
                        for token in _recovery_date_tokens(rec):
                            d=_recovery_date_norm(token)
                            if d: dates.append(d)
        row["unique_records"] = len(unique)
        if dates: row["min_date"], row["max_date"] = min(dates), max(dates)
    except Exception as exc:
        row["error"] = f"{type(exc).__name__}: {str(exc)[:240]}"
    finally:
        if folder: _rec_shutil.rmtree(folder, ignore_errors=True)
    return row


def _recovery_local_db_stats() -> dict:
    path = str(globals().get("DB_FILE") or "")
    if not path or not _rec_os.path.isfile(path):
        return {"path": path, "error": "local DB missing"}
    folder = _rec_tempfile.mkdtemp(prefix="v181_local_scan_")
    gz = _rec_os.path.join(folder, "local.sqlite3.gz")
    try:
        with open(path, "rb") as src, _rec_gzip.open(gz, "wb") as dst:
            _rec_shutil.copyfileobj(src, dst)
        return _recovery_inspect_local_gz(gz, "LOCAL:" + path)
    finally:
        _rec_shutil.rmtree(folder, ignore_errors=True)


def _recovery_inspect_local_gz(local_gz: str, label: str) -> dict:
    # Reuse inspector without MEGA download.
    row = {"remote": label, "kind": "sqlite", "valid": False, "error": "", "created_at": "", "chats": 0,
           "records": 0, "ars_records": 0, "usd_records": 0, "unique_records": 0, "min_date": "", "max_date": "", "chat_counts": {}}
    folder=_rec_tempfile.mkdtemp(prefix="v181_local_inspect_")
    raw=_rec_os.path.join(folder,"inspect.sqlite3")
    try:
        with _rec_gzip.open(local_gz,"rb") as src, open(raw,"wb") as dst: _rec_shutil.copyfileobj(src,dst)
        con=_rec_sqlite3.connect(f"file:{raw}?mode=ro", uri=True)
        try:
            chk=con.execute("PRAGMA quick_check").fetchone(); row["valid"]=bool(chk and str(chk[0]).lower()=="ok")
            try:
                m=con.execute("SELECT v FROM meta WHERE kind='db_snapshot' AND k='main'").fetchone()
                if m: row["created_at"]=str((_rec_json.loads(m[0]) or {}).get("created_at") or "")
            except Exception: pass
            try: row["chats"]=int(con.execute("SELECT COUNT(*) FROM chats").fetchone()[0] or 0)
            except Exception: pass
            unique=set(); dates=[]
            try: fields=con.execute("SELECT chat_id,k,v FROM cold_fields WHERE k IN ('records','ars_records','usd_records')").fetchall()
            except Exception: fields=[]
            for chat_id,key,payload in fields:
                try: arr=_rec_json.loads(payload) or []
                except Exception: arr=[]
                if not isinstance(arr,list): continue
                row[key]=int(row.get(key,0) or 0)+len(arr)
                c=row["chat_counts"].setdefault(str(chat_id),{"records":0,"ars_records":0,"usd_records":0,"unique":0}); c[key]+=len(arr)
                lu=set()
                for rec in arr:
                    rk=_recovery_record_key(rec); unique.add((str(chat_id),rk)); lu.add(rk)
                    for token in _recovery_date_tokens(rec):
                        d=_recovery_date_norm(token)
                        if d: dates.append(d)
                c["unique"]=max(c["unique"],len(lu))
            row["unique_records"]=len(unique)
            if dates: row["min_date"],row["max_date"]=min(dates),max(dates)
        finally: con.close()
    except Exception as exc: row["error"]=f"{type(exc).__name__}: {str(exc)[:240]}"
    finally: _rec_shutil.rmtree(folder,ignore_errors=True)
    return row


def _recovery_scan_sync(chat_id: int) -> dict:
    global _RECOVERY_LAST_SCAN, _RECOVERY_LAST_REPORT, _RECOVERY_SCAN_RUNNING
    started=_rec_time.monotonic(); results=[]; inventory={}; errors=[]
    try:
        roots=_recovery_roots()
        for root in roots:
            dbdir=f"{root}/database"; hist=f"{dbdir}/history"; pre=f"{dbdir}/pre_restore"
            candidates=[]
            candidates += _recovery_find(dbdir, "latest_bot_state.sqlite3.gz", 5)
            candidates += _recovery_find(hist, "bot_state_*.sqlite3.gz", 30)
            candidates += _recovery_find(pre, "*.sqlite3.gz", 30)
            seen=set()
            for remote in candidates:
                if remote in seen: continue
                seen.add(remote)
                results.append(_recovery_inspect_sqlite_gz(remote))
            # Legacy global snapshots can contain data from before SQLite LOW-RAM migration.
            globals_remote=[]
            globals_remote += _recovery_find(root, "latest_global.json", 5)
            globals_remote += _recovery_find(f"{root}/history", "global_*.json", 20)
            for remote in globals_remote:
                if remote in seen: continue
                seen.add(remote); results.append(_recovery_inspect_global_json(remote))
            # Inventory only: enough to prove additional recovery layers exist without downloading everything.
            monthly=_recovery_find(f"{root}/monthly", "*.json", 2000)
            chats=_recovery_find(f"{root}/chats", "*.json", 2000)
            deltas=_recovery_find(f"{root}/deltas", "delta_*.json", 2000)
            inventory[root]={"monthly_files_seen":len(monthly),"chat_files_seen":len(chats),"delta_files_seen":len(deltas),
                             "monthly_newest":monthly[0] if monthly else "","monthly_oldest":monthly[-1] if monthly else "",
                             "chat_newest":chats[0] if chats else "","chat_oldest":chats[-1] if chats else "",
                             "delta_newest":deltas[0] if deltas else "","delta_oldest":deltas[-1] if deltas else ""}
        local=_recovery_local_db_stats()
        valid=[r for r in results if r.get("valid")]
        valid_sorted=sorted(valid,key=lambda r:(int(r.get("unique_records",0) or 0), str(r.get("created_at") or "")), reverse=True)
        likely=valid_sorted[0] if valid_sorted else None
        lines=[
            "V181 EMERGENCY RECOVERY SCAN — READ ONLY",
            f"Generated: {globals().get('now_local', lambda: _rec_datetime.now())()}",
            "MEGA WRITES: HARD BLOCKED (put/mv/rm/mkdir)",
            "Roots scanned: " + ", ".join(_recovery_roots()),
            "",
            "CURRENT LOCAL DB:",
            f"valid={local.get('valid')} created_at={local.get('created_at','')} chats={local.get('chats',0)} unique={local.get('unique_records',0)} records={local.get('records',0)} ars={local.get('ars_records',0)} usd={local.get('usd_records',0)} dates={local.get('min_date','')}..{local.get('max_date','')}",
            "",
            "REMOTE SNAPSHOTS (sorted by record coverage):",
        ]
        for idx,r in enumerate(valid_sorted,1):
            mark="  <== LIKELY BEST HISTORY" if likely is r else ""
            lines.append(f"[{idx}] unique={r.get('unique_records',0)} records={r.get('records',0)} ars={r.get('ars_records',0)} usd={r.get('usd_records',0)} chats={r.get('chats',0)} dates={r.get('min_date','')}..{r.get('max_date','')} created={r.get('created_at','')} kind={r.get('kind')} {r.get('remote')}{mark}")
            cc=r.get("chat_counts") or {}
            for cid,c in sorted(cc.items(), key=lambda kv:int((kv[1] or {}).get('unique',0) or 0), reverse=True)[:12]:
                lines.append(f"    chat {cid}: unique={c.get('unique',0)} records={c.get('records',0)} ars={c.get('ars_records',0)} usd={c.get('usd_records',0)}")
        invalid=[r for r in results if not r.get("valid")]
        if invalid:
            lines += ["", "UNREADABLE/INVALID CANDIDATES:"]
            for r in invalid[:40]: lines.append(f"- {r.get('remote')} :: {r.get('error')}")
        lines += ["", "ADDITIONAL RECOVERY INVENTORY:"]
        for root,inv in inventory.items():
            lines.append(f"{root}: monthly={inv['monthly_files_seen']} chats={inv['chat_files_seen']} deltas={inv['delta_files_seen']}")
            if inv['monthly_files_seen']: lines.append(f"  monthly newest={inv['monthly_newest']} | oldest={inv['monthly_oldest']}")
            if inv['chat_files_seen']: lines.append(f"  chats newest={inv['chat_newest']} | oldest={inv['chat_oldest']}")
            if inv['delta_files_seen']: lines.append(f"  deltas newest={inv['delta_newest']} | oldest={inv['delta_oldest']}")
        lines += ["", f"Elapsed: {_rec_time.monotonic()-started:.1f}s", "DO NOT restore blindly. Send this TXT to ChatGPT; recovery merge will be built from the best candidate + recent state."]
        report="\n".join(lines)
        _RECOVERY_LAST_SCAN={"generated_at":str(globals().get('now_local',lambda:_rec_datetime.now())()),"results":results,"inventory":inventory,"local":local,"likely":likely}
        folder=_rec_tempfile.mkdtemp(prefix="v181_report_"); path=_rec_os.path.join(folder,"RECOVERY_SCAN_v181.txt"); _RecPath(path).write_text(report,encoding="utf-8")
        _RECOVERY_LAST_REPORT=path
        try:
            with open(path,"rb") as fh: bot.send_document(int(chat_id), fh, caption="🛟 RECOVERY SCAN v181 — MEGA только чтение")
        except Exception as exc:
            try: bot.send_message(int(chat_id), report[:3900])
            except Exception: pass
        return _RECOVERY_LAST_SCAN
    finally:
        _RECOVERY_SCAN_RUNNING=False


def _recovery_scan_async(chat_id: int):
    global _RECOVERY_SCAN_RUNNING
    if _RECOVERY_SCAN_RUNNING:
        try: bot.send_message(int(chat_id), "🛟 Сканирование уже выполняется.")
        except Exception: pass
        return
    _RECOVERY_SCAN_RUNNING=True
    try:
        pool=globals().get("GENERAL_TASK_POOL")
        if pool is not None and hasattr(pool,"submit_unique"):
            pool.submit_unique("v181-recovery-scan", _recovery_scan_sync, int(chat_id)); return
    except Exception:
        pass
    import threading as _rec_threading
    _rec_threading.Thread(target=_recovery_scan_sync,args=(int(chat_id),),name="v181-recovery-scan",daemon=True).start()


@bot.message_handler(commands=["recovery_scan", "recovery", "rescue_scan"])
def v181_recovery_scan_command(msg):
    if not _recovery_is_owner(msg):
        bot.reply_to(msg,"⛔ Только владелец платформы."); return
    bot.reply_to(msg,
        "🛟 RECOVERY READ-ONLY\n"
        "MEGA-запись полностью заблокирована. Сейчас проверю /TelegramBotBackups и /TelegramBotBackupsStart, историю SQLite, pre_restore и старые global snapshots.\n"
        "После завершения пришлю RECOVERY_SCAN_v181.txt.")
    _recovery_scan_async(int(msg.chat.id))


@bot.message_handler(commands=["recovery_status"])
def v181_recovery_status_command(msg):
    if not _recovery_is_owner(msg):
        bot.reply_to(msg,"⛔ Только владелец платформы."); return
    likely=(_RECOVERY_LAST_SCAN or {}).get("likely") or {}
    bot.reply_to(msg,
        "🛟 v181 RECOVERY READ-ONLY\n"
        f"Сканирование: {'идёт' if _RECOVERY_SCAN_RUNNING else 'нет'}\n"
        "MEGA put/mv/rm/mkdir: ЗАБЛОКИРОВАНЫ\n"
        f"Лучший найденный snapshot: {likely.get('remote') or 'пока нет'}\n"
        f"Записей: {likely.get('unique_records',0)} · даты: {likely.get('min_date','')}..{likely.get('max_date','')}")

try:
    bot_journal("v181_recovery_readonly_loaded", int(globals().get("OWNER_ID") or 0) or None, "MEGA writes hard-blocked; scan roots=/TelegramBotBackups,/TelegramBotBackupsStart", "WARN")
except Exception:
    pass
# v181_recovery_readonly
