import os
import io
import json
import csv
import re
import html
import logging
import threading
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import requests
import telebot
from telebot import types
from telebot.types import InputMediaPhoto, InputMediaVideo, InputMediaDocument, InputMediaAudio
from flask import Flask, request
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from googleapiclient.discovery import build
from google.oauth2 import service_account

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
BACKUP_CHAT_ID = os.getenv("BACKUP_CHAT_ID", "").strip()
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
GDRIVE_FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID", "").strip()
APP_URL = os.getenv("APP_URL", "").strip()
PORT = int(os.getenv("PORT", "8443"))

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set")

VERSION = "Code_022_unified_no_owner"
DEFAULT_TZ = "America/Argentina/Buenos_Aires"
KEEP_ALIVE_INTERVAL_SECONDS = 60

DATA_FILE = "data.json"
CSV_FILE = "data.csv"
CSV_META_FILE = "csv_meta.json"

backup_flags = {"drive": True, "channel": True}
restore_mode = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

bot = telebot.TeleBot(BOT_TOKEN, parse_mode=None)
app = Flask(__name__)

data = {}
finance_active_chats = set()

def log_info(x): logger.info(x)
def log_error(x): logger.error(x)

def get_tz():
    try: return ZoneInfo(DEFAULT_TZ)
    except: return timezone(timedelta(hours=-3))

def now_local(): return datetime.now(get_tz())
def today_key(): return now_local().strftime("%Y-%m-%d")

def _load_json(path, default):
    if not os.path.exists(path): return default
    try:
        with open(path,"r",encoding="utf-8") as f:
            return json.load(f)
    except:
        return default

def _save_json(path,obj):
    try:
        with open(path,"w",encoding="utf-8") as f:
            json.dump(obj,f,ensure_ascii=False,indent=2)
    except Exception as e:
        log_error(f"save {path}: {e}")

def _load_csv_meta():
    return _load_json(CSV_META_FILE, {})

def _save_csv_meta(x):
    _save_json(CSV_META_FILE, x)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CHAT_BACKUP_META_FILE = os.path.join(BASE_DIR, "chat_backup_meta.json")

def _load_chat_backup_meta():
    if not os.path.exists(CHAT_BACKUP_META_FILE):
        return {}
    return _load_json(CHAT_BACKUP_META_FILE, {})

def _save_chat_backup_meta(x):
    _save_json(CHAT_BACKUP_META_FILE, x)

def chat_json_file(cid): return f"data_{cid}.json"
def chat_csv_file(cid): return f"data_{cid}.csv"
def chat_meta_file(cid): return f"csv_meta_{cid}.json"

def default_data():
    return {
        "overall_balance":0,
        "records":[],
        "chats":{},
        "active_messages":{},
        "next_id":1,
        "backup_flags":{"drive":True,"channel":True},
        "finance_active_chats":{},
        "forward_rules":{},
        "known_chats":{}
    }

def load_data():
    d = _load_json(DATA_FILE, default_data())
    base = default_data()
    for k,v in base.items():
        if k not in d: d[k]=v
    flags=d.get("backup_flags") or {}
    backup_flags["drive"]=bool(flags.get("drive",True))
    backup_flags["channel"]=bool(flags.get("channel",True))
    fac=d.get("finance_active_chats") or {}
    finance_active_chats.clear()
    for cid,en in fac.items():
        if en:
            try: finance_active_chats.add(int(cid))
            except: pass
    return d

def save_data(d):
    fac={}
    for cid in finance_active_chats:
        fac[str(cid)]=True
    d["finance_active_chats"]=fac
    d["backup_flags"]={"drive":backup_flags["drive"],"channel":backup_flags["channel"]}
    _save_json(DATA_FILE, d)

def get_chat_store(cid):
    chats=data.setdefault("chats",{})
    store=chats.setdefault(str(cid),{
        "info":{},
        "known_chats":{},
        "balance":0,
        "records":[],
        "daily_records":{},
        "next_id":1,
        "active_windows":{},
        "edit_wait":None,
        "edit_target":None,
        "current_view_day":today_key(),
        "settings":{"auto_add":False}
    })
    if "known_chats" not in store:
        store["known_chats"]={}
    return store

def save_chat_json(cid):
    store=data.get("chats",{}).get(str(cid))
    if not store: store=get_chat_store(cid)
    jp=chat_json_file(cid)
    cp=chat_csv_file(cid)
    mp=chat_meta_file(cid)
    for p in (jp,cp,mp):
        if not os.path.exists(p):
            with open(p,"a",encoding="utf-8"): pass
    payload={
        "chat_id":cid,
        "balance":store.get("balance",0),
        "records":store.get("records",[]),
        "daily_records":store.get("daily_records",{}),
        "next_id":store.get("next_id",1),
        "info":store.get("info",{}),
        "known_chats":store.get("known_chats",{})
    }
    _save_json(jp,payload)
    with open(cp,"w",newline="",encoding="utf-8") as f:
        w=csv.writer(f)
        w.writerow(["chat_id","ID","short_id","timestamp","amount","note","owner","day_key"])
        daily=store.get("daily_records",{})
        for dk in sorted(daily.keys()):
            recs=sorted(daily[dk],key=lambda r:r.get("timestamp",""))
            for r in recs:
                w.writerow([
                    cid,
                    r.get("id"), r.get("short_id"), r.get("timestamp"),
                    r.get("amount"), r.get("note"), r.get("owner"), dk
                ])
    meta={"last_saved":now_local().isoformat(timespec="seconds"),
          "record_count":sum(len(v) for v in store.get("daily_records",{}).values())}
    _save_json(mp,meta)
    log_info(f"saved chat {cid}")

num_re=re.compile(r"[+\-–]?\s*\d[\d\s.,_'’]*")

def fmt_num(x):
    sgn="+" if x>=0 else "-"
    x=abs(x)
    s=f"{x:.12f}".rstrip("0").rstrip(".")
    if "." in s: a,b=s.split(".")
    else: a,b=s,""
    a=f"{int(a):,}".replace(",",".")
    if b: s=f"{a},{b}"
    else: s=a
    return f"{sgn}{s}"

def parse_amount(raw):
    s=raw.strip()
    neg=s.startswith("-") or s.startswith("–")
    pos=s.startswith("+")
    sc=s.lstrip("+-–").strip()
    sc=sc.replace(" ","").replace("_","").replace("’","").replace("'","")
    if "," not in sc and "." not in sc:
        v=float(sc)
        if not pos and not neg: neg=True
        return -v if neg else v
    if "." in sc and "," in sc:
        if sc.rfind(",")>sc.rfind("."):
            sc=sc.replace(".","").replace(",",".")
        else:
            sc=sc.replace(",","")
    else:
        if "," in sc:
            p=sc.rfind(",")
            if len(sc)-p-1 in (1,2):
                sc=sc.replace(".","").replace(",",".")
            else:
                sc=sc.replace(",","")
        elif "." in sc:
            p=sc.rfind(".")
            if len(sc)-p-1 in (1,2):
                sc=sc.replace(",","")
            else:
                sc=sc.replace(".","")
    v=float(sc)
    if not pos and not neg: neg=True
    return -v if neg else v

def split_amount_and_note(t):
    m=num_re.search(t)
    if not m: raise ValueError
    raw=m.group(0)
    amt=parse_amount(raw)
    note=t.replace(raw," ").strip()
    note=re.sub(r"\s+"," ",note).lower()
    return amt,note

def looks_like_amount(t):
    try:
        split_amount_and_note(t)
        return True
    except:
        return False
#🟢🟢🟢🟢🟢🟢🟢🟢🔴🔴🔴🔴🔴🔴🔴🔴
def _get_drive_service():
    if not GOOGLE_SERVICE_ACCOUNT_JSON or not GDRIVE_FOLDER_ID:
        return None
    try:
        info=json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
        creds=service_account.Credentials.from_service_account_info(
            info, scopes=["https://www.googleapis.com/auth/drive"]
        )
        return build("drive","v3",credentials=creds)
    except Exception as e:
        log_error(f"drive: {e}")
        return None

def upload_to_gdrive(path,mime_type=None,description=None):
    if not backup_flags.get("drive",True): return
    service=_get_drive_service()
    if not service: return
    if not os.path.exists(path): return
    fname=os.path.basename(path)
    meta={"name":fname,"parents":[GDRIVE_FOLDER_ID],"description":description or ""}
    media=MediaFileUpload(path,mimetype=mime_type,resumable=True)
    try:
        found=service.files().list(
            q=f"name='{fname}' and '{GDRIVE_FOLDER_ID}' in parents and trashed=false",
            spaces="drive",fields="files(id,name)"
        ).execute().get("files",[])
        if found:
            fid=found[0]["id"]
            service.files().update(fileId=fid,media_body=media,body={"description":description or ""}).execute()
        else:
            service.files().create(body=meta,media_body=media,fields="id").execute()
    except Exception as e:
        log_error(f"upload: {e}")

def download_from_gdrive(filename,dest):
    service=_get_drive_service()
    if not service: return False
    try:
        r=service.files().list(
            q=f"name='{filename}' and '{GDRIVE_FOLDER_ID}' in parents and trashed=false",
            spaces="drive",fields="files(id,name)"
        ).execute().get("files",[])
        if not r: return False
        fid=r[0]["id"]
        req=service.files().get_media(fileId=fid)
        with io.FileIO(dest,"wb") as f:
            dl=MediaIoBaseDownload(f,req)
            done=False
            while not done:
                status,done=dl.next_chunk()
        return True
    except:
        return False

def restore_from_gdrive_if_needed():
    ok=False
    if not os.path.exists(DATA_FILE):
        if download_from_gdrive(DATA_FILE,DATA_FILE): ok=True
    if not os.path.exists(CSV_FILE):
        if download_from_gdrive(CSV_FILE,CSV_FILE): ok=True
    if not os.path.exists(CSV_META_FILE):
        if download_from_gdrive(CSV_META_FILE,CSV_META_FILE): ok=True
    return ok

def export_global_csv(d):
    try:
        with open(CSV_FILE,"w",newline="",encoding="utf-8") as f:
            w=csv.writer(f)
            w.writerow(["chat_id","ID","short_id","timestamp","amount","note","owner","day_key"])
            for cid,cdata in d.get("chats",{}).items():
                for dk,recs in cdata.get("daily_records",{}).items():
                    for r in recs:
                        w.writerow([
                            cid, r.get("id"), r.get("short_id"), r.get("timestamp"),
                            r.get("amount"), r.get("note"), r.get("owner"), dk
                        ])
    except Exception as e:
        log_error(f"export_global_csv: {e}")

EMOJI_DIGITS={"0":"0️⃣","1":"1️⃣","2":"2️⃣","3":"3️⃣","4":"4️⃣","5":"5️⃣","6":"6️⃣","7":"7️⃣","8":"8️⃣","9":"9️⃣"}
backup_channel_notified_chats=set()

def format_chat_id_emoji(cid):
    return "".join(EMOJI_DIGITS.get(c,c) for c in str(cid))

def _safe_chat_title_for_filename(t):
    if not t: return ""
    t=str(t).strip().replace(" ","_")
    t=re.sub(r"[^0-9A-Za-zА-Яа-я_\-]+","",t)
    return t[:32]

def _get_chat_title(cid):
    try:
        s=data.get("chats",{}).get(str(cid),{})
        t=s.get("info",{}).get("title")
        if t: return t
    except: pass
    return f"chat_{cid}"

def send_backup_to_channel_for_file(path,key,title=None):
    if not BACKUP_CHAT_ID: return
    if not os.path.exists(path): return
    try:
        meta=_load_csv_meta()
        mk=f"msg_{key}"
        tk=f"timestamp_{key}"
        base=os.path.basename(path)
        name,_,ext=base.partition(".")
        safe=_safe_chat_title_for_filename(title)
        if safe:
            fname=f"{name}_{safe}"
            if ext: fname+=f".{ext}"
        else:
            fname=base
        cap=f"{fname} — {now_local().strftime('%Y-%m-%d %H:%M')}"
        def _open():
            if not os.path.exists(path): return None
            with open(path,"rb") as f: b=f.read()
            if not b: return None
            buf=io.BytesIO(b)
            buf.name=fname
            buf.seek(0)
            return buf
        if meta.get(mk):
            try:
                fobj=_open()
                if not fobj: return
                bot.edit_message_media(
                    chat_id=int(BACKUP_CHAT_ID),
                    message_id=meta[mk],
                    media=InputMediaDocument(fobj,caption=cap)
                )
            except:
                fobj=_open()
                if not fobj: return
                sent=bot.send_document(int(BACKUP_CHAT_ID),fobj,caption=cap)
                meta[mk]=sent.message_id
        else:
            fobj=_open()
            if not fobj: return
            sent=bot.send_document(int(BACKUP_CHAT_ID),fobj,caption=cap)
            meta[mk]=sent.message_id
        meta[tk]=now_local().isoformat(timespec="seconds")
        _save_csv_meta(meta)
    except Exception as e:
        log_error(f"send_backup_file: {e}")

def send_backup_to_channel(cid):
    if not BACKUP_CHAT_ID: return
    if not backup_flags.get("channel",True): return
    try:
        bc=int(BACKUP_CHAT_ID)
    except:
        return
    save_chat_json(cid)
    export_global_csv(data)
    save_data(data)
    title=_get_chat_title(cid)
    if cid not in backup_channel_notified_chats:
        try:
            bot.send_message(bc, format_chat_id_emoji(cid))
            backup_channel_notified_chats.add(cid)
        except: pass
    jp=chat_json_file(cid)
    cp=chat_csv_file(cid)
    send_backup_to_channel_for_file(jp,f"json_{cid}",title)
    send_backup_to_channel_for_file(cp,f"csv_{cid}",title)
    send_backup_to_channel_for_file(DATA_FILE,"global_data","ALL_CHATS")
    send_backup_to_channel_for_file(CSV_FILE,"global_csv","ALL_CHATS")

def send_backup_to_chat(cid):
    try:
        save_chat_json(cid)
        jp=chat_json_file(cid)
        if not os.path.exists(jp): return
        meta=_load_chat_backup_meta()
        mk=f"msg_chat_{cid}"
        tk=f"timestamp_chat_{cid}"
        title=_get_chat_title(cid)
        cap=f"Авто-бэкап: {title}\n{now_local().strftime('%Y-%m-%d %H:%M:%S')}"
        def _open():
            try:
                with open(jp,"rb") as f: b=f.read()
            except: return None
            if not b: return None
            safe=_safe_chat_title_for_filename(title)
            base=os.path.basename(jp)
            n,e=os.path.splitext(base)
            if safe: fname=f"{n}_{safe}{e}"
            else: fname=base
            buf=io.BytesIO(b)
            buf.name=fname
            return buf
        mid=meta.get(mk)
        if mid:
            fobj=_open()
            if fobj:
                try:
                    bot.edit_message_media(
                        chat_id=cid,
                        message_id=mid,
                        media=InputMediaDocument(fobj,caption=cap)
                    )
                    meta[tk]=now_local().isoformat(timespec="seconds")
                    _save_chat_backup_meta(meta)
                    return
                except: pass
        fobj=_open()
        if not fobj: return
        sent=bot.send_document(cid,fobj,caption=cap)
        meta[mk]=sent.message_id
        meta[tk]=now_local().isoformat(timespec="seconds")
        _save_chat_backup_meta(meta)
    except Exception as e:
        log_error(f"send_backup_to_chat: {e}")

def resolve_forward_targets(cid):
    fr=data.get("forward_rules",{})
    src=str(cid)
    out=[]
    if src not in fr: return []
    for dst,mode in fr[src].items():
        try: out.append((int(dst),mode))
        except: pass
    return out

def add_forward_link(a,b,mode):
    fr=data.setdefault("forward_rules",{})
    fr.setdefault(str(a),{})[str(b)]=mode
    save_data(data)

def remove_forward_link(a,b):
    fr=data.get("forward_rules",{})
    sa=str(a); sb=str(b)
    if sa in fr and sb in fr[sa]:
        del fr[sa][sb]
    if sa in fr and not fr[sa]:
        del fr[sa]
    save_data(data)

def clear_forward_all():
    data["forward_rules"]={}
    save_data(data)

def forward_text_anon(src,msg,targets):
    for dst,_ in targets:
        try: bot.copy_message(dst,src,msg.message_id)
        except: pass

def forward_media_anon(src,msg,targets):
    for dst,_ in targets:
        try: bot.copy_message(dst,src,msg.message_id)
        except: pass
#🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴🔴👍👍👍👍👍
_media_group_cache={}

def collect_media_group(cid,msg):
    gid=msg.media_group_id
    if not gid: return [msg]
    grp=_media_group_cache.setdefault(cid,{})
    arr=grp.setdefault(gid,[])
    arr.append(msg)
    if len(arr)==1:
        time.sleep(0.2)
    comp=grp.pop(gid,arr)
    return comp

def forward_media_group_anon(src,msgs,targets):
    if not msgs: return
    media=[]
    for m in msgs:
        ct=m.content_type
        if ct=="photo":
            media.append(InputMediaPhoto(m.photo[-1].file_id,caption=m.caption))
        elif ct=="video":
            media.append(InputMediaVideo(m.video.file_id,caption=m.caption))
        elif ct=="document":
            media.append(InputMediaDocument(m.document.file_id,caption=m.caption))
        elif ct=="audio":
            media.append(InputMediaAudio(m.audio.file_id,caption=m.caption))
        else:
            for dst,_ in targets:
                try: bot.copy_message(dst,src,m.message_id)
                except: pass
            return
    for dst,_ in targets:
        try: bot.send_media_group(dst,media)
        except: pass

def render_day_window(cid,dk):
    s=get_chat_store(cid)
    recs=s.get("daily_records",{}).get(dk,[])
    lines=[]
    lines.append(f"📅 <b>{dk}</b>")
    lines.append("")
    inc=0.0
    exp=0.0
    rs=sorted(recs,key=lambda x:x.get("timestamp"))
    for r in rs:
        amt=r["amount"]
        if amt>=0: inc+=amt
        else: exp+=-amt
        note=html.escape(r.get("note",""))
        sid=r.get("short_id",f"R{r['id']}")
        lines.append(f"{sid} {fmt_num(amt)} <i>{note}</i>")
    if not rs:
        lines.append("Нет записей за этот день.")
    lines.append("")
    if rs:
        lines.append(f"📉 Расход: {fmt_num(-exp if exp else 0)}")
        lines.append(f"📈 Приход: {fmt_num(inc if inc else 0)}")
    bal=s.get("balance",0)
    lines.append(f"🏦 Остаток: {fmt_num(bal)}")
    total=inc-exp
    return "\n".join(lines),total

def build_main_keyboard(dk,cid=None):
    kb=types.InlineKeyboardMarkup(row_width=3)
    kb.row(
        types.InlineKeyboardButton("➕ Добавить",callback_data=f"d:{dk}:add"),
        types.InlineKeyboardButton("📝 Редактировать",callback_data=f"d:{dk}:edit_menu")
    )
    kb.row(
        types.InlineKeyboardButton("⬅️ Вчера",callback_data=f"d:{dk}:prev"),
        types.InlineKeyboardButton("📅 Сегодня",callback_data=f"d:{dk}:today"),
        types.InlineKeyboardButton("➡️ Завтра",callback_data=f"d:{dk}:next")
    )
    kb.row(
        types.InlineKeyboardButton("📅 Календарь",callback_data=f"d:{dk}:calendar"),
        types.InlineKeyboardButton("📊 Отчёт",callback_data=f"d:{dk}:report")
    )
    kb.row(
        types.InlineKeyboardButton("ℹ️ Инфо",callback_data=f"d:{dk}:info"),
        types.InlineKeyboardButton("💰 Общий итог",callback_data=f"d:{dk}:total")
    )
    return kb

def build_calendar_keyboard(center,cid=None):
    kb=types.InlineKeyboardMarkup(row_width=4)
    daily={}
    if cid is not None:
        s=get_chat_store(cid)
        daily=s.get("daily_records",{})
    start=center - timedelta(days=15)
    for w in range(0,32,4):
        row=[]
        for d in range(4):
            day=start+timedelta(days=w+d)
            label=day.strftime("%d.%m")
            key=day.strftime("%Y-%m-%d")
            if daily.get(key):
                label="📝 "+label
            row.append(types.InlineKeyboardButton(label,callback_data=f"d:{key}:open"))
        kb.row(*row)
    kb.row(
        types.InlineKeyboardButton("⬅️ −31",callback_data=f"c:{(center - timedelta(days=31)).strftime('%Y-%m-%d')}"),
        types.InlineKeyboardButton("➡️ +31",callback_data=f"c:{(center + timedelta(days=31)).strftime('%Y-%m-%d')}")
    )
    kb.row(types.InlineKeyboardButton("📅 Сегодня",callback_data=f"d:{today_key()}:open"))
    return kb

def build_edit_menu_keyboard(dk,cid=None):
    kb=types.InlineKeyboardMarkup(row_width=2)
    kb.row(
        types.InlineKeyboardButton("📝 Записи",callback_data=f"d:{dk}:edit_list"),
        types.InlineKeyboardButton("📂 Общий CSV",callback_data=f"d:{dk}:csv_all")
    )
    kb.row(
        types.InlineKeyboardButton("📅 CSV за день",callback_data=f"d:{dk}:csv_day"),
        types.InlineKeyboardButton("⚙️ Обнулить",callback_data=f"d:{dk}:reset")
    )
    kb.row(
        types.InlineKeyboardButton("📅 Сегодня",callback_data=f"d:{today_key()}:open"),
        types.InlineKeyboardButton("📆 Выбрать день",callback_data=f"d:{dk}:pick_date")
    )
    kb.row(
        types.InlineKeyboardButton("ℹ️ Инфо",callback_data=f"d:{dk}:info"),
        types.InlineKeyboardButton("🔙 Назад",callback_data=f"d:{dk}:back_main")
    )
    return kb

def build_forward_chat_list(dk,cid):
    kb=types.InlineKeyboardMarkup()
    known=data.get("known_chats",{})
    rules=data.get("forward_rules",{})
    for cid2,info in known.items():
        try: c2=int(cid2)
        except: continue
        title=info.get("title") or f"Чат {cid2}"
        cur=rules.get(str(cid),{}).get(cid2)
        if cur=="oneway_to": label=f"{title} ➡️"
        elif cur=="oneway_from": label=f"{title} ⬅️"
        elif cur=="twoway": label=f"{title} ↔️"
        else: label=title
        kb.row(types.InlineKeyboardButton(label,callback_data=f"d:{dk}:fw_cfg_{cid2}"))
    kb.row(types.InlineKeyboardButton("🔙 Назад",callback_data=f"d:{dk}:edit_menu"))
    return kb

def build_forward_direction_menu(dk,a,b):
    kb=types.InlineKeyboardMarkup(row_width=1)
    kb.row(types.InlineKeyboardButton(f"➡️ {a} → {b}",callback_data=f"d:{dk}:fw_one_{b}"))
    kb.row(types.InlineKeyboardButton(f"⬅️ {b} → {a}",callback_data=f"d:{dk}:fw_rev_{b}"))
    kb.row(types.InlineKeyboardButton("↔️ Двусторонняя",callback_data=f"d:{dk}:fw_two_{b}"))
    kb.row(types.InlineKeyboardButton("❌ Удалить",callback_data=f"d:{dk}:fw_del_{b}"))
    kb.row(types.InlineKeyboardButton("🔙 Назад",callback_data=f"d:{dk}:forward_old"))
    return kb

def set_active_window_id(cid,dk,mid):
    aw=data.setdefault("active_messages",{}).setdefault(str(cid),{})
    aw[dk]=mid
    save_data(data)

def get_active_window_id(cid,dk):
    aw=data.setdefault("active_messages",{}).setdefault(str(cid),{})
    return aw.get(dk)

def delete_active_window_if_exists(cid,dk):
    mid=get_active_window_id(cid,dk)
    if not mid: return
    try: bot.delete_message(cid,mid)
    except: pass
    aw=data.setdefault("active_messages",{}).setdefault(str(cid),{})
    if dk in aw: del aw[dk]
    save_data(data)

def update_or_send_day_window(cid,dk):
    txt,_=render_day_window(cid,dk)
    kb=build_main_keyboard(dk,cid)
    mid=get_active_window_id(cid,dk)
    if mid:
        try:
            bot.edit_message_text(txt,cid,mid,reply_markup=kb,parse_mode="HTML")
            return
        except: pass
    sent=bot.send_message(cid,txt,reply_markup=kb,parse_mode="HTML")
    set_active_window_id(cid,dk,sent.message_id)

def is_finance_mode(cid):
    return cid in finance_active_chats

def set_finance_mode(cid,en):
    if en: finance_active_chats.add(cid)
    else: finance_active_chats.discard(cid)

def require_finance(cid):
    if not is_finance_mode(cid):
        send_and_auto_delete(cid,"Финансовый режим выключен.\nКоманда /поехали")
        return False
    return True

def refresh_total_message_if_any(cid):
    s=get_chat_store(cid)
    mid=s.get("total_msg_id")
    if not mid: return
    try:
        bal=s.get("balance",0)
        txt=f"💰 Итог по чату: {fmt_num(bal)}"
        bot.edit_message_text(txt,cid,mid,parse_mode="HTML")
    except:
        s["total_msg_id"]=None
        save_data(data)

def send_info(cid,t): send_and_auto_delete(cid,t,10)

@bot.message_handler(commands=["ok","поехали"])
def cmd_ok(msg):
    cid=msg.chat.id
    delete_message_later(cid,msg.message_id,15)
    set_finance_mode(cid,True)
    save_data(data)
    send_info(cid,"Финансовый режим включён.\nОтправьте /start")

@bot.message_handler(commands=["start"])
def cmd_start(msg):
    cid=msg.chat.id
    delete_message_later(cid,msg.message_id,15)
    if not require_finance(cid): return
    dk=today_key()
    txt,_=render_day_window(cid,dk)
    kb=build_main_keyboard(dk,cid)
    sent=bot.send_message(cid,txt,reply_markup=kb,parse_mode="HTML")
    set_active_window_id(cid,dk,sent.message_id)
#🔵🔵🔵🔵🟡🟡🟡🟡🟡🟡🟡🟢🟢🎈🎈🎈🎈
@bot.message_handler(commands=["help"])
def cmd_help(msg):
    cid=msg.chat.id
    delete_message_later(cid,msg.message_id,15)
    if not is_finance_mode(cid):
        send_info(cid,"Финансовый режим выключен")
        return
    txt=(
        f"Бот — версия {VERSION}\n\n"
        "/ok, /поехали — включить\n"
        "/start — сегодня\n"
        "/view YYYY-MM-DD — день\n"
        "/prev — вчера\n"
        "/next — завтра\n"
        "/balance — баланс\n"
        "/report — отчёт\n"
        "/csv — CSV чата\n"
        "/json — JSON чата\n"
        "/reset — обнулить\n"
        "/stopforward — отключить пересылку\n"
        "/ping — жив?\n"
        "/backup_gdrive_on/off — GDrive\n"
        "/backup_channel_on/off — бэкап в канал\n"
        "/restore — режим восстановления\n"
        "/autoadd_info — авто-добавление\n"
    )
    send_info(cid,txt)

@bot.message_handler(commands=["restore"])
def cmd_restore(msg):
    global restore_mode
    restore_mode=True
    send_and_auto_delete(
        msg.chat.id,
        "Режим восстановления включён. Отправьте JSON/CSV."
    )

@bot.message_handler(commands=["restore_off"])
def cmd_restore_off(msg):
    global restore_mode
    restore_mode=False
    send_and_auto_delete(msg.chat.id,"Режим восстановления выключен.")

@bot.message_handler(commands=["ping"])
def cmd_ping(msg):
    send_info(msg.chat.id,"PONG")

@bot.message_handler(commands=["view"])
def cmd_view(msg):
    cid=msg.chat.id
    delete_message_later(cid,msg.message_id,15)
    if not require_finance(cid): return
    parts=(msg.text or "").split()
    if len(parts)<2:
        send_info(cid,"Использование: /view YYYY-MM-DD")
        return
    dk=parts[1]
    try: datetime.strptime(dk,"%Y-%m-%d")
    except:
        send_info(cid,"Неверная дата")
        return
    txt,_=render_day_window(cid,dk)
    kb=build_main_keyboard(dk,cid)
    sent=bot.send_message(cid,txt,reply_markup=kb,parse_mode="HTML")
    set_active_window_id(cid,dk,sent.message_id)

@bot.message_handler(commands=["prev"])
def cmd_prev(msg):
    cid=msg.chat.id
    delete_message_later(cid,msg.message_id,15)
    if not require_finance(cid): return
    d=datetime.strptime(today_key(),"%Y-%m-%d")-timedelta(days=1)
    dk=d.strftime("%Y-%m-%d")
    txt,_=render_day_window(cid,dk)
    kb=build_main_keyboard(dk,cid)
    sent=bot.send_message(cid,txt,reply_markup=kb,parse_mode="HTML")
    set_active_window_id(cid,dk,sent.message_id)

@bot.message_handler(commands=["next"])
def cmd_next(msg):
    cid=msg.chat.id
    delete_message_later(cid,msg.message_id,15)
    if not require_finance(cid): return
    d=datetime.strptime(today_key(),"%Y-%m-%d")+timedelta(days=1)
    dk=d.strftime("%Y-%m-%d")
    txt,_=render_day_window(cid,dk)
    kb=build_main_keyboard(dk,cid)
    sent=bot.send_message(cid,txt,reply_markup=kb,parse_mode="HTML")
    set_active_window_id(cid,dk,sent.message_id)

@bot.message_handler(commands=["balance"])
def cmd_balance(msg):
    cid=msg.chat.id
    delete_message_later(cid,msg.message_id,15)
    if not require_finance(cid): return
    s=get_chat_store(cid)
    send_info(cid,f"Баланс: {fmt_num(s.get('balance',0))}")

@bot.message_handler(commands=["report"])
def cmd_report(msg):
    cid=msg.chat.id
    delete_message_later(cid,msg.message_id,15)
    if not require_finance(cid): return
    s=get_chat_store(cid)
    lines=["Отчёт:"]
    for dk,recs in sorted(s.get("daily_records",{}).items()):
        sm=sum(r["amount"] for r in recs)
        lines.append(f"{dk}: {fmt_num(sm)}")
    send_info(cid,"\n".join(lines))

def cmd_csv_all(cid):
    if not require_finance(cid): return
    try:
        save_chat_json(cid)
        p=chat_csv_file(cid)
        if not os.path.exists(p):
            send_info(cid,"CSV ещё нет")
            return
        with open(p,"rb") as f:
            bot.send_document(cid,f,caption=f"CSV чата {cid}")
    except Exception as e:
        log_error(e)

def cmd_csv_day(cid,dk):
    if not require_finance(cid): return
    s=get_chat_store(cid)
    dr=s.get("daily_records",{}).get(dk,[])
    if not dr:
        send_info(cid,"Нет записей")
        return
    tmp=f"data_{cid}_{dk}.csv"
    try:
        with open(tmp,"w",newline="",encoding="utf-8") as f:
            w=csv.writer(f)
            w.writerow(["chat_id","ID","short_id","timestamp","amount","note","owner","day_key"])
            for r in dr:
                w.writerow([cid,r.get("id"),r.get("short_id"),r.get("timestamp"),
                            r.get("amount"),r.get("note"),r.get("owner"),dk])
        upload_to_gdrive(tmp)
        with open(tmp,"rb") as f:
            bot.send_document(cid,f,caption=f"CSV за день {dk}")
    finally:
        try: os.remove(tmp)
        except: pass

@bot.message_handler(commands=["csv"])
def _cmd_csv(msg):
    cid=msg.chat.id
    delete_message_later(cid,msg.message_id,15)
    if not require_finance(cid): return
    export_global_csv(data)
    save_chat_json(cid)
    p=chat_csv_file(cid)
    if os.path.exists(p):
        upload_to_gdrive(p)
        with open(p,"rb") as f:
            bot.send_document(cid,f,caption="CSV этого чата")
    send_backup_to_channel(cid)

@bot.message_handler(commands=["json"])
def _cmd_json(msg):
    cid=msg.chat.id
    delete_message_later(cid,msg.message_id,15)
    if not require_finance(cid): return
    save_chat_json(cid)
    p=chat_json_file(cid)
    if os.path.exists(p):
        with open(p,"rb") as f:
            bot.send_document(cid,f,caption="JSON чата")

@bot.message_handler(commands=["reset"])
def cmd_reset(msg):
    cid=msg.chat.id
    if not require_finance(cid): return
    s=get_chat_store(cid)
    s["reset_wait"]=True
    s["reset_time"]=time.time()
    save_data(data)
    send_and_auto_delete(cid,"Напишите ДА, чтобы подтвердить",15)
    schedule_cancel_wait(cid,15)

@bot.message_handler(commands=["stopforward"])
def cmd_stopforward(msg):
    clear_forward_all()
    send_info(msg.chat.id,"Пересылка отключена.")

@bot.message_handler(commands=["backup_gdrive_on"])
def cmd_on_drive(msg):
    backup_flags["drive"]=True
    save_data(data)
    send_info(msg.chat.id,"GDrive включён")

@bot.message_handler(commands=["backup_gdrive_off"])
def cmd_off_drive(msg):
    backup_flags["drive"]=False
    save_data(data)
    send_info(msg.chat.id,"GDrive выключен")

@bot.message_handler(commands=["backup_channel_on"])
def cmd_on_channel(msg):
    backup_flags["channel"]=True
    save_data(data)
    send_info(msg.chat.id,"Бэкап в канал включён")

@bot.message_handler(commands=["backup_channel_off"])
def cmd_off_channel(msg):
    backup_flags["channel"]=False
    save_data(data)
    send_info(msg.chat.id,"Бэкап в канал выключен")

@bot.message_handler(commands=["autoadd_info","autoadd.info"])
def cmd_autoadd_info(msg):
    cid=msg.chat.id
    delete_message_later(cid,msg.message_id,15)
    s=get_chat_store(cid)
    st=s.setdefault("settings",{})
    new=not st.get("auto_add",False)
    st["auto_add"]=new
    save_chat_json(cid)
    send_and_auto_delete(
        cid,
        f"Авто-добавление: {'ВКЛ' if new else 'ВЫКЛ'}"
    )

def send_and_auto_delete(cid,text,delay=10):
    try:
        m=bot.send_message(cid,text)
        def _del():
            time.sleep(delay)
            try: bot.delete_message(cid,m.message_id)
            except: pass
        threading.Thread(target=_del,daemon=True).start()
    except Exception as e:
        log_error(e)

def delete_message_later(cid,mid,delay=10):
    def _job():
        time.sleep(delay)
        try: bot.delete_message(cid,mid)
        except: pass
    threading.Thread(target=_job,daemon=True).start()

_edit_cancel_timers={}

def schedule_cancel_wait(cid,delay=15):
    def _job():
        try:
            s=get_chat_store(cid)
            ch=False
            w=s.get("edit_wait")
            if w and w.get("type")=="add":
                s["edit_wait"]=None; ch=True
            if s.get("reset_wait",False):
                s["reset_wait"]=False; s["reset_time"]=0; ch=True
            if ch: save_data(data)
        except: pass
    p=_edit_cancel_timers.get(cid)
    if p and p.is_alive():
        try: p.cancel()
        except: pass
    t=threading.Timer(delay,_job)
    _edit_cancel_timers[cid]=t
    t.start()

@bot.message_handler(content_types=["text"])
def handle_text(msg):
    try:
        cid=msg.chat.id
        text=(msg.text or "").strip()
        update_chat_info_from_message(msg)
        targets=resolve_forward_targets(cid)
        if targets:
            forward_text_anon(cid,msg,targets)
        s=get_chat_store(cid)
        w=s.get("edit_wait")
        auto=s.get("settings",{}).get("auto_add",False)
        do=False
        if w and w.get("type")=="add" and looks_like_amount(text):
            do=True; dk=w.get("day_key")
        elif auto and looks_like_amount(text):
            do=True; dk=s.get("current_view_day",today_key())
        if do:
            lines=text.split("\n"); added=False
            for line in lines:
                line=line.strip()
                if not line: continue
                try: amt,note=split_amount_and_note(line)
                except:
                    send_and_auto_delete(cid,f"Ошибка суммы: {line}")
                    continue
                rid=s.get("next_id",1)
                rec={
                    "id":rid,"short_id":f"R{rid}",
                    "timestamp":now_local().isoformat(timespec="seconds"),
                    "amount":amt,"note":note,
                    "owner":msg.from_user.id,
                    "msg_id":msg.message_id,
                    "origin_msg_id":msg.message_id,
                }
                s.setdefault("records",[]).append(rec)
                s.setdefault("daily_records",{}).setdefault(dk,[]).append(rec)
                s["next_id"]=rid+1
                added=True
            if added:
                update_or_send_day_window(cid,dk)
                schedule_finalize(cid,dk)
            s["balance"]=sum(x["amount"] for x in s.get("records",[]))
            data["records"]=[]
            for c2,st in data.get("chats",{}).items():
                data["records"].extend(st.get("records",[]))
            data["overall_balance"]=sum(x["amount"] for x in data["records"])
            save_data(data)
            save_chat_json(cid)
            export_global_csv(data)
            send_backup_to_channel(cid)
            s["edit_wait"]=None
            save_data(data)
            return
        if w and w.get("type")=="edit":
            rid=w.get("rid")
            dk=w.get("day_key",s.get("current_view_day",today_key()))
            lines=[ln.strip() for ln in text.split("\n") if ln.strip()]
            target=None
            for day,recs in s.get("daily_records",{}).items():
                for r in recs:
                    if r.get("id")==rid: target=r; dk=day; break
                if target: break
            if not target:
                send_and_auto_delete(cid,"Запись не найдена.")
                s["edit_wait"]=None
                return
            delete_record_in_chat(cid,rid)
            for line in lines:
                try: amt,note=split_amount_and_note(line)
                except:
                    bot.send_message(cid,f"Ошибка: {line}")
                    continue
                rid2=s.get("next_id",1)
                nr={
                    "id":rid2,"short_id":f"R{rid2}",
                    "timestamp":now_local().isoformat(timespec="seconds"),
                    "amount":amt,"note":note,
                    "owner":msg.from_user.id,
                    "msg_id":msg.message_id,
                    "origin_msg_id":msg.message_id,
                }
                s.setdefault("records",[]).append(nr)
                s.setdefault("daily_records",{}).setdefault(dk,[]).append(nr)
                s["next_id"]=rid2+1
            update_record_in_chat(cid,rid,amt,note)
            schedule_finalize(cid,dk)
            refresh_total_message_if_any(cid)
            s["edit_wait"]=None
            save_data(data)
            return
        if text.upper()=="ДА":
            if s.get("reset_wait",False) and (time.time()-s.get("reset_time",0)<=15):
                reset_chat_data(cid)
                send_and_auto_delete(cid,"Данные обнулены",15)
            else:
                send_and_auto_delete(cid,"Нет активного запроса")
            s["reset_wait"]=False; s["reset_time"]=0
            save_data(data)
            return
        if s.get("reset_wait",False):
            s["reset_wait"]=False; s["reset_time"]=0
            save_data(data)
    except Exception as e:
        log_error(f"handle_text: {e}")

def update_chat_info_from_message(msg):
    cid=msg.chat.id
    s=get_chat_store(cid)
    info=s.setdefault("info",{})
    info["title"]=msg.chat.title or info.get("title") or f"Чат {cid}"
    info["username"]=msg.chat.username or info.get("username")
    info["type"]=msg.chat.type
    kc=data.setdefault("known_chats",{})
    kc[str(cid)]={"title":info["title"],"username":info["username"],"type":info["type"]}
    save_chat_json(cid)

def delete_record_in_chat(cid,rid):
    s=get_chat_store(cid)
    s["records"]=[x for x in s["records"] if x["id"]!=rid]
    for day,arr in list(s.get("daily_records",{}).items()):
        arr2=[x for x in arr if x["id"]!=rid]
        if arr2: s["daily_records"][day]=arr2
        else: del s["daily_records"][day]
    renumber_chat_records(cid)
    s["balance"]=sum(x["amount"] for x in s["records"])
    data["records"]=[x for x in data.get("records",[]) if x["id"]!=rid]
    data["overall_balance"]=sum(x["amount"] for x in data["records"])
    save_data(data); save_chat_json(cid); export_global_csv(data); send_backup_to_channel(cid); send_backup_to_chat(cid)

def renumber_chat_records(cid):
    s=get_chat_store(cid)
    d=s.get("daily_records",{})
    all=[]
    for dk in sorted(d.keys()):
        recs=sorted(d[dk],key=lambda r:r.get("timestamp",""))
        d[dk]=recs
        for r in recs: all.append(r)
    i=1
    for r in all:
        r["id"]=i; r["short_id"]=f"R{i}"
        i+=1
    s["records"]=list(all); s["next_id"]=i

def update_record_in_chat(cid,rid,amt,note):
    s=get_chat_store(cid)
    f=None
    for r in s.get("records",[]):
        if r["id"]==rid:
            r["amount"]=amt; r["note"]=note; f=r; break
    if not f: return
    for day,arr in s.get("daily_records",{}).items():
        for r in arr:
            if r["id"]==rid: r.update(f)
    s["balance"]=sum(x["amount"] for x in s["records"])
    data["records"]=[x if x["id"]!=rid else f for x in data.get("records",[])]
    data["overall_balance"]=sum(x["amount"] for x in data["records"])
    save_data(data); save_chat_json(cid); export_global_csv(data)
    send_backup_to_channel(cid); send_backup_to_chat(cid)

def reset_chat_data(cid):
    s=get_chat_store(cid)
    s["balance"]=0
    s["records"]=[]
    s["daily_records"]={}
    s["next_id"]=1
    s["active_windows"]={}
    s["edit_wait"]=None
    s["edit_target"]=None
    save_data(data); save_chat_json(cid); export_global_csv(data)
    send_backup_to_channel(cid); send_backup_to_chat(cid)
    dk=s.get("current_view_day",today_key())
    update_or_send_day_window(cid,dk)
    refresh_total_message_if_any(cid)

@bot.message_handler(
    content_types=["photo","audio","video","voice","video_note","sticker","animation"]
)
def handle_media(msg):
    try:
        cid=msg.chat.id
        update_chat_info_from_message(msg)
        try: bot_id=bot.get_me().id
        except: bot_id=None
        if bot_id and msg.from_user and msg.from_user.id==bot_id: return
        targets=resolve_forward_targets(cid)
        if not targets: return
        grp=collect_media_group(cid,msg)
        if not grp: return
        if len(grp)>1:
            forward_media_group_anon(cid,grp,targets)
            return
        for dst,_ in targets:
            try: bot.copy_message(dst,cid,msg.message_id)
            except: pass
    except Exception as e:
        log_error(e)

@bot.message_handler(content_types=["location","contact","poll","venue"])
def handle_special(msg):
    global restore_mode
    if restore_mode: return
    try:
        cid=msg.chat.id
        update_chat_info_from_message(msg)
        try: bot_id=bot.get_me().id
        except: bot_id=None
        if bot_id and msg.from_user and msg.from_user.id==bot_id: return
        targets=resolve_forward_targets(cid)
        if not targets: return
        for dst,_ in targets:
            try: bot.copy_message(dst,cid,msg.message_id)
            except: pass
    except Exception as e:
        log_error(e)

@bot.message_handler(content_types=["document"])
def handle_doc(msg):
    global restore_mode,data
    cid=msg.chat.id
    update_chat_info_from_message(msg)
    f=msg.document
    name=(f.file_name or "").lower()
    if restore_mode:
        if not (name.endswith(".json") or name.endswith(".csv")):
            send_and_auto_delete(cid,f"Неверный файл {name}")
            return
        try:
            info=bot.get_file(f.file_id)
            raw=bot.download_file(info.file_path)
        except Exception as e:
            send_and_auto_delete(cid,f"Ошибка скачивания: {e}")
            return
        tmp=f"restore_{cid}_{name}"
        with open(tmp,"wb") as ff: ff.write(raw)
        if name=="data.json":
            try:
                os.replace(tmp,"data.json")
                data=load_data()
                restore_mode=False
                send_and_auto_delete(cid,"data.json восстановлен")
            except Exception as e:
                send_and_auto_delete(cid,f"Ошибка: {e}")
            return
        if name=="csv_meta.json":
            try:
                os.replace(tmp,"csv_meta.json")
                restore_mode=False
                send_and_auto_delete(cid,"csv_meta восстановлен")
            except Exception as e:
                send_and_auto_delete(cid,f"Ошибка: {e}")
            return
        if name.startswith("data_") and name.endswith(".json"):
            try:
                tgt=int(name.replace("data_","").replace(".json",""))
            except:
                send_and_auto_delete(cid,"Не определить chat_id")
                return
            try:
                os.replace(tmp,name)
                store=_load_json(name,{})
                if not store:
                    send_and_auto_delete(cid,"Файл пуст")
                    return
                store["balance"]=sum(r.get("amount",0) for r in store.get("records",[]))
                data.setdefault("chats",{})[str(tgt)]=store
                finance_active_chats.add(tgt)
                all=[]
                for _,st in data.get("chats",{}).items(): all.extend(st.get("records",[]))
                data["records"]=all
                data["overall_balance"]=sum(r.get("amount",0) for r in all)
                save_data(data); save_chat_json(tgt)
                update_or_send_day_window(tgt,today_key())
                restore_mode=False
                send_and_auto_delete(
                    cid,f"Чат {tgt} восстановлен. Записей: {len(store.get('records',[]))}"
                )
            except Exception as e:
                send_and_auto_delete(cid,f"Ошибка: {e}")
            return
        if name.startswith("data_") and name.endswith(".csv"):
            try:
                os.replace(tmp,name)
                restore_mode=False
                send_and_auto_delete(cid,f"CSV восстановлен {name}")
            except Exception as e:
                send_and_auto_delete(cid,f"Ошибка: {e}")
            return
        send_and_auto_delete(cid,f"Неподдерживаемый: {name}")
        return
    try:
        try: bot_id=bot.get_me().id
        except: bot_id=None
        if bot_id and msg.from_user and msg.from_user.id==bot_id: return
        targets=resolve_forward_targets(cid)
        if not targets: return
        grp=collect_media_group(cid,msg)
        if not grp: return
        if len(grp)>1:
            forward_media_group_anon(cid,grp,targets)
            return
        for dst,_ in targets:
            try: bot.copy_message(dst,cid,msg.message_id)
            except: pass
    except Exception as e:
        log_error(f"document: {e}")

@bot.edited_message_handler(content_types=["text"])
def handle_edit(msg):
    cid=msg.chat.id
    mid=msg.message_id
    new=(msg.text or "").strip()
    if not is_finance_mode(cid): return
    if restore_mode: return
    update_chat_info_from_message(msg)
    s=get_chat_store(cid)
    dk=today_key()
    target=None
    for day,recs in s.get("daily_records",{}).items():
        for r in recs:
            if r.get("msg_id")==mid or r.get("origin_msg_id")==mid:
                target=r; dk=day; break
        if target: break
    if not target: return
    try:
        amt,note=split_amount_and_note(new)
    except:
        bot.send_message(cid,"Ошибка суммы")
        return
    rid=target["id"]
    update_record_in_chat(cid,rid,amt,note)
    update_or_send_day_window(cid,dk)

@bot.message_handler(content_types=["deleted_message"])
def handle_deleted(msg):
    try:
        cid=msg.chat.id
        s=get_chat_store(cid)
        if s.get("reset_wait",False):
            s["reset_wait"]=False; s["reset_time"]=0
            save_data(data)
    except: pass

def schedule_finalize(cid,dk,delay=2.0):
    def _job():
        try:
            s=get_chat_store(cid)
            s["balance"]=sum(r.get("amount",0) for r in s.get("records",[]))
            all=[]
            for _,st in data.get("chats",{}).items():
                all.extend(st.get("records",[]))
            data["records"]=all
            data["overall_balance"]=sum(r.get("amount",0) for r in all)
            save_chat_json(cid); save_data(data); export_global_csv(data)
            send_backup_to_channel(cid); send_backup_to_chat(cid)
            old=get_active_window_id(cid,dk)
            txt,_=render_day_window(cid,dk)
            kb=build_main_keyboard(dk,cid)
            new=None
            try:
                m=bot.send_message(cid,txt,reply_markup=kb,parse_mode="HTML")
                new=m.message_id
                set_active_window_id(cid,dk,new)
            except:
                try:
                    update_or_send_day_window(cid,dk)
                    new=get_active_window_id(cid,dk)
                except: pass
            if old and new and old!=new:
                def _del():
                    time.sleep(1)
                    try: bot.delete_message(cid,old)
                    except: pass
                threading.Thread(target=_del,daemon=True).start()
            refresh_total_message_if_any(cid)
        except Exception as e:
            log_error(f"finalize: {e}")
    prev=_finalize_timers.get(cid)
    if prev and prev.is_alive():
        try: prev.cancel()
        except: pass
    t=threading.Timer(delay,_job)
    _finalize_timers[cid]=t
    t.start()

_finalize_timers={}

def keep_alive_task():
    while True:
        try:
            if APP_URL:
                try:
                    r=requests.get(APP_URL,timeout=10)
                except: pass
        except: pass
        time.sleep(max(10,KEEP_ALIVE_INTERVAL_SECONDS))

def start_keep_alive_thread():
    t=threading.Thread(target=keep_alive_task,daemon=True)
    t.start()

@app.route(f"/{BOT_TOKEN}",methods=["POST"])
def webhook():
    js=request.get_data().decode("utf-8")
    upd=telebot.types.Update.de_json(js)
    bot.process_new_updates([upd])
    return "OK",200

def set_webhook():
    if not APP_URL: return
    url=APP_URL.rstrip("/") + f"/{BOT_TOKEN}"
    bot.remove_webhook()
    time.sleep(0.5)
    bot.set_webhook(url=url)

def main():
    global data
    restore_from_gdrive_if_needed()
    data=load_data()
    set_webhook()
    start_keep_alive_thread()
    app.run(host="0.0.0.0",port=PORT)

if __name__=="__main__":
    main()
    