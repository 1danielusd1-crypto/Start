import os
import io
import json
import csv
import re
import html
import logging
import threading
import time
from datetime import datetime,timedelta,timezone
from zoneinfo import ZoneInfo
import requests
import telebot
from telebot import types
from telebot.types import InputMediaPhoto,InputMediaVideo,InputMediaDocument,InputMediaAudio
from flask import Flask,request
from googleapiclient.http import MediaFileUpload,MediaIoBaseDownload
from googleapiclient.discovery import build
from google.oauth2 import service_account

BOT_TOKEN=os.getenv("BOT_TOKEN","").strip()
OWNER_ID=os.getenv("OWNER_ID","").strip()
BACKUP_CHAT_ID=os.getenv("BACKUP_CHAT_ID","").strip()
GOOGLE_SERVICE_ACCOUNT_JSON=os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON","").strip()
GDRIVE_FOLDER_ID=os.getenv("GDRIVE_FOLDER_ID","").strip()
APP_URL=os.getenv("APP_URL","").strip()
PORT=int(os.getenv("PORT","8443"))
if not BOT_TOKEN:raise RuntimeError("BOT_TOKEN is not set")
VERSION="Code_022.9.11 🎈с4-15/18/20"
DEFAULT_TZ="America/Argentina/Buenos_Aires"
KEEP_ALIVE_INTERVAL_SECONDS=60
DATA_FILE="data.json"
CSV_FILE="data.csv"
CSV_META_FILE="csv_meta.json"
backup_flags={"drive":True,"channel":True}
restore_mode=False
logging.basicConfig(level=logging.INFO,format="%(asctime)s [%(levelname)s] %(message)s")
logger=logging.getLogger(__name__)
bot=telebot.TeleBot(BOT_TOKEN,parse_mode=None)
app=Flask(__name__)
data={}
finance_active_chats=set()

def log_info(msg):logger.info(msg)
def log_error(msg):logger.error(msg)

def get_tz():
    try:return ZoneInfo(DEFAULT_TZ)
    except:return timezone(timedelta(hours=-3))

def now_local():return datetime.now(get_tz())
def today_key():return now_local().strftime("%Y-%m-%d")

def _load_json(path,default):
    if not os.path.exists(path):return default
    try:
        with open(path,"r",encoding="utf-8") as f:return json.load(f)
    except Exception as e:
        log_error(f"JSON load error {path}: {e}")
        return default

def _save_json(path,obj):
    try:
        with open(path,"w",encoding="utf-8") as f:json.dump(obj,f,ensure_ascii=False,indent=2)
    except Exception as e:log_error(f"JSON save error {path}: {e}")

def _load_csv_meta():return _load_json(CSV_META_FILE,{})
def _save_csv_meta(meta):
    try:_save_json(CSV_META_FILE,meta);log_info("csv_meta.json updated")
    except Exception as e:log_error(f"_save_csv_meta: {e}")

BASE_DIR=os.path.dirname(os.path.abspath(__file__))
CHAT_BACKUP_META_FILE=os.path.join(BASE_DIR,"chat_backup_meta.json")
log_info(f"chat_backup_meta.json PATH = {CHAT_BACKUP_META_FILE}")

def _load_chat_backup_meta():
    try:
        if not os.path.exists(CHAT_BACKUP_META_FILE):return {}
        return _load_json(CHAT_BACKUP_META_FILE,{})
    except Exception as e:
        log_error(f"_load_chat_backup_meta: {e}")
        return {}

def _save_chat_backup_meta(meta):
    try:
        log_info(f"SAVING META TO: {os.path.abspath(CHAT_BACKUP_META_FILE)}")
        _save_json(CHAT_BACKUP_META_FILE,meta)
        log_info("chat_backup_meta.json updated")
    except Exception as e:log_error(f"_save_chat_backup_meta: {e}")

def send_backup_to_chat(chat_id):
    try:
        if not chat_id:return
        try:save_chat_json(chat_id)
        except Exception as e:log_error(f"send_backup_to_chat save_chat_json({chat_id}): {e}")
        json_path=chat_json_file(chat_id)
        if not os.path.exists(json_path):
            log_error(f"send_backup_to_chat: {json_path} NOT FOUND")
            return
        meta=_load_chat_backup_meta()
        msg_key=f"msg_chat_{chat_id}"
        ts_key=f"timestamp_chat_{chat_id}"
        chat_title=_get_chat_title_for_backup(chat_id)
        caption=f"🧾 Авто-бэкап JSON чата: {chat_title}\n⏱ {now_local().strftime('%Y-%m-%d %H:%M:%S')}"
        def _open_file():
            try:
                with open(json_path,"rb") as f:data_bytes=f.read()
            except Exception as e:
                log_error(f"send_backup_to_chat open({json_path}): {e}")
                return None
            if not data_bytes:return None
            base=os.path.basename(json_path)
            name_no_ext,dot,ext=base.partition(".")
            suffix=get_chat_name_for_filename(chat_id)
            file_name=f"{name_no_ext}_{suffix}" if suffix else name_no_ext
            if dot:file_name+=f".{ext}"
            buf=io.BytesIO(data_bytes);buf.name=file_name
            return buf
        msg_id=meta.get(msg_key)
        if msg_id:
            fobj=_open_file()
            if not fobj:return
            try:
                bot.edit_message_media(chat_id=chat_id,message_id=msg_id,media=telebot.types.InputMediaDocument(fobj,caption=caption))
                meta[ts_key]=now_local().isoformat(timespec="seconds")
                _save_chat_backup_meta(meta)
                return
            except Exception as e:log_error(f"send_backup_to_chat edit FAILED in {chat_id}: {e}")
        fobj=_open_file()
        if not fobj:return
        sent=bot.send_document(chat_id,fobj,caption=caption)
        meta[msg_key]=sent.message_id
        meta[ts_key]=now_local().isoformat(timespec="seconds")
        _save_chat_backup_meta(meta)
    except Exception as e:log_error(f"send_backup_to_chat({chat_id}): {e}")

def default_data():
    return{
        "overall_balance":0,
        "records":[],
        "chats":{},
        "active_messages":{},
        "next_id":1,
        "backup_flags":{"drive":True,"channel":True},
        "finance_active_chats":{},
        "forward_rules":{}
    }

def load_data():
    d=_load_json(DATA_FILE,default_data())
    base=default_data()
    for k,v in base.items():
        if k not in d:d[k]=v
    flags=d.get("backup_flags") or {}
    backup_flags["drive"]=bool(flags.get("drive",True))
    backup_flags["channel"]=bool(flags.get("channel",True))
    fac=d.get("finance_active_chats") or {}
    finance_active_chats.clear()
    for cid,enabled in fac.items():
        if enabled:
            try:finance_active_chats.add(int(cid))
            except:pass
    return d

def save_data(d):
    fac={}
    for cid in finance_active_chats:fac[str(cid)]=True
    d["finance_active_chats"]=fac
    d["backup_flags"]={"drive":bool(backup_flags.get("drive",True)),"channel":bool(backup_flags.get("channel",True))}
    _save_json(DATA_FILE,d)
    
    def chat_json_file(chat_id):return f"data_{chat_id}.json"
def chat_csv_file(chat_id):return f"data_{chat_id}.csv"
def chat_meta_file(chat_id):return f"csv_meta_{chat_id}.json"

def get_chat_store(chat_id):
    chats=data.setdefault("chats",{})
    store=chats.setdefault(str(chat_id),{
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
    if "known_chats" not in store:store["known_chats"]={}
    return store

def save_chat_json(chat_id):
    try:
        store=data.get("chats",{}).get(str(chat_id))
        if not store:store=get_chat_store(chat_id)
        chat_path_json=chat_json_file(chat_id)
        chat_path_csv=chat_csv_file(chat_id)
        chat_path_meta=chat_meta_file(chat_id)
        for p in(chat_path_json,chat_path_csv,chat_path_meta):
            if not os.path.exists(p):
                with open(p,"a",encoding="utf-8"):pass
        payload={
            "chat_id":chat_id,
            "balance":store.get("balance",0),
            "records":store.get("records",[]),
            "daily_records":store.get("daily_records",{}),
            "next_id":store.get("next_id",1),
            "info":store.get("info",{}),
            "known_chats":store.get("known_chats",{})
        }
        _save_json(chat_path_json,payload)
        with open(chat_path_csv,"w",newline="",encoding="utf-8") as f:
            w=csv.writer(f)
            w.writerow(["chat_id","ID","short_id","timestamp","amount","note","owner","day_key"])
            daily=store.get("daily_records",{})
            for dk in sorted(daily.keys()):
                recs=daily.get(dk,[])
                recs_sorted=sorted(recs,key=lambda r:r.get("timestamp",""))
                for r in recs_sorted:
                    w.writerow([chat_id,r.get("id"),r.get("short_id"),r.get("timestamp"),
                    r.get("amount"),r.get("note"),r.get("owner"),dk])
        meta={
            "last_saved":now_local().isoformat(timespec="seconds"),
            "record_count":sum(len(v) for v in store.get("daily_records",{}).values())
        }
        _save_json(chat_path_meta,meta)
    except Exception as e:log_error(f"save_chat_json({chat_id}): {e}")

def fmt_num(x):
    sign="+" if x>=0 else "-"
    x=abs(x)
    s=f"{x:.12f}".rstrip("0").rstrip(".")
    if "." in s:int_part,dec_part=s.split(".")
    else:int_part,dec_part=s,""
    int_part=f"{int(int_part):,}".replace(",",".")
    s=f"{int_part},{dec_part}" if dec_part else int_part
    return f"{sign}{s}"

num_re=re.compile(r"[+\-–]?\s*\d[\d\s.,_'’]*")

def parse_amount(raw):
    s=raw.strip()
    is_negative=s.startswith("-") or s.startswith("–")
    is_positive=s.startswith("+")
    s_clean=s.lstrip("+-–").strip().replace(" ","").replace("_","").replace("’","").replace("'","")
    if "," not in s_clean and "." not in s_clean:
        value=float(s_clean)
        if not is_positive and not is_negative:is_negative=True
        return -value if is_negative else value
    if "." in s_clean and "," in s_clean:
        if s_clean.rfind(",")>s_clean.rfind("."):
            s_clean=s_clean.replace(".","").replace(",",".")
        else:s_clean=s_clean.replace(",","")
    else:
        if "," in s_clean:
            pos=s_clean.rfind(",")
            if len(s_clean)-pos-1 in(1,2):
                s_clean=s_clean.replace(".","").replace(",",".")
            else:s_clean=s_clean.replace(",","")
        elif "." in s_clean:
            pos=s_clean.rfind(".")
            if len(s_clean)-pos-1 in(1,2):s_clean=s_clean.replace(",","")
            else:s_clean=s_clean.replace(".","")
    value=float(s_clean)
    if not is_positive and not is_negative:is_negative=True
    return -value if is_negative else value

def split_amount_and_note(text):
    m=num_re.search(text)
    if not m:raise ValueError("no number found")
    raw_number=m.group(0)
    amount=parse_amount(raw_number)
    note=text.replace(raw_number," ").strip()
    note=re.sub(r"\s+"," ",note).lower()
    return amount,note

def looks_like_amount(text):
    try:split_amount_and_note(text);return True
    except:return False

def _get_drive_service():
    if not GOOGLE_SERVICE_ACCOUNT_JSON or not GDRIVE_FOLDER_ID:return None
    try:
        info=json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
        creds=service_account.Credentials.from_service_account_info(info,
            scopes=["https://www.googleapis.com/auth/drive"])
        service=build("drive","v3",credentials=creds)
        return service
    except Exception as e:log_error(f"Drive service error: {e}");return None

def upload_to_gdrive(path,mime_type=None,description=None):
    flags=backup_flags or {}
    if not flags.get("drive",True):return
    service=_get_drive_service()
    if service is None:return
    if not os.path.exists(path):return
    fname=os.path.basename(path)
    file_metadata={"name":fname,"parents":[GDRIVE_FOLDER_ID],
                   "description":description or ""}
    media=MediaFileUpload(path,mimetype=mime_type,resumable=True)
    try:
        existing=service.files().list(
            q=f"name = '{fname}' and '{GDRIVE_FOLDER_ID}' in parents and trashed = false",
            spaces="drive",fields="files(id,name)").execute()
        items=existing.get("files",[])
        if items:
            file_id=items[0]["id"]
            service.files().update(fileId=file_id,media_body=media,
                body={"description":description or ""}).execute()
        else:
            created=service.files().create(body=file_metadata,
                media_body=media,fields="id").execute()
    except Exception as e:log_error(f"upload_to_gdrive({path}): {e}")

def download_from_gdrive(filename,dest_path):
    service=_get_drive_service()
    if service is None:return False
    try:
        res=service.files().list(
            q=f"name = '{filename}' and '{GDRIVE_FOLDER_ID}' in parents and trashed = false",
            spaces="drive",fields="files(id,name,mimeType,size)").execute()
        items=res.get("files",[])
        if not items:return False
        file_id=items[0]["id"]
        request=service.files().get_media(fileId=file_id)
        fh=io.FileIO(dest_path,"wb")
        downloader=MediaIoBaseDownload(fh,request)
        done=False
        while not done:status,done=downloader.next_chunk()
        return True
    except Exception as e:log_error(f"download_from_gdrive({filename}): {e}");return False

def restore_from_gdrive_if_needed():
    restored=False
    if not os.path.exists(DATA_FILE):
        if download_from_gdrive(os.path.basename(DATA_FILE),DATA_FILE):restored=True
    if not os.path.exists(CSV_FILE):
        if download_from_gdrive(os.path.basename(CSV_FILE),CSV_FILE):restored=True
    if not os.path.exists(CSV_META_FILE):
        if download_from_gdrive(os.path.basename(CSV_META_FILE),CSV_META_FILE):restored=True
    return restored

def export_global_csv(d):
    try:
        with open(CSV_FILE,"w",newline="",encoding="utf-8") as f:
            w=csv.writer(f)
            w.writerow(["chat_id","ID","short_id","timestamp","amount","note","owner","day_key"])
            for cid,cdata in d.get("chats",{}).items():
                for dk,records in cdata.get("daily_records",{}).items():
                    for r in records:
                        w.writerow([cid,r.get("id"),r.get("short_id"),
                        r.get("timestamp"),r.get("amount"),
                        r.get("note"),r.get("owner"),dk])
    except Exception as e:log_error(f"export_global_csv: {e}")

EMOJI_DIGITS={"0":"0️⃣","1":"1️⃣","2":"2️⃣","3":"3️⃣","4":"4️⃣","5":"5️⃣","6":"6️⃣","7":"7️⃣","8":"8️⃣","9":"9️⃣"}
backup_channel_notified_chats=set()
def format_chat_id_emoji(chat_id):return "".join(EMOJI_DIGITS.get(ch,ch) for ch in str(chat_id))

def _safe_chat_title_for_filename(title):
    if not title:return ""
    title=str(title).strip().replace(" ","_")
    title=re.sub(r"[^0-9A-Za-zА-Яа-я_\-]+","",title)
    return title[:32]

def get_chat_name_for_filename(chat_id):
    try:
        store=get_chat_store(chat_id)
        info=store.get("info",{})
        username=info.get("username")
        title=info.get("title")
        base=username.lstrip("@") if username else title if title else str(chat_id)
        return _safe_chat_title_for_filename(base)
    except Exception as e:
        log_error(f"get_chat_name_for_filename({chat_id}): {e}")
        return _safe_chat_title_for_filename(str(chat_id))

def _get_chat_title_for_backup(chat_id):
    try:
        store=get_chat_store(chat_id)
        info=store.get("info",{})
        title=info.get("title")
        if title:return title
    except Exception as e:log_error(f"_get_chat_title_for_backup({chat_id}): {e}")
    return f"chat_{chat_id}"
    
    def send_backup_to_channel_for_file(base_path,meta_key_prefix,chat_title=None):
    if not BACKUP_CHAT_ID:return
    if not os.path.exists(base_path):
        log_error(f"send_backup_to_channel_for_file: {base_path} not found")
        return
    try:
        meta=_load_csv_meta()
        msg_key=f"msg_{meta_key_prefix}"
        ts_key=f"timestamp_{meta_key_prefix}"
        base_name=os.path.basename(base_path)
        name_without_ext,dot,ext=base_name.partition(".")
        safe_title=_safe_chat_title_for_filename(chat_title)
        if safe_title:
            file_name=f"{name_without_ext}_{safe_title}"
            if dot:file_name+=f".{ext}"
        else:file_name=base_name
        caption=f"📦 {file_name} — {now_local().strftime('%Y-%m-%d %H:%M')}"
        def _open_for_telegram():
            if not os.path.exists(base_path):
                log_error(f"send_backup_to_channel_for_file: {base_path} not found")
                return None
            with open(base_path,"rb") as src:data_bytes=src.read()
            if not data_bytes:return None
            buf=io.BytesIO(data_bytes);buf.name=file_name;buf.seek(0)
            return buf
        if meta.get(msg_key):
            try:
                fobj=_open_for_telegram()
                if not fobj:return
                bot.edit_message_media(chat_id=int(BACKUP_CHAT_ID),message_id=meta[msg_key],
                media=telebot.types.InputMediaDocument(fobj,caption=caption))
            except Exception as e:
                log_error(f"edit_message_media {base_path}: {e}")
                try:bot.delete_message(int(BACKUP_CHAT_ID),meta[msg_key])
                except Exception as del_e:log_error(f"delete_message {base_path}: {del_e}")
                fobj=_open_for_telegram()
                if not fobj:return
                sent=bot.send_document(int(BACKUP_CHAT_ID),fobj,caption=caption)
                meta[msg_key]=sent.message_id
        else:
            fobj=_open_for_telegram()
            if not fobj:return
            sent=bot.send_document(int(BACKUP_CHAT_ID),fobj,caption=caption)
            meta[msg_key]=sent.message_id
        meta[ts_key]=now_local().isoformat(timespec="seconds")
        _save_csv_meta(meta)
    except Exception as e:log_error(f"send_backup_to_channel_for_file({base_path}): {e}")

def send_backup_to_channel(chat_id):
    try:
        if not BACKUP_CHAT_ID:return
        if not backup_flags.get("channel",True):return
        try:backup_chat_id=int(BACKUP_CHAT_ID)
        except:return
        save_chat_json(chat_id)
        export_global_csv(data)
        save_data(data)
        chat_title=_get_chat_title_for_backup(chat_id)
        if chat_id not in backup_channel_notified_chats:
            try:
                emoji_id=format_chat_id_emoji(chat_id)
                bot.send_message(backup_chat_id,emoji_id)
                backup_channel_notified_chats.add(chat_id)
            except Exception as e:log_error(f"send_backup_to_channel: {e}")
        json_path=chat_json_file(chat_id)
        csv_path=chat_csv_file(chat_id)
        send_backup_to_channel_for_file(json_path,f"json_{chat_id}",chat_title)
        send_backup_to_channel_for_file(csv_path,f"csv_{chat_id}",chat_title)
    except Exception as e:log_error(f"send_backup_to_channel({chat_id}): {e}")

def _owner_data_file():
    if not OWNER_ID:return None
    try:return f"data_{int(OWNER_ID)}.json"
    except:return None

def load_forward_rules():
    try:
        path=_owner_data_file()
        if not path or not os.path.exists(path):return {}
        payload=_load_json(path,{}) or {}
        fr=payload.get("forward_rules",{})
        upgraded={}
        for src,value in fr.items():
            if isinstance(value,list):
                upgraded[src]={}
                for dst in value:upgraded[src][dst]="oneway_to"
            elif isinstance(value,dict):upgraded[src]=value
        return upgraded
    except Exception as e:log_error(f"load_forward_rules: {e}");return {}

def persist_forward_rules_to_owner():
    try:
        path=_owner_data_file()
        if not path:return
        payload={}
        if os.path.exists(path):
            payload=_load_json(path,{})
            if not isinstance(payload,dict):payload={}
        payload["forward_rules"]=data.get("forward_rules",{})
        _save_json(path,payload)
    except Exception as e:log_error(f"persist_forward_rules_to_owner: {e}")

def resolve_forward_targets(source_chat_id):
    fr=data.get("forward_rules",{})
    src=str(source_chat_id)
    if src not in fr:return []
    out=[]
    for dst,mode in fr[src].items():
        try:out.append((int(dst),mode))
        except:continue
    return out

def add_forward_link(src_chat_id,dst_chat_id,mode):
    fr=data.setdefault("forward_rules",{})
    src=str(src_chat_id);dst=str(dst_chat_id)
    fr.setdefault(src,{})[dst]=mode
    save_data(data)

def remove_forward_link(src_chat_id,dst_chat_id):
    fr=data.get("forward_rules",{})
    src=str(src_chat_id);dst=str(dst_chat_id)
    if src in fr and dst in fr[src]:del fr[src][dst]
    if src in fr and not fr[src]:del fr[src]
    save_data(data)

def clear_forward_all():
    data["forward_rules"]={}
    persist_forward_rules_to_owner()
    save_data(data)

def forward_text_anon(source_chat_id,msg,targets):
    for dst,mode in targets:
        try:bot.copy_message(dst,source_chat_id,msg.message_id)
        except Exception as e:log_error(f"forward_text_anon to {dst}: {e}")

def forward_media_anon(source_chat_id,msg,targets):
    for dst,mode in targets:
        try:bot.copy_message(dst,source_chat_id,msg.message_id)
        except Exception as e:log_error(f"forward_media_anon to {dst}: {e}")

_media_group_cache={}
def collect_media_group(chat_id,msg):
    gid=msg.media_group_id
    if not gid:return[msg]
    group=_media_group_cache.setdefault(chat_id,{})
    arr=group.setdefault(gid,[])
    arr.append(msg)
    if len(arr)==1:time.sleep(0.2)
    complete=group.pop(gid,arr)
    return complete

def forward_media_group_anon(source_chat_id,messages,targets):
    if not messages:return
    media_list=[]
    for msg in messages:
        if msg.content_type=="photo":
            file_id=msg.photo[-1].file_id
            caption=msg.caption or None
            media_list.append(InputMediaPhoto(file_id,caption=caption))
        elif msg.content_type=="video":
            file_id=msg.video.file_id
            caption=msg.caption or None
            media_list.append(InputMediaVideo(file_id,caption=caption))
        elif msg.content_type=="document":
            file_id=msg.document.file_id
            caption=msg.caption or None
            media_list.append(InputMediaDocument(file_id,caption=caption))
        elif msg.content_type=="audio":
            file_id=msg.audio.file_id
            caption=msg.caption or None
            media_list.append(InputMediaAudio(file_id,caption=caption))
        else:
            for dst,mode in targets:
                try:bot.copy_message(dst,source_chat_id,msg.message_id)
                except:pass
            return
    for dst,mode in targets:
        try:bot.send_media_group(dst,media_list)
        except Exception as e:log_error(f"forward_media_group_anon to {dst}: {e}")

def render_day_window(chat_id,day_key):
    store=get_chat_store(chat_id)
    recs=store.get("daily_records",{}).get(day_key,[])
    lines=[]
    lines.append(f"📅 <b>{day_key}</b>")
    lines.append("")
    total_income=0.0
    total_expense=0.0
    recs_sorted=sorted(recs,key=lambda x:x.get("timestamp"))
    for r in recs_sorted:
        amt=r["amount"]
        if amt>=0:total_income+=amt
        else:total_expense+=-amt
        note=html.escape(r.get("note",""))
        sid=r.get("short_id",f"R{r['id']}")
        lines.append(f"{sid} {fmt_num(amt)} <i>{note}</i>")
    if not recs_sorted:lines.append("Нет записей за этот день.")
    lines.append("")
    if recs_sorted:
        lines.append(f"📉 Расход за день: {fmt_num(-total_expense) if total_expense else fmt_num(0)}")
        lines.append(f"📈 Приход за день: {fmt_num(total_income) if total_income else fmt_num(0)}")
    bal_chat=store.get("balance",0)
    lines.append(f"🏦 Остаток по чату: {fmt_num(bal_chat)}")
    total=total_income-total_expense
    return "\n".join(lines),total

def build_main_keyboard(day_key,chat_id=None):
    kb=types.InlineKeyboardMarkup(row_width=3)
    kb.row(types.InlineKeyboardButton("➕ Добавить",callback_data=f"d:{day_key}:add"),
           types.InlineKeyboardButton("📝 Редактировать",callback_data=f"d:{day_key}:edit_menu"))
    kb.row(types.InlineKeyboardButton("⬅️ Вчера",callback_data=f"d:{day_key}:prev"),
           types.InlineKeyboardButton("📅 Сегодня",callback_data=f"d:{day_key}:today"),
           types.InlineKeyboardButton("➡️ Завтра",callback_data=f"d:{day_key}:next"))
    kb.row(types.InlineKeyboardButton("📅 Календарь",callback_data=f"d:{day_key}:calendar"),
           types.InlineKeyboardButton("📊 Отчёт",callback_data=f"d:{day_key}:report"))
    kb.row(types.InlineKeyboardButton("ℹ️ Инфо",callback_data=f"d:{day_key}:info"),
           types.InlineKeyboardButton("💰 Общий итог",callback_data=f"d:{day_key}:total"))
    return kb

def build_calendar_keyboard(center_day,chat_id=None):
    kb=types.InlineKeyboardMarkup(row_width=4)
    daily={}
    if chat_id is not None:
        store=get_chat_store(chat_id)
        daily=store.get("daily_records",{})
    start_day=center_day-timedelta(days=15)
    for week in range(0,32,4):
        row=[]
        for d in range(4):
            day=start_day+timedelta(days=week+d)
            label=day.strftime("%d.%m")
            key=day.strftime("%Y-%m-%d")
            if daily.get(key):label="📝 "+label
            row.append(types.InlineKeyboardButton(label,callback_data=f"d:{key}:open"))
        kb.row(*row)
    kb.row(types.InlineKeyboardButton("⬅️ −31",callback_data=f"c:{(center_day-timedelta(days=31)).strftime('%Y-%m-%d')}"),
           types.InlineKeyboardButton("➡️ +31",callback_data=f"c:{(center_day+timedelta(days=31)).strftime('%Y-%m-%d')}"))
    kb.row(types.InlineKeyboardButton("📅 Сегодня",callback_data=f"d:{today_key()}:open"))
    return kb
    
    def build_edit_menu_keyboard(chat_id,day_key):
    store=get_chat_store(chat_id)
    recs=store.get("daily_records",{}).get(day_key,[])
    kb=types.InlineKeyboardMarkup(row_width=1)
    recs_sorted=sorted(recs,key=lambda x:x.get("timestamp",""))
    for r in recs_sorted:
        sid=r.get("short_id",f"R{r['id']}")
        amt=fmt_num(r.get("amount",0))
        note=(r.get("note") or "").lower()
        kb.row(types.InlineKeyboardButton(f"{sid} {amt} {note}",callback_data=f"e:{day_key}:{r['id']}"))
    kb.row(types.InlineKeyboardButton("⬅️ Назад",callback_data=f"d:{day_key}:open"))
    return kb

def build_edit_record_keyboard(record_id,day_key):
    kb=types.InlineKeyboardMarkup(row_width=2)
    kb.row(types.InlineKeyboardButton("✏️ Изменить",callback_data=f"eu:{day_key}:{record_id}"),
           types.InlineKeyboardButton("🗑 Удалить",callback_data=f"ed:{day_key}:{record_id}"))
    kb.row(types.InlineKeyboardButton("⬅️ Назад",callback_data=f"d:{day_key}:edit_menu"))
    return kb

def update_chat_info_from_message(msg):
    try:
        cid=msg.chat.id
        store=get_chat_store(cid)
        info=store.setdefault("info",{})
        if msg.chat.type in("group","supergroup"):
            info["title"]=msg.chat.title or ""
            info["username"]=msg.chat.username or None
        elif msg.chat.type=="channel":
            info["title"]=msg.chat.title or ""
            info["username"]=msg.chat.username or None
        else:
            user=msg.from_user
            info["title"]=f"{user.first_name or ''} {user.last_name or ''}".strip()
            info["username"]=user.username or None
    except Exception as e:log_error(f"update_chat_info_from_message: {e}")

def update_known_chats(owner_store,msg):
    try:
        cid=msg.chat.id
        info=owner_store.setdefault("known_chats",{}).setdefault(str(cid),{})
        if msg.chat.type in("group","supergroup"):
            info["title"]=msg.chat.title or ""
            info["username"]=msg.chat.username or None
        elif msg.chat.type=="channel":
            info["title"]=msg.chat.title or ""
            info["username"]=msg.chat.username or None
        else:
            u=msg.from_user
            info["title"]=f"{u.first_name or ''} {u.last_name or ''}".strip()
            info["username"]=u.username or None
    except Exception as e:log_error(f"update_known_chats: {e}")

def send_day_window(chat_id,day_key):
    text,total=render_day_window(chat_id,day_key)
    kb=build_main_keyboard(day_key,chat_id)
    store=get_chat_store(chat_id)
    aw=store.setdefault("active_windows",{})
    msg_id=aw.get(day_key)
    try:
        if msg_id:
            msg=bot.edit_message_text(text,chat_id,msg_id,reply_markup=kb,parse_mode="HTML")
        else:
            msg=bot.send_message(chat_id,text,reply_markup=kb,parse_mode="HTML")
            aw[day_key]=msg.message_id
        return msg,total
    except Exception as e:
        log_error(f"send_day_window({chat_id},{day_key}): {e}")
        try:
            msg=bot.send_message(chat_id,text,reply_markup=kb,parse_mode="HTML")
            aw[day_key]=msg.message_id
            return msg,total
        except Exception as e2:
            log_error(f"send_day_window fallback({chat_id}): {e2}")
            return None,total

def add_record_to_day(chat_id,amount,note,day_key):
    store=get_chat_store(chat_id)
    record_id=store.get("next_id",1)
    store["next_id"]=record_id+1
    sid=f"R{record_id}"
    ts=now_local().isoformat(timespec="seconds")
    r={"id":record_id,"short_id":sid,"timestamp":ts,"amount":amount,"note":note,"owner":chat_id,"day_key":day_key}
    daily=store.setdefault("daily_records",{})
    arr=daily.setdefault(day_key,[])
    arr.append(r)
    store["records"].append(r)
    store["balance"]=store.get("balance",0)+amount
    save_chat_json(chat_id)
    save_data(data)
    send_backup_to_chat(chat_id)
    send_backup_to_channel(chat_id)
    return r

def update_record(chat_id,record_id,new_amount,new_note,day_key):
    store=get_chat_store(chat_id)
    daily=store.get("daily_records",{})
    arr=daily.get(day_key,[])
    rec=None
    for r in arr:
        if r["id"]==record_id:rec=r;break
    if not rec:return False
    diff=new_amount-rec["amount"]
    store["balance"]=store.get("balance",0)+diff
    rec["amount"]=new_amount
    rec["note"]=new_note
    rec["timestamp"]=now_local().isoformat(timespec="seconds")
    save_chat_json(chat_id)
    save_data(data)
    send_backup_to_chat(chat_id)
    send_backup_to_channel(chat_id)
    return True

def delete_record(chat_id,record_id,day_key):
    store=get_chat_store(chat_id)
    daily=store.get("daily_records",{})
    arr=daily.get(day_key,[])
    new_arr=[]
    removed=None
    for r in arr:
        if r["id"]==record_id:removed=r
        else:new_arr.append(r)
    daily[day_key]=new_arr
    if removed:store["balance"]=store.get("balance",0)-removed.get("amount",0)
    save_chat_json(chat_id)
    save_data(data)
    send_backup_to_chat(chat_id)
    send_backup_to_channel(chat_id)
    return removed is not None

def handle_edit_menu(call,chat_id,day_key):
    kb=build_edit_menu_keyboard(chat_id,day_key)
    try:bot.edit_message_reply_markup(chat_id,call.message.message_id,reply_markup=kb)
    except Exception as e:log_error(f"handle_edit_menu: {e}")

def handle_edit_record(call,chat_id,day_key,record_id):
    kb=build_edit_record_keyboard(record_id,day_key)
    try:
        bot.edit_message_text(f"Редактирование записи {record_id}",chat_id,call.message.message_id,
            reply_markup=kb,parse_mode="HTML")
    except Exception as e:log_error(f"handle_edit_record: {e}")

def ask_new_value_for_update(chat_id,record_id,day_key):
    store=get_chat_store(chat_id)
    store["edit_wait"]="update_value"
    store["edit_target"]={"record_id":record_id,"day_key":day_key}
    save_data(data)
    bot.send_message(chat_id,"Введите новую сумму и заметку (формат: amount note)")

def ask_confirm_delete(chat_id,record_id,day_key):
    store=get_chat_store(chat_id)
    store["edit_wait"]="delete_confirm"
    store["edit_target"]={"record_id":record_id,"day_key":day_key}
    save_data(data)
    bot.send_message(chat_id,"Напишите ДА для удаления записи.")

def handle_info_request(chat_id):
    store=get_chat_store(chat_id)
    info=store.get("info",{})
    username=info.get("username")
    title=info.get("title")
    bal=store.get("balance",0)
    txt=f"Информация о чате:\nНазвание: {title or '-'}\nUsername: @{username}" if username else f"Информация о чате:\nНазвание: {title or '-'}\nUsername: -"
    txt+=f"\nБаланс: {fmt_num(bal)}"
    bot.send_message(chat_id,txt)

def handle_total_request(chat_id):
    d=data.get("chats",{})
    total=0
    for cid,cdata in d.items():total+=cdata.get("balance",0)
    bot.send_message(chat_id,f"💰 Общий итог по всем чатам: {fmt_num(total)}")

def handle_report_request(chat_id,day_key):
    store=get_chat_store(chat_id)
    daily=store.get("daily_records",{})
    keys=sorted(daily.keys())
    lines=["📊 Отчёт по дням:"]
    for k in keys:
        arr=daily.get(k,[])
        total=0
        for r in arr:total+=r.get("amount",0)
        lines.append(f"{k}: {fmt_num(total)}")
    bot.send_message(chat_id,"\n".join(lines))
    
    @app.route("/"+BOT_TOKEN,methods=["POST"])
def webhook():
    try:
        update=telebot.types.Update.de_json(request.stream.read().decode("utf-8"))
        bot.process_new_updates([update])
    except Exception as e:log_error(f"webhook: {e}")
    return "OK",200

@app.route("/")
def index():return "Bot is running!",200

def keep_alive_thread():
    while True:
        try:
            bot.send_chat_action(OWNER_ID,"typing")
            time.sleep(KEEP_ALIVE_INTERVAL_SECONDS)
        except:time.sleep(KEEP_ALIVE_INTERVAL_SECONDS)

@bot.message_handler(commands=["start","поехали"])
def cmd_start(message):
    chat_id=message.chat.id
    update_chat_info_from_message(message)
    finance_active_chats.add(chat_id)
    save_data(data)
    bot.send_message(chat_id,"Финансовый учёт включён.\nОтправьте сумму, чтобы добавить запись.")
    send_day_window(chat_id,today_key())

@bot.message_handler(commands=["stop"])
def cmd_stop(message):
    chat_id=message.chat.id
    if chat_id in finance_active_chats:finance_active_chats.remove(chat_id)
    save_data(data)
    bot.send_message(chat_id,"Финансовый учёт отключён.")

@bot.callback_query_handler(func=lambda c:True)
def on_callback(call):
    try:
        chat_id=call.message.chat.id
        update_chat_info_from_message(call.message)
        if call.data.startswith("d:"):
            _,day_key,action=call.data.split(":")
            if action=="add":
                bot.answer_callback_query(call.id,"Введите сумму и заметку")
            elif action=="edit_menu":
                handle_edit_menu(call,chat_id,day_key)
            elif action=="prev":
                prev_day=(datetime.strptime(day_key,"%Y-%m-%d")-timedelta(days=1)).strftime("%Y-%m-%d")
                send_day_window(chat_id,prev_day)
            elif action=="next":
                next_day=(datetime.strptime(day_key,"%Y-%m-%d")+timedelta(days=1)).strftime("%Y-%m-%d")
                send_day_window(chat_id,next_day)
            elif action=="today":
                send_day_window(chat_id,today_key())
            elif action=="calendar":
                center=datetime.strptime(day_key,"%Y-%m-%d")
                kb=build_calendar_keyboard(center,chat_id)
                bot.edit_message_reply_markup(chat_id,call.message.message_id,reply_markup=kb)
            elif action=="report":
                handle_report_request(chat_id,day_key)
            elif action=="info":
                handle_info_request(chat_id)
            elif action=="total":
                handle_total_request(chat_id)
            elif action=="open":
                send_day_window(chat_id,day_key)
        elif call.data.startswith("c:"):
            _,ckey=call.data.split(":")
            center=datetime.strptime(ckey,"%Y-%m-%d")
            kb=build_calendar_keyboard(center,chat_id)
            bot.edit_message_reply_markup(chat_id,call.message.message_id,reply_markup=kb)
        elif call.data.startswith("e:"):
            _,day_key,rec_id_s=call.data.split(":")
            handle_edit_record(call,chat_id,day_key,int(rec_id_s))
        elif call.data.startswith("eu:"):
            _,day_key,rec_id_s=call.data.split(":")
            ask_new_value_for_update(chat_id,int(rec_id_s),day_key)
        elif call.data.startswith("ed:"):
            _,day_key,rec_id_s=call.data.split(":")
            ask_confirm_delete(chat_id,int(rec_id_s),day_key)
    except Exception as e:log_error(f"on_callback: {e}")

@bot.message_handler(content_types=["text"])
def on_text(message):
    chat_id=message.chat.id
    update_chat_info_from_message(message)
    if chat_id not in finance_active_chats:return
    store=get_chat_store(chat_id)
    if store.get("edit_wait")=="update_value":
        tgt=store.get("edit_target") or {}
        rid=tgt.get("record_id")
        dk=tgt.get("day_key")
        try:
            amt,note=split_amount_and_note(message.text)
            if update_record(chat_id,rid,amt,note,dk):
                bot.send_message(chat_id,"Запись обновлена.")
                send_day_window(chat_id,dk)
            else:bot.send_message(chat_id,"Не удалось обновить запись.")
        except Exception as e:
            bot.send_message(chat_id,f"Ошибка: {e}")
        store["edit_wait"]=None
        store["edit_target"]=None
        save_data(data)
        return
    if store.get("edit_wait")=="delete_confirm":
        tgt=store.get("edit_target") or {}
        rid=tgt.get("record_id")
        dk=tgt.get("day_key")
        if message.text.strip().lower()=="да":
            if delete_record(chat_id,rid,dk):
                bot.send_message(chat_id,"Запись удалена.")
                send_day_window(chat_id,dk)
            else:bot.send_message(chat_id,"Не удалось удалить запись.")
        else:bot.send_message(chat_id,"Удаление отменено.")
        store["edit_wait"]=None
        store["edit_target"]=None
        save_data(data)
        return
    if looks_like_amount(message.text):
        try:
            amt,note=split_amount_and_note(message.text)
            dk=today_key()
            r=add_record_to_day(chat_id,amt,note,dk)
            send_day_window(chat_id,dk)
        except Exception as e:
            bot.send_message(chat_id,f"Ошибка суммы: {message.text}\n{e}")
    else:
        owner_store=get_chat_store(int(OWNER_ID)) if OWNER_ID else None
        if owner_store:update_known_chats(owner_store,message)
        targets=resolve_forward_targets(chat_id)
        if targets:
            if message.media_group_id:
                arr=collect_media_group(chat_id,message);forward_media_group_anon(chat_id,arr,targets)
            else:
                if message.content_type=="text":forward_text_anon(chat_id,message,targets)
                elif message.content_type in("photo","video","document","audio"):
                    forward_media_anon(chat_id,message,targets)

@bot.message_handler(content_types=["audio","photo","video","document","sticker","voice","location","contact"])
def on_media(message):
    chat_id=message.chat.id
    update_chat_info_from_message(message)
    owner_store=get_chat_store(int(OWNER_ID)) if OWNER_ID else None
    if owner_store:update_known_chats(owner_store,message)
    targets=resolve_forward_targets(chat_id)
    if targets:
        if message.media_group_id:
            arr=collect_media_group(chat_id,message);forward_media_group_anon(chat_id,arr,targets)
        else:forward_media_anon(chat_id,message,targets)

def init_webhook():
    try:
        bot.remove_webhook()
        time.sleep(1)
        webhook_url=f"{APP_URL}/{BOT_TOKEN}"
        bot.set_webhook(url=webhook_url)
    except Exception as e:log_error(f"init_webhook: {e}")

restore_from_gdrive_if_needed()
data=load_data()
if APP_URL:init_webhook()
threading.Thread(target=keep_alive_thread,daemon=True).start()

if __name__=="__main__":
    try:
        if OWNER_ID:
            bot.send_message(
                int(OWNER_ID),
                f"🚀 Бот запущен.\nВерсия: {VERSION}"
            )
    except:
        pass
    app.run(host="0.0.0.0",port=PORT)
    
    