import os
import json
from datetime import datetime
from flask import Flask, request
import telebot
from telebot.types import InputMediaDocument

# ========================
# ENV
# ========================

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
APP_URL = os.getenv("APP_URL", "").strip()  # https://my-render-app.onrender.com
PORT = int(os.getenv("PORT", "8443"))

bot = telebot.TeleBot(BOT_TOKEN)
app = Flask(__name__)

META_FILE = "chat_backup_meta.json"


# ========================
# META helpers
# ========================

def load_meta():
    if not os.path.exists(META_FILE):
        return {}
    try:
        with open(META_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return {}


def save_meta(m):
    with open(META_FILE, "w", encoding="utf-8") as f:
        json.dump(m, f, ensure_ascii=False, indent=2)


def chat_json_file(chat_id: int) -> str:
    return f"data_{chat_id}.json"


# ========================
# BACKUP TO SAME CHAT
# ========================

def backup_to_same_chat(chat_id: int, payload: dict):
    meta = load_meta()

    msg_key = f"msg_chat_{chat_id}"
    ts_key = f"ts_chat_{chat_id}"

    path = chat_json_file(chat_id)

    # always save JSON file to disk
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    # 1) try edit existing backup message
    if msg_key in meta:
        try:
            with open(path, "rb") as f:
                bot.edit_message_media(
                    chat_id=chat_id,
                    message_id=meta[msg_key],
                    media=InputMediaDocument(f)
                )
            meta[ts_key] = datetime.now().isoformat(timespec="seconds")
            save_meta(meta)
            return
        except Exception as e:
            print("EDIT FAILED:", e)
            meta.pop(msg_key, None)
            meta.pop(ts_key, None)
            save_meta(meta)

    # 2) send new backup file
    with open(path, "rb") as f:
        msg = bot.send_document(chat_id, f, caption=f"Backup JSON for {chat_id}")

    meta[msg_key] = msg.message_id
    meta[ts_key] = datetime.now().isoformat(timespec="seconds")
    save_meta(meta)


# ========================
# TEXT HANDLER
# ========================

@bot.message_handler(content_types=["text"])
def handle(msg):
    chat_id = msg.chat.id

    payload = {
        "chat_id": chat_id,
        "last_text": msg.text,
        "timestamp": datetime.now().isoformat(timespec="seconds")
    }

    backup_to_same_chat(chat_id, payload)


# ========================
# WEBHOOK
# ========================

@app.route(f"/{BOT_TOKEN}", methods=["POST"])
def webhook():
    json_str = request.get_data().decode("utf-8")
    update = telebot.types.Update.de_json(json_str)
    bot.process_new_updates([update])
    return "OK", 200


def set_webhook():
    if not APP_URL:
        print("APP_URL не указан — работаю без webhook (polling отключён).")
        return

    wh_url = APP_URL.rstrip("/") + f"/{BOT_TOKEN}"

    try:
        bot.remove_webhook()
        import time
        time.sleep(0.5)
        bot.set_webhook(url=wh_url)
        print("Webhook установлен:", wh_url)
    except Exception as e:
        print("Ошибка установки webhook:", e)


# ========================
# MAIN
# ========================

if __name__ == "__main__":
    set_webhook()
    print("TEST BOT with Webhook is running...")
    app.run(host="0.0.0.0", port=PORT)