# DATA CONSTITUTION v197 — Chat Identity Authority

Наследует DATA CONSTITUTION v196 без ослабления storage/restore/finance/branch-protection правил.

## Каноническая идентичность чатов
- Роль владельца/пространства/финрежима никогда не подменяет Telegram-имя чата декоративным emoji или ID.
- Каноническое имя группы/супергруппы/канала = актуальный `title` из Telegram `getChat`.
- Каноническое имя private-чата = актуальные `first_name + last_name`, затем username, затем последнее валидное имя/ID fallback.
- `Пересылка → 📡 Проверить чаты` делает полную синхронизацию всех известных chat_id, включая основной owner-chat.
- При полной проверке сохраняются доступные боту title/username/type, Telegram-visible metadata, member count, bot membership/rights и administrator fingerprint, если API разрешает это получить.
- Проверка не переименовывает Telegram-чат: она только приводит локальный `chat.info`, `known_chats` и отображаемые меню к фактическому состоянию Telegram.
- Недоступность/удаление бота хранится отдельным статусом и не должна превращать корректное последнее название в имя владельца/emoji.
- Массовая проверка завершает изменения одним основным persist/config-backup контуром, чтобы не плодить лишние backup-задачи на каждое поле.

## Защищённые ветки
- Добавлена ветка `ui.chat_identity` с отдельным семантическим контрактом.
- В текущей версии затронута `forward.core`; её существующий контракт не изменён, regression = PASS.

Все прежние правила `/restore REPLACE`, `/TelegramBotBackups`, DATA CONSTITUTION, ARS/USD isolation, canonical Excel и protected branches остаются обязательными.


## v198 — OWNER TECHNICAL ALERT SCOPE
- DATA CONSTITUTION storage rules are unchanged.
- Technical alarms/🚨 are owner-private diagnostics: only primary `OWNER_ID` receives full technical text and internal W/Ф helper markers.
- Other contours may receive only a short functional rejection when their attempted business action is blocked; no internal codes or diagnostic details.
- Chat full-sync uses the single canonical extended `probe_bot_in_chat(deep,persist,schedule_backup)` authority; legacy lifecycle helper may not override it.
