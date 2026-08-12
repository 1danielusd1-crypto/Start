# BOT FUNCTION MAP

Версия: `bot_v196_protected_branches_final`

Эта карта описывает семантические ветки бота. Код внутри ветки можно рефакторить, но защищённый контракт и регрессионные тесты должны сохраняться.

## Дерево функций

### 💰 Финансы
- **ARS · основной учёт** (`finance.ars`, contract r1) — Принимать, хранить и считать финансовые операции в аргентинских песо. Зависимости: storage.sqlite, storage.constitution, ui.main.
- **USD · независимый учёт** (`finance.usd`, contract r1) — Вести долларовые операции независимо от ARS. Зависимости: finance.ars, storage.sqlite.
- **Остаток / с ост** (`finance.balance`, contract r2) — Показывать остаток начала дня и остаток после каждой операции. Зависимости: finance.ars, finance.gomonk, ui.main.
- **Гомонковые ARS/USD** (`finance.gomonk`, contract r2) — Отдельно учитывать резерв/гомонковые для ARS и USD. Зависимости: finance.balance.
- **Записи · редактирование/удаление** (`finance.records`, contract r1) — Безопасно изменять существующие финансовые записи. Зависимости: finance.ars, storage.constitution.

### 📊 Таблицы
- **Excel · единый ARS/USD** (`export.excel`, contract r4) — Строить все XLSX из одного канонического набора финансовых данных. Зависимости: finance.ars, finance.usd, finance.balance.
- **CSV · единый расчёт** (`export.csv`, contract r2) — Выгружать CSV с тем же каноническим расчётом, что Excel. Зависимости: export.excel.
- **Google Sheets / Drive** (`export.google`, contract r3) — Заливать тот же отчёт, который скачивается в Telegram. Зависимости: export.excel, export.csv.

### 🪟 Интерфейс
- **Основное окно · единственный источник UI-даты** (`ui.main`, contract r2) — Держать одно последнее основное финансовое окно на чат. Зависимости: finance.ars.
- **Инфо / служебные меню** (`ui.info`, contract r2) — Безопасно открывать диагностику, настройки и служебные функции. Зависимости: ui.main, diagnostics.journal.

### ♻️ Восстановление
- **/restore · REPLACE FROM FILE** (`restore.strict`, contract r3) — Восстанавливать выбранный scope ровно из backup без merge с live-состоянием. Зависимости: storage.constitution, storage.mega.
- **Аккуратное восстановление** (`restore.careful`, contract r2) — Ручное дозаполнение отсутствующих дней после строгого restore. Зависимости: ui.main, finance.ars.

### 💾 Хранилище
- **SQLite · рабочее состояние** (`storage.sqlite`, contract r1) — Хранить материализованное рабочее состояние на текущем экземпляре Render. Зависимости: —.
- **MEGA · durable storage** (`storage.mega`, contract r3) — Переживать deploy/restart и хранить долговечные snapshots/tasks/deltas. Зависимости: storage.sqlite.
- **MEGA delta · compact WAL** (`storage.delta`, contract r2) — Закрывать промежуток между полными SQLite generations маленькими изменениями. Зависимости: storage.mega, storage.constitution.
- **DATA CONSTITUTION** (`storage.constitution`, contract r3) — Защищать финансовую историю от тихой семантической потери. Зависимости: storage.sqlite, storage.mega.

### 🔁 Пересылка
- **Пересылка · правила/доставка** (`forward.core`, contract r1) — Пересылать разрешённый контент между настроенными чатами без дублей. Зависимости: storage.mega.
- **Пересылка · media groups** (`forward.media`, contract r1) — Собирать и доставлять Telegram media_group как логическую группу. Зависимости: forward.core.

### 📋 Задачи
- **Диспетчер задач** (`tasks.dispatcher`, contract r1) — Создавать, вести и завершать задачи в выбранных чатах. Зависимости: storage.mega, multitenant.core.

### ⏰ Напоминания
- **Напоминания** (`reminders.core`, contract r1) — Надёжно планировать и доставлять напоминания. Зависимости: storage.sqlite, storage.mega.

### 🏢 Доступ
- **Пространства / круги / роли** (`multitenant.core`, contract r1) — Изолировать владельца, пространства и доступные пользователям функции. Зависимости: —.

### 🩺 Диагностика
- **Журналы / диагностика** (`diagnostics.journal`, contract r2) — Фиксировать ошибки, события, скорость и давать скачиваемые журналы. Зависимости: storage.mega.
- **Процессы / скорость** (`runtime.performance`, contract r1) — Измерять UI latency и управлять необязательными тяжёлыми процессами. Зависимости: diagnostics.journal.

### 🔐 Прочее
- **SECRET / скрытые функции** (`secret.core`, contract r1) — Сохранять штатную SECRET-функциональность и её доступы. Зависимости: multitenant.core.

### 🛡 Защита
- **Ветки функций / контракты** (`system.branch_registry`, contract r1) — Позволять владельцу фиксировать проверенные функциональные ветки как защищённые контракты. Зависимости: ui.info, storage.sqlite, storage.mega.

