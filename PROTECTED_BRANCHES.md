# PROTECTED BRANCHES — карточки функций

Статус галочек хранится runtime в `_global_settings.protected_branches_registry` и попадает в скачиваемый из Telegram журнал веток.

## Правки текущей версии

- 🛠 **Чаты · реальные имена / полная проверка** (`ui.chat_identity`) — Проверить чаты теперь делает полную Telegram-синхронизацию всех известных чатов, включая основной owner-chat; убрана подмена имени владельца символом 🏀. Regression: **PASS**.
- 🛠 **Пересылка · правила/доставка** (`forward.core`) — Массовая проверка пересылки обновляет реальные chat title/username/type/access и сохраняет доступные bot-visible параметры одной синхронизацией. Regression: **PASS**.

## 💰 Финансы / ARS · основной учёт

**ID:** `finance.ars`  
**Contract:** r1  
**Суть:** Принимать, хранить и считать финансовые операции в аргентинских песо.

**Точки входа:**
- финансовый текст
- редактирование/удаление записи
- финансовые окна

**Рабочая цепочка:**
- ввод → распознавание
- record.amount → SQLite
- Constitution witness
- один finalize → UI

**Нельзя ломать:**
- record.amount — бухгалтерский источник истины ARS; старый текст не переразбирается для повторного расчёта
- USD не попадает в ARS-суммы
- одна логическая операция не создаёт дубль
- финансовая запись переживает restart/deploy через durable storage

**Регрессионные тесты:**
- ARS add/edit/delete
- duplicate guard
- restart replay
- ARS/USD isolation

**Зависимости:** storage.sqlite, storage.constitution, ui.main

## 💰 Финансы / USD · независимый учёт

**ID:** `finance.usd`  
**Contract:** r1  
**Суть:** Вести долларовые операции независимо от ARS.

**Точки входа:**
- USD финансовый ввод
- USD операции
- USD окна

**Рабочая цепочка:**
- USD ввод → USD record
- USD ledger
- USD итог/окно/export

**Нельзя ломать:**
- USD и ARS не складываются
- чистый USD не создаёт фиктивный ARS
- исторические ARS-клоны не считаются USD

**Регрессионные тесты:**
- pure USD
- mixed ARS+USD
- polluted legacy USD filter

**Зависимости:** finance.ars, storage.sqlite

## 💰 Финансы / Остаток / с ост

**ID:** `finance.balance`  
**Contract:** r2  
**Суть:** Показывать остаток начала дня и остаток после каждой операции.

**Точки входа:**
- кнопка «с ост»
- remaining_open:*
- переход день ←/→

**Рабочая цепочка:**
- выбранный день
- opening = закрытие предыдущего дня
- операции дня по порядку
- текущий остаток

**Нельзя ломать:**
- остаток с прошлого раза = фактический остаток на конец предыдущего дня
- финансовый текст не переразбирается для восстановления суммы
- переключение гомонковых не меняет ledger
- день окна не меняется фоновым finalize

**Регрессионные тесты:**
- previous-day carry
- 11.08→12.08 carry
- remaining rows
- gomonk ON/OFF idempotent

**Зависимости:** finance.ars, finance.gomonk, ui.main

## 💰 Финансы / Гомонковые ARS/USD

**ID:** `finance.gomonk`  
**Contract:** r2  
**Суть:** Отдельно учитывать резерв/гомонковые для ARS и USD.

**Точки входа:**
- кнопки гомонковых
- remaining SET ON/OFF
- Инфо

**Рабочая цепочка:**
- явное состояние ON/OFF
- вычет только при отображении/расчёте
- persist setting

**Нельзя ломать:**
- ARS/USD гомонковые независимы
- toggle идемпотентный SET, не слепая инверсия
- ledger не изменяется

**Регрессионные тесты:**
- SET ON twice
- SET OFF twice
- ARS/USD independence

**Зависимости:** finance.balance

## 💰 Финансы / Записи · редактирование/удаление

**ID:** `finance.records`  
**Contract:** r1  
**Суть:** Безопасно изменять существующие финансовые записи.

**Точки входа:**
- редактор записей
- edited Telegram message
- delete/bulk delete

**Рабочая цепочка:**
- выбор записи
- изменение
- integrity ledger
- rebuild derived state

**Нельзя ломать:**
- каждое изменение имеет witness
- удаление не затрагивает чужие записи
- derived indexes перестраиваются

**Регрессионные тесты:**
- edit
- delete
- bulk delete
- integrity event

**Зависимости:** finance.ars, storage.constitution

## 📊 Таблицы / Excel · единый ARS/USD

**ID:** `export.excel`  
**Contract:** r4  
**Суть:** Строить все XLSX из одного канонического набора финансовых данных.

**Точки входа:**
- Excel
- Excel статьи
- /tabl_lsx
- monthly XLSX
- Telegram download

**Рабочая цепочка:**
- period bounds
- canonical ARS/USD
- opening balance
- formulas
- OOXML validation
- delivery

**Нельзя ломать:**
- исправление Excel применяется ко всем Excel-путям и всем контурам
- ARS/USD рассчитываются отдельно
- остаток начала периода един для Excel/CSV/Google/UI
- USD формулы не ссылаются на ARS-блок
- операция с описанием «приход/расход» не является служебной итоговой строкой

**Регрессионные тесты:**
- all periods
- formula refs
- opening carry
- category override
- Telegram workbook

**Зависимости:** finance.ars, finance.usd, finance.balance

## 📊 Таблицы / CSV · единый расчёт

**ID:** `export.csv`  
**Contract:** r2  
**Суть:** Выгружать CSV с тем же каноническим расчётом, что Excel.

**Точки входа:**
- CSV день/неделя/месяц/Чт–Ср/всё

**Рабочая цепочка:**
- period → canonical records → opening/totals → file

**Нельзя ломать:**
- CSV не имеет отдельной бухгалтерской логики
- ARS/USD не смешиваются
- opening совпадает с Excel

**Регрессионные тесты:**
- period parity with Excel
- delivery

**Зависимости:** export.excel

## 📊 Таблицы / Google Sheets / Drive

**ID:** `export.google`  
**Contract:** r3  
**Суть:** Заливать тот же отчёт, который скачивается в Telegram.

**Точки входа:**
- Залить в Google Sheets
- Google Drive
- авто Чт–Ср

**Рабочая цепочка:**
- canonical workbook/data → Google upload → delivery proof

**Нельзя ломать:**
- Google и Telegram одного периода получают один источник данных
- Google не пересчитывает ошибочные межблочные ссылки
- успех подтверждается отдельно от Telegram send_document

**Регрессионные тесты:**
- Telegram/Google parity
- external delivery proof
- Thu-Wed 7 days

**Зависимости:** export.excel, export.csv

## 🪟 Интерфейс / Основное окно · единственный источник UI-даты

**ID:** `ui.main`  
**Contract:** r2  
**Суть:** Держать одно последнее основное финансовое окно на чат.

**Точки входа:**
- /start
- день ←/→
- календарь
- возврат в осн. окно

**Рабочая цепочка:**
- open day → primary main window
- new main retires old
- stale callback redirects

**Нельзя ломать:**
- одно каноническое основное окно
- старое окно не выполняет бизнес-действие
- finance finalize не меняет UI-день

**Регрессионные тесты:**
- stale main redirect
- day navigation
- background refresh does not resurrect old window

**Зависимости:** finance.ars

## 🪟 Интерфейс / Инфо / служебные меню

**ID:** `ui.info`  
**Contract:** r2  
**Суть:** Безопасно открывать диагностику, настройки и служебные функции.

**Точки входа:**
- ℹ️ Инфо
- служебные callbacks

**Рабочая цепочка:**
- Info → submenu → back/close

**Нельзя ломать:**
- права владельца соблюдаются
- служебное меню не меняет финансы
- ветки доступны из Инфо

**Регрессионные тесты:**
- owner menu
- back/close
- branches entry

**Зависимости:** ui.main, diagnostics.journal

## 🪟 Интерфейс / Чаты · реальные имена / полная проверка

**ID:** `ui.chat_identity`  
**Contract:** r1  
**Суть:** Хранить и показывать реальную Telegram-идентичность каждого известного чата и по кнопке полностью синхронизировать доступные изменения.

**Точки входа:**
- Пересылка → 📡 Проверить чаты
- getChat
- обычные Telegram updates

**Рабочая цепочка:**
- known chat ids
- getChat including primary owner
- canonical title/username/type
- bot-visible metadata/rights
- persist + one config backup

**Нельзя ломать:**
- роль владельца не подменяет имя чата emoji
- owner chat тоже проходит полную проверку
- группа/канал используют Telegram title, private — реальное имя/username
- проверка не переименовывает Telegram-чат — только синхронизирует локальную карточку
- недоступный чат помечается отдельно, его последнее известное имя не заменяется ID без необходимости

**Регрессионные тесты:**
- owner title replaces legacy basketball
- all-known includes owner
- group/private title authority
- metadata refresh
- removed/access state

**Зависимости:** forward.core, multitenant.core

## ♻️ Восстановление / /restore · REPLACE FROM FILE

**ID:** `restore.strict`  
**Contract:** r3  
**Суть:** Восстанавливать выбранный scope ровно из backup без merge с live-состоянием.

**Точки входа:**
- /restore
- JSON/ISON
- GZ/SQLite
- CSV finance

**Рабочая цепочка:**
- validate
- mandatory pre_restore
- clear scope
- replace from file
- rehydrate
- Constitution checkpoint

**Нельзя ломать:**
- никакого автоматического merge
- pre_restore обязателен
- восстановленный файл — источник истины
- derived caches можно пересчитать, бизнес-данные нельзя подмешать

**Регрессионные тесты:**
- JSON exact replace
- GZ schema validation
- forward edges exact
- pre_restore gate

**Зависимости:** storage.constitution, storage.mega

## ♻️ Восстановление / Аккуратное восстановление

**ID:** `restore.careful`  
**Contract:** r2  
**Суть:** Ручное дозаполнение отсутствующих дней после строгого restore.

**Точки входа:**
- Инфо → 🩹 Аккуратное восстановление
- ручной финансовый ввод

**Рабочая цепочка:**
- enable
- target = primary main day
- accepted finance → target day
- 120s inactivity → OFF

**Нельзя ломать:**
- не является merge
- target только канонический день main window
- restart выключает режим
- каждая принятая сумма продлевает 120s

**Регрессионные тесты:**
- target day
- timeout
- normal input outside mode

**Зависимости:** ui.main, finance.ars

## 💾 Хранилище / SQLite · рабочее состояние

**ID:** `storage.sqlite`  
**Contract:** r1  
**Суть:** Хранить материализованное рабочее состояние на текущем экземпляре Render.

**Точки входа:**
- load/save state
- snapshot

**Рабочая цепочка:**
- RAM ↔ SQLite
- snapshot → MEGA

**Нельзя ломать:**
- локальный Render disk не считается долговечным
- SQLite integrity проверяется
- low-RAM cold fields сохраняются

**Регрессионные тесты:**
- quick_check
- save/load
- cold field roundtrip

**Зависимости:** —

## 💾 Хранилище / MEGA · durable storage

**ID:** `storage.mega`  
**Contract:** r3  
**Суть:** Переживать deploy/restart и хранить долговечные snapshots/tasks/deltas.

**Точки входа:**
- durable witness
- delta
- generation
- runtime backup

**Рабочая цепочка:**
- small witness → background ledger/delta → verified full snapshot

**Нельзя ломать:**
- единственный canonical root /TelegramBotBackups
- пропавший root/path пересоздаётся
- пользователь не ждёт тяжёлый full snapshot

**Регрессионные тесты:**
- root self-heal
- one-put hot path
- restart recovery

**Зависимости:** storage.sqlite

## 💾 Хранилище / MEGA delta · compact WAL

**ID:** `storage.delta`  
**Contract:** r2  
**Суть:** Закрывать промежуток между полными SQLite generations маленькими изменениями.

**Точки входа:**
- state change
- scheduled delta

**Рабочая цепочка:**
- coalesce → compact payload → MEGA → prune after verified snapshot

**Нельзя ломать:**
- не копировать растущие operation ledger histories
- delta остаётся компактной
- не удалять recovery bridge до проверенного snapshot

**Регрессионные тесты:**
- real delta size
- coalescing
- retention

**Зависимости:** storage.mega, storage.constitution

## 💾 Хранилище / DATA CONSTITUTION

**ID:** `storage.constitution`  
**Contract:** r3  
**Суть:** Защищать финансовую историю от тихой семантической потери.

**Точки входа:**
- finance mutation
- snapshot publish
- boot restore
- manual restore

**Рабочая цепочка:**
- immutable witness
- semantic manifest
- generation
- quarantine on unexplained loss

**Нельзя ломать:**
- SQLite integrity недостаточно — нужна semantic completeness
- unexplained history loss → quarantine
- restore creates checkpoint/reanchor

**Регрессионные тесты:**
- semantic loss rejection
- generation fallback
- restore reanchor
- protected symbols

**Зависимости:** storage.sqlite, storage.mega

## 🔁 Пересылка / Пересылка · правила/доставка

**ID:** `forward.core`  
**Contract:** r1  
**Суть:** Пересылать разрешённый контент между настроенными чатами без дублей.

**Точки входа:**
- forward rules
- message router

**Рабочая цепочка:**
- source message → rule → durable task → destination

**Нельзя ломать:**
- нет дублей
- правила других чатов не повреждаются
- restore edges exact

**Регрессионные тесты:**
- single forward
- duplicate guard
- restore edges

**Зависимости:** storage.mega

## 🔁 Пересылка / Пересылка · media groups

**ID:** `forward.media`  
**Contract:** r1  
**Суть:** Собирать и доставлять Telegram media_group как логическую группу.

**Точки входа:**
- photo/video album

**Рабочая цепочка:**
- collect media_group → finalize → durable delivery

**Нельзя ломать:**
- группа не дробится на дубли
- restart не теряет durable delivery

**Регрессионные тесты:**
- album collect
- restart
- duplicate group

**Зависимости:** forward.core

## 📋 Задачи / Диспетчер задач

**ID:** `tasks.dispatcher`  
**Contract:** r1  
**Суть:** Создавать, вести и завершать задачи в выбранных чатах.

**Точки входа:**
- Task Dispatcher buttons
- task messages

**Рабочая цепочка:**
- create → active → action/result → close/archive

**Нельзя ломать:**
- каждая задача имеет начало/состояние/завершение
- выбор чатов соблюдается
- restart не теряет критическое состояние

**Регрессионные тесты:**
- create
- selected chats
- close
- restart

**Зависимости:** storage.mega, multitenant.core

## ⏰ Напоминания / Напоминания

**ID:** `reminders.core`  
**Contract:** r1  
**Суть:** Надёжно планировать и доставлять напоминания.

**Точки входа:**
- reminder commands/UI
- scheduler

**Рабочая цепочка:**
- create → scheduler → delivery → next/complete

**Нельзя ломать:**
- не теряются после restart
- не дублируются при replay
- очередь имеет завершение

**Регрессионные тесты:**
- one-shot
- recurring
- restart
- dedupe

**Зависимости:** storage.sqlite, storage.mega

## 🏢 Доступ / Пространства / круги / роли

**ID:** `multitenant.core`  
**Contract:** r1  
**Суть:** Изолировать владельца, пространства и доступные пользователям функции.

**Точки входа:**
- space menu
- permissions
- circle views

**Рабочая цепочка:**
- actor → tenant/role → permission → operation

**Нельзя ломать:**
- данные пространств не смешиваются
- глобальные исправления функций действуют во всех контурах где функция доступна
- owner-only операции закрыты

**Регрессионные тесты:**
- owner
- circle1
- circle2
- permission deny

**Зависимости:** —

## 🩺 Диагностика / Журналы / диагностика

**ID:** `diagnostics.journal`  
**Contract:** r2  
**Суть:** Фиксировать ошибки, события, скорость и давать скачиваемые журналы.

**Точки входа:**
- Инфо → Журнал
- runtime events
- download

**Рабочая цепочка:**
- event → runtime journal → optional MEGA → download

**Нельзя ломать:**
- диагностика не выключается Fast Test/Minimum автоматически
- имя скачиваемого журнала сохраняется
- битая remote строка не ломает runtime

**Регрессионные тесты:**
- current/full journal
- custom filename
- warm tail

**Зависимости:** storage.mega

## 🩺 Диагностика / Процессы / скорость

**ID:** `runtime.performance`  
**Contract:** r1  
**Суть:** Измерять UI latency и управлять необязательными тяжёлыми процессами.

**Точки входа:**
- Инфо → Процессы / скорость

**Рабочая цепочка:**
- toggle optional process → repeat transitions → P50/P90

**Нельзя ломать:**
- ядро бота нельзя выключить диагностикой
- финансы/пересылка/SQLite остаются core
- diagnostics remain available in test profiles

**Регрессионные тесты:**
- profile fast
- profile minimal
- locked core

**Зависимости:** diagnostics.journal

## 🔐 Прочее / SECRET / скрытые функции

**ID:** `secret.core`  
**Contract:** r1  
**Суть:** Сохранять штатную SECRET-функциональность и её доступы.

**Точки входа:**
- SECRET controls/messages

**Рабочая цепочка:**
- permission → action → persist

**Нельзя ломать:**
- не отключается меню диагностики
- доступы соблюдаются

**Регрессионные тесты:**
- permission
- persistence

**Зависимости:** multitenant.core

## 🛡 Защита / Ветки функций / контракты

**ID:** `system.branch_registry`  
**Contract:** r1  
**Суть:** Позволять владельцу фиксировать проверенные функциональные ветки как защищённые контракты.

**Точки входа:**
- Инфо → 🌿 Ветки
- галочка ветки
- скачать журнал веток

**Рабочая цепочка:**
- карточка → пользователь проверил → ✅ fix → persistent contract snapshot → regression gate

**Нельзя ломать:**
- галочка переживает restart/deploy/restore
- фиксируется семантический контракт, не только hash кода
- будущая смена contract rev видна как ⚠️
- снятие защиты — только явной кнопкой владельца

**Регрессионные тесты:**
- toggle/persist
- contract hash
- journal export
- future-version carry

**Зависимости:** ui.info, storage.sqlite, storage.mega
