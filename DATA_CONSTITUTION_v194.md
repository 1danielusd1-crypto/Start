# DATA CONSTITUTION v190 — MEGA LIGHT / FAST RECOVERY

## 1. Единственный устойчивый корень
Канонический корень MEGA: `/TelegramBotBackups` либо точное значение `MEGA_BACKUP_DIR` в Render.
Render-local SQLite — рабочая копия, но не единственный источник восстановления.

Если канонический корень или его подпапка были удалены/переименованы во время работы процесса, любой `mega-put` с ошибкой `Couldn't find destination folder` обязан:
1. сбросить process-lifetime cache путей;
2. пересоздать весь требуемый путь;
3. повторить запись ровно один раз;
4. если был заново создан сам канонический корень — поставить полный SQLite snapshot в фоновую очередь как можно скорее после освобождения пользовательских lanes.

Бот не угадывает произвольное новое имя вручную переименованной старой папки. Это архив; для автоматического восстановления после нового deploy старую папку нужно вернуть в каноническое имя либо явно восстановить из неё вручную.

## 2. Горячий путь пользователя
Критическая Telegram-операция до исполнения имеет один маленький write-before-execute witness:
`/TelegramBotBackups/tasks/pending/task_<update_id>.json`.

v190 запрещает лишний foreground round-trip `pending -> running`. Пока бизнес-логика выполняется, remote task остаётся `pending`; local process держит claim в RAM. После подтверждения результата task фоном переходит напрямую в `done`/`failed`.

Финансовый immutable ledger загружается вне пользовательского worker, если операция уже защищена durable task. Durable task не имеет права перейти в `done`, пока соответствующий ledger token не записан в MEGA.

## 3. Compact delta = короткий WAL, не вторая база
Delta содержит только изменения бизнес-состояния, необходимые после последнего SQLite snapshot.
Из delta исключаются растущие диагностические структуры:
- `operation_ledger_v141`;
- полная `finance_integrity_v141.events` (оставляется compact head);
- `chat_lifecycle_v150.history`.

Delta сериализуется compact JSON. Несколько быстрых изменений коалесцируются; приоритетная finance-delta по умолчанию не чаще примерно 12 секунд, обычная — 15 секунд.

Хранится не более 60 последних delta-файлов после проверенного full snapshot. Delta не удаляется до появления проверенного snapshot.

## 4. Проверенный SQLite snapshot
Полный SQLite создаётся в фоне:
- после ~120 секунд тишины;
- либо максимум примерно через 600 секунд непрерывных изменений;
- запуск откладывается, пока content/UI/finance lanes заняты.

Новый snapshot проходит SQLite integrity + DATA CONSTITUTION semantic checks. Урезанный snapshot не может стать активным.

## 5. Быстрый BOOT
`database/latest_bot_state.sqlite3.gz` снова является быстрым boot mirror, но только защищённым: он обновляется ПОСЛЕ успешного immutable generation + semantic acceptance.

Новые v190 snapshots содержат semantic manifest внутри SQLite (`data_constitution_snapshot/main`). Нормальный restart:
1. скачивает один `latest_bot_state.sqlite3.gz`;
2. проверяет SQLite + embedded semantic manifest;
3. применяет последующие compact deltas;
4. проверяет live state против локального semantic baseline;
5. переходит READY.

`current_manifest.json -> immutable generation` остаётся fallback/rollback, а не обязательным дополнительным сетевым round-trip на каждом нормальном boot.

## 6. Bounded history
По умолчанию:
- done durable tasks: 30;
- compact deltas: 60;
- immutable generations: 24 (настраивается 12–48);
- generation manifests: тот же предел;
- current-manifest history: тот же предел;
- legacy SQLite history: не менее 24.

Finance ledger event — write-ahead witness между поколениями. Событие разрешено удалить только когда его sequence уже покрыт предыдущим И текущим проверенными generations. Поэтому ledger не растёт бесконечно и не теряет единственный свежий witness.

## 7. Restore
`/restore` остаётся STRICT REPLACE FROM FILE:
- обязательный pre_restore;
- JSON/ISON/GZ восстанавливает ровно состояние файла в его scope;
- никакого merge с live state;
- rehydrate производных индексов;
- DATA CONSTITUTION checkpoint;
- новый проверенный generation.

`🩹 Аккуратное восстановление` — отдельный ручной режим дозаполнения и не меняет контракт `/restore`.

## 8. Неизменяемые запреты
- UI/performance/diagnostics не переопределяют storage-core.
- Нельзя продвигать snapshot только потому, что SQLite технически открывается.
- Нельзя удалять последнюю внешнюю точку восстановления до подтверждённого следующего поколения.
- Нельзя заставлять пользовательскую UI/finance lane ждать full snapshot, history prune, journal export или diagnostic MEGA work.
- `/TelegramBotBackupsStart` запрещён.


## v192 GZ compatibility clarification
`bot_version` inside a validated GZ is metadata, not a compatibility gate. Compatibility is determined by the stable restore schema/format and integrity/checksum checks. DATA CONSTITUTION storage semantics are otherwise unchanged from v190.


## v192 — Export integrity (non-destructive)
- Excel/Google export is a read-only projection of canonical financial state; it must not mutate finance records.
- ARS and USD projections are independent. Historical ARS-clone pollution in a USD snapshot is filtered at export time rather than destructively rewriting stored backups.
- Legacy ARS records may contribute to USD reports only through an explicit non-zero `usd_amount` component.
- Spreadsheet formulas are live formulas and the XLSX package requests full recalculation on open.
- Export delivery status must distinguish Telegram documents from Google Sheets/Drive external delivery.

- Legacy mixed currency rows are canonically re-parsed for export: a pure USD source contributes zero to ARS; a mixed source contributes only its parsed ARS component.


## v193 Excel / Google single-source invariant
- ARS and USD report sections are independent.
- When a prebuilt USD table is appended below ARS, every A1 formula reference is shifted by the exact row offset.
- A formula in the USD section is forbidden from referencing rows above the USD section; export fails closed.
- Telegram cached formula values and Google recalculation must represent the same business totals.
- Product/food totals honor category_override_slug exactly like expense category columns.


## v194 — TABLE / EXCEL CANONICAL FINANCIAL PROJECTION

- Every visible financial table (XLSX, CSV, Google Sheets, monthly table, /tabl_lsx) uses the same canonical currency projection.
- ARS and USD are independent ledgers for reporting.
- Opening balance is defined once: the sum of canonical movements of that currency strictly before the report boundary. For an exact ARS start record, earlier records on that same day are included and the selected record is excluded.
- The closing balance of a completed period must equal the opening balance of the immediately following period for the same currency.
- Legacy `record.amount`, `daily_records`, cached UI balance, or current UI currency cannot be independent sources for a visible table opening balance.
- Telegram and Google delivery may differ only in transport/formatting; financial rows and formulas must originate from the same builders.
- Lossless JSON/GZ backup data is not rewritten merely to normalize reporting.
