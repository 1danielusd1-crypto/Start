# DATA CONSTITUTION v193 — MEGA LIGHT / LIFECYCLE HARDENING

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


## v193 — crash ambiguity and terminal-state truth
- The v190 one-write hot path remains unchanged: the durable task is written once to `tasks/pending` before business execution; there is no mandatory foreground `pending -> running` write.
- Consequence: after an ungraceful crash, a remote `pending` file does **not** prove that business execution never started. It is an ambiguous witness.
- Startup recovery therefore performs effect verification and only idempotent repair. It never blindly repeats a non-idempotent callback, mutation command, SECRET source write, or Telegram forwarding send merely because the remote state is `pending`.
- Proven effects go through the normal finalizer. Ambiguous missing effects move to `failed/needs_review` so a human or a safe repair path can resolve them without a duplicate.
- A local `processed` marker is not sufficient on its own to move the MEGA task to `done`; the standard effect/ledger/critical-delta finalizer is still used.
- `delta_critical=OFF` is a deliberate loss of the required external witness and therefore cannot be reported as successful durable completion.

## v193 — process and export lifecycle
- Self-scheduling background processes that can be disabled must have explicit STOP/START semantics. Reminders and the durable-journal tick are cancelled when disabled and re-armed when enabled.
- Interactive file generation is single-flight. A concurrent request is registered as waiting and receives a terminal `ready`, `failed`, or delivered path after the active job finishes.
- Corrupt historical journal chunks are non-authoritative diagnostics: they are skipped with WARN/counters while valid chunks continue restoring. They never block business-state recovery.
