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
