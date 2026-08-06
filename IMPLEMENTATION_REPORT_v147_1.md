# Отчёт об исправлении bot_v147_1_startup_hotfix

## Ошибка
`NameError: name 'list_chat_ids' is not defined` при исполнении `ensure_default_tenant()` во время запуска.

## Исправление
- добавлена безопасная обёртка `list_chat_ids()`;
- tenant-инициализация перенесена после восстановления состояния;
- обновлена runtime-версия;
- выполнена статическая проверка всех модулей и FULL-файла;
- manifest пересчитан.

## Изменённые исходники
- `00_core.py`;
- `93_v147_extensions.py`;
- `bot.py`;
- `modules_manifest.json`;
- `FULL_bot_v147_1_startup_hotfix.py`.
