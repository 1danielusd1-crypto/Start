# bot_v180_total_final_diagnostics

## Диагностика
- TOTAL_DIAGNOSTICS включена по умолчанию для всех чатов и контуров.
- Общий журнал включается миграцией v180.
- Быстрый тест и Минимум НЕ меняют диагностические переключатели. Они выключаются только вручную.
- Добавлены callback/user/chat/contour context, worker-pool wait/exec, SQLite timings, MEGA timings, Telegram API timings, queue/runtime/memory/window tails.
- Полный журнал можно выгрузить прямо из страницы «🧪 Диагностика».

## Финализация
- Один callback-handler для всего бота.
- Один restore validator.
- v172/v174 message wrappers убраны: task input встроен в canonical on_any_message.
- Неиспользуемые callback registrar/interceptor функции удалены.
- Историческая restore-validator цепочка v153…v178 удалена; поддержка старых backup версий находится в одном validator.
- 0 повторных top-level def/class имён по всему runtime.
- Старые callback payload префиксы сохраняются только как compatibility decoder для уже открытых Telegram окон.

## MEGA
- Единственный корень устойчивого состояния: /TelegramBotBackups (или значение MEGA_BACKUP_DIR Render).
- /TelegramBotBackupsStart не используется.
