# DATA CONSTITUTION v196 — Protected Branches

Наследует DATA CONSTITUTION v195 без ослабления storage/restore правил.

## Новый слой: семантические защищённые ветки
- Владелец подтверждает проверенную ветку через `Инфо → 🌿 Ветки`.
- Подтверждение хранит snapshot семантического контракта: purpose, входы, flow, storage, dependencies, invariants, tests, contract hash, version/time.
- Галочка сохраняется в `_global_settings.protected_branches_registry` и уходит в обычный SQLite/MEGA backup/delta контур.
- Будущая версия должна явно перечислять изменённые функциональные ветки и иметь PASS регрессию для каждой изменённой ветки.
- Если текущая карточка защищённой ветки изменилась относительно подтверждённого snapshot, UI показывает `⚠️`, а изменение нельзя считать незаметным.
- Защита семантическая: hash исходника не является единственным контрактом; код можно оптимизировать без изменения поведения при сохранённых инвариантах/tests.
- Снять защиту можно только явным действием владельца в меню веток.

Все прежние правила `/restore REPLACE`, DATA CONSTITUTION, canonical MEGA root `/TelegramBotBackups`, ARS/USD isolation и durable finance остаются действующими.
