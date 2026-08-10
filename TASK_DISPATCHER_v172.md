# 📋 Диспетчер задач v172

## Модель данных

Каждая задача имеет постоянный `uid` и последовательный `number` внутри одного `chat_id`.
`number` предназначен людям, `uid` — для безопасной внутренней адресации. Изменение порядка, статуса и фильтра не меняет UID.

Основные поля: `uid`, `number`, `chat_id`, `type`, `title`, `description`, `object`, `creator_user_id`, `creator_name`, `source_message_id`, `source_author_name`, `assignees`, `status`, `priority`, `deadline`, `cost`, `comments`, `history`, `created_at`, `updated_at`, `completed_at`, `deleted`.

## Типы

`task`: new → work → wait → deferred → done.

`purchase`: need → search → ordered → bought → received.

## Надёжность

Task registry хранится в root maps:

- `_tasks_v172`: uid → task;
- `_task_settings_v172`: chat_id → enabled/next_number;
- `_task_source_index_v172`: chat_id:message_id → uid.

Все три карты добавлены в `_DELTA_ROOT_MAP_KEYS`. Это позволяет существующему MEGA delta-механизму сохранять только изменённые элементы карты. Локальная рабочая копия немедленно пишется в SQLite root.

## Разделение

Любой список сначала фильтруется по `task.chat_id`. Данные другого чата/контура не участвуют в карточке, поиске, объектах и исполнителях текущего чата.

## Права

Создавать задачу можно в чате с включённым диспетчером. Любой участник может взять задачу себе и менять рабочий статус. Текст/назначение других исполнителей/удаление ограничены автором задачи или управляющим/владельцем.

## Следующий слой

После live-проверки архитектура готова к deadline reminders, ежедневным сводкам, вложениям, Google Sheets и статистике без смены UID/основной модели данных.
