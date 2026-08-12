# DATA CONSTITUTION v186 — Telegram Bot

## Immutable rules

1. Canonical durable root is `/TelegramBotBackups` (or the exact `MEGA_BACKUP_DIR` Render secret). No alternative `Start` root is allowed.
2. Immutable finance ledger + immutable SQLite generations are the durable source of truth. Render local files are disposable working state.
3. `latest_bot_state.sqlite3.gz` is compatibility only. Canonical boot restore is `database/current_manifest.json -> database/generations/generation_*.sqlite3.gz`.
4. A new generation is uploaded under a unique immutable name first. Only after technical + semantic validation may `current_manifest.json` move to it.
5. SQLite `integrity_check=ok` is insufficient. A snapshot must preserve finance record counts/history per chat, except for explicitly recorded delete events or an explicit manual restore.
6. Automatic deploy/restart/cleanup/performance code may never reduce finance history. An unexplained reduction activates DATA CONSTITUTION quarantine.
7. Quarantine blocks new finance mutations and automatic snapshot promotion. Durable recovery/restore tools remain available.
8. Every finance add/edit/delete/bulk-delete writes an immutable MEGA ledger event. Ledger write failure activates quarantine.
9. Bootstrap GENESIS anchors pre-constitution history, followed by the first immutable generation.
10. Deltas are not deleted immediately after a full snapshot. They remain an independent recovery path.
11. Manual `/restore` ALWAYS creates `pre_restore` first.
12. `/restore` is STRICT REPLACE FROM FILE. It never merges unique live records, current settings, or other current business state into the selected backup.
13. Per-chat JSON/ISON replaces exactly that chat scope from the file; unrelated chats remain untouched. Global JSON/GZ replaces global scope; tenant GZ replaces the selected tenant scope.
14. After restore, only derived indexes/balances/caches and compatible runtime defaults may be rebuilt. Saved business fields/order/IDs/short IDs are not rewritten merely to match current live state.
15. `pre_restore` is a rollback safety copy only; it is never automatically overlaid after restore.
16. Any merge must be a separately named explicit operation and may never be hidden inside `/restore`.
17. After manual restore, restore checkpoint + immutable generation are mandatory before guard/quarantine is cleared.
18. Storage-core functions in `11_data_constitution.py` are protected. UI/performance/diagnostic modules may not redefine them.
19. Release tests must verify compile, zero duplicate top-level callables, storage symbol uniqueness, semantic-loss rejection, strict restore contract, hashes/markers, and package re-extraction.

## Canonical MEGA layout

- `/TelegramBotBackups/ledger/finance/...` — immutable finance events
- `/TelegramBotBackups/ledger/finance/genesis/...` — genesis anchor
- `/TelegramBotBackups/ledger/finance/checkpoints/...` — manual restore checkpoints
- `/TelegramBotBackups/database/generations/generation_*.sqlite3.gz` — immutable full DB generations
- `/TelegramBotBackups/database/manifests/generation_*.json` — immutable semantic manifests
- `/TelegramBotBackups/database/current_manifest.json` — active generation pointer
- `/TelegramBotBackups/database/manifest_history/...` — previous pointers
- `/TelegramBotBackups/database/latest_bot_state.sqlite3.gz` — compatibility mirror only
- `/TelegramBotBackups/database/history/...` — compatibility history, keep >=24
- `/TelegramBotBackups/database/pre_restore/...` — safety copy before manual restore
- `/TelegramBotBackups/deltas/...` — delta recovery stream
