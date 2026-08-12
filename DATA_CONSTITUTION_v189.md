# DATA CONSTITUTION v189 — Telegram Bot

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


## Restore modes are strictly separated

- `/restore` is destructive **REPLACE FROM FILE** for the selected scope after `pre_restore`. It never merges live business data into the backup.
- `🩹 Аккуратное восстановление` is not a restore engine. It is a RAM-only manual replay helper. While enabled, every manually received and successfully parsed finance value is assigned to the day currently open in the main finance window.
- The mode auto-disables after 120 seconds without an accepted finance value; every accepted value resets the inactivity timer.
- Automatic bot-to-bot forwarding remains outside this temporary mode. When the mode is OFF, ordinary fresh finance messages keep normal date semantics.
- DATA CONSTITUTION ledger/snapshot/checkpoint protections remain active for every newly replayed finance operation.


## Restore forwarding invariant
Per-chat restore must restore forwarding edges exactly from the backup file. Current live incoming/outgoing edges for the restored chat are cleared first. Unrelated chats are not modified. Restore forwarding state must not invoke per-edge persistence/UI side effects.


## v189 — Single Main Window Authority

20. Each chat has exactly one authoritative finance main window: `primary_main_window_id` + `primary_main_window_day`.
21. `current_view_day` is UI navigation state only. Business finalizers, forwarded message timestamps, ordinary finance record dates, restore/delta workers, and background refreshes may not assign it.
22. A callback from a stale/older main window may not execute business logic. The stale window is closed and a fresh copy of the authoritative latest main window is sent.
23. Opening/navigating a main day atomically promotes that message/day and retires prior main windows. Historical main windows are never background-refreshed or resurrected.
24. `record.day_key` and main-window day are independent. In `🩹 Аккуратное восстановление`, the accepted record day is explicitly overridden by the current authoritative main-window day.
25. Manual restore re-anchors the compact-delta baseline only after the immutable restore generation/checkpoint succeeds. A pre-restore delta baseline may never survive a destructive restore.
26. Compact finance deltas must not carry the full growing `finance_integrity_v141.events` history. They carry only a compact chain head (`event_seq`, `tips`, `anchor`); immutable event detail remains in Constitution ledger/full generations.
27. UI-only window identifiers/state are excluded from critical finance deltas and persist through lower-priority UI/config persistence.

## v189 — Journal download names

28. Owner may define one persistent base filename in Journal UI. Full/current-version journal exports use that base plus type/date suffix. Filename input is control-plane input and must never be parsed as a finance value.
