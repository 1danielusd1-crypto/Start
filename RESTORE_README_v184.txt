v184 FULL RESTORE CONTRACT

/restore accepts GZ / JSON / ISON / CSV.

JSON/ISON chat_full_backup:
- restores every persistent field stored for that chat scope;
- restores all saved settings_backup fields;
- restores ARS/USD gomonk settings, categories, currency/display modes, quick/hidden finance, auto-backup, journal/process settings, Excel settings, known chats, info, current view/panel state;
- restores forwarding edges that belong to the target chat and finance-active membership;
- platform owner may also restore saved global backup flags;
- backup rows win duplicate identities, but unique live rows are preserved so an older chat backup cannot delete newer finance operations.

Global JSON/ISON and GZ:
- restore the complete saved global/SQLite state;
- run one post-restore rehydrate path for runtime mirrors, forwarding index, tenants, balances and finance windows;
- current DB is saved to /TelegramBotBackups/database/pre_restore before apply.

Derived values (balance/daily indexes/global total) are recomputed and verified rather than blindly trusted.
