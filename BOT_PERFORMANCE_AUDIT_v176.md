# bot_v176_process_control_center — performance map

## What was found

1. The runtime is a long ordered override chain. v176 has 46 executable modules, 64k+ source lines, 2237 unique top-level symbols and 227 symbols overridden later in the chain. CODEMAP_v176.json and ACTIVE_DEFINITIONS_v176.json resolve the final active implementation.
2. UI edit retry stacking is a concrete latency candidate. v160 `fast_ui_edit_message_text` may sleep for its per-window gap and retry once after Telegram 429; v161 `_v161_edit_retry` can then repeat the whole edit up to three times with additional sleeps. Under Telegram rate-limit/error conditions this can make a button feel much slower.
3. v171 wraps callbacks with `button_chain_press` / `button_chain_result`, while the base callback router also logs `button_pressed`. These are useful diagnostics but add work to every click.
4. Window diagnostics and window-registry persistence run around UI activity and can write/schedule extra state work.
5. The v175 single light switch was incomplete: it did not gate the recurring `v153-window-reconcile-loop`, and it did not gate v153 MEGA migration/runtime cleanup. v176 gates these separately.
6. The bot creates many isolated task pools. Isolation prevents one slow subsystem from blocking another, but on a small Render CPU the total thread count/context switching can still contribute to latency under load.

## How v176 diagnoses it

Open INFO -> `⚙️ Процессы / скорость`.

Recommended first experiment: `⚡ Быстрый тест`. It disables UI diagnostics/tracing and nonessential background work while keeping the locked business core, reminders, critical MEGA witness, instance lease and memory guard.

Then open `📊 Скорость кнопок`, perform 10-20 identical menu transitions, and compare median (P50), P90 and MAX. Re-enable one suspect at a time to identify the actual contributor on the live Render instance.

## Safety

The locked core page keeps Telegram webhook/update handling, finance, forwarding, SECRET, task dispatcher, SQLite and web keep-alive always enabled from this diagnostic menu. Critical MEGA delta remains switchable only as an explicit diagnostic item and is ON in all ready-made profiles.
