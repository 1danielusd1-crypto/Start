# bot_v178_global_performance_final

## Goal
Make UI latency improvements global for every available user contour, not owner-only.

## Static result
- Runtime modules: **46**
- Source lines: **65761**
- Duplicate top-level functions/classes: **0**
- Direct `bot.edit_message_text/reply_markup/caption` calls inside functions receiving `call`: **0**

## Global UI
`d:...:open/prev/next/today` now always renders through one `safe_edit` path for owner, first circle and second circle.

## MEGA
Critical durable writes receive higher command priority than runtime/journal diagnostics. `mega-whoami` and repeated `mega-mkdir` calls are cached. Remote diagnostic heartbeat defaults to 180 seconds.

## Background defaults
Detailed window/button diagnostics and historical failed-task scans are disabled once on migration to v178. They remain switchable from the owner's process center.

## Memory / threads
Auxiliary v166 pools are smaller on default config. Critical RAM pressure obeys the trim cooldown; emergency behavior is unchanged.
