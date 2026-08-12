# DATA CONSTITUTION v195 — BALANCE AUTHORITY / REMAINING FINAL

Version: `bot_v195_balance_authority_remaining_final`

This release keeps the v190 MEGA LIGHT / DATA CONSTITUTION durability model unchanged and adds a financial reporting invariant.

## Immutable accounting invariant

1. For ARS, the stored accounting `amount` in the active ARS ledger is the financial fact. Historical `source_finance_text` MUST NOT be re-parsed to recalculate past ARS amounts during Excel, CSV, Google or Remaining rendering.
2. `Остаток с прошлого раза` means the ARS balance immediately before the selected period/day and MUST be identical in the main day accounting, Remaining window, XLSX, CSV and Google Sheets.
3. USD remains a fully separate currency ledger. No USD amount may enter ARS opening, expense, income or closing formulas.
4. Derived indexes/caches may be rebuilt from the ledger, but may not reinterpret old notes to change stored money.

## Remaining window invariant

`Остаток после каждого расхода` starts from the same canonical opening used by all exports. Every line applies the stored transaction amount in chronological order. `with gomonk` changes only the displayed reserve deduction; it does not change the accounting balance.

Gomonk mode buttons carry the desired state (`0` or `1`) instead of a blind toggle. Repeated or delayed Telegram callbacks therefore remain idempotent.

## Formula invariant

Summary labels (`Приход`, `Расход`, `Остаток на руках`, etc.) are resolved from the bottom summary block. A transaction whose description happens to be `приход` or `расход` must never be formulaized as a summary row. Circular `#REF!` output is prohibited.

## Existing durability rules preserved

- canonical MEGA root `/TelegramBotBackups`;
- Render local state is disposable;
- strict `/restore` = REPLACE FROM FILE with pre_restore;
- immutable generations / semantic manifest / quarantine;
- compact deltas and MEGA self-heal;
- no `/TelegramBotBackupsStart`.
