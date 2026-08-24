# Transaction Statement Node — Design Specification

## Overview

Promotes `BEGIN` and `START TRANSACTION` from Tier 2 to Tier 1 and introduces `NODE_TRANSACTION_STMT`, so that the transaction characteristics they carry (`READ ONLY`, `READ WRITE`, `ISOLATION LEVEL ...`) survive the parse. Today no mode reaches the AST, so a consumer has no structured way to tell a read-only transaction from a writable one.

### Goals

- **Every transaction mode reaches the AST:** `READ ONLY` / `READ WRITE` in both dialects, plus PostgreSQL's four isolation levels and `[NOT] DEFERRABLE` and MySQL's `WITH CONSISTENT SNAPSHOT`. A mode no consumer reads is still parsed, so it cannot hide a mode behind it.
- **`BEGIN` distinguishable from `BEGIN READ ONLY`:** the node is emitted even with no modes, so its absence is not overloaded to mean "no modes".
- **Digest stability:** modes are stored and re-emitted under their canonical spelling, so the digest text is unchanged for canonically written input, and casing, internal spacing, and PostgreSQL's optional commas all normalize onto that one form instead of producing a digest each. No mode is dropped, so no transaction form shortens its own digest text.
- **No behavior change for `COMMIT` / `ROLLBACK` / `SAVEPOINT`:** they stay Tier 2.

### Constraints

- **Each dialect accepts only its own grammar:** a mode one dialect does not define is not parsed for it, so the node never asserts semantics for a statement the server would reject.
- `BEGIN` is a benchmarked statement (README publishes 29 ns); added cost must stay well inside that.
- `scan_to_end()` must still run, so multi-statement `remaining` handling is untouched.

---

## Problem

`extract_transaction()` classifies `BEGIN`, `START TRANSACTION`, `COMMIT`, `ROLLBACK`, and `SAVEPOINT` as Tier-2 statements: it sets `stmt_type`, then calls `scan_to_end()` to consume the rest of the input without parsing it. No AST node is produced.

For `COMMIT`, `ROLLBACK`, and `SAVEPOINT` that is sufficient; the statement type carries all the meaning. For the two transaction-*starting* statements it is not, because the modes that follow decide whether the transaction may write (needed for our read-only classification work). No mode is parsed, so the statement type and the AST are the same whether the transaction can write or not. The modes survive only as an unparsed text tail, which also clears `full_input`:

| input | status | stmt_type | ast | full_input | remaining |
|---|---|---|---|---|---|
| `BEGIN` | OK | BEGIN | `nullptr` | true | `""` |
| `BEGIN READ ONLY` | OK | BEGIN | `nullptr` | false | `"READ ONLY"` |
| `START TRANSACTION READ ONLY` | OK | START_TRANSACTION | `nullptr` | false | `"READ ONLY"` |

`SET TRANSACTION READ ONLY` — the same characteristics on a different statement — *is* parsed, producing `NODE_SET_TRANSACTION` with the mode as an identifier child (`set_parser.h`, `parse_set_transaction()`). The grammar is already implemented, it is simply not reachable from the two statements where it matters most.

### Motivating consumer

ProxySQL routes queries to backend hostgroups. Routing a read-only transaction to a read replica requires knowing, at `BEGIN` time, that the transaction cannot write. A transaction is the unit a pooler would pin to a replica, so this is the case that matters most for that feature.

---

## Chosen Approach

Promote `BEGIN` and `START TRANSACTION` to Tier 1, with a dedicated `NODE_TRANSACTION_STMT` whose children are the parsed modes as `NODE_IDENTIFIER` nodes.

The node is produced even when no modes are present, so `BEGIN` yields an empty `NODE_TRANSACTION_STMT` rather than `nullptr`. That makes "plain `BEGIN`" and "`BEGIN READ ONLY`" distinguishable, and keeps the node's presence a property of the statement type rather than of its arguments.

### Classifier Updates

The switch in `Parser<D>::classify_and_dispatch()`:

- `TK_BEGIN` → `parse_transaction()` (was `extract_transaction()`)
- `TK_START` → `parse_transaction()` (was `extract_transaction()`)
- `TK_COMMIT`, `TK_ROLLBACK`, `TK_SAVEPOINT` → `extract_transaction()` (unchanged)

Those three take no arguments, so Tier 2 remains correct for them — the five only shared a
function because they shared a *lack* of parsing.

---

## New NodeType Additions

```cpp
// TRANSACTION
NODE_TRANSACTION_STMT,
```

Flags for a `NODE_TRANSACTION_STMT` mode child:

```cpp
static constexpr uint16_t FLAG_TXN_MODE_ISOLATION = 0x01;
```

The above flag is set on a mode child that is an isolation level, so the emitter re-inserts the `ISOLATION LEVEL` keywords the parser consumed. Without it the emitter would have to guess from the value, which breaks once modes other than the access modes are stored.

## New Token Additions

**None.** Every mode this spec parses is spelled with existing tokens: `TK_READ`, `TK_ONLY`, `TK_WRITE`, `TK_ISOLATION`, `TK_LEVEL`, `TK_SERIALIZABLE`, `TK_REPEATABLE`, `TK_COMMITTED`, `TK_UNCOMMITTED`, `TK_COMMA`, `TK_TRANSACTION`, plus `TK_NOT` for `NOT DEFERRABLE` and `TK_WITH` for `WITH CONSISTENT SNAPSHOT`.

`WORK` has no token either and is matched on identifier text, as `set_parser.h` already does for `CHARACTERISTICS` and `AUTHORIZATION`. It is a pure noise word — PostgreSQL's `opt_transaction` production carries no semantic action, so the server discards it too — which is why it needs no form flag and re-emits as plain `BEGIN`.

`DEFERRABLE`, `CONSISTENT` and `SNAPSHOT` are matched the same way. Unlike `WORK` they are not noise words, so they are stored as mode children and reproduced by the emitter, each is gated by `if constexpr` to the dialect whose grammar has it: `[NOT] DEFERRABLE` for PostgreSQL, `WITH CONSISTENT SNAPSHOT` for MySQL.

---

## PostgreSQL Syntax

```
BEGIN [ WORK | TRANSACTION ] [ transaction_mode [, ...] ]
START TRANSACTION [ transaction_mode [, ...] ]

transaction_mode:
    ISOLATION LEVEL { SERIALIZABLE | REPEATABLE READ | READ COMMITTED | READ UNCOMMITTED }
    READ WRITE | READ ONLY
    [ NOT ] DEFERRABLE
```

Modes are comma-separated, though PostgreSQL accepts them with the commas omitted, so the parser treats the separator as optional. `WORK` and `TRANSACTION` are noise words after `BEGIN`.

## MySQL Syntax

```
BEGIN [ WORK ]
START TRANSACTION [ transaction_characteristic [, ...] ]

transaction_characteristic:
    WITH CONSISTENT SNAPSHOT
    READ WRITE | READ ONLY
```

MySQL has no `DEFERRABLE`, and sets the isolation level through `SET TRANSACTION` rather than on `START TRANSACTION`. `WITH CONSISTENT SNAPSHOT` is MySQL-only.

One mode loop serves both dialects, but only `READ ONLY` / `READ WRITE` are common to them. Everything else is gated by `if constexpr` to the dialect whose grammar has it:

| construct | PostgreSQL | MySQL |
|---|---|---|
| `READ ONLY` / `READ WRITE` | after `BEGIN` or `START TRANSACTION` | after `START TRANSACTION` only |
| `ISOLATION LEVEL …` | ✅ | ✗ |
| `[NOT] DEFERRABLE` | ✅ | ✗ |
| `WITH CONSISTENT SNAPSHOT` | ✗ | ✅ |
| `TRANSACTION` after `BEGIN` | ✅ | ✗ (`WORK` only) |
| any mode after bare `BEGIN` | ✅ | ✗ |
| comma between modes | optional | required |

A construct the dialect does not define terminates the loop and falls to `scan_to_end()`, so it lands in `remaining` rather than becoming a mode child. This follows `set_parser.h`, which gates `SET LOCAL`, `SET ROLE`, `SET CONSTRAINTS`, `SET SCHEMA`, `SET SEED` and `SET TIME ZONE` to PostgreSQL for the same reason: a shared parse would emit a node for syntax the other server rejects.

---

## AST Structure

```
BEGIN READ ONLY                                                   (PostgreSQL)
└── NODE_TRANSACTION_STMT "BEGIN"
    └── NODE_IDENTIFIER "READ ONLY"

BEGIN ISOLATION LEVEL READ COMMITTED, READ ONLY                   (PostgreSQL)
└── NODE_TRANSACTION_STMT "BEGIN"
    ├── NODE_IDENTIFIER "READ COMMITTED"   flags = FLAG_TXN_MODE_ISOLATION
    └── NODE_IDENTIFIER "READ ONLY"

BEGIN ISOLATION LEVEL SERIALIZABLE, READ ONLY, DEFERRABLE         (PostgreSQL)
└── NODE_TRANSACTION_STMT "BEGIN"
    ├── NODE_IDENTIFIER "SERIALIZABLE"     flags = FLAG_TXN_MODE_ISOLATION
    ├── NODE_IDENTIFIER "READ ONLY"
    └── NODE_IDENTIFIER "DEFERRABLE"

START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY             (MySQL)
└── NODE_TRANSACTION_STMT "START TRANSACTION"
    ├── NODE_IDENTIFIER "WITH CONSISTENT SNAPSHOT"
    └── NODE_IDENTIFIER "READ ONLY"

START TRANSACTION READ ONLY                                       (both)
└── NODE_TRANSACTION_STMT "START TRANSACTION"
    └── NODE_IDENTIFIER "READ ONLY"

START TRANSACTION                                                 (both)
└── NODE_TRANSACTION_STMT "START TRANSACTION"
```

Mode values are stored under their **canonical spelling**, not as a span of the input, so a consumer can compare a child against `"READ ONLY"` without first normalizing case or internal whitespace. `select_parser.h` already does this for `NOWAIT` and `SKIP LOCKED`. Spanning the source instead would make `READ   ONLY` a different string from `READ ONLY`, which every consumer would then have to work around.

`ISOLATION LEVEL` is not stored, the level alone is — the same storage convention as `NODE_SET_TRANSACTION`. That node's emitter recovers the keywords by inference: anything that is not `READ ONLY` or `READ WRITE` is assumed to be an isolation level. This node records it instead, on `FLAG_TXN_MODE_ISOLATION`, because the inference breaks as soon as a mode that is neither is stored, which `[NOT] DEFERRABLE` and `WITH CONSISTENT SNAPSHOT` are.

### Recording the introducing keywords

`stmt_type` distinguishes `BEGIN` from `START TRANSACTION`, but not `BEGIN` from `BEGIN TRANSACTION`. The emitter needs that to round-trip, so the introducing keywords are stored as the node's **value**, under their canonical spelling — following `NODE_SET_OPERATION`, which likewise holds its mutually exclusive operator (`UNION` / `INTERSECT` / `EXCEPT`) in the value and reserves `flags` for the independent `ALL` modifier. A canonical literal rather than a source span keeps casing out of the digest: `begin read only` and `BEGIN READ ONLY` must normalize to the same digest text, as they did when the statement had no AST and fell through to the token-level digest path, which uppercases keyword tokens.

---

## Emitter Extensions

One new method, `emit_transaction_stmt()`, plus its dispatch case. It writes the node's value — the introducing keywords — then each mode. A single mode is emitted without a comma so the common forms round-trip exactly; multiple modes are comma-separated, which PostgreSQL accepts and MySQL requires. No case folding happens here as the parser already stored each mode canonically.

| input | emitted | dialect |
|---|---|---|
| `BEGIN` | `BEGIN` | both |
| `begin read only` | `BEGIN READ ONLY` | PostgreSQL |
| `BEGIN TRANSACTION READ ONLY` | `BEGIN TRANSACTION READ ONLY` | PostgreSQL |
| `START TRANSACTION READ WRITE` | `START TRANSACTION READ WRITE` | both |
| `BEGIN ISOLATION LEVEL SERIALIZABLE` | `BEGIN ISOLATION LEVEL SERIALIZABLE` | PostgreSQL |

---

## Scope Boundaries

`COMMIT`, `ROLLBACK`, and `SAVEPOINT` keep their Tier-2 treatment and produce no AST. Their statement type is their entire meaning.

`scan_to_end()` still runs after the modes are parsed, so multi-statement handling is unaffected: `BEGIN READ ONLY; SELECT 1` continues to report `remaining = "SELECT 1"`, and `full_input` stays false for it while every single-statement form sets it.

An input that starts a mode without completing it — `BEGIN READ`, `BEGIN ISOLATION LEVEL` — reports `PARTIAL`, matching how the Tier-1 parsers treat unexpected EOF.

---

## Implementation

1. `NODE_TRANSACTION_STMT` and `FLAG_TXN_MODE_ISOLATION` in `common.h`.
2. `parse_transaction()` in `parser.cpp`, declared in the Tier-1 block of `parser.h`; `TK_BEGIN` / `TK_START` routed to it from `classify_and_dispatch()` and removed from `extract_transaction()`.
3. `parse_transaction_modes(ParseResult&, StringRef)` as the mode loop, following the Tier-1 conventions: `ERROR` if the node cannot be allocated, `PARTIAL` on an incomplete mode. Each mode is stored under its canonical spelling, as `select_parser.h` already does for `NOWAIT` and `SKIP LOCKED`, so a consumer never has to normalize before comparing.
4. `emit_transaction_stmt()` and its dispatch case in `emitter.h`.
5. Tests in `tests/test_misc_stmts.cpp`, beside the other Tier-1 statements that live in `parser.cpp`, plus digest coverage in `tests/test_digest.cpp`.

---