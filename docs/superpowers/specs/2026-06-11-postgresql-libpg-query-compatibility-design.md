# PostgreSQL libpg_query Compatibility Design

**Goal:** Detect and track PostgreSQL syntax accepted by the latest stable `libpg_query` branch but not fully supported by ParserSQL's PostgreSQL dialect.

**Initial target:** Compare `libpg_query` `17-latest` and `18-latest`, with `18-latest` as the current compatibility target. Branch commits are resolved to immutable SHAs for every run.

## Problem

ParserSQL is a hand-written parser and can lag PostgreSQL syntax as new releases add statements, clauses, options, keywords, or expression forms. Existing corpus tests measure queries already present in selected corpora, but they do not prove coverage of the complete syntax surface of a PostgreSQL release.

`libpg_query` embeds PostgreSQL's parser and publishes stable branches aligned with PostgreSQL major versions. It is therefore the executable syntax oracle for ParserSQL's PostgreSQL dialect.

The system must provide two views:

- a full backlog of syntax gaps against `18-latest`
- a focused release delta showing syntax introduced or changed from `17-latest` to `18-latest`

Only a successful deep parse with a non-null AST counts as full support. ParserSQL deliberately returns `ParseResult::OK` for Tier-2 lightweight extractors that classify and scan statements without validating their complete grammar. Those results remain useful for ProxySQL routing but do not count as parser parity. `PARTIAL` also remains useful for classification but does not count as full support.

## Chosen Approach

Use a hybrid extractor rather than attempting to generate SQL from every Bison production:

1. Extract executable statements from the PostgreSQL regression SQL included in each `libpg_query` branch.
2. Validate every extracted statement with the corresponding `libpg_query` parser.
3. Diff the accepted PG17 and PG18 inventories to identify release-added statements.
4. Diff grammar and parser metadata to detect syntax changes that do not have an executable regression-test witness.
5. Maintain a small reviewed witness file for structural changes that need a hand-written minimal statement.
6. Compare all accepted PG18 statements and reviewed witnesses with ParserSQL.

PostgreSQL's grammar contains semantic actions, optional productions, context-sensitive names, and terminals that cannot be converted mechanically into valid standalone SQL without substantial grammar-specific knowledge. Regression SQL provides executable witnesses; grammar metadata acts as a completeness alarm.

## Source Management

The compatibility tools use a configurable cache directory outside the repository, defaulting to `/tmp/parsersql-pg-compat`.

The fetch stage:

1. Fetches `17-latest` and `18-latest` from `https://github.com/pganalyze/libpg_query.git`.
2. Resolves both branch heads to commit SHAs.
3. Reads `PG_VERSION` and `PG_VERSION_NUM` from each branch's `Makefile`.
4. Downloads the matching official PostgreSQL source archives from `ftp.postgresql.org`.
5. Records repository URL, branch, commit SHA, PostgreSQL version, archive URL, and archive checksum in a generated run manifest.

Normal comparison runs use committed pin metadata and do not silently advance branches. An explicit refresh command resolves new branch heads and displays the upstream changes before updating the pins and baseline.

`libpg_query` remains a development and test dependency. It is never linked into the production ParserSQL library.

## Extraction Pipeline

### Executable statement inventory

For every SQL file under `test/sql/postgres_regress/`:

1. Read the complete file.
2. Split it with `pg_query_split_with_parser()`, preserving source file, byte offset, and statement length. If intentional negative tests or psql-only content make parser splitting reject the complete file, fall back to `pg_query_split_with_scanner()` and validate every resulting candidate independently.
3. Parse each statement independently through `libpg_query` protobuf output.
4. Exclude statements rejected by the oracle, psql-only input, and empty fragments from parity totals.
5. Store accepted statements in a deterministic JSON Lines inventory.

Each inventory record contains:

- stable statement ID derived from normalized SQL and top-level protobuf node type
- source branch, commit, file, offset, and line
- original SQL
- normalized comparison key
- top-level PostgreSQL protobuf node type
- oracle parse status

Normalization uses the `libpg_query` scanner token stream: comments and insignificant whitespace are removed, unquoted keywords are canonicalized, and identifiers, quoted tokens, literals, and operators retain their text. This avoids release-delta noise from formatting changes without collapsing distinct syntax forms the way a semantic fingerprint could.

Duplicate normalized statements are one compatibility case with a list of source occurrences. Moving a statement between regression files therefore does not change its stable ID.

The extractor does not execute SQL. Statements that require catalog objects or would fail semantically remain valid syntax witnesses as long as `libpg_query` accepts them.

### Release delta

The PG17-to-PG18 delta compares normalized accepted inventories. It reports:

- statements newly present in PG18
- statements removed from PG18
- statements changed in the same source file region
- new top-level protobuf node types

Textual inventory differences are evidence of new test coverage, not proof that every changed statement represents new grammar. Structural diffs supply that additional signal.

### Structural inventory

For both PostgreSQL source versions, extract and compare:

- `src/backend/parser/gram.y`
- `src/include/parser/kwlist.h`
- `src/include/nodes/parsenodes.h`
- `libpg_query`'s `protobuf/pg_query.proto`

The structural report records:

- added or removed grammar productions and alternatives
- added, removed, or recategorized keywords
- added parse node types
- added fields or enum values in parse nodes and protobuf messages

A structural change not associated with an accepted SQL witness is classified as `UNWITNESSED_FEATURE`. It does not count toward the numerical parity result, but remains visible until a reviewed witness maps it to executable SQL or it is explicitly marked non-syntax/internal.

## Reviewed Witnesses

Repository-owned witnesses live in `tests/pg_compat/witnesses.sql` with adjacent metadata identifying:

- stable feature ID
- first PostgreSQL major version
- structural source, such as grammar production or parse-node field
- expected top-level statement type
- SQL statement
- optional explanatory note

Witnesses are parsed by `libpg_query` before ParserSQL sees them. A witness rejected by the pinned oracle is a harness error.

The witness file covers structural changes missed by regression SQL. It is not intended to become a second manually maintained PostgreSQL grammar.

## Differential Runner

Add a C++ tool that links ParserSQL and the pinned `libpg_query` build. For each accepted inventory statement or reviewed witness, it:

1. Obtains the expected top-level statement type from `libpg_query` protobuf output.
2. Parses the complete statement with `Parser<Dialect::PostgreSQL>`.
3. Checks ParserSQL status.
4. Checks whether ParserSQL produced a non-null AST, distinguishing a deep parse from Tier-2 classification.
5. Checks that no meaningful SQL remains unconsumed.
6. Maps the PostgreSQL protobuf node type to ParserSQL `StmtType` and checks classification.
7. Emits one structured result record.

Version one validates deep syntax acceptance, Tier-2 classification coverage, and top-level classification. It does not compare AST structures because PostgreSQL's typed parse tree and ParserSQL's compact AST intentionally have different shapes.

## Result Contract

Every oracle-accepted statement receives exactly one compatibility result:

- `DEEP_SUPPORTED`: ParserSQL returns `OK`, produces a non-null AST, consumes the complete statement, and reports the expected top-level statement type.
- `CLASSIFIED_ONLY`: ParserSQL returns `OK`, produces no AST, consumes the complete statement, and reports the expected top-level statement type.
- `PARTIAL`: ParserSQL returns `PARTIAL`.
- `ERROR`: ParserSQL returns `ERROR`.
- `TRAILING_INPUT`: ParserSQL returns `OK` but leaves non-whitespace, non-semicolon input.
- `TYPE_MISMATCH`: ParserSQL returns `OK` with a different top-level statement type.

Extraction diagnostics use:

- `ORACLE_REJECTED`: candidate statement rejected by `libpg_query`; excluded from parity totals.
- `UNWITNESSED_FEATURE`: structural syntax signal with no accepted SQL witness; excluded from parity totals but displayed prominently.

Only `DEEP_SUPPORTED` counts as full PostgreSQL compatibility. `CLASSIFIED_ONLY` is shown separately as useful ProxySQL routing coverage.

## Statement-Type Mapping

The runner owns an explicit mapping from relevant `libpg_query` protobuf node types to ParserSQL `StmtType`.

Mapping behavior:

- known equivalent types must match
- PostgreSQL node types intentionally grouped by ParserSQL classification map to that shared type
- a new unmapped protobuf node type is a harness failure, not an automatic ParserSQL gap
- mapping changes require focused unit tests

This keeps classification expectations reviewable and prevents unknown PostgreSQL statements from being silently treated as `UNKNOWN`.

## Baseline and Reports

Generated outputs:

- machine-readable JSON results for automation and historical comparison
- a Markdown report at `docs/compatibility/postgresql-18.md`
- a committed full result baseline at `tests/pg_compat/expected_results.jsonl`
- a committed deterministic pull-request case set at `tests/pg_compat/ci_cases.jsonl`
- committed upstream pin metadata

The Markdown report includes:

- exact `libpg_query` SHAs and PostgreSQL patch versions
- full PG18 totals by compatibility result
- full PG18 gap backlog grouped by top-level statement type
- PG17-to-PG18 release-delta results
- newly supported and newly regressed statements
- unwitnessed structural features
- reproduction commands

Stable statement and feature IDs allow baseline comparison even if report ordering changes.

`expected_results.jsonl` records the expected result for every deduplicated full-suite statement. It is compact baseline metadata, not a second copy of the upstream SQL corpus. `ci_cases.jsonl` includes SQL text for all reviewed witnesses, all known gaps, all PG17-to-PG18 delta cases, and representative `DEEP_SUPPORTED` and `CLASSIFIED_ONLY` cases for each mapped top-level statement type where those levels exist. This allows ordinary CI to run without fetching the full upstream regression suite.

## CI Policy

Ordinary pull-request CI runs a deterministic compatibility subset against committed upstream pins and baseline data.

It fails when:

- a previously `DEEP_SUPPORTED` statement becomes anything else
- a previously `CLASSIFIED_ONLY` statement becomes `PARTIAL`, `ERROR`, `TRAILING_INPUT`, or `TYPE_MISMATCH`
- a new gap appears without an approved baseline update
- ParserSQL leaves trailing input or misclassifies a previously supported statement
- a protobuf statement node lacks an explicit mapping
- a reviewed witness is rejected by `libpg_query`

Accepted improvements are:

- any result to `DEEP_SUPPORTED`
- `PARTIAL`, `ERROR`, `TRAILING_INPUT`, or `TYPE_MISMATCH` to `CLASSIFIED_ONLY`

For any other case, the committed result must remain unchanged. Different unsupported outcomes are orthogonal diagnostics rather than a global severity ordering, so transitions such as `ERROR` to `PARTIAL` require review and baseline regeneration.

Existing committed gaps do not make every CI run fail.

The full upstream suite runs:

- in scheduled CI
- during explicit `make pg-compat`
- during `make pg-compat-refresh`

`pg-compat-refresh` is the only workflow that advances branch SHAs. Its generated pin, inventory, baseline, and report changes are reviewed together.

## Repository Layout

```text
tools/pg_compat/
    pg_compat.cpp
    statement_type_map.cpp
    statement_type_map.h
scripts/pg_compat/
    fetch_libpg_query.sh
    extract_statements.py
    compare_versions.py
    generate_report.py
tests/pg_compat/
    witnesses.sql
    witnesses.json
    expected_results.jsonl
    ci_cases.jsonl
    upstream_pins.json
    fixtures/
docs/compatibility/
    postgresql-18.md
```

The SQL witness file contains statements only. `witnesses.json` carries feature metadata keyed by stable witness ID so SQL splitting remains independent of annotation syntax.

## Commands

Add Makefile targets:

```text
make build-pg-compat
make pg-compat
make pg-compat-refresh
make test-pg-compat
```

- `build-pg-compat`: build the differential runner and pinned `libpg_query`.
- `pg-compat`: run the full comparison using committed pins and check the baseline.
- `pg-compat-refresh`: fetch branch heads, regenerate inventories and reports, and show baseline changes.
- `test-pg-compat`: run deterministic extractor and mapping fixtures plus `ci_cases.jsonl`.

## Error Handling

The tooling distinguishes infrastructure failure from compatibility failure.

Infrastructure failures include missing build tools, failed downloads, checksum mismatch, invalid pins, malformed inventories, oracle crashes, unmapped node types, and invalid reviewed witnesses. They terminate the run with a nonzero status and do not update reports.

Compatibility failures are valid oracle-accepted statements that ParserSQL does not fully support. They are written to results and evaluated against the committed baseline.

Generated files are written to temporary paths and moved into place only after a complete successful run, preventing partial baseline updates.

## Testing

Add focused tests for:

- SQL splitting across comments, dollar-quoted bodies, procedural definitions, and multiple statements
- exclusion of psql meta-commands and oracle-rejected negative tests
- deterministic statement IDs and deduplication
- PG17-to-PG18 inventory diff behavior
- grammar, keyword, parse-node, and protobuf structural diff extraction
- witness metadata validation
- protobuf node to ParserSQL `StmtType` mapping
- all six ParserSQL compatibility outcomes
- baseline regression and improvement detection
- report generation from fixed fixture results

A small checked-in fixture set exercises the entire pipeline without network access. Full upstream tests verify integration with real `libpg_query` branches.

## Scope Boundaries

Included:

- PostgreSQL dialect only
- latest stable `libpg_query` compatibility, initially `18-latest`
- full target-version backlog
- previous-to-current major release delta
- deep syntax acceptance, Tier-2 classification coverage, complete consumption, and top-level statement classification
- structural alerts for grammar changes without witnesses

Explicitly deferred:

- AST structural equivalence
- semantic validation against a running PostgreSQL server
- arbitrary Bison grammar generation
- MySQL compatibility
- production runtime fallback to `libpg_query`
- tracking unreleased PostgreSQL development branches

## Success Criteria

The design is successful when one command can reproducibly answer:

1. Which `libpg_query` PG18 statements ParserSQL deeply parses into an AST.
2. Which accepted statements are only classified by Tier-2 extractors.
3. Which accepted statements return `PARTIAL`, `ERROR`, trailing input, or a type mismatch.
4. Which gaps are part of the full PG18 backlog.
5. Which gaps are associated with the PG17-to-PG18 release delta.
6. Which structural PostgreSQL syntax changes still lack an executable witness.
7. Whether a ParserSQL change introduced a new PostgreSQL compatibility regression.
