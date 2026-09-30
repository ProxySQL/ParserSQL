# Curated MySQL compatibility checks

This harness tracks a small, deliberately selected set of MySQL syntax witnesses and known ParserSQL gaps. It does not measure the percentage of the MySQL dialect supported, implement mysqltest/MTR extraction, or establish query-engine execution support.

ParserSQL now preserves MySQL JSON arrows, named/inherited windows, `GROUP_CONCAT` modifiers and separators, and `GROUP BY ... WITH ROLLUP` as structured ASTs. Database-qualified column names are retained. The local engine rejects these unsupported semantics rather than executing a simplified interpretation; parameterization remains conservative for JSON arrows and `GROUP_CONCAT`. Empty MySQL SELECT target lists are rejected.

ParserSQL also preserves partition lists, `USE`/`FORCE`/`IGNORE INDEX` or `KEY`, hint scopes and empty `USE INDEX ()`, and `LATERAL` derived SELECTs with required aliases and optional alias-column lists. The local engine rejects these unsupported table-reference semantics. `SELECT` and `UPDATE` place `PARTITION` before the table alias; single-table `DELETE` places it after the alias. LATERAL derived queries include `SELECT`, `TABLE`, and `VALUES ROW(...)` expressions and their set operations.

MySQL query expressions also preserve explicit `TABLE name` and `VALUES ROW(...)` constructors in standalone, compound, derived, LATERAL, scalar, IN, EXISTS, and query-only CTE contexts. `INTERSECT` binds more tightly than `UNION` and `EXCEPT`; explicit parentheses and branch-local ordering/limits are retained. Recursive CTEs retain their keyword and optional column lists. Query locking clauses, including `LOCK IN SHARE MODE`, remain structured. This is parser/AST coverage; the local engine rejects unsupported query forms.

Top-level MySQL `WITH` and `WITH RECURSIVE` also retain structured UPDATE and DELETE main statements, including join/comma UPDATEs, both multi-table DELETE target-list forms, quoted CTE names, partitions, statement options, and single-table ordering/limits. CTE bodies and subqueries remain query-only; DML inside them and a leading `WITH ... INSERT` are rejected. UPDATE assignments require one-, two-, or three-part column names and accept both `=` and `:=` (emitted as `=`), including `DEFAULT` values. UPDATE options follow `LOW_PRIORITY` then `IGNORE`; DELETE options may appear in any order and repeat. The [8.4.8 UPDATE/DELETE productions](https://github.com/mysql/mysql-server/blob/0896fcd61dec11a0904166911a0126f59daaa1bf/sql/sql_yacc.yy#L13389) and [9.7.2 UPDATE/DELETE productions](https://github.com/mysql/mysql-server/blob/008e09c2834b98143a8c067d4d225c90953050cf/sql/sql_yacc.yy#L13945) attach an optional WITH clause to each main statement. Routing metadata identifies the main DML target, including wildcard DELETE targets. JOIN conditions require complete ON/USING operands and retain quoted USING column names. These are parser/AST guarantees; the local engine rejects CTE-prefixed DML.

The native grammar permits empty `VALUES ROW()`, `VALUES ROW(DEFAULT)`, and repeated locking clauses. Both pinned servers return semantic errors 3942, 3943, and 3569 respectively for the curated standalone or overlapping-lock witnesses. These remain `SERVER_ERROR` observations, not syntax rejections, while ParserSQL preserves the grammar-valid ASTs. `TABLE` takes a table identifier and query-level tails; aliases, partitions, index hints, and WHERE clauses directly after its table name are malformed controls. The [8.4.8 query-primary productions](https://github.com/mysql/mysql-server/blob/0896fcd61dec11a0904166911a0126f59daaa1bf/sql/sql_yacc.yy#L9831) and [9.7.2 query-primary productions](https://github.com/mysql/mysql-server/blob/008e09c2834b98143a8c067d4d225c90953050cf/sql/sql_yacc.yy#L10145) include both constructors. The corpus records exact anchors for their rows, subquery contexts, locking, CTEs, and malformed controls.

Table identifiers and aliases use the common 259-word reserved baseline observed in the pinned 8.4.8 and 9.7.2 releases. ParserSQL has no MySQL version setting, so the five 9.7-only reserved additions (`CUBE`, `EXTERNAL`, `LIBRARY`, `QUALIFY`, and `TABLESAMPLE`) remain accepted as names. Strict version-specific identifier rules remain a gap.

Numeric-looking unquoted qualified table names such as `db.123` are valid natively but remain unsupported: the tokenizer reads `.123` as a numeric literal. ParserSQL rejects these forms; earlier behavior could emit an incorrect alias. Lossless parsing of this identifier edge remains a gap.

The corpus pins the official `mysql/mysql-server` sources:

| Version | Commit |
| --- | --- |
| MySQL 8.4.8 | `0896fcd61dec11a0904166911a0126f59daaa1bf` |
| MySQL 9.7.2 | `008e09c2834b98143a8c067d4d225c90953050cf` |

[The corpus](../tests/mysql_compat/cases.json) contains hand-reduced SQL, intended validity, explicit expected ParserSQL outcomes, and source grammar paths, lines and excerpts for both versions. A grammar anchor identifies related upstream syntax; it does not claim that the reduced query is verbatim upstream test SQL or that source inspection is a native parser oracle. Hand-written controls are identified separately. The runner can verify the source commits and excerpts locally.

Run from the repository root:

```sh
make -B lib
# The CI gate is also available as: make test-mysql-compat
python3 scripts/mysql_compat/build_probe.py --output /tmp/parsersql-mysql-probe
python3 -m unittest discover -s tests/mysql_compat -v
python3 scripts/mysql_compat/run.py --probe /tmp/parsersql-mysql-probe \
  --report /tmp/parsersql-mysql-report.json
```

`build_probe.py` compiles a standalone C++17 executable against the existing `libsqlparser.a`; it does not rebuild that archive. Rebuild the library with `make -B lib` before checking parser changes: the Makefile does not track all header dependencies, so a plain incremental build can leave a stale archive. `CXX` selects the compiler. Keep binaries and generated reports outside the repository.

To also verify the source provenance, add `--source84 /path/to/mysql-8.4.8` and `--source97 /path/to/mysql-9.7.2`. Each checkout must have exactly the pinned HEAD and contain `sql/sql_yacc.yy`; no native MySQL build is required for this check. These are the official source links for the [8.4.8 grammar](https://github.com/mysql/mysql-server/blob/0896fcd61dec11a0904166911a0126f59daaa1bf/sql/sql_yacc.yy) and [9.7.2 grammar](https://github.com/mysql/mysql-server/blob/008e09c2834b98143a8c067d4d225c90953050cf/sql/sql_yacc.yy).

The strict parse gate requires status `OK`, `full_input=true`, and a non-null AST. Failure categories are `ERROR`, `PARTIAL`, `NO_AST`, and `TRAILING_INPUT`. `NO_AST` takes precedence over trailing input; the report retains the raw flags and remaining SQL so neither fact is lost. In particular, classification-only DDL must not count as structured parsing.

Complete parses have their emitted SQL reparsed through the same strict gate. The emitted text must stabilize after that reparse. Positive controls also specify exact expected SQL, including JSON `->` and `->>` operators. These checks can expose dropped or changed syntax even when the altered SQL still parses. They are bounded emission checks, not a proof of AST or execution equivalence. `CHECKED` means only that these checks passed. The optimizer-hint witness records a known `EMISSION_MISMATCH`, because dropping a hint is not faithful preservation.

Malformed controls are handled separately: a complete AST is `MALFORMED_ACCEPTED`, even when emission changes. The `SELECT FROM` and malformed JSON-arrow controls require rejection. A rejection is `REJECTED`; the original record still identifies whether it was an error, incomplete input, or a missing AST.

Every fixture freezes one exact expected outcome. Known gaps do not make arbitrary failures acceptable. Both regression and unexpected improvement cause exit status 1, so a fixed gap must be reviewed and explicitly promoted in the corpus. When promoting, add an independently derived `expected_sql`, set `known_gap=false`, and require `CHECKED`. Harness/transport/source errors return 2. Exit status 0 means the expected baseline matched, including its documented known gaps.

The 137-case baseline has 72 `CHECKED` witnesses and 50 `REJECTED` negative controls. Its 15 remaining known gaps are ten `TRAILING_INPUT`, three `NO_AST`, one `PARTIAL`, and one `EMISSION_MISMATCH`. CTE-prefixed UPDATE/DELETE now pass complete parse, exact emission, and stable reparse checks. Source verification checks 134 exact grammar anchors per pinned release. These counts describe this corpus only.

The JSON report includes all original and reparsed records, source verification counts, pinned revisions, SQL-mode metadata, and SHA-256 hashes of the corpus and probe. Counts apply only to these witnesses. The probe uses hex-line input and hex fields in its TSV response, preserving tabs, newlines and embedded NUL bytes. It emits while both the input string and parser arena are alive; Python reparses owned copies in a second probe invocation.

The default corpus oracle state is **NOT VERIFIED**. Its intended `sql_mode` is the empty string; ParserSQL accepts no mode configuration here and the harness does not apply or emulate MySQL session modes. A native server check requires its exact version, actual session mode, schema fixtures, SQL sent, and result/error code. Missing tables, mode-dependent errors, or unsupported runtime features must not be relabeled as syntax rejection. Without native options the harness does not connect to a database. An optional native run uses an explicit absolute socket path to an already-running, isolated server:

```sh
python3 scripts/mysql_compat/run.py --probe /tmp/parsersql-mysql-probe \
  --native-socket /absolute/path/to/isolated-mysql.sock \
  --mysql-client /absolute/path/to/mysql \
  --report /tmp/parsersql-mysql-native.json
```

The client uses `--no-defaults`, the supplied socket, and `root` unless `--native-user` is specified. There is no host discovery or automatic connection. Use an isolated test server: this option explicitly creates a uniquely named `parsersql_compat_<uuid>` database, installs the small fixture in [native.py](../scripts/mysql_compat/native.py), and drops only that owned database in cleanup. The partition witnesses use a separate `pt` table with populated `p0` and `p1` range partitions; `t` retains its fulltext index for the existing search witness. The supplied user needs permission to create/drop that database and its tables. The helper requires server version 8.4.8 or 9.7.2, records the exact reported version and session SQL mode, and reapplies that mode for each query. It does not assert that the native mode equals the corpus's empty intended mode.

Each curated case explicitly chooses direct query execution, `EXPLAIN` for DML (including CTE-prefixed DML), or no native action. DDL and procedures remain **NOT VERIFIED**. These are query-execution or EXPLAIN observations, not access to MySQL's raw parser. Returned row values are evidence only; they are not compared with ParserSQL execution. The helper neither executes emitted SQL nor claims semantic round-trip validation.

Native results are stored separately in `native_oracle`: `ACCEPTED`, `SYNTAX_REJECTED` (1064/1149), `SERVER_ERROR`, `CLIENT_ERROR`, or **NOT VERIFIED**. For example, a derived table missing its required alias returns error 1248; this remains a server error, even though the corpus requires ParserSQL to reject that input. Accepted invalid controls and syntax-rejected valid controls count as expectation mismatches, using per-version validity when specified. Server errors remain explicitly unresolved; client errors return status 2. Thus exit status 0 still does not mean all native cases were verified. The default corpus metadata remains **NOT VERIFIED**, while the optional report describes the actual native observations.

A native check of all 137 witnesses under `ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION` found no native expectation mismatches. MySQL 8.4.8 accepted 77, syntax-rejected 52, and returned five server errors (two 1248 missing-alias errors, 3942, 3943, and 3569); MySQL 9.7.2 accepted 79, syntax-rejected 48, and returned seven server errors (the same five plus 1235 and 3889). Both runs skipped the three DDL/procedure witnesses and cleaned up their owned schemas. All shared positive query/DML witnesses except the three explicitly grammar-valid semantic-error controls were accepted, including partition selection, index hints, query constructors, recursive CTEs, and CTE-prefixed UPDATE/DELETE. Re-run the command on each isolated socket to reproduce current observations; reports are intentionally not committed.

The corpus is deliberately selected and is not a representative workload. It contains 133 shared witnesses and four version-specific query witnesses; this does not imply identical dialects in 8.4.8 and 9.7.2. The version-specific entries record `applicability`, source productions for both versions, `native_validity` per version, and observed native outcomes. Their generic `validity=valid` denotes the 9.7.2 grammar target. The native helper selects the intended validity using the actual server version, so an expected 8.4.8 syntax rejection is not treated as a corpus error. Stored native observations are context, not substitutes for a current run. These additions remain ParserSQL gaps. The following differences include those four query witnesses and two separate DDL spot checks:

| Reduced feature | MySQL 8.4.8 observation | MySQL 9.7.2 observation |
| --- | --- | --- |
| `CREATE TABLE vector_test(v VECTOR(3))` | Syntax error 1064 | Accepted |
| `CREATE VIEW IF NOT EXISTS view_test AS SELECT 1 AS a` | Syntax error 1064 | Accepted |
| `EXPLAIN ANALYZE FORMAT=JSON INTO @plan SELECT 1` | Syntax error 1064 | Accepted |
| `JSON_ARRAYAGG(a NULL ON NULL)` | Syntax error 1064 | Accepted |
| `JSON_ARRAYAGG(a ABSENT ON NULL)` | Syntax error 1064 | Error 1235 outside JSON duality views |
| `GROUP BY GROUPING SETS ((id),())` | Syntax error 1064 | Error 3889 without a secondary engine on the fixture table |

The pinned 9.7.2 grammar contains the corresponding [VECTOR type](https://github.com/mysql/mysql-server/blob/008e09c2834b98143a8c067d4d225c90953050cf/sql/sql_yacc.yy#L7240), [view clause](https://github.com/mysql/mysql-server/blob/008e09c2834b98143a8c067d4d225c90953050cf/sql/sql_yacc.yy#L18217), [EXPLAIN options](https://github.com/mysql/mysql-server/blob/008e09c2834b98143a8c067d4d225c90953050cf/sql/sql_yacc.yy#L14612), [JSON aggregate clause](https://github.com/mysql/mysql-server/blob/008e09c2834b98143a8c067d4d225c90953050cf/sql/sql_yacc.yy#L11397), and [GROUPING SETS](https://github.com/mysql/mysql-server/blob/008e09c2834b98143a8c067d4d225c90953050cf/sql/sql_yacc.yy#L12893). Grammar presence and successful execution are different observations: unsupported features and missing engine prerequisites remain unresolved, not accepted by the native helper. Likewise, window `GROUPS` and `EXCLUDE` appear in upstream grammar but returned unsupported-feature errors in both versions; this batch does not claim to implement them.
