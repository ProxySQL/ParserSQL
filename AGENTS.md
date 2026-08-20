# Agent notes

Trust the `Makefile` over prose. Extension recipes live in `CLAUDE.md`. `docs/superpowers/` is historical, not current behavior.

## Layout

- Parser: header-only templates in `include/sql_parser/` except `src/sql_parser/{arena,parser}.cpp`
- Engine: headers in `include/sql_engine/` (`operators/`, `functions/`, `rules/`); compiled files are the explicit `ENGINE_SRCS` list
- High-level API: `Session<D>` (`include/sql_engine/session.h`) — parse → plan → optimize → distribute → execute
- Production remote path: `ThreadSafeMultiRemoteExecutor` (pooled MySQL **and** PostgreSQL), not the single-connection executors
- All shard routing (SELECT prune and DML) goes through `ShardMap`. Do not add a private hash in the planner.
- Backend URL / shard-spec parsing: `tool_config_parser` — do not add another copy in tools
- Do not edit `third_party/`

## Commands

```bash
make all                 # libsqlparser.a + full GoogleTest suite
make lib
make test                # rebuild ./run_tests and run it
./run_tests --gtest_filter='*WindowFunc*'
make build-sqlengine     # ./sqlengine
make build-corpus-test   # ./corpus_test
make mysql-server engine-stress bench-distributed
make bench               # -O2; release+corpus report: bash scripts/run_benchmarks.sh report.md
make test-pg-compat      # committed PG18 gate (needs PG_COMPAT_CACHE / libpg_query)
```

New `tests/test_*.cpp` must be appended to `TEST_SRCS`. New `src/sql_engine/*.cpp` must be appended to `ENGINE_SRCS`. Otherwise they never build.

No repo formatter. C++17, `-Wall -Wextra`. Match neighboring files. Includes: `"sql_parser/..."`, `"sql_engine/..."`.

macOS needs client libs: `brew install mysql-client postgresql zstd`, then
`LIBRARY_PATH=/opt/homebrew/lib make all MYSQL_CFLAGS="-I/opt/homebrew/opt/mysql-client/include"`.
Tests and tools link libmysqlclient + libpq even when no live backend is used.

## Parser gotchas

- Dialect is compile-time: `Parser<Dialect::MySQL>` / `Parser<Dialect::PostgreSQL>`. One `Parser` per thread (non-copyable).
- `parse(sql, len)` takes an explicit length. `StringRef` views the input — keep the SQL buffer alive until you are done with the AST.
- `parser.reset()` rewinds the arena; AST and emitter output are invalid after reset.
- Keyword lookup is a hash table from `keywords_mysql.h` / `keywords_pgsql.h`. Keep those arrays alphabetically sorted. New keywords also need `token.h`, and usually `is_keyword_as_identifier()` in `expression_parser.h` plus `is_alias_start()` in `table_ref_parser.h`.
- Classifier switch: `classify_and_dispatch()` in `src/sql_parser/parser.cpp`.
- Status is `OK` / `PARTIAL` / `ERROR`. `PARTIAL` can still have a usable AST (e.g. multi-assign SET with one bad element). Do not treat PARTIAL as a hard failure without checking the AST.

## Tests

Default gate: `make test`. Add coverage in the nearest `tests/test_<area>.cpp`.

Live-backend tests `GTEST_SKIP` when unreachable:
- MySQL `127.0.0.1:13306` root/test/testdb — `scripts/start_test_backends.sh`
- PostgreSQL `127.0.0.1:15432` postgres/test/testdb — same script
- `test_single_backend_txn.cpp` / `test_distributed_txn.cpp` skip unless `MYSQL_TEST_HOST` is set

`scripts/start_test_backends.sh` and `scripts/start_sharding_demo.sh` both bind **13306** — do not run them together.

`make test-sqlengine` drives `./sqlengine` and **fails loudly** (exit 2) if containers are missing. Start them first:
- in-memory: no backend
- single: `scripts/setup_single_backend.sh` (port 13308)
- sharded: `scripts/start_sharding_demo.sh` (13306 + 13307)

Corpus is not in-tree. `./corpus_test <mysql|pgsql> [files...]`. Full download: `scripts/run_benchmarks.sh`. CI runs `make all` plus a corpus subset. For grammar/dialect work, also build `corpus_test`.

## Commits / PRs

Conventional prefixes (`feat:`, `fix:`, `test:`, `docs:`, `chore:`, `build:`). PRs target `main`. Do not commit `*.o`, `libsqlparser.a`, `run_tests`, `sqlengine`, `corpus_test`, `run_bench*`, or benchmark artifacts.
