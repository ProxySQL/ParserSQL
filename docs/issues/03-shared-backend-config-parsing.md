# Issue 03: Extract Shared Backend And Shard Configuration Parsing

## Priority

`P1`

## Status

Resolved 2026-04-18 (commit f0f2915 — landed alongside issue 09).

## Problem

Backend URL parsing and shard spec parsing are duplicated across multiple tools. The code paths are intended to stay aligned, but they are maintained by copy-paste.

## Evidence

- [tools/sqlengine.cpp](/data/rene/ParserSQL/tools/sqlengine.cpp:203)
- [tools/mysql_server.cpp](/data/rene/ParserSQL/tools/mysql_server.cpp:65)
- [tools/bench_distributed.cpp](/data/rene/ParserSQL/tools/bench_distributed.cpp:51)
- [tools/engine_stress_test.cpp](/data/rene/ParserSQL/tools/engine_stress_test.cpp:53)

## Risk

- Tool behavior diverges over time
- SSL, naming, or shard parsing bugs are fixed in one entry point but not others
- Extra friction for integration testing and documentation

## Desired Outcome

One shared parser for backend URLs and shard specs, reused by all tool entry points and test helpers.

## Scope

- Extract reusable parsing helpers
- Update all current tool entry points to use the shared code
- Add focused unit tests for parsing behavior

## Acceptance Criteria

- No copy-pasted backend/shard parsing remains across tool binaries
- Existing CLI behavior is preserved
- Parsing behavior is covered by direct tests

## Resolution Notes

- Added a shared parser interface in `include/sql_engine/tool_config_parser.h`.
- Implemented shared backend URL and shard spec parsing in `src/sql_engine/tool_config_parser.cpp`.
- Updated `sqlengine`, `mysql_server`, `bench_distributed`, and `engine_stress_test` to use the shared parser instead of local copies.
- Switched `tests/test_ssl_config.cpp` to hit the shared parser directly and added shard parsing coverage.

## Verification

- `make run_tests && ./run_tests --gtest_filter="SSLConfigTest.*" --gtest_brief=1`
- `make build-sqlengine bench-distributed engine-stress mysql-server`
- `./run_tests --gtest_brief=1`
