# Issue 05: Tighten Expression And Type Semantics

## Priority

`P2`

## Status

In progress.

## Problem

Several expression and type paths still return `NULL` or simplified behavior where real semantics are expected. This includes tuple values, non-literal arrays, composite field access, simplified string length behavior, and string-backed decimals.

## Evidence

- [include/sql_engine/expression_eval.h](/data/rene/ParserSQL/include/sql_engine/expression_eval.h:688)
- [include/sql_engine/functions/string.h](/data/rene/ParserSQL/include/sql_engine/functions/string.h:70)
- [include/sql_engine/value.h](/data/rene/ParserSQL/include/sql_engine/value.h:21)
- [include/sql_engine/functions/cast.h](/data/rene/ParserSQL/include/sql_engine/functions/cast.h:43)

## Risk

- Dialect inconsistencies
- Incorrect or lossy semantics for non-trivial expressions
- Limited headroom for richer type support

## Desired Outcome

Close the most user-visible semantic gaps first, with explicit tests per behavior.

## Scope

- Prioritize UTF-8-aware length semantics, clearer array behavior, and targeted cast improvements
- Defer large type-system expansions unless directly needed by query behavior

## Acceptance Criteria

- Unsupported paths are reduced or explicitly surfaced
- Added semantics are covered by targeted unit tests

## Progress Notes

- `CHAR_LENGTH` now counts UTF-8 code points, while `LENGTH` remains byte-based.
- PostgreSQL explicit casts now accept `on` / `off` for boolean strings and support string-to-`DATE` / `TIME` / `DATETIME` / `TIMESTAMP` helper paths.
- Array constructors, tuple standalone values, composite field access, and string-backed decimal semantics remain open.

## Verification So Far

- `rm -f tests/test_string_funcs.o tests/test_eval_integration.o src/sql_engine/function_registry.o run_tests && make run_tests && ./run_tests --gtest_filter="StringFuncTest.*Length*:EvalIntegrationTest.SelectLength*:EvalIntegrationTest.SelectCharLengthUsesCodePointCountForUtf8:EvalIntegrationTest.PgSelectCharLengthUsesCodePointCountForUtf8" --gtest_brief=1`
- `rm -f tests/test_cast.o run_tests && make run_tests && ./run_tests --gtest_filter="CastTest.PgSQL*:CastTest.UnsupportedTarget" --gtest_brief=1`
- `./run_tests --gtest_brief=1`
