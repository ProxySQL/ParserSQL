# Repository Guidelines

## Project Structure & Module Organization
Core parser headers live in `include/sql_parser/` and parser implementations in `src/sql_parser/`. SQL engine, remote execution, and transaction interfaces live in `include/sql_engine/` with implementations in `src/sql_engine/`. Tests are in `tests/`, mostly as focused `test_<area>.cpp` files plus `corpus_test.cpp` for large parser corpora. Developer tools live in `tools/`, automation scripts in `scripts/`, benchmark reports in `docs/benchmarks/`, and vendored dependencies in `third_party/`.

## Build, Test, and Development Commands
Use the `Makefile` as the source of truth:

- `make all` builds `libsqlparser.a` and runs the full GoogleTest suite.
- `make test` rebuilds `run_tests` and executes all tests locally.
- `make lib` builds just the static library.
- `make build-sqlengine` builds the interactive CLI as `./sqlengine`.
- `make build-corpus-test` builds `./corpus_test` for external SQL corpus validation.
- `make bench` runs the benchmark binary; use it for parser or executor performance changes.
- `make clean` removes generated objects and binaries.

## Coding Style & Naming Conventions
This repository is C++17 with warnings enabled via `-Wall -Wextra`. Match the existing style: 4-space indentation, opening braces on the same line, and concise comments only where the code is not obvious. Use `PascalCase` for types, `snake_case` for functions and methods, `UPPER_SNAKE_CASE` for include guards and macros, and keep file names module-oriented such as `parser.cpp`, `distributed_txn.h`, and `test_select.cpp`. There is no repo-wide formatter config outside vendored code, so follow surrounding files closely.

## Testing Guidelines
Tests use GoogleTest through `tests/test_main.cpp`. Add coverage in the nearest existing `test_<feature>.cpp`, or create a new file with that pattern if the area is new. Prefer small, focused `TEST` or `TEST_F` cases that mirror the production module name. Run `make test` before opening a PR; for grammar or dialect work, also run `make build-corpus-test`.

## Commit & Pull Request Guidelines
Recent history uses short conventional prefixes such as `feat:`, `fix:`, `test:`, `docs:`, and `chore:`. Keep commit titles imperative and specific, for example `feat: add UTC normalization for PgSQL timestamps`. PRs should target `main`, explain parser/engine behavior changes, list the commands you ran, and link related issues. Include benchmark or corpus-test notes when performance or SQL coverage changes. Do not commit generated `.o` files, binaries, or benchmark artifacts.
