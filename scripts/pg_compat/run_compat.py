#!/usr/bin/env python3

import argparse
from collections import Counter
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

if __package__:
    from .baseline import (
        build_ci_cases,
        evaluate_baseline,
        load_reviewed_witnesses,
        write_ci_cases,
    )
    from .common import atomic_write_text, load_pins, read_jsonl
    from .compare_versions import compare_inventories, compare_structural_sources
    from .extract_statements import build_inventory, partition_rows
    from .generate_report import generate_report
else:
    from baseline import (
        build_ci_cases,
        evaluate_baseline,
        load_reviewed_witnesses,
        write_ci_cases,
    )
    from common import atomic_write_text, load_pins, read_jsonl
    from compare_versions import compare_inventories, compare_structural_sources
    from extract_statements import build_inventory, partition_rows
    from generate_report import generate_report


ROLES = ("previous", "target")
ROLE_MAJOR = {"previous": "17", "target": "18"}
DEFAULT_CACHE = Path("/tmp/parsersql-pg-compat")
TIMEOUT_SECONDS = 120


def runner_path(cache, role):
    return Path(cache) / "bin" / f"pg_compat-{ROLE_MAJOR[role]}"


def _role_checkout(cache, role):
    return Path(cache) / "libpg_query" / role


def _repo_path(root, *parts):
    return Path(root).joinpath(*parts)


def _run(command, *, cwd=None, env=None, timeout=None):
    result = subprocess.run(
        command,
        cwd=cwd,
        env=env,
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
    )
    if result.returncode != 0:
        details = result.stderr.strip() or result.stdout.strip()
        raise RuntimeError(
            f"command failed with exit {result.returncode}: "
            f"{' '.join(map(str, command))}\n{details}"
        )
    return result


def _require_file(path, description):
    path = Path(path)
    if not path.is_file():
        raise FileNotFoundError(f"missing {description}: {path}")
    return path


def _include_flag(path):
    return f"-I{path}"


def _libpg_query_includes(checkout):
    checkout = Path(checkout)
    return [
        _include_flag(checkout),
        _include_flag(checkout / "src"),
        _include_flag(checkout / "src" / "postgres" / "include"),
        _include_flag(checkout / "protobuf"),
        _include_flag(checkout / "vendor"),
        _include_flag(checkout / "vendor" / "protobuf-c"),
    ]


def build_runner(cache, role, repo_root, *, compiler=None, dry_run=False):
    cache = Path(cache)
    repo_root = Path(repo_root)
    checkout = _role_checkout(cache, role)
    output = runner_path(cache, role)
    compiler = compiler or os.environ.get("CXX", "c++")

    if not checkout.is_dir():
        raise FileNotFoundError(f"missing libpg_query checkout: {checkout}")

    make_command = ["make", "-C", str(checkout)]
    compile_command = [
        compiler,
        "-std=c++17",
        "-Wall",
        "-Wextra",
        "-O2",
        _include_flag(repo_root / "include"),
        _include_flag(repo_root),
        *_libpg_query_includes(checkout),
        str(repo_root / "tools" / "pg_compat" / "pg_compat.cpp"),
        str(repo_root / "tools" / "pg_compat" / "statement_type_map.cpp"),
        str(repo_root / "libsqlparser.a"),
        str(checkout / "libpg_query.a"),
        "-lpthread",
        "-o",
        str(output),
    ]

    if dry_run:
        return output

    output.parent.mkdir(parents=True, exist_ok=True)
    _run(make_command, timeout=TIMEOUT_SECONDS)
    _require_file(checkout / "pg_query.h", "libpg_query C API header")
    _require_file(checkout / "protobuf" / "pg_query.pb-c.h", "protobuf header")
    _require_file(checkout / "libpg_query.a", "libpg_query static library")
    _require_file(repo_root / "libsqlparser.a", "ParserSQL static library")
    _run(compile_command, timeout=TIMEOUT_SECONDS)
    return output


def build_statement_type_test(cache, repo_root, *, compiler=None):
    cache = Path(cache)
    repo_root = Path(repo_root)
    checkout = _role_checkout(cache, "target")
    output = cache / "bin" / "statement_type_cases"
    compiler = compiler or os.environ.get("CXX", "c++")
    output.parent.mkdir(parents=True, exist_ok=True)
    command = [
        compiler,
        "-std=c++17",
        "-Wall",
        "-Wextra",
        "-O2",
        _include_flag(repo_root / "include"),
        _include_flag(repo_root),
        *_libpg_query_includes(checkout),
        str(repo_root / "tests" / "pg_compat" / "statement_type_cases.cpp"),
        str(repo_root / "tools" / "pg_compat" / "statement_type_map.cpp"),
        "-o",
        str(output),
    ]
    _require_file(checkout / "protobuf" / "pg_query.pb-c.h", "protobuf header")
    _run(command, timeout=TIMEOUT_SECONDS)
    return output


def _load_jsonl(path):
    return list(read_jsonl(path))


def _jsonl_text(rows):
    output = [
        json.dumps(row, allow_nan=False, sort_keys=True, separators=(",", ":"))
        for row in rows
    ]
    return "\n".join(output) + ("\n" if output else "")


def _run_runner_file(runner, sql_path, *, branch, commit):
    result = subprocess.run(
        [
            str(runner),
            "--input",
            str(sql_path),
            "--branch",
            branch,
            "--commit",
            commit,
        ],
        capture_output=True,
        text=True,
        timeout=TIMEOUT_SECONDS,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"{sql_path}: {result.stderr.strip() or result.stdout.strip()}"
        )
    return [
        json.loads(line)
        for line in result.stdout.splitlines()
        if line.strip()
    ]


def _postgres_source_dir(cache, version):
    return Path(cache) / "postgresql" / f"postgresql-{version}"


def _postgres_sql_files(cache, version):
    source_dir = _postgres_source_dir(cache, version)
    if not source_dir.is_dir():
        raise FileNotFoundError(f"missing PostgreSQL source directory: {source_dir}")
    return sorted(source_dir.rglob("*.sql"))


def _structural_sources(cache, role, version):
    checkout = _role_checkout(cache, role)
    postgres = _postgres_source_dir(cache, version)
    return {
        "grammar": _require_file(
            postgres / "src" / "backend" / "parser" / "gram.y",
            "PostgreSQL grammar",
        ).read_text(encoding="utf-8"),
        "keyword": _require_file(
            postgres / "src" / "include" / "parser" / "kwlist.h",
            "PostgreSQL keyword list",
        ).read_text(encoding="utf-8"),
        "parse_node": _require_file(
            postgres / "src" / "include" / "nodes" / "parsenodes.h",
            "PostgreSQL parse node header",
        ).read_text(encoding="utf-8"),
        "protobuf": _require_file(
            checkout / "protobuf" / "pg_query.proto",
            "libpg_query protobuf schema",
        ).read_text(encoding="utf-8"),
    }


def collect_inventory(cache, role, repo_root, pins):
    version = pins["versions"][role]
    runner = runner_path(cache, role)
    rows = []
    errors = []
    for sql_file in _postgres_sql_files(cache, version["pg_version"]):
        try:
            rows.extend(
                _run_runner_file(
                    runner,
                    sql_file,
                    branch=version["branch"],
                    commit=version["commit"],
                )
            )
        except RuntimeError as error:
            errors.append(str(error))
    if errors:
        raise RuntimeError(
            "PostgreSQL compatibility runner failed:\n"
            + "\n".join(errors[:50])
            + (f"\n... {len(errors) - 50} more" if len(errors) > 50 else "")
        )

    accepted, _diagnostics = partition_rows(rows)
    return build_inventory(accepted)


def _target_runner_args(pins):
    target = pins["versions"]["target"]
    return ["--branch", target["branch"], "--commit", target["commit"]]


def run_committed_ci_cases(repo_root, runner, runner_args):
    repo_root = Path(repo_root)
    cases_path = repo_root / "tests" / "pg_compat" / "ci_cases.jsonl"
    if not cases_path.exists():
        return {"checked": 0, "skipped": True}

    cases = _load_jsonl(cases_path)
    branch = runner_args[runner_args.index("--branch") + 1]
    commit = runner_args[runner_args.index("--commit") + 1]
    checked = 0
    for case in cases:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            suffix=".sql",
        ) as sql_file:
            sql_file.write(case["sql"].rstrip() + ";\n")
            sql_file.flush()
            rows = _run_runner_file(
                runner,
                sql_file.name,
                branch=branch,
                commit=commit,
            )
        if len(rows) != 1:
            raise AssertionError(f"CI case {case['id']}: expected one row")
        actual = rows[0]
        if actual["result"] != case["expected_result"]:
            raise AssertionError(
                f"CI case {case['id']}: expected result "
                f"{case['expected_result']}, got {actual['result']}"
            )
        if actual["oracle_node"] != case["oracle_node"]:
            raise AssertionError(
                f"CI case {case['id']}: expected oracle node "
                f"{case['oracle_node']}, got {actual['oracle_node']}"
            )
        checked += 1
    return {"checked": checked, "skipped": False}


def run_build(args, repo_root, pins):
    for role in ROLES:
        build_runner(args.cache, role, repo_root)
    build_statement_type_test(args.cache, repo_root)


def run_test(args, repo_root, pins):
    statement_test = build_statement_type_test(args.cache, repo_root)
    _run([str(statement_test)], timeout=TIMEOUT_SECONDS)
    result = run_committed_ci_cases(
        repo_root,
        runner_path(args.cache, "target"),
        _target_runner_args(pins),
    )
    if result["skipped"]:
        print("No committed pg_compat CI cases yet; skipping case replay.")
    else:
        print(f"Checked {result['checked']} committed pg_compat CI cases.")


def _fetch_sources(repo_root, cache):
    env = os.environ.copy()
    env["PG_COMPAT_CACHE"] = str(cache)
    _run(
        [
            str(repo_root / "scripts" / "pg_compat" / "fetch_libpg_query.sh"),
            "--with-postgres-source",
        ],
        cwd=repo_root,
        env=env,
        timeout=None,
    )


def _report_context(repo_root, pins, mode):
    commit = subprocess.check_output(
        ["git", "rev-parse", "HEAD"],
        cwd=repo_root,
        text=True,
    ).strip()
    return {
        "generated_at": datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z"),
        "parsersql_commit": commit,
        "pins": pins,
        "commands": [
            f"make pg-compat-refresh PG_COMPAT_CACHE={DEFAULT_CACHE}",
            f"make pg-compat PG_COMPAT_CACHE={DEFAULT_CACHE}",
            f"python3 ./scripts/pg_compat/run_compat.py {mode} "
            f"--cache {DEFAULT_CACHE}",
        ],
    }


def _load_structural_dispositions(repo_root):
    path = repo_root / "tests" / "pg_compat" / "structural_dispositions.json"
    if not path.exists():
        return []
    with path.open(encoding="utf-8") as input_file:
        value = json.load(input_file)
    if not isinstance(value, list):
        raise ValueError("structural_dispositions.json must contain a JSON array")
    return value


def _apply_structural_dispositions(features, dispositions):
    by_id = {row["feature_id"]: row for row in dispositions}
    output = []
    for feature in features:
        row = dict(feature)
        disposition = by_id.get(row["id"])
        if disposition is not None:
            row["disposition"] = disposition["disposition"]
            row["reason"] = disposition["reason"]
        output.append(row)
    return output


def run_full_or_refresh(args, repo_root, pins, *, refresh):
    _fetch_sources(repo_root, args.cache)
    run_build(args, repo_root, pins)

    previous_inventory = collect_inventory(args.cache, "previous", repo_root, pins)
    target_inventory = collect_inventory(args.cache, "target", repo_root, pins)
    delta = compare_inventories(previous_inventory, target_inventory)

    expected_path = repo_root / "tests" / "pg_compat" / "expected_results.jsonl"
    if expected_path.exists():
        expected_rows = _load_jsonl(expected_path)
        baseline = evaluate_baseline(expected_rows, target_inventory)
    else:
        baseline = {
            "allowed": [],
            "regressions": [],
            "review_required": [],
            "new_cases": target_inventory,
            "missing_ids": [],
        }

    target = pins["versions"]["target"]
    witness_result = load_reviewed_witnesses(
        repo_root / "tests" / "pg_compat" / "witnesses.sql",
        repo_root / "tests" / "pg_compat" / "witnesses.json",
        runner_path(args.cache, "target"),
        target_major=int(str(target["pg_version"]).split(".", maxsplit=1)[0]),
        branch=target["branch"],
        commit=target["commit"],
    )

    structural = compare_structural_sources(
        _structural_sources(
            args.cache,
            "previous",
            pins["versions"]["previous"]["pg_version"],
        ),
        _structural_sources(
            args.cache,
            "target",
            target["pg_version"],
        ),
    )
    structural = _apply_structural_dispositions(
        structural,
        _load_structural_dispositions(repo_root),
    )

    ci_cases = build_ci_cases(
        target_inventory,
        delta["added"],
        witness_result["witnesses"],
    )
    report = generate_report(
        _report_context(repo_root, pins, "refresh" if refresh else "full"),
        target_inventory,
        delta["added"],
        structural,
        baseline,
        witness_result,
    )

    if refresh:
        atomic_write_text(expected_path, _jsonl_text(target_inventory))
        write_ci_cases(
            repo_root / "tests" / "pg_compat" / "ci_cases.jsonl",
            ci_cases,
        )
        atomic_write_text(
            repo_root / "docs" / "compatibility" / "postgresql-18.md",
            report,
        )
        print(f"Wrote {len(target_inventory)} baseline rows.")
        print(f"Wrote {len(ci_cases)} CI cases.")
        print(f"Structural features: {len(structural)}.")
        return

    failures = []
    for key in ("regressions", "review_required", "new_cases", "missing_ids"):
        if baseline.get(key):
            failures.append(f"{key}: {len(baseline[key])}")
    if failures:
        raise AssertionError(
            "committed PostgreSQL compatibility baseline differs: "
            + ", ".join(failures)
        )
    run_test(args, repo_root, pins)
    print("Committed PostgreSQL compatibility baseline matches.")


def parse_args(argv):
    parser = argparse.ArgumentParser(
        description="Run ParserSQL PostgreSQL compatibility workflows.",
    )
    parser.add_argument(
        "mode",
        choices=("build", "test", "full", "refresh"),
    )
    parser.add_argument(
        "--cache",
        type=Path,
        default=DEFAULT_CACHE,
    )
    parser.add_argument(
        "--pins",
        type=Path,
        default=Path("tests/pg_compat/upstream_pins.json"),
    )
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv or sys.argv[1:])
    repo_root = Path(__file__).resolve().parents[2]
    pins = load_pins(repo_root / args.pins)
    if args.mode == "build":
        run_build(args, repo_root, pins)
    elif args.mode == "test":
        run_test(args, repo_root, pins)
    elif args.mode == "full":
        run_full_or_refresh(args, repo_root, pins, refresh=False)
    elif args.mode == "refresh":
        run_full_or_refresh(args, repo_root, pins, refresh=True)
    else:
        raise AssertionError(args.mode)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
