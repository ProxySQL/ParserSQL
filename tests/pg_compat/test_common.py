import hashlib
import json
import os
import re
import signal
import subprocess
import tempfile
import time
import unittest
from pathlib import Path

from scripts.pg_compat.common import (
    atomic_write_text,
    load_pins,
    parse_makefile_pg_version,
    read_jsonl,
    statement_id,
)


ROOT = Path(__file__).resolve().parents[2]
PINS_PATH = ROOT / "tests" / "pg_compat" / "upstream_pins.json"
FETCH_SCRIPT = ROOT / "scripts" / "pg_compat" / "fetch_libpg_query.sh"


def valid_version(pg_version="18.4", pg_version_num=180004, postgres_sha256=None):
    return {
        "branch": "18-latest",
        "commit": "a" * 40,
        "pg_version": pg_version,
        "pg_version_num": pg_version_num,
        "postgres_sha256": postgres_sha256 or "b" * 64,
    }


def valid_pins(postgres_sha256=None):
    return {
        "libpg_query_url": "https://example.invalid/libpg_query.git",
        "versions": {
            "previous": valid_version(
                pg_version="17.7",
                pg_version_num=170007,
                postgres_sha256=postgres_sha256,
            ),
            "target": valid_version(postgres_sha256=postgres_sha256),
        },
    }


class LoadPinsTest(unittest.TestCase):
    def write_pins(self, directory, data):
        path = Path(directory) / "pins.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data), encoding="utf-8")
        return path

    def test_rejects_missing_previous_role(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self.write_pins(
                directory,
                {"libpg_query_url": "https://example.invalid/repo.git", "versions": {"target": {}}},
            )

            with self.assertRaisesRegex(ValueError, r"versions\.previous"):
                load_pins(path)

    def test_rejects_missing_target_role(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self.write_pins(
                directory,
                {
                    "libpg_query_url": "https://example.invalid/repo.git",
                    "versions": {"previous": valid_version("17.7", 170007)},
                },
            )

            with self.assertRaisesRegex(ValueError, r"versions\.target"):
                load_pins(path)

    def test_rejects_missing_url_and_version_fields(self):
        cases = [
            ({}, "libpg_query_url"),
            (
                {
                    "libpg_query_url": "https://example.invalid/repo.git",
                    "versions": {"previous": {}, "target": valid_version()},
                },
                r"versions\.previous\.branch",
            ),
        ]

        with tempfile.TemporaryDirectory() as directory:
            for index, (data, message) in enumerate(cases):
                with self.subTest(message=message):
                    path = self.write_pins(Path(directory) / str(index), data)
                    with self.assertRaisesRegex(ValueError, message):
                        load_pins(path)

    def test_rejects_malformed_pin_values(self):
        cases = [
            ("libpg_query_url", "", "non-empty string"),
            ("branch", "", "non-empty string"),
            ("branch", "   ", "non-empty string"),
            ("commit", "A" * 40, "40 lowercase hexadecimal"),
            ("commit", "a" * 39, "40 lowercase hexadecimal"),
            ("pg_version", "../18.4", "numeric dotted"),
            ("pg_version", "18beta1", "numeric dotted"),
            ("pg_version_num", True, "integer"),
            ("pg_version_num", "180004", "integer"),
            ("postgres_sha256", "B" * 64, "64 lowercase hexadecimal"),
            ("postgres_sha256", "b" * 63, "64 lowercase hexadecimal"),
        ]

        with tempfile.TemporaryDirectory() as directory:
            for index, (field, value, message) in enumerate(cases):
                with self.subTest(field=field, value=value):
                    pins = valid_pins()
                    if field == "libpg_query_url":
                        pins[field] = value
                        expected = field
                    else:
                        pins["versions"]["previous"][field] = value
                        expected = rf"versions\.previous\.{field}"
                    path = self.write_pins(Path(directory) / str(index), pins)

                    with self.assertRaisesRegex(
                        ValueError, rf"{expected}.*{message}"
                    ):
                        load_pins(path)

    def test_rejects_ascii_control_characters_in_branch(self):
        with tempfile.TemporaryDirectory() as directory:
            for codepoint in (*range(0x20), 0x7F):
                with self.subTest(codepoint=codepoint):
                    pins = valid_pins()
                    pins["versions"]["previous"]["branch"] = (
                        f"17-{chr(codepoint)}latest"
                    )
                    path = self.write_pins(
                        Path(directory) / str(codepoint), pins
                    )

                    with self.assertRaisesRegex(
                        ValueError,
                        r"versions\.previous\.branch.*ASCII control",
                    ):
                        load_pins(path)


class ParseMakefileVersionTest(unittest.TestCase):
    def test_parses_pg_version_and_numeric_version(self):
        text = """
OTHER = PG_VERSION = 1.0
PG_VERSION = 18.4
PG_VERSION_NUM = 180004
"""

        self.assertEqual(parse_makefile_pg_version(text), ("18.4", 180004))

    def test_rejects_missing_version_values(self):
        with self.assertRaisesRegex(ValueError, "PG_VERSION_NUM"):
            parse_makefile_pg_version("PG_VERSION = 18.4\n")


class RepositoryPinsTest(unittest.TestCase):
    def test_commits_and_postgres_hashes_are_immutable_lowercase_hex(self):
        pins = load_pins(PINS_PATH)

        for role in ("previous", "target"):
            with self.subTest(role=role):
                version = pins["versions"][role]
                self.assertRegex(version["commit"], re.compile(r"^[0-9a-f]{40}$"))
                self.assertRegex(version["postgres_sha256"], re.compile(r"^[0-9a-f]{64}$"))


class CommonHelpersTest(unittest.TestCase):
    def test_statement_id_hashes_node_nul_sql_and_truncates(self):
        normalized_sql = "SELECT $1"
        oracle_node = "SelectStmt"
        expected = hashlib.sha256(b"SelectStmt\0SELECT $1").hexdigest()[:24]

        self.assertEqual(statement_id(normalized_sql, oracle_node), expected)

    def test_read_jsonl_skips_blanks_and_decodes_objects(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "records.jsonl"
            path.write_text('{"id": 1}\n\n  \n{"id": 2}\n', encoding="utf-8")

            self.assertEqual(list(read_jsonl(path)), [{"id": 1}, {"id": 2}])

    def test_read_jsonl_reports_path_and_line_for_invalid_json(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "records.jsonl"
            path.write_text('{"id": 1}\n\nnot-json\n', encoding="utf-8")

            with self.assertRaisesRegex(ValueError, rf"{re.escape(str(path))}:3"):
                list(read_jsonl(path))

    def test_atomic_write_text_creates_parents_and_replaces_content(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "nested" / "output.txt"
            atomic_write_text(path, "first")
            atomic_write_text(path, "second")

            self.assertEqual(path.read_text(encoding="utf-8"), "second")


class FetchScriptIntegrationTest(unittest.TestCase):
    def write_executable(self, path, text):
        path.write_text(text, encoding="utf-8")
        path.chmod(0o755)

    def create_harness(self, directory, pins):
        root = Path(directory)
        bin_dir = root / "bin"
        bin_dir.mkdir()
        pins_path = root / "pins.json"
        pins_path.write_text(json.dumps(pins), encoding="utf-8")
        cache = root / "cache"
        content = root / "postgres.tar.bz2"
        content.write_bytes(b"good archive")

        self.write_executable(
            bin_dir / "git",
            """#!/usr/bin/env bash
set -euo pipefail
if [[ "${STUB_GIT_CALLED:-}" != "" ]]; then
    touch "$STUB_GIT_CALLED"
fi
if [[ "$1" == "clone" ]]; then
    checkout="${!#}"
    mkdir -p "$checkout/.git"
    if [[ "$(basename "$checkout")" == "previous" ]]; then
        printf 'PG_VERSION = 17.7\\nPG_VERSION_NUM = 170007\\n' > "$checkout/Makefile"
    else
        printf 'PG_VERSION = 18.4\\nPG_VERSION_NUM = 180004\\n' > "$checkout/Makefile"
    fi
    if [[ "${STUB_BLOCK_STARTED:-}" != "" && "$(basename "$checkout")" == "previous" ]]; then
        touch "$STUB_BLOCK_STARTED"
        while [[ ! -e "$STUB_BLOCK_RELEASE" ]]; do
            sleep 0.02
        done
    fi
    exit 0
fi
if [[ "$1" == "-C" ]]; then
    checkout="$2"
    shift 2
    case "$1" in
        remote|fetch)
            exit 0
            ;;
        checkout)
            printf '%s\\n' "${!#}" > "$checkout/.git/HEAD"
            exit 0
            ;;
        rev-parse)
            cat "$checkout/.git/HEAD"
            exit 0
            ;;
    esac
fi
echo "unexpected git invocation: $*" >&2
exit 64
""",
        )
        self.write_executable(
            bin_dir / "curl",
            """#!/usr/bin/env bash
set -euo pipefail
output=
while [[ $# -gt 0 ]]; do
    if [[ "$1" == "--output" ]]; then
        output="$2"
        shift 2
    else
        shift
    fi
done
cp "$STUB_CONTENT_FILE" "$output"
""",
        )
        self.write_executable(
            bin_dir / "tar",
            """#!/usr/bin/env bash
set -euo pipefail
archive=
destination=
while [[ $# -gt 0 ]]; do
    case "$1" in
        -xjf)
            archive="$2"
            shift 2
            ;;
        -C)
            destination="$2"
            shift 2
            ;;
        *)
            shift
            ;;
    esac
done
name="$(basename "$archive" .tar.bz2)"
mkdir -p "$destination/$name"
printf 'extracted\\n' > "$destination/$name/marker"
""",
        )

        environment = os.environ.copy()
        environment.update(
            {
                "PATH": f"{bin_dir}{os.pathsep}{environment['PATH']}",
                "PG_COMPAT_CACHE": str(cache),
                "PG_COMPAT_PINS": str(pins_path),
                "STUB_CONTENT_FILE": str(content),
            }
        )
        return root, cache, content, environment

    def run_fetch(self, environment, *arguments, timeout=10):
        return subprocess.run(
            [str(FETCH_SCRIPT), *arguments],
            cwd=ROOT,
            env=environment,
            text=True,
            capture_output=True,
            timeout=timeout,
        )

    def create_lock(self, cache, owner=None):
        lock_dir = cache / ".pg_compat.lock"
        lock_dir.mkdir(parents=True)
        if owner is not None:
            (lock_dir / "owner").write_text(owner, encoding="utf-8")
        return lock_dir

    def test_bad_download_is_not_published_and_poisoned_cache_recovers(self):
        good_content = b"good archive"
        expected_hash = hashlib.sha256(good_content).hexdigest()
        with tempfile.TemporaryDirectory() as directory:
            _, cache, content, environment = self.create_harness(
                directory, valid_pins(expected_hash)
            )
            content.write_bytes(b"bad archive")
            archive = cache / "postgresql" / "postgresql-17.7.tar.bz2"

            failed = self.run_fetch(environment, "--with-postgres-source")

            self.assertNotEqual(failed.returncode, 0)
            self.assertFalse(archive.exists())

            archive.parent.mkdir(parents=True, exist_ok=True)
            archive.write_bytes(b"poisoned canonical archive")
            content.write_bytes(good_content)
            recovered = self.run_fetch(environment, "--with-postgres-source")

            self.assertEqual(recovered.returncode, 0, recovered.stderr)
            self.assertEqual(archive.read_bytes(), good_content)
            self.assertTrue(
                (cache / "postgresql" / "postgresql-17.7" / "marker").is_file()
            )
            self.assertTrue(
                (cache / "postgresql" / "postgresql-18.4" / "marker").is_file()
            )

    def test_fetch_rejects_malformed_pins_before_invoking_git(self):
        pins = valid_pins()
        pins["versions"]["previous"]["commit"] = "not-a-commit"
        with tempfile.TemporaryDirectory() as directory:
            root, _, _, environment = self.create_harness(directory, pins)
            git_called = root / "git-called"
            environment["STUB_GIT_CALLED"] = str(git_called)

            result = self.run_fetch(environment)

            self.assertNotEqual(result.returncode, 0)
            self.assertIn("versions.previous.commit", result.stderr)
            self.assertFalse(git_called.exists())

    def test_concurrent_fetch_is_rejected_without_mutating_cache(self):
        with tempfile.TemporaryDirectory() as directory:
            root, cache, _, environment = self.create_harness(
                directory, valid_pins()
            )
            started = root / "started"
            release = root / "release"
            environment["STUB_BLOCK_STARTED"] = str(started)
            environment["STUB_BLOCK_RELEASE"] = str(release)

            first = subprocess.Popen(
                [str(FETCH_SCRIPT)],
                cwd=ROOT,
                env=environment,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            deadline = time.monotonic() + 5
            while not started.exists() and time.monotonic() < deadline:
                time.sleep(0.02)
            self.assertTrue(started.exists(), "first fetch did not reach locked operation")

            second_environment = environment.copy()
            second_environment.pop("STUB_BLOCK_STARTED")
            second_environment.pop("STUB_BLOCK_RELEASE")
            second = self.run_fetch(second_environment, timeout=2)
            release.touch()
            first_stdout, first_stderr = first.communicate(timeout=5)

            self.assertNotEqual(second.returncode, 0)
            self.assertIn("cache is locked", second.stderr)
            self.assertEqual(first.returncode, 0, first_stderr or first_stdout)
            self.assertEqual(
                sorted(path.name for path in (cache / "libpg_query").iterdir()),
                ["previous", "target"],
            )
            self.assertFalse((cache / ".pg_compat.lock").exists())

    def test_valid_dead_pid_lock_is_recovered(self):
        with tempfile.TemporaryDirectory() as directory:
            _, cache, _, environment = self.create_harness(
                directory, valid_pins()
            )
            dead_process = subprocess.Popen(["true"])
            dead_process.wait(timeout=5)
            self.create_lock(cache, f"{dead_process.pid}\tstale-token\n")

            result = self.run_fetch(environment)

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertFalse((cache / ".pg_compat.lock").exists())

    def test_live_pid_lock_is_rejected_without_reclamation(self):
        with tempfile.TemporaryDirectory() as directory:
            root, cache, _, environment = self.create_harness(
                directory, valid_pins()
            )
            owner = f"{os.getpid()}\tlive-token\n"
            lock_dir = self.create_lock(cache, owner)
            git_called = root / "git-called"
            environment["STUB_GIT_CALLED"] = str(git_called)

            result = self.run_fetch(environment)

            self.assertNotEqual(result.returncode, 0)
            self.assertIn("cache is locked", result.stderr)
            self.assertEqual(
                (lock_dir / "owner").read_text(encoding="utf-8"), owner
            )
            self.assertFalse(git_called.exists())

    def test_malformed_or_missing_owner_lock_is_rejected_without_reclamation(self):
        cases = (None, "not-a-valid-owner\n")
        with tempfile.TemporaryDirectory() as directory:
            for index, owner in enumerate(cases):
                with self.subTest(owner=owner):
                    harness_root = Path(directory) / str(index)
                    harness_root.mkdir()
                    root, cache, _, environment = self.create_harness(
                        harness_root, valid_pins()
                    )
                    lock_dir = self.create_lock(cache, owner)
                    git_called = root / "git-called"
                    environment["STUB_GIT_CALLED"] = str(git_called)

                    result = self.run_fetch(environment)

                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn("cache is locked", result.stderr)
                    self.assertTrue(lock_dir.is_dir())
                    if owner is None:
                        self.assertFalse((lock_dir / "owner").exists())
                    else:
                        self.assertEqual(
                            (lock_dir / "owner").read_text(encoding="utf-8"),
                            owner,
                        )
                    self.assertFalse(git_called.exists())

    def test_terminated_fetch_releases_lock_without_continuing(self):
        with tempfile.TemporaryDirectory() as directory:
            root, cache, _, environment = self.create_harness(
                directory, valid_pins()
            )
            started = root / "started"
            release = root / "release"
            environment["STUB_BLOCK_STARTED"] = str(started)
            environment["STUB_BLOCK_RELEASE"] = str(release)

            process = subprocess.Popen(
                [str(FETCH_SCRIPT)],
                cwd=ROOT,
                env=environment,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            deadline = time.monotonic() + 5
            while not started.exists() and time.monotonic() < deadline:
                time.sleep(0.02)
            self.assertTrue(started.exists(), "fetch did not reach locked operation")

            os.kill(process.pid, signal.SIGTERM)
            release.touch()
            process.communicate(timeout=5)

            self.assertNotEqual(process.returncode, 0)
            self.assertFalse((cache / ".pg_compat.lock").exists())

            retry_environment = environment.copy()
            retry_environment.pop("STUB_BLOCK_STARTED")
            retry_environment.pop("STUB_BLOCK_RELEASE")
            retry = self.run_fetch(retry_environment)
            self.assertEqual(retry.returncode, 0, retry.stderr)


if __name__ == "__main__":
    unittest.main()
