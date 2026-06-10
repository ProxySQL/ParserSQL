import hashlib
import json
import re
import tempfile
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
        previous = {
            "branch": "17-latest",
            "commit": "a" * 40,
            "pg_version": "17.7",
            "pg_version_num": 170007,
            "postgres_sha256": "b" * 64,
        }
        with tempfile.TemporaryDirectory() as directory:
            path = self.write_pins(
                directory,
                {
                    "libpg_query_url": "https://example.invalid/repo.git",
                    "versions": {"previous": previous},
                },
            )

            with self.assertRaisesRegex(ValueError, r"versions\.target"):
                load_pins(path)

    def test_rejects_missing_url_and_version_fields(self):
        valid_version = {
            "branch": "18-latest",
            "commit": "a" * 40,
            "pg_version": "18.4",
            "pg_version_num": 180004,
            "postgres_sha256": "b" * 64,
        }
        cases = [
            ({}, "libpg_query_url"),
            (
                {"libpg_query_url": "url", "versions": {"previous": {}, "target": valid_version}},
                r"versions\.previous\.branch",
            ),
        ]

        with tempfile.TemporaryDirectory() as directory:
            for index, (data, message) in enumerate(cases):
                with self.subTest(message=message):
                    path = self.write_pins(Path(directory) / str(index), data)
                    with self.assertRaisesRegex(ValueError, message):
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


if __name__ == "__main__":
    unittest.main()
