import copy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

from scripts.pg_compat.common import statement_id
from scripts.pg_compat.extract_statements import build_inventory, partition_rows


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "pg_compat" / "extract_statements.py"


def accepted_row(
    *,
    source_file="b.sql",
    offset=20,
    line=2,
    sql="SELECT 1",
    normalized_sql="SELECT 1",
    oracle_node="SelectStmt",
    result="DEEP_SUPPORTED",
    **metadata,
):
    return {
        "source_file": source_file,
        "offset": offset,
        "line": line,
        "sql": sql,
        "normalized_sql": normalized_sql,
        "oracle_node": oracle_node,
        "result": result,
        **metadata,
    }


class PartitionRowsTest(unittest.TestCase):
    def test_partitions_oracle_rejections_without_mutating_input(self):
        accepted = accepted_row()
        rejected = accepted_row(
            source_file="bad.sql",
            result="ORACLE_REJECTED",
            oracle_error="syntax error",
        )
        rows = [accepted, rejected]
        original = copy.deepcopy(rows)

        accepted_rows, diagnostics = partition_rows(rows)

        self.assertEqual(accepted_rows, [accepted])
        self.assertEqual(diagnostics, [rejected])
        self.assertEqual(rows, original)


class BuildInventoryTest(unittest.TestCase):
    def test_groups_occurrences_and_uses_sorted_first_as_canonical(self):
        later = accepted_row(
            source_file="z.sql",
            offset=20,
            line=4,
            branch="target",
        )
        earlier = accepted_row(
            source_file="a.sql",
            offset=10,
            line=2,
            branch="previous",
        )

        inventory = build_inventory([later, earlier])

        self.assertEqual(len(inventory), 1)
        record = inventory[0]
        self.assertEqual(
            record["id"],
            statement_id("SELECT 1", "SelectStmt"),
        )
        self.assertEqual(record["source_file"], "a.sql")
        self.assertEqual(record["offset"], 10)
        self.assertEqual(record["line"], 2)
        self.assertEqual(record["branch"], "previous")
        self.assertEqual(
            record["occurrences"],
            [
                {"source_file": "a.sql", "offset": 10, "line": 2},
                {"source_file": "z.sql", "offset": 20, "line": 4},
            ],
        )

    def test_duplicate_identical_occurrence_collapses(self):
        row = accepted_row(source_file="same.sql", offset=7, line=3)

        inventory = build_inventory([row, copy.deepcopy(row)])

        self.assertEqual(
            inventory[0]["occurrences"],
            [{"source_file": "same.sql", "offset": 7, "line": 3}],
        )

    def test_ignores_oracle_rejections_defensively(self):
        rejected = accepted_row(result="ORACLE_REJECTED")
        accepted = accepted_row(source_file="good.sql")

        self.assertEqual(build_inventory([rejected, accepted]), build_inventory([accepted]))

    def test_same_normalized_sql_with_different_nodes_has_distinct_ids(self):
        rows = [
            accepted_row(oracle_node="SelectStmt"),
            accepted_row(oracle_node="ExplainStmt"),
        ]

        inventory = build_inventory(rows)

        self.assertEqual(len(inventory), 2)
        ids = [record["id"] for record in inventory]
        self.assertEqual(len(set(ids)), 2)
        self.assertEqual(ids, sorted(ids))
        self.assertEqual(
            {record["oracle_node"] for record in inventory},
            {"SelectStmt", "ExplainStmt"},
        )

    def test_result_is_independent_of_input_order(self):
        rows = [
            accepted_row(source_file="z.sql", offset=2, line=3),
            accepted_row(source_file="a.sql", offset=5, line=1),
            accepted_row(
                source_file="m.sql",
                offset=4,
                line=2,
                sql="INSERT INTO t VALUES (1)",
                normalized_sql="INSERT INTO t VALUES ($1)",
                oracle_node="InsertStmt",
            ),
        ]

        self.assertEqual(build_inventory(rows), build_inventory(list(reversed(rows))))

    def test_missing_required_fields_report_row_index_and_field(self):
        required_fields = (
            "result",
            "normalized_sql",
            "oracle_node",
            "source_file",
            "offset",
            "line",
            "sql",
        )

        for field in required_fields:
            with self.subTest(field=field):
                row = accepted_row()
                del row[field]
                with self.assertRaisesRegex(
                    ValueError,
                    rf"row 0.*missing required field.*{field}",
                ):
                    build_inventory([row])

    def test_detects_statement_id_collision(self):
        rows = [
            accepted_row(normalized_sql="SELECT 1", oracle_node="SelectStmt"),
            accepted_row(
                normalized_sql="INSERT INTO t VALUES ($1)",
                oracle_node="InsertStmt",
            ),
        ]

        with mock.patch(
            "scripts.pg_compat.extract_statements.statement_id",
            return_value="a" * 24,
        ):
            with self.assertRaisesRegex(ValueError, "statement ID collision"):
                build_inventory(rows)


class ExtractStatementsCliTest(unittest.TestCase):
    def run_cli(self, *arguments):
        return subprocess.run(
            [sys.executable, str(SCRIPT), *map(str, arguments)],
            cwd=ROOT,
            capture_output=True,
            text=True,
        )

    def test_writes_deterministic_jsonl_preserves_diagnostics_and_is_idempotent(self):
        rows = [
            accepted_row(source_file="z.sql", offset=20, line=4, extra="later"),
            accepted_row(source_file="a.sql", offset=10, line=2, extra="first"),
            accepted_row(
                source_file="bad-2.sql",
                offset=30,
                line=6,
                result="ORACLE_REJECTED",
                diagnostic="second",
            ),
            accepted_row(
                source_file="bad-1.sql",
                offset=5,
                line=1,
                result="ORACLE_REJECTED",
                diagnostic="first",
            ),
        ]

        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            input_path = directory / "raw.jsonl"
            inventory_path = directory / "inventory.jsonl"
            diagnostics_path = directory / "diagnostics.jsonl"
            input_path.write_text(
                "".join(json.dumps(row) + "\n" for row in rows),
                encoding="utf-8",
            )

            first = self.run_cli(
                "--input",
                input_path,
                "--inventory",
                inventory_path,
                "--diagnostics",
                diagnostics_path,
            )
            self.assertEqual(first.returncode, 0, first.stderr)
            first_inventory = inventory_path.read_text(encoding="utf-8")
            first_diagnostics = diagnostics_path.read_text(encoding="utf-8")

            second = self.run_cli(
                "--input",
                input_path,
                "--inventory",
                inventory_path,
                "--diagnostics",
                diagnostics_path,
            )
            self.assertEqual(second.returncode, 0, second.stderr)
            self.assertEqual(
                inventory_path.read_text(encoding="utf-8"),
                first_inventory,
            )
            self.assertEqual(
                diagnostics_path.read_text(encoding="utf-8"),
                first_diagnostics,
            )

            inventory_lines = first_inventory.splitlines()
            diagnostics_lines = first_diagnostics.splitlines()
            self.assertTrue(first_inventory.endswith("\n"))
            self.assertTrue(first_diagnostics.endswith("\n"))
            self.assertEqual(len(inventory_lines), 1)
            self.assertEqual(
                json.loads(inventory_lines[0])["occurrences"],
                [
                    {"line": 2, "offset": 10, "source_file": "a.sql"},
                    {"line": 4, "offset": 20, "source_file": "z.sql"},
                ],
            )
            self.assertEqual(
                [json.loads(line)["diagnostic"] for line in diagnostics_lines],
                ["second", "first"],
            )
            self.assertEqual(
                inventory_lines[0],
                json.dumps(
                    json.loads(inventory_lines[0]),
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            )

    def test_validation_failure_does_not_publish_either_output(self):
        invalid_row = accepted_row()
        del invalid_row["normalized_sql"]

        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            input_path = directory / "raw.jsonl"
            inventory_path = directory / "inventory.jsonl"
            diagnostics_path = directory / "diagnostics.jsonl"
            input_path.write_text(json.dumps(invalid_row) + "\n", encoding="utf-8")
            inventory_path.write_text("old inventory\n", encoding="utf-8")
            diagnostics_path.write_text("old diagnostics\n", encoding="utf-8")

            result = self.run_cli(
                "--input",
                input_path,
                "--inventory",
                inventory_path,
                "--diagnostics",
                diagnostics_path,
            )

            self.assertNotEqual(result.returncode, 0)
            self.assertIn("row 0", result.stderr)
            self.assertIn("normalized_sql", result.stderr)
            self.assertEqual(
                inventory_path.read_text(encoding="utf-8"),
                "old inventory\n",
            )
            self.assertEqual(
                diagnostics_path.read_text(encoding="utf-8"),
                "old diagnostics\n",
            )

    def test_argparse_rejects_missing_and_unknown_options(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            missing = self.run_cli()
            unknown = self.run_cli(
                "--input",
                directory / "raw.jsonl",
                "--inventory",
                directory / "inventory.jsonl",
                "--diagnostics",
                directory / "diagnostics.jsonl",
                "--unknown",
            )

        self.assertNotEqual(missing.returncode, 0)
        self.assertNotEqual(unknown.returncode, 0)

    def test_argparse_rejects_abbreviated_long_options(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            input_path = directory / "raw.jsonl"
            inventory_path = directory / "inventory.jsonl"
            diagnostics_path = directory / "diagnostics.jsonl"
            input_path.write_text("", encoding="utf-8")
            option_cases = (
                ("--inp", "--inventory", "--diagnostics"),
                ("--input", "--invent", "--diagnostics"),
                ("--input", "--inventory", "--diag"),
            )

            for input_option, inventory_option, diagnostics_option in option_cases:
                with self.subTest(
                    input_option=input_option,
                    inventory_option=inventory_option,
                    diagnostics_option=diagnostics_option,
                ):
                    result = self.run_cli(
                        input_option,
                        input_path,
                        inventory_option,
                        inventory_path,
                        diagnostics_option,
                        diagnostics_path,
                    )

                    self.assertNotEqual(result.returncode, 0)
