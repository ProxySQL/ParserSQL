import copy
from contextlib import redirect_stderr
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

from scripts.pg_compat.common import statement_id
from scripts.pg_compat.extract_statements import (
    build_inventory,
    main,
    partition_rows,
)


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
        accepted = accepted_row(metadata={"tags": ["accepted"]})
        rejected = accepted_row(
            source_file="bad.sql",
            result="ORACLE_REJECTED",
            oracle_error="syntax error",
            metadata={"tags": ["rejected"]},
        )
        rows = [accepted, rejected]
        original = copy.deepcopy(rows)

        accepted_rows, diagnostics = partition_rows(rows)

        self.assertEqual(accepted_rows, [accepted])
        self.assertEqual(diagnostics, [rejected])
        accepted_rows[0]["metadata"]["tags"].append("changed")
        diagnostics[0]["metadata"]["tags"].append("changed")
        self.assertEqual(rows, original)

    def test_validates_diagnostic_result_and_common_metadata(self):
        self.assertEqual(
            partition_rows([{"result": "ORACLE_REJECTED"}]),
            ([], [{"result": "ORACLE_REJECTED"}]),
        )
        invalid_rows = (
            ({"result": 1}, "result"),
            ({"result": "UNKNOWN"}, "result"),
            ({"result": "ORACLE_REJECTED", "source_file": 1}, "source_file"),
            ({"result": "ORACLE_REJECTED", "sql": 1}, "sql"),
            ({"result": "ORACLE_REJECTED", "oracle_error": 1}, "oracle_error"),
            ({"result": "ORACLE_REJECTED", "offset": True}, "offset"),
            ({"result": "ORACLE_REJECTED", "offset": -1}, "offset"),
            ({"result": "ORACLE_REJECTED", "line": 0}, "line"),
        )

        for row, field in invalid_rows:
            with self.subTest(row=row):
                with self.assertRaisesRegex(ValueError, rf"row 0.*field.*{field}"):
                    partition_rows([row])


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

    def test_validates_accepted_field_types_and_ranges(self):
        invalid_values = {
            "result": (1, "UNKNOWN"),
            "normalized_sql": (1,),
            "oracle_node": (1,),
            "source_file": (1,),
            "sql": (1,),
            "offset": ("2", True, 1.5, -1),
            "line": ("1", True, 1.5, 0),
            "branch": (1,),
            "commit": (1,),
        }

        for field, values in invalid_values.items():
            for value in values:
                with self.subTest(field=field, value=value):
                    row = accepted_row()
                    row[field] = value
                    with self.assertRaisesRegex(
                        ValueError,
                        rf"row 0.*field.*{field}",
                    ):
                        build_inventory([row])

    def test_rejects_mixed_invalid_occurrence_offsets_cleanly(self):
        rows = [
            accepted_row(source_file="a.sql", offset=1),
            accepted_row(source_file="b.sql", offset="2"),
        ]

        with self.assertRaisesRegex(ValueError, r"row 1.*field.*offset"):
            build_inventory(rows)

    def test_rejects_non_string_row_keys_with_context(self):
        row = accepted_row()
        row[1] = "invalid key"

        with self.assertRaisesRegex(ValueError, r"row 0.*field.*1.*string"):
            build_inventory([row])

    def test_rejects_non_utf8_top_level_field_names_with_context(self):
        row = accepted_row()
        row["\ud800"] = "invalid key"

        with self.assertRaises(ValueError) as raised:
            build_inventory([row])

        message = str(raised.exception)
        self.assertIn("row 0", message)
        self.assertIn("field name", message)
        self.assertIn("\\ud800", message)
        self.assertIn("UTF-8", message)

    def test_rejects_non_finite_and_non_utf8_metadata(self):
        invalid_metadata = (
            (float("nan"), "JSON"),
            ("\ud800", "UTF-8"),
        )

        for value, message in invalid_metadata:
            with self.subTest(message=message):
                row = accepted_row(metadata={"value": value})
                with self.assertRaisesRegex(
                    ValueError,
                    rf"row 0.*field.*metadata.*{message}",
                ):
                    build_inventory([row])

    def test_inventory_deep_copies_nested_metadata(self):
        row = accepted_row(metadata={"tags": ["original"]})

        inventory = build_inventory([row])
        inventory[0]["metadata"]["tags"].append("changed")

        self.assertEqual(row["metadata"], {"tags": ["original"]})

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

    def test_strict_json_failures_are_clean_and_do_not_publish(self):
        invalid_rows = (
            accepted_row(metadata={"value": float("nan")}),
            accepted_row(metadata={"value": "\ud800"}),
        )

        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            input_path = directory / "raw.jsonl"
            inventory_path = directory / "inventory.jsonl"
            diagnostics_path = directory / "diagnostics.jsonl"

            for invalid_row in invalid_rows:
                with self.subTest(invalid_row=repr(invalid_row)):
                    input_path.write_text(
                        json.dumps(invalid_row, ensure_ascii=True) + "\n",
                        encoding="utf-8",
                    )
                    inventory_path.write_text("old inventory\n", encoding="utf-8")
                    diagnostics_path.write_text(
                        "old diagnostics\n",
                        encoding="utf-8",
                    )

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
                    self.assertIn("metadata", result.stderr)
                    self.assertNotIn("Traceback", result.stderr)
                    self.assertEqual(
                        inventory_path.read_text(encoding="utf-8"),
                        "old inventory\n",
                    )
                    self.assertEqual(
                        diagnostics_path.read_text(encoding="utf-8"),
                        "old diagnostics\n",
                    )

    def test_rejects_same_path_aliases_before_reading_or_writing(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            input_path = directory / "raw.jsonl"
            shared_output_path = directory / "shared.jsonl"
            input_path.write_text("not JSON\n", encoding="utf-8")

            result = self.run_cli(
                "--input",
                input_path,
                "--inventory",
                shared_output_path,
                "--diagnostics",
                shared_output_path,
            )

            self.assertNotEqual(result.returncode, 0)
            self.assertIn("distinct files", result.stderr)
            self.assertEqual(input_path.read_text(encoding="utf-8"), "not JSON\n")
            self.assertFalse(shared_output_path.exists())

    def test_rejects_nonexistent_output_names_differing_only_by_case(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            input_path = directory / "raw.jsonl"
            inventory_path = directory / "Inventory.jsonl"
            diagnostics_path = directory / "inventory.jsonl"
            input_path.write_text("", encoding="utf-8")

            result = self.run_cli(
                "--input",
                input_path,
                "--inventory",
                inventory_path,
                "--diagnostics",
                diagnostics_path,
            )

            self.assertNotEqual(result.returncode, 0)
            self.assertIn("distinct files", result.stderr)
            self.assertFalse(inventory_path.exists())
            self.assertFalse(diagnostics_path.exists())

    def test_rejects_nonexistent_output_names_with_equivalent_unicode(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            input_path = directory / "raw.jsonl"
            inventory_path = directory / "caf\u00e9.jsonl"
            diagnostics_path = directory / "cafe\u0301.jsonl"
            input_path.write_text("", encoding="utf-8")

            result = self.run_cli(
                "--input",
                input_path,
                "--inventory",
                inventory_path,
                "--diagnostics",
                diagnostics_path,
            )

            self.assertNotEqual(result.returncode, 0)
            self.assertIn("distinct files", result.stderr)
            self.assertFalse(inventory_path.exists())
            self.assertFalse(diagnostics_path.exists())

    @unittest.skipUnless(hasattr(os, "symlink"), "symlinks are unavailable")
    def test_rejects_existing_symlink_path_aliases(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            input_path = directory / "raw.jsonl"
            inventory_path = directory / "inventory.jsonl"
            diagnostics_path = directory / "diagnostics.jsonl"
            input_path.write_text("", encoding="utf-8")
            inventory_path.symlink_to(input_path)

            result = self.run_cli(
                "--input",
                input_path,
                "--inventory",
                inventory_path,
                "--diagnostics",
                diagnostics_path,
            )

            self.assertNotEqual(result.returncode, 0)
            self.assertIn("distinct files", result.stderr)
            self.assertFalse(diagnostics_path.exists())

    def test_atomic_publication_errors_are_reported_without_traceback(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            input_path = directory / "raw.jsonl"
            input_path.write_text("", encoding="utf-8")
            stderr = io.StringIO()

            with mock.patch(
                "scripts.pg_compat.extract_statements.atomic_write_text",
                side_effect=OSError("disk full"),
            ):
                with redirect_stderr(stderr), self.assertRaises(SystemExit) as raised:
                    main(
                        [
                            "--input",
                            str(input_path),
                            "--inventory",
                            str(directory / "inventory.jsonl"),
                            "--diagnostics",
                            str(directory / "diagnostics.jsonl"),
                        ]
                    )

            self.assertEqual(raised.exception.code, 1)
            self.assertIn("disk full", stderr.getvalue())
            self.assertNotIn("Traceback", stderr.getvalue())

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
