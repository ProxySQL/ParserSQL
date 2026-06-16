import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


RUNNER_VALUE = os.environ.get("PG_COMPAT_RUNNER")
if not RUNNER_VALUE:
    raise RuntimeError(
        "PG_COMPAT_RUNNER must name the compiled pg_compat runner"
    )

RUNNER = Path(RUNNER_VALUE)
REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURE = REPO_ROOT / "tests" / "pg_compat" / "runner_cases.sql"
USAGE = "Usage: pg_compat_runner --input FILE --branch NAME --commit SHA\n"
TIMEOUT_SECONDS = 5


class RunnerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not RUNNER.is_file():
            raise RuntimeError(f"PG_COMPAT_RUNNER does not exist: {RUNNER}")

    def run_runner(self, *arguments, **kwargs):
        return subprocess.run(
            [str(RUNNER), *map(str, arguments)],
            capture_output=True,
            timeout=TIMEOUT_SECONDS,
            **kwargs,
        )

    def run_sql(self, contents, *, branch="test", commit="deadbeef"):
        data = contents if isinstance(contents, bytes) else contents.encode()
        with tempfile.NamedTemporaryFile(suffix=".sql") as sql_file:
            sql_file.write(data)
            sql_file.flush()
            return self.run_runner(
                "--input",
                sql_file.name,
                "--branch",
                branch,
                "--commit",
                commit,
            )

    @staticmethod
    def json_rows(result):
        return [
            json.loads(line)
            for line in result.stdout.decode("utf-8").splitlines()
        ]

    def test_committed_fixture(self):
        result = self.run_runner(
            "--input",
            FIXTURE,
            "--branch",
            "18-latest",
            "--commit",
            "fixture-commit",
        )

        self.assertEqual(result.returncode, 0, result.stderr.decode())
        self.assertEqual(result.stderr, b"")
        rows = self.json_rows(result)
        self.assertEqual(len(rows), 5)
        self.assertEqual(
            [row["result"] for row in rows],
            [
                "DEEP_SUPPORTED",
                "CLASSIFIED_ONLY",
                "TYPE_MISMATCH",
                "DEEP_SUPPORTED",
                "DEEP_SUPPORTED",
            ],
        )
        self.assertEqual([row["offset"] for row in rows], [0, 35, 68, 82, 92])
        self.assertEqual([row["line"] for row in rows], [1, 2, 3, 4, 4])
        self.assertEqual([row["sql"] for row in rows[-2:]], ["SELECT 1", "SELECT 2"])
        self.assertTrue(all(row["source_file"] == str(FIXTURE) for row in rows))
        self.assertTrue(all(row["branch"] == "18-latest" for row in rows))
        self.assertTrue(all(row["commit"] == "fixture-commit" for row in rows))
        required_fields = {
            "source_file",
            "offset",
            "line",
            "splitter",
            "sql",
            "normalized_sql",
            "oracle_node",
            "expected_stmt_type",
            "parser_status",
            "parser_stmt_type",
            "has_ast",
            "remaining",
            "result",
            "branch",
            "commit",
        }
        self.assertTrue(all(required_fields <= row.keys() for row in rows))
        self.assertTrue(
            all(not row["remaining"].strip(" \t\r\n\f\v;") for row in rows)
        )

    def test_cli_no_arguments(self):
        result = self.run_runner()

        self.assertEqual(result.returncode, 2)
        self.assertEqual(result.stdout, b"")
        self.assertEqual(result.stderr.decode(), USAGE)

    def test_cli_missing_input_file(self):
        missing = Path(tempfile.gettempdir()) / "pg-compat-missing-input.sql"
        missing.unlink(missing_ok=True)

        result = self.run_runner(
            "--input",
            missing,
            "--branch",
            "test",
            "--commit",
            "deadbeef",
        )

        self.assertEqual(result.returncode, 1)
        self.assertEqual(result.stdout, b"")
        self.assertEqual(
            result.stderr.decode(),
            f"infrastructure error: cannot open input file: {missing}\n",
        )

    def test_cli_rejects_unknown_missing_and_duplicate_options(self):
        argument_cases = [
            ["--input", FIXTURE, "--branch", "test"],
            [
                "--input",
                FIXTURE,
                "--branch",
                "test",
                "--commit",
                "deadbeef",
                "--extra",
                "value",
            ],
            [
                "--input",
                FIXTURE,
                "--input",
                FIXTURE,
                "--branch",
                "test",
                "--commit",
                "deadbeef",
            ],
            [FIXTURE, "--branch", "test", "--commit", "deadbeef"],
            [
                "--input",
                FIXTURE,
                "--branch",
                "",
                "--commit",
                "deadbeef",
            ],
            [
                "--input",
                FIXTURE,
                "--branch",
                "--unknown",
                "--commit",
                "deadbeef",
            ],
        ]

        for arguments in argument_cases:
            with self.subTest(arguments=arguments):
                result = self.run_runner(*arguments)
                self.assertEqual(result.returncode, 2)
                self.assertEqual(result.stdout, b"")
                self.assertEqual(result.stderr.decode(), USAGE)

    def test_scanner_fallback_uses_physical_code_lines(self):
        result = self.run_sql(
            "SELECT FROM;\n"
            "-- comment before second\n"
            "SELECT 1;\n"
            "/* outer\n"
            " * /* nested */\n"
            " */\n"
            "SELECT 2;\n"
        )

        self.assertEqual(result.returncode, 0, result.stderr.decode())
        rows = self.json_rows(result)
        self.assertEqual(
            [row["result"] for row in rows],
            ["ORACLE_REJECTED", "DEEP_SUPPORTED", "DEEP_SUPPORTED"],
        )
        self.assertTrue(all(row["splitter"] == "scanner" for row in rows))
        self.assertEqual([row["line"] for row in rows[1:]], [3, 7])
        self.assertEqual(
            [row["sql"].strip()[-8:] for row in rows[1:]],
            ["SELECT 1", "SELECT 2"],
        )

    def test_no_equivalent_node_is_classified_not_infrastructure_failure(self):
        result = self.run_sql("LOAD 'foo';\n")

        self.assertEqual(result.returncode, 0, result.stderr.decode())
        rows = self.json_rows(result)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["oracle_node"], "PG_QUERY__NODE__NODE_LOAD_STMT")
        self.assertEqual(rows[0]["expected_stmt_type"], "UNKNOWN")
        self.assertEqual(rows[0]["parser_stmt_type"], "LOAD_DATA")
        self.assertEqual(rows[0]["result"], "PARTIAL")

    def test_comments_do_not_affect_normalization(self):
        result = self.run_sql(
            "SELECT 1;\n"
            "SELECT -- line comment\n"
            " 1;\n"
            "SELECT /* block comment */ 1;\n"
            "SELECT /* outer /* nested */ block */ 1;\n"
        )

        self.assertEqual(result.returncode, 0, result.stderr.decode())
        rows = self.json_rows(result)
        self.assertEqual(len(rows), 4)
        self.assertEqual(
            [row["normalized_sql"] for row in rows],
            ["SELECT 1"] * 4,
        )

    def test_json_escaping_preserves_valid_utf8_and_controls(self):
        sql = "SELECT 'quote \" slash \\\\ tab\t control\x01\nnext café';\n"
        result = self.run_sql(
            sql,
            branch='branch"quoted',
            commit="commit\\slash",
        )

        self.assertEqual(result.returncode, 0, result.stderr.decode())
        self.assertNotIn(b"\x01", result.stdout)
        self.assertIn(b"\\u0001", result.stdout)
        rows = self.json_rows(result)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["sql"], sql[:-2])
        self.assertEqual(rows[0]["branch"], 'branch"quoted')
        self.assertEqual(rows[0]["commit"], "commit\\slash")
        self.assertIn("café", rows[0]["sql"])

    def test_oracle_rejection_is_a_record(self):
        result = self.run_sql("SELECT FROM;\nSELECT 1;\n")

        self.assertEqual(result.returncode, 0, result.stderr.decode())
        rows = self.json_rows(result)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["result"], "ORACLE_REJECTED")
        self.assertTrue(rows[0]["oracle_error"])
        self.assertEqual(rows[1]["result"], "DEEP_SUPPORTED")

    def test_dual_splitter_failure_is_infrastructure_error(self):
        result = self.run_sql("SELECT 'unterminated")

        self.assertEqual(result.returncode, 1)
        self.assertEqual(result.stdout, b"")
        self.assertTrue(
            result.stderr.decode().startswith(
                "infrastructure error: statement splitting failed; parser: "
            )
        )
        self.assertIn("; scanner: ", result.stderr.decode())

    def test_embedded_nul_is_rejected_before_output(self):
        result = self.run_sql(b"SELECT 1;\x00SELECT 2;")

        self.assertEqual(result.returncode, 1)
        self.assertEqual(result.stdout, b"")
        self.assertIn(
            "input contains a NUL byte at offset 9",
            result.stderr.decode(),
        )

    def test_malformed_utf8_is_rejected_before_output(self):
        result = self.run_sql(b"SELECT '\xff';\n")

        self.assertEqual(result.returncode, 1)
        self.assertEqual(result.stdout, b"")
        self.assertIn("invalid UTF-8 at byte offset 8", result.stderr.decode())

    @unittest.skipUnless(os.name == "posix", "requires POSIX file descriptors")
    def test_closed_stdout_is_nonzero(self):
        def close_stdout():
            os.close(1)

        result = subprocess.run(
            [
                str(RUNNER),
                "--input",
                str(FIXTURE),
                "--branch",
                "closed",
                "--commit",
                "stdout",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            preexec_fn=close_stdout,
            timeout=TIMEOUT_SECONDS,
        )

        self.assertNotEqual(result.returncode, 0, result.stderr.decode())
        self.assertEqual(result.stdout, b"")


if __name__ == "__main__":
    unittest.main()
