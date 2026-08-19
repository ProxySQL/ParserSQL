from pathlib import Path
import json
import tempfile
import unittest
from unittest import mock

from scripts.pg_compat.run_compat import (
    _apply_structural_dispositions,
    build_runner,
    is_skippable_runner_error,
    run_committed_ci_cases,
    runner_path,
)


class RunCompatBuildTest(unittest.TestCase):
    def test_runner_path_uses_role_major_versions(self):
        cache = Path("/tmp/cache")

        self.assertEqual(
            runner_path(cache, "previous"),
            cache / "bin" / "pg_compat-17",
        )
        self.assertEqual(
            runner_path(cache, "target"),
            cache / "bin" / "pg_compat-18",
        )

    @mock.patch("scripts.pg_compat.run_compat.subprocess.run")
    def test_build_runner_builds_libpg_query_and_cache_binary(self, run):
        run.return_value = mock.Mock(returncode=0, stdout="", stderr="")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cache = root / "cache"
            checkout = cache / "libpg_query" / "target"
            checkout.mkdir(parents=True)
            (root / "libsqlparser.a").write_text("", encoding="utf-8")
            (checkout / "pg_query.h").write_text("", encoding="utf-8")
            (checkout / "libpg_query.a").write_text("", encoding="utf-8")
            (checkout / "protobuf").mkdir()
            (checkout / "protobuf" / "pg_query.pb-c.h").write_text(
                "",
                encoding="utf-8",
            )
            (checkout / "vendor").mkdir()
            (checkout / "src").mkdir()

            output = build_runner(
                cache,
                "target",
                root,
                compiler="c++",
                dry_run=False,
            )

        self.assertEqual(output, cache / "bin" / "pg_compat-18")
        self.assertEqual(run.call_args_list[0].args[0][:3], ["make", "-C", str(checkout)])
        compile_command = run.call_args_list[1].args[0]
        self.assertEqual(compile_command[0], "c++")
        self.assertIn(str(root / "tools" / "pg_compat" / "pg_compat.cpp"), compile_command)
        self.assertIn(str(checkout / "libpg_query.a"), compile_command)
        self.assertIn(str(output), compile_command)


class RunCompatCiCasesTest(unittest.TestCase):
    def test_structural_dispositions_support_exact_and_kind_rules(self):
        features = [
            {"id": "one", "kind": "grammar", "symbol": "stmt"},
            {"id": "two", "kind": "protobuf", "symbol": "Node.stmt"},
        ]
        dispositions = [
            {
                "kind": "protobuf",
                "disposition": "internal_non_syntax",
                "reason": "libpg_query schema field.",
            },
            {
                "feature_id": "one",
                "disposition": "witness_required",
                "reason": "Grammar productions need explicit SQL.",
            },
        ]

        self.assertEqual(
            _apply_structural_dispositions(features, dispositions),
            [
                {
                    "id": "one",
                    "kind": "grammar",
                    "symbol": "stmt",
                    "disposition": "witness_required",
                    "reason": "Grammar productions need explicit SQL.",
                },
                {
                    "id": "two",
                    "kind": "protobuf",
                    "symbol": "Node.stmt",
                    "disposition": "internal_non_syntax",
                    "reason": "libpg_query schema field.",
                },
            ],
        )

    def test_only_non_mapping_infrastructure_errors_are_skippable(self):
        self.assertTrue(is_skippable_runner_error("invalid UTF-8 at byte offset 11"))
        self.assertTrue(is_skippable_runner_error("statement splitting failed"))
        self.assertFalse(
            is_skippable_runner_error(
                "unmapped PostgreSQL top-level node: PG_QUERY__NODE__NODE_FOO"
            )
        )

    def test_missing_committed_ci_cases_is_a_noop_before_refresh(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)

            self.assertEqual(
                run_committed_ci_cases(root, Path("/no/runner"), []),
                {"checked": 0, "skipped": True},
            )

    @mock.patch("scripts.pg_compat.run_compat.subprocess.run")
    def test_committed_ci_cases_compare_expected_result_and_node(self, run):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cases_path = root / "tests" / "pg_compat" / "ci_cases.jsonl"
            cases_path.parent.mkdir(parents=True)
            cases_path.write_text(
                json.dumps(
                    {
                        "id": "case-1",
                        "sql": "SELECT 1",
                        "expected_result": "DEEP_SUPPORTED",
                        "oracle_node": "PG_QUERY__NODE__NODE_SELECT_STMT",
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            with cases_path.open("a", encoding="utf-8") as file:
                file.write(
                    json.dumps(
                        {
                            "id": "case-2",
                            "sql": "CREATE TABLE t (id int)",
                            "expected_result": "CLASSIFIED_ONLY",
                            "oracle_node": "PG_QUERY__NODE__NODE_CREATE_STMT",
                        }
                    )
                    + "\n"
                )
            run.return_value = mock.Mock(
                returncode=0,
                stdout=json.dumps(
                    {
                        "result": "DEEP_SUPPORTED",
                        "oracle_node": "PG_QUERY__NODE__NODE_SELECT_STMT",
                    }
                )
                + "\n"
                + json.dumps(
                    {
                        "result": "CLASSIFIED_ONLY",
                        "oracle_node": "PG_QUERY__NODE__NODE_CREATE_STMT",
                    }
                )
                + "\n",
                stderr="",
            )

            self.assertEqual(
                run_committed_ci_cases(
                    root,
                    Path("/runner"),
                    ["--branch", "18-latest", "--commit", "deadbeef"],
                ),
                {"checked": 2, "skipped": False},
            )

        self.assertEqual(run.call_count, 1)

    @mock.patch("scripts.pg_compat.run_compat.subprocess.run")
    def test_committed_ci_cases_fail_when_batch_row_count_differs(self, run):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cases_path = root / "tests" / "pg_compat" / "ci_cases.jsonl"
            cases_path.parent.mkdir(parents=True)
            cases_path.write_text(
                json.dumps(
                    {
                        "id": "case-1",
                        "sql": "SELECT 1",
                        "expected_result": "DEEP_SUPPORTED",
                        "oracle_node": "PG_QUERY__NODE__NODE_SELECT_STMT",
                    }
                )
                + "\n"
                + json.dumps(
                    {
                        "id": "case-2",
                        "sql": "SELECT 2",
                        "expected_result": "DEEP_SUPPORTED",
                        "oracle_node": "PG_QUERY__NODE__NODE_SELECT_STMT",
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            run.return_value = mock.Mock(
                returncode=0,
                stdout=json.dumps(
                    {
                        "result": "DEEP_SUPPORTED",
                        "oracle_node": "PG_QUERY__NODE__NODE_SELECT_STMT",
                    }
                )
                + "\n",
                stderr="",
            )

            with self.assertRaisesRegex(AssertionError, "expected 2 row"):
                run_committed_ci_cases(
                    root,
                    Path("/runner"),
                    ["--branch", "18-latest", "--commit", "deadbeef"],
                )

    @mock.patch("scripts.pg_compat.run_compat.subprocess.run")
    def test_committed_ci_cases_terminate_sql_after_line_comments(self, run):
        captured_sql = []

        def capture(command, **_kwargs):
            captured_sql.append(Path(command[2]).read_text(encoding="utf-8"))
            return mock.Mock(
                returncode=0,
                stdout=json.dumps(
                    {
                        "result": "DEEP_SUPPORTED",
                        "oracle_node": "PG_QUERY__NODE__NODE_SELECT_STMT",
                    }
                )
                + "\n",
                stderr="",
            )

        run.side_effect = capture
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cases_path = root / "tests" / "pg_compat" / "ci_cases.jsonl"
            cases_path.parent.mkdir(parents=True)
            cases_path.write_text(
                json.dumps(
                    {
                        "id": "case-1",
                        "sql": "SELECT 1 -- trailing comment",
                        "expected_result": "DEEP_SUPPORTED",
                        "oracle_node": "PG_QUERY__NODE__NODE_SELECT_STMT",
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            run_committed_ci_cases(
                root,
                Path("/runner"),
                ["--branch", "18-latest", "--commit", "deadbeef"],
            )

        self.assertIn("SELECT 1 -- trailing comment\n;\n", captured_sql[0])


if __name__ == "__main__":
    unittest.main()
