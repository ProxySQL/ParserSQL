from pathlib import Path
import json
import tempfile
import unittest
from unittest import mock

from scripts.pg_compat.run_compat import (
    build_runner,
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

            self.assertEqual(
                run_committed_ci_cases(
                    root,
                    Path("/runner"),
                    ["--branch", "18-latest", "--commit", "deadbeef"],
                ),
                {"checked": 1, "skipped": False},
            )

        self.assertEqual(run.call_count, 1)


if __name__ == "__main__":
    unittest.main()
