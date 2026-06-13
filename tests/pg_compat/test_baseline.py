import copy
import itertools
import json
from pathlib import Path
import tempfile
import unittest

from scripts.pg_compat.baseline import (
    RESULTS,
    build_ci_cases,
    evaluate_baseline,
    transition_allowed,
    write_ci_cases,
)


def result_row(
    record_id,
    result,
    *,
    oracle_node="SelectStmt",
    sql=None,
):
    return {
        "id": record_id,
        "oracle_node": oracle_node,
        "result": result,
        "sql": sql or f"SELECT '{record_id}'",
    }


class TransitionPolicyTest(unittest.TestCase):
    def test_complete_transition_matrix(self):
        classified_improvements = {
            "PARTIAL",
            "ERROR",
            "TRAILING_INPUT",
            "TYPE_MISMATCH",
        }

        for previous, current in itertools.product(RESULTS, repeat=2):
            expected = (
                previous == current
                or current == "DEEP_SUPPORTED"
                or (
                    previous in classified_improvements
                    and current == "CLASSIFIED_ONLY"
                )
            )
            with self.subTest(previous=previous, current=current):
                self.assertEqual(
                    transition_allowed(previous, current),
                    expected,
                )

    def test_rejects_unknown_results(self):
        with self.assertRaisesRegex(ValueError, "previous.*UNKNOWN"):
            transition_allowed("UNKNOWN", "DEEP_SUPPORTED")
        with self.assertRaisesRegex(ValueError, "current.*UNKNOWN"):
            transition_allowed("ERROR", "UNKNOWN")
        with self.assertRaisesRegex(ValueError, "previous.*unsupported"):
            transition_allowed([], "ERROR")


class BaselineEvaluationTest(unittest.TestCase):
    def test_reports_allowed_regressed_review_new_and_missing_cases(self):
        previous = [
            result_row("unchanged", "ERROR"),
            result_row("improved", "PARTIAL"),
            result_row("regressed", "DEEP_SUPPORTED"),
            result_row("review", "ERROR"),
            result_row("missing", "CLASSIFIED_ONLY"),
        ]
        current = [
            result_row("unchanged", "ERROR"),
            result_row("improved", "CLASSIFIED_ONLY"),
            result_row("regressed", "PARTIAL"),
            result_row("review", "TYPE_MISMATCH"),
            result_row("new", "DEEP_SUPPORTED"),
        ]

        evaluation = evaluate_baseline(previous, current)

        self.assertEqual(
            [row["id"] for row in evaluation["allowed"]],
            ["improved", "unchanged"],
        )
        self.assertEqual(
            evaluation["regressions"],
            [
                {
                    **current[2],
                    "previous_result": "DEEP_SUPPORTED",
                    "current_result": "PARTIAL",
                }
            ],
        )
        self.assertEqual(
            evaluation["review_required"],
            [
                {
                    **current[3],
                    "previous_result": "ERROR",
                    "current_result": "TYPE_MISMATCH",
                }
            ],
        )
        self.assertEqual(evaluation["new_cases"], [current[4]])
        self.assertEqual(evaluation["missing_ids"], ["missing"])

    def test_is_input_order_independent_and_does_not_mutate_inputs(self):
        previous = [
            result_row("b", "ERROR"),
            result_row("a", "DEEP_SUPPORTED"),
        ]
        current = [
            result_row("a", "DEEP_SUPPORTED"),
            result_row("b", "CLASSIFIED_ONLY"),
        ]
        original_previous = copy.deepcopy(previous)
        original_current = copy.deepcopy(current)

        forward = evaluate_baseline(previous, current)
        reverse = evaluate_baseline(
            list(reversed(previous)),
            list(reversed(current)),
        )

        self.assertEqual(forward, reverse)
        self.assertEqual(previous, original_previous)
        self.assertEqual(current, original_current)

    def test_rejects_duplicate_ids_and_invalid_rows(self):
        duplicate = result_row("same", "ERROR")
        with self.assertRaisesRegex(ValueError, "previous.*duplicate ID"):
            evaluate_baseline([duplicate, duplicate], [])

        with self.assertRaisesRegex(ValueError, "current row 0.*result"):
            evaluate_baseline([], [{"id": "bad", "result": "UNKNOWN"}])
        with self.assertRaisesRegex(ValueError, "current row 0.*result"):
            evaluate_baseline([], [{"id": "bad", "result": []}])


class CiCaseSelectionTest(unittest.TestCase):
    def test_includes_required_cases_and_node_representatives(self):
        inventory = [
            result_row("select-deep-b", "DEEP_SUPPORTED"),
            result_row("select-deep-a", "DEEP_SUPPORTED"),
            result_row("select-classified", "CLASSIFIED_ONLY"),
            result_row("select-error", "ERROR"),
            result_row(
                "insert-deep",
                "DEEP_SUPPORTED",
                oracle_node="InsertStmt",
            ),
            result_row(
                "insert-partial",
                "PARTIAL",
                oracle_node="InsertStmt",
            ),
        ]
        release_delta = [
            result_row(
                "release-deep",
                "DEEP_SUPPORTED",
                oracle_node="MergeStmt",
                sql="MERGE INTO target USING source ON true",
            )
        ]
        witnesses = [
            result_row(
                "witness-deep",
                "DEEP_SUPPORTED",
                oracle_node="SelectStmt",
                sql="SELECT json_object('a' VALUE 1)",
            )
        ]

        cases = build_ci_cases(inventory, release_delta, witnesses)

        self.assertEqual(
            [row["id"] for row in cases],
            [
                "insert-deep",
                "insert-partial",
                "release-deep",
                "select-classified",
                "select-deep-a",
                "select-error",
                "witness-deep",
            ],
        )
        self.assertNotIn("select-deep-b", {row["id"] for row in cases})
        self.assertTrue(
            all(row["expected_result"] == row["result"] for row in cases)
        )
        self.assertEqual(
            next(row for row in cases if row["id"] == "release-deep")["sql"],
            "MERGE INTO target USING source ON true",
        )

    def test_deduplicates_identical_cases_from_multiple_sources(self):
        shared = result_row("shared", "ERROR", sql="SELECT invalid")

        cases = build_ci_cases([shared], [copy.deepcopy(shared)], [shared])

        self.assertEqual(len(cases), 1)
        self.assertEqual(cases[0]["expected_result"], "ERROR")

    def test_rejects_conflicting_duplicate_ids_and_missing_sql(self):
        with self.assertRaisesRegex(ValueError, "conflicting.*same"):
            build_ci_cases(
                [result_row("same", "ERROR")],
                [result_row("same", "PARTIAL")],
                [],
            )

        invalid = result_row("missing-sql", "ERROR")
        del invalid["sql"]
        with self.assertRaisesRegex(ValueError, "inventory row 0.*sql"):
            build_ci_cases([invalid], [], [])

    def test_writes_sorted_strict_json_lines_with_complete_sql(self):
        cases = [
            {
                **result_row(
                    "b",
                    "ERROR",
                    sql="SELECT 'first line'\nFROM broken",
                ),
                "expected_result": "ERROR",
            },
            {
                **result_row("a", "PARTIAL"),
                "expected_result": "PARTIAL",
            },
        ]

        with tempfile.TemporaryDirectory() as temporary_directory:
            output = Path(temporary_directory) / "ci_cases.jsonl"
            write_ci_cases(output, cases)
            lines = output.read_text(encoding="utf-8").splitlines()

        rows = [json.loads(line) for line in lines]
        self.assertEqual([row["id"] for row in rows], ["a", "b"])
        self.assertEqual(rows[1]["sql"], "SELECT 'first line'\nFROM broken")
        self.assertEqual(rows[1]["expected_result"], "ERROR")


if __name__ == "__main__":
    unittest.main()
