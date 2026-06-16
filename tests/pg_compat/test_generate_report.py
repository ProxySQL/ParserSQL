import unittest

from scripts.pg_compat.generate_report import generate_report


def row(
    record_id,
    result,
    *,
    oracle_node="PG_QUERY__NODE__NODE_SELECT_STMT",
    sql=None,
):
    return {
        "id": record_id,
        "oracle_node": oracle_node,
        "result": result,
        "sql": sql or f"SELECT '{record_id}'",
    }


class GenerateReportTest(unittest.TestCase):
    def context(self):
        return {
            "generated_at": "2026-06-16T12:00:00Z",
            "parsersql_commit": "abcdef123456",
            "pins": {
                "versions": {
                    "previous": {
                        "branch": "17-latest",
                        "commit": "1" * 40,
                        "pg_version": "17.7",
                    },
                    "target": {
                        "branch": "18-latest",
                        "commit": "2" * 40,
                        "pg_version": "18.4",
                    },
                }
            },
            "commands": [
                "make pg-compat-refresh PG_COMPAT_CACHE=/tmp/cache",
                "make pg-compat PG_COMPAT_CACHE=/tmp/cache",
            ],
        }

    def sample_inputs(self):
        long_sql = "SELECT '" + ("x" * 260) + "'"
        results = [
            row(
                "z-error",
                "ERROR",
                oracle_node="PG_QUERY__NODE__NODE_UPDATE_STMT",
                sql=long_sql,
            ),
            row("a-classified", "CLASSIFIED_ONLY"),
            row("m-deep", "DEEP_SUPPORTED"),
            row("b-partial", "PARTIAL"),
            row("t-trailing", "TRAILING_INPUT"),
            row("x-type", "TYPE_MISMATCH"),
        ]
        release_delta = [
            row(
                "release-added",
                "TYPE_MISMATCH",
                sql="MERGE INTO t USING s ON true",
            )
        ]
        structural_features = [
            {
                "id": "feature-2",
                "kind": "protobuf",
                "symbol": "Node.merge_stmt",
                "target": "MergeStmt merge_stmt",
                "result": "UNWITNESSED_FEATURE",
                "disposition": "internal_non_syntax",
                "reason": "Protobuf-only field; tracked through grammar witnesses.",
            },
            {
                "id": "feature-1",
                "kind": "grammar",
                "symbol": "merge_statement",
                "target": "MERGE INTO",
                "result": "UNWITNESSED_FEATURE",
            },
        ]
        baseline = {
            "allowed": [
                {
                    **row("newly-supported", "DEEP_SUPPORTED"),
                    "previous_result": "ERROR",
                    "current_result": "DEEP_SUPPORTED",
                }
            ],
            "regressions": [
                {
                    **row("regressed", "PARTIAL"),
                    "previous_result": "DEEP_SUPPORTED",
                    "current_result": "PARTIAL",
                }
            ],
            "review_required": [],
            "new_cases": [],
            "missing_ids": [],
        }
        witness_result = {
            "witnesses": [
                {
                    **row(
                        "pg-merge-statement",
                        "TYPE_MISMATCH",
                        oracle_node="PG_QUERY__NODE__NODE_MERGE_STMT",
                        sql="MERGE INTO target USING source ON true",
                    ),
                    "structural_feature_ids": ["feature-1"],
                },
                {
                    **row("pg-json-object-constructor", "DEEP_SUPPORTED"),
                    "structural_feature_ids": [],
                },
            ],
            "unlinked_witnesses": ["pg-json-object-constructor"],
        }
        return results, release_delta, structural_features, baseline, witness_result

    def test_contains_required_sections_and_metadata(self):
        report = generate_report(
            self.context(),
            *self.sample_inputs(),
        )

        required = (
            "# PostgreSQL 18 Compatibility",
            "DEEP_SUPPORTED",
            "CLASSIFIED_ONLY",
            "PG17 to PG18 Release Delta",
            "Unwitnessed Structural Features",
            "Reviewed Structural Dispositions",
            "internal_non_syntax",
            "Reproduction",
            "Generated: 2026-06-16T12:00:00Z",
            "ParserSQL commit: `abcdef123456`",
            "`17-latest`",
            "`18-latest`",
            "PostgreSQL 17.7",
            "PostgreSQL 18.4",
            "make pg-compat-refresh PG_COMPAT_CACHE=/tmp/cache",
        )
        for text in required:
            with self.subTest(text=text):
                self.assertIn(text, report)

    def test_orders_rows_deterministically_and_truncates_sql(self):
        report = generate_report(
            self.context(),
            *self.sample_inputs(),
        )

        self.assertLess(report.index("a-classified"), report.index("z-error"))
        self.assertLess(report.index("z-error"), report.index("b-partial"))
        self.assertLess(report.index("feature-1"), report.index("feature-2"))
        self.assertLess(
            report.index("newly-supported"),
            report.index("regressed"),
        )
        self.assertIn("...", report)
        self.assertNotIn("x" * 220, report)


if __name__ == "__main__":
    unittest.main()
