from pathlib import Path
import unittest

from scripts.pg_compat.common import read_jsonl, statement_id
from scripts.pg_compat.compare_versions import (
    compare_inventories,
    compare_structural_sources,
)


ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "tests" / "pg_compat" / "fixtures"


def fixture_text(name):
    return (FIXTURES / name).read_text(encoding="utf-8")


class InventoryDeltaTest(unittest.TestCase):
    def setUp(self):
        self.previous = list(read_jsonl(FIXTURES / "pg17-inventory.jsonl"))
        self.target = list(read_jsonl(FIXTURES / "pg18-inventory.jsonl"))

    def test_reports_added_removed_and_nearest_source_change(self):
        delta = compare_inventories(self.previous, self.target)

        shared_id = statement_id(
            "SELECT 1",
            "PG_QUERY__NODE__NODE_SELECT_STMT",
        )
        self.assertIn(shared_id, {row["id"] for row in self.previous})
        self.assertIn(shared_id, {row["id"] for row in self.target})
        self.assertEqual(
            [row["id"] for row in delta["added"]],
            ["6edce6d7aac4705e6c755458"],
        )
        self.assertEqual(
            [row["id"] for row in delta["removed"]],
            ["34d8acda66155ea517be12bb"],
        )
        self.assertEqual(
            delta["changed"],
            [
                {
                    "source_file": "create_table.sql",
                    "previous_id": "34d8acda66155ea517be12bb",
                    "previous_line": 10,
                    "previous_normalized_sql": (
                        "CREATE TABLE old_table ( id integer )"
                    ),
                    "target_id": "6edce6d7aac4705e6c755458",
                    "target_line": 11,
                    "target_normalized_sql": (
                        "CREATE TABLE new_table ( id integer )"
                    ),
                }
            ],
        )
        self.assertEqual(delta["new_oracle_nodes"], [])

    def test_reports_new_oracle_nodes_and_is_input_order_independent(self):
        merge = {
            "id": "f" * 24,
            "normalized_sql": "MERGE INTO target USING source ON TRUE",
            "oracle_node": "PG_QUERY__NODE__NODE_MERGE_STMT",
            "source_file": "merge.sql",
            "line": 1,
        }
        forward = compare_inventories(
            self.previous,
            [*self.target, merge],
        )
        reverse = compare_inventories(
            list(reversed(self.previous)),
            [merge, *reversed(self.target)],
        )

        self.assertEqual(forward, reverse)
        self.assertEqual(
            forward["new_oracle_nodes"],
            ["PG_QUERY__NODE__NODE_MERGE_STMT"],
        )


class StructuralDeltaTest(unittest.TestCase):
    def sources(self, protobuf):
        return {
            "grammar": "%%\nstatement: SELECT;\n%%\n",
            "keyword": "",
            "parse_node": "",
            "protobuf": protobuf,
        }

    def test_extracts_all_structural_change_kinds_with_stable_ids(self):
        previous = {
            "grammar": fixture_text("pg17-gram.y"),
            "keyword": fixture_text("pg17-kwlist.h"),
            "parse_node": fixture_text("pg17-parsenodes.h"),
            "protobuf": fixture_text("pg17.proto"),
        }
        target = {
            "grammar": fixture_text("pg18-gram.y"),
            "keyword": fixture_text("pg18-kwlist.h"),
            "parse_node": fixture_text("pg18-parsenodes.h"),
            "protobuf": fixture_text("pg18.proto"),
        }

        features = compare_structural_sources(previous, target)

        self.assertEqual(
            {feature["kind"] for feature in features},
            {"grammar", "keyword", "parse_node", "protobuf"},
        )
        self.assertEqual(len(features), 4)
        self.assertTrue(
            all(feature["result"] == "UNWITNESSED_FEATURE" for feature in features)
        )
        self.assertTrue(
            all(len(feature["id"]) == 24 for feature in features)
        )
        self.assertEqual(
            {feature["id"] for feature in features},
            {
                "1b919e114a4609a838e4a151",
                "32754506a22795e52392acee",
                "7a54e1a3d270ce2eabc05227",
                "ee196601d4be83634c03cc34",
            },
        )
        self.assertEqual(
            features,
            compare_structural_sources(previous, target),
        )

    def test_ignores_protobuf_tag_renumbering(self):
        previous = self.sources(
            "message SelectStmt { bool distinct = 1; }\n"
        )
        target = self.sources(
            "message SelectStmt { bool distinct = 20; }\n"
        )

        self.assertEqual(
            compare_structural_sources(previous, target),
            [],
        )

    def test_extracts_first_field_inside_new_oneof_block(self):
        previous = self.sources("message Node {}\n")
        target = self.sources(
            "message Node {\n"
            "  oneof node {\n"
            "    MergeStmt merge_stmt = 1;\n"
            "  }\n"
            "}\n"
        )

        features = compare_structural_sources(previous, target)

        self.assertEqual(len(features), 1)
        self.assertEqual(features[0]["kind"], "protobuf")
        self.assertEqual(features[0]["symbol"], "Node.merge_stmt")
        self.assertEqual(features[0]["target"], "MergeStmt merge_stmt")


if __name__ == "__main__":
    unittest.main()
