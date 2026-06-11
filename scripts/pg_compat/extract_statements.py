#!/usr/bin/env python3

import argparse
import json

if __package__:
    from .common import atomic_write_text, read_jsonl, statement_id
else:
    from common import atomic_write_text, read_jsonl, statement_id


REQUIRED_INVENTORY_FIELDS = (
    "result",
    "normalized_sql",
    "oracle_node",
    "source_file",
    "offset",
    "line",
    "sql",
)


def _validate_row(row, row_index, required_fields):
    if not isinstance(row, dict):
        raise ValueError(f"row {row_index}: expected a JSON object")

    for field in required_fields:
        if field not in row:
            raise ValueError(
                f"row {row_index}: missing required field {field!r}"
            )


def _serialized_row_key(row, row_index):
    try:
        return json.dumps(
            row,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    except (TypeError, ValueError) as error:
        raise ValueError(
            f"row {row_index}: not JSON serializable: {error}"
        ) from error


def partition_rows(rows):
    accepted_rows = []
    diagnostics = []

    for row_index, row in enumerate(rows):
        _validate_row(row, row_index, ("result",))
        if row["result"] == "ORACLE_REJECTED":
            diagnostics.append(row)
        else:
            accepted_rows.append(row)

    return accepted_rows, diagnostics


def build_inventory(rows):
    groups = {}

    for row_index, row in enumerate(rows):
        _validate_row(row, row_index, ("result",))
        if row["result"] == "ORACLE_REJECTED":
            continue

        _validate_row(row, row_index, REQUIRED_INVENTORY_FIELDS)
        group_key = (row["oracle_node"], row["normalized_sql"])
        occurrence_key = (
            row["source_file"],
            row["offset"],
            row["line"],
        )
        row_copy = dict(row)
        serialized_key = _serialized_row_key(row_copy, row_index)
        occurrences = groups.setdefault(group_key, {})
        existing = occurrences.get(occurrence_key)
        if existing is None or serialized_key < existing[0]:
            occurrences[occurrence_key] = (serialized_key, row_copy)

    inventory = []
    ids = {}
    for group_key, rows_by_occurrence in groups.items():
        oracle_node, normalized_sql = group_key
        record_id = statement_id(normalized_sql, oracle_node)
        existing_key = ids.get(record_id)
        if existing_key is not None and existing_key != group_key:
            raise ValueError(
                "statement ID collision: "
                f"{record_id} maps to both {existing_key!r} and {group_key!r}"
            )
        ids[record_id] = group_key

        sorted_occurrence_keys = sorted(rows_by_occurrence)
        canonical_row = rows_by_occurrence[sorted_occurrence_keys[0]][1]
        record = dict(canonical_row)
        record["id"] = record_id
        record["occurrences"] = [
            {
                "source_file": source_file,
                "offset": offset,
                "line": line,
            }
            for source_file, offset, line in sorted_occurrence_keys
        ]
        inventory.append(record)

    return sorted(inventory, key=lambda record: record["id"])


def _jsonl_text(rows):
    serialized_rows = [
        json.dumps(
            row,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        for row in rows
    ]
    if not serialized_rows:
        return ""
    return "\n".join(serialized_rows) + "\n"


def _argument_parser():
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument("--input", required=True)
    parser.add_argument("--inventory", required=True)
    parser.add_argument("--diagnostics", required=True)
    return parser


def main(argv=None):
    parser = _argument_parser()
    arguments = parser.parse_args(argv)

    try:
        rows = list(read_jsonl(arguments.input))
        accepted_rows, diagnostics = partition_rows(rows)
        inventory = build_inventory(accepted_rows)
        inventory_text = _jsonl_text(inventory)
        diagnostics_text = _jsonl_text(diagnostics)
    except (OSError, TypeError, ValueError) as error:
        parser.exit(1, f"error: {error}\n")

    atomic_write_text(arguments.inventory, inventory_text)
    atomic_write_text(arguments.diagnostics, diagnostics_text)


if __name__ == "__main__":
    main()
