#!/usr/bin/env python3

import argparse
import copy
import json
import os
from pathlib import Path
import unicodedata

if __package__:
    from .common import atomic_write_text, read_jsonl, statement_id
else:
    from common import atomic_write_text, read_jsonl, statement_id


VALID_RESULTS = frozenset(
    (
        "DEEP_SUPPORTED",
        "CLASSIFIED_ONLY",
        "PARTIAL",
        "ERROR",
        "TRAILING_INPUT",
        "TYPE_MISMATCH",
        "ORACLE_REJECTED",
    )
)
REQUIRED_INVENTORY_FIELDS = (
    "result",
    "normalized_sql",
    "oracle_node",
    "source_file",
    "offset",
    "line",
    "sql",
)


def _validate_row_object(row, row_index):
    if not isinstance(row, dict):
        raise ValueError(f"row {row_index}: expected a JSON object")

    for field in row:
        if not isinstance(field, str):
            raise ValueError(
                f"row {row_index}: field name {field!r} must be a string"
            )
        try:
            field.encode("utf-8")
        except UnicodeError as error:
            raise ValueError(
                f"row {row_index}: field name {field!r} is not UTF-8 "
                f"encodable: {error}"
            ) from error


def _validate_required_fields(row, row_index, required_fields):
    for field in required_fields:
        if field not in row:
            raise ValueError(
                f"row {row_index}: missing required field {field!r}"
            )


def _validate_string_field(row, row_index, field):
    if not isinstance(row[field], str):
        raise ValueError(
            f"row {row_index}: field {field!r} must be a string"
        )


def _validate_integer_field(row, row_index, field, minimum):
    value = row[field]
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(
            f"row {row_index}: field {field!r} must be an integer"
        )
    if value < minimum:
        raise ValueError(
            f"row {row_index}: field {field!r} must be at least {minimum}"
        )


def _strict_json_dumps(value):
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _validate_json_fields(row, row_index):
    for field, value in row.items():
        try:
            serialized = _strict_json_dumps(value)
        except (OverflowError, RecursionError, TypeError, ValueError) as error:
            raise ValueError(
                f"row {row_index}: field {field!r} is not strict JSON "
                f"serializable: {error}"
            ) from error

        try:
            serialized.encode("utf-8")
        except UnicodeError as error:
            raise ValueError(
                f"row {row_index}: field {field!r} is not UTF-8 encodable: "
                f"{error}"
            ) from error


def _validate_result(row, row_index):
    _validate_required_fields(row, row_index, ("result",))
    _validate_string_field(row, row_index, "result")
    if row["result"] not in VALID_RESULTS:
        raise ValueError(
            f"row {row_index}: field 'result' has unsupported value "
            f"{row['result']!r}"
        )


def _validate_optional_provenance(row, row_index):
    for field in ("branch", "commit"):
        if field in row:
            _validate_string_field(row, row_index, field)


def _validate_accepted_row(row, row_index):
    _validate_row_object(row, row_index)
    _validate_required_fields(row, row_index, REQUIRED_INVENTORY_FIELDS)
    _validate_result(row, row_index)
    if row["result"] == "ORACLE_REJECTED":
        raise ValueError(
            f"row {row_index}: field 'result' must describe an accepted row"
        )

    for field in ("normalized_sql", "oracle_node", "source_file", "sql"):
        _validate_string_field(row, row_index, field)
    _validate_integer_field(row, row_index, "offset", 0)
    _validate_integer_field(row, row_index, "line", 1)
    _validate_optional_provenance(row, row_index)
    _validate_json_fields(row, row_index)


def _validate_diagnostic_row(row, row_index):
    _validate_row_object(row, row_index)
    _validate_result(row, row_index)
    if row["result"] != "ORACLE_REJECTED":
        raise ValueError(
            f"row {row_index}: field 'result' must be 'ORACLE_REJECTED'"
        )

    for field in ("source_file", "sql", "oracle_error"):
        if field in row:
            _validate_string_field(row, row_index, field)
    if "offset" in row:
        _validate_integer_field(row, row_index, "offset", 0)
    if "line" in row:
        _validate_integer_field(row, row_index, "line", 1)
    _validate_optional_provenance(row, row_index)
    _validate_json_fields(row, row_index)


def _validate_and_classify_row(row, row_index):
    _validate_row_object(row, row_index)
    _validate_result(row, row_index)
    if row["result"] == "ORACLE_REJECTED":
        _validate_diagnostic_row(row, row_index)
        return "diagnostic"

    _validate_accepted_row(row, row_index)
    return "accepted"


def _serialized_row_key(row):
    try:
        return _strict_json_dumps(row)
    except (OverflowError, RecursionError, TypeError, ValueError) as error:
        raise ValueError(f"row is not strict JSON serializable: {error}") from error


def partition_rows(rows):
    accepted_rows = []
    diagnostics = []

    for row_index, row in enumerate(rows):
        classification = _validate_and_classify_row(row, row_index)
        row_copy = copy.deepcopy(row)
        if classification == "diagnostic":
            diagnostics.append(row_copy)
        else:
            accepted_rows.append(row_copy)

    return accepted_rows, diagnostics


def build_inventory(rows):
    groups = {}

    for row_index, row in enumerate(rows):
        classification = _validate_and_classify_row(row, row_index)
        if classification == "diagnostic":
            continue

        row_copy = copy.deepcopy(row)
        group_key = (row_copy["oracle_node"], row_copy["normalized_sql"])
        occurrence_key = (
            row_copy["source_file"],
            row_copy["offset"],
            row_copy["line"],
        )
        serialized_key = _serialized_row_key(row_copy)
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
    serialized_rows = [_strict_json_dumps(row) for row in rows]
    if not serialized_rows:
        return ""

    text = "\n".join(serialized_rows) + "\n"
    try:
        text.encode("utf-8")
    except UnicodeError as error:
        raise ValueError(f"output is not UTF-8 encodable: {error}") from error
    return text


def _validate_distinct_paths(input_path, inventory_path, diagnostics_path):
    named_paths = (
        ("--input", Path(input_path)),
        ("--inventory", Path(inventory_path)),
        ("--diagnostics", Path(diagnostics_path)),
    )
    resolved_paths = []
    for option, path in named_paths:
        try:
            resolved_paths.append((option, path, path.resolve(strict=False)))
        except (OSError, RuntimeError) as error:
            raise ValueError(f"{option} path cannot be resolved: {error}") from error

    for index, (left_option, left_path, left_resolved) in enumerate(
        resolved_paths
    ):
        for right_option, right_path, right_resolved in resolved_paths[index + 1 :]:
            aliases = left_resolved == right_resolved
            if not aliases:
                try:
                    aliases = os.path.samefile(left_path, right_path)
                except OSError:
                    aliases = False

            if not aliases:
                left_parent = left_resolved.parent
                right_parent = right_resolved.parent
                same_parent = left_parent == right_parent
                if not same_parent:
                    try:
                        same_parent = os.path.samefile(
                            left_parent,
                            right_parent,
                        )
                    except OSError:
                        same_parent = False

                left_name = unicodedata.normalize(
                    "NFC",
                    left_resolved.name,
                ).casefold()
                right_name = unicodedata.normalize(
                    "NFC",
                    right_resolved.name,
                ).casefold()
                aliases = same_parent and left_name == right_name

            if aliases:
                raise ValueError(
                    f"{left_option} and {right_option} must identify distinct files"
                )


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
        _validate_distinct_paths(
            arguments.input,
            arguments.inventory,
            arguments.diagnostics,
        )
        rows = list(read_jsonl(arguments.input))
        accepted_rows, diagnostics = partition_rows(rows)
        inventory = build_inventory(accepted_rows)
        inventory_text = _jsonl_text(inventory)
        diagnostics_text = _jsonl_text(diagnostics)
    except (OSError, TypeError, ValueError) as error:
        parser.exit(1, f"error: {error}\n")

    try:
        atomic_write_text(arguments.inventory, inventory_text)
        atomic_write_text(arguments.diagnostics, diagnostics_text)
    except (OSError, UnicodeError) as error:
        parser.exit(1, f"error: cannot publish outputs: {error}\n")


if __name__ == "__main__":
    main()
