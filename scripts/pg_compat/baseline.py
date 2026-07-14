#!/usr/bin/env python3

import copy
import json
import subprocess
from pathlib import Path

if __package__:
    from .common import atomic_write_text
else:
    from common import atomic_write_text


RESULTS = (
    "DEEP_SUPPORTED",
    "CLASSIFIED_ONLY",
    "PARTIAL",
    "ERROR",
    "TRAILING_INPUT",
    "TYPE_MISMATCH",
)
RESULT_SET = frozenset(RESULTS)
CLASSIFIED_IMPROVEMENTS = frozenset(
    ("PARTIAL", "ERROR", "TRAILING_INPUT", "TYPE_MISMATCH")
)
SUPPORT_LEVEL = {
    "DEEP_SUPPORTED": 2,
    "CLASSIFIED_ONLY": 1,
    "PARTIAL": 0,
    "ERROR": 0,
    "TRAILING_INPUT": 0,
    "TYPE_MISMATCH": 0,
}


def _validate_result(result, label):
    if not isinstance(result, str) or result not in RESULT_SET:
        raise ValueError(f"{label} result has unsupported value {result!r}")


def transition_allowed(previous, current):
    _validate_result(previous, "previous")
    _validate_result(current, "current")

    if previous == current:
        return True
    if current == "DEEP_SUPPORTED":
        return True
    return (
        previous in CLASSIFIED_IMPROVEMENTS
        and current == "CLASSIFIED_ONLY"
    )


def _records_by_id(rows, label, *, require_sql=False):
    records = {}
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            raise ValueError(f"{label} row {index}: expected a JSON object")

        record_id = row.get("id")
        if not isinstance(record_id, str) or not record_id:
            raise ValueError(
                f"{label} row {index}: field 'id' must be a non-empty string"
            )
        if record_id in records:
            raise ValueError(f"{label} has duplicate ID {record_id!r}")

        result = row.get("result")
        if not isinstance(result, str) or result not in RESULT_SET:
            raise ValueError(
                f"{label} row {index}: field 'result' has unsupported value "
                f"{result!r}"
            )

        if require_sql:
            sql = row.get("sql")
            if not isinstance(sql, str) or not sql:
                raise ValueError(
                    f"{label} row {index}: field 'sql' must be a non-empty string"
                )
            oracle_node = row.get("oracle_node")
            if not isinstance(oracle_node, str) or not oracle_node:
                raise ValueError(
                    f"{label} row {index}: field 'oracle_node' must be a "
                    "non-empty string"
                )

        records[record_id] = row
    return records


def _transition_record(previous, current):
    record = copy.deepcopy(current)
    record["previous_result"] = previous["result"]
    record["current_result"] = current["result"]
    return record


def evaluate_baseline(previous_rows, current_rows):
    previous = _records_by_id(previous_rows, "previous")
    current = _records_by_id(current_rows, "current")

    allowed = []
    regressions = []
    review_required = []
    for record_id in sorted(previous.keys() & current.keys()):
        previous_row = previous[record_id]
        current_row = current[record_id]
        transition = _transition_record(previous_row, current_row)
        if transition_allowed(previous_row["result"], current_row["result"]):
            allowed.append(transition)
        elif (
            SUPPORT_LEVEL[current_row["result"]]
            < SUPPORT_LEVEL[previous_row["result"]]
        ):
            regressions.append(transition)
        else:
            review_required.append(transition)

    return {
        "allowed": allowed,
        "regressions": regressions,
        "review_required": review_required,
        "new_cases": [
            copy.deepcopy(current[record_id])
            for record_id in sorted(current.keys() - previous.keys())
        ],
        "missing_ids": sorted(previous.keys() - current.keys()),
    }


def _case_identity(row):
    return (
        row["id"],
        row["sql"],
        row["oracle_node"],
        row["result"],
    )


def build_ci_cases(inventory_rows, release_delta_rows, witness_rows):
    sources = (
        ("inventory", inventory_rows),
        ("release delta", release_delta_rows),
        ("witness", witness_rows),
    )
    validated = {
        label: _records_by_id(rows, label, require_sql=True)
        for label, rows in sources
    }

    selected_ids = set()
    inventory = validated["inventory"]
    for record_id, row in inventory.items():
        if row["result"] != "DEEP_SUPPORTED":
            selected_ids.add(record_id)

    representatives = {}
    for record_id, row in inventory.items():
        if row["result"] not in ("DEEP_SUPPORTED", "CLASSIFIED_ONLY"):
            continue
        key = (row["oracle_node"], row["result"])
        representatives[key] = min(record_id, representatives.get(key, record_id))
    selected_ids.update(representatives.values())

    selected = {}

    def add_case(row):
        record_id = row["id"]
        existing = selected.get(record_id)
        if existing is not None:
            if _case_identity(existing) != _case_identity(row):
                raise ValueError(f"conflicting records for ID {record_id!r}")
            return

        case = copy.deepcopy(row)
        case["expected_result"] = case["result"]
        selected[record_id] = case

    for record_id in sorted(selected_ids):
        add_case(inventory[record_id])
    for label in ("release delta", "witness"):
        for record_id in sorted(validated[label]):
            add_case(validated[label][record_id])

    return [selected[record_id] for record_id in sorted(selected)]


def _read_json_array(path, label):
    path = Path(path)
    with path.open(encoding="utf-8") as input_file:
        value = json.load(input_file)
    if not isinstance(value, list):
        raise ValueError(f"{label} must contain a JSON array")
    return value


def parse_witnesses_with_runner(
    sql_path,
    runner_path,
    *,
    branch,
    commit,
    timeout_seconds=30,
):
    result = subprocess.run(
        [
            str(runner_path),
            "--input",
            str(sql_path),
            "--branch",
            branch,
            "--commit",
            commit,
        ],
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            "witness runner failed: "
            f"{result.stderr.strip() or result.stdout.strip()}"
        )

    rows = []
    for line_number, line in enumerate(result.stdout.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError as error:
            raise ValueError(
                f"witness runner stdout line {line_number}: invalid JSON: "
                f"{error.msg}"
            ) from error
    return rows


def _validate_metadata_row(row, index, target_major):
    if not isinstance(row, dict):
        raise ValueError(f"witness metadata row {index}: expected a JSON object")

    witness_id = row.get("id")
    if not isinstance(witness_id, str) or not witness_id:
        raise ValueError(
            f"witness metadata row {index}: field 'id' must be a "
            "non-empty string"
        )

    first_major = row.get("first_postgresql_major")
    if (
        isinstance(first_major, bool)
        or not isinstance(first_major, int)
        or first_major < 1
        or first_major > target_major
    ):
        raise ValueError(
            f"witness metadata row {index}: field 'first_postgresql_major' "
            f"must be an integer from 1 to {target_major}"
        )

    expected_node = row.get("expected_oracle_node")
    if not isinstance(expected_node, str) or not expected_node:
        raise ValueError(
            f"witness metadata row {index}: field 'expected_oracle_node' "
            "must be a non-empty string"
        )

    feature_ids = row.get("structural_feature_ids")
    if not isinstance(feature_ids, list):
        raise ValueError(
            f"witness metadata row {index}: field 'structural_feature_ids' "
            "must be a list"
        )
    for feature_index, feature_id in enumerate(feature_ids):
        if not isinstance(feature_id, str) or not feature_id:
            raise ValueError(
                f"witness metadata row {index}: structural_feature_ids"
                f"[{feature_index}] must be a non-empty string"
            )

    note = row.get("note")
    if note is not None and not isinstance(note, str):
        raise ValueError(
            f"witness metadata row {index}: field 'note' must be a string"
        )


def _validate_oracle_witness_row(row, index):
    if not isinstance(row, dict):
        raise ValueError(f"witness oracle row {index}: expected a JSON object")
    for field in ("oracle_node", "result", "sql"):
        value = row.get(field)
        if not isinstance(value, str) or not value:
            raise ValueError(
                f"witness oracle row {index}: field {field!r} must be a "
                "non-empty string"
            )
    if row["result"] == "ORACLE_REJECTED":
        raise ValueError(
            f"witness oracle row {index}: statement must be oracle accepted"
        )


def validate_witnesses(oracle_rows, metadata_rows, *, target_major):
    if isinstance(target_major, bool) or not isinstance(target_major, int):
        raise ValueError("target_major must be an integer")
    if target_major < 1:
        raise ValueError("target_major must be positive")

    if len(oracle_rows) != len(metadata_rows):
        raise ValueError(
            f"witness metadata count {len(metadata_rows)} does not match "
            f"{len(oracle_rows)} parsed SQL statement(s)"
        )

    witnesses = []
    unlinked_witnesses = []
    seen_ids = set()
    for index, (oracle_row, metadata_row) in enumerate(
        zip(oracle_rows, metadata_rows)
    ):
        _validate_oracle_witness_row(oracle_row, index)
        _validate_metadata_row(metadata_row, index, target_major)

        witness_id = metadata_row["id"]
        if witness_id in seen_ids:
            raise ValueError(f"duplicate witness ID {witness_id!r}")
        seen_ids.add(witness_id)

        expected_node = metadata_row["expected_oracle_node"]
        actual_node = oracle_row["oracle_node"]
        if actual_node != expected_node:
            raise ValueError(
                f"witness {witness_id!r}: oracle node differs from "
                f"expected {expected_node!r}: {actual_node!r}"
            )

        witness = copy.deepcopy(oracle_row)
        witness.update(copy.deepcopy(metadata_row))
        witness["oracle_node"] = actual_node
        witness["result"] = oracle_row["result"]
        witnesses.append(witness)
        if not witness["structural_feature_ids"]:
            unlinked_witnesses.append(witness_id)

    return {
        "witnesses": witnesses,
        "unlinked_witnesses": sorted(unlinked_witnesses),
    }


def load_reviewed_witnesses(
    sql_path,
    metadata_path,
    runner_path,
    *,
    target_major,
    branch,
    commit,
):
    oracle_rows = parse_witnesses_with_runner(
        sql_path,
        runner_path,
        branch=branch,
        commit=commit,
    )
    metadata_rows = _read_json_array(metadata_path, "witness metadata")
    return validate_witnesses(
        oracle_rows,
        metadata_rows,
        target_major=target_major,
    )


def write_ci_cases(path, cases):
    records = _records_by_id(cases, "CI case", require_sql=True)
    output = []
    for record_id in sorted(records):
        row = copy.deepcopy(records[record_id])
        expected_result = row.get("expected_result")
        if (
            not isinstance(expected_result, str)
            or expected_result not in RESULT_SET
        ):
            raise ValueError(
                f"CI case {record_id!r}: field 'expected_result' has "
                f"unsupported value {expected_result!r}"
            )
        if expected_result != row["result"]:
            raise ValueError(
                f"CI case {record_id!r}: expected_result does not match result"
            )
        output.append(
            json.dumps(
                row,
                allow_nan=False,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            )
        )

    text = "\n".join(output)
    if output:
        text += "\n"
    atomic_write_text(path, text)
