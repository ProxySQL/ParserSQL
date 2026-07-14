#!/usr/bin/env python3

import copy
import hashlib
import re


STRUCTURAL_KINDS = ("grammar", "keyword", "parse_node", "protobuf")
IDENTIFIER_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def _inventory_by_id(rows, label):
    records = {}
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            raise ValueError(f"{label} row {index}: expected a JSON object")
        record_id = row.get("id")
        if not isinstance(record_id, str) or not record_id:
            raise ValueError(f"{label} row {index}: missing string field 'id'")
        if record_id in records:
            raise ValueError(f"{label} inventory has duplicate ID {record_id!r}")
        records[record_id] = row
    return records


def _oracle_nodes(records, label):
    nodes = set()
    for record_id, row in records.items():
        node = row.get("oracle_node")
        if not isinstance(node, str) or not node:
            raise ValueError(
                f"{label} inventory row {record_id!r}: "
                "missing string field 'oracle_node'"
            )
        nodes.add(node)
    return nodes


def _locations(row):
    raw_locations = row.get("occurrences")
    if raw_locations is None:
        raw_locations = [row]
    if not isinstance(raw_locations, list):
        raise ValueError(
            f"inventory row {row.get('id')!r}: 'occurrences' must be a list"
        )

    locations = set()
    for occurrence in raw_locations:
        if not isinstance(occurrence, dict):
            raise ValueError(
                f"inventory row {row.get('id')!r}: "
                "occurrence must be a JSON object"
            )
        source_file = occurrence.get("source_file")
        line = occurrence.get("line")
        if not isinstance(source_file, str) or not source_file:
            raise ValueError(
                f"inventory row {row.get('id')!r}: "
                "occurrence needs string field 'source_file'"
            )
        if isinstance(line, bool) or not isinstance(line, int) or line < 1:
            raise ValueError(
                f"inventory row {row.get('id')!r}: "
                "occurrence needs positive integer field 'line'"
            )
        locations.add((source_file, line))
    return sorted(locations)


def _normalized_sql(row):
    normalized_sql = row.get("normalized_sql")
    if not isinstance(normalized_sql, str):
        raise ValueError(
            f"inventory row {row.get('id')!r}: "
            "missing string field 'normalized_sql'"
        )
    return normalized_sql


def _changed_statements(removed, added):
    candidates = []
    for previous_id, previous in removed.items():
        previous_sql = _normalized_sql(previous)
        for target_id, target in added.items():
            target_sql = _normalized_sql(target)
            for previous_file, previous_line in _locations(previous):
                for target_file, target_line in _locations(target):
                    if previous_file != target_file:
                        continue
                    candidates.append(
                        (
                            abs(previous_line - target_line),
                            previous_file,
                            previous_line,
                            target_line,
                            previous_id,
                            target_id,
                            previous_sql,
                            target_sql,
                        )
                    )

    matched_previous = set()
    matched_target = set()
    changed = []
    for candidate in sorted(candidates):
        (
            _distance,
            source_file,
            previous_line,
            target_line,
            previous_id,
            target_id,
            previous_sql,
            target_sql,
        ) = candidate
        if previous_id in matched_previous or target_id in matched_target:
            continue
        matched_previous.add(previous_id)
        matched_target.add(target_id)
        changed.append(
            {
                "source_file": source_file,
                "previous_id": previous_id,
                "previous_line": previous_line,
                "previous_normalized_sql": previous_sql,
                "target_id": target_id,
                "target_line": target_line,
                "target_normalized_sql": target_sql,
            }
        )

    return sorted(
        changed,
        key=lambda row: (
            row["source_file"],
            row["previous_line"],
            row["target_line"],
            row["previous_id"],
            row["target_id"],
        ),
    )


def compare_inventories(previous_rows, target_rows):
    previous = _inventory_by_id(previous_rows, "previous")
    target = _inventory_by_id(target_rows, "target")

    added_ids = sorted(target.keys() - previous.keys())
    removed_ids = sorted(previous.keys() - target.keys())
    added = {record_id: target[record_id] for record_id in added_ids}
    removed = {record_id: previous[record_id] for record_id in removed_ids}

    return {
        "added": [copy.deepcopy(added[record_id]) for record_id in added_ids],
        "removed": [
            copy.deepcopy(removed[record_id]) for record_id in removed_ids
        ],
        "changed": _changed_statements(removed, added),
        "new_oracle_nodes": sorted(
            _oracle_nodes(target, "target")
            - _oracle_nodes(previous, "previous")
        ),
    }


def _strip_comments(text):
    output = []
    index = 0
    quote = None
    while index < len(text):
        char = text[index]
        next_char = text[index + 1] if index + 1 < len(text) else ""
        if quote is not None:
            output.append(char)
            if char == "\\" and index + 1 < len(text):
                index += 1
                output.append(text[index])
            elif char == quote:
                quote = None
            index += 1
            continue
        if char in ("'", '"'):
            quote = char
            output.append(char)
            index += 1
            continue
        if char == "/" and next_char == "*":
            index += 2
            while index + 1 < len(text) and text[index : index + 2] != "*/":
                if text[index] == "\n":
                    output.append("\n")
                index += 1
            index = min(index + 2, len(text))
            continue
        if char == "/" and next_char == "/":
            index += 2
            while index < len(text) and text[index] != "\n":
                index += 1
            continue
        output.append(char)
        index += 1
    return "".join(output)


def _strip_braced_blocks(text):
    output = []
    index = 0
    depth = 0
    quote = None
    while index < len(text):
        char = text[index]
        if quote is not None:
            if depth == 0:
                output.append(char)
            if char == "\\" and index + 1 < len(text):
                index += 1
                if depth == 0:
                    output.append(text[index])
            elif char == quote:
                quote = None
            index += 1
            continue
        if char in ("'", '"'):
            quote = char
            if depth == 0:
                output.append(char)
            index += 1
            continue
        if char == "{":
            depth += 1
            index += 1
            continue
        if char == "}" and depth:
            depth -= 1
            index += 1
            continue
        if depth == 0:
            output.append(char)
        elif char == "\n":
            output.append("\n")
        index += 1
    if depth:
        raise ValueError("unbalanced braced block in grammar")
    return "".join(output)


def _normalize_declaration(text):
    return " ".join(text.split())


def _split_quoted(text, delimiter):
    parts = []
    start = 0
    quote = None
    escaped = False
    for index, char in enumerate(text):
        if quote is not None:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == quote:
                quote = None
            continue
        if char in ("'", '"'):
            quote = char
        elif char == delimiter:
            parts.append(text[start:index])
            start = index + 1
    parts.append(text[start:])
    return parts


def _extract_grammar(text):
    sections = text.split("%%")
    grammar = sections[1] if len(sections) >= 3 else text
    grammar = _strip_braced_blocks(_strip_comments(grammar))
    declarations = {}
    production_pattern = re.compile(
        r"(?ms)^[ \t]*([A-Za-z_][A-Za-z0-9_]*)[ \t]*:"
        r"(.*?)(?=^[ \t]*;[ \t]*(?:\n|$))"
    )
    for match in production_pattern.finditer(grammar):
        production = match.group(1)
        for alternative in _split_quoted(match.group(2), "|"):
            normalized = _normalize_declaration(alternative)
            if normalized:
                declarations[f"{production}:{normalized}"] = normalized
    return declarations


def _extract_keywords(text):
    declarations = {}
    pattern = re.compile(
        r'PG_KEYWORD\(\s*"([^"]+)"\s*,\s*'
        r"([A-Za-z_][A-Za-z0-9_]*)\s*,\s*"
        r"([A-Za-z_][A-Za-z0-9_]*)\s*,\s*"
        r"([A-Za-z_][A-Za-z0-9_]*)\s*\)"
    )
    for word, token, category, bare_label in pattern.findall(
        _strip_comments(text)
    ):
        declarations[word] = (
            f'{word} token={token} category={category} label={bare_label}'
        )
    return declarations


def _declaration_name(declaration):
    before_assignment = declaration.split("=", 1)[0]
    identifiers = IDENTIFIER_PATTERN.findall(before_assignment)
    return identifiers[-1] if identifiers else None


def _extract_parse_nodes(text):
    declarations = {}
    cleaned = _strip_comments(text)
    pattern = re.compile(
        r"(?ms)typedef\s+struct\s+([A-Za-z_][A-Za-z0-9_]*)"
        r"\s*\{(.*?)\}\s*([A-Za-z_][A-Za-z0-9_]*)\s*;"
    )
    for tag_name, body, typedef_name in pattern.findall(cleaned):
        struct_name = typedef_name or tag_name
        declarations[f"{struct_name}.__type__"] = f"struct {struct_name}"
        for raw_field in _split_quoted(body, ";"):
            field = _normalize_declaration(raw_field)
            if not field:
                continue
            field_name = _declaration_name(field)
            symbol = (
                f"{struct_name}.{field_name}"
                if field_name is not None
                else f"{struct_name}:{field}"
            )
            declarations[symbol] = field
    return declarations


def _matching_brace(text, opening):
    depth = 0
    quote = None
    escaped = False
    for index in range(opening, len(text)):
        char = text[index]
        if quote is not None:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == quote:
                quote = None
            continue
        if char in ("'", '"'):
            quote = char
        elif char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return index
    raise ValueError("unbalanced protobuf declaration")


def _protobuf_field(declaration):
    declaration = _normalize_declaration(declaration)
    if not declaration:
        return None
    if re.match(r"^(?:option|reserved|extensions)\b", declaration):
        return None

    before_assignment = _normalize_declaration(
        declaration.split("=", 1)[0]
    )
    field_name = _declaration_name(before_assignment)
    if field_name is None:
        return None
    return field_name, before_assignment


def _extract_protobuf_body(body, block_name, declarations):
    nested_pattern = re.compile(
        r"\b(oneof|message|enum)\s+([A-Za-z_][A-Za-z0-9_]*)\s*\{"
    )
    position = 0
    while True:
        match = nested_pattern.search(body, position)
        segment_end = match.start() if match is not None else len(body)
        for raw_declaration in _split_quoted(
            body[position:segment_end],
            ";",
        ):
            field = _protobuf_field(raw_declaration)
            if field is None:
                continue
            field_name, normalized = field
            declarations[f"{block_name}.{field_name}"] = normalized

        if match is None:
            return

        nested_kind, nested_name = match.groups()
        opening = match.end() - 1
        closing = _matching_brace(body, opening)
        nested_body = body[opening + 1 : closing]
        if nested_kind == "oneof":
            _extract_protobuf_body(
                nested_body,
                block_name,
                declarations,
            )
        else:
            qualified_name = f"{block_name}.{nested_name}"
            declarations[f"{qualified_name}.__type__"] = (
                f"{nested_kind} {qualified_name}"
            )
            _extract_protobuf_body(
                nested_body,
                qualified_name,
                declarations,
            )
        position = closing + 1


def _extract_protobuf(text):
    declarations = {}
    cleaned = _strip_comments(text)
    block_pattern = re.compile(
        r"\b(message|enum)\s+([A-Za-z_][A-Za-z0-9_]*)\s*\{"
    )
    position = 0
    while True:
        match = block_pattern.search(cleaned, position)
        if match is None:
            break
        kind, block_name = match.groups()
        opening = match.end() - 1
        closing = _matching_brace(cleaned, opening)
        body = cleaned[opening + 1 : closing]
        declarations[f"{block_name}.__type__"] = f"{kind} {block_name}"
        _extract_protobuf_body(body, block_name, declarations)
        position = closing + 1
    return declarations


EXTRACTORS = {
    "grammar": _extract_grammar,
    "keyword": _extract_keywords,
    "parse_node": _extract_parse_nodes,
    "protobuf": _extract_protobuf,
}


def _feature_id(kind, symbol, normalized_change):
    payload = f"{kind}\0{symbol}\0{normalized_change}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:24]


def compare_structural_sources(previous_sources, target_sources):
    for label, sources in (
        ("previous", previous_sources),
        ("target", target_sources),
    ):
        if not isinstance(sources, dict):
            raise ValueError(f"{label} structural sources must be a mapping")
        missing = [kind for kind in STRUCTURAL_KINDS if kind not in sources]
        if missing:
            raise ValueError(
                f"{label} structural sources missing: {', '.join(missing)}"
            )

    features = []
    seen_ids = {}
    for kind in STRUCTURAL_KINDS:
        previous = EXTRACTORS[kind](previous_sources[kind])
        target = EXTRACTORS[kind](target_sources[kind])
        for symbol in sorted(target):
            target_value = target[symbol]
            previous_value = previous.get(symbol)
            if previous_value == target_value:
                continue
            change_type = "added" if previous_value is None else "changed"
            normalized_change = f"{change_type}\0{target_value}"
            if previous_value is not None:
                normalized_change += f"\0from\0{previous_value}"
            record_id = _feature_id(kind, symbol, normalized_change)
            identity = (kind, symbol, normalized_change)
            if record_id in seen_ids and seen_ids[record_id] != identity:
                raise ValueError(
                    f"structural feature ID collision for {record_id}"
                )
            seen_ids[record_id] = identity
            feature = {
                "id": record_id,
                "kind": kind,
                "symbol": symbol,
                "change_type": change_type,
                "target": target_value,
                "result": "UNWITNESSED_FEATURE",
            }
            if previous_value is not None:
                feature["previous"] = previous_value
            features.append(feature)

    return sorted(
        features,
        key=lambda feature: (
            feature["kind"],
            feature["symbol"],
            feature["id"],
        ),
    )
