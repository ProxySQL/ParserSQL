#!/usr/bin/env python3

from collections import Counter, defaultdict

if __package__:
    from .baseline import RESULTS
else:
    from baseline import RESULTS


MAX_SQL_DISPLAY = 200


def _major(pg_version):
    return str(pg_version).split(".", maxsplit=1)[0]


def _escape_cell(value):
    text = "" if value is None else str(value)
    return (
        text.replace("\\", "\\\\")
        .replace("|", "\\|")
        .replace("\r", " ")
        .replace("\n", " ")
    )


def _sql_excerpt(sql):
    text = " ".join(str(sql).split())
    if len(text) <= MAX_SQL_DISPLAY:
        return text
    return text[: MAX_SQL_DISPLAY - 3] + "..."


def _table(headers, rows):
    output = []
    output.append("| " + " | ".join(headers) + " |")
    output.append("| " + " | ".join(["---"] * len(headers)) + " |")
    if not rows:
        output.append("| " + " | ".join(["-"] * len(headers)) + " |")
        return output
    for row in rows:
        output.append(
            "| "
            + " | ".join(_escape_cell(value) for value in row)
            + " |"
        )
    return output


def _sorted_statement_rows(rows):
    return sorted(
        rows,
        key=lambda row: (
            str(row.get("result", "")),
            str(row.get("oracle_node", "")),
            str(row.get("id", "")),
        ),
    )


def _statement_table(rows, include_transition=False):
    headers = ["Result", "Oracle Node", "ID", "SQL"]
    if include_transition:
        headers = ["Previous", "Current", "Oracle Node", "ID", "SQL"]
    output_rows = []
    for row in rows:
        if include_transition:
            output_rows.append(
                [
                    row.get("previous_result", ""),
                    row.get("current_result", row.get("result", "")),
                    row.get("oracle_node", ""),
                    row.get("id", ""),
                    _sql_excerpt(row.get("sql", "")),
                ]
            )
        else:
            output_rows.append(
                [
                    row.get("result", ""),
                    row.get("oracle_node", ""),
                    row.get("id", ""),
                    _sql_excerpt(row.get("sql", "")),
                ]
            )
    return _table(headers, output_rows)


def _classified_routing_rows(results):
    counts = Counter(
        row.get("oracle_node", "")
        for row in results
        if row.get("result") == "CLASSIFIED_ONLY"
    )
    return [
        [oracle_node, count]
        for oracle_node, count in sorted(counts.items())
    ]


def _witness_links(witness_result):
    links = defaultdict(list)
    for witness in witness_result.get("witnesses", []):
        for feature_id in witness.get("structural_feature_ids", []):
            links[feature_id].append(witness.get("id", ""))
    return {
        feature_id: sorted(witness_ids)
        for feature_id, witness_ids in sorted(links.items())
    }


def _structural_rows(structural_features, links, *, only_unwitnessed):
    rows = []
    for feature in sorted(
        structural_features,
        key=lambda row: (
            str(row.get("kind", "")),
            str(row.get("symbol", "")),
            str(row.get("id", "")),
        ),
    ):
        feature_id = feature.get("id", "")
        witness_ids = links.get(feature_id, [])
        if only_unwitnessed and witness_ids:
            continue
        if not only_unwitnessed and not witness_ids:
            continue
        rows.append(
            [
                feature_id,
                feature.get("kind", ""),
                feature.get("symbol", ""),
                ", ".join(witness_ids) if witness_ids else "-",
                feature.get("target", ""),
            ]
        )
    return rows


def _newly_supported_rows(baseline_evaluation):
    rows = []
    for row in baseline_evaluation.get("allowed", []):
        if (
            row.get("previous_result") != "DEEP_SUPPORTED"
            and row.get("current_result") == "DEEP_SUPPORTED"
        ):
            rows.append(row)
    return sorted(rows, key=lambda row: str(row.get("id", "")))


def generate_report(
    context,
    results,
    release_delta,
    structural_features,
    baseline_evaluation,
    witness_result,
):
    pins = context["pins"]["versions"]
    previous = pins["previous"]
    target = pins["target"]
    target_major = _major(target["pg_version"])
    counts = Counter(row.get("result") for row in results)
    links = _witness_links(witness_result)

    lines = [
        f"# PostgreSQL {target_major} Compatibility",
        "",
        f"Generated: {context['generated_at']}",
        f"ParserSQL commit: `{context['parsersql_commit']}`",
        (
            "libpg_query previous: "
            f"`{previous['branch']}` `{previous['commit']}` "
            f"(PostgreSQL {previous['pg_version']})"
        ),
        (
            "libpg_query target: "
            f"`{target['branch']}` `{target['commit']}` "
            f"(PostgreSQL {target['pg_version']})"
        ),
        "",
        "## Result Totals",
        "",
    ]
    lines.extend(
        _table(
            ["Result", "Count"],
            [[result, counts[result]] for result in RESULTS],
        )
    )
    lines.extend(
        [
            "",
            "## PG18 Backlog",
            "",
        ]
    )
    lines.extend(
        _statement_table(
            _sorted_statement_rows(
                row for row in results if row.get("result") != "DEEP_SUPPORTED"
            )
        )
    )
    lines.extend(
        [
            "",
            "## CLASSIFIED_ONLY Routing Coverage",
            "",
        ]
    )
    lines.extend(
        _table(
            ["Oracle Node", "Count"],
            _classified_routing_rows(results),
        )
    )
    lines.extend(
        [
            "",
            "## PG17 to PG18 Release Delta",
            "",
        ]
    )
    lines.extend(_statement_table(_sorted_statement_rows(release_delta)))
    lines.extend(
        [
            "",
            "## Newly Supported Baseline Transitions",
            "",
        ]
    )
    lines.extend(
        _statement_table(
            _newly_supported_rows(baseline_evaluation),
            include_transition=True,
        )
    )
    lines.extend(
        [
            "",
            "## Regressed Baseline Transitions",
            "",
        ]
    )
    lines.extend(
        _statement_table(
            sorted(
                baseline_evaluation.get("regressions", []),
                key=lambda row: str(row.get("id", "")),
            ),
            include_transition=True,
        )
    )
    lines.extend(
        [
            "",
            "## Structural Feature Witness Links",
            "",
        ]
    )
    lines.extend(
        _table(
            ["Feature ID", "Kind", "Symbol", "Witnesses", "Target"],
            _structural_rows(structural_features, links, only_unwitnessed=False),
        )
    )
    lines.extend(
        [
            "",
            "## Unwitnessed Structural Features",
            "",
        ]
    )
    lines.extend(
        _table(
            ["Feature ID", "Kind", "Symbol", "Witnesses", "Target"],
            _structural_rows(structural_features, links, only_unwitnessed=True),
        )
    )
    lines.extend(
        [
            "",
            "## Unlinked Witnesses",
            "",
        ]
    )
    lines.extend(
        _table(
            ["Witness ID"],
            [
                [witness_id]
                for witness_id in sorted(
                    witness_result.get("unlinked_witnesses", [])
                )
            ],
        )
    )
    lines.extend(
        [
            "",
            "## Reproduction",
            "",
        ]
    )
    for command in context.get("commands", []):
        lines.append(f"- `{command}`")

    return "\n".join(lines) + "\n"
