#!/usr/bin/env python3
"""Check a bounded, curated MySQL witness corpus; this is not a server oracle."""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
PINS = {'8.4.8': '0896fcd61dec11a0904166911a0126f59daaa1bf',
        '9.7.2': '008e09c2834b98143a8c067d4d225c90953050cf'}


def classify(result):
    if result['status'] != 0:
        return {1: 'PARTIAL', 2: 'ERROR'}[result['status']]
    if not result['ast']:
        return 'NO_AST'
    if not result['full_input']:
        return 'TRAILING_INPUT'
    return 'COMPLETE'


def evaluate(case, original, reparsed):
    category = classify(original)
    if case['validity'] == 'invalid':
        return 'MALFORMED_ACCEPTED' if category == 'COMPLETE' else 'REJECTED'
    if category != 'COMPLETE':
        return category
    if 'expected_sql' in case and original['emitted'] != case['expected_sql']:
        return 'EMISSION_MISMATCH'
    if reparsed is None:
        return 'REPARSE_MISSING'
    if classify(reparsed) != 'COMPLETE':
        return 'REPARSE_' + classify(reparsed)
    if original['emitted'] != reparsed['emitted']:
        return 'EMISSION_UNSTABLE'
    return 'CHECKED'


def matches_expectation(case, outcome):
    # Promotions must be reviewed and recorded; broad "anything failed" baselines hide drift.
    return case['expected'] == outcome


def run_probe(probe, queries):
    payload = b''.join(query.hex().encode('ascii') + b'\n' for query in queries)
    process = subprocess.run([str(probe)], input=payload, capture_output=True,
                             check=True, timeout=60)
    rows = process.stdout.decode('ascii').splitlines()
    if len(rows) != len(queries):
        raise ValueError('Probe returned a different number of records')
    results = []
    for row in rows:
        status, full, ast, remaining, emitted = row.split('\t')
        if status not in ('0', '1', '2') or full not in ('0', '1') or ast not in ('0', '1'):
            raise ValueError('Invalid probe status or boolean')
        results.append(dict(status=int(status), full_input=full == '1', ast=ast == '1',
                            remaining=bytes.fromhex(remaining).decode('utf-8', 'surrogateescape'),
                            emitted=bytes.fromhex(emitted).decode('utf-8', 'surrogateescape')))
    return results


def verify_sources(corpus, version, source):
    commit = subprocess.check_output(['git', '-C', str(source), 'rev-parse', 'HEAD'], text=True).strip()
    if commit != PINS[version]:
        raise ValueError(f'{version}: expected {PINS[version]}, found {commit}')
    count = 0
    for case in corpus['cases']:
        for ref in case.get('sources', []):
            if ref['version'] != version:
                continue
            lines = (source / ref['path']).read_text().splitlines()
            if lines[ref['line'] - 1].strip() != ref['excerpt']:
                raise ValueError(f"Source drift: {case['id']} {ref['path']}:{ref['line']}")
            count += 1
    return count


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--probe', type=Path, required=True)
    parser.add_argument('--corpus', type=Path, default=ROOT / 'tests/mysql_compat/cases.json')
    parser.add_argument('--source84', type=Path)
    parser.add_argument('--source97', type=Path)
    parser.add_argument('--report', type=Path, help='Optional JSON report, preferably outside the repo')
    parser.add_argument('--native-socket', help='Explicit isolated server socket; creates and drops an owned fixture schema')
    parser.add_argument('--mysql-client', default='mysql', help='Client executable for optional native checks')
    parser.add_argument('--native-user', default='root')
    args = parser.parse_args()
    raw = args.corpus.read_bytes()
    corpus = json.loads(raw)
    if corpus['pins'] != PINS or corpus['oracle']['state'] != 'NOT VERIFIED':
        raise ValueError('Unexpected corpus pin/oracle metadata')
    cases = corpus['cases']
    if len({case['id'] for case in cases}) != len(cases):
        raise ValueError('Duplicate witness ID')
    verified = {}
    for version, source in [('8.4.8', args.source84), ('9.7.2', args.source97)]:
        if source:
            verified[version] = verify_sources(corpus, version, source)
    original = run_probe(args.probe.resolve(), [c['sql'].encode('utf-8') for c in cases])
    indices = [i for i, result in enumerate(original) if classify(result) == 'COMPLETE']
    emitted = [original[i]['emitted'].encode('utf-8', 'surrogateescape') for i in indices]
    reparsed = dict(zip(indices, run_probe(args.probe.resolve(), emitted)))
    rows = []
    for i, case in enumerate(cases):
        outcome = evaluate(case, original[i], reparsed.get(i))
        matched = matches_expectation(case, outcome)
        rows.append(dict(id=case['id'], outcome=outcome, expected=case['expected'],
                         matched=matched, original=original[i], reparsed=reparsed.get(i)))
        print(f"{'OK' if matched else 'DRIFT'} {case['id']}: {outcome} (expected {case['expected']})")
    report = dict(scope='curated witnesses only; no dialect coverage percentage',
                  oracle=corpus['oracle'], pins=PINS, sql_mode=corpus['sql_mode'],
                  corpus_sha256=hashlib.sha256(raw).hexdigest(),
                  probe_sha256=hashlib.sha256(args.probe.read_bytes()).hexdigest(),
                  source_references_verified=verified,
                  outcomes=dict(Counter(row['outcome'] for row in rows)), results=rows)
    if args.native_socket:
        from native import run_native
        report['native_oracle'] = run_native(cases, args.mysql_client, args.native_socket, args.native_user)
        states = Counter(case['state'] for case in report['native_oracle']['cases'])
        print('Native observations: ' + ', '.join(f'{state}={count}' for state, count in sorted(states.items())))
    else:
        report['native_oracle'] = dict(state='NOT VERIFIED')
    if args.report:
        args.report.write_text(json.dumps(report, indent=2) + '\n')
    failures = sum(not row['matched'] for row in rows)
    native_rows = report['native_oracle'].get('cases', [])
    failures += sum(row.get('matches_intended_validity') is False for row in native_rows)
    if any(row['state'] == 'CLIENT_ERROR' for row in native_rows):
        return 2
    print(f"{len(rows)} curated witnesses; {failures} expectation mismatches; native oracle {report['native_oracle']['state']}")
    return bool(failures)


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (OSError, ValueError, KeyError, RuntimeError, subprocess.SubprocessError) as error:
        print(f'mysql_compat: {error}', file=sys.stderr)
        sys.exit(2)
