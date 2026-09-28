"""Run with python3 -m unittest discover -s tests/mysql_compat -v."""
import importlib.util
import json
import pathlib
import subprocess
import tempfile
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[2]
MODULE = ROOT / 'scripts/mysql_compat/run.py'
spec = importlib.util.spec_from_file_location('mysql_compat', MODULE)
compat = importlib.util.module_from_spec(spec)
if MODULE.exists():
    spec.loader.exec_module(compat)


def record(**kwargs):
    result = dict(status=0, full_input=True, ast=True, remaining='', emitted='SELECT 1')
    result.update(kwargs)
    return result


class ClassificationTests(unittest.TestCase):
    def test_partial_error_and_missing_ast_never_count_as_complete(self):
        for changes, want in [({}, 'COMPLETE'), ({'status': 1}, 'PARTIAL'),
                              ({'status': 2}, 'ERROR'), ({'ast': False}, 'NO_AST'),
                              ({'ast': False, 'full_input': False}, 'NO_AST'),
                              ({'full_input': False}, 'TRAILING_INPUT')]:
            with self.subTest(want=want, changes=changes):
                self.assertEqual(compat.classify(record(**changes)), want)

    def test_reparse_alone_does_not_hide_json_operator_corruption(self):
        case = dict(validity='valid', expected_sql="SELECT j -> '$.x' FROM t")
        damaged = record(emitted="SELECT j > '$.x' FROM t")
        self.assertEqual(compat.evaluate(case, damaged, damaged), 'EMISSION_MISMATCH')

    def test_negative_control_rejects_complete_ast_even_if_emission_changes(self):
        self.assertEqual(compat.evaluate(dict(validity='invalid'), record(), record()),
                         'MALFORMED_ACCEPTED')
        self.assertEqual(compat.evaluate(dict(validity='invalid'), record(status=2), None),
                         'REJECTED')

    def test_emission_must_reparse_completely_and_stabilize(self):
        case = dict(validity='valid')
        self.assertEqual(compat.evaluate(case, record(), record(ast=False)), 'REPARSE_NO_AST')
        self.assertEqual(compat.evaluate(case, record(), record(emitted='SELECT 2')),
                         'EMISSION_UNSTABLE')
        self.assertEqual(compat.evaluate(case, record(), record()), 'CHECKED')

    def test_fixed_known_gap_requires_explicit_expectation_update(self):
        case = dict(expected='TRAILING_INPUT', known_gap=True)
        self.assertFalse(compat.matches_expectation(case, 'CHECKED'))
        self.assertFalse(compat.matches_expectation(case, 'ERROR'))
        self.assertTrue(compat.matches_expectation(case, 'TRAILING_INPUT'))


class ProbeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory()
        cls.probe = pathlib.Path(cls.directory.name) / 'probe'
        subprocess.run(['python3', str(ROOT / 'scripts/mysql_compat/build_probe.py'),
                        '--output', str(cls.probe)], check=True)

    @classmethod
    def tearDownClass(cls):
        cls.directory.cleanup()

    def test_byte_framing_and_multiple_inputs_keep_ast_storage_alive(self):
        queries = [b"SELECT 'first'", b"SELECT 'a\nb\tc'", b"SELECT 'a\x00b'", b'SELECT 9876']
        results = compat.run_probe(self.probe, queries)
        self.assertEqual(len(results), 4)
        self.assertEqual(results[0]['emitted'], "SELECT 'first'")
        self.assertEqual(results[1]['emitted'], "SELECT 'a\nb\tc'")
        self.assertEqual(results[2]['emitted'], "SELECT 'a\x00b'")
        self.assertEqual(results[3]['emitted'], 'SELECT 9876')

    def test_runner_exit_status_tracks_expectation_drift_and_writes_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory)
            fixture = dict(pins=compat.PINS, oracle=dict(state='NOT VERIFIED'), sql_mode={},
                           cases=[dict(id='control', sql='SELECT 1', validity='valid',
                                       expected='CHECKED', expected_sql='SELECT 1')])
            corpus = path / 'cases.json'
            report = path / 'report.json'
            command = ['python3', str(MODULE), '--probe', str(self.probe),
                       '--corpus', str(corpus), '--report', str(report)]
            corpus.write_text(json.dumps(fixture))
            passed = subprocess.run(command, capture_output=True, text=True)
            self.assertEqual(passed.returncode, 0, passed.stderr)
            evidence = json.loads(report.read_text())
            self.assertEqual(evidence['results'][0]['original']['emitted'], 'SELECT 1')
            self.assertEqual(evidence['results'][0]['reparsed']['emitted'], 'SELECT 1')
            fixture['cases'][0]['expected'] = 'TRAILING_INPUT'
            fixture['cases'][0]['known_gap'] = True
            corpus.write_text(json.dumps(fixture))
            drift = subprocess.run(command, capture_output=True, text=True)
            self.assertEqual(drift.returncode, 1, drift.stderr)
            self.assertFalse(json.loads(report.read_text())['results'][0]['matched'])

    def test_bad_transport_fails_instead_of_truncating_input(self):
        for payload in (b'0\n', b'zz\n'):
            result = subprocess.run([str(self.probe)], input=payload, capture_output=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(result.stdout, b'')


if __name__ == '__main__':
    unittest.main()
