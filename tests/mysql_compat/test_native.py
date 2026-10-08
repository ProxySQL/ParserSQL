"""Unit tests for bounded native-oracle result handling and owned DB cleanup."""
import importlib.util
import pathlib
import tempfile
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[2]
MODULE = ROOT / 'scripts/mysql_compat/native.py'
spec = importlib.util.spec_from_file_location('mysql_native', MODULE)
native = importlib.util.module_from_spec(spec)
if MODULE.exists():
    spec.loader.exec_module(native)


class NativeTests(unittest.TestCase):
    def test_syntax_errors_are_distinct_from_semantic_and_client_errors(self):
        for rc, stderr, want in [
            (0, '', 'ACCEPTED'),
            (1, 'ERROR 1064 (42000): syntax', 'SYNTAX_REJECTED'),
            (1, 'ERROR 1747 (HY000): not partitioned', 'SERVER_ERROR'),
            (1, 'ERROR 1235 (42000): unsupported', 'SERVER_ERROR'),
            (1, 'ERROR 2002 (HY000): cannot connect', 'CLIENT_ERROR'),
            (1, 'client failed unexpectedly', 'CLIENT_ERROR'),
        ]:
            with self.subTest(stderr=stderr):
                self.assertEqual(native.classify_native(rc, stderr)['state'], want)

    def test_skip_ddl_explain_dml_and_cleanup_only_owned_schema(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = pathlib.Path(directory)
            client = directory / 'mysql'
            log = directory / 'log'
            client.write_text("#!/usr/bin/env python3\nimport sys\n"
                              f"with open({str(log)!r}, 'a') as f: f.write(repr(sys.stdin.read()) + '\\n')\n"
                              "if '--database' not in sys.argv: print('8.4.8\\tSTRICT_TRANS_TABLES')\n")
            client.chmod(0o755)
            cases = [dict(id='dml', sql='UPDATE t SET n=1', validity='valid', native_action='explain'),
                     dict(id='ddl', sql='CREATE PROCEDURE p() BEGIN SELECT 1; END',
                          native_action='skip'),
                     dict(id='bad', sql='SELECT FROM', validity='invalid', native_action='execute'),
                     dict(id='versioned', sql='SELECT new_syntax()', validity='valid',
                          native_validity={'8.4.8': 'invalid', '9.7.2': 'valid'},
                          native_action='execute')]
            report = native.run_native(cases, str(client), '/explicit/isolated.sock', 'root')
            self.assertEqual(report['cases'][0]['executed_sql'], 'EXPLAIN UPDATE t SET n=1')
            self.assertEqual(report['cases'][1]['state'], 'NOT VERIFIED')
            statements = log.read_text()
            self.assertIn('CREATE DATABASE `parsersql_compat_', statements)
            self.assertIn('DROP DATABASE `parsersql_compat_', statements)
            self.assertNotIn('CREATE PROCEDURE', statements)
            self.assertTrue(report['schema_cleaned_up'])
            self.assertFalse(report['cases'][2]['matches_intended_validity'])
            self.assertFalse(report['cases'][3]['matches_intended_validity'])

    def test_ddl_cases_use_fresh_owned_schemas_and_preserve_procedure_semicolons(self):
        import ast
        with tempfile.TemporaryDirectory() as directory:
            directory = pathlib.Path(directory)
            client = directory / 'mysql'
            log = directory / 'log'
            client.write_text("#!/usr/bin/env python3\nimport sys\nsql=sys.stdin.read()\n"
                              f"with open({str(log)!r}, 'a') as f: f.write(repr((sys.argv, sql)) + '\\n')\n"
                              "if sql.startswith('SELECT @@version'): print('8.4.8\\tSTRICT_TRANS_TABLES')\n"
                              "if 'BROKEN' in sql:\n print('ERROR 1064 (42000): syntax', file=sys.stderr); sys.exit(1)\n")
            client.chmod(0o755)
            procedure = "CREATE PROCEDURE p() BEGIN SELECT '$$'; SELECT 2; END"
            cases = [dict(id='create', sql='CREATE TABLE t(id INT)', validity='valid', native_action='ddl'),
                     dict(id='alter', sql='ALTER TABLE t ADD COLUMN j JSON', validity='valid',
                          native_action='ddl', native_fixture_sql='CREATE TABLE t(id INT);'),
                     dict(id='proc', sql=procedure, validity='valid', native_action='ddl'),
                     dict(id='bad', sql='CREATE TABLE BROKEN', validity='invalid', native_action='ddl')]
            report = native.run_native(cases, str(client), '/explicit/isolated.sock', 'root')
            records = report['cases']
            self.assertEqual([r['state'] for r in records],
                             ['ACCEPTED', 'ACCEPTED', 'ACCEPTED', 'SYNTAX_REJECTED'])
            schemas = [r['fixture_schema'] for r in records]
            self.assertEqual(len(set(schemas)), 4)
            self.assertNotIn(report['fixture_schema'], schemas)
            self.assertTrue(all(r['schema_cleaned_up'] for r in records))
            self.assertEqual(records[2]['executed_sql'], procedure)
            calls = [ast.literal_eval(line) for line in log.read_text().splitlines()]
            proc_input = next(sql for _, sql in calls if procedure in sql)
            self.assertIn('DELIMITER ', proc_input)
            marker = proc_input.split('DELIMITER ', 1)[1].splitlines()[0]
            self.assertNotIn(marker, procedure)
            self.assertIn(procedure + '\n' + marker + '\nDELIMITER ;', proc_input)
            for schema in schemas:
                self.assertIn(f'DROP DATABASE `{schema}`;', [sql for _, sql in calls])
            alter_calls = [sql for args, sql in calls if schemas[1] in args]
            self.assertTrue(any('CREATE TABLE t(id INT);' in sql for sql in alter_calls))
            self.assertFalse(any(native.FIXTURE_SQL in sql for args, sql in calls
                                 if any(schema in args for schema in schemas)))

    def test_ddl_fixture_failure_cleans_case_and_main_schemas(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = pathlib.Path(directory)
            client = directory / 'mysql'
            log = directory / 'log'
            client.write_text("#!/usr/bin/env python3\nimport sys\nsql=sys.stdin.read()\n"
                              f"with open({str(log)!r}, 'a') as f: f.write(repr(sql) + '\\n')\n"
                              "if sql.startswith('SELECT @@version'): print('8.4.8\\t')\n"
                              "if 'BAD_FIXTURE' in sql: sys.exit(1)\n")
            client.chmod(0o755)
            cases = [dict(id='ddl', sql='CREATE TABLE t(id INT)', validity='valid',
                          native_action='ddl', native_fixture_sql='BAD_FIXTURE')]
            with self.assertRaisesRegex(RuntimeError, 'fixture setup'):
                native.run_native(cases, str(client), '/explicit/isolated.sock', 'root')
            statements = log.read_text()
            self.assertEqual(statements.count('CREATE DATABASE'), 2)
            self.assertEqual(statements.count('DROP DATABASE'), 2)

    def test_failed_ddl_schema_creation_drops_only_main_owned_schema(self):
        import ast
        with tempfile.TemporaryDirectory() as directory:
            directory = pathlib.Path(directory)
            client = directory / 'mysql'
            log = directory / 'log'
            client.write_text("#!/usr/bin/env python3\nimport sys\nsql=sys.stdin.read()\n"
                              f"from pathlib import Path\nlog=Path({str(log)!r})\n"
                              "previous=log.read_text() if log.exists() else ''\n"
                              "with log.open('a') as f: f.write(repr(sql) + '\\n')\n"
                              "if sql.startswith('SELECT @@version'): print('8.4.8\\t')\n"
                              "if sql.startswith('CREATE DATABASE') and 'CREATE DATABASE' in previous: sys.exit(1)\n")
            client.chmod(0o755)
            cases = [dict(id='ddl', sql='CREATE TABLE t(id INT)', validity='valid', native_action='ddl')]
            with self.assertRaisesRegex(RuntimeError, 'Cannot create isolated DDL schema'):
                native.run_native(cases, str(client), '/explicit/isolated.sock', 'root')
            statements = [ast.literal_eval(line) for line in log.read_text().splitlines()]
            created = [sql for sql in statements if sql.startswith('CREATE DATABASE')]
            dropped = [sql for sql in statements if sql.startswith('DROP DATABASE')]
            self.assertEqual(len(created), 2)
            self.assertEqual(dropped, [created[0].replace('CREATE DATABASE', 'DROP DATABASE')])

    def test_create_failure_never_drops_database_it_does_not_own(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = pathlib.Path(directory)
            client = directory / 'mysql'
            log = directory / 'log'
            client.write_text("#!/usr/bin/env python3\nimport sys\nsql=sys.stdin.read()\n"
                              f"with open({str(log)!r}, 'a') as f: f.write(sql + '\\n')\n"
                              "if sql.startswith('SELECT @@version'): print('8.4.8\\t')\n"
                              "else: sys.exit(1)\n")
            client.chmod(0o755)
            with self.assertRaises(RuntimeError):
                native.run_native([], str(client), '/explicit/isolated.sock', 'root')
            self.assertNotIn('DROP DATABASE', log.read_text())


if __name__ == '__main__':
    unittest.main()
