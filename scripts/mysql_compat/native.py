"""Optional native checks on an explicitly selected isolated local MySQL server."""
import re
import subprocess
import uuid

FIXTURE_SQL = """CREATE TABLE t(
 id INT PRIMARY KEY, n INT, j JSON, data JSON, body TEXT, d DATETIME,
 KEY idx(n), FULLTEXT KEY(body));
CREATE TABLE u(id INT, n INT);
CREATE TABLE pt(id INT PRIMARY KEY, n INT, KEY idx(n))
 PARTITION BY RANGE(id) (PARTITION p0 VALUES LESS THAN (10),
 PARTITION p1 VALUES LESS THAN MAXVALUE);
INSERT INTO t VALUES(1,2,JSON_OBJECT('name','mysql','n',1),
 JSON_OBJECT('name','mysql','n',1),'mysql','2026-01-01');
INSERT INTO pt VALUES(1,2),(11,3);
"""


def classify_native(returncode, stderr):
    if returncode == 0:
        return dict(state='ACCEPTED', error_code=None)
    match = re.search(r'ERROR (\d+)\b', stderr)
    code = int(match[1]) if match else None
    if code in (1064, 1149):
        state = 'SYNTAX_REJECTED'
    elif code is None or 2000 <= code < 3000:
        state = 'CLIENT_ERROR'
    else:
        state = 'SERVER_ERROR'
    return dict(state=state, error_code=code)


def run_native(cases, client, socket, user):
    if not socket or not socket.startswith('/'):
        raise ValueError('Native oracle requires an explicit absolute path to an isolated server socket')
    command = [client, '--no-defaults', '--protocol=SOCKET', '--socket=' + socket,
               '--user=' + user, '--batch', '--raw', '--skip-column-names',
               '--default-character-set=utf8mb4', '--connect-timeout=5']

    def query(sql, schema=None):
        args = command + (['--database', schema] if schema else [])
        return subprocess.run(args, input=sql, text=True, capture_output=True, timeout=30)

    metadata = query('SELECT @@version, @@session.sql_mode;')
    if metadata.returncode:
        raise RuntimeError('Cannot query native server metadata: ' + metadata.stderr.strip())
    version, mode = metadata.stdout.rstrip('\n').split('\t')
    if not re.match(r'^(8\.4\.8|9\.7\.2)(?:-|$)', version):
        raise ValueError('Native server must be MySQL 8.4.8 or 9.7.2; found ' + version)
    schema = 'parsersql_compat_' + uuid.uuid4().hex
    create = query(f'CREATE DATABASE `{schema}`;')
    if create.returncode:
        raise RuntimeError('Cannot create isolated native fixture schema: ' + create.stderr.strip())
    report = dict(state='RUN', version=version, sql_mode=mode, socket=socket,
                  fixture_sql=FIXTURE_SQL, fixture_schema=schema, schema_cleaned_up=False,
                  scope='SQL execution/EXPLAIN observations; not a raw-parser or equivalence oracle',
                  cases=[])
    try:
        fixture = query(FIXTURE_SQL, schema)
        if fixture.returncode:
            raise RuntimeError('Native fixture setup failed: ' + fixture.stderr.strip())
        for case in cases:
            action = case.get('native_action', 'skip')
            if action == 'skip':
                report['cases'].append(dict(id=case['id'], state='NOT VERIFIED',
                                            reason='No native action configured'))
                continue
            if action not in ('execute', 'explain', 'ddl'):
                raise ValueError('Unknown native action: ' + action)
            sql = ('EXPLAIN ' if action == 'explain' else '') + case['sql']
            # Reapply the captured mode for each connection, keeping every witness comparable.
            escaped_mode = mode.replace("'", "''")
            session = f"SET SESSION sql_mode='{escaped_mode}';\n"
            ddl_metadata = {}
            if action == 'ddl':
                # DDL commits implicitly. A separate owned schema avoids both
                # fixture-name collisions and cross-case changes to shared tables.
                ddl_schema = 'parsersql_compat_' + uuid.uuid4().hex
                created = query(f'CREATE DATABASE `{ddl_schema}`;')
                if created.returncode:
                    raise RuntimeError('Cannot create isolated DDL schema: ' + created.stderr.strip())
                fixture_sql = case.get('native_fixture_sql', '')
                ddl_metadata = dict(fixture_schema=ddl_schema, fixture_sql=fixture_sql,
                                    schema_cleaned_up=False)
                try:
                    if fixture_sql:
                        setup = query(session + fixture_sql, ddl_schema)
                        if setup.returncode:
                            raise RuntimeError('Native DDL fixture setup failed: ' + setup.stderr.strip())
                    # mysql's client delimiter must not split procedure bodies;
                    # choose a marker absent from this exact witness.
                    delimiter = '$$'
                    while delimiter in sql:
                        delimiter = '$' + delimiter + '$'
                    client_input = session + 'DELIMITER ' + delimiter + '\n' + sql + '\n' + delimiter + '\nDELIMITER ;\n'
                    result = query(client_input, ddl_schema)
                    ddl_metadata['client_input'] = client_input
                finally:
                    cleaned = query(f'DROP DATABASE `{ddl_schema}`;')
                    ddl_metadata['schema_cleaned_up'] = cleaned.returncode == 0
                    if cleaned.returncode:
                        raise RuntimeError(f'Native DDL schema cleanup failed for {ddl_schema}: ' + cleaned.stderr.strip())
            else:
                result = query(session + sql, schema)
            record = classify_native(result.returncode, result.stderr)
            record.update(ddl_metadata)
            intended_validity = case.get('native_validity', {}).get(version.split('-')[0], case['validity'])
            record['intended_validity'] = intended_validity
            record['matches_intended_validity'] = (
                intended_validity == 'valid' if record['state'] == 'ACCEPTED' else
                intended_validity == 'invalid' if record['state'] == 'SYNTAX_REJECTED' else None)
            record.update(id=case['id'], sql=case['sql'], action=action, executed_sql=sql,
                          returncode=result.returncode, stdout=result.stdout.rstrip('\n'),
                          stderr=result.stderr.rstrip('\n'))
            report['cases'].append(record)
    finally:
        cleanup = query(f'DROP DATABASE `{schema}`;')
        report['schema_cleaned_up'] = cleanup.returncode == 0
        if cleanup.returncode:
            raise RuntimeError(f'Native schema cleanup failed for {schema}: ' + cleanup.stderr.strip())
    return report
