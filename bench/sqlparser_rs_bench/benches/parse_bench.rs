use criterion::{criterion_group, criterion_main, Criterion};
use sqlparser::dialect::{MySqlDialect, PostgreSqlDialect};
use sqlparser::parser::Parser;

fn bench_queries(c: &mut Criterion) {
    let mysql = MySqlDialect {};
    let pgsql = PostgreSqlDialect {};

    let queries = vec![
        ("simple_select",   "SELECT col FROM t WHERE id = 1"),
        ("select_join",     "SELECT u.id, o.total FROM users u JOIN orders o ON u.id = o.user_id WHERE o.status = 'active'"),
        ("select_complex",  "SELECT u.id, u.name, COUNT(o.id) AS order_count \
                             FROM users u LEFT JOIN orders o ON u.id = o.user_id \
                             WHERE u.status = 'active' \
                             GROUP BY u.id, u.name \
                             HAVING COUNT(o.id) > 5 \
                             ORDER BY order_count DESC LIMIT 50"),
        ("insert_values",   "INSERT INTO users (name, email) VALUES ('John', 'john@example.com')"),
        ("update_simple",   "UPDATE users SET status = 'inactive' WHERE last_login < '2024-01-01'"),
        ("delete_simple",   "DELETE FROM users WHERE id = 42"),
        ("set_simple",      "SET session.wait_timeout = 600"),
        ("begin",           "BEGIN"),
    ];

    // MySQL dialect benchmarks
    for (name, sql) in &queries {
        c.bench_function(&format!("sqlparser_rs_mysql_{}", name), |b| {
            b.iter(|| {
                let result = Parser::parse_sql(&mysql, sql);
                criterion::black_box(result)
            })
        });
    }

    // PostgreSQL dialect benchmarks
    for (name, sql) in &queries {
        c.bench_function(&format!("sqlparser_rs_pgsql_{}", name), |b| {
            b.iter(|| {
                let result = Parser::parse_sql(&pgsql, sql);
                criterion::black_box(result)
            })
        });
    }
}

criterion_group!(benches, bench_queries);
criterion_main!(benches);
