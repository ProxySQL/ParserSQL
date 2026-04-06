// bench_distributed.cpp — Distributed query benchmark with pipeline breakdown
//
// Measures latency for each stage of the distributed query pipeline:
//   parse -> plan -> optimize -> distribute -> execute
//
// Runs each of 7 demo queries N times and reports avg/min/max/p95.
//
// Usage:
//   ./bench_distributed \
//       --backend "mysql://root:test@127.0.0.1:13306/testdb?name=shard1" \
//       --backend "mysql://root:test@127.0.0.1:13307/testdb?name=shard2" \
//       --shard "users:id:shard1,shard2" \
//       --shard "orders:id:shard1,shard2" \
//       [--iterations 100]

#include <iostream>
#include <string>
#include <vector>
#include <sstream>
#include <chrono>
#include <algorithm>
#include <cstring>
#include <iomanip>
#include <numeric>
#include <cmath>

#include "sql_parser/parser.h"
#include "sql_parser/common.h"
#include "sql_engine/session.h"
#include "sql_engine/in_memory_catalog.h"
#include "sql_engine/data_source.h"
#include "sql_engine/local_txn.h"
#include "sql_engine/multi_remote_executor.h"
#include "sql_engine/shard_map.h"
#include "sql_engine/backend_config.h"
#include "sql_engine/result_set.h"
#include "sql_engine/plan_builder.h"
#include "sql_engine/plan_executor.h"
#include "sql_engine/optimizer.h"
#include "sql_engine/distributed_planner.h"
#include "sql_engine/value.h"

using namespace sql_parser;
using namespace sql_engine;

using Clock = std::chrono::high_resolution_clock;
using Duration = std::chrono::nanoseconds;

// ============================================================
// Backend URL parsing (reused from sqlengine.cpp)
// ============================================================

struct ParsedBackend {
    BackendConfig config;
    bool ok = false;
    std::string error;
};

static ParsedBackend parse_backend_url(const std::string& url) {
    ParsedBackend pb;

    size_t scheme_end = url.find("://");
    if (scheme_end == std::string::npos) {
        pb.error = "Invalid URL (no scheme): " + url;
        return pb;
    }
    std::string scheme = url.substr(0, scheme_end);
    if (scheme == "mysql") {
        pb.config.dialect = Dialect::MySQL;
    } else if (scheme == "pgsql" || scheme == "postgres" || scheme == "postgresql") {
        pb.config.dialect = Dialect::PostgreSQL;
    } else {
        pb.error = "Unknown scheme: " + scheme;
        return pb;
    }

    std::string rest = url.substr(scheme_end + 3);

    size_t qpos = rest.find('?');
    std::string query_part;
    if (qpos != std::string::npos) {
        query_part = rest.substr(qpos + 1);
        rest = rest.substr(0, qpos);
    }

    if (!query_part.empty()) {
        size_t name_pos = query_part.find("name=");
        if (name_pos != std::string::npos) {
            size_t vstart = name_pos + 5;
            size_t vend = query_part.find('&', vstart);
            pb.config.name = query_part.substr(vstart,
                vend == std::string::npos ? std::string::npos : vend - vstart);
        }
    }

    size_t at_pos = rest.find('@');
    if (at_pos != std::string::npos) {
        std::string userpass = rest.substr(0, at_pos);
        rest = rest.substr(at_pos + 1);
        size_t colon = userpass.find(':');
        if (colon != std::string::npos) {
            pb.config.user = userpass.substr(0, colon);
            pb.config.password = userpass.substr(colon + 1);
        } else {
            pb.config.user = userpass;
        }
    }

    size_t slash = rest.find('/');
    std::string hostport;
    if (slash != std::string::npos) {
        hostport = rest.substr(0, slash);
        pb.config.database = rest.substr(slash + 1);
    } else {
        hostport = rest;
    }

    size_t colon = hostport.find(':');
    if (colon != std::string::npos) {
        pb.config.host = hostport.substr(0, colon);
        try {
            pb.config.port = static_cast<uint16_t>(std::stoi(hostport.substr(colon + 1)));
        } catch (...) {
            pb.error = "Invalid port in: " + url;
            return pb;
        }
    } else {
        pb.config.host = hostport;
        pb.config.port = (pb.config.dialect == Dialect::MySQL) ? 3306 : 5432;
    }

    if (pb.config.name.empty())
        pb.config.name = pb.config.host + ":" + std::to_string(pb.config.port);

    pb.ok = true;
    return pb;
}

// ============================================================
// Shard spec parsing
// ============================================================

struct ParsedShard {
    TableShardConfig config;
    bool ok = false;
    std::string error;
};

static ParsedShard parse_shard_spec(const std::string& spec) {
    ParsedShard ps;

    size_t c1 = spec.find(':');
    if (c1 == std::string::npos) {
        ps.error = "Invalid shard spec: " + spec;
        return ps;
    }
    size_t c2 = spec.find(':', c1 + 1);
    if (c2 == std::string::npos) {
        ps.error = "Invalid shard spec: " + spec;
        return ps;
    }

    ps.config.table_name = spec.substr(0, c1);
    ps.config.shard_key = spec.substr(c1 + 1, c2 - c1 - 1);
    std::string shards_str = spec.substr(c2 + 1);

    std::istringstream ss(shards_str);
    std::string shard;
    while (std::getline(ss, shard, ',')) {
        if (!shard.empty())
            ps.config.shards.push_back(ShardInfo{shard});
    }

    if (ps.config.shards.empty()) {
        ps.error = "No shards specified in: " + spec;
        return ps;
    }

    ps.ok = true;
    return ps;
}

// ============================================================
// Statistics
// ============================================================

struct Stats {
    double avg_ns;
    double min_ns;
    double max_ns;
    double p50_ns;
    double p95_ns;
    double stddev_ns;
};

static Stats compute_stats(std::vector<int64_t>& samples) {
    Stats s{};
    if (samples.empty()) return s;

    std::sort(samples.begin(), samples.end());
    size_t n = samples.size();

    double sum = 0;
    for (auto v : samples) sum += static_cast<double>(v);
    s.avg_ns = sum / static_cast<double>(n);
    s.min_ns = static_cast<double>(samples.front());
    s.max_ns = static_cast<double>(samples.back());
    s.p50_ns = static_cast<double>(samples[n / 2]);
    s.p95_ns = static_cast<double>(samples[static_cast<size_t>(static_cast<double>(n) * 0.95)]);

    double sq_sum = 0;
    for (auto v : samples) {
        double d = static_cast<double>(v) - s.avg_ns;
        sq_sum += d * d;
    }
    s.stddev_ns = std::sqrt(sq_sum / static_cast<double>(n));

    return s;
}

static std::string format_ns(double ns) {
    if (ns < 1000.0)
        return std::to_string(static_cast<int>(ns)) + " ns";
    if (ns < 1000000.0) {
        std::ostringstream oss;
        oss << std::fixed << std::setprecision(1) << (ns / 1000.0) << " us";
        return oss.str();
    }
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(2) << (ns / 1000000.0) << " ms";
    return oss.str();
}

// ============================================================
// Per-stage timing for one query iteration
// ============================================================

struct PipelineTiming {
    int64_t parse_ns;
    int64_t plan_ns;
    int64_t optimize_ns;
    int64_t distribute_ns;
    int64_t execute_ns;
    int64_t total_ns;
    size_t row_count;
    bool success;
};

// ============================================================
// Query definitions
// ============================================================

struct QueryDef {
    const char* name;
    const char* sql;
    const char* description;
};

static const QueryDef QUERIES[] = {
    {
        "full_scan",
        "SELECT * FROM users",
        "Scan all rows from both shards"
    },
    {
        "filter_pushdown",
        "SELECT name, age, salary FROM users WHERE dept = 'Engineering'",
        "Filter pushed to both shards"
    },
    {
        "distributed_agg",
        "SELECT dept, COUNT(*) FROM users GROUP BY dept",
        "Count by department, merged from 2 shards"
    },
    {
        "sort_limit",
        "SELECT name, salary FROM users ORDER BY salary DESC LIMIT 3",
        "Top 3 highest paid, merge-sort across shards"
    },
    {
        "cross_shard_join",
        "SELECT u.name, o.total, o.status FROM users u JOIN orders o ON u.id = o.user_id",
        "Join users and orders fetched from both shards"
    },
    {
        "expression_only",
        "SELECT 1 + 2, UPPER('distributed'), COALESCE(NULL, 'sql'), 42 * 3",
        "Pure expression, no backend needed"
    },
    {
        "subquery",
        "SELECT name, age FROM users WHERE age > (SELECT AVG(age) FROM users)",
        "Subquery: users with above-average age"
    },
};

static constexpr size_t NUM_QUERIES = sizeof(QUERIES) / sizeof(QUERIES[0]);

// ============================================================
// Benchmark runner
// ============================================================

static PipelineTiming run_single_iteration(
    const char* sql,
    InMemoryCatalog& catalog,
    FunctionRegistry<Dialect::MySQL>& functions,
    Optimizer<Dialect::MySQL>& optimizer,
    const ShardMap* shard_map,
    RemoteExecutor* remote_exec)
{
    PipelineTiming t{};
    size_t sql_len = std::strlen(sql);

    auto t0 = Clock::now();

    // 1. Parse
    Parser<Dialect::MySQL> parser;
    auto pr = parser.parse(sql, sql_len);
    auto t1 = Clock::now();

    if (pr.status != ParseResult::OK || !pr.ast) {
        t.success = false;
        t.parse_ns = std::chrono::duration_cast<Duration>(t1 - t0).count();
        t.total_ns = t.parse_ns;
        return t;
    }

    // 2. Build plan
    PlanBuilder<Dialect::MySQL> builder(catalog, parser.arena());
    PlanNode* plan = builder.build(pr.ast);
    auto t2 = Clock::now();

    if (!plan) {
        t.success = false;
        t.parse_ns = std::chrono::duration_cast<Duration>(t1 - t0).count();
        t.plan_ns = std::chrono::duration_cast<Duration>(t2 - t1).count();
        t.total_ns = std::chrono::duration_cast<Duration>(t2 - t0).count();
        return t;
    }

    // 3. Optimize
    plan = optimizer.optimize(plan, parser.arena());
    auto t3 = Clock::now();

    // 4. Distribute
    if (shard_map && remote_exec) {
        DistributedPlanner<Dialect::MySQL> dplanner(*shard_map, catalog, parser.arena(),
                                                     remote_exec, &functions);
        plan = dplanner.distribute(plan);
    }
    auto t4 = Clock::now();

    // 5. Execute
    PlanExecutor<Dialect::MySQL> executor(functions, catalog, parser.arena());
    if (remote_exec)
        executor.set_remote_executor(remote_exec);
    if (shard_map && remote_exec) {
        executor.set_distribute_fn(
            [&](PlanNode* p) -> PlanNode* {
                DistributedPlanner<Dialect::MySQL> dp(*shard_map, catalog, parser.arena(),
                                                       remote_exec, &functions);
                return dp.distribute(p);
            });
    }
    ResultSet rs = executor.execute(plan);
    auto t5 = Clock::now();

    t.parse_ns      = std::chrono::duration_cast<Duration>(t1 - t0).count();
    t.plan_ns        = std::chrono::duration_cast<Duration>(t2 - t1).count();
    t.optimize_ns    = std::chrono::duration_cast<Duration>(t3 - t2).count();
    t.distribute_ns  = std::chrono::duration_cast<Duration>(t4 - t3).count();
    t.execute_ns     = std::chrono::duration_cast<Duration>(t5 - t4).count();
    t.total_ns       = std::chrono::duration_cast<Duration>(t5 - t0).count();
    t.row_count      = rs.row_count();
    t.success        = true;
    return t;
}

// ============================================================
// Main
// ============================================================

static void print_usage(const char* prog) {
    std::cerr << "Usage: " << prog << " [OPTIONS]\n"
              << "\n"
              << "Options:\n"
              << "  --backend URL       Add a backend (mysql://... or pgsql://...)\n"
              << "  --shard SPEC        Add shard config (table:key:shard1,shard2)\n"
              << "  --iterations N      Iterations per query (default: 100)\n"
              << "  --warmup N          Warmup iterations (default: 5)\n"
              << "  --csv               Output CSV format\n"
              << "  --help              Show this help\n";
}

int main(int argc, char* argv[]) {
    std::vector<BackendConfig> backends;
    std::vector<TableShardConfig> shards;
    int iterations = 100;
    int warmup = 5;
    bool csv_mode = false;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--help" || arg == "-h") {
            print_usage(argv[0]);
            return 0;
        } else if (arg == "--backend" && i + 1 < argc) {
            ++i;
            auto pb = parse_backend_url(argv[i]);
            if (!pb.ok) { std::cerr << "Error: " << pb.error << "\n"; return 1; }
            backends.push_back(std::move(pb.config));
        } else if (arg == "--shard" && i + 1 < argc) {
            ++i;
            auto ps = parse_shard_spec(argv[i]);
            if (!ps.ok) { std::cerr << "Error: " << ps.error << "\n"; return 1; }
            shards.push_back(std::move(ps.config));
        } else if (arg == "--iterations" && i + 1 < argc) {
            ++i;
            iterations = std::stoi(argv[i]);
        } else if (arg == "--warmup" && i + 1 < argc) {
            ++i;
            warmup = std::stoi(argv[i]);
        } else if (arg == "--csv") {
            csv_mode = true;
        } else {
            std::cerr << "Unknown option: " << arg << "\n";
            print_usage(argv[0]);
            return 1;
        }
    }

    // Set up catalog
    InMemoryCatalog catalog;

    // Set up shard map
    ShardMap shard_map;
    for (auto& sc : shards) {
        shard_map.add_table(sc);
    }

    // Set up multi-remote executor
    MultiRemoteExecutor* remote_exec = nullptr;
    if (!backends.empty()) {
        remote_exec = new MultiRemoteExecutor();
        for (auto& bc : backends) {
            remote_exec->add_backend(bc);
        }
    }

    // Auto-discover table schemas
    if (remote_exec && !shards.empty()) {
        for (auto& sc : shards) {
            const char* first_backend = sc.shards.empty() ? nullptr : sc.shards[0].backend_name.c_str();
            if (!first_backend) continue;

            std::string show_sql = "SHOW COLUMNS FROM " + sc.table_name;
            StringRef sql_ref{show_sql.c_str(), static_cast<uint32_t>(show_sql.size())};
            try {
                ResultSet cols = remote_exec->execute(first_backend, sql_ref);
                std::vector<ColumnDef> col_defs;
                for (size_t i = 0; i < cols.row_count(); ++i) {
                    const Row& r = cols.rows[i];
                    std::string col_name;
                    if (r.column_count > 0 && !r.get(0).is_null())
                        col_name.assign(r.get(0).str_val.ptr, r.get(0).str_val.len);
                    std::string col_type_str;
                    if (r.column_count > 1 && !r.get(1).is_null())
                        col_type_str.assign(r.get(1).str_val.ptr, r.get(1).str_val.len);
                    SqlType st;
                    if (col_type_str.find("int") != std::string::npos ||
                        col_type_str.find("INT") != std::string::npos)
                        st = SqlType::make_int();
                    else if (col_type_str.find("decimal") != std::string::npos ||
                             col_type_str.find("DECIMAL") != std::string::npos)
                        st = SqlType::make_decimal(10, 2);
                    else if (col_type_str.find("date") != std::string::npos ||
                             col_type_str.find("DATE") != std::string::npos)
                        st = SqlType{SqlType::DATE};
                    else
                        st = SqlType::make_varchar(255);
                    bool nullable = true;
                    if (r.column_count > 2 && !r.get(2).is_null()) {
                        std::string null_str(r.get(2).str_val.ptr, r.get(2).str_val.len);
                        nullable = (null_str == "YES");
                    }
                    col_defs.push_back(ColumnDef{strdup(col_name.c_str()), st, nullable});
                }
                if (!col_defs.empty())
                    catalog.add_table("", sc.table_name.c_str(), col_defs);
            } catch (...) {
                std::cerr << "Warning: schema discovery failed for " << sc.table_name << "\n";
            }
        }
    }

    // Set up function registry and optimizer
    FunctionRegistry<Dialect::MySQL> functions;
    functions.register_builtins();
    Optimizer<Dialect::MySQL> optimizer(catalog, functions);

    const ShardMap* shard_map_ptr = shards.empty() ? nullptr : &shard_map;

    // Print header
    if (!csv_mode) {
        std::cout << "================================================================\n"
                  << "  Distributed SQL Engine - Pipeline Benchmark\n"
                  << "================================================================\n"
                  << "Backends: " << backends.size() << "\n";
        for (auto& b : backends)
            std::cout << "  - " << b.name << " (" << b.host << ":" << b.port << "/" << b.database << ")\n";
        std::cout << "Iterations: " << iterations << " (warmup: " << warmup << ")\n"
                  << "Queries: " << NUM_QUERIES << "\n"
                  << "================================================================\n\n";
    } else {
        std::cout << "query,stage,avg_ns,min_ns,max_ns,p50_ns,p95_ns,stddev_ns,rows\n";
    }

    // Run benchmarks
    for (size_t q = 0; q < NUM_QUERIES; ++q) {
        const QueryDef& qdef = QUERIES[q];

        // Warmup
        for (int w = 0; w < warmup; ++w) {
            run_single_iteration(qdef.sql, catalog, functions, optimizer,
                                 shard_map_ptr, remote_exec);
        }

        // Collect samples
        std::vector<int64_t> parse_samples, plan_samples, opt_samples,
                             dist_samples, exec_samples, total_samples;
        parse_samples.reserve(iterations);
        plan_samples.reserve(iterations);
        opt_samples.reserve(iterations);
        dist_samples.reserve(iterations);
        exec_samples.reserve(iterations);
        total_samples.reserve(iterations);

        size_t row_count = 0;
        int successes = 0;

        for (int i = 0; i < iterations; ++i) {
            auto t = run_single_iteration(qdef.sql, catalog, functions, optimizer,
                                           shard_map_ptr, remote_exec);
            if (t.success) {
                parse_samples.push_back(t.parse_ns);
                plan_samples.push_back(t.plan_ns);
                opt_samples.push_back(t.optimize_ns);
                dist_samples.push_back(t.distribute_ns);
                exec_samples.push_back(t.execute_ns);
                total_samples.push_back(t.total_ns);
                row_count = t.row_count;
                ++successes;
            }
        }

        if (successes == 0) {
            if (!csv_mode)
                std::cout << "QUERY: " << qdef.name << " -- ALL ITERATIONS FAILED\n\n";
            continue;
        }

        auto s_parse = compute_stats(parse_samples);
        auto s_plan  = compute_stats(plan_samples);
        auto s_opt   = compute_stats(opt_samples);
        auto s_dist  = compute_stats(dist_samples);
        auto s_exec  = compute_stats(exec_samples);
        auto s_total = compute_stats(total_samples);

        if (csv_mode) {
            auto print_csv = [&](const char* stage, const Stats& s) {
                std::cout << qdef.name << ","
                          << stage << ","
                          << static_cast<int64_t>(s.avg_ns) << ","
                          << static_cast<int64_t>(s.min_ns) << ","
                          << static_cast<int64_t>(s.max_ns) << ","
                          << static_cast<int64_t>(s.p50_ns) << ","
                          << static_cast<int64_t>(s.p95_ns) << ","
                          << static_cast<int64_t>(s.stddev_ns) << ","
                          << row_count << "\n";
            };
            print_csv("parse", s_parse);
            print_csv("plan", s_plan);
            print_csv("optimize", s_opt);
            print_csv("distribute", s_dist);
            print_csv("execute", s_exec);
            print_csv("total", s_total);
        } else {
            std::cout << "--------------------------------------------------------------\n"
                      << "QUERY: " << qdef.name << "\n"
                      << "  SQL: " << qdef.sql << "\n"
                      << "  " << qdef.description << "\n"
                      << "  Rows: " << row_count << " | Successes: " << successes << "/" << iterations << "\n"
                      << "\n";

            auto print_stage = [](const char* name, const Stats& s) {
                std::cout << "  " << std::left << std::setw(12) << name
                          << "  avg=" << std::right << std::setw(12) << format_ns(s.avg_ns)
                          << "  p50=" << std::setw(12) << format_ns(s.p50_ns)
                          << "  p95=" << std::setw(12) << format_ns(s.p95_ns)
                          << "  min=" << std::setw(12) << format_ns(s.min_ns)
                          << "  max=" << std::setw(12) << format_ns(s.max_ns)
                          << "\n";
            };

            print_stage("parse",      s_parse);
            print_stage("plan",       s_plan);
            print_stage("optimize",   s_opt);
            print_stage("distribute", s_dist);
            print_stage("execute",    s_exec);
            std::cout << "  " << std::string(78, '-') << "\n";
            print_stage("TOTAL",      s_total);
            std::cout << "\n";

            // Show pipeline breakdown as percentages
            double total_avg = s_total.avg_ns;
            if (total_avg > 0) {
                std::cout << "  Pipeline breakdown:\n"
                          << "    parse:      " << std::fixed << std::setprecision(1)
                          << (s_parse.avg_ns / total_avg * 100.0) << "%\n"
                          << "    plan:       " << (s_plan.avg_ns / total_avg * 100.0) << "%\n"
                          << "    optimize:   " << (s_opt.avg_ns / total_avg * 100.0) << "%\n"
                          << "    distribute: " << (s_dist.avg_ns / total_avg * 100.0) << "%\n"
                          << "    execute:    " << (s_exec.avg_ns / total_avg * 100.0) << "%\n";
            }
            std::cout << "\n";
        }
    }

    // Summary
    if (!csv_mode) {
        std::cout << "================================================================\n"
                  << "  Benchmark Complete\n"
                  << "================================================================\n";
    }

    // Cleanup
    if (remote_exec) {
        remote_exec->disconnect_all();
        delete remote_exec;
    }

    return 0;
}
