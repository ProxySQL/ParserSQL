// engine_stress_test.cpp -- Multi-threaded benchmark calling Session<D> API directly
//
// Each thread gets its own Session, ThreadSafeMultiRemoteExecutor, arena, etc.
// The catalog and shard map are shared (read-only after setup).
//
// Output: JSON compatible with compare.py
//
// Usage:
//   ./engine_stress_test \
//       --backend "mysql://root:test@127.0.0.1:13306/vt_testks?name=shard1" \
//       --backend "mysql://root:test@127.0.0.1:13307/vt_testks?name=shard2" \
//       --shard "users:id:shard1,shard2" \
//       --shard "orders:user_id:shard1,shard2" \
//       --threads 16,64,128,256 --duration 10 --label "ParserSQL_direct"

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
#include <thread>
#include <atomic>
#include <mutex>
#include <random>
#include <functional>

#include "sql_parser/parser.h"
#include "sql_parser/common.h"
#include "sql_engine/session.h"
#include "sql_engine/in_memory_catalog.h"
#include "sql_engine/data_source.h"
#include "sql_engine/local_txn.h"
#include "sql_engine/multi_remote_executor.h"
#include "sql_engine/thread_safe_executor.h"
#include "sql_engine/shard_map.h"
#include "sql_engine/backend_config.h"
#include "sql_engine/tool_config_parser.h"
#include "sql_engine/result_set.h"
#include "sql_engine/dml_result.h"
#include "sql_engine/value.h"
#include "sql_engine/thread_pool.h"

using namespace sql_parser;
using namespace sql_engine;

using Clock = std::chrono::high_resolution_clock;

// ============================================================
// Query definitions (matching Vitess stress_test)
// ============================================================

struct QueryDef {
    const char* name;
    const char* sql;
};

static const QueryDef QUERIES[] = {
    {"point_lookup",  "SELECT * FROM users WHERE id = 42"},
    {"full_scan",     "SELECT * FROM users"},
    {"filter",        "SELECT * FROM users WHERE dept = 'Engineering' AND age > 30"},
    {"aggregation",   "SELECT dept, COUNT(*), AVG(salary), MIN(salary), MAX(salary) FROM users GROUP BY dept"},
    {"sort_limit",    "SELECT name, salary FROM users ORDER BY salary DESC LIMIT 10"},
    {"join",          "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id WHERE o.total > 100"},
    {"subquery",      "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE total > 200)"},
    {"count_star",    "SELECT COUNT(*) FROM users"},
};

static constexpr size_t NUM_QUERIES = sizeof(QUERIES) / sizeof(QUERIES[0]);

// ============================================================
// Per-thread statistics
// ============================================================

struct ThreadStats {
    uint64_t queries = 0;
    uint64_t errors = 0;
    std::vector<double> latencies_us; // microseconds
};

// ============================================================
// Thread worker
// ============================================================

static void worker_thread(
    int thread_id,
    const std::vector<BackendConfig>& backends,
    const InMemoryCatalog& catalog,
    const ShardMap& shard_map,
    bool has_shards,
    const char* query_sql,
    int warmup_sec,
    int duration_sec,
    std::atomic<bool>& start_flag,
    std::atomic<bool>& stop_flag,
    ThreadStats& stats,
    ThreadPool* pool)
{
    (void)thread_id;

    // Each thread gets its own arena, txn manager, executor, and session
    Arena txn_arena{65536, 1048576};
    LocalTransactionManager txn_mgr(txn_arena);

    ThreadSafeMultiRemoteExecutor remote_exec;
    for (auto& bc : backends) {
        remote_exec.add_backend(bc);
    }

    Session<Dialect::MySQL> session(catalog, txn_mgr);
    session.set_remote_executor(&remote_exec);
    session.set_parallel_open(true);  // thread-safe executor enables parallel shard I/O
    if (pool)
        session.set_thread_pool(pool);
    if (has_shards) {
        session.set_shard_map(&shard_map);
    }

    size_t sql_len = std::strlen(query_sql);

    // Wait for the start signal
    while (!start_flag.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }

    // Warmup phase
    auto warmup_end = Clock::now() + std::chrono::seconds(warmup_sec);
    while (Clock::now() < warmup_end && !stop_flag.load(std::memory_order_relaxed)) {
        session.execute_query(query_sql, sql_len);
    }

    // Measurement phase
    auto measure_end = Clock::now() + std::chrono::seconds(duration_sec);
    stats.latencies_us.reserve(100000);

    while (Clock::now() < measure_end && !stop_flag.load(std::memory_order_relaxed)) {
        auto t0 = Clock::now();
        ResultSet rs = session.execute_query(query_sql, sql_len);
        auto t1 = Clock::now();

        double lat_us = std::chrono::duration<double, std::micro>(t1 - t0).count();
        stats.latencies_us.push_back(lat_us);
        stats.queries++;

        if (rs.empty() && rs.column_names.empty()) {
            // Could be a parse/execution error
            stats.errors++;
        }
    }

    remote_exec.disconnect_all();
}

// ============================================================
// Percentile calculation
// ============================================================

static double percentile(std::vector<double>& sorted, double p) {
    if (sorted.empty()) return 0.0;
    double idx = p / 100.0 * static_cast<double>(sorted.size() - 1);
    size_t lo = static_cast<size_t>(idx);
    size_t hi = lo + 1;
    if (hi >= sorted.size()) return sorted.back();
    double frac = idx - static_cast<double>(lo);
    return sorted[lo] * (1.0 - frac) + sorted[hi] * frac;
}

// ============================================================
// Run one benchmark (one query, one thread count)
// ============================================================

struct BenchResult {
    std::string query_name;
    int threads;
    double qps;
    double p50_us;
    double p95_us;
    double p99_us;
    double avg_us;
    uint64_t total_queries;
    uint64_t total_errors;
    double duration_sec;
};

static BenchResult run_benchmark(
    const char* query_name,
    const char* query_sql,
    int num_threads,
    int warmup_sec,
    int duration_sec,
    const std::vector<BackendConfig>& backends,
    const InMemoryCatalog& catalog,
    const ShardMap& shard_map,
    bool has_shards)
{
    std::vector<ThreadStats> all_stats(num_threads);
    std::vector<std::thread> threads;
    threads.reserve(num_threads);

    std::atomic<bool> start_flag{false};
    std::atomic<bool> stop_flag{false};

    // Shared thread pool for parallel shard I/O.
    // Each query dispatches 2 tasks (one per shard). Pool needs enough workers
    // so tasks aren't queued behind each other. num_threads workers means each
    // stress thread gets ~1 pool worker for its parallel shard query.
    size_t pool_size = static_cast<size_t>(std::max(num_threads, 4));
    if (pool_size > 128) pool_size = 128;
    ThreadPool pool(pool_size);

    // Launch threads
    for (int i = 0; i < num_threads; i++) {
        threads.emplace_back(worker_thread,
            i, std::cref(backends), std::cref(catalog), std::cref(shard_map),
            has_shards, query_sql, warmup_sec, duration_sec,
            std::ref(start_flag), std::ref(stop_flag),
            std::ref(all_stats[i]),
            &pool);
    }

    // Signal start
    start_flag.store(true, std::memory_order_release);

    // Wait for warmup + duration
    std::this_thread::sleep_for(std::chrono::seconds(warmup_sec + duration_sec));
    stop_flag.store(true, std::memory_order_release);

    // Join all threads
    for (auto& t : threads) {
        t.join();
    }

    // Aggregate stats
    uint64_t total_queries = 0;
    uint64_t total_errors = 0;
    std::vector<double> all_latencies;

    for (auto& s : all_stats) {
        total_queries += s.queries;
        total_errors += s.errors;
        all_latencies.insert(all_latencies.end(),
            s.latencies_us.begin(), s.latencies_us.end());
    }

    std::sort(all_latencies.begin(), all_latencies.end());

    BenchResult result;
    result.query_name = query_name;
    result.threads = num_threads;
    result.duration_sec = static_cast<double>(duration_sec);
    result.total_queries = total_queries;
    result.total_errors = total_errors;
    result.qps = static_cast<double>(total_queries) / result.duration_sec;
    result.p50_us = percentile(all_latencies, 50.0);
    result.p95_us = percentile(all_latencies, 95.0);
    result.p99_us = percentile(all_latencies, 99.0);

    if (!all_latencies.empty()) {
        double sum = 0;
        for (auto v : all_latencies) sum += v;
        result.avg_us = sum / static_cast<double>(all_latencies.size());
    } else {
        result.avg_us = 0;
    }

    return result;
}

// ============================================================
// JSON output
// ============================================================

static std::string escape_json(const std::string& s) {
    std::string out;
    out.reserve(s.size() + 8);
    for (char c : s) {
        switch (c) {
            case '"': out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            default: out += c; break;
        }
    }
    return out;
}

static void output_json(const std::string& label,
                         const std::vector<BenchResult>& results,
                         const std::vector<int>& thread_counts) {
    std::cout << "{\n";
    std::cout << "  \"label\": \"" << escape_json(label) << "\",\n";
    std::cout << "  \"thread_counts\": [";
    for (size_t i = 0; i < thread_counts.size(); i++) {
        if (i > 0) std::cout << ", ";
        std::cout << thread_counts[i];
    }
    std::cout << "],\n";
    std::cout << "  \"results\": [\n";

    for (size_t i = 0; i < results.size(); i++) {
        auto& r = results[i];
        std::cout << "    {\n";
        std::cout << "      \"query\": \"" << escape_json(r.query_name) << "\",\n";
        std::cout << "      \"threads\": " << r.threads << ",\n";
        std::cout << "      \"qps\": " << std::fixed << std::setprecision(1) << r.qps << ",\n";
        std::cout << "      \"avg_latency_us\": " << std::setprecision(1) << r.avg_us << ",\n";
        std::cout << "      \"p50_latency_us\": " << std::setprecision(1) << r.p50_us << ",\n";
        std::cout << "      \"p95_latency_us\": " << std::setprecision(1) << r.p95_us << ",\n";
        std::cout << "      \"p99_latency_us\": " << std::setprecision(1) << r.p99_us << ",\n";
        std::cout << "      \"total_queries\": " << r.total_queries << ",\n";
        std::cout << "      \"errors\": " << r.total_errors << ",\n";
        std::cout << "      \"duration_sec\": " << std::setprecision(1) << r.duration_sec << "\n";
        std::cout << "    }";
        if (i + 1 < results.size()) std::cout << ",";
        std::cout << "\n";
    }

    std::cout << "  ]\n";
    std::cout << "}\n";
}

// ============================================================
// Auto-discover catalog schemas from backend
// ============================================================

static void discover_schemas(InMemoryCatalog& catalog,
                              MultiRemoteExecutor& remote_exec,
                              const std::vector<TableShardConfig>& shards) {
    for (auto& sc : shards) {
        const char* first_backend = sc.shards.empty() ? nullptr : sc.shards[0].backend_name.c_str();
        if (!first_backend) continue;

        std::string show_sql = "SHOW COLUMNS FROM " + sc.table_name;
        StringRef sql_ref{show_sql.c_str(), static_cast<uint32_t>(show_sql.size())};
        try {
            ResultSet cols = remote_exec.execute(first_backend, sql_ref);
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

// ============================================================
// Load test data via MySQL
// ============================================================

static void load_test_data(MultiRemoteExecutor& remote_exec,
                            const std::vector<TableShardConfig>& shards) {
    // Collect all unique backend names
    std::vector<std::string> all_backends;
    for (auto& sc : shards) {
        for (auto& si : sc.shards) {
            bool found = false;
            for (auto& b : all_backends) {
                if (b == si.backend_name) { found = true; break; }
            }
            if (!found) all_backends.push_back(si.backend_name);
        }
    }
    if (all_backends.empty()) return;

    // Create tables on all backends
    const char* create_users =
        "CREATE TABLE IF NOT EXISTS users ("
        "  id INT PRIMARY KEY,"
        "  name VARCHAR(100),"
        "  dept VARCHAR(50),"
        "  age INT,"
        "  salary DECIMAL(10,2)"
        ")";
    const char* create_orders =
        "CREATE TABLE IF NOT EXISTS orders ("
        "  id INT PRIMARY KEY,"
        "  user_id INT,"
        "  total DECIMAL(10,2),"
        "  status VARCHAR(20)"
        ")";

    for (auto& bn : all_backends) {
        StringRef cu{create_users, static_cast<uint32_t>(std::strlen(create_users))};
        StringRef co{create_orders, static_cast<uint32_t>(std::strlen(create_orders))};
        try { remote_exec.execute_dml(bn.c_str(), cu); } catch (...) {}
        try { remote_exec.execute_dml(bn.c_str(), co); } catch (...) {}
    }

    // Check if data already exists
    const char* count_sql = "SELECT COUNT(*) FROM users";
    StringRef count_ref{count_sql, static_cast<uint32_t>(std::strlen(count_sql))};
    try {
        ResultSet rs = remote_exec.execute(all_backends[0].c_str(), count_ref);
        if (!rs.empty() && rs.rows[0].column_count > 0) {
            int64_t count = rs.rows[0].get(0).to_int64();
            if (count > 0) {
                std::cerr << "Data already exists (" << count << " users in first shard). Skipping load.\n";
                return;
            }
        }
    } catch (...) {}

    // Insert test data
    std::cerr << "Loading test data...\n";
    const char* depts[] = {"Engineering", "Marketing", "Sales", "HR", "Finance"};
    std::mt19937 rng(42);

    for (int i = 1; i <= 1000; i++) {
        std::string dept = depts[rng() % 5];
        int age = 22 + static_cast<int>(rng() % 40);
        int salary = 40000 + static_cast<int>(rng() % 160000);
        std::string name = "user_" + std::to_string(i);

        std::ostringstream sql;
        sql << "INSERT INTO users VALUES (" << i << ", '" << name << "', '"
            << dept << "', " << age << ", " << salary << ")";
        std::string sql_str = sql.str();
        StringRef ref{sql_str.c_str(), static_cast<uint32_t>(sql_str.size())};

        const std::string& bn = all_backends[static_cast<size_t>(i) % all_backends.size()];
        try { remote_exec.execute_dml(bn.c_str(), ref); } catch (...) {}
    }

    for (int i = 1; i <= 2000; i++) {
        int user_id = 1 + static_cast<int>(rng() % 1000);
        int total = 10 + static_cast<int>(rng() % 990);
        const char* statuses[] = {"pending", "shipped", "delivered", "cancelled"};
        std::string status = statuses[rng() % 4];

        std::ostringstream sql;
        sql << "INSERT INTO orders VALUES (" << i << ", " << user_id << ", "
            << total << ", '" << status << "')";
        std::string sql_str = sql.str();
        StringRef ref{sql_str.c_str(), static_cast<uint32_t>(sql_str.size())};

        const std::string& bn = all_backends[static_cast<size_t>(i) % all_backends.size()];
        try { remote_exec.execute_dml(bn.c_str(), ref); } catch (...) {}
    }

    std::cerr << "Loaded 1000 users and 2000 orders.\n";
}

// ============================================================
// Parse thread count list: "16,64,128,256"
// ============================================================

static std::vector<int> parse_thread_list(const std::string& s) {
    std::vector<int> result;
    std::istringstream ss(s);
    std::string token;
    while (std::getline(ss, token, ',')) {
        if (!token.empty()) {
            try { result.push_back(std::stoi(token)); } catch (...) {}
        }
    }
    return result;
}

// ============================================================
// Main
// ============================================================

static void print_usage(const char* prog) {
    std::cerr << "Usage: " << prog << " [OPTIONS]\n"
              << "\n"
              << "Options:\n"
              << "  --backend URL       Add a backend (mysql://...)\n"
              << "  --shard SPEC        Add shard config (table:key:shard1,shard2)\n"
              << "  --threads LIST      Comma-separated thread counts (default: 16,64,128,256)\n"
              << "  --duration SEC      Measurement duration per run (default: 15)\n"
              << "  --warmup SEC        Warmup duration (default: 3)\n"
              << "  --label NAME        Label for JSON output (default: ParserSQL)\n"
              << "  --load-data         Load test data before benchmarking\n"
              << "  --query NAME        Run only this query (default: all)\n"
              << "  --help              Show this help\n";
}

int main(int argc, char* argv[]) {
    std::vector<BackendConfig> backends;
    std::vector<TableShardConfig> shards;
    std::vector<int> thread_counts = {16, 64, 128, 256};
    int duration_sec = 15;
    int warmup_sec = 3;
    std::string label = "ParserSQL";
    bool do_load_data = false;
    std::string only_query;

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
        } else if (arg == "--threads" && i + 1 < argc) {
            ++i;
            thread_counts = parse_thread_list(argv[i]);
        } else if (arg == "--duration" && i + 1 < argc) {
            ++i;
            duration_sec = std::stoi(argv[i]);
        } else if (arg == "--warmup" && i + 1 < argc) {
            ++i;
            warmup_sec = std::stoi(argv[i]);
        } else if (arg == "--label" && i + 1 < argc) {
            ++i;
            label = argv[i];
        } else if (arg == "--load-data") {
            do_load_data = true;
        } else if (arg == "--query" && i + 1 < argc) {
            ++i;
            only_query = argv[i];
        } else {
            std::cerr << "Unknown option: " << arg << "\n";
            print_usage(argv[0]);
            return 1;
        }
    }

    if (backends.empty()) {
        std::cerr << "Error: at least one --backend is required\n";
        return 1;
    }

    // Set up shared catalog (read-only after discovery)
    InMemoryCatalog catalog;

    // Set up shared shard map (read-only after setup)
    ShardMap shard_map;
    for (auto& sc : shards) {
        shard_map.add_table(sc);
    }
    bool has_shards = !shards.empty();

    // Create a temporary executor for schema discovery and data loading
    MultiRemoteExecutor setup_exec;
    for (auto& bc : backends) {
        setup_exec.add_backend(bc);
    }

    // Load data if requested
    if (do_load_data) {
        load_test_data(setup_exec, shards);
    }

    // Auto-discover table schemas
    if (has_shards) {
        discover_schemas(catalog, setup_exec, shards);
    }

    setup_exec.disconnect_all();

    // Print banner to stderr (JSON goes to stdout)
    std::cerr << "================================================================\n"
              << "  ParserSQL Engine Stress Test (Direct API)\n"
              << "================================================================\n"
              << "Backends: " << backends.size() << "\n";
    for (auto& b : backends)
        std::cerr << "  - " << b.name << " (" << b.host << ":" << b.port << "/" << b.database << ")\n";
    std::cerr << "Thread counts: ";
    for (size_t i = 0; i < thread_counts.size(); i++) {
        if (i > 0) std::cerr << ", ";
        std::cerr << thread_counts[i];
    }
    std::cerr << "\n"
              << "Duration: " << duration_sec << "s (warmup: " << warmup_sec << "s)\n"
              << "Label: " << label << "\n"
              << "================================================================\n\n";

    // Run benchmarks
    std::vector<BenchResult> all_results;

    for (size_t q = 0; q < NUM_QUERIES; q++) {
        if (!only_query.empty() && only_query != QUERIES[q].name)
            continue;

        for (int tc : thread_counts) {
            std::cerr << "Running: " << QUERIES[q].name
                      << " with " << tc << " threads... " << std::flush;

            auto result = run_benchmark(
                QUERIES[q].name, QUERIES[q].sql,
                tc, warmup_sec, duration_sec,
                backends, catalog, shard_map, has_shards);

            std::cerr << std::fixed << std::setprecision(0)
                      << result.qps << " QPS, "
                      << std::setprecision(1)
                      << "p50=" << result.p50_us << "us, "
                      << "p99=" << result.p99_us << "us, "
                      << "errors=" << result.total_errors << "\n";

            all_results.push_back(std::move(result));
        }
    }

    // Output JSON to stdout
    output_json(label, all_results, thread_counts);

    return 0;
}
