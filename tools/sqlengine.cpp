// sqlengine.cpp — Interactive SQL engine CLI tool
//
// Usage:
//   ./sqlengine                                       # In-memory mode
//   ./sqlengine --backend "mysql://user:pass@host:port/db?name=shard1"
//   ./sqlengine --backend "..." --shard "table:key:shard1,shard2"
//   echo "SELECT 1 + 2" | ./sqlengine                # Piped mode
//
// In-memory mode evaluates expressions without any backend.
// Backend mode connects to real MySQL/PostgreSQL databases.

#include <iostream>
#include <string>
#include <vector>
#include <sstream>
#include <chrono>
#include <algorithm>
#include <cstring>
#include <iomanip>
#include <unistd.h>

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
#include "sql_engine/datetime_parse.h"
#include "sql_engine/dml_result.h"
#include "sql_engine/value.h"

using namespace sql_parser;
using namespace sql_engine;

// ============================================================
// Value to string conversion
// ============================================================

static std::string value_to_string(const Value& v) {
    switch (v.tag) {
        case Value::TAG_NULL:
            return "NULL";
        case Value::TAG_BOOL:
            return v.bool_val ? "1" : "0";
        case Value::TAG_INT64:
            return std::to_string(v.int_val);
        case Value::TAG_UINT64:
            return std::to_string(v.uint_val);
        case Value::TAG_DOUBLE: {
            std::ostringstream oss;
            oss << v.double_val;
            return oss.str();
        }
        case Value::TAG_STRING:
        case Value::TAG_DECIMAL:
        case Value::TAG_JSON:
            if (v.str_val.ptr && v.str_val.len > 0)
                return std::string(v.str_val.ptr, v.str_val.len);
            return "";
        case Value::TAG_BYTES:
            if (v.str_val.ptr && v.str_val.len > 0)
                return std::string(v.str_val.ptr, v.str_val.len);
            return "(binary)";
        case Value::TAG_DATE: {
            char buf[16];
            size_t n = datetime_parse::format_date(v.date_val, buf, sizeof(buf));
            return std::string(buf, n);
        }
        case Value::TAG_TIME: {
            char buf[32];
            size_t n = datetime_parse::format_time(v.time_val, buf, sizeof(buf));
            return std::string(buf, n);
        }
        case Value::TAG_DATETIME: {
            char buf[32];
            size_t n = datetime_parse::format_datetime(v.datetime_val, buf, sizeof(buf));
            return std::string(buf, n);
        }
        case Value::TAG_TIMESTAMP: {
            char buf[32];
            size_t n = datetime_parse::format_datetime(v.timestamp_val, buf, sizeof(buf));
            return std::string(buf, n);
        }
        default:
            return "?";
    }
}

// ============================================================
// MySQL-style table output
// ============================================================

static void print_result_table(const ResultSet& rs, double elapsed_sec) {
    if (rs.empty() && rs.column_names.empty()) {
        std::cout << "Empty set";
        std::cout << " (" << std::fixed << std::setprecision(3)
                  << elapsed_sec << " sec)" << std::endl;
        return;
    }

    uint16_t ncols = rs.column_count;
    if (ncols == 0) ncols = static_cast<uint16_t>(rs.column_names.size());
    if (ncols == 0 && !rs.rows.empty()) ncols = rs.rows[0].column_count;
    if (ncols == 0) {
        std::cout << "Empty set (" << std::fixed << std::setprecision(3)
                  << elapsed_sec << " sec)" << std::endl;
        return;
    }

    // Build string grid
    std::vector<std::vector<std::string>> grid;
    grid.reserve(rs.rows.size());
    for (auto& row : rs.rows) {
        std::vector<std::string> cells;
        cells.reserve(ncols);
        for (uint16_t c = 0; c < ncols; ++c) {
            if (c < row.column_count)
                cells.push_back(value_to_string(row.get(c)));
            else
                cells.push_back("NULL");
        }
        grid.push_back(std::move(cells));
    }

    // Calculate column widths
    std::vector<size_t> widths(ncols, 0);
    for (uint16_t c = 0; c < ncols; ++c) {
        if (c < rs.column_names.size())
            widths[c] = rs.column_names[c].size();
        else
            widths[c] = 3; // "???"
    }
    for (auto& row_cells : grid) {
        for (uint16_t c = 0; c < ncols; ++c) {
            if (c < row_cells.size())
                widths[c] = std::max(widths[c], row_cells[c].size());
        }
    }

    // Print separator
    auto print_sep = [&]() {
        std::cout << "+";
        for (uint16_t c = 0; c < ncols; ++c) {
            std::cout << "-";
            for (size_t i = 0; i < widths[c]; ++i) std::cout << "-";
            std::cout << "-+";
        }
        std::cout << "\n";
    };

    // Print header
    print_sep();
    std::cout << "|";
    for (uint16_t c = 0; c < ncols; ++c) {
        std::string name;
        if (c < rs.column_names.size())
            name = rs.column_names[c];
        else
            name = "???";
        std::cout << " " << std::left << std::setw(static_cast<int>(widths[c])) << name << " |";
    }
    std::cout << "\n";
    print_sep();

    // Print rows
    for (auto& row_cells : grid) {
        std::cout << "|";
        for (uint16_t c = 0; c < ncols; ++c) {
            std::string val;
            if (c < row_cells.size())
                val = row_cells[c];
            else
                val = "NULL";
            std::cout << " " << std::right << std::setw(static_cast<int>(widths[c])) << val << " |";
        }
        std::cout << "\n";
    }
    print_sep();

    // Row count
    std::cout << rs.rows.size() << " row" << (rs.rows.size() != 1 ? "s" : "")
              << " in set (" << std::fixed << std::setprecision(3)
              << elapsed_sec << " sec)" << std::endl;
}

static void print_dml_result(const DmlResult& dr, double elapsed_sec) {
    if (dr.success) {
        std::cout << "Query OK, " << dr.affected_rows << " row"
                  << (dr.affected_rows != 1 ? "s" : "") << " affected ("
                  << std::fixed << std::setprecision(3) << elapsed_sec
                  << " sec)" << std::endl;
    } else {
        std::cout << "ERROR: " << dr.error_message << std::endl;
    }
}

// ============================================================
// Determine if a SQL statement is a SELECT/expression query
// ============================================================

static bool is_query_statement(StmtType st) {
    switch (st) {
        case StmtType::SELECT:
        case StmtType::SHOW:
        case StmtType::DESCRIBE:
        case StmtType::EXPLAIN:
            return true;
        default:
            return false;
    }
}

// ============================================================
// Main
// ============================================================

static void print_usage(const char* prog) {
    std::cerr << "Usage: " << prog << " [OPTIONS]\n"
              << "\n"
              << "Options:\n"
              << "  --backend URL    Add a backend (mysql://... or pgsql://...)\n"
              << "  --shard SPEC     Add shard config (table:key:shard1,shard2)\n"
              << "  --help           Show this help\n"
              << "\n"
              << "In-memory mode (no --backend): evaluates expressions locally.\n"
              << "Reads SQL from stdin. One statement per line.\n"
              << "\n"
              << "Examples:\n"
              << "  echo \"SELECT 1 + 2, UPPER('hello')\" | " << prog << "\n"
              << "  " << prog << " --backend \"mysql://root:test@127.0.0.1:13306/testdb?name=shard1\"\n";
}

int main(int argc, char* argv[]) {
    std::vector<BackendConfig> backends;
    std::vector<TableShardConfig> shards;

    // Parse command-line args
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--help" || arg == "-h") {
            print_usage(argv[0]);
            return 0;
        } else if (arg == "--backend" && i + 1 < argc) {
            ++i;
            auto pb = parse_backend_url(argv[i]);
            if (!pb.ok) {
                std::cerr << "Error: " << pb.error << std::endl;
                return 1;
            }
            backends.push_back(std::move(pb.config));
        } else if (arg == "--shard" && i + 1 < argc) {
            ++i;
            auto ps = parse_shard_spec(argv[i]);
            if (!ps.ok) {
                std::cerr << "Error: " << ps.error << std::endl;
                return 1;
            }
            shards.push_back(std::move(ps.config));
        } else {
            std::cerr << "Unknown option: " << arg << std::endl;
            print_usage(argv[0]);
            return 1;
        }
    }

    // Set up catalog
    InMemoryCatalog catalog;

    // Set up arena for transaction manager
    Arena txn_arena{65536, 1048576};
    LocalTransactionManager txn_mgr(txn_arena);

    // Set up shard map
    ShardMap shard_map;
    for (auto& sc : shards) {
        shard_map.add_table(sc);
    }

    // Set up multi-remote executor (for backend mode)
    ThreadSafeMultiRemoteExecutor* remote_exec = nullptr;
    if (!backends.empty()) {
        remote_exec = new ThreadSafeMultiRemoteExecutor();
        for (auto& bc : backends) {
            remote_exec->add_backend(bc);
        }
    }

    // Auto-discover table schemas from the first backend for each sharded table
    if (remote_exec && !shards.empty()) {
        for (auto& sc : shards) {
            const char* first_backend = sc.shards.empty() ? nullptr : sc.shards[0].backend_name.c_str();
            if (!first_backend) continue;

            // Query SHOW COLUMNS FROM table
            std::string show_sql = "SHOW COLUMNS FROM " + sc.table_name;
            sql_parser::StringRef sql_ref{show_sql.c_str(), static_cast<uint32_t>(show_sql.size())};
            try {
                ResultSet cols = remote_exec->execute(first_backend, sql_ref);
                std::vector<ColumnDef> col_defs;
                for (size_t i = 0; i < cols.row_count(); ++i) {
                    const Row& r = cols.rows[i];
                    std::string col_name;
                    if (r.column_count > 0 && !r.get(0).is_null()) {
                        col_name.assign(r.get(0).str_val.ptr, r.get(0).str_val.len);
                    }
                    std::string col_type_str;
                    if (r.column_count > 1 && !r.get(1).is_null()) {
                        col_type_str.assign(r.get(1).str_val.ptr, r.get(1).str_val.len);
                    }
                    // Map MySQL type string to SqlType
                    SqlType st;
                    if (col_type_str.find("int") != std::string::npos ||
                        col_type_str.find("INT") != std::string::npos) {
                        st = SqlType::make_int();
                    } else if (col_type_str.find("decimal") != std::string::npos ||
                               col_type_str.find("DECIMAL") != std::string::npos) {
                        st = SqlType::make_decimal(10, 2);
                    } else if (col_type_str.find("date") != std::string::npos ||
                               col_type_str.find("DATE") != std::string::npos) {
                        st = SqlType{SqlType::DATE};
                    } else {
                        st = SqlType::make_varchar(255);
                    }
                    bool nullable = true;
                    if (r.column_count > 2 && !r.get(2).is_null()) {
                        std::string null_str(r.get(2).str_val.ptr, r.get(2).str_val.len);
                        nullable = (null_str == "YES");
                    }
                    // Store column name persistently
                    col_defs.push_back(ColumnDef{strdup(col_name.c_str()), st, nullable});
                }
                if (!col_defs.empty()) {
                    catalog.add_table("", sc.table_name.c_str(), col_defs);
                }
            } catch (...) {
                // Schema discovery failed — continue without catalog entry
            }
        }
    }

    // Create session
    Session<Dialect::MySQL> session(catalog, txn_mgr);
    if (remote_exec) {
        session.set_remote_executor(remote_exec);
        session.set_parallel_open(true);  // thread-safe executor enables parallel shard I/O
    }
    if (!shards.empty()) {
        session.set_shard_map(&shard_map);
    }

    // Detect interactive mode
    bool interactive = isatty(fileno(stdin));
    if (interactive) {
        std::cout << "ParserSQL Engine v1.0 — Interactive SQL Shell\n";
        if (backends.empty()) {
            std::cout << "Mode: in-memory (no backends)\n";
        } else {
            std::cout << "Mode: " << backends.size() << " backend"
                      << (backends.size() > 1 ? "s" : "") << " connected\n";
            for (auto& b : backends) {
                std::cout << "  - " << b.name << " (" << b.host << ":"
                          << b.port << "/" << b.database << ")\n";
            }
        }
        std::cout << "Type SQL statements (one per line). Ctrl+D to exit.\n\n";
    }

    std::string line;
    while (true) {
        if (interactive) {
            std::cout << "sql> " << std::flush;
        }
        if (!std::getline(std::cin, line)) break;

        // Strip leading/trailing whitespace
        size_t start = line.find_first_not_of(" \t\r\n");
        if (start == std::string::npos) continue;
        size_t end = line.find_last_not_of(" \t\r\n;");
        if (end == std::string::npos) continue;
        std::string sql = line.substr(start, end - start + 1);
        if (sql.empty()) continue;

        // Skip comments
        if (sql.size() >= 2 && sql[0] == '-' && sql[1] == '-') continue;
        if (sql.size() >= 2 && sql[0] == '/' && sql[1] == '*') continue;

        // Quit commands
        if (sql == "quit" || sql == "exit" || sql == "\\q") break;

        // Classify the statement to decide query vs DML
        Parser<Dialect::MySQL> classifier;
        auto pr = classifier.parse(sql.c_str(), sql.size());

        auto t_start = std::chrono::high_resolution_clock::now();

        if (pr.status == ParseResult::ERROR) {
            auto t_end = std::chrono::high_resolution_clock::now();
            double elapsed = std::chrono::duration<double>(t_end - t_start).count();
            std::cout << "ERROR: parse error";
            if (pr.error.message.ptr && pr.error.message.len > 0)
                std::cout << " — " << std::string(pr.error.message.ptr, pr.error.message.len);
            std::cout << " (" << std::fixed << std::setprecision(3)
                      << elapsed << " sec)" << std::endl;
            continue;
        }

        if (is_query_statement(pr.stmt_type)) {
            // SELECT / SHOW / DESCRIBE / EXPLAIN — execute as query
            ResultSet rs = session.execute_query(sql.c_str(), sql.size());
            auto t_end = std::chrono::high_resolution_clock::now();
            double elapsed = std::chrono::duration<double>(t_end - t_start).count();
            print_result_table(rs, elapsed);
        } else {
            // DML or transaction control
            DmlResult dr = session.execute_statement(sql.c_str(), sql.size());
            auto t_end = std::chrono::high_resolution_clock::now();
            double elapsed = std::chrono::duration<double>(t_end - t_start).count();
            print_dml_result(dr, elapsed);
        }
    }

    // Cleanup
    if (remote_exec) {
        remote_exec->disconnect_all();
        delete remote_exec;
    }

    return 0;
}
