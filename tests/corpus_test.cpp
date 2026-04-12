#include "sql_parser/parser.h"
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <algorithm>
#include <map>

using namespace sql_parser;

static std::string trim(const std::string& s) {
    size_t start = s.find_first_not_of(" \t\r\n");
    if (start == std::string::npos) return "";
    size_t end = s.find_last_not_of(" \t\r\n");
    return s.substr(start, end - start + 1);
}

static std::string classify_error(const std::string& sql) {
    // Identify the leading keyword(s) for failure categorization
    std::string upper;
    for (size_t i = 0; i < std::min(sql.size(), (size_t)80); i++) {
        char c = sql[i];
        if (c >= 'a' && c <= 'z') c -= 32;
        upper += c;
    }

    // Check for specific syntax patterns
    if (upper.find("WITH ") == 0 || upper.find("WITH\t") == 0) return "WITH/CTE";
    if (upper.find("CREATE ") == 0) return "CREATE";
    if (upper.find("ALTER ") == 0) return "ALTER";
    if (upper.find("DROP ") == 0) return "DROP";
    if (upper.find("GRANT ") == 0 || upper.find("REVOKE ") == 0) return "GRANT/REVOKE";
    if (upper.find("SELECT ") == 0 || upper.find("SELECT\t") == 0 || upper.find("(SELECT") == 0) return "SELECT";
    if (upper.find("INSERT ") == 0) return "INSERT";
    if (upper.find("UPDATE ") == 0) return "UPDATE";
    if (upper.find("DELETE ") == 0) return "DELETE";
    if (upper.find("SET ") == 0) return "SET";
    if (upper.find("SHOW ") == 0) return "SHOW";
    if (upper.find("BEGIN") == 0 || upper.find("START ") == 0) return "TRANSACTION";
    if (upper.find("COMMIT") == 0 || upper.find("ROLLBACK") == 0) return "TRANSACTION";
    if (upper.find("EXPLAIN ") == 0 || upper.find("DESCRIBE ") == 0) return "EXPLAIN";
    if (upper.find("TRUNCATE ") == 0) return "TRUNCATE";
    if (upper.find("LOCK ") == 0 || upper.find("UNLOCK ") == 0) return "LOCK";
    if (upper.find("PREPARE ") == 0 || upper.find("EXECUTE ") == 0 || upper.find("DEALLOCATE ") == 0) return "PREPARED_STMT";
    if (upper.find("USE ") == 0) return "USE";
    if (upper.find("LOAD ") == 0) return "LOAD";
    if (upper.find("CALL ") == 0) return "CALL";
    if (upper.find("DO ") == 0) return "DO";
    if (upper.find("REPLACE ") == 0) return "REPLACE";
    if (upper.find("MERGE ") == 0) return "MERGE";
    if (upper.find("COPY ") == 0) return "COPY";
    if (upper.find("VACUUM ") == 0 || upper.find("ANALYZE ") == 0 || upper.find("ANALYSE ") == 0) return "MAINTENANCE";
    if (upper.find("COMMENT ") == 0) return "COMMENT";
    if (upper.find("IMPORT ") == 0 || upper.find("EXPORT ") == 0) return "IMPORT/EXPORT";
    if (upper.find("REINDEX") == 0) return "REINDEX";
    if (upper.find("CLUSTER") == 0) return "CLUSTER";
    if (upper.find("REFRESH") == 0) return "REFRESH";
    if (upper.find("DISCARD") == 0) return "DISCARD";
    if (upper.find("REASSIGN") == 0) return "REASSIGN";
    if (upper.find("SECURITY") == 0) return "SECURITY";
    if (upper.find("VALUES") == 0) return "VALUES_STMT";
    if (upper.find("TABLE ") == 0) return "TABLE_STMT";
    if (upper.find("FETCH ") == 0 || upper.find("MOVE ") == 0) return "CURSOR";
    if (upper.find("DECLARE ") == 0) return "CURSOR";
    if (upper.find("CLOSE ") == 0) return "CURSOR";
    if (upper.find("LISTEN") == 0 || upper.find("NOTIFY") == 0 || upper.find("UNLISTEN") == 0) return "LISTEN/NOTIFY";
    if (upper.find("RELEASE ") == 0 || upper.find("SAVEPOINT ") == 0) return "SAVEPOINT";
    if (upper.find("RESET ") == 0) return "RESET";
    if (upper.find("ABORT") == 0) return "ABORT";
    if (upper.find("END") == 0) return "END";

    return "OTHER(" + upper.substr(0, std::min(upper.size(), (size_t)20)) + ")";
}

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "Usage: corpus_test <mysql|pgsql> [file...]\n";
        std::cerr << "  Reads SQL queries (one per line) from files or stdin.\n";
        std::cerr << "  Lines starting with -- or # are skipped.\n";
        return 1;
    }

    bool is_mysql = std::string(argv[1]) == "mysql";

    std::vector<std::istream*> inputs;
    std::vector<std::ifstream*> owned;
    if (argc > 2) {
        for (int i = 2; i < argc; i++) {
            auto* f = new std::ifstream(argv[i]);
            if (!f->is_open()) {
                std::cerr << "Cannot open: " << argv[i] << "\n";
                delete f;
                continue;
            }
            inputs.push_back(f);
            owned.push_back(f);
        }
    } else {
        inputs.push_back(&std::cin);
    }

    int total = 0, ok = 0, partial = 0, errors = 0;
    std::map<std::string, int> error_categories;
    std::map<std::string, int> partial_categories;
    std::vector<std::pair<std::string, std::string>> error_examples;   // (sql, error_info)
    std::vector<std::pair<std::string, std::string>> partial_examples; // (sql, category)

    auto run_mysql = [&](const std::string& sql) {
        Parser<Dialect::MySQL> parser;
        return parser.parse(sql.c_str(), sql.size());
    };
    auto run_pgsql = [&](const std::string& sql) {
        Parser<Dialect::PostgreSQL> parser;
        return parser.parse(sql.c_str(), sql.size());
    };

    for (auto* in : inputs) {
        std::string line;
        while (std::getline(*in, line)) {
            std::string sql = trim(line);
            if (sql.empty()) continue;
            if (sql[0] == '#') continue;
            if (sql.size() >= 2 && sql[0] == '-' && sql[1] == '-') continue;

            // Strip trailing semicolons (our parser handles them, but some test files have extra)
            while (!sql.empty() && sql.back() == ';') sql.pop_back();
            sql = trim(sql);
            if (sql.empty()) continue;

            total++;

            ParseResult result = is_mysql ? run_mysql(sql) : run_pgsql(sql);

            switch (result.status) {
            case ParseResult::OK:
                ok++;
                break;
            case ParseResult::PARTIAL: {
                partial++;
                std::string cat = classify_error(sql);
                partial_categories[cat]++;
                if (partial_examples.size() < 50) {
                    partial_examples.push_back({sql, cat});
                }
                break;
            }
            case ParseResult::ERROR: {
                errors++;
                std::string cat = classify_error(sql);
                error_categories[cat]++;
                if (error_examples.size() < 50) {
                    std::string info = "offset=" + std::to_string(result.error.offset);
                    if (result.error.message.ptr && result.error.message.len > 0) {
                        info += " msg=" + std::string(result.error.message.ptr, result.error.message.len);
                    }
                    error_examples.push_back({sql, info});
                }
                break;
            }
            }
        }
    }

    // Report
    std::cout << "\n=== Corpus Test Results ===\n";
    std::cout << "Dialect: " << (is_mysql ? "MySQL" : "PostgreSQL") << "\n";
    std::cout << "Total queries: " << total << "\n";
    std::cout << "OK:      " << ok << " (" << (total > 0 ? 100.0 * ok / total : 0) << "%)\n";
    std::cout << "PARTIAL: " << partial << " (" << (total > 0 ? 100.0 * partial / total : 0) << "%)\n";
    std::cout << "ERROR:   " << errors << " (" << (total > 0 ? 100.0 * errors / total : 0) << "%)\n";
    std::cout << "Success rate (OK+PARTIAL): " << (total > 0 ? 100.0 * (ok + partial) / total : 0) << "%\n";

    if (!error_categories.empty()) {
        std::cout << "\n--- ERROR categories ---\n";
        // Sort by count descending
        std::vector<std::pair<std::string, int>> sorted(error_categories.begin(), error_categories.end());
        std::sort(sorted.begin(), sorted.end(), [](auto& a, auto& b) { return a.second > b.second; });
        for (auto& [cat, cnt] : sorted) {
            std::cout << "  " << cat << ": " << cnt << "\n";
        }
    }

    if (!partial_categories.empty()) {
        std::cout << "\n--- PARTIAL categories ---\n";
        std::vector<std::pair<std::string, int>> sorted(partial_categories.begin(), partial_categories.end());
        std::sort(sorted.begin(), sorted.end(), [](auto& a, auto& b) { return a.second > b.second; });
        for (auto& [cat, cnt] : sorted) {
            std::cout << "  " << cat << ": " << cnt << "\n";
        }
    }

    if (!error_examples.empty()) {
        std::cout << "\n--- Top ERROR examples (up to 10) ---\n";
        for (size_t i = 0; i < std::min(error_examples.size(), (size_t)10); i++) {
            std::string display_sql = error_examples[i].first;
            if (display_sql.size() > 120) display_sql = display_sql.substr(0, 120) + "...";
            std::cout << "  [" << (i+1) << "] " << error_examples[i].second << "\n";
            std::cout << "      SQL: " << display_sql << "\n";
        }
    }

    if (!partial_examples.empty()) {
        std::cout << "\n--- Top PARTIAL examples (up to 10) ---\n";
        for (size_t i = 0; i < std::min(partial_examples.size(), (size_t)10); i++) {
            std::string display_sql = partial_examples[i].first;
            if (display_sql.size() > 120) display_sql = display_sql.substr(0, 120) + "...";
            std::cout << "  [" << (i+1) << "] category=" << partial_examples[i].second << "\n";
            std::cout << "      SQL: " << display_sql << "\n";
        }
    }

    for (auto* f : owned) delete f;
    return 0;
}
