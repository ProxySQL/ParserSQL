#ifndef SQL_ENGINE_SHARD_MAP_H
#define SQL_ENGINE_SHARD_MAP_H

#include "sql_parser/common.h"
#include <string>
#include <vector>
#include <unordered_map>
#include <algorithm>

namespace sql_engine {

struct ShardInfo {
    std::string backend_name;
};

struct TableShardConfig {
    std::string table_name;
    std::string shard_key;          // empty if unsharded
    std::vector<ShardInfo> shards;  // 1 if unsharded, N if sharded
};

class ShardMap {
public:
    void add_table(const TableShardConfig& config) {
        std::string key = to_lower(config.table_name);
        tables_[key] = config;
    }

    bool is_sharded(sql_parser::StringRef table_name) const {
        std::string key = to_lower(std::string(table_name.ptr, table_name.len));
        auto it = tables_.find(key);
        if (it == tables_.end()) return false;
        return !it->second.shard_key.empty() && it->second.shards.size() > 1;
    }

    const std::vector<ShardInfo>& get_shards(sql_parser::StringRef table_name) const {
        std::string key = to_lower(std::string(table_name.ptr, table_name.len));
        auto it = tables_.find(key);
        if (it != tables_.end()) return it->second.shards;
        static const std::vector<ShardInfo> empty;
        return empty;
    }

    sql_parser::StringRef get_shard_key(sql_parser::StringRef table_name) const {
        std::string key = to_lower(std::string(table_name.ptr, table_name.len));
        auto it = tables_.find(key);
        if (it != tables_.end() && !it->second.shard_key.empty()) {
            const std::string& sk = it->second.shard_key;
            return sql_parser::StringRef{sk.c_str(), static_cast<uint32_t>(sk.size())};
        }
        return sql_parser::StringRef{nullptr, 0};
    }

    bool has_table(sql_parser::StringRef table_name) const {
        std::string key = to_lower(std::string(table_name.ptr, table_name.len));
        return tables_.find(key) != tables_.end();
    }

    // Get the single backend for an unsharded table
    const char* get_backend(sql_parser::StringRef table_name) const {
        std::string key = to_lower(std::string(table_name.ptr, table_name.len));
        auto it = tables_.find(key);
        if (it != tables_.end() && !it->second.shards.empty()) {
            return it->second.shards[0].backend_name.c_str();
        }
        return nullptr;
    }

private:
    std::unordered_map<std::string, TableShardConfig> tables_;

    static std::string to_lower(const std::string& s) {
        std::string r = s;
        for (auto& c : r) { if (c >= 'A' && c <= 'Z') c += 32; }
        return r;
    }
};

} // namespace sql_engine

#endif // SQL_ENGINE_SHARD_MAP_H
