#ifndef SQL_ENGINE_SHARD_MAP_H
#define SQL_ENGINE_SHARD_MAP_H

#include "sql_parser/common.h"
#include <string>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <functional>

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

    // Determine which shard index a value maps to (hash-based sharding).
    // Returns the shard index in [0, num_shards).
    size_t shard_index_for_int(sql_parser::StringRef table_name, int64_t value) const {
        std::string key = to_lower(std::string(table_name.ptr, table_name.len));
        auto it = tables_.find(key);
        if (it == tables_.end() || it->second.shards.empty()) return 0;
        size_t num_shards = it->second.shards.size();
        // Use std::hash for consistent hash-based routing
        size_t h = std::hash<int64_t>{}(value);
        return h % num_shards;
    }

    // Determine shard index for a string value (hash-based sharding).
    size_t shard_index_for_string(sql_parser::StringRef table_name,
                                   const char* val, uint32_t val_len) const {
        std::string key = to_lower(std::string(table_name.ptr, table_name.len));
        auto it = tables_.find(key);
        if (it == tables_.end() || it->second.shards.empty()) return 0;
        size_t num_shards = it->second.shards.size();
        size_t h = std::hash<std::string>{}(std::string(val, val_len));
        return h % num_shards;
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
