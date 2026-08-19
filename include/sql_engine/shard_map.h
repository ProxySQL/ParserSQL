#ifndef SQL_ENGINE_SHARD_MAP_H
#define SQL_ENGINE_SHARD_MAP_H

#include "sql_parser/common.h"
#include <cstdint>
#include <climits>
#include <cstring>
#include <string>
#include <vector>
#include <unordered_map>
#include <algorithm>

namespace sql_engine {

struct ShardInfo {
    std::string backend_name;
};

// Routing strategy for sharded tables. The default (HASH) preserves the
// historical behaviour, but with a portable hash function — the original
// implementation used std::hash<int64_t> which on libstdc++ is the
// identity function and produces routes equivalent to value % num_shards.
//
// RANGE and LIST exist to make demos and real ProxySQL-shaped deployments
// explicit. ProxySQL itself routes via mysql_query_rules; a future
// strategy will delegate to that table, fitting in alongside these three.
enum class RoutingStrategy : uint8_t {
    HASH,    // FNV-1a hash of the key value, modulo num_shards
    RANGE,   // ordered (upper_inclusive, shard_index) — first match wins
    LIST,    // explicit value -> shard_index map
};

// Used by RANGE strategy. Entries must be sorted by upper_inclusive
// ascending. Routing picks the first entry whose upper_inclusive >= value.
// Values exceeding the largest upper_inclusive route to the last entry's
// shard_index (matches the MySQL PARTITION BY RANGE ... LESS THAN MAXVALUE
// idiom). Today this strategy is integer-keyed only.
struct ShardRange {
    int64_t upper_inclusive = 0;
    size_t shard_index = 0;
};

// Used by LIST strategy. Each entry maps a single key value to a shard
// index. Key may be int or string, but a single TableShardConfig must
// stay one or the other (mixed lists are not supported). Lookups that
// miss every entry fall back to shard 0.
struct ShardListEntry {
    bool is_int = true;
    int64_t int_val = 0;
    std::string str_val;
    size_t shard_index = 0;
};

// One component of a (possibly composite) shard key. n==1 reuses the
// single-column HASH/RANGE/LIST path so existing routes stay identical.
struct ShardKeyPart {
    bool is_int = true;
    int64_t int_val = 0;
    const char* str = nullptr;
    uint32_t str_len = 0;
};

struct TableShardConfig {
    std::string table_name;
    std::string shard_key;          // empty if unsharded; "a+b" for composite
    std::vector<ShardInfo> shards;  // 1 if unsharded, N if sharded
    RoutingStrategy strategy = RoutingStrategy::HASH;
    std::vector<ShardRange> ranges;       // used iff strategy == RANGE
    std::vector<ShardListEntry> list;     // used iff strategy == LIST
    std::vector<std::string> shard_keys;  // filled by ShardMap::add_table
};

class ShardMap {
public:
    void add_table(const TableShardConfig& config) {
        TableShardConfig copy = config;
        if (copy.shard_keys.empty() && !copy.shard_key.empty()) {
            split_keys(copy.shard_key, copy.shard_keys);
        } else if (copy.shard_key.empty() && !copy.shard_keys.empty()) {
            copy.shard_key = join_keys(copy.shard_keys);
        }
        if (copy.strategy == RoutingStrategy::RANGE) {
            std::sort(copy.ranges.begin(), copy.ranges.end(),
                      [](const ShardRange& a, const ShardRange& b) {
                          return a.upper_inclusive < b.upper_inclusive;
                      });
        }
        std::string key = to_lower(copy.table_name);
        tables_[key] = std::move(copy);
    }

    bool is_sharded(sql_parser::StringRef table_name) const {
        const TableShardConfig* cfg = lookup(table_name);
        if (!cfg) return false;
        return !cfg->shard_keys.empty() && cfg->shards.size() > 1;
    }

    const std::vector<ShardInfo>& get_shards(sql_parser::StringRef table_name) const {
        const TableShardConfig* cfg = lookup(table_name);
        if (cfg) return cfg->shards;
        static const std::vector<ShardInfo> empty;
        return empty;
    }

    sql_parser::StringRef get_shard_key(sql_parser::StringRef table_name) const {
        const TableShardConfig* cfg = lookup(table_name);
        if (cfg && !cfg->shard_keys.empty()) {
            const std::string& sk = cfg->shard_keys[0];
            return sql_parser::StringRef{sk.c_str(), static_cast<uint32_t>(sk.size())};
        }
        return sql_parser::StringRef{nullptr, 0};
    }

    const std::vector<std::string>& get_shard_keys(sql_parser::StringRef table_name) const {
        const TableShardConfig* cfg = lookup(table_name);
        if (cfg) return cfg->shard_keys;
        static const std::vector<std::string> empty;
        return empty;
    }

    bool has_table(sql_parser::StringRef table_name) const {
        return lookup(table_name) != nullptr;
    }

    // False if the table is unknown, has no shards, or a LIST value is unmapped.
    bool try_shard_index_for_int(sql_parser::StringRef table_name, int64_t value,
                                 size_t& out) const {
        const TableShardConfig* cfg = lookup(table_name);
        if (!cfg || cfg->shards.empty()) return false;
        size_t n = cfg->shards.size();
        switch (cfg->strategy) {
            case RoutingStrategy::HASH:
                out = fnv1a_int64(value) % n;
                return true;
            case RoutingStrategy::RANGE:
                out = shard_index_for_int(table_name, value);
                return true;
            case RoutingStrategy::LIST:
                for (const auto& e : cfg->list) {
                    if (e.is_int && e.int_val == value) {
                        out = clamp_index(e.shard_index, n);
                        return true;
                    }
                }
                return false;
        }
        return false;
    }

    bool try_shard_index_for_string(sql_parser::StringRef table_name,
                                    const char* val, uint32_t val_len,
                                    size_t& out) const {
        const TableShardConfig* cfg = lookup(table_name);
        if (!cfg || cfg->shards.empty()) return false;
        size_t n = cfg->shards.size();
        switch (cfg->strategy) {
            case RoutingStrategy::HASH:
                out = fnv1a_bytes(reinterpret_cast<const uint8_t*>(val), val_len) % n;
                return true;
            case RoutingStrategy::RANGE:
                return false;
            case RoutingStrategy::LIST:
                for (const auto& e : cfg->list) {
                    if (!e.is_int && e.str_val.size() == val_len &&
                        std::memcmp(e.str_val.data(), val, val_len) == 0) {
                        out = clamp_index(e.shard_index, n);
                        return true;
                    }
                }
                return false;
        }
        return false;
    }

    bool try_shard_index_for_parts(sql_parser::StringRef table_name,
                                   const ShardKeyPart* parts, size_t n,
                                   size_t& out) const {
        const TableShardConfig* cfg = lookup(table_name);
        if (!cfg || cfg->shards.empty() || !parts || n == 0) return false;
        if (n == 1) {
            return parts[0].is_int
                ? try_shard_index_for_int(table_name, parts[0].int_val, out)
                : try_shard_index_for_string(table_name, parts[0].str,
                                             parts[0].str_len, out);
        }
        if (cfg->strategy != RoutingStrategy::HASH) return false;
        uint64_t h = 0xcbf29ce484222325ULL;
        for (size_t i = 0; i < n; ++i) {
            if (parts[i].is_int) {
                uint64_t u = static_cast<uint64_t>(parts[i].int_val);
                uint8_t bytes[8];
                for (int b = 0; b < 8; ++b)
                    bytes[b] = static_cast<uint8_t>((u >> (b * 8)) & 0xff);
                h = fnv1a_mix(h, bytes, 8);
            } else {
                h = fnv1a_mix(h,
                              reinterpret_cast<const uint8_t*>(parts[i].str),
                              parts[i].str_len);
            }
            uint8_t sep = 0xff;
            h = fnv1a_mix(h, &sep, 1);
        }
        out = static_cast<size_t>(h % cfg->shards.size());
        return true;
    }

    // Determine which shard index a value maps to. Dispatches on the
    // configured RoutingStrategy. Returns 0 if the table is unknown or
    // has no shards — prefer try_shard_index_for_* which fails closed.
    size_t shard_index_for_int(sql_parser::StringRef table_name, int64_t value) const {
        const TableShardConfig* cfg = lookup(table_name);
        if (!cfg || cfg->shards.empty()) return 0;
        size_t n = cfg->shards.size();
        switch (cfg->strategy) {
            case RoutingStrategy::HASH:
                return fnv1a_int64(value) % n;
            case RoutingStrategy::RANGE: {
                if (cfg->ranges.empty()) return 0;
                for (const auto& r : cfg->ranges) {
                    if (value <= r.upper_inclusive) {
                        return clamp_index(r.shard_index, n);
                    }
                }
                // Above all upper bounds: fall through to last shard.
                return clamp_index(cfg->ranges.back().shard_index, n);
            }
            case RoutingStrategy::LIST:
                for (const auto& e : cfg->list) {
                    if (e.is_int && e.int_val == value) {
                        return clamp_index(e.shard_index, n);
                    }
                }
                return 0;
        }
        return 0;
    }

    size_t shard_index_for_string(sql_parser::StringRef table_name,
                                   const char* val, uint32_t val_len) const {
        const TableShardConfig* cfg = lookup(table_name);
        if (!cfg || cfg->shards.empty()) return 0;
        size_t n = cfg->shards.size();
        switch (cfg->strategy) {
            case RoutingStrategy::HASH:
                return fnv1a_bytes(reinterpret_cast<const uint8_t*>(val), val_len) % n;
            case RoutingStrategy::RANGE:
                // RANGE is integer-keyed only. Fall back to scatter-friendly
                // shard 0 rather than producing a misleading single-shard
                // route from a string key.
                return 0;
            case RoutingStrategy::LIST:
                for (const auto& e : cfg->list) {
                    if (!e.is_int && e.str_val.size() == val_len &&
                        std::memcmp(e.str_val.data(), val, val_len) == 0) {
                        return clamp_index(e.shard_index, n);
                    }
                }
                return 0;
        }
        return 0;
    }

    RoutingStrategy routing_strategy(sql_parser::StringRef table_name) const {
        const TableShardConfig* cfg = lookup(table_name);
        return cfg ? cfg->strategy : RoutingStrategy::HASH;
    }

    // LIST only. Inclusive [lo, hi]. Pushes every mapped int in the window.
    void collect_int_list_shards(sql_parser::StringRef table_name,
                                 int64_t lo, int64_t hi,
                                 std::vector<size_t>& out) const {
        const TableShardConfig* cfg = lookup(table_name);
        if (!cfg || cfg->strategy != RoutingStrategy::LIST || cfg->list.empty())
            return;
        if (lo > hi) return;
        size_t n = cfg->shards.size();
        for (const auto& e : cfg->list) {
            if (e.is_int && e.int_val >= lo && e.int_val <= hi)
                out.push_back(clamp_index(e.shard_index, n));
        }
    }

    // RANGE only. Inclusive [lo, hi]. HASH/LIST yield no indices (caller scatters).
    void collect_int_range_shards(sql_parser::StringRef table_name,
                                  int64_t lo, int64_t hi,
                                  std::vector<size_t>& out) const {
        const TableShardConfig* cfg = lookup(table_name);
        if (!cfg || cfg->strategy != RoutingStrategy::RANGE || cfg->ranges.empty())
            return;
        if (lo > hi) return;
        size_t n = cfg->shards.size();
        const auto& ranges = cfg->ranges;
        for (size_t i = 0; i < ranges.size(); ++i) {
            int64_t seg_hi = (i + 1 == ranges.size())
                ? INT64_MAX : ranges[i].upper_inclusive;
            int64_t seg_lo = (i == 0) ? INT64_MIN
                : (cfg->ranges[i - 1].upper_inclusive == INT64_MAX
                   ? INT64_MAX : cfg->ranges[i - 1].upper_inclusive + 1);
            if (seg_lo <= hi && seg_hi >= lo)
                out.push_back(clamp_index(ranges[i].shard_index, n));
        }
    }

    bool same_routing(sql_parser::StringRef a, sql_parser::StringRef b) const {
        const TableShardConfig* ca = lookup(a);
        const TableShardConfig* cb = lookup(b);
        if (!ca || !cb) return false;
        if (ca->strategy != cb->strategy) return false;
        if (ca->shards.size() != cb->shards.size() || ca->shards.empty()) return false;
        for (size_t i = 0; i < ca->shards.size(); ++i) {
            if (ca->shards[i].backend_name != cb->shards[i].backend_name) return false;
        }
        if (ca->strategy == RoutingStrategy::RANGE) {
            if (ca->ranges.size() != cb->ranges.size()) return false;
            for (size_t i = 0; i < ca->ranges.size(); ++i) {
                if (ca->ranges[i].upper_inclusive != cb->ranges[i].upper_inclusive ||
                    ca->ranges[i].shard_index != cb->ranges[i].shard_index) return false;
            }
        }
        if (ca->strategy == RoutingStrategy::LIST) {
            if (ca->list.size() != cb->list.size()) return false;
            for (size_t i = 0; i < ca->list.size(); ++i) {
                if (ca->list[i].is_int != cb->list[i].is_int ||
                    ca->list[i].int_val != cb->list[i].int_val ||
                    ca->list[i].str_val != cb->list[i].str_val ||
                    ca->list[i].shard_index != cb->list[i].shard_index) return false;
            }
        }
        return true;
    }

    // Get the single backend for an unsharded table.
    const char* get_backend(sql_parser::StringRef table_name) const {
        const TableShardConfig* cfg = lookup(table_name);
        if (cfg && !cfg->shards.empty()) {
            return cfg->shards[0].backend_name.c_str();
        }
        return nullptr;
    }

private:
    std::unordered_map<std::string, TableShardConfig> tables_;

    const TableShardConfig* lookup(sql_parser::StringRef table_name) const {
        std::string key = to_lower(std::string(table_name.ptr, table_name.len));
        auto it = tables_.find(key);
        return it == tables_.end() ? nullptr : &it->second;
    }

    static std::string to_lower(const std::string& s) {
        std::string r = s;
        for (auto& c : r) { if (c >= 'A' && c <= 'Z') c += 32; }
        return r;
    }

    static size_t clamp_index(size_t idx, size_t n) {
        return idx < n ? idx : (n == 0 ? 0 : n - 1);
    }

    static void split_keys(const std::string& spec, std::vector<std::string>& out) {
        size_t start = 0;
        while (start <= spec.size()) {
            size_t plus = spec.find('+', start);
            if (plus == std::string::npos) {
                out.push_back(spec.substr(start));
                break;
            }
            out.push_back(spec.substr(start, plus - start));
            start = plus + 1;
        }
        out.erase(std::remove_if(out.begin(), out.end(),
                                 [](const std::string& s) { return s.empty(); }),
                  out.end());
    }

    static std::string join_keys(const std::vector<std::string>& keys) {
        std::string out;
        for (size_t i = 0; i < keys.size(); ++i) {
            if (i) out += '+';
            out += keys[i];
        }
        return out;
    }

    // FNV-1a 64-bit. Deterministic across compilers, fast, good enough for
    // shard-key routing where adversarial inputs are not a concern.
    static uint64_t fnv1a_mix(uint64_t h, const uint8_t* data, size_t len) {
        for (size_t i = 0; i < len; ++i) {
            h ^= static_cast<uint64_t>(data[i]);
            h *= 0x100000001b3ULL;
        }
        return h;
    }

    static uint64_t fnv1a_bytes(const uint8_t* data, size_t len) {
        return fnv1a_mix(0xcbf29ce484222325ULL, data, len);
    }

    static uint64_t fnv1a_int64(int64_t v) {
        uint64_t u = static_cast<uint64_t>(v);
        uint8_t bytes[8];
        for (int i = 0; i < 8; ++i) {
            bytes[i] = static_cast<uint8_t>((u >> (i * 8)) & 0xff);
        }
        return fnv1a_bytes(bytes, 8);
    }
};

} // namespace sql_engine

#endif // SQL_ENGINE_SHARD_MAP_H
