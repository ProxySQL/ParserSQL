#ifndef SQL_ENGINE_OPERATORS_SET_OP_OP_H
#define SQL_ENGINE_OPERATORS_SET_OP_OP_H

#include "sql_engine/operator.h"
#include "sql_engine/plan_node.h"
#include <unordered_set>
#include <string>
#include <vector>
#include <future>

namespace sql_engine {

class SetOpOperator : public Operator {
public:
    SetOpOperator(Operator* left, Operator* right, uint8_t op, bool all,
                  bool parallel_open = false)
        : left_(left), right_(right), op_(op), all_(all),
          parallel_open_(parallel_open) {}

    void open() override {
        if (parallel_open_) {
            // Parallel open (#26): launch left and right opens concurrently.
            // This is beneficial when both children are RemoteScan operators
            // (each performing a network round-trip in open()).
            auto fl = std::async(std::launch::async, [this]{ left_->open(); });
            auto fr = std::async(std::launch::async, [this]{ right_->open(); });
            fl.get();
            fr.get();
        } else {
            left_->open();
            right_->open();
        }
        reading_left_ = true;
        seen_.clear();

        if (op_ == SET_OP_INTERSECT || op_ == SET_OP_EXCEPT) {
            // Materialize right side into a set
            right_set_.clear();
            Row r{};
            while (right_->next(r)) {
                right_set_.insert(row_key(r));
            }
            right_->close();
        }
    }

    bool next(Row& out) override {
        if (op_ == SET_OP_UNION && !all_) {
            // UNION (deduplicated)
            while (true) {
                bool got = false;
                if (reading_left_) {
                    got = left_->next(out);
                    if (!got) { reading_left_ = false; }
                }
                if (!reading_left_) {
                    got = right_->next(out);
                    if (!got) return false;
                }
                if (got) {
                    std::string key = row_key(out);
                    if (seen_.insert(key).second) return true;
                }
            }
        }

        if (op_ == SET_OP_UNION && all_) {
            // UNION ALL: yield left then right
            if (reading_left_) {
                if (left_->next(out)) return true;
                reading_left_ = false;
            }
            return right_->next(out);
        }

        if (op_ == SET_OP_INTERSECT) {
            // Yield left rows that also appear in right
            while (left_->next(out)) {
                std::string key = row_key(out);
                if (right_set_.count(key)) {
                    if (all_ || seen_.insert(key).second)
                        return true;
                }
            }
            return false;
        }

        if (op_ == SET_OP_EXCEPT) {
            // Yield left rows that don't appear in right
            while (left_->next(out)) {
                std::string key = row_key(out);
                if (!right_set_.count(key)) {
                    if (all_ || seen_.insert(key).second)
                        return true;
                }
            }
            return false;
        }

        return false;
    }

    void close() override {
        left_->close();
        right_->close();
        seen_.clear();
        right_set_.clear();
    }

private:
    Operator* left_;
    Operator* right_;
    uint8_t op_;
    bool all_;
    bool parallel_open_;
    bool reading_left_ = true;
    std::unordered_set<std::string> seen_;
    std::unordered_set<std::string> right_set_;

    static std::string row_key(const Row& row) {
        std::string key;
        for (uint16_t i = 0; i < row.column_count; ++i) {
            const Value& v = row.values[i];
            if (v.is_null()) {
                key += "N";
            } else {
                switch (v.tag) {
                    case Value::TAG_BOOL: key += v.bool_val ? "T" : "F"; break;
                    case Value::TAG_INT64: key += std::to_string(v.int_val); break;
                    case Value::TAG_UINT64: key += std::to_string(v.uint_val); break;
                    case Value::TAG_DOUBLE: key += std::to_string(v.double_val); break;
                    case Value::TAG_STRING:
                        key.append(v.str_val.ptr, v.str_val.len);
                        break;
                    default: key += "?"; break;
                }
            }
            key += '\x01';
        }
        return key;
    }
};

} // namespace sql_engine

#endif // SQL_ENGINE_OPERATORS_SET_OP_OP_H
