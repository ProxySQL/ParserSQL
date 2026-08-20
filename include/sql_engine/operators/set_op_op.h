#ifndef SQL_ENGINE_OPERATORS_SET_OP_OP_H
#define SQL_ENGINE_OPERATORS_SET_OP_OP_H

#include "sql_engine/operator.h"
#include "sql_engine/plan_node.h"
#include "sql_engine/thread_pool.h"
#include "sql_engine/engine_limits.h"
#include <unordered_set>
#include <string>
#include <vector>
#include <future>
#include <stdexcept>

namespace sql_engine {

class SetOpOperator : public Operator {
public:
    SetOpOperator(Operator* left, Operator* right, uint8_t op, bool all,
                  bool parallel_open = false, ThreadPool* pool = nullptr)
        : op_(op), all_(all), parallel_open_(parallel_open), pool_(pool) {
        children_.push_back(left);
        children_.push_back(right);
    }

    explicit SetOpOperator(std::vector<Operator*> children,
                           bool parallel_open = false, ThreadPool* pool = nullptr)
        : children_(std::move(children)), op_(SET_OP_UNION), all_(true),
          parallel_open_(parallel_open), pool_(pool) {}

    void open() override {
        if (children_.empty()) return;
        if (parallel_open_ && children_.size() > 1) {
            std::vector<std::future<void>> futures;
            futures.reserve(children_.size());
            for (size_t i = 0; i < children_.size(); ++i) {
                auto launcher = [this, i]{ children_[i]->open(); };
                if (pool_) {
                    futures.push_back(pool_->submit(std::move(launcher)));
                } else {
                    futures.push_back(std::async(std::launch::async, std::move(launcher)));
                }
            }
            for (auto& f : futures) f.get();
        } else {
            for (auto* c : children_) c->open();
        }
        child_idx_ = 0;
        seen_.clear();
        expected_col_count_ = -1;

        if ((op_ == SET_OP_INTERSECT || op_ == SET_OP_EXCEPT) && children_.size() >= 2) {
            right_set_.clear();
            Row r{};
            while (children_[1]->next(r)) {
                check_col_count(r);
                check_operator_row_limit(right_set_.size(), kDefaultMaxOperatorRows, "SetOpOperator");
                right_set_.insert(row_key(r));
            }
            children_[1]->close();
            right_closed_ = true;
        }
    }

    bool next(Row& out) override {
        if (children_.empty()) return false;

        if (op_ == SET_OP_UNION && !all_) {
            while (child_idx_ < children_.size()) {
                if (!children_[child_idx_]->next(out)) {
                    ++child_idx_;
                    continue;
                }
                check_col_count(out);
                std::string key = row_key(out);
                if (seen_.find(key) == seen_.end()) {
                    check_operator_row_limit(seen_.size(), kDefaultMaxOperatorRows, "SetOpOperator");
                }
                if (seen_.insert(key).second) return true;
            }
            return false;
        }

        if (op_ == SET_OP_UNION && all_) {
            while (child_idx_ < children_.size()) {
                if (children_[child_idx_]->next(out)) {
                    check_col_count(out);
                    return true;
                }
                ++child_idx_;
            }
            return false;
        }

        if (op_ == SET_OP_INTERSECT) {
            while (children_[0]->next(out)) {
                check_col_count(out);
                std::string key = row_key(out);
                if (right_set_.count(key)) {
                    if (all_ || seen_.insert(key).second)
                        return true;
                }
            }
            return false;
        }

        if (op_ == SET_OP_EXCEPT) {
            while (children_[0]->next(out)) {
                check_col_count(out);
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
        for (size_t i = 0; i < children_.size(); ++i) {
            if (right_closed_ && i == 1) continue;
            children_[i]->close();
        }
        seen_.clear();
        right_set_.clear();
    }

private:
    std::vector<Operator*> children_;
    uint8_t op_;
    bool all_;
    bool parallel_open_;
    ThreadPool* pool_ = nullptr;
    size_t child_idx_ = 0;
    bool right_closed_ = false;
    std::unordered_set<std::string> seen_;
    std::unordered_set<std::string> right_set_;
    // Column count established by the first row we see. Used to detect
    // schema mismatches between the two sides of a UNION/INTERSECT/EXCEPT --
    // which the SQL standard says must have the same number of columns.
    // Previously, mismatched column counts silently produced wrong results
    // because row_key() would iterate a different number of values on each
    // side. Now we throw a clear error at execution time.
    int16_t expected_col_count_ = -1;

    void check_col_count(const Row& row) {
        if (expected_col_count_ < 0) {
            expected_col_count_ = static_cast<int16_t>(row.column_count);
            return;
        }
        if (row.column_count != static_cast<uint16_t>(expected_col_count_)) {
            throw std::runtime_error(
                "set operation column count mismatch: expected " +
                std::to_string(expected_col_count_) + " columns, got " +
                std::to_string(row.column_count));
        }
    }

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
