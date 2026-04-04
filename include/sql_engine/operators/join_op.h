#ifndef SQL_ENGINE_OPERATORS_JOIN_OP_H
#define SQL_ENGINE_OPERATORS_JOIN_OP_H

#include "sql_engine/operator.h"
#include "sql_engine/expression_eval.h"
#include "sql_engine/catalog.h"
#include "sql_engine/plan_node.h"
#include "sql_parser/arena.h"
#include <functional>
#include <vector>

namespace sql_engine {

template <sql_parser::Dialect D>
class NestedLoopJoinOperator : public Operator {
public:
    NestedLoopJoinOperator(Operator* left, Operator* right,
                           uint8_t join_type,
                           const sql_parser::AstNode* condition,
                           uint16_t left_cols, uint16_t right_cols,
                           const Catalog& catalog,
                           const std::vector<const TableInfo*>& left_tables,
                           const std::vector<const TableInfo*>& right_tables,
                           FunctionRegistry<D>& functions,
                           sql_parser::Arena& arena)
        : left_(left), right_(right), join_type_(join_type),
          condition_(condition), left_cols_(left_cols), right_cols_(right_cols),
          catalog_(catalog), left_tables_(left_tables), right_tables_(right_tables),
          functions_(functions), arena_(arena) {}

    void open() override {
        // Materialize right side
        right_->open();
        right_rows_.clear();
        Row r{};
        while (right_->next(r)) {
            right_rows_.push_back(r);
        }
        right_->close();

        left_->open();
        has_left_row_ = false;
        right_idx_ = 0;
        left_matched_ = false;
        left_exhausted_ = false;
    }

    bool next(Row& out) override {
        uint16_t total_cols = left_cols_ + right_cols_;

        while (true) {
            if (!has_left_row_) {
                if (left_exhausted_) return false;
                if (!left_->next(left_row_)) {
                    left_exhausted_ = true;
                    return false;
                }
                has_left_row_ = true;
                right_idx_ = 0;
                left_matched_ = false;
            }

            // CROSS JOIN or condition-based join
            if (join_type_ == JOIN_CROSS) {
                if (right_idx_ < right_rows_.size()) {
                    out = combine_rows(left_row_, right_rows_[right_idx_], total_cols);
                    right_idx_++;
                    return true;
                }
                has_left_row_ = false;
                continue;
            }

            // INNER or LEFT join
            while (right_idx_ < right_rows_.size()) {
                const Row& rr = right_rows_[right_idx_];
                right_idx_++;

                Row combined = combine_rows(left_row_, rr, total_cols);
                if (!condition_ || eval_condition(combined)) {
                    left_matched_ = true;
                    out = combined;
                    return true;
                }
            }

            // Done scanning right side for this left row
            if (join_type_ == JOIN_LEFT && !left_matched_) {
                // Emit left row + NULLs
                out = make_row(arena_, total_cols);
                for (uint16_t i = 0; i < left_cols_; ++i)
                    out.set(i, left_row_.get(i));
                for (uint16_t i = 0; i < right_cols_; ++i)
                    out.set(left_cols_ + i, value_null());
                has_left_row_ = false;
                return true;
            }

            has_left_row_ = false;
        }
    }

    void close() override {
        left_->close();
        right_rows_.clear();
    }

private:
    Operator* left_;
    Operator* right_;
    uint8_t join_type_;
    const sql_parser::AstNode* condition_;
    uint16_t left_cols_;
    uint16_t right_cols_;
    const Catalog& catalog_;
    std::vector<const TableInfo*> left_tables_;
    std::vector<const TableInfo*> right_tables_;
    FunctionRegistry<D>& functions_;
    sql_parser::Arena& arena_;

    std::vector<Row> right_rows_;
    Row left_row_{};
    bool has_left_row_ = false;
    size_t right_idx_ = 0;
    bool left_matched_ = false;
    bool left_exhausted_ = false;

    Row combine_rows(const Row& left, const Row& right, uint16_t total_cols) {
        Row out = make_row(arena_, total_cols);
        for (uint16_t i = 0; i < left_cols_ && i < left.column_count; ++i)
            out.set(i, left.get(i));
        for (uint16_t i = 0; i < right_cols_ && i < right.column_count; ++i)
            out.set(left_cols_ + i, right.get(i));
        return out;
    }

    bool eval_condition(const Row& combined) {
        // Build a resolver over all tables (left then right)
        std::vector<const TableInfo*> all_tables;
        all_tables.insert(all_tables.end(), left_tables_.begin(), left_tables_.end());
        all_tables.insert(all_tables.end(), right_tables_.begin(), right_tables_.end());

        auto resolver = [this, &combined, &all_tables](sql_parser::StringRef col_name) -> Value {
            uint16_t offset = 0;
            for (const auto* table : all_tables) {
                if (!table) continue;
                const ColumnInfo* col = catalog_.get_column(table, col_name);
                if (col) {
                    uint16_t idx = offset + col->ordinal;
                    if (idx < combined.column_count) return combined.get(idx);
                }
                offset += table->column_count;
            }
            return value_null();
        };

        Value result = evaluate_expression<D>(condition_, resolver, functions_, arena_);
        if (result.is_null()) return false;
        if (result.tag == Value::TAG_BOOL) return result.bool_val;
        if (result.tag == Value::TAG_INT64) return result.int_val != 0;
        return true;
    }
};

} // namespace sql_engine

#endif // SQL_ENGINE_OPERATORS_JOIN_OP_H
