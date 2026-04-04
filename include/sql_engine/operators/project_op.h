#ifndef SQL_ENGINE_OPERATORS_PROJECT_OP_H
#define SQL_ENGINE_OPERATORS_PROJECT_OP_H

#include "sql_engine/operator.h"
#include "sql_engine/expression_eval.h"
#include "sql_engine/catalog.h"
#include "sql_parser/arena.h"
#include <functional>
#include <vector>

namespace sql_engine {

template <sql_parser::Dialect D>
class ProjectOperator : public Operator {
public:
    ProjectOperator(Operator* child,
                    const sql_parser::AstNode** exprs,
                    uint16_t expr_count,
                    const Catalog& catalog,
                    const std::vector<const TableInfo*>& tables,
                    FunctionRegistry<D>& functions,
                    sql_parser::Arena& arena)
        : child_(child), exprs_(exprs), expr_count_(expr_count),
          catalog_(catalog), tables_(tables), functions_(functions), arena_(arena) {}

    void open() override {
        if (child_) child_->open();
        no_from_done_ = false;
    }

    bool next(Row& out) override {
        if (!child_) {
            // No FROM clause: produce one row
            if (no_from_done_) return false;
            no_from_done_ = true;
            Row dummy{};
            dummy.values = nullptr;
            dummy.column_count = 0;
            return evaluate_project(dummy, out);
        }

        Row input{};
        if (!child_->next(input)) return false;
        return evaluate_project(input, out);
    }

    void close() override {
        if (child_) child_->close();
    }

private:
    Operator* child_;
    const sql_parser::AstNode** exprs_;
    uint16_t expr_count_;
    const Catalog& catalog_;
    std::vector<const TableInfo*> tables_;
    FunctionRegistry<D>& functions_;
    sql_parser::Arena& arena_;
    bool no_from_done_ = false;

    bool evaluate_project(const Row& input, Row& out) {
        out = make_row(arena_, expr_count_);
        auto resolver = make_multi_table_resolver(input);
        for (uint16_t i = 0; i < expr_count_; ++i) {
            out.set(i, evaluate_expression<D>(exprs_[i], resolver, functions_, arena_));
        }
        return true;
    }

    std::function<Value(sql_parser::StringRef)> make_multi_table_resolver(const Row& row) {
        return [this, &row](sql_parser::StringRef col_name) -> Value {
            uint16_t offset = 0;
            for (const auto* table : tables_) {
                if (!table) continue;
                const ColumnInfo* col = catalog_.get_column(table, col_name);
                if (col) {
                    uint16_t idx = offset + col->ordinal;
                    if (idx < row.column_count) return row.get(idx);
                }
                offset += table->column_count;
            }
            return value_null();
        };
    }
};

} // namespace sql_engine

#endif // SQL_ENGINE_OPERATORS_PROJECT_OP_H
