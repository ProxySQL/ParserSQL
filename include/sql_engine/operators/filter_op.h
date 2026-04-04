#ifndef SQL_ENGINE_OPERATORS_FILTER_OP_H
#define SQL_ENGINE_OPERATORS_FILTER_OP_H

#include "sql_engine/operator.h"
#include "sql_engine/expression_eval.h"
#include "sql_engine/catalog.h"
#include "sql_engine/subquery_executor.h"
#include "sql_parser/arena.h"
#include <functional>
#include <vector>

namespace sql_engine {

template <sql_parser::Dialect D>
class FilterOperator : public Operator {
public:
    FilterOperator(Operator* child,
                   const sql_parser::AstNode* expr,
                   const Catalog& catalog,
                   const std::vector<const TableInfo*>& tables,
                   FunctionRegistry<D>& functions,
                   sql_parser::Arena& arena,
                   SubqueryExecutor<D>* subquery_exec = nullptr)
        : child_(child), expr_(expr), catalog_(catalog),
          tables_(tables), functions_(functions), arena_(arena),
          subquery_exec_(subquery_exec) {}

    void open() override {
        child_->open();
    }

    bool next(Row& out) override {
        while (child_->next(out)) {
            // Build a resolver that can look up column names in any of the known tables
            auto resolver = make_multi_table_resolver(out);
            Value result = evaluate_expression<D>(expr_, resolver, functions_, arena_, subquery_exec_);
            if (is_truthy(result)) return true;
        }
        return false;
    }

    void close() override {
        child_->close();
    }

private:
    Operator* child_;
    const sql_parser::AstNode* expr_;
    const Catalog& catalog_;
    std::vector<const TableInfo*> tables_;
    FunctionRegistry<D>& functions_;
    sql_parser::Arena& arena_;
    SubqueryExecutor<D>* subquery_exec_ = nullptr;

    std::function<Value(sql_parser::StringRef)> make_multi_table_resolver(const Row& row) {
        return [this, &row](sql_parser::StringRef col_name) -> Value {
            // Try each table's columns
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

    static bool is_truthy(const Value& v) {
        if (v.is_null()) return false;
        if (v.tag == Value::TAG_BOOL) return v.bool_val;
        if (v.tag == Value::TAG_INT64) return v.int_val != 0;
        if (v.tag == Value::TAG_DOUBLE) return v.double_val != 0.0;
        return true; // non-null non-numeric = truthy
    }
};

} // namespace sql_engine

#endif // SQL_ENGINE_OPERATORS_FILTER_OP_H
