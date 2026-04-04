// derived_scan_op.h -- DerivedScanOperator for subqueries in FROM clause
//
// Materializes the inner plan's result set on open(), then yields
// rows one at a time from the materialized buffer on next().

#ifndef SQL_ENGINE_OPERATORS_DERIVED_SCAN_OP_H
#define SQL_ENGINE_OPERATORS_DERIVED_SCAN_OP_H

#include "sql_engine/operator.h"
#include "sql_engine/row.h"
#include <vector>

namespace sql_engine {

class DerivedScanOperator : public Operator {
public:
    // Takes ownership of the inner operator. On open(), pulls all rows
    // from the inner operator and stores them. On next(), yields them.
    explicit DerivedScanOperator(Operator* inner)
        : inner_(inner) {}

    void open() override {
        rows_.clear();
        cursor_ = 0;
        if (inner_) {
            inner_->open();
            Row row{};
            while (inner_->next(row)) {
                rows_.push_back(row);
            }
            inner_->close();
        }
    }

    bool next(Row& out) override {
        if (cursor_ >= rows_.size()) return false;
        out = rows_[cursor_++];
        return true;
    }

    void close() override {
        rows_.clear();
        cursor_ = 0;
    }

private:
    Operator* inner_;
    std::vector<Row> rows_;
    size_t cursor_ = 0;
};

} // namespace sql_engine

#endif // SQL_ENGINE_OPERATORS_DERIVED_SCAN_OP_H
