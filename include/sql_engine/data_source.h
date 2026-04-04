#ifndef SQL_ENGINE_DATA_SOURCE_H
#define SQL_ENGINE_DATA_SOURCE_H

#include "sql_engine/catalog.h"
#include "sql_engine/row.h"
#include <vector>

namespace sql_engine {

class DataSource {
public:
    virtual ~DataSource() = default;
    virtual const TableInfo* table_info() const = 0;
    virtual void open() = 0;
    virtual bool next(Row& out) = 0;
    virtual void close() = 0;
};

class InMemoryDataSource : public DataSource {
public:
    InMemoryDataSource(const TableInfo* table, std::vector<Row> rows)
        : table_(table), rows_(std::move(rows)) {}

    const TableInfo* table_info() const override { return table_; }

    void open() override { cursor_ = 0; }

    bool next(Row& out) override {
        if (cursor_ >= rows_.size()) return false;
        out = rows_[cursor_++];
        return true;
    }

    void close() override { cursor_ = 0; }

private:
    const TableInfo* table_;
    std::vector<Row> rows_;
    size_t cursor_ = 0;
};

} // namespace sql_engine

#endif // SQL_ENGINE_DATA_SOURCE_H
