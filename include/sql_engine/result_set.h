#ifndef SQL_ENGINE_RESULT_SET_H
#define SQL_ENGINE_RESULT_SET_H

#include "sql_engine/row.h"
#include <vector>
#include <string>

namespace sql_engine {

struct ResultSet {
    std::vector<Row> rows;
    std::vector<std::string> column_names;
    uint16_t column_count = 0;

    size_t row_count() const { return rows.size(); }
    bool empty() const { return rows.empty(); }
};

} // namespace sql_engine

#endif // SQL_ENGINE_RESULT_SET_H
