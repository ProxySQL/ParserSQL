#ifndef TOOLS_PG_COMPAT_STATEMENT_TYPE_MAP_H
#define TOOLS_PG_COMPAT_STATEMENT_TYPE_MAP_H

#include "sql_parser/common.h"

#include "protobuf/pg_query.pb-c.h"

#include <cstddef>

namespace pg_compat {

enum class MappingKind {
    Equivalent,
    NoEquivalent,
    Unmapped,
};

struct StatementTypeMapping {
    MappingKind kind = MappingKind::Unmapped;
    sql_parser::StmtType type = sql_parser::StmtType::UNKNOWN;
};

StatementTypeMapping expected_stmt_type(const PgQuery__Node& node);
const char* oracle_node_name(PgQuery__Node__NodeCase node_case);
std::size_t reviewed_node_case_count();
const char* stmt_type_name(sql_parser::StmtType type);

} // namespace pg_compat

#endif // TOOLS_PG_COMPAT_STATEMENT_TYPE_MAP_H
