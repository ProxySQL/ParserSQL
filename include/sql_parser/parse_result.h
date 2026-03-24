#ifndef SQL_PARSER_PARSE_RESULT_H
#define SQL_PARSER_PARSE_RESULT_H

#include "sql_parser/common.h"
#include "sql_parser/ast.h"

namespace sql_parser {

struct ErrorInfo {
    uint32_t offset = 0;
    StringRef message;
};

struct ParseResult {
    enum Status : uint8_t { OK = 0, PARTIAL, ERROR };

    Status status = ERROR;
    StmtType stmt_type = StmtType::UNKNOWN;
    AstNode* ast = nullptr;
    ErrorInfo error;
    StringRef remaining;

    StringRef table_name;
    StringRef schema_name;
    StringRef database_name;

    bool ok() const { return status == OK; }
    bool has_remaining() const { return !remaining.empty(); }
};

} // namespace sql_parser

#endif // SQL_PARSER_PARSE_RESULT_H
