#ifndef SQL_PARSER_PARSE_RESULT_H
#define SQL_PARSER_PARSE_RESULT_H

#include "sql_parser/common.h"
#include "sql_parser/ast.h"
#include <vector>

namespace sql_parser {

struct ErrorInfo {
    uint32_t offset = 0;
    StringRef message;
};

struct BoundValue {
    enum Type : uint8_t { INT, FLOAT, DOUBLE, STRING, BLOB, NULL_VAL, DATETIME, DECIMAL };
    Type type = NULL_VAL;
    union {
        int64_t int_val;
        float float32_val;
        double float64_val;
        StringRef str_val;
    };

    BoundValue() : type(NULL_VAL), int_val(0) {}
    BoundValue(const BoundValue&) = default;
    BoundValue& operator=(const BoundValue&) = default;
};

struct ParamBindings {
    BoundValue* values = nullptr;
    uint16_t count = 0;
};

struct ParseResult {
    enum Status : uint8_t { OK = 0, PARTIAL, ERROR };

    Status status = ERROR;
    StmtType stmt_type = StmtType::UNKNOWN;
    AstNode* ast = nullptr;
    ErrorInfo error;
    StringRef remaining;
    bool full_input = false;
    bool has_user_variables = false;

    StringRef table_name;
    StringRef schema_name;
    StringRef database_name;

    ParamBindings bindings;    // populated by execute()

    bool ok() const { return status == OK; }
    bool has_remaining() const { return !remaining.empty(); }
};

// The vector owns result records, not ASTs or source text. ASTs live in the
// parser arena until parse(), parse_all(), or reset(); input text must outlive
// its results, just as for the single-statement API.
struct ParsedStatement {
    ParseResult result;
    StringRef source;
    uint32_t offset = 0;
};

struct BatchParseResult {
    std::vector<ParsedStatement> statements;

    bool ok() const {
        for (const auto& statement : statements) {
            if (!statement.result.ok() || !statement.result.full_input ||
                statement.result.stmt_type == StmtType::UNKNOWN) return false;
        }
        return true;
    }
};

} // namespace sql_parser

#endif // SQL_PARSER_PARSE_RESULT_H
