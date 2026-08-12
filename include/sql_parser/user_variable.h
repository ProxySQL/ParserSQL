#ifndef SQL_PARSER_USER_VARIABLE_H
#define SQL_PARSER_USER_VARIABLE_H

#include "sql_parser/ast.h"
#include "sql_parser/parse_result.h"
#include "sql_parser/token.h"

#include <cstdint>

namespace sql_parser {

enum class UserVariableUsage : uint8_t {
    NO_USER_VARIABLE,
    READ_ONLY,
    UNSAFE_OR_UNKNOWN
};

// Decode a TK_USER_VARIABLE into a stable identity while retaining its exact
// replayable spelling. Delimiter doubling is unescaped; backslashes are kept
// verbatim because their meaning depends on the session SQL mode.
inline AstNode* make_mysql_user_variable_node(Arena& arena, const Token& token) {
    if (token.type != TokenType::TK_USER_VARIABLE || !token.source.ptr ||
        token.source.len < 2 || token.source.ptr[0] != '@') {
        return nullptr;
    }

    const char* source = token.source.ptr;
    uint32_t source_len = token.source.len;
    StringRef decoded;
    char delimiter = source[1];
    bool quoted = delimiter == '\'' || delimiter == '"' || delimiter == '`';

    if (!quoted) {
        decoded = StringRef{source + 1, source_len - 1};
        if (decoded.len > 64) return nullptr;
    } else {
        if (source_len < 3 || source[source_len - 1] != delimiter) return nullptr;
        uint32_t inner_len = source_len - 3;
        char* buffer = static_cast<char*>(arena.allocate(inner_len));
        if (!buffer && inner_len != 0) return nullptr;
        uint32_t written = 0;
        for (uint32_t i = 2; i + 1 < source_len; ++i) {
            char c = source[i];
            if (c == delimiter && i + 2 < source_len && source[i + 1] == delimiter) {
                ++i;
            }
            if (written == 64) return nullptr;
            buffer[written++] = c;
        }
        decoded = StringRef{buffer, written};
    }

    AstNode* node = make_node(arena, NodeType::NODE_USER_VARIABLE, decoded);
    if (node) node->set_source(token.source);
    return node;
}

UserVariableUsage classify_mysql_user_variable_usage(const ParseResult& result);

} // namespace sql_parser

#endif // SQL_PARSER_USER_VARIABLE_H
