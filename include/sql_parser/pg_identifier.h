#ifndef SQL_PARSER_PG_IDENTIFIER_H
#define SQL_PARSER_PG_IDENTIFIER_H
#include "sql_parser/token.h"

namespace sql_parser {

// ColLabel after a dot accepts identifiers and all keyword categories.
inline bool pg_column_label(const Token& token) {
    if (token.type == TokenType::TK_IDENTIFIER) return true;
    if (token.text.empty() || token.source.ptr != token.text.ptr) return false;
    unsigned char first = static_cast<unsigned char>(token.text.ptr[0]);
    return (first >= 'a' && first <= 'z') || (first >= 'A' && first <= 'Z');
}

// PostgreSQL 18 ColId for CTE names and output columns: unreserved and
// column-name keywords are allowed; reserved and type/function keywords are not.
inline bool pg_column_name(const Token& token) {
    if (token.type == TokenType::TK_IDENTIFIER && token.source.ptr != token.text.ptr) return true;
    if (!pg_column_label(token)) return false;
    static constexpr StringRef excluded[] = {
        {"all", 3}, {"analyse", 7}, {"analyze", 7}, {"and", 3},
        {"any", 3}, {"array", 5}, {"as", 2}, {"asc", 3},
        {"asymmetric", 10}, {"authorization", 13}, {"binary", 6}, {"both", 4},
        {"case", 4}, {"cast", 4}, {"check", 5}, {"collate", 7},
        {"collation", 9}, {"column", 6}, {"concurrently", 12}, {"constraint", 10},
        {"create", 6}, {"cross", 5}, {"current_catalog", 15}, {"current_date", 12},
        {"current_role", 12}, {"current_schema", 14}, {"current_time", 12}, {"current_timestamp", 17},
        {"current_user", 12}, {"default", 7}, {"deferrable", 10}, {"desc", 4},
        {"distinct", 8}, {"do", 2}, {"else", 4}, {"end", 3},
        {"except", 6}, {"false", 5}, {"fetch", 5}, {"for", 3},
        {"foreign", 7}, {"freeze", 6}, {"from", 4}, {"full", 4},
        {"grant", 5}, {"group", 5}, {"having", 6}, {"ilike", 5},
        {"in", 2}, {"initially", 9}, {"inner", 5}, {"intersect", 9},
        {"into", 4}, {"is", 2}, {"isnull", 6}, {"join", 4},
        {"lateral", 7}, {"leading", 7}, {"left", 4}, {"like", 4},
        {"limit", 5}, {"localtime", 9}, {"localtimestamp", 14}, {"natural", 7},
        {"not", 3}, {"notnull", 7}, {"null", 4}, {"offset", 6},
        {"on", 2}, {"only", 4}, {"or", 2}, {"order", 5},
        {"outer", 5}, {"overlaps", 8}, {"placing", 7}, {"primary", 7},
        {"references", 10}, {"returning", 9}, {"right", 5}, {"select", 6},
        {"session_user", 12}, {"similar", 7}, {"some", 4}, {"symmetric", 9},
        {"system_user", 11}, {"table", 5}, {"tablesample", 11}, {"then", 4},
        {"to", 2}, {"trailing", 8}, {"true", 4}, {"union", 5},
        {"unique", 6}, {"user", 4}, {"using", 5}, {"variadic", 8},
        {"verbose", 7}, {"when", 4}, {"where", 5}, {"window", 6},
        {"with", 4},
    };
    size_t lo = 0, hi = sizeof(excluded) / sizeof(excluded[0]);
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        int order = ci_cmp(token.text.ptr, token.text.len, excluded[mid].ptr,
            static_cast<uint8_t>(excluded[mid].len));
        if (order == 0) return false;
        if (order < 0) hi = mid;
        else lo = mid + 1;
    }
    return true;
}

// PostgreSQL 18 type_function_name (also used for named function arguments):
// identifiers, unreserved keywords and type/function-name keywords. The other
// two categories are listed here even when our tokenizer leaves them as IDENT.
// Reference: PostgreSQL src/include/parser/kwlist.h and parser/gram.y.
inline bool pg_type_function_name(const Token& token) {
    if (token.type == TokenType::TK_IDENTIFIER && token.source.ptr != token.text.ptr) return true;
    if (token.text.empty() || token.source.ptr != token.text.ptr) return false;
    unsigned char first = static_cast<unsigned char>(token.text.ptr[0]);
    if (!((first >= 'a' && first <= 'z') || (first >= 'A' && first <= 'Z') || first == '_' || first >= 128)) return false;
    static constexpr StringRef excluded[] = {
        {"all", 3}, {"analyse", 7}, {"analyze", 7}, {"and", 3},
        {"any", 3}, {"array", 5}, {"as", 2}, {"asc", 3},
        {"asymmetric", 10}, {"between", 7}, {"bigint", 6}, {"bit", 3},
        {"boolean", 7}, {"both", 4}, {"case", 4}, {"cast", 4},
        {"char", 4}, {"character", 9}, {"check", 5}, {"coalesce", 8},
        {"collate", 7}, {"column", 6}, {"constraint", 10}, {"create", 6},
        {"current_catalog", 15}, {"current_date", 12}, {"current_role", 12}, {"current_time", 12},
        {"current_timestamp", 17}, {"current_user", 12}, {"dec", 3}, {"decimal", 7},
        {"default", 7}, {"deferrable", 10}, {"desc", 4}, {"distinct", 8},
        {"do", 2}, {"else", 4}, {"end", 3}, {"except", 6},
        {"exists", 6}, {"extract", 7}, {"false", 5}, {"fetch", 5},
        {"float", 5}, {"for", 3}, {"foreign", 7}, {"from", 4},
        {"grant", 5}, {"greatest", 8}, {"group", 5}, {"grouping", 8},
        {"having", 6}, {"in", 2}, {"initially", 9}, {"inout", 5},
        {"int", 3}, {"integer", 7}, {"intersect", 9}, {"interval", 8},
        {"into", 4}, {"json", 4}, {"json_array", 10}, {"json_arrayagg", 13},
        {"json_exists", 11}, {"json_object", 11}, {"json_objectagg", 14}, {"json_query", 10},
        {"json_scalar", 11}, {"json_serialize", 14}, {"json_table", 10}, {"json_value", 10},
        {"lateral", 7}, {"leading", 7}, {"least", 5}, {"limit", 5},
        {"localtime", 9}, {"localtimestamp", 14}, {"merge_action", 12}, {"national", 8},
        {"nchar", 5}, {"none", 4}, {"normalize", 9}, {"not", 3},
        {"null", 4}, {"nullif", 6}, {"numeric", 7}, {"offset", 6},
        {"on", 2}, {"only", 4}, {"or", 2}, {"order", 5},
        {"out", 3}, {"overlay", 7}, {"placing", 7}, {"position", 8},
        {"precision", 9}, {"primary", 7}, {"real", 4}, {"references", 10},
        {"returning", 9}, {"row", 3}, {"select", 6}, {"session_user", 12},
        {"setof", 5}, {"smallint", 8}, {"some", 4}, {"substring", 9},
        {"symmetric", 9}, {"system_user", 11}, {"table", 5}, {"then", 4},
        {"time", 4}, {"timestamp", 9}, {"to", 2}, {"trailing", 8},
        {"treat", 5}, {"trim", 4}, {"true", 4}, {"union", 5},
        {"unique", 6}, {"user", 4}, {"using", 5}, {"values", 6},
        {"varchar", 7}, {"variadic", 8}, {"when", 4}, {"where", 5},
        {"window", 6}, {"with", 4}, {"xmlattributes", 13}, {"xmlconcat", 9},
        {"xmlelement", 10}, {"xmlexists", 9}, {"xmlforest", 9}, {"xmlnamespaces", 13},
        {"xmlparse", 8}, {"xmlpi", 5}, {"xmlroot", 7}, {"xmlserialize", 12},
        {"xmltable", 8},
    };
    size_t lo = 0, hi = sizeof(excluded) / sizeof(excluded[0]);
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        int order = ci_cmp(token.text.ptr, token.text.len, excluded[mid].ptr,
            static_cast<uint8_t>(excluded[mid].len));
        if (order == 0) return false;
        if (order < 0) hi = mid;
        else lo = mid + 1;
    }
    return true;
}

} // namespace sql_parser
#endif
