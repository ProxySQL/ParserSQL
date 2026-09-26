#ifndef SQL_PARSER_PG_IDENTIFIER_H
#define SQL_PARSER_PG_IDENTIFIER_H
#include "sql_parser/token.h"

namespace sql_parser {

// Full PostgreSQL 18.4 keyword membership, including words represented as
// TK_IDENTIFIER by the shared tokenizer. Derived from PostgreSQL's kwlist.h.
// Quoted identifiers are identifiers even when their spelling is a keyword.
inline bool pg_keyword(const Token& token) {
    if (token.source.ptr != token.text.ptr || token.text.empty()) return false;
    static constexpr StringRef keywords[] = {
        {"abort", 5}, {"absent", 6}, {"absolute", 8}, {"access", 6}, {"action", 6}, {"add", 3},
        {"admin", 5}, {"after", 5}, {"aggregate", 9}, {"all", 3}, {"also", 4}, {"alter", 5},
        {"always", 6}, {"analyse", 7}, {"analyze", 7}, {"and", 3}, {"any", 3}, {"array", 5},
        {"as", 2}, {"asc", 3}, {"asensitive", 10}, {"assertion", 9}, {"assignment", 10}, {"asymmetric", 10},
        {"at", 2}, {"atomic", 6}, {"attach", 6}, {"attribute", 9}, {"authorization", 13}, {"backward", 8},
        {"before", 6}, {"begin", 5}, {"between", 7}, {"bigint", 6}, {"binary", 6}, {"bit", 3},
        {"boolean", 7}, {"both", 4}, {"breadth", 7}, {"by", 2}, {"cache", 5}, {"call", 4},
        {"called", 6}, {"cascade", 7}, {"cascaded", 8}, {"case", 4}, {"cast", 4}, {"catalog", 7},
        {"chain", 5}, {"char", 4}, {"character", 9}, {"characteristics", 15}, {"check", 5}, {"checkpoint", 10},
        {"class", 5}, {"close", 5}, {"cluster", 7}, {"coalesce", 8}, {"collate", 7}, {"collation", 9},
        {"column", 6}, {"columns", 7}, {"comment", 7}, {"comments", 8}, {"commit", 6}, {"committed", 9},
        {"compression", 11}, {"concurrently", 12}, {"conditional", 11}, {"configuration", 13}, {"conflict", 8}, {"connection", 10},
        {"constraint", 10}, {"constraints", 11}, {"content", 7}, {"continue", 8}, {"conversion", 10}, {"copy", 4},
        {"cost", 4}, {"create", 6}, {"cross", 5}, {"csv", 3}, {"cube", 4}, {"current", 7},
        {"current_catalog", 15}, {"current_date", 12}, {"current_role", 12}, {"current_schema", 14}, {"current_time", 12}, {"current_timestamp", 17},
        {"current_user", 12}, {"cursor", 6}, {"cycle", 5}, {"data", 4}, {"database", 8}, {"day", 3},
        {"deallocate", 10}, {"dec", 3}, {"decimal", 7}, {"declare", 7}, {"default", 7}, {"defaults", 8},
        {"deferrable", 10}, {"deferred", 8}, {"definer", 7}, {"delete", 6}, {"delimiter", 9}, {"delimiters", 10},
        {"depends", 7}, {"depth", 5}, {"desc", 4}, {"detach", 6}, {"dictionary", 10}, {"disable", 7},
        {"discard", 7}, {"distinct", 8}, {"do", 2}, {"document", 8}, {"domain", 6}, {"double", 6},
        {"drop", 4}, {"each", 4}, {"else", 4}, {"empty", 5}, {"enable", 6}, {"encoding", 8},
        {"encrypted", 9}, {"end", 3}, {"enforced", 8}, {"enum", 4}, {"error", 5}, {"escape", 6},
        {"event", 5}, {"except", 6}, {"exclude", 7}, {"excluding", 9}, {"exclusive", 9}, {"execute", 7},
        {"exists", 6}, {"explain", 7}, {"expression", 10}, {"extension", 9}, {"external", 8}, {"extract", 7},
        {"false", 5}, {"family", 6}, {"fetch", 5}, {"filter", 6}, {"finalize", 8}, {"first", 5},
        {"float", 5}, {"following", 9}, {"for", 3}, {"force", 5}, {"foreign", 7}, {"format", 6},
        {"forward", 7}, {"freeze", 6}, {"from", 4}, {"full", 4}, {"function", 8}, {"functions", 9},
        {"generated", 9}, {"global", 6}, {"grant", 5}, {"granted", 7}, {"greatest", 8}, {"group", 5},
        {"grouping", 8}, {"groups", 6}, {"handler", 7}, {"having", 6}, {"header", 6}, {"hold", 4},
        {"hour", 4}, {"identity", 8}, {"if", 2}, {"ilike", 5}, {"immediate", 9}, {"immutable", 9},
        {"implicit", 8}, {"import", 6}, {"in", 2}, {"include", 7}, {"including", 9}, {"increment", 9},
        {"indent", 6}, {"index", 5}, {"indexes", 7}, {"inherit", 7}, {"inherits", 8}, {"initially", 9},
        {"inline", 6}, {"inner", 5}, {"inout", 5}, {"input", 5}, {"insensitive", 11}, {"insert", 6},
        {"instead", 7}, {"int", 3}, {"integer", 7}, {"intersect", 9}, {"interval", 8}, {"into", 4},
        {"invoker", 7}, {"is", 2}, {"isnull", 6}, {"isolation", 9}, {"join", 4}, {"json", 4},
        {"json_array", 10}, {"json_arrayagg", 13}, {"json_exists", 11}, {"json_object", 11}, {"json_objectagg", 14}, {"json_query", 10},
        {"json_scalar", 11}, {"json_serialize", 14}, {"json_table", 10}, {"json_value", 10}, {"keep", 4}, {"key", 3},
        {"keys", 4}, {"label", 5}, {"language", 8}, {"large", 5}, {"last", 4}, {"lateral", 7},
        {"leading", 7}, {"leakproof", 9}, {"least", 5}, {"left", 4}, {"level", 5}, {"like", 4},
        {"limit", 5}, {"listen", 6}, {"load", 4}, {"local", 5}, {"localtime", 9}, {"localtimestamp", 14},
        {"location", 8}, {"lock", 4}, {"locked", 6}, {"logged", 6}, {"mapping", 7}, {"match", 5},
        {"matched", 7}, {"materialized", 12}, {"maxvalue", 8}, {"merge", 5}, {"merge_action", 12}, {"method", 6},
        {"minute", 6}, {"minvalue", 8}, {"mode", 4}, {"month", 5}, {"move", 4}, {"name", 4},
        {"names", 5}, {"national", 8}, {"natural", 7}, {"nchar", 5}, {"nested", 6}, {"new", 3},
        {"next", 4}, {"nfc", 3}, {"nfd", 3}, {"nfkc", 4}, {"nfkd", 4}, {"no", 2},
        {"none", 4}, {"normalize", 9}, {"normalized", 10}, {"not", 3}, {"nothing", 7}, {"notify", 6},
        {"notnull", 7}, {"nowait", 6}, {"null", 4}, {"nullif", 6}, {"nulls", 5}, {"numeric", 7},
        {"object", 6}, {"objects", 7}, {"of", 2}, {"off", 3}, {"offset", 6}, {"oids", 4},
        {"old", 3}, {"omit", 4}, {"on", 2}, {"only", 4}, {"operator", 8}, {"option", 6},
        {"options", 7}, {"or", 2}, {"order", 5}, {"ordinality", 10}, {"others", 6}, {"out", 3},
        {"outer", 5}, {"over", 4}, {"overlaps", 8}, {"overlay", 7}, {"overriding", 10}, {"owned", 5},
        {"owner", 5}, {"parallel", 8}, {"parameter", 9}, {"parser", 6}, {"partial", 7}, {"partition", 9},
        {"passing", 7}, {"password", 8}, {"path", 4}, {"period", 6}, {"placing", 7}, {"plan", 4},
        {"plans", 5}, {"policy", 6}, {"position", 8}, {"preceding", 9}, {"precision", 9}, {"prepare", 7},
        {"prepared", 8}, {"preserve", 8}, {"primary", 7}, {"prior", 5}, {"privileges", 10}, {"procedural", 10},
        {"procedure", 9}, {"procedures", 10}, {"program", 7}, {"publication", 11}, {"quote", 5}, {"quotes", 6},
        {"range", 5}, {"read", 4}, {"real", 4}, {"reassign", 8}, {"recursive", 9}, {"ref", 3},
        {"references", 10}, {"referencing", 11}, {"refresh", 7}, {"reindex", 7}, {"relative", 8}, {"release", 7},
        {"rename", 6}, {"repeatable", 10}, {"replace", 7}, {"replica", 7}, {"reset", 5}, {"restart", 7},
        {"restrict", 8}, {"return", 6}, {"returning", 9}, {"returns", 7}, {"revoke", 6}, {"right", 5},
        {"role", 4}, {"rollback", 8}, {"rollup", 6}, {"routine", 7}, {"routines", 8}, {"row", 3},
        {"rows", 4}, {"rule", 4}, {"savepoint", 9}, {"scalar", 6}, {"schema", 6}, {"schemas", 7},
        {"scroll", 6}, {"search", 6}, {"second", 6}, {"security", 8}, {"select", 6}, {"sequence", 8},
        {"sequences", 9}, {"serializable", 12}, {"server", 6}, {"session", 7}, {"session_user", 12}, {"set", 3},
        {"setof", 5}, {"sets", 4}, {"share", 5}, {"show", 4}, {"similar", 7}, {"simple", 6},
        {"skip", 4}, {"smallint", 8}, {"snapshot", 8}, {"some", 4}, {"source", 6}, {"sql", 3},
        {"stable", 6}, {"standalone", 10}, {"start", 5}, {"statement", 9}, {"statistics", 10}, {"stdin", 5},
        {"stdout", 6}, {"storage", 7}, {"stored", 6}, {"strict", 6}, {"string", 6}, {"strip", 5},
        {"subscription", 12}, {"substring", 9}, {"support", 7}, {"symmetric", 9}, {"sysid", 5}, {"system", 6},
        {"system_user", 11}, {"table", 5}, {"tables", 6}, {"tablesample", 11}, {"tablespace", 10}, {"target", 6},
        {"temp", 4}, {"template", 8}, {"temporary", 9}, {"text", 4}, {"then", 4}, {"ties", 4},
        {"time", 4}, {"timestamp", 9}, {"to", 2}, {"trailing", 8}, {"transaction", 11}, {"transform", 9},
        {"treat", 5}, {"trigger", 7}, {"trim", 4}, {"true", 4}, {"truncate", 8}, {"trusted", 7},
        {"type", 4}, {"types", 5}, {"uescape", 7}, {"unbounded", 9}, {"uncommitted", 11}, {"unconditional", 13},
        {"unencrypted", 11}, {"union", 5}, {"unique", 6}, {"unknown", 7}, {"unlisten", 8}, {"unlogged", 8},
        {"until", 5}, {"update", 6}, {"user", 4}, {"using", 5}, {"vacuum", 6}, {"valid", 5},
        {"validate", 8}, {"validator", 9}, {"value", 5}, {"values", 6}, {"varchar", 7}, {"variadic", 8},
        {"varying", 7}, {"verbose", 7}, {"version", 7}, {"view", 4}, {"views", 5}, {"virtual", 7},
        {"volatile", 8}, {"when", 4}, {"where", 5}, {"whitespace", 10}, {"window", 6}, {"with", 4},
        {"within", 6}, {"without", 7}, {"work", 4}, {"wrapper", 7}, {"write", 5}, {"xml", 3},
        {"xmlattributes", 13}, {"xmlconcat", 9}, {"xmlelement", 10}, {"xmlexists", 9}, {"xmlforest", 9}, {"xmlnamespaces", 13},
        {"xmlparse", 8}, {"xmlpi", 5}, {"xmlroot", 7}, {"xmlserialize", 12}, {"xmltable", 8}, {"year", 4},
        {"yes", 3}, {"zone", 4},
    };
    size_t lo = 0, hi = sizeof(keywords) / sizeof(keywords[0]);
    while (lo < hi) {
        const size_t mid = lo + (hi - lo) / 2;
        const int order = ci_cmp(token.text.ptr, token.text.len, keywords[mid].ptr,
            static_cast<uint8_t>(keywords[mid].len));
        if (!order) return true;
        if (order < 0) hi = mid; else lo = mid + 1;
    }
    return false;
}

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
