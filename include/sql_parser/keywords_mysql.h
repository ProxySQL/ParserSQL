#ifndef SQL_PARSER_KEYWORDS_MYSQL_H
#define SQL_PARSER_KEYWORDS_MYSQL_H

#include "sql_parser/token.h"
#include <algorithm>
#include <cstring>

namespace sql_parser {
namespace mysql_keywords {

struct KeywordEntry {
    const char* text;
    uint8_t len;
    TokenType token;
};

inline constexpr KeywordEntry KEYWORDS[] = {
    {"ALL", 3, TokenType::TK_ALL},
    {"ALTER", 5, TokenType::TK_ALTER},
    {"ANALYZE", 7, TokenType::TK_ANALYZE},
    {"AND", 3, TokenType::TK_AND},
    {"ARRAY", 5, TokenType::TK_ARRAY},
    {"AS", 2, TokenType::TK_AS},
    {"ASC", 3, TokenType::TK_ASC},
    {"AVG", 3, TokenType::TK_AVG},
    {"BEGIN", 5, TokenType::TK_BEGIN},
    {"BETWEEN", 7, TokenType::TK_BETWEEN},
    {"BUFFERS", 7, TokenType::TK_BUFFERS},
    {"BY", 2, TokenType::TK_BY},
    {"CALL", 4, TokenType::TK_CALL},
    {"CASE", 4, TokenType::TK_CASE},
    {"CHARACTER", 9, TokenType::TK_CHARACTER},
    {"CHARSET", 7, TokenType::TK_CHARSET},
    {"COLLATE", 7, TokenType::TK_COLLATE},
    {"COLUMNS", 7, TokenType::TK_COLUMNS},
    {"COMMIT", 6, TokenType::TK_COMMIT},
    {"COMMITTED", 9, TokenType::TK_COMMITTED},
    {"CONCURRENT", 10, TokenType::TK_CONCURRENT},
    {"CONFLICT", 8, TokenType::TK_CONFLICT},
    {"CONSTRAINT", 10, TokenType::TK_CONSTRAINT},
    {"COSTS", 5, TokenType::TK_COSTS},
    {"COUNT", 5, TokenType::TK_COUNT},
    {"CREATE", 6, TokenType::TK_CREATE},
    {"CROSS", 5, TokenType::TK_CROSS},
    {"DATA", 4, TokenType::TK_DATA},
    {"DATABASE", 8, TokenType::TK_DATABASE},
    {"DEALLOCATE", 10, TokenType::TK_DEALLOCATE},
    {"DEFAULT", 7, TokenType::TK_DEFAULT},
    {"DELAYED", 7, TokenType::TK_DELAYED},
    {"DELETE", 6, TokenType::TK_DELETE},
    {"DESC", 4, TokenType::TK_DESC},
    {"DESCRIBE", 8, TokenType::TK_DESCRIBE},
    {"DISTINCT", 8, TokenType::TK_DISTINCT},
    {"DO", 2, TokenType::TK_DO},
    {"DROP", 4, TokenType::TK_DROP},
    {"DUMPFILE", 8, TokenType::TK_DUMPFILE},
    {"DUPLICATE", 9, TokenType::TK_DUPLICATE},
    {"ELSE", 4, TokenType::TK_ELSE},
    {"ENCLOSED", 8, TokenType::TK_ENCLOSED},
    {"END", 3, TokenType::TK_END},
    {"ESCAPED", 7, TokenType::TK_ESCAPED},
    {"EXCEPT", 6, TokenType::TK_EXCEPT},
    {"EXECUTE", 7, TokenType::TK_EXECUTE},
    {"EXISTS", 6, TokenType::TK_EXISTS},
    {"EXPLAIN", 7, TokenType::TK_EXPLAIN},
    {"FALSE", 5, TokenType::TK_FALSE},
    {"FETCH", 5, TokenType::TK_FETCH},
    {"FIELDS", 6, TokenType::TK_FIELDS},
    {"FOR", 3, TokenType::TK_FOR},
    {"FORMAT", 6, TokenType::TK_FORMAT},
    {"FROM", 4, TokenType::TK_FROM},
    {"FULL", 4, TokenType::TK_FULL},
    {"GLOBAL", 6, TokenType::TK_GLOBAL},
    {"GRANT", 5, TokenType::TK_GRANT},
    {"GROUP", 5, TokenType::TK_GROUP},
    {"HAVING", 6, TokenType::TK_HAVING},
    {"HIGH_PRIORITY", 13, TokenType::TK_HIGH_PRIORITY},
    {"IF", 2, TokenType::TK_IF},
    {"IGNORE", 6, TokenType::TK_IGNORE},
    {"IN", 2, TokenType::TK_IN},
    {"INDEX", 5, TokenType::TK_INDEX},
    {"INFILE", 6, TokenType::TK_INFILE},
    {"INNER", 5, TokenType::TK_INNER},
    {"INSERT", 6, TokenType::TK_INSERT},
    {"INTERSECT", 9, TokenType::TK_INTERSECT},
    {"INTO", 4, TokenType::TK_INTO},
    {"IS", 2, TokenType::TK_IS},
    {"ISOLATION", 9, TokenType::TK_ISOLATION},
    {"JOIN", 4, TokenType::TK_JOIN},
    {"KEY", 3, TokenType::TK_KEY},
    {"LEFT", 4, TokenType::TK_LEFT},
    {"LEVEL", 5, TokenType::TK_LEVEL},
    {"LIKE", 4, TokenType::TK_LIKE},
    {"LIMIT", 5, TokenType::TK_LIMIT},
    {"LINES", 5, TokenType::TK_LINES},
    {"LOAD", 4, TokenType::TK_LOAD},
    {"LOCAL", 5, TokenType::TK_LOCAL},
    {"LOCK", 4, TokenType::TK_LOCK},
    {"LOCKED", 6, TokenType::TK_LOCKED},
    {"LOW_PRIORITY", 12, TokenType::TK_LOW_PRIORITY},
    {"MAX", 3, TokenType::TK_MAX},
    {"MIN", 3, TokenType::TK_MIN},
    {"NAMES", 5, TokenType::TK_NAMES},
    {"NATURAL", 7, TokenType::TK_NATURAL},
    {"NOT", 3, TokenType::TK_NOT},
    {"NOTHING", 7, TokenType::TK_NOTHING},
    {"NOWAIT", 6, TokenType::TK_NOWAIT},
    {"NULL", 4, TokenType::TK_NULL},
    {"OF", 2, TokenType::TK_OF},
    {"OFFSET", 6, TokenType::TK_OFFSET},
    {"ON", 2, TokenType::TK_ON},
    {"ONLY", 4, TokenType::TK_ONLY},
    {"OPTIONALLY", 10, TokenType::TK_OPTIONALLY},
    {"OR", 2, TokenType::TK_OR},
    {"ORDER", 5, TokenType::TK_ORDER},
    {"OUTER", 5, TokenType::TK_OUTER},
    {"OUTFILE", 7, TokenType::TK_OUTFILE},
    {"PERSIST", 7, TokenType::TK_PERSIST},
    {"PREPARE", 7, TokenType::TK_PREPARE},
    {"PROCEDURE", 9, TokenType::TK_PROCEDURE},
    {"QUICK", 5, TokenType::TK_QUICK},
    {"READ", 4, TokenType::TK_READ},
    {"REPEATABLE", 10, TokenType::TK_REPEATABLE},
    {"REPLACE", 7, TokenType::TK_REPLACE},
    {"RESET", 5, TokenType::TK_RESET},
    {"RETURNING", 9, TokenType::TK_RETURNING},
    {"REVOKE", 6, TokenType::TK_REVOKE},
    {"RIGHT", 5, TokenType::TK_RIGHT},
    {"ROLLBACK", 8, TokenType::TK_ROLLBACK},
    {"ROW", 3, TokenType::TK_ROW},
    {"ROWS", 4, TokenType::TK_ROWS},
    {"SAVEPOINT", 9, TokenType::TK_SAVEPOINT},
    {"SCHEMA", 6, TokenType::TK_SCHEMA},
    {"SELECT", 6, TokenType::TK_SELECT},
    {"SERIALIZABLE", 12, TokenType::TK_SERIALIZABLE},
    {"SESSION", 7, TokenType::TK_SESSION},
    {"SET", 3, TokenType::TK_SET},
    {"SETTINGS", 8, TokenType::TK_SETTINGS},
    {"SHARE", 5, TokenType::TK_SHARE},
    {"SHOW", 4, TokenType::TK_SHOW},
    {"SKIP", 4, TokenType::TK_SKIP},
    {"SQL_CALC_FOUND_ROWS", 19, TokenType::TK_SQL_CALC_FOUND_ROWS},
    {"START", 5, TokenType::TK_START},
    {"STARTING", 8, TokenType::TK_STARTING},
    {"SUM", 3, TokenType::TK_SUM},
    {"SUMMARY", 7, TokenType::TK_SUMMARY},
    {"TABLE", 5, TokenType::TK_TABLE},
    {"TERMINATED", 10, TokenType::TK_TERMINATED},
    {"THEN", 4, TokenType::TK_THEN},
    {"TIMING", 6, TokenType::TK_TIMING},
    {"TO", 2, TokenType::TK_TO},
    {"TRANSACTION", 11, TokenType::TK_TRANSACTION},
    {"TRUE", 4, TokenType::TK_TRUE},
    {"TRUNCATE", 8, TokenType::TK_TRUNCATE},
    {"UNION", 5, TokenType::TK_UNION},
    {"UNCOMMITTED", 11, TokenType::TK_UNCOMMITTED},
    {"UNLOCK", 6, TokenType::TK_UNLOCK},
    {"UPDATE", 6, TokenType::TK_UPDATE},
    {"USE", 3, TokenType::TK_USE},
    {"USING", 5, TokenType::TK_USING},
    {"VALUES", 6, TokenType::TK_VALUES},
    {"VERBOSE", 7, TokenType::TK_VERBOSE},
    {"VIEW", 4, TokenType::TK_VIEW},
    {"WAL", 3, TokenType::TK_WAL},
    {"WHEN", 4, TokenType::TK_WHEN},
    {"WHERE", 5, TokenType::TK_WHERE},
    {"WRITE", 5, TokenType::TK_WRITE},
};

inline constexpr size_t KEYWORD_COUNT = sizeof(KEYWORDS) / sizeof(KEYWORDS[0]);

inline TokenType lookup(const char* text, uint32_t len) {
    size_t lo = 0, hi = KEYWORD_COUNT;
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        int cmp = sql_parser::ci_cmp(text, len, KEYWORDS[mid].text, KEYWORDS[mid].len);
        if (cmp == 0) return KEYWORDS[mid].token;
        if (cmp < 0) hi = mid;
        else lo = mid + 1;
    }
    return TokenType::TK_IDENTIFIER;
}

} // namespace mysql_keywords
} // namespace sql_parser

#endif // SQL_PARSER_KEYWORDS_MYSQL_H
