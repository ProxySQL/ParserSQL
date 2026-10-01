#ifndef SQL_PARSER_MYSQL_IDENTIFIER_H
#define SQL_PARSER_MYSQL_IDENTIFIER_H

#include "sql_parser/token.h"
#include <cstddef>

namespace sql_parser {

// Keywords may be identifiers after a qualification dot, including reserved ones.
inline bool mysql_identifier_word(const Token& token) {
    return token.type == TokenType::TK_IDENTIFIER ||
        (token.type >= TokenType::TK_SELECT && token.type <= TokenType::TK_RECURSIVE);
}

inline bool mysql_identifier_token(const Token& token) {
    if (!mysql_identifier_word(token)) return false;
    if (token.source.ptr != token.text.ptr) return true; // delimited identifier
    // Sorted reserved-word baseline from INFORMATION_SCHEMA.KEYWORDS, MySQL
    // 8.4.8 (0896fcd61dec11a0904166911a0126f59daaa1bf), also reserved in
    // 9.7.2 (008e09c2834b98143a8c067d4d225c90953050cf). Version-specific
    // additions are not rejected without a server-version parser setting.
    static constexpr const char* reserved[] = {
        "ACCESSIBLE", "ADD", "ALL", "ALTER", "ANALYZE",
        "AND", "AS", "ASC", "ASENSITIVE", "BEFORE",
        "BETWEEN", "BIGINT", "BINARY", "BLOB", "BOTH",
        "BY", "CALL", "CASCADE", "CASE", "CHANGE",
        "CHAR", "CHARACTER", "CHECK", "COLLATE", "COLUMN",
        "CONDITION", "CONSTRAINT", "CONTINUE", "CONVERT", "CREATE",
        "CROSS", "CUME_DIST", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP",
        "CURRENT_USER", "CURSOR", "DATABASE", "DATABASES", "DAY_HOUR",
        "DAY_MICROSECOND", "DAY_MINUTE", "DAY_SECOND", "DEC", "DECIMAL",
        "DECLARE", "DEFAULT", "DELAYED", "DELETE", "DENSE_RANK",
        "DESC", "DESCRIBE", "DETERMINISTIC", "DISTINCT", "DISTINCTROW",
        "DIV", "DOUBLE", "DROP", "DUAL", "EACH",
        "ELSE", "ELSEIF", "EMPTY", "ENCLOSED", "ESCAPED",
        "EXCEPT", "EXISTS", "EXIT", "EXPLAIN", "FALSE",
        "FETCH", "FIRST_VALUE", "FLOAT", "FLOAT4", "FLOAT8",
        "FOR", "FORCE", "FOREIGN", "FROM", "FULLTEXT",
        "FUNCTION", "GENERATED", "GET", "GRANT", "GROUP",
        "GROUPING", "GROUPS", "HAVING", "HIGH_PRIORITY", "HOUR_MICROSECOND",
        "HOUR_MINUTE", "HOUR_SECOND", "IF", "IGNORE", "IN",
        "INDEX", "INFILE", "INNER", "INOUT", "INSENSITIVE",
        "INSERT", "INT", "INT1", "INT2", "INT3",
        "INT4", "INT8", "INTEGER", "INTERSECT", "INTERVAL",
        "INTO", "IO_AFTER_GTIDS", "IO_BEFORE_GTIDS", "IS", "ITERATE",
        "JOIN", "JSON_TABLE", "KEY", "KEYS", "KILL",
        "LAG", "LAST_VALUE", "LATERAL", "LEAD", "LEADING",
        "LEAVE", "LEFT", "LIKE", "LIMIT", "LINEAR",
        "LINES", "LOAD", "LOCALTIME", "LOCALTIMESTAMP", "LOCK",
        "LONG", "LONGBLOB", "LONGTEXT", "LOOP", "LOW_PRIORITY",
        "MATCH", "MAXVALUE", "MEDIUMBLOB", "MEDIUMINT", "MEDIUMTEXT",
        "MIDDLEINT", "MINUTE_MICROSECOND", "MINUTE_SECOND", "MOD", "MODIFIES",
        "NATURAL", "NOT", "NO_WRITE_TO_BINLOG", "NTH_VALUE", "NTILE",
        "NULL", "NUMERIC", "OF", "ON", "OPTIMIZE",
        "OPTIMIZER_COSTS", "OPTION", "OPTIONALLY", "OR", "ORDER",
        "OUT", "OUTER", "OUTFILE", "OVER", "PARTITION",
        "PERCENT_RANK", "PRECISION", "PRIMARY", "PROCEDURE", "PURGE",
        "RANGE", "RANK", "READ", "READS", "READ_WRITE",
        "REAL", "RECURSIVE", "REFERENCES", "REGEXP", "RELEASE",
        "RENAME", "REPEAT", "REPLACE", "REQUIRE", "RESIGNAL",
        "RESTRICT", "RETURN", "REVOKE", "RIGHT", "RLIKE",
        "ROW", "ROWS", "ROW_NUMBER", "SCHEMA", "SCHEMAS",
        "SECOND_MICROSECOND", "SELECT", "SENSITIVE", "SEPARATOR", "SET",
        "SHOW", "SIGNAL", "SMALLINT", "SPATIAL", "SPECIFIC",
        "SQL", "SQLEXCEPTION", "SQLSTATE", "SQLWARNING", "SQL_BIG_RESULT",
        "SQL_CALC_FOUND_ROWS", "SQL_SMALL_RESULT", "SSL", "STARTING", "STORED",
        "STRAIGHT_JOIN", "SYSTEM", "TABLE", "TERMINATED", "THEN",
        "TINYBLOB", "TINYINT", "TINYTEXT", "TO", "TRAILING",
        "TRIGGER", "TRUE", "UNDO", "UNION", "UNIQUE",
        "UNLOCK", "UNSIGNED", "UPDATE", "USAGE", "USE",
        "USING", "UTC_DATE", "UTC_TIME", "UTC_TIMESTAMP", "VALUES",
        "VARBINARY", "VARCHAR", "VARCHARACTER", "VARYING", "VIRTUAL",
        "WHEN", "WHERE", "WHILE", "WINDOW", "WITH",
        "WRITE", "XOR", "YEAR_MONTH", "ZEROFILL",
    };
    size_t lo = 0, hi = sizeof(reserved) / sizeof(reserved[0]);
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        const char* word = reserved[mid];
        uint32_t i = 0;
        int difference = 0;
        for (; i < token.text.len && word[i]; ++i) {
            unsigned char ch = static_cast<unsigned char>(token.text.ptr[i]);
            if (ch >= 'a' && ch <= 'z') ch -= 'a' - 'A';
            difference = static_cast<int>(ch) - static_cast<unsigned char>(word[i]);
            if (difference) break;
        }
        if (!difference) difference = i < token.text.len ? 1 : word[i] ? -1 : 0;
        if (!difference) return false;
        if (difference < 0) hi = mid;
        else lo = mid + 1;
    }
    return true;
}

} // namespace sql_parser
#endif // SQL_PARSER_MYSQL_IDENTIFIER_H
