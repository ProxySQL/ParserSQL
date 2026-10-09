#ifndef SQL_PARSER_MYSQL_CHARSET_H
#define SQL_PARSER_MYSQL_CHARSET_H

#include "sql_parser/token.h"
#include <string_view>

namespace sql_parser {

// Native lexer introducers: the 41 SHOW CHARACTER SET names shared by the
// pinned MySQL 8.4.8 and 9.7.2 releases, plus their utf8 alias. Unknown _names
// remain identifiers; charset/collation operands use grammar validation instead.
inline bool mysql_charset_introducer(const Token& token) {
    if (token.type != TokenType::TK_IDENTIFIER || token.source.ptr != token.text.ptr ||
        token.text.len < 2 || token.text.ptr[0] != '_') return false;
    static constexpr std::string_view names[] = {
        "armscii8", "ascii", "big5", "binary", "cp1250", "cp1251",
        "cp1256", "cp1257", "cp850", "cp852", "cp866", "cp932",
        "dec8", "eucjpms", "euckr", "gb18030", "gb2312", "gbk",
        "geostd8", "greek", "hebrew", "hp8", "keybcs2", "koi8r",
        "koi8u", "latin1", "latin2", "latin5", "latin7", "macce",
        "macroman", "sjis", "swe7", "tis620", "ucs2", "ujis",
        "utf16", "utf16le", "utf32", "utf8mb3", "utf8mb4", "utf8",
    };
    for (auto name : names)
        if (token.text.len == name.size() + 1 &&
            StringRef{token.text.ptr + 1, token.text.len - 1}.equals_ci(name.data(), name.size())) return true;
    return false;
}

} // namespace sql_parser
#endif // SQL_PARSER_MYSQL_CHARSET_H
