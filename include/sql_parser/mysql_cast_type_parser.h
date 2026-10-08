#ifndef SQL_PARSER_MYSQL_CAST_TYPE_PARSER_H
#define SQL_PARSER_MYSQL_CAST_TYPE_PARSER_H

#include "sql_parser/tokenizer.h"
#include "sql_parser/mysql_identifier.h"
#include "sql_parser/mysql_charset.h"

namespace sql_parser {

class MySQLCastTypeParser {
public:
    explicit MySQLCastTypeParser(Tokenizer<Dialect::MySQL>& tok) : tok_(tok) {}

    // MySQL's cast_type is deliberately narrower than a column data type.
    // Preserve validated syntax, including modifiers, without treating it as
    // a value expression or allowing arbitrary catalog type names.
    StringRef parse() {
        const Token first = tok_.peek();
        if (word(first, "CHAR") || word(first, "CHARACTER")) {
            take();
            if (!modifiers(1, true) || !character_attributes()) return fail();
        } else if (word(first, "NCHAR") || word(first, "NATIONAL")) {
            take();
            if (word(first, "NATIONAL")) {
                if (!character_word(tok_.peek())) return fail();
                take();
            }
            if (!modifiers(1, true)) return fail();
        } else if (word(first, "BINARY") || word(first, "FLOAT") || word(first, "FLOAT4")) {
            take();
            if (!modifiers(1, true)) return fail();
        } else if (word(first, "DECIMAL") || word(first, "DEC")) {
            take();
            if (!modifiers(2, true)) return fail();
        } else if (word(first, "TIME") || word(first, "DATETIME")) {
            take();
            if (!modifiers(1, false)) return fail();
        } else if (word(first, "SIGNED") || word(first, "UNSIGNED")) {
            take();
            if (word(tok_.peek(), "INT") || word(tok_.peek(), "INTEGER") || word(tok_.peek(), "INT4")) take();
        } else if (word(first, "DOUBLE") || word(first, "FLOAT8")) {
            take();
            if (word(tok_.peek(), "PRECISION")) take();
        } else if (word(first, "DATE") || word(first, "YEAR") ||
                   word(first, "REAL") || word(first, "JSON") ||
                   word(first, "POINT") || word(first, "LINESTRING") ||
                   word(first, "POLYGON") || word(first, "MULTIPOINT") ||
                   word(first, "MULTILINESTRING") || word(first, "MULTIPOLYGON") ||
                   word(first, "GEOMETRYCOLLECTION") || word(first, "GEOMCOLLECTION")) {
            take();
        } else {
            return fail();
        }
        if (tok_.has_error()) return fail();
        return {first.source.ptr, static_cast<uint32_t>(last_.ptr + last_.len - first.source.ptr)};
    }

private:
    Tokenizer<Dialect::MySQL>& tok_;
    StringRef last_;

    static bool word(const Token& token, std::string_view value) {
        return token.source.ptr == token.text.ptr &&
            token.text.equals_ci(value.data(), static_cast<uint32_t>(value.size()));
    }

    static bool character_word(const Token& token) {
        return word(token, "CHAR") || word(token, "CHARACTER");
    }

    void take() { last_ = tok_.next_token().source; }

    StringRef fail() {
        tok_.flag_fatal_error_at(tok_.peek().source);
        return {};
    }

    // field_length accepts NUM, LONG_NUM, ULONGLONG_NUM and DECIMAL_NUM.
    // The two-argument precision production and datetime precision use NUM.
    // Check their spelling rather than TK_FLOAT alone, which also covers
    // exponent notation. NUM ends at INT32_MAX; other numeric ranges are
    // server semantic checks, beyond the lexer token distinction.
    static bool numeric_modifier(const Token& token, bool decimal) {
        if (token.type != TokenType::TK_INTEGER && token.type != TokenType::TK_FLOAT) return false;
        bool digit = false;
        bool dot = false;
        uint32_t integer = 0;
        for (uint32_t i = 0; i < token.source.len; ++i) {
            const char c = token.source.ptr[i];
            if (c >= '0' && c <= '9') {
                digit = true;
                if (!decimal) {
                    const auto value = static_cast<uint32_t>(c - '0');
                    // Native NUM excludes LONG_NUM and larger numeric tokens.
                    if (integer > (2147483647u - value) / 10) return false;
                    integer = integer * 10 + value;
                }
            }
            else if (decimal && c == '.' && !dot) dot = true;
            else return false;
        }
        return digit;
    }

    bool modifiers(unsigned maximum, bool decimal) {
        if (tok_.peek().type != TokenType::TK_LPAREN) return true;
        take();
        const Token first = tok_.peek();
        if (!numeric_modifier(first, decimal)) return false;
        take();
        if (tok_.peek().type == TokenType::TK_COMMA) {
            if (maximum < 2 || !numeric_modifier(first, false)) return false;
            take();
            if (!numeric_modifier(tok_.peek(), false)) return false;
            take();
        }
        if (tok_.peek().type != TokenType::TK_RPAREN) return false;
        take();
        return true;
    }

    bool charset_clause() {
        if (character_word(tok_.peek())) {
            take();
            if (!word(tok_.peek(), "SET")) return false;
            take();
        } else {
            // The caller has recognized CHARSET.
            take();
        }
        const Token name = tok_.peek();
        if (mysql_charset_introducer(name) ||
            (!mysql_identifier_token(name) && name.type != TokenType::TK_STRING &&
             !word(name, "BINARY"))) return false;
        take();
        return true;
    }

    bool character_attributes() {
        const bool leading_binary = word(tok_.peek(), "BINARY");
        if (leading_binary) take();
        if (word(tok_.peek(), "ASCII") || word(tok_.peek(), "UNICODE")) {
            take();
            if (!leading_binary && word(tok_.peek(), "BINARY")) take();
        } else if (character_word(tok_.peek()) || word(tok_.peek(), "CHARSET")) {
            if (!charset_clause()) return false;
            if (!leading_binary && word(tok_.peek(), "BINARY")) take();
        } else if (!leading_binary && word(tok_.peek(), "BYTE")) {
            take();
        }
        return true;
    }
};

} // namespace sql_parser
#endif
