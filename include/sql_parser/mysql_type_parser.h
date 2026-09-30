#ifndef SQL_PARSER_MYSQL_TYPE_PARSER_H
#define SQL_PARSER_MYSQL_TYPE_PARSER_H

#include "sql_parser/tokenizer.h"
#include <initializer_list>
#include "sql_parser/mysql_identifier.h"
#include "sql_parser/mysql_charset.h"

namespace sql_parser {

class MySQLTypeParser {
public:
    explicit MySQLTypeParser(Tokenizer<Dialect::MySQL>& tok) : tok_(tok) {}

    // Native MySQL column/routine/JSON_TABLE type production. COLLATE is a
    // separate column attribute. Catalog type names and opaque suffixes are
    // never accepted. Numeric ranges remain server semantic checks.
    StringRef parse() {
        const Token first = tok_.peek();
        bool numeric = false, characters = false, national = false;
        if (one_of({"INT", "INTEGER", "INT1", "INT2", "INT3", "INT4", "INT8",
                    "TINYINT", "SMALLINT", "MEDIUMINT", "MIDDLEINT", "BIGINT", "YEAR"})) {
            take(); numeric = true;
            if (!modifiers(1, true)) return fail();
        } else if (one_of({"DOUBLE", "FLOAT8", "REAL"})) {
            bool real = is("REAL"); take();
            if (!real && is("PRECISION")) take();
            numeric = true;
            if (tok_.peek().type == TokenType::TK_LPAREN && !precision()) return fail();
        } else if (one_of({"DECIMAL", "DEC", "NUMERIC", "FIXED", "FLOAT", "FLOAT4"})) {
            take(); numeric = true;
            if (!modifiers(2, true)) return fail();
        } else if (one_of({"TIME", "TIMESTAMP", "DATETIME"})) {
            take(); if (!modifiers(1, false)) return fail();
        } else if (one_of({"BIT", "BINARY", "BLOB", "VECTOR"})) {
            take(); if (!modifiers(1, true)) return fail();
        } else if (is("VARBINARY")) {
            take();
            if (tok_.peek().type != TokenType::TK_LPAREN || !modifiers(1, true)) return fail();
        } else if (one_of({"CHAR", "CHARACTER", "VARCHAR", "VARCHARACTER", "NCHAR", "NVARCHAR", "NATIONAL"})) {
            bool varying = one_of({"VARCHAR", "VARCHARACTER", "NVARCHAR"});
            national = one_of({"NCHAR", "NVARCHAR", "NATIONAL"});
            bool national_prefix = is("NATIONAL"), nchar = is("NCHAR");
            take();
            if (national_prefix) {
                varying = one_of({"VARCHAR", "VARCHARACTER"});
                if (!varying && !character_word(tok_.peek())) return fail();
                take();
            }
            if (!varying && (is("VARYING") || (nchar && one_of({"VARCHAR", "VARCHARACTER"})))) {
                varying = true; take();
            }
            if (varying && tok_.peek().type != TokenType::TK_LPAREN) return fail();
            if (!modifiers(1, true)) return fail();
            characters = true;
        } else if (one_of({"TEXT", "TINYTEXT", "MEDIUMTEXT", "LONGTEXT"})) {
            bool length = is("TEXT"); take(); characters = true;
            if (length && !modifiers(1, true)) return fail();
        } else if (is("LONG")) {
            take();
            if (is("VARBINARY")) take();
            else {
                if (one_of({"VARCHAR", "VARCHARACTER"})) take();
                else if (character_word(tok_.peek())) { take(); if (!is("VARYING")) return fail(); take(); }
                characters = true;
            }
        } else if (one_of({"ENUM", "SET"})) {
            take();
            if (tok_.peek().type != TokenType::TK_LPAREN) return fail();
            take();
            do {
                if (tok_.peek().type != TokenType::TK_STRING) return fail();
                take();
                if (tok_.peek().type != TokenType::TK_COMMA) break;
                take();
            } while (true);
            if (tok_.peek().type != TokenType::TK_RPAREN) return fail();
            take(); characters = true;
        } else if (one_of({"BOOL", "BOOLEAN", "DATE", "TINYBLOB", "MEDIUMBLOB", "LONGBLOB", "SERIAL", "JSON",
                           "GEOMETRY", "POINT", "LINESTRING", "POLYGON", "MULTIPOINT", "MULTILINESTRING",
                           "MULTIPOLYGON", "GEOMETRYCOLLECTION", "GEOMCOLLECTION"})) {
            take();
        } else return fail();
        if (numeric) while (one_of({"SIGNED", "UNSIGNED", "ZEROFILL"})) take();
        if (characters) {
            if (national) { if (is("BINARY")) take(); }
            else if (!character_attributes()) return fail();
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

    bool is(const char* value) { return word(tok_.peek(), value); }
    bool one_of(std::initializer_list<const char*> values) {
        for (const char* value : values) if (is(value)) return true;
        return false;
    }
    bool precision() {
        take();
        if (!numeric_modifier(tok_.peek(), false)) return false;
        take();
        if (tok_.peek().type != TokenType::TK_COMMA) return false;
        take();
        if (!numeric_modifier(tok_.peek(), false)) return false;
        take();
        if (tok_.peek().type != TokenType::TK_RPAREN) return false;
        take(); return true;
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
