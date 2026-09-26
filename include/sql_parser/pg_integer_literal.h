#ifndef SQL_PARSER_PG_INTEGER_LITERAL_H
#define SQL_PARSER_PG_INTEGER_LITERAL_H

#include "sql_parser/tokenizer.h"

namespace sql_parser {

// PostgreSQL ICONST is an unsigned magnitude fitting int32; unary signs belong
// to SignedIconst. The common tokenizer splits base prefixes and digit
// separators, so join only contiguous source tokens before validating them.
inline bool pg_integer_literal(Tokenizer<Dialect::PostgreSQL>& tok, Token& result) {
    Token first = tok.peek();
    if (first.type != TokenType::TK_INTEGER) return false;
    auto look = tok;
    look.skip();
    Token suffix = look.peek();
    char next = suffix.source.len ? suffix.source.ptr[0] : '\0';
    bool word_suffix = (next >= 'a' && next <= 'z') || (next >= 'A' && next <= 'Z') || next == '_';
    bool joined = suffix.source.ptr == first.source.ptr + first.source.len && word_suffix;
    uint32_t length = first.source.len + (joined ? suffix.source.len : 0);
    const char* text = first.source.ptr;
    uint32_t position = 0, base = 10, magnitude = 0;
    if (length > 2 && text[0] == '0') {
        switch (text[1]) {
            case 'x': case 'X': base = 16; break;
            case 'o': case 'O': base = 8; break;
            case 'b': case 'B': base = 2; break;
            default: break;
        }
        if (base != 10) {
            position = 2;
            if (text[position] == '_') ++position;
        }
    }
    bool digit_seen = false;
    for (; position < length; ++position) {
        char c = text[position];
        if (c == '_') {
            if (!digit_seen || position + 1 == length) return false;
            digit_seen = false; continue;
        }
        unsigned digit = c >= '0' && c <= '9' ? static_cast<unsigned>(c - '0') :
            c >= 'a' && c <= 'f' ? static_cast<unsigned>(c - 'a' + 10) :
            c >= 'A' && c <= 'F' ? static_cast<unsigned>(c - 'A' + 10) : 16;
        if (digit >= base || magnitude > (2147483647u - digit) / base) return false;
        magnitude = magnitude * base + digit;
        digit_seen = true;
    }
    if (!digit_seen) return false;
    result = first;
    result.source = result.text = {text, length};
    tok.skip();
    if (joined) tok.skip();
    return true;
}

} // namespace sql_parser
#endif // SQL_PARSER_PG_INTEGER_LITERAL_H
