#ifndef SQL_PARSER_TOKENIZER_H
#define SQL_PARSER_TOKENIZER_H

#include "sql_parser/token.h"
#include "sql_parser/keywords_mysql.h"
#include "sql_parser/keywords_pgsql.h"

namespace sql_parser {

template <Dialect D>
class Tokenizer {
public:
    void reset(const char* input, size_t len) {
        start_ = input;
        cursor_ = input;
        end_ = input + len;
        has_peeked_ = false;
        has_error_ = false;
        has_user_variables_ = false;
        error_source_ = {};
    }

    // True iff the tokenizer has emitted at least one TK_ERROR token since
    // the last reset(). Lets callers distinguish "couldn't build an AST"
    // from "the input was syntactically invalid" and surface the latter
    // as ParseResult::ERROR rather than PARTIAL.
    bool has_error() const { return has_error_; }
    bool has_user_variables() const { return has_user_variables_; }
    StringRef error_source() const { return error_source_; }

    // Hook for parser-level (non-tokenizer) errors -- when the parser
    // detects clearly invalid input (e.g. `SET = X`, `SET x = ;`,
    // `SET x = ,foo`) it can flag the error so the eventual ParseResult
    // is ERROR rather than PARTIAL with a null AST.
    void flag_error() { has_error_ = true; }
    void flag_error_at(StringRef source) {
        has_error_ = true;
        if (error_source_.empty()) error_source_ = source;
    }

    Token next_token() {
        if (has_peeked_) {
            has_peeked_ = false;
            return peeked_;
        }
        return scan_token();
    }

    Token peek() {
        if (!has_peeked_) {
            peeked_ = scan_token();
            has_peeked_ = true;
        }
        return peeked_;
    }

    void skip() {
        if (has_peeked_) {
            has_peeked_ = false;
        } else {
            scan_token();
        }
    }

    // Expose start/end of input for remaining-input calculation and for
    // safe lookback (e.g. detecting whether an identifier token was source-
    // delimited via a preceding backtick / double-quote byte).
    const char* input_begin() const { return start_; }
    const char* input_end() const { return end_; }

private:
    const char* start_ = nullptr;
    const char* cursor_ = nullptr;
    const char* end_ = nullptr;
    Token peeked_;
    bool has_peeked_ = false;
    bool has_error_ = false;
    bool has_user_variables_ = false;
    StringRef error_source_;

    uint32_t offset() const {
        return static_cast<uint32_t>(cursor_ - start_);
    }

    char current() const { return (cursor_ < end_) ? *cursor_ : '\0'; }
    char advance() {
        char c = current();
        if (cursor_ < end_) ++cursor_;
        return c;
    }
    char peek_char(size_t ahead = 0) const {
        const char* p = cursor_ + ahead;
        return (p < end_) ? *p : '\0';
    }

    void skip_whitespace_and_comments() {
        while (cursor_ < end_) {
            char c = *cursor_;

            // Whitespace
            if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
                ++cursor_;
                continue;
            }

            // -- line comment (MySQL requires space after --, PgSQL doesn't but we handle both)
            if (c == '-' && peek_char(1) == '-') {
                cursor_ += 2;
                while (cursor_ < end_ && *cursor_ != '\n') ++cursor_;
                continue;
            }

            // # line comment (MySQL only)
            if constexpr (D == Dialect::MySQL) {
                if (c == '#') {
                    ++cursor_;
                    while (cursor_ < end_ && *cursor_ != '\n') ++cursor_;
                    continue;
                }
            }

            // /* block comment */
            if (c == '/' && peek_char(1) == '*') {
                cursor_ += 2;
                if constexpr (D == Dialect::PostgreSQL) {
                    // PostgreSQL supports nested block comments
                    int depth = 1;
                    while (cursor_ < end_ && depth > 0) {
                        if (*cursor_ == '/' && peek_char(1) == '*') {
                            ++depth;
                            cursor_ += 2;
                        } else if (*cursor_ == '*' && peek_char(1) == '/') {
                            --depth;
                            cursor_ += 2;
                        } else {
                            ++cursor_;
                        }
                    }
                } else {
                    // MySQL: no nesting
                    while (cursor_ < end_) {
                        if (*cursor_ == '*' && peek_char(1) == '/') {
                            cursor_ += 2;
                            break;
                        }
                        ++cursor_;
                    }
                }
                continue;
            }

            break;  // not whitespace or comment
        }
    }

    Token make_token(TokenType type, const char* text_start, uint32_t text_len,
                     const char* source_start = nullptr, uint32_t source_len = 0) {
        if (!source_start) {
            source_start = text_start;
            source_len = text_len;
        }
        if (type == TokenType::TK_ERROR) {
            has_error_ = true;
            if (error_source_.empty()) error_source_ = StringRef{source_start, source_len};
        }
        if (type == TokenType::TK_USER_VARIABLE) has_user_variables_ = true;
        return Token{type, StringRef{text_start, text_len},
                     StringRef{source_start, source_len},
                     static_cast<uint32_t>(source_start - start_)};
    }

    Token scan_identifier_or_keyword() {
        const char* start = cursor_;
        while (cursor_ < end_) {
            char c = *cursor_;
            bool is_cont = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                           (c >= '0' && c <= '9') || c == '_';
            // PostgreSQL allows `$` as an identifier continuation char (but
            // not as the first char, which is enforced because $ at start
            // is handled by the $$ / $N branches in next_token_impl()).
            // e.g. `SET search_path = schema$1` — `schema$1` is a single
            // identifier, not `schema` followed by the placeholder `$1`.
            if (!is_cont && D == Dialect::PostgreSQL && c == '$' && cursor_ > start) {
                is_cont = true;
            }
            if (is_cont) {
                ++cursor_;
            } else {
                break;
            }
        }
        uint32_t len = static_cast<uint32_t>(cursor_ - start);

        // Keyword lookup
        TokenType kw;
        if constexpr (D == Dialect::MySQL) {
            kw = mysql_keywords::lookup(start, len);
        } else {
            kw = pgsql_keywords::lookup(start, len);
        }
        return make_token(kw, start, len);
    }

    Token scan_number() {
        const char* start = cursor_;
        bool has_dot = false;
        while (cursor_ < end_ && *cursor_ >= '0' && *cursor_ <= '9') ++cursor_;
        if (cursor_ < end_ && *cursor_ == '.') {
            has_dot = true;
            ++cursor_;
            while (cursor_ < end_ && *cursor_ >= '0' && *cursor_ <= '9') ++cursor_;
        }
        bool has_exponent = false;
        if (cursor_ < end_ && (*cursor_ == 'e' || *cursor_ == 'E')) {
            has_exponent = true;
            ++cursor_;
            if (cursor_ < end_ && (*cursor_ == '+' || *cursor_ == '-')) ++cursor_;
            const char* exponent_digits = cursor_;
            while (cursor_ < end_ && *cursor_ >= '0' && *cursor_ <= '9') ++cursor_;
            if (cursor_ == exponent_digits) {
                return make_token(TokenType::TK_ERROR, start,
                    static_cast<uint32_t>(cursor_ - start));
            }
        }
        uint32_t len = static_cast<uint32_t>(cursor_ - start);
        return make_token(has_dot || has_exponent ? TokenType::TK_FLOAT : TokenType::TK_INTEGER,
                          start, len);
    }

    static bool is_hex_digit(char c) {
        return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') ||
               (c >= 'A' && c <= 'F');
    }

    static bool is_token_word_char(char c) {
        return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') ||
               (c >= 'A' && c <= 'Z') || c == '_' || c == '$';
    }

    Token scan_prefixed_base_literal(bool hex) {
        const char* start = cursor_;
        cursor_ += 2;
        const char* digits = cursor_;
        while (cursor_ < end_ && (hex ? is_hex_digit(*cursor_) : (*cursor_ == '0' || *cursor_ == '1'))) {
            ++cursor_;
        }
        bool invalid = cursor_ == digits;
        if (cursor_ < end_ && is_token_word_char(*cursor_)) {
            invalid = true;
            while (cursor_ < end_ && is_token_word_char(*cursor_)) ++cursor_;
        }
        uint32_t len = static_cast<uint32_t>(cursor_ - start);
        return make_token(invalid ? TokenType::TK_ERROR :
                          (hex ? TokenType::TK_HEX_LITERAL : TokenType::TK_BIT_LITERAL),
                          start, len);
    }

    Token scan_quoted_base_literal(bool hex) {
        const char* start = cursor_;
        cursor_ += 2; // prefix and opening quote
        bool invalid = false;
        while (cursor_ < end_ && *cursor_ != '\'') {
            if (hex ? !is_hex_digit(*cursor_) : (*cursor_ != '0' && *cursor_ != '1')) invalid = true;
            ++cursor_;
        }
        if (cursor_ >= end_) {
            return make_token(TokenType::TK_ERROR, start,
                              static_cast<uint32_t>(cursor_ - start));
        }
        ++cursor_;
        uint32_t len = static_cast<uint32_t>(cursor_ - start);
        return make_token(invalid ? TokenType::TK_ERROR :
                          (hex ? TokenType::TK_HEX_LITERAL : TokenType::TK_BIT_LITERAL),
                          start, len);
    }

    Token scan_single_quoted_string() {
        const char* source_start = cursor_;
        ++cursor_;  // skip opening quote
        const char* content_start = cursor_;
        while (cursor_ < end_) {
            if (*cursor_ == '\'') {
                // Check for doubled single-quote escape ('')
                if (cursor_ + 1 < end_ && *(cursor_ + 1) == '\'') {
                    cursor_ += 2;  // skip both quotes
                    continue;
                }
                break;  // end of string
            }
            if (*cursor_ == '\\') {
                ++cursor_;  // skip escaped char
                if (cursor_ < end_) ++cursor_;
            } else {
                ++cursor_;
            }
        }
        uint32_t len = static_cast<uint32_t>(cursor_ - content_start);
        if (cursor_ >= end_) {
            return make_token(TokenType::TK_ERROR, source_start,
                              static_cast<uint32_t>(cursor_ - source_start));
        }
        ++cursor_;  // skip closing quote
        return make_token(TokenType::TK_STRING, content_start, len, source_start,
                          static_cast<uint32_t>(cursor_ - source_start));
    }

    Token scan_double_quoted_string() {
        const char* source_start = cursor_;
        ++cursor_;
        const char* content_start = cursor_;
        while (cursor_ < end_) {
            if (*cursor_ == '"') {
                if (cursor_ + 1 < end_ && cursor_[1] == '"') {
                    cursor_ += 2;
                    continue;
                }
                break;
            }
            if (*cursor_ == '\\') {
                ++cursor_;
                if (cursor_ < end_) ++cursor_;
            } else {
                ++cursor_;
            }
        }
        uint32_t len = static_cast<uint32_t>(cursor_ - content_start);
        if (cursor_ >= end_) {
            return make_token(TokenType::TK_ERROR, source_start,
                              static_cast<uint32_t>(cursor_ - source_start));
        }
        ++cursor_;
        return make_token(TokenType::TK_STRING, content_start, len, source_start,
                          static_cast<uint32_t>(cursor_ - source_start));
    }

    Token scan_mysql_user_variable() {
        has_user_variables_ = true;
        const char* source_start = cursor_;
        ++cursor_; // @
        if (cursor_ >= end_) return make_token(TokenType::TK_AT, source_start, 1);

        char delimiter = *cursor_;
        if (delimiter == '\'' || delimiter == '"' || delimiter == '`') {
            ++cursor_;
            const char* content_start = cursor_;
            while (cursor_ < end_) {
                if (*cursor_ == delimiter) {
                    if (cursor_ + 1 < end_ && cursor_[1] == delimiter) {
                        cursor_ += 2;
                        continue;
                    }
                    uint32_t text_len = static_cast<uint32_t>(cursor_ - content_start);
                    ++cursor_;
                    return make_token(TokenType::TK_USER_VARIABLE, content_start, text_len,
                                      source_start,
                                      static_cast<uint32_t>(cursor_ - source_start));
                }
                if (*cursor_ == '\\') {
                    ++cursor_;
                    if (cursor_ < end_) ++cursor_;
                } else {
                    ++cursor_;
                }
            }
            return make_token(TokenType::TK_ERROR, source_start,
                              static_cast<uint32_t>(cursor_ - source_start));
        }

        const char* name_start = cursor_;
        while (cursor_ < end_) {
            char c = *cursor_;
            if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                (c >= '0' && c <= '9') || c == '.' || c == '_' || c == '$') {
                ++cursor_;
            } else {
                break;
            }
        }
        if (cursor_ == name_start) return make_token(TokenType::TK_AT, source_start, 1);
        return make_token(TokenType::TK_USER_VARIABLE, name_start,
                          static_cast<uint32_t>(cursor_ - name_start), source_start,
                          static_cast<uint32_t>(cursor_ - source_start));
    }

    // MySQL: backtick-quoted identifier
    // Unclosed backticks emit TK_ERROR so the parser can fail cleanly with
    // ParseResult::ERROR -- otherwise the tokenizer would silently swallow
    // the rest of the input as one giant identifier (e.g. `SET x = `foo`
    // would become identifier `foo` followed by an unclosed-backtick scan
    // that consumes everything to EOF as another identifier).
    Token scan_backtick_identifier() {
        const char* open_pos = cursor_;
        ++cursor_;  // skip opening backtick
        const char* content_start = cursor_;
        while (cursor_ < end_ && *cursor_ != '`') ++cursor_;
        if (cursor_ >= end_) {
            return make_token(TokenType::TK_ERROR, open_pos, 1);
        }
        uint32_t len = static_cast<uint32_t>(cursor_ - content_start);
        ++cursor_;  // skip closing backtick
        return make_token(TokenType::TK_IDENTIFIER, content_start, len, open_pos,
                          static_cast<uint32_t>(cursor_ - open_pos));
    }

    // PostgreSQL: double-quoted identifier
    // Unclosed `"` emits TK_ERROR -- same rationale as scan_backtick_identifier
    // above. `SET search_path = "unclosed_quote, public` would otherwise be
    // treated as identifier `unclosed_quote, public` (commas, spaces and all),
    // pass validation, and corrupt search_path with garbage.
    Token scan_double_quoted_identifier() {
        const char* open_pos = cursor_;
        ++cursor_;  // skip opening quote
        const char* content_start = cursor_;
        while (cursor_ < end_ && *cursor_ != '"') ++cursor_;
        if (cursor_ >= end_) {
            return make_token(TokenType::TK_ERROR, open_pos, 1);
        }
        uint32_t len = static_cast<uint32_t>(cursor_ - content_start);
        ++cursor_;  // skip closing quote
        return make_token(TokenType::TK_IDENTIFIER, content_start, len, open_pos,
                          static_cast<uint32_t>(cursor_ - open_pos));
    }

    // PostgreSQL: $$...$$ dollar-quoted string
    Token scan_dollar_string() {
        // We're at the first $. Simple form: $$content$$
        cursor_ += 2;  // skip opening $$
        const char* content_start = cursor_;
        while (cursor_ < end_) {
            if (*cursor_ == '$' && peek_char(1) == '$') {
                uint32_t len = static_cast<uint32_t>(cursor_ - content_start);
                cursor_ += 2;  // skip closing $$
                return make_token(TokenType::TK_STRING, content_start, len);
            }
            ++cursor_;
        }
        // Unterminated — return what we have
        uint32_t len = static_cast<uint32_t>(cursor_ - content_start);
        return make_token(TokenType::TK_STRING, content_start, len);
    }

    Token scan_token() {
        skip_whitespace_and_comments();

        if (cursor_ >= end_) {
            return make_token(TokenType::TK_EOF, cursor_, 0);
        }

        char c = *cursor_;

        if constexpr (D == Dialect::MySQL) {
            if ((c == 'x' || c == 'X') && peek_char(1) == '\'') {
                return scan_quoted_base_literal(true);
            }
            if ((c == 'b' || c == 'B') && peek_char(1) == '\'') {
                return scan_quoted_base_literal(false);
            }
            if (c == '0' && (peek_char(1) == 'x' || peek_char(1) == 'X')) {
                return scan_prefixed_base_literal(true);
            }
            if (c == '0' && (peek_char(1) == 'b' || peek_char(1) == 'B')) {
                return scan_prefixed_base_literal(false);
            }
        }

        // Identifiers and keywords
        if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_') {
            return scan_identifier_or_keyword();
        }

        // Numbers
        if (c >= '0' && c <= '9') {
            return scan_number();
        }

        // Dot — could be start of .123 float or just dot
        if (c == '.' && cursor_ + 1 < end_ &&
            peek_char(1) >= '0' && peek_char(1) <= '9') {
            return scan_number();
        }

        // String literals
        if (c == '\'') return scan_single_quoted_string();

        // MySQL: double-quoted strings; PostgreSQL: double-quoted identifiers
        if (c == '"') {
            if constexpr (D == Dialect::MySQL) {
                return scan_double_quoted_string();
            } else {
                return scan_double_quoted_identifier();
            }
        }

        // Backtick identifier (MySQL only)
        if constexpr (D == Dialect::MySQL) {
            if (c == '`') return scan_backtick_identifier();
        }

        // @ and @@
        if (c == '@') {
            if (peek_char(1) == '@') {
                const char* s = cursor_;
                cursor_ += 2;
                return make_token(TokenType::TK_DOUBLE_AT, s, 2);
            }
            if constexpr (D == Dialect::MySQL) {
                return scan_mysql_user_variable();
            } else {
                const char* s = cursor_;
                ++cursor_;
                return make_token(TokenType::TK_AT, s, 1);
            }
        }

        // $ — PostgreSQL: $N placeholder or $$string$$
        if constexpr (D == Dialect::PostgreSQL) {
            if (c == '$') {
                if (peek_char(1) == '$') {
                    return scan_dollar_string();
                }
                if (peek_char(1) >= '0' && peek_char(1) <= '9') {
                    const char* start = cursor_;
                    ++cursor_;  // skip $
                    while (cursor_ < end_ && *cursor_ >= '0' && *cursor_ <= '9')
                        ++cursor_;
                    uint32_t len = static_cast<uint32_t>(cursor_ - start);
                    return make_token(TokenType::TK_DOLLAR_NUM, start, len);
                }
                // $<letter|underscore>... is NOT a valid PG token at this
                // position -- it looks like a parameter placeholder but is
                // syntactically invalid (placeholders must be numeric, e.g.
                // $1). Emit TK_ERROR so the caller can fail cleanly with
                // ParseResult::ERROR instead of returning PARTIAL with a
                // null AST and confusing downstream consumers.
                {
                    const char* start = cursor_;
                    ++cursor_;  // consume $ so error offset is precise
                    return make_token(TokenType::TK_ERROR, start, 1);
                }
            }
        }

        // Two-character operators
        if (cursor_ + 1 < end_) {
            char c2 = peek_char(1);

            if (c == '<' && c2 == '<') { auto s = cursor_; cursor_ += 2; return make_token(TokenType::TK_SHIFT_LEFT, s, 2); }
            if (c == '>' && c2 == '>') { auto s = cursor_; cursor_ += 2; return make_token(TokenType::TK_SHIFT_RIGHT, s, 2); }
            if (c == '<' && c2 == '=') { auto s = cursor_; cursor_ += 2; return make_token(TokenType::TK_LESS_EQUAL, s, 2); }
            if (c == '>' && c2 == '=') { auto s = cursor_; cursor_ += 2; return make_token(TokenType::TK_GREATER_EQUAL, s, 2); }
            if (c == '!' && c2 == '=') { auto s = cursor_; cursor_ += 2; return make_token(TokenType::TK_NOT_EQUAL, s, 2); }
            if (c == '<' && c2 == '>') { auto s = cursor_; cursor_ += 2; return make_token(TokenType::TK_NOT_EQUAL, s, 2); }
            if (c == '|' && c2 == '|') { auto s = cursor_; cursor_ += 2; return make_token(TokenType::TK_DOUBLE_PIPE, s, 2); }

            if constexpr (D == Dialect::MySQL) {
                if (c == ':' && c2 == '=') { auto s = cursor_; cursor_ += 2; return make_token(TokenType::TK_COLON_EQUAL, s, 2); }
            }

            if constexpr (D == Dialect::PostgreSQL) {
                if (c == ':' && c2 == ':') { auto s = cursor_; cursor_ += 2; return make_token(TokenType::TK_DOUBLE_COLON, s, 2); }
            }
        }

        // Single-character operators/punctuation
        const char* s = cursor_;
        ++cursor_;
        switch (c) {
            case '(': return make_token(TokenType::TK_LPAREN, s, 1);
            case ')': return make_token(TokenType::TK_RPAREN, s, 1);
            case '[': return make_token(TokenType::TK_LBRACKET, s, 1);
            case ']': return make_token(TokenType::TK_RBRACKET, s, 1);
            case ',': return make_token(TokenType::TK_COMMA, s, 1);
            case ';': return make_token(TokenType::TK_SEMICOLON, s, 1);
            case '.': return make_token(TokenType::TK_DOT, s, 1);
            case '*': return make_token(TokenType::TK_ASTERISK, s, 1);
            case '+': return make_token(TokenType::TK_PLUS, s, 1);
            case '-': return make_token(TokenType::TK_MINUS, s, 1);
            case '/': return make_token(TokenType::TK_SLASH, s, 1);
            case '%': return make_token(TokenType::TK_PERCENT, s, 1);
            case '=': return make_token(TokenType::TK_EQUAL, s, 1);
            case '<': return make_token(TokenType::TK_LESS, s, 1);
            case '>': return make_token(TokenType::TK_GREATER, s, 1);
            case '&': return make_token(TokenType::TK_AMPERSAND, s, 1);
            case '|': return make_token(TokenType::TK_PIPE, s, 1);
            case '^': return make_token(TokenType::TK_CARET, s, 1);
            case '~': return make_token(TokenType::TK_TILDE, s, 1);
            case '!': return make_token(TokenType::TK_EXCLAIM, s, 1);
            case ':': return make_token(TokenType::TK_COLON, s, 1);
            case '?': return make_token(TokenType::TK_QUESTION, s, 1);
            case '#': return make_token(TokenType::TK_HASH, s, 1);
            default:  return make_token(TokenType::TK_ERROR, s, 1);
        }
    }
};

} // namespace sql_parser

#endif // SQL_PARSER_TOKENIZER_H
