#ifndef SQL_PARSER_PG_TYPE_PARSER_H
#define SQL_PARSER_PG_TYPE_PARSER_H

#include "sql_parser/tokenizer.h"

namespace sql_parser {

// A type is syntax, not a value expression. Retain its validated spelling so
// modifiers, quoted names and array bounds never become bind parameters.
class PgTypeParser {
public:
    explicit PgTypeParser(Tokenizer<Dialect::PostgreSQL>& tok) : tok_(tok) {}

    static bool word(const Token& token, const char* value) {
        return token.source.ptr == token.text.ptr &&
            token.text.equals_ci(value, static_cast<uint32_t>(std::strlen(value)));
    }

    static bool name_token(const Token& token) {
        return token.type == TokenType::TK_IDENTIFIER ||
            token.type == TokenType::TK_CHARACTER || token.type == TokenType::TK_INTERVAL ||
            token.type == TokenType::TK_DATA || token.type == TokenType::TK_SCHEMA;
    }

    StringRef parse(bool arrays = true, bool constant = false, const Token* leading = nullptr) {
        Token first = leading ? *leading : tok_.peek();
        if (!name_token(first)) return {};
        if (leading) last_ = first.source;
        else take();
        bool qualified = false;
        while (tok_.peek().type == TokenType::TK_DOT) {
            qualified = true;
            take();
            // PostgreSQL accepts every keyword as a label after a dot.
            Token component = tok_.peek();
            bool keyword_label = component.source.ptr == component.text.ptr && component.text.len &&
                ((component.text.ptr[0] >= 'A' && component.text.ptr[0] <= 'Z') ||
                 (component.text.ptr[0] >= 'a' && component.text.ptr[0] <= 'z'));
            if (component.type != TokenType::TK_IDENTIFIER && !keyword_label) return {};
            take();
        }
        const bool builtin = !qualified && first.source.ptr == first.text.ptr;
        bool datetime = builtin && (word(first, "TIME") || word(first, "TIMESTAMP"));
        bool interval = builtin && word(first, "INTERVAL");
        bool character = builtin && (word(first, "CHAR") || word(first, "CHARACTER") ||
            word(first, "NCHAR") || word(first, "NATIONAL"));
        if (builtin && word(first, "DOUBLE")) {
            if (!word(tok_.peek(), "PRECISION")) return {};
            take();
        }
        if (character && word(first, "NATIONAL")) {
            if (!word(tok_.peek(), "CHAR") && !word(tok_.peek(), "CHARACTER")) return {};
            take();
        }
        bool varying = (character || (builtin && word(first, "BIT"))) && word(tok_.peek(), "VARYING");
        if (varying) take();
        const bool has_modifiers = tok_.peek().type == TokenType::TK_LPAREN;
        if (has_modifiers && builtin && (word(first, "INT") || word(first, "INTEGER") ||
            word(first, "SMALLINT") || word(first, "BIGINT") || word(first, "BOOLEAN") ||
            word(first, "REAL") || word(first, "DOUBLE"))) return {};
        const bool single_modifier = datetime || interval || character ||
            (builtin && (word(first, "VARCHAR") || word(first, "FLOAT")));
        if (!modifiers(single_modifier ? 1 : 0, !single_modifier)) return {};
        if (datetime && (word(tok_.peek(), "WITH") || word(tok_.peek(), "WITHOUT"))) {
            take();
            if (!word(tok_.peek(), "TIME")) return {};
            take();
            if (!word(tok_.peek(), "ZONE")) return {};
            take();
        }
        if (interval) {
            int begin = interval_field(tok_.peek());
            if (begin >= 0) {
                if (has_modifiers) return {};
                take();
                int end = begin;
                if (tok_.peek().type == TokenType::TK_TO) {
                    take();
                    end = interval_field(tok_.peek());
                    if (!((begin == 0 && end == 1) || (begin >= 2 && end > begin))) return {};
                    take();
                }
                if (end == 5 && !modifiers(1, false)) return {};
            }
        }
        if (arrays) {
            bool standard = tok_.peek().type == TokenType::TK_ARRAY;
            if (standard) take();
            while (tok_.peek().type == TokenType::TK_LBRACKET) {
                take();
                if (tok_.peek().type == TokenType::TK_INTEGER) take();
                else if (standard) return {};
                if (tok_.peek().type != TokenType::TK_RBRACKET) return {};
                take();
                if (standard) break;
            }
        }
        // Prefix CHAR/BIT constants default to unconstrained length, unlike
        // CAST's SQL builtin spelling. Generic catalog names preserve that.
        if (constant && !has_modifiers && !varying) {
            if (character) return {"pg_catalog.bpchar", 17};
            if (builtin && word(first, "BIT")) return {"pg_catalog.bit", 14};
        }
        return {first.source.ptr, static_cast<uint32_t>(last_.ptr + last_.len - first.source.ptr)};
    }

private:
    Tokenizer<Dialect::PostgreSQL>& tok_;
    StringRef last_;
    void take() { last_ = tok_.next_token().source; }

    bool modifiers(unsigned maximum = 0, bool signs = true) {
        if (tok_.peek().type != TokenType::TK_LPAREN) return true;
        take();
        unsigned count = 0;
        do {
            if (maximum && ++count > maximum) return false;
            if (signs && (tok_.peek().type == TokenType::TK_PLUS || tok_.peek().type == TokenType::TK_MINUS)) take();
            if (tok_.peek().type != TokenType::TK_INTEGER) return false;
            take();
            if (tok_.peek().type != TokenType::TK_COMMA) break;
            take();
        } while (true);
        if (tok_.peek().type != TokenType::TK_RPAREN) return false;
        take();
        return true;
    }

    static int interval_field(const Token& token) {
        const char* fields[] = {"YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND"};
        for (int i = 0; i < 6; ++i) if (word(token, fields[i])) return i;
        return -1;
    }
};

} // namespace sql_parser
#endif
