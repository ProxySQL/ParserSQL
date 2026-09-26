#ifndef SQL_PARSER_PG_UTILITY_PARSER_H
#define SQL_PARSER_PG_UTILITY_PARSER_H

#include "sql_parser/compound_query_parser.h"
#include "sql_parser/subquery_parse_callback.h"
#include "sql_parser/parse_result.h"

namespace sql_parser {

// A deliberately bounded PostgreSQL utility grammar. Unknown tails remain
// unconsumed for the normal parser completeness check.
class PgUtilityParser {
public:
    PgUtilityParser(Tokenizer<Dialect::PostgreSQL>& tok, Arena& arena)
        : tok_(tok), arena_(arena) {}

    static bool word(const Token& token, const char* value) {
        if (token.type == TokenType::TK_STRING || token.type == TokenType::TK_EOF ||
            (token.source.len && token.source.ptr[0] == '"')) return false;
        return token.text.equals_ci(value, static_cast<uint32_t>(std::strlen(value)));
    }

    ParseResult transaction(const Token& first) {
        ParseResult result;
        const bool start = word(first, "START");
        const bool begin = word(first, "BEGIN") || start;
        const bool rollback = word(first, "ROLLBACK") || word(first, "ABORT");
        const bool commit = word(first, "COMMIT") || word(first, "END");
        const bool prepare = word(first, "PREPARE");
        const bool save = word(first, "SAVEPOINT");
        result.stmt_type = begin ? (start ? StmtType::START_TRANSACTION : StmtType::BEGIN)
            : rollback ? StmtType::ROLLBACK : commit ? StmtType::COMMIT
            : prepare ? StmtType::PREPARE : save ? StmtType::SAVEPOINT
            : StmtType::RELEASE_SAVEPOINT;
        AstNode* root = node(NodeType::NODE_TRANSACTION_STMT,
            start ? "START TRANSACTION" : begin ? "BEGIN" : rollback ? "ROLLBACK"
            : commit ? "COMMIT" : prepare ? "PREPARE TRANSACTION"
            : save ? "SAVEPOINT" : "RELEASE SAVEPOINT");
        bool work = false;
        if (start || prepare) require("TRANSACTION");
        else if (begin || rollback || commit) work = take("WORK") || take("TRANSACTION");

        if (begin) {
            bool had_option = false;
            while (!failed_) {
                const char* option = nullptr;
                if (take("ISOLATION")) {
                    require("LEVEL");
                    if (take("SERIALIZABLE")) option = "ISOLATION LEVEL SERIALIZABLE";
                    else if (take("REPEATABLE")) { require("READ"); option = "ISOLATION LEVEL REPEATABLE READ"; }
                    else if (take("READ")) {
                        if (take("COMMITTED")) option = "ISOLATION LEVEL READ COMMITTED";
                        else { require("UNCOMMITTED"); option = "ISOLATION LEVEL READ UNCOMMITTED"; }
                    } else fail();
                } else if (take("READ")) {
                    if (take("ONLY")) option = "READ ONLY";
                    else { require("WRITE"); option = "READ WRITE"; }
                } else if (take("NOT")) {
                    require("DEFERRABLE"); option = "NOT DEFERRABLE";
                } else if (take("DEFERRABLE")) option = "DEFERRABLE";
                else {
                    if (had_option) fail(); // comma without a following option
                    break;
                }
                if (failed_) break;
                append(root, node(NodeType::NODE_TRANSACTION_OPTION, option));
                had_option = tok_.peek().type == TokenType::TK_COMMA;
                if (had_option) tok_.skip();
            }
        } else if (prepare || ((rollback || commit) && take("PREPARED"))) {
            if (work) fail();
            if (!prepare) append(root, node(NodeType::NODE_TRANSACTION_OPTION, "PREPARED"));
            append(root, string_value());
        } else if (save || (!rollback && !commit)) {
            if (!save) take("SAVEPOINT");
            append(root, identifier());
        } else if (rollback && take("TO")) {
            take("SAVEPOINT");
            append(root, node(NodeType::NODE_TRANSACTION_OPTION, "TO SAVEPOINT"));
            append(root, identifier());
        } else if (take("AND")) {
            bool no = take("NO");
            require("CHAIN");
            append(root, node(NodeType::NODE_TRANSACTION_OPTION, no ? "AND NO CHAIN" : "AND CHAIN"));
        }
        result.ast = root;
        result.status = failed_ ? ParseResult::ERROR : ParseResult::OK;
        return result;
    }

    ParseResult copy() {
        ParseResult result;
        result.stmt_type = StmtType::COPY;
        AstNode* root = node(NodeType::NODE_COPY_STMT, "");
        bool query = tok_.peek().type == TokenType::TK_LPAREN;
        if (query) {
            tok_.skip();
            CompoundQueryParser<Dialect::PostgreSQL> parser(tok_, arena_, true);
            parser.set_subquery_callback(&parse_strict_subquery);
            AstNode* inner = parser.parse(TokenType::TK_EOF);
            AstNode* subquery = node(NodeType::NODE_SUBQUERY, "");
            append(subquery, inner);
            append(root, subquery);
            require(TokenType::TK_RPAREN);
        } else {
            AstNode* table = node(NodeType::NODE_TABLE_REF, "");
            AstNode* name = identifier();
            if (tok_.peek().type == TokenType::TK_DOT) {
                tok_.skip();
                AstNode* qualified = node(NodeType::NODE_QUALIFIED_NAME, "");
                append(qualified, name);
                append(qualified, identifier());
                name = qualified;
            }
            append(table, name);
            append(root, table);
            if (tok_.peek().type == TokenType::TK_LPAREN)
                append(root, identifier_list(NodeType::NODE_INSERT_COLUMNS));
        }
        bool from = take("FROM");
        if (!from) require("TO");
        if (root) root->set_value(from ? StringRef{"FROM", 4} : StringRef{"TO", 2});
        if (query && from) fail();
        AstNode* endpoint = nullptr;
        if (take("STDIN")) { endpoint = node(NodeType::NODE_COPY_ENDPOINT, "STDIN"); if (!from) fail(); }
        else if (take("STDOUT")) { endpoint = node(NodeType::NODE_COPY_ENDPOINT, "STDOUT"); if (from) fail(); }
        else {
            bool program = take("PROGRAM");
            endpoint = node(NodeType::NODE_COPY_ENDPOINT, program ? "PROGRAM" : "FILE");
            append(endpoint, string_value());
        }
        append(root, endpoint);
        bool with = take("WITH");
        if (tok_.peek().type == TokenType::TK_LPAREN) {
            tok_.skip();
            do {
                append(root, copy_option());
                if (failed_ || tok_.peek().type != TokenType::TK_COMMA) break;
                tok_.skip();
            } while (true);
            require(TokenType::TK_RPAREN);
        } else if (with) fail();
        if (take("WHERE")) {
            if (!from) fail();
            ExpressionParser<Dialect::PostgreSQL> expr(tok_, arena_, true);
            expr.set_subquery_callback(&parse_strict_subquery);
            AstNode* where = node(NodeType::NODE_WHERE_CLAUSE, "");
            append(where, expr.parse());
            append(root, where);
        }
        result.ast = root;
        result.status = failed_ ? ParseResult::ERROR : ParseResult::OK;
        return result;
    }

private:
    Tokenizer<Dialect::PostgreSQL>& tok_;
    Arena& arena_;
    bool failed_ = false;

    static AstNode* parse_strict_subquery(Tokenizer<Dialect::PostgreSQL>& tok, Arena& arena) {
        CompoundQueryParser<Dialect::PostgreSQL> parser(tok, arena, true);
        parser.set_subquery_callback(&parse_strict_subquery);
        return parser.parse(TokenType::TK_EOF);
    }

    void fail() { failed_ = true; tok_.flag_error_at(tok_.peek().source); }
    bool take(const char* value) {
        if (!word(tok_.peek(), value)) return false;
        tok_.skip(); return true;
    }
    void require(const char* value) { if (!take(value)) fail(); }
    void require(TokenType type) {
        if (tok_.peek().type != type) fail(); else tok_.skip();
    }
    AstNode* node(NodeType type, const char* value) {
        AstNode* n = make_node(arena_, type, StringRef{value, static_cast<uint32_t>(std::strlen(value))});
        if (!n) fail();
        return n;
    }
    AstNode* token_node(NodeType type, Token token) {
        AstNode* n = make_node_from_token(arena_, type, token);
        if (!n) fail();
        else if (type == NodeType::NODE_IDENTIFIER && token.source.len && token.source.ptr[0] == '"')
            n->flags |= FLAG_IDENT_DELIMITED;
        return n;
    }
    void append(AstNode* parent, AstNode* child) {
        if (!parent || !child) { fail(); return; }
        parent->add_child(child);
    }
    AstNode* string_value() {
        Token value = tok_.peek();
        if (word(value, "E")) {
            tok_.skip();
            Token quoted = tok_.peek();
            if (quoted.type != TokenType::TK_STRING ||
                quoted.source.ptr != value.source.ptr + value.source.len ||
                !quoted.source.len || quoted.source.ptr[0] != '\'') {
                fail(); return nullptr;
            }
            tok_.skip();
            quoted.source = StringRef{value.source.ptr, value.source.len + quoted.source.len};
            return token_node(NodeType::NODE_LITERAL_STRING, quoted);
        }
        if (value.type != TokenType::TK_STRING) { fail(); return nullptr; }
        tok_.skip();
        return token_node(NodeType::NODE_LITERAL_STRING, value);
    }
    AstNode* identifier() {
        Token t = tok_.peek();
        // Non-reserved keyword identifiers are accepted by the tokenizer as
        // keywords; reserved structural words are intentionally excluded here.
        bool valid = t.type == TokenType::TK_IDENTIFIER || t.type == TokenType::TK_KEY;
        if (!valid) { fail(); return nullptr; }
        tok_.skip();
        return token_node(NodeType::NODE_IDENTIFIER, t);
    }
    AstNode* identifier_list(NodeType type) {
        require(TokenType::TK_LPAREN);
        AstNode* list = node(type, "");
        do {
            append(list, identifier());
            if (failed_ || tok_.peek().type != TokenType::TK_COMMA) break;
            tok_.skip();
        } while (true);
        require(TokenType::TK_RPAREN);
        return list;
    }
    AstNode* copy_option() {
        Token key = tok_.peek();
        static const char* names[] = {"FORMAT", "FREEZE", "DELIMITER", "NULL", "DEFAULT",
            "HEADER", "QUOTE", "ESCAPE", "FORCE_QUOTE", "FORCE_NOT_NULL", "FORCE_NULL",
            "ENCODING", "ON_ERROR", "REJECT_LIMIT", "LOG_VERBOSITY"};
        const char* matched = nullptr;
        for (const char* candidate : names) if (word(key, candidate)) { matched = candidate; break; }
        if (!matched) { fail(); return nullptr; }
        tok_.skip();
        AstNode* option = node(NodeType::NODE_COPY_OPTION, matched);
        if (word(key, "FORCE_QUOTE") || word(key, "FORCE_NOT_NULL") || word(key, "FORCE_NULL")) {
            if (tok_.peek().type == TokenType::TK_ASTERISK) {
                tok_.skip(); append(option, node(NodeType::NODE_ASTERISK, "*"));
            } else append(option, identifier_list(NodeType::NODE_TUPLE));
        } else {
            Token value = tok_.peek();
            bool optional = word(key, "HEADER") || word(key, "FREEZE");
            if (optional && (value.type == TokenType::TK_COMMA || value.type == TokenType::TK_RPAREN)) return option;
            if (value.type == TokenType::TK_STRING || word(value, "E")) {
                append(option, string_value());
            } else if (value.type == TokenType::TK_INTEGER || value.type == TokenType::TK_IDENTIFIER ||
                       value.type == TokenType::TK_TRUE || value.type == TokenType::TK_FALSE ||
                       value.type == TokenType::TK_ON || value.type == TokenType::TK_IGNORE ||
                       value.type == TokenType::TK_VERBOSE || value.type == TokenType::TK_DEFAULT) {
                tok_.skip(); append(option, token_node(value.type == TokenType::TK_INTEGER
                    ? NodeType::NODE_LITERAL_INT : NodeType::NODE_IDENTIFIER, value));
            } else fail();
        }
        return option;
    }
};

} // namespace sql_parser
#endif
