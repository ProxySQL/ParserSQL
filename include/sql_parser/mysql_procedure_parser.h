#ifndef SQL_PARSER_MYSQL_PROCEDURE_PARSER_H
#define SQL_PARSER_MYSQL_PROCEDURE_PARSER_H

#include "sql_parser/mysql_type_parser.h"
#include "sql_parser/subquery_parse_callback.h"
#include "sql_parser/insert_parser.h"
#include "sql_parser/user_variable.h"
#include "sql_parser/mysql_value_syntax.h"

namespace sql_parser {

// A bounded SQL routine grammar. The body contains ordinary statement ASTs,
// never a source-text tail. Declarations, labels and procedural control flow
// are deliberately unsupported; encountering one is a parse error.
class MySQLProcedureParser {
public:
    MySQLProcedureParser(Tokenizer<Dialect::MySQL>& tok, Arena& arena,
                         SubqueryParseCallback<Dialect::MySQL> callback = nullptr)
        : tok_(tok), arena_(arena), callback_(callback ? callback :
              &parse_subquery_select<Dialect::MySQL>) {}

    static bool handles(const Token& token) { return word(token, "PROCEDURE"); }

    // CREATE has been consumed. Leave the outer statement semicolon to the
    // caller, while consuming every semicolon belonging to a compound body.
    AstNode* parse() {
        if (!take("PROCEDURE")) return fail();
        auto* root = node(NodeType::NODE_MYSQL_CREATE_PROCEDURE);
        auto* name = identifier();
        if (!root || !name) return fail();
        if (take(TokenType::TK_DOT)) {
            auto* qualified = node(NodeType::NODE_QUALIFIED_NAME);
            auto* second = identifier(true);
            if (!qualified || !second) return fail();
            qualified->add_child(name);
            qualified->add_child(second);
            name = qualified;
        }
        root->add_child(name);
        auto* params = parameters();
        if (!params) return fail();
        root->add_child(params);
        while (characteristic_start()) {
            auto* item = characteristic();
            if (!item) return fail();
            root->add_child(item);
        }
        auto* body = statement(0);
        if (!body || tok_.has_error()) return fail();
        root->add_child(body);
        if (tok_.peek().type != TokenType::TK_EOF &&
            tok_.peek().type != TokenType::TK_SEMICOLON) return fail();
        return root;
    }

private:
    Tokenizer<Dialect::MySQL>& tok_;
    Arena& arena_;
    SubqueryParseCallback<Dialect::MySQL> callback_;

    static bool word(const Token& token, const char* value) {
        return token.source.ptr == token.text.ptr &&
            token.text.equals_ci(value, static_cast<uint32_t>(std::strlen(value)));
    }
    bool is(const char* value) { return word(tok_.peek(), value); }
    bool take(const char* value) {
        if (!is(value)) return false;
        tok_.skip(); return true;
    }
    bool take(TokenType type) {
        if (tok_.peek().type != type) return false;
        tok_.skip(); return true;
    }
    AstNode* fail() { tok_.flag_error_at(tok_.peek().source); return nullptr; }
    AstNode* node(NodeType type, const char* value = "") {
        auto* out = make_node(arena_, type,
            {value, static_cast<uint32_t>(std::strlen(value))});
        if (!out) return fail();
        return out;
    }
    AstNode* identifier(bool qualified = false) {
        const Token token = tok_.peek();
        if (qualified ? !mysql_identifier_word(token) : !mysql_identifier_token(token))
            return fail();
        tok_.skip();
        auto* out = make_node_from_token(arena_, NodeType::NODE_IDENTIFIER, token,
            token.source.ptr != token.text.ptr ? FLAG_IDENT_DELIMITED : 0);
        return out ? out : fail();
    }
    AstNode* parameters() {
        if (!take(TokenType::TK_LPAREN)) return fail();
        auto* list = node(NodeType::NODE_MYSQL_PROCEDURE_PARAMS);
        if (!list) return fail();
        if (take(TokenType::TK_RPAREN)) return list;
        do {
            const char* direction = "";
            if (take("IN")) direction = "IN";
            else if (take("OUT")) direction = "OUT";
            else if (take("INOUT")) direction = "INOUT";
            auto* param = node(NodeType::NODE_MYSQL_PROCEDURE_PARAM, direction);
            auto* name = identifier();
            if (!param || !name) return fail();
            StringRef type = MySQLTypeParser(tok_).parse();
            if (type.empty()) return fail();
            if (take("COLLATE")) {
                const Token collation = tok_.peek();
                if (mysql_charset_introducer(collation) ||
                    (!mysql_identifier_token(collation) && collation.type != TokenType::TK_STRING &&
                     !word(collation, "BINARY")))
                    return fail();
                tok_.skip();
                type.len = static_cast<uint32_t>(collation.source.ptr + collation.source.len - type.ptr);
            }
            auto* data_type = make_node(arena_, NodeType::NODE_TYPE_NAME, type);
            if (!data_type) return fail();
            param->add_child(name);
            param->add_child(data_type);
            list->add_child(param);
        } while (take(TokenType::TK_COMMA));
        return take(TokenType::TK_RPAREN) ? list : fail();
    }
    bool characteristic_start() {
        return is("LANGUAGE") || is("DETERMINISTIC") || is("NOT") ||
            is("COMMENT") || is("SQL") || is("CONTAINS") || is("NO") ||
            is("READS") || is("MODIFIES");
    }
    AstNode* characteristic() {
        const char* phrase = nullptr;
        if (take("LANGUAGE")) {
            if (!take("SQL")) return fail();
            phrase = "LANGUAGE SQL";
        } else if (take("DETERMINISTIC")) phrase = "DETERMINISTIC";
        else if (take("NOT")) {
            if (!take("DETERMINISTIC")) return fail();
            phrase = "NOT DETERMINISTIC";
        } else if (take("SQL")) {
            if (!take("SECURITY")) return fail();
            if (take("DEFINER")) phrase = "SQL SECURITY DEFINER";
            else if (take("INVOKER")) phrase = "SQL SECURITY INVOKER";
            else return fail();
        } else if (take("NO")) {
            if (!take("SQL")) return fail();
            phrase = "NO SQL";
        } else if (take("CONTAINS")) {
            if (!take("SQL")) return fail();
            phrase = "CONTAINS SQL";
        } else if (is("READS") || is("MODIFIES")) {
            const bool reads = take("READS");
            if (!reads) tok_.skip();
            if (!take("SQL") || !take("DATA")) return fail();
            phrase = reads ? "READS SQL DATA" : "MODIFIES SQL DATA";
        } else if (take("COMMENT")) {
            auto* out = node(NodeType::NODE_MYSQL_PROCEDURE_CHARACTERISTIC, "COMMENT");
            if (tok_.peek().type != TokenType::TK_STRING) return fail();
            auto* value = make_node_from_token(arena_, NodeType::NODE_LITERAL_STRING, tok_.next_token());
            if (!out || !value) return fail();
            out->add_child(value);
            return out;
        } else return fail();
        return node(NodeType::NODE_MYSQL_PROCEDURE_CHARACTERISTIC, phrase);
    }
    AstNode* block(unsigned depth) {
        if (depth >= 64 || !take("BEGIN")) return fail();
        auto* out = node(NodeType::NODE_MYSQL_PROCEDURE_BLOCK);
        if (!out) return fail();
        while (!take("END")) {
            auto* child = statement(depth + 1);
            if (!child || tok_.has_error() || !take(TokenType::TK_SEMICOLON)) return fail();
            out->add_child(child);
        }
        return out;
    }
    AstNode* assignments() {
        auto* out = node(NodeType::NODE_SET_STMT);
        if (!out) return fail();
        ExpressionParser<Dialect::MySQL> expr(tok_, arena_, true);
        expr.set_subquery_callback(callback_);
        do {
            auto* assignment = node(NodeType::NODE_VAR_ASSIGNMENT);
            auto* target = node(NodeType::NODE_VAR_TARGET);
            AstNode* name = nullptr;
            if (tok_.peek().type == TokenType::TK_USER_VARIABLE)
                name = make_mysql_user_variable_node(arena_, tok_.next_token());
            else name = identifier();
            if (!assignment || !target || !name ||
                (!take(TokenType::TK_EQUAL) && !take(TokenType::TK_COLON_EQUAL))) return fail();
            auto* value = expr.parse_complete();
            if (!value || tok_.has_error() || !mysql_value_expression(value, true)) return fail();
            target->add_child(name);
            assignment->add_child(target);
            assignment->add_child(value);
            out->add_child(assignment);
        } while (take(TokenType::TK_COMMA));
        return out;
    }
    AstNode* statement(unsigned depth) {
        if (is("BEGIN")) return block(depth);
        if (take("SET")) return assignments();
        if (is("SELECT") || is("TABLE") || is("VALUES") ||
            tok_.peek().type == TokenType::TK_LPAREN) return callback_(tok_, arena_);
        if (take("WITH")) return parse_mysql_with(tok_, arena_, true);
        if (is("INSERT") || is("REPLACE")) {
            const bool replace = take("REPLACE");
            if (!replace) tok_.skip();
            InsertParser<Dialect::MySQL> parser(tok_, arena_, replace);
            parser.set_subquery_callback(callback_);
            return parser.parse();
        }
        if (take("UPDATE")) {
            UpdateParser<Dialect::MySQL> parser(tok_, arena_);
            parser.set_subquery_callback(callback_);
            return parser.parse();
        }
        if (take("DELETE")) {
            DeleteParser<Dialect::MySQL> parser(tok_, arena_);
            parser.set_subquery_callback(callback_);
            return parser.parse();
        }
        return fail();
    }
};

} // namespace sql_parser
#endif // SQL_PARSER_MYSQL_PROCEDURE_PARSER_H
