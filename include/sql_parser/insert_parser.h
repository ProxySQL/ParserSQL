#ifndef SQL_PARSER_INSERT_PARSER_H
#define SQL_PARSER_INSERT_PARSER_H

#include "sql_parser/common.h"
#include "sql_parser/token.h"
#include "sql_parser/tokenizer.h"
#include "sql_parser/ast.h"
#include "sql_parser/arena.h"
#include "sql_parser/expression_parser.h"
#include "sql_parser/table_ref_parser.h"
#include "sql_parser/pg_merge_parser.h"

namespace sql_parser {

// Flag on NODE_INSERT_STMT to indicate REPLACE
static constexpr uint16_t FLAG_REPLACE = 0x01;

template <Dialect D>
class InsertParser {
public:
    InsertParser(Tokenizer<D>& tokenizer, Arena& arena, bool is_replace = false)
        : tok_(tokenizer), arena_(arena), expr_parser_(tokenizer, arena, D == Dialect::MySQL),
          table_ref_parser_(tokenizer, arena, expr_parser_), is_replace_(is_replace) {}

    void set_subquery_callback(SubqueryParseCallback<D> cb) {
        subquery_cb_ = cb;
        expr_parser_.set_subquery_callback(cb);
        table_ref_parser_.set_subquery_callback(cb);
    }

    // INSERT/REPLACE keyword already consumed.
    AstNode* parse() {
        if constexpr (D == Dialect::PostgreSQL) {
            return PgDmlParser(tok_, arena_, subquery_cb_).insert();
        } else {
            auto* root = make_node(arena_, NodeType::NODE_INSERT_STMT, {},
                                   is_replace_ ? FLAG_REPLACE : uint16_t(0));
            if (!root) return error();
            if (auto* options = parse_stmt_options()) root->add_child(options);
            take(TokenType::TK_INTO);

            // INSERT targets are table_ident, not SELECT table references:
            // no target alias, join, derived table or index hint is permitted.
            auto* target = make_node(arena_, NodeType::NODE_TABLE_REF);
            auto* name = parse_name(2);
            if (!target || !name) return error();
            target->add_child(name);
            if (tok_.peek().type == TokenType::TK_PARTITION) {
                auto* partition = table_ref_parser_.parse_mysql_partition_selection();
                if (!partition) return error();
                target->add_child(partition);
            }
            root->add_child(target);

            bool columns = false;
            if (tok_.peek().type == TokenType::TK_LPAREN && !query_source_start()) {
                auto* list = parse_column_list();
                if (!list) return error();
                root->add_child(list);
                columns = true;
            }

            bool alias_allowed = false;
            AstNode* source = nullptr;
            if (query_source_start()) {
                if (!subquery_cb_) return error();
                source = subquery_cb_(tok_, arena_);
            } else if (take(TokenType::TK_VALUES) || take_word("VALUE")) {
                source = parse_values_clause();
                alias_allowed = !is_replace_;
            } else if (!columns && take(TokenType::TK_SET)) {
                source = parse_assignments(NodeType::NODE_INSERT_SET_CLAUSE);
                alias_allowed = !is_replace_;
            } else return error();
            if (!source) return error();
            root->add_child(source);

            if (take(TokenType::TK_AS)) {
                if (!alias_allowed) return error();
                auto* alias = parse_values_reference();
                if (!alias) return error();
                root->add_child(alias);
            }
            if (take(TokenType::TK_ON)) {
                if (is_replace_ || !take(TokenType::TK_DUPLICATE) ||
                    !take(TokenType::TK_KEY) || !take(TokenType::TK_UPDATE)) return error();
                auto* updates = parse_assignments(NodeType::NODE_ON_DUPLICATE_KEY);
                if (!updates) return error();
                root->add_child(updates);
            }
            return expr_parser_.has_operand_error() ? error() : root;
        }
    }

private:
    Tokenizer<D>& tok_;
    Arena& arena_;
    ExpressionParser<D> expr_parser_;
    TableRefParser<D> table_ref_parser_;
    bool is_replace_;
    SubqueryParseCallback<D> subquery_cb_ = nullptr;

    AstNode* error() { return expr_parser_.syntax_error(); }

    bool take(TokenType type) {
        if (tok_.peek().type != type) return false;
        tok_.skip(); return true;
    }

    bool take_word(std::string_view word) {
        if (!ExpressionParser<D>::keyword(tok_.peek(), word)) return false;
        tok_.skip(); return true;
    }

    // Only inspect the leading keyword here: an INSERT query may end before
    // ON DUPLICATE KEY, which is not a general subquery boundary. The query
    // parser still validates every group and its complete contents.
    bool query_source_start() {
        auto look = tok_;
        while (look.peek().type == TokenType::TK_LPAREN) look.skip();
        return ExpressionParser<D>::starts_query(look);
    }

    AstNode* identifier(const Token& token) {
        auto* node = make_node_from_token(arena_, NodeType::NODE_IDENTIFIER, token);
        if (node && token.source.ptr != token.text.ptr) node->flags |= FLAG_IDENT_DELIMITED;
        return node;
    }

    // Native ident uses keyword rules only on the leading name component.
    AstNode* parse_name(unsigned maximum) {
        Token first = tok_.next_token();
        if (!mysql_identifier_token(first) || mysql_charset_introducer(first)) return error();
        auto* name = identifier(first);
        if (!name) return error();
        if (tok_.peek().type != TokenType::TK_DOT) return name;
        auto* qualified = make_node(arena_, NodeType::NODE_QUALIFIED_NAME);
        if (!qualified) return error();
        qualified->add_child(name);
        unsigned parts = 1;
        while (take(TokenType::TK_DOT)) {
            Token component = tok_.next_token();
            if (++parts > maximum || !mysql_identifier_word(component)) return error();
            auto* child = identifier(component);
            if (!child) return error();
            qualified->add_child(child);
        }
        return qualified;
    }

    AstNode* parse_stmt_options() {
        auto type = tok_.peek().type;
        AstNode* options = nullptr;
        if (type == TokenType::TK_LOW_PRIORITY || type == TokenType::TK_DELAYED ||
            (!is_replace_ && type == TokenType::TK_HIGH_PRIORITY)) {
            options = make_node(arena_, NodeType::NODE_STMT_OPTIONS);
            if (!options) return error();
            auto* option = identifier(tok_.next_token());
            if (!option) return error();
            options->add_child(option);
        }
        if (!is_replace_ && tok_.peek().type == TokenType::TK_IGNORE) {
            if (!options) options = make_node(arena_, NodeType::NODE_STMT_OPTIONS);
            if (!options) return error();
            auto* option = identifier(tok_.next_token());
            if (!option) return error();
            options->add_child(option);
        }
        return options;
    }

    AstNode* parse_column_list() {
        tok_.skip(); // (
        auto* list = make_node(arena_, NodeType::NODE_INSERT_COLUMNS);
        if (!list) return error();
        if (take(TokenType::TK_RPAREN)) return list; // native empty target list
        do {
            auto* column = parse_name(3);
            if (!column) return error();
            list->add_child(column);
        } while (take(TokenType::TK_COMMA));
        return take(TokenType::TK_RPAREN) ? list : error();
    }

    AstNode* parse_values_clause() {
        auto* values = make_node(arena_, NodeType::NODE_VALUES_CLAUSE);
        if (!values) return error();
        do {
            if (!take(TokenType::TK_LPAREN)) return error();
            auto* row = make_node(arena_, NodeType::NODE_VALUES_ROW);
            if (!row) return error();
            if (!take(TokenType::TK_RPAREN)) {
                do {
                    auto* value = expr_parser_.parse_complete();
                    if (!value || !mysql_value_expression(value, true)) return error();
                    row->add_child(value);
                } while (take(TokenType::TK_COMMA));
                if (!take(TokenType::TK_RPAREN)) return error();
            }
            values->add_child(row);
        } while (take(TokenType::TK_COMMA));
        return values;
    }

    AstNode* parse_assignments(NodeType type) {
        auto* list = make_node(arena_, type);
        if (!list) return error();
        do {
            auto* item = make_node(arena_, NodeType::NODE_UPDATE_SET_ITEM);
            auto* column = parse_name(3);
            if (!item || !column || (!take(TokenType::TK_EQUAL) && !take(TokenType::TK_COLON_EQUAL)))
                return error();
            auto* value = expr_parser_.parse_complete();
            if (!value || !mysql_value_expression(value, true)) return error();
            item->add_child(column); item->add_child(value);
            list->add_child(item);
        } while (take(TokenType::TK_COMMA));
        return list;
    }

    // The native SYM_FN names shared by the pinned 8.4.8 and 9.7.2
    // sql/lex.h tables become function tokens only with an adjacent '(' in
    // default SQL mode. Whitespace, comments and quoting keep them identifiers.
    bool function_token_alias(const Token& name) {
        if (name.source.ptr != name.text.ptr || tok_.peek().type != TokenType::TK_LPAREN ||
            name.source.ptr + name.source.len != tok_.peek().source.ptr) return false;
        static constexpr std::string_view functions[] = {
            "ADDDATE", "BIT_AND", "BIT_OR", "BIT_XOR", "CAST", "COUNT", "CURDATE", "CURTIME",
            "DATE_ADD", "DATE_SUB", "EXTRACT", "GROUP_CONCAT", "JSON_OBJECTAGG", "JSON_ARRAYAGG",
            "MAX", "MID", "MIN", "NOW", "POSITION", "SESSION_USER", "STD", "STDDEV", "STDDEV_POP",
            "STDDEV_SAMP", "ST_COLLECT", "SUBDATE", "SUBSTR", "SUBSTRING", "SUM", "SYSDATE",
            "SYSTEM_USER", "TRIM", "VARIANCE", "VAR_POP", "VAR_SAMP"
        };
        for (auto function : functions)
            if (name.text.equals_ci(function.data(), function.size())) return true;
        return false;
    }

    AstNode* parse_values_reference() {
        const Token first = tok_.peek();
        auto* alias = make_node(arena_, NodeType::NODE_MYSQL_INSERT_ALIAS);
        auto* name = parse_name(1);
        if (!alias || !name || function_token_alias(first)) return error();
        alias->add_child(name);
        if (take(TokenType::TK_LPAREN)) {
            do {
                auto* column = parse_name(1);
                if (!column) return error();
                alias->add_child(column);
            } while (take(TokenType::TK_COMMA));
            if (!take(TokenType::TK_RPAREN)) return error();
        }
        return alias;
    }
};

} // namespace sql_parser

#endif // SQL_PARSER_INSERT_PARSER_H
