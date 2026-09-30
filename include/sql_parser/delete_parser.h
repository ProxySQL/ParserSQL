#ifndef SQL_PARSER_DELETE_PARSER_H
#define SQL_PARSER_DELETE_PARSER_H

#include "sql_parser/common.h"
#include "sql_parser/token.h"
#include "sql_parser/tokenizer.h"
#include "sql_parser/ast.h"
#include "sql_parser/arena.h"
#include "sql_parser/expression_parser.h"
#include "sql_parser/table_ref_parser.h"
#include "sql_parser/pg_merge_parser.h"
#include "sql_parser/mysql_value_syntax.h"

namespace sql_parser {

// Flags on NODE_DELETE_STMT
static constexpr uint16_t FLAG_DELETE_MULTI_TABLE = 0x01;   // multi-table form
static constexpr uint16_t FLAG_DELETE_FORM2 = 0x02;         // MySQL form 2 (DELETE FROM ... USING)

template <Dialect D>
class DeleteParser {
public:
    DeleteParser(Tokenizer<D>& tokenizer, Arena& arena)
        : tok_(tokenizer), arena_(arena), expr_parser_(tokenizer, arena, D == Dialect::MySQL),
          table_ref_parser_(tokenizer, arena, expr_parser_) {}

    void set_subquery_callback(SubqueryParseCallback<D> cb) {
        subquery_cb_ = cb;
        expr_parser_.set_subquery_callback(cb);
        table_ref_parser_.set_subquery_callback(cb);
    }

    // Parse DELETE statement (DELETE keyword already consumed).
    AstNode* parse() {
        if constexpr (D == Dialect::PostgreSQL)
            return PgDmlParser(tok_, arena_, subquery_cb_).remove();
        AstNode* root = make_node(arena_, NodeType::NODE_DELETE_STMT);
        if (!root) return nullptr;

        if constexpr (D == Dialect::MySQL) {
            return parse_mysql(root);
        } else {
            return parse_pgsql(root);
        }
    }

private:
    AstNode* make_identifier(const Token& token, NodeType type = NodeType::NODE_IDENTIFIER) {
        AstNode* node = make_node_from_token(arena_, type, token);
        if (node && token.type == TokenType::TK_IDENTIFIER && token.source.ptr != token.text.ptr)
            node->flags |= FLAG_IDENT_DELIMITED;
        return node;
    }

    Tokenizer<D>& tok_;
    Arena& arena_;
    ExpressionParser<D> expr_parser_;
    TableRefParser<D> table_ref_parser_;
    SubqueryParseCallback<D> subquery_cb_ = nullptr;

    // ---- MySQL DELETE ----
    // Single-table: DELETE [LOW_PRIORITY] [QUICK] [IGNORE] FROM table [WHERE] [ORDER BY] [LIMIT]
    // Multi-table form 1: DELETE [opts] t1, t2 FROM table_refs [WHERE]
    // Multi-table form 2: DELETE [opts] FROM t1, t2 USING table_refs [WHERE]

    AstNode* parse_mysql(AstNode* root) {
        if (auto* opts = parse_stmt_options()) root->add_child(opts);
        const bool from_first = tok_.peek().type == TokenType::TK_FROM;
        if (from_first) tok_.skip();
        auto* first = parse_simple_table_ref();
        if (!first) return expr_parser_.syntax_error();
        bool wildcard = target_wildcard(first);
        if (from_first && !wildcard) {
            table_ref_parser_.parse_optional_alias(first);
            if (tok_.peek().type == TokenType::TK_PARTITION) {
                auto* partition = table_ref_parser_.parse_mysql_partition_selection();
                if (!partition) return expr_parser_.syntax_error();
                first->add_child(partition);
            }
        }
        root->add_child(first);
        const bool multi = !from_first || tok_.peek().type == TokenType::TK_COMMA ||
                           tok_.peek().type == TokenType::TK_USING;
        if (multi) {
            // Target lists have no aliases or partition selections.
            if (first->first_child->next_sibling) return expr_parser_.syntax_error();
            while (tok_.peek().type == TokenType::TK_COMMA) {
                tok_.skip();
                auto* target = parse_simple_table_ref();
                if (!target) return expr_parser_.syntax_error();
                root->add_child(target);
            }
            auto separator = from_first ? TokenType::TK_USING : TokenType::TK_FROM;
            if (tok_.peek().type != separator) return expr_parser_.syntax_error();
            tok_.skip();
            root->flags = FLAG_DELETE_MULTI_TABLE | (from_first ? FLAG_DELETE_FORM2 : 0);
            auto* sources = table_ref_parser_.parse_from_clause();
            if (!sources || !sources->first_child) return expr_parser_.syntax_error();
            if (from_first) sources->type = NodeType::NODE_DELETE_USING_CLAUSE;
            root->add_child(sources);
        } else if (wildcard) return expr_parser_.syntax_error();
        if (tok_.peek().type == TokenType::TK_WHERE) {
            tok_.skip();
            auto* where = parse_where_clause();
            if (!where) return expr_parser_.syntax_error();
            root->add_child(where);
        }
        if (!multi && tok_.peek().type == TokenType::TK_ORDER) {
            tok_.skip();
            if (tok_.peek().type != TokenType::TK_BY) return expr_parser_.syntax_error();
            tok_.skip();
            auto* order = parse_order_by();
            if (!order) return expr_parser_.syntax_error();
            root->add_child(order);
        }
        if (!multi && tok_.peek().type == TokenType::TK_LIMIT) {
            tok_.skip();
            auto* limit = parse_limit();
            if (!limit) return expr_parser_.syntax_error();
            root->add_child(limit);
        }
        return expr_parser_.has_operand_error() ? expr_parser_.syntax_error() : root;
    }

    static bool target_wildcard(const AstNode* ref) {
        const auto* name = ref->first_child;
        if (!name || name->type != NodeType::NODE_QUALIFIED_NAME) return false;
        for (const auto* part = name->first_child; part; part = part->next_sibling)
            if (part->type == NodeType::NODE_ASTERISK) return true;
        return false;
    }

    // ---- PostgreSQL DELETE ----
    // DELETE FROM [ONLY] table [[AS] alias] [USING using_list] [WHERE] [RETURNING]

    AstNode* parse_pgsql(AstNode* root) {
        // Consume FROM
        if (tok_.peek().type == TokenType::TK_FROM) {
            tok_.skip();
        }

        // Optional ONLY keyword
        if (tok_.peek().type == TokenType::TK_ONLY) {
            AstNode* opts = make_node(arena_, NodeType::NODE_STMT_OPTIONS);
            Token only_tok = tok_.next_token();
            opts->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, only_tok.text));
            root->add_child(opts);
        }

        // Single table reference with optional alias
        AstNode* table_ref = table_ref_parser_.parse_table_reference();
        if (table_ref) root->add_child(table_ref);

        // USING clause
        if (tok_.peek().type == TokenType::TK_USING) {
            tok_.skip();
            AstNode* using_clause = make_node(arena_, NodeType::NODE_DELETE_USING_CLAUSE);

            // Parse table list (comma-separated, potentially with JOINs)
            while (true) {
                AstNode* tref = table_ref_parser_.parse_table_reference();
                if (tref) using_clause->add_child(tref);
                if (tok_.peek().type == TokenType::TK_COMMA) {
                    tok_.skip();
                } else {
                    break;
                }
            }
            root->add_child(using_clause);
        }

        // WHERE
        if (tok_.peek().type == TokenType::TK_WHERE) {
            tok_.skip();
            AstNode* where = parse_where_clause();
            if (where) root->add_child(where);
        }

        // RETURNING
        if (tok_.peek().type == TokenType::TK_RETURNING) {
            AstNode* ret = parse_returning();
            if (ret) root->add_child(ret);
        }

        return root;
    }

    // ---- Shared helpers ----

    // Parse a simple table reference (name or schema.name, no alias parsing for target tables)
    AstNode* parse_simple_table_ref() {
        AstNode* ref = make_node(arena_, NodeType::NODE_TABLE_REF);
        if (!ref) return nullptr;

        Token name = tok_.next_token();
        if (!mysql_identifier_token(name)) return expr_parser_.syntax_error();
        auto* target = make_identifier(name);
        if (!target) return expr_parser_.syntax_error();
        if (tok_.peek().type == TokenType::TK_DOT) {
            auto* qualified = make_node(arena_, NodeType::NODE_QUALIFIED_NAME);
            if (!qualified) return expr_parser_.syntax_error();
            qualified->add_child(target);
            unsigned parts = 1;
            while (tok_.peek().type == TokenType::TK_DOT) {
                tok_.skip();
                Token component = tok_.next_token();
                const bool star = component.type == TokenType::TK_ASTERISK;
                if (++parts > (star ? 3u : 2u) ||
                    (!star && !mysql_identifier_word(component))) return expr_parser_.syntax_error();
                auto* part = make_identifier(component, star ? NodeType::NODE_ASTERISK : NodeType::NODE_IDENTIFIER);
                if (!part) return expr_parser_.syntax_error();
                qualified->add_child(part);
                if (star) break;
            }
            target = qualified;
        }
        ref->add_child(target);

        return ref;
    }

    // Parse MySQL options: LOW_PRIORITY, QUICK, IGNORE
    AstNode* parse_stmt_options() {
        AstNode* opts = nullptr;
        while (true) {
            Token t = tok_.peek();
            if (t.type == TokenType::TK_LOW_PRIORITY ||
                t.type == TokenType::TK_QUICK ||
                t.type == TokenType::TK_IGNORE) {
                if (!opts) opts = make_node(arena_, NodeType::NODE_STMT_OPTIONS);
                tok_.skip();
                opts->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, t.text));
            } else {
                break;
            }
        }
        return opts;
    }

    // Parse WHERE clause
    AstNode* parse_where_clause() {
        AstNode* where = make_node(arena_, NodeType::NODE_WHERE_CLAUSE);
        if (!where) return nullptr;
        AstNode* expr = expr_parser_.parse();
        if (!expr || !mysql_value_expression(expr)) return expr_parser_.syntax_error();
        where->add_child(expr);
        return where;
    }

    // Parse ORDER BY clause
    AstNode* parse_order_by() {
        AstNode* order_by = make_node(arena_, NodeType::NODE_ORDER_BY_CLAUSE);
        if (!order_by) return nullptr;

        while (true) {
            AstNode* expr = expr_parser_.parse();
            if (!expr || !mysql_value_expression(expr)) return expr_parser_.syntax_error();

            AstNode* item = make_node(arena_, NodeType::NODE_ORDER_BY_ITEM);
            item->add_child(expr);

            // Optional ASC/DESC
            Token dir = tok_.peek();
            if (dir.type == TokenType::TK_ASC || dir.type == TokenType::TK_DESC) {
                tok_.skip();
                item->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, dir.text));
            }

            order_by->add_child(item);

            if (tok_.peek().type == TokenType::TK_COMMA) {
                tok_.skip();
            } else {
                break;
            }
        }
        return order_by;
    }

    // Parse LIMIT clause
    AstNode* parse_limit() {
        AstNode* limit = make_node(arena_, NodeType::NODE_LIMIT_CLAUSE);
        if (!limit) return nullptr;

        AstNode* count = nullptr;
        if constexpr (D == Dialect::MySQL) count = mysql_limit_value(tok_, arena_);
        else count = expr_parser_.parse();
        if (!count) return expr_parser_.syntax_error();
        limit->add_child(count);

        return limit;
    }

    // Parse PostgreSQL RETURNING clause
    AstNode* parse_returning() {
        if (tok_.peek().type != TokenType::TK_RETURNING) return nullptr;
        tok_.skip();  // RETURNING

        AstNode* ret = make_node(arena_, NodeType::NODE_RETURNING_CLAUSE);
        if (!ret) return nullptr;

        while (true) {
            AstNode* expr = expr_parser_.parse();
            if (!expr) break;
            ret->add_child(expr);

            // Check for optional alias
            Token next = tok_.peek();
            if (next.type == TokenType::TK_AS) {
                tok_.skip();
                Token alias_name = tok_.next_token();
                AstNode* alias = make_node_from_token(arena_, NodeType::NODE_ALIAS, alias_name);
                if (alias && alias_name.source.ptr != alias_name.text.ptr)
                    alias->flags |= FLAG_IDENT_DELIMITED;
                ret->add_child(alias);
            }

            if (tok_.peek().type == TokenType::TK_COMMA) {
                tok_.skip();
            } else {
                break;
            }
        }

        return ret;
    }
};

} // namespace sql_parser

#endif // SQL_PARSER_DELETE_PARSER_H
