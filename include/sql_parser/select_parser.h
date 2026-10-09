#ifndef SQL_PARSER_SELECT_PARSER_H
#define SQL_PARSER_SELECT_PARSER_H

#include "sql_parser/common.h"
#include "sql_parser/token.h"
#include "sql_parser/tokenizer.h"
#include "sql_parser/ast.h"
#include "sql_parser/arena.h"
#include "sql_parser/expression_parser.h"
#include "sql_parser/table_ref_parser.h"
#include "sql_parser/pg_query_clauses.h"

namespace sql_parser {

template <Dialect D>
class SelectParser {
public:
    SelectParser(Tokenizer<D>& tokenizer, Arena& arena, bool compound_mode = false,
                 bool require_complete_operands = false)
        : tok_(tokenizer), arena_(arena), expr_parser_(tokenizer, arena, require_complete_operands),
          table_ref_parser_(tokenizer, arena, expr_parser_),
          compound_mode_(compound_mode), require_complete_operands_(require_complete_operands) {}

    // Propagate subquery callback to internal expression and table ref parsers
    void set_subquery_callback(SubqueryParseCallback<D> cb) {
        expr_parser_.set_subquery_callback(cb);
        table_ref_parser_.set_subquery_callback(cb);
    }

    // Parse a SELECT statement (SELECT keyword already consumed by classifier).
    // In compound_mode, stops before ORDER BY / LIMIT so they can be claimed
    // by the compound query parser.
    AstNode* parse() {
        AstNode* root = make_node(arena_, NodeType::NODE_SELECT_STMT);
        if (!root) return nullptr;

        if constexpr (D == Dialect::MySQL) {
            if (tok_.peek().type == TokenType::TK_MYSQL_OPTIMIZER_HINT) {
                auto token = tok_.next_token();
                auto* hint = make_node_from_token(arena_, NodeType::NODE_MYSQL_OPTIMIZER_HINT, token);
                if (!hint) return nullptr;
                root->add_child(hint);
            }
        }

        // SELECT options: DISTINCT, ALL, SQL_CALC_FOUND_ROWS
        AstNode* opts = parse_select_options();
        if (opts) root->add_child(opts);

        // Select item list
        AstNode* items = parse_select_item_list();
        if (items) root->add_child(items);

        if constexpr (D == Dialect::PostgreSQL) {
            if (tok_.peek().type == TokenType::TK_INTO) {
                auto* into = PgQueryClauses<D>(tok_, arena_, expr_parser_).into();
                if (!into) return expr_parser_.syntax_error();
                root->add_child(into);
            }
        }

        // FROM clause
        if (tok_.peek().type == TokenType::TK_FROM) {
            tok_.skip();
            AstNode* from = table_ref_parser_.parse_from_clause();
            if (from) root->add_child(from);
        }

        // WHERE clause
        if (tok_.peek().type == TokenType::TK_WHERE) {
            tok_.skip();
            AstNode* where = parse_where_clause();
            if (where) root->add_child(where);
        }

        // GROUP BY clause
        if (tok_.peek().type == TokenType::TK_GROUP) {
            tok_.skip();
            if (tok_.peek().type != TokenType::TK_BY) return expr_parser_.syntax_error();
            tok_.skip();
            AstNode* group_by = parse_group_by();
            if (group_by) root->add_child(group_by);
        }

        // HAVING clause
        if (tok_.peek().type == TokenType::TK_HAVING) {
            tok_.skip();
            AstNode* having = parse_having();
            if (having) root->add_child(having);
        }

        if (ExpressionParser<D>::keyword(tok_.peek(), "WINDOW")) {
            tok_.skip();
            AstNode* windows = make_node(arena_, NodeType::NODE_WINDOW_CLAUSE);
            if (!windows) return expr_parser_.syntax_error();
            while (true) {
                Token name = tok_.peek();
                if (!ExpressionParser<D>::window_name_token(name)) return expr_parser_.syntax_error();
                tok_.skip();
                if (tok_.peek().type != TokenType::TK_AS) return expr_parser_.syntax_error();
                tok_.skip();
                AstNode* spec = expr_parser_.parse_window_spec();
                if (!spec) return nullptr;
                AstNode* definition = make_node(arena_, NodeType::NODE_WINDOW_DEFINITION,
                    name.source.empty() ? name.text : name.source);
                if (!definition) return expr_parser_.syntax_error();
                definition->add_child(spec);
                windows->add_child(definition);
                if (tok_.peek().type != TokenType::TK_COMMA) break;
                tok_.skip();
            }
            root->add_child(windows);
        }

        // In compound_mode, stop before ORDER BY / LIMIT so the compound
        // query parser can claim them as applying to the compound result.
        if (!compound_mode_) {
            // ORDER BY clause
            if (tok_.peek().type == TokenType::TK_ORDER) {
                tok_.skip();
                if (tok_.peek().type == TokenType::TK_BY) tok_.skip();
                AstNode* order_by = parse_order_by();
                if (order_by) root->add_child(order_by);
            }

            // LIMIT clause
            if constexpr (D == Dialect::PostgreSQL) {
                if (!PgQueryClauses<D>(tok_, arena_, expr_parser_).tail(root)) return nullptr;
            } else if (tok_.peek().type == TokenType::TK_LIMIT) {
                tok_.skip();
                AstNode* limit = parse_limit();
                if (limit) root->add_child(limit);
            }

            // FOR UPDATE / FOR SHARE (locking)
            if (D == Dialect::MySQL && tok_.peek().type == TokenType::TK_FOR) {
                AstNode* lock = parse_locking();
                if (lock) root->add_child(lock);
            }

            // INTO (MySQL: can appear here too -- INTO OUTFILE/DUMPFILE/var)
            if constexpr (D == Dialect::MySQL) {
                if (tok_.peek().type == TokenType::TK_INTO) {
                    AstNode* into = parse_into();
                    if (into) root->add_child(into);
                }
            }
        }

        if (require_complete_operands_ && expr_parser_.has_operand_error())
            return expr_parser_.syntax_error();
        return root;
    }

private:
    Tokenizer<D>& tok_;
    Arena& arena_;
    ExpressionParser<D> expr_parser_;
    TableRefParser<D> table_ref_parser_;
    bool compound_mode_;
    bool require_complete_operands_;

    // ---- SELECT options ----

    AstNode* parse_select_options() {
        AstNode* opts = nullptr;
        while (true) {
            Token t = tok_.peek();
            if (t.type == TokenType::TK_DISTINCT || t.type == TokenType::TK_ALL) {
                if (!opts) opts = make_node(arena_, NodeType::NODE_SELECT_OPTIONS);
                tok_.skip();
                if constexpr (D == Dialect::PostgreSQL) {
                    if (t.type == TokenType::TK_DISTINCT && tok_.peek().type == TokenType::TK_ON) {
                        tok_.skip();
                        if (tok_.peek().type != TokenType::TK_LPAREN) return expr_parser_.syntax_error();
                        tok_.skip();
                        AstNode* on = make_node(arena_, NodeType::NODE_DISTINCT_ON);
                        while (true) {
                            AstNode* expr = expr_parser_.parse_complete();
                            if (!expr) return expr_parser_.syntax_error();
                            on->add_child(expr);
                            if (tok_.peek().type != TokenType::TK_COMMA) break;
                            tok_.skip();
                        }
                        if (tok_.peek().type != TokenType::TK_RPAREN) return expr_parser_.syntax_error();
                        tok_.skip();
                        opts->add_child(on);
                        break;
                    }
                }
                opts->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, ExpressionParser<D>::canonical_keyword(t)));
            } else if (t.type == TokenType::TK_SQL_CALC_FOUND_ROWS) {
                if (!opts) opts = make_node(arena_, NodeType::NODE_SELECT_OPTIONS);
                tok_.skip();
                opts->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, ExpressionParser<D>::canonical_keyword(t)));
            } else {
                break;
            }
        }
        return opts;
    }

    // ---- Select item list ----

    AstNode* parse_select_item_list() {
        AstNode* list = make_node(arena_, NodeType::NODE_SELECT_ITEM_LIST);
        if (!list) return nullptr;

        if (D == Dialect::PostgreSQL && require_complete_operands_) {
            // PostgreSQL permits SELECT with an empty target list. Do not turn
            // its clause boundary into a failed expression operand.
            Token t = tok_.peek();
            switch (t.type) {
                case TokenType::TK_INTO: case TokenType::TK_FROM: case TokenType::TK_WHERE:
                case TokenType::TK_GROUP: case TokenType::TK_HAVING:
                case TokenType::TK_ORDER: case TokenType::TK_LIMIT:
                case TokenType::TK_OFFSET: case TokenType::TK_FETCH:
                case TokenType::TK_FOR: case TokenType::TK_RPAREN:
                case TokenType::TK_EOF: case TokenType::TK_SEMICOLON:
                case TokenType::TK_UNION: case TokenType::TK_INTERSECT:
                case TokenType::TK_EXCEPT:
                    return list;
                default: break;
            }
            if (ExpressionParser<D>::keyword(t, "WINDOW")) return list;
        }
        while (true) {
            AstNode* item = parse_select_item();
            if (!item) {
                if constexpr (D == Dialect::MySQL) return expr_parser_.syntax_error();
                break;
            }
            list->add_child(item);
            if (tok_.peek().type == TokenType::TK_COMMA) {
                tok_.skip();
            } else {
                break;
            }
        }
        return list;
    }

    AstNode* parse_select_item() {
        AstNode* item = make_node(arena_, NodeType::NODE_SELECT_ITEM);
        if (!item) return nullptr;

        AstNode* expr = expr_parser_.parse();
        if (!expr) return nullptr;

        // Check for * EXCEPT(...) or * REPLACE(...)
        bool is_star = (expr->type == NodeType::NODE_ASTERISK);
        if (!is_star && expr->type == NodeType::NODE_QUALIFIED_NAME) {
            for (const AstNode* c = expr->first_child; c; c = c->next_sibling) {
                if (!c->next_sibling) {
                    // table.* produces NODE_IDENTIFIER with value "*"
                    if (c->type == NodeType::NODE_ASTERISK) {
                        is_star = true;
                    } else if (c->type == NodeType::NODE_IDENTIFIER &&
                               c->value_len == 1 && c->value_ptr[0] == '*') {
                        is_star = true;
                    }
                }
            }
        }

        if (is_star) {
            Token next = tok_.peek();
            auto look = tok_; look.skip();
            bool except_columns = look.peek().type == TokenType::TK_LPAREN;
            if (except_columns) {
                look.skip();
                except_columns = !ExpressionParser<D>::starts_query(look.peek().type) &&
                    look.peek().type != TokenType::TK_LPAREN;
            }
            // Preserve ParserSQL's explicit star-column extension while
            // allowing PostgreSQL EXCEPT query operands to reach the set parser.
            if (next.type == TokenType::TK_EXCEPT && (D == Dialect::MySQL || except_columns)) {
                tok_.skip();
                AstNode* except_node = make_node(arena_, NodeType::NODE_STAR_EXCEPT);
                except_node->add_child(expr);
                if (tok_.peek().type == TokenType::TK_LPAREN) {
                    tok_.skip();
                    while (tok_.peek().type != TokenType::TK_RPAREN &&
                           tok_.peek().type != TokenType::TK_EOF) {
                        Token col = tok_.next_token();
                        AstNode* col_node = make_node(arena_, NodeType::NODE_IDENTIFIER, col.text);
                        except_node->add_child(col_node);
                        if (tok_.peek().type == TokenType::TK_COMMA) tok_.skip();
                    }
                    if (tok_.peek().type == TokenType::TK_RPAREN) tok_.skip();
                }
                item->add_child(except_node);
                return item;
            }
            if (next.type == TokenType::TK_REPLACE) {
                tok_.skip();
                AstNode* replace_node = make_node(arena_, NodeType::NODE_STAR_REPLACE);
                replace_node->add_child(expr);
                if (tok_.peek().type == TokenType::TK_LPAREN) {
                    tok_.skip();
                    while (tok_.peek().type != TokenType::TK_RPAREN &&
                           tok_.peek().type != TokenType::TK_EOF) {
                        AstNode* replace_expr = expr_parser_.parse();
                        AstNode* replace_item = make_node(arena_, NodeType::NODE_REPLACE_ITEM);
                        if (replace_expr) replace_item->add_child(replace_expr);
                        if (tok_.peek().type == TokenType::TK_AS) {
                            tok_.skip();
                            Token col = tok_.next_token();
                            AstNode* col_node = make_node(arena_, NodeType::NODE_IDENTIFIER, col.text);
                            replace_item->add_child(col_node);
                        }
                        replace_node->add_child(replace_item);
                        if (tok_.peek().type == TokenType::TK_COMMA) tok_.skip();
                    }
                    if (tok_.peek().type == TokenType::TK_RPAREN) tok_.skip();
                }
                item->add_child(replace_node);
                return item;
            }
        }

        item->add_child(expr);

        if constexpr (D == Dialect::PostgreSQL) {
            if (expr->type == NodeType::NODE_ASTERISK) return item;
        }

        // Optional alias: AS name, or just name (implicit alias)
        Token next = tok_.peek();
        if (next.type == TokenType::TK_AS) {
            tok_.skip();
            Token alias_name = tok_.next_token();
            if constexpr (D == Dialect::PostgreSQL) {
                if (!pg_column_label(alias_name)) return expr_parser_.syntax_error();
            }
            AstNode* alias = make_node_from_token(arena_, NodeType::NODE_ALIAS, alias_name,
                alias_name.source.ptr != alias_name.text.ptr ? FLAG_IDENT_DELIMITED : 0);
            item->add_child(alias);
        } else if (TableRefParser<D>::is_alias_token(next) && !TableRefParser<D>::starts_json_format(tok_)) {
            // Implicit alias (no AS keyword): SELECT expr alias_name
            tok_.skip();
            AstNode* alias = make_node_from_token(arena_, NodeType::NODE_ALIAS, next,
                next.source.ptr != next.text.ptr ? FLAG_IDENT_DELIMITED : 0);
            item->add_child(alias);
        }
        return item;
    }

    // ---- WHERE ----

    AstNode* parse_where_clause() {
        AstNode* where = make_node(arena_, NodeType::NODE_WHERE_CLAUSE);
        if (!where) return nullptr;
        AstNode* expr = expr_parser_.parse();
        if (expr) where->add_child(expr);
        return where;
    }

    // ---- GROUP BY ----

    AstNode* parse_group_by() {
        AstNode* group_by = make_node(arena_, NodeType::NODE_GROUP_BY_CLAUSE);
        if (!group_by) return nullptr;
        if constexpr (D == Dialect::PostgreSQL) {
            if (tok_.peek().type == TokenType::TK_DISTINCT || tok_.peek().type == TokenType::TK_ALL)
                group_by->set_value(tok_.next_token().text);
        }
        if constexpr (D == Dialect::MySQL) {
            if (mysql_grouping_operation_start()) {
                auto* operation = parse_mysql_grouping_operation();
                if (!operation) return expr_parser_.syntax_error();
                group_by->add_child(operation);
                return group_by; // Native grouping operations are whole-clause alternatives.
            }
        }
        while (true) {
            AstNode* expr;
            if constexpr (D == Dialect::PostgreSQL)
                expr = PgQueryClauses<D>(tok_, arena_, expr_parser_).grouping();
            else {
                if (mysql_grouping_operation_start()) return expr_parser_.syntax_error();
                expr = expr_parser_.parse_complete();
                if (expr && (!mysql_value_expression(expr) || mysql_nested_grouping_keyword(expr)))
                    return expr_parser_.syntax_error();
            }
            if (!expr) {
                return expr_parser_.syntax_error();
            }
            group_by->add_child(expr);
            if (tok_.peek().type != TokenType::TK_COMMA) break;
            tok_.skip();
        }
        if constexpr (D == Dialect::MySQL) {
            if (tok_.peek().type == TokenType::TK_WITH) {
                tok_.skip();
                if (!ExpressionParser<D>::keyword(tok_.peek(), "ROLLUP")) return expr_parser_.syntax_error();
                tok_.skip();
                group_by->set_value({"WITH ROLLUP", 11});
            }
        }
        return group_by;
    }

    bool mysql_grouping_operation_start() {
        const Token first = tok_.peek();
        auto look = tok_; look.skip();
        return (look.peek().type == TokenType::TK_LPAREN &&
                (ExpressionParser<D>::keyword(first, "ROLLUP") || ExpressionParser<D>::keyword(first, "CUBE"))) ||
            (ExpressionParser<D>::keyword(first, "GROUPING") &&
             ExpressionParser<D>::keyword(look.peek(), "SETS"));
    }

    // ROLLUP/CUBE are whole GROUP BY alternatives, not value functions.
    // Generic function nodes retain name delimiters, so quoted UDF names
    // remain distinct from these unquoted native grouping keywords.
    static bool mysql_nested_grouping_keyword(const AstNode* node) {
        if (node->type == NodeType::NODE_FUNCTION_CALL &&
            (node->value().equals_ci("ROLLUP", 6) || node->value().equals_ci("CUBE", 4))) return true;
        for (const auto* child = node->first_child; child; child = child->next_sibling)
            if (mysql_nested_grouping_keyword(child)) return true;
        return false;
    }

    bool parse_mysql_grouping_items(AstNode* parent, bool allow_empty) {
        if (tok_.peek().type == TokenType::TK_RPAREN) return allow_empty;
        do {
            auto* expr = expr_parser_.parse_complete();
            if (!expr || !mysql_value_expression(expr) || mysql_nested_grouping_keyword(expr)) return false;
            parent->add_child(expr);
            if (tok_.peek().type != TokenType::TK_COMMA) break;
            tok_.skip();
        } while (true);
        return true;
    }

    AstNode* parse_mysql_grouping_operation() {
        const Token name = tok_.next_token();
        const bool sets = ExpressionParser<D>::keyword(name, "GROUPING");
        if (sets) tok_.skip(); // SETS recognized by lookahead
        if (tok_.peek().type != TokenType::TK_LPAREN) return expr_parser_.syntax_error();
        tok_.skip();
        auto* group = make_node(arena_, NodeType::NODE_GROUPING_SET,
            sets ? StringRef{"GROUPING SETS", 13} : name.text);
        if (!group) return expr_parser_.syntax_error();
        if (sets) {
            do {
                if (tok_.peek().type != TokenType::TK_LPAREN) return expr_parser_.syntax_error();
                tok_.skip();
                auto* tuple = make_node(arena_, NodeType::NODE_TUPLE);
                if (!tuple || !parse_mysql_grouping_items(tuple, true) ||
                    tok_.peek().type != TokenType::TK_RPAREN) return expr_parser_.syntax_error();
                tok_.skip(); group->add_child(tuple);
                if (tok_.peek().type != TokenType::TK_COMMA) break;
                tok_.skip();
            } while (true);
        } else if (!parse_mysql_grouping_items(group, false)) return expr_parser_.syntax_error();
        if (tok_.peek().type != TokenType::TK_RPAREN) return expr_parser_.syntax_error();
        tok_.skip();
        return group;
    }

    // ---- HAVING ----

    AstNode* parse_having() {
        AstNode* having = make_node(arena_, NodeType::NODE_HAVING_CLAUSE);
        if (!having) return nullptr;
        AstNode* expr = expr_parser_.parse();
        if (expr) having->add_child(expr);
        return having;
    }

    // ---- ORDER BY ----

    AstNode* parse_order_by() {
        AstNode* order_by = make_node(arena_, NodeType::NODE_ORDER_BY_CLAUSE);
        if (!order_by) return nullptr;

        while (true) {
            AstNode* expr = expr_parser_.parse();
            if (!expr) break;

            AstNode* item = make_node(arena_, NodeType::NODE_ORDER_BY_ITEM);
            item->add_child(expr);

            // Optional ASC/DESC
            Token dir = tok_.peek();
            if (dir.type == TokenType::TK_ASC || dir.type == TokenType::TK_DESC) {
                tok_.skip();
                item->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, ExpressionParser<D>::canonical_keyword(dir)));
            }

            if constexpr (D == Dialect::PostgreSQL) {
                if (dir.type == TokenType::TK_USING) {
                    auto* op = expr_parser_.parse_sort_operator();
                    if (!op) return nullptr;
                    item->add_child(op);
                }
                if (ExpressionParser<D>::keyword(tok_.peek(), "NULLS")) {
                    tok_.skip(); auto placement = tok_.peek();
                    bool first = ExpressionParser<D>::keyword(placement, "FIRST");
                    if (!first && !ExpressionParser<D>::keyword(placement, "LAST")) return expr_parser_.syntax_error();
                    tok_.skip(); item->flags |= FLAG_ORDER_NULLS;
                    auto* nulls = make_node(arena_, NodeType::NODE_IDENTIFIER,
                        first ? StringRef{"NULLS FIRST", 11} : StringRef{"NULLS LAST", 10});
                    if (!nulls) return expr_parser_.syntax_error();
                    item->add_child(nulls);
                }
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

    // ---- LIMIT ----

    AstNode* parse_limit() {
        AstNode* limit = make_node(arena_, NodeType::NODE_LIMIT_CLAUSE);
        if (!limit) return nullptr;

        // LIMIT count [OFFSET offset]  or  LIMIT offset, count (MySQL)
        AstNode* first = expr_parser_.parse();
        if (first) limit->add_child(first);

        if (tok_.peek().type == TokenType::TK_OFFSET) {
            tok_.skip();
            AstNode* offset = expr_parser_.parse();
            if (offset) limit->add_child(offset);
        } else if (tok_.peek().type == TokenType::TK_COMMA) {
            // MySQL: LIMIT offset, count
            tok_.skip();
            AstNode* count = expr_parser_.parse();
            limit->flags |= FLAG_LIMIT_COMMA;
            limit->first_child = nullptr;
            if (count) limit->add_child(count);
            if (first) limit->add_child(first);
        }

        if constexpr (D == Dialect::PostgreSQL) {
            // PostgreSQL also supports FETCH FIRST N ROWS ONLY after LIMIT/OFFSET
            // We handle OFFSET here too since PgSQL uses LIMIT x OFFSET y
        }

        return limit;
    }

public:
    // Shared by standalone SELECT and compound-query tails.
    AstNode* parse_locking() {
        if (tok_.peek().type == TokenType::TK_LOCK) {
            tok_.skip();
            if (tok_.next_token().type != TokenType::TK_IN ||
                tok_.next_token().type != TokenType::TK_SHARE ||
                !ExpressionParser<D>::keyword(tok_.next_token(), "MODE")) return expr_parser_.syntax_error();
            auto* lock = make_node(arena_, NodeType::NODE_LOCKING_CLAUSE, StringRef{"LOCK IN SHARE MODE", 18});
            return lock ? lock : expr_parser_.syntax_error();
        }
        if (tok_.next_token().type != TokenType::TK_FOR) return expr_parser_.syntax_error();
        Token strength = tok_.next_token();
        if (strength.type != TokenType::TK_UPDATE && strength.type != TokenType::TK_SHARE)
            return expr_parser_.syntax_error();
        auto* lock = make_node(arena_, NodeType::NODE_LOCKING_CLAUSE);
        auto* mode = make_node(arena_, NodeType::NODE_IDENTIFIER, ExpressionParser<D>::canonical_keyword(strength));
        if (!lock || !mode) return expr_parser_.syntax_error();
        lock->add_child(mode);
        if (tok_.peek().type == TokenType::TK_OF) {
            tok_.skip();
            auto* targets = make_node(arena_, NodeType::NODE_MYSQL_LOCK_TARGETS);
            if (!targets) return expr_parser_.syntax_error();
            do {
                Token name = tok_.next_token();
                if (!mysql_identifier_token(name)) return expr_parser_.syntax_error();
                auto* target = make_node_from_token(arena_, NodeType::NODE_IDENTIFIER, name,
                    name.source.ptr != name.text.ptr ? FLAG_IDENT_DELIMITED : 0);
                if (!target) return expr_parser_.syntax_error();
                if (tok_.peek().type == TokenType::TK_DOT) {
                    auto* qualified = make_node(arena_, NodeType::NODE_QUALIFIED_NAME);
                    if (!qualified) return expr_parser_.syntax_error();
                    qualified->add_child(target);
                    tok_.skip();
                    Token part = tok_.next_token();
                    if (!mysql_identifier_word(part) && part.type != TokenType::TK_ASTERISK)
                        return expr_parser_.syntax_error();
                    auto* item = make_node_from_token(arena_, part.type == TokenType::TK_ASTERISK ?
                        NodeType::NODE_ASTERISK : NodeType::NODE_IDENTIFIER, part,
                        part.source.ptr != part.text.ptr ? FLAG_IDENT_DELIMITED : 0);
                    if (!item) return expr_parser_.syntax_error();
                    qualified->add_child(item);
                    if (part.type != TokenType::TK_ASTERISK && tok_.peek().type == TokenType::TK_DOT) {
                        tok_.skip();
                        if (tok_.next_token().type != TokenType::TK_ASTERISK) return expr_parser_.syntax_error();
                        auto* star = make_node(arena_, NodeType::NODE_ASTERISK, StringRef{"*", 1});
                        if (!star) return expr_parser_.syntax_error();
                        qualified->add_child(star);
                    }
                    target = qualified;
                }
                targets->add_child(target);
                if (tok_.peek().type != TokenType::TK_COMMA) break;
                tok_.skip();
            } while (true);
            lock->add_child(targets);
        }
        StringRef action;
        if (tok_.peek().type == TokenType::TK_NOWAIT) {
            tok_.skip(); action = {"NOWAIT", 6};
        } else if (tok_.peek().type == TokenType::TK_SKIP) {
            tok_.skip();
            if (tok_.next_token().type != TokenType::TK_LOCKED) return expr_parser_.syntax_error();
            action = {"SKIP LOCKED", 11};
        }
        if (!action.empty()) {
            auto* node = make_node(arena_, NodeType::NODE_IDENTIFIER, action);
            if (!node) return expr_parser_.syntax_error();
            lock->add_child(node);
        }
        return lock;
    }

private:
    // ---- INTO (MySQL: INTO OUTFILE/DUMPFILE/@var) ----

    AstNode* parse_into() {
        AstNode* into = make_node(arena_, NodeType::NODE_INTO_CLAUSE);
        if (!into) return nullptr;

        tok_.skip(); // consume INTO
        Token t = tok_.peek();

        if (t.type == TokenType::TK_OUTFILE) {
            tok_.skip();
            Token filename = tok_.next_token();
            into->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER,
                StringRef{"OUTFILE", 7}));
            into->add_child(make_node(arena_, NodeType::NODE_LITERAL_STRING, filename.text));
        } else if (t.type == TokenType::TK_DUMPFILE) {
            tok_.skip();
            Token filename = tok_.next_token();
            into->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER,
                StringRef{"DUMPFILE", 8}));
            into->add_child(make_node(arena_, NodeType::NODE_LITERAL_STRING, filename.text));
        } else {
            // INTO @var1, @var2, ...
            while (true) {
                AstNode* var = expr_parser_.parse();
                if (var) into->add_child(var);
                if (tok_.peek().type == TokenType::TK_COMMA) {
                    tok_.skip();
                } else {
                    break;
                }
            }
        }

        return into;
    }
};

} // namespace sql_parser

#endif // SQL_PARSER_SELECT_PARSER_H
