#ifndef SQL_PARSER_SELECT_PARSER_H
#define SQL_PARSER_SELECT_PARSER_H

#include "sql_parser/common.h"
#include "sql_parser/token.h"
#include "sql_parser/tokenizer.h"
#include "sql_parser/ast.h"
#include "sql_parser/arena.h"
#include "sql_parser/expression_parser.h"

namespace sql_parser {

template <Dialect D>
class SelectParser {
public:
    SelectParser(Tokenizer<D>& tokenizer, Arena& arena)
        : tok_(tokenizer), arena_(arena), expr_parser_(tokenizer, arena) {}

    // Parse a SELECT statement (SELECT keyword already consumed by classifier).
    AstNode* parse() {
        AstNode* root = make_node(arena_, NodeType::NODE_SELECT_STMT);
        if (!root) return nullptr;

        // SELECT options: DISTINCT, ALL, SQL_CALC_FOUND_ROWS
        AstNode* opts = parse_select_options();
        if (opts) root->add_child(opts);

        // Select item list
        AstNode* items = parse_select_item_list();
        if (items) root->add_child(items);

        // INTO (before FROM in some MySQL variants -- skip for now, handle after FROM)

        // FROM clause
        if (tok_.peek().type == TokenType::TK_FROM) {
            tok_.skip();
            AstNode* from = parse_from_clause();
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
            if (tok_.peek().type == TokenType::TK_BY) tok_.skip();
            AstNode* group_by = parse_group_by();
            if (group_by) root->add_child(group_by);
        }

        // HAVING clause
        if (tok_.peek().type == TokenType::TK_HAVING) {
            tok_.skip();
            AstNode* having = parse_having();
            if (having) root->add_child(having);
        }

        // ORDER BY clause
        if (tok_.peek().type == TokenType::TK_ORDER) {
            tok_.skip();
            if (tok_.peek().type == TokenType::TK_BY) tok_.skip();
            AstNode* order_by = parse_order_by();
            if (order_by) root->add_child(order_by);
        }

        // LIMIT clause
        if (tok_.peek().type == TokenType::TK_LIMIT) {
            tok_.skip();
            AstNode* limit = parse_limit();
            if (limit) root->add_child(limit);
        }

        // FOR UPDATE / FOR SHARE (locking)
        if (tok_.peek().type == TokenType::TK_FOR) {
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

        return root;
    }

private:
    Tokenizer<D>& tok_;
    Arena& arena_;
    ExpressionParser<D> expr_parser_;

    // ---- SELECT options ----

    AstNode* parse_select_options() {
        AstNode* opts = nullptr;
        while (true) {
            Token t = tok_.peek();
            if (t.type == TokenType::TK_DISTINCT || t.type == TokenType::TK_ALL) {
                if (!opts) opts = make_node(arena_, NodeType::NODE_SELECT_OPTIONS);
                tok_.skip();
                opts->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, t.text));
            } else if (t.type == TokenType::TK_SQL_CALC_FOUND_ROWS) {
                if (!opts) opts = make_node(arena_, NodeType::NODE_SELECT_OPTIONS);
                tok_.skip();
                opts->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, t.text));
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

        while (true) {
            AstNode* item = parse_select_item();
            if (!item) break;
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
        item->add_child(expr);

        // Optional alias: AS name, or just name (implicit alias)
        Token next = tok_.peek();
        if (next.type == TokenType::TK_AS) {
            tok_.skip();
            Token alias_name = tok_.next_token();
            AstNode* alias = make_node(arena_, NodeType::NODE_ALIAS, alias_name.text);
            item->add_child(alias);
        } else if (is_alias_start(next.type)) {
            // Implicit alias (no AS keyword): SELECT expr alias_name
            tok_.skip();
            AstNode* alias = make_node(arena_, NodeType::NODE_ALIAS, next.text);
            item->add_child(alias);
        }
        return item;
    }

    // ---- FROM clause ----

    AstNode* parse_from_clause() {
        AstNode* from = make_node(arena_, NodeType::NODE_FROM_CLAUSE);
        if (!from) return nullptr;

        // First table reference
        AstNode* table_ref = parse_table_reference();
        if (table_ref) from->add_child(table_ref);

        // Additional table refs (comma join) or explicit JOINs
        while (true) {
            Token t = tok_.peek();
            if (t.type == TokenType::TK_COMMA) {
                // Comma join: FROM t1, t2
                tok_.skip();
                AstNode* next_ref = parse_table_reference();
                if (next_ref) from->add_child(next_ref);
            } else if (is_join_start(t.type)) {
                // Explicit JOIN
                AstNode* join = parse_join(from->first_child);
                if (join) {
                    // Replace the last table ref with the join node
                    // Actually, append the join as a child of FROM
                    from->add_child(join);
                }
            } else {
                break;
            }
        }

        return from;
    }

    AstNode* parse_table_reference() {
        Token t = tok_.peek();

        // Subquery: (SELECT ...)
        if (t.type == TokenType::TK_LPAREN) {
            tok_.skip();
            if (tok_.peek().type == TokenType::TK_SELECT) {
                AstNode* subq = make_node(arena_, NodeType::NODE_SUBQUERY);
                // Skip to matching paren
                int depth = 1;
                while (depth > 0) {
                    Token st = tok_.next_token();
                    if (st.type == TokenType::TK_LPAREN) ++depth;
                    else if (st.type == TokenType::TK_RPAREN) --depth;
                    else if (st.type == TokenType::TK_EOF) break;
                }
                // Optional alias
                AstNode* ref = make_node(arena_, NodeType::NODE_TABLE_REF);
                ref->add_child(subq);
                parse_optional_alias(ref);
                return ref;
            }
            // Parenthesized table reference -- parse inner
            AstNode* inner = parse_table_reference();
            if (tok_.peek().type == TokenType::TK_RPAREN) tok_.skip();
            return inner;
        }

        // Simple table name or schema.table
        AstNode* ref = make_node(arena_, NodeType::NODE_TABLE_REF);
        Token name = tok_.next_token();

        if (tok_.peek().type == TokenType::TK_DOT) {
            // Qualified: schema.table
            tok_.skip();
            Token table_name = tok_.next_token();
            AstNode* qname = make_node(arena_, NodeType::NODE_QUALIFIED_NAME);
            qname->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, name.text));
            qname->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, table_name.text));
            ref->add_child(qname);
        } else {
            ref->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, name.text));
        }

        // Optional alias
        parse_optional_alias(ref);
        return ref;
    }

    void parse_optional_alias(AstNode* parent) {
        Token t = tok_.peek();
        if (t.type == TokenType::TK_AS) {
            tok_.skip();
            Token alias_name = tok_.next_token();
            parent->add_child(make_node(arena_, NodeType::NODE_ALIAS, alias_name.text));
        } else if (is_alias_start(t.type)) {
            tok_.skip();
            parent->add_child(make_node(arena_, NodeType::NODE_ALIAS, t.text));
        }
    }

    // ---- JOIN ----

    AstNode* parse_join(AstNode* /* left_ref */) {
        AstNode* join = make_node(arena_, NodeType::NODE_JOIN_CLAUSE);
        if (!join) return nullptr;

        // Consume join type tokens
        Token t = tok_.peek();
        StringRef join_type_start = t.text;
        StringRef join_type_end = t.text;

        // Optional: NATURAL, LEFT, RIGHT, FULL, INNER, OUTER, CROSS
        while (t.type == TokenType::TK_NATURAL || t.type == TokenType::TK_LEFT ||
               t.type == TokenType::TK_RIGHT || t.type == TokenType::TK_FULL ||
               t.type == TokenType::TK_INNER || t.type == TokenType::TK_OUTER ||
               t.type == TokenType::TK_CROSS) {
            tok_.skip();
            join_type_end = t.text;
            t = tok_.peek();
        }

        // Expect JOIN keyword
        if (t.type == TokenType::TK_JOIN) {
            join_type_end = t.text;
            tok_.skip();
        }

        // Set join type as value (covers the span from first modifier to JOIN)
        StringRef join_type{join_type_start.ptr,
            static_cast<uint32_t>((join_type_end.ptr + join_type_end.len) - join_type_start.ptr)};
        join->value_ptr = join_type.ptr;
        join->value_len = join_type.len;

        // Right table reference
        AstNode* right_ref = parse_table_reference();
        if (right_ref) join->add_child(right_ref);

        // Join condition: ON expr or USING (col_list)
        if (tok_.peek().type == TokenType::TK_ON) {
            tok_.skip();
            AstNode* on_expr = expr_parser_.parse();
            if (on_expr) join->add_child(on_expr);
        } else if (tok_.peek().type == TokenType::TK_USING) {
            tok_.skip();
            if (tok_.peek().type == TokenType::TK_LPAREN) {
                tok_.skip();
                AstNode* using_list = make_node(arena_, NodeType::NODE_IDENTIFIER, StringRef{"USING", 5});
                while (true) {
                    Token col = tok_.next_token();
                    using_list->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, col.text));
                    if (tok_.peek().type == TokenType::TK_COMMA) {
                        tok_.skip();
                    } else {
                        break;
                    }
                }
                if (tok_.peek().type == TokenType::TK_RPAREN) tok_.skip();
                join->add_child(using_list);
            }
        }

        return join;
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

        while (true) {
            AstNode* expr = expr_parser_.parse();
            if (!expr) break;
            group_by->add_child(expr);
            if (tok_.peek().type == TokenType::TK_COMMA) {
                tok_.skip();
            } else {
                break;
            }
        }
        return group_by;
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
            if (count) limit->add_child(count);
        }

        if constexpr (D == Dialect::PostgreSQL) {
            // PostgreSQL also supports FETCH FIRST N ROWS ONLY after LIMIT/OFFSET
            // We handle OFFSET here too since PgSQL uses LIMIT x OFFSET y
        }

        return limit;
    }

    // ---- FOR UPDATE / FOR SHARE ----

    AstNode* parse_locking() {
        AstNode* lock = make_node(arena_, NodeType::NODE_LOCKING_CLAUSE);
        if (!lock) return nullptr;

        tok_.skip(); // consume FOR
        Token strength = tok_.next_token(); // UPDATE or SHARE
        lock->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, strength.text));

        // Optional: OF table_list
        if (tok_.peek().type == TokenType::TK_OF) {
            tok_.skip();
            while (true) {
                Token table = tok_.next_token();
                lock->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, table.text));
                if (tok_.peek().type == TokenType::TK_COMMA) {
                    tok_.skip();
                } else {
                    break;
                }
            }
        }

        // Optional: NOWAIT or SKIP LOCKED
        if (tok_.peek().type == TokenType::TK_NOWAIT) {
            tok_.skip();
            lock->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, StringRef{"NOWAIT", 6}));
        } else if (tok_.peek().type == TokenType::TK_SKIP) {
            tok_.skip();
            if (tok_.peek().type == TokenType::TK_LOCKED) tok_.skip();
            lock->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, StringRef{"SKIP LOCKED", 11}));
        }

        return lock;
    }

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

    // ---- Helpers ----

    static bool is_join_start(TokenType type) {
        return type == TokenType::TK_JOIN || type == TokenType::TK_INNER ||
               type == TokenType::TK_LEFT || type == TokenType::TK_RIGHT ||
               type == TokenType::TK_FULL || type == TokenType::TK_OUTER ||
               type == TokenType::TK_CROSS || type == TokenType::TK_NATURAL;
    }

    // Check if a token can start an implicit alias (identifier-like, not a clause keyword)
    static bool is_alias_start(TokenType type) {
        if (type == TokenType::TK_IDENTIFIER) return true;
        // Some keywords are NOT valid alias starts because they start clauses
        switch (type) {
            case TokenType::TK_FROM:
            case TokenType::TK_WHERE:
            case TokenType::TK_GROUP:
            case TokenType::TK_HAVING:
            case TokenType::TK_ORDER:
            case TokenType::TK_LIMIT:
            case TokenType::TK_FOR:
            case TokenType::TK_INTO:
            case TokenType::TK_JOIN:
            case TokenType::TK_INNER:
            case TokenType::TK_LEFT:
            case TokenType::TK_RIGHT:
            case TokenType::TK_FULL:
            case TokenType::TK_OUTER:
            case TokenType::TK_CROSS:
            case TokenType::TK_NATURAL:
            case TokenType::TK_ON:
            case TokenType::TK_USING:
            case TokenType::TK_UNION:
            case TokenType::TK_SEMICOLON:
            case TokenType::TK_RPAREN:
            case TokenType::TK_EOF:
            case TokenType::TK_COMMA:
            case TokenType::TK_SET:
            case TokenType::TK_LOCK:
            case TokenType::TK_UNLOCK:
                return false;
            default:
                return true;  // Keywords not in the blocklist can be implicit aliases
        }
    }
};

} // namespace sql_parser

#endif // SQL_PARSER_SELECT_PARSER_H
