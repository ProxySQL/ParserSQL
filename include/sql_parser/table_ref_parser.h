#ifndef SQL_PARSER_TABLE_REF_PARSER_H
#define SQL_PARSER_TABLE_REF_PARSER_H

#include "sql_parser/common.h"
#include "sql_parser/token.h"
#include "sql_parser/tokenizer.h"
#include "sql_parser/ast.h"
#include "sql_parser/arena.h"
#include "sql_parser/expression_parser.h"

namespace sql_parser {

template <Dialect D>
class TableRefParser {
public:
    TableRefParser(Tokenizer<D>& tokenizer, Arena& arena,
                   ExpressionParser<D>& expr_parser)
        : tok_(tokenizer), arena_(arena), expr_parser_(expr_parser) {}

    void set_subquery_callback(SubqueryParseCallback<D> cb) { subquery_cb_ = cb; }

    // Parse a FROM clause: table_ref [, table_ref | JOIN ...]*
    AstNode* parse_from_clause() {
        if constexpr (D == Dialect::PostgreSQL) return parse_pg_from();
        AstNode* from = make_node(arena_, NodeType::NODE_FROM_CLAUSE);
        if (!from) return nullptr;

        // First table reference
        AstNode* table_ref = parse_table_reference(true);
        if (table_ref) from->add_child(table_ref);

        // Additional table refs (comma join) or explicit JOINs
        while (true) {
            Token t = tok_.peek();
            if (t.type == TokenType::TK_COMMA) {
                // Comma join: FROM t1, t2
                tok_.skip();
                AstNode* next_ref = parse_table_reference(true);
                if (next_ref) from->add_child(next_ref);
            } else if (is_join_start(t.type)) {
                // Explicit JOIN
                AstNode* join = parse_join(from->first_child);
                if (join) {
                    from->add_child(join);
                }
            } else {
                break;
            }
        }

        return from;
    }

    // Parse a single table reference (simple name, qualified name, subquery)
    AstNode* parse_table_reference(bool from_context = false) {
        Token t = tok_.peek();

        if constexpr (D == Dialect::PostgreSQL) {
            if (from_context && ExpressionParser<D>::keyword(t, "ROWS")) {
                auto look = tok_; look.skip();
                if (look.peek().type == TokenType::TK_FROM) return parse_rows_from();
            }
            if (from_context && ExpressionParser<D>::keyword(t, "LATERAL")) {
                tok_.skip();
                AstNode* ref = parse_table_reference(true);
                if (!ref || !ref->first_child ||
                    (ref->first_child->type != NodeType::NODE_SUBQUERY &&
                     ref->first_child->type != NodeType::NODE_FUNCTION_CALL &&
                     ref->first_child->type != NodeType::NODE_PG_JSON_XML &&
                     ref->first_child->type != NodeType::NODE_PG_ROWS_FROM))
                    return expr_parser_.syntax_error();
                AstNode* lateral = make_node(arena_, NodeType::NODE_LATERAL);
                if (!lateral) return expr_parser_.syntax_error();
                lateral->add_child(ref);
                return lateral;
            }
            if (from_context && (ExpressionParser<D>::keyword(t, "JSON_TABLE") ||
                                 ExpressionParser<D>::keyword(t, "XMLTABLE"))) {
                tok_.skip();
                auto* structured = PgSqlJsonParser<D, ExpressionParser<D>>(tok_, arena_, expr_parser_).parse(t);
                if (!structured) return expr_parser_.syntax_error();
                auto* ref = make_node(arena_, NodeType::NODE_TABLE_REF);
                if (!ref) return expr_parser_.syntax_error();
                ref->add_child(structured);
                parse_optional_alias(ref, from_context);
                return ref;
            }
        }

        // Subquery: (SELECT ...)
        if (t.type == TokenType::TK_LPAREN) {
            tok_.skip();
            if (ExpressionParser<D>::starts_query(tok_.peek().type)) {
                AstNode* subq = nullptr;
                if (subquery_cb_) {
                    subq = make_node(arena_, NodeType::NODE_SUBQUERY);
                    if (!subq) return expr_parser_.syntax_error();
                    AstNode* inner = subquery_cb_(tok_, arena_);
                    if constexpr (D == Dialect::PostgreSQL) {
                        if (!inner || tok_.peek().type != TokenType::TK_RPAREN)
                            return expr_parser_.syntax_error();
                    }
                    if (inner) subq->add_child(inner);
                    if (tok_.peek().type == TokenType::TK_RPAREN) tok_.skip();
                } else {
                    subq = make_node(arena_, NodeType::NODE_SUBQUERY);
                    // Legacy: skip to matching paren
                    int depth = 1;
                    while (depth > 0) {
                        Token st = tok_.next_token();
                        if (st.type == TokenType::TK_LPAREN) ++depth;
                        else if (st.type == TokenType::TK_RPAREN) --depth;
                        else if (st.type == TokenType::TK_EOF) break;
                    }
                }
                // Optional alias
                AstNode* ref = make_node(arena_, NodeType::NODE_TABLE_REF);
                if (!ref) return expr_parser_.syntax_error();
                ref->add_child(subq);
                parse_optional_alias(ref, from_context);
                return ref;
            }
            // Parenthesized table reference -- parse inner
            AstNode* inner = nullptr;
            if constexpr (D == Dialect::PostgreSQL) inner = parse_pg_joined();
            else inner = parse_table_reference(from_context);
            if constexpr (D == Dialect::PostgreSQL) {
                if (!inner || tok_.peek().type != TokenType::TK_RPAREN)
                    return expr_parser_.syntax_error();
                if (inner->type == NodeType::NODE_PG_JOIN_TREE ||
                    (inner->type == NodeType::NODE_TABLE_REF && inner->first_child &&
                     inner->first_child->type == NodeType::NODE_PG_TABLE_GROUP)) {
                    auto* group = make_node(arena_, NodeType::NODE_PG_TABLE_GROUP);
                    auto* ref = make_node(arena_, NodeType::NODE_TABLE_REF);
                    if (!group || !ref) return expr_parser_.syntax_error();
                    group->add_child(inner); ref->add_child(group); inner = ref;
                } else if (!inner->first_child || inner->first_child->type != NodeType::NODE_SUBQUERY ||
                           inner->first_child->next_sibling) return expr_parser_.syntax_error();
            }
            if (tok_.peek().type == TokenType::TK_RPAREN) tok_.skip();
            if constexpr (D == Dialect::PostgreSQL) {
                if (inner) parse_optional_alias(inner, from_context);
            }
            return inner;
        }

        // Simple table name or schema.table
        AstNode* ref = make_node(arena_, NodeType::NODE_TABLE_REF);
        if (!ref) return expr_parser_.syntax_error();
        bool only_parens = false;
        if constexpr (D == Dialect::PostgreSQL) {
            if (tok_.peek().type == TokenType::TK_ONLY) {
                tok_.skip(); ref->flags |= FLAG_TABLE_ONLY;
                only_parens = tok_.peek().type == TokenType::TK_LPAREN;
                if (only_parens) tok_.skip();
            }
            if (!pg_column_name(tok_.peek()) || starts_json_format(tok_)) return expr_parser_.syntax_error();
        }
        Token name = tok_.next_token();
        unsigned name_parts = 1;

        if (tok_.peek().type == TokenType::TK_DOT) {
            // Qualified: schema.table
            tok_.skip();
            Token table_name = tok_.next_token();
            if constexpr (D == Dialect::PostgreSQL) {
                if (!pg_column_label(table_name)) return expr_parser_.syntax_error();
            }
            AstNode* qname = make_node(arena_, NodeType::NODE_QUALIFIED_NAME);
            if (!qname) return expr_parser_.syntax_error();
            qname->add_child(make_identifier(name));
            qname->add_child(make_identifier(table_name));
            if constexpr (D == Dialect::PostgreSQL) {
                name_parts = 2;
                while (tok_.peek().type == TokenType::TK_DOT) {
                    ++name_parts;
                    tok_.skip(); auto part = tok_.next_token();
                    if (!pg_column_label(part)) return expr_parser_.syntax_error();
                    qname->add_child(make_identifier(part));
                }
            }
            ref->add_child(qname);
        } else {
            ref->add_child(make_identifier(name));
        }

        if constexpr (D == Dialect::PostgreSQL) {
            // RangeVar permits at most catalog.schema.relation; func_name can
            // contain more dotted names in PostgreSQL's raw grammar.
            if (name_parts > 3 && (!from_context || only_parens || tok_.peek().type != TokenType::TK_LPAREN))
                return expr_parser_.syntax_error();
            if (only_parens) {
                if (tok_.peek().type != TokenType::TK_RPAREN) return expr_parser_.syntax_error();
                tok_.skip();
            }
            if (!(ref->flags & FLAG_TABLE_ONLY) && tok_.peek().type == TokenType::TK_ASTERISK) {
                tok_.skip(); ref->flags |= FLAG_TABLE_INHERIT;
            }
            if (from_context && tok_.peek().type == TokenType::TK_LPAREN) {
                if (ref->flags & (FLAG_TABLE_ONLY | FLAG_TABLE_INHERIT)) return expr_parser_.syntax_error();
                // Table functions retain a structured name (including schema).
                AstNode* name_node = ref->first_child;
                AstNode* func = make_node(arena_, NodeType::NODE_FUNCTION_CALL);
                if (!func) return expr_parser_.syntax_error();
                func->flags = FLAG_FUNCTION_TABLE; // first child is the function name
                ref->first_child = nullptr;
                ref->add_child(func);
                func->add_child(name_node);
                tok_.skip();
                if (tok_.peek().type != TokenType::TK_RPAREN) {
                    while (true) {
                        AstNode* arg = expr_parser_.parse_argument(true);
                        if (!arg) return expr_parser_.syntax_error();
                        func->add_child(arg);
                        if (tok_.peek().type != TokenType::TK_COMMA) break;
                        tok_.skip();
                    }
                }
                if (tok_.peek().type != TokenType::TK_RPAREN) return expr_parser_.syntax_error();
                tok_.skip();
                if (tok_.peek().type == TokenType::TK_WITH) {
                    tok_.skip();
                    if (!ExpressionParser<D>::keyword(tok_.peek(), "ORDINALITY")) return expr_parser_.syntax_error();
                    tok_.skip();
                    auto* ordinality = make_node(arena_, NodeType::NODE_ORDINALITY);
                    if (!ordinality) return expr_parser_.syntax_error();
                    ref->add_child(ordinality);
                }
            }
        }

        // Optional alias
        parse_optional_alias(ref, from_context);
        if constexpr (D == Dialect::PostgreSQL) {
            if (from_context && ExpressionParser<D>::keyword(tok_.peek(), "TABLESAMPLE")) {
                if (!ref->first_child || (ref->first_child->type != NodeType::NODE_IDENTIFIER &&
                    ref->first_child->type != NodeType::NODE_QUALIFIED_NAME)) return expr_parser_.syntax_error();
                auto* sample = parse_sample();
                if (!sample) return expr_parser_.syntax_error();
                ref->add_child(sample);
            }
        }
        return ref;
    }

    // Parse a JOIN clause
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
        AstNode* right_ref = parse_table_reference(true);
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

    // Parse optional alias (AS name or implicit alias)
    void parse_optional_alias(AstNode* parent, bool from_context = false) {
        Token t = tok_.peek();
        if (starts_json_format(tok_)) return;
        AstNode* alias = nullptr;
        if (t.type == TokenType::TK_AS) {
            tok_.skip();
            if constexpr (D == Dialect::PostgreSQL) {
                if (from_context && tok_.peek().type == TokenType::TK_LPAREN && parent->first_child &&
                    parent->first_child->type == NodeType::NODE_FUNCTION_CALL) {
                    alias = make_node(arena_, NodeType::NODE_ALIAS);
                    if (!alias) { expr_parser_.syntax_error(); return; }
                }
            }
            if (!alias) t = tok_.next_token();
            if constexpr (D == Dialect::PostgreSQL) {
                if (!alias && !pg_column_name(t)) { expr_parser_.syntax_error(); return; }
            }
            if (!alias && !is_alias_start(t.type)) { expr_parser_.syntax_error(); return; }
            if (!alias) alias = make_node(arena_, NodeType::NODE_ALIAS, t.source.empty() ? t.text : t.source);
            if (!alias) { expr_parser_.syntax_error(); return; }
        } else if (is_alias_token(t) && (D != Dialect::PostgreSQL || pg_column_name(t))) {
            tok_.skip();
            alias = make_node(arena_, NodeType::NODE_ALIAS, t.source.empty() ? t.text : t.source);
            if (!alias) { expr_parser_.syntax_error(); return; }
        }
        if (!alias) return;
        parent->add_child(alias);
        if constexpr (D == Dialect::PostgreSQL) {
            if (from_context && tok_.peek().type == TokenType::TK_LPAREN) {
                tok_.skip();
                bool typed = false, names_only = false;
                bool function = parent->first_child && parent->first_child->type == NodeType::NODE_FUNCTION_CALL;
                while (true) {
                    Token column = tok_.peek();
                    if (!pg_column_name(column)) { expr_parser_.syntax_error(); return; }
                    tok_.skip();
                    AstNode* name = make_identifier(column);
                    if (tok_.peek().type != TokenType::TK_COMMA && tok_.peek().type != TokenType::TK_RPAREN) {
                        if (!function || names_only) { expr_parser_.syntax_error(); return; }
                        StringRef type = expr_parser_.parse_type_name();
                        if (type.empty()) { expr_parser_.syntax_error(); return; }
                        auto* definition = make_node(arena_, NodeType::NODE_FUNCTION_COLUMN);
                        if (!definition) { expr_parser_.syntax_error(); return; }
                        definition->add_child(name);
                        definition->add_child(make_node(arena_, NodeType::NODE_TYPE_NAME, type));
                        alias->add_child(definition); typed = true;
                    } else {
                        if (typed || alias->value().empty()) { expr_parser_.syntax_error(); return; }
                        names_only = true; alias->add_child(name);
                    }
                    if (tok_.peek().type != TokenType::TK_COMMA) break;
                    tok_.skip();
                }
                if (tok_.peek().type != TokenType::TK_RPAREN) { expr_parser_.syntax_error(); return; }
                tok_.skip();
            }
        }
    }

    // Check if a token can start a JOIN
    static bool is_join_start(TokenType type) {
        return type == TokenType::TK_JOIN || type == TokenType::TK_INNER ||
               type == TokenType::TK_LEFT || type == TokenType::TK_RIGHT ||
               type == TokenType::TK_FULL || type == TokenType::TK_OUTER ||
               type == TokenType::TK_CROSS || type == TokenType::TK_NATURAL;
    }

    static bool starts_json_format(Tokenizer<D> tokenizer) {
        if constexpr (D == Dialect::PostgreSQL) {
            if (ExpressionParser<D>::keyword(tokenizer.peek(), "FORMAT")) {
                tokenizer.skip();
                return ExpressionParser<D>::keyword(tokenizer.peek(), "JSON");
            }
        }
        return false;
    }

    static bool is_alias_token(const Token& token) {
        if constexpr (D == Dialect::PostgreSQL) {
            if (!pg_column_label(token)) return false;
            if (ExpressionParser<D>::keyword(token, "WINDOW") ||
                ExpressionParser<D>::keyword(token, "FILTER") ||
                ExpressionParser<D>::keyword(token, "LATERAL") ||
                ExpressionParser<D>::keyword(token, "TABLESAMPLE") ||
                ExpressionParser<D>::keyword(token, "REPEATABLE")) return false;
        }
        return is_alias_start(token.type);
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
            case TokenType::TK_OFFSET:
            case TokenType::TK_FETCH:
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
            case TokenType::TK_INTERSECT:
            case TokenType::TK_EXCEPT:
            case TokenType::TK_SEMICOLON:
            case TokenType::TK_RPAREN:
            case TokenType::TK_LPAREN:
            case TokenType::TK_EOF:
            case TokenType::TK_COMMA:
            case TokenType::TK_SET:
            case TokenType::TK_LOCK:
            case TokenType::TK_UNLOCK:
            case TokenType::TK_VALUES:
            case TokenType::TK_SELECT:
            case TokenType::TK_DEFAULT:
            case TokenType::TK_RETURNING:
            case TokenType::TK_CONFLICT:
            case TokenType::TK_DO:
            case TokenType::TK_NOTHING:
            case TokenType::TK_DUPLICATE:
            case TokenType::TK_OVER:
            case TokenType::TK_WITH:
            case TokenType::TK_PARTITION:
            case TokenType::TK_REPLACE:
                return false;
            default:
                return true;  // Keywords not in the blocklist can be implicit aliases
        }
    }

private:
    bool take(TokenType type) {
        if (tok_.peek().type != type) return false;
        tok_.skip(); return true;
    }
    AstNode* parse_pg_from() {
        auto* from = make_node(arena_, NodeType::NODE_FROM_CLAUSE);
        if (!from) return expr_parser_.syntax_error();
        while (true) {
            auto* item = parse_pg_joined();
            if (!item) return expr_parser_.syntax_error();
            from->add_child(item);
            if (!take(TokenType::TK_COMMA)) break;
        }
        // Preserve the execution engine's established representation for a
        // single simple join chain. Comma items retain their own join scope.
        if (from->first_child && !from->first_child->next_sibling && flattenable(from->first_child)) {
            auto* tree = from->first_child; from->first_child = nullptr;
            flatten_join(from, tree);
        }
        return from;
    }
    static bool flattenable(const AstNode* node) {
        if (node->type == NodeType::NODE_TABLE_REF) return true;
        if (node->type != NodeType::NODE_PG_JOIN_TREE) return false;
        const auto* left = node->first_child;
        const auto* right = left->next_sibling;
        const auto* qual = right->next_sibling;
        return right->type == NodeType::NODE_TABLE_REF && flattenable(left) &&
            (!qual || qual->type != NodeType::NODE_PG_JOIN_USING || qual->value().empty());
    }
    static void flatten_join(AstNode* from, AstNode* node) {
        if (node->type == NodeType::NODE_TABLE_REF) { from->add_child(node); return; }
        auto* left = node->first_child;
        auto* right = left->next_sibling;
        left->next_sibling = nullptr;
        flatten_join(from, left);
        node->type = NodeType::NODE_JOIN_CLAUSE; node->first_child = right;
        if (right->next_sibling && right->next_sibling->type == NodeType::NODE_PG_JOIN_USING) {
            auto* qual = right->next_sibling; qual->type = NodeType::NODE_IDENTIFIER;
            qual->value_ptr = "USING"; qual->value_len = 5;
        }
        from->add_child(node);
    }
    AstNode* parse_pg_joined() {
        auto* left = parse_table_reference(true);
        if (!left) return expr_parser_.syntax_error();
        while (is_join_start(tok_.peek().type)) {
            Token start = tok_.peek();
            bool natural = take(TokenType::TK_NATURAL);
            bool cross = !natural && take(TokenType::TK_CROSS);
            if (!cross) {
                if (take(TokenType::TK_LEFT) || take(TokenType::TK_RIGHT) || take(TokenType::TK_FULL))
                    take(TokenType::TK_OUTER);
                else take(TokenType::TK_INNER);
            }
            Token end = tok_.peek();
            if (!take(TokenType::TK_JOIN)) return expr_parser_.syntax_error();
            auto* right = (natural || cross) ? parse_table_reference(true) : parse_pg_joined();
            if (!right) return expr_parser_.syntax_error();
            AstNode* qual = nullptr;
            if (!natural && !cross) {
                if (take(TokenType::TK_ON)) {
                    qual = expr_parser_.parse_complete();
                    if (!qual || qual->type == NodeType::NODE_ASTERISK) return expr_parser_.syntax_error();
                } else if (take(TokenType::TK_USING)) {
                    if (!take(TokenType::TK_LPAREN)) return expr_parser_.syntax_error();
                    qual = make_node(arena_, NodeType::NODE_PG_JOIN_USING);
                    if (!qual) return expr_parser_.syntax_error();
                    do {
                        auto col = tok_.next_token();
                        if (!pg_column_name(col)) return expr_parser_.syntax_error();
                        qual->add_child(make_identifier(col));
                    } while (take(TokenType::TK_COMMA));
                    if (!take(TokenType::TK_RPAREN)) return expr_parser_.syntax_error();
                    if (take(TokenType::TK_AS)) {
                        auto alias = tok_.next_token();
                        if (!pg_column_name(alias)) return expr_parser_.syntax_error();
                        auto value = alias.source.empty() ? alias.text : alias.source;
                        qual->value_ptr = value.ptr; qual->value_len = value.len;
                    }
                } else return expr_parser_.syntax_error();
            }
            auto* join = make_node(arena_, NodeType::NODE_PG_JOIN_TREE,
                {start.text.ptr, static_cast<uint32_t>(end.text.ptr + end.text.len - start.text.ptr)});
            if (!join) return expr_parser_.syntax_error();
            join->add_child(left); join->add_child(right); join->add_child(qual); left = join;
        }
        return left;
    }
    AstNode* parse_sample() {
        tok_.skip();
        auto* node = make_node(arena_, NodeType::NODE_PG_TABLESAMPLE);
        auto* name = make_node(arena_, NodeType::NODE_QUALIFIED_NAME);
        auto* args = make_node(arena_, NodeType::NODE_TUPLE);
        if (!node || !name || !args) return expr_parser_.syntax_error();
        auto token = tok_.next_token();
        const bool qualified = tok_.peek().type == TokenType::TK_DOT;
        if (!(qualified ? pg_column_name(token) : pg_type_function_name(token)))
            return expr_parser_.syntax_error();
        name->add_child(make_identifier(token));
        while (take(TokenType::TK_DOT)) {
            token = tok_.next_token();
            if (!pg_column_label(token)) return expr_parser_.syntax_error();
            name->add_child(make_identifier(token));
        }
        if (!take(TokenType::TK_LPAREN)) return expr_parser_.syntax_error();
        do {
            auto* arg = expr_parser_.parse_complete();
            if (!arg || arg->type == NodeType::NODE_ASTERISK) return expr_parser_.syntax_error();
            args->add_child(arg);
        } while (take(TokenType::TK_COMMA));
        if (!take(TokenType::TK_RPAREN)) return expr_parser_.syntax_error();
        node->add_child(name); node->add_child(args);
        if (ExpressionParser<D>::keyword(tok_.peek(), "REPEATABLE")) {
            tok_.skip(); if (!take(TokenType::TK_LPAREN)) return expr_parser_.syntax_error();
            auto* repeat = expr_parser_.parse_complete();
            if (!repeat || repeat->type == NodeType::NODE_ASTERISK || !take(TokenType::TK_RPAREN))
                return expr_parser_.syntax_error();
            node->add_child(repeat);
        }
        return node;
    }
    AstNode* parse_rows_from() {
        tok_.skip(); tok_.skip();
        if (!take(TokenType::TK_LPAREN)) return expr_parser_.syntax_error();
        auto* ref = make_node(arena_, NodeType::NODE_TABLE_REF);
        auto* rows = make_node(arena_, NodeType::NODE_PG_ROWS_FROM);
        if (!ref || !rows) return expr_parser_.syntax_error();
        do {
            auto* item = parse_table_reference(true);
            if (!item || item->type != NodeType::NODE_TABLE_REF || !item->first_child ||
                item->first_child->type != NodeType::NODE_FUNCTION_CALL) return expr_parser_.syntax_error();
            auto* alias = item->first_child->next_sibling;
            if (alias && (alias->type != NodeType::NODE_ALIAS || !alias->value().empty()))
                return expr_parser_.syntax_error();
            rows->add_child(item);
        } while (take(TokenType::TK_COMMA));
        if (!take(TokenType::TK_RPAREN)) return expr_parser_.syntax_error();
        ref->add_child(rows);
        if (take(TokenType::TK_WITH)) {
            if (!ExpressionParser<D>::keyword(tok_.peek(), "ORDINALITY")) return expr_parser_.syntax_error();
            tok_.skip(); auto* ord = make_node(arena_, NodeType::NODE_ORDINALITY);
            if (!ord) return expr_parser_.syntax_error();
            ref->add_child(ord);
        }
        parse_optional_alias(ref, true);
        return ref;
    }
    AstNode* make_identifier(const Token& token) {
        AstNode* node = make_node_from_token(arena_, NodeType::NODE_IDENTIFIER, token);
        if (!node) return expr_parser_.syntax_error();
        if (node && token.type == TokenType::TK_IDENTIFIER && token.source.ptr != token.text.ptr)
            node->flags |= FLAG_IDENT_DELIMITED;
        return node;
    }

    Tokenizer<D>& tok_;
    Arena& arena_;
    ExpressionParser<D>& expr_parser_;
    SubqueryParseCallback<D> subquery_cb_ = nullptr;
};

} // namespace sql_parser

#endif // SQL_PARSER_TABLE_REF_PARSER_H
