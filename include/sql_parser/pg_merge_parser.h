#ifndef SQL_PARSER_PG_MERGE_PARSER_H
#define SQL_PARSER_PG_MERGE_PARSER_H

#include "sql_parser/expression_parser.h"
#include "sql_parser/table_ref_parser.h"

namespace sql_parser {

// PostgreSQL DML shares strict target, assignment, and RETURNING productions.
// Query callbacks deliberately remain query-only, even inside DML expressions.
class PgDmlParser {
public:
    using Tok = Tokenizer<Dialect::PostgreSQL>;
    using Expr = ExpressionParser<Dialect::PostgreSQL>;
    PgDmlParser(Tok& tok, Arena& arena, SubqueryParseCallback<Dialect::PostgreSQL> query)
        : tok_(tok), arena_(arena), expr_(tok, arena), tables_(tok, arena, expr_), query_(query) {
        expr_.set_subquery_callback(query);
        tables_.set_subquery_callback(query);
    }

    AstNode* insert() {
        AstNode* root = node(NodeType::NODE_INSERT_STMT);
        if (!root) return error();
        if (!take("INTO")) return error();
        AstNode* relation = target(false, true);
        if (!relation) return error();
        root->add_child(relation);
        if (!insert_source(root, false)) return error();
        if (take("ON")) {
            if (!take("CONFLICT")) return error();
            AstNode* conflict = node(NodeType::NODE_ON_CONFLICT);
            if (!conflict) return error();
            if (take("(")) {
                AstNode* cols = node(NodeType::NODE_CONFLICT_TARGET);
                if (!cols) return error();
                do {
                    Token start = tok_.peek();
                    AstNode* value = expr_.parse_complete();
                    if (!value) return error();
                    const bool column = value->type == NodeType::NODE_COLUMN_REF && pg_column_name(start);
                    const bool expression = value->type == NodeType::NODE_EXPRESSION;
                    const bool function = value->type == NodeType::NODE_FUNCTION_CALL ||
                        value->type == NodeType::NODE_PG_EXTRACT || value->type == NodeType::NODE_PG_SUBSTRING ||
                        value->type == NodeType::NODE_PG_TRIM || value->type == NodeType::NODE_PG_NORMALIZE ||
                        value->type == NodeType::NODE_PG_JSON_XML;
                    if (!column && !expression && !function) return error();
                    cols->add_child(value);
                } while (take(","));
                if (!take(")")) return error();
                conflict->add_child(cols);
                if (is("WHERE")) {
                    AstNode* predicate = where();
                    if (!predicate) return error();
                    conflict->add_child(predicate);
                }
            } else if (take("ON")) {
                if (!take("CONSTRAINT")) return error();
                AstNode* constraint = node(NodeType::NODE_CONFLICT_TARGET, "ON CONSTRAINT");
                if (!constraint) return error();
                AstNode* name = identifier();
                if (!name) return error();
                constraint->add_child(name);
                conflict->add_child(constraint);
            }
            if (!take("DO")) return error();
            AstNode* action = node(NodeType::NODE_CONFLICT_ACTION);
            if (!action) return error();
            if (take("NOTHING")) action->set_value(ref("NOTHING"));
            else if (take("UPDATE")) {
                action->set_value(ref("UPDATE"));
                AstNode* assignments = set_clause();
                if (!assignments) return error();
                for (AstNode* c = assignments->first_child; c;) {
                    AstNode* next = c->next_sibling;
                    c->next_sibling = nullptr;
                    action->add_child(c);
                    c = next;
                }
                if (is("WHERE")) {
                    AstNode* predicate = where();
                    if (!predicate) return error();
                    action->add_child(predicate);
                }
            } else return error();
            conflict->add_child(action);
            root->add_child(conflict);
        }
        if (!returning(root)) return error();
        return root;
    }

    AstNode* update() {
        AstNode* root = node(NodeType::NODE_UPDATE_STMT);
        if (!root) return error();
        AstNode* relation = target(true, false);
        if (!relation) return error();
        root->add_child(relation);
        AstNode* assignments = set_clause();
        if (!assignments) return error();
        root->add_child(assignments);
        if (take("FROM")) {
            AstNode* from = tables_.parse_from_clause();
            if (!from || !valid_sources(from)) return error();
            root->add_child(from);
        }
        if (is("WHERE")) {
            AstNode* predicate = where();
            if (!predicate) return error();
            root->add_child(predicate);
        }
        if (!returning(root)) return error();
        return root;
    }

    AstNode* remove() {
        AstNode* root = node(NodeType::NODE_DELETE_STMT);
        if (!root) return error();
        if (!take("FROM")) return error();
        AstNode* relation = target(true, false);
        if (!relation) return error();
        root->add_child(relation);
        if (take("USING")) {
            AstNode* from = tables_.parse_from_clause();
            if (!from || !valid_sources(from)) return error();
            from->type = NodeType::NODE_DELETE_USING_CLAUSE;
            root->add_child(from);
        }
        if (is("WHERE")) {
            AstNode* predicate = where();
            if (!predicate) return error();
            root->add_child(predicate);
        }
        if (!returning(root)) return error();
        return root;
    }

    AstNode* merge() {
        AstNode* root = node(NodeType::NODE_MERGE_STMT);
        if (!root) return error();
        if (!take("INTO")) return error();
        AstNode* relation = target(true, false);
        if (!relation || !take("USING")) return error();
        root->add_child(relation);
        AstNode* source = tables_.parse_from_clause();
        if (!source || !valid_sources(source) || !take("ON")) return error();
        // MERGE accepts a single table_ref (which can contain joins), not a FROM list.
        for (AstNode* c = source->first_child ? source->first_child->next_sibling : nullptr; c; c = c->next_sibling)
            if (c->type != NodeType::NODE_JOIN_CLAUSE) return error();
        source->type = NodeType::NODE_DELETE_USING_CLAUSE;
        root->add_child(source);
        AstNode* condition = scalar_expression();
        if (!condition) return error();
        root->add_child(clause("ON", condition));
        if (!is("WHEN")) return error();
        while (take("WHEN")) {
            bool unmatched = take("NOT");
            if (!take("MATCHED")) return error();
            const char* kind = unmatched ? "NOT MATCHED" : "MATCHED";
            bool insert_allowed = unmatched;
            if (unmatched && take("BY")) {
                if (take("SOURCE")) { kind = "NOT MATCHED BY SOURCE"; insert_allowed = false; }
                else if (take("TARGET")) kind = "NOT MATCHED BY TARGET";
                else return error();
            }
            AstNode* branch = node(NodeType::NODE_MERGE_WHEN, kind);
            if (!branch) return error();
            if (take("AND")) {
                AstNode* predicate = scalar_expression();
                if (!predicate) return error();
                branch->add_child(clause("AND", predicate));
            }
            if (!take("THEN")) return error();
            AstNode* action = nullptr;
            if (take("DO")) {
                if (!take("NOTHING")) return error();
                action = clause("DO NOTHING");
            } else if (!insert_allowed && take("DELETE")) action = clause("DELETE");
            else if (!insert_allowed && take("UPDATE")) {
                AstNode* assignments = set_clause();
                if (!assignments) return error();
                action = clause("UPDATE", assignments);
            } else if (insert_allowed && take("INSERT")) {
                action = clause("INSERT");
                if (!insert_source(action, true)) return error();
            } else return error();
            branch->add_child(clause("THEN", action));
            root->add_child(branch);
        }
        if (!returning(root)) return error();
        return root;
    }

private:
    Tok& tok_;
    Arena& arena_;
    Expr expr_;
    TableRefParser<Dialect::PostgreSQL> tables_;
    SubqueryParseCallback<Dialect::PostgreSQL> query_;

    static StringRef ref(const char* value) { return {value, static_cast<uint32_t>(std::strlen(value))}; }
    bool is(const char* word) const { return Expr::keyword(tok_.peek(), word); }
    bool take(const char* word) { if (!is(word)) return false; tok_.skip(); return true; }
    AstNode* error() { return expr_.syntax_error(); }
    AstNode* node(NodeType type, const char* value = "") {
        AstNode* result = make_node(arena_, type, ref(value));
        return result ? result : error();
    }
    AstNode* clause(const char* value, AstNode* child = nullptr) {
        AstNode* result = node(NodeType::NODE_PG_DML_CLAUSE, value);
        if (result) result->add_child(child);
        return result;
    }
    AstNode* identifier() {
        Token token = tok_.next_token();
        if (!pg_column_name(token)) return error();
        AstNode* result = make_node_from_token(arena_, NodeType::NODE_IDENTIFIER, token,
            token.source.ptr != token.text.ptr ? FLAG_IDENT_DELIMITED : 0);
        return result ? result : error();
    }
    AstNode* column() {
        AstNode* name = identifier();
        if (!name) return nullptr;
        if (take(".")) {
            AstNode* qualified = node(NodeType::NODE_QUALIFIED_NAME);
            if (!qualified) return error();
            qualified->add_child(name);
            do {
                AstNode* part = identifier();
                if (!part) return nullptr;
                qualified->add_child(part);
            } while (take("."));
            return qualified;
        }
        return name;
    }
    AstNode* target(bool inheritance, bool insert_target) {
        bool only = inheritance && take("ONLY");
        bool parens = only && take("(");
        AstNode* name = column();
        if (!name || (parens && !take(")"))) return error();
        AstNode* relation = node(NodeType::NODE_TABLE_REF);
        if (!relation) return error();
        if (only) name = clause("ONLY", name);
        if (!name) return error();
        if (!only && inheritance && take("*")) {
            name = clause("", name);
            if (!name) return error();
            name->add_child(clause("*"));
        }
        relation->add_child(name);
        bool alias = take("AS");
        if (alias || (!insert_target && pg_column_name(tok_.peek()) && !is("SET") && !is("USING"))) {
            AstNode* alias_name = identifier();
            if (!alias_name) return error();
            // Existing alias nodes contain source spelling, including quotes.
            alias_name->type = NodeType::NODE_ALIAS;
            alias_name->set_value(alias_name->source());
            relation->add_child(alias_name);
        }
        return relation;
    }
    AstNode* assignment_target() {
        AstNode* name = column();
        if (!name) return nullptr;
        while (true) {
            if (take("[")) {
                AstNode* index = scalar_expression();
                if (!index || !take("]")) return error();
                AstNode* subscript = node(NodeType::NODE_ARRAY_SUBSCRIPT);
                if (!subscript) return error();
                subscript->add_child(name);
                subscript->add_child(index);
                name = subscript;
            } else if (take(".")) {
                AstNode* field = identifier();
                if (!field) return error();
                AstNode* access = node(NodeType::NODE_PG_ASSIGNMENT_FIELD);
                if (!access) return error();
                access->add_child(name);
                access->add_child(field);
                name = access;
            } else break;
        }
        return name;
    }
    bool valid_sources(const AstNode* ast) {
        if (ast->type == NodeType::NODE_TABLE_REF) {
            const AstNode* name = ast->first_child;
            if (!name) return false;
            const bool qualified = name->type == NodeType::NODE_QUALIFIED_NAME;
            if (qualified) name = name->first_child;
            if (name && name->type == NodeType::NODE_IDENTIFIER) {
                bool first = true;
                do {
                    StringRef source = name->source().empty() ? name->value() : name->source();
                    if (source.empty()) return false;
                    Tok checker;
                    checker.reset(source.ptr, source.len);
                    Token token = checker.next_token();
                    if (first ? !pg_column_name(token) : !pg_column_label(token)) return false;
                    first = false;
                    name = qualified ? name->next_sibling : nullptr;
                } while (name);
            }
        }
        for (const AstNode* c = ast->first_child; c; c = c->next_sibling)
            if (!valid_sources(c)) return false;
        return true;
    }
    AstNode* set_clause() {
        if (!take("SET")) return error();
        AstNode* result = node(NodeType::NODE_UPDATE_SET_CLAUSE);
        if (!result) return error();
        do {
            AstNode* lhs = nullptr;
            if (take("(")) {
                lhs = node(NodeType::NODE_TUPLE);
                if (!lhs) return error();
                do {
                    AstNode* col = assignment_target();
                    if (!col) return error();
                    lhs->add_child(col);
                } while (take(","));
                if (!take(")")) return error();
            } else lhs = assignment_target();
            if (!lhs || !take("=")) return error();
            AstNode* rhs = default_expression();
            if (!rhs) return error();
            AstNode* item = node(NodeType::NODE_UPDATE_SET_ITEM);
            if (!item) return error();
            item->add_child(lhs);
            item->add_child(rhs);
            result->add_child(item);
        } while (take(","));
        return result;
    }
    AstNode* scalar_expression() {
        AstNode* value = expr_.parse_complete();
        return value && value->type != NodeType::NODE_ASTERISK ? value : error();
    }
    AstNode* default_expression() {
        if (take("DEFAULT")) return clause("DEFAULT");
        return scalar_expression();
    }
    bool insert_source(AstNode* root, bool merge_action) {
        if (!root) return false;
        bool has_columns = false;
        if (is("(")) {
            Tok lookahead = tok_;
            lookahead.skip();
            if (!Expr::starts_query(lookahead.peek().type)) {
                tok_.skip();
                AstNode* cols = node(NodeType::NODE_INSERT_COLUMNS);
                if (!cols) return false;
                do {
                    AstNode* name = assignment_target();
                    if (!name) return false;
                    cols->add_child(name);
                } while (take(","));
                if (!take(")")) return false;
                root->add_child(cols);
                has_columns = true;
            }
        }
        bool overriding = take("OVERRIDING");
        if (overriding) {
            const char* kind = take("SYSTEM") ? "OVERRIDING SYSTEM VALUE" :
                               take("USER") ? "OVERRIDING USER VALUE" : nullptr;
            if (!kind || !take("VALUE")) return false;
            root->add_child(clause(kind));
        }
        if (take("DEFAULT")) {
            if (has_columns || overriding || !take("VALUES")) return false;
            root->add_child(node(NodeType::NODE_VALUES_CLAUSE, "DEFAULT VALUES"));
        } else if (is("VALUES") && !merge_action && query_) {
            AstNode* source = query_(tok_, arena_);
            if (!source) return false;
            if (source->type == NodeType::NODE_COMPOUND_QUERY && source->first_child &&
                !source->first_child->next_sibling && source->first_child->type == NodeType::NODE_VALUES_CLAUSE)
                source = source->first_child;
            root->add_child(source);
        } else if (take("VALUES")) {
            AstNode* values = node(NodeType::NODE_VALUES_CLAUSE);
            if (!values) return false;
            do {
                if (!take("(")) return false;
                AstNode* row = node(NodeType::NODE_VALUES_ROW);
                if (!row) return false;
                do {
                    AstNode* value = default_expression();
                    if (!value) return false;
                    row->add_child(value);
                } while (take(","));
                if (!take(")")) return false;
                values->add_child(row);
                if (merge_action) break;
            } while (take(","));
            root->add_child(values);
        } else {
            if (merge_action || !query_) return false;
            AstNode* source = query_(tok_, arena_);
            if (!source) return false;
            root->add_child(source);
        }
        return true;
    }
    AstNode* where() {
        if (!take("WHERE")) return error();
        AstNode* predicate = nullptr;
        if (take("CURRENT")) {
            if (!take("OF")) return error();
            AstNode* name = identifier();
            if (!name) return error();
            predicate = clause("CURRENT OF", name);
        } else predicate = scalar_expression();
        if (!predicate) return error();
        AstNode* result = node(NodeType::NODE_WHERE_CLAUSE);
        if (!result) return error();
        result->add_child(predicate);
        return result;
    }
    bool returning(AstNode* root) {
        if (!take("RETURNING")) return true;
        AstNode* result = node(NodeType::NODE_RETURNING_CLAUSE);
        if (!result) return false;
        if (take("WITH")) {
            if (!take("(")) return false;
            AstNode* options = node(NodeType::NODE_PG_RETURNING_OPTIONS);
            if (!options) return false;
            do {
                const char* kind = take("OLD") ? "OLD AS" : take("NEW") ? "NEW AS" : nullptr;
                if (!kind || !take("AS")) return false;
                AstNode* name = identifier();
                if (!name) return false;
                options->add_child(clause(kind, name));
            } while (take(","));
            if (!take(")")) return false;
            result->add_child(options);
        }
        do {
            AstNode* value = expr_.parse_complete();
            if (!value) return false;
            result->add_child(value);
            if (take("AS")) {
                Token alias = tok_.next_token();
                if (!pg_column_label(alias)) return false;
                AstNode* name = make_node(arena_, NodeType::NODE_ALIAS,
                    alias.source.empty() ? alias.text : alias.source);
                if (!name) { error(); return false; }
                result->add_child(name);
            } else if (tok_.peek().type == TokenType::TK_IDENTIFIER) {
                Token alias = tok_.next_token();
                AstNode* name = make_node(arena_, NodeType::NODE_ALIAS,
                    alias.source.empty() ? alias.text : alias.source);
                if (!name) { error(); return false; }
                result->add_child(name);
            }
        } while (take(","));
        root->add_child(result);
        return true;
    }
};

} // namespace sql_parser
#endif
