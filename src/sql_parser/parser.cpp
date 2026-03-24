#include "sql_parser/parser.h"
#include "sql_parser/expression_parser.h"
#include "sql_parser/set_parser.h"
#include "sql_parser/select_parser.h"
#include "sql_parser/compound_query_parser.h"
#include "sql_parser/insert_parser.h"
#include "sql_parser/update_parser.h"
#include "sql_parser/delete_parser.h"

namespace sql_parser {

template <Dialect D>
Parser<D>::Parser(const ParserConfig& config)
    : arena_(config.arena_block_size, config.arena_max_size),
      stmt_cache_(config.stmt_cache_capacity) {}

template <Dialect D>
void Parser<D>::reset() {
    arena_.reset();
}

template <Dialect D>
ParseResult Parser<D>::parse(const char* sql, size_t len) {
    arena_.reset();
    tokenizer_.reset(sql, len);
    return classify_and_dispatch();
}

template <Dialect D>
ParseResult Parser<D>::classify_and_dispatch() {
    Token first = tokenizer_.next_token();

    if (first.type == TokenType::TK_EOF) {
        ParseResult r;
        r.status = ParseResult::ERROR;
        r.stmt_type = StmtType::UNKNOWN;
        return r;
    }

    switch (first.type) {
        case TokenType::TK_SELECT:   return parse_select();
        case TokenType::TK_LPAREN: {
            // Parenthesized SELECT / compound query: (SELECT ...) UNION ...
            Token next = tokenizer_.peek();
            if (next.type == TokenType::TK_SELECT || next.type == TokenType::TK_LPAREN) {
                return parse_select_from_lparen();
            }
            return extract_unknown(first);
        }
        case TokenType::TK_SET:      return parse_set();
        case TokenType::TK_INSERT:   return parse_insert(false);
        case TokenType::TK_UPDATE:   return parse_update();
        case TokenType::TK_DELETE:   return parse_delete();
        case TokenType::TK_REPLACE:  return parse_insert(true);
        case TokenType::TK_BEGIN:
        case TokenType::TK_START:
        case TokenType::TK_COMMIT:
        case TokenType::TK_ROLLBACK:
        case TokenType::TK_SAVEPOINT:return extract_transaction(first);
        case TokenType::TK_USE:      return extract_use(first);
        case TokenType::TK_SHOW:     return extract_show(first);
        case TokenType::TK_PREPARE:  return extract_prepare(first);
        case TokenType::TK_EXECUTE:  return extract_execute(first);
        case TokenType::TK_DEALLOCATE: return extract_deallocate(first);
        case TokenType::TK_CREATE:
        case TokenType::TK_ALTER:
        case TokenType::TK_DROP:
        case TokenType::TK_TRUNCATE: return extract_ddl(first);
        case TokenType::TK_GRANT:
        case TokenType::TK_REVOKE:   return extract_acl(first);
        case TokenType::TK_LOCK:
        case TokenType::TK_UNLOCK:   return extract_lock(first);
        case TokenType::TK_LOAD:     return extract_load(first);
        case TokenType::TK_RESET:    return extract_reset(first);
        default:                     return extract_unknown(first);
    }
}

// ---- Tier 1 stubs ----

template <Dialect D>
ParseResult Parser<D>::parse_select() {
    ParseResult r;
    r.stmt_type = StmtType::SELECT;

    CompoundQueryParser<D> compound_parser(tokenizer_, arena_);
    AstNode* ast = compound_parser.parse();

    if (ast) {
        r.status = ParseResult::OK;
        r.ast = ast;
    } else {
        r.status = ParseResult::PARTIAL;
    }

    scan_to_end(r);
    return r;
}

template <Dialect D>
ParseResult Parser<D>::parse_select_from_lparen() {
    // Called when classifier consumed '(' and peeked SELECT or '('
    // We need to parse the inner compound query, then check for set operators
    // after the closing ')'.
    //
    // Strategy: parse inner as a fresh compound expression, expect ')',
    // then check if a set operator follows (making this a compound query).

    ParseResult r;
    r.stmt_type = StmtType::SELECT;

    // We're inside '(' already consumed.
    // Parse inner: could be SELECT or another '('
    AstNode* inner = nullptr;
    if (tokenizer_.peek().type == TokenType::TK_SELECT) {
        tokenizer_.skip(); // consume SELECT
        SelectParser<D> sp(tokenizer_, arena_);
        inner = sp.parse();

        // Check for set operators inside the parens
        Token t = tokenizer_.peek();
        while (t.type == TokenType::TK_UNION ||
               t.type == TokenType::TK_INTERSECT ||
               t.type == TokenType::TK_EXCEPT) {
            tokenizer_.skip();
            StringRef op_text = t.text;
            uint16_t flags = 0;
            if (tokenizer_.peek().type == TokenType::TK_ALL) {
                tokenizer_.skip();
                flags = FLAG_SET_OP_ALL;
            }
            // Next SELECT
            if (tokenizer_.peek().type == TokenType::TK_SELECT) {
                tokenizer_.skip();
            }
            SelectParser<D> sp2(tokenizer_, arena_);
            AstNode* right = sp2.parse();

            AstNode* setop = make_node(arena_, NodeType::NODE_SET_OPERATION, op_text);
            if (setop) {
                setop->flags = flags;
                setop->add_child(inner);
                if (right) setop->add_child(right);
                inner = setop;
            }
            t = tokenizer_.peek();
        }
    } else {
        // Nested parenthesized -- recursively handle
        // This is an edge case; for now parse as compound
        CompoundQueryParser<D> cp(tokenizer_, arena_);
        inner = cp.parse();
    }

    // Expect closing ')'
    if (tokenizer_.peek().type == TokenType::TK_RPAREN) {
        tokenizer_.skip();
    }

    // Now check if a set operator follows after the ')'
    Token t = tokenizer_.peek();
    if (t.type == TokenType::TK_UNION ||
        t.type == TokenType::TK_INTERSECT ||
        t.type == TokenType::TK_EXCEPT) {
        // This is a compound query starting with a parenthesized operand.
        // Use CompoundQueryParser to continue, but we already have the left operand.
        // We'll build the compound manually.
        AstNode* left = inner;
        while (true) {
            t = tokenizer_.peek();
            if (t.type != TokenType::TK_UNION &&
                t.type != TokenType::TK_INTERSECT &&
                t.type != TokenType::TK_EXCEPT) break;

            tokenizer_.skip();
            StringRef op_text = t.text;
            uint16_t flags = 0;
            if (tokenizer_.peek().type == TokenType::TK_ALL) {
                tokenizer_.skip();
                flags = FLAG_SET_OP_ALL;
            }

            AstNode* right = nullptr;
            if (tokenizer_.peek().type == TokenType::TK_LPAREN) {
                // Parenthesized right operand
                tokenizer_.skip();
                if (tokenizer_.peek().type == TokenType::TK_SELECT) {
                    tokenizer_.skip();
                }
                SelectParser<D> sp3(tokenizer_, arena_);
                right = sp3.parse();
                if (tokenizer_.peek().type == TokenType::TK_RPAREN) {
                    tokenizer_.skip();
                }
            } else if (tokenizer_.peek().type == TokenType::TK_SELECT) {
                tokenizer_.skip();
                SelectParser<D> sp3(tokenizer_, arena_);
                right = sp3.parse();
            }

            AstNode* setop = make_node(arena_, NodeType::NODE_SET_OPERATION, op_text);
            if (setop) {
                setop->flags = flags;
                setop->add_child(left);
                if (right) setop->add_child(right);
                left = setop;
            }
        }

        // Wrap in COMPOUND_QUERY
        AstNode* compound = make_node(arena_, NodeType::NODE_COMPOUND_QUERY);
        if (compound) {
            compound->add_child(left);

            // Trailing ORDER BY
            if (tokenizer_.peek().type == TokenType::TK_ORDER) {
                tokenizer_.skip();
                if (tokenizer_.peek().type == TokenType::TK_BY) tokenizer_.skip();
                ExpressionParser<D> ep(tokenizer_, arena_);
                AstNode* order_by = make_node(arena_, NodeType::NODE_ORDER_BY_CLAUSE);
                if (order_by) {
                    while (true) {
                        AstNode* expr = ep.parse();
                        if (!expr) break;
                        AstNode* item = make_node(arena_, NodeType::NODE_ORDER_BY_ITEM);
                        item->add_child(expr);
                        Token dir = tokenizer_.peek();
                        if (dir.type == TokenType::TK_ASC || dir.type == TokenType::TK_DESC) {
                            tokenizer_.skip();
                            item->add_child(make_node(arena_, NodeType::NODE_IDENTIFIER, dir.text));
                        }
                        order_by->add_child(item);
                        if (tokenizer_.peek().type == TokenType::TK_COMMA) {
                            tokenizer_.skip();
                        } else {
                            break;
                        }
                    }
                    compound->add_child(order_by);
                }
            }

            // Trailing LIMIT
            if (tokenizer_.peek().type == TokenType::TK_LIMIT) {
                tokenizer_.skip();
                ExpressionParser<D> ep(tokenizer_, arena_);
                AstNode* limit = make_node(arena_, NodeType::NODE_LIMIT_CLAUSE);
                if (limit) {
                    AstNode* val = ep.parse();
                    if (val) limit->add_child(val);
                    if (tokenizer_.peek().type == TokenType::TK_OFFSET) {
                        tokenizer_.skip();
                        AstNode* off = ep.parse();
                        if (off) limit->add_child(off);
                    }
                    compound->add_child(limit);
                }
            }

            r.status = ParseResult::OK;
            r.ast = compound;
        }
    } else {
        // Just a parenthesized SELECT, no compound
        if (inner) {
            r.status = ParseResult::OK;
            r.ast = inner;
        } else {
            r.status = ParseResult::PARTIAL;
        }
    }

    scan_to_end(r);
    return r;
}

template <Dialect D>
ParseResult Parser<D>::parse_set() {
    ParseResult r;
    r.stmt_type = StmtType::SET;

    SetParser<D> set_parser(tokenizer_, arena_);
    AstNode* ast = set_parser.parse();

    if (ast) {
        r.status = ParseResult::OK;
        r.ast = ast;
    } else {
        r.status = ParseResult::PARTIAL;
    }

    scan_to_end(r);
    return r;
}

template <Dialect D>
ParseResult Parser<D>::parse_insert(bool is_replace) {
    ParseResult r;
    r.stmt_type = is_replace ? StmtType::REPLACE : StmtType::INSERT;

    InsertParser<D> insert_parser(tokenizer_, arena_, is_replace);
    AstNode* ast = insert_parser.parse();

    if (ast) {
        r.status = ParseResult::OK;
        r.ast = ast;

        // Extract table_name/schema_name from AST for backward compatibility
        for (const AstNode* child = ast->first_child; child; child = child->next_sibling) {
            if (child->type == NodeType::NODE_TABLE_REF) {
                const AstNode* name_node = child->first_child;
                if (name_node && name_node->type == NodeType::NODE_QUALIFIED_NAME) {
                    // schema.table
                    const AstNode* schema = name_node->first_child;
                    const AstNode* table = schema ? schema->next_sibling : nullptr;
                    if (schema) r.schema_name = schema->value();
                    if (table) r.table_name = table->value();
                } else if (name_node && name_node->type == NodeType::NODE_IDENTIFIER) {
                    r.table_name = name_node->value();
                }
                break;
            }
        }
    } else {
        r.status = ParseResult::PARTIAL;
    }

    scan_to_end(r);
    return r;
}

template <Dialect D>
ParseResult Parser<D>::parse_update() {
    ParseResult r;
    r.stmt_type = StmtType::UPDATE;

    UpdateParser<D> update_parser(tokenizer_, arena_);
    AstNode* ast = update_parser.parse();

    if (ast) {
        r.status = ParseResult::OK;
        r.ast = ast;

        // Extract table_name/schema_name from AST for backward compatibility
        for (const AstNode* child = ast->first_child; child; child = child->next_sibling) {
            if (child->type == NodeType::NODE_TABLE_REF) {
                const AstNode* name_node = child->first_child;
                if (name_node && name_node->type == NodeType::NODE_QUALIFIED_NAME) {
                    const AstNode* schema = name_node->first_child;
                    const AstNode* table = schema ? schema->next_sibling : nullptr;
                    if (schema) r.schema_name = schema->value();
                    if (table) r.table_name = table->value();
                } else if (name_node && name_node->type == NodeType::NODE_IDENTIFIER) {
                    r.table_name = name_node->value();
                }
                break;
            }
        }
    } else {
        r.status = ParseResult::PARTIAL;
    }

    scan_to_end(r);
    return r;
}

template <Dialect D>
ParseResult Parser<D>::parse_delete() {
    ParseResult r;
    r.stmt_type = StmtType::DELETE_STMT;

    DeleteParser<D> delete_parser(tokenizer_, arena_);
    AstNode* ast = delete_parser.parse();

    if (ast) {
        r.status = ParseResult::OK;
        r.ast = ast;

        // Extract table_name/schema_name from AST for backward compatibility
        for (const AstNode* child = ast->first_child; child; child = child->next_sibling) {
            if (child->type == NodeType::NODE_TABLE_REF) {
                const AstNode* name_node = child->first_child;
                if (name_node && name_node->type == NodeType::NODE_QUALIFIED_NAME) {
                    const AstNode* schema = name_node->first_child;
                    const AstNode* table = schema ? schema->next_sibling : nullptr;
                    if (schema) r.schema_name = schema->value();
                    if (table) r.table_name = table->value();
                } else if (name_node && name_node->type == NodeType::NODE_IDENTIFIER) {
                    r.table_name = name_node->value();
                }
                break;
            }
        }
    } else {
        r.status = ParseResult::PARTIAL;
    }

    scan_to_end(r);
    return r;
}

// ---- Helpers ----

template <Dialect D>
Token Parser<D>::read_table_name(StringRef& schema_out) {
    Token name = tokenizer_.next_token();
    if (name.type != TokenType::TK_IDENTIFIER &&
        name.type != TokenType::TK_EOF) {
        // Keywords used as table names (e.g., CREATE TABLE `user`)
        // The tokenizer returns keyword tokens for reserved words.
        // Accept any non-punctuation token as a potential name.
    }

    // Check for qualified name: schema.table
    if (tokenizer_.peek().type == TokenType::TK_DOT) {
        schema_out = name.text;
        tokenizer_.skip();  // consume dot
        Token table = tokenizer_.next_token();
        return table;
    }

    schema_out = StringRef{};
    return name;
}

template <Dialect D>
void Parser<D>::scan_to_end(ParseResult& result) {
    while (true) {
        Token t = tokenizer_.next_token();
        if (t.type == TokenType::TK_EOF) break;
        if (t.type == TokenType::TK_SEMICOLON) {
            Token next = tokenizer_.peek();
            if (next.type != TokenType::TK_EOF) {
                const char* remaining_start = next.text.ptr;
                const char* input_end = tokenizer_.input_end();
                result.remaining = StringRef{
                    remaining_start,
                    static_cast<uint32_t>(input_end - remaining_start)
                };
            }
            break;
        }
    }
}

// ---- Tier 2 Extractors ----

template <Dialect D>
ParseResult Parser<D>::extract_insert(const Token& /* first */) {
    ParseResult r;
    r.status = ParseResult::OK;
    r.stmt_type = StmtType::INSERT;

    // Expect optional INTO
    Token t = tokenizer_.peek();
    if (t.type == TokenType::TK_INTO) {
        tokenizer_.skip();
    }

    // Read table name
    Token table = read_table_name(r.schema_name);
    r.table_name = table.text;
    scan_to_end(r);
    return r;
}

template <Dialect D>
ParseResult Parser<D>::extract_update(const Token& /* first */) {
    ParseResult r;
    r.status = ParseResult::OK;
    r.stmt_type = StmtType::UPDATE;

    // Optional LOW_PRIORITY / IGNORE
    Token t = tokenizer_.peek();
    while (t.type == TokenType::TK_LOW_PRIORITY || t.type == TokenType::TK_IGNORE) {
        tokenizer_.skip();
        t = tokenizer_.peek();
    }

    Token table = read_table_name(r.schema_name);
    r.table_name = table.text;
    scan_to_end(r);
    return r;
}

template <Dialect D>
ParseResult Parser<D>::extract_delete(const Token& /* first */) {
    ParseResult r;
    r.status = ParseResult::OK;
    r.stmt_type = StmtType::DELETE_STMT;

    // Optional LOW_PRIORITY / QUICK / IGNORE
    Token t = tokenizer_.peek();
    while (t.type == TokenType::TK_LOW_PRIORITY ||
           t.type == TokenType::TK_QUICK ||
           t.type == TokenType::TK_IGNORE) {
        tokenizer_.skip();
        t = tokenizer_.peek();
    }

    // Expect FROM
    if (tokenizer_.peek().type == TokenType::TK_FROM) {
        tokenizer_.skip();
    }

    Token table = read_table_name(r.schema_name);
    r.table_name = table.text;
    scan_to_end(r);
    return r;
}

template <Dialect D>
ParseResult Parser<D>::extract_replace(const Token& /* first */) {
    ParseResult r;
    r.status = ParseResult::OK;
    r.stmt_type = StmtType::REPLACE;

    if (tokenizer_.peek().type == TokenType::TK_INTO) {
        tokenizer_.skip();
    }

    Token table = read_table_name(r.schema_name);
    r.table_name = table.text;
    scan_to_end(r);
    return r;
}

template <Dialect D>
ParseResult Parser<D>::extract_transaction(const Token& first) {
    ParseResult r;
    r.status = ParseResult::OK;

    switch (first.type) {
        case TokenType::TK_BEGIN:
            r.stmt_type = StmtType::BEGIN;
            break;
        case TokenType::TK_START:
            r.stmt_type = StmtType::START_TRANSACTION;
            // consume TRANSACTION if present
            if (tokenizer_.peek().type == TokenType::TK_TRANSACTION)
                tokenizer_.skip();
            break;
        case TokenType::TK_COMMIT:
            r.stmt_type = StmtType::COMMIT;
            break;
        case TokenType::TK_ROLLBACK:
            r.stmt_type = StmtType::ROLLBACK;
            break;
        case TokenType::TK_SAVEPOINT:
            r.stmt_type = StmtType::SAVEPOINT;
            break;
        default:
            r.stmt_type = StmtType::UNKNOWN;
            break;
    }

    scan_to_end(r);
    return r;
}

template <Dialect D>
ParseResult Parser<D>::extract_use(const Token& /* first */) {
    ParseResult r;
    r.status = ParseResult::OK;
    r.stmt_type = StmtType::USE;

    Token db = tokenizer_.next_token();
    r.database_name = db.text;
    scan_to_end(r);
    return r;
}

template <Dialect D>
ParseResult Parser<D>::extract_show(const Token& /* first */) {
    ParseResult r;
    r.status = ParseResult::OK;
    r.stmt_type = StmtType::SHOW;
    scan_to_end(r);
    return r;
}

template <Dialect D>
ParseResult Parser<D>::extract_prepare(const Token& /* first */) {
    ParseResult r;
    r.status = ParseResult::OK;
    r.stmt_type = StmtType::PREPARE;
    scan_to_end(r);
    return r;
}

template <Dialect D>
ParseResult Parser<D>::extract_execute(const Token& /* first */) {
    ParseResult r;
    r.status = ParseResult::OK;
    r.stmt_type = StmtType::EXECUTE;
    scan_to_end(r);
    return r;
}

template <Dialect D>
ParseResult Parser<D>::extract_deallocate(const Token& /* first */) {
    ParseResult r;
    r.status = ParseResult::OK;
    r.stmt_type = StmtType::DEALLOCATE;
    scan_to_end(r);
    return r;
}

template <Dialect D>
ParseResult Parser<D>::extract_ddl(const Token& first) {
    ParseResult r;
    r.status = ParseResult::OK;

    switch (first.type) {
        case TokenType::TK_CREATE:   r.stmt_type = StmtType::CREATE; break;
        case TokenType::TK_ALTER:    r.stmt_type = StmtType::ALTER; break;
        case TokenType::TK_DROP:     r.stmt_type = StmtType::DROP; break;
        case TokenType::TK_TRUNCATE: r.stmt_type = StmtType::TRUNCATE; break;
        default:                     r.stmt_type = StmtType::UNKNOWN; break;
    }

    // Try to extract object name: CREATE/ALTER/DROP [IF EXISTS/NOT EXISTS] TABLE/INDEX/VIEW name
    Token t = tokenizer_.next_token();

    // Skip optional IF [NOT] EXISTS
    if (t.type == TokenType::TK_IF) {
        t = tokenizer_.next_token(); // NOT or EXISTS
        if (t.type == TokenType::TK_NOT) {
            t = tokenizer_.next_token(); // EXISTS
        }
        // Skip EXISTS
        t = tokenizer_.next_token(); // should be TABLE/INDEX/etc.
    }

    // Now t should be TABLE, INDEX, VIEW, DATABASE, SCHEMA, or a name
    if (t.type == TokenType::TK_TABLE || t.type == TokenType::TK_INDEX ||
        t.type == TokenType::TK_VIEW || t.type == TokenType::TK_DATABASE ||
        t.type == TokenType::TK_SCHEMA) {
        // Optional IF [NOT] EXISTS after object type for CREATE/DROP
        Token maybe_if = tokenizer_.peek();
        if (maybe_if.type == TokenType::TK_IF) {
            tokenizer_.skip(); // IF
            Token next = tokenizer_.next_token();
            if (next.type == TokenType::TK_NOT) {
                tokenizer_.skip(); // EXISTS
            }
        }
        Token name = read_table_name(r.schema_name);
        r.table_name = name.text;
    }

    scan_to_end(r);
    return r;
}

template <Dialect D>
ParseResult Parser<D>::extract_acl(const Token& first) {
    ParseResult r;
    r.status = ParseResult::OK;
    r.stmt_type = (first.type == TokenType::TK_GRANT) ? StmtType::GRANT : StmtType::REVOKE;
    scan_to_end(r);
    return r;
}

template <Dialect D>
ParseResult Parser<D>::extract_lock(const Token& first) {
    ParseResult r;
    r.status = ParseResult::OK;
    r.stmt_type = (first.type == TokenType::TK_LOCK) ? StmtType::LOCK : StmtType::UNLOCK;
    scan_to_end(r);
    return r;
}

template <Dialect D>
ParseResult Parser<D>::extract_load(const Token& /* first */) {
    ParseResult r;
    r.status = ParseResult::OK;
    r.stmt_type = StmtType::LOAD_DATA;
    scan_to_end(r);
    return r;
}

template <Dialect D>
ParseResult Parser<D>::extract_reset(const Token& /* first */) {
    ParseResult r;
    r.status = ParseResult::OK;
    r.stmt_type = StmtType::RESET;
    scan_to_end(r);
    return r;
}

template <Dialect D>
ParseResult Parser<D>::extract_unknown(const Token& /* first */) {
    ParseResult r;
    r.status = ParseResult::OK;
    r.stmt_type = StmtType::UNKNOWN;
    scan_to_end(r);
    return r;
}

// ---- Prepared statement support ----

template <Dialect D>
ParseResult Parser<D>::parse_and_cache(const char* sql, size_t len, uint32_t stmt_id) {
    ParseResult r = parse(sql, len);
    if (r.ast) {
        stmt_cache_.store(stmt_id, r.stmt_type, r.ast);
    }
    return r;
}

template <Dialect D>
ParseResult Parser<D>::execute(uint32_t stmt_id, const ParamBindings& params) {
    ParseResult r;
    const CachedStmt* cached = stmt_cache_.lookup(stmt_id);
    if (!cached) {
        r.status = ParseResult::ERROR;
        r.stmt_type = StmtType::UNKNOWN;
        return r;
    }
    r.status = ParseResult::OK;
    r.stmt_type = cached->stmt_type;
    r.ast = cached->ast;
    r.bindings = params;
    return r;
}

template <Dialect D>
void Parser<D>::prepare_cache_evict(uint32_t stmt_id) {
    stmt_cache_.evict(stmt_id);
}

// ---- Explicit template instantiations ----

template class Parser<Dialect::MySQL>;
template class Parser<Dialect::PostgreSQL>;

} // namespace sql_parser
