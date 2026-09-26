#ifndef SQL_PARSER_PG_SQL_JSON_PARSER_H
#define SQL_PARSER_PG_SQL_JSON_PARSER_H
#include "sql_parser/ast.h"
#include "sql_parser/tokenizer.h"
#include "sql_parser/pg_identifier.h"

namespace sql_parser {

// SQL/JSON and SQL/XML use keyword-delimited expressions rather than ordinary
// argument lists. Every syntax leaf below comes from a checked production;
// operands, defaults and paths remain normal traversable expression children.
template <Dialect D, class Expr>
class PgSqlJsonParser {
public:
    PgSqlJsonParser(Tokenizer<D>& tok, Arena& arena, Expr& expr)
        : tok_(tok), arena_(arena), expr_(expr) {}

    static bool recognizes(const Token& name) {
        for (const char* word : {"JSON", "JSON_SCALAR", "JSON_SERIALIZE", "JSON_ARRAY", "JSON_OBJECT",
             "JSON_QUERY", "JSON_VALUE", "JSON_EXISTS", "JSON_ARRAYAGG", "JSON_OBJECTAGG", "XMLPARSE", "XMLSERIALIZE", "XMLCONCAT",
             "XMLELEMENT", "XMLFOREST", "XMLPI", "XMLROOT", "XMLEXISTS"})
            if (Expr::keyword(name, word)) return true;
        return false;
    }

    AstNode* parse(const Token& name) {
        AstNode* n = make_node(arena_, NodeType::NODE_PG_JSON_XML, name.source, 1);
        if (!n || !punct(TokenType::TK_LPAREN)) return fail();
        bool ok = false;
        const bool array_agg = Expr::keyword(name,"JSON_ARRAYAGG");
        const bool object_agg = Expr::keyword(name,"JSON_OBJECTAGG");
        if (Expr::keyword(name,"JSON_TABLE")) ok = json_table(n);
        else if (Expr::keyword(name,"XMLTABLE")) ok = xml_table(n);
        else if (array_agg) ok = value(n) && aggregate_order(n) && nulls(n) && returning(n);
        else if (object_agg) {
            Token start = tok_.peek();
            AstNode* key = expr_.parse_complete();
            ok = key && key->type != NodeType::NODE_ASTERISK && add(n,key);
            if (is("VALUE")) ok = ok && common_operand(key,start) && take(n,"VALUE");
            else ok = ok && punct(TokenType::TK_COLON) && syntax(n,":");
            ok = ok && value(n) && nulls(n) && uniqueness(n) && returning(n);
        }
        else if (Expr::keyword(name,"JSON_ARRAY")) ok = constructor(n, false);
        else if (Expr::keyword(name,"JSON_OBJECT")) ok = constructor(n, true);
        else if (Expr::keyword(name,"JSON")) ok = value(n) && uniqueness(n);
        else if (Expr::keyword(name,"JSON_SCALAR")) ok = expression(n);
        else if (Expr::keyword(name,"JSON_SERIALIZE")) ok = value(n) && returning(n);
        else if (Expr::keyword(name,"JSON_QUERY") || Expr::keyword(name,"JSON_VALUE") || Expr::keyword(name,"JSON_EXISTS")) {
            bool exists = Expr::keyword(name,"JSON_EXISTS");
            ok = value(n) && comma(n) && expression(n) && passing(n) &&
                 (exists || returning(n)) &&
                 (!Expr::keyword(name,"JSON_QUERY") || (wrapper(n) && quotes(n))) && behavior(n, exists);
        } else ok = xml(n, name);
        if (!ok || failed_ || !punct(TokenType::TK_RPAREN)) return fail();
        if (array_agg || object_agg) {
            if (is("FILTER")) {
                AstNode* filtered = make_node(arena_, NodeType::NODE_PG_JSON_XML);
                if (!add(filtered,n) || !take(filtered,"FILTER") || !punct(TokenType::TK_LPAREN)) return fail();
                AstNode* predicate = clause(filtered,"",true);
                if (!take(predicate,"WHERE") || !expression(predicate) || !punct(TokenType::TK_RPAREN)) return fail();
                n = filtered;
            }
            if (failed_) return fail();
            return expr_.parse_json_xml_window(n);
        }
        return n;
    }
private:
    Tokenizer<D>& tok_;
    Arena& arena_;
    Expr& expr_;
    bool failed_ = false;
    bool add(AstNode* parent, AstNode* child) {
        if (!parent || !child) { failed_ = true; return false; }
        parent->add_child(child); return true;
    }
    AstNode* fail() { return expr_.syntax_error(); }
    bool is(const char* word) { return Expr::keyword(tok_.peek(), word); }
    bool end() { return tok_.peek().type == TokenType::TK_RPAREN; }
    bool syntax(AstNode* n, StringRef s) {
        return !failed_ && add(n, make_node(arena_, NodeType::NODE_PG_JSON_XML_SYNTAX, s));
    }
    bool syntax(AstNode* n, const char* s) { return syntax(n, StringRef{s, static_cast<uint32_t>(std::strlen(s))}); }
    bool take(AstNode* n, const char* word) {
        if (failed_ || !n || !is(word)) return false;
        return syntax(n, tok_.next_token().source);
    }
    bool punct(TokenType type) { if (failed_ || tok_.peek().type != type) return false; tok_.skip(); return true; }
    bool comma(AstNode* n) { if (!punct(TokenType::TK_COMMA)) return false; return syntax(n, ","); }
    bool name(AstNode* n, bool label = true) {
        if (failed_ || !n) return false;
        Token t = tok_.peek();
        if (!(label ? pg_column_label(t) : pg_column_name(t))) return false;
        tok_.skip(); return syntax(n,t.source);
    }
    bool expression(AstNode* n) {
        if (failed_ || !n) return false;
        AstNode* child = expr_.parse_complete();
        if (!child || child->type == NodeType::NODE_ASTERISK) return false;
        return add(n,child);
    }
    static bool common_operand(const AstNode* child, const Token& start) {
        if (!child) return false;
        switch (child->type) {
            case NodeType::NODE_ASTERISK:
            case NodeType::NODE_IS_NULL:
            case NodeType::NODE_IS_NOT_NULL:
            case NodeType::NODE_IN_LIST:
            case NodeType::NODE_BETWEEN:
            case NodeType::NODE_BINARY_OP:
            case NodeType::NODE_UNARY_OP:
            case NodeType::NODE_PG_TIME_ZONE: return false;
            case NodeType::NODE_TYPE_CAST:
                // CAST(...) and typed string literals are c_expr productions;
                // postfix :: casts require parentheses in these positions.
                if (Expr::keyword(start, "CAST")) return child->source_ptr == start.source.ptr;
                return (child->first_child && child->first_child->type == NodeType::NODE_LITERAL_STRING &&
                     start.type != TokenType::TK_STRING);
            default: return true;
        }
    }
    bool common_expression(AstNode* n) {
        if (failed_ || !n) return false;
        Token start = tok_.peek();
        AstNode* child = expr_.parse_complete();
        if (!common_operand(child, start)) return false;
        return add(n,child);
    }
    static bool restricted_operand(const AstNode* child) {
        if (!child || child->type == NodeType::NODE_ASTERISK ||
            child->type == NodeType::NODE_PG_QUANTIFIED_OPERAND) return false;
        if (child->type == NodeType::NODE_UNARY_OP && child->value().equals_ci("NOT", 3)) return false;
        if (child->type == NodeType::NODE_BINARY_OP || child->type == NodeType::NODE_UNARY_OP ||
            child->type == NodeType::NODE_TYPE_CAST) {
            for (const AstNode* c = child->first_child; c; c = c->next_sibling)
                if (!restricted_operand(c)) return false;
        }
        return true;
    }
    bool restricted_expression(AstNode* n) {
        if (failed_ || !n) return false;
        AstNode* child = expr_.parse_json_xml_restricted();
        if (!restricted_operand(child)) return false;
        return add(n,child);
    }
    bool string(AstNode* n) {
        if (failed_ || !n || tok_.peek().type != TokenType::TK_STRING) return false;
        return add(n,make_node_from_token(arena_, NodeType::NODE_LITERAL_STRING, tok_.next_token()));
    }
    bool type(AstNode* n, bool arrays = true) {
        StringRef t = expr_.parse_type_name(arrays);
        if (t.empty()) return false;
        return syntax(n,t);
    }
    AstNode* clause(AstNode* n, const char* text, bool parens = false) {
        AstNode* child = make_node(arena_, NodeType::NODE_PG_JSON_XML,
            StringRef{text,static_cast<uint32_t>(std::strlen(text))},parens ? 1 : 0);
        return add(n,child) ? child : nullptr;
    }
    bool format(AstNode* n) {
        if (!take(n,"FORMAT")) return true;
        if (!take(n,"JSON")) return false;
        if (take(n,"ENCODING")) {
            if (!take(n,"UTF8") && !take(n,"UTF16") && !take(n,"UTF32")) return false;
        }
        return true;
    }
    bool value(AstNode* n) { return expression(n) && format(n); }
    bool returning(AstNode* n) { return !take(n,"RETURNING") || (type(n) && format(n)); }
    bool uniqueness(AstNode* n) {
        if (!take(n,"WITH") && !take(n,"WITHOUT")) return true;
        if (!take(n,"UNIQUE")) return false;
        take(n,"KEYS"); return true;
    }
    bool nulls(AstNode* n) {
        if (!take(n,"NULL") && !take(n,"ABSENT")) return true;
        return take(n,"ON") && take(n,"NULL");
    }
    bool passing(AstNode* n) {
        if (!take(n,"PASSING")) return true;
        do { if (!value(n) || !take(n,"AS") || !name(n)) return false; } while (comma(n));
        return true;
    }
    bool wrapper(AstNode* n) {
        if (take(n,"WITHOUT")) { take(n,"ARRAY"); return take(n,"WRAPPER"); }
        if (!take(n,"WITH")) return true;
        if (!take(n,"CONDITIONAL")) take(n,"UNCONDITIONAL");
        take(n,"ARRAY"); return take(n,"WRAPPER");
    }
    bool quotes(AstNode* n) {
        if (!take(n,"KEEP") && !take(n,"OMIT")) return true;
        if (!take(n,"QUOTES")) return false;
        return !take(n,"ON") || (take(n,"SCALAR") && take(n,"STRING"));
    }
    bool starts_behavior() {
        return is("DEFAULT") || is("ERROR") || is("NULL") || is("TRUE") || is("FALSE") || is("UNKNOWN") || is("EMPTY");
    }
    bool behavior(AstNode* n, bool only_error = false) {
        if (!starts_behavior()) return true;
        for (int i = 0; i != 2; ++i) {
            AstNode* b = clause(n, "");
            if (take(b,"DEFAULT")) { if (!expression(b)) return false; }
            else if (take(b,"EMPTY")) { if (!take(b,"ARRAY")) take(b,"OBJECT"); }
            else { syntax(b,tok_.next_token().source); }
            if (!take(b,"ON")) return false;
            if (take(b,"ERROR")) return true;
            if (only_error || i || !take(b,"EMPTY")) return false;
            if (!starts_behavior()) return true;
            only_error = true;
        }
        return false;
    }
    bool constructor(AstNode* n, bool object) {
        if (end() || is("RETURNING")) return returning(n);
        if (!object && Expr::starts_query(tok_.peek().type)) {
            AstNode* query = expr_.parse_json_array_query();
            if (!query) return false;
            if (!add(n,query)) return false;
            return format(n) && returning(n);
        }
        // An object may use the legacy comma-separated json_object arguments.
        Token first_start = tok_.peek();
        AstNode* first = expr_.parse_complete();
        if (!first || first->type == NodeType::NODE_ASTERISK) return false;
        if (!add(n,first)) return false;
        bool pairs = object && (is("VALUE") || tok_.peek().type == TokenType::TK_COLON);
        if (pairs) {
            if (is("VALUE") && !common_operand(first, first_start)) return false;
            if (!take(n,"VALUE")) { tok_.skip(); syntax(n,":"); }
            if (!value(n)) return false;
        } else if (!object && !format(n)) return false;
        while (comma(n)) {
            Token key_start = tok_.peek();
            AstNode* key = expr_.parse_complete();
            if (!key || key->type == NodeType::NODE_ASTERISK) return false;
            if (!add(n,key)) return false;
            if (pairs) {
                if (is("VALUE") && !common_operand(key, key_start)) return false;
                if (!take(n,"VALUE")) { if (!punct(TokenType::TK_COLON)) return false; syntax(n,":"); }
                if (!value(n)) return false;
            } else if (!object && !format(n)) return false;
        }
        if (object && !pairs) return true;
        return nulls(n) && (!object || uniqueness(n)) && returning(n);
    }
    bool aggregate_order(AstNode* n) {
        if (!take(n,"ORDER")) return true;
        if (!take(n,"BY")) return false;
        do {
            if (!expression(n)) return false;
            if (!take(n,"ASC")) take(n,"DESC");
            if (take(n,"NULLS") && !take(n,"FIRST") && !take(n,"LAST")) return false;
        } while (comma(n));
        return true;
    }
    bool path(AstNode* n) { return !take(n,"PATH") || string(n); }
    bool path_name(AstNode* n) { return !take(n,"AS") || name(n,false); }
    bool json_columns(AstNode* n) {
        if (!take(n,"COLUMNS") || !punct(TokenType::TK_LPAREN)) return false;
        AstNode* columns = clause(n,"",true);
        do {
            AstNode* col = clause(columns, "");
            if (take(col,"NESTED")) {
                take(col,"PATH");
                if (!string(col) || !path_name(col) || !json_columns(col)) return false;
            } else {
                if (!name(col,false)) return false;
                if (take(col,"FOR")) { if (!take(col,"ORDINALITY")) return false; }
                else {
                    if (!type(col)) return false;
                    if (take(col,"EXISTS")) { if (!path(col) || !behavior(col,true)) return false; }
                    else if (!format(col) || !path(col) || !wrapper(col) || !quotes(col) || !behavior(col)) return false;
                }
            }
        } while (comma(columns));
        return punct(TokenType::TK_RPAREN);
    }
    bool json_table(AstNode* n) {
        return value(n) && comma(n) && string(n) && path_name(n) && passing(n) && json_columns(n) && behavior(n,true);
    }
    bool xml_mechanism(AstNode* n) { return !take(n,"BY") || take(n,"REF") || take(n,"VALUE"); }
    bool xml_passing(AstNode* n) { return take(n,"PASSING") && xml_mechanism(n) && common_expression(n) && xml_mechanism(n); }
    bool xml_attributes(AstNode* n) {
        do { if (!expression(n) || (take(n,"AS") && !name(n))) return false; } while (comma(n));
        return true;
    }
    bool xml_table(AstNode* n) {
        if (take(n,"XMLNAMESPACES")) {
            if (!punct(TokenType::TK_LPAREN)) return false;
            AstNode* ns = clause(n,"",true);
            do {
                if (take(ns,"DEFAULT")) { if (!restricted_expression(ns)) return false; }
                else if (!restricted_expression(ns) || !take(ns,"AS") || !name(ns)) return false;
            } while (comma(ns));
            if (!punct(TokenType::TK_RPAREN) || !comma(n)) return false;
        }
        if (!common_expression(n) || !xml_passing(n) || !take(n,"COLUMNS")) return false;
        do {
            AstNode* col = clause(n, "");
            if (!name(col,false)) return false;
            if (take(col,"FOR")) { if (!take(col,"ORDINALITY")) return false; continue; }
            if (!type(col)) return false;
            unsigned seen = 0;
            while (is("PATH") || is("DEFAULT") || is("NULL") || is("NOT")) {
                unsigned bit = is("PATH") ? 1 : is("DEFAULT") ? 2 : 4;
                if (seen & bit) return false;
                seen |= bit;
                if (bit == 4) { take(col,"NOT"); if (!take(col,"NULL")) return false; }
                else {
                    syntax(col,tok_.next_token().source);
                    if (!restricted_expression(col)) return false;
                }
            }
        } while (comma(n));
        return true;
    }
    bool xml(AstNode* n, const Token& function) {
        if (Expr::keyword(function,"XMLPARSE") || Expr::keyword(function,"XMLSERIALIZE")) {
            if ((!take(n,"DOCUMENT") && !take(n,"CONTENT")) || !expression(n)) return false;
            if (Expr::keyword(function,"XMLPARSE")) {
                if (take(n,"PRESERVE") || take(n,"STRIP")) return take(n,"WHITESPACE");
                return true;
            }
            if (!take(n,"AS") || !type(n,false)) return false;
            if (take(n,"NO")) return take(n,"INDENT");
            take(n,"INDENT"); return true;
        }
        if (Expr::keyword(function,"XMLEXISTS")) return common_expression(n) && xml_passing(n);
        if (Expr::keyword(function,"XMLFOREST")) return xml_attributes(n);
        if (Expr::keyword(function,"XMLROOT")) {
            if (!expression(n) || !comma(n) || !take(n,"VERSION")) return false;
            if (take(n,"NO")) { if (!take(n,"VALUE")) return false; }
            else if (!expression(n)) return false;
            if (!comma(n)) return true;
            if (!take(n,"STANDALONE")) return false;
            if (take(n,"YES")) return true;
            if (!take(n,"NO")) return false;
            take(n,"VALUE"); return true;
        }
        if (Expr::keyword(function,"XMLELEMENT") || Expr::keyword(function,"XMLPI")) {
            if (!take(n,"NAME") || !name(n)) return false;
            if (!comma(n)) return true;
            if (Expr::keyword(function,"XMLPI")) return expression(n);
            if (take(n,"XMLATTRIBUTES")) {
                if (!punct(TokenType::TK_LPAREN)) return false;
                AstNode* attrs = clause(n,"",true);
                if (!xml_attributes(attrs) || !punct(TokenType::TK_RPAREN)) return false;
                if (!comma(n)) return true;
            }
        }
        do { if (!expression(n)) return false; } while (comma(n));
        return true;
    }
};
} // namespace sql_parser
#endif
