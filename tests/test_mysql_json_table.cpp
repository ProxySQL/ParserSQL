#include <gtest/gtest.h>
#include "sql_parser/parser.h"
#include "sql_parser/emitter.h"
#include "sql_parser/parameterize.h"
#include "sql_engine/plan_builder.h"
#include <cstring>
#include <string>

using namespace sql_parser;

namespace {
void accept_json_table(const char* sql) {
    SCOPED_TRACE(sql);
    Parser<Dialect::MySQL> parser;
    auto result = parser.parse(sql, std::strlen(sql));
    ASSERT_TRUE(result.ok());
    ASSERT_TRUE(result.full_input);
    Emitter<Dialect::MySQL> emitter(parser.arena());
    emitter.emit(result.ast);
    auto output = emitter.result();
    EXPECT_EQ(std::string(output.ptr, output.len), sql);
    Parser<Dialect::MySQL> again;
    auto reparsed = again.parse(output.ptr, output.len);
    EXPECT_TRUE(reparsed.ok());
    EXPECT_TRUE(reparsed.full_input);
    EXPECT_FALSE(sql_engine::PlanBuilder<Dialect::MySQL>::supports_query_features(result.ast));
    EXPECT_FALSE(sql_engine::PlanBuilder<Dialect::MySQL>::supports_dml_features(result.ast));
    Arena destination;
    EXPECT_FALSE(parameterize_ast<Dialect::MySQL>(result, destination).ok());
}
void reject_json_table(const std::string& sql) {
    SCOPED_TRACE(sql);
    Parser<Dialect::MySQL> parser;
    auto result = parser.parse(sql.data(), sql.size());
    EXPECT_FALSE(result.ok() && result.full_input);
}
}

TEST(MySQLJsonTable, OrdinalityTypedColumnsAndRequiredAlias) {
    accept_json_table("SELECT * FROM JSON_TABLE('[1,2]', '$[*]' COLUMNS (n FOR ORDINALITY, value INT PATH '$')) AS jt");
    accept_json_table("SELECT jt.n FROM docs AS d JOIN JSON_TABLE(d.payload, '$[*]' COLUMNS (`n` BIGINT UNSIGNED EXISTS PATH '$.id')) AS `select` ON jt.n = d.id");
    accept_json_table("SELECT * FROM JSON_TABLE(JSON_ARRAY(1, 2), '$[*]' COLUMNS (n DECIMAL(10,2) PATH '$', label VARCHAR(30) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin PATH '$.name')) AS jt");
}

TEST(MySQLJsonTable, NestedColumnsAndBothNativeResponseOrders) {
    accept_json_table("SELECT * FROM JSON_TABLE('{}', '$' COLUMNS (n INT PATH '$.n' NULL ON EMPTY ERROR ON ERROR, NESTED PATH '$.items[*]' COLUMNS (ord FOR ORDINALITY, value INT PATH '$' DEFAULT '0' ON ERROR DEFAULT '1' ON EMPTY))) AS jt");
    accept_json_table("SELECT * FROM JSON_TABLE('{}', '$' COLUMNS (n INT EXISTS PATH '$.n' ERROR ON EMPTY, NESTED PATH '$.a' COLUMNS (NESTED PATH '$.b' COLUMNS (x INT PATH '$')))) AS jt");
}

TEST(MySQLJsonTable, NativeTextAndDefaultLiteralForms) {
    accept_json_table("SELECT * FROM JSON_TABLE('{}', _utf8mb4 '$' COLUMNS (n INT PATH N'$.n' DEFAULT -1 ON EMPTY, s TEXT PATH '$.' 's' DEFAULT _utf8mb4 '\"x\"' ON ERROR)) AS jt");
    accept_json_table("SELECT * FROM JSON_TABLE('{}', '$' COLUMNS (n INT PATH '$' DEFAULT 2.5 ON EMPTY, b INT PATH '$' DEFAULT TRUE ON ERROR, h INT PATH '$' DEFAULT X'31' ON ERROR, v INT PATH '$' DEFAULT b'01' ON ERROR, d DATE PATH '$' DEFAULT DATE '2026-09-30' ON ERROR)) AS jt");
}

TEST(MySQLJsonTable, RejectMalformedStructureAndIncompleteOperands) {
    for (const char* body : {
        "'{}', '$' COLUMNS ()", "'{}', '$' COLUMNS (n FOR)",
        "'{}', '$' COLUMNS (n FOR ORDINALITY,)", "'{}', '$' COLUMNS (n bogus PATH '$')",
        "'{}', '$' COLUMNS (n INT PATH)", "'{}', '$' COLUMNS (n INT '$')",
        "'{}', '$' COLUMNS (n INT EXISTS '$')", "'{}', '$' COLUMNS (n INT PATH 1)",
        "'{}', '$' COLUMNS (n INT PATH '$' NULL EMPTY)",
        "'{}', '$' COLUMNS (n INT PATH '$' NULL ON EMPTY NULL ON EMPTY)",
        "'{}', '$' COLUMNS (n INT PATH '$' NULL ON ERROR ERROR ON ERROR)",
        "'{}', '$' COLUMNS (n INT PATH '$' DEFAULT ON EMPTY)",
        "'{}', '$' COLUMNS (n INT PATH '$' DEFAULT NULL ON EMPTY)",
        "'{}', '$' COLUMNS (n INT PATH '$' DEFAULT (1) ON EMPTY)",
        "'{}', '$' COLUMNS (n INT PATH '$' DEFAULT 1 + 2 ON EMPTY)",
        "'{}', '$' COLUMNS (n INT PATH '$' DEFAULT ? ON EMPTY)",
        "'{}', '$' COLUMNS (n FOR ORDINALITY PATH '$')",
        "'{}', '$' COLUMNS (NESTED '$.a' COLUMNS (n INT PATH '$'))",
        "'{}', '$' COLUMNS (NESTED PATH '$.a' COLUMNS ())",
        "'{}', '$' COLUMNS (n INT COLLATE PATH '$')",
        "'{}', '$' COLUMNS (n INT PATH '$' UNKNOWN)",
        "'{}', ? COLUMNS (n INT PATH '$')", "'{}', X'24' COLUMNS (n INT PATH '$')",
        "'{}', N '$' COLUMNS (n INT PATH '$')", "'{}' +, '$' COLUMNS (n INT PATH '$')",
        "*, '$' COLUMNS (n INT PATH '$')", "DEFAULT, '$' COLUMNS (n INT PATH '$')",
        "'{}', '$' COLUMNS (PRIMARY INT PATH '$')"})
        reject_json_table(std::string("SELECT * FROM JSON_TABLE(") + body + ") AS jt");
    for (const char* suffix : {"", " AS", " AS 1", " AS 'jt'", " AS jt(n)", " AS jt USE INDEX (idx)"})
        reject_json_table(std::string("SELECT * FROM JSON_TABLE('{}', '$' COLUMNS (n INT PATH '$'))") + suffix);
    reject_json_table("SELECT JSON_TABLE('{}', '$' COLUMNS (n INT PATH '$'))");
    reject_json_table("SELECT * FROM LATERAL JSON_TABLE('{}', '$' COLUMNS (n INT PATH '$')) AS jt");
}

TEST(MySQLJsonTable, RejectExcessiveNestedColumns) {
    std::string sql = "SELECT * FROM JSON_TABLE('{}', '$' COLUMNS (";
    for (unsigned i = 0; i < 300; ++i) sql += "NESTED PATH '$' COLUMNS (";
    sql += "n INT PATH '$'";
    sql.append(300, ')');
    sql += ")) AS jt";
    reject_json_table(sql);
}

TEST(MySQLJsonTable, ExposesDocumentPathColumnAndResponseNodes) {
    const char* sql = "SELECT * FROM JSON_TABLE(doc, '$' COLUMNS (`n` FOR ORDINALITY, x INT EXISTS PATH '$.x' DEFAULT '0' ON EMPTY, NESTED PATH '$.items[*]' COLUMNS (y INT PATH '$'))) AS jt";
    Parser<Dialect::MySQL> parser;
    auto result = parser.parse(sql, std::strlen(sql));
    ASSERT_TRUE(result.ok());
    ASSERT_TRUE(result.full_input);
    const AstNode* from = result.ast->first_child;
    while (from && from->type != NodeType::NODE_FROM_CLAUSE) from = from->next_sibling;
    ASSERT_NE(from, nullptr);
    const AstNode* table = from->first_child;
    ASSERT_NE(table, nullptr);
    EXPECT_EQ(table->type, NodeType::NODE_TABLE_REF);
    const AstNode* function = table->first_child;
    ASSERT_NE(function, nullptr);
    EXPECT_EQ(function->type, NodeType::NODE_MYSQL_JSON_TABLE);
    ASSERT_NE(function->next_sibling, nullptr);
    EXPECT_EQ(function->next_sibling->type, NodeType::NODE_ALIAS);
    const AstNode* document = function->first_child;
    ASSERT_NE(document, nullptr);
    EXPECT_EQ(std::string(document->value_ptr, document->value_len), "doc");
    const AstNode* path = document->next_sibling;
    ASSERT_NE(path, nullptr);
    EXPECT_EQ(path->type, NodeType::NODE_MYSQL_JSON_TABLE_LITERAL);
    EXPECT_EQ(std::string(path->value_ptr, path->value_len), "'$'");
    const AstNode* columns = path->next_sibling;
    ASSERT_NE(columns, nullptr);
    EXPECT_EQ(columns->type, NodeType::NODE_MYSQL_JSON_TABLE_COLUMNS);
    const AstNode* ordinal = columns->first_child;
    ASSERT_NE(ordinal, nullptr);
    EXPECT_EQ(ordinal->type, NodeType::NODE_MYSQL_JSON_TABLE_COLUMN);
    EXPECT_EQ(ordinal->flags, 1);
    ASSERT_NE(ordinal->first_child, nullptr);
    EXPECT_NE(ordinal->first_child->flags & FLAG_IDENT_DELIMITED, 0);
    const AstNode* value = ordinal->next_sibling;
    ASSERT_NE(value, nullptr);
    EXPECT_EQ(value->flags, 2);
    ASSERT_NE(value->first_child, nullptr);
    const AstNode* type = value->first_child->next_sibling;
    ASSERT_NE(type, nullptr);
    EXPECT_EQ(type->type, NodeType::NODE_TYPE_NAME);
    EXPECT_EQ(std::string(type->value_ptr, type->value_len), "INT");
    ASSERT_NE(type->next_sibling, nullptr);
    const AstNode* response = type->next_sibling->next_sibling;
    ASSERT_NE(response, nullptr);
    EXPECT_EQ(response->type, NodeType::NODE_MYSQL_JSON_TABLE_RESPONSE);
    EXPECT_EQ(std::string(response->value_ptr, response->value_len), "DEFAULT ON EMPTY");
    ASSERT_NE(response->first_child, nullptr);
    EXPECT_EQ(std::string(response->first_child->value_ptr, response->first_child->value_len), "'0'");
    const AstNode* nested = value->next_sibling;
    ASSERT_NE(nested, nullptr);
    EXPECT_EQ(nested->type, NodeType::NODE_MYSQL_JSON_TABLE_NESTED);
    ASSERT_NE(nested->first_child, nullptr);
    ASSERT_NE(nested->first_child->next_sibling, nullptr);
    EXPECT_EQ(nested->first_child->next_sibling->type, NodeType::NODE_MYSQL_JSON_TABLE_COLUMNS);
}
