#include <gtest/gtest.h>
#include "sql_parser/parser.h"
#include "sql_parser/emitter.h"

using namespace sql_parser;

TEST(MySQLTableDdl, NativeCreateAndAlterAreStructured) {
    for (const char* sql : {"CREATE TABLE t (id INT)", "ALTER TABLE t ADD COLUMN n VARCHAR(20)"}) {
        SCOPED_TRACE(sql);
        Parser<Dialect::MySQL> parser;
        auto result = parser.parse(sql, std::strlen(sql));
        ASSERT_TRUE(result.ok());
        ASSERT_TRUE(result.full_input);
        ASSERT_NE(result.ast, nullptr);
        EXPECT_NE(result.ast->type, NodeType::NODE_STATEMENT);
    }
}

TEST(MySQLTableDdl, CommonNativeFormsRoundTrip) {
    const char* queries[] = {
        "CREATE TEMPORARY TABLE IF NOT EXISTS db.accounts (id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL, amount DECIMAL(12,2) DEFAULT 0, created TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6), UNIQUE KEY uq (name(20) DESC), CONSTRAINT positive CHECK (amount >= 0) NOT ENFORCED) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='accounts'",
        "CREATE TABLE t (id INT, parent_id INT, CONSTRAINT fk FOREIGN KEY (parent_id) REFERENCES parent (id) ON DELETE CASCADE ON UPDATE SET NULL, KEY ix USING BTREE (id))",
        "CREATE TABLE t (a INT, b INT GENERATED ALWAYS AS (a + 1) STORED, c JSON DEFAULT (JSON_OBJECT()), CHECK (a > 0))",
        "CREATE TABLE t (kind ENUM('a','b') NOT NULL, options SET('x','y'), bits BIT(8), bytes VARBINARY(16), flag BOOLEAN, shape GEOMETRY SRID 4326)",
        "ALTER TABLE db.t ADD COLUMN n VARCHAR(20) DEFAULT 'hi' AFTER id, ADD UNIQUE KEY uq (n), DROP COLUMN old, MODIFY COLUMN id BIGINT UNSIGNED NOT NULL FIRST",
        "ALTER TABLE t CHANGE COLUMN old renamed INT, RENAME COLUMN x TO y, RENAME INDEX ix TO iy, DROP INDEX iz, DROP PRIMARY KEY",
        "ALTER TABLE t ADD (a INT, b VARCHAR(5)), ADD CONSTRAINT ck CHECK (a > 0), DROP CHECK ck, DROP FOREIGN KEY fk, RENAME TO db.u",
        "ALTER TABLE t ENGINE=InnoDB, ALGORITHM=INPLACE, LOCK=NONE",
        "CREATE TABLE t (a INT DEFAULT -1, b DOUBLE(10,2), c FLOAT(12), d NATIONAL CHAR VARYING(20) BINARY, e INT CHECK(e > 0)) AUTO_INCREMENT=10 ROW_FORMAT=DYNAMIC"
    };
    for (const char* sql : queries) {
        SCOPED_TRACE(sql);
        Parser<Dialect::MySQL> parser;
        auto result = parser.parse(sql, std::strlen(sql));
        ASSERT_TRUE(result.ok());
        ASSERT_TRUE(result.full_input);
        Emitter<Dialect::MySQL> emitter(parser.arena());
        emitter.emit(result.ast);
        auto out = emitter.result();
        std::string emitted(out.ptr, out.len);
        SCOPED_TRACE(emitted);
        auto again = parser.parse(emitted.data(), emitted.size());
        EXPECT_TRUE(again.ok());
        EXPECT_TRUE(again.full_input);
    }
}

TEST(MySQLTableDdl, RejectsMalformedAndUnsupportedForms) {
    for (const char* sql : {
        "CREATE TABLE t ()", "CREATE TABLE t (id)", "CREATE TABLE t (id int,)",
        "CREATE TABLE t (id madeup)", "CREATE TABLE t (id VARCHAR)", "CREATE TABLE t (id INT DEFAULT)",
        "CREATE TABLE t (id INT DEFAULT arbitrary)", "CREATE TABLE t (id INT DEFAULT 1+2)",
        "CREATE TABLE t (id INT ON UPDATE 1)", "CREATE TABLE t (CHECK ())", "CREATE TABLE t (UNIQUE ())",
        "CREATE TABLE t (FOREIGN KEY (x) REFERENCES p)", "CREATE TABLE t (x INT) ENGINE=", "CREATE TABLE t (x INT) arbitrary tail",
        "CREATE TABLE t (x INT ENFORCED)", "CREATE TABLE t (x INT NOT NULL AS (1))",
        "CREATE TABLE t (x INT COLLATE DEFAULT)", "CREATE TABLE t (x INT) ENGINE=DEFAULT",
        "CREATE TABLE t (x INT DEFAULT NOW)", "CREATE TABLE t (FULLTEXT KEY ix USING HASH (x))",
        "CREATE TABLE t (x INT, SPATIAL INDEX ix (x) USING BTREE)", "CREATE TABLE t (x INT GENERATED AS (1))",
        "ALTER TABLE t ADD", "ALTER TABLE t DROP", "ALTER TABLE t MODIFY x", "ALTER TABLE t CHANGE x INT",
        "ALTER TABLE t RENAME COLUMN x y", "ALTER TABLE t ADD x INT,", "ALTER TABLE t ALGORITHM=nonsense",
        "ALTER TABLE t ADD x INT AFTER", "ALTER TABLE t DROP COLUMN x CASCADE", "ALTER TABLE t ADD x INT arbitrary"
    }) {
        SCOPED_TRACE(sql);
        Parser<Dialect::MySQL> parser;
        auto result = parser.parse(sql, std::strlen(sql));
        EXPECT_FALSE(result.ok() && result.full_input);
    }
}

#include "sql_parser/mysql_type_parser.h"
TEST(MySQLFieldType, NativeSpansAndBoundaries) {
    for (const char* type : {"INT(11) UNSIGNED ZEROFILL", "INTEGER SIGNED", "INT1", "INT2", "INT3", "INT4", "INT8", "MIDDLEINT", "DOUBLE PRECISION(10,2)", "REAL(10,2)", "FLOAT(24)", "FLOAT4(10,2)", "FLOAT8(10,2)", "DEC(12,2)", "FIXED", "NUMERIC(12)", "TIME(6)", "TIMESTAMP(3)", "DATE", "YEAR(4)", "BOOLEAN", "CHAR", "CHARACTER VARYING(20)", "VARCHARACTER(10)", "NATIONAL VARCHAR(20)", "NCHAR VARCHAR(20)", "NVARCHAR(20)", "NATIONAL CHAR VARYING(10) BINARY", "BINARY(10)", "VARBINARY(20)", "BIT(8)", "TEXT(100) CHARSET utf8mb4 BINARY", "LONG VARCHAR CHARSET utf8mb4", "LONG VARBINARY", "ENUM('a','b') CHARACTER SET utf8mb4", "SET('x','y')", "JSON", "POINT", "GEOMETRY", "SERIAL", "VECTOR(128)", "CHAR(1.5)", "CHAR(999999999999999999999999999)"}) {
        SCOPED_TRACE(type);
        std::string sql = std::string(type) + " COLLATE utf8mb4_bin";
        Tokenizer<Dialect::MySQL> tok;
        tok.reset(sql.data(), sql.size());
        auto result = MySQLTypeParser(tok).parse();
        ASSERT_FALSE(result.empty());
        EXPECT_EQ(std::string(result.ptr, result.len), type);
        EXPECT_TRUE(tok.peek().text.equals_ci("COLLATE", 7));
        EXPECT_FALSE(tok.has_error());
    }
}

TEST(MySQLFieldType, RejectsInvalidTypeProductions) {
    for (const char* type : {"madeup", "`INT`", "'INT'", "VARCHAR", "VARBINARY", "NATIONAL", "NATIONAL TEXT", "INT()", "INT(-1)", "INT(1e2)", "INT(1,2)", "DOUBLE(12)", "REAL(12)", "DECIMAL(1.2,2)", "DECIMAL(1,2,3)", "TIME(1.5)", "ENUM()", "ENUM('a',)", "SET(1)", "CHAR CHARSET", "CHAR CHARSET=foo"}) {
        SCOPED_TRACE(type);
        Tokenizer<Dialect::MySQL> tok;
        tok.reset(type, std::strlen(type));
        EXPECT_TRUE(MySQLTypeParser(tok).parse().empty());
        EXPECT_TRUE(tok.has_error());
    }
}

TEST(MySQLTableDdl, ColumnTypesAndNamesAreEditableAstOperands) {
    const char* sql = "CREATE TABLE `db`.`t` (`id` BIGINT UNSIGNED, amount DECIMAL(10,2) DEFAULT 1, CHECK(amount > 0)) ENGINE=InnoDB";
    Parser<Dialect::MySQL> parser;
    auto result = parser.parse(sql, std::strlen(sql));
    ASSERT_TRUE(result.ok());
    ASSERT_TRUE(result.full_input);
    ASSERT_EQ(result.ast->type, NodeType::NODE_MYSQL_CREATE_TABLE);
    EXPECT_EQ(std::string(result.table_name.ptr, result.table_name.len), "t");
    EXPECT_EQ(std::string(result.schema_name.ptr, result.schema_name.len), "db");
    auto* definitions = result.ast->first_child;
    while (definitions && definitions->type != NodeType::NODE_MYSQL_DDL_LIST) definitions = definitions->next_sibling;
    ASSERT_NE(definitions, nullptr);
    auto* column = definitions->first_child;
    ASSERT_NE(column, nullptr);
    ASSERT_EQ(column->type, NodeType::NODE_MYSQL_COLUMN_DEF);
    auto* identifier = column->first_child;
    ASSERT_NE(identifier, nullptr);
    ASSERT_EQ(identifier->type, NodeType::NODE_IDENTIFIER);
    EXPECT_NE(identifier->flags & FLAG_IDENT_DELIMITED, 0);
    auto* type = identifier->next_sibling;
    ASSERT_NE(type, nullptr);
    ASSERT_EQ(type->type, NodeType::NODE_TYPE_NAME);
    EXPECT_EQ(std::string(type->source().ptr, type->source().len), "BIGINT UNSIGNED");
    identifier->set_value({"item_id", 7});
    identifier->set_source({}); // Discard the original delimited spelling when renaming.
    type->set_value({"INTEGER", 7});
    Emitter<Dialect::MySQL> emitter(parser.arena());
    emitter.emit(result.ast);
    auto out = emitter.result();
    std::string emitted(out.ptr, out.len);
    EXPECT_NE(emitted.find("`item_id` INTEGER"), std::string::npos);
    EXPECT_EQ(emitted.find("BIGINT"), std::string::npos);
    auto again = parser.parse(emitted.data(), emitted.size());
    EXPECT_TRUE(again.ok());
    EXPECT_TRUE(again.full_input);
}

TEST(MySQLTableDdl, RejectsNonNumPrecisionAndGeneratedAfterAttributes) {
    for (const char* sql : {
        "CREATE TABLE t (id INT, KEY ix (id(2147483648)))",
        "CREATE TABLE t (id TIMESTAMP DEFAULT CURRENT_TIMESTAMP(2147483648))",
        "CREATE TABLE t (id CHAR COLLATE utf8mb4_bin COLLATE utf8mb4_bin AS ('a'))",
        "CREATE TABLE t (id CHAR COLLATE _utf8mb4)", "CREATE TABLE t (id INT) CHARSET=_utf8mb4"
    }) {
        SCOPED_TRACE(sql);
        Parser<Dialect::MySQL> parser;
        auto result = parser.parse(sql, std::strlen(sql));
        EXPECT_FALSE(result.ok() && result.full_input);
    }
}

TEST(MySQLTableDdl, RejectsInvalidValueOperandsAndSpacedNow) {
    for (const char* sql : {
        "CREATE TABLE t (id INT DEFAULT (t.*))", "CREATE TABLE t (id INT CHECK(t.*))",
        "CREATE TABLE t (id INT AS (t.*))", "CREATE TABLE t (id INT DEFAULT (DEFAULT))",
        "CREATE TABLE t (id INT CHECK(DEFAULT))", "CREATE TABLE t (id INT DEFAULT (1 + *))",
        "CREATE TABLE t (ts TIMESTAMP DEFAULT NOW ())", "CREATE TABLE t (ts TIMESTAMP ON UPDATE NOW ())"
    }) {
        SCOPED_TRACE(sql);
        Parser<Dialect::MySQL> parser;
        auto result = parser.parse(sql, std::strlen(sql));
        EXPECT_FALSE(result.ok() && result.full_input);
    }
}

TEST(MySQLTableDdl, NowFunctionsKeepLexicallySignificantParenthesisAdjacency) {
    const char* sql = "CREATE TABLE t (ts TIMESTAMP DEFAULT NOW() ON UPDATE NOW(6), other TIMESTAMP DEFAULT CURRENT_TIMESTAMP)";
    Parser<Dialect::MySQL> parser;
    auto result = parser.parse(sql, std::strlen(sql));
    ASSERT_TRUE(result.ok());
    ASSERT_TRUE(result.full_input);
    Emitter<Dialect::MySQL> emitter(parser.arena());
    emitter.emit(result.ast);
    auto out = emitter.result();
    std::string emitted(out.ptr, out.len);
    EXPECT_NE(emitted.find("DEFAULT NOW()"), std::string::npos);
    EXPECT_NE(emitted.find("ON UPDATE NOW(6)"), std::string::npos);
    auto again = parser.parse(emitted.data(), emitted.size());
    EXPECT_TRUE(again.ok());
    EXPECT_TRUE(again.full_input);
}

TEST(MySQLTableDdl, LikeAndQuerySourcesRoundTrip) {
    for (const char* sql : {
        "CREATE TABLE copy LIKE original",
        "CREATE TEMPORARY TABLE IF NOT EXISTS db.copy (LIKE src.original)",
        "CREATE TABLE `copy` LIKE `src`.`original`",
        "CREATE TABLE t AS SELECT 1 AS n",
        "CREATE TABLE t SELECT 1 AS n",
        "CREATE TABLE t (n INT PRIMARY KEY) AS SELECT 1 AS n",
        "CREATE TABLE t (extra INT DEFAULT 7) ENGINE=InnoDB COMMENT='copy' AS SELECT 1 AS n",
        "CREATE TABLE t ENGINE=InnoDB, COMMENT='copy' IGNORE AS SELECT 1 AS n",
        "CREATE TABLE t REPLACE SELECT 1 AS n UNION ALL SELECT 2",
        "CREATE TABLE t AS WITH c AS (SELECT 1 AS n) SELECT n FROM c",
        "CREATE TABLE t WITH c AS (SELECT 1 AS n) SELECT n FROM c",
        "CREATE TABLE t (SELECT 1 AS n)", "CREATE TABLE t (SELECT 1 AS n);",
        "CREATE TABLE t ((SELECT 1 AS n) UNION ALL SELECT 2) ORDER BY n LIMIT 1",
        "CREATE TABLE t (n INT) (SELECT 1 AS n)",
        "CREATE TABLE t AS TABLE original",
        "CREATE TABLE t AS VALUES ROW(1, 2), ROW(3, 4)",
        "CREATE TABLE t AS SELECT /*+ MAX_EXECUTION_TIME(1000) */ n FROM original FOR SHARE",
        "CREATE TABLE t", "CREATE TABLE t ENGINE=InnoDB"}) {
        SCOPED_TRACE(sql);
        Parser<Dialect::MySQL> parser;
        auto result = parser.parse(sql, std::strlen(sql));
        ASSERT_TRUE(result.ok()); ASSERT_TRUE(result.full_input); ASSERT_NE(result.ast, nullptr);
        EXPECT_EQ(std::string(result.table_name.ptr, result.table_name.len),
                  std::strstr(sql, "LIKE") ? "copy" : "t");
        Emitter<Dialect::MySQL> emitter(parser.arena()); emitter.emit(result.ast);
        auto out = emitter.result(); std::string emitted(out.ptr, out.len); SCOPED_TRACE(emitted);
        Parser<Dialect::MySQL> again; auto r = again.parse(emitted.data(), emitted.size());
        ASSERT_TRUE(r.ok()); ASSERT_TRUE(r.full_input);
        Emitter<Dialect::MySQL> second(again.arena()); second.emit(r.ast);
        EXPECT_EQ(std::string(second.result().ptr, second.result().len), emitted);
    }
}

TEST(MySQLTableDdl, RejectsMalformedLikeAndQuerySources) {
    for (const char* sql : {
        "CREATE TABLE t LIKE", "CREATE TABLE t LIKE db.", "CREATE TABLE t LIKE db.src.extra",
        "CREATE TABLE t LIKE src extra", "CREATE TABLE t LIKE src AS SELECT 1",
        "CREATE TABLE t LIKE src ENGINE=InnoDB", "CREATE TABLE t ENGINE=InnoDB LIKE src",
        "CREATE TABLE t (LIKE src", "CREATE TABLE t (LIKE src, id INT)",
        "CREATE TABLE t (id INT) LIKE src", "CREATE TABLE t ((LIKE src))",
        "CREATE TABLE t AS", "CREATE TABLE t IGNORE", "CREATE TABLE t REPLACE AS",
        "CREATE TABLE t IGNORE REPLACE SELECT 1", "CREATE TABLE t AS IGNORE SELECT 1",
        "CREATE TABLE t AS SELECT FROM src", "CREATE TABLE t AS SELECT 1 +",
        "CREATE TABLE t AS SELECT 1 UNION", "CREATE TABLE t AS SELECT 1; SELECT 2",
        "CREATE TABLE t () AS SELECT 1", "CREATE TABLE t (id) AS SELECT 1",
        "CREATE TABLE t AS WITH c AS (SELECT 1) UPDATE src SET n=1",
        "CREATE TABLE t AS UPDATE src SET n=1",
        "CREATE TABLE t ENGINE=InnoDB, AS SELECT 1", "CREATE TABLE t ENGINE=InnoDB, SELECT 1",
        "CREATE TABLE t ENGINE=InnoDB,", "CREATE TABLE t AS SELECT 1 ENGINE=InnoDB"
    }) {
        SCOPED_TRACE(sql); Parser<Dialect::MySQL> parser;
        auto r=parser.parse(sql,std::strlen(sql));
        EXPECT_FALSE(r.ok() && r.full_input && r.ast);
    }
}


#include "sql_parser/ast_transform.h"
#include "sql_parser/parameterize.h"
#include "sql_engine/plan_builder.h"
TEST(MySQLTableDdl, SourcesRemainTraversableOwnedAndGuarded) {
    Arena owned;
    AstNode* copy = nullptr;
    {
        std::string sql = "CREATE TABLE dst.t REPLACE AS SELECT 1 AS n FROM src.s";
        Parser<Dialect::MySQL> p; auto r = p.parse(sql.data(),sql.size());
        ASSERT_TRUE(r.ok() && r.full_input);
        EXPECT_EQ(std::string(r.table_name.ptr,r.table_name.len), "t");
        EXPECT_EQ(std::string(r.schema_name.ptr,r.schema_name.len), "dst");
        size_t queries=0, selects=0;
        auto walk = walk_ast(r.ast, [&](const AstNode& n, const AstVisitContext&) {
            queries += n.type == NodeType::NODE_MYSQL_CREATE_QUERY;
            selects += n.type == NodeType::NODE_SELECT_STMT;
            return AstVisitAction::Continue;
        });
        EXPECT_EQ(walk.status, AstWalkStatus::Completed);
        EXPECT_EQ(queries,1u); EXPECT_EQ(selects,1u);
        EXPECT_FALSE(sql_engine::PlanBuilder<Dialect::MySQL>::supports_query_features(r.ast));
        Arena parameters; EXPECT_FALSE(parameterize_ast<Dialect::MySQL>(r,parameters).ok());
        auto cloned=clone_ast(r.ast,owned); ASSERT_TRUE(cloned.ok()); copy=cloned.ast;
        p.reset(); sql.assign(sql.size(),'!');
    }
    Emitter<Dialect::MySQL> emitter(owned); emitter.emit(copy);
    EXPECT_EQ(std::string(emitter.result().ptr,emitter.result().len),
              "CREATE TABLE dst.t REPLACE AS SELECT 1 AS n FROM src.s");
    const char* sql="CREATE TABLE dst.t (LIKE src.s)";
    Parser<Dialect::MySQL> p; auto r=p.parse(sql,std::strlen(sql));
    ASSERT_TRUE(r.ok() && r.full_input);
    EXPECT_EQ(std::string(r.table_name.ptr,r.table_name.len),"t");
    EXPECT_EQ(std::string(r.schema_name.ptr,r.schema_name.len),"dst");
    auto* source=r.ast->first_child;
    while(source && source->type!=NodeType::NODE_MYSQL_CREATE_LIKE) source=source->next_sibling;
    ASSERT_NE(source,nullptr); ASSERT_NE(source->first_child,nullptr);
    EXPECT_EQ(source->first_child->type,NodeType::NODE_QUALIFIED_NAME);
    Emitter<Dialect::MySQL> like(p.arena()); like.emit(r.ast);
    EXPECT_EQ(std::string(like.result().ptr,like.result().len),"CREATE TABLE dst.t LIKE src.s");
}


TEST(MySQLTableDdl, DuplicateReplaceDiscardsItsUnusedHintOnly) {
    for (const char* sql : {
        "CREATE TABLE t REPLACE /*+ MAX_EXECUTION_TIME(1) */ SELECT 1 AS n",
        "CREATE TABLE t REPLACE /*+ MAX_EXECUTION_TIME(1) */ AS SELECT 1 AS n"}) {
        Parser<Dialect::MySQL> p; auto r=p.parse(sql,std::strlen(sql));
        ASSERT_TRUE(r.ok() && r.full_input);
        Emitter<Dialect::MySQL> e(p.arena()); e.emit(r.ast);
        EXPECT_EQ(std::string(e.result().ptr,e.result().len),"CREATE TABLE t REPLACE AS SELECT 1 AS n");
    }
    const char* sql="CREATE TABLE t REPLACE /*+ A() */ SELECT /*+ MAX_EXECUTION_TIME(1) */ 1 AS n";
    Parser<Dialect::MySQL> p; auto r=p.parse(sql,std::strlen(sql)); ASSERT_TRUE(r.ok() && r.full_input);
    Emitter<Dialect::MySQL> e(p.arena()); e.emit(r.ast);
    EXPECT_EQ(std::string(e.result().ptr,e.result().len),
              "CREATE TABLE t REPLACE AS SELECT /*+ MAX_EXECUTION_TIME(1) */ 1 AS n");
}

TEST(MySQLTableDdl, NativeCreateSelectIntoRemainsExplicitlyUnsupported) {
    // Both native pins accept this production; do not label it malformed.
    const char* sql="CREATE TABLE t AS SELECT 1 INTO OUTFILE 'x'";
    Parser<Dialect::MySQL> p; auto r=p.parse(sql,std::strlen(sql));
    EXPECT_FALSE(r.ok() && r.full_input && r.ast);
}
