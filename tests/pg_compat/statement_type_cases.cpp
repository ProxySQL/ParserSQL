#include "tools/pg_compat/result.h"
#include "tools/pg_compat/statement_type_map.h"

#include <cassert>
#include <cstring>

namespace {

using pg_compat::CompatibilityResult;
using pg_compat::MappingKind;
using pg_compat::StatementTypeMapping;
using sql_parser::StmtType;

struct MappingCase {
    PgQuery__Node__NodeCase node_case;
    MappingKind kind;
    StmtType type;
};

struct NameCase {
    PgQuery__Node__NodeCase node_case;
    const char* name;
};

struct StmtTypeNameCase {
    StmtType type;
    const char* name;
};

void assert_mapping(
    const StatementTypeMapping& mapping,
    MappingKind expected_kind,
    StmtType expected_type) {
    assert(mapping.kind == expected_kind);
    assert(mapping.type == expected_type);
}

void test_result_names() {
    struct ResultNameCase {
        CompatibilityResult result;
        const char* name;
    };

    const ResultNameCase cases[] = {
        {CompatibilityResult::DeepSupported, "DEEP_SUPPORTED"},
        {CompatibilityResult::ClassifiedOnly, "CLASSIFIED_ONLY"},
        {CompatibilityResult::Partial, "PARTIAL"},
        {CompatibilityResult::Error, "ERROR"},
        {CompatibilityResult::TrailingInput, "TRAILING_INPUT"},
        {CompatibilityResult::TypeMismatch, "TYPE_MISMATCH"},
        {CompatibilityResult::OracleRejected, "ORACLE_REJECTED"},
    };

    for (const auto& test_case : cases) {
        assert(std::strcmp(pg_compat::result_name(test_case.result), test_case.name) == 0);
    }

    assert(std::strcmp(
        pg_compat::result_name(static_cast<CompatibilityResult>(999)),
        "ERROR") == 0);
}

void test_default_and_simple_mappings() {
    const StatementTypeMapping default_mapping;
    assert_mapping(default_mapping, MappingKind::Unmapped, StmtType::UNKNOWN);

    const MappingCase cases[] = {
        {PG_QUERY__NODE__NODE_SELECT_STMT, MappingKind::Equivalent, StmtType::SELECT},
        {PG_QUERY__NODE__NODE_INSERT_STMT, MappingKind::Equivalent, StmtType::INSERT},
        {PG_QUERY__NODE__NODE_UPDATE_STMT, MappingKind::Equivalent, StmtType::UPDATE},
        {PG_QUERY__NODE__NODE_DELETE_STMT, MappingKind::Equivalent, StmtType::DELETE_STMT},
        {PG_QUERY__NODE__NODE_VARIABLE_SET_STMT, MappingKind::Equivalent, StmtType::SET},
        {PG_QUERY__NODE__NODE_VARIABLE_SHOW_STMT, MappingKind::Equivalent, StmtType::SHOW},
        {PG_QUERY__NODE__NODE_PREPARE_STMT, MappingKind::Equivalent, StmtType::PREPARE},
        {PG_QUERY__NODE__NODE_EXECUTE_STMT, MappingKind::Equivalent, StmtType::EXECUTE},
        {PG_QUERY__NODE__NODE_DEALLOCATE_STMT, MappingKind::Equivalent, StmtType::DEALLOCATE},
        {PG_QUERY__NODE__NODE_EXPLAIN_STMT, MappingKind::Equivalent, StmtType::EXPLAIN},
        {PG_QUERY__NODE__NODE_CALL_STMT, MappingKind::Equivalent, StmtType::CALL},
        {PG_QUERY__NODE__NODE_DO_STMT, MappingKind::Equivalent, StmtType::DO_STMT},
        {PG_QUERY__NODE__NODE_TRUNCATE_STMT, MappingKind::Equivalent, StmtType::TRUNCATE},
        {PG_QUERY__NODE__NODE_LOCK_STMT, MappingKind::Equivalent, StmtType::LOCK},
        {PG_QUERY__NODE__NODE_CREATE_STMT, MappingKind::Equivalent, StmtType::CREATE},
        {PG_QUERY__NODE__NODE_CREATE_SCHEMA_STMT, MappingKind::Equivalent, StmtType::CREATE},
        {PG_QUERY__NODE__NODE_CREATE_TABLE_AS_STMT, MappingKind::Equivalent, StmtType::CREATE},
        {PG_QUERY__NODE__NODE_INDEX_STMT, MappingKind::Equivalent, StmtType::CREATE},
        {PG_QUERY__NODE__NODE_CREATE_FUNCTION_STMT, MappingKind::Equivalent, StmtType::CREATE},
        {PG_QUERY__NODE__NODE_CREATE_ROLE_STMT, MappingKind::Equivalent, StmtType::CREATE},
        {PG_QUERY__NODE__NODE_CREATE_SEQ_STMT, MappingKind::Equivalent, StmtType::CREATE},
        {PG_QUERY__NODE__NODE_CREATE_DOMAIN_STMT, MappingKind::Equivalent, StmtType::CREATE},
        {PG_QUERY__NODE__NODE_CREATE_ENUM_STMT, MappingKind::Equivalent, StmtType::CREATE},
        {PG_QUERY__NODE__NODE_CREATE_RANGE_STMT, MappingKind::Equivalent, StmtType::CREATE},
        {PG_QUERY__NODE__NODE_CREATE_AM_STMT, MappingKind::Equivalent, StmtType::CREATE},
        {PG_QUERY__NODE__NODE_CREATE_CAST_STMT, MappingKind::Equivalent, StmtType::CREATE},
        {PG_QUERY__NODE__NODE_CREATE_CONVERSION_STMT, MappingKind::Equivalent, StmtType::CREATE},
        {PG_QUERY__NODE__NODE_CREATE_EVENT_TRIG_STMT, MappingKind::Equivalent, StmtType::CREATE},
        {PG_QUERY__NODE__NODE_CREATE_EXTENSION_STMT, MappingKind::Equivalent, StmtType::CREATE},
        {PG_QUERY__NODE__NODE_CREATE_FDW_STMT, MappingKind::Equivalent, StmtType::CREATE},
        {PG_QUERY__NODE__NODE_CREATE_FOREIGN_SERVER_STMT, MappingKind::Equivalent, StmtType::CREATE},
        {PG_QUERY__NODE__NODE_CREATE_FOREIGN_TABLE_STMT, MappingKind::Equivalent, StmtType::CREATE},
        {PG_QUERY__NODE__NODE_CREATE_OP_CLASS_STMT, MappingKind::Equivalent, StmtType::CREATE},
        {PG_QUERY__NODE__NODE_CREATE_OP_FAMILY_STMT, MappingKind::Equivalent, StmtType::CREATE},
        {PG_QUERY__NODE__NODE_CREATE_PLANG_STMT, MappingKind::Equivalent, StmtType::CREATE},
        {PG_QUERY__NODE__NODE_CREATE_POLICY_STMT, MappingKind::Equivalent, StmtType::CREATE},
        {PG_QUERY__NODE__NODE_CREATE_PUBLICATION_STMT, MappingKind::Equivalent, StmtType::CREATE},
        {PG_QUERY__NODE__NODE_CREATE_STATS_STMT, MappingKind::Equivalent, StmtType::CREATE},
        {PG_QUERY__NODE__NODE_CREATE_SUBSCRIPTION_STMT, MappingKind::Equivalent, StmtType::CREATE},
        {PG_QUERY__NODE__NODE_CREATE_TABLE_SPACE_STMT, MappingKind::Equivalent, StmtType::CREATE},
        {PG_QUERY__NODE__NODE_CREATE_TRANSFORM_STMT, MappingKind::Equivalent, StmtType::CREATE},
        {PG_QUERY__NODE__NODE_CREATE_TRIG_STMT, MappingKind::Equivalent, StmtType::CREATE},
        {PG_QUERY__NODE__NODE_CREATE_USER_MAPPING_STMT, MappingKind::Equivalent, StmtType::CREATE},
        {PG_QUERY__NODE__NODE_COMPOSITE_TYPE_STMT, MappingKind::Equivalent, StmtType::CREATE},
        {PG_QUERY__NODE__NODE_RULE_STMT, MappingKind::Equivalent, StmtType::CREATE},
        {PG_QUERY__NODE__NODE_VIEW_STMT, MappingKind::Equivalent, StmtType::CREATE},
        {PG_QUERY__NODE__NODE_CREATEDB_STMT, MappingKind::Equivalent, StmtType::CREATE},
        {PG_QUERY__NODE__NODE_ALTER_TABLE_STMT, MappingKind::Equivalent, StmtType::ALTER},
        {PG_QUERY__NODE__NODE_ALTER_FUNCTION_STMT, MappingKind::Equivalent, StmtType::ALTER},
        {PG_QUERY__NODE__NODE_ALTER_ROLE_STMT, MappingKind::Equivalent, StmtType::ALTER},
        {PG_QUERY__NODE__NODE_ALTER_SEQ_STMT, MappingKind::Equivalent, StmtType::ALTER},
        {PG_QUERY__NODE__NODE_ALTER_DOMAIN_STMT, MappingKind::Equivalent, StmtType::ALTER},
        {PG_QUERY__NODE__NODE_ALTER_ENUM_STMT, MappingKind::Equivalent, StmtType::ALTER},
        {PG_QUERY__NODE__NODE_ALTER_DATABASE_STMT, MappingKind::Equivalent, StmtType::ALTER},
        {PG_QUERY__NODE__NODE_ALTER_DATABASE_SET_STMT, MappingKind::Equivalent, StmtType::ALTER},
        {PG_QUERY__NODE__NODE_ALTER_COLLATION_STMT, MappingKind::Equivalent, StmtType::ALTER},
        {PG_QUERY__NODE__NODE_ALTER_DEFAULT_PRIVILEGES_STMT, MappingKind::Equivalent, StmtType::ALTER},
        {PG_QUERY__NODE__NODE_ALTER_EVENT_TRIG_STMT, MappingKind::Equivalent, StmtType::ALTER},
        {PG_QUERY__NODE__NODE_ALTER_EXTENSION_STMT, MappingKind::Equivalent, StmtType::ALTER},
        {PG_QUERY__NODE__NODE_ALTER_EXTENSION_CONTENTS_STMT, MappingKind::Equivalent, StmtType::ALTER},
        {PG_QUERY__NODE__NODE_ALTER_FOREIGN_SERVER_STMT, MappingKind::Equivalent, StmtType::ALTER},
        {PG_QUERY__NODE__NODE_ALTER_FDW_STMT, MappingKind::Equivalent, StmtType::ALTER},
        {PG_QUERY__NODE__NODE_ALTER_OBJECT_DEPENDS_STMT, MappingKind::Equivalent, StmtType::ALTER},
        {PG_QUERY__NODE__NODE_ALTER_OBJECT_SCHEMA_STMT, MappingKind::Equivalent, StmtType::ALTER},
        {PG_QUERY__NODE__NODE_ALTER_OPERATOR_STMT, MappingKind::Equivalent, StmtType::ALTER},
        {PG_QUERY__NODE__NODE_ALTER_OP_FAMILY_STMT, MappingKind::Equivalent, StmtType::ALTER},
        {PG_QUERY__NODE__NODE_ALTER_OWNER_STMT, MappingKind::Equivalent, StmtType::ALTER},
        {PG_QUERY__NODE__NODE_ALTER_POLICY_STMT, MappingKind::Equivalent, StmtType::ALTER},
        {PG_QUERY__NODE__NODE_ALTER_PUBLICATION_STMT, MappingKind::Equivalent, StmtType::ALTER},
        {PG_QUERY__NODE__NODE_ALTER_ROLE_SET_STMT, MappingKind::Equivalent, StmtType::ALTER},
        {PG_QUERY__NODE__NODE_ALTER_STATS_STMT, MappingKind::Equivalent, StmtType::ALTER},
        {PG_QUERY__NODE__NODE_ALTER_SUBSCRIPTION_STMT, MappingKind::Equivalent, StmtType::ALTER},
        {PG_QUERY__NODE__NODE_ALTER_SYSTEM_STMT, MappingKind::Equivalent, StmtType::ALTER},
        {PG_QUERY__NODE__NODE_ALTER_TABLE_MOVE_ALL_STMT, MappingKind::Equivalent, StmtType::ALTER},
        {PG_QUERY__NODE__NODE_ALTER_TABLE_SPACE_OPTIONS_STMT, MappingKind::Equivalent, StmtType::ALTER},
        {PG_QUERY__NODE__NODE_ALTER_TYPE_STMT, MappingKind::Equivalent, StmtType::ALTER},
        {PG_QUERY__NODE__NODE_ALTER_TSCONFIGURATION_STMT, MappingKind::Equivalent, StmtType::ALTER},
        {PG_QUERY__NODE__NODE_ALTER_TSDICTIONARY_STMT, MappingKind::Equivalent, StmtType::ALTER},
        {PG_QUERY__NODE__NODE_ALTER_USER_MAPPING_STMT, MappingKind::Equivalent, StmtType::ALTER},
        {PG_QUERY__NODE__NODE_RENAME_STMT, MappingKind::Equivalent, StmtType::ALTER},
        {PG_QUERY__NODE__NODE_DROP_STMT, MappingKind::Equivalent, StmtType::DROP},
        {PG_QUERY__NODE__NODE_DROPDB_STMT, MappingKind::Equivalent, StmtType::DROP},
        {PG_QUERY__NODE__NODE_DROP_ROLE_STMT, MappingKind::Equivalent, StmtType::DROP},
        {PG_QUERY__NODE__NODE_DROP_TABLE_SPACE_STMT, MappingKind::Equivalent, StmtType::DROP},
        {PG_QUERY__NODE__NODE_DROP_OWNED_STMT, MappingKind::Equivalent, StmtType::DROP},
        {PG_QUERY__NODE__NODE_DROP_SUBSCRIPTION_STMT, MappingKind::Equivalent, StmtType::DROP},
        {PG_QUERY__NODE__NODE_DROP_USER_MAPPING_STMT, MappingKind::Equivalent, StmtType::DROP},
        {PG_QUERY__NODE__NODE_REASSIGN_OWNED_STMT, MappingKind::Equivalent, StmtType::ALTER},
        {PG_QUERY__NODE__NODE_CONSTRAINTS_SET_STMT, MappingKind::Equivalent, StmtType::SET},
        {PG_QUERY__NODE__NODE_COPY_STMT, MappingKind::NoEquivalent, StmtType::UNKNOWN},
        {PG_QUERY__NODE__NODE_MERGE_STMT, MappingKind::NoEquivalent, StmtType::UNKNOWN},
        {PG_QUERY__NODE__NODE_VACUUM_STMT, MappingKind::NoEquivalent, StmtType::UNKNOWN},
        {PG_QUERY__NODE__NODE_NOTIFY_STMT, MappingKind::NoEquivalent, StmtType::UNKNOWN},
        {PG_QUERY__NODE__NODE_LISTEN_STMT, MappingKind::NoEquivalent, StmtType::UNKNOWN},
        {PG_QUERY__NODE__NODE_UNLISTEN_STMT, MappingKind::NoEquivalent, StmtType::UNKNOWN},
        {PG_QUERY__NODE__NODE_CHECK_POINT_STMT, MappingKind::NoEquivalent, StmtType::UNKNOWN},
        {PG_QUERY__NODE__NODE_CLUSTER_STMT, MappingKind::NoEquivalent, StmtType::UNKNOWN},
        {PG_QUERY__NODE__NODE_CLOSE_PORTAL_STMT, MappingKind::NoEquivalent, StmtType::UNKNOWN},
        {PG_QUERY__NODE__NODE_COMMENT_STMT, MappingKind::NoEquivalent, StmtType::UNKNOWN},
        {PG_QUERY__NODE__NODE_DECLARE_CURSOR_STMT, MappingKind::NoEquivalent, StmtType::UNKNOWN},
        {PG_QUERY__NODE__NODE_DEFINE_STMT, MappingKind::NoEquivalent, StmtType::UNKNOWN},
        {PG_QUERY__NODE__NODE_DISCARD_STMT, MappingKind::NoEquivalent, StmtType::UNKNOWN},
        {PG_QUERY__NODE__NODE_FETCH_STMT, MappingKind::NoEquivalent, StmtType::UNKNOWN},
        {PG_QUERY__NODE__NODE_IMPORT_FOREIGN_SCHEMA_STMT, MappingKind::NoEquivalent, StmtType::UNKNOWN},
        {PG_QUERY__NODE__NODE_LOAD_STMT, MappingKind::NoEquivalent, StmtType::UNKNOWN},
        {PG_QUERY__NODE__NODE_REFRESH_MAT_VIEW_STMT, MappingKind::NoEquivalent, StmtType::UNKNOWN},
        {PG_QUERY__NODE__NODE_REINDEX_STMT, MappingKind::NoEquivalent, StmtType::UNKNOWN},
        {PG_QUERY__NODE__NODE_SEC_LABEL_STMT, MappingKind::NoEquivalent, StmtType::UNKNOWN},
    };

    for (const auto& test_case : cases) {
        PgQuery__Node node{};
        node.node_case = test_case.node_case;
        assert_mapping(
            pg_compat::expected_stmt_type(node),
            test_case.kind,
            test_case.type);
    }

    PgQuery__Node unknown{};
    unknown.node_case = PG_QUERY__NODE__NODE__NOT_SET;
    assert_mapping(
        pg_compat::expected_stmt_type(unknown),
        MappingKind::Unmapped,
        StmtType::UNKNOWN);
}

void test_transaction_mappings() {
    struct TransactionCase {
        PgQuery__TransactionStmtKind kind;
        StmtType type;
    };

    const TransactionCase cases[] = {
        {PG_QUERY__TRANSACTION_STMT_KIND__TRANS_STMT_BEGIN, StmtType::BEGIN},
        {PG_QUERY__TRANSACTION_STMT_KIND__TRANS_STMT_START, StmtType::START_TRANSACTION},
        {PG_QUERY__TRANSACTION_STMT_KIND__TRANS_STMT_COMMIT, StmtType::COMMIT},
        {PG_QUERY__TRANSACTION_STMT_KIND__TRANS_STMT_COMMIT_PREPARED, StmtType::COMMIT},
        {PG_QUERY__TRANSACTION_STMT_KIND__TRANS_STMT_ROLLBACK, StmtType::ROLLBACK},
        {PG_QUERY__TRANSACTION_STMT_KIND__TRANS_STMT_ROLLBACK_TO, StmtType::ROLLBACK},
        {PG_QUERY__TRANSACTION_STMT_KIND__TRANS_STMT_ROLLBACK_PREPARED, StmtType::ROLLBACK},
        {PG_QUERY__TRANSACTION_STMT_KIND__TRANS_STMT_SAVEPOINT, StmtType::SAVEPOINT},
        {PG_QUERY__TRANSACTION_STMT_KIND__TRANS_STMT_RELEASE, StmtType::SAVEPOINT},
        {PG_QUERY__TRANSACTION_STMT_KIND__TRANS_STMT_PREPARE, StmtType::PREPARE},
    };

    PgQuery__Node node{};
    node.node_case = PG_QUERY__NODE__NODE_TRANSACTION_STMT;

    PgQuery__TransactionStmt transaction{};
    node.transaction_stmt = &transaction;
    for (const auto& test_case : cases) {
        transaction.kind = test_case.kind;
        assert_mapping(
            pg_compat::expected_stmt_type(node),
            MappingKind::Equivalent,
            test_case.type);
    }

    transaction.kind =
        PG_QUERY__TRANSACTION_STMT_KIND__TRANSACTION_STMT_KIND_UNDEFINED;
    assert_mapping(
        pg_compat::expected_stmt_type(node),
        MappingKind::Unmapped,
        StmtType::UNKNOWN);

    transaction.kind = static_cast<PgQuery__TransactionStmtKind>(999);
    assert_mapping(
        pg_compat::expected_stmt_type(node),
        MappingKind::Unmapped,
        StmtType::UNKNOWN);

    node.transaction_stmt = nullptr;
    assert_mapping(
        pg_compat::expected_stmt_type(node),
        MappingKind::Unmapped,
        StmtType::UNKNOWN);
}

void test_grant_and_revoke_mappings() {
    PgQuery__Node grant_node{};
    grant_node.node_case = PG_QUERY__NODE__NODE_GRANT_STMT;
    assert_mapping(
        pg_compat::expected_stmt_type(grant_node),
        MappingKind::Unmapped,
        StmtType::UNKNOWN);

    PgQuery__GrantStmt grant{};
    grant_node.grant_stmt = &grant;
    grant.is_grant = 1;
    assert_mapping(
        pg_compat::expected_stmt_type(grant_node),
        MappingKind::Equivalent,
        StmtType::GRANT);
    grant.is_grant = 0;
    assert_mapping(
        pg_compat::expected_stmt_type(grant_node),
        MappingKind::Equivalent,
        StmtType::REVOKE);

    PgQuery__Node grant_role_node{};
    grant_role_node.node_case = PG_QUERY__NODE__NODE_GRANT_ROLE_STMT;
    assert_mapping(
        pg_compat::expected_stmt_type(grant_role_node),
        MappingKind::Unmapped,
        StmtType::UNKNOWN);

    PgQuery__GrantRoleStmt grant_role{};
    grant_role_node.grant_role_stmt = &grant_role;
    grant_role.is_grant = 1;
    assert_mapping(
        pg_compat::expected_stmt_type(grant_role_node),
        MappingKind::Equivalent,
        StmtType::GRANT);
    grant_role.is_grant = 0;
    assert_mapping(
        pg_compat::expected_stmt_type(grant_role_node),
        MappingKind::Equivalent,
        StmtType::REVOKE);
}

void test_oracle_node_names() {
    constexpr NameCase cases[] = {
        {PG_QUERY__NODE__NODE_SELECT_STMT, "PG_QUERY__NODE__NODE_SELECT_STMT"},
        {PG_QUERY__NODE__NODE_INSERT_STMT, "PG_QUERY__NODE__NODE_INSERT_STMT"},
        {PG_QUERY__NODE__NODE_UPDATE_STMT, "PG_QUERY__NODE__NODE_UPDATE_STMT"},
        {PG_QUERY__NODE__NODE_DELETE_STMT, "PG_QUERY__NODE__NODE_DELETE_STMT"},
        {PG_QUERY__NODE__NODE_VARIABLE_SET_STMT, "PG_QUERY__NODE__NODE_VARIABLE_SET_STMT"},
        {PG_QUERY__NODE__NODE_VARIABLE_SHOW_STMT, "PG_QUERY__NODE__NODE_VARIABLE_SHOW_STMT"},
        {PG_QUERY__NODE__NODE_PREPARE_STMT, "PG_QUERY__NODE__NODE_PREPARE_STMT"},
        {PG_QUERY__NODE__NODE_EXECUTE_STMT, "PG_QUERY__NODE__NODE_EXECUTE_STMT"},
        {PG_QUERY__NODE__NODE_DEALLOCATE_STMT, "PG_QUERY__NODE__NODE_DEALLOCATE_STMT"},
        {PG_QUERY__NODE__NODE_EXPLAIN_STMT, "PG_QUERY__NODE__NODE_EXPLAIN_STMT"},
        {PG_QUERY__NODE__NODE_CALL_STMT, "PG_QUERY__NODE__NODE_CALL_STMT"},
        {PG_QUERY__NODE__NODE_DO_STMT, "PG_QUERY__NODE__NODE_DO_STMT"},
        {PG_QUERY__NODE__NODE_TRUNCATE_STMT, "PG_QUERY__NODE__NODE_TRUNCATE_STMT"},
        {PG_QUERY__NODE__NODE_LOCK_STMT, "PG_QUERY__NODE__NODE_LOCK_STMT"},
        {PG_QUERY__NODE__NODE_TRANSACTION_STMT, "PG_QUERY__NODE__NODE_TRANSACTION_STMT"},
        {PG_QUERY__NODE__NODE_CREATE_STMT, "PG_QUERY__NODE__NODE_CREATE_STMT"},
        {PG_QUERY__NODE__NODE_CREATE_SCHEMA_STMT, "PG_QUERY__NODE__NODE_CREATE_SCHEMA_STMT"},
        {PG_QUERY__NODE__NODE_CREATE_TABLE_AS_STMT, "PG_QUERY__NODE__NODE_CREATE_TABLE_AS_STMT"},
        {PG_QUERY__NODE__NODE_INDEX_STMT, "PG_QUERY__NODE__NODE_INDEX_STMT"},
        {PG_QUERY__NODE__NODE_CREATE_FUNCTION_STMT, "PG_QUERY__NODE__NODE_CREATE_FUNCTION_STMT"},
        {PG_QUERY__NODE__NODE_CREATE_ROLE_STMT, "PG_QUERY__NODE__NODE_CREATE_ROLE_STMT"},
        {PG_QUERY__NODE__NODE_CREATE_SEQ_STMT, "PG_QUERY__NODE__NODE_CREATE_SEQ_STMT"},
        {PG_QUERY__NODE__NODE_CREATE_DOMAIN_STMT, "PG_QUERY__NODE__NODE_CREATE_DOMAIN_STMT"},
        {PG_QUERY__NODE__NODE_CREATE_ENUM_STMT, "PG_QUERY__NODE__NODE_CREATE_ENUM_STMT"},
        {PG_QUERY__NODE__NODE_CREATE_RANGE_STMT, "PG_QUERY__NODE__NODE_CREATE_RANGE_STMT"},
        {PG_QUERY__NODE__NODE_CREATE_AM_STMT, "PG_QUERY__NODE__NODE_CREATE_AM_STMT"},
        {PG_QUERY__NODE__NODE_CREATE_CAST_STMT, "PG_QUERY__NODE__NODE_CREATE_CAST_STMT"},
        {PG_QUERY__NODE__NODE_CREATE_CONVERSION_STMT, "PG_QUERY__NODE__NODE_CREATE_CONVERSION_STMT"},
        {PG_QUERY__NODE__NODE_CREATE_EVENT_TRIG_STMT, "PG_QUERY__NODE__NODE_CREATE_EVENT_TRIG_STMT"},
        {PG_QUERY__NODE__NODE_CREATE_EXTENSION_STMT, "PG_QUERY__NODE__NODE_CREATE_EXTENSION_STMT"},
        {PG_QUERY__NODE__NODE_CREATE_FDW_STMT, "PG_QUERY__NODE__NODE_CREATE_FDW_STMT"},
        {PG_QUERY__NODE__NODE_CREATE_FOREIGN_SERVER_STMT, "PG_QUERY__NODE__NODE_CREATE_FOREIGN_SERVER_STMT"},
        {PG_QUERY__NODE__NODE_CREATE_FOREIGN_TABLE_STMT, "PG_QUERY__NODE__NODE_CREATE_FOREIGN_TABLE_STMT"},
        {PG_QUERY__NODE__NODE_CREATE_OP_CLASS_STMT, "PG_QUERY__NODE__NODE_CREATE_OP_CLASS_STMT"},
        {PG_QUERY__NODE__NODE_CREATE_OP_FAMILY_STMT, "PG_QUERY__NODE__NODE_CREATE_OP_FAMILY_STMT"},
        {PG_QUERY__NODE__NODE_CREATE_PLANG_STMT, "PG_QUERY__NODE__NODE_CREATE_PLANG_STMT"},
        {PG_QUERY__NODE__NODE_CREATE_POLICY_STMT, "PG_QUERY__NODE__NODE_CREATE_POLICY_STMT"},
        {PG_QUERY__NODE__NODE_CREATE_PUBLICATION_STMT, "PG_QUERY__NODE__NODE_CREATE_PUBLICATION_STMT"},
        {PG_QUERY__NODE__NODE_CREATE_STATS_STMT, "PG_QUERY__NODE__NODE_CREATE_STATS_STMT"},
        {PG_QUERY__NODE__NODE_CREATE_SUBSCRIPTION_STMT, "PG_QUERY__NODE__NODE_CREATE_SUBSCRIPTION_STMT"},
        {PG_QUERY__NODE__NODE_CREATE_TABLE_SPACE_STMT, "PG_QUERY__NODE__NODE_CREATE_TABLE_SPACE_STMT"},
        {PG_QUERY__NODE__NODE_CREATE_TRANSFORM_STMT, "PG_QUERY__NODE__NODE_CREATE_TRANSFORM_STMT"},
        {PG_QUERY__NODE__NODE_CREATE_TRIG_STMT, "PG_QUERY__NODE__NODE_CREATE_TRIG_STMT"},
        {PG_QUERY__NODE__NODE_CREATE_USER_MAPPING_STMT, "PG_QUERY__NODE__NODE_CREATE_USER_MAPPING_STMT"},
        {PG_QUERY__NODE__NODE_COMPOSITE_TYPE_STMT, "PG_QUERY__NODE__NODE_COMPOSITE_TYPE_STMT"},
        {PG_QUERY__NODE__NODE_RULE_STMT, "PG_QUERY__NODE__NODE_RULE_STMT"},
        {PG_QUERY__NODE__NODE_VIEW_STMT, "PG_QUERY__NODE__NODE_VIEW_STMT"},
        {PG_QUERY__NODE__NODE_CREATEDB_STMT, "PG_QUERY__NODE__NODE_CREATEDB_STMT"},
        {PG_QUERY__NODE__NODE_ALTER_TABLE_STMT, "PG_QUERY__NODE__NODE_ALTER_TABLE_STMT"},
        {PG_QUERY__NODE__NODE_ALTER_FUNCTION_STMT, "PG_QUERY__NODE__NODE_ALTER_FUNCTION_STMT"},
        {PG_QUERY__NODE__NODE_ALTER_ROLE_STMT, "PG_QUERY__NODE__NODE_ALTER_ROLE_STMT"},
        {PG_QUERY__NODE__NODE_ALTER_SEQ_STMT, "PG_QUERY__NODE__NODE_ALTER_SEQ_STMT"},
        {PG_QUERY__NODE__NODE_ALTER_DOMAIN_STMT, "PG_QUERY__NODE__NODE_ALTER_DOMAIN_STMT"},
        {PG_QUERY__NODE__NODE_ALTER_ENUM_STMT, "PG_QUERY__NODE__NODE_ALTER_ENUM_STMT"},
        {PG_QUERY__NODE__NODE_ALTER_DATABASE_STMT, "PG_QUERY__NODE__NODE_ALTER_DATABASE_STMT"},
        {PG_QUERY__NODE__NODE_ALTER_DATABASE_SET_STMT, "PG_QUERY__NODE__NODE_ALTER_DATABASE_SET_STMT"},
        {PG_QUERY__NODE__NODE_ALTER_COLLATION_STMT, "PG_QUERY__NODE__NODE_ALTER_COLLATION_STMT"},
        {PG_QUERY__NODE__NODE_ALTER_DEFAULT_PRIVILEGES_STMT, "PG_QUERY__NODE__NODE_ALTER_DEFAULT_PRIVILEGES_STMT"},
        {PG_QUERY__NODE__NODE_ALTER_EVENT_TRIG_STMT, "PG_QUERY__NODE__NODE_ALTER_EVENT_TRIG_STMT"},
        {PG_QUERY__NODE__NODE_ALTER_EXTENSION_STMT, "PG_QUERY__NODE__NODE_ALTER_EXTENSION_STMT"},
        {PG_QUERY__NODE__NODE_ALTER_EXTENSION_CONTENTS_STMT, "PG_QUERY__NODE__NODE_ALTER_EXTENSION_CONTENTS_STMT"},
        {PG_QUERY__NODE__NODE_ALTER_FOREIGN_SERVER_STMT, "PG_QUERY__NODE__NODE_ALTER_FOREIGN_SERVER_STMT"},
        {PG_QUERY__NODE__NODE_ALTER_FDW_STMT, "PG_QUERY__NODE__NODE_ALTER_FDW_STMT"},
        {PG_QUERY__NODE__NODE_ALTER_OBJECT_DEPENDS_STMT, "PG_QUERY__NODE__NODE_ALTER_OBJECT_DEPENDS_STMT"},
        {PG_QUERY__NODE__NODE_ALTER_OBJECT_SCHEMA_STMT, "PG_QUERY__NODE__NODE_ALTER_OBJECT_SCHEMA_STMT"},
        {PG_QUERY__NODE__NODE_ALTER_OPERATOR_STMT, "PG_QUERY__NODE__NODE_ALTER_OPERATOR_STMT"},
        {PG_QUERY__NODE__NODE_ALTER_OP_FAMILY_STMT, "PG_QUERY__NODE__NODE_ALTER_OP_FAMILY_STMT"},
        {PG_QUERY__NODE__NODE_ALTER_OWNER_STMT, "PG_QUERY__NODE__NODE_ALTER_OWNER_STMT"},
        {PG_QUERY__NODE__NODE_ALTER_POLICY_STMT, "PG_QUERY__NODE__NODE_ALTER_POLICY_STMT"},
        {PG_QUERY__NODE__NODE_ALTER_PUBLICATION_STMT, "PG_QUERY__NODE__NODE_ALTER_PUBLICATION_STMT"},
        {PG_QUERY__NODE__NODE_ALTER_ROLE_SET_STMT, "PG_QUERY__NODE__NODE_ALTER_ROLE_SET_STMT"},
        {PG_QUERY__NODE__NODE_ALTER_STATS_STMT, "PG_QUERY__NODE__NODE_ALTER_STATS_STMT"},
        {PG_QUERY__NODE__NODE_ALTER_SUBSCRIPTION_STMT, "PG_QUERY__NODE__NODE_ALTER_SUBSCRIPTION_STMT"},
        {PG_QUERY__NODE__NODE_ALTER_SYSTEM_STMT, "PG_QUERY__NODE__NODE_ALTER_SYSTEM_STMT"},
        {PG_QUERY__NODE__NODE_ALTER_TABLE_MOVE_ALL_STMT, "PG_QUERY__NODE__NODE_ALTER_TABLE_MOVE_ALL_STMT"},
        {PG_QUERY__NODE__NODE_ALTER_TABLE_SPACE_OPTIONS_STMT, "PG_QUERY__NODE__NODE_ALTER_TABLE_SPACE_OPTIONS_STMT"},
        {PG_QUERY__NODE__NODE_ALTER_TYPE_STMT, "PG_QUERY__NODE__NODE_ALTER_TYPE_STMT"},
        {PG_QUERY__NODE__NODE_ALTER_TSCONFIGURATION_STMT, "PG_QUERY__NODE__NODE_ALTER_TSCONFIGURATION_STMT"},
        {PG_QUERY__NODE__NODE_ALTER_TSDICTIONARY_STMT, "PG_QUERY__NODE__NODE_ALTER_TSDICTIONARY_STMT"},
        {PG_QUERY__NODE__NODE_ALTER_USER_MAPPING_STMT, "PG_QUERY__NODE__NODE_ALTER_USER_MAPPING_STMT"},
        {PG_QUERY__NODE__NODE_RENAME_STMT, "PG_QUERY__NODE__NODE_RENAME_STMT"},
        {PG_QUERY__NODE__NODE_DROP_STMT, "PG_QUERY__NODE__NODE_DROP_STMT"},
        {PG_QUERY__NODE__NODE_DROPDB_STMT, "PG_QUERY__NODE__NODE_DROPDB_STMT"},
        {PG_QUERY__NODE__NODE_DROP_ROLE_STMT, "PG_QUERY__NODE__NODE_DROP_ROLE_STMT"},
        {PG_QUERY__NODE__NODE_DROP_TABLE_SPACE_STMT, "PG_QUERY__NODE__NODE_DROP_TABLE_SPACE_STMT"},
        {PG_QUERY__NODE__NODE_DROP_OWNED_STMT, "PG_QUERY__NODE__NODE_DROP_OWNED_STMT"},
        {PG_QUERY__NODE__NODE_DROP_SUBSCRIPTION_STMT, "PG_QUERY__NODE__NODE_DROP_SUBSCRIPTION_STMT"},
        {PG_QUERY__NODE__NODE_DROP_USER_MAPPING_STMT, "PG_QUERY__NODE__NODE_DROP_USER_MAPPING_STMT"},
        {PG_QUERY__NODE__NODE_REASSIGN_OWNED_STMT, "PG_QUERY__NODE__NODE_REASSIGN_OWNED_STMT"},
        {PG_QUERY__NODE__NODE_CONSTRAINTS_SET_STMT, "PG_QUERY__NODE__NODE_CONSTRAINTS_SET_STMT"},
        {PG_QUERY__NODE__NODE_COPY_STMT, "PG_QUERY__NODE__NODE_COPY_STMT"},
        {PG_QUERY__NODE__NODE_MERGE_STMT, "PG_QUERY__NODE__NODE_MERGE_STMT"},
        {PG_QUERY__NODE__NODE_VACUUM_STMT, "PG_QUERY__NODE__NODE_VACUUM_STMT"},
        {PG_QUERY__NODE__NODE_NOTIFY_STMT, "PG_QUERY__NODE__NODE_NOTIFY_STMT"},
        {PG_QUERY__NODE__NODE_LISTEN_STMT, "PG_QUERY__NODE__NODE_LISTEN_STMT"},
        {PG_QUERY__NODE__NODE_UNLISTEN_STMT, "PG_QUERY__NODE__NODE_UNLISTEN_STMT"},
        {PG_QUERY__NODE__NODE_CHECK_POINT_STMT, "PG_QUERY__NODE__NODE_CHECK_POINT_STMT"},
        {PG_QUERY__NODE__NODE_CLUSTER_STMT, "PG_QUERY__NODE__NODE_CLUSTER_STMT"},
        {PG_QUERY__NODE__NODE_CLOSE_PORTAL_STMT, "PG_QUERY__NODE__NODE_CLOSE_PORTAL_STMT"},
        {PG_QUERY__NODE__NODE_COMMENT_STMT, "PG_QUERY__NODE__NODE_COMMENT_STMT"},
        {PG_QUERY__NODE__NODE_DECLARE_CURSOR_STMT, "PG_QUERY__NODE__NODE_DECLARE_CURSOR_STMT"},
        {PG_QUERY__NODE__NODE_DEFINE_STMT, "PG_QUERY__NODE__NODE_DEFINE_STMT"},
        {PG_QUERY__NODE__NODE_DISCARD_STMT, "PG_QUERY__NODE__NODE_DISCARD_STMT"},
        {PG_QUERY__NODE__NODE_FETCH_STMT, "PG_QUERY__NODE__NODE_FETCH_STMT"},
        {PG_QUERY__NODE__NODE_IMPORT_FOREIGN_SCHEMA_STMT, "PG_QUERY__NODE__NODE_IMPORT_FOREIGN_SCHEMA_STMT"},
        {PG_QUERY__NODE__NODE_LOAD_STMT, "PG_QUERY__NODE__NODE_LOAD_STMT"},
        {PG_QUERY__NODE__NODE_REFRESH_MAT_VIEW_STMT, "PG_QUERY__NODE__NODE_REFRESH_MAT_VIEW_STMT"},
        {PG_QUERY__NODE__NODE_REINDEX_STMT, "PG_QUERY__NODE__NODE_REINDEX_STMT"},
        {PG_QUERY__NODE__NODE_SEC_LABEL_STMT, "PG_QUERY__NODE__NODE_SEC_LABEL_STMT"},
        {PG_QUERY__NODE__NODE_GRANT_STMT, "PG_QUERY__NODE__NODE_GRANT_STMT"},
        {PG_QUERY__NODE__NODE_GRANT_ROLE_STMT, "PG_QUERY__NODE__NODE_GRANT_ROLE_STMT"},
    };

    constexpr std::size_t case_count = sizeof(cases) / sizeof(cases[0]);
    static_assert(case_count == 111);
    assert(case_count == pg_compat::reviewed_node_case_count());

    for (const auto& test_case : cases) {
        const char* actual = pg_compat::oracle_node_name(test_case.node_case);
        assert(std::strcmp(actual, "UNMAPPED_NODE_CASE") != 0);
        assert(std::strcmp(actual, test_case.name) == 0);
    }

    assert(std::strcmp(
        pg_compat::oracle_node_name(PG_QUERY__NODE__NODE__NOT_SET),
        "UNMAPPED_NODE_CASE") == 0);
}

void test_stmt_type_names() {
    const StmtTypeNameCase cases[] = {
        {StmtType::UNKNOWN, "UNKNOWN"},
        {StmtType::SELECT, "SELECT"},
        {StmtType::INSERT, "INSERT"},
        {StmtType::UPDATE, "UPDATE"},
        {StmtType::DELETE_STMT, "DELETE"},
        {StmtType::REPLACE, "REPLACE"},
        {StmtType::SET, "SET"},
        {StmtType::USE, "USE"},
        {StmtType::SHOW, "SHOW"},
        {StmtType::BEGIN, "BEGIN"},
        {StmtType::START_TRANSACTION, "START_TRANSACTION"},
        {StmtType::COMMIT, "COMMIT"},
        {StmtType::ROLLBACK, "ROLLBACK"},
        {StmtType::SAVEPOINT, "SAVEPOINT"},
        {StmtType::PREPARE, "PREPARE"},
        {StmtType::EXECUTE, "EXECUTE"},
        {StmtType::DEALLOCATE, "DEALLOCATE"},
        {StmtType::CREATE, "CREATE"},
        {StmtType::ALTER, "ALTER"},
        {StmtType::DROP, "DROP"},
        {StmtType::TRUNCATE, "TRUNCATE"},
        {StmtType::GRANT, "GRANT"},
        {StmtType::REVOKE, "REVOKE"},
        {StmtType::LOCK, "LOCK"},
        {StmtType::UNLOCK, "UNLOCK"},
        {StmtType::LOAD_DATA, "LOAD_DATA"},
        {StmtType::RESET, "RESET"},
        {StmtType::EXPLAIN, "EXPLAIN"},
        {StmtType::DESCRIBE, "DESCRIBE"},
        {StmtType::CALL, "CALL"},
        {StmtType::DO_STMT, "DO"},
    };

    for (const auto& test_case : cases) {
        assert(std::strcmp(
            pg_compat::stmt_type_name(test_case.type),
            test_case.name) == 0);
    }

    assert(std::strcmp(
        pg_compat::stmt_type_name(static_cast<StmtType>(255)),
        "OTHER") == 0);
}

} // namespace

int main() {
    test_result_names();
    test_default_and_simple_mappings();
    test_transaction_mappings();
    test_grant_and_revoke_mappings();
    test_oracle_node_names();
    test_stmt_type_names();
    return 0;
}
