#include "tools/pg_compat/statement_type_map.h"

namespace pg_compat {

#define PG_COMPAT_REVIEWED_NODE_CASES(SIMPLE, PAYLOAD) \
    SIMPLE(PG_QUERY__NODE__NODE_SELECT_STMT, Equivalent, SELECT) \
    SIMPLE(PG_QUERY__NODE__NODE_INSERT_STMT, Equivalent, INSERT) \
    SIMPLE(PG_QUERY__NODE__NODE_UPDATE_STMT, Equivalent, UPDATE) \
    SIMPLE(PG_QUERY__NODE__NODE_DELETE_STMT, Equivalent, DELETE_STMT) \
    SIMPLE(PG_QUERY__NODE__NODE_VARIABLE_SET_STMT, Equivalent, SET) \
    SIMPLE(PG_QUERY__NODE__NODE_VARIABLE_SHOW_STMT, Equivalent, SHOW) \
    SIMPLE(PG_QUERY__NODE__NODE_PREPARE_STMT, Equivalent, PREPARE) \
    SIMPLE(PG_QUERY__NODE__NODE_EXECUTE_STMT, Equivalent, EXECUTE) \
    SIMPLE(PG_QUERY__NODE__NODE_DEALLOCATE_STMT, Equivalent, DEALLOCATE) \
    SIMPLE(PG_QUERY__NODE__NODE_EXPLAIN_STMT, Equivalent, EXPLAIN) \
    SIMPLE(PG_QUERY__NODE__NODE_CALL_STMT, Equivalent, CALL) \
    SIMPLE(PG_QUERY__NODE__NODE_DO_STMT, Equivalent, DO_STMT) \
    SIMPLE(PG_QUERY__NODE__NODE_TRUNCATE_STMT, Equivalent, TRUNCATE) \
    SIMPLE(PG_QUERY__NODE__NODE_LOCK_STMT, Equivalent, LOCK) \
    SIMPLE(PG_QUERY__NODE__NODE_CREATE_STMT, Equivalent, CREATE) \
    SIMPLE(PG_QUERY__NODE__NODE_CREATE_SCHEMA_STMT, Equivalent, CREATE) \
    SIMPLE(PG_QUERY__NODE__NODE_CREATE_TABLE_AS_STMT, Equivalent, CREATE) \
    SIMPLE(PG_QUERY__NODE__NODE_INDEX_STMT, Equivalent, CREATE) \
    SIMPLE(PG_QUERY__NODE__NODE_CREATE_FUNCTION_STMT, Equivalent, CREATE) \
    SIMPLE(PG_QUERY__NODE__NODE_CREATE_ROLE_STMT, Equivalent, CREATE) \
    SIMPLE(PG_QUERY__NODE__NODE_CREATE_SEQ_STMT, Equivalent, CREATE) \
    SIMPLE(PG_QUERY__NODE__NODE_CREATE_DOMAIN_STMT, Equivalent, CREATE) \
    SIMPLE(PG_QUERY__NODE__NODE_CREATE_ENUM_STMT, Equivalent, CREATE) \
    SIMPLE(PG_QUERY__NODE__NODE_CREATE_RANGE_STMT, Equivalent, CREATE) \
    SIMPLE(PG_QUERY__NODE__NODE_CREATE_AM_STMT, Equivalent, CREATE) \
    SIMPLE(PG_QUERY__NODE__NODE_CREATE_CAST_STMT, Equivalent, CREATE) \
    SIMPLE(PG_QUERY__NODE__NODE_CREATE_CONVERSION_STMT, Equivalent, CREATE) \
    SIMPLE(PG_QUERY__NODE__NODE_CREATE_EVENT_TRIG_STMT, Equivalent, CREATE) \
    SIMPLE(PG_QUERY__NODE__NODE_CREATE_EXTENSION_STMT, Equivalent, CREATE) \
    SIMPLE(PG_QUERY__NODE__NODE_CREATE_FDW_STMT, Equivalent, CREATE) \
    SIMPLE(PG_QUERY__NODE__NODE_CREATE_FOREIGN_SERVER_STMT, Equivalent, CREATE) \
    SIMPLE(PG_QUERY__NODE__NODE_CREATE_FOREIGN_TABLE_STMT, Equivalent, CREATE) \
    SIMPLE(PG_QUERY__NODE__NODE_CREATE_OP_CLASS_STMT, Equivalent, CREATE) \
    SIMPLE(PG_QUERY__NODE__NODE_CREATE_OP_FAMILY_STMT, Equivalent, CREATE) \
    SIMPLE(PG_QUERY__NODE__NODE_CREATE_PLANG_STMT, Equivalent, CREATE) \
    SIMPLE(PG_QUERY__NODE__NODE_CREATE_POLICY_STMT, Equivalent, CREATE) \
    SIMPLE(PG_QUERY__NODE__NODE_CREATE_PUBLICATION_STMT, Equivalent, CREATE) \
    SIMPLE(PG_QUERY__NODE__NODE_CREATE_STATS_STMT, Equivalent, CREATE) \
    SIMPLE(PG_QUERY__NODE__NODE_CREATE_SUBSCRIPTION_STMT, Equivalent, CREATE) \
    SIMPLE(PG_QUERY__NODE__NODE_CREATE_TABLE_SPACE_STMT, Equivalent, CREATE) \
    SIMPLE(PG_QUERY__NODE__NODE_CREATE_TRANSFORM_STMT, Equivalent, CREATE) \
    SIMPLE(PG_QUERY__NODE__NODE_CREATE_TRIG_STMT, Equivalent, CREATE) \
    SIMPLE(PG_QUERY__NODE__NODE_CREATE_USER_MAPPING_STMT, Equivalent, CREATE) \
    SIMPLE(PG_QUERY__NODE__NODE_COMPOSITE_TYPE_STMT, Equivalent, CREATE) \
    SIMPLE(PG_QUERY__NODE__NODE_RULE_STMT, Equivalent, CREATE) \
    SIMPLE(PG_QUERY__NODE__NODE_VIEW_STMT, Equivalent, CREATE) \
    SIMPLE(PG_QUERY__NODE__NODE_CREATEDB_STMT, Equivalent, CREATE) \
    SIMPLE(PG_QUERY__NODE__NODE_ALTER_TABLE_STMT, Equivalent, ALTER) \
    SIMPLE(PG_QUERY__NODE__NODE_ALTER_FUNCTION_STMT, Equivalent, ALTER) \
    SIMPLE(PG_QUERY__NODE__NODE_ALTER_ROLE_STMT, Equivalent, ALTER) \
    SIMPLE(PG_QUERY__NODE__NODE_ALTER_SEQ_STMT, Equivalent, ALTER) \
    SIMPLE(PG_QUERY__NODE__NODE_ALTER_DOMAIN_STMT, Equivalent, ALTER) \
    SIMPLE(PG_QUERY__NODE__NODE_ALTER_ENUM_STMT, Equivalent, ALTER) \
    SIMPLE(PG_QUERY__NODE__NODE_ALTER_DATABASE_STMT, Equivalent, ALTER) \
    SIMPLE(PG_QUERY__NODE__NODE_ALTER_DATABASE_SET_STMT, Equivalent, ALTER) \
    SIMPLE(PG_QUERY__NODE__NODE_ALTER_COLLATION_STMT, Equivalent, ALTER) \
    SIMPLE(PG_QUERY__NODE__NODE_ALTER_DEFAULT_PRIVILEGES_STMT, Equivalent, ALTER) \
    SIMPLE(PG_QUERY__NODE__NODE_ALTER_EVENT_TRIG_STMT, Equivalent, ALTER) \
    SIMPLE(PG_QUERY__NODE__NODE_ALTER_EXTENSION_STMT, Equivalent, ALTER) \
    SIMPLE(PG_QUERY__NODE__NODE_ALTER_EXTENSION_CONTENTS_STMT, Equivalent, ALTER) \
    SIMPLE(PG_QUERY__NODE__NODE_ALTER_FOREIGN_SERVER_STMT, Equivalent, ALTER) \
    SIMPLE(PG_QUERY__NODE__NODE_ALTER_FDW_STMT, Equivalent, ALTER) \
    SIMPLE(PG_QUERY__NODE__NODE_ALTER_OBJECT_DEPENDS_STMT, Equivalent, ALTER) \
    SIMPLE(PG_QUERY__NODE__NODE_ALTER_OBJECT_SCHEMA_STMT, Equivalent, ALTER) \
    SIMPLE(PG_QUERY__NODE__NODE_ALTER_OPERATOR_STMT, Equivalent, ALTER) \
    SIMPLE(PG_QUERY__NODE__NODE_ALTER_OP_FAMILY_STMT, Equivalent, ALTER) \
    SIMPLE(PG_QUERY__NODE__NODE_ALTER_OWNER_STMT, Equivalent, ALTER) \
    SIMPLE(PG_QUERY__NODE__NODE_ALTER_POLICY_STMT, Equivalent, ALTER) \
    SIMPLE(PG_QUERY__NODE__NODE_ALTER_PUBLICATION_STMT, Equivalent, ALTER) \
    SIMPLE(PG_QUERY__NODE__NODE_ALTER_ROLE_SET_STMT, Equivalent, ALTER) \
    SIMPLE(PG_QUERY__NODE__NODE_ALTER_STATS_STMT, Equivalent, ALTER) \
    SIMPLE(PG_QUERY__NODE__NODE_ALTER_SUBSCRIPTION_STMT, Equivalent, ALTER) \
    SIMPLE(PG_QUERY__NODE__NODE_ALTER_SYSTEM_STMT, Equivalent, ALTER) \
    SIMPLE(PG_QUERY__NODE__NODE_ALTER_TABLE_MOVE_ALL_STMT, Equivalent, ALTER) \
    SIMPLE(PG_QUERY__NODE__NODE_ALTER_TABLE_SPACE_OPTIONS_STMT, Equivalent, ALTER) \
    SIMPLE(PG_QUERY__NODE__NODE_ALTER_TYPE_STMT, Equivalent, ALTER) \
    SIMPLE(PG_QUERY__NODE__NODE_ALTER_TSCONFIGURATION_STMT, Equivalent, ALTER) \
    SIMPLE(PG_QUERY__NODE__NODE_ALTER_TSDICTIONARY_STMT, Equivalent, ALTER) \
    SIMPLE(PG_QUERY__NODE__NODE_ALTER_USER_MAPPING_STMT, Equivalent, ALTER) \
    SIMPLE(PG_QUERY__NODE__NODE_RENAME_STMT, Equivalent, ALTER) \
    SIMPLE(PG_QUERY__NODE__NODE_DROP_STMT, Equivalent, DROP) \
    SIMPLE(PG_QUERY__NODE__NODE_DROPDB_STMT, Equivalent, DROP) \
    SIMPLE(PG_QUERY__NODE__NODE_DROP_ROLE_STMT, Equivalent, DROP) \
    SIMPLE(PG_QUERY__NODE__NODE_DROP_TABLE_SPACE_STMT, Equivalent, DROP) \
    SIMPLE(PG_QUERY__NODE__NODE_DROP_OWNED_STMT, Equivalent, DROP) \
    SIMPLE(PG_QUERY__NODE__NODE_DROP_SUBSCRIPTION_STMT, Equivalent, DROP) \
    SIMPLE(PG_QUERY__NODE__NODE_DROP_USER_MAPPING_STMT, Equivalent, DROP) \
    SIMPLE(PG_QUERY__NODE__NODE_REASSIGN_OWNED_STMT, Equivalent, ALTER) \
    SIMPLE(PG_QUERY__NODE__NODE_CONSTRAINTS_SET_STMT, Equivalent, SET) \
    SIMPLE(PG_QUERY__NODE__NODE_COPY_STMT, NoEquivalent, UNKNOWN) \
    SIMPLE(PG_QUERY__NODE__NODE_MERGE_STMT, NoEquivalent, UNKNOWN) \
    SIMPLE(PG_QUERY__NODE__NODE_VACUUM_STMT, NoEquivalent, UNKNOWN) \
    SIMPLE(PG_QUERY__NODE__NODE_NOTIFY_STMT, NoEquivalent, UNKNOWN) \
    SIMPLE(PG_QUERY__NODE__NODE_LISTEN_STMT, NoEquivalent, UNKNOWN) \
    SIMPLE(PG_QUERY__NODE__NODE_UNLISTEN_STMT, NoEquivalent, UNKNOWN) \
    SIMPLE(PG_QUERY__NODE__NODE_CHECK_POINT_STMT, NoEquivalent, UNKNOWN) \
    SIMPLE(PG_QUERY__NODE__NODE_CLUSTER_STMT, NoEquivalent, UNKNOWN) \
    SIMPLE(PG_QUERY__NODE__NODE_CLOSE_PORTAL_STMT, NoEquivalent, UNKNOWN) \
    SIMPLE(PG_QUERY__NODE__NODE_COMMENT_STMT, NoEquivalent, UNKNOWN) \
    SIMPLE(PG_QUERY__NODE__NODE_DECLARE_CURSOR_STMT, NoEquivalent, UNKNOWN) \
    SIMPLE(PG_QUERY__NODE__NODE_DEFINE_STMT, NoEquivalent, UNKNOWN) \
    SIMPLE(PG_QUERY__NODE__NODE_DISCARD_STMT, NoEquivalent, UNKNOWN) \
    SIMPLE(PG_QUERY__NODE__NODE_FETCH_STMT, NoEquivalent, UNKNOWN) \
    SIMPLE(PG_QUERY__NODE__NODE_IMPORT_FOREIGN_SCHEMA_STMT, NoEquivalent, UNKNOWN) \
    SIMPLE(PG_QUERY__NODE__NODE_LOAD_STMT, NoEquivalent, UNKNOWN) \
    SIMPLE(PG_QUERY__NODE__NODE_REFRESH_MAT_VIEW_STMT, NoEquivalent, UNKNOWN) \
    SIMPLE(PG_QUERY__NODE__NODE_REINDEX_STMT, NoEquivalent, UNKNOWN) \
    SIMPLE(PG_QUERY__NODE__NODE_SEC_LABEL_STMT, NoEquivalent, UNKNOWN) \
    PAYLOAD(PG_QUERY__NODE__NODE_TRANSACTION_STMT, map_transaction_stmt) \
    PAYLOAD(PG_QUERY__NODE__NODE_GRANT_STMT, map_grant_stmt) \
    PAYLOAD(PG_QUERY__NODE__NODE_GRANT_ROLE_STMT, map_grant_role_stmt)

namespace {

StatementTypeMapping map_transaction_stmt(const PgQuery__Node& node) {
    using sql_parser::StmtType;

    if (node.transaction_stmt == nullptr) {
        return {};
    }
    switch (node.transaction_stmt->kind) {
    case PG_QUERY__TRANSACTION_STMT_KIND__TRANS_STMT_BEGIN:
        return {MappingKind::Equivalent, StmtType::BEGIN};
    case PG_QUERY__TRANSACTION_STMT_KIND__TRANS_STMT_START:
        return {MappingKind::Equivalent, StmtType::START_TRANSACTION};
    case PG_QUERY__TRANSACTION_STMT_KIND__TRANS_STMT_COMMIT:
    case PG_QUERY__TRANSACTION_STMT_KIND__TRANS_STMT_COMMIT_PREPARED:
        return {MappingKind::Equivalent, StmtType::COMMIT};
    case PG_QUERY__TRANSACTION_STMT_KIND__TRANS_STMT_ROLLBACK:
    case PG_QUERY__TRANSACTION_STMT_KIND__TRANS_STMT_ROLLBACK_TO:
    case PG_QUERY__TRANSACTION_STMT_KIND__TRANS_STMT_ROLLBACK_PREPARED:
        return {MappingKind::Equivalent, StmtType::ROLLBACK};
    case PG_QUERY__TRANSACTION_STMT_KIND__TRANS_STMT_SAVEPOINT:
    case PG_QUERY__TRANSACTION_STMT_KIND__TRANS_STMT_RELEASE:
        return {MappingKind::Equivalent, StmtType::SAVEPOINT};
    case PG_QUERY__TRANSACTION_STMT_KIND__TRANS_STMT_PREPARE:
        return {MappingKind::Equivalent, StmtType::PREPARE};
    default:
        return {};
    }
}

StatementTypeMapping map_grant_stmt(const PgQuery__Node& node) {
    using sql_parser::StmtType;

    if (node.grant_stmt == nullptr) {
        return {};
    }
    return {
        MappingKind::Equivalent,
        node.grant_stmt->is_grant ? StmtType::GRANT : StmtType::REVOKE,
    };
}

StatementTypeMapping map_grant_role_stmt(const PgQuery__Node& node) {
    using sql_parser::StmtType;

    if (node.grant_role_stmt == nullptr) {
        return {};
    }
    return {
        MappingKind::Equivalent,
        node.grant_role_stmt->is_grant ? StmtType::GRANT : StmtType::REVOKE,
    };
}

} // namespace

StatementTypeMapping expected_stmt_type(const PgQuery__Node& node) {
    using sql_parser::StmtType;

    switch (node.node_case) {
#define PG_COMPAT_SIMPLE_MAPPING_CASE(node_case, mapping_kind, stmt_type) \
    case node_case: \
        return {MappingKind::mapping_kind, StmtType::stmt_type};
#define PG_COMPAT_PAYLOAD_MAPPING_CASE(node_case, helper) \
    case node_case: \
        return helper(node);
    PG_COMPAT_REVIEWED_NODE_CASES(
        PG_COMPAT_SIMPLE_MAPPING_CASE,
        PG_COMPAT_PAYLOAD_MAPPING_CASE)
#undef PG_COMPAT_PAYLOAD_MAPPING_CASE
#undef PG_COMPAT_SIMPLE_MAPPING_CASE
    default:
        return {};
    }
}

const char* oracle_node_name(PgQuery__Node__NodeCase node_case) {
    switch (node_case) {
#define PG_COMPAT_SIMPLE_NODE_NAME_CASE(node_case, mapping_kind, stmt_type) \
    case node_case: \
        return #node_case;
#define PG_COMPAT_PAYLOAD_NODE_NAME_CASE(node_case, helper) \
    case node_case: \
        return #node_case;
    PG_COMPAT_REVIEWED_NODE_CASES(
        PG_COMPAT_SIMPLE_NODE_NAME_CASE,
        PG_COMPAT_PAYLOAD_NODE_NAME_CASE)
#undef PG_COMPAT_PAYLOAD_NODE_NAME_CASE
#undef PG_COMPAT_SIMPLE_NODE_NAME_CASE
    default:
        return "UNMAPPED_NODE_CASE";
    }
}

std::size_t reviewed_node_case_count() {
#define PG_COMPAT_SIMPLE_NODE_COUNT(node_case, mapping_kind, stmt_type) + 1
#define PG_COMPAT_PAYLOAD_NODE_COUNT(node_case, helper) + 1
    return 0 PG_COMPAT_REVIEWED_NODE_CASES(
        PG_COMPAT_SIMPLE_NODE_COUNT,
        PG_COMPAT_PAYLOAD_NODE_COUNT);
#undef PG_COMPAT_PAYLOAD_NODE_COUNT
#undef PG_COMPAT_SIMPLE_NODE_COUNT
}

const char* stmt_type_name(sql_parser::StmtType type) {
    using sql_parser::StmtType;

    // Missing enum cases are errors so StmtType additions cannot drift silently.
#if defined(__clang__) || defined(__GNUC__)
#pragma GCC diagnostic push
#pragma GCC diagnostic error "-Wswitch"
#endif
    switch (type) {
    case StmtType::UNKNOWN:
        return "UNKNOWN";
    case StmtType::SELECT:
        return "SELECT";
    case StmtType::INSERT:
        return "INSERT";
    case StmtType::UPDATE:
        return "UPDATE";
    case StmtType::DELETE_STMT:
        return "DELETE";
    case StmtType::REPLACE:
        return "REPLACE";
    case StmtType::SET:
        return "SET";
    case StmtType::USE:
        return "USE";
    case StmtType::SHOW:
        return "SHOW";
    case StmtType::BEGIN:
        return "BEGIN";
    case StmtType::START_TRANSACTION:
        return "START_TRANSACTION";
    case StmtType::COMMIT:
        return "COMMIT";
    case StmtType::ROLLBACK:
        return "ROLLBACK";
    case StmtType::SAVEPOINT:
        return "SAVEPOINT";
    case StmtType::PREPARE:
        return "PREPARE";
    case StmtType::EXECUTE:
        return "EXECUTE";
    case StmtType::DEALLOCATE:
        return "DEALLOCATE";
    case StmtType::CREATE:
        return "CREATE";
    case StmtType::ALTER:
        return "ALTER";
    case StmtType::DROP:
        return "DROP";
    case StmtType::TRUNCATE:
        return "TRUNCATE";
    case StmtType::GRANT:
        return "GRANT";
    case StmtType::REVOKE:
        return "REVOKE";
    case StmtType::LOCK:
        return "LOCK";
    case StmtType::UNLOCK:
        return "UNLOCK";
    case StmtType::LOAD_DATA:
        return "LOAD_DATA";
    case StmtType::RESET:
        return "RESET";
    case StmtType::EXPLAIN:
        return "EXPLAIN";
    case StmtType::DESCRIBE:
        return "DESCRIBE";
    case StmtType::CALL:
        return "CALL";
    case StmtType::DO_STMT:
        return "DO";
    }
#if defined(__clang__) || defined(__GNUC__)
#pragma GCC diagnostic pop
#endif
    return "OTHER";
}

#undef PG_COMPAT_REVIEWED_NODE_CASES

} // namespace pg_compat
