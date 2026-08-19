#ifndef TOOLS_PG_COMPAT_RESULT_H
#define TOOLS_PG_COMPAT_RESULT_H

namespace pg_compat {

enum class CompatibilityResult {
    DeepSupported,
    ClassifiedOnly,
    Partial,
    Error,
    TrailingInput,
    TypeMismatch,
    OracleRejected,
};

inline const char* result_name(CompatibilityResult result) {
    switch (result) {
    case CompatibilityResult::DeepSupported:
        return "DEEP_SUPPORTED";
    case CompatibilityResult::ClassifiedOnly:
        return "CLASSIFIED_ONLY";
    case CompatibilityResult::Partial:
        return "PARTIAL";
    case CompatibilityResult::Error:
        return "ERROR";
    case CompatibilityResult::TrailingInput:
        return "TRAILING_INPUT";
    case CompatibilityResult::TypeMismatch:
        return "TYPE_MISMATCH";
    case CompatibilityResult::OracleRejected:
        return "ORACLE_REJECTED";
    }
    return "ERROR";
}

} // namespace pg_compat

#endif // TOOLS_PG_COMPAT_RESULT_H
