// Tests for ResultSet.own_string string-storage stability.
//
// The original implementation used std::vector<std::string> for owned_strings
// and returned StringRef{c_str(), size()}. For short strings using SSO, the
// character data lives inside the std::string object itself, so a vector
// reallocation moved every previously-stored string to a new address and
// invalidated every StringRef the caller had captured. This produced silent
// data corruption for short text columns coming back through the wire
// protocol -- the symptom seen in the bare-metal benchmarks where
// SELECT * FROM users LIMIT 3 returned garbled bytes for name/dept columns.
//
// The fix replaces owned_strings with std::deque<std::string>, which has
// stable element addresses across push_back. These tests lock in that
// invariant.

#include "sql_engine/result_set.h"
#include "gtest/gtest.h"
#include <vector>
#include <string>
#include <cstring>

using namespace sql_engine;
using sql_parser::StringRef;

// Force enough push_back calls to trigger several reallocations had we
// kept std::vector. Verify every previously-handed-out StringRef still
// points at the correct bytes.
TEST(ResultSetOwnString, ShortStringsRemainValidAfterManyPushBacks) {
    ResultSet rs;
    std::vector<StringRef> refs;
    std::vector<std::string> originals;

    // 200 short strings -- well past any reasonable initial vector capacity.
    // All of these are SSO-eligible (<= 15 chars on glibc).
    for (int i = 0; i < 200; ++i) {
        std::string s = "row_" + std::to_string(i);
        originals.push_back(s);
        refs.push_back(rs.own_string(s.data(), static_cast<uint32_t>(s.size())));
    }

    // Every previously-stored ref must still match its original.
    for (size_t i = 0; i < refs.size(); ++i) {
        ASSERT_EQ(refs[i].len, originals[i].size())
            << "ref " << i << " length wrong";
        ASSERT_EQ(0, std::memcmp(refs[i].ptr, originals[i].data(), refs[i].len))
            << "ref " << i << " data corrupted -- expected '"
            << originals[i] << "'";
    }
}

// Long strings (> SSO threshold) historically were less likely to corrupt
// because std::string's move constructor steals the heap pointer. Validate
// they also still work after the deque change.
TEST(ResultSetOwnString, LongStringsRemainValidAfterManyPushBacks) {
    ResultSet rs;
    std::vector<StringRef> refs;
    std::vector<std::string> originals;

    for (int i = 0; i < 200; ++i) {
        std::string s = "this_is_definitely_longer_than_sso_for_row_" + std::to_string(i);
        originals.push_back(s);
        refs.push_back(rs.own_string(s.data(), static_cast<uint32_t>(s.size())));
    }

    for (size_t i = 0; i < refs.size(); ++i) {
        ASSERT_EQ(refs[i].len, originals[i].size());
        ASSERT_EQ(0, std::memcmp(refs[i].ptr, originals[i].data(), refs[i].len));
    }
}

// Mixed sizes -- the most realistic case for actual MySQL result sets.
TEST(ResultSetOwnString, MixedSizesRemainValidAfterManyPushBacks) {
    ResultSet rs;
    std::vector<StringRef> refs;
    std::vector<std::string> originals;

    for (int i = 0; i < 500; ++i) {
        std::string s;
        if (i % 3 == 0) s = "x";                                    // 1 char (SSO)
        else if (i % 3 == 1) s = "Engineering";                     // 11 chars (SSO)
        else s = "this_is_a_string_longer_than_sso_threshold_" + std::to_string(i);
        originals.push_back(s);
        refs.push_back(rs.own_string(s.data(), static_cast<uint32_t>(s.size())));
    }

    for (size_t i = 0; i < refs.size(); ++i) {
        ASSERT_EQ(refs[i].len, originals[i].size())
            << "ref " << i << " length wrong, expected '" << originals[i] << "'";
        ASSERT_EQ(0, std::memcmp(refs[i].ptr, originals[i].data(), refs[i].len))
            << "ref " << i << " data corrupted, expected '" << originals[i] << "'";
    }
}

// Strings stored as Value::str_val should round-trip through the wire-style
// access pattern: store the string, build a Value referring to it, store
// hundreds more strings, then read the Value and confirm the data is intact.
TEST(ResultSetOwnString, ValueStrValRemainsValidAfterManyPushBacks) {
    ResultSet rs;
    Row& row = rs.add_heap_row(1);

    StringRef first = rs.own_string("Alice", 5);
    row.set(0, value_string(first));

    // Add 1000 more strings to force several deque-block allocations.
    for (int i = 0; i < 1000; ++i) {
        std::string filler = "filler_row_" + std::to_string(i);
        rs.own_string(filler.data(), static_cast<uint32_t>(filler.size()));
    }

    // The Value stored in the row must still point at "Alice".
    const Value& v = row.get(0);
    ASSERT_EQ(v.tag, Value::TAG_STRING);
    ASSERT_EQ(v.str_val.len, 5u);
    ASSERT_EQ(0, std::memcmp(v.str_val.ptr, "Alice", 5));
}
