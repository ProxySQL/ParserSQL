# Three Features Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add auto-compaction to WAL, correctly handle PostgreSQL TIMESTAMPTZ, and run the corpus test suite against the current main branch.

**Architecture:** (1) WAL auto-compaction: counter-based threshold in `DistributedTransactionManager`, fires `compact()` after N completions. (2) TIMESTAMPTZ: extend `datetime_parse` to extract timezone offset and normalize to UTC — no Value struct changes. (3) Corpus testing: run `scripts/run_benchmarks.sh`, compare to March baseline, fix any regressions.

**Tech Stack:** C++17, Google Test, POSIX file APIs, libpq.

---

## Task 1: WAL Auto-Compaction

**Files:**
- Modify: `include/sql_engine/distributed_txn.h` (add threshold config + counter)
- Modify: `tests/test_distributed_txn.cpp` (add tests)

### Step 1.1: Write failing test for auto-compaction threshold

- [ ] **Add test to `tests/test_distributed_txn.cpp`** (add near the other DurableTransactionLog tests around line 400-500)

```cpp
TEST(DurableTxnLogAutoCompactTest, AutoCompactFiresAtThreshold) {
    // Create a temp log path
    std::string log_path = "/tmp/test_auto_compact_" +
                           std::to_string(::getpid()) + ".log";
    ::unlink(log_path.c_str());

    DurableTransactionLog log;
    ASSERT_TRUE(log.open(log_path));

    MockRemoteExecutor exec;
    exec.add_mock_backend("b1");
    DistributedTransactionManager mgr(exec);
    mgr.set_durable_log(&log);
    mgr.set_auto_compact_threshold(3);  // compact after 3 completions

    // Run 3 complete 2PC transactions
    for (int i = 0; i < 3; ++i) {
        ASSERT_TRUE(mgr.begin());
        ASSERT_TRUE(mgr.enlist_backend("b1"));
        ASSERT_TRUE(mgr.commit());
    }

    // After 3 completions, compaction should have fired.
    // Verify: in-doubt set is empty, file is small.
    auto in_doubt = DurableTransactionLog::scan_in_doubt(log_path);
    EXPECT_TRUE(in_doubt.empty());

    // File should contain only trailing records (compaction rewrites to just in-doubt = empty)
    struct stat st;
    ASSERT_EQ(::stat(log_path.c_str(), &st), 0);
    // Compacted file should be ≤ some small size (exact size depends on whether
    // a 4th commit happened after compact; the key check is that in_doubt is empty)
    EXPECT_LT(st.st_size, 1024);  // sanity bound

    ::unlink(log_path.c_str());
}

TEST(DurableTxnLogAutoCompactTest, ThresholdZeroDisablesAutoCompact) {
    std::string log_path = "/tmp/test_auto_compact_disabled_" +
                           std::to_string(::getpid()) + ".log";
    ::unlink(log_path.c_str());

    DurableTransactionLog log;
    ASSERT_TRUE(log.open(log_path));

    MockRemoteExecutor exec;
    exec.add_mock_backend("b1");
    DistributedTransactionManager mgr(exec);
    mgr.set_durable_log(&log);
    // Threshold = 0 (default) means auto-compact is disabled.

    // Run several transactions
    for (int i = 0; i < 5; ++i) {
        ASSERT_TRUE(mgr.begin());
        ASSERT_TRUE(mgr.enlist_backend("b1"));
        ASSERT_TRUE(mgr.commit());
    }

    // No compaction happened, so file should contain all DECISION + COMPLETE records
    // (5 txns × 2 records each = 10 records, each ~50 bytes)
    struct stat st;
    ASSERT_EQ(::stat(log_path.c_str(), &st), 0);
    EXPECT_GT(st.st_size, 100);  // at least ~100 bytes for 10 records

    ::unlink(log_path.c_str());
}

TEST(DurableTxnLogAutoCompactTest, CounterResetsAfterCompact) {
    std::string log_path = "/tmp/test_auto_compact_reset_" +
                           std::to_string(::getpid()) + ".log";
    ::unlink(log_path.c_str());

    DurableTransactionLog log;
    ASSERT_TRUE(log.open(log_path));

    MockRemoteExecutor exec;
    exec.add_mock_backend("b1");
    DistributedTransactionManager mgr(exec);
    mgr.set_durable_log(&log);
    mgr.set_auto_compact_threshold(2);

    // First 2 commits → compact fires
    for (int i = 0; i < 2; ++i) {
        ASSERT_TRUE(mgr.begin());
        ASSERT_TRUE(mgr.enlist_backend("b1"));
        ASSERT_TRUE(mgr.commit());
    }

    // 3rd commit: counter should have reset, so no compact fires yet
    ASSERT_TRUE(mgr.begin());
    ASSERT_TRUE(mgr.enlist_backend("b1"));
    ASSERT_TRUE(mgr.commit());

    struct stat st1;
    ASSERT_EQ(::stat(log_path.c_str(), &st1), 0);
    off_t size_after_3 = st1.st_size;

    // 4th commit → counter hits 2 again, compact fires, size drops
    ASSERT_TRUE(mgr.begin());
    ASSERT_TRUE(mgr.enlist_backend("b1"));
    ASSERT_TRUE(mgr.commit());

    struct stat st2;
    ASSERT_EQ(::stat(log_path.c_str(), &st2), 0);
    EXPECT_LE(st2.st_size, size_after_3);  // compact should have reduced or preserved size

    ::unlink(log_path.c_str());
}
```

- [ ] **Run tests to verify they fail** (method `set_auto_compact_threshold` doesn't exist yet)

```bash
make build-tests 2>&1 | grep 'set_auto_compact_threshold'
```

Expected: compilation error `no member named 'set_auto_compact_threshold'`.

- [ ] **Commit**

```bash
git add tests/test_distributed_txn.cpp
git commit -m "test(wal): add failing tests for auto-compaction threshold"
```

### Step 1.2: Implement auto-compact in DistributedTransactionManager

- [ ] **Modify `include/sql_engine/distributed_txn.h`**

Add the threshold setter near the other config setters (after `set_phase_statement_timeout_ms` at line 75):

```cpp
    // Auto-compact the durable log every N successful completions. When > 0,
    // the manager calls txn_log_->compact() after every Nth completed txn.
    // 0 (default) = disabled. Callers should pick a value balancing compaction
    // cost vs. log growth (e.g., 100-10000 depending on txn rate).
    void set_auto_compact_threshold(uint32_t n) {
        auto_compact_threshold_ = n;
    }
```

Add the counter and threshold fields to the private section (near line 276 with `txn_log_`):

```cpp
    DurableTransactionLog* txn_log_ = nullptr;
    bool require_durable_log_ = false;
    uint32_t phase_statement_timeout_ms_ = 0;

    // Auto-compaction: count completions, fire compact when counter hits threshold.
    uint32_t auto_compact_threshold_ = 0;
    uint32_t completions_since_compact_ = 0;
```

Modify `maybe_log_complete()` (currently at line 328-330) to trigger compaction:

```cpp
    void maybe_log_complete() {
        if (!txn_log_) return;
        txn_log_->log_complete(txn_id_);
        if (auto_compact_threshold_ > 0) {
            ++completions_since_compact_;
            if (completions_since_compact_ >= auto_compact_threshold_) {
                completions_since_compact_ = 0;
                txn_log_->compact();
            }
        }
    }
```

- [ ] **Run tests to verify they pass**

```bash
make test 2>&1 | grep -E 'DurableTxnLogAutoCompact|PASSED|FAILED' | head -20
```

Expected: All 3 AutoCompact tests PASS, no regressions.

- [ ] **Commit**

```bash
git add include/sql_engine/distributed_txn.h
git commit -m "feat(wal): add auto-compaction threshold to DistributedTransactionManager"
```

### Step 1.3: Verify full test suite

- [ ] **Run full tests**

```bash
make clean && make test 2>&1 | grep -E '^\[  (PASSED|FAILED)'
```

Expected: All tests PASS, 0 failures.

---

## Task 2: PostgreSQL TIMESTAMPTZ Normalization

**Files:**
- Modify: `include/sql_engine/datetime_parse.h` (add declaration)
- Modify: `src/sql_engine/datetime_parse.cpp` (implement timezone-aware parse)
- Modify: `src/sql_engine/pgsql_remote_executor.cpp:215-225` (use new function)
- Modify: `tests/test_datetime_format.cpp` (add tests)

### Step 2.1: Write failing tests for timezone-aware parsing

- [ ] **Find the datetime_parse header location**

```bash
find /data/rene/ParserSQL -name 'datetime_parse*' -type f
```

Expected: `include/sql_engine/datetime_parse.h` and `src/sql_engine/datetime_parse.cpp`.

- [ ] **Add tests to `tests/test_datetime_format.cpp`** (near the other parse_datetime tests)

```cpp
TEST(DatetimeParseTimezoneTest, ParseDatetimeWithPositiveOffset) {
    // "2024-06-15 14:30:00+05:30" → normalized to UTC: 09:00:00 same day
    int64_t us = sql_engine::datetime_parse::parse_datetime_tz(
        "2024-06-15 14:30:00+05:30");
    // 09:00:00 UTC on 2024-06-15
    // days since 1970-01-01 for 2024-06-15 = 19889
    int64_t expected_us = 19889LL * 86400LL * 1000000LL
                        + 9LL * 3600LL * 1000000LL;
    EXPECT_EQ(us, expected_us);
}

TEST(DatetimeParseTimezoneTest, ParseDatetimeWithNegativeOffset) {
    // "2024-06-15 14:30:00-08:00" → normalized to UTC: 22:30:00 same day
    int64_t us = sql_engine::datetime_parse::parse_datetime_tz(
        "2024-06-15 14:30:00-08:00");
    int64_t expected_us = 19889LL * 86400LL * 1000000LL
                        + 22LL * 3600LL * 1000000LL
                        + 30LL * 60LL * 1000000LL;
    EXPECT_EQ(us, expected_us);
}

TEST(DatetimeParseTimezoneTest, ParseDatetimeWithZOffset) {
    // Postgres also uses 'Z' for UTC in ISO-8601
    int64_t us = sql_engine::datetime_parse::parse_datetime_tz(
        "2024-06-15 14:30:00Z");
    int64_t expected_us = 19889LL * 86400LL * 1000000LL
                        + 14LL * 3600LL * 1000000LL
                        + 30LL * 60LL * 1000000LL;
    EXPECT_EQ(us, expected_us);
}

TEST(DatetimeParseTimezoneTest, ParseDatetimeWithNoOffsetSameAsPlain) {
    // When no timezone is present, parse_datetime_tz returns the same as parse_datetime
    int64_t us_tz = sql_engine::datetime_parse::parse_datetime_tz(
        "2024-06-15 14:30:00");
    int64_t us_plain = sql_engine::datetime_parse::parse_datetime(
        "2024-06-15 14:30:00");
    EXPECT_EQ(us_tz, us_plain);
}

TEST(DatetimeParseTimezoneTest, ParseDatetimeWithZeroOffset) {
    // +00 or +00:00 or Z are all UTC
    int64_t us1 = sql_engine::datetime_parse::parse_datetime_tz(
        "2024-06-15 14:30:00+00:00");
    int64_t us2 = sql_engine::datetime_parse::parse_datetime_tz(
        "2024-06-15 14:30:00+00");
    int64_t us3 = sql_engine::datetime_parse::parse_datetime_tz(
        "2024-06-15 14:30:00Z");
    EXPECT_EQ(us1, us2);
    EXPECT_EQ(us2, us3);
}

TEST(DatetimeParseTimezoneTest, ParseDatetimeWithFractionalAndTZ) {
    // Fractional seconds followed by timezone offset
    int64_t us = sql_engine::datetime_parse::parse_datetime_tz(
        "2024-06-15 14:30:00.123456+05:30");
    // 09:00:00.123456 UTC
    int64_t expected_us = 19889LL * 86400LL * 1000000LL
                        + 9LL * 3600LL * 1000000LL
                        + 123456LL;
    EXPECT_EQ(us, expected_us);
}

TEST(DatetimeParseTimezoneTest, ParseDatetimeShortOffsetPostgres) {
    // Postgres sometimes returns just "+05" without minutes
    int64_t us = sql_engine::datetime_parse::parse_datetime_tz(
        "2024-06-15 14:30:00+05");
    // 09:30:00 UTC
    int64_t expected_us = 19889LL * 86400LL * 1000000LL
                        + 9LL * 3600LL * 1000000LL
                        + 30LL * 60LL * 1000000LL;
    EXPECT_EQ(us, expected_us);
}
```

- [ ] **Run tests to verify they fail**

```bash
make test 2>&1 | grep 'parse_datetime_tz'
```

Expected: compilation error — `parse_datetime_tz` undefined.

- [ ] **Commit**

```bash
git add tests/test_datetime_format.cpp
git commit -m "test(pgsql): add failing tests for timezone-aware datetime parsing"
```

### Step 2.2: Declare `parse_datetime_tz` in the header

- [ ] **Read current datetime_parse.h to understand the interface**

```bash
cat include/sql_engine/datetime_parse.h
```

- [ ] **Add declaration in `include/sql_engine/datetime_parse.h`**

Add near the existing `parse_datetime` declaration:

```cpp
// Parse a datetime string that MAY include a timezone offset.
// Accepts: "YYYY-MM-DD HH:MM:SS[.UUUUUU][+HH:MM|+HH|Z|-HH:MM|-HH]"
// If a timezone offset is present, normalizes the timestamp to UTC.
// If no timezone is present, behaves identically to parse_datetime().
// Returns microseconds since UTC epoch 1970-01-01 00:00:00.
int64_t parse_datetime_tz(const char* s);
```

- [ ] **Run to verify headers compile**

```bash
make lib 2>&1 | tail -5
```

Expected: compiles (no implementation yet — that's next).

### Step 2.3: Implement `parse_datetime_tz`

- [ ] **Add implementation to `src/sql_engine/datetime_parse.cpp`**

Add after the existing `parse_datetime()` function (after line 107):

```cpp
// Parse an optional timezone offset at position `s`. Updates `s` past the offset.
// Returns the offset in microseconds (positive for east of UTC, negative for west).
// Recognized formats: "Z", "+HH", "+HH:MM", "-HH", "-HH:MM" (and without colon).
// Returns 0 if no offset found.
static int64_t parse_tz_offset_us(const char*& s) {
    if (!s || !*s) return 0;
    if (*s == 'Z' || *s == 'z') {
        ++s;
        return 0;
    }
    if (*s != '+' && *s != '-') return 0;
    int sign = (*s == '-') ? -1 : 1;
    ++s;
    int hours = parse_int(s, 2);
    int minutes = 0;
    if (*s == ':') {
        ++s;
        minutes = parse_int(s, 2);
    } else if (*s >= '0' && *s <= '9') {
        minutes = parse_int(s, 2);
    }
    int64_t offset_us = (static_cast<int64_t>(hours) * 3600LL
                       + static_cast<int64_t>(minutes) * 60LL)
                      * 1000000LL;
    return sign * offset_us;
}

int64_t parse_datetime_tz(const char* s) {
    if (!s || !*s) return 0;
    const char* p = s;
    int year = parse_int(p, 4);
    if (*p == '-') ++p;
    int month = parse_int(p, 2);
    if (*p == '-') ++p;
    int day = parse_int(p, 2);
    if (*p == ' ' || *p == 'T') ++p;
    int hour = parse_int(p, 2);
    if (*p == ':') ++p;
    int minute = parse_int(p, 2);
    if (*p == ':') ++p;
    int second = parse_int(p, 2);
    int64_t frac = parse_frac_us(p);

    int32_t days = days_since_epoch(year, month, day);
    int64_t us = static_cast<int64_t>(days) * 86400LL * 1000000LL
               + static_cast<int64_t>(hour) * 3600LL * 1000000LL
               + static_cast<int64_t>(minute) * 60LL * 1000000LL
               + static_cast<int64_t>(second) * 1000000LL
               + frac;

    // Parse optional timezone offset and normalize to UTC.
    // "2024-06-15 14:30:00+05:30" → UTC 09:00:00: subtract the offset.
    int64_t tz_us = parse_tz_offset_us(p);
    return us - tz_us;
}
```

- [ ] **Run the new tests**

```bash
./run_tests --gtest_filter="*DatetimeParseTimezone*"
```

Expected: All 7 tests PASS.

- [ ] **Commit**

```bash
git add include/sql_engine/datetime_parse.h src/sql_engine/datetime_parse.cpp
git commit -m "feat(pgsql): add parse_datetime_tz with UTC normalization"
```

### Step 2.4: Use `parse_datetime_tz` in PgSQL TIMESTAMPTZ handling

- [ ] **Modify `src/sql_engine/pgsql_remote_executor.cpp` at line 219-225**

Current code:

```cpp
case TIMESTAMPTZOID: {
    // PostgreSQL returns "YYYY-MM-DD HH:MM:SS+TZ" -- parse the datetime
    // part, ignoring timezone for now (as per spec: timezone handling deferred).
    int64_t us = datetime_parse::parse_datetime(data);
    return value_timestamp(us);
}
```

Replace with:

```cpp
case TIMESTAMPTZOID: {
    // PostgreSQL returns "YYYY-MM-DD HH:MM:SS+TZ". Parse and normalize to UTC.
    int64_t us = datetime_parse::parse_datetime_tz(data);
    return value_timestamp(us);
}
```

- [ ] **Run full tests**

```bash
make test 2>&1 | grep -E '^\[  (PASSED|FAILED)'
```

Expected: All tests PASS, 0 failures.

- [ ] **Commit**

```bash
git add src/sql_engine/pgsql_remote_executor.cpp
git commit -m "feat(pgsql): normalize TIMESTAMPTZ values to UTC using parse_datetime_tz"
```

---

## Task 3: Run Corpus Test Suite

**Files:**
- Modify: `docs/benchmarks/latest.md` (update with fresh results)
- Potentially: parser code (if regressions found)

### Step 3.1: Run the corpus benchmark script

- [ ] **Run the benchmark script and capture the report**

```bash
cd /data/rene/ParserSQL
bash scripts/run_benchmarks.sh /tmp/corpus_fresh_report.md 2>&1 | tee /tmp/corpus_run.log
```

Expected: script completes. It clones corpora to `/tmp/sql_corpora/` (reuses existing if present), builds release, runs benchmarks, runs corpus tests, generates a markdown report.

This can take 5-10 minutes depending on network and CPU.

- [ ] **Inspect the fresh report**

```bash
cat /tmp/corpus_fresh_report.md | head -80
```

Look for:
- Total queries tested
- OK / PARTIAL / ERROR counts per corpus
- Error categories (which statement types fail most)
- Any new errors vs. the March baseline

### Step 3.2: Compare to March baseline

- [ ] **Read the March baseline**

```bash
cat /data/rene/ParserSQL/docs/benchmarks/latest.md | head -100
```

- [ ] **Diff key numbers**

Record these for each corpus:
- Total queries
- OK / PARTIAL / ERROR counts
- Success rate

If any corpus shows MORE errors than the baseline, this is a regression from recent work and needs investigation.

### Step 3.3: Investigate any regressions (conditional)

- [ ] **If regressions found, identify affected queries**

Look at the "Top ERROR examples" section of the report. For each regression:
1. Copy the SQL example
2. Parse it locally: `echo "THE SQL" | ./sqlengine --parse-only` (if this flag exists, otherwise via corpus_test)
3. Identify which parser commit introduced the regression: `git bisect` or inspection
4. If it's a clear parser bug introduced by the Task 5 changes in the previous plan (`* EXCEPT/REPLACE`), fix it. Otherwise, document as known limitation.

- [ ] **Fix regressions or document as known limitations**

Any fixes should:
- Include a test case in the appropriate `tests/test_*.cpp` file
- Use the failing SQL as the test input
- Be committed separately: `git commit -m "fix(parser): <description>"`

### Step 3.4: Update `docs/benchmarks/latest.md`

- [ ] **Copy the fresh report over the existing latest.md**

```bash
cp /tmp/corpus_fresh_report.md /data/rene/ParserSQL/docs/benchmarks/latest.md
```

- [ ] **Sanity check** — verify the file looks reasonable (has markdown tables, date is today)

```bash
head -20 /data/rene/ParserSQL/docs/benchmarks/latest.md
```

- [ ] **Commit**

```bash
git add docs/benchmarks/latest.md
git commit -m "chore: refresh benchmark and corpus test results"
```

### Step 3.5: Push all work

- [ ] **Run final full test suite**

```bash
make clean && make test 2>&1 | grep -E '^\[  (PASSED|FAILED)'
```

Expected: All tests PASS.

- [ ] **Push**

```bash
git push
```
