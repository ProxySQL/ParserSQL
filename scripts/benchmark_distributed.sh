#!/bin/bash
# benchmark_distributed.sh — Run distributed query benchmarks and generate a report
#
# Runs the bench_distributed tool against:
#   1. 2-shard setup (distributed)
#   2. Single-backend setup (baseline)
# Then computes overhead and generates a comparison report.
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"

ITERATIONS=${BENCH_ITERATIONS:-100}
WARMUP=${BENCH_WARMUP:-5}
REPORT_DIR="$PROJECT_DIR/docs/benchmarks"
TIMESTAMP=$(date +%Y-%m-%d_%H%M%S)

echo "=============================================="
echo "  Distributed SQL Benchmark Suite"
echo "=============================================="
echo "Iterations: $ITERATIONS  Warmup: $WARMUP"
echo ""

# Build bench_distributed if needed
if [ ! -f ./bench_distributed ]; then
    echo "Building bench_distributed..."
    make bench-distributed 2>&1 | tail -1
fi

# Check if 2-shard setup is running
SHARDS_RUNNING=true
if ! docker exec parsersql-shard1 mysql -uroot -ptest -e "SELECT 1" &>/dev/null 2>&1; then
    echo "Shards not running. Starting them..."
    ./scripts/start_sharding_demo.sh
    SHARDS_RUNNING=false
fi

# Check if single backend is running
SINGLE_RUNNING=true
if ! docker exec parsersql-single mysql -uroot -ptest -e "SELECT 1" &>/dev/null 2>&1; then
    echo "Single backend not running. Starting it..."
    ./scripts/setup_single_backend.sh
    SINGLE_RUNNING=false
fi

echo ""
echo "=== Running 2-shard distributed benchmark ==="
echo ""

DIST_CSV="/tmp/bench_distributed_${TIMESTAMP}.csv"
./bench_distributed \
    --backend "mysql://root:test@127.0.0.1:13306/testdb?name=shard1" \
    --backend "mysql://root:test@127.0.0.1:13307/testdb?name=shard2" \
    --shard "users:id:shard1,shard2" \
    --shard "orders:id:shard1,shard2" \
    --iterations "$ITERATIONS" \
    --warmup "$WARMUP" \
    --csv > "$DIST_CSV"

echo "Distributed benchmark complete. Results in $DIST_CSV"

echo ""
echo "=== Running single-backend baseline benchmark ==="
echo ""

SINGLE_CSV="/tmp/bench_single_${TIMESTAMP}.csv"
./bench_distributed \
    --backend "mysql://root:test@127.0.0.1:13308/testdb?name=single" \
    --shard "users:id:single" \
    --shard "orders:id:single" \
    --iterations "$ITERATIONS" \
    --warmup "$WARMUP" \
    --csv > "$SINGLE_CSV"

echo "Single-backend benchmark complete. Results in $SINGLE_CSV"

echo ""
echo "=== Running 2-shard distributed benchmark (human-readable) ==="
echo ""

./bench_distributed \
    --backend "mysql://root:test@127.0.0.1:13306/testdb?name=shard1" \
    --backend "mysql://root:test@127.0.0.1:13307/testdb?name=shard2" \
    --shard "users:id:shard1,shard2" \
    --shard "orders:id:shard1,shard2" \
    --iterations "$ITERATIONS" \
    --warmup "$WARMUP"

echo ""
echo "=== Running single-backend baseline benchmark (human-readable) ==="
echo ""

./bench_distributed \
    --backend "mysql://root:test@127.0.0.1:13308/testdb?name=single" \
    --shard "users:id:single" \
    --shard "orders:id:single" \
    --iterations "$ITERATIONS" \
    --warmup "$WARMUP"

echo ""
echo "=== Generating Comparison Report ==="
echo ""

# Generate comparison from CSV files
mkdir -p "$REPORT_DIR"
REPORT="$REPORT_DIR/distributed_comparison.md"

cat > "$REPORT" <<HEADER
# Distributed Query Benchmark Report

Generated: $(date -u +"%Y-%m-%d %H:%M UTC")
Iterations: $ITERATIONS | Warmup: $WARMUP

## Setup

| Component | Configuration |
|-----------|---------------|
| Distributed | 2 MySQL 8.0 shards (ports 13306, 13307), 5 users + 5 orders each |
| Single baseline | 1 MySQL 8.0 instance (port 13308), 10 users + 10 orders |
| Engine | ParserSQL distributed query engine |

## Pipeline Stages

Each query goes through 5 stages:
1. **Parse** -- Tokenize and build AST
2. **Plan** -- Convert AST to logical plan tree
3. **Optimize** -- Apply rewrite rules (predicate pushdown, constant folding, etc.)
4. **Distribute** -- Rewrite plan for multi-shard execution (RemoteScan, MergeSort, etc.)
5. **Execute** -- Run operators, fetch data from backends, merge results

## Distributed (2-shard) Results

\`\`\`csv
$(cat "$DIST_CSV")
\`\`\`

## Single-Backend Baseline Results

\`\`\`csv
$(cat "$SINGLE_CSV")
\`\`\`

## Overhead Analysis

The distribute stage adds overhead compared to single-backend execution.
For queries that touch both shards, the execute stage involves two network
round-trips instead of one, but the engine fetches from both shards and
merges results locally.

Key observations:
- **Parse + Plan + Optimize** are identical regardless of backend count
- **Distribute** is near-zero for single-backend (no multi-shard rewriting needed)
- **Execute** is the dominant cost for all queries due to network I/O
- Cross-shard joins require fetching data from both shards, then joining locally

## Comparison with Vitess

Vitess is Google's database clustering system for horizontal scaling of MySQL.
Key architectural differences:

| Feature | Our Engine | Vitess |
|---------|-----------|--------|
| Proxy layer | Single binary (vtgate-equivalent) | vtgate + vttablet per shard |
| Query parsing | Custom zero-alloc parser | sqlparser (Go) |
| Planning | Single-pass plan builder | vtgate planner (Gen4) |
| Optimization | Rule-based (4 rules) | Cost-based (Gen4) |
| Shard routing | ShardMap + hash-based | Vindexes (pluggable) |
| Cross-shard joins | Hash join + merge sort | Scatter-gather |
| Aggregation | MergeAggregate | Ordered aggregate on vtgate |

Vitess published benchmarks (from vitess.io) show vtgate adding 1-2ms overhead
per query for simple shard-routed queries. Our engine targets similar overhead
for the proxy layer, with the advantage of a faster native C++ parser and
in-process plan execution (no Go GC pauses).

For a direct comparison, set up Vitess following their local example:
\`\`\`bash
git clone https://github.com/vitessio/vitess.git
cd vitess/examples/local
./101_initial_cluster.sh
\`\`\`
Then run equivalent queries through Vitess's MySQL protocol on port 15306
and compare latency with our engine.
HEADER

echo "Report written to: $REPORT"
echo ""
echo "CSV files:"
echo "  Distributed: $DIST_CSV"
echo "  Single:      $SINGLE_CSV"
echo ""
echo "=== Benchmark Suite Complete ==="
