#!/bin/bash
# publish_report.sh — Run full benchmarks on physical server, commit report
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
REPORT_DIR="$PROJECT_DIR/docs/benchmarks"

cd "$PROJECT_DIR"
git pull --ff-only

mkdir -p "$REPORT_DIR"

# Generate report
"$SCRIPT_DIR/run_benchmarks.sh" "$REPORT_DIR/latest.md"

# Also save a dated copy
DATE=$(date +%Y-%m-%d)
cp "$REPORT_DIR/latest.md" "$REPORT_DIR/report-$DATE.md"

echo ""
echo "Reports saved to:"
echo "  $REPORT_DIR/latest.md"
echo "  $REPORT_DIR/report-$DATE.md"
echo ""
echo "To publish: git add docs/benchmarks/ && git commit -m 'bench: update performance report' && git push"
