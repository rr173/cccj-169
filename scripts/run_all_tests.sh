#!/usr/bin/env bash
# Reproduce every acceptance check for mdm_merge in a clean stdlib-only setup.
# Run from the repository root:  bash scripts/run_all_tests.sh [SCALE_OPS]
set -euo pipefail

cd "$(dirname "$0")/.."
SCALE_OPS="${1:-50000}"

echo "== 1. hand-written scenarios (3 real SQLite replicas, fresh processes) =="
python3 -I tests/demo_scenarios.py

echo
echo "== 2. differential fuzz vs independent causal-graph oracle =="
python3 -I tests/test_fuzz_vs_oracle.py

echo
echo "== 3. protocol validation (reject malformed/tampered input wholesale) =="
python3 -I tests/test_protocol_validation.py

echo
echo "== 4. hard-crash atomicity (real os._exit inside a merge) =="
python3 -I tests/test_crash_atomicity.py

echo
echo "== 5. bounded-state scalability: ${SCALE_OPS} ops x3 phases + old replay =="
python3 -I tests/test_scale_bounded.py "${SCALE_OPS}"

echo
echo "All acceptance checks completed."
