#!/usr/bin/env bash
# Reproduce every acceptance check for mdm_merge in a clean stdlib-only setup.
# Run from the repository root:
#   bash scripts/run_all_tests.sh [SCALE_OPS] [SCALE_ROWS]
# SCALE_OPS   controls the legacy bounded-state merge test (default 50000)
# SCALE_ROWS  controls the 100k subscription memory drill (default 100000)
set -euo pipefail

cd "$(dirname "$0")/.."
SCALE_OPS="${1:-50000}"
SCALE_ROWS="${2:-100000}"

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
echo "== 6. subscription outlet: hand-written event/relational expectations =="
python3 -I tests/test_subscription_flow.py

echo
echo "== 7. two-speed consumers, cut stability, partition isolation, compact =="
python3 -I tests/test_subscription_consumers.py

echo
echo "== 8. negative paths: gaps, tamper, release, reclaim, reload =="
python3 -I tests/test_subscription_failures.py

echo
echo "== 9. hard crashes on source event write and landing apply/ack =="
python3 -I tests/test_subscription_crash.py

echo
echo "== 10. bounded-memory drill: ${SCALE_ROWS}-row partition, small pages =="
python3 -I tests/test_subscription_scale.py "${SCALE_ROWS}" 500

echo
echo "All acceptance checks completed."
