# mdm_merge

Offline, multi-replica causal merging for device master data, in pure Python
(standard library only) on SQLite. No central node, no wall-clock/arrival-order
arbitration: replicas exchange signed checkpoint/delta packages and converge on
observed-causality semantics.

## Layout

```
mdm_merge/            # library + CLI (core.py, protocol.py, cli.py)
  subscriptions.py    # resumable downstream change-stream outlet (source side)
  landing.py          # independent SQLite landing end (reference consumer)
mdm                   # thin CLI launcher for the source replica (python3 -I ./mdm ...)
mdmland               # thin CLI launcher for the landing database
tests/
  demo_scenarios.py       # hand-written headline scenarios, real SQLite, new processes
  oracle.py               # independent causal-graph reference (no production calls)
  test_fuzz_vs_oracle.py  # randomized differential/convergence fuzzing
  test_protocol_validation.py
  test_crash_atomicity.py # real os._exit inside a merge, atomicity checks
  test_scale_bounded.py   # 50k -> 100k bounded-state + old-package replay
  test_subscription_flow.py       # hand-written change/relational event assertions
  test_subscription_consumers.py  # two-speed consumers, cut stability, isolation
  test_subscription_failures.py   # gaps/tamper/release/reclaim/reload rejects
  test_subscription_crash.py      # hard crashes on both ends, idempotent replay
  test_subscription_scale.py      # 100k-row bounded-memory/backlog/cleanup drill
samples/batch.example.json
scripts/run_all_tests.sh
PROTOCOL.md           # causal model, handoff wire protocol, identity/backup rules
SUBSCRIPTIONS.md      # change-stream + landing formats, cursors, lifecycle, CLI
```

## Quick start

```bash
python3 -I ./mdm init site_a.db --replica-id A
python3 -I ./mdm batch site_a.db samples/batch.example.json
python3 -I ./mdm checkpoint site_a.db Acme cp_acme.json
python3 -I ./mdm inspect cp_acme.json
python3 -I ./mdm get site_a.db Acme pump-001
python3 -I ./mdm stats site_a.db
```

### Resumable downstream subscription → independent landing database

```bash
# 1. one consistent cut point (works on an upgraded pre-existing database)
python3 -I ./mdm sub-create site_a.db Acme --page-size 500
# 2. paged initial load, landed and confirmed page by page
python3 -I ./mdm sub-load site_a.db <load-token> --out p0.json
python3 -I ./mdmland land-begin warehouse.db p0.json
python3 -I ./mdm sub-ack-load site_a.db p0.json
python3 -I ./mdmland land-complete warehouse.db Acme
python3 -I ./mdm sub-complete site_a.db <sub-id>
# 3. change transactions after the cut, applied then acknowledged
python3 -I ./mdm sub-events site_a.db <event-token> --out e0.json
python3 -I ./mdmland land-apply-events warehouse.db e0.json
python3 -I ./mdm sub-ack-events site_a.db e0.json
# 4. backlog, explicit release, bounded cleanup
python3 -I ./mdm sub-backlog site_a.db
python3 -I ./mdm sub-release site_a.db <sub-id>
python3 -I ./mdm sub-cleanup site_a.db
```

See `SUBSCRIPTIONS.md` for the full wire formats, cursor binding, retention
rules, reload-after-invalidation flow and every guarantee.

See `PROTOCOL.md` for the complete causal model, package formats, compaction
bounds, replica-identity/backup constraints and validation guarantees.

## Run all acceptance checks

```bash
bash scripts/run_all_tests.sh          # full 50k scalability run
bash scripts/run_all_tests.sh 4000     # faster smoke run
```

No third-party packages are required (CPython 3.9+ with built-in sqlite3).
