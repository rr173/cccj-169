# mdm_merge

Offline, multi-replica causal merging for device master data, in pure Python
(standard library only) on SQLite. No central node, no wall-clock/arrival-order
arbitration: replicas exchange signed checkpoint/delta packages and converge on
observed-causality semantics.

## Layout

```
mdm_merge/            # library + CLI (core.py, protocol.py, cli.py)
mdm                   # thin CLI launcher (use: python3 -I ./mdm ...)
tests/
  demo_scenarios.py       # hand-written headline scenarios, real SQLite, new processes
  oracle.py               # independent causal-graph reference (no production calls)
  test_fuzz_vs_oracle.py  # randomized differential/convergence fuzzing
  test_crash_atomicity.py # real os._exit inside a merge, atomicity checks
  test_scale_bounded.py   # 50k -> 100k bounded-state + old-package replay
samples/batch.example.json
scripts/run_all_tests.sh
PROTOCOL.md           # full semantics, wire protocol, identity/backup rules
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

See `PROTOCOL.md` for the complete causal model, package formats, compaction
bounds, replica-identity/backup constraints and validation guarantees.

## Run all acceptance checks

```bash
bash scripts/run_all_tests.sh          # full 50k scalability run
bash scripts/run_all_tests.sh 4000     # faster smoke run
```

No third-party packages are required (CPython 3.9+ with built-in sqlite3).
