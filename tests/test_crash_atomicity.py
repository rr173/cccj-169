"""Hard-crash atomicity test.

A merge subprocess is killed with os._exit() at a deterministic point inside
the uncommitted merge transaction. After reopening the same SQLite file we
verify:

* no business value is visible without its matching causal summary
  (field_state liveness, watermarks and buffered tombstones move together);
* the database is intact and the exact same complete package can be merged
  again to reach the correct final state;
* a crash before any write leaves the pre-merge state untouched.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from mdm_merge import Replica, build_handoff        # noqa: E402
from mdm_merge import protocol                      # noqa: E402


def checkpoint(adds, kills, cid, sender="A", supplier="S"):
    return build_handoff(
        kind="checkpoint", replica_id=sender, supplier=supplier,
        frontier={sender: max([a["counter"] for a in adds], default=0)},
        adds=adds, kills=kills, checkpoint_id=cid)


def crash_merge(db_path, pkg_path, crash_at):
    code = (
        "import sys, os; sys.path.insert(0, %r);"
        "from mdm_merge import Replica, protocol;"
        "rep=Replica(%r);"
        "pkg=protocol.load_handoff_file(%r);"
        "rep.merge_package(pkg)"
        % (ROOT, db_path, pkg_path))
    env = dict(os.environ, MDM_CRASH_AT=str(crash_at))
    p = subprocess.run([sys.executable, "-I", "-c", code],
                       capture_output=True, text=True, env=env)
    return p


def main():
    work = tempfile.mkdtemp(prefix="mdm-crash-")
    db = os.path.join(work, "crash.db")
    rep = Replica.create(db, "R")

    pkg = checkpoint(
        adds=[{"supplier": "S", "asset_id": "r", "field": "location",
               "creator": "A", "counter": 1, "value": "L0"},
              {"supplier": "S", "asset_id": "r", "field": "model",
               "creator": "A", "counter": 1, "value": "M0"}],
        kills=[], cid="cp-1")
    rep.merge_package(pkg)
    before = rep.stats()
    rep.close()

    # A package that adds two versions and removes one (self-contained pair),
    # giving the merge several logical steps to crash inside.
    crash_pkg = checkpoint(
        adds=[{"supplier": "S", "asset_id": "r", "field": "location",
               "creator": "A", "counter": 2, "value": "L1"},
              {"supplier": "S", "asset_id": "r", "field": "location",
               "creator": "B", "counter": 1, "value": "LB"},
              # backing add fact for the remove
              {"supplier": "S", "asset_id": "r", "field": "model",
               "creator": "A", "counter": 1, "value": "M0"}],
        kills=[{"supplier": "S", "asset_id": "r", "field": "model",
                "creator": "A", "counter": 1}],
        cid="cp-2", sender="B")
    pkg_path = os.path.join(work, "crash_pkg.json")
    protocol.write_json_atomic(pkg_path, crash_pkg)

    failures = []

    def check(label, cond):
        print(f"  [{'PASS' if cond else 'FAIL'}] {label}")
        if not cond:
            failures.append(label)

    n_steps = len(crash_pkg["adds"]) + len(crash_pkg["kills"])
    for crash_at in range(1, n_steps + 1):
        # fresh copy of the pre-crash database for each attempt
        attempt_db = os.path.join(work, f"attempt_{crash_at}.db")
        subprocess.run(["cp", db, attempt_db], check=True)
        p = crash_merge(attempt_db, pkg_path, crash_at)
        check(f"crash_at={crash_at}: process exited hard (code 37)",
              p.returncode == 37)

        # reopen: the crashed transaction must be fully rolled back
        r2 = Replica(attempt_db)
        st = r2.stats()
        row = r2.read_row("S", "r")
        loc = [(v["creator"], v["counter"])
               for v in row["fields"]["location"]["versions"]]
        model = [(v["creator"], v["counter"])
                 for v in row["fields"]["model"]["versions"]]
        check(f"crash_at={crash_at}: no partial business values "
              "(only pre-crash L0/M0 visible)",
              loc == [("A", 1)] and model == [("A", 1)])
        check(f"crash_at={crash_at}: no partial causal summary "
              "(no orphan buffered kills, no new watermarks)",
              st["pending_kill_entries"] == before["pending_kill_entries"]
              and st["watermark_entries"] == before["watermark_entries"]
              and st["live_versions"] == before["live_versions"])
        r2.close()

        # resend the SAME complete package: must merge cleanly now
        r3 = Replica(attempt_db)
        r3.merge_file(pkg_path)
        row = r3.read_row("S", "r")
        loc = sorted((v["creator"], v["counter"], v["value"])
                     for v in row["fields"]["location"]["versions"])
        model = row["fields"]["model"]["versions"]
        check(f"crash_at={crash_at}: re-merge converges to L1 + LB, "
              "M0 removed",
              loc == [("A", 2, "L1"), ("B", 1, "LB")] and model == [])
        r3.close()

    # integrity check on every database file
    for f in sorted(os.listdir(work)):
        if f.endswith(".db"):
            import sqlite3
            c = sqlite3.connect(os.path.join(work, f))
            ok = c.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
            check(f"{f}: SQLite integrity_check ok", ok)
            c.close()

    if failures:
        print(f"\n{len(failures)} CRASH TEST FAILURE(S)")
        sys.exit(1)
    print("\nCrash atomicity: all hard-kill attempts rolled back cleanly and "
          "re-merged to the correct state.")


if __name__ == "__main__":
    main()
