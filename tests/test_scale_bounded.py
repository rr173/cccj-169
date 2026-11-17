"""Steady-state scalability test.

Three real SQLite replicas repeatedly overwrite/erase a fixed small set of
rows while staying converged. The requirement under test is that durable
state is bounded by *contacted keys / participating replicas / unresolved
causal gaps*, never by the total number of operations:

* run N converged operations, compact, reopen, count logical entries and
  measure reclaimed on-disk bytes and wall time;
* run another N operations (2N total), compact again, and show the logical
  entry counts and disk usage do not double;
* finally deliver the oldest checkpoint produced during the run and prove
  no superseded/deleted value is resurrected.

All large test data is generated at runtime; nothing large is committed.
"""

from __future__ import annotations

import json
import os
import random
import shutil
import sys
import tempfile
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from mdm_merge import Replica, protocol           # noqa: E402

REPLICAS = ("A", "B", "C")
SUPPLIER = "SCALE"
ROWS = tuple(f"asset{i}" for i in range(8))
FIELDS = ("location", "model", "category")


def logical_entries(rep: Replica) -> int:
    s = rep.stats()
    # Entries that must remain bounded independent of operation count.
    return (s["field_state_entries"] + s["watermark_entries"]
            + s["frontier_entries"] + s["pending_kill_entries"]
            + s["checkpoint_summaries"])


def converge(rep_paths, work, rng):
    """One gossip round of current full checkpoints, shuffled per target."""
    cps = []
    for rid, path in rep_paths.items():
        out = os.path.join(work, f"cp_{rid}_{SUPPLIER}.json")
        with Replica(path) as rep:
            rep.export_checkpoint(SUPPLIER, out)
        cps.append(out)
    for rid, path in rep_paths.items():
        order = cps[:]
        rng.shuffle(order)
        with Replica(path) as rep:
            for cp in order:
                rep.merge_file(cp)


def run_phase(rep_paths, work, ops, rng, oldest_pkg_holder, start_counter=0):
    """Issue `ops` local batches across replicas, syncing periodically."""
    local_count = 0
    sync_every = 1000
    while local_count < ops:
        rid = rng.choice(REPLICAS)
        path = rep_paths[rid]
        supplier = SUPPLIER
        asset = rng.choice(ROWS)
        batch = []
        if rng.random() < 0.3:
            batch.append({"op": "erase", "supplier": supplier,
                          "asset_id": asset})
        else:
            fields = {f: rng.choice(("x", "y", "z", None))
                      for f in rng.sample(FIELDS, rng.randint(1, 3))}
            batch.append({"op": "put", "supplier": supplier,
                          "asset_id": asset, "fields": fields})
        with Replica(path) as rep:
            rep.import_batch(batch)
            if local_count == 0 and not oldest_pkg_holder["path"]:
                # snapshot the very first state as the "earliest package"
                out = os.path.join(work, "earliest.json")
                rep.export_checkpoint(SUPPLIER, out)
                oldest_pkg_holder["path"] = out
        local_count += 1
        if local_count % sync_every == 0:
            converge(rep_paths, work, rng)
    converge(rep_paths, work, rng)


def measure(rep_paths, label, rng):
    print(f"\n--- {label} ---")
    totals = {}
    for rid, path in rep_paths.items():
        with Replica(path) as rep:
            t0 = time.perf_counter()
            removed = rep.compact()
            compact_s = time.perf_counter() - t0
            s = rep.stats()
        # reopen from disk to prove persistence independent of the process
        with Replica(path) as rep:
            s2 = rep.stats()
        assert s == s2, "stats changed after reopen without writes"
        totals[rid] = {"logical": logical_entries(rep) if False else
                       sum((s2["field_state_entries"],
                            s2["watermark_entries"],
                            s2["frontier_entries"],
                            s2["pending_kill_entries"],
                            s2["checkpoint_summaries"])),
                       "db_bytes": s2["db_bytes"],
                       "compact_s": round(compact_s, 4),
                       "removed": removed,
                       "stats": s2}
        print(f"  {rid}: logical_entries={totals[rid]['logical']:6d} "
              f"db_bytes={s2['db_bytes']:8d} compact={compact_s:.3f}s "
              f"live={s2['live_versions']} pending={s2['pending_kill_entries']}")
    return totals


def main():
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 50_000
    work = tempfile.mkdtemp(prefix="mdm-scale-")
    rng = random.Random(20261117)
    rep_paths = {rid: os.path.join(work, f"{rid}.db") for rid in REPLICAS}
    for rid, path in rep_paths.items():
        Replica.create(path, rid)
    oldest = {"path": None}
    failures = []

    def check(label, cond):
        print(f"  [{'PASS' if cond else 'FAIL'}] {label}")
        if not cond:
            failures.append(label)

    run_phase(rep_paths, work, n, rng, oldest)
    first = measure(rep_paths, f"after {n:,} converged ops", rng)

    # bounded-by-keys sanity: fixed 8 rows x 3 fields x 3 replicas
    key_cap = len(ROWS) * len(FIELDS) * len(REPLICAS)
    for rid, m in first.items():
        check(f"{rid}: entries ~ key space ({key_cap}), not op count",
              m["logical"] <= key_cap * 4 + len(REPLICAS) + len(REPLICAS))

    run_phase(rep_paths, work, n, rng, oldest)   # 2N total
    second = measure(rep_paths, f"after {2*n:,} converged ops (2x work)", rng)

    print("\n--- growth check (N -> 2x operations) ---")
    for rid in REPLICAS:
        g = second[rid]["logical"] / max(first[rid]["logical"], 1)
        b = second[rid]["db_bytes"] / max(first[rid]["db_bytes"], 1)
        print(f"  {rid}: logical ratio={g:.2f}  disk ratio={b:.2f}")
        # The headline requirement: logical state must never scale with the
        # operation count.
        check(f"{rid}: logical entries did NOT double (ratio {g:.2f} < 1.5)",
              g < 1.5)

    # Third equal phase: per-key blobs are now capped by _blob_keep and every
    # table is key-bounded, so further work must plateau on disk too. This is
    # what proves we do not keep a full event ledger.
    run_phase(rep_paths, work, n, rng, oldest)   # 3N total
    third = measure(rep_paths, f"after {3*n:,} converged ops (3x work)", rng)

    print("\n--- plateau check (2x -> 3x operations, blobs capped) ---")
    for rid in REPLICAS:
        g = third[rid]["logical"] / max(second[rid]["logical"], 1)
        b = third[rid]["db_bytes"] / max(second[rid]["db_bytes"], 1)
        print(f"  {rid}: logical ratio={g:.2f}  disk ratio={b:.2f}")
        check(f"{rid}: logical entries plateaued (ratio {g:.2f} < 1.25)",
              g < 1.25)
        check(f"{rid}: disk usage plateaued (ratio {b:.2f} < 1.25)",
              b < 1.25)

    # oldest package must not resurrect anything on any converged replica
    print("\n--- oldest package resurrection check ---")
    before = {}
    for rid, path in rep_paths.items():
        with Replica(path) as rep:
            before[rid] = json.dumps(rep.read_partition(SUPPLIER),
                                     sort_keys=True)
            rep.merge_file(oldest["path"])          # replay earliest cp
    for rid, path in rep_paths.items():
        with Replica(path) as rep:
            after = json.dumps(rep.read_partition(SUPPLIER), sort_keys=True)
        check(f"{rid}: earliest checkpoint resurrected nothing",
              after == before[rid])

    # show which quantities can still grow under disorder
    print("\nGrowth semantics:")
    print("  * field_state/dead_watermark: bounded by touched "
          "(row,field,creator) keys")
    print("  * pending_kills: grows only with unresolved out-of-order causal "
          "gaps; drains to zero once the matching adds arrive")
    print("  * replica_frontier/checkpoints: bounded by participating "
          "replicas x suppliers")
    print(f"  work dir: {work}")

    if failures:
        print(f"\n{len(failures)} SCALE FAILURE(S)")
        sys.exit(1)
    print("\nScalability: state stayed key-bounded across 2x operations and "
          "the oldest package resurrected nothing.")


if __name__ == "__main__":
    main()
