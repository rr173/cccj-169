"""Differential fuzz test: production SQLite replicas vs the naive oracle.

Random but seeded (deterministic).  Three replicas independently do legal
local batches and exchange checkpoints/deltas through shuffled, duplicated,
third-party-forwarded deliveries.  After every local operation and every
merge, every live (row, field) version identity of every production replica
must equal the reference causal-graph result.  Compaction is exercised
mid-stream; old packages are then replayed to prove summaries alone keep
deletions correct.
"""

from __future__ import annotations

import json
import os
import random
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from mdm_merge import Replica, protocol, MergeError  # noqa: E402
from oracle import OracleReplica, FIELDS         # noqa: E402

REPLICAS = ("A", "B", "C")
SUPPLIERS = ("S1", "S2")
ROWS = ("r1", "r2", "r3", "r4")
VALUES = ("v1", "v2", "v3", "same", None, "")


def rand_fields(rng):
    names = rng.sample(FIELDS, rng.randint(1, len(FIELDS)))
    return {f: rng.choice(VALUES) for f in names}


def live_map(rep: Replica):
    out = {}
    for supplier in SUPPLIERS:
        for row in rep.read_partition(supplier):
            for fld, info in row["fields"].items():
                if info["versions"]:
                    out[(supplier, row["asset_id"], fld)] = sorted(
                        (v["creator"], v["counter"], v["value"])
                        for v in info["versions"])
    return out


def oracle_map(orc: OracleReplica):
    snap = orc.snapshot()
    return {k: [tuple(v) for v in vs] for k, vs in snap.items()}


def assert_equal(rep, orc, phase):
    got, want = live_map(rep), oracle_map(orc)
    if got != want:
        only_got = {k: got[k] for k in got.keys() - want.keys()}
        only_want = {k: want[k] for k in want.keys() - got.keys()}
        diff = {k: (got.get(k), want.get(k))
                for k in got.keys() & want.keys() if got[k] != want[k]}
        raise AssertionError(
            f"divergence at {phase} on replica {rep.replica_id}\n"
            f" extra-in-prod={only_got}\n missing-in-prod={only_want}\n"
            f" mismatched={diff}")


def run(seed, iterations=400):
    rng = random.Random(seed)
    work = tempfile.mkdtemp(prefix=f"mdm-fuzz-{seed}-")
    try:
        reps, orcs = {}, {}
        # baselines[rid][supplier] = list of checkpoint dicts exported there
        baselines = {rid: {s: [] for s in SUPPLIERS} for rid in REPLICAS}
        pkg_dir = os.path.join(work, "packages")
        os.makedirs(pkg_dir, exist_ok=True)
        for rid in REPLICAS:
            reps[rid] = Replica.create(os.path.join(work, f"{rid}.db"), rid)
            orcs[rid] = OracleReplica(rid)

        def oracle_local(rid, ops):
            for op in ops:
                if op["op"] == "put":
                    orcs[rid].local_put(op["supplier"], op["asset_id"],
                                        op["fields"])
                else:
                    orcs[rid].local_erase(op["supplier"], op["asset_id"])

        def deliver(pkg, target, phase):
            # random direct delivery, third-party byte-copy forward, or
            # repeated delivery
            reps[target].merge_package(pkg)
            orcs[target].apply_handoff(pkg)
            if rng.random() < 0.35:                      # duplicate
                reps[target].merge_package(pkg)
                orcs[target].apply_handoff(pkg)
            assert_equal(reps[target], orcs[target], phase)

        for step in range(iterations):
            rid = rng.choice(REPLICAS)
            rep, orc = reps[rid], orcs[rid]
            choice = rng.random()

            if choice < 0.55:                         # local batch
                n_ops = rng.randint(1, 3)
                ops = []
                for _ in range(n_ops):
                    supplier = rng.choice(SUPPLIERS)
                    asset = rng.choice(ROWS)
                    if rng.random() < 0.25:
                        ops.append({"op": "erase", "supplier": supplier,
                                    "asset_id": asset})
                    else:
                        ops.append({"op": "put", "supplier": supplier,
                                    "asset_id": asset,
                                    "fields": rand_fields(rng)})
                rep.import_batch(ops)
                oracle_local(rid, ops)
                assert_equal(rep, orc, f"step {step} local batch @{rid}")

            elif choice < 0.8:                        # checkpoint exchange
                supplier = rng.choice(SUPPLIERS)
                path = os.path.join(pkg_dir, f"cp_{rid}_{supplier}_{step}.json")
                cid = rep.export_checkpoint(supplier, path)
                with open(path, encoding="utf-8") as fh:
                    pkg = json.load(fh)
                baselines[rid][supplier].append(pkg)
                # forward via an arbitrary third replica's file copy
                target = rng.choice([x for x in REPLICAS if x != rid])
                fwd = path + ".forwarded"
                shutil.copyfile(path, fwd)
                with open(fwd, encoding="utf-8") as fh:
                    fwd_pkg = json.load(fh)
                deliver(fwd_pkg, target, f"step {step} checkpoint {rid}->{target}")

            else:                                    # delta exchange
                supplier = rng.choice(SUPPLIERS)
                if not baselines[rid][supplier]:
                    continue
                base = rng.choice(baselines[rid][supplier])
                path = os.path.join(pkg_dir, f"dl_{rid}_{supplier}_{step}.json")
                try:
                    rep.export_delta(supplier, base, path)
                except MergeError:
                    # Baseline snapshot was pruned by compaction: a delta is
                    # no longer available for it, so exchange a fresh full
                    # checkpoint instead (always self-contained).
                    rep.export_checkpoint(supplier, path)
                with open(path, encoding="utf-8") as fh:
                    pkg = json.load(fh)
                if pkg["kind"] == "delta":
                    # Structural guarantee: every delta record is a real
                    # per-key change versus the baseline, and no record key
                    # repeats -- an unchanged partition yields an empty
                    # delta. A delta add whose counter equals the baseline is
                    # allowed only as the self-contained add fact of a kill.
                    base_coords = {(r["asset_id"], r["field"],
                                    r["creator"], r["counter"])
                                   for r in base["adds"]}
                    kill_key_set = {(r["asset_id"], r["field"],
                                     r["creator"]) for r in pkg["kills"]}
                    seen = set()
                    for r in pkg["adds"]:
                        k = (r["asset_id"], r["field"], r["creator"],
                             r["counter"])
                        assert k not in seen, "duplicate delta add"
                        seen.add(k)
                        if k in base_coords:
                            assert (r["asset_id"], r["field"],
                                    r["creator"]) in kill_key_set, \
                                "delta smuggles an unchanged live version"
                    seen_k = set()
                    for r in pkg["kills"]:
                        k = (r["asset_id"], r["field"], r["creator"],
                             r["counter"])
                        assert k not in seen_k, "duplicate delta kill"
                        seen_k.add(k)
                target = rng.choice([x for x in REPLICAS if x != rid])
                deliver(pkg, target, f"step {step} exchange {rid}->{target}")

            # periodic full mesh: shuffle all retained packages per target
            if step and step % 50 == 0:
                all_pkgs = []
                for rid2 in REPLICAS:
                    for s in SUPPLIERS:
                        all_pkgs.extend(baselines[rid2][s])
                rng.shuffle(all_pkgs)
                for pkg in all_pkgs:
                    target = rng.choice(REPLICAS)
                    deliver(pkg, target, f"step {step} mesh replay")

            # periodic compaction, then replay old packages out of order
            if step and step % 120 == 0:
                for rid2 in REPLICAS:
                    reps[rid2].compact()
                    orcs[rid2].compact()
                    assert_equal(reps[rid2], orcs[rid2],
                                 f"step {step} post-compaction @{rid2}")
                old = list(baselines[rng.choice(REPLICAS)][rng.choice(SUPPLIERS)])
                rng.shuffle(old)
                for pkg in old[:5]:
                    target = rng.choice(REPLICAS)
                    deliver(pkg, target,
                            f"step {step} old package after compaction")

        # Final convergence via each replica's *current* full checkpoint:
        # the only self-contained snapshot that carries every live add and
        # every (add, remove) tombstone pair. Replaying only random historical
        # packages is not a valid convergence test -- an old checkpoint
        # predates deletions the sender made later. Two gossip rounds of
        # current checkpoints suffice for full union convergence.
        current = []
        for rid2 in REPLICAS:
            for s in SUPPLIERS:
                path = os.path.join(work, "packages",
                                    f"final_{rid2}_{s}.json")
                reps[rid2].export_checkpoint(s, path)
                with open(path, encoding="utf-8") as fh:
                    current.append(json.load(fh))
        rng.shuffle(current)
        for round_no in range(2):
            for pkg in current:
                for rid2 in REPLICAS:
                    reps[rid2].merge_package(pkg)
                    orcs[rid2].apply_handoff(pkg)
        prod_maps = {rid2: live_map(reps[rid2]) for rid2 in REPLICAS}
        for rid2 in REPLICAS:
            assert_equal(reps[rid2], orcs[rid2], "final")
        if not (prod_maps[REPLICAS[0]] == prod_maps[REPLICAS[1]]
                == prod_maps[REPLICAS[2]]):
            raise AssertionError(
                f"production replicas failed to converge: "
                f"{ {r: len(m) for r, m in prod_maps.items()} }")
        return prod_maps[REPLICAS[0]]
    finally:
        shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    seeds = [int(x) for x in sys.argv[1:]] or [1, 2, 7, 42, 99]
    total = 0
    for seed in seeds:
        ref = run(seed)
        total += sum(len(v) for v in ref.values())
        print(f"[PASS] fuzz seed={seed}: converged, "
              f"{len(ref)} live (row,field) keys")
    print(f"All differential fuzz runs matched the causal oracle "
          f"({len(seeds)} seeds).")
