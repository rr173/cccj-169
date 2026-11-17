"""Hand-written headline scenarios, executed against three real SQLite
replicas through the CLI in fresh OS processes.

Every assertion states the expected *source identity* (replica, counter) of
each live version -- never just a value -- so same-text/different-origin
situations cannot pass by accident.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MDM = os.path.join(ROOT, "mdm")
FAILURES = []


def run(*args, check=True):
    p = subprocess.run([sys.executable, "-I", MDM, *args],
                       capture_output=True, text=True)
    if check and p.returncode != 0:
        raise AssertionError(f"command failed: {args}\n{p.stderr}")
    return p.returncode, json.loads(p.stdout) if p.stdout.strip() else {}


def write_batch(d, name, ops):
    path = os.path.join(d, name)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump({"format": "mdm-batch/v1", "protocol_version": 1,
                   "ops": ops}, fh)
    return path


def check(label, cond, detail=""):
    tag = "PASS" if cond else "FAIL"
    print(f"  [{tag}] {label}" + (f" -- {detail}" if detail and not cond else ""))
    if not cond:
        FAILURES.append(label)


def live(row, field):
    return [(v["creator"], v["counter"], v["value"])
            for v in row["fields"][field]["versions"]]


def expect(d, db, supplier, asset, field, want, label):
    _, row = run("get", os.path.join(d, db), supplier, asset)
    got = live(row, field)
    check(label, got == want, f"got={got} want={want}")
    return row


def absent(d, db, supplier, asset, label):
    _, row = run("get", os.path.join(d, db), supplier, asset)
    check(label, row["exists"] is False)


# ---------------------------------------------------------------------------
def scenario_erase_vs_unseen_put(d):
    print("\n=== Scenario 1: A erase vs B's unseen put -> only B's L1 ===")
    run("init", f"{d}/a.db", "--replica-id", "A")
    run("init", f"{d}/b.db", "--replica-id", "B")
    run("init", f"{d}/c.db", "--replica-id", "C")

    b = write_batch(d, "a_put.json", [
        {"op": "put", "supplier": "S", "asset_id": "r1",
         "fields": {"location": "L0"}}])
    run("batch", f"{d}/a.db", b)
    expect(d, "a.db", "S", "r1", "location", [("A", 1, "L0")],
           "A.1: A has (A,1)=L0")
    run("checkpoint", f"{d}/a.db", "S", f"{d}/cp_a1.json")

    # B receives checkpoint: L0 present
    run("merge", f"{d}/b.db", f"{d}/cp_a1.json")
    expect(d, "b.db", "S", "r1", "location", [("A", 1, "L0")],
           "A.2: B receives (A,1)=L0")

    # A erases locally (has observed L0)
    b = write_batch(d, "a_erase.json", [
        {"op": "erase", "supplier": "S", "asset_id": "r1"}])
    run("batch", f"{d}/a.db", b)
    absent(d, "a.db", "S", "r1", "A.3: row gone at A after erase")

    # Pure delete delta exported immediately: at this moment A's only change
    # since cp_a1 is the erase, so the delta is kill-only.
    run("delta", f"{d}/a.db", "S", f"{d}/cp_a1.json", f"{d}/dl_erase.json")
    _, dl0 = run("inspect", f"{d}/dl_erase.json")
    check("A.3b: erase delta is change-scoped (one tombstone kill) and "
          "self-contained, carrying only its own backing add fact",
          dl0["kill_records"] == 1 and dl0["add_records"] <= 1)

    # B writes L1 without having seen the erase (concurrent with it).
    # B HAS seen L0, so its ordinary put replaces L0 locally -- the erase
    # racing with this put is the unseen event, not L0.
    b = write_batch(d, "b_put.json", [
        {"op": "put", "supplier": "S", "asset_id": "r1",
         "fields": {"location": "L1"}}])
    run("batch", f"{d}/b.db", b)
    expect(d, "b.db", "S", "r1", "location", [("B", 1, "L1")],
           "A.4: B's put replaces the L0 it observed; L1 is the version "
           "concurrent with A's unseen erase")

    # A exports: checkpoint carries kill of (A,1), no live adds for r1 loc
    run("checkpoint", f"{d}/a.db", "S", f"{d}/cp_a2.json")
    # B exports checkpoint carrying (B,1)
    run("checkpoint", f"{d}/b.db", "S", f"{d}/cp_b1.json")

    # Cross-merge both directions
    run("merge", f"{d}/a.db", f"{d}/cp_b1.json")
    run("merge", f"{d}/b.db", f"{d}/cp_a2.json")
    expect(d, "a.db", "S", "r1", "location", [("B", 1, "L1")],
           "A.5: A ends with only (B,1)=L1, not empty, not L0")
    expect(d, "b.db", "S", "r1", "location", [("B", 1, "L1")],
           "A.6: B's L0 killed by A's erase; L1 survives")

    # C: delete-delta first (no baseline!), oldest checkpoint second.
    # dl_erase.json is the pure kill-only delta exported right after erase.
    _, dl = run("inspect", f"{d}/dl_erase.json")
    check("A.7: delta carries the L0 deletion tombstone (kill present)",
          dl["kill_records"] >= 1)
    check("A.7b: delta is change-scoped, not a whole-partition checkpoint "
          "(every add is either a live change or a tombstone's backing add; "
          "no unrelated rows are smuggled in)",
          dl["add_records"] <= dl["kill_records"] + 1
          and dl["kind"] == "delta")
    run("merge", f"{d}/c.db", f"{d}/dl_erase.json")
    _, crow = run("get", f"{d}/c.db", "S", "r1")
    check("A.8: delete-only tombstone applies with no prior baseline -- "
          "no row exists at C", crow["exists"] is False)
    run("merge", f"{d}/c.db", f"{d}/cp_a1.json")  # the OLD checkpoint
    _, crow = run("get", f"{d}/c.db", "S", "r1")
    check("A.9: oldest checkpoint arriving after the delete cannot bring L0 "
          "back", crow["exists"] is False)
    # repeat with a second fresh replica to make ordering irrelevant
    run("init", f"{d}/c2.db", "--replica-id", "C2")
    run("merge", f"{d}/c2.db", f"{d}/dl_erase.json")
    run("merge", f"{d}/c2.db", f"{d}/cp_a1.json")
    _, c2 = run("get", f"{d}/c2.db", "S", "r1")
    check("A.9b: old L0 never resurrects on a second replica either",
          c2["exists"] is False)
    # and the genuinely newer L1 is still accepted afterward
    run("merge", f"{d}/c.db", f"{d}/cp_b1.json")
    expect(d, "c.db", "S", "r1", "location", [("B", 1, "L1")],
           "A.10: C then accepts the genuinely newer L1")


def scenario_model_conflict(d):
    print("\n=== Scenario 2: M1/M2 conflict, resolve to M3, unseen M4 kept ===")
    run("init", f"{d}/m_a.db", "--replica-id", "A")
    run("init", f"{d}/m_b.db", "--replica-id", "B")
    b = write_batch(d, "base.json", [
        {"op": "put", "supplier": "S", "asset_id": "r2",
         "fields": {"model": "M0"}}])
    run("batch", f"{d}/m_a.db", b)
    run("checkpoint", f"{d}/m_a.db", "S", f"{d}/m_base.json")
    run("merge", f"{d}/m_b.db", f"{d}/m_base.json")

    b = write_batch(d, "a_m1.json", [
        {"op": "put", "supplier": "S", "asset_id": "r2",
         "fields": {"model": "M1"}}])
    run("batch", f"{d}/m_a.db", b)
    b = write_batch(d, "b_m2.json", [
        {"op": "put", "supplier": "S", "asset_id": "r2",
         "fields": {"model": "M2"}}])
    run("batch", f"{d}/m_b.db", b)
    run("checkpoint", f"{d}/m_a.db", "S", f"{d}/m_cp_a.json")
    run("checkpoint", f"{d}/m_b.db", "S", f"{d}/m_cp_b.json")
    run("merge", f"{d}/m_a.db", f"{d}/m_cp_b.json")
    expect(d, "m_a.db", "S", "r2", "model",
           [("A", 2, "M1"), ("B", 1, "M2")],
           "B.1: after exchange both source identities remain (M1, M2)")

    # User resolves at A: ordinary put kills both observed competitors
    b = write_batch(d, "a_m3.json", [
        {"op": "put", "supplier": "S", "asset_id": "r2",
         "fields": {"model": "M3"}}])
    run("batch", f"{d}/m_a.db", b)
    expect(d, "m_a.db", "S", "r2", "model", [("A", 3, "M3")],
           "B.2: A's resolution put kills M1 and M2 locally")
    # B writes M4 *before* receiving M3: M4 is concurrent with A's M3,
    # a competitor A has not observed.
    b = write_batch(d, "b_m4.json", [
        {"op": "put", "supplier": "S", "asset_id": "r2",
         "fields": {"model": "M4"}}])
    run("batch", f"{d}/m_b.db", b)
    # B only receives the M3 delta (no fresh checkpoint, no baseline re-send)
    run("delta", f"{d}/m_a.db", "S", f"{d}/m_cp_a.json", f"{d}/m_dl_m3.json")
    run("merge", f"{d}/m_b.db", f"{d}/m_dl_m3.json")
    expect(d, "m_b.db", "S", "r2", "model",
           [("A", 3, "M3"), ("B", 2, "M4")],
           "B.3: delta alone delivers M3; B's concurrent M4 stays (M3/M4)")

    # Exchange again: A never saw M4, so both source identities survive.
    run("checkpoint", f"{d}/m_b.db", "S", f"{d}/m_cp_b2.json")
    run("merge", f"{d}/m_a.db", f"{d}/m_cp_b2.json")
    expect(d, "m_a.db", "S", "r2", "model",
           [("A", 3, "M3"), ("B", 2, "M4")],
           "B.4: exchange keeps M3 and A-unseen M4 at A too")
    # A later resolution must kill only versions A observed; a concurrent
    # put made by B after B's export remains untouchable.
    b = write_batch(d, "b_m6.json", [
        {"op": "put", "supplier": "S", "asset_id": "r2",
         "fields": {"model": "M6"}}])
    run("batch", f"{d}/m_b.db", b)
    b = write_batch(d, "a_resolve.json", [
        {"op": "put", "supplier": "S", "asset_id": "r2",
         "fields": {"model": "M5"}}])
    run("batch", f"{d}/m_a.db", b)
    run("checkpoint", f"{d}/m_a.db", "S", f"{d}/m_cp_a2.json")
    run("merge", f"{d}/m_b.db", f"{d}/m_cp_a2.json")
    expect(d, "m_b.db", "S", "r2", "model",
           [("A", 4, "M5"), ("B", 3, "M6")],
           "B.5: A's resolution kills observed M3/M4 but cannot touch B's "
           "unseen concurrent M6")


def scenario_field_and_null_edge_cases(d):
    print("\n=== Scenario 3: concurrent fields, same value/different origin, "
          "null vs missing, same-batch write-then-delete ===")
    run("init", f"{d}/e_a.db", "--replica-id", "A")
    run("init", f"{d}/e_b.db", "--replica-id", "B")
    # baseline
    b = write_batch(d, "e_base.json", [
        {"op": "put", "supplier": "S", "asset_id": "g1",
         "fields": {"location": "L", "model": "M", "category": "C"}}])
    run("batch", f"{d}/e_a.db", b)
    run("checkpoint", f"{d}/e_a.db", "S", f"{d}/e_base_cp.json")
    run("merge", f"{d}/e_b.db", f"{d}/e_base_cp.json")
    # A changes location only; B changes model only; both put same category
    b = write_batch(d, "e_a.json", [
        {"op": "put", "supplier": "S", "asset_id": "g1",
         "fields": {"location": "L2", "category": "same"}}])
    run("batch", f"{d}/e_a.db", b)
    b = write_batch(d, "e_b.json", [
        {"op": "put", "supplier": "S", "asset_id": "g1",
         "fields": {"model": "M2", "category": "same"}}])
    run("batch", f"{d}/e_b.db", b)
    run("checkpoint", f"{d}/e_a.db", "S", f"{d}/e_cp_a.json")
    run("checkpoint", f"{d}/e_b.db", "S", f"{d}/e_cp_b.json")
    run("merge", f"{d}/e_a.db", f"{d}/e_cp_b.json")
    expect(d, "e_a.db", "S", "g1", "location", [("A", 2, "L2")],
           "C.1: different-field concurrent edits both apply (location)")
    expect(d, "e_a.db", "S", "g1", "model", [("B", 1, "M2")],
           "C.2: different-field concurrent edits both apply (model)")
    expect(d, "e_a.db", "S", "g1", "category",
           [("A", 2, "same"), ("B", 1, "same")],
           "C.3: identical text keeps BOTH source identities")

    # null is a known value; an omitted field stays untouched
    b = write_batch(d, "e_null.json", [
        {"op": "put", "supplier": "S", "asset_id": "g2",
         "fields": {"location": None, "model": "kept"}}])
    run("batch", f"{d}/e_a.db", b)
    b = write_batch(d, "e_null2.json", [
        {"op": "put", "supplier": "S", "asset_id": "g2",
         "fields": {"category": "cat"}}])  # does not name location/model
    run("batch", f"{d}/e_a.db", b)
    row = expect(d, "e_a.db", "S", "g2", "location", [("A", 3, None)],
                 "C.4: explicit null stored as a live known value")
    check("C.5: omitted field keeps its prior value",
          live(row, "model") == [("A", 3, "kept")])
    check("C.6: separately put field present",
          live(row, "category") == [("A", 4, "cat")])

    # same-batch put then erase -> nothing survives; erase sees the put
    b = write_batch(d, "e_pd.json", [
        {"op": "put", "supplier": "S", "asset_id": "g3",
         "fields": {"location": "tmp"}},
        {"op": "erase", "supplier": "S", "asset_id": "g3"}])
    run("batch", f"{d}/e_a.db", b)
    absent(d, "e_a.db", "S", "g3",
           "C.7: same-batch write-then-delete leaves no row")


def scenario_partition_isolation(d):
    print("\n=== Scenario 4: supplier partition isolation ===")
    run("init", f"{d}/p_a.db", "--replica-id", "A")
    run("init", f"{d}/p_b.db", "--replica-id", "B")
    for sup, val in (("X", "x1"), ("Y", "y1")):
        b = write_batch(d, f"p_{sup}.json", [
            {"op": "put", "supplier": sup, "asset_id": "k",
             "fields": {"location": val}}])
        run("batch", f"{d}/p_a.db", b)
    run("checkpoint", f"{d}/p_a.db", "X", f"{d}/p_cp_x.json")
    run("merge", f"{d}/p_b.db", f"{d}/p_cp_x.json")
    expect(d, "p_b.db", "X", "k", "location", [("A", 1, "x1")],
           "D.1: supplier X syncs")
    absent(d, "p_b.db", "Y", "k",
           "D.2: supplier Y untouched by an X package")
    _, out = run("suppliers", f"{d}/p_b.db")
    check("D.3: B knows only partition X", out["suppliers"] == ["X"],
          str(out["suppliers"]))
    # Y erase at A then another X checkpoint: B's X row must not be deleted
    b = write_batch(d, "p_y_erase.json", [
        {"op": "erase", "supplier": "Y", "asset_id": "k"}])
    run("batch", f"{d}/p_a.db", b)
    run("checkpoint", f"{d}/p_a.db", "X", f"{d}/p_cp_x2.json")
    run("merge", f"{d}/p_b.db", f"{d}/p_cp_x2.json")
    expect(d, "p_b.db", "X", "k", "location", [("A", 1, "x1")],
           "D.4: rows absent from a package are never deleted")


def scenario_delivery_modes(d):
    print("\n=== Scenario 5: third-party forward, duplicates, reordering ===")
    run("init", f"{d}/f_a.db", "--replica-id", "A")
    run("init", f"{d}/f_b.db", "--replica-id", "B")
    run("init", f"{d}/f_c.db", "--replica-id", "C")
    b = write_batch(d, "f_put.json", [
        {"op": "put", "supplier": "S", "asset_id": "q1",
         "fields": {"location": "v1"}}])
    run("batch", f"{d}/f_a.db", b)
    run("checkpoint", f"{d}/f_a.db", "S", f"{d}/f_cp1.json")
    # A keeps editing; B forwards cp1 to C (byte copy), C merges 3x
    with open(f"{d}/f_cp1.json", "rb") as src, \
         open(f"{d}/f_forward.json", "wb") as dst:
        dst.write(src.read())
    for _ in range(3):
        run("merge", f"{d}/f_c.db", f"{d}/f_forward.json")
    expect(d, "f_c.db", "S", "q1", "location", [("A", 1, "v1")],
           "E.1: third-party forwarded package, merged 3x, still one version")
    b = write_batch(d, "f_put2.json", [
        {"op": "put", "supplier": "S", "asset_id": "q1",
         "fields": {"location": "v2"}}])
    run("batch", f"{d}/f_a.db", b)
    run("checkpoint", f"{d}/f_a.db", "S", f"{d}/f_cp2.json")
    # C receives newer checkpoint FIRST, then the older one again
    run("merge", f"{d}/f_c.db", f"{d}/f_cp2.json")
    run("merge", f"{d}/f_c.db", f"{d}/f_cp1.json")
    expect(d, "f_c.db", "S", "q1", "location", [("A", 2, "v2")],
           "E.2: out-of-order re-delivery of old checkpoint cannot roll back")
    # delete the row at sender after handoff: package stays valid
    b = write_batch(d, "f_erase.json", [
        {"op": "erase", "supplier": "S", "asset_id": "q1"}])
    run("batch", f"{d}/f_a.db", b)
    run("merge", f"{d}/f_b.db", f"{d}/f_cp2.json")  # package from before delete
    expect(d, "f_b.db", "S", "q1", "location", [("A", 2, "v2")],
           "E.3: sender later deleting locally does not invalidate an "
           "already-handed package")
    # corrupted package rejected wholesale
    with open(f"{d}/f_cp2.json", encoding="utf-8") as fh:
        pkg = json.load(fh)
    pkg["adds"][0]["value"] = "TAMPERED"
    with open(f"{d}/f_bad.json", "w", encoding="utf-8") as fh:
        json.dump(pkg, fh)
    rc, _ = run("merge", f"{d}/f_b.db", f"{d}/f_bad.json", check=False)
    check("E.4: tampered package rejected", rc == 2)
    expect(d, "f_b.db", "S", "q1", "location", [("A", 2, "v2")],
           "E.5: rejected package changed nothing")
    # unknown field rejected
    b = write_batch(d, "f_bad_batch.json", [
        {"op": "put", "supplier": "S", "asset_id": "q1",
         "fields": {"bogus": "x"}}])
    rc, _ = run("batch", f"{d}/f_b.db", b, check=False)
    check("E.6: unknown field in batch rejected", rc == 2)
    # truncated file rejected
    with open(f"{d}/f_cp2.json", "rb") as fh:
        data = fh.read()
    with open(f"{d}/f_trunc.json", "wb") as fh:
        fh.write(data[: len(data) // 2])
    rc, _ = run("merge", f"{d}/f_b.db", f"{d}/f_trunc.json", check=False)
    check("E.7: truncated package rejected", rc == 2)


def main():
    with tempfile.TemporaryDirectory(prefix="mdm-demo-") as d:
        scenario_erase_vs_unseen_put(d)
        scenario_model_conflict(d)
        scenario_field_and_null_edge_cases(d)
        scenario_partition_isolation(d)
        scenario_delivery_modes(d)
    if FAILURES:
        print(f"\n{len(FAILURES)} FAILURE(S)")
        sys.exit(1)
    print("\nAll hand-written scenario assertions passed.")


if __name__ == "__main__":
    main()
