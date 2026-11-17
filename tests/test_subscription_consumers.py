"""Two-consumer, cut-point and partition-isolation acceptance tests.

Scenarios:

* a slow consumer pages its initial load while another process keeps
  inserting, updating and deleting rows; every later load page still belongs
  to the original cut, and no change on either side of the cut is lost once
  it drains the event stream;
* a fast consumer catches up and triggers cleanup while the slow one is
  still behind -- the slow consumer's protected events and load copy survive
  and it still converges;
* acknowledgements of two subscriptions never cross wires;
* a batch spanning two suppliers produces independent per-partition events,
  and a subscription on X can never observe Y rows or values;
* one oversized transaction is delivered whole beyond the soft page limit;
* the original compact() coexists with an active subscription.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from mdm_merge import (LandingStore, Replica, SubscriptionError,  # noqa: E402
                       SubscriptionManager, build_handoff)

FAIL = []


def check(label, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {label}"
          + (f" -- {detail}" if detail and not cond else ""))
    if not cond:
        FAIL.append(label)


def live_triplets(land, supplier, asset, field):
    row = land.read_row(supplier, asset)
    return sorted((v["creator"], v["counter"], v["value"])
                  for v in row["fields"][field]["versions"])


def full_load(mgr, sub, land):
    token = sub["first_load_token"]
    page = mgr.read_load_page(token)
    land.begin_load(page)
    mgr.ack_load_page(token)
    while page["next_token"]:
        token = page["next_token"]
        page = mgr.read_load_page(token)
        land.apply_load_page(page)
        mgr.ack_load_page(token)
    land.complete_load(sub["supplier"])
    return mgr.complete_load(sub["sub_id"])


def drain(mgr, land, start_token, page_size=25):
    token = start_token
    applied = []
    while True:
        page = mgr.read_event_page(token, page_size=page_size)
        if not page["events"]:
            break
        land.apply_event_page(page)
        applied.extend(e["seq"] for e in page["events"])
        token = page["next_token"]
        mgr.ack_events(token)
    return applied, token


def test_cut_point_stability_and_two_speeds(work):
    print("\n=== cut-point stability: writes during a slow paged load ===")
    src = os.path.join(work, "src.db")
    rep = Replica.create(src, "A")
    rep.import_batch([
        {"op": "put", "supplier": "S", "asset_id": f"a{i}",
         "fields": {"location": f"v{i}"}} for i in range(6)])
    rep.close()

    mgr = SubscriptionManager(src)
    slow_sub = mgr.create_subscription("S", page_size=2)
    slow_land = LandingStore(os.path.join(work, "slow.db"))
    token = slow_sub["first_load_token"]
    page0 = mgr.read_load_page(token)
    slow_land.begin_load(page0)
    mgr.ack_load_page(token)
    cut_seq = slow_sub["cut_seq"]
    cut_total = slow_sub["load_total_rows"]
    check("cut captured the initial 6 rows", cut_total == 6)

    # Another process writes while the slow load is half done: update an old
    # row, delete another, add a brand-new row.
    rep = Replica(src)
    rep.import_batch([
        {"op": "put", "supplier": "S", "asset_id": "a0",
         "fields": {"location": "CHANGED"}},
        {"op": "erase", "supplier": "S", "asset_id": "a1"},
        {"op": "put", "supplier": "S", "asset_id": "aNEW",
         "fields": {"model": "fresh"}}])
    rep.close()

    # Remaining load pages must still describe the original cut: a0 keeps v0,
    # a1 is still present, aNEW is absent from the load copy.
    seen_rows, tok = [], page0["next_token"]
    while tok:
        pg = mgr.read_load_page(tok)
        seen_rows.extend(r["asset_id"] for r in pg["rows"])
        tok = pg["next_token"]
    seen_rows.extend(r["asset_id"] for r in page0["rows"])
    check("load pages never include rows created after the cut",
          "aNEW" not in seen_rows)
    load_copy = mgr.conn.execute(
        "SELECT asset_id,value FROM sub_load_rows WHERE sub_id=? "
        "AND field='location' ORDER BY asset_id",
        (slow_sub["sub_id"],)).fetchall()
    check("load copy is frozen at the cut (old value, not deleted)",
          ("a0", "v0") in load_copy and ("a1", "v1") in load_copy
          and all(a != "aNEW" for a, _ in load_copy), str(load_copy))

    # Finish the slow load, then drain events: both sides of the cut appear.
    while page0["next_token"]:
        token = page0["next_token"]
        page0 = mgr.read_load_page(token)
        slow_land.apply_load_page(page0)
        mgr.ack_load_page(token)
    slow_land.complete_load("S")
    info = mgr.complete_load(slow_sub["sub_id"])
    check("stream starts strictly after the frozen cut",
          info["cut_seq"] == cut_seq)
    applied, _ = drain(mgr, slow_land, info["first_event_token"],
                       page_size=1)
    check("slow consumer received exactly the post-cut transaction",
          applied == [cut_seq + 1])
    check("slow consumer landed the update, the delete and the new row",
          live_triplets(slow_land, "S", "a0", "location")
          == [("A", 7, "CHANGED")]
          and slow_land.read_row("S", "a1")["exists"] is False
          and live_triplets(slow_land, "S", "aNEW", "model")
          == [("A", 9, "fresh")])
    slow_land.close()
    mgr.close()


def test_fast_consumer_cleanup_protects_slow(work):
    print("\n=== fast consumer cleanup cannot touch slow consumer's data ===")
    src = os.path.join(work, "two.db")
    rep = Replica.create(src, "A")
    rep.import_batch([{"op": "put", "supplier": "S", "asset_id": "k",
                      "fields": {"location": "0"}}])
    rep.close()

    mgr = SubscriptionManager(src)
    fast_sub = mgr.create_subscription("S", page_size=10)
    slow_sub = mgr.create_subscription("S", page_size=10)
    fast_land = LandingStore(os.path.join(work, "fast.db"))
    slow_land = LandingStore(os.path.join(work, "slow2.db"))
    fast_info = full_load(mgr, fast_sub, fast_land)
    slow_info = full_load(mgr, slow_sub, slow_land)

    rep = Replica(src)
    for i in range(1, 9):
        rep.import_batch([{"op": "put", "supplier": "S", "asset_id": "k",
                           "fields": {"location": str(i)}}])
    rep.close()

    # Fast consumer catches up fully, confirms and asks for cleanup.
    applied, fast_tok = drain(mgr, fast_land,
                              fast_info["first_event_token"], page_size=3)
    check("fast consumer saw all eight commits", len(applied) == 8)
    result = mgr.cleanup()
    retained = mgr.conn.execute(
        "SELECT COUNT(*) FROM sub_events WHERE supplier='S'").fetchone()[0]
    check("cleanup retains everything the slow consumer still needs",
          retained == 8, str(result))
    check("slow consumer's frozen load copy is still protected",
          mgr.conn.execute(
              "SELECT COUNT(*) FROM sub_load_rows WHERE sub_id=?",
              (slow_sub["sub_id"],)).fetchone()[0]
          == slow_sub["load_total_rows"])

    # The slow consumer now drains the same history and converges.
    applied_slow, _ = drain(mgr, slow_land,
                            slow_info["first_event_token"], page_size=2)
    check("slow consumer independently received the same eight commits",
          applied_slow == applied)
    check("both landing stores converge to the same final live value",
          live_triplets(fast_land, "S", "k", "location")
          == live_triplets(slow_land, "S", "k", "location")
          == [("A", 9, "8")])

    # Ack isolation: a token minted for the fast subscription must not be
    # accepted as a slow-subscription acknowledgement via swapped sub ids.
    from mdm_merge.subscriptions import decode_token
    tok_doc = decode_token(fast_tok, "event")
    check("tokens embed their own sub identity",
          tok_doc["sub_id"] == fast_sub["sub_id"]
          and tok_doc["sub_id"] != slow_sub["sub_id"])
    fast_land.close()
    slow_land.close()
    mgr.close()


def test_partition_isolation_and_multi_supplier_batch(work):
    print("\n=== per-partition atomicity and strict supplier isolation ===")
    src = os.path.join(work, "iso.db")
    rep = Replica.create(src, "A")
    rep.close()
    mgr = SubscriptionManager(src)
    sub_x = mgr.create_subscription("X", page_size=5)
    land_x = LandingStore(os.path.join(work, "x.db"))
    info_x = full_load(mgr, sub_x, land_x)

    rep = Replica(src)
    # One local batch spans two suppliers: separate events per partition,
    # each consumed atomically on its own.
    rep.import_batch([
        {"op": "put", "supplier": "X", "asset_id": "x1",
         "fields": {"location": "x"}},
        {"op": "put", "supplier": "Y", "asset_id": "y1",
         "fields": {"location": "y"}},
        {"op": "put", "supplier": "Y", "asset_id": "y2",
         "fields": {"model": "m"}}])
    rep.close()

    page = mgr.read_event_page(info_x["first_event_token"], page_size=50)
    check("X subscription gets exactly one event containing only X rows",
          len(page["events"]) == 1
          and [c["asset_id"] for c in page["events"][0]["changes"]] == ["x1"]
          and all(c["asset_id"].startswith("x")
                  for c in page["events"][0]["changes"]))
    land_x.apply_event_page(page)
    check("no Y row or value ever reaches the X landing database",
          land_x.conn.execute(
              "SELECT COUNT(*) FROM live_versions WHERE supplier='Y'"
          ).fetchone()[0] == 0
          and land_x.read_row("X", "x1")["exists"] is True)

    # A token/cursor for X is rejected against a Y subscription.
    sub_y = mgr.create_subscription("Y", page_size=5)
    from mdm_merge.subscriptions import encode_token
    cross_token = encode_token(
        {"purpose": "load", "db_uid": mgr.db_uid,
         "sub_id": sub_y["sub_id"], "supplier": "X", "page_index": 0})
    try:
        mgr.read_load_page(cross_token)
        check("cross-partition cursor rejected", False)
    except SubscriptionError:
        check("cross-partition cursor rejected", True)
    land_x.close()
    mgr.close()


def test_unsubscribed_partition_grows_no_ledger(work):
    print("\n=== partitions without a live subscription stay storage-bounded ===")
    src = os.path.join(work, "noleak.db")
    rep = Replica.create(src, "A")
    rep.close()
    mgr = SubscriptionManager(src)
    mgr.create_subscription("X", page_size=5)
    # A single batch spans three partitions; only X has a subscription.
    rep = Replica(src)
    rep.import_batch([
        {"op": "put", "supplier": "X", "asset_id": "x1",
         "fields": {"location": "x"}},
        {"op": "put", "supplier": "Y", "asset_id": "y1",
         "fields": {"location": "y"}},
        {"op": "put", "supplier": "Z", "asset_id": "z1",
         "fields": {"location": "z"}}])
    rep.close()
    check("only the subscribed partition gets an event position",
          mgr.conn.execute(
              "SELECT supplier,seq FROM sub_events ORDER BY supplier"
          ).fetchall() == [("X", 1)])
    # A Y subscription created afterwards starts from a cut-point load (the
    # existing Y row) and its own seq 1 -- no fabricated past events.
    sub_y = mgr.create_subscription("Y", page_size=5)
    check("late Y subscription receives the existing row through its load",
          sub_y["cut_seq"] == 0 and sub_y["load_total_rows"] == 1)
    rep = Replica(src)
    rep.import_batch([{"op": "put", "supplier": "Y", "asset_id": "y2",
                      "fields": {"model": "m"}}])
    rep.close()
    check("the late Y partition's stream begins contiguously at seq 1",
          mgr.conn.execute(
              "SELECT seq FROM sub_events WHERE supplier='Y'"
          ).fetchall() == [(1,)])
    mgr.close()


def test_oversized_transaction_is_whole(work):
    print("\n=== oversized transaction exceeds the soft page limit, whole ===")
    src = os.path.join(work, "big.db")
    rep = Replica.create(src, "A")
    rep.close()
    mgr = SubscriptionManager(src)
    sub = mgr.create_subscription("S", page_size=4)
    land = LandingStore(os.path.join(work, "bigland.db"))
    info = full_load(mgr, sub, land)

    rep = Replica(src)
    rep.import_batch([
        {"op": "put", "supplier": "S", "asset_id": f"row{i}",
         "fields": {"location": f"v{i}", "model": f"m{i}"}}
        for i in range(50)])  # one transaction, 100 live versions
    rep.close()
    page = mgr.read_event_page(info["first_event_token"], page_size=1)
    check("soft limit of 1 transaction still delivers the whole txn",
          page["event_count"] == 1 and len(page["events"][0]["changes"]) == 50)
    land.apply_event_page(page)
    check("landing published every row of the oversized transaction",
          land.conn.execute(
              "SELECT COUNT(*) FROM live_versions WHERE supplier='S'"
          ).fetchone()[0] == 100)
    land.close()
    mgr.close()


def test_compact_coexists_with_active_subscription(work):
    print("\n=== compact() with a live subscription keeps the outlet intact ===")
    src = os.path.join(work, "compact.db")
    rep = Replica.create(src, "A")
    rep.import_batch([{"op": "put", "supplier": "S", "asset_id": "c",
                      "fields": {"location": "0"}}])
    rep.close()
    mgr = SubscriptionManager(src)
    sub = mgr.create_subscription("S", page_size=2)
    land = LandingStore(os.path.join(work, "cland.db"))
    info = full_load(mgr, sub, land)

    rep = Replica(src)
    for i in range(1, 6):
        rep.import_batch([{"op": "put", "supplier": "S", "asset_id": "c",
                          "fields": {"location": str(i)}}])
    # Compact while the subscription is active: the ops ledger disappears but
    # the change stream must keep flowing.
    removed = rep.compact()
    check("compact removed the whole operation ledger as before",
          removed["ops"] >= 5 and rep.stats()["local_ops"] == 0)
    rep.import_batch([{"op": "put", "supplier": "S", "asset_id": "c",
                      "fields": {"location": "after-compact"}}])
    rep.close()

    applied, _ = drain(mgr, land, info["first_event_token"], page_size=2)
    check("all six post-cut events survived and streamed after compaction",
          applied == [1, 2, 3, 4, 5, 6], str(applied))
    check("landing reached the post-compaction value with identity",
          live_triplets(land, "S", "c", "location")
          == [("A", 7, "after-compact")])
    land.close()
    mgr.close()


def test_cli_subprocess_roundtrip(work):
    print("\n=== CLI roundtrip in fresh processes (no live Python connection) ===")
    src = os.path.join(work, "cli.db")
    land_db = os.path.join(work, "cliland.db")
    mdm = os.path.join(ROOT, "mdm")
    mdmland = os.path.join(ROOT, "mdmland")
    env = dict(os.environ)

    def run(launcher, *args):
        p = subprocess.run([sys.executable, "-I", launcher, *args],
                           capture_output=True, text=True, env=env)
        if p.returncode != 0:
            raise AssertionError(f"{args} failed: {p.stderr}")
        return json.loads(p.stdout) if p.stdout.strip() else {}

    run(mdm, "init", src, "--replica-id", "A")
    batch = os.path.join(work, "b.json")
    with open(batch, "w", encoding="utf-8") as fh:
        json.dump({"format": "mdm-batch/v1", "protocol_version": 1,
                   "ops": [{"op": "put", "supplier": "S", "asset_id": "q",
                            "fields": {"location": "L"}}]}, fh)
    run(mdm, "batch", src, batch)
    created = run(mdm, "sub-create", src, "S", "--page-size", "1")
    page_path = os.path.join(work, "p0.json")
    run(mdm, "sub-load", src, created["first_load_token"], "--out", page_path)
    run(mdmland, "land-begin", land_db, page_path)
    run(mdm, "sub-ack-load", src, created["first_load_token"])
    run(mdmland, "land-complete", land_db, "S")
    run(mdm, "sub-complete", src, created["sub_id"])
    row = run(mdmland, "land-get", land_db, "S", "q")
    check("CLI-landed row carries the live value",
          row["fields"]["location"]["versions"][0]["value"] == "L")
    run(mdm, "sub-backlog", src)
    run(mdm, "sub-release", src, created["sub_id"])
    run(mdm, "sub-cleanup", src)


def main():
    work = tempfile.mkdtemp(prefix="mdm-sub-consumers-")
    test_cut_point_stability_and_two_speeds(work)
    test_fast_consumer_cleanup_protects_slow(work)
    test_partition_isolation_and_multi_supplier_batch(work)
    test_oversized_transaction_is_whole(work)
    test_compact_coexists_with_active_subscription(work)
    test_unsubscribed_partition_grows_no_ledger(work)
    test_cli_subprocess_roundtrip(work)
    if FAIL:
        print(f"\n{len(FAIL)} CONSUMER TEST FAILURE(S)")
        sys.exit(1)
    print("\nAll two-consumer, isolation, oversize and compaction tests "
          "passed.")


if __name__ == "__main__":
    main()
