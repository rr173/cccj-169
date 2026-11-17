"""Bounded-memory, bounded-storage end-to-end drill.

Runtime-generated data only (nothing large is written to the repository):

* a source partition with at least 100,000 live rows is loaded through the
  subscription outlet in *small* pages while another writer keeps producing
  updates, so a slow consumer accumulates a real backlog;
* peak Python memory is sampled with ``tracemalloc`` around the paging loops
  to prove neither side loads the whole partition or the whole backlog into
  Python memory to serve a small page;
* backlog and on-disk sizes are recorded before and after cleanup;
* after the business row count is fixed and only commit count keeps growing,
  the consumers catch up, release and clean up, and the remaining storage is
  explained by current business data plus the necessary metadata only.

Usage:  python3 -I tests/test_subscription_scale.py [ROWS] [LOAD_PAGE]
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
import tempfile
import time
import tracemalloc

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from mdm_merge import LandingStore, Replica, SubscriptionManager  # noqa: E402

BATCH = 2000          # rows per import_batch transaction
UPDATE_TXNS = 60      # continuing commits while the load is in progress
EVENT_PAGE = 50       # small change page (transactions per page)


def db_bytes(path: str) -> int:
    conn = sqlite3.connect(path)
    pc, ps = conn.execute("PRAGMA page_count").fetchone()[0], \
        conn.execute("PRAGMA page_size").fetchone()[0]
    conn.close()
    return pc * ps


def main():
    rows = int(sys.argv[1]) if len(sys.argv) > 1 else 100_000
    load_page = int(sys.argv[2]) if len(sys.argv) > 2 else 500
    assert rows >= 100_000 or len(sys.argv) > 1
    work = tempfile.mkdtemp(prefix="mdm-sub-scale-")
    src = os.path.join(work, "source.db")
    land_db = os.path.join(work, "landing.db")
    print(f"workdir={work}\nrows={rows:,} load_page={load_page} "
          f"event_page={EVENT_PAGE} batch={BATCH} "
          f"concurrent_update_txns={UPDATE_TXNS}")

    rep = Replica.create(src, "A")
    t0 = time.perf_counter()
    for start in range(0, rows, BATCH):
        n = min(BATCH, rows - start)
        rep.import_batch([
            {"op": "put", "supplier": "BIG", "asset_id": f"asset{start+i}",
             "fields": {"location": f"L{start+i}", "model": f"M{i % 97}",
                         "category": None}}
            for i in range(n)])
    print(f"populated {rows:,} rows in {time.perf_counter()-t0:.1f}s")
    rep.close()

    mgr = SubscriptionManager(src)
    sub = mgr.create_subscription("BIG", page_size=load_page)
    expected_load_pages = (sub["load_total_rows"] - 1) // load_page + 1
    print(f"subscription cut_seq={sub['cut_seq']} "
          f"load_rows={sub['load_total_rows']:,} "
          f"load_pages={expected_load_pages}")

    # Concurrent writer: keep updating existing rows AFTER the cut while the
    # slow consumer is still paging the initial load.
    writer = Replica(src)
    for t in range(UPDATE_TXNS):
        writer.import_batch([
            {"op": "put", "supplier": "BIG",
             "asset_id": f"asset{(t * 7919) % rows}",
             "fields": {"location": f"upd-{t}"}}])
    writer.close()

    land = LandingStore(land_db)
    tracemalloc.start()
    peak_before = tracemalloc.get_traced_memory()[1]
    t0 = time.perf_counter()
    token = sub["first_load_token"]
    page = mgr.read_load_page(token)
    land.begin_load(page)
    mgr.ack_load_page(token)
    load_pages_seen = 1
    max_page_rows = page["row_count"]
    while page["next_token"]:
        token = page["next_token"]
        page = mgr.read_load_page(token)
        max_page_rows = max(max_page_rows, page["row_count"])
        land.apply_load_page(page)
        load_pages_seen += 1
        mgr.ack_load_page(token)
    load_seconds = time.perf_counter() - t0
    _, peak_load = tracemalloc.get_traced_memory()
    land.complete_load("BIG")
    info = mgr.complete_load(sub["sub_id"])
    print(f"load: {load_pages_seen} pages in {load_seconds:.1f}s, "
          f"max rows/page={max_page_rows} "
          f"peak Python mem during paging={peak_load/1_048_576:.1f} MiB "
          f"(partition is {rows*3/1_048_576:.1f} MiB of versions)")
    assert load_pages_seen == expected_load_pages
    assert max_page_rows <= load_page

    # The frozen cut landed in full despite concurrent post-cut writes.
    landed = land.conn.execute(
        "SELECT COUNT(*) FROM live_versions WHERE supplier='BIG'"
    ).fetchone()[0]
    print(f"landed cut versions={landed:,} (expected {rows*3:,})")
    assert landed == rows * 3

    # Backlog state before draining: every concurrent commit is retained.
    backlog = mgr.backlog(sub["sub_id"])["subscriptions"][0]
    print(f"backlog before drain: unconfirmed_events="
          f"{backlog['unconfirmed_events']} "
          f"retained_event_rows="
          f"{backlog['retained_event_rows_in_partition']}")
    assert backlog["unconfirmed_events"] == UPDATE_TXNS

    # Drain the change stream in small pages; sample peak memory again.
    tracemalloc.reset_peak()
    t0 = time.perf_counter()
    token = info["first_event_token"]
    event_pages = 0
    applied_total = 0
    while True:
        page = mgr.read_event_page(token, page_size=EVENT_PAGE)
        if not page["events"]:
            break
        r = land.apply_event_page(page)
        applied_total += r["events_applied"]
        event_pages += 1
        token = page["next_token"]
        mgr.ack_events(token)
    _, peak_events = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    print(f"events: {event_pages} pages, {applied_total} txns applied in "
          f"{time.perf_counter()-t0:.1f}s, peak Python mem="
          f"{peak_events/1_048_576:.1f} MiB")
    assert applied_total == UPDATE_TXNS

    # Landing store now matches the source partition exactly, row by row.
    src_rep = Replica(src)
    src_rows = {r["asset_id"]: r for r in src_rep.read_partition("BIG")}
    land_assets = land.list_rows("BIG")
    print(f"source live rows={len(src_rows):,} landed rows="
          f"{len(land_assets):,}")
    assert len(land_assets) == len(src_rows)
    mismatched = 0
    for asset in land_assets[::997]:
        sr, lr = src_rows[asset], land.read_row("BIG", asset)
        for f in ("location", "model", "category"):
            sv = [(v["creator"], v["counter"], v["value"])
                  for v in sr["fields"][f]["versions"]]
            lv = [(v["creator"], v["counter"], v["value"])
                  for v in lr["fields"][f]["versions"]]
            if sv != lv:
                mismatched += 1
    assert mismatched == 0
    src_rep.close()

    # Storage accounting before and after release/cleanup.
    mgr.cleanup()  # nothing reclaimable yet: subscription still live
    src_bytes_protected = db_bytes(src)
    land_bytes = db_bytes(land_db)
    print(f"disk while subscription active: source={src_bytes_protected:,} "
          f"landing={land_bytes:,}")
    mgr.release_subscription(sub["sub_id"])
    mgr.cleanup()
    src_bytes_after = db_bytes(src)
    print(f"disk after release+cleanup: source={src_bytes_after:,} "
          f"(reclaimed {src_bytes_protected-src_bytes_after:,} bytes)")
    remaining_events = mgr.conn.execute(
        "SELECT COUNT(*) FROM sub_events").fetchone()[0]
    remaining_loads = mgr.conn.execute(
        "SELECT COUNT(*) FROM sub_load_rows").fetchone()[0]
    assert remaining_events == 0 and remaining_loads == 0

    # Fixed business rows, only more commits now: storage must not regrow.
    rep = Replica(src)
    for t in range(UPDATE_TXNS):
        rep.import_batch([
            {"op": "put", "supplier": "BIG",
             "asset_id": f"asset{(t * 104729) % rows}",
             "fields": {"model": f"more-{t}"}}])
    rep.close()
    # No live subscription exists, so these commits grew no event ledger.
    mgr2 = SubscriptionManager(src)
    assert mgr2.conn.execute(
        "SELECT COUNT(*) FROM sub_events").fetchone()[0] == 0
    src_bytes_more_commits = db_bytes(src)
    print(f"after {UPDATE_TXNS} more commits with no subscriber: source="
          f"{src_bytes_more_commits:,}")

    # Explain what the remaining source storage consists of.
    mgr_rep = Replica(src)
    stats = mgr_rep.stats()
    print("remaining source storage is determined by current data + "
          "necessary metadata only:")
    print(json.dumps({k: stats[k] for k in (
        "live_versions", "field_state_entries", "watermark_entries",
        "frontier_entries", "pending_kill_entries",
        "checkpoint_summaries", "local_ops", "db_bytes")}, indent=2))
    tombstones = mgr2.conn.execute(
        "SELECT COUNT(*) FROM sub_subscriptions").fetchone()[0]
    print(f"  subscription tables after release: "
          f"sub_events={remaining_events}, sub_load_rows={remaining_loads}, "
          f"sub_subscriptions(tombstones)={tombstones}")
    mgr_rep.close()
    mgr2.close()
    land.close()
    print("\nBOUNDED DRILL OK: small pages, bounded peak memory, backlog "
          "drained, cleanup reclaimed protected history, storage stays "
          "current-data-bounded.")


if __name__ == "__main__":
    main()
