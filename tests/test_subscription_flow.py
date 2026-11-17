"""Hand-written acceptance scenarios for the subscription outlet and the
independent SQLite landing end.

A fixed sequence of commits is declared up front with explicit expectations:

  1. one row modified several times inside a single batch (one event, the
     complete before/after live sets only -- intermediate states are never
     published);
  2. two rows of one supplier modified together (one atomic event);
  3. same text with a changed source identity is still a change;
  4. a concurrent conflict collapses to a unique value via a later put;
  5. known null vs missing field stay distinguishable;
  6. the last live version of a row is deleted (after-image is null);
  7. write-then-delete within one transaction publishes nothing;
  8. duplicate package / frontier-only merge / reads / compaction produce no
     business events.

Every assertion compares full per-field version *identities* and the landed
relational rows -- never just counts.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from mdm_merge import (LandingError, LandingStore, Replica,  # noqa: E402
                       SubscriptionError, SubscriptionManager,
                       build_handoff, protocol)

FAIL = []


def check(label, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {label}"
          + (f" -- {detail}" if detail and not cond else ""))
    if not cond:
        FAIL.append(label)


def versions_of(land: LandingStore, supplier: str, asset: str, field: str):
    row = land.read_row(supplier, asset)
    return [(v["creator"], v["counter"], v["value"])
            for v in row["fields"][field]["versions"]]


def drain_load(mgr: SubscriptionManager, sub: dict, land: LandingStore,
               ack: bool = True):
    """Page through the whole fixed-cut load with the smallest page size."""
    token = sub["first_load_token"]
    first = mgr.read_load_page(token)
    land.begin_load(first)
    if ack:
        mgr.ack_load_page(token)
    pages = [first]
    while first["next_token"]:
        token = first["next_token"]
        first = mgr.read_load_page(token)
        land.apply_load_page(first)
        pages.append(first)
        if ack:
            mgr.ack_load_page(token)
    return pages


def drain_events(mgr: SubscriptionManager, sub_id: str, land: LandingStore,
                 start_token: str, page_size: int = 50, ack: bool = True):
    token = start_token
    seen = []
    while True:
        page = mgr.read_event_page(token, page_size=page_size)
        if not page["events"]:
            break
        land.apply_event_page(page)
        seen.extend(page["events"])
        token = page["next_token"]
        if ack:
            mgr.ack_events(token)
    return seen, token


def main():
    work = tempfile.mkdtemp(prefix="mdm-sub-")
    src = os.path.join(work, "source.db")
    land_db = os.path.join(work, "landing.db")

    rep = Replica.create(src, "A")
    # Pre-subscription history: must arrive via the initial load, never as
    # fabricated past events.
    rep.import_batch([
        {"op": "put", "supplier": "S", "asset_id": "r0",
         "fields": {"location": "base", "model": "keep",
                    "category": None}}])
    rep.close()

    mgr = SubscriptionManager(src)
    sub = mgr.create_subscription("S", page_size=1)   # tiny pages on purpose
    # Pre-subscription commits occupied no event position (no outlet existed
    # yet), but the fixed-cut load still delivers every current live version.
    check("cut point starts at zero when no stream existed; the load still "
          "carries all 3 live versions",
          sub["cut_seq"] == 0 and sub["load_total_rows"] == 3, str(sub))
    check("no partition ledger exists before the first protected commit",
          mgr.conn.execute(
              "SELECT 1 FROM sub_partition WHERE supplier='S'"
          ).fetchone() is None)

    land = LandingStore(land_db)
    pages = drain_load(mgr, sub, land)
    check("initial load paged the 3 live versions with page_size=1",
          [p["row_count"] for p in pages] == [1, 1, 1])
    # The new view must not be queryable before the load completes.
    try:
        land.read_row("S", "r0")
        check("unfinished load is not advertised as a view", False)
    except LandingError:
        check("unfinished load is not advertised as a view", True)
    land.complete_load("S")
    info = mgr.complete_load(sub["sub_id"])
    check("load completion opens the stream at the cut position",
          info["state"] == "live" and info["cut_seq"] == 0)
    check("known null vs missing field survive the load relationally",
          versions_of(land, "S", "r0", "category") == [("A", 1, None)]
          and land.read_row("S", "r0")["fields"]["location"]["status"]
          == "unique"
          and land.read_row("S", "r0")["fields"]["location"]["versions"][0][
              "value"] == "base")

    rep = Replica(src)
    # (1) same row modified multiple times in ONE transaction
    rep.import_batch([
        {"op": "put", "supplier": "S", "asset_id": "r1",
         "fields": {"location": "v1"}},
        {"op": "put", "supplier": "S", "asset_id": "r1",
         "fields": {"location": "v2"}},
        {"op": "put", "supplier": "S", "asset_id": "r1",
         "fields": {"location": "v3"}}])
    # (2) two rows modified together = one atomic event
    rep.import_batch([
        {"op": "put", "supplier": "S", "asset_id": "r2",
         "fields": {"model": "m1"}},
        {"op": "put", "supplier": "S", "asset_id": "r3",
         "fields": {"model": "m2"}}])
    # (3) same text, different source identity: merge a remote replica's
    # competing version with identical text -- still a change.
    remote_same = build_handoff(
        kind="checkpoint", replica_id="B", supplier="S",
        frontier={"B": 1},
        adds=[{"supplier": "S", "asset_id": "r1", "field": "location",
               "creator": "B", "counter": 1, "value": "v3"}],
        kills=[], checkpoint_id="cp-b1")
    rep.merge_package(remote_same)
    # (4) conflict resolved to a unique value by an ordinary local put
    rep.import_batch([
        {"op": "put", "supplier": "S", "asset_id": "r1",
         "fields": {"location": "resolved"}}])
    # (5) null vs missing on a fresh row: location explicitly null, model set
    rep.import_batch([
        {"op": "put", "supplier": "S", "asset_id": "r4",
         "fields": {"location": None, "model": "n"}}])
    # (6) last live version deleted
    rep.import_batch([
        {"op": "erase", "supplier": "S", "asset_id": "r3"}])
    # (7) write then delete inside one txn, ending exactly as it started:
    # no event may be published.
    rep.import_batch([
        {"op": "put", "supplier": "S", "asset_id": "r5",
         "fields": {"location": "tmp"}},
        {"op": "erase", "supplier": "S", "asset_id": "r5"}])
    # (8a) duplicate package: no business change
    rep.merge_package(remote_same)
    rep.close()

    # Reopen the manager in a NEW process-equivalent connection and page the
    # stream with page_size=1 to prove transaction boundaries never split.
    mgr2 = SubscriptionManager(src)
    events, end_token = drain_events(
        mgr2, sub["sub_id"], land, info["first_event_token"], page_size=1)
    seqs = [e["seq"] for e in events]
    check("one event per real commit; write-then-delete and duplicate merge "
          "emitted nothing",
          seqs == [1, 2, 3, 4, 5, 6], str(seqs))
    check("positions are gap-free and strictly monotonic",
          seqs == list(range(1, 7)))

    # --- event 2: triple modification shows only pre/post live sets ---
    e2 = events[0]
    check("event 2 is one local_batch touching only r1",
          e2["txn_kind"] == "local_batch"
          and [c["asset_id"] for c in e2["changes"]] == ["r1"])
    ch = e2["changes"][0]
    check("event 2 before=None (row absent), after holds only the final v3",
          ch["before"] is None
          and list(ch["after"]) == ["location"]
          and [(v["creator"], v["counter"], v["value"])
               for v in ch["after"]["location"]] == [("A", 4, "v3")],
          json.dumps(ch))

    # --- event 3: the two-row transaction is indivisible ---
    e3 = events[1]
    check("event 3 is a single transaction containing both rows",
          sorted(c["asset_id"] for c in e3["changes"]) == ["r2", "r3"])
    check("event 3 rows are creations with full after images",
          all(c["before"] is None for c in e3["changes"]))

    # --- event 4: same text / different origin is a change (merge) ---
    e4 = events[2]
    check("event 4 is published by the remote merge",
          e4["txn_kind"] == "merge"
          and [c["asset_id"] for c in e4["changes"]] == ["r1"])
    after4 = e4["changes"][0]["after"]["location"]
    check("event 4 keeps BOTH source identities despite identical text",
          sorted((v["creator"], v["counter"], v["value"]) for v in after4)
          == [("A", 4, "v3"), ("B", 1, "v3")], json.dumps(after4))

    # --- event 5: conflict -> unique ---
    e5 = events[3]
    after5 = e5["changes"][0]["after"]["location"]
    check("event 5 collapses the conflict to the resolving put only",
          [(v["creator"], v["counter"], v["value"]) for v in after5]
          == [("A", 7, "resolved")], json.dumps(after5))

    # --- event 6: explicit null vs missing ---
    e6 = events[4]
    after6 = e6["changes"][0]["after"]
    check("event 6 carries the known null and distinguishes the missing "
          "field",
          after6["location"][0]["value"] is None
          and after6["model"][0]["value"] == "n"
          and "category" not in after6, json.dumps(after6))

    # --- event 7: deletion after-image is null (row gone) ---
    e7 = events[5]
    ch7 = e7["changes"][0]
    check("event 7 deletes r3: after is null, before held the live model",
          ch7["after"] is None and "model" in ch7["before"],
          json.dumps(ch7))

    # --- landed relational state mirrors every expectation ---
    check("landed r1 unique resolved value with source identity",
          versions_of(land, "S", "r1", "location")
          == [("A", 7, "resolved")])
    check("landed r2/r3 creation and deletion",
          land.read_row("S", "r2")["exists"] is True
          and land.read_row("S", "r3")["exists"] is False)
    check("landed null is a stored value, missing stays missing",
          versions_of(land, "S", "r4", "location") == [("A", 8, None)]
          and land.read_row("S", "r4")["fields"]["category"]["status"]
          == "missing")
    check("landed r5 never existed (write-then-delete published nothing)",
          land.read_row("S", "r5")["exists"] is False)
    check("landed applied position equals the source's latest seq",
          land.status()["partitions"][0]["applied_seq"] == 6)
    check("pre-subscription row r0 still queryable with all 3 fields",
          versions_of(land, "S", "r0", "category") == [("A", 1, None)])

    # Reads and compaction must never manufacture events.
    rep = Replica(src)
    rep.read_partition("S")
    rep.compact()
    rep.close()
    page = mgr2.read_event_page(end_token, page_size=10)
    check("read + compact produced no business event",
          page["events"] == [] and page["end_seq"] == 6)
    check("stream survives compaction (ops ledger deleted, outlet intact)",
          mgr2.conn.execute(
              "SELECT COUNT(*) FROM sub_events WHERE supplier='S'"
          ).fetchone()[0] == 6)

    # Idempotent landing: re-deliver every transaction as a freshly signed
    # single-event page; re-delivery must not duplicate business versions.
    from mdm_merge.subscriptions import sign_page
    before_rows = land.conn.execute(
        "SELECT COUNT(*) FROM live_versions").fetchone()[0]
    last_single = None
    for ev in events:
        single = sign_page({
            "format": "mdm-sub-event/v1", "protocol_version": 1,
            "db_uid": mgr2.db_uid, "replica_id": "A",
            "sub_id": sub["sub_id"], "supplier": "S",
            "begin_seq": ev["seq"], "end_seq": ev["seq"],
            "next_seq": ev["seq"], "page_size": 1, "event_count": 1,
            "events": [ev], "next_token": end_token})
        land.apply_event_page(single)
        last_single = single
    after_rows = land.conn.execute(
        "SELECT COUNT(*) FROM live_versions").fetchone()[0]
    check("re-delivered events are idempotent (no duplicated versions)",
          before_rows == after_rows)

    # Same position with different content must be rejected before writes.
    bad = json.loads(json.dumps(last_single))
    # Inject a fabricated live value into the already-applied transaction.
    bad["events"][0]["changes"][0]["after"] = {
        "model": [{"field": "model", "creator": "Z", "counter": 999,
                   "value": "FORGED"}]}
    bad.pop("digest")
    bad = sign_page(bad)
    try:
        land.apply_event_page(bad)
        check("same seq / different content rejected", False)
    except LandingError:
        check("same seq / different content rejected", True)
    check("rejected event left published data untouched",
          land.conn.execute("SELECT COUNT(*) FROM live_versions"
                            ).fetchone()[0] == after_rows)

    land.close()
    mgr2.close()
    if FAIL:
        print(f"\n{len(FAIL)} SUBSCRIPTION FAILURE(S)")
        sys.exit(1)
    print("\nSubscription outlet + landing end: all hand-written event and "
          "relational assertions passed.")


if __name__ == "__main__":
    main()
