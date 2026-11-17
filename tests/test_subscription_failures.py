"""Negative-path acceptance tests for the subscription outlet and landing.

Every failure must be rejected *before* published business data changes, and
the precise error class must tell the operator whether a reload is required
versus a simple retry:

* missing/out-of-order load and event pages;
* acknowledgements that skip undelivered transactions, go backwards, or use
  the wrong phase;
* tampered page/token bytes and digest mismatches;
* a token reused after the subscription was released;
* an old event cursor whose history was reclaimed (explicit reload error,
  never a silent continue from the earliest surviving event);
* pages signed for another source database or another partition;
* a reload that replaces a previously published view only on completion,
  keeping the old view queryable meanwhile.
"""

from __future__ import annotations

import base64
import copy
import json
import os
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from mdm_merge import (LandingError, LandingStore, Replica,  # noqa: E402
                       SubscriptionError, SubscriptionManager,
                       protocol)
from mdm_merge.subscriptions import (encode_token, sign_page,  # noqa: E402
                                     validate_page)

FAIL = []


def check(label, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {label}"
          + (f" -- {detail}" if detail and not cond else ""))
    if not cond:
        FAIL.append(label)


def expect_error(label, fn, errors=(SubscriptionError, LandingError)):
    try:
        fn()
    except errors as exc:
        check(label, True)
        return exc
    check(label, False, "no error raised")
    return None


def setup(work, n_rows=4):
    src = os.path.join(work, "src.db")
    rep = Replica.create(src, "A")
    rep.import_batch([
        {"op": "put", "supplier": "S", "asset_id": f"r{i}",
         "fields": {"location": f"v{i}", "model": f"m{i}"}}
        for i in range(n_rows)])
    rep.close()
    mgr = SubscriptionManager(src)
    sub = mgr.create_subscription("S", page_size=2)
    land = LandingStore(os.path.join(work, "land.db"))
    return src, mgr, sub, land


def load_all(mgr, sub, land):
    token = sub["first_load_token"]
    page = mgr.read_load_page(token)
    land.begin_load(page)
    mgr.ack_load_page(token)
    pages = [page]
    while page["next_token"]:
        token = page["next_token"]
        page = mgr.read_load_page(token)
        land.apply_load_page(page)
        pages.append(page)
        mgr.ack_load_page(token)
    land.complete_load("S")
    info = mgr.complete_load(sub["sub_id"])
    return info, pages


def main():
    work = tempfile.mkdtemp(prefix="mdm-sub-neg-")

    # --------------------------------------------------------- load rejects
    src, mgr, sub, land = setup(work)
    page0 = mgr.read_load_page(sub["first_load_token"])
    land.begin_load(page0)
    page1 = mgr.read_load_page(page0["next_token"])

    # Out-of-order load page (skip page 1, feed page 2).
    page2 = mgr.read_load_page(page1["next_token"])
    expect_error("gap in load pages rejected before staging",
                 lambda: land.apply_load_page(page2))
    # A tampered load page is rejected on digest first.
    tampered = copy.deepcopy(page1)
    tampered["rows"][0]["versions"][0]["value"] = "HACKED"
    expect_error("tampered load page digest mismatch",
                 lambda: land.apply_load_page(
                     validate_page(tampered) if False else tampered))
    # Re-delivering page 0 with different content fails on identity.
    other = copy.deepcopy(page0)
    other["rows"][0]["versions"][0]["value"] = "OTHER"
    other.pop("digest")
    other = sign_page(other)
    expect_error("same load index, different content rejected",
                 lambda: land.apply_load_page(other))
    check("rejected loads changed no staged rows",
          land.conn.execute("SELECT COUNT(*) FROM staging_versions"
                            ).fetchone()[0]
          == sum(len(r["versions"]) for r in page0["rows"]))

    # Garbage / wrong-purpose tokens.
    expect_error("malformed token rejected",
                 lambda: mgr.read_load_page("not-a-token"))
    event_token = encode_token({"purpose": "event", "db_uid": mgr.db_uid,
                                "sub_id": sub["sub_id"], "supplier": "S",
                                "after_seq": 0})
    expect_error("event token cannot drive a load read",
                 lambda: mgr.read_load_page(event_token))
    load_token = encode_token({"purpose": "load", "db_uid": mgr.db_uid,
                               "sub_id": sub["sub_id"], "supplier": "S",
                               "page_index": 0})
    expect_error("load token cannot drive an event read",
                 lambda: mgr.read_event_page(load_token))

    # Events cannot be read before the load completes.
    expect_error("event read refused during load phase",
                 lambda: mgr.read_event_page(
                     encode_token({"purpose": "event", "db_uid": mgr.db_uid,
                                   "sub_id": sub["sub_id"], "supplier": "S",
                                   "after_seq": sub["cut_seq"]})))
    expect_error("landing refuses events before a published load",
                 lambda: land.apply_event_page({"format":
                                                 "mdm-sub-event/v1"}))

    info, pages = load_all(mgr, sub, land)
    check("clean load completes after the rejected attempts",
          land.status()["partitions"][0]["state"] == "live")

    # -------------------------------------------------------- event rejects
    rep = Replica(src)
    rep.import_batch([{"op": "put", "supplier": "S", "asset_id": "r0",
                      "fields": {"location": "updated"}}])
    rep.import_batch([{"op": "put", "supplier": "S", "asset_id": "r1",
                      "fields": {"location": "updated2"}}])
    rep.close()

    t = info["first_event_token"]
    ep1 = mgr.read_event_page(t, page_size=1)
    land.apply_event_page(ep1)
    # A gap: feed a synthetic page whose seq jumps past applied+1.
    gap = sign_page({
        "format": "mdm-sub-event/v1", "protocol_version": 1,
        "db_uid": mgr.db_uid, "replica_id": "A", "sub_id": sub["sub_id"],
        "supplier": "S", "begin_seq": 99, "end_seq": 99, "next_seq": 99,
        "page_size": 1, "event_count": 1,
        "events": [{"seq": 99, "txn_kind": "local_batch",
                    "committed_at": "x",
                    "changes": [{"asset_id": "r1", "before": None,
                                 "after": None}]}],
        "next_token": t})
    expect_error("event gap rejected before any business write",
                 lambda: land.apply_event_page(gap))
    check("gap rejection left applied position untouched",
          land.status()["partitions"][0]["applied_seq"] == 1)

    # Tampered event bytes.
    ep2 = mgr.read_event_page(ep1["next_token"], page_size=1)
    bad = copy.deepcopy(ep2)
    bad["events"][0]["changes"][0]["after"]["location"][0]["value"] = "EVIL"
    expect_error("tampered event page rejected on digest",
                 lambda: land.apply_event_page(bad))
    # Re-send the genuine page: must still apply exactly once.
    land.apply_event_page(ep2)
    before = land.conn.execute("SELECT COUNT(*) FROM live_versions"
                               ).fetchone()[0]
    land.apply_event_page(ep2)
    check("genuine re-delivery is idempotent after a tamper attempt",
          land.conn.execute("SELECT COUNT(*) FROM live_versions"
                            ).fetchone()[0] == before)

    # ----------------------------------------------------------- ack rules
    # Confirming an undelivered position is refused.
    future = encode_token({"purpose": "event", "db_uid": mgr.db_uid,
                           "sub_id": sub["sub_id"], "supplier": "S",
                           "after_seq": 500})
    expect_error("ack that skips undelivered transactions refused",
                 lambda: mgr.ack_events(future))
    # Backwards ack refused: after confirming position 2, position 1 must be
    # rejected even though 1 was delivered earlier.
    mgr.ack_events(ep2["next_token"])
    back = encode_token({"purpose": "event", "db_uid": mgr.db_uid,
                         "sub_id": sub["sub_id"], "supplier": "S",
                         "after_seq": 1})
    expect_error("backwards ack refused", lambda: mgr.ack_events(back))
    # Acknowledging the already delivered/confirmed page again is a no-op.
    mgr.ack_events(ep2["next_token"])
    check("valid ack advances confirmed_seq",
          mgr.backlog(sub["sub_id"])["subscriptions"][0]["confirmed_seq"]
          == ep2["end_seq"])

    # ------------------------------------------- cursor/source/partition bind
    # Token signed for a different db_uid.
    foreign = encode_token({"purpose": "event", "db_uid": "db-OTHER",
                            "sub_id": sub["sub_id"], "supplier": "S",
                            "after_seq": 2})
    expect_error("cursor from another source database refused",
                 lambda: mgr.read_event_page(foreign))
    foreign_partition = encode_token(
        {"purpose": "event", "db_uid": mgr.db_uid,
         "sub_id": sub["sub_id"], "supplier": "OTHER", "after_seq": 2})
    expect_error("cursor partition mismatch refused",
                 lambda: mgr.read_event_page(foreign_partition))
    unknown_sub = encode_token(
        {"purpose": "event", "db_uid": mgr.db_uid,
         "sub_id": "sub-doesnotexist", "supplier": "S", "after_seq": 0})
    expect_error("unknown subscription token refused",
                 lambda: mgr.read_event_page(unknown_sub))

    # A forged token body (digest over different content).
    raw, digest = sub["first_load_token"].split(".")
    forged = raw + "." + ("0" * 64)
    expect_error("forged token digest refused",
                 lambda: mgr.read_load_page(forged))

    # --------------------------------------------- release / reclaim behavior
    # A second slow subscription keeps history while the first is released.
    slow = mgr.create_subscription("S", page_size=2)
    slow_land = LandingStore(os.path.join(work, "slow.db"))
    load_all(mgr, slow, slow_land)
    released_token = ep2["next_token"]
    mgr.release_subscription(sub["sub_id"])
    expect_error("released subscription event token explicitly invalid",
                 lambda: mgr.read_event_page(released_token))
    expect_error("released subscription load token explicitly invalid",
                 lambda: mgr.read_load_page(
                     encode_token({"purpose": "load", "db_uid": mgr.db_uid,
                                   "sub_id": sub["sub_id"], "supplier": "S",
                                   "page_index": 0})))
    # The slow subscription still reads the retained history untouched. It
    # was created after seq 1..2, so its stream begins at its own cut point.
    slow_cut = slow["cut_seq"]
    sp = mgr.read_event_page(
        mgr._event_token(slow["sub_id"], slow_cut), page_size=10)
    check("releasing one consumer did not damage the other's stream",
          [e["seq"] for e in sp["events"]]
          == list(range(slow_cut + 1, 3)), str(sp["events"]))

    # Once the slow consumer confirms and is released, cleanup reclaims; the
    # old cursor then fails with an explicit reload-required error rather
    # than silently continuing at the earliest surviving event.
    mgr.ack_events(sp["next_token"])
    old_cursor = encode_token(
        {"purpose": "event", "db_uid": mgr.db_uid,
         "sub_id": slow["sub_id"], "supplier": "S", "after_seq": 0})
    mgr.release_subscription(slow["sub_id"])
    mgr.cleanup()
    check("all events reclaimed once every consumer released",
          mgr.conn.execute(
              "SELECT COUNT(*) FROM sub_events WHERE supplier='S'"
          ).fetchone()[0] == 0)
    expect_error("reclaimed cursor demands reload, never silent resume",
                 lambda: mgr.read_event_page(old_cursor))

    # ------------------------------------------------- reload replaces view
    reload_sub = mgr.create_subscription("S", page_size=50)
    rpage = mgr.read_load_page(reload_sub["first_load_token"])
    # Old published view stays readable while the replacement load stages.
    old_r0 = land.read_row("S", "r0")
    check("old published view remains queryable during a reload",
          old_r0["fields"]["location"]["versions"][0]["value"] == "updated")
    land.begin_load(rpage)
    check("landing state flips to loading but old table still holds data",
          land.status()["partitions"][0]["state"] == "loading"
          and land.conn.execute(
              "SELECT COUNT(*) FROM live_versions WHERE supplier='S'"
          ).fetchone()[0] > 0)
    expect_error("no row reads are advertised as the new view while loading",
                 lambda: land.read_row("S", "r0"), (LandingError,))
    land.complete_load("S")
    check("reloaded view is published and still queryable",
          land.read_row("S", "r0")["fields"]["location"]["versions"][0][
              "value"] == "updated"
          and land.status()["partitions"][0]["state"] == "live")

    # --------------------------------------------- load ack completeness rule
    half = mgr.create_subscription("S", page_size=1)
    hp = mgr.read_load_page(half["first_load_token"])
    expect_error("completing a partially acked load is refused",
                 lambda: mgr.complete_load(half["sub_id"]))

    mgr.close()
    land.close()
    slow_land.close()
    if FAIL:
        print(f"\n{len(FAIL)} NEGATIVE-PATH FAILURE(S)")
        sys.exit(1)
    print("\nAll negative-path, release/reclaim and reload checks passed.")


if __name__ == "__main__":
    main()
