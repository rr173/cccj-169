"""Hard-crash tests for the subscription outlet and landing end.

Real ``os._exit`` kills are injected at deterministic points (via environment
variables read by the production code) and the same databases are reopened in
fresh processes:

1. source crash inside the change-publication step of a business transaction
   -- business writes and change rows must roll back together, leaving no
   half event and no position gap; the same input is then replayed;
2. landing crash between business-row replacement and the position commit of
   an event page -- the whole local transaction rolls back, and re-delivery
   converges idempotently;
3. landing has committed an event but the process dies before acknowledging
   the source -- on reopen the source position is unchanged, the page is
   re-fetched, re-applied idempotently and only then acknowledged.

No distributed transaction is involved: durability is achieved by
"land-commit first, source-ack second" plus idempotent replay.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from mdm_merge import LandingStore, Replica, SubscriptionManager  # noqa: E402

FAIL = []


def check(label, cond, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {label}"
          + (f" -- {detail}" if detail and not cond else ""))
    if not cond:
        FAIL.append(label)


def run_py(code, env_extra=None):
    env = dict(os.environ)
    if env_extra:
        env.update(env_extra)
    p = subprocess.run([sys.executable, "-I", "-c", code],
                       capture_output=True, text=True, env=env)
    return p


def test_source_crash_mid_event(work):
    print("\n=== 1. source hard-crash inside event publication ===")
    src = os.path.join(work, "src.db")
    code = (
        "import sys; sys.path.insert(0, %r);"
        "from mdm_merge import Replica, SubscriptionManager, LandingStore;"
        "import tempfile, os;"
        "rep=Replica.create(%r,'A');"
        "rep.import_batch([{'op':'put','supplier':'S','asset_id':'r0',"
        "'fields':{'location':'base'}}]);"
        "rep.close();"
        "m=SubscriptionManager(%r);"
        "s=m.create_subscription('S', page_size=10);"
        "land=LandingStore(os.path.join(%r,'land0.db'));"
        "pg=m.read_load_page(s['first_load_token']);"
        "land.begin_load(pg); m.ack_load_page(s['first_load_token']);"
        "land.complete_load('S'); m.complete_load(s['sub_id']);"
        "import json; print(json.dumps(s)); m.close(); land.close()"
        % (ROOT, src, src, work))
    p = run_py(code)
    assert p.returncode == 0, p.stderr
    sub = json.loads(p.stdout)

    # Crash inside publish_changes on the next business txn.
    crash_code = (
        "import sys; sys.path.insert(0, %r);"
        "from mdm_merge import Replica;"
        "rep=Replica(%r);"
        "rep.import_batch([{'op':'put','supplier':'S','asset_id':'r1',"
        "'fields':{'location':'x'}}, "
        "{'op':'put','supplier':'S','asset_id':'r2',"
        "'fields':{'location':'y'}}])"
        % (ROOT, src))
    p = run_py(crash_code, {"MDM_CRASH_AT": "1", "MDM_CRASH_TAG": "event"})
    check("source process died hard inside the event step",
          p.returncode == 37, p.stderr[-200:])

    # Reopen: no business row, no event, no partition gap.
    mgr = SubscriptionManager(src)
    rep = Replica(src)
    check("business writes rolled back with the event (r1 absent)",
          rep.read_row("S", "r1")["exists"] is False
          and rep.read_row("S", "r2")["exists"] is False)
    check("no event position was consumed by the crashed txn",
          mgr.conn.execute(
              "SELECT COALESCE(MAX(seq),0) FROM sub_events "
              "WHERE supplier='S'").fetchone()[0] == 0)
    check("no half-written sub_events rows exist",
          mgr.conn.execute(
              "SELECT COUNT(*) FROM sub_events").fetchone()[0] == 0)
    rep.close()

    # Replay the exact same input in a clean process: it now commits once.
    p = run_py(crash_code)
    check("replayed batch committed after reopen", p.returncode == 0,
          p.stderr)
    events = []
    tok = mgr._event_token(sub["sub_id"], sub["cut_seq"])
    while True:
        page = mgr.read_event_page(tok, page_size=10)
        if not page["events"]:
            break
        events.extend(page["events"])
        tok = page["next_token"]
    check("exactly one event exists after replay, carrying both rows",
          len(events) == 1
          and sorted(c["asset_id"] for c in events[0]["changes"])
          == ["r1", "r2"])
    mgr.close()


def test_landing_crash_mid_apply(work):
    print("\n=== 2. landing hard-crash between business write and position "
          "commit ===")
    src = os.path.join(work, "src2.db")
    land_db = os.path.join(work, "land2.db")
    rep = Replica.create(src, "A")
    rep.import_batch([{"op": "put", "supplier": "S", "asset_id": "k",
                      "fields": {"location": "0"}}])
    rep.close()
    mgr = SubscriptionManager(src)
    sub = mgr.create_subscription("S", page_size=10)
    land = LandingStore(land_db)
    p0 = mgr.read_load_page(sub["first_load_token"])
    land.begin_load(p0)
    mgr.ack_load_page(sub["first_load_token"])
    land.complete_load("S")
    info = mgr.complete_load(sub["sub_id"])
    land.close()

    rep = Replica(src)
    rep.import_batch([{"op": "put", "supplier": "S", "asset_id": "k",
                      "fields": {"location": "1"}}])
    rep.close()
    page = mgr.read_event_page(info["first_event_token"], page_size=10)
    page_path = os.path.join(work, "event_page.json")
    with open(page_path, "w", encoding="utf-8") as fh:
        json.dump(page, fh)

    # Crash at several deterministic points inside apply_event_page.
    for tag in ("event-begin", "event-step", "event-end"):
        snapshot = land_db + ".snap"
        subprocess.run(["cp", land_db, snapshot], check=True)
        code = (
            "import sys; sys.path.insert(0, %r);"
            "import json;"
            "from mdm_merge import LandingStore;"
            "page=json.load(open(%r));"
            "land=LandingStore(%r);"
            "land.apply_event_page(page)"
            % (ROOT, page_path, snapshot))
        p = run_py(code, {"MDM_LAND_CRASH_AT": "1",
                          "MDM_LAND_CRASH_TAG": tag})
        check(f"landing hard-exited at {tag}", p.returncode == 37,
              p.stderr[-150:])
        # Reopen the crashed copy: pre-event state only.
        check_land = LandingStore(snapshot)
        st = check_land.status()["partitions"][0]
        check(f"{tag}: position did not advance (still at cut)",
              st["applied_seq"] == 0 and st["state"] == "live")
        check(f"{tag}: business value is still the old one",
              check_land.read_row("S", "k")["fields"]["location"][
                  "versions"][0]["value"] == "0")
        # Re-deliver the same page: applies exactly once.
        result = check_land.apply_event_page(page)
        check(f"{tag}: re-delivery after crash applies exactly once",
              result["events_applied"] == 1
              and check_land.read_row("S", "k")["fields"]["location"][
                  "versions"][0]["value"] == "1")
        check_land.apply_event_page(page)  # second retry is a no-op
        check(f"{tag}: second re-delivery is idempotent",
              check_land.status()["partitions"][0]["applied_seq"] == 1)
        check_land.close()
        os.remove(snapshot)
    mgr.close()


def test_landing_committed_source_unacknowledged(work):
    print("\n=== 3. landing committed, source not yet acknowledged ===")
    src = os.path.join(work, "src3.db")
    land_db = os.path.join(work, "land3.db")
    rep = Replica.create(src, "A")
    rep.import_batch([{"op": "put", "supplier": "S", "asset_id": "z",
                      "fields": {"model": "m0"}}])
    rep.close()
    mgr = SubscriptionManager(src)
    sub = mgr.create_subscription("S", page_size=10)
    land = LandingStore(land_db)
    p0 = mgr.read_load_page(sub["first_load_token"])
    land.begin_load(p0)
    mgr.ack_load_page(sub["first_load_token"])
    land.complete_load("S")
    info = mgr.complete_load(sub["sub_id"])
    land.close()

    rep = Replica(src)
    for i in range(1, 4):
        rep.import_batch([{"op": "put", "supplier": "S", "asset_id": "z",
                           "fields": {"model": f"m{i}"}}])
    rep.close()

    # Consumer process: fetch + land-commit, then hard-exit BEFORE acking.
    fetch_code = (
        "import sys, json; sys.path.insert(0, %r);"
        "from mdm_merge import SubscriptionManager, LandingStore;"
        "mgr=SubscriptionManager(%r);"
        "page=mgr.read_event_page(%r, page_size=2);"
        "json.dump(page, open(%r,'w'));"
        "land=LandingStore(%r); land.apply_event_page(page);"
        "land.close();"
        "import os; os._exit(37)"
        % (ROOT, src, info["first_event_token"],
           os.path.join(work, "p3a.json"), land_db))
    p = run_py(fetch_code)
    check("consumer exited before acknowledging the source",
          p.returncode == 37)

    # Source side: delivered advanced but confirmed stayed at the cut.
    progress = mgr.backlog(sub["sub_id"])["subscriptions"][0]
    check("source distinguishes delivered from confirmed",
          progress["delivered_seq"] == 2
          and progress["confirmed_seq"] == sub["cut_seq"])

    # Restart: re-fetch from the unconfirmed cursor, re-apply, then ack.
    land = LandingStore(land_db)
    again = mgr.read_event_page(info["first_event_token"], page_size=2)
    result = land.apply_event_page(again)
    check("replayed page is idempotent after the un-acked crash",
          result["events_applied"] == 0
          and land.status()["partitions"][0]["applied_seq"] == 2)
    mgr.ack_events(again["next_token"])
    after = mgr.backlog(sub["sub_id"])["subscriptions"][0]
    check("acknowledgement after restart advances the confirmed watermark",
          after["confirmed_seq"] == 2)
    check("landed value through the crash reflects event seq 2 (m2)",
          land.read_row("S", "z")["fields"]["model"]["versions"][0][
              "value"] == "m2")
    # Drain the remaining event m3: no data was lost by the un-acked crash.
    rest = mgr.read_event_page(again["next_token"], page_size=10)
    land.apply_event_page(rest)
    mgr.ack_events(rest["next_token"])
    check("remaining events still flow and converge to the final value",
          land.read_row("S", "z")["fields"]["model"]["versions"][0][
              "value"] == "m3"
          and land.status()["partitions"][0]["applied_seq"] == 3)
    land.close()
    mgr.close()


def test_integrity_after_all(work):
    print("\n=== SQLite integrity of every crashed database ===")
    import sqlite3
    for name in os.listdir(work):
        if not name.endswith(".db"):
            continue
        conn = sqlite3.connect(os.path.join(work, name))
        ok = conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        check(f"{name}: integrity_check ok", ok)
        conn.close()


def main():
    work = tempfile.mkdtemp(prefix="mdm-sub-crash-")
    test_source_crash_mid_event(work)
    test_landing_crash_mid_apply(work)
    test_landing_committed_source_unacknowledged(work)
    test_integrity_after_all(work)
    if FAIL:
        print(f"\n{len(FAIL)} CRASH TEST FAILURE(S)")
        sys.exit(1)
    print("\nAll subscription/landing hard-crash paths recovered correctly.")


if __name__ == "__main__":
    main()
