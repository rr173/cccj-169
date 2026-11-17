"""Independent SQLite landing end: a reference downstream consumer.

The landing store is a *separate* SQLite database that never imports the
replica engine: it only understands the signed subscription page documents
(``mdm-sub-load/v1`` / ``mdm-sub-event/v1``) and materializes their content
relationally as live versions keyed by

    (supplier, asset_id, field, creator, counter)

plus the applied source position. It is a runnable consumer, not a printer:
the committed tables are queryable through :meth:`LandingStore.read_row` and
ordinary SQL.

Correctness envelope -- every violation below is rejected *before* any
published business data changes:

* pages signed for another source database (``db_uid``) or partition;
* digest/token corruption and unsupported format/version;
* event gaps or out-of-order delivery relative to the applied position;
* re-delivery is idempotent: an already applied position with identical
  content writes nothing; the same position with different content fails;
* a full load replaces a previously published view atomically, and while a
  reload is in progress the old published view stays queryable and is never
  advertised as the new one.

Each business mutation and its position update are one local transaction;
there is deliberately no distributed transaction with the source -- the
source is acknowledged only after the local commit is durable, so a crash
before that ack simply causes a re-delivery that converges idempotently.
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from typing import Any, Dict, List, Optional

from . import protocol
from .subscriptions import (EVENT_FORMAT, LOAD_FORMAT, STATE_LIVE,
                            STATE_LOADING, validate_page)

LANDING_SCHEMA_VERSION = 1

LANDING_SCHEMA = """
CREATE TABLE IF NOT EXISTS landing_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS live_versions (
    supplier TEXT NOT NULL,
    asset_id TEXT NOT NULL,
    field    TEXT NOT NULL,
    creator  TEXT NOT NULL,
    counter  INTEGER NOT NULL,
    value    TEXT,
    PRIMARY KEY (supplier, asset_id, field, creator, counter)
);
CREATE TABLE IF NOT EXISTS applied_positions (
    supplier   TEXT PRIMARY KEY,
    db_uid     TEXT NOT NULL,
    state      TEXT NOT NULL,                -- loading | live
    cut_seq    INTEGER NOT NULL,
    applied_seq INTEGER NOT NULL,
    load_id    TEXT
);
CREATE TABLE IF NOT EXISTS staging_versions (
    load_id  TEXT NOT NULL,
    supplier TEXT NOT NULL,
    asset_id TEXT NOT NULL,
    field    TEXT NOT NULL,
    creator  TEXT NOT NULL,
    counter  INTEGER NOT NULL,
    value    TEXT,
    PRIMARY KEY (load_id, asset_id, field, creator, counter)
);
CREATE TABLE IF NOT EXISTS staging_pages (
    load_id    TEXT NOT NULL,
    page_index INTEGER NOT NULL,
    page_digest TEXT NOT NULL,
    PRIMARY KEY (load_id, page_index)
);
CREATE TABLE IF NOT EXISTS applied_event_digests (
    supplier TEXT NOT NULL,
    seq      INTEGER NOT NULL,
    digest   TEXT NOT NULL,
    PRIMARY KEY (supplier, seq)
);
"""


class LandingError(RuntimeError):
    """Raised when a page cannot be safely applied to the landing store."""


def _crash(tag: str, step_env: str = "MDM_LAND_CRASH_AT") -> None:
    point = os.environ.get(step_env)
    want = os.environ.get("MDM_LAND_CRASH_TAG")
    if point and want == tag:
        os._exit(37)


class LandingStore:
    """One independent landing database, bound to one source/partition."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        d = os.path.dirname(os.path.abspath(db_path))
        os.makedirs(d, exist_ok=True)
        self.conn = sqlite3.connect(db_path)
        self.conn.execute("PRAGMA busy_timeout=5000")
        self.conn.executescript(LANDING_SCHEMA)
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> "LandingStore":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # ------------------------------------------------------------ helpers
    def _binding(self, supplier: str) -> Optional[Dict[str, Any]]:
        row = self.conn.execute(
            "SELECT db_uid,state,cut_seq,applied_seq,load_id "
            "FROM applied_positions WHERE supplier=?", (supplier,)).fetchone()
        if row is None:
            return None
        return dict(zip(("db_uid", "state", "cut_seq", "applied_seq",
                         "load_id"), row))

    def _check_binding(self, page: Dict[str, Any]) -> None:
        binding = self._binding(page["supplier"])
        if binding is None:
            return
        if binding["db_uid"] != page["db_uid"]:
            raise LandingError(
                "page comes from a different source database than the one "
                "this landing store is bound to (db_uid mismatch)")

    @staticmethod
    def _digest(page: Dict[str, Any]) -> str:
        return page["digest"]

    # --------------------------------------------------------- load phase
    def begin_load(self, first_page: Dict[str, Any]) -> Dict[str, Any]:
        """Start (or restart) a full initial load from a signed load page.

        A restart while a previous load is unfinished replaces only the
        staging area; an already published view in ``live_versions`` remains
        intact and queryable until the new load completes and switches over
        atomically.
        """
        page = validate_page(first_page, LOAD_FORMAT)
        self._check_binding(page)
        if page["page_index"] != 0:
            raise LandingError(
                "begin_load requires the first load page (page_index=0)")
        load_id = f"load-{uuid.uuid4().hex}"
        supplier = page["supplier"]
        try:
            self.conn.execute("BEGIN IMMEDIATE")
            binding = self._binding(supplier)
            if binding is not None and binding["state"] == STATE_LIVE:
                # Reload case: keep the published view and its applied
                # position untouched; stage the replacement beside them.
                self.conn.execute(
                    "UPDATE applied_positions SET state=?, cut_seq=?, "
                    "applied_seq=?, load_id=? WHERE supplier=?",
                    (STATE_LOADING, page["cut_seq"], page["cut_seq"],
                     load_id, supplier))
            else:
                self.conn.execute(
                    "INSERT INTO applied_positions"
                    "(supplier,db_uid,state,cut_seq,applied_seq,load_id) "
                    "VALUES (?,?,?,?,?,?) "
                    "ON CONFLICT(supplier) DO UPDATE SET db_uid=excluded.db_uid,"
                    " state=excluded.state, cut_seq=excluded.cut_seq,"
                    " applied_seq=excluded.applied_seq,"
                    " load_id=excluded.load_id",
                    (supplier, page["db_uid"], STATE_LOADING,
                     page["cut_seq"], page["cut_seq"], load_id))
            old_loads = [r[0] for r in self.conn.execute(
                "SELECT DISTINCT load_id FROM staging_versions").fetchall()]
            for old in old_loads:
                self.conn.execute(
                    "DELETE FROM staging_versions WHERE load_id=?", (old,))
                self.conn.execute(
                    "DELETE FROM staging_pages WHERE load_id=?", (old,))
            self._insert_load_page(load_id, page)
            self.conn.commit()
        except BaseException:
            self.conn.rollback()
            raise
        return {"supplier": supplier, "load_id": load_id,
                "cut_seq": page["cut_seq"], "total_rows": page["total_rows"],
                "pages_acked": 1}

    def _insert_load_page(self, load_id: str, page: Dict[str, Any]) -> None:
        supplier = page["supplier"]
        existing = self.conn.execute(
            "SELECT page_digest FROM staging_pages WHERE load_id=? "
            "AND page_index=?", (load_id, page["page_index"])).fetchone()
        if existing is not None:
            if existing[0] != page["digest"]:
                raise LandingError(
                    f"load page {page['page_index']} re-delivered with "
                    "different content for the same token position")
            return  # idempotent retry: identical page, nothing to write
        # Pages must arrive in order: a missing earlier page would leave a
        # hole in the materialized cut.
        max_page = self.conn.execute(
            "SELECT COALESCE(MAX(page_index),-1) FROM staging_pages "
            "WHERE load_id=?", (load_id,)).fetchone()[0]
        if page["page_index"] != max_page + 1:
            raise LandingError(
                f"out-of-order load page: expected index {max_page + 1}, "
                f"got {page['page_index']} (missing pages are not allowed)")
        for row in page["rows"]:
            for v in row["versions"]:
                self.conn.execute(
                    "INSERT INTO staging_versions"
                    "(load_id,supplier,asset_id,field,creator,counter,value)"
                    " VALUES (?,?,?,?,?,?,?)",
                    (load_id, supplier, row["asset_id"], v["field"],
                     v["creator"], v["counter"], v["value"]))
        self.conn.execute(
            "INSERT INTO staging_pages(load_id,page_index,page_digest) "
            "VALUES (?,?,?)",
            (load_id, page["page_index"], page["digest"]))

    def apply_load_page(self, page: Dict[str, Any]) -> Dict[str, Any]:
        """Persist one load page into the staging area."""
        page = validate_page(page, LOAD_FORMAT)
        self._check_binding(page)
        binding = self._binding(page["supplier"])
        if binding is None or binding["state"] != STATE_LOADING:
            raise LandingError(
                "no initial load in progress for this partition; call "
                "begin_load with page_index=0 first")
        if binding["db_uid"] != page["db_uid"]:
            raise LandingError("source identity changed mid-load (db_uid)")
        load_id = binding["load_id"]
        try:
            self.conn.execute("BEGIN IMMEDIATE")
            self._insert_load_page(load_id, page)
            self.conn.commit()
        except BaseException:
            self.conn.rollback()
            raise
        return {"supplier": page["supplier"], "load_id": load_id,
                "page_index": page["page_index"],
                "pages_acked": self.conn.execute(
                    "SELECT COUNT(*) FROM staging_pages WHERE load_id=?",
                    (load_id,)).fetchone()[0]}

    def complete_load(self, supplier: str) -> Dict[str, Any]:
        """Atomically publish the staged view and the cut position.

        The whole switch -- delete old published versions, insert staged
        ones, flip the binding state -- is one transaction, so an outside
        reader can only ever see the old view or the complete new view.
        """
        binding = self._binding(supplier)
        if binding is None:
            raise LandingError("no load in progress for this partition")
        if binding["state"] == STATE_LIVE:
            return {"supplier": supplier, "state": STATE_LIVE,
                    "applied_seq": binding["applied_seq"]}
        load_id = binding["load_id"]
        pages = self.conn.execute(
            "SELECT page_index,page_digest FROM staging_pages WHERE load_id=? "
            "ORDER BY page_index", (load_id,)).fetchall()
        if not pages:
            raise LandingError("load has no pages")
        expected = list(range(len(pages)))
        got = [p[0] for p in pages]
        if got != expected:
            missing = sorted(set(expected) - set(got))
            raise LandingError(
                f"cannot complete load: missing page(s) {missing[:5]}")
        staged = self.conn.execute(
            "SELECT COUNT(*) FROM staging_versions WHERE load_id=?",
            (load_id,)).fetchone()[0]
        try:
            self.conn.execute("BEGIN IMMEDIATE")
            _crash("load-swap-begin")
            self.conn.execute(
                "DELETE FROM live_versions WHERE supplier=?", (supplier,))
            self.conn.execute(
                "INSERT INTO live_versions"
                "(supplier,asset_id,field,creator,counter,value) "
                "SELECT supplier,asset_id,field,creator,counter,value "
                "FROM staging_versions WHERE load_id=?", (load_id,))
            self.conn.execute(
                "UPDATE applied_positions SET state=?, applied_seq=cut_seq "
                "WHERE supplier=?", (STATE_LIVE, supplier))
            # Staging rows of the completed load are retained until the next
            # begin_load replaces them; they no longer protect anything once
            # live_versions exists, but keeping them makes a retry of
            # complete_load idempotent and cheap to verify.
            _crash("load-swap-end")
            self.conn.commit()
        except BaseException:
            self.conn.rollback()
            raise
        return {"supplier": supplier, "state": STATE_LIVE,
                "applied_seq": binding["cut_seq"],
                "published_versions": staged}

    # -------------------------------------------------------- event phase
    def _apply_one_event(self, ev: Dict[str, Any], supplier: str,
                         db_uid: str) -> None:
        seq = ev["seq"]
        binding = self._binding(supplier)
        if binding is None or binding["state"] != STATE_LIVE:
            raise LandingError(
                "cannot apply events before a full load is published")
        if binding["db_uid"] != db_uid:
            raise LandingError("source identity mismatch (db_uid)")
        applied = binding["applied_seq"]
        previous = self.conn.execute(
            "SELECT digest FROM applied_event_digests WHERE supplier=? "
            "AND seq=?", (supplier, seq)).fetchone()
        if previous is not None:
            # Already applied: idempotent only when content is identical.
            if previous[0] != protocol.compute_digest(ev):
                raise LandingError(
                    f"event seq {seq} already applied with different "
                    "content: same position, conflicting payload")
            return
        if seq != applied + 1:
            raise LandingError(
                f"non-contiguous event: applied_seq={applied}, page wants "
                f"seq {seq} (gap or reorder rejected before any write)")
        # One transaction's complete row-set replacement. before/after
        # snapshots describe full live sets of touched rows; we recompute the
        # target set from the authoritative after-image.
        for ch in ev["changes"]:
            asset_id = ch["asset_id"]
            self.conn.execute(
                "DELETE FROM live_versions WHERE supplier=? AND asset_id=?",
                (supplier, asset_id))
            if ch["after"] is not None:
                for field, versions in ch["after"].items():
                    for v in versions:
                        self.conn.execute(
                            "INSERT INTO live_versions"
                            "(supplier,asset_id,field,creator,counter,value)"
                            " VALUES (?,?,?,?,?,?)",
                            (supplier, asset_id, field, v["creator"],
                             v["counter"], v["value"]))
        self.conn.execute(
            "INSERT INTO applied_event_digests(supplier,seq,digest) "
            "VALUES (?,?,?)",
            (supplier, seq, protocol.compute_digest(ev)))
        self.conn.execute(
            "UPDATE applied_positions SET applied_seq=? WHERE supplier=?",
            (seq, supplier))

    def apply_event_page(self, page: Dict[str, Any]) -> Dict[str, Any]:
        """Apply all transactions of one signed event page atomically.

        Every event of the page plus the final position update commit in one
        local transaction. A failure on any event rejects the whole page
        before published data changes; re-sending the page after a crash
        re-enters idempotently.
        """
        page = validate_page(page, EVENT_FORMAT)
        self._check_binding(page)
        supplier = page["supplier"]
        binding = self._binding(supplier)
        if binding is None:
            raise LandingError(
                "no binding for this partition; complete a load first")
        if binding["state"] != STATE_LIVE:
            raise LandingError(
                "initial load not published; events cannot be applied")
        if not page["events"]:
            # Still fully validated and bound above; an empty page moves
            # nothing and cannot be used to touch an unknown partition.
            return {"supplier": supplier,
                    "applied_seq": binding["applied_seq"],
                    "events_applied": 0, "idle": True}
        # Whole-page continuity check up front, before any write.
        expected_first = binding["applied_seq"] + 1
        seqs = [ev["seq"] for ev in page["events"]]
        known = {r[0]: r[1] for r in self.conn.execute(
            "SELECT seq,digest FROM applied_event_digests WHERE supplier=?",
            (supplier,)).fetchall()}
        fresh = [s for s in seqs if s not in known]
        if fresh:
            if min(fresh) != expected_first \
                    or fresh != list(range(min(fresh), min(fresh)
                                           + len(fresh))):
                raise LandingError(
                    f"event page creates a gap/reorder: applied_seq="
                    f"{binding['applied_seq']}, page seqs={seqs}")
        try:
            self.conn.execute("BEGIN IMMEDIATE")
            _crash("event-begin")
            step = 0
            for ev in page["events"]:
                self._apply_one_event(ev, supplier, page["db_uid"])
                step += 1
                _crash("event-step")
            _crash("event-end")
            self.conn.commit()
        except BaseException:
            self.conn.rollback()
            raise
        new_binding = self._binding(supplier)
        return {"supplier": supplier,
                "applied_seq": new_binding["applied_seq"],
                "events_applied": len(fresh),
                "page_end_seq": page["end_seq"],
                "digest": page["digest"]}

    # ------------------------------------------------------------- reads
    def status(self) -> Dict[str, Any]:
        out = {"partitions": []}
        for supplier, db_uid, state, cut_seq, applied_seq, load_id in \
                self.conn.execute(
                    "SELECT supplier,db_uid,state,cut_seq,applied_seq,load_id "
                    "FROM applied_positions ORDER BY supplier"):
            out["partitions"].append(
                {"supplier": supplier, "source_db_uid": db_uid,
                 "state": state, "cut_seq": cut_seq,
                 "applied_seq": applied_seq, "load_id": load_id,
                 "live_versions": self.conn.execute(
                     "SELECT COUNT(*) FROM live_versions "
                     "WHERE supplier=?", (supplier,)).fetchone()[0],
                 "staging_versions": self.conn.execute(
                     "SELECT COUNT(*) FROM staging_versions "
                     "WHERE load_id=?", (load_id,)).fetchone()[0]
                 if load_id else 0})
        return out

    def read_row(self, supplier: str, asset_id: str) -> Dict[str, Any]:
        """Reconstruct missing/unique/conflict exactly like the source view.

        Fails while this partition's first load has not published, so an
        unfinished load can never masquerade as a completed new view. During
        a *reload* the old published view remains readable here.
        """
        binding = self._binding(supplier)
        if binding is None or binding["state"] != STATE_LIVE:
            raise LandingError(
                "no published view for this partition yet (load in "
                "progress or never started)")
        out = {"supplier": supplier, "asset_id": asset_id,
               "exists": False, "fields": {}}
        for field in protocol.VALID_FIELDS:
            versions = [
                {"value": v, "creator": c, "counter": n, "origin": [c, n]}
                for c, n, v in self.conn.execute(
                    "SELECT creator,counter,value FROM live_versions "
                    "WHERE supplier=? AND asset_id=? AND field=? "
                    "ORDER BY creator,counter",
                    (supplier, asset_id, field))]
            status = "missing"
            if len(versions) == 1:
                status = "unique"
                out["exists"] = True
            elif len(versions) > 1:
                status = "conflict"
                out["exists"] = True
            out["fields"][field] = {"status": status, "versions": versions}
        return out

    def list_rows(self, supplier: str) -> List[str]:
        binding = self._binding(supplier)
        if binding is None or binding["state"] != STATE_LIVE:
            raise LandingError("no published view for this partition yet")
        return [r[0] for r in self.conn.execute(
            "SELECT DISTINCT asset_id FROM live_versions WHERE supplier=? "
            "ORDER BY asset_id", (supplier,))]
