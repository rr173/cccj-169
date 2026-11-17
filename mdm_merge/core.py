"""Multi-replica causal merge engine over SQLite.

Model (per supplier partition, row key = (supplier, asset_id),
fixed fields location/model/category):

* Every local put/erase gets a unique, never-reused source coordinate
  (replica_id, counter).  A put carrying several fields still owns one
  coordinate; each field value is the version (replica_id, counter, field).
* An operation *observes* every currently-live field version it replaces
  (put: the named fields; erase: every field of the row).  Observation is
  derived solely from committed local imports / merges -- never from wall
  clocks, arrival order or replica_id ordering.
* A field version is live iff no operation whose observation set contains
  it has ever been integrated.  Per (field, creator) the dead versions form
  a counter prefix, so all history collapses into one integer watermark.
* Concurrent puts (neither observed the other) stay live together even if
  their text is identical; identity is (creator, counter), never value.

Steady-state tables (field_state + dead_watermark + replica_frontier) are
enough to merge any package, including very old ones after compaction:
an arriving version whose counter is <= the watermark is born dead and
cannot be resurrected.
"""

from __future__ import annotations

import json
import os
import sqlite3
import uuid
from typing import Any, Dict, List, Optional, Sequence, Tuple

from . import protocol

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS ops (
    counter    INTEGER PRIMARY KEY,          -- this replica's own coordinate
    supplier   TEXT NOT NULL,
    asset_id   TEXT NOT NULL,
    op         TEXT NOT NULL,                -- put | erase
    fields     TEXT                          -- JSON for put
);
CREATE TABLE IF NOT EXISTS op_observes (
    op_counter      INTEGER NOT NULL,
    supplier        TEXT NOT NULL,
    asset_id        TEXT NOT NULL,
    field           TEXT NOT NULL,
    observed_creator TEXT NOT NULL,
    observed_counter INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_obs_op ON op_observes(op_counter);
CREATE TABLE IF NOT EXISTS field_state (
    supplier TEXT NOT NULL,
    asset_id TEXT NOT NULL,
    field    TEXT NOT NULL,
    creator  TEXT NOT NULL,
    counter  INTEGER NOT NULL,              -- creator's coordinate counter
    value    TEXT,                          -- NULL means the known value null
    live     INTEGER NOT NULL,
    PRIMARY KEY (supplier, asset_id, field, creator)
);
CREATE TABLE IF NOT EXISTS dead_watermark (
    supplier     TEXT NOT NULL,
    asset_id     TEXT NOT NULL,
    field        TEXT NOT NULL,
    creator      TEXT NOT NULL,
    dead_counter INTEGER NOT NULL,
    -- value of the version at dead_counter, retained so the remove can be
    -- re-exported as a self-contained (add, remove) pair.
    dead_value   TEXT,
    -- compact row-level causal summary: JSON list of [counter, value] for
    -- dead coordinates <= dead_counter that may still be needed to release
    -- a peer's out-of-order buffered remove. One row per touched
    -- (row, field, creator), so size stays key-bounded, not op-bounded.
    dead_blob    TEXT,
    PRIMARY KEY (supplier, asset_id, field, creator)
);
CREATE TABLE IF NOT EXISTS replica_frontier (
    supplier  TEXT NOT NULL,
    replica   TEXT NOT NULL,
    frontier_counter INTEGER NOT NULL,
    PRIMARY KEY (supplier, replica)
);
CREATE TABLE IF NOT EXISTS pending_kills (
    supplier TEXT NOT NULL,
    asset_id TEXT NOT NULL,
    field    TEXT NOT NULL,
    creator  TEXT NOT NULL,
    counter  INTEGER NOT NULL,
    PRIMARY KEY (supplier, asset_id, field, creator, counter)
);
CREATE TABLE IF NOT EXISTS checkpoints (
    checkpoint_id TEXT PRIMARY KEY,
    supplier      TEXT NOT NULL,
    snapshot      TEXT NOT NULL
);
"""

Version = Dict[str, Any]


class MergeError(RuntimeError):
    pass


class Replica:
    """One independently persistent master-data replica."""

    # Max dead-coordinate facts retained per (row, field, creator) blob after
    # compaction. Bounds the row-level causal summary independently of the
    # number of historical operations; the prefix watermark tip is always kept.
    _blob_keep = 64

    # ------------------------------------------------------------------ open
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA busy_timeout=5000")
        self.conn.executescript(SCHEMA)
        self.conn.commit()
        rid = self.conn.execute(
            "SELECT value FROM meta WHERE key='replica_id'").fetchone()
        if rid is None:
            raise MergeError(
                f"{db_path} is not an initialized replica store")
        self._replica_id = rid[0]

    @classmethod
    def create(cls, db_path: str, replica_id: Optional[str] = None) -> "Replica":
        replica_id = replica_id or str(uuid.uuid4())
        protocol._check_id(replica_id, "replica_id")
        d = os.path.dirname(os.path.abspath(db_path))
        os.makedirs(d, exist_ok=True)
        conn = sqlite3.connect(db_path)
        try:
            conn.executescript(SCHEMA)
            conn.execute("INSERT INTO meta(key,value) VALUES('replica_id',?)",
                         (replica_id,))
            conn.execute("INSERT INTO meta(key,value) VALUES('counter', '0')")
            conn.commit()
        except sqlite3.IntegrityError as exc:
            conn.close()
            raise MergeError(f"cannot create replica at {db_path}: {exc}")
        conn.close()
        return cls(db_path)

    @property
    def replica_id(self) -> str:
        return self._replica_id

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> "Replica":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # ------------------------------------------------------------- primitives
    def _next_counter(self) -> int:
        cur = self.conn.execute(
            "SELECT value FROM meta WHERE key='counter'")
        nxt = int(cur.fetchone()[0]) + 1
        self.conn.execute(
            "UPDATE meta SET value=? WHERE key='counter'", (str(nxt),))
        return nxt

    def _live_versions(self, supplier: str, asset_id: str,
                       fields: Optional[Sequence[str]] = None) -> List[Version]:
        q = ("SELECT field, creator, counter, value FROM field_state "
             "WHERE supplier=? AND asset_id=? AND live=1")
        args: List[Any] = [supplier, asset_id]
        if fields is not None:
            q += " AND field IN (%s)" % ",".join("?" * len(fields))
            args.extend(fields)
        return [{"field": f, "creator": c, "counter": n, "value": v}
                for f, c, n, v in self.conn.execute(q, args)]

    def _apply_kill(self, supplier: str, asset_id: str, field: str,
                    creator: str, counter: int,
                    value: Optional[str] = None) -> None:
        """Record a prefix tombstone, folding the backing add into one row.

        Per (row, field, creator) this keeps the highest dead counter plus a
        compact JSON blob of dead [counter, value] coordinates, so every
        remove can be re-exported as a self-contained (add, remove) pair and
        a peer's out-of-order buffered remove can always be released. One row
        per touched key keeps storage bounded independently of op count.
        """
        if value is None:
            row = self.conn.execute(
                "SELECT value FROM field_state WHERE supplier=? AND asset_id=? "
                "AND field=? AND creator=? AND counter=?",
                (supplier, asset_id, field, creator, counter)).fetchone()
            if row is not None:
                value = row[0]
        existing = self.conn.execute(
            "SELECT dead_counter,dead_blob FROM dead_watermark "
            "WHERE supplier=? AND asset_id=? AND field=? AND creator=?",
            (supplier, asset_id, field, creator)).fetchone()
        blob = []
        if existing is not None and existing[1]:
            blob = json.loads(existing[1])
        blob = [e for e in blob if e[0] != counter]
        blob.append([counter, value])
        blob.sort(key=lambda e: e[0])
        self.conn.execute(
            """INSERT INTO dead_watermark
               (supplier,asset_id,field,creator,dead_counter,dead_value,
                dead_blob)
               VALUES (?,?,?,?,?,?,?)
               ON CONFLICT(supplier,asset_id,field,creator) DO UPDATE SET
                 dead_counter=MAX(dead_counter, excluded.dead_counter),
                 dead_value=CASE WHEN excluded.dead_counter>=dead_counter
                                 THEN excluded.dead_value ELSE dead_value END,
                 dead_blob=excluded.dead_blob""",
            (supplier, asset_id, field, creator, counter, value,
             json.dumps(blob, sort_keys=True)))

    def _apply_add(self, supplier: str, asset_id: str, field: str,
                   creator: str, counter: int, value: Optional[str]) -> None:
        # Record the add fact unconditionally first; liveness is derived
        # from the watermark at merge commit time, never cached on the row.
        # This makes add-before-kill and kill-before-add order-independent.
        row = self.conn.execute(
            "SELECT counter FROM field_state "
            "WHERE supplier=? AND asset_id=? AND field=? AND creator=?",
            (supplier, asset_id, field, creator)).fetchone()
        if row is None:
            self.conn.execute(
                "INSERT INTO field_state"
                "(supplier,asset_id,field,creator,counter,value,live)"
                " VALUES (?,?,?,?,?,?,"
                "  CASE WHEN ? > COALESCE((SELECT dead_counter FROM "
                "        dead_watermark WHERE supplier=? AND asset_id=? "
                "        AND field=? AND creator=?),0) THEN 1 ELSE 0 END)",
                (supplier, asset_id, field, creator, counter, value,
                 counter, supplier, asset_id, field, creator))
        elif counter > row[0]:
            self.conn.execute(
                """UPDATE field_state SET counter=?, value=?,
                   live = CASE WHEN ? > COALESCE((SELECT dead_counter FROM
                       dead_watermark WHERE supplier=? AND asset_id=?
                       AND field=? AND creator=?),0) THEN 1 ELSE 0 END
                   WHERE supplier=? AND asset_id=? AND field=? AND creator=?""",
                (counter, value, counter, supplier, asset_id, field, creator,
                 supplier, asset_id, field, creator))
        # equal/older coordinates: idempotent duplicate or superseded chain

    def _recompute_liveness(self, supplier: str) -> None:
        """Derive every live flag from watermarks after adds+kills settle."""
        self.conn.execute(
            """UPDATE field_state SET live =
                 CASE WHEN counter > COALESCE((
                        SELECT dead_counter FROM dead_watermark w
                        WHERE w.supplier=field_state.supplier
                          AND w.asset_id=field_state.asset_id
                          AND w.field=field_state.field
                          AND w.creator=field_state.creator),0)
                      THEN 1 ELSE 0 END
               WHERE supplier=?""", (supplier,))

    def _advance_frontier(self, supplier: str, replica: str, counter: int) -> None:
        self.conn.execute(
            """INSERT INTO replica_frontier(supplier,replica,frontier_counter)
               VALUES (?,?,?)
               ON CONFLICT(supplier,replica) DO UPDATE SET
               frontier_counter=MAX(frontier_counter, excluded.frontier_counter)""",
            (supplier, replica, counter))

    # ----------------------------------------------------------- local batch
    def import_batch(self, ops: List[Dict[str, Any]]) -> List[int]:
        """Apply one site batch atomically; later ops see earlier ops."""
        norm = protocol.validate_batch(
            {"format": protocol.BATCH_FORMAT,
             "protocol_version": protocol.PROTOCOL_VERSION, "ops": ops})
        counters: List[int] = []
        try:
            self.conn.execute("BEGIN")
            for op in norm["ops"]:
                counter = self._next_counter()
                counters.append(counter)
                s, a = op["supplier"], op["asset_id"]
                if op["op"] == "put":
                    fields = list(op["fields"].keys())
                    observed = self._live_versions(s, a, fields)
                    self.conn.execute(
                        "INSERT INTO ops(counter,supplier,asset_id,op,fields)"
                        " VALUES (?,?,?,?,?)",
                        (counter, s, a, "put",
                         json.dumps(op["fields"], sort_keys=True)))
                    for v in observed:
                        self._apply_kill(s, a, v["field"], v["creator"],
                                         v["counter"])
                        self.conn.execute(
                            "INSERT INTO op_observes(op_counter,supplier,"
                            "asset_id,field,observed_creator,observed_counter)"
                            " VALUES (?,?,?,?,?,?)",
                            (counter, s, a, v["field"], v["creator"],
                             v["counter"]))
                    for fname, fval in op["fields"].items():
                        self._apply_add(s, a, fname, self._replica_id,
                                        counter, fval)
                else:  # erase: observe every live field version of the row
                    observed = self._live_versions(s, a)
                    self.conn.execute(
                        "INSERT INTO ops(counter,supplier,asset_id,op,fields)"
                        " VALUES (?,?,?,?,NULL)",
                        (counter, s, a, "erase"))
                    for v in observed:
                        self._apply_kill(s, a, v["field"], v["creator"],
                                         v["counter"])
                        self.conn.execute(
                            "INSERT INTO op_observes(op_counter,supplier,"
                            "asset_id,field,observed_creator,observed_counter)"
                            " VALUES (?,?,?,?,?,?)",
                            (counter, s, a, v["field"], v["creator"],
                             v["counter"]))
                self._advance_frontier(s, self._replica_id, counter)
            touched = {op["supplier"] for op in norm["ops"]}
            for s in touched:
                self._recompute_liveness(s)
            self.conn.commit()
        except BaseException:
            self.conn.rollback()
            raise
        return counters

    def import_batch_file(self, path: str) -> List[int]:
        return self.import_batch(protocol.load_batch_file(path))

    # --------------------------------------------------------------- merging
    def _knows_exact(self, supplier: str, asset_id: str, field: str,
                     creator: str, counter: int) -> bool:
        """Whether the add fact for this exact coordinate is present.

        Sources: a current field_state row, the watermark tip
        (dead_counter), or a retained dead coordinate inside dead_blob.
        """
        row = self.conn.execute(
            "SELECT 1 FROM field_state WHERE supplier=? AND asset_id=? "
            "AND field=? AND creator=? AND counter=? LIMIT 1",
            (supplier, asset_id, field, creator, counter)).fetchone()
        if row is not None:
            return True
        wm = self.conn.execute(
            "SELECT dead_counter,dead_blob FROM dead_watermark "
            "WHERE supplier=? AND asset_id=? AND field=? AND creator=?",
            (supplier, asset_id, field, creator)).fetchone()
        if wm is None:
            return False
        if wm[0] == counter:
            return True
        if wm[1]:
            return any(coord[0] == counter for coord in json.loads(wm[1]))
        return False

    def _remote_kill(self, supplier: str, asset_id: str, field: str,
                     creator: str, counter: int) -> None:
        """Buffer a remove observed by another replica.

        Every remote remove goes through pending_kills first; release is
        decided solely by _drain_pending against exact add facts. This keeps
        distinct coordinates of one chain independent -- a tombstone for a
        high counter can never ride in on a lower one's add.
        """
        self.conn.execute(
            "INSERT OR IGNORE INTO pending_kills"
            "(supplier,asset_id,field,creator,counter) "
            "VALUES (?,?,?,?,?)",
            (supplier, asset_id, field, creator, counter))

    def _drain_pending(self, supplier: str,
                       add_values: Optional[Dict[Any, Any]] = None) -> int:
        """Release buffered removes whose exact add fact has arrived.

        Each buffered coordinate is judged independently: a pending kill of
        N is applied only when the add fact N is present as a current
        field_state row, as a folded watermark value, or in the adds of the
        package currently being merged. No collapsing across counters -- a
        high tombstone cannot ride in on a different version's add.
        """
        add_values = add_values or {}
        rows = self.conn.execute(
            "SELECT DISTINCT asset_id,field,creator,counter "
            "FROM pending_kills WHERE supplier=?", (supplier,)).fetchall()
        drained = 0
        for asset_id, field, creator, counter in rows:
            key = (supplier, asset_id, field, creator, counter)
            if key in add_values:
                self._apply_kill(supplier, asset_id, field, creator, counter,
                                 value=add_values[key])
                self.conn.execute(
                    "DELETE FROM pending_kills WHERE supplier=? AND asset_id=? "
                    "AND field=? AND creator=? AND counter=?",
                    (supplier, asset_id, field, creator, counter))
                drained += 1
            elif self._knows_exact(supplier, asset_id, field, creator,
                                   counter):
                self._apply_kill(supplier, asset_id, field, creator, counter)
                self.conn.execute(
                    "DELETE FROM pending_kills WHERE supplier=? AND asset_id=? "
                    "AND field=? AND creator=? AND counter=?",
                    (supplier, asset_id, field, creator, counter))
                drained += 1
        return drained

    def merge_package(self, pkg: Dict[str, Any]) -> Dict[str, int]:
        """Integrate one validated handoff package atomically.

        Idempotent and order-independent: duplicates, third-party forwards
        and delta-before-checkpoint delivery all converge identically.
        Only the package's supplier partition is touched.
        """
        pkg = protocol.validate_handoff(pkg)
        supplier = pkg["supplier"]
        adds, kills = pkg["adds"], pkg["kills"]
        crash_at = os.environ.get("MDM_CRASH_AT")
        steps = 0
        try:
            self.conn.execute("BEGIN")
            # Integrate the causal frontier first: it is an independent union
            # summary and decides which buffered tombstones are reachable.
            if pkg["kind"] == protocol.KIND_CHECKPOINT:
                # Full checkpoint: frontier is a complete causal summary.
                for rid, cnt in pkg["frontier"].items():
                    self._advance_frontier(supplier, rid, cnt)
            else:
                # Delta: only the sender's own chain is guaranteed complete.
                own = pkg["frontier"].get(pkg["replica_id"], 0)
                if own:
                    self._advance_frontier(supplier, pkg["replica_id"], own)
            # Order of adds vs kills does not matter: a kill whose target
            # add fact is absent is buffered instead of applied.
            add_values = {(r["supplier"], r["asset_id"], r["field"],
                           r["creator"], r["counter"]): r["value"] for r in adds}
            for rec in adds:
                if rec["supplier"] != supplier:
                    raise MergeError("package record escapes its supplier scope")
                self._apply_add(rec["supplier"], rec["asset_id"],
                                rec["field"], rec["creator"],
                                rec["counter"], rec["value"])
                steps += 1
                if crash_at and steps == int(crash_at):
                    self._crash()
            for rec in kills:
                if rec["supplier"] != supplier:
                    raise MergeError("package record escapes its supplier scope")
                self._remote_kill(rec["supplier"], rec["asset_id"],
                                  rec["field"], rec["creator"],
                                  rec["counter"])
                steps += 1
                if crash_at and steps == int(crash_at):
                    self._crash()
            # New adds/frontier knowledge may release buffered tombstones.
            drained = self._drain_pending(supplier, add_values)
            # Re-derive every live flag from the settled watermarks so add
            # and kill application order within the package cannot matter.
            self._recompute_liveness(supplier)
            self.conn.commit()
        except BaseException:
            self.conn.rollback()
            raise
        return {"add_records": len(adds), "kill_records": len(kills),
                "tombstones_released": drained,
                "records": len(adds) + len(kills)}

    def merge_file(self, path: str) -> Dict[str, int]:
        return self.merge_package(protocol.load_handoff_file(path))

    @staticmethod
    def _crash() -> None:
        # Hard exit mid-transaction: WAL/rollback guarantees nothing from
        # this uncommitted merge is visible after reopen.
        os._exit(37)

    # --------------------------------------------------------------- export
    def _frontier_map(self, supplier: str) -> Dict[str, int]:
        return {r: c for r, c in self.conn.execute(
            "SELECT replica, frontier_counter FROM replica_frontier "
            "WHERE supplier=?", (supplier,))}

    def _partition_snapshot(self, supplier: str) -> Dict[str, Any]:
        """Compact (row,field,creator) -> state map used as a delta basis.

        Live state carries its counter+value; dead state carries the
        watermark counter.  This is exactly the checkpoint's coverage, so a
        delta becomes a per-key diff against it.
        """
        snap: Dict[str, Any] = {"frontier": self._frontier_map(supplier),
                                "live": {}, "dead": {}}
        for s, a, f, c, n, v in self.conn.execute(
                "SELECT supplier,asset_id,field,creator,counter,value "
                "FROM field_state WHERE supplier=? AND live=1",
                (supplier,)):
            snap["live"][f"{a}\x1f{f}\x1f{c}"] = [n, v]
        for s, a, f, c, n in self.conn.execute(
                "SELECT supplier,asset_id,field,creator,dead_counter "
                "FROM dead_watermark WHERE supplier=?", (supplier,)):
            snap["dead"][f"{a}\x1f{f}\x1f{c}"] = n
        return snap

    def export_checkpoint(self, supplier: str, out_path: str) -> str:
        """Full partition checkpoint: live versions plus OR-set tombstone
        (add, remove) facts and the causal death summary.

        For every watermark key whose dead coordinate is not already
        shadowed by a strictly later *live* version of the same chain, the
        checkpoint also carries the dead add fact. A peer that never saw
        that add still receives both the add and its remove, so the deletion
        can be applied without ever resurrecting the value.
        """
        adds = [{"supplier": s, "asset_id": a, "field": f, "creator": c,
                 "counter": n, "value": v}
                for s, a, f, c, n, v in self.conn.execute(
                    "SELECT supplier,asset_id,field,creator,counter,value "
                    "FROM field_state WHERE supplier=? AND live=1 "
                    "ORDER BY asset_id,field,creator", (supplier,))]
        kills = [{"supplier": s, "asset_id": a, "field": f, "creator": c,
                  "counter": n}
                 for s, a, f, c, n in self.conn.execute(
                     "SELECT supplier,asset_id,field,creator,dead_counter "
                     "FROM dead_watermark WHERE supplier=? "
                     "ORDER BY asset_id,field,creator", (supplier,))]
        # Backing add facts make every remove self-contained. The watermark
        # tip add is emitted unless shadowed by a strictly later live chain
        # version; every retained dead_blob coordinate is also emitted so a
        # peer buffering an out-of-order remove can match its exact add.
        live_chain: Dict[Tuple[str, str, str, str], int] = {}
        for r in adds:
            k = (r["supplier"], r["asset_id"], r["field"], r["creator"])
            live_chain[k] = max(live_chain.get(k, 0), r["counter"])
        add_keys = {(r["supplier"], r["asset_id"], r["field"], r["creator"],
                     r["counter"]) for r in adds}

        def emit_add(s, a, f, c, n, v):
            key = (s, a, f, c, n)
            if key not in add_keys:
                adds.append({"supplier": s, "asset_id": a, "field": f,
                             "creator": c, "counter": n, "value": v})
                add_keys.add(key)

        for s, a, f, c, n, v, blob in self.conn.execute(
                "SELECT supplier,asset_id,field,creator,dead_counter,"
                "dead_value,dead_blob FROM dead_watermark WHERE supplier=?",
                (supplier,)):
            if live_chain.get((s, a, f, c), 0) <= n:
                emit_add(s, a, f, c, n, v)       # watermark tip add
            if blob:
                for dn, dv in json.loads(blob):  # retained dead coordinates
                    emit_add(s, a, f, c, dn, dv)
        cid = f"cp-{uuid.uuid4().hex}"
        pkg = protocol.build_handoff(
            kind=protocol.KIND_CHECKPOINT, replica_id=self._replica_id,
            supplier=supplier, frontier=self._frontier_map(supplier),
            adds=adds, kills=kills, checkpoint_id=cid)
        protocol.write_json_atomic(out_path, pkg)
        # Remember the exported coverage so later deltas diff against the
        # exact bytes the peer received -- not against a frontier guess.
        self.conn.execute(
            "INSERT OR REPLACE INTO checkpoints(checkpoint_id,supplier,snapshot)"
            " VALUES (?,?,?)",
            (cid, supplier,
             json.dumps(self._partition_snapshot(supplier), sort_keys=True)))
        self.conn.commit()
        return cid

    def export_delta(self, supplier: str, baseline: Dict[str, Any],
                     out_path: str) -> str:
        """Delta against an already exported checkpoint.

        Contains only post-baseline changes: newer live versions, newly dead
        keys and raised death watermarks -- never the whole unchanged
        partition.  The baseline must be a checkpoint this replica exported,
        so its exact pre-change coverage is on record.
        """
        base = protocol.validate_handoff(baseline)
        if base["kind"] != protocol.KIND_CHECKPOINT or \
                base["supplier"] != supplier:
            raise MergeError("delta baseline must be a checkpoint of the "
                             "same supplier")
        row = self.conn.execute(
            "SELECT snapshot FROM checkpoints WHERE checkpoint_id=?",
            (base["checkpoint_id"],)).fetchone()
        if row is None:
            raise MergeError(
                f"baseline {base['checkpoint_id']} was not exported by this "
                "replica; build the delta on the site that made the "
                "checkpoint")
        old = json.loads(row[0])
        cur = self._partition_snapshot(supplier)
        adds, kills = [], []
        add_seen = set()

        def add_add(rec):
            key = (rec["supplier"], rec["asset_id"], rec["field"],
                   rec["creator"], rec["counter"])
            if key not in add_seen:
                adds.append(rec)
                add_seen.add(key)

        def split_key(k: str):
            asset_id, field, creator = k.split("\x1f")
            return asset_id, field, creator

        for k, state in cur["live"].items():
            n, v = state
            if old["live"].get(k) != state:       # new or changed live version
                asset_id, field, creator = split_key(k)
                add_add({"supplier": supplier, "asset_id": asset_id,
                         "field": field, "creator": creator,
                         "counter": n, "value": v})
        for k, n in cur["dead"].items():
            old_n = old["dead"].get(k)
            if old_n is None or n > old_n:        # new tombstone / higher mark
                asset_id, field, creator = split_key(k)
                kills.append({"supplier": supplier, "asset_id": asset_id,
                              "field": field, "creator": creator,
                              "counter": n})
                # Carry the single folded backing add for this watermark,
                # unless a strictly later live version of the same chain is
                # already in this delta (it causally contains the dead one).
                shadowed = any(
                    r["asset_id"] == asset_id and r["field"] == field
                    and r["creator"] == creator and r["counter"] > n
                    for r in adds)
                if not shadowed:
                    wm = self.conn.execute(
                        "SELECT dead_counter,dead_value FROM dead_watermark "
                        "WHERE supplier=? AND asset_id=? AND field=? "
                        "AND creator=?",
                        (supplier, asset_id, field, creator)).fetchone()
                    if wm is not None:
                        add_add({"supplier": supplier,
                                 "asset_id": asset_id, "field": field,
                                 "creator": creator,
                                 "counter": wm[0], "value": wm[1]})
        pkg = protocol.build_handoff(
            kind=protocol.KIND_DELTA, replica_id=self._replica_id,
            supplier=supplier, frontier=cur["frontier"],
            adds=adds, kills=kills,
            checkpoint_id=f"dl-{uuid.uuid4().hex}",
            baseline={"checkpoint_id": base["checkpoint_id"],
                      "frontier": old["frontier"]})
        protocol.write_json_atomic(out_path, pkg)
        return pkg["checkpoint_id"]

    # ------------------------------------------------------------- compaction
    def compact(self) -> Dict[str, int]:
        """Collapse operation provenance into causal summaries.

        After compaction the store keeps only live competing versions,
        per-key death watermarks and frontiers -- the same information a
        checkpoint carries.  Re-running any old package stays correct
        because watermarks make dead versions born-dead on arrival.

        Dead ``field_state`` rows are deliberately retained: they are the
        add facts that make every carried remove applicable on a peer that
        has never seen the add (the OR-set (add, remove) pair). Their count
        is bounded by touched (row, field, creator) keys, never by operation
        count, so steady-state size stays independent of history length.
        """
        removed = {"ops": 0, "observes": 0, "blob_coords": 0,
                   "checkpoint_summaries": 0}
        try:
            self.conn.execute("BEGIN")
            removed["ops"] = self.conn.execute("DELETE FROM ops").rowcount
            removed["observes"] = self.conn.execute(
                "DELETE FROM op_observes").rowcount
            # Bound each key's dead-coordinate blob: keep the newest K facts.
            # Older dead coords are covered by the prefix watermark once
            # their add has propagated; peers still buffering a very old
            # remove converge on the next full checkpoint.
            rows = self.conn.execute(
                "SELECT supplier,asset_id,field,creator,dead_blob "
                "FROM dead_watermark WHERE dead_blob IS NOT NULL").fetchall()
            keep = self._blob_keep
            for s, a, f, c, raw in rows:
                blob = json.loads(raw)
                if len(blob) > keep:
                    pruned = blob[-keep:]
                    removed["blob_coords"] += len(blob) - len(pruned)
                    self.conn.execute(
                        "UPDATE dead_watermark SET dead_blob=? "
                        "WHERE supplier=? AND asset_id=? AND field=? "
                        "AND creator=?",
                        (json.dumps(pruned, sort_keys=True), s, a, f, c))
            # Bound baseline snapshots too: keep only the newest exported
            # checkpoint per supplier, so this table cannot grow without
            # limit. A delta against a pruned baseline is refused with a
            # MergeError and the caller exchanges a full checkpoint instead.
            removed["checkpoint_summaries"] = self.conn.execute(
                """DELETE FROM checkpoints
                   WHERE checkpoint_id NOT IN (
                     SELECT checkpoint_id FROM (
                       SELECT checkpoint_id,
                         ROW_NUMBER() OVER (
                           PARTITION BY supplier ORDER BY rowid DESC) rn
                       FROM checkpoints) WHERE rn = 1)"""
            ).rowcount
            self.conn.commit()
        except BaseException:
            self.conn.rollback()
            raise
        self.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        self.conn.execute("VACUUM")
        return removed

    # ---------------------------------------------------------------- reads
    def read_row(self, supplier: str, asset_id: str) -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "replica_id": self._replica_id, "supplier": supplier,
            "asset_id": asset_id, "exists": False, "fields": {}}
        for field in protocol.VALID_FIELDS:
            versions = [
                {"value": v, "origin": [c, n], "creator": c, "counter": n}
                for c, n, v in self.conn.execute(
                    "SELECT creator,counter,value FROM field_state "
                    "WHERE supplier=? AND asset_id=? AND field=? AND live=1 "
                    "ORDER BY creator,counter",
                    (supplier, asset_id, field))]
            if not versions:
                status = "missing"
            elif len(versions) == 1:
                status = "unique"
                out["exists"] = True
            else:
                status = "conflict"
                out["exists"] = True
            out["fields"][field] = {"status": status, "versions": versions}
        return out

    def read_partition(self, supplier: str) -> List[Dict[str, Any]]:
        keys = [r for r in self.conn.execute(
            "SELECT DISTINCT asset_id FROM field_state "
            "WHERE supplier=? AND live=1 ORDER BY asset_id", (supplier,))]
        return [self.read_row(supplier, a) for (a,) in keys]

    def list_suppliers(self) -> List[str]:
        return [r[0] for r in self.conn.execute(
            "SELECT DISTINCT supplier FROM field_state WHERE live=1 "
            "UNION SELECT DISTINCT supplier FROM dead_watermark ORDER BY 1")]

    # ---------------------------------------------------------------- stats
    def stats(self) -> Dict[str, int]:
        def count(table: str) -> int:
            return self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        page_cnt = self.conn.execute("PRAGMA page_count").fetchone()[0]
        page_sz = self.conn.execute("PRAGMA page_size").fetchone()[0]
        return {
            "replica_id": self._replica_id,
            "live_versions": self.conn.execute(
                "SELECT COUNT(*) FROM field_state WHERE live=1").fetchone()[0],
            "field_state_entries": count("field_state"),
            "watermark_entries": count("dead_watermark"),
            "pending_kill_entries": count("pending_kills"),
            "frontier_entries": count("replica_frontier"),
            "checkpoint_summaries": count("checkpoints"),
            "local_ops": count("ops"),
            "observe_edges": count("op_observes"),
            "page_count": page_cnt,
            "page_size": page_sz,
            "db_bytes": page_cnt * page_sz,
        }
