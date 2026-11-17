"""Resumable downstream subscription outlet for a replica database.

This module consumes the *committed visible results* of a replica -- the
output of local ``import_batch`` transactions and remote package merges --
and exposes them as a per-supplier change stream with a resumable initial
load. It never treats site-to-site handoff packages as downstream events:
the wire formats here (``mdm-sub-load/v1``, ``mdm-sub-event/v1``) are a
separate, explicitly versioned protocol.

Two distinct coordinate spaces coexist on purpose:

* **causal source coordinates** ``(creator, counter)`` identify *where a
  field version came from* and are exchanged in handoff packages;
* **subscriptions positions** ``(supplier, seq)`` identify *when the local
  replica committed a visible change*. A seq is assigned only when a commit
  actually changes a live version set, in local commit order, so a very old
  package arriving today gets today's seq. They are never interchangeable.

All stream writes happen inside the same SQLite transaction as the business
write (see ``publish_changes``); a crash or a rejected input therefore can
never leave a half event or move a position without its content.

Storage is reclaimable: events and load copies are retained only while at
least one live subscription still needs them. Releasing a subscription is
the operator's explicit "I accept a future full reload" action; cleanup
never silently drops protected data, and a cursor whose history has been
reclaimed fails with an explicit ``reload required`` error instead of
silently continuing from the earliest surviving event.
"""

from __future__ import annotations

import base64
import datetime
import json
import os
import sqlite3
import uuid
from typing import Any, Dict, List, Optional, Sequence

from . import protocol

SUB_PROTOCOL_VERSION = 1
LOAD_FORMAT = "mdm-sub-load/v1"
EVENT_FORMAT = "mdm-sub-event/v1"
TOKEN_FORMAT = "mdm-sub-token/v1"
VALID_FIELDS = protocol.VALID_FIELDS

STATE_LOADING = "loading"
STATE_LIVE = "live"
STATE_RELEASED = "released"

# Soft page limits; a single change transaction larger than an event page is
# delivered whole (documented in SUBSCRIPTIONS.md).
DEFAULT_LOAD_PAGE = 1000
DEFAULT_EVENT_PAGE = 100


class SubscriptionError(RuntimeError):
    """Raised on any invalid subscription/token/ack/stream operation."""


# --------------------------------------------------------------------------
# change publication -- called inside the business write transaction
# --------------------------------------------------------------------------

def _now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def touched_assets(ops: Sequence[Dict[str, Any]]) -> Dict[str, List[str]]:
    """Map supplier -> distinct asset_ids a validated local batch may touch."""
    out: Dict[str, List[str]] = {}
    for op in ops:
        out.setdefault(op["supplier"], [])
        if op["asset_id"] not in out[op["supplier"]]:
            out[op["supplier"]].append(op["asset_id"])
    return out


def touched_assets_records(supplier: str, adds: Sequence[Dict[str, Any]],
                           kills: Sequence[Dict[str, Any]]
                           ) -> Dict[str, List[str]]:
    """Map supplier -> distinct asset_ids named by a handoff package."""
    assets: List[str] = []
    for rec in (*adds, *kills):
        if rec["supplier"] != supplier:
            # core.merge_package already rejects scope escapes; stay
            # defensive so a stream never spans two partitions.
            raise SubscriptionError("package record escapes its supplier "
                                    "scope")
        if rec["asset_id"] not in assets:
            assets.append(rec["asset_id"])
    return {supplier: assets}


def snapshot_live(conn: sqlite3.Connection,
                  touched: Dict[str, List[str]]
                  ) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """Full live version sets of the touched rows at a transaction boundary.

    Returns ``{supplier: {asset_id: {field: [version, ...]}}}`` where every
    version is ``{"creator", "counter", "value"}`` with ``value`` being a
    string or JSON ``None`` (the known null). Missing fields are simply
    absent from the field map; a row with no live versions is absent as a key
    -- so null, missing-field and row-deletion stay distinguishable.
    """
    out: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for supplier, assets in touched.items():
        part: Dict[str, Dict[str, Any]] = {}
        if not assets:
            out[supplier] = part
            continue
        marks = ",".join("?" * len(assets))
        rows = conn.execute(
            f"SELECT asset_id,field,creator,counter,value FROM field_state "
            f"WHERE supplier=? AND live=1 AND asset_id IN ({marks})",
            [supplier, *assets]).fetchall()
        for asset_id, field, creator, counter, value in rows:
            row = part.setdefault(asset_id, {})
            row.setdefault(field, []).append(
                {"field": field, "creator": creator, "counter": counter,
                 "value": value})
        for fields in part.values():
            for versions in fields.values():
                versions.sort(key=lambda v: (v["creator"], v["counter"]))
        out[supplier] = part
    return out


def _crash(tag: str) -> None:
    point = os.environ.get("MDM_CRASH_AT")
    want = os.environ.get("MDM_CRASH_TAG")
    if point and want == tag:
        os._exit(37)


def publish_changes(conn: sqlite3.Connection,
                    touched: Dict[str, List[str]],
                    before: Dict[str, Dict[str, Dict[str, Any]]],
                    kind: str, crash_tag: str = "event") -> int:
    """Publish per-partition before/after change events in this txn.

    One event per *supplier partition* changed by the transaction, even when
    several rows changed: downstream consumers never see half a transaction.
    A partition whose committed live sets are identical to its pre-txn
    boundary (write-then-delete within the txn, a duplicate package, a merge
    that only advanced causal knowledge) gets no event and no seq -- positions
    stay gap-free and reads/compaction can never manufacture changes.

    Ledger growth is per partition: an event is stored for a supplier only
    while a live (loading/live) subscription on that exact supplier exists.
    Partitions nobody subscribes to stay storage-bounded, and a subscription
    created later obtains their current rows through its own cut-point load.
    """
    after = snapshot_live(conn, touched)
    published = 0
    for supplier in sorted(touched):
        b, a = before.get(supplier, {}), after.get(supplier, {})
        assets = sorted(set(b) | set(a))
        changes: List[Dict[str, Any]] = []
        for asset_id in assets:
            rb, ra = b.get(asset_id), a.get(asset_id)
            # Equality compares full per-field version lists including the
            # source identity of every version: same text with a different
            # creator/counter is a change; the key being present vs absent is
            # row creation/deletion; a field key present vs absent is the
            # missing-field distinction; an explicit null is a normal value.
            if rb != ra:
                changes.append({"asset_id": asset_id,
                                "before": rb or None,
                                "after": ra or None})
        if not changes:
            continue
        # Per-partition protection: a partition grows its event ledger only
        # while a live (loading or live) subscription on THAT supplier
        # exists. A batch spanning X and Y with only X subscribers publishes
        # nothing for Y; a later Y subscription starts from its own cut-point
        # load and never needs these earlier events. This keeps an
        # unsubscribed partition storage-bounded exactly like the rest of
        # the store.
        active = conn.execute(
            "SELECT 1 FROM sub_subscriptions WHERE supplier=? "
            "AND state IN (?,?) LIMIT 1",
            (supplier, STATE_LOADING, STATE_LIVE)).fetchone()
        if active is None:
            continue
        seq_row = conn.execute(
            "SELECT latest_seq FROM sub_partition WHERE supplier=?",
            (supplier,)).fetchone()
        seq = (seq_row[0] + 1) if seq_row else 1
        payload = json.dumps(
            {"format": EVENT_FORMAT,
             "protocol_version": SUB_PROTOCOL_VERSION,
             "supplier": supplier, "seq": seq, "txn_kind": kind,
             "committed_at": _now(), "changes": changes},
            sort_keys=True, ensure_ascii=False)
        conn.execute(
            "INSERT INTO sub_events(supplier,seq,txn_kind,payload) "
            "VALUES (?,?,?,?)", (supplier, seq, kind, payload))
        if seq_row is None:
            conn.execute(
                "INSERT INTO sub_partition(supplier,earliest_seq,latest_seq)"
                " VALUES (?,?,?)", (supplier, seq, seq))
        else:
            conn.execute(
                "UPDATE sub_partition SET latest_seq=? WHERE supplier=?",
                (seq, supplier))
        _crash(crash_tag)
        published += 1
    return published


# --------------------------------------------------------------------------
# self-describing, digest-signed tokens and page documents
# --------------------------------------------------------------------------

def _b64e(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii")


def _b64d(text: str) -> bytes:
    return base64.urlsafe_b64decode(text.encode("ascii"))


def encode_token(doc: Dict[str, Any]) -> str:
    doc = {"format": TOKEN_FORMAT,
           "protocol_version": SUB_PROTOCOL_VERSION, **doc}
    body = _b64e(protocol.canonical(doc))
    digest = protocol.compute_digest(doc)
    return f"{body}.{digest}"


def decode_token(token: Any, expect_purpose: str) -> Dict[str, Any]:
    if not isinstance(token, str) or token.count(".") != 1:
        raise SubscriptionError("malformed page token")
    body, digest = token.split(".")
    try:
        doc = json.loads(_b64d(body))
    except (ValueError, json.JSONDecodeError) as exc:
        raise SubscriptionError("malformed page token") from exc
    if not isinstance(doc, dict) or doc.get("format") != TOKEN_FORMAT \
            or doc.get("protocol_version") != SUB_PROTOCOL_VERSION:
        raise SubscriptionError("unsupported/unknown token format/version")
    if protocol.compute_digest(doc) != digest:
        raise SubscriptionError("page token digest mismatch: token is "
                                "corrupted or was not issued by this outlet")
    if doc.get("purpose") != expect_purpose:
        raise SubscriptionError(
            f"token purpose {doc.get('purpose')!r} cannot be used here "
            f"(expected {expect_purpose!r})")
    return doc


def sign_page(doc: Dict[str, Any]) -> Dict[str, Any]:
    doc = {**doc, "digest": protocol.compute_digest(doc)}
    return validate_page(doc, doc["format"])


def validate_page(doc: Any, expected_format: Optional[str] = None
                  ) -> Dict[str, Any]:
    """Strict shape/type/digest validation for both downstream page kinds."""
    if not isinstance(doc, dict):
        raise SubscriptionError("page must be a JSON object")
    fmt = doc.get("format")
    if expected_format is not None and fmt != expected_format:
        raise SubscriptionError(f"unsupported page format: {fmt!r}")
    if fmt not in (LOAD_FORMAT, EVENT_FORMAT):
        raise SubscriptionError(f"unknown page format: {fmt!r}")
    if doc.get("protocol_version") != SUB_PROTOCOL_VERSION:
        raise SubscriptionError("unknown subscription protocol_version")
    if not isinstance(doc.get("digest"), str) or len(doc["digest"]) != 64:
        raise SubscriptionError("missing/malformed page digest")
    if protocol.compute_digest(doc) != doc["digest"]:
        raise SubscriptionError("content digest mismatch: page corrupted or "
                                "modified in transit")
    for key in ("db_uid", "supplier", "sub_id"):
        if not isinstance(doc.get(key), str) or not doc[key]:
            raise SubscriptionError(f"page field {key!r} missing or invalid")

    def check_version(v: Any, where: str) -> None:
        if not isinstance(v, dict):
            raise SubscriptionError(f"{where} must be an object")
        if v.get("field") not in VALID_FIELDS:
            raise SubscriptionError(f"{where}.field invalid")
        if not isinstance(v.get("creator"), str) or not v["creator"]:
            raise SubscriptionError(f"{where}.creator invalid")
        if not isinstance(v.get("counter"), int) or isinstance(v["counter"],
                                                                bool) \
                or v["counter"] < 1:
            raise SubscriptionError(f"{where}.counter must be int >= 1")
        if v.get("value") is not None and not isinstance(v["value"], str):
            raise SubscriptionError(f"{where}.value must be string or null")

    if fmt == LOAD_FORMAT:
        if not isinstance(doc.get("cut_seq"), int) or doc["cut_seq"] < 0:
            raise SubscriptionError("load page cut_seq must be int >= 0")
        if not isinstance(doc.get("page_index"), int) \
                or doc["page_index"] < 0:
            raise SubscriptionError("load page page_index must be int >= 0")
        if not isinstance(doc.get("page_size"), int) or doc["page_size"] < 1:
            raise SubscriptionError("load page page_size must be int >= 1")
        if not isinstance(doc.get("total_rows"), int) \
                or doc["total_rows"] < 0:
            raise SubscriptionError("load page total_rows must be int >= 0")
        rows = doc.get("rows")
        if not isinstance(rows, list):
            raise SubscriptionError("load page rows must be a list")
        for i, r in enumerate(rows):
            if not isinstance(r, dict) or not isinstance(
                    r.get("asset_id"), str) or not r["asset_id"]:
                raise SubscriptionError(f"rows[{i}].asset_id invalid")
            if not isinstance(r.get("versions"), list) or not r["versions"]:
                raise SubscriptionError(
                    f"rows[{i}].versions must be a non-empty list")
            for j, v in enumerate(r["versions"]):
                check_version(v, f"rows[{i}].versions[{j}]")
    else:
        for key in ("begin_seq", "end_seq", "next_seq"):
            if not isinstance(doc.get(key), int) or doc[key] < 0:
                raise SubscriptionError(f"event page {key} must be int >= 0")
        if doc["begin_seq"] > doc["end_seq"] + 1:
            raise SubscriptionError("event page begin/end range invalid")
        events = doc.get("events")
        if not isinstance(events, list):
            raise SubscriptionError("event page events must be a list")
        seqs = []
        for i, ev in enumerate(events):
            if not isinstance(ev, dict):
                raise SubscriptionError(f"events[{i}] must be an object")
            if ev.get("txn_kind") not in ("local_batch", "merge"):
                raise SubscriptionError(f"events[{i}].txn_kind invalid")
            if not isinstance(ev.get("seq"), int) or ev["seq"] < 1:
                raise SubscriptionError(f"events[{i}].seq must be int >= 1")
            changes = ev.get("changes")
            if not isinstance(changes, list) or not changes:
                raise SubscriptionError(
                    f"events[{i}].changes must be a non-empty list")
            seqs.append(ev["seq"])
            for k, ch in enumerate(changes):
                if not isinstance(ch, dict) or not isinstance(
                        ch.get("asset_id"), str) or not ch["asset_id"]:
                    raise SubscriptionError(
                        f"events[{i}].changes[{k}].asset_id invalid")
                for side in ("before", "after"):
                    snap = ch.get(side)
                    if snap is None:
                        continue
                    if not isinstance(snap, dict):
                        raise SubscriptionError(
                            f"events[{i}].changes[{k}].{side} must be an "
                            "object or null")
                    for fname, versions in snap.items():
                        if fname not in VALID_FIELDS:
                            raise SubscriptionError(
                                f"events[{i}].changes[{k}].{side}: unknown "
                                f"field {fname!r}")
                        if not isinstance(versions, list) or not versions:
                            raise SubscriptionError(
                                f"events[{i}].changes[{k}].{side}."
                                f"{fname} must be a non-empty list")
                        for j, v in enumerate(versions):
                            check_version(
                                v,
                                f"events[{i}].changes[{k}].{side}."
                                f"{fname}[{j}]")
        if seqs != sorted(seqs) or len(seqs) != len(set(seqs)):
            raise SubscriptionError("event page seqs must be unique and "
                                    "ordered")
    return doc


# --------------------------------------------------------------------------
# source-side subscription manager
# --------------------------------------------------------------------------

class SubscriptionManager:
    """Create and serve resumable subscriptions against a replica database.

    Opens its own SQLite connection: a page request never requires the
    original Python process/connection to stay alive, and every operation
    commits durably before returning. The manager can be instantiated on an
    existing (upgraded) database any number of times.
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.execute("PRAGMA busy_timeout=5000")
        # Upgrade: executing the full schema is a no-op on new tables and
        # creates the outlet tables on a pre-subscription database.
        from .core import SCHEMA
        self.conn.executescript(SCHEMA)
        self.conn.commit()
        rid = self.conn.execute(
            "SELECT value FROM meta WHERE key='replica_id'").fetchone()
        if rid is None:
            raise SubscriptionError(
                f"{db_path} is not an initialized replica store")
        self.replica_id = rid[0]
        self.db_uid = self._ensure_db_uid()

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> "SubscriptionManager":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def _ensure_db_uid(self) -> str:
        """Stable identity of this source database for cursor binding.

        Stored in ``meta`` at first use; cursors carry it so a cursor
        created against another source database (or a rebuilt store with a
        new identity) is refused outright rather than misapplied.
        """
        row = self.conn.execute(
            "SELECT value FROM meta WHERE key='db_uid'").fetchone()
        if row is not None:
            return row[0]
        uid = f"db-{uuid.uuid4().hex}"
        self.conn.execute(
            "INSERT INTO meta(key,value) VALUES('db_uid',?)", (uid,))
        self.conn.commit()
        return uid

    # ------------------------------------------------------------- create
    def create_subscription(self, supplier: str,
                            page_size: int = DEFAULT_LOAD_PAGE
                            ) -> Dict[str, Any]:
        """Establish a consistent cut point in one transaction.

        The cut sequence and the materialized load copy are taken together:
        no "scan current tables first, fetch a position afterwards" race.
        Rows inserted/deleted by another process while pages are being read
        do not affect this subscription -- every load page belongs to the
        same cut, and changes with ``seq > cut_seq`` arrive as events.
        """
        if not isinstance(supplier, str) or not supplier:
            raise SubscriptionError("supplier must be a non-empty string")
        if not isinstance(page_size, int) or isinstance(page_size, bool) \
                or page_size < 1:
            raise SubscriptionError("page_size must be a positive int")
        sub_id = f"sub-{uuid.uuid4().hex}"
        try:
            self.conn.execute("BEGIN IMMEDIATE")
            part = self.conn.execute(
                "SELECT latest_seq FROM sub_partition WHERE supplier=?",
                (supplier,)).fetchone()
            cut_seq = part[0] if part else 0
            # Materialize entirely in SQL: a 100k-row partition never passes
            # through Python memory to build its load copy.
            self.conn.execute(
                "INSERT INTO sub_load_rows"
                "(sub_id,supplier,asset_id,field,creator,counter,value) "
                "SELECT ?,supplier,asset_id,field,creator,counter,value "
                "FROM field_state WHERE supplier=? AND live=1",
                (sub_id, supplier))
            total = self.conn.execute(
                "SELECT COUNT(*) FROM sub_load_rows WHERE sub_id=?",
                (sub_id,)).fetchone()[0]
            self.conn.execute(
                "INSERT INTO sub_subscriptions"
                "(sub_id,supplier,cut_seq,state,page_size,load_total,"
                "created_at) VALUES (?,?,?,?,?,?,?)",
                (sub_id, supplier, cut_seq, STATE_LOADING, page_size,
                 total, _now()))
            self.conn.execute(
                "INSERT INTO sub_progress(sub_id,delivered_seq,confirmed_seq)"
                " VALUES (?,?,?)", (sub_id, cut_seq, cut_seq))
            self.conn.commit()
        except BaseException:
            self.conn.rollback()
            raise
        return {"sub_id": sub_id, "supplier": supplier, "db_uid": self.db_uid,
                "replica_id": self.replica_id, "cut_seq": cut_seq,
                "load_total_rows": total, "state": STATE_LOADING,
                "page_size": page_size,
                "first_load_token": self._load_token(sub_id, supplier, 0)}

    def _get_sub(self, sub_id: str) -> Dict[str, Any]:
        row = self.conn.execute(
            "SELECT sub_id,supplier,cut_seq,state,page_size,load_total "
            "FROM sub_subscriptions WHERE sub_id=?", (sub_id,)).fetchone()
        if row is None:
            raise SubscriptionError(f"unknown subscription {sub_id!r}")
        return dict(zip(("sub_id", "supplier", "cut_seq", "state",
                         "page_size", "load_total"), row))

    def _check_token_binding(self, doc: Dict[str, Any], sub_id: str,
                             supplier: str) -> None:
        if doc.get("db_uid") != self.db_uid:
            raise SubscriptionError(
                "cursor belongs to a different source database (db_uid "
                "mismatch): it cannot be replayed against this store")
        if doc.get("sub_id") != sub_id:
            raise SubscriptionError("token does not belong to this "
                                    "subscription")
        if doc.get("supplier") != supplier:
            raise SubscriptionError("cursor partition mismatch: token was "
                                    f"issued for {doc.get('supplier')!r}, "
                                    f"not {supplier!r}")

    def _load_token(self, sub_id: str, supplier: str,
                    page_index: int) -> str:
        return encode_token({"purpose": "load", "db_uid": self.db_uid,
                             "sub_id": sub_id, "supplier": supplier,
                             "page_index": page_index})

    # ----------------------------------------------------------- load pages
    def read_load_page(self, token: str) -> Dict[str, Any]:
        """Return one fixed-cut load page; retries return identical content."""
        doc = decode_token(token, "load")
        sub = self._get_sub(doc["sub_id"])
        self._check_token_binding(doc, sub["sub_id"], sub["supplier"])
        if sub["state"] == STATE_RELEASED:
            raise SubscriptionError(
                "subscription was released; its load copy is no longer "
                "retained -- create a new subscription and reload from a "
                "fresh cut point")
        page_index = doc["page_index"]
        page_size = sub["page_size"]
        offset = page_index * page_size
        if offset > sub["load_total"]:
            raise SubscriptionError(
                f"load page {page_index} is past the end of the cut "
                f"(total_rows={sub['load_total']})")
        rows_raw = self.conn.execute(
            "SELECT asset_id,field,creator,counter,value FROM sub_load_rows "
            "WHERE sub_id=? ORDER BY asset_id,field,creator,counter "
            "LIMIT ? OFFSET ?",
            (sub["sub_id"], page_size, offset)).fetchall()
        grouped: Dict[str, List[Dict[str, Any]]] = {}
        for asset_id, field, creator, counter, value in rows_raw:
            grouped.setdefault(asset_id, []).append(
                {"field": field, "creator": creator, "counter": counter,
                 "value": value})
        rows = [{"asset_id": a, "versions": grouped[a]}
                for a in sorted(grouped)]
        last = offset + len(rows_raw)
        next_token = None
        if last < sub["load_total"]:
            next_token = self._load_token(sub["sub_id"], sub["supplier"],
                                          page_index + 1)
        page = sign_page({
            "format": LOAD_FORMAT,
            "protocol_version": SUB_PROTOCOL_VERSION,
            "db_uid": self.db_uid, "replica_id": self.replica_id,
            "sub_id": sub["sub_id"], "supplier": sub["supplier"],
            "cut_seq": sub["cut_seq"], "page_index": page_index,
            "page_size": page_size, "total_rows": sub["load_total"],
            "row_count": len(rows), "rows": rows,
            "next_token": next_token})
        return page

    def ack_load_page(self, token: str) -> Dict[str, Any]:
        """Record one durable load page acknowledgement.

        Acks are per subscription: two consumers with separate subscriptions
        can never advance each other. Acknowledging a page is not the same as
        having it merely exported.
        """
        doc = decode_token(token, "load")
        sub = self._get_sub(doc["sub_id"])
        self._check_token_binding(doc, sub["sub_id"], sub["supplier"])
        if sub["state"] == STATE_RELEASED:
            raise SubscriptionError(
                "subscription released; load acks are no longer accepted -- "
                "a new subscription is required")
        page_index = doc["page_index"]
        if page_index * sub["page_size"] > sub["load_total"]:
            raise SubscriptionError("ack for a load page past the end")
        self.conn.execute(
            "INSERT OR IGNORE INTO sub_load_acks(sub_id,page_index) "
            "VALUES (?,?)", (sub["sub_id"], page_index))
        self.conn.commit()
        return {"sub_id": sub["sub_id"], "acked_page": page_index}

    def complete_load(self, sub_id: str) -> Dict[str, Any]:
        """Move a subscription from loading to live once every page is acked.

        Refuses while any page of the cut is unacknowledged, so a consumer
        cannot start consuming events over a half-applied initial view.
        """
        sub = self._get_sub(sub_id)
        if sub["state"] == STATE_RELEASED:
            raise SubscriptionError("subscription already released")
        if sub["state"] == STATE_LIVE:
            return {"sub_id": sub_id, "state": STATE_LIVE,
                    "cut_seq": sub["cut_seq"]}
        expected = 0
        if sub["load_total"]:
            expected = (sub["load_total"] - 1) // sub["page_size"] + 1
        missing = [p for p in range(expected)
                   if self.conn.execute(
                       "SELECT 1 FROM sub_load_acks WHERE sub_id=? "
                       "AND page_index=?", (sub_id, p)).fetchone() is None]
        if missing:
            raise SubscriptionError(
                f"initial load incomplete: {len(missing)} page(s) not "
                f"acknowledged (first missing: {missing[0]})")
        self.conn.execute(
            "UPDATE sub_subscriptions SET state=? WHERE sub_id=?",
            (STATE_LIVE, sub_id))
        self.conn.commit()
        return {"sub_id": sub_id, "state": STATE_LIVE,
                "cut_seq": sub["cut_seq"],
                "first_event_token": self._event_token(sub_id,
                                                       sub["cut_seq"])}

    # ---------------------------------------------------------- event pages
    def _event_token(self, sub_id: str, after_seq: int) -> str:
        return encode_token({"purpose": "event", "db_uid": self.db_uid,
                             "sub_id": sub_id,
                             "supplier": self._get_sub(sub_id)["supplier"],
                             "after_seq": after_seq})

    def read_event_page(self, token: str,
                        page_size: int = DEFAULT_EVENT_PAGE) -> Dict[str, Any]:
        """Return committed change transactions after the token's position.

        Paging is by *transaction count*, never splitting one transaction:
        a single oversized transaction is delivered whole even beyond the
        soft page limit, and a multi-row transaction can never be confirmed
        in halves. The delivered watermark only advances for events actually
        handed out here, so acks cannot skip un-delivered transactions.
        """
        if not isinstance(page_size, int) or isinstance(page_size, bool) \
                or page_size < 1:
            raise SubscriptionError("page_size must be a positive int")
        doc = decode_token(token, "event")
        sub = self._get_sub(doc["sub_id"])
        self._check_token_binding(doc, sub["sub_id"], sub["supplier"])
        if sub["state"] == STATE_RELEASED:
            raise SubscriptionError(
                "subscription was released; its change history is no longer "
                "retained -- create a new subscription and reload")
        if sub["state"] != STATE_LIVE:
            raise SubscriptionError(
                "initial load not completed (finish load pages and call "
                "complete_load before reading change events)")
        after_seq = doc["after_seq"]
        if after_seq < sub["cut_seq"]:
            raise SubscriptionError(
                "event cursor is behind this subscription's cut point")
        part = self.conn.execute(
            "SELECT earliest_seq,latest_seq FROM sub_partition "
            "WHERE supplier=?", (sub["supplier"],)).fetchone()
        earliest = part[0] if part else sub["cut_seq"]
        latest = part[1] if part else sub["cut_seq"]
        if after_seq < earliest - 1:
            raise SubscriptionError(
                f"event history at/after seq {after_seq + 1} has been "
                f"reclaimed (earliest retained seq is {earliest}); this "
                "cursor can no longer be resumed -- release and create a new "
                "subscription to reload from a fresh cut point")
        if after_seq > latest:
            raise SubscriptionError(
                "event cursor points past the source's current commit "
                "position; it may have been forged against another store")
        rows = self.conn.execute(
            "SELECT seq,txn_kind,payload FROM sub_events "
            "WHERE supplier=? AND seq>? ORDER BY seq LIMIT ?",
            (sub["supplier"], after_seq, page_size)).fetchall()
        events: List[Dict[str, Any]] = []
        for seq, txn_kind, payload in rows:
            ev = json.loads(payload)
            assert ev["seq"] == seq and ev["txn_kind"] == txn_kind
            events.append({"seq": seq, "txn_kind": txn_kind,
                           "committed_at": ev["committed_at"],
                           "changes": ev["changes"]})
        # Oversized-transaction exception: if the soft limit ended exactly on
        # a boundary there is nothing more to do; otherwise we already have
        # whole transactions only (each sub_events row is one transaction).
        end_seq = events[-1]["seq"] if events else after_seq
        next_after = end_seq
        next_token = self._event_token(sub["sub_id"], next_after)
        if events:
            self.conn.execute(
                "UPDATE sub_progress SET delivered_seq=? WHERE sub_id=? "
                "AND delivered_seq<?", (end_seq, sub["sub_id"], end_seq))
            self.conn.commit()
        page = sign_page({
            "format": EVENT_FORMAT,
            "protocol_version": SUB_PROTOCOL_VERSION,
            "db_uid": self.db_uid, "replica_id": self.replica_id,
            "sub_id": sub["sub_id"], "supplier": sub["supplier"],
            "begin_seq": (events[0]["seq"] if events else after_seq + 1),
            "end_seq": end_seq,
            "next_seq": next_after,
            "page_size": page_size,
            "event_count": len(events), "events": events,
            "next_token": next_token})
        return page

    def ack_events(self, token: str) -> Dict[str, Any]:
        """Advance the confirmed position for exactly this subscription.

        Rejected: positions never delivered, backwards movement, acks during
        the load phase, and acks against released/reclaimed history. Two
        consumers hold independent progress rows and can never cross wires.
        """
        doc = decode_token(token, "event")
        sub = self._get_sub(doc["sub_id"])
        self._check_token_binding(doc, sub["sub_id"], sub["supplier"])
        if sub["state"] == STATE_RELEASED:
            raise SubscriptionError(
                "subscription released; event acks are no longer accepted")
        if sub["state"] != STATE_LIVE:
            raise SubscriptionError(
                "cannot confirm events before the initial load is complete")
        seq = doc["after_seq"]
        progress = self.conn.execute(
            "SELECT delivered_seq,confirmed_seq FROM sub_progress "
            "WHERE sub_id=?", (sub["sub_id"],)).fetchone()
        delivered, confirmed = progress
        # Backwards movement is rejected against the actual confirmed
        # watermark first (the cut itself is a legal confirmed position).
        if seq < confirmed:
            raise SubscriptionError(
                f"refusing to move the cursor backwards: already confirmed "
                f"seq {confirmed}, token only reaches {seq}")
        if seq < sub["cut_seq"]:
            raise SubscriptionError("refusing to confirm behind the cut "
                                    "point")
        if seq > delivered:
            raise SubscriptionError(
                f"refusing to confirm seq {seq}: only seq {delivered} has "
                "ever been delivered to this subscription (cannot skip "
                "transactions)")
        part = self.conn.execute(
            "SELECT earliest_seq FROM sub_partition WHERE supplier=?",
            (sub["supplier"],)).fetchone()
        if part is not None and seq < part[0] - 1:
            raise SubscriptionError(
                "the history backing this position was already reclaimed; "
                "reload through a new subscription")
        self.conn.execute(
            "UPDATE sub_progress SET confirmed_seq=? WHERE sub_id=?",
            (seq, sub["sub_id"]))
        self.conn.commit()
        return {"sub_id": sub["sub_id"], "confirmed_seq": seq}

    # ------------------------------------------------------- status/release
    def backlog(self, sub_id: Optional[str] = None) -> Dict[str, Any]:
        """Show retention/backlog information for one or all subscriptions."""
        if sub_id is not None:
            subs = [self._get_sub(sub_id)]
        else:
            subs = [dict(zip(("sub_id", "supplier", "cut_seq", "state",
                              "page_size", "load_total"), r))
                    for r in self.conn.execute(
                        "SELECT sub_id,supplier,cut_seq,state,page_size,"
                        "load_total FROM sub_subscriptions ORDER BY sub_id")]
        out: Dict[str, Any] = {"db_uid": self.db_uid,
                               "replica_id": self.replica_id,
                               "subscriptions": []}
        for sub in subs:
            part = self.conn.execute(
                "SELECT earliest_seq,latest_seq FROM sub_partition "
                "WHERE supplier=?", (sub["supplier"],)).fetchone()
            earliest = part[0] if part else 0
            latest = part[1] if part else 0
            progress = self.conn.execute(
                "SELECT delivered_seq,confirmed_seq FROM sub_progress "
                "WHERE sub_id=?", (sub["sub_id"],)).fetchone()
            delivered, confirmed = progress if progress else (0, 0)
            load_acked = self.conn.execute(
                "SELECT COUNT(*) FROM sub_load_acks WHERE sub_id=?",
                (sub["sub_id"],)).fetchone()[0]
            load_pages = 0
            if sub["load_total"]:
                load_pages = ((sub["load_total"] - 1)
                              // sub["page_size"] + 1)
            retained_events = self.conn.execute(
                "SELECT COUNT(*) FROM sub_events WHERE supplier=?",
                (sub["supplier"],)).fetchone()[0]
            load_rows = self.conn.execute(
                "SELECT COUNT(*) FROM sub_load_rows WHERE sub_id=?",
                (sub["sub_id"],)).fetchone()[0]
            out["subscriptions"].append({
                "sub_id": sub["sub_id"], "supplier": sub["supplier"],
                "state": sub["state"], "cut_seq": sub["cut_seq"],
                "partition_earliest_seq": earliest,
                "partition_latest_seq": latest,
                "delivered_seq": delivered, "confirmed_seq": confirmed,
                "unconfirmed_events": max(0, latest - confirmed),
                "load_total_rows": sub["load_total"],
                "load_pages": load_pages,
                "load_pages_acked": load_acked,
                "retained_load_rows": load_rows,
                "retained_event_rows_in_partition": retained_events})
        return out

    def release_subscription(self, sub_id: str) -> Dict[str, Any]:
        """Explicitly release a subscription.

        This is the operator's accepted "a future consumer of this slot must
        reload" action: its load copy becomes reclaimable immediately, its
        tokens fail explicitly afterwards, and cleanup can drop events no
        other live subscription needs. The subscription row is kept as a
        tombstone so reused tokens get a precise error.
        """
        sub = self._get_sub(sub_id)
        if sub["state"] == STATE_RELEASED:
            return {"sub_id": sub_id, "state": STATE_RELEASED}
        self.conn.execute(
            "UPDATE sub_subscriptions SET state=?, released_at=? "
            "WHERE sub_id=?", (STATE_RELEASED, _now(), sub_id))
        self.conn.execute(
            "DELETE FROM sub_load_rows WHERE sub_id=?", (sub_id,))
        self.conn.execute(
            "DELETE FROM sub_load_acks WHERE sub_id=?", (sub_id,))
        self.conn.commit()
        # Opportunistically reclaim events that nobody still needs.
        self.cleanup()
        return {"sub_id": sub_id, "state": STATE_RELEASED}

    def cleanup(self) -> Dict[str, Any]:
        """Reclaim event history and dead metadata protected by no one.

        The retention floor per partition is the minimum confirmed position
        across its *live* subscriptions (a subscription still in its initial
        load protects everything from seq 1). Events at or below the floor
        and partition gaps below it are deleted; if any live subscription
        still needs an event it is retained regardless of age. Released
        subscriptions never protect anything. Nothing here is automatic:
        callers invoke cleanup explicitly, and protected data is never
        removed by it.
        """
        removed_events = 0
        floors: Dict[str, int] = {}
        for supplier, in self.conn.execute(
                "SELECT DISTINCT supplier FROM sub_subscriptions "
                "WHERE state=?", (STATE_LIVE,)).fetchall():
            floor = self.conn.execute(
                "SELECT MIN(p.confirmed_seq) FROM sub_progress p "
                "JOIN sub_subscriptions s ON s.sub_id=p.sub_id "
                "WHERE s.supplier=? AND s.state=?",
                (supplier, STATE_LIVE)).fetchone()[0]
            floors[supplier] = floor
        # loading subscriptions protect the entire stream from seq 1
        for supplier, in self.conn.execute(
                "SELECT DISTINCT supplier FROM sub_subscriptions "
                "WHERE state=?", (STATE_LOADING,)).fetchall():
            floors[supplier] = 0
        for supplier, floor in floors.items():
            cur = self.conn.execute(
                "DELETE FROM sub_events WHERE supplier=? AND seq<=?",
                (supplier, floor)).rowcount
            removed_events += cur
            row = self.conn.execute(
                "SELECT MIN(seq) FROM sub_events WHERE supplier=?",
                (supplier,)).fetchone()
            if row[0] is None:
                self.conn.execute(
                    "DELETE FROM sub_partition WHERE supplier=?",
                    (supplier,))
            else:
                self.conn.execute(
                    "UPDATE sub_partition SET earliest_seq=? WHERE "
                    "supplier=?", (row[0], supplier))
        # progress rows of released subscriptions are dead metadata
        removed_progress = self.conn.execute(
            "DELETE FROM sub_progress WHERE sub_id IN ("
            "SELECT sub_id FROM sub_subscriptions WHERE state=?)",
            (STATE_RELEASED,)).rowcount
        self.conn.commit()
        self.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        self.conn.execute("VACUUM")
        return {"reclaimed_event_rows": removed_events,
                "reclaimed_progress_rows": removed_progress,
                "retention_floors": floors}
